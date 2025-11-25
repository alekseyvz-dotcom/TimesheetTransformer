import os
import re
import sys
import json
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any

import tkinter as tk
from tkinter import ttk, messagebox

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from datetime import datetime, date, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse, parse_qs

# ========================= БАЗОВЫЕ КОНСТАНТЫ =========================

APP_TITLE = "Заказ питания"

CONFIG_FILE = "tabel_config.ini"
CONFIG_SECTION_UI = "UI"
CONFIG_SECTION_INTEGR = "Integrations"

KEY_SELECTED_DEP = "selected_department"
KEY_MEALS_PLANNING_ENABLED = "meals_planning_enabled"

SPRAVOCHNIK_FILE = "Справочник.xlsx"  # оставлен для совместимости, больше не используется
ORDERS_DIR = "Заявки_питание"


def exe_dir() -> Path:
    """Каталог, откуда запущена программа/скрипт."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def config_path() -> Path:
    """Путь к ini‑конфигу для fallback‑режима (без settings_manager)."""
    return exe_dir() / CONFIG_FILE


# ========================= РАБОТА С НАСТРОЙКАМИ =========================

try:
    import settings_manager as Settings
except Exception:
    Settings = None


if Settings:
    ensure_config = Settings.ensure_config
    read_config = Settings.read_config
    write_config = Settings.write_config

    def get_saved_dep() -> str:
        return Settings.get_selected_department_from_config()

    def set_saved_dep(dep: str):
        return Settings.set_selected_department_from_config(dep)

else:
    def ensure_config():
        cp = config_path()
        if cp.exists():
            cfg = configparser.ConfigParser()
            cfg.read(cp, encoding="utf-8")
            changed = False

            if not cfg.has_section(CONFIG_SECTION_UI):
                cfg[CONFIG_SECTION_UI] = {}
                changed = True
            if KEY_SELECTED_DEP not in cfg[CONFIG_SECTION_UI]:
                cfg[CONFIG_SECTION_UI][KEY_SELECTED_DEP] = "Все"
                changed = True

            if not cfg.has_section(CONFIG_SECTION_INTEGR):
                cfg[CONFIG_SECTION_INTEGR] = {}
                changed = True
            if KEY_MEALS_PLANNING_ENABLED not in cfg[CONFIG_SECTION_INTEGR]:
                cfg[CONFIG_SECTION_INTEGR][KEY_MEALS_PLANNING_ENABLED] = "true"
                changed = True

            if changed:
                with open(cp, "w", encoding="utf-8") as f:
                    cfg.write(f)
            return

        cfg = configparser.ConfigParser()
        cfg[CONFIG_SECTION_UI] = {KEY_SELECTED_DEP: "Все"}
        cfg[CONFIG_SECTION_INTEGR] = {
            KEY_MEALS_PLANNING_ENABLED: "true",
        }
        with open(cp, "w", encoding="utf-8") as f:
            cfg.write(f)

    import configparser

    def read_config() -> configparser.ConfigParser:
        ensure_config()
        cfg = configparser.ConfigParser()
        cfg.read(config_path(), encoding="utf-8")
        return cfg

    def write_config(cfg: configparser.ConfigParser):
        with open(config_path(), "w", encoding="utf-8") as f:
            cfg.write(f)

    def get_saved_dep() -> str:
        cfg = read_config()
        return cfg.get(CONFIG_SECTION_UI, KEY_SELECTED_DEP, fallback="Все")

    def set_saved_dep(dep: str):
        cfg = read_config()
        if not cfg.has_section(CONFIG_SECTION_UI):
            cfg[CONFIG_SECTION_UI] = {}
        cfg[CONFIG_SECTION_UI][KEY_SELECTED_DEP] = dep or "Все"
        write_config(cfg)


def get_meals_planning_enabled() -> bool:
    if Settings and hasattr(Settings, "get_meals_planning_enabled_from_config"):
        return Settings.get_meals_planning_enabled_from_config()
    cfg = read_config()
    v = cfg.get(CONFIG_SECTION_INTEGR, KEY_MEALS_PLANNING_ENABLED, fallback="true").strip().lower()
    return v in ("1", "true", "yes", "on")


# ========================= РАБОТА С БД =========================

def get_db_connection():
    """
    Возвращает подключение к БД на основе настроек из settings_manager.
    Ожидается provider=postgres и DATABASE_URL в формате:
      postgresql://user:password@host:port/dbname?sslmode=...
    """
    if not Settings:
        raise RuntimeError("settings_manager не доступен, не могу прочитать параметры БД")

    provider = Settings.get_db_provider().strip().lower()
    if provider != "postgres":
        raise RuntimeError(f"Ожидался provider=postgres, а в настройках: {provider!r}")

    db_url = Settings.get_database_url().strip()
    if not db_url:
        raise RuntimeError("В настройках не указана строка подключения (DATABASE_URL)")

    url = urlparse(db_url)
    if url.scheme not in ("postgresql", "postgres"):
        raise RuntimeError(f"Неверная схема в DATABASE_URL: {url.scheme}")

    user = url.username
    password = url.password
    host = url.hostname or "localhost"
    port = url.port or 5432
    dbname = url.path.lstrip("/")

    q = parse_qs(url.query)
    sslmode = (q.get("sslmode", [Settings.get_db_sslmode()])[0] or "require")

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
    )
    return conn


def get_or_create_department(cur, name: str):
    if not name:
        return None
    cur.execute("SELECT id FROM departments WHERE name = %s", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO departments (name) VALUES (%s) RETURNING id", (name,))
    return cur.fetchone()[0]


def get_or_create_object(cur, excel_id: str, address: str):
    """
    Поддержка схемы с excel_id/ excel_id.
    """
    excel_id = (excel_id or "").strip()
    address = (address or "").strip()
    if excel_id:
        # новая схема
        try:
            cur.execute("SELECT id FROM objects WHERE excel_id = %s", (excel_id,))
            row = cur.fetchone()
            if row:
                return row[0]
        except Exception:
            pass

        cur.execute("SELECT id FROM objects WHERE excel_id = %s", (excel_id,))
        row = cur.fetchone()
        if row:
            return row[0]
        # вставка
        try:
            cur.execute(
                "INSERT INTO objects (excel_id, address) VALUES (%s, %s) RETURNING id",
                (excel_id, address),
            )
        except Exception:
            cur.execute(
                "INSERT INTO objects (excel_id, address) VALUES (%s, %s) RETURNING id",
                (excel_id, address),
            )
        return cur.fetchone()[0]

    # без кода — по адресу
    cur.execute("SELECT id FROM objects WHERE address = %s", (address,))
    row = cur.fetchone()
    if row:
        return row[0]
    try:
        cur.execute(
            "INSERT INTO objects (excel_id, address) VALUES (NULL, %s) RETURNING id",
            (address,),
        )
    except Exception:
        cur.execute(
            "INSERT INTO objects (excel_id, address) VALUES (NULL, %s) RETURNING id",
            (address,),
        )
    return cur.fetchone()[0]


def get_or_create_meal_type(cur, name: str):
    name = (name or "").strip()
    if not name:
        return None
    cur.execute("SELECT id FROM meal_types WHERE name = %s", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO meal_types (name, price) VALUES (%s, 0) RETURNING id", (name,))
    return cur.fetchone()[0]


def find_employee(cur, fio: str, tbn: str = None):
    fio = (fio or "").strip()
    tbn = (tbn or "").strip()
    if tbn:
        cur.execute("SELECT id FROM employees WHERE tbn = %s", (tbn,))
        row = cur.fetchone()
        if row:
            return row[0]
    if fio:
        cur.execute("SELECT id FROM employees WHERE fio = %s", (fio,))
        row = cur.fetchone()
        if row:
            return row[0]
    return None


# ---------- Загрузка справочников из БД ----------

def load_employees_from_db() -> List[Dict[str, Any]]:
    """
    Возвращает список сотрудников:
      [{'fio': ..., 'tbn': ..., 'pos': ..., 'dep': ...}, ...]
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.fio, e.tbn, e.position, d.name AS dep
                  FROM employees e
                  LEFT JOIN departments d ON d.id = e.department_id
                 WHERE COALESCE(e.is_fired, FALSE) = FALSE
              ORDER BY e.fio
                """
            )
            res = []
            for fio, tbn, pos, dep in cur.fetchall():
                res.append({
                    "fio": fio or "",
                    "tbn": tbn or "",
                    "pos": pos or "",
                    "dep": dep or "",
                })
            return res
    finally:
        conn.close()


def load_objects_from_db() -> List[Tuple[str, str]]:
    """
    Возвращает список объектов: [(excel_id, address), ...]
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(NULLIF(excel_id, ''), '') AS code,
                    address
                  FROM objects
                 ORDER BY address
                """
            )
            res = []
            for code, addr in cur.fetchall():
                res.append((code or "", addr or ""))
            return res
    finally:
        conn.close()

def load_meal_types_from_db() -> List[Dict[str, Any]]:
    """
    Возвращает список типов питания с ценой:
      [{'id': 1, 'name': 'Одноразовое', 'price': 200.0}, ...]
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, COALESCE(price, 0) AS price
                  FROM meal_types
              ORDER BY id
                """
            )
            rows = cur.fetchall()
            if not rows:
                # создаём три стандартных типа, если таблица пустая
                defaults = [("Одноразовое", 0), ("Двухразовое", 0), ("Трехразовое", 0)]
                for name, price in defaults:
                    cur.execute(
                        "INSERT INTO meal_types (name, price) VALUES (%s, %s) RETURNING id, name, price",
                        (name, price),
                    )
                    rows.append(cur.fetchone())
                conn.commit()
            return rows
    finally:
        conn.close()

def get_meal_type_price_map() -> Dict[str, float]:
    """
    Возвращает словарь: {имя_типа_питания: цена}.
    Если в meal_types нет записи, цена считается 0.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name, COALESCE(price, 0) FROM meal_types"
            )
            return {
                (name or "").strip(): float(price or 0)
                for name, price in cur.fetchall()
            }
    finally:
        conn.close()

# ---------------- Сохранение заказов, реестры, проверки ----------------

def save_order_to_db(data: dict) -> int:
    """
    Сохраняет заявку (dict из _build_order_dict) в PostgreSQL.

    Правила:
      - Если для того же сотрудника на ТУ ЖЕ дату и ТОТ ЖЕ объект уже есть строки,
        они удаляются и записываются заново (перезапись по объекту).
      - Записи на другие объекты не трогаем.
    """
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                dept_name = (data.get("department") or "").strip()
                dept_id = get_or_create_department(cur, dept_name) if dept_name else None

                obj = data.get("object") or {}
                obj_excel_id = (obj.get("id") or "").strip()
                obj_address = (obj.get("address") or "").strip()
                object_id = get_or_create_object(cur, obj_excel_id, obj_address)

                created_at = datetime.strptime(data["created_at"], "%Y-%m-%dT%H:%M:%S")
                order_date = datetime.strptime(data["date"], "%Y-%m-%d").date()
                team_name = (data.get("team_name") or "").strip()

                cur.execute(
                    """
                    INSERT INTO meal_orders (created_at, date, department_id, team_name, object_id)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (created_at, order_date, dept_id, team_name, object_id),
                )
                order_id = cur.fetchone()[0]

                for emp in data.get("employees", []):
                    fio = (emp.get("fio") or "").strip()
                    tbn = (emp.get("tbn") or "").strip()
                    position = (emp.get("position") or "").strip()
                    meal_type_name = (emp.get("meal_type") or "").strip()
                    comment = (emp.get("comment") or "").strip()

                    meal_type_id = get_or_create_meal_type(cur, meal_type_name)
                    employee_id = find_employee(cur, fio, tbn)

                    # перезапись по этому же объекту/дате/сотруднику
                    if tbn:
                        cur.execute(
                            """
                            DELETE FROM meal_order_items moi
                            WHERE EXISTS (
                                SELECT 1
                                FROM meal_orders mo
                                LEFT JOIN employees e ON e.id = moi.employee_id
                                WHERE moi.order_id = mo.id
                                  AND mo.date = %s
                                  AND mo.object_id = %s
                                  AND (
                                       moi.tbn_text = %s
                                       OR (e.tbn IS NOT NULL AND e.tbn = %s)
                                  )
                            )
                            """,
                            (order_date, object_id, tbn, tbn),
                        )
                    else:
                        cur.execute(
                            """
                            DELETE FROM meal_order_items moi
                            WHERE EXISTS (
                                SELECT 1
                                FROM meal_orders mo
                                LEFT JOIN employees e ON e.id = moi.employee_id
                                WHERE moi.order_id = mo.id
                                  AND mo.date = %s
                                  AND mo.object_id = %s
                                  AND (
                                       moi.fio_text = %s
                                       OR (e.fio IS NOT NULL AND e.fio = %s)
                                  )
                            )
                            """,
                            (order_date, object_id, fio, fio),
                        )

                    cur.execute(
                        """
                        INSERT INTO meal_order_items
                        (order_id, employee_id, fio_text, tbn_text, position_text,
                         meal_type_id, meal_type_text, comment)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            order_id,
                            employee_id,
                            fio,
                            tbn,
                            position,
                            meal_type_id,
                            meal_type_name,
                            comment,
                        ),
                    )

        return order_id
    finally:
        conn.close()


def get_registry_from_db(
    filter_date: Optional[str] = None,
    filter_address: Optional[str] = None,
    filter_department: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Возвращает агрегированный реестр по объектам.
    Ключ: (date, address), внутри:
      - total_count
      - by_department
      - order_ids: список id заявок (по этому объекту и дате)
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            params = []
            where_clauses = []

            if filter_date:
                where_clauses.append("mo.date = %s")
                params.append(filter_date)

            if filter_address:
                where_clauses.append("o.address ILIKE %s")
                params.append(f"%{filter_address}%")

            if filter_department and filter_department.lower() != "все":
                where_clauses.append("d.name = %s")
                params.append(filter_department)

            where_sql = ""
            if where_clauses:
                where_sql = "WHERE " + " AND ".join(where_clauses)

            sql = f"""
                SELECT
                    mo.id                    AS order_id,
                    mo.date::text            AS date,
                    COALESCE(o.address, '')  AS address,
                    COALESCE(d.name, '')     AS department,
                    COALESCE(mti.name, moi.meal_type_text, '') AS meal_type
                FROM meal_orders mo
                JOIN meal_order_items moi ON moi.order_id = mo.id
                LEFT JOIN objects o       ON o.id = mo.object_id
                LEFT JOIN departments d   ON d.id = mo.department_id
                LEFT JOIN meal_types mti  ON mti.id = moi.meal_type_id
                {where_sql}
            """
            cur.execute(sql, params)
            rows = cur.fetchall()

        result: Dict[Tuple[str, str], Dict[str, Any]] = {}

        for order_id, date_str, address, dept, meal_type in rows:
            key = (date_str, address)
            rec = result.setdefault(
                key,
                {
                    "date": date_str,
                    "address": address,
                    "total_count": 0,
                    "by_department": {},
                    "order_ids": set(),  # множество id заявок
                },
            )
            rec["total_count"] += 1
            rec["order_ids"].add(order_id)

            dept_name = dept or "Без подразделения"
            by_dep = rec["by_department"].setdefault(
                dept_name,
                {
                    "total": 0,
                    "by_meal_type": {},
                },
            )
            by_dep["total"] += 1

            mt = meal_type or "Не указан"
            by_mt = by_dep["by_meal_type"]
            by_mt[mt] = by_mt.get(mt, 0) + 1

        # приводим order_ids к списку для сериализации
        for rec in result.values():
            rec["order_ids"] = list(rec["order_ids"])

        return list(result.values())

    finally:
        conn.close()

def get_details_from_db(
    filter_date: Optional[str] = None,
    filter_address: Optional[str] = None,
    filter_department: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Возвращает детализированный список заявок.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            params = []
            where_clauses = []

            if filter_date:
                where_clauses.append("mo.date = %s")
                params.append(filter_date)

            if filter_address:
                where_clauses.append("o.address ILIKE %s")
                params.append(f"%{filter_address}%")

            if filter_department and filter_department.lower() != "все":
                where_clauses.append("d.name = %s")
                params.append(filter_department)

            where_sql = ""
            if where_clauses:
                where_sql = "WHERE " + " AND ".join(where_clauses)

            sql = f"""
                SELECT
                    mo.date::text        AS date,
                    COALESCE(o.address, '')       AS address,
                    COALESCE(o.excel_id, '')        AS object_excel_id,
                    COALESCE(d.name, '')          AS department,
                    COALESCE(mo.team_name, '')    AS team_name,
                    COALESCE(moi.fio_text, '')    AS fio,
                    COALESCE(moi.tbn_text, '')    AS tbn,
                    COALESCE(moi.position_text, '') AS position,
                    COALESCE(mti.name, moi.meal_type_text, '') AS meal_type,
                    COALESCE(moi.comment, '')     AS comment
                FROM meal_orders mo
                JOIN meal_order_items moi ON moi.order_id = mo.id
                LEFT JOIN objects o       ON o.id = mo.object_id
                LEFT JOIN departments d   ON d.id = mo.department_id
                LEFT JOIN meal_types mti  ON mti.id = moi.meal_type_id
                {where_sql}
                ORDER BY mo.date, o.address, d.name, mo.team_name, moi.fio_text
            """
            cur.execute(sql, params)
            rows = cur.fetchall()

        result = []
        for r in rows:
            (
                date_str,
                address,
                object_excel_id,
                department,
                team_name,
                fio,
                tbn,
                position,
                meal_type,
                comment,
            ) = r
            result.append(
                {
                    "date": date_str,
                    "address": address,
                    "object_id": object_excel_id,
                    "department": department,
                    "team_name": team_name,
                    "fio": fio,
                    "tbn": tbn,
                    "position": position,
                    "meal_type": meal_type,
                    "comment": comment,
                }
            )
        return result

    finally:
        conn.close()


def find_conflicting_meal_orders_same_date_other_object(data: dict) -> List[Dict[str, Any]]:
    """
    Ищет в БД записи о том, что на этих же людей уже оформлено питание
    в ТУ ЖЕ дату, но на ДРУГОМ объекте.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            order_date = datetime.strptime(data["date"], "%Y-%m-%d").date()
            obj = data.get("object") or {}
            obj_excel_id = (obj.get("id") or "").strip()
            obj_address = (obj.get("address") or "").strip()

            current_object_id = get_or_create_object(cur, obj_excel_id, obj_address)

            conflicts: List[Dict[str, Any]] = []

            for emp in data.get("employees", []):
                fio = (emp.get("fio") or "").strip()
                tbn = (emp.get("tbn") or "").strip()
                if not fio and not tbn:
                    continue

                params = [order_date, current_object_id]
                where_emp = []

                if tbn:
                    where_emp.append("(moi.tbn_text = %s OR e.tbn = %s)")
                    params.extend([tbn, tbn])
                else:
                    where_emp.append("(moi.fio_text = %s OR e.fio = %s)")
                    params.extend([fio, fio])

                where_emp_sql = " AND ".join(where_emp)

                sql = f"""
                    SELECT
                        mo.date::text,
                        COALESCE(o.address, '')       AS address,
                        COALESCE(mo.team_name, '')    AS team_name,
                        COALESCE(d.name, '')          AS department
                    FROM meal_orders mo
                    JOIN meal_order_items moi ON moi.order_id = mo.id
                    LEFT JOIN employees e    ON e.id = moi.employee_id
                    LEFT JOIN objects o      ON o.id = mo.object_id
                    LEFT JOIN departments d  ON d.id = mo.department_id
                    WHERE mo.date = %s
                      AND mo.object_id <> %s
                      AND {where_emp_sql}
                    LIMIT 5
                """
                cur.execute(sql, params)
                rows = cur.fetchall()
                for r in rows:
                    date_str, addr, team_name, dep = r
                    conflicts.append(
                        {
                            "fio": fio,
                            "tbn": tbn,
                            "date": date_str,
                            "address": addr,
                            "team_name": team_name,
                            "department": dep,
                        }
                    )

            return conflicts
    finally:
        conn.close()


# ========================= УТИЛИТЫ =========================

def parse_date_any(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def post_json(url: str, payload: dict, token: str = "") -> Tuple[bool, str]:
    try:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        if token:
            sep = "&" if ("?" in url) else "?"
            url = f"{url}{sep}token={urllib.parse.quote(token)}"
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=25) as resp:
            code = resp.getcode()
            text = resp.read().decode("utf-8", errors="replace")
            return (200 <= code < 300, f"{code}: {text}")
    except urllib.error.HTTPError as e:
        try:
            txt = e.read().decode("utf-8", errors="replace")
        except Exception:
            txt = str(e)
        return (False, f"HTTPError {e.code}: {txt}")
    except Exception as e:
        return (False, f"Error: {e}")


def safe_filename(s: str, maxlen: int = 60) -> str:
    if not s:
        return "NOID"
    s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", str(s)).strip()
    s = re.sub(r"_+", "_", s)
    return s[:maxlen] if len(s) > maxlen else s


# ========================= ВИДЖЕТЫ =========================

class AutoCompleteCombobox(ttk.Combobox):
    def __init__(self, master=None, **kw):
        super().__init__(master, **kw)
        self._all_values: List[str] = []
        self.bind("<KeyRelease>", self._on_keyrelease)
        self.bind("<Control-BackSpace>", self._clear_all)

    def set_completion_list(self, values: List[str]):
        self._all_values = list(values)
        self["values"] = self._all_values

    def _clear_all(self, _=None):
        self.delete(0, tk.END)
        self["values"] = self._all_values

    def _on_keyrelease(self, event):
        if event.keysym in (
            "Up",
            "Down",
            "Left",
            "Right",
            "Home",
            "End",
            "Return",
            "Escape",
            "Tab",
        ):
            return
        typed = self.get().strip()
        if not typed:
            self["values"] = self._all_values
            return
        self["values"] = [x for x in self._all_values if typed.lower() in x.lower()]


EMP_COL_WIDTHS = {
    0: 320,
    1: 90,
    2: 230,
    3: 140,
    4: 260,
    5: 80,
}


# ========================= СТРОКА СОТРУДНИКА =========================

class EmployeeRow:
    ERR_BG = "#ffccbc"
    ZEBRA_EVEN = "#ffffff"
    ZEBRA_ODD = "#f6f8fa"

    def __init__(self, parent, idx: int, emp_names: List[str], meal_types: List[str], on_delete):
        self.parent = parent
        self.idx = idx
        self.on_delete = on_delete
        self.emp_names = emp_names
        self.meal_types = meal_types
        self.frame = tk.Frame(parent)

        self.fio_var = tk.StringVar()
        self.cmb_fio = AutoCompleteCombobox(self.frame, textvariable=self.fio_var, width=40)
        self.cmb_fio.set_completion_list(emp_names)
        self.cmb_fio.grid(row=0, column=0, padx=2, pady=1, sticky="w")

        self.lbl_tbn = tk.Label(self.frame, text="", width=12, anchor="w", bg=self.ZEBRA_EVEN)
        self.lbl_tbn.grid(row=0, column=1, padx=2, sticky="w")

        self.lbl_pos = tk.Label(self.frame, text="", width=30, anchor="w", bg=self.ZEBRA_EVEN)
        self.lbl_pos.grid(row=0, column=2, padx=2, sticky="w")

        self.cmb_meal_type = ttk.Combobox(self.frame, values=meal_types, state="readonly", width=16)
        if meal_types:
            self.cmb_meal_type.set(meal_types[0])
        self.cmb_meal_type.grid(row=0, column=3, padx=2)

        self.ent_comment = ttk.Entry(self.frame, width=32)
        self.ent_comment.grid(row=0, column=4, padx=2, sticky="w")

        self.btn_del = ttk.Button(self.frame, text="Удалить", width=9, command=self._delete)
        self.btn_del.grid(row=0, column=5, padx=2)

        for i in range(6):
            self.frame.grid_columnconfigure(i, minsize=EMP_COL_WIDTHS.get(i, 80))

    def grid(self, row: int):
        self.frame.grid(row=row, column=0, sticky="w")

    def destroy(self):
        self.frame.destroy()

    def apply_zebra(self, row0: int):
        bg = self.ZEBRA_ODD if (row0 % 2 == 1) else self.ZEBRA_EVEN
        for w in (self.cmb_fio, self.cmb_meal_type, self.ent_comment, self.lbl_tbn, self.lbl_pos):
            try:
                w.configure(background=bg)
            except Exception:
                pass

    def _delete(self):
        self.on_delete(self)

    def validate(self) -> bool:
        ok = True
        fio = (self.cmb_fio.get() or "").strip()
        if not fio:
            self._mark_err(self.cmb_fio)
            ok = False
        else:
            self._clear_err(self.cmb_fio)
        meal_type = (self.cmb_meal_type.get() or "").strip()
        if not meal_type:
            self._mark_err(self.cmb_meal_type)
            ok = False
        else:
            self._clear_err(self.cmb_meal_type)
        return ok

    def _mark_err(self, widget):
        try:
            widget.configure(background=self.ERR_BG)
        except Exception:
            pass

    def _clear_err(self, widget):
        try:
            widget.configure(background="white")
        except Exception:
            pass

    def get_dict(self) -> Dict:
        return {
            "fio": (self.cmb_fio.get() or "").strip(),
            "tbn": (self.lbl_tbn.cget("text") or "").strip(),
            "position": (self.lbl_pos.cget("text") or "").strip(),
            "meal_type": (self.cmb_meal_type.get() or "").strip(),
            "comment": (self.ent_comment.get() or "").strip(),
        }


# ========================= СТРАНИЦА СОЗДАНИЯ ЗАЯВКИ =========================

class MealOrderPage(tk.Frame):
    """Страница для создания/редактирования заявок на питание"""

    def __init__(self, master, existing_data: dict = None, order_id: int = None, on_saved=None):
        super().__init__(master, bg="#f7f7f7")
        ensure_config()
        self.base_dir = exe_dir()
        self.orders_dir = self.base_dir / ORDERS_DIR
        self.orders_dir.mkdir(parents=True, exist_ok=True)

        self.edit_order_id = order_id       # id редактируемой заявки (или None)
        self.on_saved = on_saved            # callback для обновления реестра

        self._load_refs_from_db()
        self._build_ui()

        if existing_data:
            self._fill_from_existing(existing_data)

    def _load_refs_from_db(self):
        emps = load_employees_from_db()
        self.emps = emps
        self.emp_by_fio = {r["fio"]: r for r in emps}

        self.objects = load_objects_from_db()
        self.meal_types_full = load_meal_types_from_db()
        self.meal_types = [mt["name"] for mt in self.meal_types_full] or [
            "Одноразовое",
            "Двухразовое",
            "Трехразовое",
        ]

        self.deps = ["Все"] + sorted(
            {(r["dep"] or "").strip() for r in self.emps if (r["dep"] or "").strip()}
        )
        self.emp_names_all = [r["fio"] for r in self.emps]

        self.addr_to_ids: Dict[str, List[str]] = {}
        for oid, addr in self.objects:
            if not addr:
                continue
            self.addr_to_ids.setdefault(addr, [])
            if oid and oid not in self.addr_to_ids[addr]:
                self.addr_to_ids[addr].append(oid)
        addresses_set = set(self.addr_to_ids.keys())
        addresses_set.update(addr for _, addr in self.objects if addr)
        self.addresses = sorted(addresses_set)

    def _build_ui(self):
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(fill="x", padx=10, pady=8)

        tk.Label(top, text="Дата заказа*:", bg="#f7f7f7").grid(row=0, column=0, sticky="w")
        self.ent_date = ttk.Entry(top, width=12)
        self.ent_date.grid(row=0, column=1, sticky="w", padx=(4, 12))
        self.ent_date.insert(0, (date.today() + timedelta(days=1)).strftime("%Y-%m-%d"))
        self.ent_date.bind("<KeyRelease>", lambda e: self._update_date_hint())
        self.ent_date.bind("<FocusOut>", lambda e: self._update_date_hint())

        tk.Label(top, text="Подразделение*:", bg="#f7f7f7").grid(row=0, column=2, sticky="w")
        self.cmb_dep = ttk.Combobox(top, state="readonly", values=self.deps, width=40)
        saved_dep = get_saved_dep()
        self.cmb_dep.set(saved_dep if saved_dep in self.deps else self.deps[0])
        self.cmb_dep.grid(row=0, column=3, columnspan=3, sticky="we", padx=(4, 12))

        def on_dep_changed(event=None):
            set_saved_dep(self.cmb_dep.get())
            for r in self.emp_rows:
                r.destroy()
            self.emp_rows.clear()
            self.add_employee()

        self.cmb_dep.bind("<<ComboboxSelected>>", on_dep_changed)

        tk.Label(top, text="Адрес объекта*:", bg="#f7f7f7").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.cmb_address = AutoCompleteCombobox(top, width=40)
        self.cmb_address.set_completion_list(self.addresses)
        self.cmb_address.grid(row=1, column=1, columnspan=2, sticky="we", padx=(4, 12), pady=(8, 0))
        self.cmb_address.bind("<<ComboboxSelected>>", lambda e: self._sync_ids_by_address())
        self.cmb_address.bind("<FocusOut>", lambda e: self._sync_ids_by_address())
        self.cmb_address.bind("<Return>", lambda e: self._sync_ids_by_address())

        tk.Label(top, text="ID объекта:", bg="#f7f7f7").grid(
            row=1,
            column=3,
            sticky="e",
            pady=(8, 0),
            padx=(0, 4),
        )
        self.cmb_object_id = ttk.Combobox(top, state="readonly", values=[], width=18)
        self.cmb_object_id.grid(row=1, column=4, sticky="w", padx=(4, 0), pady=(8, 0))

        self.lbl_date_hint = tk.Label(top, text="", fg="#555", bg="#f7f7f7")
        self.lbl_date_hint.grid(row=1, column=5, sticky="w", padx=(12, 0), pady=(8, 0))

        tk.Label(top, text="Наименование бригады*:", bg="#f7f7f7").grid(
            row=2, column=0, sticky="w", pady=(8, 0)
        )
        self.ent_team = ttk.Entry(top, width=60)
        self.ent_team.grid(row=2, column=1, columnspan=5, sticky="we", padx=(4, 12), pady=(8, 0))

        emp_wrap = tk.LabelFrame(self, text="Сотрудники")
        emp_wrap.pack(fill="both", expand=True, padx=10, pady=(6, 8))

        hdr = tk.Frame(emp_wrap)
        hdr.pack(fill="x")

        for i in range(6):
            hdr.grid_columnconfigure(i, minsize=EMP_COL_WIDTHS.get(i, 80))

        tk.Label(hdr, text="ФИО сотрудника*", anchor="w").grid(row=0, column=0, padx=2)
        tk.Label(hdr, text="Таб. №", anchor="w").grid(row=0, column=1, padx=2)
        tk.Label(hdr, text="Должность", anchor="w").grid(row=0, column=2, padx=2)
        tk.Label(hdr, text="Тип питания*", anchor="w").grid(row=0, column=3, padx=2)
        tk.Label(hdr, text="Комментарий", anchor="w").grid(row=0, column=4, padx=2)
        tk.Label(hdr, text="Действие", anchor="center").grid(row=0, column=5, padx=2)

        wrap = tk.Frame(emp_wrap)
        wrap.pack(fill="both", expand=True)
        self.cv = tk.Canvas(wrap, borderwidth=0, highlightthickness=0)
        self.rows_holder = tk.Frame(self.cv)
        self.cv.create_window((0, 0), window=self.rows_holder, anchor="nw")
        self.cv.pack(side="left", fill="both", expand=True)
        self.vscroll = ttk.Scrollbar(wrap, orient="vertical", command=self.cv.yview)
        self.vscroll.pack(side="right", fill="y")
        self.cv.configure(yscrollcommand=self.vscroll.set)
        self.rows_holder.bind(
            "<Configure>",
            lambda e: self.cv.configure(scrollregion=self.cv.bbox("all")),
        )
        self.cv.bind(
            "<MouseWheel>",
            lambda e: (self.cv.yview_scroll(int(-1 * (e.delta / 120)), "units"), "break"),
        )

        self.emp_rows: List[EmployeeRow] = []
        btns = tk.Frame(emp_wrap)
        btns.pack(fill="x")
        ttk.Button(btns, text="Добавить сотрудника", command=self.add_employee).pack(
            side="left", padx=2, pady=4
        )
        ttk.Button(btns, text="Добавить подразделение", command=self.add_department).pack(
            side="left", padx=4, pady=4
        )

        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(bottom, text="Сохранить заявку", command=self.save_order).pack(
            side="left", padx=4
        )
        ttk.Button(bottom, text="Очистить форму", command=self.clear_form).pack(
            side="left", padx=4
        )
        ttk.Button(bottom, text="Открыть папку заявок", command=self.open_orders_dir).pack(
            side="left", padx=4
        )

        for c in range(6):
            top.grid_columnconfigure(c, weight=0)
        top.grid_columnconfigure(1, weight=1)
        top.grid_columnconfigure(3, weight=1)
        top.grid_columnconfigure(5, weight=0)

        self._update_emp_list()
        self._update_date_hint()
        self.add_employee()

    def _fill_from_existing(self, data: dict):
        # дата
        self.ent_date.delete(0, "end")
        self.ent_date.insert(0, data.get("date", ""))
        # подразделение
        dep = data.get("department", "") or "Все"
        if dep not in self.deps:
            self.deps.append(dep)
            self.cmb_dep["values"] = self.deps
        self.cmb_dep.set(dep)
        # адрес и ID объекта
        obj = data.get("object") or {}
        addr = obj.get("address", "") or ""
        oid = obj.get("id", "") or ""
        if addr and addr not in self.addresses:
            self.addresses.append(addr)
            self.addresses.sort()
            self.cmb_address.set_completion_list(self.addresses)
        self.cmb_address.set(addr)
        self._sync_ids_by_address()
        if oid:
            # добавить id в список, если его там нет
            ids = list(self.cmb_object_id["values"])
            if oid and oid not in ids:
                ids.append(oid)
                self.cmb_object_id["values"] = ids
            self.cmb_object_id.set(oid)

        # бригада
        self.ent_team.delete(0, "end")
        self.ent_team.insert(0, data.get("team_name", ""))

        # сотрудники
        for r in self.emp_rows:
            r.destroy()
        self.emp_rows.clear()

        for emp in data.get("employees", []):
            self.add_employee()
            row = self.emp_rows[-1]
            row.fio_var.set(emp.get("fio", ""))
            self._fill_emp_info(row)
            # тип питания
            mt = (emp.get("meal_type") or "").strip()
            if mt and mt not in self.meal_types:
                self.meal_types.append(mt)
                for r in self.emp_rows:
                    r.cmb_meal_type["values"] = self.meal_types
            row.cmb_meal_type.set(mt or self.meal_types[0])
            # комментарий
            row.ent_comment.delete(0, "end")
            row.ent_comment.insert(0, emp.get("comment", ""))

        self._update_date_hint()

    def _build_order_dict_core(self) -> Dict:
        req_date = parse_date_any(self.ent_date.get()) or date.today()
        addr = (self.cmb_address.get() or "").strip()
        oid = (self.cmb_object_id.get() or "").strip()
        employees = [r.get_dict() for r in self.emp_rows]
        return {
            "date": req_date.strftime("%Y-%m-%d"),
            "department": (self.cmb_dep.get() or "").strip(),
            "team_name": (self.ent_team.get() or "").strip(),
            "object": {"id": oid, "address": addr},
            "employees": employees,
        }

    def _build_order_dict(self) -> Dict:
        core = self._build_order_dict_core()
        if self.edit_order_id:
            # при редактировании created_at можно не менять
            core["created_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        else:
            core["created_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        return core

    def _update_emp_list(self):
        dep = (self.cmb_dep.get() or "Все").strip()
        if dep == "Все":
            names = [r["fio"] for r in self.emps]
        else:
            names = [r["fio"] for r in self.emps if (r["dep"] or "").strip() == dep]
        seen, filtered = set(), []
        for n in names:
            if n not in seen:
                seen.add(n)
                filtered.append(n)
        if not filtered and dep != "Все":
            filtered = [r["fio"] for r in self.emps]
        for row in self.emp_rows:
            row.cmb_fio.set_completion_list(filtered)

    def _update_date_hint(self):
        try:
            req = parse_date_any(self.ent_date.get())
            today = date.today()
            tomorrow = today + timedelta(days=1)
            if req is None:
                self.lbl_date_hint.config(
                    text="Укажите дату в формате YYYY-MM-DD или DD.MM.YYYY",
                    fg="#b00020",
                )
            elif req < tomorrow:
                self.lbl_date_hint.config(
                    text=f"Заявка возможна только на {tomorrow.strftime('%Y-%m-%d')} и позже",
                    fg="#b00020",
                )
            else:
                self.lbl_date_hint.config(
                    text="Ок: заявка на допустимую дату",
                    fg="#2e7d32",
                )
        except Exception:
            self.lbl_date_hint.config(text="", fg="#555")

    def _sync_ids_by_address(self):
        addr = (self.cmb_address.get() or "").strip()
        ids = sorted(self.addr_to_ids.get(addr, []))
        if ids:
            self.cmb_object_id.config(state="readonly", values=ids)
            if self.cmb_object_id.get() not in ids:
                self.cmb_object_id.set(ids[0])
        else:
            self.cmb_object_id.config(state="normal", values=[])
            self.cmb_object_id.set("")

    def add_employee(self):
        dep = (self.cmb_dep.get() or "Все").strip()
        if dep == "Все":
            names = [r["fio"] for r in self.emps]
        else:
            names = [r["fio"] for r in self.emps if (r["dep"] or "").strip() == dep]
        seen, filtered = set(), []
        for n in names:
            if n not in seen:
                seen.add(n)
                filtered.append(n)
        row = EmployeeRow(
            self.rows_holder,
            len(self.emp_rows) + 1,
            filtered,
            self.meal_types,
            self.delete_employee,
        )
        row.grid(len(self.emp_rows))
        row.apply_zebra(len(self.emp_rows))

        row.cmb_fio.bind(
            "<<ComboboxSelected>>", lambda e, r=row: self._fill_emp_info(r)
        )
        row.cmb_fio.bind("<FocusOut>", lambda e, r=row: self._fill_emp_info(r))

        self.emp_rows.append(row)

    def delete_employee(self, emp_row: EmployeeRow):
        try:
            self.emp_rows.remove(emp_row)
        except Exception:
            pass
        emp_row.destroy()
        for i, r in enumerate(self.emp_rows, start=0):
            r.grid(i)
            r.apply_zebra(i)

    def _validate_form(self) -> bool:
        req = parse_date_any(self.ent_date.get())
        today = date.today()
        tomorrow = today + timedelta(days=1)
        if req is None or req < tomorrow:
            messagebox.showwarning(
                "Заявка",
                f"Дата заявки должна быть не раньше {tomorrow.strftime('%Y-%m-%d')}.",
            )
            return False
        if not (self.cmb_dep.get() or "").strip():
            messagebox.showwarning("Заявка", "Выберите Подразделение.")
            return False
        addr = (self.cmb_address.get() or "").strip()
        if not addr:
            messagebox.showwarning("Заявка", "Укажите Адрес объекта.")
            return False
        team_name = (self.ent_team.get() or "").strip()
        if not team_name:
            messagebox.showwarning("Заявка", "Укажите Наименование бригады.")
            return False
        if not self.emp_rows:
            messagebox.showwarning("Заявка", "Добавьте хотя бы одного сотрудника.")
            return False
        all_ok = True
        for r in self.emp_rows:
            all_ok = r.validate() and all_ok
        if not all_ok:
            messagebox.showwarning(
                "Заявка",
                "Исправьте подсвеченные поля (ФИО и Тип питания обязательны).",
            )
            return False
        return True

    def _fill_emp_info(self, row: EmployeeRow):
        fio = row.fio_var.get().strip()
        info = self.emp_by_fio.get(fio)
        if not info:
            row.lbl_tbn.config(text="")
            row.lbl_pos.config(text="")
            return
        row.lbl_tbn.config(text=info.get("tbn", ""))
        row.lbl_pos.config(text=info.get("pos", ""))

    def save_order(self):
        if not self._validate_form():
            return

        data = self._build_order_dict()
        total_items = len(data.get("employees", []))

        # проверка пересечений как было
        try:
            conflicts = find_conflicting_meal_orders_same_date_other_object(data)
        except Exception as e:
            if not messagebox.askokcancel(
                "Проверка пересечений",
                f"Не удалось проверить пересечения по БД:\n{e}\n\n"
                f"Нажмите «ОК», чтобы продолжить сохранение,\n"
                f"или «Отмена», чтобы вернуться к заявке.",
            ):
                return
            conflicts = []

        if conflicts:
            # ... блок с текстом остался без изменений ...
            lines = []
            for c in conflicts:
                fio = c.get("fio") or "?"
                tbn = c.get("tbn") or ""
                who = f"{fio} ({tbn})" if tbn else fio
                addr = c.get("address") or "неизвестный адрес"
                date_str = c.get("date") or ""
                team = c.get("team_name") or ""
                dep = c.get("department") or ""
                extra = f", бригада: {team}" if team else ""
                if dep:
                    extra += f", подразделение: {dep}"
                lines.append(f"- {who}: {date_str}, объект: {addr}{extra}")

            text = (
                "Обнаружены сотрудники, на которых в ЭТУ ЖЕ дату уже заказано питание\n"
                "на ДРУГОМ объекте:\n\n"
                + "\n".join(lines[:20])
                + ("\n\n(Показаны первые 20 совпадений)" if len(lines) > 20 else "")
                + "\n\nНажмите «ОК», чтобы всё равно добавить текущую заявку в реестр,\n"
                "или «Отмена», чтобы вернуться к редактированию заявки."
            )

            if not messagebox.askokcancel(
                "Пересечение заявок по сотрудникам", text
            ):
                return

        try:
            if self.edit_order_id:
                # редактирование: удаляем старые строки, перезаписываем items,
                # при желании можно также обновить заголовок заявки (date, dep, object ...)
                delete_order_items_from_db(self.edit_order_id)

                # сохраняем только строки сотрудников, используя уже существующий order_id
                conn = get_db_connection()
                try:
                    with conn:
                        with conn.cursor() as cur:
                            dept_name = (data.get("department") or "").strip()
                            dept_id = get_or_create_department(cur, dept_name) if dept_name else None

                            obj = data.get("object") or {}
                            obj_excel_id = (obj.get("id") or "").strip()
                            obj_address = (obj.get("address") or "").strip()
                            object_id = get_or_create_object(cur, obj_excel_id, obj_address)

                            order_date = datetime.strptime(data["date"], "%Y-%m-%d").date()
                            team_name = (data.get("team_name") or "").strip()

                            # обновляем заголовок заявки
                            cur.execute(
                                """
                                UPDATE meal_orders
                                   SET date = %s,
                                       department_id = %s,
                                       team_name = %s,
                                       object_id = %s
                                 WHERE id = %s
                                """,
                                (order_date, dept_id, team_name, object_id, self.edit_order_id),
                            )

                            # вставляем новые строки сотрудников
                            for emp in data.get("employees", []):
                                fio = (emp.get("fio") or "").strip()
                                tbn = (emp.get("tbn") or "").strip()
                                position = (emp.get("position") or "").strip()
                                meal_type_name = (emp.get("meal_type") or "").strip()
                                comment = (emp.get("comment") or "").strip()

                                meal_type_id = get_or_create_meal_type(cur, meal_type_name)
                                employee_id = find_employee(cur, fio, tbn)

                                cur.execute(
                                    """
                                    INSERT INTO meal_order_items
                                    (order_id, employee_id, fio_text, tbn_text, position_text,
                                     meal_type_id, meal_type_text, comment)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                    """,
                                    (
                                        self.edit_order_id,
                                        employee_id,
                                        fio,
                                        tbn,
                                        position,
                                        meal_type_id,
                                        meal_type_name,
                                        comment,
                                    ),
                                )
                finally:
                    conn.close()
                order_db_id = self.edit_order_id
            else:
                # создание новой заявки — старое поведение
                order_db_id = save_order_to_db(data)

        except Exception as e:
            messagebox.showerror(
                "Сохранение в БД",
                f"Не удалось сохранить заявку в базу данных:\n{e}",
            )
            return

        # формирование XLSX можно оставить как есть при создании,
        # при редактировании можно либо не создавать новый файл, либо оставить поведение
        # ниже – без изменений
        ts = datetime.now().strftime("%H%M%S")
        id_part = data["object"]["id"] or safe_filename(data["object"]["address"])
        fname = f"Заявка_питание_{data['date']}_{ts}_{id_part or 'NOID'}.xlsx"
        fpath = self.orders_dir / fname

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Заявка"
            ws.append(["Создано", data["created_at"]])
            ws.append(["Дата", data["date"]])
            ws.append(["Подразделение", data["department"]])
            ws.append(["Наименование бригады", data.get("team_name", "")])
            ws.append(["ID объекта", data["object"]["id"]])
            ws.append(["Адрес", data["object"]["address"]])
            ws.append([])
            hdr = ["#", "ФИО", "Тип питания", "Комментарий"]
            ws.append(hdr)
            for i, emp in enumerate(data["employees"], start=1):
                ws.append([i, emp["fio"], emp["meal_type"], emp["comment"]])
            for col, w in enumerate([4, 40, 20, 40], start=1):
                ws.column_dimensions[get_column_letter(col)].width = w
            ws.freeze_panes = "A8"
            wb.save(fpath)
        except Exception as e:
            messagebox.showerror("Сохранение", f"Не удалось сохранить XLSX:\n{e}")
            return

        messagebox.showinfo(
            "Сохранение",
            f"Заявка сохранена в реестр.\n"
            f"Файл:\n{fpath}\n\n"
            f"Сохранено записей: {total_items}",
        )

        if self.on_saved:
            self.on_saved()

    def clear_form(self):
        self.ent_date.delete(0, "end")
        self.ent_date.insert(0, (date.today() + timedelta(days=1)).strftime("%Y-%m-%d"))
        self.cmb_address.set("")
        self.cmb_object_id.config(values=[])
        self.cmb_object_id.set("")
        self.ent_team.delete(0, "end")
        for r in self.emp_rows:
            r.destroy()
        self.emp_rows.clear()
        self.add_employee()
        self._update_date_hint()

    def add_department(self):
        dep = (self.cmb_dep.get() or "Все").strip()

        if dep == "Все":
            candidates = self.emps[:]
        else:
            candidates = [e for e in self.emps if (e["dep"] or "").strip() == dep]

        if not candidates:
            messagebox.showinfo("Питание", f"В подразделении «{dep}» нет сотрудников.")
            return

        existing_fio = {
            row.cmb_fio.get().strip()
            for row in self.emp_rows
            if row.cmb_fio.get().strip()
        }
        added = 0

        for e in candidates:
            fio = e["fio"]
            if fio in existing_fio:
                continue
            row = EmployeeRow(
                self.rows_holder,
                len(self.emp_rows) + 1,
                [],
                self.meal_types,
                self.delete_employee,
            )
            row.grid(len(self.emp_rows))
            row.apply_zebra(len(self.emp_rows))
            row.fio_var.set(fio)
            self._fill_emp_info(row)
            self.emp_rows.append(row)
            existing_fio.add(fio)
            added += 1

        self._update_emp_list()
        messagebox.showinfo("Питание", f"Добавлено сотрудников: {added}")

    def open_orders_dir(self):
        try:
            os.startfile(self.orders_dir)
        except Exception as e:
            messagebox.showerror("Папка", f"Не удалось открыть папку:\n{e}")


# ========================= СТРАНИЦА ПЛАНИРОВАНИЯ ПИТАНИЯ =========================

class MealPlanningPage(tk.Frame):
    """Страница планирования питания"""

    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        self.row_meta: Dict[str, Dict[str, Any]] = {}
        self._build_ui()

    def _build_ui(self):
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(fill="x", padx=10, pady=8)

        tk.Label(top, text="Дата:", bg="#f7f7f7").grid(row=0, column=0, sticky="w")
        self.ent_filter_date = ttk.Entry(top, width=12)
        self.ent_filter_date.grid(row=0, column=1, padx=4)
        self.ent_filter_date.insert(0, date.today().strftime("%Y-%m-%d"))

        tk.Label(top, text="Подразделение:", bg="#f7f7f7").grid(
            row=0, column=2, sticky="w", padx=(12, 0)
        )
        # возможные подразделения можно отдельно тянуть из БД;
        # для простоты вводим вручную
        self.cmb_filter_dep = ttk.Combobox(top, state="readonly", values=["Все"], width=20)
        self.cmb_filter_dep.grid(row=0, column=3, padx=4)
        self.cmb_filter_dep.set("Все")

        tk.Label(top, text="Адрес:", bg="#f7f7f7").grid(
            row=0, column=4, sticky="w", padx=(12, 0)
        )
        self.ent_filter_address = ttk.Entry(top, width=30)
        self.ent_filter_address.grid(row=0, column=5, padx=4)

        ttk.Button(top, text="🔄 Загрузить реестр", command=self.load_registry).grid(
            row=0, column=6, padx=12
        )
        ttk.Button(top, text="📊 Сформировать Excel", command=self.export_to_excel).grid(
            row=0, column=7, padx=4
        )

        ttk.Button(top, text="Заявка поставщика", command=self.export_supplier_order).grid(
            row=0, column=8, padx=4
        )

        table_frame = tk.LabelFrame(self, text="Реестр заказа питания по объектам")
        table_frame.pack(fill="both", expand=True, padx=10, pady=8)

        columns = ("date", "address", "total_count", "details")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=15)

        headers = {
            "date": "Дата",
            "address": "Адрес объекта",
            "total_count": "Всего заявок",
            "details": "Детали (двойной клик)",
        }
        widths = {"date": 100, "address": 400, "total_count": 120, "details": 300}

        for col in columns:
            self.tree.heading(col, text=headers.get(col, col))
            self.tree.column(col, width=widths.get(col, 100))

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)

        self.tree.bind("<Double-1>", self.on_row_double_click)

    def export_supplier_order(self):
        """
        Формирует Excel:
          Заголовок: "Заявка питания на <дата>"
          Далее итоги по видам питания (Одноразовое, Двухразовое, ...).
          Ниже таблица:
            Объект (адрес) | Бригада | Тип питания | Количество
        """
        try:
            filter_date = self.ent_filter_date.get().strip()
            if not filter_date:
                messagebox.showwarning("Заявка поставщика", "Укажите дату фильтра.")
                return

            orders = get_details_from_db(
                filter_date=filter_date or None,
                filter_address=None,
                filter_department=None,
            )

            if not orders:
                messagebox.showinfo(
                    "Заявка поставщика",
                    "Нет данных по указанной дате.",
                )
                return

            # агрегируем по видам питания (для шапки)
            total_by_type: Dict[str, int] = {}
            # агрегируем для таблицы: (адрес, бригада, тип) -> количество
            per_object_team_type: Dict[tuple, int] = {}

            for o in orders:
                mt = (o.get("meal_type") or "").strip() or "Не указан"
                addr = (o.get("address") or "").strip()
                team = (o.get("team_name") or "").strip()
                key_global = mt
                total_by_type[key_global] = total_by_type.get(key_global, 0) + 1

                key_row = (addr, team, mt)
                per_object_team_type[key_row] = per_object_team_type.get(key_row, 0) + 1

            # формируем Excel
            wb = Workbook()
            ws = wb.active
            ws.title = "Заявка поставщика"

            # Заголовок
            ws.append([f"Заявка питания на {filter_date}"])
            ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=4)

            ws.append([])  # пустая строка

            # Итоги по типам питания
            ws.append(["Итоги по видам питания"])
            ws.merge_cells(start_row=3, start_column=1, end_row=3, end_column=4)
            ws.append(["Тип питания", "Кол-во человек"])
            for mt, cnt in sorted(total_by_type.items()):
                ws.append([mt, cnt])

            ws.append([])

            # Таблица с разбивкой по объектам/бригадам/типам
            ws.append(["Объект (адрес)", "Бригада", "Тип питания", "Кол-во человек"])
            for (addr, team, mt), cnt in sorted(per_object_team_type.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
                ws.append([addr, team, mt, cnt])

            # ширины колонок
            widths = [40, 30, 20, 18]
            for col, width in enumerate(widths, start=1):
                ws.column_dimensions[get_column_letter(col)].width = width

            ws.freeze_panes = "A8"

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"Заявка_поставщика_{filter_date}_{ts}.xlsx"
            fpath = exe_dir() / ORDERS_DIR / fname
            fpath.parent.mkdir(parents=True, exist_ok=True)
            wb.save(fpath)

            messagebox.showinfo(
                "Заявка поставщика",
                f"Файл сформирован:\n{fpath}",
            )

            try:
                os.startfile(fpath)
            except Exception:
                pass

        except Exception as e:
            messagebox.showerror(
                "Заявка поставщика",
                f"Ошибка формирования Excel:\n{e}",
            )

    def load_registry(self):
        try:
            filter_date = self.ent_filter_date.get().strip()
            filter_address = self.ent_filter_address.get().strip()
            filter_dep = self.cmb_filter_dep.get().strip()

            registry = get_registry_from_db(
                filter_date=filter_date or None,
                filter_address=filter_address or None,
                filter_department=filter_dep or None,
            )

            self._populate_tree(registry)
            messagebox.showinfo("Загрузка", f"Загружено объектов: {len(registry)}")
        except Exception as e:
            messagebox.showerror(
                "Ошибка", f"Не удалось загрузить реестр из БД:\n{e}"
            )

    def _populate_tree(self, registry: List[Dict]):
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.row_meta = {}

        for entry in registry:
            req_date = entry.get("date", "")
            address = entry.get("address", "")
            total = entry.get("total_count", 0)
            details_text = self._format_details(entry.get("by_department", {}))
            item_id = self.tree.insert(
                "", "end", values=(req_date, address, total, details_text)
            )
            self.row_meta[item_id] = entry

    def _format_details(self, by_dept: Dict) -> str:
        if not by_dept:
            return "Нет данных"
        parts = []
        for dept, data in by_dept.items():
            total = data.get("total", 0)
            parts.append(f"{dept}: {total} чел.")
        return " | ".join(parts[:3]) + (" ..." if len(parts) > 3 else "")

    def on_row_double_click(self, event):
        selection = self.tree.selection()
        if not selection:
            return
        item_id = selection[0]
        entry = self.row_meta.get(item_id)
        if not entry:
            return

        order_ids = entry.get("order_ids") or []
        if not order_ids:
            messagebox.showinfo("Заявка", "Для этого объекта нет связанных заявок.")
            return

        # Если на объекте несколько заявок, пока откроем первую.
        # При необходимости можно сделать выбор конкретной.
        order_id = order_ids[0]

        try:
            order_data = get_order_with_items_from_db(order_id)
        except Exception as e:
            messagebox.showerror(
                "Загрузка заявки",
                f"Не удалось загрузить заявку id={order_id}:\n{e}",
                parent=self,
            )
            return

        # Окно с формой заявки
        win = tk.Toplevel(self)
        win.title(f"Редактирование заявки #{order_id}")
        win.geometry("1100x720")

        def on_saved_callback():
            # после сохранения перезагружаем реестр
            self.load_registry()

        page = MealOrderPage(win, existing_data=order_data, order_id=order_id, on_saved=on_saved_callback)
        page.pack(fill="both", expand=True)


    def _show_details_dialog(self, entry: Dict):
        dialog = tk.Toplevel(self)
        dialog.title("Детальная информация")
        dialog.geometry("800x600")
        dialog.resizable(True, True)
        dialog.transient(self)
        dialog.grab_set()

        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (800 // 2)
        y = (dialog.winfo_screenheight() // 2) - (600 // 2)
        dialog.geometry(f"800x600+{x}+{y}")

        header = tk.Frame(dialog, bg="#e3f2fd", relief="solid", borderwidth=1)
        header.pack(fill="x", padx=0, pady=0)
        tk.Label(
            header,
            text=f"📅 Дата: {entry.get('date', '')} | 📍 {entry.get('address', '')}",
            font=("Arial", 12, "bold"),
            bg="#e3f2fd",
            fg="#0066cc",
            padx=15,
            pady=12,
        ).pack(anchor="w")

        info_frame = tk.Frame(dialog, bg="#f7f7f7")
        info_frame.pack(fill="x", padx=15, pady=10)
        tk.Label(
            info_frame,
            text=f"Всего заявок: {entry.get('total_count', 0)} человек",
            font=("Arial", 11, "bold"),
            bg="#f7f7f7",
        ).pack(anchor="w")

        table_frame = tk.LabelFrame(
            dialog,
            text="Детализация по подразделениям и типам питания",
            padx=10,
            pady=10,
        )
        table_frame.pack(fill="both", expand=True, padx=15, pady=(0, 10))

        columns = ("department", "meal_type", "count")
        tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=15)
        tree.heading("department", text="Подразделение")
        tree.heading("meal_type", text="Тип питания")
        tree.heading("count", text="Количество")
        tree.column("department", width=300)
        tree.column("meal_type", width=200)
        tree.column("count", width=100)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        by_dept = entry.get("by_department", {})
        totals_by_type = {}

        for dept, data in sorted(by_dept.items()):
            by_type = data.get("by_meal_type", {})
            for meal_type, count in sorted(by_type.items()):
                tree.insert("", "end", values=(dept, meal_type, count))
                totals_by_type[meal_type] = totals_by_type.get(meal_type, 0) + count

        if totals_by_type:
            tree.insert("", "end", values=("", "", ""), tags=("separator",))
            tree.tag_configure("separator", background="#e0e0e0")
            for meal_type, total in sorted(totals_by_type.items()):
                tree.insert(
                    "", "end", values=("ИТОГО", meal_type, total), tags=("total",)
                )
            tree.tag_configure(
                "total", background="#fff3cd", font=("Arial", 9, "bold")
            )

        ttk.Button(dialog, text="Закрыть", command=dialog.destroy, width=20).pack(
            pady=15
        )

    def export_to_excel(self):
        try:
            filter_date = self.ent_filter_date.get().strip()
            filter_address = self.ent_filter_address.get().strip()
            filter_dep = self.cmb_filter_dep.get().strip()

            orders = get_details_from_db(
                filter_date=filter_date or None,
                filter_address=filter_address or None,
                filter_department=filter_dep or None,
            )

            if not orders:
                messagebox.showinfo(
                    "Экспорт",
                    "Нет данных для экспорта (по заданным фильтрам)",
                )
                return

            # цены типов питания
            price_map = get_meal_type_price_map()

            # ---------- определяем дубликаты по (ФИО, Таб.№) ----------
            freq: Dict[tuple, int] = {}
            for o in orders:
                fio = (o.get("fio") or "").strip()
                tbn = (o.get("tbn") or "").strip()
                key = (fio.lower(), tbn.lower())
                if fio or tbn:
                    freq[key] = freq.get(key, 0) + 1

            duplicates_mark: List[str] = []
            for o in orders:
                fio = (o.get("fio") or "").strip()
                tbn = (o.get("tbn") or "").strip()
                key = (fio.lower(), tbn.lower())
                mark = "дубль" if (fio or tbn) and freq.get(key, 0) > 1 else ""
                duplicates_mark.append(mark)

            # ---------- формируем Excel ----------
            wb = Workbook()
            ws = wb.active
            ws.title = "Реестр питания"

            # 1) Свод по объектам и типам питания: считаем и людей, и сумму
            summary: Dict[str, Dict[str, Dict[str, float]]] = {}
            # summary[addr][meal_type] = {"count": N, "amount": S}

            for o in orders:
                addr = o.get("address", "") or ""
                mt = (o.get("meal_type", "") or "").strip()
                if not addr or not mt:
                    continue
                price = price_map.get(mt, 0.0)

                addr_dict = summary.setdefault(addr, {})
                mt_dict = addr_dict.setdefault(mt, {"count": 0.0, "amount": 0.0})
                mt_dict["count"] += 1.0
                mt_dict["amount"] += price

            ws.append(["Свод по объектам, типам питания и стоимости"])
            ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=4)
            ws.append(["Адрес", "Тип питания", "Кол-во человек", "Сумма, руб."])

            for addr, by_type in summary.items():
                for mt, agg in by_type.items():
                    ws.append([
                        addr,
                        mt,
                        agg["count"],
                        agg["amount"],
                    ])

            ws.append([])

            # 2) Детальный список
            headers = [
                "Дата",
                "Адрес",
                "ID объекта",
                "Подразделение",
                "Наименование бригады",
                "ФИО",
                "Табельный №",
                "Должность",
                "Тип питания",
                "Цена, руб.",
                "Сумма, руб.",
                "Комментарий",
                "Дубликаты",
            ]
            ws.append(headers)

            for order, mark in zip(orders, duplicates_mark):
                mt = (order.get("meal_type") or "").strip()
                price = float(price_map.get(mt, 0.0))
                amount = price  # если когда-нибудь появится поле "количество", можно перемножить

                ws.append([
                    order.get("date", ""),
                    order.get("address", ""),
                    order.get("object_id", ""),
                    order.get("department", ""),
                    order.get("team_name", ""),
                    order.get("fio", ""),
                    order.get("tbn", ""),
                    order.get("position", ""),
                    mt,
                    price,
                    amount,
                    order.get("comment", ""),
                    mark,
                ])

            # подстроим ширины с учётом новых двух столбцов цены/суммы
            widths = [
                12,  # Дата
                40,  # Адрес
                15,  # ID объекта
                25,  # Подразделение
                25,  # Наименование бригады
                30,  # ФИО
                15,  # Табельный №
                25,  # Должность
                18,  # Тип питания
                12,  # Цена, руб.
                14,  # Сумма, руб.
                40,  # Комментарий
                12,  # Дубликаты
            ]
            for col, width in enumerate(widths, start=1):
                ws.column_dimensions[get_column_letter(col)].width = width

            # заморозим строки до шапки детальной таблицы
            # сейчас: 1 — заголовок свода, 2 — заголовки свода, 3..N — свод, потом пустая строка, потом шапка детально.
            # Чтобы не вычислять динамически, можно заморозить произвольную строку выше детальной шапки.
            # Предположим ограниченно, что свод небольшой — замораживаем всегда строку 5 (будет работать приемлемо).
            ws.freeze_panes = "A5"

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"Реестр_питания_{filter_date or 'все'}_{ts}.xlsx"
            fpath = exe_dir() / ORDERS_DIR / fname
            fpath.parent.mkdir(parents=True, exist_ok=True)

            wb.save(fpath)
            messagebox.showinfo(
                "Экспорт",
                f"Реестр успешно сформирован:\n{fpath}\n\nЗаписей: {len(orders)}",
            )

            try:
                os.startfile(fpath)
            except Exception:
                pass

        except Exception as e:
            messagebox.showerror(
                "Ошибка",
                f"Не удалось сформировать реестр из БД:\n{e}",
            )

# ========================= СТРАНИЦА НАСТРОЕК ТИПОВ ПИТАНИЯ =========================

class MealsSettingsPage(tk.Frame):
    """
    Вкладка "Настройки" для типов питания и цен.
    Доступна только администраторам (роль 'admin' контролирует внешний код).
    """

    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        self.name_vars: List[tk.StringVar] = []
        self.price_vars: List[tk.StringVar] = []
        self._build_ui()
        self.load_meal_types()

    def _build_ui(self):
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(fill="x", padx=10, pady=10)

        tk.Label(
            top,
            text="Настройки типов питания",
            font=("Arial", 12, "bold"),
            bg="#f7f7f7",
        ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 8))

        hdr = tk.Frame(self, bg="#f7f7f7")
        hdr.pack(fill="x", padx=10)
        tk.Label(hdr, text="Название типа питания", bg="#f7f7f7").grid(
            row=0, column=0, sticky="w", padx=4
        )
        tk.Label(hdr, text="Цена, руб.", bg="#f7f7f7").grid(
            row=0, column=1, sticky="w", padx=4
        )

        self.rows_frame = tk.Frame(self, bg="#f7f7f7")
        self.rows_frame.pack(fill="x", padx=10, pady=4)

        for i in range(3):
            nv = tk.StringVar()
            pv = tk.StringVar()
            self.name_vars.append(nv)
            self.price_vars.append(pv)
            ttk.Entry(self.rows_frame, textvariable=nv, width=30).grid(
                row=i, column=0, padx=4, pady=2, sticky="w"
            )
            ttk.Entry(self.rows_frame, textvariable=pv, width=10).grid(
                row=i, column=1, padx=4, pady=2, sticky="w"
            )

        btns = tk.Frame(self, bg="#f7f7f7")
        btns.pack(fill="x", padx=10, pady=(8, 10))
        ttk.Button(
            btns, text="Сохранить типы питания", command=self.save_meal_types
        ).pack(side="left", padx=4)

    def load_meal_types(self):
        try:
            mts = load_meal_types_from_db()
        except Exception as e:
            messagebox.showerror(
                "Настройки питания",
                f"Ошибка загрузки типов питания:\n{e}",
                parent=self,
            )
            return

        for i in range(3):
            if i < len(mts):
                self.name_vars[i].set(mts[i]["name"] or "")
                self.price_vars[i].set(f'{mts[i]["price"]:.2f}')
            else:
                self.name_vars[i].set("")
                self.price_vars[i].set("0.00")

    def save_meal_types(self):
        data: List[Tuple[str, float]] = []
        for nv, pv in zip(self.name_vars, self.price_vars):
            name = (nv.get() or "").strip()
            if not name:
                continue
            p_str = (pv.get() or "0").replace(",", ".")
            try:
                price = float(p_str)
            except Exception:
                messagebox.showwarning(
                    "Настройки питания",
                    f"Цена должна быть числом: '{p_str}'",
                    parent=self,
                )
                return
            data.append((name, price))

        if not data:
            messagebox.showwarning(
                "Настройки питания",
                "Укажите хотя бы один тип питания.",
                parent=self,
            )
            return

        conn = get_db_connection()
        try:
            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # читаем текущие типы
                    cur.execute("SELECT id, name, price FROM meal_types;")
                    existing = list(cur.fetchall())

                    # индекс по имени (в нижнем регистре, чтобы не было проблем с регистром)
                    by_name: Dict[str, Dict[str, Any]] = {
                        (row["name"] or "").strip().lower(): row for row in existing
                    }

                    for name, price in data:
                        key = name.strip().lower()
                        row = by_name.get(key)
                        if row:
                            # обновляем цену у существующей записи
                            cur.execute(
                                "UPDATE meal_types SET price = %s WHERE id = %s",
                                (price, row["id"]),
                            )
                        else:
                            # создаём новую запись
                            cur.execute(
                                "INSERT INTO meal_types (name, price) VALUES (%s, %s)",
                                (name, price),
                            )

            messagebox.showinfo(
                "Настройки питания",
                "Типы питания и цены сохранены.",
                parent=self,
            )
            # перечитаем и отобразим сохранённые значения
            self.load_meal_types()

        except Exception as e:
            messagebox.showerror(
                "Настройки питания",
                f"Ошибка сохранения:\n{e}",
                parent=self,
            )
        finally:
            conn.close()

# ========================= STANDALONE ОКНО =========================

class MealsApp(tk.Tk):
    """Standalone приложение для модуля питания"""

    def __init__(self, current_user_role: str = "user"):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1100x720")
        self.resizable(True, True)

        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True)

        order_page = MealOrderPage(notebook)
        notebook.add(order_page, text="Создать заявку")

        if get_meals_planning_enabled():
            planning_page = MealPlanningPage(notebook)
            notebook.add(planning_page, text="Планирование питания")

        if current_user_role == "admin":
            settings_page = MealsSettingsPage(notebook)
            notebook.add(settings_page, text="Настройки")


# ========================= API ДЛЯ ВСТРАИВАНИЯ =========================

def get_order_with_items_from_db(order_id: int) -> Dict[str, Any]:
    """
    Возвращает заявку с сотрудниками по id:
    {
        'id': ...,
        'created_at': 'YYYY-MM-DDTHH:MM:SS',
        'date': 'YYYY-MM-DD',
        'department': '...',
        'team_name': '...',
        'object': {'id': excel_id, 'address': '...'},
        'employees': [
            {'fio': ..., 'tbn': ..., 'position': ..., 'meal_type': ..., 'comment': ...},
            ...
        ]
    }
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    mo.id,
                    mo.created_at,
                    mo.date::text,
                    COALESCE(d.name, '') AS department,
                    COALESCE(mo.team_name, '') AS team_name,
                    COALESCE(o.excel_id, '') AS object_excel_id,
                    COALESCE(o.address, '') AS object_address
                FROM meal_orders mo
                LEFT JOIN departments d ON d.id = mo.department_id
                LEFT JOIN objects o     ON o.id = mo.object_id
                WHERE mo.id = %s
                """,
                (order_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Заявка id={order_id} не найдена")

            (oid, created_at, date_str,
             department, team_name, obj_excel_id, obj_address) = row

            # сотрудники
            cur.execute(
                """
                SELECT
                    COALESCE(moi.fio_text, '')      AS fio,
                    COALESCE(moi.tbn_text, '')      AS tbn,
                    COALESCE(moi.position_text, '') AS position,
                    COALESCE(mti.name, moi.meal_type_text, '') AS meal_type,
                    COALESCE(moi.comment, '')       AS comment
                FROM meal_order_items moi
                LEFT JOIN meal_types mti ON mti.id = moi.meal_type_id
                WHERE moi.order_id = %s
                ORDER BY moi.fio_text
                """,
                (order_id,),
            )
            emps = []
            for fio, tbn, position, meal_type, comment in cur.fetchall():
                emps.append(
                    {
                        "fio": fio,
                        "tbn": tbn,
                        "position": position,
                        "meal_type": meal_type,
                        "comment": comment,
                    }
                )

        return {
            "id": oid,
            "created_at": created_at.strftime("%Y-%m-%dT%H:%M:%S"),
            "date": date_str,
            "department": department,
            "team_name": team_name,
            "object": {"id": obj_excel_id, "address": obj_address},
            "employees": emps,
        }
    finally:
        conn.close()

def create_meals_order_page(parent) -> tk.Frame:
    ensure_config()
    try:
        return MealOrderPage(parent)
    except Exception:
        import traceback
        messagebox.showerror(
            "Питание — ошибка", traceback.format_exc(), parent=parent
        )
        return tk.Frame(parent)


def create_meals_planning_page(parent) -> tk.Frame:
    ensure_config()
    try:
        return MealPlanningPage(parent)
    except Exception:
        import traceback
        messagebox.showerror(
            "Планирование питания — ошибка", traceback.format_exc(), parent=parent
        )
        return tk.Frame(parent)

def delete_order_items_from_db(order_id: int):
    """Удаляет все строки сотрудников по заявке (оставляя сам заголовок заявки)."""
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM meal_order_items WHERE order_id = %s", (order_id,))
    finally:
        conn.close()

def create_meals_settings_page(parent, current_user_role: str) -> Optional[tk.Frame]:
    """
    Вкладка настроек для главного приложения.
    Если роль не admin — возвращает None.
    """
    if current_user_role != "admin":
        return None
    ensure_config()
    try:
        return MealsSettingsPage(parent)
    except Exception:
        import traceback
        messagebox.showerror(
            "Настройки питания — ошибка", traceback.format_exc(), parent=parent
        )
        return tk.Frame(parent)


def open_meals_module(parent=None, current_user_role: str = "user"):
    if parent is None:
        app = MealsApp(current_user_role=current_user_role)
        app.mainloop()
        return app

    win = tk.Toplevel(parent)
    win.title(APP_TITLE)
    win.geometry("1100x720")

    notebook = ttk.Notebook(win)
    notebook.pack(fill="both", expand=True)

    order_page = MealOrderPage(notebook)
    notebook.add(order_page, text="Создать заявку")

    if get_meals_planning_enabled():
        planning_page = MealPlanningPage(notebook)
        notebook.add(planning_page, text="Планирование питания")

    if current_user_role == "admin":
        settings_page = MealsSettingsPage(notebook)
        notebook.add(settings_page, text="Настройки")

    return win


if __name__ == "__main__":
    ensure_config()
    app = MealsApp(current_user_role="admin")
    app.mainloop()
