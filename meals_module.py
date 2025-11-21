import os
import re
import sys
import csv
import json
import configparser
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

# Конфигурация (часть настроек используется через settings_manager)
CONFIG_FILE = "tabel_config.ini"
CONFIG_SECTION_UI = "UI"
CONFIG_SECTION_INTEGR = "Integrations"

KEY_SELECTED_DEP = "selected_department"
KEY_MEALS_PLANNING_ENABLED = "meals_planning_enabled"

SPRAVOCHNIK_FILE = "Справочник.xlsx"  # больше не используется, но оставим для совместимости
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

# Мягкий импорт менеджера настроек
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
        return Settings.set_selected_department_in_config(dep)

else:
    # Локальный (старый) способ хранения настроек в INI — почти не используется,
    # но оставлен как fallback.
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


def get_or_create_object(cur, ext_id: str, address: str):
    ext_id = (ext_id or "").strip()
    address = (address or "").strip()
    if ext_id:
        cur.execute("SELECT id FROM objects WHERE excel_id = %s OR ext_id = %s", (ext_id, ext_id))
        row = cur.fetchone()
        if row:
            return row[0]
        # пытаемся вставить в новую схему (excel_id, address)
        try:
            cur.execute(
                "INSERT INTO objects (excel_id, address) VALUES (%s, %s) RETURNING id",
                (ext_id, address),
            )
        except Exception:
            cur.execute(
                "INSERT INTO objects (ext_id, address) VALUES (%s, %s) RETURNING id",
                (ext_id, address),
            )
        return cur.fetchone()[0]

    # без ext_id ищем по адресу
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
            "INSERT INTO objects (ext_id, address) VALUES (NULL, %s) RETURNING id",
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
    Возвращает список объектов: [(excel_id_or_ext_id, address), ...]
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # поддерживаем обе схемы: excel_id или ext_id
            cur.execute(
                """
                SELECT
                    COALESCE(NULLIF(excel_id, ''), NULLIF(ext_id, '')) AS code,
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


# ---------------- Сохранение заказов, реестры, проверки ----------------
# (НИЖЕ блок save_order_to_db, get_registry_from_db, get_details_from_db и т.п.
#  ОСТАВЛЯЕМ БЕЗ ИЗМЕНЕНИЙ — они уже работают с БД напрямую)

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
                obj_ext_id = (obj.get("id") or "").strip()
                obj_address = (obj.get("address") or "").strip()
                object_id = get_or_create_object(cur, obj_ext_id, obj_address)

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


# (get_registry_from_db, get_details_from_db, find_conflicting_meal_orders_same_date_other_object
# и т.д. оставляем как в вашем коде — они уже работают по БД; опускаю их здесь,
# чтобы не дублировать полностью. Вы можете оставить их без изменений.)

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
    """Страница для создания заявок на питание"""

    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        ensure_config()
        self.base_dir = exe_dir()
        self.orders_dir = self.base_dir / ORDERS_DIR
        self.orders_dir.mkdir(parents=True, exist_ok=True)

        self._load_refs_from_db()
        self._build_ui()

    def _load_refs_from_db(self):
        # сотрудники
        emps = load_employees_from_db()
        self.emps = emps
        self.emp_by_fio = {r["fio"]: r for r in emps}

        # объекты
        self.objects = load_objects_from_db()
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

        # типы питания
        meal_types = load_meal_types_from_db()
        self.meal_types_full = meal_types
        self.meal_types = [mt["name"] for mt in meal_types] if meal_types else [
            "Одноразовое",
            "Двухразовое",
            "Трехразовое",
        ]

        # список подразделений
        self.deps = ["Все"] + sorted(
            { (r["dep"] or "").strip() for r in self.emps if (r["dep"] or "").strip() }
        )
        self.emp_names_all = [r["fio"] for r in self.emps]

    # дальше — _build_ui, _update_emp_list, _update_date_hint, _sync_ids_by_address,
    # save_order и т.д. — те же, что в вашем коде, только _load_spr заменён на _load_refs_from_db.
    # Из-за ограничений по длине ответа я не повторяю эти методы полностью: вы можете оставить
    # их без изменений, просто заменить вызов self._load_spr() на self._load_refs_from_db().

    # ...

# ========================= СТРАНИЦА НАСТРОЕК ТИПОВ ПИТАНИЯ =========================

class MealsSettingsPage(tk.Frame):
    """
    Вкладка "Настройки" для типов питания и цен.
    Доступна только администраторам (роль 'admin').
    """

    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        self.rows: List[Dict[str, Any]] = []
        self._build_ui()
        self.load_meal_types()

    def _build_ui(self):
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(fill="x", padx=10, pady=10)

        tk.Label(top, text="Настройки типов питания", font=("Arial", 12, "bold"), bg="#f7f7f7").grid(
            row=0, column=0, columnspan=3, sticky="w", pady=(0, 8)
        )

        hdr = tk.Frame(self, bg="#f7f7f7")
        hdr.pack(fill="x", padx=10)
        tk.Label(hdr, text="Название типа питания", bg="#f7f7f7").grid(row=0, column=0, sticky="w", padx=4)
        tk.Label(hdr, text="Цена, руб.", bg="#f7f7f7").grid(row=0, column=1, sticky="w", padx=4)

        self.rows_frame = tk.Frame(self, bg="#f7f7f7")
        self.rows_frame.pack(fill="x", padx=10, pady=4)

        # 3 строки по умолчанию
        self.name_vars: List[tk.StringVar] = []
        self.price_vars: List[tk.StringVar] = []
        for i in range(3):
            nv = tk.StringVar()
            pv = tk.StringVar()
            self.name_vars.append(nv)
            self.price_vars.append(pv)
            ttk.Entry(self.rows_frame, textvariable=nv, width=30).grid(row=i, column=0, padx=4, pady=2, sticky="w")
            ttk.Entry(self.rows_frame, textvariable=pv, width=10).grid(row=i, column=1, padx=4, pady=2, sticky="w")

        btns = tk.Frame(self, bg="#f7f7f7")
        btns.pack(fill="x", padx=10, pady=(8, 10))
        ttk.Button(btns, text="Сохранить типы питания", command=self.save_meal_types).pack(side="left", padx=4)

    def load_meal_types(self):
        try:
            mts = load_meal_types_from_db()
        except Exception as e:
            messagebox.showerror("Настройки питания", f"Ошибка загрузки типов питания:\n{e}", parent=self)
            return

        # Заполняем первые 3 строки из БД, если есть
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
                messagebox.showwarning("Настройки питания", f"Цена должна быть числом: '{p_str}'", parent=self)
                return
            data.append((name, price))

        if not data:
            messagebox.showwarning("Настройки питания", "Укажите хотя бы один тип питания.", parent=self)
            return

        conn = get_db_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    # Полная перезапись meal_types тремя типами
                    cur.execute("DELETE FROM meal_types;")
                    for name, price in data:
                        cur.execute(
                            "INSERT INTO meal_types (name, price) VALUES (%s, %s)",
                            (name, price),
                        )
            messagebox.showinfo("Настройки питания", "Типы питания и цены сохранены.", parent=self)
        except Exception as e:
            messagebox.showerror("Настройки питания", f"Ошибка сохранения:\n{e}", parent=self)
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
            from copy import deepcopy  # при необходимости
            planning_page = MealPlanningPage(notebook)
            notebook.add(planning_page, text="Планирование питания")

        # вкладка Настройки только для администратора
        if current_user_role == "admin":
            settings_page = MealsSettingsPage(notebook)
            notebook.add(settings_page, text="Настройки")


# ========================= API ДЛЯ ВСТРАИВАНИЯ =========================

def create_meals_order_page(parent) -> tk.Frame:
    ensure_config()
    try:
        return MealOrderPage(parent)
    except Exception:
        import traceback
        messagebox.showerror("Питание — ошибка", traceback.format_exc(), parent=parent)
        return tk.Frame(parent)


def create_meals_planning_page(parent) -> tk.Frame:
    ensure_config()
    try:
        return MealPlanningPage(parent)
    except Exception:
        import traceback
        messagebox.showerror("Планирование питания — ошибка", traceback.format_exc(), parent=parent)
        return tk.Frame(parent)


def create_meals_settings_page(parent, current_user_role: str) -> Optional[tk.Frame]:
    """
    Возвращает страницу настроек для встраивания в главное приложение.
    Если роль не admin — возвращает None.
    """
    if current_user_role != "admin":
        return None
    ensure_config()
    try:
        return MealsSettingsPage(parent)
    except Exception:
        import traceback
        messagebox.showerror("Настройки питания — ошибка", traceback.format_exc(), parent=parent)
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
    # Для теста можно указать роль:
    app = MealsApp(current_user_role="admin")
    app.mainloop()
