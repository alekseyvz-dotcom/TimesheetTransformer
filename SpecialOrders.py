# python
import os
import re
import sys
import csv
import json
import calendar
import configparser
import urllib.request
import urllib.error
import urllib.parse
from urllib.parse import urlparse, parse_qs
import psycopg2
from psycopg2.extras import RealDictCursor
from io import BytesIO
from pathlib import Path
from typing import List, Tuple, Optional, Any, Dict

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
from datetime import datetime, date, timedelta

# Мягкий импорт менеджера настроек (зашифрованные настройки)
try:
    import settings_manager as Settings
except Exception:
    Settings = None

APP_TITLE = "Заказ спецтехники"

# Конфиг и файлы (ключи совместимы с main_app/settings_manager)
CONFIG_FILE = "tabel_config.ini"
CONFIG_SECTION_PATHS   = "Paths"
CONFIG_SECTION_UI      = "UI"
CONFIG_SECTION_INTEGR  = "Integrations"
CONFIG_SECTION_ORDERS  = "Orders"
CONFIG_SECTION_REMOTE  = "Remote"   # удалённый справочник (Яндекс Диск — публичная ссылка)

KEY_SPR                 = "spravochnik_path"
KEY_SELECTED_DEP        = "selected_department"

KEY_PLANNING_ENABLED    = "planning_enabled"          # true|false
KEY_PLANNING_PASSWORD   = "planning_password"

# Настройки отсечки подачи заявок
KEY_CUTOFF_ENABLED      = "cutoff_enabled"            # true|false
KEY_CUTOFF_HOUR         = "cutoff_hour"               # 0..23
KEY_DRIVER_DEPARTMENTS  = "driver_departments"

# Удалённый справочник (Я.Диск)
KEY_REMOTE_USE          = "use_remote"                # true|false
KEY_YA_PUBLIC_LINK      = "yadisk_public_link"        # публичная ссылка (public_key)
KEY_YA_PUBLIC_PATH      = "yadisk_public_path"        # если опубликована папка — путь к файлу внутри неё

SPRAVOCHNIK_FILE = "Справочник.xlsx"
ORDERS_DIR = "Заявки_спецтехники"

# Если доступен settings_manager — подменяем конфиг-функции на зашифрованное хранилище
if Settings:
    ensure_config = Settings.ensure_config
    read_config = Settings.read_config
    write_config = Settings.write_config

    # Совместимые обертки под старые имена
    def get_spr_path() -> Path:
        return Settings.get_spr_path_from_config()

    def get_saved_dep() -> str:
        return Settings.get_selected_department_from_config()

    def set_saved_dep(dep: str):
        return Settings.set_selected_department_in_config(dep)


# ------------------------- Утилиты конфигурации -------------------------

# ------------------------- БД: подключение -------------------------

def get_db_connection():
    """
    Подключение к БД по настройкам из settings_manager.
    Ожидается provider=postgres и корректный DATABASE_URL.
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

def get_or_create_object(cur, ext_id: str, address: str) -> Optional[int]:
    """
    Переиспользует таблицу objects (совместно с модулем питания).
    ext_id — ID объекта из справочника (OBJ-001).
    """
    ext_id = (ext_id or "").strip()
    address = (address or "").strip()
    if not (ext_id or address):
        return None

    if ext_id:
        cur.execute("SELECT id FROM objects WHERE ext_id = %s", (ext_id,))
        row = cur.fetchone()
        if row:
            return row[0]
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
    cur.execute(
        "INSERT INTO objects (ext_id, address) VALUES (NULL, %s) RETURNING id",
        (address,),
    )
    return cur.fetchone()[0]

def save_transport_order_to_db(data: dict) -> int:
    """
    Сохраняет заявку на спецтехнику в PostgreSQL.
    data — словарь из SpecialOrdersPage._build_order_dict().
    """
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                obj = data.get("object") or {}
                obj_ext_id = (obj.get("id") or "").strip()
                obj_address = (obj.get("address") or "").strip()
                object_id = get_or_create_object(cur, obj_ext_id, obj_address)

                created_at = datetime.strptime(data["created_at"], "%Y-%m-%dT%H:%M:%S")
                order_date = datetime.strptime(data["date"], "%Y-%m-%d").date()

                cur.execute(
                    """
                    INSERT INTO transport_orders
                        (created_at, date, department, requester_fio, requester_phone,
                         object_id, object_address, comment)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        created_at,
                        order_date,
                        (data.get("department") or "").strip(),
                        (data.get("requester_fio") or "").strip(),
                        (data.get("requester_phone") or "").strip(),
                        object_id,
                        obj_address,
                        (data.get("comment") or "").strip(),
                    ),
                )
                order_id = cur.fetchone()[0]

                for p in data.get("positions", []):
                    tech = (p.get("tech") or "").strip()
                    qty = int(p.get("qty") or 0)
                    time_str = (p.get("time") or "").strip()
                    hours = float(p.get("hours") or 0.0)
                    note = (p.get("note") or "").strip()

                    tval = None
                    if time_str:
                        try:
                            tval = datetime.strptime(time_str, "%H:%M").time()
                        except Exception:
                            tval = None

                    cur.execute(
                        """
                        INSERT INTO transport_order_positions
                            (order_id, tech, qty, time, hours, note, assigned_vehicle, driver, status)
                        VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL, 'Новая')
                        """,
                        (
                            order_id,
                            tech,
                            qty,
                            tval,
                            hours,
                            note,
                        ),
                    )

        return order_id
    finally:
        conn.close()
def get_transport_orders_for_planning(
    filter_date: Optional[str] = None,
    filter_department: Optional[str] = None,
    filter_status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Возвращает список позиций заявок для планирования транспорта.
    Формат элементов совместим с TransportPlanningPage._populate_tree().
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            params = []
            where = []

            if filter_date:
                where.append("o.date = %s")
                params.append(filter_date)

            if filter_department and filter_department.lower() != "все":
                where.append("o.department = %s")
                params.append(filter_department)

            if filter_status and filter_status.lower() != "все":
                where.append("p.status = %s")
                params.append(filter_status)

            where_sql = "WHERE " + " AND ".join(where) if where else ""

            sql = f"""
                SELECT
                    p.id                                   AS id,
                    to_char(o.created_at, 'YYYY-MM-DD"T"HH24:MI:SS') AS created_at,
                    o.date::text                          AS date,
                    COALESCE(o.department,'')             AS department,
                    COALESCE(o.requester_fio,'')          AS requester_fio,
                    COALESCE(o.object_address,'')         AS object_address,
                    COALESCE(obj.ext_id,'')               AS object_id,
                    COALESCE(p.tech,'')                   AS tech,
                    COALESCE(p.qty,0)                     AS qty,
                    COALESCE(to_char(p.time, 'HH24:MI'),'') AS time,
                    COALESCE(p.hours,0)                   AS hours,
                    COALESCE(p.assigned_vehicle,'')       AS assigned_vehicle,
                    COALESCE(p.driver,'')                 AS driver,
                    COALESCE(p.status,'Новая')            AS status,
                    COALESCE(o.comment,'')                AS comment,
                    COALESCE(p.note,'')                   AS position_note
                FROM transport_order_positions p
                JOIN transport_orders o ON o.id = p.order_id
                LEFT JOIN objects obj ON obj.id = o.object_id
                {where_sql}
                ORDER BY o.date, o.created_at, p.id
            """
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()

def exe_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

def config_path() -> Path:
    return exe_dir() / CONFIG_FILE

# Старые ini-функции конфигурации — только если нет settings_manager
if not Settings:
    def get_planning_password() -> str:
        cfg = read_config()
        return cfg.get(CONFIG_SECTION_INTEGR, KEY_PLANNING_PASSWORD, fallback="2025").strip()

    def ensure_config():
        cp = config_path()
        if cp.exists():
            cfg = configparser.ConfigParser()
            cfg.read(cp, encoding="utf-8")
            changed = False

            # --- Paths ---
            if not cfg.has_section(CONFIG_SECTION_PATHS):
                cfg[CONFIG_SECTION_PATHS] = {}
                changed = True
            if KEY_SPR not in cfg[CONFIG_SECTION_PATHS]:
                cfg[CONFIG_SECTION_PATHS][KEY_SPR] = str(exe_dir() / SPRAVOCHNIK_FILE)
                changed = True

            # --- UI ---
            if not cfg.has_section(CONFIG_SECTION_UI):
                cfg[CONFIG_SECTION_UI] = {}
                changed = True
            if KEY_SELECTED_DEP not in cfg[CONFIG_SECTION_UI]:
                cfg[CONFIG_SECTION_UI][KEY_SELECTED_DEP] = "Все"
                changed = True

            # --- Integrations (только то, что реально нужно модулю) ---
            if not cfg.has_section(CONFIG_SECTION_INTEGR):
                cfg[CONFIG_SECTION_INTEGR] = {}
                changed = True
            # planning_enabled
            if KEY_PLANNING_ENABLED not in cfg[CONFIG_SECTION_INTEGR]:
                cfg[CONFIG_SECTION_INTEGR][KEY_PLANNING_ENABLED] = "false"
                changed = True
            # driver_departments
            if KEY_DRIVER_DEPARTMENTS not in cfg[CONFIG_SECTION_INTEGR]:
                cfg[CONFIG_SECTION_INTEGR][KEY_DRIVER_DEPARTMENTS] = "Служба гаража, Автопарк, Транспортный цех"
                changed = True
            # planning_password
            if KEY_PLANNING_PASSWORD not in cfg[CONFIG_SECTION_INTEGR]:
                cfg[CONFIG_SECTION_INTEGR][KEY_PLANNING_PASSWORD] = "2025"
                changed = True

            # --- Orders (отсечка по времени) ---
            if not cfg.has_section(CONFIG_SECTION_ORDERS):
                cfg[CONFIG_SECTION_ORDERS] = {}
                changed = True
            if KEY_CUTOFF_ENABLED not in cfg[CONFIG_SECTION_ORDERS]:
                cfg[CONFIG_SECTION_ORDERS][KEY_CUTOFF_ENABLED] = "true"
                changed = True
            if KEY_CUTOFF_HOUR not in cfg[CONFIG_SECTION_ORDERS]:
                cfg[CONFIG_SECTION_ORDERS][KEY_CUTOFF_HOUR] = "13"
                changed = True

            # --- Remote (Яндекс.Диск для справочника) ---
            if not cfg.has_section(CONFIG_SECTION_REMOTE):
                cfg[CONFIG_SECTION_REMOTE] = {}
                changed = True
            if KEY_REMOTE_USE not in cfg[CONFIG_SECTION_REMOTE]:
                cfg[CONFIG_SECTION_REMOTE][KEY_REMOTE_USE] = "false"
                changed = True
            if KEY_YA_PUBLIC_LINK not in cfg[CONFIG_SECTION_REMOTE]:
                cfg[CONFIG_SECTION_REMOTE][KEY_YA_PUBLIC_LINK] = ""
                changed = True
            if KEY_YA_PUBLIC_PATH not in cfg[CONFIG_SECTION_REMOTE]:
                cfg[CONFIG_SECTION_REMOTE][KEY_YA_PUBLIC_PATH] = ""
                changed = True

            if changed:
                with open(cp, "w", encoding="utf-8") as f:
                    cfg.write(f)
            return

        # создаём ini с нуля (только если нет settings_manager)
        cfg = configparser.ConfigParser()

        cfg[CONFIG_SECTION_PATHS] = {
            KEY_SPR: str(exe_dir() / SPRAVOCHNIK_FILE),
        }

        cfg[CONFIG_SECTION_UI] = {
            KEY_SELECTED_DEP: "Все",
        }

        cfg[CONFIG_SECTION_INTEGR] = {
            KEY_PLANNING_ENABLED: "false",
            KEY_DRIVER_DEPARTMENTS: "Служба гаража, Автопарк, Транспортный цех",
            KEY_PLANNING_PASSWORD: "2025",
        }

        cfg[CONFIG_SECTION_ORDERS] = {
            KEY_CUTOFF_ENABLED: "true",
            KEY_CUTOFF_HOUR: "13",
        }

        cfg[CONFIG_SECTION_REMOTE] = {
            KEY_REMOTE_USE: "false",
            KEY_YA_PUBLIC_LINK: "",
            KEY_YA_PUBLIC_PATH: "",
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

    def get_spr_path() -> Path:
        cfg = read_config()
        raw = cfg.get(CONFIG_SECTION_PATHS, KEY_SPR, fallback=str(exe_dir() / SPRAVOCHNIK_FILE))
        return Path(os.path.expandvars(raw))

    def get_saved_dep() -> str:
        cfg = read_config()
        return cfg.get(CONFIG_SECTION_UI, KEY_SELECTED_DEP, fallback="Все")

    def set_saved_dep(dep: str):
        cfg = read_config()
        if not cfg.has_section(CONFIG_SECTION_UI):
            cfg[CONFIG_SECTION_UI] = {}
        cfg[CONFIG_SECTION_UI][KEY_SELECTED_DEP] = dep or "Все"
        write_config(cfg)

else:
    # Если Settings есть, дополнительные геттеры на его Proxy
    def get_planning_password() -> str:
        cfg = read_config()
        return cfg.get(CONFIG_SECTION_INTEGR, KEY_PLANNING_PASSWORD, fallback="2025").strip()

def get_planning_enabled() -> bool:
    cfg = read_config()
    v = cfg.get(CONFIG_SECTION_INTEGR, KEY_PLANNING_ENABLED, fallback="false").strip().lower()
    return v in ("1", "true", "yes", "on")

# Настройки отсечки приёма заявок
def get_cutoff_enabled() -> bool:
    cfg = read_config()
    v = cfg.get(CONFIG_SECTION_ORDERS, KEY_CUTOFF_ENABLED, fallback="true").strip().lower()
    return v in ("1", "true", "yes", "on")

def get_cutoff_hour() -> int:
    cfg = read_config()
    try:
        h = int(cfg.get(CONFIG_SECTION_ORDERS, KEY_CUTOFF_HOUR, fallback="13").strip())
        return min(23, max(0, h))
    except Exception:
        return 13

def is_past_cutoff_for_date(req_date: date, cutoff_hour: int) -> bool:
    now = datetime.now()
    if req_date != now.date():
        return False
    cutoff = now.replace(hour=cutoff_hour, minute=0, second=0, microsecond=0)
    return now >= cutoff


# ------------------------- Справочник: локально/Я.Диск -------------------------

def ensure_spravochnik(path: Path):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    if path.exists():
        return
    wb = Workbook()
    # Сотрудники
    ws1 = wb.active
    ws1.title = "Сотрудники"
    ws1.append(["ФИО", "Табельный №", "Должность", "Подразделение"])
    ws1.append(["Иванов И. И.", "ST00-00001", "Слесарь", "Монтаж"])
    ws1.append(["Петров П. П.", "ST00-00002", "Электромонтер", "Электрика"])
    ws1.append(["Сидорова А. А.", "ST00-00003", "Инженер", "ИТ"])
    # Объекты
    ws2 = wb.create_sheet("Объекты")
    ws2.append(["ID объекта", "Адрес"])
    ws2.append(["OBJ-001", "ул. Пушкина, д. 1"])
    ws2.append(["OBJ-002", "пр. Строителей, 25"])
    # Техника
    ws3 = wb.create_sheet("Техника")
    ws3.append(["Тип", "Наименование", "Гос№", "Подразделение", "Примечание"])
    ws3.append(["Автокран", "КС-45717", "А123ВС77", "", "25 т."])
    ws3.append(["Манипулятор", "Isuzu Giga", "М456ОР77", "", "Борт 7 т."])
    ws3.append(["Экскаватор", "JCB 3CX", "Е789КУ77", "", ""])
    wb.save(path)

def fetch_yadisk_public_bytes(public_link: str, public_path: str = "") -> bytes:
    if not public_link:
        raise RuntimeError("Не задана публичная ссылка Я.Диска")
    api = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
    params = {"public_key": public_link}
    if public_path:
        params["path"] = public_path
    url = api + "?" + urllib.parse.urlencode(params, safe="/")
    with urllib.request.urlopen(url, timeout=15) as r:
        meta = json.loads(r.read().decode("utf-8", errors="replace"))
    href = meta.get("href")
    if not href:
        raise RuntimeError(f"Я.Диск не вернул href: {meta}")
    with urllib.request.urlopen(href, timeout=60) as f:
        return f.read()

def _s(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    return str(v).strip()

def load_spravochnik_from_wb(wb) -> Tuple[
    List[Tuple[str,str,str,str]],
    List[Tuple[str,str]],
    List[Tuple[str,str,str,str,str]]
]:
    employees: List[Tuple[str,str,str,str]] = []
    objects:   List[Tuple[str,str]] = []
    tech:      List[Tuple[str,str,str,str,str]] = []

    if "Сотрудники" in wb.sheetnames:
        ws = wb["Сотрудники"]
        hdr = [_s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        have_pos = ("должность" in hdr) or (len(hdr) >= 3)
        have_dep = ("подразделение" in hdr) or (len(hdr) >= 4)
        for r in ws.iter_rows(min_row=2, values_only=True):
            fio = _s(r[0] if r and len(r)>0 else "")
            tbn = _s(r[1] if r and len(r)>1 else "")
            pos = _s(r[2] if have_pos and r and len(r)>2 else "")
            dep = _s(r[3] if have_dep and r and len(r)>3 else "")
            if fio:
                employees.append((fio, tbn, pos, dep))

    if "Объекты" in wb.sheetnames:
        ws = wb["Объекты"]
        hdr = [_s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        have_two = ("id объекта" in hdr) or (len(hdr) >= 2)
        for r in ws.iter_rows(min_row=2, values_only=True):
            if have_two:
                oid = _s(r[0] if r and len(r)>0 else "")
                addr = _s(r[1] if r and len(r)>1 else "")
            else:
                oid = ""
                addr = _s(r[0] if r and len(r)>0 else "")
            if oid or addr:
                objects.append((oid, addr))

    if "Техника" in wb.sheetnames:
        ws = wb["Техника"]
        hdr = [_s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        for r in ws.iter_rows(min_row=2, values_only=True):
            tp  = _s(r[0] if r and len(r)>0 else "")
            nm  = _s(r[1] if r and len(r)>1 else "")
            pl  = _s(r[2] if r and len(r)>2 else "")
            dep = _s(r[3] if r and len(r)>3 else "")
            note= _s(r[4] if r and len(r)>4 else "")
            if tp or nm or pl:
                tech.append((tp, nm, pl, dep, note))

    return employees, objects, tech

def load_spravochnik_remote_or_local(local_path: Path):
    cfg = read_config()
    use_remote = cfg.get(CONFIG_SECTION_REMOTE, KEY_REMOTE_USE, fallback="false").strip().lower() in ("1","true","yes","on")
    if use_remote:
        try:
            public_link = cfg.get(CONFIG_SECTION_REMOTE, KEY_YA_PUBLIC_LINK, fallback="").strip()
            public_path = cfg.get(CONFIG_SECTION_REMOTE, KEY_YA_PUBLIC_PATH, fallback="").strip()
            raw = fetch_yadisk_public_bytes(public_link, public_path)
            wb = load_workbook(BytesIO(raw), read_only=True, data_only=True)
            return load_spravochnik_from_wb(wb)
        except Exception as e:
            print(f"[Remote YaDisk] ошибка: {e} — локальный справочник используем только если существует")
            if local_path.exists():
                wb = load_workbook(local_path, read_only=True, data_only=True)
                return load_spravochnik_from_wb(wb)
            # НЕ создаём локальный файл при удаленном режиме — пустые данные
            return [], [], []

    # Локальный режим — допускаем автосоздание
    ensure_spravochnik(local_path)
    wb = load_workbook(local_path, read_only=True, data_only=True)
    return load_spravochnik_from_wb(wb)


# ------------------------- Парсинг значений -------------------------

def parse_hours_value(v: Any) -> Optional[float]:
    s = str(v or "").strip()
    if not s:
        return None
    if "/" in s:
        total = 0.0
        any_part = False
        for part in s.split("/"):
            n = parse_hours_value(part)
            if isinstance(n, (int, float)):
                total += float(n); any_part = True
        return total if any_part else None
    if ":" in s:
        p = s.split(":")
        try:
            hh = float(p[0].replace(",", "."))
            mm = float((p[1] if len(p)>1 else "0").replace(",", "."))
            ss = float((p[2] if len(p)>2 else "0").replace(",", "."))
            return hh + mm/60.0 + ss/3600.0
        except:
            pass
    s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return None

def parse_time_str(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return f"{hh:02d}:{mm:02d}"

def parse_date_any(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    return None


# ------------------------- Виджеты -------------------------

class AutoCompleteCombobox(ttk.Combobox):
    def __init__(self, master=None, **kw):
        super().__init__(master, **kw)
        self._all_values: List[str] = []
        self.bind("<KeyRelease>", self._on_keyrelease)
        self.bind("<Control-BackSpace>", self._clear_all)

    def set_completion_list(self, values: List[str]):
        self._all_values = list(values)
        self['values'] = self._all_values

    def _clear_all(self, _=None):
        self.delete(0, tk.END)
        self['values'] = self._all_values

    def _on_keyrelease(self, event):
        if event.keysym in ("Up", "Down", "Left", "Right", "Home", "End", "Return", "Escape", "Tab"):
            return
        typed = self.get().strip()
        if not typed:
            self['values'] = self._all_values
            return
        self['values'] = [x for x in self._all_values if typed.lower() in x.lower()]


# ------------------------- Строка позиции -------------------------

class PositionRow:
    ERR_BG = "#ffccbc"
    ZEBRA_EVEN = "#ffffff"
    ZEBRA_ODD  = "#f6f8fa"

    def __init__(self, parent, idx: int, tech_values: List[str], on_delete):
        self.parent = parent
        self.idx = idx
        self.on_delete = on_delete
        self.tech_values = tech_values

        self.frame = tk.Frame(parent)

        self.cmb_tech = ttk.Combobox(self.frame, values=tech_values, width=46)
        self.cmb_tech.grid(row=0, column=0, padx=2, pady=1, sticky="w")

        self.ent_qty = ttk.Entry(self.frame, width=6, justify="center")
        self.ent_qty.grid(row=0, column=1, padx=2)
        self.ent_qty.insert(0, "1")

        # ===== ИЗМЕНЕНИЯ ДЛЯ АВТОФОРМАТИРОВАНИЯ ВРЕМЕНИ =====
        self.time_var = tk.StringVar()
        self.time_var.trace_add("write", self._on_time_changed)
        self._formatting_time = False
        self._format_timer = None  # Таймер для отложенного форматирования
        
        self.ent_time = ttk.Entry(self.frame, width=8, justify="center", textvariable=self.time_var)
        self.ent_time.grid(row=0, column=2, padx=2)
        
        # Форматирование при потере фокуса (мгновенно)
        self.ent_time.bind("<FocusOut>", self._format_immediately)
        self.ent_time.bind("<Return>", self._format_immediately)
        # ====================================================

        self.ent_hours = ttk.Entry(self.frame, width=8, justify="center")
        self.ent_hours.grid(row=0, column=3, padx=2)
        self.ent_hours.insert(0, "4")

        self.ent_note = ttk.Entry(self.frame, width=34)
        self.ent_note.grid(row=0, column=4, padx=2, sticky="w")

        self.btn_del = ttk.Button(self.frame, text="Удалить", width=9, command=self._delete)
        self.btn_del.grid(row=0, column=5, padx=2)

        for i in range(6):
            self.frame.grid_columnconfigure(i, minsize=[380, 50, 70, 70, 280, 80][i])

    # ===== НОВЫЕ МЕТОДЫ ДЛЯ АВТОФОРМАТИРОВАНИЯ =====
    def _on_time_changed(self, *args):
        """Вызывается при каждом изменении - запускает отложенное форматирование"""
        if self._formatting_time:
            return
        if self._format_timer:
            self.ent_time.after_cancel(self._format_timer)
        self._format_timer = self.ent_time.after(500, self._do_format)
    
    def _format_immediately(self, event=None):
        """Форматирует немедленно (при FocusOut или Enter)"""
        if self._format_timer:
            self.ent_time.after_cancel(self._format_timer)
            self._format_timer = None
        self._do_format()
        return None
    
    def _do_format(self):
        """Выполняет форматирование"""
        if self._formatting_time:
            return
        current = self.time_var.get()
        formatted = self._auto_format_time_input(current)
        if formatted != current:
            self._formatting_time = True
            try:
                self.time_var.set(formatted)
                self.ent_time.icursor(tk.END)
            finally:
                self._formatting_time = False
    
    def _auto_format_time_input(self, raw: str) -> str:
        """
        Автоматически форматирует ввод времени в формат ЧЧ:ММ
        Примеры:
        - '8' → '08:00'
        - '13' → '13:00'
        - '130' → '01:30'
        - '1300' → '13:00'
        - '13.00' → '13:00'
        - '9.45' → '09:45'
        """
        if not raw:
            return ""
        digits = ''.join(c for c in raw if c.isdigit())
        if not digits:
            return ""
        if len(digits) == 1:
            hh = int(digits)
            return f"{hh:02d}:00"
        elif len(digits) == 2:
            hh = min(int(digits), 23)
            return f"{hh:02d}:00"
        elif len(digits) == 3:
            hh = int(digits[0])
            mm = min(int(digits[1:3]), 59)
            return f"{hh:02d}:{mm:02d}"
        else:
            hh = min(int(digits[:2]), 23)
            mm = min(int(digits[2:4]), 59)
            return f"{hh:02d}:{mm:02d}"
    # ===============================================

    def grid(self, row: int):
        self.frame.grid(row=row, column=0, sticky="w")

    def destroy(self):
        self.frame.destroy()

    def apply_zebra(self, row0: int):
        bg = self.ZEBRA_ODD if (row0 % 2 == 1) else self.ZEBRA_EVEN
        for w in (self.cmb_tech, self.ent_qty, self.ent_time, self.ent_hours, self.ent_note):
            try:
                w.configure(background=bg)
            except Exception:
                pass

    def _delete(self):
        self.on_delete(self)

    def validate(self) -> bool:
        ok = True
        val = (self.cmb_tech.get() or "").strip()
        if not val:
            self._mark_err(self.cmb_tech); ok = False
        else:
            self._clear_err(self.cmb_tech)

        try:
            qty = int((self.ent_qty.get() or "0").strip())
            if qty <= 0:
                raise ValueError
            self._clear_err(self.ent_qty)
        except Exception:
            self._mark_err(self.ent_qty); ok = False

        # время ПОДАЧИ — обязательно
        tstr = (self.ent_time.get() or "").strip()
        if not tstr or parse_time_str(tstr) is None:
            self._mark_err(self.ent_time); ok = False
        else:
            self._clear_err(self.ent_time)

        hv = parse_hours_value(self.ent_hours.get())
        if hv is None or hv <= 0:
            self._mark_err(self.ent_hours); ok = False
        else:
            self._clear_err(self.ent_hours)
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
            "tech": (self.cmb_tech.get() or "").strip(),
            "qty": int((self.ent_qty.get() or "0").strip() or 0),
            "time": (parse_time_str(self.ent_time.get()) or ""),
            "hours": float(parse_hours_value(self.ent_hours.get()) or 0.0),
            "note": (self.ent_note.get() or "").strip(),
        }
        
# ------------------------- Встраиваемая страница -------------------------

class SpecialOrdersPage(tk.Frame):
    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        ensure_config()  # из settings_manager, если доступен
        self.base_dir = exe_dir()
        self.spr_path = get_spr_path()
        self.orders_dir = self.base_dir / ORDERS_DIR
        self.orders_dir.mkdir(parents=True, exist_ok=True)

        self._load_spr()
        self._build_ui()

    # Ниже — те же методы, что использует standalone-окно, но работают в рамках Frame
    def _load_spr(self):
        employees, objects, tech = load_spravochnik_remote_or_local(self.spr_path)
        self.emps = [{'fio': fio, 'tbn': tbn, 'pos': pos, 'dep': dep} for (fio, tbn, pos, dep) in employees]
        self.objects = objects

        # ========== ТЕХНИКА: ТОЛЬКО УНИКАЛЬНЫЕ ТИПЫ ДЛЯ ЗАЯВКИ ==========
        self.techs = []
        tech_types = set()
    
        for tp, nm, pl, dep, note in tech:
            if tp:
                tech_types.add(tp)
            self.techs.append({'type': tp, 'name': nm, 'plate': pl, 'dep': dep, 'note': note})
    
        self.tech_values = sorted(list(tech_types))
        # ================================================================

        self.deps = ["Все"] + sorted({(r['dep'] or "").strip() for r in self.emps if (r['dep'] or "").strip()})
        self.emp_names_all = [r['fio'] for r in self.emps]

        self.addr_to_ids = {}
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

        tk.Label(top, text="Подразделение*:", bg="#f7f7f7").grid(row=0, column=0, sticky="w")
        self.cmb_dep = ttk.Combobox(top, state="readonly", values=self.deps, width=48)
        saved_dep = get_saved_dep()
        self.cmb_dep.set(saved_dep if saved_dep in self.deps else self.deps[0])
        self.cmb_dep.grid(row=0, column=1, sticky="w", padx=(4, 12))
        self.cmb_dep.bind("<<ComboboxSelected>>",
                          lambda e: (set_saved_dep(self.cmb_dep.get()), self._update_fio_list()))

        tk.Label(top, text="ФИО*:", bg="#f7f7f7").grid(row=0, column=2, sticky="w")
        self.fio_var = tk.StringVar()
        self.cmb_fio = AutoCompleteCombobox(top, textvariable=self.fio_var, width=36)
        self.cmb_fio.grid(row=0, column=3, sticky="w", padx=(4, 12))

        tk.Label(top, text="Телефон*:", bg="#f7f7f7").grid(row=0, column=4, sticky="w")
        self.ent_phone = ttk.Entry(top, width=18)
        self.ent_phone.grid(row=0, column=5, sticky="w", padx=(4, 12))

        tk.Label(top, text="Дата*:", bg="#f7f7f7").grid(row=0, column=6, sticky="w")
        self.ent_date = ttk.Entry(top, width=12)
        self.ent_date.grid(row=0, column=7, sticky="w", padx=(4, 0))
        # по умолчанию — завтра
        self.ent_date.delete(0, "end")
        self.ent_date.insert(0, (date.today() + timedelta(days=1)).strftime("%Y-%m-%d"))
        self.ent_date.bind("<KeyRelease>", lambda e: self._update_tomorrow_hint())
        self.ent_date.bind("<FocusOut>", lambda e: self._update_tomorrow_hint())

        tk.Label(top, text="Адрес*:", bg="#f7f7f7").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.cmb_address = AutoCompleteCombobox(top, width=56)
        self.cmb_address.set_completion_list(self.addresses)
        self.cmb_address.grid(row=1, column=1, columnspan=3, sticky="w", padx=(4, 12), pady=(8, 0))
        self.cmb_address.bind("<<ComboboxSelected>>", lambda e: self._sync_ids_by_address())
        self.cmb_address.bind("<FocusOut>", lambda e: self._sync_ids_by_address())
        self.cmb_address.bind("<Return>", lambda e: self._sync_ids_by_address())

        tk.Label(top, text="ID объекта:", bg="#f7f7f7").grid(row=1, column=4, sticky="w", pady=(8, 0))
        self.cmb_object_id = ttk.Combobox(top, state="readonly", values=[], width=20)
        self.cmb_object_id.grid(row=1, column=5, sticky="w", padx=(4, 12), pady=(8, 0))

        # новая подсказка по дате (вместо отсечки)
        self.lbl_date_hint = tk.Label(top, text="", fg="#555", bg="#f7f7f7")
        self.lbl_date_hint.grid(row=1, column=6, columnspan=2, sticky="w", pady=(8, 0))

        tk.Label(top, text="Комментарий*:", bg="#f7f7f7").grid(row=2, column=0, sticky="nw", pady=(8, 0))
        self.txt_comment = tk.Text(top, height=3, width=96)
        self.txt_comment.grid(row=2, column=1, columnspan=7, sticky="we", padx=(4, 0), pady=(8, 0))

        pos_wrap = tk.LabelFrame(self, text="Позиции")
        pos_wrap.pack(fill="both", expand=True, padx=10, pady=(6, 8))

        hdr = tk.Frame(pos_wrap)
        hdr.pack(fill="x")
        tk.Label(hdr, text="Техника*", width=52, anchor="w").grid(row=0, column=0, padx=2)
        tk.Label(hdr, text="Кол-во*", width=6, anchor="center").grid(row=0, column=1, padx=2)
        tk.Label(hdr, text="Подача (чч:мм)*", width=12, anchor="center").grid(row=0, column=2, padx=2)
        tk.Label(hdr, text="Часы*", width=10, anchor="center").grid(row=0, column=3, padx=2)
        tk.Label(hdr, text="Примечание", width=38, anchor="w").grid(row=0, column=4, padx=2)
        tk.Label(hdr, text="Действие", width=10, anchor="center").grid(row=0, column=5, padx=2)

        wrap = tk.Frame(pos_wrap)
        wrap.pack(fill="both", expand=True)
        self.cv = tk.Canvas(wrap, borderwidth=0, highlightthickness=0)
        self.rows_holder = tk.Frame(self.cv)
        self.cv.create_window((0, 0), window=self.rows_holder, anchor="nw")
        self.cv.pack(side="left", fill="both", expand=True)
        self.vscroll = ttk.Scrollbar(wrap, orient="vertical", command=self.cv.yview)
        self.vscroll.pack(side="right", fill="y")
        self.cv.configure(yscrollcommand=self.vscroll.set)
        self.rows_holder.bind("<Configure>", lambda e: self.cv.configure(scrollregion=self.cv.bbox("all")))
        self.cv.bind("<MouseWheel>", lambda e: (self.cv.yview_scroll(int(-1*(e.delta/120)), "units"), "break"))

        self.pos_rows: List[PositionRow] = []
        btns = tk.Frame(pos_wrap)
        btns.pack(fill="x")
        ttk.Button(btns, text="Добавить позицию", command=self.add_position).pack(side="left", padx=2, pady=4)

        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(bottom, text="Сохранить заявку", command=self.save_order).pack(side="left", padx=4)
        ttk.Button(bottom, text="Очистить форму", command=self.clear_form).pack(side="left", padx=4)
        ttk.Button(bottom, text="Открыть папку заявок", command=self.open_orders_dir).pack(side="left", padx=4)

        self._update_fio_list()
        self._update_tomorrow_hint()
        self.add_position()

        for c in range(8):
            top.grid_columnconfigure(c, weight=0)
        top.grid_columnconfigure(1, weight=1)
        top.grid_columnconfigure(5, weight=0)

    # Методы логики/валидации/сохранения — те же, что и в standalone
    def _update_fio_list(self):
        dep = (self.cmb_dep.get() or "Все").strip()
        if dep == "Все":
            names = [r['fio'] for r in self.emps]
        else:
            names = [r['fio'] for r in self.emps if (r['dep'] or "") == dep]
        seen, filtered = set(), []
        for n in names:
            if n not in seen:
                seen.add(n)
                filtered.append(n)
        if not filtered and dep != "Все":
            filtered = [r['fio'] for r in self.emps]
        self.cmb_fio.set_completion_list(filtered)

    def _update_tomorrow_hint(self):
        """Подсказка: заявки принимаются только на завтрашнюю дату"""
        try:
            req = parse_date_any(self.ent_date.get())
            tomorrow = date.today() + timedelta(days=1)
            if req is None:
                self.lbl_date_hint.config(text="Укажите дату в формате YYYY-MM-DD или DD.MM.YYYY", fg="#b00020")
            elif req != tomorrow:
                self.lbl_date_hint.config(text=f"Заявка возможна только на {tomorrow.strftime('%Y-%m-%d')}", fg="#b00020")
            else:
                self.lbl_date_hint.config(text="Ок: заявка на завтрашнюю дату", fg="#2e7d32")
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

    def add_position(self):
        row = PositionRow(self.rows_holder, len(self.pos_rows) + 1, self.tech_values, self.delete_position)
        row.grid(len(self.pos_rows))
        row.apply_zebra(len(self.pos_rows))
        self.pos_rows.append(row)

    def delete_position(self, prow: PositionRow):
        try:
            self.pos_rows.remove(prow)
        except Exception:
            pass
        prow.destroy()
        for i, r in enumerate(self.pos_rows, start=0):
            r.grid(i)
            r.apply_zebra(i)

    def _validate_form(self) -> bool:
        # Подразделение
        if not (self.cmb_dep.get() or "").strip():
            messagebox.showwarning("Заявка", "Выберите Подразделение.")
            return False
        # ФИО
        if not (self.cmb_fio.get() or "").strip():
            messagebox.showwarning("Заявка", "Укажите ФИО.")
            return False
        # Телефон (хотя бы 5 цифр)
        phone = (self.ent_phone.get() or "").strip()
        digits = re.sub(r"\D+", "", phone)
        if not phone or len(digits) < 5:
            messagebox.showwarning("Заявка", "Укажите номер телефона (минимум 5 цифр).")
            return False
        # Дата — строго завтра
        req = parse_date_any(self.ent_date.get())
        tomorrow = date.today() + timedelta(days=1)
        if req is None or req != tomorrow:
            messagebox.showwarning("Заявка", f"Заявка возможна только на дату: {tomorrow.strftime('%Y-%m-%d')}.")
            return False
        # Адрес (обязателен)
        addr = (self.cmb_address.get() or "").strip()
        if not addr:
            messagebox.showwarning("Заявка", "Укажите Адрес.")
            return False
        # Комментарий
        comment = self.txt_comment.get("1.0", "end").strip()
        if not comment:
            messagebox.showwarning("Заявка", "Добавьте комментарий к заявке.")
            return False
        # Позиции
        if not self.pos_rows:
            messagebox.showwarning("Заявка", "Добавьте хотя бы одну позицию.")
            return False
        all_ok = True
        for r in self.pos_rows:
            all_ok = r.validate() and all_ok
        if not all_ok:
            messagebox.showwarning("Заявка", "Исправьте подсвеченные поля в позициях (Техника, Кол-во, Подача, Часы).")
            return False
        return True

    def _build_order_dict(self) -> Dict:
        created_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        req_date = parse_date_any(self.ent_date.get()) or date.today()
        addr = (self.cmb_address.get() or "").strip()
        oid = (self.cmb_object_id.get() or "").strip()
        comment = self.txt_comment.get("1.0", "end").strip()
        positions = [r.get_dict() for r in self.pos_rows]
        return {
            "created_at": created_at,
            "date": req_date.strftime("%Y-%m-%d"),
            "department": (self.cmb_dep.get() or "").strip(),
            "requester_fio": (self.cmb_fio.get() or "").strip(),
            "requester_phone": (self.ent_phone.get() or "").strip(),
            "object": {"id": oid, "address": addr},
            "comment": comment,
            "positions": positions,
        }

    def save_order(self):
        if not self._validate_form():
            return
            
        req_date = parse_date_any(self.ent_date.get()) or (date.today() + timedelta(days=1))
        tomorrow = date.today() + timedelta(days=1)
        if req_date != tomorrow:
            messagebox.showwarning("Заявка", f"Заявка возможна только на {tomorrow.strftime('%Y-%m-%d')}.")
            return
        
        data = self._build_order_dict()

        # 1. Сохранение в БД
        order_db_id = None
        db_error = None
        try:
            if Settings:
                order_db_id = save_transport_order_to_db(data)
        except Exception as e:
            db_error = e

        # 2. XLSX
        ts = datetime.now().strftime("%H%M%S")
        id_part = data["object"]["id"] or safe_filename(data["object"]["address"])
        fname = f"Заявка_спецтехники_{data['date']}_{ts}_{id_part or 'NOID'}.xlsx"
        fpath = self.orders_dir / fname

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Заявка"
            ws.append(["Создано", data["created_at"]])
            ws.append(["Дата", data["date"]])
            ws.append(["Подразделение", data["department"]])
            ws.append(["Заявитель (ФИО)", data["requester_fio"]])
            ws.append(["Телефон", data["requester_phone"]])
            ws.append(["ID объекта", data["object"]["id"]])
            ws.append(["Адрес", data["object"]["address"]])
            ws.append(["Комментарий", data["comment"]])
            if order_db_id is not None:
                ws.append(["ID заявки в БД", order_db_id])
            ws.append([])
            hdr = ["#", "Техника", "Кол-во", "Подача (чч:мм)", "Часы", "Примечание"]
            ws.append(hdr)
            for i, p in enumerate(data["positions"], start=1):
                ws.append([i, p["tech"], p["qty"], (p["time"] or None), p["hours"], p["note"]])
            for col, w in enumerate([4, 48, 8, 14, 10, 36], start=1):
                ws.column_dimensions[get_column_letter(col)].width = w
            ws.freeze_panes = "A12"
            wb.save(fpath)
        except Exception as e:
            messagebox.showerror("Сохранение", f"Не удалось сохранить XLSX:\n{e}")
            return

        # 3. CSV (архив)
        csv_path = self.orders_dir / f"Свод_заявок_{data['date'][:7].replace('-', '_')}.csv"
        try:
            new = not csv_path.exists()
            with open(csv_path, "a", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f, delimiter=";")
                if new:
                    w.writerow([
                        "Создано","Дата","Подразделение","ФИО","Телефон","ID объекта","Адрес",
                        "Техника","Кол-во","Подача","Часы","Примечание","Комментарий заявки"
                    ])
                for p in data["positions"]:
                    w.writerow([
                        data["created_at"], data["date"], data["department"], data["requester_fio"],
                        data["requester_phone"], data["object"]["id"], data["object"]["address"],
                        p["tech"], p["qty"], p["time"], p["hours"], p["note"], data["comment"]
                    ])
        except Exception as e:
            messagebox.showwarning("Сводный CSV", f"XLSX сохранён, но не удалось добавить в CSV:\n{e}")

        # 4. Итоговое сообщение
        extra = ""
        if db_error:
            extra = f"\n\nВНИМАНИЕ: не удалось сохранить в БД:\n{db_error}"
        messagebox.showinfo(
            "Сохранение",
            f"Заявка сохранена.\n"
            f"ID в БД: {order_db_id if order_db_id is not None else '—'}\n\n"
            f"XLSX:\n{fpath}\nCSV:\n{csv_path}{extra}"
        )

    def clear_form(self):
        self.fio_var.set("")
        self.ent_phone.delete(0, "end")
        self.ent_date.delete(0, "end")
        self.ent_date.insert(0, (date.today() + timedelta(days=1)).strftime("%Y-%m-%d"))
        self.cmb_address.set("")
        self.cmb_object_id.config(values=[])
        self.cmb_object_id.set("")
        self.txt_comment.delete("1.0", "end")
        for r in self.pos_rows:
            r.destroy()
        self.pos_rows.clear()
        self.add_position()
        self._update_tomorrow_hint()

    def open_orders_dir(self):
        try:
            os.startfile(self.orders_dir)
        except Exception as e:
            messagebox.showerror("Папка", f"Не удалось открыть папку:\n{e}")

# ------------------------- Планирование транспорта -------------------------

class TransportPlanningPage(tk.Frame):
    """Вкладка планирования транспорта"""
    
    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        self.spr_path = get_spr_path()
        self.authenticated = False
        self.row_meta: Dict[str, Dict[str, str]] = {} 

        # ПРОВЕРКА ПАРОЛЯ
        if not self._check_password():
            self._show_access_denied()
            return

        self.authenticated = True
        self._load_spr()
        self._build_ui()

    def _check_password(self) -> bool:
        """Проверка пароля (аналогично summary_export)"""
        required_password = get_planning_password()
        
        # Если пароль пустой - доступ без авторизации
        if not required_password:
            return True
        
        # Запрос пароля через стандартный диалог
        pwd = simpledialog.askstring(
            "Планирование транспорта", 
            "Введите пароль для доступа:", 
            show="*", 
            parent=self
        )
        
        if pwd is None:
            return False
        
        if pwd != required_password:
            messagebox.showerror("Доступ запрещён", "Неверный пароль.", parent=self)
            return False
        
        return True

    def _show_access_denied(self):
        """Экран отказа в доступе"""
        container = tk.Frame(self, bg="#f7f7f7")
        container.place(relx=0.5, rely=0.5, anchor="center")
        
        tk.Label(
            container,
            text="Доступ запрещён",
            font=("Segoe UI", 18, "bold"),
            bg="#f7f7f7",
            fg="#666"
        ).pack(pady=(0, 10))
        
        tk.Label(
            container,
            text="Для просмотра этого раздела требуется пароль",
            font=("Segoe UI", 10),
            bg="#f7f7f7",
            fg="#888"
        ).pack()
        
    def _load_spr(self):
        """Загрузка справочника"""
        employees, objects, tech = load_spravochnik_remote_or_local(self.spr_path)
    
        # ========== ТРАНСПОРТ: полная структура ==========
        self.vehicles = []
        self.vehicle_types = set()
    
        for tp, nm, pl, dep, note in tech:
            self.vehicles.append({'type': tp, 'name': nm, 'plate': pl, 'dep': dep, 'note': note})
            if tp:
                self.vehicle_types.add(tp)
    
        self.vehicle_types = sorted(list(self.vehicle_types))
        # ======================================================================
    
        # Водители
        cfg = read_config()
        driver_depts_str = cfg.get(CONFIG_SECTION_INTEGR, KEY_DRIVER_DEPARTMENTS, fallback="Служба гаража")
        DRIVER_DEPARTMENTS = [d.strip() for d in driver_depts_str.split(",") if d.strip()]
    
        self.drivers = []
        for fio, tbn, pos, dep in employees:
            is_driver_dept = dep in DRIVER_DEPARTMENTS
            if is_driver_dept:  # ✅ Только из настроенных подразделений
                self.drivers.append({'fio': fio, 'tbn': tbn, 'pos': pos, 'dep': dep})
    
        self.drivers.sort(key=lambda x: x['fio'])
        self.departments = ["Все"] + sorted({dep for _, _, _, dep in employees if dep})
        
    def _build_ui(self):
        """Построение интерфейса"""
        # Верхняя панель с фильтрами
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(fill="x", padx=10, pady=8)
        
        tk.Label(top, text="Дата:", bg="#f7f7f7").grid(row=0, column=0, sticky="w")
        self.ent_filter_date = ttk.Entry(top, width=12)
        self.ent_filter_date.grid(row=0, column=1, padx=4)
        self.ent_filter_date.insert(0, date.today().strftime("%Y-%m-%d"))
        
        tk.Label(top, text="Подразделение:", bg="#f7f7f7").grid(row=0, column=2, sticky="w", padx=(12,0))
        self.cmb_filter_dep = ttk.Combobox(top, state="readonly", values=self.departments, width=20)
        self.cmb_filter_dep.set("Все")
        self.cmb_filter_dep.grid(row=0, column=3, padx=4)
        
        tk.Label(top, text="Статус:", bg="#f7f7f7").grid(row=0, column=4, sticky="w", padx=(12,0))
        self.cmb_filter_status = ttk.Combobox(
            top, state="readonly", 
            values=["Все", "Новая", "Назначена", "В работе", "Выполнена"], 
            width=15
        )
        self.cmb_filter_status.set("Все")
        self.cmb_filter_status.grid(row=0, column=5, padx=4)
        
        ttk.Button(top, text="🔄 Обновить", command=self.load_orders).grid(row=0, column=6, padx=12)
        ttk.Button(top, text="💾 Сохранить назначения", command=self.save_assignments).grid(row=0, column=7, padx=4)
        
        # Таблица заявок
        table_frame = tk.Frame(self)
        table_frame.pack(fill="both", expand=True, padx=10, pady=8)
        
        columns = (
            "id", "created", "date", "dept", "requester", 
            "object", "tech", "qty", "time", "hours", 
            "assigned_vehicle", "driver", "status"
        )
        
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=20)
        
        headers = {
            "id": "ID", "created": "Создано", "date": "Дата", 
            "dept": "Подразделение", "requester": "Заявитель",
            "object": "Объект/Адрес", "tech": "Техника", "qty": "Кол-во",
            "time": "Подача", "hours": "Часы", 
            "assigned_vehicle": "Назначен авто", "driver": "Водитель", 
            "status": "Статус"
        }
        
        widths = {
            "id": 80, "created": 130, "date": 90, "dept": 120, 
            "requester": 150, "object": 200, "tech": 180, 
            "qty": 50, "time": 60, "hours": 50, 
            "assigned_vehicle": 180, "driver": 150, "status": 100
        }
        
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
        
        self.tree.tag_configure('Новая', background='#fff3cd')
        self.tree.tag_configure('Назначена', background='#d1ecf1')
        self.tree.tag_configure('В работе', background='#d4edda')
        self.tree.tag_configure('Выполнена', background='#e2e3e5')
        
    def load_orders(self):
        """Загрузка заявок из PostgreSQL"""
        try:
            filter_date = self.ent_filter_date.get().strip()
            filter_dept = self.cmb_filter_dep.get().strip()
            filter_status = self.cmb_filter_status.get().strip()

            orders = get_transport_orders_for_planning(
                filter_date=filter_date or None,
                filter_department=filter_dept or None,
                filter_status=filter_status or None,
            )
            self._populate_tree(orders)
            messagebox.showinfo("Загрузка", f"Загружено заявок: {len(orders)}")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить заявки из БД:\n{e}")

    def _check_vehicle_conflict(self, vehicle_full: str, req_date: str, req_time: str, current_id: str) -> List[Dict]:
        """
        Проверяет, не назначен ли этот автомобиль на другую заявку в это же время
        vehicle_full: "Автокран | КС-45717 | А123ВС77"
        """
        if not vehicle_full or not req_date:
            return []
    
        conflicts = []
    
        for item_id in self.tree.get_children():
            values = self.tree.item(item_id)['values']
            if values[0] == current_id:
                continue
            other_date = values[2]
            other_vehicle = values[10]
            other_time = values[8]
            other_requester = values[4]
            other_object = values[5]
            other_status = values[12]
        
            if (other_vehicle == vehicle_full and 
                other_date == req_date and
                other_status not in ['Выполнена', 'Отменена']):
                if not req_time or not other_time:
                    conflicts.append({'time': other_time or 'не указано',
                                      'requester': other_requester,
                                      'object': other_object,
                                      'status': other_status})
                elif req_time == other_time:
                    conflicts.append({'time': other_time,
                                      'requester': other_requester,
                                      'object': other_object,
                                      'status': other_status})
    
        return conflicts
    
    def _populate_tree(self, orders: List[Dict]):
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.row_meta = {}

        for order in orders:
            obj_display = order.get('object_address', '') or order.get('object_id', '')
            status = order.get('status', 'Новая')

            item_id = self.tree.insert("", "end", values=(
                order.get('id', ''),
                order.get('created_at', ''),
                order.get('date', ''),
                order.get('department', ''),
                order.get('requester_fio', ''),
                obj_display,
                order.get('tech', ''),
                order.get('qty', ''),
                order.get('time', ''),
                order.get('hours', ''),
                order.get('assigned_vehicle', ''),
                order.get('driver', ''),
                status
            ), tags=(status,))

            self.row_meta[item_id] = {
                "comment": order.get("comment") or order.get("order_comment") or "",
                "note": order.get("note") or order.get("position_note") or "",
            }
    
    def on_row_double_click(self, event):
        selection = self.tree.selection()
        if not selection:
            return
        item = self.tree.item(selection[0])
        values = item['values']
        self._show_assignment_dialog(selection[0], values)

    def _show_assignment_dialog(self, item_id, values):
        dialog = tk.Toplevel(self)
        dialog.title("Назначение транспорта")
        dialog.geometry("640x700")
        dialog.resizable(True, True)
        dialog.transient(self)
        dialog.grab_set()

        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (640 // 2)
        y = (dialog.winfo_screenheight() // 2) - (700 // 2)
        dialog.geometry(f"640x700+{x}+{y}")

        # Контейнер со скроллом
        scroll_container = tk.Frame(dialog)
        scroll_container.pack(fill="both", expand=True, padx=0, pady=0)

        canvas = tk.Canvas(scroll_container, highlightthickness=0)
        scrollbar = ttk.Scrollbar(scroll_container, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas)

        def update_scroll_region(event=None):
            canvas.configure(scrollregion=canvas.bbox("all"))

        scrollable_frame.bind("<Configure>", update_scroll_region)

        canvas_window = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        def on_canvas_configure(event):
            canvas.itemconfig(canvas_window, width=event.width)
        canvas.bind("<Configure>", on_canvas_configure)

        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")

        def bind_mousewheel(event=None):
            canvas.bind_all("<MouseWheel>", _on_mousewheel)

        def unbind_mousewheel(event=None):
            canvas.unbind_all("<MouseWheel>")

        canvas.bind("<Enter>", bind_mousewheel)
        canvas.bind("<Leave>", unbind_mousewheel)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Информация о заявке
        info_frame = tk.LabelFrame(scrollable_frame, text="📋 Информация о заявке", padx=12, pady=10)
        info_frame.pack(fill="x", padx=15, pady=10)

        info_data = [
            ("Дата:", values[2]),
            ("Время подачи:", values[8] or 'не указано'),
            ("Заявитель:", values[4]),
            ("Объект:", values[5]),
        ]

        for label, value in info_data:
            row = tk.Frame(info_frame)
            row.pack(fill="x", pady=2)
            tk.Label(row, text=label, font=("Arial", 9), width=15, anchor="w").pack(side="left")
            tk.Label(row, text=value, font=("Arial", 9), anchor="w").pack(side="left", fill="x", expand=True)

        # Техника
        tech_frame = tk.Frame(info_frame, bg="#e3f2fd", relief="solid", borderwidth=1)
        tech_frame.pack(fill="x", pady=(8, 2), padx=5)
        tk.Label(
            tech_frame, 
            text=f"🚛 Техника: {values[6]} x {values[7]} ({values[9]} ч.)", 
            font=("Arial", 10, "bold"), 
            fg="#0066cc",
            bg="#e3f2fd",
            padx=8,
            pady=8
        ).pack(anchor="w")

        # Тексты
        meta = self.row_meta.get(item_id, {})
        order_comment = (meta.get("comment") or "").strip()
        position_note = (meta.get("note") or "").strip()

        texts_frame = tk.LabelFrame(scrollable_frame, text="🗒 Тексты заявки", padx=12, pady=10)
        texts_frame.pack(fill="x", padx=15, pady=(0, 8))

        row_c = tk.Frame(texts_frame)
        row_c.pack(fill="x", pady=2)
        tk.Label(row_c, text="Комментарий:", font=("Arial", 9), width=15, anchor="w").pack(side="left")
        tk.Label(row_c, text=(order_comment or "—"), font=("Arial", 9),
                 anchor="w", justify="left", wraplength=560).pack(side="left", fill="x", expand=True)

        row_n = tk.Frame(texts_frame)
        row_n.pack(fill="x", pady=2)
        tk.Label(row_n, text="Примечание:", font=("Arial", 9), width=15, anchor="w").pack(side="left")
        tk.Label(row_n, text=(position_note or "—"), font=("Arial", 9),
                 anchor="w", justify="left", wraplength=560).pack(side="left", fill="x", expand=True)

        warning_frame = tk.Frame(scrollable_frame, bg="#fff3cd", relief="solid", borderwidth=1)
        warning_label = tk.Label(
            warning_frame, 
            text="", 
            font=("Arial", 9), 
            bg="#fff3cd",
            fg="#856404",
            wraplength=580,
            justify="left"
        )
        warning_label.pack(padx=10, pady=8)

        assign_frame = tk.LabelFrame(scrollable_frame, text="🚗 Назначение транспорта", padx=15, pady=15)
        assign_frame.pack(fill="both", expand=True, padx=15, pady=5)

        current_assignment = values[10]
        current_type = ""
        current_name = ""
        current_plate = ""

        if current_assignment and " | " in current_assignment:
            parts = current_assignment.split(" | ")
            current_type = parts[0].strip() if len(parts) > 0 else ""
            current_name = parts[1].strip() if len(parts) > 1 else ""
            current_plate = parts[2].strip() if len(parts) > 2 else ""
        elif current_assignment:
            current_type = current_assignment.strip()

        tk.Label(assign_frame, text="Тип техники:", font=("Arial", 9, "bold")).grid(
            row=0, column=0, sticky="w", pady=(5, 2)
        )
        vehicle_type_var = tk.StringVar(value=current_type)
        cmb_vehicle_type = ttk.Combobox(
            assign_frame, 
            textvariable=vehicle_type_var,
            values=self.vehicle_types,
            state="readonly",
            width=55,
            font=("Arial", 9)
        )
        cmb_vehicle_type.grid(row=1, column=0, pady=(0, 12), sticky="we")

        tk.Label(assign_frame, text="Наименование:", font=("Arial", 9, "bold")).grid(
            row=2, column=0, sticky="w", pady=(5, 2)
        )
        vehicle_name_var = tk.StringVar(value="")
        cmb_vehicle_name = ttk.Combobox(
            assign_frame, 
            textvariable=vehicle_name_var,
            values=[],
            state="readonly",
            width=55,
            font=("Arial", 9)
        )
        cmb_vehicle_name.grid(row=3, column=0, pady=(0, 12), sticky="we")

        tk.Label(assign_frame, text="Гос. номер:", font=("Arial", 9, "bold")).grid(
            row=4, column=0, sticky="w", pady=(5, 2)
        )
        vehicle_plate_var = tk.StringVar(value="")
        cmb_vehicle_plate = ttk.Combobox(
            assign_frame, 
            textvariable=vehicle_plate_var,
            values=[],
            state="readonly",
            width=55,
            font=("Arial", 9)
        )
        cmb_vehicle_plate.grid(row=5, column=0, pady=(0, 12), sticky="we")

        selection_info = tk.Label(
            assign_frame,
            text="Выберите сначала тип, затем наименование и гос. номер",
            font=("Arial", 8),
            fg="#666"
        )
        selection_info.grid(row=6, column=0, sticky="w", pady=(0, 10))

        def update_names(*args):
            selected_type = vehicle_type_var.get()
            vehicle_name_var.set("")
            vehicle_plate_var.set("")
    
            if not selected_type:
                cmb_vehicle_name['values'] = []
                cmb_vehicle_plate['values'] = []
                cmb_vehicle_name.state(['disabled'])
                cmb_vehicle_plate.state(['disabled'])
                selection_info.config(text="Выберите тип техники", fg="#666")
                return
    
            names = sorted(set(
                v['name'] for v in self.vehicles 
                if v['type'] == selected_type and v['name']
            ))
    
            cmb_vehicle_name['values'] = names
            cmb_vehicle_name.state(['!disabled'])
            cmb_vehicle_plate['values'] = []
            cmb_vehicle_plate.state(['disabled'])
    
            if len(names) == 0:
                selection_info.config(text="Нет доступных наименований для этого типа", fg="#dc3545")
            elif len(names) == 1:
                vehicle_name_var.set(names[0])
            else:
                selection_info.config(text=f"Доступно наименований: {len(names)}", fg="#666")

        def update_plates(*args):
            selected_type = vehicle_type_var.get()
            selected_name = vehicle_name_var.get()
            vehicle_plate_var.set("")
    
            if not selected_type or not selected_name:
                cmb_vehicle_plate['values'] = []
                cmb_vehicle_plate.state(['disabled'])
                return
    
            plates = sorted(set(
                v['plate'] for v in self.vehicles 
                if v['type'] == selected_type 
                and v['name'] == selected_name 
                and v['plate']
            ))
    
            cmb_vehicle_plate['values'] = plates
            cmb_vehicle_plate.state(['!disabled'])
    
            if len(plates) == 0:
                selection_info.config(text="Нет доступных гос. номеров", fg="#dc3545")
            elif len(plates) == 1:
                vehicle_plate_var.set(plates[0])
                selection_info.config(text=f"✓ Назначен: {get_full_vehicle_string()}", fg="#28a745")
            else:
                selection_info.config(text=f"Доступно гос. номеров: {len(plates)}", fg="#666")

        def get_full_vehicle_string() -> str:
            parts = []
            if vehicle_type_var.get():
                parts.append(vehicle_type_var.get())
            if vehicle_name_var.get():
                parts.append(vehicle_name_var.get())
            if vehicle_plate_var.get():
                parts.append(vehicle_plate_var.get())
            return " | ".join(parts) if parts else ""

        vehicle_type_var.trace_add("write", update_names)
        vehicle_name_var.trace_add("write", update_plates)

        ttk.Separator(assign_frame, orient='horizontal').grid(
            row=7, column=0, sticky='ew', pady=15
        )

        tk.Label(assign_frame, text="Водитель:", font=("Arial", 9, "bold")).grid(
            row=8, column=0, sticky="w", pady=(5, 2)
        )

        driver_count_label = tk.Label(
            assign_frame, 
            text=f"(доступно: {len(self.drivers)} чел.)",
            font=("Arial", 8),
            fg="#666"
        )
        driver_count_label.grid(row=8, column=0, sticky="e", pady=(5, 2))

        driver_var = tk.StringVar(value=values[11])

        driver_display_list = []
        for d in self.drivers:
            display = f"{d['fio']}"
            if d.get('dep'):
                display += f" ({d['dep']})"
            driver_display_list.append(display)

        cmb_driver = ttk.Combobox(
            assign_frame,
            textvariable=driver_var,
            values=driver_display_list,
            width=55,
            font=("Arial", 9)
        )
        cmb_driver.grid(row=9, column=0, pady=(0, 12), sticky="we")

        tk.Label(assign_frame, text="Статус:", font=("Arial", 9, "bold")).grid(
            row=10, column=0, sticky="w", pady=(5, 2)
        )
        status_var = tk.StringVar(value=values[12])
        cmb_status = ttk.Combobox(
            assign_frame,
            textvariable=status_var,
            values=["Новая", "Назначена", "В работе", "Выполнена"],
            state="readonly",
            width=55,
            font=("Arial", 9)
        )
        cmb_status.grid(row=11, column=0, pady=(0, 15), sticky="we")

        assign_frame.grid_columnconfigure(0, weight=1)

        def check_conflicts(*args):
            selected_vehicle = get_full_vehicle_string()
            if not selected_vehicle:
                warning_frame.pack_forget()
                return
    
            req_date = values[2]
            req_time = values[8]
            current_id = values[0]
    
            conflicts = self._check_vehicle_conflict(selected_vehicle, req_date, req_time, current_id)
    
            if conflicts:
                warning_text = f"⚠️ ВНИМАНИЕ! Автомобиль '{selected_vehicle}' уже назначен на {len(conflicts)} заявку(-и) в этот день:\n\n"
                for i, conf in enumerate(conflicts, 1):
                    warning_text += f"{i}. {conf['time']} — {conf['requester']} ({conf['object']}) [{conf['status']}]\n"
                warning_text += "\nПроверьте возможность выполнения заявок!"
        
                warning_label.config(text=warning_text)
                warning_frame.pack(fill="x", padx=15, pady=(0, 5))
            else:
                warning_frame.pack_forget()

        def on_vehicle_or_driver_change(*args):
            if get_full_vehicle_string() and driver_var.get():
                if status_var.get() == "Новая":
                    status_var.set("Назначена")

        vehicle_plate_var = tk.StringVar(value="")
        cmb_vehicle_plate['textvariable'] = vehicle_plate_var
        vehicle_plate_var.trace_add("write", on_vehicle_or_driver_change)
        driver_var.trace_add("write", on_vehicle_or_driver_change)

        button_container = tk.Frame(dialog, bg="#f0f0f0", relief="raised", borderwidth=1)
        button_container.pack(fill="x", side="bottom", padx=0, pady=0)

        def save_and_close():
            if not get_full_vehicle_string():
                messagebox.showwarning("Назначение", "Выберите транспорт!", parent=dialog)
                return
    
            driver_name = driver_var.get()
            if " (" in driver_name:
                driver_name = driver_name.split(" (")[0].strip()
    
            new_values = list(values)
            new_values[10] = get_full_vehicle_string()
            new_values[11] = driver_name
            new_values[12] = status_var.get()
            self.tree.item(item_id, values=new_values, tags=(new_values[12],))
    
            unbind_mousewheel()
            dialog.destroy()

        def cancel_and_close():
            unbind_mousewheel()
            dialog.destroy()

        ttk.Button(button_container, text="✓ Сохранить", command=save_and_close, width=20).pack(side="left", padx=15, pady=12)
        ttk.Button(button_container, text="✗ Отмена", command=cancel_and_close, width=20).pack(side="left", padx=5, pady=12)

        dialog.update_idletasks()
        scrollable_frame.update_idletasks()
        canvas.update_idletasks()
    
        if current_type:
            vehicle_type_var.set(current_type)
            dialog.update_idletasks()
            # После установки типа имена подтянутся через trace
            if current_name:
                vehicle_name_var.set(current_name)
                dialog.update_idletasks()
                if current_plate:
                    vehicle_plate_var.set(current_plate)
                    dialog.update_idletasks()

        canvas.configure(scrollregion=canvas.bbox("all"))
        canvas.yview_moveto(0)
        dialog.update()

        cmb_vehicle_type.focus_set()
        dialog.bind("<Return>", lambda e: save_and_close())
        dialog.bind("<Escape>", lambda e: cancel_and_close())

        check_conflicts()

    def save_assignments(self):
        """Сохранение назначений в PostgreSQL"""
        try:
            assignments = []
            for item in self.tree.get_children():
                values = self.tree.item(item)['values']
                assignments.append({
                    'id': values[0],             # id позиции (transport_order_positions.id)
                    'assigned_vehicle': values[10],
                    'driver': values[11],
                    'status': values[12],
                })

            if not assignments:
                messagebox.showwarning("Сохранение", "Нет данных для сохранения")
                return

            conn = get_db_connection()
            try:
                with conn:
                    with conn.cursor() as cur:
                        for a in assignments:
                            pos_id = a.get('id')
                            if not pos_id:
                                continue
                            cur.execute(
                                """
                                UPDATE transport_order_positions
                                SET assigned_vehicle = %s,
                                    driver = %s,
                                    status = %s
                                WHERE id = %s
                                """,
                                (
                                    (a.get('assigned_vehicle') or "").strip(),
                                    (a.get('driver') or "").strip(),
                                    (a.get('status') or "Новая").strip(),
                                    pos_id,
                                ),
                            )
                messagebox.showinfo("Сохранение", f"Назначения успешно сохранены.\nОбновлено записей: {len(assignments)}")
            finally:
                conn.close()

        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка сохранения в БД:\n{e}")

# ------------------------- Вариант standalone-окна -------------------------

class SpecialOrdersApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1180x720")
        self.resizable(True, True)
        page = SpecialOrdersPage(self)
        page.pack(fill="both", expand=True)


# ------------------------- API для встраивания -------------------------

def create_page(parent) -> tk.Frame:
    ensure_config()
    try:
        return SpecialOrdersPage(parent)
    except Exception:
        import traceback
        messagebox.showerror("Заявка — ошибка", traceback.format_exc(), parent=parent)
        return tk.Frame(parent)

def create_planning_page(parent) -> tk.Frame:
    ensure_config()
    try:
        return TransportPlanningPage(parent)
    except Exception:
        import traceback
        messagebox.showerror("Планирование — ошибка", traceback.format_exc(), parent=parent)
        return tk.Frame(parent)

def open_special_orders(parent=None):
    """
    Совместимость: если parent задан — открываем Toplevel с встраиваемой страницей.
    Если не задан — отдельное окно как раньше.
    """
    if parent is None:
        app = SpecialOrdersApp()
        app.mainloop()
        return app
    win = tk.Toplevel(parent)
    win.title(APP_TITLE)
    win.geometry("1180x720")
    page = SpecialOrdersPage(win)
    page.pack(fill="both", expand=True)
    return win

# ------------------------- Утилиты -------------------------

def safe_filename(s: str, maxlen: int = 60) -> str:
    if not s:
        return "NOID"
    s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", str(s)).strip()
    s = re.sub(r"_+", "_", s)
    return s[:maxlen] if len(s) > maxlen else s


if __name__ == "__main__":
    ensure_config()
    app = SpecialOrdersApp()
    app.mainloop()
