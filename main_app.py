import os
import re
import sys
import csv
import json
import math
import calendar
import subprocess
import configparser
import urllib.request
import urllib.error
import urllib.parse
import traceback
import threading
import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse, parse_qs
import hashlib
import os as _os
from io import BytesIO
from datetime import datetime, date
from pathlib import Path
from typing import List, Tuple, Optional, Any, Dict, NamedTuple
import base64

# --- Импорты сторонних библиотек ---
try:
    import pandas as pd
except ImportError:
    pd = None

try:
    from PIL import Image, ImageTk
except Exception:
    Image = ImageTk = None

import logging

# Простейшее логирование в файл рядом с программой
logging.basicConfig(
    filename="main_app_log.txt",
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8",
)
logging.debug("=== main_app запущен ===")

# Мягкий импорт модулей
try:
    import BudgetAnalyzer  # должен содержать create_page(parent)
except Exception:
    BudgetAnalyzer = None

try:
    import assets_logo as _assets_logo
    _LOGO_BASE64 = getattr(_assets_logo, "LOGO_BASE64", None)
except Exception:
    _LOGO_BASE64 = None

try:
    import SpecialOrders  # должен содержать create_page/create_planning_page
except Exception:
    SpecialOrders = None

try:
    import timesheet_transformer  # должен содержать open_converter(parent)
except Exception:
    timesheet_transformer = None

# --- логируем импорт модуля питания ---
logging.debug("Пробуем импортировать meals_module...")
try:
    import meals_module  # обновлённый модуль питания (работает с БД)
    logging.debug(f"meals_module импортирован: {meals_module}")
except Exception:
    logging.exception("Ошибка при импорте meals_module")
    meals_module = None

# --- логируем импорт settings_manager ---
logging.debug("Пробуем импортировать settings_manager...")
try:
    import settings_manager as Settings
    logging.debug("settings_manager импортирован успешно")
except Exception:
    logging.exception("Ошибка при импорте settings_manager")
    Settings = None

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

APP_NAME = "Управление строительством (Главное меню)"

# ------------- КОНФИГ, СХЕМЫ И КОНСТАНТЫ -------------

CONFIG_FILE = "tabel_config.ini"
CONFIG_SECTION_PATHS = "Paths"
CONFIG_SECTION_UI = "UI"
CONFIG_SECTION_INTEGR = "Integrations"

KEY_OUTPUT_DIR = "output_dir"
KEY_EXPORT_PWD = "export_password"
KEY_SELECTED_DEP = "selected_department"

OUTPUT_DIR_DEFAULT = "Объектные_табели"
RAW_LOGO_URL = "https://raw.githubusercontent.com/alekseyvz-dotcom/TimesheetTransformer/main/logo.png"
TINY_PNG_BASE64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/w8AAn8B9w3G2kIAAAAASUVORK5CYII="
)

# Если settings_manager есть — используем его
if Settings:
    ensure_config = Settings.ensure_config
    read_config = Settings.read_config
    write_config = Settings.write_config

    get_output_dir_from_config = Settings.get_output_dir_from_config
    get_export_password_from_config = Settings.get_export_password_from_config

    get_selected_department_from_config = Settings.get_selected_department_from_config
    set_selected_department_in_config = Settings.set_selected_department_in_config
else:
    # fallback на ini‑файл
    def exe_dir() -> Path:
        if getattr(sys, "frozen", False):
            return Path(sys.executable).resolve().parent
        return Path(__file__).resolve().parent

    def config_path() -> Path:
        return exe_dir() / CONFIG_FILE

    def ensure_config():
        cp = config_path()
        if cp.exists():
            cfg = configparser.ConfigParser()
            cfg.read(cp, encoding="utf-8")
            changed = False
            if not cfg.has_section(CONFIG_SECTION_PATHS):
                cfg[CONFIG_SECTION_PATHS] = {}
                changed = True
            if KEY_OUTPUT_DIR not in cfg[CONFIG_SECTION_PATHS]:
                cfg[CONFIG_SECTION_PATHS][KEY_OUTPUT_DIR] = str(exe_dir() / OUTPUT_DIR_DEFAULT)
                changed = True
            if not cfg.has_section(CONFIG_SECTION_UI):
                cfg[CONFIG_SECTION_UI] = {}
                changed = True
            if KEY_SELECTED_DEP not in cfg[CONFIG_SECTION_UI]:
                cfg[CONFIG_SECTION_UI][KEY_SELECTED_DEP] = "Все"
                changed = True
            if not cfg.has_section(CONFIG_SECTION_INTEGR):
                cfg[CONFIG_SECTION_INTEGR] = {}
                changed = True
            if KEY_EXPORT_PWD not in cfg[CONFIG_SECTION_INTEGR]:
                cfg[CONFIG_SECTION_INTEGR][KEY_EXPORT_PWD] = "2025"
                changed = True
            if changed:
                with open(cp, "w", encoding="utf-8") as f:
                    cfg.write(f)
            return

        cfg = configparser.ConfigParser()
        cfg[CONFIG_SECTION_PATHS] = {
            KEY_OUTPUT_DIR: str(exe_dir() / OUTPUT_DIR_DEFAULT),
        }
        cfg[CONFIG_SECTION_UI] = {KEY_SELECTED_DEP: "Все"}
        cfg[CONFIG_SECTION_INTEGR] = {KEY_EXPORT_PWD: "2025"}
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

    def get_output_dir_from_config() -> Path:
        cfg = read_config()
        raw = cfg.get(CONFIG_SECTION_PATHS, KEY_OUTPUT_DIR, fallback=str(exe_dir() / OUTPUT_DIR_DEFAULT))
        return Path(os.path.expandvars(raw))

    def get_export_password_from_config() -> str:
        cfg = read_config()
        return cfg.get(CONFIG_SECTION_INTEGR, KEY_EXPORT_PWD, fallback="2025")

    def get_selected_department_from_config() -> str:
        cfg = read_config()
        return cfg.get(CONFIG_SECTION_UI, KEY_SELECTED_DEP, fallback="Все")

    def set_selected_department_in_config(dep: str):
        cfg = read_config()
        if not cfg.has_section(CONFIG_SECTION_UI):
            cfg[CONFIG_SECTION_UI] = {}
        cfg[CONFIG_SECTION_UI][KEY_SELECTED_DEP] = dep or "Все"
        write_config(cfg)


def exe_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def embedded_logo_image(parent, max_w=360, max_h=160):
    b64 = _LOGO_BASE64

    if not b64:
        try:
            data = urllib.request.urlopen(RAW_LOGO_URL, timeout=5).read()
            b64 = base64.b64encode(data).decode("ascii")
        except Exception:
            b64 = TINY_PNG_BASE64

    if Image and ImageTk:
        try:
            raw = base64.b64decode(b64.strip())
            im = Image.open(BytesIO(raw))
            im.thumbnail((max_w, max_h), Image.LANCZOS)
            return ImageTk.PhotoImage(im, master=parent)
        except Exception:
            pass

    try:
        ph = tk.PhotoImage(data=b64.strip(), master=parent)
        w, h = ph.width(), ph.height()
        k = max(w / max_w, h / max_h, 1)
        if k > 1:
            k = max(1, int(k))
            ph = ph.subsample(k, k)
        return ph
    except Exception:
        return None

# ================= БД: подключение и пользователи =================

def get_db_connection():
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


def _hash_password(password: str, salt: Optional[bytes] = None) -> str:
    if salt is None:
        salt = _os.urandom(16)
    iterations = 260000
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${dk.hex()}"


def _verify_password(password: str, stored_hash: str) -> bool:
    try:
        if stored_hash.startswith("pbkdf2_sha256$"):
            _, it_str, salt_hex, hash_hex = stored_hash.split("$", 3)
            iterations = int(it_str)
            salt = bytes.fromhex(salt_hex)
            dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
            return dk.hex() == hash_hex
        else:
            return password == stored_hash
    except Exception:
        return False


def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    logging.debug(f"authenticate_user: пытаемся авторизовать {username!r}")
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id,
                       username,
                       password_hash,
                       is_active,
                       full_name,
                       role
                FROM app_users
                WHERE username = %s
                """,
                (username,),
            )
            row = cur.fetchone()
            if not row:
                return None
            if not row["is_active"]:
                return None
            if not _verify_password(password, row["password_hash"]):
                return None
            row.pop("password_hash", None)
            return dict(row)
    finally:
        conn.close()

# ---------- Справочники из БД ----------

def load_employees_from_db() -> List[Tuple[str, str, str, str]]:
    """
    Возвращает список сотрудников:
      [(fio, tbn, position, department), ...]
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
            rows = cur.fetchall()
            return [(fio or "", tbn or "", pos or "", dep or "") for fio, tbn, pos, dep in rows]
    finally:
        conn.close()


def load_objects_from_db() -> List[Tuple[str, str]]:
    """
    Возвращает список объектов [(code, address)], где code — excel_id/ext_id.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(NULLIF(excel_id, ''), NULLIF(ext_id, '')) AS code,
                    address
                  FROM objects
                 ORDER BY address
                """
            )
            rows = cur.fetchall()
            return [(code or "", addr or "") for code, addr in rows]
    finally:
        conn.close()

# ------------- Утилиты для работы со временем и данными -------------

def month_days(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]

def month_name_ru(month: int) -> str:
    names = [
        "Январь","Февраль","Март","Апрель","Май","Июнь",
        "Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"
    ]
    return names[month-1]

def parse_hours_value(v: Any) -> Optional[float]:
    s = str(v or "").strip()
    if not s:
        return None
    if "(" in s:
        s = s.split("(")[0].strip()
    if "/" in s:
        total = 0.0
        any_part = False
        for part in s.split("/"):
            n = parse_hours_value(part)
            if isinstance(n, (int, float)):
                total += float(n)
                any_part = True
        return total if any_part else None
    if ":" in s:
        p = s.split(":")
        try:
            hh = float(p[0].replace(",", "."))
            mm = float((p[1] if len(p) > 1 else "0").replace(",", "."))
            ss = float((p[2] if len(p) > 2 else "0").replace(",", "."))
            return hh + mm / 60.0 + ss / 3600.0
        except Exception:
            pass
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def parse_overtime(v: Any) -> Tuple[Optional[float], Optional[float]]:
    s = str(v or "").strip()
    if "(" not in s or ")" not in s:
        return None, None
    try:
        start = s.index("(")
        end = s.index(")")
        overtime_str = s[start + 1:end].strip()
        if "/" in overtime_str:
            parts = overtime_str.split("/")
            day_ot = float(parts[0].replace(",", ".")) if parts[0].strip() else 0.0
            night_ot = float(parts[1].replace(",", ".")) if len(parts) > 1 and parts[1].strip() else 0.0
            return day_ot, night_ot
        else:
            ot = float(overtime_str.replace(",", "."))
            return ot, 0.0
    except Exception:
        return None, None

def safe_filename(s: str, maxlen: int = 60) -> str:
    if not s:
        return "NOID"
    s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", str(s)).strip()
    s = re.sub(r"_+", "_", s)
    return s[:maxlen] if len(s) > maxlen else s

def find_logo_path() -> Optional[Path]:
    candidates = [
        exe_dir() / "assets" / "logo.png",
        exe_dir() / "assets" / "logo.gif",
        exe_dir() / "assets" / "logo.jpg",
        exe_dir() / "logo.png",
        exe_dir() / "logo.gif",
        exe_dir() / "logo.jpg",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None

# ------------- Ряд реестра (RowWidget), диалоги и т.д. -------------
# (ниже — ваш неизменённый код RowWidget, CopyFromDialog, BatchAddDialog, HoursFillDialog,
#  AutoCompleteCombobox, ExportMonthDialog, perform_summary_export — я оставляю их как есть)

# ...  (ОПУЩЕНО ДЛЯ КРАТКОСТИ — ваш текущий код RowWidget и диалогов без изменений)

# ================= СТРАНИЦЫ =================

class HomePage(tk.Frame):
    # как в вашем коде (без изменений)
    ...

class LoginPage(tk.Frame):
    # как в вашем коде (без изменений)
    ...

class TimesheetPage(tk.Frame):
    COLPX = {"fio": 200, "tbn": 100, "day": 36, "days": 46, "hours": 56, "btn52": 40, "del": 66}
    MIN_FIO_PX = 140
    MAX_FIO_PX = 260
    HEADER_BG = "#d0d0d0"

    def __init__(self, master):
        super().__init__(master)
        self.base_dir = exe_dir()
        self.out_dir = get_output_dir_from_config()
        self.out_dir.mkdir(parents=True, exist_ok=True)

        self.DAY_ENTRY_FONT = ("Segoe UI", 8)
        self._fit_job = None

        self._load_spr_data_from_db()
        self.model_rows: List[Dict[str, Any]] = []
        self.current_page = 1
        self.page_size = tk.IntVar(value=50)
        self._suspend_sync = False

        self._build_ui()
        self._render_page(1)
        self._load_existing_rows()

        self.bind("<Configure>", self._on_window_configure)
        self.after(120, self._auto_fit_columns)

    def _load_spr_data_from_db(self):
        employees = load_employees_from_db()
        objects = load_objects_from_db()

        self.employees = employees
        self.objects = objects

        self.emp_names = [fio for (fio, _, _, _) in self.employees]
        self.emp_info = {fio: (tbn, pos) for (fio, tbn, pos, _) in self.employees}

        deps = sorted({(dep or "").strip() for (_, _, _, dep) in self.employees if (dep or "").strip()})
        self.departments = ["Все"] + deps

        self.addr_to_ids: Dict[str, List[str]] = {}
        for oid, addr in self.objects:
            if not addr:
                continue
            self.addr_to_ids.setdefault(addr, [])
            if oid and oid not in self.addr_to_ids[addr]:
                self.addr_to_ids[addr].append(oid)
        addresses_set = set(self.addr_to_ids.keys()) | {addr for _, addr in self.objects if addr}
        self.address_options = sorted(addresses_set)

    # остальная часть TimesheetPage остаётся такой же, только:
    # - self._load_spr_data() → self._load_spr_data_from_db()
    # - методы reload_spravochnik/open_spravochnik/refresh_spravochnik_global удаляем,
    #   вместо reload_spravochnik можно просто снова вызвать _load_spr_data_from_db()
    #   и переинициализировать комбобоксы.

# ------------- MainApp с вкладкой настроек питания -------------

class MainApp(tk.Tk):
    def __init__(self, current_user: Optional[Dict[str, Any]] = None):
        super().__init__()
        self.current_user: Dict[str, Any] = current_user or {}
        self.is_authenticated: bool = bool(current_user)
        self.title(APP_NAME)
        self.geometry("1024x720")
        self.minsize(980, 640)
        self.resizable(True, True)

        ensure_config()
        self._pages: Dict[str, tk.Widget] = {}
        self._menubar = None
        self._menu_meals = None
        self._menu_transport = None
        self._menu_meals_planning_index = None
        self._menu_meals_settings_index = None
        self._menu_transport_planning_index = None
        self._menu_transport_registry_index = None
        self._menu_settings_index = None

        menubar = tk.Menu(self)

        menubar.add_command(label="Главная", command=self.show_home)

        m_ts = tk.Menu(menubar, tearoff=0)
        m_ts.add_command(
            label="Создать",
            command=lambda: self._show_page("timesheet", lambda parent: TimesheetPage(parent)),
        )
        menubar.add_cascade(label="Объектный табель", menu=m_ts)

        # Автотранспорт — как у вас (без изменений)
        m_transport = tk.Menu(menubar, tearoff=0)
        self._menu_transport = m_transport
        if SpecialOrders and hasattr(SpecialOrders, "create_page"):
            m_transport.add_command(
                label="📝 Создать заявку",
                command=lambda: self._show_page("transport", lambda parent: SpecialOrders.create_page(parent)),
            )
        else:
            m_transport.add_command(label="📝 Создать заявку", command=self.run_special_orders_exe)

        self._menu_transport_planning_index = None
        if SpecialOrders and hasattr(SpecialOrders, "create_planning_page"):
            self._menu_transport_planning_index = 1
            m_transport.add_command(
                label="🚛Планирование транспорта",
                command=lambda: self._show_page(
                    "planning", lambda parent: SpecialOrders.create_planning_page(parent)
                ),
            )

        self._menu_transport_registry_index = None
        if SpecialOrders and hasattr(SpecialOrders, "create_transport_registry_page"):
            self._menu_transport_registry_index = (
                m_transport.index("end") + 1 if m_transport.index("end") is not None else 0
            )
            m_transport.add_command(
                label="🚘Реестр транспорта",
                command=lambda: self._show_page(
                    "transport_registry",
                    lambda parent: SpecialOrders.create_transport_registry_page(parent),
                ),
            )
        menubar.add_cascade(label="Автотранспорт", menu=m_transport)

        # ====== МЕНЮ ПИТАНИЕ (с вкладкой "Настройки") ======
        logging.debug(f"Строим меню Питание. meals_module={meals_module}")
        m_meals = tk.Menu(menubar, tearoff=0)
        self._menu_meals = m_meals

        if meals_module and hasattr(meals_module, "create_meals_order_page"):
            m_meals.add_command(
                label="📝 Создать заявку",
                command=lambda: self._show_page(
                    "meals_order",
                    lambda parent: meals_module.create_meals_order_page(parent),
                ),
            )
        else:
            m_meals.add_command(label="📝 Создать заявку", command=self.run_meals_exe)

        self._menu_meals_planning_index = None
        if meals_module and hasattr(meals_module, "create_meals_planning_page"):
            self._menu_meals_planning_index = 1
            m_meals.add_command(
                label="🍽️Планирование питания",
                command=lambda: self._show_page(
                    "meals_planning",
                    lambda parent: meals_module.create_meals_planning_page(parent),
                ),
            )

        # Вкладка "Настройки" для питания: добавляем пункт меню,
        # но фактическую доступность будем управлять по роли
        self._menu_meals_settings_index = None
        if meals_module and hasattr(meals_module, "create_meals_settings_page"):
            self._menu_meals_settings_index = m_meals.index("end") + 1 if m_meals.index("end") is not None else 0
            m_meals.add_command(
                label="⚙ Настройки питания",
                command=lambda: self._show_page(
                    "meals_settings",
                    lambda parent: meals_module.create_meals_settings_page(
                        parent, (self.current_user or {}).get("role") or "specialist"
                    ),
                ),
            )

        m_meals.add_separator()
        m_meals.add_command(label="📂 Открыть папку заявок", command=self.open_meals_folder)
        menubar.add_cascade(label="Питание", menu=m_meals)
        # ==================================

        m_analytics = tk.Menu(menubar, tearoff=0)
        m_analytics.add_command(label="Экспорт свода (XLSX/CSV)", command=self.summary_export)
        menubar.add_cascade(label="Аналитика", menu=m_analytics)

        m_tools = tk.Menu(menubar, tearoff=0)
        if timesheet_transformer and hasattr(timesheet_transformer, "open_converter"):
            m_tools.add_command(
                label="Конвертер табеля (1С)",
                command=lambda: timesheet_transformer.open_converter(self),
            )
        else:
            m_tools.add_command(label="Конвертер табеля (1С)", command=self.run_converter_exe)
        if BudgetAnalyzer and hasattr(BudgetAnalyzer, "create_page"):
            m_tools.add_command(
                label="Анализ смет",
                command=lambda: self._show_page(
                    "budget", lambda parent: BudgetAnalyzer.create_page(parent)
                ),
            )
        else:
            m_tools.add_command(
                label="Анализ смет",
                command=lambda: messagebox.showwarning(
                    "Анализ смет", "Модуль BudgetAnalyzer.py не найден."
                ),
            )
        menubar.add_cascade(label="Инструменты", menu=m_tools)

        m_settings = tk.Menu(menubar, tearoff=0)
        m_settings.add_command(
            label="Открыть настройки",
            command=lambda: Settings.open_settings_window(self)
            if Settings
            else messagebox.showwarning(
                "Настройки", "Модуль settings_manager не найден."
            ),
        )
        menubar.add_cascade(label="Настройки", menu=m_settings)
        self._menu_settings_index = menubar.index("end")

        self.config(menu=menubar)
        self._menubar = menubar

        self._set_user(None)

        header = tk.Frame(self)
        header.pack(fill="x", padx=12, pady=(10, 4))
        tk.Label(header, text="Управление строительством", font=("Segoe UI", 16, "bold")).pack(side="left")
        tk.Label(header, text="Выберите раздел в верхнем меню", font=("Segoe UI", 10), fg="#555").pack(side="right")

        self.content = tk.Frame(self, bg="#f7f7f7")
        self.content.pack(fill="both", expand=True)

        footer = tk.Frame(self)
        footer.pack(fill="x", padx=12, pady=(0, 10))
        tk.Label(
            footer,
            text="Разработал Алексей Зезюкин, АНО МЛСТ 2025",
            font=("Segoe UI", 8),
            fg="#666",
        ).pack(side="right")

        self.show_login()

    def _set_user(self, user: Optional[Dict[str, Any]]):
        """Устанавливает текущего пользователя и обновляет заголовок окна."""
        self.current_user = user or {}
        self.is_authenticated = bool(user)
        caption = ""
        if user:
            fn = user.get("full_name") or ""
            un = user.get("username") or ""
            caption = f" — {fn or un}"
        self.title(APP_NAME + caption)
        self._apply_role_visibility()

    def show_login(self):
        self._show_page("login", lambda parent: LoginPage(parent, app_ref=self))

    def on_login_success(self, user: Dict[str, Any]):
        """Вызывается LoginPage при успешной авторизации."""
        logging.debug(f"MainApp.on_login_success: {user!r}")
        self._set_user(user)
        # После логина показываем домашнюю страницу
        self.show_home()

    def _show_page(self, key: str, builder):
        # Если пользователь не авторизован — разрешаем только страницу логина
        if not self.is_authenticated and key not in ("login",):
            messagebox.showwarning(
                "Доступ ограничен",
                "Для доступа к разделу необходимо войти в систему.",
                parent=self,
            )
            # принудительно показываем логин
            self.show_login()
            return

        # очистить контейнер
        for w in self.content.winfo_children():
            try:
                w.destroy()
            except Exception:
                pass

        # построить новый
        try:
            page = builder(self.content)
        except Exception as e:
            traceback.print_exc()
            messagebox.showerror("Ошибка", f"Не удалось открыть страницу:\n{e}")
            # Резерв — домашняя страница (если уже есть доступ)
            if self.is_authenticated:
                self.show_home()
            else:
                self.show_login()
            return

        if isinstance(page, tk.Widget) and page.master is self.content:
            try:
                page.pack_forget()
            except Exception:
                pass
        try:
            page.pack(fill="both", expand=True)
        except Exception:
            pass
        self._pages[key] = page

    def show_home(self):
        self._show_page("home", lambda parent: HomePage(parent))

    def _apply_role_visibility(self):
        """Включает/выключает пункты меню в зависимости от роли пользователя."""
        role = (self.current_user or {}).get("role") or "specialist"

        # --- Питание ---
        if self._menu_meals is not None:
            try:
                # "Создать заявку" (индекс 0) — всегда активен
                self._menu_meals.entryconfig(0, state="normal")
                # Планирование (индекс 1, если есть)
                if self._menu_meals_planning_index is not None:
                    st = "normal" if role in ("admin", "planner") else "disabled"
                    self._menu_meals.entryconfig(self._menu_meals_planning_index, state=st)
            except Exception:
                pass
        # --- Питание / Настройки питания только для admin ---
        if self._menu_meals is not None and self._menu_meals_settings_index is not None:
            try:
                st = "normal" if role == "admin" else "disabled"
                self._menu_meals.entryconfig(self._menu_meals_settings_index, state=st)
            except Exception:
                pass

        # --- Автотранспорт ---
        if self._menu_transport is not None:
            try:
                # "Создать заявку" (индекс 0) — всегда доступен
                self._menu_transport.entryconfig(0, state="normal")

                # Планирование — только admin/planner
                if self._menu_transport_planning_index is not None:
                    st = "normal" if role in ("admin", "planner") else "disabled"
                    self._menu_transport.entryconfig(self._menu_transport_planning_index, state=st)

                # Реестр транспорта — admin, planner, head
                if self._menu_transport_registry_index is not None:
                    st = "normal" if role in ("admin", "planner", "head") else "disabled"
                    self._menu_transport.entryconfig(self._menu_transport_registry_index, state=st)
            except Exception:
                pass


        # --- Верхнее меню "Настройки" только для admin ---
        if self._menubar is not None and self._menu_settings_index is not None:
            try:
                # Получаем текущее состояние пункта
                label = self._menubar.entrycget(self._menu_settings_index, "label")
                # Если роль не admin — "Прячем" пункт: делаем его "disabled"
                # (Tkinter не умеет полностью скрыть, только отключить. Если нужно
                # именно убрать пункт, можно перестраивать меню целиком.)
                state = "normal" if role == "admin" else "disabled"
                self._menubar.entryconfig(self._menu_settings_index, state=state)
            except Exception:
                pass

    # --- Справочник ---
    def open_spravochnik(self):
        path = get_spr_path_from_config()
        cfg = read_config()
        use_remote = cfg.get(CONFIG_SECTION_REMOTE, KEY_REMOTE_USE, fallback="false").strip().lower() in ("1","true","yes","on")
        if not path.exists():
            if use_remote:
                messagebox.showwarning("Справочник", "Включён удалённый справочник. Локальный файл отсутствует.")
                return
            if not messagebox.askyesno("Справочник", f"Локальный файл не найден:\n{path}\n\nСоздать пустой справочник?"):
                return
            try:
                ensure_spravochnik_local(path)
            except Exception as e:
                messagebox.showerror("Справочник", f"Не удалось создать файл:\n{e}")
                return
        try:
            os.startfile(path)
        except Exception as e:
            messagebox.showerror("Справочник", f"Не удалось открыть файл:\n{e}")

    def refresh_spravochnik_global(self):
        cfg = read_config()
        use_remote = cfg.get(CONFIG_SECTION_REMOTE, KEY_REMOTE_USE, fallback="false")
        link = cfg.get(CONFIG_SECTION_REMOTE, KEY_YA_PUBLIC_LINK, fallback="")
        path = get_spr_path_from_config()
        messagebox.showinfo(
            "Справочник",
            "Проверка параметров завершена.\n"
            f"Удалённый доступ: use_remote={use_remote}\n"
            f"Публичная ссылка: {link or '(не задана)'}\n"
            f"Локальный путь: {path}\n\n"
            "В окнах используйте «Обновить справочник» для перечтения."
        )

    # ========== НОВЫЙ МЕТОД: Открыть папку заявок ==========
    def open_orders_folder(self):
        """Открывает папку с заявками на автотранспорт"""
        try:
            orders_dir = exe_dir() / "Заявки_спецтехники"
            orders_dir.mkdir(parents=True, exist_ok=True)
            os.startfile(orders_dir)
        except Exception as e:
            messagebox.showerror("Папка заявок", f"Не удалось открыть папку:\n{e}")

    def open_meals_folder(self):
        """Открывает папку с заявками на питание"""
        try:
            meals_dir = exe_dir() / "Заявки_питание"
            meals_dir.mkdir(parents=True, exist_ok=True)
            os.startfile(meals_dir)
        except Exception as e:
            messagebox.showerror("Папка заявок", f"Не удалось открыть папку:\n{e}")

    def run_meals_exe(self):
        """Запуск standalone версии модуля питания"""
        try:
            p = exe_dir() / "meals_module.exe"
            if not p.exists():
                messagebox.showwarning("Заказ питания", "Не найден meals_module.exe рядом с программой.")
                return
            subprocess.Popen([str(p)], shell=False)
        except Exception as e:
            messagebox.showerror("Заказ питания", f"Не удалось запустить модуль:\n{e}")

    # ======================================================

    # --- Аналитика ---
    def summary_export(self):
        pwd = simpledialog.askstring("Сводный экспорт", "Введите пароль:", show="*", parent=self)
        if pwd is None:
            return
        if pwd != get_export_password_from_config():
            messagebox.showerror("Сводный экспорт", "Неверный пароль.")
            return

        dlg = ExportMonthDialog(self)
        if not getattr(dlg, "result", None):
            return
        y = dlg.result["year"]
        m = dlg.result["month"]
        fmt = dlg.result["fmt"]
        try:
            count, paths = perform_summary_export(y, m, fmt)
            if count <= 0:
                messagebox.showinfo("Сводный экспорт", "Не найдено строк для выгрузки.")
                return
            msg = f"Экспортировано строк: {count}\n\nФайлы:\n" + "\n".join(str(p) for p in paths)
            if paths and messagebox.askyesno("Экспорт завершен", msg + "\n\nОткрыть папку с отчетами?"):
                os.startfile(paths[0].parent)
        except Exception as e:
            messagebox.showerror("Сводный экспорт", f"Ошибка выгрузки:\n{e}")

    # --- Резервные запуски внешних EXE ---
    def run_special_orders_exe(self):
        try:
            p = exe_dir() / "SpecialOrders.exe"
            if not p.exists():
                messagebox.showwarning("Заказ спецтехники", "Не найден SpecialOrders.exe рядом с программой.")
                return
            subprocess.Popen([str(p)], shell=False)
        except Exception as e:
            messagebox.showerror("Заказ спецтехники", f"Не удалось запустить модуль:\n{e}")

    def run_converter_exe(self):
        try:
            p = exe_dir() / "TabelConverter.exe"
            if not p.exists():
                messagebox.showwarning("Конвертер", f"Не найден TabelConverter.exe рядом с программой.")
                return
            subprocess.Popen([str(p)], shell=False)
        except Exception as e:
            messagebox.showerror("Конвертер", f"Не удалось запустить конвертер:\n{e}")

logging.debug("Модуль main_app импортирован, готов к запуску.")

if __name__ == "__main__":
    logging.debug("Старт приложения без внешней авторизации (логин-страница внутри MainApp).")
    app = MainApp()
    app.mainloop()
