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
from typing import List, Tuple, Optional, Any, Dict
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
from tkinter import ttk, messagebox, simpledialog

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


def exe_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def config_path() -> Path:
    return exe_dir() / CONFIG_FILE


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


def embedded_logo_image(parent, max_w=360, max_h=160):
    """
    Источники по приоритету:
    1) _LOGO_BASE64 из assets_logo.py (если есть)
    2) RAW-скачивание из GitHub
    3) tiny PNG
    """
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
    """
    Проверяет логин/пароль в таблице app_users.
    При успехе возвращает dict с данными пользователя (без password_hash), иначе None.
    """
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


# ------------- Утилиты для времени и табеля -------------

def month_days(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def month_name_ru(month: int) -> str:
    names = [
        "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
        "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
    ]
    return names[month - 1]


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


# ------------- Логотип / домашняя страница -------------

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


class HomePage(tk.Frame):
    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")

        outer = tk.Frame(self, bg="#f7f7f7")
        outer.pack(fill="both", expand=True)

        center = tk.Frame(outer, bg="#f7f7f7")
        center.place(relx=0.5, rely=0.5, anchor="center")

        self.logo_img = embedded_logo_image(center, max_w=360, max_h=360)
        if self.logo_img:
            tk.Label(center, image=self.logo_img, bg="#f7f7f7").pack(anchor="center", pady=(0, 12))

        tk.Label(
            center,
            text="Добро пожаловать!",
            font=("Segoe UI", 18, "bold"),
            bg="#f7f7f7",
        ).pack(anchor="center", pady=(4, 6))
        tk.Label(
            center,
            text="Выберите раздел в верхнем меню.\nОбъектный табель → Создать — для работы с табелями.",
            font=("Segoe UI", 10),
            fg="#444",
            bg="#f7f7f7",
            justify="center",
        ).pack(anchor="center")


class LoginPage(tk.Frame):
    def __init__(self, master, app_ref: "MainApp"):
        super().__init__(master, bg="#f7f7f7")
        self.app_ref = app_ref

        outer = tk.Frame(self, bg="#f7f7f7")
        outer.pack(fill="both", expand=True)

        center = tk.Frame(outer, bg="#f7f7f7")
        center.place(relx=0.5, rely=0.5, anchor="center")

        tk.Label(
            center,
            text="Управление строительством",
            font=("Segoe UI", 16, "bold"),
            bg="#f7f7f7",
        ).grid(row=0, column=0, columnspan=2, pady=(0, 10))

        tk.Label(
            center,
            text="Вход в систему",
            font=("Segoe UI", 11),
            fg="#555",
            bg="#f7f7f7",
        ).grid(row=1, column=0, columnspan=2, pady=(0, 15))

        tk.Label(center, text="Логин:", bg="#f7f7f7").grid(row=2, column=0, sticky="e", padx=(0, 6), pady=4)
        tk.Label(center, text="Пароль:", bg="#f7f7f7").grid(row=3, column=0, sticky="e", padx=(0, 6), pady=4)

        self.ent_login = ttk.Entry(center, width=26)
        self.ent_login.grid(row=2, column=1, sticky="w", pady=4)
        self.ent_pass = ttk.Entry(center, width=26, show="*")
        self.ent_pass.grid(row=3, column=1, sticky="w", pady=4)

        btns = tk.Frame(center, bg="#f7f7f7")
        btns.grid(row=4, column=0, columnspan=2, pady=(12, 0), sticky="e")

        ttk.Button(btns, text="Войти", width=12, command=self._on_login).pack(side="left", padx=5)
        ttk.Button(btns, text="Выход", width=10, command=self._on_exit).pack(side="left", padx=5)

        self.ent_login.focus_set()
        self.bind_all("<Return>", self._on_enter)

    def _on_enter(self, event):
        if self.winfo_ismapped():
            self._on_login()

    def _on_login(self):
        username = self.ent_login.get().strip()
        password = self.ent_pass.get().strip()
        if not username or not password:
            messagebox.showwarning("Вход", "Укажите логин и пароль.", parent=self)
            return
        try:
            logging.debug(f"LoginPage: пробуем авторизовать {username!r}")
            user = authenticate_user(username, password)
        except Exception as e:
            logging.exception("Ошибка при обращении к БД в authenticate_user")
            messagebox.showerror("Вход", f"Ошибка при обращении к БД:\n{e}", parent=self)
            return

        if not user:
            messagebox.showerror("Вход", "Неверный логин или пароль.", parent=self)
            return

        self.app_ref.on_login_success(user)

    def _on_exit(self):
        self.app_ref.destroy()

# ================= СТРАНИЦА ТАБЕЛЕЙ (ИСПОЛЬЗУЕТ БАЗУ) =================

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

    def _build_ui(self):
        # Верхняя панель
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=8)

        tk.Label(top, text="Подразделение:").grid(row=0, column=0, sticky="w")
        deps = self.departments or ["Все"]
        self.cmb_department = ttk.Combobox(top, state="readonly", values=deps, width=48)
        self.cmb_department.grid(row=0, column=1, sticky="w", padx=(4, 12))
        try:
            saved_dep = get_selected_department_from_config()
            self.cmb_department.set(saved_dep if saved_dep in deps else deps[0])
        except Exception:
            self.cmb_department.set(deps[0])
        self.cmb_department.bind("<<ComboboxSelected>>", lambda e: self._on_department_select())

        tk.Label(top, text="Месяц:").grid(row=1, column=0, sticky="w", padx=(0, 4), pady=(8, 0))
        self.cmb_month = ttk.Combobox(top, state="readonly", width=12, values=[month_name_ru(i) for i in range(1, 13)])
        self.cmb_month.grid(row=1, column=1, sticky="w", pady=(8, 0))
        self.cmb_month.current(datetime.now().month - 1)
        self.cmb_month.bind("<<ComboboxSelected>>", lambda e: self._on_period_change())

        tk.Label(top, text="Год:").grid(row=1, column=2, sticky="w", padx=(16, 4), pady=(8, 0))
        self.spn_year = tk.Spinbox(top, from_=2000, to=2100, width=6, command=self._on_period_change)
        self.spn_year.grid(row=1, column=3, sticky="w", pady=(8, 0))
        self.spn_year.delete(0, "end")
        self.spn_year.insert(0, datetime.now().year)
        self.spn_year.bind("<FocusOut>", lambda e: self._on_period_change())

        tk.Label(top, text="Адрес:").grid(row=1, column=4, sticky="w", padx=(20, 4), pady=(8, 0))
        self.cmb_address = AutoCompleteCombobox(top, width=46)
        self.cmb_address.set_completion_list(self.address_options)
        self.cmb_address.grid(row=1, column=5, sticky="w", pady=(8, 0))
        self.cmb_address.bind("<<ComboboxSelected>>", self._on_address_select)
        self.cmb_address.bind("<FocusOut>", self._on_address_select)
        self.cmb_address.bind("<Return>", lambda e: self._on_address_select())
        self.cmb_address.bind("<KeyRelease>", lambda e: self._on_address_change(), add="+")

        tk.Label(top, text="ID объекта:").grid(row=1, column=6, sticky="w", padx=(16, 4), pady=(8, 0))
        self.cmb_object_id = ttk.Combobox(top, state="readonly", values=[], width=18)
        self.cmb_object_id.grid(row=1, column=7, sticky="w", pady=(8, 0))
        self.cmb_object_id.bind("<<ComboboxSelected>>", lambda e: self._load_existing_rows())

        tk.Label(top, text="ФИО:").grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.fio_var = tk.StringVar()
        self.cmb_fio = AutoCompleteCombobox(top, textvariable=self.fio_var, width=30)
        self.cmb_fio.set_completion_list(self.emp_names)
        self.cmb_fio.grid(row=2, column=1, sticky="w", pady=(8, 0))
        self.cmb_fio.bind("<<ComboboxSelected>>", self._on_fio_select)

        tk.Label(top, text="Табельный №:").grid(row=2, column=2, sticky="w", padx=(16, 4), pady=(8, 0))
        self.ent_tbn = ttk.Entry(top, width=14)
        self.ent_tbn.grid(row=2, column=3, sticky="w", pady=(8, 0))

        tk.Label(top, text="Должность:").grid(row=2, column=4, sticky="w", padx=(16, 4), pady=(8, 0))
        self.pos_var = tk.StringVar()
        self.ent_pos = ttk.Entry(top, textvariable=self.pos_var, width=40, state="readonly")
        self.ent_pos.grid(row=2, column=5, sticky="w", pady=(8, 0))

        # Кнопки действий
        btns = tk.Frame(top)
        btns.grid(row=3, column=0, columnspan=8, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Добавить в табель", command=self.add_row).grid(row=0, column=0, padx=4)
        ttk.Button(btns, text="Добавить подразделение", command=self.add_department_all).grid(row=0, column=1, padx=4)
        ttk.Button(btns, text="5/2 всем", command=self.fill_52_all).grid(row=0, column=2, padx=4)
        ttk.Button(btns, text="Проставить часы", command=self.fill_hours_all).grid(row=0, column=3, padx=4)
        ttk.Button(btns, text="Очистить все строки", command=self.clear_all_rows).grid(row=0, column=4, padx=4)
        ttk.Button(btns, text="Обновить справочник", command=self.reload_spravochnik).grid(row=0, column=5, padx=4)
        ttk.Button(btns, text="Копировать из месяца…", command=self.copy_from_month).grid(row=0, column=6, padx=4)
        ttk.Button(btns, text="Сохранить", command=self.save_all).grid(row=0, column=7, padx=4)

        # Основной контейнер с прокруткой
        main_frame = tk.Frame(self)
        main_frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        # Канвас для шапки (фиксирован сверху)
        self.header_canvas = tk.Canvas(main_frame, borderwidth=0, highlightthickness=0, height=28)
        self.header_canvas.grid(row=0, column=0, sticky="ew")

        # Канвас с телом таблицы (вертикально скроллится)
        self.main_canvas = tk.Canvas(main_frame, borderwidth=0, highlightthickness=0)
        self.main_canvas.grid(row=1, column=0, sticky="nsew")

        # Скроллбары
        self.vscroll = ttk.Scrollbar(main_frame, orient="vertical", command=self.main_canvas.yview)
        self.vscroll.grid(row=1, column=1, sticky="ns")
        self.hscroll = ttk.Scrollbar(main_frame, orient="horizontal")
        self.hscroll.grid(row=2, column=0, sticky="ew")

        main_frame.grid_rowconfigure(1, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)

        # Таблицы внутри канвасов
        self.header_table = tk.Frame(self.header_canvas, bg="#ffffff")
        self.header_window = self.header_canvas.create_window((0, 0), window=self.header_table, anchor="nw")

        self.table = tk.Frame(self.main_canvas, bg="#ffffff")
        self.canvas_window = self.main_canvas.create_window((0, 0), window=self.table, anchor="nw")

        # Привязки скролла
        self.main_canvas.configure(yscrollcommand=self.vscroll.set, xscrollcommand=self._on_xscroll_main)
        # Горизонтальный скролл двигает оба канваса
        self.hscroll.configure(command=self._xscroll_both)

        # Обновление области прокрутки
        self.table.bind("<Configure>", self._on_scroll_frame_configure)

        # Создаём шапку в первой строке таблицы
        self._configure_table_columns()
        self._configure_table_columns()   # обновим оба фрейма (см. ниже)
        self._build_header_row(self.header_table)

        # Обработчики колеса мыши
        self.main_canvas.bind("<MouseWheel>", self._on_wheel)
        self.main_canvas.bind("<Shift-MouseWheel>", self._on_shift_wheel)
        self.bind_all("<MouseWheel>", self._on_wheel_anywhere)

        # Коллекция строк
        self.rows: List[RowWidget] = []

        # Нижняя панель
        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=8, pady=(0, 8))

        self.lbl_object_total = tk.Label(
            bottom, text="Сумма: сотрудников 0 | дней 0 | часов 0",
            font=("Segoe UI", 10, "bold")
        )
        self.lbl_object_total.pack(side="left")

        # Пагинация справа
        pag = tk.Frame(bottom)
        pag.pack(side="right")

        ttk.Label(pag, text="На странице:").pack(side="left", padx=(0, 4))
        self.cmb_page_size = ttk.Combobox(pag, state="readonly", width=6,
                                          values=[25, 50, 100])
        self.cmb_page_size.pack(side="left")
        self.cmb_page_size.set(str(self.page_size.get()))
        self.cmb_page_size.bind(
            "<<ComboboxSelected>>",
            lambda e: self._on_page_size_change()
        )

        ttk.Button(pag, text="⟨", width=3, command=lambda: self._render_page(self.current_page - 1)).pack(side="left", padx=4)
        self.lbl_page = ttk.Label(pag, text="Стр. 1 / 1")
        self.lbl_page.pack(side="left")
        ttk.Button(pag, text="⟩", width=3, command=lambda: self._render_page(self.current_page + 1)).pack(side="left", padx=4)

        self._on_department_select()

    def _build_header_row(self, parent):
        hb = self.HEADER_BG
        tk.Label(parent, text="ФИО", bg=hb, anchor="w", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=0, padx=0, pady=(0, 2), sticky="nsew")
        tk.Label(parent, text="Таб.№", bg=hb, anchor="center", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=1, padx=0, pady=(0, 2), sticky="nsew")

        for d in range(1, 32):
            tk.Label(parent, text=str(d), bg=hb, anchor="center", font=("Segoe UI", 9, "bold")).grid(
                row=0, column=1 + d, padx=0, pady=(0, 2), sticky="nsew")

        tk.Label(parent, text="Дней", bg=hb, anchor="e", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=33, padx=(4, 1), pady=(0, 2), sticky="nsew")
        tk.Label(parent, text="Часы", bg=hb, anchor="e", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=34, padx=(4, 1), pady=(0, 2), sticky="nsew")

        tk.Label(parent, text="Пер.день", bg=hb, anchor="e", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=35, padx=(4, 1), pady=(0, 2), sticky="nsew")
        tk.Label(parent, text="Пер.ночь", bg=hb, anchor="e", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=36, padx=(4, 1), pady=(0, 2), sticky="nsew")

        tk.Label(parent, text="5/2", bg=hb, anchor="center", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=37, padx=1, pady=(0, 2), sticky="nsew")
        tk.Label(parent, text="Удалить", bg=hb, anchor="center", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=38, padx=1, pady=(0, 2), sticky="nsew")

    def _on_scroll_frame_configure(self, _=None):
        """
        Вызывается при изменении размеров фрейма с телом таблицы.
        Обновляет область прокрутки и синхронизирует ширину шапки с телом.
        """
        # Область прокрутки для тела
        self.main_canvas.configure(scrollregion=self.main_canvas.bbox("all"))
        try:
            content_bbox = self.main_canvas.bbox("all")
            if content_bbox:
                x1, y1, x2, y2 = content_bbox
                # Область прокрутки по X для шапки
                self.header_canvas.configure(scrollregion=(0, 0, x2, 0))
            # ВАЖНО: делаем ширину header_canvas такой же, как у main_canvas,
            # чтобы grid‑колонки шапки и тела физически совпадали по ширине.
            self.header_canvas.configure(width=self.main_canvas.winfo_width())
        except Exception:
            pass

    def _configure_table_columns(self):
        px = self.COLPX
        # для тела
        for frame in (self.table, self.header_table):
            if not frame:
                continue
            frame.grid_columnconfigure(0, minsize=px['fio'], weight=0)
            frame.grid_columnconfigure(1, minsize=px['tbn'], weight=0)
            for col in range(2, 33):
                frame.grid_columnconfigure(col, minsize=px['day'], weight=0)
            frame.grid_columnconfigure(33, minsize=px['days'], weight=0)
            frame.grid_columnconfigure(34, minsize=px['hours'], weight=0)
            frame.grid_columnconfigure(35, minsize=px['hours'], weight=0)
            frame.grid_columnconfigure(36, minsize=px['hours'], weight=0)
            frame.grid_columnconfigure(37, minsize=px['btn52'], weight=0)
            frame.grid_columnconfigure(38, minsize=px['del'], weight=0)

    def _on_wheel(self, event):
        if self.main_canvas.winfo_exists():
            self.main_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        return "break"

    def _on_wheel_anywhere(self, event):
        try:
            widget = event.widget
            while widget:
                if widget == self.main_canvas or widget == self.table:
                    return self._on_wheel(event)
                widget = widget.master
        except:
            pass
        return None

    def _on_shift_wheel(self, event):
        if self.main_canvas.winfo_exists():
            dx = int(-1 * (event.delta / 120))
            self.main_canvas.xview_scroll(dx, "units")
            try:
                self.header_canvas.xview_scroll(dx, "units")
            except Exception:
                pass
        return "break"

    def _xscroll_both(self, *args):
        try:
            self.main_canvas.xview(*args)
            self.header_canvas.xview(*args)
        except Exception:
            pass

    def _on_xscroll_main(self, first, last):
        try:
            self.hscroll.set(first, last)
            # Двигаем шапку вслед за телом
            self.header_canvas.xview_moveto(first)
        except Exception:
            pass


    def _on_period_change(self):
        self._update_rows_days_enabled()
        self._load_existing_rows()

    def _on_address_change(self, *_):
        addr = self.cmb_address.get().strip()
        ids = sorted(self.addr_to_ids.get(addr, []))
        if ids:
            self.cmb_object_id.config(state="readonly", values=ids)
            if self.cmb_object_id.get() not in ids:
                self.cmb_object_id.set(ids[0])
        else:
            self.cmb_object_id.config(state="normal", values=[])
            self.cmb_object_id.set("")

    def _on_address_select(self, *_):
        self._on_address_change()
        # очищаем модель и UI при смене адреса
        self.model_rows.clear()
        for r in list(self.rows):
            r.destroy()
        self.rows.clear()
        self._regrid_rows()
        self._load_existing_rows()

    def get_year_month(self) -> Tuple[int, int]:
        return int(self.spn_year.get()), self.cmb_month.current() + 1

    def _update_rows_days_enabled(self):
        y, m = self.get_year_month()
        CHUNK = 20
        rows_list = list(self.rows)

        def apply_chunk(idx: int = 0):
            end = min(idx + CHUNK, len(rows_list))
            for j in range(idx, end):
                r = rows_list[j]
                r.set_day_font(self.DAY_ENTRY_FONT)
                r.update_days_enabled(y, m)
            if end < len(rows_list):
                self.after(1, lambda: apply_chunk(end))
            else:
                self._recalc_object_total()

        apply_chunk(0)

    def _regrid_rows(self):
        # Перегрид всех строк под заголовком (начиная с 1)
        for idx, r in enumerate(self.rows, start=1):
            r.regrid_to(idx)
            r.set_day_font(self.DAY_ENTRY_FONT)
        self.after(30, self._on_scroll_frame_configure)
        self._recalc_object_total()

    def _on_page_size_change(self):
        try:
            sz = int(self.cmb_page_size.get())
            if sz not in (25, 50, 100):
                sz = 50
            self.page_size.set(sz)
        except Exception:
            self.page_size.set(50)
        # Перед сменой страницы — сохранить правки из видимых строк в модель
        self._sync_visible_to_model()
        self._render_page(1)

    def _page_count(self) -> int:
        sz = max(1, int(self.page_size.get()))
        n = len(self.model_rows)
        return max(1, math.ceil(n / sz))

    def _update_page_label(self):
        self.lbl_page.config(text=f"Стр. {self.current_page} / {self._page_count()}")

    def _sync_visible_to_model(self):
        """Считывает значения из видимых RowWidget в модель."""
        if not self.rows:
            return
        sz = max(1, int(self.page_size.get()))
        start = (self.current_page - 1) * sz
        for i, roww in enumerate(self.rows):
            idx = start + i
            if 0 <= idx < len(self.model_rows):
                # забираем сырые значения строками (с переработкой)
                vals = []
                for e in roww.day_entries:
                    raw = e.get().strip()
                    vals.append(raw if raw else None)
                self.model_rows[idx]["hours"] = vals

    def _render_page(self, page: Optional[int] = None):
        """Рендерит только текущую страницу из модели."""
        # Сохраняем видимые правки, если не в массовом режиме
        if not getattr(self, "_suspend_sync", False):
            self._sync_visible_to_model()

        # Очистка текущих UI-строк
        for r in list(getattr(self, "rows", [])):
            try:
                r.destroy()
            except Exception:
                pass
        self.rows = []

        total_pages = self._page_count()
        if page is None:
            page = self.current_page
        page = max(1, min(total_pages, page))
        self.current_page = page

        sz = max(1, int(self.page_size.get()))
        start = (page - 1) * sz
        end = min(start + sz, len(self.model_rows))

        y, m = self.get_year_month()
        # Создаём виджеты только для среза
        for i in range(start, end):
            rec = self.model_rows[i]
            row_index = len(self.rows) + 1
            w = RowWidget(self.table, row_index, rec["fio"], rec["tbn"], self.get_year_month, self.delete_row)
            w.set_day_font(self.DAY_ENTRY_FONT)

            # применим формат дней только один раз
            w.update_days_enabled(y, m)

            # подставим значения часов
            hours = rec.get("hours") or [None] * 31
            w.set_hours(hours)
            self.rows.append(w)

        self._regrid_rows()
        self._update_page_label()
        self._recalc_object_total()  # итоги по всей модели, не только по странице

    def _recalc_object_total(self):
        tot_h = 0.0
        tot_d = 0
        tot_ot_day = 0.0
        tot_ot_night = 0.0

        for rec in self.model_rows:
            hours = rec.get("hours") or [None] * 31
            for raw in hours:
                if not raw:
                    continue
                hv = parse_hours_value(raw)
                d_ot, n_ot = parse_overtime(raw)
                if isinstance(hv, (int, float)) and hv > 1e-12:
                    tot_h += float(hv)
                    tot_d += 1
                if isinstance(d_ot, (int, float)):
                    tot_ot_day += float(d_ot)
                if isinstance(n_ot, (int, float)):
                    tot_ot_night += float(n_ot)

        sh = f"{tot_h:.2f}".rstrip("0").rstrip(".")
        sod = f"{tot_ot_day:.2f}".rstrip("0").rstrip(".")
        son = f"{tot_ot_night:.2f}".rstrip("0").rstrip(".")
        cnt = len(self.model_rows)

        self.lbl_object_total.config(
            text=f"Сумма: сотрудников {cnt} | дней {tot_d} | часов {sh} | пер.день {sod} | пер.ночь {son}"
        )

    def add_row(self):
        fio = self.fio_var.get().strip()
        tbn = self.ent_tbn.get().strip()
        if not fio:
            messagebox.showwarning("Объектный табель", "Выберите ФИО.")
            return

        key = (fio.strip().lower(), tbn.strip())
        existing = {(r["fio"].strip().lower(), r["tbn"].strip()) for r in self.model_rows}
        if key in existing:
            if not messagebox.askyesno("Дублирование",
                                       f"Сотрудник уже есть в реестре:\n{fio} (Таб.№ {tbn}).\nДобавить ещё одну строку?"):
                return

        self.model_rows.append({"fio": fio, "tbn": tbn, "hours": [None] * 31})
        self._render_page(self.current_page)

    def add_department_all(self):
        dep_sel = (self.cmb_department.get() or "Все").strip()

        # Подбор списка сотрудников по подразделению
        if dep_sel == "Все":
            candidates = self.employees[:]  # все сотрудники
            if not candidates:
                messagebox.showinfo("Объектный табель", "Справочник сотрудников пуст.")
                return
            if not messagebox.askyesno("Добавить всех", f"Добавить в реестр всех сотрудников ({len(candidates)})?"):
                return
        else:
            candidates = [e for e in self.employees if len(e) > 3 and (e[3] or "").strip() == dep_sel]
            if not candidates:
                messagebox.showinfo("Объектный табель", f"В подразделении «{dep_sel}» нет сотрудников.")
                return

        # Уникальность по (fio.lower, tbn)
        existing = {(r["fio"].strip().lower(), r["tbn"].strip()) for r in self.model_rows}

        # Диалог прогресса и пакетная обработка
        dlg = BatchAddDialog(self, total=len(candidates), title="Добавление сотрудников")

        CHUNK = 50  # крупнее пакет, т.к. мы не создаем виджеты
        added = 0

        def add_chunk(start_idx: int = 0):
            nonlocal added, existing
            if dlg.cancelled:
                finalize()
                return

            end_idx = min(start_idx + CHUNK, len(candidates))
            for i in range(start_idx, end_idx):
                fio, tbn, pos, dep = candidates[i]
                key = (fio.strip().lower(), (tbn or "").strip())
                if key in existing:
                    dlg.step(1)
                    continue
                self.model_rows.append({"fio": fio, "tbn": tbn, "hours": [None] * 31})
                existing.add(key)
                added += 1
                dlg.step(1)

            if end_idx >= len(candidates):
                finalize()
            else:
                self.after(1, lambda: add_chunk(end_idx))

        def finalize():
            try:
                dlg.close()
            except Exception:
                pass
            self._render_page(1)
            messagebox.showinfo("Объектный табель", f"Добавлено сотрудников: {added}")

        add_chunk(0)

    def _on_fio_select(self, *_):
        fio = self.fio_var.get().strip()
        tbn, pos = self.emp_info.get(fio, ("", ""))
        self.ent_tbn.delete(0, "end")
        self.ent_tbn.insert(0, tbn)
        self.pos_var.set(pos)

    def reload_spravochnik(self):
        try:
            cur_dep = (self.cmb_department.get() or "Все").strip()
            cur_addr = (self.cmb_address.get() or "").strip()
            cur_id = (self.cmb_object_id.get() or "").strip()
            cur_fio = (self.fio_var.get() or "").strip()

            self._load_spr_data()

            self.cmb_department.config(values=self.departments)
            if cur_dep in self.departments:
                self.cmb_department.set(cur_dep)
            else:
                try:
                    saved_dep = get_selected_department_from_config()
                    self.cmb_department.set(saved_dep if saved_dep in self.departments else self.departments[0])
                except Exception:
                    self.cmb_department.set(self.departments[0] if self.departments else "Все")

            self.cmb_address.set_completion_list(self.address_options)
            if cur_addr in self.address_options:
                self.cmb_address.set(cur_addr)
            else:
                self.cmb_address.set("")
            self._on_address_change()
            if cur_id and cur_id in (self.cmb_object_id.cget("values") or []):
                self.cmb_object_id.set(cur_id)

            self._on_department_select()
            dep_sel = (self.cmb_department.get() or "Все").strip()
            if dep_sel == "Все":
                allowed = [e[0] for e in self.employees]
            else:
                allowed = [e[0] for e in self.employees if len(e) > 3 and (e[3] or "").strip() == dep_sel]
            seen = set()
            allowed = [n for n in allowed if (n not in seen and not seen.add(n))]
            if cur_fio and cur_fio in allowed:
                self.fio_var.set(cur_fio)
                self._on_fio_select()
            else:
                self.fio_var.set("")
                self.ent_tbn.delete(0, "end")
                self.pos_var.set("")

            messagebox.showinfo("Справочник", "Справочник обновлён.")
        except Exception as e:
            messagebox.showerror("Справочник", f"Ошибка перечтения справочника:\n{e}")

    def fill_52_all(self):
        if not self.model_rows:
            messagebox.showinfo("5/2 всем", "Список сотрудников пуст.")
            return

        y, m = self.get_year_month()
        days = month_days(y, m)

        for rec in self.model_rows:
            hrs = [None] * 31
            for d in range(1, days + 1):
                wd = datetime(y, m, d).weekday()
                if wd < 4:
                    hrs[d - 1] = "8,25"
                elif wd == 4:
                    hrs[d - 1] = "7"
                else:
                    hrs[d - 1] = None
            rec["hours"] = hrs

        # ВАЖНО: перерисовываем без синхронизации видимых значений
        self._suspend_sync = True
        try:
            self._render_page(self.current_page)
        finally:
            self._suspend_sync = False

        messagebox.showinfo("5/2 всем", "Режим 5/2 установлен всем сотрудникам текущего реестра.")

    def fill_hours_all(self):
        if not self.model_rows:
            messagebox.showinfo("Проставить часы", "Список сотрудников пуст.")
            return
        y, m = self.get_year_month()
        max_day = month_days(y, m)
        dlg = HoursFillDialog(self, max_day)
        if not getattr(dlg, "result", None):
            return
        day = dlg.result["day"]
        clear = bool(dlg.result.get("clear", False))
        if day > max_day:
            messagebox.showwarning("Проставить часы", f"В {month_name_ru(m)} {y} только {max_day} дней.")
            return

        if clear:
            for rec in self.model_rows:
                hrs = rec.get("hours") or [None] * 31
                hrs[day - 1] = None
                rec["hours"] = hrs
            self._suspend_sync = True
            try:
                self._render_page(self.current_page)
            finally:
                self._suspend_sync = False
            messagebox.showinfo("Проставить часы", f"День {day} очищен у {len(self.model_rows)} сотрудников.")
            return

        hours_val = float(dlg.result["hours"])
        s = f"{hours_val:.2f}".rstrip("0").rstrip(".").replace(".", ",")
        for rec in self.model_rows:
            hrs = rec.get("hours") or [None] * 31
            hrs[day - 1] = s if hours_val > 1e-12 else None
            rec["hours"] = hrs

        self._suspend_sync = True
        try:
            self._render_page(self.current_page)
        finally:
            self._suspend_sync = False

        messagebox.showinfo("Проставить часы", f"Проставлено {s} ч в день {day} для {len(self.model_rows)} сотрудников.")


    def delete_row(self, roww: RowWidget):
        # Синхронизируем видимые правки
        self._sync_visible_to_model()
        try:
            # Определяем глобальный индекс
            sz = max(1, int(self.page_size.get()))
            start = (self.current_page - 1) * sz
            local_idx = self.rows.index(roww)
            global_idx = start + local_idx
        except Exception:
            global_idx = None

        try:
            roww.destroy()
        except Exception:
            pass
        try:
            self.rows.remove(roww)
        except Exception:
            pass

        if global_idx is not None and 0 <= global_idx < len(self.model_rows):
            del self.model_rows[global_idx]

        # Перерендерим текущую страницу (возможно, перелистнём назад, если страница опустела)
        if self.current_page > self._page_count():
            self.current_page = self._page_count()
        self._render_page(self.current_page)

    def clear_all_rows(self):
        if not self.model_rows:
            return
        if not messagebox.askyesno("Объектный табель", "Очистить все строки?"):
            return
        self.model_rows.clear()
        self._render_page(1)

    # ========== ИСПРАВЛЕННЫЕ МЕТОДЫ ==========

    def _current_file_path(self) -> Optional[Path]:
        """Генерирует путь к файлу с учетом подразделения"""
        addr = self.cmb_address.get().strip()
        oid = self.cmb_object_id.get().strip()
        dep = self.cmb_department.get().strip()
    
        if not addr and not oid:
            return None
    
        y, m = self.get_year_month()
        id_part = oid if oid else safe_filename(addr)
    
        # Добавляем подразделение в имя файла
        dep_part = safe_filename(dep) if dep and dep != "Все" else "ВсеПодразделения"
    
        return self.out_dir / f"Объектный_табель_{id_part}_{dep_part}_{y}_{m:02d}.xlsx"

    def _file_path_for(self, year: int, month: int, addr: Optional[str] = None, 
                   oid: Optional[str] = None, department: Optional[str] = None) -> Optional[Path]:
        """Генерирует путь к файлу для заданных параметров"""
        addr = (addr if addr is not None else self.cmb_address.get().strip())
        oid = (oid if oid is not None else self.cmb_object_id.get().strip())
        dep = (department if department is not None else self.cmb_department.get().strip())
    
        if not addr and not oid:
            return None
    
        id_part = oid if oid else safe_filename(addr)
        dep_part = safe_filename(dep) if dep and dep != "Все" else "ВсеПодразделения"
    
        return self.out_dir / f"Объектный_табель_{id_part}_{dep_part}_{year}_{month:02d}.xlsx"

    def _ensure_sheet(self, wb) -> Any:
        """Проверяет наличие листа 'Табель' с правильной структурой и создает его при необходимости"""
        if "Табель" in wb.sheetnames:
            ws = wb["Табель"]
            hdr_first = str(ws.cell(1, 1).value or "")
            # Проверяем наличие новых столбцов (включая Подразделение)
            if hdr_first == "ID объекта" and ws.max_column >= (7 + 31 + 4):  # +1 для подразделения, +4 для итогов и переработок
                return ws
            # Если структура не совпадает, переименовываем старый лист
            base = "Табель_OLD"
            new_name = base
            i = 1
            while new_name in wb.sheetnames:
                i += 1
                new_name = f"{base}{i}"
            ws.title = new_name

        # Создаем новый лист с правильной структурой
        ws2 = wb.create_sheet("Табель")
        hdr = ["ID объекта", "Адрес", "Месяц", "Год", "ФИО", "Табельный №", "Подразделение"] + \
              [str(i) for i in range(1, 32)] + \
              ["Итого дней", "Итого часов по табелю", "Переработка день", "Переработка ночь"]
        ws2.append(hdr)

        # Настройка ширины столбцов
        ws2.column_dimensions["A"].width = 14  # ID объекта
        ws2.column_dimensions["B"].width = 40  # Адрес
        ws2.column_dimensions["C"].width = 10  # Месяц
        ws2.column_dimensions["D"].width = 8   # Год
        ws2.column_dimensions["E"].width = 28  # ФИО
        ws2.column_dimensions["F"].width = 14  # Табельный №
        ws2.column_dimensions["G"].width = 20  # Подразделение

        # Дни месяца (1-31) - столбцы 8-38
        for i in range(8, 8 + 31):
            ws2.column_dimensions[get_column_letter(i)].width = 6

        # Итоговые столбцы
        ws2.column_dimensions[get_column_letter(39)].width = 10  # Итого дней
        ws2.column_dimensions[get_column_letter(40)].width = 18  # Итого часов по табелю
        ws2.column_dimensions[get_column_letter(41)].width = 14  # Переработка день
        ws2.column_dimensions[get_column_letter(42)].width = 14  # Переработка ночь

        ws2.freeze_panes = "A2"
        return ws2

    def _load_existing_rows(self):
        fpath = self._current_file_path()
        # Очистим модель
        self.model_rows.clear()

        if not fpath or not fpath.exists():
            self._render_page(1)
            return

        try:
            wb = load_workbook(fpath, data_only=True)
            ws = self._ensure_sheet(wb)
            y, m = self.get_year_month()
            addr = self.cmb_address.get().strip()
            oid = self.cmb_object_id.get().strip()
            current_dep = self.cmb_department.get().strip()

            for r in range(2, ws.max_row + 1):
                row_oid = (ws.cell(r, 1).value or "")
                row_addr = (ws.cell(r, 2).value or "")
                row_m = int(ws.cell(r, 3).value or 0)
                row_y = int(ws.cell(r, 4).value or 0)
                fio = (ws.cell(r, 5).value or "")
                tbn = (ws.cell(r, 6).value or "")
                row_department = (ws.cell(r, 7).value or "")

                if row_m != m or row_y != y:
                    continue
                if oid:
                    if row_oid != oid:
                        continue
                else:
                    if row_addr != addr:
                        continue
                if current_dep != "Все" and row_department != current_dep:
                    continue

                hours_raw: List[Optional[str]] = []
                for c in range(8, 8 + 31):
                    v = ws.cell(r, c).value
                    hours_raw.append(str(v) if v else None)

                self.model_rows.append({"fio": fio, "tbn": tbn, "hours": hours_raw})

            self._render_page(1)

        except Exception as e:
            messagebox.showerror("Загрузка", f"Не удалось загрузить существующие строки:\n{e}")
            self._render_page(1)

    def save_all(self):
        fpath = self._current_file_path()
        if not fpath:
            messagebox.showwarning("Сохранение", "Укажите адрес и/или ID объекта, а также период.")
            return

        # Сохраним правки с текущей страницы
        self._sync_visible_to_model()

        addr = self.cmb_address.get().strip()
        oid = self.cmb_object_id.get().strip()
        y, m = self.get_year_month()
        current_dep = self.cmb_department.get().strip()

        try:
            if fpath.exists():
                wb = load_workbook(fpath)
            else:
                fpath.parent.mkdir(parents=True, exist_ok=True)
                wb = Workbook()
                if wb.active:
                    wb.remove(wb.active)

            ws = self._ensure_sheet(wb)

            # Удаляем старые записи ТЕКУЩЕГО подразделения
            to_del = []
            for r in range(2, ws.max_row + 1):
                row_oid = (ws.cell(r, 1).value or "")
                row_addr = (ws.cell(r, 2).value or "")
                row_m = int(ws.cell(r, 3).value or 0)
                row_y = int(ws.cell(r, 4).value or 0)
                row_dep = (ws.cell(r, 7).value or "")

                match_obj = (oid and row_oid == oid) or (not oid and row_addr == addr)
                match_period = (row_m == m and row_y == y)
                match_dep = (current_dep == "Все" or row_dep == current_dep)

                if match_obj and match_period and match_dep:
                    to_del.append(r)

            for r in reversed(to_del):
                ws.delete_rows(r, 1)

            # Записываем модель
            for rec in self.model_rows:
                fio = rec["fio"]
                tbn = rec["tbn"]
                hours_list = rec.get("hours") or [None] * 31

                # Определяем подразделение
                department = current_dep if current_dep != "Все" else ""
                for emp_fio, emp_tbn, emp_pos, emp_dep in self.employees:
                    if emp_fio == fio:
                        if emp_dep:
                            department = emp_dep
                        break

                # Рассчеты итогов из строковых значений
                total_hours = 0.0
                total_days = 0
                total_ot_day = 0.0
                total_ot_night = 0.0

                day_values = []
                for raw in hours_list:
                    if not raw:
                        day_values.append(None)
                        continue
                    hrs = parse_hours_value(raw)
                    d_ot, n_ot = parse_overtime(raw)

                    if isinstance(hrs, (int, float)) and hrs > 1e-12:
                        total_hours += hrs
                        total_days += 1

                    # Нормализуем запись в ячейку (сохраняем исходное, если корректно)
                    cell_str = None
                    try:
                        base = f"{hrs:.2f}".rstrip("0").rstrip(".") if hrs is not None else None
                        if base:
                            if d_ot or n_ot:
                                d_ot_val = d_ot if d_ot else 0
                                n_ot_val = n_ot if n_ot else 0
                                cell_str = f"{base}({d_ot_val:.0f}/{n_ot_val:.0f})"
                                total_ot_day += d_ot_val
                                total_ot_night += n_ot_val
                            else:
                                cell_str = base
                    except Exception:
                        cell_str = str(raw)

                    day_values.append(cell_str)

                row_values = [oid, addr, m, y, fio, tbn, department] + day_values + [
                    total_days if total_days else None,
                    None if abs(total_hours) < 1e-12 else total_hours,
                    None if abs(total_ot_day) < 1e-12 else total_ot_day,
                    None if abs(total_ot_night) < 1e-12 else total_ot_night
                ]
                ws.append(row_values)

            wb.save(fpath)
            messagebox.showinfo("Сохранение", f"Сохранено:\n{fpath}")
        except Exception as e:
            messagebox.showerror("Сохранение", f"Ошибка сохранения:\n{e}")

    def _on_department_select(self):
        """Обработчик смены подразделения"""
        dep_sel = (self.cmb_department.get() or "Все").strip()
        set_selected_department_in_config(dep_sel)
    
        # КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: очищаем реестр и загружаем данные для нового подразделения
        for r in list(self.rows):
            r.destroy()
        self.rows.clear()
    
        # Фильтруем список сотрудников
        if dep_sel == "Все":
            names = [e[0] for e in self.employees]
        else:
            names = [e[0] for e in self.employees if len(e) > 3 and (e[3] or "").strip() == dep_sel]
    
        seen = set()
        filtered = []
        for n in names:
            if n not in seen:
                seen.add(n)
                filtered.append(n)
    
        self.cmb_fio.set_completion_list(filtered)
    
        cur = self.fio_var.get().strip()
        if cur and cur not in filtered:
            self.fio_var.set("")
            self.ent_tbn.delete(0, "end")
            self.pos_var.set("")
    
        # Загружаем сохраненные данные для выбранного подразделения
        self._load_existing_rows()

    def copy_from_month(self):
        """Копирование с учетом подразделения (в модель с пагинацией)"""
        addr = self.cmb_address.get().strip()
        oid = self.cmb_object_id.get().strip()
        current_dep = self.cmb_department.get().strip()

        if not addr and not oid:
            messagebox.showwarning("Копирование", "Укажите адрес и/или ID объекта для назначения.")
            return

        cy, cm = self.get_year_month()
        src_y, src_m = cy, cm - 1
        if src_m < 1:
            src_m = 12
            src_y -= 1

        dlg = CopyFromDialog(self, init_year=src_y, init_month=src_m)
        if not getattr(dlg, "result", None):
            return

        src_y = dlg.result["year"]
        src_m = dlg.result["month"]
        with_hours = dlg.result["with_hours"]
        mode = dlg.result["mode"]

        # Путь к исходному файлу С УЧЕТОМ подразделения
        src_path = self._file_path_for(src_y, src_m, addr=addr, oid=oid, department=current_dep)
        if not src_path or not src_path.exists():
            messagebox.showwarning("Копирование",
                f"Не найден файл источника для подразделения «{current_dep}»:\n{src_path.name if src_path else 'N/A'}")
            return

        try:
            wb = load_workbook(src_path, data_only=True)
            ws = self._ensure_sheet(wb)

            found = []
            for r in range(2, ws.max_row + 1):
                row_oid = (ws.cell(r, 1).value or "")
                row_addr = (ws.cell(r, 2).value or "")
                row_m = int(ws.cell(r, 3).value or 0)
                row_y = int(ws.cell(r, 4).value or 0)
                fio = str(ws.cell(r, 5).value or "").strip()
                tbn = str(ws.cell(r, 6).value or "").strip()
                row_dep = str(ws.cell(r, 7).value or "").strip()

                if row_m != src_m or row_y != src_y:
                    continue
                if oid:
                    if row_oid != oid:
                        continue
                else:
                    if row_addr != addr:
                        continue
                if current_dep != "Все" and row_dep != current_dep:
                    continue

                hrs = None
                if with_hours:
                    hrs = []
                    for c in range(8, 8 + 31):
                        v = ws.cell(r, c).value
                        hrs.append(str(v) if v else None)

                if fio:
                    found.append((fio, tbn, hrs))

            if not found:
                messagebox.showinfo("Копирование",
                    f"В источнике нет сотрудников подразделения «{current_dep}» для выбранного объекта и периода.")
                return

            # Убираем дубликаты
            uniq = {}
            for fio, tbn, hrs in found:
                key = (fio.strip().lower(), tbn.strip())
                if key not in uniq:
                    uniq[key] = (fio, tbn, hrs)
            found = list(uniq.values())

            # Сохраним правки видимой страницы
            self._sync_visible_to_model()

            added = 0
            if mode == "replace":
                self.model_rows.clear()

            existing = {(r["fio"].strip().lower(), r["tbn"].strip()) for r in self.model_rows}
            for fio, tbn, hrs in found:
                key = (fio.strip().lower(), tbn.strip())
                if mode == "merge" and key in existing:
                    continue
                self.model_rows.append({
                    "fio": fio,
                    "tbn": tbn,
                    "hours": hrs if hrs is not None else [None] * 31
                })
                existing.add(key)
                added += 1

            # Перерисовываем без синхронизации, чтобы не затирать модель пустыми Entry
            self._suspend_sync = True
            try:
                self._render_page(1 if mode == "replace" else self.current_page)
            finally:
                self._suspend_sync = False

            messagebox.showinfo("Копирование", f"Добавлено сотрудников: {added}")

        except Exception as e:
            messagebox.showerror("Копирование", f"Ошибка копирования:\n{e}")



    def _content_total_width(self, fio_px: Optional[int] = None) -> int:
        """
        Полная ширина содержимого таблицы в пикселях, с учетом всех колонок:
        ФИО, Таб.№, 31 день, Дней, Часы, Пер.день, Пер.ночь, 5/2, Удалить.
        """
        px = self.COLPX.copy()
        if fio_px is not None:
            px["fio"] = fio_px

        # fio + tbn + 31 * day + days + hours + overtime_day + overtime_night + btn52 + del
        return (
            px["fio"] +
            px["tbn"] +
            31 * px["day"] +
            px["days"] +
            px["hours"] +  # "Часы"
            px["hours"] +  # "Пер.день"
            px["hours"] +  # "Пер.ночь"
            px["btn52"] +
            px["del"]
        )

    def _auto_fit_columns(self):
        """
        Автоматически подгоняет ширину колонки ФИО под текущую ширину окна.
        После изменения ширины колонок синхронизирует шапку с телом.
        """
        try:
            viewport = self.main_canvas.winfo_width()
        except Exception:
            viewport = 0

        # Окно еще не отрисовано – повторим позже
        if viewport <= 1:
            self.after(120, self._auto_fit_columns)
            return

        total = self._content_total_width()
        new_fio = self.COLPX["fio"]

        if total > viewport:
            # Не помещаемся – уменьшаем ФИО
            deficit = total - viewport
            new_fio = max(self.MIN_FIO_PX, self.COLPX["fio"] - deficit)
        elif total < viewport:
            # Есть запас – чуть расширим ФИО до MAX_FIO_PX
            surplus = viewport - total
            new_fio = min(self.MAX_FIO_PX, self.COLPX["fio"] + surplus)

        if int(new_fio) != int(self.COLPX["fio"]):
            self.COLPX["fio"] = int(new_fio)
            self._configure_table_columns()
            self._on_scroll_frame_configure()
        else:
            # Даже если ширина не изменилась, синхронизируем ширину шапки
            try:
                self.header_canvas.configure(width=self.main_canvas.winfo_width())
            except Exception:
                pass

    def _on_window_configure(self, _evt):
        try:
            self.after_cancel(self._fit_job)
        except Exception:
            pass
        self._fit_job = self.after(150, self._auto_fit_columns)

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
        self._menu_meals_planning_index = None
        self._menu_meals_settings_index = None
        self._menu_transport = None
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

        # Автотранспорт
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
            idx = m_transport.index("end")
            self._menu_transport_registry_index = idx + 1 if idx is not None else 0
            m_transport.add_command(
                label="🚘Реестр транспорта",
                command=lambda: self._show_page(
                    "transport_registry",
                    lambda parent: SpecialOrders.create_transport_registry_page(parent),
                ),
            )
        menubar.add_cascade(label="Автотранспорт", menu=m_transport)

        # Питание
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

        # Настройки питания (видны только admin — управляем в _apply_role_visibility)
        self._menu_meals_settings_index = None
        if meals_module and hasattr(meals_module, "create_meals_settings_page"):
            idx = m_meals.index("end")
            self._menu_meals_settings_index = idx + 1 if idx is not None else 0
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

        # Аналитика
        m_analytics = tk.Menu(menubar, tearoff=0)
        m_analytics.add_command(label="Экспорт свода (XLSX/CSV)", command=self.summary_export)
        menubar.add_cascade(label="Аналитика", menu=m_analytics)

        # Инструменты
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

        # Настройки (общие)
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
        tk.Label(
            header,
            text="Выберите раздел в верхнем меню",
            font=("Segoe UI", 10),
            fg="#555",
        ).pack(side="right")

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

    # --- управление пользователем / страницами ---

    def _set_user(self, user: Optional[Dict[str, Any]]):
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
        logging.debug(f"MainApp.on_login_success: {user!r}")
        self._set_user(user)
        self.show_home()

    def _show_page(self, key: str, builder):
        if not self.is_authenticated and key not in ("login",):
            messagebox.showwarning(
                "Доступ ограничен",
                "Для доступа к разделу необходимо войти в систему.",
                parent=self,
            )
            self.show_login()
            return

        for w in self.content.winfo_children():
            try:
                w.destroy()
            except Exception:
                pass

        try:
            page = builder(self.content)
        except Exception as e:
            traceback.print_exc()
            messagebox.showerror("Ошибка", f"Не удалось открыть страницу:\n{e}")
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
        role = (self.current_user or {}).get("role") or "specialist"

        # Питание
        if self._menu_meals is not None:
            try:
                self._menu_meals.entryconfig(0, state="normal")  # Создать
                if self._menu_meals_planning_index is not None:
                    st = "normal" if role in ("admin", "planner") else "disabled"
                    self._menu_meals.entryconfig(self._menu_meals_planning_index, state=st)
                if self._menu_meals_settings_index is not None:
                    st = "normal" if role == "admin" else "disabled"
                    self._menu_meals.entryconfig(self._menu_meals_settings_index, state=st)
            except Exception:
                pass

        # Автотранспорт
        if self._menu_transport is not None:
            try:
                self._menu_transport.entryconfig(0, state="normal")
                if self._menu_transport_planning_index is not None:
                    st = "normal" if role in ("admin", "planner") else "disabled"
                    self._menu_transport.entryconfig(self._menu_transport_planning_index, state=st)
                if self._menu_transport_registry_index is not None:
                    st = "normal" if role in ("admin", "planner", "head") else "disabled"
                    self._menu_transport.entryconfig(self._menu_transport_registry_index, state=st)
            except Exception:
                pass

        # Верхнее "Настройки" только для admin (отключаем для остальных)
        if self._menubar is not None and self._menu_settings_index is not None:
            try:
                state = "normal" if role == "admin" else "disabled"
                self._menubar.entryconfig(self._menu_settings_index, state=state)
            except Exception:
                pass

    # --- Папки / внешние EXE ---

    def open_meals_folder(self):
        try:
            meals_dir = exe_dir() / "Заявки_питание"
            meals_dir.mkdir(parents=True, exist_ok=True)
            os.startfile(meals_dir)
        except Exception as e:
            messagebox.showerror("Папка заявок", f"Не удалось открыть папку:\n{e}")

    def run_meals_exe(self):
        try:
            p = exe_dir() / "meals_module.exe"
            if not p.exists():
                messagebox.showwarning("Заказ питания", "Не найден meals_module.exe рядом с программой.")
                return
            subprocess.Popen([str(p)], shell=False)
        except Exception as e:
            messagebox.showerror("Заказ питания", f"Не удалось запустить модуль:\n{e}")

    def open_orders_folder(self):
        try:
            orders_dir = exe_dir() / "Заявки_спецтехники"
            orders_dir.mkdir(parents=True, exist_ok=True)
            os.startfile(orders_dir)
        except Exception as e:
            messagebox.showerror("Папка заявок", f"Не удалось открыть папку:\n{e}")

    def summary_export(self):
        pwd = simpledialog.askstring("Сводный экспорт", "Введите пароль:", show="*", parent=self)
        if pwd is None:
            return
        if pwd != get_export_password_from_config():
            messagebox.showerror("Сводный экспорт", "Неверный пароль.")
            return

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
                messagebox.showwarning("Конвертер", "Не найден TabelConverter.exe рядом с программой.")
                return
            subprocess.Popen([str(p)], shell=False)
        except Exception as e:
            messagebox.showerror("Конвертер", f"Не удалось запустить конвертер:\n{e}")

logging.debug("Модуль main_app импортирован, готов к запуску.")

if __name__ == "__main__":
    logging.debug("Старт приложения без внешней авторизации (логин-страница внутри MainApp).")
    app = MainApp()
    app.mainloop()
