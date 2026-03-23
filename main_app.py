import os
import re
import sys
import json
import calendar
import subprocess
import traceback
import logging
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse, parse_qs
import hashlib
import os as _os
from io import BytesIO
from datetime import datetime, date
from pathlib import Path
from typing import List, Tuple, Optional, Any, Dict
import base64

# --- ИМПОРТ GUI ---
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
try:
    from PIL import Image, ImageTk
except ImportError:
    Image = ImageTk = None


# --- FALLBACK-ЛОГО ---
TINY_PNG_BASE64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/x8AAusB9Wn2XxwAAAAASUVORK5CYII="
)


# --- НАСТРОЙКИ ЛОГИРОВАНИЯ ---
def exe_dir() -> Path:
    """Определяет директорию запущенного .exe или .py файла."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


LOG_FILE = exe_dir() / "main_app_log.txt"
SETTINGS_FILE = exe_dir() / "settings.dat"

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8",
)
logging.debug("=== main_app запущен ===")


# ================================================================== #
#  Локальное хранилище учётных данных (settings.dat)
# ================================================================== #

def _load_local_settings() -> dict:
    """Читает settings.dat и возвращает словарь."""
    try:
        if SETTINGS_FILE.exists():
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        logging.exception("Ошибка чтения settings.dat")
    return {}


def _save_local_settings(data: dict):
    """Записывает словарь в settings.dat."""
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        logging.exception("Ошибка записи settings.dat")


def _obfuscate(text: str) -> str:
    """
    Простая обфускация (base64). НЕ является криптостойким шифрованием,
    но не хранит пароль в открытом виде в файле.
    """
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _deobfuscate(text: str) -> str:
    try:
        return base64.b64decode(text.encode("ascii")).decode("utf-8")
    except Exception:
        return ""


def load_saved_credentials() -> Tuple[str, str, bool]:
    """
    Возвращает (username, password, remember_me).
    Если данных нет — пустые строки и False.
    """
    cfg = _load_local_settings()
    remember = cfg.get("remember_me", False)
    if not remember:
        return "", "", False
    username = cfg.get("saved_username", "")
    password = _deobfuscate(cfg.get("saved_password_b64", ""))
    return username, password, True


def save_credentials(username: str, password: str, remember: bool):
    """Сохраняет или удаляет учётные данные в settings.dat."""
    cfg = _load_local_settings()
    cfg["remember_me"] = remember
    if remember:
        cfg["saved_username"] = username
        cfg["saved_password_b64"] = _obfuscate(password)
    else:
        cfg.pop("saved_username", None)
        cfg.pop("saved_password_b64", None)
    _save_local_settings(cfg)


# --- ИМПОРТ ВСЕХ МОДУЛЕЙ ПРИЛОЖЕНИЯ ---

BudgetAnalyzer = None
EstimateResourceDecoder = None
_assets_logo = None
_LOGO_BASE64 = None
SpecialOrders = None
meals_module = None
meals_employees_module = None
lodging_module = None
objects = None
Settings = None
timesheet_module = None
analytics_module = None
timesheet_transformer = None
employees_module = None
timesheet_compare = None
meals_reports_module = None
employee_card_module = None
payroll_module = None
brigades_module = None
gpr_module = None
gpr_task_dialog = None
gpr_dictionaries = None
EstimateResourceDecoder = None


def perform_heavy_imports():
    global BudgetAnalyzer, EstimateResourceDecoder, _assets_logo, _LOGO_BASE64, SpecialOrders, \
           meals_module, objects, Settings, timesheet_module, \
           analytics_module, timesheet_transformer, employees_module, \
           timesheet_compare, meals_employees_module, lodging_module, \
           meals_reports_module, employee_card_module, payroll_module, \
           brigades_module, gpr_module, gpr_task_dialog, gpr_dictionaries

    import BudgetAnalyzer
    import estimate_resource_decoder as EstimateResourceDecoder
    import assets_logo as _assets_logo
    _LOGO_BASE64 = getattr(_assets_logo, "LOGO_BASE64", None)
    import SpecialOrders
    import meals_module
    import meals_reports as meals_reports_module
    import objects
    import settings_manager as Settings
    import timesheet_module
    import gpr_module as gpr_module
    import gpr_dictionaries as gpr_dictionaries
    import gpr_task_dialog as gpr_task_dialog
    import analytics_module
    import employees as employees_module
    import timesheet_compare
    import meals_employees as meals_employees_module
    import lodging_module as lodging_module
    import brigades_module as brigades_module
    import payroll_module as payroll_module
    import employee_card as employee_card_module
    try:
        import timesheet_transformer
    except ImportError:
        timesheet_transformer = None


# --- КОНСТАНТЫ И ГЛОБАЛЬНЫЕ НАСТРОЙКИ ---
APP_NAME = "Управление строительством (Главное меню)"
db_connection_pool = None


# ================================================================== #
#  ERP/1C ПАЛИТРА И СТИЛИ
# ================================================================== #

ERP = {
    "bg": "#f2f4f7",
    "panel": "#e9edf2",
    "panel2": "#dde4ec",
    "card": "#ffffff",
    "line": "#cfd7e3",
    "line_dark": "#b8c3d1",
    "text": "#1f2937",
    "muted": "#5f6b7a",
    "soft": "#7b8794",
    "blue": "#2f74c0",
    "blue_dark": "#215b9a",
    "blue_soft": "#e8f1fb",
    "green": "#2f855a",
    "orange": "#c97a20",
    "red": "#c05656",
    "sidebar": "#e6ebf1",
    "sidebar_active": "#d6e4f5",
    "header": "#f8fafc",
    "login_bg": "#eef2f6",
}

FONT_H1 = ("Segoe UI", 15, "bold")
FONT_H2 = ("Segoe UI", 12, "bold")
FONT_H3 = ("Segoe UI", 10, "bold")
FONT_BODY = ("Segoe UI", 9)
FONT_SMALL = ("Segoe UI", 8)
FONT_MONO = ("Consolas", 9)


def setup_ttk_styles(root):
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except Exception:
        pass

    style.configure(
        "ERP.TButton",
        font=("Segoe UI", 9),
        padding=(10, 6),
    )

    style.configure(
        "ERPPrimary.TButton",
        font=("Segoe UI", 9, "bold"),
        padding=(10, 6),
        foreground="white",
        background=ERP["blue"],
        borderwidth=1,
        relief="flat",
    )
    style.map(
        "ERPPrimary.TButton",
        background=[("active", ERP["blue_dark"])],
    )

    style.configure(
        "ERPTool.TButton",
        font=("Segoe UI", 8),
        padding=(8, 4),
        background="#f4f7fa",
    )

    style.configure(
        "ERP.TEntry",
        padding=5,
    )

    style.configure(
        "ERP.Treeview",
        font=("Segoe UI", 9),
        rowheight=24,
        fieldbackground="white",
        background="white",
        foreground=ERP["text"],
        bordercolor=ERP["line"],
        lightcolor=ERP["line"],
        darkcolor=ERP["line"],
    )
    style.configure(
        "ERP.Treeview.Heading",
        font=("Segoe UI", 9, "bold"),
        background=ERP["panel"],
        foreground=ERP["text"],
        relief="flat",
        padding=6,
    )
    style.map(
        "ERP.Treeview.Heading",
        background=[("active", ERP["panel2"])],
    )

    style.configure(
        "ERP.Horizontal.TProgressbar",
        troughcolor="#e5e7eb",
        background=ERP["blue"],
        bordercolor="#d1d5db",
        lightcolor=ERP["blue"],
        darkcolor=ERP["blue"],
    )

    style.configure(
        "TCheckbutton",
        background=ERP["login_bg"],
        font=("Segoe UI", 9),
    )


# --- ГЛАВНЫЕ УТИЛИТЫ ПРИЛОЖЕНИЯ ---

def initialize_db_pool():
    """Создает пул соединений с БД. Вызывается один раз при старте приложения."""
    global db_connection_pool
    if db_connection_pool:
        return
    try:
        provider = Settings.get_db_provider().strip().lower()
        if provider != "postgres":
            raise RuntimeError(f"Ожидался provider=postgres, а в настройках: {provider!r}")
        db_url = Settings.get_database_url().strip()
        if not db_url:
            raise RuntimeError("В настройках не указана строка подключения (DATABASE_URL)")

        url = urlparse(db_url)
        db_connection_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            host=url.hostname or "localhost",
            port=url.port or 5432,
            dbname=url.path.lstrip("/"),
            user=url.username,
            password=url.password,
            sslmode=(parse_qs(url.query).get("sslmode", [Settings.get_db_sslmode()])[0] or "require"),
        )
        logging.info("Пул соединений с БД успешно инициализирован.")
    except Exception as e:
        logging.exception("Критическая ошибка: не удалось создать пул соединений с БД.")
        db_connection_pool = None
        raise e


def close_db_pool():
    """Закрывает все соединения в пуле. Вызывается при выходе из приложения."""
    global db_connection_pool
    if db_connection_pool:
        logging.info("Закрытие пула соединений с БД...")
        db_connection_pool.closeall()
        db_connection_pool = None


def sync_permissions_from_menu_spec():
    from menu_spec import MENU_SPEC, TOP_LEVEL

    rows = []
    for sec in MENU_SPEC:
        for e in sec.entries:
            if e.perm:
                rows.append((e.perm, e.title or e.perm, e.group or "core"))

    for e in TOP_LEVEL:
        if e.perm:
            rows.append((e.perm, e.title or e.perm, e.group or "core"))

    uniq = {}
    for code, title, group in rows:
        uniq[code] = (code, title, group)
    rows = list(uniq.values())

    if not rows:
        return

    conn = None
    try:
        if not db_connection_pool:
            raise RuntimeError("Пул соединений недоступен.")
        conn = db_connection_pool.getconn()
        with conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO public.app_permissions(code, title, group_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (code) DO UPDATE
                      SET title = EXCLUDED.title,
                          group_name = EXCLUDED.group_name
                    """,
                    rows,
                )
    finally:
        if conn and db_connection_pool:
            db_connection_pool.putconn(conn)


# --- АУТЕНТИФИКАЦИЯ ---

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
        return password == stored_hash
    except Exception:
        return False


def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    conn = None
    try:
        if not db_connection_pool:
            raise RuntimeError("Пул соединений недоступен.")
        conn = db_connection_pool.getconn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, username, password_hash, is_active, full_name, role "
                "FROM app_users WHERE username = %s",
                (username,),
            )
            row = cur.fetchone()
            if not row or not row["is_active"] or not _verify_password(password, row["password_hash"]):
                return None
            row.pop("password_hash", None)
            return dict(row)
    finally:
        if conn and db_connection_pool:
            db_connection_pool.putconn(conn)


def load_user_permissions(user_id: int) -> set[str]:
    conn = None
    try:
        if not db_connection_pool:
            raise RuntimeError("Пул соединений недоступен.")
        conn = db_connection_pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT perm_code FROM public.app_user_permissions WHERE user_id = %s",
                (user_id,),
            )
            return {r[0] for r in cur.fetchall()}
    finally:
        if conn and db_connection_pool:
            db_connection_pool.putconn(conn)


# ================================================================== #
#  Быстрая статистика для домашней страницы
# ================================================================== #

def _load_home_stats() -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "employees_count": 0,
        "objects_count": 0,
        "timesheets_month": 0,
        "transport_today": 0,
        "meals_today": 0,
    }
    if not db_connection_pool:
        return stats

    conn = None
    try:
        conn = db_connection_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM employees WHERE NOT is_fired")
            stats["employees_count"] = cur.fetchone()[0]

            cur.execute("SELECT count(*) FROM objects")
            stats["objects_count"] = cur.fetchone()[0]

            now = datetime.now()
            cur.execute(
                "SELECT count(*) FROM timesheet_headers WHERE year=%s AND month=%s",
                (now.year, now.month),
            )
            stats["timesheets_month"] = cur.fetchone()[0]

            cur.execute(
                "SELECT count(*) FROM transport_orders WHERE date=%s",
                (now.date(),),
            )
            stats["transport_today"] = cur.fetchone()[0]

            cur.execute(
                "SELECT count(*) FROM meal_orders WHERE date=%s",
                (now.date(),),
            )
            stats["meals_today"] = cur.fetchone()[0]
    except Exception:
        logging.exception("Ошибка загрузки статистики для главной страницы")
    finally:
        if conn and db_connection_pool:
            db_connection_pool.putconn(conn)
    return stats


def embedded_logo_image(parent, max_w=360, max_h=160):
    b64 = _LOGO_BASE64 or TINY_PNG_BASE64

    if Image and ImageTk:
        try:
            raw = base64.b64decode(b64.strip())
            im = Image.open(BytesIO(raw))
            im.thumbnail((max_w, max_h), Image.LANCZOS)
            return ImageTk.PhotoImage(im, master=parent)
        except Exception as e:
            logging.error(f"Ошибка загрузки логотипа через PIL: {e}")

    try:
        ph = tk.PhotoImage(data=b64.strip(), master=parent)
        w, h = ph.width(), ph.height()
        if w > max_w or h > max_h:
            k = max(w / max_w, h / max_h, 1)
            k = max(1, int(k))
            ph = ph.subsample(k, k)
        return ph
    except Exception as e:
        logging.error(f"Критическая ошибка загрузки логотипа через tkinter: {e}")
        return None


# ================================================================== #
#  ERP ВИДЖЕТЫ
# ================================================================== #

class ERPPanel(tk.Frame):
    def __init__(self, master, bg=None, border=True, **kw):
        super().__init__(
            master,
            bg=bg or ERP["card"],
            highlightbackground=ERP["line"],
            highlightthickness=1 if border else 0,
            bd=0,
            **kw,
        )


class ERPSectionHeader(tk.Frame):
    def __init__(self, master, title: str, toolbar: Optional[list] = None, **kw):
        super().__init__(master, bg=ERP["panel"], height=32, **kw)
        self.pack_propagate(False)

        tk.Label(
            self,
            text=title,
            font=FONT_H3,
            fg=ERP["text"],
            bg=ERP["panel"],
            anchor="w",
        ).pack(side="left", padx=10)

        if toolbar:
            btns = tk.Frame(self, bg=ERP["panel"])
            btns.pack(side="right", padx=6)
            for text, cmd in toolbar:
                ttk.Button(
                    btns,
                    text=text,
                    command=cmd,
                    style="ERPTool.TButton",
                ).pack(side="left", padx=2, pady=3)


class ERPSidebarButton(tk.Frame):
    def __init__(self, master, text: str, command=None, active=False, enabled=True, **kw):
        bg = ERP["sidebar_active"] if active else ERP["sidebar"]
        fg = ERP["text"] if enabled else ERP["soft"]
        super().__init__(
            master,
            bg=bg,
            highlightbackground=ERP["line"],
            highlightthickness=1,
            cursor="hand2" if enabled else "",
            **kw,
        )
        self.command = command
        self.enabled = enabled
        self._normal_bg = bg
        self._hover_bg = "#dbe7f5" if enabled else bg

        self.label = tk.Label(
            self,
            text=text,
            font=FONT_BODY,
            fg=fg,
            bg=bg,
            anchor="w",
            padx=10,
            pady=7,
        )
        self.label.pack(fill="x")

        if enabled:
            self.bind("<Enter>", self._on_enter)
            self.bind("<Leave>", self._on_leave)
            self.bind("<Button-1>", self._on_click)
            self.label.bind("<Enter>", self._on_enter)
            self.label.bind("<Leave>", self._on_leave)
            self.label.bind("<Button-1>", self._on_click)

    def _on_enter(self, _e=None):
        self.configure(bg=self._hover_bg)
        self.label.configure(bg=self._hover_bg)

    def _on_leave(self, _e=None):
        self.configure(bg=self._normal_bg)
        self.label.configure(bg=self._normal_bg)

    def _on_click(self, _e=None):
        if self.command and self.enabled:
            self.command()


class ERPStatBox(tk.Frame):
    def __init__(self, master, title: str, value: Any, accent="#2f74c0", command=None, **kw):
        super().__init__(
            master,
            bg=ERP["card"],
            highlightbackground=ERP["line"],
            highlightthickness=1,
            cursor="hand2" if command else "",
            **kw,
        )
        self.command = command
        self._normal_bg = ERP["card"]
        self._hover_bg = "#f6f9fd"

        top = tk.Frame(self, bg=accent, height=4)
        top.pack(fill="x")

        body = tk.Frame(self, bg=ERP["card"])
        body.pack(fill="both", expand=True, padx=10, pady=10)

        self.lbl_title = tk.Label(
            body,
            text=title,
            font=FONT_SMALL,
            fg=ERP["muted"],
            bg=ERP["card"],
            anchor="w",
        )
        self.lbl_title.pack(fill="x")

        self.lbl_value = tk.Label(
            body,
            text=str(value),
            font=("Segoe UI", 16, "bold"),
            fg=ERP["text"],
            bg=ERP["card"],
            anchor="w",
        )
        self.lbl_value.pack(fill="x", pady=(6, 0))

        if command:
            for w in (self, top, body, self.lbl_title, self.lbl_value):
                w.bind("<Enter>", self._on_enter)
                w.bind("<Leave>", self._on_leave)
                w.bind("<Button-1>", self._on_click)

    def _set_bg(self, color):
        self.configure(bg=color)
        for child in self.winfo_children():
            try:
                child.configure(bg=color)
            except Exception:
                pass
            for sub in child.winfo_children():
                try:
                    sub.configure(bg=color)
                except Exception:
                    pass

    def _on_enter(self, _e=None):
        self._set_bg(self._hover_bg)

    def _on_leave(self, _e=None):
        self._set_bg(self._normal_bg)

    def _on_click(self, _e=None):
        if self.command:
            self.command()


# ================================================================== #
#  HomePage — 1C/ERP стиль
# ================================================================== #

class HomePage(tk.Frame):
    """
    Главная страница в стиле 1С/ERP:
    - слева: разделы и быстрые переходы
    - справа: рабочий стол, показатели, служебная информация
    """

    NAV_ITEMS = [
        ("Главная", "home"),
        ("Создать табель", "timesheet"),
        ("Мои табели", "my_timesheets"),
        ("Заявка на транспорт", "transport"),
        ("Заказ питания", "meals_order"),
        ("Реестр объектов", "objects_registry"),
        ("Проживание", "lodging_registry"),
        ("Карточка сотрудника", "employee_card"),
        ("Реестр табелей", "timesheet_registry"),
        ("Реестр транспорта", "transport_registry"),
        ("Аналитика", "analytics_dashboard"),
    ]

    PAGE_BUILDERS = {
        "timesheet": lambda p, app: timesheet_module.create_timesheet_page(p, app),
        "my_timesheets": lambda p, app: timesheet_module.create_my_timesheets_page(p, app),
        "transport": lambda p, app: SpecialOrders.create_page(p, app),
        "meals_order": lambda p, app: meals_module.create_meals_order_page(p, app),
        "analytics_dashboard": lambda p, app: analytics_module.AnalyticsPage(p, app),
        "objects_registry": lambda p, app: objects.ObjectsRegistryPage(p, app),
        "lodging_registry": lambda p, app: lodging_module.create_lodging_registry_page(p, app),
        "employee_card": lambda p, app: employee_card_module.create_employee_card_page(p, app),
        "timesheet_registry": lambda p, app: timesheet_module.create_timesheet_registry_page(p, app),
        "transport_registry": lambda p, app: SpecialOrders.create_transport_registry_page(p),
    }

    def __init__(self, master, app_ref: "MainApp" = None):
        super().__init__(master, bg=ERP["bg"])
        self._app_ref = app_ref
        self.logo_img = None
        self._build()

    def _build(self):
        root = tk.Frame(self, bg=ERP["bg"])
        root.pack(fill="both", expand=True, padx=8, pady=8)

        root.columnconfigure(1, weight=1)
        root.rowconfigure(0, weight=1)

        # --- ЛЕВАЯ ПАНЕЛЬ ---
        sidebar = ERPPanel(root, bg=ERP["sidebar"], width=250)
        sidebar.grid(row=0, column=0, sticky="nsw", padx=(0, 8))
        sidebar.pack_propagate(False)

        side_head = tk.Frame(sidebar, bg=ERP["panel"])
        side_head.pack(fill="x")

        tk.Label(
            side_head,
            text="Разделы системы",
            font=FONT_H3,
            fg=ERP["text"],
            bg=ERP["panel"],
            anchor="w",
            padx=10,
            pady=8,
        ).pack(fill="x")

        side_body = tk.Frame(sidebar, bg=ERP["sidebar"])
        side_body.pack(fill="both", expand=True, padx=6, pady=6)

        for title, key in self.NAV_ITEMS:
            if key != "home" and not self._has_access(key):
                continue
            btn = ERPSidebarButton(
                side_body,
                text=title,
                active=(key == "home"),
                enabled=True,
                command=(lambda k=key: self._go(k)),
            )
            btn.pack(fill="x", pady=2)

        side_sep = tk.Frame(sidebar, bg=ERP["line"], height=1)
        side_sep.pack(fill="x", padx=6, pady=6)

        user_name = ""
        role = ""
        if self._app_ref and self._app_ref.current_user:
            user_name = self._app_ref.current_user.get("full_name") or self._app_ref.current_user.get("username") or ""
            role = self._app_ref.current_user.get("role") or ""

        user_box = tk.Frame(sidebar, bg=ERP["sidebar"])
        user_box.pack(fill="x", padx=10, pady=(0, 10))

        tk.Label(
            user_box,
            text="Текущий пользователь",
            font=FONT_SMALL,
            fg=ERP["muted"],
            bg=ERP["sidebar"],
            anchor="w",
        ).pack(fill="x")
        tk.Label(
            user_box,
            text=user_name or "-",
            font=FONT_BODY,
            fg=ERP["text"],
            bg=ERP["sidebar"],
            anchor="w",
        ).pack(fill="x", pady=(2, 0))
        tk.Label(
            user_box,
            text=f"Роль: {role or '-'}",
            font=FONT_SMALL,
            fg=ERP["soft"],
            bg=ERP["sidebar"],
            anchor="w",
        ).pack(fill="x", pady=(2, 0))

        # --- ПРАВАЯ РАБОЧАЯ ОБЛАСТЬ ---
        work = tk.Frame(root, bg=ERP["bg"])
        work.grid(row=0, column=1, sticky="nsew")
        work.rowconfigure(3, weight=1)
        work.columnconfigure(0, weight=1)

        # Верхняя информационная полоса
        info = ERPPanel(work, bg=ERP["card"])
        info.grid(row=0, column=0, sticky="ew", pady=(0, 8))

        info_inner = tk.Frame(info, bg=ERP["card"])
        info_inner.pack(fill="x", padx=12, pady=10)

        left = tk.Frame(info_inner, bg=ERP["card"])
        left.pack(side="left", fill="x", expand=True)

        self.logo_img = embedded_logo_image(info_inner, max_w=160, max_h=52)
        if self.logo_img:
            tk.Label(info_inner, image=self.logo_img, bg=ERP["card"]).pack(side="right", padx=(12, 0))

        now = datetime.now()
        wd_map = {
            "Monday": "понедельник",
            "Tuesday": "вторник",
            "Wednesday": "среда",
            "Thursday": "четверг",
            "Friday": "пятница",
            "Saturday": "суббота",
            "Sunday": "воскресенье",
        }

        tk.Label(
            left,
            text="Рабочий стол системы управления строительством",
            font=FONT_H1,
            fg=ERP["text"],
            bg=ERP["card"],
            anchor="w",
        ).pack(fill="x")

        tk.Label(
            left,
            text=f"Текущая дата: {now.strftime('%d.%m.%Y')}  |  {wd_map.get(now.strftime('%A'), '')}",
            font=FONT_BODY,
            fg=ERP["muted"],
            bg=ERP["card"],
            anchor="w",
        ).pack(fill="x", pady=(4, 0))

        # Показатели
        stats = _load_home_stats()

        stats_panel = tk.Frame(work, bg=ERP["bg"])
        stats_panel.grid(row=1, column=0, sticky="ew", pady=(0, 8))

        stats_cfg = [
            ("Сотрудники", stats["employees_count"], ERP["blue"], None),
            ("Объекты", stats["objects_count"], ERP["orange"], "objects_registry"),
            ("Табели за месяц", stats["timesheets_month"], ERP["green"], "my_timesheets"),
            ("Транспорт на сегодня", stats["transport_today"], "#7b61c9", "transport"),
            ("Питание на сегодня", stats["meals_today"], ERP["red"], "meals_order"),
        ]

        for i, (title, value, accent, key) in enumerate(stats_cfg):
            box = ERPStatBox(
                stats_panel,
                title=title,
                value=value,
                accent=accent,
                command=(lambda k=key: self._go(k)) if key else None,
            )
            box.grid(row=0, column=i, sticky="nsew", padx=(0 if i == 0 else 6, 0))
            stats_panel.columnconfigure(i, weight=1)

        # Средняя зона: рабочие области
        center = tk.Frame(work, bg=ERP["bg"])
        center.grid(row=2, column=0, sticky="nsew", pady=(0, 8))
        center.columnconfigure(0, weight=3)
        center.columnconfigure(1, weight=2)

        # Быстрые операции
        quick_wrap = ERPPanel(center, bg=ERP["card"])
        quick_wrap.grid(row=0, column=0, sticky="nsew", padx=(0, 8))

        ERPSectionHeader(quick_wrap, "Быстрые операции").pack(fill="x")

        quick_body = tk.Frame(quick_wrap, bg=ERP["card"])
        quick_body.pack(fill="both", expand=True, padx=8, pady=8)

        operations = [
            ("Создать табель", "timesheet"),
            ("Открыть мои табели", "my_timesheets"),
            ("Создать заявку на транспорт", "transport"),
            ("Создать заявку на питание", "meals_order"),
            ("Открыть реестр объектов", "objects_registry"),
            ("Открыть проживание", "lodging_registry"),
            ("Открыть карточку сотрудника", "employee_card"),
            ("Открыть аналитику", "analytics_dashboard"),
        ]

        visible_ops = [(title, key) for title, key in operations if self._has_access(key)]

        for i, (title, key) in enumerate(visible_ops):
            r = i // 2
            c = i % 2
            btn = ttk.Button(
                quick_body,
                text=title,
                style="ERP.TButton",
                command=lambda k=key: self._go(k),
            )
            btn.grid(row=r, column=c, sticky="ew", padx=4, pady=4)
            quick_body.columnconfigure(c, weight=1)

        # Информация / поручения
        note_wrap = ERPPanel(center, bg=ERP["card"])
        note_wrap.grid(row=0, column=1, sticky="nsew")

        ERPSectionHeader(note_wrap, "Служебная информация").pack(fill="x")

        note_body = tk.Frame(note_wrap, bg=ERP["card"])
        note_body.pack(fill="both", expand=True, padx=10, pady=10)

        lines = [
            "Проверьте полноту заполнения табелей за текущий месяц.",
            "Оформляйте заявки на транспорт и питание заранее.",
            "Для доступа к полному функционалу используйте верхнее меню.",
            "При отсутствии доступа к разделу обратитесь к администратору системы.",
        ]
        for line in lines:
            tk.Label(
                note_body,
                text="• " + line,
                font=FONT_BODY,
                fg=ERP["muted"],
                bg=ERP["card"],
                anchor="w",
                justify="left",
                wraplength=320,
            ).pack(fill="x", pady=3)

        # Нижняя зона: список возможностей
        bottom = ERPPanel(work, bg=ERP["card"])
        bottom.grid(row=3, column=0, sticky="nsew")

        ERPSectionHeader(
            bottom,
            "Доступные разделы",
            toolbar=[
                ("Обновить", self._refresh_home),
                ("Главная", lambda: self._go("home")),
            ],
        ).pack(fill="x")

        table_wrap = tk.Frame(bottom, bg=ERP["card"])
        table_wrap.pack(fill="both", expand=True, padx=8, pady=8)

        columns = ("section", "status", "description")
        tree = ttk.Treeview(table_wrap, columns=columns, show="headings", style="ERP.Treeview")
        tree.heading("section", text="Раздел")
        tree.heading("status", text="Доступ")
        tree.heading("description", text="Описание")
        tree.column("section", width=220, anchor="w")
        tree.column("status", width=90, anchor="center")
        tree.column("description", width=520, anchor="w")

        ysb = ttk.Scrollbar(table_wrap, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=ysb.set)

        tree.pack(side="left", fill="both", expand=True)
        ysb.pack(side="right", fill="y")

        descriptions = {
            "timesheet": "Создание и заполнение объектного табеля.",
            "my_timesheets": "Просмотр и редактирование личных табелей.",
            "transport": "Формирование заявок на транспорт и спецтехнику.",
            "meals_order": "Формирование заявок на питание.",
            "objects_registry": "Работа с объектами строительства.",
            "lodging_registry": "Учет проживания и заселения работников.",
            "employee_card": "Сводная карточка по сотруднику.",
            "timesheet_registry": "Общий реестр табелей.",
            "transport_registry": "Общий реестр транспортных заявок.",
            "analytics_dashboard": "Сводные управленческие показатели.",
        }

        for title, key in self.NAV_ITEMS:
            if key == "home":
                continue
            allowed = self._has_access(key)
            tree.insert(
                "",
                "end",
                values=(
                    title,
                    "Да" if allowed else "Нет",
                    descriptions.get(key, ""),
                ),
            )

        tree.bind("<Double-1>", lambda e: self._on_tree_open(tree))

    def _refresh_home(self):
        self._app_ref.show_home()

    def _on_tree_open(self, tree: ttk.Treeview):
        item = tree.focus()
        if not item:
            return
        values = tree.item(item, "values")
        if not values:
            return
        section_title = values[0]

        mapping = {title: key for title, key in self.NAV_ITEMS}
        key = mapping.get(section_title)
        if key and key != "home" and self._has_access(key):
            self._go(key)

    def _has_access(self, page_key: str, perm: Optional[str] = None) -> bool:
        if page_key == "home":
            return True
        if not self._app_ref:
            return True
        if perm:
            return self._app_ref.has_perm(perm)
        required = self._app_ref._perm_for_key(page_key)
        if not required:
            return True
        return self._app_ref.has_perm(required)

    def _go(self, page_key: str):
        if not self._app_ref:
            return
        if page_key == "home":
            self._app_ref.show_home()
            return
        builder_fn = self.PAGE_BUILDERS.get(page_key)
        if builder_fn:
            app = self._app_ref
            self._app_ref._show_page(
                page_key,
                lambda p, _fn=builder_fn, _app=app: _fn(p, _app),
            )


# ================================================================== #
#  LoginPage — строгий ERP стиль
# ================================================================== #

class LoginPage(tk.Frame):
    """Страница входа в строгом ERP-стиле."""
    def __init__(self, master, app_ref: "MainApp"):
        super().__init__(master, bg=ERP["login_bg"])
        self.app_ref = app_ref
        self._show_pass = False
        self.logo_img = None
        self._build()
        self.bind_all("<Return>", self._on_enter)

    def _build(self):
        center = tk.Frame(self, bg=ERP["login_bg"])
        center.place(relx=0.5, rely=0.5, anchor="center")

        card = ERPPanel(center, bg=ERP["card"], width=440, height=320)
        card.pack()
        card.pack_propagate(False)

        top = tk.Frame(card, bg=ERP["panel"])
        top.pack(fill="x")

        tk.Label(
            top,
            text="Авторизация",
            font=FONT_H2,
            fg=ERP["text"],
            bg=ERP["panel"],
            anchor="w",
            padx=12,
            pady=8,
        ).pack(fill="x")

        body = tk.Frame(card, bg=ERP["card"])
        body.pack(fill="both", expand=True, padx=18, pady=16)

        self.logo_img = embedded_logo_image(body, max_w=180, max_h=56)
        if self.logo_img:
            tk.Label(body, image=self.logo_img, bg=ERP["card"]).grid(
                row=0, column=0, columnspan=3, pady=(0, 8)
            )

        tk.Label(
            body,
            text="Система управления строительством",
            font=FONT_H3,
            fg=ERP["text"],
            bg=ERP["card"],
        ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(0, 12))

        tk.Label(body, text="Логин", font=FONT_BODY, fg=ERP["text"], bg=ERP["card"]).grid(
            row=2, column=0, sticky="w", pady=(0, 4)
        )
        self.ent_login = ttk.Entry(body, width=34, style="ERP.TEntry")
        self.ent_login.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(0, 10), ipady=2)

        tk.Label(body, text="Пароль", font=FONT_BODY, fg=ERP["text"], bg=ERP["card"]).grid(
            row=4, column=0, sticky="w", pady=(0, 4)
        )

        self.ent_pass = ttk.Entry(body, width=30, show="*", style="ERP.TEntry")
        self.ent_pass.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(0, 10), ipady=2)

        self.btn_eye = ttk.Button(
            body,
            text="Показать",
            width=10,
            command=self._toggle_password,
            style="ERP.TButton",
        )
        self.btn_eye.grid(row=5, column=2, sticky="e", padx=(6, 0))

        self.var_remember = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            body,
            text="Запомнить учетные данные",
            variable=self.var_remember,
        ).grid(row=6, column=0, columnspan=3, sticky="w", pady=(0, 14))

        btns = tk.Frame(body, bg=ERP["card"])
        btns.grid(row=7, column=0, columnspan=3, sticky="e")

        ttk.Button(
            btns,
            text="Войти",
            command=self._on_login,
            style="ERPPrimary.TButton",
        ).pack(side="left", padx=(0, 6))

        ttk.Button(
            btns,
            text="Выход",
            command=self._on_exit,
            style="ERP.TButton",
        ).pack(side="left")

        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=1)

        saved_user, saved_pass, remember = load_saved_credentials()
        if remember:
            self.ent_login.insert(0, saved_user)
            self.ent_pass.insert(0, saved_pass)
            self.var_remember.set(True)

        if saved_user:
            self.ent_pass.focus_set()
        else:
            self.ent_login.focus_set()

    def _toggle_password(self):
        self._show_pass = not self._show_pass
        self.ent_pass.configure(show="" if self._show_pass else "*")
        self.btn_eye.configure(text="Скрыть" if self._show_pass else "Показать")

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
            user = authenticate_user(username, password)
        except Exception as e:
            messagebox.showerror("Вход", f"Ошибка при обращении к БД:\n{e}", parent=self)
            return

        if not user:
            messagebox.showerror("Вход", "Неверный логин или пароль.", parent=self)
            return

        save_credentials(username, password, self.var_remember.get())
        self.app_ref.on_login_success(user)

    def _on_exit(self):
        self.app_ref.destroy()


class SplashScreen(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Загрузка...")
        self.overrideredirect(True)

        width = 460
        height = 220

        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x = (screen_width // 2) - (width // 2)
        y = (screen_height // 2) - (height // 2)
        self.geometry(f"{width}x{height}+{x}+{y}")

        self.config(bg=ERP["card"], relief="solid", borderwidth=1)

        container = tk.Frame(self, bg=ERP["card"])
        container.pack(fill="both", expand=True, padx=20, pady=20)

        tk.Label(
            container,
            text="Управление строительством",
            font=FONT_H2,
            fg=ERP["text"],
            bg=ERP["card"],
        ).pack(pady=(24, 10))

        tk.Label(
            container,
            text="Идет запуск системы...",
            font=FONT_BODY,
            fg=ERP["muted"],
            bg=ERP["card"],
        ).pack()

        self.progress = ttk.Progressbar(
            container,
            mode="indeterminate",
            style="ERP.Horizontal.TProgressbar",
        )
        self.progress.pack(pady=20, padx=20, fill="x")
        self.progress.start(10)

        self.status_label = tk.Label(
            container,
            text="Инициализация...",
            font=FONT_SMALL,
            fg=ERP["muted"],
            bg=ERP["card"],
        )
        self.status_label.pack(side="bottom", fill="x", ipady=6)

    def update_status(self, text):
        self.status_label.config(text=text)
        self.update_idletasks()


class MainApp(tk.Tk):
    """Главный класс приложения (каркас)."""
    def __init__(self, current_user: Optional[Dict[str, Any]] = None):
        super().__init__()

        setup_ttk_styles(self)

        self.current_user: Dict[str, Any] = current_user or {}
        self.is_authenticated: bool = bool(current_user)

        self.title(APP_NAME)
        self.geometry("1280x820")
        self.minsize(1080, 700)
        self.configure(bg=ERP["bg"])

        self._pages: Dict[str, tk.Widget] = {}
        self._build_menu()

        # --- Верхняя служебная панель ---
        self.header = tk.Frame(self, bg=ERP["header"])
        self.header.pack(fill="x")

        top_line = tk.Frame(self.header, bg=ERP["panel"], height=1)
        top_line.pack(fill="x", side="top")

        self.header_inner = tk.Frame(self.header, bg=ERP["header"])
        self.header_inner.pack(fill="x", padx=12, pady=8)

        header_left = tk.Frame(self.header_inner, bg=ERP["header"])
        header_left.pack(side="left", fill="x", expand=True)

        self.lbl_header_title = tk.Label(
            header_left,
            text="",
            font=FONT_H1,
            fg=ERP["text"],
            bg=ERP["header"],
        )
        self.lbl_header_title.pack(side="left")

        self.lbl_header_hint = tk.Label(
            header_left,
            text="",
            font=FONT_BODY,
            fg=ERP["muted"],
            bg=ERP["header"],
        )
        self.lbl_header_hint.pack(side="left", padx=(12, 0))

        header_right = tk.Frame(self.header_inner, bg=ERP["header"])
        header_right.pack(side="right")

        self.lbl_user_info = tk.Label(
            header_right,
            text="",
            font=FONT_BODY,
            fg=ERP["muted"],
            bg=ERP["header"],
        )
        self.lbl_user_info.pack(side="left", padx=(0, 10))

        self.btn_logout = ttk.Button(
            header_right,
            text="Выход из системы",
            width=16,
            command=self._on_logout,
            style="ERP.TButton",
        )
        self.btn_logout.pack(side="left")
        self.btn_logout.pack_forget()

        tk.Frame(self, height=1, bg=ERP["line"]).pack(fill="x")

        self.content = tk.Frame(self, bg=ERP["bg"])
        self.content.pack(fill="both", expand=True)

        footer = tk.Frame(self, bg=ERP["panel"])
        footer.pack(fill="x")
        tk.Frame(footer, height=1, bg=ERP["line"]).pack(fill="x", side="top")
        tk.Label(
            footer,
            text="Разработал Алексей Зезюкин, 2025",
            font=FONT_SMALL,
            fg=ERP["muted"],
            bg=ERP["panel"],
        ).pack(side="right", padx=12, pady=4)

        self._set_user(None)
        self.show_login()

    # ------------------------------------------------------------------ #
    #  Выход из аккаунта
    # ------------------------------------------------------------------ #
    def _on_logout(self):
        self.show_login()

    def _perm_for_key(self, key: str) -> Optional[str]:
        from menu_spec import MENU_SPEC
        for sec in MENU_SPEC:
            for e in sec.entries:
                if e.kind == "page" and e.key == key:
                    return e.perm
        return None

    def _build_menu(self):
        self._menubar = tk.Menu(self)
        self.config(menu=self._menubar)

        self._menubar.add_command(label="Главная", command=self.show_home)

        # === Объектный табель ===
        m_ts = tk.Menu(self._menubar, tearoff=0)
        m_ts.add_command(
            label="Создать",
            command=lambda: self._show_page(
                "timesheet",
                lambda p: timesheet_module.create_timesheet_page(p, self),
            ),
        )
        m_ts.add_command(
            label="Мои табели",
            command=lambda: self._show_page(
                "my_timesheets",
                lambda p: timesheet_module.create_my_timesheets_page(p, self),
            ),
        )
        m_ts.add_command(
            label="Бригады",
            command=lambda: self._show_page(
                "brigades",
                lambda p: brigades_module.create_brigades_page(p, self),
            ),
        )
        m_ts.add_command(
            label="Реестр табелей",
            command=lambda: self._show_page(
                "timesheet_registry",
                lambda p: timesheet_module.create_timesheet_registry_page(p, self),
            ),
        )
        m_ts.add_command(
            label="Работники",
            command=lambda: self._show_page(
                "workers",
                lambda p: employees_module.create_workers_page(p, self),
            ),
        )
        m_ts.add_command(
            label="Сравнение с 1С",
            command=lambda: self._show_page(
                "timesheet_compare",
                lambda p: timesheet_compare.create_timesheet_compare_page(p, self),
            ),
        )
        self._menu_timesheets_registry_index = m_ts.index("end")
        self._menubar.add_cascade(label="Объектный табель", menu=m_ts)
        self._menu_timesheets = m_ts

        m_gpr = tk.Menu(self._menubar, tearoff=0)
        m_gpr.add_command(
            label="ГПР (Диаграмма Ганта)",
            command=lambda: self._show_page(
                "gpr",
                lambda p: gpr_module.create_gpr_page(p, self),
            ),
        )
        m_gpr.add_command(
            label="Справочники ГПР",
            command=lambda: self._show_page(
                "gpr_dicts",
                lambda p: gpr_dictionaries.create_gpr_dicts_page(p, self),
            ),
        )
        self._menubar.add_cascade(label="Планирование (ГПР)", menu=m_gpr)
        self._menu_gpr = m_gpr

        # === Автотранспорт ===
        m_transport = tk.Menu(self._menubar, tearoff=0)
        m_transport.add_command(
            label="Создать заявку",
            command=lambda: self._show_page("transport", lambda p: SpecialOrders.create_page(p, self)),
        )
        m_transport.add_command(
            label="Мои заявки",
            command=lambda: self._show_page("my_transport_orders", lambda p: SpecialOrders.create_my_transport_orders_page(p, self)),
        )
        self._menu_transport_planning_index = m_transport.index("end")
        m_transport.add_command(
            label="Планирование",
            command=lambda: self._show_page("planning", lambda p: SpecialOrders.create_planning_page(p)),
        )
        self._menu_transport_registry_index = m_transport.index("end")
        m_transport.add_command(
            label="Реестр",
            command=lambda: self._show_page("transport_registry", lambda p: SpecialOrders.create_transport_registry_page(p)),
        )
        self._menubar.add_cascade(label="Автотранспорт", menu=m_transport)
        self._menu_transport = m_transport

        # === Питание ===
        m_meals = tk.Menu(self._menubar, tearoff=0)
        m_meals.add_command(
            label="Создать заявку",
            command=lambda: self._show_page("meals_order", lambda p: meals_module.create_meals_order_page(p, self)),
        )
        m_meals.add_command(
            label="Мои заявки",
            command=lambda: self._show_page("my_meals_orders", lambda p: meals_module.create_my_meals_orders_page(p, self)),
        )
        m_meals.add_command(
            label="Планирование",
            command=lambda: self._show_page("meals_planning", lambda p: meals_module.create_meals_planning_page(p, self)),
        )
        m_meals.add_command(
            label="Реестр",
            command=lambda: self._show_page("meals_registry", lambda p: meals_module.create_all_meals_orders_page(p, self)),
        )
        m_meals.add_command(
            label="Отчеты",
            command=lambda: self._show_page("meals_reports", lambda p: meals_reports_module.create_meals_reports_page(p, self)),
        )
        m_meals.add_command(
            label="Работники (питание)",
            command=lambda: self._show_page("meals_workers", lambda p: meals_employees_module.create_meals_workers_page(p, self)),
        )
        self._menu_meals_settings_index = m_meals.index("end")
        m_meals.add_command(
            label="Настройки",
            command=lambda: self._show_page("meals_settings", lambda p: meals_module.create_meals_settings_page(p, self.current_user.get("role"))),
        )
        self._menubar.add_cascade(label="Питание", menu=m_meals)
        self._menu_meals = m_meals

        m_lodging = tk.Menu(self._menubar, tearoff=0)
        m_lodging.add_command(
            label="Реестр проживаний",
            command=lambda: self._show_page(
                "lodging_registry",
                lambda p: lodging_module.create_lodging_registry_page(p, self),
            ),
        )
        m_lodging.add_command(
            label="Общежития и комнаты",
            command=lambda: self._show_page(
                "lodging_dorms",
                lambda p: lodging_module.create_dorms_page(p, self),
            ),
        )
        m_lodging.add_command(
            label="Тарифы (цена за сутки)",
            command=lambda: self._show_page(
                "lodging_rates",
                lambda p: lodging_module.create_rates_page(p, self),
            ),
        )
        self._menubar.add_cascade(label="Проживание", menu=m_lodging)
        self._menu_lodging = m_lodging

        # === Объекты ===
        m_objects = tk.Menu(self._menubar, tearoff=0)
        m_objects.add_command(
            label="Создать/Редактировать",
            command=lambda: self._show_page("object_create", lambda p: objects.ObjectCreatePage(p, self)),
        )
        m_objects.add_command(
            label="Реестр",
            command=lambda: self._show_page("objects_registry", lambda p: objects.ObjectsRegistryPage(p, self)),
        )
        self._menubar.add_cascade(label="Объекты", menu=m_objects)
        self._menu_objects = m_objects

        # === Аналитика ===
        m_analytics = tk.Menu(self._menubar, tearoff=0)
        m_analytics.add_command(
            label="Операционная аналитика",
            command=lambda: self._show_page(
                "analytics_dashboard",
                lambda p: analytics_module.AnalyticsPage(p, self),
            ),
        )
        m_analytics.add_command(
            label="Затраты (ФОТ)",
            command=lambda: self._show_page(
                "payroll",
                lambda p: payroll_module.create_payroll_page(p, self),
            ),
        )
        self._menubar.add_cascade(label="Аналитика", menu=m_analytics)
        self._menu_analytics = m_analytics

        m_emp = tk.Menu(self._menubar, tearoff=0)
        m_emp.add_command(
            label="Карточка сотрудника",
            command=lambda: self._show_page(
                "employee_card",
                lambda p: employee_card_module.create_employee_card_page(p, self),
            ),
        )
        self._menubar.add_cascade(label="Сотрудники", menu=m_emp)
        self._menu_employees_card = m_emp

        # === Инструменты и Настройки ===
        m_tools = tk.Menu(self._menubar, tearoff=0)
        if timesheet_transformer and hasattr(timesheet_transformer, "open_converter"):
            m_tools.add_command(
                label="Конвертер табеля (1С)",
                command=lambda: timesheet_transformer.open_converter(self),
            )
        if BudgetAnalyzer and hasattr(BudgetAnalyzer, "create_page"):
            m_tools.add_command(
                label="Анализ смет",
                command=lambda: self._show_page("budget", lambda p: BudgetAnalyzer.create_page(p)),
            )
        if EstimateResourceDecoder and hasattr(EstimateResourceDecoder, "create_page"):
            m_tools.add_command(
                label="Раскрытие ресурсов сметы",
                command=lambda: self._show_page(
                    "estimate_resource_decoder",
                    lambda p: EstimateResourceDecoder.create_page(p),
                ),
            )
        self._menubar.add_cascade(label="Инструменты", menu=m_tools)
        self._menu_tools = m_tools

        self._menu_settings_index = self._menubar.index("end")
        self._menubar.add_command(label="Настройки", command=lambda: Settings.open_settings_window(self))

    # ------------------------------------------------------------------ #
    #  Управление пользователем
    # ------------------------------------------------------------------ #
    def _set_user(self, user: Optional[Dict[str, Any]]):
        self.current_user = user or {}
        self.is_authenticated = bool(user)

        caption = ""
        if user:
            name = user.get("full_name") or user.get("username") or ""
            caption = f" — {name}"
            self.lbl_user_info.config(text=f"Пользователь: {name}")
            self.btn_logout.pack(side="left")
        else:
            self.lbl_user_info.config(text="")
            self.btn_logout.pack_forget()

        self.title(APP_NAME + caption)
        self._apply_permissions_visibility()

    def on_login_success(self, user: Dict[str, Any]):
        logging.debug(f"MainApp.on_login_success: {user!r}")
        try:
            user["permissions"] = load_user_permissions(user["id"])
        except Exception as e:
            logging.exception("Не удалось загрузить права пользователя")
            messagebox.showerror("Права", f"Не удалось загрузить права пользователя:\n{e}")
            return
        self._set_user(user)
        self.show_home()

    def has_perm(self, perm_code: str) -> bool:
        perms = self.current_user.get("permissions")
        return bool(perms and perm_code in perms)

    def show_home(self):
        self._show_page("home", lambda p: HomePage(p, app_ref=self))

    def show_login(self):
        self._set_user(None)
        self._show_page("login", lambda p: LoginPage(p, self))

    def _show_page(self, key: str, builder):
        """Отображает нужную страницу, создавая ее при необходимости."""
        if not self.is_authenticated and key != "login":
            self.show_login()
            return

        required = self._perm_for_key(key)
        if key not in ("login", "home") and required and not self.has_perm(required):
            messagebox.showwarning("Доступ запрещён", "У вас нет прав на этот пункт.")
            self.show_home()
            return

        headers = {
            "home": ("Рабочий стол", "Главная страница системы"),
            "timesheet": ("Объектный табель", ""),
            "my_timesheets": ("Мои табели", ""),
            "timesheet_registry": ("Реестр табелей", ""),
            "brigades": ("Бригады", "Назначение бригадиров по подразделениям"),
            "workers": ("Работники", "Поиск по сотруднику и его объектам"),
            "timesheet_compare": ("Сравнение табелей", "Объектный vs Кадровый (1С)"),
            "gpr": ("ГПР (Диаграмма Ганта)", "План работ по объекту"),
            "transport": ("Заявка на спецтехнику", ""),
            "my_transport_orders": ("Мои заявки на транспорт", ""),
            "planning": ("Планирование транспорта", ""),
            "transport_registry": ("Реестр транспорта", ""),
            "meals_order": ("Заказ питания", ""),
            "my_meals_orders": ("Мои заявки на питание", ""),
            "meals_planning": ("Планирование питания", ""),
            "meals_registry": ("Реестр заявок на питание", ""),
            "meals_reports": ("Отчеты по питанию", "Дневной и месячный свод"),
            "meals_workers": ("Работники (питание)", "История питания по сотруднику"),
            "meals_settings": ("Настройки питания", ""),
            "lodging_registry": ("Проживание", "Реестр заселений/выселений"),
            "lodging_dorms": ("Проживание", "Общежития и комнаты"),
            "lodging_rates": ("Проживание", "Тарифы проживания"),
            "object_create": ("Объекты: Создание/Редактирование", ""),
            "payroll": ("Затраты (ФОТ)", "Загрузка начислений и распределение по объектам"),
            "objects_registry": ("Реестр объектов", ""),
            "employee_card": ("Сотрудники", "Карточка сотрудника"),
            "budget": ("Анализ смет", ""),
            "estimate_resource_decoder": ("Раскрытие ресурсов сметы", "Расшифровка ресурсов"),
            "login": ("Авторизация", "Вход в систему"),
            "analytics_dashboard": ("Операционная аналитика", "Сводные показатели"),
        }
        title, hint = headers.get(key, (key.replace("_", " ").title(), ""))
        self._set_header(title, hint)

        for w in self.content.winfo_children():
            w.destroy()

        try:
            page = builder(self.content)
            page.pack(fill="both", expand=True)
            self._pages[key] = page
        except Exception as e:
            logging.exception(f"Ошибка при открытии страницы '{key}'")
            messagebox.showerror("Ошибка", f"Не удалось открыть страницу '{key}':\n{e}")
            if self.is_authenticated:
                self.show_home()
            else:
                self.show_login()

    def _set_header(self, title: str, hint: str = ""):
        self.lbl_header_title.config(text=title)
        self.lbl_header_hint.config(text=hint or "")

    def _apply_permissions_visibility(self):
        from menu_spec import MENU_SPEC, TOP_LEVEL

        def set_state(menu: tk.Menu, label_text: str, allowed: bool):
            if not menu:
                return
            try:
                idx = menu.index(label_text)
                menu.entryconfig(idx, state="normal" if allowed else "disabled")
            except tk.TclError:
                pass

        menus_by_section = {
            "Объектный табель": getattr(self, "_menu_timesheets", None),
            "Автотранспорт": getattr(self, "_menu_transport", None),
            "Питание": getattr(self, "_menu_meals", None),
            "Проживание": getattr(self, "_menu_lodging", None),
            "Объекты": getattr(self, "_menu_objects", None),
            "Сотрудники": getattr(self, "_menu_employees_card", None),
            "Аналитика": getattr(self, "_menu_analytics", None),
            "Инструменты": getattr(self, "_menu_tools", None),
        }

        for sec in MENU_SPEC:
            menu = menus_by_section.get(sec.label)
            for e in sec.entries:
                if e.kind != "page":
                    continue
                allowed = True if not e.perm else self.has_perm(e.perm)
                set_state(menu, e.label, allowed)

        set_state(self._menubar, "Главная", True)

        for sec in MENU_SPEC:
            any_allowed = any(
                (e.kind == "page") and ((not e.perm) or self.has_perm(e.perm))
                for e in sec.entries
            )
            set_state(self._menubar, sec.label, any_allowed)

        for e in TOP_LEVEL:
            allowed = True if not e.perm else self.has_perm(e.perm)
            set_state(self._menubar, e.label, allowed)

    def destroy(self):
        """Корректное завершение работы приложения."""
        logging.info("Приложение закрывается. Закрываем пул соединений.")
        close_db_pool()
        super().destroy()


# --- ТОЧКА ВХОДА ПРИЛОЖЕНИЯ ---

if __name__ == "__main__":
    root = tk.Tk()
    root.withdraw()

    setup_ttk_styles(root)

    splash = SplashScreen(root)

    def start_application():
        try:
            splash.update_status("Загрузка модулей приложения...")
            perform_heavy_imports()

            splash.update_status("Проверка конфигурации...")
            Settings.ensure_config()

            splash.update_status("Подключение к базе данных...")
            initialize_db_pool()

            sync_permissions_from_menu_spec()

            splash.update_status("Передача настроек в модули...")
            modules_to_init = [
                meals_module,
                meals_reports_module,
                SpecialOrders,
                objects,
                Settings,
                timesheet_module,
                gpr_module,
                gpr_task_dialog,
                gpr_dictionaries,
                analytics_module,
                employees_module,
                timesheet_compare,
                meals_employees_module,
                lodging_module,
                employee_card_module,
                payroll_module,
                brigades_module,
            ]
            for module in modules_to_init:
                if module and hasattr(module, "set_db_pool"):
                    module.set_db_pool(db_connection_pool)

            splash.destroy()
            root.destroy()

            logging.debug("Инициализация успешна. Запускаем главный цикл приложения.")
            app = MainApp()
            app.protocol("WM_DELETE_WINDOW", app.destroy)
            app.mainloop()

        except Exception as e:
            logging.critical("Приложение не может быть запущено из-за ошибки инициализации.", exc_info=True)
            splash.destroy()
            messagebox.showerror(
                "Критическая ошибка",
                f"Не удалось инициализировать приложение.\n\nОшибка: {e}\n\nПроверьте настройки и доступность БД.",
            )
            root.destroy()
            sys.exit(1)

    root.after(100, start_application)
    root.mainloop()
