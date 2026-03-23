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
from typing import List, Tuple, Optional, Any, Dict, Callable
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
    try:
        if SETTINGS_FILE.exists():
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        logging.exception("Ошибка чтения settings.dat")
    return {}


def _save_local_settings(data: dict):
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        logging.exception("Ошибка записи settings.dat")


def _obfuscate(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _deobfuscate(text: str) -> str:
    try:
        return base64.b64decode(text.encode("ascii")).decode("utf-8")
    except Exception:
        return ""


def load_saved_credentials() -> Tuple[str, str, bool]:
    cfg = _load_local_settings()
    remember = cfg.get("remember_me", False)
    if not remember:
        return "", "", False
    username = cfg.get("saved_username", "")
    password = _deobfuscate(cfg.get("saved_password_b64", ""))
    return username, password, True


def save_credentials(username: str, password: str, remember: bool):
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


APP_NAME = "Управление строительством"
db_connection_pool = None


# ================================================================== #
#  ТЕМА
# ================================================================== #

UI = {
    "bg": "#edf1f5",
    "panel": "#f7f9fb",
    "panel2": "#e7edf4",
    "line": "#c9d3df",
    "line_dark": "#b6c1ce",
    "sidebar": "#e3eaf2",
    "sidebar_hover": "#d7e3f1",
    "sidebar_active": "#c9dbef",
    "tab_bg": "#eef3f8",
    "tab_active": "#ffffff",
    "white": "#ffffff",
    "text": "#1f2937",
    "muted": "#5b6776",
    "soft": "#7f8a98",
    "blue": "#2f74c0",
    "blue_dark": "#255f9d",
    "green": "#2f855a",
    "orange": "#c97a20",
    "red": "#c05656",
    "status": "#f8fafc",
    "login_bg": "#e9eef4",
}

FONT_H1 = ("Segoe UI", 12, "bold")
FONT_H2 = ("Segoe UI", 10, "bold")
FONT_BODY = ("Segoe UI", 9)
FONT_SMALL = ("Segoe UI", 8)


def setup_ttk_styles(root):
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except Exception:
        pass

    style.configure(
        "Sidebar.Treeview",
        font=("Segoe UI", 9),
        rowheight=22,
        background=UI["sidebar"],
        fieldbackground=UI["sidebar"],
        foreground=UI["text"],
        bordercolor=UI["line"],
        lightcolor=UI["line"],
        darkcolor=UI["line"],
    )

    style.configure(
        "Sidebar.Treeview.Heading",
        font=("Segoe UI", 9, "bold"),
        background=UI["panel2"],
        foreground=UI["text"],
        relief="flat",
    )

    style.map(
        "Sidebar.Treeview",
        background=[("selected", UI["sidebar_active"])],
        foreground=[("selected", UI["text"])],
    )

    style.configure(
        "App.TButton",
        font=("Segoe UI", 9),
        padding=(8, 5),
    )

    style.configure(
        "Primary.TButton",
        font=("Segoe UI", 9, "bold"),
        padding=(10, 6),
        foreground="white",
        background=UI["blue"],
        borderwidth=1,
    )
    style.map(
        "Primary.TButton",
        background=[("active", UI["blue_dark"])],
    )

    style.configure(
        "App.TEntry",
        padding=5,
    )

    style.configure(
        "Main.TNotebook",
        background=UI["bg"],
        borderwidth=0,
        tabmargins=(0, 0, 0, 0),
    )
    style.configure(
        "Main.TNotebook.Tab",
        font=("Segoe UI", 9),
        padding=(10, 4),
        background=UI["tab_bg"],
        foreground=UI["text"],
        borderwidth=1,
    )
    style.map(
        "Main.TNotebook.Tab",
        background=[("selected", UI["tab_active"]), ("active", "#f8fbff")],
        foreground=[("selected", UI["text"])],
    )

    style.configure(
        "App.Horizontal.TProgressbar",
        troughcolor="#e5e7eb",
        background=UI["blue"],
        bordercolor="#d1d5db",
        lightcolor=UI["blue"],
        darkcolor=UI["blue"],
    )

    style.configure(
        "TCheckbutton",
        background=UI["login_bg"],
        font=("Segoe UI", 9),
    )


# ================================================================== #
#  УТИЛИТЫ
# ================================================================== #

def initialize_db_pool():
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

class CompactInfoBox(tk.Frame):
    def __init__(self, master, title: str, value: str, accent="#2f74c0", **kw):
        super().__init__(
            master,
            bg=UI["white"],
            highlightbackground=UI["line"],
            highlightthickness=1,
            **kw,
        )
        tk.Frame(self, bg=accent, width=4).pack(side="left", fill="y")
        body = tk.Frame(self, bg=UI["white"])
        body.pack(side="left", fill="both", expand=True, padx=8, pady=6)

        tk.Label(
            body,
            text=title,
            font=FONT_SMALL,
            fg=UI["muted"],
            bg=UI["white"],
            anchor="w",
        ).pack(fill="x")
        tk.Label(
            body,
            text=value,
            font=FONT_H2,
            fg=UI["text"],
            bg=UI["white"],
            anchor="w",
        ).pack(fill="x", pady=(2, 0))


# ================================================================== #
#  HOME
# ================================================================== #

class HomePage(tk.Frame):
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
        super().__init__(master, bg=UI["bg"])
        self._app_ref = app_ref
        self._build()

    def _build(self):
        root = tk.Frame(self, bg=UI["bg"])
        root.pack(fill="both", expand=True, padx=8, pady=8)

        top = tk.Frame(root, bg=UI["white"], highlightbackground=UI["line"], highlightthickness=1)
        top.pack(fill="x")

        inner = tk.Frame(top, bg=UI["white"])
        inner.pack(fill="x", padx=10, pady=8)

        name = ""
        if self._app_ref and self._app_ref.current_user:
            name = self._app_ref.current_user.get("full_name") or self._app_ref.current_user.get("username") or ""

        tk.Label(
            inner,
            text=f"Рабочий стол пользователя: {name or '-'}",
            font=FONT_H1,
            fg=UI["text"],
            bg=UI["white"],
            anchor="w",
        ).pack(side="left")

        stats = _load_home_stats()

        stats_wrap = tk.Frame(root, bg=UI["bg"])
        stats_wrap.pack(fill="x", pady=(8, 8))

        items = [
            ("Сотрудники", str(stats["employees_count"]), UI["blue"]),
            ("Объекты", str(stats["objects_count"]), UI["orange"]),
            ("Табели/месяц", str(stats["timesheets_month"]), UI["green"]),
            ("Транспорт/день", str(stats["transport_today"]), "#7c5cc4"),
            ("Питание/день", str(stats["meals_today"]), UI["red"]),
        ]

        for i, (title, value, accent) in enumerate(items):
            box = CompactInfoBox(stats_wrap, title, value, accent=accent)
            box.grid(row=0, column=i, sticky="nsew", padx=(0 if i == 0 else 6, 0))
            stats_wrap.columnconfigure(i, weight=1)

        middle = tk.Frame(root, bg=UI["bg"])
        middle.pack(fill="both", expand=True)

        left = tk.Frame(middle, bg=UI["bg"])
        left.pack(side="left", fill="both", expand=True)

        right = tk.Frame(middle, bg=UI["bg"], width=290)
        right.pack(side="right", fill="y", padx=(8, 0))
        right.pack_propagate(False)

        quick = tk.Frame(left, bg=UI["white"], highlightbackground=UI["line"], highlightthickness=1)
        quick.pack(fill="x")

        tk.Label(
            quick,
            text="Быстрые действия",
            font=FONT_H2,
            fg=UI["text"],
            bg=UI["panel"],
            anchor="w",
            padx=10,
            pady=6,
        ).pack(fill="x")

        quick_body = tk.Frame(quick, bg=UI["white"])
        quick_body.pack(fill="x", padx=8, pady=8)

        actions = [
            ("Создать табель", "timesheet"),
            ("Мои табели", "my_timesheets"),
            ("Заявка на транспорт", "transport"),
            ("Заказ питания", "meals_order"),
            ("Реестр объектов", "objects_registry"),
            ("Проживание", "lodging_registry"),
            ("Карточка сотрудника", "employee_card"),
            ("Аналитика", "analytics_dashboard"),
        ]

        visible = [(t, k) for t, k in actions if self._has_access(k)]

        for i, (title, key) in enumerate(visible):
            btn = ttk.Button(
                quick_body,
                text=title,
                style="App.TButton",
                command=lambda kk=key: self._go(kk),
            )
            btn.grid(row=i // 2, column=i % 2, sticky="ew", padx=4, pady=4)
            quick_body.columnconfigure(i % 2, weight=1)

        side = tk.Frame(right, bg=UI["white"], highlightbackground=UI["line"], highlightthickness=1)
        side.pack(fill="both", expand=True)

        tk.Label(
            side,
            text="Информация",
            font=FONT_H2,
            fg=UI["text"],
            bg=UI["panel"],
            anchor="w",
            padx=10,
            pady=6,
        ).pack(fill="x")

        side_body = tk.Frame(side, bg=UI["white"])
        side_body.pack(fill="both", expand=True, padx=10, pady=10)

        notes = [
            "Используйте вкладки для быстрого переключения между разделами.",
            "Кнопки Назад и Вперёд позволяют вернуться к предыдущим страницам.",
            "Левая панель построена по всем разделам системы.",
            "Настройки доступны только пользователям с соответствующим правом.",
        ]
        for note in notes:
            tk.Label(
                side_body,
                text="• " + note,
                font=FONT_BODY,
                fg=UI["muted"],
                bg=UI["white"],
                anchor="w",
                justify="left",
                wraplength=250,
            ).pack(fill="x", pady=3)

    def _has_access(self, page_key: str, perm: Optional[str] = None) -> bool:
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
        builder_fn = self.PAGE_BUILDERS.get(page_key)
        if builder_fn:
            app = self._app_ref
            self._app_ref.open_page_in_tab(
                page_key,
                lambda p, _fn=builder_fn, _app=app: _fn(p, _app),
            )


# ================================================================== #
#  LOGIN
# ================================================================== #

class LoginPage(tk.Frame):
    def __init__(self, master, app_ref: "MainApp"):
        super().__init__(master, bg=UI["login_bg"])
        self.app_ref = app_ref
        self._show_pass = False
        self.logo_img = None
        self._build()
        self.bind_all("<Return>", self._on_enter)

    def _build(self):
        outer = tk.Frame(self, bg=UI["login_bg"])
        outer.pack(fill="both", expand=True)

        center = tk.Frame(outer, bg=UI["login_bg"])
        center.place(relx=0.5, rely=0.5, anchor="center")

        card = tk.Frame(
            center,
            bg=UI["white"],
            highlightbackground=UI["line"],
            highlightthickness=1,
        )
        card.pack()

        head = tk.Frame(card, bg=UI["panel"])
        head.pack(fill="x")
        tk.Label(
            head,
            text="Авторизация",
            font=FONT_H1,
            fg=UI["text"],
            bg=UI["panel"],
            anchor="w",
            padx=14,
            pady=8,
        ).pack(fill="x")

        body = tk.Frame(card, bg=UI["white"])
        body.pack(fill="both", expand=True, padx=18, pady=16)

        self.logo_img = embedded_logo_image(body, max_w=180, max_h=56)
        if self.logo_img:
            tk.Label(body, image=self.logo_img, bg=UI["white"]).grid(row=0, column=0, columnspan=3, pady=(0, 8))

        tk.Label(
            body,
            text="Управление строительством",
            font=FONT_H1,
            fg=UI["text"],
            bg=UI["white"],
            anchor="w",
        ).grid(row=1, column=0, columnspan=3, sticky="w")

        tk.Label(
            body,
            text="Введите учетные данные для входа в систему",
            font=FONT_BODY,
            fg=UI["muted"],
            bg=UI["white"],
            anchor="w",
        ).grid(row=2, column=0, columnspan=3, sticky="w", pady=(0, 14))

        tk.Label(body, text="Логин", font=FONT_BODY, fg=UI["text"], bg=UI["white"]).grid(
            row=3, column=0, columnspan=3, sticky="w"
        )
        self.ent_login = ttk.Entry(body, width=34, style="App.TEntry")
        self.ent_login.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(4, 10), ipady=2)

        tk.Label(body, text="Пароль", font=FONT_BODY, fg=UI["text"], bg=UI["white"]).grid(
            row=5, column=0, columnspan=3, sticky="w"
        )
        self.ent_pass = ttk.Entry(body, width=28, show="*", style="App.TEntry")
        self.ent_pass.grid(row=6, column=0, columnspan=2, sticky="ew", pady=(4, 10), ipady=2)

        self.btn_eye = ttk.Button(
            body,
            text="Показать",
            width=10,
            style="App.TButton",
            command=self._toggle_password,
        )
        self.btn_eye.grid(row=6, column=2, sticky="e", padx=(6, 0))

        self.var_remember = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            body,
            text="Запомнить учетные данные",
            variable=self.var_remember,
        ).grid(row=7, column=0, columnspan=3, sticky="w", pady=(0, 14))

        sep = tk.Frame(body, bg=UI["line"], height=1)
        sep.grid(row=8, column=0, columnspan=3, sticky="ew", pady=(0, 12))

        btns = tk.Frame(body, bg=UI["white"])
        btns.grid(row=9, column=0, columnspan=3, sticky="e")

        ttk.Button(
            btns,
            text="Войти",
            style="Primary.TButton",
            command=self._on_login,
        ).pack(side="left", padx=(0, 6))

        ttk.Button(
            btns,
            text="Выход",
            style="App.TButton",
            command=self._on_exit,
        ).pack(side="left")

        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=1)
        body.columnconfigure(2, weight=0)

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

        width = 440
        height = 220

        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x = (screen_width // 2) - (width // 2)
        y = (screen_height // 2) - (height // 2)
        self.geometry(f"{width}x{height}+{x}+{y}")

        self.config(bg=UI["white"], relief="solid", borderwidth=1)

        container = tk.Frame(self, bg=UI["white"])
        container.pack(fill="both", expand=True, padx=20, pady=20)

        tk.Label(
            container,
            text="Управление строительством",
            font=FONT_H1,
            fg=UI["text"],
            bg=UI["white"],
        ).pack(pady=(26, 10))

        tk.Label(
            container,
            text="Запуск системы...",
            font=FONT_BODY,
            fg=UI["muted"],
            bg=UI["white"],
        ).pack()

        self.progress = ttk.Progressbar(
            container,
            mode="indeterminate",
            style="App.Horizontal.TProgressbar",
        )
        self.progress.pack(pady=20, padx=20, fill="x")
        self.progress.start(10)

        self.status_label = tk.Label(
            container,
            text="Инициализация...",
            font=FONT_SMALL,
            fg=UI["muted"],
            bg=UI["white"],
        )
        self.status_label.pack(side="bottom", fill="x", ipady=6)

    def update_status(self, text):
        self.status_label.config(text=text)
        self.update_idletasks()


# ================================================================== #
#  MAIN APP
# ================================================================== #

class MainApp(tk.Tk):
    FAVORITES_DEFAULT = [
        "my_timesheets",
        "transport",
        "meals_order",
    ]

    def __init__(self, current_user: Optional[Dict[str, Any]] = None):
        super().__init__()

        setup_ttk_styles(self)

        self.current_user: Dict[str, Any] = current_user or {}
        self.is_authenticated: bool = bool(current_user)

        self.title(APP_NAME)
        self.geometry("1360x840")
        self.minsize(1120, 700)
        self.configure(bg=UI["bg"])

        self._pages: Dict[str, tk.Widget] = {}
        self._tab_frames: Dict[str, tk.Frame] = {}
        self._tab_builders: Dict[str, Callable] = {}
        self._tab_titles: Dict[str, str] = {}
        self._history_back: List[str] = []
        self._history_forward: List[str] = []
        self._current_key: Optional[str] = None
        self._favorite_keys = list(self.FAVORITES_DEFAULT)

        self._build_shell()

        self._set_user(None)
        self.show_login()

    # ------------------------------------------------------------------ #
    #  Каркас
    # ------------------------------------------------------------------ #
    def _build_shell(self):
        self.topbar = tk.Frame(self, bg=UI["panel"], height=34, highlightbackground=UI["line"], highlightthickness=1)
        self.topbar.pack(fill="x")
        self.topbar.pack_propagate(False)

        left_tools = tk.Frame(self.topbar, bg=UI["panel"])
        left_tools.pack(side="left", padx=6)

        self.btn_back = ttk.Button(left_tools, text="← Назад", style="App.TButton", command=self.go_back)
        self.btn_back.pack(side="left", padx=(0, 4), pady=4)

        self.btn_forward = ttk.Button(left_tools, text="Вперёд →", style="App.TButton", command=self.go_forward)
        self.btn_forward.pack(side="left", padx=(0, 4), pady=4)

        self.btn_home = ttk.Button(left_tools, text="Главная", style="App.TButton", command=self.show_home)
        self.btn_home.pack(side="left", padx=(0, 4), pady=4)

        self.btn_refresh = ttk.Button(left_tools, text="Обновить", style="App.TButton", command=self.refresh_current_tab)
        self.btn_refresh.pack(side="left", padx=(0, 4), pady=4)

        self.btn_close_tab = ttk.Button(left_tools, text="Закрыть вкладку", style="App.TButton", command=self.close_current_tab)
        self.btn_close_tab.pack(side="left", padx=(0, 4), pady=4)

        center_info = tk.Frame(self.topbar, bg=UI["panel"])
        center_info.pack(side="left", fill="x", expand=True, padx=10)

        self.lbl_header_title = tk.Label(
            center_info,
            text="",
            font=FONT_H1,
            fg=UI["text"],
            bg=UI["panel"],
            anchor="w",
        )
        self.lbl_header_title.pack(side="left")

        self.lbl_header_hint = tk.Label(
            center_info,
            text="",
            font=FONT_BODY,
            fg=UI["muted"],
            bg=UI["panel"],
            anchor="w",
        )
        self.lbl_header_hint.pack(side="left", padx=(10, 0))

        right_tools = tk.Frame(self.topbar, bg=UI["panel"])
        right_tools.pack(side="right", padx=8)

        self.lbl_user_info = tk.Label(
            right_tools,
            text="",
            font=FONT_BODY,
            fg=UI["muted"],
            bg=UI["panel"],
        )
        self.lbl_user_info.pack(side="left", padx=(0, 8))

        self.btn_logout = ttk.Button(
            right_tools,
            text="Выход",
            style="App.TButton",
            command=self._on_logout,
        )
        self.btn_logout.pack(side="left", pady=4)
        self.btn_logout.pack_forget()

        body = tk.Frame(self, bg=UI["bg"])
        body.pack(fill="both", expand=True)

        self.sidebar = tk.Frame(body, bg=UI["sidebar"], width=270, highlightbackground=UI["line"], highlightthickness=1)
        self.sidebar.pack(side="left", fill="y")
        self.sidebar.pack_propagate(False)

        side_head = tk.Frame(self.sidebar, bg=UI["panel2"], height=32)
        side_head.pack(fill="x")
        side_head.pack_propagate(False)

        tk.Label(
            side_head,
            text="Навигация",
            font=FONT_H2,
            fg=UI["text"],
            bg=UI["panel2"],
            anchor="w",
            padx=10,
        ).pack(fill="both", expand=True)

        tree_wrap = tk.Frame(self.sidebar, bg=UI["sidebar"])
        tree_wrap.pack(fill="both", expand=True, padx=4, pady=4)

        self.nav_tree = ttk.Treeview(
            tree_wrap,
            show="tree",
            selectmode="browse",
            style="Sidebar.Treeview",
        )
        self.nav_tree.pack(side="left", fill="both", expand=True)

        self.nav_scroll = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.nav_tree.yview)
        self.nav_scroll.pack(side="right", fill="y")
        self.nav_tree.configure(yscrollcommand=self.nav_scroll.set)

        self.nav_tree.bind("<Double-1>", self._on_nav_tree_open)
        self.nav_tree.bind("<Return>", self._on_nav_tree_open)
        self.nav_tree.tag_configure("group", font=("Segoe UI", 9, "bold"))
        self.nav_tree.tag_configure("page", font=("Segoe UI", 9))
        self.nav_tree.tag_configure("service", font=("Segoe UI", 9, "bold"))
        self.nav_tree.tag_configure("favorite", font=("Segoe UI", 9, "bold"))

        workspace = tk.Frame(body, bg=UI["bg"])
        workspace.pack(side="left", fill="both", expand=True)

        self.notebook = ttk.Notebook(workspace, style="Main.TNotebook")
        self.notebook.pack(fill="both", expand=True)
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        self.statusbar = tk.Frame(self, bg=UI["status"], height=24, highlightbackground=UI["line"], highlightthickness=1)
        self.statusbar.pack(fill="x")
        self.statusbar.pack_propagate(False)

        self.lbl_status_left = tk.Label(
            self.statusbar,
            text="Готово",
            font=FONT_SMALL,
            fg=UI["muted"],
            bg=UI["status"],
            anchor="w",
        )
        self.lbl_status_left.pack(side="left", padx=8)

        self.lbl_status_right = tk.Label(
            self.statusbar,
            text="",
            font=FONT_SMALL,
            fg=UI["soft"],
            bg=UI["status"],
            anchor="e",
        )
        self.lbl_status_right.pack(side="right", padx=8)

        footer = tk.Frame(self, bg=UI["panel2"], height=20)
        footer.pack(fill="x")
        footer.pack_propagate(False)
        tk.Label(
            footer,
            text="Разработал Алексей Зезюкин, 2025",
            font=FONT_SMALL,
            fg=UI["muted"],
            bg=UI["panel2"],
        ).pack(side="right", padx=8)

    # ------------------------------------------------------------------ #
    #  Menu spec helpers
    # ------------------------------------------------------------------ #
    def _has_access_to_entry(self, entry) -> bool:
        if not self.is_authenticated:
            return False
        if not entry.perm:
            return True
        return self.has_perm(entry.perm)

    def _visible_menu_sections(self):
        from menu_spec import MENU_SPEC
        visible = []
        for sec in MENU_SPEC:
            visible_entries = [
                e for e in sec.entries
                if e.kind == "page" and self._has_access_to_entry(e)
            ]
            if visible_entries:
                visible.append((sec, visible_entries))
        return visible

    def _favorites_titles(self) -> Dict[str, str]:
        from menu_spec import MENU_SPEC
        result = {"home": "Главная"}
        for sec in MENU_SPEC:
            for e in sec.entries:
                if e.kind == "page" and e.key:
                    result[e.key] = e.label
        return result

    def _rebuild_sidebar(self):
        from menu_spec import TOP_LEVEL
    
        if not hasattr(self, "nav_tree"):
            return
    
        for item in self.nav_tree.get_children():
            self.nav_tree.delete(item)
    
        active_key = self._current_key or "home"
        titles_map = self._favorites_titles()
    
        self._nav_actions = {}
    
        # --- Избранное ---
        fav_root = self.nav_tree.insert("", "end", text="★ Избранное", open=True, tags=("favorite",))
        home_id = self.nav_tree.insert(fav_root, "end", text="• Главная", tags=("page",))
        self._nav_actions[home_id] = ("home", "home")
    
        for key in self._favorite_keys:
            if key == "home":
                continue
            required = self._perm_for_key(key)
            if required and not self.has_perm(required):
                continue
            title = titles_map.get(key, key)
            iid = self.nav_tree.insert(fav_root, "end", text=f"• {title}", tags=("page",))
            self._nav_actions[iid] = ("page", key)
    
        # --- Основные разделы ---
        if self.is_authenticated:
            visible_sections = self._visible_menu_sections()
            for sec, entries in visible_sections:
                sec_id = self.nav_tree.insert(
                    "",
                    "end",
                    text=f"📁 {sec.label}",
                    open=False,
                    tags=("group",),
                )
                for entry in entries:
                    if not entry.key:
                        continue
                    iid = self.nav_tree.insert(
                        sec_id,
                        "end",
                        text=f"• {entry.label}",
                        tags=("page",),
                    )
                    self._nav_actions[iid] = ("page", entry.key)
    
            # --- Служебные ---
            settings_allowed = any(
                e.label == "Настройки" and self._has_access_to_entry(e)
                for e in TOP_LEVEL
            )
    
            if settings_allowed or self.is_authenticated:
                srv_id = self.nav_tree.insert(
                    "",
                    "end",
                    text="⚙ Служебные",
                    open=True,
                    tags=("service",),
                )
    
                if settings_allowed:
                    iid = self.nav_tree.insert(
                        srv_id,
                        "end",
                        text="• Настройки",
                        tags=("page",),
                    )
                    self._nav_actions[iid] = ("settings", None)
    
                iid_add = self.nav_tree.insert(
                    srv_id,
                    "end",
                    text="• Добавить текущую в избранное",
                    tags=("page",),
                )
                self._nav_actions[iid_add] = ("fav_add", None)
    
                iid_rem = self.nav_tree.insert(
                    srv_id,
                    "end",
                    text="• Убрать текущую из избранного",
                    tags=("page",),
                )
                self._nav_actions[iid_rem] = ("fav_remove", None)
    
        self._select_tree_item_by_key(active_key)

    def _select_tree_item_by_key(self, key: str):
        if not hasattr(self, "_nav_actions"):
            return
    
        for item_id, action in self._nav_actions.items():
            kind, value = action
            if kind in ("home", "page") and value == key:
                try:
                    self.nav_tree.selection_set(item_id)
                    self.nav_tree.focus(item_id)
                    self._ensure_tree_item_visible(item_id)
                except Exception:
                    pass
                return
    
    
    def _ensure_tree_item_visible(self, item_id: str):
        try:
            parent = self.nav_tree.parent(item_id)
            while parent:
                self.nav_tree.item(parent, open=True)
                parent = self.nav_tree.parent(parent)
            self.nav_tree.see(item_id)
        except Exception:
            pass
    
    
    def _on_nav_tree_open(self, _event=None):
        sel = self.nav_tree.selection()
        if not sel:
            return
    
        item_id = sel[0]
        action = self._nav_actions.get(item_id)
        if not action:
            # если это просто раздел, переключаем раскрытие
            try:
                is_open = self.nav_tree.item(item_id, "open")
                self.nav_tree.item(item_id, open=not is_open)
            except Exception:
                pass
            return
    
        kind, value = action
    
        if kind == "home":
            self.show_home()
        elif kind == "page" and value:
            self._open_known_page(value)
        elif kind == "settings":
            if self.is_authenticated and self.has_perm("page.settings"):
                Settings.open_settings_window(self)
        elif kind == "fav_add":
            self.add_current_to_favorites()
        elif kind == "fav_remove":
            self.remove_current_from_favorites()

    # ------------------------------------------------------------------ #
    #  История
    # ------------------------------------------------------------------ #
    def _push_history(self, key: str):
        if self._current_key and self._current_key != key:
            self._history_back.append(self._current_key)
            if len(self._history_back) > 50:
                self._history_back = self._history_back[-50:]
            self._history_forward.clear()

    def go_back(self):
        if not self._history_back:
            return
        if self._current_key:
            self._history_forward.append(self._current_key)
        key = self._history_back.pop()
        self._activate_tab_by_key(key, add_to_history=False)

    def go_forward(self):
        if not self._history_forward:
            return
        if self._current_key:
            self._history_back.append(self._current_key)
        key = self._history_forward.pop()
        self._activate_tab_by_key(key, add_to_history=False)

    # ------------------------------------------------------------------ #
    #  Пользователь
    # ------------------------------------------------------------------ #
    def _on_logout(self):
        self.show_login()

    def _set_user(self, user: Optional[Dict[str, Any]]):
        self.current_user = user or {}
        self.is_authenticated = bool(user)

        caption = ""
        if user:
            name = user.get("full_name") or user.get("username") or ""
            caption = f" — {name}"
            self.lbl_user_info.config(text=f"Пользователь: {name}")
            self.btn_logout.pack(side="left", pady=4)
        else:
            self.lbl_user_info.config(text="")
            self.btn_logout.pack_forget()

        self.title(APP_NAME + caption)
        self._rebuild_sidebar()

    def on_login_success(self, user: Dict[str, Any]):
        logging.debug(f"MainApp.on_login_success: {user!r}")
        try:
            user["permissions"] = load_user_permissions(user["id"])
        except Exception as e:
            logging.exception("Не удалось загрузить права пользователя")
            messagebox.showerror("Права", f"Не удалось загрузить права пользователя:\n{e}")
            return
    
        # закрываем вкладку логина, если она существует
        login_frame = self._tab_frames.get("login")
        if login_frame:
            try:
                idx = self.notebook.index(login_frame)
                self.notebook.forget(idx)
            except Exception:
                pass
            self._tab_frames.pop("login", None)
            self._tab_builders.pop("login", None)
            self._tab_titles.pop("login", None)
            self._pages.pop("login", None)
    
        self._history_back = [k for k in self._history_back if k != "login"]
        self._history_forward = [k for k in self._history_forward if k != "login"]
        if self._current_key == "login":
            self._current_key = None
    
        self._set_user(user)
        self.show_home()

    def has_perm(self, perm_code: str) -> bool:
        perms = self.current_user.get("permissions")
        return bool(perms and perm_code in perms)

    def _perm_for_key(self, key: str) -> Optional[str]:
        from menu_spec import MENU_SPEC
        for sec in MENU_SPEC:
            for e in sec.entries:
                if e.kind == "page" and e.key == key:
                    return e.perm
        return None

    # ------------------------------------------------------------------ #
    #  Заголовки
    # ------------------------------------------------------------------ #
    def _headers_map(self):
        return {
            "home": ("Главная", "Рабочий стол"),
            "timesheet": ("Создать табель", ""),
            "my_timesheets": ("Мои табели", ""),
            "brigades": ("Бригады", "Назначение бригадиров"),
            "timesheet_registry": ("Реестр табелей", ""),
            "workers": ("Работники", "Поиск по сотрудникам"),
            "timesheet_compare": ("Сравнение с 1С", "Объектный vs 1С"),
            "gpr": ("ГПР", "Диаграмма Ганта"),
            "gpr_dicts": ("Справочники ГПР", ""),
            "transport": ("Создать заявку", "Автотранспорт"),
            "my_transport_orders": ("Мои заявки", "Автотранспорт"),
            "planning": ("Планирование", "Автотранспорт"),
            "transport_registry": ("Реестр", "Автотранспорт"),
            "meals_order": ("Создать заявку", "Питание"),
            "my_meals_orders": ("Мои заявки", "Питание"),
            "meals_planning": ("Планирование", "Питание"),
            "meals_registry": ("Реестр", "Питание"),
            "meals_reports": ("Отчеты", "Питание"),
            "meals_workers": ("Работники (питание)", ""),
            "meals_settings": ("Настройки", "Питание"),
            "lodging_registry": ("Реестр проживаний", ""),
            "lodging_dorms": ("Общежития и комнаты", ""),
            "lodging_rates": ("Тарифы (цена за сутки)", ""),
            "object_create": ("Создать/Редактировать", "Объекты"),
            "objects_registry": ("Реестр", "Объекты"),
            "employee_card": ("Карточка сотрудника", ""),
            "analytics_dashboard": ("Операционная аналитика", ""),
            "payroll": ("Затраты (ФОТ)", ""),
            "budget": ("Анализ смет", ""),
            "estimate_resource_decoder": ("Раскрытие ресурсов сметы", ""),
            "login": ("Вход", "Авторизация"),
        }

    def _make_tab_title(self, key: str) -> str:
        return self._headers_map().get(key, (key.replace("_", " ").title(), ""))[0]

    def _set_header(self, title: str, hint: str = ""):
        self.lbl_header_title.config(text=title)
        self.lbl_header_hint.config(text=hint or "")
        self.lbl_status_right.config(text=hint or "")

    def set_status(self, text: str):
        self.lbl_status_left.config(text=text)

    def _sync_header_for_key(self, key: str):
        title, hint = self._headers_map().get(key, (key.replace("_", " ").title(), ""))
        self._set_header(title, hint)
        self.set_status(f"Открыт раздел: {title}")

    # ------------------------------------------------------------------ #
    #  Вкладки
    # ------------------------------------------------------------------ #
    def _activate_tab_by_key(self, key: str, add_to_history: bool = True):
        frame = self._tab_frames.get(key)
        if not frame:
            return
        if add_to_history:
            self._push_history(key)
        self.notebook.select(frame)
        self._current_key = key
        self._sync_header_for_key(key)
        self._rebuild_sidebar()

    def open_page_in_tab(self, key: str, builder):
        if not self.is_authenticated and key != "login":
            self.show_login()
            return

        required = self._perm_for_key(key)
        if key not in ("login", "home") and required and not self.has_perm(required):
            messagebox.showwarning("Доступ запрещён", "У вас нет прав на этот пункт.")
            return

        if key in self._tab_frames:
            self._activate_tab_by_key(key)
            return

        frame = tk.Frame(self.notebook, bg=UI["bg"])
        self._tab_frames[key] = frame
        self._tab_builders[key] = builder
        self._tab_titles[key] = self._make_tab_title(key)

        try:
            page = builder(frame)
            page.pack(fill="both", expand=True)
            self._pages[key] = page
        except Exception as e:
            logging.exception(f"Ошибка при открытии страницы '{key}'")
            messagebox.showerror("Ошибка", f"Не удалось открыть страницу '{key}':\n{e}")
            self._tab_frames.pop(key, None)
            self._tab_builders.pop(key, None)
            self._tab_titles.pop(key, None)
            return

        self.notebook.add(frame, text=self._tab_titles[key])
        self._activate_tab_by_key(key)

    def refresh_current_tab(self):
        if not self._current_key or self._current_key == "login":
            return
        builder = self._tab_builders.get(self._current_key)
        frame = self._tab_frames.get(self._current_key)
        if not builder or not frame:
            return

        for w in frame.winfo_children():
            w.destroy()

        try:
            page = builder(frame)
            page.pack(fill="both", expand=True)
            self._pages[self._current_key] = page
            self.set_status(f"Раздел '{self._tab_titles.get(self._current_key, self._current_key)}' обновлён")
        except Exception as e:
            logging.exception(f"Ошибка обновления вкладки '{self._current_key}'")
            messagebox.showerror("Ошибка", f"Не удалось обновить вкладку:\n{e}")

    def close_current_tab(self):
        current = self._current_key
        if not current or current in ("home", "login"):
            return

        frame = self._tab_frames.get(current)
        if frame:
            try:
                idx = self.notebook.index(frame)
                self.notebook.forget(idx)
            except Exception:
                pass

        self._tab_frames.pop(current, None)
        self._tab_builders.pop(current, None)
        self._tab_titles.pop(current, None)
        self._pages.pop(current, None)

        if "home" in self._tab_frames:
            self._activate_tab_by_key("home", add_to_history=False)
        elif self._tab_frames:
            first_key = next(iter(self._tab_frames.keys()))
            self._activate_tab_by_key(first_key, add_to_history=False)
        else:
            self.show_home()

    def _on_tab_changed(self, _event=None):
        try:
            selected = self.notebook.select()
            if not selected:
                return
            selected_widget = self.nametowidget(selected)
            for key, frame in self._tab_frames.items():
                if str(frame) == str(selected_widget):
                    self._current_key = key
                    self._sync_header_for_key(key)
                    self._rebuild_sidebar()
                    break
        except Exception:
            logging.exception("Ошибка обработки смены вкладки")

    # ------------------------------------------------------------------ #
    #  Избранное
    # ------------------------------------------------------------------ #
    def add_current_to_favorites(self):
        if not self._current_key or self._current_key in ("login",):
            return
        if self._current_key not in self._favorite_keys:
            self._favorite_keys.append(self._current_key)
            self._rebuild_sidebar()
            self.set_status("Раздел добавлен в избранное")

    def remove_current_from_favorites(self):
        if not self._current_key:
            return
        if self._current_key in self._favorite_keys:
            self._favorite_keys.remove(self._current_key)
            self._rebuild_sidebar()
            self.set_status("Раздел удалён из избранного")

    # ------------------------------------------------------------------ #
    #  Маршрутизация
    # ------------------------------------------------------------------ #
    def _open_known_page(self, key: str):
        mapping = {
            "timesheet": lambda p: timesheet_module.create_timesheet_page(p, self),
            "my_timesheets": lambda p: timesheet_module.create_my_timesheets_page(p, self),
            "brigades": lambda p: brigades_module.create_brigades_page(p, self),
            "timesheet_registry": lambda p: timesheet_module.create_timesheet_registry_page(p, self),
            "workers": lambda p: employees_module.create_workers_page(p, self),
            "timesheet_compare": lambda p: timesheet_compare.create_timesheet_compare_page(p, self),

            "gpr": lambda p: gpr_module.create_gpr_page(p, self),
            "gpr_dicts": lambda p: gpr_dictionaries.create_gpr_dicts_page(p, self),

            "transport": lambda p: SpecialOrders.create_page(p, self),
            "my_transport_orders": lambda p: SpecialOrders.create_my_transport_orders_page(p, self),
            "planning": lambda p: SpecialOrders.create_planning_page(p),
            "transport_registry": lambda p: SpecialOrders.create_transport_registry_page(p),

            "meals_order": lambda p: meals_module.create_meals_order_page(p, self),
            "my_meals_orders": lambda p: meals_module.create_my_meals_orders_page(p, self),
            "meals_planning": lambda p: meals_module.create_meals_planning_page(p, self),
            "meals_registry": lambda p: meals_module.create_all_meals_orders_page(p, self),
            "meals_reports": lambda p: meals_reports_module.create_meals_reports_page(p, self),
            "meals_workers": lambda p: meals_employees_module.create_meals_workers_page(p, self),
            "meals_settings": lambda p: meals_module.create_meals_settings_page(p, self.current_user.get("role")),

            "lodging_registry": lambda p: lodging_module.create_lodging_registry_page(p, self),
            "lodging_dorms": lambda p: lodging_module.create_dorms_page(p, self),
            "lodging_rates": lambda p: lodging_module.create_rates_page(p, self),

            "object_create": lambda p: objects.ObjectCreatePage(p, self),
            "objects_registry": lambda p: objects.ObjectsRegistryPage(p, self),

            "employee_card": lambda p: employee_card_module.create_employee_card_page(p, self),

            "analytics_dashboard": lambda p: analytics_module.AnalyticsPage(p, self),
            "payroll": lambda p: payroll_module.create_payroll_page(p, self),

            "budget": lambda p: BudgetAnalyzer.create_page(p),
            "estimate_resource_decoder": lambda p: EstimateResourceDecoder.create_page(p),
        }

        builder = mapping.get(key)
        if not builder:
            messagebox.showinfo("Раздел", f"Для раздела '{key}' не найден обработчик.")
            return

        self.open_page_in_tab(key, builder)

    def show_home(self):
        if not self.is_authenticated:
            self.show_login()
            return
        self.open_page_in_tab("home", lambda p: HomePage(p, app_ref=self))

    def show_login(self):
        self.current_user = {}
        self.is_authenticated = False

        for _, frame in list(self._tab_frames.items()):
            try:
                idx = self.notebook.index(frame)
                self.notebook.forget(idx)
            except Exception:
                pass

        self._pages.clear()
        self._tab_frames.clear()
        self._tab_builders.clear()
        self._tab_titles.clear()
        self._history_back.clear()
        self._history_forward.clear()
        self._current_key = None

        self._set_header("Вход", "Авторизация")
        self.set_status("Ожидание авторизации")
        self._set_user(None)
        self.open_page_in_tab("login", lambda p: LoginPage(p, self))

    def _show_page(self, key: str, builder):
        self.open_page_in_tab(key, builder)

    def destroy(self):
        logging.info("Приложение закрывается. Закрываем пул соединений.")
        close_db_pool()
        super().destroy()


# --- ТОЧКА ВХОДА ---

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
