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
# 1x1 png base64, чтобы приложение не падало, если внешний logo не загружен
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
#  ТЕМА / СТИЛИ
# ================================================================== #

COLORS = {
    "bg_app": "#eef2f7",
    "bg_page": "#f5f7fb",
    "bg_card": "#ffffff",
    "bg_card_soft": "#f8fafc",
    "bg_header": "#ffffff",
    "bg_dark": "#0f172a",
    "bg_dark_2": "#172554",
    "text": "#0f172a",
    "text_muted": "#64748b",
    "text_soft": "#94a3b8",
    "line": "#e2e8f0",
    "line_dark": "#cbd5e1",
    "primary": "#2563eb",
    "primary_hover": "#1d4ed8",
    "success": "#16a34a",
    "warning": "#d97706",
    "danger": "#e11d48",
    "violet": "#7c3aed",
    "cyan": "#0891b2",
}

FONT_H1 = ("Segoe UI", 20, "bold")
FONT_H2 = ("Segoe UI", 14, "bold")
FONT_H3 = ("Segoe UI", 11, "bold")
FONT_BODY = ("Segoe UI", 10)
FONT_SMALL = ("Segoe UI", 9)
FONT_TINY = ("Segoe UI", 8)


def setup_ttk_styles(root):
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except Exception:
        pass

    style.configure(
        "App.TButton",
        font=("Segoe UI", 10),
        padding=(10, 7),
    )
    style.configure(
        "Primary.TButton",
        font=("Segoe UI", 10, "bold"),
        padding=(12, 8),
        foreground="#ffffff",
        background=COLORS["primary"],
        borderwidth=0,
    )
    style.map(
        "Primary.TButton",
        background=[("active", COLORS["primary_hover"])],
        foreground=[("disabled", "#dbeafe")],
    )

    style.configure(
        "Soft.TButton",
        font=("Segoe UI", 10),
        padding=(10, 8),
        foreground=COLORS["text"],
        background="#e8eefc",
        borderwidth=0,
    )
    style.map(
        "Soft.TButton",
        background=[("active", "#dbe7ff")],
    )

    style.configure(
        "App.TEntry",
        padding=6,
    )

    style.configure(
        "TCheckbutton",
        background=COLORS["bg_page"],
        font=("Segoe UI", 9),
    )

    style.configure(
        "App.Horizontal.TProgressbar",
        troughcolor="#e5e7eb",
        background=COLORS["primary"],
        bordercolor="#e5e7eb",
        lightcolor=COLORS["primary"],
        darkcolor=COLORS["primary"],
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
    """
    Загружает краткую сводку для главной страницы.
    Возвращает словарь с ключами:
      employees_count, objects_count, timesheets_month,
      transport_today, meals_today
    """
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


# --- ГРАФИЧЕСКИЙ ИНТЕРФЕЙС ---

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
#  UI-виджеты
# ================================================================== #

class Card(tk.Frame):
    """Универсальная карточка."""
    def __init__(self, master, bg=None, border="#e2e8f0", pad=1, radius_like=False, **kw):
        outer_bg = "#dfe5ef"
        super().__init__(master, bg=outer_bg, **kw)
        self.inner = tk.Frame(self, bg=bg or COLORS["bg_card"])
        self.inner.pack(fill="both", expand=True, padx=pad, pady=pad)
        self._bg = bg or COLORS["bg_card"]
        self._border = border

    @property
    def body(self):
        return self.inner


class SectionTitle(tk.Frame):
    def __init__(self, master, title: str, subtitle: str = "", **kw):
        super().__init__(master, bg=master.cget("bg"), **kw)
        tk.Label(
            self,
            text=title,
            font=FONT_H2,
            fg=COLORS["text"],
            bg=self.cget("bg"),
            anchor="w",
        ).pack(fill="x")
        if subtitle:
            tk.Label(
                self,
                text=subtitle,
                font=FONT_SMALL,
                fg=COLORS["text_muted"],
                bg=self.cget("bg"),
                anchor="w",
            ).pack(fill="x", pady=(2, 0))


class HeroBanner(tk.Frame):
    def __init__(self, master, app_ref: "MainApp", **kw):
        super().__init__(master, bg=COLORS["bg_page"], **kw)
        self.app_ref = app_ref
        self.logo_img = None
        self._build()

    def _build(self):
        card = Card(self, bg=COLORS["bg_dark"], pad=1)
        card.pack(fill="x")
        body = card.body
        body.configure(bg=COLORS["bg_dark"])

        content = tk.Frame(body, bg=COLORS["bg_dark"])
        content.pack(fill="x", padx=26, pady=24)

        left = tk.Frame(content, bg=COLORS["bg_dark"])
        left.pack(side="left", fill="both", expand=True)

        right = tk.Frame(content, bg=COLORS["bg_dark"])
        right.pack(side="right", anchor="ne", padx=(20, 0))

        self.logo_img = embedded_logo_image(right, max_w=180, max_h=70)
        if self.logo_img:
            tk.Label(right, image=self.logo_img, bg=COLORS["bg_dark"]).pack(anchor="e", pady=(0, 8))

        name = ""
        role = ""
        if self.app_ref and self.app_ref.current_user:
            name = self.app_ref.current_user.get("full_name") or self.app_ref.current_user.get("username") or ""
            role = self.app_ref.current_user.get("role") or ""

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
        wd = wd_map.get(now.strftime("%A"), "")

        tk.Label(
            left,
            text=f"Здравствуйте, {name}!" if name else "Здравствуйте!",
            font=FONT_H1,
            fg="#ffffff",
            bg=COLORS["bg_dark"],
            anchor="w",
        ).pack(fill="x")

        tk.Label(
            left,
            text=f"Сегодня {now.strftime('%d.%m.%Y')} • {wd}",
            font=FONT_BODY,
            fg="#cbd5e1",
            bg=COLORS["bg_dark"],
            anchor="w",
        ).pack(fill="x", pady=(6, 0))

        role_text = f"Роль: {role}" if role else "Авторизованный пользователь"
        tk.Label(
            left,
            text=role_text,
            font=FONT_SMALL,
            fg="#93c5fd",
            bg=COLORS["bg_dark"],
            anchor="w",
        ).pack(fill="x", pady=(10, 0))

        info_bar = tk.Frame(left, bg=COLORS["bg_dark"])
        info_bar.pack(fill="x", pady=(16, 0))

        badge1 = tk.Label(
            info_bar,
            text="● Система готова к работе",
            font=FONT_SMALL,
            fg="#86efac",
            bg=COLORS["bg_dark"],
            padx=10,
            pady=5,
        )
        badge1.pack(side="left")

        badge2 = tk.Label(
            info_bar,
            text="Единое окно управления строительством",
            font=FONT_SMALL,
            fg="#e2e8f0",
            bg="#1e293b",
            padx=10,
            pady=5,
        )
        badge2.pack(side="left", padx=(8, 0))


class MetricTile(tk.Frame):
    def __init__(self, master, value, label: str, accent="#2563EB", on_click=None, **kw):
        super().__init__(master, bg=COLORS["bg_page"], **kw)
        self._on_click = on_click
        self._accent = accent

        self.card = Card(self, bg=COLORS["bg_card"], pad=1)
        self.card.pack(fill="both", expand=True)

        body = self.card.body
        body.configure(bg=COLORS["bg_card"], cursor="hand2" if on_click else "")

        top = tk.Frame(body, bg=COLORS["bg_card"], height=4)
        top.pack(fill="x")
        tk.Frame(top, bg=accent, height=4).pack(fill="x")

        wrap = tk.Frame(body, bg=COLORS["bg_card"])
        wrap.pack(fill="both", expand=True, padx=16, pady=14)

        tk.Label(
            wrap,
            text=str(value),
            font=("Segoe UI", 22, "bold"),
            fg=COLORS["text"],
            bg=COLORS["bg_card"],
            anchor="w",
        ).pack(fill="x")

        tk.Label(
            wrap,
            text=label,
            font=FONT_SMALL,
            fg=COLORS["text_muted"],
            bg=COLORS["bg_card"],
            anchor="w",
        ).pack(fill="x", pady=(3, 0))

        if on_click:
            self._bind_recursive(body)

    def _bind_recursive(self, widget):
        widget.bind("<Button-1>", lambda e: self._on_click() if self._on_click else None)
        widget.bind("<Enter>", self._on_enter)
        widget.bind("<Leave>", self._on_leave)
        for child in widget.winfo_children():
            self._bind_recursive(child)

    def _on_enter(self, _e=None):
        self.card.configure(bg="#cfd8e8")

    def _on_leave(self, _e=None):
        self.card.configure(bg="#dfe5ef")


class ActionCard(tk.Frame):
    def __init__(self, master, icon: str, title: str, subtitle: str, command=None, enabled: bool = True, **kw):
        super().__init__(master, bg=COLORS["bg_page"], **kw)
        self._enabled = enabled
        self._command = command

        self.normal_card = "#dfe5ef"
        self.hover_card = "#c7d7ff"

        self.card = Card(self, bg=COLORS["bg_card"] if enabled else "#f3f4f6", pad=1)
        self.card.pack(fill="both", expand=True)

        body = self.card.body
        body.configure(
            bg=COLORS["bg_card"] if enabled else "#f3f4f6",
            cursor="hand2" if enabled else "",
        )

        fg_icon = COLORS["primary"] if enabled else "#cbd5e1"
        fg_title = COLORS["text"] if enabled else "#94a3b8"
        fg_sub = COLORS["text_muted"] if enabled else "#cbd5e1"

        wrap = tk.Frame(body, bg=body.cget("bg"))
        wrap.pack(fill="both", expand=True, padx=16, pady=14)

        tk.Label(
            wrap,
            text=icon,
            font=("Segoe UI Emoji", 28),
            bg=body.cget("bg"),
            fg=fg_icon,
            anchor="w",
        ).pack(anchor="w", pady=(0, 8))

        tk.Label(
            wrap,
            text=title,
            font=FONT_H3,
            bg=body.cget("bg"),
            fg=fg_title,
            anchor="w",
        ).pack(fill="x")

        tk.Label(
            wrap,
            text=subtitle,
            font=FONT_SMALL,
            bg=body.cget("bg"),
            fg=fg_sub,
            anchor="w",
            justify="left",
            wraplength=230,
        ).pack(fill="x", pady=(4, 0))

        bottom = tk.Frame(wrap, bg=body.cget("bg"))
        bottom.pack(fill="x", pady=(10, 0))

        tk.Label(
            bottom,
            text="Открыть →" if enabled else "Недоступно",
            font=FONT_SMALL,
            bg=body.cget("bg"),
            fg=COLORS["primary"] if enabled else "#cbd5e1",
            anchor="w",
        ).pack(side="left")

        if enabled:
            self._bind_recursive(body)

    def _bind_recursive(self, widget):
        widget.bind("<Button-1>", self._on_click)
        widget.bind("<Enter>", self._on_enter)
        widget.bind("<Leave>", self._on_leave)
        for child in widget.winfo_children():
            self._bind_recursive(child)

    def _on_enter(self, _e=None):
        if self._enabled:
            self.card.configure(bg=self.hover_card)

    def _on_leave(self, _e=None):
        if self._enabled:
            self.card.configure(bg=self.normal_card)

    def _on_click(self, _e=None):
        if self._enabled and self._command:
            self._command()


class InfoPanel(tk.Frame):
    def __init__(self, master, title: str, lines: List[str], accent="#2563eb", **kw):
        super().__init__(master, bg=COLORS["bg_page"], **kw)
        card = Card(self, bg=COLORS["bg_card"], pad=1)
        card.pack(fill="both", expand=True)
        body = card.body

        head = tk.Frame(body, bg=COLORS["bg_card"])
        head.pack(fill="x", padx=16, pady=(14, 8))

        tk.Frame(head, bg=accent, width=6, height=24).pack(side="left", padx=(0, 10))
        tk.Label(
            head,
            text=title,
            font=FONT_H3,
            fg=COLORS["text"],
            bg=COLORS["bg_card"],
        ).pack(side="left")

        content = tk.Frame(body, bg=COLORS["bg_card"])
        content.pack(fill="both", expand=True, padx=16, pady=(0, 14))

        for line in lines:
            tk.Label(
                content,
                text=f"• {line}",
                font=FONT_SMALL,
                fg=COLORS["text_muted"],
                bg=COLORS["bg_card"],
                anchor="w",
                justify="left",
                wraplength=340,
            ).pack(fill="x", pady=2)


# ================================================================== #
#  HomePage — обновлённый dashboard
# ================================================================== #

class HomePage(tk.Frame):
    """
    Главная страница — современная информационная панель.
    Показывает:
    - приветствие,
    - KPI/метрики,
    - быстрые действия,
    - подсказки по работе.
    """

    ACTIONS_PRIMARY = [
        ("📋", "Создать табель", "Заполнить табель рабочего времени", "timesheet", None),
        ("📑", "Мои табели", "Просмотр и редактирование ваших табелей", "my_timesheets", None),
        ("🚛", "Заявка на транспорт", "Оформить заявку на спецтехнику", "transport", None),
        ("🍽️", "Заказ питания", "Подать заявку на питание бригады", "meals_order", None),
    ]

    ACTIONS_EXTENDED = [
        ("📊", "Аналитика", "Сводные показатели и метрики по работе", "analytics_dashboard", None),
        ("🏗️", "Реестр объектов", "Список и состояние объектов компании", "objects_registry", None),
        ("🏠", "Проживание", "Реестр заселений, комнат и общежитий", "lodging_registry", None),
        ("👤", "Карточка сотрудника", "Работа, питание и проживание сотрудника", "employee_card", None),
        ("📂", "Реестр табелей", "Общий реестр табелей всех пользователей", "timesheet_registry", None),
        ("🚚", "Реестр транспорта", "Все заявки на транспорт и спецтехнику", "transport_registry", None),
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
        super().__init__(master, bg=COLORS["bg_page"])
        self._app_ref = app_ref

        self._canvas = tk.Canvas(self, bg=COLORS["bg_page"], highlightthickness=0)
        self._vsb = ttk.Scrollbar(self, orient="vertical", command=self._canvas.yview)
        self._inner = tk.Frame(self._canvas, bg=COLORS["bg_page"])

        self._inner_id = self._canvas.create_window((0, 0), window=self._inner, anchor="nw")
        self._inner.bind("<Configure>", lambda e: self._canvas.configure(scrollregion=self._canvas.bbox("all")))
        self._canvas.bind("<Configure>", self._on_canvas_resize)
        self._canvas.configure(yscrollcommand=self._vsb.set)

        self._canvas.pack(side="left", fill="both", expand=True)
        self._vsb.pack(side="right", fill="y")

        self.bind("<Map>", self._bind_mousewheel_safe)
        self._build()

    def _bind_mousewheel_safe(self, _event=None):
        self._canvas.bind_all("<MouseWheel>", self._on_mousewheel_windows)
        self._canvas.bind_all("<Button-4>", self._on_mousewheel_linux_up)
        self._canvas.bind_all("<Button-5>", self._on_mousewheel_linux_down)

    def _on_mousewheel_windows(self, event):
        self._canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _on_mousewheel_linux_up(self, event):
        self._canvas.yview_scroll(-1, "units")

    def _on_mousewheel_linux_down(self, event):
        self._canvas.yview_scroll(1, "units")

    def _on_canvas_resize(self, event):
        self._canvas.itemconfig(self._inner_id, width=event.width)

    def _build(self):
        c = self._inner
        now = datetime.now()
        stats = _load_home_stats()

        outer = tk.Frame(c, bg=COLORS["bg_page"])
        outer.pack(fill="both", expand=True, padx=28, pady=22)

        # HERO
        HeroBanner(outer, self._app_ref).pack(fill="x", pady=(0, 18))

        # Метрики
        SectionTitle(
            outer,
            "Оперативная сводка",
            "Ключевые показатели на текущий момент",
        ).pack(fill="x", pady=(0, 10))

        metrics = tk.Frame(outer, bg=COLORS["bg_page"])
        metrics.pack(fill="x")

        metrics_cfg = [
            (stats["employees_count"], "Сотрудников", COLORS["cyan"], None),
            (stats["objects_count"], "Объектов", COLORS["warning"], "objects_registry"),
            (stats["timesheets_month"], f"Табелей за {now.strftime('%m.%Y')}", COLORS["success"], "my_timesheets"),
            (stats["transport_today"], "Транспорт сегодня", COLORS["violet"], "transport"),
            (stats["meals_today"], "Питание сегодня", COLORS["danger"], "meals_order"),
        ]

        for i, (val, lbl, accent, page_key) in enumerate(metrics_cfg):
            cmd = (lambda k=page_key: self._go(k)) if page_key else None
            tile = MetricTile(metrics, val, lbl, accent=accent, on_click=cmd)
            tile.grid(row=0, column=i, padx=6, pady=4, sticky="nsew")
            metrics.columnconfigure(i, weight=1)

        # Две колонки: действия + панель задач
        mid = tk.Frame(outer, bg=COLORS["bg_page"])
        mid.pack(fill="x", pady=(20, 0))
        mid.columnconfigure(0, weight=3)
        mid.columnconfigure(1, weight=2)

        left = tk.Frame(mid, bg=COLORS["bg_page"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))

        right = tk.Frame(mid, bg=COLORS["bg_page"])
        right.grid(row=0, column=1, sticky="nsew", padx=(10, 0))

        # Быстрые действия
        SectionTitle(
            left,
            "Быстрые действия",
            "Самые востребованные операции для ежедневной работы",
        ).pack(fill="x", pady=(0, 10))

        primary_grid = tk.Frame(left, bg=COLORS["bg_page"])
        primary_grid.pack(fill="x")

        self._build_actions_grid(primary_grid, self.ACTIONS_PRIMARY, max_cols=2)

        visible_extended = [a for a in self.ACTIONS_EXTENDED if self._has_access(a[3], a[4])]
        if visible_extended:
            SectionTitle(
                left,
                "Управление и отчёты",
                "Разделы управления, контроля и аналитики",
            ).pack(fill="x", pady=(18, 10))
            ext_grid = tk.Frame(left, bg=COLORS["bg_page"])
            ext_grid.pack(fill="x")
            self._build_actions_grid(ext_grid, visible_extended, max_cols=2)

        # Правая панель
        SectionTitle(
            right,
            "Что сделать сегодня",
            "Короткие ориентиры для пользователя",
        ).pack(fill="x", pady=(0, 10))

        tips = [
            "Проверьте, созданы ли табели за текущий месяц.",
            "Если требуется техника — оформите заявку заранее.",
            "Сверьте заказы на питание на текущую дату.",
            "Используйте верхнее меню для перехода в специализированные разделы.",
        ]
        InfoPanel(right, "Рекомендации", tips, accent=COLORS["primary"]).pack(fill="x", pady=(0, 12))

        user_name = ""
        if self._app_ref and self._app_ref.current_user:
            user_name = self._app_ref.current_user.get("full_name") or self._app_ref.current_user.get("username") or ""

        personal_lines = [
            f"Пользователь: {user_name or '—'}",
            f"Дата входа: {now.strftime('%d.%m.%Y %H:%M')}",
            "Для полного набора функций используйте верхнее меню.",
        ]
        InfoPanel(right, "Текущая сессия", personal_lines, accent=COLORS["success"]).pack(fill="x")

        tk.Label(
            outer,
            text="Главная панель разработана для быстрого старта: метрики сверху, действия по центру, рекомендации справа.",
            font=FONT_TINY,
            fg=COLORS["text_soft"],
            bg=COLORS["bg_page"],
        ).pack(anchor="w", pady=(18, 6))

    def _build_actions_grid(self, parent, actions: list, max_cols: int = 2):
        col, row = 0, 0
        for icon, ttl, sub, page_key, perm in actions:
            enabled = self._has_access(page_key, perm)
            card = ActionCard(
                parent,
                icon=icon,
                title=ttl,
                subtitle=sub,
                command=(lambda k=page_key: self._go(k)) if enabled else None,
                enabled=enabled,
            )
            card.grid(row=row, column=col, padx=6, pady=6, sticky="nsew")
            parent.columnconfigure(col, weight=1)
            col += 1
            if col >= max_cols:
                col = 0
                row += 1

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
            self._app_ref._show_page(
                page_key,
                lambda p, _fn=builder_fn, _app=app: _fn(p, _app),
            )


# ================================================================== #
#  LoginPage — обновлённый экран входа
# ================================================================== #

class LoginPage(tk.Frame):
    """Страница входа в систему с современным оформлением и сохранением учётных данных."""
    def __init__(self, master, app_ref: "MainApp"):
        super().__init__(master, bg=COLORS["bg_page"])
        self.app_ref = app_ref
        self._show_pass = False
        self.logo_img = None

        self._build()
        self.bind_all("<Return>", self._on_enter)

    def _build(self):
        layout = tk.Frame(self, bg=COLORS["bg_page"])
        layout.place(relx=0.5, rely=0.5, anchor="center")

        card = Card(layout, bg=COLORS["bg_card"], pad=1)
        card.pack()
        body = card.body
        body.configure(bg=COLORS["bg_card"])

        wrap = tk.Frame(body, bg=COLORS["bg_card"])
        wrap.pack(padx=28, pady=26)

        self.logo_img = embedded_logo_image(wrap, max_w=220, max_h=72)
        if self.logo_img:
            tk.Label(wrap, image=self.logo_img, bg=COLORS["bg_card"]).grid(
                row=0, column=0, columnspan=2, pady=(0, 12)
            )

        tk.Label(
            wrap,
            text="Управление строительством",
            font=("Segoe UI", 18, "bold"),
            fg=COLORS["text"],
            bg=COLORS["bg_card"],
        ).grid(row=1, column=0, columnspan=2, pady=(0, 4))

        tk.Label(
            wrap,
            text="Вход в систему",
            font=("Segoe UI", 10),
            fg=COLORS["text_muted"],
            bg=COLORS["bg_card"],
        ).grid(row=2, column=0, columnspan=2, pady=(0, 18))

        tk.Label(
            wrap,
            text="Логин",
            font=FONT_SMALL,
            fg=COLORS["text"],
            bg=COLORS["bg_card"],
            anchor="w",
        ).grid(row=3, column=0, columnspan=2, sticky="w", pady=(0, 4))

        self.ent_login = ttk.Entry(wrap, width=34, style="App.TEntry")
        self.ent_login.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(0, 12), ipady=3)

        tk.Label(
            wrap,
            text="Пароль",
            font=FONT_SMALL,
            fg=COLORS["text"],
            bg=COLORS["bg_card"],
            anchor="w",
        ).grid(row=5, column=0, columnspan=2, sticky="w", pady=(0, 4))

        pass_frame = tk.Frame(wrap, bg=COLORS["bg_card"])
        pass_frame.grid(row=6, column=0, columnspan=2, sticky="ew")
        pass_frame.columnconfigure(0, weight=1)

        self.ent_pass = ttk.Entry(pass_frame, width=30, show="*", style="App.TEntry")
        self.ent_pass.grid(row=0, column=0, sticky="ew", ipady=3)

        self.btn_eye = ttk.Button(
            pass_frame,
            text="👁",
            width=3,
            command=self._toggle_password,
            style="Soft.TButton",
        )
        self.btn_eye.grid(row=0, column=1, padx=(6, 0))

        self.var_remember = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            wrap,
            text="Запомнить меня",
            variable=self.var_remember,
        ).grid(row=7, column=0, columnspan=2, sticky="w", pady=(12, 0))

        btns = tk.Frame(wrap, bg=COLORS["bg_card"])
        btns.grid(row=8, column=0, columnspan=2, pady=(18, 0), sticky="ew")
        btns.columnconfigure(0, weight=1)
        btns.columnconfigure(1, weight=1)

        ttk.Button(
            btns,
            text="Войти",
            command=self._on_login,
            style="Primary.TButton",
        ).grid(row=0, column=0, sticky="ew", padx=(0, 6))

        ttk.Button(
            btns,
            text="Выход",
            command=self._on_exit,
            style="App.TButton",
        ).grid(row=0, column=1, sticky="ew", padx=(6, 0))

        tk.Label(
            wrap,
            text="Используйте корпоративные учётные данные для входа в систему.",
            font=FONT_TINY,
            fg=COLORS["text_soft"],
            bg=COLORS["bg_card"],
        ).grid(row=9, column=0, columnspan=2, pady=(14, 0))

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
        self.btn_eye.configure(text="🔒" if self._show_pass else "👁")

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

        width = 480
        height = 260

        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x = (screen_width // 2) - (width // 2)
        y = (screen_height // 2) - (height // 2)
        self.geometry(f"{width}x{height}+{x}+{y}")

        self.config(bg=COLORS["bg_card"], relief="solid", borderwidth=1)

        container = tk.Frame(self, bg=COLORS["bg_card"])
        container.pack(fill="both", expand=True, padx=24, pady=24)

        tk.Label(
            container,
            text="Управление строительством",
            font=("Segoe UI", 18, "bold"),
            fg=COLORS["text"],
            bg=COLORS["bg_card"],
        ).pack(pady=(24, 8))

        tk.Label(
            container,
            text="Пожалуйста, подождите...",
            font=FONT_BODY,
            fg=COLORS["text_muted"],
            bg=COLORS["bg_card"],
        ).pack()

        self.progress = ttk.Progressbar(
            container,
            mode="indeterminate",
            style="App.Horizontal.TProgressbar",
        )
        self.progress.pack(pady=24, fill="x")
        self.progress.start(10)

        self.status_label = tk.Label(
            container,
            text="Инициализация...",
            font=FONT_SMALL,
            fg=COLORS["text_muted"],
            bg=COLORS["bg_card"],
        )
        self.status_label.pack(side="bottom", fill="x", ipady=8)

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
        self.geometry("1220x820")
        self.minsize(1020, 680)
        self.configure(bg=COLORS["bg_app"])

        self._pages: Dict[str, tk.Widget] = {}
        self._build_menu()

        # Верхний header
        self.header = tk.Frame(self, bg=COLORS["bg_header"])
        self.header.pack(fill="x")

        self.header_inner = tk.Frame(self.header, bg=COLORS["bg_header"])
        self.header_inner.pack(fill="x", padx=16, pady=10)

        header_left = tk.Frame(self.header_inner, bg=COLORS["bg_header"])
        header_left.pack(side="left", fill="x", expand=True)

        self.lbl_header_title = tk.Label(
            header_left,
            text="",
            font=("Segoe UI", 15, "bold"),
            fg=COLORS["text"],
            bg=COLORS["bg_header"],
        )
        self.lbl_header_title.pack(side="left")

        self.lbl_header_hint = tk.Label(
            header_left,
            text="",
            font=FONT_SMALL,
            fg=COLORS["text_muted"],
            bg=COLORS["bg_header"],
        )
        self.lbl_header_hint.pack(side="left", padx=(12, 0))

        header_right = tk.Frame(self.header_inner, bg=COLORS["bg_header"])
        header_right.pack(side="right")

        self.lbl_user_info = tk.Label(
            header_right,
            text="",
            font=FONT_SMALL,
            fg=COLORS["text_muted"],
            bg=COLORS["bg_header"],
        )
        self.lbl_user_info.pack(side="left", padx=(0, 10))

        self.btn_logout = ttk.Button(
            header_right,
            text="Выйти",
            width=10,
            command=self._on_logout,
            style="App.TButton",
        )
        self.btn_logout.pack(side="left")
        self.btn_logout.pack_forget()

        tk.Frame(self, height=1, bg=COLORS["line"]).pack(fill="x")

        self.content = tk.Frame(self, bg=COLORS["bg_page"])
        self.content.pack(fill="both", expand=True)

        footer = tk.Frame(self, bg="#fafbfc")
        footer.pack(fill="x")
        tk.Frame(footer, height=1, bg=COLORS["line"]).pack(fill="x")
        tk.Label(
            footer,
            text="Разработал Алексей Зезюкин, 2025",
            font=FONT_TINY,
            fg=COLORS["text_soft"],
            bg="#fafbfc",
        ).pack(side="right", padx=12, pady=5)

        self._set_user(None)
        self.show_login()

    # ------------------------------------------------------------------ #
    #  Выход из аккаунта
    # ------------------------------------------------------------------ #
    def _on_logout(self):
        """Выход из учётной записи — возврат на страницу логина."""
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
            self.lbl_user_info.config(text=f"👤 {name}")
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
            "home": ("Управление строительством", "Единая стартовая панель и быстрый доступ к основным разделам"),
            "timesheet": ("Объектный табель", ""),
            "my_timesheets": ("Мои табели", ""),
            "timesheet_registry": ("Реестр табелей", ""),
            "brigades": ("Бригады", "Назначение бригадиров по подразделениям"),
            "workers": ("Работники", "Поиск по сотруднику и его объектам"),
            "timesheet_compare": ("Сравнение табелей", "Объектный vs Кадровый (1С)"),
            "gpr": ("ГПР (Диаграмма Ганта)", "План работ по объекту (диапазон дат)"),
            "transport": ("Заявка на спецтехнику", ""),
            "my_transport_orders": ("Мои заявки на транспорт", ""),
            "planning": ("Планирование транспорта", ""),
            "transport_registry": ("Реестр транспорта", ""),
            "meals_order": ("Заказ питания", ""),
            "my_meals_orders": ("Мои заявки на питание", ""),
            "meals_planning": ("Планирование питания", ""),
            "meals_registry": ("Реестр заявок на питание", ""),
            "meals_reports": ("Отчеты по питанию", "Дневной и месячный свод по комплексам"),
            "meals_workers": ("Работники (питание)", "История питания по сотруднику"),
            "meals_settings": ("Настройки питания", ""),
            "lodging_registry": ("Проживание", "Реестр заселений/выселений"),
            "lodging_dorms": ("Проживание", "Общежития и комнаты"),
            "lodging_rates": ("Проживание", "Тарифы (цена за сутки)"),
            "object_create": ("Объекты: Создание/Редактирование", ""),
            "payroll": ("Затраты (ФОТ)", "Загрузка начислений и распределение по объектам"),
            "objects_registry": ("Реестр объектов", ""),
            "employee_card": ("Сотрудники", "Карточка сотрудника (работа/питание/проживание)"),
            "budget": ("Анализ смет", ""),
            "estimate_resource_decoder": ("Раскрытие ресурсов сметы", "Расшифровка расценок до конкретных ресурсов"),
            "login": ("Управление строительством", "Вход в систему"),
            "analytics_dashboard": ("Операционная аналитика", "Сводные показатели по ключевым метрикам"),
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
