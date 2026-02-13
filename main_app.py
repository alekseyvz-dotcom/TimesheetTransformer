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
    Простая обфускация (base64).  НЕ является криптостойким шифрованием,
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

def perform_heavy_imports():
    global BudgetAnalyzer, _assets_logo, _LOGO_BASE64, SpecialOrders, \
           meals_module, objects, Settings, timesheet_module, \
           analytics_module, timesheet_transformer, employees_module, \
           timesheet_compare, meals_employees_module, lodging_module, \
           meals_reports_module, employee_card_module
           
    import BudgetAnalyzer
    import assets_logo as _assets_logo
    _LOGO_BASE64 = getattr(_assets_logo, "LOGO_BASE64", None)
    import SpecialOrders
    import meals_module
    import meals_reports as meals_reports_module
    import objects
    import settings_manager as Settings
    import timesheet_module 
    import analytics_module
    import employees as employees_module
    import timesheet_compare
    import meals_employees as meals_employees_module
    import lodging_module as lodging_module
    import employee_card as employee_card_module
    try:
        import timesheet_transformer
    except ImportError:
        timesheet_transformer = None

# --- КОНСТАНТЫ И ГЛОБАЛЬНЫЕ НАСТРОЙКИ ---
APP_NAME = "Управление строительством (Главное меню)"

db_connection_pool = None

# --- ГЛАВНЫЕ УТИЛИТЫ ПРИЛОЖЕНИЯ ---

def initialize_db_pool():
    """Создает пул соединений с БД. Вызывается один раз при старте приложения."""
    global db_connection_pool
    if db_connection_pool: return
    try:
        provider = Settings.get_db_provider().strip().lower()
        if provider != "postgres": raise RuntimeError(f"Ожидался provider=postgres, а в настройках: {provider!r}")
        db_url = Settings.get_database_url().strip()
        if not db_url: raise RuntimeError("В настройках не указана строка подключения (DATABASE_URL)")

        url = urlparse(db_url)
        db_connection_pool = pool.SimpleConnectionPool(
            minconn=1, maxconn=10,
            host=url.hostname or "localhost", port=url.port or 5432,
            dbname=url.path.lstrip("/"), user=url.username, password=url.password,
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
        if not db_connection_pool: raise RuntimeError("Пул соединений недоступен.")
        conn = db_connection_pool.getconn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, username, password_hash, is_active, full_name, role FROM app_users WHERE username = %s", (username,))
            row = cur.fetchone()
            if not row or not row["is_active"] or not _verify_password(password, row["password_hash"]):
                return None
            row.pop("password_hash", None)
            return dict(row)
    finally:
        if conn and db_connection_pool: db_connection_pool.putconn(conn)

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
            # Сотрудники (не уволенные)
            cur.execute("SELECT count(*) FROM employees WHERE NOT is_fired")
            stats["employees_count"] = cur.fetchone()[0]

            # Объекты
            cur.execute("SELECT count(*) FROM objects")
            stats["objects_count"] = cur.fetchone()[0]

            # Табели за текущий месяц
            now = datetime.now()
            cur.execute(
                "SELECT count(*) FROM timesheet_headers WHERE year=%s AND month=%s",
                (now.year, now.month),
            )
            stats["timesheets_month"] = cur.fetchone()[0]

            # Заявки на транспорт сегодня
            cur.execute(
                "SELECT count(*) FROM transport_orders WHERE date=%s",
                (now.date(),),
            )
            stats["transport_today"] = cur.fetchone()[0]

            # Заявки на питание сегодня
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
#  HomePage — улучшенная главная с карточками-виджетами
# ================================================================== #

class _StatCard(tk.Frame):
    """Одна карточка со счётчиком для домашней страницы."""
    def __init__(self, master, icon_char: str, value: Any, label: str,
                 bg_color="#ffffff", fg_accent="#2563EB", **kw):
        super().__init__(master, bg=bg_color, highlightbackground="#ddd",
                         highlightthickness=1, **kw)
        self.configure(padx=18, pady=14)

        top = tk.Frame(self, bg=bg_color)
        top.pack(fill="x")

        tk.Label(
            top, text=icon_char, font=("Segoe UI Emoji", 22),
            bg=bg_color, fg=fg_accent,
        ).pack(side="left")

        tk.Label(
            top, text=str(value), font=("Segoe UI", 22, "bold"),
            bg=bg_color, fg="#111",
        ).pack(side="right", padx=(8, 0))

        tk.Label(
            self, text=label, font=("Segoe UI", 9), fg="#666", bg=bg_color,
            wraplength=140, justify="center",
        ).pack(pady=(6, 0))


class HomePage(tk.Frame):
    """Домашняя страница с логотипом, приветствием и информационными карточками."""
    def __init__(self, master, app_ref: "MainApp" = None):
        super().__init__(master, bg="#f7f7f7")
        self._app_ref = app_ref

        # --- Верхний блок: лого + приветствие ---
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(pady=(24, 8))

        self.logo_img = embedded_logo_image(top, max_w=280, max_h=280)
        if self.logo_img:
            tk.Label(top, image=self.logo_img, bg="#f7f7f7").pack(anchor="center", pady=(0, 8))

        greeting = "Добро пожаловать!"
        if app_ref and app_ref.current_user:
            name = app_ref.current_user.get("full_name") or app_ref.current_user.get("username") or ""
            if name:
                greeting = f"Добро пожаловать, {name}!"
        tk.Label(
            top, text=greeting, font=("Segoe UI", 16, "bold"), bg="#f7f7f7",
        ).pack(anchor="center", pady=(0, 2))

        today_str = datetime.now().strftime("%d.%m.%Y, %A")
        tk.Label(
            top, text=today_str, font=("Segoe UI", 10), fg="#888", bg="#f7f7f7",
        ).pack(anchor="center")

        # --- Карточки статистики ---
        cards_frame = tk.Frame(self, bg="#f7f7f7")
        cards_frame.pack(pady=(16, 12))

        stats = _load_home_stats()
        now = datetime.now()

        cards_data = [
            ("👷", stats["employees_count"], "Сотрудников\n(активных)", "#E0F2FE", "#0284C7"),
            ("🏗️", stats["objects_count"], "Объектов\nв базе", "#FEF3C7", "#D97706"),
            ("📋", stats["timesheets_month"], f"Табелей\nза {now.strftime('%B %Y')}", "#DCFCE7", "#16A34A"),
            ("🚛", stats["transport_today"], "Заявок на транспорт\nсегодня", "#EDE9FE", "#7C3AED"),
            ("🍽️", stats["meals_today"], "Заявок на питание\nсегодня", "#FFE4E6", "#E11D48"),
        ]

        for i, (icon, val, lbl, bg_c, fg_c) in enumerate(cards_data):
            card = _StatCard(cards_frame, icon, val, lbl, bg_color=bg_c, fg_accent=fg_c)
            card.grid(row=0, column=i, padx=8, pady=4)

        # --- Подсказка ---
        tk.Label(
            self, text="Выберите раздел в верхнем меню для начала работы.",
            font=("Segoe UI", 10), fg="#555", bg="#f7f7f7",
        ).pack(pady=(8, 0))

        # --- Кнопки быстрого доступа ---
        quick = tk.Frame(self, bg="#f7f7f7")
        quick.pack(pady=(16, 8))

        quick_buttons = [
            ("📋  Создать табель", "timesheet"),
            ("🚛  Заявка на транспорт", "transport"),
            ("🍽️  Заказ питания", "meals_order"),
            ("📊  Аналитика", "analytics_dashboard"),
        ]

        for text, page_key in quick_buttons:
            btn = ttk.Button(
                quick, text=text, width=26,
                command=lambda k=page_key: self._go(k),
            )
            btn.pack(side="left", padx=6)

    def _go(self, page_key: str):
        if self._app_ref:
            # Используем уже существующие лямбды из _build_menu
            # Самый простой путь — вызвать _show_page с билдером
            builders = {
                "timesheet": lambda p: timesheet_module.create_timesheet_page(p, self._app_ref),
                "transport": lambda p: SpecialOrders.create_page(p, self._app_ref),
                "meals_order": lambda p: meals_module.create_meals_order_page(p, self._app_ref),
                "analytics_dashboard": lambda p: analytics_module.AnalyticsPage(p, self._app_ref),
            }
            builder = builders.get(page_key)
            if builder:
                self._app_ref._show_page(page_key, builder)


# ================================================================== #
#  LoginPage — с галочкой «Запомнить меня»
# ================================================================== #

class LoginPage(tk.Frame):
    """Страница входа в систему с возможностью сохранить учётные данные."""
    def __init__(self, master, app_ref: "MainApp"):
        super().__init__(master, bg="#f7f7f7")
        self.app_ref = app_ref

        center = tk.Frame(self, bg="#f7f7f7")
        center.place(relx=0.5, rely=0.5, anchor="center")

        # --- Заголовок ---
        tk.Label(
            center, text="Управление строительством",
            font=("Segoe UI", 16, "bold"), bg="#f7f7f7",
        ).grid(row=0, column=0, columnspan=2, pady=(0, 4))

        tk.Label(
            center, text="Вход в систему",
            font=("Segoe UI", 11), fg="#555", bg="#f7f7f7",
        ).grid(row=1, column=0, columnspan=2, pady=(0, 15))

        # --- Логин ---
        tk.Label(center, text="Логин:", bg="#f7f7f7").grid(
            row=2, column=0, sticky="e", padx=(0, 6), pady=4)
        self.ent_login = ttk.Entry(center, width=28)
        self.ent_login.grid(row=2, column=1, sticky="w", pady=4)

        # --- Пароль ---
        tk.Label(center, text="Пароль:", bg="#f7f7f7").grid(
            row=3, column=0, sticky="e", padx=(0, 6), pady=4)

        pass_frame = tk.Frame(center, bg="#f7f7f7")
        pass_frame.grid(row=3, column=1, sticky="w", pady=4)

        self.ent_pass = ttk.Entry(pass_frame, width=22, show="*")
        self.ent_pass.pack(side="left")

        self._show_pass = False
        self.btn_eye = ttk.Button(pass_frame, text="👁", width=3, command=self._toggle_password)
        self.btn_eye.pack(side="left", padx=(4, 0))

        # --- Запомнить меня ---
        self.var_remember = tk.BooleanVar(value=False)
        chk = ttk.Checkbutton(
            center, text="Запомнить меня",
            variable=self.var_remember,
        )
        chk.grid(row=4, column=1, sticky="w", pady=(4, 0))

        # --- Кнопки ---
        btns = tk.Frame(center, bg="#f7f7f7")
        btns.grid(row=5, column=0, columnspan=2, pady=(14, 0))

        ttk.Button(btns, text="Войти", width=14, command=self._on_login).pack(
            side="left", padx=6)
        ttk.Button(btns, text="Выход", width=10, command=self._on_exit).pack(
            side="left", padx=6)

        # --- Загрузка сохранённых данных ---
        saved_user, saved_pass, remember = load_saved_credentials()
        if remember:
            self.ent_login.insert(0, saved_user)
            self.ent_pass.insert(0, saved_pass)
            self.var_remember.set(True)

        # Фокус
        if saved_user:
            self.ent_pass.focus_set()
        else:
            self.ent_login.focus_set()

        self.bind_all("<Return>", self._on_enter)

    def _toggle_password(self):
        """Показать/скрыть пароль."""
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

        # --- Сохраняем / удаляем учётные данные ---
        save_credentials(username, password, self.var_remember.get())

        self.app_ref.on_login_success(user)

    def _on_exit(self):
        self.app_ref.destroy()


class SplashScreen(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Загрузка...")
        self.overrideredirect(True)
        
        width = 450
        height = 250

        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x = (screen_width // 2) - (width // 2)
        y = (screen_height // 2) - (height // 2)
        self.geometry(f'{width}x{height}+{x}+{y}')

        self.config(bg="#f0f0f0", relief="solid", borderwidth=1)
        
        tk.Label(
            self, text="Управление строительством", 
            font=("Segoe UI", 16, "bold"), bg="#f0f0f0"
        ).pack(pady=(40, 10))

        tk.Label(
            self, text="Пожалуйста, подождите...", 
            font=("Segoe UI", 10), bg="#f0f0f0"
        ).pack()

        self.status_label = tk.Label(
            self, text="Инициализация...", 
            font=("Segoe UI", 9), fg="#555", bg="#f0f0f0"
        )
        self.status_label.pack(side="bottom", fill="x", ipady=10)

        self.progress = ttk.Progressbar(self, mode='indeterminate')
        self.progress.pack(pady=20, padx=40, fill="x")
        self.progress.start(10)

    def update_status(self, text):
        self.status_label.config(text=text)
        self.update_idletasks()


class MainApp(tk.Tk):
    """Главный класс приложения (каркас)."""
    def __init__(self, current_user: Optional[Dict[str, Any]] = None):
        super().__init__()
    
        self.current_user: Dict[str, Any] = current_user or {}
        self.is_authenticated: bool = bool(current_user)
        self.title(APP_NAME)
        self.geometry("1100x768")
        self.minsize(980, 640)
        
        self._pages: Dict[str, tk.Widget] = {}
        self._build_menu()

        # --- Основная компоновка окна ---
        # Верхний хедер с заголовком и информацией о пользователе
        self.header = tk.Frame(self, bg="#ffffff", relief="flat")
        self.header.pack(fill="x", padx=0, pady=0)

        # Левая часть — заголовок
        header_left = tk.Frame(self.header, bg="#ffffff")
        header_left.pack(side="left", padx=12, pady=8)
        self.lbl_header_title = tk.Label(
            header_left, text="", font=("Segoe UI", 14, "bold"), bg="#ffffff",
        )
        self.lbl_header_title.pack(side="left")
        self.lbl_header_hint = tk.Label(
            header_left, text="", font=("Segoe UI", 9), fg="#888", bg="#ffffff",
        )
        self.lbl_header_hint.pack(side="left", padx=(12, 0))

        # Правая часть — пользователь + выход
        header_right = tk.Frame(self.header, bg="#ffffff")
        header_right.pack(side="right", padx=12, pady=8)
        self.lbl_user_info = tk.Label(
            header_right, text="", font=("Segoe UI", 9), fg="#555", bg="#ffffff",
        )
        self.lbl_user_info.pack(side="left", padx=(0, 8))
        self.btn_logout = ttk.Button(
            header_right, text="⏻ Выйти", width=10, command=self._on_logout,
        )
        self.btn_logout.pack(side="left")
        self.btn_logout.pack_forget()  # скрыт до авторизации

        # Тонкая линия-разделитель
        sep = tk.Frame(self, height=1, bg="#ddd")
        sep.pack(fill="x")

        self.content = tk.Frame(self, bg="#f7f7f7")
        self.content.pack(fill="both", expand=True)

        # Футер
        footer = tk.Frame(self, bg="#fafafa", relief="flat")
        footer.pack(fill="x", padx=0, pady=0)
        sep2 = tk.Frame(footer, height=1, bg="#eee")
        sep2.pack(fill="x")
        tk.Label(
            footer, text="Разработал Алексей Зезюкин, 2025",
            font=("Segoe UI", 8), fg="#999", bg="#fafafa",
        ).pack(side="right", padx=12, pady=4)

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
        self._menu_timesheets_registry_index = m_ts.index("end")
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
        self._menubar.add_cascade(label="Объектный табель", menu=m_ts)
        self._menu_timesheets = m_ts

        # === Автотранспорт ===
        m_transport = tk.Menu(self._menubar, tearoff=0)
        m_transport.add_command(label="Создать заявку", command=lambda: self._show_page("transport", lambda p: SpecialOrders.create_page(p, self)))
        m_transport.add_command(label="Мои заявки", command=lambda: self._show_page("my_transport_orders", lambda p: SpecialOrders.create_my_transport_orders_page(p, self)))
        self._menu_transport_planning_index = m_transport.index("end")
        m_transport.add_command(label="Планирование", command=lambda: self._show_page("planning", lambda p: SpecialOrders.create_planning_page(p)))
        self._menu_transport_registry_index = m_transport.index("end")
        m_transport.add_command(label="Реестр", command=lambda: self._show_page("transport_registry", lambda p: SpecialOrders.create_transport_registry_page(p)))
        self._menubar.add_cascade(label="Автотранспорт", menu=m_transport)
        self._menu_transport = m_transport

        # === Питание ===
        m_meals = tk.Menu(self._menubar, tearoff=0)
        m_meals.add_command(label="Создать заявку", command=lambda: self._show_page("meals_order", lambda p: meals_module.create_meals_order_page(p, self)))
        m_meals.add_command(label="Мои заявки", command=lambda: self._show_page("my_meals_orders", lambda p: meals_module.create_my_meals_orders_page(p, self)))
        m_meals.add_command(label="Планирование", command=lambda: self._show_page("meals_planning", lambda p: meals_module.create_meals_planning_page(p, self)))
        m_meals.add_command(label="Реестр", command=lambda: self._show_page("meals_registry", lambda p: meals_module.create_all_meals_orders_page(p, self)))
        m_meals.add_command(label="Отчеты", command=lambda: self._show_page("meals_reports", lambda p: meals_reports_module.create_meals_reports_page(p, self)))
        m_meals.add_command(label="Работники (питание)", command=lambda: self._show_page("meals_workers", lambda p: meals_employees_module.create_meals_workers_page(p, self)))
        self._menu_meals_settings_index = m_meals.index("end")
        m_meals.add_command(label="Настройки", command=lambda: self._show_page("meals_settings", lambda p: meals_module.create_meals_settings_page(p, self.current_user.get('role'))))
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
                lambda p: lodging_module.create_rates_page(p, self)
            ),
        )
        self._menubar.add_cascade(label="Проживание", menu=m_lodging)
        self._menu_lodging = m_lodging

        # === Объекты ===
        m_objects = tk.Menu(self._menubar, tearoff=0)
        m_objects.add_command(label="Создать/Редактировать", command=lambda: self._show_page("object_create", lambda p: objects.ObjectCreatePage(p, self)))
        m_objects.add_command(label="Реестр", command=lambda: self._show_page("objects_registry", lambda p: objects.ObjectsRegistryPage(p, self)))
        self._menubar.add_cascade(label="Объекты", menu=m_objects)
        self._menu_objects = m_objects

        # === Аналитика ===
        m_analytics = tk.Menu(self._menubar, tearoff=0)
        m_analytics.add_command(label="Операционная аналитика", command=lambda: self._show_page("analytics_dashboard", lambda p: analytics_module.AnalyticsPage(p, self)))
        self._menubar.add_cascade(label="Аналитика", menu=m_analytics)
        self._menu_analytics = m_analytics

        m_emp = tk.Menu(self._menubar, tearoff=0)
        m_emp.add_command(label="Карточка сотрудника",
                          command=lambda: self._show_page("employee_card",
                              lambda p: employee_card_module.create_employee_card_page(p, self)))
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
            m_tools.add_command(label="Анализ смет", command=lambda: self._show_page("budget", lambda p: BudgetAnalyzer.create_page(p)))
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
            name = user.get('full_name') or user.get('username') or ""
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
            "home": ("Управление строительством", "Выберите раздел в верхнем меню"),
            "timesheet": ("Объектный табель", ""),
            "my_timesheets": ("Мои табели", ""),
            "timesheet_registry": ("Реестр табелей", ""),
            "workers": ("Работники", "Поиск по сотруднику и его объектам"),
            "timesheet_compare": ("Сравнение табелей", "Объектный vs Кадровый (1С)"),
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
            "objects_registry": ("Реестр объектов", ""),
            "employee_card": ("Сотрудники", "Карточка сотрудника (работа/питание/проживание)"),
            "budget": ("Анализ смет", ""),
            "login": ("Управление строительством", "Вход в систему"),
            "analytics_dashboard": (
                "Операционная аналитика",
                "Сводные показатели по ключевым метрикам",
            ),
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
                analytics_module,
                employees_module,
                timesheet_compare,
                meals_employees_module,
                lodging_module,
                employee_card_module,
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
            messagebox.showerror("Критическая ошибка", f"Не удалось инициализировать приложение.\n\nОшибка: {e}\n\nПроверьте настройки и доступность БД.")
            root.destroy()
            sys.exit(1)

    root.after(100, start_application)
    root.mainloop()
