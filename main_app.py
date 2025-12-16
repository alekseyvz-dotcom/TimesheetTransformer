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

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8",
)
logging.debug("=== main_app запущен ===")


# --- ИМПОРТ ВСЕХ МОДУЛЕЙ ПРИЛОЖЕНИЯ ---

BudgetAnalyzer = None
_assets_logo = None
_LOGO_BASE64 = None
SpecialOrders = None
meals_module = None
meals_employees_module = None
objects = None
Settings = None
timesheet_module = None
analytics_module = None
timesheet_transformer = None
employees_module = None
timesheet_compare = None

def perform_heavy_imports():
    global BudgetAnalyzer, _assets_logo, _LOGO_BASE64, SpecialOrders, \
           meals_module, objects, Settings, timesheet_module, \
           analytics_module, timesheet_transformer, employees_module, \
           timesheet_compare, meals_employees_module
           
    import BudgetAnalyzer
    import assets_logo as _assets_logo
    _LOGO_BASE64 = getattr(_assets_logo, "LOGO_BASE64", None)
    import SpecialOrders
    import meals_module
    import objects
    import settings_manager as Settings
    import timesheet_module 
    import analytics_module
    import employees as employees_module
    import timesheet_compare
    import meals_employees as meals_employees_module

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

# --- АУТЕНТИФИКАЦИЯ ---

def _hash_password(password: str, salt: Optional[bytes] = None) -> str:
    """Хеширует пароль с использованием PBKDF2."""
    if salt is None:
        salt = _os.urandom(16)
    iterations = 260000
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${dk.hex()}"

def _verify_password(password: str, stored_hash: str) -> bool:
    """Проверяет пароль по хешу."""
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
    """Проверяет логин/пароль в таблице app_users."""
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

# --- ГРАФИЧЕСКИЙ ИНТЕРФЕЙС ---

def embedded_logo_image(parent, max_w=360, max_h=160):
    """
    Загружает логотип из встроенной переменной _LOGO_BASE64.
    Если переменная не найдена, использует крошечную заглушку.
    """
    # Используем либо найденный логотип, либо заглушку, если импорт провалился
    b64 = _LOGO_BASE64 or TINY_PNG_BASE64 

    if Image and ImageTk:
        try:
            raw = base64.b64decode(b64.strip())
            im = Image.open(BytesIO(raw))
            im.thumbnail((max_w, max_h), Image.LANCZOS)
            return ImageTk.PhotoImage(im, master=parent)
        except Exception as e:
            logging.error(f"Ошибка загрузки логотипа через PIL: {e}")
            # Пытаемся загрузить как обычный PhotoImage на случай, если PIL не справился

    try:
        # Пытаемся напрямую через tkinter, он менее требователен
        ph = tk.PhotoImage(data=b64.strip(), master=parent)
        w, h = ph.width(), ph.height()
        # Масштабирование, если нужно
        if w > max_w or h > max_h:
            k = max(w / max_w, h / max_h, 1)
            k = max(1, int(k))
            ph = ph.subsample(k, k)
        return ph
    except Exception as e:
        logging.error(f"Критическая ошибка загрузки логотипа через tkinter: {e}")
        return None # Если ничего не сработало

class HomePage(tk.Frame):
    """Домашняя страница с логотипом и приветствием."""
    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        center = tk.Frame(self, bg="#f7f7f7")
        center.place(relx=0.5, rely=0.5, anchor="center")
        self.logo_img = embedded_logo_image(center, max_w=360, max_h=360)
        if self.logo_img:
            tk.Label(center, image=self.logo_img, bg="#f7f7f7").pack(anchor="center", pady=(0, 12))
        tk.Label(center, text="Добро пожаловать!", font=("Segoe UI", 18, "bold"), bg="#f7f7f7").pack(anchor="center", pady=(4, 6))
        tk.Label(center, text="Выберите раздел в верхнем меню.", font=("Segoe UI", 10), fg="#444", bg="#f7f7f7").pack(anchor="center")

class LoginPage(tk.Frame):
    """Страница входа в систему."""
    def __init__(self, master, app_ref: "MainApp"):
        super().__init__(master, bg="#f7f7f7")
        self.app_ref = app_ref
        center = tk.Frame(self, bg="#f7f7f7")
        center.place(relx=0.5, rely=0.5, anchor="center")
        tk.Label(center, text="Управление строительством", font=("Segoe UI", 16, "bold"), bg="#f7f7f7").grid(row=0, column=0, columnspan=2, pady=(0, 10))
        tk.Label(center, text="Вход в систему", font=("Segoe UI", 11), fg="#555", bg="#f7f7f7").grid(row=1, column=0, columnspan=2, pady=(0, 15))
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
        if self.winfo_ismapped(): self._on_login()
    def _on_login(self):
        username = self.ent_login.get().strip()
        password = self.ent_pass.get().strip()
        if not username or not password: messagebox.showwarning("Вход", "Укажите логин и пароль.", parent=self); return
        try: user = authenticate_user(username, password)
        except Exception as e: messagebox.showerror("Вход", f"Ошибка при обращении к БД:\n{e}", parent=self); return
        if not user: messagebox.showerror("Вход", "Неверный логин или пароль.", parent=self); return
        self.app_ref.on_login_success(user)
    def _on_exit(self):
        self.app_ref.destroy()
        
class SplashScreen(tk.Toplevel):
    """
    Класс для создания и управления окном-заставкой (splash screen).
    """
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Загрузка...")
        
        # Убираем рамки окна
        self.overrideredirect(True)
        
        width = 450
        height = 250

        # Центрируем окно на экране
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x = (screen_width // 2) - (width // 2)
        y = (screen_height // 2) - (height // 2)
        self.geometry(f'{width}x{height}+{x}+{y}')

        # Дизайн
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

        # Прогресс-бар
        self.progress = ttk.Progressbar(self, mode='indeterminate')
        self.progress.pack(pady=20, padx=40, fill="x")
        self.progress.start(10) # Запускаем анимацию

    def update_status(self, text):
        """Обновляет текст статуса на заставке."""
        self.status_label.config(text=text)
        self.update_idletasks() # Принудительно обновляем GUI

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
        self.header = tk.Frame(self)
        self.header.pack(fill="x", padx=12, pady=(10, 4))
        self.lbl_header_title = tk.Label(self.header, text="", font=("Segoe UI", 16, "bold"))
        self.lbl_header_title.pack(side="left")
        self.lbl_header_hint = tk.Label(self.header, text="", font=("Segoe UI", 10), fg="#555")
        self.lbl_header_hint.pack(side="right")
        self.content = tk.Frame(self, bg="#f7f7f7")
        self.content.pack(fill="both", expand=True)
        footer = tk.Frame(self)
        footer.pack(fill="x", padx=12, pady=(0, 10))
        tk.Label(footer, text="Разработал Алексей Зезюкин, 2025", font=("Segoe UI", 8), fg="#666").pack(side="right")

        self._set_user(None)
        self.show_login()

    def _build_menu(self):
        self._menubar = tk.Menu(self)
        self.config(menu=self._menubar)
        
        self._menubar.add_command(label="Главная", command=self.show_home)

        # === Объектный табель (через новый модуль) ===
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
        m_meals.add_command(label="Работники (питание)", command=lambda: self._show_page("meals_workers", lambda p: meals_employees_module.create_meals_workers_page(p, self)))
        self._menu_meals_settings_index = m_meals.index("end")
        m_meals.add_command(label="Настройки", command=lambda: self._show_page("meals_settings", lambda p: meals_module.create_meals_settings_page(p, self.current_user.get('role'))))
        self._menubar.add_cascade(label="Питание", menu=m_meals)
        self._menu_meals = m_meals

        # === Объекты ===
        m_objects = tk.Menu(self._menubar, tearoff=0)
        m_objects.add_command(label="Создать/Редактировать", command=lambda: self._show_page("object_create", lambda p: objects.ObjectCreatePage(p, self)))
        m_objects.add_command(label="Реестр", command=lambda: self._show_page("objects_registry", lambda p: objects.ObjectsRegistryPage(p, self)))
        self._menubar.add_cascade(label="Объекты", menu=m_objects)
        self._menu_objects = m_objects

        # === АНАЛИТИКА (НОВЫЙ РАЗДЕЛ) ===
        m_analytics = tk.Menu(self._menubar, tearoff=0)
        m_analytics.add_command(label="Операционная аналитика", command=lambda: self._show_page("analytics_dashboard", lambda p: analytics_module.AnalyticsPage(p, self)))
        self._menubar.add_cascade(label="Аналитика", menu=m_analytics)
        self._menu_analytics = m_analytics

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
        
        self._menu_settings_index = self._menubar.index("end")
        self._menubar.add_command(label="Настройки", command=lambda: Settings.open_settings_window(self))

    def _set_user(self, user: Optional[Dict[str, Any]]):
        """Обновляет состояние приложения при входе/выходе пользователя."""
        self.current_user = user or {}
        self.is_authenticated = bool(user)
        caption = f" — {user.get('full_name') or user.get('username')}" if user else ""
        self.title(APP_NAME + caption)
        self._apply_role_visibility()

    def on_login_success(self, user: Dict[str, Any]):
        logging.debug(f"MainApp.on_login_success: {user!r}")
        self._set_user(user)
        self.show_home()

    def show_home(self):
        self._show_page("home", lambda p: HomePage(p))

    def show_login(self):
        self._set_user(None)
        self._show_page("login", lambda p: LoginPage(p, self))

    def _show_page(self, key: str, builder):
        """Отображает нужную страницу, создавая ее при необходимости."""
        if not self.is_authenticated and key != "login":
            self.show_login()
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
            "meals_workers": ("Работники (питание)", "История питания по сотруднику"),
            "meals_settings": ("Настройки питания", ""),
            "object_create": ("Объекты: Создание/Редактирование", ""),
            "objects_registry": ("Реестр объектов", ""),
            "budget": ("Анализ смет", ""),
            "login": ("Управление строительством", "Вход в систему"),
            "analytics_dashboard": (
                "Операционная аналитика",
                "Сводные показатели по ключевым метрикам",
            ),
        }
        title, hint = headers.get(key, (key.replace("_", " ").title(), ""))
        self._set_header(title, hint)

        for w in self.content.winfo_children(): w.destroy()
        try:
            page = builder(self.content)
            page.pack(fill="both", expand=True)
            self._pages[key] = page
        except Exception as e:
            # Логируем полную трассировку в файл
            logging.exception(f"Ошибка при открытии страницы '{key}'")
            # И показываем пользователю краткое сообщение
            messagebox.showerror("Ошибка", f"Не удалось открыть страницу '{key}':\n{e}")
            if self.is_authenticated:
                self.show_home()
            else:
                self.show_login()
            
    def _set_header(self, title: str, hint: str = ""):
        """Обновляет заголовок над содержимым."""
        self.lbl_header_title.config(text=title)
        self.lbl_header_hint.config(text=hint or "")

    def _apply_role_visibility(self):
        """
        Настраивает видимость пунктов меню в зависимости от роли пользователя.
        Использует названия пунктов меню (label) вместо индексов для надежности.
        """
        role = self.current_user.get("role", "specialist") # по умолчанию 'specialist'
        is_admin = (role == "admin")
        is_manager = (role in ("admin", "manager"))
        is_planner = (role in ("admin", "planner", "manager"))

        # Вспомогательная функция для упрощения кода
        def set_state(menu, label_text, condition):
            if not menu: return
            try:
                # Находим индекс по тексту
                idx = menu.index(label_text)
                # Устанавливаем состояние
                menu.entryconfig(idx, state="normal" if condition else "disabled")
            except tk.TclError:
                # Игнорируем ошибку, если пункт меню с таким label не найден
                pass

        # --- Настройка меню "Объектный табель" ---
        # "Создать" и "Мои табели" доступны всем авторизованным пользователям.
        set_state(self._menu_timesheets, "Создать", True)
        set_state(self._menu_timesheets, "Мои табели", True)
        # "Реестр табелей" доступен только менеджерам и администраторам.
        set_state(self._menu_timesheets, "Реестр табелей", is_manager)
        set_state(self._menu_timesheets, "Работники", True)

        # --- Настройка меню "Автотранспорт" ---
        # "Создать заявку" и "Мои заявки" доступны всем.
        set_state(self._menu_transport, "Создать заявку", True)
        set_state(self._menu_transport, "Мои заявки", True)
        # "Планирование" и "Реестр" доступны планировщикам, менеджерам и админам.
        set_state(self._menu_transport, "Планирование", is_planner)
        set_state(self._menu_transport, "Реестр", is_planner)

        # --- Настройка меню "Питание" ---
        # "Создать заявку" и "Мои заявки" доступны всем.
        set_state(self._menu_meals, "Создать заявку", True)
        set_state(self._menu_meals, "Мои заявки", True)
        # "Планирование" доступно планировщикам, менеджерам и админам.
        set_state(self._menu_meals, "Планирование", is_planner)
        set_state(self._menu_meals, "Реестр", is_planner)
        set_state(self._menu_meals, "Работники (питание)", is_planner)
        # "Настройки" доступны только администратору.
        set_state(self._menu_meals, "Настройки", is_admin)

        # --- Настройка меню "Объекты" ---
        # "Создавать/Редактировать" объекты могут менеджеры и админы.
        set_state(self._menu_objects, "Создать/Редактировать", is_manager)
        # "Реестр" объектов доступен всем (для просмотра и выбора).
        set_state(self._menu_objects, "Реестр", True)

        # --- Настройка меню "Аналитика" ---
        set_state(self._menubar, "Аналитика", is_manager)

        # --- Настройка корневого меню ---
        # Главный пункт "Настройки" доступен только администратору.
        set_state(self._menubar, "Настройки", is_admin)

    def destroy(self):
        """Корректное завершение работы приложения."""
        logging.info("Приложение закрывается. Закрываем пул соединений.")
        close_db_pool()
        super().destroy()

# --- ТОЧКА ВХОДА ПРИЛОЖЕНИЯ ---

if __name__ == "__main__":
    # 1. Создаем временное корневое окно и сразу его скрываем.
    # Оно нужно, чтобы наша заставка (Toplevel) могла существовать.
    root = tk.Tk()
    root.withdraw()

    # 2. Создаем и отображаем заставку
    splash = SplashScreen(root)
    
    # 3. Определяем функцию, которая выполнит всю тяжелую работу
    def start_application():
        try:
            # Обновляем статус на заставке
            splash.update_status("Загрузка модулей приложения...")
            perform_heavy_imports() # Выполняем отложенные импорты
            
            splash.update_status("Проверка конфигурации...")
            Settings.ensure_config()

            splash.update_status("Подключение к базе данных...")
            initialize_db_pool()

            splash.update_status("Передача настроек в модули...")
            modules_to_init = [
                meals_module,
                SpecialOrders,
                objects,
                Settings,
                timesheet_module,
                analytics_module,
                employees_module,
                timesheet_compare,
                meals_employees_module, 
            ]
            for module in modules_to_init:
                if module and hasattr(module, "set_db_pool"):
                    module.set_db_pool(db_connection_pool)

            # Вся инициализация прошла успешно.
            # Уничтожаем заставку.
            splash.destroy()

            # Уничтожаем временное скрытое окно.
            root.destroy()
            
            # И запускаем наше настоящее главное приложение!
            logging.debug("Инициализация успешна. Запускаем главный цикл приложения.")
            app = MainApp()
            app.protocol("WM_DELETE_WINDOW", app.destroy)
            app.mainloop()

        except Exception as e:
            # Если на этапе инициализации произошла ошибка
            logging.critical("Приложение не может быть запущено из-за ошибки инициализации.", exc_info=True)
            splash.destroy() # Сначала убираем заставку
            messagebox.showerror("Критическая ошибка", f"Не удалось инициализировать приложение.\n\nОшибка: {e}\n\nПроверьте настройки и доступность БД.")
            root.destroy() # Закрываем временное окно
            sys.exit(1)

    # 4. Запускаем тяжелую функцию с небольшой задержкой (например, 100 мс).
    # Это дает tkinter время, чтобы гарантированно отрисовать окно заставки.
    root.after(100, start_application)
    
    # 5. Запускаем главный цикл для временного окна. 
    # Он будет работать, пока мы не запустим основной app.mainloop() или не закроем все по ошибке.
    root.mainloop()
