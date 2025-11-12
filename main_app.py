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
from io import BytesIO
from datetime import datetime
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
    import SpecialOrders  # должен содержать open_special_orders(parent)
except Exception:
    SpecialOrders = None
try:
    import timesheet_transformer  # должен содержать open_converter(parent)
except Exception:
    timesheet_transformer = None
try:
    import settings_manager as Settings
except Exception:
    Settings = None
try:
    import db
except Exception:
    db = None
    
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
CONFIG_SECTION_REMOTE = "Remote"
KEY_SPR = "spravochnik_path"
KEY_OUTPUT_DIR = "output_dir"
KEY_EXPORT_PWD = "export_password"
KEY_PLANNING_PASSWORD = "planning_password"
KEY_SELECTED_DEP = "selected_department"
KEY_REMOTE_USE = "use_remote"
KEY_YA_PUBLIC_LINK = "yadisk_public_link"
KEY_YA_PUBLIC_PATH = "yadisk_public_path"
SPRAVOCHNIK_FILE_DEFAULT = "Справочник.xlsx"
OUTPUT_DIR_DEFAULT = "Объектные_табели"
RAW_LOGO_URL = "https://raw.githubusercontent.com/alekseyvz-dotcom/TimesheetTransformer/main/logo.png"
TINY_PNG_BASE64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/w8AAn8B9w3G2kIAAAAASUVORK5CYII="
)

if Settings:
    ensure_config = Settings.ensure_config
    read_config = Settings.read_config
    write_config = Settings.write_config

    get_spr_path_from_config = Settings.get_spr_path_from_config
    get_output_dir_from_config = Settings.get_output_dir_from_config
    get_export_password_from_config = Settings.get_export_password_from_config

    get_selected_department_from_config = Settings.get_selected_department_from_config
    set_selected_department_in_config = Settings.set_selected_department_in_config

# ------------- БАЗОВЫЕ УТИЛИТЫ И КОНФИГУРАЦИЯ -------------

def exe_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

def config_path() -> Path:
    return exe_dir() / CONFIG_FILE

# ОСТАЛЬНЫЕ ФУНКЦИИ КОНФИГА ОБЪЯВЛЯЕМ ТОЛЬКО ЕСЛИ Settings НЕ ДОСТУПЕН
if not Settings:
    def ensure_config():
        cp = config_path()
        if cp.exists():
            cfg = configparser.ConfigParser()
            cfg.read(cp, encoding="utf-8")
            changed = False
            if not cfg.has_section(CONFIG_SECTION_PATHS): cfg[CONFIG_SECTION_PATHS] = {}; changed = True
            if KEY_SPR not in cfg[CONFIG_SECTION_PATHS]: cfg[CONFIG_SECTION_PATHS][KEY_SPR] = str(exe_dir() / SPRAVOCHNIK_FILE_DEFAULT); changed = True
            if KEY_OUTPUT_DIR not in cfg[CONFIG_SECTION_PATHS]: cfg[CONFIG_SECTION_PATHS][KEY_OUTPUT_DIR] = str(exe_dir() / OUTPUT_DIR_DEFAULT); changed = True
            if not cfg.has_section(CONFIG_SECTION_UI): cfg[CONFIG_SECTION_UI] = {}; changed = True
            if KEY_SELECTED_DEP not in cfg[CONFIG_SECTION_UI]: cfg[CONFIG_SECTION_UI][KEY_SELECTED_DEP] = "Все"; changed = True
            if not cfg.has_section(CONFIG_SECTION_INTEGR): cfg[CONFIG_SECTION_INTEGR] = {}; changed = True
            if KEY_EXPORT_PWD not in cfg[CONFIG_SECTION_INTEGR]: cfg[CONFIG_SECTION_INTEGR][KEY_EXPORT_PWD] = "2025"; changed = True
            if KEY_PLANNING_PASSWORD not in cfg[CONFIG_SECTION_INTEGR]: cfg[CONFIG_SECTION_INTEGR][KEY_PLANNING_PASSWORD] = "2025"; changed = True
            if not cfg.has_section(CONFIG_SECTION_REMOTE): cfg[CONFIG_SECTION_REMOTE] = {}; changed = True
            if KEY_REMOTE_USE not in cfg[CONFIG_SECTION_REMOTE]: cfg[CONFIG_SECTION_REMOTE][KEY_REMOTE_USE] = "false"; changed = True
            if KEY_YA_PUBLIC_LINK not in cfg[CONFIG_SECTION_REMOTE]: cfg[CONFIG_SECTION_REMOTE][KEY_YA_PUBLIC_LINK] = ""; changed = True
            if KEY_YA_PUBLIC_PATH not in cfg[CONFIG_SECTION_REMOTE]: cfg[CONFIG_SECTION_REMOTE][KEY_YA_PUBLIC_PATH] = ""; changed = True
            if not cfg.has_section("Orders"): cfg["Orders"] = {}; changed = True
            if "cutoff_enabled" not in cfg["Orders"]: cfg["Orders"]["cutoff_enabled"] = "false"; changed = True
            if "cutoff_hour" not in cfg["Orders"]: cfg["Orders"]["cutoff_hour"] = "13"; changed = True
            if changed:
                with open(cp, "w", encoding="utf-8") as f:
                    cfg.write(f)
            return

        cfg = configparser.ConfigParser()
        cfg[CONFIG_SECTION_PATHS] = {
            KEY_SPR: str(exe_dir() / SPRAVOCHNIK_FILE_DEFAULT),
            KEY_OUTPUT_DIR: str(exe_dir() / OUTPUT_DIR_DEFAULT),
        }
        cfg[CONFIG_SECTION_UI] = {KEY_SELECTED_DEP: "Все"}
        cfg[CONFIG_SECTION_INTEGR] = {KEY_EXPORT_PWD: "2025", KEY_PLANNING_PASSWORD: "2025"}
        cfg[CONFIG_SECTION_REMOTE] = {KEY_REMOTE_USE: "false", KEY_YA_PUBLIC_LINK: "", KEY_YA_PUBLIC_PATH: ""}
        cfg["Orders"] = {"cutoff_enabled": "false", "cutoff_hour": "13"}
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

    def get_spr_path_from_config() -> Path:
        cfg = read_config()
        raw = cfg.get(CONFIG_SECTION_PATHS, KEY_SPR, fallback=str(exe_dir() / SPRAVOCHNIK_FILE_DEFAULT))
        return Path(os.path.expandvars(raw))

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
            import urllib.request
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

    try:
        import db
        db.init_db()
    except Exception as e:
        print(f"[DB init] {e}")

# ------------- УДАЛЕННЫЙ СПРАВОЧНИК И ДРУГИЕ УТИЛИТЫ -------------

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

def load_spravochnik_from_wb(wb) -> Tuple[List[Tuple[str,str,str,str]], List[Tuple[str,str]]]:
    employees: List[Tuple[str,str,str,str]] = []
    objects:   List[Tuple[str,str]] = []

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

    return employees, objects

def ensure_spravochnik_local(path: Path):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    if path.exists():
        return
    wb = Workbook()
    ws1 = wb.active
    ws1.title = "Сотрудники"
    ws1.append(["ФИО", "Табельный №", "Должность", "Подразделение"])
    ws1.append(["Иванов И. И.", "ST00-00001", "Слесарь", "Монтаж"])
    ws2 = wb.create_sheet("Объекты")
    ws2.append(["ID объекта", "Адрес"])
    ws2.append(["OBJ-001", "ул. Пушкина, д. 1"])
    ws2.append(["OBJ-002", "пр. Строителей, 25"])
    wb.save(path)

def load_spravochnik_remote_or_local(local_path: Path) -> Tuple[List[Tuple[str,str,str,str]], List[Tuple[str,str]]]:
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
            return [], []  # НЕ создаём файл, возвращаем пустые данные

    # Локальный режим — допускаем автосоздание
    ensure_spravochnik_local(local_path)
    wb = load_workbook(local_path, read_only=True, data_only=True)
    return load_spravochnik_from_wb(wb)

# ------------- Утилиты для работы со временем и данными -------------

def month_days(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]

def month_name_ru(month: int) -> str:
    names = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
    return names[month-1]

def parse_hours_value(v: Any) -> Optional[float]:
    """
    Парсит часы БЕЗ переработки.
    Форматы: 8 | 8,25 | 8:30 | 1/7 | 8,25(6/2) <- из последнего берёт только 8,25
    """
    s = str(v or "").strip()
    if not s:
        return None
    
    # Убираем переработку в скобках для базового парсинга
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
        except:
            pass
    
    s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return None
        
def parse_overtime(v: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    Извлекает переработку из формата: 8,25(6/2)
    Возвращает: (дневная_переработка, ночная_переработка)
    """
    s = str(v or "").strip()
    if "(" not in s or ")" not in s:
        return None, None
    
    try:
        # Извлекаем содержимое скобок
        start = s.index("(")
        end = s.index(")")
        overtime_str = s[start + 1:end].strip()
        
        if "/" in overtime_str:
            parts = overtime_str.split("/")
            day_ot = float(parts[0].replace(",", ".")) if parts[0].strip() else 0.0
            night_ot = float(parts[1].replace(",", ".")) if len(parts) > 1 and parts[1].strip() else 0.0
            return day_ot, night_ot
        else:
            # Если нет дроби — считаем дневной переработкой
            ot = float(overtime_str.replace(",", "."))
            return ot, 0.0
    except:
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

# ------------- Ряд реестра (RowWidget) и Диалоги (Сохраненный код) -------------

class RowWidget:
    WEEK_BG_SAT = "#fff8e1"
    WEEK_BG_SUN = "#ffebee"
    ZEBRA_EVEN = "#ffffff"
    ZEBRA_ODD = "#f6f8fa"
    ERR_BG = "#ffccbc"
    DISABLED_BG = "#f0f0f0"

    def __init__(self, table: tk.Frame, row_index: int, fio: str, tbn: str,
                 get_year_month_callable, on_delete_callable):
        self.table = table
        self.row = row_index
        self.get_year_month = get_year_month_callable
        self.on_delete = on_delete_callable
        self._suspend_sync = False

        zebra_bg = self.ZEBRA_EVEN if (row_index % 2 == 0) else self.ZEBRA_ODD
        self.widgets: List[tk.Widget] = []

        # ФИО
        self.lbl_fio = tk.Label(self.table, text=fio, anchor="w", bg=zebra_bg)
        self.lbl_fio.grid(row=self.row, column=0, padx=0, pady=1, sticky="nsew")
        self.widgets.append(self.lbl_fio)

        # Таб.№
        self.lbl_tbn = tk.Label(self.table, text=tbn, anchor="center", bg=zebra_bg)
        self.lbl_tbn.grid(row=self.row, column=1, padx=0, pady=1, sticky="nsew")
        self.widgets.append(self.lbl_tbn)

        # Дни месяца (col 2..32)
        self.day_entries: List[tk.Entry] = []
        for d in range(1, 32):
            e = tk.Entry(self.table, width=4, justify="center", relief="solid", bd=1)
            e.grid(row=self.row, column=1 + d, padx=0, pady=1, sticky="nsew")
            e.bind("<FocusOut>", lambda ev, _d=d: self.update_total())
            e.bind("<Button-2>", lambda ev: "break")
            e.bind("<ButtonRelease-2>", lambda ev: "break")
            self.day_entries.append(e)
            self.widgets.append(e)

        # Итоги
        self.lbl_days = tk.Label(self.table, text="0", anchor="e", bg=zebra_bg)
        self.lbl_days.grid(row=self.row, column=33, padx=(4, 1), pady=1, sticky="nsew")
        self.widgets.append(self.lbl_days)

        self.lbl_total = tk.Label(self.table, text="0", anchor="e", bg=zebra_bg)
        self.lbl_total.grid(row=self.row, column=34, padx=(4, 1), pady=1, sticky="nsew")
        self.widgets.append(self.lbl_total)

        # НОВЫЕ МЕТКИ ДЛЯ ПЕРЕРАБОТКИ
        self.lbl_overtime_day = tk.Label(self.table, text="0", anchor="e", bg=zebra_bg)
        self.lbl_overtime_day.grid(row=self.row, column=35, padx=(4, 1), pady=1, sticky="nsew")
        self.widgets.append(self.lbl_overtime_day)

        self.lbl_overtime_night = tk.Label(self.table, text="0", anchor="e", bg=zebra_bg)
        self.lbl_overtime_night.grid(row=self.row, column=36, padx=(4, 1), pady=1, sticky="nsew")
        self.widgets.append(self.lbl_overtime_night)

        # 5/2
        self.btn_52 = ttk.Button(self.table, text="5/2", width=4, command=self.fill_52)
        self.btn_52.grid(row=self.row, column=37, padx=1, pady=0, sticky="nsew")
        self.widgets.append(self.btn_52)

        # Удалить
        self.btn_del = ttk.Button(self.table, text="Удалить", width=7, command=self.delete_row)
        self.btn_del.grid(row=self.row, column=38, padx=1, pady=0, sticky="nsew")
        self.widgets.append(self.btn_del)

    def apply_pixel_column_widths(self, _px: dict):
        return

    def set_day_font(self, font_tuple):
        for e in self.day_entries:
            e.configure(font=font_tuple)

    def regrid_to(self, new_row: int):
        self.row = new_row
        self.lbl_fio.grid_configure(row=new_row, column=0)
        self.lbl_tbn.grid_configure(row=new_row, column=1)
        for i, e in enumerate(self.day_entries, start=2):
            e.grid_configure(row=new_row, column=i)
        self.lbl_days.grid_configure(row=new_row, column=33)
        self.lbl_total.grid_configure(row=new_row, column=34)
        self.lbl_overtime_day.grid_configure(row=new_row, column=35)
        self.lbl_overtime_night.grid_configure(row=new_row, column=36)
        self.btn_52.grid_configure(row=new_row, column=37)
        self.btn_del.grid_configure(row=new_row, column=38)

    def destroy(self):
        for w in self.widgets:
            try:
                w.destroy()
            except Exception:
                pass
        self.widgets.clear()

    def fio(self) -> str:
        return self.lbl_fio.cget("text")

    def tbn(self) -> str:
        return self.lbl_tbn.cget("text")

    def set_hours(self, arr: List[Optional[str]]):
        """Принимает массив строк вида '8,25(6/2)' или просто '8'"""
        days = len(arr)
        for i in range(31):
            self.day_entries[i].delete(0, "end")
            if i < days and arr[i]:
                self.day_entries[i].insert(0, str(arr[i]))
        self.update_total()

    def get_hours(self) -> List[Optional[float]]:
        """Возвращает только базовые часы (без переработки)"""
        return [parse_hours_value(e.get().strip()) for e in self.day_entries]

    def get_hours_with_overtime(self) -> List[Tuple[Optional[float], Optional[float], Optional[float]]]:
        """
        Возвращает: [(часы, переработка_день, переработка_ночь), ...]
        """
        result = []
        for e in self.day_entries:
            raw = e.get().strip()
            hours = parse_hours_value(raw) if raw else None
            day_ot, night_ot = parse_overtime(raw) if raw else (None, None)
            result.append((hours, day_ot, night_ot))
        return result

    def _bg_for_day(self, year: int, month: int, day: int) -> str:
        wd = datetime(year, month, day).weekday()
        if wd == 5:
            return self.WEEK_BG_SAT
        if wd == 6:
            return self.WEEK_BG_SUN
        return "white"

    def _repaint_day_cell(self, i0: int, year: int, month: int):
        day = i0 + 1
        e = self.day_entries[i0]
        days = month_days(year, month)
        
        if day > days:
            e.configure(state="disabled", disabledbackground=self.DISABLED_BG)
            e.delete(0, "end")
            return
        
        e.configure(state="normal")
        raw = e.get().strip()
        
        # Проверка корректности формата
        invalid = False
        if raw:
            val = parse_hours_value(raw)
            if val is None or val < 0 or val > 24:
                invalid = True
            
            # Проверка переработки
            if "(" in raw:
                day_ot, night_ot = parse_overtime(raw)
                if day_ot is None and night_ot is None:
                    invalid = True
        
        if invalid:
            e.configure(bg=self.ERR_BG)
        else:
            e.configure(bg=self._bg_for_day(year, month, day))

    def update_days_enabled(self, year: int, month: int):
        for i in range(31):
            self._repaint_day_cell(i, year, month)
        self.update_total()

    def update_total(self):
        # Если страница сообщает о массовой операции — пропускаем тяжелые перекраски,
        # но все равно посчитаем итоги быстро.
        parent_page = None
        try:
            # self.table — это frame таблицы на странице TimesheetPage
            parent_page = self.table.master.master.master  # аккуратно: Table -> Canvas window -> main_frame -> Page
        except Exception:
            parent_page = None

        fast_mode = False
        try:
            fast_mode = bool(getattr(parent_page, "_mass_add_in_progress", False))
        except Exception:
            fast_mode = False
            
        total_hours = 0.0
        total_days = 0
        total_overtime_day = 0.0
        total_overtime_night = 0.0
        
        y, m = self.get_year_month()
        days_in_m = month_days(y, m)
        
        for i, e in enumerate(self.day_entries, start=1):
            raw = e.get().strip()
            if not fast_mode:
                self._repaint_day_cell(i - 1, y, m)
            
            if i <= days_in_m and raw:
                hours = parse_hours_value(raw)
                day_ot, night_ot = parse_overtime(raw)
                
                if isinstance(hours, (int, float)) and hours > 1e-12:
                    total_hours += float(hours)
                    total_days += 1
                
                if isinstance(day_ot, (int, float)):
                    total_overtime_day += float(day_ot)
                if isinstance(night_ot, (int, float)):
                    total_overtime_night += float(night_ot)
        
        self.lbl_days.config(text=str(total_days))
        
        sh = f"{total_hours:.2f}".rstrip("0").rstrip(".")
        self.lbl_total.config(text=sh)
        
        sod = f"{total_overtime_day:.2f}".rstrip("0").rstrip(".")
        self.lbl_overtime_day.config(text=sod)
        
        son = f"{total_overtime_night:.2f}".rstrip("0").rstrip(".")
        self.lbl_overtime_night.config(text=son)

    def fill_52(self):
        y, m = self.get_year_month()
        days = month_days(y, m)
        for d in range(1, days + 1):
            wd = datetime(y, m, d).weekday()
            e = self.day_entries[d - 1]
            e.delete(0, "end")
            if wd < 4:
                e.insert(0, "8,25")
            elif wd == 4:
                e.insert(0, "7")
        for d in range(days + 1, 32):
            self.day_entries[d - 1].delete(0, "end")
        self.update_total()

    def delete_row(self):
        self.on_delete(self)

# ------------- Диалоги и прочее (Сохраненный код) -------------

class CopyFromDialog(simpledialog.Dialog):
    def __init__(self, parent, init_year: int, init_month: int):
        self.init_year = init_year
        self.init_month = init_month
        self.result = None
        super().__init__(parent, title="Копировать сотрудников из месяца")

    def body(self, master):
        tk.Label(master, text="Источник").grid(row=0, column=0, sticky="w", pady=(2, 6), columnspan=4)

        tk.Label(master, text="Месяц:").grid(row=1, column=0, sticky="e")
        self.cmb_month = ttk.Combobox(master, state="readonly", width=18,
                                      values=[month_name_ru(i) for i in range(1, 13)])
        self.cmb_month.grid(row=1, column=1, sticky="w")
        self.cmb_month.current(max(0, min(11, self.init_month - 1)))

        tk.Label(master, text="Год:").grid(row=1, column=2, sticky="e", padx=(10, 4))
        self.spn_year = tk.Spinbox(master, from_=2000, to=2100, width=6)
        self.spn_year.grid(row=1, column=3, sticky="w")
        self.spn_year.delete(0, "end")
        self.spn_year.insert(0, str(self.init_year))

        self.var_copy_hours = tk.BooleanVar(value=False)
        ttk.Checkbutton(master, text="Копировать часы", variable=self.var_copy_hours)\
            .grid(row=2, column=1, sticky="w", pady=(8, 2))

        tk.Label(master, text="Режим:").grid(row=3, column=0, sticky="e", pady=(6, 2))
        self.var_mode = tk.StringVar(value="replace")
        frame_mode = tk.Frame(master)
        frame_mode.grid(row=3, column=1, columnspan=3, sticky="w", pady=(6, 2))
        ttk.Radiobutton(frame_mode, text="Заменить текущий список", value="replace", variable=self.var_mode)\
            .pack(anchor="w")
        ttk.Radiobutton(frame_mode, text="Объединить (добавить недостающих)", value="merge", variable=self.var_mode)\
            .pack(anchor="w")
        return self.cmb_month

    def validate(self):
        try:
            y = int(self.spn_year.get())
            if not (2000 <= y <= 2100):
                raise ValueError
            return True
        except Exception:
            messagebox.showwarning("Копирование", "Введите корректный год (2000–2100).")
            return False

    def apply(self):
        self.result = {
            "year": int(self.spn_year.get()),
            "month": self.cmb_month.current() + 1,
            "with_hours": bool(self.var_copy_hours.get()),
            "mode": self.var_mode.get(),
        }

class BatchAddDialog(tk.Toplevel):
    def __init__(self, parent, total: int, title: str = "Добавление сотрудников"):
        super().__init__(parent)
        self.parent = parent
        self.total = max(1, int(total))
        self.done = 0
        self.cancelled = False
        self.title(title)
        self.resizable(False, False)
        self.grab_set()

        frm = tk.Frame(self, padx=12, pady=12)
        frm.pack(fill="both", expand=True)

        self.lbl = tk.Label(frm, text=f"Добавлено: 0 из {self.total}")
        self.lbl.pack(fill="x")

        self.pb = ttk.Progressbar(frm, mode="determinate", maximum=self.total, length=420)
        self.pb.pack(fill="x", pady=(8, 8))

        self.btn_cancel = ttk.Button(frm, text="Отмена", command=self._on_cancel)
        self.btn_cancel.pack(anchor="e", pady=(6, 0))

        # позиционируем по центру родителя
        try:
            self.update_idletasks()
            px = parent.winfo_rootx()
            py = parent.winfo_rooty()
            pw = parent.winfo_width()
            ph = parent.winfo_height()
            sw = self.winfo_width()
            sh = self.winfo_height()
            self.geometry(f"+{px + (pw - sw)//2}+{py + (ph - sh)//2}")
        except Exception:
            pass

    def step(self, n: int = 1):
        if self.cancelled:
            return
        self.done += n
        if self.done > self.total:
            self.done = self.total
        self.pb['value'] = self.done
        self.lbl.config(text=f"Добавлено: {self.done} из {self.total}")
        self.update_idletasks()

    def _on_cancel(self):
        self.cancelled = True

    def close(self):
        try:
            self.grab_release()
        except Exception:
            pass
        self.destroy()

class HoursFillDialog(simpledialog.Dialog):
    def __init__(self, parent, max_day: int):
        self.max_day = max_day
        self.result = None
        super().__init__(parent, title="Проставить часы всем")

    def body(self, master):
        tk.Label(master, text=f"В текущем месяце дней: {self.max_day}").grid(row=0, column=0, columnspan=3, sticky="w", pady=(2, 6))
        tk.Label(master, text="День:").grid(row=1, column=0, sticky="e")
        self.spn_day = tk.Spinbox(master, from_=1, to=31, width=4)
        self.spn_day.grid(row=1, column=1, sticky="w")
        self.spn_day.delete(0, "end")
        self.spn_day.insert(0, "1")

        self.var_clear = tk.BooleanVar(value=False)
        ttk.Checkbutton(master, text="Очистить день (пусто)", variable=self.var_clear, command=self._on_toggle_clear)\
            .grid(row=2, column=1, sticky="w", pady=(6, 2))

        tk.Label(master, text="Часы:").grid(row=3, column=0, sticky="e", pady=(6, 0))
        self.ent_hours = ttk.Entry(master, width=12)
        self.ent_hours.grid(row=3, column=1, sticky="w", pady=(6, 0))
        self.ent_hours.insert(0, "8")

        tk.Label(master, text="Форматы: 8 | 8,25 | 8:30 | 1/7").grid(row=4, column=0, columnspan=3, sticky="w", pady=(6, 2))
        return self.spn_day

    def _on_toggle_clear(self):
        if self.var_clear.get():
            self.ent_hours.configure(state="disabled")
        else:
            self.ent_hours.configure(state="normal")

    def validate(self):
        try:
            d = int(self.spn_day.get())
            if not (1 <= d <= 31):
                raise ValueError
        except Exception:
            messagebox.showwarning("Проставить часы", "День должен быть числом от 1 до 31.")
            return False

        if self.var_clear.get():
            self._d = d
            self._h = 0.0
            self._clear = True
            return True

        hv = parse_hours_value(self.ent_hours.get().strip())
        if hv is None or hv < 0:
            messagebox.showwarning("Проставить часы", "Введите корректное значение часов (например, 8, 8:30, 1/7).")
            return False
        self._d = d
        self._h = float(hv)
        self._clear = False
        return True

    def apply(self):
        self.result = {
            "day": self._d,
            "hours": self._h,
            "clear": self._clear,
        }

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
        if event.keysym in ("Up", "Down", "Left", "Right", "Home", "End", "Return", "Escape", "Tab"):
            return
        typed = self.get().strip()
        if not typed:
            self["values"] = self._all_values
            return
        self["values"] = [x for x in self._all_values if typed.lower() in x.lower()]

# ------------- Диалоги экспорта -------------

class ExportMonthDialog(simpledialog.Dialog):
    def __init__(self, parent):
        self.result = None
        super().__init__(parent, title="Сводный экспорт по месяцу")

    def body(self, master):
        now = datetime.now()
        tk.Label(master, text="Месяц:").grid(row=0, column=0, sticky="e")
        self.cmb_month = ttk.Combobox(master, state="readonly", width=18,
                                      values=[month_name_ru(i) for i in range(1, 13)])
        self.cmb_month.grid(row=0, column=1, sticky="w")
        self.cmb_month.current(now.month - 1)

        tk.Label(master, text="Год:").grid(row=0, column=2, sticky="e", padx=(10, 4))
        self.spn_year = tk.Spinbox(master, from_=2000, to=2100, width=6)
        self.spn_year.grid(row=0, column=3, sticky="w")
        self.spn_year.delete(0, "end")
        self.spn_year.insert(0, str(now.year))

        tk.Label(master, text="Формат:").grid(row=1, column=0, sticky="e", pady=(8, 0))
        self.var_fmt = tk.StringVar(value="both")
        fmtf = tk.Frame(master)
        fmtf.grid(row=1, column=1, columnspan=3, sticky="w", pady=(8, 0))
        ttk.Radiobutton(fmtf, text="XLSX", value="xlsx", variable=self.var_fmt).pack(anchor="w")
        ttk.Radiobutton(fmtf, text="CSV",  value="csv",  variable=self.var_fmt).pack(anchor="w")
        ttk.Radiobutton(fmtf, text="Оба (XLSX+CSV)", value="both", variable=self.var_fmt).pack(anchor="w")
        return self.cmb_month

    def validate(self):
        try:
            y = int(self.spn_year.get())
            if not (2000 <= y <= 2100):
                raise ValueError
            return True
        except Exception:
            messagebox.showwarning("Сводный экспорт", "Введите корректный год (2000–2100).")
            return False

    def apply(self):
        self.result = {
            "year": int(self.spn_year.get()),
            "month": self.cmb_month.current() + 1,
            "fmt": self.var_fmt.get(),
        }

# ------------- Функции экспорта -------------

def perform_summary_export(year: int, month: int, fmt: str) -> Tuple[int, List[Path]]:
    base_out = get_output_dir_from_config()
    base_out.mkdir(parents=True, exist_ok=True)
    pattern = f"Объектный_табель_*_{year}_{month:02d}.xlsx"
    files = list(base_out.glob(pattern))
    rows = []

    for f in files:
        try:
            wb = load_workbook(f, read_only=True, data_only=True)
        except Exception:
            continue
        if "Табель" not in wb.sheetnames:
            continue
        ws = wb["Табель"]
        for r in range(2, ws.max_row + 1):
            row_oid = (ws.cell(r, 1).value or "")
            row_addr = (ws.cell(r, 2).value or "")
            row_m = int(ws.cell(r, 3).value or 0)
            row_y = int(ws.cell(r, 4).value or 0)
            
            if row_m != month or row_y != year:
                continue
                
            fio = (ws.cell(r, 5).value or "")
            tbn = (ws.cell(r, 6).value or "")
            department = (ws.cell(r, 7).value or "")
            
            hours: List[Optional[str]] = []
            for c in range(8, 8 + 31):
                v = ws.cell(r, c).value
                hours.append(str(v) if v else None)
            
            total_days_val = ws.cell(r, 39).value
            total_hours_val = ws.cell(r, 40).value
            overtime_day_val = ws.cell(r, 41).value
            overtime_night_val = ws.cell(r, 42).value
            
            total_days = int(total_days_val) if total_days_val else None
            total_hours = float(total_hours_val) if total_hours_val else None
            overtime_day = float(overtime_day_val) if overtime_day_val else None
            overtime_night = float(overtime_night_val) if overtime_night_val else None
            
            row_values = [row_oid, row_addr, month, year, fio, tbn, department] + hours + [
                total_days, total_hours, overtime_day, overtime_night
            ]
            rows.append(row_values)

    if not rows:
        return 0, []

    sum_dir = exe_dir() / "Сводные_отчеты"
    sum_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []

    hdr = ["ID объекта", "Адрес", "Месяц", "Год", "ФИО", "Табельный №", "Подразделение"] + \
          [str(i) for i in range(1, 32)] + \
          ["Итого дней", "Итого часов по табелю", "Переработка день", "Переработка ночь"]

    if fmt in ("xlsx", "both"):
        wb_out = Workbook()
        ws_out = wb_out.active
        ws_out.title = "Сводный"
        ws_out.append(hdr)
        for rv in rows:
            ws_out.append(rv)
        ws_out.freeze_panes = "A2"
        ws_out.column_dimensions["A"].width = 14
        ws_out.column_dimensions["B"].width = 40
        ws_out.column_dimensions["C"].width = 10
        ws_out.column_dimensions["D"].width = 8
        ws_out.column_dimensions["E"].width = 28
        ws_out.column_dimensions["F"].width = 14
        ws_out.column_dimensions["G"].width = 20
        for i in range(8, 8 + 31):
            ws_out.column_dimensions[get_column_letter(i)].width = 6
        ws_out.column_dimensions[get_column_letter(39)].width = 10
        ws_out.column_dimensions[get_column_letter(40)].width = 18
        ws_out.column_dimensions[get_column_letter(41)].width = 14
        ws_out.column_dimensions[get_column_letter(42)].width = 14
        p = sum_dir / f"Сводный_{year}_{month:02d}.xlsx"
        wb_out.save(p)
        paths.append(p)

    if fmt in ("csv", "both"):
        p = sum_dir / f"Сводный_{year}_{month:02d}.csv"
        with open(p, "w", encoding="utf-8-sig", newline="") as fcsv:
            writer = csv.writer(fcsv, delimiter=";")
            writer.writerow(hdr)
            for rv in rows:
                writer.writerow(rv)
        paths.append(p)

    return len(rows), paths

# ------------- СТРАНИЦЫ И АСИНХРОННАЯ ЗАГРУЗКА -------------

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

        tk.Label(center, text="Добро пожаловать!", font=("Segoe UI", 18, "bold"), bg="#f7f7f7")\
            .pack(anchor="center", pady=(4, 6))
        tk.Label(center, text="Выберите раздел в верхнем меню.\nОбъектный табель → Создать — для работы с табелями.",
                 font=("Segoe UI", 10), fg="#444", bg="#f7f7f7", justify="center").pack(anchor="center")

class TimesheetPage(tk.Frame):
    COLPX = {"fio": 200, "tbn": 100, "day": 36, "days": 46, "hours": 56, "btn52": 40, "del": 66}
    MIN_FIO_PX = 140
    MAX_FIO_PX = 260
    HEADER_BG = "#d0d0d0"

    def __init__(self, master):
        super().__init__(master)
        self.base_dir = exe_dir()
        self.spr_path = get_spr_path_from_config()
        self.out_dir = get_output_dir_from_config()
        self.out_dir.mkdir(parents=True, exist_ok=True)

        self.DAY_ENTRY_FONT = ("Segoe UI", 8)
        self._fit_job = None

        self._load_spr_data()
        # ИНИЦИАЛИЗИРУЕМ ПАГИНАЦИЮ И МОДЕЛЬ ДО ПОСТРОЕНИЯ UI
        self.model_rows: List[Dict[str, Any]] = []
        self.current_page = 1
        self.page_size = tk.IntVar(value=50)
        # флаг подавления синхронизации видимых значений в модель
        self._suspend_sync = False

        self._build_ui()
        # Отрисуем первую пустую страницу
        self._render_page(1)
        self._load_existing_rows()

        self.bind("<Configure>", self._on_window_configure)
        self.after(120, self._auto_fit_columns)

    def _load_spr_data(self):
        try:
            employees, objects = load_spravochnik_remote_or_local(self.spr_path)
        except Exception:
            employees, objects = [], []

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
        # Тело
        self.main_canvas.configure(scrollregion=self.main_canvas.bbox("all"))
        # Шапка: обновим ширину области по ширине тела
        try:
            content_bbox = self.main_canvas.bbox("all")
            if content_bbox:
                x1, y1, x2, y2 = content_bbox
                self.header_canvas.configure(scrollregion=(0, 0, x2, 0))
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
        """Загружает строки реестра из БД (Supabase) для выбранного объекта/подразделения/периода"""
        # Очистим модель
        self.model_rows.clear()

        # Проверки входных
        addr = (self.cmb_address.get() or "").strip()
        oid = (self.cmb_object_id.get() or "").strip()
        dep = (self.cmb_department.get() or "Все").strip()
        y, m = self.get_year_month()

        if not addr and not oid:
            # Без указания объекта грузить нечего — просто перерисуем пусто
            self._render_page(1)
            return

        if not db:
            messagebox.showerror("База данных", "Модуль db.py не найден. Проверьте файлы приложения.")
            self._render_page(1)
            return

        try:
            rows = db.load_timesheet(object_code=(oid or None),
                                     object_address=addr,
                                     dep_name=dep,
                                     year=y,
                                     month=m)
            # rows: [{'fio':..., 'tbn':..., 'hours':[...31...]}]
            for r in rows:
                self.model_rows.append({
                    "fio": r.get("fio", ""),
                    "tbn": r.get("tbn", ""),
                    "hours": r.get("hours") or [None] * 31
                })
            self._render_page(1)
        except Exception as e:
            messagebox.showerror("Загрузка из БД", f"Не удалось загрузить табель:\n{e}")
            self._render_page(1)

    def save_all(self):
        """Сохраняет текущий реестр в БД (Supabase)"""
        if not db:
           messagebox.showerror("Сохранение", "Модуль db.py не найден.")
           return

        addr = (self.cmb_address.get() or "").strip()
        oid = (self.cmb_object_id.get() or "").strip()
        y, m = self.get_year_month()
        dep = (self.cmb_department.get() or "Все").strip()

        if not addr and not oid:
            messagebox.showwarning("Сохранение", "Укажите адрес и/или ID объекта, а также период.")
            return

        # Сохраним правки с текущей страницы в модель
        self._sync_visible_to_model()

        try:
            db.save_timesheet(object_code=(oid or None),
                              object_address=addr,
                              dep_name=dep,
                              year=y,
                              month=m,
                              model_rows=self.model_rows)
            messagebox.showinfo("Сохранение", "Данные сохранены в БД.")
            # Перечитаем из БД, чтобы убедиться и синхронизировать
            self._load_existing_rows()
        except Exception as e:
            messagebox.showerror("Сохранение", f"Ошибка сохранения в БД:\n{e}")

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
        px = self.COLPX.copy()
        if fio_px is not None:
            px["fio"] = fio_px
        return px["fio"] + px["tbn"] + 31*px["day"] + px["days"] + px["hours"] + px["btn52"] + px["del"]

    def _auto_fit_columns(self):
        try:
            viewport = self.main_canvas.winfo_width()
        except Exception:
            viewport = 0
        if viewport <= 1:
            self.after(120, self._auto_fit_columns)
            return
        total = self._content_total_width()
        new_fio = self.COLPX["fio"]
        if total > viewport:
            deficit = total - viewport
            new_fio = max(self.MIN_FIO_PX, self.COLPX["fio"] - deficit)
        elif total < viewport:
            surplus = viewport - total
            new_fio = min(self.MAX_FIO_PX, self.COLPX["fio"] + surplus)
        if int(new_fio) != int(self.COLPX["fio"]):
            self.COLPX["fio"] = int(new_fio)
            self._configure_table_columns()
            self._on_scroll_frame_configure()

    def _on_window_configure(self, _evt):
        try:
            self.after_cancel(self._fit_job)
        except Exception:
            pass
        self._fit_job = self.after(150, self._auto_fit_columns)

class MainApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1024x720")
        self.minsize(980, 640)
        self.resizable(True, True)

        ensure_config()

        try:
            import db
            db.init_db()
            print("[DB] init done")
        except Exception as e:
            print(f"[DB init] {e}")
        self._pages: Dict[str, tk.Widget] = {}

        # Меню
        menubar = tk.Menu(self)

        # Кнопка Главная (возврат на стартовый экран)
        menubar.add_command(label="Главная", command=self.show_home)

        m_ts = tk.Menu(menubar, tearoff=0)
        m_ts.add_command(label="Создать", command=lambda: self._show_page("timesheet", lambda parent: TimesheetPage(parent)))
        menubar.add_cascade(label="Объектный табель", menu=m_ts)

        # ========== ОБНОВЛЕННОЕ МЕНЮ АВТОТРАНСПОРТ ==========
        m_transport = tk.Menu(menubar, tearoff=0)
        if SpecialOrders and hasattr(SpecialOrders, "create_page"):
            m_transport.add_command(
                label="📝 Создать заявку",
                command=lambda: self._show_page("transport", lambda parent: SpecialOrders.create_page(parent))
            )
        else:
            m_transport.add_command(label="📝 Создать заявку", command=self.run_special_orders_exe)
        # Добавляем планирование (если включено в конфиге)
        if SpecialOrders and hasattr(SpecialOrders, "create_planning_page"):
            m_transport.add_command(
                label="🚛 Планирование транспорта",
                command=lambda: self._show_page("planning", lambda parent: SpecialOrders.create_planning_page(parent))
            )
        m_transport.add_separator()
        m_transport.add_command(
            label="📂 Открыть папку заявок",
            command=self.open_orders_folder
        )
        menubar.add_cascade(label="Автотранспорт", menu=m_transport)
        # ===================================================

        m_spr = tk.Menu(menubar, tearoff=0)
        m_spr.add_command(label="Открыть справочник", command=self.open_spravochnik)
        m_spr.add_command(label="Обновить справочник", command=self.refresh_spravochnik_global)
        menubar.add_cascade(label="Справочник", menu=m_spr)

        m_analytics = tk.Menu(menubar, tearoff=0)
        m_analytics.add_command(label="Экспорт свода (XLSX/CSV)", command=self.summary_export)
        menubar.add_cascade(label="Аналитика", menu=m_analytics)

        m_tools = tk.Menu(menubar, tearoff=0)
        if timesheet_transformer and hasattr(timesheet_transformer, "open_converter"):
            m_tools.add_command(label="Конвертер табеля (1С)", command=lambda: timesheet_transformer.open_converter(self))
        else:
            m_tools.add_command(label="Конвертер табеля (1С)", command=self.run_converter_exe)
        if BudgetAnalyzer and hasattr(BudgetAnalyzer, "create_page"):
            m_tools.add_command(label="Анализ смет", command=lambda: self._show_page("budget", lambda parent: BudgetAnalyzer.create_page(parent)))
        else:
            m_tools.add_command(label="Анализ смет", command=lambda: messagebox.showwarning("Анализ смет", "Модуль BudgetAnalyzer.py не найден."))

        menubar.add_cascade(label="Инструменты", menu=m_tools)

        m_settings = tk.Menu(menubar, tearoff=0)
        m_settings.add_command(label="Открыть настройки", command=lambda: Settings.open_settings_window(self) if Settings else messagebox.showwarning("Настройки", "Модуль settings_manager не найден."))
        menubar.add_cascade(label="Настройки", menu=m_settings)

        self.config(menu=menubar)

        # Шапка
        header = tk.Frame(self)
        header.pack(fill="x", padx=12, pady=(10, 4))
        tk.Label(header, text="Управление строительством", font=("Segoe UI", 16, "bold")).pack(side="left")
        tk.Label(header, text="Выберите раздел в верхнем меню", font=("Segoe UI", 10), fg="#555").pack(side="right")

        # Контент — контейнер для страниц
        self.content = tk.Frame(self, bg="#f7f7f7")
        self.content.pack(fill="both", expand=True)

        # Копирайт
        footer = tk.Frame(self)
        footer.pack(fill="x", padx=12, pady=(0, 10))
        tk.Label(footer, text="Разработал Алексей Зезюкин, АНО МЛСТ 2025",
                 font=("Segoe UI", 8), fg="#666").pack(side="right")

        # Показать домашнюю страницу при запуске
        self.show_home()

    def _show_page(self, key: str, builder):
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
            # Покажем главную страницу как резерв
            self.show_home()
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

if __name__ == "__main__":
    app = MainApp()
    app.mainloop()
