import os
import re
import sys
import csv
import json
import calendar
import subprocess
import configparser
import urllib.request
import urllib.error
import urllib.parse
import traceback
from io import BytesIO
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional, Any, Dict, NamedTuple
import base64

# --- Импорты сторонних библиотек ---
try:
    import pandas as pd
except ImportError:
    # Заглушка, если pandas не установлен.
    # Для сводного экспорта он критически важен.
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
    
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

APP_NAME = "Управление строительством (Главное меню)"

# ------------- Конфиг и файлы -------------
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
CONVERTER_EXE = "TabelConverter.exe"
RAW_LOGO_URL = "https://raw.githubusercontent.com/alekseyvz-dotcom/TimesheetTransformer/main/logo.png"
TINY_PNG_BASE64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/w8AAn8B9w3G2kIAAAAASUVORK5CYII="
)

# ------------- СХЕМА ТАБЕЛЯ (Критично для сопровождения) -------------
# Индексы колонок в листе "Табель" (начиная с 1)
class TimesheetSchema(NamedTuple):
    ID_OBJECT: int = 1
    ADDRESS: int = 2
    MONTH: int = 3
    YEAR: int = 4
    FIO: int = 5
    TBN: int = 6
    DEPARTMENT: int = 7
    DAILY_HOURS_START: int = 8 # Начало колонок для 1-го дня месяца
    TOTAL_DAYS: int = 39
    TOTAL_HOURS: int = 40
    OVERTIME_DAY: int = 41
    OVERTIME_NIGHT: int = 42
    
TS_SCHEMA = TimesheetSchema()
# Общее количество колонок для данных + итогов
TOTAL_DATA_COLUMNS = TS_SCHEMA.OVERTIME_NIGHT

# ------------- Базовые утилиты (без изменений) -------------
def embedded_logo_image(parent, max_w=360, max_h=160):
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

def exe_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

def config_path() -> Path:
    return exe_dir() / CONFIG_FILE

def ensure_config():
    # (Логика configparser, не изменена)
    cp = config_path()
    if cp.exists():
        cfg = configparser.ConfigParser()
        cfg.read(cp, encoding="utf-8")
        changed = False
        # Paths
        if not cfg.has_section(CONFIG_SECTION_PATHS): cfg[CONFIG_SECTION_PATHS] = {}; changed = True
        if KEY_SPR not in cfg[CONFIG_SECTION_PATHS]: cfg[CONFIG_SECTION_PATHS][KEY_SPR] = str(exe_dir() / SPRAVOCHNIK_FILE_DEFAULT); changed = True
        if KEY_OUTPUT_DIR not in cfg[CONFIG_SECTION_PATHS]: cfg[CONFIG_SECTION_PATHS][KEY_OUTPUT_DIR] = str(exe_dir() / OUTPUT_DIR_DEFAULT); changed = True
        # UI
        if not cfg.has_section(CONFIG_SECTION_UI): cfg[CONFIG_SECTION_UI] = {}; changed = True
        if KEY_SELECTED_DEP not in cfg[CONFIG_SECTION_UI]: cfg[CONFIG_SECTION_UI][KEY_SELECTED_DEP] = "Все"; changed = True
        # Integrations
        if not cfg.has_section(CONFIG_SECTION_INTEGR): cfg[CONFIG_SECTION_INTEGR] = {}; changed = True
        if KEY_EXPORT_PWD not in cfg[CONFIG_SECTION_INTEGR]: cfg[CONFIG_SECTION_INTEGR][KEY_EXPORT_PWD] = "2025"; changed = True
        if KEY_PLANNING_PASSWORD not in cfg[CONFIG_SECTION_INTEGR]: cfg[CONFIG_SECTION_INTEGR][KEY_PLANNING_PASSWORD] = "2025"; changed = True
        # Remote
        if not cfg.has_section(CONFIG_SECTION_REMOTE): cfg[CONFIG_SECTION_REMOTE] = {}; changed = True
        if KEY_REMOTE_USE not in cfg[CONFIG_SECTION_REMOTE]: cfg[CONFIG_SECTION_REMOTE][KEY_REMOTE_USE] = "false"; changed = True
        if KEY_YA_PUBLIC_LINK not in cfg[CONFIG_SECTION_REMOTE]: cfg[CONFIG_SECTION_REMOTE][KEY_YA_PUBLIC_LINK] = ""; changed = True
        if KEY_YA_PUBLIC_PATH not in cfg[CONFIG_SECTION_REMOTE]: cfg[CONFIG_SECTION_REMOTE][KEY_YA_PUBLIC_PATH] = ""; changed = True
        # Orders
        if not cfg.has_section("Orders"): cfg["Orders"] = {}; changed = True
        if "cutoff_enabled" not in cfg["Orders"]: cfg["Orders"]["cutoff_enabled"] = "false"; changed = True
        if "cutoff_hour" not in cfg["Orders"]: cfg["Orders"]["cutoff_hour"] = "13"; changed = True

        if changed:
            with open(cp, "w", encoding="utf-8") as f:
                cfg.write(f)
        return

    # новый файл
    cfg = configparser.ConfigParser()
    cfg[CONFIG_SECTION_PATHS] = {
        KEY_SPR: str(exe_dir() / SPRAVOCHNIK_FILE_DEFAULT),
        KEY_OUTPUT_DIR: str(exe_dir() / OUTPUT_DIR_DEFAULT),
    }
    cfg[CONFIG_SECTION_UI] = {KEY_SELECTED_DEP: "Все"}
    cfg[CONFIG_SECTION_INTEGR] = {KEY_EXPORT_PWD: "2025", KEY_PLANNING_PASSWORD: "2025"}
    cfg[CONFIG_SECTION_REMOTE] = {
        KEY_REMOTE_USE: "false",
        KEY_YA_PUBLIC_LINK: "",
        KEY_YA_PUBLIC_PATH: "",
    }
    cfg["Orders"] = {
        "cutoff_enabled": "false",
        "cutoff_hour": "13",
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

# ------------- Удалённый справочник: Я.Диск (без изменений) -------------

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
    # (Логика загрузки справочника, не изменена)
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
    # (Логика создания дефолтного справочника, не изменена)
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
    # (Логика загрузки, не изменена)
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
            print(f"[Remote YaDisk] ошибка: {e} — используем локальный файл")

    ensure_spravochnik_local(local_path)
    wb = load_workbook(local_path, read_only=True, data_only=True)
    return load_spravochnik_from_wb(wb)

# ------------- Утилиты для работы со временем и данными -------------

class ParsedHours(NamedTuple):
    hours: float = 0.0
    ot_day: float = 0.0
    ot_night: float = 0.0
    raw_input: Optional[str] = None
    is_valid: bool = False

def parse_time_string(s: str) -> float:
    """Парсит 'HH:MM:SS' или 'HH,MM' в часы (float)."""
    s = s.strip()
    if not s: return 0.0
    
    if ":" in s:
        p = s.split(":")
        try:
            hh = float(p[0].replace(",", "."))
            mm = float((p[1] if len(p) > 1 else "0").replace(",", "."))
            ss = float((p[2] if len(p) > 2 else "0").replace(",", "."))
            return hh + mm / 60.0 + ss / 3600.0
        except:
            return 0.0
    
    # Прямое число с запятой или точкой
    try:
        return float(s.replace(",", "."))
    except:
        return 0.0

def parse_day_entry(v: Any) -> ParsedHours:
    """
    Унифицированный парсер: 8 | 8,25 | 8:30 | 1/7 | 8,25(6/2)
    """
    s = str(v or "").strip()
    if not s:
        return ParsedHours(is_valid=True) # Пустая ячейка считается валидной
    
    base_hours_str = s
    ot_day = 0.0
    ot_night = 0.0
    
    # 1. Извлечение переработки в скобках
    if "(" in s and ")" in s:
        try:
            start = s.index("(")
            end = s.index(")")
            ot_str = s[start + 1:end].strip()
            base_hours_str = s[:start].strip()

            if "/" in ot_str:
                parts = ot_str.split("/")
                ot_day = parse_time_string(parts[0])
                ot_night = parse_time_string(parts[1]) if len(parts) > 1 else 0.0
            else:
                ot_day = parse_time_string(ot_str)
                ot_night = 0.0
        except Exception:
            # Неправильный формат переработки, считаем строку невалидной
            return ParsedHours(raw_input=s)

    # 2. Извлечение базовых часов (до скобок)
    total_base_hours = 0.0
    
    if "/" in base_hours_str:
        # Формат 1/7
        any_part = False
        for part in base_hours_str.split("/"):
            h = parse_time_string(part)
            total_base_hours += h
            if h > 1e-12: any_part = True
        if not any_part and base_hours_str.strip(): # Если были символы, но не распарсилось
             return ParsedHours(raw_input=s)
    else:
        # Формат 8 или 8:30
        total_base_hours = parse_time_string(base_hours_str)
        if total_base_hours == 0.0 and base_hours_str.strip():
             return ParsedHours(raw_input=s)

    # 3. Валидация итогов
    if total_base_hours < 0 or total_base_hours > 24 or ot_day < 0 or ot_night < 0:
        return ParsedHours(raw_input=s)

    return ParsedHours(
        hours=total_base_hours,
        ot_day=ot_day,
        ot_night=ot_night,
        raw_input=s,
        is_valid=True
    )

def month_days(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]

def month_name_ru(month: int) -> str:
    names = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
    return names[month-1]

def safe_filename(s: str, maxlen: int = 60) -> str:
    if not s:
        return "NOID"
    s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", str(s)).strip()
    s = re.sub(r"_+", "_", s)
    return s[:maxlen] if len(s) > maxlen else s
    
# (find_logo_path не используется в коде MainApp, но оставим)
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

# ------------- Ряд реестра (RowWidget) - Улучшенный UX -------------

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

        zebra_bg = self.ZEBRA_EVEN if (row_index % 2 == 0) else self.ZEBRA_ODD
        self.widgets: List[tk.Widget] = []
        
        # --- КЭШ ПАРСИНГА ДАННЫХ ДЛЯ ПРОИЗВОДИТЕЛЬНОСТИ ---
        # Хранит результаты ParsedHours для каждого дня
        self.parsed_hours_cache: List[ParsedHours] = [ParsedHours() for _ in range(31)]

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
            e.bind("<FocusOut>", lambda ev, _d=d: self._on_entry_change(_d - 1))
            e.bind("<Return>", lambda ev, _d=d: self._on_entry_change(_d - 1))
            # Улучшенный UX: Добавление поддержки Paste
            e.bind("<<Paste>>", self._on_paste_in_entry, add='+')
            self.day_entries.append(e)
            self.widgets.append(e)

        # Итоги
        self.lbl_days = tk.Label(self.table, text="0", anchor="e", bg=zebra_bg)
        self.lbl_days.grid(row=self.row, column=TS_SCHEMA.TOTAL_DAYS - 1, padx=(4, 1), pady=1, sticky="nsew")
        self.widgets.append(self.lbl_days)

        self.lbl_total = tk.Label(self.table, text="0", anchor="e", bg=zebra_bg)
        self.lbl_total.grid(row=self.row, column=TS_SCHEMA.TOTAL_HOURS - 1, padx=(4, 1), pady=1, sticky="nsew")
        self.widgets.append(self.lbl_total)

        # МЕТКИ ДЛЯ ПЕРЕРАБОТКИ
        self.lbl_overtime_day = tk.Label(self.table, text="0", anchor="e", bg=zebra_bg)
        self.lbl_overtime_day.grid(row=self.row, column=TS_SCHEMA.OVERTIME_DAY - 1, padx=(4, 1), pady=1, sticky="nsew")
        self.widgets.append(self.lbl_overtime_day)

        self.lbl_overtime_night = tk.Label(self.table, text="0", anchor="e", bg=zebra_bg)
        self.lbl_overtime_night.grid(row=self.row, column=TS_SCHEMA.OVERTIME_NIGHT - 1, padx=(4, 1), pady=1, sticky="nsew")
        self.widgets.append(self.lbl_overtime_night)

        # 5/2
        self.btn_52 = ttk.Button(self.table, text="5/2", width=4, command=self.fill_52)
        self.btn_52.grid(row=self.row, column=TS_SCHEMA.OVERTIME_NIGHT, padx=1, pady=0, sticky="nsew")
        self.widgets.append(self.btn_52)

        # Удалить
        self.btn_del = ttk.Button(self.table, text="Удалить", width=7, command=self.delete_row)
        self.btn_del.grid(row=self.row, column=TS_SCHEMA.OVERTIME_NIGHT + 1, padx=1, pady=0, sticky="nsew")
        self.widgets.append(self.btn_del)

    # --- Новая логика для массового копирования (UX) ---
    def _on_paste_in_entry(self, event):
        try:
            pasted_data = self.table.clipboard_get()
            if not pasted_data:
                return
            
            # Проверяем, откуда пришел Paste
            col = self.table.grid_info(event.widget)['column']
            
            # Если вставлено в одну из ячеек дней (индекс 2-32)
            if TS_SCHEMA.DAILY_HOURS_START <= col <= TS_SCHEMA.DAILY_HOURS_START + 30:
                col_index = col - TS_SCHEMA.DAILY_HOURS_START
                
                # Парсим данные, разделенные табами или переводами строк (типично для Excel)
                values = re.split(r'[\t\n\r]+', pasted_data.strip())
                
                # Заполняем ячейки, начиная с текущей
                for i, val in enumerate(values):
                    if col_index + i < 31:
                        e = self.day_entries[col_index + i]
                        e.delete(0, tk.END)
                        e.insert(0, val)
                
                # Обновляем итоги
                self.update_total()
                return "break" # Предотвращаем стандартный Paste
                
        except Exception as e:
            # print(f"Paste error: {e}")
            pass
        
        # Если не смогли обработать как массовую вставку, даем Tkinter продолжить
        return None

    def _on_entry_change(self, index: int):
        """Обновляет кэш и перекрашивает ячейку после потери фокуса."""
        y, m = self.get_year_month()
        self._update_parsed_cache(index)
        self._repaint_day_cell(index, y, m)
        self.update_total()

    def _update_parsed_cache(self, index: int):
        """Обновляет одну запись в кэше парсинга."""
        raw = self.day_entries[index].get().strip()
        self.parsed_hours_cache[index] = parse_day_entry(raw)

    def regrid_to(self, new_row: int):
        self.row = new_row
        zebra_bg = self.ZEBRA_EVEN if (new_row % 2 == 0) else self.ZEBRA_ODD
        
        # Перенастраиваем фон и grid
        self.lbl_fio.grid_configure(row=new_row); self.lbl_fio.config(bg=zebra_bg)
        self.lbl_tbn.grid_configure(row=new_row); self.lbl_tbn.config(bg=zebra_bg)
        
        for i, e in enumerate(self.day_entries, start=TS_SCHEMA.DAILY_HOURS_START):
            e.grid_configure(row=new_row, column=i)
        
        self.lbl_days.grid_configure(row=new_row); self.lbl_days.config(bg=zebra_bg)
        self.lbl_total.grid_configure(row=new_row); self.lbl_total.config(bg=zebra_bg)
        self.lbl_overtime_day.grid_configure(row=new_row); self.lbl_overtime_day.config(bg=zebra_bg)
        self.lbl_overtime_night.grid_configure(row=new_row); self.lbl_overtime_night.config(bg=zebra_bg)
        self.btn_52.grid_configure(row=new_row)
        self.btn_del.grid_configure(row=new_row)

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
            raw_input = str(arr[i]) if i < days and arr[i] else ""
            self.day_entries[i].delete(0, "end")
            if raw_input:
                self.day_entries[i].insert(0, raw_input)
            
            # Обновляем кэш при загрузке
            self.parsed_hours_cache[i] = parse_day_entry(raw_input)
            
        self.update_total()

    def get_hours_with_overtime(self) -> List[ParsedHours]:
        """Возвращает кэшированные результаты парсинга для всех 31 дня."""
        return self.parsed_hours_cache

    def _bg_for_day(self, year: int, month: int, day: int) -> str:
        wd = datetime(year, month, day).weekday()
        if wd == 5: return self.WEEK_BG_SAT
        if wd == 6: return self.WEEK_BG_SUN
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
        
        parsed = self.parsed_hours_cache[i0]
        
        if not parsed.is_valid and parsed.raw_input:
            e.configure(bg=self.ERR_BG)
        else:
            e.configure(bg=self._bg_for_day(year, month, day))

    def update_days_enabled(self, year: int, month: int):
        for i in range(31):
            # Обновляем кэш, если ячейка была заполнена, но фокус не терялся
            if not self.parsed_hours_cache[i].raw_input and self.day_entries[i].get():
                self._update_parsed_cache(i)
            self._repaint_day_cell(i, year, month)
        self.update_total()

    def update_total(self):
        total_hours = 0.0
        total_days = 0
        total_overtime_day = 0.0
        total_overtime_night = 0.0
        
        y, m = self.get_year_month()
        days_in_m = month_days(y, m)
        
        for i, parsed in enumerate(self.parsed_hours_cache):
            if i >= days_in_m:
                continue
            
            # Перепроверяем, если ячейка была изменена, но кэш не обновлен (редко, но возможно)
            if parsed.raw_input != self.day_entries[i].get().strip():
                self._update_parsed_cache(i)
                parsed = self.parsed_hours_cache[i]
                self._repaint_day_cell(i, y, m)
            
            if parsed.is_valid and parsed.hours > 1e-12:
                total_hours += parsed.hours
                total_days += 1
                total_overtime_day += parsed.ot_day
                total_overtime_night += parsed.ot_night
        
        # Обновление меток
        self.lbl_days.config(text=str(total_days))
        sh = f"{total_hours:.2f}".rstrip("0").rstrip(".")
        self.lbl_total.config(text=sh)
        sod = f"{total_overtime_day:.2f}".rstrip("0").rstrip(".")
        self.lbl_overtime_day.config(text=sod)
        son = f"{total_overtime_night:.2f}".rstrip("0").rstrip(".")
        self.lbl_overtime_night.config(text=son)
        
        # Запрос на пересчет общих итогов объекта
        if hasattr(self.table.master.master, '_recalc_object_total'):
            self.table.master.master._recalc_object_total()


    def fill_52(self):
        y, m = self.get_year_month()
        days = month_days(y, m)
        for d in range(1, days + 1):
            wd = datetime(y, m, d).weekday()
            e = self.day_entries[d - 1]
            e.delete(0, "end")
            val = ""
            if wd < 4:
                val = "8,25"
            elif wd == 4:
                val = "7"
            
            e.insert(0, val)
            self.parsed_hours_cache[d - 1] = parse_day_entry(val)
            
        for d in range(days + 1, 32):
            self.day_entries[d - 1].delete(0, "end")
            self.parsed_hours_cache[d - 1] = ParsedHours()
            
        self.update_total()

    def delete_row(self):
        self.on_delete(self)

# ------------- Диалоги -------------
# (Оставлены без изменений, так как они достаточно изолированы)

class CopyFromDialog(simpledialog.Dialog):
    # ... (неизмененный код) ...
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

class HoursFillDialog(simpledialog.Dialog):
    # ... (неизмененный код) ...
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

        tk.Label(master, text="Форматы: 8 | 8,25 | 8:30 | 1/7 (Переработка: 8(2/1))").grid(row=4, column=0, columnspan=3, sticky="w", pady=(6, 2))
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
            self._h_str = ""
            self._clear = True
            return True

        h_str = self.ent_hours.get().strip()
        parsed = parse_day_entry(h_str)
        
        if not parsed.is_valid:
            messagebox.showwarning("Проставить часы", "Введите корректное значение часов (например, 8, 8:30, 8(2/1)).")
            return False
            
        self._d = d
        self._h_str = h_str
        self._clear = False
        return True

    def apply(self):
        self.result = {
            "day": self._d,
            "hours_str": self._h_str,
            "clear": self._clear,
        }

# ------------- Страница Объектного табеля (Frame) - Обновлена схема -------------

class TimesheetPage(tk.Frame):
    # Используем TS_SCHEMA для определения колонок
    
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
        self._build_ui()
        self._load_existing_rows()

        self.bind("<Configure>", self._on_window_configure)
        self.after(120, self._auto_fit_columns)

    def _load_spr_data(self):
        # (Логика загрузки справочника, не изменена)
        try:
            employees, objects = load_spravochnik_remote_or_local(self.spr_path)
        except Exception as e:
            messagebox.showerror("Справочник", f"Ошибка загрузки справочника: {e}")
            employees, objects = [], []

        self.employees = employees
        self.objects = objects

        self.emp_names = [fio for (fio, _, _, _) in self.employees]
        # Используем FIO + TBN как более надежный ключ, но храним только FIO в UI
        self.emp_info = {fio: (tbn, pos) for (fio, tbn, pos, _) in self.employees} 
        
        # Индекс для быстрого поиска департамента по FIO
        self.emp_dep_map = {fio: dep for (fio, _, _, dep) in self.employees}

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
        # (UI построение, не изменено)
        # ... (Код UI) ...
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=8)

        # Row 0
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

        # Row 1 (Период, Адрес, ID)
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

        # Row 2 (Новый сотрудник)
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

        # Row 3 (Кнопки действий)
        btns = tk.Frame(top)
        btns.grid(row=3, column=0, columnspan=8, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Добавить в табель", command=self.add_row).grid(row=0, column=0, padx=4)
        ttk.Button(btns, text="Добавить подразделение", command=self.add_department_all).grid(row=0, column=1, padx=4)
        ttk.Button(btns, text="5/2 всем", command=self.fill_52_all).grid(row=0, column=2, padx=4)
        ttk.Button(btns, text="Проставить часы", command=self.fill_hours_all).grid(row=0, column=3, padx=4)
        ttk.Button(btns, text="Очистить все строки", command=self.clear_all_rows).grid(row=0, column=4, padx=4)
        ttk.Button(btns, text="Обновить справочник", command=self.reload_spravochnik).grid(row=0, column=5, padx=4)
        ttk.Button(btns, text="Копировать из месяца…", command=self.copy_from_month).grid(row=0, column=6, padx=4)
        
        # Кнопка сохранения вынесена отдельно для большей заметности (UX)
        self.btn_save = ttk.Button(btns, text="Сохранить", command=self.save_all, style="Accent.TButton")
        self.btn_save.grid(row=0, column=7, padx=8)
        
        # Основной контейнер с прокруткой
        main_frame = tk.Frame(self)
        main_frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        self.main_canvas = tk.Canvas(main_frame, borderwidth=0, highlightthickness=0)
        self.main_canvas.grid(row=0, column=0, sticky="nsew")

        self.vscroll = ttk.Scrollbar(main_frame, orient="vertical", command=self.main_canvas.yview)
        self.vscroll.grid(row=0, column=1, sticky="ns")
        self.hscroll = ttk.Scrollbar(main_frame, orient="horizontal", command=self.main_canvas.xview)
        self.hscroll.grid(row=1, column=0, sticky="ew")

        main_frame.grid_rowconfigure(0, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)

        # Единая таблица (header + rows в одном grid)
        self.table = tk.Frame(self.main_canvas, bg="#ffffff")
        self.canvas_window = self.main_canvas.create_window((0, 0), window=self.table, anchor="nw")

        self.main_canvas.configure(yscrollcommand=self.vscroll.set, xscrollcommand=self.hscroll.set)
        self.table.bind("<Configure>", self._on_scroll_frame_configure)

        # Создаём шапку в первой строке таблицы
        self._configure_table_columns()
        self._build_header_row()

        # Обработчики колеса мыши
        self.main_canvas.bind("<MouseWheel>", self._on_wheel)
        self.main_canvas.bind("<Shift-MouseWheel>", self._on_shift_wheel)
        self.bind_all("<MouseWheel>", self._on_wheel_anywhere)

        # Коллекция строк
        self.rows: List[RowWidget] = []

        # Нижняя панель
        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=8, pady=(0, 8))
        self.lbl_object_total = tk.Label(bottom, text="Сумма: сотрудников 0 | дней 0 | часов 0",
                                         font=("Segoe UI", 10, "bold"))
        self.lbl_object_total.pack(side="left")

        self._on_department_select()
        # ... (Конец кода UI) ...

    def _build_header_row(self):
        hb = self.HEADER_BG
        
        # Используем TS_SCHEMA для колонок
        tk.Label(self.table, text="ФИО", bg=hb, anchor="w", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=TS_SCHEMA.FIO - 1, padx=0, pady=(0, 2), sticky="nsew")
        tk.Label(self.table, text="Таб.№", bg=hb, anchor="center", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=TS_SCHEMA.TBN - 1, padx=0, pady=(0, 2), sticky="nsew")
    
        for d in range(1, 32):
            tk.Label(self.table, text=str(d), bg=hb, anchor="center", font=("Segoe UI", 9, "bold")).grid(
                row=0, column=(TS_SCHEMA.DAILY_HOURS_START - 1) + d, padx=0, pady=(0, 2), sticky="nsew")
    
        tk.Label(self.table, text="Дней", bg=hb, anchor="e", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=TS_SCHEMA.TOTAL_DAYS - 1, padx=(4, 1), pady=(0, 2), sticky="nsew")
        tk.Label(self.table, text="Часы", bg=hb, anchor="e", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=TS_SCHEMA.TOTAL_HOURS - 1, padx=(4, 1), pady=(0, 2), sticky="nsew")
    
        tk.Label(self.table, text="Пер.день", bg=hb, anchor="e", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=TS_SCHEMA.OVERTIME_DAY - 1, padx=(4, 1), pady=(0, 2), sticky="nsew")
        tk.Label(self.table, text="Пер.ночь", bg=hb, anchor="e", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=TS_SCHEMA.OVERTIME_NIGHT - 1, padx=(4, 1), pady=(0, 2), sticky="nsew")
    
        tk.Label(self.table, text="5/2", bg=hb, anchor="center", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=TS_SCHEMA.OVERTIME_NIGHT, padx=1, pady=(0, 2), sticky="nsew")
        tk.Label(self.table, text="Удалить", bg=hb, anchor="center", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=TS_SCHEMA.OVERTIME_NIGHT + 1, padx=1, pady=(0, 2), sticky="nsew")

    def _configure_table_columns(self):
        # Используем TS_SCHEMA для конфигурации, FIO и TBN - это колонки 0 и 1
        px = self.COLPX
        self.table.grid_columnconfigure(0, minsize=px['fio'], weight=0) # FIO (col 0, schema 5)
        self.table.grid_columnconfigure(1, minsize=px['tbn'], weight=0) # TBN (col 1, schema 6)
        
        # Дни
        for col in range(2, 2 + 31):
            self.table.grid_columnconfigure(col, minsize=px['day'], weight=0)
            
        # Итоги
        self.table.grid_columnconfigure(TS_SCHEMA.TOTAL_DAYS - 1, minsize=px['days'], weight=0)
        self.table.grid_columnconfigure(TS_SCHEMA.TOTAL_HOURS - 1, minsize=px['hours'], weight=0)
        self.table.grid_columnconfigure(TS_SCHEMA.OVERTIME_DAY - 1, minsize=px['hours'], weight=0)
        self.table.grid_columnconfigure(TS_SCHEMA.OVERTIME_NIGHT - 1, minsize=px['hours'], weight=0)
        
        # Кнопки
        self.table.grid_columnconfigure(TS_SCHEMA.OVERTIME_NIGHT, minsize=px['btn52'], weight=0)
        self.table.grid_columnconfigure(TS_SCHEMA.OVERTIME_NIGHT + 1, minsize=px['del'], weight=0)

    # (Остальные методы TimesheetPage: _on_wheel, _on_period_change, _on_address_change,
    #  get_year_month, _update_rows_days_enabled, _regrid_rows - без изменений)
    
    def _recalc_object_total(self):
        # Запускается RowWidget.update_total() после изменения ячейки
        tot_h = 0.0
        tot_d = 0
        tot_ot_day = 0.0
        tot_ot_night = 0.0
    
        for r in self.rows:
            # Читаем данные напрямую из виджетов, поскольку они обновляются в RowWidget.update_total
            try:
                h = float(r.lbl_total.cget("text").replace(",", ".") or 0)
            except Exception:
                h = 0.0
            try:
                d = int(r.lbl_days.cget("text") or 0)
            except Exception:
                d = 0
            try:
                od = float(r.lbl_overtime_day.cget("text").replace(",", ".") or 0)
            except Exception:
                od = 0.0
            try:
                on = float(r.lbl_overtime_night.cget("text").replace(",", ".") or 0)
            except Exception:
                on = 0.0
        
            tot_h += h
            tot_d += d
            tot_ot_day += od
            tot_ot_night += on
    
        sh = f"{tot_h:.2f}".rstrip("0").rstrip(".")
        sod = f"{tot_ot_day:.2f}".rstrip("0").rstrip(".")
        son = f"{tot_ot_night:.2f}".rstrip("0").rstrip(".")
        cnt = len(self.rows)
    
        self.lbl_object_total.config(
            text=f"Сумма: сотрудников {cnt} | дней {tot_d} | часов {sh} | пер.день {sod} | пер.ночь {son}"
        )

    def fill_hours_all(self):
        # Логика изменена для поддержки строкового формата с переработкой
        if not self.rows:
            messagebox.showinfo("Проставить часы", "Список сотрудников пуст.")
            return
        y, m = self.get_year_month()
        max_day = month_days(y, m)
        dlg = HoursFillDialog(self, max_day)
        if not getattr(dlg, "result", None):
            return
            
        day = dlg.result["day"]
        clear = bool(dlg.result.get("clear", False))
        hours_str = dlg.result["hours_str"]
        
        if day > max_day:
            messagebox.showwarning("Проставить часы", f"В {month_name_ru(m)} {y} только {max_day} дней.")
            return

        parsed_val = parse_day_entry(hours_str)
        
        for r in self.rows:
            i = day - 1
            e = r.day_entries[i]
            e.delete(0, "end")
            
            if clear:
                r.parsed_hours_cache[i] = ParsedHours()
            else:
                e.insert(0, hours_str)
                r.parsed_hours_cache[i] = parsed_val
                
            r.update_total() # Запускает _repaint_day_cell и пересчет
            
        self._recalc_object_total()
        action = "очищен" if clear else f"проставлено '{hours_str}'"
        messagebox.showinfo("Проставить часы", f"День {day} {action} у {len(self.rows)} сотрудников.")


    # --- Логика работы с файлами (Обновление схемы колонок) ---
    
    def _ensure_sheet(self, wb) -> Any:
        # Обновлено использование TS_SCHEMA
        
        required_cols = TOTAL_DATA_COLUMNS # 42 колонки
        
        if "Табель" in wb.sheetnames:
            ws = wb["Табель"]
            hdr_first = str(ws.cell(1, 1).value or "")
            # Проверяем наличие новых столбцов (Подразделение + Переработки)
            if hdr_first == "ID объекта" and ws.max_column >= required_cols:
                return ws
            
            # Если формат старый, переименовываем лист
            base = "Табель_OLD"
            new_name = base
            i = 1
            while new_name in wb.sheetnames:
                i += 1
                new_name = f"{base}{i}"
            ws.title = new_name
    
        ws2 = wb.create_sheet("Табель")
        hdr = [
            "ID объекта", "Адрес", "Месяц", "Год", "ФИО", "Табельный №", "Подразделение"
        ] + [
            str(i) for i in range(1, 32)
        ] + [
            "Итого дней", "Итого часов по табелю", "Переработка день", "Переработка ночь"
        ]
        
        ws2.append(hdr)
        
        # Установка ширины колонок по TS_SCHEMA
        ws2.column_dimensions[get_column_letter(TS_SCHEMA.ID_OBJECT)].width = 14
        ws2.column_dimensions[get_column_letter(TS_SCHEMA.ADDRESS)].width = 40
        ws2.column_dimensions[get_column_letter(TS_SCHEMA.MONTH)].width = 10
        ws2.column_dimensions[get_column_letter(TS_SCHEMA.YEAR)].width = 8
        ws2.column_dimensions[get_column_letter(TS_SCHEMA.FIO)].width = 28
        ws2.column_dimensions[get_column_letter(TS_SCHEMA.TBN)].width = 14
        ws2.column_dimensions[get_column_letter(TS_SCHEMA.DEPARTMENT)].width = 20
        
        # Дни месяца (1-31)
        for i in range(TS_SCHEMA.DAILY_HOURS_START, TS_SCHEMA.DAILY_HOURS_START + 31):
            ws2.column_dimensions[get_column_letter(i)].width = 6
        
        ws2.column_dimensions[get_column_letter(TS_SCHEMA.TOTAL_DAYS)].width = 10
        ws2.column_dimensions[get_column_letter(TS_SCHEMA.TOTAL_HOURS)].width = 18
        ws2.column_dimensions[get_column_letter(TS_SCHEMA.OVERTIME_DAY)].width = 14
        ws2.column_dimensions[get_column_letter(TS_SCHEMA.OVERTIME_NIGHT)].width = 14
    
        ws2.freeze_panes = "A2"
        return ws2

    def _load_existing_rows(self):
        # Используем TS_SCHEMA
        for r in list(self.rows):
            r.destroy()
        self.rows.clear()
        self._regrid_rows()

        fpath = self._current_file_path()
        if not fpath or not fpath.exists():
            return
    
        try:
            wb = load_workbook(fpath)
            ws = self._ensure_sheet(wb) # Создаст новый, если старый формат
            y, m = self.get_year_month()
            addr = self.cmb_address.get().strip()
            oid = self.cmb_object_id.get().strip()
        
            for r in range(2, ws.max_row + 1):
                row_oid = (ws.cell(r, TS_SCHEMA.ID_OBJECT).value or "")
                row_addr = (ws.cell(r, TS_SCHEMA.ADDRESS).value or "")
                row_m = int(ws.cell(r, TS_SCHEMA.MONTH).value or 0)
                row_y = int(ws.cell(r, TS_SCHEMA.YEAR).value or 0)
                fio = (ws.cell(r, TS_SCHEMA.FIO).value or "")
                tbn = (ws.cell(r, TS_SCHEMA.TBN).value or "")
            
                if row_m != m or row_y != y:
                    continue
                if oid:
                    if row_oid != oid: continue
                else:
                    if row_addr != addr: continue
            
                # Загружаем ячейки КАК ЕСТЬ (с переработкой) - с колонки DAILY_HOURS_START
                hours_raw: List[Optional[str]] = []
                for c in range(TS_SCHEMA.DAILY_HOURS_START, TS_SCHEMA.DAILY_HOURS_START + 31):
                    v = ws.cell(r, c).value
                    # openpyxl может вернуть float даже для строкового формата, приводим к строке
                    hours_raw.append(str(v).replace('.', ',') if v is not None else None)
            
                roww = RowWidget(self.table, len(self.rows) + 1, fio, tbn, self.get_year_month, self.delete_row)
                roww.set_day_font(self.DAY_ENTRY_FONT)
                roww.set_hours(hours_raw) # set_hours обновляет кэш и вызывает update_days_enabled
                self.rows.append(roww)
        
            self._regrid_rows()
        except Exception as e:
            messagebox.showerror("Загрузка", f"Не удалось загрузить существующие строки:\n{e}")
            traceback.print_exc()

    def save_all(self):
        # Используем TS_SCHEMA и кэшированные ParsedHours
        fpath = self._current_file_path()
        if not fpath:
            messagebox.showwarning("Сохранение", "Укажите адрес и/или ID объекта, а также период.")
            return

        addr = self.cmb_address.get().strip()
        oid = self.cmb_object_id.get().strip()
        y, m = self.get_year_month()

        # --- Валидация перед сохранением (UX) ---
        errors = self._validate_before_save()
        if errors:
            if not messagebox.askyesno("Сохранение: Обнаружены ошибки", 
                                       "Найдены невалидные часы в следующих строках:\n\n" + 
                                       "\n".join(errors) + 
                                       "\n\nПродолжить сохранение (с сохранением невалидных значений)?"):
                return
        
        try:
            if fpath.exists():
                wb = load_workbook(fpath)
            else:
                fpath.parent.mkdir(parents=True, exist_ok=True)
                wb = Workbook()
                if wb.active:
                    wb.remove(wb.active)
        
            ws = self._ensure_sheet(wb)

            # Удаляем старые записи
            to_del = []
            for r in range(2, ws.max_row + 1):
                row_oid = (ws.cell(r, TS_SCHEMA.ID_OBJECT).value or "")
                row_addr = (ws.cell(r, TS_SCHEMA.ADDRESS).value or "")
                row_m = int(ws.cell(r, TS_SCHEMA.MONTH).value or 0)
                row_y = int(ws.cell(r, TS_SCHEMA.YEAR).value or 0)
                if row_m == m and row_y == y and ((oid and row_oid == oid) or (not oid and row_addr == addr)):
                    to_del.append(r)
            for r in reversed(to_del):
                ws.delete_rows(r, 1)

            # Записываем новые
            for roww in self.rows:
                # Получаем кэшированные, актуальные данные
                parsed_data = roww.get_hours_with_overtime()
                
                total_hours = 0.0
                total_days = 0
                total_ot_day = 0.0
                total_ot_night = 0.0
            
                day_values = []
                for parsed in parsed_data:
                    # Сохраняем в исходном строковом формате (для сохранения переработки)
                    if parsed.raw_input:
                        day_values.append(parsed.raw_input)
                    else:
                        day_values.append(None)
                        
                    if parsed.is_valid:
                        if parsed.hours > 1e-12:
                            total_hours += parsed.hours
                            total_days += 1
                        total_ot_day += parsed.ot_day
                        total_ot_night += parsed.ot_night
            
                # Получаем подразделение сотрудника из карты (для скорости)
                fio = roww.fio()
                department = self.emp_dep_map.get(fio, "")
            
                row_values = [
                    oid, addr, m, y, fio, roww.tbn(), department
                ] + day_values + [
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
            traceback.print_exc()
            
    def _validate_before_save(self) -> List[str]:
        """Возвращает список ФИО с невалидными часами."""
        errors: List[str] = []
        for roww in self.rows:
            parsed_data = roww.get_hours_with_overtime()
            invalid_days = []
            for i, parsed in enumerate(parsed_data, start=1):
                if not parsed.is_valid and parsed.raw_input:
                    invalid_days.append(f"День {i} ('{parsed.raw_input}')")
            
            if invalid_days:
                errors.append(f"{roww.fio()} ({roww.tbn()}): {', '.join(invalid_days)}")
        return errors

# ------------- Сводный экспорт (Полностью переписан на Pandas) -------------

def perform_summary_export(year: int, month: int, fmt: str) -> Tuple[int, List[Path]]:
    if pd is None:
        messagebox.showerror("Ошибка", "Pandas не установлен. Сводный экспорт невозможен.")
        return 0, []
        
    base_out = get_output_dir_from_config()
    pattern = f"Объектный_табель_*_{year}_{month:02d}.xlsx"
    files = list(base_out.glob(pattern))

    if not files:
        return 0, []

    # 1. Сборка всех данных в один DataFrame
    all_data_frames = []
    
    # Заголовки для чтения
    daily_cols = {i: str(i) for i in range(1, 32)}
    all_col_names = {
        TS_SCHEMA.ID_OBJECT - 1: "ID объекта",
        TS_SCHEMA.ADDRESS - 1: "Адрес",
        TS_SCHEMA.MONTH - 1: "Месяц",
        TS_SCHEMA.YEAR - 1: "Год",
        TS_SCHEMA.FIO - 1: "ФИО",
        TS_SCHEMA.TBN - 1: "Табельный №",
        TS_SCHEMA.DEPARTMENT - 1: "Подразделение",
        TS_SCHEMA.TOTAL_DAYS - 1: "Итого дней",
        TS_SCHEMA.TOTAL_HOURS - 1: "Итого часов по табелю",
        TS_SCHEMA.OVERTIME_DAY - 1: "Переработка день",
        TS_SCHEMA.OVERTIME_NIGHT - 1: "Переработка ночь",
    }
    
    # Добавляем дневные колонки в схему
    for d in range(1, 32):
        all_col_names[TS_SCHEMA.DAILY_HOURS_START - 1 + (d - 1)] = str(d)

    # Задаем типы данных для ускорения
    dtype_map = {
        "ID объекта": str, "Адрес": str, "ФИО": str, "Табельный №": str, "Подразделение": str,
        "Месяц": 'int16', "Год": 'int16',
    }
    
    # 2. Итеративное чтение файлов с использованием Pandas
    for f in files:
        try:
            # Читаем Excel, пропуская первую строку (заголовок)
            df = pd.read_excel(
                f, 
                sheet_name="Табель", 
                header=None, 
                skiprows=1,
                dtype=dtype_map
            )
            
            # Переименовываем колонки в соответствии с нашей схемой
            df = df.rename(columns=all_col_names)
            
            # Фильтруем по периоду
            df = df[(df['Год'] == year) & (df['Месяц'] == month)]
            
            # Убеждаемся, что DataFrame не пустой
            if not df.empty:
                all_data_frames.append(df)
                
        except Exception as e:
            print(f"Ошибка чтения файла {f.name}: {e}")
            continue

    if not all_data_frames:
        return 0, []

    # 3. Объединение и очистка
    final_df = pd.concat(all_data_frames, ignore_index=True)
    
    # Убедимся, что колонки для экспорта имеют нужный порядок
    final_cols = [v for k, v in sorted(all_col_names.items())]
    final_df = final_df[final_cols]
    
    count = len(final_df)
    
    # 4. Сохранение результатов
    sum_dir = exe_dir() / "Сводные_отчеты"
    sum_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []

    file_name_base = f"Сводный_{year}_{month:02d}"

    if fmt in ("xlsx", "both"):
        p = sum_dir / f"{file_name_base}.xlsx"
        try:
            # Используем ExcelWriter для сохранения с автоматическим форматированием
            writer = pd.ExcelWriter(p, engine='xlsxwriter')
            final_df.to_excel(writer, sheet_name='Сводный', index=False)
            
            # Дополнительное форматирование (опционально, но полезно)
            workbook = writer.book
            worksheet = writer.sheets['Сводный']
            
            # Ширина колонок (базовая настройка)
            for i, col_name in enumerate(final_df.columns):
                width = 10 
                if col_name == "Адрес": width = 40
                elif col_name == "ФИО": width = 28
                elif col_name == "Подразделение": width = 20
                elif len(col_name) <= 2: width = 6 # Дни
                worksheet.set_column(i, i, width)
                
            writer.close()
            paths.append(p)
        except Exception as e:
            print(f"Ошибка записи XLSX: {e}")
            messagebox.showerror("Экспорт", f"Ошибка записи XLSX:\n{e}")

    if fmt in ("csv", "both"):
        p = sum_dir / f"{file_name_base}.csv"
        try:
            # Сохранение в CSV с разделителем ";" и кодировкой utf-8-sig
            final_df.to_csv(p, sep=';', encoding='utf-8-sig', index=False)
            paths.append(p)
        except Exception as e:
            print(f"Ошибка записи CSV: {e}")
            messagebox.showerror("Экспорт", f"Ошибка записи CSV:\n{e}")

    return count, paths

# ------------- Остальной код (MainApp, Dialogs, AutoCompleteCombobox) -------------

# (Класс AutoCompleteCombobox - без изменений)
class AutoCompleteCombobox(ttk.Combobox):
    # ... (неизмененный код) ...
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

# (Класс ExportMonthDialog - без изменений)
class ExportMonthDialog(simpledialog.Dialog):
    # ... (неизмененный код) ...
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

# (Класс HomePage - без изменений)
class HomePage(tk.Frame):
    # ... (неизмененный код) ...
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


# (Класс MainApp - с добавлением стиля Accent)
class MainApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1024x720")
        self.minsize(980, 640)
        self.resizable(True, True)

        ensure_config()
        
        # Добавляем стиль для акцентной кнопки (Save)
        s = ttk.Style(self)
        s.configure('Accent.TButton', background='#4CAF50', foreground='black', font=('Segoe UI', 9, 'bold'))
        s.map('Accent.TButton', background=[('active', '#66BB6A')])


        # Меню
        menubar = tk.Menu(self)

        # Кнопка Главная (возврат на стартовый экран)
        menubar.add_command(label="Главная", command=self.show_home)

        m_ts = tk.Menu(menubar, tearoff=0)
        m_ts.add_command(label="Создать", command=lambda: self._show_page("timesheet", lambda parent: TimesheetPage(parent)))
        menubar.add_cascade(label="Объектный табель", menu=m_ts)

        # ========== АВТОТРАНСПОРТ (Используем встроенные модули напрямую) ==========
        m_transport = tk.Menu(menubar, tearoff=0)
        
        if SpecialOrders and hasattr(SpecialOrders, "create_page"):
            m_transport.add_command(
                label="📝 Создать заявку",
                command=lambda: self._show_page("transport", lambda parent: SpecialOrders.create_page(parent))
            )
        else:
            # Fallback для случая, если импорт все-таки не удался (например, отсутствует BudgetAnalyzer.py)
            m_transport.add_command(label="📝 Создать заявку", 
                                    command=lambda: messagebox.showwarning("Автотранспорт", "Модуль SpecialOrders.py не найден."))
             
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
        # =========================================================================

        m_spr = tk.Menu(menubar, tearoff=0)
        m_spr.add_command(label="Открыть справочник", command=self.open_spravochnik)
        m_spr.add_command(label="Обновить справочник", command=self.refresh_spravochnik_global)
        menubar.add_cascade(label="Справочник", menu=m_spr)

        m_analytics = tk.Menu(menubar, tearoff=0)
        m_analytics.add_command(label="Экспорт свода (XLSX/CSV)", command=self.summary_export)
        menubar.add_cascade(label="Аналитика", menu=m_analytics)

        # ========== ИНСТРУМЕНТЫ (Используем встроенные модули напрямую) ==========
        m_tools = tk.Menu(menubar, tearoff=0)
        
        if timesheet_transformer and hasattr(timesheet_transformer, "open_converter"):
            m_tools.add_command(label="Конвертер табеля (1С)", 
                                command=lambda: timesheet_transformer.open_converter(self))
        else:
            m_tools.add_command(label="Конвертер табеля (1С)", 
                                command=lambda: messagebox.showwarning("Конвертер", "Модуль timesheet_transformer.py не найден."))
            
        if BudgetAnalyzer and hasattr(BudgetAnalyzer, "create_page"):
            m_tools.add_command(label="Анализ смет", 
                                command=lambda: self._show_page("budget", lambda parent: BudgetAnalyzer.create_page(parent)))
        else:
            m_tools.add_command(label="Анализ смет", 
                                command=lambda: messagebox.showwarning("Анализ смет", "Модуль BudgetAnalyzer.py не найден."))

        menubar.add_cascade(label="Инструменты", menu=m_tools)
        # =========================================================================
        
        self.config(menu=menubar)

        # Шапка
        header = tk.Frame(self)
        header.pack(fill="x", padx=12, pady=(10, 4))
        tk.Label(header, text="Управление строительством", font=("Segoe UI", 16, "bold")).pack(side="left")
        tk.Label(header, text="Выберите раздел в верхнем меню", font=("Segoe UI", 10), fg="#555").pack(side="right")

        # Контент — контейнер для страниц
        self.content = tk.Frame(self, bg="#f7f7f7")
        self.content.pack(fill="both", expand=True)
        self._pages: Dict[str, tk.Widget] = {}

        # Копирайт
        footer = tk.Frame(self)
        footer.pack(fill="x", padx=12, pady=(0, 10))
        tk.Label(footer, text="Разработал Алексей Зезюкин, АНО МЛСТ 2025",
                 font=("Segoe UI", 8), fg="#666").pack(side="right")

    def run_special_orders_exe(self):
        # Этот метод больше не используется в меню, но если вдруг нужен как фолбэк:
        messagebox.showwarning("Запуск", "Модуль Заявок должен быть встроен в TabelSuite. Проверьте импорт.")

    def run_converter_exe(self):
        # Этот метод больше не используется в меню, но если вдруг нужен как фолбэк:
        messagebox.showwarning("Запуск", "Модуль Конвертера должен быть встроен в TabelSuite. Проверьте импорт.")

    def start_app():
    # Эта функция будет вызываться только после того, как MainApp полностью создан
    app = MainApp()
    
    # Теперь мы вызываем show_home здесь, гарантируя, что объект app полностью готов
    try:
        app.show_home()
    except AttributeError:
        # Если даже здесь ошибка, просто продолжаем, так как пользователь может использовать меню
        print("Warning: Failed to auto-display home page.")
        
    app.mainloop()

# --- Секция запуска (CLEANUP) ---
if __name__ == "__main__":
    # 1. Удаляем проверку 'if pd is None:'
    # В собранном EXE Pandas будет включен, а в среде разработки это задача разработчика.
    start_app()
