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
    pd = None

# PIL/ImageTk удалены, чтобы устранить блокировку

# Мягкий импорт модулей
try:
    import BudgetAnalyzer
except Exception:
    BudgetAnalyzer = None
try:
    import assets_logo as _assets_logo
    _LOGO_BASE64 = getattr(_assets_logo, "LOGO_BASE64", None)
except Exception:
    _LOGO_BASE64 = None
try:
    import SpecialOrders
except Exception:
    SpecialOrders = None
try:
    import timesheet_transformer
except Exception:
    timesheet_transformer = None
    
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

class TimesheetSchema(NamedTuple):
    ID_OBJECT: int = 1
    ADDRESS: int = 2
    MONTH: int = 3
    YEAR: int = 4
    FIO: int = 5
    TBN: int = 6
    DEPARTMENT: int = 7
    DAILY_HOURS_START: int = 8
    TOTAL_DAYS: int = 39
    TOTAL_HOURS: int = 40
    OVERTIME_DAY: int = 41
    OVERTIME_NIGHT: int = 42
    
TS_SCHEMA = TimesheetSchema()
TOTAL_DATA_COLUMNS = TS_SCHEMA.OVERTIME_NIGHT

# ------------- БАЗОВЫЕ УТИЛИТЫ -------------

def exe_dir() -> Path:
    """Определяет корневую директорию EXE или скрипта."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

def config_path() -> Path:
    return exe_dir() / CONFIG_FILE
    
# Функции embedded_logo_image и log_message удалены

# ------------- ФУНКЦИИ КОНФИГУРАЦИИ -------------

def ensure_config():
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
    
    try:
        return float(s.replace(",", "."))
    except:
        return 0.0

def parse_day_entry(v: Any) -> ParsedHours:
    s = str(v or "").strip()
    if not s:
        return ParsedHours(is_valid=True)
    
    base_hours_str = s
    ot_day = 0.0
    ot_night = 0.0
    
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
            return ParsedHours(raw_input=s)

    total_base_hours = 0.0
    
    if "/" in base_hours_str:
        any_part = False
        for part in base_hours_str.split("/"):
            h = parse_time_string(part)
            total_base_hours += h
            if h > 1e-12: any_part = True
        if not any_part and base_hours_str.strip():
             return ParsedHours(raw_input=s)
    else:
        total_base_hours = parse_time_string(base_hours_str)
        if total_base_hours == 0.0 and base_hours_str.strip():
             return ParsedHours(raw_input=s)

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

        zebra_bg = self.ZEBRA_EVEN if (row_index % 2 == 0) else self.ZEBRA_ODD
        self.widgets: List[tk.Widget] = []
        
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
            
            col = self.table.grid_info(event.widget)['column']
            
            if TS_SCHEMA.DAILY_HOURS_START <= col <= TS_SCHEMA.DAILY_HOURS_START + 30:
                col_index = col - TS_SCHEMA.DAILY_HOURS_START
                
                values = re.split(r'[\t\n\r]+', pasted_data.strip())
                
                for i, val in enumerate(values):
                    if col_index + i < 31:
                        e = self.day_entries[col_index + i]
                        e.delete(0, tk.END)
                        e.insert(0, val)
                
                self.update_total()
                return "break"
                
        except Exception:
            pass
        
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
            
            self.parsed_hours_cache[i] = parse_day_entry(raw_input)
            
        self.update_total()

    def get_hours_with_overtime(self) -> List[ParsedHours]:
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
            
            if parsed.raw_input != self.day_entries[i].get().strip():
                self._update_parsed_cache(i)
                parsed = self.parsed_hours_cache[i]
                self._repaint_day_cell(i, y, m)
            
            if parsed.is_valid and parsed.hours > 1e-12:
                total_hours += parsed.hours
                total_days += 1
                total_overtime_day += parsed.ot_day
                total_overtime_night += parsed.ot_night
        
        self.lbl_days.config(text=str(total_days))
        sh = f"{total_hours:.2f}".rstrip("0").rstrip(".")
        self.lbl_total.config(text=sh)
        sod = f"{total_overtime_day:.2f}".rstrip("0").rstrip(".")
        self.lbl_overtime_day.config(text=sod)
        son = f"{total_overtime_night:.2f}".rstrip("0").rstrip(".")
        self.lbl_overtime_night.config(text=son)
        
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

class HomePage(tk.Frame):
    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        # log_message("HOMEPAGE: Super init done.") # Отладочное логирование убрано

        outer = tk.Frame(self, bg="#f7f7f7")
        outer.pack(fill="both", expand=True)

        center = tk.Frame(outer, bg="#f7f7f7")
        center.place(relx=0.5, rely=0.5, anchor="center")

        # --- БЛОК ЛОГОТИПА УДАЛЕН ---
        
        tk.Label(center, text="Добро пожаловать!", font=("Segoe UI", 18, "bold"), bg="#f7f7f7")\
            .pack(anchor="center", pady=(4, 6))
        tk.Label(center, text="Выберите раздел в верхнем меню.\nОбъектный табель → Создать — для работы с табелями.",
                 font=("Segoe UI", 10), fg="#444", bg="#f7f7f7", justify="center").pack(anchor="center")
        # log_message("HOMEPAGE: UI constructed successfully.")

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

    all_data_frames = []
    
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
    
    for d in range(1, 32):
        all_col_names[TS_SCHEMA.DAILY_HOURS_START - 1 + (d - 1)] = str(d)

    dtype_map = {
        "ID объекта": str, "Адрес": str, "ФИО": str, "Табельный №": str, "Подразделение": str,
        "Месяц": 'int16', "Год": 'int16',
    }
    
    for f in files:
        try:
            df = pd.read_excel(
                f, 
                sheet_name="Табель", 
                header=None, 
                skiprows=1,
                dtype=dtype_map
            )
            
            df = df.rename(columns=all_col_names)
            
            df = df[(df['Год'] == year) & (df['Месяц'] == month)]
            
            if not df.empty:
                all_data_frames.append(df)
                
        except Exception as e:
            print(f"Ошибка чтения файла {f.name}: {e}")
            continue

    if not all_data_frames:
        return 0, []

    final_df = pd.concat(all_data_frames, ignore_index=True)
    
    final_cols = [v for k, v in sorted(all_col_names.items())]
    final_df = final_df[final_cols]
    
    count = len(final_df)
    
    sum_dir = exe_dir() / "Сводные_отчеты"
    sum_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []

    file_name_base = f"Сводный_{year}_{month:02d}"

    if fmt in ("xlsx", "both"):
        p = sum_dir / f"{file_name_base}.xlsx"
        try:
            writer = pd.ExcelWriter(p, engine='xlsxwriter')
            final_df.to_excel(writer, sheet_name='Сводный', index=False)
            
            workbook = writer.book
            worksheet = writer.sheets['Сводный']
            
            for i, col_name in enumerate(final_df.columns):
                width = 10 
                if col_name == "Адрес": width = 40
                elif col_name == "ФИО": width = 28
                elif col_name == "Подразделение": width = 20
                elif len(col_name) <= 2: width = 6
                worksheet.set_column(i, i, width)
                
            writer.close()
            paths.append(p)
        except Exception as e:
            messagebox.showerror("Экспорт", f"Ошибка записи XLSX:\n{e}")

    if fmt in ("csv", "both"):
        p = sum_dir / f"{file_name_base}.csv"
        try:
            final_df.to_csv(p, sep=';', encoding='utf-8-sig', index=False)
            paths.append(p)
        except Exception as e:
            messagebox.showerror("Экспорт", f"Ошибка записи CSV:\n{e}")

    return count, paths

# ------------- Главное окно (единоe) -------------

class MainApp(tk.Tk):
    # --- МЕТОДЫ-УТИЛИТЫ ---

    def _show_page(self, key: str, builder):
        for w in self.content.winfo_children():
            try: w.destroy()
            except Exception: pass
        page = builder(self.content)
        if isinstance(page, tk.Widget) and page.master is self.content:
            try: page.pack_forget()
            except Exception: pass
        try: page.pack(fill="both", expand=True)
        except Exception: pass
        self._pages[key] = page

    def show_home(self):
        self._show_page("home", lambda parent: HomePage(parent))

    def open_spravochnik(self):
        path = get_spr_path_from_config()
        ensure_spravochnik_local(path)
        try:
            os.startfile(path)
        except Exception as e:
            messagebox.showerror("Справочник", f"Не удалось открыть файл:\n{e}")

    def refresh_spravochnik_global(self):
        cfg = read_config()
        use_remote = cfg.get(CONFIG_SECTION_REMOTE, KEY_REMOTE_USE, fallback="false")
        link = cfg.get(CONFIG_SECTION_REMOTE, KEY_YA_PUBLIC_LINK, fallback="")
        path = get_spr_path_from_config()
        ensure_spravochnik_local(path)
        messagebox.showinfo(
            "Справочник",
            "Справочник проверен/создан локально.\n"
            f"Удалённый доступ: use_remote={use_remote}\n"
            f"Публичная ссылка: {link or '(не задана)'}\n\n"
            "В окнах используйте «Обновить справочник» для перечтения."
        )

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
            traceback.print_exc()

    def run_special_orders_exe(self):
        messagebox.showwarning("Запуск", "Модуль Заявок должен быть встроен в TabelSuite. Проверьте импорт.")

    def run_converter_exe(self):
        messagebox.showwarning("Запуск", "Модуль Конвертера должен быть встроен в TabelSuite. Проверьте импорт.")
    
    # --- КОНСТРУКТОР ---
    def __init__(self):
        super().__init__()
        # log_message("INIT: Starting MainApp.__init__") # Удалено логирование
        
        ensure_config()
        # log_message("INIT: Config ensured.") # Удалено логирование

        self.title(APP_NAME)
        self.geometry("1024x720")
        self.minsize(980, 640)
        self.resizable(True, True)

        s = ttk.Style(self)
        s.configure('Accent.TButton', background='#4CAF50', foreground='black', font=('Segoe UI', 9, 'bold'))
        s.map('Accent.TButton', background=[('active', '#66BB6A')])

        # Меню
        menubar = tk.Menu(self)

        menubar.add_command(label="Главная", command=self.show_home) 
        m_ts = tk.Menu(menubar, tearoff=0)
        m_ts.add_command(label="Создать", command=lambda: self._show_page("timesheet", lambda parent: TimesheetPage(parent)))
        menubar.add_cascade(label="Объектный табель", menu=m_ts)

        m_transport = tk.Menu(menubar, tearoff=0)
        if SpecialOrders and hasattr(SpecialOrders, "create_page"):
            m_transport.add_command(label="📝 Создать заявку", command=lambda: self._show_page("transport", lambda parent: SpecialOrders.create_page(parent)))
        else:
            m_transport.add_command(label="📝 Создать заявку", command=self.run_special_orders_exe)
             
        if SpecialOrders and hasattr(SpecialOrders, "create_planning_page"):
            m_transport.add_command(label="🚛 Планирование транспорта", command=lambda: self._show_page("planning", lambda parent: SpecialOrders.create_planning_page(parent)))
        m_transport.add_separator()
        m_transport.add_command(label="📂 Открыть папку заявок", command=self.open_orders_folder)
        menubar.add_cascade(label="Автотранспорт", menu=m_transport)
        
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

        self.config(menu=menubar)

        header = tk.Frame(self)
        header.pack(fill="x", padx=12, pady=(10, 4))
        tk.Label(header, text="Управление строительством", font=("Segoe UI", 16, "bold")).pack(side="left")
        tk.Label(header, text="Выберите раздел в верхнем меню", font=("Segoe UI", 10), fg="#555").pack(side="right")

        self.content = tk.Frame(self, bg="#f7f7f7")
        self.content.pack(fill="both", expand=True)
        self._pages: Dict[str, tk.Widget] = {}

        footer = tk.Frame(self)
        footer.pack(fill="x", padx=12, pady=(0, 10))
        tk.Label(footer, text="Разработал Алексей Зезюкин, АНО МЛСТ 2025",
                 font=("Segoe UI", 8), fg="#666").pack(side="right")
        
        self.after(100, self.show_home) 
        # log_message("INIT: after(100, show_home) scheduled.") # Удалено логирование


# ------------- СЕКЦИЯ ЗАПУСКА -------------

if __name__ == "__main__":
    # УДАЛЕНА ОЧИСТКА ЛОГА
    # log_message("START: Application launch initiated.") # Удалено логирование
    
    app = MainApp()
    app.mainloop()
    
    # log_message("END: Application closed normally.") # Удалено логирование
