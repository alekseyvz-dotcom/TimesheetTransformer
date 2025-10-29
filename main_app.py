# python
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
from io import BytesIO
from datetime import datetime, date
from pathlib import Path
from typing import List, Tuple, Optional, Any, Dict

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

# Встроенные модули (если доступны)
try:
    import SpecialOrders
except Exception:
    SpecialOrders = None

try:
    import timesheet_transformer
except Exception:
    timesheet_transformer = None

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
KEY_SELECTED_DEP = "selected_department"
KEY_REMOTE_USE = "use_remote"
KEY_YA_PUBLIC_LINK = "yadisk_public_link"
KEY_YA_PUBLIC_PATH = "yadisk_public_path"

SPRAVOCHNIK_FILE_DEFAULT = "Справочник.xlsx"
OUTPUT_DIR_DEFAULT = "Объектные_табели"
CONVERTER_EXE = "TabelConverter.exe"

# ------------- Базовые утилиты -------------

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
        if KEY_SPR not in cfg[CONFIG_SECTION_PATHS]:
            cfg[CONFIG_SECTION_PATHS][KEY_SPR] = str(exe_dir() / SPRAVOCHNIK_FILE_DEFAULT)
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

        if not cfg.has_section(CONFIG_SECTION_REMOTE):
            cfg[CONFIG_SECTION_REMOTE] = {}
            changed = True
        if KEY_REMOTE_USE not in cfg[CONFIG_SECTION_REMOTE]:
            cfg[CONFIG_SECTION_REMOTE][KEY_REMOTE_USE] = "false"
            changed = True
        if KEY_YA_PUBLIC_LINK not in cfg[CONFIG_SECTION_REMOTE]:
            cfg[CONFIG_SECTION_REMOTE][KEY_YA_PUBLIC_LINK] = ""
            changed = True
        if KEY_YA_PUBLIC_PATH not in cfg[CONFIG_SECTION_REMOTE]:
            cfg[CONFIG_SECTION_REMOTE][KEY_YA_PUBLIC_PATH] = ""
            changed = True

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
    cfg[CONFIG_SECTION_INTEGR] = {KEY_EXPORT_PWD: "2025"}
    cfg[CONFIG_SECTION_REMOTE] = {
        KEY_REMOTE_USE: "false",
        KEY_YA_PUBLIC_LINK: "",
        KEY_YA_PUBLIC_PATH: "",
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

# ------------- Удалённый справочник: Я.Диск -------------

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
    objects: List[Tuple[str,str]] = []

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
    ws1.append(["Петров П. П.", "ST00-00002", "Электромонтер", "Электрика"])
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

# ------------- Общие утилиты -------------

def month_days(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]

def month_name_ru(month: int) -> str:
    names = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
    return names[month-1]

def parse_hours_value(v: Any) -> Optional[float]:
    s = str(v or "").strip()
    if not s:
        return None
    if "/" in s:
        total = 0.0
        any_part = False
        for part in s.split("/"):
            n = parse_hours_value(part)
            if isinstance(n, (int, float)):
                total += float(n); any_part = True
        return total if any_part else None
    if ":" in s:
        p = s.split(":")
        try:
            hh = float(p[0].replace(",", "."))
            mm = float((p[1] if len(p)>1 else "0").replace(",", "."))
            ss = float((p[2] if len(p)>2 else "0").replace(",", "."))
            return hh + mm/60.0 + ss/3600.0
        except:
            pass
    s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return None

def safe_filename(s: str, maxlen: int = 60) -> str:
    if not s:
        return "NOID"
    s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", str(s)).strip()
    s = re.sub(r"_+", "_", s)
    return s[:maxlen] if len(s) > maxlen else s

# ------------- Ряд реестра -------------

class RowWidget:
    WEEK_BG_SAT = "#fff8e1"
    WEEK_BG_SUN = "#ffebee"
    ZEBRA_EVEN = "#ffffff"
    ZEBRA_ODD = "#f6f8fa"
    ERR_BG = "#ffccbc"
    DISABLED_BG = "#f0f0f0"

    def __init__(self, parent, idx: int, fio: str, tbn: str, get_year_month_callable, on_delete_callable):
        self.parent = parent
        self.idx = idx
        self.get_year_month = get_year_month_callable
        self.on_delete = on_delete_callable

        self.zebra_bg = self.ZEBRA_EVEN if idx % 2 == 0 else self.ZEBRA_ODD
        self.frame = tk.Frame(parent, bd=0, bg=self.zebra_bg)

        # Колонка 0: ФИО
        self.cell_fio = tk.Frame(self.frame, width=200, height=1, bg=self.zebra_bg, bd=0)
        self.cell_fio.grid(row=0, column=0, padx=0, pady=0, sticky="ew")
        self.cell_fio.grid_propagate(False)
        self.lbl_fio = tk.Label(self.cell_fio, text=fio, anchor="w", bg=self.zebra_bg, padx=2)
        self.lbl_fio.pack(fill="both", expand=True)

        # Колонка 1: Таб.№
        self.cell_tbn = tk.Frame(self.frame, width=100, height=1, bg=self.zebra_bg, bd=0)
        self.cell_tbn.grid(row=0, column=1, padx=0, pady=0, sticky="ew")
        self.cell_tbn.grid_propagate(False)
        self.lbl_tbn = tk.Label(self.cell_tbn, text=tbn, anchor="center", bg=self.zebra_bg)
        self.lbl_tbn.pack(fill="both", expand=True)

        # Колонки 2-32: Дни
        self.day_entries: List[tk.Entry] = []
        self.day_frames: List[tk.Frame] = []
        for d in range(1, 32):
            day_frame = tk.Frame(self.frame, width=36, height=1, bg=self.zebra_bg, bd=0)
            day_frame.grid(row=0, column=1 + d, padx=0, pady=0, sticky="ew")
            day_frame.grid_propagate(False)
            self.day_frames.append(day_frame)
            
            e = tk.Entry(day_frame, width=4, justify="center", bd=1, relief="solid")
            e.pack(fill="both", expand=True, padx=0, pady=0)
            e.bind("<FocusOut>", lambda ev, _d=d: self.update_total())
            e.bind("<Button-2>", lambda ev: "break")
            e.bind("<ButtonRelease-2>", lambda ev: "break")
            self.day_entries.append(e)

        # Колонка 33: Дней
        self.lbl_days = tk.Label(self.frame, text="0", width=5, anchor="e", bg=self.zebra_bg)
        self.lbl_days.grid(row=0, column=33, padx=(4, 1), pady=0, sticky="ew")

        # Колонка 34: Часы
        self.lbl_total = tk.Label(self.frame, text="0", width=7, anchor="e", bg=self.zebra_bg)
        self.lbl_total.grid(row=0, column=34, padx=(4, 1), pady=0, sticky="ew")

        # Колонка 35: 5/2
        self.btn_52 = ttk.Button(self.frame, text="5/2", width=4, command=self.fill_52)
        self.btn_52.grid(row=0, column=35, padx=1, sticky="ew")

        # Колонка 36: Удалить
        self.btn_del = ttk.Button(self.frame, text="Удалить", width=7, command=self.delete_row)
        self.btn_del.grid(row=0, column=36, padx=1, sticky="ew")

    def apply_pixel_column_widths(self, px: dict):
        self.cell_fio.configure(width=px['fio'])
        self.cell_tbn.configure(width=px['tbn'])
        for day_frame in self.day_frames:
            day_frame.configure(width=px['day'])

        f = self.frame
        f.grid_columnconfigure(0, minsize=px['fio'], weight=0)
        f.grid_columnconfigure(1, minsize=px['tbn'], weight=0)
        for col in range(2, 33):
            f.grid_columnconfigure(col, minsize=px['day'], weight=0)
        f.grid_columnconfigure(33, minsize=px['days'], weight=0)
        f.grid_columnconfigure(34, minsize=px['hours'], weight=0)
        f.grid_columnconfigure(35, minsize=px['btn52'], weight=0)
        f.grid_columnconfigure(36, minsize=px['del'], weight=0)

    def set_day_font(self, font_tuple):
        for e in self.day_entries:
            e.configure(font=font_tuple)

    def grid(self, row: int):
        self.frame.grid(row=row, column=0, sticky="ew", padx=0, pady=0)

    def destroy(self):
        self.frame.destroy()

    def fio(self) -> str:
        return self.lbl_fio.cget("text")

    def tbn(self) -> str:
        return self.lbl_tbn.cget("text")

    def set_hours(self, arr: List[Optional[float]]):
        days = len(arr)
        for i in range(31):
            self.day_entries[i].delete(0, "end")
            if i < days and isinstance(arr[i], (int, float)) and abs(arr[i]) > 1e-12:
                s = f"{float(arr[i]):.2f}".rstrip("0").rstrip(".")
                self.day_entries[i].insert(0, s)
        self.update_total()

    def get_hours(self) -> List[Optional[float]]:
        return [parse_hours_value(e.get().strip()) for e in self.day_entries]

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
        val = parse_hours_value(raw) if raw else None
        invalid = False
        if raw and (val is None or val < 0 or val > 24):
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
        total_hours = 0.0
        total_days = 0
        y, m = self.get_year_month()
        days_in_m = month_days(y, m)
        for i, e in enumerate(self.day_entries, start=1):
            raw = e.get().strip()
            n = parse_hours_value(raw) if raw else None
            self._repaint_day_cell(i - 1, y, m)
            if i <= days_in_m and isinstance(n, (int, float)) and n > 1e-12:
                total_hours += float(n)
                total_days += 1
        self.lbl_days.config(text=str(total_days))
        sh = f"{total_hours:.2f}".rstrip("0").rstrip(".")
        self.lbl_total.config(text=sh)

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


# ------------- Автокомплит -------------

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

# ------------- Диалоги -------------

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

# ------------- Страница Объектного табеля (Frame) -------------

class TimesheetPage(tk.Frame):
    COLPX = {"fio": 200, "tbn": 100, "day": 36, "days": 46, "hours": 56, "btn52": 40, "del": 66}
    MIN_FIO_PX = 140
    MAX_FIO_PX = 260

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

    # ... методы _load_spr_data остаются без изменений ...

    def _build_ui(self):
      try:
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=8)

        # Ряд 0 — Подразделение
        tk.Label(top, text="Подразделение:").grid(row=0, column=0, sticky="w")
        deps = self.departments if getattr(self, "departments", None) else ["Все"]
        self.cmb_department = ttk.Combobox(top, state="readonly", values=deps, width=48)
        self.cmb_department.grid(row=0, column=1, sticky="w", padx=(4, 12))
        try:
            saved_dep = get_selected_department_from_config()
            if saved_dep in deps:
                self.cmb_department.set(saved_dep)
            else:
                self.cmb_department.set(deps[0])
        except Exception:
            self.cmb_department.set(deps[0])
        self.cmb_department.bind("<<ComboboxSelected>>", lambda e: self._on_department_select())

        # Ряд 1 — Период и Объект
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

        # Ряд 2 — ФИО/Таб№/Должность
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

        # Ряд 3 — Кнопки
        btns = tk.Frame(top)
        btns.grid(row=3, column=0, columnspan=8, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Добавить в табель", command=self.add_row).grid(row=0, column=0, padx=4)
        ttk.Button(btns, text="5/2 всем", command=self.fill_52_all).grid(row=0, column=1, padx=4)
        ttk.Button(btns, text="Проставить часы", command=self.fill_hours_all).grid(row=0, column=2, padx=4)
        ttk.Button(btns, text="Очистить все строки", command=self.clear_all_rows).grid(row=0, column=3, padx=4)
        ttk.Button(btns, text="Обновить справочник", command=self.reload_spravochnik).grid(row=0, column=4, padx=4)
        ttk.Button(btns, text="Копировать из месяца…", command=self.copy_from_month).grid(row=0, column=5, padx=4)
        ttk.Button(btns, text="Сохранить", command=self.save_all).grid(row=0, column=6, padx=4)

        # Основной контейнер с прокруткой
        main_frame = tk.Frame(self)
        main_frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        # Canvas для содержимого
        self.main_canvas = tk.Canvas(main_frame, borderwidth=0, highlightthickness=0)
        self.main_canvas.grid(row=0, column=0, sticky="nsew")

        # Скроллбары
        self.vscroll = ttk.Scrollbar(main_frame, orient="vertical", command=self.main_canvas.yview)
        self.vscroll.grid(row=0, column=1, sticky="ns")
        self.hscroll = ttk.Scrollbar(main_frame, orient="horizontal", command=self.main_canvas.xview)
        self.hscroll.grid(row=1, column=0, sticky="ew")

        # Настройка весов для grid
        main_frame.grid_rowconfigure(0, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)

        # Фрейм внутри Canvas
        self.scroll_frame = tk.Frame(self.main_canvas)
        self.canvas_window = self.main_canvas.create_window((0, 0), window=self.scroll_frame, anchor="nw")

        # Привязка скроллбаров
        self.main_canvas.configure(
            yscrollcommand=self.vscroll.set,
            xscrollcommand=self.hscroll.set
        )

        # Заголовок таблицы - теперь в scroll_frame как первая строка
        self.header_frame = tk.Frame(self.scroll_frame, relief="raised", bd=1, bg="#e0e0e0")
        self.header_frame.pack(anchor="nw", fill="x", pady=(0, 2))

        # Создание заголовков с фиксированными Frame
        self.header_cells = []

        # Колонка 0: ФИО
        cell_fio = tk.Frame(self.header_frame, width=200, height=1, bg="#d0d0d0", relief="flat", bd=0)
        cell_fio.grid(row=0, column=0, padx=0, pady=2, sticky="ew")
        cell_fio.grid_propagate(False)
        lbl = tk.Label(cell_fio, text="ФИО", anchor="w", font=("Segoe UI", 9, "bold"), bg="#d0d0d0", padx=2)
        lbl.pack(fill="both", expand=True)
        self.header_cells.append(cell_fio)

        # Колонка 1: Таб.№
        cell_tbn = tk.Frame(self.header_frame, width=100, height=1, bg="#d0d0d0", relief="flat", bd=0)
        cell_tbn.grid(row=0, column=1, padx=0, pady=2, sticky="ew")
        cell_tbn.grid_propagate(False)
        lbl = tk.Label(cell_tbn, text="Таб.№", anchor="center", font=("Segoe UI", 9, "bold"), bg="#d0d0d0")
        lbl.pack(fill="both", expand=True)
        self.header_cells.append(cell_tbn)

        # Колонки 2-32: Дни месяца
        self.header_day_cells = []
        for d in range(1, 32):
            cell = tk.Frame(self.header_frame, width=36, height=1, bg="#d0d0d0", relief="flat", bd=0)
            cell.grid(row=0, column=1 + d, padx=0, pady=2, sticky="ew")
            cell.grid_propagate(False)
            lbl = tk.Label(cell, text=str(d), anchor="center", font=("Segoe UI", 8, "bold"), bg="#d0d0d0")
            lbl.pack(fill="both", expand=True)
            self.header_day_cells.append(cell)

        # Колонка 33: Дней
        lbl = tk.Label(self.header_frame, text="Дней", anchor="e", font=("Segoe UI", 9, "bold"), bg="#d0d0d0")
        lbl.grid(row=0, column=33, padx=(4, 1), pady=2, sticky="ew")

        # Колонка 34: Часы
        lbl = tk.Label(self.header_frame, text="Часы", anchor="e", font=("Segoe UI", 9, "bold"), bg="#d0d0d0")
        lbl.grid(row=0, column=34, padx=(4, 1), pady=2, sticky="ew")

        # Колонка 35: 5/2
        lbl = tk.Label(self.header_frame, text="5/2", anchor="center", font=("Segoe UI", 9, "bold"), bg="#d0d0d0")
        lbl.grid(row=0, column=35, padx=1, pady=2, sticky="ew")

        # Колонка 36: Удалить
        lbl = tk.Label(self.header_frame, text="Удалить", anchor="center", font=("Segoe UI", 9, "bold"), bg="#d0d0d0")
        lbl.grid(row=0, column=36, padx=1, pady=2, sticky="ew")

        # Применяем ширины к заголовку
        self._apply_column_widths(self.header_frame)
        self._apply_header_widths()

        # Контейнер для строк данных
        self.rows_holder = tk.Frame(self.scroll_frame, bg="#ffffff")
        self.rows_holder.pack(anchor="nw", fill="both", expand=True)

        # Обновление области прокрутки
        self.scroll_frame.bind("<Configure>", self._on_scroll_frame_configure)

        # Обработка колеса мыши
        self.main_canvas.bind("<MouseWheel>", self._on_wheel)
        self.main_canvas.bind("<Shift-MouseWheel>", self._on_shift_wheel)
        self.bind_all("<MouseWheel>", self._on_wheel_anywhere)

        self.rows: List[RowWidget] = []

        # Нижняя панель
        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=8, pady=(0, 8))
        self.lbl_object_total = tk.Label(bottom, text="Сумма: сотрудников 0 | дней 0 | часов 0",
                                         font=("Segoe UI", 10, "bold"))
        self.lbl_object_total.pack(side="left")

        self._on_department_select()

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"Ошибка построения UI: {tb}")
        messagebox.showerror("Табель — ошибка построения UI", f"{e}\n\n{tb}")
        raise

    def _apply_column_widths(self, frame: tk.Frame):
        """Применение ширин колонок к сетке"""
        px = self.COLPX
        frame.grid_columnconfigure(0, minsize=px['fio'], weight=0)
        frame.grid_columnconfigure(1, minsize=px['tbn'], weight=0)
        for col in range(2, 33):
            frame.grid_columnconfigure(col, minsize=px['day'], weight=0)
        frame.grid_columnconfigure(33, minsize=px['days'], weight=0)
        frame.grid_columnconfigure(34, minsize=px['hours'], weight=0)
        frame.grid_columnconfigure(35, minsize=px['btn52'], weight=0)
        frame.grid_columnconfigure(36, minsize=px['del'], weight=0)

    def _apply_header_widths(self):
        """Применение точных ширин к ячейкам заголовка"""
        px = self.COLPX
        if self.header_cells and len(self.header_cells) >= 2:
            self.header_cells[0].configure(width=px['fio'])
            self.header_cells[1].configure(width=px['tbn'])
        for cell in self.header_day_cells:
            cell.configure(width=px['day'])

    def _regrid_rows(self):
        """Перерисовка всех строк данных"""
        for i, r in enumerate(self.rows):
            r.grid(i)
            r.apply_pixel_column_widths(self.COLPX)
            r.set_day_font(self.DAY_ENTRY_FONT)
        
        # Синхронизируем ширину rows_holder с header_frame
        self.rows_holder.update_idletasks()
        self.header_frame.update_idletasks()
        
        self.after(30, self._on_scroll_frame_configure)
        self._recalc_object_total()

    def _recalc_object_total(self):
        tot_h = 0.0
        tot_d = 0
        for r in self.rows:
            try:
                h = float(r.lbl_total.cget("text").replace(",", ".") or 0)
            except Exception:
                h = 0.0
            try:
                d = int(r.lbl_days.cget("text") or 0)
            except Exception:
                d = 0
            tot_h += h
            tot_d += d
        sh = f"{tot_h:.2f}".rstrip("0").rstrip(".")
        cnt = len(self.rows)
        self.lbl_object_total.config(text=f"Сумма: сотрудников {cnt} | дней {tot_d} | часов {sh}")

    def add_row(self):
        fio = self.fio_var.get().strip()
        tbn = self.ent_tbn.get().strip()
        if not fio:
            messagebox.showwarning("Объектный табель", "Выберите ФИО.")
            return

        key = (fio.strip().lower(), tbn.strip())
        if any((r.fio().strip().lower(), r.tbn().strip()) == key for r in self.rows):
            if not messagebox.askyesno("Дублирование",
                                       f"Сотрудник уже есть в реестре:\n{fio} (Таб.№ {tbn}).\nДобавить ещё одну строку?"):
                return

        w = RowWidget(self.rows_holder, len(self.rows) + 1, fio, tbn, self.get_year_month, self.delete_row)
        w.apply_pixel_column_widths(self.COLPX)
        w.set_day_font(self.DAY_ENTRY_FONT)
        y, m = self.get_year_month()
        w.update_days_enabled(y, m)
        self.rows.append(w)
        self._regrid_rows()

    def _on_department_select(self):
        dep_sel = (self.cmb_department.get() or "Все").strip()
        set_selected_department_in_config(dep_sel)
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
        for r in self.rows:
            r.fill_52()
        self._recalc_object_total()

    def fill_hours_all(self):
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
        if day > max_day:
            messagebox.showwarning("Проставить часы", f"В {month_name_ru(m)} {y} только {max_day} дней.")
            return

        if clear:
            for r in self.rows:
                e = r.day_entries[day - 1]
                e.delete(0, "end")
                r.update_total()
            self._recalc_object_total()
            messagebox.showinfo("Проставить часы", f"День {day} очищен у {len(self.rows)} сотрудников.")
            return

        hours_val = float(dlg.result["hours"])
        s = f"{hours_val:.2f}".rstrip("0").rstrip(".").replace(".", ",")
        for r in self.rows:
            e = r.day_entries[day - 1]
            e.delete(0, "end")
            if hours_val > 1e-12:
                e.insert(0, s)
            r.update_total()
        self._recalc_object_total()
        messagebox.showinfo("Проставить часы", f"Проставлено {s} ч в день {day} для {len(self.rows)} сотрудников.")

    def delete_row(self, roww: RowWidget):
        try:
            self.rows.remove(roww)
        except Exception:
            pass
        roww.destroy()
        self._regrid_rows()

    def clear_all_rows(self):
        if not self.rows:
            return
        if not messagebox.askyesno("Объектный табель", "Очистить все строки?"):
            return
        for r in self.rows:
            r.destroy()
        self.rows.clear()
        self._regrid_rows()

    def _current_file_path(self) -> Optional[Path]:
        addr = self.cmb_address.get().strip()
        oid = self.cmb_object_id.get().strip()
        if not addr and not oid:
            return None
        y, m = self.get_year_month()
        id_part = oid if oid else safe_filename(addr)
        return self.out_dir / f"Объектный_табель_{id_part}_{y}_{m:02d}.xlsx"

    def _file_path_for(self, year: int, month: int, addr: Optional[str] = None, oid: Optional[str] = None) -> Optional[Path]:
        addr = (addr if addr is not None else self.cmb_address.get().strip())
        oid = (oid if oid is not None else self.cmb_object_id.get().strip())
        if not addr and not oid:
            return None
        id_part = oid if oid else safe_filename(addr)
        return self.out_dir / f"Объектный_табель_{id_part}_{year}_{month:02d}.xlsx"

    def _ensure_sheet(self, wb) -> Any:
        if "Табель" in wb.sheetnames:
            ws = wb["Табель"]
            hdr_first = str(ws.cell(1, 1).value or "")
            if hdr_first == "ID объекта" and ws.max_column >= (6 + 31 + 2):
                return ws
            base = "Табель_OLD"
            new_name = base
            i = 1
            while new_name in wb.sheetnames:
                i += 1
                new_name = f"{base}{i}"
            ws.title = new_name
        ws2 = wb.create_sheet("Табель")
        hdr = ["ID объекта", "Адрес", "Месяц", "Год", "ФИО", "Табельный №"] + [str(i) for i in range(1, 32)] + [
            "Итого дней", "Итого часов"
        ]
        ws2.append(hdr)
        ws2.column_dimensions["A"].width = 14
        ws2.column_dimensions["B"].width = 40
        ws2.column_dimensions["C"].width = 10
        ws2.column_dimensions["D"].width = 8
        ws2.column_dimensions["E"].width = 28
        ws2.column_dimensions["F"].width = 14
        for i in range(7, 7 + 31):
            ws2.column_dimensions[get_column_letter(i)].width = 6
        ws2.column_dimensions[get_column_letter(7 + 31)].width = 10
        ws2.column_dimensions[get_column_letter(7 + 31 + 1)].width = 12
        ws2.freeze_panes = "A2"
        return ws2

    def _load_existing_rows(self):
        for r in list(self.rows):
            r.destroy()
        self.rows.clear()
        self._regrid_rows()

        fpath = self._current_file_path()
        if not fpath or not fpath.exists():
            return
        try:
            wb = load_workbook(fpath)
            ws = self._ensure_sheet(wb)
            y, m = self.get_year_month()
            addr = self.cmb_address.get().strip()
            oid = self.cmb_object_id.get().strip()
            for r in range(2, ws.max_row + 1):
                row_oid = (ws.cell(r, 1).value or "")
                row_addr = (ws.cell(r, 2).value or "")
                row_m = int(ws.cell(r, 3).value or 0)
                row_y = int(ws.cell(r, 4).value or 0)
                fio = (ws.cell(r, 5).value or "")
                tbn = (ws.cell(r, 6).value or "")
                if row_m != m or row_y != y:
                    continue
                if oid:
                    if row_oid != oid:
                        continue
                else:
                    if row_addr != addr:
                        continue
                hours: List[Optional[float]] = []
                for c in range(7, 7 + 31):
                    v = ws.cell(r, c).value
                    try:
                        n = float(v) if isinstance(v, (int, float)) else parse_hours_value(v)
                    except Exception:
                        n = None
                    hours.append(n)
                roww = RowWidget(self.rows_holder, len(self.rows) + 1, fio, tbn, self.get_year_month, self.delete_row)
                roww.apply_pixel_column_widths(self.COLPX)
                roww.set_day_font(self.DAY_ENTRY_FONT)
                roww.update_days_enabled(y, m)
                roww.set_hours(hours)
                self.rows.append(roww)
            self._regrid_rows()
        except Exception as e:
            messagebox.showerror("Загрузка", f"Не удалось загрузить существующие строки:\n{e}")

    def save_all(self):
        fpath = self._current_file_path()
        if not fpath:
            messagebox.showwarning("Сохранение", "Укажите адрес и/или ID объекта, а также период.")
            return

        addr = self.cmb_address.get().strip()
        oid = self.cmb_object_id.get().strip()
        y, m = self.get_year_month()

        try:
            if fpath.exists():
                wb = load_workbook(fpath)
            else:
                fpath.parent.mkdir(parents=True, exist_ok=True)
                wb = Workbook()
                if wb.active:
                    wb.remove(wb.active)
            ws = self._ensure_sheet(wb)

            to_del = []
            for r in range(2, ws.max_row + 1):
                row_oid = (ws.cell(r, 1).value or "")
                row_addr = (ws.cell(r, 2).value or "")
                row_m = int(ws.cell(r, 3).value or 0)
                row_y = int(ws.cell(r, 4).value or 0)
                if row_m == m and row_y == y and ((oid and row_oid == oid) or (not oid and row_addr == addr)):
                    to_del.append(r)
            for r in reversed(to_del):
                ws.delete_rows(r, 1)

            for roww in self.rows:
                hours = roww.get_hours()
                total_hours = sum(h for h in hours if isinstance(h, (int, float))) if hours else 0.0
                total_days = sum(1 for h in hours if isinstance(h, (int, float)) and h > 1e-12)
                row_values = [oid, addr, m, y, roww.fio(), roww.tbn()] + [
                    (None if hours[i] is None or abs(float(hours[i])) < 1e-12 else float(hours[i]))
                    for i in range(31)
                ] + [total_days if total_days else None, None if abs(total_hours) < 1e-12 else float(total_hours)]
                ws.append(row_values)

            wb.save(fpath)
            messagebox.showinfo("Сохранение", f"Сохранено:\n{fpath}")
        except Exception as e:
            messagebox.showerror("Сохранение", f"Ошибка сохранения:\n{e}")

    def copy_from_month(self):
        addr = self.cmb_address.get().strip()
        oid = self.cmb_object_id.get().strip()
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

        src_path = self._file_path_for(src_y, src_m, addr=addr, oid=oid)
        if not src_path or not src_path.exists():
            messagebox.showwarning("Копирование", f"Не найден файл источника:\n{src_path}")
            return

        try:
            wb = load_workbook(src_path, data_only=True)
            ws = self._ensure_sheet(wb)
            y, m = self.get_year_month()

            found = []
            for r in range(2, ws.max_row + 1):
                row_oid = (ws.cell(r, 1).value or "")
                row_addr = (ws.cell(r, 2).value or "")
                row_m = int(ws.cell(r, 3).value or 0)
                row_y = int(ws.cell(r, 4).value or 0)
                fio = str(ws.cell(r, 5).value or "").strip()
                tbn = str(ws.cell(r, 6).value or "").strip()

                if row_m != src_m or row_y != src_y:
                    continue
                if oid:
                    if row_oid != oid:
                        continue
                else:
                    if row_addr != addr:
                        continue

                hrs = []
                if with_hours:
                    for c in range(7, 7 + 31):
                        v = ws.cell(r, c).value
                        try:
                            n = float(v) if isinstance(v, (int, float)) else parse_hours_value(v)
                        except Exception:
                            n = None
                        hrs.append(n)

                if fio:
                    found.append((fio, tbn, hrs))

            if not found:
                messagebox.showinfo("Копирование", "В источнике нет сотрудников для выбранного объекта и периода.")
                return

            uniq = {}
            for fio, tbn, hrs in found:
                key = (fio.strip().lower(), tbn.strip())
                if key not in uniq:
                    uniq[key] = (fio, tbn, hrs)
            found = list(uniq.values())

            added = 0
            if mode == "replace":
                for r in self.rows:
                    r.destroy()
                self.rows.clear()

            existing = {(r.fio().strip().lower(), r.tbn().strip()) for r in self.rows}

            dy, dm = self.get_year_month()
            for fio, tbn, hrs in found:
                key = (fio.strip().lower(), tbn.strip())
                if mode == "merge" and key in existing:
                    continue
                roww = RowWidget(self.rows_holder, len(self.rows) + 1, fio, tbn, self.get_year_month, self.delete_row)
                roww.apply_pixel_column_widths(self.COLPX)
                roww.set_day_font(self.DAY_ENTRY_FONT)
                roww.update_days_enabled(dy, dm)
                if with_hours and hrs:
                    roww.set_hours(hrs)
                self.rows.append(roww)
                added += 1

            self._regrid_rows()
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
            self._apply_column_widths(self.header_frame)
            self._apply_header_widths()
            for r in self.rows:
                r.apply_pixel_column_widths(self.COLPX)
            self._on_scroll_frame_configure()

    def _on_window_configure(self, _evt):
        try:
            self.after_cancel(self._fit_job)
        except Exception:
            pass
        self._fit_job = self.after(150, self._auto_fit_columns)

# ------------- Сводный экспорт -------------

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
            row_m = int(ws.cell(r, 3).value or 0)
            row_y = int(ws.cell(r, 4).value or 0)
            if row_m != month or row_y != year:
                continue
            row_oid = (ws.cell(r, 1).value or "")
            row_addr = (ws.cell(r, 2).value or "")
            fio = (ws.cell(r, 5).value or "")
            tbn = (ws.cell(r, 6).value or "")
            hours: List[Optional[float]] = []
            for c in range(7, 7 + 31):
                v = ws.cell(r, c).value
                try:
                    n = float(v) if isinstance(v, (int, float)) else parse_hours_value(v)
                except Exception:
                    n = None
                hours.append(n)
            total_days = sum(1 for h in hours if isinstance(h, (int, float)) and h > 1e-12)
            total_hours = sum(h for h in hours if isinstance(h, (int, float)))
            row_values = [row_oid, row_addr, month, year, fio, tbn] + [
                (None if (h is None or abs(float(h)) < 1e-12) else float(h)) for h in hours
            ] + [total_days if total_days else None,
                 None if (not isinstance(total_hours, (int, float)) or abs(total_hours) < 1e-12) else float(total_hours)]
            rows.append(row_values)

    if not rows:
        return 0, []

    sum_dir = exe_dir() / "Сводные_отчеты"
    sum_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []

    hdr = ["ID объекта", "Адрес", "Месяц", "Год", "ФИО", "Табельный №"] + [str(i) for i in range(1, 32)] + [
        "Итого дней", "Итого часов"
    ]

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
        for i in range(7, 7 + 31):
            ws_out.column_dimensions[get_column_letter(i)].width = 6
        ws_out.column_dimensions[get_column_letter(7 + 31)].width = 10
        ws_out.column_dimensions[get_column_letter(7 + 31 + 1)].width = 12
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

# ------------- Домашняя страница -------------

class HomePage(tk.Frame):
    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        wrap = tk.Frame(self, bg="#f7f7f7")
        wrap.pack(pady=30)
        tk.Label(wrap, text="Добро пожаловать!", font=("Segoe UI", 16, "bold"), bg="#f7f7f7").pack(anchor="center", pady=(0, 6))
        tk.Label(wrap, text="Выберите раздел в верхнем меню.\n"
                            "Объектный табель → Создать — для работы с табелями.",
                 font=("Segoe UI", 10), fg="#444", bg="#f7f7f7", justify="center").pack(anchor="center")

# ------------- Главное окно (единое) -------------

class MainApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1024x720")
        self.minsize(980, 640)
        self.resizable(True, True)

        ensure_config()

        # Меню
        menubar = tk.Menu(self)

        menubar.add_command(label="Главная", command=self.show_home)

        m_ts = tk.Menu(menubar, tearoff=0)
        m_ts.add_command(label="Создать", command=lambda: self._show_page("timesheet", lambda parent: TimesheetPage(parent)))
        menubar.add_cascade(label="Объектный табель", menu=m_ts)

        m_transport = tk.Menu(menubar, tearoff=0)
        if SpecialOrders and hasattr(SpecialOrders, "open_special_orders"):
            m_transport.add_command(label="Заявка на автотранспорт", command=lambda: SpecialOrders.open_special_orders(self))
        else:
            m_transport.add_command(label="Заявка на автотранспорт", command=self.run_special_orders_exe)
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
        menubar.add_cascade(label="Инструменты", menu=m_tools)

        self.config(menu=menubar)

        # Шапка
        header = tk.Frame(self)
        header.pack(fill="x", padx=12, pady=(10, 4))
        tk.Label(header, text="Управление строительством", font=("Segoe UI", 16, "bold")).pack(side="left")
        tk.Label(header, text="Выберите раздел в верхнем меню", font=("Segoe UI", 10), fg="#555").pack(side="right")

        # Контент
        self.content = tk.Frame(self, bg="#f7f7f7")
        self.content.pack(fill="both", expand=True)
        self._pages: Dict[str, tk.Widget] = {}

        # Копирайт
        footer = tk.Frame(self)
        footer.pack(fill="x", padx=12, pady=(0, 10))
        tk.Label(footer, text="Разработал Алексей Зезюкин, АНО МЛСТ 2025",
                 font=("Segoe UI", 8), fg="#666").pack(side="right")

        self.show_home()

    def _show_page(self, key: str, builder):
        for w in self.content.winfo_children():
            try:
                w.destroy()
            except Exception:
                pass
        page = builder(self.content)
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
            messagebox.showinfo("Сводный экспорт", msg)
        except Exception as e:
            messagebox.showerror("Сводный экспорт", f"Ошибка выгрузки:\n{e}")

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
            p = exe_dir() / CONVERTER_EXE
            if not p.exists():
                messagebox.showwarning("Конвертер", f"Не найден {CONVERTER_EXE} рядом с программой.")
                return
            subprocess.Popen([str(p)], shell=False)
        except Exception as e:
            messagebox.showerror("Конвертер", f"Не удалось запустить конвертер:\n{e}")

if __name__ == "__main__":
    app = MainApp()
    app.mainloop()


