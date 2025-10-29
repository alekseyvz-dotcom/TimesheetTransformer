import os
import re
import sys
import subprocess
import calendar
import configparser
import json
import urllib.request
import urllib.parse
from io import BytesIO
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional, Any

import tkinter as tk
from tkinter import ttk, messagebox

try:
    import timesheet_transformer  # для пункта меню "Конвертер"
except Exception:
    timesheet_transformer = None

# openpyxl
from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

APP_NAME = "Управление строительством (Главное меню)"

# Конфиг и файлы
CONFIG_FILE = "tabel_config.ini"

CONFIG_SECTION_PATHS = "Paths"
CONFIG_SECTION_UI = "UI"
CONFIG_SECTION_INTEGR = "Integrations"
CONFIG_SECTION_REMOTE = "Remote"

# Paths
KEY_SPR = "spravochnik_path"
KEY_OUTPUT_DIR = "output_dir"

# UI
KEY_EXPORT_PWD = "export_password"

# Remote spravochnik
KEY_REMOTE_USE = "use_remote"                # true|false
KEY_YADISK_PUBLIC_LINK = "yadisk_public_link"
KEY_YADISK_PUBLIC_PATH = "yadisk_public_path"

# Прочее
SPRAVOCHNIK_FILE = "Справочник.xlsx"
OUTPUT_DIR_DEFAULT = "Объектные_табели"

SUMMARY_DIR = "Сводные_отчеты"

# ------------------------- Утилиты/конфиг -------------------------

def exe_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

def config_path() -> Path:
    return exe_dir() / CONFIG_FILE

def ensure_config_exists():
    cp = config_path()
    if cp.exists():
        cfg = configparser.ConfigParser()
        cfg.read(cp, encoding="utf-8")
        changed = False

        if not cfg.has_section(CONFIG_SECTION_PATHS):
            cfg[CONFIG_SECTION_PATHS] = {}
            changed = True
        if KEY_SPR not in cfg[CONFIG_SECTION_PATHS]:
            cfg[CONFIG_SECTION_PATHS][KEY_SPR] = str(exe_dir() / SPRAVOCHNIK_FILE); changed = True
        if KEY_OUTPUT_DIR not in cfg[CONFIG_SECTION_PATHS]:
            cfg[CONFIG_SECTION_PATHS][KEY_OUTPUT_DIR] = str(exe_dir() / OUTPUT_DIR_DEFAULT); changed = True

        if not cfg.has_section(CONFIG_SECTION_UI):
            cfg[CONFIG_SECTION_UI] = {}
            changed = True
        if KEY_EXPORT_PWD not in cfg[CONFIG_SECTION_UI]:
            cfg[CONFIG_SECTION_UI][KEY_EXPORT_PWD] = "2025"; changed = True

        if not cfg.has_section(CONFIG_SECTION_REMOTE):
            cfg[CONFIG_SECTION_REMOTE] = {}
            changed = True
        if KEY_REMOTE_USE not in cfg[CONFIG_SECTION_REMOTE]:
            cfg[CONFIG_SECTION_REMOTE][KEY_REMOTE_USE] = "false"; changed = True
        if KEY_YADISK_PUBLIC_LINK not in cfg[CONFIG_SECTION_REMOTE]:
            cfg[CONFIG_SECTION_REMOTE][KEY_YADISK_PUBLIC_LINK] = ""; changed = True
        if KEY_YADISK_PUBLIC_PATH not in cfg[CONFIG_SECTION_REMOTE]:
            cfg[CONFIG_SECTION_REMOTE][KEY_YADISK_PUBLIC_PATH] = ""; changed = True

        if changed:
            with open(cp, "w", encoding="utf-8") as f:
                cfg.write(f)
        return

    # создать с нуля
    cfg = configparser.ConfigParser()
    cfg[CONFIG_SECTION_PATHS] = {
        KEY_SPR: str(exe_dir() / SPRAVOCHNIK_FILE),
        KEY_OUTPUT_DIR: str(exe_dir() / OUTPUT_DIR_DEFAULT),
    }
    cfg[CONFIG_SECTION_UI] = {
        KEY_EXPORT_PWD: "2025"
    }
    cfg[CONFIG_SECTION_REMOTE] = {
        KEY_REMOTE_USE: "false",
        KEY_YADISK_PUBLIC_LINK: "",
        KEY_YADISK_PUBLIC_PATH: ""
    }
    with open(cp, "w", encoding="utf-8") as f:
        cfg.write(f)

def read_config() -> configparser.ConfigParser:
    ensure_config_exists()
    cfg = configparser.ConfigParser()
    cfg.read(config_path(), encoding="utf-8")
    return cfg

def get_spr_path_from_config() -> Path:
    cfg = read_config()
    raw = cfg.get(CONFIG_SECTION_PATHS, KEY_SPR, fallback=str(exe_dir() / SPRAVOCHNIK_FILE))
    return Path(os.path.expandvars(raw))

def get_output_dir_from_config() -> Path:
    cfg = read_config()
    raw = cfg.get(CONFIG_SECTION_PATHS, KEY_OUTPUT_DIR, fallback=str(exe_dir() / OUTPUT_DIR_DEFAULT))
    return Path(os.path.expandvars(raw))

def get_export_password_from_config() -> str:
    cfg = read_config()
    return cfg.get(CONFIG_SECTION_UI, KEY_EXPORT_PWD, fallback="2025").strip()

# ------------------------- Remote spravochnik (Яндекс Диск) -------------------------

def fetch_yadisk_public_bytes(public_link: str, public_path: str = "") -> bytes:
    """
    По публичной ссылке Я.Диска получаем прямую ссылку и скачиваем файл.
    Если public_path задан (для публичной ПАПКИ) — указываем относительный путь внутри ресурса.
    """
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
    with urllib.request.urlopen(href, timeout=45) as f:
        return f.read()

def month_days(year: int, month: int) -> int:
    import calendar as cal
    return cal.monthrange(year, month)[1]

def month_name_ru(month: int) -> str:
    names = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
    return names[month-1]

def safe_filename(s: str, maxlen: int = 60) -> str:
    if not s:
        return "NOID"
    s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", str(s)).strip()
    s = re.sub(r"_+", "_", s)
    return s[:maxlen] if len(s) > maxlen else s

def load_spravochnik_from_wb(wb) -> Tuple[List[Tuple[str,str,str,str]], List[Tuple[str,str]]]:
    """
    Парсинг openpyxl.Workbook -> (employees, objects)
    employees: [(fio,tbn,pos,dep)]
    objects: [(id, addr)]
    """
    def s(v):
        if v is None: return ""
        if isinstance(v, float) and v.is_integer(): v = int(v)
        return str(v).strip()

    employees: List[Tuple[str,str,str,str]] = []
    objects: List[Tuple[str,str]] = []

    if "Сотрудники" in wb.sheetnames:
        ws = wb["Сотрудники"]
        hdr = [s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        have_pos = ("должность" in hdr) or (len(hdr) >= 3)
        have_dep = ("подразделение" in hdr) or (len(hdr) >= 4)
        for r in ws.iter_rows(min_row=2, values_only=True):
            fio = s(r[0] if r and len(r)>0 else "")
            tbn = s(r[1] if r and len(r)>1 else "")
            pos = s(r[2] if have_pos and r and len(r)>2 else "")
            dep = s(r[3] if have_dep and r and len(r)>3 else "")
            if fio:
                employees.append((fio, tbn, pos, dep))

    if "Объекты" in wb.sheetnames:
        ws = wb["Объекты"]
        hdr = [s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        have_two = ("id объекта" in hdr) or (len(hdr) >= 2)
        for r in ws.iter_rows(min_row=2, values_only=True):
            if have_two:
                oid = s(r[0] if r and len(r)>0 else "")
                addr = s(r[1] if r and len(r)>1 else "")
            else:
                oid = ""
                addr = s(r[0] if r and len(r)>0 else "")
            if oid or addr:
                objects.append((oid, addr))

    return employees, objects

def load_spravochnik_remote_or_local(local_path: Path) -> Tuple[List[Tuple[str,str,str,str]], List[Tuple[str,str]]]:
    """
    Если [Remote]use_remote=true и задана ссылка — грузим Справочник.xlsx с Я.Диска.
    Иначе — читаем локальный файл (fallback).
    """
    cfg = read_config()
    use_remote = cfg.get(CONFIG_SECTION_REMOTE, KEY_REMOTE_USE, fallback="false").strip().lower() in ("1","true","yes","on")
    if use_remote:
        try:
            public_link = cfg.get(CONFIG_SECTION_REMOTE, KEY_YADISK_PUBLIC_LINK, fallback="").strip()
            public_path = cfg.get(CONFIG_SECTION_REMOTE, KEY_YADISK_PUBLIC_PATH, fallback="").strip()
            raw = fetch_yadisk_public_bytes(public_link, public_path)
            wb = load_workbook(BytesIO(raw), read_only=True, data_only=True)
            return load_spravochnik_from_wb(wb)
        except Exception as e:
            print(f"[Remote YaDisk] ошибка: {e} — fallback на локальный файл")

    # fallback локально
    ensure_spravochnik(local_path)
    wb = load_workbook(local_path, read_only=True, data_only=True)
    return load_spravochnik_from_wb(wb)

# ------------------------- Локальный справочник (fallback scaffolding) -------------------------

def ensure_spravochnik(path: Path):
    if path.exists():
        return
    wb = Workbook()
    ws1 = wb.active; ws1.title = "Сотрудники"
    ws1.append(["ФИО", "Табельный №", "Должность", "Подразделение"])
    ws1.append(["Иванов И. И.", "ST00-00001", "Слесарь", "Монтаж"])
    ws1.append(["Петров П. П.", "ST00-00002", "Электромонтер", "Электрика"])

    ws2 = wb.create_sheet("Объекты")
    ws2.append(["ID объекта", "Адрес"])
    ws2.append(["OBJ-001", "ул. Пушкина, д. 1"])
    ws2.append(["OBJ-002", "пр. Строителей, 25"])
    wb.save(path)

# ------------------------- Модель строки реестра -------------------------

class RowWidget:
    WEEK_BG_SAT = "#fff8e1"
    WEEK_BG_SUN = "#ffebee"
    ZEBRA_EVEN = "#ffffff"
    ZEBRA_ODD  = "#f6f8fa"
    ERR_BG     = "#ffccbc"
    DISABLED_BG= "#f0f0f0"

    def __init__(self, parent, idx: int, fio: str, tbn: str, get_year_month_callable, on_delete_callable):
        self.parent = parent
        self.idx = idx
        self.get_year_month = get_year_month_callable
        self.on_delete = on_delete_callable

        self.zebra_bg = self.ZEBRA_EVEN

        self.frame = tk.Frame(parent, bd=0)
        self.lbl_fio = tk.Label(self.frame, text=fio, anchor="w", bg=self.zebra_bg)
        self.lbl_fio.grid(row=0, column=0, padx=1, pady=1, sticky="w")
        self.lbl_tbn = tk.Label(self.frame, text=tbn, anchor="center", bg=self.zebra_bg)
        self.lbl_tbn.grid(row=0, column=1, padx=1, pady=1)

        self.day_entries: List[tk.Entry] = []
        for d in range(1, 32):
            e = tk.Entry(self.frame, width=4, justify="center")
            e.grid(row=0, column=1 + d, padx=0, pady=1)
            e.bind("<FocusOut>", lambda ev, _d=d: self.update_total())
            e.bind("<Button-2>", lambda ev: "break")
            e.bind("<ButtonRelease-2>", lambda ev: "break")
            self.day_entries.append(e)

        self.lbl_days = tk.Label(self.frame, text="0", width=5, anchor="e", bg=self.zebra_bg)
        self.lbl_days.grid(row=0, column=33, padx=(4,1), pady=1)
        self.lbl_total = tk.Label(self.frame, text="0", width=7, anchor="e", bg=self.zebra_bg)
        self.lbl_total.grid(row=0, column=34, padx=(4,1), pady=1)

        self.btn_52 = ttk.Button(self.frame, text="5/2", width=4, command=self.fill_52)
        self.btn_52.grid(row=0, column=35, padx=1)
        self.btn_del = ttk.Button(self.frame, text="Удалить", width=7, command=self.delete_row)
        self.btn_del.grid(row=0, column=36, padx=1)

    def apply_pixel_column_widths(self, colpx: dict):
        f = self.frame
        f.grid_columnconfigure(0, minsize=colpx['fio'])
        f.grid_columnconfigure(1, minsize=colpx['tbn'])
        for col in range(2, 33):
            f.grid_columnconfigure(col, minsize=colpx['day'])
        f.grid_columnconfigure(33, minsize=colpx['days'])
        f.grid_columnconfigure(34, minsize=colpx['hours'])
        f.grid_columnconfigure(35, minsize=colpx['btn52'])
        f.grid_columnconfigure(36, minsize=colpx['del'])

    def apply_zebra(self, index0: int):
        self.zebra_bg = self.ZEBRA_ODD if (index0 % 2 == 1) else self.ZEBRA_EVEN
        for w in (self.lbl_fio, self.lbl_tbn, self.lbl_days, self.lbl_total):
            w.configure(bg=self.zebra_bg)
        y, m = self.get_year_month()
        self._repaint_all_days(y, m)

    def grid(self, rindex: int):
        self.frame.grid(row=rindex, column=0, sticky="w")

    def destroy(self):
        self.frame.destroy()

    def fio(self) -> str:
        return self.lbl_fio.cget("text")

    def tbn(self) -> str:
        return self.lbl_tbn.cget("text")

    def set_day_font(self, font_tuple):
        for e in self.day_entries:
            e.configure(font=font_tuple)

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
        from datetime import datetime as dt
        wd = dt(year, month, day).weekday()
        if wd == 5: return self.WEEK_BG_SAT
        if wd == 6: return self.WEEK_BG_SUN
        return self.zebra_bg

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

    def _repaint_all_days(self, year: int, month: int):
        for i in range(31):
            self._repaint_day_cell(i, year, month)

    def update_days_enabled(self, year: int, month: int):
        self._repaint_all_days(year, month)
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
        self.lbl_total.config(text=f"{total_hours:.2f}".rstrip("0").rstrip("."))

    def fill_52(self):
        from datetime import datetime as dt
        y, m = self.get_year_month()
        days = month_days(y, m)
        for d in range(1, days + 1):
            wd = dt(y, m, d).weekday()
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

# ------------------------- Объектный табель (Toplevel) -------------------------

class ObjectTimesheet(tk.Toplevel):
    COLPX = {'fio':200,'tbn':100,'day':36,'days':46,'hours':56,'btn52':40,'del':66}
    MIN_FIO_PX = 140
    MAX_FIO_PX = 260

    def __init__(self, master):
        super().__init__(master)
        self.title("Объектный табель")
        self.geometry("1280x740")
        self.resizable(True, True)

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
        employees, objects = load_spravochnik_remote_or_local(self.spr_path)
        # employees: [(fio,tbn,pos,dep)]
        self.employees = employees
        self.objects = objects

        self.emp_names = [e[0] for e in self.employees]
        # fio -> (tbn,pos)
        self.emp_info = { fio: (tbn, pos) for (fio,tbn,pos,_) in self.employees }

        self.addr_to_ids = {}
        for oid, addr in self.objects:
            if not addr: continue
            self.addr_to_ids.setdefault(addr, [])
            if oid and oid not in self.addr_to_ids[addr]:
                self.addr_to_ids[addr].append(oid)

        self.address_options = sorted(self.addr_to_ids.keys() | {addr for _, addr in self.objects if addr})

    def _build_ui(self):
        top = tk.Frame(self); top.pack(fill="x", padx=8, pady=8)

        # Меню-подсказка под датой: сделаем вывод в шапке (внизу) — опустим, не критично

        tk.Label(top, text="Месяц:").grid(row=0, column=0, sticky="w", padx=(0,4))
        self.cmb_month = ttk.Combobox(top, state="readonly", width=12, values=[month_name_ru(i) for i in range(1,13)])
        self.cmb_month.grid(row=0, column=1, sticky="w")
        self.cmb_month.current(datetime.now().month - 1)
        self.cmb_month.bind("<<ComboboxSelected>>", lambda e: (self._on_period_change(), self._refresh_header_styles()))

        tk.Label(top, text="Год:").grid(row=0, column=2, sticky="w", padx=(16,4))
        self.spn_year = tk.Spinbox(top, from_=2000, to=2100, width=6, command=lambda: (self._on_period_change(), self._refresh_header_styles()))
        self.spn_year.grid(row=0, column=3, sticky="w")
        self.spn_year.delete(0,"end"); self.spn_year.insert(0, datetime.now().year)
        self.spn_year.bind("<FocusOut>", lambda e: (self._on_period_change(), self._refresh_header_styles()))

        tk.Label(top, text="Адрес:").grid(row=0, column=4, sticky="w", padx=(20,4))
        self.cmb_address = ttk.Combobox(top, values=self.address_options, width=46)
        self.cmb_address.grid(row=0, column=5, sticky="w")
        self.cmb_address.bind("<<ComboboxSelected>>", self._on_address_select)
        self.cmb_address.bind("<FocusOut>", self._on_address_select)
        self.cmb_address.bind("<Return>", lambda e: self._on_address_select())

        tk.Label(top, text="ID объекта:").grid(row=0, column=6, sticky="w", padx=(16,4))
        self.cmb_object_id = ttk.Combobox(top, state="readonly", values=[], width=18)
        self.cmb_object_id.grid(row=0, column=7, sticky="w")
        self.cmb_object_id.bind("<<ComboboxSelected>>", lambda e: self._load_existing_rows())

        tk.Label(top, text="ФИО:").grid(row=1, column=0, sticky="w", pady=(8,0))
        self.fio_var = tk.StringVar()
        self.cmb_fio = ttk.Combobox(top, textvariable=self.fio_var, values=self.emp_names, width=30)
        self.cmb_fio.grid(row=1, column=1, sticky="w", pady=(8,0))
        self.cmb_fio.bind("<<ComboboxSelected>>", self._on_fio_select)

        tk.Label(top, text="Табельный №:").grid(row=1, column=2, sticky="w", padx=(16,4), pady=(8,0))
        self.ent_tbn = ttk.Entry(top, width=14)
        self.ent_tbn.grid(row=1, column=3, sticky="w", pady=(8,0))

        tk.Label(top, text="Должность:").grid(row=1, column=4, sticky="w", padx=(16,4), pady=(8,0))
        self.pos_var = tk.StringVar()
        self.ent_pos = ttk.Entry(top, textvariable=self.pos_var, width=40, state="readonly")
        self.ent_pos.grid(row=1, column=5, sticky="w", pady=(8,0))

        # Кнопки
        btns = tk.Frame(top); btns.grid(row=2, column=0, columnspan=8, sticky="w", pady=(8,0))
        ttk.Button(btns, text="Добавить в табель", command=self.add_row).grid(row=0, column=0, padx=4)
        ttk.Button(btns, text="5/2 всем", command=self.fill_52_all).grid(row=0, column=1, padx=4)
        ttk.Button(btns, text="Проставить часы", command=self.fill_hours_all).grid(row=0, column=2, padx=4)
        ttk.Button(btns, text="Очистить все строки", command=self.clear_all_rows).grid(row=0, column=3, padx=4)
        ttk.Button(btns, text="Копировать из месяца…", command=self.copy_from_month).grid(row=0, column=4, padx=4)
        ttk.Button(btns, text="Сохранить", command=self.save_all).grid(row=0, column=5, padx=4)

        # Шапка
        header_wrap = tk.Frame(self); header_wrap.pack(fill="x", padx=8)
        self.header_canvas = tk.Canvas(header_wrap, height=26, borderwidth=0, highlightthickness=0)
        self.header_holder = tk.Frame(self.header_canvas)
        self.header_canvas.create_window((0,0), window=self.header_holder, anchor="nw")
        self.header_canvas.pack(fill="x", expand=True)

        tk.Label(self.header_holder, text="ФИО", anchor="w").grid(row=0, column=0, padx=1)
        tk.Label(self.header_holder, text="Таб.№", anchor="center").grid(row=0, column=1, padx=1)
        self.header_day_labels: List[tk.Label] = []
        for d in range(1, 32):
            lbl = tk.Label(self.header_holder, text=str(d), width=3, anchor="center", font=("Segoe UI", 8))
            lbl.grid(row=0, column=1+d, padx=0)
            self.header_day_labels.append(lbl)
        tk.Label(self.header_holder, text="Дней", width=5, anchor="e").grid(row=0, column=33, padx=(4,1))
        tk.Label(self.header_holder, text="Часы", width=7, anchor="e").grid(row=0, column=34, padx=(4,1))
        tk.Label(self.header_holder, text="5/2", width=4, anchor="center").grid(row=0, column=35, padx=1)
        tk.Label(self.header_holder, text="Удалить", width=7, anchor="center").grid(row=0, column=36, padx=1)

        # Строки
        wrap = tk.Frame(self); wrap.pack(fill="both", expand=True, padx=8, pady=(4,8))
        self.rows_canvas = tk.Canvas(wrap, borderwidth=0, highlightthickness=0)
        self.rows_holder = tk.Frame(self.rows_canvas)
        self.rows_canvas.create_window((0,0), window=self.rows_holder, anchor="nw")
        self.rows_canvas.pack(side="left", fill="both", expand=True)

        self.vscroll = ttk.Scrollbar(wrap, orient="vertical", command=self.rows_canvas.yview)
        self.vscroll.pack(side="right", fill="y")
        self.hscroll = ttk.Scrollbar(self, orient="horizontal", command=self._xscroll)
        self.hscroll.pack(fill="x", padx=8, pady=(0,8))

        self.rows_canvas.configure(yscrollcommand=self.vscroll.set, xscrollcommand=self._on_rows_xview)
        self.rows_holder.bind("<Configure>", lambda e: self.rows_canvas.configure(scrollregion=self.rows_canvas.bbox("all")))
        self.header_holder.bind("<Configure>", lambda e: self.header_canvas.configure(scrollregion=self.header_canvas.bbox("all")))

        self.rows_canvas.bind("<MouseWheel>", self._on_wheel)
        self.rows_canvas.bind("<Shift-MouseWheel>", self._on_shift_wheel)

        self.rows: List[RowWidget] = []
        self._apply_column_widths(self.header_holder)

        bottom = tk.Frame(self); bottom.pack(fill="x", padx=8, pady=(0,8))
        self.lbl_object_total = tk.Label(bottom, text="Сумма: сотрудников 0 | дней 0 | часов 0",
                                         font=("Segoe UI", 10, "bold"))
        self.lbl_object_total.pack(side="left")

        self._refresh_header_styles()

    def _refresh_header_styles(self):
        try:
            y, m = self.get_year_month()
        except Exception:
            now = datetime.now(); y, m = now.year, now.month
        days = month_days(y, m)
        now = datetime.now()
        for i,lbl in enumerate(self.header_day_labels, start=1):
            if i > days:
                lbl.configure(bg="#f0f0f0", fg="#999"); continue
            from datetime import datetime as dt
            wd = dt(y,m,i).weekday()
            bg = "#ffffff"
            if wd == 5: bg = RowWidget.WEEK_BG_SAT
            elif wd == 6: bg = RowWidget.WEEK_BG_SUN
            if y==now.year and m==now.month and i==now.day:
                bg = "#c8e6c9"
            lbl.configure(bg=bg, fg="#000")

    def _apply_column_widths(self, frame: tk.Frame):
        px = self.COLPX
        frame.grid_columnconfigure(0, minsize=px['fio'])
        frame.grid_columnconfigure(1, minsize=px['tbn'])
        for col in range(2, 33):
            frame.grid_columnconfigure(col, minsize=px['day'])
        frame.grid_columnconfigure(33, minsize=px['days'])
        frame.grid_columnconfigure(34, minsize=px['hours'])
        frame.grid_columnconfigure(35, minsize=px['btn52'])
        frame.grid_columnconfigure(36, minsize=px['del'])

    def _on_rows_xview(self, first, last):
        try: frac = float(first)
        except: frac = 0.0
        self.header_canvas.xview_moveto(frac)
        self.hscroll.set(first,last)

    def _xscroll(self, *args):
        self.rows_canvas.xview(*args)

    def _on_wheel(self, event):
        self.rows_canvas.yview_scroll(int(-1 * (event.delta/120)), "units")
        return "break"

    def _on_shift_wheel(self, event):
        step = -1 if event.delta > 0 else 1
        self._xscroll("scroll", step, "units")
        return "break"

    def _on_window_configure(self, _evt):
        if self._fit_job:
            try: self.after_cancel(self._fit_job)
            except: pass
        self._fit_job = self.after(100, self._auto_fit_columns)

    def _auto_fit_columns(self):
        try:
            viewport = self.rows_canvas.winfo_width()
        except:
            viewport = 0
        if viewport <= 1:
            self.after(120, self._auto_fit_columns); return
        total = self._content_total_width()
        new_fio = self.COLPX['fio']
        if total > viewport:
            deficit = total - viewport
            new_fio = max(self.MIN_FIO_PX, self.COLPX['fio'] - deficit)
        elif total < viewport:
            surplus = viewport - total
            new_fio = min(self.MAX_FIO_PX, self.COLPX['fio'] + surplus)
        if int(new_fio) != int(self.COLPX['fio']):
            self.COLPX['fio'] = int(new_fio)
            self._apply_column_widths(self.header_holder)
            for r in self.rows:
                r.apply_pixel_column_widths(self.COLPX)
            self.rows_canvas.configure(scrollregion=self.rows_canvas.bbox("all"))
            self.header_canvas.configure(scrollregion=self.header_canvas.bbox("all"))
            self.header_canvas.xview_moveto(self.rows_canvas.xview()[0])

    def _content_total_width(self, fio_px: Optional[int] = None) -> int:
        px = self.COLPX.copy()
        if fio_px is not None: px['fio'] = fio_px
        return px['fio'] + px['tbn'] + 31*px['day'] + px['days'] + px['hours'] + px['btn52'] + px['del']

    def _on_period_change(self):
        self._update_rows_days_enabled()
        self._load_existing_rows()
        self._refresh_header_styles()

    def _on_address_select(self, *_):
        addr = self.cmb_address.get().strip()
        ids = sorted(self.addr_to_ids.get(addr, []))
        if ids:
            self.cmb_object_id.config(state="readonly", values=ids)
            self.cmb_object_id.set(ids[0])
        else:
            self.cmb_object_id.config(state="normal", values=[])
            self.cmb_object_id.set("")
        self._load_existing_rows()

    def get_year_month(self) -> Tuple[int,int]:
        return int(self.spn_year.get()), self.cmb_month.current() + 1

    # Операции со строками
    def add_row(self):
        fio = self.fio_var.get().strip()
        tbn = self.ent_tbn.get().strip()
        if not fio:
            messagebox.showwarning("Объектный табель", "Выберите ФИО."); return
        # защита от дублей
        key = (fio.lower(), tbn.strip())
        if any((r.fio().strip().lower(), r.tbn().strip()) == key for r in self.rows):
            if not messagebox.askyesno("Дублирование", f"{fio} (Таб.№ {tbn}) уже добавлен.\nДобавить ещё одну строку?"):
                return

        w = RowWidget(self.rows_holder, len(self.rows)+1, fio, tbn, self.get_year_month, self.delete_row)
        w.apply_pixel_column_widths(self.COLPX)
        w.set_day_font(self.DAY_ENTRY_FONT)
        y,m = self.get_year_month()
        w.apply_zebra(len(self.rows))
        w.update_days_enabled(y,m)
        self.rows.append(w)
        self._regrid_rows()
        self._recalc_object_total()

    def fill_52_all(self):
        for r in self.rows: r.fill_52()
        self._recalc_object_total()

    def fill_hours_all(self):
        # простая версия: спросим день и часы в двух простых диалогах
        try:
            from tkinter.simpledialog import askinteger, askstring
            y,m = self.get_year_month()
            day = askinteger("Проставить часы","День (1..31):",minvalue=1,maxvalue=31,parent=self)
            if day is None: return
            from calendar import monthrange
            if day > monthrange(y,m)[1]:
                messagebox.showwarning("Часы","В выбранном месяце меньше дней."); return
            raw = askstring("Проставить часы","Часы (напр. 8, 8:30, 1/7):",parent=self)
            if raw is None: return
            n = parse_hours_value(raw)
            if n is None or n < 0:
                messagebox.showwarning("Часы","Введите корректное значение часов."); return
            s = f"{n:.2f}".rstrip("0").rstrip(".").replace(".",",")
            for r in self.rows:
                e = r.day_entries[day-1]
                e.delete(0,"end")
                if n > 1e-12: e.insert(0, s)
                r.update_total()
            self._recalc_object_total()
            messagebox.showinfo("Проставить часы", f"Проставлено {s} ч в день {day} для {len(self.rows)} сотрудников.")
        except Exception as e:
            messagebox.showerror("Проставить часы", f"Ошибка: {e}")

    def delete_row(self, roww: RowWidget):
        try: self.rows.remove(roww)
        except: pass
        roww.destroy()
        self._regrid_rows()
        self._recalc_object_total()

    def clear_all_rows(self):
        if not self.rows: return
        if not messagebox.askyesno("Объектный табель", "Очистить все строки?"): return
        for r in self.rows: r.destroy()
        self.rows.clear()
        self._regrid_rows()
        self._recalc_object_total()

    def _regrid_rows(self):
        for i,r in enumerate(self.rows, start=0):
            r.grid(i); r.apply_zebra(i)
        self.after(30, lambda: (
            self.rows_canvas.configure(scrollregion=self.rows_canvas.bbox("all")),
            self.header_canvas.configure(scrollregion=self.header_canvas.bbox("all")),
            self.header_canvas.xview_moveto(self.rows_canvas.xview()[0]),
            self._auto_fit_columns(),
        ))

    def _update_rows_days_enabled(self):
        y,m = self.get_year_month()
        for i,r in enumerate(self.rows, start=0):
            r.apply_zebra(i)
            r.update_days_enabled(y,m)

    def _recalc_object_total(self):
        tot_h = 0.0; tot_d = 0
        for r in self.rows:
            try: h = float(r.lbl_total.cget("text").replace(",", ".") or 0)
            except: h = 0.0
            try: d = int(r.lbl_days.cget("text") or 0)
            except: d = 0
            tot_h += h; tot_d += d
        sh = f"{tot_h:.2f}".rstrip("0").rstrip(".")
        self.lbl_object_total.config(text=f"Сумма: сотрудников {len(self.rows)} | дней {tot_d} | часов {sh}")

    def _current_file_path(self) -> Optional[Path]:
        addr = self.cmb_address.get().strip()
        oid  = self.cmb_object_id.get().strip()
        if not addr and not oid: return None
        y,m = self.get_year_month()
        id_part = oid if oid else safe_filename(addr)
        return self.out_dir / f"Объектный_табель_{id_part}_{y}_{m:02d}.xlsx"

    def _ensure_sheet(self, wb) -> Any:
        if "Табель" in wb.sheetnames:
            ws = wb["Табель"]
            hdr_first = str(ws.cell(1,1).value or "")
            if hdr_first == "ID объекта" and ws.max_column >= (6+31+2):
                return ws
            base="Табель_OLD"; new_name=base; i=1
            while new_name in wb.sheetnames:
                i+=1; new_name=f"{base}{i}"
            ws.title = new_name
        ws2 = wb.create_sheet("Табель")
        hdr = ["ID объекта","Адрес","Месяц","Год","ФИО","Табельный №"] + [str(i) for i in range(1,32)] + ["Итого дней","Итого часов"]
        ws2.append(hdr)
        ws2.column_dimensions["A"].width=14
        ws2.column_dimensions["B"].width=40
        ws2.column_dimensions["C"].width=10
        ws2.column_dimensions["D"].width=8
        ws2.column_dimensions["E"].width=28
        ws2.column_dimensions["F"].width=14
        for i in range(7,7+31):
            ws2.column_dimensions[get_column_letter(i)].width=6
        ws2.column_dimensions[get_column_letter(7+31)].width=10
        ws2.column_dimensions[get_column_letter(7+31+1)].width=12
        ws2.freeze_panes = "A2"
        return ws2

    def _load_existing_rows(self):
        # очистка
        for r in self.rows: r.destroy()
        self.rows.clear()
        self._regrid_rows()
        self._recalc_object_total()

        fpath = self._current_file_path()
        if not fpath or not fpath.exists(): return
        try:
            wb = load_workbook(fpath)
            ws = self._ensure_sheet(wb)
            y,m = self.get_year_month()
            addr = self.cmb_address.get().strip()
            oid  = self.cmb_object_id.get().strip()
            for r in range(2, ws.max_row+1):
                row_oid = (ws.cell(r,1).value or "")
                row_addr= (ws.cell(r,2).value or "")
                row_m   = int(ws.cell(r,3).value or 0)
                row_y   = int(ws.cell(r,4).value or 0)
                fio     = (ws.cell(r,5).value or "")
                tbn     = (ws.cell(r,6).value or "")
                if row_m != m or row_y != y: continue
                if oid:
                    if row_oid != oid: continue
                else:
                    if row_addr != addr: continue
                hours: List[Optional[float]] = []
                for c in range(7,7+31):
                    v = ws.cell(r,c).value
                    try:
                        n = float(v) if isinstance(v, (int,float)) else parse_hours_value(v)
                    except: n=None
                    hours.append(n)
                roww = RowWidget(self.rows_holder, len(self.rows)+1, fio, tbn, self.get_year_month, self.delete_row)
                roww.apply_pixel_column_widths(self.COLPX)
                roww.set_day_font(self.DAY_ENTRY_FONT)
                roww.update_days_enabled(y,m)
                roww.set_hours(hours)
                self.rows.append(roww)
            self._regrid_rows()
            self._recalc_object_total()
        except Exception as e:
            messagebox.showerror("Загрузка", f"Не удалось загрузить существующие строки:\n{e}")

    def save_all(self):
        fpath = self._current_file_path()
        if not fpath:
            messagebox.showwarning("Сохранение", "Укажите адрес и/или ID объекта, а также период.")
            return

        addr = self.cmb_address.get().strip()
        oid  = self.cmb_object_id.get().strip()
        y,m  = self.get_year_month()

        try:
            if fpath.exists():
                wb = load_workbook(fpath)
            else:
                fpath.parent.mkdir(parents=True, exist_ok=True)
                wb = Workbook()
                if wb.active: wb.remove(wb.active)
            ws = self._ensure_sheet(wb)

            # удалить строки этого объекта/периода
            to_del = []
            for r in range(2, ws.max_row+1):
                row_oid=(ws.cell(r,1).value or "")
                row_addr=(ws.cell(r,2).value or "")
                row_m=int(ws.cell(r,3).value or 0)
                row_y=int(ws.cell(r,4).value or 0)
                if row_m==m and row_y==y and ((oid and row_oid==oid) or (not oid and row_addr==addr)):
                    to_del.append(r)
            for r in reversed(to_del):
                ws.delete_rows(r,1)

            idx_total_days = 7+31
            idx_total_hours= 7+31+1

            for roww in self.rows:
                hours = roww.get_hours()
                total_hours = sum(h for h in hours if isinstance(h,(int,float))) if hours else 0.0
                total_days  = sum(1 for h in hours if isinstance(h,(int,float)) and h>1e-12)
                row_values = [oid, addr, m, y, roww.fio(), roww.tbn()] + [
                    (None if hours[i] is None or abs(float(hours[i]))<1e-12 else float(hours[i]))
                    for i in range(31)
                ] + [ total_days if total_days else None,
                      None if abs(total_hours)<1e-12 else float(total_hours) ]
                ws.append(row_values)
                rlast = ws.max_row
                for c in range(7,7+31):
                    v = ws.cell(rlast,c).value
                    if isinstance(v,(int,float)): ws.cell(rlast,c).number_format = "General"
                if isinstance(ws.cell(rlast,idx_total_days).value,(int,float)):
                    ws.cell(rlast,idx_total_days).number_format = "0"
                if isinstance(ws.cell(rlast,idx_total_hours).value,(int,float)):
                    ws.cell(rlast,idx_total_hours).number_format = "General"

            wb.save(fpath)
            messagebox.showinfo("Сохранение", f"Сохранено:\n{fpath}")
        except Exception as e:
            messagebox.showerror("Сохранение", f"Ошибка:\n{e}")

    def copy_from_month(self):
        from tkinter.simpledialog import askinteger
        addr = self.cmb_address.get().strip()
        oid  = self.cmb_object_id.get().strip()
        if not addr and not oid:
            messagebox.showwarning("Копирование", "Укажите адрес и/или ID объекта для назначения.")
            return

        cy, cm = self.get_year_month()
        ym = askinteger("Копирование", "Источник: ГГГГММ (например 202411) ?", parent=self)
        if not ym: return
        try:
            src_y = ym // 100; src_m = ym % 100
            if not (2000<=src_y<=2100 and 1<=src_m<=12): raise ValueError
        except Exception:
            messagebox.showwarning("Копирование","Формат ГГГГММ некорректен."); return

        src_path = self.out_dir / f"Объектный_табель_{(oid if oid else safe_filename(addr))}_{src_y}_{src_m:02d}.xlsx"
        if not src_path.exists():
            messagebox.showwarning("Копирование", f"Не найден файл:\n{src_path}"); return

        with load_workbook(src_path, data_only=True) as wb:
            ws = wb["Табель"] if "Табель" in wb.sheetnames else wb.active
            found=[]
            for r in range(2, ws.max_row+1):
                row_oid=(ws.cell(r,1).value or "")
                row_addr=(ws.cell(r,2).value or "")
                row_m=int(ws.cell(r,3).value or 0)
                row_y=int(ws.cell(r,4).value or 0)
                fio=(ws.cell(r,5).value or "")
                tbn=(ws.cell(r,6).value or "")
                if row_y!=src_y or row_m!=src_m: continue
                if oid:
                    if row_oid!=oid: continue
                else:
                    if row_addr!=addr: continue
                hrs=[]
                for c in range(7,7+31):
                    v = ws.cell(r,c).value
                    try: n = float(v) if isinstance(v,(int,float)) else parse_hours_value(v)
                    except: n=None
                    hrs.append(n)
                found.append((fio,tbn,hrs))
        if not found:
            messagebox.showinfo("Копирование","Источник пуст."); return

        # Очистим текущие строки и загрузим
        for r in self.rows: r.destroy()
        self.rows.clear()
        dy,dm = self.get_year_month()
        for fio,tbn,hrs in found:
            rw = RowWidget(self.rows_holder, len(self.rows)+1, fio, tbn, self.get_year_month, self.delete_row)
            rw.apply_pixel_column_widths(self.COLPX)
            rw.set_day_font(self.DAY_ENTRY_FONT)
            rw.update_days_enabled(dy,dm)
            rw.set_hours(hrs)
            self.rows.append(rw)
        self._regrid_rows()
        self._recalc_object_total()
        messagebox.showinfo("Копирование", f"Скопировано: {len(found)} сотрудников")

# ------------------------- Сводный экспорт -------------------------

def perform_summary_export(year: int, month: int, fmt: str) -> Tuple[int, List[Path]]:
    base = exe_dir()
    out_dir = get_output_dir_from_config()
    out_dir.mkdir(parents=True, exist_ok=True)

    pattern = f"Объектный_табель_*_{year}_{month:02d}.xlsx"
    files = list(out_dir.glob(pattern))
    rows = []
    for f in files:
        try:
            wb = load_workbook(f, read_only=True, data_only=True)
        except: continue
        if "Табель" not in wb.sheetnames: continue
        ws = wb["Табель"]
        for r in range(2, ws.max_row+1):
            row_m = int(ws.cell(r,3).value or 0); row_y = int(ws.cell(r,4).value or 0)
            if row_m!=month or row_y!=year: continue
            row_oid = (ws.cell(r,1).value or ""); row_addr=(ws.cell(r,2).value or "")
            fio=(ws.cell(r,5).value or ""); tbn=(ws.cell(r,6).value or "")
            hours=[]
            for c in range(7,7+31):
                v=ws.cell(r,c).value
                try: n=float(v) if isinstance(v,(int,float)) else parse_hours_value(v)
                except: n=None
                hours.append(n)
            total_days = sum(1 for h in hours if isinstance(h,(int,float)) and h>1e-12)
            total_hours= sum(h for h in hours if isinstance(h,(int,float)))
            row_values=[row_oid,row_addr,month,year,fio,tbn]+[
                (None if (h is None or abs(float(h))<1e-12) else float(h)) for h in hours
            ] + [ total_days if total_days else None,
                  None if (not isinstance(total_hours,(int,float)) or abs(total_hours)<1e-12) else float(total_hours)]
            rows.append(row_values)

    if not rows: return 0, []

    sum_dir = base / SUMMARY_DIR
    sum_dir.mkdir(parents=True, exist_ok=True)
    paths=[]
    hdr = ["ID объекта","Адрес","Месяц","Год","ФИО","Табельный №"]+[str(i) for i in range(1,32)]+["Итого дней","Итого часов"]

    if fmt in ("xlsx","both"):
        wb_out=Workbook(); ws_out=wb_out.active; ws_out.title="Сводный"
        ws_out.append(hdr)
        for rv in rows: ws_out.append(rv)
        ws_out.freeze_panes="A2"
        ws_out.column_dimensions["A"].width=14
        ws_out.column_dimensions["B"].width=40
        ws_out.column_dimensions["C"].width=10
        ws_out.column_dimensions["D"].width=8
        ws_out.column_dimensions["E"].width=28
        ws_out.column_dimensions["F"].width=14
        for i in range(7,7+31): ws_out.column_dimensions[get_column_letter(i)].width=6
        ws_out.column_dimensions[get_column_letter(7+31)].width=10
        ws_out.column_dimensions[get_column_letter(7+31+1)].width=12
        p = sum_dir / f"Сводный_{year}_{month:02d}.xlsx"; wb_out.save(p); paths.append(p)

    if fmt in ("csv","both"):
        import csv
        p = sum_dir / f"Сводный_{year}_{month:02d}.csv"
        with open(p,"w",encoding="utf-8-sig",newline="") as f:
            w=csv.writer(f, delimiter=";"); w.writerow(hdr)
            for rv in rows: w.writerow(rv)
        paths.append(p)

    return len(rows), paths

# ------------------------- Главное меню (Tk) -------------------------

class MainApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("900x480")
        self.resizable(False, False)

        ensure_config_exists()

        # Меню
        menubar = tk.Menu(self)

        m_ts = tk.Menu(menubar, tearoff=0)
        m_ts.add_command(label="Создать", command=lambda: ObjectTimesheet(self))
        menubar.add_cascade(label="Объектный табель", menu=m_ts)

        m_transport = tk.Menu(menubar, tearoff=0)
        try:
            import SpecialOrders
            m_transport.add_command(label="Заявка на автотранспорт", command=lambda: SpecialOrders.open_special_orders(self))
        except Exception:
            m_transport.add_command(label="Заявка на автотранспорт", command=lambda: messagebox.showinfo("Модуль","SpecialOrders не найден."))
        menubar.add_cascade(label="Автотранспорт", menu=m_transport)

        m_spr = tk.Menu(menubar, tearoff=0)
        m_spr.add_command(label="Открыть справочник", command=self.open_spravochnik)
        m_spr.add_command(label="Обновить справочник", command=self.refresh_spravochnik_global)
        menubar.add_cascade(label="Справочник", menu=m_spr)

        m_an = tk.Menu(menubar, tearoff=0)
        m_an.add_command(label="Экспорт свода (XLSX/CSV)", command=self.summary_export)
        menubar.add_cascade(label="Аналитика", menu=m_an)

        if timesheet_transformer:
            m_tools = tk.Menu(menubar, tearoff=0)
            m_tools.add_command(label="Конвертер табеля (1С)", command=lambda: timesheet_transformer.open_converter(self))
            menubar.add_cascade(label="Инструменты", menu=m_tools)

        self.config(menu=menubar)

        # Привет экран
        tk.Label(self, text="Управление строительством", font=("Segoe UI", 16, "bold")).pack(pady=(18,8))
        tk.Label(self, text="Выберите раздел в верхнем меню.\nСправочник может быть локальным или удалённым (Яндекс.Диск).",
                 font=("Segoe UI", 10), fg="#444").pack(pady=(0,18))
        tk.Label(self, text="Разработал Алексей Зезюкин, АНО МЛСТ 2025", font=("Segoe UI", 8), fg="#666").pack(side="bottom", pady=(0,8))

    def open_spravochnik(self):
        path = get_spr_path_from_config()
        ensure_spravochnik(path)
        try:
            os.startfile(path)
        except Exception as e:
            messagebox.showerror("Справочник", f"Не удалось открыть файл:\n{e}")

    def refresh_spravochnik_global(self):
        # просто уведомление — данные подтянутся при открытии модулей
        cfg = read_config()
        remote = cfg.get(CONFIG_SECTION_REMOTE, KEY_REMOTE_USE, fallback="false")
        if remote.strip().lower() in ("1","true","yes","on"):
            link = cfg.get(CONFIG_SECTION_REMOTE, KEY_YADISK_PUBLIC_LINK, fallback="")
            pth  = cfg.get(CONFIG_SECTION_REMOTE, KEY_YADISK_PUBLIC_PATH, fallback="")
            messagebox.showinfo("Справочник",
                "Режим: удалённый (Яндекс.Диск)\n"
                f"Ссылка: {link}\nВложенный путь (если папка): {pth}\n\n"
                "Модули перечитают справочник при открытии.")
        else:
            path = get_spr_path_from_config()
            ensure_spravochnik(path)
            messagebox.showinfo("Справочник",
                f"Режим: локальный файл\nПуть: {path}\n\n"
                "Модули перечитают справочник при открытии.")

    def summary_export(self):
        from tkinter.simpledialog import askinteger, askstring
        pwd = askstring("Сводный экспорт","Введите пароль:", show="*", parent=self)
        if pwd is None: return
        if pwd != get_export_password_from_config():
            messagebox.showerror("Сводный экспорт","Неверный пароль."); return
        y = askinteger("Сводный экспорт","Год:",minvalue=2000,maxvalue=2100,parent=self)
        m = askinteger("Сводный экспорт","Месяц (1..12):",minvalue=1,maxvalue=12,parent=self)
        if not y or not m: return
        fmt = "both"
        try:
            count, paths = perform_summary_export(y,m,fmt)
            if count<=0:
                messagebox.showinfo("Сводный экспорт","Не найдено строк для выгрузки."); return
            msg = f"Экспортировано строк: {count}\n\nФайлы:\n" + "\n".join(str(p) for p in paths)
            messagebox.showinfo("Сводный экспорт", msg)
        except Exception as e:
            messagebox.showerror("Сводный экспорт", f"Ошибка выгрузки:\n{e}")

if __name__ == "__main__":
    app = MainApp()
    app.mainloop()
