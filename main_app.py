import os
import re
import sys
import subprocess
import calendar
import configparser
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional, Any

import tkinter as tk
from tkinter import ttk, messagebox

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

# ————— Брендинг/имя приложения —————
APP_NAME = "Управление строительством"

# ————— Конфиг и пути —————
CONFIG_FILE = "tabel_config.ini"
CONFIG_SECTION_PATHS = "Paths"
CONFIG_SECTION_INTEGR = "Integrations"
CONFIG_SECTION_UI = "UI"

KEY_SPR = "spravochnik_path"
KEY_OUTPUT_DIR = "output_dir"           # папка для объектных табелей
KEY_EXPORT_PWD = "export_password"

SPRAVOCHNIK_FILE = "Справочник.xlsx"
DEFAULT_OUTPUT_DIR = "Объектные_табели"

# ————— Запуск внешних exe (если нужно) —————
ORDERS_EXE = "SpecialOrders.exe"
CONVERTER_EXE = "TabelConverter.exe"

# ===================== Утилиты/конфиг =====================

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
            cfg[CONFIG_SECTION_PATHS][KEY_SPR] = str(exe_dir() / SPRAVOCHNIK_FILE)
            changed = True
        if KEY_OUTPUT_DIR not in cfg[CONFIG_SECTION_PATHS]:
            cfg[CONFIG_SECTION_PATHS][KEY_OUTPUT_DIR] = str(exe_dir() / DEFAULT_OUTPUT_DIR)
            changed = True
        if not cfg.has_section(CONFIG_SECTION_INTEGR):
            cfg[CONFIG_SECTION_INTEGR] = {}
            changed = True
        if KEY_EXPORT_PWD not in cfg[CONFIG_SECTION_INTEGR]:
            cfg[CONFIG_SECTION_INTEGR][KEY_EXPORT_PWD] = "2025"
            changed = True
        if not cfg.has_section(CONFIG_SECTION_UI):
            cfg[CONFIG_SECTION_UI] = {}
            changed = True
        if changed:
            with open(cp, "w", encoding="utf-8") as f:
                cfg.write(f)
        return
    # создать с нуля
    cfg = configparser.ConfigParser()
    cfg[CONFIG_SECTION_PATHS] = {
        KEY_SPR: str(exe_dir() / SPRAVOCHNIK_FILE),
        KEY_OUTPUT_DIR: str(exe_dir() / DEFAULT_OUTPUT_DIR),
    }
    cfg[CONFIG_SECTION_INTEGR] = {
        KEY_EXPORT_PWD: "2025",
    }
    cfg[CONFIG_SECTION_UI] = {}
    with open(cp, "w", encoding="utf-8") as f:
        cfg.write(f)

def read_config() -> configparser.ConfigParser:
    ensure_config_exists()
    cfg = configparser.ConfigParser()
    cfg.read(config_path(), encoding="utf-8")
    return cfg

def get_spr_path() -> Path:
    cfg = read_config()
    raw = cfg.get(CONFIG_SECTION_PATHS, KEY_SPR, fallback=str(exe_dir() / SPRAVOCHNIK_FILE))
    return Path(os.path.expandvars(raw))

def get_output_dir() -> Path:
    cfg = read_config()
    raw = cfg.get(CONFIG_SECTION_PATHS, KEY_OUTPUT_DIR, fallback=str(exe_dir() / DEFAULT_OUTPUT_DIR))
    return Path(os.path.expandvars(raw))

def get_export_password() -> str:
    cfg = read_config()
    return cfg.get(CONFIG_SECTION_INTEGR, KEY_EXPORT_PWD, fallback="2025").strip()

# ===================== Справочник =====================

def ensure_spravochnik(path: Path):
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    from openpyxl import Workbook
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

def load_spravochnik(path: Path) -> Tuple[List[Tuple[str,str,str,str]], List[Tuple[str,str]]]:
    def s(v) -> str:
        if v is None:
            return ""
        if isinstance(v, float) and v.is_integer():
            v = int(v)
        return str(v).strip()

    ensure_spravochnik(path)
    wb = load_workbook(path, read_only=True, data_only=True)
    employees: List[Tuple[str,str,str,str]] = []
    objects: List[Tuple[str,str]] = []

    if "Сотрудники" in wb.sheetnames:
        ws = wb["Сотрудники"]
        hdr = [s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        have_pos = ("должность" in hdr) or (len(hdr) >= 3)
        have_dep = ("подразделение" in hdr) or (len(hdr) >= 4)
        for r in ws.iter_rows(min_row=2, values_only=True):
            fio = s(r[0] if len(r) > 0 else None)
            tbn = s(r[1] if len(r) > 1 else None)
            pos = s(r[2] if have_pos and len(r) > 2 else None)
            dep = s(r[3] if have_dep and len(r) > 3 else None)
            if fio:
                employees.append((fio, tbn, pos, dep))

    if "Объекты" in wb.sheetnames:
        ws = wb["Объекты"]
        hdr = [s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        have_two = ("id объекта" in hdr) or (len(hdr) >= 2)
        for r in ws.iter_rows(min_row=2, values_only=True):
            if have_two:
                oid = s(r[0] if len(r) > 0 else None)
                addr = s(r[1] if len(r) > 1 else None)
            else:
                oid = ""
                addr = s(r[0] if len(r) > 0 else None)
            if oid or addr:
                objects.append((oid, addr))

    return employees, objects

# ===================== Парсинг часов =====================

def month_days(year: int, month: int) -> int:
    import calendar
    return calendar.monthrange(year, month)[1]

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
        except: pass
    s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return None

# ===================== Виджеты строки реестра =====================

class RowWidget:
    def __init__(self, parent, idx: int, fio: str, tbn: str, get_year_month_callable, on_delete_callable):
        self.parent = parent
        self.idx = idx
        self.get_year_month = get_year_month_callable
        self.on_delete = on_delete_callable

        self.frame = tk.Frame(parent, bd=0)

        self.lbl_fio = tk.Label(self.frame, text=fio, width=28, anchor="w")
        self.lbl_fio.grid(row=0, column=0, padx=2, pady=1, sticky="w")

        self.lbl_tbn = tk.Label(self.frame, text=tbn, width=12, anchor="center")
        self.lbl_tbn.grid(row=0, column=1, padx=2, pady=1)

        self.day_entries: List[tk.Entry] = []
        for d in range(1, 32):
            e = tk.Entry(self.frame, width=5, justify="center")
            e.grid(row=0, column=1 + d, padx=1, pady=1)
            e.bind("<FocusOut>", lambda ev, _d=d: self.update_total())
            e.bind("<Button-2>", lambda ev: "break")
            e.bind("<ButtonRelease-2>", lambda ev: "break")
            self.day_entries.append(e)

        self.lbl_days = tk.Label(self.frame, text="0", width=6, anchor="e")
        self.lbl_days.grid(row=0, column=33, padx=(6, 2), pady=1)

        self.lbl_total = tk.Label(self.frame, text="0", width=8, anchor="e")
        self.lbl_total.grid(row=0, column=34, padx=(6, 2), pady=1)

        self.btn_52 = ttk.Button(self.frame, text="5/2", width=5, command=self.fill_52)
        self.btn_52.grid(row=0, column=35, padx=2)

        self.btn_del = ttk.Button(self.frame, text="Удалить", width=8, command=self.delete_row)
        self.btn_del.grid(row=0, column=36, padx=2)

    def grid(self, rindex: int):
        self.frame.grid(row=rindex, column=0, sticky="w")

    def destroy(self):
        self.frame.destroy()

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

    def fio(self) -> str:
        return self.lbl_fio.cget("text")

    def tbn(self) -> str:
        return self.lbl_tbn.cget("text")

    def set_hours(self, arr: List[Optional[float]]):
        for i in range(31):
            self.day_entries[i].delete(0, "end")
            if i < len(arr) and isinstance(arr[i], (int, float)) and abs(arr[i]) > 1e-12:
                s = f"{float(arr[i]):.2f}".rstrip("0").rstrip(".")
                self.day_entries[i].insert(0, s)
        self.update_total()

    def get_hours(self) -> List[Optional[float]]:
        return [parse_hours_value(e.get().strip()) for e in self.day_entries]

    def update_days_enabled(self, year: int, month: int):
        days = month_days(year, month)
        for i, e in enumerate(self.day_entries, start=1):
            if i <= days:
                e.config(state="normal")
            else:
                e.delete(0, "end")
                e.config(state="disabled")
        self.update_total()

    def update_total(self):
        total_hours = 0.0
        total_days = 0
        for e in self.day_entries:
            n = parse_hours_value(e.get().strip())
            if isinstance(n, (int, float)) and n > 1e-12:
                total_hours += float(n)
                total_days += 1
        self.lbl_days.config(text=str(total_days))
        sh = f"{total_hours:.2f}".rstrip("0").rstrip(".")
        self.lbl_total.config(text=sh)

    def fill_52(self):
        y, m = self.get_year_month()
        days = month_days(y, m)
        for d in range(1, days + 1):
            wd = datetime(y, m, d).weekday()  # 0=Mon..6=Sun
            e = self.day_entries[d - 1]
            e.delete(0, "end")
            if wd < 4:      # Пн..Чт
                e.insert(0, "8,25")
            elif wd == 4:   # Пт
                e.insert(0, "7")
        for d in range(days + 1, 32):
            self.day_entries[d - 1].delete(0, "end")
        self.update_total()

    def delete_row(self):
        self.on_delete(self)

# ===================== Автокомплит =====================

class AutoCompleteCombobox(ttk.Combobox):
    def __init__(self, master=None, **kw):
        super().__init__(master, **kw)
        self._all_values: List[str] = []
        self.bind("<KeyRelease>", self._on_keyrelease)
        self.bind("<Control-BackSpace>", self._clear_all)

    def set_completion_list(self, values: List[str]):
        self._all_values = list(values)
        self['values'] = self._all_values

    def _clear_all(self, _=None):
        self.delete(0, tk.END)
        self['values'] = self._all_values

    def _on_keyrelease(self, event):
        if event.keysym in ("Up", "Down", "Left", "Right", "Home", "End", "Return", "Escape", "Tab"):
            return
        typed = self.get().strip()
        if not typed:
            self['values'] = self._all_values
            return
        self['values'] = [x for x in self._all_values if typed.lower() in x.lower()]

# ===================== Окно реестра (Объектный табель) =====================

class ObjectTimesheet(tk.Toplevel):
    COLPX = {'fio': 240, 'tbn': 110, 'day': 43, 'days': 50, 'hours': 60, 'btn52': 44, 'del': 70}

    def __init__(self, master):
        super().__init__(master)
        self.title("Объектный табель")
        self.geometry("1280x740")
        self.resizable(True, True)

        self.base_dir = exe_dir()
        self.spr_path = get_spr_path()
        self.out_dir = get_output_dir()
        self.out_dir.mkdir(parents=True, exist_ok=True)

        # справочник
        self.employees, self.objects = load_spravochnik(self.spr_path)
        self.emp_names = [e[0] for e in self.employees]
        self.emp_info = {e[0]: (e[1], (e[2] if len(e) > 2 else "")) for e in self.employees}

        self.addr_to_ids = {}
        for oid, addr in self.objects:
            if not addr:
                continue
            self.addr_to_ids.setdefault(addr, [])
            if oid and oid not in self.addr_to_ids[addr]:
                self.addr_to_ids[addr].append(oid)
        self.address_options = sorted(self.addr_to_ids.keys() | {addr for _, addr in self.objects if addr})

        self._build_ui()
        self._load_existing_rows()

    def _build_ui(self):
        top = tk.Frame(self); top.pack(fill="x", padx=8, pady=8)

        # Период
        tk.Label(top, text="Месяц:").grid(row=0, column=0, sticky="w")
        self.cmb_month = ttk.Combobox(top, state="readonly", width=12, values=[self._month_name_ru(i) for i in range(1, 13)])
        self.cmb_month.grid(row=0, column=1, sticky="w")
        self.cmb_month.current(datetime.now().month - 1)
        self.cmb_month.bind("<<ComboboxSelected>>", lambda e: self._on_period_change())

        tk.Label(top, text="Год:").grid(row=0, column=2, sticky="w", padx=(16, 4))
        self.spn_year = tk.Spinbox(top, from_=2000, to=2100, width=6, command=self._on_period_change)
        self.spn_year.grid(row=0, column=3, sticky="w")
        self.spn_year.delete(0, "end"); self.spn_year.insert(0, datetime.now().year)
        self.spn_year.bind("<FocusOut>", lambda e: self._on_period_change())

        # Адрес/ID
        tk.Label(top, text="Адрес:").grid(row=0, column=4, sticky="w", padx=(20, 4))
        self.cmb_address = AutoCompleteCombobox(top, width=46)
        self.cmb_address.set_completion_list(self.address_options)
        self.cmb_address.grid(row=0, column=5, sticky="w")
        self.cmb_address.bind("<<ComboboxSelected>>", self._on_address_select)
        self.cmb_address.bind("<FocusOut>", self._on_address_select)
        self.cmb_address.bind("<Return>", lambda e: self._on_address_select())
        self.cmb_address.bind("<KeyRelease>", lambda e: self._on_address_change(), add="+")

        tk.Label(top, text="ID объекта:").grid(row=0, column=6, sticky="w", padx=(16, 4))
        self.cmb_object_id = ttk.Combobox(top, state="readonly", values=[], width=18)
        self.cmb_object_id.grid(row=0, column=7, sticky="w")
        self.cmb_object_id.bind("<<ComboboxSelected>>", lambda e: self._load_existing_rows())

        # ФИО
        tk.Label(top, text="ФИО:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.fio_var = tk.StringVar()
        self.cmb_fio = AutoCompleteCombobox(top, textvariable=self.fio_var, width=30)
        self.cmb_fio.set_completion_list(self.emp_names)
        self.cmb_fio.grid(row=1, column=1, sticky="w", pady=(8, 0))
        self.cmb_fio.bind("<<ComboboxSelected>>", self._on_fio_select)

        tk.Label(top, text="Табельный №:").grid(row=1, column=2, sticky="w", padx=(16, 4), pady=(8, 0))
        self.ent_tbn = ttk.Entry(top, width=14); self.ent_tbn.grid(row=1, column=3, sticky="w", pady=(8, 0))

        tk.Label(top, text="Должность:").grid(row=1, column=4, sticky="w", padx=(16, 4), pady=(8, 0))
        self.pos_var = tk.StringVar()
        self.ent_pos = ttk.Entry(top, textvariable=self.pos_var, width=40, state="readonly")
        self.ent_pos.grid(row=1, column=5, sticky="w", pady=(8, 0))

        # Кнопки
        btns = tk.Frame(top); btns.grid(row=2, column=0, columnspan=8, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Добавить в табель", command=self.add_row).grid(row=0, column=0, padx=4)
        ttk.Button(btns, text="5/2 всем", command=self.fill_52_all).grid(row=0, column=1, padx=4)
        ttk.Button(btns, text="Очистить все строки", command=self.clear_all_rows).grid(row=0, column=2, padx=4)
        ttk.Button(btns, text="Сохранить", command=self.save_all).grid(row=0, column=3, padx=4)

        # Шапка (days)
        header_wrap = tk.Frame(self); header_wrap.pack(fill="x", padx=8)
        self.header_canvas = tk.Canvas(header_wrap, height=26, borderwidth=0, highlightthickness=0)
        self.header_holder = tk.Frame(self.header_canvas)
        self.header_canvas.create_window((0, 0), window=self.header_holder, anchor="nw")
        self.header_canvas.pack(fill="x", expand=True)

        tk.Label(self.header_holder, text="ФИО", width=28, anchor="w").grid(row=0, column=0, padx=2)
        tk.Label(self.header_holder, text="Таб.№", width=12, anchor="center").grid(row=0, column=1, padx=2)
        for d in range(1, 32):
            tk.Label(self.header_holder, text=str(d), width=5, anchor="center").grid(row=0, column=1 + d, padx=1)
        tk.Label(self.header_holder, text="Дней", width=6, anchor="e").grid(row=0, column=33, padx=(6, 2))
        tk.Label(self.header_holder, text="Часы", width=8, anchor="e").grid(row=0, column=34, padx=(6, 2))
        tk.Label(self.header_holder, text="5/2", width=5, anchor="center").grid(row=0, column=35, padx=2)
        tk.Label(self.header_holder, text="Удалить", width=8, anchor="center").grid(row=0, column=36, padx=2)

        # Canvas для строк
        wrap = tk.Frame(self); wrap.pack(fill="both", expand=True, padx=8, pady=(4, 8))
        self.rows_canvas = tk.Canvas(wrap, borderwidth=0, highlightthickness=0)
        self.rows_holder = tk.Frame(self.rows_canvas)
        self.rows_canvas.create_window((0, 0), window=self.rows_holder, anchor="nw")
        self.rows_canvas.pack(side="left", fill="both", expand=True)

        self.vscroll = ttk.Scrollbar(wrap, orient="vertical", command=self.rows_canvas.yview)
        self.vscroll.pack(side="right", fill="y")
        self.hscroll = ttk.Scrollbar(self, orient="horizontal", command=lambda *a: self.rows_canvas.xview(*a))
        self.hscroll.pack(fill="x", padx=8, pady=(0, 8))

        def _on_rows_xview(first, last):
            try: frac = float(first)
            except Exception: frac = 0.0
            self.header_canvas.xview_moveto(frac); self.hscroll.set(first, last)
        self.rows_canvas.configure(yscrollcommand=self.vscroll.set, xscrollcommand=_on_rows_xview)

        self.rows_holder.bind("<Configure>", lambda e: self.rows_canvas.configure(scrollregion=self.rows_canvas.bbox("all")))
        self.header_holder.bind("<Configure>", lambda e: self.header_canvas.configure(scrollregion=self.header_canvas.bbox("all")))
        self.rows_canvas.bind("<MouseWheel>", lambda e: (self.rows_canvas.yview_scroll(int(-1*(e.delta/120)), "units"), "break"))

        self.rows: List[RowWidget] = []
        self._apply_column_widths(self.header_holder)

        bottom = tk.Frame(self); bottom.pack(fill="x", padx=8, pady=(0, 8))
        self.lbl_object_total = tk.Label(bottom, text="Сумма: дней 0 | часов 0", font=("Segoe UI", 10, "bold"))
        self.lbl_object_total.pack(side="left")

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

    def _month_name_ru(self, month: int) -> str:
        names = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
        return names[month-1]

    def _on_fio_select(self, *_):
        fio = self.fio_var.get().strip()
        tbn, pos = self.emp_info.get(fio, ("", ""))
        self.ent_tbn.delete(0, "end"); self.ent_tbn.insert(0, tbn)
        self.pos_var.set(pos)

    def _on_address_change(self, *_):
        addr = self.cmb_address.get().strip()
        ids = sorted(self.addr_to_ids.get(addr, []))
        if ids:
            self.cmb_object_id.config(state="readonly", values=ids)
            if self.cmb_object_id.get() not in ids:
                self.cmb_object_id.set(ids[0])
        else:
            self.cmb_object_id.config(state="normal", values=[]); self.cmb_object_id.set("")

    def _on_period_change(self):
        self._update_rows_days_enabled(); self._load_existing_rows()

    def _on_address_select(self, *_):
        self._on_address_change(); self._load_existing_rows()

    def get_year_month(self) -> Tuple[int,int]:
        return int(self.spn_year.get()), self.cmb_month.current() + 1

    def add_row(self):
        fio = self.fio_var.get().strip()
        tbn = self.ent_tbn.get().strip()
        if not fio:
            messagebox.showwarning("Объектный табель", "Выберите ФИО.")
            return
        w = RowWidget(self.rows_holder, len(self.rows) + 1, fio, tbn, self.get_year_month, self.delete_row)
        w.apply_pixel_column_widths(self.COLPX)
        y, m = self.get_year_month()
        w.update_days_enabled(y, m)
        self.rows.append(w)
        self._regrid_rows(); self._recalc_object_total()

    def fill_52_all(self):
        for r in self.rows: r.fill_52()
        self._recalc_object_total()

    def delete_row(self, roww: RowWidget):
        try: self.rows.remove(roww)
        except Exception: pass
        roww.destroy()
        self._regrid_rows(); self._recalc_object_total()

    def clear_all_rows(self):
        if not self.rows: return
        if not messagebox.askyesno("Объектный табель", "Очистить все строки?"): return
        for r in self.rows: r.destroy()
        self.rows.clear()
        self._regrid_rows(); self._recalc_object_total()

    def _regrid_rows(self):
        for i, r in enumerate(self.rows, start=0):
            r.grid(i)
        self.after(30, lambda: (
            self.rows_canvas.configure(scrollregion=self.rows_canvas.bbox("all")),
            self.header_canvas.configure(scrollregion=self.header_canvas.bbox("all")),
            self.header_canvas.xview_moveto(self.rows_canvas.xview()[0]),
        ))

    def _update_rows_days_enabled(self):
        y, m = self.get_year_month()
        for r in self.rows: r.update_days_enabled(y, m)

    def _recalc_object_total(self):
        tot_h = 0.0; tot_d = 0
        for r in self.rows:
            try: h = float(r.lbl_total.cget("text").replace(",", ".") or 0)
            except: h = 0.0
            try: d = int(r.lbl_days.cget("text") or 0)
            except: d = 0
            tot_h += h; tot_d += d
        sh = f"{tot_h:.2f}".rstrip("0").rstrip(".")
        self.lbl_object_total.config(text=f"Сумма: дней {tot_d} | часов {sh}")

    # ===== загрузка/сохранение =====

    def _current_file_path(self) -> Optional[Path]:
        addr = self.cmb_address.get().strip()
        oid = self.cmb_object_id.get().strip()
        if not addr and not oid: return None
        y, m = self.get_year_month()
        id_part = oid if oid else self._safe_filename(addr)
        return get_output_dir() / f"Объектный_табель_{id_part}_{y}_{m:02d}.xlsx"

    def _safe_filename(self, s: str, maxlen: int = 60) -> str:
        if not s: return "NOID"
        s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", str(s)).strip()
        s = re.sub(r"_+", "_", s)
        return s[:maxlen] if len(s) > maxlen else s

    def _ensure_sheet(self, wb) -> Any:
        if "Табель" in wb.sheetnames:
            ws = wb["Табель"]
            hdr_first = str(ws.cell(1,1).value or "")
            if hdr_first == "ID объекта" and ws.max_column >= (6 + 31 + 2): return ws
            ws.title = "Табель_OLD"
        ws2 = wb.create_sheet("Табель")
        hdr = ["ID объекта","Адрес","Месяц","Год","ФИО","Табельный №"] + [str(i) for i in range(1,32)] + ["Итого дней","Итого часов"]
        ws2.append(hdr)
        ws2.column_dimensions["A"].width=14; ws2.column_dimensions["B"].width=40
        ws2.column_dimensions["C"].width=10; ws2.column_dimensions["D"].width=8
        ws2.column_dimensions["E"].width=28; ws2.column_dimensions["F"].width=14
        for i in range(7, 7+31): ws2.column_dimensions[get_column_letter(i)].width=6
        ws2.column_dimensions[get_column_letter(7+31)].width=10
        ws2.column_dimensions[get_column_letter(7+31+1)].width=12
        ws2.freeze_panes = "A2"
        return ws2

    def _load_existing_rows(self):
        for r in self.rows: r.destroy()
        self.rows.clear(); self._regrid_rows(); self._recalc_object_total()
        fpath = self._current_file_path()
        if not fpath or not fpath.exists(): return
        try:
            wb = load_workbook(fpath); ws = self._ensure_sheet(wb)
            y, m = self.get_year_month(); addr = self.cmb_address.get().strip(); oid = self.cmb_object_id.get().strip()
            for r in range(2, ws.max_row+1):
                row_oid = (ws.cell(r,1).value or ""); row_addr = (ws.cell(r,2).value or "")
                row_m = int(ws.cell(r,3).value or 0); row_y = int(ws.cell(r,4).value or 0)
                fio = (ws.cell(r,5).value or ""); tbn = (ws.cell(r,6).value or "")
                if row_m!=m or row_y!=y: continue
                if oid:
                    if row_oid!=oid: continue
                else:
                    if row_addr!=addr: continue
                hours: List[Optional[float]] = []
                for c in range(7, 7+31):
                    v = ws.cell(r, c).value
                    try: n = float(v) if isinstance(v, (int,float)) else parse_hours_value(v)
                    except: n = None
                    hours.append(n)
                roww = RowWidget(self.rows_holder, len(self.rows)+1, fio, tbn, self.get_year_month, self.delete_row)
                roww.apply_pixel_column_widths(self.COLPX); roww.update_days_enabled(y, m); roww.set_hours(hours)
                self.rows.append(roww)
            self._regrid_rows(); self._recalc_object_total()
        except Exception as e:
            messagebox.showerror("Загрузка", f"Не удалось загрузить строки:\n{e}")

    def save_all(self):
        fpath = self._current_file_path()
        if not fpath:
            messagebox.showwarning("Сохранение", "Укажите адрес/ID и период.")
            return
        addr = self.cmb_address.get().strip()
        oid = self.cmb_object_id.get().strip()
        y, m = self.get_year_month()
        try:
            if fpath.exists():
                wb = load_workbook(fpath)
            else:
                fpath.parent.mkdir(parents=True, exist_ok=True)
                wb = Workbook(); 
                if wb.active: wb.remove(wb.active)
            ws = self._ensure_sheet(wb)
            # удалить старые записи
            to_del = []
            for r in range(2, ws.max_row+1):
                row_oid = (ws.cell(r,1).value or ""); row_addr=(ws.cell(r,2).value or "")
                row_m = int(ws.cell(r,3).value or 0); row_y=int(ws.cell(r,4).value or 0)
                if row_m==m and row_y==y and ((oid and row_oid==oid) or (not oid and row_addr==addr)):
                    to_del.append(r)
            for r in reversed(to_del): ws.delete_rows(r,1)

            idx_total_days = 7+31; idx_total_hours = idx_total_days+1
            for roww in self.rows:
                hours = roww.get_hours()
                total_hours = sum(h for h in hours if isinstance(h, (int,float))) if hours else 0.0
                total_days = sum(1 for h in hours if isinstance(h, (int,float)) and h > 1e-12)
                row_values = [oid, addr, m, y, roww.fio(), roww.tbn()] + [
                    (None if hours[i] is None or abs(float(hours[i]))<1e-12 else float(hours[i])) for i in range(31)
                ] + [total_days if total_days else None, None if abs(total_hours)<1e-12 else float(total_hours)]
                ws.append(row_values)
                rlast = ws.max_row
                for c in range(7, 7+31):
                    v = ws.cell(rlast, c).value
                    if isinstance(v, (int,float)): ws.cell(rlast, c).number_format = "General"
                if isinstance(ws.cell(rlast, idx_total_days).value, (int,float)): ws.cell(rlast, idx_total_days).number_format="0"
                if isinstance(ws.cell(rlast, idx_total_hours).value, (int,float)): ws.cell(rlast, idx_total_hours).number_format="General"

            wb.save(fpath); messagebox.showinfo("Сохранение", f"Сохранено:\n{fpath}")
        except Exception as e:
            messagebox.showerror("Сохранение", f"Ошибка сохранения:\n{e}")

# ===================== Аналитика: сводный экспорт =====================

class ExportMonthDialog(tk.simpledialog.Dialog):
    def __init__(self, parent):
        self.result=None; super().__init__(parent, title="Сводный экспорт за месяц")

    def body(self, master):
        now = datetime.now()
        tk.Label(master, text="Месяц:").grid(row=0, column=0, sticky="e")
        self.cmb_month = ttk.Combobox(master, state="readonly", width=18,
                                      values=["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"])
        self.cmb_month.grid(row=0, column=1, sticky="w"); self.cmb_month.current(now.month-1)
        tk.Label(master, text="Год:").grid(row=0, column=2, sticky="e", padx=(12,4))
        self.spn_year = tk.Spinbox(master, from_=2000, to=2100, width=6)
        self.spn_year.grid(row=0, column=3, sticky="w"); self.spn_year.delete(0,"end"); self.spn_year.insert(0,str(now.year))

        tk.Label(master, text="Формат:").grid(row=1, column=0, sticky="e", pady=(8,0))
        self.var_fmt = tk.StringVar(value="both")
        frm = tk.Frame(master); frm.grid(row=1, column=1, columnspan=3, sticky="w", pady=(8,0))
        ttk.Radiobutton(frm, text="XLSX", value="xlsx", variable=self.var_fmt).pack(anchor="w")
        ttk.Radiobutton(frm, text="CSV",  value="csv",  variable=self.var_fmt).pack(anchor="w")
        ttk.Radiobutton(frm, text="Оба",  value="both", variable=self.var_fmt).pack(anchor="w")
        return self.cmb_month

    def validate(self):
        try:
            y = int(self.spn_year.get()); return 2000 <= y <= 2100
        except: return False

    def apply(self):
        self.result = {"year": int(self.spn_year.get()), "month": self.cmb_month.current()+1, "fmt": self.var_fmt.get()}

def perform_summary_export(year: int, month: int, fmt: str) -> Tuple[int, List[Path]]:
    out_dir = get_output_dir()
    pattern = f"Объектный_табель_*_{year}_{month:02d}.xlsx"
    files = list(out_dir.glob(pattern))
    rows = []
    for f in files:
        try: wb = load_workbook(f, read_only=True, data_only=True)
        except: continue
        if "Табель" not in wb.sheetnames: continue
        ws = wb["Табель"]
        for r in range(2, ws.max_row+1):
            row_m = int(ws.cell(r,3).value or 0); row_y = int(ws.cell(r,4).value or 0)
            if row_m != month or row_y != year: continue
            row_oid = (ws.cell(r,1).value or ""); row_addr=(ws.cell(r,2).value or "")
            fio = (ws.cell(r,5).value or ""); tbn=(ws.cell(r,6).value or "")
            hours=[]
            for c in range(7, 7+31):
                v = ws.cell(r, c).value
                try: n = float(v) if isinstance(v, (int,float)) else parse_hours_value(v)
                except: n = None
                hours.append(n)
            total_days = sum(1 for h in hours if isinstance(h,(int,float)) and h>1e-12)
            total_hours= sum(h for h in hours if isinstance(h,(int,float)))
            rows.append([row_oid,row_addr,month,year,fio,tbn] + [
                (None if (h is None or abs(float(h))<1e-12) else float(h)) for h in hours
            ] + [total_days if total_days else None,
                 None if (not isinstance(total_hours,(int,float)) or abs(total_hours)<1e-12) else float(total_hours)])

    if not rows: return 0, []

    sum_dir = exe_dir() / "Сводные_отчеты"; sum_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []
    hdr = ["ID объекта","Адрес","Месяц","Год","ФИО","Табельный №"]+[str(i) for i in range(1,32)]+["Итого дней","Итого часов"]

    if fmt in ("xlsx","both"):
        wb_out = Workbook(); ws_out = wb_out.active; ws_out.title="Сводный"; ws_out.append(hdr)
        for rv in rows: ws_out.append(rv)
        ws_out.freeze_panes="A2"
        ws_out.column_dimensions["A"].width=14; ws_out.column_dimensions["B"].width=40
        ws_out.column_dimensions["C"].width=10; ws_out.column_dimensions["D"].width=8
        ws_out.column_dimensions["E"].width=28; ws_out.column_dimensions["F"].width=14
        from openpyxl.utils import get_column_letter
        for i in range(7, 7+31): ws_out.column_dimensions[get_column_letter(i)].width=6
        ws_out.column_dimensions[get_column_letter(7+31)].width=10
        ws_out.column_dimensions[get_column_letter(7+31+1)].width=12
        p = sum_dir / f"Сводный_{year}_{month:02d}.xlsx"; wb_out.save(p); paths.append(p)

    if fmt in ("csv","both"):
        import csv
        p = sum_dir / f"Сводный_{year}_{month:02d}.csv"
        with open(p,"w",encoding="utf-8-sig",newline="") as fcsv:
            w = csv.writer(fcsv, delimiter=";"); w.writerow(hdr)
            for rv in rows: w.writerow(rv)
        paths.append(p)

    return len(rows), paths

# ===================== Запуск внешних модулей (как exe) =====================

def run_special_orders_exe():
    orders_path = exe_dir() / ORDERS_EXE
    if not orders_path.exists():
        messagebox.showwarning("Заказ спецтехники", f"Не найден {ORDERS_EXE} рядом с программой.")
        return
    try: subprocess.Popen([str(orders_path)], shell=False)
    except Exception as e: messagebox.showerror("Заказ спецтехники", f"Не удалось запустить модуль:\n{e}")

def run_converter_exe():
    conv_path = exe_dir() / CONVERTER_EXE
    if not conv_path.exists():
        messagebox.showwarning("Конвертер", f"Не найден {CONVERTER_EXE} рядом с программой.")
        return
    try: subprocess.Popen([str(conv_path)], shell=False)
    except Exception as e: messagebox.showerror("Конвертер", f"Не удалось запустить конвертер:\n{e}")

# ===================== Главное окно (единое) =====================

class MainApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("960x560")
        self.resizable(True, True)
        ensure_config_exists()

        # Меню
        menubar = tk.Menu(self)

        m_ts = tk.Menu(menubar, tearoff=0)
        m_ts.add_command(label="Создать", command=lambda: ObjectTimesheet(self))
        menubar.add_cascade(label="Объектный табель", menu=m_ts)

        m_transport = tk.Menu(menubar, tearoff=0)
        # Вариант А: встроенный модуль (если файл доступен)
        try:
            import SpecialOrders
            m_transport.add_command(label="Заявка на автотранспорт",
                                    command=lambda: SpecialOrders.open_special_orders(self))
        except Exception:
            # Вариант Б: запуск внешнего exe
            m_transport.add_command(label="Заявка на автотранспорт", command=run_special_orders_exe)
        menubar.add_cascade(label="Автотранспорт", menu=m_transport)

        m_spr = tk.Menu(menubar, tearoff=0)
        m_spr.add_command(label="Открыть справочник", command=self.open_spravochnik)
        m_spr.add_command(label="Обновить справочник", command=self.refresh_spravochnik_global)
        menubar.add_cascade(label="Справочник", menu=m_spr)

        m_analytics = tk.Menu(menubar, tearoff=0)
        m_analytics.add_command(label="Экспорт свода (XLSX/CSV)", command=self.summary_export)
        menubar.add_cascade(label="Аналитика", menu=m_analytics)

        self.config(menu=menubar)

        # Простой экран приветствия
        frm = tk.Frame(self); frm.pack(expand=True)
        tk.Label(frm, text="Управление строительством", font=("Segoe UI", 16, "bold")).pack(pady=(40, 10))
        tk.Label(frm, text="Используйте верхнее меню для выбора модуля.", font=("Segoe UI", 11)).pack(pady=(0, 10))
        tk.Label(frm, text="Справочник и пути настраиваются в tabel_config.ini", font=("Segoe UI", 9), fg="#666").pack()

    # ———— Справочник ————
    def open_spravochnik(self):
        path = get_spr_path()
        ensure_spravochnik(path)
        try: os.startfile(path)
        except Exception as e: messagebox.showerror("Справочник", f"Не удалось открыть файл:\n{e}")

    def refresh_spravochnik_global(self):
        path = get_spr_path()
        ensure_spravochnik(path)
        messagebox.showinfo("Справочник", f"Справочник проверен/создан:\n{path}\n\n"
                             f"Путь задаётся в {config_path()}")

    # ———— Аналитика ————
    def summary_export(self):
        pwd = tk.simpledialog.askstring("Сводный экспорт", "Введите пароль:", show="*", parent=self)
        if pwd is None: return
        if pwd != get_export_password():
            messagebox.showerror("Сводный экспорт", "Неверный пароль.")
            return
        dlg = ExportMonthDialog(self)
        if not getattr(dlg, "result", None): return
        y, m, fmt = dlg.result["year"], dlg.result["month"], dlg.result["fmt"]
        try:
            count, paths = perform_summary_export(y, m, fmt)
            if count <= 0:
                messagebox.showinfo("Сводный экспорт", "Не найдено строк для выгрузки.")
                return
            msg = f"Экспортировано строк: {count}\n\nФайлы:\n" + "\n".join(str(p) for p in paths)
            messagebox.showinfo("Сводный экспорт", msg)
        except Exception as e:
            messagebox.showerror("Сводный экспорт", f"Ошибка выгрузки:\n{e}")

if __name__ == "__main__":
    app = MainApp()
    app.mainloop()
