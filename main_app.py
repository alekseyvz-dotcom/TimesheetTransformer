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
from tkinter import ttk, messagebox, simpledialog

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

APP_NAME = "Управление строительством (Главное меню)"

# Конфиг и файлы
CONFIG_FILE = "tabel_config.ini"                # лежит рядом с программой
CONFIG_SECTION = "Paths"
CONFIG_SECTION_UI = "UI"
CONFIG_KEY_SPR = "spravochnik_path"
CONFIG_KEY_EXPORT_PWD = "export_password"
CONFIG_KEY_DEPARTMENT = "selected_department"

SPRAVOCHNIK_FILE = "Справочник.xlsx"            # имя по умолчанию (если в конфиге не задан путь)
CONVERTER_EXE = "TabelConverter.exe"            # ваш конвертер (лежит рядом)
OUTPUT_DIR = "Объектные_табели"                 # папка для объектных табелей (рядом с программой)
SUMMARY_DIR = "Сводные_отчеты"                   # папка для сводных выгрузок
ORDERS_EXE = "SpecialOrders.exe"  # имя exe модуля заявок, лежит рядом с программой


# ------------------------- Утилиты и конфиг -------------------------

def exe_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

def config_path() -> Path:
    return exe_dir() / CONFIG_FILE

def ensure_config_exists():
    cfg_path = config_path()
    if cfg_path.exists():
        # добавим недостающие секции/ключи при необходимости
        cfg = configparser.ConfigParser()
        cfg.read(cfg_path, encoding="utf-8")
        changed = False
        if not cfg.has_section(CONFIG_SECTION):
            cfg[CONFIG_SECTION] = {}
            changed = True
        if CONFIG_KEY_SPR not in cfg[CONFIG_SECTION]:
            cfg[CONFIG_SECTION][CONFIG_KEY_SPR] = str(exe_dir() / SPRAVOCHNIK_FILE)
            changed = True
        if CONFIG_KEY_EXPORT_PWD not in cfg[CONFIG_SECTION]:
            cfg[CONFIG_SECTION][CONFIG_KEY_EXPORT_PWD] = "2025"
            changed = True
        if not cfg.has_section(CONFIG_SECTION_UI):
            cfg[CONFIG_SECTION_UI] = {}
            changed = True
        if CONFIG_KEY_DEPARTMENT not in cfg[CONFIG_SECTION_UI]:
            cfg[CONFIG_SECTION_UI][CONFIG_KEY_DEPARTMENT] = "Все"
            changed = True
        if changed:
            with open(cfg_path, "w", encoding="utf-8") as f:
                cfg.write(f)
        return
    # создаём с нуля
    cfg = configparser.ConfigParser()
    cfg[CONFIG_SECTION] = {
        CONFIG_KEY_SPR: str(exe_dir() / SPRAVOCHNIK_FILE),
        CONFIG_KEY_EXPORT_PWD: "2025"
    }
    cfg[CONFIG_SECTION_UI] = {
        CONFIG_KEY_DEPARTMENT: "Все"
    }
    with open(cfg_path, "w", encoding="utf-8") as f:
        cfg.write(f)

def read_config() -> configparser.ConfigParser:
    ensure_config_exists()
    cfg = configparser.ConfigParser()
    cfg.read(config_path(), encoding="utf-8")
    return cfg

def write_config(cfg: configparser.ConfigParser):
    with open(config_path(), "w", encoding="utf-8") as f:
        cfg.write(f)

def get_spravochnik_path_from_config() -> Path:
    cfg = read_config()
    raw = cfg.get(CONFIG_SECTION, CONFIG_KEY_SPR, fallback=str(exe_dir() / SPRAVOCHNIK_FILE))
    return Path(os.path.expandvars(raw))

def get_export_password_from_config() -> str:
    cfg = read_config()
    return cfg.get(CONFIG_SECTION, CONFIG_KEY_EXPORT_PWD, fallback="2025")

def get_selected_department_from_config() -> str:
    cfg = read_config()
    return cfg.get(CONFIG_SECTION_UI, CONFIG_KEY_DEPARTMENT, fallback="Все")

def set_selected_department_to_config(dep: str):
    cfg = read_config()
    if not cfg.has_section(CONFIG_SECTION_UI):
        cfg[CONFIG_SECTION_UI] = {}
    cfg[CONFIG_SECTION_UI][CONFIG_KEY_DEPARTMENT] = dep or "Все"
    write_config(cfg)

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

def ensure_spravochnik(path: Path):
    # Создаём директории и файл при необходимости
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    if path.exists():
        return
    wb = Workbook()
    # Лист Сотрудники — с колонкой «Подразделение»
    ws1 = wb.active
    ws1.title = "Сотрудники"
    ws1.append(["ФИО", "Табельный №", "Должность", "Подразделение"])
    ws1.append(["Иванов И. И.", "ST00-00001", "Слесарь", "Монтаж"])
    ws1.append(["Петров П. П.", "ST00-00002", "Электромонтер", "Электрика"])
    ws1.append(["Сидорова А. А.", "ST00-00003", "Инженер", "ИТ"])
    # Лист Объекты (ID + Адрес)
    ws2 = wb.create_sheet("Объекты")
    ws2.append(["ID объекта", "Адрес"])
    ws2.append(["1", "ул. Пушкина, д. 1"])
    ws2.append(["2", "пр. Строителей, 25"])
    wb.save(path)

def load_spravochnik(path: Path) -> Tuple[List[Tuple[str,str,str,str]], List[Tuple[str,str]]]:
    """
    Возвращает:
    - employees: [(ФИО, Таб№, Должность, Подразделение)]
    - objects:   [(ID, Адрес)]
    Поддерживает старые справочники без колонки 'Подразделение' и/или 'Должность'.
    """
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

    # Сотрудники
    if "Сотрудники" in wb.sheetnames:
        ws = wb["Сотрудники"]
        hdr_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
        hdr = [s(c).lower() for c in hdr_row]
        have_pos = ("должность" in hdr) or (len(hdr) >= 3)
        have_dep = ("подразделение" in hdr) or (len(hdr) >= 4)
        for r in ws.iter_rows(min_row=2, values_only=True):
            fio = s(r[0] if len(r) > 0 else None)
            tbn = s(r[1] if len(r) > 1 else None)
            pos = s(r[2] if have_pos and len(r) > 2 else None)
            dep = s(r[3] if have_dep and len(r) > 3 else None)
            if fio:
                employees.append((fio, tbn, pos, dep))

    # Объекты
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

def parse_hours_value(v: Any) -> Optional[float]:
    # Понимает: 8 | 8,25 | 8.5 | 8:30 | 1/7 (сумма частей)
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


# ------------------------- Ряд реестра (строка) -------------------------
class RowWidget:
    WEEK_BG_SAT = "#fff8e1"   # светло-жёлтый
    WEEK_BG_SUN = "#ffebee"   # светло-розовый
    ZEBRA_EVEN = "#ffffff"
    ZEBRA_ODD  = "#f6f8fa"
    ERR_BG     = "#ffccbc"    # ошибка (некорректные/отрицательные/>24)
    DISABLED_BG= "#f0f0f0"

    def __init__(self, parent, idx: int, fio: str, tbn: str, get_year_month_callable, on_delete_callable):
        self.parent = parent
        self.idx = idx
        self.get_year_month = get_year_month_callable
        self.on_delete = on_delete_callable

        self.zebra_bg = self.ZEBRA_EVEN

        self.frame = tk.Frame(parent, bd=0)
        # ФИО / Таб.№
        self.lbl_fio = tk.Label(self.frame, text=fio, anchor="w", bg=self.zebra_bg)
        self.lbl_fio.grid(row=0, column=0, padx=1, pady=1, sticky="w")

        self.lbl_tbn = tk.Label(self.frame, text=tbn, anchor="center", bg=self.zebra_bg)
        self.lbl_tbn.grid(row=0, column=1, padx=1, pady=1)

        # 31 ячейка по дням
        self.day_entries: List[tk.Entry] = []
        for d in range(1, 32):
            e = tk.Entry(self.frame, width=4, justify="center")
            e.grid(row=0, column=1 + d, padx=0, pady=1)
            e.bind("<FocusOut>", lambda ev, _d=d: self.update_total())
            e.bind("<Button-2>", lambda ev: "break")
            e.bind("<ButtonRelease-2>", lambda ev: "break")
            self.day_entries.append(e)

        # Итоги и кнопки
        self.lbl_days = tk.Label(self.frame, text="0", width=5, anchor="e", bg=self.zebra_bg)
        self.lbl_days.grid(row=0, column=33, padx=(4, 1), pady=1)

        self.lbl_total = tk.Label(self.frame, text="0", width=7, anchor="e", bg=self.zebra_bg)
        self.lbl_total.grid(row=0, column=34, padx=(4, 1), pady=1)

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

    def set_day_font(self, font_tuple):
        for e in self.day_entries:
            e.configure(font=font_tuple)

    def apply_zebra(self, index0: int):
        # index0: 0,1,2,... (чёт/нечёт)
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
        wd = datetime(year, month, day).weekday()  # 0..6
        if wd == 5:
            return self.WEEK_BG_SAT
        if wd == 6:
            return self.WEEK_BG_SUN
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


# ------------------------- Автокомплит -------------------------
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


# ------------------------- Диалоги действий -------------------------
class CopyFromDialog(simpledialog.Dialog):
    def __init__(self, parent, init_year: int, init_month: int):
        self.init_year = init_year
        self.init_month = init_month  # 1..12
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

        vraw = self.ent_hours.get().strip()
        hv = parse_hours_value(vraw)
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


# ------------------------- Объектный табель -------------------------
class ObjectTimesheet(tk.Toplevel):
    # Базовые ширины (px) — компактная версия
    COLPX = {
        'fio':   200,  # ФИО (будет динамически подгоняться)
        'tbn':   100,  # Таб.№
        'day':    36,  # День (Entry width=4 + отступы)
        'days':   46,  # Итого дней
        'hours':  56,  # Итого часов
        'btn52':  40,  # 5/2
        'del':    66   # Удалить
    }
    MIN_FIO_PX = 140
    MAX_FIO_PX = 260

    def __init__(self, master):
        super().__init__(master)
        self.title("Объектный табель")
        self.geometry("1280x740")
        self.resizable(True, True)

        self.base_dir = exe_dir()
        self.spr_path = get_spravochnik_path_from_config()
        self.out_dir = self.base_dir / OUTPUT_DIR
        self.out_dir.mkdir(parents=True, exist_ok=True)

        # Компактный шрифт для ячеек дней
        self.DAY_ENTRY_FONT = ("Segoe UI", 8)

        self._fit_job = None

        self._load_spr_data()
        self._build_ui()
        self._load_existing_rows()

        # Автоподгон
        self.bind("<Configure>", self._on_window_configure)
        self.after(120, self._auto_fit_columns)

    # ---- справочник ----
    def _load_spr_data(self):
        employees, self.objects = load_spravochnik(self.spr_path)
        # Преобразуем в записи
        self.emp_records = [
            {'fio': fio, 'tbn': tbn, 'pos': pos, 'dep': dep}
            for (fio, tbn, pos, dep) in employees
        ]
        # Список подразделений
        deps = sorted({(r['dep'] or "").strip() for r in self.emp_records if (r['dep'] or "").strip()})
        self.departments = ["Все"] + deps

        # Адреса/ID
        self.addr_to_ids = {}
        for oid, addr in self.objects:
            if not addr:
                continue
            self.addr_to_ids.setdefault(addr, [])
            if oid and oid not in self.addr_to_ids[addr]:
                self.addr_to_ids[addr].append(oid)
        self.address_options = sorted(self.addr_to_ids.keys() | {addr for _, addr in self.objects if addr})

    # ---- UI ----
    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=8)

        # Ряд 0 — Подразделение (слева, главное условие)
        tk.Label(top, text="Подразделение:").grid(row=0, column=0, sticky="w", padx=(0, 4))
        self.cmb_department = ttk.Combobox(top, state="readonly", values=self.departments, width=48)
        self.cmb_department.grid(row=0, column=1, sticky="w")
        # установим выбранное подразделение из ini (или "Все")
        saved_dep = get_selected_department_from_config()
        if saved_dep in self.departments:
            self.cmb_department.set(saved_dep)
        else:
            self.cmb_department.set(self.departments[0] if self.departments else "Все")
        self.cmb_department.bind("<<ComboboxSelected>>", lambda e: self._on_department_select())

        # Ряд 1 — Период и Объект
        tk.Label(top, text="Месяц:").grid(row=1, column=0, sticky="w", padx=(0, 4), pady=(8, 0))
        self.cmb_month = ttk.Combobox(top, state="readonly", width=12, values=[month_name_ru(i) for i in range(1, 13)])
        self.cmb_month.grid(row=1, column=1, sticky="w", pady=(8, 0))
        self.cmb_month.current(datetime.now().month - 1)
        self.cmb_month.bind("<<ComboboxSelected>>", lambda e: (self._on_period_change(), self._refresh_header_styles()))

        tk.Label(top, text="Год:").grid(row=1, column=2, sticky="w", padx=(16, 4), pady=(8, 0))
        self.spn_year = tk.Spinbox(top, from_=2000, to=2100, width=6, command=lambda: (self._on_period_change(), self._refresh_header_styles()))
        self.spn_year.grid(row=1, column=3, sticky="w", pady=(8, 0))
        self.spn_year.delete(0, "end")
        self.spn_year.insert(0, datetime.now().year)
        self.spn_year.bind("<FocusOut>", lambda e: (self._on_period_change(), self._refresh_header_styles()))

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

        # Ряд 2 — ФИО/Таб№/Должность (с фильтром по подразделению)
        tk.Label(top, text="ФИО:").grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.fio_var = tk.StringVar()
        self.cmb_fio = AutoCompleteCombobox(top, textvariable=self.fio_var, width=30)
        self.cmb_fio.grid(row=2, column=1, sticky="w", pady=(8, 0))
        self.cmb_fio.bind("<<ComboboxSelected>>", self._on_fio_select)

        tk.Label(top, text="Табельный №:").grid(row=2, column=2, sticky="w", padx=(16, 4), pady=(8, 0))
        self.ent_tbn = ttk.Entry(top, width=14)
        self.ent_tbn.grid(row=2, column=3, sticky="w", pady=(8, 0))

        tk.Label(top, text="Должность:").grid(row=2, column=4, sticky="w", padx=(16, 4), pady=(8, 0))
        self.pos_var = tk.StringVar()
        self.ent_pos = ttk.Entry(top, textvariable=self.pos_var, width=40, state="readonly")
        self.ent_pos.grid(row=2, column=5, sticky="w", pady=(8, 0))

        # Подготовим список ФИО согласно текущему подразделению
        self._update_fio_list()

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

        # Шапка
        header_wrap = tk.Frame(self)
        header_wrap.pack(fill="x", padx=8)
        self.header_canvas = tk.Canvas(header_wrap, height=26, borderwidth=0, highlightthickness=0)
        self.header_holder = tk.Frame(self.header_canvas)
        self.header_canvas.create_window((0, 0), window=self.header_holder, anchor="nw")
        self.header_canvas.pack(fill="x", expand=True)

        tk.Label(self.header_holder, text="ФИО", anchor="w").grid(row=0, column=0, padx=1)
        tk.Label(self.header_holder, text="Таб.№", anchor="center").grid(row=0, column=1, padx=1)
        self.header_day_labels: List[tk.Label] = []
        for d in range(1, 32):
            lbl = tk.Label(self.header_holder, text=str(d), width=3, anchor="center", font=("Segoe UI", 8))
            lbl.grid(row=0, column=1 + d, padx=0)
            self.header_day_labels.append(lbl)
        tk.Label(self.header_holder, text="Дней", width=5, anchor="e").grid(row=0, column=33, padx=(4, 1))
        tk.Label(self.header_holder, text="Часы", width=7, anchor="e").grid(row=0, column=34, padx=(4, 1))
        tk.Label(self.header_holder, text="5/2", width=4, anchor="center").grid(row=0, column=35, padx=1)
        tk.Label(self.header_holder, text="Удалить", width=7, anchor="center").grid(row=0, column=36, padx=1)

        # Строки (Canvas)
        wrap = tk.Frame(self)
        wrap.pack(fill="both", expand=True, padx=8, pady=(4, 8))
        self.rows_canvas = tk.Canvas(wrap, borderwidth=0, highlightthickness=0)
        self.rows_holder = tk.Frame(self.rows_canvas)
        self.rows_canvas.create_window((0, 0), window=self.rows_holder, anchor="nw")
        self.rows_canvas.pack(side="left", fill="both", expand=True)

        # Скроллы
        self.vscroll = ttk.Scrollbar(wrap, orient="vertical", command=self.rows_canvas.yview)
        self.vscroll.pack(side="right", fill="y")
        self.hscroll = ttk.Scrollbar(self, orient="horizontal", command=self._xscroll)
        self.hscroll.pack(fill="x", padx=8, pady=(0, 8))

        # Привязки
        self.rows_canvas.configure(yscrollcommand=self.vscroll.set, xscrollcommand=self._on_rows_xview)

        # Авто‑scrollregion
        self.rows_holder.bind("<Configure>", lambda e: self.rows_canvas.configure(scrollregion=self.rows_canvas.bbox("all")))
        self.header_holder.bind("<Configure>", lambda e: self.header_canvas.configure(scrollregion=self.header_canvas.bbox("all")))

        # Колёсико
        self.rows_canvas.bind("<MouseWheel>", self._on_wheel)
        self.rows_canvas.bind("<Shift-MouseWheel>", self._on_shift_wheel)
        self.bind_all("<MouseWheel>", self._on_wheel_global)
        self.bind_all("<Shift-MouseWheel>", self._on_shift_wheel_global)
        self.bind_all("<Button-4>", lambda e: self._on_wheel_global(e, linux=+1))
        self.bind_all("<Button-5>", lambda e: self._on_wheel_global(e, linux=-1))

        # Список строк
        self.rows: List[RowWidget] = []

        # Применяем фиксированные ширины к шапке и стили
        self._apply_column_widths(self.header_holder)
        self._refresh_header_styles()

        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=8, pady=(0, 8))
        self.lbl_object_total = tk.Label(bottom, text="Сумма: сотрудников 0 | дней 0 | часов 0", font=("Segoe UI", 10, "bold"))
        self.lbl_object_total.pack(side="left")

    def _refresh_header_styles(self):
        try:
            y, m = self.get_year_month()
        except Exception:
            y, m = datetime.now().year, datetime.now().month
        days = month_days(y, m)
        now = datetime.now()
        for i, lbl in enumerate(self.header_day_labels, start=1):
            if i > days:
                lbl.configure(bg="#f0f0f0", fg="#999")
                continue
            wd = datetime(y, m, i).weekday()
            bg = "#ffffff"
            if wd == 5:
                bg = RowWidget.WEEK_BG_SAT
            elif wd == 6:
                bg = RowWidget.WEEK_BG_SUN
            if (y == now.year and m == now.month and i == now.day):
                bg = "#c8e6c9"
            lbl.configure(bg=bg, fg="#000")

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

    def _find_emp_record(self, fio: str) -> Optional[dict]:
        dep_sel = (self.cmb_department.get() or "").strip()
        # Сначала пытаемся найти в выбранном подразделении
        for r in self.emp_records:
            if r['fio'] == fio and (dep_sel == "Все" or (r['dep'] or "") == dep_sel):
                return r
        # Иначе возьмём любую запись с таким ФИО
        for r in self.emp_records:
            if r['fio'] == fio:
                return r
        return None

    def _on_fio_select(self, *_):
        fio = self.fio_var.get().strip()
        rec = self._find_emp_record(fio)
        tbn = rec['tbn'] if rec else ""
        pos = rec['pos'] if rec else ""
        self.ent_tbn.delete(0, "end")
        self.ent_tbn.insert(0, tbn)
        self.pos_var.set(pos or "")

    def _update_fio_list(self):
        dep_sel = (self.cmb_department.get() or "Все").strip()
        if dep_sel == "Все":
            names = [r['fio'] for r in self.emp_records]
        else:
            names = [r['fio'] for r in self.emp_records if (r['dep'] or "") == dep_sel]
        # Удалим дубли ФИО
        seen = set()
        filtered = []
        for n in names:
            if n not in seen:
                seen.add(n)
                filtered.append(n)
        self.cmb_fio.set_completion_list(filtered)
        # если текущий ФИО не в списке — очистим
        cur_fio = self.fio_var.get().strip()
        if cur_fio and cur_fio not in filtered:
            self.fio_var.set("")
            self.ent_tbn.delete(0, "end")
            self.pos_var.set("")

    def _on_department_select(self):
        dep = (self.cmb_department.get() or "Все").strip()
        set_selected_department_to_config(dep)   # сохраняем в ini при каждом выборе
        self._update_fio_list()

    def _on_rows_xview(self, first, last):
        try:
            frac = float(first)
        except Exception:
            frac = 0.0
        self.header_canvas.xview_moveto(frac)
        self.hscroll.set(first, last)

    def _xscroll(self, *args):
        self.rows_canvas.xview(*args)

    def _on_wheel(self, event):
        self.rows_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        return "break"

    def _on_shift_wheel(self, event):
        step = -1 if event.delta > 0 else 1
        self._xscroll("scroll", step, "units")
        return "break"

    def _is_under_rows(self, widget) -> bool:
        try:
            w = widget
            while True:
                if w is self.rows_holder:
                    return True
                parent_name = w.winfo_parent()
                if not parent_name:
                    return False
                w = self.nametowidget(parent_name)
        except Exception:
            return False

    def _on_wheel_global(self, event, linux: int = 0):
        if not self._is_under_rows(event.widget):
            return
        units = (-1 if linux < 0 else 1) if linux != 0 else int(-1 * (event.delta / 120))
        if units == 0:
            units = -1 if event.delta > 0 else 1
        self.rows_canvas.yview_scroll(units, "units")
        return "break"

    def _on_shift_wheel_global(self, event, linux: int = 0):
        if not self._is_under_rows(event.widget):
            return
        step = -1 if (linux > 0 or event.delta > 0) else 1
        self._xscroll("scroll", step, "units")
        return "break"

    # События шапки
    def _on_period_change(self):
        self._update_rows_days_enabled()
        self._load_existing_rows()
        self._refresh_header_styles()

    def _on_address_select(self, *_):
        self._on_address_change()
        self._load_existing_rows()
        addr = self.cmb_address.get().strip()
        ids = sorted(self.addr_to_ids.get(addr, []))
        if ids:
            self.cmb_object_id.config(state="readonly", values=ids)
            self.cmb_object_id.set(ids[0])
        else:
            self.cmb_object_id.config(state="normal", values=[])
            self.cmb_object_id.set("")
        self._load_existing_rows()

    # Вспомогательные
    def get_year_month(self) -> Tuple[int, int]:
        return int(self.spn_year.get()), self.cmb_month.current() + 1

    def _content_total_width(self, fio_px: Optional[int] = None) -> int:
        px = self.COLPX.copy()
        if fio_px is not None:
            px['fio'] = fio_px
        return px['fio'] + px['tbn'] + 31 * px['day'] + px['days'] + px['hours'] + px['btn52'] + px['del']

    def _auto_fit_columns(self):
        try:
            viewport = self.rows_canvas.winfo_width()
        except Exception:
            viewport = 0
        if viewport <= 1:
            self.after(120, self._auto_fit_columns)
            return

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

    def _on_window_configure(self, _evt):
        if self._fit_job:
            try:
                self.after_cancel(self._fit_job)
            except Exception:
                pass
        self._fit_job = self.after(100, self._auto_fit_columns)

    # Операции со строками
    def add_row(self):
        fio = self.fio_var.get().strip()
        tbn = self.ent_tbn.get().strip()
        if not fio:
            messagebox.showwarning("Объектный табель", "Выберите ФИО.")
            return

        # Защита от дублей (ФИО + Таб№)
        key = (fio.strip().lower(), tbn.strip())
        if any((r.fio().strip().lower(), r.tbn().strip()) == key for r in self.rows):
            if not messagebox.askyesno("Дублирование",
                                       f"Сотрудник уже есть в реестре:\n{fio} (Таб.№ {tbn}).\nДобавить ещё одну строку?"):
                return

        w = RowWidget(self.rows_holder, len(self.rows) + 1, fio, tbn, self.get_year_month, self.delete_row)
        w.apply_pixel_column_widths(self.COLPX)
        w.set_day_font(self.DAY_ENTRY_FONT)
        y, m = self.get_year_month()
        w.apply_zebra(len(self.rows))
        w.update_days_enabled(y, m)
        self.rows.append(w)
        self._regrid_rows()
        self._recalc_object_total()

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
        self._recalc_object_total()

    def clear_all_rows(self):
        if not self.rows:
            return
        if not messagebox.askyesno("Объектный табель", "Очистить все строки?"):
            return
        for r in self.rows:
            r.destroy()
        self.rows.clear()
        self._regrid_rows()
        self._recalc_object_total()

    def _regrid_rows(self):
        for i, r in enumerate(self.rows, start=0):
            r.grid(i)
            r.apply_zebra(i)
        self.after(
            30,
            lambda: (
                self.rows_canvas.configure(scrollregion=self.rows_canvas.bbox("all")),
                self.header_canvas.configure(scrollregion=self.header_canvas.bbox("all")),
                self.header_canvas.xview_moveto(self.rows_canvas.xview()[0]),
                self._auto_fit_columns(),
            ),
        )

    def _update_rows_days_enabled(self):
        y, m = self.get_year_month()
        for i, r in enumerate(self.rows, start=0):
            r.apply_zebra(i)
            r.update_days_enabled(y, m)

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

    # загрузка/сохранение/справочник
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
            "Итого дней",
            "Итого часов",
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
        for r in self.rows:
            r.destroy()
        self.rows.clear()
        self._regrid_rows()
        self._recalc_object_total()

        fpath = self._current_file_path()
        if not fpath or not fpath.exists():
            self._auto_fit_columns()
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
                roww.apply_zebra(len(self.rows))
                roww.update_days_enabled(y, m)
                roww.set_hours(hours)
                self.rows.append(roww)
            self._regrid_rows()
            self._recalc_object_total()
            self._auto_fit_columns()
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

            # удалить строки этого объекта/периода
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

            idx_total_days = 7 + 31
            idx_total_hours = 7 + 31 + 1

            for roww in self.rows:
                hours = roww.get_hours()
                total_hours = sum(h for h in hours if isinstance(h, (int, float))) if hours else 0.0
                total_days = sum(1 for h in hours if isinstance(h, (int, float)) and h > 1e-12)
                row_values = [oid, addr, m, y, roww.fio(), roww.tbn()] + [
                    (None if hours[i] is None or abs(float(hours[i])) < 1e-12 else float(hours[i]))
                    for i in range(31)
                ] + [total_days if total_days else None, None if abs(total_hours) < 1e-12 else float(total_hours)]
                ws.append(row_values)
                rlast = ws.max_row
                for c in range(7, 7 + 31):
                    v = ws.cell(rlast, c).value
                    if isinstance(v, (int, float)):
                        ws.cell(rlast, c).number_format = "General"
                if isinstance(ws.cell(rlast, idx_total_days).value, (int, float)):
                    ws.cell(rlast, idx_total_days).number_format = "0"
                if isinstance(ws.cell(rlast, idx_total_hours).value, (int, float)):
                    ws.cell(rlast, idx_total_hours).number_format = "General"

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

            # Уникализируем по (ФИО, Таб№)
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
                roww.apply_zebra(len(self.rows))
                roww.update_days_enabled(dy, dm)
                if with_hours and hrs:
                    roww.set_hours(hrs)
                self.rows.append(roww)
                added += 1

            self._regrid_rows()
            self._recalc_object_total()
            self._auto_fit_columns()
            messagebox.showinfo("Копирование", f"Добавлено сотрудников: {added}")

        except Exception as e:
            messagebox.showerror("Копирование", f"Ошибка копирования:\n{e}")

    def reload_spravochnik(self):
        # перечитать с учётом возможного изменения пути в конфиге
        self.spr_path = get_spravochnik_path_from_config()

        cur_addr = self.cmb_address.get().strip()
        cur_id = self.cmb_object_id.get().strip()
        cur_dep = (self.cmb_department.get() or "Все").strip()
        cur_fio = self.fio_var.get().strip()

        self._load_spr_data()

        # Адреса/ID
        self.cmb_address.set_completion_list(self.address_options)
        if cur_addr in self.address_options:
            self.cmb_address.set(cur_addr)
        else:
            self.cmb_address.set("")
        self._on_address_change()
        if cur_id and cur_id in (self.cmb_object_id.cget("values") or []):
            self.cmb_object_id.set(cur_id)
        else:
            self.cmb_object_id.config(values=[])
            self.cmb_object_id.set("")

        # Подразделения
        self.cmb_department.config(values=self.departments)
        # приоритет: текущий выбор; если он «пропал», возьмём сохранённый в ini; иначе "Все"
        saved_dep = get_selected_department_from_config()
        if cur_dep in self.departments:
            self.cmb_department.set(cur_dep)
        elif saved_dep in self.departments:
            self.cmb_department.set(saved_dep)
        else:
            self.cmb_department.set(self.departments[0] if self.departments else "Все")

        # Сотрудники (с учётом фильтра)
        self._update_fio_list()
        if cur_fio and cur_fio in (self.cmb_fio.cget("values") or []):
            self.fio_var.set(cur_fio)
            self._on_fio_select()
        else:
            self.fio_var.set("")
            self.ent_tbn.delete(0, "end")
            self.pos_var.set("")

        messagebox.showinfo("Справочник", f"Справочник перечитан.\nПуть: {self.spr_path}")


# ------------------------- Конвертер (внешний EXE) -------------------------

def run_converter():
    conv_path = exe_dir() / CONVERTER_EXE
    if not conv_path.exists():
        messagebox.showwarning("Конвертер", f"Не найден {CONVERTER_EXE} рядом с программой.\n"
                                            f"Положите файл рядом и повторите.")
        return
    try:
        subprocess.Popen([str(conv_path)], shell=False)
    except Exception as e:
        messagebox.showerror("Конвертер", f"Не удалось запустить конвертер:\n{e}")

def run_special_orders():
    orders_path = exe_dir() / ORDERS_EXE
    if not orders_path.exists():
        messagebox.showwarning("Заказ спецтехники",
                               f"Не найден {ORDERS_EXE} рядом с программой.\n"
                               f"Соберите EXE из SpecialOrders.py и положите рядом.")
        return
    try:
        subprocess.Popen([str(orders_path)], shell=False)
    except Exception as e:
        messagebox.showerror("Заказ спецтехники", f"Не удалось запустить модуль:\n{e}")

# ------------------------- Сводный экспорт -------------------------

def perform_summary_export(year: int, month: int, fmt: str) -> Tuple[int, List[Path]]:
    """
    Собирает все строки за указанный месяц/год из всех файлов в OUTPUT_DIR и сохраняет сводный отчёт.
    fmt: xlsx | csv | both
    Возвращает (количество строк, список путей выгрузки)
    """
    base = exe_dir()
    out_dir = base / OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    pattern = f"Объектный_табель_*_{year}_{month:02d}.xlsx"
    files = list(out_dir.glob(pattern))
    rows = []

    # Читаем все строки из всех файлов за период
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
            # 31 день
            hours: List[Optional[float]] = []
            for c in range(7, 7 + 31):
                v = ws.cell(r, c).value
                try:
                    n = float(v) if isinstance(v, (int, float)) else parse_hours_value(v)
                except Exception:
                    n = None
                hours.append(n)
            # Итого
            total_days = sum(1 for h in hours if isinstance(h, (int, float)) and h > 1e-12)
            total_hours = sum(h for h in hours if isinstance(h, (int, float)))
            row_values = [row_oid, row_addr, month, year, fio, tbn] + [
                (None if (h is None or abs(float(h)) < 1e-12) else float(h)) for h in hours
            ] + [total_days if total_days else None,
                 None if (not isinstance(total_hours, (int, float)) or abs(total_hours) < 1e-12) else float(total_hours)]
            rows.append(row_values)

    if not rows:
        return 0, []

    # Подготовка директории вывода
    sum_dir = base / SUMMARY_DIR
    sum_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []

    hdr = ["ID объекта", "Адрес", "Месяц", "Год", "ФИО", "Табельный №"] + [str(i) for i in range(1, 32)] + [
        "Итого дней",
        "Итого часов",
    ]

    # XLSX
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

    # CSV
    if fmt in ("csv", "both"):
        import csv
        p = sum_dir / f"Сводный_{year}_{month:02d}.csv"
        with open(p, "w", encoding="utf-8-sig", newline="") as fcsv:
            writer = csv.writer(fcsv, delimiter=";")
            writer.writerow(hdr)
            for rv in rows:
                writer.writerow(rv)
        paths.append(p)

    return len(rows), paths


# ------------------------- Главное меню -------------------------

class MainApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("720x460")
        self.resizable(False, False)

        ensure_config_exists()

        tk.Label(self, text="Выберите модуль", font=("Segoe UI", 14, "bold")).pack(pady=(16, 6))

        ttk.Button(self, text="Объектный табель (реестр)", width=36,
                   command=lambda: ObjectTimesheet(self)).pack(pady=(4, 10))
        ttk.Button(self, text="Заказ спецтехники", width=36, command=run_special_orders)\
            .pack(pady=(0, 8))

        spr_frame = tk.Frame(self)
        spr_frame.pack(pady=(0, 12))
        ttk.Button(spr_frame, text="Открыть справочник", width=24, command=self.open_spravochnik)\
            .grid(row=0, column=0, padx=6, pady=6)
        ttk.Button(spr_frame, text="Обновить справочник", width=24, command=self.refresh_spravochnik_global)\
            .grid(row=0, column=1, padx=6, pady=6)

        ttk.Button(self, text="Конвертер табеля (1С)", width=36, command=run_converter).pack(pady=(0, 8))

        ttk.Button(self, text="Сводный экспорт (XLSX/CSV)", width=36, command=self.summary_export)\
            .pack(pady=(0, 12))

        ttk.Button(self, text="Помощь", width=18, command=self.show_help).pack(pady=(0, 8))
        ttk.Button(self, text="Выход", width=18, command=self.destroy).pack(pady=(0, 12))

        tk.Label(self, text="Разработал Алексей Зезюкин, АНО МЛСТ 2025",
                 font=("Segoe UI", 8), fg="#666").pack(side="bottom", pady=(0, 8))

        self.bind("<F1>", lambda e: self.show_help())

    def open_spravochnik(self):
        path = get_spravochnik_path_from_config()
        ensure_spravochnik(path)
        try:
            os.startfile(path)
        except Exception as e:
            messagebox.showerror("Справочник", f"Не удалось открыть файл:\n{e}")

    def refresh_spravochnik_global(self):
        path = get_spravochnik_path_from_config()
        ensure_spravochnik(path)
        messagebox.showinfo(
            "Справочник",
            f"Справочник проверен/создан по пути:\n{path}\n\n"
            f"Путь берётся из файла конфигурации:\n{config_path()}\n"
            f"Можно указать сетевой путь (например, \\\\server\\share\\Справочник.xlsx)."
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

    def show_help(self):
        path = get_spravochnik_path_from_config()
        dep = get_selected_department_from_config()
        text = (
            "Как пользоваться модулем «Объектный табель (реестр)»\n\n"
            "1) Подразделение:\n"
            "   • Вверху выберите Подразделение — в списке ФИО будут сотрудники только этого подразделения.\n"
            f"   • Выбор сохраняется между сессиями (сейчас: «{dep}»).\n\n"
            "2) Период и объект:\n"
            "   • Выберите Месяц и Год.\n"
            "   • Выберите Адрес; список ID подставится автоматически.\n\n"
            "3) Сотрудники:\n"
            "   • ФИО → автоподстановка Таб.№ и Должности.\n\n"
            "4) Массовые действия:\n"
            "   • «5/2 всем», «Проставить часы» (есть режим очистки дня), удаление/очистка строк.\n\n"
            "5) Сохранение и загрузка:\n"
            "   • «Сохранить» — файл «Объектный_табель_{ID|Адрес}_{ГГГГ}_{ММ}.xlsx» в папке «Объектные_табели».\n"
            "   • При смене периода/адреса/ID строки подгружаются из файла (если есть).\n\n"
            "6) Копирование списка из другого месяца:\n"
            "   • «Копировать из месяца…» — выбрать месяц/год источника, режим (Заменить/Объединить), часы опционально.\n\n"
            "7) Визуальные подсказки:\n"
            "   • Выходные подсвечены, «сегодня» выделен в шапке; зебра-строки.\n"
            "   • Ошибочные ячейки (некорректный ввод, <0 или >24) подсвечиваются.\n\n"
            "8) Справочник:\n"
            "   • Путь к «Справочник.xlsx» — {cfg}\n"
            f"   • Текущий путь: {path}\n\n"
            "9) Сводный экспорт:\n"
            "   • Меню «Сводный экспорт (XLSX/CSV)», пароль хранится в ini (по умолчанию 2025).\n"
        ).format(cfg=config_path())
        messagebox.showinfo("Помощь — Объектный табель", text)


if __name__ == "__main__":
    app = MainApp()
    app.mainloop()
