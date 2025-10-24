import os
import sys
import subprocess
import calendar
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional, Any

import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

APP_NAME = "Табель‑конвертер (Главное меню)"
SPRAVOCHNIK_FILE = "Справочник.xlsx"   # рядом с exe
CONVERTER_EXE = "TabelConverter.exe"   # ваш собранный конвертер (положить рядом)
OUTPUT_DIR = "ObjectTimesheets"        # папка для сохранения объектных табелей

# ------------------------- Утилиты -------------------------

def exe_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

def month_days(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]

def month_name_ru(month: int) -> str:
    names = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
    return names[month-1]

def ensure_spravochnik(path: Path):
    if path.exists():
        return
    wb = Workbook()
    # Лист Сотрудники
    ws1 = wb.active
    ws1.title = "Сотрудники"
    ws1.append(["ФИО", "Табельный №"])
    ws1.append(["Иванов И. И.", "ST00-00001"])
    ws1.append(["Петров П. П.", "ST00-00002"])
    # Лист Объекты
    ws2 = wb.create_sheet("Объекты")
    ws2.append(["Адрес"])
    ws2.append(["ул. Пушкина, д. 1"])
    ws2.append(["пр. Строителей, 25"])
    wb.save(path)

def load_spravochnik(path: Path) -> Tuple[List[Tuple[str,str]], List[str]]:
    ensure_spravochnik(path)
    wb = load_workbook(path)
    employees: List[Tuple[str,str]] = []
    objects: List[str] = []
    # Сотрудники
    if "Сотрудники" in wb.sheetnames:
        ws = wb["Сотрудники"]
        for r in ws.iter_rows(min_row=2, values_only=True):
            fio = (r[0] or "").strip()
            tbn = (r[1] or "").strip()
            if fio:
                employees.append((fio, tbn))
    # Объекты
    if "Объекты" in wb.sheetnames:
        ws = wb["Объекты"]
        for r in ws.iter_rows(min_row=2, values_only=True):
            addr = (r[0] or "").strip()
            if addr:
                objects.append(addr)
    return employees, objects

def parse_hours_value(v: Any) -> Optional[float]:
    # Понимает: 8 | 8,5 | 8.5 | 8:30 | 1/7
    s = str(v or "").strip()
    if not s:
        return None
    # сумма по слэшу
    if "/" in s:
        total = 0.0
        any_part = False
        for part in s.split("/"):
            n = parse_hours_value(part)
            if isinstance(n, (int, float)):
                total += float(n)
                any_part = True
        return total if any_part else None
    # время h:mm(:ss)
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

# ------------------------- Объектный табель (окно) -------------------------

class ObjectTimesheet(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.title("Объектный табель")
        self.geometry("1100x600")
        self.resizable(True, True)

        self.base_dir = exe_dir()
        self.spr_path = self.base_dir / SPRAVOCHNIK_FILE
        self.out_dir = self.base_dir / OUTPUT_DIR
        self.out_dir.mkdir(parents=True, exist_ok=True)

        self.employees, self.objects = load_spravochnik(self.spr_path)

        self._build_ui()

    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=8)

        # Месяц/Год
        tk.Label(top, text="Месяц:").grid(row=0, column=0, sticky="w", padx=(0,4))
        self.cmb_month = ttk.Combobox(top, state="readonly", width=12,
                                      values=[month_name_ru(i) for i in range(1,13)])
        self.cmb_month.grid(row=0, column=1, sticky="w")
        m_now = datetime.now().month
        self.cmb_month.current(m_now-1)

        tk.Label(top, text="Год:").grid(row=0, column=2, sticky="w", padx=(16,4))
        self.spn_year = tk.Spinbox(top, from_=2000, to=2100, width=6)
        self.spn_year.grid(row=0, column=3, sticky="w")
        self.spn_year.delete(0,"end")
        self.spn_year.insert(0, datetime.now().year)

        # Объект (адрес)
        tk.Label(top, text="Объект (адрес):").grid(row=0, column=4, sticky="w", padx=(20,4))
        self.cmb_object = ttk.Combobox(top, values=self.objects, width=40)
        self.cmb_object.grid(row=0, column=5, sticky="w")

        # ФИО, Таб.№
        tk.Label(top, text="ФИО:").grid(row=1, column=0, sticky="w", pady=(8,0))
        self.cmb_fio = ttk.Combobox(top, values=[e[0] for e in self.employees], width=30)
        self.cmb_fio.grid(row=1, column=1, sticky="w", pady=(8,0))
        self.cmb_fio.bind("<<ComboboxSelected>>", self._on_fio_select)

        tk.Label(top, text="Табельный №:").grid(row=1, column=2, sticky="w", padx=(16,4), pady=(8,0))
        self.ent_tbn = ttk.Entry(top, width=14)
        self.ent_tbn.grid(row=1, column=3, sticky="w", pady=(8,0))

        # Кнопки действий
        btns = tk.Frame(top)
        btns.grid(row=1, column=4, columnspan=2, sticky="w", padx=(20,0), pady=(8,0))
        ttk.Button(btns, text="Заполнить 8 по будням", command=self.fill_8_workdays).grid(row=0, column=0, padx=4)
        ttk.Button(btns, text="Очистить", command=self.clear_hours).grid(row=0, column=1, padx=4)
        ttk.Button(btns, text="Сохранить", command=self.save_timesheet).grid(row=0, column=2, padx=4)

        # Таблица 1..31
        frm_table = tk.Frame(self, borderwidth=1, relief="groove")
        frm_table.pack(fill="both", expand=True, padx=8, pady=8)

        hdr = tk.Frame(frm_table)
        hdr.pack(fill="x")
        tk.Label(hdr, text="День", width=6, anchor="center").grid(row=0, column=0, padx=2, pady=2)
        tk.Label(hdr, text="Часы", width=7, anchor="center").grid(row=0, column=1, padx=2, pady=2)
        tk.Label(hdr, text="День", width=6, anchor="center").grid(row=0, column=2, padx=12, pady=2)
        tk.Label(hdr, text="Часы", width=7, anchor="center").grid(row=0, column=3, padx=2, pady=2)

        self.entries: List[tk.Entry] = []
        self.rows_frame = tk.Frame(frm_table)
        self.rows_frame.pack(fill="both", expand=True)

        # 31 день — рисуем в два столбца (чтобы помещалось по высоте)
        self._draw_day_entries()

        # Итого часов
        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=8, pady=(0,8))
        self.lbl_total = tk.Label(bottom, text="Итого часов: 0", font=("Segoe UI", 10, "bold"))
        self.lbl_total.pack(side="left")

        self.update_day_state()

    def _draw_day_entries(self):
        # Чистим
        for w in self.rows_frame.winfo_children():
            w.destroy()
        self.entries.clear()

        year = int(self.spn_year.get())
        month = self.cmb_month.current()+1
        days = month_days(year, month)

        # два столбца по ~16 строк
        left_col_days = list(range(1, min(16, days)+1))
        right_col_days = list(range(17, days+1))

        def make_day_row(parent, r, d, col_offset):
            day_lbl = tk.Label(parent, text=str(d), width=6, anchor="center")
            day_lbl.grid(row=r, column=col_offset, padx=2, pady=1)
            ent = tk.Entry(parent, width=8, justify="center")
            ent.grid(row=r, column=col_offset+1, padx=2, pady=1)
            ent.bind("<FocusOut>", lambda e: self._on_entry_change())
            self.entries.append((d, ent))

        for i, d in enumerate(left_col_days, start=0):
            make_day_row(self.rows_frame, i, d, 0)
        for i, d in enumerate(right_col_days, start=0):
            make_day_row(self.rows_frame, i, d, 2)

        # отключим отсутствующие дни (например, февраль 29..31)
        self.update_idletasks()
        self.update_total()

    def _on_fio_select(self, *_):
        fio = self.cmb_fio.get().strip()
        tbn = ""
        for f, num in self.employees:
            if f == fio:
                tbn = num
                break
        self.ent_tbn.delete(0,"end")
        self.ent_tbn.insert(0, tbn)

    def _on_entry_change(self):
        # валидация и пересчёт итога по потере фокуса
        self.update_total()

    def update_day_state(self):
        # Перерисовать таблицу при смене месяца/года
        self._draw_day_entries()

    def clear_hours(self):
        for _, ent in self.entries:
            ent.delete(0, "end")
        self.update_total()

    def fill_8_workdays(self):
        # 8 часов в будни (Пн‑Пт), пусто в выходные
        year = int(self.spn_year.get())
        month = self.cmb_month.current()+1
        for d, ent in self.entries:
            wd = datetime(year, month, d).weekday()  # 0=Mon..6=Sun
            ent.delete(0, "end")
            if wd < 5:
                ent.insert(0, "8")
        self.update_total()

    def update_total(self):
        total = 0.0
        for d, ent in self.entries:
            val = ent.get().strip()
            n = parse_hours_value(val)
            if isinstance(n, (int,float)):
                total += float(n)
        s = f"{total:.2f}".rstrip("0").rstrip(".")
        self.lbl_total.config(text=f"Итого часов: {s}")

    def _collect_hours(self) -> List[Optional[float]]:
        # вернём массив на 31 позицию: None или число
        year = int(self.spn_year.get())
        month = self.cmb_month.current()+1
        days = month_days(year, month)
        arr: List[Optional[float]] = [None]*31
        for d, ent in self.entries:
            s = ent.get().strip()
            n = parse_hours_value(s)
            arr[d-1] = n
        # для отсутствующих дней -> None
        for d in range(days+1, 32):
            arr[d-1] = None
        return arr

    def save_timesheet(self):
        # сбор данных
        month = self.cmb_month.current()+1
        year = int(self.spn_year.get())
        obj = self.cmb_object.get().strip()
        fio = self.cmb_fio.get().strip()
        tbn = self.ent_tbn.get().strip()

        if not obj:
            messagebox.showwarning("Объектный табель", "Укажите объект (адрес).")
            return
        if not fio:
            messagebox.showwarning("Объектный табель", "Укажите ФИО.")
            return

        hours = self._collect_hours()
        total = sum(h for h in hours if isinstance(h,(int,float))) if hours else 0.0

        # файл назначения
        fname = f"Объектный_табель_{year}_{month:02d}.xlsx"
        fpath = self.out_dir / fname

        if fpath.exists():
            wb = load_workbook(fpath)
        else:
            wb = Workbook()
            wb.remove(wb.active)
            ws = wb.create_sheet("Табель")
            # Заголовок
            hdr = ["Объект","Месяц","Год","ФИО","Табельный №"] + [str(i) for i in range(1,32)] + ["Итого часов"]
            ws.append(hdr)
            # ширины
            ws.column_dimensions["A"].width = 40
            ws.column_dimensions["B"].width = 10
            ws.column_dimensions["C"].width = 8
            ws.column_dimensions["D"].width = 28
            ws.column_dimensions["E"].width = 14
            for i in range(6, 6+31):
                ws.column_dimensions[get_column_letter(i)].width = 6
            ws.column_dimensions[get_column_letter(6+31)].width = 12
            # заморозка шапки
            ws.freeze_panes = "A2"

        ws = wb["Табель"]

        # проверим, есть ли уже запись (по ключу объект+месяц+год+таб№)
        key_cols = {"Объект":1, "Месяц":2, "Год":3, "ФИО":4, "Таб№":5}
        found_row = None
        for r in range(2, ws.max_row+1):
            if (ws.cell(r,1).value == obj and
                int(ws.cell(r,2).value or 0) == month and
                int(ws.cell(r,3).value or 0) == year and
                (ws.cell(r,5).value or "").strip() == tbn):
                found_row = r
                break

        row_values = [obj, month, year, fio, tbn] + [
            (None if hours[i] is None or abs(float(hours[i])) < 1e-12 else float(hours[i])) for i in range(31)
        ] + [None if abs(total) < 1e-12 else float(total)]

        if found_row:
            if not messagebox.askyesno("Объектный табель",
                                       "Запись для этого объекта/месяца/года и таб.№ уже есть.\nПерезаписать?"):
                return
            for c, val in enumerate(row_values, start=1):
                cell = ws.cell(found_row, c)
                cell.value = val
                if isinstance(val,(int,float)):
                    cell.number_format = "General"
        else:
            r = ws.max_row + 1
            ws.append(row_values)
            # Форматы чисел = General, нули не пишем (уже None)
            for c in range(6, 6+31):
                v = ws.cell(r, c).value
                if isinstance(v,(int,float)):
                    ws.cell(r, c).number_format = "General"
            v = ws.cell(r, 6+31).value
            if isinstance(v,(int,float)):
                ws.cell(r, 6+31).number_format = "General"

        wb.save(fpath)
        messagebox.showinfo("Объектный табель", f"Сохранено:\n{fpath}")

# ------------------------- Конвертер (обёртка) -------------------------

def run_converter():
    # Запустить собранный вами конвертер рядом с программой
    conv_path = exe_dir() / CONVERTER_EXE
    if not conv_path.exists():
        messagebox.showwarning("Конвертер", f"Не найден {CONVERTER_EXE} рядом с программой.\n"
                                            f"Положите файл рядом и повторите.")
        return
    try:
        subprocess.Popen([str(conv_path)], shell=False)
    except Exception as e:
        messagebox.showerror("Конвертер", f"Не удалось запустить конвертер:\n{e}")

# ------------------------- Главное меню -------------------------

class MainApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("560x300")
        self.resizable(False, False)

        tk.Label(self, text="Выберите модуль", font=("Segoe UI", 14, "bold")).pack(pady=(16, 6))

        frm = tk.Frame(self)
        frm.pack(pady=8)

        ttk.Button(frm, text="Конвертер табеля (1С)", width=32, command=run_converter)\
            .grid(row=0, column=0, padx=8, pady=8)

        ttk.Button(frm, text="Объектный табель", width=32, command=lambda: ObjectTimesheet(self))\
            .grid(row=1, column=0, padx=8, pady=8)

        ttk.Button(frm, text="Открыть справочник", width=32, command=self.open_spravochnik)\
            .grid(row=2, column=0, padx=8, pady=8)

        ttk.Button(self, text="Выход", width=18, command=self.destroy).pack(pady=(12, 8))

        # Подпись
        tk.Label(self, text="Разработал Алексей Зезюкин, 2025", font=("Segoe UI", 8), fg="#666").pack(side="bottom", pady=(0, 8))

    def open_spravochnik(self):
        path = exe_dir() / SPRAVOCHNIK_FILE
        ensure_spravochnik(path)
        try:
            os.startfile(path)  # Windows
        except Exception:
            # универсально
            try:
                subprocess.Popen(["xdg-open", str(path)])
            except Exception as e:
                messagebox.showerror("Справочник", f"Не удалось открыть файл:\n{e}")

if __name__ == "__main__":
    app = MainApp()
    app.mainloop()
