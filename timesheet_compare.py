# timesheet_compare.py
from __future__ import annotations

import os
import tempfile
import logging
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from openpyxl import load_workbook, Workbook
from openpyxl.styles import PatternFill, Alignment, Font
from openpyxl.utils import get_column_letter

import timesheet_transformer

# Импорт из модуля табеля
from timesheet_module import (
    load_all_timesheet_headers,
    load_timesheet_rows_by_header_id,
    month_name_ru,
    month_days,
    set_db_pool as _set_db_pool_from_timesheet,
)


def set_db_pool(pool):
    _set_db_pool_from_timesheet(pool)


def normalize_tbn(val: Any) -> str:
    """
    Убирает ведущие нули и пробелы для корректного сравнения табельных (0055 == 55).
    """
    s = str(val or "").strip()
    return s.lstrip("0") if s else ""


def normalize_val(val: Any) -> str:
    """
    Нормализация значения ячейки (часов) для сравнения.
    Убирает регистр, пробелы, заменяет запятые на точки, убирает .0.
    """
    if val is None:
        return ""
    
    # 1. В строку и нижний регистр
    s = str(val).strip().lower()
    
    # 2. Замена запятой (8,5 -> 8.5)
    s = s.replace(",", ".")
    
    # 3. Убираем "хвост" .0 (8.0 -> 8)
    if s.endswith(".0"):
        s = s[:-2]
        
    return s


class TimesheetComparePage(tk.Frame):
    def __init__(self, master, app_ref):
        super().__init__(master)
        self.app_ref = app_ref

        self._headers: List[Dict[str, Any]] = []
        self._obj_rows: List[Dict[str, Any]] = []
        self._hr_rows: List[Dict[str, Any]] = []
        
        # Хранит итоговые данные для отображения и экспорта
        self._merged_rows: List[Dict[str, Any]] = []
        
        self._agg_headers: List[Dict[str, Any]] = []

        self._build_ui()
        self._load_headers()

    # ---------- UI ----------

    def _build_ui(self):
        # --- Верхняя панель (Фильтры) ---
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=(8, 4))

        tk.Label(top, text="Сравнение табелей (Объект vs 1С)", font=("Segoe UI", 12, "bold"))\
            .grid(row=0, column=0, columnspan=6, sticky="w", pady=(0, 8))

        # Год / Месяц
        row_f = 1
        from datetime import datetime
        
        tk.Label(top, text="Год:").grid(row=row_f, column=0, sticky="e", padx=(0, 4))
        self.var_year = tk.StringVar(value=str(datetime.now().year))
        tk.Spinbox(top, from_=2000, to=2100, width=6, textvariable=self.var_year)\
            .grid(row=row_f, column=1, sticky="w")

        tk.Label(top, text="Месяц:").grid(row=row_f, column=2, sticky="e", padx=(12, 4))
        self.var_month = tk.StringVar(value="Все")
        ttk.Combobox(top, state="readonly", width=12, textvariable=self.var_month,
                     values=["Все"] + [month_name_ru(i) for i in range(1, 13)])\
            .grid(row=row_f, column=3, sticky="w")

        row_f += 1
        tk.Label(top, text="Подразделение:").grid(row=row_f, column=0, sticky="e", padx=(0, 4), pady=4)
        self.var_dep = tk.StringVar(value="Все")
        self.cmb_dep = ttk.Combobox(top, state="readonly", width=40, textvariable=self.var_dep, values=["Все"])
        self.cmb_dep.grid(row=row_f, column=1, columnspan=3, sticky="w", pady=4)

        # Кнопки управления
        btns = tk.Frame(top)
        btns.grid(row=row_f + 1, column=0, columnspan=6, sticky="w", pady=(8, 0))
        
        ttk.Button(btns, text="Обновить список", command=self._load_headers).pack(side="left", padx=(0, 4))
        ttk.Button(btns, text="Сбросить", command=self._reset_filters).pack(side="left", padx=4)
        ttk.Button(btns, text="Загрузить 1С (xlsx)...", command=self._load_hr_from_1c).pack(side="left", padx=12)

        # --- Список объектных табелей ---
        headers_frame = tk.LabelFrame(self, text="Объектные табели (доступные в базе)")
        headers_frame.pack(fill="x", padx=8, pady=4)

        cols = ("year", "month", "department")
        self.tree_headers = ttk.Treeview(headers_frame, columns=cols, show="headings", height=5, selectmode="browse")
        self.tree_headers.heading("year", text="Год"); self.tree_headers.column("year", width=60, anchor="center")
        self.tree_headers.heading("month", text="Месяц"); self.tree_headers.column("month", width=100, anchor="center")
        self.tree_headers.heading("department", text="Подразделение"); self.tree_headers.column("department", width=300, anchor="w")

        vsb_h = ttk.Scrollbar(headers_frame, command=self.tree_headers.yview)
        self.tree_headers.configure(yscrollcommand=vsb_h.set)
        
        self.tree_headers.pack(side="left", fill="x", expand=True)
        vsb_h.pack(side="right", fill="y")
        
        self.tree_headers.bind("<Double-1>", lambda e: self._on_select_header())

        # Кнопки действий
        act_frame = tk.Frame(headers_frame)
        act_frame.pack(side="bottom", fill="x", padx=4, pady=4)
        ttk.Button(act_frame, text="Сравнить выбранное", command=self._on_select_header).pack(side="left", fill="x", expand=True, padx=(0,2))
        ttk.Button(act_frame, text="Экспорт в Excel (с заливкой)", command=self._export_to_excel).pack(side="left", fill="x", expand=True, padx=(2,0))

        # --- Таблица сравнения ---
        compare_frame = tk.LabelFrame(self, text="Результат сравнения")
        compare_frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        tree_cont = tk.Frame(compare_frame)
        tree_cont.pack(fill="both", expand=True)

        self.tree_compare = ttk.Treeview(tree_cont, show="headings", selectmode="browse")
        
        vsb_c = ttk.Scrollbar(tree_cont, orient="vertical", command=self.tree_compare.yview)
        hsb_c = ttk.Scrollbar(tree_cont, orient="horizontal", command=self.tree_compare.xview)
        
        self.tree_compare.configure(yscrollcommand=vsb_c.set, xscrollcommand=hsb_c.set)

        self.tree_compare.grid(row=0, column=0, sticky="nsew")
        vsb_c.grid(row=0, column=1, sticky="ns")
        hsb_c.grid(row=1, column=0, sticky="ew")

        tree_cont.grid_rowconfigure(0, weight=1)
        tree_cont.grid_columnconfigure(0, weight=1)

        self._configure_compare_columns(31)

        # Теги стилей
        self.tree_compare.tag_configure("pair_even", background="#ffffff")
        self.tree_compare.tag_configure("pair_odd", background="#f8f9fa")
        # Строки с ошибками подсвечиваем, но детали будут в тексте ячеек
        self.tree_compare.tag_configure("mismatch_row", background="#fff2cc") 

    # ---------- Логика ----------

    def _reset_filters(self):
        from datetime import datetime
        self.var_year.set(str(datetime.now().year))
        self.var_month.set("Все")
        self.var_dep.set("Все")
        if hasattr(self, "cmb_dep"):
            self.cmb_dep.set("Все")
        self._load_headers()

    def _load_headers(self):
        self.tree_headers.delete(*self.tree_headers.get_children())
        self._headers.clear()
        
        try:
            y = int(self.var_year.get().strip())
        except: y = None
        
        m_name = self.var_month.get().strip()
        m = None
        if m_name and m_name != "Все":
            for i in range(1, 13):
                if month_name_ru(i) == m_name:
                    m = i; break
        
        d = self.var_dep.get().strip()
        dep = d if d and d != "Все" else None

        try:
            # ИСПРАВЛЕНО: Добавлены обязательные аргументы object_addr_substr и object_id_substr
            headers = load_all_timesheet_headers(
                year=y, 
                month=m, 
                department=dep,
                object_addr_substr=None,
                object_id_substr=None
            )
        except Exception as e:
            logging.exception("Load headers error")
            messagebox.showerror("Ошибка", f"Не удалось загрузить список:\n{e}", parent=self)
            return

        self._headers = headers
        self._fill_departments_combo(headers)

        agg_map = {}
        for h in headers:
            key = (int(h["year"]), int(h["month"]), (h.get("department") or "").strip())
            if key not in agg_map:
                agg_map[key] = {"year": key[0], "month": key[1], "department": key[2], "headers": []}
            agg_map[key]["headers"].append(h)

        self._agg_headers = list(agg_map.values())
        self._agg_headers.sort(key=lambda a: (a["year"], a["month"], a["department"]), reverse=True)

        for agg in self._agg_headers:
            iid = f"{agg['year']}:{agg['month']}:{agg['department']}"
            m_ru = month_name_ru(agg['month'])
            self.tree_headers.insert("", "end", iid=iid, values=(agg['year'], m_ru, agg['department']))

    def _load_hr_from_1c(self):
        path = filedialog.askopenfilename(title="Табель 1С (xlsx)", filetypes=[("Excel", "*.xlsx *.xlsm")])
        if not path: return

        try:
            fd, temp_path = tempfile.mkstemp(suffix=".xlsx", prefix="1c_converted_")
            os.close(fd)

            timesheet_transformer.transform_file(path, temp_path, parent=self)

            wb = load_workbook(temp_path, data_only=True)
            ws = wb.active
            
            rows = []
            for r in range(2, ws.max_row + 1):
                fio = str(ws.cell(r, 2).value or "").strip()
                if not fio: continue
                tbn = str(ws.cell(r, 4).value or "").strip()
                
                days = []
                for c in range(6, 6 + 31):
                    v = ws.cell(r, c).value
                    days.append(str(v).strip() if v is not None else None)
                
                rows.append({"fio": fio, "tbn": tbn, "days": days})

            self._hr_rows = rows
            try: os.remove(temp_path)
            except: pass

            messagebox.showinfo("Успех", f"Загружен табель 1С: {len(rows)} сотр.", parent=self)
            self._rebuild_comparison()

        except Exception as e:
            logging.exception("1C Load Error")
            messagebox.showerror("Ошибка", f"Сбой загрузки 1С:\n{e}", parent=self)

    def _on_select_header(self):
        sel = self.tree_headers.selection()
        if not sel: return
        
        iid = sel[0]
        agg = next((a for a in self._agg_headers if f"{a['year']}:{a['month']}:{a['department']}" == iid), None)
        if not agg: return

        obj_rows = []
        try:
            for h in agg["headers"]:
                hid = int(h["id"])
                obj_name = (h.get("object_addr") or "").strip()
                oid = (h.get("object_id") or "").strip()
                if oid: obj_name = f"[{oid}] {obj_name}"

                db_rows = load_timesheet_rows_by_header_id(hid)
                for r in db_rows:
                    days = []
                    raw = r.get("hours_raw") or []
                    for val in raw[:31]:
                        days.append(str(val).strip() if val is not None else None)
                    
                    obj_rows.append({
                        "fio": (r["fio"] or "").strip(),
                        "tbn": (r["tbn"] or "").strip(),
                        "object_display": obj_name,
                        "days": days
                    })
        except Exception as e:
            messagebox.showerror("Ошибка БД", str(e), parent=self)
            return

        self._obj_rows = obj_rows
        
        dim = month_days(agg["year"], agg["month"])
        self._configure_compare_columns(dim)
        
        self._rebuild_comparison()

    def _configure_compare_columns(self, days_in_month: int):
        cols = ["fio", "tbn", "object", "kind"] + [f"d{i}" for i in range(1, days_in_month + 1)]
        self.tree_compare["columns"] = cols
        
        self.tree_compare.heading("fio", text="ФИО"); self.tree_compare.column("fio", width=200, minwidth=150)
        self.tree_compare.heading("tbn", text="Т.№"); self.tree_compare.column("tbn", width=60, anchor="center")
        self.tree_compare.heading("object", text="Объект"); self.tree_compare.column("object", width=250)
        self.tree_compare.heading("kind", text="Источник"); self.tree_compare.column("kind", width=120, anchor="center")
        
        for i in range(1, days_in_month + 1):
            col = f"d{i}"
            self.tree_compare.heading(col, text=str(i))
            # Чуть шире колонка, чтобы влезло "8(≠7)"
            self.tree_compare.column(col, width=45, anchor="center", stretch=False)

    def _rebuild_comparison(self):
        """
        Перестройка таблицы с учетом нормализации регистра.
        Если есть отличие, в ячейку пишем "A (≠B)", чтобы видеть ошибку без Excel.
        """
        self.tree_compare.delete(*self.tree_compare.get_children())
        self._merged_rows.clear()
        
        if not self._obj_rows and not self._hr_rows: return

        # Индекс HR по нормализованному TBN
        hr_map = {normalize_tbn(r["tbn"]): r for r in self._hr_rows}
        
        # Группировка OBJ по TBN
        obj_map: Dict[str, List[Dict]] = {}
        for r in self._obj_rows:
            key = normalize_tbn(r["tbn"])
            obj_map.setdefault(key, []).append(r)

        all_tbns = sorted(set(hr_map.keys()) | set(obj_map.keys()))
        days_count = len(self.tree_compare["columns"]) - 4

        for tbn in all_tbns:
            hr_row = hr_map.get(tbn)
            obj_rows_list = sorted(obj_map.get(tbn, []), key=lambda x: x.get("object_display", ""))

            # 1. ОБЪЕКТНЫЕ СТРОКИ
            for o_row in obj_rows_list:
                raw_days = o_row["days"]
                display_days = list(raw_days) # Копия для отображения в UI
                diff_mask = [False] * 31
                
                has_diff = False
                
                if hr_row:
                    hr_days = hr_row["days"]
                    for i in range(days_count):
                        # Нормализуем перед сравнением (убираем регистр)
                        val_o = normalize_val(raw_days[i] if i < len(raw_days) else None)
                        val_h = normalize_val(hr_days[i] if i < len(hr_days) else None)
                        
                        if val_o != val_h:
                            diff_mask[i] = True
                            has_diff = True
                            # ТРЮК: Пишем разницу прямо в ячейку для UI
                            # Если было '8', а в 1С '7', будет '8 (≠7)'
                            orig = raw_days[i] if i < len(raw_days) and raw_days[i] is not None else ""
                            compare_to = hr_days[i] if i < len(hr_days) and hr_days[i] is not None else ""
                            display_days[i] = f"{orig} (≠{compare_to})"
                
                self._merged_rows.append({
                    "fio": o_row["fio"], "tbn": o_row["tbn"], 
                    "object": o_row["object_display"], "kind": "Объект",
                    "days": raw_days,        # Чистые данные для Excel
                    "display_days": display_days, # Данные с пометками для UI
                    "diff_mask": diff_mask, 
                    "has_diff": has_diff,
                    "is_hr": False
                })

            # 2. СТРОКА 1С (HR)
            if hr_row:
                hr_days = hr_row["days"]
                self._merged_rows.append({
                    "fio": hr_row["fio"], "tbn": hr_row["tbn"],
                    "object": "", "kind": "1С Кадры",
                    "days": hr_days, 
                    "display_days": hr_days, # У 1С отображаем как есть
                    "diff_mask": [False]*31, 
                    "has_diff": False,
                    "is_hr": True
                })

        # Рендеринг
        grp_idx = 0
        prev_tbn = None
        
        for row in self._merged_rows:
            curr_tbn = normalize_tbn(row["tbn"])
            if curr_tbn != prev_tbn:
                grp_idx += 1
                prev_tbn = curr_tbn
            
            tag_bg = "pair_even" if grp_idx % 2 == 0 else "pair_odd"
            tags = [tag_bg]
            
            if not row["is_hr"] and row["has_diff"]:
                tags.append("mismatch_row")

            vals = [row["fio"], row["tbn"], row["object"], row["kind"]]
            
            # В Treeview загружаем display_days (где текст изменен при ошибке)
            d_vals = row["display_days"][:days_count]
            if len(d_vals) < days_count: d_vals += [""]*(days_count - len(d_vals))
            
            self.tree_compare.insert("", "end", values=vals + d_vals, tags=tags)


    def _fill_departments_combo(self, headers):
        deps = sorted({(h.get("department") or "").strip() for h in headers if h.get("department")})
        vals = ["Все"] + deps
        self.cmb_dep.configure(values=vals)
        if self.var_dep.get() not in vals:
            self.var_dep.set("Все")

    def _export_to_excel(self):
        """
        Экспорт в Excel.
        - Используем нормализацию для чистоты данных.
        - Красим конкретные ячейки, а не строки.
        """
        if not self._merged_rows:
            messagebox.showwarning("Экспорт", "Нет данных для экспорта", parent=self)
            return

        fpath = filedialog.asksaveasfilename(
            title="Сохранить свод", 
            defaultextension=".xlsx", 
            initialfile="Свод_сравнения.xlsx",
            filetypes=[("Excel", "*.xlsx")]
        )
        if not fpath: return

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Сравнение"
            
            days_cnt = len(self.tree_compare["columns"]) - 4
            headers = ["ФИО", "Таб.№", "Объект", "Источник"] + [str(i) for i in range(1, days_cnt+1)]
            ws.append(headers)

            fill_diff = PatternFill("solid", fgColor="FF9999") # Красный для ошибок
            fill_hr = PatternFill("solid", fgColor="EFEFEF")   # Серый для 1С
            font_bold = Font(bold=True)
            
            center = Alignment(horizontal="center")

            for row_data in self._merged_rows:
                out = [
                    row_data["fio"], row_data["tbn"], 
                    row_data["object"], row_data["kind"]
                ]
                
                # В Excel пишем ЧИСТЫЕ данные (row_data["days"]), без приписок (≠7)
                d_vals = row_data["days"][:days_cnt]
                d_vals += [None]*(days_cnt - len(d_vals))
                out.extend(d_vals)
                
                ws.append(out)
                cur_row_idx = ws.max_row
                
                # Применяем стили
                if row_data["is_hr"]:
                    # Строка 1С - серая
                    for c in range(1, len(out)+1):
                        ws.cell(cur_row_idx, c).fill = fill_hr
                else:
                    # Строка Объекта - красим только ячейки с ошибками
                    mask = row_data.get("diff_mask", [])
                    for i, is_diff in enumerate(mask):
                        if i >= days_cnt: break
                        if is_diff:
                            # +5, т.к. данные начинаются с 5 колонки (E)
                            cell = ws.cell(cur_row_idx, 5 + i)
                            cell.fill = fill_diff
                            cell.font = font_bold # Жирный шрифт для ошибки

            # Красота колонок
            ws.column_dimensions["A"].width = 30
            ws.column_dimensions["C"].width = 40
            for i in range(days_cnt):
                col_letter = get_column_letter(5 + i)
                ws.column_dimensions[col_letter].width = 6
            
            wb.save(fpath)
            messagebox.showinfo("Готово", f"Сохранено:\n{fpath}", parent=self)

        except Exception as e:
            logging.exception("Export Error")
            messagebox.showerror("Ошибка", f"Не удалось сохранить Excel:\n{e}", parent=self)


# ---- API ----
def create_timesheet_compare_page(parent, app_ref) -> TimesheetComparePage:
    return TimesheetComparePage(parent, app_ref=app_ref)
