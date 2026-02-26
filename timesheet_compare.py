# timesheet_compare.py
from __future__ import annotations

import os
import tempfile
import logging
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from openpyxl import load_workbook, Workbook
from openpyxl.styles import PatternFill, Alignment, Font, Border, Side
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
    """Убирает ведущие нули и пробелы (0055 -> 55)."""
    s = str(val or "").strip()
    return s.lstrip("0") if s else ""


def normalize_val(val: Any) -> str:
    """
    Нормализация значения для сравнения.
    Возвращает пустую строку, если val is None.
    """
    if val is None:
        return ""
    s = str(val).strip().lower()
    s = s.replace(",", ".")
    if s.endswith(".0"):
        s = s[:-2]
    if s == "none":
        return ""
    return s


class TimesheetComparePage(tk.Frame):
    def __init__(self, master, app_ref):
        super().__init__(master)
        self.app_ref = app_ref

        self._headers: List[Dict[str, Any]] = []
        self._obj_rows: List[Dict[str, Any]] = []
        self._hr_rows: List[Dict[str, Any]] = []
        
        self._merged_groups: List[Dict[str, Any]] = []
        self._agg_headers: List[Dict[str, Any]] = []

        self._build_ui()
        self._load_headers()

    # ---------- UI ----------

    def _build_ui(self):
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

        # Кнопки
        btns = tk.Frame(top)
        btns.grid(row=row_f + 1, column=0, columnspan=6, sticky="w", pady=(8, 0))
        
        ttk.Button(btns, text="Обновить список", command=self._load_headers).pack(side="left", padx=(0, 4))
        ttk.Button(btns, text="Сбросить", command=self._reset_filters).pack(side="left", padx=4)
        ttk.Button(btns, text="Загрузить 1С (xlsx)...", command=self._load_hr_from_1c).pack(side="left", padx=12)

        # --- Headers ---
        headers_frame = tk.LabelFrame(self, text="Объектные табели")
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
        ttk.Button(act_frame, text="Экспорт в Excel", command=self._export_to_excel).pack(side="left", fill="x", expand=True, padx=(2,0))

        # --- Compare Grid ---
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

        # Теги для визуализации
        self.tree_compare.tag_configure("group_start", background="#ffffff") 
        self.tree_compare.tag_configure("group_item", background="#ffffff")
        self.tree_compare.tag_configure("hr_row", background="#f2f2f2") # Серый фон для 1С

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
        
        try: y = int(self.var_year.get().strip())
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
            headers = load_all_timesheet_headers(year=y, month=m, department=dep, object_addr_substr=None, object_id_substr=None)
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
                    days.append(v)
                
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
                        days.append(val)
                    
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
            self.tree_compare.column(col, width=45, anchor="center", stretch=False)

    def _rebuild_comparison(self):
        """
        Перестройка данных. Группируем по TBN.
        ПРИОРИТЕТ ФИО: Сначала из объектного табеля (программы), затем 1С.
        """
        self.tree_compare.delete(*self.tree_compare.get_children())
        self._merged_groups.clear()
        
        if not self._obj_rows and not self._hr_rows: return

        # 1. Индексируем HR
        hr_map = {normalize_tbn(r["tbn"]): r for r in self._hr_rows}
        
        # 2. Группируем OBJ
        obj_map: Dict[str, List[Dict]] = {}
        for r in self._obj_rows:
            key = normalize_tbn(r["tbn"])
            obj_map.setdefault(key, []).append(r)

        all_tbns = sorted(set(hr_map.keys()) | set(obj_map.keys()))
        days_count = len(self.tree_compare["columns"]) - 4

        for tbn in all_tbns:
            hr_row = hr_map.get(tbn)
            obj_rows_list = sorted(obj_map.get(tbn, []), key=lambda x: x.get("object_display", ""))
            
            # --- ПРИОРИТЕТ ФИО ИЗ ПРОГРАММЫ ---
            if obj_rows_list:
                main_fio = obj_rows_list[0]["fio"]
                main_tbn = obj_rows_list[0]["tbn"]
            elif hr_row:
                main_fio = hr_row["fio"]
                main_tbn = hr_row["tbn"]
            else:
                main_fio = "???"
                main_tbn = tbn

            group = {
                "tbn_key": tbn,
                "display_fio": main_fio,
                "display_tbn": main_tbn,
                "hr_row": hr_row,
                "obj_rows": obj_rows_list
            }
            self._merged_groups.append(group)
            
            # --- РЕНДЕРИНГ В ГРИД ---
            
            # А. Объекты
            first_row = True
            for o_row in obj_rows_list:
                fio_cell = main_fio if first_row else ""
                tbn_cell = main_tbn if first_row else ""
                
                vals = [fio_cell, tbn_cell, o_row["object_display"], "Объект"]
                
                display_days = []
                raw_days = o_row["days"]
                hr_days = hr_row["days"] if hr_row else []
                
                for i in range(days_count):
                    val_o = raw_days[i] if i < len(raw_days) else None
                    val_h = hr_days[i] if i < len(hr_days) else None
                    
                    norm_o = normalize_val(val_o)
                    norm_h = normalize_val(val_h)
                    
                    if not norm_o:
                        display_days.append("") 
                    else:
                        if norm_o == norm_h:
                            display_days.append(str(val_o))
                        else:
                            h_str = str(val_h) if val_h is not None else ""
                            display_days.append(f"{val_o} (≠{h_str})")

                tag = "group_start" if first_row else "group_item"
                self.tree_compare.insert("", "end", values=vals + display_days, tags=(tag,))
                first_row = False

            # Б. 1С (Выводим ФИО снова, чтобы было понятно, чья строка)
            if hr_row:
                # ВНИМАНИЕ: Здесь теперь всегда пишем ФИО и ТБН для ясности
                fio_cell = main_fio
                tbn_cell = main_tbn
                
                vals = [fio_cell, tbn_cell, "", "1С Кадры"]
                
                d_vals = []
                for v in hr_row["days"][:days_count]:
                    d_vals.append(str(v) if v is not None else "")
                
                self.tree_compare.insert("", "end", values=vals + d_vals, tags=("hr_row",))


    def _fill_departments_combo(self, headers):
        deps = sorted({(h.get("department") or "").strip() for h in headers if h.get("department")})
        vals = ["Все"] + deps
        self.cmb_dep.configure(values=vals)
        if self.var_dep.get() not in vals:
            self.var_dep.set("Все")

    def _export_to_excel(self):
        """
        Экспорт в Excel. 
        - Приоритет ФИО из Программы.
        - В строке 1С ФИО дублируется.
        """
        if not self._merged_groups:
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

            fill_error = PatternFill("solid", fgColor="FF9999")
            fill_hr = PatternFill("solid", fgColor="EFEFEF") 
            fill_miss = PatternFill("solid", fgColor="FFCC99")
            
            border_bottom = Border(bottom=Side(style='thin', color='B0B0B0'))
            font_bold = Font(bold=True)

            for grp in self._merged_groups:
                hr_row = grp.get("hr_row")
                obj_rows = grp.get("obj_rows", [])
                
                main_fio = grp["display_fio"]
                main_tbn = grp["display_tbn"]

                first_in_group = True
                
                # 1. Объекты
                for o_row in obj_rows:
                    fio_val = main_fio if first_in_group else ""
                    tbn_val = main_tbn if first_in_group else ""
                    
                    row_data = [fio_val, tbn_val, o_row["object_display"], "Объект"]
                    
                    days_raw = o_row["days"][:days_cnt]
                    days_raw += [None]*(days_cnt - len(days_raw))
                    days_clean = [ (v if v is not None else "") for v in days_raw ]
                    row_data.extend(days_clean)
                    
                    ws.append(row_data)
                    cur_idx = ws.max_row
                    
                    if hr_row:
                        hr_days = hr_row["days"]
                        for i in range(days_cnt):
                            val_o = days_raw[i] 
                            norm_o = normalize_val(val_o)
                            
                            if not norm_o: continue
                                
                            val_h = hr_days[i] if i < len(hr_days) else None
                            norm_h = normalize_val(val_h)
                            
                            if norm_o != norm_h:
                                cell = ws.cell(cur_idx, 5 + i)
                                cell.fill = fill_error
                                cell.font = font_bold
                    
                    first_in_group = False

                # 2. 1С (Теперь всегда пишем ФИО)
                if hr_row:
                    fio_val = main_fio # Дублируем имя для ясности
                    tbn_val = main_tbn
                    
                    row_data = [fio_val, tbn_val, "", "1С Кадры"]
                    
                    days_raw = hr_row["days"][:days_cnt]
                    days_raw += [None]*(days_cnt - len(days_raw))
                    days_clean = [ (v if v is not None else "") for v in days_raw ]
                    row_data.extend(days_clean)
                    
                    ws.append(row_data)
                    cur_idx = ws.max_row
                    
                    # Стиль 1С строки
                    for c in range(1, len(row_data)+1):
                        cell = ws.cell(cur_idx, c)
                        cell.fill = fill_hr
                        cell.border = border_bottom
                    
                    # Проверка "Потеряшек"
                    for i in range(days_cnt):
                        val_h = days_raw[i]
                        norm_h = normalize_val(val_h)
                        if not norm_h: continue
                        
                        found_in_objects = False
                        for o_row in obj_rows:
                            d_list = o_row["days"]
                            v_o = d_list[i] if i < len(d_list) else None
                            if normalize_val(v_o):
                                found_in_objects = True
                                break
                        
                        if not found_in_objects:
                            ws.cell(cur_idx, 5 + i).fill = fill_miss
                
                else:
                    if obj_rows:
                        for c in range(1, len(headers)+1):
                            ws.cell(ws.max_row, c).border = border_bottom

            # Ширина
            ws.column_dimensions["A"].width = 30
            ws.column_dimensions["C"].width = 40
            for i in range(days_cnt):
                col_letter = get_column_letter(5 + i)
                ws.column_dimensions[col_letter].width = 6
            
            wb.save(fpath)
            messagebox.showinfo("Готово", f"Файл сохранен:\n{fpath}", parent=self)

        except Exception as e:
            logging.exception("Export Error")
            messagebox.showerror("Ошибка", f"Не удалось сохранить Excel:\n{e}", parent=self)


# ---- API ----
def create_timesheet_compare_page(parent, app_ref) -> TimesheetComparePage:
    return TimesheetComparePage(parent, app_ref=app_ref)
