# timesheet_compare.py
from __future__ import annotations

import os
import tempfile
import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from openpyxl import load_workbook, Workbook
from openpyxl.styles import PatternFill, Alignment, Font, Border, Side
from openpyxl.utils import get_column_letter

import timesheet_transformer

from timesheet_module import (
    load_all_timesheet_headers,
    load_timesheet_rows_by_header_id,
    month_name_ru,
    month_days,
    set_db_pool as _set_db_pool_from_timesheet,
)


def set_db_pool(pool):
    _set_db_pool_from_timesheet(pool)


# ============================================================
#  Цветовая схема
# ============================================================
CMP_COLORS = {
    "bg":            "#f0f2f5",
    "panel":         "#ffffff",
    "accent":        "#1565c0",
    "accent_light":  "#e3f2fd",
    "warning":       "#b00020",
    "border":        "#dde1e7",
    "btn_save_bg":   "#1565c0",
    "btn_save_fg":   "#ffffff",
    # строки таблицы сравнения
    "row_obj_ok":    "#ffffff",
    "row_obj_diff":  "#ffcdd2",   # красноватый — расхождение
    "row_1c":        "#f5f5f5",   # серый — строка 1С
    "row_only_obj":  "#fff9c4",   # жёлтый — только в объекте
    "row_only_1c":   "#ffe0b2",   # оранжевый — только в 1С
    "cell_diff":     "#ef9a9a",   # ячейка с расхождением
    "cell_miss_1c":  "#ffcc80",   # ячейка: есть в 1С, нет в объекте
}


# ============================================================
#  Утилиты
# ============================================================

def normalize_tbn(val: Any) -> str:
    s = str(val or "").strip()
    return s.lstrip("0") if s else ""

def fio_sort_key(fio: Any) -> str:
    s = str(fio or "").strip().lower()
    s = " ".join(s.split())
    s = s.replace("ё", "е")
    return s

def normalize_val(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip().lower().replace(",", ".")
    if s.endswith(".0"):
        s = s[:-2]
    return "" if s == "none" else s


# ============================================================
#  Главная страница
# ============================================================

class TimesheetComparePage(tk.Frame):

    def __init__(self, master, app_ref):
        super().__init__(master, bg=CMP_COLORS["bg"])
        self.app_ref = app_ref

        self._headers:       List[Dict[str, Any]] = []
        self._obj_rows:      List[Dict[str, Any]] = []
        self._hr_rows:       List[Dict[str, Any]] = []
        self._merged_groups: List[Dict[str, Any]] = []
        self._agg_headers:   List[Dict[str, Any]] = []

        # Статистика последнего сравнения
        self._stat_total    = 0
        self._stat_diff     = 0
        self._stat_only_obj = 0
        self._stat_only_1c  = 0

        self._build_ui()
        self._load_headers()

    # ──────────────────────────────────────────────────────────
    #  UI
    # ──────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Заголовок ─────────────────────────────────────────
        hdr = tk.Frame(self, bg=CMP_COLORS["accent"], pady=6)
        hdr.pack(fill="x")
        tk.Label(
            hdr, text="🔍  Сравнение табелей (Объект vs 1С)",
            font=("Segoe UI", 12, "bold"),
            bg=CMP_COLORS["accent"], fg="white", padx=12
        ).pack(side="left")

        self.lbl_hr_status = tk.Label(
            hdr, text="1С: не загружен",
            font=("Segoe UI", 9),
            bg=CMP_COLORS["accent"], fg="#ffcc80", padx=10
        )
        self.lbl_hr_status.pack(side="right")

        # ── Панель фильтров ───────────────────────────────────
        filter_pnl = tk.LabelFrame(
            self, text=" 🔍 Фильтры ",
            font=("Segoe UI", 9, "bold"),
            bg=CMP_COLORS["panel"], fg=CMP_COLORS["accent"],
            relief="groove", bd=1, padx=10, pady=8
        )
        filter_pnl.pack(fill="x", padx=10, pady=(8, 4))
        filter_pnl.grid_columnconfigure(1, weight=0)
        filter_pnl.grid_columnconfigure(3, weight=0)
        filter_pnl.grid_columnconfigure(5, weight=1)

        # Год
        tk.Label(filter_pnl, text="Год:",
                 font=("Segoe UI", 9), bg=CMP_COLORS["panel"]
                 ).grid(row=0, column=0, sticky="e", padx=(0, 6), pady=3)
        self.var_year = tk.StringVar(value=str(datetime.now().year))
        tk.Spinbox(filter_pnl, from_=2000, to=2100, width=7,
                   textvariable=self.var_year, font=("Segoe UI", 9)
                   ).grid(row=0, column=1, sticky="w", pady=3)

        # Месяц
        tk.Label(filter_pnl, text="Месяц:",
                 font=("Segoe UI", 9), bg=CMP_COLORS["panel"]
                 ).grid(row=0, column=2, sticky="e", padx=(16, 6), pady=3)
        self.var_month = tk.StringVar(value="Все")
        cmb_m = ttk.Combobox(
            filter_pnl, state="readonly", width=14,
            textvariable=self.var_month,
            values=["Все"] + [month_name_ru(i) for i in range(1, 13)]
        )
        cmb_m.grid(row=0, column=3, sticky="w", pady=3)
        cmb_m.bind("<<ComboboxSelected>>", lambda e: self._load_headers())

        # Подразделение
        tk.Label(filter_pnl, text="Подразделение:",
                 font=("Segoe UI", 9), bg=CMP_COLORS["panel"]
                 ).grid(row=0, column=4, sticky="e", padx=(16, 6), pady=3)
        self.var_dep = tk.StringVar(value="Все")
        self.cmb_dep = ttk.Combobox(
            filter_pnl, state="readonly", width=36,
            textvariable=self.var_dep, values=["Все"]
        )
        self.cmb_dep.grid(row=0, column=5, sticky="ew", pady=3)
        self.cmb_dep.bind("<<ComboboxSelected>>", lambda e: self._load_headers())

        # Кнопки фильтра
        btn_f = tk.Frame(filter_pnl, bg=CMP_COLORS["panel"])
        btn_f.grid(row=0, column=6, sticky="e", padx=(16, 0))

        tk.Button(
            btn_f, text="🔄 Обновить",
            font=("Segoe UI", 9, "bold"),
            bg=CMP_COLORS["btn_save_bg"], fg=CMP_COLORS["btn_save_fg"],
            activebackground="#0d47a1", activeforeground="white",
            relief="flat", cursor="hand2", padx=10, pady=3,
            command=self._load_headers
        ).pack(side="left", padx=(0, 4))

        ttk.Button(btn_f, text="Сбросить",
                   command=self._reset_filters
                   ).pack(side="left", padx=(0, 4))

        # Кнопка загрузки 1С — акцентная
        tk.Button(
            btn_f, text="📂 Загрузить 1С (xlsx)…",
            font=("Segoe UI", 9, "bold"),
            bg="#2e7d32", fg="white",
            activebackground="#1b5e20", activeforeground="white",
            relief="flat", cursor="hand2", padx=10, pady=3,
            command=self._load_hr_from_1c
        ).pack(side="left")

        # ── Секция 1: выбор периода/подразделения ─────────────
        sel_pnl = tk.LabelFrame(
            self,
            text=" 1️⃣  Выбор табеля для сравнения ",
            font=("Segoe UI", 9, "bold"),
            bg=CMP_COLORS["panel"], fg=CMP_COLORS["accent"],
            relief="groove", bd=1
        )
        sel_pnl.pack(fill="x", padx=10, pady=(4, 2))

        # Заголовок таблицы + кнопка рядом
        sel_top = tk.Frame(sel_pnl, bg=CMP_COLORS["panel"])
        sel_top.pack(fill="x", padx=8, pady=(4, 2))

        tk.Label(
            sel_top,
            text="Двойной щелчок или кнопка «Сравнить» — загрузить данные:",
            font=("Segoe UI", 8), fg="#555",
            bg=CMP_COLORS["panel"]
        ).pack(side="left")

        tk.Button(
            sel_top, text="▶  Сравнить выбранное",
            font=("Segoe UI", 9, "bold"),
            bg=CMP_COLORS["btn_save_bg"], fg=CMP_COLORS["btn_save_fg"],
            activebackground="#0d47a1", activeforeground="white",
            relief="flat", cursor="hand2", padx=10, pady=3,
            command=self._on_select_header
        ).pack(side="right", padx=(0, 4))

        self.btn_export = tk.Button(
            sel_top, text="📊 Экспорт в Excel",
            font=("Segoe UI", 9, "bold"),
            bg="#2e7d32", fg="white",
            activebackground="#1b5e20", activeforeground="white",
            relief="flat", cursor="hand2", padx=10, pady=3,
            command=self._start_export_thread
        )
        self.btn_export.pack(side="right", padx=(0, 8))

        # Таблица выбора
        hdr_tbl = tk.Frame(sel_pnl, bg=CMP_COLORS["panel"])
        hdr_tbl.pack(fill="x", padx=8, pady=(0, 6))

        cols_h = ("year", "month", "department", "obj_count")
        self.tree_headers = ttk.Treeview(
            hdr_tbl, columns=cols_h,
            show="headings", height=4, selectmode="browse"
        )
        self.tree_headers.heading("year",       text="Год")
        self.tree_headers.heading("month",      text="Месяц")
        self.tree_headers.heading("department", text="Подразделение")
        self.tree_headers.heading("obj_count",  text="Объектов")

        self.tree_headers.column("year",       width=60,  anchor="center", stretch=False)
        self.tree_headers.column("month",      width=110, anchor="center", stretch=False)
        self.tree_headers.column("department", width=360, anchor="w")
        self.tree_headers.column("obj_count",  width=80,  anchor="center", stretch=False)

        vsb_h = ttk.Scrollbar(hdr_tbl, orient="vertical",
                               command=self.tree_headers.yview)
        self.tree_headers.configure(yscrollcommand=vsb_h.set)
        self.tree_headers.pack(side="left", fill="x", expand=True)
        vsb_h.pack(side="right", fill="y")

        self.tree_headers.bind("<Double-1>",
                               lambda e: self._on_select_header())

        # ── Секция 2: результат + статистика ──────────────────
        cmp_outer = tk.Frame(self, bg=CMP_COLORS["bg"])
        cmp_outer.pack(fill="both", expand=True, padx=10, pady=(4, 4))
        cmp_outer.grid_rowconfigure(0, weight=1)
        cmp_outer.grid_columnconfigure(0, weight=1)

        cmp_pnl = tk.LabelFrame(
            cmp_outer,
            text=" 2️⃣  Результат сравнения ",
            font=("Segoe UI", 9, "bold"),
            bg=CMP_COLORS["panel"], fg=CMP_COLORS["accent"],
            relief="groove", bd=1
        )
        cmp_pnl.grid(row=0, column=0, sticky="nsew")

        # Тулбар результата
        cmp_tool = tk.Frame(cmp_pnl, bg=CMP_COLORS["accent_light"], pady=4)
        cmp_tool.pack(fill="x")

        # Фильтр расхождений
        self.var_only_diff = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            cmp_tool,
            text="Только расхождения",
            variable=self.var_only_diff,
            command=self._rebuild_comparison
        ).pack(side="left", padx=(8, 4))

        tk.Frame(cmp_tool, bg=CMP_COLORS["border"], width=1).pack(
            side="left", fill="y", padx=6
        )

        # Легенда цветов
        for color, label in [
            ("#ffcdd2", "Расхождение"),
            ("#ffe0b2", "Только в 1С"),
            ("#fff9c4", "Только в объекте"),
            ("#f5f5f5", "Строка 1С"),
        ]:
            dot = tk.Label(cmp_tool, text="  ", bg=color,
                           relief="solid", bd=1)
            dot.pack(side="left", padx=(4, 2))
            tk.Label(cmp_tool, text=label,
                     font=("Segoe UI", 8),
                     bg=CMP_COLORS["accent_light"]
                     ).pack(side="left", padx=(0, 6))

        # Статистика справа
        self.lbl_stat = tk.Label(
            cmp_tool,
            text="",
            font=("Segoe UI", 9, "bold"),
            fg=CMP_COLORS["accent"],
            bg=CMP_COLORS["accent_light"]
        )
        self.lbl_stat.pack(side="right", padx=12)

        # Таблица результата
        tree_cont = tk.Frame(cmp_pnl, bg=CMP_COLORS["panel"])
        tree_cont.pack(fill="both", expand=True)
        tree_cont.grid_rowconfigure(0, weight=1)
        tree_cont.grid_columnconfigure(0, weight=1)

        self.tree_compare = ttk.Treeview(
            tree_cont, show="headings", selectmode="browse"
        )

        vsb_c = ttk.Scrollbar(tree_cont, orient="vertical",
                               command=self.tree_compare.yview)
        hsb_c = ttk.Scrollbar(tree_cont, orient="horizontal",
                               command=self.tree_compare.xview)
        self.tree_compare.configure(
            yscrollcommand=vsb_c.set,
            xscrollcommand=hsb_c.set
        )
        self.tree_compare.grid(row=0, column=0, sticky="nsew")
        vsb_c.grid(row=0, column=1, sticky="ns")
        hsb_c.grid(row=1, column=0, sticky="ew")

        # Теги строк
        self.tree_compare.tag_configure(
            "obj_ok",   background=CMP_COLORS["row_obj_ok"])
        self.tree_compare.tag_configure(
            "obj_diff", background=CMP_COLORS["row_obj_diff"])
        self.tree_compare.tag_configure(
            "hr_row",   background=CMP_COLORS["row_1c"])
        self.tree_compare.tag_configure(
            "only_obj", background=CMP_COLORS["row_only_obj"])
        self.tree_compare.tag_configure(
            "only_1c",  background=CMP_COLORS["row_only_1c"])

        self._configure_compare_columns(31)

        # ── Статус-бар ────────────────────────────────────────
        status_bar = tk.Frame(self, bg=CMP_COLORS["panel"],
                              relief="sunken", bd=1)
        status_bar.pack(side="bottom", fill="x")

        self.var_status = tk.StringVar(value="Готов к работе")
        tk.Label(
            status_bar, textvariable=self.var_status,
            anchor="w", font=("Segoe UI", 9),
            bg=CMP_COLORS["panel"]
        ).pack(side="left", padx=6)

        self.progress_bar = ttk.Progressbar(
            status_bar, orient="horizontal",
            mode="determinate", length=240
        )
        self.progress_bar.pack(side="right", padx=6, pady=2)

    # ──────────────────────────────────────────────────────────
    #  Вспомогательные методы UI
    # ──────────────────────────────────────────────────────────

    def _configure_compare_columns(self, days_in_month: int):
        cols = (
            ["status", "fio", "tbn", "object", "kind"]
            + [f"d{i}" for i in range(1, days_in_month + 1)]
            + ["total_obj", "total_1c"]
        )
        self.tree_compare["columns"] = cols

        self.tree_compare.heading("status", text="")
        self.tree_compare.column("status", width=26, anchor="center",
                                 stretch=False)

        self.tree_compare.heading("fio", text="ФИО")
        self.tree_compare.column("fio", width=200, minwidth=140)

        self.tree_compare.heading("tbn", text="Таб.№")
        self.tree_compare.column("tbn", width=65, anchor="center",
                                 stretch=False)

        self.tree_compare.heading("object", text="Объект")
        self.tree_compare.column("object", width=240)

        self.tree_compare.heading("kind", text="Источник")
        self.tree_compare.column("kind", width=90, anchor="center",
                                 stretch=False)

        for i in range(1, days_in_month + 1):
            col = f"d{i}"
            self.tree_compare.heading(col, text=str(i))
            self.tree_compare.column(col, width=34, anchor="center",
                                     stretch=False)

        self.tree_compare.heading("total_obj", text="∑ Об.")
        self.tree_compare.column("total_obj", width=52, anchor="center",
                                 stretch=False)
        self.tree_compare.heading("total_1c", text="∑ 1С")
        self.tree_compare.column("total_1c", width=52, anchor="center",
                                 stretch=False)

    def _update_stat_label(self):
        parts = [f"Всего: {self._stat_total}"]
        if self._stat_diff:
            parts.append(f"⚠ Расхождений: {self._stat_diff}")
        if self._stat_only_obj:
            parts.append(f"Только объект: {self._stat_only_obj}")
        if self._stat_only_1c:
            parts.append(f"Только 1С: {self._stat_only_1c}")
        try:
            self.lbl_stat.config(text="  |  ".join(parts))
        except Exception:
            pass

    def _fill_departments_combo(self, headers):
        deps = sorted({
            (h.get("department") or "").strip()
            for h in headers if h.get("department")
        })
        vals = ["Все"] + deps
        self.cmb_dep.configure(values=vals)
        if self.var_dep.get() not in vals:
            self.var_dep.set("Все")

    # ──────────────────────────────────────────────────────────
    #  Загрузка данных
    # ──────────────────────────────────────────────────────────

    def _reset_filters(self):
        self.var_year.set(str(datetime.now().year))
        self.var_month.set("Все")
        self.var_dep.set("Все")
        self._load_headers()

    def _load_headers(self):
        self.tree_headers.delete(*self.tree_headers.get_children())
        self._headers.clear()

        try:
            y = int(self.var_year.get().strip())
        except Exception:
            y = None

        m_name = self.var_month.get().strip()
        m = None
        if m_name and m_name != "Все":
            for i in range(1, 13):
                if month_name_ru(i) == m_name:
                    m = i
                    break

        d = self.var_dep.get().strip()
        dep = d if d and d != "Все" else None

        try:
            headers = load_all_timesheet_headers(
                year=y, month=m, department=dep,
                object_addr_substr=None, object_id_substr=None
            )
        except Exception as e:
            logging.exception("Load headers error")
            messagebox.showerror("Ошибка",
                                 f"Не удалось загрузить список:\n{e}",
                                 parent=self)
            return

        self._headers = headers
        self._fill_departments_combo(headers)

        # Агрегация по (год, месяц, подразделение)
        agg_map: Dict[Tuple, Dict] = {}
        for h in headers:
            key = (
                int(h["year"]),
                int(h["month"]),
                (h.get("department") or "").strip()
            )
            if key not in agg_map:
                agg_map[key] = {
                    "year": key[0], "month": key[1],
                    "department": key[2], "headers": []
                }
            agg_map[key]["headers"].append(h)

        self._agg_headers = sorted(
            agg_map.values(),
            key=lambda a: (a["year"], a["month"], a["department"]),
            reverse=True
        )

        for agg in self._agg_headers:
            iid     = f"{agg['year']}:{agg['month']}:{agg['department']}"
            m_ru    = month_name_ru(agg["month"])
            obj_cnt = len(agg["headers"])
            self.tree_headers.insert(
                "", "end", iid=iid,
                values=(agg["year"], m_ru, agg["department"], obj_cnt)
            )

        self.var_status.set(
            f"Загружено периодов: {len(self._agg_headers)}"
        )

    def _load_hr_from_1c(self):
        path = filedialog.askopenfilename(
            title="Табель 1С (xlsx)",
            filetypes=[("Excel", "*.xlsx *.xlsm")]
        )
        if not path:
            return

        try:
            fd, temp_path = tempfile.mkstemp(
                suffix=".xlsx", prefix="1c_converted_"
            )
            os.close(fd)

            self.var_status.set("Конвертация файла 1С…")
            self.update_idletasks()

            timesheet_transformer.transform_file(path, temp_path, parent=self)

            wb = load_workbook(temp_path, data_only=True)
            ws = wb.active
            rows = []
            for r in range(2, ws.max_row + 1):
                fio = str(ws.cell(r, 2).value or "").strip()
                if not fio:
                    continue
                tbn  = str(ws.cell(r, 4).value or "").strip()
                days = [ws.cell(r, c).value for c in range(6, 6 + 31)]
                rows.append({"fio": fio, "tbn": tbn, "days": days})

            self._hr_rows = rows
            try:
                os.remove(temp_path)
            except Exception:
                pass

            self.var_status.set(
                f"Загружен табель 1С: {len(rows)} сотрудников"
            )
            try:
                self.lbl_hr_status.config(
                    text=f"1С: {len(rows)} чел. ✓",
                    fg="#a5d6a7"
                )
            except Exception:
                pass

            messagebox.showinfo(
                "Загрузка 1С",
                f"Файл успешно загружен.\nСотрудников: {len(rows)}",
                parent=self
            )
            self._rebuild_comparison()

        except Exception as e:
            logging.exception("1C Load Error")
            messagebox.showerror("Ошибка",
                                 f"Сбой загрузки 1С:\n{e}", parent=self)
            self.var_status.set("Ошибка загрузки 1С")

    def _on_select_header(self):
        sel = self.tree_headers.selection()
        if not sel:
            messagebox.showinfo("Сравнение",
                                "Выберите строку в таблице.", parent=self)
            return

        iid = sel[0]
        agg = next(
            (a for a in self._agg_headers
             if f"{a['year']}:{a['month']}:{a['department']}" == iid),
            None
        )
        if not agg:
            return

        self.var_status.set("Загрузка данных из БД…")
        self.update_idletasks()

        obj_rows = []
        try:
            for h in agg["headers"]:
                hid      = int(h["id"])
                obj_name = (h.get("object_addr") or "").strip()
                oid      = (h.get("object_id") or "").strip()
                if oid:
                    obj_name = f"[{oid}] {obj_name}"

                for r in load_timesheet_rows_by_header_id(hid):
                    raw  = r.get("hours_raw") or []
                    days = list(raw[:31])
                    obj_rows.append({
                        "fio":            (r["fio"] or "").strip(),
                        "tbn":            (r["tbn"] or "").strip(),
                        "object_display": obj_name,
                        "days":           days,
                    })
        except Exception as e:
            messagebox.showerror("Ошибка БД", str(e), parent=self)
            return

        self._obj_rows = obj_rows
        dim = month_days(agg["year"], agg["month"])
        self._configure_compare_columns(dim)
        self._rebuild_comparison()

    # ──────────────────────────────────────────────────────────
    #  Сравнение
    # ──────────────────────────────────────────────────────────

    def _sum_days(self, days: List, count: int) -> float:
        """Суммирует числовые значения дней (для итоговых колонок)."""
        total = 0.0
        for i in range(min(count, len(days))):
            v = normalize_val(days[i])
            try:
                total += float(v)
            except Exception:
                pass
        return total

    def _render_compare_from_groups(self):
        self.tree_compare.delete(*self.tree_compare.get_children())
    
        # Колонок дней (без status/fio/tbn/object/kind/total_obj/total_1c)
        all_cols   = list(self.tree_compare["columns"])
        days_count = len(all_cols) - 7
    
        def _fmt(v: float) -> str:
            return f"{v:.1f}".rstrip("0").rstrip(".") if v else ""
    
        for grp in self._merged_groups:
            situation = grp.get("situation", "both")
            has_diff  = grp.get("has_diff", False)
            hr_row    = grp.get("hr_row")
            obj_rows  = grp.get("obj_rows", [])
            main_fio  = grp["display_fio"]
            main_tbn  = grp["display_tbn"]
    
            # ── Тег и иконка ───────────────────────────────
            if situation == "only_obj":
                tag_obj = "only_obj"
                icon    = "⚪"
            elif situation == "only_1c":
                tag_obj = "only_1c"
                icon    = "🟠"
            elif has_diff:
                tag_obj = "obj_diff"
                icon    = "🔴"
            else:
                tag_obj = "obj_ok"
                icon    = "🟢"
    
            # ── Суммы ──────────────────────────────────────
            sum_obj = sum(self._sum_days(o["days"], days_count) for o in obj_rows)
            sum_1c  = self._sum_days(hr_row["days"], days_count) if hr_row else 0.0
    
            # ── Строки объектов ────────────────────────────
            first = True
            for o_row in obj_rows:
                fio_cell = main_fio if first else ""
                tbn_cell = main_tbn if first else ""
                s_cell   = icon     if first else ""
    
                vals = [s_cell, fio_cell, tbn_cell, o_row["object_display"], "Объект"]
    
                day_vals = []
                for i in range(days_count):
                    raw_v   = o_row["days"][i] if i < len(o_row["days"]) else None
                    norm_o  = normalize_val(raw_v)
                    display = str(raw_v) if raw_v is not None else ""
    
                    if situation == "both" and hr_row and norm_o:
                        norm_h = normalize_val(
                            hr_row["days"][i] if i < len(hr_row["days"]) else None
                        )
                        if norm_o != norm_h:
                            display = f"≠{display}"
    
                    day_vals.append(display)
    
                vals += day_vals + [
                    _fmt(sum_obj) if first else "",
                    _fmt(sum_1c)  if first else ""
                ]
    
                self.tree_compare.insert("", "end", values=vals, tags=(tag_obj,))
                first = False
    
            # ── Строка 1С ──────────────────────────────────
            if hr_row:
                hr_days_disp = []
                for i in range(days_count):
                    raw_h   = hr_row["days"][i] if i < len(hr_row["days"]) else None
                    norm_h  = normalize_val(raw_h)
                    display = str(raw_h) if raw_h is not None else ""
    
                    if norm_h and situation == "both":
                        found_in_obj = any(
                            normalize_val(o["days"][i] if i < len(o["days"]) else None)
                            for o in obj_rows
                        )
                        if not found_in_obj:
                            display = f"!{display}"
    
                    hr_days_disp.append(display)
    
                tag_1c = "only_1c" if situation == "only_1c" else "hr_row"
                self.tree_compare.insert(
                    "", "end",
                    values=(["", main_fio, main_tbn, "", "1С Кадры"] + hr_days_disp + ["", _fmt(sum_1c)]),
                    tags=(tag_1c,)
                )

    def _rebuild_comparison(self):
        self._merged_groups.clear()

        if not self._obj_rows and not self._hr_rows:
            self.tree_compare.delete(*self.tree_compare.get_children())
            return

        only_diff  = self.var_only_diff.get()

        # Колонок дней (без status/fio/tbn/object/kind/total_obj/total_1c)
        all_cols   = list(self.tree_compare["columns"])
        days_count = len(all_cols) - 7   # 5 фикс. + 2 итога

        hr_map: Dict[str, Dict]       = {
            normalize_tbn(r["tbn"]): r for r in self._hr_rows
        }
        obj_map: Dict[str, List[Dict]] = {}
        for r in self._obj_rows:
            obj_map.setdefault(normalize_tbn(r["tbn"]), []).append(r)

        all_tbns = sorted(set(hr_map) | set(obj_map))

        stat_total = stat_diff = stat_only_obj = stat_only_1c = 0

        for tbn in all_tbns:
            hr_row       = hr_map.get(tbn)
            obj_rows_lst = sorted(
                obj_map.get(tbn, []),
                key=lambda x: x.get("object_display", "")
            )

            has_obj = bool(obj_rows_lst)
            has_1c  = bool(hr_row)

            if obj_rows_lst:
                main_fio = obj_rows_lst[0]["fio"]
                main_tbn = obj_rows_lst[0]["tbn"]
            else:
                main_fio = hr_row["fio"] if hr_row else "???"
                main_tbn = tbn

            # ── Тип ситуации ────────────────────────────────
            if has_obj and not has_1c:
                situation = "only_obj"   # жёлтый
            elif has_1c and not has_obj:
                situation = "only_1c"    # оранжевый
            else:
                situation = "both"

            # ── Анализ расхождений (только когда есть оба) ──
            group_has_diff = False
            if situation == "both":
                hr_days = hr_row["days"]
                for o_row in obj_rows_lst:
                    for i in range(days_count):
                        norm_o = normalize_val(
                            o_row["days"][i] if i < len(o_row["days"]) else None
                        )
                        if not norm_o:
                            continue
                        norm_h = normalize_val(
                            hr_days[i] if i < len(hr_days) else None
                        )
                        if norm_o != norm_h:
                            group_has_diff = True
                            break
                    if group_has_diff:
                        break

                # Есть в 1С, нет ни в одном объекте
                if not group_has_diff:
                    for i in range(days_count):
                        norm_h = normalize_val(
                            hr_row["days"][i]
                            if i < len(hr_row["days"]) else None
                        )
                        if not norm_h:
                            continue
                        found = any(
                            normalize_val(
                                o["days"][i] if i < len(o["days"]) else None
                            )
                            for o in obj_rows_lst
                        )
                        if not found:
                            group_has_diff = True
                            break
                            
            # ── Фильтр ──────────────────────────────────────
            is_problem = (
                situation in ("only_obj", "only_1c") or group_has_diff
            )
            if only_diff and not is_problem:
                continue

            # ── Статистика ──────────────────────────────────
            stat_total += 1
            if situation == "only_obj":
                stat_only_obj += 1
            elif situation == "only_1c":
                stat_only_1c += 1
            elif group_has_diff:
                stat_diff += 1

            self._merged_groups.append({
                "tbn_key":     tbn,
                "display_fio": main_fio,
                "display_tbn": main_tbn,
                "hr_row":      hr_row,
                "obj_rows":    obj_rows_lst,
                "situation":   situation,
                "has_diff":    group_has_diff,
            })

        # Сортировка по ФИО и отрисовка (ОДИН раз после формирования групп)
        self._merged_groups.sort(
            key=lambda g: (
                fio_sort_key(g.get("display_fio")),
                normalize_tbn(g.get("display_tbn")),
            )
        )
        self._render_compare_from_groups()        
        # ── Итог ────────────────────────────────────────────
        self._stat_total    = stat_total
        self._stat_diff     = stat_diff
        self._stat_only_obj = stat_only_obj
        self._stat_only_1c  = stat_only_1c
        self._update_stat_label()

        self.var_status.set(
            f"Сравнение завершено — "
            f"сотрудников: {stat_total} | "
            f"расхождений: {stat_diff} | "
            f"только объект: {stat_only_obj} | "
            f"только 1С: {stat_only_1c}"
        )

    # ──────────────────────────────────────────────────────────
    #  Экспорт в Excel (в потоке)
    # ──────────────────────────────────────────────────────────

    def _start_export_thread(self):
        if not self._merged_groups:
            messagebox.showwarning("Экспорт",
                                   "Нет данных для экспорта.", parent=self)
            return

        fpath = filedialog.asksaveasfilename(
            title="Сохранить сравнение",
            defaultextension=".xlsx",
            initialfile=(
                f"Сравнение_табелей_"
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            ),
            filetypes=[("Excel", "*.xlsx")]
        )
        if not fpath:
            return

        self.btn_export.configure(state="disabled")
        self.var_status.set("Подготовка к экспорту…")
        self.progress_bar["value"] = 0

        threading.Thread(
            target=self._export_process, args=(fpath,), daemon=True
        ).start()

    def _export_process(self, fpath: str):
        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Сравнение"

            all_cols  = list(self.tree_compare["columns"])
            days_cnt  = len(all_cols) - 7

            # ── Заголовок файла ──────────────────────────────
            ws.append([
                f"Сравнение табелей. Экспорт: "
                f"{datetime.now().strftime('%d.%m.%Y %H:%M')}"
            ])
            ws.append([
                f"Итого сотрудников: {self._stat_total} | "
                f"Расхождений: {self._stat_diff} | "
                f"Только объект: {self._stat_only_obj} | "
                f"Только 1С: {self._stat_only_1c}"
            ])
            ws.append([])

            # ── Шапка таблицы ────────────────────────────────
            hdr_row = (
                ["Статус", "ФИО", "Таб.№", "Объект", "Источник"]
                + [str(i) for i in range(1, days_cnt + 1)]
                + ["∑ Объект", "∑ 1С"]
            )
            ws.append(hdr_row)
            hdr_excel_row = ws.max_row

            # Стили
            fill_diff     = PatternFill("solid", fgColor="FF9999")
            fill_hr       = PatternFill("solid", fgColor="EFEFEF")
            fill_only_obj = PatternFill("solid", fgColor="FFF9C4")
            fill_only_1c  = PatternFill("solid", fgColor="FFE0B2")
            fill_cell_diff = PatternFill("solid", fgColor="EF9A9A")
            fill_cell_miss = PatternFill("solid", fgColor="FFCC80")

            font_bold   = Font(bold=True)
            font_header = Font(bold=True, color="FFFFFF")
            fill_header = PatternFill("solid", fgColor="1565C0")

            border_thick = Border(
                top=Side(style="medium"),
                bottom=Side(style="medium")
            )
            border_thin  = Border(
                bottom=Side(style="thin", color="CCCCCC")
            )

            for c in range(1, len(hdr_row) + 1):
                cell = ws.cell(hdr_excel_row, c)
                cell.font   = font_header
                cell.fill   = fill_header
                cell.alignment = Alignment(horizontal="center",
                                           vertical="center")

            total_grp = len(self._merged_groups)
            for idx, grp in enumerate(self._merged_groups):
                pct = int(idx / total_grp * 100) if total_grp else 0
                self.after(
                    0, lambda v=pct: self.progress_bar.configure(value=v)
                )
                self.after(
                    0,
                    lambda i=idx, t=total_grp:
                    self.var_status.set(f"Экспорт: {i+1} / {t}")
                )

                situation = grp.get("situation", "both")
                has_diff  = grp.get("has_diff", False)
                hr_row    = grp.get("hr_row")
                obj_rows  = grp.get("obj_rows", [])
                main_fio  = grp["display_fio"]
                main_tbn  = grp["display_tbn"]

                # Иконка
                if situation == "only_obj":
                    icon = "⚪ Только объект"
                elif situation == "only_1c":
                    icon = "🟠 Только 1С"
                elif has_diff:
                    icon = "🔴 Расхождение"
                else:
                    icon = "🟢 OK"

                # Суммы
                sum_obj = sum(
                    self._sum_days(o["days"], days_cnt)
                    for o in obj_rows
                )
                sum_1c  = (
                    self._sum_days(hr_row["days"], days_cnt)
                    if hr_row else 0.0
                )

                def _fmt(v):
                    return round(v, 2) if v else ""

                start_row = ws.max_row + 1

                # Строки объектов
                first = True
                for o_row in obj_rows:
                    days_raw = (o_row["days"][:days_cnt]
                                + [None] * days_cnt)[:days_cnt]
                    row_data = (
                        [icon if first else "",
                         main_fio if first else "",
                         main_tbn if first else "",
                         o_row["object_display"], "Объект"]
                        + [(v if v is not None else "") for v in days_raw]
                        + [_fmt(sum_obj) if first else "",
                           _fmt(sum_1c)  if first else ""]
                    )
                    ws.append(row_data)
                    cur = ws.max_row

                    # Подсветка расхождений в ячейках
                    if hr_row and situation == "both":
                        for i in range(days_cnt):
                            norm_o = normalize_val(days_raw[i])
                            if not norm_o:
                                continue
                            norm_h = normalize_val(
                                hr_row["days"][i]
                                if i < len(hr_row["days"]) else None
                            )
                            if norm_o != norm_h:
                                c = ws.cell(cur, 6 + i)
                                c.fill = fill_cell_diff
                                c.font = font_bold

                    if situation == "only_obj":
                        for c_idx in range(1, len(row_data) + 1):
                            ws.cell(cur, c_idx).fill = fill_only_obj
                    elif situation == "both" and has_diff:
                        for c_idx in range(1, 6):
                            ws.cell(cur, c_idx).fill = fill_diff

                    for c_idx in range(1, len(row_data) + 1):
                        ws.cell(cur, c_idx).border = border_thin

                    first = False

                # Строка 1С
                if hr_row:
                    hr_days = (hr_row["days"][:days_cnt]
                               + [None] * days_cnt)[:days_cnt]
                    row_data = (
                        ["", main_fio, main_tbn, "", "1С Кадры"]
                        + [(v if v is not None else "") for v in hr_days]
                        + ["", _fmt(sum_1c)]
                    )
                    ws.append(row_data)
                    cur = ws.max_row

                    fill_1c = (fill_only_1c
                               if situation == "only_1c" else fill_hr)
                    for c_idx in range(1, len(row_data) + 1):
                        ws.cell(cur, c_idx).fill   = fill_1c
                        ws.cell(cur, c_idx).border = border_thin

                    # Ячейки: в 1С есть, в объектах нет
                    if situation == "both":
                        for i in range(days_cnt):
                            norm_h = normalize_val(hr_days[i])
                            if not norm_h:
                                continue
                            found = any(
                                normalize_val(
                                    o["days"][i]
                                    if i < len(o["days"]) else None
                                )
                                for o in obj_rows
                            )
                            if not found:
                                ws.cell(cur, 6 + i).fill = fill_cell_miss

                # Рамка блока
                end_row = ws.max_row
                if end_row >= start_row:
                    for c_idx in range(1, len(hdr_row) + 1):
                        t = ws.cell(start_row, c_idx)
                        b = ws.cell(end_row,   c_idx)
                        t.border = Border(
                            top=Side(style="medium"),
                            bottom=t.border.bottom
                        )
                        b.border = Border(
                            top=b.border.top,
                            bottom=Side(style="medium")
                        )
                    # Объединяем ФИО и Таб.№ по блоку
                    if end_row > start_row:
                        for col_n in (1, 2, 3):
                            ws.merge_cells(
                                start_row=start_row, start_column=col_n,
                                end_row=end_row,     end_column=col_n
                            )
                            ws.cell(start_row, col_n).alignment = Alignment(
                                horizontal="left" if col_n == 2 else "center",
                                vertical="top", wrap_text=True
                            )

            # Ширины колонок
            ws.column_dimensions["A"].width = 22
            ws.column_dimensions["B"].width = 34
            ws.column_dimensions["C"].width = 10
            ws.column_dimensions["D"].width = 44
            ws.column_dimensions["E"].width = 12
            for i in range(days_cnt):
                ws.column_dimensions[get_column_letter(6 + i)].width = 5
            ws.column_dimensions[get_column_letter(6 + days_cnt)].width = 8
            ws.column_dimensions[get_column_letter(7 + days_cnt)].width = 8

            ws.freeze_panes = f"A{hdr_excel_row + 1}"

            wb.save(fpath)
            self.after(0, lambda: self._export_finished(True, fpath))

        except Exception as e:
            logging.exception("Export Error")
            self.after(0, lambda: self._export_finished(False, str(e)))

    def _export_finished(self, success: bool, msg: str):
        self.btn_export.configure(state="normal")
        self.progress_bar["value"] = 100 if success else 0
        if success:
            self.var_status.set("✅ Экспорт завершён успешно")
            messagebox.showinfo("Готово",
                                f"Файл сохранён:\n{msg}", parent=self)
        else:
            self.var_status.set("❌ Ошибка экспорта")
            messagebox.showerror("Ошибка",
                                 f"Не удалось сохранить файл:\n{msg}",
                                 parent=self)


# ── API ───────────────────────────────────────────────────────
def create_timesheet_compare_page(parent, app_ref) -> TimesheetComparePage:
    return TimesheetComparePage(parent, app_ref=app_ref)
