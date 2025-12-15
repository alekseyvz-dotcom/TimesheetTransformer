# timesheet_compare.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from openpyxl import load_workbook
import timesheet_transformer

# Эти функции и типы берём из модуля табеля
from timesheet_module import (
    load_all_timesheet_headers,
    load_timesheet_rows_by_header_id,
    month_name_ru,
    month_days,
    set_db_pool as _set_db_pool_from_timesheet,  # чтобы разделить API
)


def set_db_pool(pool):
    """
    Прокидываем пул соединений в timesheet_module (там уже есть логика работы с БД).
    Сам модуль сравнения напрямую к БД не лезет.
    """
    _set_db_pool_from_timesheet(pool)


class TimesheetComparePage(tk.Frame):
    """
    Раздел: сравнение объектного табеля с кадровым табелем (после конвертера 1С).
    """

    def __init__(self, master, app_ref):
        super().__init__(master)
        self.app_ref = app_ref

        self._headers: List[Dict[str, Any]] = []   # заголовки табелей из БД
        self._obj_rows: List[Dict[str, Any]] = []  # строки объектного табеля
        self._hr_rows: List[Dict[str, Any]] = []   # строки кадрового табеля
        self._merged_rows: List[Dict[str, Any]] = []

        self._build_ui()
        self._load_headers()

    # ---------- UI ----------

    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=(8, 4))

        tk.Label(top, text="Сравнение табелей (Объектный vs Кадровый 1С)",
                 font=("Segoe UI", 12, "bold")).grid(
            row=0, column=0, columnspan=6, sticky="w", pady=(0, 8)
        )

        row_f = 1

        # Фильтры для поиска табелей
        tk.Label(top, text="Год:").grid(row=row_f, column=0, sticky="e", padx=(0, 4))
        self.var_year = tk.StringVar()
        from datetime import datetime

        spn_year = tk.Spinbox(top, from_=2000, to=2100, width=6, textvariable=self.var_year)
        spn_year.grid(row=row_f, column=1, sticky="w")
        self.var_year.set(str(datetime.now().year))

        tk.Label(top, text="Месяц:").grid(row=row_f, column=2, sticky="e", padx=(12, 4))
        self.var_month = tk.StringVar()
        cmb_month = ttk.Combobox(
            top,
            state="readonly",
            width=12,
            textvariable=self.var_month,
            values=["Все"] + [month_name_ru(i) for i in range(1, 13)],
        )
        cmb_month.grid(row=row_f, column=3, sticky="w")
        self.var_month.set("Все")

        row_f += 1

        tk.Label(top, text="Подразделение:").grid(row=row_f, column=0, sticky="e", padx=(0, 4), pady=(4, 0))
        self.var_dep = tk.StringVar()
        ent_dep = ttk.Entry(top, width=24, textvariable=self.var_dep)
        ent_dep.grid(row=row_f, column=1, sticky="w", pady=(4, 0))

        tk.Label(top, text="Адрес/объект:").grid(row=row_f, column=2, sticky="e", padx=(12, 4), pady=(4, 0))
        self.var_addr = tk.StringVar()
        ent_addr = ttk.Entry(top, width=34, textvariable=self.var_addr)
        ent_addr.grid(row=row_f, column=3, sticky="w", pady=(4, 0))

        btns = tk.Frame(top)
        btns.grid(row=row_f + 1, column=0, columnspan=6, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Обновить список табелей", command=self._load_headers)\
            .pack(side="left", padx=(0, 4))
        ttk.Button(btns, text="Сбросить фильтры", command=self._reset_filters)\
            .pack(side="left", padx=4)
        ttk.Button(btns, text="Загрузить кадровый табель 1С…", command=self._load_hr_file)\
            .pack(side="left", padx=12)

        # ---- Таблица заголовков объектных табелей ----
        headers_frame = tk.LabelFrame(self, text="Табели подразделений (объектные)")
        headers_frame.pack(fill="x", padx=8, pady=(4, 4))

        cols = ("year", "month", "object", "department", "user")
        self.tree_headers = ttk.Treeview(headers_frame, columns=cols, show="headings",
                                         height=6, selectmode="browse")

        self.tree_headers.heading("year", text="Год")
        self.tree_headers.heading("month", text="Месяц")
        self.tree_headers.heading("object", text="Объект")
        self.tree_headers.heading("department", text="Подразделение")
        self.tree_headers.heading("user", text="Пользователь")

        self.tree_headers.column("year", width=60, anchor="center")
        self.tree_headers.column("month", width=90, anchor="center")
        self.tree_headers.column("object", width=260, anchor="w")
        self.tree_headers.column("department", width=180, anchor="w")
        self.tree_headers.column("user", width=150, anchor="w")

        vsb_h = ttk.Scrollbar(headers_frame, orient="vertical", command=self.tree_headers.yview)
        self.tree_headers.configure(yscrollcommand=vsb_h.set)

        self.tree_headers.pack(side="left", fill="x", expand=True)
        vsb_h.pack(side="right", fill="y")

        self.tree_headers.bind("<Double-1>", lambda e: self._on_select_header())
        self.tree_headers.bind("<Return>", lambda e: self._on_select_header())

        ttk.Button(headers_frame, text="Выбрать табель и сравнить",
                   command=self._on_select_header)\
            .pack(fill="x", padx=4, pady=4)

        # ---- Нижняя часть: таблица сравнений ----
        compare_frame = tk.LabelFrame(self, text="Сравнение по сотрудникам")
        compare_frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        self.tree_compare = ttk.Treeview(compare_frame, show="headings",
                                         selectmode="browse")
        self.tree_compare.pack(side="left", fill="both", expand=True)
        vsb_c = ttk.Scrollbar(compare_frame, orient="vertical", command=self.tree_compare.yview)
        self.tree_compare.configure(yscrollcommand=vsb_c.set)
        vsb_c.pack(side="right", fill="y")

        self._configure_compare_columns(days_in_month=31)

    # ---------- Загрузка заголовков из БД ----------

    def _reset_filters(self):
        from datetime import datetime
        self.var_year.set(str(datetime.now().year))
        self.var_month.set("Все")
        self.var_dep.set("")
        self.var_addr.set("")
        self._load_headers()

    def _load_headers(self):
        self.tree_headers.delete(*self.tree_headers.get_children())
        self._headers.clear()

        year = None
        try:
            y = int(self.var_year.get().strip())
            if 2000 <= y <= 2100:
                year = y
        except Exception:
            pass

        month = None
        m_name = (self.var_month.get() or "").strip()
        if m_name and m_name != "Все":
            for i in range(1, 13):
                if month_name_ru(i) == m_name:
                    month = i
                    break

        dep = (self.var_dep.get() or "").strip() or None
        addr_sub = (self.var_addr.get() or "").strip() or None

        try:
            headers = load_all_timesheet_headers(
                year=year,
                month=month,
                department=dep,
                object_addr_substr=addr_sub,
                object_id_substr=None,
            )
        except Exception as e:
            import logging
            logging.exception("Ошибка загрузки заголовков табелей для сравнения")
            messagebox.showerror("Сравнение табелей",
                                 f"Ошибка загрузки списка табелей:\n{e}",
                                 parent=self)
            return

        self._headers = headers

        for h in headers:
            y = h["year"]
            m = h["month"]
            addr = h["object_addr"] or ""
            obj_id = h.get("object_id") or ""
            dep = h.get("department") or ""
            user = h.get("full_name") or h.get("username") or ""

            m_ru = month_name_ru(m) if 1 <= m <= 12 else str(m)
            obj_display = addr
            if obj_id:
                obj_display = f"[{obj_id}] {addr}"

            iid = str(h["id"])
            self.tree_headers.insert(
                "",
                "end",
                iid=iid,
                values=(y, m_ru, obj_display, dep, user),
            )

    def _get_selected_header(self) -> Optional[Dict[str, Any]]:
        sel = self.tree_headers.selection()
        if not sel:
            return None
        iid = sel[0]
        try:
            hid = int(iid)
        except Exception:
            return None
        for h in self._headers:
            if int(h["id"]) == hid:
                return h
        return None

    # ---------- Кадровый табель (файл после конвертера 1С) ----------

    def _load_hr_file(self):
        """Выбор исходного файла 1С, конвертация через timesheet_transformer и загрузка результата."""
        path = filedialog.askopenfilename(
            parent=self,
            title="Выберите исходный табель 1С (xlsx/xlsm)",
            filetypes=[("Excel", "*.xlsx *.xlsm"), ("Все файлы", "*.*")],
        )
        if not path:
            return

        try:
            # 1) Вычислим путь для временного результата рядом с исходником
            from pathlib import Path
            src = Path(path)
            # имя как в timesheet_transformer: <stem>_result.xlsx
            out_path = src.with_name(src.stem + "_result_for_compare.xlsx")

            # 2) Запускаем конвертер в "встроенном" режиме.
            # transform_file(file_path, out_path=None, parent=None)
            # Твоя функция в конце показывает msg_info, это нормально.
            timesheet_transformer.transform_file(str(src), str(out_path), parent=self)

            # 3) Теперь out_path должен существовать; читаем его как и раньше
            if not out_path.exists():
                messagebox.showerror("Сравнение табелей",
                                     f"Не найден результат конвертации:\n{out_path}",
                                     parent=self)
                return

            wb = load_workbook(str(out_path), data_only=True)
            ws = wb["Результат"] if "Результат" in wb.sheetnames else wb.active

            rows: List[Dict[str, Any]] = []
            for r in range(2, ws.max_row + 1):
                fio = (ws.cell(r, 2).value or "").strip()
                if not fio:
                    continue
                tbn = str(ws.cell(r, 4).value or "").strip()
                days_vals: List[Optional[str]] = []
                for c in range(6, 6 + 31):
                    v = ws.cell(r, c).value
                    if v is None or v == "":
                        days_vals.append(None)
                    else:
                        days_vals.append(str(v).strip())
                rows.append({"fio": fio, "tbn": tbn, "days": days_vals})

            self._hr_rows = rows
            messagebox.showinfo(
                "Сравнение табелей",
                f"Исходный табель 1С сконвертирован и загружен.\n"
                f"Строк: {len(rows)}\nФайл результата: {out_path.name}",
                parent=self,
            )
            self._rebuild_comparison()

        except Exception as e:
            import logging, traceback
            logging.exception("Ошибка конвертации/чтения кадрового табеля")
            messagebox.showerror(
                "Сравнение табелей",
                f"Ошибка конвертации или чтения табеля 1С:\n{e}",
                parent=self,
            )

    # ---------- Выбор объектного табеля и сбор данных ----------

    def _on_select_header(self):
        h = self._get_selected_header()
        if not h:
            messagebox.showwarning("Сравнение табелей",
                                   "Выберите табель из списка.",
                                   parent=self)
            return

        header_id = int(h["id"])
        try:
            rows = load_timesheet_rows_by_header_id(header_id)
        except Exception as e:
            import logging
            logging.exception("Ошибка загрузки строк табеля для сравнения")
            messagebox.showerror("Сравнение табелей",
                                 f"Ошибка загрузки строк табеля:\n{e}",
                                 parent=self)
            return

        obj_rows: List[Dict[str, Any]] = []
        for r in rows:
            fio = (r["fio"] or "").strip()
            tbn = (r["tbn"] or "").strip()
            hours_raw = r.get("hours_raw") or [None] * 31
            days: List[Optional[str]] = []
            for v in hours_raw[:31]:
                if v is None or v == "":
                    days.append(None)
                else:
                    days.append(str(v).strip())
            obj_rows.append({"fio": fio, "tbn": tbn, "days": days})

        self._obj_rows = obj_rows

        y, m = h["year"], h["month"]
        days_in_m = month_days(y, m)
        self._configure_compare_columns(days_in_month=days_in_m)

        self._rebuild_comparison()

    # ---------- Объединение и подсветка ----------

    def _configure_compare_columns(self, days_in_month: int):
        cols = ["fio", "tbn", "kind"] + [f"d{i}" for i in range(1, days_in_month + 1)]
        self.tree_compare["columns"] = cols

        self.tree_compare.heading("fio", text="ФИО")
        self.tree_compare.heading("tbn", text="Таб.№")
        self.tree_compare.heading("kind", text="Источник")

        self.tree_compare.column("fio", width=240, anchor="w")
        self.tree_compare.column("tbn", width=80, anchor="center")
        self.tree_compare.column("kind", width=120, anchor="center")

        for i in range(1, days_in_month + 1):
            col_id = f"d{i}"
            self.tree_compare.heading(col_id, text=str(i))
            self.tree_compare.column(col_id, width=36, anchor="center")

    def _rebuild_comparison(self):
        self.tree_compare.delete(*self.tree_compare.get_children())
        self._merged_rows.clear()

        if not self._obj_rows or not self._hr_rows:
            return

        # индекс кадрового табеля по (fio.lower, tbn)
        hr_index: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for r in self._hr_rows:
            key = (r["fio"].strip().lower(), r["tbn"].strip())
            hr_index[key] = r

        used_hr_keys = set()

        # базовый проход: все из объектного
        for o in self._obj_rows:
            key = (o["fio"].strip().lower(), o["tbn"].strip())
            hr = hr_index.get(key)
            if hr:
                used_hr_keys.add(key)

            self._merged_rows.append({
                "fio": o["fio"],
                "tbn": o["tbn"],
                "kind": "Объектный табель",
                "days": o["days"],
                "pair_key": key,
            })
            self._merged_rows.append({
                "fio": hr["fio"] if hr else o["fio"],
                "tbn": hr["tbn"] if hr else o["tbn"],
                "kind": "Кадровый табель",
                "days": hr["days"] if hr else [None] * 31,
                "pair_key": key,
            })

        # те, кто есть только в кадровом
        for key, hr in hr_index.items():
            if key in used_hr_keys:
                continue
            self._merged_rows.append({
                "fio": hr["fio"],
                "tbn": hr["tbn"],
                "kind": "Объектный табель",
                "days": [None] * 31,
                "pair_key": key,
            })
            self._merged_rows.append({
                "fio": hr["fio"],
                "tbn": hr["tbn"],
                "kind": "Кадровый табель",
                "days": hr["days"],
                "pair_key": key,
            })

        # сортировка: ФИО, таб.№, источник
        self._merged_rows.sort(
            key=lambda r: (r["fio"].lower(), r["tbn"], 0 if r["kind"] == "Объектный табель" else 1)
        )

        days_in_m = len(self.tree_compare["columns"]) - 3
        for row in self._merged_rows:
            vals = [row["fio"], row["tbn"], row["kind"]]
            for i in range(days_in_m):
                v = row["days"][i] if i < len(row["days"]) and row["days"][i] is not None else ""
                vals.append(v)
            self.tree_compare.insert("", "end", values=vals)

        self._highlight_differences(days_in_m)

    def _highlight_differences(self, days_in_month: int):
        # Подсвечиваем пары строк, где есть расхождения по дням
        items = list(self.tree_compare.get_children())
        mismatch_tag = "mismatch"
        self.tree_compare.tag_configure(mismatch_tag, background="#fff2cc")  # жёлтый фон

        for i in range(0, len(items), 2):
            if i + 1 >= len(items):
                break
            iid_obj = items[i]
            iid_hr = items[i + 1]

            v_obj = self.tree_compare.item(iid_obj, "values")
            v_hr = self.tree_compare.item(iid_hr, "values")

            mismatch = False
            for idx in range(3, 3 + days_in_month):
                vo = v_obj[idx] if idx < len(v_obj) else ""
                vh = v_hr[idx] if idx < len(v_hr) else ""
                if (vo == "" and vh == "") or (vo == vh):
                    continue
                mismatch = True
                break

            if mismatch:
                self.tree_compare.item(iid_obj, tags=(mismatch_tag,))
                self.tree_compare.item(iid_hr, tags=(mismatch_tag,))


# ---- API для main_app ----

def create_timesheet_compare_page(parent, app_ref) -> TimesheetComparePage:
    return TimesheetComparePage(parent, app_ref=app_ref)
