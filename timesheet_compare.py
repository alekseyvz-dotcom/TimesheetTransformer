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
        self._agg_headers: List[Dict[str, Any]] = []

        self._build_ui()
        self._load_headers()

    # ---------- UI ----------

    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=(8, 4))

        tk.Label(
            top,
            text="Сравнение табеля объекта с кадровым табелем (1С)",
            font=("Segoe UI", 12, "bold"),
        ).grid(row=0, column=0, columnspan=6, sticky="w", pady=(0, 8))

        row_f = 1

        # Год / месяц
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

        # Подразделение — выпадающий список
        tk.Label(top, text="Подразделение:").grid(
            row=row_f, column=0, sticky="e", padx=(0, 4), pady=(4, 0)
        )
        self.var_dep = tk.StringVar()
        self.cmb_dep = ttk.Combobox(
            top,
            state="readonly",
            width=40,
            textvariable=self.var_dep,
            values=["Все"],  # заполним реальными значениями после загрузки заголовков
        )
        self.cmb_dep.grid(row=row_f, column=1, columnspan=3, sticky="w", pady=(4, 0))
        self.var_dep.set("Все")

        btns = tk.Frame(top)
        btns.grid(row=row_f + 1, column=0, columnspan=6, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Обновить список табелей", command=self._load_headers).pack(
            side="left", padx=(0, 4)
        )
        ttk.Button(btns, text="Сбросить фильтры", command=self._reset_filters).pack(
            side="left", padx=4
        )
        ttk.Button(
            btns,
            text="Загрузить исходный табель 1С…",
            command=self._load_hr_from_1c,
        ).pack(side="left", padx=12)
        # ---- Таблица заголовков объектных табелей ----
        headers_frame = tk.LabelFrame(self, text="Объектные табели подразделений")
        headers_frame.pack(fill="x", padx=8, pady=(4, 4))

        cols = ("year", "month", "department")
        self.tree_headers = ttk.Treeview(
            headers_frame,
            columns=cols,
            show="headings",
            height=6,
            selectmode="browse",
        )

        self.tree_headers.heading("year", text="Год")
        self.tree_headers.heading("month", text="Месяц")
        self.tree_headers.heading("department", text="Подразделение")

        self.tree_headers.column("year", width=80, anchor="center")
        self.tree_headers.column("month", width=120, anchor="center")
        self.tree_headers.column("department", width=260, anchor="w")

        vsb_h = ttk.Scrollbar(headers_frame, orient="vertical", command=self.tree_headers.yview)
        self.tree_headers.configure(yscrollcommand=vsb_h.set)

        self.tree_headers.pack(side="left", fill="x", expand=True)
        vsb_h.pack(side="right", fill="y")

        self.tree_headers.bind("<Double-1>", lambda e: self._on_select_header())
        self.tree_headers.bind("<Return>", lambda e: self._on_select_header())

        ttk.Button(
            headers_frame,
            text="Выбрать табель и сравнить",
            command=self._on_select_header,
        ).pack(fill="x", padx=4, pady=4)
        ttk.Button(
            headers_frame,
            text="Выгрузить свод в Excel…",
            command=self._export_to_excel,
        ).pack(fill="x", padx=4, pady=(0, 4))

        # ---- Нижняя часть: таблица сравнений ----
        compare_frame = tk.LabelFrame(self, text="Сравнение по сотрудникам")
        compare_frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        self.tree_compare = ttk.Treeview(
            compare_frame,
            show="headings",
            selectmode="browse",
        )
        self.tree_compare.pack(side="left", fill="both", expand=True)

        vsb_c = ttk.Scrollbar(compare_frame, orient="vertical", command=self.tree_compare.yview)
        self.tree_compare.configure(yscrollcommand=vsb_c.set)
        vsb_c.pack(side="right", fill="y")

        # первоначальная конфигурация колонок
        self._configure_compare_columns(days_in_month=31)
        # Теги для чередования фона по парам строк
        self.tree_compare.tag_configure("pair_even", background="#f5f5f5")
        self.tree_compare.tag_configure("pair_odd", background="#e0e0e0")

    # ---------- Загрузка заголовков из БД ----------

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

        dep = (self.var_dep.get() or "").strip()
        if dep == "" or dep == "Все":
            dep_filter = None
        else:
            dep_filter = dep

        # Адрес здесь больше не фильтруем
        addr_sub = None

        try:
            headers = load_all_timesheet_headers(
                year=year,
                month=month,
                department=dep_filter,
                object_addr_substr=addr_sub,
                object_id_substr=None,
            )
        except Exception as e:
            import logging

            logging.exception("Ошибка загрузки заголовков табелей для сравнения")
            messagebox.showerror(
                "Сравнение табелей",
                f"Ошибка загрузки списка табелей:\n{e}",
                parent=self,
            )
            return

        self._headers = headers

        # Заполняем комбобокс подразделений реальными значениями
        self._fill_departments_combo(headers)

        # Агрегируем заголовки по (year, month, department)
        agg_map: Dict[Tuple[int, int, str], Dict[str, Any]] = {}
        for h in headers:
            y = int(h["year"])
            m = int(h["month"])
            dep_val = (h.get("department") or "").strip()
            key = (y, m, dep_val)
            if key not in agg_map:
                agg_map[key] = {
                    "year": y,
                    "month": m,
                    "department": dep_val,
                    "headers": [],  # список исходных заголовков timesheet_headers
                }
            agg_map[key]["headers"].append(h)

        self._agg_headers = list(agg_map.values())

        # Заполняем таблицу агрегированными строками
        for agg in sorted(self._agg_headers, key=lambda a: (a["year"], a["month"], a["department"]), reverse=True):
            y = agg["year"]
            m = agg["month"]
            dep_val = agg["department"]
            m_ru = month_name_ru(m) if 1 <= m <= 12 else str(m)

            # iid делаем строкой "year:month:dep", чтобы потом по ней искать
            iid = f"{y}:{m}:{dep_val}"
            self.tree_headers.insert(
                "",
                "end",
                iid=iid,
                values=(y, m_ru, dep_val),
            )

    def _get_selected_agg(self) -> Optional[Dict[str, Any]]:
        sel = self.tree_headers.selection()
        if not sel:
            return None
        iid = sel[0]  # формат "year:month:department"
        try:
            y_str, m_str, dep_val = iid.split(":", 2)
            y = int(y_str)
            m = int(m_str)
        except Exception:
            return None

        for agg in self._agg_headers:
            if agg["year"] == y and agg["month"] == m and agg["department"] == dep_val:
                return agg
        return None

    # ---------- Кадровый табель (файл после конвертера 1С) ----------

    def _load_hr_from_1c(self):
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
        agg = self._get_selected_agg()
        if not agg:
            messagebox.showwarning(
                "Сравнение табелей",
                "Выберите строку (год/месяц/подразделение).",
                parent=self,
            )
            return

        headers = agg.get("headers") or []
        if not headers:
            messagebox.showwarning(
                "Сравнение табелей",
                "Не найдены табели для выбранного подразделения.",
                parent=self,
            )
            return

        obj_rows: List[Dict[str, Any]] = []

        try:
            for h in headers:
                header_id = int(h["id"])
                rows = load_timesheet_rows_by_header_id(header_id)
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
        except Exception as e:
            import logging
            logging.exception("Ошибка загрузки строк табелей для сравнения")
            messagebox.showerror(
                "Сравнение табелей",
                f"Ошибка загрузки строк табелей:\n{e}",
                parent=self,
            )
            return

        self._obj_rows = obj_rows

        y, m = agg["year"], agg["month"]
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

        # индекс кадрового табеля по табельному номеру
        hr_index: Dict[str, Dict[str, Any]] = {}
        for r in self._hr_rows:
            tbn_key = r["tbn"].strip()
            if not tbn_key:
                continue  # без таб.№ не можем сопоставить
            hr_index[tbn_key] = r

        used_hr_keys = set()

        # базовый проход: все из объектного (ключ — табельный номер)
        for o in self._obj_rows:
            tbn_key = o["tbn"].strip()
            hr = hr_index.get(tbn_key) if tbn_key else None
            if hr and tbn_key:
                used_hr_keys.add(tbn_key)

            self._merged_rows.append({
                "fio": o["fio"],
                "tbn": o["tbn"],
                "kind": "Объектный табель",
                "days": o["days"],
                "pair_key": tbn_key,
            })
            self._merged_rows.append({
                "fio": hr["fio"] if hr else o["fio"],
                "tbn": hr["tbn"] if hr else o["tbn"],
                "kind": "Кадровый табель",
                "days": hr["days"] if hr else [None] * 31,
                "pair_key": tbn_key,
            })

        # те, кто есть только в кадровом (по табельному номеру)
        for tbn_key, hr in hr_index.items():
            if tbn_key in used_hr_keys:
                continue
            self._merged_rows.append({
                "fio": hr["fio"],
                "tbn": hr["tbn"],
                "kind": "Объектный табель",
                "days": [None] * 31,
                "pair_key": tbn_key,
            })
            self._merged_rows.append({
                "fio": hr["fio"],
                "tbn": hr["tbn"],
                "kind": "Кадровый табель",
                "days": hr["days"],
                "pair_key": tbn_key,
            })

        # сортировка: ФИО, таб.№, источник
        self._merged_rows.sort(
            key=lambda r: (r["tbn"], r["fio"].lower(), 0 if r["kind"] == "Объектный табель" else 1)
        )

        days_in_m = len(self.tree_compare["columns"]) - 3

        # Чередуем фон по парам строк (Объектный / Кадровый)
        pair_index = 0  # 0,1,0,1,...
        for i, row in enumerate(self._merged_rows):
            vals = [row["fio"], row["tbn"], row["kind"]]
            for d in range(days_in_m):
                v = row["days"][d] if d < len(row["days"]) and row["days"][d] is not None else ""
                vals.append(v)

            tag = "pair_even" if pair_index % 2 == 0 else "pair_odd"
            self.tree_compare.insert("", "end", values=vals, tags=(tag,))

            # после каждой второй строки (пары) переключаем цвет
            if i % 2 == 1:
                pair_index += 1

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
    def _fill_departments_combo(self, headers: List[Dict[str, Any]]):
        """Заполнить выпадающий список подразделений по загруженным заголовкам."""
        deps_set = set()
        for h in headers:
            d = (h.get("department") or "").strip()
            if d:
                deps_set.add(d)
        deps_list = sorted(deps_set)
        values = ["Все"] + deps_list
        try:
            self.cmb_dep.configure(values=values)
        except Exception:
            return
        # если выбранное подразделение больше не существует в фильтре — сбрасываем на "Все"
        cur = (self.var_dep.get() or "").strip()
        if cur not in values:
            self.var_dep.set("Все")
            self.cmb_dep.set("Все")

    def _export_to_excel(self):
        """Выгрузить текущий свод сравнения в Excel с подсветкой отличий по дням."""
        if not self._merged_rows:
            messagebox.showinfo(
                "Экспорт свода",
                "Нет данных для экспорта. Сначала выберите объектный табель и загрузите кадровый.",
                parent=self,
            )
            return

        from tkinter import filedialog
        from openpyxl import Workbook
        from openpyxl.styles import PatternFill, Alignment
        from openpyxl.utils import get_column_letter

        # Выбор файла
        default_name = "Свод_сравнения_табелей.xlsx"
        path = filedialog.asksaveasfilename(
            parent=self,
            title="Сохранить свод сравнения в Excel",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel файлы", "*.xlsx"), ("Все файлы", "*.*")],
        )
        if not path:
            return

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Свод сравнения"

            days_in_m = len(self.tree_compare["columns"]) - 3

            # Заголовок
            header = ["ФИО", "Таб.№", "Источник"] + [str(i) for i in range(1, days_in_m + 1)]
            ws.append(header)

            # Выравнивание по центру для дневных ячеек
            center = Alignment(horizontal="center", vertical="center", wrap_text=True)

            # Подсветка отличий
            diff_fill = PatternFill(fill_type="solid", fgColor="FFF2CC")  # светло-жёлтый

            items = self._merged_rows
            row_excel = 2  # первая строка с данными

            i = 0
            while i < len(items):
                row_obj = items[i]
                vals_obj = [row_obj["fio"], row_obj["tbn"], row_obj["kind"]]
                for d in range(days_in_m):
                    v = row_obj["days"][d] if d < len(row_obj["days"]) and row_obj["days"][d] is not None else ""
                    vals_obj.append(v)
                ws.append(vals_obj)

                row_hr = items[i + 1] if i + 1 < len(items) else None
                if row_hr:
                    vals_hr = [row_hr["fio"], row_hr["tbn"], row_hr["kind"]]
                    for d in range(days_in_m):
                        v = row_hr["days"][d] if d < len(row_hr["days"]) and row_hr["days"][d] is not None else ""
                        vals_hr.append(v)
                    ws.append(vals_hr)

                    # Подсветка отличий по дням
                    for d in range(1, days_in_m + 1):
                        col_idx = 3 + d  # A=1, B=2, C=3, дни начинаются с 4-го столбца
                        c_obj = ws.cell(row=row_excel, column=col_idx)
                        c_hr = ws.cell(row=row_excel + 1, column=col_idx)

                        vo = (c_obj.value or "")
                        vh = (c_hr.value or "")
                        if (vo == "" and vh == "") or (str(vo) == str(vh)):
                            continue

                        c_obj.fill = diff_fill
                        c_hr.fill = diff_fill

                    row_excel += 2
                    i += 2
                else:
                    # нет пары — только объектный табель
                    row_excel += 1
                    i += 1

            # Ширина колонок
            ws.column_dimensions["A"].width = 32  # ФИО
            ws.column_dimensions["B"].width = 10  # Таб.№
            ws.column_dimensions["C"].width = 16  # Источник
            for d in range(1, days_in_m + 1):
                col_letter = get_column_letter(3 + d)
                ws.column_dimensions[col_letter].width = 5.5

            # Выравнивание по центру для всех дневных ячеек
            max_row = ws.max_row
            for r in range(2, max_row + 1):
                for c in range(4, 4 + days_in_m):
                    ws.cell(r, c).alignment = center

            wb.save(path)
            messagebox.showinfo(
                "Экспорт свода",
                f"Свод сравнения сохранён в файл:\n{path}",
                parent=self,
            )

        except Exception as e:
            import logging
            logging.exception("Ошибка экспорта свода сравнения в Excel")
            messagebox.showerror("Экспорт свода", f"Ошибка при сохранении файла:\n{e}", parent=self)

# ---- API для main_app ----

def create_timesheet_compare_page(parent, app_ref) -> TimesheetComparePage:
    return TimesheetComparePage(parent, app_ref=app_ref)
