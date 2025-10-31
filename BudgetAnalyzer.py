# BudgetAnalyzer.py
# Анализ смет: поддержка смет Smeta.RU (лист «ЛОКАЛЬНАЯ СМЕТА», 11 колонок).
# Логика (Smeta.RU режим):
# - Ищем лист с надписью «ЛОКАЛЬНАЯ СМЕТА».
# - Находим строку заголовков: либо явные заголовки, либо строка нумерации 1..11.
# - Колонка наименований — 3-я (0-based индекс 2). Колонки «ВСЕГО» — приоритетно 11-я, затем 10-я.
# - Итоги ищем по всей строке (без остановки на первом): берём последний «Итого по смете», иначе последний «Итого по локальной смете».
# - Категории:
#     • Заработная плата: строки 3-й колонки с «ЗП», «в т.ч. ЗПМ», «оплата труда», «заработ…».
#     • Материалы: ТОЛЬКО по inline-правилу — если строка позиции (в 1-й колонке номер) с наименованием (3-я колонка)
#       имеет ненулевую стоимость в 11/10 кол., и при этом ед. изм. не проценты (%) и не трудочасы (чел-ч).
#       Поиск ключевых слов «МР/материалы/мат.» отключён.
#     • Прочие = Итог − Материалы − Заработная плата.
# - Из данных исключаем:
#     • строку нумерации столбцов (1..11) после заголовков и, если встречается дальше;
#     • все сводные/итоговые/НДС/справочные блоки (распознаём по текстам и условию: первые 2 колонки пусты, 3-я содержит текст).
# - Общий режим (generic) для простых CSV/XLSX с ручным сопоставлением колонок — без специальной логики Smeta.RU.

import re
import csv
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from openpyxl import Workbook, load_workbook

# matplotlib (опционально). Если не установлена — рисуем диаграмму на Tk Canvas
try:
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    import matplotlib.pyplot as plt
    MPL_AVAILABLE = True
except Exception:
    MPL_AVAILABLE = False


# ------------------------- Диалог сопоставления колонок (общий режим) -------------------------

class ColumnMappingDialog(simpledialog.Dialog):
    def __init__(self, parent, headers: List[str], cur_map: Dict[str, Optional[int]]):
        self.headers = headers
        self.cur_map = cur_map or {}
        self.result = None
        super().__init__(parent, title="Настройка соответствия колонок")

    def body(self, master):
        tk.Label(master, text="Укажите, какие колонки соответствуют показателям:",
                 font=("Segoe UI", 10, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(4, 8))

        tk.Label(master, text="Итого (строительные затраты):").grid(row=1, column=0, sticky="e", padx=(0, 6))
        tk.Label(master, text="Материалы:").grid(row=2, column=0, sticky="e", padx=(0, 6))
        tk.Label(master, text="Заработная плата:").grid(row=3, column=0, sticky="e", padx=(0, 6))

        vals = self.headers or ["— нет колонок —"]
        self.cmb_total = ttk.Combobox(master, values=vals, state="readonly", width=46)
        self.cmb_materials = ttk.Combobox(master, values=vals, state="readonly", width=46)
        self.cmb_wages = ttk.Combobox(master, values=vals, state="readonly", width=46)

        def set_by_index(cmb, idx):
            if idx is not None and 0 <= idx < len(vals):
                cmb.current(idx)
            else:
                cmb.set("")

        set_by_index(self.cmb_total, self.cur_map.get("total"))
        set_by_index(self.cmb_materials, self.cur_map.get("materials"))
        set_by_index(self.cmb_wages, self.cur_map.get("wages"))

        self.cmb_total.grid(row=1, column=1, sticky="w")
        self.cmb_materials.grid(row=2, column=1, sticky="w")
        self.cmb_wages.grid(row=3, column=1, sticky="w")
        return self.cmb_total

    def apply(self):
        def idx_of(cmb):
            v = cmb.get().strip()
            try:
                return self.headers.index(v)
            except Exception:
                return None
        self.result = {
            "total": idx_of(self.cmb_total),
            "materials": idx_of(self.cmb_materials),
            "wages": idx_of(self.cmb_wages),
        }


# ------------------------- Страница Анализ смет -------------------------

class BudgetAnalysisPage(tk.Frame):
    COLORS = {
        "materials": "#42a5f5",  # blue
        "wages":     "#66bb6a",  # green
        "other":     "#ffa726",  # orange
    }

    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        self.file_path: Optional[Path] = None

        # Общий режим (generic)
        self.headers: List[str] = []
        self.rows: List[List[Any]] = []
        self.mapping: Dict[str, Optional[int]] = {"total": None, "materials": None, "wages": None}

        # Smeta-режим (специальный парсер Smeta.RU)
        self.mode: str = "generic"  # "smeta" | "generic"
        self.smeta_sheet_name: Optional[str] = None
        self.smeta_name_col: Optional[int] = None      # индекс колонки "Наименование работ и затрат" (0-based, обычно 2)
        self.smeta_cost_cols: List[int] = []           # индексы колонок «ВСЕГО » (приоритет: текущий уровень → базис)
        self.smeta_data_rows: List[List[Any]] = []     # строки данных (после шапки, с пропусками сводных блоков)

        # Итоги и расшифровка
        self.stats = {"total": 0.0, "materials": 0.0, "wages": 0.0, "other": 0.0}
        self.breakdown_rows: List[Dict[str, Any]] = []  # {"category": str, "name": str, "amount": float}

        # UI: заголовок
        header = tk.Frame(self, bg="#f7f7f7")
        header.pack(fill="x", padx=12, pady=(10, 6))
        tk.Label(header, text="Анализ смет", font=("Segoe UI", 16, "bold"), bg="#f7f7f7").pack(side="left")

        # Панель кнопок
        ctrl = tk.Frame(self, bg="#f7f7f7")
        ctrl.pack(fill="x", padx=12, pady=(0, 8))
        self.btn_open = ttk.Button(ctrl, text="Открыть смету (XLSX/CSV)", command=self._open_file)
        self.btn_open.pack(side="left")
        self.btn_map = ttk.Button(ctrl, text="Настроить соответствие колонок", command=self._open_mapping, state="disabled")
        self.btn_map.pack(side="left", padx=(8, 0))
        self.btn_export = ttk.Button(ctrl, text="Сохранить свод", command=self._export_summary, state="disabled")
        self.btn_export.pack(side="left", padx=(8, 0))

        self.lbl_file = tk.Label(self, text="Файл не выбран", fg="#555", bg="#f7f7f7")
        self.lbl_file.pack(anchor="w", padx=12, pady=(0, 2))

        self.lbl_sheet = tk.Label(self, text="", fg="#777", bg="#f7f7f7")
        self.lbl_sheet.pack(anchor="w", padx=12, pady=(0, 6))

        # Сводные показатели (карточка)
        card = tk.Frame(self, bg="#ffffff", bd=1, relief="solid")
        card.pack(fill="x", padx=12, pady=(0, 10))

        grid = tk.Frame(card, bg="#ffffff")
        grid.pack(fill="x", padx=12, pady=12)

        tk.Label(grid, text="Показатель", font=("Segoe UI", 10, "bold"), bg="#ffffff").grid(row=0, column=0, sticky="w")
        tk.Label(grid, text="Сумма (руб.)", font=("Segoe UI", 10, "bold"), bg="#ffffff").grid(row=0, column=1, sticky="e")
        tk.Label(grid, text="Доля", font=("Segoe UI", 10, "bold"), bg="#ffffff").grid(row=0, column=2, sticky="e")

        self._row_total     = self._add_metric_row(grid, 1, "Строительные затраты (Итого)")
        self._row_materials = self._add_metric_row(grid, 2, "Материалы")
        self._row_wages     = self._add_metric_row(grid, 3, "Заработная плата")
        self._row_other     = self._add_metric_row(grid, 4, "Прочие")

        for c in range(3):
            grid.grid_columnconfigure(c, weight=1)

        # Подсказка
        hint = tk.Label(
            self,
            text=("Smeta.RU: лист «ЛОКАЛЬНАЯ СМЕТА», 11 колонок.\n"
                  "Суммы — из 11-й (приоритет) или 10-й «ВСЕГО». "
                  "Материалы считаются ТОЛЬКО по inline-правилу (стоимость в строке наименования; ед. изм. не %/не чел-ч) "
                  "и только для строк с номером позиции в 1-й колонке. "
                  "ЗП — по строкам «ЗП/в т.ч. ЗПМ/оплата труда/заработ…». Прочие = Итого − Материалы − ЗП.\n"
                  "Сводные блоки в конце (Итоги/НДС/Справочно) автоматически исключаются; поддержаны несколько частей сметы."),
            fg="#666", bg="#f7f7f7", justify="left", wraplength=980
        )
        hint.pack(fill="x", padx=12, pady=(0, 10))

        # Расшифровка + Диаграмма (две колонки)
        main_split = tk.Frame(self, bg="#f7f7f7")
        main_split.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        # Левая колонка — расшифровка (таблица)
        left = tk.Frame(main_split, bg="#f7f7f7")
        left.pack(side="left", fill="both", expand=True, padx=(0, 6))

        tk.Label(left, text="Расшифровка строк", font=("Segoe UI", 11, "bold"), bg="#f7f7f7").pack(anchor="w", pady=(0, 6))

        # Фильтры категорий
        flt = tk.Frame(left, bg="#f7f7f7")
        flt.pack(anchor="w", pady=(0, 6))
        self.var_show_mat = tk.BooleanVar(value=True)
        self.var_show_wag = tk.BooleanVar(value=True)
        self.var_show_oth = tk.BooleanVar(value=True)
        ttk.Checkbutton(flt, text="Материалы", variable=self.var_show_mat, command=self._fill_breakdown_table).pack(side="left", padx=(0, 8))
        ttk.Checkbutton(flt, text="Заработная плата", variable=self.var_show_wag, command=self._fill_breakdown_table).pack(side="left", padx=(0, 8))
        ttk.Checkbutton(flt, text="Прочие", variable=self.var_show_oth, command=self._fill_breakdown_table).pack(side="left")

        # Таблица расшифровки
        tree_wrap = tk.Frame(left)
        tree_wrap.pack(fill="both", expand=True)

        cols = ("category", "name", "amount")
        self.tree = ttk.Treeview(tree_wrap, columns=cols, show="headings", height=12)
        self.tree.heading("category", text="Категория")
        self.tree.heading("name", text="Наименование")
        self.tree.heading("amount", text="Сумма, руб.")
        self.tree.column("category", width=140, anchor="w")
        self.tree.column("name", width=420, anchor="w")
        self.tree.column("amount", width=120, anchor="e")

        yscroll = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll.set)
        self.tree.pack(side="left", fill="both", expand=True)
        yscroll.pack(side="right", fill="y")

        # Правая колонка — диаграмма
        right = tk.Frame(main_split, bg="#f7f7f7")
        right.pack(side="left", fill="both", expand=False, padx=(6, 0))

        tk.Label(right, text="Диаграмма структуры", font=("Segoe UI", 11, "bold"), bg="#f7f7f7").pack(anchor="w", pady=(0, 6))

        self.chart_area = tk.Frame(right, bg="#ffffff", bd=1, relief="solid")
        self.chart_area.pack(fill="both", expand=False)
        self.chart_area.configure(width=420, height=320)
        self.chart_area.pack_propagate(False)

        # ссылки для графика (чтобы не убило GC)
        self._mpl_fig = None
        self._mpl_canvas = None
        self._tk_canvas = None
        self._chart_placeholder = None

    def _add_metric_row(self, grid, r, title: str):
        tk.Label(grid, text=title, bg="#ffffff").grid(row=r, column=0, sticky="w", pady=3)
        val_lbl = tk.Label(grid, text="-", bg="#ffffff", anchor="e")
        val_lbl.grid(row=r, column=1, sticky="e", pady=3)
        pct_lbl = tk.Label(grid, text="-", bg="#ffffff", anchor="e")
        pct_lbl.grid(row=r, column=2, sticky="e", pady=3)
        return {"val": val_lbl, "pct": pct_lbl}

    # ---------- Файл ----------
    def _open_file(self):
        try:
            from tkinter import filedialog as fd
        except Exception:
            messagebox.showerror("Файл", "Не удалось открыть диалог выбора файлов.")
            return
        fname = fd.askopenfilename(
            title="Выберите файл сметы (XLSX/CSV)",
            filetypes=[("Excel", "*.xlsx;*.xlsm"), ("CSV", "*.csv"), ("Все файлы", "*.*")]
        )
        if not fname:
            return
        self.file_path = Path(fname)
        self.lbl_file.config(text=f"Файл: {self.file_path}")

        ok = self._load_file(self.file_path)
        # Ручное сопоставление — только в generic-режиме
        self.btn_map.config(state=("normal" if (ok and self.mode == "generic") else "disabled"))
        self.btn_export.config(state=("normal" if ok else "disabled"))
        if not ok:
            messagebox.showwarning("Анализ смет", "Не удалось распознать структуру файла. "
                                                  "Попробуйте ручное сопоставление (для CSV/XLSX-таблиц).")

    def _load_file(self, path: Path) -> bool:
        self.mode = "generic"
        self.headers, self.rows = [], []
        self.breakdown_rows = []
        self.smeta_sheet_name = None
        self.smeta_name_col = None
        self.smeta_cost_cols = []
        self.smeta_data_rows = []
        self.lbl_sheet.config(text="")

        ext = path.suffix.lower()
        try:
            if ext in (".xlsx", ".xlsm"):
                if self._parse_xlsx_smeta_ru(path):
                    self.mode = "smeta"
                    self._analyze_smeta()
                    return True
                # не похоже на smeta.ru — общий режим
                self._parse_xlsx_generic(path)
                self.mapping = self._detect_mapping(self.headers, self.rows)
                self._analyze_generic()
                return True
            elif ext == ".csv":
                self._parse_csv_generic(path)
                self.mapping = self._detect_mapping(self.headers, self.rows)
                self._analyze_generic()
                return True
            else:
                try:
                    if self._parse_xlsx_smeta_ru(path):
                        self.mode = "smeta"
                        self._analyze_smeta()
                        return True
                    self._parse_xlsx_generic(path)
                    self.mapping = self._detect_mapping(self.headers, self.rows)
                    self._analyze_generic()
                    return True
                except Exception:
                    self._parse_csv_generic(path)
                    self.mapping = self._detect_mapping(self.headers, self.rows)
                    self._analyze_generic()
                    return True
        except Exception as e:
            messagebox.showerror("Загрузка сметы", f"Ошибка чтения файла:\n{e}")
            return False

    # ---------- Smeta.RU режим ----------

    def _parse_xlsx_smeta_ru(self, path: Path) -> bool:
        """
        Ищем лист, где в верхних строках встречается «ЛОКАЛЬНАЯ СМЕТА».
        На нём ищем шапку таблицы: колонку «Наименование работ и затрат» и колонки «ВСЕГО».
        Считываем строки, пропуская сводные блоки; поддержаны несколько частей сметы.
        """
        wb = load_workbook(path, read_only=True, data_only=True)
        target_ws = None
        for ws in wb.worksheets:
            if self._sheet_has_local_smeta_marker(ws):
                target_ws = ws
                break
        if target_ws is None:
            return False

        # Найти шапку
        hdr_row_idx, name_col, cost_cols = self._find_table_header(target_ws)
        if hdr_row_idx is None or name_col is None or not cost_cols:
            return False

        data_rows: List[List[Any]] = []
        # Для итогов (берём последние встреченные)
        last_local_total: Optional[float] = None
        last_grand_total: Optional[float] = None

        # Чтение всех строк после шапки (без остановки на первом «итого»)
        for row in target_ws.iter_rows(min_row=hdr_row_idx + 1, values_only=True):
            cells = list(row)

            # Пропустить полностью пустые
            if not any(c is not None and str(c).strip() for c in cells):
                continue

            # Определить текст в колонке наименования
            name_cell = self._str(cells[name_col]) if name_col < len(cells) else ""

            # Пропустить строку нумерации 1..11 (часто идёт сразу после заголовков или внутри второй части)
            if self._is_numbering_row(cells):
                continue

            # Если это сводная строка (итоги/НДС/справочно) — сохранить итог (если есть) и пропустить её
            if self._is_summary_row(cells, name_col):
                # Считать сумму: сначала из приоритетных cost_cols, потом соседние
                val = self._first_number_from_cols(cells, cost_cols)
                if not isinstance(val, float):
                    for base in cost_cols:
                        for j in (base - 1, base + 1):
                            if 0 <= j < len(cells):
                                v2 = self._to_number(cells[j])
                                if isinstance(v2, float):
                                    val = v2
                                    break
                        if isinstance(val, float):
                            break

                low = name_cell.lower()
                if "итого по смете" in low or ("итого" in low and "смете" in low):
                    if isinstance(val, float):
                        last_grand_total = val
                if "итого по локальной смете" in low:
                    if isinstance(val, float):
                        last_local_total = val
                # Пропускаем
                continue

            # Обычная (данная) строка
            data_rows.append(cells)

        self.smeta_sheet_name = target_ws.title
        self.smeta_name_col = name_col
        self.smeta_cost_cols = cost_cols  # упорядочены: 11-я, затем 10-я
        self.smeta_data_rows = data_rows

        # Выбрать итог: сначала «Итого по смете», иначе «Итого по локальной смете»
        total = last_grand_total if isinstance(last_grand_total, float) else last_local_total
        self.stats = {"total": float(total or 0.0), "materials": 0.0, "wages": 0.0, "other": 0.0}

        self.lbl_sheet.config(text=f"Лист: {self.smeta_sheet_name} (режим Smeta.RU)")
        return True

    @staticmethod
    def _sheet_has_local_smeta_marker(ws) -> bool:
        try:
            for _r, row in enumerate(ws.iter_rows(min_row=1, max_row=30, values_only=True), start=1):
                for c in row:
                    if isinstance(c, str) and "локальная смета" in c.lower():
                        return True
        except Exception:
            pass
        return False

    @staticmethod
    def _normalize_header_text(s: Any) -> str:
        txt = str(s or "").strip()
        txt = txt.replace("\n", " ").replace("\r", " ")
        return re.sub(r"\s+", " ", txt).lower()

    def _find_table_header(self, ws) -> Tuple[Optional[int], Optional[int], List[int]]:
        """
        Ищем строку заголовков. Нужно:
        - «Наименование работ и затрат» (name_col)
        - все колонки с текстом, содержащим «всего» (cost_cols),
          при этом приоритет у «в текущем уровне цен».
        Если явных подписей нет, но встречается строка 1..11 — используем name_col=2 (3-я), cost_cols=[10, 9].
        """
        name_col: Optional[int] = None
        hdr_row_idx: Optional[int] = None
        ordered_cost_cols: List[int] = []

        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            raw_vals = list(row)
            vals_norm = [self._normalize_header_text(v) for v in raw_vals]

            # Попытка: строка нумерации 1..11
            only_digits = [str(v).strip() for v in raw_vals if v is not None]
            if only_digits and self._is_sequential_digits_list(only_digits):
                hdr_row_idx = i
                name_col = 2
                ordered_cost_cols = [10, 9]  # 11-я, затем 10-я (0-based)
                break

            if not any(vals_norm):
                continue

            has_name = any(
                ("наименование работ" in v and "затрат" in v) or ("наименование работ и затрат" in v)
                for v in vals_norm
            )
            # собираем «всего» без колонок коэффициентов
            idx_current = [idx for idx, v in enumerate(vals_norm) if ("всего" in v and "коэфф" not in v and "текущ" in v)]
            idx_other   = [idx for idx, v in enumerate(vals_norm) if ("всего" in v and "коэфф" not in v and "текущ" not in v)]

            if has_name and (idx_current or idx_other):
                hdr_row_idx = i
                # индекс колонки наименования
                if name_col is None:
                    try:
                        name_col = vals_norm.index(next(v for v in vals_norm if ("наименование работ" in v and "затрат" in v) or ("наименование работ и затрат" in v)))
                    except StopIteration:
                        name_col = 2
                # приоритет: текущие → прочие
                ordered_cost_cols = idx_current + idx_other
                # подстраховка
                if not ordered_cost_cols:
                    ordered_cost_cols = [10, 9]
                break

        # Фолбэк: поиск строки нумерации, если не нашли заголовки
        if hdr_row_idx is None:
            for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
                only_digits = [str(v).strip() for v in row if v is not None]
                if only_digits and self._is_sequential_digits_list(only_digits):
                    hdr_row_idx = i
                    name_col = 2
                    ordered_cost_cols = [10, 9]
                    break

        return hdr_row_idx, name_col, ordered_cost_cols

    def _is_numbering_row(self, cells: List[Any]) -> bool:
        """Строка нумерации столбцов, вида 1..11 (встречается после заголовков и между частями)."""
        vals = [str(v).strip() for v in cells if v is not None and str(v).strip() != ""]
        if not vals:
            return False
        return self._is_sequential_digits_list(vals)

    @staticmethod
    def _is_sequential_digits_list(vals: List[str]) -> bool:
        try:
            nums = [int(v) for v in vals if v.isdigit()]
        except Exception:
            return False
        if not nums:
            return False
        # допускаем последовательность от 1 до N (обычно до 11), без пропусков
        return nums == list(range(1, len(nums) + 1)) and len(nums) >= 5

    def _is_summary_row(self, cells: List[Any], name_col: int) -> bool:
        """
        Свод/итоги/НДС/справочно — исключаем из данных.
        Условие:
        - первые две колонки (0 и 1) пустые И
        - 3-я колонка (name_col) содержит текст с маркерами итогов/сводов.
        """
        col0_empty = (len(cells) < 1) or (self._str(cells[0]) == "")
        col1_empty = (len(cells) < 2) or (self._str(cells[1]) == "")
        name = self._str(cells[name_col]) if name_col < len(cells) else ""
        if not (col0_empty and col1_empty and name):
            return False
        return self._is_summary_name(name)

    @staticmethod
    def _is_summary_name(name: Any) -> bool:
        """Распознаём свод/итоги/НДС/справочно по тексту."""
        s = re.sub(r"\s+", " ", str(name or "")).strip().lower()
        if not s:
            return False
        # Не считаем «Всего по позиции» сводом (внутрипозиционная строка)
        if "по позиции" in s:
            return False
        patterns = [
            "итого по локальной смете",
            "итоги по смете", "итоги по разделу", "итоги по", "итог по",
            "итого прямые затраты", "итого прямые", "итого по смете",
            "всего по смете", "всего по разделу", "всего по",
            "справочно", "ндс", "итого с ндс", "всего с ндс",
        ]
        return any(p in s for p in patterns)

    # ---------- Вспомогательные ----------

    @staticmethod
    def _str(x: Any) -> str:
        return str(x or "").strip()

    @staticmethod
    def _to_number(x: Any) -> Optional[float]:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return None
        s = s.replace("\u00A0", " ")
        s = re.sub(r"[^0-9,.\-]", "", s)
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None

    def _first_number_from_cols(self, row: List[Any], cols: List[int]) -> Optional[float]:
        for j in cols:
            if 0 <= j < len(row):
                v = self._to_number(row[j])
                if isinstance(v, float):
                    return v
        return None

    def _is_labor_or_percent_unit(self, unit: Any) -> bool:
        """
        Возвращает True, если ед. изм. похоже на проценты (%) или трудочасы (чел-ч).
        """
        if unit is None:
            return False
        u = str(unit).strip().lower()
        if not u:
            return False
        if "%" in u:
            return True
        if "чел" in u:  # чел-ч, чел.-ч. и т.п.
            return True
        return False

    def _has_numeric_position(self, cell: Any) -> bool:
        s = str(cell or "").strip()
        return bool(s) and s[0].isdigit()

    # ---------- Классификация строк ----------

    def _classify_row(self, row: List[Any]) -> Optional[str]:
        """
        Возвращает категорию строки:
          - 'wages'     — ЗП, в т.ч. ЗПМ, оплата труда
          - 'materials' — ТОЛЬКО inline-материал: строка позиции (1-я колонка начинается с цифры)
                          с прямой стоимостью в 11/10 кол. (ед. изм. не %/не чел-ч)
          - None        — игнорировать (ЭМ, НР, СП, ЗТР, служебные и сводные строки)
        """
        # Сводные — сразу мимо
        if self._is_summary_row(row, self.smeta_name_col or 2):
            return None

        name = self._str(row[self.smeta_name_col]) if (self.smeta_name_col is not None and self.smeta_name_col < len(row)) else ""
        n = name.lower()

        # Служебные/неучитываемые строки
        if not n:
            return None
        if n.startswith("всего по позиции"):
            return None
        if n.startswith("итого"):
            return None
        if n.startswith("раздел:") or n.startswith("локальная смета") or "итого по разделу" in n or "итого по смете" in n:
            return None
        if "зтр" in n or n.startswith("эм") or n.startswith("нр ") or "нр от зп" in n or "сп от зп" in n or "нр и сп" in n:
            return None

        # Явная ЗП
        if n == "зп" or n == "з/п" or "оплата труда" in n or "заработ" in n or n == "зпм" or "в т.ч. зпм" in n:
            return "wages"

        # Материалы — только inline-правило для строк позиций (есть номер в 1-й колонке)
        if self.smeta_cost_cols:
            if self._has_numeric_position(row[0] if len(row) > 0 else None):
                val = self._first_number_from_cols(row, self.smeta_cost_cols)
                if isinstance(val, float) and val > 0:
                    unit = row[3] if len(row) > 3 else ""
                    if not self._is_labor_or_percent_unit(unit):
                        return "materials"

        return None

    # ---------- Аналитика (Smeta.RU) ----------

    def _analyze_smeta(self):
        if self.smeta_name_col is None or not self.smeta_cost_cols:
            raise RuntimeError("Не заданы индексы колонок для сметы.")

        wages_sum = 0.0
        mats_sum = 0.0
        self.breakdown_rows = []

        for row in self.smeta_data_rows:
            cat = self._classify_row(row)
            if not cat:
                continue

            val = self._first_number_from_cols(row, self.smeta_cost_cols)
            if not isinstance(val, float):
                continue

            name = self._str(row[self.smeta_name_col]) if self.smeta_name_col < len(row) else ""
            if cat == "wages":
                wages_sum += val
                self.breakdown_rows.append({"category": "Заработная плата", "name": name, "amount": val})
            elif cat == "materials":
                mats_sum += val
                self.breakdown_rows.append({"category": "Материалы", "name": name, "amount": val})

        total = float(self.stats.get("total") or 0.0)
        if total <= 0:
            total = mats_sum + wages_sum

        other = max(0.0, total - mats_sum - wages_sum)

        self.stats = {"total": total, "materials": mats_sum, "wages": wages_sum, "other": other}
        self._render_stats()
        self._fill_breakdown_table()
        self._render_chart()

    # ---------- Общий режим (XLSX/CSV) ----------

    def _parse_xlsx_generic(self, path: Path):
        wb = load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        hdr_row_idx = None
        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            cells = [self._str(c) for c in row]
            if sum(1 for c in cells if c) >= 2:
                hdr_row_idx = i
                self.headers = [self._norm_header(c) for c in cells]
                break
        if hdr_row_idx is None:
            raise RuntimeError("Не найдена строка заголовков")
        self.rows = [list(row) for row in ws.iter_rows(min_row=hdr_row_idx + 1, values_only=True)]
        self.lbl_sheet.config(text=f"Лист: {ws.title} (общий режим)")

    def _parse_csv_generic(self, path: Path):
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096)
            f.seek(0)
            try:
                sniffer = csv.Sniffer()
                dialect = sniffer.sniff(sample, delimiters=";,")
            except Exception:
                class D:
                    delimiter = ";"
                dialect = D()
            reader = csv.reader(f, dialect=dialect)
            rows = list(reader)
        if not rows:
            raise RuntimeError("CSV пустой")
        hdr_idx = next((i for i, row in enumerate(rows) if any((c or "").strip() for c in row)), None)
        if hdr_idx is None:
            raise RuntimeError("Не найдена строка заголовков")
        self.headers = [self._norm_header(c) for c in rows[hdr_idx]]
        self.rows = rows[hdr_idx + 1:]
        self.lbl_sheet.config(text="CSV (общий режим)")

    @staticmethod
    def _norm_header(s: Any) -> str:
        txt = str(s or "").strip()
        txt = txt.replace("\n", " ").replace("\r", " ")
        return re.sub(r"\s+", " ", txt)

    def _detect_mapping(self, headers: List[str], rows: List[List[Any]]) -> Dict[str, Optional[int]]:
        hlow = [h.lower() for h in headers]

        def find_candidates(patterns: List[str]) -> List[int]:
            return [i for i, h in enumerate(hlow) if any(p in h for p in patterns)]

        def best_index(cands: List[int]) -> Optional[int]:
            best_i, best_sum = None, -1.0
            for idx in cands:
                s = 0.0
                for r in rows:
                    if idx < len(r):
                        v = self._to_number(r[idx])
                        if isinstance(v, float):
                            s += v
                if s > best_sum:
                    best_sum, best_i = s, idx
            return best_i

        return {
            "total":     best_index(find_candidates(["итого", "всего", "стоим", "смет", "общая стоимость"])),
            "materials": best_index(find_candidates(["матер", "материа", "мр"])),
            "wages":     best_index(find_candidates(["зараб", "оплата труда", "з/п", "зп", "труд"])),
        }

    def _sum_column(self, idx: Optional[int]) -> float:
        if idx is None:
            return 0.0
        s = 0.0
        for r in self.rows:
            if idx < len(r):
                v = self._to_number(r[idx])
                if isinstance(v, float):
                    s += v
        return s

    def _analyze_generic(self):
        total     = self._sum_column(self.mapping.get("total"))
        materials = self._sum_column(self.mapping.get("materials"))
        wages     = self._sum_column(self.mapping.get("wages"))

        if total <= 0:
            total = materials + wages

        other = max(0.0, total - materials - wages)
        self.stats = {"total": total, "materials": materials, "wages": wages, "other": other}
        self.breakdown_rows = []
        self._render_stats()
        self._fill_breakdown_table()
        self._render_chart()

    # ---------- Отрисовка результатов ----------

    @staticmethod
    def _fmt_money(x: Optional[float]) -> str:
        if x is None:
            return "-"
        try:
            s = f"{float(x):,.2f}"
            s = s.replace(",", " ").replace(".", ",")
            return s
        except Exception:
            return str(x)

    @staticmethod
    def _fmt_pct(x: Optional[float]) -> str:
        if x is None:
            return "-"
        try:
            return f"{x:.1f}%"
        except Exception:
            return "-"

    def _safe_pct(self, part: float) -> Optional[float]:
        t = self.stats.get("total", 0.0)
        return (part / t * 100.0) if t and t > 1e-12 else None

    def _render_stats(self):
        total     = float(self.stats.get("total") or 0.0)
        materials = float(self.stats.get("materials") or 0.0)
        wages     = float(self.stats.get("wages") or 0.0)
        other     = float(self.stats.get("other") or 0.0)

        p_mat = (materials / total * 100.0) if total > 1e-12 else None
        p_wag = (wages     / total * 100.0) if total > 1e-12 else None
        p_oth = (other     / total * 100.0) if total > 1e-12 else None

        self._row_total["val"].config(text=self._fmt_money(total))
        self._row_total["pct"].config(text="100%")

        self._row_materials["val"].config(text=self._fmt_money(materials))
        self._row_materials["pct"].config(text=self._fmt_pct(p_mat))

        self._row_wages["val"].config(text=self._fmt_money(wages))
        self._row_wages["pct"].config(text=self._fmt_pct(p_wag))

        self._row_other["val"].config(text=self._fmt_money(other))
        self._row_other["pct"].config(text=self._fmt_pct(p_oth))

    # ---------- Расшифровка (таблица) ----------

    def _fill_breakdown_table(self):
        # Очистить
        for i in self.tree.get_children():
            self.tree.delete(i)
        if not self.breakdown_rows:
            # Нечего показывать
            return
        show_mat = self.var_show_mat.get()
        show_wag = self.var_show_wag.get()
        show_oth = self.var_show_oth.get()

        for row in self.breakdown_rows:
            cat = row["category"]
            if (cat == "Материалы" and not show_mat) or (cat == "Заработная плата" and not show_wag) or (cat == "Прочие" and not show_oth):
                continue
            name = str(row["name"])
            amt = float(row["amount"] or 0.0)
            self.tree.insert("", "end", values=(cat, name, self._fmt_money(amt)))

    # ---------- Диаграмма ----------

    def _render_chart(self):
        # Очистка области диаграммы
        for w in self.chart_area.winfo_children():
            try:
                w.destroy()
            except Exception:
                pass
        self._mpl_fig = None
        self._mpl_canvas = None
        self._tk_canvas = None
        self._chart_placeholder = None

        vals = [
            float(self.stats.get("materials") or 0.0),
            float(self.stats.get("wages") or 0.0),
            float(self.stats.get("other") or 0.0),
        ]
        labels = ["Материалы", "Заработная плата", "Прочие"]
        colors = [self.COLORS["materials"], self.COLORS["wages"], self.COLORS["other"]]
        total = float(self.stats.get("total") or 0.0)

        if total <= 0 or sum(vals) <= 0:
            self._chart_placeholder = tk.Label(self.chart_area, text="Нет данных для диаграммы", bg="#ffffff", fg="#888")
            self._chart_placeholder.pack(fill="both", expand=True)
            return

        if MPL_AVAILABLE:
            # Matplotlib pie
            self._mpl_fig = plt.Figure(figsize=(4.2, 3.0), dpi=100)
            ax = self._mpl_fig.add_subplot(111)

            def autopct_fmt(pct):
                return f"{pct:.1f}%" if pct >= 1.0 else ""

            ax.pie(
                vals,
                labels=labels,
                colors=colors,
                autopct=autopct_fmt,
                startangle=90,
                counterclock=False
            )
            ax.axis("equal")
            ax.set_title("Структура затрат")
            self._mpl_canvas = FigureCanvasTkAgg(self._mpl_fig, master=self.chart_area)
            self._mpl_canvas.draw()
            self._mpl_canvas.get_tk_widget().pack(fill="both", expand=True)
        else:
            # Tk Canvas fallback
            c = tk.Canvas(self.chart_area, width=420, height=280, bg="#ffffff", highlightthickness=0)
            c.pack(fill="both", expand=True)
            cx, cy, r = 150, 140, 110
            start = 0.0
            s = sum(vals)
            for v, col in zip(vals, colors):
                if v <= 0:
                    continue
                extent = 360.0 * v / s
                c.create_arc(cx - r, cy - r, cx + r, cy + r, start=start, extent=extent,
                             fill=col, outline="#ffffff", width=1)
                start += extent
            # Легенда
            lx, ly = 300, 80
            for lbl, col, v in zip(labels, colors, vals):
                c.create_rectangle(lx, ly, lx + 14, ly + 14, fill=col, outline=col)
                pct = (v / s * 100.0) if s > 1e-12 else 0.0
                c.create_text(lx + 20, ly + 7, text=f"{lbl} — {pct:.1f}%", anchor="w", fill="#333", font=("Segoe UI", 9))
                ly += 22

    # ---------- Действия пользователя ----------

    def _open_mapping(self):
        if not self.headers or self.mode != "generic":
            return
        dlg = ColumnMappingDialog(self, headers=self.headers, cur_map=self.mapping)
        if getattr(dlg, "result", None):
            self.mapping = dlg.result
            self._analyze_generic()

    def _export_summary(self):
        try:
            from tkinter import filedialog as fd
        except Exception:
            messagebox.showerror("Экспорт", "Не удалось открыть диалог сохранения.")
            return
        if not self.stats:
            return

        fname = fd.asksaveasfilename(
            title="Сохранить свод",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv")]
        )
        if not fname:
            return
        out = Path(fname)

        try:
            if out.suffix.lower() == ".csv":
                with open(out, "w", encoding="utf-8-sig", newline="") as f:
                    w = csv.writer(f, delimiter=";")
                    w.writerow(["Показатель", "Сумма (руб.)", "Доля"])
                    w.writerow(["Строительные затраты (Итого)", f"{self._fmt_money(self.stats['total'])}", "100%"])
                    w.writerow(["Материалы", f"{self._fmt_money(self.stats['materials'])}",
                                self._fmt_pct(self._safe_pct(self.stats['materials']))])
                    w.writerow(["Заработная плата", f"{self._fmt_money(self.stats['wages'])}",
                                self._fmt_pct(self._safe_pct(self.stats['wages']))])
                    w.writerow(["Прочие", f"{self._fmt_money(self.stats['other'])}",
                                self._fmt_pct(self._safe_pct(self.stats['other']))])
                    # Расшифровка
                    w.writerow([])
                    w.writerow(["Расшифровка", "", ""])
                    w.writerow(["Категория", "Наименование", "Сумма, руб."])
                    for row in self.breakdown_rows:
                        w.writerow([row["category"], row["name"], f"{self._fmt_money(row['amount'])}"])
            else:
                # XLSX — числа пишем как числа
                wb = Workbook()
                ws = wb.active
                ws.title = "Анализ сметы"
                ws.append(["Показатель", "Сумма (руб.)", "Доля"])
                ws.append(["Строительные затраты (Итого)", float(self.stats.get("total", 0.0)), "100%"])
                ws.append([
                    "Материалы",
                    float(self.stats.get("materials", 0.0)),
                    self._fmt_pct(self._safe_pct(self.stats.get("materials", 0.0))),
                ])
                ws.append([
                    "Заработная плата",
                    float(self.stats.get("wages", 0.0)),
                    self._fmt_pct(self._safe_pct(self.stats.get("wages", 0.0))),
                ])
                ws.append([
                    "Прочие",
                    float(self.stats.get("other", 0.0)),
                    self._fmt_pct(self._safe_pct(self.stats.get("other", 0.0))),
                ])
                ws.append([])
                ws.append(["Расшифровка"])
                ws.append(["Категория", "Наименование", "Сумма, руб."])
                for row in self.breakdown_rows:
                    ws.append([
                        row["category"],
                        row["name"],
                        float(row.get("amount", 0.0) or 0.0)
                    ])
                ws.column_dimensions["A"].width = 36
                ws.column_dimensions["B"].width = 60
                ws.column_dimensions["C"].width = 18
                wb.save(out)

            messagebox.showinfo("Экспорт", f"Свод сохранён:\n{out}")
        except Exception as e:
            messagebox.showerror("Экспорт", f"Не удалось сохранить свод:\n{e}")


# --------- API для встраивания/стендалон ---------

def create_page(parent) -> tk.Frame:
    page = BudgetAnalysisPage(parent)
    page.pack(fill="both", expand=True)
    return page

def open_budget_analyzer(parent=None):
    if parent is None:
        root = tk.Tk()
        root.title("Анализ смет")
        root.geometry("1100x740")
        BudgetAnalysisPage(root).pack(fill="both", expand=True)
        root.mainloop()
        return root
    win = tk.Toplevel(parent)
    win.title("Анализ смет")
    win.geometry("1100x740")
    BudgetAnalysisPage(win).pack(fill="both", expand=True)
    return win

if __name__ == "__main__":
    open_budget_analyzer()
