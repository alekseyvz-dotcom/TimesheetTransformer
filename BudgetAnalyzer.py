# BudgetAnalyzer.py
# Анализ смет: поддержка смет Smeta.RU (лист «ЛОКАЛЬНАЯ СМЕТА», 11 колонок),
# расшифровка строк и диаграмма структуры затрат.
# Логика:
# - Ищем лист с “ЛОКАЛЬНАЯ СМЕТА”
# - Находим шапку: строка заголовков или нумерации 1..11 → name_col=2 (3-я), cost_cols=[10,9] (11-я приоритет, затем 10-я)
# - Ищем строку «Итого по локальной смете» по ВСЕЙ строке; берём сумму из cost_cols
# - Учитываем ресурсные строки в 3-й колонке: ЗП/в т.ч. ЗПМ → wages; МР/Материалы/Мат. → materials
# - Прочие = Итого − Материалы − ЗП

import re
import csv
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from openpyxl import Workbook, load_workbook

# matplotlib (опционально)
try:
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    import matplotlib.pyplot as plt
    MPL_AVAILABLE = True
except Exception:
    MPL_AVAILABLE = False


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


class BudgetAnalysisPage(tk.Frame):
    COLORS = {"materials": "#42a5f5", "wages": "#66bb6a", "other": "#ffa726"}

    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        self.file_path: Optional[Path] = None

        # generic
        self.headers: List[str] = []
        self.rows: List[List[Any]] = []
        self.mapping: Dict[str, Optional[int]] = {"total": None, "materials": None, "wages": None}

        # smeta
        self.mode: str = "generic"
        self.smeta_sheet_name: Optional[str] = None
        self.smeta_name_col: Optional[int] = None      # 0-based, обычно 2
        self.smeta_cost_cols: List[int] = []           # приоритет [11-я, 10-я] = [10,9]
        self.smeta_data_rows: List[List[Any]] = []     # строки данных (после шапки, до итога)

        # итоги/расшифровка
        self.stats = {"total": 0.0, "materials": 0.0, "wages": 0.0, "other": 0.0}
        self.breakdown_rows: List[Dict[str, Any]] = []

        # UI
        header = tk.Frame(self, bg="#f7f7f7"); header.pack(fill="x", padx=12, pady=(10, 6))
        tk.Label(header, text="Анализ смет", font=("Segoe UI", 16, "bold"), bg="#f7f7f7").pack(side="left")

        ctrl = tk.Frame(self, bg="#f7f7f7"); ctrl.pack(fill="x", padx=12, pady=(0, 8))
        self.btn_open = ttk.Button(ctrl, text="Открыть смету (XLSX/CSV)", command=self._open_file); self.btn_open.pack(side="left")
        self.btn_map  = ttk.Button(ctrl, text="Настроить соответствие колонок", command=self._open_mapping, state="disabled"); self.btn_map.pack(side="left", padx=(8,0))
        self.btn_export = ttk.Button(ctrl, text="Сохранить свод", command=self._export_summary, state="disabled"); self.btn_export.pack(side="left", padx=(8,0))

        self.lbl_file  = tk.Label(self, text="Файл не выбран", fg="#555", bg="#f7f7f7"); self.lbl_file.pack(anchor="w", padx=12, pady=(0, 2))
        self.lbl_sheet = tk.Label(self, text="", fg="#777", bg="#f7f7f7"); self.lbl_sheet.pack(anchor="w", padx=12, pady=(0, 6))

        card = tk.Frame(self, bg="#ffffff", bd=1, relief="solid"); card.pack(fill="x", padx=12, pady=(0, 10))
        grid = tk.Frame(card, bg="#ffffff"); grid.pack(fill="x", padx=12, pady=12)

        tk.Label(grid, text="Показатель", font=("Segoe UI", 10, "bold"), bg="#ffffff").grid(row=0, column=0, sticky="w")
        tk.Label(grid, text="Сумма (руб.)", font=("Segoe UI", 10, "bold"), bg="#ffffff").grid(row=0, column=1, sticky="e")
        tk.Label(grid, text="Доля", font=("Segoe UI", 10, "bold"), bg="#ffffff").grid(row=0, column=2, sticky="e")

        self._row_total     = self._add_metric_row(grid, 1, "Строительные затраты (Итого)")
        self._row_materials = self._add_metric_row(grid, 2, "Материалы")
        self._row_wages     = self._add_metric_row(grid, 3, "Заработная плата")
        self._row_other     = self._add_metric_row(grid, 4, "Прочие")
        for c in range(3): grid.grid_columnconfigure(c, weight=1)

        hint = tk.Label(self, text=("Поддержка Smeta.RU: лист «ЛОКАЛЬНАЯ СМЕТА», 11 колонок.\n"
                                    "Суммы — из 11-й (приоритет) или 10-й «ВСЕГО». "
                                    "ЗП/в т.ч. ЗПМ → «Заработная плата», МР/Материалы/Мат. → «Материалы». "
                                    "Прочие = Итого − Материалы − ЗП."),
                        fg="#666", bg="#f7f7f7", justify="left", wraplength=980)
        hint.pack(fill="x", padx=12, pady=(0, 10))

        main_split = tk.Frame(self, bg="#f7f7f7"); main_split.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        left = tk.Frame(main_split, bg="#f7f7f7"); left.pack(side="left", fill="both", expand=True, padx=(0, 6))
        tk.Label(left, text="Расшифровка строк", font=("Segoe UI", 11, "bold"), bg="#f7f7f7").pack(anchor="w", pady=(0, 6))

        flt = tk.Frame(left, bg="#f7f7f7"); flt.pack(anchor="w", pady=(0, 6))
        self.var_show_mat = tk.BooleanVar(value=True); self.var_show_wag = tk.BooleanVar(value=True); self.var_show_oth = tk.BooleanVar(value=True)
        ttk.Checkbutton(flt, text="Материалы", variable=self.var_show_mat, command=self._fill_breakdown_table).pack(side="left", padx=(0, 8))
        ttk.Checkbutton(flt, text="Заработная плата", variable=self.var_show_wag, command=self._fill_breakdown_table).pack(side="left", padx=(0, 8))
        ttk.Checkbutton(flt, text="Прочие", variable=self.var_show_oth, command=self._fill_breakdown_table).pack(side="left")

        tree_wrap = tk.Frame(left); tree_wrap.pack(fill="both", expand=True)
        cols = ("category", "name", "amount")
        self.tree = ttk.Treeview(tree_wrap, columns=cols, show="headings", height=12)
        self.tree.heading("category", text="Категория"); self.tree.column("category", width=140, anchor="w")
        self.tree.heading("name", text="Наименование"); self.tree.column("name", width=420, anchor="w")
        self.tree.heading("amount", text="Сумма, руб."); self.tree.column("amount", width=120, anchor="e")
        yscroll = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll.set); self.tree.pack(side="left", fill="both", expand=True); yscroll.pack(side="right", fill="y")

        right = tk.Frame(main_split, bg="#f7f7f7"); right.pack(side="left", fill="both", expand=False, padx=(6, 0))
        tk.Label(right, text="Диаграмма структуры", font=("Segoe UI", 11, "bold"), bg="#f7f7f7").pack(anchor="w", pady=(0, 6))
        self.chart_area = tk.Frame(right, bg="#ffffff", bd=1, relief="solid"); self.chart_area.pack(fill="both", expand=False)
        self.chart_area.configure(width=420, height=320); self.chart_area.pack_propagate(False)

        self._mpl_fig = None; self._mpl_canvas = None; self._tk_canvas = None; self._chart_placeholder = None

    def _add_metric_row(self, grid, r, title: str):
        tk.Label(grid, text=title, bg="#ffffff").grid(row=r, column=0, sticky="w", pady=3)
        val_lbl = tk.Label(grid, text="-", bg="#ffffff", anchor="e"); val_lbl.grid(row=r, column=1, sticky="e", pady=3)
        pct_lbl = tk.Label(grid, text="-", bg="#ffffff", anchor="e"); pct_lbl.grid(row=r, column=2, sticky="e", pady=3)
        return {"val": val_lbl, "pct": pct_lbl}

    # ---------- Файл ----------
    def _open_file(self):
        try:
            from tkinter import filedialog as fd
        except Exception:
            messagebox.showerror("Файл", "Не удалось открыть диалог выбора файлов.")
            return
        fname = fd.askopenfilename(title="Выберите файл сметы (XLSX/CSV)",
                                   filetypes=[("Excel", "*.xlsx;*.xlsm"), ("CSV", "*.csv"), ("Все файлы", "*.*")])
        if not fname: return
        self.file_path = Path(fname); self.lbl_file.config(text=f"Файл: {self.file_path}")

        ok = self._load_file(self.file_path)
        self.btn_map.config(state=("normal" if (ok and self.mode == "generic") else "disabled"))
        self.btn_export.config(state=("normal" if ok else "disabled"))
        if not ok:
            messagebox.showwarning("Анализ смет", "Не удалось распознать структуру файла. Попробуйте ручное сопоставление (CSV/XLSX).")

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
                    self.mode = "smeta"; self._analyze_smeta(); return True
                self._parse_xlsx_generic(path); self.mapping = self._detect_mapping(self.headers, self.rows); self._analyze_generic(); return True
            elif ext == ".csv":
                self._parse_csv_generic(path); self.mapping = self._detect_mapping(self.headers, self.rows); self._analyze_generic(); return True
            else:
                try:
                    if self._parse_xlsx_smeta_ru(path):
                        self.mode = "smeta"; self._analyze_smeta(); return True
                    self._parse_xlsx_generic(path); self.mapping = self._detect_mapping(self.headers, self.rows); self._analyze_generic(); return True
                except Exception:
                    self._parse_csv_generic(path); self.mapping = self._detect_mapping(self.headers, self.rows); self._analyze_generic(); return True
        except Exception as e:
            messagebox.showerror("Загрузка сметы", f"Ошибка чтения файла:\n{e}")
            return False

    # ---------- Smeta.RU ----------
    def _parse_xlsx_smeta_ru(self, path: Path) -> bool:
        wb = load_workbook(path, read_only=True, data_only=True)
        target_ws = None
        for ws in wb.worksheets:
            if self._sheet_has_local_smeta_marker(ws):
                target_ws = ws; break
        if target_ws is None: return False

        hdr_row_idx, name_col, cost_cols = self._find_table_header(target_ws)
        if hdr_row_idx is None or name_col is None or not cost_cols: return False

        data_rows = []
        for row in target_ws.iter_rows(min_row=hdr_row_idx + 1, values_only=True):
            cells = list(row)
            # стоп по фразе в любой ячейке
            stop = False
            for c in cells:
                if isinstance(c, str) and "итого по локальной смете" in c.lower():
                    stop = True; break
            if stop: break
            data_rows.append(cells)

        self.smeta_sheet_name = target_ws.title
        self.smeta_name_col = name_col
        self.smeta_cost_cols = cost_cols  # уже в приоритете: [11-я, 10-я]
        self.smeta_data_rows = data_rows

        total = self._find_local_total(target_ws, hdr_row_idx, name_col, cost_cols)
        self.stats = {"total": total or 0.0, "materials": 0.0, "wages": 0.0, "other": 0.0}
        self.lbl_sheet.config(text=f"Лист: {self.smeta_sheet_name} (режим Smeta.RU)")
        return True

    @staticmethod
    def _sheet_has_local_smeta_marker(ws) -> bool:
        try:
            for row in ws.iter_rows(min_row=1, max_row=30, values_only=True):
                for c in row:
                    if isinstance(c, str) and "локальная смета" in c.lower():
                        return True
        except Exception:
            pass
        return False

    @staticmethod
    def _normalize_header_text(s: Any) -> str:
        txt = str(s or "").strip().replace("\n", " ").replace("\r", " ")
        return re.sub(r"\s+", " ", txt).lower()

    def _find_table_header(self, ws) -> Tuple[Optional[int], Optional[int], List[int]]:
        name_col: Optional[int] = None
        hdr_row_idx: Optional[int] = None
        ordered_cost_cols: List[int] = []

        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            raw_vals = list(row)
            vals_norm = [self._normalize_header_text(v) for v in raw_vals]
            if not any(vals_norm):
                # Попытка: строка нумерации 1..11
                only_digits = [str(v).strip() for v in raw_vals if v is not None]
                if only_digits and all(x.isdigit() for x in only_digits):
                    hdr_row_idx = i; name_col = 2; ordered_cost_cols = [10, 9]; break
                continue

            has_name = any(("наименование работ" in v and "затрат" in v) or ("наименование работ и затрат" in v) for v in vals_norm)
            # Собираем ВСЕГО (исключаем столбец "коэфф...")
            idx_current = [idx for idx, v in enumerate(vals_norm) if ("всего" in v and "коэфф" not in v and "текущ" in v)]
            idx_other   = [idx for idx, v in enumerate(vals_norm) if ("всего" in v and "коэфф" not in v and "текущ" not in v)]

            if has_name and (idx_current or idx_other):
                hdr_row_idx = i
                # Выявляем индекс колонки наименований:
                if name_col is None:
                    try:
                        name_col = vals_norm.index(next(v for v in vals_norm if ("наименование работ" in v and "затрат" in v) or ("наименование работ и затрат" in v)))
                    except StopIteration:
                        name_col = 2  # безопасное значение по умолчанию
                # Приоритет: текущие → прочие
                ordered_cost_cols = idx_current + idx_other
                if not ordered_cost_cols:
                    ordered_cost_cols = [10, 9]
                break

            # Альтернатива: если попалась чисто нумерация 1..11
            only_digits = [str(v).strip() for v in raw_vals if v is not None]
            if only_digits and all(x.isdigit() for x in only_digits):
                hdr_row_idx = i; name_col = 2; ordered_cost_cols = [10, 9]; break

        # Фолбэк
        if hdr_row_idx is None:
            # последнее: попробуем найти строку с цифрами
            for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
                only_digits = [str(v).strip() for v in row if v is not None]
                if only_digits and all(x.isdigit() for x in only_digits):
                    hdr_row_idx = i; name_col = 2; ordered_cost_cols = [10, 9]; break

        return hdr_row_idx, name_col, ordered_cost_cols

    def _find_local_total(self, ws, start_row: int, name_col: int, cost_cols: List[int]) -> Optional[float]:
        for row in ws.iter_rows(min_row=start_row + 1, values_only=True):
            cells = list(row)
            # ищем фразу в любой ячейке
            if any(isinstance(c, str) and "итого по локальной смете" in c.lower() for c in cells):
                # предпочитаем значения из cost_cols по приоритету
                for j in cost_cols:
                    if 0 <= j < len(cells):
                        v = self._to_number(cells[j])
                        if isinstance(v, float):
                            return v
                # подстраховка: соседние колонки
                for base in cost_cols:
                    for j in (base - 1, base + 1):
                        if 0 <= j < len(cells):
                            v = self._to_number(cells[j])
                            if isinstance(v, float):
                                return v
        return None

    @staticmethod
    def _str(x: Any) -> str:
        return str(x or "").strip()

    @staticmethod
    def _to_number(x: Any) -> Optional[float]:
        if x is None: return None
        if isinstance(x, (int, float)): return float(x)
        s = str(x).strip()
        if not s: return None
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

    def _is_resource_row_we_care(self, name: str) -> Optional[str]:
        """
        Возвращает 'wages' для ЗП/в т.ч. ЗПМ/оплата труда
                  'materials' для МР/Материалы/Мат.
        Иначе None (игнор).
        """
        n = name.strip().lower()
        if not n: return None
        # служебные — мимо
        if n.startswith("всего по позиции"): return None
        if n.startswith("итого"): return None
        if n.startswith("раздел:") or n.startswith("локальная смета"): return None
        if "итого по разделу" in n or "итого по смете" in n: return None
        if "зтр" in n or n.startswith("эм") or n.startswith("нр ") or "нр от зп" in n or "сп от зп" in n or "нр и сп" in n:
            return None
        # категории
        if n == "зп" or n == "з/п" or "оплата труда" in n or "заработ" in n or n == "зпм" or "в т.ч. зпм" in n:
            return "wages"
        if n in ("мр", "мат", "мат.", "материалы") or "материал" in n:
            return "materials"
        return None

    def _analyze_smeta(self):
        if self.smeta_name_col is None or not self.smeta_cost_cols:
            raise RuntimeError("Не заданы индексы колонок для сметы.")

        wages_sum = 0.0
        mats_sum = 0.0
        self.breakdown_rows = []

        for row in self.smeta_data_rows:
            if self.smeta_name_col >= len(row): continue
            name = self._str(row[self.smeta_name_col])
            cat = self._is_resource_row_we_care(name)
            if not cat: continue

            val = self._first_number_from_cols(row, self.smeta_cost_cols)
            if not isinstance(val, float): continue

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
        self._render_stats(); self._fill_breakdown_table(); self._render_chart()

    # ---------- Generic ----------
    def _parse_xlsx_generic(self, path: Path):
        wb = load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        hdr_row_idx = None
        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            cells = [self._str(c) for c in row]
            if sum(1 for c in cells if c) >= 2:
                hdr_row_idx = i; self.headers = [self._norm_header(c) for c in cells]; break
        if hdr_row_idx is None: raise RuntimeError("Не найдена строка заголовков")
        self.rows = [list(row) for row in ws.iter_rows(min_row=hdr_row_idx + 1, values_only=True)]
        self.lbl_sheet.config(text=f"Лист: {ws.title} (общий режим)")

    def _parse_csv_generic(self, path: Path):
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096); f.seek(0)
            try:
                sniffer = csv.Sniffer(); dialect = sniffer.sniff(sample, delimiters=";,")
            except Exception:
                class D: delimiter = ";"
                dialect = D()
            reader = csv.reader(f, dialect=dialect); rows = list(reader)
        if not rows: raise RuntimeError("CSV пустой")
        hdr_idx = next((i for i, row in enumerate(rows) if any((c or "").strip() for c in row)), None)
        if hdr_idx is None: raise RuntimeError("Не найдена строка заголовков")
        self.headers = [self._norm_header(c) for c in rows[hdr_idx]]; self.rows = rows[hdr_idx + 1:]
        self.lbl_sheet.config(text="CSV (общий режим)")

    @staticmethod
    def _norm_header(s: Any) -> str:
        txt = str(s or "").strip().replace("\n", " ").replace("\r", " ")
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
                        if isinstance(v, float): s += v
                if s > best_sum: best_sum, best_i = s, idx
            return best_i
        return {
            "total":     best_index(find_candidates(["итого", "всего", "стоим", "смет", "общая стоимость"])),
            "materials": best_index(find_candidates(["матер", "материа", "мр"])),
            "wages":     best_index(find_candidates(["зараб", "оплата труда", "з/п", "зп", "труд"])),
        }

    def _sum_column(self, idx: Optional[int]) -> float:
        if idx is None: return 0.0
        s = 0.0
        for r in self.rows:
            if idx < len(r):
                v = self._to_number(r[idx])
                if isinstance(v, float): s += v
        return s

    def _analyze_generic(self):
        total     = self._sum_column(self.mapping.get("total"))
        materials = self._sum_column(self.mapping.get("materials"))
        wages     = self._sum_column(self.mapping.get("wages"))
        if total <= 0: total = materials + wages
        other = max(0.0, total - materials - wages)
        self.stats = {"total": total, "materials": materials, "wages": wages, "other": other}
        self.breakdown_rows = []
        self._render_stats(); self._fill_breakdown_table(); self._render_chart()

    # ---------- Отрисовка ----------
    @staticmethod
    def _fmt_money(x: Optional[float]) -> str:
        if x is None: return "-"
        try:
            s = f"{float(x):,.2f}".replace(",", " ").replace(".", ",")
            return s
        except Exception:
            return str(x)

    @staticmethod
    def _fmt_pct(x: Optional[float]) -> str:
        if x is None: return "-"
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

        self._row_total["val"].config(text=self._fmt_money(total)); self._row_total["pct"].config(text="100%")
        self._row_materials["val"].config(text=self._fmt_money(materials)); self._row_materials["pct"].config(text=self._fmt_pct(p_mat))
        self._row_wages["val"].config(text=self._fmt_money(wages)); self._row_wages["pct"].config(text=self._fmt_pct(p_wag))
        self._row_other["val"].config(text=self._fmt_money(other)); self._row_other["pct"].config(text=self._fmt_pct(p_oth))

    def _fill_breakdown_table(self):
        for i in self.tree.get_children(): self.tree.delete(i)
        if not self.breakdown_rows: return
        show_mat = self.var_show_mat.get(); show_wag = self.var_show_wag.get(); show_oth = self.var_show_oth.get()
        for row in self.breakdown_rows:
            cat = row["category"]
            if (cat == "Материалы" and not show_mat) or (cat == "Заработная плата" and not show_wag) or (cat == "Прочие" and not show_oth):
                continue
            self.tree.insert("", "end", values=(cat, str(row["name"]), self._fmt_money(float(row["amount"] or 0.0))))

    def _render_chart(self):
        for w in self.chart_area.winfo_children():
            try: w.destroy()
            except Exception: pass
        self._mpl_fig = None; self._mpl_canvas = None; self._tk_canvas = None; self._chart_placeholder = None

        vals = [float(self.stats.get("materials") or 0.0), float(self.stats.get("wages") or 0.0), float(self.stats.get("other") or 0.0)]
        labels = ["Материалы", "Заработная плата", "Прочие"]
        colors = [self.COLORS["materials"], self.COLORS["wages"], self.COLORS["other"]]
        total = float(self.stats.get("total") or 0.0)

        if total <= 0 or sum(vals) <= 0:
            self._chart_placeholder = tk.Label(self.chart_area, text="Нет данных для диаграммы", bg="#ffffff", fg="#888")
            self._chart_placeholder.pack(fill="both", expand=True); return

        if MPL_AVAILABLE:
            self._mpl_fig = plt.Figure(figsize=(4.2, 3.0), dpi=100); ax = self._mpl_fig.add_subplot(111)
            def autopct_fmt(pct): return f"{pct:.1f}%" if pct >= 1.0 else ""
            ax.pie(vals, labels=labels, colors=colors, autopct=autopct_fmt, startangle=90, counterclock=False)
            ax.axis("equal"); ax.set_title("Структура затрат")
            self._mpl_canvas = FigureCanvasTkAgg(self._mpl_fig, master=self.chart_area); self._mpl_canvas.draw()
            self._mpl_canvas.get_tk_widget().pack(fill="both", expand=True)
        else:
            c = tk.Canvas(self.chart_area, width=420, height=280, bg="#ffffff", highlightthickness=0)
            c.pack(fill="both", expand=True); cx, cy, r = 150, 140, 110; start = 0.0; s = sum(vals)
            for v, col in zip(vals, colors):
                if v <= 0: continue
                extent = 360.0 * v / s
                c.create_arc(cx - r, cy - r, cx + r, cy + r, start=start, extent=extent, fill=col, outline="#fff", width=1); start += extent
            lx, ly = 300, 80
            for lbl, col, v in zip(labels, colors, vals):
                c.create_rectangle(lx, ly, lx + 14, ly + 14, fill=col, outline=col)
                pct = (v / s * 100.0) if s > 1e-12 else 0.0
                c.create_text(lx + 20, ly + 7, text=f"{lbl} — {pct:.1f}%", anchor="w", fill="#333", font=("Segoe UI", 9)); ly += 22

    # ---------- Действия ----------
    def _open_mapping(self):
        if not self.headers or self.mode != "generic": return
        dlg = ColumnMappingDialog(self, headers=self.headers, cur_map=self.mapping)
        if getattr(dlg, "result", None): self.mapping = dlg.result; self._analyze_generic()

    def _export_summary(self):
        try:
            from tkinter import filedialog as fd
        except Exception:
            messagebox.showerror("Экспорт", "Не удалось открыть диалог сохранения."); return
        if not self.stats: return

        fname = fd.asksaveasfilename(title="Сохранить свод", defaultextension=".xlsx",
                                     filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv")])
        if not fname: return
        out = Path(fname)
        try:
            if out.suffix.lower() == ".csv":
                with open(out, "w", encoding="utf-8-sig", newline="") as f:
                    w = csv.writer(f, delimiter=";")
                    w.writerow(["Показатель", "Сумма (руб.)", "Доля"])
                    w.writerow(["Строительные затраты (Итого)", f"{self._fmt_money(self.stats['total'])}", "100%"])
                    w.writerow(["Материалы", f"{self._fmt_money(self.stats['materials'])}", self._fmt_pct(self._safe_pct(self.stats['materials']))])
                    w.writerow(["Заработная плата", f"{self._fmt_money(self.stats['wages'])}", self._fmt_pct(self._safe_pct(self.stats['wages']))])
                    w.writerow(["Прочие", f"{self._fmt_money(self.stats['other'])}", self._fmt_pct(self._safe_pct(self.stats['other']))])
                    w.writerow([]); w.writerow(["Расшифровка", "", ""]); w.writerow(["Категория", "Наименование", "Сумма, руб."])
                    for row in self.breakdown_rows:
                        w.writerow([row["category"], row["name"], f"{self._fmt_money(row['amount'])}"])
            else:
                wb = Workbook(); ws = wb.active; ws.title = "Анализ сметы"
                ws.append(["Показатель", "Сумма (руб.)", "Доля"])
                ws.append(["Строительные затраты (Итого)", float(self.stats.get("total", 0.0)), "100%"])
                ws.append(["Материалы", float(self.stats.get("materials", 0.0)), self._fmt_pct(self._safe_pct(self.stats.get("materials", 0.0)))])
                ws.append(["Заработная плата", float(self.stats.get("wages", 0.0)), self._fmt_pct(self._safe_pct(self.stats.get("wages", 0.0)))])
                ws.append(["Прочие", float(self.stats.get("other", 0.0)), self._fmt_pct(self._safe_pct(self.stats.get("other", 0.0)))])
                ws.append([]); ws.append(["Расшифровка"]); ws.append(["Категория", "Наименование", "Сумма, руб."])
                for row in self.breakdown_rows:
                    ws.append([row["category"], row["name"], float(row.get("amount", 0.0) or 0.0)])
                ws.column_dimensions["A"].width = 36; ws.column_dimensions["B"].width = 60; ws.column_dimensions["C"].width = 18
                wb.save(out)
            messagebox.showinfo("Экспорт", f"Свод сохранён:\n{out}")
        except Exception as e:
            messagebox.showerror("Экспорт", f"Не удалось сохранить свод:\n{e}")


# --------- API ---------

def create_page(parent) -> tk.Frame:
    page = BudgetAnalysisPage(parent); page.pack(fill="both", expand=True); return page

def open_budget_analyzer(parent=None):
    if parent is None:
        root = tk.Tk(); root.title("Анализ смет"); root.geometry("1100x740")
        BudgetAnalysisPage(root).pack(fill="both", expand=True); root.mainloop(); return root
    win = tk.Toplevel(parent); win.title("Анализ смет"); win.geometry("1100x740")
    BudgetAnalysisPage(win).pack(fill="both", expand=True); return win

if __name__ == "__main__":
    open_budget_analyzer()
