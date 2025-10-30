# python
import re
import csv
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from openpyxl import Workbook, load_workbook


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
    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        self.file_path: Optional[Path] = None

        # Общий режим
        self.headers: List[str] = []
        self.rows: List[List[Any]] = []
        self.mapping: Dict[str, Optional[int]] = {"total": None, "materials": None, "wages": None}

        # Smeta-режим (специализированный парсер Smeta.RU)
        self.mode: str = "generic"  # "smeta" | "generic"
        self.smeta_sheet_name: Optional[str] = None
        self.smeta_name_col: Optional[int] = None   # индекс колонки "Наименование работ и затрат" (0-based)
        self.smeta_cost_col: Optional[int] = None   # индекс колонки "ВСЕГО затрат, руб." (0-based)
        self.smeta_data_rows: List[List[Any]] = []  # чистые строки данных после заголовка

        # Итоги
        self.stats = {"total": 0.0, "materials": 0.0, "wages": 0.0, "other": 0.0}

        # UI
        header = tk.Frame(self, bg="#f7f7f7")
        header.pack(fill="x", padx=12, pady=(10, 6))
        tk.Label(header, text="Анализ смет", font=("Segoe UI", 16, "bold"), bg="#f7f7f7").pack(side="left")

        ctrl = tk.Frame(self, bg="#f7f7f7")
        ctrl.pack(fill="x", padx=12, pady=(0, 8))
        ttk.Button(ctrl, text="Открыть смету (XLSX/CSV)", command=self._open_file).pack(side="left")
        ttk.Button(ctrl, text="Настроить соответствие колонок", command=self._open_mapping, state="disabled").pack(side="left", padx=(8, 0))
        ttk.Button(ctrl, text="Сохранить свод", command=self._export_summary, state="disabled").pack(side="left", padx=(8, 0))
        self.btn_map = ctrl.winfo_children()[1]
        self.btn_export = ctrl.winfo_children()[2]

        self.lbl_file = tk.Label(self, text="Файл не выбран", fg="#555", bg="#f7f7f7")
        self.lbl_file.pack(anchor="w", padx=12, pady=(0, 2))

        self.lbl_sheet = tk.Label(self, text="", fg="#777", bg="#f7f7f7")
        self.lbl_sheet.pack(anchor="w", padx=12, pady=(0, 6))

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

        hint = tk.Label(
            self,
            text=("Поддержка Smeta.RU: выбирается лист с “ЛОКАЛЬНАЯ СМЕТА”, берутся 11-колоночные строки.\n"
                  "Суммы — из 10-й колонки; ЗП/в т.ч. ЗПМ — в «Заработная плата», «Материалы/МАТ» — в «Материалы», "
                  "остальное — в «Прочие». Если автоопределение не сработало — используйте ручное сопоставление колонок."),
            fg="#666", bg="#f7f7f7", justify="left", wraplength=980
        )
        hint.pack(fill="x", padx=12, pady=(0, 12))

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
        self.btn_map.config(state=("normal" if (ok and self.mode == "generic") else "disabled"))
        self.btn_export.config(state=("normal" if ok else "disabled"))
        if not ok:
            messagebox.showwarning("Анализ смет", "Не удалось распознать структуру файла. "
                                                  "Попробуйте настроить соответствие колонок вручную (для CSV/XLSX с таблицей).")

    def _load_file(self, path: Path) -> bool:
        self.mode = "generic"
        self.headers, self.rows = [], []
        self.smeta_sheet_name = None
        self.smeta_name_col = None
        self.smeta_cost_col = None
        self.smeta_data_rows = []
        self.lbl_sheet.config(text="")

        ext = path.suffix.lower()
        try:
            if ext in (".xlsx", ".xlsm"):
                if self._parse_xlsx_smeta_ru(path):
                    self.mode = "smeta"
                    self._analyze_smeta()
                    return True
                # если не похоже на смета.ру — пробуем общий режим
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
                # Попытка xlsx → csv
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

    # ---------- Smeta.RU режим (лист «ЛОКАЛЬНАЯ СМЕТА», 11 колонок) ----------

    def _parse_xlsx_smeta_ru(self, path: Path) -> bool:
        """
        Ищем лист, где в верхних 30 строках встречается «ЛОКАЛЬНАЯ СМЕТА».
        На нём ищем шапку 11-колоночной таблицы с «Наименование работ и затрат» и «ВСЕГО затрат».
        Дальше читаем строки до «Итого по локальной смете».
        """
        wb = load_workbook(path, read_only=True, data_only=True)
        target_ws = None
        for ws in wb.worksheets:
            if self._sheet_has_local_smeta_marker(ws):
                target_ws = ws
                break
        if target_ws is None:
            return False

        # Найти строку заголовков таблицы и индексы нужных колонок
        hdr_row_idx, name_col, cost_col = self._find_table_header(target_ws)
        if hdr_row_idx is None or name_col is None or cost_col is None:
            return False

        # Считать данные до "Итого по локальной смете"
        data_rows = []
        for row in target_ws.iter_rows(min_row=hdr_row_idx + 1, values_only=True):
            cells = list(row)
            if not any(c is not None and str(c).strip() for c in cells):
                # пустая строка — пропустим, но не завершаем
                continue
            # стоп-маркеры итога
            name_cell = self._str(cells[name_col])
            if "итого по локальной смете" in name_cell.lower():
                break
            data_rows.append(cells)

        # Сохраняем параметры smeta-режима
        self.smeta_sheet_name = target_ws.title
        self.smeta_name_col = name_col
        self.smeta_cost_col = cost_col
        self.smeta_data_rows = data_rows

        # Найдём явный итог «Итого по локальной смете»
        total = self._find_local_total(target_ws, hdr_row_idx, name_col, cost_col)
        self.stats = {"total": total or 0.0, "materials": 0.0, "wages": 0.0, "other": 0.0}

        self.lbl_sheet.config(text=f"Лист: {self.smeta_sheet_name} (режим Smeta.RU)")
        return True

    @staticmethod
    def _sheet_has_local_smeta_marker(ws) -> bool:
        try:
            for r, row in enumerate(ws.iter_rows(min_row=1, max_row=30, values_only=True), start=1):
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

    def _find_table_header(self, ws) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """
        Ищем строку, где встречаются как минимум:
        - «Наименование работ и затрат»
        - «ВСЕГО затрат» (или «всего затрат, руб.»)
        Также допускаем строку с нумерацией 1..11.
        """
        name_col = None
        cost_col = None
        hdr_row_idx = None

        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            vals = [self._normalize_header_text(v) for v in row]
            if not any(vals):
                continue

            # Попытка 1: полноценные заголовки
            if any("наименование работ" in v and "затрат" in v for v in vals) and any(
                "всего" in v and "затрат" in v for v in vals
            ):
                hdr_row_idx = i
                for idx, v in enumerate(vals):
                    if "наименование работ" in v and "затрат" in v and name_col is None:
                        name_col = idx
                    if "всего" in v and "затрат" in v and cost_col is None:
                        cost_col = idx
                break

            # Попытка 2: строка 1..11 (цифры)
            only_digits = [str(v).strip() for v in row if v is not None]
            if only_digits and all(x.isdigit() for x in only_digits):
                # Часто name_col=2 (3-я), cost_col=9 (10-я) при 1-based
                hdr_row_idx = i
                name_col = 2
                cost_col = 9
                break

        return hdr_row_idx, name_col, cost_col

    def _find_local_total(self, ws, start_row: int, name_col: int, cost_col: int) -> Optional[float]:
        """
        Находим строку «Итого по локальной смете» ниже шапки и берём сумму из cost_col.
        Если нет — вернём None.
        """
        for row in ws.iter_rows(min_row=start_row + 1, values_only=True):
            cells = list(row)
            name = self._str(cells[name_col]) if name_col < len(cells) else ""
            if "итого по локальной смете" in name.lower():
                if cost_col < len(cells):
                    return self._to_number(cells[cost_col])
                # иногда сумма стоит в соседней колонке (сместилась) — проверим ещё +-1
                for j in (cost_col - 1, cost_col + 1):
                    if 0 <= j < len(cells):
                        v = self._to_number(cells[j])
                        if isinstance(v, float):
                            return v
        return None

    # ---------- Аналитика для Smeta.RU ----------

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
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            if "," in s:
                s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None

    def _analyze_smeta(self):
        if self.smeta_name_col is None or self.smeta_cost_col is None:
            raise RuntimeError("Не заданы индексы колонок для сметы.")

        wages_sum = 0.0
        mats_sum = 0.0

        # Паттерны распознавания по 3-й колонке (наименование затрат)
        def is_wages(name: str) -> bool:
            n = name.lower()
            return (
                n == "зп"
                or "заработ" in n
                or "з/п" in n
                or "зпм" in n
                or "в т.ч. зпм" in n
                or "оплата труда" in n
            )

        def is_materials(name: str) -> bool:
            n = name.lower()
            # возможные варианты для материалов
            return (
                n in ("м", "мат", "мат.", "материалы")
                or "материа" in n
                or "(м)" in n
            )

        for row in self.smeta_data_rows:
            if self.smeta_name_col >= len(row):
                continue
            name = self._str(row[self.smeta_name_col])
            if not name:
                continue
            # суммы в 10-й колонке (1-based) → индекс 9; но у нас вычисленный cost_col
            val = self._to_number(row[self.smeta_cost_col]) if self.smeta_cost_col < len(row) else None
            if not isinstance(val, float):
                continue

            if is_wages(name):
                wages_sum += val
            elif is_materials(name):
                mats_sum += val
            else:
                # прочие (ЭМ, НР, СП и т.п.) учтём в "прочие" через разницу
                pass

        total = float(self.stats.get("total") or 0.0)
        if total <= 0:
            # Если явный «Итого по локальной смете» не найден — подстрахуемся суммой по всем строкам с именем
            total = 0.0
            for row in self.smeta_data_rows:
                if self.smeta_name_col < len(row) and self._str(row[self.smeta_name_col]):
                    v = self._to_number(row[self.smeta_cost_col]) if self.smeta_cost_col < len(row) else None
                    if isinstance(v, float):
                        total += v

        other = max(0.0, total - mats_sum - wages_sum)

        self.stats = {"total": total, "materials": mats_sum, "wages": wages_sum, "other": other}
        self._render_stats()

    # ---------- Общий режим (XLSX/CSV) ----------

    def _parse_xlsx_generic(self, path: Path):
        wb = load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        # Первая непустая строка — заголовки
        hdr_row_idx = None
        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            cells = [self._str(c) for c in row]
            if sum(1 for c in cells if c) >= 2:
                hdr_row_idx = i
                self.headers = [self._norm_header(c) for c in cells]
                break
        if hdr_row_idx is None:
            raise RuntimeError("Не найдена строка заголовков")
        self.rows = []
        for row in ws.iter_rows(min_row=hdr_row_idx + 1, values_only=True):
            self.rows.append(list(row))
        self.lbl_sheet.config(text=f"Лист: {ws.title} (общий режим)")

    def _parse_csv_generic(self, path: Path):
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096)
            f.seek(0)
            try:
                sniffer = csv.Sniffer()
                dialect = sniffer.sniff(sample, delimiters=";,")
            except Exception:
                class D: delimiter = ";"
                dialect = D()
            reader = csv.reader(f, dialect=dialect)
            rows = list(reader)
        if not rows:
            raise RuntimeError("CSV пустой")
        hdr_idx = None
        for i, row in enumerate(rows):
            if any((c or "").strip() for c in row):
                hdr_idx = i
                break
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
            res = []
            for i, h in enumerate(hlow):
                if any(p in h for p in patterns):
                    res.append(i)
            return res

        cand_total = find_candidates(["итого", "всего", "стоим", "смет", "общая стоимость"])
        cand_mat   = find_candidates(["матер", "материа"])
        cand_wage  = find_candidates(["зараб", "оплата труда", "з/п", "зп", "труд"])

        def best_index(candidates: List[int]) -> Optional[int]:
            best_i, best_sum = None, -1.0
            for idx in candidates:
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
            "total":     best_index(cand_total),
            "materials": best_index(cand_mat),
            "wages":     best_index(cand_wage),
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
        self._render_stats()

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

    # ---------- Действия ----------
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
            else:
                wb = Workbook()
                ws = wb.active
                ws.title = "Анализ сметы"
                ws.append(["Показатель", "Сумма (руб.)", "Доля"])
                ws.append(["Строительные затраты (Итого)", self.stats["total"], "100%"])
                ws.append(["Материалы", self.stats["materials"], self._fmt_pct(self._safe_pct(self.stats['materials']))])
                ws.append(["Заработная плата", self.stats["wages"], self._fmt_pct(self._safe_pct(self.stats['wages']))])
                ws.append(["Прочие", self.stats["other"], self._fmt_pct(self._safe_pct(self.stats['other']))])
                ws.column_dimensions["A"].width = 36
                ws.column_dimensions["B"].width = 18
                ws.column_dimensions["C"].width = 10
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
        root.geometry("1000x700")
        BudgetAnalysisPage(root).pack(fill="both", expand=True)
        root.mainloop()
        return root
    win = tk.Toplevel(parent)
    win.title("Анализ смет")
    win.geometry("1000x700")
    BudgetAnalysisPage(win).pack(fill="both", expand=True)
    return win

if __name__ == "__main__":
    open_budget_analyzer()
