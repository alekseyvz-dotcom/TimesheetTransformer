# python
import re
import csv
from pathlib import Path
from typing import List, Dict, Any, Optional

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from openpyxl import Workbook, load_workbook


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
    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        self.file_path: Optional[Path] = None
        self.headers: List[str] = []
        self.rows: List[List[Any]] = []
        self.mapping: Dict[str, Optional[int]] = {"total": None, "materials": None, "wages": None}
        self.stats = {"total": 0.0, "materials": 0.0, "wages": 0.0, "other": 0.0}

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
        self.lbl_file.pack(anchor="w", padx=12, pady=(0, 6))

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
            text=("Поддерживаются XLSX (первая строка — заголовки) и CSV (автоопределение ; или ,). "
                  "Колонки распознаются по именам ('Итого','Всего','Материалы','Заработная плата'). "
                  "При необходимости используйте «Настроить соответствие колонок»."),
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
        self.btn_map.config(state=("normal" if ok else "disabled"))
        self.btn_export.config(state=("normal" if ok else "disabled"))
        if not ok:
            messagebox.showwarning("Анализ смет", "Не удалось распознать структуру файла. "
                                                  "Попробуйте настроить соответствие колонок вручную.")

    def _load_file(self, path: Path) -> bool:
        self.headers, self.rows = [], []
        ext = path.suffix.lower()
        try:
            if ext in (".xlsx", ".xlsm"):
                self._parse_xlsx(path)
            elif ext == ".csv":
                self._parse_csv(path)
            else:
                try:
                    self._parse_xlsx(path)
                except Exception:
                    self._parse_csv(path)
        except Exception as e:
            messagebox.showerror("Загрузка сметы", f"Ошибка чтения файла:\n{e}")
            return False

        if not self.headers or not self.rows:
            return False

        self.mapping = self._detect_mapping(self.headers, self.rows)
        self._analyze()
        return True

    def _parse_xlsx(self, path: Path):
        wb = load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        hdr_row_idx = None
        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            cells = [str(c).strip() if c is not None else "" for c in row]
            if sum(1 for c in cells if c) >= 2:
                hdr_row_idx = i
                self.headers = [self._norm_header(c) for c in cells]
                break
        if hdr_row_idx is None:
            raise RuntimeError("Не найдена строка заголовков")
        self.rows = []
        for row in ws.iter_rows(min_row=hdr_row_idx + 1, values_only=True):
            self.rows.append(list(row))

    def _parse_csv(self, path: Path):
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

    # ---------- Расчет ----------
    @staticmethod
    def _norm_header(s: Any) -> str:
        txt = str(s or "").strip()
        txt = txt.replace("\n", " ").replace("\r", " ")
        return re.sub(r"\s+", " ", txt)

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

    def _analyze(self):
        total     = self._sum_column(self.mapping.get("total"))
        materials = self._sum_column(self.mapping.get("materials"))
        wages     = self._sum_column(self.mapping.get("wages"))

        if total <= 0:
            total = materials + wages

        other = max(0.0, total - materials - wages)
        self.stats = {"total": total, "materials": materials, "wages": wages, "other": other}

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
        if not self.headers:
            return
        dlg = ColumnMappingDialog(self, headers=self.headers, cur_map=self.mapping)
        if getattr(dlg, "result", None):
            self.mapping = dlg.result
            self._analyze()

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
