import csv
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import tkinter as tk
from tkinter import ttk, messagebox

from openpyxl import Workbook, load_workbook


class EstimateResourceDecoderPage(tk.Frame):
    RESOURCE_TYPE_MAP = {
        1: "ЗП",
        2: "ЭМ",
        3: "МР",
    }

    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")

        self.file_path: Optional[Path] = None
        self.workbook = None

        self.local_sheet_name: Optional[str] = None
        self.source_sheet_name: Optional[str] = None
        self.etalon_sheet_name: Optional[str] = None
        self.smtres_sheet_name: Optional[str] = None

        self.local_works: List[Dict[str, Any]] = []
        self.rate_resources: Dict[int, List[Dict[str, Any]]] = {}
        self.code_to_rate_ids: Dict[str, List[int]] = {}
        self.decoded_rows: List[Dict[str, Any]] = []

        self.last_report: str = "Отчет пока не сформирован."

        self._build_ui()

    def _build_ui(self):
        header = tk.Frame(self, bg="#f7f7f7")
        header.pack(fill="x", padx=12, pady=(10, 6))

        tk.Label(
            header,
            text="Раскрытие ресурсов расценок",
            font=("Segoe UI", 16, "bold"),
            bg="#f7f7f7"
        ).pack(side="left")

        ctrl = tk.Frame(self, bg="#f7f7f7")
        ctrl.pack(fill="x", padx=12, pady=(0, 8))

        ttk.Button(ctrl, text="Открыть книгу сметы", command=self._open_file).pack(side="left")
        self.btn_decode = ttk.Button(ctrl, text="Раскрыть ресурсы", command=self._decode_all, state="disabled")
        self.btn_decode.pack(side="left", padx=(8, 0))

        self.btn_export = ttk.Button(ctrl, text="Сохранить результат", command=self._export_result, state="disabled")
        self.btn_export.pack(side="left", padx=(8, 0))

        self.btn_report = ttk.Button(ctrl, text="Показать отчет", command=self._show_report, state="disabled")
        self.btn_report.pack(side="left", padx=(8, 0))

        self.lbl_file = tk.Label(self, text="Файл не выбран", fg="#555", bg="#f7f7f7")
        self.lbl_file.pack(anchor="w", padx=12, pady=(0, 2))

        self.lbl_sheets = tk.Label(self, text="", fg="#777", bg="#f7f7f7", justify="left")
        self.lbl_sheets.pack(anchor="w", padx=12, pady=(0, 8))

        filter_frame = tk.Frame(self, bg="#f7f7f7")
        filter_frame.pack(fill="x", padx=12, pady=(0, 6))

        tk.Label(filter_frame, text="Фильтр по позиции:", bg="#f7f7f7").pack(side="left")
        self.var_pos_filter = tk.StringVar()
        ent = ttk.Entry(filter_frame, textvariable=self.var_pos_filter, width=12)
        ent.pack(side="left", padx=(6, 8))
        ent.bind("<KeyRelease>", lambda e: self._fill_tree())

        tk.Label(filter_frame, text="Фильтр по шифру:", bg="#f7f7f7").pack(side="left")
        self.var_code_filter = tk.StringVar()
        ent2 = ttk.Entry(filter_frame, textvariable=self.var_code_filter, width=18)
        ent2.pack(side="left", padx=(6, 8))
        ent2.bind("<KeyRelease>", lambda e: self._fill_tree())

        tk.Label(filter_frame, text="Тип ресурса:", bg="#f7f7f7").pack(side="left")
        self.cmb_type = ttk.Combobox(filter_frame, state="readonly", width=12, values=["Все", "ЗП", "ЭМ", "МР"])
        self.cmb_type.current(0)
        self.cmb_type.pack(side="left", padx=(6, 8))
        self.cmb_type.bind("<<ComboboxSelected>>", lambda e: self._fill_tree())

        wrap = tk.Frame(self)
        wrap.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        cols = (
            "work_num", "work_code", "rate_id", "res_type",
            "res_code", "res_name", "unit", "norm_qty",
            "work_qty", "res_qty", "price", "base_cost"
        )

        self.tree = ttk.Treeview(wrap, columns=cols, show="headings")
        self.tree.pack(side="left", fill="both", expand=True)

        headers = {
            "work_num": "Поз.",
            "work_code": "Шифр",
            "rate_id": "Rate ID",
            "res_type": "Тип",
            "res_code": "Код ресурса",
            "res_name": "Наименование ресурса",
            "unit": "Ед.",
            "norm_qty": "Норма",
            "work_qty": "Кол-во поз.",
            "res_qty": "Расход",
            "price": "Цена",
            "base_cost": "Стоимость"
        }

        widths = {
            "work_num": 60,
            "work_code": 120,
            "rate_id": 70,
            "res_type": 60,
            "res_code": 110,
            "res_name": 420,
            "unit": 70,
            "norm_qty": 90,
            "work_qty": 90,
            "res_qty": 90,
            "price": 90,
            "base_cost": 110
        }

        for c in cols:
            self.tree.heading(c, text=headers[c])
            self.tree.column(c, width=widths[c], anchor="w" if c not in ("norm_qty", "work_qty", "res_qty", "price", "base_cost") else "e")

        yscroll = ttk.Scrollbar(wrap, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll.set)
        yscroll.pack(side="right", fill="y")

    def _open_file(self):
        try:
            from tkinter import filedialog as fd
        except Exception:
            messagebox.showerror("Файл", "Не удалось открыть диалог выбора файла.")
            return

        fname = fd.askopenfilename(
            title="Выберите Excel-книгу со сметой",
            filetypes=[("Excel", "*.xlsx;*.xlsm"), ("Все файлы", "*.*")]
        )
        if not fname:
            return

        self.file_path = Path(fname)
        self.lbl_file.config(text=f"Файл: {self.file_path}")

        try:
            self._load_workbook()
            self.btn_decode.config(state="normal")
            self.btn_report.config(state="normal")
            messagebox.showinfo("Готово", "Книга загружена. Теперь можно выполнить раскрытие ресурсов.")
        except Exception as e:
            self.btn_decode.config(state="disabled")
            self.btn_export.config(state="disabled")
            self.btn_report.config(state="disabled")
            messagebox.showerror("Ошибка", f"Не удалось загрузить книгу:\n{e}")

    def _load_workbook(self):
        if not self.file_path:
            raise RuntimeError("Файл не выбран")

        self.workbook = load_workbook(self.file_path, data_only=True)

        sheetnames = self.workbook.sheetnames
        self.local_sheet_name = self._detect_local_sheet(sheetnames)
        self.source_sheet_name = self._detect_source_sheet(sheetnames)
        self.etalon_sheet_name = self._detect_etalon_sheet(sheetnames)
        self.smtres_sheet_name = self._detect_smtres_sheet(sheetnames)

        info = [
            f"Локальная смета: {self.local_sheet_name or 'не найдено'}",
            f"Sourse: {self.source_sheet_name or 'не найдено'}",
            f"EtalonRes: {self.etalon_sheet_name or 'не найдено'}",
            f"SmtRes: {self.smtres_sheet_name or 'не найдено'}",
        ]
        self.lbl_sheets.config(text="\n".join(info))

    def _detect_local_sheet(self, names: List[str]) -> Optional[str]:
        for name in names:
            ws = self.workbook[name]
            if self._sheet_has_local_smeta_marker(ws):
                return name
        return names[0] if names else None

    def _detect_source_sheet(self, names: List[str]) -> Optional[str]:
        for name in names:
            low = name.lower()
            if "sourse" in low or "source" in low:
                return name
        return None

    def _detect_etalon_sheet(self, names: List[str]) -> Optional[str]:
        for name in names:
            low = name.lower()
            if "etalon" in low:
                return name
        return None

    def _detect_smtres_sheet(self, names: List[str]) -> Optional[str]:
        for name in names:
            low = name.lower()
            if "smtres" in low:
                return name
        return None

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
    def _s(v: Any) -> str:
        return str(v or "").strip()

    @staticmethod
    def _f(v: Any) -> Optional[float]:
        if v is None or v == "":
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace(" ", "").replace("\u00A0", "").replace(",", ".")
        s = re.sub(r"[^0-9.\-]", "", s)
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None

    @staticmethod
    def _is_main_pos(v: Any) -> bool:
        if isinstance(v, int):
            return True
        if isinstance(v, float) and float(v).is_integer():
            return True
        s = str(v or "").strip()
        return bool(re.fullmatch(r"\d+", s))

    def _decode_all(self):
        if not self.workbook:
            return

        try:
            self.local_works = self._parse_local_works()
            self.rate_resources = self._parse_rate_resources()
            self.code_to_rate_ids = self._build_code_to_rate_map()
            self.decoded_rows = self._decode_rows()

            self._fill_tree()
            self.btn_export.config(state="normal")

            messagebox.showinfo(
                "Готово",
                f"Раскрытие выполнено.\n"
                f"Позиции: {len(self.local_works)}\n"
                f"Раскрытых строк ресурсов: {len(self.decoded_rows)}"
            )
        except Exception as e:
            messagebox.showerror("Ошибка раскрытия", str(e))

    def _parse_local_works(self) -> List[Dict[str, Any]]:
        if not self.local_sheet_name:
            raise RuntimeError("Не найден лист локальной сметы.")

        ws = self.workbook[self.local_sheet_name]
        works: List[Dict[str, Any]] = []
        current_section = ""
        current_work: Optional[Dict[str, Any]] = None
        header_found = False

        for row in ws.iter_rows(values_only=True):
            row = list(row)

            if not any(v is not None and str(v).strip() for v in row):
                continue

            first = self._s(row[0]) if len(row) > 0 else ""
            third = self._s(row[2]) if len(row) > 2 else ""

            if first == "№ п/п" or third == "Наименование работ и затрат":
                header_found = True
                continue

            if not header_found:
                continue

            if first.startswith("Раздел:"):
                current_section = first.replace("Раздел:", "").strip()
                continue

            if self._is_main_pos(row[0] if len(row) > 0 else None):
                pos_num = self._s(row[0])
                code = self._s(row[1] if len(row) > 1 else "")
                name = self._s(row[2] if len(row) > 2 else "")
                unit = self._s(row[3] if len(row) > 3 else "")
                qty = self._f(row[4] if len(row) > 4 else None)

                if not name or name.lower().startswith("итого"):
                    continue

                current_work = {
                    "pos_num": pos_num,
                    "work_code": code,
                    "name": name,
                    "unit": unit,
                    "qty": qty or 0.0,
                    "section": current_section,
                    "zp_base": None,
                    "em_base": None,
                    "mr_base": None,
                    "zpm_base": None,
                }
                works.append(current_work)
                continue

            if current_work is None:
                continue

            if third == "ЗП":
                current_work["zp_base"] = self._f(row[8] if len(row) > 8 else None)
            elif third == "ЭМ":
                current_work["em_base"] = self._f(row[8] if len(row) > 8 else None)
            elif third == "МР":
                current_work["mr_base"] = self._f(row[8] if len(row) > 8 else None)
            elif third == "в т.ч. ЗПМ":
                current_work["zpm_base"] = self._f(row[8] if len(row) > 8 else None)

        return works

    def _parse_rate_resources(self) -> Dict[int, List[Dict[str, Any]]]:
        sheet_name = self.etalon_sheet_name or self.smtres_sheet_name
        if not sheet_name:
            raise RuntimeError("Не найден лист EtalonRes/SmtRes.")

        ws = self.workbook[sheet_name]
        result: Dict[int, List[Dict[str, Any]]] = {}

        for row in ws.iter_rows(values_only=True):
            vals = list(row)
            if not any(v is not None and str(v).strip() for v in vals):
                continue

            rate_id = self._try_parse_int(vals[0] if len(vals) > 0 else None)
            if rate_id is None:
                continue

            resource_type = self._try_parse_int(vals[7] if len(vals) > 7 else None)
            resource_code = self._s(vals[8] if len(vals) > 8 else "")
            resource_name = self._s(vals[10] if len(vals) > 10 else "")
            unit = self._extract_unit(vals)
            norm_qty = self._extract_norm_qty(vals)
            price = self._extract_price(vals)

            if resource_type not in (1, 2, 3):
                continue
            if not resource_code and not resource_name:
                continue
            if norm_qty is None:
                continue

            result.setdefault(rate_id, []).append({
                "rate_id": rate_id,
                "resource_type_id": resource_type,
                "resource_type": self.RESOURCE_TYPE_MAP.get(resource_type, str(resource_type)),
                "resource_code": resource_code,
                "resource_name": resource_name,
                "unit": unit,
                "norm_qty": norm_qty,
                "price": price,
            })

        return result

    def _build_code_to_rate_map(self) -> Dict[str, List[int]]:
        code_map: Dict[str, List[int]] = {}

        if self.source_sheet_name:
            ws = self.workbook[self.source_sheet_name]
            for row in ws.iter_rows(values_only=True):
                vals = list(row)
                if not any(v is not None and str(v).strip() for v in vals):
                    continue

                rate_id = None
                code = None

                for v in vals:
                    s = self._s(v)
                    if rate_id is None:
                        rate_id = self._try_parse_int(v)
                    if code is None and self._looks_like_rate_code(s):
                        code = s

                if rate_id is not None and code:
                    code_map.setdefault(code, [])
                    if rate_id not in code_map[code]:
                        code_map[code].append(rate_id)

        if code_map:
            return code_map

        for sheet_name in [self.etalon_sheet_name, self.smtres_sheet_name]:
            if not sheet_name:
                continue
            ws = self.workbook[sheet_name]
            for row in ws.iter_rows(values_only=True):
                vals = list(row)
                if not any(v is not None and str(v).strip() for v in vals):
                    continue

                rate_id = self._try_parse_int(vals[0] if len(vals) > 0 else None)
                if rate_id is None:
                    continue

                found_codes = [self._s(v) for v in vals if self._looks_like_rate_code(self._s(v))]
                for code in found_codes:
                    code_map.setdefault(code, [])
                    if rate_id not in code_map[code]:
                        code_map[code].append(rate_id)

        return code_map

    def _decode_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        report_lines = []

        report_lines.append("ОТЧЕТ ПО РАСКРЫТИЮ РЕСУРСОВ")
        report_lines.append("=" * 100)

        matched = 0
        unmatched = 0

        for work in self.local_works:
            code = work.get("work_code") or ""
            qty = work.get("qty") or 0.0

            if not code:
                unmatched += 1
                continue

            rate_ids = self.code_to_rate_ids.get(code, [])
            if not rate_ids:
                unmatched += 1
                report_lines.append(f"[NO RATE_ID] Поз. {work['pos_num']} | {code} | {work['name']}")
                continue

            rate_id = rate_ids[0]
            resources = self.rate_resources.get(rate_id, [])
            if not resources:
                unmatched += 1
                report_lines.append(f"[NO RESOURCES] Поз. {work['pos_num']} | {code} | rate_id={rate_id}")
                continue

            matched += 1
            report_lines.append(f"[OK] Поз. {work['pos_num']} | {code} -> rate_id={rate_id} | ресурсов={len(resources)}")

            for res in resources:
                norm_qty = res.get("norm_qty") or 0.0
                price = res.get("price")
                res_qty = norm_qty * qty
                base_cost = (res_qty * price) if isinstance(price, (int, float)) else None

                rows.append({
                    "work_num": work["pos_num"],
                    "work_code": code,
                    "work_name": work["name"],
                    "work_qty": qty,
                    "rate_id": rate_id,
                    "resource_type": res["resource_type"],
                    "resource_code": res["resource_code"],
                    "resource_name": res["resource_name"],
                    "unit": res["unit"],
                    "norm_qty": norm_qty,
                    "resource_qty": res_qty,
                    "price": price,
                    "base_cost": base_cost,
                })

        report_lines.append("")
        report_lines.append(f"Всего позиций: {len(self.local_works)}")
        report_lines.append(f"Сопоставлено: {matched}")
        report_lines.append(f"Не сопоставлено: {unmatched}")

        self.last_report = "\n".join(report_lines)
        return rows

    def _fill_tree(self):
        for item in self.tree.get_children():
            self.tree.delete(item)

        pos_filter = self.var_pos_filter.get().strip().lower()
        code_filter = self.var_code_filter.get().strip().lower()
        type_filter = self.cmb_type.get().strip()

        for row in self.decoded_rows:
            if pos_filter and pos_filter not in str(row["work_num"]).lower():
                continue
            if code_filter and code_filter not in str(row["work_code"]).lower():
                continue
            if type_filter != "Все" and row["resource_type"] != type_filter:
                continue

            self.tree.insert(
                "",
                "end",
                values=(
                    row["work_num"],
                    row["work_code"],
                    row["rate_id"],
                    row["resource_type"],
                    row["resource_code"],
                    row["resource_name"],
                    row["unit"],
                    self._fmt_num(row["norm_qty"]),
                    self._fmt_num(row["work_qty"]),
                    self._fmt_num(row["resource_qty"]),
                    self._fmt_num(row["price"]),
                    self._fmt_num(row["base_cost"]),
                )
            )

    def _show_report(self):
        win = tk.Toplevel(self)
        win.title("Отчет по раскрытию ресурсов")
        win.geometry("1000x700")

        wrap = tk.Frame(win)
        wrap.pack(fill="both", expand=True, padx=10, pady=10)

        txt = tk.Text(wrap, wrap="word", font=("Consolas", 9))
        txt.pack(side="left", fill="both", expand=True)

        scr = ttk.Scrollbar(wrap, orient="vertical", command=txt.yview)
        scr.pack(side="right", fill="y")
        txt.configure(yscrollcommand=scr.set)

        txt.insert("1.0", self.last_report or "")
        txt.configure(state="disabled")

    def _export_result(self):
        if not self.decoded_rows:
            messagebox.showwarning("Экспорт", "Нет данных для сохранения.")
            return

        try:
            from tkinter import filedialog as fd
        except Exception:
            messagebox.showerror("Экспорт", "Не удалось открыть диалог сохранения.")
            return

        fname = fd.asksaveasfilename(
            title="Сохранить раскрытие ресурсов",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv")]
        )
        if not fname:
            return

        out = Path(fname)

        headers = [
            "Поз.", "Шифр расценки", "Наименование работы", "Кол-во позиции",
            "Rate ID", "Тип ресурса", "Код ресурса", "Наименование ресурса",
            "Ед.", "Норма", "Расход по позиции", "Цена", "Базовая стоимость"
        ]

        try:
            if out.suffix.lower() == ".csv":
                with open(out, "w", encoding="utf-8-sig", newline="") as f:
                    w = csv.writer(f, delimiter=";")
                    w.writerow(headers)
                    for r in self.decoded_rows:
                        w.writerow([
                            r["work_num"], r["work_code"], r["work_name"], r["work_qty"],
                            r["rate_id"], r["resource_type"], r["resource_code"], r["resource_name"],
                            r["unit"], r["norm_qty"], r["resource_qty"], r["price"], r["base_cost"]
                        ])
            else:
                wb = Workbook()
                ws = wb.active
                ws.title = "Раскрытие ресурсов"
                ws.append(headers)

                for r in self.decoded_rows:
                    ws.append([
                        r["work_num"], r["work_code"], r["work_name"], r["work_qty"],
                        r["rate_id"], r["resource_type"], r["resource_code"], r["resource_name"],
                        r["unit"], r["norm_qty"], r["resource_qty"], r["price"], r["base_cost"]
                    ])

                wb.save(out)

            messagebox.showinfo("Экспорт", f"Файл сохранен:\n{out}")
        except Exception as e:
            messagebox.showerror("Экспорт", f"Ошибка сохранения:\n{e}")

    @staticmethod
    def _fmt_num(v: Any) -> str:
        if v is None:
            return ""
        try:
            return f"{float(v):,.4f}".replace(",", " ").replace(".", ",")
        except Exception:
            return str(v)

    @staticmethod
    def _try_parse_int(v: Any) -> Optional[int]:
        if v is None:
            return None
        if isinstance(v, int):
            return v
        if isinstance(v, float) and float(v).is_integer():
            return int(v)
        s = str(v).strip()
        if re.fullmatch(r"\d+", s):
            return int(s)
        return None

    @staticmethod
    def _looks_like_rate_code(s: str) -> bool:
        s = (s or "").strip()
        return bool(re.fullmatch(r"\d+\.\d+(?:-\d+)+", s))

    def _extract_unit(self, vals: List[Any]) -> str:
        for i in [14, 15, 13, 16]:
            if i < len(vals):
                s = self._s(vals[i])
                if s and len(s) <= 20:
                    return s
        return ""

    def _extract_norm_qty(self, vals: List[Any]) -> Optional[float]:
        for i in [23, 24, 22, 34]:
            if i < len(vals):
                f = self._f(vals[i])
                if f is not None:
                    return f
        return None

    def _extract_price(self, vals: List[Any]) -> Optional[float]:
        for i in [25, 26, 27]:
            if i < len(vals):
                f = self._f(vals[i])
                if f is not None and f >= 0:
                    return f
        return None


def create_page(parent) -> tk.Frame:
    page = EstimateResourceDecoderPage(parent)
    page.pack(fill="both", expand=True)
    return page


def open_estimate_resource_decoder(parent=None):
    if parent is None:
        root = tk.Tk()
        root.title("Раскрытие ресурсов расценок")
        root.geometry("1400x800")
        EstimateResourceDecoderPage(root).pack(fill="both", expand=True)
        root.mainloop()
        return root

    win = tk.Toplevel(parent)
    win.title("Раскрытие ресурсов расценок")
    win.geometry("1400x800")
    EstimateResourceDecoderPage(win).pack(fill="both", expand=True)
    return win


if __name__ == "__main__":
    open_estimate_resource_decoder()
