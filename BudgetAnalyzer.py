# BudgetAnalyzer.py
# Анализ смет: поддержка смет Smeta.RU (лист «ЛОКАЛЬНАЯ СМЕТА», 11 колонок).
# Логика (Smeta.RU режим):
# - Введены детализированные категории затрат: ЗП, ЭМ (экспл. машин), МР (материалы), НР (накладные расходы), СП (сметная прибыль), НР и СП от ЗПМ.
# - Коррекция ЭМ: ЭМ = ЭМ_гросс - в т.ч. ЗПМ (во избежание двойного учета ЗПМ).
# - Материалы (МР) рассчитываются по inline-правилу (позиция с ценой, не проценты/не трудочасы).
# - Диаграмма удалена, расшифровка строк занимает полную ширину и включает номера позиций и шифры расценок.
# - Добавлена функция начисления НДС 20%

import re
import csv
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from openpyxl import Workbook, load_workbook

# matplotlib и диаграмма удалены, импорты не нужны.
MPL_AVAILABLE = False # Выключаем флаг, даже если библиотека есть


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

        # Оставляем старые поля для generic режима, хотя он менее детализирован
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
                # Индексы в заголовках начинаются с 0
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
    
    # Новые ключи для детализированного анализа Smeta.RU
    COST_KEYS = ["zp", "em", "mr", "nr", "sp", "nr_sp_zpm"]
    REFERENCE_KEYS = ["zpm_incl"]
    
    DISPLAY_CATEGORIES = [
        ("zp", "Заработная плата (ЗП)"),
        ("em", "Эксплуатация машин (ЭМ)"),
        ("mr", "Материалы (МР)"),
        ("nr", "Накладные расходы (НР)"),
        ("sp", "Сметная прибыль (СП)"),
        ("nr_sp_zpm", "НР и СП от ЗПМ"),
    ]
    
    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        self.file_path: Optional[Path] = None

        self.headers: List[str] = []
        self.rows: List[List[Any]] = []
        self.mapping: Dict[str, Optional[int]] = {"total": None, "materials": None, "wages": None}

        self.mode: str = "generic"
        self.smeta_sheet_name: Optional[str] = None
        self.smeta_name_col: Optional[int] = None
        self.smeta_cost_cols: List[int] = []
        self.smeta_data_rows: List[List[Any]] = []

        # stats_base теперь хранит детализированные данные до НДС
        self.stats_base: Dict[str, float] = {k: 0.0 for k in self.COST_KEYS + self.REFERENCE_KEYS + ["total"]}
        self.stats: Dict[str, float] = self.stats_base.copy() # После НДС
        self.breakdown_rows: List[Dict[str, Any]] = []
        
        self.vat_enabled = tk.BooleanVar(value=False)

        header = tk.Frame(self, bg="#f7f7f7")
        header.pack(fill="x", padx=12, pady=(10, 6))
        tk.Label(header, text="Анализ смет", font=("Segoe UI", 16, "bold"), bg="#f7f7f7").pack(side="left")

        ctrl = tk.Frame(self, bg="#f7f7f7")
        ctrl.pack(fill="x", padx=12, pady=(0, 8))
        self.btn_open = ttk.Button(ctrl, text="Открыть смету (XLSX/CSV)", command=self._open_file)
        self.btn_open.pack(side="left")
        self.btn_map = ttk.Button(ctrl, text="Настроить соответствие колонок (Общий режим)", command=self._open_mapping, state="disabled")
        self.btn_map.pack(side="left", padx=(8, 0))
        self.btn_export = ttk.Button(ctrl, text="Сохранить свод", command=self._export_summary, state="disabled")
        self.btn_export.pack(side="left", padx=(8, 0))
        
        self.chk_vat = ttk.Checkbutton(ctrl, text="Начислить НДС 20%", variable=self.vat_enabled, command=self._on_vat_toggle)
        self.chk_vat.pack(side="left", padx=(16, 0))

        self.lbl_file = tk.Label(self, text="Файл не выбран", fg="#555", bg="#f7f7f7")
        self.lbl_file.pack(anchor="w", padx=12, pady=(0, 2))

        self.lbl_sheet = tk.Label(self, text="", fg="#777", bg="#f7f7f7")
        self.lbl_sheet.pack(anchor="w", padx=12, pady=(0, 6))

        # --------------------- Таблица показателей ---------------------
        card = tk.Frame(self, bg="#ffffff", bd=1, relief="solid")
        card.pack(fill="x", padx=12, pady=(0, 10))

        grid = tk.Frame(card, bg="#ffffff")
        grid.pack(fill="x", padx=12, pady=12)

        tk.Label(grid, text="Показатель", font=("Segoe UI", 10, "bold"), bg="#ffffff").grid(row=0, column=0, sticky="w")
        tk.Label(grid, text="Сумма (руб.)", font=("Segoe UI", 10, "bold"), bg="#ffffff").grid(row=0, column=1, sticky="e")
        tk.Label(grid, text="Доля", font=("Segoe UI", 10, "bold"), bg="#ffffff").grid(row=0, column=2, sticky="e")

        row_idx = 1
        self._metric_rows = {}
        
        # 1. Итого (для расчета процентов)
        self._row_total = self._add_metric_row(grid, row_idx, "Строительные затраты (Итого)")
        row_idx += 1
        
        # 2-7. Детализированные категории
        for key, title in self.DISPLAY_CATEGORIES:
            self._metric_rows[key] = self._add_metric_row(grid, row_idx, title)
            row_idx += 1
            
        # 8. Справочная информация ЗПМ
        self._row_zpm_ref = self._add_metric_row(grid, row_idx, "в т.ч. ЗПМ (Справочно)")
        self._row_zpm_ref["label"].config(fg="#888888") # Серая строка
        row_idx += 1

        # 9. НДС
        self._row_vat = self._add_metric_row(grid, row_idx, "НДС 20%")
        self._row_vat["label"].config(text="НДС 20%", bg="#ffffff", fg="#d32f2f")
        self._row_vat["label"].grid(row=row_idx, column=0, sticky="w", pady=3)
        self._row_vat["label"].grid_remove()
        self._row_vat["val"].grid_remove()
        self._row_vat["pct"].grid_remove()
        row_idx += 1
        
        # 10. Всего с НДС
        self._row_total_vat = self._add_metric_row(grid, row_idx, "Всего с НДС")
        self._row_total_vat["label"].config(text="Всего с НДС", bg="#ffffff", font=("Segoe UI", 10, "bold"), fg="#1976d2")
        self._row_total_vat["label"].grid(row=row_idx, column=0, sticky="w", pady=3)
        self._row_total_vat["label"].grid_remove()
        self._row_total_vat["val"].grid_remove()
        self._row_total_vat["pct"].grid_remove()

        for c in range(3):
            grid.grid_columnconfigure(c, weight=1)

        hint_text = ("Smeta.RU: лист «ЛОКАЛЬНАЯ СМЕТА». Расчет ведется по детализированным статьям (ЗП, ЭМ, МР, НР, СП).\n"
                     "Эксплуатация машин (ЭМ) автоматически корректируется на сумму 'в т.ч. ЗПМ' для избежания двойного учета.\n"
                     "Чекбокс «Начислить НДС 20%» увеличивает все суммы на 20%.")
        hint = tk.Label(self, text=hint_text, fg="#666", bg="#f7f7f7", justify="left", wraplength=980)
        hint.pack(fill="x", padx=12, pady=(0, 10))

        # --------------------- Расшифровка строк (занимает полную ширину) ---------------------
        
        main_split = tk.Frame(self, bg="#f7f7f7")
        main_split.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        # LEFT frame теперь заполняет всю область
        left = tk.Frame(main_split, bg="#f7f7f7")
        left.pack(side="left", fill="both", expand=True, padx=(0, 0)) # Убираем правый паддинг

        tk.Label(left, text="Расшифровка строк", font=("Segoe UI", 11, "bold"), bg="#f7f7f7").pack(anchor="w", pady=(0, 6))

        # Фильтры
        flt = tk.Frame(left, bg="#f7f7f7")
        flt.pack(anchor="w", pady=(0, 6))
        self.var_show_mat = tk.BooleanVar(value=True) # МР
        self.var_show_wag = tk.BooleanVar(value=True) # ЗП, ЭМ, в т.ч. ЗПМ
        self.var_show_oth = tk.BooleanVar(value=True) # НР, СП, НР_СП_ЗПМ
        
        ttk.Checkbutton(flt, text="Материалы (МР)", variable=self.var_show_mat, command=self._fill_breakdown_table).pack(side="left", padx=(0, 8))
        ttk.Checkbutton(flt, text="Трудозатраты/Машины (ЗП, ЭМ)", variable=self.var_show_wag, command=self._fill_breakdown_table).pack(side="left", padx=(0, 8))
        ttk.Checkbutton(flt, text="Накладные/Прибыль (НР, СП)", variable=self.var_show_oth, command=self._fill_breakdown_table).pack(side="left")

        tree_wrap = tk.Frame(left)
        tree_wrap.pack(fill="both", expand=True)

        # Новые колонки: Номер позиции и Шифр расценки
        cols = ("pos_num", "rate_code", "category", "name", "amount")
        self.tree = ttk.Treeview(tree_wrap, columns=cols, show="headings", height=12)
        
        self.tree.heading("pos_num", text="Поз.")
        self.tree.heading("rate_code", text="Шифр расценки")
        self.tree.heading("category", text="Категория")
        self.tree.heading("name", text="Наименование")
        self.tree.heading("amount", text="Сумма, руб.")
        
        self.tree.column("pos_num", width=60, anchor="w")
        self.tree.column("rate_code", width=140, anchor="w")
        self.tree.column("category", width=180, anchor="w")
        self.tree.column("name", stretch=True, minwidth=250, anchor="w") 
        self.tree.column("amount", width=120, anchor="e")

        yscroll = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll.set)
        self.tree.pack(side="left", fill="both", expand=True)
        yscroll.pack(side="right", fill="y")
        
        # Удалены все упоминания chart_area, _mpl_fig, _mpl_canvas, _tk_canvas.

    def _add_metric_row(self, grid, r, title: str):
        tk.Label(grid, text=title, bg="#ffffff").grid(row=r, column=0, sticky="w", pady=3)
        val_lbl = tk.Label(grid, text="-", bg="#ffffff", anchor="e")
        val_lbl.grid(row=r, column=1, sticky="e", pady=3)
        pct_lbl = tk.Label(grid, text="-", bg="#ffffff", anchor="e")
        pct_lbl.grid(row=r, column=2, sticky="e", pady=3)
        return {"label": tk.Label(grid, text=title, bg="#ffffff"), "val": val_lbl, "pct": pct_lbl}

    def _on_vat_toggle(self):
        if not self.stats_base.get("total"):
            return
        self._apply_vat()
        self._render_stats()
        self._fill_breakdown_table()
        # Диаграмма удалена, _render_chart не вызываем

    def _apply_vat(self):
        multiplier = 1.2 if self.vat_enabled.get() else 1.0
        self.stats = {}
        
        # Применяем множитель ко всем COST_KEYS и REFERENCE_KEYS, а также к общему total
        for key in self.COST_KEYS + self.REFERENCE_KEYS + ["total"]:
            self.stats[key] = self.stats_base.get(key, 0.0) * multiplier

        for row in self.breakdown_rows:
            if "amount_base" in row:
                row["amount"] = row["amount_base"] * multiplier

    def _open_file(self):
        # ... (логика открытия файла, осталась прежней) ...
        try:
            from tkinter import filedialog as fd
        except Exception:
            messagebox.showerror("Файл", "Не удалось открыть диалог выбора файлов.")
            return
        fname = fd.askopenfilename(title="Выберите файл сметы (XLSX/CSV)", filetypes=[("Excel", "*.xlsx;*.xlsm"), ("CSV", "*.csv"), ("Все файлы", "*.*")])
        if not fname:
            return
        self.file_path = Path(fname)
        self.lbl_file.config(text=f"Файл: {self.file_path}")
        ok = self._load_file(self.file_path)
        self.btn_map.config(state=("normal" if (ok and self.mode == "generic") else "disabled"))
        self.btn_export.config(state=("normal" if ok else "disabled"))
        if not ok:
            messagebox.showwarning("Анализ смет", "Не удалось распознать структуру файла.")

    # ... (методы _load_file, _parse_xlsx_smeta_ru, _sheet_has_local_smeta_marker, _normalize_header_text, 
    # _find_table_header, _is_numbering_row, _is_sequential_digits_list, _is_summary_row, _is_summary_name,
    # _str, _to_number, _first_number_from_cols, _is_labor_or_percent_unit, _has_numeric_position
    # остаются без изменений, кроме того, что они теперь используются новым _classify_smeta_row_new) ...
    
    # -------- НОВАЯ ЛОГИКА КЛАССИФИКАЦИИ СТРОК Smeta.RU --------
    
    def _classify_smeta_row_new(self, row: List[Any]) -> Tuple[Optional[str], Optional[float]]:
        if self.smeta_name_col is None:
            return None, None
            
        if self._is_summary_row(row, self.smeta_name_col):
            return None, None
        
        name = self._str(row[self.smeta_name_col]) if self.smeta_name_col < len(row) else ""
        n = re.sub(r"[^а-яa-z0-9]", "", name.lower()) # Нормализованное имя (без пробелов, знаков)

        val = self._first_number_from_cols(row, self.smeta_cost_cols)
        if not isinstance(val, float) or val <= 0:
            return None, None

        # 1. Справочная ЗПМ (должна быть проверена первой, чтобы потом вычесть из ЭМ)
        if "втчзпм" in n or "втомчислезпм" in n:
            return "zpm_incl", val

        # 2. ЗП (Заработная плата)
        if "оплататруда" in n or n == "зп" or n == "зпм" or "заработн" in n:
            return "zp", val
            
        # 3. ЭМ (Эксплуатация машин) - Гросс (до вычета ЗПМ)
        if n.startswith("эм") or n.startswith("эмм") or "эксплуатациямашин" in n:
            # Классифицируем как EM, вычет произойдет в _analyze_smeta_new
            return "em_gross", val 

        # 4. НР / СП
        if n.startswith("нр") or "накладные" in n or n.startswith("сп") or "сметнаяприбыль" in n:
            # Комбинированные НР/СП от ЗПМ
            if "отзпм" in n or "нриспотзпм" in n:
                return "nr_sp_zpm", val
            # Стандартные НР
            if "нротзп" in n or "нр" == n or "накладные" in n:
                return "nr", val
            # Стандартная СП
            if "спотзп" in n or "сп" == n or "сметнаяприбыль" in n:
                return "sp", val
            # Если явно не указано 'от ЗП', но есть НР/СП, берем их
            if "нр" in n:
                return "nr", val
            if "сп" in n:
                return "sp", val

        # 5. МР (Материалы) - Inline Rule
        # Проверяем, что это строка позиции (не НР/СП/ЗП)
        if self._has_numeric_position(row[0] if len(row) > 0 else None):
            # Проверяем единицу измерения (Колонка 3) - не трудочасы и не проценты
            unit = row[3] if len(row) > 3 else ""
            if not self._is_labor_or_percent_unit(unit):
                # Если это позиция с ценой, но не ЗП/ЭМ/НР/СП, классифицируем как Материалы
                return "mr", val

        return None, None
    
    # -------- НОВАЯ ЛОГИКА АНАЛИЗА Smeta.RU --------

    def _analyze_smeta(self):
        if self.smeta_name_col is None or not self.smeta_cost_cols:
            raise RuntimeError("Не заданы индексы колонок для сметы.")

        # Инициализация для накопления
        gross_stats: Dict[str, float] = {k: 0.0 for k in self.COST_KEYS + self.REFERENCE_KEYS + ["em_gross"]}
        self.breakdown_rows = []
        
        name_col_idx = self.smeta_name_col

        for row in self.smeta_data_rows:
            # 1. Извлечение данных для расшифровки
            pos_num = self._str(row[0]) if len(row) > 0 else ""
            rate_code = self._str(row[1]) if len(row) > 1 else ""
            
            # 2. Классификация
            cat, val = self._classify_smeta_row_new(row)
            
            if not cat or not isinstance(val, float) or val <= 0:
                continue
                
            name = self._str(row[name_col_idx])
            
            # 3. Накопление
            gross_stats[cat] = gross_stats.get(cat, 0.0) + val
            
            # 4. Сохранение в расшифровке
            display_cat = self.DISPLAY_CATEGORIES_MAP.get(cat, cat)
            self.breakdown_rows.append({
                "pos_num": pos_num,
                "rate_code": rate_code,
                "category": display_cat, 
                "name": name, 
                "amount": val, 
                "amount_base": val # Базовая сумма до НДС
            })

        # --- Финальный расчет и коррекция ---
        
        # 1. ЭМ (нетто) = ЭМ (гросс) - в т.ч. ЗПМ
        em_gross_total = gross_stats.pop("em_gross", 0.0)
        zpm_incl_total = gross_stats["zpm_incl"]
        em_net_total = max(0.0, em_gross_total - zpm_incl_total)
        
        final_stats = {
            "zp": gross_stats.get("zp", 0.0),
            "em": em_net_total, 
            "mr": gross_stats.get("mr", 0.0),
            "nr": gross_stats.get("nr", 0.0),
            "sp": gross_stats.get("sp", 0.0),
            "nr_sp_zpm": gross_stats.get("nr_sp_zpm", 0.0),
        }
        
        total_cost = sum(final_stats.values())
        
        # Обновляем self.stats_base для дальнейшей работы (НДС, отображение)
        self.stats_base = final_stats
        self.stats_base["total"] = total_cost
        self.stats_base["zpm_incl"] = zpm_incl_total # Справочная информация
        
        self.stats_base["materials"] = self.stats_base["mr"] # Для generic экспорта
        self.stats_base["wages"] = self.stats_base["zp"] # Для generic экспорта
        
        self._apply_vat()
        self._render_stats()
        self._fill_breakdown_table()
        # Диаграмма удалена.

    # Карта для отображения категорий в расшифровке
    DISPLAY_CATEGORIES_MAP = {
        "zp": "Заработная плата (ЗП)",
        "em_gross": "Эксплуатация машин (ЭМ)", # Даже если ЭМ гросс, отображаем как ЭМ
        "em": "Эксплуатация машин (ЭМ)", 
        "mr": "Материалы (МР)",
        "nr": "Накладные расходы (НР)",
        "sp": "Сметная прибыль (СП)",
        "nr_sp_zpm": "НР и СП от ЗПМ",
        "zpm_incl": "в т.ч. ЗПМ",
    }
    
    # ... (методы _parse_xlsx_generic, _parse_csv_generic, _norm_header, _detect_mapping, _sum_column, _analyze_generic остаются прежними) ...

    def _render_stats(self):
        total     = float(self.stats.get("total") or 0.0)
        
        self._row_total["val"].config(text=self._fmt_money(total))
        self._row_total["pct"].config(text="100%")

        # 1. Основные категории
        for key, _ in self.DISPLAY_CATEGORIES:
            val = float(self.stats.get(key) or 0.0)
            p = (val / total * 100.0) if total > 1e-12 else None
            self._metric_rows[key]["val"].config(text=self._fmt_money(val))
            self._metric_rows[key]["pct"].config(text=self._fmt_pct(p))
            
        # 2. Справочная ЗПМ
        zpm_incl = float(self.stats.get("zpm_incl") or 0.0)
        self._row_zpm_ref["val"].config(text=self._fmt_money(zpm_incl))
        self._row_zpm_ref["pct"].config(text="-") 
        
        # 3. НДС
        if self.vat_enabled.get():
            total_base = self.stats_base.get("total", 0.0)
            vat_amount = total_base * 0.2
            total_with_vat = total_base * 1.2
            
            self._row_vat["label"].grid()
            self._row_vat["val"].grid()
            self._row_vat["pct"].grid()
            
            self._row_total_vat["label"].grid()
            self._row_total_vat["val"].grid()
            self._row_total_vat["pct"].grid()
            
            self._row_vat["val"].config(text=self._fmt_money(vat_amount))
            self._row_vat["pct"].config(text="20%")
            self._row_total_vat["val"].config(text=self._fmt_money(total_with_vat))
            self._row_total_vat["pct"].config(text="120%")
        else:
            self._row_vat["label"].grid_remove()
            self._row_vat["val"].grid_remove()
            self._row_vat["pct"].grid_remove()
            self._row_total_vat["label"].grid_remove()
            self._row_total_vat["val"].grid_remove()
            self._row_total_vat["pct"].grid_remove()

    def _fill_breakdown_table(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        if not self.breakdown_rows:
            return
            
        show_mat = self.var_show_mat.get()
        show_wag = self.var_show_wag.get()
        show_oth = self.var_show_oth.get()

        # Группировка категорий для фильтрации
        WAGE_CATS = [self.DISPLAY_CATEGORIES_MAP["zp"], self.DISPLAY_CATEGORIES_MAP["em_gross"], self.DISPLAY_CATEGORIES_MAP["zpm_incl"]]
        OTHER_CATS = [self.DISPLAY_CATEGORIES_MAP["nr"], self.DISPLAY_CATEGORIES_MAP["sp"], self.DISPLAY_CATEGORIES_MAP["nr_sp_zpm"]]
        MATERIAL_CATS = [self.DISPLAY_CATEGORIES_MAP["mr"]]

        for row in self.breakdown_rows:
            cat = row["category"]
            
            is_mat = cat in MATERIAL_CATS
            is_wag = cat in WAGE_CATS
            is_oth = cat in OTHER_CATS
            
            if (is_mat and not show_mat) or (is_wag and not show_wag) or (is_oth and not show_oth):
                continue
            
            amt = float(row["amount"] or 0.0)
            
            self.tree.insert("", "end", values=(
                row.get("pos_num", ""), 
                row.get("rate_code", ""), 
                cat, 
                str(row["name"]), 
                self._fmt_money(amt)
            ))

    # _render_chart удален
    def _render_chart(self):
        pass # Заглушка, чтобы не ломать старые вызовы

    def _open_mapping(self):
        if not self.headers or self.mode != "generic":
            return
        dlg = ColumnMappingDialog(self, headers=self.headers, cur_map=self.mapping)
        if getattr(dlg, "result", None):
            self.mapping = dlg.result
            self._analyze_generic()

    def _export_summary(self):
        # Логика экспорта адаптирована под новые категории
        try:
            from tkinter import filedialog as fd
        except Exception:
            messagebox.showerror("Экспорт", "Не удалось открыть диалог сохранения.")
            return
        if not self.stats:
            return
        fname = fd.asksaveasfilename(title="Сохранить свод", defaultextension=".xlsx", filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv")])
        if not fname:
            return
        out = Path(fname)
        
        # Подготовка данных для экспорта
        export_metrics = [
            ("Строительные затраты (Итого)", self.stats.get("total", 0.0), "100%"),
        ]
        for key, title in self.DISPLAY_CATEGORIES:
            val = self.stats.get(key, 0.0)
            pct = self._fmt_pct(self._safe_pct(val))
            export_metrics.append((title, val, pct))
        
        ref_zpm = self.stats.get("zpm_incl", 0.0)
        export_metrics.append(("в т.ч. ЗПМ (Справочно)", ref_zpm, "-"))

        if self.vat_enabled.get():
            total_base = self.stats_base.get("total", 0.0)
            vat_amount = total_base * 0.2
            total_with_vat = total_base * 1.2
            export_metrics.append(("НДС 20%", vat_amount, "20%"))
            export_metrics.append(("Всего с НДС", total_with_vat, "120%"))

        try:
            if out.suffix.lower() == ".csv":
                with open(out, "w", encoding="utf-8-sig", newline="") as f:
                    w = csv.writer(f, delimiter=";")
                    w.writerow(["Показатель", "Сумма (руб.)", "Доля"])
                    for title, val, pct in export_metrics:
                        w.writerow([title, f"{self._fmt_money(val)}", pct])

                    w.writerow([])
                    w.writerow(["Расшифровка", "", "", "", ""])
                    w.writerow(["Поз.", "Шифр расценки", "Категория", "Наименование", "Сумма, руб."])
                    for row in self.breakdown_rows:
                        w.writerow([row.get("pos_num", ""), row.get("rate_code", ""), row["category"], row["name"], f"{self._fmt_money(row['amount'])}"])
            else:
                wb = Workbook()
                ws = wb.active
                ws.title = "Анализ сметы"
                
                ws.append(["Показатель", "Сумма (руб.)", "Доля"])
                for title, val, pct in export_metrics:
                    ws.append([title, float(val), pct])

                ws.append([])
                ws.append(["Расшифровка"])
                ws.append(["Поз.", "Шифр расценки", "Категория", "Наименование", "Сумма, руб."])
                
                for row in self.breakdown_rows:
                    ws.append([row.get("pos_num", ""), row.get("rate_code", ""), row["category"], row["name"], float(row.get("amount", 0.0) or 0.0)])
                
                ws.column_dimensions["A"].width = 10 # Поз.
                ws.column_dimensions["B"].width = 20 # Шифр
                ws.column_dimensions["C"].width = 36
                ws.column_dimensions["D"].width = 60
                ws.column_dimensions["E"].width = 18
                wb.save(out)
            messagebox.showinfo("Экспорт", f"Свод сохранён:\n{out}")
        except Exception as e:
            messagebox.showerror("Экспорт", f"Не удалось сохранить свод:\n{e}")


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
