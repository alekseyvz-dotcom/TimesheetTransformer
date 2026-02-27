# employees.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from psycopg2.extras import RealDictCursor

import timesheet_module

# ============================================================
#  Цветовая схема (единая с остальными модулями)
# ============================================================
WK_COLORS = {
    "bg":           "#f0f2f5",
    "panel":        "#ffffff",
    "accent":       "#1565c0",
    "accent_light": "#e3f2fd",
    "success":      "#2e7d32",
    "warning":      "#b00020",
    "border":       "#dde1e7",
    "btn_save_bg":  "#1565c0",
    "btn_save_fg":  "#ffffff",
    # строки таблицы
    "row_even":     "#ffffff",
    "row_odd":      "#f8f9fb",
    "row_ot":       "#fff9c4",   # переработка
    "row_night":    "#e8f5e9",   # ночные
}

# ============================================================
#  Пул соединений
# ============================================================
db_connection_pool = None


def set_db_pool(pool):
    global db_connection_pool
    db_connection_pool = pool


def get_db_connection():
    if not db_connection_pool:
        raise RuntimeError("Пул соединений не установлен для employees.py")
    return db_connection_pool.getconn()


def release_db_connection(conn):
    if db_connection_pool and conn:
        db_connection_pool.putconn(conn)


# ============================================================
#  DB API
# ============================================================

def find_employee_work_summary(
    fio: Optional[str] = None,
    tbn: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    department: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Свод по сотруднику: объекты, периоды, дни/часы/ночные/переработка."""
    if not fio and not tbn:
        return []

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            where  = ["1=1"]
            params: List[Any] = []

            if fio:
                where.append("LOWER(TRIM(r.fio)) = LOWER(TRIM(%s))")
                params.append(fio)
            if tbn:
                where.append("COALESCE(TRIM(r.tbn), '') = TRIM(%s)")
                params.append(tbn)
            if year is not None:
                where.append("h.year = %s")
                params.append(year)
            if month is not None:
                where.append("h.month = %s")
                params.append(month)
            if department:
                where.append("COALESCE(h.department, '') = %s")
                params.append(department)

            cur.execute(
                f"""
                SELECT
                    h.object_id,
                    h.object_addr,
                    h.year,
                    h.month,
                    COALESCE(h.department, '')         AS department,
                    SUM(COALESCE(r.total_days,    0))  AS total_days,
                    SUM(COALESCE(r.total_hours,   0))  AS total_hours,
                    SUM(COALESCE(r.night_hours,   0))  AS night_hours,
                    SUM(COALESCE(r.overtime_day,  0))  AS overtime_day,
                    SUM(COALESCE(r.overtime_night,0))  AS overtime_night
                FROM timesheet_headers h
                JOIN timesheet_rows r ON r.header_id = h.id
                WHERE {" AND ".join(where)}
                GROUP BY
                    h.object_id, h.object_addr,
                    h.year, h.month,
                    COALESCE(h.department, '')
                ORDER BY
                    h.year DESC, h.month DESC,
                    h.object_addr,
                    COALESCE(h.department, '')
                """,
                params,
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        if conn:
            release_db_connection(conn)


# ============================================================
#  Переиспользуем из timesheet_module
# ============================================================
AutoCompleteCombobox = timesheet_module.AutoCompleteCombobox
month_name_ru        = timesheet_module.month_name_ru
load_employees_from_db = timesheet_module.load_employees_from_db


# ============================================================
#  Страница «Работники»
# ============================================================

class WorkersPage(tk.Frame):
    """
    Раздел «Работники» в едином стиле.
    Новое:
      - карточка сотрудника (должность, подразделение)
      - итоговая строка по результатам
      - зебра + цветовые теги строк
      - экспорт в Excel
      - статус-бар с количеством найденных записей
    """

    def __init__(self, master, app_ref):
        super().__init__(master, bg=WK_COLORS["bg"])
        self.app_ref = app_ref

        # Справочник
        self.employees     = load_employees_from_db()
        self.emp_names     = [e[0] for e in self.employees]
        self.emp_info: Dict[str, Dict[str, str]] = {}
        for fio, tbn, pos, dep in self.employees:
            self.emp_info[fio] = {"tbn": tbn, "pos": pos or "", "dep": dep or ""}

        deps_set = {
            (dep or "").strip()
            for _, _, _, dep in self.employees
            if (dep or "").strip()
        }
        self.departments = ["Все"] + sorted(deps_set)

        # Переменные формы
        self.var_fio   = tk.StringVar()
        self.var_tbn   = tk.StringVar()
        self.var_year  = tk.StringVar(value="")
        self.var_month = tk.StringVar(value="Все")
        self.var_dep   = tk.StringVar(value="Все")

        self.tree  = None
        self._rows: List[Dict[str, Any]] = []

        self._build_ui()

    # ──────────────────────────────────────────────────────────
    #  UI
    # ──────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Заголовок ─────────────────────────────────────────
        hdr = tk.Frame(self, bg=WK_COLORS["accent"], pady=6)
        hdr.pack(fill="x")
        tk.Label(
            hdr, text="👷  Работники — история по объектам",
            font=("Segoe UI", 12, "bold"),
            bg=WK_COLORS["accent"], fg="white", padx=12
        ).pack(side="left")

        # ── Панель поиска сотрудника ──────────────────────────
        search_pnl = tk.LabelFrame(
            self, text=" 🔍 Поиск сотрудника ",
            font=("Segoe UI", 9, "bold"),
            bg=WK_COLORS["panel"], fg=WK_COLORS["accent"],
            relief="groove", bd=1, padx=10, pady=8
        )
        search_pnl.pack(fill="x", padx=10, pady=(8, 4))
        search_pnl.grid_columnconfigure(1, weight=1)
        search_pnl.grid_columnconfigure(3, weight=0)

        # ФИО
        self._lbl(search_pnl, "ФИО", 0, 0, required=True)
        self.cmb_fio = AutoCompleteCombobox(
            search_pnl, width=40,
            textvariable=self.var_fio,
            font=("Segoe UI", 9)
        )
        self.cmb_fio.set_completion_list(self.emp_names)
        self.cmb_fio.grid(row=0, column=1, sticky="ew", pady=3)
        self.cmb_fio.bind("<<ComboboxSelected>>", self._on_fio_selected)

        # Таб. №
        self._lbl(search_pnl, "Таб. №", 0, 2)
        self.ent_tbn = ttk.Entry(
            search_pnl, width=14,
            textvariable=self.var_tbn,
            font=("Segoe UI", 9)
        )
        self.ent_tbn.grid(row=0, column=3, sticky="w", padx=(0, 12), pady=3)

        # Карточка сотрудника (авто-заполняется)
        self.lbl_card = tk.Label(
            search_pnl, text="",
            font=("Segoe UI", 8, "italic"), fg="#555",
            bg=WK_COLORS["panel"], anchor="w"
        )
        self.lbl_card.grid(row=1, column=1, columnspan=3,
                           sticky="w", pady=(0, 3))

        # ── Панель фильтров периода ───────────────────────────
        flt_pnl = tk.LabelFrame(
            self, text=" 📅 Фильтр периода (необязательно) ",
            font=("Segoe UI", 9, "bold"),
            bg=WK_COLORS["panel"], fg=WK_COLORS["accent"],
            relief="groove", bd=1, padx=10, pady=8
        )
        flt_pnl.pack(fill="x", padx=10, pady=(2, 4))

        # Год
        self._lbl(flt_pnl, "Год", 0, 0)
        spn = tk.Spinbox(
            flt_pnl, from_=2000, to=2100, width=7,
            textvariable=self.var_year,
            font=("Segoe UI", 9)
        )
        spn.grid(row=0, column=1, sticky="w", pady=3)

        # Месяц
        self._lbl(flt_pnl, "Месяц", 0, 2)
        ttk.Combobox(
            flt_pnl, state="readonly", width=14,
            textvariable=self.var_month,
            values=["Все"] + [month_name_ru(i) for i in range(1, 13)]
        ).grid(row=0, column=3, sticky="w", padx=(0, 16), pady=3)

        # Подразделение
        self._lbl(flt_pnl, "Подразделение", 0, 4)
        ttk.Combobox(
            flt_pnl, state="readonly", width=32,
            textvariable=self.var_dep,
            values=self.departments
        ).grid(row=0, column=5, sticky="ew", pady=3)
        flt_pnl.grid_columnconfigure(5, weight=1)

        # Кнопки
        btn_pnl = tk.Frame(flt_pnl, bg=WK_COLORS["panel"])
        btn_pnl.grid(row=0, column=6, sticky="e", padx=(16, 0))

        tk.Button(
            btn_pnl,
            text="🔍  Найти",
            font=("Segoe UI", 9, "bold"),
            bg=WK_COLORS["btn_save_bg"], fg=WK_COLORS["btn_save_fg"],
            activebackground="#0d47a1", activeforeground="white",
            relief="flat", cursor="hand2", padx=12, pady=4,
            command=self._search
        ).pack(side="left", padx=(0, 6))

        ttk.Button(
            btn_pnl, text="Сбросить",
            command=self._reset
        ).pack(side="left", padx=(0, 6))

        ttk.Button(
            btn_pnl, text="📊 Excel",
            command=self._export_excel
        ).pack(side="left")

        # ── Таблица результатов ───────────────────────────────
        tbl_pnl = tk.LabelFrame(
            self, text=" 📋 История работы на объектах ",
            font=("Segoe UI", 9, "bold"),
            bg=WK_COLORS["panel"], fg=WK_COLORS["accent"],
            relief="groove", bd=1
        )
        tbl_pnl.pack(fill="both", expand=True, padx=10, pady=(2, 4))

        cols = (
            "period", "object", "object_id", "department",
            "total_days", "total_hours", "night_hours",
            "overtime_day", "overtime_night",
        )
        self.tree = ttk.Treeview(
            tbl_pnl, columns=cols,
            show="headings", selectmode="browse"
        )

        heads = {
            "period":         ("Период",        100, "center"),
            "object":         ("Объект (адрес)", 340, "w"),
            "object_id":      ("ID объекта",      90, "center"),
            "department":     ("Подразделение",  160, "w"),
            "total_days":     ("Дни",             60, "center"),
            "total_hours":    ("Часы",            80, "e"),
            "night_hours":    ("Ночных ч.",       80, "e"),
            "overtime_day":   ("Пер. день",       90, "e"),
            "overtime_night": ("Пер. ночь",       90, "e"),
        }
        for col, (text, width, anchor) in heads.items():
            self.tree.heading(col, text=text)
            self.tree.column(col, width=width, anchor=anchor,
                             stretch=(col == "object"))

        # Теги цветов
        self.tree.tag_configure("even",  background=WK_COLORS["row_even"])
        self.tree.tag_configure("odd",   background=WK_COLORS["row_odd"])
        self.tree.tag_configure("ot",    background=WK_COLORS["row_ot"])
        self.tree.tag_configure("night", background=WK_COLORS["row_night"])
        self.tree.tag_configure(
            "total",
            background=WK_COLORS["accent_light"],
            font=("Segoe UI", 9, "bold")
        )

        vsb = ttk.Scrollbar(tbl_pnl, orient="vertical",
                            command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        # ── Нижняя панель (итоги + статус) ───────────────────
        bottom = tk.Frame(self, bg=WK_COLORS["accent_light"], pady=5)
        bottom.pack(fill="x", padx=10, pady=(0, 8))

        self.lbl_total = tk.Label(
            bottom,
            text="",
            font=("Segoe UI", 9, "bold"),
            fg=WK_COLORS["accent"],
            bg=WK_COLORS["accent_light"]
        )
        self.lbl_total.pack(side="left", padx=10)

        tk.Label(
            bottom,
            text="Строки с переработкой выделены жёлтым, "
                 "с ночными часами — зелёным.",
            font=("Segoe UI", 8, "italic"), fg="#555",
            bg=WK_COLORS["accent_light"]
        ).pack(side="right", padx=10)

    # ──────────────────────────────────────────────────────────
    #  Вспомогательный метод меток
    # ──────────────────────────────────────────────────────────

    def _lbl(self, parent, text: str, row: int, col: int,
             required: bool = False):
        display = f"{text}  *:" if required else f"{text}:"
        fg = WK_COLORS["warning"] if required else "#333"
        tk.Label(
            parent, text=display,
            font=("Segoe UI", 9), fg=fg,
            bg=WK_COLORS["panel"], anchor="e"
        ).grid(row=row, column=col, sticky="e",
               padx=(0, 6), pady=3)

    # ──────────────────────────────────────────────────────────
    #  Логика
    # ──────────────────────────────────────────────────────────

    def _on_fio_selected(self, event=None):
        fio  = self.var_fio.get().strip()
        info = self.emp_info.get(fio, {})
        tbn  = info.get("tbn", "")
        pos  = info.get("pos", "")
        dep  = info.get("dep", "")

        if tbn:
            self.var_tbn.set(tbn)

        # Карточка
        parts = []
        if tbn:
            parts.append(f"Таб. №: {tbn}")
        if pos:
            parts.append(f"Должность: {pos}")
        if dep:
            parts.append(f"Подразделение: {dep}")
            # Авто-устанавливаем подразделение в фильтр
            if dep in self.departments:
                self.var_dep.set(dep)

        try:
            self.lbl_card.config(
                text="  |  ".join(parts) if parts else ""
            )
        except Exception:
            pass

    def _reset(self):
        self.var_fio.set("")
        self.var_tbn.set("")
        self.var_year.set("")
        self.var_month.set("Все")
        self.var_dep.set("Все")
        self._rows.clear()
        if self.tree:
            self.tree.delete(*self.tree.get_children())
        try:
            self.lbl_card.config(text="")
            self.lbl_total.config(text="")
        except Exception:
            pass

    def _search(self):
        fio = self.var_fio.get().strip()
        tbn = self.var_tbn.get().strip()

        if not fio and not tbn:
            messagebox.showwarning(
                "Работники",
                "Введите ФИО и/или табельный номер для поиска."
            )
            return

        # Год
        year = None
        y_str = self.var_year.get().strip()
        if y_str:
            try:
                y = int(y_str)
                if not (2000 <= y <= 2100):
                    raise ValueError
                year = y
            except ValueError:
                messagebox.showwarning(
                    "Работники",
                    "Год введён некорректно (ожидается 2000–2100)."
                )
                return

        # Месяц
        month  = None
        m_name = self.var_month.get().strip()
        if m_name and m_name != "Все":
            try:
                month = [month_name_ru(i) for i in range(1, 13)].index(m_name) + 1
            except ValueError:
                pass

        # Подразделение
        dep_val = self.var_dep.get().strip()
        dep = dep_val if (dep_val and dep_val != "Все") else None

        try:
            rows = find_employee_work_summary(
                fio=fio or None,
                tbn=tbn or None,
                year=year,
                month=month,
                department=dep,
            )
        except Exception as e:
            logging.exception("Ошибка поиска работника")
            messagebox.showerror("Работники",
                                 f"Ошибка при обращении к БД:\n{e}")
            return

        self._rows = rows
        self._fill_tree()

        if not rows:
            messagebox.showinfo(
                "Работники",
                "По заданным условиям ничего не найдено.\n"
                "Проверьте правильность ФИО или табельного номера."
            )

    # ──────────────────────────────────────────────────────────
    #  Заполнение таблицы
    # ──────────────────────────────────────────────────────────

    def _fmt(self, v) -> str:
        if v is None:
            return ""
        if isinstance(v, float):
            return f"{v:.2f}".rstrip("0").rstrip(".")
        return str(v)

    def _fill_tree(self):
        self.tree.delete(*self.tree.get_children())

        if not self._rows:
            try:
                self.lbl_total.config(text="Ничего не найдено")
            except Exception:
                pass
            return

        # Накопители для итоговой строки
        sum_days  = 0.0
        sum_hours = 0.0
        sum_night = 0.0
        sum_otd   = 0.0
        sum_otn   = 0.0

        for idx, r in enumerate(self._rows):
            yr  = r.get("year")
            mn  = r.get("month")
            period_str = (
                f"{month_name_ru(mn)} {yr}"
                if yr and mn else ""
            )

            td  = float(r.get("total_days",    0) or 0)
            th  = float(r.get("total_hours",   0) or 0)
            nh  = float(r.get("night_hours",   0) or 0)
            otd = float(r.get("overtime_day",  0) or 0)
            otn = float(r.get("overtime_night",0) or 0)

            sum_days  += td
            sum_hours += th
            sum_night += nh
            sum_otd   += otd
            sum_otn   += otn

            # Тег: переработка приоритетнее ночных
            if otd > 0 or otn > 0:
                tag = "ot"
            elif nh > 0:
                tag = "night"
            elif idx % 2 == 0:
                tag = "even"
            else:
                tag = "odd"

            self.tree.insert(
                "", "end", iid=str(idx),
                values=(
                    period_str,
                    r.get("object_addr") or "",
                    r.get("object_id")   or "",
                    r.get("department")  or "",
                    self._fmt(td)  if td  else "",
                    self._fmt(th)  if th  else "",
                    self._fmt(nh)  if nh  else "",
                    self._fmt(otd) if otd else "",
                    self._fmt(otn) if otn else "",
                ),
                tags=(tag,)
            )

        # ── Итоговая строка ───────────────────────────────────
        self.tree.insert(
            "", "end", iid="__total__",
            values=(
                "ИТОГО",
                f"Записей: {len(self._rows)}",
                "",
                "",
                self._fmt(sum_days),
                self._fmt(sum_hours),
                self._fmt(sum_night),
                self._fmt(sum_otd),
                self._fmt(sum_otn),
            ),
            tags=("total",)
        )

        # ── Статус-бар ────────────────────────────────────────
        parts = [f"Найдено записей: {len(self._rows)}"]
        if sum_days:
            parts.append(f"Всего дней: {self._fmt(sum_days)}")
        if sum_hours:
            parts.append(f"Часов: {self._fmt(sum_hours)}")
        if sum_night:
            parts.append(f"Ночных: {self._fmt(sum_night)}")
        if sum_otd or sum_otn:
            parts.append(
                f"Переработка: {self._fmt(sum_otd)} / {self._fmt(sum_otn)}"
            )

        try:
            self.lbl_total.config(text="  |  ".join(parts))
        except Exception:
            pass

    # ──────────────────────────────────────────────────────────
    #  Экспорт в Excel
    # ──────────────────────────────────────────────────────────

    def _export_excel(self):
        if not self._rows:
            messagebox.showinfo("Экспорт",
                                "Нет данных для выгрузки.")
            return

        fio = self.var_fio.get().strip()
        tbn = self.var_tbn.get().strip()
        who = fio or tbn or "работник"

        path = filedialog.asksaveasfilename(
            title="Сохранить историю работника",
            defaultextension=".xlsx",
            initialfile=(
                f"История_{who.replace(' ', '_')}_"
                f"{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            ),
            filetypes=[("Excel", "*.xlsx"), ("Все", "*.*")]
        )
        if not path:
            return

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "История работы"

            from openpyxl.styles import Font, PatternFill, Alignment

            # Шапка
            ws.append([f"История работы: {who}"])
            ws.append([
                f"Экспорт: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            ])
            ws.append([])

            header = [
                "Период", "Объект (адрес)", "ID объекта",
                "Подразделение", "Дни", "Часы",
                "Ночных ч.", "Пер. день", "Пер. ночь"
            ]
            ws.append(header)
            hdr_row = ws.max_row

            from openpyxl.styles import PatternFill, Font
            fill_hdr   = PatternFill("solid", fgColor="1565C0")
            fill_ot    = PatternFill("solid", fgColor="FFF9C4")
            fill_night = PatternFill("solid", fgColor="E8F5E9")
            fill_total = PatternFill("solid", fgColor="E3F2FD")
            font_hdr   = Font(bold=True, color="FFFFFF")
            font_total = Font(bold=True)

            for c in range(1, len(header) + 1):
                cell      = ws.cell(hdr_row, c)
                cell.font = font_hdr
                cell.fill = fill_hdr
                cell.alignment = Alignment(horizontal="center")

            sum_days = sum_hours = sum_night = sum_otd = sum_otn = 0.0

            for r in self._rows:
                yr  = r.get("year")
                mn  = r.get("month")
                period_str = (
                    f"{month_name_ru(mn)} {yr}"
                    if yr and mn else ""
                )

                td  = float(r.get("total_days",    0) or 0)
                th  = float(r.get("total_hours",   0) or 0)
                nh  = float(r.get("night_hours",   0) or 0)
                otd = float(r.get("overtime_day",  0) or 0)
                otn = float(r.get("overtime_night",0) or 0)

                sum_days  += td
                sum_hours += th
                sum_night += nh
                sum_otd   += otd
                sum_otn   += otn

                ws.append([
                    period_str,
                    r.get("object_addr") or "",
                    r.get("object_id")   or "",
                    r.get("department")  or "",
                    td or None, th or None,
                    nh or None, otd or None, otn or None,
                ])
                cur = ws.max_row
                if otd > 0 or otn > 0:
                    for c in range(1, len(header) + 1):
                        ws.cell(cur, c).fill = fill_ot
                elif nh > 0:
                    for c in range(1, len(header) + 1):
                        ws.cell(cur, c).fill = fill_night

            # Итоговая строка
            ws.append([
                "ИТОГО", f"Записей: {len(self._rows)}", "", "",
                sum_days or None, sum_hours or None,
                sum_night or None, sum_otd or None, sum_otn or None,
            ])
            tot_row = ws.max_row
            for c in range(1, len(header) + 1):
                cell      = ws.cell(tot_row, c)
                cell.fill = fill_total
                cell.font = font_total

            # Ширины
            widths = [14, 44, 12, 22, 8, 10, 10, 12, 12]
            for i, w in enumerate(widths, 1):
                ws.column_dimensions[get_column_letter(i)].width = w

            ws.freeze_panes = f"A{hdr_row + 1}"

            wb.save(path)
            messagebox.showinfo(
                "Экспорт",
                f"Файл сохранён:\n{path}\nЗаписей: {len(self._rows)}"
            )
        except Exception as e:
            logging.exception("Ошибка экспорта работника")
            messagebox.showerror("Экспорт", f"Ошибка:\n{e}")


# ============================================================
#  API
# ============================================================

def create_workers_page(parent, app_ref) -> WorkersPage:
    return WorkersPage(parent, app_ref=app_ref)
