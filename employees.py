# employees.py
import logging
from typing import List, Dict, Any, Optional

import tkinter as tk
from tkinter import ttk, messagebox

from psycopg2.extras import RealDictCursor

# Можем переиспользовать функции/классы из timesheet_module
import timesheet_module


# ------------------------- Работа с пулом соединений -------------------------

db_connection_pool = None

def set_db_pool(pool):
    """
    Вызывается из main_app, чтобы employees.py использовал тот же пул БД.
    """
    global db_connection_pool
    db_connection_pool = pool


def get_db_connection():
    if not db_connection_pool:
        raise RuntimeError("Пул соединений не был установлен для employees.py")
    return db_connection_pool.getconn()


def release_db_connection(conn):
    if db_connection_pool and conn:
        db_connection_pool.putconn(conn)


# ------------------------- Функции для работы с БД -------------------------

def find_employee_work_summary(
    fio: Optional[str] = None,
    tbn: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    department: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Возвращает свод по сотруднику:
      - на каких объектах работал
      - за какие периоды (год/месяц)
      - сколько дней/часов/ночных/переработки

    Группировка по объекту + периоду (год/месяц) + подразделению.
    """
    if not fio and not tbn:
        return []

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            where = ["1=1"]
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

            where_sql = " AND ".join(where)

            cur.execute(
                f"""
                SELECT
                    h.object_id,
                    h.object_addr,
                    h.year,
                    h.month,
                    COALESCE(h.department, '') AS department,
                    SUM(COALESCE(r.total_days, 0))      AS total_days,
                    SUM(COALESCE(r.total_hours, 0))     AS total_hours,
                    SUM(COALESCE(r.night_hours, 0))     AS night_hours,
                    SUM(COALESCE(r.overtime_day, 0))    AS overtime_day,
                    SUM(COALESCE(r.overtime_night, 0))  AS overtime_night
                FROM timesheet_headers h
                JOIN timesheet_rows r ON r.header_id = h.id
                WHERE {where_sql}
                GROUP BY
                    h.object_id,
                    h.object_addr,
                    h.year,
                    h.month,
                    COALESCE(h.department, '')
                ORDER BY
                    h.year DESC,
                    h.month DESC,
                    h.object_addr,
                    COALESCE(h.department, '')
                """,
                params,
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        if conn:
            release_db_connection(conn)


# ------------------------- GUI: страница "Работники" -------------------------

# Переиспользуем вспомогательные вещи из timesheet_module
AutoCompleteCombobox = timesheet_module.AutoCompleteCombobox
month_name_ru = timesheet_module.month_name_ru
load_employees_from_db = timesheet_module.load_employees_from_db


class WorkersPage(tk.Frame):
    """
    Раздел 'Работники':
    - Поиск по ФИО и/или табельному номеру
    - Показ всех объектов и периодов, где сотрудник работал,
      с указанием дней/часов/ночных/переработки.
    """
    def __init__(self, master, app_ref: "MainApp"):
        super().__init__(master)
        self.app_ref = app_ref

        # Справочник сотрудников
        self.employees = load_employees_from_db()
        self.emp_names = [e[0] for e in self.employees]
        self.emp_tbn_by_fio = {}
        for fio, tbn, pos, dep in self.employees:
            self.emp_tbn_by_fio[fio] = tbn

        # Список подразделений для выпадающего списка
        deps_set = { (dep or "").strip() for _, _, _, dep in self.employees if (dep or "").strip() }
        self.departments = ["Все"] + sorted(deps_set)

        # Переменные формы
        self.var_fio = tk.StringVar()
        self.var_tbn = tk.StringVar()
        self.var_year = tk.StringVar()
        self.var_month = tk.StringVar()
        self.var_dep = tk.StringVar()

        self.tree = None
        self._rows: List[Dict[str, Any]] = []

        self._build_ui()

    def _build_ui(self):
        # Верхняя панель с фильтрами
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=(8, 4))

        tk.Label(top, text="Работники", font=("Segoe UI", 12, "bold")).grid(
            row=0, column=0, columnspan=6, sticky="w", pady=(0, 8)
        )

        row_f = 1

        # ФИО (автодополнение)
        tk.Label(top, text="ФИО:").grid(row=row_f, column=0, sticky="e", padx=(0, 4))
        cmb_fio = AutoCompleteCombobox(top, width=40, textvariable=self.var_fio)
        cmb_fio.set_completion_list(self.emp_names)
        cmb_fio.grid(row=row_f, column=1, sticky="w")
        cmb_fio.bind("<<ComboboxSelected>>", self._on_fio_selected)

        # Табельный №
        tk.Label(top, text="Табельный №:").grid(row=row_f, column=2, sticky="e", padx=(12, 4))
        ent_tbn = ttk.Entry(top, width=16, textvariable=self.var_tbn)
        ent_tbn.grid(row=row_f, column=3, sticky="w")

        row_f += 1

        # Год / Месяц (необязательно)
        tk.Label(top, text="Год:").grid(row=row_f, column=0, sticky="e", padx=(0, 4), pady=(4, 0))
        spn_year = tk.Spinbox(top, from_=2000, to=2100, width=6, textvariable=self.var_year)
        spn_year.grid(row=row_f, column=1, sticky="w", pady=(4, 0))
        self.var_year.set("")  # по умолчанию — без ограничения

        tk.Label(top, text="Месяц:").grid(row=row_f, column=2, sticky="e", padx=(12, 4), pady=(4, 0))
        cmb_month = ttk.Combobox(
            top,
            state="readonly",
            width=12,
            textvariable=self.var_month,
            values=["Все"] + [month_name_ru(i) for i in range(1, 13)],
        )
        cmb_month.grid(row=row_f, column=3, sticky="w", pady=(4, 0))
        self.var_month.set("Все")

        row_f += 1

        tk.Label(top, text="Подразделение:").grid(row=row_f, column=0, sticky="e", padx=(0, 4), pady=(4, 0))
        cmb_dep = ttk.Combobox(
            top,
            state="readonly",
            width=40,
            textvariable=self.var_dep,
            values=self.departments,
        )
        cmb_dep.grid(row=row_f, column=1, sticky="w", pady=(4, 0))
        # по умолчанию "Все"
        if not self.var_dep.get():
            self.var_dep.set("Все")

        # Кнопки
        btns = tk.Frame(top)
        btns.grid(row=row_f, column=3, columnspan=3, sticky="e", pady=(4, 0))
        ttk.Button(btns, text="Найти", command=self._search).pack(side="left", padx=2)
        ttk.Button(btns, text="Сбросить", command=self._reset).pack(side="left", padx=2)

        # Таблица результатов
        frame = tk.Frame(self)
        frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        cols = (
            "period",
            "object",
            "object_id",
            "department",
            "total_days",
            "total_hours",
            "night_hours",
            "overtime_day",
            "overtime_night",
        )
        self.tree = ttk.Treeview(frame, columns=cols, show="headings", selectmode="browse")

        self.tree.heading("period", text="Период")
        self.tree.heading("object", text="Объект (адрес)")
        self.tree.heading("object_id", text="ID объекта")
        self.tree.heading("department", text="Подразделение")
        self.tree.heading("total_days", text="Дни")
        self.tree.heading("total_hours", text="Часы")
        self.tree.heading("night_hours", text="Ночных")
        self.tree.heading("overtime_day", text="Пер. день")
        self.tree.heading("overtime_night", text="Пер. ночь")

        self.tree.column("period", width=90, anchor="center", stretch=False)
        self.tree.column("object", width=320, anchor="w")
        self.tree.column("object_id", width=90, anchor="center", stretch=False)
        self.tree.column("department", width=140, anchor="w")
        self.tree.column("total_days", width=60, anchor="center", stretch=False)
        self.tree.column("total_hours", width=80, anchor="e", stretch=False)
        self.tree.column("night_hours", width=80, anchor="e", stretch=False)
        self.tree.column("overtime_day", width=90, anchor="e", stretch=False)
        self.tree.column("overtime_night", width=90, anchor="e", stretch=False)

        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        # Нижняя подсказка
        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=8, pady=(0, 8))
        tk.Label(
            bottom,
            text="Введите ФИО и/или табельный номер и нажмите 'Найти'.",
            font=("Segoe UI", 9),
            fg="#555",
        ).pack(side="left")

    def _on_fio_selected(self, event=None):
        fio = self.var_fio.get().strip()
        tbn = self.emp_tbn_by_fio.get(fio, "")
        if tbn:
            self.var_tbn.set(tbn)

    def _reset(self):
        self.var_fio.set("")
        self.var_tbn.set("")
        self.var_year.set("")
        self.var_month.set("Все")
        self.var_dep.set("Все")
        self._rows.clear()
        self.tree.delete(*self.tree.get_children())

    def _search(self):
        fio = self.var_fio.get().strip()
        tbn = self.var_tbn.get().strip()
        if not fio and not tbn:
            messagebox.showwarning("Работники", "Введите ФИО и/или табельный номер.")
            return

        year = None
        if self.var_year.get().strip():
            try:
                y = int(self.var_year.get().strip())
                if 2000 <= y <= 2100:
                    year = y
                else:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("Работники", "Год введён некорректно (ожидается число 2000–2100).")
                return

        month = None
        m_name = self.var_month.get().strip()
        if m_name and m_name != "Все":
            try:
                month = [month_name_ru(i) for i in range(1, 13)].index(m_name) + 1
            except ValueError:
                month = None

        dep_val = self.var_dep.get().strip()
        if dep_val and dep_val != "Все":
            dep = dep_val
        else:
            dep = None

        try:
            rows = find_employee_work_summary(
                fio=fio or None,
                tbn=tbn or None,
                year=year,
                month=month,
                department=dep,
            )
        except Exception as e:
            logging.exception("Ошибка поиска по работнику")
            messagebox.showerror("Работники", f"Ошибка при обращении к БД:\n{e}")
            return

        self._rows = rows
        self._fill_tree()

        if not rows:
            messagebox.showinfo("Работники", "По заданным условиям ничего не найдено.")

    def _fill_tree(self):
        self.tree.delete(*self.tree.get_children())
        for idx, r in enumerate(self._rows):
            year = r.get("year")
            month = r.get("month")
            period_str = f"{month_name_ru(month)} {year}" if year and month else ""

            obj_addr = r.get("object_addr") or ""
            obj_id = r.get("object_id") or ""
            dep = r.get("department") or ""

            def fmt(v):
                if v is None:
                    return ""
                if isinstance(v, float):
                    s = f"{v:.2f}".rstrip("0").rstrip(".")
                    return s
                return str(v)

            self.tree.insert(
                "",
                "end",
                iid=str(idx),
                values=(
                    period_str,
                    obj_addr,
                    obj_id,
                    dep,
                    fmt(r.get("total_days")),
                    fmt(r.get("total_hours")),
                    fmt(r.get("night_hours")),
                    fmt(r.get("overtime_day")),
                    fmt(r.get("overtime_night")),
                ),
            )

# ------------------------- API для main_app -------------------------

def create_workers_page(parent, app_ref) -> WorkersPage:
    """Создает страницу 'Работники'."""
    return WorkersPage(parent, app_ref=app_ref)
