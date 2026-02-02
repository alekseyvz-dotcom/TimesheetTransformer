# meals_employees.py

import logging
from typing import List, Dict, Any, Optional

import tkinter as tk
from tkinter import ttk, messagebox

from psycopg2.extras import RealDictCursor

# Переиспользуем классы/функции из модуля питания
import meals_module  # имя подставь под своё (файл, где у тебя MealOrderPage и т.п.)

# ------------------------- Работа с пулом соединений -------------------------

db_connection_pool = None


def set_db_pool(pool):
    """
    Вызывается из main_app, чтобы meals_employees.py использовал тот же пул БД.
    """
    global db_connection_pool
    db_connection_pool = pool


def get_db_connection():
    if not db_connection_pool:
        raise RuntimeError("Пул соединений не был установлен для meals_employees.py")
    return db_connection_pool.getconn()


def release_db_connection(conn):
    if db_connection_pool and conn:
        db_connection_pool.putconn(conn)


# ------------------------- Функции для работы с БД -------------------------

def find_employee_meals(
    fio: Optional[str] = None,
    tbn: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    department: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Возвращает свод по питанию сотрудника:
      - дата
      - фактический адрес (объект)
      - подразделение
      - бригада
      - тип питания
      - количество
      - цена за единицу (по справочнику типов питания)
      - итоговая сумма (кол-во * цена)

    Фильтры:
      fio, tbn  – по ФИО и/или табельному;
      year      – год (по дате заявки питания);
      month     – месяц (1–12);
      department – подразделение (по названию).
    """

    if not fio and not tbn:
        return []

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            where = ["1=1"]
            params: List[Any] = []

            # Фильтры по сотруднику
            if fio:
                where.append(
                    "(LOWER(TRIM(moi.fio_text)) = LOWER(TRIM(%s)) "
                    " OR LOWER(TRIM(e.fio)) = LOWER(TRIM(%s)))"
                )
                params.extend([fio, fio])

            if tbn:
                where.append(
                    "(COALESCE(TRIM(moi.tbn_text), '') = TRIM(%s) "
                    " OR COALESCE(TRIM(e.tbn), '') = TRIM(%s))"
                )
                params.extend([tbn, tbn])

            # Фильтры по дате
            if year is not None:
                where.append("EXTRACT(YEAR FROM mo.date) = %s")
                params.append(year)

            if month is not None:
                where.append("EXTRACT(MONTH FROM mo.date) = %s")
                params.append(month)

            # Фильтр по подразделению
            if department:
                where.append("COALESCE(d.name, '') = %s")
                params.append(department)

            where_sql = " AND ".join(where)

            # Берём:
            #  - дату,
            #  - фактический адрес (или адрес объекта),
            #  - подразделение,
            #  - бригаду,
            #  - тип питания (по имени типа или тексту),
            #  - количество,
            #  - цену из meal_types
            cur.execute(
                f"""
                SELECT
                    mo.date::date           AS date,
                    COALESCE(mo.fact_address, o.address, '') AS address,
                    COALESCE(d.name, '')   AS department,
                    COALESCE(mo.team_name, '') AS team_name,
                    COALESCE(mt.name, moi.meal_type_text, '') AS meal_type,
                    COALESCE(moi.quantity, 1) AS quantity,
                    COALESCE(mt.price, 0) AS price
                FROM meal_order_items moi
                JOIN meal_orders mo       ON mo.id = moi.order_id
                LEFT JOIN employees e     ON e.id = moi.employee_id
                LEFT JOIN departments d   ON d.id = mo.department_id
                LEFT JOIN objects o       ON o.id = mo.object_id
                LEFT JOIN meal_types mt   ON mt.id = moi.meal_type_id
                WHERE {where_sql}
                ORDER BY mo.date DESC, address, department, team_name, meal_type
                """,
                params,
            )

            rows = [dict(r) for r in cur.fetchall()]

        # Добавим расчёт суммы на стороне Python
        for r in rows:
            qty = float(r.get("quantity") or 1)
            price = float(r.get("price") or 0)
            r["amount"] = qty * price

        return rows

    finally:
        if conn:
            release_db_connection(conn)


# ------------------------- GUI: страница "Работники (питание)" -------------------------

# Переиспользуем автодополнение и загрузку сотрудников из модуля питания
AutoCompleteCombobox = meals_module.AutoCompleteCombobox
load_employees_from_db = meals_module.load_employees_from_db


class MealsWorkersPage(tk.Frame):
    """
    Раздел 'Работники (питание)':
      - Поиск по ФИО и/или табельному номеру
      - Показ всех случаев питания: когда, где, что, сколько и на какую сумму.
    """

    def __init__(self, master, app_ref=None):
        super().__init__(master)
        self.app_ref = app_ref

        # Справочник сотрудников
        # ожидается, что load_employees_from_db вернёт [{'fio', 'tbn', 'pos', 'dep'}, ...]
        self.employees = load_employees_from_db()
        self.emp_names = [e["fio"] for e in self.employees]
        self.emp_tbn_by_fio = {
            e["fio"]: (e["tbn"] or "") for e in self.employees
        }

        # Список подразделений
        deps_set = {
            (e.get("dep") or "").strip()
            for e in self.employees
            if (e.get("dep") or "").strip()
        }
        self.departments = ["Все"] + sorted(deps_set)

        # Переменные формы
        self.var_fio = tk.StringVar()
        self.var_tbn = tk.StringVar()
        self.var_year = tk.StringVar()
        self.var_month = tk.StringVar()
        self.var_dep = tk.StringVar()
        self.var_total_qty = tk.StringVar(value="0")
        self.var_total_amount = tk.StringVar(value="0.00")

        self.tree = None
        self._rows: List[Dict[str, Any]] = []

        self._build_ui()

    def _build_ui(self):
        # Верхняя панель с фильтрами
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=(8, 4))

        tk.Label(
            top,
            text="Работники (питание)",
            font=("Segoe UI", 12, "bold"),
        ).grid(row=0, column=0, columnspan=6, sticky="w", pady=(0, 8))

        row_f = 1

        # ФИО (автодополнение)
        tk.Label(top, text="ФИО:").grid(row=row_f, column=0, sticky="e", padx=(0, 4))
        cmb_fio = AutoCompleteCombobox(top, width=40, textvariable=self.var_fio)
        cmb_fio.set_completion_list(self.emp_names)
        cmb_fio.grid(row=row_f, column=1, sticky="w")
        cmb_fio.bind("<<ComboboxSelected>>", self._on_fio_selected)

        # Табельный №
        tk.Label(top, text="Табельный №:").grid(
            row=row_f, column=2, sticky="e", padx=(12, 4)
        )
        ent_tbn = ttk.Entry(top, width=16, textvariable=self.var_tbn)
        ent_tbn.grid(row=row_f, column=3, sticky="w")
        row_f += 1

        # Год (необязательно)
        tk.Label(top, text="Год:").grid(
            row=row_f, column=0, sticky="e", padx=(0, 4), pady=(4, 0)
        )
        spn_year = tk.Spinbox(
            top, from_=2000, to=2100, width=6, textvariable=self.var_year
        )
        spn_year.grid(row=row_f, column=1, sticky="w", pady=(4, 0))
        self.var_year.set("")  # по умолчанию — без ограничения

        # Месяц (номер 1–12, вводится числом — чтобы не тащить календарь)
        tk.Label(top, text="Месяц (1-12):").grid(
            row=row_f, column=2, sticky="e", padx=(12, 4), pady=(4, 0)
        )
        ent_month = ttk.Entry(top, width=6, textvariable=self.var_month)
        ent_month.grid(row=row_f, column=3, sticky="w", pady=(4, 0))
        row_f += 1

        # Подразделение
        tk.Label(top, text="Подразделение:").grid(
            row=row_f, column=0, sticky="e", padx=(0, 4), pady=(4, 0)
        )
        cmb_dep = ttk.Combobox(
            top,
            state="readonly",
            width=40,
            textvariable=self.var_dep,
            values=self.departments,
        )
        cmb_dep.grid(row=row_f, column=1, sticky="w", pady=(4, 0))
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
            "date",
            "address",
            "department",
            "team_name",
            "meal_type",
            "quantity",
            "price",
            "amount",
        )

        self.tree = ttk.Treeview(
            frame,
            columns=cols,
            show="headings",
            selectmode="browse",
        )

        self.tree.heading("date", text="Дата")
        self.tree.heading("address", text="Адрес (фактический)")
        self.tree.heading("department", text="Подразделение")
        self.tree.heading("team_name", text="Бригада")
        self.tree.heading("meal_type", text="Тип питания")
        self.tree.heading("quantity", text="Кол-во")
        self.tree.heading("price", text="Цена, руб.")
        self.tree.heading("amount", text="Сумма, руб.")

        self.tree.column("date", width=90, anchor="center", stretch=False)
        self.tree.column("address", width=260, anchor="w")
        self.tree.column("department", width=160, anchor="w")
        self.tree.column("team_name", width=180, anchor="w")
        self.tree.column("meal_type", width=140, anchor="w")
        self.tree.column("quantity", width=70, anchor="e", stretch=False)
        self.tree.column("price", width=80, anchor="e", stretch=False)
        self.tree.column("amount", width=90, anchor="e", stretch=False)

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

        totals = tk.Frame(self)
        totals.pack(fill="x", padx=8, pady=(0, 8))

        ttk.Separator(totals, orient="horizontal").pack(fill="x", pady=(0, 6))

        tk.Label(totals, text="ИТОГО:", font=("Segoe UI", 9, "bold")).pack(side="left")

        tk.Label(totals, text="  Кол-во:").pack(side="left")
        tk.Label(totals, textvariable=self.var_total_qty, font=("Segoe UI", 9, "bold")).pack(side="left")

        tk.Label(totals, text="   Сумма:").pack(side="left", padx=(12, 0))
        tk.Label(totals, textvariable=self.var_total_amount, font=("Segoe UI", 9, "bold")).pack(side="left")

        tk.Label(totals, text=" руб.", fg="#555").pack(side="left")

    def _on_fio_selected(self, event=None):
        fio = self.var_fio.get().strip()
        tbn = self.emp_tbn_by_fio.get(fio, "")
        if tbn:
            self.var_tbn.set(tbn)

    def _reset(self):
        self.var_fio.set("")
        self.var_tbn.set("")
        self.var_year.set("")
        self.var_month.set("")
        self.var_dep.set("Все")
        self._rows.clear()
        self.tree.delete(*self.tree.get_children())
        self.var_total_qty.set("0")
        self.var_total_amount.set("0.00")

    def _search(self):
        fio = self.var_fio.get().strip()
        tbn = self.var_tbn.get().strip()

        if not fio and not tbn:
            messagebox.showwarning(
                "Работники (питание)",
                "Введите ФИО и/или табельный номер.",
            )
            return

        # Год
        year = None
        year_txt = self.var_year.get().strip()
        if year_txt:
            try:
                y = int(year_txt)
                if 2000 <= y <= 2100:
                    year = y
                else:
                    raise ValueError
            except ValueError:
                messagebox.showwarning(
                    "Работники (питание)",
                    "Год введён некорректно (ожидается число 2000–2100).",
                )
                return

        # Месяц
        month = None
        month_txt = self.var_month.get().strip()
        if month_txt:
            try:
                m = int(month_txt)
                if 1 <= m <= 12:
                    month = m
                else:
                    raise ValueError
            except ValueError:
                messagebox.showwarning(
                    "Работники (питание)",
                    "Месяц должен быть числом от 1 до 12.",
                )
                return

        # Подразделение
        dep_val = self.var_dep.get().strip()
        if dep_val and dep_val != "Все":
            dep = dep_val
        else:
            dep = None

        try:
            rows = find_employee_meals(
                fio=fio or None,
                tbn=tbn or None,
                year=year,
                month=month,
                department=dep,
            )
        except Exception as e:
            logging.exception("Ошибка поиска по питанию работника")
            messagebox.showerror(
                "Работники (питание)",
                f"Ошибка при обращении к БД:\n{e}",
            )
            return

        self._rows = rows
        self._fill_tree()
        self._recalc_totals()

        if not rows:
            messagebox.showinfo(
                "Работники (питание)",
                "По заданным условиям ничего не найдено.",
            )

    def _fill_tree(self):
        self.tree.delete(*self.tree.get_children())

        def fmt_num(v):
            if v is None:
                return ""
            try:
                f = float(v)
            except Exception:
                return str(v)
            s = f"{f:.2f}".rstrip("0").rstrip(".")
            return s

        for idx, r in enumerate(self._rows):
            dt = r.get("date")
            if hasattr(dt, "strftime"):
                date_str = dt.strftime("%Y-%m-%d")
            else:
                date_str = str(dt or "")

            self.tree.insert(
                "",
                "end",
                iid=str(idx),
                values=(
                    date_str,
                    r.get("address") or "",
                    r.get("department") or "",
                    r.get("team_name") or "",
                    r.get("meal_type") or "",
                    fmt_num(r.get("quantity")),
                    fmt_num(r.get("price")),
                    fmt_num(r.get("amount")),
                ),
            )

    def _recalc_totals(self):
        total_qty = 0.0
        total_amount = 0.0

        for r in self._rows:
            try:
                total_qty += float(r.get("quantity") or 0)
            except Exception:
                pass
            try:
                total_amount += float(r.get("amount") or 0)
            except Exception:
                pass

        # формат
        qty_str = f"{total_qty:.2f}".rstrip("0").rstrip(".")
        amt_str = f"{total_amount:.2f}"

        self.var_total_qty.set(qty_str)
        self.var_total_amount.set(amt_str)


# ------------------------- API для main_app -------------------------

def create_meals_workers_page(parent, app_ref=None) -> MealsWorkersPage:
    """
    Создает страницу 'Работники (питание)'.
    """
    return MealsWorkersPage(parent, app_ref=app_ref)
