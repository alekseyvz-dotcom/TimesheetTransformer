# objects.py
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional

import tkinter as tk
from tkinter import ttk, messagebox

try:
    from psycopg2.extras import RealDictCursor
except Exception:
    RealDictCursor = None  # тип, чтобы не падать при импорте без psycopg2

# ВАЖНО: импортируем get_db_connection и month_name_ru из main_app
from main_app import get_db_connection, month_name_ru


# ---------- БД: объекты ----------

def load_objects_full_from_db() -> List[Dict[str, Any]]:
    """
    Возвращает все объекты со всеми основными полями.
    Колонки совпадают с import_objects_from_excel.
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id,
                       excel_id,
                       year,
                       program_name,
                       customer_name,
                       address,
                       contract_number,
                       contract_date,
                       short_name,
                       executor_department,
                       contract_type
                  FROM objects
              ORDER BY address
                """
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def create_or_update_object(
    obj_id: Optional[int],
    excel_id: Optional[str],
    year: Optional[str],
    program_name: Optional[str],
    customer_name: Optional[str],
    address: str,
    contract_number: Optional[str],
    contract_date: Optional[date],
    short_name: Optional[str],
    executor_department: Optional[str],
    contract_type: Optional[str],
) -> int:
    """
    Создаёт новый объект или обновляет существующий.
    Возвращает id объекта.
    """
    conn = get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            if obj_id:
                cur.execute(
                    """
                    UPDATE objects
                       SET excel_id = %s,
                           year = %s,
                           program_name = %s,
                           customer_name = %s,
                           address = %s,
                           contract_number = %s,
                           contract_date = %s,
                           short_name = %s,
                           executor_department = %s,
                           contract_type = %s
                     WHERE id = %s
                    """,
                    (
                        excel_id or None,
                        year or None,
                        program_name or None,
                        customer_name or None,
                        address or None,
                        contract_number or None,
                        contract_date,
                        short_name or None,
                        executor_department or None,
                        contract_type or None,
                        obj_id,
                    ),
                )
                return obj_id
            else:
                cur.execute(
                    """
                    INSERT INTO objects (
                        excel_id, year, program_name, customer_name,
                        address, contract_number, contract_date,
                        short_name, executor_department, contract_type
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                    """,
                    (
                        excel_id or None,
                        year or None,
                        program_name or None,
                        customer_name or None,
                        address or None,
                        contract_number or None,
                        contract_date,
                        short_name or None,
                        executor_department or None,
                        contract_type or None,
                    ),
                )
                return cur.fetchone()[0]
    finally:
        conn.close()


# ---------- UI: страница создания/редактирования объекта ----------

class ObjectCreatePage(tk.Frame):
    """
    Страница создания/редактирования одного объекта.
    Пока используется для создания новых объектов из меню.
    """
    def __init__(self, master, app_ref: "MainApp", obj_data: Optional[Dict[str, Any]] = None):
        super().__init__(master)
        self.app_ref = app_ref
        self.obj_data = obj_data or {}
        self._build_ui()
        self._fill_from_data()

    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=8)

        tk.Label(top, text="Создание объекта", font=("Segoe UI", 12, "bold")).pack(side="left")

        body = tk.Frame(self)
        body.pack(fill="both", expand=True, padx=12, pady=8)

        lbl_w = 26
        row = 0

        def add_row(label: str, var_name: str, width: int = 40):
            nonlocal row
            tk.Label(body, text=label, anchor="e", width=lbl_w).grid(
                row=row, column=0, sticky="e", padx=(0, 6), pady=3
            )
            var = tk.StringVar()
            ent = ttk.Entry(body, textvariable=var, width=width)
            ent.grid(row=row, column=1, sticky="w", pady=3)
            setattr(self, f"var_{var_name}", var)
            setattr(self, f"ent_{var_name}", ent)
            row += 1

        add_row("ID (excel_id):", "excel_id", width=20)
        add_row("Год реализации программы:", "year", width=10)
        add_row("Наименование программы:", "program_name", width=50)
        add_row("Наименование заказчика:", "customer_name", width=50)
        add_row("Адрес объекта:", "address", width=60)
        add_row("№ договора:", "contract_number", width=20)
        add_row("Дата договора (ДД.ММ.ГГГГ):", "contract_date", width=16)
        add_row("Сокращённое наименование объекта:", "short_name", width=50)
        add_row("Подразделение исполнителя:", "executor_department", width=40)
        add_row("Тип договора:", "contract_type", width=30)

        btns = tk.Frame(self)
        btns.pack(fill="x", padx=12, pady=(4, 10))
        ttk.Button(btns, text="Сохранить", command=self._on_save).pack(side="right", padx=4)
        ttk.Button(btns, text="Очистить", command=self._on_clear).pack(side="right", padx=4)

    def _fill_from_data(self):
        d = self.obj_data
        if not d:
            return
        self.var_excel_id.set(d.get("excel_id") or "")
        self.var_year.set(d.get("year") or "")
        self.var_program_name.set(d.get("program_name") or "")
        self.var_customer_name.set(d.get("customer_name") or "")
        self.var_address.set(d.get("address") or "")
        self.var_contract_number.set(d.get("contract_number") or "")
        cd = d.get("contract_date")
        if isinstance(cd, (datetime, date)):
            self.var_contract_date.set(cd.strftime("%d.%m.%Y"))
        elif cd:
            self.var_contract_date.set(str(cd))
        self.var_short_name.set(d.get("short_name") or "")
        self.var_executor_department.set(d.get("executor_department") or "")
        self.var_contract_type.set(d.get("contract_type") or "")

    def _on_clear(self):
        for name in (
            "excel_id", "year", "program_name", "customer_name", "address",
            "contract_number", "contract_date", "short_name",
            "executor_department", "contract_type",
        ):
            getattr(self, f"var_{name}").set("")

    def _on_save(self):
        addr = self.var_address.get().strip()
        if not addr:
            messagebox.showwarning("Объект", "Адрес объекта обязателен.")
            return

        cd_raw = self.var_contract_date.get().strip()
        cd_val: Optional[date] = None
        if cd_raw:
            try:
                cd_val = datetime.strptime(cd_raw, "%d.%m.%Y").date()
            except Exception:
                messagebox.showwarning(
                    "Объект",
                    "Дата договора должна быть в формате ДД.ММ.ГГГГ или оставьте поле пустым.",
                )
                return

        try:
            obj_id = self.obj_data.get("id") if self.obj_data else None
        except Exception:
            obj_id = None

        try:
            new_id = create_or_update_object(
                obj_id=obj_id,
                excel_id=self.var_excel_id.get().strip() or None,
                year=self.var_year.get().strip() or None,
                program_name=self.var_program_name.get().strip() or None,
                customer_name=self.var_customer_name.get().strip() or None,
                address=addr,
                contract_number=self.var_contract_number.get().strip() or None,
                contract_date=cd_val,
                short_name=self.var_short_name.get().strip() or None,
                executor_department=self.var_executor_department.get().strip() or None,
                contract_type=self.var_contract_type.get().strip() or None,
            )
        except Exception as e:
            logging.exception("Ошибка сохранения объекта")
            messagebox.showerror("Объект", f"Ошибка сохранения в БД:\n{e}")
            return

        self.obj_data["id"] = new_id
        messagebox.showinfo("Объект", "Объект сохранён в базе данных.")


# ---------- UI: реестр объектов ----------

class ObjectsRegistryPage(tk.Frame):
    """
    Реестр всех объектов из таблицы objects.
    """
    def __init__(self, master, app_ref: "MainApp"):
        super().__init__(master)
        self.app_ref = app_ref
        self.tree = None
        self._objects: List[Dict[str, Any]] = []
        self.var_filter_addr = tk.StringVar()
        self.var_filter_excel = tk.StringVar()
        self._build_ui()
        self._load_data()

    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=(8, 4))

        tk.Label(top, text="Реестр объектов", font=("Segoe UI", 12, "bold")).grid(
            row=0, column=0, columnspan=4, sticky="w"
        )

        tk.Label(top, text="Фильтр по адресу:").grid(row=1, column=0, sticky="e", padx=(0, 4), pady=(4, 0))
        ent_addr = ttk.Entry(top, textvariable=self.var_filter_addr, width=40)
        ent_addr.grid(row=1, column=1, sticky="w", pady=(4, 0))

        tk.Label(top, text="Фильтр по ID объекта:").grid(row=1, column=2, sticky="e", padx=(12, 4), pady=(4, 0))
        ent_excel = ttk.Entry(top, textvariable=self.var_filter_excel, width=18)
        ent_excel.grid(row=1, column=3, sticky="w", pady=(4, 0))

        btns = tk.Frame(top)
        btns.grid(row=1, column=4, sticky="w", padx=(8, 0), pady=(4, 0))
        ttk.Button(btns, text="Применить", command=self._load_data).pack(side="left", padx=2)
        ttk.Button(btns, text="Сброс", command=self._reset_filters).pack(side="left", padx=2)

        frame = tk.Frame(self)
        frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        cols = (
            "excel_id",
            "address",
            "year",
            "program_name",
            "customer_name",
            "short_name",
            "executor_department",
            "contract_number",
            "contract_date",
            "contract_type",
        )
        self.tree = ttk.Treeview(frame, columns=cols, show="headings", selectmode="browse")

        self.tree.heading("excel_id", text="ID объекта")
        self.tree.heading("address", text="Адрес")
        self.tree.heading("year", text="Год")
        self.tree.heading("program_name", text="Программа")
        self.tree.heading("customer_name", text="Заказчик")
        self.tree.heading("short_name", text="Краткое имя")
        self.tree.heading("executor_department", text="Подразделение исполнителя")
        self.tree.heading("contract_number", text="№ договора")
        self.tree.heading("contract_date", text="Дата договора")
        self.tree.heading("contract_type", text="Тип договора")

        self.tree.column("excel_id", width=90, anchor="w")
        self.tree.column("address", width=260, anchor="w")
        self.tree.column("year", width=60, anchor="center")
        self.tree.column("program_name", width=180, anchor="w")
        self.tree.column("customer_name", width=160, anchor="w")
        self.tree.column("short_name", width=160, anchor="w")
        self.tree.column("executor_department", width=160, anchor="w")
        self.tree.column("contract_number", width=110, anchor="w")
        self.tree.column("contract_date", width=100, anchor="center")
        self.tree.column("contract_type", width=120, anchor="w")

        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

    def _reset_filters(self):
        self.var_filter_addr.set("")
        self.var_filter_excel.set("")
        self._load_data()

    def _load_data(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        self._objects.clear()

        try:
            objs = load_objects_full_from_db()
        except Exception as e:
            logging.exception("Ошибка загрузки реестра объектов")
            messagebox.showerror("Реестр объектов", f"Ошибка загрузки объектов из БД:\n{e}")
            return

        addr_filter = (self.var_filter_addr.get() or "").strip().lower()
        excel_filter = (self.var_filter_excel.get() or "").strip().lower()

        for o in objs:
            addr = (o.get("address") or "").strip()
            excel_id = (o.get("excel_id") or "").strip()

            if addr_filter and addr_filter not in addr.lower():
                continue
            if excel_filter and excel_filter not in excel_id.lower():
                continue

            self._objects.append(o)

            cd = o.get("contract_date")
            if isinstance(cd, (datetime, date)):
                cd_str = cd.strftime("%d.%m.%Y")
            else:
                cd_str = str(cd or "")

            self.tree.insert(
                "",
                "end",
                values=(
                    excel_id,
                    addr,
                    o.get("year") or "",
                    o.get("program_name") or "",
                    o.get("customer_name") or "",
                    o.get("short_name") or "",
                    o.get("executor_department") or "",
                    o.get("contract_number") or "",
                    cd_str,
                    o.get("contract_type") or "",
                ),
            )
