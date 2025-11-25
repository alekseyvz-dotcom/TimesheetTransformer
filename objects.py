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
                       contract_type,
                       status
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
    status: Optional[str] = None,
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
                           contract_type = %s,
                           status = %s
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
                        status or None,
                        obj_id,
                    ),
                )
                return obj_id
            else:
                # если статус не задан явно, считаем новый объект "Новый"
                if not status:
                    status = "Новый"

                cur.execute(
                    """
                    INSERT INTO objects (
                        excel_id, year, program_name, customer_name,
                        address, contract_number, contract_date,
                        short_name, executor_department, contract_type,
                        status
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                        status or None,
                    ),
                )
                return cur.fetchone()[0]
    finally:
        conn.close()

def get_next_excel_id() -> str:
    """
    Возвращает следующий числовой excel_id как строку.
    Берём MAX(excel_id::bigint), игнорируя нечисловые значения.
    Если нет ни одного — вернём '1'.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT MAX((NULLIF(excel_id, '')::bigint))
                  FROM objects
                 WHERE excel_id ~ '^[0-9]+$'
                """
            )
            row = cur.fetchone()
            max_id = row[0] if row else None
            if max_id is None:
                return "1"
            return str(max_id + 1)
    except Exception:
        # на всякий случай
        return "1"
    finally:
        conn.close()

# ---------- UI: страница создания/редактирования объекта ----------

class ObjectCreatePage(tk.Frame):
    """
    Страница создания/редактирования одного объекта.
    """
    def __init__(self, master, app_ref: "MainApp", obj_data: Optional[Dict[str, Any]] = None):
        super().__init__(master, bg="#f7f7f7")
        self.app_ref = app_ref
        self.obj_data = obj_data or {}
        self._build_ui()
        self._fill_from_data_or_default()

    def _build_ui(self):
        # Заголовок
        header = tk.Frame(self, bg="#f7f7f7")
        header.pack(fill="x", padx=12, pady=(10, 4))

        tk.Label(
            header,
            text="Создание объекта",
            font=("Segoe UI", 14, "bold"),
            bg="#f7f7f7",
        ).pack(side="left")

        tk.Label(
            header,
            text="Укажите основные данные по объекту, затем нажмите «Сохранить»",
            font=("Segoe UI", 9),
            fg="#555",
            bg="#f7f7f7",
        ).pack(side="right")

        # Основная область
        body_outer = tk.Frame(self, bg="#f7f7f7")
        body_outer.pack(fill="both", expand=True, padx=12, pady=8)

        body = tk.Frame(body_outer)
        body.pack(fill="both", expand=True)

        # Левая и правая колонка
        left = tk.LabelFrame(body, text="Общие сведения", padx=10, pady=8)
        right = tk.LabelFrame(body, text="Договор", padx=10, pady=8)

        left.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=0)
        right.grid(row=0, column=1, sticky="nsew", padx=(8, 0), pady=0)

        body.grid_columnconfigure(0, weight=1)
        body.grid_columnconfigure(1, weight=1)

        # ---------- Левая колонка: общие сведения ----------
        row_l = 0

        def add_left(label: str, var_name: str, width: int = 40, note: str = ""):
            nonlocal row_l
            tk.Label(left, text=label, anchor="e").grid(
                row=row_l, column=0, sticky="e", padx=(0, 6), pady=3
            )
            var = tk.StringVar()
            ent = ttk.Entry(left, textvariable=var, width=width)
            ent.grid(row=row_l, column=1, sticky="w", pady=3)
            if note:
                tk.Label(left, text=note, fg="#777", font=("Segoe UI", 8)).grid(
                    row=row_l, column=2, sticky="w", padx=(6, 0)
                )
            setattr(self, f"var_{var_name}", var)
            setattr(self, f"ent_{var_name}", ent)
            row_l += 1

        add_left("ID объекта (excel_id):", "excel_id", width=16, note="числовой, подставляется автоматически")
        add_left("Год реализации программы:", "year", width=8)
        add_left("Наименование программы:", "program_name", width=46)
        add_left("Наименование заказчика:", "customer_name", width=46)
        add_left("Адрес объекта:", "address", width=52)

        # ---------- Правая колонка: договор ----------
        row_r = 0

        def add_right(label: str, var_name: str, width: int = 32, note: str = ""):
            nonlocal row_r
            tk.Label(right, text=label, anchor="e").grid(
                row=row_r, column=0, sticky="e", padx=(0, 6), pady=3
            )
            var = tk.StringVar()
            ent = ttk.Entry(right, textvariable=var, width=width)
            ent.grid(row=row_r, column=1, sticky="w", pady=3)
            if note:
                tk.Label(right, text=note, fg="#777", font=("Segoe UI", 8)).grid(
                    row=row_r, column=2, sticky="w", padx=(6, 0)
                )
            setattr(self, f"var_{var_name}", var)
            setattr(self, f"ent_{var_name}", ent)
            row_r += 1

        add_right("№ договора:", "contract_number", width=20)
        add_right("Дата договора:", "contract_date", width=12, note="ДД.ММ.ГГГГ")
        add_right("Сокращённое наименование:", "short_name", width=40)
        add_right("Подразделение исполнителя:", "executor_department", width=34)
        add_right("Тип договора:", "contract_type", width=26)

        # Нижняя панель с кнопками
        btns = tk.Frame(self, bg="#f7f7f7")
        btns.pack(fill="x", padx=12, pady=(4, 10))

        ttk.Button(btns, text="Сохранить", command=self._on_save).pack(side="right", padx=4)
        ttk.Button(btns, text="Очистить", command=self._on_clear).pack(side="right", padx=4)

    # ---------- заполнение полей ----------

    def _fill_from_data_or_default(self):
        d = self.obj_data
        if d:
            # режим редактирования (на будущее)
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
        else:
            # новый объект — подставляем следующий excel_id
            try:
                next_id = get_next_excel_id()
            except Exception:
                next_id = "1"
            self.var_excel_id.set(next_id)

    # ---------- действия ----------

    def _on_clear(self):
        # при очистке excel_id тоже можно пересчитать
        try:
            next_id = get_next_excel_id()
        except Exception:
            next_id = ""
        for name in (
            "excel_id", "year", "program_name", "customer_name", "address",
            "contract_number", "contract_date", "short_name",
            "executor_department", "contract_type",
        ):
            getattr(self, f"var_{name}").set("")
        if next_id:
            self.var_excel_id.set(next_id)

    def _on_save(self):
        addr = self.var_address.get().strip()
        if not addr:
            messagebox.showwarning("Объект", "Адрес объекта обязателен.")
            return

        # excel_id: проверим, что это число (по твоей логике)
        excel_id_raw = self.var_excel_id.get().strip()
        if excel_id_raw:
            if not excel_id_raw.isdigit():
                if not messagebox.askyesno(
                    "ID объекта",
                    "ID (excel_id) не является числом.\n"
                    "Продолжить сохранение с таким значением?",
                ):
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
                excel_id=excel_id_raw or None,
                year=self.var_year.get().strip() or None,
                program_name=self.var_program_name.get().strip() or None,
                customer_name=self.var_customer_name.get().strip() or None,
                address=addr,
                contract_number=self.var_contract_number.get().strip() or None,
                contract_date=cd_val,
                short_name=self.var_short_name.get().strip() or None,
                executor_department=self.var_executor_department.get().strip() or None,
                contract_type=self.var_contract_type.get().strip() or None,
                status = self.obj_data.get("status") or "Новый"
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

        # Панель смены статуса
        status_frame = tk.Frame(top)
        status_frame.grid(row=2, column=0, columnspan=5, sticky="w", pady=(6, 0))

        tk.Label(status_frame, text="Статус выбранного объекта:").pack(side="left", padx=(0, 4))

        self.var_status = tk.StringVar(value="Новый")
        cmb_status = ttk.Combobox(
            status_frame,
            textvariable=self.var_status,
            values=["Новый", "В работе", "Закрыт"],
            width=12,
            state="readonly",
        )
        cmb_status.pack(side="left", padx=(0, 4))

        ttk.Button(
            status_frame,
            text="Установить статус",
            command=self._on_change_status
        ).pack(side="left", padx=(4, 0))

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
            "status",
        )
        self.tree = ttk.Treeview(frame, columns=cols, show="headings", selectmode="browse")

        # Настройка цветов по статусам
        # Цвета можете подобрать другие
        self.tree.tag_configure("status_new", background="#e0ffe0")       # светло-зелёный
        self.tree.tag_configure("status_inwork", background="#fff8dc")    # светло-жёлтый
        self.tree.tag_configure("status_closed", background="#ffe4e1")    # светло-розовый

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
        self.tree.heading("status", text="Статус")

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
        self.tree.column("status", width=90, anchor="center")


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

            status = o.get("status") or "Новый"

            # определяем тег по статусу
            if status == "Новый":
                tags = ("status_new",)
            elif status == "В работе":
                tags = ("status_inwork",)
            elif status == "Закрыт":
                tags = ("status_closed",)
            else:
                tags = ()

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
                    status,
                ),
                tags=tags,
            )

    def _on_change_status(self):
        """Установить новый статус для выбранной строки."""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Статус", "Выберите объект в списке.")
            return

        item_id = selected[0]
        # индекс выбранной строки в self._objects
        index = self.tree.index(item_id)
        if index < 0 or index >= len(self._objects):
            messagebox.showerror("Статус", "Не удалось определить объект.")
            return

        obj = self._objects[index]
        obj_db_id = obj.get("id")
        if not obj_db_id:
            messagebox.showerror("Статус", "У объекта нет ID в базе.")
            return

        new_status = self.var_status.get()
        if new_status not in ("Новый", "В работе", "Закрыт"):
            messagebox.showerror("Статус", "Недопустимое значение статуса.")
            return

        # Обновляем статус в БД
        conn = get_db_connection()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE objects SET status = %s WHERE id = %s",
                    (new_status, obj_db_id),
                )
        except Exception as e:
            logging.exception("Ошибка смены статуса объекта")
            messagebox.showerror("Статус", f"Ошибка обновления статуса в БД:\n{e}")
            return
        finally:
            conn.close()

        # Обновляем локальный объект и перерисовываем реестр
        obj["status"] = new_status
        self._load_data()

