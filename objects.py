# objects.py — профессиональный модуль реестра объектов PRO+
from __future__ import annotations

import os
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional

import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

try:
    from psycopg2.extras import RealDictCursor
except Exception:
    RealDictCursor = None


logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  THEME
# ═══════════════════════════════════════════════════════════════
C = {
    "bg": "#f0f2f5",
    "panel": "#ffffff",
    "accent": "#1565c0",
    "accent_light": "#e3f2fd",
    "success": "#2e7d32",
    "warning": "#ed6c02",
    "error": "#d32f2f",
    "border": "#dde1e7",
    "text": "#1a1a2e",
    "text2": "#555",
    "text3": "#999",
    "btn_bg": "#1565c0",
    "btn_fg": "#ffffff",
    "readonly": "#f7f9fc",
}

OBJECT_STATUSES = ["Новый", "В работе", "Закрыт"]

STATUS_TAGS = {
    "Новый": "status_new",
    "В работе": "status_inwork",
    "Закрыт": "status_closed",
}

# ═══════════════════════════════════════════════════════════════
#  DB POOL
# ═══════════════════════════════════════════════════════════════
db_connection_pool = None


def set_db_pool(pool):
    global db_connection_pool
    db_connection_pool = pool


def get_db_connection():
    if db_connection_pool is None:
        raise RuntimeError(
            "Пул соединений не был установлен из главного приложения."
        )
    return db_connection_pool.getconn()


def release_db_connection(conn):
    if db_connection_pool and conn:
        db_connection_pool.putconn(conn)


class _DBConn:
    """Context-manager для безопасной работы с БД."""

    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = get_db_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is not None:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
            release_db_connection(self.conn)
            self.conn = None
        return False


# ═══════════════════════════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════════════════════════
def month_name_ru(month: int) -> str:
    names = [
        "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
        "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
    ]
    if 1 <= month <= 12:
        return names[month - 1]
    return str(month)


def _clean_text(v: Any) -> str:
    return str(v or "").strip()


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def _fmt_date(v: Any) -> str:
    if not v:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%d.%m.%Y")
    if isinstance(v, date):
        return v.strftime("%d.%m.%Y")
    return str(v)


def _fmt_datetime(v: Any) -> str:
    if not v:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%d.%m.%Y %H:%M")
    return str(v)


def _parse_ru_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    return datetime.strptime(s, "%d.%m.%Y").date()


def _today_year() -> int:
    return datetime.now().year


def _can_edit(role: str) -> bool:
    return role in ("admin", "manager")


# ═══════════════════════════════════════════════════════════════
#  SERVICE LAYER
# ═══════════════════════════════════════════════════════════════
class ObjectsService:
    """Сервисный слой для работы с таблицей objects."""

    @staticmethod
    def list_objects() -> List[Dict[str, Any]]:
        with _DBConn() as conn:
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
                      FROM public.objects
                  ORDER BY
                       CASE
                           WHEN excel_id ~ '^[0-9]+$' THEN LPAD(excel_id, 20, '0')
                           ELSE excel_id
                       END,
                       address,
                       id
                    """
                )
                return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def get_unique_program_names() -> List[str]:
        with _DBConn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT program_name
                      FROM public.objects
                     WHERE program_name IS NOT NULL
                       AND program_name <> ''
                  ORDER BY program_name
                    """
                )
                return [row[0] for row in cur.fetchall()]

    @staticmethod
    def get_unique_years() -> List[str]:
        with _DBConn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT year
                      FROM public.objects
                     WHERE year IS NOT NULL
                       AND year <> ''
                  ORDER BY year DESC
                    """
                )
                return [str(row[0]) for row in cur.fetchall()]

    @staticmethod
    def get_next_excel_id(max_len: int = 6) -> str:
        """
        Возвращает следующий порядковый excel_id как строку.
        Игнорирует нечисловые и слишком длинные значения.
        """
        try:
            with _DBConn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COALESCE(MAX(excel_id::bigint), 0)
                          FROM public.objects
                         WHERE excel_id ~ '^[0-9]+$'
                           AND length(excel_id) <= %s
                        """,
                        (max_len,),
                    )
                    row = cur.fetchone()
                    max_id = row[0] if row and row[0] is not None else 0
                    return str(int(max_id) + 1)
        except Exception:
            logger.exception("Ошибка получения следующего excel_id")
            return "1"

    @staticmethod
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
        excel_id = _clean_text(excel_id) or None
        year = _clean_text(year) or None
        program_name = _clean_text(program_name) or None
        customer_name = _clean_text(customer_name) or None
        address = _clean_text(address)
        contract_number = _clean_text(contract_number) or None
        short_name = _clean_text(short_name) or None
        executor_department = _clean_text(executor_department) or None
        contract_type = _clean_text(contract_type) or None
        status = _clean_text(status) or "Новый"

        if not address:
            raise ValueError("Адрес объекта обязателен.")

        if status not in OBJECT_STATUSES:
            raise ValueError(f"Недопустимый статус: {status}")

        with _DBConn() as conn:
            with conn:
                with conn.cursor() as cur:
                    if obj_id:
                        cur.execute(
                            """
                            SELECT 1
                              FROM public.objects
                             WHERE excel_id = %s
                               AND id <> %s
                             LIMIT 1
                            """,
                            (excel_id, obj_id),
                        )
                        if excel_id and cur.fetchone():
                            raise ValueError(
                                f"ID объекта '{excel_id}' уже используется."
                            )

                        cur.execute(
                            """
                            UPDATE public.objects
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
                                status,
                                obj_id,
                            ),
                        )
                        return int(obj_id)

                    if not excel_id:
                        excel_id = ObjectsService.get_next_excel_id()

                    cur.execute(
                        """
                        SELECT 1
                          FROM public.objects
                         WHERE excel_id = %s
                         LIMIT 1
                        """,
                        (excel_id,),
                    )
                    if excel_id and cur.fetchone():
                        raise ValueError(
                            f"Объект с ID объекта '{excel_id}' уже существует."
                        )

                    cur.execute(
                        """
                        INSERT INTO public.objects (
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
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
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
                            status,
                        ),
                    )
                    return int(cur.fetchone()[0])

    @staticmethod
    def update_status(obj_id: int, new_status: str) -> None:
        if new_status not in OBJECT_STATUSES:
            raise ValueError("Недопустимое значение статуса.")
        with _DBConn() as conn:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE public.objects SET status = %s WHERE id = %s",
                        (new_status, obj_id),
                    )


# ═══════════════════════════════════════════════════════════════
#  BACKWARD COMPAT API
# ═══════════════════════════════════════════════════════════════
def load_objects_full_from_db() -> List[Dict[str, Any]]:
    return ObjectsService.list_objects()


def get_unique_program_names() -> List[str]:
    try:
        return ObjectsService.get_unique_program_names()
    except Exception as e:
        logger.error("Ошибка получения списка программ: %s", e)
        return []


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
    return ObjectsService.create_or_update_object(
        obj_id=obj_id,
        excel_id=excel_id,
        year=year,
        program_name=program_name,
        customer_name=customer_name,
        address=address,
        contract_number=contract_number,
        contract_date=contract_date,
        short_name=short_name,
        executor_department=executor_department,
        contract_type=contract_type,
        status=status,
    )


def get_next_excel_id() -> str:
    return ObjectsService.get_next_excel_id()


# ═══════════════════════════════════════════════════════════════
#  EDIT DIALOG
# ═══════════════════════════════════════════════════════════════
class ObjectEditDialog(tk.Toplevel):
    """Диалог редактирования объекта из реестра без изменения ID и статуса."""

    def __init__(self, parent, obj_data: Dict[str, Any]):
        super().__init__(parent)
        self.title("Редактирование объекта")
        self.obj_data = obj_data
        self.result = None

        self.transient(parent)
        self.grab_set()
        self.configure(bg=C["bg"])

        frm = tk.Frame(self, bg=C["panel"], padx=12, pady=12)
        frm.pack(fill="both", expand=True)

        row = 0

        def add_row(label, key, width=40):
            nonlocal row
            tk.Label(
                frm,
                text=label + ":",
                anchor="e",
                bg=C["panel"],
                fg=C["text2"],
            ).grid(row=row, column=0, sticky="e", padx=(0, 8), pady=4)
            var = tk.StringVar(value=str(self.obj_data.get(key) or ""))
            ent = ttk.Entry(frm, textvariable=var, width=width)
            ent.grid(row=row, column=1, sticky="w", pady=4)
            row += 1
            return var, ent

        tk.Label(
            frm, text="ID объекта:", anchor="e",
            bg=C["panel"], fg=C["text2"]
        ).grid(row=row, column=0, sticky="e", padx=(0, 8), pady=4)
        tk.Label(
            frm, text=str(self.obj_data.get("excel_id") or ""),
            anchor="w", bg=C["panel"], fg=C["text"]
        ).grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        tk.Label(
            frm, text="Статус:", anchor="e",
            bg=C["panel"], fg=C["text2"]
        ).grid(row=row, column=0, sticky="e", padx=(0, 8), pady=4)
        tk.Label(
            frm, text=str(self.obj_data.get("status") or "Новый"),
            anchor="w", bg=C["panel"], fg=C["text"]
        ).grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        self.var_year, _ = add_row("Год реализации программы", "year", width=10)

        tk.Label(
            frm,
            text="Наименование программы:",
            anchor="e",
            bg=C["panel"],
            fg=C["text2"],
        ).grid(row=row, column=0, sticky="e", padx=(0, 8), pady=4)
        self.var_program_name = tk.StringVar(
            value=str(self.obj_data.get("program_name") or "")
        )
        self.cmb_program_name = ttk.Combobox(
            frm,
            textvariable=self.var_program_name,
            values=get_unique_program_names(),
            width=46,
        )
        self.cmb_program_name.grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        self.var_customer_name, _ = add_row(
            "Наименование заказчика", "customer_name", width=46
        )
        self.var_address, _ = add_row("Адрес объекта", "address", width=54)
        self.var_short_name, _ = add_row(
            "Сокращённое наименование", "short_name", width=46
        )
        self.var_executor_department, _ = add_row(
            "Подразделение исполнителя", "executor_department", width=46
        )
        self.var_contract_number, _ = add_row("№ договора", "contract_number", width=20)

        tk.Label(
            frm,
            text="Дата договора:",
            anchor="e",
            bg=C["panel"],
            fg=C["text2"],
        ).grid(row=row, column=0, sticky="e", padx=(0, 8), pady=4)
        cd_val = self.obj_data.get("contract_date")
        if isinstance(cd_val, (datetime, date)):
            cd_str = cd_val.strftime("%d.%m.%Y")
        else:
            cd_str = str(cd_val or "")
        self.var_contract_date = tk.StringVar(value=cd_str)
        ttk.Entry(frm, textvariable=self.var_contract_date, width=14).grid(
            row=row, column=1, sticky="w", pady=4
        )
        row += 1

        self.var_contract_type, _ = add_row("Тип договора", "contract_type", width=26)

        btns = tk.Frame(frm, bg=C["panel"])
        btns.grid(row=row, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(btns, text="Сохранить", command=self._on_ok).pack(side="left", padx=4)
        ttk.Button(btns, text="Отмена", command=self._on_cancel).pack(side="left", padx=4)

        self.bind("<Return>", lambda e: self._on_ok())
        self.bind("<Escape>", lambda e: self._on_cancel())

        self.update_idletasks()
        try:
            px = parent.winfo_rootx()
            py = parent.winfo_rooty()
            pw = parent.winfo_width()
            ph = parent.winfo_height()
            sw = self.winfo_width()
            sh = self.winfo_height()
            self.geometry(f"+{px + (pw - sw)//2}+{py + (ph - sh)//2}")
        except Exception:
            pass

    def _on_ok(self):
        try:
            cd_val = _parse_ru_date(self.var_contract_date.get())
        except Exception:
            messagebox.showwarning(
                "Объект",
                "Дата договора должна быть в формате ДД.ММ.ГГГГ или оставьте поле пустым.",
                parent=self,
            )
            return

        self.result = {
            "year": self.var_year.get().strip() or None,
            "program_name": self.var_program_name.get().strip() or None,
            "customer_name": self.var_customer_name.get().strip() or None,
            "address": self.var_address.get().strip() or None,
            "short_name": self.var_short_name.get().strip() or None,
            "executor_department": self.var_executor_department.get().strip() or None,
            "contract_number": self.var_contract_number.get().strip() or None,
            "contract_date": cd_val,
            "contract_type": self.var_contract_type.get().strip() or None,
        }
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()


# ═══════════════════════════════════════════════════════════════
#  OBJECT CREATE PAGE (отдельный раздел)
# ═══════════════════════════════════════════════════════════════
class ObjectCreatePage(tk.Frame):
    """
    Отдельный раздел создания/редактирования одного объекта.
    """

    def __init__(self, master, app_ref=None, obj_data: Optional[Dict[str, Any]] = None):
        super().__init__(master, bg=C["bg"])
        self.app_ref = app_ref
        self.obj_data = obj_data or {}
        self.current_role = (
            (getattr(self.app_ref, "current_user", {}) or {}).get("role")
            or "specialist"
        )

        self._build_ui()
        self._fill_from_data_or_default()
        self._apply_permissions()

    def _build_ui(self):
        header = tk.Frame(self, bg=C["accent"], pady=6)
        header.pack(fill="x")

        title = "Редактирование объекта" if self.obj_data else "Создание объекта"
        tk.Label(
            header,
            text=f"🏗  {title}",
            font=("Segoe UI", 12, "bold"),
            bg=C["accent"],
            fg="white",
            padx=12,
        ).pack(side="left")

        tk.Label(
            header,
            text="Отдельный раздел для создания объектов",
            font=("Segoe UI", 9),
            fg="#bbdefb",
            bg=C["accent"],
            padx=12,
        ).pack(side="right")

        outer = tk.Frame(self, bg=C["bg"])
        outer.pack(fill="both", expand=True, padx=10, pady=8)

        body = tk.Frame(outer, bg=C["bg"])
        body.pack(fill="both", expand=True)

        left = tk.LabelFrame(
            body,
            text=" Общие сведения ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=10,
            pady=8,
        )
        right = tk.LabelFrame(
            body,
            text=" Договор ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=10,
            pady=8,
        )

        left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        right.grid(row=0, column=1, sticky="nsew", padx=(8, 0))
        body.grid_columnconfigure(0, weight=1)
        body.grid_columnconfigure(1, weight=1)

        row_l = 0

        def add_entry(parent, label, var_name, width=40, note=""):
            nonlocal row_l
            tk.Label(parent, text=label, anchor="e", bg=C["panel"], fg=C["text2"]).grid(
                row=row_l, column=0, sticky="e", padx=(0, 6), pady=4
            )
            var = tk.StringVar()
            ent = ttk.Entry(parent, textvariable=var, width=width)
            ent.grid(row=row_l, column=1, sticky="w", pady=4)
            if note:
                tk.Label(
                    parent,
                    text=note,
                    fg=C["text3"],
                    bg=C["panel"],
                    font=("Segoe UI", 8),
                ).grid(row=row_l, column=2, sticky="w", padx=(6, 0))
            setattr(self, f"var_{var_name}", var)
            setattr(self, f"ent_{var_name}", ent)
            row_l += 1

        add_entry(
            left,
            "ID объекта (excel_id):",
            "excel_id",
            width=16,
            note="подставляется автоматически",
        )
        add_entry(left, "Год реализации программы:", "year", width=8)

        tk.Label(
            left,
            text="Наименование программы:",
            anchor="e",
            bg=C["panel"],
            fg=C["text2"],
        ).grid(row=row_l, column=0, sticky="e", padx=(0, 6), pady=4)
        self.var_program_name = tk.StringVar()
        self.cmb_program_name = ttk.Combobox(
            left,
            textvariable=self.var_program_name,
            values=get_unique_program_names(),
            width=44,
        )
        self.cmb_program_name.grid(row=row_l, column=1, sticky="w", pady=4)
        row_l += 1

        add_entry(left, "Наименование заказчика:", "customer_name", width=46)
        add_entry(left, "Адрес объекта:", "address", width=52)

        row_r = 0

        def add_right(label: str, var_name: str, width: int = 32, note: str = ""):
            nonlocal row_r
            tk.Label(right, text=label, anchor="e", bg=C["panel"], fg=C["text2"]).grid(
                row=row_r, column=0, sticky="e", padx=(0, 6), pady=4
            )
            var = tk.StringVar()
            ent = ttk.Entry(right, textvariable=var, width=width)
            ent.grid(row=row_r, column=1, sticky="w", pady=4)
            if note:
                tk.Label(
                    right,
                    text=note,
                    fg=C["text3"],
                    bg=C["panel"],
                    font=("Segoe UI", 8),
                ).grid(row=row_r, column=2, sticky="w", padx=(6, 0))
            setattr(self, f"var_{var_name}", var)
            setattr(self, f"ent_{var_name}", ent)
            row_r += 1

        add_right("№ договора:", "contract_number", width=20)
        add_right("Дата договора:", "contract_date", width=12, note="ДД.ММ.ГГГГ")
        add_right("Сокращённое наименование:", "short_name", width=40)
        add_right("Подразделение исполнителя:", "executor_department", width=34)
        add_right("Тип договора:", "contract_type", width=26)

        info = tk.Label(
            self,
            text="После сохранения форма подготовится для ввода следующего объекта.",
            bg=C["bg"],
            fg=C["text2"],
            anchor="w",
            padx=12,
        )
        info.pack(fill="x", pady=(0, 2))

        btns = tk.Frame(self, bg=C["accent_light"], pady=6)
        btns.pack(fill="x", padx=10, pady=(2, 10))

        self.btn_clear = ttk.Button(btns, text="Очистить форму", command=self._on_clear)
        self.btn_clear.pack(side="right", padx=4)

        self.btn_save = tk.Button(
            btns,
            text="💾 Сохранить",
            font=("Segoe UI", 9, "bold"),
            bg=C["btn_bg"],
            fg=C["btn_fg"],
            activebackground="#0d47a1",
            activeforeground="white",
            relief="flat",
            cursor="hand2",
            padx=10,
            pady=3,
            command=self._on_save,
        )
        self.btn_save.pack(side="right", padx=4)

        try:
            self.ent_excel_id.configure(state="readonly")
        except Exception:
            pass

    def _apply_permissions(self):
        # Предполагаем: создавать/редактировать здесь могут admin/manager
        # Если у тебя есть отдельная роль creator/operator — легко поменяем.
        if not _can_edit(self.current_role):
            for name in (
                "year",
                "customer_name",
                "address",
                "contract_number",
                "contract_date",
                "short_name",
                "executor_department",
                "contract_type",
            ):
                try:
                    getattr(self, f"ent_{name}").configure(state="disabled")
                except Exception:
                    pass
            try:
                self.cmb_program_name.configure(state="disabled")
            except Exception:
                pass
            self.btn_clear.configure(state="disabled")
            self.btn_save.configure(state="disabled")

            tk.Label(
                self,
                text="У вас нет прав на создание/редактирование объектов.",
                bg=C["bg"],
                fg=C["error"],
                anchor="w",
                padx=12,
            ).pack(fill="x", pady=(0, 8))

    def _fill_from_data_or_default(self):
        d = self.obj_data
        if d:
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
            self._on_clear()

    def _on_clear(self):
        is_new = not bool(self.obj_data.get("id")) if self.obj_data else True
        next_id = ObjectsService.get_next_excel_id() if is_new else ""

        for name in (
            "year",
            "program_name",
            "customer_name",
            "address",
            "contract_number",
            "contract_date",
            "short_name",
            "executor_department",
            "contract_type",
        ):
            getattr(self, f"var_{name}").set("")

        self.var_excel_id.set(next_id)
        self.var_year.set(str(_today_year()))
        try:
            self.ent_address.focus_set()
        except Exception:
            pass

    def _on_save(self):
        if not _can_edit(self.current_role):
            messagebox.showwarning(
                "Объект",
                "У вас нет прав на создание/редактирование объектов.",
                parent=self,
            )
            return

        addr = self.var_address.get().strip()
        if not addr:
            messagebox.showwarning("Объект", "Адрес объекта обязателен.", parent=self)
            return

        excel_id_raw = self.var_excel_id.get().strip()
        if excel_id_raw and not excel_id_raw.isdigit():
            if not messagebox.askyesno(
                "ID объекта",
                "ID (excel_id) не является числом.\n"
                "Продолжить сохранение с таким значением?",
                parent=self,
            ):
                return

        try:
            cd_val = _parse_ru_date(self.var_contract_date.get())
        except Exception:
            messagebox.showwarning(
                "Объект",
                "Дата договора должна быть в формате ДД.ММ.ГГГГ или оставьте поле пустым.",
                parent=self,
            )
            return

        obj_id = self.obj_data.get("id") if self.obj_data else None

        try:
            create_or_update_object(
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
                status=self.obj_data.get("status") or "Новый",
            )
        except Exception as e:
            logger.exception("Ошибка сохранения объекта")
            messagebox.showerror("Объект", f"Ошибка сохранения в БД:\n{e}", parent=self)
            return

        if not obj_id:
            messagebox.showinfo(
                "Объект",
                "Объект успешно сохранён в базе данных.",
                parent=self,
            )
            self.var_excel_id.set(ObjectsService.get_next_excel_id())
            self.var_address.set("")
            self.var_short_name.set("")
            self.var_contract_number.set("")
            self.var_contract_date.set("")
            self.ent_address.focus_set()

            current_programs = list(self.cmb_program_name["values"])
            new_program = self.var_program_name.get().strip()
            if new_program and new_program not in current_programs:
                self.cmb_program_name["values"] = get_unique_program_names()
        else:
            messagebox.showinfo("Объект", "Изменения сохранены.", parent=self)


# ═══════════════════════════════════════════════════════════════
#  REGISTRY PAGE
# ═══════════════════════════════════════════════════════════════
class ObjectsRegistryPage(tk.Frame):
    """
    Реестр объектов: просмотр, фильтрация, экспорт, статус, редактирование по правам.
    Создание вынесено в отдельный раздел.
    """

    def __init__(self, master, app_ref=None):
        super().__init__(master, bg=C["bg"])
        self.app_ref = app_ref
        self.current_role = (
            (getattr(self.app_ref, "current_user", {}) or {}).get("role")
            or "specialist"
        )

        self.tree = None
        self._objects: List[Dict[str, Any]] = []
        self._objects_by_id: Dict[int, Dict[str, Any]] = {}

        self.var_filter_addr = tk.StringVar()
        self.var_filter_excel = tk.StringVar()
        self.var_filter_program = tk.StringVar(value="Все")
        self.var_filter_status = tk.StringVar(value="Все")
        self.var_filter_year = tk.StringVar(value="Все")
        self.var_search = tk.StringVar()

        self.sort_col = "excel_id"
        self.sort_desc = False

        self._build_ui()
        self._load_filter_values()
        self._load_data()

    def _build_ui(self):
        hdr = tk.Frame(self, bg=C["accent"], pady=6)
        hdr.pack(fill="x")

        tk.Label(
            hdr,
            text="🏗  Реестр объектов",
            font=("Segoe UI", 12, "bold"),
            bg=C["accent"],
            fg="white",
            padx=12,
        ).pack(side="left")

        self.lbl_header_info = tk.Label(
            hdr,
            text="",
            font=("Segoe UI", 8),
            bg=C["accent"],
            fg="#bbdefb",
            padx=12,
        )
        self.lbl_header_info.pack(side="right")

        top = tk.LabelFrame(
            self,
            text=" Фильтры и действия ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            relief="groove",
            bd=1,
            padx=10,
            pady=8,
        )
        top.pack(fill="x", padx=10, pady=(8, 4))
        top.grid_columnconfigure(7, weight=1)

        tk.Label(top, text="Адрес:", bg=C["panel"]).grid(
            row=0, column=0, sticky="e", padx=(0, 6), pady=3
        )
        ent_addr = ttk.Entry(top, textvariable=self.var_filter_addr, width=28)
        ent_addr.grid(row=0, column=1, sticky="w", pady=3)

        tk.Label(top, text="ID объекта:", bg=C["panel"]).grid(
            row=0, column=2, sticky="e", padx=(12, 6), pady=3
        )
        ent_excel = ttk.Entry(top, textvariable=self.var_filter_excel, width=14)
        ent_excel.grid(row=0, column=3, sticky="w", pady=3)

        tk.Label(top, text="Год:", bg=C["panel"]).grid(
            row=0, column=4, sticky="e", padx=(12, 6), pady=3
        )
        self.cmb_year_filter = ttk.Combobox(
            top,
            textvariable=self.var_filter_year,
            width=10,
            state="readonly",
        )
        self.cmb_year_filter.grid(row=0, column=5, sticky="w", pady=3)

        tk.Label(top, text="Статус:", bg=C["panel"]).grid(
            row=0, column=6, sticky="e", padx=(12, 6), pady=3
        )
        self.cmb_status_filter = ttk.Combobox(
            top,
            textvariable=self.var_filter_status,
            values=["Все"] + OBJECT_STATUSES,
            width=14,
            state="readonly",
        )
        self.cmb_status_filter.grid(row=0, column=7, sticky="w", pady=3)

        tk.Label(top, text="Программа:", bg=C["panel"]).grid(
            row=1, column=0, sticky="e", padx=(0, 6), pady=3
        )
        self.cmb_program_filter = ttk.Combobox(
            top,
            textvariable=self.var_filter_program,
            width=40,
            state="readonly",
        )
        self.cmb_program_filter.grid(row=1, column=1, columnspan=3, sticky="w", pady=3)

        tk.Label(top, text="Общий поиск:", bg=C["panel"]).grid(
            row=1, column=4, sticky="e", padx=(12, 6), pady=3
        )
        ent_search = ttk.Entry(top, textvariable=self.var_search, width=32)
        ent_search.grid(row=1, column=5, columnspan=2, sticky="w", pady=3)

        btns = tk.Frame(top, bg=C["panel"])
        btns.grid(row=0, column=8, rowspan=2, sticky="e", padx=(12, 0))

        ttk.Button(btns, text="Применить", command=self._load_data).pack(
            side="left", padx=2
        )
        ttk.Button(btns, text="Сброс", command=self._reset_filters).pack(
            side="left", padx=2
        )
        ttk.Button(btns, text="Обновить", command=self._refresh_all).pack(
            side="left", padx=2
        )
        ttk.Button(btns, text="Выгрузить в Excel", command=self._export_to_excel).pack(
            side="left", padx=2
        )

        bar = tk.Frame(self, bg=C["accent_light"], pady=5)
        bar.pack(fill="x", padx=10)

        self.btn_edit = ttk.Button(
            bar,
            text="✏️ Редактировать выбранный объект",
            command=self._edit_selected,
        )
        self.btn_edit.pack(side="left", padx=2)

        tk.Frame(bar, bg=C["border"], width=1).pack(side="left", fill="y", padx=8)

        tk.Label(
            bar,
            text="Статус выбранного объекта:",
            bg=C["accent_light"],
        ).pack(side="left", padx=(0, 6))

        self.var_status = tk.StringVar(value="Новый")
        self.cmb_status = ttk.Combobox(
            bar,
            textvariable=self.var_status,
            values=OBJECT_STATUSES,
            width=12,
            state="readonly",
        )
        self.cmb_status.pack(side="left", padx=(0, 6))

        self.btn_set_status = ttk.Button(
            bar,
            text="Установить статус",
            command=self._on_change_status,
        )
        self.btn_set_status.pack(side="left", padx=(4, 0))

        tk.Frame(bar, bg=C["border"], width=1).pack(side="left", fill="y", padx=8)

        self.lbl_perm = tk.Label(
            bar,
            text="Создание объектов выполняется в отдельном разделе.",
            bg=C["accent_light"],
            fg=C["text2"],
        )
        self.lbl_perm.pack(side="left", padx=(4, 0))

        if not _can_edit(self.current_role):
            self.btn_edit.configure(state="disabled")
            self.cmb_status.configure(state="disabled")
            self.btn_set_status.configure(state="disabled")
            self.lbl_perm.config(
                text="Режим просмотра. Создание/редактирование доступно по правам."
            )

        body = tk.PanedWindow(
            self,
            orient="horizontal",
            sashrelief="raised",
            bg=C["bg"],
        )
        body.pack(fill="both", expand=True, padx=10, pady=(4, 4))

        left = tk.Frame(body, bg=C["panel"])
        right = tk.Frame(body, bg=C["panel"])
        body.add(left, minsize=900)
        body.add(right, minsize=300)

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
        self.tree = ttk.Treeview(
            left,
            columns=cols,
            show="headings",
            selectmode="browse",
        )

        self.tree.tag_configure("status_new", background="#e8f5e9")
        self.tree.tag_configure("status_inwork", background="#fff8e1")
        self.tree.tag_configure("status_closed", background="#ffebee")

        headings = {
            "excel_id": ("ID объекта", 100, "w"),
            "address": ("Адрес", 310, "w"),
            "year": ("Год", 70, "center"),
            "program_name": ("Программа", 180, "w"),
            "customer_name": ("Заказчик", 170, "w"),
            "short_name": ("Краткое имя", 160, "w"),
            "executor_department": ("Подразделение исполнителя", 180, "w"),
            "contract_number": ("№ договора", 110, "w"),
            "contract_date": ("Дата договора", 100, "center"),
            "contract_type": ("Тип договора", 120, "w"),
            "status": ("Статус", 90, "center"),
        }

        for col, (text, width, anchor) in headings.items():
            self.tree.heading(col, text=text, command=lambda c=col: self._sort_by(c))
            self.tree.column(col, width=width, anchor=anchor)

        vsb = ttk.Scrollbar(left, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(left, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        left.grid_rowconfigure(0, weight=1)
        left.grid_columnconfigure(0, weight=1)

        self.tree.bind("<Double-1>", self._on_row_double_click)
        self.tree.bind("<Return>", lambda _e: self._edit_selected())
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._show_details())

        details = tk.LabelFrame(
            right,
            text=" Карточка объекта ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            relief="groove",
            bd=1,
            padx=10,
            pady=8,
        )
        details.pack(fill="both", expand=True)

        self.detail_vars = {
            "id": tk.StringVar(),
            "excel_id": tk.StringVar(),
            "address": tk.StringVar(),
            "year": tk.StringVar(),
            "program_name": tk.StringVar(),
            "customer_name": tk.StringVar(),
            "short_name": tk.StringVar(),
            "executor_department": tk.StringVar(),
            "contract_number": tk.StringVar(),
            "contract_date": tk.StringVar(),
            "contract_type": tk.StringVar(),
            "status": tk.StringVar(),
        }

        detail_fields = [
            ("ID БД", "id"),
            ("ID объекта", "excel_id"),
            ("Адрес", "address"),
            ("Год", "year"),
            ("Программа", "program_name"),
            ("Заказчик", "customer_name"),
            ("Краткое имя", "short_name"),
            ("Подразделение", "executor_department"),
            ("№ договора", "contract_number"),
            ("Дата договора", "contract_date"),
            ("Тип договора", "contract_type"),
            ("Статус", "status"),
        ]

        for i, (label, key) in enumerate(detail_fields):
            tk.Label(
                details,
                text=f"{label}:",
                bg=C["panel"],
                fg=C["text2"],
                anchor="ne",
                width=14,
            ).grid(row=i, column=0, sticky="ne", padx=(0, 8), pady=3)

            tk.Label(
                details,
                textvariable=self.detail_vars[key],
                bg=C["panel"],
                fg=C["text"],
                anchor="nw",
                justify="left",
                wraplength=270,
            ).grid(row=i, column=1, sticky="nw", pady=3)

        details.grid_columnconfigure(1, weight=1)

        self.lbl_bottom = tk.Label(
            self,
            text="Готово.",
            bg=C["bg"],
            fg=C["text3"],
            font=("Segoe UI", 8),
            anchor="w",
            padx=14,
            pady=4,
        )
        self.lbl_bottom.pack(fill="x")

        ent_addr.bind("<Return>", lambda _e: self._load_data())
        ent_excel.bind("<Return>", lambda _e: self._load_data())
        ent_search.bind("<Return>", lambda _e: self._load_data())

    def _load_filter_values(self):
        try:
            programs = ObjectsService.get_unique_program_names()
        except Exception:
            logger.exception("Ошибка загрузки списка программ")
            programs = []

        try:
            years = ObjectsService.get_unique_years()
        except Exception:
            logger.exception("Ошибка загрузки списка годов")
            years = []

        self.cmb_program_filter["values"] = ["Все"] + programs
        self.cmb_year_filter["values"] = ["Все"] + years

        if not self.var_filter_program.get():
            self.var_filter_program.set("Все")
        if not self.var_filter_year.get():
            self.var_filter_year.set("Все")

    def _refresh_all(self):
        self._load_filter_values()
        self._load_data()

    def _reset_filters(self):
        self.var_filter_addr.set("")
        self.var_filter_excel.set("")
        self.var_filter_program.set("Все")
        self.var_filter_status.set("Все")
        self.var_filter_year.set("Все")
        self.var_search.set("")
        self._load_data()

    def _sort_by(self, col: str):
        if self.sort_col == col:
            self.sort_desc = not self.sort_desc
        else:
            self.sort_col = col
            self.sort_desc = False
        self._render_tree()

    def _sort_key(self, obj: Dict[str, Any]):
        val = obj.get(self.sort_col)

        if self.sort_col == "excel_id":
            s = _clean_text(val)
            if s.isdigit():
                return (0, int(s))
            return (1, s.lower())

        if self.sort_col == "year":
            return _safe_int(val) or 0

        if self.sort_col == "contract_date":
            if isinstance(val, (datetime, date)):
                return val
            return _clean_text(val)

        return _clean_text(val).lower()

    def _match_search(self, obj: Dict[str, Any], q: str) -> bool:
        if not q:
            return True
        haystack = " | ".join([
            _clean_text(obj.get("excel_id")),
            _clean_text(obj.get("address")),
            _clean_text(obj.get("year")),
            _clean_text(obj.get("program_name")),
            _clean_text(obj.get("customer_name")),
            _clean_text(obj.get("short_name")),
            _clean_text(obj.get("executor_department")),
            _clean_text(obj.get("contract_number")),
            _fmt_date(obj.get("contract_date")),
            _clean_text(obj.get("contract_type")),
            _clean_text(obj.get("status")),
        ]).lower()
        return q in haystack

    def _load_data(self):
        try:
            all_objects = load_objects_full_from_db()
        except Exception as e:
            logger.exception("Ошибка загрузки реестра объектов")
            messagebox.showerror(
                "Реестр объектов",
                f"Ошибка загрузки объектов из БД:\n{e}",
                parent=self,
            )
            return

        addr_filter = _clean_text(self.var_filter_addr.get()).lower()
        excel_filter = _clean_text(self.var_filter_excel.get()).lower()
        program_filter = _clean_text(self.var_filter_program.get())
        status_filter = _clean_text(self.var_filter_status.get())
        year_filter = _clean_text(self.var_filter_year.get())
        search_q = _clean_text(self.var_search.get()).lower()

        filtered: List[Dict[str, Any]] = []
        by_id: Dict[int, Dict[str, Any]] = {}

        for o in all_objects:
            addr = _clean_text(o.get("address"))
            excel_id = _clean_text(o.get("excel_id"))
            program_name = _clean_text(o.get("program_name"))
            status = _clean_text(o.get("status")) or "Новый"
            year = _clean_text(o.get("year"))

            if addr_filter and addr_filter not in addr.lower():
                continue
            if excel_filter and excel_filter not in excel_id.lower():
                continue
            if program_filter != "Все" and program_name != program_filter:
                continue
            if status_filter != "Все" and status != status_filter:
                continue
            if year_filter != "Все" and year != year_filter:
                continue
            if not self._match_search(o, search_q):
                continue

            filtered.append(o)
            try:
                by_id[int(o["id"])] = o
            except Exception:
                pass

        self._objects = filtered
        self._objects_by_id = by_id
        self._render_tree()
        self._update_summary()

    def _render_tree(self):
        self.tree.delete(*self.tree.get_children())

        rows = sorted(
            self._objects,
            key=self._sort_key,
            reverse=self.sort_desc,
        )

        for o in rows:
            status = _clean_text(o.get("status")) or "Новый"
            tags = (STATUS_TAGS.get(status),) if STATUS_TAGS.get(status) else ()
            iid = str(o.get("id"))

            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    _clean_text(o.get("excel_id")),
                    _clean_text(o.get("address")),
                    _clean_text(o.get("year")),
                    _clean_text(o.get("program_name")),
                    _clean_text(o.get("customer_name")),
                    _clean_text(o.get("short_name")),
                    _clean_text(o.get("executor_department")),
                    _clean_text(o.get("contract_number")),
                    _fmt_date(o.get("contract_date")),
                    _clean_text(o.get("contract_type")),
                    status,
                ),
                tags=tags,
            )

        self._clear_details()

    def _update_summary(self):
        total = len(self._objects)
        new_cnt = sum(1 for o in self._objects if (_clean_text(o.get("status")) or "Новый") == "Новый")
        inwork_cnt = sum(1 for o in self._objects if _clean_text(o.get("status")) == "В работе")
        closed_cnt = sum(1 for o in self._objects if _clean_text(o.get("status")) == "Закрыт")

        self.lbl_header_info.config(
            text=f"Всего: {total} | Новый: {new_cnt} | В работе: {inwork_cnt} | Закрыт: {closed_cnt}"
        )
        self.lbl_bottom.config(text=f"Загружено объектов: {total}")

    def _get_selected_object(self) -> Optional[Dict[str, Any]]:
        selected = self.tree.selection()
        if not selected:
            return None
        iid = selected[0]
        try:
            return self._objects_by_id.get(int(iid))
        except Exception:
            return None

    def _clear_details(self):
        for v in self.detail_vars.values():
            v.set("")

    def _show_details(self):
        obj = self._get_selected_object()
        if not obj:
            self._clear_details()
            return

        self.detail_vars["id"].set(str(obj.get("id") or ""))
        self.detail_vars["excel_id"].set(_clean_text(obj.get("excel_id")))
        self.detail_vars["address"].set(_clean_text(obj.get("address")))
        self.detail_vars["year"].set(_clean_text(obj.get("year")))
        self.detail_vars["program_name"].set(_clean_text(obj.get("program_name")))
        self.detail_vars["customer_name"].set(_clean_text(obj.get("customer_name")))
        self.detail_vars["short_name"].set(_clean_text(obj.get("short_name")))
        self.detail_vars["executor_department"].set(_clean_text(obj.get("executor_department")))
        self.detail_vars["contract_number"].set(_clean_text(obj.get("contract_number")))
        self.detail_vars["contract_date"].set(_fmt_date(obj.get("contract_date")))
        self.detail_vars["contract_type"].set(_clean_text(obj.get("contract_type")))
        self.detail_vars["status"].set(_clean_text(obj.get("status")) or "Новый")

        current_status = _clean_text(obj.get("status")) or "Новый"
        if current_status in OBJECT_STATUSES:
            self.var_status.set(current_status)

    def _edit_selected(self):
        if not _can_edit(self.current_role):
            messagebox.showinfo(
                "Редактирование объекта",
                "Редактирование доступно только руководителю и администратору.",
                parent=self,
            )
            return

        obj = self._get_selected_object()
        if not obj:
            messagebox.showinfo(
                "Редактирование объекта",
                "Выберите объект в списке.",
                parent=self,
            )
            return

        dlg = ObjectEditDialog(self, obj)
        self.wait_window(dlg)

        if not dlg.result:
            return

        try:
            updated = dlg.result
            create_or_update_object(
                obj_id=obj.get("id"),
                excel_id=obj.get("excel_id"),
                year=updated["year"],
                program_name=updated["program_name"],
                customer_name=updated["customer_name"],
                address=updated["address"] or "",
                contract_number=updated["contract_number"],
                contract_date=updated["contract_date"],
                short_name=updated["short_name"],
                executor_department=updated["executor_department"],
                contract_type=updated["contract_type"],
                status=obj.get("status"),
            )
        except Exception as e:
            logger.exception("Ошибка обновления объекта")
            messagebox.showerror(
                "Редактирование объекта",
                f"Ошибка сохранения в БД:\n{e}",
                parent=self,
            )
            return

        self._refresh_all()
        try:
            self.tree.selection_set(str(obj.get("id")))
            self.tree.focus(str(obj.get("id")))
            self.tree.see(str(obj.get("id")))
            self._show_details()
        except Exception:
            pass

    def _on_row_double_click(self, event=None):
        self._edit_selected()

    def _export_to_excel(self):
        if not self._objects:
            messagebox.showinfo(
                "Выгрузка в Excel",
                "Нет данных для выгрузки.",
                parent=self,
            )
            return

        default_name = f"objects_registry_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        file_path = filedialog.asksaveasfilename(
            title="Сохранить как",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel файлы", "*.xlsx"), ("Все файлы", "*.*")],
        )
        if not file_path:
            return

        rows = []
        for o in self._objects:
            rows.append({
                "ID в БД": o.get("id"),
                "ID объекта (excel_id)": _clean_text(o.get("excel_id")),
                "Адрес": _clean_text(o.get("address")),
                "Год": _clean_text(o.get("year")),
                "Программа": _clean_text(o.get("program_name")),
                "Заказчик": _clean_text(o.get("customer_name")),
                "Краткое имя": _clean_text(o.get("short_name")),
                "Подразделение исполнителя": _clean_text(o.get("executor_department")),
                "№ договора": _clean_text(o.get("contract_number")),
                "Дата договора": _fmt_date(o.get("contract_date")),
                "Тип договора": _clean_text(o.get("contract_type")),
                "Статус": _clean_text(o.get("status")) or "Новый",
            })

        try:
            df = pd.DataFrame(rows)
            dir_name = os.path.dirname(file_path)
            if dir_name:
                os.makedirs(dir_name, exist_ok=True)
            df.to_excel(file_path, index=False)
        except Exception as e:
            logger.exception("Ошибка выгрузки в Excel")
            messagebox.showerror(
                "Выгрузка в Excel",
                f"Ошибка при сохранении файла:\n{e}",
                parent=self,
            )
            return

        messagebox.showinfo(
            "Выгрузка в Excel",
            f"Файл успешно сохранён:\n{file_path}",
            parent=self,
        )

    def _on_change_status(self):
        if not _can_edit(self.current_role):
            messagebox.showwarning(
                "Статус",
                "Изменение статуса доступно только руководителю и администратору.",
                parent=self,
            )
            return

        obj = self._get_selected_object()
        if not obj:
            messagebox.showwarning("Статус", "Выберите объект в списке.", parent=self)
            return

        obj_db_id = obj.get("id")
        if not obj_db_id:
            messagebox.showerror("Статус", "У объекта нет ID в базе.", parent=self)
            return

        new_status = self.var_status.get()
        if new_status not in OBJECT_STATUSES:
            messagebox.showerror("Статус", "Недопустимое значение статуса.", parent=self)
            return

        try:
            ObjectsService.update_status(int(obj_db_id), new_status)
        except Exception as e:
            logger.exception("Ошибка смены статуса объекта")
            messagebox.showerror(
                "Статус",
                f"Ошибка обновления статуса в БД:\n{e}",
                parent=self,
            )
            return

        obj["status"] = new_status
        self._load_data()

        try:
            self.tree.selection_set(str(obj_db_id))
            self.tree.focus(str(obj_db_id))
            self.tree.see(str(obj_db_id))
            self._show_details()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
#  FACTORY API
# ═══════════════════════════════════════════════════════════════
def create_objects_registry_page(parent, app_ref=None) -> ObjectsRegistryPage:
    return ObjectsRegistryPage(parent, app_ref=app_ref)


def create_object_create_page(parent, app_ref=None, obj_data=None) -> ObjectCreatePage:
    return ObjectCreatePage(parent, app_ref=app_ref, obj_data=obj_data)
