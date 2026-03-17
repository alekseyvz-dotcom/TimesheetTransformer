"""
task_dialog.py
Диалог создания/редактирования задачи ГПР
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import tkinter as tk
from tkinter import ttk, messagebox

from gpr_common import (
    C,
    STATUS_LIST,
    STATUS_LABELS,
    STATUS_COLORS,
    today,
    parse_date,
    fmt_date,
    fmt_qty,
    safe_float,
)

from employee_service import EmployeeService


# роли работников
TASK_ROLES = {
    "executor": "Исполнитель",
    "foreman": "Бригадир",
    "inspector": "Контролёр",
}

TASK_ROLE_LIST = list(TASK_ROLES.keys())
TASK_ROLE_LABELS = list(TASK_ROLES.values())


class TaskEditDialog(tk.Toplevel):

    def __init__(
        self,
        parent,
        work_types: List[Dict],
        uoms: List[Dict],
        init: Optional[Dict[str, Any]] = None,
        user_id: Optional[int] = None,
    ):

        super().__init__(parent)

        self.transient(parent)
        self.grab_set()

        self.work_types = work_types
        self.uoms = uoms
        self.init = init or {}
        self.user_id = user_id

        self.result: Optional[Dict[str, Any]] = None

        self._assignments: List[Dict] = []
        self._emp_results: List[Dict] = []

        name = self.init.get("name", "")

        if name:
            self.title(f"✏ Работа: {name[:60]}")
        else:
            self.title("➕ Новая работа")

        self.minsize(680, 540)

        self._build_ui()
        self._fill_init()
        self._load_assignments()

        self._center()

    # ----------------------------------------------------
    # UI
    # ----------------------------------------------------
    def _build_ui(self):

        hdr = tk.Frame(self, bg=C["accent"], pady=6)
        hdr.pack(fill="x")

        tk.Label(
            hdr,
            text="📋 Карточка работы",
            bg=C["accent"],
            fg="white",
            font=("Segoe UI", 11, "bold"),
            padx=10,
        ).pack(side="left")

        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=10, pady=(8, 4))

        tab_main = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_main, text="Основные")

        tab_assign = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_assign, text="Работники")

        self._build_main_tab(tab_main)
        self._build_assign_tab(tab_assign)

        # кнопки
        bot = tk.Frame(self, bg=C["bg"], pady=8)
        bot.pack(fill="x")

        ttk.Button(bot, text="Сохранить", command=self._on_ok).pack(
            side="right", padx=8
        )

        ttk.Button(bot, text="Отмена", command=self._on_cancel).pack(
            side="right"
        )

    # ----------------------------------------------------
    # MAIN TAB
    # ----------------------------------------------------
    def _build_main_tab(self, parent):

        frame = tk.Frame(parent, bg=C["panel"])
        frame.pack(fill="x", padx=12, pady=10)

        r = 0

        tk.Label(frame, text="Тип работ:", bg=C["panel"]).grid(
            row=r, column=0, sticky="e"
        )

        self.cmb_wt = ttk.Combobox(
            frame,
            state="readonly",
            values=[w["name"] for w in self.work_types],
            width=40,
        )

        self.cmb_wt.grid(row=r, column=1, sticky="w")

        r += 1

        tk.Label(frame, text="Вид работ:", bg=C["panel"]).grid(
            row=r, column=0, sticky="e"
        )

        self.ent_name = ttk.Entry(frame, width=42)
        self.ent_name.grid(row=r, column=1, sticky="w")

        r += 1

        tk.Label(frame, text="Ед. изм:", bg=C["panel"]).grid(
            row=r, column=0, sticky="e"
        )

        self.cmb_uom = ttk.Combobox(
            frame,
            state="readonly",
            values=["—"] + [u["code"] for u in self.uoms],
            width=20,
        )

        self.cmb_uom.grid(row=r, column=1, sticky="w")

        r += 1

        tk.Label(frame, text="Объем:", bg=C["panel"]).grid(
            row=r, column=0, sticky="e"
        )

        self.ent_qty = ttk.Entry(frame, width=12)
        self.ent_qty.grid(row=r, column=1, sticky="w")

        r += 1

        tk.Label(frame, text="Начало:", bg=C["panel"]).grid(
            row=r, column=0, sticky="e"
        )

        self.ent_start = ttk.Entry(frame, width=12)
        self.ent_start.grid(row=r, column=1, sticky="w")

        r += 1

        tk.Label(frame, text="Окончание:", bg=C["panel"]).grid(
            row=r, column=0, sticky="e"
        )

        self.ent_finish = ttk.Entry(frame, width=12)
        self.ent_finish.grid(row=r, column=1, sticky="w")

        r += 1

        tk.Label(frame, text="Статус:", bg=C["panel"]).grid(
            row=r, column=0, sticky="e"
        )

        self.cmb_status = ttk.Combobox(
            frame,
            state="readonly",
            values=[STATUS_LABELS[s] for s in STATUS_LIST],
            width=20,
        )

        self.cmb_status.grid(row=r, column=1, sticky="w")

        r += 1

        self.var_milestone = tk.BooleanVar()

        ttk.Checkbutton(
            frame,
            text="Веха",
            variable=self.var_milestone,
        ).grid(row=r, column=1, sticky="w")

    # ----------------------------------------------------
    # ASSIGN TAB
    # ----------------------------------------------------
    def _build_assign_tab(self, parent):

        search = tk.Frame(parent, bg=C["panel"])
        search.pack(fill="x", padx=10, pady=6)

        self.var_search = tk.StringVar()

        ttk.Entry(search, textvariable=self.var_search, width=30).pack(
            side="left"
        )

        ttk.Button(
            search,
            text="Найти",
            command=self._search_employees,
        ).pack(side="left", padx=4)

        # результаты
        cols = ("fio", "tbn", "position", "department")

        self.emp_tree = ttk.Treeview(
            parent,
            columns=cols,
            show="headings",
            height=5,
        )

        for c in cols:
            self.emp_tree.heading(c, text=c)

        self.emp_tree.pack(fill="x", padx=10)

        ttk.Button(
            parent,
            text="Назначить",
            command=self._assign_selected,
        ).pack(pady=4)

        # назначенные
        self.assign_tree = ttk.Treeview(
            parent,
            columns=("fio", "role"),
            show="headings",
            height=6,
        )

        self.assign_tree.heading("fio", text="ФИО")
        self.assign_tree.heading("role", text="Роль")

        self.assign_tree.pack(fill="both", expand=True, padx=10, pady=6)

    # ----------------------------------------------------
    # INIT
    # ----------------------------------------------------
    def _fill_init(self):

        if self.work_types:
            self.cmb_wt.current(0)

        if self.uoms:
            self.cmb_uom.current(0)

        self.cmb_status.current(0)

        d0 = self.init.get("plan_start") or today()
        d1 = self.init.get("plan_finish") or today()

        if isinstance(d0, str):
            d0 = datetime.fromisoformat(d0).date()

        if isinstance(d1, str):
            d1 = datetime.fromisoformat(d1).date()

        self.ent_start.insert(0, fmt_date(d0))
        self.ent_finish.insert(0, fmt_date(d1))

    def _load_assignments(self):

        tid = self.init.get("id")

        if not tid:
            return

        try:

            self._assignments = EmployeeService.load_task_assignments(tid)

        except Exception:

            logging.exception("load assignments error")

    # ----------------------------------------------------
    # EMPLOYEES
    # ----------------------------------------------------
    def _search_employees(self):

        q = self.var_search.get().strip()

        self.emp_tree.delete(*self.emp_tree.get_children())

        try:

            self._emp_results = EmployeeService.search_employees(q)

        except Exception as e:

            messagebox.showerror("Ошибка", str(e), parent=self)
            return

        for emp in self._emp_results:

            self.emp_tree.insert(
                "",
                "end",
                values=(
                    emp["fio"],
                    emp.get("tbn"),
                    emp.get("position"),
                    emp.get("department"),
                ),
            )

    def _assign_selected(self):

        sel = self.emp_tree.selection()

        if not sel:
            return

        idx = self.emp_tree.index(sel[0])

        emp = self._emp_results[idx]

        self._assignments.append(
            {
                "employee_id": emp["id"],
                "fio": emp["fio"],
                "role_in_task": "executor",
            }
        )

        self.assign_tree.insert(
            "",
            "end",
            values=(emp["fio"], "Исполнитель"),
        )

    # ----------------------------------------------------
    # OK / CANCEL
    # ----------------------------------------------------
    def _on_ok(self):

        try:

            wi = self.cmb_wt.current()

            if wi < 0:
                raise ValueError("Выберите тип работ")

            wt_id = self.work_types[wi]["id"]

            name = self.ent_name.get().strip()

            if not name:
                raise ValueError("Введите название работы")

            qty = safe_float(self.ent_qty.get())

            ds = parse_date(self.ent_start.get())
            df = parse_date(self.ent_finish.get())

            if df < ds:
                raise ValueError("Окончание раньше начала")

            si = self.cmb_status.current()
            status = STATUS_LIST[si]

            self.result = {
                "work_type_id": wt_id,
                "name": name,
                "uom_code": None,
                "plan_qty": qty,
                "plan_start": ds,
                "plan_finish": df,
                "status": status,
                "is_milestone": self.var_milestone.get(),
                "_assignments": list(self._assignments),
            }

            self.destroy()

        except Exception as e:

            messagebox.showwarning("Ошибка", str(e), parent=self)

    def _on_cancel(self):

        self.result = None
        self.destroy()

    # ----------------------------------------------------
    # WINDOW POSITION
    # ----------------------------------------------------
    def _center(self):

        self.update_idletasks()

        w = self.winfo_width()
        h = self.winfo_height()

        pw = self.master.winfo_width()
        ph = self.master.winfo_height()

        px = self.master.winfo_rootx()
        py = self.master.winfo_rooty()

        x = px + (pw - w) // 2
        y = py + (ph - h) // 2

        self.geometry(f"+{x}+{y}")


# ----------------------------------------------------
# API
# ----------------------------------------------------
def open_task_dialog(parent, work_types, uoms, init=None, user_id=None):

    dlg = TaskEditDialog(
        parent,
        work_types,
        uoms,
        init=init,
        user_id=user_id,
    )

    parent.wait_window(dlg)

    return dlg.result
