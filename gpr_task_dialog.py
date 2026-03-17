from __future__ import annotations

import logging
import tkinter as tk
from datetime import date, datetime
from tkinter import messagebox, simpledialog, ttk
from typing import Any, Dict, List, Mapping, Optional, Sequence

from gpr_common import (
    C,
    STATUS_LABELS,
    STATUS_LIST,
    TASK_ROLE_LABELS,
    TASK_ROLE_LIST,
    GprAssignment,
    GprTask,
    fmt_date,
    fmt_qty,
    normalize_spaces,
    normalize_task_role,
    parse_date,
    role_label,
    safe_float,
    status_fill_color,
    task_to_dialog_init,
    today,
)
from gpr_db import GprEmployeeService
from gpr_dialogs import SelectEmployeesDialog, center_toplevel, setup_modal_window

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  COMPATIBILITY EXPORT
# ═══════════════════════════════════════════════════════════════
# Для совместимости со старым кодом:
# from gpr_task_dialog import open_task_dialog, _EmployeeService
_EmployeeService = GprEmployeeService


# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════
def _work_type_id(item: Any) -> Optional[int]:
    if isinstance(item, Mapping):
        value = item.get("id")
    else:
        value = getattr(item, "id", None)

    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def _work_type_name(item: Any) -> str:
    if isinstance(item, Mapping):
        value = item.get("name")
    else:
        value = getattr(item, "name", "")
    return normalize_spaces(value or "")


def _uom_code(item: Any) -> str:
    if isinstance(item, Mapping):
        value = item.get("code")
    else:
        value = getattr(item, "code", "")
    return normalize_spaces(value or "")


def _uom_name(item: Any) -> str:
    if isinstance(item, Mapping):
        value = item.get("name")
    else:
        value = getattr(item, "name", "")
    return normalize_spaces(value or "")


def _assignment_from_any(item: Any) -> GprAssignment:
    if isinstance(item, GprAssignment):
        return item.normalized_copy()

    if isinstance(item, Mapping):
        return GprAssignment(
            assignment_id=item.get("assignment_id"),
            employee_id=item.get("employee_id"),
            fio=item.get("fio") or "",
            tbn=item.get("tbn") or "",
            position=item.get("position") or "",
            department=item.get("department") or "",
            role_in_task=item.get("role_in_task") or "executor",
            note=item.get("note"),
        ).normalized_copy()

    return GprAssignment(employee_id=None, fio="").normalized_copy()


def _deduplicate_assignments(assignments: Sequence[GprAssignment]) -> List[GprAssignment]:
    seen: set[tuple[Any, ...]] = set()
    out: List[GprAssignment] = []

    for item in assignments:
        a = item.normalized_copy()
        key = (
            int(a.employee_id) if a.employee_id is not None else None,
            normalize_spaces(a.fio).lower(),
            normalize_spaces(a.tbn),
            normalize_task_role(a.role_in_task),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(a)

    return out


# ═══════════════════════════════════════════════════════════════
#  ROLE SELECT DIALOG
# ═══════════════════════════════════════════════════════════════
class RoleSelectDialog(simpledialog.Dialog):
    def __init__(self, parent, current_role: str):
        self.current_role = normalize_task_role(current_role)
        self.result: Optional[str] = None
        super().__init__(parent, title="Выбор роли")

    def body(self, master):
        tk.Label(master, text="Роль на задаче:").grid(row=0, column=0, sticky="w", pady=(2, 6))
        self.cmb = ttk.Combobox(master, state="readonly", width=22, values=TASK_ROLE_LABELS)
        self.cmb.grid(row=1, column=0, sticky="w")

        try:
            idx = TASK_ROLE_LIST.index(self.current_role)
            self.cmb.current(idx)
        except Exception:
            self.cmb.current(0)

        return self.cmb

    def validate(self):
        idx = self.cmb.current()
        if idx < 0:
            messagebox.showwarning("Роль", "Выберите роль.", parent=self)
            return False
        self._role = TASK_ROLE_LIST[idx]
        return True

    def apply(self):
        self.result = self._role


# ═══════════════════════════════════════════════════════════════
#  TASK EDIT DIALOG
# ═══════════════════════════════════════════════════════════════
class TaskEditDialogPro(tk.Toplevel):
    """
    Диалог карточки работы ГПР.

    Важно:
    - диалог НЕ сохраняет назначения сам в БД;
    - он только возвращает все данные наверх;
    - общее сохранение делает основной модуль через gpr_db.save_plan_tasks_diff().
    """

    def __init__(
        self,
        parent,
        work_types: Sequence[Any],
        uoms: Sequence[Any],
        init: Optional[Dict[str, Any] | GprTask] = None,
        user_id: Optional[int] = None,
    ):
        super().__init__(parent)

        self.parent = parent
        self.work_types = list(work_types or [])
        self.uoms = list(uoms or [])
        self.user_id = user_id
        self.init = task_to_dialog_init(init) if init is not None else {}
        self.result: Optional[Dict[str, Any]] = None

        self._assignments: List[GprAssignment] = []

        task_name = normalize_spaces(self.init.get("name") or "")
        self.title(f"✏️ Работа: {task_name[:60]}" if task_name else "➕ Новая работа ГПР")
        self.minsize(760, 600)
        self.resizable(True, True)

        setup_modal_window(self, parent)

        self._build_ui()
        self._fill_init()
        self._load_assignments()
        self._update_duration()
        self._update_status_color()
        self._update_info()

        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.bind("<Escape>", lambda _e: self._on_cancel())

        center_toplevel(self, parent)

    # --------------------------------------------------------
    # BUILD UI
    # --------------------------------------------------------

    def _build_ui(self):
        hdr = tk.Frame(self, bg=C["accent"], pady=6)
        hdr.pack(fill="x")
        tk.Label(
            hdr,
            text="📋 Карточка работы ГПР",
            font=("Segoe UI", 11, "bold"),
            bg=C["accent"],
            fg="white",
            padx=10,
        ).pack(side="left")

        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=10, pady=(8, 4))

        tab_main = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_main, text="  📝 Основные данные  ")
        self._build_main_tab(tab_main)

        tab_assign = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_assign, text="  👷 Назначения работников  ")
        self._build_assign_tab(tab_assign)

        bot = tk.Frame(self, bg=C["bg"], pady=8)
        bot.pack(fill="x")

        btn_ok = tk.Button(
            bot,
            text="✅ Сохранить",
            font=("Segoe UI", 10, "bold"),
            bg=C["btn_bg"],
            fg=C["btn_fg"],
            activebackground="#0d47a1",
            activeforeground="white",
            relief="flat",
            cursor="hand2",
            padx=20,
            pady=6,
            command=self._on_ok,
        )
        btn_ok.pack(side="right", padx=(0, 16))
        btn_ok.bind("<Enter>", lambda _e: btn_ok.config(bg="#0d47a1"))
        btn_ok.bind("<Leave>", lambda _e: btn_ok.config(bg=C["btn_bg"]))

        btn_cancel = tk.Button(
            bot,
            text="Отмена",
            font=("Segoe UI", 9),
            bg="#e0e0e0",
            fg="#333",
            relief="flat",
            cursor="hand2",
            padx=16,
            pady=6,
            command=self._on_cancel,
        )
        btn_cancel.pack(side="right", padx=(0, 8))

        self.lbl_info = tk.Label(
            bot,
            text="",
            font=("Segoe UI", 8),
            fg=C["text3"],
            bg=C["bg"],
        )
        self.lbl_info.pack(side="left", padx=16)

    def _build_main_tab(self, parent):
        grp1 = tk.LabelFrame(
            parent,
            text=" 🔧 Работа ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=12,
            pady=8,
        )
        grp1.pack(fill="x", padx=12, pady=(10, 4))
        grp1.grid_columnconfigure(1, weight=1)

        r = 0

        tk.Label(grp1, text="Тип работ *:", bg=C["panel"], font=("Segoe UI", 9)).grid(
            row=r, column=0, sticky="e", padx=(0, 8), pady=4
        )
        wt_vals = [_work_type_name(w) for w in self.work_types]
        self.cmb_wt = ttk.Combobox(grp1, state="readonly", width=44, values=wt_vals, font=("Segoe UI", 9))
        self.cmb_wt.grid(row=r, column=1, sticky="w", pady=4)
        r += 1

        tk.Label(grp1, text="Вид работ *:", bg=C["panel"], font=("Segoe UI", 9)).grid(
            row=r, column=0, sticky="e", padx=(0, 8), pady=4
        )
        self.ent_name = ttk.Entry(grp1, width=48, font=("Segoe UI", 9))
        self.ent_name.grid(row=r, column=1, sticky="ew", pady=4)
        r += 1

        tk.Label(grp1, text="Ед. изм.:", bg=C["panel"], font=("Segoe UI", 9)).grid(
            row=r, column=0, sticky="e", padx=(0, 8), pady=4
        )
        uom_frame = tk.Frame(grp1, bg=C["panel"])
        uom_frame.grid(row=r, column=1, sticky="w", pady=4)

        uom_vals = ["—"] + [
            f"{_uom_code(u)} — {_uom_name(u)}" if _uom_name(u) else _uom_code(u)
            for u in self.uoms
        ]
        self.cmb_uom = ttk.Combobox(uom_frame, state="readonly", width=22, values=uom_vals, font=("Segoe UI", 9))
        self.cmb_uom.pack(side="left")

        tk.Label(uom_frame, text="   Объём план:", bg=C["panel"], font=("Segoe UI", 9)).pack(
            side="left", padx=(16, 4)
        )
        self.ent_qty = ttk.Entry(uom_frame, width=14, font=("Segoe UI", 9))
        self.ent_qty.pack(side="left")
        r += 1

        grp2 = tk.LabelFrame(
            parent,
            text=" 📅 Сроки ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=12,
            pady=8,
        )
        grp2.pack(fill="x", padx=12, pady=4)
        grp2.grid_columnconfigure(1, weight=1)

        r = 0

        tk.Label(grp2, text="Начало *:", bg=C["panel"], font=("Segoe UI", 9)).grid(
            row=r, column=0, sticky="e", padx=(0, 8), pady=4
        )
        date_frame = tk.Frame(grp2, bg=C["panel"])
        date_frame.grid(row=r, column=1, sticky="w", pady=4)

        self.ent_start = ttk.Entry(date_frame, width=12, font=("Segoe UI", 9))
        self.ent_start.pack(side="left")
        self.ent_start.bind("<FocusOut>", lambda _e: self._update_duration())

        tk.Label(date_frame, text="   Окончание *:", bg=C["panel"], font=("Segoe UI", 9)).pack(
            side="left", padx=(16, 4)
        )
        self.ent_finish = ttk.Entry(date_frame, width=12, font=("Segoe UI", 9))
        self.ent_finish.pack(side="left")
        self.ent_finish.bind("<FocusOut>", lambda _e: self._update_duration())

        tk.Label(date_frame, text="   Длительность:", bg=C["panel"], font=("Segoe UI", 9)).pack(
            side="left", padx=(16, 4)
        )
        self.lbl_duration = tk.Label(
            date_frame,
            text="—",
            bg=C["panel"],
            font=("Segoe UI", 9, "bold"),
            fg=C["accent"],
        )
        self.lbl_duration.pack(side="left")
        r += 1

        tk.Label(
            grp2,
            text="Формат даты: ДД.ММ.ГГГГ",
            bg=C["panel"],
            font=("Segoe UI", 7),
            fg=C["text3"],
        ).grid(row=r, column=1, sticky="w", pady=(0, 2))

        grp3 = tk.LabelFrame(
            parent,
            text=" 📊 Статус и параметры ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=12,
            pady=8,
        )
        grp3.pack(fill="x", padx=12, pady=4)
        grp3.grid_columnconfigure(1, weight=1)

        r = 0

        tk.Label(grp3, text="Статус:", bg=C["panel"], font=("Segoe UI", 9)).grid(
            row=r, column=0, sticky="e", padx=(0, 8), pady=4
        )
        status_frame = tk.Frame(grp3, bg=C["panel"])
        status_frame.grid(row=r, column=1, sticky="w", pady=4)

        st_vals = [STATUS_LABELS.get(s, s) for s in STATUS_LIST]
        self.cmb_status = ttk.Combobox(status_frame, state="readonly", width=18, values=st_vals, font=("Segoe UI", 9))
        self.cmb_status.pack(side="left")
        self.cmb_status.bind("<<ComboboxSelected>>", lambda _e: self._update_status_color())

        self.cv_status = tk.Canvas(status_frame, width=16, height=16, bg=C["panel"], highlightthickness=0)
        self.cv_status.pack(side="left", padx=(8, 0))
        r += 1

        self.var_milestone = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            grp3,
            text="  Веха (milestone) — ключевое событие",
            variable=self.var_milestone,
        ).grid(row=r, column=0, columnspan=2, sticky="w", pady=(4, 2))

    def _build_assign_tab(self, parent):
        bar = tk.Frame(parent, bg=C["panel"])
        bar.pack(fill="x", padx=12, pady=(10, 4))

        tk.Button(
            bar,
            text="👷 Выбрать работников…",
            font=("Segoe UI", 9, "bold"),
            bg=C["btn_bg"],
            fg=C["btn_fg"],
            activebackground="#0d47a1",
            activeforeground="white",
            relief="flat",
            cursor="hand2",
            padx=12,
            pady=4,
            command=self._open_employee_selector,
        ).pack(side="left", padx=2)

        ttk.Button(bar, text="🗑 Снять выбранного", command=self._remove_assignment).pack(
            side="left", padx=(12, 2)
        )
        ttk.Button(bar, text="✏ Изменить роль", command=self._change_selected_role).pack(
            side="left", padx=(6, 2)
        )

        self.lbl_assign_count = tk.Label(
            bar,
            text="Назначено: 0",
            bg=C["panel"],
            font=("Segoe UI", 9),
            fg=C["text2"],
        )
        self.lbl_assign_count.pack(side="right", padx=8)

        role_frame = tk.Frame(parent, bg=C["panel"])
        role_frame.pack(fill="x", padx=12, pady=(0, 4))

        tk.Label(role_frame, text="Роль для новых:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.cmb_role = ttk.Combobox(
            role_frame,
            state="readonly",
            width=14,
            values=TASK_ROLE_LABELS,
            font=("Segoe UI", 9),
        )
        self.cmb_role.pack(side="left", padx=(6, 0))
        self.cmb_role.current(0)

        assign_frame = tk.LabelFrame(
            parent,
            text=" Назначенные работники ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=10,
            pady=6,
        )
        assign_frame.pack(fill="both", expand=True, padx=12, pady=(4, 8))

        cols = ("fio", "tbn", "position", "role", "department")
        self.assign_tree = ttk.Treeview(
            assign_frame,
            columns=cols,
            show="headings",
            selectmode="browse",
            height=10,
        )
        for c, t, w in [
            ("fio", "ФИО", 220),
            ("tbn", "ТБН", 80),
            ("position", "Должность", 160),
            ("role", "Роль", 120),
            ("department", "Подразделение", 160),
        ]:
            self.assign_tree.heading(c, text=t)
            self.assign_tree.column(c, width=w, anchor="w")

        vsb = ttk.Scrollbar(assign_frame, orient="vertical", command=self.assign_tree.yview)
        self.assign_tree.configure(yscrollcommand=vsb.set)
        self.assign_tree.pack(side="left", fill="both", expand=True, pady=(4, 0))
        vsb.pack(side="right", fill="y", pady=(4, 0))

        self.assign_tree.tag_configure("foreman", background="#e3f2fd")
        self.assign_tree.tag_configure("inspector", background="#fff3e0")

        self.assign_tree.bind("<Double-1>", lambda _e: self._change_selected_role())

    # --------------------------------------------------------
    # INIT / LOAD
    # --------------------------------------------------------

    def _fill_init(self):
        iw = self.init.get("work_type_id")
        if iw:
            for i, w in enumerate(self.work_types):
                if _work_type_id(w) == int(iw):
                    self.cmb_wt.current(i)
                    break
        elif self.work_types:
            self.cmb_wt.current(0)

        self.ent_name.insert(0, self.init.get("name", ""))

        iu = normalize_spaces(self.init.get("uom_code") or "")
        if iu:
            for i, u in enumerate(self.uoms, start=1):
                if _uom_code(u) == iu:
                    self.cmb_uom.current(i)
                    break
        else:
            self.cmb_uom.current(0)

        if self.init.get("plan_qty") is not None:
            self.ent_qty.insert(0, fmt_qty(self.init.get("plan_qty")))

        d0 = self.init.get("plan_start") or today()
        d1 = self.init.get("plan_finish") or today()

        if isinstance(d0, str):
            try:
                d0 = datetime.fromisoformat(d0).date()
            except Exception:
                d0 = parse_date(d0)

        if isinstance(d1, str):
            try:
                d1 = datetime.fromisoformat(d1).date()
            except Exception:
                d1 = parse_date(d1)

        self.ent_start.insert(0, fmt_date(d0))
        self.ent_finish.insert(0, fmt_date(d1))

        init_status = normalize_spaces(self.init.get("status") or "planned")
        try:
            self.cmb_status.current(STATUS_LIST.index(init_status))
        except Exception:
            self.cmb_status.current(0)

        self.var_milestone.set(bool(self.init.get("is_milestone")))

    def _load_assignments(self):
        src_assignments = self.init.get("_assignments") or self.init.get("assignments")
        if src_assignments:
            self._assignments = [_assignment_from_any(x) for x in src_assignments]
            self._assignments = _deduplicate_assignments(self._assignments)
            self._render_assignments()
            return

        task_id = self.init.get("id")
        if task_id:
            try:
                self._assignments = GprEmployeeService.load_task_assignments(int(task_id))
                self._assignments = _deduplicate_assignments(self._assignments)
            except Exception:
                logger.exception("Ошибка загрузки назначений задачи")
                self._assignments = []

        self._render_assignments()

    # --------------------------------------------------------
    # HELPERS
    # --------------------------------------------------------

    def _update_duration(self):
        try:
            ds = parse_date(self.ent_start.get())
            df = parse_date(self.ent_finish.get())
            dur = (df - ds).days + 1
            if dur < 0:
                self.lbl_duration.config(text="⚠ ошибка", fg=C["error"])
            else:
                self.lbl_duration.config(text=f"{dur} дн.", fg=C["accent"])
        except Exception:
            self.lbl_duration.config(text="—", fg=C["text3"])

    def _update_status_color(self):
        si = self.cmb_status.current()
        code = STATUS_LIST[si] if si >= 0 else "planned"
        fill = status_fill_color(code)
        self.cv_status.delete("all")
        self.cv_status.create_oval(2, 2, 14, 14, fill=fill, outline="#999")

    def _update_info(self):
        tid = self.init.get("id")
        if tid:
            self.lbl_info.config(text=f"ID задачи: {tid}")
        else:
            self.lbl_info.config(text="Новая задача")

    def _selected_new_role_code(self) -> str:
        idx = self.cmb_role.current()
        if idx < 0:
            return "executor"
        return TASK_ROLE_LIST[idx]

    # --------------------------------------------------------
    # ASSIGNMENTS
    # --------------------------------------------------------

    def _open_employee_selector(self):
        try:
            employees = GprEmployeeService.load_all_active_employees()
        except Exception as e:
            logger.exception("Ошибка загрузки сотрудников")
            messagebox.showerror("Сотрудники", f"Не удалось загрузить сотрудников:\n{e}", parent=self)
            return

        if not employees:
            messagebox.showinfo("Сотрудники", "Список сотрудников пуст.", parent=self)
            return

        preselected = [(a.fio, a.tbn) for a in self._assignments]
        dlg = SelectEmployeesDialog(self, employees, current_dep="Все", preselected_keys=preselected)
        self.wait_window(dlg)

        if dlg.result is None:
            return

        role_code = self._selected_new_role_code()

        existing_keys = {
            (
                int(a.employee_id) if a.employee_id is not None else None,
                normalize_spaces(a.fio).lower(),
                normalize_spaces(a.tbn),
            )
            for a in self._assignments
        }

        for fio, tbn, position, department in dlg.result:
            emp_id = None
            try:
                emp_id = GprEmployeeService.find_employee_id(fio, tbn)
            except Exception:
                logger.exception("Ошибка поиска employee_id для %r / %r", fio, tbn)

            key = (
                int(emp_id) if emp_id is not None else None,
                normalize_spaces(fio).lower(),
                normalize_spaces(tbn),
            )
            if key in existing_keys:
                continue

            self._assignments.append(
                GprAssignment(
                    employee_id=emp_id,
                    fio=fio,
                    tbn=tbn,
                    position=position,
                    department=department,
                    role_in_task=role_code,
                    note=None,
                ).normalized_copy()
            )
            existing_keys.add(key)

        self._assignments = _deduplicate_assignments(self._assignments)
        self._render_assignments()

    def _remove_assignment(self):
        sel = self.assign_tree.selection()
        if not sel:
            return
        idx = self.assign_tree.index(sel[0])
        if 0 <= idx < len(self._assignments):
            self._assignments.pop(idx)
            self._render_assignments()

    def _change_selected_role(self):
        sel = self.assign_tree.selection()
        if not sel:
            return

        idx = self.assign_tree.index(sel[0])
        if not (0 <= idx < len(self._assignments)):
            return

        current_assignment = self._assignments[idx]
        dlg = RoleSelectDialog(self, current_role=current_assignment.role_in_task)
        if dlg.result:
            self._assignments[idx].role_in_task = dlg.result
            self._assignments[idx] = self._assignments[idx].normalized_copy()
            self._render_assignments()

    def _render_assignments(self):
        self.assign_tree.delete(*self.assign_tree.get_children())

        for a in self._assignments:
            role_text = role_label(a.role_in_task)
            tags = (a.role_in_task,) if a.role_in_task in ("foreman", "inspector") else ()
            self.assign_tree.insert(
                "",
                "end",
                values=(
                    a.fio or "",
                    a.tbn or "",
                    a.position or "",
                    role_text,
                    a.department or "",
                ),
                tags=tags,
            )

        self.lbl_assign_count.config(text=f"Назначено: {len(self._assignments)}")

    # --------------------------------------------------------
    # OK / CANCEL
    # --------------------------------------------------------

    def _on_ok(self):
        try:
            wi = self.cmb_wt.current()
            if wi < 0:
                raise ValueError("Выберите тип работ")

            wt_obj = self.work_types[wi]
            wt_id = _work_type_id(wt_obj)
            wt_name = _work_type_name(wt_obj)
            if wt_id is None:
                raise ValueError("Не удалось определить тип работ")

            name = normalize_spaces(self.ent_name.get())
            if not name:
                raise ValueError("Введите вид работ")

            uom_code = None
            ui = self.cmb_uom.current()
            if ui > 0:
                uom_code = _uom_code(self.uoms[ui - 1]) or None

            qty_raw = normalize_spaces(self.ent_qty.get())
            qty = safe_float(qty_raw) if qty_raw else None
            if qty is not None and qty < 0:
                raise ValueError("Объём не может быть отрицательным")

            ds = parse_date(self.ent_start.get())
            df = parse_date(self.ent_finish.get())
            if df < ds:
                raise ValueError("Окончание раньше начала")

            si = self.cmb_status.current()
            status = STATUS_LIST[si] if si >= 0 else "planned"

            self._assignments = _deduplicate_assignments(self._assignments)

            self.result = {
                "id": self.init.get("id"),
                "client_id": self.init.get("client_id"),
                "parent_id": self.init.get("parent_id"),
                "work_type_id": wt_id,
                "work_type_name": wt_name,
                "name": name,
                "uom_code": uom_code,
                "plan_qty": qty,
                "plan_start": ds,
                "plan_finish": df,
                "status": status,
                "is_milestone": bool(self.var_milestone.get()),
                "sort_order": int(self.init.get("sort_order") or 0),
                "_assignments": [a.to_dict() for a in self._assignments],
            }

            try:
                self.grab_release()
            except Exception:
                pass
            self.destroy()

        except Exception as e:
            messagebox.showwarning("Работа ГПР", str(e), parent=self)
            try:
                self.nb.select(0)
            except Exception:
                pass

    def _on_cancel(self):
        self.result = None
        try:
            self.grab_release()
        except Exception:
            pass
        self.destroy()


# ═══════════════════════════════════════════════════════════════
#  API
# ═══════════════════════════════════════════════════════════════
def open_task_dialog(parent, work_types, uoms, init=None, user_id=None) -> Optional[Dict[str, Any]]:
    """
    Фабрика, совместимая со старым API.
    """
    dlg = TaskEditDialogPro(parent, work_types, uoms, init=init, user_id=user_id)
    parent.wait_window(dlg)
    return dlg.result


__all__ = [
    "_EmployeeService",
    "RoleSelectDialog",
    "TaskEditDialogPro",
    "open_task_dialog",
]
