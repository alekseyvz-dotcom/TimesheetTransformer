from __future__ import annotations

import tkinter as tk
from datetime import date
from tkinter import messagebox, simpledialog, ttk
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from gpr_common import (
    C,
    STATUS_LABELS,
    STATUS_LIST,
    TASK_ROLES,
    TASK_ROLE_LABELS,
    TASK_ROLE_LIST,
    GprAssignment,
    GprTask,
    coerce_to_date,
    fmt_date,
    fmt_qty,
    normalize_spaces,
    normalize_status,
    normalize_task_role,
    parse_date,
    role_label,
    safe_float,
    task_to_dialog_init,
    today,
)

# Типы колбэков, чтобы не тащить БД прямо в UI
EmployeeListLoader = Callable[[], List[Tuple[str, str, str, str]]]
TaskAssignmentsLoader = Callable[[int], List[Dict[str, Any]]]


# ============================================================
# Общие UI helpers
# ============================================================

def center_toplevel(win: tk.Toplevel, parent: tk.Misc | None = None) -> None:
    try:
        win.update_idletasks()
        if parent is not None and parent.winfo_exists():
            px = parent.winfo_rootx()
            py = parent.winfo_rooty()
            pw = parent.winfo_width()
            ph = parent.winfo_height()
            sw = win.winfo_width()
            sh = win.winfo_height()
            x = px + max(0, (pw - sw) // 2)
            y = py + max(0, (ph - sh) // 2)
            win.geometry(f"+{x}+{y}")
            return

        screen_w = win.winfo_screenwidth()
        screen_h = win.winfo_screenheight()
        sw = win.winfo_width()
        sh = win.winfo_height()
        x = max(0, (screen_w - sw) // 2)
        y = max(0, (screen_h - sh) // 2)
        win.geometry(f"+{x}+{y}")
    except Exception:
        pass


def setup_modal_window(win: tk.Toplevel, parent: tk.Misc | None = None) -> None:
    try:
        if parent is not None:
            win.transient(parent)
    except Exception:
        pass

    try:
        win.grab_set()
    except Exception:
        pass

    try:
        win.focus_set()
    except Exception:
        pass

    center_toplevel(win, parent)


def _get_field(item: Any, key: str, default=None):
    if isinstance(item, Mapping):
        return item.get(key, default)
    return getattr(item, key, default)


def _work_type_name(item: Any) -> str:
    return normalize_spaces(_get_field(item, "name", ""))


def _work_type_id(item: Any) -> Optional[int]:
    value = _get_field(item, "id")
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def _uom_code(item: Any) -> str:
    return normalize_spaces(_get_field(item, "code", ""))


def _uom_name(item: Any) -> str:
    return normalize_spaces(_get_field(item, "name", ""))


# ============================================================
# Автодополняемый combobox
# ============================================================

class AutoCompleteCombobox(ttk.Combobox):
    def __init__(self, master=None, **kw):
        super().__init__(master, **kw)
        self._all_values: List[str] = []
        self.bind("<KeyRelease>", self._on_keyrelease)
        self.bind("<Control-BackSpace>", self._clear_all)
        self.bind("<FocusOut>", self._on_focus_out)

    def set_values(self, values: Sequence[str]):
        self._all_values = list(values) if values is not None else []
        self.config(values=self._all_values)

    def set_completion_list(self, values: Sequence[str]):
        self.set_values(values)

    def _on_keyrelease(self, event):
        if event.keysym in ("BackSpace", "Left", "Right", "Up", "Down", "Return", "Tab", "Escape"):
            return

        text = normalize_spaces(self.get())
        if not text:
            self.config(values=self._all_values)
            return

        filtered = [v for v in self._all_values if text.lower() in v.lower()]
        self.config(values=filtered)

    def _clear_all(self, event=None):
        self.delete(0, tk.END)
        self.config(values=self._all_values)
        return "break"

    def _on_focus_out(self, event):
        current = normalize_spaces(self.get())
        if current and current not in self._all_values:
            self.set("")


# ============================================================
# Диалог диапазона дат
# ============================================================

class DateRangeDialog(simpledialog.Dialog):
    def __init__(self, parent, d0: date, d1: date):
        self._d0 = d0
        self._d1 = d1
        self.result: Optional[Tuple[date, date]] = None
        super().__init__(parent, title="Диапазон дат отображения")

    def body(self, master):
        tk.Label(master, text="С (дд.мм.гггг):").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=4)
        self.e0 = ttk.Entry(master, width=14)
        self.e0.grid(row=0, column=1, pady=4)
        self.e0.insert(0, fmt_date(self._d0))

        tk.Label(master, text="По (дд.мм.гггг):").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=4)
        self.e1 = ttk.Entry(master, width=14)
        self.e1.grid(row=1, column=1, pady=4)
        self.e1.insert(0, fmt_date(self._d1))
        return self.e0

    def validate(self):
        try:
            a = parse_date(self.e0.get())
            b = parse_date(self.e1.get())
            if b < a:
                raise ValueError("Дата окончания не может быть раньше даты начала.")
            self._a = a
            self._b = b
            return True
        except Exception as e:
            messagebox.showwarning("Даты", str(e), parent=self)
            return False

    def apply(self):
        self.result = (self._a, self._b)


# ============================================================
# Диалог выбора шаблона
# ============================================================

class TemplateSelectDialog(simpledialog.Dialog):
    def __init__(self, parent, templates: Sequence[Mapping[str, Any]]):
        self.templates = list(templates or [])
        self.result: Optional[int] = None
        super().__init__(parent, title="Выбор шаблона ГПР")

    def body(self, master):
        tk.Label(master, text="Выберите шаблон:").pack(anchor="w", pady=(0, 6))
        self.lb = tk.Listbox(master, width=50, height=min(15, max(4, len(self.templates))))
        for tpl in self.templates:
            self.lb.insert("end", normalize_spaces(tpl.get("name") or ""))
        self.lb.pack(fill="both", expand=True)
        if self.templates:
            self.lb.selection_set(0)
        return self.lb

    def validate(self):
        sel = self.lb.curselection()
        if not sel:
            messagebox.showwarning("Шаблон", "Выберите шаблон.", parent=self)
            return False
        self._idx = sel[0]
        return True

    def apply(self):
        self.result = int(self.templates[self._idx]["id"])


# ============================================================
# Диалог выбора сотрудников
# ============================================================

class SelectEmployeesDialog(tk.Toplevel):
    def __init__(self, parent, employees, current_dep: str):
        super().__init__(parent)
        self.parent = parent
        self.employees = list(employees or [])
        self.current_dep = normalize_spaces(current_dep or "")
        self.result = None

        self.title("Выбор сотрудников")
        self.resizable(True, True)

        setup_modal_window(self, parent)

        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.bind("<Escape>", lambda _e: self._on_cancel())

        self.var_only_dep = tk.BooleanVar(value=bool(self.current_dep and self.current_dep != "Все"))
        self.var_search = tk.StringVar()

        main = tk.Frame(self, padx=10, pady=10)
        main.pack(fill="both", expand=True)

        top = tk.Frame(main)
        top.pack(fill="x")

        tk.Label(
            top,
            text=f"Подразделение: {self.current_dep or 'Все'}",
            font=("Segoe UI", 10, "bold"),
        ).grid(row=0, column=0, columnspan=2, sticky="w")

        ttk.Checkbutton(
            top,
            text="Показывать только сотрудников этого подразделения",
            variable=self.var_only_dep,
            command=self._refilter,
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 4))

        tk.Label(top, text="Поиск (ФИО / таб.№):").grid(row=2, column=0, sticky="w", pady=(4, 2))
        ent_search = ttk.Entry(top, textvariable=self.var_search, width=40)
        ent_search.grid(row=2, column=1, sticky="w", pady=(4, 2))
        ent_search.bind("<KeyRelease>", lambda _e: self._refilter())

        tbl_frame = tk.Frame(main)
        tbl_frame.pack(fill="both", expand=True, pady=(8, 4))

        columns = ("fio", "tbn", "pos", "dep")
        self.tree = ttk.Treeview(tbl_frame, columns=columns, show="headings", selectmode="none")
        self.tree.heading("fio", text="ФИО")
        self.tree.heading("tbn", text="Таб.№")
        self.tree.heading("pos", text="Должность")
        self.tree.heading("dep", text="Подразделение")

        self.tree.column("fio", width=260, anchor="w")
        self.tree.column("tbn", width=80, anchor="center", stretch=False)
        self.tree.column("pos", width=180, anchor="w")
        self.tree.column("dep", width=140, anchor="w")

        self.tree.tag_configure("checked", font=("Segoe UI", 9, "bold"))
        self.tree.tag_configure("unchecked", font=("Segoe UI", 9))

        vsb = ttk.Scrollbar(tbl_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.bind("<Button-1>", self._on_tree_click)
        self.tree.bind("<Double-1>", self._on_tree_click)

        self._filtered_indices: List[int] = []
        self._selected_indices: set[int] = set()

        sel_frame = tk.Frame(main)
        sel_frame.pack(fill="x")
        ttk.Button(sel_frame, text="Отметить всех", command=self._select_all).pack(side="left", padx=(0, 4))
        ttk.Button(sel_frame, text="Снять все", command=self._clear_all).pack(side="left", padx=4)

        self.lbl_selected = tk.Label(sel_frame, text="Выбрано: 0", bg=sel_frame["bg"])
        self.lbl_selected.pack(side="right")

        btns = tk.Frame(main)
        btns.pack(fill="x", pady=(8, 0))
        ttk.Button(btns, text="OK", command=self._on_ok).pack(side="right", padx=(4, 0))
        ttk.Button(btns, text="Отмена", command=self._on_cancel).pack(side="right")

        self._refilter()
        self._update_selected_count()
        center_toplevel(self, parent)

    def _update_selected_count(self):
        try:
            self.lbl_selected.config(text=f"Выбрано: {len(self._selected_indices)}")
        except Exception:
            pass

    def _refilter(self):
        search = normalize_spaces(self.var_search.get()).lower()
        only_dep = bool(self.var_only_dep.get())
        dep_sel = self.current_dep

        self.tree.delete(*self.tree.get_children())
        self._filtered_indices.clear()

        for idx, (fio, tbn, pos, dep) in enumerate(self.employees):
            fio = normalize_spaces(fio)
            tbn = normalize_spaces(tbn)
            pos = normalize_spaces(pos)
            dep = normalize_spaces(dep)

            if only_dep and dep_sel and dep_sel != "Все":
                if dep != dep_sel:
                    continue

            if search:
                if search not in fio.lower() and search not in tbn.lower():
                    continue

            checked = idx in self._selected_indices
            display_fio = f"[{'x' if checked else ' '}] {fio}"

            iid = f"emp_{idx}"
            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(display_fio, tbn, pos, dep),
                tags=("checked" if checked else "unchecked",),
            )
            self._filtered_indices.append(idx)

        self._update_selected_count()

    def _toggle_index(self, idx: int):
        if idx in self._selected_indices:
            self._selected_indices.remove(idx)
        else:
            self._selected_indices.add(idx)
        self._update_selected_count()

    def _on_tree_click(self, event):
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return

        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return

        try:
            pos_in_view = self.tree.index(row_id)
            emp_index = self._filtered_indices[pos_in_view]
        except Exception:
            return

        self._toggle_index(emp_index)

        fio, tbn, pos, dep = self.employees[emp_index]
        checked = emp_index in self._selected_indices
        display_fio = f"[{'x' if checked else ' '}] {fio}"
        self.tree.item(
            row_id,
            values=(display_fio, tbn, pos, dep),
            tags=("checked" if checked else "unchecked",),
        )

    def _select_all(self):
        for emp_index in self._filtered_indices:
            self._selected_indices.add(emp_index)
        self._refilter()

    def _clear_all(self):
        self._selected_indices.clear()
        self._refilter()

    def _on_ok(self):
        if not self._selected_indices:
            if not messagebox.askyesno(
                "Выбор сотрудников",
                "Не выбрано ни одного сотрудника.\nЗакрыть окно?",
                parent=self,
            ):
                return
            self.result = []
        else:
            self.result = [self.employees[i] for i in sorted(self._selected_indices)]
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()


# ============================================================
# Карточка работы ГПР
# ============================================================

class TaskEditDialogPro(tk.Toplevel):
    """
    Профессиональный диалог задачи ГПР:
    - основные данные
    - назначения сотрудников
    """

    def __init__(
        self,
        parent,
        work_types: Sequence[Any],
        uoms: Sequence[Any],
        init: Optional[GprTask | Mapping[str, Any]] = None,
        user_id: Optional[int] = None,
        employee_list_loader: Optional[EmployeeListLoader] = None,
        task_assignments_loader: Optional[TaskAssignmentsLoader] = None,
    ):
        super().__init__(parent)
        self.parent = parent
        self.transient(parent)

        self.work_types = list(work_types or [])
        self.uoms = list(uoms or [])
        self.user_id = user_id

        self.employee_list_loader = employee_list_loader
        self.task_assignments_loader = task_assignments_loader

        self.init = task_to_dialog_init(init) if init is not None else {}
        self.result: Optional[Dict[str, Any]] = None

        self._assignments: List[GprAssignment] = []

        task_name = normalize_spaces(self.init.get("name") or "")
        self.title(f"✏️ Работа: {task_name[:60]}" if task_name else "➕ Новая работа ГПР")
        self.minsize(720, 580)
        self.resizable(True, True)

        setup_modal_window(self, parent)

        self._build_ui()
        self._fill_init()
        self._load_assignments()
        self._center()

        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.bind("<Escape>", lambda _e: self._on_cancel())

    # --------------------------------------------------------
    # UI
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

        self.lbl_info = tk.Label(bot, text="", font=("Segoe UI", 8), fg=C["text3"], bg=C["bg"])
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

        row = 0
        tk.Label(grp1, text="Тип работ *:", bg=C["panel"], font=("Segoe UI", 9)).grid(
            row=row, column=0, sticky="e", padx=(0, 8), pady=4
        )
        wt_values = [_work_type_name(w) for w in self.work_types]
        self.cmb_wt = ttk.Combobox(grp1, state="readonly", width=44, values=wt_values, font=("Segoe UI", 9))
        self.cmb_wt.grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        tk.Label(grp1, text="Вид работ *:", bg=C["panel"], font=("Segoe UI", 9)).grid(
            row=row, column=0, sticky="e", padx=(0, 8), pady=4
        )
        self.ent_name = ttk.Entry(grp1, width=48, font=("Segoe UI", 9))
        self.ent_name.grid(row=row, column=1, sticky="ew", pady=4)
        row += 1

        tk.Label(grp1, text="Ед. изм.:", bg=C["panel"], font=("Segoe UI", 9)).grid(
            row=row, column=0, sticky="e", padx=(0, 8), pady=4
        )
        uom_frame = tk.Frame(grp1, bg=C["panel"])
        uom_frame.grid(row=row, column=1, sticky="w", pady=4)

        uom_values = ["—"] + [
            f"{_uom_code(u)} — {_uom_name(u)}" if _uom_name(u) else _uom_code(u)
            for u in self.uoms
        ]
        self.cmb_uom = ttk.Combobox(uom_frame, state="readonly", width=22, values=uom_values, font=("Segoe UI", 9))
        self.cmb_uom.pack(side="left")

        tk.Label(uom_frame, text="   Объём план:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left", padx=(16, 4))
        self.ent_qty = ttk.Entry(uom_frame, width=14, font=("Segoe UI", 9))
        self.ent_qty.pack(side="left")
        row += 1

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

        row = 0
        tk.Label(grp2, text="Начало *:", bg=C["panel"], font=("Segoe UI", 9)).grid(
            row=row, column=0, sticky="e", padx=(0, 8), pady=4
        )
        date_frame = tk.Frame(grp2, bg=C["panel"])
        date_frame.grid(row=row, column=1, sticky="w", pady=4)

        self.ent_start = ttk.Entry(date_frame, width=12, font=("Segoe UI", 9))
        self.ent_start.pack(side="left")
        self.ent_start.bind("<FocusOut>", lambda _e: self._update_duration())

        tk.Label(date_frame, text="   Окончание *:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left", padx=(16, 4))
        self.ent_finish = ttk.Entry(date_frame, width=12, font=("Segoe UI", 9))
        self.ent_finish.pack(side="left")
        self.ent_finish.bind("<FocusOut>", lambda _e: self._update_duration())

        tk.Label(date_frame, text="   Длительность:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left", padx=(16, 4))
        self.lbl_duration = tk.Label(date_frame, text="—", bg=C["panel"], font=("Segoe UI", 9, "bold"), fg=C["accent"])
        self.lbl_duration.pack(side="left")
        row += 1

        tk.Label(grp2, text="Формат: ДД.ММ.ГГГГ", bg=C["panel"], font=("Segoe UI", 7), fg=C["text3"]).grid(
            row=row, column=1, sticky="w", pady=(0, 2)
        )

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

        row = 0
        tk.Label(grp3, text="Статус:", bg=C["panel"], font=("Segoe UI", 9)).grid(
            row=row, column=0, sticky="e", padx=(0, 8), pady=4
        )
        status_frame = tk.Frame(grp3, bg=C["panel"])
        status_frame.grid(row=row, column=1, sticky="w", pady=4)

        status_values = [STATUS_LABELS.get(s, s) for s in STATUS_LIST]
        self.cmb_status = ttk.Combobox(
            status_frame,
            state="readonly",
            width=18,
            values=status_values,
            font=("Segoe UI", 9),
        )
        self.cmb_status.pack(side="left")

        self.cv_status = tk.Canvas(status_frame, width=16, height=16, bg=C["panel"], highlightthickness=0)
        self.cv_status.pack(side="left", padx=(8, 0))
        self.cmb_status.bind("<<ComboboxSelected>>", lambda _e: self._update_status_color())
        row += 1

        self.var_milestone = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            grp3,
            text="  Веха (milestone) — ключевое событие",
            variable=self.var_milestone,
        ).grid(row=row, column=0, columnspan=2, sticky="w", pady=(4, 2))

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

        ttk.Button(bar, text="🗑 Снять выбранного", command=self._remove_assignment).pack(side="left", padx=(12, 2))

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

        ttk.Button(role_frame, text="Сделать исполнителем", command=lambda: self._set_selected_role("executor")).pack(
            side="left", padx=(16, 2)
        )
        ttk.Button(role_frame, text="Сделать бригадиром", command=lambda: self._set_selected_role("foreman")).pack(
            side="left", padx=2
        )
        ttk.Button(role_frame, text="Сделать контролёром", command=lambda: self._set_selected_role("inspector")).pack(
            side="left", padx=2
        )

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

        cols_a = ("fio", "tbn", "position", "role", "department")
        self.assign_tree = ttk.Treeview(
            assign_frame,
            columns=cols_a,
            show="headings",
            selectmode="browse",
            height=10,
        )
        for c, t, w in [
            ("fio", "ФИО", 220),
            ("tbn", "ТБН", 80),
            ("position", "Должность", 160),
            ("role", "Роль", 110),
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

    # --------------------------------------------------------
    # Init / fill
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
            for i, u in enumerate(self.uoms, 1):
                if _uom_code(u) == iu:
                    self.cmb_uom.current(i)
                    break
        else:
            self.cmb_uom.current(0)

        if self.init.get("plan_qty") is not None:
            self.ent_qty.insert(0, fmt_qty(self.init["plan_qty"]))

        d0 = coerce_to_date(self.init.get("plan_start")) or today()
        d1 = coerce_to_date(self.init.get("plan_finish")) or today()
        self.ent_start.insert(0, fmt_date(d0))
        self.ent_finish.insert(0, fmt_date(d1))

        init_status = normalize_status(self.init.get("status") or "planned")
        try:
            self.cmb_status.current(STATUS_LIST.index(init_status))
        except Exception:
            self.cmb_status.current(0)

        self.var_milestone.set(bool(self.init.get("is_milestone")))

        self._update_duration()
        self._update_status_color()
        self._update_info()

    def _load_assignments(self):
        source = self.init.get("_assignments") or self.init.get("assignments") or []
        if source:
            self._assignments = [self._assignment_from_any(x) for x in source]
            self._render_assignments()
            return

        task_id = self.init.get("id")
        if task_id and self.task_assignments_loader:
            try:
                loaded = self.task_assignments_loader(int(task_id))
                self._assignments = [self._assignment_from_any(x) for x in loaded]
            except Exception as e:
                self._assignments = []
                messagebox.showwarning(
                    "Назначения",
                    f"Не удалось загрузить назначения работников:\n{e}",
                    parent=self,
                )

        self._render_assignments()

    def _assignment_from_any(self, item: Any) -> GprAssignment:
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

    # --------------------------------------------------------
    # Helpers
    # --------------------------------------------------------

    def _center(self):
        center_toplevel(self, self.parent)

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
        if si >= 0:
            code = STATUS_LIST[si]
            col = C["border"]
            try:
                from gpr_common import status_fill_color  # локально, чтобы не плодить импорт вверху
                col = status_fill_color(code)
            except Exception:
                pass
            self.cv_status.delete("all")
            self.cv_status.create_oval(2, 2, 14, 14, fill=col, outline="#999")

    def _update_info(self):
        tid = self.init.get("id")
        if tid:
            self.lbl_info.config(text=f"ID задачи: {tid}")
        else:
            self.lbl_info.config(text="Новая задача")

    def _selected_role_code_for_new(self) -> str:
        idx = self.cmb_role.current()
        if idx < 0:
            return "executor"
        return TASK_ROLE_LIST[idx]

    # --------------------------------------------------------
    # Assignments
    # --------------------------------------------------------

    def _open_employee_selector(self):
        if not self.employee_list_loader:
            messagebox.showwarning(
                "Сотрудники",
                "Для этого окна не передан загрузчик сотрудников.",
                parent=self,
            )
            return

        try:
            employees = self.employee_list_loader()
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить сотрудников:\n{e}", parent=self)
            return

        if not employees:
            messagebox.showinfo("Сотрудники", "Список сотрудников пуст.", parent=self)
            return

        dlg = SelectEmployeesDialog(self, employees, current_dep="Все")
        self.wait_window(dlg)

        if dlg.result is None:
            return

        role_code = self._selected_role_code_for_new()

        for fio, tbn, position, department in dlg.result:
            new_assignment = GprAssignment(
                employee_id=None,
                fio=fio,
                tbn=tbn,
                position=position,
                department=department,
                role_in_task=role_code,
                note=None,
            ).normalized_copy()

            duplicate = False
            for a in self._assignments:
                same_emp_id = (
                    a.employee_id is not None
                    and new_assignment.employee_id is not None
                    and int(a.employee_id) == int(new_assignment.employee_id)
                )
                same_fio_tbn = (
                    normalize_spaces(a.fio).lower() == normalize_spaces(new_assignment.fio).lower()
                    and normalize_spaces(a.tbn) == normalize_spaces(new_assignment.tbn)
                )
                if same_emp_id or same_fio_tbn:
                    duplicate = True
                    break

            if duplicate:
                continue

            self._assignments.append(new_assignment)

        self._render_assignments()

    def _remove_assignment(self):
        sel = self.assign_tree.selection()
        if not sel:
            return
        idx = self.assign_tree.index(sel[0])
        if 0 <= idx < len(self._assignments):
            self._assignments.pop(idx)
            self._render_assignments()

    def _set_selected_role(self, role_code: str):
        role_code = normalize_task_role(role_code)
        sel = self.assign_tree.selection()
        if not sel:
            return
        idx = self.assign_tree.index(sel[0])
        if 0 <= idx < len(self._assignments):
            self._assignments[idx].role_in_task = role_code
            self._render_assignments()
            try:
                self.assign_tree.selection_set(self.assign_tree.get_children()[idx])
            except Exception:
                pass

    def _render_assignments(self):
        self.assign_tree.delete(*self.assign_tree.get_children())

        for a in self._assignments:
            tags = (a.role_in_task,) if a.role_in_task in ("foreman", "inspector") else ()
            self.assign_tree.insert(
                "",
                "end",
                values=(
                    a.fio or "",
                    a.tbn or "",
                    a.position or "",
                    role_label(a.role_in_task),
                    a.department or "",
                ),
                tags=tags,
            )

        self.lbl_assign_count.config(text=f"Назначено: {len(self._assignments)}")

    # --------------------------------------------------------
    # OK / Cancel
    # --------------------------------------------------------

    def _on_ok(self):
        try:
            wi = self.cmb_wt.current()
            if wi < 0:
                raise ValueError("Выберите тип работ")
            wt_id = _work_type_id(self.work_types[wi])
            if not wt_id:
                raise ValueError("Не удалось определить тип работ")

            nm = normalize_spaces(self.ent_name.get())
            if not nm:
                raise ValueError("Введите вид работ")

            uom_code = None
            ui = self.cmb_uom.current()
            if ui > 0:
                selected_uom = self.uoms[ui - 1]
                uom_code = _uom_code(selected_uom) or None

            qty = safe_float(self.ent_qty.get())
            ds = parse_date(self.ent_start.get())
            df = parse_date(self.ent_finish.get())
            if df < ds:
                raise ValueError("Окончание раньше начала")

            si = self.cmb_status.current()
            st = STATUS_LIST[si] if si >= 0 else "planned"

            self.result = {
                "work_type_id": wt_id,
                "name": nm,
                "uom_code": uom_code,
                "plan_qty": qty,
                "plan_start": ds,
                "plan_finish": df,
                "status": st,
                "is_milestone": bool(self.var_milestone.get()),
                "_assignments": [a.to_dict() for a in self._assignments],
                "parent_id": self.init.get("parent_id"),
            }
            self.destroy()

        except Exception as e:
            messagebox.showwarning("Работа ГПР", str(e), parent=self)
            self.nb.select(0)

    def _on_cancel(self):
        self.result = None
        self.destroy()


# ============================================================
# API factory
# ============================================================

def open_task_dialog(
    parent,
    work_types,
    uoms,
    init=None,
    user_id=None,
    employee_list_loader: Optional[EmployeeListLoader] = None,
    task_assignments_loader: Optional[TaskAssignmentsLoader] = None,
) -> Optional[Dict[str, Any]]:
    dlg = TaskEditDialogPro(
        parent,
        work_types=work_types,
        uoms=uoms,
        init=init,
        user_id=user_id,
        employee_list_loader=employee_list_loader,
        task_assignments_loader=task_assignments_loader,
    )
    parent.wait_window(dlg)
    return dlg.result


__all__ = [
    "AutoCompleteCombobox",
    "DateRangeDialog",
    "TemplateSelectDialog",
    "SelectEmployeesDialog",
    "TaskEditDialogPro",
    "open_task_dialog",
    "center_toplevel",
    "setup_modal_window",
]
