from __future__ import annotations

import logging
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, messagebox

from psycopg2.extras import RealDictCursor

# ═══════════════════════════════════════════════════════════════
#  Собственный пул (устанавливается из main_app)
# ═══════════════════════════════════════════════════════════════
db_connection_pool = None

def set_db_pool(pool):
    global db_connection_pool
    db_connection_pool = pool

def _conn():
    if not db_connection_pool:
        raise RuntimeError("DB pool not set (gpr_task_dialog)")
    return db_connection_pool.getconn()

def _release(conn):
    if db_connection_pool and conn:
        db_connection_pool.putconn(conn)

# ═══════════════════════════════════════════════════════════════
#  Константы (копия из gpr_module, без импорта)
# ═══════════════════════════════════════════════════════════════
C = {
    "bg":           "#f0f2f5",
    "panel":        "#ffffff",
    "accent":       "#1565c0",
    "accent_light": "#e3f2fd",
    "success":      "#2e7d32",
    "warning":      "#ed6c02",
    "error":        "#d32f2f",
    "border":       "#dde1e7",
    "text":         "#1a1a2e",
    "text2":        "#555",
    "text3":        "#999",
    "btn_bg":       "#1565c0",
    "btn_fg":       "#ffffff",
}

STATUS_COLORS = {
    "planned":     ("#90caf9", "#1565c0", "Запланировано"),
    "in_progress": ("#ffcc80", "#e65100", "В работе"),
    "done":        ("#a5d6a7", "#1b5e20", "Выполнено"),
    "paused":      ("#fff176", "#f9a825", "Приостановлено"),
    "canceled":    ("#ef9a9a", "#b71c1c", "Отменено"),
}

STATUS_LIST = ["planned", "in_progress", "done", "paused", "canceled"]
STATUS_LABELS = {k: v[2] for k, v in STATUS_COLORS.items()}

def _parse_date(s: str) -> date:
    return datetime.strptime(s.strip(), "%d.%m.%Y").date()

def _fmt_date(d) -> str:
    if isinstance(d, date):
        return d.strftime("%d.%m.%Y")
    return str(d or "")

def _today() -> date:
    return datetime.now().date()

def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return None

def _fmt_qty(v) -> str:
    f = _safe_float(v)
    if f is None:
        return ""
    return f"{f:.3f}".rstrip("0").rstrip(".")

# Загрузка всех активных сотрудников для SelectEmployeesDialog
def _load_all_employees() -> List[Tuple[str, str, str, str]]:
    """Возвращает [(fio, tbn, position, department), ...]."""
    conn = None
    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT e.fio,
                       COALESCE(e.tbn, '') AS tbn,
                       COALESCE(e.position, '') AS position,
                       COALESCE(d.name, '') AS department
                FROM public.employees e
                LEFT JOIN public.departments d ON d.id = e.department_id
                WHERE e.is_fired = false
                ORDER BY e.fio
            """)
            return cur.fetchall()
    finally:
        _release(conn)

def _find_employee_id(fio: str, tbn: str) -> Optional[int]:
    """По ФИО и ТБН находим id сотрудника."""
    conn = None
    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM public.employees WHERE fio=%s AND COALESCE(tbn,'')=%s LIMIT 1",
                (fio, tbn))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        _release(conn)
        
# ═══════════════════════════════════════════════════════════════
#  Сервис: работники
# ═══════════════════════════════════════════════════════════════
class _EmployeeService:
    """Загрузка и поиск работников для назначения на задачи."""

    @staticmethod
    def search_employees(query: str = "", limit: int = 50) -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if query.strip():
                    cur.execute("""
                        SELECT e.id, e.fio, e.tbn,
                               COALESCE(e.position, '') AS position,
                               COALESCE(d.name, '') AS department
                        FROM public.employees e
                        LEFT JOIN public.departments d ON d.id = e.department_id
                        WHERE e.is_fired = false
                          AND (
                            e.fio ILIKE %s
                            OR e.tbn ILIKE %s
                            OR e.position ILIKE %s
                          )
                        ORDER BY e.fio
                        LIMIT %s
                    """, (f"%{query}%", f"%{query}%", f"%{query}%", limit))
                else:
                    cur.execute("""
                        SELECT e.id, e.fio, e.tbn,
                               COALESCE(e.position, '') AS position,
                               COALESCE(d.name, '') AS department
                        FROM public.employees e
                        LEFT JOIN public.departments d ON d.id = e.department_id
                        WHERE e.is_fired = false
                        ORDER BY e.fio
                        LIMIT %s
                    """, (limit,))
                return [dict(r) for r in cur.fetchall()]
        finally:
            _release(conn)

    @staticmethod
    def load_task_assignments(task_id: int) -> List[Dict[str, Any]]:
        if not task_id:
            return []
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT a.id AS assignment_id,
                           a.employee_id, a.role_in_task, a.note,
                           e.fio, e.tbn,
                           COALESCE(e.position, '') AS position,
                           COALESCE(d.name, '') AS department
                    FROM public.gpr_task_assignments a
                    JOIN public.employees e ON e.id = a.employee_id
                    LEFT JOIN public.departments d ON d.id = e.department_id
                    WHERE a.task_id = %s
                    ORDER BY a.role_in_task, e.fio
                """, (task_id,))
                return [dict(r) for r in cur.fetchall()]
        finally:
            _release(conn)

    @staticmethod
    def save_task_assignments(task_id: int, assignments: List[Dict[str, Any]],
                               user_id: Optional[int] = None) -> None:
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM public.gpr_task_assignments WHERE task_id = %s",
                    (task_id,))
                for a in assignments:
                    cur.execute("""
                        INSERT INTO public.gpr_task_assignments
                            (task_id, employee_id, role_in_task, note, assigned_by)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (task_id, employee_id) DO UPDATE
                            SET role_in_task = EXCLUDED.role_in_task,
                                note = EXCLUDED.note
                    """, (
                        task_id,
                        a["employee_id"],
                        a.get("role_in_task", "executor"),
                        a.get("note") or None,
                        user_id,
                    ))
        finally:
            _release(conn)


# ═══════════════════════════════════════════════════════════════
#  Роли на задаче
# ═══════════════════════════════════════════════════════════════
TASK_ROLES = {
    "executor":  "Исполнитель",
    "foreman":   "Бригадир",
    "inspector": "Контролёр",
}
TASK_ROLE_LIST = list(TASK_ROLES.keys())
TASK_ROLE_LABELS = list(TASK_ROLES.values())

class SelectEmployeesDialog(tk.Toplevel):

    def __init__(self, parent, employees, current_dep: str):
        super().__init__(parent)
        self.parent = parent
        self.employees = employees
        self.current_dep = (current_dep or "").strip()
        self.result = None

        self.title("Выбор сотрудников")
        self.resizable(True, True)
        self.grab_set()

        self.var_only_dep = tk.BooleanVar(
            value=bool(self.current_dep and self.current_dep != "Все")
        )
        self.var_search = tk.StringVar()

        main = tk.Frame(self, padx=10, pady=10)
        main.pack(fill="both", expand=True)

        # --- Верхняя панель ---
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

        tk.Label(top, text="Поиск (ФИО / таб.№):").grid(
            row=2, column=0, sticky="w", pady=(4, 2)
        )
        ent_search = ttk.Entry(top, textvariable=self.var_search, width=40)
        ent_search.grid(row=2, column=1, sticky="w", pady=(4, 2))
        ent_search.bind("<KeyRelease>", lambda e: self._refilter())

        tbl_frame = tk.Frame(main)
        tbl_frame.pack(fill="both", expand=True, pady=(8, 4))

        columns = ("fio", "tbn", "pos", "dep")
        # первая псевдо-колонка "#" под чекбокс
        self.tree = ttk.Treeview(
            tbl_frame,
            columns=columns,
            show="headings",
            selectmode="none",  # выбор только через чекбокс
        )

        self.tree.heading("fio", text="ФИО")
        self.tree.heading("tbn", text="Таб.№")
        self.tree.heading("pos", text="Должность")
        self.tree.heading("dep", text="Подразделение")

        self.tree.column("fio", width=260, anchor="w")
        self.tree.column("tbn", width=80, anchor="center", stretch=False)
        self.tree.column("pos", width=180, anchor="w")
        self.tree.column("dep", width=140, anchor="w")

        bold_font = ("Segoe UI", 9, "bold")
        normal_font = ("Segoe UI", 9)
        self.tree.tag_configure("checked", font=bold_font)
        self.tree.tag_configure("unchecked", font=normal_font)

        vsb = ttk.Scrollbar(tbl_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.bind("<Button-1>", self._on_tree_click)

        self._filtered_indices = []

        self._selected_indices = set()

        self._refilter()
        self._update_selected_count()

        # --- Кнопки управления выбором ---
        sel_frame = tk.Frame(main)
        sel_frame.pack(fill="x")
        ttk.Button(sel_frame, text="Отметить всех", command=self._select_all).pack(
            side="left", padx=(0, 4)
        )
        ttk.Button(sel_frame, text="Снять все", command=self._clear_all).pack(
            side="left", padx=4
        )

        self.lbl_selected = tk.Label(
            sel_frame,
            text="Выбрано: 0",
            bg=sel_frame["bg"],
        )
        self.lbl_selected.pack(side="right")

        # --- Низ: OK / Отмена ---
        btns = tk.Frame(main)
        btns.pack(fill="x", pady=(8, 0))
        ttk.Button(btns, text="OK", command=self._on_ok).pack(
            side="right", padx=(4, 0)
        )
        ttk.Button(btns, text="Отмена", command=self._on_cancel).pack(
            side="right"
        )

        main.rowconfigure(2, weight=1)
        main.columnconfigure(0, weight=1)

        # Центрируем
        try:
            self.update_idletasks()
            px = parent.winfo_rootx()
            py = parent.winfo_rooty()
            pw = parent.winfo_width()
            ph = parent.winfo_height()
            sw = self.winfo_width()
            sh = self.winfo_height()
            self.geometry(f"+{px + (pw - sw)//2}+{py + (ph - sh)//2}")
        except Exception:
            pass

    def _update_selected_count(self):
        """Обновляет текст 'Выбрано: N'."""
        try:
            self.lbl_selected.config(text=f"Выбрано: {len(self._selected_indices)}")
        except Exception:
            pass

    def _refilter(self):
        """Перестроить список в treeview по фильтрам."""
        search = self.var_search.get().strip().lower()
        only_dep = self.var_only_dep.get()
        dep_sel = self.current_dep

        self.tree.delete(*self.tree.get_children())
        self._filtered_indices.clear()

        for idx, (fio, tbn, pos, dep) in enumerate(self.employees):
            if only_dep and dep_sel and dep_sel != "Все":
                if (dep or "").strip() != dep_sel:
                    continue

            if search:
                if search not in fio.lower() and search not in (tbn or "").lower():
                    continue

            # Отобразим строку
            # "чекбокс" будем рисовать через префикс [x]/[ ] у ФИО либо через tag
            checked = (idx in self._selected_indices)
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

    def _toggle_index(self, idx: int):
        """Переключает выбранность сотрудника по глобальному индексу employees."""
        if idx in self._selected_indices:
            self._selected_indices.remove(idx)
        else:
            self._selected_indices.add(idx)
        self._update_selected_count()

    def _on_tree_click(self, event):
        """
        ЛКМ по строке — переключаем чекбокс.
        """
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return

        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return

        # Ищем индекс в _filtered_indices
        try:
            # i — позиция в текущем отфильтрованном списке
            i = self.tree.index(row_id)
            emp_index = self._filtered_indices[i]
        except Exception:
            return

        self._toggle_index(emp_index)
        # Обновим отображение только этой строки
        fio, tbn, pos, dep = self.employees[emp_index]
        checked = (emp_index in self._selected_indices)
        display_fio = f"[{'x' if checked else ' '}] {fio}"
        self.tree.item(
            row_id,
            values=(display_fio, tbn, pos, dep),
            tags=("checked" if checked else "unchecked",),
        )

    def _select_all(self):
        """Отметить всех в текущей выборке."""
        for emp_index in self._filtered_indices:
            self._selected_indices.add(emp_index)
        self._refilter()
        self._update_selected_count()

    def _clear_all(self):
        """Снять все отметки (по всему списку)."""
        self._selected_indices.clear()
        self._refilter()
        self._update_selected_count()

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
            chosen = [self.employees[i] for i in sorted(self._selected_indices)]
            self.result = chosen
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()

# ═══════════════════════════════════════════════════════════════
#  Профессиональный диалог работы ГПР
# ═══════════════════════════════════════════════════════════════
class TaskEditDialogPro(tk.Toplevel):
    """
    Диалог с вкладками:
      1) Основные данные (тип, вид, даты, объём, статус)
      2) Назначения работников
    """

    def __init__(self, parent, work_types: List[Dict], uoms: List[Dict],
                 init: Optional[Dict[str, Any]] = None,
                 user_id: Optional[int] = None):
        super().__init__(parent)
        self.transient(parent)
        self.grab_set()

        self.work_types = work_types
        self.uoms = uoms
        self.init = init or {}
        self.user_id = user_id
        self.result: Optional[Dict[str, Any]] = None

        # Назначения (локальный список для редактирования)
        self._assignments: List[Dict[str, Any]] = []
        self._emp_search_results: List[Dict[str, Any]] = []

        # Заголовок окна
        task_name = self.init.get("name", "")
        if task_name:
            self.title(f"✏️ Работа: {task_name[:60]}")
        else:
            self.title("➕ Новая работа ГПР")

        self.minsize(680, 560)
        self.resizable(True, True)

        self._build_ui()
        self._fill_init()
        self._load_assignments()
        self._center()

    # ══════════════════════════════════════════════════════
    #  BUILD UI
    # ══════════════════════════════════════════════════════
    def _build_ui(self):
        # Заголовок
        hdr = tk.Frame(self, bg=C["accent"], pady=6)
        hdr.pack(fill="x")
        tk.Label(hdr, text="📋  Карточка работы ГПР",
                 font=("Segoe UI", 11, "bold"),
                 bg=C["accent"], fg="white", padx=10).pack(side="left")

        # Notebook
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=10, pady=(8, 4))

        # Вкладка 1: Основные данные
        tab_main = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_main, text="  📝 Основные данные  ")
        self._build_main_tab(tab_main)

        # Вкладка 2: Назначения
        tab_assign = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_assign, text="  👷 Назначения работников  ")
        self._build_assign_tab(tab_assign)

        # Кнопки внизу
        bot = tk.Frame(self, bg=C["bg"], pady=8)
        bot.pack(fill="x")

        btn_ok = tk.Button(bot, text="✅  Сохранить",
                           font=("Segoe UI", 10, "bold"),
                           bg=C["btn_bg"], fg=C["btn_fg"],
                           activebackground="#0d47a1", activeforeground="white",
                           relief="flat", cursor="hand2", padx=20, pady=6,
                           command=self._on_ok)
        btn_ok.pack(side="right", padx=(0, 16))
        btn_ok.bind("<Enter>", lambda e: btn_ok.config(bg="#0d47a1"))
        btn_ok.bind("<Leave>", lambda e: btn_ok.config(bg=C["btn_bg"]))

        btn_cancel = tk.Button(bot, text="Отмена",
                               font=("Segoe UI", 9),
                               bg="#e0e0e0", fg="#333",
                               relief="flat", cursor="hand2", padx=16, pady=6,
                               command=self._on_cancel)
        btn_cancel.pack(side="right", padx=(0, 8))

        # Информационная панель
        self.lbl_info = tk.Label(bot, text="", font=("Segoe UI", 8),
                                  fg=C["text3"], bg=C["bg"])
        self.lbl_info.pack(side="left", padx=16)

        self.bind("<Escape>", lambda e: self._on_cancel())

    # ── Вкладка: Основные данные ──
    def _build_main_tab(self, parent):
        # Группа: Работа
        grp1 = tk.LabelFrame(parent, text=" 🔧 Работа ",
                              font=("Segoe UI", 9, "bold"),
                              bg=C["panel"], fg=C["accent"],
                              padx=12, pady=8)
        grp1.pack(fill="x", padx=12, pady=(10, 4))
        grp1.grid_columnconfigure(1, weight=1)

        r = 0
        # Тип работ
        tk.Label(grp1, text="Тип работ *:", bg=C["panel"],
                 font=("Segoe UI", 9)).grid(row=r, column=0, sticky="e", padx=(0, 8), pady=4)
        wt_vals = [w["name"] for w in self.work_types]
        self.cmb_wt = ttk.Combobox(grp1, state="readonly", width=44, values=wt_vals,
                                    font=("Segoe UI", 9))
        self.cmb_wt.grid(row=r, column=1, sticky="w", pady=4)
        r += 1

        # Вид работ
        tk.Label(grp1, text="Вид работ *:", bg=C["panel"],
                 font=("Segoe UI", 9)).grid(row=r, column=0, sticky="e", padx=(0, 8), pady=4)
        self.ent_name = ttk.Entry(grp1, width=48, font=("Segoe UI", 9))
        self.ent_name.grid(row=r, column=1, sticky="ew", pady=4)
        r += 1

        # Ед. изм. + Объём (в одной строке)
        tk.Label(grp1, text="Ед. изм.:", bg=C["panel"],
                 font=("Segoe UI", 9)).grid(row=r, column=0, sticky="e", padx=(0, 8), pady=4)
        uom_frame = tk.Frame(grp1, bg=C["panel"])
        uom_frame.grid(row=r, column=1, sticky="w", pady=4)

        uom_vals = ["—"] + [f"{u['code']} — {u['name']}" for u in self.uoms]
        self.cmb_uom = ttk.Combobox(uom_frame, state="readonly", width=20,
                                     values=uom_vals, font=("Segoe UI", 9))
        self.cmb_uom.pack(side="left")

        tk.Label(uom_frame, text="   Объём план:", bg=C["panel"],
                 font=("Segoe UI", 9)).pack(side="left", padx=(16, 4))
        self.ent_qty = ttk.Entry(uom_frame, width=14, font=("Segoe UI", 9))
        self.ent_qty.pack(side="left")
        r += 1

        # Группа: Сроки
        grp2 = tk.LabelFrame(parent, text=" 📅 Сроки ",
                              font=("Segoe UI", 9, "bold"),
                              bg=C["panel"], fg=C["accent"],
                              padx=12, pady=8)
        grp2.pack(fill="x", padx=12, pady=4)
        grp2.grid_columnconfigure(1, weight=1)

        r = 0
        tk.Label(grp2, text="Начало *:", bg=C["panel"],
                 font=("Segoe UI", 9)).grid(row=r, column=0, sticky="e", padx=(0, 8), pady=4)
        date_frame = tk.Frame(grp2, bg=C["panel"])
        date_frame.grid(row=r, column=1, sticky="w", pady=4)

        self.ent_start = ttk.Entry(date_frame, width=12, font=("Segoe UI", 9))
        self.ent_start.pack(side="left")
        self.ent_start.bind("<FocusOut>", lambda e: self._update_duration())

        tk.Label(date_frame, text="   Окончание *:", bg=C["panel"],
                 font=("Segoe UI", 9)).pack(side="left", padx=(16, 4))
        self.ent_finish = ttk.Entry(date_frame, width=12, font=("Segoe UI", 9))
        self.ent_finish.pack(side="left")
        self.ent_finish.bind("<FocusOut>", lambda e: self._update_duration())

        tk.Label(date_frame, text="   Длительность:", bg=C["panel"],
                 font=("Segoe UI", 9)).pack(side="left", padx=(16, 4))
        self.lbl_duration = tk.Label(date_frame, text="—", bg=C["panel"],
                                      font=("Segoe UI", 9, "bold"), fg=C["accent"])
        self.lbl_duration.pack(side="left")
        r += 1

        # Подсказка формата
        tk.Label(grp2, text="Формат: ДД.ММ.ГГГГ", bg=C["panel"],
                 font=("Segoe UI", 7), fg=C["text3"]).grid(
            row=r, column=1, sticky="w", pady=(0, 2))

        # Группа: Статус
        grp3 = tk.LabelFrame(parent, text=" 📊 Статус и параметры ",
                              font=("Segoe UI", 9, "bold"),
                              bg=C["panel"], fg=C["accent"],
                              padx=12, pady=8)
        grp3.pack(fill="x", padx=12, pady=4)
        grp3.grid_columnconfigure(1, weight=1)

        r = 0
        tk.Label(grp3, text="Статус:", bg=C["panel"],
                 font=("Segoe UI", 9)).grid(row=r, column=0, sticky="e", padx=(0, 8), pady=4)
        status_frame = tk.Frame(grp3, bg=C["panel"])
        status_frame.grid(row=r, column=1, sticky="w", pady=4)

        st_vals = [STATUS_LABELS.get(s, s) for s in STATUS_LIST]
        self.cmb_status = ttk.Combobox(status_frame, state="readonly", width=18,
                                        values=st_vals, font=("Segoe UI", 9))
        self.cmb_status.pack(side="left")

        # Цветной индикатор статуса
        self.cv_status = tk.Canvas(status_frame, width=16, height=16,
                                    bg=C["panel"], highlightthickness=0)
        self.cv_status.pack(side="left", padx=(8, 0))
        self.cmb_status.bind("<<ComboboxSelected>>", lambda e: self._update_status_color())
        r += 1

        self.var_milestone = tk.BooleanVar(value=False)
        ttk.Checkbutton(grp3, text="  Веха (milestone) — ключевое событие",
                         variable=self.var_milestone).grid(
            row=r, column=0, columnspan=2, sticky="w", pady=(4, 2))


    def _build_assign_tab(self, parent):
        # Кнопка добавления
        bar = tk.Frame(parent, bg=C["panel"])
        bar.pack(fill="x", padx=12, pady=(10, 4))

        tk.Button(bar, text="👷 Выбрать работников…",
                  font=("Segoe UI", 9, "bold"),
                  bg=C["btn_bg"], fg=C["btn_fg"],
                  activebackground="#0d47a1", activeforeground="white",
                  relief="flat", cursor="hand2", padx=12, pady=4,
                  command=self._open_employee_selector).pack(side="left", padx=2)

        ttk.Button(bar, text="🗑 Снять выбранного",
                   command=self._remove_assignment).pack(side="left", padx=(12, 2))

        self.lbl_assign_count = tk.Label(bar, text="Назначено: 0",
                                          bg=C["panel"], font=("Segoe UI", 9),
                                          fg=C["text2"])
        self.lbl_assign_count.pack(side="right", padx=8)

        # Роль по умолчанию
        role_frame = tk.Frame(parent, bg=C["panel"])
        role_frame.pack(fill="x", padx=12, pady=(0, 4))
        tk.Label(role_frame, text="Роль для новых:", bg=C["panel"],
                 font=("Segoe UI", 9)).pack(side="left")
        self.cmb_role = ttk.Combobox(role_frame, state="readonly", width=14,
                                      values=TASK_ROLE_LABELS,
                                      font=("Segoe UI", 9))
        self.cmb_role.pack(side="left", padx=(6, 0))
        self.cmb_role.current(0)

        # Таблица назначенных
        assign_frame = tk.LabelFrame(parent, text=" Назначенные работники ",
                                      font=("Segoe UI", 9, "bold"),
                                      bg=C["panel"], fg=C["accent"],
                                      padx=10, pady=6)
        assign_frame.pack(fill="both", expand=True, padx=12, pady=(4, 8))

        cols_a = ("fio", "tbn", "position", "role", "department")
        self.assign_tree = ttk.Treeview(assign_frame, columns=cols_a,
                                         show="headings", selectmode="browse",
                                         height=10)
        for c, t, w in [
            ("fio",        "ФИО",          220),
            ("tbn",        "ТБН",          80),
            ("position",   "Должность",    160),
            ("role",       "Роль",         110),
            ("department", "Подразделение", 160),
        ]:
            self.assign_tree.heading(c, text=t)
            self.assign_tree.column(c, width=w, anchor="w")

        vsb = ttk.Scrollbar(assign_frame, orient="vertical",
                             command=self.assign_tree.yview)
        self.assign_tree.configure(yscrollcommand=vsb.set)
        self.assign_tree.pack(side="left", fill="both", expand=True, pady=(4, 0))
        vsb.pack(side="right", fill="y", pady=(4, 0))

        self.assign_tree.tag_configure("foreman", background="#e3f2fd")
        self.assign_tree.tag_configure("inspector", background="#fff3e0")

    def _open_employee_selector(self):
        """Открывает знакомый диалог выбора сотрудников."""
        try:
            employees = _load_all_employees()
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить сотрудников:\n{e}",
                                 parent=self)
            return

        if not employees:
            messagebox.showinfo("Сотрудники", "Список сотрудников пуст.", parent=self)
            return

        from gpr_task_dialog import SelectEmployeesDialog
        dlg = SelectEmployeesDialog(self, employees, current_dep="Все")
        self.wait_window(dlg)

        if dlg.result is None:
            return  # отмена

        # Роль
        ri = self.cmb_role.current()
        role_code = TASK_ROLE_LIST[ri] if ri >= 0 else "executor"

        for (fio, tbn, position, department) in dlg.result:
            # Проверяем дубль
            already = False
            for a in self._assignments:
                if a.get("fio") == fio and a.get("tbn", "") == tbn:
                    already = True
                    break
            if already:
                continue

            emp_id = _find_employee_id(fio, tbn)
            self._assignments.append({
                "employee_id": emp_id,
                "fio": fio,
                "tbn": tbn,
                "position": position,
                "department": department,
                "role_in_task": role_code,
                "note": None,
            })

        self._render_assignments()

    # ══════════════════════════════════════════════════════
    #  FILL / INIT
    # ══════════════════════════════════════════════════════
    def _fill_init(self):
        # Тип работ
        iw = self.init.get("work_type_id")
        if iw:
            for i, w in enumerate(self.work_types):
                if int(w["id"]) == int(iw):
                    self.cmb_wt.current(i)
                    break
        elif self.work_types:
            self.cmb_wt.current(0)

        # Вид работ
        self.ent_name.insert(0, self.init.get("name", ""))

        # Ед. изм.
        iu = self.init.get("uom_code")
        if iu:
            for i, u in enumerate(self.uoms, 1):
                if u["code"] == iu:
                    self.cmb_uom.current(i)
                    break
        else:
            self.cmb_uom.current(0)

        # Объём
        if self.init.get("plan_qty") is not None:
            self.ent_qty.insert(0, _fmt_qty(self.init["plan_qty"]))

        # Даты
        d0 = self.init.get("plan_start") or _today()
        d1 = self.init.get("plan_finish") or _today()
        if isinstance(d0, str):
            d0 = datetime.fromisoformat(d0).date()
        if isinstance(d1, str):
            d1 = datetime.fromisoformat(d1).date()
        self.ent_start.insert(0, _fmt_date(d0))
        self.ent_finish.insert(0, _fmt_date(d1))

        # Статус
        ist = self.init.get("status", "planned")
        try:
            self.cmb_status.current(STATUS_LIST.index(ist))
        except Exception:
            self.cmb_status.current(0)

        # Веха
        self.var_milestone.set(bool(self.init.get("is_milestone")))

        self._update_duration()
        self._update_status_color()
        self._update_info()

    def _load_assignments(self):
        task_id = self.init.get("id")
        if task_id:
            try:
                self._assignments = _EmployeeService.load_task_assignments(task_id)
            except Exception as e:
                logging.exception("Load assignments error")
                self._assignments = []
        self._render_assignments()

    # ══════════════════════════════════════════════════════
    #  HELPERS
    # ══════════════════════════════════════════════════════
    def _update_duration(self):
        try:
            ds = _parse_date(self.ent_start.get())
            df = _parse_date(self.ent_finish.get())
            dur = (df - ds).days + 1
            if dur < 0:
                self.lbl_duration.config(text="⚠ ошибка", fg=C["error"])
            else:
                self.lbl_duration.config(
                    text=f"{dur} дн.", fg=C["accent"])
        except Exception:
            self.lbl_duration.config(text="—", fg=C["text3"])

    def _update_status_color(self):
        si = self.cmb_status.current()
        if si >= 0:
            code = STATUS_LIST[si]
            col, _, _ = STATUS_COLORS.get(code, ("#ccc", "#333", ""))
            self.cv_status.delete("all")
            self.cv_status.create_oval(2, 2, 14, 14, fill=col, outline="#999")

    def _update_info(self):
        tid = self.init.get("id")
        if tid:
            self.lbl_info.config(text=f"ID задачи: {tid}")
        else:
            self.lbl_info.config(text="Новая задача")

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
        self.geometry(f"+{max(0,x)}+{max(0,y)}")

    # ══════════════════════════════════════════════════════
    #  РАБОТНИКИ: поиск и назначение
    # ══════════════════════════════════════════════════════
    def _search_employees(self):
        q = self.var_emp_search.get().strip()
        self.emp_tree.delete(*self.emp_tree.get_children())
        self._emp_search_results.clear()
        try:
            self._emp_search_results = _EmployeeService.search_employees(q)
        except Exception as e:
            messagebox.showerror("Поиск", f"Ошибка:\n{e}", parent=self)
            return
        for emp in self._emp_search_results:
            self.emp_tree.insert("", "end", values=(
                emp["fio"], emp.get("tbn") or "",
                emp.get("position") or "", emp.get("department") or "",
            ))

    def _assign_selected(self):
        sel = self.emp_tree.selection()
        if not sel:
            messagebox.showinfo("Назначение",
                "Выберите работника из результатов поиска.", parent=self)
            return
        idx = self.emp_tree.index(sel[0])
        if idx < 0 or idx >= len(self._emp_search_results):
            return
        emp = self._emp_search_results[idx]

        # Проверяем дубль
        for a in self._assignments:
            if int(a["employee_id"]) == int(emp["id"]):
                messagebox.showinfo("Назначение",
                    f"{emp['fio']} уже назначен.", parent=self)
                return

        # Роль
        ri = self.cmb_role.current()
        role_code = TASK_ROLE_LIST[ri] if ri >= 0 else "executor"

        self._assignments.append({
            "employee_id": emp["id"],
            "fio": emp["fio"],
            "tbn": emp.get("tbn") or "",
            "position": emp.get("position") or "",
            "department": emp.get("department") or "",
            "role_in_task": role_code,
            "note": None,
        })
        self._render_assignments()

    def _remove_assignment(self):
        sel = self.assign_tree.selection()
        if not sel:
            return
        idx = self.assign_tree.index(sel[0])
        if 0 <= idx < len(self._assignments):
            self._assignments.pop(idx)
            self._render_assignments()

    def _render_assignments(self):
        self.assign_tree.delete(*self.assign_tree.get_children())
        for a in self._assignments:
            role_label = TASK_ROLES.get(a.get("role_in_task", ""), "?")
            role_code = a.get("role_in_task", "executor")
            tags = (role_code,) if role_code in ("foreman", "inspector") else ()
            self.assign_tree.insert("", "end", values=(
                a.get("fio") or "",
                a.get("tbn") or "",
                a.get("position") or "",
                role_label,
                a.get("department") or "",
            ), tags=tags)
        self.lbl_assign_count.config(text=f"Назначено: {len(self._assignments)}")

    # ══════════════════════════════════════════════════════
    #  OK / CANCEL
    # ══════════════════════════════════════════════════════
    def _on_ok(self):
        try:
            wi = self.cmb_wt.current()
            if wi < 0:
                raise ValueError("Выберите тип работ")
            wt_id = int(self.work_types[wi]["id"])

            nm = self.ent_name.get().strip()
            if not nm:
                raise ValueError("Введите вид работ")

            uom = None
            ui = self.cmb_uom.current()
            if ui > 0:
                uom = self.uoms[ui - 1]["code"]

            qty = _safe_float(self.ent_qty.get())
            ds = _parse_date(self.ent_start.get())
            df = _parse_date(self.ent_finish.get())
            if df < ds:
                raise ValueError("Окончание раньше начала")

            si = self.cmb_status.current()
            st = STATUS_LIST[si] if si >= 0 else "planned"

            self.result = {
                "work_type_id": wt_id,
                "name": nm,
                "uom_code": uom,
                "plan_qty": qty,
                "plan_start": ds,
                "plan_finish": df,
                "status": st,
                "is_milestone": bool(self.var_milestone.get()),
                "_assignments": list(self._assignments),
            }
            self.destroy()

        except Exception as e:
            messagebox.showwarning("Работа ГПР", str(e), parent=self)
            self.nb.select(0)  # переключаемся на первую вкладку

    def _on_cancel(self):
        self.result = None
        self.destroy()


# ═══════════════════════════════════════════════════════════════
#  API — фабрика для вызова из GprPage
# ═══════════════════════════════════════════════════════════════
def open_task_dialog(parent, work_types, uoms,
                     init=None, user_id=None) -> Optional[Dict[str, Any]]:
    """
    Открывает диалог, ждёт закрытия, возвращает result или None.
    """
    dlg = TaskEditDialogPro(parent, work_types, uoms,
                             init=init, user_id=user_id)
    parent.wait_window(dlg)
    return dlg.result
