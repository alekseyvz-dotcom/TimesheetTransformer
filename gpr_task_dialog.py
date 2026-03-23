# gpr_task_dialog.py — Профессиональный диалог добавления/редактирования работы ГПР
from __future__ import annotations

import logging
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, messagebox

from psycopg2.extras import RealDictCursor

from gpr_module import (
    _conn, _release, _today, _fmt_date, _parse_date,
    _fmt_qty, _safe_float, _to_date,
    C, STATUS_LABELS, STATUS_LIST, STATUS_COLORS,
)

logger = logging.getLogger(__name__)


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
        except Exception:
            logger.exception("search_employees error")
            raise
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
        except Exception:
            logger.exception("load_task_assignments error")
            raise
        finally:
            _release(conn)

    @staticmethod
    def save_task_assignments(task_id: int, assignments: List[Dict[str, Any]],
                               user_id: Optional[int] = None) -> None:
        if not task_id:
            return
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM public.gpr_task_assignments WHERE task_id = %s",
                    (task_id,))
                for a in assignments:
                    emp_id = a.get("employee_id")
                    if not emp_id:
                        continue
                    cur.execute("""
                        INSERT INTO public.gpr_task_assignments
                            (task_id, employee_id, role_in_task, note, assigned_by)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (task_id, employee_id) DO UPDATE
                            SET role_in_task = EXCLUDED.role_in_task,
                                note = EXCLUDED.note
                    """, (
                        task_id,
                        int(emp_id),
                        a.get("role_in_task", "executor"),
                        a.get("note") or None,
                        user_id,
                    ))
        except Exception:
            logger.exception("save_task_assignments error")
            raise
        finally:
            _release(conn)

class _TaskFactService:
    """Работа с фактом выполнения по задаче."""

    @staticmethod
    def load_task_facts(task_id: int) -> List[Dict[str, Any]]:
        if not task_id:
            return []

        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT f.id,
                           f.task_id,
                           f.fact_date,
                           f.period_type,
                           f.fact_qty,
                           COALESCE(f.comment, '') AS comment,
                           f.created_at,
                           COALESCE(u.full_name, '') AS creator_name
                    FROM public.gpr_task_facts f
                    LEFT JOIN public.app_users u ON u.id = f.created_by
                    WHERE f.task_id = %s
                    ORDER BY f.fact_date, f.period_type, f.id
                    """,
                    (task_id,),
                )
                rows = []
                for r in cur.fetchall():
                    d = dict(r)
                    d["fact_date"] = _to_date(d.get("fact_date"))
                    rows.append(d)
                return rows
        except Exception:
            logger.exception("load_task_facts error")
            raise
        finally:
            _release(conn)

    @staticmethod
    def save_task_facts(
        task_id: int,
        facts: List[Dict[str, Any]],
        user_id: Optional[int] = None,
    ) -> None:
        """Сохраняет весь список фактов задачи:
        - новые/изменённые: upsert по (task_id, fact_date, period_type)
        - удалённые из списка: удаляются из БД
        """
        if not task_id:
            return

        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM public.gpr_task_facts
                    WHERE task_id = %s
                    """,
                    (task_id,),
                )
                existing_ids = {int(r["id"]) for r in cur.fetchall()}
                kept_ids = set()

                for f in facts:
                    fact_date = _to_date(f.get("fact_date"))
                    if not fact_date:
                        raise ValueError("Не указана дата факта")

                    period_type = (f.get("period_type") or "day").strip()
                    if period_type not in ("day", "week"):
                        period_type = "day"

                    fact_qty = _safe_float(f.get("fact_qty"))
                    if fact_qty is None or fact_qty <= 0:
                        raise ValueError("Объём факта должен быть больше 0")

                    comment = (f.get("comment") or "").strip() or None

                    cur.execute(
                        """
                        INSERT INTO public.gpr_task_facts
                            (task_id, fact_date, period_type, fact_qty, comment, created_by)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (task_id, fact_date, period_type)
                        DO UPDATE SET
                            fact_qty = EXCLUDED.fact_qty,
                            comment = EXCLUDED.comment
                        RETURNING id
                        """,
                        (
                            task_id,
                            fact_date,
                            period_type,
                            fact_qty,
                            comment,
                            user_id,
                        ),
                    )
                    row = cur.fetchone()
                    if row:
                        kept_ids.add(int(row["id"]))

                ids_to_delete = list(existing_ids - kept_ids)
                if ids_to_delete:
                    cur.execute(
                        """
                        DELETE FROM public.gpr_task_facts
                        WHERE task_id = %s
                          AND id = ANY(%s)
                        """,
                        (task_id, ids_to_delete),
                    )

        except Exception:
            logger.exception("save_task_facts error")
            raise
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

FACT_PERIODS = {
    "day": "За день",
    "week": "За неделю",
}
FACT_PERIOD_LIST = list(FACT_PERIODS.keys())
FACT_PERIOD_LABELS = [FACT_PERIODS[x] for x in FACT_PERIOD_LIST]
FACT_PERIOD_FROM_LABEL = {v: k for k, v in FACT_PERIODS.items()}


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

        self.work_types = work_types
        self.uoms = uoms
        self.init = init or {}
        self.user_id = user_id
        self.result: Optional[Dict[str, Any]] = None

        # Назначения (локальный список для редактирования)
        self._assignments: List[Dict[str, Any]] = []
        self._emp_search_results: List[Dict[str, Any]] = []

        # Факт выполнения
        self._facts: List[Dict[str, Any]] = []
        self._fact_edit_idx: Optional[int] = None
        self._facts_changed: bool = False
        self._has_fact_tab: bool = False

        # Флаг: диалог уже закрыт (защита от двойного destroy)
        self._destroyed = False

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
        self._load_facts()

        # grab_set ПОСЛЕ построения UI — иначе может упасть на некоторых ОС
        self.grab_set()

        # Центрируем после полной отрисовки
        self.after(10, self._center)

        # Обработка закрытия крестиком
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

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

        # Вкладка 3: Факт выполнения
        tab_fact = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_fact, text="  📈 Факт выполнения  ")
        self._build_fact_tab(tab_fact)

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
        btn_ok.bind("<Enter>", lambda _e: btn_ok.config(bg="#0d47a1"))
        btn_ok.bind("<Leave>", lambda _e: btn_ok.config(bg=C["btn_bg"]))

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

        self.bind("<Escape>", lambda _e: self._on_cancel())
        self.bind("<Control-Return>", lambda _e: self._on_ok())

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
                 font=("Segoe UI", 9)).grid(
            row=r, column=0, sticky="e", padx=(0, 8), pady=4)
        wt_vals = [w["name"] for w in self.work_types]
        self.cmb_wt = ttk.Combobox(grp1, state="readonly", width=44,
                                    values=wt_vals, font=("Segoe UI", 9))
        self.cmb_wt.grid(row=r, column=1, sticky="w", pady=4)
        r += 1

        # Вид работ
        tk.Label(grp1, text="Вид работ *:", bg=C["panel"],
                 font=("Segoe UI", 9)).grid(
            row=r, column=0, sticky="e", padx=(0, 8), pady=4)
        self.ent_name = ttk.Entry(grp1, width=48, font=("Segoe UI", 9))
        self.ent_name.grid(row=r, column=1, sticky="ew", pady=4)
        r += 1

        # Ед. изм. + Объём (в одной строке)
        tk.Label(grp1, text="Ед. изм.:", bg=C["panel"],
                 font=("Segoe UI", 9)).grid(
            row=r, column=0, sticky="e", padx=(0, 8), pady=4)
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
                 font=("Segoe UI", 9)).grid(
            row=r, column=0, sticky="e", padx=(0, 8), pady=4)
        date_frame = tk.Frame(grp2, bg=C["panel"])
        date_frame.grid(row=r, column=1, sticky="w", pady=4)

        self.ent_start = ttk.Entry(date_frame, width=12, font=("Segoe UI", 9))
        self.ent_start.pack(side="left")
        self.ent_start.bind("<FocusOut>", lambda _e: self._update_duration())

        tk.Label(date_frame, text="   Окончание *:", bg=C["panel"],
                 font=("Segoe UI", 9)).pack(side="left", padx=(16, 4))
        self.ent_finish = ttk.Entry(date_frame, width=12, font=("Segoe UI", 9))
        self.ent_finish.pack(side="left")
        self.ent_finish.bind("<FocusOut>", lambda _e: self._update_duration())

        tk.Label(date_frame, text="   Длительность:", bg=C["panel"],
                 font=("Segoe UI", 9)).pack(side="left", padx=(16, 4))
        self.lbl_duration = tk.Label(date_frame, text="—", bg=C["panel"],
                                      font=("Segoe UI", 9, "bold"),
                                      fg=C["accent"])
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
                 font=("Segoe UI", 9)).grid(
            row=r, column=0, sticky="e", padx=(0, 8), pady=4)
        status_frame = tk.Frame(grp3, bg=C["panel"])
        status_frame.grid(row=r, column=1, sticky="w", pady=4)

        st_vals = [STATUS_LABELS.get(s, s) for s in STATUS_LIST]
        self.cmb_status = ttk.Combobox(status_frame, state="readonly",
                                        width=18, values=st_vals,
                                        font=("Segoe UI", 9))
        self.cmb_status.pack(side="left")

        # Цветной индикатор статуса
        self.cv_status = tk.Canvas(status_frame, width=16, height=16,
                                    bg=C["panel"], highlightthickness=0)
        self.cv_status.pack(side="left", padx=(8, 0))
        self.cmb_status.bind(
            "<<ComboboxSelected>>",
            lambda _e: self._update_status_color()
        )
        r += 1

        self.var_milestone = tk.BooleanVar(value=False)
        ttk.Checkbutton(grp3, text="  Веха (milestone) — ключевое событие",
                         variable=self.var_milestone).grid(
            row=r, column=0, columnspan=2, sticky="w", pady=(4, 2))

    # ── Вкладка: Назначения работников ──
    def _build_assign_tab(self, parent):
        # Верхняя часть: поиск
        search_frame = tk.LabelFrame(
            parent, text=" 🔍 Поиск работника ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"], fg=C["accent"],
            padx=10, pady=6)
        search_frame.pack(fill="x", padx=12, pady=(10, 4))

        sf = tk.Frame(search_frame, bg=C["panel"])
        sf.pack(fill="x")

        tk.Label(sf, text="ФИО / ТБН / Должность:", bg=C["panel"],
                 font=("Segoe UI", 9)).pack(side="left")
        self.var_emp_search = tk.StringVar()
        self.ent_emp_search = ttk.Entry(
            sf, textvariable=self.var_emp_search,
            width=32, font=("Segoe UI", 9))
        self.ent_emp_search.pack(side="left", padx=(6, 8))
        self.ent_emp_search.bind(
            "<Return>", lambda _e: self._search_employees())

        ttk.Button(sf, text="Найти",
                   command=self._search_employees).pack(side="left", padx=2)

        tk.Label(sf, text="  Роль:", bg=C["panel"],
                 font=("Segoe UI", 9)).pack(side="left", padx=(12, 4))
        self.cmb_role = ttk.Combobox(
            sf, state="readonly", width=14,
            values=TASK_ROLE_LABELS, font=("Segoe UI", 9))
        self.cmb_role.pack(side="left")
        self.cmb_role.current(0)

        ttk.Button(sf, text="➕ Назначить выбранного",
                   command=self._assign_selected).pack(
            side="left", padx=(12, 0))

        # Результаты поиска
        src_frame = tk.Frame(search_frame, bg=C["panel"])
        src_frame.pack(fill="x", pady=(6, 0))

        cols_s = ("fio", "tbn", "position", "department")
        self.emp_tree = ttk.Treeview(
            src_frame, columns=cols_s, show="headings",
            selectmode="browse", height=5)
        for c, t, w in [
            ("fio",        "ФИО",          200),
            ("tbn",        "ТБН",          80),
            ("position",   "Должность",    160),
            ("department", "Подразделение", 160),
        ]:
            self.emp_tree.heading(c, text=t)
            self.emp_tree.column(c, width=w, anchor="w")

        vsb_e = ttk.Scrollbar(src_frame, orient="vertical",
                               command=self.emp_tree.yview)
        self.emp_tree.configure(yscrollcommand=vsb_e.set)
        self.emp_tree.pack(side="left", fill="x", expand=True)
        vsb_e.pack(side="right", fill="y")

        self.emp_tree.bind("<Double-1>",
                           lambda _e: self._assign_selected())

        # Нижняя часть: текущие назначения
        assign_frame = tk.LabelFrame(
            parent, text=" 👷 Назначенные работники ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"], fg=C["accent"],
            padx=10, pady=6)
        assign_frame.pack(fill="both", expand=True, padx=12, pady=(4, 8))

        bar = tk.Frame(assign_frame, bg=C["panel"])
        bar.pack(fill="x")
        ttk.Button(bar, text="🗑 Снять назначение",
                   command=self._remove_assignment).pack(
            side="left", padx=2)
        self.lbl_assign_count = tk.Label(
            bar, text="Назначено: 0",
            bg=C["panel"], font=("Segoe UI", 8), fg=C["text2"])
        self.lbl_assign_count.pack(side="right", padx=8)

        cols_a = ("fio", "tbn", "position", "role", "department")
        self.assign_tree = ttk.Treeview(
            assign_frame, columns=cols_a,
            show="headings", selectmode="browse", height=6)
        for c, t, w in [
            ("fio",        "ФИО",          200),
            ("tbn",        "ТБН",          80),
            ("position",   "Должность",    140),
            ("role",       "Роль",         100),
            ("department", "Подразделение", 140),
        ]:
            self.assign_tree.heading(c, text=t)
            self.assign_tree.column(c, width=w, anchor="w")

        vsb_a = ttk.Scrollbar(assign_frame, orient="vertical",
                               command=self.assign_tree.yview)
        self.assign_tree.configure(yscrollcommand=vsb_a.set)
        self.assign_tree.pack(side="left", fill="both", expand=True,
                              pady=(4, 0))
        vsb_a.pack(side="right", fill="y", pady=(4, 0))

        # Подкраска ролей
        self.assign_tree.tag_configure("foreman", background="#e3f2fd")
        self.assign_tree.tag_configure("inspector", background="#fff3e0")

    # ── Вкладка: Факт выполнения ──
    def _build_fact_tab(self, parent):
        task_id = self.init.get("id")
        if not task_id:
            tk.Label(
                parent,
                text=(
                    "Факт выполнения можно вносить только для уже сохранённой задачи.\n"
                    "Сначала сохраните задачу в ГПР, затем откройте её повторно."
                ),
                bg=C["panel"],
                fg=C["text2"],
                font=("Segoe UI", 10),
                justify="left",
                padx=20,
                pady=20,
            ).pack(anchor="nw")
            self._has_fact_tab = False
            return

        self._has_fact_tab = True

        form = tk.LabelFrame(
            parent,
            text=" ➕ Добавить / изменить факт ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"], fg=C["accent"],
            padx=10, pady=8,
        )
        form.pack(fill="x", padx=12, pady=(10, 4))

        row1 = tk.Frame(form, bg=C["panel"])
        row1.pack(fill="x", pady=2)

        tk.Label(row1, text="Дата:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.ent_fact_date = ttk.Entry(row1, width=12, font=("Segoe UI", 9))
        self.ent_fact_date.pack(side="left", padx=(4, 10))
        self.ent_fact_date.insert(0, _fmt_date(_today()))

        ttk.Button(row1, text="Сегодня", command=self._fact_set_today).pack(side="left", padx=(0, 12))

        tk.Label(row1, text="Период:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.cmb_fact_period = ttk.Combobox(
            row1, state="readonly", width=14,
            values=FACT_PERIOD_LABELS, font=("Segoe UI", 9)
        )
        self.cmb_fact_period.pack(side="left", padx=(4, 12))
        self.cmb_fact_period.current(0)

        tk.Label(row1, text="Объём:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.ent_fact_qty = ttk.Entry(row1, width=14, font=("Segoe UI", 9))
        self.ent_fact_qty.pack(side="left", padx=(4, 12))

        ttk.Button(row1, text="Остаток", command=self._fact_fill_remaining).pack(side="left")

        row2 = tk.Frame(form, bg=C["panel"])
        row2.pack(fill="x", pady=(8, 2))

        tk.Label(row2, text="Комментарий:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.ent_fact_comment = ttk.Entry(row2, width=60, font=("Segoe UI", 9))
        self.ent_fact_comment.pack(side="left", padx=(4, 8), fill="x", expand=True)

        btns = tk.Frame(form, bg=C["panel"])
        btns.pack(fill="x", pady=(8, 0))

        self.btn_fact_add = ttk.Button(btns, text="Добавить факт", command=self._fact_add_or_update)
        self.btn_fact_add.pack(side="left", padx=2)

        ttk.Button(btns, text="Очистить", command=self._fact_clear_form).pack(side="left", padx=2)
        ttk.Button(btns, text="Удалить выбранный", command=self._fact_remove_selected).pack(side="left", padx=12)

        self.lbl_fact_summary = tk.Label(
            btns, text="",
            bg=C["panel"], fg=C["text2"], font=("Segoe UI", 8)
        )
        self.lbl_fact_summary.pack(side="right", padx=4)

        list_frame = tk.LabelFrame(
            parent,
            text=" 📚 Внесённый факт ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"], fg=C["accent"],
            padx=10, pady=6,
        )
        list_frame.pack(fill="both", expand=True, padx=12, pady=(4, 8))

        cols = ("date", "period", "qty", "comment", "creator")
        self.fact_tree = ttk.Treeview(
            list_frame, columns=cols, show="headings",
            selectmode="browse", height=8
        )

        for c, t, w, a in [
            ("date", "Дата", 90, "center"),
            ("period", "Период", 110, "center"),
            ("qty", "Объём", 90, "e"),
            ("comment", "Комментарий", 240, "w"),
            ("creator", "Кто внёс", 140, "w"),
        ]:
            self.fact_tree.heading(c, text=t)
            self.fact_tree.column(c, width=w, anchor=a)

        vsb = ttk.Scrollbar(list_frame, orient="vertical", command=self.fact_tree.yview)
        self.fact_tree.configure(yscrollcommand=vsb.set)
        self.fact_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.fact_tree.bind("<<TreeviewSelect>>", lambda _e: self._fact_pick_selected())
        self.fact_tree.bind("<Double-1>", lambda _e: self._fact_pick_selected())

    def _load_facts(self):
        if not self._has_fact_tab:
            return

        task_id = self.init.get("id")
        if not task_id:
            self._facts = []
            return

        try:
            self._facts = _TaskFactService.load_task_facts(int(task_id))
        except Exception:
            logger.exception("Load facts error for task %s", task_id)
            self._facts = []

        self._render_facts()
        self._fact_clear_form()

    def _fact_set_today(self):
        if not self._has_fact_tab:
            return
        self.ent_fact_date.delete(0, "end")
        self.ent_fact_date.insert(0, _fmt_date(_today()))

    def _fact_fill_remaining(self):
        if not self._has_fact_tab:
            return

        plan_qty = _safe_float(self.ent_qty.get())
        if plan_qty is None:
            plan_qty = _safe_float(self.init.get("plan_qty"))

        if plan_qty is None or plan_qty <= 0:
            messagebox.showinfo(
                "Факт",
                "Невозможно рассчитать остаток: не задан плановый объём.",
                parent=self,
            )
            return

        total_fact = sum(_safe_float(x.get("fact_qty")) or 0 for x in self._facts)
        remain = max(0.0, plan_qty - total_fact)

        self.ent_fact_qty.delete(0, "end")
        self.ent_fact_qty.insert(0, _fmt_qty(remain))

    def _fact_clear_form(self):
        if not self._has_fact_tab:
            return

        self._fact_edit_idx = None
        self.ent_fact_date.delete(0, "end")
        self.ent_fact_date.insert(0, _fmt_date(_today()))

        self.ent_fact_qty.delete(0, "end")
        self.ent_fact_comment.delete(0, "end")

        if self.cmb_fact_period["values"]:
            self.cmb_fact_period.current(0)

        self.btn_fact_add.config(text="Добавить факт")

        try:
            self.fact_tree.selection_remove(self.fact_tree.selection())
        except tk.TclError:
            pass

    def _fact_pick_selected(self):
        if not self._has_fact_tab:
            return

        sel = self.fact_tree.selection()
        if not sel:
            return

        try:
            idx = self.fact_tree.index(sel[0])
        except tk.TclError:
            return

        if not (0 <= idx < len(self._facts)):
            return

        self._fact_edit_idx = idx
        row = self._facts[idx]

        self.ent_fact_date.delete(0, "end")
        self.ent_fact_date.insert(0, _fmt_date(row.get("fact_date")))

        period_code = (row.get("period_type") or "day").strip()
        period_label = FACT_PERIODS.get(period_code, FACT_PERIODS["day"])
        try:
            self.cmb_fact_period.current(FACT_PERIOD_LABELS.index(period_label))
        except ValueError:
            self.cmb_fact_period.current(0)

        self.ent_fact_qty.delete(0, "end")
        self.ent_fact_qty.insert(0, _fmt_qty(row.get("fact_qty")))

        self.ent_fact_comment.delete(0, "end")
        self.ent_fact_comment.insert(0, row.get("comment") or "")

        self.btn_fact_add.config(text="Обновить факт")

    def _fact_add_or_update(self):
        if not self._has_fact_tab:
            return

        try:
            fact_date = _parse_date(self.ent_fact_date.get())

            period_label = (self.cmb_fact_period.get() or "").strip()
            period_type = FACT_PERIOD_FROM_LABEL.get(period_label, "day")

            fact_qty = _safe_float(self.ent_fact_qty.get())
            if fact_qty is None or fact_qty <= 0:
                raise ValueError("Введите корректный объём факта больше 0")

            comment = (self.ent_fact_comment.get() or "").strip()

            # Проверка дубля (task_id + fact_date + period_type)
            duplicate_idx = None
            for i, row in enumerate(self._facts):
                if i == self._fact_edit_idx:
                    continue
                if _to_date(row.get("fact_date")) == fact_date and \
                   (row.get("period_type") or "day").strip() == period_type:
                    duplicate_idx = i
                    break

            if duplicate_idx is not None:
                if not messagebox.askyesno(
                    "Факт",
                    "На эту дату и период уже есть запись. Заменить её?",
                    parent=self,
                ):
                    return
                self._fact_edit_idx = duplicate_idx

            row = {
                "fact_date": fact_date,
                "period_type": period_type,
                "fact_qty": fact_qty,
                "comment": comment,
                "creator_name": self.init.get("creator_name", "") or "",
            }

            if self._fact_edit_idx is None:
                self._facts.append(row)
            else:
                old = self._facts[self._fact_edit_idx]
                if old.get("id"):
                    row["id"] = old["id"]
                if old.get("created_at"):
                    row["created_at"] = old["created_at"]
                if old.get("creator_name"):
                    row["creator_name"] = old["creator_name"]
                self._facts[self._fact_edit_idx] = row

            self._facts.sort(
                key=lambda x: (
                    _to_date(x.get("fact_date")) or _today(),
                    0 if (x.get("period_type") or "day") == "day" else 1,
                )
            )

            self._facts_changed = True
            self._render_facts()
            self._fact_clear_form()

        except Exception as e:
            messagebox.showwarning("Факт", str(e), parent=self)

    def _fact_remove_selected(self):
        if not self._has_fact_tab:
            return

        sel = self.fact_tree.selection()
        if not sel:
            messagebox.showinfo("Факт", "Выберите запись факта.", parent=self)
            return

        try:
            idx = self.fact_tree.index(sel[0])
        except tk.TclError:
            return

        if not (0 <= idx < len(self._facts)):
            return

        row = self._facts[idx]
        if not messagebox.askyesno(
            "Факт",
            f"Удалить запись факта от {_fmt_date(row.get('fact_date'))}?",
            parent=self,
        ):
            return

        self._facts.pop(idx)
        self._facts_changed = True
        self._render_facts()
        self._fact_clear_form()

    def _render_facts(self):
        if not self._has_fact_tab:
            return

        self.fact_tree.delete(*self.fact_tree.get_children())

        for row in self._facts:
            period_code = (row.get("period_type") or "day").strip()
            period_label = FACT_PERIODS.get(period_code, period_code)

            self.fact_tree.insert(
                "",
                "end",
                values=(
                    _fmt_date(row.get("fact_date")),
                    period_label,
                    _fmt_qty(row.get("fact_qty")),
                    row.get("comment") or "",
                    row.get("creator_name") or "",
                ),
            )

        self._update_fact_summary()

    def _update_fact_summary(self):
        if not self._has_fact_tab:
            return

        total_fact = sum(_safe_float(x.get("fact_qty")) or 0 for x in self._facts)
        plan_qty = _safe_float(self.ent_qty.get())
        if plan_qty is None:
            plan_qty = _safe_float(self.init.get("plan_qty"))

        if plan_qty and plan_qty > 0:
            pct = min(100.0, total_fact / plan_qty * 100.0)
            text = f"Накопительный факт: {_fmt_qty(total_fact)}  |  Выполнение: {pct:.1f}%"
        else:
            text = f"Накопительный факт: {_fmt_qty(total_fact)}"

        self.lbl_fact_summary.config(text=text)

    # ══════════════════════════════════════════════════════
    #  FILL / INIT
    # ══════════════════════════════════════════════════════
    def _fill_init(self):
        # Тип работ
        iw = self.init.get("work_type_id")
        if iw is not None:
            for i, w in enumerate(self.work_types):
                if int(w["id"]) == int(iw):
                    self.cmb_wt.current(i)
                    break
            else:
                # work_type_id не найден в списке
                if self.work_types:
                    self.cmb_wt.current(0)
        elif self.work_types:
            self.cmb_wt.current(0)

        # Вид работ
        self.ent_name.insert(0, self.init.get("name", ""))

        # Ед. изм.
        iu = self.init.get("uom_code")
        if iu:
            found = False
            for i, u in enumerate(self.uoms):
                if u["code"] == iu:
                    self.cmb_uom.current(i + 1)  # +1 из-за "—"
                    found = True
                    break
            if not found:
                self.cmb_uom.current(0)
        else:
            self.cmb_uom.current(0)

        # Объём
        if self.init.get("plan_qty") is not None:
            self.ent_qty.insert(0, _fmt_qty(self.init["plan_qty"]))

        # Даты — используем безопасный _to_date
        d0 = _to_date(self.init.get("plan_start")) or _today()
        d1 = _to_date(self.init.get("plan_finish")) or _today()
        self.ent_start.insert(0, _fmt_date(d0))
        self.ent_finish.insert(0, _fmt_date(d1))

        # Статус
        ist = self.init.get("status", "planned")
        try:
            self.cmb_status.current(STATUS_LIST.index(ist))
        except ValueError:
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
                self._assignments = _EmployeeService.load_task_assignments(
                    task_id)
            except Exception:
                logger.exception("Load assignments error for task %s",
                                 task_id)
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
        except (ValueError, AttributeError):
            self.lbl_duration.config(text="—", fg=C["text3"])

    def _update_status_color(self):
        si = self.cmb_status.current()
        if 0 <= si < len(STATUS_LIST):
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
        if self.master and self.master.winfo_exists():
            pw = self.master.winfo_width()
            ph = self.master.winfo_height()
            px = self.master.winfo_rootx()
            py = self.master.winfo_rooty()
            x = px + (pw - w) // 2
            y = py + (ph - h) // 2
        else:
            # Центр экрана
            sw = self.winfo_screenwidth()
            sh = self.winfo_screenheight()
            x = (sw - w) // 2
            y = (sh - h) // 2
        self.geometry(f"+{max(0, x)}+{max(0, y)}")

    def _safe_destroy(self):
        """Безопасное закрытие — защита от двойного вызова."""
        if self._destroyed:
            return
        self._destroyed = True
        try:
            self.grab_release()
        except tk.TclError:
            pass
        try:
            self.destroy()
        except tk.TclError:
            pass

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

        if not self._emp_search_results:
            # Показываем подсказку если ничего не найдено
            self.emp_tree.insert("", "end", values=(
                "Не найдено", "", "", ""))
            return

        for emp in self._emp_search_results:
            self.emp_tree.insert("", "end", values=(
                emp.get("fio") or "",
                emp.get("tbn") or "",
                emp.get("position") or "",
                emp.get("department") or "",
            ))

    def _assign_selected(self):
        sel = self.emp_tree.selection()
        if not sel:
            messagebox.showinfo(
                "Назначение",
                "Выберите работника из результатов поиска.",
                parent=self,
            )
            return

        try:
            idx = self.emp_tree.index(sel[0])
        except tk.TclError:
            return

        if idx < 0 or idx >= len(self._emp_search_results):
            return

        emp = self._emp_search_results[idx]
        emp_id = emp.get("id")
        if not emp_id:
            return

        # Проверяем дубль
        for a in self._assignments:
            if int(a["employee_id"]) == int(emp_id):
                messagebox.showinfo(
                    "Назначение",
                    f"{emp.get('fio', '')} уже назначен на эту задачу.",
                    parent=self,
                )
                return

        # Роль
        ri = self.cmb_role.current()
        role_code = TASK_ROLE_LIST[ri] if 0 <= ri < len(TASK_ROLE_LIST) else "executor"

        self._assignments.append({
            "employee_id": int(emp_id),
            "fio": emp.get("fio") or "",
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
            messagebox.showinfo(
                "Назначение",
                "Выберите работника для снятия назначения.",
                parent=self,
            )
            return
        try:
            idx = self.assign_tree.index(sel[0])
        except tk.TclError:
            return

        if 0 <= idx < len(self._assignments):
            removed = self._assignments.pop(idx)
            logger.debug(
                "Removed assignment: %s",
                removed.get("fio", "?"),
            )
            self._render_assignments()

    def _render_assignments(self):
        self.assign_tree.delete(*self.assign_tree.get_children())
        for a in self._assignments:
            role_label = TASK_ROLES.get(
                a.get("role_in_task", ""), "?"
            )
            role_code = a.get("role_in_task", "executor")
            tags = (
                (role_code,)
                if role_code in ("foreman", "inspector")
                else ()
            )
            self.assign_tree.insert("", "end", values=(
                a.get("fio") or "",
                a.get("tbn") or "",
                a.get("position") or "",
                role_label,
                a.get("department") or "",
            ), tags=tags)
        self.lbl_assign_count.config(
            text=f"Назначено: {len(self._assignments)}"
        )

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
            if ui > 0 and (ui - 1) < len(self.uoms):
                uom = self.uoms[ui - 1]["code"]

            qty = _safe_float(self.ent_qty.get())

            ds = _parse_date(self.ent_start.get())
            df = _parse_date(self.ent_finish.get())
            if df < ds:
                raise ValueError("Дата окончания раньше даты начала")

            si = self.cmb_status.current()
            st = (
                STATUS_LIST[si]
                if 0 <= si < len(STATUS_LIST)
                else "planned"
            )

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
                "_facts": list(self._facts),
                "_facts_changed": bool(self._facts_changed),
            }
            self._safe_destroy()

        except ValueError as e:
            messagebox.showwarning("Работа ГПР", str(e), parent=self)
            # Переключаемся на первую вкладку где ошибка
            try:
                self.nb.select(0)
            except tk.TclError:
                pass
        except Exception as e:
            logger.exception("TaskEditDialogPro._on_ok unexpected error")
            messagebox.showerror(
                "Ошибка",
                f"Непредвиденная ошибка:\n{e}",
                parent=self,
            )

    def _on_cancel(self):
        self.result = None
        self._safe_destroy()


# ═══════════════════════════════════════════════════════════════
#  API — фабрика для вызова из GprPage
# ═══════════════════════════════════════════════════════════════
def open_task_dialog(
    parent,
    work_types,
    uoms,
    init=None,
    user_id=None,
) -> Optional[Dict[str, Any]]:
    """
    Открывает диалог, ждёт закрытия, возвращает result или None.
    """
    dlg = TaskEditDialogPro(
        parent, work_types, uoms,
        init=init, user_id=user_id,
    )
    parent.wait_window(dlg)
    return dlg.result
            
