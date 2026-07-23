# gpr_task_dialog.py — Профессиональный диалог добавления/редактирования работы ГПР
from __future__ import annotations

import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from psycopg2.extras import RealDictCursor, execute_values

from gpr_module import (
    _conn, _release, _today, _fmt_date, _parse_date,
    _fmt_qty, _safe_float, _to_date,
    C, STATUS_LABELS, STATUS_LIST, STATUS_COLORS,
)

logger = logging.getLogger(__name__)


def _fmt_dt(v) -> str:
    if isinstance(v, datetime):
        return v.strftime("%d.%m.%Y %H:%M")
    if isinstance(v, date):
        return v.strftime("%d.%m.%Y")
    if isinstance(v, str) and v.strip():
        return v.strip()
    return ""

class _GprWorkItemService:
    """Загрузка конкретных работ и актуальных норм ЗТР из справочника."""

    @staticmethod
    def load_by_work_type(
        work_type_id: int,
        norm_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """
        Возвращает активные работы выбранного типа.

        Для каждой работы подгружается активная норма, действующая
        на norm_date. Если нормы нет — работа всё равно доступна для выбора.
        """
        if not work_type_id:
            return []

        norm_date = norm_date or _today()

        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        wi.id,
                        wi.work_type_id,
                        COALESCE(wi.code, '') AS code,
                        wi.name,
                        wi.uom_code,
                        COALESCE(wi.note, '') AS note,

                        n.id AS labor_norm_id,
                        n.labor_hours_per_unit,
                        n.default_productivity_factor,
                        n.effective_from AS norm_effective_from,
                        n.effective_to AS norm_effective_to,
                        COALESCE(n.source_name, '') AS norm_source_name,
                        COALESCE(n.source_code, '') AS norm_source_code

                    FROM public.gpr_work_items wi

                    LEFT JOIN LATERAL (
                        SELECT
                            ln.id,
                            ln.labor_hours_per_unit,
                            ln.default_productivity_factor,
                            ln.effective_from,
                            ln.effective_to,
                            ln.source_name,
                            ln.source_code
                        FROM public.gpr_labor_norms ln
                        WHERE ln.work_item_id = wi.id
                          AND ln.is_active = true
                          AND ln.effective_from <= %s
                          AND (
                              ln.effective_to IS NULL
                              OR ln.effective_to >= %s
                          )
                        ORDER BY ln.effective_from DESC, ln.id DESC
                        LIMIT 1
                    ) n ON true

                    WHERE wi.work_type_id = %s
                      AND wi.is_active = true

                    ORDER BY wi.sort_order, wi.name
                    """,
                    (norm_date, norm_date, work_type_id),
                )
                return [dict(row) for row in cur.fetchall()]
        except Exception:
            logger.exception(
                "Ошибка загрузки работ типа %s",
                work_type_id,
            )
            raise
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
                    cur.execute(
                        """
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
                        """,
                        (f"%{query}%", f"%{query}%", f"%{query}%", limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT e.id, e.fio, e.tbn,
                               COALESCE(e.position, '') AS position,
                               COALESCE(d.name, '') AS department
                        FROM public.employees e
                        LEFT JOIN public.departments d ON d.id = e.department_id
                        WHERE e.is_fired = false
                        ORDER BY e.fio
                        LIMIT %s
                        """,
                        (limit,),
                    )
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
                cur.execute(
                    """
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
                    """,
                    (task_id,),
                )
                return [dict(r) for r in cur.fetchall()]
        except Exception:
            logger.exception("load_task_assignments error")
            raise
        finally:
            _release(conn)

    @staticmethod
    def save_task_assignments(
        task_id: int,
        assignments: List[Dict[str, Any]],
        user_id: Optional[int] = None,
    ) -> None:
        if not task_id:
            return

        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM public.gpr_task_assignments WHERE task_id = %s",
                    (task_id,),
                )
                for a in assignments:
                    emp_id = a.get("employee_id")
                    if not emp_id:
                        continue
                    cur.execute(
                        """
                        INSERT INTO public.gpr_task_assignments
                            (task_id, employee_id, role_in_task, note, assigned_by)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (task_id, employee_id) DO UPDATE
                            SET role_in_task = EXCLUDED.role_in_task,
                                note = EXCLUDED.note
                        """,
                        (
                            task_id,
                            int(emp_id),
                            a.get("role_in_task", "executor"),
                            a.get("note") or None,
                            user_id,
                        ),
                    )
        except Exception:
            logger.exception("save_task_assignments error")
            raise
        finally:
            _release(conn)


# ═══════════════════════════════════════════════════════════════
#  Сервис: факт выполнения
# ═══════════════════════════════════════════════════════════════
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
                           f.workers_count,
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
        """Сохраняет весь список фактов задачи."""
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
    
                    workers_count = _safe_float(f.get("workers_count"))
                    if workers_count is None or workers_count <= 0 or int(workers_count) != workers_count:
                        raise ValueError("Количество людей должно быть целым числом больше 0")
                    workers_count = int(workers_count)
    
                    comment = (f.get("comment") or "").strip() or None
    
                    cur.execute(
                        """
                        INSERT INTO public.gpr_task_facts
                            (task_id, fact_date, period_type, fact_qty, workers_count, comment, created_by)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (task_id, fact_date, period_type)
                        DO UPDATE SET
                            fact_qty = EXCLUDED.fact_qty,
                            workers_count = EXCLUDED.workers_count,
                            comment = EXCLUDED.comment
                        RETURNING id
                        """,
                        (
                            task_id,
                            fact_date,
                            period_type,
                            fact_qty,
                            workers_count,
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
#  Константы
# ═══════════════════════════════════════════════════════════════
TASK_ROLES = {
    "executor": "Исполнитель",
    "foreman": "Бригадир",
    "inspector": "Контролёр",
}
TASK_ROLE_LIST = list(TASK_ROLES.keys())
TASK_ROLE_LABELS = list(TASK_ROLES.values())
TASK_ROLE_BY_LABEL = {v: k for k, v in TASK_ROLES.items()}

FACT_PERIODS = {
    "day": "За день",
    "week": "За неделю",
}
FACT_PERIOD_LIST = list(FACT_PERIODS.keys())
FACT_PERIOD_LABELS = [FACT_PERIODS[x] for x in FACT_PERIOD_LIST]
FACT_PERIOD_FROM_LABEL = {v: k for k, v in FACT_PERIODS.items()}


# ═══════════════════════════════════════════════════════════════
#  Малый диалог редактирования назначения
# ═══════════════════════════════════════════════════════════════
class _AssignmentEditDialog(simpledialog.Dialog):
    def __init__(self, parent, init_role: str = "executor", init_note: str = ""):
        self.init_role = init_role
        self.init_note = init_note or ""
        self.result: Optional[Dict[str, str]] = None
        super().__init__(parent, title="Параметры назначения")

    def body(self, master):
        tk.Label(master, text="Роль:").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=4)
        self.cmb_role = ttk.Combobox(
            master,
            state="readonly",
            width=20,
            values=TASK_ROLE_LABELS,
        )
        self.cmb_role.grid(row=0, column=1, sticky="w", pady=4)

        role_label = TASK_ROLES.get(self.init_role, TASK_ROLES["executor"])
        try:
            self.cmb_role.current(TASK_ROLE_LABELS.index(role_label))
        except ValueError:
            self.cmb_role.current(0)

        tk.Label(master, text="Примечание:").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=4)
        self.ent_note = ttk.Entry(master, width=42)
        self.ent_note.grid(row=1, column=1, sticky="ew", pady=4)
        self.ent_note.insert(0, self.init_note)

        master.grid_columnconfigure(1, weight=1)
        return self.cmb_role

    def validate(self):
        label = (self.cmb_role.get() or "").strip()
        if not label:
            messagebox.showwarning("Назначение", "Выберите роль.", parent=self)
            return False
        self._role_code = TASK_ROLE_BY_LABEL.get(label, "executor")
        self._note = (self.ent_note.get() or "").strip()
        return True

    def apply(self):
        self.result = {
            "role_in_task": self._role_code,
            "note": self._note,
        }

class _WorkItemsMultiSelectDialog(tk.Toplevel):
    """
    Выбор одной или нескольких конкретных работ из gpr_work_items.

    Результат:
        self.result = List[Dict[str, Any]] или None при отмене.
    """

    def __init__(
        self,
        parent,
        work_type: Dict[str, Any],
        norm_date: Optional[date] = None,
        preselected_ids: Optional[List[int]] = None,
        allow_multiple: bool = True,
    ):
        super().__init__(parent)
        self.transient(parent)

        self.work_type = work_type
        self.norm_date = norm_date or _today()
        self.allow_multiple = allow_multiple

        self.result: Optional[List[Dict[str, Any]]] = None
        self._destroyed = False

        self._all_items: List[Dict[str, Any]] = []
        self._visible_items: List[Dict[str, Any]] = []
        self._checked_ids = {
            int(x)
            for x in (preselected_ids or [])
            if x is not None
        }

        self.var_search = tk.StringVar()

        self.title(
            f"Выбор работ: {work_type.get('name') or ''}"
        )
        self.minsize(820, 520)
        self.geometry("900x600")
        self.configure(bg=C["bg"])

        self._build_ui()
        self._load_items()

        self.grab_set()
        self.after(20, self._center)
        self.after(50, self.ent_search.focus_set())
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

    def _build_ui(self):
        hdr = tk.Frame(self, bg=C["accent"], pady=7)
        hdr.pack(fill="x")

        tk.Label(
            hdr,
            text="☑  Выбор работ из справочника",
            font=("Segoe UI", 11, "bold"),
            bg=C["accent"],
            fg="white",
            padx=12,
        ).pack(side="left")

        top = tk.Frame(self, bg=C["panel"], padx=12, pady=10)
        top.pack(fill="x", padx=10, pady=(10, 4))

        tk.Label(
            top,
            text=(
                f"Тип работ: {self.work_type.get('name') or '—'}\n"
                f"Дата подбора нормы: {_fmt_date(self.norm_date)}"
            ),
            bg=C["panel"],
            fg=C["text2"],
            font=("Segoe UI", 9),
            justify="left",
            anchor="w",
        ).pack(side="left")

        search_frame = tk.Frame(self, bg=C["panel"], padx=12, pady=6)
        search_frame.pack(fill="x", padx=10, pady=(0, 4))

        tk.Label(
            search_frame,
            text="Поиск:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).pack(side="left")

        self.ent_search = ttk.Entry(
            search_frame,
            textvariable=self.var_search,
            width=42,
        )
        self.ent_search.pack(side="left", padx=(6, 10))
        self.ent_search.bind(
            "<KeyRelease>",
            lambda _e: self._apply_filter(),
        )

        ttk.Button(
            search_frame,
            text="☑ Выбрать все",
            command=self._check_all_visible,
        ).pack(side="left", padx=2)

        ttk.Button(
            search_frame,
            text="☐ Снять все",
            command=self._uncheck_all_visible,
        ).pack(side="left", padx=2)

        self.lbl_count = tk.Label(
            search_frame,
            text="",
            bg=C["panel"],
            fg=C["text2"],
            font=("Segoe UI", 8),
        )
        self.lbl_count.pack(side="right")

        table_frame = tk.Frame(self, bg=C["panel"])
        table_frame.pack(
            fill="both",
            expand=True,
            padx=10,
            pady=(0, 8),
        )

        cols = ("selected", "code", "name", "uom", "norm", "source")

        self.tree = ttk.Treeview(
            table_frame,
            columns=cols,
            show="headings",
            selectmode="browse",
        )

        columns = [
            ("selected", "Выбор", 65, "center"),
            ("code", "Код", 100, "w"),
            ("name", "Наименование работы", 390, "w"),
            ("uom", "Ед.", 65, "center"),
            ("norm", "ЗТР", 95, "e"),
            ("source", "Источник нормы", 170, "w"),
        ]

        for code, title, width, anchor in columns:
            self.tree.heading(code, text=title)
            self.tree.column(code, width=width, anchor=anchor)

        vsb = ttk.Scrollbar(
            table_frame,
            orient="vertical",
            command=self.tree.yview,
        )
        hsb = ttk.Scrollbar(
            table_frame,
            orient="horizontal",
            command=self.tree.xview,
        )

        self.tree.configure(
            yscrollcommand=vsb.set,
            xscrollcommand=hsb.set,
        )

        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")

        self.tree.tag_configure(
            "has_norm",
            background="#e8f5e9",
        )
        self.tree.tag_configure(
            "no_norm",
            background="#fff3e0",
        )
        self.tree.tag_configure(
            "checked",
            font=("Segoe UI", 9, "bold"),
        )

        self.tree.bind("<Button-1>", self._on_tree_click)
        self.tree.bind("<Double-1>", self._on_tree_double_click)
        self.tree.bind("<space>", self._toggle_selected_tree_row)
        self.tree.bind("<Return>", self._toggle_selected_tree_row)

        hint = tk.Label(
            self,
            text=(
                "Нажмите на строку, пробел или двойной клик, чтобы поставить / снять отметку. "
                "Работы без нормы можно добавить, но расчёт ЗТР для них будет недоступен."
            ),
            bg=C["bg"],
            fg=C["text3"],
            font=("Segoe UI", 8),
            justify="left",
            anchor="w",
        )
        hint.pack(fill="x", padx=14, pady=(0, 5))

        bot = tk.Frame(self, bg=C["bg"], pady=8)
        bot.pack(fill="x")

        ttk.Button(
            bot,
            text="Отмена",
            command=self._on_cancel,
        ).pack(side="right", padx=(4, 14))

        self.btn_ok = tk.Button(
            bot,
            text="✅ Добавить выбранные",
            command=self._on_ok,
            bg=C["btn_bg"],
            fg=C["btn_fg"],
            activebackground="#0d47a1",
            activeforeground="white",
            relief="flat",
            cursor="hand2",
            padx=16,
            pady=5,
        )
        self.btn_ok.pack(side="right", padx=4)

    def _load_items(self):
        try:
            self._all_items = _GprWorkItemService.load_by_work_type(
                int(self.work_type["id"]),
                self.norm_date,
            )
        except Exception as exc:
            messagebox.showerror(
                "Выбор работ",
                f"Не удалось загрузить работы:\n{exc}",
                parent=self,
            )
            self._all_items = []

        self._apply_filter()

    def _apply_filter(self):
        query = (self.var_search.get() or "").strip().casefold()

        if query:
            self._visible_items = [
                item
                for item in self._all_items
                if query in (item.get("name") or "").casefold()
                or query in (item.get("code") or "").casefold()
                or query in (item.get("uom_code") or "").casefold()
            ]
        else:
            self._visible_items = list(self._all_items)

        self._render()

    def _render(self):
        self.tree.delete(*self.tree.get_children())

        for item in self._visible_items:
            item_id = int(item["id"])
            checked = item_id in self._checked_ids

            marker = "☑" if checked else "☐"
            norm = item.get("labor_hours_per_unit")

            source_parts = [
                item.get("norm_source_name") or "",
                item.get("norm_source_code") or "",
            ]
            source = " | ".join(
                x for x in source_parts if x.strip()
            )

            tags = ["has_norm" if norm is not None else "no_norm"]
            if checked:
                tags.append("checked")

            self.tree.insert(
                "",
                "end",
                iid=str(item_id),
                values=(
                    marker,
                    item.get("code") or "",
                    item.get("name") or "",
                    item.get("uom_code") or "",
                    _fmt_qty(norm),
                    source,
                ),
                tags=tuple(tags),
            )

        selected_count = len(self._checked_ids)
        self.lbl_count.config(
            text=(
                f"Показано: {len(self._visible_items)}  |  "
                f"Выбрано: {selected_count}"
            )
        )

        self.btn_ok.config(
            text=(
                f"✅ Добавить выбранные ({selected_count})"
                if selected_count
                else "✅ Добавить выбранные"
            )
        )

    def _toggle_item(self, item_id: int):
        if not self.allow_multiple:
            self._checked_ids.clear()

        if item_id in self._checked_ids:
            self._checked_ids.remove(item_id)
        else:
            self._checked_ids.add(item_id)

        self._render()

    def _on_tree_click(self, event):
        region = self.tree.identify_region(event.x, event.y)
        if region not in ("cell", "tree"):
            return

        iid = self.tree.identify_row(event.y)
        if not iid:
            return

        try:
            self._toggle_item(int(iid))
        except ValueError:
            return

    def _on_tree_double_click(self, event):
        iid = self.tree.identify_row(event.y)
        if not iid:
            return

        try:
            self._toggle_item(int(iid))
        except ValueError:
            return

    def _toggle_selected_tree_row(self, _event=None):
        selection = self.tree.selection()
        if not selection:
            return "break"

        try:
            self._toggle_item(int(selection[0]))
        except ValueError:
            pass

        return "break"

    def _check_all_visible(self):
        if not self.allow_multiple:
            return

        for item in self._visible_items:
            self._checked_ids.add(int(item["id"]))

        self._render()

    def _uncheck_all_visible(self):
        for item in self._visible_items:
            self._checked_ids.discard(int(item["id"]))

        self._render()

    def _on_ok(self):
        if not self._checked_ids:
            messagebox.showwarning(
                "Выбор работ",
                "Отметьте хотя бы одну работу.",
                parent=self,
            )
            return

        selected = [
            item
            for item in self._all_items
            if int(item["id"]) in self._checked_ids
        ]

        if not self.allow_multiple and len(selected) > 1:
            selected = selected[:1]

        without_norm = [
            item
            for item in selected
            if not item.get("labor_norm_id")
            or item.get("labor_hours_per_unit") is None
        ]

        if without_norm:
            names = "\n".join(
                f"• {item.get('name') or 'Без названия'}"
                for item in without_norm[:10]
            )

            if len(without_norm) > 10:
                names += f"\n• ... ещё {len(without_norm) - 10}"

            messagebox.showwarning(
                "Не найдены нормы ЗТР",
                (
                    f"Для выбранных работ не найдены действующие нормы "
                    f"на дату {_fmt_date(self.norm_date)}:\n\n"
                    f"{names}\n\n"
                    "Проверьте даты действия норм в справочнике "
                    "или дату начала работ в ГПР."
                ),
                parent=self,
            )
            return

        self.result = selected
        self._safe_destroy()

    def _on_cancel(self):
        self.result = None
        self._safe_destroy()

    def _safe_destroy(self):
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

    def _center(self):
        self.update_idletasks()

        width = self.winfo_width()
        height = self.winfo_height()

        if self.master and self.master.winfo_exists():
            parent_width = self.master.winfo_width()
            parent_height = self.master.winfo_height()
            parent_x = self.master.winfo_rootx()
            parent_y = self.master.winfo_rooty()

            x = parent_x + (parent_width - width) // 2
            y = parent_y + (parent_height - height) // 2
        else:
            x = (self.winfo_screenwidth() - width) // 2
            y = (self.winfo_screenheight() - height) // 2

        self.geometry(f"+{max(0, x)}+{max(0, y)}")

# ═══════════════════════════════════════════════════════════════
#  Профессиональный диалог работы ГПР
# ═══════════════════════════════════════════════════════════════
class TaskEditDialogPro(tk.Toplevel):
    """
    Диалог с вкладками:
      1) Основные данные
      2) Назначения работников
      3) Факт выполнения
    """

    def __init__(
        self,
        parent,
        work_types: List[Dict[str, Any]],
        uoms: List[Dict[str, Any]],
        init: Optional[Dict[str, Any]] = None,
        user_id: Optional[int] = None,
    ):
        super().__init__(parent)
        self.transient(parent)

        self.work_types = work_types
        self.uoms = uoms
        self.init = init or {}
        self.user_id = user_id
        self.result: Optional[Dict[str, Any]] = None

        self._assignments: List[Dict[str, Any]] = []
        self._emp_search_results: List[Dict[str, Any]] = []
        self._facts: List[Dict[str, Any]] = []

        self._fact_edit_idx: Optional[int] = None
        self._has_fact_tab = False
        self._has_assign_tab = False

        self._destroyed = False
        self._dirty = False
        self._search_after_id: Optional[str] = None

        self._selected_work_items: List[Dict[str, Any]] = []
        self._bulk_mode = False
        self._suspend_work_type_dialog = False

        task_name = self.init.get("name", "")
        if task_name:
            self.title(f"✏️ Работа: {task_name[:60]}")
        else:
            self.title("➕ Новая работа ГПР")

        self.minsize(760, 640)
        self.resizable(True, True)

        self._build_ui()
        self._bind_change_tracking()

        self._fill_init()
        self._load_assignments()
        self._load_facts()
        self._refresh_overview()

        self._dirty = False
        self._update_window_title()

        self.grab_set()
        self.after(10, self._center)
        self.after(30, lambda: self.cmb_wt.focus_set())
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

    # ══════════════════════════════════════════════════════
    #  BUILD UI
    # ══════════════════════════════════════════════════════
    def _build_ui(self):
        hdr = tk.Frame(self, bg=C["accent"], pady=6)
        hdr.pack(fill="x")

        tk.Label(
            hdr,
            text="📋  Карточка работы ГПР",
            font=("Segoe UI", 11, "bold"),
            bg=C["accent"],
            fg="white",
            padx=10,
        ).pack(side="left")

        self.lbl_head_meta = tk.Label(
            hdr,
            text="",
            font=("Segoe UI", 8),
            bg=C["accent"],
            fg="#bbdefb",
            padx=10,
        )
        self.lbl_head_meta.pack(side="right")

        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=10, pady=(8, 4))

        tab_main = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_main, text="  📝 Основные данные  ")
        self._build_main_tab(tab_main)

        tab_assign = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_assign, text="  👷 Назначения работников  ")
        self._build_assign_tab(tab_assign)

        tab_fact = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_fact, text="  📈 Факт выполнения  ")
        self._build_fact_tab(tab_fact)

        bot = tk.Frame(self, bg=C["bg"], pady=8)
        bot.pack(fill="x")

        self.lbl_info = tk.Label(
            bot,
            text="",
            font=("Segoe UI", 8),
            fg=C["text3"],
            bg=C["bg"],
        )
        self.lbl_info.pack(side="left", padx=16)

        self.btn_ok = tk.Button(
            bot,
            text="✅  Сохранить",
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
        self.btn_ok.pack(side="right", padx=(0, 16))
        self.btn_ok.bind("<Enter>", lambda _e: self.btn_ok.config(bg="#0d47a1"))
        self.btn_ok.bind("<Leave>", lambda _e: self.btn_ok.config(bg=C["btn_bg"]))

        self.btn_cancel = tk.Button(
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
        self.btn_cancel.pack(side="right", padx=(0, 8))

        self.bind("<Escape>", lambda _e: self._on_cancel())
        self.bind("<Control-Return>", lambda _e: self._on_ok())
        self.bind("<Control-s>", lambda _e: self._on_ok())

    def _build_main_tab(self, parent):
        parent.grid_columnconfigure(0, weight=1)

        # ── Выбор работ ───────────────────────────────────
        grp_work = tk.LabelFrame(
            parent,
            text=" 1. Выбор работ ",
            font=("Segoe UI", 10, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=12,
            pady=10,
        )
        grp_work.pack(fill="x", padx=12, pady=(12, 6))
        grp_work.grid_columnconfigure(1, weight=1)

        tk.Label(
            grp_work,
            text="Тип работ:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).grid(
            row=0,
            column=0,
            sticky="e",
            padx=(0, 8),
            pady=4,
        )

        wt_vals = [w["name"] for w in self.work_types]

        self.cmb_wt = ttk.Combobox(
            grp_work,
            state="readonly",
            width=52,
            values=wt_vals,
            font=("Segoe UI", 9),
        )
        self.cmb_wt.grid(
            row=0,
            column=1,
            sticky="ew",
            pady=4,
        )

        self.btn_select_work_items = ttk.Button(
            grp_work,
            text="☑ Выбрать работы из справочника",
            command=self._open_work_items_selector,
        )
        self.btn_select_work_items.grid(
            row=0,
            column=2,
            sticky="w",
            padx=(8, 0),
            pady=4,
        )

        tk.Label(
            grp_work,
            text="Выбрано:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).grid(
            row=1,
            column=0,
            sticky="ne",
            padx=(0, 8),
            pady=(8, 4),
        )

        self.lbl_selected_works = tk.Label(
            grp_work,
            text="Сначала выберите тип работ и нажмите «Выбрать работы».",
            bg=C["panel"],
            fg=C["text3"],
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            wraplength=650,
        )
        self.lbl_selected_works.grid(
            row=1,
            column=1,
            columnspan=2,
            sticky="ew",
            pady=(8, 4),
        )

        # Техническое поле для совместимости существующего кода.
        # Пользователь его не видит и изменить не может.
        self.ent_name = ttk.Entry(grp_work)
        self.ent_name.grid_remove()

        # ── Норма ─────────────────────────────────────────
        grp_norm = tk.LabelFrame(
            parent,
            text=" 2. Норма и плановый объём ",
            font=("Segoe UI", 10, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=12,
            pady=10,
        )
        grp_norm.pack(fill="x", padx=12, pady=6)
        grp_norm.grid_columnconfigure(1, weight=1)

        tk.Label(
            grp_norm,
            text="Единица:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).grid(
            row=0,
            column=0,
            sticky="e",
            padx=(0, 8),
            pady=4,
        )

        uom_vals = ["—"] + [
            f"{u['code']} — {u['name']}"
            for u in self.uoms
        ]

        self.cmb_uom = ttk.Combobox(
            grp_norm,
            state="readonly",
            width=20,
            values=uom_vals,
            font=("Segoe UI", 9),
        )
        self.cmb_uom.grid(
            row=0,
            column=1,
            sticky="w",
            pady=4,
        )

        tk.Label(
            grp_norm,
            text="Норма ЗТР:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).grid(
            row=0,
            column=2,
            sticky="e",
            padx=(24, 8),
            pady=4,
        )

        self.lbl_labor_norm = tk.Label(
            grp_norm,
            text="—",
            bg=C["panel"],
            fg=C["text3"],
            font=("Segoe UI", 10, "bold"),
            anchor="w",
        )
        self.lbl_labor_norm.grid(
            row=0,
            column=3,
            sticky="w",
            pady=4,
        )

        tk.Label(
            grp_norm,
            text="Плановый объём:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).grid(
            row=1,
            column=0,
            sticky="e",
            padx=(0, 8),
            pady=8,
        )

        self.ent_qty = ttk.Entry(
            grp_norm,
            width=18,
            font=("Segoe UI", 9),
        )
        self.ent_qty.grid(
            row=1,
            column=1,
            sticky="w",
            pady=8,
        )

        tk.Label(
            grp_norm,
            text="Плановая трудоёмкость:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).grid(
            row=1,
            column=2,
            sticky="e",
            padx=(24, 8),
            pady=8,
        )

        self.lbl_total_labor = tk.Label(
            grp_norm,
            text="—",
            bg=C["panel"],
            fg=C["accent"],
            font=("Segoe UI", 10, "bold"),
            anchor="w",
        )
        self.lbl_total_labor.grid(
            row=1,
            column=3,
            sticky="w",
            pady=8,
        )

        self.lbl_norm_info = tk.Label(
            grp_norm,
            text="Норма определяется на дату начала работы.",
            bg=C["panel"],
            fg=C["text3"],
            font=("Segoe UI", 8),
            anchor="w",
        )
        self.lbl_norm_info.grid(
            row=2,
            column=1,
            columnspan=3,
            sticky="w",
        )

        # ── Сроки ─────────────────────────────────────────
        grp_dates = tk.LabelFrame(
            parent,
            text=" 3. Сроки ",
            font=("Segoe UI", 10, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=12,
            pady=10,
        )
        grp_dates.pack(fill="x", padx=12, pady=6)

        tk.Label(
            grp_dates,
            text="Начало:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).pack(side="left")

        self.ent_start = ttk.Entry(
            grp_dates,
            width=12,
            font=("Segoe UI", 9),
        )
        self.ent_start.pack(side="left", padx=(6, 6))

        ttk.Button(
            grp_dates,
            text="Сегодня",
            command=self._set_start_today,
        ).pack(side="left", padx=(0, 18))

        tk.Label(
            grp_dates,
            text="Окончание:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).pack(side="left")

        self.ent_finish = ttk.Entry(
            grp_dates,
            width=12,
            font=("Segoe UI", 9),
        )
        self.ent_finish.pack(side="left", padx=(6, 6))

        ttk.Button(
            grp_dates,
            text="= началу",
            command=self._copy_start_to_finish,
        ).pack(side="left", padx=(0, 18))

        tk.Label(
            grp_dates,
            text="Длительность:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).pack(side="left")

        self.lbl_duration = tk.Label(
            grp_dates,
            text="—",
            bg=C["panel"],
            fg=C["accent"],
            font=("Segoe UI", 9, "bold"),
        )
        self.lbl_duration.pack(side="left", padx=(6, 0))

        # ── Статус ────────────────────────────────────────
        grp_status = tk.LabelFrame(
            parent,
            text=" 4. Статус ",
            font=("Segoe UI", 10, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=12,
            pady=10,
        )
        grp_status.pack(fill="x", padx=12, pady=(6, 10))

        tk.Label(
            grp_status,
            text="Статус:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).pack(side="left")

        st_vals = [
            STATUS_LABELS.get(status, status)
            for status in STATUS_LIST
        ]

        self.cmb_status = ttk.Combobox(
            grp_status,
            state="readonly",
            width=20,
            values=st_vals,
            font=("Segoe UI", 9),
        )
        self.cmb_status.pack(side="left", padx=(6, 8))

        self.cv_status = tk.Canvas(
            grp_status,
            width=16,
            height=16,
            bg=C["panel"],
            highlightthickness=0,
        )
        self.cv_status.pack(side="left", padx=(0, 16))

        ttk.Button(
            grp_status,
            text="Статус по факту",
            command=self._apply_status_from_fact,
        ).pack(side="left", padx=(0, 18))

        self.var_milestone = tk.BooleanVar(value=False)

        ttk.Checkbutton(
            grp_status,
            text="Веха",
            variable=self.var_milestone,
        ).pack(side="left")
        
    def _build_assign_tab(self, parent):
        task_id = self.init.get("id")
        if not task_id:
            self._has_assign_tab = False
            tk.Label(
                parent,
                text=(
                    "Назначения работников доступны только после первого сохранения задачи.\n"
                    "Это исключает потерю назначений у ещё не созданной записи."
                ),
                bg=C["panel"],
                fg=C["text2"],
                font=("Segoe UI", 10),
                justify="left",
                padx=20,
                pady=20,
            ).pack(anchor="nw")
            return

        self._has_assign_tab = True

        search_frame = tk.LabelFrame(
            parent,
            text=" 🔍 Поиск работника ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=10,
            pady=6,
        )
        search_frame.pack(fill="x", padx=12, pady=(10, 4))

        sf = tk.Frame(search_frame, bg=C["panel"])
        sf.pack(fill="x")

        tk.Label(
            sf,
            text="ФИО / ТБН / Должность:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).pack(side="left")

        self.var_emp_search = tk.StringVar()
        self.ent_emp_search = ttk.Entry(
            sf,
            textvariable=self.var_emp_search,
            width=32,
            font=("Segoe UI", 9),
        )
        self.ent_emp_search.pack(side="left", padx=(6, 8))
        self.ent_emp_search.bind("<Return>", lambda _e: self._search_employees())
        self.ent_emp_search.bind("<KeyRelease>", lambda _e: self._schedule_emp_search())

        ttk.Button(sf, text="Найти", command=self._search_employees).pack(side="left", padx=2)

        tk.Label(sf, text="  Роль:", bg=C["panel"], font=("Segoe UI", 9)).pack(
            side="left", padx=(12, 4)
        )
        self.cmb_role = ttk.Combobox(
            sf,
            state="readonly",
            width=14,
            values=TASK_ROLE_LABELS,
            font=("Segoe UI", 9),
        )
        self.cmb_role.pack(side="left")
        self.cmb_role.current(0)

        ttk.Button(
            sf,
            text="➕ Назначить выбранного",
            command=self._assign_selected,
        ).pack(side="left", padx=(12, 0))

        self.lbl_emp_found = tk.Label(
            search_frame,
            text="",
            bg=C["panel"],
            fg=C["text2"],
            font=("Segoe UI", 8),
        )
        self.lbl_emp_found.pack(anchor="e", pady=(4, 0))

        src_frame = tk.Frame(search_frame, bg=C["panel"])
        src_frame.pack(fill="x", pady=(6, 0))

        cols_s = ("fio", "tbn", "position", "department")
        self.emp_tree = ttk.Treeview(
            src_frame,
            columns=cols_s,
            show="headings",
            selectmode="browse",
            height=6,
        )
        for c, t, w in [
            ("fio", "ФИО", 220),
            ("tbn", "ТБН", 80),
            ("position", "Должность", 180),
            ("department", "Подразделение", 180),
        ]:
            self.emp_tree.heading(c, text=t)
            self.emp_tree.column(c, width=w, anchor="w")

        vsb_e = ttk.Scrollbar(src_frame, orient="vertical", command=self.emp_tree.yview)
        self.emp_tree.configure(yscrollcommand=vsb_e.set)
        self.emp_tree.pack(side="left", fill="x", expand=True)
        vsb_e.pack(side="right", fill="y")
        self.emp_tree.bind("<Double-1>", lambda _e: self._assign_selected())

        assign_frame = tk.LabelFrame(
            parent,
            text=" 👷 Назначенные работники ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=10,
            pady=6,
        )
        assign_frame.pack(fill="both", expand=True, padx=12, pady=(4, 8))

        bar = tk.Frame(assign_frame, bg=C["panel"])
        bar.pack(fill="x")

        ttk.Button(bar, text="✏️ Изменить", command=self._edit_assignment_selected).pack(
            side="left", padx=2
        )
        ttk.Button(bar, text="🗑 Снять назначение", command=self._remove_assignment).pack(
            side="left", padx=2
        )

        self.lbl_assign_count = tk.Label(
            bar,
            text="",
            bg=C["panel"],
            font=("Segoe UI", 8),
            fg=C["text2"],
        )
        self.lbl_assign_count.pack(side="right", padx=8)

        cols_a = ("fio", "tbn", "position", "role", "department", "note")
        self.assign_tree = ttk.Treeview(
            assign_frame,
            columns=cols_a,
            show="headings",
            selectmode="browse",
            height=8,
        )
        for c, t, w in [
            ("fio", "ФИО", 200),
            ("tbn", "ТБН", 80),
            ("position", "Должность", 140),
            ("role", "Роль", 100),
            ("department", "Подразделение", 140),
            ("note", "Примечание", 180),
        ]:
            self.assign_tree.heading(c, text=t)
            self.assign_tree.column(c, width=w, anchor="w")

        vsb_a = ttk.Scrollbar(assign_frame, orient="vertical", command=self.assign_tree.yview)
        self.assign_tree.configure(yscrollcommand=vsb_a.set)
        self.assign_tree.pack(side="left", fill="both", expand=True, pady=(4, 0))
        vsb_a.pack(side="right", fill="y", pady=(4, 0))

        self.assign_tree.tag_configure("foreman", background="#e3f2fd")
        self.assign_tree.tag_configure("inspector", background="#fff3e0")
        self.assign_tree.bind("<Double-1>", lambda _e: self._edit_assignment_selected())

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
            bg=C["panel"],
            fg=C["accent"],
            padx=10,
            pady=8,
        )
        form.pack(fill="x", padx=12, pady=(10, 4))

        row1 = tk.Frame(form, bg=C["panel"])
        row1.pack(fill="x", pady=2)

        tk.Label(row1, text="Дата:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.ent_fact_date = ttk.Entry(row1, width=12, font=("Segoe UI", 9))
        self.ent_fact_date.pack(side="left", padx=(4, 10))
        self.ent_fact_date.insert(0, _fmt_date(_today()))

        ttk.Button(row1, text="Сегодня", command=self._fact_set_today).pack(
            side="left", padx=(0, 12)
        )

        tk.Label(row1, text="Период:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.cmb_fact_period = ttk.Combobox(
            row1,
            state="readonly",
            width=14,
            values=FACT_PERIOD_LABELS,
            font=("Segoe UI", 9),
        )
        self.cmb_fact_period.pack(side="left", padx=(4, 12))
        self.cmb_fact_period.current(0)

        tk.Label(row1, text="Объём:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.ent_fact_qty = ttk.Entry(row1, width=14, font=("Segoe UI", 9))
        self.ent_fact_qty.pack(side="left", padx=(4, 12))
        
        tk.Label(row1, text="Людей:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.ent_fact_workers = ttk.Entry(row1, width=10, font=("Segoe UI", 9))
        self.ent_fact_workers.pack(side="left", padx=(4, 12))
        
        ttk.Button(row1, text="Остаток", command=self._fact_fill_remaining).pack(side="left")

        row2 = tk.Frame(form, bg=C["panel"])
        row2.pack(fill="x", pady=(8, 2))

        tk.Label(row2, text="Комментарий:", bg=C["panel"], font=("Segoe UI", 9)).pack(
            side="left"
        )
        self.ent_fact_comment = ttk.Entry(row2, width=60, font=("Segoe UI", 9))
        self.ent_fact_comment.pack(side="left", padx=(4, 8), fill="x", expand=True)

        btns = tk.Frame(form, bg=C["panel"])
        btns.pack(fill="x", pady=(8, 0))

        self.btn_fact_add = ttk.Button(
            btns,
            text="Добавить факт",
            command=self._fact_add_or_update,
        )
        self.btn_fact_add.pack(side="left", padx=2)

        ttk.Button(btns, text="Очистить", command=self._fact_clear_form).pack(
            side="left", padx=2
        )
        ttk.Button(btns, text="Удалить выбранный", command=self._fact_remove_selected).pack(
            side="left", padx=12
        )

        self.lbl_fact_summary = tk.Label(
            btns,
            text="",
            bg=C["panel"],
            fg=C["text2"],
            font=("Segoe UI", 8),
        )
        self.lbl_fact_summary.pack(side="right", padx=4)

        list_frame = tk.LabelFrame(
            parent,
            text=" 📚 Внесённый факт ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            padx=10,
            pady=6,
        )
        list_frame.pack(fill="both", expand=True, padx=12, pady=(4, 8))

        cols = ("date", "period", "qty", "workers", "comment", "creator")
        self.fact_tree = ttk.Treeview(
            list_frame,
            columns=cols,
            show="headings",
            selectmode="browse",
            height=8,
        )

        for c, t, w, a in [
            ("date", "Дата", 90, "center"),
            ("period", "Период", 110, "center"),
            ("qty", "Объём", 90, "e"),
            ("workers", "Людей", 70, "center"),
            ("comment", "Комментарий", 220, "w"),
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

    # ══════════════════════════════════════════════════════
    #  BINDINGS / DIRTY
    # ══════════════════════════════════════════════════════
    def _bind_change_tracking(self):
        for widget in (
            self.ent_qty,
            self.ent_start,
            self.ent_finish,
        ):
            widget.bind(
                "<KeyRelease>",
                lambda _e: self._on_main_field_changed(),
                add="+",
            )
            widget.bind(
                "<FocusOut>",
                lambda _e: self._on_main_field_changed(),
                add="+",
            )

        for widget in (
            self.cmb_wt,
            self.cmb_uom,
            self.cmb_status,
        ):
            widget.bind(
                "<<ComboboxSelected>>",
                lambda _e: self._on_main_field_changed(),
                add="+",
            )

        self.cmb_wt.bind(
            "<<ComboboxSelected>>",
            self._on_work_type_selected,
            add="+",
        )

        self.cmb_status.bind(
            "<<ComboboxSelected>>",
            lambda _e: self._update_status_color(),
            add="+",
        )

        self.var_milestone.trace_add(
            "write",
            lambda *_: self._mark_dirty(),
        )

    def _mark_dirty(self):
        self._dirty = True
        self._update_window_title()

    def _update_window_title(self):
        task_name = (self.ent_name.get().strip() if hasattr(self, "ent_name") else "") or self.init.get("name", "")
        base = f"✏️ Работа: {task_name[:60]}" if task_name else "➕ Новая работа ГПР"
        self.title(("* " if self._dirty else "") + base)

    def _on_main_field_changed(self):
        self._update_duration()
        self._update_status_color()
        self._refresh_labor_info()
        self._refresh_overview()
        self._mark_dirty()

    def _on_work_type_selected(self, _event=None):
        """
        При смене типа работ у новой задачи автоматически открываем
        окно выбора конкретных работ из справочника.
        """
        if self._suspend_work_type_dialog:
            return

        # При редактировании уже существующей задачи не создаём
        # несколько новых задач автоматически.
        if self.init.get("id"):
            return

        self._selected_work_items = []
        self._bulk_mode = False

        self._set_name_entry_state("normal")
        self.ent_name.delete(0, "end")
        self.lbl_selected_works.config(text="")

        self._open_work_items_selector()

    def _selected_work_type(self) -> Optional[Dict[str, Any]]:
        index = self.cmb_wt.current()

        if 0 <= index < len(self.work_types):
            return self.work_types[index]

        return None

    def _get_norm_date(self) -> date:
        """
        Дата, на которую подбирается норма.
        Используем дату начала задачи; если она пока не заполнена —
        сегодняшнюю дату.
        """
        try:
            return _parse_date(self.ent_start.get())
        except Exception:
            return _today()

    def _open_work_items_selector(self):
        work_type = self._selected_work_type()

        if not work_type:
            messagebox.showwarning(
                "Работы ГПР",
                "Сначала выберите тип работ.",
                parent=self,
            )
            return

        # Редактирование существующей задачи: только один выбор.
        is_edit_mode = bool(self.init.get("id"))

        preselected_ids = [
            int(item["id"])
            for item in self._selected_work_items
            if item.get("id")
        ]

        if not preselected_ids and self.init.get("work_item_id"):
            preselected_ids = [int(self.init["work_item_id"])]

        dlg = _WorkItemsMultiSelectDialog(
            self,
            work_type=work_type,
            norm_date=self._get_norm_date(),
            preselected_ids=preselected_ids,
            allow_multiple=not is_edit_mode,
        )

        self.wait_window(dlg)

        if not dlg.result:
            return

        selected_items = dlg.result

        if is_edit_mode and len(selected_items) > 1:
            selected_items = selected_items[:1]

        self._apply_selected_work_items(selected_items)

    def _apply_selected_work_items(
        self,
        selected_items: List[Dict[str, Any]],
    ):
        self._selected_work_items = list(selected_items or [])
        self._bulk_mode = len(self._selected_work_items) > 1

        if not self._selected_work_items:
            self.ent_name.configure(state="normal")
            self.ent_name.delete(0, "end")
            self.ent_name.configure(state="readonly")

            self.lbl_selected_works.config(
                text="Работы не выбраны.",
                fg=C["text3"],
            )

            self._refresh_labor_info()
            return

        # Одна работа: подставляем имя и единицу измерения.
        if len(self._selected_work_items) == 1:
            item = self._selected_work_items[0]

            self.ent_name.configure(state="normal")
            self.ent_name.delete(0, "end")
            self.ent_name.insert(0, item.get("name") or "")
            self.ent_name.configure(state="readonly")

            self._set_uom_code(item.get("uom_code"))

            norm = _safe_float(item.get("labor_hours_per_unit"))

            if norm is None:
                text = (
                    f"⚠ {item.get('name') or 'Работа'}\n"
                    "Для выбранной даты не найдена действующая норма ЗТР."
                )
                color = C["error"]
            else:
                text = (
                    f"✓ {item.get('name') or 'Работа'}\n"
                    f"Ед. изм.: {item.get('uom_code') or '—'}   |   "
                    f"ЗТР: {_fmt_qty(norm)} чел.-ч / ед."
                )
                color = C["success"]

            self.lbl_selected_works.config(
                text=text,
                fg=color,
            )

        # Несколько работ: имя и единица у каждой задачи будут свои.
        else:
            self.ent_name.configure(state="normal")
            self.ent_name.delete(0, "end")
            self.ent_name.insert(
                0,
                f"Выбрано работ: {len(self._selected_work_items)}",
            )
            self.ent_name.configure(state="readonly")

            self.cmb_uom.current(0)

            with_norm = sum(
                1
                for item in self._selected_work_items
                if item.get("labor_norm_id")
                and item.get("labor_hours_per_unit") is not None
            )

            without_norm = len(self._selected_work_items) - with_norm

            text = (
                f"Выбрано работ: {len(self._selected_work_items)}. "
                f"Нормы найдены: {with_norm}."
            )

            if without_norm:
                text += (
                    f"\n⚠ Без нормы на выбранную дату: {without_norm}."
                )

            self.lbl_selected_works.config(
                text=text,
                fg=C["error"] if without_norm else C["success"],
            )

    def _set_name_entry_state(self, state: str):
        try:
            self.ent_name.configure(state=state)
        except tk.TclError:
            pass

    def _set_uom_code(self, uom_code: Optional[str]):
        if not uom_code:
            self.cmb_uom.current(0)
            return

        for index, uom in enumerate(self.uoms, start=1):
            if uom.get("code") == uom_code:
                self.cmb_uom.current(index)
                return

        self.cmb_uom.current(0)

    def _refresh_labor_info(self):
        """
        Обновляет отображение нормы ЗТР и общей трудоёмкости.
        """
        if not hasattr(self, "lbl_labor_norm"):
            return

        if len(self._selected_work_items) != 1:
            if len(self._selected_work_items) > 1:
                self.lbl_labor_norm.config(
                    text="индивидуально по каждой работе",
                    fg=C["text2"],
                )
                self.lbl_total_labor.config(
                    text="рассчитывается отдельно",
                    fg=C["text2"],
                )
                self.lbl_norm_info.config(
                    text=(
                        "Для нескольких работ единица, ЗТР и трудоёмкость "
                        "определяются индивидуально по каждой строке ГПР."
                    ),
                    fg=C["text3"],
                )
            else:
                self.lbl_labor_norm.config(text="—", fg=C["text3"])
                self.lbl_total_labor.config(text="—", fg=C["text3"])
                self.lbl_norm_info.config(
                    text="Выберите работу из справочника.",
                    fg=C["text3"],
                )
            return

        item = self._selected_work_items[0]

        norm = _safe_float(item.get("labor_hours_per_unit"))
        factor = _safe_float(
            item.get("default_productivity_factor")
        ) or 1.0

        if norm is None:
            self.lbl_labor_norm.config(
                text="не найдена",
                fg=C["error"],
            )
            self.lbl_total_labor.config(
                text="—",
                fg=C["text3"],
            )
            self.lbl_norm_info.config(
                text=(
                    f"Нет действующей нормы на дату "
                    f"{_fmt_date(self._get_norm_date())}."
                ),
                fg=C["error"],
            )
            return

        self.lbl_labor_norm.config(
            text=f"{_fmt_qty(norm)} чел.-ч / ед.",
            fg=C["success"],
        )

        qty = _safe_float(self.ent_qty.get())

        if qty is not None:
            total = qty * norm * factor
            self.lbl_total_labor.config(
                text=f"{_fmt_qty(total)} чел.-ч",
                fg=C["accent"],
            )
        else:
            self.lbl_total_labor.config(
                text="укажите объём",
                fg=C["text3"],
            )

        source = item.get("norm_source_name") or "Справочник норм"

        self.lbl_norm_info.config(
            text=(
                f"Норма действует с "
                f"{_fmt_date(item.get('norm_effective_from'))}. "
                f"Источник: {source}. "
                f"Коэффициент: {_fmt_qty(factor)}."
            ),
            fg=C["text3"],
        )

    # ══════════════════════════════════════════════════════
    #  INIT / LOAD
    # ══════════════════════════════════════════════════════
    def _fill_init(self):

        if self.init.get("work_item_id"):
            self._selected_work_items = [
                {
                    "id": self.init.get("work_item_id"),
                    "work_type_id": self.init.get("work_type_id"),
                    "name": self.init.get("name") or "",
                    "uom_code": self.init.get("uom_code"),
                    "labor_norm_id": self.init.get("labor_norm_id"),
                    "labor_hours_per_unit": self.init.get("labor_hours_per_unit"),
                    "default_productivity_factor": self.init.get(
                        "productivity_factor",
                        1,
                    ),
                }
            ]
        iw = self.init.get("work_type_id")
        if iw is not None:
            for i, w in enumerate(self.work_types):
                if int(w["id"]) == int(iw):
                    self.cmb_wt.current(i)
                    break
            else:
                if self.work_types:
                    self.cmb_wt.current(0)
        elif self.work_types:
            self.cmb_wt.current(0)

        self.ent_name.insert(0, self.init.get("name", ""))

        if self._selected_work_items:
            item = self._selected_work_items[0]

            if item.get("labor_hours_per_unit") is not None:
                self.lbl_selected_works.config(
                    text=(
                        "Работа связана со справочником. "
                        f"Норма ЗТР: {_fmt_qty(item.get('labor_hours_per_unit'))} чел.-ч/ед."
                    ),
                    fg=C["success"],
                )
            else:
                self.lbl_selected_works.config(
                    text="Работа связана со справочником, но норма ЗТР не сохранена.",
                    fg=C["warning"],
                )

        iu = self.init.get("uom_code")
        if iu:
            found = False
            for i, u in enumerate(self.uoms):
                if u["code"] == iu:
                    self.cmb_uom.current(i + 1)
                    found = True
                    break
            if not found:
                self.cmb_uom.current(0)
        else:
            self.cmb_uom.current(0)

        if self.init.get("plan_qty") is not None:
            self.ent_qty.insert(0, _fmt_qty(self.init["plan_qty"]))

        d0 = _to_date(self.init.get("plan_start")) or _today()
        d1 = _to_date(self.init.get("plan_finish")) or _today()
        self.ent_start.insert(0, _fmt_date(d0))
        self.ent_finish.insert(0, _fmt_date(d1))

        ist = self.init.get("status", "planned")
        try:
            self.cmb_status.current(STATUS_LIST.index(ist))
        except ValueError:
            self.cmb_status.current(0)

        self.var_milestone.set(bool(self.init.get("is_milestone")))

        self._update_duration()
        self._update_status_color()
        self._update_info()
        self._refresh_labor_info()

    def _load_assignments(self):
        if not self._has_assign_tab:
            self._assignments = []
            return

        task_id = self.init.get("id")
        if task_id:
            try:
                self._assignments = _EmployeeService.load_task_assignments(task_id)
            except Exception:
                logger.exception("Load assignments error for task %s", task_id)
                self._assignments = []
        self._render_assignments()

    def _load_facts(self):
        if not self._has_fact_tab:
            self._facts = []
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

    # ══════════════════════════════════════════════════════
    #  HELPERS / OVERVIEW
    # ══════════════════════════════════════════════════════
    def _update_duration(self):
        try:
            ds = _parse_date(self.ent_start.get())
            df = _parse_date(self.ent_finish.get())
            dur = (df - ds).days + 1
            if dur <= 0:
                self.lbl_duration.config(text="⚠ ошибка", fg=C["error"])
            else:
                self.lbl_duration.config(text=f"{dur} дн.", fg=C["accent"])
        except Exception:
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
        self.lbl_info.config(text=f"ID задачи: {tid}" if tid else "Новая задача")

    def _refresh_overview(self):
        tid = self.init.get("id")
        creator = self.init.get("creator_name") or "—"
        created_at = _fmt_dt(self.init.get("created_at"))
        updated_at = _fmt_dt(self.init.get("updated_at"))

        meta_parts = [f"ID: {tid}" if tid else "Новая задача", f"Создал: {creator}"]
        if created_at:
            meta_parts.append(f"Создано: {created_at}")
        if updated_at:
            meta_parts.append(f"Обновлено: {updated_at}")

        meta_text = "  |  ".join(meta_parts)
        self.lbl_meta.config(text=meta_text)
        self.lbl_head_meta.config(text=meta_text)

        plan_qty = _safe_float(self.ent_qty.get())
        if plan_qty is None:
            plan_qty = _safe_float(self.init.get("plan_qty"))

        total_fact = sum(_safe_float(x.get("fact_qty")) or 0 for x in self._facts)
        remain = None if plan_qty is None else max(0.0, plan_qty - total_fact)

        if plan_qty and plan_qty > 0:
            pct = min(100.0, total_fact / plan_qty * 100.0)
        else:
            pct = 0.0

        self.prg_progress["value"] = pct

        uom = None
        ui = self.cmb_uom.current()
        if ui > 0 and (ui - 1) < len(self.uoms):
            uom = self.uoms[ui - 1]["code"]
        elif self.init.get("uom_code"):
            uom = self.init.get("uom_code")

        uom_s = f" {uom}" if uom else ""

        if plan_qty is not None:
            self.lbl_kpi.config(
                text=(
                    f"План: {_fmt_qty(plan_qty)}{uom_s}  |  "
                    f"Факт: {_fmt_qty(total_fact)}{uom_s}  |  "
                    f"Остаток: {_fmt_qty(remain)}{uom_s}  |  "
                    f"{pct:.1f}%"
                )
            )
        else:
            self.lbl_kpi.config(text=f"Факт: {_fmt_qty(total_fact)}{uom_s}")

        if self._has_fact_tab and hasattr(self, "lbl_fact_summary"):
            self._update_fact_summary()

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
            sw = self.winfo_screenwidth()
            sh = self.winfo_screenheight()
            x = (sw - w) // 2
            y = (sh - h) // 2
        self.geometry(f"+{max(0, x)}+{max(0, y)}")

    def _safe_destroy(self):
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

    def _set_start_today(self):
        self.ent_start.delete(0, "end")
        self.ent_start.insert(0, _fmt_date(_today()))
        self._on_main_field_changed()

    def _copy_start_to_finish(self):
        self.ent_finish.delete(0, "end")
        self.ent_finish.insert(0, self.ent_start.get().strip())
        self._on_main_field_changed()

    def _apply_status_from_fact(self):
        total_fact = sum(_safe_float(x.get("fact_qty")) or 0 for x in self._facts)
        plan_qty = _safe_float(self.ent_qty.get())
        if plan_qty is None:
            plan_qty = _safe_float(self.init.get("plan_qty"))

        if total_fact <= 0:
            status_code = "planned"
        elif plan_qty and total_fact >= plan_qty:
            status_code = "done"
        else:
            status_code = "in_progress"

        try:
            self.cmb_status.current(STATUS_LIST.index(status_code))
        except ValueError:
            self.cmb_status.current(0)

        self._refresh_labor_info()
        self._mark_dirty()
        self._refresh_overview()

    # ══════════════════════════════════════════════════════
    #  EMPLOYEES / ASSIGNMENTS
    # ══════════════════════════════════════════════════════
    def _schedule_emp_search(self):
        if not self._has_assign_tab:
            return
        if self._search_after_id:
            try:
                self.after_cancel(self._search_after_id)
            except Exception:
                pass
        self._search_after_id = self.after(250, self._search_employees)

    def _search_employees(self):
        if not self._has_assign_tab:
            return

        q = self.var_emp_search.get().strip()
        self.emp_tree.delete(*self.emp_tree.get_children())
        self._emp_search_results.clear()

        try:
            self._emp_search_results = _EmployeeService.search_employees(q)
        except Exception as e:
            messagebox.showerror("Поиск", f"Ошибка:\n{e}", parent=self)
            return

        if not self._emp_search_results:
            self.emp_tree.insert("", "end", values=("Не найдено", "", "", ""))
            self.lbl_emp_found.config(text="Ничего не найдено")
            return

        for emp in self._emp_search_results:
            self.emp_tree.insert(
                "",
                "end",
                values=(
                    emp.get("fio") or "",
                    emp.get("tbn") or "",
                    emp.get("position") or "",
                    emp.get("department") or "",
                ),
            )

        self.lbl_emp_found.config(text=f"Найдено: {len(self._emp_search_results)}")

    def _assign_selected(self):
        if not self._has_assign_tab:
            return

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

        for a in self._assignments:
            if int(a["employee_id"]) == int(emp_id):
                messagebox.showinfo(
                    "Назначение",
                    f"{emp.get('fio', '')} уже назначен на эту задачу.",
                    parent=self,
                )
                return

        ri = self.cmb_role.current()
        role_code = TASK_ROLE_LIST[ri] if 0 <= ri < len(TASK_ROLE_LIST) else "executor"

        self._assignments.append(
            {
                "employee_id": int(emp_id),
                "fio": emp.get("fio") or "",
                "tbn": emp.get("tbn") or "",
                "position": emp.get("position") or "",
                "department": emp.get("department") or "",
                "role_in_task": role_code,
                "note": "",
            }
        )
        self._render_assignments()
        self._mark_dirty()

    def _edit_assignment_selected(self):
        if not self._has_assign_tab:
            return

        sel = self.assign_tree.selection()
        if not sel:
            messagebox.showinfo("Назначение", "Выберите назначение.", parent=self)
            return

        try:
            idx = self.assign_tree.index(sel[0])
        except tk.TclError:
            return

        if not (0 <= idx < len(self._assignments)):
            return

        row = self._assignments[idx]
        dlg = _AssignmentEditDialog(
            self,
            init_role=row.get("role_in_task", "executor"),
            init_note=row.get("note", "") or "",
        )
        if not dlg.result:
            return

        row["role_in_task"] = dlg.result["role_in_task"]
        row["note"] = dlg.result["note"]
        self._render_assignments()
        self._mark_dirty()

    def _remove_assignment(self):
        if not self._has_assign_tab:
            return

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
            self._assignments.pop(idx)
            self._render_assignments()
            self._mark_dirty()

    def _render_assignments(self):
        if not self._has_assign_tab:
            return

        self.assign_tree.delete(*self.assign_tree.get_children())

        role_counts = {k: 0 for k in TASK_ROLE_LIST}
        for a in self._assignments:
            role_code = a.get("role_in_task", "executor")
            role_label = TASK_ROLES.get(role_code, "?")
            if role_code in role_counts:
                role_counts[role_code] += 1

            tags = (role_code,) if role_code in ("foreman", "inspector") else ()
            self.assign_tree.insert(
                "",
                "end",
                values=(
                    a.get("fio") or "",
                    a.get("tbn") or "",
                    a.get("position") or "",
                    role_label,
                    a.get("department") or "",
                    a.get("note") or "",
                ),
                tags=tags,
            )

        parts = [f"Назначено: {len(self._assignments)}"]
        if role_counts["executor"]:
            parts.append(f"исп.: {role_counts['executor']}")
        if role_counts["foreman"]:
            parts.append(f"бриг.: {role_counts['foreman']}")
        if role_counts["inspector"]:
            parts.append(f"контр.: {role_counts['inspector']}")

        self.lbl_assign_count.config(text="  |  ".join(parts))

    # ══════════════════════════════════════════════════════
    #  FACTS
    # ══════════════════════════════════════════════════════
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
        self.ent_fact_workers.delete(0, "end")
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
        
        self.ent_fact_workers.delete(0, "end")
        if row.get("workers_count") is not None:
            self.ent_fact_workers.insert(0, str(int(row.get("workers_count"))))
        
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
    
            workers_count = _safe_float(self.ent_fact_workers.get())
            if workers_count is None or workers_count <= 0 or int(workers_count) != workers_count:
                raise ValueError("Введите корректное количество людей больше 0")
            workers_count = int(workers_count)
    
            comment = (self.ent_fact_comment.get() or "").strip()
    
            duplicate_idx = None
            for i, row in enumerate(self._facts):
                if i == self._fact_edit_idx:
                    continue
                if (
                    _to_date(row.get("fact_date")) == fact_date
                    and (row.get("period_type") or "day").strip() == period_type
                ):
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
    
            plan_qty = _safe_float(self.ent_qty.get())
            if plan_qty is None:
                plan_qty = _safe_float(self.init.get("plan_qty"))
    
            projected_total = sum(_safe_float(x.get("fact_qty")) or 0 for x in self._facts)
            if self._fact_edit_idx is not None:
                projected_total -= _safe_float(self._facts[self._fact_edit_idx].get("fact_qty")) or 0
            projected_total += fact_qty
    
            if plan_qty and plan_qty > 0 and projected_total > plan_qty:
                if not messagebox.askyesno(
                    "Факт",
                    (
                        f"Суммарный факт станет {_fmt_qty(projected_total)}, "
                        f"что превышает план {_fmt_qty(plan_qty)}.\n\nПродолжить?"
                    ),
                    parent=self,
                ):
                    return
    
            row = {
                "fact_date": fact_date,
                "period_type": period_type,
                "fact_qty": fact_qty,
                "workers_count": workers_count,
                "comment": comment,
                "creator_name": "Текущий пользователь",
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
    
            self._render_facts()
            self._fact_clear_form()
            self._mark_dirty()
            self._refresh_overview()
    
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
        self._render_facts()
        self._fact_clear_form()
        self._mark_dirty()
        self._refresh_overview()

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
                    row.get("workers_count") if row.get("workers_count") is not None else "",
                    row.get("comment") or "",
                    row.get("creator_name") or "",
                ),
            )
    
        self._update_fact_summary()

    def _update_fact_summary(self):
        if not self._has_fact_tab:
            return
    
        total_fact = sum(_safe_float(x.get("fact_qty")) or 0 for x in self._facts)
        total_workers = sum(int(_safe_float(x.get("workers_count")) or 0) for x in self._facts)
    
        plan_qty = _safe_float(self.ent_qty.get())
        if plan_qty is None:
            plan_qty = _safe_float(self.init.get("plan_qty"))
    
        if plan_qty and plan_qty > 0:
            pct = min(100.0, total_fact / plan_qty * 100.0)
            remain = max(0.0, plan_qty - total_fact)
            text = (
                f"Накопительный факт: {_fmt_qty(total_fact)}  |  "
                f"Людей суммарно: {total_workers}  |  "
                f"Остаток: {_fmt_qty(remain)}  |  "
                f"Выполнение: {pct:.1f}%"
            )
        else:
            text = (
                f"Накопительный факт: {_fmt_qty(total_fact)}  |  "
                f"Людей суммарно: {total_workers}"
            )
    
        self.lbl_fact_summary.config(text=text)

    # ══════════════════════════════════════════════════════
    #  SAVE / CANCEL
    # ══════════════════════════════════════════════════════
    def _on_ok(self):
        try:
            wi = self.cmb_wt.current()
            if wi < 0:
                raise ValueError("Выберите тип работ")

            wt_id = int(self.work_types[wi]["id"])

            if not self._selected_work_items:
                raise ValueError(
                    "Выберите работу из справочника."
                )

            qty = _safe_float(self.ent_qty.get())

            ds = _parse_date(self.ent_start.get())
            df = _parse_date(self.ent_finish.get())

            if df < ds:
                raise ValueError("Дата окончания раньше даты начала")

            si = self.cmb_status.current()
            st = STATUS_LIST[si] if 0 <= si < len(STATUS_LIST) else "planned"

            is_milestone = bool(self.var_milestone.get())

            # ───────────────────────────────────────────────
            # Массовое создание задач по выбранным работам.
            # Доступно только при создании новой записи.
            # ───────────────────────────────────────────────
            if self._bulk_mode and not self.init.get("id"):
                bulk_tasks: List[Dict[str, Any]] = []

                for item in self._selected_work_items:
                    bulk_tasks.append(
                        {
                            "work_type_id": wt_id,
                            "work_item_id": int(item["id"]),
                            "labor_norm_id": item.get("labor_norm_id"),

                            # Снимок нормы на момент создания задачи.
                            "labor_hours_per_unit": item.get(
                                "labor_hours_per_unit"
                            ),
                            "productivity_factor": item.get(
                                "default_productivity_factor"
                            ) or 1.0,

                            "name": item.get("name") or "",
                            "uom_code": item.get("uom_code"),
                            "plan_qty": qty,
                            "plan_start": ds,
                            "plan_finish": df,
                            "status": st,
                            "is_milestone": is_milestone,
                        }
                    )

                if not bulk_tasks:
                    raise ValueError("Не выбрано ни одной работы")

                self.result = {
                    "_bulk_tasks": bulk_tasks,
                    "_assignments": [],
                    "_facts": [],
                    "_facts_changed": False,
                }

                self._dirty = False
                self._safe_destroy()
                return

            # ───────────────────────────────────────────────
            # Обычное создание или редактирование одной задачи.
            # ───────────────────────────────────────────────
            selected_item = (
                self._selected_work_items[0]
                if len(self._selected_work_items) == 1
                else None
            )

            if not selected_item:
                raise ValueError(
                    "Для одной задачи должна быть выбрана одна работа "
                    "из справочника."
                )

            nm = (selected_item.get("name") or "").strip()

            if not nm:
                raise ValueError(
                    "У выбранной работы не заполнено наименование."
                )

            if (
                not selected_item.get("labor_norm_id")
                or selected_item.get("labor_hours_per_unit") is None
            ):
                raise ValueError(
                    "Для выбранной работы не найдена действующая норма ЗТР. "
                    "Проверьте дату начала работы и даты действия нормы."
                )

            uom = None
            ui = self.cmb_uom.current()

            if ui > 0 and (ui - 1) < len(self.uoms):
                uom = self.uoms[ui - 1]["code"]

            self.result = {
                "work_type_id": wt_id,
                "work_item_id": (
                    int(selected_item["id"])
                    if selected_item and selected_item.get("id")
                    else None
                ),
                "labor_norm_id": (
                    selected_item.get("labor_norm_id")
                    if selected_item
                    else None
                ),
                "labor_hours_per_unit": (
                    selected_item.get("labor_hours_per_unit")
                    if selected_item
                    else None
                ),
                "productivity_factor": (
                    selected_item.get(
                        "default_productivity_factor",
                        1.0,
                    )
                    if selected_item
                    else 1.0
                ),
                "name": nm,
                "uom_code": uom,
                "plan_qty": qty,
                "plan_start": ds,
                "plan_finish": df,
                "status": st,
                "is_milestone": is_milestone,
                "_assignments": (
                    list(self._assignments)
                    if self._has_assign_tab
                    else []
                ),
                "_facts": (
                    list(self._facts)
                    if self._has_fact_tab
                    else []
                ),
                "_facts_changed": bool(self._has_fact_tab),
            }

            self._dirty = False
            self._safe_destroy()

        except ValueError as exc:
            messagebox.showwarning(
                "Работа ГПР",
                str(exc),
                parent=self,
            )
            try:
                self.nb.select(0)
            except tk.TclError:
                pass

        except Exception as exc:
            logger.exception("TaskEditDialogPro._on_ok unexpected error")
            messagebox.showerror(
                "Ошибка",
                f"Непредвиденная ошибка:\n{exc}",
                parent=self,
            )

    def _on_cancel(self):
        if self._dirty:
            if not messagebox.askyesno(
                "Работа ГПР",
                "Есть несохранённые изменения.\nЗакрыть диалог без сохранения?",
                parent=self,
            ):
                return
        self.result = None
        self._safe_destroy()

class TaskFactBatchDialog(tk.Toplevel):
    """
    Быстрый массовый ввод факта:
    - слева Treeview со списком работ;
    - справа панель редактирования выбранной строки;
    - на 1000+ строках не зависает, потому что нет тысяч Entry-виджетов.
    """

    COL_WIDTHS = [340, 170, 60, 90, 90, 70, 230]

    def __init__(
        self,
        parent,
        tasks: List[Dict[str, Any]],
        user_id: Optional[int] = None,
        fact_date: Optional[date] = None,
    ):
        super().__init__(parent)
        self.transient(parent)

        self.tasks_src = tasks or []
        self.user_id = user_id
        self.result: Optional[Dict[str, Any]] = None
        self._destroyed = False

        self._all_rows: List[Dict[str, Any]] = self._prepare_rows(self.tasks_src)
        self._filtered_rows: List[Dict[str, Any]] = []

        self._loaded_fact_map: Dict[int, Dict[str, Any]] = {}
        self._pending_edits: Dict[int, Optional[Dict[str, Any]]] = {}

        self._task_iid_map: Dict[int, str] = {}
        self._iid_to_row: Dict[str, Dict[str, Any]] = {}

        self._selected_task_id: Optional[int] = None
        self._editor_task_id: Optional[int] = None
        self._editor_dirty = False
        self._ignore_tree_event = False
        self._suspend_editor_trace = False

        self.title("📈 Массовое заполнение факта")
        self.minsize(1260, 760)
        self.geometry("1280x800")
        self.resizable(True, True)
        self.configure(bg=C["bg"])

        self.var_fact_date = tk.StringVar(value=_fmt_date(fact_date or _today()))
        self.var_period = tk.StringVar(value=FACT_PERIODS["day"])
        self.var_search = tk.StringVar(value="")
        self.var_title = tk.StringVar(value="Все")
        self.var_group = tk.StringVar(value="Все")

        self._build_ui()
        self._fill_filters()
        
        # Показываем все строки сразу, потом подгружаем факт и рисуем один раз
        self._filtered_rows = list(self._all_rows)
        self._reload_existing_facts(clear_pending=False)

        self.grab_set()
        self.after(20, self._center)
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

    # ─────────────────────────────────────────────────────
    # DATA
    # ─────────────────────────────────────────────────────
    def _prepare_rows(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        cur_title = ""
        cur_group = ""

        for t in tasks:
            row_kind = (t.get("row_kind") or "task").strip()
            name = (t.get("name") or "").strip()

            if row_kind == "title":
                cur_title = name
                cur_group = ""
                rows.append(
                    {
                        "row_kind": "title",
                        "title_name": cur_title,
                        "group_name": "",
                        "display_name": name,
                    }
                )
                continue

            if row_kind == "group":
                cur_group = name
                rows.append(
                    {
                        "row_kind": "group",
                        "title_name": cur_title,
                        "group_name": cur_group,
                        "display_name": name,
                    }
                )
                continue

            if row_kind != "task":
                continue

            tid = t.get("id")
            if not tid:
                continue

            rows.append(
                {
                    "row_kind": "task",
                    "task_id": int(tid),
                    "title_name": cur_title,
                    "group_name": cur_group,
                    "display_name": name,
                    "work_type_name": t.get("work_type_name") or "",
                    "uom_code": t.get("uom_code") or "",
                    "plan_qty": t.get("plan_qty"),
                    "task": t,
                }
            )

        return rows

    def _task_ids(self) -> List[int]:
        return [
            int(r["task_id"])
            for r in self._all_rows
            if r.get("row_kind") == "task" and r.get("task_id")
        ]

    def _load_existing_facts(self) -> Dict[int, Dict[str, Any]]:
        task_ids = self._task_ids()
        if not task_ids:
            return {}

        fact_date = _parse_date(self.var_fact_date.get())
        period_label = (self.cmb_period.get() or "").strip()
        period_type = FACT_PERIOD_FROM_LABEL.get(period_label, "day")

        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        task_id,
                        id,
                        fact_qty,
                        workers_count,
                        COALESCE(comment, '') AS comment
                    FROM public.gpr_task_facts
                    WHERE task_id = ANY(%s)
                      AND fact_date = %s
                      AND period_type = %s
                    """,
                    (task_ids, fact_date, period_type),
                )
                out: Dict[int, Dict[str, Any]] = {}
                for r in cur.fetchall():
                    d = dict(r)
                    tid = int(d["task_id"])
                    out[tid] = {
                        "id": int(d["id"]),
                        "fact_qty": d.get("fact_qty"),
                        "workers_count": d.get("workers_count"),
                        "comment": d.get("comment") or "",
                    }
                return out
        finally:
            _release(conn)

    def _display_state_for_task(self, task_id: int) -> Tuple[str, str, str]:
        """
        Возвращает:
          fact_qty_text, workers_text, comment_text
        """
        if task_id in self._pending_edits:
            pending = self._pending_edits[task_id]
            if pending is None:
                return "", "", ""
            return (
                _fmt_qty(pending.get("fact_qty")),
                str(int(pending["workers_count"])) if pending.get("workers_count") is not None else "",
                pending.get("comment") or "",
            )

        loaded = self._loaded_fact_map.get(task_id)
        if loaded:
            return (
                _fmt_qty(loaded.get("fact_qty")),
                str(int(loaded["workers_count"])) if loaded.get("workers_count") is not None else "",
                loaded.get("comment") or "",
            )

        return "", "", ""

    # ─────────────────────────────────────────────────────
    # UI
    # ─────────────────────────────────────────────────────
    def _build_ui(self):
        top = tk.LabelFrame(
            self,
            text=" Параметры ввода ",
            bg=C["panel"],
            fg=C["accent"],
            font=("Segoe UI", 9, "bold"),
            padx=10,
            pady=8,
        )
        top.pack(fill="x", padx=10, pady=(10, 6))

        row1 = tk.Frame(top, bg=C["panel"])
        row1.pack(fill="x", pady=2)

        tk.Label(row1, text="Дата факта:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.ent_fact_date = ttk.Entry(row1, textvariable=self.var_fact_date, width=12)
        self.ent_fact_date.pack(side="left", padx=(6, 10))
        self.ent_fact_date.bind("<FocusOut>", lambda _e: self._on_fact_params_changed())
        self.ent_fact_date.bind("<Return>", lambda _e: self._on_fact_params_changed())

        ttk.Button(row1, text="Сегодня", command=self._set_today).pack(side="left", padx=(0, 14))

        tk.Label(row1, text="Период:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.cmb_period = ttk.Combobox(
            row1,
            state="readonly",
            width=14,
            values=FACT_PERIOD_LABELS,
            textvariable=self.var_period,
        )
        self.cmb_period.pack(side="left", padx=(6, 14))
        self.cmb_period.current(0)
        self.cmb_period.bind("<<ComboboxSelected>>", lambda _e: self._on_fact_params_changed())

        btns = tk.Frame(row1, bg=C["panel"])
        btns.pack(side="right")

        ttk.Button(btns, text="Сохранить", command=self._on_ok).pack(side="right", padx=2)
        ttk.Button(btns, text="Отмена", command=self._on_cancel).pack(side="right", padx=2)
        ttk.Button(btns, text="Очистить всё", command=self._clear_inputs).pack(side="right", padx=2)
        ttk.Button(btns, text="Подтянуть факт", command=self._reload_existing_facts_button).pack(side="right", padx=2)

        row2 = tk.Frame(top, bg=C["panel"])
        row2.pack(fill="x", pady=(8, 2))

        tk.Label(row2, text="Поиск:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        ent_search = ttk.Entry(row2, textvariable=self.var_search, width=28)
        ent_search.pack(side="left", padx=(6, 12))
        ent_search.bind("<KeyRelease>", lambda _e: self._apply_filter())

        tk.Label(row2, text="Титул:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.cmb_title = ttk.Combobox(
            row2,
            state="readonly",
            width=26,
            textvariable=self.var_title,
        )
        self.cmb_title.pack(side="left", padx=(6, 12))
        self.cmb_title.bind("<<ComboboxSelected>>", lambda _e: self._on_title_filter_changed())

        tk.Label(row2, text="Группа:", bg=C["panel"], font=("Segoe UI", 9)).pack(side="left")
        self.cmb_group = ttk.Combobox(
            row2,
            state="readonly",
            width=26,
            textvariable=self.var_group,
        )
        self.cmb_group.pack(side="left", padx=(6, 12))
        self.cmb_group.bind("<<ComboboxSelected>>", lambda _e: self._apply_filter())

        ttk.Button(row2, text="Очистить видимые", command=self._clear_visible_inputs).pack(side="right", padx=2)

        self.lbl_summary = tk.Label(
            self,
            text="",
            bg=C["bg"],
            fg=C["text2"],
            font=("Segoe UI", 8),
            anchor="w",
        )
        self.lbl_summary.pack(fill="x", padx=14, pady=(0, 4))

        split = tk.PanedWindow(self, orient="horizontal", sashrelief="raised", bg=C["bg"])
        split.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        left = tk.Frame(split, bg=C["panel"])
        right = tk.Frame(split, bg=C["panel"])
        split.add(left, minsize=820)
        split.add(right, minsize=360)

        # Лист работ
        tree_wrap = tk.Frame(left, bg=C["panel"])
        tree_wrap.pack(fill="both", expand=True)

        cols = ("name", "work_type", "uom", "plan", "fact", "workers", "comment")
        self.tree = ttk.Treeview(
            tree_wrap,
            columns=cols,
            show="headings",
            selectmode="browse",
        )

        headers = {
            "name": ("Работа", 340, "w"),
            "work_type": ("Тип работ", 170, "w"),
            "uom": ("Ед.", 60, "center"),
            "plan": ("План", 90, "center"),
            "fact": ("Факт", 90, "center"),
            "workers": ("Людей", 70, "center"),
            "comment": ("Комментарий", 230, "w"),
        }

        for c, (title, w, anchor) in headers.items():
            self.tree.heading(c, text=title)
            self.tree.column(c, width=w, anchor=anchor)

        vsb = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_wrap, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")

        self.tree.tag_configure("title", background="#dceeff", font=("Segoe UI", 9, "bold"))
        self.tree.tag_configure("group", background="#eef5ff", font=("Segoe UI", 9, "bold"))
        self.tree.tag_configure("loaded", background="#e8f5e9")
        self.tree.tag_configure("edited", background="#e3f2fd")
        self.tree.tag_configure("deleted", background="#ffebee")
        self.tree.tag_configure("task", background="#ffffff")

        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self.tree.bind("<Double-1>", lambda _e: self._focus_qty())
        self.tree.bind("<Return>", lambda _e: self._focus_qty())

        # Панель редактирования
        self._build_editor(right)

    def _build_editor(self, parent):
        box = tk.LabelFrame(
            parent,
            text=" Редактирование выбранной строки ",
            bg=C["panel"],
            fg=C["accent"],
            font=("Segoe UI", 9, "bold"),
            padx=10,
            pady=8,
        )
        box.pack(fill="both", expand=True, padx=10, pady=10)

        self.lbl_selected = tk.Label(
            box,
            text="Выберите строку в списке",
            bg=C["panel"],
            fg=C["text2"],
            font=("Segoe UI", 9, "bold"),
            justify="left",
            anchor="w",
        )
        self.lbl_selected.pack(fill="x", pady=(0, 10))

        self.lbl_selected_meta = tk.Label(
            box,
            text="",
            bg=C["panel"],
            fg=C["text2"],
            font=("Segoe UI", 8),
            justify="left",
            anchor="w",
            wraplength=320,
        )
        self.lbl_selected_meta.pack(fill="x", pady=(0, 12))

        form = tk.Frame(box, bg=C["panel"])
        form.pack(fill="x")

        tk.Label(form, text="Объём:", bg=C["panel"], font=("Segoe UI", 9)).grid(row=0, column=0, sticky="e", padx=(0, 8), pady=4)
        self.var_edit_qty = tk.StringVar()
        self.ent_edit_qty = ttk.Entry(form, textvariable=self.var_edit_qty, width=16)
        self.ent_edit_qty.grid(row=0, column=1, sticky="w", pady=4)

        tk.Label(form, text="Людей:", bg=C["panel"], font=("Segoe UI", 9)).grid(row=1, column=0, sticky="e", padx=(0, 8), pady=4)
        self.var_edit_workers = tk.StringVar()
        self.ent_edit_workers = ttk.Entry(form, textvariable=self.var_edit_workers, width=16)
        self.ent_edit_workers.grid(row=1, column=1, sticky="w", pady=4)

        tk.Label(form, text="Комментарий:", bg=C["panel"], font=("Segoe UI", 9)).grid(row=2, column=0, sticky="ne", padx=(0, 8), pady=4)
        self.var_edit_comment = tk.StringVar()
        self.ent_edit_comment = ttk.Entry(form, textvariable=self.var_edit_comment, width=34)
        self.ent_edit_comment.grid(row=2, column=1, sticky="ew", pady=4)

        form.grid_columnconfigure(1, weight=1)

        self.lbl_editor_hint = tk.Label(
            box,
            text="Enter: сохранить и перейти к следующей строке",
            bg=C["panel"],
            fg=C["text3"],
            font=("Segoe UI", 8),
            anchor="w",
        )
        self.lbl_editor_hint.pack(fill="x", pady=(8, 10))

        btns = tk.Frame(box, bg=C["panel"])
        btns.pack(fill="x", pady=(4, 0))

        ttk.Button(btns, text="Применить", command=self._apply_current_row).pack(side="left", padx=2)
        ttk.Button(btns, text="Применить и следующая", command=self._apply_and_next).pack(side="left", padx=2)
        ttk.Button(btns, text="Очистить строку", command=self._clear_current_row).pack(side="left", padx=2)

        self.lbl_editor_state = tk.Label(
            box,
            text="",
            bg=C["panel"],
            fg=C["text2"],
            font=("Segoe UI", 8),
            anchor="w",
            justify="left",
        )
        self.lbl_editor_state.pack(fill="x", pady=(10, 0))

        self._bind_editor_tracking()
        self.ent_edit_qty.bind("<Return>", lambda _e: self.ent_edit_workers.focus_set())
        self.ent_edit_workers.bind("<Return>", lambda _e: self.ent_edit_comment.focus_set())
        self.ent_edit_comment.bind("<Return>", lambda _e: self._apply_and_next())
        self.ent_edit_qty.bind("<Down>", lambda _e: self._apply_and_next())
        self.ent_edit_workers.bind("<Down>", lambda _e: self._apply_and_next())
        self.ent_edit_comment.bind("<Down>", lambda _e: self._apply_and_next())

    def _bind_editor_tracking(self):
        def mark_dirty(*_args):
            if self._suspend_editor_trace:
                return
            self._editor_dirty = True
            self._update_editor_state()

        self.var_edit_qty.trace_add("write", mark_dirty)
        self.var_edit_workers.trace_add("write", mark_dirty)
        self.var_edit_comment.trace_add("write", mark_dirty)

    # ─────────────────────────────────────────────────────
    # FILTER / RENDER
    # ─────────────────────────────────────────────────────
    def _fill_filters(self):
        titles = sorted({
            r["title_name"]
            for r in self._all_rows
            if r.get("title_name")
        })
        groups = sorted({
            r["group_name"]
            for r in self._all_rows
            if r.get("group_name")
        })
    
        self.cmb_title["values"] = ["Все"] + titles
        self.cmb_group["values"] = ["Все"] + groups
    
        self.cmb_title.current(0)
        self.cmb_group.current(0)

    def _on_title_filter_changed(self):
        self._apply_filter()

    def _apply_filter(self):
        q = (self.var_search.get() or "").strip().lower()
        title_filter = (self.var_title.get() or "Все").strip()
        group_filter = (self.var_group.get() or "Все").strip()
    
        visible_task_ids = set()
        visible_titles = set()
        visible_groups = set()
    
        for row in self._all_rows:
            if row.get("row_kind") != "task":
                continue
    
            title_name = row.get("title_name") or ""
            group_name = row.get("group_name") or ""
    
            if title_filter != "Все" and title_name != title_filter:
                continue
    
            if group_filter != "Все" and group_name != group_filter:
                continue
    
            if q:
                hay = " ".join(
                    [
                        title_name,
                        group_name,
                        row.get("work_type_name") or "",
                        row.get("display_name") or "",
                        row.get("uom_code") or "",
                        _fmt_qty(row.get("plan_qty")),
                    ]
                ).lower()
    
                if q not in hay:
                    continue
    
            task_id = row.get("task_id")
            if task_id:
                visible_task_ids.add(int(task_id))
    
            if title_name:
                visible_titles.add(title_name)
    
            if group_name:
                visible_groups.add((title_name, group_name))
    
        final_rows: List[Dict[str, Any]] = []
        added_titles = set()
        added_groups = set()
    
        for row in self._all_rows:
            row_kind = row.get("row_kind")
    
            if row_kind == "title":
                title_name = row.get("display_name") or row.get("title_name") or ""
    
                if title_name in visible_titles and title_name not in added_titles:
                    final_rows.append(row)
                    added_titles.add(title_name)
    
                continue
    
            if row_kind == "group":
                title_name = row.get("title_name") or ""
                group_name = row.get("display_name") or row.get("group_name") or ""
                group_key = (title_name, group_name)
    
                if group_key in visible_groups and group_key not in added_groups:
                    final_rows.append(row)
                    added_groups.add(group_key)
    
                continue
    
            if row_kind == "task":
                task_id = row.get("task_id")
                if task_id and int(task_id) in visible_task_ids:
                    final_rows.append(row)
    
        self._filtered_rows = final_rows
        self._render_table()
        self._update_summary()
        self._restore_selection_after_render()

    def _render_table(self):
        self.tree.delete(*self.tree.get_children())
        self._task_iid_map.clear()
        self._iid_to_row.clear()

        for idx, row in enumerate(self._filtered_rows):
            row_kind = row["row_kind"]

            if row_kind == "title":
                iid = f"title_{idx}"
                self._iid_to_row[iid] = row
                self.tree.insert(
                    "",
                    "end",
                    iid=iid,
                    values=(row.get("display_name") or "", "", "", "", "", "", ""),
                    tags=("title",),
                )
                continue

            if row_kind == "group":
                iid = f"group_{idx}"
                self._iid_to_row[iid] = row
                self.tree.insert(
                    "",
                    "end",
                    iid=iid,
                    values=(f"   {row.get('display_name') or ''}", "", "", "", "", "", ""),
                    tags=("group",),
                )
                continue

            task_id = int(row["task_id"])
            iid = f"task_{task_id}"
            self._task_iid_map[task_id] = iid
            self._iid_to_row[iid] = row

            fact_qty_text, workers_text, comment_text = self._display_state_for_task(task_id)

            tags = ["task"]
            if task_id in self._pending_edits:
                if self._pending_edits[task_id] is None:
                    tags = ["deleted"]
                else:
                    tags = ["edited"]
            elif task_id in self._loaded_fact_map:
                tags = ["loaded"]

            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    row.get("display_name") or "",
                    row.get("work_type_name") or "",
                    row.get("uom_code") or "",
                    _fmt_qty(row.get("plan_qty")),
                    fact_qty_text,
                    workers_text,
                    comment_text[:80],
                ),
                tags=tuple(tags),
            )

    def _restore_selection_after_render(self):
        if self._selected_task_id is not None:
            iid = self._task_iid_map.get(self._selected_task_id)
            if iid and self.tree.exists(iid):
                self._ignore_tree_event = True
                try:
                    self.tree.selection_set(iid)
                    self.tree.focus(iid)
                    self.tree.see(iid)
                finally:
                    self.after_idle(self._reset_tree_event_flag)
                self._load_editor_for_task(self._selected_task_id)
                return

        # если ничего не выбрано — выбираем первую видимую работу
        first_task_id = next(
            (
                int(r["task_id"])
                for r in self._filtered_rows
                if r.get("row_kind") == "task" and r.get("task_id")
            ),
            None,
        )
        if first_task_id is not None:
            self._select_task(first_task_id)
        else:
            self._clear_editor()
            self._selected_task_id = None

    def _reset_tree_event_flag(self):
        self._ignore_tree_event = False

    def _update_summary(self):
        total_tasks = sum(1 for r in self._all_rows if r["row_kind"] == "task")
        shown_tasks = sum(1 for r in self._filtered_rows if r["row_kind"] == "task")
        loaded_cnt = len(
            [
                tid for tid in self._loaded_fact_map.keys()
                if tid in self._task_iid_map
            ]
        )
        pending_cnt = sum(1 for v in self._pending_edits.values() if v is not None)
        deleted_cnt = sum(1 for v in self._pending_edits.values() if v is None)

        self.lbl_summary.config(
            text=(
                f"Всего работ: {total_tasks}  |  "
                f"Показано: {shown_tasks}  |  "
                f"Загружено фактов: {loaded_cnt}  |  "
                f"Изменено: {pending_cnt}  |  "
                f"Удаление: {deleted_cnt}"
            )
        )

    # ─────────────────────────────────────────────────────
    # SELECTION / EDITOR
    # ─────────────────────────────────────────────────────
    def _on_tree_select(self, _event=None):
        if self._ignore_tree_event:
            return

        sel = self.tree.selection()
        if not sel:
            return

        iid = sel[0]
        row = self._iid_to_row.get(iid)
        if not row:
            return

        if row["row_kind"] != "task":
            self._selected_task_id = None
            self._clear_editor()
            return

        task_id = int(row["task_id"])

        if self._editor_dirty and self._editor_task_id is not None and task_id != self._editor_task_id:
            res = messagebox.askyesnocancel(
                "Факт",
                "Сохранить изменения текущей строки?",
                parent=self,
            )
            if res is None:
                self._ignore_tree_event = True
                try:
                    prev_iid = self._task_iid_map.get(self._editor_task_id)
                    if prev_iid:
                        self.tree.selection_set(prev_iid)
                        self.tree.focus(prev_iid)
                        self.tree.see(prev_iid)
                finally:
                    self.after_idle(self._reset_tree_event_flag)
                return
            if res:
                if not self._apply_current_row(quiet=True):
                    self._ignore_tree_event = True
                    try:
                        prev_iid = self._task_iid_map.get(self._editor_task_id)
                        if prev_iid:
                            self.tree.selection_set(prev_iid)
                            self.tree.focus(prev_iid)
                            self.tree.see(prev_iid)
                    finally:
                        self.after_idle(self._reset_tree_event_flag)
                    return
            else:
                self._editor_dirty = False

        self._selected_task_id = task_id
        self._load_editor_for_task(task_id)

    def _load_editor_for_task(self, task_id: int):
        row = next(
            (r for r in self._filtered_rows if r.get("row_kind") == "task" and int(r["task_id"]) == int(task_id)),
            None,
        )
        if not row:
            self._clear_editor()
            return

        self._editor_task_id = task_id
        self._suspend_editor_trace = True
        try:
            fact_qty, workers, comment = self._display_state_for_task(task_id)
            self.var_edit_qty.set(fact_qty)
            self.var_edit_workers.set(workers)
            self.var_edit_comment.set(comment)
        finally:
            self._suspend_editor_trace = False

        self._editor_dirty = False
        self.lbl_selected.config(text=f"Выбрано: {row.get('display_name') or ''}")
        self.lbl_selected_meta.config(
            text=(
                f"Тип работ: {row.get('work_type_name') or '—'}\n"
                f"Ед.: {row.get('uom_code') or '—'}\n"
                f"План: {_fmt_qty(row.get('plan_qty')) or '—'}\n"
                f"Титул: {row.get('title_name') or '—'}\n"
                f"Группа: {row.get('group_name') or '—'}"
            )
        )
        self._update_editor_state()

    def _clear_editor(self):
        self._editor_task_id = None
        self._suspend_editor_trace = True
        try:
            self.var_edit_qty.set("")
            self.var_edit_workers.set("")
            self.var_edit_comment.set("")
        finally:
            self._suspend_editor_trace = False

        self._editor_dirty = False
        self.lbl_selected.config(text="Выберите строку в списке")
        self.lbl_selected_meta.config(text="")
        self._update_editor_state()

    def _update_editor_state(self):
        if self._editor_task_id is None:
            self.lbl_editor_state.config(text="Строка не выбрана.")
            return

        state = "изменена" if self._editor_dirty else "без изменений"
        self.lbl_editor_state.config(
            text=f"ID работы: {self._editor_task_id}  |  Состояние: {state}"
        )

    def _select_task(self, task_id: int):
        iid = self._task_iid_map.get(task_id)
        if not iid or not self.tree.exists(iid):
            return

        self._ignore_tree_event = True
        try:
            self.tree.selection_set(iid)
            self.tree.focus(iid)
            self.tree.see(iid)
        finally:
            self.after_idle(self._reset_tree_event_flag)

        self._selected_task_id = task_id
        self._load_editor_for_task(task_id)

    def _focus_qty(self):
        if self._editor_task_id is not None:
            self.ent_edit_qty.focus_set()

    def _apply_current_row(self, quiet: bool = False) -> bool:
        if self._editor_task_id is None:
            return True

        task_id = int(self._editor_task_id)
        qty_s = (self.var_edit_qty.get() or "").strip()
        workers_s = (self.var_edit_workers.get() or "").strip()
        comment = (self.var_edit_comment.get() or "").strip()

        # пустая строка = удалить факт по этой работе, если он был
        if not qty_s and not workers_s and not comment:
            if task_id in self._loaded_fact_map or task_id in self._pending_edits:
                self._pending_edits[task_id] = None
            else:
                self._pending_edits.pop(task_id, None)
            self._editor_dirty = False
            self._render_table()
            self._update_summary()
            self._update_editor_state()
            return True

        try:
            qty = _safe_float(qty_s)
            if qty is None or qty <= 0:
                raise ValueError("Введите корректный объём факта больше 0")

            workers = _safe_float(workers_s)
            if workers is None or workers <= 0 or int(workers) != workers:
                raise ValueError("Введите корректное количество людей больше 0")
            workers = int(workers)

            fact_date = _parse_date(self.var_fact_date.get())
            period_label = (self.cmb_period.get() or "").strip()
            period_type = FACT_PERIOD_FROM_LABEL.get(period_label, "day")

            self._pending_edits[task_id] = {
                "task_id": task_id,
                "fact_date": fact_date,
                "period_type": period_type,
                "fact_qty": qty,
                "workers_count": workers,
                "comment": comment or None,
            }

            self._editor_dirty = False
            self._render_table()
            self._update_summary()
            self._update_editor_state()
            return True

        except Exception as e:
            if not quiet:
                messagebox.showwarning("Факт", str(e), parent=self)
            return False

    def _apply_and_next(self):
        if not self._apply_current_row():
            return
        nxt = self._next_visible_task_id(self._editor_task_id)
        if nxt is not None:
            self._select_task(nxt)

    def _next_visible_task_id(self, task_id: Optional[int]) -> Optional[int]:
        ids = [
            int(r["task_id"])
            for r in self._filtered_rows
            if r.get("row_kind") == "task" and r.get("task_id")
        ]
        if task_id not in ids:
            return None
        idx = ids.index(task_id)
        if idx + 1 < len(ids):
            return ids[idx + 1]
        return None

    def _clear_current_row(self):
        if self._editor_task_id is None:
            return

        self._suspend_editor_trace = True
        try:
            self.var_edit_qty.set("")
            self.var_edit_workers.set("")
            self.var_edit_comment.set("")
        finally:
            self._suspend_editor_trace = False

        self._editor_dirty = True
        self._update_editor_state()

    # ─────────────────────────────────────────────────────
    # LOADING / RELOAD
    # ─────────────────────────────────────────────────────
    def _on_fact_params_changed(self):
        """
        Смена даты/��ериода = новый набор фактов.
        Чтобы не смешивать состояния, сбрасываем staged-изменения.
        """
        if self._pending_edits or self._editor_dirty:
            if not messagebox.askyesno(
                "Факт",
                "Изменить дату/период и сбросить несохранённые изменения?",
                parent=self,
            ):
                return

        self._reload_existing_facts(clear_pending=True)

    def _reload_existing_facts_button(self):
        if self._pending_edits or self._editor_dirty:
            if not messagebox.askyesno(
                "Факт",
                "Перезагрузить факт и сбросить несохранённые изменения?",
                parent=self,
            ):
                return
        self._reload_existing_facts(clear_pending=True)

    def _reload_existing_facts(self, clear_pending: bool = False):
        try:
            self._loaded_fact_map = self._load_existing_facts()
        except Exception as e:
            logger.exception("TaskFactBatchDialog load existing facts error")
            messagebox.showwarning(
                "Факт",
                f"Не удалось загрузить существующий факт:\n{e}",
                parent=self,
            )
            self._loaded_fact_map = {}

        if clear_pending:
            self._pending_edits.clear()
            self._editor_dirty = False

        self._render_table()
        self._update_summary()
        self._restore_selection_after_render()

    # ─────────────────────────────────────────────────────
    # ACTIONS
    # ─────────────────────────────────────────────────────
    def _set_today(self):
        self.var_fact_date.set(_fmt_date(_today()))
        self._on_fact_params_changed()

    def _clear_inputs(self):
        self._pending_edits.clear()
        self._editor_dirty = False
        self._render_table()
        self._update_summary()
        self._restore_selection_after_render()

    def _clear_visible_inputs(self):
        visible_ids = [
            int(r["task_id"])
            for r in self._filtered_rows
            if r.get("row_kind") == "task" and r.get("task_id")
        ]
        for tid in visible_ids:
            self._pending_edits.pop(tid, None)
        self._editor_dirty = False
        self._render_table()
        self._update_summary()
        self._restore_selection_after_render()

    def _collect_data(self) -> Tuple[List[Dict[str, Any]], List[int]]:
        fact_date = _parse_date(self.var_fact_date.get())
        period_label = (self.cmb_period.get() or "").strip()
        period_type = FACT_PERIOD_FROM_LABEL.get(period_label, "day")
    
        upserts: List[Dict[str, Any]] = []
        delete_ids: List[int] = []
    
        for task_id, pending in self._pending_edits.items():
            task_id = int(task_id)
    
            if pending is None:
                delete_ids.append(task_id)
                continue
    
            pending = dict(pending)
            pending["task_id"] = task_id
            pending["fact_date"] = fact_date
            pending["period_type"] = period_type
            pending["comment"] = pending.get("comment") or None
            upserts.append(pending)
    
        return upserts, delete_ids

    def _save_batch(self, facts: List[Dict[str, Any]], delete_ids: List[int]):
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                if facts:
                    values = [
                        (
                            f["task_id"],
                            f["fact_date"],
                            f["period_type"],
                            f["fact_qty"],
                            f["workers_count"],
                            f.get("comment"),
                            self.user_id,
                        )
                        for f in facts
                    ]
                    execute_values(
                        cur,
                        """
                        INSERT INTO public.gpr_task_facts
                            (task_id, fact_date, period_type, fact_qty, workers_count, comment, created_by)
                        VALUES %s
                        ON CONFLICT (task_id, fact_date, period_type)
                        DO UPDATE SET
                            fact_qty = EXCLUDED.fact_qty,
                            workers_count = EXCLUDED.workers_count,
                            comment = EXCLUDED.comment
                        """,
                        values,
                    )

                if delete_ids:
                    fact_date = _parse_date(self.var_fact_date.get())
                    period_label = (self.cmb_period.get() or "").strip()
                    period_type = FACT_PERIOD_FROM_LABEL.get(period_label, "day")
                    cur.execute(
                        """
                        DELETE FROM public.gpr_task_facts
                        WHERE task_id = ANY(%s)
                          AND fact_date = %s
                          AND period_type = %s
                        """,
                        (delete_ids, fact_date, period_type),
                    )

        except Exception:
            logger.exception("TaskFactBatchDialog._save_batch error")
            raise
        finally:
            _release(conn)

    def _on_ok(self):
        try:
            if self._editor_dirty:
                res = messagebox.askyesnocancel(
                    "Факт",
                    "Сохранить изменения текущей строки?",
                    parent=self,
                )
                if res is None:
                    return
                if res:
                    if not self._apply_current_row():
                        return
                else:
                    self._editor_dirty = False

            facts, delete_ids = self._collect_data()

            if not facts and not delete_ids:
                if not messagebox.askyesno(
                    "Факт",
                    "Нет заполненных строк. Закрыть окно?",
                    parent=self,
                ):
                    return
                self.result = {"saved": False, "count": 0, "deleted": 0, "changed_task_ids": []}
                self._safe_destroy()
                return

            self._save_batch(facts, delete_ids)

            changed_ids = sorted(
                {int(f["task_id"]) for f in facts} | {int(x) for x in delete_ids}
            )

            self.result = {
                "saved": True,
                "count": len(facts),
                "deleted": len(delete_ids),
                "changed_task_ids": changed_ids,
                "fact_date": _parse_date(self.var_fact_date.get()),
                "period_type": FACT_PERIOD_FROM_LABEL.get((self.cmb_period.get() or "").strip(), "day"),
            }
            self._safe_destroy()

        except Exception as e:
            messagebox.showwarning("Массовое заполнение факта", str(e), parent=self)

    def _on_cancel(self):
        self.result = None
        self._safe_destroy()

    # ─────────────────────────────────────────────────────
    # MISC
    # ─────────────────────────────────────────────────────
    def _safe_destroy(self):
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
            sw = self.winfo_screenwidth()
            sh = self.winfo_screenheight()
            x = (sw - w) // 2
            y = (sh - h) // 2
        self.geometry(f"+{max(0, x)}+{max(0, y)}")
        
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
        parent,
        work_types,
        uoms,
        init=init,
        user_id=user_id,
    )
    parent.wait_window(dlg)
    return dlg.result

def open_task_fact_batch_dialog(
    parent,
    tasks: List[Dict[str, Any]],
    user_id: Optional[int] = None,
    fact_date: Optional[date] = None,
) -> Optional[Dict[str, Any]]:
    dlg = TaskFactBatchDialog(
        parent,
        tasks=tasks,
        user_id=user_id,
        fact_date=fact_date,
    )
    parent.wait_window(dlg)
    return dlg.result
