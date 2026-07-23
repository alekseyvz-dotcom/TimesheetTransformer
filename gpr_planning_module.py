from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable, Dict, List, Optional

import tkinter as tk
from tkinter import ttk, messagebox

from psycopg2.extras import RealDictCursor, execute_values

logger = logging.getLogger(__name__)

_db_pool = None

PERIOD_TYPE_WEEK = "week"

# Независимая палитра модуля.
# При желании позднее можно вынести общую тему в отдельный файл.
C = {
    "bg": "#edf1f5",
    "panel": "#f7f9fb",
    "text": "#1f2937",
    "text2": "#5b6776",
    "text3": "#7f8a98",
    "accent": "#2f74c0",
    "success": "#2f855a",
    "warning": "#c97a20",
    "error": "#c05656",
}


def set_db_pool(pool) -> None:
    """
    Вызывается из main_app.py после инициализации подключения к БД.
    """
    global _db_pool
    _db_pool = pool


def _conn():
    """Получает соединение из центрального пула приложения."""
    if _db_pool is None:
        raise RuntimeError(
            "Пул БД не передан в gpr_planning_module. "
            "Проверьте вызов set_db_pool()."
        )
    return _db_pool.getconn()


def _release(conn) -> None:
    """Возвращает соединение в пул."""
    if conn is not None and _db_pool is not None:
        _db_pool.putconn(conn)


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if isinstance(value, str):
        value = value.strip()

        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                pass

    return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None

    try:
        return float(str(value).replace(",", ".").replace(" ", ""))
    except (ValueError, TypeError):
        return None


def _fmt_date(value: Any) -> str:
    day = _as_date(value)
    return day.strftime("%d.%m.%Y") if day else "—"


def _fmt_qty(value: Any, digits: int = 3) -> str:
    number = _safe_float(value)

    if number is None:
        return "—"

    text = f"{number:.{digits}f}".rstrip("0").rstrip(".")
    return text.replace(".", ",")


def _round_qty(value: Any, digits: int = 3) -> float:
    number = _safe_float(value) or 0.0
    quant = Decimal("1").scaleb(-digits)

    return float(
        Decimal(str(number)).quantize(
            quant,
            rounding=ROUND_HALF_UP,
        )
    )


# ═══════════════════════════════════════════════════════════════
# Вспомогательные функции дат и чисел
# ═══════════════════════════════════════════════════════════════

def _as_date(value: Any) -> Optional[date]:
    """Безопасно приводит значение к date."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    try:
        return _to_date(value)
    except Exception:
        return None


def _round_qty(value: Optional[float], digits: int = 3) -> float:
    """Округление без типичных ошибок float."""
    if value is None:
        return 0.0

    quant = Decimal("1").scaleb(-digits)

    return float(
        Decimal(str(value)).quantize(
            quant,
            rounding=ROUND_HALF_UP,
        )
    )


def _monday(day: date) -> date:
    """Понедельник недели, в которой находится day."""
    return day - timedelta(days=day.weekday())


def _sunday(day: date) -> date:
    """Воскресенье недели, в которой находится day."""
    return _monday(day) + timedelta(days=6)


def _is_default_workday(day: date) -> bool:
    """
    Базовый календарь: понедельник–пятница рабочие.

    Позже можно заменить на проверку таблицы
    public.gpr_calendar_days.
    """
    return day.weekday() < 5


def _working_days_between(
    start: date,
    finish: date,
) -> List[date]:
    """Список рабочих дней в диапазоне включительно."""
    result: List[date] = []

    current = start
    while current <= finish:
        if _is_default_workday(current):
            result.append(current)
        current += timedelta(days=1)

    return result


# ═══════════════════════════════════════════════════════════════
# Сервис недельного планирования
# ═══════════════════════════════════════════════════════════════

class GprPlanningService:
    """
    Сервис работы с недельными планами задач ГПР.

    Важно:
    - Недельный план может быть автоматическим или ручным.
    - Автоматическая генерация не перезаписывает ручные строки,
      если force=False.
    - ЗТР рассчитывается по снимку нормы, сохранённому в gpr_tasks:
      labor_hours_per_unit * productivity_factor.
    """

    @staticmethod
    def load_period_plans(
        task_ids: List[int],
        period_type: str = PERIOD_TYPE_WEEK,
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Загружает планы и возвращает словарь:

        {
            task_id: [
                {...план недели 1...},
                {...план недели 2...},
            ]
        }
        """
        result: Dict[int, List[Dict[str, Any]]] = {}

        task_ids = [
            int(task_id)
            for task_id in task_ids
            if task_id
        ]

        if not task_ids:
            return result

        conn = None

        try:
            conn = _conn()

            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        task_id,
                        period_type,
                        period_start,
                        period_finish,
                        plan_qty,
                        plan_labor_hours,
                        planned_workers_count,
                        shift_hours,
                        is_manual,
                        COALESCE(comment, '') AS comment,
                        created_at,
                        updated_at
                    FROM public.gpr_task_period_plans
                    WHERE task_id = ANY(%s)
                      AND period_type = %s
                    ORDER BY task_id, period_start
                    """,
                    (task_ids, period_type),
                )

                for row in cur.fetchall():
                    item = dict(row)
                    task_id = int(item["task_id"])

                    item["period_start"] = _as_date(
                        item.get("period_start")
                    )
                    item["period_finish"] = _as_date(
                        item.get("period_finish")
                    )

                    result.setdefault(task_id, []).append(item)

            return result

        except Exception:
            logger.exception("Ошибка загрузки недельных планов")
            raise

        finally:
            _release(conn)

    @staticmethod
    def save_period_plans(
        rows: List[Dict[str, Any]],
    ) -> None:
        """
        Сохраняет строки недельного планирования.

        Ожидаемые поля:
        task_id, period_start, period_finish, plan_qty,
        plan_labor_hours, planned_workers_count,
        shift_hours, is_manual, comment.
        """
        if not rows:
            return

        values = []

        for row in rows:
            task_id = row.get("task_id")
            period_start = _as_date(row.get("period_start"))
            period_finish = _as_date(row.get("period_finish"))

            if not task_id or not period_start or not period_finish:
                continue

            workers = _safe_float(
                row.get("planned_workers_count")
            )

            if workers is not None:
                workers = int(workers)

            values.append(
                (
                    int(task_id),
                    PERIOD_TYPE_WEEK,
                    period_start,
                    period_finish,
                    _safe_float(row.get("plan_qty")),
                    _safe_float(row.get("plan_labor_hours")),
                    workers,
                    _safe_float(row.get("shift_hours")),
                    bool(row.get("is_manual")),
                    (row.get("comment") or "").strip() or None,
                )
            )

        if not values:
            return

        conn = None

        try:
            conn = _conn()

            with conn, conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO public.gpr_task_period_plans (
                        task_id,
                        period_type,
                        period_start,
                        period_finish,
                        plan_qty,
                        plan_labor_hours,
                        planned_workers_count,
                        shift_hours,
                        is_manual,
                        comment
                    )
                    VALUES %s
                    ON CONFLICT (task_id, period_type, period_start)
                    DO UPDATE SET
                        period_finish = EXCLUDED.period_finish,
                        plan_qty = EXCLUDED.plan_qty,
                        plan_labor_hours = EXCLUDED.plan_labor_hours,
                        planned_workers_count =
                            EXCLUDED.planned_workers_count,
                        shift_hours = EXCLUDED.shift_hours,
                        is_manual = EXCLUDED.is_manual,
                        comment = EXCLUDED.comment,
                        updated_at = now()
                    """,
                    values,
                )

        except Exception:
            logger.exception("Ошибка сохранения недельных планов")
            raise

        finally:
            _release(conn)

    @staticmethod
    def delete_period_plans(
        task_ids: List[int],
        period_type: str = PERIOD_TYPE_WEEK,
    ) -> None:
        """Удаляет недельный план указанных задач."""
        task_ids = [
            int(task_id)
            for task_id in task_ids
            if task_id
        ]

        if not task_ids:
            return

        conn = None

        try:
            conn = _conn()

            with conn, conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM public.gpr_task_period_plans
                    WHERE task_id = ANY(%s)
                      AND period_type = %s
                    """,
                    (task_ids, period_type),
                )

        except Exception:
            logger.exception("Ошибка удаления недельных планов")
            raise

        finally:
            _release(conn)

    @staticmethod
    def build_week_periods(
        plan_start: date,
        plan_finish: date,
    ) -> List[Dict[str, Any]]:
        """
        Создаёт периоды «понедельник–воскресенье», ограниченные
        реальными датами задачи.

        Например, задача 01.07–15.07 вернёт:
        01.07–05.07
        06.07–12.07
        13.07–15.07
        """
        if plan_finish < plan_start:
            return []

        periods: List[Dict[str, Any]] = []

        week_start = _monday(plan_start)

        while week_start <= plan_finish:
            week_finish = _sunday(week_start)

            actual_start = max(plan_start, week_start)
            actual_finish = min(plan_finish, week_finish)

            working_days = _working_days_between(
                actual_start,
                actual_finish,
            )

            periods.append(
                {
                    "period_start": actual_start,
                    "period_finish": actual_finish,
                    "working_days": len(working_days),
                    "week_start": week_start,
                    "week_finish": week_finish,
                }
            )

            week_start += timedelta(days=7)

        return periods

    @staticmethod
    def generate_task_week_plan(
        task: Dict[str, Any],
        existing_rows: Optional[List[Dict[str, Any]]] = None,
        force: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Создаёт автоматический недельный план одной задачи.

        force=False:
            существующие ручные строки сохраняются без изменений.

        force=True:
            все строки пересчитываются автоматически.
        """
        task_id = task.get("id")

        if not task_id:
            return []

        plan_start = _as_date(task.get("plan_start"))
        plan_finish = _as_date(task.get("plan_finish"))

        plan_qty = _safe_float(task.get("plan_qty"))

        if not plan_start or not plan_finish or plan_qty is None:
            return []

        if plan_finish < plan_start or plan_qty < 0:
            return []

        periods = GprPlanningService.build_week_periods(
            plan_start,
            plan_finish,
        )

        if not periods:
            return []

        existing_rows = existing_rows or {}

        if isinstance(existing_rows, list):
            existing_map = {
                _as_date(row.get("period_start")): row
                for row in existing_rows
                if _as_date(row.get("period_start"))
            }
        else:
            existing_map = existing_rows

        total_work_days = sum(
            row["working_days"]
            for row in periods
        )

        # Если у задачи нет рабочих дней (например, она только в выходные),
        # распределяем равномерно по недельным периодам.
        if total_work_days <= 0:
            total_weight = len(periods)
            weights = [1 for _ in periods]
        else:
            total_weight = total_work_days
            weights = [
                row["working_days"]
                for row in periods
            ]

        norm = _safe_float(
            task.get("labor_hours_per_unit")
        )
        factor = _safe_float(
            task.get("productivity_factor")
        ) or 1.0

        default_workers = _safe_float(
            task.get("planned_workers_count")
        )
        default_shift_hours = _safe_float(
            task.get("shift_hours")
        ) or 8.0

        output: List[Dict[str, Any]] = []
        distributed_qty = 0.0

        for index, period in enumerate(periods):
            period_start = period["period_start"]
            existing = existing_map.get(period_start)

            # Ручную строку не меняем при обычной автогенерации.
            if (
                existing
                and bool(existing.get("is_manual"))
                and not force
            ):
                row = dict(existing)
                row["task_id"] = int(task_id)
                row["period_type"] = PERIOD_TYPE_WEEK
                output.append(row)

                distributed_qty += (
                    _safe_float(row.get("plan_qty")) or 0.0
                )
                continue

            # На последнюю автоматически распределяемую неделю
            # отдаём остаток — так сумма не потеряет объём из-за округления.
            is_last = index == len(periods) - 1

            if is_last:
                week_qty = max(0.0, plan_qty - distributed_qty)
            else:
                weight = weights[index]
                week_qty = _round_qty(
                    plan_qty * weight / total_weight
                )

            distributed_qty += week_qty

            labor_hours = None

            if norm is not None:
                labor_hours = _round_qty(
                    week_qty * norm * factor
                )

            output.append(
                {
                    "task_id": int(task_id),
                    "period_type": PERIOD_TYPE_WEEK,
                    "period_start": period_start,
                    "period_finish": period["period_finish"],
                    "plan_qty": week_qty,
                    "plan_labor_hours": labor_hours,
                    "planned_workers_count": (
                        int(default_workers)
                        if default_workers is not None
                        else None
                    ),
                    "shift_hours": default_shift_hours,
                    "is_manual": False,
                    "comment": "",
                    "working_days": period["working_days"],
                }
            )

        return output

    @staticmethod
    def generate_all_week_plans(
        tasks: List[Dict[str, Any]],
        existing_plans: Optional[
            Dict[int, List[Dict[str, Any]]]
        ] = None,
        force: bool = False,
    ) -> List[Dict[str, Any]]:
        """Автоматически строит недельный план для списка задач."""
        output: List[Dict[str, Any]] = []
        existing_plans = existing_plans or {}

        for task in tasks:
            task_id = task.get("id")

            if not task_id:
                continue

            output.extend(
                GprPlanningService.generate_task_week_plan(
                    task=task,
                    existing_rows=existing_plans.get(
                        int(task_id),
                        [],
                    ),
                    force=force,
                )
            )

        return output


# ═══════════════════════════════════════════════════════════════
# UI: вкладка «Планирование»
# ═══════════════════════════════════════════════════════════════

class GprPlanningPanel(tk.Frame):
    """
    Вкладка недельного планирования.

    get_tasks_callback должен вернуть актуальный список задач текущего ГПР.
    В список можно передавать технические строки title/group — они будут
    проигнорированы. Рабочими считаются записи row_kind == 'task'
    или записи с заполненным id.
    """

    def __init__(
        self,
        parent,
        get_tasks_callback: Callable[[], List[Dict[str, Any]]],
        on_saved_callback: Optional[Callable[[], None]] = None,
    ):
        super().__init__(parent, bg=C["bg"])

        self.get_tasks_callback = get_tasks_callback
        self.on_saved_callback = on_saved_callback

        self._tasks: List[Dict[str, Any]] = []
        self._task_by_id: Dict[int, Dict[str, Any]] = {}
        self._plans_by_task: Dict[
            int,
            List[Dict[str, Any]]
        ] = {}

        self._selected_task_id: Optional[int] = None
        self._dirty = False

        self.var_workers = tk.StringVar()
        self.var_shift_hours = tk.StringVar(value="8")
        self.var_week_qty = tk.StringVar()
        self.var_comment = tk.StringVar()

        self._build_ui()

    # ─────────────────────────────────────────────────────
    # Построение интерфейса
    # ─────────────────────────────────────────────────────

    def _build_ui(self):
        header = tk.Frame(self, bg=C["accent"], pady=7)
        header.pack(fill="x")

        tk.Label(
            header,
            text="📅  Планирование работ по неделям",
            bg=C["accent"],
            fg="white",
            font=("Segoe UI", 11, "bold"),
            padx=12,
        ).pack(side="left")

        self.lbl_header_info = tk.Label(
            header,
            text="",
            bg=C["accent"],
            fg="#bbdefb",
            font=("Segoe UI", 8),
            padx=12,
        )
        self.lbl_header_info.pack(side="right")

        toolbar = tk.Frame(
            self,
            bg=C["panel"],
            padx=12,
            pady=8,
        )
        toolbar.pack(fill="x", padx=10, pady=(10, 6))

        ttk.Button(
            toolbar,
            text="🔄 Обновить задачи",
            command=self.reload,
        ).pack(side="left", padx=2)

        ttk.Button(
            toolbar,
            text="⚙ Сформировать план",
            command=self._generate_plan,
        ).pack(side="left", padx=2)

        ttk.Button(
            toolbar,
            text="♻ Пересчитать всё",
            command=self._regenerate_all,
        ).pack(side="left", padx=2)

        ttk.Button(
            toolbar,
            text="💾 Сохранить план",
            command=self._save,
        ).pack(side="left", padx=2)

        self.lbl_summary = tk.Label(
            toolbar,
            text="",
            bg=C["panel"],
            fg=C["text2"],
            font=("Segoe UI", 8),
        )
        self.lbl_summary.pack(side="right", padx=4)

        split = tk.PanedWindow(
            self,
            orient="horizontal",
            sashrelief="raised",
            bg=C["bg"],
        )
        split.pack(
            fill="both",
            expand=True,
            padx=10,
            pady=(0, 10),
        )

        left = tk.Frame(split, bg=C["panel"])
        right = tk.Frame(split, bg=C["panel"])

        split.add(left, minsize=430)
        split.add(right, minsize=600)

        self._build_task_list(left)
        self._build_week_editor(right)

    def _build_task_list(self, parent):
        box = tk.LabelFrame(
            parent,
            text=" Работы ГПР ",
            bg=C["panel"],
            fg=C["accent"],
            font=("Segoe UI", 9, "bold"),
            padx=8,
            pady=8,
        )
        box.pack(fill="both", expand=True, padx=8, pady=8)

        cols = (
            "name",
            "uom",
            "qty",
            "norm",
            "labor",
            "status",
        )

        self.task_tree = ttk.Treeview(
            box,
            columns=cols,
            show="headings",
            selectmode="browse",
        )

        columns = [
            ("name", "Работа", 250, "w"),
            ("uom", "Ед.", 50, "center"),
            ("qty", "Объём", 80, "e"),
            ("norm", "ЗТР", 80, "e"),
            ("labor", "Всего ч-ч", 90, "e"),
            ("status", "Статус", 110, "w"),
        ]

        for code, title, width, anchor in columns:
            self.task_tree.heading(code, text=title)
            self.task_tree.column(
                code,
                width=width,
                anchor=anchor,
            )

        vsb = ttk.Scrollbar(
            box,
            orient="vertical",
            command=self.task_tree.yview,
        )
        self.task_tree.configure(
            yscrollcommand=vsb.set,
        )

        self.task_tree.pack(
            side="left",
            fill="both",
            expand=True,
        )
        vsb.pack(side="right", fill="y")

        self.task_tree.bind(
            "<<TreeviewSelect>>",
            self._on_task_selected,
        )

    def _build_week_editor(self, parent):
        top = tk.LabelFrame(
            parent,
            text=" Недельный план выбранной работы ",
            bg=C["panel"],
            fg=C["accent"],
            font=("Segoe UI", 9, "bold"),
            padx=10,
            pady=8,
        )
        top.pack(fill="x", padx=8, pady=(8, 4))

        self.lbl_task_title = tk.Label(
            top,
            text="Выберите работу слева.",
            bg=C["panel"],
            fg=C["text2"],
            font=("Segoe UI", 10, "bold"),
            anchor="w",
            justify="left",
            wraplength=700,
        )
        self.lbl_task_title.pack(fill="x")

        self.lbl_task_meta = tk.Label(
            top,
            text="",
            bg=C["panel"],
            fg=C["text2"],
            font=("Segoe UI", 8),
            anchor="w",
            justify="left",
        )
        self.lbl_task_meta.pack(fill="x", pady=(6, 0))

        table_box = tk.LabelFrame(
            parent,
            text=" Распределение по неделям ",
            bg=C["panel"],
            fg=C["accent"],
            font=("Segoe UI", 9, "bold"),
            padx=8,
            pady=8,
        )
        table_box.pack(
            fill="both",
            expand=True,
            padx=8,
            pady=4,
        )

        cols = (
            "period",
            "work_days",
            "qty",
            "labor",
            "workers",
            "shift",
            "capacity",
            "balance",
            "manual",
            "comment",
        )

        self.week_tree = ttk.Treeview(
            table_box,
            columns=cols,
            show="headings",
            selectmode="browse",
            height=12,
        )

        columns = [
            ("period", "Неделя", 145, "w"),
            ("work_days", "Раб. дн.", 65, "center"),
            ("qty", "План объёма", 100, "e"),
            ("labor", "План ЗТР", 95, "e"),
            ("workers", "Людей", 65, "center"),
            ("shift", "Смена", 65, "center"),
            ("capacity", "Мощность", 90, "e"),
            ("balance", "Резерв", 85, "e"),
            ("manual", "Ручной", 60, "center"),
            ("comment", "Комментарий", 180, "w"),
        ]

        for code, title, width, anchor in columns:
            self.week_tree.heading(code, text=title)
            self.week_tree.column(
                code,
                width=width,
                anchor=anchor,
            )

        self.week_tree.tag_configure(
            "manual",
            background="#fff3e0",
        )
        self.week_tree.tag_configure(
            "overload",
            background="#ffebee",
        )
        self.week_tree.tag_configure(
            "normal",
            background="#ffffff",
        )

        vsb = ttk.Scrollbar(
            table_box,
            orient="vertical",
            command=self.week_tree.yview,
        )
        hsb = ttk.Scrollbar(
            table_box,
            orient="horizontal",
            command=self.week_tree.xview,
        )

        self.week_tree.configure(
            yscrollcommand=vsb.set,
            xscrollcommand=hsb.set,
        )

        self.week_tree.pack(
            side="left",
            fill="both",
            expand=True,
        )
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")

        self.week_tree.bind(
            "<<TreeviewSelect>>",
            self._on_week_selected,
        )

        editor = tk.LabelFrame(
            parent,
            text=" Корректировка недели ",
            bg=C["panel"],
            fg=C["accent"],
            font=("Segoe UI", 9, "bold"),
            padx=10,
            pady=8,
        )
        editor.pack(fill="x", padx=8, pady=(4, 8))

        tk.Label(
            editor,
            text="Плановый объём:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).grid(
            row=0,
            column=0,
            sticky="e",
            padx=(0, 6),
            pady=4,
        )

        self.ent_week_qty = ttk.Entry(
            editor,
            textvariable=self.var_week_qty,
            width=16,
        )
        self.ent_week_qty.grid(
            row=0,
            column=1,
            sticky="w",
            pady=4,
        )

        tk.Label(
            editor,
            text="Плановая бригада:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).grid(
            row=0,
            column=2,
            sticky="e",
            padx=(20, 6),
            pady=4,
        )

        self.ent_workers = ttk.Entry(
            editor,
            textvariable=self.var_workers,
            width=10,
        )
        self.ent_workers.grid(
            row=0,
            column=3,
            sticky="w",
            pady=4,
        )

        tk.Label(
            editor,
            text="Часов в смене:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).grid(
            row=0,
            column=4,
            sticky="e",
            padx=(20, 6),
            pady=4,
        )

        self.ent_shift_hours = ttk.Entry(
            editor,
            textvariable=self.var_shift_hours,
            width=10,
        )
        self.ent_shift_hours.grid(
            row=0,
            column=5,
            sticky="w",
            pady=4,
        )

        tk.Label(
            editor,
            text="Комментарий:",
            bg=C["panel"],
            font=("Segoe UI", 9),
        ).grid(
            row=1,
            column=0,
            sticky="e",
            padx=(0, 6),
            pady=4,
        )

        self.ent_comment = ttk.Entry(
            editor,
            textvariable=self.var_comment,
            width=70,
        )
        self.ent_comment.grid(
            row=1,
            column=1,
            columnspan=5,
            sticky="ew",
            pady=4,
        )

        editor.grid_columnconfigure(5, weight=1)

        buttons = tk.Frame(editor, bg=C["panel"])
        buttons.grid(
            row=2,
            column=0,
            columnspan=6,
            sticky="w",
            pady=(8, 0),
        )

        ttk.Button(
            buttons,
            text="Применить к неделе",
            command=self._apply_week_change,
        ).pack(side="left", padx=2)

        ttk.Button(
            buttons,
            text="Вернуть авторасчёт недели",
            command=self._restore_auto_for_selected_week,
        ).pack(side="left", padx=2)

        self.lbl_validation = tk.Label(
            editor,
            text="",
            bg=C["panel"],
            fg=C["text2"],
            font=("Segoe UI", 8),
        )
        self.lbl_validation.grid(
            row=3,
            column=0,
            columnspan=6,
            sticky="w",
            pady=(8, 0),
        )

    # ─────────────────────────────────────────────────────
    # Загрузка данных
    # ─────────────────────────────────────────────────────

    def reload(self):
        """Загружает задачи текущего ГПР и сохранённый недельный план."""
        previous_task_id = self._selected_task_id

        try:
            source_rows = self.get_tasks_callback() or []
        except Exception:
            logger.exception("Ошибка получения задач для планирования")
            messagebox.showerror(
                "Планирование",
                "Не удалось получить список работ ГПР.",
                parent=self,
            )
            return

        self._tasks = []

        for row in source_rows:
            row_kind = (row.get("row_kind") or "task").strip()

            if row_kind not in ("task", ""):
                continue

            if not row.get("id"):
                continue

            self._tasks.append(dict(row))

        self._task_by_id = {
            int(task["id"]): task
            for task in self._tasks
        }

        try:
            self._plans_by_task = (
                GprPlanningService.load_period_plans(
                    list(self._task_by_id.keys())
                )
            )
        except Exception as exc:
            messagebox.showerror(
                "Планирование",
                f"Не удалось загрузить недельные планы:\n{exc}",
                parent=self,
            )
            self._plans_by_task = {}

        self._render_tasks()

        if previous_task_id in self._task_by_id:
            self._select_task(previous_task_id)
        elif self._tasks:
            self._select_task(int(self._tasks[0]["id"]))
        else:
            self._selected_task_id = None
            self._clear_right_panel()

        self._dirty = False
        self._update_summary()

    # ─────────────────────────────────────────────────────
    # Отрисовка
    # ─────────────────────────────────────────────────────

    def _render_tasks(self):
        self.task_tree.delete(*self.task_tree.get_children())

        for task in self._tasks:
            task_id = int(task["id"])

            qty = _safe_float(task.get("plan_qty"))
            norm = _safe_float(
                task.get("labor_hours_per_unit")
            )
            factor = _safe_float(
                task.get("productivity_factor")
            ) or 1.0

            total_labor = None

            if qty is not None and norm is not None:
                total_labor = qty * norm * factor

            self.task_tree.insert(
                "",
                "end",
                iid=str(task_id),
                values=(
                    task.get("name") or "",
                    task.get("uom_code") or "",
                    _fmt_qty(qty),
                    _fmt_qty(norm),
                    _fmt_qty(total_labor),
                    task.get("status_label")
                    or task.get("status")
                    or "",
                ),
            )

    def _render_weeks(self):
        self.week_tree.delete(*self.week_tree.get_children())

        task_id = self._selected_task_id

        if not task_id:
            return

        for index, row in enumerate(
            self._plans_by_task.get(task_id, [])
        ):
            working_days = row.get("working_days")

            if working_days is None:
                working_days = len(
                    _working_days_between(
                        _as_date(row.get("period_start")),
                        _as_date(row.get("period_finish")),
                    )
                )

            workers = _safe_float(
                row.get("planned_workers_count")
            )
            shift_hours = _safe_float(
                row.get("shift_hours")
            ) or 8.0

            labor = _safe_float(
                row.get("plan_labor_hours")
            )

            capacity = None
            balance = None

            if workers is not None and workers > 0:
                capacity = workers * shift_hours * working_days

                if labor is not None:
                    balance = capacity - labor

            tag = "normal"

            if row.get("is_manual"):
                tag = "manual"

            if balance is not None and balance < 0:
                tag = "overload"

            self.week_tree.insert(
                "",
                "end",
                iid=str(index),
                values=(
                    (
                        f"{_fmt_date(row.get('period_start'))} — "
                        f"{_fmt_date(row.get('period_finish'))}"
                    ),
                    working_days,
                    _fmt_qty(row.get("plan_qty")),
                    _fmt_qty(labor),
                    (
                        int(workers)
                        if workers is not None
                        else ""
                    ),
                    _fmt_qty(shift_hours),
                    _fmt_qty(capacity),
                    _fmt_qty(balance),
                    "Да" if row.get("is_manual") else "",
                    row.get("comment") or "",
                ),
                tags=(tag,),
            )

        self._validate_selected_task_plan()

    # ─────────────────────────────────────────────────────
    # Выбор работы / недели
    # ─────────────────────────────────────────────────────

    def _on_task_selected(self, _event=None):
        selected = self.task_tree.selection()

        if not selected:
            return

        try:
            task_id = int(selected[0])
        except (ValueError, TypeError):
            return

        self._selected_task_id = task_id
        self._render_selected_task_info()
        self._render_weeks()
        self._clear_week_editor()

    def _select_task(self, task_id: int):
        iid = str(task_id)

        if not self.task_tree.exists(iid):
            return

        self.task_tree.selection_set(iid)
        self.task_tree.focus(iid)
        self.task_tree.see(iid)

        self._selected_task_id = task_id
        self._render_selected_task_info()
        self._render_weeks()
        self._clear_week_editor()

    def _on_week_selected(self, _event=None):
        selected = self.week_tree.selection()

        if not selected or self._selected_task_id is None:
            return

        try:
            index = int(selected[0])
        except (ValueError, TypeError):
            return

        rows = self._plans_by_task.get(
            self._selected_task_id,
            [],
        )

        if not (0 <= index < len(rows)):
            return

        row = rows[index]

        self.var_week_qty.set(
            _fmt_qty(row.get("plan_qty"))
        )

        workers = row.get("planned_workers_count")
        self.var_workers.set(
            str(int(workers))
            if workers is not None
            else ""
        )

        self.var_shift_hours.set(
            _fmt_qty(
                _safe_float(row.get("shift_hours")) or 8.0
            )
        )

        self.var_comment.set(row.get("comment") or "")

    # ─────────────────────────────────────────────────────
    # Автогенерация
    # ─────────────────────────────────────────────────────

    def _generate_plan(self):
        """
        Создаёт недельный план только там, где его ещё нет.
        Ручные строки не перезаписываются.
        """
        if not self._tasks:
            messagebox.showinfo(
                "Планирование",
                "В текущем ГПР нет работ для планирования.",
                parent=self,
            )
            return

        generated = (
            GprPlanningService.generate_all_week_plans(
                tasks=self._tasks,
                existing_plans=self._plans_by_task,
                force=False,
            )
        )

        grouped: Dict[int, List[Dict[str, Any]]] = {}

        for row in generated:
            grouped.setdefault(
                int(row["task_id"]),
                [],
            ).append(row)

        self._plans_by_task = grouped
        self._dirty = True

        self._render_weeks()
        self._update_summary()

        messagebox.showinfo(
            "Планирование",
            (
                "Недельный план сформирован.\n\n"
                "Проверьте объёмы и сохраните изменения."
            ),
            parent=self,
        )

    def _regenerate_all(self):
        """
        Полностью пересчитывает недельный план.
        Ручные корректировки будут потеряны.
        """
        if not self._tasks:
            return

        if not messagebox.askyesno(
            "Пересчитать план",
            (
                "Все недельные планы будут пересчитаны автоматически.\n"
                "Ручные корректировки будут заменены.\n\n"
                "Продолжить?"
            ),
            parent=self,
        ):
            return

        generated = (
            GprPlanningService.generate_all_week_plans(
                tasks=self._tasks,
                existing_plans={},
                force=True,
            )
        )

        grouped: Dict[int, List[Dict[str, Any]]] = {}

        for row in generated:
            grouped.setdefault(
                int(row["task_id"]),
                [],
            ).append(row)

        self._plans_by_task = grouped
        self._dirty = True

        self._render_weeks()
        self._update_summary()

    # ─────────────────────────────────────────────────────
    # Ручное редактирование недели
    # ─────────────────────────────────────────────────────

    def _apply_week_change(self):
        if self._selected_task_id is None:
            messagebox.showwarning(
                "Планирование",
                "Сначала выберите работу.",
                parent=self,
            )
            return

        selected = self.week_tree.selection()

        if not selected:
            messagebox.showwarning(
                "Планирование",
                "Выберите неделю для корректировки.",
                parent=self,
            )
            return

        try:
            index = int(selected[0])
        except (ValueError, TypeError):
            return

        rows = self._plans_by_task.get(
            self._selected_task_id,
            [],
        )

        if not (0 <= index < len(rows)):
            return

        qty = _safe_float(self.var_week_qty.get())

        if qty is None or qty < 0:
            messagebox.showwarning(
                "Планирование",
                "Введите корректный плановый объём.",
                parent=self,
            )
            return

        workers = _safe_float(self.var_workers.get())

        if workers is not None:
            if workers <= 0 or int(workers) != workers:
                messagebox.showwarning(
                    "Планирование",
                    (
                        "Количество работников должно быть "
                        "целым числом больше 0."
                    ),
                    parent=self,
                )
                return

            workers = int(workers)

        shift_hours = _safe_float(
            self.var_shift_hours.get()
        )

        if shift_hours is None or shift_hours <= 0:
            messagebox.showwarning(
                "Планирование",
                "Введите корректную продолжительность смены.",
                parent=self,
            )
            return

        task = self._task_by_id.get(
            self._selected_task_id,
            {},
        )

        norm = _safe_float(
            task.get("labor_hours_per_unit")
        )
        factor = _safe_float(
            task.get("productivity_factor")
        ) or 1.0

        labor_hours = None

        if norm is not None:
            labor_hours = _round_qty(
                qty * norm * factor
            )

        row = rows[index]
        row["plan_qty"] = _round_qty(qty)
        row["plan_labor_hours"] = labor_hours
        row["planned_workers_count"] = workers
        row["shift_hours"] = shift_hours
        row["comment"] = (
            self.var_comment.get() or ""
        ).strip()
        row["is_manual"] = True

        self._dirty = True

        self._render_weeks()
        self._select_week_index(index)
        self._update_summary()

    def _restore_auto_for_selected_week(self):
        """Возвращает автоматический расчёт только одной недели."""
        if self._selected_task_id is None:
            return

        selected = self.week_tree.selection()

        if not selected:
            messagebox.showinfo(
                "Планирование",
                "Выберите неделю.",
                parent=self,
            )
            return

        try:
            index = int(selected[0])
        except (ValueError, TypeError):
            return

        task = self._task_by_id.get(
            self._selected_task_id,
        )

        if not task:
            return

        old_rows = self._plans_by_task.get(
            self._selected_task_id,
            [],
        )

        auto_rows = (
            GprPlanningService.generate_task_week_plan(
                task=task,
                existing_rows={},
                force=True,
            )
        )

        if not (
            0 <= index < len(old_rows)
            and index < len(auto_rows)
        ):
            return

        old_rows[index] = auto_rows[index]

        self._dirty = True

        self._render_weeks()
        self._select_week_index(index)
        self._update_summary()

    def _select_week_index(self, index: int):
        iid = str(index)

        if self.week_tree.exists(iid):
            self.week_tree.selection_set(iid)
            self.week_tree.focus(iid)
            self.week_tree.see(iid)
            self._on_week_selected()

    # ─────────────────────────────────────────────────────
    # Проверка суммы недель
    # ─────────────────────────────────────────────────────

    def _validate_selected_task_plan(self):
        if self._selected_task_id is None:
            self.lbl_validation.config(text="")
            return

        task = self._task_by_id.get(
            self._selected_task_id,
            {},
        )

        task_qty = _safe_float(task.get("plan_qty"))

        rows = self._plans_by_task.get(
            self._selected_task_id,
            [],
        )

        if task_qty is None:
            self.lbl_validation.config(
                text=(
                    "У задачи не указан общий плановый объём."
                ),
                fg=C["warning"],
            )
            return

        if not rows:
            self.lbl_validation.config(
                text=(
                    "Недельный план ещё не сформирован."
                ),
                fg=C["text3"],
            )
            return

        weekly_qty = sum(
            _safe_float(row.get("plan_qty")) or 0.0
            for row in rows
        )

        difference = _round_qty(weekly_qty - task_qty)

        if abs(difference) <= 0.001:
            self.lbl_validation.config(
                text=(
                    f"✓ Сумма недель совпадает с объёмом задачи: "
                    f"{_fmt_qty(task_qty)}."
                ),
                fg=C["success"],
            )
        else:
            self.lbl_validation.config(
                text=(
                    f"⚠ Сумма недель: {_fmt_qty(weekly_qty)}; "
                    f"объём задачи: {_fmt_qty(task_qty)}; "
                    f"разница: {_fmt_qty(difference)}."
                ),
                fg=C["error"],
            )

    # ─────────────────────────────────────────────────────
    # Сохранение
    # ─────────────────────────────────────────────────────

    def _save(self):
        rows: List[Dict[str, Any]] = []

        for task_rows in self._plans_by_task.values():
            rows.extend(task_rows)

        if not rows:
            messagebox.showinfo(
                "Планирование",
                "Нет сформированных недельных планов.",
                parent=self,
            )
            return

        invalid_tasks = []

        for task_id, task_rows in self._plans_by_task.items():
            task = self._task_by_id.get(task_id, {})
            task_qty = _safe_float(task.get("plan_qty"))

            if task_qty is None:
                continue

            weekly_qty = sum(
                _safe_float(row.get("plan_qty")) or 0.0
                for row in task_rows
            )

            if abs(weekly_qty - task_qty) > 0.001:
                invalid_tasks.append(
                    task.get("name") or f"ID {task_id}"
                )

        if invalid_tasks:
            names = "\n".join(
                f"• {name}"
                for name in invalid_tasks[:10]
            )

            if not messagebox.askyesno(
                "Несовпадение объёмов",
                (
                    "Для части работ сумма недель не совпадает "
                    "с общим объёмом:\n\n"
                    f"{names}\n\n"
                    "Сохранить план несмотря на это?"
                ),
                parent=self,
            ):
                return

        try:
            GprPlanningService.save_period_plans(rows)

        except Exception as exc:
            messagebox.showerror(
                "Планирование",
                f"Не удалось сохранить недельный план:\n{exc}",
                parent=self,
            )
            return

        self._dirty = False
        self._update_summary()

        if self.on_saved_callback:
            try:
                self.on_saved_callback()
            except Exception:
                logger.exception(
                    "Ошибка callback после сохранения плана"
                )

        messagebox.showinfo(
            "Планирование",
            "Недельный план сохранён.",
            parent=self,
        )

    # ─────────────────────────────────────────────────────
    # Сводка и очистка
    # ─────────────────────────────────────────────────────

    def _update_summary(self):
        tasks_count = len(self._tasks)

        planned_tasks = sum(
            1
            for task_id in self._task_by_id
            if self._plans_by_task.get(task_id)
        )

        weeks_count = sum(
            len(rows)
            for rows in self._plans_by_task.values()
        )

        dirty_text = "  |  Есть несохранённые изменения" if self._dirty else ""

        self.lbl_summary.config(
            text=(
                f"Работ: {tasks_count}  |  "
                f"Запланировано: {planned_tasks}  |  "
                f"Недель: {weeks_count}"
                f"{dirty_text}"
            )
        )

        self.lbl_header_info.config(
            text=(
                "Распределение по рабочим дням "
                "(Пн–Пт)"
            )
        )

    def _render_selected_task_info(self):
        task = self._task_by_id.get(
            self._selected_task_id,
            {},
        )

        if not task:
            self._clear_right_panel()
            return

        qty = _safe_float(task.get("plan_qty"))
        norm = _safe_float(
            task.get("labor_hours_per_unit")
        )
        factor = _safe_float(
            task.get("productivity_factor")
        ) or 1.0

        total_labor = None

        if qty is not None and norm is not None:
            total_labor = qty * norm * factor

        self.lbl_task_title.config(
            text=task.get("name") or "Без наименования"
        )

        self.lbl_task_meta.config(
            text=(
                f"Срок: {_fmt_date(task.get('plan_start'))} — "
                f"{_fmt_date(task.get('plan_finish'))}\n"
                f"Объём: {_fmt_qty(qty)} "
                f"{task.get('uom_code') or ''}   |   "
                f"ЗТР: {_fmt_qty(norm)} чел.-ч/ед.   |   "
                f"Всего: {_fmt_qty(total_labor)} чел.-ч"
            )
        )

    def _clear_week_editor(self):
        self.var_week_qty.set("")
        self.var_workers.set("")
        self.var_shift_hours.set("8")
        self.var_comment.set("")

    def _clear_right_panel(self):
        self.lbl_task_title.config(
            text="Выберите работу слева."
        )
        self.lbl_task_meta.config(text="")
        self.week_tree.delete(*self.week_tree.get_children())
        self._clear_week_editor()
        self.lbl_validation.config(text="")
