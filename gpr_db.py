from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from psycopg2.extras import RealDictCursor

from gpr_common import (
    GprAssignment,
    GprPlanInfo,
    GprTask,
    assignment_from_db_row,
    coerce_to_datetime,
    normalize_spaces,
    normalize_status,
    normalize_task,
    normalize_tasks,
    safe_float,
    split_tasks_for_save,
    task_from_db_row,
)

logger = logging.getLogger(__name__)

db_connection_pool = None


# ═══════════════════════════════════════════════════════════════
#  DB POOL
# ═══════════════════════════════════════════════════════════════
def set_db_pool(pool):
    global db_connection_pool
    db_connection_pool = pool


def get_db_connection():
    if not db_connection_pool:
        raise RuntimeError("DB pool not set (gpr_db.set_db_pool)")
    return db_connection_pool.getconn()


def release_db_connection(conn):
    """
    ВАЖНО:
    перед возвратом в пул обязательно rollback,
    чтобы не отдавать соединение в состоянии idle in transaction.
    """
    if conn is None:
        return

    try:
        if not getattr(conn, "closed", True):
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        try:
            if db_connection_pool:
                db_connection_pool.putconn(conn)
            else:
                conn.close()
        except Exception:
            logger.exception("Ошибка возврата соединения в пул GPR")


@contextmanager
def db_cursor(dict_rows: bool = False) -> Iterator[tuple[Any, Any]]:
    conn = get_db_connection()
    try:
        factory = RealDictCursor if dict_rows else None
        with conn.cursor(cursor_factory=factory) as cur:
            yield conn, cur
        if not conn.closed:
            conn.commit()
    except Exception:
        try:
            if not conn.closed:
                conn.rollback()
        except Exception:
            pass
        raise
    finally:
        release_db_connection(conn)


# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════
_ADVISORY_LOCK_NAMESPACE = 240517  # любое стабильное число для lock-namespace


def _advisory_lock_object(cur, object_db_id: int) -> None:
    """
    Защита от гонки при создании current-плана.
    """
    cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (_ADVISORY_LOCK_NAMESPACE, int(object_db_id)))


def _load_plan_info_by_id_cur(cur, plan_id: int) -> Optional[GprPlanInfo]:
    cur.execute(
        """
        SELECT
            p.id,
            p.object_db_id,
            p.version_no,
            p.is_current,
            p.is_baseline,
            p.created_by,
            p.created_at,
            p.updated_at,
            COALESCE(u.full_name, '') AS creator_name
        FROM public.gpr_plans p
        LEFT JOIN public.app_users u ON u.id = p.created_by
        WHERE p.id = %s
        """,
        (int(plan_id),),
    )
    row = cur.fetchone()
    if not row:
        return None
    return GprPlanInfo(
        id=int(row["id"]),
        object_db_id=int(row["object_db_id"]),
        version_no=int(row.get("version_no") or 1),
        is_current=bool(row.get("is_current")),
        is_baseline=bool(row.get("is_baseline")),
        created_by=row.get("created_by"),
        creator_name=normalize_spaces(row.get("creator_name") or ""),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


def _load_assignments_for_task_ids_cur(cur, task_ids: Sequence[int]) -> Dict[int, List[GprAssignment]]:
    out: Dict[int, List[GprAssignment]] = {}
    task_ids = [int(x) for x in task_ids if x is not None]
    if not task_ids:
        return out

    cur.execute(
        """
        SELECT
            a.task_id,
            a.id AS assignment_id,
            a.employee_id,
            a.role_in_task,
            a.note,
            e.fio,
            COALESCE(e.tbn, '') AS tbn,
            COALESCE(e.position, '') AS position,
            COALESCE(d.name, '') AS department
        FROM public.gpr_task_assignments a
        JOIN public.employees e ON e.id = a.employee_id
        LEFT JOIN public.departments d ON d.id = e.department_id
        WHERE a.task_id = ANY(%s)
        ORDER BY a.task_id, a.role_in_task, e.fio
        """,
        (task_ids,),
    )

    for row in cur.fetchall():
        task_id = int(row["task_id"])
        out.setdefault(task_id, []).append(assignment_from_db_row(row))

    return out


def _load_plan_tasks_cur(cur, plan_id: int) -> List[GprTask]:
    cur.execute(
        """
        SELECT
            t.id,
            t.plan_id,
            t.parent_id,
            t.work_type_id,
            COALESCE(wt.name, '') AS work_type_name,
            t.name,
            t.uom_code,
            t.plan_qty,
            t.plan_start,
            t.plan_finish,
            t.status,
            t.sort_order,
            t.is_milestone,
            t.created_by,
            t.created_at,
            t.updated_at
        FROM public.gpr_tasks t
        LEFT JOIN public.gpr_work_types wt ON wt.id = t.work_type_id
        WHERE t.plan_id = %s
        ORDER BY t.sort_order, wt.name, t.name, t.id
        """,
        (int(plan_id),),
    )
    rows = cur.fetchall()
    if not rows:
        return []

    task_ids = [int(r["id"]) for r in rows if r.get("id") is not None]
    assignments_map = _load_assignments_for_task_ids_cur(cur, task_ids)

    tasks: List[GprTask] = []
    for row in rows:
        tid = int(row["id"])
        assignments = assignments_map.get(tid, [])
        tasks.append(task_from_db_row(row, assignments=assignments))
    return tasks


def _normalize_assignments_payload(assignments: Sequence[GprAssignment | Mapping[str, Any]]) -> List[GprAssignment]:
    out: List[GprAssignment] = []

    for item in assignments or []:
        if isinstance(item, GprAssignment):
            normalized = item.normalized_copy()
        else:
            normalized = GprAssignment(
                assignment_id=item.get("assignment_id"),
                employee_id=item.get("employee_id"),
                fio=item.get("fio") or "",
                tbn=item.get("tbn") or "",
                position=item.get("position") or "",
                department=item.get("department") or "",
                role_in_task=item.get("role_in_task") or "executor",
                note=item.get("note"),
            ).normalized_copy()

        out.append(normalized)

    # защита от дублей одного и того же employee_id на одной задаче
    seen_employee_ids: set[int] = set()
    seen_fio_tbn: set[tuple[str, str]] = set()

    for a in out:
        if a.employee_id is not None:
            if int(a.employee_id) in seen_employee_ids:
                raise RuntimeError(
                    f"Дублирующее назначение одного и того же сотрудника на задачу: {a.fio} ({a.tbn})"
                )
            seen_employee_ids.add(int(a.employee_id))
        else:
            key = (normalize_spaces(a.fio).lower(), normalize_spaces(a.tbn))
            if key in seen_fio_tbn and (key[0] or key[1]):
                raise RuntimeError(
                    f"Дублирующее назначение сотрудника на задачу: {a.fio} ({a.tbn})"
                )
            seen_fio_tbn.add(key)

    return out


def _replace_task_assignments_cur(
    cur,
    task_id: int,
    assignments: Sequence[GprAssignment | Mapping[str, Any]],
    user_id: Optional[int],
) -> None:
    normalized = _normalize_assignments_payload(assignments)

    cur.execute(
        "DELETE FROM public.gpr_task_assignments WHERE task_id = %s",
        (int(task_id),),
    )

    if not normalized:
        return

    for a in normalized:
        if a.employee_id is None:
            # Без employee_id в БД сохранять нельзя.
            # Лучше пропустить с логом, чем упасть молча или записать мусор.
            logger.warning(
                "Пропуск назначения без employee_id для task_id=%s: fio=%r tbn=%r",
                task_id,
                a.fio,
                a.tbn,
            )
            continue

        cur.execute(
            """
            INSERT INTO public.gpr_task_assignments
                (task_id, employee_id, role_in_task, note, assigned_by)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (task_id, employee_id)
            DO UPDATE SET
                role_in_task = EXCLUDED.role_in_task,
                note = EXCLUDED.note,
                assigned_by = EXCLUDED.assigned_by
            """,
            (
                int(task_id),
                int(a.employee_id),
                a.role_in_task,
                a.note or None,
                int(user_id) if user_id else None,
            ),
        )


def _load_tasks_with_facts_cur(cur, task_ids: Sequence[int]) -> List[Dict[str, Any]]:
    ids = [int(x) for x in task_ids if x is not None]
    if not ids:
        return []

    cur.execute(
        """
        SELECT
            t.id,
            t.name,
            COALESCE(SUM(f.fact_qty), 0) AS total_fact
        FROM public.gpr_tasks t
        JOIN public.gpr_task_facts f ON f.task_id = t.id
        WHERE t.id = ANY(%s)
        GROUP BY t.id, t.name
        ORDER BY t.name, t.id
        """,
        (ids,),
    )
    return [dict(r) for r in cur.fetchall()]


def _plan_row_locked_cur(cur, plan_id: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            p.id,
            p.object_db_id,
            p.version_no,
            p.is_current,
            p.is_baseline,
            p.created_by,
            p.created_at,
            p.updated_at
        FROM public.gpr_plans p
        WHERE p.id = %s
        FOR UPDATE
        """,
        (int(plan_id),),
    )
    row = cur.fetchone()
    return dict(row) if row else None


# ═══════════════════════════════════════════════════════════════
#  OBJECTS / DICTIONARIES
# ═══════════════════════════════════════════════════════════════
def load_objects_short() -> List[Dict[str, Any]]:
    with db_cursor(dict_rows=True) as (_conn, cur):
        cur.execute(
            """
            SELECT
                id,
                COALESCE(short_name, '') AS short_name,
                COALESCE(address, '') AS address,
                COALESCE(excel_id, '') AS excel_id,
                COALESCE(status, '') AS status
            FROM public.objects
            ORDER BY address, short_name, id
            """
        )
        return [dict(r) for r in cur.fetchall()]


def load_work_types() -> List[Dict[str, Any]]:
    with db_cursor(dict_rows=True) as (_conn, cur):
        cur.execute(
            """
            SELECT
                id,
                COALESCE(code, '') AS code,
                COALESCE(name, '') AS name,
                COALESCE(sort_order, 0) AS sort_order
            FROM public.gpr_work_types
            WHERE is_active = true
            ORDER BY sort_order, name, id
            """
        )
        return [dict(r) for r in cur.fetchall()]


def load_uoms() -> List[Dict[str, Any]]:
    with db_cursor(dict_rows=True) as (_conn, cur):
        cur.execute(
            """
            SELECT
                COALESCE(code, '') AS code,
                COALESCE(name, '') AS name
            FROM public.gpr_uom
            ORDER BY code
            """
        )
        return [dict(r) for r in cur.fetchall()]


def load_statuses() -> List[Dict[str, Any]]:
    with db_cursor(dict_rows=True) as (_conn, cur):
        try:
            cur.execute(
                """
                SELECT
                    COALESCE(code, '') AS code,
                    COALESCE(name, '') AS name
                FROM public.gpr_statuses
                ORDER BY code
                """
            )
            return [dict(r) for r in cur.fetchall()]
        except Exception:
            logger.exception("Не удалось загрузить gpr_statuses, будет использован fallback")
            return []


# ═══════════════════════════════════════════════════════════════
#  PLANS
# ═══════════════════════════════════════════════════════════════
def get_or_create_current_plan(object_db_id: int, user_id: Optional[int]) -> GprPlanInfo:
    with db_cursor(dict_rows=True) as (_conn, cur):
        _advisory_lock_object(cur, int(object_db_id))

        cur.execute(
            """
            SELECT
                p.id,
                p.object_db_id,
                p.version_no,
                p.is_current,
                p.is_baseline,
                p.created_by,
                p.created_at,
                p.updated_at,
                COALESCE(u.full_name, '') AS creator_name
            FROM public.gpr_plans p
            LEFT JOIN public.app_users u ON u.id = p.created_by
            WHERE p.object_db_id = %s AND p.is_current = true
            ORDER BY p.id DESC
            LIMIT 1
            """,
            (int(object_db_id),),
        )
        row = cur.fetchone()
        if row:
            return GprPlanInfo(
                id=int(row["id"]),
                object_db_id=int(row["object_db_id"]),
                version_no=int(row.get("version_no") or 1),
                is_current=bool(row.get("is_current")),
                is_baseline=bool(row.get("is_baseline")),
                created_by=row.get("created_by"),
                creator_name=normalize_spaces(row.get("creator_name") or ""),
                created_at=row.get("created_at"),
                updated_at=row.get("updated_at"),
            )

        cur.execute(
            """
            INSERT INTO public.gpr_plans
                (object_db_id, version_no, is_current, is_baseline, created_by)
            VALUES (%s, 1, true, false, %s)
            RETURNING id
            """,
            (int(object_db_id), int(user_id) if user_id else None),
        )
        plan_id = int(cur.fetchone()["id"])

        plan = _load_plan_info_by_id_cur(cur, plan_id)
        if not plan:
            raise RuntimeError("Не удалось создать текущий план ГПР.")
        return plan


def load_plan_info(plan_id: int) -> Optional[GprPlanInfo]:
    with db_cursor(dict_rows=True) as (_conn, cur):
        return _load_plan_info_by_id_cur(cur, int(plan_id))


# ═══════════════════════════════════════════════════════════════
#  TASKS / FACTS
# ═══════════════════════════════════════════════════════════════
def load_plan_tasks(plan_id: int) -> List[GprTask]:
    with db_cursor(dict_rows=True) as (_conn, cur):
        return _load_plan_tasks_cur(cur, int(plan_id))


def load_task_assignments(task_id: int) -> List[GprAssignment]:
    if not task_id:
        return []

    with db_cursor(dict_rows=True) as (_conn, cur):
        mapping = _load_assignments_for_task_ids_cur(cur, [int(task_id)])
        return mapping.get(int(task_id), [])


def load_task_facts_cumulative(task_ids: Sequence[int]) -> Dict[int, float]:
    ids = [int(x) for x in task_ids if x is not None]
    if not ids:
        return {}

    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT task_id, SUM(fact_qty) AS total
            FROM public.gpr_task_facts
            WHERE task_id = ANY(%s)
            GROUP BY task_id
            """,
            (ids,),
        )
        return {int(r[0]): float(r[1]) for r in cur.fetchall()}


def update_task_status(task_id: int, new_status: str) -> None:
    status = normalize_status(new_status)
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            UPDATE public.gpr_tasks
            SET status = %s, updated_at = now()
            WHERE id = %s
            """,
            (status, int(task_id)),
        )


def save_task_assignments(task_id: int, assignments: Sequence[GprAssignment | Mapping[str, Any]], user_id: Optional[int] = None) -> None:
    with db_cursor() as (_conn, cur):
        _replace_task_assignments_cur(cur, int(task_id), assignments, user_id)


def save_plan_tasks(
    plan_id: int,
    user_id: Optional[int],
    tasks: Sequence[GprTask | Mapping[str, Any]],
    *,
    expected_plan_updated_at: Optional[datetime] = None,
    forbid_delete_if_has_facts: bool = True,
) -> Dict[str, Any]:
    """
    Главный метод сохранения плана БЕЗ delete+insert всего списка.

    Логика:
    - существующие задачи: UPDATE
    - новые задачи: INSERT
    - удалённые задачи: DELETE
    - назначения сотрудников сохраняются вместе с задачей в той же транзакции
    - task_id существующих задач сохраняется
    """
    normalized_current = normalize_tasks(tasks)

    with db_cursor(dict_rows=True) as (_conn, cur):
        plan_row = _plan_row_locked_cur(cur, int(plan_id))
        if not plan_row:
            raise RuntimeError("План ГПР не найден.")

        current_updated_at = plan_row.get("updated_at")
        expected_dt = coerce_to_datetime(expected_plan_updated_at)
        if expected_dt is not None and current_updated_at is not None:
            if current_updated_at != expected_dt:
                raise RuntimeError(
                    "План был изменён другим пользователем.\n"
                    "Обновите данные и повторите сохранение."
                )

        existing_db_tasks = _load_plan_tasks_cur(cur, int(plan_id))
        diff = split_tasks_for_save(existing_db_tasks, normalized_current)

        # Перед сохранением убеждаемся, что у всех задач правильный plan_id
        for task in normalized_current:
            task.plan_id = int(plan_id)

        # 1) DELETE (только реально удалённые)
        delete_ids = [int(t.id) for t in diff["delete"] if t.id is not None]
        if delete_ids:
            if forbid_delete_if_has_facts:
                tasks_with_facts = _load_tasks_with_facts_cur(cur, delete_ids)
                if tasks_with_facts:
                    names = []
                    for item in tasks_with_facts[:10]:
                        nm = normalize_spaces(item.get("name") or f"id={item.get('id')}")
                        fact_total = safe_float(item.get("total_fact")) or 0.0
                        names.append(f"- {nm} (факт: {fact_total})")

                    msg = (
                        "Нельзя удалить задачи, по которым уже внесены фактические объёмы:\n\n"
                        + "\n".join(names)
                    )
                    if len(tasks_with_facts) > 10:
                        msg += f"\n\n... и ещё {len(tasks_with_facts) - 10}"
                    raise RuntimeError(msg)

            cur.execute(
                "DELETE FROM public.gpr_task_assignments WHERE task_id = ANY(%s)",
                (delete_ids,),
            )
            cur.execute(
                "DELETE FROM public.gpr_tasks WHERE id = ANY(%s)",
                (delete_ids,),
            )

        # 2) UPDATE существующих задач
        for task in diff["update"]:
            if task.id is None:
                continue

            cur.execute(
                """
                UPDATE public.gpr_tasks
                SET
                    parent_id = %s,
                    work_type_id = %s,
                    name = %s,
                    uom_code = %s,
                    plan_qty = %s,
                    plan_start = %s,
                    plan_finish = %s,
                    status = %s,
                    sort_order = %s,
                    is_milestone = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                (
                    int(task.parent_id) if task.parent_id is not None else None,
                    int(task.work_type_id) if task.work_type_id is not None else None,
                    task.name,
                    task.uom_code,
                    task.plan_qty,
                    task.plan_start,
                    task.plan_finish,
                    normalize_status(task.status),
                    int(task.sort_order or 0),
                    bool(task.is_milestone),
                    int(task.id),
                ),
            )
            _replace_task_assignments_cur(cur, int(task.id), task.assignments, user_id)

        # 3) INSERT новых задач
        inserted_map: Dict[str, int] = {}
        for task in diff["insert"]:
            cur.execute(
                """
                INSERT INTO public.gpr_tasks
                    (plan_id, parent_id, work_type_id, name, uom_code,
                     plan_qty, plan_start, plan_finish,
                     status, sort_order, is_milestone, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    int(plan_id),
                    int(task.parent_id) if task.parent_id is not None else None,
                    int(task.work_type_id) if task.work_type_id is not None else None,
                    task.name,
                    task.uom_code,
                    task.plan_qty,
                    task.plan_start,
                    task.plan_finish,
                    normalize_status(task.status),
                    int(task.sort_order or 0),
                    bool(task.is_milestone),
                    int(user_id) if user_id else None,
                ),
            )
            new_id = int(cur.fetchone()["id"])
            inserted_map[task.client_id] = new_id
            task.id = new_id
            task.plan_id = int(plan_id)

            _replace_task_assignments_cur(cur, new_id, task.assignments, user_id)

        # 4) Обновляем план
        cur.execute(
            """
            UPDATE public.gpr_plans
            SET updated_at = now()
            WHERE id = %s
            """,
            (int(plan_id),),
        )

        plan_info = _load_plan_info_by_id_cur(cur, int(plan_id))

    # После commit перечитываем фактическое состояние
    reloaded_tasks = load_plan_tasks(int(plan_id))
    facts = load_task_facts_cumulative([t.id for t in reloaded_tasks if t.id is not None])

    return {
        "plan_info": plan_info,
        "tasks": reloaded_tasks,
        "facts": facts,
        "inserted_id_map": inserted_map,
    }


# ═══════════════════════════════════════════════════════════════
#  TEMPLATES
# ═══════════════════════════════════════════════════════════════
def load_templates() -> List[Dict[str, Any]]:
    with db_cursor(dict_rows=True) as (_conn, cur):
        cur.execute(
            """
            SELECT id, COALESCE(name, '') AS name
            FROM public.gpr_templates
            WHERE is_active = true
            ORDER BY name, id
            """
        )
        return [dict(r) for r in cur.fetchall()]


def load_template_tasks(template_id: int) -> List[Dict[str, Any]]:
    with db_cursor(dict_rows=True) as (_conn, cur):
        cur.execute(
            """
            SELECT
                id,
                parent_id,
                work_type_id,
                COALESCE(name, '') AS name,
                uom_code,
                default_qty,
                is_milestone,
                COALESCE(sort_order, 0) AS sort_order
            FROM public.gpr_template_tasks
            WHERE template_id = %s
            ORDER BY sort_order, id
            """,
            (int(template_id),),
        )
        return [dict(r) for r in cur.fetchall()]


# ═══════════════════════════════════════════════════════════════
#  EMPLOYEES / ASSIGNMENTS
# ═══════════════════════════════════════════════════════════════
def load_all_employees() -> List[Tuple[str, str, str, str]]:
    """
    Возвращает [(fio, tbn, position, department), ...]
    """
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT
                e.fio,
                COALESCE(e.tbn, '') AS tbn,
                COALESCE(e.position, '') AS position,
                COALESCE(d.name, '') AS department
            FROM public.employees e
            LEFT JOIN public.departments d ON d.id = e.department_id
            WHERE COALESCE(e.is_fired, false) = false
            ORDER BY e.fio, e.tbn
            """
        )
        return [
            (
                normalize_spaces(r[0] or ""),
                normalize_spaces(r[1] or ""),
                normalize_spaces(r[2] or ""),
                normalize_spaces(r[3] or ""),
            )
            for r in cur.fetchall()
        ]


def find_employee_id(fio: str, tbn: str) -> Optional[int]:
    fio_norm = normalize_spaces(fio)
    tbn_norm = normalize_spaces(tbn)

    if not fio_norm and not tbn_norm:
        return None

    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT id
            FROM public.employees
            WHERE fio = %s
              AND COALESCE(tbn, '') = %s
            ORDER BY id
            LIMIT 1
            """,
            (fio_norm, tbn_norm),
        )
        row = cur.fetchone()
        return int(row[0]) if row else None


def search_employees(query: str = "", limit: int = 50) -> List[Dict[str, Any]]:
    q = normalize_spaces(query)

    with db_cursor(dict_rows=True) as (_conn, cur):
        if q:
            pattern = f"%{q}%"
            cur.execute(
                """
                SELECT
                    e.id,
                    e.fio,
                    COALESCE(e.tbn, '') AS tbn,
                    COALESCE(e.position, '') AS position,
                    COALESCE(d.name, '') AS department
                FROM public.employees e
                LEFT JOIN public.departments d ON d.id = e.department_id
                WHERE COALESCE(e.is_fired, false) = false
                  AND (
                        e.fio ILIKE %s
                     OR COALESCE(e.tbn, '') ILIKE %s
                     OR COALESCE(e.position, '') ILIKE %s
                  )
                ORDER BY e.fio, e.tbn
                LIMIT %s
                """,
                (pattern, pattern, pattern, int(limit)),
            )
        else:
            cur.execute(
                """
                SELECT
                    e.id,
                    e.fio,
                    COALESCE(e.tbn, '') AS tbn,
                    COALESCE(e.position, '') AS position,
                    COALESCE(d.name, '') AS department
                FROM public.employees e
                LEFT JOIN public.departments d ON d.id = e.department_id
                WHERE COALESCE(e.is_fired, false) = false
                ORDER BY e.fio, e.tbn
                LIMIT %s
                """,
                (int(limit),),
            )
        return [dict(r) for r in cur.fetchall()]


# ═══════════════════════════════════════════════════════════════
#  SERVICE WRAPPERS (удобно для совместимости и main UI)
# ═══════════════════════════════════════════════════════════════
class GprService:
    @staticmethod
    def load_objects_short() -> List[Dict[str, Any]]:
        return load_objects_short()

    @staticmethod
    def load_work_types() -> List[Dict[str, Any]]:
        return load_work_types()

    @staticmethod
    def load_uoms() -> List[Dict[str, Any]]:
        return load_uoms()

    @staticmethod
    def load_statuses() -> List[Dict[str, Any]]:
        return load_statuses()

    @staticmethod
    def get_or_create_current_plan(object_db_id: int, user_id: Optional[int]) -> GprPlanInfo:
        return get_or_create_current_plan(object_db_id, user_id)

    @staticmethod
    def load_plan_info(plan_id: int) -> Optional[GprPlanInfo]:
        return load_plan_info(plan_id)

    @staticmethod
    def load_plan_tasks(plan_id: int) -> List[GprTask]:
        return load_plan_tasks(plan_id)

    @staticmethod
    def load_task_facts_cumulative(task_ids: Sequence[int]) -> Dict[int, float]:
        return load_task_facts_cumulative(task_ids)

    @staticmethod
    def save_plan_tasks(
        plan_id: int,
        user_id: Optional[int],
        tasks: Sequence[GprTask | Mapping[str, Any]],
        *,
        expected_plan_updated_at: Optional[datetime] = None,
        forbid_delete_if_has_facts: bool = True,
    ) -> Dict[str, Any]:
        return save_plan_tasks(
            plan_id=plan_id,
            user_id=user_id,
            tasks=tasks,
            expected_plan_updated_at=expected_plan_updated_at,
            forbid_delete_if_has_facts=forbid_delete_if_has_facts,
        )

    @staticmethod
    def update_task_status(task_id: int, new_status: str) -> None:
        update_task_status(task_id, new_status)

    @staticmethod
    def load_templates() -> List[Dict[str, Any]]:
        return load_templates()

    @staticmethod
    def load_template_tasks(template_id: int) -> List[Dict[str, Any]]:
        return load_template_tasks(template_id)


class EmployeeService:
    @staticmethod
    def load_all_employees() -> List[Tuple[str, str, str, str]]:
        return load_all_employees()

    @staticmethod
    def find_employee_id(fio: str, tbn: str) -> Optional[int]:
        return find_employee_id(fio, tbn)

    @staticmethod
    def search_employees(query: str = "", limit: int = 50) -> List[Dict[str, Any]]:
        return search_employees(query=query, limit=limit)

    @staticmethod
    def load_task_assignments(task_id: int) -> List[GprAssignment]:
        return load_task_assignments(task_id)

    @staticmethod
    def save_task_assignments(
        task_id: int,
        assignments: Sequence[GprAssignment | Mapping[str, Any]],
        user_id: Optional[int] = None,
    ) -> None:
        save_task_assignments(task_id, assignments, user_id)


__all__ = [
    "set_db_pool",
    "get_db_connection",
    "release_db_connection",
    "db_cursor",
    "load_objects_short",
    "load_work_types",
    "load_uoms",
    "load_statuses",
    "get_or_create_current_plan",
    "load_plan_info",
    "load_plan_tasks",
    "load_task_assignments",
    "load_task_facts_cumulative",
    "update_task_status",
    "save_task_assignments",
    "save_plan_tasks",
    "load_templates",
    "load_template_tasks",
    "load_all_employees",
    "find_employee_id",
    "search_employees",
    "GprService",
    "EmployeeService",
]
