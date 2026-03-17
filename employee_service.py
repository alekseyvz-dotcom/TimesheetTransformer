"""
employee_service.py
Сервис работы с сотрудниками для ГПР
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from psycopg2.extras import RealDictCursor

from gpr_db import get_conn, release_conn


class EmployeeService:
    """
    Загрузка сотрудников и управление назначениями задач ГПР
    """

    # ─────────────────────────────────────────────
    # SEARCH EMPLOYEES
    # ─────────────────────────────────────────────
    @staticmethod
    def search_employees(query: str = "", limit: int = 50) -> List[Dict[str, Any]]:

        conn = None

        try:
            conn = get_conn()

            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                if query.strip():

                    cur.execute("""
                        SELECT
                            e.id,
                            e.fio,
                            e.tbn,
                            COALESCE(e.position,'') AS position,
                            COALESCE(d.name,'') AS department
                        FROM public.employees e
                        LEFT JOIN public.departments d
                            ON d.id = e.department_id
                        WHERE e.is_fired = false
                          AND (
                                e.fio ILIKE %s
                             OR e.tbn ILIKE %s
                             OR e.position ILIKE %s
                          )
                        ORDER BY e.fio
                        LIMIT %s
                    """, (
                        f"%{query}%",
                        f"%{query}%",
                        f"%{query}%",
                        limit
                    ))

                else:

                    cur.execute("""
                        SELECT
                            e.id,
                            e.fio,
                            e.tbn,
                            COALESCE(e.position,'') AS position,
                            COALESCE(d.name,'') AS department
                        FROM public.employees e
                        LEFT JOIN public.departments d
                            ON d.id = e.department_id
                        WHERE e.is_fired = false
                        ORDER BY e.fio
                        LIMIT %s
                    """, (limit,))

                return [dict(r) for r in cur.fetchall()]

        finally:
            release_conn(conn)

    # ─────────────────────────────────────────────
    # LOAD ASSIGNMENTS
    # ─────────────────────────────────────────────
    @staticmethod
    def load_task_assignments(task_id: int) -> List[Dict[str, Any]]:

        if not task_id:
            return []

        conn = None

        try:
            conn = get_conn()

            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                cur.execute("""
                    SELECT
                        a.id AS assignment_id,
                        a.employee_id,
                        a.role_in_task,
                        a.note,

                        e.fio,
                        e.tbn,
                        COALESCE(e.position,'') AS position,
                        COALESCE(d.name,'') AS department

                    FROM public.gpr_task_assignments a

                    JOIN public.employees e
                        ON e.id = a.employee_id

                    LEFT JOIN public.departments d
                        ON d.id = e.department_id

                    WHERE a.task_id = %s
                    ORDER BY a.role_in_task, e.fio
                """, (task_id,))

                return [dict(r) for r in cur.fetchall()]

        finally:
            release_conn(conn)

    # ─────────────────────────────────────────────
    # SAVE ASSIGNMENTS
    # ─────────────────────────────────────────────
    @staticmethod
    def save_task_assignments(
        task_id: int,
        assignments: List[Dict[str, Any]],
        user_id: Optional[int] = None
    ) -> None:

        conn = None

        try:
            conn = get_conn()

            with conn, conn.cursor() as cur:

                # удаляем старые назначения
                cur.execute("""
                    DELETE FROM public.gpr_task_assignments
                    WHERE task_id = %s
                """, (task_id,))

                # вставляем новые
                for a in assignments:

                    cur.execute("""
                        INSERT INTO public.gpr_task_assignments
                        (
                            task_id,
                            employee_id,
                            role_in_task,
                            note,
                            assigned_by
                        )
                        VALUES (%s,%s,%s,%s,%s)
                    """, (
                        task_id,
                        a["employee_id"],
                        a.get("role_in_task", "executor"),
                        a.get("note"),
                        user_id
                    ))

        finally:
            release_conn(conn)
