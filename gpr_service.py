"""
gpr_service.py
Сервисный слой ГПР (работа с БД)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from psycopg2.extras import RealDictCursor

from gpr_db import get_conn, release_conn


class GprService:

    # ─────────────────────────────────────────────
    # OBJECTS
    # ─────────────────────────────────────────────
    @staticmethod
    def load_objects_short() -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = get_conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id,
                           COALESCE(short_name,'') AS short_name,
                           address,
                           COALESCE(excel_id,'') AS excel_id,
                           COALESCE(status,'') AS status
                    FROM public.objects
                    ORDER BY address, short_name
                """)
                return [dict(r) for r in cur.fetchall()]
        finally:
            release_conn(conn)

    # ─────────────────────────────────────────────
    # DICTIONARIES
    # ─────────────────────────────────────────────
    @staticmethod
    def load_work_types() -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = get_conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, COALESCE(code,'') AS code, name
                    FROM public.gpr_work_types
                    WHERE is_active = true
                    ORDER BY sort_order, name
                """)
                return [dict(r) for r in cur.fetchall()]
        finally:
            release_conn(conn)

    @staticmethod
    def load_uoms() -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = get_conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT code, name
                    FROM public.gpr_uom
                    ORDER BY code
                """)
                return [dict(r) for r in cur.fetchall()]
        finally:
            release_conn(conn)

    # ─────────────────────────────────────────────
    # PLANS
    # ─────────────────────────────────────────────
    @staticmethod
    def get_or_create_current_plan(object_db_id: int,
                                   user_id: Optional[int]) -> Dict[str, Any]:

        conn = None
        try:
            conn = get_conn()

            with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:

                cur.execute("""
                    SELECT p.*, u.full_name AS creator_name
                    FROM public.gpr_plans p
                    LEFT JOIN public.app_users u ON u.id = p.created_by
                    WHERE p.object_db_id = %s
                      AND p.is_current = true
                """, (object_db_id,))

                row = cur.fetchone()

                if row:
                    return dict(row)

                cur.execute("""
                    INSERT INTO public.gpr_plans
                        (object_db_id, version_no, is_current,
                         is_baseline, created_by)
                    VALUES (%s, 1, true, false, %s)
                    RETURNING id
                """, (object_db_id, user_id))

                pid = cur.fetchone()["id"]

                cur.execute("""
                    SELECT p.*, u.full_name AS creator_name
                    FROM public.gpr_plans p
                    LEFT JOIN public.app_users u ON u.id = p.created_by
                    WHERE p.id = %s
                """, (pid,))

                return dict(cur.fetchone())

        finally:
            release_conn(conn)

    # ─────────────────────────────────────────────
    # TASKS
    # ─────────────────────────────────────────────
    @staticmethod
    def load_plan_tasks(plan_id: int) -> List[Dict[str, Any]]:

        conn = None
        try:
            conn = get_conn()

            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                cur.execute("""
                    SELECT t.id,
                           t.parent_id,
                           t.work_type_id,
                           wt.name AS work_type_name,
                           t.name,
                           t.uom_code,
                           t.plan_qty,
                           t.plan_start,
                           t.plan_finish,
                           t.status,
                           t.sort_order,
                           t.is_milestone
                    FROM public.gpr_tasks t
                    JOIN public.gpr_work_types wt
                      ON wt.id = t.work_type_id
                    WHERE t.plan_id = %s
                    ORDER BY t.sort_order, t.id
                """, (plan_id,))

                return [dict(r) for r in cur.fetchall()]

        finally:
            release_conn(conn)

    # ─────────────────────────────────────────────
    # FACTS
    # ─────────────────────────────────────────────
    @staticmethod
    def load_task_facts_cumulative(task_ids: List[int]) -> Dict[int, float]:

        if not task_ids:
            return {}

        conn = None
        try:
            conn = get_conn()

            with conn.cursor() as cur:

                cur.execute("""
                    SELECT task_id, SUM(fact_qty)
                    FROM public.gpr_task_facts
                    WHERE task_id = ANY(%s)
                    GROUP BY task_id
                """, (task_ids,))

                return {r[0]: float(r[1]) for r in cur.fetchall()}

        finally:
            release_conn(conn)

    # ─────────────────────────────────────────────
    # SAVE TASKS (УЛУЧШЕННАЯ ЛОГИКА)
    # ─────────────────────────────────────────────
    @staticmethod
    def save_plan_tasks(plan_id: int,
                        user_id: Optional[int],
                        tasks: List[Dict[str, Any]]) -> None:
        """
        Сохраняет задачи без удаления всех записей.
        """

        conn = None

        try:
            conn = get_conn()

            with conn, conn.cursor() as cur:

                # текущие задачи в БД
                cur.execute("""
                    SELECT id
                    FROM public.gpr_tasks
                    WHERE plan_id = %s
                """, (plan_id,))

                existing_ids = {r[0] for r in cur.fetchall()}
                incoming_ids = {
                    t["id"] for t in tasks if t.get("id")
                }

                # удалить отсутствующие
                to_delete = existing_ids - incoming_ids

                if to_delete:
                    cur.execute("""
                        DELETE FROM public.gpr_tasks
                        WHERE id = ANY(%s)
                    """, (list(to_delete),))

                # insert / update
                for i, t in enumerate(tasks):

                    if t.get("id"):

                        cur.execute("""
                            UPDATE public.gpr_tasks
                            SET work_type_id=%s,
                                name=%s,
                                uom_code=%s,
                                plan_qty=%s,
                                plan_start=%s,
                                plan_finish=%s,
                                status=%s,
                                sort_order=%s,
                                is_milestone=%s,
                                updated_at=now()
                            WHERE id=%s
                        """, (
                            t["work_type_id"],
                            t["name"],
                            t.get("uom_code"),
                            t.get("plan_qty"),
                            t["plan_start"],
                            t["plan_finish"],
                            t.get("status", "planned"),
                            t.get("sort_order", i),
                            t.get("is_milestone", False),
                            t["id"],
                        ))

                    else:

                        cur.execute("""
                            INSERT INTO public.gpr_tasks
                            (
                                plan_id,
                                work_type_id,
                                name,
                                uom_code,
                                plan_qty,
                                plan_start,
                                plan_finish,
                                status,
                                sort_order,
                                is_milestone,
                                created_by
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (
                            plan_id,
                            t["work_type_id"],
                            t["name"],
                            t.get("uom_code"),
                            t.get("plan_qty"),
                            t["plan_start"],
                            t["plan_finish"],
                            t.get("status", "planned"),
                            t.get("sort_order", i),
                            t.get("is_milestone", False),
                            user_id,
                        ))

                cur.execute("""
                    UPDATE public.gpr_plans
                    SET updated_at = now()
                    WHERE id = %s
                """, (plan_id,))

        finally:
            release_conn(conn)

    # ─────────────────────────────────────────────
    # TEMPLATES
    # ─────────────────────────────────────────────
    @staticmethod
    def load_templates() -> List[Dict[str, Any]]:

        conn = None
        try:
            conn = get_conn()

            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                cur.execute("""
                    SELECT id, name
                    FROM public.gpr_templates
                    WHERE is_active = true
                    ORDER BY name
                """)

                return [dict(r) for r in cur.fetchall()]

        finally:
            release_conn(conn)

    @staticmethod
    def load_template_tasks(template_id: int) -> List[Dict[str, Any]]:

        conn = None

        try:
            conn = get_conn()

            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                cur.execute("""
                    SELECT id,
                           parent_id,
                           work_type_id,
                           name,
                           uom_code,
                           default_qty,
                           is_milestone,
                           sort_order
                    FROM public.gpr_template_tasks
                    WHERE template_id = %s
                    ORDER BY sort_order, id
                """, (template_id,))

                return [dict(r) for r in cur.fetchall()]

        finally:
            release_conn(conn)
