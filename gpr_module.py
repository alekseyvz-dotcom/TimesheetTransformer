# gpr_module.py  — профессиональный модуль ГПР v3 (bugfix + perf)
from __future__ import annotations

import sys
import logging
import calendar
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set
from pathlib import Path

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

try:
    from openpyxl import Workbook, load_workbook
    from openpyxl.utils import get_column_letter
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  COLORS / THEME
# ═══════════════════════════════════════════════════════════════
C = {
    "bg": "#f0f2f5",
    "panel": "#ffffff",
    "accent": "#1565c0",
    "accent_light": "#e3f2fd",
    "success": "#2e7d32",
    "warning": "#ed6c02",
    "error": "#d32f2f",
    "border": "#dde1e7",
    "text": "#1a1a2e",
    "text2": "#555",
    "text3": "#999",
    "btn_bg": "#1565c0",
    "btn_fg": "#ffffff",
}

STATUS_COLORS = {
    "planned": ("#90caf9", "#1565c0", "Запланировано"),
    "in_progress": ("#ffcc80", "#e65100", "В работе"),
    "done": ("#a5d6a7", "#1b5e20", "Выполнено"),
    "paused": ("#fff176", "#f9a825", "Приостановлено"),
    "canceled": ("#ef9a9a", "#b71c1c", "Отменено"),
}

STATUS_LIST = ["planned", "in_progress", "done", "paused", "canceled"]
STATUS_LABELS = {k: v[2] for k, v in STATUS_COLORS.items()}

# Обратное отображение: label → code
_STATUS_LABEL_TO_CODE = {v[2]: k for k, v in STATUS_COLORS.items()}


# ═══════════════════════════════════════════════════════════════
#  DB POOL  (с context-manager для безопасности)
# ═══════════════════════════════════════════════════════════════
db_connection_pool = None


def set_db_pool(pool):
    global db_connection_pool
    db_connection_pool = pool


class _DBConn:
    """Context-manager: гарантированный возврат соединения в пул."""

    def __init__(self):
        self.conn = None

    def __enter__(self):
        if not db_connection_pool:
            raise RuntimeError("DB pool not set (gpr_module.set_db_pool)")
        self.conn = db_connection_pool.getconn()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if db_connection_pool and self.conn:
            if exc_type is not None:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
            db_connection_pool.putconn(self.conn)
            self.conn = None
        return False  # не подавляем исключения

# ═══════════════════════════════════════════════════════════════
#  ОБРАТНАЯ СОВМЕСТИМОСТЬ: _conn / _release
#  Используются в gpr_task_dialog.py, gpr_dictionaries.py и др.
# ═══════════════════════════════════════════════════════════════
def _conn():
    """Получить соединение из пула (legacy API).
    
    ВНИМАНИЕ: вызывающий код ОБЯЗАН вызвать _release(conn)
    в finally-блоке. Для нового кода используйте _DBConn().
    """
    if not db_connection_pool:
        raise RuntimeError("DB pool not set (gpr_module.set_db_pool)")
    return db_connection_pool.getconn()


def _release(conn):
    """Вернуть соединение в пул (legacy API)."""
    if db_connection_pool and conn:
        try:
            db_connection_pool.putconn(conn)
        except Exception:
            logger.exception("Error releasing DB connection")

# ═══════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════
def _parse_date(s: str) -> date:
    """Парсит дату из строки дд.мм.гггг"""
    s = s.strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Неверный формат даты: '{s}' (ожидается дд.мм.гггг)")


def _to_date(d) -> Optional[date]:
    """Безопасное приведение к date."""
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str) and d.strip():
        try:
            return _parse_date(d)
        except ValueError:
            try:
                return datetime.fromisoformat(d).date()
            except Exception:
                return None
    return None


def _fmt_date(d) -> str:
    dt = _to_date(d) if not isinstance(d, date) else d
    if isinstance(dt, date):
        return dt.strftime("%d.%m.%Y")
    return str(d or "")


def _today() -> date:
    return datetime.now().date()


def _quarter_range() -> Tuple[date, date]:
    t = _today()
    q_start_month = ((t.month - 1) // 3) * 3 + 1
    d0 = date(t.year, q_start_month, 1)
    end_month = q_start_month + 2
    d1 = date(t.year, end_month, calendar.monthrange(t.year, end_month)[1])
    return d0, d1


def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


def _fmt_qty(v) -> str:
    f = _safe_float(v)
    if f is None:
        return ""
    return f"{f:.3f}".rstrip("0").rstrip(".")


def _mouse_delta(event) -> int:
    """Кроссплатформенный расчёт направления колёсика мыши."""
    if event.delta:
        # Windows/macOS
        return -1 if event.delta > 0 else 1
    # Linux: event.num == 4 (up) / 5 (down)
    if hasattr(event, 'num'):
        return -1 if event.num == 4 else 1
    return 0

class GprExcelImportService:
    REQUIRED_HEADERS = {
        "type": "Тип работ",
        "name": "Вид работ",
        "uom": "Ед. изм.",
        "qty": "Объём план",
        "start": "Начало",
        "finish": "Окончание",
        "status": "Статус",
    }

    @staticmethod
    def _norm(s: Any) -> str:
        return " ".join(str(s or "").strip().lower().split())

    @staticmethod
    def _build_work_type_map(work_types: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        out = {}
        for w in work_types:
            name = GprExcelImportService._norm(w.get("name"))
            code = GprExcelImportService._norm(w.get("code"))
            if name:
                out[name] = w
            if code:
                out[code] = w
        return out

    @staticmethod
    def _build_uom_map(uoms: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        out = {}
        for u in uoms:
            code = GprExcelImportService._norm(u.get("code"))
            name = GprExcelImportService._norm(u.get("name"))
            pair = GprExcelImportService._norm(f"{u.get('code', '')} — {u.get('name', '')}")
            if code:
                out[code] = u
            if name:
                out[name] = u
            if pair:
                out[pair] = u
        return out

    @staticmethod
    def _build_status_map() -> Dict[str, str]:
        out = {}
        for code in STATUS_LIST:
            out[GprExcelImportService._norm(code)] = code
            out[GprExcelImportService._norm(STATUS_LABELS.get(code, code))] = code
        return out

    @staticmethod
    def _find_header_row(ws) -> Tuple[int, Dict[str, int]]:
        for row_idx in range(1, min(ws.max_row, 20) + 1):
            row_values = [ws.cell(row_idx, c).value for c in range(1, ws.max_column + 1)]
            normalized = {
                GprExcelImportService._norm(v): i
                for i, v in enumerate(row_values, start=1)
                if str(v or "").strip()
            }

            found = {}
            ok = True
            for key, title in GprExcelImportService.REQUIRED_HEADERS.items():
                col = normalized.get(GprExcelImportService._norm(title))
                if not col:
                    ok = False
                    break
                found[key] = col

            if ok:
                return row_idx, found

        raise ValueError("Не найден заголовок таблицы на листе 'ГПР'")

    @staticmethod
    def import_tasks_from_excel(path: str, work_types: List[Dict[str, Any]], uoms: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not HAS_OPENPYXL:
            raise RuntimeError("Для импорта необходима библиотека openpyxl")

        wb = load_workbook(path, data_only=True)
        if "ГПР" not in wb.sheetnames:
            raise ValueError("В файле отсутствует лист 'ГПР'")

        ws = wb["ГПР"]

        header_row, cols = GprExcelImportService._find_header_row(ws)
        wt_map = GprExcelImportService._build_work_type_map(work_types)
        uom_map = GprExcelImportService._build_uom_map(uoms)
        status_map = GprExcelImportService._build_status_map()

        tasks = []
        errors = []

        for row_idx in range(header_row + 1, ws.max_row + 1):
            raw_type = ws.cell(row_idx, cols["type"]).value
            raw_name = ws.cell(row_idx, cols["name"]).value
            raw_uom = ws.cell(row_idx, cols["uom"]).value
            raw_qty = ws.cell(row_idx, cols["qty"]).value
            raw_start = ws.cell(row_idx, cols["start"]).value
            raw_finish = ws.cell(row_idx, cols["finish"]).value
            raw_status = ws.cell(row_idx, cols["status"]).value

            if all(not str(v or "").strip() for v in [raw_type, raw_name, raw_uom, raw_qty, raw_start, raw_finish, raw_status]):
                continue

            type_text = str(raw_type or "").strip()
            name_text = str(raw_name or "").strip()
            
            if not name_text:
                errors.append(f"Строка {row_idx}: пустое поле 'Вид работ'")
                continue
            
            type_norm = GprExcelImportService._norm(type_text)
            
            if type_norm in ("группа", "group"):
                tasks.append({
                    "id": None,
                    "row_kind": "group",
                    "work_type_id": int(work_types[0]["id"]) if work_types else 1,
                    "work_type_name": "",
                    "name": name_text,
                    "uom_code": None,
                    "plan_qty": None,
                    "plan_start": _today(),
                    "plan_finish": _today(),
                    "status": "planned",
                    "is_milestone": False,
                    "sort_order": len(tasks) * 10,
                })
                continue
            
            if type_norm in ("титул", "title"):
                tasks.append({
                    "id": None,
                    "row_kind": "title",
                    "work_type_id": int(work_types[0]["id"]) if work_types else 1,
                    "work_type_name": "",
                    "name": name_text,
                    "uom_code": None,
                    "plan_qty": None,
                    "plan_start": _today(),
                    "plan_finish": _today(),
                    "status": "planned",
                    "is_milestone": False,
                    "sort_order": len(tasks) * 10,
                })
                continue

            wt = wt_map.get(type_norm)
            if not wt:
                errors.append(f"Строка {row_idx}: тип работ '{type_text}' не найден в справочнике")
                continue

            uom_code = None
            if str(raw_uom or "").strip():
                uom = uom_map.get(GprExcelImportService._norm(raw_uom))
                if not uom:
                    errors.append(f"Строка {row_idx}: единица измерения '{raw_uom}' не найдена в справочнике")
                    continue
                uom_code = uom["code"]

            qty = _safe_float(raw_qty) if raw_qty not in (None, "") else None

            ds = _to_date(raw_start)
            df = _to_date(raw_finish)

            if not ds:
                errors.append(f"Строка {row_idx}: неверная дата начала '{raw_start}'")
                continue
            if not df:
                errors.append(f"Строка {row_idx}: неверная дата окончания '{raw_finish}'")
                continue
            if df < ds:
                errors.append(f"Строка {row_idx}: окончание раньше начала")
                continue

            status = "planned"
            if str(raw_status or "").strip():
                status = status_map.get(GprExcelImportService._norm(raw_status))
                if not status:
                    errors.append(f"Строка {row_idx}: неизвестный статус '{raw_status}'")
                    continue

            tasks.append({
                "id": None,
                "row_kind": "task",
                "work_type_id": int(wt["id"]),
                "work_type_name": wt["name"],
                "name": name_text,
                "uom_code": uom_code,
                "plan_qty": qty,
                "plan_start": ds,
                "plan_finish": df,
                "status": status,
                "is_milestone": False,
                "sort_order": len(tasks) * 10,
            })

        return {
            "tasks": tasks,
            "errors": errors,
            "count": len(tasks),
        }

# ═══════════════════════════════════════════════════════════════
#  SERVICE LAYER
# ═══════════════════════════════════════════════════════════════
class GprService:

    # ── objects ──
    @staticmethod
    def load_objects_short() -> List[Dict[str, Any]]:
        with _DBConn() as conn:
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

    # ── dictionaries ──
    @staticmethod
    def load_work_types() -> List[Dict[str, Any]]:
        with _DBConn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, COALESCE(code,'') AS code, name
                    FROM public.gpr_work_types WHERE is_active=true
                    ORDER BY sort_order, name
                """)
                return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def load_uoms() -> List[Dict[str, Any]]:
        with _DBConn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT code, name FROM public.gpr_uom ORDER BY code")
                return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def load_statuses() -> List[Dict[str, Any]]:
        with _DBConn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT code, name FROM public.gpr_statuses ORDER BY code"
                )
                return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def load_gpr_registry() -> List[Dict[str, Any]]:
        with _DBConn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        o.id AS object_db_id,
                        COALESCE(o.excel_id, '') AS excel_id,
                        COALESCE(o.short_name, '') AS short_name,
                        o.address,
                        COALESCE(o.status, '') AS object_status,
    
                        p.id AS plan_id,
                        p.version_no,
                        p.updated_at,
                        COALESCE(u.full_name, '') AS creator_name,
    
                        COALESCE(s.task_count, 0) AS task_count,
                        COALESCE(s.done_count, 0) AS done_count,
                        COALESCE(s.in_progress_count, 0) AS in_progress_count,
                        COALESCE(s.overdue_count, 0) AS overdue_count
    
                    FROM public.objects o
                    LEFT JOIN public.gpr_plans p
                           ON p.object_db_id = o.id
                          AND p.is_current = true
                    LEFT JOIN public.app_users u
                           ON u.id = p.created_by
                    LEFT JOIN (
                        SELECT
                            t.plan_id,
                            COUNT(*) FILTER (
                                WHERE COALESCE(t.is_deleted, false) = false
                                  AND COALESCE(t.row_kind, 'task') = 'task'
                            ) AS task_count,
                            COUNT(*) FILTER (
                                WHERE COALESCE(t.is_deleted, false) = false
                                  AND COALESCE(t.row_kind, 'task') = 'task'
                                  AND t.status = 'done'
                            ) AS done_count,
                            COUNT(*) FILTER (
                                WHERE COALESCE(t.is_deleted, false) = false
                                  AND COALESCE(t.row_kind, 'task') = 'task'
                                  AND t.status = 'in_progress'
                            ) AS in_progress_count,
                            COUNT(*) FILTER (
                                WHERE COALESCE(t.is_deleted, false) = false
                                  AND COALESCE(t.row_kind, 'task') = 'task'
                                  AND t.status NOT IN ('done', 'canceled')
                                  AND t.plan_finish < CURRENT_DATE
                            ) AS overdue_count
                        FROM public.gpr_tasks t
                        GROUP BY t.plan_id
                    ) s ON s.plan_id = p.id
                    ORDER BY o.address, o.short_name
                    """
                )
                return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def load_task_fact_info(task_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        if not task_ids:
            return {}
    
        with _DBConn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH qty AS (
                        SELECT
                            task_id,
                            COALESCE(SUM(fact_qty), 0) AS fact_qty_total
                        FROM public.gpr_task_facts
                        WHERE task_id = ANY(%s)
                        GROUP BY task_id
                    ),
                    last_fact AS (
                        SELECT DISTINCT ON (task_id)
                            task_id,
                            workers_count,
                            fact_date,
                            id
                        FROM public.gpr_task_facts
                        WHERE task_id = ANY(%s)
                        ORDER BY task_id, fact_date DESC, id DESC
                    ),
                    agg AS (
                        SELECT
                            task_id,
                            MAX(workers_count) AS workers_max,
                            COALESCE(SUM(workers_count), 0) AS workers_sum
                        FROM public.gpr_task_facts
                        WHERE task_id = ANY(%s)
                        GROUP BY task_id
                    )
                    SELECT
                        q.task_id,
                        q.fact_qty_total,
                        lf.workers_count AS workers_last,
                        a.workers_max,
                        a.workers_sum
                    FROM qty q
                    LEFT JOIN last_fact lf ON lf.task_id = q.task_id
                    LEFT JOIN agg a ON a.task_id = q.task_id
                    """
                    ,
                    (task_ids, task_ids, task_ids),
                )
    
                out = {}
                for r in cur.fetchall():
                    d = dict(r)
                    out[int(d["task_id"])] = {
                        "fact_qty_total": float(d.get("fact_qty_total") or 0),
                        "workers_last": int(d["workers_last"]) if d.get("workers_last") is not None else None,
                        "workers_max": int(d["workers_max"]) if d.get("workers_max") is not None else None,
                        "workers_sum": int(d["workers_sum"]) if d.get("workers_sum") is not None else 0,
                    }
                return out

    # ── plans ──
    @staticmethod
    def get_or_create_current_plan(
        object_db_id: int, user_id: Optional[int]
    ) -> Dict[str, Any]:
        with _DBConn() as conn:
            with conn:  # autocommit-block
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT p.*, u.full_name AS creator_name
                        FROM public.gpr_plans p
                        LEFT JOIN public.app_users u ON u.id = p.created_by
                        WHERE p.object_db_id=%s AND p.is_current=true
                        LIMIT 1
                    """,
                        (object_db_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        return dict(row)

                    cur.execute(
                        """
                        INSERT INTO public.gpr_plans
                            (object_db_id, version_no, is_current,
                             is_baseline, created_by)
                        VALUES (%s, 1, true, false, %s)
                        RETURNING id
                    """,
                        (object_db_id, user_id),
                    )
                    pid = cur.fetchone()["id"]

                    cur.execute(
                        """
                        SELECT p.*, u.full_name AS creator_name
                        FROM public.gpr_plans p
                        LEFT JOIN public.app_users u ON u.id = p.created_by
                        WHERE p.id=%s
                    """,
                        (pid,),
                    )
                    return dict(cur.fetchone())

    @staticmethod
    def load_plan_tasks(plan_id: int) -> List[Dict[str, Any]]:
        with _DBConn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                        SELECT t.id, t.parent_id, t.work_type_id,
                               wt.name AS work_type_name,
                               t.name, t.uom_code, t.plan_qty,
                               t.plan_start, t.plan_finish,
                               t.status, t.sort_order, t.is_milestone,
                               t.row_kind,
                               t.created_by, t.created_at, t.updated_at
                        FROM public.gpr_tasks t
                        JOIN public.gpr_work_types wt ON wt.id = t.work_type_id
                        WHERE t.plan_id = %s
                          AND COALESCE(t.is_deleted, false) = false
                        ORDER BY t.sort_order, wt.sort_order, wt.name,
                                 t.name, t.plan_start, t.id
                    """,
                    (plan_id,),
                )
                rows = []
                for r in cur.fetchall():
                    d = dict(r)
                    d["row_kind"] = (d.get("row_kind") or "task").strip()
                    d["plan_start"] = _to_date(d.get("plan_start"))
                    d["plan_finish"] = _to_date(d.get("plan_finish"))
                    rows.append(d)
                return rows

    @staticmethod
    def load_task_facts_cumulative(task_ids: List[int]) -> Dict[int, float]:
        info = GprService.load_task_fact_info(task_ids)
        return {
            task_id: float(v.get("fact_qty_total") or 0)
            for task_id, v in info.items()
        }

    @staticmethod
    def replace_plan_tasks(
        plan_id: int, user_id: Optional[int], tasks: List[Dict[str, Any]]
    ) -> None:

        with _DBConn() as conn:
            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Все задачи плана (включая уже архивные)
                    cur.execute(
                        """
                        SELECT id, COALESCE(is_deleted, false) AS is_deleted
                        FROM public.gpr_tasks
                        WHERE plan_id = %s
                        """,
                        (plan_id,),
                    )
                    existing_rows = cur.fetchall()
                    existing_ids: Set[int] = {int(r["id"]) for r in existing_rows}
                    existing_active_ids: Set[int] = {
                        int(r["id"]) for r in existing_rows if not r["is_deleted"]
                    }
    
                    seen_ids: Set[int] = set()
                    inserts: List[Tuple[Any, ...]] = []
    
                    for i, t in enumerate(tasks):
                        row_kind = (t.get("row_kind") or "task").strip()
                        if row_kind not in ("task", "group", "title"):
                            row_kind = "task"
    
                        name = (t.get("name") or "").strip()
                        if not name:
                            raise ValueError(f"Строка {i + 1}: пустое название")
    
                        work_type_id = t.get("work_type_id")
                        if work_type_id is None:
                            raise ValueError(f"Строка {i + 1} '{name}': не указан тип работ")
    
                        try:
                            work_type_id = int(work_type_id)
                        except (ValueError, TypeError):
                            raise ValueError(f"Строка {i + 1} '{name}': неверный work_type_id")
    
                        parent_id = t.get("parent_id")
                        if parent_id in ("", 0):
                            parent_id = None
                        elif parent_id is not None:
                            try:
                                parent_id = int(parent_id)
                            except (ValueError, TypeError):
                                parent_id = None
    
                        ps = _to_date(t.get("plan_start"))
                        pf = _to_date(t.get("plan_finish"))
    
                        if row_kind == "task":
                            if not ps or not pf:
                                raise ValueError(f"Задача '{name}': невалидные даты")
                            if pf < ps:
                                raise ValueError(f"Задача '{name}': окончание раньше начала")
                        else:
                            # Для group/title держим технические даты,
                            # чтобы не ломать текущую схему БД.
                            if not ps:
                                ps = _today()
                            if not pf:
                                pf = ps
    
                        status = (t.get("status") or "planned").strip()
                        if status not in STATUS_LIST:
                            status = "planned"
    
                        sort_order = int(
                            t.get("sort_order")
                            if t.get("sort_order") is not None
                            else i * 10
                        )
    
                        is_milestone = bool(t.get("is_milestone") or False)
                        uom_code = t.get("uom_code") or None
                        plan_qty = t.get("plan_qty")
    
                        task_id = t.get("id")
                        if task_id is not None:
                            try:
                                task_id = int(task_id)
                            except (ValueError, TypeError):
                                task_id = None
    
                        # ── update existing ──
                        if task_id and task_id in existing_ids:
                            cur.execute(
                                """
                                UPDATE public.gpr_tasks
                                   SET parent_id=%s,
                                       work_type_id=%s,
                                       name=%s,
                                       uom_code=%s,
                                       plan_qty=%s,
                                       plan_start=%s,
                                       plan_finish=%s,
                                       status=%s,
                                       sort_order=%s,
                                       is_milestone=%s,
                                       row_kind=%s,
                                       is_deleted=false,
                                       deleted_at=NULL,
                                       deleted_by=NULL
                                 WHERE id=%s
                                   AND plan_id=%s
                                """,
                                (
                                    parent_id,
                                    work_type_id,
                                    name,
                                    uom_code,
                                    plan_qty,
                                    ps,
                                    pf,
                                    status,
                                    sort_order,
                                    is_milestone,
                                    row_kind,
                                    task_id,
                                    plan_id,
                                ),
                            )
                            seen_ids.add(task_id)
                        else:
                            # ── insert new ──
                            inserts.append(
                                (
                                    plan_id,
                                    work_type_id,
                                    parent_id,
                                    name,
                                    uom_code,
                                    plan_qty,
                                    ps,
                                    pf,
                                    status,
                                    is_milestone,
                                    sort_order,
                                    user_id,
                                    row_kind,
                                )
                            )
    
                    if inserts:
                        execute_values(
                            cur,
                            """
                            INSERT INTO public.gpr_tasks
                            (plan_id, work_type_id, parent_id, name, uom_code, plan_qty,
                             plan_start, plan_finish, status, is_milestone,
                             sort_order, created_by, row_kind)
                            VALUES %s
                            """,
                            inserts,
                        )
    
                    # Всё, что было активным, но не пришло в новом списке — архивируем
                    ids_to_soft_delete = list(existing_active_ids - seen_ids)
                    if ids_to_soft_delete:
                        cur.execute(
                            """
                            UPDATE public.gpr_tasks
                               SET is_deleted=true,
                                   deleted_at=now(),
                                   deleted_by=%s,
                                   status=CASE
                                       WHEN status IN ('planned', 'in_progress', 'paused')
                                       THEN 'canceled'
                                       ELSE status
                                   END
                             WHERE id = ANY(%s)
                               AND plan_id=%s
                            """,
                            (user_id, ids_to_soft_delete, plan_id),
                        )
    
                    cur.execute(
                        "UPDATE public.gpr_plans SET updated_at=now() WHERE id=%s",
                        (plan_id,),
                    )

    @staticmethod
    def update_task_status(task_id: int, new_status: str) -> None:
        if new_status not in STATUS_LIST:
            raise ValueError(f"Неизвестный статус: {new_status}")
        with _DBConn() as conn:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE public.gpr_tasks
                        SET status=%s, updated_at=now()
                        WHERE id=%s
                          AND COALESCE(is_deleted, false)=false
                        """,
                        (new_status, task_id),
                    )

    # ── templates ──
    @staticmethod
    def load_templates() -> List[Dict[str, Any]]:
        with _DBConn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, name FROM public.gpr_templates
                    WHERE is_active=true ORDER BY name
                """
                )
                return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def load_template_tasks(template_id: int) -> List[Dict[str, Any]]:
        with _DBConn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                        SELECT id, parent_id, work_type_id, name, uom_code,
                               default_qty, is_milestone, sort_order, row_kind
                        FROM public.gpr_template_tasks
                        WHERE template_id=%s ORDER BY sort_order, id
                    """,
                    (template_id,),
                )
                return [dict(r) for r in cur.fetchall()]


# ═══════════════════════════════════════════════════════════════
#  AUTOCOMPLETE COMBOBOX (исправлен)
# ═══════════════════════════════════════════════════════════════
class _AutoCombo(ttk.Combobox):
    """Combobox с автодополнением. Хранит полный список и
    отображение label→index для корректного получения выбранного
    элемента даже после фильтрации."""

    # Клавиши, которые НЕ должны вызывать фильтрацию
    _IGNORE_KEYS = {
        "Return", "Escape", "Tab", "Up", "Down",
        "Left", "Right", "Home", "End",
        "Shift_L", "Shift_R", "Control_L", "Control_R",
        "Alt_L", "Alt_R", "Caps_Lock",
    }

    def __init__(self, master=None, **kw):
        super().__init__(master, **kw)
        self._all: List[str] = []
        self._label_to_idx: Dict[str, int] = {}
        self.bind("<KeyRelease>", self._on_key)

    def set_values(self, vals: List[str]):
        self._all = list(vals or [])
        self._label_to_idx = {v: i for i, v in enumerate(self._all)}
        self.config(values=self._all)

    def get_original_index(self) -> int:
        """Возвращает индекс в ОРИГИНАЛЬНОМ списке, -1 если не найден."""
        text = self.get().strip()
        return self._label_to_idx.get(text, -1)

    def _on_key(self, event):
        if event.keysym in self._IGNORE_KEYS:
            return
        q = self.get().strip().lower()
        if not q:
            self.config(values=self._all)
            return
        filtered = [v for v in self._all if q in v.lower()]
        self.config(values=filtered)


# ═══════════════════════════════════════════════════════════════
#  DIALOGS
# ═══════════════════════════════════════════════════════════════
class DateRangeDialog(simpledialog.Dialog):
    def __init__(self, parent, d0: date, d1: date):
        self._d0, self._d1 = d0, d1
        self.result: Optional[Tuple[date, date]] = None
        super().__init__(parent, title="Диапазон дат отображения")

    def body(self, m):
        tk.Label(m, text="С (дд.мм.гггг):").grid(
            row=0, column=0, sticky="e", padx=(0, 6), pady=4
        )
        self.e0 = ttk.Entry(m, width=14)
        self.e0.grid(row=0, column=1, pady=4)
        self.e0.insert(0, _fmt_date(self._d0))

        tk.Label(m, text="По (дд.мм.гггг):").grid(
            row=1, column=0, sticky="e", padx=(0, 6), pady=4
        )
        self.e1 = ttk.Entry(m, width=14)
        self.e1.grid(row=1, column=1, pady=4)
        self.e1.insert(0, _fmt_date(self._d1))

        ttk.Button(m, text="Текущий квартал", command=self._set_quarter).grid(
            row=2, column=0, columnspan=2, pady=(8, 0)
        )
        return self.e0

    def _set_quarter(self):
        d0, d1 = _quarter_range()
        self.e0.delete(0, "end")
        self.e0.insert(0, _fmt_date(d0))
        self.e1.delete(0, "end")
        self.e1.insert(0, _fmt_date(d1))

    def validate(self):
        try:
            a = _parse_date(self.e0.get())
            b = _parse_date(self.e1.get())
            if b < a:
                raise ValueError("Дата окончания раньше даты начала")
            self._a, self._b = a, b
            return True
        except Exception as e:
            messagebox.showwarning("Даты", str(e), parent=self)
            return False

    def apply(self):
        self.result = (self._a, self._b)


class TaskEditDialog(simpledialog.Dialog):
    """Встроенный диалог редактирования задачи (fallback если нет
    внешнего gpr_task_dialog)."""

    def __init__(self, parent, wt, uoms, statuses_db=None, init=None):
        self.wt = wt
        self.uoms = uoms
        self.init = init or {}
        self._statuses_db = statuses_db or []
        self.result: Optional[Dict[str, Any]] = None
        super().__init__(parent, title="Работа ГПР")

    def body(self, m):
        wt_v = [w["name"] for w in self.wt]
        uom_v = [f"{u['code']} — {u['name']}" for u in self.uoms]
        st_v = [STATUS_LABELS.get(s, s) for s in STATUS_LIST]

        r = 0
        tk.Label(m, text="Тип работ *:").grid(
            row=r, column=0, sticky="e", padx=(0, 6), pady=3
        )
        self.cmb_wt = ttk.Combobox(
            m, state="readonly", width=42, values=wt_v
        )
        self.cmb_wt.grid(row=r, column=1, pady=3)
        r += 1

        tk.Label(m, text="Вид работ *:").grid(
            row=r, column=0, sticky="e", padx=(0, 6), pady=3
        )
        self.ent_name = ttk.Entry(m, width=46)
        self.ent_name.grid(row=r, column=1, pady=3)
        r += 1

        tk.Label(m, text="Ед. изм.:").grid(
            row=r, column=0, sticky="e", padx=(0, 6), pady=3
        )
        self.cmb_uom = ttk.Combobox(
            m, state="readonly", width=42, values=["—"] + uom_v
        )
        self.cmb_uom.grid(row=r, column=1, pady=3)
        r += 1

        tk.Label(m, text="Объём план:").grid(
            row=r, column=0, sticky="e", padx=(0, 6), pady=3
        )
        self.ent_qty = ttk.Entry(m, width=18)
        self.ent_qty.grid(row=r, column=1, sticky="w", pady=3)
        r += 1

        tk.Label(m, text="Начало *:").grid(
            row=r, column=0, sticky="e", padx=(0, 6), pady=3
        )
        self.ent_s = ttk.Entry(m, width=14)
        self.ent_s.grid(row=r, column=1, sticky="w", pady=3)
        r += 1

        tk.Label(m, text="Окончание *:").grid(
            row=r, column=0, sticky="e", padx=(0, 6), pady=3
        )
        self.ent_f = ttk.Entry(m, width=14)
        self.ent_f.grid(row=r, column=1, sticky="w", pady=3)
        r += 1

        tk.Label(m, text="Статус:").grid(
            row=r, column=0, sticky="e", padx=(0, 6), pady=3
        )
        self.cmb_st = ttk.Combobox(
            m, state="readonly", width=20, values=st_v
        )
        self.cmb_st.grid(row=r, column=1, sticky="w", pady=3)
        r += 1

        self.var_ms = tk.BooleanVar(
            value=bool(self.init.get("is_milestone"))
        )
        ttk.Checkbutton(
            m, text="Веха (milestone)", variable=self.var_ms
        ).grid(row=r, column=1, sticky="w", pady=3)

        # ── заполняем начальные значения ──
        iw = self.init.get("work_type_id")
        if iw:
            for i, w in enumerate(self.wt):
                if int(w["id"]) == int(iw):
                    self.cmb_wt.current(i)
                    break
            else:
                if self.wt:
                    self.cmb_wt.current(0)
        elif self.wt:
            self.cmb_wt.current(0)

        self.ent_name.insert(0, self.init.get("name", ""))

        iu = self.init.get("uom_code")
        if iu:
            for i, u in enumerate(self.uoms):
                if u["code"] == iu:
                    self.cmb_uom.current(i + 1)  # +1 из-за "—"
                    break
            else:
                self.cmb_uom.current(0)
        else:
            self.cmb_uom.current(0)

        if self.init.get("plan_qty") is not None:
            self.ent_qty.insert(0, _fmt_qty(self.init["plan_qty"]))

        d0 = _to_date(self.init.get("plan_start")) or _today()
        d1 = _to_date(self.init.get("plan_finish")) or _today()
        self.ent_s.insert(0, _fmt_date(d0))
        self.ent_f.insert(0, _fmt_date(d1))

        ist = self.init.get("status", "planned")
        try:
            self.cmb_st.current(STATUS_LIST.index(ist))
        except ValueError:
            self.cmb_st.current(0)

        return self.ent_name

    def validate(self):
        try:
            wi = self.cmb_wt.current()
            if wi < 0:
                raise ValueError("Выберите тип работ")
            wt_id = int(self.wt[wi]["id"])
            nm = (self.ent_name.get() or "").strip()
            if not nm:
                raise ValueError("Введите вид работ")

            uom = None
            ui = self.cmb_uom.current()
            if ui > 0:
                uom = self.uoms[ui - 1]["code"]

            qty = _safe_float(self.ent_qty.get())
            ds = _parse_date(self.ent_s.get())
            df = _parse_date(self.ent_f.get())
            if df < ds:
                raise ValueError("Окончание раньше начала")

            si = self.cmb_st.current()
            st = STATUS_LIST[si] if 0 <= si < len(STATUS_LIST) else "planned"

            self._out = dict(
                work_type_id=wt_id,
                name=nm,
                uom_code=uom,
                plan_qty=qty,
                plan_start=ds,
                plan_finish=df,
                status=st,
                is_milestone=bool(self.var_ms.get()),
            )
            return True
        except Exception as e:
            messagebox.showwarning("Работа", str(e), parent=self)
            return False

    def apply(self):
        self.result = dict(self._out)


class TemplateSelectDialog(simpledialog.Dialog):
    def __init__(self, parent, templates):
        self.templates = templates
        self.result: Optional[int] = None
        super().__init__(parent, title="Выбор шаблона ГПР")

    def body(self, m):
        tk.Label(m, text="Выберите шаблон:").pack(anchor="w", pady=(0, 6))
        self.lb = tk.Listbox(
            m, width=50, height=min(15, max(4, len(self.templates)))
        )
        for t in self.templates:
            self.lb.insert("end", t["name"])
        self.lb.pack(fill="both", expand=True)
        if self.templates:
            self.lb.selection_set(0)
        return self.lb

    def validate(self):
        sel = self.lb.curselection()
        if not sel:
            messagebox.showwarning(
                "Шаблон", "Выберите шаблон.", parent=self
            )
            return False
        self._idx = sel[0]
        return True

    def apply(self):
        self.result = int(self.templates[self._idx]["id"])


# ═══════════════════════════════════════════════════════════════
#  GANTT CANVAS (optimized scroll performance)
# ═══════════════════════════════════════════════════════════════

class GanttCanvas(tk.Frame):
    """Гант, синхронизированный с Treeview по позициям строк."""

    MONTH_H = 20
    DAY_H = 22
    HEADER_H = MONTH_H + DAY_H

    def __init__(self, master, *, day_px=20, linked_tree=None):
        super().__init__(master, bg=C["panel"])
        self.day_px = day_px
        self._tree = linked_tree
    
        self.hdr = tk.Canvas(
            self, height=self.HEADER_H, bg="#e8eaed", highlightthickness=0
        )
        self.body = tk.Canvas(self, bg="#ffffff", highlightthickness=0)
        self.hsb = ttk.Scrollbar(
            self, orient="horizontal", command=self._xview
        )
    
        self.hdr.grid(row=0, column=0, sticky="ew")
        self.body.grid(row=1, column=0, sticky="nsew")
        self.hsb.grid(row=2, column=0, sticky="ew")
        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)
    
        self.body.configure(xscrollcommand=self._on_xscroll)
    
        self._range: Tuple[date, date] = _quarter_range()
        self._rows: List[Dict[str, Any]] = []
        self._facts: Dict[int, float] = {}
        self._fact_info: Dict[int, Dict[str, Any]] = {}
    
        self._heavy_redraw_pending: Optional[str] = None
        self._is_mapped = False
        self._header_cache_key = None
    
        self.body.bind("<Configure>", self._on_configure)
        self.body.bind("<MouseWheel>", self._wheel)
        self.body.bind("<Button-4>", self._wheel)
        self.body.bind("<Button-5>", self._wheel)
        self.body.bind("<Shift-MouseWheel>", self._hwheel)
        self.bind("<Map>", self._on_map)

    def _on_map(self, _e=None):
        self._is_mapped = True
        self._schedule_heavy_redraw()

    def _on_configure(self, _e=None):
        self._schedule_heavy_redraw()

    def set_tree(self, tree):
        self._tree = tree

    def set_range(self, d0, d1):
        self._range = (d0, d1)
        self._header_cache_key = None
        self._schedule_heavy_redraw()

    def set_data(self, rows, facts=None, fact_info=None):
        self._rows = rows or []
        self._facts = facts or {}
        self._fact_info = fact_info or {}
        self._schedule_heavy_redraw()

    def _schedule_heavy_redraw(self):
        if self._heavy_redraw_pending:
            self.after_cancel(self._heavy_redraw_pending)
        self._heavy_redraw_pending = self.after(40, self._full_redraw)

    def _full_redraw(self):
        self._heavy_redraw_pending = None
        if not self._is_mapped:
            return
        try:
            self._draw_header()
            self._draw_bars()
        except Exception:
            logger.exception("GanttCanvas._full_redraw error")

    def redraw_bars_only(self):
        if not self._is_mapped:
            return
        try:
            self._draw_bars()
        except Exception:
            logger.exception("GanttCanvas.redraw_bars_only error")

    def _xview(self, *a):
        self.body.xview(*a)
        self.hdr.xview(*a)

    def _on_xscroll(self, f0, f1):
        self.hsb.set(f0, f1)
        self.hdr.xview_moveto(float(f0))

    def _wheel(self, e):
        if self._tree:
            d = _mouse_delta(e)
            if d:
                self._tree.yview_scroll(d, "units")
                self.after_idle(self.redraw_bars_only)
        return "break"

    def _hwheel(self, e):
        d = _mouse_delta(e)
        if d:
            self.body.xview_scroll(d, "units")
            self.hdr.xview_scroll(d, "units")
        return "break"

    def _get_tree_row_positions(self) -> List[Optional[Tuple[int, int]]]:
        """Позиции строк Treeview, нормализованные относительно
        первой видимой строки. Это устраняет потерю первой строки
        и выравнивает бары по строкам."""
        if not self._tree:
            return []
    
        items = self._tree.get_children()
        if not items:
            return []
    
        raw_positions: List[Optional[Tuple[int, int]]] = []
        first_visible_y: Optional[int] = None
    
        for iid in items:
            try:
                bbox = self._tree.bbox(iid)
                if bbox:
                    _x, y, _w, h = bbox
                    raw_positions.append((y, y + h))
                    if first_visible_y is None:
                        first_visible_y = y
                else:
                    raw_positions.append(None)
            except (tk.TclError, Exception):
                raw_positions.append(None)
    
        if first_visible_y is None:
            return raw_positions
    
        norm_positions: List[Optional[Tuple[int, int]]] = []
        for pos in raw_positions:
            if pos is None:
                norm_positions.append(None)
            else:
                y0, y1 = pos
                norm_positions.append((y0 - first_visible_y, y1 - first_visible_y))
    
        return norm_positions

    def _draw_header(self):
        d0, d1 = self._range
        if d1 < d0:
            return

        days = (d1 - d0).days + 1
        tw = max(1, days * self.day_px)

        cache_key = (d0, d1, self.day_px)
        if self._header_cache_key == cache_key:
            return
        self._header_cache_key = cache_key

        self.hdr.delete("all")
        self.hdr.configure(scrollregion=(0, 0, tw, self.HEADER_H))

        cur = date(d0.year, d0.month, 1)
        while cur <= d1:
            mr = calendar.monthrange(cur.year, cur.month)[1]
            ms = max(cur, d0)
            me = min(date(cur.year, cur.month, mr), d1)
            x0 = (ms - d0).days * self.day_px
            x1 = ((me - d0).days + 1) * self.day_px
            self.hdr.create_rectangle(
                x0, 0, x1, self.MONTH_H, fill="#d6dbe0", outline="#bbb"
            )
            if (x1 - x0) > 40:
                self.hdr.create_text(
                    (x0 + x1) / 2,
                    self.MONTH_H / 2,
                    text=cur.strftime("%b %Y"),
                    font=("Segoe UI", 8, "bold"),
                    fill="#333",
                )
            if cur.month == 12:
                cur = date(cur.year + 1, 1, 1)
            else:
                cur = date(cur.year, cur.month + 1, 1)

        for i in range(days):
            x0 = i * self.day_px
            x1 = x0 + self.day_px
            d = d0 + timedelta(days=i)
            fill = "#ffecec" if d.weekday() >= 5 else "#f3f4f6"
            self.hdr.create_rectangle(
                x0, self.MONTH_H, x1, self.HEADER_H,
                fill=fill, outline="#d0d0d0"
            )
            if self.day_px >= 14:
                self.hdr.create_text(
                    (x0 + x1) / 2,
                    self.MONTH_H + self.DAY_H / 2,
                    text=str(d.day),
                    font=("Segoe UI", 7),
                    fill="#555",
                )

        td = _today()
        if d0 <= td <= d1:
            tx = (td - d0).days * self.day_px + self.day_px // 2
            self.hdr.create_line(
                tx, 0, tx, self.HEADER_H, fill=C["error"], width=2
            )

    def _draw_bars(self):
        d0, d1 = self._range
        if d1 < d0:
            return
    
        days = (d1 - d0).days + 1
        tw = max(1, days * self.day_px)
        body_h = self.body.winfo_height()
        if body_h < 10:
            body_h = 600
    
        self.body.delete("all")
        self.body.configure(scrollregion=(0, 0, tw, body_h))
    
        td = _today()
        if d0 <= td <= d1:
            tx = (td - d0).days * self.day_px + self.day_px // 2
            self.body.create_line(
                tx, 0, tx, body_h,
                fill=C["error"], width=1, dash=(4, 2)
            )
    
        step = 7 if self.day_px >= 10 else 14
        for i in range(0, days, step):
            x = i * self.day_px
            self.body.create_line(x, 0, x, body_h, fill="#eeeeee")
    
        positions = self._get_tree_row_positions()
        if not positions:
            return
    
        for row_idx, t in enumerate(self._rows):
            if row_idx >= len(positions) or positions[row_idx] is None:
                continue
    
            y0, y1 = positions[row_idx]
    
            if y1 < -5 or y0 > body_h + 5:
                continue
    
            row_kind = (t.get("row_kind") or "task").strip()
    
            if row_kind == "group":
                bg = "#eef5ff"
            elif row_kind == "title":
                bg = "#dff1ff"
            else:
                bg = "#ffffff" if row_idx % 2 == 0 else "#f8f9fa"
    
            self.body.create_rectangle(0, y0, tw, y1, fill=bg, outline="")
    
            if row_kind in ("group", "title"):
                fg = "#1a3d7c" if row_kind == "group" else "#0b5394"
                prefix = "📁 " if row_kind == "group" else "🟦 "
                self.body.create_text(
                    6,
                    (y0 + y1) / 2,
                    text=prefix + (t.get("name") or ""),
                    anchor="w",
                    font=("Segoe UI", 8, "bold"),
                    fill=fg,
                )
                continue
    
            ts = _to_date(t.get("plan_start"))
            tf = _to_date(t.get("plan_finish"))
            if not ts or not tf:
                continue
            if tf < d0 or ts > d1:
                continue
    
            s2 = max(ts, d0)
            f2 = min(tf, d1)
            bx0 = (s2 - d0).days * self.day_px
            bx1 = ((f2 - d0).days + 1) * self.day_px
    
            st = (t.get("status") or "planned").strip()
            col, _, _ = STATUS_COLORS.get(st, ("#90caf9", "#555", ""))
    
            by0 = y0 + 4
            by1 = y1 - 4
            if by1 - by0 < 4:
                by0 = y0 + 2
                by1 = y1 - 2
    
            self.body.create_rectangle(
                bx0 + 1, by0, bx1 - 1, by1,
                fill=col, outline="#5f6368"
            )
    
            tid = t.get("id")
            pq = _safe_float(t.get("plan_qty"))
            fq = self._facts.get(tid, 0) if tid else 0
            fact_info = self._fact_info.get(tid, {}) if tid else {}
            workers_last = fact_info.get("workers_last")
    
            if pq and pq > 0 and fq > 0:
                pct = min(1.0, fq / pq)
                fw = max(2, int((bx1 - bx0 - 2) * pct))
                self.body.create_rectangle(
                    bx0 + 1, by0, bx0 + 1 + fw, by1,
                    fill="#388e3c", outline=""
                )
    
            if t.get("is_milestone"):
                cx = bx0 + 6
                cy = (y0 + y1) / 2
                self.body.create_polygon(
                    cx, cy, cx + 7, cy - 5,
                    cx + 14, cy, cx + 7, cy + 5,
                    fill="#1a73e8", outline=""
                )
    
            bar_w = bx1 - bx0
    
            info_parts = []
            if fq and pq and pq > 0:
                info_parts.append(f"{_fmt_qty(fq)}/{_fmt_qty(pq)}")
            elif fq:
                info_parts.append(_fmt_qty(fq))
    
            if workers_last:
                info_parts.append(f"{workers_last}чел")
    
            info_text = " · ".join(info_parts)
    
            if info_text and bar_w > 90:
                self.body.create_text(
                    bx1 - 4,
                    (y0 + y1) / 2,
                    text=info_text,
                    anchor="e",
                    font=("Segoe UI", 7),
                    fill="#222"
                )
    
            if bar_w > 60:
                nm = (t.get("name") or "")[:30]
                self.body.create_text(
                    bx0 + 4, (y0 + y1) / 2,
                    text=nm,
                    anchor="w",
                    font=("Segoe UI", 7),
                    fill="#333"
                )
            elif workers_last and bar_w > 26:
                self.body.create_text(
                    (bx0 + bx1) / 2,
                    (y0 + y1) / 2,
                    text=f"{workers_last}",
                    anchor="center",
                    font=("Segoe UI", 7, "bold"),
                    fill="#222"
                )

# ═══════════════════════════════════════════════════════════════
#  MAIN PAGE
# ═══════════════════════════════════════════════════════════════

class GprPage(tk.Frame):

    def __init__(self, master, app_ref):
        super().__init__(master, bg=C["bg"])
        self.app_ref = app_ref
    
        self.objects: List[Dict[str, Any]] = []
        self.work_types: List[Dict[str, Any]] = []
        self.uoms: List[Dict[str, Any]] = []
    
        self.object_db_id: Optional[int] = None
        self.plan_info: Optional[Dict[str, Any]] = None
        self.plan_id: Optional[int] = None
    
        self.tasks: List[Dict[str, Any]] = []
        self.tasks_filtered: List[Dict[str, Any]] = []
        self.facts: Dict[int, float] = {}
        self.fact_info: Dict[int, Dict[str, Any]] = {}
    
        self.registry_rows: List[Dict[str, Any]] = []
        self.registry_filtered: List[Dict[str, Any]] = []
    
        self._new_task_counter = 0
    
        q = _quarter_range()
        self.range_from: date = q[0]
        self.range_to: date = q[1]
    
        self._build_ui()
        self._load_refs()
        self._refresh_registry()
        self._update_range_label()

    def _build_ui(self):
        hdr = tk.Frame(self, bg=C["accent"], pady=6)
        hdr.pack(fill="x")

        tk.Label(
            hdr,
            text="📊  ГПР — График производства работ",
            font=("Segoe UI", 12, "bold"),
            bg=C["accent"],
            fg="white",
            padx=12,
        ).pack(side="left")

        self.lbl_plan_info = tk.Label(
            hdr,
            text="",
            font=("Segoe UI", 8),
            bg=C["accent"],
            fg="#bbdefb",
            padx=12,
        )
        self.lbl_plan_info.pack(side="right")

        self.nb_main = ttk.Notebook(self)
        self.nb_main.pack(fill="both", expand=True, padx=0, pady=0)

        self.tab_registry = tk.Frame(self.nb_main, bg=C["bg"])
        self.tab_editor = tk.Frame(self.nb_main, bg=C["bg"])

        self.nb_main.add(self.tab_registry, text="  📚 Реестр ГПР  ")
        self.nb_main.add(self.tab_editor, text="  🛠 Редактор ГПР  ")

        self._build_registry_tab(self.tab_registry)
        self._build_editor_tab(self.tab_editor)

    # ══════════════════════════════════════════════════════
    #  REGISTRY TAB
    # ══════════════════════════════════════════════════════
    def _build_registry_tab(self, parent):
        top = tk.LabelFrame(
            parent,
            text=" 🔎 Поиск и фильтрация ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            relief="groove",
            bd=1,
            padx=10,
            pady=8,
        )
        top.pack(fill="x", padx=10, pady=(8, 4))

        row1 = tk.Frame(top, bg=C["panel"])
        row1.pack(fill="x")

        tk.Label(
            row1, text="Поиск:", bg=C["panel"], font=("Segoe UI", 9)
        ).pack(side="left")
        self.var_registry_search = tk.StringVar()
        ent = ttk.Entry(row1, textvariable=self.var_registry_search, width=40)
        ent.pack(side="left", padx=(6, 12))
        ent.bind("<KeyRelease>", lambda _e: self._apply_registry_filter())

        tk.Label(
            row1, text="Фильтр:", bg=C["panel"], font=("Segoe UI", 9)
        ).pack(side="left")
        self.cmb_registry_filter = ttk.Combobox(
            row1,
            state="readonly",
            width=20,
            values=[
                "Все объекты",
                "Только с ГПР",
                "Без ГПР",
                "С просрочкой",
            ],
        )
        self.cmb_registry_filter.pack(side="left", padx=(6, 12))
        self.cmb_registry_filter.current(0)
        self.cmb_registry_filter.bind(
            "<<ComboboxSelected>>",
            lambda _e: self._apply_registry_filter(),
        )

        ttk.Button(
            row1, text="Обновить", command=self._refresh_registry
        ).pack(side="right", padx=2)

        ttk.Button(
            row1, text="Открыть выбранный", command=self._open_selected_registry
        ).pack(side="right", padx=2)

        self.lbl_registry_summary = tk.Label(
            parent,
            text="",
            bg=C["bg"],
            fg=C["text2"],
            font=("Segoe UI", 8),
            anchor="w",
        )
        self.lbl_registry_summary.pack(fill="x", padx=14, pady=(2, 0))

        wrap = tk.Frame(parent, bg=C["panel"])
        wrap.pack(fill="both", expand=True, padx=10, pady=(4, 8))

        cols = (
            "excel_id",
            "short_name",
            "address",
            "object_status",
            "version_no",
            "updated_at",
            "creator_name",
            "task_count",
            "done_count",
            "overdue_count",
        )
        self.registry_tree = ttk.Treeview(
            wrap, columns=cols, show="headings", selectmode="browse"
        )

        heads = {
            "excel_id": ("Код", 80),
            "short_name": ("Краткое имя", 180),
            "address": ("Адрес", 420),
            "object_status": ("Статус объекта", 110),
            "version_no": ("Версия", 60),
            "updated_at": ("Обновлён", 120),
            "creator_name": ("Создал", 140),
            "task_count": ("Работ", 70),
            "done_count": ("Выполнено", 85),
            "overdue_count": ("Просрочено", 90),
        }

        for c, (t, w) in heads.items():
            self.registry_tree.heading(c, text=t)
            anc = "center" if c in (
                "excel_id", "version_no", "task_count", "done_count", "overdue_count"
            ) else "w"
            self.registry_tree.column(c, width=w, anchor=anc)

        vsb = ttk.Scrollbar(
            wrap, orient="vertical", command=self.registry_tree.yview
        )
        hsb = ttk.Scrollbar(
            parent.winfo_toplevel() if False else wrap,
            orient="horizontal",
            command=self.registry_tree.xview,
        )
        self.registry_tree.configure(
            yscrollcommand=vsb.set,
            xscrollcommand=hsb.set,
        )

        self.registry_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")

        self.registry_tree.bind(
            "<Double-1>", lambda _e: self._open_selected_registry()
        )
        self.registry_tree.bind(
            "<Return>", lambda _e: self._open_selected_registry()
        )

        self.registry_tree.tag_configure("no_plan", foreground="#777")
        self.registry_tree.tag_configure("overdue", background="#fff3f3")

    def _refresh_registry(self):
        try:
            self.registry_rows = GprService.load_gpr_registry()
        except Exception as e:
            logger.exception("GPR registry load error")
            messagebox.showerror(
                "ГПР", f"Ошибка загрузки реестра ГПР:\n{e}", parent=self
            )
            return

        self._apply_registry_filter()

    def _apply_registry_filter(self):
        q = (self.var_registry_search.get() or "").strip().lower()
        mode = self.cmb_registry_filter.get().strip()

        res = []
        for r in self.registry_rows:
            has_plan = bool(r.get("plan_id"))
            overdue = int(r.get("overdue_count") or 0)

            if mode == "Только с ГПР" and not has_plan:
                continue
            if mode == "Без ГПР" and has_plan:
                continue
            if mode == "С просрочкой" and overdue <= 0:
                continue

            if q:
                hay = " ".join([
                    str(r.get("excel_id") or ""),
                    str(r.get("short_name") or ""),
                    str(r.get("address") or ""),
                    str(r.get("creator_name") or ""),
                    str(r.get("object_status") or ""),
                ]).lower()
                if q not in hay:
                    continue

            res.append(r)

        self.registry_filtered = res
        self._render_registry()

    def _render_registry(self):
        self.registry_tree.delete(*self.registry_tree.get_children())

        total = len(self.registry_filtered)
        with_plan = 0
        overdue_cnt = 0

        for r in self.registry_filtered:
            oid = int(r["object_db_id"])
            iid = f"obj_{oid}"

            plan_id = r.get("plan_id")
            if plan_id:
                with_plan += 1

            overdue = int(r.get("overdue_count") or 0)
            if overdue > 0:
                overdue_cnt += 1

            upd = r.get("updated_at")
            if isinstance(upd, datetime):
                upd_s = upd.strftime("%d.%m.%Y %H:%M")
            else:
                upd_s = ""

            tags = []
            if not plan_id:
                tags.append("no_plan")
            if overdue > 0:
                tags.append("overdue")

            self.registry_tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    r.get("excel_id") or "",
                    r.get("short_name") or "",
                    r.get("address") or "",
                    r.get("object_status") or "",
                    r.get("version_no") or "",
                    upd_s,
                    r.get("creator_name") or "",
                    r.get("task_count") or 0,
                    r.get("done_count") or 0,
                    overdue,
                ),
                tags=tuple(tags),
            )

        self.lbl_registry_summary.config(
            text=(
                f"Объектов: {total}  |  "
                f"С ГПР: {with_plan}  |  "
                f"С просрочкой: {overdue_cnt}"
            )
        )

    def _open_selected_registry(self):
        sel = self.registry_tree.selection()
        if not sel:
            messagebox.showinfo(
                "ГПР", "Выберите объект в реестре.", parent=self
            )
            return

        iid = sel[0]
        if not iid.startswith("obj_"):
            return

        try:
            oid = int(iid[4:])
        except ValueError:
            return

        self._open_object_by_id(oid)

    def _open_fact_batch(self):
        if not self.plan_id:
            messagebox.showinfo("ГПР", "Сначала откройте объект.", parent=self)
            return
    
        task_rows = [t for t in self.tasks if (t.get("row_kind") or "task") == "task" and t.get("id")]
        if not task_rows:
            messagebox.showinfo(
                "ГПР",
                "Нет сохранённых работ для массового ввода факта.",
                parent=self,
            )
            return
    
        uid = (self.app_ref.current_user or {}).get("id")
    
        try:
            from gpr_task_dialog import open_task_fact_batch_dialog
        except ImportError as e:
            logger.exception("Cannot import batch fact dialog")
            messagebox.showerror(
                "ГПР",
                f"Не удалось открыть диалог массового ввода факта:\n{e}",
                parent=self,
            )
            return
    
        result = open_task_fact_batch_dialog(
            self,
            tasks=self.tasks,
            user_id=uid,
            fact_date=_today(),
        )
    
        if not result or not result.get("saved"):
            return
    
        try:
            tids = [
                t["id"] for t in self.tasks
                if t.get("id") and (t.get("row_kind") or "task") == "task"
            ]
            self.fact_info = GprService.load_task_fact_info(tids)
            self.facts = {
                task_id: float(v.get("fact_qty_total") or 0)
                for task_id, v in self.fact_info.items()
            }
    
            self._apply_filter()
            self._update_summary()
    
            messagebox.showinfo(
                "ГПР",
                f"Сохранено записей факта: {result.get('count', 0)}",
                parent=self,
            )
        except Exception as e:
            logger.exception("Refresh facts after batch save error")
            messagebox.showwarning(
                "ГПР",
                f"Факт сохранён, но не удалось обновить отображение:\n{e}",
                parent=self,
            )

    # ══════════════════════════════════════════════════════
    #  EDITOR TAB
    # ══════════════════════════════════════════════════════
    def _build_editor_tab(self, parent):
        top = tk.LabelFrame(
            parent,
            text=" 📍 Объект и диапазон ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            relief="groove",
            bd=1,
            padx=10,
            pady=8,
        )
        top.pack(fill="x", padx=10, pady=(8, 4))
        top.grid_columnconfigure(1, weight=1)
    
        tk.Label(
            top, text="Объект:", bg=C["panel"], font=("Segoe UI", 9)
        ).grid(row=0, column=0, sticky="e", padx=(0, 6))
    
        self.cmb_obj = _AutoCombo(top, width=60, font=("Segoe UI", 9))
        self.cmb_obj.grid(row=0, column=1, sticky="ew", pady=3)
    
        btn_f = tk.Frame(top, bg=C["panel"])
        btn_f.grid(row=0, column=2, padx=(8, 0))
        self._accent_btn(btn_f, "▶  Открыть", self._open_object)
    
        tk.Label(
            top, text="Диапазон:", bg=C["panel"], font=("Segoe UI", 9)
        ).grid(row=1, column=0, sticky="e", padx=(0, 6))
    
        range_f = tk.Frame(top, bg=C["panel"])
        range_f.grid(row=1, column=1, sticky="w", pady=3)
    
        self.lbl_range = tk.Label(
            range_f,
            text="",
            bg=C["panel"],
            fg=C["text2"],
            font=("Segoe UI", 9),
        )
        self.lbl_range.pack(side="left")
    
        ttk.Button(
            range_f, text="Изменить…", command=self._change_range
        ).pack(side="left", padx=(12, 0))
        ttk.Button(
            range_f, text="По работам", command=self._fit_range
        ).pack(side="left", padx=(6, 0))
    
        bar = tk.Frame(parent, bg=C["accent_light"], pady=5)
        bar.pack(fill="x", padx=10)
    
        self.btn_add = self._tb_btn(bar, "➕ Добавить", self._add_task)
        self.btn_group = self._tb_btn(bar, "📁 Группа", self._add_group)
        self.btn_title = self._tb_btn(bar, "🟦 Титул", self._add_title)
        self.btn_edit = self._tb_btn(bar, "✏️ Редактировать", self._edit_selected)
        self.btn_delete = self._tb_btn(bar, "🗑 Удалить", self._delete_selected)
        self.btn_up = self._tb_btn(bar, "⬆ Вверх", self._move_selected_up)
        self.btn_down = self._tb_btn(bar, "⬇ Вниз", self._move_selected_down)
    
        tk.Frame(bar, bg=C["border"], width=1).pack(
            side="left", fill="y", padx=8
        )
    
        self.btn_template = self._tb_btn(bar, "📋 Из шаблона…", self._apply_template)
        self.btn_fact_batch = self._tb_btn(bar, "📈 Заполнить факт", self._open_fact_batch)
        self.btn_import = self._tb_btn(bar, "📤 Импорт Excel", self._import_excel)
        self.btn_export = self._tb_btn(bar, "📥 Экспорт Excel", self._export_excel)
    
        tk.Frame(bar, bg=C["border"], width=1).pack(
            side="left", fill="y", padx=8
        )
    
        self._tb_btn(bar, "🔍−", lambda: self._zoom(-2))
        self._tb_btn(bar, "🔍+", lambda: self._zoom(2))
    
        self.btn_save = self._accent_btn(bar, "💾  СОХРАНИТЬ", self._save)
        self.btn_save.pack_forget()
        self.btn_save.pack(side="right", padx=(4, 8))
    
        fbar = tk.Frame(parent, bg=C["bg"], pady=4)
        fbar.pack(fill="x", padx=10)
    
        tk.Label(
            fbar, text="Фильтр тип:", bg=C["bg"], font=("Segoe UI", 8)
        ).pack(side="left")
        self.cmb_filt_wt = ttk.Combobox(
            fbar, state="readonly", width=20, values=["Все"]
        )
        self.cmb_filt_wt.pack(side="left", padx=(4, 12))
        self.cmb_filt_wt.current(0)
        self.cmb_filt_wt.bind(
            "<<ComboboxSelected>>", lambda _e: self._apply_filter()
        )
    
        tk.Label(
            fbar, text="Статус:", bg=C["bg"], font=("Segoe UI", 8)
        ).pack(side="left")
        self.cmb_filt_st = ttk.Combobox(
            fbar,
            state="readonly",
            width=16,
            values=["Все"] + [STATUS_LABELS[s] for s in STATUS_LIST],
        )
        self.cmb_filt_st.pack(side="left", padx=(4, 12))
        self.cmb_filt_st.current(0)
        self.cmb_filt_st.bind(
            "<<ComboboxSelected>>", lambda _e: self._apply_filter()
        )
    
        tk.Label(
            fbar, text="Поиск:", bg=C["bg"], font=("Segoe UI", 8)
        ).pack(side="left")
        self.var_search = tk.StringVar()
        ent_s = ttk.Entry(fbar, textvariable=self.var_search, width=24)
        ent_s.pack(side="left", padx=(4, 0))
        ent_s.bind("<KeyRelease>", lambda _e: self._apply_filter())
    
        self.lbl_summary = tk.Label(
            parent,
            text="",
            bg=C["bg"],
            font=("Segoe UI", 8),
            fg=C["text2"],
            anchor="w",
        )
        self.lbl_summary.pack(fill="x", padx=14, pady=(2, 0))
    
        leg = tk.Frame(parent, bg=C["bg"])
        leg.pack(fill="x", padx=14, pady=(0, 2))
        for code in STATUS_LIST:
            col, _, label = STATUS_COLORS[code]
            f = tk.Frame(leg, bg=C["bg"])
            f.pack(side="left", padx=(0, 12))
            tk.Canvas(
                f,
                width=12,
                height=12,
                bg=col,
                highlightthickness=1,
                highlightbackground="#999",
            ).pack(side="left", padx=(0, 3))
            tk.Label(
                f,
                text=label,
                bg=C["bg"],
                font=("Segoe UI", 7),
                fg=C["text2"],
            ).pack(side="left")
    
        pw = tk.PanedWindow(
            parent, orient="horizontal", sashrelief="raised", bg=C["bg"]
        )
        pw.pack(fill="both", expand=True, padx=10, pady=(4, 4))
    
        left = tk.Frame(pw, bg=C["panel"])
        right = tk.Frame(pw, bg=C["panel"])
        pw.add(left, minsize=560)
        pw.add(right, minsize=400)
    
        self.tree_top_spacer = tk.Frame(
            left,
            bg="#d6dbe0",
            height=GanttCanvas.MONTH_H,
            highlightthickness=0,
            bd=0,
        )
        self.tree_top_spacer.pack(side="top", fill="x")
        self.tree_top_spacer.pack_propagate(False)
    
        tree_wrap = tk.Frame(left, bg=C["panel"])
        tree_wrap.pack(side="top", fill="both", expand=True)
    
        style = ttk.Style(self)
        style.configure("GPR.Treeview", rowheight=24)
        style.configure("GPR.Treeview.Heading", font=("Segoe UI", 9, "bold"))
    
        cols = ("type", "name", "start", "finish", "uom", "qty", "workers", "status")
        self.tree = ttk.Treeview(
            tree_wrap,
            columns=cols,
            show="headings",
            selectmode="browse",
            style="GPR.Treeview",
        )
    
        heads = {
            "type": ("Тип работ", 130),
            "name": ("Вид работ", 260),
            "start": ("Начало", 85),
            "finish": ("Конец", 85),
            "uom": ("Ед.", 50),
            "qty": ("Объём", 75),
            "workers": ("Людей", 70),
            "status": ("Статус", 100),
        }
    
        for c, (t, w) in heads.items():
            self.tree.heading(c, text=t)
            anc = (
                "center"
                if c in ("start", "finish", "uom", "workers", "status")
                else ("e" if c == "qty" else "w")
            )
            self.tree.column(c, width=w, anchor=anc)
    
        vsb = ttk.Scrollbar(
            tree_wrap, orient="vertical", command=self._on_tree_scroll
        )
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
    
        self.tree.bind("<Double-1>", lambda _e: self._edit_selected())
        self.tree.bind("<Return>", lambda _e: self._edit_selected())
        self.tree.bind("<MouseWheel>", self._on_tree_wheel)
        self.tree.bind("<Button-4>", self._on_tree_wheel)
        self.tree.bind("<Button-5>", self._on_tree_wheel)
        self.tree.bind("<Control-Up>", lambda _e: self._move_selected_up())
        self.tree.bind("<Control-Down>", lambda _e: self._move_selected_down())
    
        self.gantt = GanttCanvas(right, day_px=20, linked_tree=self.tree)
        self.gantt.pack(fill="both", expand=True)
    
        self.after_idle(self._sync_tree_header_spacer)
    
        bot = tk.Frame(parent, bg=C["border"], height=1)
        bot.pack(fill="x", padx=10)
    
        self.lbl_bottom = tk.Label(
            parent,
            text="Выберите объект в реестре или откройте его вручную",
            bg=C["bg"],
            fg=C["text3"],
            font=("Segoe UI", 8),
            anchor="w",
            padx=14,
            pady=2,
        )
        self.lbl_bottom.pack(fill="x", padx=0, pady=(0, 6))
    # ══════════════════════════════════════════════════════
    #  COMMON
    # ══════════════════════════════════════════════════════
    def _sync_tree_header_spacer(self):
        try:
            self.update_idletasks()

            items = self.tree.get_children()
            if not items:
                self.tree_top_spacer.config(height=GanttCanvas.MONTH_H)
                return

            first_bbox = None
            for iid in items:
                try:
                    bb = self.tree.bbox(iid)
                    if bb:
                        first_bbox = bb
                        break
                except tk.TclError:
                    continue

            if not first_bbox:
                self.tree_top_spacer.config(height=GanttCanvas.MONTH_H)
                return

            tree_header_h = max(0, int(first_bbox[1]))
            spacer_h = max(0, int(self.gantt.HEADER_H) - tree_header_h)
            self.tree_top_spacer.config(height=spacer_h)

        except Exception:
            logger.exception("Error syncing tree/gantt header heights")
            self.tree_top_spacer.config(height=GanttCanvas.MONTH_H)

    def _accent_btn(self, parent, text, cmd):
        b = tk.Button(
            parent,
            text=text,
            font=("Segoe UI", 9, "bold"),
            bg=C["btn_bg"],
            fg=C["btn_fg"],
            activebackground="#0d47a1",
            activeforeground="white",
            relief="flat",
            cursor="hand2",
            padx=10,
            pady=3,
            command=cmd,
        )
        b.pack(side="left", padx=2)
        b.bind("<Enter>", lambda _e: b.config(bg="#0d47a1"))
        b.bind("<Leave>", lambda _e: b.config(bg=C["btn_bg"]))
        return b

    def _tb_btn(self, parent, text, cmd):
        b = ttk.Button(parent, text=text, command=cmd)
        b.pack(side="left", padx=2)
        return b

    def _on_tree_scroll(self, *args):
        self.tree.yview(*args)
        self.gantt.after_idle(self.gantt.redraw_bars_only)

    def _on_tree_wheel(self, event):
        d = _mouse_delta(event)
        if d:
            self.tree.yview_scroll(d, "units")
            self.gantt.after_idle(self.gantt.redraw_bars_only)
        return "break"

    def _load_refs(self):
        try:
            self.objects = GprService.load_objects_short()
            self.work_types = GprService.load_work_types()
            self.uoms = GprService.load_uoms()
        except Exception as e:
            logger.exception("GPR refs error")
            messagebox.showerror(
                "ГПР", f"Ошибка загрузки справочников:\n{e}", parent=self
            )
            return

        vals = []
        for o in self.objects:
            sn = (o.get("short_name") or "").strip()
            addr = (o.get("address") or "").strip()
            eid = str(o.get("excel_id") or "").strip()
            db_id = str(o.get("id") or "")

            tag = f"[{eid}]" if eid else f"[id:{db_id}]"
            lbl = f"{sn} — {addr} — {tag}" if sn else f"{addr} — {tag}"
            vals.append(lbl)

        self.cmb_obj.set_values(vals)

        wt_names = ["Все"] + [w["name"] for w in self.work_types]
        self.cmb_filt_wt.config(values=wt_names)

    def _update_range_label(self):
        self.lbl_range.config(
            text=f"{_fmt_date(self.range_from)} — {_fmt_date(self.range_to)}"
        )
        self.gantt.set_range(self.range_from, self.range_to)

    def _update_plan_info(self):
        p = self.plan_info
        if not p:
            self.lbl_plan_info.config(text="")
            return

        cr = p.get("creator_name") or "—"
        upd = p.get("updated_at")
        if isinstance(upd, datetime):
            upd_s = upd.strftime("%d.%m.%Y %H:%M")
        else:
            upd_s = str(upd or "")

        v = p.get("version_no", 1)
        self.lbl_plan_info.config(
            text=f"Версия: {v}  |  Создал: {cr}  |  Обновлён: {upd_s}"
        )

    def _update_summary(self):
        total = len([t for t in self.tasks if (t.get("row_kind") or "task") == "task"])
        by_st: Dict[str, int] = {}
        overdue = 0
        td = _today()

        for t in self.tasks:
            if (t.get("row_kind") or "task") != "task":
                continue
            st = t.get("status", "planned")
            by_st[st] = by_st.get(st, 0) + 1
            if st not in ("done", "canceled"):
                pf = _to_date(t.get("plan_finish"))
                if pf and pf < td:
                    overdue += 1

        groups = sum(1 for t in self.tasks if (t.get("row_kind") or "task") == "group")
        titles = sum(1 for t in self.tasks if (t.get("row_kind") or "task") == "title")

        parts = [f"Работ: {total}"]
        if groups:
            parts.append(f"Групп: {groups}")
        if titles:
            parts.append(f"Титулов: {titles}")

        for s in STATUS_LIST:
            cnt = by_st.get(s, 0)
            if cnt > 0:
                parts.append(f"{STATUS_LABELS[s]}: {cnt}")
        if overdue > 0:
            parts.append(f"⚠ Просрочено: {overdue}")

        self.lbl_summary.config(text="  |  ".join(parts))

    def _sel_obj_id(self) -> Optional[int]:
        idx = self.cmb_obj.get_original_index()
        if idx < 0 or idx >= len(self.objects):
            return None
        return int(self.objects[idx]["id"])

    def _recalc_sort_order(self):
        for i, t in enumerate(self.tasks):
            t["sort_order"] = i * 10

    def _preserve_selection_by_task(self, task_ref):
        if task_ref is None:
            return
        for iid in self.tree.get_children():
            try:
                idx = self.tree.index(iid)
            except tk.TclError:
                continue
            if 0 <= idx < len(self.tasks_filtered) and self.tasks_filtered[idx] is task_ref:
                self.tree.selection_set(iid)
                self.tree.focus(iid)
                self.tree.see(iid)
                break

    # ══════════════════════════════════════════════════════
    #  OPEN OBJECT
    # ══════════════════════════════════════════════════════
    def _open_object(self):
        oid = self._sel_obj_id()
        if not oid:
            messagebox.showwarning(
                "ГПР", "Выберите объект из списка.", parent=self
            )
            return
        self._open_object_by_id(oid)

    def _open_object_by_id(self, oid: int):
        self.object_db_id = oid
        uid = (self.app_ref.current_user or {}).get("id")
    
        try:
            self.plan_info = GprService.get_or_create_current_plan(oid, uid)
            self.plan_id = int(self.plan_info["id"])
            self.tasks = GprService.load_plan_tasks(self.plan_id)
    
            for t in self.tasks:
                t["row_kind"] = (t.get("row_kind") or "task").strip()
    
            tids = [
                t["id"] for t in self.tasks
                if t.get("id") and (t.get("row_kind") or "task") == "task"
            ]
    
            self.fact_info = GprService.load_task_fact_info(tids)
            self.facts = {
                task_id: float(v.get("fact_qty_total") or 0)
                for task_id, v in self.fact_info.items()
            }
    
        except Exception as e:
            logger.exception("GPR open error")
            messagebox.showerror(
                "ГПР", f"Не удалось открыть ГПР:\n{e}", parent=self
            )
            return
    
        obj = next((o for o in self.objects if int(o["id"]) == oid), None)
        if obj:
            sn = (obj.get("short_name") or "").strip()
            addr = (obj.get("address") or "").strip()
            name = sn if sn else addr
    
            label = None
            for i, o in enumerate(self.objects):
                if int(o["id"]) == oid:
                    eid = str(o.get("excel_id") or "").strip()
                    tag = f"[{eid}]" if eid else f"[id:{oid}]"
                    label = f"{sn} — {addr} — {tag}" if sn else f"{addr} — {tag}"
                    if label:
                        self.cmb_obj.set(label)
                    break
        else:
            name = str(oid)
    
        self._update_plan_info()
        self._apply_filter()
        self._update_summary()
        self.lbl_bottom.config(
            text=f"Объект: {name}  |  Строк: {len(self.tasks)}"
        )
    
        self.nb_main.select(self.tab_editor)

    # ══════════════════════════════════════════════════════
    #  FILTER / RENDER
    # ══════════════════════════════════════════════════════
    def _apply_filter(self):
        wt_idx = self.cmb_filt_wt.current()
        wt_name = None
        if wt_idx > 0 and wt_idx <= len(self.work_types):
            wt_name = self.work_types[wt_idx - 1]["name"]

        st_idx = self.cmb_filt_st.current()
        st_code = None
        if st_idx > 0 and st_idx <= len(STATUS_LIST):
            st_code = STATUS_LIST[st_idx - 1]

        q = (self.var_search.get() or "").strip().lower()

        res = []
        for t in self.tasks:
            row_kind = (t.get("row_kind") or "task").strip()

            if row_kind in ("group", "title"):
                res.append(t)
                continue

            if wt_name and (t.get("work_type_name") or "") != wt_name:
                continue
            if st_code and (t.get("status") or "") != st_code:
                continue
            if q:
                nm = (t.get("name") or "").lower()
                wtn = (t.get("work_type_name") or "").lower()
                if q not in nm and q not in wtn:
                    continue
            res.append(t)

        self.tasks_filtered = res
        self._render()

    def _gen_iid(self, task: Dict[str, Any]) -> str:
        tid = task.get("id")
        if tid is not None:
            return f"db_{tid}"
        self._new_task_counter += 1
        return f"new_{self._new_task_counter}"

    def _render(self):
        self.tree.delete(*self.tree.get_children())
    
        for t in self.tasks_filtered:
            iid = self._gen_iid(t)
            row_kind = (t.get("row_kind") or "task").strip()
    
            if row_kind == "group":
                values = ("", f"📁 {t.get('name', '')}", "", "", "", "", "", "")
                self.tree.insert("", "end", iid=iid, values=values, tags=("group",))
            elif row_kind == "title":
                values = ("", f"🟦 {t.get('name', '')}", "", "", "", "", "", "")
                self.tree.insert("", "end", iid=iid, values=values, tags=("title",))
            else:
                st_label = STATUS_LABELS.get(
                    t.get("status", ""), t.get("status", "")
                )
    
                tid = t.get("id")
                workers_last = ""
                if tid is not None:
                    info = self.fact_info.get(tid) or {}
                    if info.get("workers_last") is not None:
                        workers_last = str(info["workers_last"])
    
                self.tree.insert(
                    "",
                    "end",
                    iid=iid,
                    values=(
                        t.get("work_type_name", ""),
                        t.get("name", ""),
                        _fmt_date(t.get("plan_start")),
                        _fmt_date(t.get("plan_finish")),
                        t.get("uom_code") or "",
                        _fmt_qty(t.get("plan_qty")),
                        workers_last,
                        st_label,
                    ),
                    tags=("task",),
                )
    
        self.tree.tag_configure("group", font=("Segoe UI", 9, "bold"))
        self.tree.tag_configure(
            "title",
            font=("Segoe UI", 9, "bold"),
            background="#e3f2fd",
        )
    
        self.gantt.set_data(self.tasks_filtered, self.facts, self.fact_info)
        self.after_idle(self._sync_tree_header_spacer)
        self.after_idle(self.gantt.redraw_bars_only)

    # ══════════════════════════════════════════════════════
    #  RANGE / ZOOM
    # ══════════════════════════════════════════════════════
    def _change_range(self):
        dlg = DateRangeDialog(self, self.range_from, self.range_to)
        if dlg.result:
            self.range_from, self.range_to = dlg.result
            self._update_range_label()
            self.gantt.set_data(self.tasks_filtered, self.facts, self.fact_info)

    def _fit_range(self):
        task_rows = [t for t in self.tasks if (t.get("row_kind") or "task") == "task"]
        if not task_rows:
            messagebox.showinfo(
                "ГПР", "Нет работ для определения диапазона.", parent=self
            )
            return
    
        starts = [
            _to_date(t["plan_start"])
            for t in task_rows
            if _to_date(t.get("plan_start"))
        ]
        finishes = [
            _to_date(t["plan_finish"])
            for t in task_rows
            if _to_date(t.get("plan_finish"))
        ]
    
        if not starts or not finishes:
            messagebox.showinfo(
                "ГПР", "Нет работ с валидными датами.", parent=self
            )
            return
    
        d0 = min(starts)
        d1 = max(finishes)
        self.range_from = d0 - timedelta(days=7)
        self.range_to = d1 + timedelta(days=7)
        self._update_range_label()
        self.gantt.set_data(self.tasks_filtered, self.facts, self.fact_info)

    def _zoom(self, delta):
        self.gantt.day_px = max(6, min(50, self.gantt.day_px + delta))
        self.gantt._header_cache_key = None
        self.gantt._schedule_heavy_redraw()

    # ══════════════════════════════════════════════════════
    #  CRUD
    # ══════════════════════════════════════════════════════
    def _find_task_idx(self) -> Optional[int]:
        sel = self.tree.selection()
        if not sel:
            return None
        iid = sel[0]

        if iid.startswith("db_"):
            try:
                tid = int(iid[3:])
                for i, t in enumerate(self.tasks):
                    if t.get("id") is not None and int(t["id"]) == tid:
                        return i
            except (ValueError, TypeError):
                pass

        try:
            tree_idx = self.tree.index(iid)
            if 0 <= tree_idx < len(self.tasks_filtered):
                task_ref = self.tasks_filtered[tree_idx]
                for i, t in enumerate(self.tasks):
                    if t is task_ref:
                        return i
        except (tk.TclError, ValueError):
            pass

        return None

    def _open_task_dialog(self, init=None):
        uid = (self.app_ref.current_user or {}).get("id")

        if not hasattr(self, "_ext_dialog_func"):
            try:
                from gpr_task_dialog import open_task_dialog as _func
                self._ext_dialog_func = _func
                logger.info("gpr_task_dialog loaded successfully")
            except ImportError as e:
                logger.warning("gpr_task_dialog not available: %s", e)
                self._ext_dialog_func = None

        if self._ext_dialog_func is not None:
            try:
                return self._ext_dialog_func(
                    self, self.work_types, self.uoms,
                    init=init, user_id=uid
                )
            except Exception:
                logger.exception(
                    "External task dialog error, falling back to built-in"
                )

        logger.warning("Using built-in TaskEditDialog (no gpr_task_dialog)")
        dlg = TaskEditDialog(self, self.work_types, self.uoms, init=init)
        return dlg.result

    def _add_task(self):
        if not self.plan_id:
            messagebox.showinfo("ГПР", "Сначала откройте объект.", parent=self)
            return

        result = self._open_task_dialog(
            init={
                "plan_start": self.range_from,
                "plan_finish": self.range_from,
                "row_kind": "task",
            }
        )
        if not result:
            return

        t = dict(result)
        t["id"] = None
        t["row_kind"] = "task"
        t["work_type_name"] = next(
            (
                w["name"]
                for w in self.work_types
                if int(w["id"]) == int(t["work_type_id"])
            ),
            "",
        )
        t["sort_order"] = len(self.tasks) * 10
        t["plan_start"] = _to_date(t.get("plan_start")) or _today()
        t["plan_finish"] = _to_date(t.get("plan_finish")) or _today()

        self.tasks.append(t)
        self._recalc_sort_order()
        self._apply_filter()
        self._update_summary()

    def _add_group(self):
        if not self.plan_id:
            messagebox.showinfo("ГПР", "Сначала откройте объект.", parent=self)
            return
    
        name = simpledialog.askstring(
            "Группа",
            "Введите название группы:",
            parent=self,
        )
        if not name:
            return
    
        wt_id = int(self.work_types[0]["id"]) if self.work_types else 1
    
        t = {
            "id": None,
            "row_kind": "group",
            "work_type_id": wt_id,
            "work_type_name": "",
            "name": name.strip(),
            "uom_code": None,
            "plan_qty": None,
            "plan_start": self.range_from,
            "plan_finish": self.range_from,
            "status": "planned",
            "is_milestone": False,
            "sort_order": len(self.tasks) * 10,
        }
    
        self.tasks.append(t)
        self._recalc_sort_order()
        self._apply_filter()
        self._update_summary()

    def _add_title(self):
        if not self.plan_id:
            messagebox.showinfo("ГПР", "Сначала откройте объект.", parent=self)
            return
    
        name = simpledialog.askstring(
            "Титульная строка",
            "Введите текст титульной строки:",
            parent=self,
        )
        if not name:
            return
    
        wt_id = int(self.work_types[0]["id"]) if self.work_types else 1
    
        t = {
            "id": None,
            "row_kind": "title",
            "work_type_id": wt_id,
            "work_type_name": "",
            "name": name.strip(),
            "uom_code": None,
            "plan_qty": None,
            "plan_start": self.range_from,
            "plan_finish": self.range_from,
            "status": "planned",
            "is_milestone": False,
            "sort_order": len(self.tasks) * 10,
        }
    
        self.tasks.append(t)
        self._recalc_sort_order()
        self._apply_filter()
        self._update_summary()

    def _edit_selected(self):
        idx = self._find_task_idx()
        if idx is None:
            messagebox.showinfo(
                "ГПР", "Выберите работу для редактирования.", parent=self
            )
            return
    
        t0 = self.tasks[idx]
        row_kind = (t0.get("row_kind") or "task").strip()
    
        if row_kind in ("group", "title"):
            title = "Группа" if row_kind == "group" else "Титульная строка"
            name = simpledialog.askstring(
                title,
                "Введите новый текст:",
                initialvalue=t0.get("name", ""),
                parent=self,
            )
            if not name:
                return
            t0["name"] = name.strip()
            self._apply_filter()
            self._update_summary()
            return
    
        result = self._open_task_dialog(init=t0)
        if not result:
            return
    
        upd = dict(result)
        upd["id"] = t0.get("id")
        upd["row_kind"] = "task"
        upd["sort_order"] = t0.get("sort_order", idx * 10)
        upd["work_type_name"] = next(
            (
                w["name"]
                for w in self.work_types
                if int(w["id"]) == int(upd["work_type_id"])
            ),
            "",
        )
        upd["plan_start"] = _to_date(upd.get("plan_start")) or _today()
        upd["plan_finish"] = _to_date(upd.get("plan_finish")) or _today()
    
        task_id = t0.get("id")
        assignments = upd.pop("_assignments", None)
        facts_payload = upd.pop("_facts", None)
        facts_changed = bool(upd.pop("_facts_changed", False))
    
        uid = (self.app_ref.current_user or {}).get("id")
    
        if task_id and assignments is not None:
            try:
                from gpr_task_dialog import _EmployeeService
                _EmployeeService.save_task_assignments(task_id, assignments, uid)
            except ImportError:
                logger.warning(
                    "gpr_task_dialog not available — assignments not saved"
                )
            except Exception as e:
                logger.exception("Save assignments error")
                messagebox.showwarning(
                    "ГПР",
                    f"Ошибка сохранения назначений:\n{e}",
                    parent=self,
                )
    
        if task_id and facts_changed:
            try:
                from gpr_task_dialog import _TaskFactService
                _TaskFactService.save_task_facts(task_id, facts_payload or [], uid)
    
                fact_info_map = GprService.load_task_fact_info([task_id])
    
                if task_id in fact_info_map:
                    self.fact_info[task_id] = fact_info_map[task_id]
                    self.facts[task_id] = float(
                        fact_info_map[task_id].get("fact_qty_total") or 0
                    )
                else:
                    self.fact_info.pop(task_id, None)
                    self.facts.pop(task_id, None)
    
            except ImportError:
                logger.warning("gpr_task_dialog not available — facts not saved")
            except Exception as e:
                logger.exception("Save facts error")
                messagebox.showwarning(
                    "ГПР",
                    f"Ошибка сохранения факта:\n{e}",
                    parent=self,
                )
    
        self.tasks[idx] = upd
        self._apply_filter()
        self._update_summary()

    def _delete_selected(self):
        idx = self._find_task_idx()
        if idx is None:
            messagebox.showinfo(
                "ГПР", "Выберите строку для удаления.", parent=self
            )
            return
        row_name = self.tasks[idx].get("name", "")
        if not messagebox.askyesno(
            "ГПР",
            f"Удалить строку «{row_name}»?",
            parent=self,
        ):
            return
        self.tasks.pop(idx)
        self._recalc_sort_order()
        self._apply_filter()
        self._update_summary()

    def _move_selected_up(self):
        idx = self._find_task_idx()
        if idx is None:
            messagebox.showinfo("ГПР", "Выберите строку.", parent=self)
            return
        if idx <= 0:
            return

        task_ref = self.tasks[idx]
        self.tasks[idx - 1], self.tasks[idx] = self.tasks[idx], self.tasks[idx - 1]
        self._recalc_sort_order()
        self._apply_filter()
        self._update_summary()
        self._preserve_selection_by_task(task_ref)

    def _move_selected_down(self):
        idx = self._find_task_idx()
        if idx is None:
            messagebox.showinfo("ГПР", "Выберите строку.", parent=self)
            return
        if idx >= len(self.tasks) - 1:
            return

        task_ref = self.tasks[idx]
        self.tasks[idx], self.tasks[idx + 1] = self.tasks[idx + 1], self.tasks[idx]
        self._recalc_sort_order()
        self._apply_filter()
        self._update_summary()
        self._preserve_selection_by_task(task_ref)

    def _apply_template(self):
        if not self.plan_id:
            messagebox.showinfo(
                "ГПР", "Сначала откройте объект.", parent=self
            )
            return
        try:
            tpls = GprService.load_templates()
        except Exception as e:
            logger.exception("Load templates error")
            messagebox.showerror(
                "ГПР", f"Ошибка загрузки шаблонов:\n{e}", parent=self
            )
            return
        if not tpls:
            messagebox.showinfo("ГПР", "Шаблонов нет.", parent=self)
            return

        dlg = TemplateSelectDialog(self, tpls)
        if not dlg.result:
            return

        try:
            tt = GprService.load_template_tasks(dlg.result)
        except Exception as e:
            logger.exception("Load template tasks error")
            messagebox.showerror(
                "ГПР", f"Ошибка загрузки задач шаблона:\n{e}", parent=self
            )
            return
        if not tt:
            messagebox.showinfo(
                "ГПР", "В шаблоне нет задач.", parent=self
            )
            return
        if self.tasks and not messagebox.askyesno(
            "ГПР", "Заменить текущие работы шаблоном?", parent=self
        ):
            return

        base = self.range_from
        out = []
        for i, x in enumerate(tt):
            row_kind = (x.get("row_kind") or "task").strip()
            wid = int(x["work_type_id"])
            wn = next(
                (w["name"] for w in self.work_types if int(w["id"]) == wid),
                "",
            )

            out.append(
                dict(
                    id=None,
                    row_kind=row_kind,
                    work_type_id=wid,
                    work_type_name="" if row_kind != "task" else wn,
                    name=x["name"],
                    uom_code=x.get("uom_code"),
                    plan_qty=x.get("default_qty"),
                    plan_start=base,
                    plan_finish=base,
                    status="planned",
                    is_milestone=bool(x.get("is_milestone")),
                    sort_order=int(
                        x.get("sort_order")
                        if x.get("sort_order") is not None
                        else i * 10
                    ),
                )
            )
        self.tasks = out
        self._recalc_sort_order()
        self._apply_filter()
        self._update_summary()

    def _save(self):
        if not self.plan_id:
            messagebox.showinfo(
                "ГПР", "Сначала откройте объект.", parent=self
            )
            return
    
        errors = []
        for i, t in enumerate(self.tasks):
            row_kind = (t.get("row_kind") or "task").strip()
            name = (t.get("name") or "").strip()
    
            if not name:
                errors.append(f"Строка {i + 1}: нет названия")
                continue
    
            if row_kind != "task":
                continue
    
            ds = _to_date(t.get("plan_start"))
            df = _to_date(t.get("plan_finish"))
            if not ds or not df:
                errors.append(f"«{name}»: невалидные даты")
            elif df < ds:
                errors.append(f"«{name}»: окончание раньше начала")
    
        if errors:
            msg = "Ошибки валидации:\n\n" + "\n".join(errors[:10])
            if len(errors) > 10:
                msg += f"\n\n...и ещё {len(errors) - 10} ошибок"
            messagebox.showwarning("ГПР", msg, parent=self)
            return
    
        uid = (self.app_ref.current_user or {}).get("id")
        try:
            GprService.replace_plan_tasks(self.plan_id, uid, self.tasks)
            self.tasks = GprService.load_plan_tasks(self.plan_id)
            for t in self.tasks:
                t["row_kind"] = (t.get("row_kind") or "task").strip()
    
            tids = [
                t["id"] for t in self.tasks
                if t.get("id") and (t.get("row_kind") or "task") == "task"
            ]
    
            self.fact_info = GprService.load_task_fact_info(tids)
            self.facts = {
                task_id: float(v.get("fact_qty_total") or 0)
                for task_id, v in self.fact_info.items()
            }
    
            self.plan_info = GprService.get_or_create_current_plan(
                self.object_db_id, uid
            )
            self._update_plan_info()
            self._apply_filter()
            self._update_summary()
            self._refresh_registry()
            messagebox.showinfo("ГПР", "Сохранено успешно.", parent=self)
        except Exception as e:
            logger.exception("GPR save error")
            messagebox.showerror(
                "ГПР", f"Ошибка сохранения:\n{e}", parent=self
            )

    @staticmethod
    def _xl_fill(color: str) -> PatternFill:
        """Excel fill из hex-цвета."""
        return PatternFill(
            "solid",
            fgColor=(color or "FFFFFF").replace("#", "").upper()
        )

def _import_excel(self):
    if not self.plan_id:
        messagebox.showinfo("ГПР", "Сначала откройте объект.", parent=self)
        return

    if not HAS_OPENPYXL:
        messagebox.showwarning(
            "ГПР",
            "Для импорта необходима библиотека openpyxl.\n"
            "Установите: pip install openpyxl",
            parent=self,
        )
        return

    path = filedialog.askopenfilename(
        parent=self,
        title="Выбрать Excel-файл для импорта ГПР",
        filetypes=[("Excel", "*.xlsx"), ("Все файлы", "*.*")],
    )
    if not path:
        return

    try:
        result = GprExcelImportService.import_tasks_from_excel(
            path=path,
            work_types=self.work_types,
            uoms=self.uoms,
        )
    except Exception as e:
        logger.exception("GPR excel import parse error")
        messagebox.showerror(
            "ГПР",
            f"Ошибка чтения Excel:\n{e}",
            parent=self,
        )
        return

    errors = result.get("errors") or []
    tasks = result.get("tasks") or []

    if errors:
        preview = "\n".join(errors[:15])
        if len(errors) > 15:
            preview += f"\n... и ещё {len(errors) - 15} ошибок"
        messagebox.showwarning(
            "ГПР",
            f"Импорт невозможен. Обнаружены ошибки:\n\n{preview}",
            parent=self,
        )
        return

    if not tasks:
        messagebox.showinfo(
            "ГПР",
            "В файле не найдено строк для импорта.",
            parent=self,
        )
        return

    if self.tasks:
        ok = messagebox.askyesno(
            "ГПР",
            f"Импортировать {len(tasks)} строк и заменить текущий список работ?",
            parent=self,
        )
        if not ok:
            return

    self.tasks = tasks
    self._recalc_sort_order()
    self._apply_filter()
    self._update_summary()

    messagebox.showinfo(
        "ГПР",
        f"Импорт выполнен.\nЗагружено строк: {len(tasks)}\n\n"
        "Проверьте данные и нажмите 'СОХРАНИТЬ'.",
        parent=self,
    )
    
    def _export_excel_gantt_sheet(self, wb, obj, obj_name: str) -> None:
        """
        Создаёт второй лист Excel с диаграммой Ганта.
        Использует текущий диапазон self.range_from / self.range_to.
        """
        rows = self.tasks
    
        ws = wb.create_sheet("Диаграмма Ганта")
    
        d0 = _to_date(self.range_from) or _today()
        d1 = _to_date(self.range_to) or d0
        if d1 < d0:
            d0, d1 = d1, d0
    
        days = (d1 - d0).days + 1
    
        fixed_cols = [
            ("№", 6),
            ("Тип работ", 18),
            ("Вид работ", 36),
            ("Начало", 12),
            ("Окончание", 12),
            ("Статус", 16),
            ("Людей", 10),
        ]
        gantt_col_start = len(fixed_cols) + 1
        total_cols = len(fixed_cols) + days
    
        addr = (obj or {}).get("address", "") if obj else ""
        title = f"Диаграмма Ганта: {obj_name}"
        if addr:
            title += f" — {addr}"
    
        thin_side = Side(style="thin", color="D0D0D0")
        thin_border = Border(
            left=thin_side, right=thin_side,
            top=thin_side, bottom=thin_side
        )
    
        header_fill = PatternFill("solid", fgColor="D6DCE4")
        month_fill = PatternFill("solid", fgColor="D6DBE0")
        weekday_fill = PatternFill("solid", fgColor="F3F4F6")
        weekend_fill = PatternFill("solid", fgColor="FFECEC")
        group_fill = PatternFill("solid", fgColor="EEF5FF")
        title_fill = PatternFill("solid", fgColor="DFF1FF")
        progress_fill = PatternFill("solid", fgColor="388E3C")
    
        status_fill = {
            "planned": self._xl_fill("#90caf9"),
            "in_progress": self._xl_fill("#ffcc80"),
            "done": self._xl_fill("#a5d6a7"),
            "paused": self._xl_fill("#fff176"),
            "canceled": self._xl_fill("#ef9a9a"),
        }
    
        month_names = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
        }
        weekday_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=total_cols)
        c = ws.cell(1, 1, title)
        c.font = Font(bold=True, size=12)
        c.alignment = Alignment(horizontal="left", vertical="center")
    
        month_row = 2
        day_row = 3
        week_row = 4
        data_row = 5
    
        for col_idx, (caption, width) in enumerate(fixed_cols, start=1):
            ws.column_dimensions[get_column_letter(col_idx)].width = width
            ws.merge_cells(
                start_row=month_row, start_column=col_idx,
                end_row=week_row, end_column=col_idx
            )
            cell = ws.cell(month_row, col_idx, caption)
            cell.font = Font(bold=True, size=10)
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = thin_border
    
        cur = date(d0.year, d0.month, 1)
        while cur <= d1:
            month_last_day = calendar.monthrange(cur.year, cur.month)[1]
            ms = max(cur, d0)
            me = min(date(cur.year, cur.month, month_last_day), d1)
    
            c0 = gantt_col_start + (ms - d0).days
            c1 = gantt_col_start + (me - d0).days
    
            if c0 != c1:
                ws.merge_cells(
                    start_row=month_row, start_column=c0,
                    end_row=month_row, end_column=c1
                )
    
            mcell = ws.cell(month_row, c0, f"{month_names[cur.month]} {cur.year}")
            mcell.font = Font(bold=True, size=10)
            mcell.fill = month_fill
            mcell.alignment = Alignment(horizontal="center", vertical="center")
    
            if cur.month == 12:
                cur = date(cur.year + 1, 1, 1)
            else:
                cur = date(cur.year, cur.month + 1, 1)
    
        for i in range(days):
            col = gantt_col_start + i
            dt = d0 + timedelta(days=i)
    
            ws.column_dimensions[get_column_letter(col)].width = 3.2
            fill = weekend_fill if dt.weekday() >= 5 else weekday_fill
    
            day_cell = ws.cell(day_row, col, dt.day)
            day_cell.font = Font(bold=True, size=8)
            day_cell.fill = fill
            day_cell.alignment = Alignment(horizontal="center", vertical="center")
            day_cell.border = thin_border
    
            wd_cell = ws.cell(week_row, col, weekday_names[dt.weekday()])
            wd_cell.font = Font(size=8)
            wd_cell.fill = fill
            wd_cell.alignment = Alignment(horizontal="center", vertical="center")
            wd_cell.border = thin_border
    
        excel_no = 0
        last_row = data_row - 1
    
        for row_num, t in enumerate(rows, start=data_row):
            last_row = row_num
            ws.row_dimensions[row_num].height = 20
    
            row_kind = (t.get("row_kind") or "task").strip()
    
            if row_kind == "group":
                for col in range(1, total_cols + 1):
                    ws.cell(row_num, col).fill = group_fill
                ws.cell(row_num, 2, "ГРУППА").font = Font(bold=True)
                ws.cell(row_num, 3, t.get("name", "")).font = Font(bold=True)
                ws.cell(row_num, 3).alignment = Alignment(horizontal="left", vertical="center")
                continue
    
            if row_kind == "title":
                for col in range(1, total_cols + 1):
                    ws.cell(row_num, col).fill = title_fill
                ws.cell(row_num, 2, "ТИТУЛ").font = Font(bold=True)
                ws.cell(row_num, 3, t.get("name", "")).font = Font(bold=True)
                ws.cell(row_num, 3).alignment = Alignment(horizontal="left", vertical="center")
                continue
    
            excel_no += 1
    
            ds = _to_date(t.get("plan_start"))
            df = _to_date(t.get("plan_finish"))
            st_code = (t.get("status") or "planned").strip()
            st_label = STATUS_LABELS.get(st_code, st_code)
    
            tid = t.get("id")
            workers_last = ""
            if tid is not None:
                info = self.fact_info.get(tid) or {}
                if info.get("workers_last") is not None:
                    workers_last = info["workers_last"]
    
            left_values = [
                excel_no,
                t.get("work_type_name", ""),
                t.get("name", ""),
                _fmt_date(ds) if ds else "",
                _fmt_date(df) if df else "",
                st_label,
                workers_last,
            ]
    
            for col_idx, val in enumerate(left_values, start=1):
                cell = ws.cell(row_num, col_idx, val)
                cell.border = thin_border
                if col_idx in (1, 4, 5, 6, 7):
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                else:
                    cell.alignment = Alignment(horizontal="left", vertical="center")
    
            st_fill = status_fill.get(st_code)
            if st_fill:
                ws.cell(row_num, 6).fill = st_fill
    
            if not ds or not df:
                continue
            if df < d0 or ds > d1:
                continue
    
            clip_start = max(ds, d0)
            clip_finish = min(df, d1)
    
            col_start = gantt_col_start + (clip_start - d0).days
            col_finish = gantt_col_start + (clip_finish - d0).days
    
            bar_fill = status_fill.get(st_code, self._xl_fill("#90caf9"))
    
            for col in range(col_start, col_finish + 1):
                cell = ws.cell(row_num, col)
                cell.fill = bar_fill
                cell.border = thin_border
    
            pq = _safe_float(t.get("plan_qty"))
            fq = self.facts.get(tid, 0) if tid else 0.0
    
            if pq and pq > 0 and fq > 0:
                pct = max(0.0, min(1.0, fq / pq))
                total_task_days = max(1, (df - ds).days + 1)
                progress_days = max(1, min(total_task_days, int(round(total_task_days * pct))))
                progress_finish = ds + timedelta(days=progress_days - 1)
    
                p0 = max(ds, d0)
                p1 = min(progress_finish, d1)
    
                if p1 >= p0:
                    pcol0 = gantt_col_start + (p0 - d0).days
                    pcol1 = gantt_col_start + (p1 - d0).days
                    for col in range(pcol0, pcol1 + 1):
                        cell = ws.cell(row_num, col)
                        cell.fill = progress_fill
                        cell.border = thin_border
    
            if t.get("is_milestone"):
                milestone_date = min(max(ds, d0), d1)
                mcol = gantt_col_start + (milestone_date - d0).days
                mcell = ws.cell(row_num, mcol, "◆")
                mcell.font = Font(bold=True, color="1A73E8")
                mcell.alignment = Alignment(horizontal="center", vertical="center")
    
        td = _today()
        if d0 <= td <= d1:
            today_col = gantt_col_start + (td - d0).days
            red_side = Side(style="medium", color="FF0000")
            for rr in range(day_row, max(last_row, week_row) + 1):
                cell = ws.cell(rr, today_col)
                old_border = cell.border
                cell.border = Border(
                    left=red_side,
                    right=red_side,
                    top=old_border.top,
                    bottom=old_border.bottom
                )
    
        legend_row = last_row + 2
        ws.cell(legend_row, 1, "Легенда:").font = Font(bold=True)
    
        lc = 2
        for code in STATUS_LIST:
            ws.cell(legend_row, lc, " ").fill = status_fill[code]
            ws.cell(legend_row, lc).border = thin_border
            ws.cell(legend_row, lc + 1, STATUS_LABELS[code])
            lc += 2
    
        ws.cell(legend_row + 1, 2, " ").fill = progress_fill
        ws.cell(legend_row + 1, 2).border = thin_border
        ws.cell(legend_row + 1, 3, "Фактическое выполнение")
    
        ws.freeze_panes = ws.cell(data_row, gantt_col_start)
    
    def _export_excel(self):
        if not self.tasks:
            messagebox.showinfo(
                "ГПР", "Нет данных для выгрузки.", parent=self
            )
            return
    
        if not HAS_OPENPYXL:
            messagebox.showwarning(
                "ГПР",
                "Для экспорта необходима библиотека openpyxl.\n"
                "Установите: pip install openpyxl",
                parent=self,
            )
            return
    
        obj = next(
            (o for o in self.objects if int(o["id"]) == self.object_db_id),
            None,
        )
        if obj:
            obj_name = obj.get("short_name") or obj.get("address") or "объект"
        else:
            obj_name = "объект"
    
        default_name = f"ГПР_{obj_name}_{_today().strftime('%Y%m%d')}.xlsx"
        default_name = "".join(
            c if c.isalnum() or c in "._- ()" else "_"
            for c in default_name
        )
    
        path = filedialog.asksaveasfilename(
            parent=self,
            title="Сохранить ГПР в Excel",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel", "*.xlsx"), ("Все файлы", "*.*")],
        )
        if not path:
            return
    
        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "ГПР"
    
            if obj:
                addr = obj.get("address", "")
                ws.merge_cells("A1:L1")
                title_cell = ws.cell(1, 1, f"ГПР: {obj_name} — {addr}")
                title_cell.font = Font(bold=True, size=12)
                title_cell.alignment = Alignment(horizontal="left")
                data_start_row = 3
            else:
                data_start_row = 1
    
            headers = [
                "№",
                "Тип работ",
                "Вид работ",
                "Ед. изм.",
                "Объём план",
                "Начало",
                "Окончание",
                "Длительность (дн.)",
                "Статус",
                "Факт (накоп.)",
                "% выполнения",
                "Людей",
            ]
            widths = [6, 22, 36, 8, 14, 14, 14, 14, 16, 14, 14, 10]
    
            hdr_row = data_start_row
            for i, h in enumerate(headers, 1):
                cell = ws.cell(hdr_row, i, h)
                cell.font = Font(bold=True, size=10)
                cell.fill = PatternFill("solid", fgColor="D6DCE4")
                cell.alignment = Alignment(
                    horizontal="center",
                    vertical="center",
                    wrap_text=True,
                )
    
            for i, w in enumerate(widths, 1):
                ws.column_dimensions[get_column_letter(i)].width = w
    
            ws.freeze_panes = f"A{hdr_row + 1}"
    
            status_fill = {
                "planned": PatternFill("solid", fgColor="D6EAFF"),
                "in_progress": PatternFill("solid", fgColor="FFF3CD"),
                "done": PatternFill("solid", fgColor="D4EDDA"),
                "paused": PatternFill("solid", fgColor="FFF9C4"),
                "canceled": PatternFill("solid", fgColor="F8D7DA"),
            }
    
            excel_no = 0
            for row_num, t in enumerate(self.tasks, start=hdr_row + 1):
                row_kind = (t.get("row_kind") or "task").strip()
    
                if row_kind == "group":
                    ws.cell(row_num, 2, "ГРУППА").font = Font(bold=True)
                    ws.cell(row_num, 3, t.get("name", "")).font = Font(bold=True)
                    for c in range(1, 13):
                        ws.cell(row_num, c).fill = PatternFill("solid", fgColor="EEF5FF")
                    continue
    
                if row_kind == "title":
                    ws.cell(row_num, 2, "ТИТУЛ").font = Font(bold=True)
                    ws.cell(row_num, 3, t.get("name", "")).font = Font(bold=True)
                    for c in range(1, 13):
                        ws.cell(row_num, c).fill = PatternFill("solid", fgColor="DFF1FF")
                    continue
    
                excel_no += 1
                ds = _to_date(t.get("plan_start"))
                df = _to_date(t.get("plan_finish"))
                dur = (df - ds).days + 1 if ds and df else ""
    
                pq = _safe_float(t.get("plan_qty"))
                tid = t.get("id")
                fq = self.facts.get(tid, 0) if tid else 0
                pct = ""
                if pq and pq > 0:
                    pct = f"{min(100.0, fq / pq * 100):.1f}%"
    
                workers_last = ""
                if tid is not None:
                    info = self.fact_info.get(tid) or {}
                    if info.get("workers_last") is not None:
                        workers_last = info["workers_last"]
    
                st_code = t.get("status", "planned")
                st_label = STATUS_LABELS.get(st_code, st_code)
    
                values = [
                    excel_no,
                    t.get("work_type_name", ""),
                    t.get("name", ""),
                    t.get("uom_code") or "",
                    _fmt_qty(pq) if pq else "",
                    _fmt_date(ds) if ds else "",
                    _fmt_date(df) if df else "",
                    dur,
                    st_label,
                    _fmt_qty(fq) if fq else "",
                    pct,
                    workers_last,
                ]
    
                for col, val in enumerate(values, 1):
                    cell = ws.cell(row_num, col, val)
                    cell.alignment = Alignment(
                        horizontal="center", vertical="center"
                    )
                    if col == 9:
                        fill = status_fill.get(st_code)
                        if fill:
                            cell.fill = fill
    
            last_row = hdr_row + len(self.tasks) + 1
            task_count = sum(
                1 for t in self.tasks if (t.get("row_kind") or "task") == "task"
            )
            done_cnt = sum(
                1 for t in self.tasks
                if (t.get("row_kind") or "task") == "task" and t.get("status") == "done"
            )
    
            ws.cell(last_row, 2, f"Итого работ: {task_count}").font = Font(bold=True)
            ws.cell(last_row, 9, f"Выполнено: {done_cnt}").font = Font(bold=True)
            ws.cell(
                last_row + 1,
                2,
                f"Выгружено: {_today().strftime('%d.%m.%Y')}",
            ).font = Font(italic=True, size=8, color="888888")
    
            self._export_excel_gantt_sheet(wb, obj, obj_name)
    
            wb.save(path)
            messagebox.showinfo(
                "ГПР",
                f"Файл сохранён:\n{path}\n\n"
                f"Листы: 'ГПР' и 'Диаграмма Ганта'",
                parent=self
            )
    
        except PermissionError:
            messagebox.showerror(
                "ГПР",
                f"Нет доступа к файлу:\n{path}\n\n"
                "Возможно файл открыт в другой программе.",
                parent=self,
            )
        except Exception as e:
            logger.exception("GPR excel export error")
            messagebox.showerror(
                "ГПР", f"Ошибка экспорта:\n{e}", parent=self
            )

# ═══════════════════════════════════════════════════════════════
#  API for main_app
# ═══════════════════════════════════════════════════════════════
def create_gpr_page(parent, app_ref) -> GprPage:
    """Фабричная функция — вызывается из main_app._show_page."""
    return GprPage(parent, app_ref=app_ref)
