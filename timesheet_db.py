from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from timesheet_common import (
    calc_row_totals,
    make_row_key,
    normalize_hours_list,
    normalize_spaces,
    normalize_tbn,
)

logger = logging.getLogger(__name__)

db_connection_pool = None
USING_SHARED_POOL = False


# ============================================================
# Пул соединений / безопасная работа с БД
# ============================================================

def set_db_pool(pool) -> None:
    global db_connection_pool, USING_SHARED_POOL
    db_connection_pool = pool
    USING_SHARED_POOL = True


def get_db_connection():
    if db_connection_pool:
        return db_connection_pool.getconn()
    raise RuntimeError("Пул соединений не был установлен из главного приложения.")


def release_db_connection(conn) -> None:
    """
    КРИТИЧНО:
    перед возвратом соединения в пул делаем rollback,
    чтобы не оставлять его в 'idle in transaction' / aborted state.
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
        if db_connection_pool:
            try:
                db_connection_pool.putconn(conn)
            except Exception:
                logger.exception("Не удалось вернуть соединение в пул")
        else:
            try:
                conn.close()
            except Exception:
                pass


@contextmanager
def db_cursor(dict_rows: bool = False) -> Iterator[tuple[Any, Any]]:
    """
    Унифицированный контекст:
    - берёт соединение из пула;
    - открывает cursor;
    - commit на успехе;
    - rollback на ошибке;
    - безопасно возвращает соединение в пул.
    """
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


# ============================================================
# Внутренние helpers
# ============================================================

def _norm_header_object_id(value: Optional[str]) -> str:
    return normalize_spaces(value or "")


def _norm_header_department(value: Optional[str]) -> str:
    return normalize_spaces(value or "")


def _norm_header_address(value: str) -> str:
    return normalize_spaces(value or "")


def _header_where_sql() -> str:
    return """
        COALESCE(h.object_id, '') = COALESCE(%s, '')
        AND h.object_addr = %s
        AND COALESCE(h.department, '') = COALESCE(%s, '')
        AND h.year = %s
        AND h.month = %s
    """


def _header_params(
    object_id: Optional[str],
    object_addr: str,
    department: Optional[str],
    year: int,
    month: int,
) -> list[Any]:
    return [
        _norm_header_object_id(object_id),
        _norm_header_address(object_addr),
        _norm_header_department(department),
        int(year),
        int(month),
    ]


def _find_header_id_by_key(
    cur,
    object_id: Optional[str],
    object_addr: str,
    department: Optional[str],
    year: int,
    month: int,
    user_id: int,
) -> Optional[int]:
    cur.execute(
        f"""
        SELECT h.id
        FROM timesheet_headers h
        WHERE {_header_where_sql()}
          AND h.user_id = %s
        ORDER BY h.updated_at DESC NULLS LAST, h.id DESC
        LIMIT 1
        """,
        _header_params(object_id, object_addr, department, year, month) + [int(user_id)],
    )
    row = cur.fetchone()
    if not row:
        return None
    return int(row[0])


def _load_header_meta_by_id(cur, header_id: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            h.id,
            COALESCE(h.object_id, '') AS object_id,
            COALESCE(h.object_addr, '') AS object_addr,
            COALESCE(h.department, '') AS department,
            h.year,
            h.month,
            h.user_id,
            h.object_db_id,
            h.created_at,
            h.updated_at
        FROM timesheet_headers h
        WHERE h.id = %s
        """,
        (int(header_id),),
    )
    row = cur.fetchone()
    if not row:
        return None

    if isinstance(row, dict):
        return dict(row)

    return {
        "id": row[0],
        "object_id": row[1] or "",
        "object_addr": row[2] or "",
        "department": row[3] or "",
        "year": int(row[4]),
        "month": int(row[5]),
        "user_id": int(row[6]),
        "object_db_id": row[7],
        "created_at": row[8],
        "updated_at": row[9],
    }


# ============================================================
# Объекты / заголовки табелей
# ============================================================

def find_object_db_id_by_excel_or_address(cur, excel_id: Optional[str], address: str) -> Optional[int]:
    excel_id_norm = normalize_spaces(excel_id or "")
    addr_norm = normalize_spaces(address or "")

    if excel_id_norm:
        cur.execute(
            """
            SELECT id
            FROM objects
            WHERE COALESCE(NULLIF(excel_id, ''), '') = %s
            ORDER BY id
            LIMIT 1
            """,
            (excel_id_norm,),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])

    cur.execute(
        """
        SELECT id
        FROM objects
        WHERE address = %s
        ORDER BY id
        LIMIT 1
        """,
        (addr_norm,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def load_timesheet_header_by_id(header_id: int) -> Optional[Dict[str, Any]]:
    with db_cursor(dict_rows=True) as (_conn, cur):
        return _load_header_meta_by_id(cur, int(header_id))


def load_timesheet_full_by_header_id(header_id: int) -> Optional[Dict[str, Any]]:
    with db_cursor(dict_rows=True) as (_conn, cur):
        header = _load_header_meta_by_id(cur, int(header_id))
        if not header:
            return None

        year = int(header["year"])
        month = int(header["month"])

        cur.execute(
            """
            SELECT fio, tbn, hours_raw, total_days, total_hours, night_hours, overtime_day, overtime_night
            FROM timesheet_rows
            WHERE header_id = %s
            ORDER BY fio, tbn
            """,
            (int(header_id),),
        )

        rows: List[Dict[str, Any]] = []
        for r in cur.fetchall():
            if isinstance(r, dict):
                fio = r.get("fio") or ""
                tbn = r.get("tbn") or ""
                hours_raw = r.get("hours_raw")
                total_days = r.get("total_days")
                total_hours = r.get("total_hours")
                night_hours = r.get("night_hours")
                overtime_day = r.get("overtime_day")
                overtime_night = r.get("overtime_night")
            else:
                fio, tbn, hours_raw, total_days, total_hours, night_hours, overtime_day, overtime_night = r

            hours = normalize_hours_list(hours_raw, year, month)
            rows.append(
                {
                    "fio": fio or "",
                    "tbn": tbn or "",
                    "hours": hours,
                    "hours_raw": hours[:],
                    "total_days": int(total_days) if total_days is not None else None,
                    "total_hours": float(total_hours) if total_hours is not None else None,
                    "night_hours": float(night_hours) if night_hours is not None else None,
                    "overtime_day": float(overtime_day) if overtime_day is not None else None,
                    "overtime_night": float(overtime_night) if overtime_night is not None else None,
                }
            )

        header["rows"] = rows
        return header


def upsert_timesheet_header(
    object_id: Optional[str],
    object_addr: str,
    department: str,
    year: int,
    month: int,
    user_id: int,
) -> int:
    """
    Важное отличие от старого кода:
    сначала пытаемся найти существующий header по логическому ключу,
    затем update или insert.

    Это уменьшает зависимость от NULL-поведения ON CONFLICT и
    делает работу стабильнее даже на "грязных" исторических данных.
    """
    object_id_norm = _norm_header_object_id(object_id)
    object_addr_norm = _norm_header_address(object_addr)
    department_norm = _norm_header_department(department)

    if not object_addr_norm:
        raise RuntimeError("Не задан адрес объекта для сохранения табеля.")

    with db_cursor() as (_conn, cur):
        object_db_id = find_object_db_id_by_excel_or_address(
            cur,
            object_id_norm or None,
            object_addr_norm,
        )
        if object_db_id is None:
            raise RuntimeError(
                f"В БД не найден объект (excel_id={object_id_norm!r}, address={object_addr_norm!r}).\n"
                f"Сначала создайте объект в разделе «Объекты»."
            )

        existing_id = _find_header_id_by_key(
            cur,
            object_id_norm,
            object_addr_norm,
            department_norm,
            int(year),
            int(month),
            int(user_id),
        )

        if existing_id is not None:
            cur.execute(
                """
                UPDATE timesheet_headers
                SET
                    object_id = %s,
                    object_addr = %s,
                    department = %s,
                    year = %s,
                    month = %s,
                    user_id = %s,
                    object_db_id = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                (
                    object_id_norm,
                    object_addr_norm,
                    department_norm,
                    int(year),
                    int(month),
                    int(user_id),
                    int(object_db_id),
                    int(existing_id),
                ),
            )
            return int(existing_id)

        cur.execute(
            """
            INSERT INTO timesheet_headers
                (object_id, object_addr, department, year, month, user_id, object_db_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                object_id_norm,
                object_addr_norm,
                department_norm,
                int(year),
                int(month),
                int(user_id),
                int(object_db_id),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Не удалось создать заголовок табеля.")
        return int(row[0])


# ============================================================
# Строки табеля
# ============================================================

def replace_timesheet_rows(
    header_id: int,
    rows: Sequence[Mapping[str, Any]],
    year: int,
    month: int,
) -> None:
    """
    Полная замена строк табеля.
    ВАЖНО:
    - нормализует массив часов по реальному месяцу;
    - totals считает только по валидным дням месяца.
    """
    values: List[tuple[Any, ...]] = []

    for rec in rows:
        fio = normalize_spaces(str(rec.get("fio") or ""))
        tbn = normalize_tbn(rec.get("tbn"))

        if not fio and not tbn:
            continue

        hours_list = normalize_hours_list(rec.get("hours"), year, month)
        totals = rec.get("_totals")
        if not isinstance(totals, dict):
            totals = calc_row_totals(hours_list, year, month)

        total_days = int(totals.get("days") or 0) or None
        total_hours = float(totals.get("hours") or 0.0) or None
        total_night = float(totals.get("night_hours") or 0.0) or None
        total_ot_day = float(totals.get("ot_day") or 0.0) or None
        total_ot_night = float(totals.get("ot_night") or 0.0) or None

        values.append(
            (
                int(header_id),
                fio,
                tbn or None,
                hours_list,
                total_days,
                total_hours,
                total_night,
                total_ot_day,
                total_ot_night,
            )
        )

    with db_cursor() as (_conn, cur):
        cur.execute("DELETE FROM timesheet_rows WHERE header_id = %s", (int(header_id),))

        if not values:
            return

        insert_query = """
            INSERT INTO timesheet_rows
                (header_id, fio, tbn, hours_raw, total_days, total_hours, night_hours, overtime_day, overtime_night)
            VALUES %s
        """
        execute_values(cur, insert_query, values)


def load_timesheet_rows_from_db(
    object_id: Optional[str],
    object_addr: str,
    department: str,
    year: int,
    month: int,
    user_id: int,
) -> List[Dict[str, Any]]:
    with db_cursor() as (_conn, cur):
        header_id = _find_header_id_by_key(
            cur,
            object_id,
            object_addr,
            department,
            int(year),
            int(month),
            int(user_id),
        )
        if header_id is None:
            return []

        cur.execute(
            """
            SELECT fio, tbn, hours_raw
            FROM timesheet_rows
            WHERE header_id = %s
            ORDER BY fio, tbn
            """,
            (int(header_id),),
        )

        result: List[Dict[str, Any]] = []
        for fio, tbn, hours_raw in cur.fetchall():
            hours = normalize_hours_list(hours_raw, year, month)
            result.append(
                {
                    "fio": fio or "",
                    "tbn": tbn or "",
                    "hours": hours,
                }
            )
        return result


def load_timesheet_rows_for_copy_from_db(
    object_id: Optional[str],
    object_addr: str,
    department: str,
    year: int,
    month: int,
    user_id: int,
    with_hours: bool,
) -> List[Dict[str, Any]]:
    with db_cursor() as (_conn, cur):
        header_id = _find_header_id_by_key(
            cur,
            object_id,
            object_addr,
            department,
            int(year),
            int(month),
            int(user_id),
        )
        if header_id is None:
            return []

        cur.execute(
            """
            SELECT fio, tbn, hours_raw
            FROM timesheet_rows
            WHERE header_id = %s
            ORDER BY fio, tbn
            """,
            (int(header_id),),
        )

        result: List[Dict[str, Any]] = []
        for fio, tbn, hours_raw in cur.fetchall():
            hours = normalize_hours_list(hours_raw, year, month) if with_hours else [None] * 31
            result.append(
                {
                    "fio": fio or "",
                    "tbn": tbn or "",
                    "hours": hours,
                }
            )
        return result


def load_timesheet_rows_by_header_id(header_id: int) -> List[Dict[str, Any]]:
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT year, month
            FROM timesheet_headers
            WHERE id = %s
            """,
            (int(header_id),),
        )
        ym = cur.fetchone()
        if not ym:
            return []

        year, month = int(ym[0]), int(ym[1])

        cur.execute(
            """
            SELECT fio, tbn, hours_raw, total_days, total_hours, night_hours, overtime_day, overtime_night
            FROM timesheet_rows
            WHERE header_id = %s
            ORDER BY fio, tbn
            """,
            (int(header_id),),
        )

        result: List[Dict[str, Any]] = []
        for fio, tbn, hours_raw, total_days, total_hours, night_hours, ot_day, ot_night in cur.fetchall():
            hours = normalize_hours_list(hours_raw, year, month)
            result.append(
                {
                    "fio": fio or "",
                    "tbn": tbn or "",
                    "hours_raw": hours,
                    "total_days": int(total_days) if total_days is not None else None,
                    "total_hours": float(total_hours) if total_hours is not None else None,
                    "night_hours": float(night_hours) if night_hours is not None else None,
                    "overtime_day": float(ot_day) if ot_day is not None else None,
                    "overtime_night": float(ot_night) if ot_night is not None else None,
                }
            )
        return result


# ============================================================
# Проверка дублей сотрудников между табелями
# ============================================================

def find_duplicate_employees_for_timesheet(
    object_id: Optional[str],
    object_addr: str,
    department: str,
    year: int,
    month: int,
    user_id: int,
    employees: Sequence[Tuple[str, str]],
) -> List[Dict[str, Any]]:
    """
    Ищет сотрудников, которые уже присутствуют в табелях других пользователей
    по тому же объекту/подразделению/периоду.

    Логика:
    - если у входной строки есть tbn -> матч по (fio, tbn);
    - если tbn пустой -> матч по fio.
    """
    with_tbn: set[tuple[str, str]] = set()
    without_tbn: set[str] = set()

    for fio, tbn in employees:
        fio_norm = normalize_spaces(fio or "")
        tbn_norm = normalize_tbn(tbn)
        if not fio_norm and not tbn_norm:
            continue

        if tbn_norm:
            with_tbn.add(make_row_key(fio_norm, tbn_norm))
        else:
            without_tbn.add(fio_norm.lower())

    if not with_tbn and not without_tbn:
        return []

    with db_cursor(dict_rows=True) as (_conn, cur):
        cur.execute(
            f"""
            SELECT
                h.id AS header_id,
                h.user_id,
                u.username,
                u.full_name,
                r.fio,
                r.tbn
            FROM timesheet_headers h
            JOIN app_users u ON u.id = h.user_id
            JOIN timesheet_rows r ON r.header_id = h.id
            WHERE {_header_where_sql()}
              AND h.user_id <> %s
            ORDER BY h.id, r.fio, r.tbn
            """,
            _header_params(object_id, object_addr, department, year, month) + [int(user_id)],
        )

        result: List[Dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()

        for row in cur.fetchall():
            fio_db = normalize_spaces(row.get("fio") or "")
            tbn_db = normalize_tbn(row.get("tbn"))

            matched = False
            if tbn_db:
                matched = make_row_key(fio_db, tbn_db) in with_tbn
            else:
                matched = fio_db.lower() in without_tbn

            if not matched:
                continue

            dedupe_key = (
                row.get("header_id"),
                row.get("user_id"),
                fio_db.lower(),
                tbn_db,
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            result.append(
                {
                    "header_id": row.get("header_id"),
                    "user_id": row.get("user_id"),
                    "username": row.get("username"),
                    "full_name": row.get("full_name"),
                    "fio": fio_db,
                    "tbn": tbn_db,
                }
            )

        return result


# ============================================================
# Бригадиры
# ============================================================

def load_brigadier_assignments_for_department(department_name: str) -> dict[str, str | None]:
    dep = normalize_spaces(department_name or "")
    if not dep:
        return {}

    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT employee_tbn, brigadier_tbn
            FROM public.employee_brigadiers
            WHERE department = %s
            """,
            (dep,),
        )
        return {normalize_tbn(emp_tbn): normalize_tbn(br_tbn) or None for (emp_tbn, br_tbn) in cur.fetchall()}


def load_brigadier_names_for_department(department_name: str) -> dict[str, str]:
    dep = normalize_spaces(department_name or "")
    if not dep:
        return {}

    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT DISTINCT eb.brigadier_tbn
            FROM public.employee_brigadiers eb
            WHERE eb.department = %s
              AND eb.brigadier_tbn IS NOT NULL
              AND eb.brigadier_tbn <> ''
            """,
            (dep,),
        )
        brig_tbn_list = [normalize_tbn(r[0]) for r in cur.fetchall() if normalize_tbn(r[0])]
        if not brig_tbn_list:
            return {}

        cur.execute(
            """
            SELECT tbn, fio
            FROM public.employees
            WHERE tbn = ANY(%s)
            """,
            (brig_tbn_list,),
        )
        return {normalize_tbn(tbn): normalize_spaces(fio or "") for (tbn, fio) in cur.fetchall()}


def load_brigadiers_map_for_header(header_id: int) -> dict[str, str]:
    with db_cursor(dict_rows=True) as (_conn, cur):
        cur.execute(
            """
            SELECT
                r.tbn AS employee_tbn,
                COALESCE(bfio.fio, '') AS brigadier_fio
            FROM timesheet_rows r
            JOIN timesheet_headers h ON h.id = r.header_id
            LEFT JOIN employee_brigadiers eb
                ON eb.department = COALESCE(h.department, '')
               AND eb.employee_tbn = COALESCE(r.tbn, '')
            LEFT JOIN employees bfio
                ON bfio.tbn = eb.brigadier_tbn
            WHERE r.header_id = %s
            """,
            (int(header_id),),
        )

        out: dict[str, str] = {}
        for row in cur.fetchall():
            tbn = normalize_tbn(row.get("employee_tbn"))
            if not tbn:
                continue
            out[tbn] = normalize_spaces(row.get("brigadier_fio") or "")
        return out


# ============================================================
# Реестры табелей
# ============================================================

def load_user_timesheet_headers(
    user_id: int,
    year: Optional[int],
    month: Optional[int],
    department: Optional[str],
    object_addr_substr: Optional[str],
) -> List[Dict[str, Any]]:
    where: List[str] = ["h.user_id = %s"]
    params: List[Any] = [int(user_id)]

    if year is not None:
        where.append("h.year = %s")
        params.append(int(year))

    if month is not None:
        where.append("h.month = %s")
        params.append(int(month))

    dep_norm = normalize_spaces(department or "")
    if dep_norm:
        where.append("COALESCE(h.department, '') = %s")
        params.append(dep_norm)

    addr_norm = normalize_spaces(object_addr_substr or "")
    if addr_norm:
        where.append("h.object_addr ILIKE %s")
        params.append(f"%{addr_norm}%")

    where_sql = " AND ".join(where)

    with db_cursor(dict_rows=True) as (_conn, cur):
        cur.execute(
            f"""
            SELECT
                h.id,
                h.object_id,
                h.object_addr,
                h.department,
                h.year,
                h.month,
                h.user_id,
                h.created_at,
                h.updated_at
            FROM timesheet_headers h
            WHERE {where_sql}
            ORDER BY h.year DESC, h.month DESC, h.object_addr, COALESCE(h.department, '')
            """,
            params,
        )
        return [dict(r) for r in cur.fetchall()]


def load_all_timesheet_headers(
    year: Optional[int],
    month: Optional[int],
    department: Optional[str],
    object_addr_substr: Optional[str],
    object_id_substr: Optional[str],
) -> List[Dict[str, Any]]:
    where: List[str] = ["1=1"]
    params: List[Any] = []

    if year is not None:
        where.append("h.year = %s")
        params.append(int(year))

    if month is not None:
        where.append("h.month = %s")
        params.append(int(month))

    dep_norm = normalize_spaces(department or "")
    if dep_norm:
        where.append("COALESCE(h.department, '') = %s")
        params.append(dep_norm)

    addr_norm = normalize_spaces(object_addr_substr or "")
    if addr_norm:
        where.append("h.object_addr ILIKE %s")
        params.append(f"%{addr_norm}%")

    object_id_norm = normalize_spaces(object_id_substr or "")
    if object_id_norm:
        where.append("COALESCE(h.object_id, '') ILIKE %s")
        params.append(f"%{object_id_norm}%")

    where_sql = " AND ".join(where)

    with db_cursor(dict_rows=True) as (_conn, cur):
        cur.execute(
            f"""
            SELECT
                h.id,
                h.object_id,
                h.object_addr,
                h.department,
                h.year,
                h.month,
                h.user_id,
                u.username,
                u.full_name,
                h.created_at,
                h.updated_at
            FROM timesheet_headers h
            JOIN app_users u ON u.id = h.user_id
            WHERE {where_sql}
            ORDER BY h.year DESC, h.month DESC, h.object_addr, COALESCE(h.department, ''), u.full_name
            """,
            params,
        )
        return [dict(r) for r in cur.fetchall()]


# ============================================================
# Справочники
# ============================================================

def load_employees_from_db() -> List[Tuple[str, str, str, str, str]]:
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT
                e.fio,
                e.tbn,
                e.position,
                d.name AS dep,
                e.work_schedule
            FROM employees e
            LEFT JOIN departments d ON d.id = e.department_id
            WHERE COALESCE(e.is_fired, FALSE) = FALSE
            ORDER BY e.fio, e.tbn
            """
        )
        return [
            (
                normalize_spaces(r[0] or ""),
                normalize_tbn(r[1]),
                normalize_spaces(r[2] or ""),
                normalize_spaces(r[3] or ""),
                normalize_spaces(r[4] or ""),
            )
            for r in cur.fetchall()
        ]


def load_objects_from_db() -> List[Tuple[str, str]]:
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT COALESCE(NULLIF(excel_id, ''), '') AS code, address
            FROM objects
            ORDER BY address, code
            """
        )
        return [
            (
                normalize_spaces(r[0] or ""),
                normalize_spaces(r[1] or ""),
            )
            for r in cur.fetchall()
        ]


def load_objects_short_for_timesheet() -> List[Tuple[str, str, str]]:
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT
                COALESCE(NULLIF(excel_id, ''), '') AS code,
                address,
                COALESCE(short_name, '') AS short_name
            FROM objects
            ORDER BY address, code
            """
        )
        return [
            (
                normalize_spaces(r[0] or ""),
                normalize_spaces(r[1] or ""),
                normalize_spaces(r[2] or ""),
            )
            for r in cur.fetchall()
        ]


# ============================================================
# Утилиты для открытия табеля по ключу
# ============================================================

def find_timesheet_header_id(
    object_id: Optional[str],
    object_addr: str,
    department: str,
    year: int,
    month: int,
    user_id: int,
) -> Optional[int]:
    with db_cursor() as (_conn, cur):
        return _find_header_id_by_key(
            cur,
            object_id,
            object_addr,
            department,
            int(year),
            int(month),
            int(user_id),
        )


# ============================================================
# Экспортируемые имена
# ============================================================

__all__ = [
    "USING_SHARED_POOL",
    "set_db_pool",
    "get_db_connection",
    "release_db_connection",
    "db_cursor",
    "find_object_db_id_by_excel_or_address",
    "load_timesheet_header_by_id",
    "load_timesheet_full_by_header_id",
    "upsert_timesheet_header",
    "replace_timesheet_rows",
    "load_timesheet_rows_from_db",
    "load_timesheet_rows_for_copy_from_db",
    "load_timesheet_rows_by_header_id",
    "find_duplicate_employees_for_timesheet",
    "load_brigadier_assignments_for_department",
    "load_brigadier_names_for_department",
    "load_brigadiers_map_for_header",
    "load_user_timesheet_headers",
    "load_all_timesheet_headers",
    "load_employees_from_db",
    "load_objects_from_db",
    "load_objects_short_for_timesheet",
    "find_timesheet_header_id",
]
