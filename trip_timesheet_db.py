from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from psycopg2.extras import execute_values

from timesheet_common import (
    calc_row_totals,
    make_row_key,
    normalize_hours_list,
    normalize_spaces,
    normalize_tbn,
)
from timesheet_db import (
    db_cursor,
    find_object_db_id_by_excel_or_address,
)

logger = logging.getLogger(__name__)


def _norm_header_object_id(value: Optional[str]) -> str:
    return normalize_spaces(value or "")


def _norm_header_address(value: str) -> str:
    return normalize_spaces(value or "")


def _header_where_sql() -> str:
    return """
        COALESCE(h.object_id, '') = COALESCE(%s, '')
        AND h.object_addr = %s
        AND h.year = %s
        AND h.month = %s
    """

def _header_params(
    object_id: Optional[str],
    object_addr: str,
    year: int,
    month: int,
) -> list[Any]:
    return [
        _norm_header_object_id(object_id),
        _norm_header_address(object_addr),
        int(year),
        int(month),
    ]


def _find_trip_header_id_by_key(
    cur,
    object_id: Optional[str],
    object_addr: str,
    year: int,
    month: int,
) -> Optional[int]:
    object_id_norm = _norm_header_object_id(object_id)
    object_addr_norm = _norm_header_address(object_addr)

    if object_id_norm:
        cur.execute(
            """
            SELECT h.id
            FROM trip_timesheet_headers h
            WHERE COALESCE(h.object_id, '') = %s
              AND h.year = %s
              AND h.month = %s
            ORDER BY h.updated_at DESC NULLS LAST, h.id DESC
            LIMIT 1
            """,
            (object_id_norm, int(year), int(month)),
        )
    else:
        cur.execute(
            """
            SELECT h.id
            FROM trip_timesheet_headers h
            WHERE h.object_addr = %s
              AND h.year = %s
              AND h.month = %s
            ORDER BY h.updated_at DESC NULLS LAST, h.id DESC
            LIMIT 1
            """,
            (object_addr_norm, int(year), int(month)),
        )

    row = cur.fetchone()
    if not row:
        return None
    return int(row[0])


def _load_trip_header_meta_by_id(cur, header_id: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            h.id,
            COALESCE(h.object_id, '') AS object_id,
            COALESCE(h.object_addr, '') AS object_addr,
            h.year,
            h.month,
            h.user_id,
            h.object_db_id,
            h.created_at,
            h.updated_at
        FROM trip_timesheet_headers h
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
        "year": int(row[3]),
        "month": int(row[4]),
        "user_id": int(row[5]) if row[5] is not None else None,
        "object_db_id": row[6],
        "created_at": row[7],
        "updated_at": row[8],
    }


def upsert_trip_timesheet_header(
    object_id: Optional[str],
    object_addr: str,
    year: int,
    month: int,
) -> int:
    object_id_norm = _norm_header_object_id(object_id)
    object_addr_norm = _norm_header_address(object_addr)

    if not object_addr_norm:
        raise RuntimeError("Не задан адрес объекта для сохранения командировочного табеля.")

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

        existing_id = _find_trip_header_id_by_key(
            cur,
            object_id_norm,
            object_addr_norm,
            int(year),
            int(month),
        )

        if existing_id is not None:
            cur.execute(
                """
                UPDATE trip_timesheet_headers
                SET
                    object_id = %s,
                    object_addr = %s,
                    year = %s,
                    month = %s,
                    object_db_id = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                (
                    object_id_norm,
                    object_addr_norm,
                    int(year),
                    int(month),
                    int(object_db_id),
                    int(existing_id),
                ),
            )
            return int(existing_id)

        cur.execute(
            """
            INSERT INTO trip_timesheet_headers
                (object_id, object_addr, year, month, object_db_id)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                object_id_norm,
                object_addr_norm,
                int(year),
                int(month),
                int(object_db_id),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Не удалось создать заголовок командировочного табеля.")
        return int(row[0])


def replace_trip_timesheet_rows(
    header_id: int,
    rows: Sequence[Mapping[str, Any]],
    year: int,
    month: int,
) -> None:
    values: List[tuple[Any, ...]] = []
    original_records = []

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
        original_records.append(rec)

    with db_cursor() as (_conn, cur):
        # Удаляем старые строки (таблица периодов очистится автоматически из-за CASCADE)
        cur.execute("DELETE FROM trip_timesheet_rows WHERE header_id = %s", (int(header_id),))

        if not values:
            return

        insert_query = """
            INSERT INTO trip_timesheet_rows
                (
                    header_id,
                    fio,
                    tbn,
                    hours_raw,
                    total_days,
                    total_hours,
                    night_hours,
                    overtime_day,
                    overtime_night
                )
            VALUES %s
            RETURNING id
        """
        # Вставляем строки и получаем их сгенерированные ID
        returned_ids = execute_values(cur, insert_query, values, fetch=True)

        # Теперь формируем список всех периодов для вставки
        period_values = []
        for row_id_tuple, rec in zip(returned_ids, original_records):
            row_id = row_id_tuple[0]
            periods = rec.get("trip_periods", [])
            for p in periods:
                if p.get("from") and p.get("to"):
                    period_values.append((row_id, p["from"], p["to"]))

        if period_values:
            period_insert_query = """
                INSERT INTO trip_timesheet_periods (row_id, date_from, date_to)
                VALUES %s
            """
            execute_values(cur, period_insert_query, period_values)


def load_trip_timesheet_rows_from_db(
    object_id: Optional[str],
    object_addr: str,
    year: int,
    month: int,
) -> List[Dict[str, Any]]:
    with db_cursor() as (_conn, cur):
        header_id = _find_trip_header_id_by_key(
            cur,
            object_id,
            object_addr,
            int(year),
            int(month),
        )
        if header_id is None:
            return []

        cur.execute(
            """
            SELECT 
                r.id, r.fio, r.tbn, r.hours_raw, 
                p.date_from, p.date_to
            FROM trip_timesheet_rows r
            LEFT JOIN trip_timesheet_periods p ON p.row_id = r.id
            WHERE r.header_id = %s
            ORDER BY r.fio, r.tbn, p.date_from
            """,
            (int(header_id),),
        )

        rows_map = {}
        for r_id, fio, tbn, hours_raw, d_from, d_to in cur.fetchall():
            if r_id not in rows_map:
                hours = normalize_hours_list(hours_raw, year, month)
                rows_map[r_id] = {
                    "fio": fio or "",
                    "tbn": tbn or "",
                    "hours": hours,
                    "trip_periods": [],
                }
            if d_from and d_to:
                rows_map[r_id]["trip_periods"].append({"from": d_from, "to": d_to})

        return list(rows_map.values())


def load_trip_timesheet_rows_by_header_id(header_id: int) -> List[Dict[str, Any]]:
    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            SELECT year, month
            FROM trip_timesheet_headers
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
            SELECT
                r.id,
                r.fio,
                r.tbn,
                r.hours_raw,
                p.date_from as trip_date_from,
                p.date_to as trip_date_to,
                r.total_days,
                r.total_hours,
                r.night_hours,
                r.overtime_day,
                r.overtime_night
            FROM trip_timesheet_rows r
            LEFT JOIN trip_timesheet_periods p ON p.row_id = r.id
            WHERE r.header_id = %s
            ORDER BY r.fio, r.tbn, p.date_from
            """,
            (int(header_id),),
        )

        rows_map = {}
        for (
            r_id,
            fio,
            tbn,
            hours_raw,
            d_from,
            d_to,
            total_days,
            total_hours,
            night_hours,
            ot_day,
            ot_night,
        ) in cur.fetchall():
            if r_id not in rows_map:
                hours = normalize_hours_list(hours_raw, year, month)
                rows_map[r_id] = {
                    "fio": fio or "",
                    "tbn": tbn or "",
                    "hours": hours,
                    "hours_raw": hours[:],
                    "trip_periods": [],
                    "total_days": int(total_days) if total_days is not None else None,
                    "total_hours": float(total_hours) if total_hours is not None else None,
                    "night_hours": float(night_hours) if night_hours is not None else None,
                    "overtime_day": float(ot_day) if ot_day is not None else None,
                    "overtime_night": float(ot_night) if ot_night is not None else None,
                }
            if d_from and d_to:
                rows_map[r_id]["trip_periods"].append({"from": d_from, "to": d_to})

        return list(rows_map.values())


def load_trip_timesheet_full_by_header_id(header_id: int) -> Optional[Dict[str, Any]]:
    with db_cursor(dict_rows=True) as (_conn, cur):
        header = _load_trip_header_meta_by_id(cur, int(header_id))
        if not header:
            return None

        year = int(header["year"])
        month = int(header["month"])

        cur.execute(
            """
            SELECT
                r.id as row_id,
                r.fio,
                r.tbn,
                r.hours_raw,
                p.date_from as trip_date_from,
                p.date_to as trip_date_to,
                r.total_days,
                r.total_hours,
                r.night_hours,
                r.overtime_day,
                r.overtime_night
            FROM trip_timesheet_rows r
            LEFT JOIN trip_timesheet_periods p ON p.row_id = r.id
            WHERE r.header_id = %s
            ORDER BY r.fio, r.tbn, p.date_from
            """,
            (int(header_id),),
        )

        rows_map = {}
        for r in cur.fetchall():
            if isinstance(r, dict):
                r_id = r.get("row_id")
                fio = r.get("fio") or ""
                tbn = r.get("tbn") or ""
                hours_raw = r.get("hours_raw")
                trip_date_from = r.get("trip_date_from")
                trip_date_to = r.get("trip_date_to")
                total_days = r.get("total_days")
                total_hours = r.get("total_hours")
                night_hours = r.get("night_hours")
                overtime_day = r.get("overtime_day")
                overtime_night = r.get("overtime_night")
            else:
                (
                    r_id,
                    fio,
                    tbn,
                    hours_raw,
                    trip_date_from,
                    trip_date_to,
                    total_days,
                    total_hours,
                    night_hours,
                    overtime_day,
                    overtime_night,
                ) = r

            if r_id not in rows_map:
                hours = normalize_hours_list(hours_raw, year, month)
                rows_map[r_id] = {
                    "fio": fio or "",
                    "tbn": tbn or "",
                    "hours": hours,
                    "hours_raw": hours[:],
                    "trip_periods": [],
                    "total_days": int(total_days) if total_days is not None else None,
                    "total_hours": float(total_hours) if total_hours is not None else None,
                    "night_hours": float(night_hours) if night_hours is not None else None,
                    "overtime_day": float(overtime_day) if overtime_day is not None else None,
                    "overtime_night": float(overtime_night) if overtime_night is not None else None,
                }

            if trip_date_from and trip_date_to:
                rows_map[r_id]["trip_periods"].append({"from": trip_date_from, "to": trip_date_to})

        header["rows"] = list(rows_map.values())
        return header


def find_trip_timesheet_header_id(
    object_id: Optional[str],
    object_addr: str,
    year: int,
    month: int,
) -> Optional[int]:
    with db_cursor() as (_conn, cur):
        return _find_trip_header_id_by_key(
            cur,
            object_id,
            object_addr,
            int(year),
            int(month),
        )

def find_duplicate_employees_for_trip_timesheet(
    object_id: Optional[str],
    object_addr: str,
    year: int,
    month: int,
    employees: Sequence[Tuple[str, str]],
) -> List[Dict[str, Any]]:
    counts: Dict[tuple[str, str], Dict[str, Any]] = {}

    for fio, tbn in employees:
        fio_norm = normalize_spaces(fio or "")
        tbn_norm = normalize_tbn(tbn)

        if not fio_norm and not tbn_norm:
            continue

        key = (fio_norm.lower(), tbn_norm)
        if key not in counts:
            counts[key] = {
                "fio": fio_norm,
                "tbn": tbn_norm,
                "count": 0,
            }
        counts[key]["count"] += 1

    result: List[Dict[str, Any]] = []
    for item in counts.values():
        if item["count"] > 1:
            result.append(
                {
                    "header_id": None,
                    "fio": item["fio"],
                    "tbn": item["tbn"],
                    "count": item["count"],
                }
            )

    return result

__all__ = [
    "upsert_trip_timesheet_header",
    "replace_trip_timesheet_rows",
    "load_trip_timesheet_rows_from_db",
    "load_trip_timesheet_rows_by_header_id",
    "load_trip_timesheet_full_by_header_id",
    "find_trip_timesheet_header_id",
    "find_duplicate_employees_for_trip_timesheet",
]
