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

class TripTimesheetConflictError(RuntimeError):
    """Табель был изменён другим пользователем после его открытия."""

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
    user_id: int,
) -> int:
    object_id_norm = _norm_header_object_id(object_id)
    object_addr_norm = _norm_header_address(object_addr)

    if not object_addr_norm:
        raise RuntimeError("Не задан адрес объекта для сохранения командировочного табеля.")

    if user_id is None:
        raise RuntimeError(
            "Не удалось сохранить командировочный табель: не передан user_id."
        )

    try:
        user_id_int = int(user_id)
    except Exception:
        raise RuntimeError(
            f"Не удалось сохранить командировочный табель: некорректный user_id={user_id!r}."
        )

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
                    user_id = COALESCE(user_id, %s),
                    updated_at = now()
                WHERE id = %s
                """,
                (
                    object_id_norm,
                    object_addr_norm,
                    int(year),
                    int(month),
                    int(object_db_id),
                    user_id_int,
                    int(existing_id),
                ),
            )
            return int(existing_id)

        cur.execute(
            """
            INSERT INTO trip_timesheet_headers
                (
                    object_id,
                    object_addr,
                    year,
                    month,
                    user_id,
                    object_db_id
                )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                object_id_norm,
                object_addr_norm,
                int(year),
                int(month),
                user_id_int,
                int(object_db_id),
            ),
        )

        row = cur.fetchone()
        if not row:
            raise RuntimeError("Не удалось создать заголовок командировочного табеля.")

        return int(row[0])

def _prepare_trip_timesheet_values(
    header_id: int,
    rows: Sequence[Mapping[str, Any]],
    year: int,
    month: int,
) -> Tuple[List[tuple[Any, ...]], List[Mapping[str, Any]]]:
    values: List[tuple[Any, ...]] = []
    original_records: List[Mapping[str, Any]] = []

    for rec in rows:
        fio = normalize_spaces(str(rec.get("fio") or ""))
        tbn = normalize_tbn(rec.get("tbn"))

        if not fio and not tbn:
            continue

        position = normalize_spaces(str(rec.get("position") or ""))
        department = normalize_spaces(str(rec.get("department") or ""))

        hours_list = normalize_hours_list(
            rec.get("hours"),
            year,
            month,
        )

        # Итоги обязательно пересчитываются перед записью.
        totals = calc_row_totals(
            hours_list,
            year,
            month,
        )

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
                position or None,
                department or None,
                hours_list,
                total_days,
                total_hours,
                total_night,
                total_ot_day,
                total_ot_night,
            )
        )

        original_records.append(rec)

    return values, original_records

def replace_trip_timesheet_rows(
    header_id: int,
    rows: Sequence[Mapping[str, Any]],
    year: int,
    month: int,
    *,
    allow_empty: bool = False,
) -> None:
    values, original_records = _prepare_trip_timesheet_values(
        header_id=header_id,
        rows=rows,
        year=year,
        month=month,
    )

    if not values and not allow_empty:
        raise RuntimeError(
            "Сохранение отменено: табель не содержит ни одной "
            "заполненной строки. Массовое удаление заблокировано."
        )

    with db_cursor() as (_conn, cur):
        cur.execute(
            """
            DELETE FROM trip_timesheet_rows
            WHERE header_id = %s
            """,
            (int(header_id),),
        )

        if not values:
            return

        returned_ids = execute_values(
            cur,
            """
            INSERT INTO trip_timesheet_rows
                (
                    header_id,
                    fio,
                    tbn,
                    position,
                    department,
                    hours_raw,
                    total_days,
                    total_hours,
                    night_hours,
                    overtime_day,
                    overtime_night
                )
            VALUES %s
            RETURNING id
            """,
            values,
            fetch=True,
        )

        if len(returned_ids) != len(original_records):
            raise RuntimeError(
                "Количество созданных строк не совпадает "
                "с количеством сохраняемых сотрудников."
            )

        period_values: List[tuple[Any, Any, Any]] = []

        for row_id_tuple, rec in zip(
            returned_ids,
            original_records,
        ):
            row_id = int(row_id_tuple[0])

            for period in rec.get("trip_periods") or []:
                date_from = period.get("from")
                date_to = period.get("to")

                if not date_from or not date_to:
                    continue

                if date_to < date_from:
                    raise RuntimeError(
                        f"Некорректный период командировки: "
                        f"{date_from} — {date_to}."
                    )

                period_values.append(
                    (
                        row_id,
                        date_from,
                        date_to,
                    )
                )

        if period_values:
            execute_values(
                cur,
                """
                INSERT INTO trip_timesheet_periods
                    (
                        row_id,
                        date_from,
                        date_to
                    )
                VALUES %s
                """,
                period_values,
            )

def save_trip_timesheet_atomic(
    *,
    object_id: Optional[str],
    object_addr: str,
    year: int,
    month: int,
    user_id: int,
    rows: Sequence[Mapping[str, Any]],
    expected_header_id: Optional[int],
    expected_revision: Optional[int],
) -> Tuple[int, int]:
    """
    Атомарно сохраняет заголовок, строки и периоды табеля.

    Защищает от перезаписи данных, если другой пользователь
    сохранил тот же табель после его открытия.
    """
    object_id_norm = _norm_header_object_id(object_id)
    object_addr_norm = _norm_header_address(object_addr)

    if not object_addr_norm:
        raise RuntimeError(
            "Не задан адрес объекта для сохранения командировочного табеля."
        )

    if user_id is None:
        raise RuntimeError(
            "Не удалось определить пользователя для сохранения табеля."
        )

    try:
        user_id_int = int(user_id)
    except Exception as exc:
        raise RuntimeError(
            f"Некорректный user_id={user_id!r}."
        ) from exc

    if not rows:
        raise RuntimeError(
            "Сохранение пустого табеля заблокировано. "
            "В табеле нет сотрудников."
        )

    with db_cursor() as (_conn, cur):
        object_db_id = find_object_db_id_by_excel_or_address(
            cur,
            object_id_norm or None,
            object_addr_norm,
        )

        if object_db_id is None:
            raise RuntimeError(
                f"В БД не найден объект "
                f"(excel_id={object_id_norm!r}, "
                f"address={object_addr_norm!r})."
            )

        object_db_id = int(object_db_id)

        # Не позволяет двум экземплярам программы одновременно
        # сохранять один объект и один период.
        lock_key = (
            f"trip_timesheet:"
            f"{object_db_id}:"
            f"{int(year)}:"
            f"{int(month)}"
        )

        cur.execute(
            """
            SELECT pg_advisory_xact_lock(hashtext(%s))
            """,
            (lock_key,),
        )

        cur.execute(
            """
            SELECT
                h.id,
                COALESCE(h.revision, 0)
            FROM trip_timesheet_headers h
            WHERE h.object_db_id = %s
              AND h.year = %s
              AND h.month = %s
            ORDER BY h.updated_at DESC NULLS LAST, h.id DESC
            LIMIT 1
            FOR UPDATE
            """,
            (
                object_db_id,
                int(year),
                int(month),
            ),
        )

        existing = cur.fetchone()

        if existing:
            header_id = int(existing[0])
            current_revision = int(existing[1] or 0)

            if expected_header_id is None:
                raise TripTimesheetConflictError(
                    "Пока табель был открыт, другой пользователь "
                    "создал и сохранил его. Откройте табель заново."
                )

            if int(expected_header_id) != header_id:
                raise TripTimesheetConflictError(
                    "Открытая версия табеля больше не является актуальной. "
                    "Откройте табель заново."
                )

            if expected_revision is None:
                raise TripTimesheetConflictError(
                    "Не удалось проверить версию открытого табеля. "
                    "Откройте табель заново."
                )

            if int(expected_revision) != current_revision:
                raise TripTimesheetConflictError(
                    "Табель уже был изменён другим пользователем. "
                    "Ваши данные не были записаны. "
                    "Откройте табель заново и повторите изменения."
                )
        else:
            if expected_header_id is not None:
                raise TripTimesheetConflictError(
                    "Заголовок открытого табеля был удалён или изменён. "
                    "Откройте табель заново."
                )

            cur.execute(
                """
                INSERT INTO trip_timesheet_headers
                    (
                        object_id,
                        object_addr,
                        year,
                        month,
                        user_id,
                        object_db_id,
                        revision,
                        created_at,
                        updated_at
                    )
                VALUES
                    (%s, %s, %s, %s, %s, %s, 0, now(), now())
                RETURNING id
                """,
                (
                    object_id_norm,
                    object_addr_norm,
                    int(year),
                    int(month),
                    user_id_int,
                    object_db_id,
                ),
            )

            created = cur.fetchone()

            if not created:
                raise RuntimeError(
                    "Не удалось создать заголовок командировочного табеля."
                )

            header_id = int(created[0])
            current_revision = 0

        values, original_records = _prepare_trip_timesheet_values(
            header_id=header_id,
            rows=rows,
            year=year,
            month=month,
        )

        if not values:
            raise RuntimeError(
                "Сохранение отменено: после проверки не осталось "
                "ни одной заполненной строки."
            )

        cur.execute(
            """
            DELETE FROM trip_timesheet_rows
            WHERE header_id = %s
            """,
            (header_id,),
        )

        returned_ids = execute_values(
            cur,
            """
            INSERT INTO trip_timesheet_rows
                (
                    header_id,
                    fio,
                    tbn,
                    position,
                    department,
                    hours_raw,
                    total_days,
                    total_hours,
                    night_hours,
                    overtime_day,
                    overtime_night
                )
            VALUES %s
            RETURNING id
            """,
            values,
            fetch=True,
        )

        if len(returned_ids) != len(original_records):
            raise RuntimeError(
                "Не удалось сохранить все строки табеля. "
                "Операция полностью отменена."
            )

        period_values: List[tuple[Any, Any, Any]] = []

        for row_id_tuple, rec in zip(
            returned_ids,
            original_records,
        ):
            row_id = int(row_id_tuple[0])

            for period in rec.get("trip_periods") or []:
                date_from = period.get("from")
                date_to = period.get("to")

                if not date_from or not date_to:
                    continue

                if date_to < date_from:
                    raise RuntimeError(
                        f"Дата окончания командировки {date_to} "
                        f"раньше даты начала {date_from}."
                    )

                period_values.append(
                    (
                        row_id,
                        date_from,
                        date_to,
                    )
                )

        if period_values:
            execute_values(
                cur,
                """
                INSERT INTO trip_timesheet_periods
                    (
                        row_id,
                        date_from,
                        date_to
                    )
                VALUES %s
                """,
                period_values,
            )

        new_revision = current_revision + 1

        cur.execute(
            """
            UPDATE trip_timesheet_headers
            SET
                object_id = %s,
                object_addr = %s,
                object_db_id = %s,
                user_id = %s,
                revision = %s,
                updated_at = now()
            WHERE id = %s
            """,
            (
                object_id_norm,
                object_addr_norm,
                object_db_id,
                user_id_int,
                new_revision,
                header_id,
            ),
        )

        if cur.rowcount != 1:
            raise RuntimeError(
                "Не удалось обновить версию командировочного табеля."
            )

        return header_id, new_revision

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
                r.id,
                r.fio,
                r.tbn,
                r.position,
                r.department,
                r.hours_raw,
                p.date_from,
                p.date_to
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
            position,
            department,
            hours_raw,
            d_from,
            d_to,
        ) in cur.fetchall():
            if r_id not in rows_map:
                hours = normalize_hours_list(hours_raw, year, month)
                rows_map[r_id] = {
                    "fio": fio or "",
                    "tbn": tbn or "",
                    "position": position or "",
                    "department": department or "",
                    "hours": hours,
                    "trip_periods": [],
                }
            if d_from and d_to:
                rows_map[r_id]["trip_periods"].append({"from": d_from, "to": d_to})

        return list(rows_map.values())

def load_trip_timesheet_with_revision(
    object_id: Optional[str],
    object_addr: str,
    year: int,
    month: int,
) -> Tuple[List[Dict[str, Any]], Optional[int], Optional[int]]:
    """
    Возвращает:
        rows, header_id, revision
    """
    object_id_norm = _norm_header_object_id(object_id)
    object_addr_norm = _norm_header_address(object_addr)

    with db_cursor() as (_conn, cur):
        object_db_id = None

        try:
            object_db_id = find_object_db_id_by_excel_or_address(
                cur,
                object_id_norm or None,
                object_addr_norm,
            )
        except Exception:
            object_db_id = None

        if object_db_id is not None:
            cur.execute(
                """
                SELECT
                    h.id,
                    COALESCE(h.revision, 0)
                FROM trip_timesheet_headers h
                WHERE h.object_db_id = %s
                  AND h.year = %s
                  AND h.month = %s
                ORDER BY h.updated_at DESC NULLS LAST, h.id DESC
                LIMIT 1
                """,
                (
                    int(object_db_id),
                    int(year),
                    int(month),
                ),
            )
        else:
            header_id = _find_trip_header_id_by_key(
                cur,
                object_id_norm or None,
                object_addr_norm,
                int(year),
                int(month),
            )

            if header_id is None:
                return [], None, None

            cur.execute(
                """
                SELECT
                    h.id,
                    COALESCE(h.revision, 0)
                FROM trip_timesheet_headers h
                WHERE h.id = %s
                """,
                (int(header_id),),
            )

        header_row = cur.fetchone()

        if not header_row:
            return [], None, None

        header_id = int(header_row[0])
        revision = int(header_row[1] or 0)

        rows = _load_trip_rows_by_header_id_cur(
            cur,
            header_id=header_id,
            year=int(year),
            month=int(month),
        )

        return rows, header_id, revision

def _load_trip_rows_by_header_id_cur(
    cur,
    header_id: int,
    year: int,
    month: int,
) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            r.id,
            r.fio,
            r.tbn,
            r.position,
            r.department,
            r.hours_raw,
            p.date_from,
            p.date_to
        FROM trip_timesheet_rows r
        LEFT JOIN trip_timesheet_periods p ON p.row_id = r.id
        WHERE r.header_id = %s
        ORDER BY r.fio, r.tbn, p.date_from
        """,
        (int(header_id),),
    )

    rows_map: Dict[int, Dict[str, Any]] = {}

    for (
        r_id,
        fio,
        tbn,
        position,
        department,
        hours_raw,
        d_from,
        d_to,
    ) in cur.fetchall():
        if r_id not in rows_map:
            hours = normalize_hours_list(hours_raw, year, month)
            rows_map[r_id] = {
                "fio": fio or "",
                "tbn": tbn or "",
                "position": position or "",
                "department": department or "",
                "hours": hours,
                "trip_periods": [],
            }

        if d_from and d_to:
            rows_map[r_id]["trip_periods"].append(
                {
                    "from": d_from,
                    "to": d_to,
                }
            )

    return list(rows_map.values())

def load_trip_timesheet_rows_for_copy(
    object_id: Optional[str],
    object_addr: str,
    year: int,
    month: int,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    Более надёжная загрузка строк табеля для копирования сотрудников.

    Ищет табель-источник несколькими способами:
    1. по object_db_id;
    2. по object_id;
    3. по точному адресу;
    4. по нормализованному адресу.

    Возвращает:
        rows, debug_info
    """
    object_id_norm = _norm_header_object_id(object_id)
    object_addr_norm = _norm_header_address(object_addr)

    with db_cursor() as (_conn, cur):
        object_db_id = None

        try:
            object_db_id = find_object_db_id_by_excel_or_address(
                cur,
                object_id_norm or None,
                object_addr_norm,
            )
        except Exception as exc:
            logger.warning("Не удалось определить object_db_id для копирования: %s", exc)

        attempts: List[Tuple[str, tuple[Any, ...], str]] = []

        if object_db_id is not None:
            attempts.append(
                (
                    """
                    SELECT h.id
                    FROM trip_timesheet_headers h
                    WHERE h.object_db_id = %s
                      AND h.year = %s
                      AND h.month = %s
                    ORDER BY h.updated_at DESC NULLS LAST, h.id DESC
                    LIMIT 1
                    """,
                    (int(object_db_id), int(year), int(month)),
                    f"по object_db_id={object_db_id}",
                )
            )

        if object_id_norm:
            attempts.append(
                (
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
                    f"по object_id={object_id_norm}",
                )
            )

        if object_addr_norm:
            attempts.append(
                (
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
                    "по точному адресу",
                )
            )

            attempts.append(
                (
                    """
                    SELECT h.id
                    FROM trip_timesheet_headers h
                    WHERE LOWER(TRIM(REGEXP_REPLACE(COALESCE(h.object_addr, ''), '\\s+', ' ', 'g')))
                          = LOWER(%s)
                      AND h.year = %s
                      AND h.month = %s
                    ORDER BY h.updated_at DESC NULLS LAST, h.id DESC
                    LIMIT 1
                    """,
                    (object_addr_norm, int(year), int(month)),
                    "по нормализованному адресу",
                )
            )

        checked_labels: List[str] = []

        for sql, params, label in attempts:
            checked_labels.append(label)

            try:
                cur.execute(sql, params)
                row = cur.fetchone()
            except Exception as exc:
                logger.warning("Ошибка поиска табеля для копирования %s: %s", label, exc)
                continue

            if not row:
                continue

            header_id = int(row[0])
            rows = _load_trip_rows_by_header_id_cur(
                cur,
                header_id=header_id,
                year=year,
                month=month,
            )

            return rows, f"Найдено {label}, header_id={header_id}, строк={len(rows)}"

        # Диагностика: покажем, какие табели вообще есть за этот месяц
        cur.execute(
            """
            SELECT
                h.id,
                COALESCE(h.object_id, '') AS object_id,
                COALESCE(h.object_addr, '') AS object_addr,
                h.object_db_id
            FROM trip_timesheet_headers h
            WHERE h.year = %s
              AND h.month = %s
            ORDER BY h.updated_at DESC NULLS LAST, h.id DESC
            LIMIT 20
            """,
            (int(year), int(month)),
        )

        candidates = cur.fetchall()

        debug_lines = [
            f"Табель не найден за {int(month):02d}.{int(year)}.",
            "",
            "Проверялись варианты:",
        ]

        if checked_labels:
            for label in checked_labels:
                debug_lines.append(f"• {label}")
        else:
            debug_lines.append("• нет вариантов поиска")

        debug_lines.append("")
        debug_lines.append(f"Текущий object_id: {object_id_norm or '-'}")
        debug_lines.append(f"Текущий object_addr: {object_addr_norm or '-'}")
        debug_lines.append(f"Текущий object_db_id: {object_db_id or '-'}")
        debug_lines.append("")

        if candidates:
            debug_lines.append(f"Другие табели за {int(month):02d}.{int(year)} в trip_timesheet_headers:")
            for h_id, h_object_id, h_object_addr, h_object_db_id in candidates:
                debug_lines.append(
                    f"• header_id={h_id}, object_id={h_object_id or '-'}, "
                    f"object_db_id={h_object_db_id or '-'}, адрес={h_object_addr or '-'}"
                )
        else:
            debug_lines.append(
                f"В таблице trip_timesheet_headers вообще нет табелей за {int(month):02d}.{int(year)}."
            )

        return [], "\n".join(debug_lines)

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
                r.position,
                r.department,
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
            position,
            department,
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
                    "position": position or "",
                    "department": department or "",
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
                r.position,
                r.department,
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
                position = r.get("position") or ""
                department = r.get("department") or ""
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
                    position,
                    department,
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
                    "position": position or "",
                    "department": department or "",
                    "hours": hours,
                    "hours_raw": hours[:],
                    "trip_periods": [],
                    "total_days": int(total_days)
                        if total_days is not None else None,
                    "total_hours": float(total_hours)
                        if total_hours is not None else None,
                    "night_hours": float(night_hours)
                        if night_hours is not None else None,
                    "overtime_day": float(overtime_day)
                        if overtime_day is not None else None,
                    "overtime_night": float(overtime_night)
                        if overtime_night is not None else None,
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
    "TripTimesheetConflictError",
    "upsert_trip_timesheet_header",
    "replace_trip_timesheet_rows",
    "save_trip_timesheet_atomic",
    "load_trip_timesheet_rows_from_db",
    "load_trip_timesheet_with_revision",
    "load_trip_timesheet_rows_for_copy",
    "load_trip_timesheet_rows_by_header_id",
    "load_trip_timesheet_full_by_header_id",
    "find_trip_timesheet_header_id",
    "find_duplicate_employees_for_trip_timesheet",
]
