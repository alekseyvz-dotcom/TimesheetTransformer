import calendar
import re
import sys
import logging
import difflib
from datetime import datetime, date
from pathlib import Path
from typing import List, Tuple, Optional, Any, Dict

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
from virtual_timesheet_grid import VirtualTimesheetGrid

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

def exe_dir() -> Path:
    if getattr(sys, "frozen", False): return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

# ------------------------- Логика работы с пулом соединений -------------------------
db_connection_pool = None
USING_SHARED_POOL = False

def set_db_pool(pool):
    """Функция для установки пула соединений извне."""
    global db_connection_pool, USING_SHARED_POOL
    db_connection_pool = pool
    USING_SHARED_POOL = True

def get_db_connection():
    """Получает соединение из пула."""
    if db_connection_pool:
        return db_connection_pool.getconn()
    raise RuntimeError("Пул соединений не был установлен из главного приложения.")

def release_db_connection(conn):
    """Возвращает соединение обратно в пул."""
    if db_connection_pool:
        db_connection_pool.putconn(conn)

# ------------------------- Загрузка зависимостей (если нужны для standalone) -------------------------
try:
    import settings_manager as Settings
    from settings_manager import (
        get_output_dir_from_config,
        get_selected_department_from_config,
        set_selected_department_in_config,
    )
except Exception:
    Settings = None
    get_output_dir_from_config = None
    get_selected_department_from_config = None
    set_selected_department_in_config = None

# ------------------------- Функции для работы с БД (перенесены из main_app.py) -------------------------

def find_duplicate_employees_for_timesheet(
    object_id: Optional[str],
    object_addr: str,
    department: str,
    year: int,
    month: int,
    user_id: int,
    employees: List[Tuple[str, str]],
) -> List[Dict[str, Any]]:
    """
    Ищет сотрудников, которые уже есть в табелях других пользователей
    по тому же объекту/подразделению/периоду.

    employees: список (fio, tbn) из текущего табеля.
    Возвращает список словарей с информацией о найденных дублях.
    """
    if not employees:
        return []

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # подготовим набор ФИО/таб№ для поиска
            # используем только тех, у кого есть либо fio, либо tbn
            fio_tbn_pairs = [(fio.strip(), (tbn or "").strip())
                             for fio, tbn in employees
                             if fio.strip() or (tbn or "").strip()]

            if not fio_tbn_pairs:
                return []

            # будем искать по (fio,tbn). Если tbn пустой, ищем только по fio.
            # Для простоты разобьём на две группы.
            with_tbn = [(fio, tbn) for fio, tbn in fio_tbn_pairs if tbn]
            without_tbn = [fio for fio, tbn in fio_tbn_pairs if not tbn]

            results: List[Dict[str, Any]] = []

            # Общие условия по объекту/подразделению/периоду и не наш user_id
            base_where = """
                COALESCE(h.object_id, '') = COALESCE(%s, '')
                AND h.object_addr = %s
                AND COALESCE(h.department, '') = COALESCE(%s, '')
                AND h.year = %s
                AND h.month = %s
                AND h.user_id <> %s
            """
            base_params = [object_id or None, object_addr, department or None, year, month, user_id]

            # 1) Ищем совпадения по (fio, tbn)
            if with_tbn:
                cur.execute(
                    f"""
                    SELECT h.id AS header_id,
                           h.user_id,
                           u.username,
                           u.full_name,
                           r.fio,
                           r.tbn
                    FROM timesheet_headers h
                    JOIN app_users u      ON u.id = h.user_id
                    JOIN timesheet_rows r ON r.header_id = h.id
                    WHERE {base_where}
                      AND (r.fio, COALESCE(r.tbn, '')) IN %s
                    """,
                    base_params + [tuple(with_tbn)],
                )
                results.extend(cur.fetchall())

            # 2) Ищем совпадения по одному ФИО (где в нашем списке tbn пустой)
            if without_tbn:
                cur.execute(
                    f"""
                    SELECT h.id AS header_id,
                           h.user_id,
                           u.username,
                           u.full_name,
                           r.fio,
                           r.tbn
                    FROM timesheet_headers h
                    JOIN app_users u      ON u.id = h.user_id
                    JOIN timesheet_rows r ON r.header_id = h.id
                    WHERE {base_where}
                      AND r.fio = ANY(%s)
                    """,
                    base_params + [without_tbn],
                )
                results.extend(cur.fetchall())

            return results

    finally:
        if conn:
            release_db_connection(conn)

def find_object_db_id_by_excel_or_address(
    cur,  # теперь курсор передается явно
    excel_id: Optional[str],
    address: str,
) -> Optional[int]:
    """
    Ищет объект в таблице objects.
    Возвращает id объекта или None.
    """
    if excel_id:
        cur.execute("SELECT id FROM objects WHERE COALESCE(NULLIF(excel_id, ''), '') = %s", (excel_id,))
        row = cur.fetchone()
        if row:
            return row[0]
    cur.execute("SELECT id FROM objects WHERE address = %s", (address,))
    row = cur.fetchone()
    return row[0] if row else None


def upsert_timesheet_header(
    object_id: str,
    object_addr: str,
    department: str,
    year: int,
    month: int,
    user_id: int,
) -> int:
    """Находит или создаёт заголовок табеля и возвращает его id."""
    conn = None
    try:
        conn = get_db_connection()
        with conn, conn.cursor() as cur:
            object_db_id = find_object_db_id_by_excel_or_address(cur, object_id or None, object_addr)
            if object_db_id is None:
                raise RuntimeError(
                    f"В БД не найден объект (excel_id={object_id!r}, address={object_addr!r}).\n"
                    f"Сначала создайте объект в разделе «Объекты»."
                )
            
            cur.execute(
                """
                INSERT INTO timesheet_headers (object_id, object_addr, department, year, month, user_id, object_db_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (object_id, object_addr, department, year, month, user_id)
                DO UPDATE SET updated_at = now(), object_db_id = EXCLUDED.object_db_id
                RETURNING id;
                """,
                (object_id or None, object_addr, department or None, year, month, user_id, object_db_id),
            )
            return cur.fetchone()[0]
    finally:
        if conn:
            release_db_connection(conn)

def replace_timesheet_rows(header_id: int, rows: List[Dict[str, Any]]):
    """Полностью заменяет строки табеля одним запросом (Batch Insert)."""
    conn = None
    try:
        conn = get_db_connection()
        with conn, conn.cursor() as cur:
            # Сначала удаляем старые (тут всё ок)
            cur.execute("DELETE FROM timesheet_rows WHERE header_id = %s", (header_id,))
            
            if not rows:
                return

            # Подготовка данных для массовой вставки
            values = []
            for rec in rows:
                hours_list = rec.get("hours") or [None] * 31
                if len(hours_list) != 31:
                    hours_list = (hours_list + [None] * 31)[:31]

                total_hours = 0.0
                total_night_hours = 0.0
                total_days = 0
                total_ot_day = 0.0
                total_ot_night = 0.0

                for raw in hours_list:
                    if not raw:
                        continue
                    # Новая логика: обычные часы + ночные
                    hrs, night = parse_hours_and_night(raw)
                    d_ot, n_ot = parse_overtime(raw)

                    if isinstance(hrs, (int, float)) and hrs > 1e-12:
                        total_hours += float(hrs)
                        total_days += 1
                    if isinstance(night, (int, float)):
                        total_night_hours += float(night)
                    if isinstance(d_ot, (int, float)):
                        total_ot_day += float(d_ot)
                    if isinstance(n_ot, (int, float)):
                        total_ot_night += float(n_ot)

                values.append((
                    header_id,
                    rec["fio"],
                    rec.get("tbn") or None,
                    hours_list,
                    total_days or None,
                    total_hours or None,
                    total_night_hours or None,
                    total_ot_day or None,
                    total_ot_night or None,
                ))

            insert_query = """
                INSERT INTO timesheet_rows 
                (header_id, fio, tbn, hours_raw,
                 total_days, total_hours, night_hours, overtime_day, overtime_night)
                VALUES %s
            """
            execute_values(cur, insert_query, values)
            
    finally:
        if conn:
            release_db_connection(conn)

def load_timesheet_rows_from_db(object_id: str, object_addr: str, department: str, year: int, month: int, user_id: int) -> List[Dict[str, Any]]:
    """Загружает строки табеля для конкретного пользователя и контекста."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT h.id FROM timesheet_headers h
                WHERE COALESCE(h.object_id, '') = COALESCE(%s, '') AND h.object_addr = %s
                AND COALESCE(h.department, '') = COALESCE(%s, '') AND h.year = %s AND h.month = %s AND h.user_id = %s
                """,
                (object_id or None, object_addr, department or None, year, month, user_id),
            )
            row = cur.fetchone()
            if not row: return []
            header_id = row[0]

            cur.execute("SELECT fio, tbn, hours_raw FROM timesheet_rows WHERE header_id = %s ORDER BY fio, tbn", (header_id,))
            result = []
            for fio, tbn, hours_raw in cur.fetchall():
                hrs = list(hours_raw) if hours_raw is not None else [None] * 31
                result.append({"fio": fio or "", "tbn": tbn or "", "hours": [h for h in hrs]})
            return result
    finally:
        if conn:
            release_db_connection(conn)

def load_timesheet_rows_for_copy_from_db(
    object_id: Optional[str],
    object_addr: str,
    department: str,
    year: int,
    month: int,
    user_id: int,
    with_hours: bool,
) -> List[Dict[str, Any]]:
    """
    Возвращает строки табеля-источника из БД для копирования.
    Если with_hours=False — часы очищаются.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT h.id
                FROM timesheet_headers h
                WHERE COALESCE(h.object_id, '') = COALESCE(%s, '')
                  AND h.object_addr = %s
                  AND COALESCE(h.department, '') = COALESCE(%s, '')
                  AND h.year = %s
                  AND h.month = %s
                  AND h.user_id = %s
                """,
                (object_id or None, object_addr, department or None, year, month, user_id),
            )
            row = cur.fetchone()
            if not row:
                return []
            header_id = row[0]

            cur.execute(
                """
                SELECT fio, tbn, hours_raw
                FROM timesheet_rows
                WHERE header_id = %s
                ORDER BY fio, tbn
                """,
                (header_id,),
            )

            result: List[Dict[str, Any]] = []
            for fio, tbn, hours_raw in cur.fetchall():
                if with_hours:
                    hrs = list(hours_raw) if hours_raw is not None else [None] * 31
                    hrs = (hrs + [None] * 31)[:31]
                else:
                    hrs = [None] * 31

                result.append({
                    "fio": fio or "",
                    "tbn": tbn or "",
                    "hours": hrs,
                })
            return result
    finally:
        if conn:
            release_db_connection(conn)

def load_timesheet_rows_by_header_id(header_id: int) -> List[Dict[str, Any]]:
    """Загружает строки табеля по ID заголовка."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT fio, tbn, hours_raw, total_days, total_hours, night_hours, overtime_day, overtime_night "
                "FROM timesheet_rows WHERE header_id = %s ORDER BY fio, tbn", (header_id,),
            )
            result = []
            for fio, tbn, hours_raw, total_days, total_hours, night_hours, ot_day, ot_night in cur.fetchall():
                hrs = list(hours_raw) if hours_raw else [None] * 31
                result.append({
                    "fio": fio or "",
                    "tbn": tbn or "",
                    "hours_raw": [h for h in hrs],
                    "total_days": total_days,
                    "total_hours": float(total_hours) if total_hours is not None else None,
                    "night_hours": float(night_hours) if night_hours is not None else None,
                    "overtime_day": float(ot_day) if ot_day is not None else None,
                    "overtime_night": float(ot_night) if ot_night is not None else None,
                })
            return result
    finally:
        if conn:
            release_db_connection(conn)

def load_user_timesheet_headers(user_id: int, year: Optional[int], month: Optional[int],
                                department: Optional[str], object_addr_substr: Optional[str]) -> List[Dict[str, Any]]:
    """Возвращает список заголовков табелей, созданных пользователем, с фильтрами."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # ### ИЗМЕНЕНО: Добавляем блок WHERE с фильтрами ###
            where, params = ["user_id = %s"], [user_id]
            if year is not None: where.append("year = %s"); params.append(year)
            if month is not None: where.append("month = %s"); params.append(month)
            if department: where.append("COALESCE(department, '') = %s"); params.append(department)
            if object_addr_substr: where.append("object_addr ILIKE %s"); params.append(f"%{object_addr_substr}%")

            where_sql = " AND ".join(where)

            cur.execute(
                # ### ИЗМЕНЕНО: Используем where_sql в запросе ###
                f"""SELECT id, object_id, object_addr, department, year, month, created_at, updated_at 
                   FROM timesheet_headers 
                   WHERE {where_sql} 
                   ORDER BY year DESC, month DESC, object_addr, COALESCE(department, '')
                """,
                params,
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        if conn:
            release_db_connection(conn)

def load_all_timesheet_headers(year: Optional[int], month: Optional[int], department: Optional[str],
                               object_addr_substr: Optional[str], object_id_substr: Optional[str]) -> List[Dict[str, Any]]:
    """Возвращает список заголовков табелей всех пользователей с фильтрами."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            where, params = ["1=1"], []
            if year is not None: where.append("h.year = %s"); params.append(year)
            if month is not None: where.append("h.month = %s"); params.append(month)
            if department: where.append("COALESCE(h.department, '') = %s"); params.append(department)
            if object_addr_substr: where.append("h.object_addr ILIKE %s"); params.append(f"%{object_addr_substr}%")
            if object_id_substr: where.append("COALESCE(h.object_id, '') ILIKE %s"); params.append(f"%{object_id_substr}%")
            
            where_sql = " AND ".join(where)
            cur.execute(
                f"""
                SELECT h.id, h.object_id, h.object_addr, h.department, h.year, h.month, h.user_id,
                       u.username, u.full_name, h.created_at, h.updated_at
                FROM timesheet_headers h JOIN app_users u ON u.id = h.user_id
                WHERE {where_sql}
                ORDER BY h.year DESC, h.month DESC, h.object_addr, COALESCE(h.department, ''), u.full_name
                """, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        if conn: release_db_connection(conn)

def load_employees_from_db() -> List[Tuple[str, str, str, str]]:
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT e.fio, e.tbn, e.position, d.name AS dep FROM employees e "
                "LEFT JOIN departments d ON d.id = e.department_id "
                "WHERE COALESCE(e.is_fired, FALSE) = FALSE ORDER BY e.fio"
            )
            return [(r[0] or "", r[1] or "", r[2] or "", r[3] or "") for r in cur.fetchall()]
    finally:
        if conn: release_db_connection(conn)

def load_objects_from_db() -> List[Tuple[str, str]]:
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(NULLIF(excel_id, ''), '') AS code, address FROM objects ORDER BY address")
            return [(r[0] or "", r[1] or "") for r in cur.fetchall()]
    finally:
        if conn: release_db_connection(conn)

def load_objects_short_for_timesheet() -> List[Tuple[str, str, str]]:
    """
    Возвращает список объектов (excel_id, address, short_name).
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(NULLIF(excel_id, ''), '') AS code,
                    address,
                    COALESCE(short_name, '') AS short_name
                FROM objects
                ORDER BY address, code
            """)
            return [(r[0] or "", r[1] or "", r[2] or "") for r in cur.fetchall()]
    finally:
        if conn:
            release_db_connection(conn)

def load_brigadier_assignments_for_department(department_name: str) -> dict[str, str | None]:
    """
    Возвращает назначения из employee_brigadiers для подразделения:
      {employee_tbn: brigadier_tbn_or_None}
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT employee_tbn, brigadier_tbn
                FROM public.employee_brigadiers
                WHERE department = %s
                """,
                (department_name,),
            )
            return {emp_tbn: br_tbn for (emp_tbn, br_tbn) in cur.fetchall()}
    finally:
        release_db_connection(conn)


def load_brigadier_names_for_department(department_name: str) -> dict[str, str]:
    """
    Справочник таб.№ -> ФИО только для тех TBN, которые встречаются как brigadier_tbn в данном подразделении.
    Возвращает: {brigadier_tbn: fio}
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT eb.brigadier_tbn
                FROM public.employee_brigadiers eb
                WHERE eb.department = %s
                  AND eb.brigadier_tbn IS NOT NULL
                  AND eb.brigadier_tbn <> ''
                """,
                (department_name,),
            )
            brig_tbn_list = [r[0] for r in cur.fetchall()]

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
            return {tbn: fio for (tbn, fio) in cur.fetchall()}
    finally:
        release_db_connection(conn)

# ------------------------- Утилиты (перенесены из main_app.py) -------------------------

def month_days(year: int, month: int) -> int: return calendar.monthrange(year, month)[1]

def month_name_ru(month: int) -> str:
    return ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
            "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"][month - 1]

def parse_hours_value(v: Any) -> Optional[float]:
    s = str(v or "").strip()
    if not s: return None
    if "(" in s: s = s.split("(")[0].strip()
    if "/" in s:
        total = 0.0
        for part in s.split("/"):
            n = parse_hours_value(part)
            if isinstance(n, (int, float)): total += float(n)
        return total if total > 0 else None
    if ":" in s:
        p = s.split(":")
        try:
            hh = float(p[0].replace(",", "."))
            mm = float((p[1] if len(p) > 1 else "0").replace(",", "."))
            return hh + mm / 60.0
        except: pass
    try: return float(s.replace(",", "."))
    except: return None

def parse_overtime(v: Any) -> Tuple[Optional[float], Optional[float]]:
    s = str(v or "").strip()
    if "(" not in s or ")" not in s: return None, None
    try:
        overtime_str = s[s.index("(") + 1:s.index(")")].strip()
        if "/" in overtime_str:
            parts = overtime_str.split("/")
            day_ot = float(parts[0].replace(",", ".")) if parts[0].strip() else 0.0
            night_ot = float(parts[1].replace(",", ".")) if len(parts) > 1 and parts[1].strip() else 0.0
            return day_ot, night_ot
        return float(overtime_str.replace(",", ".")), 0.0
    except: return None, None

def parse_hours_and_night(v: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    Правила:
      - '8' или '8,25' или '8:30' -> (8, 0) / (8.25, 0) / (8.5, 0)
      - '8/2' (вне скобок) -> (10, 2)  (т.е. 8 всего + 2 ночных; ночные входят в общее)
      - '8/2/1' -> (11, 3) (2+1 ночных)
      - если строка содержит скобки, берём только часть ДО "(".
    """
    s = str(v or "").strip()
    if not s:
        return None, None

    # Отбрасываем переработку в скобках: "8/2(1/1)" -> "8/2"
    if "(" in s:
        s = s.split("(", 1)[0].strip()
    if not s:
        return None, None

    # Если есть /, то первая часть — "обычные", остальные — "ночные"
    if "/" in s:
        parts = [p.strip() for p in s.split("/") if p.strip()]
        if not parts:
            return None, None

        # Перевод строки в число (поддержка "," и ":" как в parse_hours_value)
        def _to_hours(x: str) -> Optional[float]:
            if not x:
                return None
            if ":" in x:
                p = x.split(":")
                try:
                    hh = float(p[0].replace(",", "."))
                    mm = float((p[1] if len(p) > 1 else "0").replace(",", "."))
                    return hh + mm / 60.0
                except:
                    return None
            try:
                return float(x.replace(",", "."))
            except:
                return None

        base = _to_hours(parts[0])
        if base is None:
            return None, None

        night_sum = 0.0
        for p in parts[1:]:
            v = _to_hours(p)
            if isinstance(v, (int, float)):
                night_sum += float(v)

        total = base + night_sum
        return (total if total > 0 else None,
                night_sum if night_sum > 0 else 0.0)

    # Без дроби: используем старую логику parse_hours_value
    total = parse_hours_value(s)
    if total is None:
        return None, None
    return total, 0.0

def calc_row_totals(hours_list: List[Optional[str]], year: int, month: int) -> Dict[str, Any]:

    days_in_m = month_days(year, month)

    total_hours = 0.0
    total_days = 0
    total_ot_day = 0.0
    total_ot_night = 0.0

    if not hours_list:
        hours_list = [None] * 31
    if len(hours_list) < 31:
        hours_list = (hours_list + [None] * 31)[:31]
    else:
        hours_list = hours_list[:31]

    for i in range(days_in_m):
        raw = hours_list[i]
        if not raw:
            continue

        hrs, _night = parse_hours_and_night(raw)
        d_ot, n_ot = parse_overtime(raw)

        if isinstance(hrs, (int, float)) and hrs > 1e-12:
            total_hours += float(hrs)
            total_days += 1
        if isinstance(d_ot, (int, float)):
            total_ot_day += float(d_ot)
        if isinstance(n_ot, (int, float)):
            total_ot_night += float(n_ot)

    return {
        "days": total_days,
        "hours": float(f"{total_hours:.2f}"),
        "ot_day": float(f"{total_ot_day:.2f}"),
        "ot_night": float(f"{total_ot_night:.2f}"),
    }

def safe_filename(s: str, maxlen: int = 60) -> str:
    s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", str(s or "")).strip()
    return re.sub(r"_+", "_", s)[:maxlen]

def _norm_fio(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("ё", "е")
    s = re.sub(r"[.\t\r\n]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def _best_fio_match_with_score(skud_fio: str, candidates: List[str]) -> Tuple[Optional[str], float]:
    nf = _norm_fio(skud_fio)
    if not nf:
        return None, 0.0

    best_name = None
    best_score = 0.0
    for cand in candidates:
        nc = _norm_fio(cand)
        if not nc:
            continue
        score = difflib.SequenceMatcher(None, nf, nc).ratio()
        if score > best_score:
            best_score = score
            best_name = cand

    return best_name, float(best_score)

def _round_hours_nearest(duration_minutes: int) -> int:
    """
    Округление до ближайшего часа с порогом 30 минут:
      7:29 -> 7
      7:30 -> 8
    """
    if duration_minutes <= 0:
        return 0
    return int((duration_minutes + 30) // 60)

def _read_skud_events_from_xlsx(path: str) -> List[Dict[str, Any]]:
    """
    Читает Excel-отчёт СКУД.
    Возвращает события:
      {"dt": datetime, "fio": str, "event": "in"|"out"}
    """
    wb = load_workbook(path, data_only=True)
    ws = wb.active  # обычно нужный лист первый

    header_row = None
    header_map: Dict[str, int] = {}

    max_scan = min(ws.max_row or 0, 60)
    max_cols = ws.max_column or 0

    for r in range(1, max_scan + 1):
        row_vals = [str(ws.cell(r, c).value or "").strip() for c in range(1, max_cols + 1)]
        if "Время" in row_vals and "Событие" in row_vals and "ФИО сотрудника" in row_vals:
            header_row = r
            for c, name in enumerate(row_vals, start=1):
                if name:
                    header_map[name] = c
            break

    if not header_row:
        raise RuntimeError("Не найден заголовок отчёта СКУД (колонки 'Время', 'ФИО сотрудника', 'Событие').")

    c_time = header_map.get("Время")
    c_fio = header_map.get("ФИО сотрудника")
    c_event = header_map.get("Событие")
    if not (c_time and c_fio and c_event):
        raise RuntimeError("В отчёте СКУД не найдены нужные колонки ('Время','ФИО сотрудника','Событие').")

    events: List[Dict[str, Any]] = []
    for r in range(header_row + 1, (ws.max_row or header_row) + 1):
        tval = ws.cell(r, c_time).value
        fio = str(ws.cell(r, c_fio).value or "").strip()
        ev = str(ws.cell(r, c_event).value or "").strip()

        if not fio:
            continue
        if ev not in ("Вход", "Выход"):
            continue

        dt = None
        if isinstance(tval, datetime):
            dt = tval
        else:
            s = str(tval or "").strip()
            if not s:
                continue
            try:
                dt = datetime.strptime(s, "%d.%m.%Y %H:%M:%S")
            except Exception:
                try:
                    dt = datetime.strptime(s, "%d.%m.%Y %H:%M")
                except Exception:
                    continue

        events.append({
            "dt": dt,
            "fio": fio,
            "event": ("in" if ev == "Вход" else "out"),
        })

    return events

def _compute_day_summary_from_events(
    events: List[Dict[str, Any]],
    target_date: date,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    by_fio: Dict[str, List[Dict[str, Any]]] = {}
    for e in events:
        dt = e.get("dt")
        if not isinstance(dt, datetime):
            continue
        if dt.date() != target_date:
            continue
        fio = (e.get("fio") or "").strip()
        if not fio:
            continue
        by_fio.setdefault(fio, []).append(e)

    summary: Dict[str, Dict[str, Any]] = {}
    problems: List[Dict[str, Any]] = []

    for fio, lst in by_fio.items():
        lst.sort(key=lambda x: x["dt"])
        ins = [x["dt"] for x in lst if x.get("event") == "in"]
        outs = [x["dt"] for x in lst if x.get("event") == "out"]

        if not ins or not outs:
            problems.append({
                "skud_fio": fio,
                "has_in": bool(ins),
                "has_out": bool(outs),
                "first_in": ins[0] if ins else None,
                "last_out": outs[-1] if outs else None,
                "count_in": len(ins),
                "count_out": len(outs),
            })
            continue

        first_in = ins[0]
        last_out = outs[-1]
        minutes = int((last_out - first_in).total_seconds() // 60)
        if minutes < 0:
            minutes = 0

        if minutes > 4 * 60:
            minutes = max(0, minutes - 60)
        
        summary[fio] = {
            "first_in": first_in,
            "last_out": last_out,
            "minutes": minutes,
            "hours_rounded": _round_hours_nearest(minutes),
            "count_in": len(ins),
            "count_out": len(outs),
        }


    return summary, problems

class SelectDateDialog(simpledialog.Dialog):
    def __init__(self, parent, init_date: date):
        self.init_date = init_date
        self.result = None
        super().__init__(parent, title="Выбор даты (СКУД)")

    def body(self, master):
        tk.Label(master, text="Дата (дд.мм.гггг):").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=(4, 4))
        self.ent = ttk.Entry(master, width=16)
        self.ent.grid(row=0, column=1, sticky="w", pady=(4, 4))
        self.ent.insert(0, self.init_date.strftime("%d.%m.%Y"))
        return self.ent

    def validate(self):
        s = (self.ent.get() or "").strip()
        try:
            d = datetime.strptime(s, "%d.%m.%Y").date()
            self._d = d
            return True
        except Exception:
            messagebox.showwarning("СКУД", "Введите дату в формате дд.мм.гггг")
            return False

    def apply(self):
        self.result = self._d

class CopyFromDialog(simpledialog.Dialog):
    def __init__(self, parent, init_year: int, init_month: int):
        self.init_year = init_year
        self.init_month = init_month
        self.result = None
        super().__init__(parent, title="Копировать сотрудников из месяца")

    def body(self, master):
        tk.Label(master, text="Источник").grid(row=0, column=0, sticky="w", pady=(2, 6), columnspan=4)

        tk.Label(master, text="Месяц:").grid(row=1, column=0, sticky="e")
        self.cmb_month = ttk.Combobox(master, state="readonly", width=18,
                                      values=[month_name_ru(i) for i in range(1, 13)])
        self.cmb_month.grid(row=1, column=1, sticky="w")
        self.cmb_month.current(max(0, min(11, self.init_month - 1)))

        tk.Label(master, text="Год:").grid(row=1, column=2, sticky="e", padx=(10, 4))
        self.spn_year = tk.Spinbox(master, from_=2000, to=2100, width=6)
        self.spn_year.grid(row=1, column=3, sticky="w")
        self.spn_year.delete(0, "end")
        self.spn_year.insert(0, str(self.init_year))

        self.var_copy_hours = tk.BooleanVar(value=False)
        ttk.Checkbutton(master, text="Копировать часы", variable=self.var_copy_hours)\
            .grid(row=2, column=1, sticky="w", pady=(8, 2))

        tk.Label(master, text="Режим:").grid(row=3, column=0, sticky="e", pady=(6, 2))
        self.var_mode = tk.StringVar(value="replace")
        frame_mode = tk.Frame(master)
        frame_mode.grid(row=3, column=1, columnspan=3, sticky="w", pady=(6, 2))
        ttk.Radiobutton(frame_mode, text="Заменить текущий список",
                        value="replace", variable=self.var_mode).pack(anchor="w")
        ttk.Radiobutton(frame_mode, text="Объединить (добавить недостающих)",
                        value="merge", variable=self.var_mode).pack(anchor="w")
        return self.cmb_month

    def validate(self):
        try:
            y = int(self.spn_year.get())
            if not (2000 <= y <= 2100):
                raise ValueError
            return True
        except Exception:
            messagebox.showwarning("Копирование", "Введите корректный год (2000–2100).")
            return False

    def apply(self):
        self.result = {
            "year": int(self.spn_year.get()),
            "month": self.cmb_month.current() + 1,
            "with_hours": bool(self.var_copy_hours.get()),
            "mode": self.var_mode.get(),
        }

class SelectObjectIdDialog(tk.Toplevel):

    def __init__(self, parent, objects_for_addr: List[Tuple[str, str, str]], addr: str):
        super().__init__(parent)
        self.title("Выбор ID объекта")
        self.resizable(True, True)
        self.grab_set()
        self.result: Optional[str] = None

        main = tk.Frame(self, padx=10, pady=10)
        main.pack(fill="both", expand=True)

        tk.Label(
            main,
            text=f"По адресу:\n{addr}\nнайдено несколько объектов.\nВыберите нужный ID:",
            justify="left",
        ).pack(anchor="w")

        cols = ("excel_id", "address", "short_name")
        self.tree = ttk.Treeview(
            main, columns=cols, show="headings", height=8, selectmode="browse"
        )
        self.tree.heading("excel_id", text="ID (excel_id)")
        self.tree.heading("address", text="Адрес")
        self.tree.heading("short_name", text="Краткое имя")

        self.tree.column("excel_id", width=120, anchor="center", stretch=False)
        self.tree.column("address", width=260, anchor="w")
        self.tree.column("short_name", width=200, anchor="w")

        vsb = ttk.Scrollbar(main, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.pack(side="left", fill="both", expand=True, pady=(8, 4))
        vsb.pack(side="right", fill="y")

        for code, a, short_name in objects_for_addr:
            self.tree.insert("", "end", values=(code, a, short_name))

        btns = tk.Frame(main)
        btns.pack(fill="x", pady=(6, 0))
        ttk.Button(btns, text="OK", command=self._on_ok).pack(side="right", padx=(4, 0))
        ttk.Button(btns, text="Отмена", command=self._on_cancel).pack(side="right")

        self.tree.bind("<Double-1>", self._on_ok)
        self.tree.bind("<Return>",  self._on_ok)

        try:
            self.update_idletasks()
            px = parent.winfo_rootx()
            py = parent.winfo_rooty()
            pw = parent.winfo_width()
            ph = parent.winfo_height()
            sw = self.winfo_width()
            sh = self.winfo_height()
            self.geometry(f"+{px + (pw - sw)//2}+{py + (ph - sh)//2}")
        except Exception:
            pass

    def _on_ok(self, event=None):
        sel = self.tree.selection()
        if not sel:
            messagebox.showwarning("Выбор ID объекта", "Сначала выберите строку.", parent=self)
            return
        vals = self.tree.item(sel[0], "values")
        if not vals:
            return
        self.result = vals[0]  # excel_id
        self.destroy()

    def _on_cancel(self, event=None):
        self.result = None
        self.destroy()

class SkudMappingReviewDialog(tk.Toplevel):
    def __init__(self, parent, rows: List[Dict[str, Any]], problems: List[Dict[str, Any]]):
        super().__init__(parent)
        self.parent = parent
        self.title("СКУД — проверка сопоставления перед применением")
        self.resizable(True, True)
        self.grab_set()

        self.result = None
        self._rows = rows
        self._problems = problems

        main = tk.Frame(self, padx=10, pady=10)
        main.pack(fill="both", expand=True)

        tk.Label(
            main,
            text="Проверьте сопоставления. Снимите галочки с неверных строк.\n"
                 "Проблемы (нет входа/выхода) показаны ниже и НЕ применяются.",
            justify="left",
        ).pack(anchor="w")

        table_frame = tk.Frame(main)
        table_frame.pack(fill="both", expand=True, pady=(8, 6))

        cols = ("apply", "skud_fio", "matched_fio", "score", "hours", "interval", "counts")
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=12)
        self.tree.heading("apply", text="Применять")
        self.tree.heading("skud_fio", text="ФИО из СКУД")
        self.tree.heading("matched_fio", text="Сопоставлено")
        self.tree.heading("score", text="Score")
        self.tree.heading("hours", text="Часы")
        self.tree.heading("interval", text="Интервал")
        self.tree.heading("counts", text="Вх/Вых")

        self.tree.column("apply", width=90, anchor="center", stretch=False)
        self.tree.column("skud_fio", width=280, anchor="w")
        self.tree.column("matched_fio", width=280, anchor="w")
        self.tree.column("score", width=70, anchor="center", stretch=False)
        self.tree.column("hours", width=60, anchor="center", stretch=False)
        self.tree.column("interval", width=220, anchor="w")
        self.tree.column("counts", width=70, anchor="center", stretch=False)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.tag_configure("low_score", foreground="#b00020")
        self.tree.tag_configure("normal", foreground="#000000")

        self._apply_state: Dict[str, bool] = {}

        for i, r in enumerate(self._rows):
            iid = f"m_{i}"
            apply_default = bool(r.get("apply", True))
            self._apply_state[iid] = apply_default

            score = r.get("score")
            score_str = f"{score:.2f}" if isinstance(score, (int, float)) else ""
            hours = r.get("hours_rounded")
            hours_str = str(hours) if hours is not None else ""

            first_in = r.get("first_in")
            last_out = r.get("last_out")
            interval = ""
            if isinstance(first_in, datetime) and isinstance(last_out, datetime):
                interval = f"{first_in.strftime('%H:%M:%S')} – {last_out.strftime('%H:%M:%S')}"

            counts = f"{r.get('count_in', 0)}/{r.get('count_out', 0)}"

            apply_mark = "[x]" if apply_default else "[ ]"

            tag = "normal"
            if isinstance(score, (int, float)) and score < 0.90:
                tag = "low_score"

            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(apply_mark, r.get("skud_fio", ""), r.get("matched_fio", ""), score_str, hours_str, interval, counts),
                tags=(tag,),
            )

        self.tree.bind("<Button-1>", self._on_click)
        self.tree.bind("<Double-1>", self._on_click)

        prob_frame = tk.LabelFrame(main, text="Проблемы (нет входа/выхода) — НЕ применяются", padx=8, pady=8)
        prob_frame.pack(fill="both", expand=True, pady=(6, 8))
        
        if not self._problems:
            tk.Label(prob_frame, text="Проблем не найдено.").pack(anchor="w")
        else:
            txt = tk.Text(prob_frame, height=7, wrap="word")
            txt.pack(fill="both", expand=True)

            txt.insert("end", "Невозможно посчитать часы автоматически:\n\n")
            for p in self._problems:
                skud_fio = p.get("skud_fio", "")
                has_in = bool(p.get("has_in"))
                has_out = bool(p.get("has_out"))

                parts = []
                if has_in and not has_out:
                    parts.append("есть ВХОД, нет ВЫХОДА")
                if has_out and not has_in:
                    parts.append("есть ВЫХОД, нет ВХОДА")

                first_in = p.get("first_in")
                last_out = p.get("last_out")
                t_in = first_in.strftime("%H:%M:%S") if isinstance(first_in, datetime) else "-"
                t_out = last_out.strftime("%H:%M:%S") if isinstance(last_out, datetime) else "-"
                cnt_in = p.get("count_in", 0)
                cnt_out = p.get("count_out", 0)

                txt.insert("end", f"- {skud_fio} ({'/'.join(parts)}) | Вх/Вых {cnt_in}/{cnt_out} | {t_in} – {t_out}\n")

            txt.configure(state="disabled")

        btns = tk.Frame(main)
        btns.pack(fill="x")

        ttk.Button(btns, text="Отметить всех", command=self._select_all).pack(side="left")
        ttk.Button(btns, text="Снять всех", command=self._clear_all).pack(side="left", padx=(6, 0))
        ttk.Button(btns, text="Выгрузить проблемы в Excel", command=self._export_problems_to_excel).pack(side="left", padx=(6, 0))
        
        ttk.Button(btns, text="Применить", command=self._on_apply).pack(side="right", padx=(6, 0))
        ttk.Button(btns, text="Отмена", command=self._on_cancel).pack(side="right")

        try:
            self.update_idletasks()
            px = parent.winfo_rootx()
            py = parent.winfo_rooty()
            pw = parent.winfo_width()
            ph = parent.winfo_height()
            sw = self.winfo_width()
            sh = self.winfo_height()
            self.geometry(f"+{px + (pw - sw)//2}+{py + (ph - sh)//2}")
        except Exception:
            pass

    def _toggle_iid(self, iid: str):
        cur = bool(self._apply_state.get(iid, True))
        new = not cur
        self._apply_state[iid] = new

        vals = list(self.tree.item(iid, "values"))
        if vals:
            vals[0] = "[x]" if new else "[ ]"
            self.tree.item(iid, values=tuple(vals))

    def _on_click(self, event=None):
        row_id = self.tree.identify_row(event.y) if event else None
        col = self.tree.identify_column(event.x) if event else None
        if not row_id or col != "#1":  # apply колонка
            return
        self._toggle_iid(row_id)

    def _select_all(self):
        for iid in self.tree.get_children():
            self._apply_state[iid] = True
            vals = list(self.tree.item(iid, "values"))
            if vals:
                vals[0] = "[x]"
                self.tree.item(iid, values=tuple(vals))

    def _clear_all(self):
        for iid in self.tree.get_children():
            self._apply_state[iid] = False
            vals = list(self.tree.item(iid, "values"))
            if vals:
                vals[0] = "[ ]"
                self.tree.item(iid, values=tuple(vals))

    def _export_problems_to_excel(self):
        if not self._problems:
            messagebox.showinfo("СКУД", "Проблем нет — выгружать нечего.", parent=self)
            return
    
        from tkinter import filedialog
        default_name = f"СКУД_проблемы_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        path = filedialog.asksaveasfilename(
            parent=self,
            title="Сохранить проблемы СКУД в Excel",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel", "*.xlsx"), ("Все файлы", "*.*")],
        )
        if not path:
            return
    
        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Проблемы СКУД"
    
            header = [
                "ФИО (СКУД)",
                "Проблема",
                "Входов",
                "Выходов",
                "Первый вход",
                "Последний выход",
            ]
            ws.append(header)
    
            # ширины колонок
            ws.column_dimensions["A"].width = 32
            ws.column_dimensions["B"].width = 26
            ws.column_dimensions["C"].width = 10
            ws.column_dimensions["D"].width = 10
            ws.column_dimensions["E"].width = 20
            ws.column_dimensions["F"].width = 20
    
            for p in self._problems:
                skud_fio = p.get("skud_fio", "") or ""
                has_in = bool(p.get("has_in"))
                has_out = bool(p.get("has_out"))
    
                if has_in and not has_out:
                    problem = "Есть вход, нет выхода"
                elif has_out and not has_in:
                    problem = "Есть выход, нет входа"
                else:
                    problem = "Неопределено"
    
                cnt_in = int(p.get("count_in") or 0)
                cnt_out = int(p.get("count_out") or 0)
    
                first_in = p.get("first_in")
                last_out = p.get("last_out")
    
                first_in_str = first_in.strftime("%d.%m.%Y %H:%M:%S") if isinstance(first_in, datetime) else ""
                last_out_str = last_out.strftime("%d.%m.%Y %H:%M:%S") if isinstance(last_out, datetime) else ""
    
                ws.append([skud_fio, problem, cnt_in, cnt_out, first_in_str, last_out_str])
    
            wb.save(path)
            messagebox.showinfo("СКУД", f"Проблемы сохранены:\n{path}", parent=self)
    
        except Exception as e:
            messagebox.showerror("СКУД", f"Ошибка выгрузки проблем в Excel:\n{e}", parent=self)

    def _on_apply(self):
        selected_rows = []
        for i, r in enumerate(self._rows):
            iid = f"m_{i}"
            if self._apply_state.get(iid, False):
                selected_rows.append(r)

        self.result = {"apply": True, "rows": selected_rows}
        self.destroy()

    def _on_cancel(self):
        self.result = {"apply": False, "rows": []}
        self.destroy()

class SelectEmployeesDialog(tk.Toplevel):

    def __init__(self, parent, employees, current_dep: str):
        super().__init__(parent)
        self.parent = parent
        self.employees = employees
        self.current_dep = (current_dep or "").strip()
        self.result = None

        self.title("Выбор сотрудников")
        self.resizable(True, True)
        self.grab_set()

        self.var_only_dep = tk.BooleanVar(
            value=bool(self.current_dep and self.current_dep != "Все")
        )
        self.var_search = tk.StringVar()

        main = tk.Frame(self, padx=10, pady=10)
        main.pack(fill="both", expand=True)

        # --- Верхняя панель ---
        top = tk.Frame(main)
        top.pack(fill="x")

        tk.Label(
            top,
            text=f"Подразделение: {self.current_dep or 'Все'}",
            font=("Segoe UI", 10, "bold"),
        ).grid(row=0, column=0, columnspan=2, sticky="w")

        ttk.Checkbutton(
            top,
            text="Показывать только сотрудников этого подразделения",
            variable=self.var_only_dep,
            command=self._refilter,
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 4))

        tk.Label(top, text="Поиск (ФИО / таб.№):").grid(
            row=2, column=0, sticky="w", pady=(4, 2)
        )
        ent_search = ttk.Entry(top, textvariable=self.var_search, width=40)
        ent_search.grid(row=2, column=1, sticky="w", pady=(4, 2))
        ent_search.bind("<KeyRelease>", lambda e: self._refilter())

        tbl_frame = tk.Frame(main)
        tbl_frame.pack(fill="both", expand=True, pady=(8, 4))

        columns = ("fio", "tbn", "pos", "dep")
        # первая псевдо-колонка "#" под чекбокс
        self.tree = ttk.Treeview(
            tbl_frame,
            columns=columns,
            show="headings",
            selectmode="none",  # выбор только через чекбокс
        )

        self.tree.heading("fio", text="ФИО")
        self.tree.heading("tbn", text="Таб.№")
        self.tree.heading("pos", text="Должность")
        self.tree.heading("dep", text="Подразделение")

        self.tree.column("fio", width=260, anchor="w")
        self.tree.column("tbn", width=80, anchor="center", stretch=False)
        self.tree.column("pos", width=180, anchor="w")
        self.tree.column("dep", width=140, anchor="w")

        bold_font = ("Segoe UI", 9, "bold")
        normal_font = ("Segoe UI", 9)
        self.tree.tag_configure("checked", font=bold_font)
        self.tree.tag_configure("unchecked", font=normal_font)

        vsb = ttk.Scrollbar(tbl_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.bind("<Button-1>", self._on_tree_click)

        self._filtered_indices = []

        self._selected_indices = set()

        self._refilter()
        self._update_selected_count()

        # --- Кнопки управления выбором ---
        sel_frame = tk.Frame(main)
        sel_frame.pack(fill="x")
        ttk.Button(sel_frame, text="Отметить всех", command=self._select_all).pack(
            side="left", padx=(0, 4)
        )
        ttk.Button(sel_frame, text="Снять все", command=self._clear_all).pack(
            side="left", padx=4
        )

        self.lbl_selected = tk.Label(
            sel_frame,
            text="Выбрано: 0",
            bg=sel_frame["bg"],
        )
        self.lbl_selected.pack(side="right")

        # --- Низ: OK / Отмена ---
        btns = tk.Frame(main)
        btns.pack(fill="x", pady=(8, 0))
        ttk.Button(btns, text="OK", command=self._on_ok).pack(
            side="right", padx=(4, 0)
        )
        ttk.Button(btns, text="Отмена", command=self._on_cancel).pack(
            side="right"
        )

        main.rowconfigure(2, weight=1)
        main.columnconfigure(0, weight=1)

        # Центрируем
        try:
            self.update_idletasks()
            px = parent.winfo_rootx()
            py = parent.winfo_rooty()
            pw = parent.winfo_width()
            ph = parent.winfo_height()
            sw = self.winfo_width()
            sh = self.winfo_height()
            self.geometry(f"+{px + (pw - sw)//2}+{py + (ph - sh)//2}")
        except Exception:
            pass

    def _update_selected_count(self):
        """Обновляет текст 'Выбрано: N'."""
        try:
            self.lbl_selected.config(text=f"Выбрано: {len(self._selected_indices)}")
        except Exception:
            pass

    def _refilter(self):
        """Перестроить список в treeview по фильтрам."""
        search = self.var_search.get().strip().lower()
        only_dep = self.var_only_dep.get()
        dep_sel = self.current_dep

        self.tree.delete(*self.tree.get_children())
        self._filtered_indices.clear()

        for idx, (fio, tbn, pos, dep) in enumerate(self.employees):
            if only_dep and dep_sel and dep_sel != "Все":
                if (dep or "").strip() != dep_sel:
                    continue

            if search:
                if search not in fio.lower() and search not in (tbn or "").lower():
                    continue

            # Отобразим строку
            # "чекбокс" будем рисовать через префикс [x]/[ ] у ФИО либо через tag
            checked = (idx in self._selected_indices)
            display_fio = f"[{'x' if checked else ' '}] {fio}"

            iid = f"emp_{idx}"
            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(display_fio, tbn, pos, dep),
                tags=("checked" if checked else "unchecked",),
            )
            self._filtered_indices.append(idx)

    def _toggle_index(self, idx: int):
        """Переключает выбранность сотрудника по глобальному индексу employees."""
        if idx in self._selected_indices:
            self._selected_indices.remove(idx)
        else:
            self._selected_indices.add(idx)
        self._update_selected_count()

    def _on_tree_click(self, event):
        """
        ЛКМ по строке — переключаем чекбокс.
        """
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return

        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return

        # Ищем индекс в _filtered_indices
        try:
            # i — позиция в текущем отфильтрованном списке
            i = self.tree.index(row_id)
            emp_index = self._filtered_indices[i]
        except Exception:
            return

        self._toggle_index(emp_index)
        # Обновим отображение только этой строки
        fio, tbn, pos, dep = self.employees[emp_index]
        checked = (emp_index in self._selected_indices)
        display_fio = f"[{'x' if checked else ' '}] {fio}"
        self.tree.item(
            row_id,
            values=(display_fio, tbn, pos, dep),
            tags=("checked" if checked else "unchecked",),
        )

    def _select_all(self):
        """Отметить всех в текущей выборке."""
        for emp_index in self._filtered_indices:
            self._selected_indices.add(emp_index)
        self._refilter()
        self._update_selected_count()

    def _clear_all(self):
        """Снять все отметки (по всему списку)."""
        self._selected_indices.clear()
        self._refilter()
        self._update_selected_count()

    def _on_ok(self):
        if not self._selected_indices:
            if not messagebox.askyesno(
                "Выбор сотрудников",
                "Не выбрано ни одного сотрудника.\nЗакрыть окно?",
                parent=self,
            ):
                return
            self.result = []
        else:
            chosen = [self.employees[i] for i in sorted(self._selected_indices)]
            self.result = chosen
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()

class BatchAddDialog(tk.Toplevel):
    def __init__(self, parent, total: int, title: str = "Добавление сотрудников"):
        super().__init__(parent)
        self.parent = parent
        self.total = max(1, int(total))
        self.done = 0
        self.cancelled = False
        self.title(title)
        self.resizable(False, False)
        self.grab_set()

        frm = tk.Frame(self, padx=12, pady=12)
        frm.pack(fill="both", expand=True)

        self.lbl = tk.Label(frm, text=f"Добавлено: 0 из {self.total}")
        self.lbl.pack(fill="x")

        self.pb = ttk.Progressbar(frm, mode="determinate",
                                  maximum=self.total, length=420)
        self.pb.pack(fill="x", pady=(8, 8))

        self.btn_cancel = ttk.Button(frm, text="Отмена", command=self._on_cancel)
        self.btn_cancel.pack(anchor="e", pady=(6, 0))

        try:
            self.update_idletasks()
            px = parent.winfo_rootx()
            py = parent.winfo_rooty()
            pw = parent.winfo_width()
            ph = parent.winfo_height()
            sw = self.winfo_width()
            sh = self.winfo_height()
            self.geometry(f"+{px + (pw - sw)//2}+{py + (ph - sh)//2}")
        except Exception:
            pass

    def step(self, n: int = 1):
        if self.cancelled:
            return
        self.done += n
        if self.done > self.total:
            self.done = self.total
        self.pb["value"] = self.done
        self.lbl.config(text=f"Добавлено: {self.done} из {self.total}")
        self.update_idletasks()

    def _on_cancel(self):
        self.cancelled = True

    def close(self):
        try:
            self.grab_release()
        except Exception:
            pass
        self.destroy()


class HoursFillDialog(simpledialog.Dialog):
    def __init__(self, parent, max_day: int):
        self.max_day = max_day
        self.result = None
        super().__init__(parent, title="Проставить часы всем")

    def body(self, master):
        tk.Label(master, text=f"В текущем месяце дней: {self.max_day}")\
            .grid(row=0, column=0, columnspan=3, sticky="w", pady=(2, 6))
        tk.Label(master, text="День:").grid(row=1, column=0, sticky="e")
        self.spn_day = tk.Spinbox(master, from_=1, to=31, width=4)
        self.spn_day.grid(row=1, column=1, sticky="w")
        self.spn_day.delete(0, "end")
        self.spn_day.insert(0, "1")

        self.var_clear = tk.BooleanVar(value=False)
        ttk.Checkbutton(master, text="Очистить день (пусто)",
                        variable=self.var_clear,
                        command=self._on_toggle_clear)\
            .grid(row=2, column=1, sticky="w", pady=(6, 2))

        tk.Label(master, text="Часы:").grid(row=3, column=0, sticky="e", pady=(6, 0))
        self.ent_hours = ttk.Entry(master, width=12)
        self.ent_hours.grid(row=3, column=1, sticky="w", pady=(6, 0))
        self.ent_hours.insert(0, "8")

        tk.Label(master, text="Форматы: 8 | 8,25 | 8:30 | 1/7")\
            .grid(row=4, column=0, columnspan=3, sticky="w", pady=(6, 2))
        return self.spn_day

    def _on_toggle_clear(self):
        if self.var_clear.get():
            self.ent_hours.configure(state="disabled")
        else:
            self.ent_hours.configure(state="normal")

    def validate(self):
        try:
            d = int(self.spn_day.get())
            if not (1 <= d <= 31):
                raise ValueError
        except Exception:
            messagebox.showwarning(
                "Проставить часы",
                "День должен быть числом от 1 до 31.",
            )
            return False

        if self.var_clear.get():
            self._d = d
            self._h = 0.0
            self._clear = True
            return True

        hv = parse_hours_value(self.ent_hours.get().strip())
        if hv is None or hv < 0:
            messagebox.showwarning(
                "Проставить часы",
                "Введите корректное значение часов (например, 8, 8:30, 1/7).",
            )
            return False
        self._d = d
        self._h = float(hv)
        self._clear = False
        return True

    def apply(self):
        self.result = {
            "day": self._d,
            "hours": self._h,
            "clear": self._clear,
        }


class AutoCompleteCombobox(ttk.Combobox):

    def __init__(self, master=None, **kw):
        super().__init__(master, **kw)
        self._all_values: List[str] = []
        self.bind("<KeyRelease>", self._on_keyrelease)
        self.bind("<Control-BackSpace>", self._clear_all)
        self.bind("<FocusOut>", self._on_focus_out)

    def set_values(self, values: List[str]):
        """Задать полный список значений для автодополнения."""
        self._all_values = list(values) if values is not None else []
        self.config(values=self._all_values)

    # Обратная совместимость со старым кодом
    def set_completion_list(self, values: List[str]):
        """Совместимый с старым кодом метод; просто вызывает set_values."""
        self.set_values(values)

    def _on_keyrelease(self, event):
        # Не перехватываем управляющие клавиши
        if event.keysym in ("BackSpace", "Left", "Right", "Up", "Down", "Return", "Tab"):
            return

        text = self.get().strip()
        if not text:
            self.config(values=self._all_values)
            return

        filtered = [v for v in self._all_values if text.lower() in v.lower()]
        self.config(values=filtered)

    def _clear_all(self, event):
        self.delete(0, tk.END)
        self.config(values=self._all_values)

    def _on_focus_out(self, event):
        """
        Строгий режим: при потере фокуса, если текущее значение не найдено
        в полном списке значений, очищаем поле.
        """
        current = self.get().strip()
        if current and current not in self._all_values:
            self.set("")

class TimeForSelectedDialog(simpledialog.Dialog):
    """
    Диалог: ввести значение часов и диапазон дней,
    которые будут проставлены у выделенных сотрудников.
    """
    def __init__(self, parent, max_day: int):
        self.max_day = max_day
        self.result = None
        super().__init__(parent, title="Время для выделенных сотрудников")

    def body(self, master):
        tk.Label(master, text=f"В текущем месяце дней: {self.max_day}")\
            .grid(row=0, column=0, columnspan=4, sticky="w", pady=(4, 4))

        # Режим: один день или диапазон
        self.var_mode = tk.StringVar(value="single")
        rb_single = ttk.Radiobutton(master, text="Один день", value="single", variable=self.var_mode)
        rb_range = ttk.Radiobutton(master, text="Диапазон дней", value="range", variable=self.var_mode)
        rb_single.grid(row=1, column=0, sticky="w", pady=(2, 2), columnspan=2)
        rb_range.grid(row=1, column=2, sticky="w", pady=(2, 2), columnspan=2)

        # Один день
        tk.Label(master, text="День:").grid(row=2, column=0, sticky="e")
        self.spn_day = tk.Spinbox(master, from_=1, to=31, width=4)
        self.spn_day.grid(row=2, column=1, sticky="w")
        self.spn_day.delete(0, "end")
        self.spn_day.insert(0, "1")

        # Диапазон
        tk.Label(master, text="С:").grid(row=3, column=0, sticky="e")
        self.spn_from = tk.Spinbox(master, from_=1, to=31, width=4)
        self.spn_from.grid(row=3, column=1, sticky="w")
        self.spn_from.delete(0, "end")
        self.spn_from.insert(0, "1")

        tk.Label(master, text="по:").grid(row=3, column=2, sticky="e")
        self.spn_to = tk.Spinbox(master, from_=1, to=31, width=4)
        self.spn_to.grid(row=3, column=3, sticky="w")
        self.spn_to.delete(0, "end")
        self.spn_to.insert(0, str(self.max_day))

        # Значение
        tk.Label(master, text="Часы:").grid(row=4, column=0, sticky="e", pady=(6, 0))
        self.ent_value = ttk.Entry(master, width=20)
        self.ent_value.grid(row=4, column=1, columnspan=3, sticky="w", pady=(6, 0))
        self.ent_value.insert(0, "8,25")  # типовое значение

        tk.Label(master, text="Примеры: 8 | 8,25 | 8:30 | 1/7 | 8/2(1/1)\n"
                              "Пусто — очистить выбранные дни")\
            .grid(row=5, column=0, columnspan=4, sticky="w", pady=(6, 0))

        return self.ent_value

    def validate(self):
        mode = self.var_mode.get()

        # Проверяем дни
        try:
            d_single = int(self.spn_day.get())
            d_from = int(self.spn_from.get())
            d_to = int(self.spn_to.get())
        except Exception:
            messagebox.showwarning("Время для выделенных", "Дни должны быть целыми числами.")
            return False

        if not (1 <= d_single <= self.max_day):
            messagebox.showwarning(
                "Время для выделенных",
                f"Один день должен быть от 1 до {self.max_day}.",
            )
            return False

        if not (1 <= d_from <= self.max_day) or not (1 <= d_to <= self.max_day):
            messagebox.showwarning(
                "Время для выделенных",
                f"Диапазон дней должен быть в пределах 1–{self.max_day}.",
            )
            return False

        if mode == "range" and d_from > d_to:
            messagebox.showwarning(
                "Время для выделенных",
                "Начальный день диапазона не может быть больше конечного.",
            )
            return False

        self._mode = mode
        if mode == "single":
            self._from = self._to = d_single
        else:
            self._from, self._to = d_from, d_to

        # Проверяем значение часов
        val = self.ent_value.get().strip()
        if not val:
            # Пустое — разрешаем (значит очистить)
            self._value = None
            return True

        hv = parse_hours_value(val)
        if hv is None or hv < 0:
            messagebox.showwarning(
                "Время для выделенных",
                "Введите корректное значение часов (например, 8, 8:30, 1/7, 8/2(1/1)).",
            )
            return False

        self._value = val
        return True

    def apply(self):
        # result: словарь с диапазоном и строковым значением (или None)
        self.result = {
            "from": self._from,
            "to": self._to,
            "value": self._value,  # None => очистка
        }

class TimesheetPage(tk.Frame):

    COLPX = {"fio": 200, "tbn": 100, "day": 36, "days": 46, "hours": 56, "del": 66}
    MIN_FIO_PX = 140
    MAX_FIO_PX = 260
    HEADER_BG = "#d0d0d0"

    def __init__(
        self,
        master,
        app_ref,
        init_object_id: Optional[str] = None,
        init_object_addr: Optional[str] = None,
        init_department: Optional[str] = None,
        init_year: Optional[int] = None,
        init_month: Optional[int] = None,
        read_only: bool = False,
        owner_user_id: Optional[int] = None,
    ):
        super().__init__(master)
        self.app_ref = app_ref
        self.read_only = bool(read_only)
        self.owner_user_id: Optional[int] = owner_user_id

        self._init_object_id = init_object_id
        self._init_object_addr = init_object_addr
        self._init_department = init_department
        self._init_year = init_year
        self._init_month = init_month
        self._suppress_object_id_dialog = False

        # Авто‑сохранение
        self._auto_save_job = None
        self._auto_save_delay_ms = 8000

        # output dir
        if get_output_dir_from_config:
            self.out_dir = get_output_dir_from_config()
        else:
            self.out_dir = Path("./output")
        self.out_dir.mkdir(parents=True, exist_ok=True)

        # Данные
        self._load_spr_data_from_db()
        self.allowed_fio_names: set[str] = set()

        # Модель табеля
        self.model_rows: List[Dict[str, Any]] = []
        self.model_rows_all: List[Dict[str, Any]] = []
        self.var_filter = tk.StringVar()

        self.var_brigadier = tk.StringVar(value="Все")
        self._brig_assign: dict[str, str | None] = {}   # employee_tbn -> brigadier_tbn
        self._brig_names: dict[str, str] = {}           # brigadier_tbn -> brigadier fio

        self._fit_job = None
        self._build_ui()

        self.bind("<Configure>", self._on_window_configure)
        self.after(120, self._auto_fit_columns)

    def _grid_selected(self) -> set[int]:
        if hasattr(self, "grid"):
            return self.grid.get_selected_indices()
        return set()

    def _grid_refresh(self, rows_changed: bool = False):
        if not hasattr(self, "grid"):
            return
        if rows_changed:
            self.grid.set_rows(self.model_rows)
        else:
            self.grid.refresh()

    def _schedule_auto_save(self):
        if self.read_only:
            return
        if self._auto_save_job is not None:
            try:
                self.after_cancel(self._auto_save_job)
            except Exception:
                pass
            self._auto_save_job = None
        self._auto_save_job = self.after(self._auto_save_delay_ms, self._auto_save_callback)

    def _auto_save_callback(self):
        self._auto_save_job = None
        self._save_all_internal(show_messages=False, is_auto=True)

    # ------------------ load reference data -------------------

    def _load_spr_data_from_db(self):
        self.employees = load_employees_from_db()  # (fio,tbn,pos,dep)
        self.objects_full = load_objects_short_for_timesheet()  # (excel_id,address,short_name)

        self.emp_names = [fio for (fio, _, _, _) in self.employees]
        self.emp_info = {fio: (tbn, pos) for (fio, tbn, pos, _) in self.employees}

        deps = sorted({(dep or "").strip() for (_, _, _, dep) in self.employees if (dep or "").strip()})
        self.departments = ["Все"] + deps

        self.addr_to_ids: Dict[str, List[str]] = {}
        for oid, addr, short_name in self.objects_full:
            if not addr:
                continue
            self.addr_to_ids.setdefault(addr, [])
            if oid and oid not in self.addr_to_ids[addr]:
                self.addr_to_ids[addr].append(oid)

        self.address_options = sorted({addr for _, addr, _ in self.objects_full if addr})

    def _build_ui(self):
        # --- Top panel ---
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=8)

        tk.Label(top, text="Подразделение:").grid(row=0, column=0, sticky="w")
        deps = self.departments or ["Все"]
        self.cmb_department = ttk.Combobox(top, state="readonly", values=deps, width=48)
        self.cmb_department.grid(row=0, column=1, sticky="w", padx=(4, 12))
        try:
            saved_dep = get_selected_department_from_config() if get_selected_department_from_config else None
            self.cmb_department.set(saved_dep if saved_dep in deps else deps[0])
        except Exception:
            self.cmb_department.set(deps[0])
        self.cmb_department.bind("<<ComboboxSelected>>", lambda e: self._on_department_select())

        tk.Label(top, text="Месяц:").grid(row=1, column=0, sticky="w", padx=(0, 4), pady=(8, 0))
        self.cmb_month = ttk.Combobox(
            top, state="readonly", width=12, values=[month_name_ru(i) for i in range(1, 13)]
        )
        self.cmb_month.grid(row=1, column=1, sticky="w", pady=(8, 0))
        self.cmb_month.current(datetime.now().month - 1)
        self.cmb_month.bind("<<ComboboxSelected>>", lambda e: self._on_period_change())

        tk.Label(top, text="Год:").grid(row=1, column=2, sticky="w", padx=(16, 4), pady=(8, 0))
        self.spn_year = tk.Spinbox(top, from_=2000, to=2100, width=6, command=self._on_period_change)
        self.spn_year.grid(row=1, column=3, sticky="w", pady=(8, 0))
        self.spn_year.delete(0, "end")
        self.spn_year.insert(0, datetime.now().year)
        self.spn_year.bind("<FocusOut>", lambda e: self._on_period_change())

        tk.Label(top, text="Адрес:").grid(row=1, column=4, sticky="w", padx=(20, 4), pady=(8, 0))
        self.cmb_address = AutoCompleteCombobox(top, width=46)
        self.cmb_address.set_completion_list(self.address_options)
        self.cmb_address.grid(row=1, column=5, sticky="w", pady=(8, 0))
        self.cmb_address.bind("<<ComboboxSelected>>", self._on_address_select)
        self.cmb_address.bind("<Return>", lambda e: self._on_address_select())

        tk.Label(top, text="ID объекта:").grid(row=1, column=6, sticky="w", padx=(16, 4), pady=(8, 0))
        self.cmb_object_id = ttk.Combobox(top, state="readonly", values=[], width=18)
        self.cmb_object_id.grid(row=1, column=7, sticky="w", pady=(8, 0))
        self.cmb_object_id.bind("<<ComboboxSelected>>", lambda e: self._load_existing_rows())

        tk.Label(top, text="ФИО:").grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.fio_var = tk.StringVar()
        self.cmb_fio = AutoCompleteCombobox(top, textvariable=self.fio_var, width=30)
        self.cmb_fio.set_completion_list(self.emp_names)
        self.cmb_fio.grid(row=2, column=1, sticky="w", pady=(8, 0))
        self.cmb_fio.bind("<<ComboboxSelected>>", self._on_fio_select)

        tk.Label(top, text="Табельный №:").grid(row=2, column=2, sticky="w", padx=(16, 4), pady=(8, 0))
        self.ent_tbn = ttk.Entry(top, width=14)
        self.ent_tbn.grid(row=2, column=3, sticky="w", pady=(8, 0))

        tk.Label(top, text="Должность:").grid(row=2, column=4, sticky="w", padx=(16, 4), pady=(8, 0))
        self.pos_var = tk.StringVar()
        self.ent_pos = ttk.Entry(top, textvariable=self.pos_var, width=40, state="readonly")
        self.ent_pos.grid(row=2, column=5, sticky="w", pady=(8, 0))

        btns = tk.Frame(top)
        btns.grid(row=3, column=0, columnspan=8, sticky="w", pady=(8, 0))
        ttk.Button(btns, text="Добавить в табель", command=self.add_row).grid(row=0, column=0, padx=4)
        ttk.Button(btns, text="Добавить подразделение", command=self.add_department_all).grid(row=0, column=1, padx=4)
        ttk.Button(btns, text="Выбрать из подразделения…", command=self.add_department_partial).grid(row=0, column=2, padx=4)
        ttk.Button(btns, text="Время (выбранные)", command=self.fill_time_selected).grid(row=0, column=3, padx=4)
        ttk.Button(btns, text="Снять выделение", command=self.clear_selection).grid(row=0, column=4, padx=4)
        ttk.Button(btns, text="Проставить часы", command=self.fill_hours_all).grid(row=0, column=5, padx=4)
        ttk.Button(btns, text="Очистить все строки", command=self.clear_all_rows).grid(row=0, column=6, padx=4)
        ttk.Button(btns, text="Загрузить из Excel", command=self.import_from_excel).grid(row=0, column=7, padx=4)
        ttk.Button(btns, text="Копировать из месяца…", command=self.copy_from_month).grid(row=0, column=8, padx=4)
        ttk.Button(btns, text="Загрузить СКУД…", command=self.import_from_skud).grid(row=0, column=9, padx=4)
        ttk.Button(btns, text="Сохранить", command=self.save_all).grid(row=0, column=10, padx=4)

        filter_frame = tk.Frame(self)
        filter_frame.pack(fill="x", padx=8, pady=(0, 4))
        
        tk.Label(filter_frame, text="Поиск:").pack(side="left")
        ent_filter = ttk.Entry(filter_frame, textvariable=self.var_filter, width=40)
        ent_filter.pack(side="left", padx=(6, 6))
        ttk.Button(filter_frame, text="Очистить", command=self._clear_filter).pack(side="left")

        tk.Label(filter_frame, text="   Бригадир:").pack(side="left", padx=(14, 0))
        
        self.cmb_brigadier = ttk.Combobox(
            filter_frame,
            state="readonly",
            width=34,
            values=["Все"],
            textvariable=self.var_brigadier,
        )
        self.cmb_brigadier.pack(side="left", padx=(6, 0))
        self.cmb_brigadier.bind("<<ComboboxSelected>>", lambda e: self._apply_filter())
        
        # debounce на ввод, чтобы не дергать фильтр на каждый символ мгновенно
        self._filter_job = None
        def _on_filter_key(_e=None):
            try:
                if self._filter_job is not None:
                    self.after_cancel(self._filter_job)
            except Exception:
                pass
            self._filter_job = self.after(120, self._apply_filter)
        
        ent_filter.bind("<KeyRelease>", _on_filter_key)

        # --- Main table: VirtualTimesheetGrid ---
        main_frame = tk.Frame(self)
        main_frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))
        main_frame.grid_rowconfigure(0, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)

        self.grid = VirtualTimesheetGrid(
            main_frame,
            get_year_month=self.get_year_month,
            on_change=self._on_cell_changed,      # <- НОВОЕ имя/сигнатура
            on_delete_row=self._grid_delete_row,
            row_height=22,
            colpx=self.COLPX,
            read_only=self.read_only,
        )
        self.grid.grid(row=0, column=0, sticky="nsew")

        # --- Bottom panel (totals + autosave label) ---
        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=8, pady=(0, 8))

        self.lbl_object_total = tk.Label(
            bottom,
            text="Сумма: сотрудников 0 | дней 0 | часов 0",
            font=("Segoe UI", 10, "bold"),
        )
        self.lbl_object_total.pack(side="left")

        self.lbl_auto_save = tk.Label(
            self,
            text="Последнее авто‑сохранение: нет",
            font=("Segoe UI", 9),
            fg="#555",
            anchor="w",
        )
        self.lbl_auto_save.pack(fill="x", padx=8, pady=(0, 6))

        # --- Initialize inputs ---
        if self._init_department and self._init_department in deps:
            self.cmb_department.set(self._init_department)
        if self._init_year:
            self.spn_year.delete(0, "end")
            self.spn_year.insert(0, str(self._init_year))
        if self._init_month and 1 <= self._init_month <= 12:
            self.cmb_month.current(self._init_month - 1)
        if self._init_object_addr and self._init_object_addr in self.address_options:
            self.cmb_address.set(self._init_object_addr)

        if self._init_object_id:
            try:
                self._suppress_object_id_dialog = True
                self._on_address_change()
            finally:
                self._suppress_object_id_dialog = False
            ids = self.cmb_object_id.cget("values") or []
            if self._init_object_id in ids:
                self.cmb_object_id.set(self._init_object_id)

        self._on_department_select()

        if self.read_only:
            try:
                for child in btns.winfo_children():
                    child.configure(state="disabled")
            except Exception:
                pass
            try:
                self.lbl_object_total.config(
                    text=self.lbl_object_total.cget("text") + " (режим просмотра)"
                )
            except Exception:
                pass

    def _sync_object_id_values_silent(self):
        addr = self.cmb_address.get().strip()
        objects_for_addr = [
            (code, a, short_name)
            for (code, a, short_name) in getattr(self, "objects_full", [])
            if a == addr
        ]
        if not objects_for_addr:
            self.cmb_object_id.config(state="normal", values=[])
            return
    
        ids = sorted({code for (code, a, short_name) in objects_for_addr if code})
        cur = (self.cmb_object_id.get() or "").strip()
    
        self.cmb_object_id.config(state="readonly", values=ids)
        if cur and cur in ids:
            return
        if len(ids) == 1:
            self.cmb_object_id.set(ids[0])

    # ---------------- period / address / department ----------------

    def get_year_month(self) -> Tuple[int, int]:
        return int(self.spn_year.get()), self.cmb_month.current() + 1

    def _on_period_change(self):
        # важно: закрыть возможный редактор в гриде
        if hasattr(self, "grid"):
            self.grid.close_editor(commit=True)
        self._load_existing_rows()

    def _on_department_select(self):
        dep_sel = (self.cmb_department.get() or "Все").strip()
        if set_selected_department_in_config:
            set_selected_department_in_config(dep_sel)

        if dep_sel == "Все":
            names = [e[0] for e in self.employees]
        else:
            names = [e[0] for e in self.employees if len(e) > 3 and (e[3] or "").strip() == dep_sel]

        self.allowed_fio_names = set(names)
        self.cmb_fio.set_completion_list(sorted(set(names)))

        if self.fio_var.get() and self.fio_var.get() not in self.allowed_fio_names:
            self.fio_var.set("")
            self.ent_tbn.delete(0, "end")
            self.pos_var.set("")
        
        self._load_existing_rows()

    def _on_address_select(self, *_):
        self._on_address_change()
        self._load_existing_rows()

    def _on_address_change(self, *_):
        addr = self.cmb_address.get().strip()
    
        objects_for_addr = [
            (code, a, short_name)
            for (code, a, short_name) in getattr(self, "objects_full", [])
            if a == addr
        ]
    
        if not objects_for_addr:
            self.cmb_object_id.config(state="normal", values=[])
            self.cmb_object_id.set("")
            return
    
        ids = sorted({code for (code, a, short_name) in objects_for_addr if code})
        current_oid = (self.cmb_object_id.get() or "").strip()
        self.cmb_object_id.config(state="readonly", values=ids)
        if current_oid and current_oid in ids:
            return
        if len(ids) == 1:
            self.cmb_object_id.set(ids[0])
            return

        if getattr(self, "_suppress_object_id_dialog", False):
            self.cmb_object_id.set("")
            return

        dlg = SelectObjectIdDialog(self, objects_for_addr, addr)
        self.wait_window(dlg)
    
        selected_id = dlg.result
        if selected_id and selected_id in ids:
            self.cmb_object_id.set(selected_id)
        else:
            self.cmb_object_id.set("")

    def _on_fio_select(self, *_):
        fio = self.fio_var.get().strip()
        tbn, pos = self.emp_info.get(fio, ("", ""))
        self.ent_tbn.delete(0, "end")
        self.ent_tbn.insert(0, tbn)
        self.pos_var.set(pos)

    def _clear_filter(self):
        try:
            self.var_filter.set("")
        except Exception:
            pass
        try:
            self.var_brigadier.set("Все")
        except Exception:
            pass
        self._apply_filter()
    
    def _parse_selected_brigadier_tbn(self) -> str | None:
        s = (self.var_brigadier.get() or "Все").strip()
        if s == "Все":
            return None
        if s == "Без бригадира":
            return ""
        # формат "ФИО (TBN)"
        if s.endswith(")") and "(" in s:
            tbn = s[s.rfind("(") + 1 : -1].strip()
            return tbn or None
        return None  
    
    def _apply_filter(self):
        self._filter_job = None
        q = (self.var_filter.get() or "").strip().lower()
        brig_tbn_sel = self._parse_selected_brigadier_tbn()
    
        res: List[Dict[str, Any]] = []
        for rec in self.model_rows_all:
            fio = (rec.get("fio") or "")
            tbn = (rec.get("tbn") or "")
            fio_l = fio.lower()
            tbn_l = tbn.lower()
    
            # --- фильтр по бригадиру ---
            if brig_tbn_sel is not None:
                assigned_brig_tbn = self._brig_assign.get((tbn or "").strip())
                if brig_tbn_sel == "":
                    if assigned_brig_tbn:
                        continue
                else:
                    if (assigned_brig_tbn or "").strip() != brig_tbn_sel:
                        continue
            if q:
                if q not in fio_l and q not in tbn_l:
                    continue
    
            res.append(rec)
    
        self.model_rows = res

        try:
            self.grid.set_selected_indices(set())
        except Exception:
            pass
    
        self.grid.set_rows(self.model_rows)

        self._recalc_object_total()

    def _recalc_row_totals_for_rec(self, rec: Dict[str, Any]):
        y, m = self.get_year_month()
        hours_list = rec.get("hours") or [None] * 31
        rec["_totals"] = calc_row_totals(hours_list, y, m)
    
    def _on_cell_changed(self, row_index: int, day_index: int):
        if 0 <= row_index < len(self.model_rows):
            rec = self.model_rows[row_index]
            self._recalc_row_totals_for_rec(rec)
    
        # перерисовать (видимые строки)
        try:
            self.grid.refresh()
        except Exception:
            pass
    
        # общий итог — по всем строкам
        self._recalc_object_total()
        self._schedule_auto_save()    

    # ---------------- totals / change ----------------
    def _recalc_all_row_totals(self):
        y, m = self.get_year_month()
        for rec in self.model_rows_all:
            hours_list = rec.get("hours") or [None] * 31
            rec["_totals"] = calc_row_totals(hours_list, y, m)

    def _recalc_object_total(self):
        tot_h, tot_d, tot_night, tot_ot_day, tot_ot_night = 0.0, 0, 0.0, 0.0, 0.0
    
        y, m = self.get_year_month()
        days_in_m = month_days(y, m)
    
        for rec in self.model_rows_all:
            hours_list = rec.get("hours") or []
            for i, raw in enumerate(hours_list):
                if i >= days_in_m:
                    continue
                if not raw:
                    continue
    
                hv, night = parse_hours_and_night(raw)
                d_ot, n_ot = parse_overtime(raw)
    
                if isinstance(hv, (int, float)) and hv > 1e-12:
                    tot_h += float(hv)
                    tot_d += 1
                if isinstance(night, (int, float)):
                    tot_night += float(night)
                if isinstance(d_ot, (int, float)):
                    tot_ot_day += float(d_ot)
                if isinstance(n_ot, (int, float)):
                    tot_ot_night += float(n_ot)
    
        sh = f"{tot_h:.2f}".rstrip("0").rstrip(".")
        sn = f"{tot_night:.2f}".rstrip("0").rstrip(".")
        sod = f"{tot_ot_day:.2f}".rstrip("0").rstrip(".")
        son = f"{tot_ot_night:.2f}".rstrip("0").rstrip(".")
        cnt = len(self.model_rows_all)
    
        self.lbl_object_total.config(
            text=f"Сумма: сотрудников {cnt} | дней {tot_d} | часов {sh} | "
                 f"в т.ч. ночных {sn} | пер.день {sod} | пер.ночь {son}"
        )

    def _grid_delete_row(self, row_index: int):
        if self.read_only:
            return
        if not (0 <= row_index < len(self.model_rows)):
            return
    
        rec = self.model_rows[row_index]
        try:
            self.model_rows_all.remove(rec)
        except ValueError:
            return
    
        self._recalc_all_row_totals()
        self._apply_filter()
        self._schedule_auto_save()

    # ---------------- operations buttons ----------------

    def _reload_brigadier_filter_data(self):
        dep = (self.cmb_department.get() or "").strip()
        if not dep or dep == "Все":
            self._brig_assign = {}
            self._brig_names = {}
            try:
                self.cmb_brigadier.configure(values=["Все"])
                self.var_brigadier.set("Все")
            except Exception:
                pass
            return
    
        try:
            self._brig_assign = load_brigadier_assignments_for_department(dep)
            self._brig_names = load_brigadier_names_for_department(dep)
        except Exception:
            # не ломаем табель из-за фильтра
            self._brig_assign = {}
            self._brig_names = {}
            try:
                self.cmb_brigadier.configure(values=["Все"])
                self.var_brigadier.set("Все")
            except Exception:
                pass
            return

        options = ["Все", "Без бригадира"]

        pairs = []
        for tbn, fio in self._brig_names.items():
            fio = (fio or "").strip()
            pairs.append((fio.lower(), f"{fio} ({tbn})"))
    
        for _k, label in sorted(pairs):
            options.append(label)
    
        cur = (self.var_brigadier.get() or "Все").strip()
        self.cmb_brigadier.configure(values=options)
        if cur not in options:
            self.var_brigadier.set("Все")

    def add_row(self):
        if self.read_only:
            return
    
        fio = self.fio_var.get().strip()
        tbn = self.ent_tbn.get().strip()
    
        if not fio:
            messagebox.showwarning("Объектный табель", "Выберите ФИО.")
            return
    
        if fio not in getattr(self, "allowed_fio_names", set()):
            messagebox.showwarning(
                "Объектный табель",
                "Такого сотрудника нет в списке сотрудников текущего подразделения.\n"
                "Добавление в табель запрещено."
            )
            return
    
        key = (fio.strip().lower(), tbn.strip())
        existing = {(r["fio"].strip().lower(), (r.get("tbn") or "").strip()) for r in self.model_rows_all}
        if key in existing:
            if not messagebox.askyesno(
                "Дублирование",
                f"Сотрудник уже есть в табеле:\n{fio} (Таб.№ {tbn}).\nДобавить ещё одну строку?"
            ):
                return
    
        self.model_rows_all.append({"fio": fio, "tbn": tbn, "hours": [None] * 31})
    
        self._recalc_all_row_totals()
        self._apply_filter()
        self._schedule_auto_save()

    def add_department_all(self):
        if self.read_only:
            return
    
        dep_sel = (self.cmb_department.get() or "Все").strip()
    
        if dep_sel == "Все":
            candidates = self.employees[:]
            if not candidates:
                messagebox.showinfo("Объектный табель", "Справочник сотрудников пуст.")
                return
            if not messagebox.askyesno("Добавить всех", f"Добавить в табель всех сотрудников ({len(candidates)})?"):
                return
        else:
            candidates = [e for e in self.employees if len(e) > 3 and (e[3] or "").strip() == dep_sel]
            if not candidates:
                messagebox.showinfo("Объектный табель", f"В подразделении «{dep_sel}» нет сотрудников.")
                return
    
        existing = {(r["fio"].strip().lower(), (r.get("tbn") or "").strip()) for r in self.model_rows_all}
        added_count = 0
    
        dlg = BatchAddDialog(self, total=len(candidates), title="Добавление сотрудников")
    
        def process_batch():
            nonlocal added_count
            for fio, tbn, _, _ in candidates:
                if dlg.cancelled:
                    break
                key = (fio.strip().lower(), (tbn or "").strip())
                if key not in existing:
                    self.model_rows_all.append({"fio": fio, "tbn": tbn, "hours": [None] * 31})
                    existing.add(key)
                    added_count += 1
                dlg.step()
    
            dlg.close()
    
            self._recalc_all_row_totals()
            self._apply_filter()
            self._schedule_auto_save()
    
            if added_count > 0:
                messagebox.showinfo("Объектный табель", f"Добавлено новых сотрудников: {added_count}")
            else:
                messagebox.showinfo("Объектный табель", "Все сотрудники из этого подразделения уже в списке.")
    
        self.after(50, process_batch)

    def add_department_partial(self):
        if self.read_only:
            return
    
        dep_sel = (self.cmb_department.get() or "Все").strip()
        if not self.employees:
            messagebox.showinfo("Объектный табель", "Справочник сотрудников пуст.")
            return
    
        dlg = SelectEmployeesDialog(self, self.employees, dep_sel)
        self.wait_window(dlg)
    
        if dlg.result is None:
            return
        selected_emps = dlg.result
        if not selected_emps:
            return
    
        existing = {(r["fio"].strip().lower(), (r.get("tbn") or "").strip()) for r in self.model_rows_all}
        added_count = 0
    
        for fio, tbn, pos, dep in selected_emps:
            key = (fio.strip().lower(), (tbn or "").strip())
            if key in existing:
                continue
            self.model_rows_all.append({"fio": fio, "tbn": tbn, "hours": [None] * 31})
            existing.add(key)
            added_count += 1
    
        self._recalc_all_row_totals()
        self._apply_filter()
        self._schedule_auto_save()
    
        if added_count > 0:
            messagebox.showinfo("Объектный табель", f"Добавлено сотрудников: {added_count}")
        else:
            messagebox.showinfo("Объектный табель", "Все выбранные сотрудники уже есть в табеле.")

    def clear_selection(self):
        if hasattr(self, "grid"):
            self.grid.set_selected_indices(set())

    def fill_time_selected(self):
        if self.read_only:
            return
        if not self.model_rows:
            messagebox.showinfo("Время для выделенных", "Список сотрудников пуст.")
            return

        selected = self._grid_selected()
        if not selected:
            messagebox.showinfo("Время для выделенных", "Не выбрано ни одного сотрудника.")
            return

        y, m = self.get_year_month()
        max_day = month_days(y, m)

        dlg = TimeForSelectedDialog(self, max_day)
        if dlg.result is None:
            return

        day_from = dlg.result["from"]
        day_to = dlg.result["to"]
        value_str = dlg.result["value"]  # None => clear

        for idx in sorted(selected):
            if not (0 <= idx < len(self.model_rows)):
                continue
            rec = self.model_rows[idx]
            hours_list = rec.get("hours") or [None] * 31
            if len(hours_list) < 31:
                hours_list = (hours_list + [None] * 31)[:31]

            for d in range(day_from, day_to + 1):
                hours_list[d - 1] = value_str
            rec["hours"] = hours_list
        
        self._recalc_all_row_totals()
        self._apply_filter()
        self._schedule_auto_save()

        msg_val = "очищены" if value_str is None else f"установлены в '{value_str}'"
        msg_days = f"для дня {day_from}" if day_from == day_to else f"для дней {day_from}–{day_to}"
        messagebox.showinfo(
            "Время для выделенных",
            f"Значения {msg_val} {msg_days} у {len(selected)} выделенных сотрудников.",
        )

    def fill_hours_all(self):
        if self.read_only:
            return
        if not self.model_rows_all:
            messagebox.showinfo("Проставить часы", "Список сотрудников пуст.")
            return
    
        y, m = self.get_year_month()
        max_day = month_days(y, m)
    
        dlg = HoursFillDialog(self, max_day)
        if not dlg.result:
            return
    
        day = dlg.result["day"]
        if not (1 <= day <= max_day):
            messagebox.showwarning("Проставить часы", f"В этом месяце нет дня №{day}.")
            return
    
        day_idx = day - 1
        is_clear = dlg.result.get("clear", False)
    
        hours_val_str = None
        if not is_clear:
            hours_val_float = float(dlg.result["hours"])
            if hours_val_float > 1e-12:
                hours_val_str = f"{hours_val_float:.2f}".rstrip("0").rstrip(".").replace(".", ",")
    
        for rec in self.model_rows_all:
            hours = rec.get("hours") or [None] * 31
            if len(hours) < 31:
                hours = (hours + [None] * 31)[:31]
            hours[day_idx] = hours_val_str
            rec["hours"] = hours
    
        self._recalc_all_row_totals()
        self._apply_filter()
        self._schedule_auto_save()
    
        if is_clear:
            messagebox.showinfo("Проставить часы", f"День {day} очищен у всех сотрудников.")
        else:
            messagebox.showinfo("Проставить часы", f"Часы '{hours_val_str}' проставлены в день {day} всем сотрудникам.")

    def clear_all_rows(self):
        if self.read_only or not self.model_rows_all:
            return
        if not messagebox.askyesno(
            "Очистка табеля",
            "Вы уверены, что хотите очистить все часы у всех сотрудников?\n\nСами сотрудники останутся в списке."
        ):
            return
    
        for rec in self.model_rows_all:
            rec["hours"] = [None] * 31
    
        self._recalc_all_row_totals()
        self._apply_filter()
        self._schedule_auto_save()
        messagebox.showinfo("Очистка", "Все часы были стерты.")

    def import_from_skud(self):
        if self.read_only:
            return
    
        current_dep = (self.cmb_department.get() or "").strip()
        if current_dep == "Все":
            messagebox.showwarning("СКУД", "Выберите конкретное подразделение (не 'Все').")
            return
    
        # дата
        y, m = self.get_year_month()
        dlg_date = SelectDateDialog(self, init_date=date(y, m, 1))
        d = dlg_date.result
        if d is None:
            return
        d = dlg_date.result
    
        # файл
        from tkinter import filedialog
        path = filedialog.askopenfilename(
            parent=self,
            title="Выберите Excel-отчёт СКУД",
            filetypes=[("Excel", "*.xlsx"), ("Все", "*.*")]
        )
        if not path:
            return
    
        try:
            events = _read_skud_events_from_xlsx(path)
    
            summary_by_skud_fio, problems = _compute_day_summary_from_events(events, target_date=d)
    
            if not summary_by_skud_fio and not problems:
                messagebox.showinfo("СКУД", "В отчёте нет событий с ФИО на выбранную дату.")
                return
    
            candidates = list(self.allowed_fio_names) if getattr(self, "allowed_fio_names", None) else self.emp_names
    
            mapping_rows: List[Dict[str, Any]] = []
            for skud_fio, info in sorted(summary_by_skud_fio.items(), key=lambda x: x[0].lower()):
                best, score = _best_fio_match_with_score(skud_fio, candidates)
    
                hours_val = info.get("hours_rounded")

                apply_default = bool(best) and (score >= 0.90) and isinstance(hours_val, int) and hours_val > 0
    
                mapping_rows.append({
                    "skud_fio": skud_fio,
                    "matched_fio": best or "",
                    "score": score,
                    "hours_rounded": hours_val,
                    "minutes": info.get("minutes"),
                    "first_in": info.get("first_in"),
                    "last_out": info.get("last_out"),
                    "count_in": info.get("count_in"),
                    "count_out": info.get("count_out"),
                    "apply": apply_default,
                })
    
            dlg = SkudMappingReviewDialog(self, rows=mapping_rows, problems=problems)
            self.wait_window(dlg)
    
            if not dlg.result or not dlg.result.get("apply"):
                return
    
            chosen = dlg.result.get("rows") or []
            if not chosen:
                messagebox.showinfo("СКУД", "Ничего не выбрано для применения.")
                return
    
            # применяем
            day_idx = max(0, min(30, d.day - 1))
    
            idx_by_fio: Dict[str, Dict[str, Any]] = {}
            for rec in self.model_rows_all:
                fio = (rec.get("fio") or "").strip()
                if fio:
                    idx_by_fio[fio] = rec
    
            applied = 0
            added = 0
            skipped = 0
    
            for r in chosen:
                matched_fio = (r.get("matched_fio") or "").strip()
                if not matched_fio:
                    skipped += 1
                    continue
    
                hours_val = r.get("hours_rounded")
                if not isinstance(hours_val, int) or hours_val <= 0:
                    skipped += 1
                    continue
    
                rec = idx_by_fio.get(matched_fio)
                if rec is None:
                    tbn, _pos = self.emp_info.get(matched_fio, ("", ""))
                    rec = {"fio": matched_fio, "tbn": tbn or "", "hours": [None] * 31}
                    self.model_rows_all.append(rec)
                    idx_by_fio[matched_fio] = rec
                    added += 1
    
                hours = rec.get("hours") or [None] * 31
                if len(hours) < 31:
                    hours = (hours + [None] * 31)[:31]
    
                # ставим целое число часов строкой
                hours[day_idx] = str(hours_val)
                rec["hours"] = hours
    
                applied += 1
    
            self._recalc_all_row_totals()
            self._apply_filter()
            self._schedule_auto_save()
    
            messagebox.showinfo(
                "СКУД",
                f"Готово.\n"
                f"Применено строк: {applied}\n"
                f"Добавлено сотрудников: {added}\n"
                f"Проблем (нет входа/выхода): {len(problems)}\n"
                f"Пропущено строк: {skipped}",
            )
    
        except Exception as e:
            try:
                logging.exception("Ошибка импорта СКУД")
            except Exception:
                pass
            messagebox.showerror("СКУД", f"Ошибка при загрузке СКУД:\n{e}")

    def import_from_excel(self):
        if self.read_only:
            return
        from tkinter import filedialog

        addr, oid = self.cmb_address.get().strip(), self.cmb_object_id.get().strip()
        y, m = self.get_year_month()
        current_dep = self.cmb_department.get().strip()
        if not addr and not oid:
            messagebox.showwarning("Импорт", "Укажите адрес/ID объекта и период.")
            return

        path = filedialog.askopenfilename(
            parent=self,
            title="Выберите Excel-файл табеля",
            filetypes=[("Excel", "*.xlsx")],
        )
        if not path:
            return

        try:
            wb = load_workbook(path, data_only=True)
            ws = self._ensure_sheet(wb)
            imported: List[Dict[str, Any]] = []

            for r in range(2, ws.max_row + 1):
                if int(ws.cell(r, 3).value or 0) != m or int(ws.cell(r, 4).value or 0) != y:
                    continue
                if oid:
                    if (ws.cell(r, 1).value or "") != oid:
                        continue
                elif (ws.cell(r, 2).value or "") != addr:
                    continue
                if current_dep != "Все" and (ws.cell(r, 7).value or "") != current_dep:
                    continue

                fio = str(ws.cell(r, 5).value or "").strip()
                if not fio:
                    continue

                hours_raw = [
                    str(ws.cell(r, c).value or "").strip() or None
                    for c in range(8, 8 + 31)
                ]
                imported.append({"fio": fio, "tbn": str(ws.cell(r, 6).value or "").strip(), "hours": hours_raw})

            if not imported:
                messagebox.showinfo("Импорт", "Подходящих строк не найдено.")
                return

            uniq: Dict[tuple, Dict[str, Any]] = {}
            for rec in imported:
                uniq[(rec["fio"].lower(), rec["tbn"])] = rec
            imported = list(uniq.values())

            replace_mode = messagebox.askyesno("Импорт", "Заменить текущий список?") if self.model_rows_all else True

            if replace_mode:
                self.model_rows_all.clear()
                self.clear_selection()
            
            existing = {(r["fio"].lower(), r.get("tbn", "")) for r in self.model_rows_all}
            added = 0
            for rec in imported:
                if (rec["fio"].lower(), rec["tbn"]) not in existing:
                    self.model_rows_all.append(rec)
                    added += 1
            
            self._recalc_all_row_totals()
            self._apply_filter()
            self._schedule_auto_save()
            messagebox.showinfo("Импорт", f"Импортировано {added} новых сотрудников.")


        except Exception as e:
            messagebox.showerror("Импорт", f"Ошибка чтения файла:\n{e}")

    def copy_from_month(self):
        if self.read_only:
            return
    
        addr, oid = self.cmb_address.get().strip(), self.cmb_object_id.get().strip()
        current_dep = (self.cmb_department.get() or "").strip()
    
        if not addr and not oid:
            messagebox.showwarning("Копирование", "Укажите адрес/ID объекта.")
            return
    
        if current_dep == "Все":
            messagebox.showwarning("Копирование", "Выберите конкретное подразделение (не 'Все').")
            return
    
        # user_id (важно: копирование логично делать из табеля того же пользователя)
        user_id = self.owner_user_id
        if user_id is None and hasattr(self, "app_ref") and getattr(self.app_ref, "current_user", None):
            user_id = (self.app_ref.current_user or {}).get("id")
    
        if not user_id:
            messagebox.showerror("Копирование", "Не удалось определить пользователя.")
            return
    
        cy, cm = self.get_year_month()
        dlg = CopyFromDialog(self, init_year=cy if cm > 1 else cy - 1, init_month=cm - 1 if cm > 1 else 12)
        if not dlg.result:
            return
    
        src_y = dlg.result["year"]
        src_m = dlg.result["month"]
        with_hours = dlg.result["with_hours"]
        mode = dlg.result["mode"]
    
        try:
            found_rows = load_timesheet_rows_for_copy_from_db(
                object_id=oid or None,
                object_addr=addr,
                department=current_dep,
                year=src_y,
                month=src_m,
                user_id=user_id,
                with_hours=with_hours,
            )
    
            if not found_rows:
                messagebox.showinfo(
                    "Копирование",
                    "В БД не найден табель-источник для выбранного месяца/объекта/подразделения.\n"
                    "Проверьте, что в прошлом месяце табель был сохранён в БД.",
                )
                return

            uniq = {}
            for rec in found_rows:
                fio = (rec.get("fio") or "").strip()
                tbn = (rec.get("tbn") or "").strip()
                if not fio and not tbn:
                    continue
                uniq[(fio.lower(), tbn)] = rec
            found_uniq = list(uniq.values())
    
            if mode == "replace":
                self.model_rows_all.clear()
                self.clear_selection()
    
            existing = {(r["fio"].lower(), (r.get("tbn") or "").strip()) for r in self.model_rows_all}
            added = 0
            for rec in found_uniq:
                key = (rec["fio"].lower(), (rec.get("tbn") or "").strip())
                if key in existing:
                    continue
                self.model_rows_all.append({
                    "fio": rec["fio"],
                    "tbn": rec.get("tbn") or "",
                    "hours": rec.get("hours") or ([None] * 31),
                })
                existing.add(key)
                added += 1
    
            self._recalc_all_row_totals()
            self._apply_filter()
            self._schedule_auto_save()
            messagebox.showinfo("Копирование", f"Скопировано {added} сотрудников.")
    
        except Exception as e:
            try:
                logging.exception("Ошибка при копировании из месяца (БД)")
            except Exception:
                pass
            messagebox.showerror("Копирование", f"Ошибка при копировании из БД:\n{e}")

    # ---------------- saving / excel sheet ----------------

    def _current_file_path(self) -> Optional[Path]:
        addr = self.cmb_address.get().strip()
        oid = self.cmb_object_id.get().strip()
        dep = self.cmb_department.get().strip()
        if not addr and not oid:
            return None
        y, m = self.get_year_month()
        id_part = oid if oid else safe_filename(addr)
        dep_part = safe_filename(dep) if dep and dep != "Все" else "ВсеПодразделения"
        return self.out_dir / f"Объектный_табель_{id_part}_{dep_part}_{y}_{m:02d}.xlsx"

    def _file_path_for(
        self,
        year: int,
        month: int,
        addr: Optional[str] = None,
        oid: Optional[str] = None,
        department: Optional[str] = None,
    ) -> Optional[Path]:
        addr = (addr if addr is not None else self.cmb_address.get().strip())
        oid = (oid if oid is not None else self.cmb_object_id.get().strip())
        dep = (department if department is not None else self.cmb_department.get().strip())
        if not addr and not oid:
            return None
        id_part = oid if oid else safe_filename(addr)
        dep_part = safe_filename(dep) if dep and dep != "Все" else "ВсеПодразделения"
        return self.out_dir / f"Объектный_табель_{id_part}_{dep_part}_{year}_{month:02d}.xlsx"

    def _ensure_sheet(self, wb) -> Any:
        if "Табель" in wb.sheetnames:
            ws = wb["Табель"]
            hdr_first = str(ws.cell(1, 1).value or "")
            if hdr_first == "ID объекта":
                return ws
            base, i = "Табель_OLD", 1
            new_name = base
            while new_name in wb.sheetnames:
                i += 1
                new_name = f"{base}{i}"
            ws.title = new_name

        ws2 = wb.create_sheet("Табель")
        hdr = (
            ["ID объекта", "Адрес", "Месяц", "Год", "ФИО", "Табельный №", "Подразделение"]
            + [str(i) for i in range(1, 32)]
            + ["Итого дней", "Итого часов по табелю", "В т.ч. ночных", "Переработка день", "Переработка ночь"]
        )
        ws2.append(hdr)
        ws2.column_dimensions["A"].width = 14
        ws2.column_dimensions["B"].width = 40
        ws2.column_dimensions["C"].width = 10
        ws2.column_dimensions["D"].width = 8
        ws2.column_dimensions["E"].width = 28
        ws2.column_dimensions["F"].width = 14
        ws2.column_dimensions["G"].width = 20
        for i in range(8, 8 + 31):
            ws2.column_dimensions[get_column_letter(i)].width = 6
        ws2.column_dimensions[get_column_letter(39)].width = 10
        ws2.column_dimensions[get_column_letter(40)].width = 18
        ws2.column_dimensions[get_column_letter(41)].width = 16
        ws2.column_dimensions[get_column_letter(42)].width = 14
        ws2.column_dimensions[get_column_letter(43)].width = 14
        ws2.freeze_panes = "A2"
        return ws2

    def _load_existing_rows(self):
        # сброс
        self.model_rows_all.clear()
        self.model_rows = self.model_rows_all
        try:
            self.grid.set_selected_indices(set())
        except Exception:
            pass
    
        addr, oid = self.cmb_address.get().strip(), self.cmb_object_id.get().strip()
        y, m = self.get_year_month()
        current_dep = (self.cmb_department.get() or "").strip()
    
        if current_dep == "Все":
            self.grid.set_rows(self.model_rows)
            self._recalc_object_total()
            return
    
        user_id = self.owner_user_id
        if user_id is None and hasattr(self, "app_ref") and getattr(self.app_ref, "current_user", None):
            user_id = (self.app_ref.current_user or {}).get("id")
    
        if not user_id:
            self.grid.set_rows(self.model_rows)
            self._recalc_object_total()
            return
    
        try:
            self._reload_brigadier_filter_data()
            db_rows = load_timesheet_rows_from_db(oid or None, addr, current_dep, y, m, user_id)
            self.model_rows_all.extend(db_rows)
    
            # totals для всех строк
            self._recalc_all_row_totals()
    
            # применяем фильтр (он сам вызовет grid.set_rows)
            self._apply_filter()
    
        except Exception as e:
            try:
                import logging
                logging.exception("Ошибка загрузки табеля из БД")
            except Exception:
                pass
            messagebox.showerror("Загрузка", f"Не удалось загрузить табель из БД:\n{e}")
            self.grid.set_rows(self.model_rows)
            self._recalc_object_total()


    def _save_all_internal(self, show_messages: bool, is_auto: bool = False):
        if self.read_only:
            if show_messages:
                messagebox.showinfo("Объектный табель", "Сохранение недоступно в режиме просмотра.")
            return

        # закрыть редактор (чтобы коммитнуть активную ячейку)
        if hasattr(self, "grid"):
            self.grid.close_editor(commit=True)

        addr, oid = self.cmb_address.get().strip(), self.cmb_object_id.get().strip()
        y, m = self.get_year_month()
        current_dep = self.cmb_department.get().strip()

        if current_dep == "Все":
            if show_messages:
                messagebox.showwarning("Сохранение", "Для сохранения выберите конкретное подразделение.")
            return

        user_id = self.owner_user_id
        if user_id is None:
            user_id = (self.app_ref.current_user or {}).get("id") if hasattr(self, "app_ref") else None
        if not user_id:
            if show_messages:
                messagebox.showerror("Сохранение", "Не удалось определить пользователя.")
            return

        # normalize address/id
        try:
            self._sync_object_id_values_silent()
        except Exception:
            pass
        addr, oid = self.cmb_address.get().strip(), self.cmb_object_id.get().strip()

        # address validation
        if not addr:
            if show_messages:
                messagebox.showwarning("Сохранение", "Не задан адрес объекта. Выберите адрес из списка.")
            return

        address_options = getattr(self, "address_options", [])
        if address_options and addr not in address_options:
            try:
                self.cmb_object_id.set("")
            except Exception:
                pass
            if show_messages:
                messagebox.showwarning(
                    "Сохранение",
                    "Адрес объекта введён вручную и не найден в справочнике.\nВыберите адрес из списка.",
                )
            return

        objects_for_addr = [
            (code, a, short_name)
            for (code, a, short_name) in getattr(self, "objects_full", [])
            if a == addr
        ]
        ids_for_addr = sorted({code for (code, a, short_name) in objects_for_addr if code})

        if len(ids_for_addr) > 1 and not oid:
            if show_messages:
                messagebox.showwarning(
                    "Сохранение",
                    "По выбранному адресу найдено несколько объектов.\nСначала выберите корректный ID объекта.",
                )
            return

        if oid:
            conn_check = get_db_connection()
            try:
                with conn_check.cursor() as cur:
                    cur.execute(
                        "SELECT address FROM objects WHERE COALESCE(NULLIF(excel_id, ''), '') = %s",
                        (oid,),
                    )
                    row = cur.fetchone()

                if not row:
                    if show_messages:
                        messagebox.showwarning(
                            "Сохранение",
                            f"ID объекта '{oid}' не найден в справочнике объектов.\nВыберите корректный ID.",
                        )
                    return

                real_addr = (row[0] or "").strip()
                if real_addr != addr:
                    if show_messages:
                        messagebox.showwarning(
                            "Сохранение",
                            "Выбранный ID объекта не соответствует адресу.\n"
                            f"ID '{oid}' связан с адресом:\n{real_addr}\n"
                            f"а вы указали адрес:\n{addr}\n\n"
                            "Исправьте адрес или ID.",
                        )
                    return
            except Exception as e:
                try:
                    import logging
                    logging.exception("Ошибка проверки соответствия ID и адреса объекта")
                except Exception:
                    pass
                if show_messages:
                    messagebox.showerror("Сохранение", f"Ошибка при проверке соответствия ID и адреса объекта:\n{e}")
                return
            finally:
                release_db_connection(conn_check)

        # duplicate employees check
        employees_for_check = []
        for rec in self.model_rows_all:
            fio = (rec.get("fio") or "").strip()
            tbn = (rec.get("tbn") or "").strip()
            if fio or tbn:
                employees_for_check.append((fio, tbn))

        try:
            duplicates = find_duplicate_employees_for_timesheet(
                object_id=oid or None,
                object_addr=addr,
                department=current_dep,
                year=y,
                month=m,
                user_id=user_id,
                employees=employees_for_check,
            )
        except Exception as e:
            try:
                import logging
                logging.exception("Ошибка проверки дублей сотрудников между табелями")
            except Exception:
                pass
            if show_messages:
                messagebox.showerror("Сохранение", f"Ошибка при проверке дублей сотрудников:\n{e}")
            return

        if duplicates:
            if show_messages:
                lines = []
                for d in duplicates:
                    emp_fio = d.get("fio") or ""
                    emp_tbn = d.get("tbn") or ""
                    uname = d.get("full_name") or d.get("username") or f"id={d.get('user_id')}"
                    lines.append(f"- {emp_fio} (таб.№ {emp_tbn}) — уже есть в табеле пользователя {uname}")

                msg = (
                    "Найдены сотрудники, которые уже есть в табелях других пользователей "
                    "по этому объекту/подразделению/месяцу:\n\n"
                    + "\n".join(lines)
                    + "\n\nСохранение отменено. Удалите этих сотрудников из табеля."
                )
                messagebox.showwarning("Дубли сотрудников", msg)
            return

        # save DB
        try:
            header_id = upsert_timesheet_header(oid or None, addr, current_dep, y, m, user_id)
            replace_timesheet_rows(header_id, self.model_rows_all)
        except Exception as e:
            try:
                import logging
                logging.exception("Ошибка сохранения табеля в БД")
            except Exception:
                pass
            if show_messages:
                messagebox.showerror("Сохранение", f"Ошибка сохранения в БД:\n{e}")
            return

        # backup to Excel (as before)
        try:
            fpath = self._current_file_path()
            if not fpath:
                if show_messages:
                    messagebox.showinfo("Сохранение", "Сохранено в БД. Локальный файл не создан (нет адреса/ID).")
                if is_auto:
                    self._update_auto_save_label()
                return

            wb = load_workbook(fpath) if fpath.exists() else Workbook()
            if "Sheet" in wb.sheetnames and len(wb.sheetnames) == 1:
                wb.remove(wb.active)

            ws = self._ensure_sheet(wb)

            to_del = [
                r
                for r in range(2, ws.max_row + 1)
                if (ws.cell(r, 1).value or "") == oid
                and (ws.cell(r, 2).value or "") == addr
                and int(ws.cell(r, 3).value or 0) == m
                and int(ws.cell(r, 4).value or 0) == y
                and (ws.cell(r, 7).value or "") == current_dep
            ]
            for r in reversed(to_del):
                ws.delete_rows(r, 1)

            for rec in self.model_rows_all:
                fio, tbn = rec.get("fio") or "", rec.get("tbn") or ""
                hours_list = rec.get("hours") or [None] * 31
                if len(hours_list) < 31:
                    hours_list = (hours_list + [None] * 31)[:31]

                department = current_dep if current_dep != "Все" else ""
                for emp_fio, emp_tbn, emp_pos, emp_dep in self.employees:
                    if emp_fio == fio:
                        if emp_dep:
                            department = emp_dep
                        break

                total_hours, total_days = 0.0, 0
                total_night = 0.0
                total_ot_day, total_ot_night = 0.0, 0.0
                day_values = []

                for raw in hours_list:
                    day_values.append(raw)
                    if not raw:
                        continue
                    hrs, night = parse_hours_and_night(raw)
                    d_ot, n_ot = parse_overtime(raw)

                    if isinstance(hrs, (int, float)) and hrs > 1e-12:
                        total_hours += float(hrs)
                        total_days += 1
                    if isinstance(night, (int, float)):
                        total_night += float(night)
                    if isinstance(d_ot, (int, float)):
                        total_ot_day += float(d_ot)
                    if isinstance(n_ot, (int, float)):
                        total_ot_night += float(n_ot)

                row_values = [oid, addr, m, y, fio, tbn, department] + day_values + [
                    total_days or None,
                    total_hours or None,
                    total_night or None,
                    total_ot_day or None,
                    total_ot_night or None,
                ]
                ws.append(row_values)

            wb.save(fpath)

            if show_messages:
                messagebox.showinfo("Сохранение", f"Табель сохранен в БД и в файл:\n{fpath}")
        except Exception as e:
            try:
                import logging
                logging.exception("Ошибка резервного сохранения в Excel")
            except Exception:
                pass
            if show_messages:
                messagebox.showwarning("Сохранение", f"В БД табель сохранён, но ошибка при записи в Excel:\n{e}")
            if is_auto:
                self._update_auto_save_label()
            return

        if is_auto:
            self._update_auto_save_label()

    def _update_auto_save_label(self):
        try:
            now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            self.lbl_auto_save.config(text=f"Последнее авто‑сохранение: {now}")
        except Exception:
            pass

    def save_all(self):
        self._save_all_internal(show_messages=True, is_auto=False)

    def _content_total_width(self, fio_px: Optional[int] = None) -> int:
        px = self.COLPX.copy()
        if fio_px is not None:
            px["fio"] = fio_px
        return (
            px["fio"]
            + px["tbn"]
            + 31 * px["day"]
            + px["days"]
            + px["hours"] * 3
            + px["del"]
        )

    def _auto_fit_columns(self):
        try:
            viewport = self.grid.body.winfo_width() if hasattr(self, "grid") else 0
        except Exception:
            viewport = 0

        if viewport <= 1:
            self.after(120, self._auto_fit_columns)
            return

        total = self._content_total_width()
        new_fio = self.COLPX["fio"]
        if total < viewport:
            new_fio = min(self.MAX_FIO_PX, self.COLPX["fio"] + (viewport - total))

        if int(new_fio) != int(self.COLPX["fio"]):
            self.COLPX["fio"] = int(new_fio)
            # push widths into grid and rebuild columns/header
            try:
                if hasattr(self, "grid"):
                    self.grid.COLPX = self.COLPX
                    self.grid._build_columns()
                    self.grid._draw_header()
                    self._grid_refresh(rows_changed=True)
            except Exception:
                pass

    def _on_window_configure(self, _evt):
        try:
            self.after_cancel(self._fit_job)
        except Exception:
            pass
        self._fit_job = self.after(150, self._auto_fit_columns)

class MyTimesheetsPage(tk.Frame):
    """
    Реестр табелей текущего пользователя с фильтрацией и экспортом.
    """
    def __init__(self, master, app_ref: "MainApp"):
        super().__init__(master)
        self.app_ref = app_ref
        if get_output_dir_from_config:
            self.out_dir = get_output_dir_from_config()
        else:
            self.out_dir = Path("./output")
        self.out_dir.mkdir(parents=True, exist_ok=True)

        self.tree = None
        self._headers: List[Dict[str, Any]] = []

        # Переменные для хранения значений фильтров
        self.var_year = tk.StringVar()
        self.var_month = tk.StringVar()
        self.var_dep = tk.StringVar()
        self.var_obj_addr = tk.StringVar()

        self._build_ui()
        self._load_data()

    def _build_ui(self):
        # --- Верхняя панель с фильтрами ---
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=(8, 4))

        tk.Label(top, text="Мои табели", font=("Segoe UI", 12, "bold")).grid(
            row=0, column=0, columnspan=6, sticky="w", pady=(0, 8)
        )

        row_f = 1
        tk.Label(top, text="Год:").grid(row=row_f, column=0, sticky="e", padx=(0, 4))
        spn_year = tk.Spinbox(top, from_=2000, to=2100, width=6, textvariable=self.var_year)
        spn_year.grid(row=row_f, column=1, sticky="w")
        self.var_year.set(str(datetime.now().year))

        tk.Label(top, text="Месяц:").grid(row=row_f, column=2, sticky="e", padx=(12, 4))
        cmb_month = ttk.Combobox(
            top, state="readonly", width=12, textvariable=self.var_month,
            values=["Все"] + [month_name_ru(i) for i in range(1, 13)],
        )
        cmb_month.grid(row=row_f, column=3, sticky="w")
        self.var_month.set("Все")

        row_f += 1
        tk.Label(top, text="Подразделение:").grid(row=row_f, column=0, sticky="e", padx=(0, 4), pady=(4, 0))
        ent_dep = ttk.Entry(top, width=24, textvariable=self.var_dep)
        ent_dep.grid(row=row_f, column=1, sticky="w", pady=(4, 0))

        tk.Label(top, text="Объект (адрес):").grid(row=row_f, column=2, sticky="e", padx=(12, 4), pady=(4, 0))
        ent_addr = ttk.Entry(top, width=34, textvariable=self.var_obj_addr)
        ent_addr.grid(row=row_f, column=3, sticky="w", pady=(4, 0))

        # Панель с кнопками
        btns_frame = tk.Frame(top)
        btns_frame.grid(row=row_f + 1, column=0, columnspan=6, sticky="w", pady=(8, 0))
        ttk.Button(btns_frame, text="Применить фильтр", command=self._load_data).pack(side="left", padx=(0, 4))
        ttk.Button(btns_frame, text="Сбросить", command=self._reset_filters).pack(side="left", padx=4)
        ttk.Button(btns_frame, text="Выгрузить в Excel", command=self._export_to_excel).pack(side="left", padx=4)

        # --- Таблица ---
        frame = tk.Frame(self)
        frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        cols = ("year", "month", "object", "department", "updated_at")
        self.tree = ttk.Treeview(frame, columns=cols, show="headings", selectmode="browse")

        self.tree.heading("year", text="Год")
        self.tree.heading("month", text="Месяц")
        self.tree.heading("object", text="Объект")
        self.tree.heading("department", text="Подразделение")
        self.tree.heading("updated_at", text="Обновлён")

        self.tree.column("year", width=80, anchor="center", stretch=False)
        self.tree.column("month", width=100, anchor="center", stretch=False)
        self.tree.column("object", width=350, anchor="w")
        self.tree.column("department", width=200, anchor="w")
        self.tree.column("updated_at", width=150, anchor="center", stretch=False)

        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.bind("<Double-1>", self._on_open)
        self.tree.bind("<Return>", self._on_open)

        # --- Нижняя панель ---
        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=8, pady=(0, 8))
        tk.Label(
            bottom, text="Двойной щелчок или Enter — открыть табель для редактирования.",
            font=("Segoe UI", 9), fg="#555"
        ).pack(side="left")

    def _reset_filters(self):
        """Сбрасывает все фильтры и перезагружает данные."""
        self.var_year.set(str(datetime.now().year))
        self.var_month.set("Все")
        self.var_dep.set("")
        self.var_obj_addr.set("")
        self._load_data()

    def _load_data(self):
        """Загружает данные из БД с учетом фильтров."""
        self.tree.delete(*self.tree.get_children())
        self._headers.clear()

        user = getattr(self.app_ref, "current_user", None)
        user_id = (user or {}).get("id")
        if not user_id:
            messagebox.showwarning("Мои табели", "Не определён текущий пользователь.")
            return

        # Считываем значения фильтров
        try:
            year = int(self.var_year.get().strip()) if self.var_year.get().strip() else None
        except ValueError:
            year = None

        month_name = self.var_month.get().strip()
        month = None
        if month_name and month_name != "Все":
            month = [month_name_ru(i) for i in range(1, 13)].index(month_name) + 1

        department = self.var_dep.get().strip() or None
        addr_substr = self.var_obj_addr.get().strip() or None

        try:
            # Используем обновленную функцию
            headers = load_user_timesheet_headers(user_id, year, month, department, addr_substr)
        except Exception as e:
            # Используем logging, если он доступен
            try: import logging; logging.exception("Ошибка загрузки 'Моих табелей'")
            except ImportError: pass
            messagebox.showerror("Мои табели", f"Ошибка загрузки списка табелей из БД:\n{e}")
            return

        self._headers = headers

        for h in headers:
            month_ru = month_name_ru(h["month"]) if 1 <= h["month"] <= 12 else str(h["month"])
            obj_display = h["object_addr"] or ""
            if h.get("object_id"):
                obj_display = f"[{h['object_id']}] {obj_display}"

            upd_str = h["updated_at"].strftime("%d.%m.%Y %H:%M") if isinstance(h.get("updated_at"), datetime) else ""
            
            self.tree.insert("", "end", iid=str(h["id"]), values=(
                h["year"], month_ru, obj_display, h.get("department") or "", upd_str
            ))

    def _export_to_excel(self):
        """Выгружает отфильтрованный список табелей в Excel."""
        if not self._headers:
            messagebox.showinfo("Экспорт в Excel", "Нет данных для выгрузки.")
            return

        from tkinter import filedialog
        default_name = f"Мои_табели_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        path = filedialog.asksaveasfilename(
            parent=self, title="Сохранить мои табели в Excel", defaultextension=".xlsx",
            initialfile=default_name, filetypes=[("Excel", "*.xlsx"), ("Все", "*.*")]
        )
        if not path:
            return

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Мои табели"
            
            # Заголовки как в TimesheetRegistryPage
            header = ["Год", "Месяц", "Адрес", "ID объекта", "Подразделение", "ФИО сотрудника", "Табельный №"] + \
                     [str(i) for i in range(1, 32)] + \
                     ["Итого дней", "Итого часов", "В т.ч. ночных", "Переработка день", "Переработка ночь"]
            ws.append(header)
            
            # Настраиваем ширину колонок
            widths = [6, 10, 40, 14, 22, 28, 12] + [6]*31 + [10, 12, 16, 16]
            for i, width in enumerate(widths, 1):
                ws.column_dimensions[get_column_letter(i)].width = width
            
            total_rows = 0
            for h in self._headers:
                rows_data = load_timesheet_rows_by_header_id(h["id"])
                for r in rows_data:
                    excel_row = [
                        h["year"], h["month"], h.get("object_addr", ""), h.get("object_id", ""),
                        h.get("department", ""), r["fio"], r["tbn"]
                    ] + (r.get("hours_raw") or [None]*31) + [
                        r.get("total_days"), r.get("total_hours"), r.get("night_hours"), 
                        r.get("overtime_day"), r.get("overtime_night")
                    ]
                    ws.append(excel_row)
                    total_rows += 1
            
            wb.save(path)
            messagebox.showinfo("Экспорт в Excel", f"Выгрузка завершена.\nСохранено строк: {total_rows}\nФайл: {path}")

        except Exception as e:
            try: import logging; logging.exception("Ошибка экспорта 'Моих табелей'")
            except ImportError: pass
            messagebox.showerror("Экспорт в Excel", f"Ошибка при выгрузке:\n{e}")

    def _get_selected_header(self) -> Optional[Dict[str, Any]]:
        sel = self.tree.selection()
        if not sel: return None
        try:
            hid = int(sel[0])
            return next((h for h in self._headers if h["id"] == hid), None)
        except (ValueError, IndexError):
            return None

    def _on_open(self, event=None):
        h = self._get_selected_header()
        if not h: return

        # Табель из "Моих табелей" всегда открывается для редактирования
        self.app_ref._show_page(
            "timesheet",
            lambda parent: TimesheetPage(
                parent,
                app_ref=self.app_ref,
                init_object_id=h.get("object_id"),
                init_object_addr=h.get("object_addr"),
                init_department=h.get("department"),
                init_year=h.get("year"),
                init_month=h.get("month"),
                read_only=False, # Всегда редактируемый
                owner_user_id=None, # Не требуется, т.к. мы уже знаем, что это табель текущего юзера
            ),
        )

class TimesheetRegistryPage(tk.Frame):
    """
    Реестр табелей всех пользователей (для руководителей/админов).
    """
    def __init__(self, master, app_ref: "MainApp"):
        super().__init__(master)
        self.app_ref = app_ref

        self.tree = None
        self._headers: List[Dict[str, Any]] = []
        self._all_departments: List[str] = []

        self.var_year = tk.StringVar()
        self.var_month = tk.StringVar()
        self.var_dep = tk.StringVar()
        self.var_obj_addr = tk.StringVar()
        self.var_obj_id = tk.StringVar()

        self._filter_job = None  # общий debounce для текстовых полей

        self._build_ui()
        self._load_departments()
        self._load_data()

    def _load_departments(self):
        """Загружает уникальные подразделения из timesheet_headers."""
        self._all_departments = []
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT department 
                    FROM timesheet_headers 
                    WHERE department IS NOT NULL 
                      AND TRIM(department) != ''
                    ORDER BY department
                """)
                self._all_departments = [row[0] for row in cur.fetchall()]
        except Exception:
            logging.exception("Не удалось загрузить подразделения из timesheet_headers")
            self._all_departments = []
        finally:
            if conn:
                release_db_connection(conn)
    
        values = ["Все"] + self._all_departments
        self._cmb_dep.configure(values=values)
        if not self.var_dep.get() or self.var_dep.get() == "Все":
            self.var_dep.set("Все")


    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=(8, 4))

        tk.Label(top, text="Реестр табелей", font=("Segoe UI", 12, "bold")).grid(
            row=0, column=0, columnspan=6, sticky="w", pady=(0, 4)
        )

        row_f = 1

        tk.Label(top, text="Год:").grid(row=row_f, column=0, sticky="e", padx=(0, 4))
        spn_year = tk.Spinbox(top, from_=2000, to=2100, width=6, textvariable=self.var_year)
        spn_year.grid(row=row_f, column=1, sticky="w")
        self.var_year.set(str(datetime.now().year))  # ← установка ДО trace

        tk.Label(top, text="Месяц:").grid(row=row_f, column=2, sticky="e", padx=(12, 4))
        cmb_month = ttk.Combobox(
            top, state="readonly", width=12, textvariable=self.var_month,
            values=["Все"] + [month_name_ru(i) for i in range(1, 13)],
        )
        cmb_month.grid(row=row_f, column=3, sticky="w")
        self.var_month.set("Все")

        tk.Label(top, text="Подразделение:").grid(row=row_f, column=4, sticky="e", padx=(12, 4))
        self._cmb_dep = ttk.Combobox(
            top, state="readonly", width=24, textvariable=self.var_dep, values=["Все"],
        )
        self._cmb_dep.grid(row=row_f, column=5, sticky="w")
        self.var_dep.set("Все")

        row_f += 1

        tk.Label(top, text="Объект (адрес):").grid(row=row_f, column=0, sticky="e", padx=(0, 4), pady=(4, 0))
        ent_addr = ttk.Entry(top, width=34, textvariable=self.var_obj_addr)
        ent_addr.grid(row=row_f, column=1, columnspan=2, sticky="w", pady=(4, 0))

        tk.Label(top, text="ID объекта:").grid(row=row_f, column=3, sticky="e", padx=(12, 4), pady=(4, 0))
        ent_oid = ttk.Entry(top, width=18, textvariable=self.var_obj_id)
        ent_oid.grid(row=row_f, column=4, sticky="w", pady=(4, 0))

        btns = tk.Frame(top)
        btns.grid(row=row_f, column=5, sticky="e", padx=(8, 0), pady=(4, 0))
        ttk.Button(btns, text="Применить фильтр", command=self._load_data).pack(side="left", padx=2)
        ttk.Button(btns, text="Сброс", command=self._reset_filters).pack(side="left", padx=2)
        ttk.Button(btns, text="Выгрузить в Excel", command=self._export_to_excel).pack(side="left", padx=2)
        ttk.Button(btns, text="Отчёт по заполненности", command=self._export_fill_report).pack(side="left", padx=2)

        self.var_obj_addr.trace_add("write", self._on_text_filter_changed)
        self.var_obj_id.trace_add("write", self._on_text_filter_changed)
        cmb_month.bind("<<ComboboxSelected>>", lambda e: self._load_data())
        self._cmb_dep.bind("<<ComboboxSelected>>", lambda e: self._load_data())
        self.var_year.trace_add("write", self._on_year_changed)

        # ---- Таблица ----
        frame = tk.Frame(self)
        frame.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        cols = ("year", "month", "object", "department", "user", "updated_at")
        self.tree = ttk.Treeview(
            frame,
            columns=cols,
            show="headings",
            selectmode="browse",
        )

        self.tree.heading("year", text="Год")
        self.tree.heading("month", text="Месяц")
        self.tree.heading("object", text="Объект")
        self.tree.heading("department", text="Подразделение")
        self.tree.heading("user", text="Пользователь")
        self.tree.heading("updated_at", text="Обновлён")

        self.tree.column("year", width=60, anchor="center")
        self.tree.column("month", width=80, anchor="center")
        self.tree.column("object", width=280, anchor="w")
        self.tree.column("department", width=160, anchor="w")
        self.tree.column("user", width=180, anchor="w")
        self.tree.column("updated_at", width=140, anchor="center")

        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.bind("<Double-1>", self._on_open)
        self.tree.bind("<Return>", self._on_open)

        # Нижняя панель
        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=8, pady=(0, 8))
        self._lbl_count = tk.Label(
            bottom,
            text="",
            font=("Segoe UI", 9),
            fg="#555",
        )
        self._lbl_count.pack(side="left")
        tk.Label(
            bottom,
            text="Двойной щелчок или Enter — открыть табель.",
            font=("Segoe UI", 9),
            fg="#888",
        ).pack(side="right")

    def _on_text_filter_changed(self, *_args):
        """Debounce для текстовых полей (адрес, ID объекта) — 400 мс."""
        if self._filter_job is not None:
            self.after_cancel(self._filter_job)
        self._filter_job = self.after(400, self._load_data)

    def _on_year_changed(self, *_args):
        """При изменении года — перезагрузка с небольшой задержкой."""
        if self._filter_job is not None:
            self.after_cancel(self._filter_job)
        self._filter_job = self.after(600, self._load_data)

    def _reset_filters(self):
        # Временно отключаем автофильтрацию чтобы не дёргать _load_data многократно
        if self._filter_job is not None:
            self.after_cancel(self._filter_job)
            self._filter_job = None

        self.var_year.set(str(datetime.now().year))
        self.var_month.set("Все")
        self.var_dep.set("Все")
        self.var_obj_addr.set("")
        self.var_obj_id.set("")
        self._load_data()

    def _load_data(self):
        # Сбрасываем pending-задачу
        if self._filter_job is not None:
            self.after_cancel(self._filter_job)
        self._filter_job = None

        self.tree.delete(*self.tree.get_children())
        self._headers.clear()

        # --- Фильтры ---
        year = None
        try:
            y = int(self.var_year.get().strip())
            if 2000 <= y <= 2100:
                year = y
        except Exception:
            pass

        month = None
        m_name = (self.var_month.get() or "").strip()
        if m_name and m_name != "Все":
            for i in range(1, 13):
                if month_name_ru(i) == m_name:
                    month = i
                    break

        dep = self.var_dep.get().strip()
        if not dep or dep == "Все":
            dep = None

        addr_sub = self.var_obj_addr.get().strip() or None
        oid_sub = self.var_obj_id.get().strip() or None

        try:
            headers = load_all_timesheet_headers(
                year=year,
                month=month,
                department=dep,
                object_addr_substr=addr_sub,
                object_id_substr=oid_sub,
            )
        except Exception as e:
            logging.exception("Ошибка загрузки реестра табелей")
            messagebox.showerror("Реестр табелей", f"Ошибка загрузки реестра из БД:\n{e}")
            return

        self._headers = headers

        for h in headers:
            yr = h["year"]
            mn = h["month"]
            addr = h["object_addr"] or ""
            obj_id = h.get("object_id") or ""
            dep_val = h.get("department") or ""
            upd = h.get("updated_at")
            full_name = h.get("full_name") or h.get("username") or ""

            month_ru = month_name_ru(mn) if 1 <= mn <= 12 else str(mn)
            obj_display = addr
            if obj_id:
                obj_display = f"[{obj_id}] {addr}"

            if isinstance(upd, datetime):
                upd_str = upd.strftime("%d.%m.%Y %H:%M")
            else:
                upd_str = str(upd or "")

            iid = str(h["id"])
            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(yr, month_ru, obj_display, dep_val, full_name, upd_str),
            )

        self._lbl_count.config(text=f"Найдено табелей: {len(headers)}")

    def _export_fill_report(self):

        if not self._headers:
            messagebox.showinfo("Отчёт по заполненности", "Нет данных для выгрузки.")
            return

        from tkinter import filedialog
        import calendar

        today = datetime.now().date()

        default_name = f"Заполненность_табелей_{today.strftime('%Y%m%d')}.xlsx"
        path = filedialog.asksaveasfilename(
            parent=self,
            title="Сохранить отчёт по заполненности",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel файлы", "*.xlsx"), ("Все файлы", "*.*")],
        )
        if not path:
            return

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Заполненность"

            # Заголовки
            ws.append([
                "Объект (адрес)",
                "ID объекта",
                "Подразделение",
                "Пользователь",
                "Год",
                "Месяц",
                "Дата обновления",
                "Дней в периоде",
                "Дней заполнено",
                "Заполненность, %",
            ])

            # Ширины столбцов
            ws.column_dimensions["A"].width = 45
            ws.column_dimensions["B"].width = 14
            ws.column_dimensions["C"].width = 24
            ws.column_dimensions["D"].width = 24
            ws.column_dimensions["E"].width = 8
            ws.column_dimensions["F"].width = 12
            ws.column_dimensions["G"].width = 20
            ws.column_dimensions["H"].width = 16
            ws.column_dimensions["I"].width = 16
            ws.column_dimensions["J"].width = 18

            # Стиль процента
            from openpyxl.styles import Font, Alignment, PatternFill
            header_font = Font(bold=True)
            for cell in ws[1]:
                cell.font = header_font

            row_num = 1
            for h in self._headers:
                header_id = int(h["id"])
                yr = int(h["year"])
                mn = int(h["month"])
                addr = h.get("object_addr") or ""
                obj_id = h.get("object_id") or ""
                dep = h.get("department") or ""
                user_display = h.get("full_name") or h.get("username") or ""
                upd = h.get("updated_at")

                if isinstance(upd, datetime):
                    upd_str = upd.strftime("%d.%m.%Y %H:%M")
                else:
                    upd_str = str(upd or "")

                month_ru = month_name_ru(mn) if 1 <= mn <= 12 else str(mn)

                # --- Определяем период: с 1-го числа месяца по min(сегодня, последний день месяца) ---
                last_day_of_month = calendar.monthrange(yr, mn)[1]
                period_end_date = min(today, datetime(yr, mn, last_day_of_month).date())
                period_start_date = datetime(yr, mn, 1).date()

                if period_end_date < period_start_date:
                    # Месяц ещё не начался (будущий) — 0%
                    days_in_period = 0
                    days_filled = 0
                else:
                    days_in_period = (period_end_date - period_start_date).days + 1  # включительно

                    rows = load_timesheet_rows_by_header_id(header_id)

                    days_filled = 0
                    for day_idx in range(days_in_period):  # 0-based
                        day_num = day_idx + 1  # 1..N
                        arr_idx = day_num - 1  # индекс в hours_raw (0..30)
                        has_data = False
                        for row in rows:
                            hours_raw = row.get("hours_raw") or []
                            if arr_idx < len(hours_raw):
                                val = hours_raw[arr_idx]
                                if val is not None and str(val).strip() != "":
                                    has_data = True
                                    break
                        if has_data:
                            days_filled += 1

                if days_in_period > 0:
                    pct = round(days_filled / days_in_period * 100, 1)
                else:
                    pct = 0.0

                ws.append([
                    addr,
                    obj_id,
                    dep,
                    user_display,
                    yr,
                    month_ru,
                    upd_str,
                    days_in_period,
                    days_filled,
                    pct,
                ])
                row_num += 1

            # Условное форматирование (цветовая подсветка процента)
            from openpyxl.styles import PatternFill
            red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
            yellow_fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
            green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")

            for row_idx in range(2, row_num + 2):
                cell = ws.cell(row=row_idx, column=10)
                if cell.value is not None:
                    try:
                        v = float(cell.value)
                        if v < 50:
                            cell.fill = red_fill
                        elif v < 90:
                            cell.fill = yellow_fill
                        else:
                            cell.fill = green_fill
                    except (ValueError, TypeError):
                        pass

            wb.save(path)
            messagebox.showinfo(
                "Отчёт по заполненности",
                f"Отчёт сохранён.\nФайл: {path}\nТабелей: {len(self._headers)}",
                parent=self,
            )
        except Exception as e:
            logging.exception("Ошибка выгрузки отчёта по заполненности")
            messagebox.showerror("Отчёт по заполненности", f"Ошибка:\n{e}", parent=self)

    def _export_to_excel(self):
        """
        Выгружает все табели, показанные в реестре (с учётом фильтров),
        в один Excel-файл.
        """
        if not self._headers:
            messagebox.showinfo("Экспорт в Excel", "Нет данных для выгрузки.")
            return

        from tkinter import filedialog

        default_name = f"Реестр_табелей_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        path = filedialog.asksaveasfilename(
            parent=self,
            title="Сохранить реестр табелей в Excel",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel файлы", "*.xlsx"), ("Все файлы", "*.*")],
        )
        if not path:
            return

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Реестр табелей"

            header_row = [
                "Год",
                "Месяц",
                "Адрес",
                "ID объекта",
                "Подразделение",
                "Пользователь",
                "ФИО",
                "Табельный №",
            ] + [f"{i}" for i in range(1, 32)] + [
                "Итого_дней",
                "Итого_часов",
                "В т.ч. ночных",
                "Переработка_день",
                "Переработка_ночь",
            ]
            ws.append(header_row)

            ws.column_dimensions["A"].width = 6
            ws.column_dimensions["B"].width = 10
            ws.column_dimensions["C"].width = 40
            ws.column_dimensions["D"].width = 14
            ws.column_dimensions["E"].width = 22
            ws.column_dimensions["F"].width = 22
            ws.column_dimensions["G"].width = 28
            ws.column_dimensions["H"].width = 12
            for col_idx in range(9, 9 + 31):
                ws.column_dimensions[get_column_letter(col_idx)].width = 6
            base = 9 + 31
            ws.column_dimensions[get_column_letter(base)].width = 10
            ws.column_dimensions[get_column_letter(base + 1)].width = 14
            ws.column_dimensions[get_column_letter(base + 2)].width = 16
            ws.column_dimensions[get_column_letter(base + 3)].width = 16

            total_rows = 0
            for h in self._headers:
                header_id = int(h["id"])
                year = int(h["year"])
                month = int(h["month"])
                addr = h.get("object_addr") or ""
                obj_id = h.get("object_id") or ""
                dep = h.get("department") or ""
                user_display = h.get("full_name") or h.get("username") or ""

                rows = load_timesheet_rows_by_header_id(header_id)

                for row in rows:
                    fio = row["fio"]
                    tbn = row["tbn"]
                    hours_raw = row.get("hours_raw") or [None] * 31
                    total_days = row.get("total_days")
                    total_hours = row.get("total_hours")
                    night_hours = row.get("night_hours")
                    ot_day = row.get("overtime_day")
                    ot_night = row.get("overtime_night")

                    excel_row = [
                        year, month, addr, obj_id, dep, user_display, fio, tbn,
                    ]
                    for v in hours_raw:
                        excel_row.append(v if v is not None else None)
                    excel_row.append(total_days if total_days is not None else None)
                    excel_row.append(total_hours if total_hours is not None else None)
                    excel_row.append(night_hours if night_hours is not None else None)
                    excel_row.append(ot_day if ot_day is not None else None)
                    excel_row.append(ot_night if ot_night is not None else None)

                    ws.append(excel_row)
                    total_rows += 1

            wb.save(path)
            messagebox.showinfo(
                "Экспорт в Excel",
                f"Выгрузка завершена.\nФайл: {path}\nСтрок табеля: {total_rows}",
                parent=self,
            )
        except Exception as e:
            logging.exception("Ошибка экспорта реестра табелей в Excel")
            messagebox.showerror("Экспорт в Excel", f"Ошибка при выгрузке:\n{e}", parent=self)

    def _get_selected_header(self) -> Optional[Dict[str, Any]]:
        sel = self.tree.selection()
        if not sel:
            return None
        iid = sel[0]
        try:
            hid = int(iid)
        except Exception:
            return None
        for h in self._headers:
            if int(h["id"]) == hid:
                return h
        return None

    def _on_open(self, event=None):
        h = self._get_selected_header()
        if not h:
            return

        object_id = h.get("object_id") or None
        object_addr = h.get("object_addr") or ""
        department = h.get("department") or ""
        year = int(h.get("year") or 0)
        month = int(h.get("month") or 0)

        owner_user_id = h.get("user_id")

        role = (self.app_ref.current_user or {}).get("role") or "specialist"
        read_only = (role != "admin")

        self.app_ref._show_page(
            "timesheet",
            lambda parent: TimesheetPage(
                parent,
                app_ref=self.app_ref,
                init_object_id=object_id,
                init_object_addr=object_addr,
                init_department=department,
                init_year=year,
                init_month=month,
                read_only=read_only,
                owner_user_id=owner_user_id,
            ),
        )

# ------------------------- API для встраивания в main_app -------------------------

def create_timesheet_page(parent, app_ref, **kwargs) -> TimesheetPage:
    """
    Создает страницу 'Создать табель'.
    kwargs передаются напрямую в конструктор TimesheetPage.
    """
    return TimesheetPage(parent, app_ref=app_ref, **kwargs)

def create_my_timesheets_page(parent, app_ref) -> MyTimesheetsPage:
    """Создает страницу 'Мои табели'."""
    return MyTimesheetsPage(parent, app_ref=app_ref)
        
def create_timesheet_registry_page(parent, app_ref) -> TimesheetRegistryPage:
    """Создает страницу 'Реестр табелей'."""
    return TimesheetRegistryPage(parent, app_ref=app_ref)
