import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import logging
import pandas as pd

from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

db_connection_pool: Optional[pool.SimpleConnectionPool] = None


def set_db_pool(db_pool: pool.SimpleConnectionPool):
    global db_connection_pool
    db_connection_pool = db_pool
    logging.info("Analytics Module: DB pool set.")


# ============================================================
#                      DATA PROVIDER
# ============================================================

class AnalyticsData:
    def __init__(self, start_date, end_date, object_type_filter: str):
        self.start_date = start_date
        self.end_date = end_date
        self.object_type_filter = object_type_filter  # objects.short_name

    def _execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        if not db_connection_pool:
            raise ConnectionError("Пул соединений с БД не был инициализирован.")
        conn = None
        try:
            conn = db_connection_pool.getconn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchall()
        except Exception as e:
            logging.exception("Analytics query error")
            messagebox.showerror("Ошибка БД", f"Не удалось получить данные для аналитики:\n{e}")
            return []
        finally:
            if conn:
                db_connection_pool.putconn(conn)

    def get_object_types(self) -> List[str]:
        query = """
        SELECT DISTINCT short_name
        FROM objects
        WHERE short_name IS NOT NULL AND short_name <> ''
        ORDER BY short_name;
        """
        results = self._execute_query(query)
        return [row["short_name"] for row in results]

    # ============================================================
    #                      1. ТРУДОЗАТРАТЫ
    # ============================================================

    def get_labor_kpi(self) -> Dict[str, Any]:
        base_query = """
        SELECT
            COALESCE(SUM(tr.total_hours), 0)                      AS total_hours,
            COALESCE(SUM(tr.total_days), 0)                       AS total_days,
            COALESCE(SUM(tr.overtime_day + tr.overtime_night), 0) AS total_overtime,
            COUNT(DISTINCT COALESCE(NULLIF(tr.tbn,''), tr.fio))   AS unique_people_key
        FROM timesheet_headers th
        JOIN timesheet_rows tr ON th.id = tr.header_id
        {join_clause}
        WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
        {filter_clause};
        """

        start_period = self.start_date.year * 100 + self.start_date.month
        end_period = self.end_date.year * 100 + self.end_date.month
        params: List[Any] = [start_period, end_period]

        join_clause = ""
        filter_clause = ""
        if self.object_type_filter:
            join_clause = "LEFT JOIN objects o ON th.object_db_id = o.id"
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = base_query.format(join_clause=join_clause, filter_clause=filter_clause)
        result = self._execute_query(query, tuple(params))
        row = result[0] if result else {}

        total_hours = float(row.get("total_hours", 0) or 0)
        total_overtime = float(row.get("total_overtime", 0) or 0)
        uniq = int(row.get("unique_people_key", 0) or 0)
        row["total_hours"] = total_hours
        row["total_days"] = float(row.get("total_days", 0) or 0)
        row["total_overtime"] = total_overtime
        row["unique_people"] = uniq
        row["hours_per_person"] = (total_hours / uniq) if uniq > 0 else 0.0
        row["overtime_share_pct"] = (total_overtime / total_hours * 100.0) if total_hours > 0 else 0.0
        return row

    def get_labor_by_object(self) -> pd.DataFrame:
        base_query = """
        SELECT 
            o.address AS object_name,
            SUM(tr.total_hours) AS total_hours
        FROM timesheet_headers th
        JOIN timesheet_rows tr ON th.id = tr.header_id
        LEFT JOIN objects o ON th.object_db_id = o.id
        WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
        {filter_clause}
        GROUP BY o.address
        HAVING o.address IS NOT NULL
        ORDER BY total_hours DESC;
        """
        start_period = self.start_date.year * 100 + self.start_date.month
        end_period = self.end_date.year * 100 + self.end_date.month
        params: List[Any] = [start_period, end_period]

        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = base_query.format(filter_clause=filter_clause)
        data = self._execute_query(query, tuple(params))
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"] = df["total_hours"].astype(float)
        return df

    def get_labor_trend_by_month(self) -> pd.DataFrame:
        base_query = """
        SELECT
            th.year,
            th.month,
            SUM(tr.total_hours) AS total_hours
        FROM timesheet_headers th
        JOIN timesheet_rows tr ON th.id = tr.header_id
        {join_clause}
        WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
        {filter_clause}
        GROUP BY th.year, th.month
        ORDER BY th.year, th.month;
        """
        start_period = self.start_date.year * 100 + self.start_date.month
        end_period = self.end_date.year * 100 + self.end_date.month
        params: List[Any] = [start_period, end_period]

        join_clause = ""
        filter_clause = ""
        if self.object_type_filter:
            join_clause = "LEFT JOIN objects o ON th.object_db_id = o.id"
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = base_query.format(join_clause=join_clause, filter_clause=filter_clause)
        data = self._execute_query(query, tuple(params))
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"] = df["total_hours"].astype(float)
        return df

    def get_top_employees_by_hours(self, limit: int = 10) -> pd.DataFrame:
        base_query = """
        SELECT
            tr.fio,
            COALESCE(SUM(tr.total_hours), 0) AS total_hours,
            COALESCE(SUM(tr.overtime_day + tr.overtime_night), 0) AS total_overtime
        FROM timesheet_headers th
        JOIN timesheet_rows tr ON th.id = tr.header_id
        {join_clause}
        WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
        {filter_clause}
        GROUP BY tr.fio
        ORDER BY total_hours DESC
        LIMIT {limit};
        """

        start_period = self.start_date.year * 100 + self.start_date.month
        end_period = self.end_date.year * 100 + self.end_date.month
        params: List[Any] = [start_period, end_period]

        join_clause = ""
        filter_clause = ""
        if self.object_type_filter:
            join_clause = "LEFT JOIN objects o ON th.object_db_id = o.id"
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = base_query.format(join_clause=join_clause, filter_clause=filter_clause, limit=limit)
        data = self._execute_query(query, tuple(params))
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"] = df["total_hours"].astype(float)
            df["total_overtime"] = df["total_overtime"].astype(float)
        return df

    def get_labor_by_department(self) -> pd.DataFrame:
        """
        Суммарные человеко-часы и количество людей по подразделениям.
        Источник названия:
          1) departments.name по timesheet_headers.department_id
          2) timesheet_headers.department (текст)
          3) '—'
        Людей считаем как DISTINCT по tbn (если есть), иначе по fio.
        """
        base_query = """
        SELECT
            COALESCE(d.name, NULLIF(th.department,''), '—') AS department_name,
            SUM(COALESCE(tr.total_hours,0)) AS total_hours,
            COUNT(DISTINCT COALESCE(NULLIF(tr.tbn,''), tr.fio)) AS people_cnt
        FROM timesheet_headers th
        JOIN timesheet_rows tr ON th.id = tr.header_id
        LEFT JOIN departments d ON th.department_id = d.id
        {join_clause}
        WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
        {filter_clause}
        GROUP BY COALESCE(d.name, NULLIF(th.department,''), '—')
        ORDER BY total_hours DESC;
        """
    
        start_period = self.start_date.year * 100 + self.start_date.month
        end_period = self.end_date.year * 100 + self.end_date.month
        params: List[Any] = [start_period, end_period]
    
        join_clause = ""
        filter_clause = ""
        if self.object_type_filter:
            join_clause = "LEFT JOIN objects o ON th.object_db_id = o.id"
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
    
        query = base_query.format(join_clause=join_clause, filter_clause=filter_clause)
        data = self._execute_query(query, tuple(params))
    
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"] = df["total_hours"].astype(float)
            df["people_cnt"] = df["people_cnt"].astype(int)
            df["department_name"] = df["department_name"].fillna("—")
        return df

    # ============================================================
    #                      2. ТРАНСПОРТ
    # ============================================================

    def get_transport_kpi(self) -> Dict[str, Any]:
        base_query = """
        SELECT
            COALESCE(SUM(tp.hours), 0) AS total_machine_hours,
            COUNT(DISTINCT t.id)      AS total_orders,
            COALESCE(SUM(tp.qty), 0)  AS total_units
        FROM transport_orders t
        JOIN transport_order_positions tp ON t.id = tp.order_id
        LEFT JOIN objects o ON t.object_id = o.id
        WHERE t.date BETWEEN %s AND %s
        {filter_clause};
        """
        params: List[Any] = [self.start_date, self.end_date]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = base_query.format(filter_clause=filter_clause)
        result = self._execute_query(query, tuple(params))
        kpi = result[0] if result else {}

        total_hours = float(kpi.get("total_machine_hours", 0) or 0)
        total_orders = int(kpi.get("total_orders", 0) or 0)
        total_units = float(kpi.get("total_units", 0) or 0)

        kpi["total_machine_hours"] = total_hours
        kpi["total_orders"] = total_orders
        kpi["total_units"] = total_units
        kpi["avg_hours_per_order"] = (total_hours / total_orders) if total_orders > 0 else 0.0
        kpi["hours_per_unit"] = (total_hours / total_units) if total_units > 0 else 0.0
        return kpi

    def get_transport_by_tech(self) -> pd.DataFrame:
        base_query = """
        SELECT
            tp.tech,
            SUM(tp.hours) AS total_hours
        FROM transport_orders t
        JOIN transport_order_positions tp ON t.id = tp.order_id
        LEFT JOIN objects o ON t.object_id = o.id
        WHERE t.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY tp.tech
        ORDER BY total_hours DESC;
        """
        params: List[Any] = [self.start_date, self.end_date]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = base_query.format(filter_clause=filter_clause)
        data = self._execute_query(query, tuple(params))
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"] = df["total_hours"].astype(float)
        return df

    # ============================================================
    #                      3. ПИТАНИЕ
    # ============================================================

    def get_meals_kpi(self) -> Dict[str, Any]:
        base_query = """
        SELECT
            COUNT(moi.id)                   AS total_portions_rows,
            COALESCE(SUM(moi.quantity), 0)  AS total_portions_qty,
            COUNT(DISTINCT mo.id)           AS total_orders,
            COUNT(DISTINCT moi.employee_id) AS unique_employees
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        LEFT JOIN objects o ON mo.object_id = o.id
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause};
        """
        params: List[Any] = [self.start_date, self.end_date]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = base_query.format(filter_clause=filter_clause)
        result = self._execute_query(query, tuple(params))
        row = result[0] if result else {}

        total_qty = float(row.get("total_portions_qty", 0) or 0)
        total_orders = int(row.get("total_orders", 0) or 0)
        unique_emp = int(row.get("unique_employees", 0) or 0)

        row["total_portions_qty"] = total_qty
        row["total_orders"] = total_orders
        row["unique_employees"] = unique_emp
        row["avg_portions_per_order"] = (total_qty / total_orders) if total_orders > 0 else 0.0
        row["avg_portions_per_person"] = (total_qty / unique_emp) if unique_emp > 0 else 0.0
        return row

    def get_meals_cost_kpi(self) -> Dict[str, Any]:
        """
        Стоимость питания за период (если meal_type_id заполнен и meal_types.price задан).
        """
        base_query = """
        SELECT
            COALESCE(SUM(COALESCE(mt.price,0) * COALESCE(moi.quantity,1)), 0)::numeric AS total_cost
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        LEFT JOIN meal_types mt ON mt.id = moi.meal_type_id
        LEFT JOIN objects o ON mo.object_id = o.id
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause};
        """
        params: List[Any] = [self.start_date, self.end_date]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = base_query.format(filter_clause=filter_clause)
        rows = self._execute_query(query, tuple(params))
        row = rows[0] if rows else {}
        return {"total_cost": float(row.get("total_cost", 0) or 0)}

    def get_meals_by_type(self) -> pd.DataFrame:
        base_query = """
        SELECT
            moi.meal_type_text,
            COALESCE(SUM(moi.quantity), 0) AS total_qty
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        LEFT JOIN objects o ON mo.object_id = o.id
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY moi.meal_type_text
        ORDER BY total_qty DESC;
        """
        params: List[Any] = [self.start_date, self.end_date]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = base_query.format(filter_clause=filter_clause)
        data = self._execute_query(query, tuple(params))
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_qty"] = df["total_qty"].astype(float)
        return df

    def get_meals_trend_by_month(self) -> pd.DataFrame:
        base_query = """
        SELECT
            date_trunc('month', mo.date) AS period,
            COALESCE(SUM(moi.quantity),0) AS total_qty
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        LEFT JOIN objects o ON mo.object_id = o.id
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY period
        ORDER BY period;
        """
        params: List[Any] = [self.start_date, self.end_date]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = base_query.format(filter_clause=filter_clause)
        data = self._execute_query(query, tuple(params))
        return pd.DataFrame(data)

    def get_meals_by_object(self, limit: int = 10) -> pd.DataFrame:
        base_query = """
        SELECT
            o.address AS object_name,
            COALESCE(SUM(moi.quantity),0) AS total_qty,
            COUNT(DISTINCT moi.employee_id) AS unique_employees
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        LEFT JOIN objects o ON mo.object_id = o.id
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY o.address
        HAVING o.address IS NOT NULL
        ORDER BY total_qty DESC
        LIMIT {limit};
        """
        params: List[Any] = [self.start_date, self.end_date]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = base_query.format(filter_clause=filter_clause, limit=limit)
        data = self._execute_query(query, tuple(params))
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_qty"] = df["total_qty"].astype(float)
            df["unique_employees"] = df["unique_employees"].astype(int)
        return df

    def get_meals_by_department(self) -> pd.DataFrame:
        query = """
        SELECT
            COALESCE(d.name,'—') AS department_name,
            COALESCE(SUM(moi.quantity),0) AS total_qty,
            COUNT(DISTINCT moi.employee_id) AS unique_employees
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        LEFT JOIN departments d ON mo.department_id = d.id
        WHERE mo.date BETWEEN %s AND %s
        GROUP BY d.name
        ORDER BY total_qty DESC;
        """
        data = self._execute_query(query, (self.start_date, self.end_date))
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_qty"] = df["total_qty"].astype(float)
            df["unique_employees"] = df["unique_employees"].astype(int)
        return df

    # ============================================================
    #        4. СКВОЗНАЯ АНАЛИТИКА ПО ОБЪЕКТАМ (TOP-N)
    # ============================================================

    def get_objects_overview(self, limit: int = 20) -> pd.DataFrame:
        start_period = self.start_date.year * 100 + self.start_date.month
        end_period = self.end_date.year * 100 + self.end_date.month

        type_filter_clause = ""
        params: List[Any] = [
            start_period, end_period,           # labor
            self.start_date, self.end_date,     # transport
            self.start_date, self.end_date,     # meals
        ]
        if self.object_type_filter:
            type_filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = """
        WITH labor AS (
            SELECT
                th.object_db_id AS object_id,
                SUM(tr.total_hours) AS labor_hours
            FROM timesheet_headers th
            JOIN timesheet_rows tr ON th.id = tr.header_id
            WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
            GROUP BY th.object_db_id
        ),
        transport AS (
            SELECT
                t.object_id,
                SUM(tp.hours) AS machine_hours
            FROM transport_orders t
            JOIN transport_order_positions tp ON t.id = tp.order_id
            WHERE t.date BETWEEN %s AND %s
            GROUP BY t.object_id
        ),
        meals AS (
            SELECT
                mo.object_id,
                COALESCE(SUM(moi.quantity),0) AS portions
            FROM meal_orders mo
            JOIN meal_order_items moi ON mo.id = moi.order_id
            WHERE mo.date BETWEEN %s AND %s
            GROUP BY mo.object_id
        )
        SELECT
            o.id,
            o.address,
            COALESCE(l.labor_hours, 0)     AS labor_hours,
            COALESCE(trp.machine_hours, 0) AS machine_hours,
            COALESCE(m.portions, 0)        AS portions,
            (COALESCE(l.labor_hours,0) + COALESCE(trp.machine_hours,0) + COALESCE(m.portions,0)) AS total_activity
        FROM objects o
        LEFT JOIN labor     l   ON o.id = l.object_id
        LEFT JOIN transport trp ON o.id = trp.object_id
        LEFT JOIN meals     m   ON o.id = m.object_id
        WHERE (COALESCE(l.labor_hours, 0)
           +  COALESCE(trp.machine_hours, 0)
           +  COALESCE(m.portions, 0)) > 0
        {type_filter_clause}
        ORDER BY labor_hours DESC
        LIMIT {limit};
        """.format(type_filter_clause=type_filter_clause, limit=limit)

        data = self._execute_query(query, tuple(params))
        df = pd.DataFrame(data)
        if not df.empty:
            df["labor_hours"] = df["labor_hours"].astype(float)
            df["machine_hours"] = df["machine_hours"].astype(float)
            df["portions"] = df["portions"].astype(float)
            df["total_activity"] = df["total_activity"].astype(float)
        return df

    # ============================================================
    #        5. АКТИВНОСТЬ ПОЛЬЗОВАТЕЛЕЙ (+ ПРОЖИВАНИЕ)
    # ============================================================

    def get_users_activity(self) -> pd.DataFrame:
        query = """
        SELECT
            u.username,
            u.full_name,
            COALESCE(th_cnt, 0)  AS timesheets_created,
            COALESCE(trp_cnt, 0) AS transport_orders_created,
            COALESCE(mo_cnt, 0)  AS meal_orders_created,
            COALESCE(ci_cnt, 0)  AS dorm_checkins,
            COALESCE(co_cnt, 0)  AS dorm_checkouts
        FROM app_users u
        LEFT JOIN (
            SELECT user_id, COUNT(*) AS th_cnt
            FROM timesheet_headers
            WHERE created_at::date BETWEEN %s AND %s
            GROUP BY user_id
        ) th ON u.id = th.user_id
        LEFT JOIN (
            SELECT user_id, COUNT(*) AS trp_cnt
            FROM transport_orders
            WHERE date BETWEEN %s AND %s
            GROUP BY user_id
        ) to2 ON u.id = to2.user_id
        LEFT JOIN (
            SELECT user_id, COUNT(*) AS mo_cnt
            FROM meal_orders
            WHERE date BETWEEN %s AND %s
            GROUP BY user_id
        ) mo ON u.id = mo.user_id
        LEFT JOIN (
            SELECT created_by AS user_id, COUNT(*) AS ci_cnt
            FROM dorm_stays
            WHERE check_in BETWEEN %s AND %s
              AND created_by IS NOT NULL
            GROUP BY created_by
        ) ci ON u.id = ci.user_id
        LEFT JOIN (
            SELECT closed_by AS user_id, COUNT(*) AS co_cnt
            FROM dorm_stays
            WHERE check_out BETWEEN %s AND %s
              AND closed_by IS NOT NULL
            GROUP BY closed_by
        ) co ON u.id = co.user_id
        WHERE u.is_active = TRUE
        ORDER BY (
            COALESCE(th_cnt,0)+COALESCE(trp_cnt,0)+COALESCE(mo_cnt,0)+COALESCE(ci_cnt,0)+COALESCE(co_cnt,0)
        ) DESC;
        """
        params = (
            self.start_date, self.end_date,  # timesheets created_at
            self.start_date, self.end_date,  # transport date
            self.start_date, self.end_date,  # meals date
            self.start_date, self.end_date,  # dorm checkins by check_in
            self.start_date, self.end_date,  # dorm checkouts by check_out
        )
        data = self._execute_query(query, params)
        df = pd.DataFrame(data)
        if not df.empty:
            for col in ("timesheets_created", "transport_orders_created", "meal_orders_created", "dorm_checkins", "dorm_checkouts"):
                df[col] = df[col].fillna(0).astype(int)
        return df

    # ============================================================
    #                      6. ПРОЖИВАНИЕ
    # ============================================================

    def get_lodging_kpi(self) -> Dict[str, Any]:
        query = """
        WITH days AS (
            SELECT generate_series(%s::date, %s::date, interval '1 day')::date AS d
        ),
        stays_on_day AS (
            SELECT
                dd.d,
                s.employee_id,
                s.dorm_id,
                s.room_id
            FROM days dd
            JOIN dorm_stays s
              ON s.check_in <= dd.d
             AND (s.check_out IS NULL OR s.check_out > dd.d)
        ),
        dorm_mode AS (
            SELECT id, rate_mode FROM dorms
        ),
        rate_on_day AS (
            SELECT
                sod.d,
                sod.employee_id,
                CASE
                    WHEN dm.rate_mode = 'PER_ROOM' THEN (
                        SELECT dr.price_per_day
                        FROM dorm_rates dr
                        WHERE dr.room_id = sod.room_id
                          AND dr.valid_from <= sod.d
                          AND dr.currency = 'RUB'
                        ORDER BY dr.valid_from DESC
                        LIMIT 1
                    )
                    ELSE (
                        SELECT dr.price_per_day
                        FROM dorm_rates dr
                        WHERE dr.dorm_id = sod.dorm_id
                          AND dr.valid_from <= sod.d
                          AND dr.currency = 'RUB'
                        ORDER BY dr.valid_from DESC
                        LIMIT 1
                    )
                END AS price_per_day
            FROM stays_on_day sod
            JOIN dorm_mode dm ON dm.id = sod.dorm_id
        )
        SELECT
            COUNT(*)::int AS bed_days,
            COALESCE(SUM(COALESCE(price_per_day, 0)), 0)::numeric AS amount_rub,
            COALESCE(AVG(price_per_day), 0)::numeric AS avg_price_rub,
            (
                SELECT COUNT(*)
                FROM dorm_stays s2
                WHERE s2.check_in <= %s::date
                  AND (s2.check_out IS NULL OR s2.check_out > %s::date)
            )::int AS active_on_end,
            (
                SELECT COUNT(*)
                FROM rate_on_day
                WHERE price_per_day IS NULL
            )::int AS missing_rate_bed_days
        FROM rate_on_day;
        """
        rows = self._execute_query(query, (self.start_date, self.end_date, self.end_date, self.end_date))
        row = rows[0] if rows else {}
        # нормализация типов
        for k in ("bed_days", "active_on_end", "missing_rate_bed_days"):
            row[k] = int(row.get(k, 0) or 0)
        for k in ("amount_rub", "avg_price_rub"):
            row[k] = float(row.get(k, 0) or 0)
        return row

    def get_lodging_daily_occupancy(self) -> pd.DataFrame:
        query = """
        WITH days AS (
            SELECT generate_series(%s::date, %s::date, interval '1 day')::date AS d
        )
        SELECT
            dd.d,
            (
                SELECT COUNT(*)
                FROM dorm_stays s
                WHERE s.check_in <= dd.d
                  AND (s.check_out IS NULL OR s.check_out > dd.d)
            )::int AS occupied_beds
        FROM days dd
        ORDER BY dd.d;
        """
        data = self._execute_query(query, (self.start_date, self.end_date))
        df = pd.DataFrame(data)
        if not df.empty:
            df["occupied_beds"] = df["occupied_beds"].astype(int)
        return df

    def get_lodging_by_dorm(self, limit: int = 10) -> pd.DataFrame:
        query = """
        WITH days AS (
            SELECT generate_series(%s::date, %s::date, interval '1 day')::date AS d
        ),
        stays_on_day AS (
            SELECT
                dd.d,
                s.dorm_id,
                s.room_id
            FROM days dd
            JOIN dorm_stays s
              ON s.check_in <= dd.d
             AND (s.check_out IS NULL OR s.check_out > dd.d)
        ),
        dorm_mode AS (
            SELECT id, rate_mode, name FROM dorms
        ),
        rated AS (
            SELECT
                sod.d,
                sod.dorm_id,
                CASE
                    WHEN dm.rate_mode = 'PER_ROOM' THEN (
                        SELECT dr.price_per_day
                        FROM dorm_rates dr
                        WHERE dr.room_id = sod.room_id
                          AND dr.valid_from <= sod.d
                          AND dr.currency = 'RUB'
                        ORDER BY dr.valid_from DESC
                        LIMIT 1
                    )
                    ELSE (
                        SELECT dr.price_per_day
                        FROM dorm_rates dr
                        WHERE dr.dorm_id = sod.dorm_id
                          AND dr.valid_from <= sod.d
                          AND dr.currency = 'RUB'
                        ORDER BY dr.valid_from DESC
                        LIMIT 1
                    )
                END AS price_per_day
            FROM stays_on_day sod
            JOIN dorm_mode dm ON dm.id = sod.dorm_id
        )
        SELECT
            dm.name AS dorm_name,
            COUNT(*)::int AS bed_days,
            COALESCE(SUM(COALESCE(r.price_per_day,0)),0)::numeric AS amount_rub,
            COALESCE(AVG(r.price_per_day),0)::numeric AS avg_price_rub,
            SUM(CASE WHEN r.price_per_day IS NULL THEN 1 ELSE 0 END)::int AS missing_rate_bed_days
        FROM rated r
        JOIN dorm_mode dm ON dm.id = r.dorm_id
        GROUP BY dm.name
        ORDER BY amount_rub DESC
        LIMIT %s;
        """
        data = self._execute_query(query, (self.start_date, self.end_date, limit))
        df = pd.DataFrame(data)
        if not df.empty:
            df["bed_days"] = df["bed_days"].astype(int)
            df["amount_rub"] = df["amount_rub"].astype(float)
            df["avg_price_rub"] = df["avg_price_rub"].astype(float)
            df["missing_rate_bed_days"] = df["missing_rate_bed_days"].astype(int)
        return df

    def get_lodging_by_department(self) -> pd.DataFrame:
        query = """
        WITH days AS (
            SELECT generate_series(%s::date, %s::date, interval '1 day')::date AS d
        ),
        stays_on_day AS (
            SELECT
                dd.d,
                s.employee_id,
                s.dorm_id,
                s.room_id
            FROM days dd
            JOIN dorm_stays s
              ON s.check_in <= dd.d
             AND (s.check_out IS NULL OR s.check_out > dd.d)
        ),
        dorm_mode AS (
            SELECT id, rate_mode FROM dorms
        ),
        rated AS (
            SELECT
                sod.d,
                sod.employee_id,
                CASE
                    WHEN dm.rate_mode = 'PER_ROOM' THEN (
                        SELECT dr.price_per_day
                        FROM dorm_rates dr
                        WHERE dr.room_id = sod.room_id
                          AND dr.valid_from <= sod.d
                          AND dr.currency = 'RUB'
                        ORDER BY dr.valid_from DESC
                        LIMIT 1
                    )
                    ELSE (
                        SELECT dr.price_per_day
                        FROM dorm_rates dr
                        WHERE dr.dorm_id = sod.dorm_id
                          AND dr.valid_from <= sod.d
                          AND dr.currency = 'RUB'
                        ORDER BY dr.valid_from DESC
                        LIMIT 1
                    )
                END AS price_per_day
            FROM stays_on_day sod
            JOIN dorm_mode dm ON dm.id = sod.dorm_id
        )
        SELECT
            COALESCE(dep.name,'—') AS department_name,
            COUNT(*)::int AS bed_days,
            COALESCE(SUM(COALESCE(r.price_per_day,0)),0)::numeric AS amount_rub,
            SUM(CASE WHEN r.price_per_day IS NULL THEN 1 ELSE 0 END)::int AS missing_rate_bed_days
        FROM rated r
        JOIN employees e ON e.id = r.employee_id
        LEFT JOIN departments dep ON dep.id = e.department_id
        GROUP BY dep.name
        ORDER BY amount_rub DESC;
        """
        data = self._execute_query(query, (self.start_date, self.end_date))
        df = pd.DataFrame(data)
        if not df.empty:
            df["bed_days"] = df["bed_days"].astype(int)
            df["amount_rub"] = df["amount_rub"].astype(float)
            df["missing_rate_bed_days"] = df["missing_rate_bed_days"].astype(int)
        return df

    def get_dorm_to_objects_people(self) -> pd.DataFrame:
        """
        Данные для pivot "общежитие → объекты" по количеству людей (уникальные tbn).
        Возвращает long-формат: dorm_name, object_name, people_cnt
        """
        start_period = self.start_date.year * 100 + self.start_date.month
        end_period = self.end_date.year * 100 + self.end_date.month

        obj_filter = ""
        params: List[Any] = [start_period, end_period]
        if self.object_type_filter:
            obj_filter = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        # Важно: tbn пустой исключаем, иначе "уникальные" развалятся
        query = f"""
        WITH ts AS (
            SELECT
                th.year,
                th.month,
                th.object_db_id AS object_id,
                tr.tbn AS tbn,
                make_date(th.year, th.month, 1)::date AS month_date
            FROM timesheet_headers th
            JOIN timesheet_rows tr ON tr.header_id = th.id
            LEFT JOIN objects o ON o.id = th.object_db_id
            WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
              AND COALESCE(tr.tbn,'') <> ''
              {obj_filter}
            GROUP BY th.year, th.month, th.object_db_id, tr.tbn
        ),
        ts_emp AS (
            SELECT
                ts.object_id,
                ts.month_date,
                e.id AS employee_id,
                ts.tbn
            FROM ts
            JOIN employees e ON COALESCE(e.tbn,'') <> '' AND e.tbn = ts.tbn
        ),
        ts_dorm AS (
            SELECT
                te.object_id,
                (
                    SELECT d.name
                    FROM dorm_stays s
                    JOIN dorms d ON d.id = s.dorm_id
                    WHERE s.employee_id = te.employee_id
                      AND s.check_in <= te.month_date
                      AND (s.check_out IS NULL OR s.check_out > te.month_date)
                    ORDER BY s.check_in DESC
                    LIMIT 1
                ) AS dorm_name,
                te.tbn
            FROM ts_emp te
        )
        SELECT
            COALESCE(td.dorm_name, '— (без проживания)') AS dorm_name,
            COALESCE(o.address, '—') AS object_name,
            COUNT(DISTINCT td.tbn)::int AS people_cnt
        FROM ts_dorm td
        LEFT JOIN objects o ON o.id = td.object_id
        GROUP BY dorm_name, object_name
        ORDER BY people_cnt DESC;
        """
        data = self._execute_query(query, tuple(params))
        df = pd.DataFrame(data)
        if not df.empty:
            df["people_cnt"] = df["people_cnt"].astype(int)
            df["dorm_name"] = df["dorm_name"].fillna("—")
            df["object_name"] = df["object_name"].fillna("—")
        return df

    def get_dorm_to_objects_people_pivot(self, top_objects: int = 10, top_dorms: int = 30) -> pd.DataFrame:
        df = self.get_dorm_to_objects_people()
        if df.empty:
            return df

        dorm_order = (
            df.groupby("dorm_name")["people_cnt"].sum().sort_values(ascending=False).head(top_dorms).index.tolist()
        )
        df = df[df["dorm_name"].isin(dorm_order)]

        obj_order = (
            df.groupby("object_name")["people_cnt"].sum().sort_values(ascending=False).head(top_objects).index.tolist()
        )
        df = df[df["object_name"].isin(obj_order)]

        pv = df.pivot_table(
            index="dorm_name",
            columns="object_name",
            values="people_cnt",
            aggfunc="sum",
            fill_value=0,
        )
        pv["ИТОГО"] = pv.sum(axis=1)
        pv = pv.sort_values("ИТОГО", ascending=False)
        return pv


# ============================================================
#                      UI: ANALYTICS PAGE
# ============================================================

class AnalyticsPage(ttk.Frame):
    def __init__(self, master, app_ref):
        super().__init__(master)
        self.app_ref = app_ref

        filter_frame = ttk.Frame(self, padding="10")
        filter_frame.pack(fill="x", side="top")

        ttk.Label(filter_frame, text="Период:").pack(side="left", padx=(0, 5))
        self.period_var = tk.StringVar(value="Текущий месяц")
        period_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.period_var,
            values=["Текущий месяц", "Прошлый месяц", "Текущий квартал", "Текущий год"],
            state="readonly",
            width=18,
        )
        period_combo.pack(side="left", padx=5)
        period_combo.bind("<<ComboboxSelected>>", self.refresh_data)

        ttk.Label(filter_frame, text="Тип объекта:").pack(side="left", padx=(10, 5))
        self.object_type_var = tk.StringVar(value="Все типы")
        self.object_type_combo = ttk.Combobox(
            filter_frame,
            textvariable=self.object_type_var,
            state="readonly",
            width=30,
        )
        self.object_type_combo.pack(side="left", padx=5)
        self.object_type_combo.bind("<<ComboboxSelected>>", self.refresh_data)

        ttk.Button(filter_frame, text="Обновить", command=self.refresh_data).pack(side="left", padx=10)

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=5)

        self.tab_labor = ttk.Frame(self.notebook)
        self.tab_transport = ttk.Frame(self.notebook)
        self.tab_meals = ttk.Frame(self.notebook)
        self.tab_objects = ttk.Frame(self.notebook)
        self.tab_users = ttk.Frame(self.notebook)
        self.tab_lodging = ttk.Frame(self.notebook)

        self.notebook.add(self.tab_labor, text="  Трудозатраты  ")
        self.notebook.add(self.tab_transport, text="  Транспорт и Техника  ")
        self.notebook.add(self.tab_meals, text="  Питание  ")
        self.notebook.add(self.tab_objects, text="  Объекты  ")
        self.notebook.add(self.tab_users, text="  Активность пользователей  ")
        self.notebook.add(self.tab_lodging, text="  Проживание  ")

        self.data_provider: Optional[AnalyticsData] = None
        self.load_filters()
        self.refresh_data()

    def load_filters(self):
        try:
            temp_provider = AnalyticsData(datetime.now().date(), datetime.now().date(), "")
            types = temp_provider.get_object_types()
            self.object_type_combo["values"] = ["Все типы"] + types
        except Exception as e:
            logging.error(f"Не удалось загрузить типы объектов: {e}")
            self.object_type_combo["values"] = ["Все типы"]

    def get_dates_from_period(self) -> Tuple[datetime.date, datetime.date]:
        period = self.period_var.get()
        today = datetime.today()
        if period == "Текущий месяц":
            start_date = today.replace(day=1)
            end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        elif period == "Прошлый месяц":
            end_date = today.replace(day=1) - timedelta(days=1)
            start_date = end_date.replace(day=1)
        elif period == "Текущий квартал":
            current_quarter = (today.month - 1) // 3 + 1
            start_date = datetime(today.year, 3 * current_quarter - 2, 1)
            end_date = (start_date + timedelta(days=95)).replace(day=1) - timedelta(days=1)
        elif period == "Текущий год":
            start_date = datetime(today.year, 1, 1)
            end_date = datetime(today.year, 12, 31)
        else:
            start_date = today.replace(day=1)
            end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        return start_date.date(), end_date.date()

    def refresh_data(self, event=None):
        start_date, end_date = self.get_dates_from_period()
        obj_type_filter = self.object_type_var.get()
        if obj_type_filter == "Все типы":
            obj_type_filter = ""
        self.data_provider = AnalyticsData(start_date, end_date, obj_type_filter)

        self._build_labor_tab()
        self._build_transport_tab()
        self._build_meals_tab()
        self._build_objects_tab()
        self._build_users_tab()
        self._build_lodging_tab()

    # ---------- UI helpers ----------

    def _clear_tab(self, tab):
        for widget in tab.winfo_children():
            widget.destroy()

    def _create_kpi_card(self, parent, title, value, unit):
        card = ttk.Frame(parent, borderwidth=2, relief="groove", padding=10)
        ttk.Label(card, text=title, font=("Segoe UI", 10, "bold")).pack()
        ttk.Label(card, text=f"{value}", font=("Segoe UI", 18, "bold"), foreground="#0078D7").pack(pady=(5, 0))
        ttk.Label(card, text=unit, font=("Segoe UI", 9)).pack()
        return card

    def _create_treeview(self, parent, columns: List[tuple], height: int = 10) -> ttk.Treeview:
        tree = ttk.Treeview(parent, columns=[c[0] for c in columns], show="headings", height=height)
        for col_id, col_text in columns:
            tree.heading(col_id, text=col_text)
            tree.column(col_id, anchor="w", width=120)
        vsb = ttk.Scrollbar(parent, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        parent.grid_rowconfigure(0, weight=1)
        parent.grid_columnconfigure(0, weight=1)
        return tree

    def _create_treeview_with_hscroll(self, parent, columns: List[str], height: int = 12) -> ttk.Treeview:
        container = ttk.Frame(parent)
        container.pack(fill="both", expand=True)

        tree = ttk.Treeview(container, columns=columns, show="headings", height=height)

        vsb = ttk.Scrollbar(container, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(container, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)
        return tree

    # ============================================================
    #                   TAB 1: ТРУДОЗАТРАТЫ
    # ============================================================

    def _build_labor_tab(self):
        self._clear_tab(self.tab_labor)

        kpi_frame = ttk.Frame(self.tab_labor)
        kpi_frame.pack(fill="x", pady=10, padx=5)

        kpi = self.data_provider.get_labor_kpi()
        cards = [
            ("Всего чел.-часов", f"{kpi.get('total_hours', 0):.1f}", "час."),
            ("Всего чел.-дней", f"{kpi.get('total_days', 0):.0f}", "дн."),
            ("Часы переработок", f"{kpi.get('total_overtime', 0):.1f}", "час."),
            ("Сотрудников", int(kpi.get("unique_people", 0) or 0), "чел."),
            ("Часов/сотр.", f"{kpi.get('hours_per_person', 0):.1f}", "час/чел"),
            ("Доля переработок", f"{kpi.get('overtime_share_pct', 0):.1f}", "%"),
        ]
        for i, (title, value, unit) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit)
            card.grid(row=0, column=i, padx=5, sticky="ew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        charts_frame = ttk.Frame(self.tab_labor)
        charts_frame.pack(fill="both", expand=True, padx=5, pady=5)

        left_frame = ttk.LabelFrame(charts_frame, text="ТОП-10 объектов по трудозатратам")
        left_frame.pack(side="left", fill="both", expand=True, padx=(0, 5))

        df_objects = self.data_provider.get_labor_by_object()
        if not df_objects.empty:
            df_objects = df_objects.copy()
            df_objects["total_hours"] = df_objects["total_hours"].fillna(0).astype(float)
            df_objects["object_name"] = df_objects["object_name"].fillna("—")

            def short_addr(a: str, max_len: int = 30) -> str:
                a = a or "—"
                return a if len(a) <= max_len else a[:max_len] + "…"

            df_objects["short_name"] = df_objects["object_name"].apply(short_addr)

            fig1 = Figure(figsize=(5, 4), dpi=100)
            ax1 = fig1.add_subplot(111)
            df_plot = df_objects.head(10).sort_values("total_hours", ascending=True)
            bars = ax1.barh(df_plot["short_name"], df_plot["total_hours"], color="#0078D7")
            ax1.set_xlabel("Человеко-часы")
            ax1.grid(axis="x", linestyle="--", alpha=0.7)
            max_val = float(df_plot["total_hours"].max() or 0.0)
            for bar in bars:
                width = float(bar.get_width() or 0.0)
                ax1.text(width + max_val * 0.02, bar.get_y() + bar.get_height() / 2, f"{width:.0f}", va="center")
            fig1.tight_layout(rect=(0.15, 0.05, 0.95, 0.95))
            canvas1 = FigureCanvasTkAgg(fig1, master=left_frame)
            canvas1.draw()
            canvas1.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        else:
            ttk.Label(left_frame, text="Нет данных.").pack(padx=10, pady=10)

        right_frame = ttk.Frame(charts_frame)
        right_frame.pack(side="left", fill="both", expand=True, padx=(5, 0))

        trend_frame = ttk.LabelFrame(right_frame, text="Динамика трудозатрат по месяцам")
        trend_frame.pack(fill="both", expand=True, padx=5, pady=(0, 5))

        df_trend = self.data_provider.get_labor_trend_by_month()
        if not df_trend.empty:
            df_trend = df_trend.copy()
            df_trend["total_hours"] = df_trend["total_hours"].fillna(0).astype(float)
            df_trend["period"] = df_trend.apply(lambda r: f"{int(r['year'])}-{int(r['month']):02d}", axis=1)

            fig2 = Figure(figsize=(5, 2.5), dpi=100)
            ax2 = fig2.add_subplot(111)
            ax2.plot(df_trend["period"], df_trend["total_hours"], marker="o", color="#5E9A2C")
            ax2.set_ylabel("Человеко-часы")
            ax2.set_xticks(range(len(df_trend["period"])))
            ax2.set_xticklabels(df_trend["period"], rotation=45, ha="right")
            ax2.grid(True, linestyle="--", alpha=0.5)
            fig2.tight_layout()
            canvas2 = FigureCanvasTkAgg(fig2, master=trend_frame)
            canvas2.draw()
            canvas2.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        else:
            ttk.Label(trend_frame, text="Нет данных.").pack(padx=10, pady=10)

        top_emp_frame = ttk.LabelFrame(right_frame, text="ТОП-10 сотрудников по часам")
        top_emp_frame.pack(fill="both", expand=True, padx=5, pady=(5, 0))

        df_emp = self.data_provider.get_top_employees_by_hours(limit=10)
        if not df_emp.empty:
            df_emp = df_emp.copy()
            df_emp["total_hours"] = df_emp["total_hours"].fillna(0).astype(float)
            df_emp["fio"] = df_emp["fio"].fillna("—")

            fig3 = Figure(figsize=(5, 2.5), dpi=100)
            ax3 = fig3.add_subplot(111)
            df_plot_emp = df_emp.sort_values("total_hours", ascending=True)
            bars_emp = ax3.barh(df_plot_emp["fio"], df_plot_emp["total_hours"], color="#FF8C00")
            ax3.set_xlabel("Человеко-часы")
            ax3.grid(axis="x", linestyle="--", alpha=0.7)
            fig3.tight_layout()
            for bar in bars_emp:
                width = float(bar.get_width() or 0.0)
                ax3.text(width + 2.0, bar.get_y() + bar.get_height() / 2, f"{width:.0f}", va="center")
            canvas3 = FigureCanvasTkAgg(fig3, master=top_emp_frame)
            canvas3.draw()
            canvas3.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        else:
            ttk.Label(top_emp_frame, text="Нет данных.").pack(padx=10, pady=10)

        dept_frame = ttk.LabelFrame(self.tab_labor, text="Нагрузка по подразделениям")
        dept_frame.pack(fill="both", expand=False, padx=5, pady=5)

        df_dept = self.data_provider.get_labor_by_department()
        if not df_dept.empty:
            tree = self._create_treeview(
                dept_frame,
                columns=[("department", "Подразделение"), ("people", "Людей"), ("hours", "Чел.-часы")],
                height=8,
            )
            tree.column("department", width=220)
            tree.column("people", width=80, anchor="e")
            tree.column("hours", width=120, anchor="e")
            for _, row in df_dept.iterrows():
                tree.insert("", "end", values=(row["department_name"], int(row["people_cnt"]), f"{float(row['total_hours']):.1f}"))
        else:
            ttk.Label(dept_frame, text="Нет данных.").pack(padx=10, pady=10)

    # ============================================================
    #                   TAB 2: ТРАНСПОРТ
    # ============================================================

    def _build_transport_tab(self):
        self._clear_tab(self.tab_transport)

        kpi_frame = ttk.Frame(self.tab_transport)
        kpi_frame.pack(fill="x", pady=10, padx=5)

        kpi = self.data_provider.get_transport_kpi()
        cards = [
            ("Всего маш.-часов", f"{kpi.get('total_machine_hours', 0):.1f}", "час."),
            ("Всего заявок", int(kpi.get("total_orders", 0) or 0), "шт."),
            ("Единиц техники", f"{kpi.get('total_units', 0):.0f}", "шт."),
            ("Среднее на заявку", f"{kpi.get('avg_hours_per_order', 0):.1f}", "час/заявку"),
            ("Часов на ед.", f"{kpi.get('hours_per_unit', 0):.1f}", "час/ед."),
        ]
        for i, (title, value, unit) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit)
            card.grid(row=0, column=i, padx=5, sticky="ew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        df = self.data_provider.get_transport_by_tech()
        if not df.empty:
            df = df.copy()
            df["total_hours"] = df["total_hours"].fillna(0).astype(float)
            df["tech"] = df["tech"].fillna("—")
            fig = Figure(figsize=(10, 5), dpi=100)
            ax = fig.add_subplot(111)
            df_plot = df.head(10).sort_values("total_hours", ascending=False)
            ax.bar(df_plot["tech"], df_plot["total_hours"], color="#5E9A2C")
            ax.set_title("ТОП-10 востребованной техники", fontsize=12, weight="bold")
            ax.set_ylabel("Машино-часы")
            ax.tick_params(axis="x", rotation=45, labelsize=9)
            ax.grid(axis="y", linestyle="--", alpha=0.7)
            fig.tight_layout(pad=2.0)
            canvas = FigureCanvasTkAgg(fig, master=self.tab_transport)
            canvas.draw()
            canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True, pady=10, padx=5)
        else:
            ttk.Label(self.tab_transport, text="Нет данных по технике за период.").pack(padx=10, pady=10)

    # ============================================================
    #                   TAB 3: ПИТАНИЕ
    # ============================================================

    def _build_meals_tab(self):
        self._clear_tab(self.tab_meals)

        kpi_frame = ttk.Frame(self.tab_meals)
        kpi_frame.pack(fill="x", pady=10, padx=5)

        kpi = self.data_provider.get_meals_kpi()
        cost = self.data_provider.get_meals_cost_kpi()

        total_qty = float(kpi.get("total_portions_qty", 0) or 0)
        total_orders = int(kpi.get("total_orders", 0) or 0)
        unique_emp = int(kpi.get("unique_employees", 0) or 0)
        total_cost = float(cost.get("total_cost", 0) or 0)

        cards = [
            ("Всего порций", f"{total_qty:,.0f}".replace(",", " "), "шт."),
            ("Всего заявок", total_orders, "шт."),
            ("Накормлено людей", unique_emp, "чел."),
            ("Порций/заявку", f"{float(kpi.get('avg_portions_per_order', 0) or 0):.2f}", ""),
            ("Порций/чел.", f"{float(kpi.get('avg_portions_per_person', 0) or 0):.2f}", ""),
            ("Стоимость (оценка)", f"{total_cost:,.0f}".replace(",", " "), "₽"),
        ]
        for i, (title, value, unit) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit)
            card.grid(row=0, column=i, padx=5, sticky="ew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        top_frame = ttk.Frame(self.tab_meals)
        top_frame.pack(fill="both", expand=True, padx=5, pady=5)

        left_frame = ttk.LabelFrame(top_frame, text="Популярность типов питания (по количеству порций)")
        left_frame.pack(side="left", fill="both", expand=True, padx=(0, 5))

        df_types = self.data_provider.get_meals_by_type()
        if not df_types.empty:
            df_types = df_types.copy()
            df_types["total_qty"] = df_types["total_qty"].fillna(0).astype(float)
            df_types["meal_type_text"] = df_types["meal_type_text"].fillna("—")

            fig1 = Figure(figsize=(5, 4), dpi=100)
            ax1 = fig1.add_subplot(111)
            labels = df_types["meal_type_text"]
            sizes = df_types["total_qty"]

            def autopct_format(values):
                def my_format(pct):
                    total = float(sum(values))
                    val = float(pct * total / 100.0)
                    return f"{pct:.1f}%\n({val:.0f})"
                return my_format

            ax1.pie(
                sizes,
                labels=labels,
                autopct=autopct_format(sizes),
                startangle=90,
                wedgeprops=dict(width=0.4),
                pctdistance=0.8,
            )
            ax1.axis("equal")
            fig1.tight_layout()
            canvas1 = FigureCanvasTkAgg(fig1, master=left_frame)
            canvas1.draw()
            canvas1.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        else:
            ttk.Label(left_frame, text="Нет данных.").pack(padx=10, pady=10)

        right_frame = ttk.LabelFrame(top_frame, text="Динамика количества порций (по месяцам)")
        right_frame.pack(side="left", fill="both", expand=True, padx=(5, 0))

        df_trend = self.data_provider.get_meals_trend_by_month()
        if not df_trend.empty:
            df_trend = df_trend.copy()
            df_trend["total_qty"] = df_trend["total_qty"].fillna(0).astype(float)
            df_trend["period"] = pd.to_datetime(df_trend["period"]).dt.strftime("%Y-%m")

            fig2 = Figure(figsize=(5, 4), dpi=100)
            ax2 = fig2.add_subplot(111)
            ax2.plot(df_trend["period"], df_trend["total_qty"], marker="o", color="#0078D7")
            ax2.set_ylabel("Порций")
            ax2.set_xticks(range(len(df_trend["period"])))
            ax2.set_xticklabels(df_trend["period"], rotation=45, ha="right")
            ax2.grid(True, linestyle="--", alpha=0.5)
            fig2.tight_layout()
            canvas2 = FigureCanvasTkAgg(fig2, master=right_frame)
            canvas2.draw()
            canvas2.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        else:
            ttk.Label(right_frame, text="Нет данных.").pack(padx=10, pady=10)

        bottom_frame = ttk.Frame(self.tab_meals)
        bottom_frame.pack(fill="both", expand=False, padx=5, pady=5)

        obj_frame = ttk.LabelFrame(bottom_frame, text="ТОП-объекты по количеству порций")
        obj_frame.pack(side="left", fill="both", expand=True, padx=(0, 5))

        df_obj = self.data_provider.get_meals_by_object(limit=10)
        if not df_obj.empty:
            tree1 = self._create_treeview(
                obj_frame,
                columns=[("object", "Объект"), ("qty", "Порций"), ("people", "Людей")],
                height=8,
            )
            tree1.column("object", width=260)
            tree1.column("qty", width=80, anchor="e")
            tree1.column("people", width=80, anchor="e")
            for _, row in df_obj.iterrows():
                tree1.insert("", "end", values=(row["object_name"], f"{float(row['total_qty']):.0f}", int(row["unique_employees"])))
        else:
            ttk.Label(obj_frame, text="Нет данных.").pack(padx=10, pady=10)

        dept_frame = ttk.LabelFrame(bottom_frame, text="Питание по подразделениям")
        dept_frame.pack(side="left", fill="both", expand=True, padx=(5, 0))

        df_dept = self.data_provider.get_meals_by_department()
        if not df_dept.empty:
            tree2 = self._create_treeview(
                dept_frame,
                columns=[("dept", "Подразделение"), ("qty", "Порций"), ("people", "Людей")],
                height=8,
            )
            tree2.column("dept", width=220)
            tree2.column("qty", width=80, anchor="e")
            tree2.column("people", width=80, anchor="e")
            for _, row in df_dept.iterrows():
                tree2.insert("", "end", values=(row["department_name"], f"{float(row['total_qty']):.0f}", int(row["unique_employees"])))
        else:
            ttk.Label(dept_frame, text="Нет данных.").pack(padx=10, pady=10)

    # ============================================================
    #                   TAB 4: ОБЪЕКТЫ
    # ============================================================

    def _build_objects_tab(self):
        self._clear_tab(self.tab_objects)

        frame = ttk.Frame(self.tab_objects)
        frame.pack(fill="both", expand=True, padx=5, pady=5)

        df = self.data_provider.get_objects_overview(limit=20)
        if df.empty:
            ttk.Label(frame, text="Нет данных по объектам за выбранный период.").pack(padx=10, pady=10)
            return

        df = df.copy()
        df["labor_hours"] = df["labor_hours"].fillna(0).astype(float)
        df["machine_hours"] = df["machine_hours"].fillna(0).astype(float)
        df["portions"] = df["portions"].fillna(0).astype(float)
        df["total_activity"] = df["total_activity"].fillna(0).astype(float)
        df["address"] = df["address"].fillna("—")

        table_frame = ttk.LabelFrame(frame, text="ТОП объектов (по трудозатратам)")
        table_frame.pack(side="left", fill="both", expand=True, padx=(0, 5), pady=5)

        tree = self._create_treeview(
            table_frame,
            columns=[("address", "Объект"), ("labor", "Чел.-часы"), ("machine", "Маш.-часы"), ("meals", "Порции"), ("total", "Итого")],
            height=15,
        )
        tree.column("address", width=280)
        tree.column("labor", width=90, anchor="e")
        tree.column("machine", width=90, anchor="e")
        tree.column("meals", width=80, anchor="e")
        tree.column("total", width=80, anchor="e")

        for _, row in df.iterrows():
            tree.insert(
                "",
                "end",
                values=(
                    row["address"],
                    f"{row['labor_hours']:.1f}",
                    f"{row['machine_hours']:.1f}",
                    f"{row['portions']:.0f}",
                    f"{row['total_activity']:.1f}",
                ),
            )

        chart_frame = ttk.LabelFrame(frame, text="Сравнение по объектам (ТОП-10)")
        chart_frame.pack(side="left", fill="both", expand=True, padx=(5, 0), pady=5)

        df_top = df.head(10)
        fig = Figure(figsize=(6, 4), dpi=100)
        ax = fig.add_subplot(111)

        x = list(range(len(df_top)))
        width = 0.25
        ax.bar([i - width for i in x], df_top["labor_hours"], width=width, label="Чел.-часы", color="#0078D7")
        ax.bar(x, df_top["machine_hours"], width=width, label="Маш.-часы", color="#5E9A2C")
        ax.bar([i + width for i in x], df_top["portions"], width=width, label="Порции", color="#FF8C00")

        ax.set_xticks(x)
        ax.set_xticklabels([a[:15] + "..." if len(a) > 15 else a for a in df_top["address"]], rotation=45, ha="right")
        ax.legend()
        ax.grid(axis="y", alpha=0.3, linestyle="--")
        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    # ============================================================
    #                   TAB 5: АКТИВНОСТЬ ПОЛЬЗОВАТЕЛЕЙ
    # ============================================================

    def _build_users_tab(self):
        self._clear_tab(self.tab_users)

        frame = ttk.Frame(self.tab_users)
        frame.pack(fill="both", expand=True, padx=5, pady=5)

        df = self.data_provider.get_users_activity()
        if df.empty:
            ttk.Label(frame, text="Нет активности пользователей за выбранный период.").pack(padx=10, pady=10)
            return

        df = df.copy()
        df["username"] = df["username"].fillna("—")
        df["full_name"] = df["full_name"].fillna("")
        df["total_ops"] = (
            df["timesheets_created"]
            + df["transport_orders_created"]
            + df["meal_orders_created"]
            + df["dorm_checkins"]
            + df["dorm_checkouts"]
        )

        top_frame = ttk.Frame(frame)
        top_frame.pack(fill="both", expand=True, pady=(0, 5))

        table_frame = ttk.LabelFrame(top_frame, text="Активность пользователей")
        table_frame.pack(side="left", fill="both", expand=True, padx=(0, 5))

        tree = self._create_treeview(
            table_frame,
            columns=[
                ("user", "Логин"),
                ("name", "ФИО"),
                ("th", "Табелей"),
                ("tr", "Транспорт"),
                ("mo", "Питание"),
                ("ci", "Заселений"),
                ("co", "Выселений"),
            ],
            height=15,
        )
        tree.column("user", width=100)
        tree.column("name", width=200)
        for c in ("th", "tr", "mo", "ci", "co"):
            tree.column(c, width=90, anchor="e")

        for _, row in df.iterrows():
            tree.insert(
                "",
                "end",
                values=(
                    row["username"],
                    row["full_name"] or "",
                    int(row["timesheets_created"]),
                    int(row["transport_orders_created"]),
                    int(row["meal_orders_created"]),
                    int(row["dorm_checkins"]),
                    int(row["dorm_checkouts"]),
                ),
            )

        chart_frame = ttk.LabelFrame(top_frame, text="ТОП пользователей по количеству операций")
        chart_frame.pack(side="left", fill="both", expand=True, padx=(5, 0))

        df_top = df.sort_values("total_ops", ascending=False).head(10)
        fig = Figure(figsize=(5, 4), dpi=100)
        ax = fig.add_subplot(111)
        bars = ax.barh(df_top["username"], df_top["total_ops"], color="#0078D7")
        ax.set_xlabel("Операций (все модули)")
        ax.invert_yaxis()
        ax.grid(axis="x", alpha=0.3, linestyle="--")
        for bar in bars:
            width = float(bar.get_width() or 0.0)
            ax.text(width + 0.5, bar.get_y() + bar.get_height() / 2, f"{int(width)}", va="center")
        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    # ============================================================
    #                   TAB 6: ПРОЖИВАНИЕ
    # ============================================================

    def _build_lodging_tab(self):
        self._clear_tab(self.tab_lodging)

        # KPI
        kpi_frame = ttk.Frame(self.tab_lodging)
        kpi_frame.pack(fill="x", pady=10, padx=5)

        kpi = self.data_provider.get_lodging_kpi()
        bed_days = int(kpi.get("bed_days", 0))
        amount_rub = float(kpi.get("amount_rub", 0))
        avg_price = float(kpi.get("avg_price_rub", 0))
        active_on_end = int(kpi.get("active_on_end", 0))
        missing_rate_bed_days = int(kpi.get("missing_rate_bed_days", 0))

        cards = [
            ("Койко-дней", bed_days, "дней"),
            ("Начислено (RUB)", f"{amount_rub:,.0f}".replace(",", " "), "₽"),
            ("Средняя цена", f"{avg_price:,.0f}".replace(",", " "), "₽/день"),
            ("Проживает на конец", active_on_end, "чел."),
        ]
        for i, (title, value, unit) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit)
            card.grid(row=0, column=i, padx=5, sticky="ew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        if missing_rate_bed_days > 0:
            ttk.Label(
                self.tab_lodging,
                text=f"Внимание: {missing_rate_bed_days} койко-дней без тарифа (RUB). Проверьте dorm_rates.",
                foreground="#B00020",
            ).pack(anchor="w", padx=10, pady=(0, 6))

        # Компактный график занятости
        charts_frame = ttk.Frame(self.tab_lodging)
        charts_frame.pack(fill="both", expand=False, padx=5, pady=5)

        occ_frame = ttk.LabelFrame(charts_frame, text="Занято мест по дням (компактно)")
        occ_frame.pack(side="left", fill="both", expand=True)

        df_occ = self.data_provider.get_lodging_daily_occupancy()
        if not df_occ.empty:
            df_occ = df_occ.copy()
            df_occ["d"] = pd.to_datetime(df_occ["d"])
            df_occ["label"] = df_occ["d"].dt.strftime("%d.%m")

            fig = Figure(figsize=(8, 2.3), dpi=100)
            ax = fig.add_subplot(111)
            ax.plot(df_occ["label"], df_occ["occupied_beds"], color="#0078D7")
            ax.set_ylabel("мест")
            step = max(1, len(df_occ) // 12)
            ax.set_xticks(list(range(0, len(df_occ), step)))
            ax.set_xticklabels(df_occ["label"].iloc[::step], rotation=0, ha="center")
            ax.grid(True, linestyle="--", alpha=0.3)
            fig.tight_layout()

            canvas = FigureCanvasTkAgg(fig, master=occ_frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        else:
            ttk.Label(occ_frame, text="Нет данных за период.").pack(padx=10, pady=10)

        # Таблицы: ТОП общежитий / по подразделениям
        bottom = ttk.Frame(self.tab_lodging)
        bottom.pack(fill="both", expand=True, padx=5, pady=5)

        dorm_frame = ttk.LabelFrame(bottom, text="ТОП общежитий по начислениям (RUB)")
        dorm_frame.pack(side="left", fill="both", expand=True, padx=(0, 5))

        dept_frame = ttk.LabelFrame(bottom, text="По подразделениям (RUB)")
        dept_frame.pack(side="left", fill="both", expand=True, padx=(5, 0))

        df_dorm = self.data_provider.get_lodging_by_dorm(limit=10)
        if not df_dorm.empty:
            tree = self._create_treeview(
                dorm_frame,
                columns=[
                    ("dorm", "Общежитие"),
                    ("bed_days", "Койко-дней"),
                    ("amount", "Начислено, ₽"),
                    ("avg", "Средн., ₽/день"),
                    ("miss", "Без тарифа"),
                ],
                height=10,
            )
            tree.column("dorm", width=240)
            tree.column("bed_days", width=90, anchor="e")
            tree.column("amount", width=120, anchor="e")
            tree.column("avg", width=120, anchor="e")
            tree.column("miss", width=90, anchor="e")
            for _, r in df_dorm.iterrows():
                tree.insert(
                    "",
                    "end",
                    values=(
                        r["dorm_name"],
                        int(r["bed_days"]),
                        f"{float(r['amount_rub']):,.0f}".replace(",", " "),
                        f"{float(r['avg_price_rub']):,.0f}".replace(",", " "),
                        int(r["missing_rate_bed_days"]),
                    ),
                )
        else:
            ttk.Label(dorm_frame, text="Нет данных.").pack(padx=10, pady=10)

        df_dept = self.data_provider.get_lodging_by_department()
        if not df_dept.empty:
            tree2 = self._create_treeview(
                dept_frame,
                columns=[
                    ("dept", "Подразделение"),
                    ("bed_days", "Койко-дней"),
                    ("amount", "Начислено, ₽"),
                    ("miss", "Без тарифа"),
                ],
                height=10,
            )
            tree2.column("dept", width=240)
            tree2.column("bed_days", width=90, anchor="e")
            tree2.column("amount", width=120, anchor="e")
            tree2.column("miss", width=90, anchor="e")
            for _, r in df_dept.iterrows():
                tree2.insert(
                    "",
                    "end",
                    values=(
                        r["department_name"],
                        int(r["bed_days"]),
                        f"{float(r['amount_rub']):,.0f}".replace(",", " "),
                        int(r["missing_rate_bed_days"]),
                    ),
                )
        else:
            ttk.Label(dept_frame, text="Нет данных.").pack(padx=10, pady=10)

        # Pivot: общежитие -> объекты (люди)
        pivot_frame = ttk.LabelFrame(self.tab_lodging, text="Общежитие → объекты (уникальные люди по TBN, pivot)")
        pivot_frame.pack(fill="both", expand=True, padx=5, pady=(0, 8))

        pv = self.data_provider.get_dorm_to_objects_people_pivot(top_objects=10, top_dorms=30)
        if pv is None or pv.empty:
            ttk.Label(pivot_frame, text="Нет данных (проверьте заполнение tbn в табелях и employees).").pack(padx=10, pady=10)
        else:
            cols = ["Общежитие"] + list(pv.columns)
            tree3 = self._create_treeview_with_hscroll(pivot_frame, columns=cols, height=12)

            for c in cols:
                tree3.heading(c, text=c)
                if c == "Общежитие":
                    tree3.column(c, width=240, anchor="w")
                else:
                    tree3.column(c, width=120, anchor="e")

            for dorm_name, row in pv.iterrows():
                values = [dorm_name] + [int(row[col]) for col in pv.columns]
                tree3.insert("", "end", values=values)
