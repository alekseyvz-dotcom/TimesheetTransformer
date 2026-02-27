import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import logging
import threading

import pandas as pd
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
import matplotlib.pyplot as plt

db_connection_pool: Optional[pool.SimpleConnectionPool] = None


def set_db_pool(db_pool: pool.SimpleConnectionPool):
    global db_connection_pool
    db_connection_pool = db_pool
    logging.info("Analytics Module: DB pool set.")


# ============================================================
#  ПАЛИТРА И СТИЛЬ
# ============================================================

PALETTE = {
    "primary":    "#1565C0",
    "success":    "#2E7D32",
    "warning":    "#E65100",
    "accent":     "#6A1B9A",
    "neutral":    "#546E7A",
    "bg_card":    "#F5F7FA",
    "positive":   "#2E7D32",
    "negative":   "#C62828",
    "text_muted": "#78909C",
    "payroll":    "#00695C",
}

SERIES_COLORS = [
    PALETTE["primary"], PALETTE["success"],
    PALETTE["warning"], PALETTE["accent"],
    "#00838F", "#AD1457", PALETTE["payroll"],
]


def apply_chart_style(ax, title: str = "", xlabel: str = "", ylabel: str = ""):
    ax.set_facecolor("#FAFAFA")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#CFD8DC")
    ax.spines["bottom"].set_color("#CFD8DC")
    ax.tick_params(colors="#546E7A", labelsize=8)
    ax.grid(axis="y", linestyle="--", alpha=0.4, color="#CFD8DC")
    if title:
        ax.set_title(title, fontsize=10, fontweight="bold", color="#263238", pad=8)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=8, color="#546E7A")
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=8, color="#546E7A")


# ============================================================
#                      DATA PROVIDER
# ============================================================

class AnalyticsData:
    def __init__(self, start_date, end_date, object_type_filter: str):
        self.start_date = start_date
        self.end_date = end_date
        self.object_type_filter = object_type_filter

    def _execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        if not db_connection_pool:
            raise ConnectionError("Пул соединений с БД не инициализирован.")
        conn = None
        try:
            conn = db_connection_pool.getconn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchall()
        except Exception as e:
            logging.exception("Analytics query error")
            messagebox.showerror("Ошибка БД", f"Ошибка аналитики:\n{e}")
            return []
        finally:
            if conn:
                db_connection_pool.putconn(conn)

    def get_object_types(self) -> List[str]:
        rows = self._execute_query("""
            SELECT DISTINCT short_name FROM objects
            WHERE short_name IS NOT NULL AND short_name <> ''
            ORDER BY short_name;
        """)
        return [r["short_name"] for r in rows]

    def _prev_period_dates(self) -> Tuple:
        delta = (self.end_date - self.start_date) + timedelta(days=1)
        prev_end = self.start_date - timedelta(days=1)
        prev_start = prev_end - delta + timedelta(days=1)
        return prev_start, prev_end

    # ----------------------------------------------------------
    #  EXECUTIVE SUMMARY
    # ----------------------------------------------------------

    def get_executive_summary(self) -> Dict[str, Any]:
        prev_start, prev_end = self._prev_period_dates()

        def _fetch(sd, ed):
            start_p = sd.year * 100 + sd.month
            end_p   = ed.year * 100 + ed.month
            rows = self._execute_query("""
                SELECT
                    (SELECT COALESCE(SUM(tr.total_hours),0)
                     FROM timesheet_headers th
                     JOIN timesheet_rows tr ON th.id = tr.header_id
                     WHERE (th.year*100+th.month) BETWEEN %s AND %s
                    )::float AS labor_hours,

                    (SELECT COALESCE(SUM(tr.overtime_day+tr.overtime_night),0)
                     FROM timesheet_headers th
                     JOIN timesheet_rows tr ON th.id = tr.header_id
                     WHERE (th.year*100+th.month) BETWEEN %s AND %s
                    )::float AS overtime_hours,

                    (SELECT COUNT(DISTINCT COALESCE(NULLIF(tr.tbn,''), tr.fio))
                     FROM timesheet_headers th
                     JOIN timesheet_rows tr ON th.id = tr.header_id
                     WHERE (th.year*100+th.month) BETWEEN %s AND %s
                    )::int AS unique_workers,

                    (SELECT COALESCE(SUM(tp.hours),0)
                     FROM transport_orders t
                     JOIN transport_order_positions tp ON t.id = tp.order_id
                     WHERE t.date BETWEEN %s AND %s
                    )::float AS machine_hours,

                    (SELECT COUNT(DISTINCT t.id)
                     FROM transport_orders t
                     WHERE t.date BETWEEN %s AND %s
                    )::int AS transport_orders,

                    (SELECT COALESCE(SUM(moi.quantity),0)
                     FROM meal_orders mo
                     JOIN meal_order_items moi ON mo.id = moi.order_id
                     WHERE mo.date BETWEEN %s AND %s
                    )::float AS meal_portions,

                    (SELECT COALESCE(SUM(COALESCE(mt.price,0)*COALESCE(moi.quantity,1)),0)
                     FROM meal_orders mo
                     JOIN meal_order_items moi ON mo.id = moi.order_id
                     LEFT JOIN meal_types mt ON mt.id = moi.meal_type_id
                     WHERE mo.date BETWEEN %s AND %s
                    )::float AS meal_cost,

                    (SELECT COUNT(*)
                     FROM dorm_stays s
                     WHERE s.check_in <= %s
                       AND (s.check_out IS NULL OR s.check_out > %s)
                    )::int AS active_residents
            """, (
                start_p, end_p,
                start_p, end_p,
                start_p, end_p,
                sd, ed,
                sd, ed,
                sd, ed,
                sd, ed,
                ed, ed,
            ))
            return rows[0] if rows else {}

        cur  = _fetch(self.start_date, self.end_date)
        prev = _fetch(prev_start, prev_end)

        def _delta(key):
            c = float(cur.get(key, 0) or 0)
            p = float(prev.get(key, 0) or 0)
            if p == 0:
                return None
            return (c - p) / p * 100.0

        result = {}
        for k in ("labor_hours", "overtime_hours", "machine_hours",
                  "meal_portions", "meal_cost", "active_residents"):
            result[k] = float(cur.get(k, 0) or 0)
        result["unique_workers"]   = int(cur.get("unique_workers", 0) or 0)
        result["transport_orders"] = int(cur.get("transport_orders", 0) or 0)

        for k in ("labor_hours", "overtime_hours", "machine_hours",
                  "meal_portions", "meal_cost"):
            result[f"delta_{k}"] = _delta(k)

        h = result["labor_hours"]
        w = result["unique_workers"]
        result["hours_per_worker"] = h / w if w > 0 else 0.0
        ot = result["overtime_hours"]
        result["overtime_pct"] = ot / h * 100 if h > 0 else 0.0
        return result

    # ----------------------------------------------------------
    #  ТРЕНД ПО МЕСЯЦАМ — ИСПРАВЛЕННЫЙ
    # ----------------------------------------------------------

    def get_monthly_trend_all(self) -> pd.DataFrame:
        """
        Возвращает DataFrame: period | labor_hours | machine_hours | meal_portions
        Нормализует значения для совместного отображения на одном графике.
        """
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month

        labor = self._execute_query("""
            SELECT th.year, th.month,
                   SUM(tr.total_hours)::float AS labor_hours
            FROM timesheet_headers th
            JOIN timesheet_rows tr ON th.id = tr.header_id
            WHERE (th.year*100+th.month) BETWEEN %s AND %s
            GROUP BY th.year, th.month
            ORDER BY th.year, th.month;
        """, (start_p, end_p))

        transport = self._execute_query("""
            SELECT EXTRACT(YEAR FROM t.date)::int  AS year,
                   EXTRACT(MONTH FROM t.date)::int AS month,
                   SUM(tp.hours)::float            AS machine_hours
            FROM transport_orders t
            JOIN transport_order_positions tp ON t.id = tp.order_id
            WHERE t.date BETWEEN %s AND %s
            GROUP BY 1, 2 ORDER BY 1, 2;
        """, (self.start_date, self.end_date))

        meals = self._execute_query("""
            SELECT EXTRACT(YEAR FROM mo.date)::int  AS year,
                   EXTRACT(MONTH FROM mo.date)::int AS month,
                   COALESCE(SUM(moi.quantity),0)::float AS meal_portions
            FROM meal_orders mo
            JOIN meal_order_items moi ON mo.id = moi.order_id
            WHERE mo.date BETWEEN %s AND %s
            GROUP BY 1, 2 ORDER BY 1, 2;
        """, (self.start_date, self.end_date))

        def to_df(rows, val_col):
            df = pd.DataFrame(rows)
            if df.empty:
                return df
            df["period"] = df.apply(
                lambda r: f"{int(r['year'])}-{int(r['month']):02d}", axis=1
            )
            return df[["period", val_col]]

        dfl = to_df(labor,     "labor_hours")
        dft = to_df(transport, "machine_hours")
        dfm = to_df(meals,     "meal_portions")

        if dfl.empty:
            return pd.DataFrame()

        df = dfl
        df = df.merge(dft, on="period", how="left") if not dft.empty else df.assign(machine_hours=0.0)
        df = df.merge(dfm, on="period", how="left") if not dfm.empty else df.assign(meal_portions=0.0)
        df = df.fillna(0)

        for c in ("labor_hours", "machine_hours", "meal_portions"):
            if c not in df.columns:
                df[c] = 0.0
        return df

    # ----------------------------------------------------------
    #  СТРУКТУРА ЗАТРАТ
    # ----------------------------------------------------------

    def get_cost_breakdown(self) -> Dict[str, float]:
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month

        payroll_rows = self._execute_query("""
            SELECT COALESCE(SUM(pr.total_accrued),0)::float AS v
            FROM payroll_uploads pu
            JOIN payroll_rows pr ON pr.upload_id = pu.id
            WHERE (pu.year*100+pu.month) BETWEEN %s AND %s;
        """, (start_p, end_p))
        payroll_amount = float((payroll_rows[0] if payroll_rows else {}).get("v", 0) or 0)

        meal_rows = self._execute_query("""
            SELECT COALESCE(SUM(COALESCE(mt.price,0)*COALESCE(moi.quantity,1)),0)::float AS v
            FROM meal_orders mo
            JOIN meal_order_items moi ON mo.id = moi.order_id
            LEFT JOIN meal_types mt ON mt.id = moi.meal_type_id
            WHERE mo.date BETWEEN %s AND %s;
        """, (self.start_date, self.end_date))
        meal_cost = float((meal_rows[0] if meal_rows else {}).get("v", 0) or 0)

        charge_rows = self._execute_query("""
            SELECT COALESCE(SUM(amount),0)::float AS v
            FROM dorm_charges
            WHERE (year*100+month) BETWEEN %s AND %s;
        """, (start_p, end_p))
        lodging_cost = float((charge_rows[0] if charge_rows else {}).get("v", 0) or 0)

        if lodging_cost == 0:
            fb_rows = self._execute_query("""
                WITH days AS (
                    SELECT generate_series(%s::date,%s::date,'1 day'::interval)::date AS d
                ),
                sod AS (SELECT dd.d,s.dorm_id,s.room_id FROM days dd
                        JOIN dorm_stays s ON s.check_in<=dd.d
                          AND (s.check_out IS NULL OR s.check_out>dd.d)),
                dm AS (SELECT id,rate_mode FROM dorms),
                rated AS (
                    SELECT CASE WHEN dm.rate_mode='PER_ROOM' THEN (
                        SELECT dr.price_per_day FROM dorm_rates dr
                        WHERE dr.room_id=sod.room_id AND dr.valid_from<=sod.d
                          AND dr.currency='RUB' ORDER BY dr.valid_from DESC LIMIT 1)
                    ELSE (SELECT dr.price_per_day FROM dorm_rates dr
                          WHERE dr.dorm_id=sod.dorm_id AND dr.valid_from<=sod.d
                          AND dr.currency='RUB' ORDER BY dr.valid_from DESC LIMIT 1)
                    END AS price_per_day
                    FROM sod JOIN dm ON dm.id=sod.dorm_id
                )
                SELECT COALESCE(SUM(COALESCE(price_per_day,0)),0)::float AS v FROM rated;
            """, (self.start_date, self.end_date))
            lodging_cost = float((fb_rows[0] if fb_rows else {}).get("v", 0) or 0)

        result = {}
        if payroll_amount > 0:
            result["ФОТ"] = payroll_amount
        if meal_cost > 0:
            result["Питание"] = meal_cost
        if lodging_cost > 0:
            result["Проживание"] = lodging_cost
        return result

    # ----------------------------------------------------------
    #  ТРУДОЗАТРАТЫ
    # ----------------------------------------------------------

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
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        params: List[Any] = [start_p, end_p]
        join_clause, filter_clause = "", ""
        if self.object_type_filter:
            join_clause   = "LEFT JOIN objects o ON th.object_db_id = o.id"
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        result = self._execute_query(
            base_query.format(join_clause=join_clause, filter_clause=filter_clause),
            tuple(params)
        )
        row = dict(result[0]) if result else {}
        total_hours    = float(row.get("total_hours", 0) or 0)
        total_overtime = float(row.get("total_overtime", 0) or 0)
        uniq           = int(row.get("unique_people_key", 0) or 0)
        row.update({
            "total_hours":        total_hours,
            "total_days":         float(row.get("total_days", 0) or 0),
            "total_overtime":     total_overtime,
            "unique_people":      uniq,
            "hours_per_person":   total_hours / uniq if uniq > 0 else 0.0,
            "overtime_share_pct": total_overtime / total_hours * 100 if total_hours > 0 else 0.0,
        })
        return row

    def get_labor_by_object(self) -> pd.DataFrame:
        base_query = """
        SELECT o.address AS object_name, SUM(tr.total_hours) AS total_hours
        FROM timesheet_headers th
        JOIN timesheet_rows tr ON th.id = tr.header_id
        LEFT JOIN objects o ON th.object_db_id = o.id
        WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
        {filter_clause}
        GROUP BY o.address HAVING o.address IS NOT NULL
        ORDER BY total_hours DESC;
        """
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        params: List[Any] = [start_p, end_p]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        data = self._execute_query(
            base_query.format(filter_clause=filter_clause), tuple(params)
        )
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"] = df["total_hours"].astype(float)
        return df

    def get_labor_trend_by_month(self) -> pd.DataFrame:
        base_query = """
        SELECT th.year, th.month, SUM(tr.total_hours) AS total_hours
        FROM timesheet_headers th
        JOIN timesheet_rows tr ON th.id = tr.header_id
        {join_clause}
        WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
        {filter_clause}
        GROUP BY th.year, th.month ORDER BY th.year, th.month;
        """
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        params: List[Any] = [start_p, end_p]
        join_clause, filter_clause = "", ""
        if self.object_type_filter:
            join_clause   = "LEFT JOIN objects o ON th.object_db_id = o.id"
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        data = self._execute_query(
            base_query.format(join_clause=join_clause, filter_clause=filter_clause),
            tuple(params)
        )
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"] = df["total_hours"].astype(float)
        return df

    def get_top_employees_by_hours(self, limit: int = 10) -> pd.DataFrame:
        base_query = """
        SELECT tr.fio,
               COALESCE(SUM(tr.total_hours), 0) AS total_hours,
               COALESCE(SUM(tr.overtime_day + tr.overtime_night), 0) AS total_overtime
        FROM timesheet_headers th
        JOIN timesheet_rows tr ON th.id = tr.header_id
        {join_clause}
        WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
        {filter_clause}
        GROUP BY tr.fio ORDER BY total_hours DESC LIMIT {limit};
        """
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        params: List[Any] = [start_p, end_p]
        join_clause, filter_clause = "", ""
        if self.object_type_filter:
            join_clause   = "LEFT JOIN objects o ON th.object_db_id = o.id"
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        data = self._execute_query(
            base_query.format(join_clause=join_clause, filter_clause=filter_clause, limit=limit),
            tuple(params)
        )
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"]    = df["total_hours"].astype(float)
            df["total_overtime"] = df["total_overtime"].astype(float)
        return df

    def get_labor_by_department(self) -> pd.DataFrame:
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
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        params: List[Any] = [start_p, end_p]
        join_clause, filter_clause = "", ""
        if self.object_type_filter:
            join_clause   = "LEFT JOIN objects o ON th.object_db_id = o.id"
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        data = self._execute_query(
            base_query.format(join_clause=join_clause, filter_clause=filter_clause),
            tuple(params)
        )
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"]     = df["total_hours"].astype(float)
            df["people_cnt"]      = df["people_cnt"].astype(int)
            df["department_name"] = df["department_name"].fillna("—")
        return df

    # ----------------------------------------------------------
    #  ТРАНСПОРТ
    # ----------------------------------------------------------

    def get_transport_kpi(self) -> Dict[str, Any]:
        base_query = """
        SELECT
            COALESCE(SUM(tp.hours), 0) AS total_machine_hours,
            COUNT(DISTINCT t.id)       AS total_orders,
            COALESCE(SUM(tp.qty), 0)   AS total_units
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
        result = self._execute_query(
            base_query.format(filter_clause=filter_clause), tuple(params)
        )
        kpi = dict(result[0]) if result else {}
        total_hours  = float(kpi.get("total_machine_hours", 0) or 0)
        total_orders = int(kpi.get("total_orders", 0) or 0)
        total_units  = float(kpi.get("total_units", 0) or 0)
        kpi.update({
            "total_machine_hours": total_hours,
            "total_orders":        total_orders,
            "total_units":         total_units,
            "avg_hours_per_order": total_hours / total_orders if total_orders > 0 else 0.0,
            "hours_per_unit":      total_hours / total_units  if total_units  > 0 else 0.0,
        })
        return kpi

    def get_transport_by_tech(self) -> pd.DataFrame:
        base_query = """
        SELECT tp.tech, SUM(tp.hours) AS total_hours,
               COUNT(DISTINCT t.id) AS order_count
        FROM transport_orders t
        JOIN transport_order_positions tp ON t.id = tp.order_id
        LEFT JOIN objects o ON t.object_id = o.id
        WHERE t.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY tp.tech ORDER BY total_hours DESC;
        """
        params: List[Any] = [self.start_date, self.end_date]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        data = self._execute_query(
            base_query.format(filter_clause=filter_clause), tuple(params)
        )
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"] = df["total_hours"].astype(float)
            df["order_count"] = df["order_count"].astype(int)
        return df

    def get_transport_by_object(self, limit: int = 10) -> pd.DataFrame:
        base_query = """
        SELECT o.address AS object_name,
               SUM(tp.hours)::float AS total_hours,
               COUNT(DISTINCT t.id)::int AS order_count
        FROM transport_orders t
        JOIN transport_order_positions tp ON t.id = tp.order_id
        LEFT JOIN objects o ON t.object_id = o.id
        WHERE t.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY o.address HAVING o.address IS NOT NULL
        ORDER BY total_hours DESC LIMIT {limit};
        """
        params: List[Any] = [self.start_date, self.end_date]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        data = self._execute_query(
            base_query.format(filter_clause=filter_clause, limit=limit), tuple(params)
        )
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"]  = df["total_hours"].astype(float)
            df["order_count"]  = df["order_count"].astype(int)
            df["object_name"]  = df["object_name"].fillna("—")
        return df

    def get_transport_trend(self) -> pd.DataFrame:
        rows = self._execute_query("""
            SELECT EXTRACT(YEAR FROM t.date)::int  AS year,
                   EXTRACT(MONTH FROM t.date)::int AS month,
                   SUM(tp.hours)::float            AS total_hours,
                   COUNT(DISTINCT t.id)::int       AS order_count
            FROM transport_orders t
            JOIN transport_order_positions tp ON t.id = tp.order_id
            WHERE t.date BETWEEN %s AND %s
            GROUP BY 1, 2 ORDER BY 1, 2;
        """, (self.start_date, self.end_date))
        df = pd.DataFrame(rows)
        if not df.empty:
            df["period"] = df.apply(
                lambda r: f"{int(r['year'])}-{int(r['month']):02d}", axis=1
            )
        return df

    def get_transport_fulfillment_summary(self) -> Dict[str, Any]:
        rows = self._execute_query("""
            SELECT
                COUNT(*)::int                                    AS total_positions,
                SUM(tp.hours)::float                             AS total_hours,
                SUM(CASE WHEN tp.status='done'
                         THEN 1 ELSE 0 END)::int                 AS done_cnt,
                SUM(CASE WHEN tp.status IN ('canceled','cancelled')
                         THEN 1 ELSE 0 END)::int                 AS canceled_cnt,
                SUM(CASE WHEN NULLIF(tp.assigned_vehicle,'') IS NOT NULL
                         THEN 1 ELSE 0 END)::int                 AS assigned_cnt,
                COUNT(DISTINCT NULLIF(tp.driver,''))::int        AS unique_drivers
            FROM transport_orders t
            JOIN transport_order_positions tp ON t.id = tp.order_id
            WHERE t.date BETWEEN %s AND %s;
        """, (self.start_date, self.end_date))
        row = dict(rows[0]) if rows else {}
        total  = int(row.get("total_positions", 0) or 0)
        done   = int(row.get("done_cnt", 0) or 0)
        cancel = int(row.get("canceled_cnt", 0) or 0)
        row["total_positions"]  = total
        row["done_cnt"]         = done
        row["canceled_cnt"]     = cancel
        row["total_hours"]      = float(row.get("total_hours", 0) or 0)
        row["assigned_cnt"]     = int(row.get("assigned_cnt", 0) or 0)
        row["unique_drivers"]   = int(row.get("unique_drivers", 0) or 0)
        row["fulfillment_pct"]  = done / total * 100 if total > 0 else 0.0
        row["cancellation_pct"] = cancel / total * 100 if total > 0 else 0.0
        return row

    # ----------------------------------------------------------
    #  ПИТАНИЕ
    # ----------------------------------------------------------

    def get_meals_kpi(self) -> Dict[str, Any]:
        base_query = """
        SELECT COUNT(moi.id)                   AS total_portions_rows,
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
        result = self._execute_query(
            base_query.format(filter_clause=filter_clause), tuple(params)
        )
        row = dict(result[0]) if result else {}
        total_qty    = float(row.get("total_portions_qty", 0) or 0)
        total_orders = int(row.get("total_orders", 0) or 0)
        unique_emp   = int(row.get("unique_employees", 0) or 0)
        row.update({
            "total_portions_qty":      total_qty,
            "total_orders":            total_orders,
            "unique_employees":        unique_emp,
            "avg_portions_per_order":  total_qty / total_orders if total_orders > 0 else 0.0,
            "avg_portions_per_person": total_qty / unique_emp   if unique_emp   > 0 else 0.0,
        })
        return row

    def get_meals_cost_kpi(self) -> Dict[str, Any]:
        base_query = """
        SELECT COALESCE(SUM(COALESCE(mt.price,0)*COALESCE(moi.quantity,1)),0)::numeric AS total_cost
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
        rows = self._execute_query(
            base_query.format(filter_clause=filter_clause), tuple(params)
        )
        row = rows[0] if rows else {}
        return {"total_cost": float(row.get("total_cost", 0) or 0)}

    def get_meals_by_type(self) -> pd.DataFrame:
        base_query = """
        SELECT moi.meal_type_text,
               COALESCE(SUM(moi.quantity), 0) AS total_qty
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        LEFT JOIN objects o ON mo.object_id = o.id
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY moi.meal_type_text ORDER BY total_qty DESC;
        """
        params: List[Any] = [self.start_date, self.end_date]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        data = self._execute_query(
            base_query.format(filter_clause=filter_clause), tuple(params)
        )
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_qty"] = df["total_qty"].astype(float)
        return df

    def get_meals_trend_by_month(self) -> pd.DataFrame:
        base_query = """
        SELECT date_trunc('month', mo.date) AS period,
               COALESCE(SUM(moi.quantity),0) AS total_qty
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        LEFT JOIN objects o ON mo.object_id = o.id
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY period ORDER BY period;
        """
        params: List[Any] = [self.start_date, self.end_date]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        data = self._execute_query(
            base_query.format(filter_clause=filter_clause), tuple(params)
        )
        return pd.DataFrame(data)

    def get_meals_by_object(self, limit: int = 10) -> pd.DataFrame:
        base_query = """
        SELECT o.address AS object_name,
               COALESCE(SUM(moi.quantity),0) AS total_qty,
               COUNT(DISTINCT moi.employee_id) AS unique_employees
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        LEFT JOIN objects o ON mo.object_id = o.id
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY o.address HAVING o.address IS NOT NULL
        ORDER BY total_qty DESC LIMIT {limit};
        """
        params: List[Any] = [self.start_date, self.end_date]
        filter_clause = ""
        if self.object_type_filter:
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        data = self._execute_query(
            base_query.format(filter_clause=filter_clause, limit=limit), tuple(params)
        )
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_qty"]        = df["total_qty"].astype(float)
            df["unique_employees"] = df["unique_employees"].astype(int)
        return df

    def get_meals_by_department(self) -> pd.DataFrame:
        rows = self._execute_query("""
            SELECT COALESCE(d.name,'—') AS department_name,
                   COALESCE(SUM(moi.quantity),0) AS total_qty,
                   COUNT(DISTINCT moi.employee_id) AS unique_employees
            FROM meal_orders mo
            JOIN meal_order_items moi ON mo.id = moi.order_id
            LEFT JOIN departments d ON mo.department_id = d.id
            WHERE mo.date BETWEEN %s AND %s
            GROUP BY d.name ORDER BY total_qty DESC;
        """, (self.start_date, self.end_date))
        df = pd.DataFrame(rows)
        if not df.empty:
            df["total_qty"]        = df["total_qty"].astype(float)
            df["unique_employees"] = df["unique_employees"].astype(int)
        return df

    # ----------------------------------------------------------
    #  ОБЪЕКТЫ
    # ----------------------------------------------------------

    def get_objects_rating(self, limit: int = 15) -> pd.DataFrame:
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        type_filter = ""
        params: List[Any] = [start_p, end_p, self.start_date, self.end_date,
                              self.start_date, self.end_date]
        if self.object_type_filter:
            type_filter = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        params.append(limit)

        rows = self._execute_query(f"""
            WITH labor AS (
                SELECT th.object_db_id AS oid,
                       SUM(tr.total_hours) AS labor_hours,
                       COUNT(DISTINCT COALESCE(NULLIF(tr.tbn,''), tr.fio)) AS workers
                FROM timesheet_headers th
                JOIN timesheet_rows tr ON th.id = tr.header_id
                WHERE (th.year*100+th.month) BETWEEN %s AND %s
                GROUP BY th.object_db_id
            ),
            transport AS (
                SELECT t.object_id AS oid, SUM(tp.hours) AS machine_hours,
                       COUNT(DISTINCT t.id) AS transport_cnt
                FROM transport_orders t
                JOIN transport_order_positions tp ON t.id = tp.order_id
                WHERE t.date BETWEEN %s AND %s
                GROUP BY t.object_id
            ),
            meals AS (
                SELECT mo.object_id AS oid,
                       COALESCE(SUM(moi.quantity),0) AS portions,
                       COALESCE(SUM(COALESCE(mt.price,0)*COALESCE(moi.quantity,1)),0) AS meal_cost
                FROM meal_orders mo
                JOIN meal_order_items moi ON mo.id = moi.order_id
                LEFT JOIN meal_types mt ON mt.id = moi.meal_type_id
                WHERE mo.date BETWEEN %s AND %s
                GROUP BY mo.object_id
            )
            SELECT o.address,
                   COALESCE(l.labor_hours,0)::float    AS labor_hours,
                   COALESCE(l.workers,0)::int           AS workers,
                   COALESCE(t.machine_hours,0)::float   AS machine_hours,
                   COALESCE(t.transport_cnt,0)::int     AS transport_cnt,
                   COALESCE(m.portions,0)::float        AS portions,
                   COALESCE(m.meal_cost,0)::float       AS meal_cost
            FROM objects o
            LEFT JOIN labor l     ON o.id = l.oid
            LEFT JOIN transport t ON o.id = t.oid
            LEFT JOIN meals m     ON o.id = m.oid
            WHERE COALESCE(l.labor_hours,0)+COALESCE(t.machine_hours,0)+COALESCE(m.portions,0) > 0
            {type_filter}
            ORDER BY labor_hours DESC
            LIMIT %s;
        """, tuple(params))

        df = pd.DataFrame(rows)
        if not df.empty:
            for c in ("labor_hours", "machine_hours", "portions", "meal_cost"):
                df[c] = df[c].astype(float)
            for c in ("workers", "transport_cnt"):
                df[c] = df[c].astype(int)
            df["address"] = df["address"].fillna("—")
        return df

    def get_objects_overview(self, limit: int = 20) -> pd.DataFrame:
        return self.get_objects_rating(limit=limit)

    def get_objects_by_status(self) -> pd.DataFrame:
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        rows = self._execute_query("""
            SELECT COALESCE(NULLIF(o.status,''), '—') AS status,
                   COUNT(DISTINCT o.id)::int           AS objects_count,
                   COALESCE(SUM(l.labor_hours),0)::float AS labor_hours
            FROM objects o
            LEFT JOIN (
                SELECT th.object_db_id AS oid, SUM(tr.total_hours) AS labor_hours
                FROM timesheet_headers th
                JOIN timesheet_rows tr ON th.id = tr.header_id
                WHERE (th.year*100+th.month) BETWEEN %s AND %s
                GROUP BY th.object_db_id
            ) l ON l.oid = o.id
            GROUP BY COALESCE(NULLIF(o.status,''), '—')
            ORDER BY labor_hours DESC;
        """, (start_p, end_p))
        df = pd.DataFrame(rows)
        if not df.empty:
            df["labor_hours"]   = df["labor_hours"].astype(float)
            df["objects_count"] = df["objects_count"].astype(int)
        return df

    # ----------------------------------------------------------
    #  ПОЛЬЗОВАТЕЛИ
    # ----------------------------------------------------------

    def get_users_activity(self) -> pd.DataFrame:
        query = """
        SELECT u.username, u.full_name,
               COALESCE(th_cnt,0)  AS timesheets_created,
               COALESCE(trp_cnt,0) AS transport_orders_created,
               COALESCE(mo_cnt,0)  AS meal_orders_created,
               COALESCE(ci_cnt,0)  AS dorm_checkins,
               COALESCE(co_cnt,0)  AS dorm_checkouts
        FROM app_users u
        LEFT JOIN (SELECT user_id, COUNT(*) AS th_cnt FROM timesheet_headers
                   WHERE created_at::date BETWEEN %s AND %s GROUP BY user_id) th ON u.id=th.user_id
        LEFT JOIN (SELECT user_id, COUNT(*) AS trp_cnt FROM transport_orders
                   WHERE date BETWEEN %s AND %s GROUP BY user_id) to2 ON u.id=to2.user_id
        LEFT JOIN (SELECT user_id, COUNT(*) AS mo_cnt FROM meal_orders
                   WHERE date BETWEEN %s AND %s GROUP BY user_id) mo ON u.id=mo.user_id
        LEFT JOIN (SELECT created_by AS user_id, COUNT(*) AS ci_cnt FROM dorm_stays
                   WHERE check_in BETWEEN %s AND %s AND created_by IS NOT NULL
                   GROUP BY created_by) ci ON u.id=ci.user_id
        LEFT JOIN (SELECT closed_by AS user_id, COUNT(*) AS co_cnt FROM dorm_stays
                   WHERE check_out BETWEEN %s AND %s AND closed_by IS NOT NULL
                   GROUP BY closed_by) co ON u.id=co.user_id
        WHERE u.is_active=TRUE
        ORDER BY (COALESCE(th_cnt,0)+COALESCE(trp_cnt,0)+COALESCE(mo_cnt,0)
                  +COALESCE(ci_cnt,0)+COALESCE(co_cnt,0)) DESC;
        """
        params = (
            self.start_date, self.end_date,
            self.start_date, self.end_date,
            self.start_date, self.end_date,
            self.start_date, self.end_date,
            self.start_date, self.end_date,
        )
        data = self._execute_query(query, params)
        df = pd.DataFrame(data)
        if not df.empty:
            for col in ("timesheets_created", "transport_orders_created",
                        "meal_orders_created", "dorm_checkins", "dorm_checkouts"):
                df[col] = df[col].fillna(0).astype(int)
        return df

    # ----------------------------------------------------------
    #  ПРОЖИВАНИЕ
    # ----------------------------------------------------------

    def get_lodging_kpi(self) -> Dict[str, Any]:
        query = """
        WITH days AS (SELECT generate_series(%s::date,%s::date,'1 day'::interval)::date AS d),
        sod AS (
            SELECT dd.d, s.employee_id, s.dorm_id, s.room_id
            FROM days dd JOIN dorm_stays s ON s.check_in<=dd.d
              AND (s.check_out IS NULL OR s.check_out>dd.d)
        ),
        dm AS (SELECT id, rate_mode FROM dorms),
        rod AS (
            SELECT sod.d, sod.employee_id,
                CASE WHEN dm.rate_mode='PER_ROOM' THEN (
                    SELECT dr.price_per_day FROM dorm_rates dr
                    WHERE dr.room_id=sod.room_id AND dr.valid_from<=sod.d
                      AND dr.currency='RUB' ORDER BY dr.valid_from DESC LIMIT 1)
                ELSE (SELECT dr.price_per_day FROM dorm_rates dr
                      WHERE dr.dorm_id=sod.dorm_id AND dr.valid_from<=sod.d
                      AND dr.currency='RUB' ORDER BY dr.valid_from DESC LIMIT 1)
                END AS price_per_day
            FROM sod JOIN dm ON dm.id=sod.dorm_id
        )
        SELECT COUNT(*)::int AS bed_days,
               COALESCE(SUM(COALESCE(price_per_day,0)),0)::numeric AS amount_rub,
               COALESCE(AVG(price_per_day),0)::numeric AS avg_price_rub,
               (SELECT COUNT(*) FROM dorm_stays s2
                WHERE s2.check_in<=%s AND (s2.check_out IS NULL OR s2.check_out>%s)
               )::int AS active_on_end,
               (SELECT COUNT(*) FROM rod WHERE price_per_day IS NULL)::int AS missing_rate_bed_days
        FROM rod;
        """
        rows = self._execute_query(query, (self.start_date, self.end_date,
                                           self.end_date, self.end_date))
        row = dict(rows[0]) if rows else {}
        for k in ("bed_days", "active_on_end", "missing_rate_bed_days"):
            row[k] = int(row.get(k, 0) or 0)
        for k in ("amount_rub", "avg_price_rub"):
            row[k] = float(row.get(k, 0) or 0)
        return row

    def get_lodging_daily_occupancy(self) -> pd.DataFrame:
        rows = self._execute_query("""
            WITH days AS (SELECT generate_series(%s::date,%s::date,'1 day'::interval)::date AS d)
            SELECT dd.d,
                   (SELECT COUNT(*) FROM dorm_stays s
                    WHERE s.check_in<=dd.d AND (s.check_out IS NULL OR s.check_out>dd.d)
                   )::int AS occupied_beds
            FROM days dd ORDER BY dd.d;
        """, (self.start_date, self.end_date))
        df = pd.DataFrame(rows)
        if not df.empty:
            df["occupied_beds"] = df["occupied_beds"].astype(int)
        return df

    def get_lodging_by_dorm(self, limit: int = 10) -> pd.DataFrame:
        query = """
        WITH days AS (SELECT generate_series(%s::date,%s::date,'1 day'::interval)::date AS d),
        sod AS (SELECT dd.d,s.dorm_id,s.room_id FROM days dd JOIN dorm_stays s
                ON s.check_in<=dd.d AND (s.check_out IS NULL OR s.check_out>dd.d)),
        dm AS (SELECT id,rate_mode,name FROM dorms),
        rated AS (
            SELECT sod.d, sod.dorm_id,
                CASE WHEN dm.rate_mode='PER_ROOM' THEN (
                    SELECT dr.price_per_day FROM dorm_rates dr WHERE dr.room_id=sod.room_id
                      AND dr.valid_from<=sod.d AND dr.currency='RUB' ORDER BY dr.valid_from DESC LIMIT 1)
                ELSE (SELECT dr.price_per_day FROM dorm_rates dr WHERE dr.dorm_id=sod.dorm_id
                      AND dr.valid_from<=sod.d AND dr.currency='RUB' ORDER BY dr.valid_from DESC LIMIT 1)
                END AS price_per_day
            FROM sod JOIN dm ON dm.id=sod.dorm_id
        )
        SELECT dm.name AS dorm_name, COUNT(*)::int AS bed_days,
               COALESCE(SUM(COALESCE(r.price_per_day,0)),0)::numeric AS amount_rub,
               COALESCE(AVG(r.price_per_day),0)::numeric AS avg_price_rub,
               SUM(CASE WHEN r.price_per_day IS NULL THEN 1 ELSE 0 END)::int AS missing_rate_bed_days
        FROM rated r JOIN dm ON dm.id=r.dorm_id
        GROUP BY dm.name ORDER BY amount_rub DESC LIMIT %s;
        """
        data = self._execute_query(query, (self.start_date, self.end_date, limit))
        df = pd.DataFrame(data)
        if not df.empty:
            df["bed_days"]              = df["bed_days"].astype(int)
            df["amount_rub"]            = df["amount_rub"].astype(float)
            df["avg_price_rub"]         = df["avg_price_rub"].astype(float)
            df["missing_rate_bed_days"] = df["missing_rate_bed_days"].astype(int)
        return df

    def get_lodging_by_department(self) -> pd.DataFrame:
        query = """
        WITH days AS (SELECT generate_series(%s::date,%s::date,'1 day'::interval)::date AS d),
        sod AS (SELECT dd.d,s.employee_id,s.dorm_id,s.room_id FROM days dd JOIN dorm_stays s
                ON s.check_in<=dd.d AND (s.check_out IS NULL OR s.check_out>dd.d)),
        dm AS (SELECT id,rate_mode FROM dorms),
        rated AS (
            SELECT sod.d, sod.employee_id,
                CASE WHEN dm.rate_mode='PER_ROOM' THEN (
                    SELECT dr.price_per_day FROM dorm_rates dr WHERE dr.room_id=sod.room_id
                      AND dr.valid_from<=sod.d AND dr.currency='RUB' ORDER BY dr.valid_from DESC LIMIT 1)
                ELSE (SELECT dr.price_per_day FROM dorm_rates dr WHERE dr.dorm_id=sod.dorm_id
                      AND dr.valid_from<=sod.d AND dr.currency='RUB' ORDER BY dr.valid_from DESC LIMIT 1)
                END AS price_per_day
            FROM sod JOIN dm ON dm.id=sod.dorm_id
        )
        SELECT COALESCE(dep.name,'—') AS department_name,
               COUNT(*)::int AS bed_days,
               COALESCE(SUM(COALESCE(r.price_per_day,0)),0)::numeric AS amount_rub,
               SUM(CASE WHEN r.price_per_day IS NULL THEN 1 ELSE 0 END)::int AS missing_rate_bed_days
        FROM rated r JOIN employees e ON e.id=r.employee_id
        LEFT JOIN departments dep ON dep.id=e.department_id
        GROUP BY dep.name ORDER BY amount_rub DESC;
        """
        data = self._execute_query(query, (self.start_date, self.end_date))
        df = pd.DataFrame(data)
        if not df.empty:
            df["bed_days"]              = df["bed_days"].astype(int)
            df["amount_rub"]            = df["amount_rub"].astype(float)
            df["missing_rate_bed_days"] = df["missing_rate_bed_days"].astype(int)
        return df

    def get_lodging_charges_kpi(self) -> Dict[str, Any]:
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        rows = self._execute_query("""
            SELECT COALESCE(SUM(dc.amount),0)::float           AS total_amount,
                   COALESCE(SUM(dc.days),0)::int               AS total_days,
                   COUNT(DISTINCT dc.stay_id)::int             AS unique_stays,
                   COALESCE(AVG(dc.avg_price_per_day),0)::float AS avg_price,
                   COUNT(DISTINCT ds.employee_id)::int          AS unique_employees,
                   COUNT(DISTINCT ds.dorm_id)::int              AS dorms_used
            FROM dorm_charges dc
            JOIN dorm_stays ds ON ds.id = dc.stay_id
            WHERE (dc.year * 100 + dc.month) BETWEEN %s AND %s;
        """, (start_p, end_p))
        row = dict(rows[0]) if rows else {}
        total_amount = float(row.get("total_amount", 0) or 0)
        row["total_amount"]     = total_amount
        row["total_days"]       = int(row.get("total_days", 0) or 0)
        row["unique_stays"]     = int(row.get("unique_stays", 0) or 0)
        row["avg_price"]        = float(row.get("avg_price", 0) or 0)
        row["unique_employees"] = int(row.get("unique_employees", 0) or 0)
        row["dorms_used"]       = int(row.get("dorms_used", 0) or 0)
        row["avg_per_employee"] = (
            total_amount / row["unique_employees"] if row["unique_employees"] > 0 else 0.0
        )
        return row

    def get_dorm_to_objects_people_pivot(self, top_objects: int = 10, top_dorms: int = 30) -> pd.DataFrame:
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        obj_filter = ""
        params: List[Any] = [start_p, end_p]
        if self.object_type_filter:
            obj_filter = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        query = f"""
        WITH ts AS (
            SELECT th.year, th.month, th.object_db_id AS object_id, tr.tbn,
                   make_date(th.year,th.month,1)::date AS month_date
            FROM timesheet_headers th
            JOIN timesheet_rows tr ON tr.header_id=th.id
            LEFT JOIN objects o ON o.id=th.object_db_id
            WHERE (th.year*100+th.month) BETWEEN %s AND %s
              AND COALESCE(tr.tbn,'') <> ''
              {obj_filter}
            GROUP BY th.year, th.month, th.object_db_id, tr.tbn
        ),
        ts_emp AS (
            SELECT ts.object_id, ts.month_date, e.id AS employee_id, ts.tbn
            FROM ts JOIN employees e ON COALESCE(e.tbn,'') <> '' AND e.tbn=ts.tbn
        ),
        ts_dorm AS (
            SELECT te.object_id,
                (SELECT d.name FROM dorm_stays s JOIN dorms d ON d.id=s.dorm_id
                 WHERE s.employee_id=te.employee_id AND s.check_in<=te.month_date
                   AND (s.check_out IS NULL OR s.check_out>te.month_date)
                 ORDER BY s.check_in DESC LIMIT 1) AS dorm_name,
                te.tbn
            FROM ts_emp te
        )
        SELECT COALESCE(td.dorm_name,'— (без проживания)') AS dorm_name,
               COALESCE(o.address,'—') AS object_name,
               COUNT(DISTINCT td.tbn)::int AS people_cnt
        FROM ts_dorm td LEFT JOIN objects o ON o.id=td.object_id
        GROUP BY dorm_name, object_name ORDER BY people_cnt DESC;
        """
        data = self._execute_query(query, tuple(params))
        df = pd.DataFrame(data)
        if df.empty:
            return df
        df["people_cnt"]  = df["people_cnt"].astype(int)
        df["dorm_name"]   = df["dorm_name"].fillna("—")
        df["object_name"] = df["object_name"].fillna("—")

        dorm_order = (df.groupby("dorm_name")["people_cnt"].sum()
                        .sort_values(ascending=False).head(top_dorms).index.tolist())
        df = df[df["dorm_name"].isin(dorm_order)]
        obj_order = (df.groupby("object_name")["people_cnt"].sum()
                       .sort_values(ascending=False).head(top_objects).index.tolist())
        df = df[df["object_name"].isin(obj_order)]

        pv = df.pivot_table(index="dorm_name", columns="object_name",
                            values="people_cnt", aggfunc="sum", fill_value=0)
        pv["ИТОГО"] = pv.sum(axis=1)
        return pv.sort_values("ИТОГО", ascending=False)

    # ----------------------------------------------------------
    #  ФОТ
    # ----------------------------------------------------------

    def get_payroll_kpi(self) -> Dict[str, Any]:
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        rows = self._execute_query("""
            SELECT
                COUNT(DISTINCT pu.id)::int               AS uploads_count,
                COUNT(DISTINCT pr.employee_id)::int       AS unique_employees,
                COALESCE(SUM(pr.total_accrued),0)::float  AS total_accrued,
                COALESCE(AVG(pr.total_accrued),0)::float  AS avg_accrued,
                COALESCE(SUM(pr.worked_hours),0)::float   AS total_worked_hours,
                COALESCE(SUM(pr.rwv_hours),0)::float      AS total_rwv_hours,
                COALESCE(SUM(pr.worked_days),0)::int      AS total_worked_days
            FROM payroll_uploads pu
            JOIN payroll_rows pr ON pr.upload_id = pu.id
            WHERE (pu.year * 100 + pu.month) BETWEEN %s AND %s;
        """, (start_p, end_p))
        row = dict(rows[0]) if rows else {}
        total_accrued      = float(row.get("total_accrued", 0) or 0)
        total_worked_hours = float(row.get("total_worked_hours", 0) or 0)
        unique_emp         = int(row.get("unique_employees", 0) or 0)
        total_rwv          = float(row.get("total_rwv_hours", 0) or 0)
        row.update({
            "total_accrued":      total_accrued,
            "total_worked_hours": total_worked_hours,
            "total_rwv_hours":    total_rwv,
            "unique_employees":   unique_emp,
            "avg_accrued":        float(row.get("avg_accrued", 0) or 0),
            "cost_per_hour":      total_accrued / total_worked_hours if total_worked_hours > 0 else 0.0,
            "rwv_share_pct":      total_rwv / total_worked_hours * 100 if total_worked_hours > 0 else 0.0,
        })
        return row

    def get_payroll_by_object(self, limit: int = 15) -> pd.DataFrame:
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        type_filter = ""
        params: List[Any] = [start_p, end_p]
        if self.object_type_filter:
            type_filter = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        params.append(limit)
        rows = self._execute_query(f"""
            SELECT o.address                               AS object_name,
                   COALESCE(SUM(pd.amount),0)::float       AS payroll_amount,
                   COUNT(DISTINCT pr.employee_id)::int     AS employee_count,
                   COALESCE(SUM(pd.hours_on_object),0)::float AS hours_on_object
            FROM payroll_distribution pd
            JOIN payroll_rows pr     ON pr.id  = pd.payroll_row_id
            JOIN payroll_uploads pu  ON pu.id  = pr.upload_id
            JOIN objects o           ON o.id   = pd.object_id
            WHERE (pu.year * 100 + pu.month) BETWEEN %s AND %s
              {type_filter}
            GROUP BY o.address
            ORDER BY payroll_amount DESC
            LIMIT %s;
        """, tuple(params))
        df = pd.DataFrame(rows)
        if not df.empty:
            df["payroll_amount"]  = df["payroll_amount"].astype(float)
            df["employee_count"]  = df["employee_count"].astype(int)
            df["hours_on_object"] = df["hours_on_object"].astype(float)
            df["object_name"]     = df["object_name"].fillna("—")
            df["cost_per_hour"]   = df.apply(
                lambda r: r["payroll_amount"] / r["hours_on_object"]
                if r["hours_on_object"] > 0 else 0.0, axis=1
            )
        return df

    def get_payroll_by_department(self) -> pd.DataFrame:
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        rows = self._execute_query("""
            SELECT COALESCE(NULLIF(pr.department_raw,''), '—') AS department_name,
                   COALESCE(SUM(pr.total_accrued),0)::float    AS total_accrued,
                   COUNT(DISTINCT pr.employee_id)::int          AS employee_count,
                   COALESCE(AVG(pr.total_accrued),0)::float    AS avg_accrued,
                   COALESCE(SUM(pr.worked_hours),0)::float      AS worked_hours
            FROM payroll_uploads pu
            JOIN payroll_rows pr ON pr.upload_id = pu.id
            WHERE (pu.year * 100 + pu.month) BETWEEN %s AND %s
            GROUP BY COALESCE(NULLIF(pr.department_raw,''), '—')
            ORDER BY total_accrued DESC;
        """, (start_p, end_p))
        df = pd.DataFrame(rows)
        if not df.empty:
            for c in ("total_accrued", "avg_accrued", "worked_hours"):
                df[c] = df[c].astype(float)
            df["employee_count"] = df["employee_count"].astype(int)
        return df

    def get_payroll_trend(self) -> pd.DataFrame:
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        rows = self._execute_query("""
            SELECT pu.year, pu.month,
                   COALESCE(SUM(pr.total_accrued),0)::float   AS total_accrued,
                   COUNT(DISTINCT pr.employee_id)::int         AS employee_count,
                   COALESCE(AVG(pr.total_accrued),0)::float   AS avg_accrued
            FROM payroll_uploads pu
            JOIN payroll_rows pr ON pr.upload_id = pu.id
            WHERE (pu.year * 100 + pu.month) BETWEEN %s AND %s
            GROUP BY pu.year, pu.month
            ORDER BY pu.year, pu.month;
        """, (start_p, end_p))
        df = pd.DataFrame(rows)
        if not df.empty:
            df["period"]       = df.apply(
                lambda r: f"{int(r['year'])}-{int(r['month']):02d}", axis=1
            )
            df["total_accrued"]  = df["total_accrued"].astype(float)
            df["avg_accrued"]    = df["avg_accrued"].astype(float)
            df["employee_count"] = df["employee_count"].astype(int)
        return df

    def get_payroll_by_position(self) -> pd.DataFrame:
        """
        ФОТ по должностям — без персональных данных.
        Показывает среднее/мин/макс начисление по должности.
        """
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        rows = self._execute_query("""
            SELECT
                COALESCE(NULLIF(pr.position_raw,''), '—')  AS position_name,
                COUNT(DISTINCT pr.employee_id)::int          AS employee_count,
                COALESCE(SUM(pr.total_accrued),0)::float    AS total_accrued,
                COALESCE(AVG(pr.total_accrued),0)::float    AS avg_accrued,
                COALESCE(MIN(pr.total_accrued),0)::float    AS min_accrued,
                COALESCE(MAX(pr.total_accrued),0)::float    AS max_accrued,
                COALESCE(SUM(pr.worked_hours),0)::float     AS worked_hours,
                COALESCE(SUM(pr.rwv_hours),0)::float        AS rwv_hours
            FROM payroll_uploads pu
            JOIN payroll_rows pr ON pr.upload_id = pu.id
            WHERE (pu.year * 100 + pu.month) BETWEEN %s AND %s
            GROUP BY COALESCE(NULLIF(pr.position_raw,''), '—')
            ORDER BY total_accrued DESC;
        """, (start_p, end_p))
        df = pd.DataFrame(rows)
        if not df.empty:
            for c in ("total_accrued", "avg_accrued",
                      "min_accrued", "max_accrued",
                      "worked_hours", "rwv_hours"):
                df[c] = df[c].astype(float)
            df["employee_count"] = df["employee_count"].astype(int)
        return df

    def get_payroll_stats_summary(self) -> pd.DataFrame:
        """
        Сводная статистика ФОТ БЕЗ персональных данных:
        только агрегаты по подразделению — кол-во людей,
        общий ФОТ, среднее, медиана, кол-во с RWV.
        """
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month
        rows = self._execute_query("""
            SELECT
                COALESCE(NULLIF(pr.department_raw,''), '—')  AS department_name,
                COUNT(DISTINCT pr.employee_id)::int            AS employee_count,
                COALESCE(SUM(pr.total_accrued),0)::float      AS total_accrued,
                COALESCE(AVG(pr.total_accrued),0)::float      AS avg_accrued,
                COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP
                    (ORDER BY pr.total_accrued),0)::float      AS median_accrued,
                COALESCE(SUM(pr.worked_hours),0)::float        AS worked_hours,
                COALESCE(SUM(pr.rwv_hours),0)::float           AS rwv_hours,
                SUM(CASE WHEN COALESCE(pr.rwv_hours,0) > 0
                         THEN 1 ELSE 0 END)::int               AS rwv_employees_cnt,
                COALESCE(SUM(pr.worked_days),0)::int           AS worked_days
            FROM payroll_uploads pu
            JOIN payroll_rows pr ON pr.upload_id = pu.id
            WHERE (pu.year * 100 + pu.month) BETWEEN %s AND %s
            GROUP BY COALESCE(NULLIF(pr.department_raw,''), '—')
            ORDER BY total_accrued DESC;
        """, (start_p, end_p))
        df = pd.DataFrame(rows)
        if not df.empty:
            for c in ("total_accrued", "avg_accrued",
                      "median_accrued", "worked_hours", "rwv_hours"):
                df[c] = df[c].astype(float)
            for c in ("employee_count", "rwv_employees_cnt", "worked_days"):
                df[c] = df[c].astype(int)
            # Стоимость часа по подразделению
            df["cost_per_hour"] = df.apply(
                lambda r: r["total_accrued"] / r["worked_hours"]
                if r["worked_hours"] > 0 else 0.0, axis=1
            )
        return df

# ============================================================
#  UI HELPERS
# ============================================================

class DeltaBadge(tk.Frame):
    def __init__(self, parent, delta_pct, bg_color="white", **kwargs):
        super().__init__(parent, bg=bg_color, **kwargs)
        if delta_pct is None:
            tk.Label(self, text="нет данных пред. периода",
                     font=("Segoe UI", 7), fg=PALETTE["text_muted"],
                     bg=bg_color).pack()
            return
        arrow = "▲" if delta_pct >= 0 else "▼"
        color = PALETTE["positive"] if delta_pct >= 0 else PALETTE["negative"]
        text  = f"{arrow} {abs(delta_pct):.1f}% к пред. периоду"
        tk.Label(self, text=text, font=("Segoe UI", 8, "bold"),
                 fg=color, bg=bg_color).pack()


# ============================================================
#  ANALYTICS PAGE
# ============================================================

class AnalyticsPage(ttk.Frame):
    def __init__(self, master, app_ref):
        super().__init__(master)
        self.app_ref = app_ref
        self._build_header()
        self._build_notebook()
        self.data_provider: Optional[AnalyticsData] = None
        self.load_filters()
        self.refresh_data()

    # ----------------------------------------------------------
    #  HEADER
    # ----------------------------------------------------------

    def _build_header(self):
        hdr = ttk.Frame(self, padding="8 6 8 6")
        hdr.pack(fill="x", side="top")

        ttk.Label(hdr, text="Период:").pack(side="left", padx=(0, 4))
        self.period_var = tk.StringVar(value="Текущий месяц")
        period_cb = ttk.Combobox(
            hdr, textvariable=self.period_var,
            values=["Текущий месяц", "Прошлый месяц",
                    "Текущий квартал", "Текущий год"],
            state="readonly", width=18,
        )
        period_cb.pack(side="left", padx=4)
        period_cb.bind("<<ComboboxSelected>>", self.refresh_data)

        ttk.Label(hdr, text="Тип объекта:").pack(side="left", padx=(12, 4))
        self.object_type_var = tk.StringVar(value="Все типы")
        self.object_type_combo = ttk.Combobox(
            hdr, textvariable=self.object_type_var,
            state="readonly", width=28,
        )
        self.object_type_combo.pack(side="left", padx=4)
        self.object_type_combo.bind("<<ComboboxSelected>>", self.refresh_data)

        ttk.Button(hdr, text="⟳  Обновить",
                   command=self.refresh_data).pack(side="left", padx=10)
        ttk.Button(hdr, text="📥  Экспорт в Excel",
                   command=self._export_to_excel).pack(side="left", padx=4)

        self.last_update_var = tk.StringVar(value="")
        ttk.Label(hdr, textvariable=self.last_update_var,
                  font=("Segoe UI", 8),
                  foreground=PALETTE["text_muted"]).pack(side="right", padx=8)

    # ----------------------------------------------------------
    #  NOTEBOOK — lazy loading
    # ----------------------------------------------------------

    def _build_notebook(self):
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=8, pady=4)

        self.tab_summary   = ttk.Frame(self.notebook)
        self.tab_labor     = ttk.Frame(self.notebook)
        self.tab_transport = ttk.Frame(self.notebook)
        self.tab_meals     = ttk.Frame(self.notebook)
        self.tab_objects   = ttk.Frame(self.notebook)
        self.tab_users     = ttk.Frame(self.notebook)
        self.tab_lodging   = ttk.Frame(self.notebook)
        self.tab_payroll   = ttk.Frame(self.notebook)

        self.notebook.add(self.tab_summary,   text="  📊 Сводка  ")
        self.notebook.add(self.tab_labor,     text="  👷 Трудозатраты  ")
        self.notebook.add(self.tab_transport, text="  🚛 Транспорт  ")
        self.notebook.add(self.tab_meals,     text="  🍽 Питание  ")
        self.notebook.add(self.tab_objects,   text="  🏗 Объекты  ")
        self.notebook.add(self.tab_users,     text="  👤 Пользователи  ")
        self.notebook.add(self.tab_lodging,   text="  🏠 Проживание  ")
        self.notebook.add(self.tab_payroll,   text="  💰 ФОТ  ")

        self._tab_built = {t: False for t in (
            "summary", "labor", "transport", "meals",
            "objects", "users", "lodging", "payroll"
        )}
        self._tab_map = {
            0: "summary",   1: "labor",   2: "transport",
            3: "meals",     4: "objects", 5: "users",
            6: "lodging",   7: "payroll",
        }
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

    def _on_tab_changed(self, event=None):
        idx  = self.notebook.index(self.notebook.select())
        name = self._tab_map.get(idx)
        if name and not self._tab_built.get(name):
            self._build_tab(name)

    def _build_tab(self, name: str):
        builders = {
            "summary":   self._build_summary_tab,
            "labor":     self._build_labor_tab,
            "transport": self._build_transport_tab,
            "meals":     self._build_meals_tab,
            "objects":   self._build_objects_tab,
            "users":     self._build_users_tab,
            "lodging":   self._build_lodging_tab,
            "payroll":   self._build_payroll_tab,
        }
        if name in builders:
            builders[name]()
            self._tab_built[name] = True

    # ----------------------------------------------------------
    #  ФИЛЬТРЫ / ОБНОВЛЕНИЕ
    # ----------------------------------------------------------

    def load_filters(self):
        try:
            types = AnalyticsData(
                datetime.now().date(), datetime.now().date(), ""
            ).get_object_types()
            self.object_type_combo["values"] = ["Все типы"] + types
        except Exception as e:
            logging.error(f"Не удалось загрузить типы объектов: {e}")
            self.object_type_combo["values"] = ["Все типы"]

    def get_dates_from_period(self) -> Tuple:
        period = self.period_var.get()
        today  = datetime.today()
        if period == "Текущий месяц":
            start = today.replace(day=1)
            end   = (start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        elif period == "Прошлый месяц":
            end   = today.replace(day=1) - timedelta(days=1)
            start = end.replace(day=1)
        elif period == "Текущий квартал":
            q     = (today.month - 1) // 3 + 1
            start = datetime(today.year, 3 * q - 2, 1)
            end   = (start + timedelta(days=95)).replace(day=1) - timedelta(days=1)
        elif period == "Текущий год":
            start = datetime(today.year, 1, 1)
            end   = datetime(today.year, 12, 31)
        else:
            start = today.replace(day=1)
            end   = (start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        return start.date(), end.date()

    def refresh_data(self, event=None):
        start_date, end_date = self.get_dates_from_period()
        obj_filter = self.object_type_var.get()
        if obj_filter == "Все типы":
            obj_filter = ""
        self.data_provider = AnalyticsData(start_date, end_date, obj_filter)

        for k in self._tab_built:
            self._tab_built[k] = False

        for tab in (self.tab_summary, self.tab_labor, self.tab_transport,
                    self.tab_meals, self.tab_objects, self.tab_users,
                    self.tab_lodging, self.tab_payroll):
            self._clear_tab(tab)

        idx  = self.notebook.index(self.notebook.select())
        name = self._tab_map.get(idx, "summary")
        self._build_tab(name)

        self.last_update_var.set(
            f"Обновлено: {datetime.now().strftime('%H:%M:%S')}  |  "
            f"{start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')}"
        )

    # ----------------------------------------------------------
    #  ОБЩИЕ UI-ХЕЛПЕРЫ
    # ----------------------------------------------------------

    def _clear_tab(self, tab):
        for w in tab.winfo_children():
            w.destroy()

    def _create_kpi_card(self, parent, title: str, value, unit: str,
                         delta_pct=None, color: str = PALETTE["primary"]):
        card = tk.Frame(parent, bg="white", bd=0,
                        highlightbackground="#E0E0E0", highlightthickness=1)

        accent = tk.Frame(card, bg=color, height=4)
        accent.pack(fill="x", side="top")

        inner = tk.Frame(card, bg="white", padx=10, pady=8)
        inner.pack(fill="both", expand=True)

        tk.Label(inner, text=title, font=("Segoe UI", 9),
                 fg=PALETTE["text_muted"], bg="white",
                 wraplength=130, justify="center").pack()

        tk.Label(inner, text=str(value), font=("Segoe UI", 20, "bold"),
                 fg=color, bg="white").pack(pady=(4, 0))

        tk.Label(inner, text=unit, font=("Segoe UI", 8),
                 fg=PALETTE["text_muted"], bg="white").pack()

        if delta_pct is not None:
            DeltaBadge(inner, delta_pct, bg_color="white").pack(pady=(4, 0))

        return card

    def _make_figure(self, figsize=(6, 3.5)) -> Tuple[Figure, Any]:
        fig = Figure(figsize=figsize, dpi=100, facecolor="#FAFAFA")
        ax  = fig.add_subplot(111)
        return fig, ax

    def _embed_figure(self, fig: Figure, parent) -> FigureCanvasTkAgg:
        fig.tight_layout(pad=1.8)
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        return canvas

    def _create_treeview(self, parent, columns: List[tuple],
                         height: int = 10) -> ttk.Treeview:
        tree = ttk.Treeview(parent, columns=[c[0] for c in columns],
                            show="headings", height=height)
        for col_id, col_text in columns:
            tree.heading(col_id, text=col_text)
            tree.column(col_id, anchor="w", width=120)
        tree.tag_configure("odd",  background="#F5F7FA")
        tree.tag_configure("even", background="white")
        vsb = ttk.Scrollbar(parent, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        parent.grid_rowconfigure(0, weight=1)
        parent.grid_columnconfigure(0, weight=1)
        return tree

    def _create_treeview_with_hscroll(self, parent, columns: List[str],
                                       height: int = 12) -> ttk.Treeview:
        container = ttk.Frame(parent)
        container.pack(fill="both", expand=True)
        tree = ttk.Treeview(container, columns=columns,
                            show="headings", height=height)
        tree.tag_configure("odd",  background="#F5F7FA")
        tree.tag_configure("even", background="white")
        vsb = ttk.Scrollbar(container, orient="vertical",   command=tree.yview)
        hsb = ttk.Scrollbar(container, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)
        return tree

    def _insert_rows(self, tree: ttk.Treeview, rows: list):
        for i, vals in enumerate(rows):
            tag = "odd" if i % 2 == 0 else "even"
            tree.insert("", "end", values=vals, tags=(tag,))

    # ----------------------------------------------------------
    #  TAB 0: СВОДКА — исправленный график
    # ----------------------------------------------------------

    def _build_summary_tab(self):
        self._clear_tab(self.tab_summary)
        dp = self.data_provider

        # ── KPI-карточки ──────────────────────────────────────
        summary = dp.get_executive_summary()

        kpi_frame = tk.Frame(self.tab_summary, bg="#F0F2F5")
        kpi_frame.pack(fill="x", padx=10, pady=10)

        labor_h   = summary.get("labor_hours", 0)
        workers   = summary.get("unique_workers", 0)
        ot_pct    = summary.get("overtime_pct", 0)
        mach_h    = summary.get("machine_hours", 0)
        portions  = summary.get("meal_portions", 0)
        meal_c    = summary.get("meal_cost", 0)
        residents = summary.get("active_residents", 0)

        cards_def = [
            ("Человеко-часов",
             f"{labor_h:,.0f}".replace(",", " "), "час.",
             summary.get("delta_labor_hours"), PALETTE["primary"]),
            ("Уникальных работников",
             workers, "чел.", None, PALETTE["primary"]),
            ("Переработки",
             f"{ot_pct:.1f}", "% от всех часов",
             summary.get("delta_overtime_hours"),
             PALETTE["negative"] if ot_pct > 20 else PALETTE["neutral"]),
            ("Машино-часов",
             f"{mach_h:,.0f}".replace(",", " "), "час.",
             summary.get("delta_machine_hours"), PALETTE["success"]),
            ("Порций питания",
             f"{portions:,.0f}".replace(",", " "), "шт.",
             summary.get("delta_meal_portions"), PALETTE["warning"]),
            ("Стоимость питания",
             f"{meal_c:,.0f}".replace(",", " "), "₽ (оценка)",
             summary.get("delta_meal_cost"), PALETTE["warning"]),
            ("Проживает сейчас",
             residents, "чел.", None, PALETTE["accent"]),
        ]
        for i, (title, value, unit, delta, color) in enumerate(cards_def):
            card = self._create_kpi_card(
                kpi_frame, title, value, unit,
                delta_pct=delta, color=color
            )
            card.grid(row=0, column=i, padx=5, pady=4, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        # ── Основная область: 2×2 сетка ───────────────────────
        grid = ttk.Frame(self.tab_summary)
        grid.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        grid.columnconfigure(0, weight=3)   # левая колонка шире
        grid.columnconfigure(1, weight=2)   # правая уже
        grid.rowconfigure(0, weight=1)
        grid.rowconfigure(1, weight=1)

        # ── [0,0] ТОП-7 объектов по чел.-часам ───────────────
        top_obj_f = ttk.LabelFrame(grid, text="ТОП объектов по чел.-часам")
        top_obj_f.grid(row=0, column=0, padx=(0, 5), pady=(0, 5), sticky="nsew")

        df_obj = dp.get_labor_by_object()
        if not df_obj.empty:
            df_obj["total_hours"] = df_obj["total_hours"].fillna(0).astype(float)
            df_obj["object_name"] = df_obj["object_name"].fillna("—")
            df_plot = df_obj.head(7).sort_values("total_hours", ascending=True).copy()
            df_plot["short"] = df_plot["object_name"].str[:35].apply(
                lambda s: s + "…" if len(s) >= 35 else s
            )
            total_sum = df_obj["total_hours"].sum()

            fig, ax = self._make_figure(figsize=(5.5, 3.2))
            colors_bar = [
                PALETTE["primary"] if i == len(df_plot) - 1
                else "#90CAF9"
                for i in range(len(df_plot))
            ]
            bars = ax.barh(
                df_plot["short"].tolist(),
                df_plot["total_hours"].tolist(),
                color=colors_bar, edgecolor="white", linewidth=0.5,
            )
            max_v = float(df_plot["total_hours"].max() or 1)
            for bar, (_, row) in zip(bars, df_plot.iterrows()):
                w = float(bar.get_width() or 0)
                pct = w / total_sum * 100 if total_sum > 0 else 0
                ax.text(
                    w + max_v * 0.01,
                    bar.get_y() + bar.get_height() / 2,
                    f"{w:,.0f}".replace(",", " ") + f"  ({pct:.1f}%)",
                    va="center", fontsize=7, color=PALETTE["neutral"],
                )
            ax.xaxis.set_major_formatter(
                mticker.FuncFormatter(
                    lambda v, _: f"{v/1000:.0f}к" if v >= 1000 else f"{v:.0f}"
                )
            )
            apply_chart_style(ax, xlabel="Чел.-часы")
            self._embed_figure(fig, top_obj_f)
        else:
            ttk.Label(top_obj_f, text="Нет данных.").pack(pady=20)

        # ── [0,1] СТРУКТУРА ЗАТРАТ — исправленная ─────────────
        cost_f = ttk.LabelFrame(grid, text="Структура затрат (оценка, ₽)")
        cost_f.grid(row=0, column=1, padx=(5, 0), pady=(0, 5), sticky="nsew")

        breakdown = dp.get_cost_breakdown()
        breakdown_nonzero = {k: v for k, v in breakdown.items() if v > 0}
        total_cost = sum(breakdown_nonzero.values())

        if breakdown_nonzero:
            fig2 = Figure(figsize=(3.8, 3.2), dpi=100, facecolor="#FAFAFA")

            if len(breakdown_nonzero) == 1:
                # Одна категория — не рисуем пончик, показываем текст
                ax2 = fig2.add_subplot(111)
                ax2.set_facecolor("#FAFAFA")
                ax2.axis("off")
                name, val = list(breakdown_nonzero.items())[0]
                ax2.text(0.5, 0.6, name, ha="center", va="center",
                         fontsize=14, fontweight="bold",
                         color=PALETTE["warning"], transform=ax2.transAxes)
                ax2.text(0.5, 0.4,
                         f"{val:,.0f} ₽".replace(",", " "),
                         ha="center", va="center", fontsize=12,
                         color=PALETTE["neutral"], transform=ax2.transAxes)
            else:
                # Горизонтальный stacked bar — читается при любых пропорциях
                ax2 = fig2.add_subplot(111)
                ax2.set_facecolor("#FAFAFA")

                colors_cost = {
                    "ФОТ":        PALETTE["primary"],
                    "Питание":    PALETTE["warning"],
                    "Проживание": PALETTE["accent"],
                }
                left_val = 0.0
                items = sorted(breakdown_nonzero.items(),
                               key=lambda x: x[1], reverse=True)

                for name, val in items:
                    color = colors_cost.get(name, PALETTE["neutral"])
                    pct   = val / total_cost * 100
                    ax2.barh(
                        [""],
                        [val],
                        left=left_val,
                        color=color,
                        edgecolor="white",
                        linewidth=2,
                        height=0.55,
                    )
                    # Подпись внутри сегмента если > 5%
                    if pct >= 5:
                        ax2.text(
                            left_val + val / 2, 0,
                            f"{pct:.1f}%",
                            ha="center", va="center",
                            fontsize=9, fontweight="bold",
                            color="white",
                        )
                    left_val += val

                ax2.set_xlim(0, total_cost)
                ax2.axis("off")

                # Легенда в виде цветных плашек под баром
                legend_y = -0.35
                n = len(items)
                for idx, (name, val) in enumerate(items):
                    color = colors_cost.get(name, PALETTE["neutral"])
                    pct   = val / total_cost * 100
                    x_pos = (idx + 0.5) / n
                    # Цветной квадрат
                    ax2.add_patch(
                        plt.Rectangle(
                            (x_pos * total_cost - total_cost / n * 0.45,
                             legend_y - 0.08),
                            total_cost / n * 0.12, 0.12,
                            transform=ax2.transData,
                            color=color, clip_on=False,
                        )
                    )
                    val_str = (f"{val/1_000_000:.1f}М ₽"
                               if val >= 1_000_000
                               else f"{val/1000:.0f}к ₽")
                    ax2.text(
                        x_pos * total_cost,
                        legend_y,
                        f"{name}\n{val_str}\n{pct:.1f}%",
                        ha="center", va="top",
                        fontsize=7.5, color=PALETTE["neutral"],
                        transform=ax2.transData,
                    )

            # Итоговая сумма сверху
            total_str = (f"{total_cost/1_000_000:.1f} млн ₽"
                         if total_cost >= 1_000_000
                         else f"{total_cost:,.0f} ₽".replace(",", " "))
            fig2.text(
                0.5, 0.97, f"Итого: {total_str}",
                ha="center", va="top",
                fontsize=10, fontweight="bold",
                color=PALETTE["neutral"],
            )
            fig2.tight_layout(rect=(0, 0.05, 1, 0.93))
            canvas2 = FigureCanvasTkAgg(fig2, master=cost_f)
            canvas2.draw()
            canvas2.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        else:
            ttk.Label(cost_f,
                      text="Нет данных о затратах.",
                      justify="center").pack(pady=30, padx=20)

        # ── [1,0] ЗАНЯТОСТЬ ОБЩЕЖИТИЙ — sparkline + цифры ────
        dorm_f = ttk.LabelFrame(grid, text="Занятость общежитий")
        dorm_f.grid(row=1, column=0, padx=(0, 5), pady=(5, 0), sticky="nsew")

        df_occ = dp.get_lodging_daily_occupancy()
        if not df_occ.empty:
            df_occ["occupied_beds"] = df_occ["occupied_beds"].astype(int)
            avg_occ = df_occ["occupied_beds"].mean()
            max_occ = df_occ["occupied_beds"].max()
            min_occ = df_occ["occupied_beds"].min()
            last_occ = int(df_occ["occupied_beds"].iloc[-1])

            # Мини-метрики сверху
            metrics_f = tk.Frame(dorm_f, bg="white")
            metrics_f.pack(fill="x", padx=8, pady=(6, 0))

            for col_idx, (lbl, val, color) in enumerate([
                ("Сейчас занято",  f"{last_occ}",        PALETTE["accent"]),
                ("Среднее за период", f"{avg_occ:.0f}",  PALETTE["neutral"]),
                ("Максимум",       f"{max_occ}",          PALETTE["primary"]),
                ("Минимум",        f"{min_occ}",          PALETTE["success"]),
            ]):
                cell = tk.Frame(metrics_f, bg="white")
                cell.grid(row=0, column=col_idx, padx=8, sticky="ew")
                metrics_f.columnconfigure(col_idx, weight=1)
                tk.Label(cell, text=lbl, font=("Segoe UI", 7),
                         fg=PALETTE["text_muted"], bg="white").pack()
                tk.Label(cell, text=f"{val} чел.",
                         font=("Segoe UI", 12, "bold"),
                         fg=color, bg="white").pack()

            # Sparkline
            df_occ["d"]     = pd.to_datetime(df_occ["d"])
            df_occ["label"] = df_occ["d"].dt.strftime("%d.%m")
            x_occ = list(range(len(df_occ)))

            fig3 = Figure(figsize=(5.5, 1.6), dpi=100, facecolor="white")
            ax3  = fig3.add_subplot(111)
            ax3.set_facecolor("white")

            ax3.fill_between(x_occ, df_occ["occupied_beds"].tolist(),
                             alpha=0.2, color=PALETTE["accent"])
            ax3.plot(x_occ, df_occ["occupied_beds"].tolist(),
                     color=PALETTE["accent"], linewidth=1.8)

            # Линия среднего
            ax3.axhline(avg_occ, color=PALETTE["neutral"],
                        linestyle="--", linewidth=1, alpha=0.6)

            # Выделяем последнюю точку
            ax3.scatter([x_occ[-1]], [last_occ],
                        color=PALETTE["accent"], s=40, zorder=5)

            step = max(1, len(df_occ) // 10)
            ax3.set_xticks(x_occ[::step])
            ax3.set_xticklabels(
                df_occ["label"].iloc[::step].tolist(),
                rotation=0, ha="center", fontsize=7,
            )
            ax3.spines["top"].set_visible(False)
            ax3.spines["right"].set_visible(False)
            ax3.spines["left"].set_color("#E0E0E0")
            ax3.spines["bottom"].set_color("#E0E0E0")
            ax3.tick_params(colors="#78909C", labelsize=7)
            ax3.set_ylabel("мест", fontsize=7, color=PALETTE["text_muted"])
            fig3.tight_layout(pad=0.8)

            canvas3 = FigureCanvasTkAgg(fig3, master=dorm_f)
            canvas3.draw()
            canvas3.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        else:
            ttk.Label(dorm_f, text="Нет данных по проживанию.").pack(pady=20)

        # ── [1,1] АЛЕРТЫ И СТАТУСЫ ────────────────────────────
        alerts_f = ttk.LabelFrame(grid, text="⚡ Требует внимания")
        alerts_f.grid(row=1, column=1, padx=(5, 0), pady=(5, 0), sticky="nsew")

        self._build_alerts_panel(alerts_f, summary, dp)

    # ----------------------------------------------------------
    #  Панель алертов — отдельный метод для читаемости
    # ----------------------------------------------------------

    def _build_alerts_panel(self, parent, summary: dict, dp: "AnalyticsData"):
        """
        Показывает список проблем/наблюдений которые требуют внимания.
        Каждый алерт — цветная строка с иконкой и пояснением.
        """
        canvas = tk.Canvas(parent, bg="white", highlightthickness=0)
        scrollbar = ttk.Scrollbar(parent, orient="vertical",
                                  command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner = tk.Frame(canvas, bg="white")
        canvas_window = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _on_configure(e):
            canvas.configure(scrollregion=canvas.bbox("all"))
            canvas.itemconfig(canvas_window, width=canvas.winfo_width())

        inner.bind("<Configure>", _on_configure)
        canvas.bind("<Configure>", _on_configure)

        alerts = []  # список (уровень, иконка, заголовок, текст)

        # 1. Переработки
        ot_pct = float(summary.get("overtime_pct", 0))
        if ot_pct > 25:
            alerts.append(("error", "🔴",
                           "Высокая доля переработок",
                           f"{ot_pct:.1f}% рабочих часов — переработки. "
                           f"Норма: до 20%."))
        elif ot_pct > 15:
            alerts.append(("warn", "🟡",
                           "Переработки выше нормы",
                           f"{ot_pct:.1f}% часов — переработки."))

        # 2. Машино-часы = 0
        if float(summary.get("machine_hours", 0)) == 0:
            alerts.append(("warn", "🟡",
                           "Нет данных по транспорту",
                           "За период не зафиксировано ни одного "
                           "машино-часа. Проверьте заявки на транспорт."))

        # 3. Проживание без тарифа
        try:
            kpi_ld = dp.get_lodging_kpi()
            missing = int(kpi_ld.get("missing_rate_bed_days", 0))
            if missing > 0:
                alerts.append(("error", "🔴",
                               "Койко-дни без тарифа",
                               f"{missing:,} койко-дн. без тарифа RUB. "
                               f"Стоимость проживания занижена."))
        except Exception:
            pass

        # 4. Дельта чел.-часов
        delta_labor = summary.get("delta_labor_hours")
        if delta_labor is not None:
            if delta_labor < -20:
                alerts.append(("error", "🔴",
                               "Резкое падение трудозатрат",
                               f"Чел.-часов стало меньше на "
                               f"{abs(delta_labor):.1f}% к пред. периоду."))
            elif delta_labor > 30:
                alerts.append(("info", "🔵",
                               "Рост трудозатрат",
                               f"+{delta_labor:.1f}% чел.-часов "
                               f"к пред. периоду."))

        # 5. Дельта питания
        delta_meal = summary.get("delta_meal_portions")
        if delta_meal is not None and delta_meal < -15:
            alerts.append(("warn", "🟡",
                           "Снижение порций питания",
                           f"−{abs(delta_meal):.1f}% порций "
                           f"к пред. периоду."))

        # 6. ФОТ — проверяем наличие данных
        try:
            kpi_pay = dp.get_payroll_kpi()
            if float(kpi_pay.get("total_accrued", 0)) == 0:
                alerts.append(("warn", "🟡",
                               "Нет данных ФОТ",
                               "Не загружены данные по зарплатной ведомости "
                               "за период."))
        except Exception:
            pass

        # Если алертов нет
        if not alerts:
            ok_f = tk.Frame(inner, bg="white")
            ok_f.pack(fill="x", padx=10, pady=20)
            tk.Label(ok_f, text="✅", font=("Segoe UI", 24),
                     bg="white").pack()
            tk.Label(ok_f,
                     text="Критичных отклонений\nне обнаружено",
                     font=("Segoe UI", 10),
                     fg=PALETTE["success"], bg="white",
                     justify="center").pack(pady=4)
            return

        # Цвета уровней
        level_colors = {
            "error": ("#FFEBEE", PALETTE["negative"]),
            "warn":  ("#FFF8E1", "#E65100"),
            "info":  ("#E3F2FD", PALETTE["primary"]),
        }

        for level, icon, title, text in alerts:
            bg_color, accent_color = level_colors.get(
                level, ("#F5F5F5", PALETTE["neutral"])
            )

            row_f = tk.Frame(inner, bg=bg_color,
                             highlightbackground=accent_color,
                             highlightthickness=1)
            row_f.pack(fill="x", padx=6, pady=4, ipady=4)

            # Цветная полоска слева
            stripe = tk.Frame(row_f, bg=accent_color, width=4)
            stripe.pack(side="left", fill="y")

            content = tk.Frame(row_f, bg=bg_color)
            content.pack(side="left", fill="both",
                         expand=True, padx=(8, 6))

            # Заголовок с иконкой
            hdr_f = tk.Frame(content, bg=bg_color)
            hdr_f.pack(fill="x")
            tk.Label(hdr_f, text=icon,
                     font=("Segoe UI", 11),
                     bg=bg_color).pack(side="left")
            tk.Label(hdr_f, text=title,
                     font=("Segoe UI", 9, "bold"),
                     fg=accent_color, bg=bg_color).pack(side="left", padx=4)

            # Текст
            tk.Label(content, text=text,
                     font=("Segoe UI", 8),
                     fg="#424242", bg=bg_color,
                     wraplength=200, justify="left",
                     anchor="w").pack(fill="x", pady=(2, 0))

    # ----------------------------------------------------------
    #  TAB 1: ТРУДОЗАТРАТЫ
    # ----------------------------------------------------------

    def _build_labor_tab(self):
        self._clear_tab(self.tab_labor)
        dp = self.data_provider

        kpi_frame = tk.Frame(self.tab_labor, bg="#F0F2F5")
        kpi_frame.pack(fill="x", padx=10, pady=10)

        kpi = dp.get_labor_kpi()
        cards = [
            ("Всего чел.-часов",
             f"{kpi.get('total_hours', 0):,.1f}".replace(",", " "),
             "час.", PALETTE["primary"]),
            ("Всего чел.-дней",
             f"{kpi.get('total_days', 0):,.0f}".replace(",", " "),
             "дн.", PALETTE["primary"]),
            ("Сотрудников",
             int(kpi.get("unique_people", 0) or 0),
             "чел.", PALETTE["neutral"]),
            ("Часов/сотрудник",
             f"{kpi.get('hours_per_person', 0):.1f}",
             "час/чел", PALETTE["neutral"]),
            ("Переработки",
             f"{kpi.get('total_overtime', 0):,.1f}".replace(",", " "),
             "час.",
             PALETTE["negative"] if kpi.get("overtime_share_pct", 0) > 20
             else PALETTE["neutral"]),
            ("Доля переработок",
             f"{kpi.get('overtime_share_pct', 0):.1f}", "%",
             PALETTE["negative"] if kpi.get("overtime_share_pct", 0) > 20
             else PALETTE["neutral"]),
        ]
        for i, (title, value, unit, color) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit, color=color)
            card.grid(row=0, column=i, padx=5, pady=4, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        charts = ttk.Frame(self.tab_labor)
        charts.pack(fill="both", expand=True, padx=10, pady=4)

        left = ttk.LabelFrame(charts, text="ТОП-10 объектов по трудозатратам")
        left.pack(side="left", fill="both", expand=True, padx=(0, 5))

        df_obj = dp.get_labor_by_object()
        if not df_obj.empty:
            df_obj["total_hours"] = df_obj["total_hours"].fillna(0).astype(float)
            df_obj["object_name"] = df_obj["object_name"].fillna("—")
            df_plot = df_obj.head(10).sort_values("total_hours", ascending=True).copy()
            df_plot["short"] = df_plot["object_name"].str[:32].apply(
                lambda s: s + "…" if len(s) >= 32 else s
            )
            fig, ax = self._make_figure(figsize=(5, 4))
            bars = ax.barh(df_plot["short"], df_plot["total_hours"],
                           color=PALETTE["primary"], edgecolor="white", linewidth=0.5)
            max_v = float(df_plot["total_hours"].max() or 1)
            for bar in bars:
                w = float(bar.get_width() or 0)
                ax.text(w + max_v * 0.015,
                        bar.get_y() + bar.get_height() / 2,
                        f"{w:,.0f}".replace(",", " "),
                        va="center", fontsize=7, color=PALETTE["neutral"])
            apply_chart_style(ax, xlabel="Человеко-часы")
            self._embed_figure(fig, left)
        else:
            ttk.Label(left, text="Нет данных.").pack(pady=20)

        right = ttk.Frame(charts)
        right.pack(side="left", fill="both", expand=True, padx=(5, 0))

        trend_f = ttk.LabelFrame(right, text="Динамика чел.-часов по месяцам")
        trend_f.pack(fill="both", expand=True, pady=(0, 5))

        df_trend = dp.get_labor_trend_by_month()
        if not df_trend.empty:
            df_trend["total_hours"] = df_trend["total_hours"].fillna(0).astype(float)
            df_trend["period"] = df_trend.apply(
                lambda r: f"{int(r['year'])}-{int(r['month']):02d}", axis=1
            )
            fig, ax = self._make_figure(figsize=(5, 2.4))
            x = list(range(len(df_trend)))
            ax.fill_between(x, df_trend["total_hours"].tolist(),
                            alpha=0.18, color=PALETTE["primary"])
            ax.plot(x, df_trend["total_hours"].tolist(),
                    marker="o", color=PALETTE["primary"],
                    linewidth=2, markersize=5)
            for xi, v in zip(x, df_trend["total_hours"].tolist()):
                ax.annotate(f"{v:,.0f}".replace(",", " "),
                            (xi, v), textcoords="offset points",
                            xytext=(0, 6), ha="center", fontsize=7)
            ax.set_xticks(x)
            ax.set_xticklabels(df_trend["period"].tolist(),
                               rotation=45, ha="right", fontsize=7)
            apply_chart_style(ax, ylabel="Чел.-часы")
            self._embed_figure(fig, trend_f)
        else:
            ttk.Label(trend_f, text="Нет данных.").pack(pady=10)

        emp_f = ttk.LabelFrame(right, text="ТОП-10 сотрудников по часам")
        emp_f.pack(fill="both", expand=True, pady=(5, 0))

        df_emp = dp.get_top_employees_by_hours(limit=10)
        if not df_emp.empty:
            df_emp["total_hours"]    = df_emp["total_hours"].fillna(0).astype(float)
            df_emp["total_overtime"] = df_emp["total_overtime"].fillna(0).astype(float)
            df_emp["fio"]            = df_emp["fio"].fillna("—")
            df_plot_e = df_emp.sort_values("total_hours", ascending=True)
            fig, ax = self._make_figure(figsize=(5, 2.4))
            normal = df_plot_e["total_hours"] - df_plot_e["total_overtime"]
            ax.barh(df_plot_e["fio"], normal.tolist(),
                    color=PALETTE["primary"], label="Норма", edgecolor="white")
            ax.barh(df_plot_e["fio"], df_plot_e["total_overtime"].tolist(),
                    left=normal.tolist(), color=PALETTE["negative"],
                    label="Переработка", alpha=0.85, edgecolor="white")
            ax.legend(fontsize=7, loc="lower right")
            apply_chart_style(ax, xlabel="Часы")
            self._embed_figure(fig, emp_f)
        else:
            ttk.Label(emp_f, text="Нет данных.").pack(pady=10)

        dept_frame = ttk.LabelFrame(self.tab_labor, text="Нагрузка по подразделениям")
        dept_frame.pack(fill="x", padx=10, pady=(0, 8))

        df_dept = dp.get_labor_by_department()
        if not df_dept.empty:
            tree = self._create_treeview(
                dept_frame,
                columns=[
                    ("department", "Подразделение"),
                    ("people",     "Людей"),
                    ("hours",      "Чел.-часы"),
                    ("avg",        "Часов/чел."),
                ],
                height=6,
            )
            tree.column("department", width=260)
            tree.column("people",     width=80,  anchor="e")
            tree.column("hours",      width=120, anchor="e")
            tree.column("avg",        width=100, anchor="e")
            self._insert_rows(tree, [
                (
                    row["department_name"],
                    int(row["people_cnt"]),
                    f"{float(row['total_hours']):,.1f}".replace(",", " "),
                    f"{float(row['total_hours'])/int(row['people_cnt']):.1f}"
                    if int(row["people_cnt"]) > 0 else "—",
                )
                for _, row in df_dept.iterrows()
            ])
        else:
            ttk.Label(dept_frame, text="Нет данных.").pack(pady=10)

    # ----------------------------------------------------------
    #  TAB 2: ТРАНСПОРТ
    # ----------------------------------------------------------

    def _build_transport_tab(self):
        self._clear_tab(self.tab_transport)
        dp = self.data_provider

        kpi_frame = tk.Frame(self.tab_transport, bg="#F0F2F5")
        kpi_frame.pack(fill="x", padx=10, pady=10)

        kpi = dp.get_transport_kpi()
        fulfill = dp.get_transport_fulfillment_summary()

        cards = [
            ("Всего маш.-часов",
             f"{kpi.get('total_machine_hours', 0):,.1f}".replace(",", " "),
             "час.", PALETTE["success"]),
            ("Всего заявок",
             int(kpi.get("total_orders", 0) or 0), "шт.", PALETTE["neutral"]),
            ("Единиц техники",
             f"{kpi.get('total_units', 0):.0f}", "шт.", PALETTE["neutral"]),
            ("% исполнения",
             f"{fulfill.get('fulfillment_pct', 0):.1f}", "%",
             PALETTE["positive"] if fulfill.get("fulfillment_pct", 0) >= 80
             else PALETTE["negative"]),
            ("Водителей задействовано",
             int(fulfill.get("unique_drivers", 0) or 0),
             "чел.", PALETTE["neutral"]),
        ]
        for i, (title, value, unit, color) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit, color=color)
            card.grid(row=0, column=i, padx=5, pady=4, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        charts = ttk.Frame(self.tab_transport)
        charts.pack(fill="both", expand=True, padx=10, pady=4)

        left = ttk.LabelFrame(charts, text="ТОП-10 видов техники (маш.-часы)")
        left.pack(side="left", fill="both", expand=True, padx=(0, 5))

        df_tech = dp.get_transport_by_tech()
        if not df_tech.empty:
            df_tech["total_hours"] = df_tech["total_hours"].fillna(0).astype(float)
            df_tech["tech"]        = df_tech["tech"].fillna("—")
            df_plot = df_tech.head(10).sort_values("total_hours", ascending=True)
            fig, ax = self._make_figure(figsize=(5, 4))
            bars = ax.barh(df_plot["tech"], df_plot["total_hours"].tolist(),
                           color=PALETTE["success"], edgecolor="white")
            max_v = float(df_plot["total_hours"].max() or 1)
            for bar in bars:
                w = float(bar.get_width() or 0)
                ax.text(w + max_v * 0.015,
                        bar.get_y() + bar.get_height() / 2,
                        f"{w:,.0f}".replace(",", " "),
                        va="center", fontsize=7, color=PALETTE["neutral"])
            apply_chart_style(ax, xlabel="Машино-часы")
            self._embed_figure(fig, left)
        else:
            ttk.Label(left, text="Нет данных по технике.").pack(pady=20)

        right = ttk.Frame(charts)
        right.pack(side="left", fill="both", expand=True, padx=(5, 0))

        trend_f = ttk.LabelFrame(right, text="Динамика маш.-часов по месяцам")
        trend_f.pack(fill="both", expand=True, pady=(0, 5))

        df_tr = dp.get_transport_trend()
        if not df_tr.empty and "period" in df_tr.columns:
            x = list(range(len(df_tr)))
            fig, ax = self._make_figure(figsize=(5, 2.4))
            ax.fill_between(x, df_tr["total_hours"].tolist(),
                            alpha=0.18, color=PALETTE["success"])
            ax.plot(x, df_tr["total_hours"].tolist(),
                    marker="o", color=PALETTE["success"],
                    linewidth=2, markersize=5)
            for xi, v in zip(x, df_tr["total_hours"].tolist()):
                ax.annotate(f"{v:,.0f}".replace(",", " "),
                            (xi, v), textcoords="offset points",
                            xytext=(0, 6), ha="center", fontsize=7)
            ax.set_xticks(x)
            ax.set_xticklabels(df_tr["period"].tolist(),
                               rotation=45, ha="right", fontsize=7)
            apply_chart_style(ax, ylabel="Маш.-часы")
            self._embed_figure(fig, trend_f)
        else:
            ttk.Label(trend_f, text="Нет данных.").pack(pady=10)

        obj_f = ttk.LabelFrame(right, text="ТОП-10 объектов по маш.-часам")
        obj_f.pack(fill="both", expand=True, pady=(5, 0))

        df_obj = dp.get_transport_by_object(limit=10)
        if not df_obj.empty:
            tree = self._create_treeview(
                obj_f,
                columns=[
                    ("object", "Объект"),
                    ("hours",  "Маш.-часы"),
                    ("orders", "Заявок"),
                ],
                height=7,
            )
            tree.column("object", width=220)
            tree.column("hours",  width=100, anchor="e")
            tree.column("orders", width=80,  anchor="e")
            self._insert_rows(tree, [
                (r["object_name"],
                 f"{r['total_hours']:,.1f}".replace(",", " "),
                 int(r["order_count"]))
                for _, r in df_obj.iterrows()
            ])
        else:
            ttk.Label(obj_f, text="Нет данных.").pack(pady=10)

    # ----------------------------------------------------------
    #  TAB 3: ПИТАНИЕ
    # ----------------------------------------------------------

    def _build_meals_tab(self):
        self._clear_tab(self.tab_meals)
        dp = self.data_provider

        kpi_frame = tk.Frame(self.tab_meals, bg="#F0F2F5")
        kpi_frame.pack(fill="x", padx=10, pady=10)

        kpi  = dp.get_meals_kpi()
        cost = dp.get_meals_cost_kpi()
        total_qty  = float(kpi.get("total_portions_qty", 0) or 0)
        total_ord  = int(kpi.get("total_orders", 0) or 0)
        unique_emp = int(kpi.get("unique_employees", 0) or 0)
        total_cost = float(cost.get("total_cost", 0) or 0)

        cards = [
            ("Всего порций",
             f"{total_qty:,.0f}".replace(",", " "), "шт.", PALETTE["warning"]),
            ("Всего заявок",   total_ord,   "шт.", PALETTE["neutral"]),
            ("Накормлено",     unique_emp,  "чел.", PALETTE["neutral"]),
            ("Порций/заявку",
             f"{kpi.get('avg_portions_per_order', 0):.1f}", "", PALETTE["neutral"]),
            ("Порций/чел.",
             f"{kpi.get('avg_portions_per_person', 0):.1f}", "", PALETTE["neutral"]),
            ("Стоимость (оценка)",
             f"{total_cost:,.0f}".replace(",", " "), "₽", PALETTE["warning"]),
        ]
        for i, (title, value, unit, color) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit, color=color)
            card.grid(row=0, column=i, padx=5, pady=4, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        top = ttk.Frame(self.tab_meals)
        top.pack(fill="both", expand=True, padx=10, pady=4)

        pie_f = ttk.LabelFrame(top, text="Структура по типам питания")
        pie_f.pack(side="left", fill="both", expand=True, padx=(0, 5))

        df_types = dp.get_meals_by_type()
        if not df_types.empty:
            df_types["total_qty"]      = df_types["total_qty"].fillna(0).astype(float)
            df_types["meal_type_text"] = df_types["meal_type_text"].fillna("—")
            fig, ax = self._make_figure(figsize=(4.5, 3.8))
            colors_pie = SERIES_COLORS * 3
            wedges, texts, autotexts = ax.pie(
                df_types["total_qty"].tolist(),
                labels=df_types["meal_type_text"].tolist(),
                autopct="%1.1f%%",
                colors=colors_pie[:len(df_types)],
                startangle=90,
                wedgeprops=dict(width=0.55, edgecolor="white"),
                pctdistance=0.80,
            )
            for at in autotexts:
                at.set_fontsize(8)
                at.set_color("white")
                at.set_fontweight("bold")
            ax.axis("equal")
            self._embed_figure(fig, pie_f)
        else:
            ttk.Label(pie_f, text="Нет данных.").pack(pady=20)

        trend_f = ttk.LabelFrame(top, text="Динамика порций по месяцам")
        trend_f.pack(side="left", fill="both", expand=True, padx=(5, 0))

        df_trend = dp.get_meals_trend_by_month()
        if not df_trend.empty:
            df_trend["total_qty"] = df_trend["total_qty"].fillna(0).astype(float)
            df_trend["period"]    = pd.to_datetime(df_trend["period"]).dt.strftime("%Y-%m")
            x = list(range(len(df_trend)))
            fig, ax = self._make_figure(figsize=(5, 3.8))
            ax.fill_between(x, df_trend["total_qty"].tolist(),
                            alpha=0.18, color=PALETTE["warning"])
            ax.plot(x, df_trend["total_qty"].tolist(),
                    marker="o", color=PALETTE["warning"],
                    linewidth=2, markersize=5)
            for xi, v in zip(x, df_trend["total_qty"].tolist()):
                ax.annotate(f"{v:,.0f}".replace(",", " "),
                            (xi, v), textcoords="offset points",
                            xytext=(0, 6), ha="center", fontsize=7)
            ax.set_xticks(x)
            ax.set_xticklabels(df_trend["period"].tolist(),
                               rotation=45, ha="right", fontsize=7)
            apply_chart_style(ax, ylabel="Порций")
            self._embed_figure(fig, trend_f)
        else:
            ttk.Label(trend_f, text="Нет данных.").pack(pady=20)

        bottom = ttk.Frame(self.tab_meals)
        bottom.pack(fill="x", padx=10, pady=(0, 8))

        obj_f = ttk.LabelFrame(bottom, text="ТОП объектов по порциям")
        obj_f.pack(side="left", fill="both", expand=True, padx=(0, 5))

        df_obj = dp.get_meals_by_object(limit=10)
        if not df_obj.empty:
            tree = self._create_treeview(
                obj_f,
                columns=[("object","Объект"),("qty","Порций"),("people","Людей")],
                height=7,
            )
            tree.column("object", width=240)
            tree.column("qty",    width=90, anchor="e")
            tree.column("people", width=80, anchor="e")
            self._insert_rows(tree, [
                (r["object_name"],
                 f"{float(r['total_qty']):,.0f}".replace(",", " "),
                 int(r["unique_employees"]))
                for _, r in df_obj.iterrows()
            ])
        else:
            ttk.Label(obj_f, text="Нет данных.").pack(pady=10)

        dept_f = ttk.LabelFrame(bottom, text="Питание по подразделениям")
        dept_f.pack(side="left", fill="both", expand=True, padx=(5, 0))

        df_dept = dp.get_meals_by_department()
        if not df_dept.empty:
            tree2 = self._create_treeview(
                dept_f,
                columns=[("dept","Подразделение"),("qty","Порций"),("people","Людей")],
                height=7,
            )
            tree2.column("dept",   width=220)
            tree2.column("qty",    width=90, anchor="e")
            tree2.column("people", width=80, anchor="e")
            self._insert_rows(tree2, [
                (r["department_name"],
                 f"{float(r['total_qty']):,.0f}".replace(",", " "),
                 int(r["unique_employees"]))
                for _, r in df_dept.iterrows()
            ])
        else:
            ttk.Label(dept_f, text="Нет данных.").pack(pady=10)

    # ----------------------------------------------------------
    #  TAB 4: ОБЪЕКТЫ
    # ----------------------------------------------------------

    def _build_objects_tab(self):
        self._clear_tab(self.tab_objects)
        dp = self.data_provider

        df = dp.get_objects_rating(limit=15)
        if df.empty:
            ttk.Label(self.tab_objects,
                      text="Нет данных по объектам за выбранный период.").pack(pady=20)
            return

        for c in ("labor_hours", "machine_hours", "portions", "meal_cost"):
            df[c] = df[c].fillna(0).astype(float)
        df["address"] = df["address"].fillna("—")

        outer = ttk.Frame(self.tab_objects)
        outer.pack(fill="both", expand=True, padx=10, pady=8)

        tbl_f = ttk.LabelFrame(outer, text="Рейтинг объектов")
        tbl_f.pack(side="left", fill="both", expand=True, padx=(0, 6))

        tree = self._create_treeview(
            tbl_f,
            columns=[
                ("address",   "Объект"),
                ("workers",   "Сотр."),
                ("labor",     "Чел.-часы"),
                ("machine",   "Маш.-часы"),
                ("portions",  "Порций"),
                ("meal_cost", "Питание, ₽"),
            ],
            height=14,
        )
        tree.column("address",   width=260)
        tree.column("workers",   width=60,  anchor="e")
        tree.column("labor",     width=100, anchor="e")
        tree.column("machine",   width=100, anchor="e")
        tree.column("portions",  width=80,  anchor="e")
        tree.column("meal_cost", width=110, anchor="e")
        self._insert_rows(tree, [
            (
                r["address"],
                int(r["workers"]),
                f"{r['labor_hours']:,.1f}".replace(",", " "),
                f"{r['machine_hours']:,.1f}".replace(",", " "),
                f"{r['portions']:,.0f}".replace(",", " "),
                f"{r['meal_cost']:,.0f}".replace(",", " "),
            )
            for _, r in df.iterrows()
        ])

        chart_f = ttk.LabelFrame(outer, text="Труд vs Транспорт (ТОП-10)")
        chart_f.pack(side="left", fill="both", expand=True, padx=(6, 0))

        df_top = df.head(10)
        fig, ax = self._make_figure(figsize=(5.5, 4.5))
        x     = list(range(len(df_top)))
        width = 0.38
        ax.bar([v - width/2 for v in x], df_top["labor_hours"].tolist(),
               width=width, label="Чел.-часы",
               color=PALETTE["primary"], edgecolor="white")
        ax.bar([v + width/2 for v in x], df_top["machine_hours"].tolist(),
               width=width, label="Маш.-часы",
               color=PALETTE["success"], edgecolor="white")
        ax.set_xticks(x)
        ax.set_xticklabels(
            [a[:14] + "…" if len(a) > 14 else a for a in df_top["address"].tolist()],
            rotation=45, ha="right", fontsize=7,
        )
        ax.legend(fontsize=8)
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(
                lambda v, _: f"{v/1000:.0f}к" if v >= 1000 else f"{v:.0f}"
            )
        )
        apply_chart_style(ax, ylabel="Часы")
        self._embed_figure(fig, chart_f)

    # ----------------------------------------------------------
    #  TAB 5: ПОЛЬЗОВАТЕЛИ
    # ----------------------------------------------------------

    def _build_users_tab(self):
        self._clear_tab(self.tab_users)
        dp = self.data_provider

        df = dp.get_users_activity()
        if df.empty:
            ttk.Label(self.tab_users,
                      text="Нет активности пользователей за выбранный период."
                      ).pack(pady=20)
            return

        df["username"]  = df["username"].fillna("—")
        df["full_name"] = df["full_name"].fillna("")
        df["total_ops"] = (
            df["timesheets_created"]
            + df["transport_orders_created"]
            + df["meal_orders_created"]
            + df["dorm_checkins"]
            + df["dorm_checkouts"]
        )

        outer = ttk.Frame(self.tab_users)
        outer.pack(fill="both", expand=True, padx=10, pady=8)

        tbl_f = ttk.LabelFrame(outer, text="Активность пользователей")
        tbl_f.pack(side="left", fill="both", expand=True, padx=(0, 6))

        tree = self._create_treeview(
            tbl_f,
            columns=[
                ("user",  "Логин"),
                ("name",  "ФИО"),
                ("th",    "Табелей"),
                ("tr",    "Транспорт"),
                ("mo",    "Питание"),
                ("ci",    "Заселений"),
                ("co",    "Выселений"),
                ("total", "Всего"),
            ],
            height=14,
        )
        tree.column("user",  width=100)
        tree.column("name",  width=200)
        for c in ("th", "tr", "mo", "ci", "co", "total"):
            tree.column(c, width=80, anchor="e")

        self._insert_rows(tree, [
            (
                r["username"],
                r["full_name"] or "",
                int(r["timesheets_created"]),
                int(r["transport_orders_created"]),
                int(r["meal_orders_created"]),
                int(r["dorm_checkins"]),
                int(r["dorm_checkouts"]),
                int(r["total_ops"]),
            )
            for _, r in df.iterrows()
        ])

        chart_f = ttk.LabelFrame(outer, text="ТОП пользователей (stacked bar)")
        chart_f.pack(side="left", fill="both", expand=True, padx=(6, 0))

        df_top = df.sort_values("total_ops", ascending=False).head(10)
        fig, ax = self._make_figure(figsize=(5, 4.5))
        y    = list(range(len(df_top)))
        cats = [
            ("timesheets_created",       "Табели",    PALETTE["primary"]),
            ("transport_orders_created", "Транспорт", PALETTE["success"]),
            ("meal_orders_created",      "Питание",   PALETTE["warning"]),
            ("dorm_checkins",            "Заселений", PALETTE["accent"]),
            ("dorm_checkouts",           "Выселений", PALETTE["neutral"]),
        ]
        left_vals = [0] * len(df_top)
        for col, label, color in cats:
            vals = df_top[col].tolist()
            ax.barh(y, vals, left=left_vals, color=color,
                    label=label, edgecolor="white", linewidth=0.4)
            left_vals = [lv + v for lv, v in zip(left_vals, vals)]

        ax.set_yticks(y)
        ax.set_yticklabels(df_top["username"].tolist(), fontsize=8)
        ax.invert_yaxis()
        ax.legend(fontsize=7, loc="lower right")
        apply_chart_style(ax, xlabel="Операций")
        self._embed_figure(fig, chart_f)

    # ----------------------------------------------------------
    #  TAB 6: ПРОЖИВАНИЕ
    # ----------------------------------------------------------

    def _build_lodging_tab(self):
        self._clear_tab(self.tab_lodging)
        dp = self.data_provider

        # Пробуем сначала dorm_charges (быстро), fallback на generate_series
        charges_kpi = dp.get_lodging_charges_kpi()
        use_charges = charges_kpi.get("total_amount", 0) > 0

        if use_charges:
            bed_days   = int(charges_kpi.get("total_days", 0))
            amount_rub = float(charges_kpi.get("total_amount", 0))
            avg_price  = float(charges_kpi.get("avg_price", 0))
            active_end = 0   # нет в charges — берём из kpi ниже
            missing    = 0
        else:
            kpi        = dp.get_lodging_kpi()
            bed_days   = int(kpi.get("bed_days", 0))
            amount_rub = float(kpi.get("amount_rub", 0))
            avg_price  = float(kpi.get("avg_price_rub", 0))
            active_end = int(kpi.get("active_on_end", 0))
            missing    = int(kpi.get("missing_rate_bed_days", 0))

        # active_on_end из charges_kpi недоступен — берём отдельно
        if use_charges:
            raw_kpi    = dp.get_lodging_kpi()
            active_end = int(raw_kpi.get("active_on_end", 0))
            missing    = int(raw_kpi.get("missing_rate_bed_days", 0))

        # KPI-карточки
        kpi_frame = tk.Frame(self.tab_lodging, bg="#F0F2F5")
        kpi_frame.pack(fill="x", padx=10, pady=10)

        cards = [
            ("Койко-дней",
             f"{bed_days:,}".replace(",", " "),
             "дней", PALETTE["accent"]),
            ("Начислено",
             f"{amount_rub:,.0f}".replace(",", " "),
             "₽", PALETTE["accent"]),
            ("Средняя цена",
             f"{avg_price:,.0f}".replace(",", " "),
             "₽/день", PALETTE["neutral"]),
            ("Проживает (на конец)",
             active_end, "чел.", PALETTE["primary"]),
            ("Без тарифа",
             missing, "койко-дн.",
             PALETTE["negative"] if missing > 0 else PALETTE["neutral"]),
        ]
        for i, (title, value, unit, color) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit, color=color)
            card.grid(row=0, column=i, padx=5, pady=4, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        if missing > 0:
            warn = tk.Label(
                self.tab_lodging,
                text=f"⚠  {missing} койко-дней без тарифа RUB. Проверьте dorm_rates.",
                fg="white", bg=PALETTE["negative"],
                font=("Segoe UI", 9, "bold"), padx=10, pady=4,
            )
            warn.pack(fill="x", padx=10, pady=(0, 6))

        # График занятости
        top = ttk.Frame(self.tab_lodging)
        top.pack(fill="both", expand=False, padx=10, pady=4)

        occ_f = ttk.LabelFrame(top, text="Динамика занятости мест по дням")
        occ_f.pack(fill="both", expand=True)

        df_occ = dp.get_lodging_daily_occupancy()
        if not df_occ.empty:
            df_occ["d"]     = pd.to_datetime(df_occ["d"])
            df_occ["label"] = df_occ["d"].dt.strftime("%d.%m")
            x_occ = list(range(len(df_occ)))

            fig, ax = self._make_figure(figsize=(11, 2.2))
            ax.fill_between(x_occ, df_occ["occupied_beds"].tolist(),
                            alpha=0.2, color=PALETTE["accent"])
            ax.plot(x_occ, df_occ["occupied_beds"].tolist(),
                    color=PALETTE["accent"], linewidth=1.5)

            # Метки среднего
            avg_occ = df_occ["occupied_beds"].mean()
            ax.axhline(avg_occ, color=PALETTE["neutral"],
                       linestyle="--", linewidth=1, alpha=0.7)
            ax.text(len(x_occ) - 1, avg_occ,
                    f" avg={avg_occ:.0f}", va="bottom",
                    fontsize=7, color=PALETTE["neutral"])

            step = max(1, len(df_occ) // 15)
            ax.set_xticks(x_occ[::step])
            ax.set_xticklabels(df_occ["label"].iloc[::step].tolist(),
                               rotation=0, ha="center", fontsize=7)
            apply_chart_style(ax, ylabel="Мест занято")
            self._embed_figure(fig, occ_f)
        else:
            ttk.Label(occ_f, text="Нет данных.").pack(pady=10)

        # Таблицы
        bottom = ttk.Frame(self.tab_lodging)
        bottom.pack(fill="both", expand=True, padx=10, pady=4)

        dorm_f = ttk.LabelFrame(bottom, text="ТОП общежитий (начислено, ₽)")
        dorm_f.pack(side="left", fill="both", expand=True, padx=(0, 5))

        dept_f = ttk.LabelFrame(bottom, text="По подразделениям (начислено, ₽)")
        dept_f.pack(side="left", fill="both", expand=True, padx=(5, 0))

        df_dorm = dp.get_lodging_by_dorm(limit=10)
        if not df_dorm.empty:
            tree = self._create_treeview(
                dorm_f,
                columns=[
                    ("dorm",   "Общежитие"),
                    ("bd",     "Койко-дн."),
                    ("amount", "Начислено, ₽"),
                    ("avg",    "₽/день"),
                    ("miss",   "Без тарифа"),
                ],
                height=10,
            )
            tree.column("dorm",   width=200)
            tree.column("bd",     width=90,  anchor="e")
            tree.column("amount", width=130, anchor="e")
            tree.column("avg",    width=100, anchor="e")
            tree.column("miss",   width=90,  anchor="e")
            self._insert_rows(tree, [
                (
                    r["dorm_name"],
                    int(r["bed_days"]),
                    f"{float(r['amount_rub']):,.0f}".replace(",", " "),
                    f"{float(r['avg_price_rub']):,.0f}".replace(",", " "),
                    int(r["missing_rate_bed_days"]),
                )
                for _, r in df_dorm.iterrows()
            ])
        else:
            ttk.Label(dorm_f, text="Нет данных.").pack(pady=10)

        df_dept = dp.get_lodging_by_department()
        if not df_dept.empty:
            tree2 = self._create_treeview(
                dept_f,
                columns=[
                    ("dept",   "Подразделение"),
                    ("bd",     "Койко-дн."),
                    ("amount", "Начислено, ₽"),
                    ("miss",   "Без тарифа"),
                ],
                height=10,
            )
            tree2.column("dept",   width=220)
            tree2.column("bd",     width=90,  anchor="e")
            tree2.column("amount", width=130, anchor="e")
            tree2.column("miss",   width=90,  anchor="e")
            self._insert_rows(tree2, [
                (
                    r["department_name"],
                    int(r["bed_days"]),
                    f"{float(r['amount_rub']):,.0f}".replace(",", " "),
                    int(r["missing_rate_bed_days"]),
                )
                for _, r in df_dept.iterrows()
            ])
        else:
            ttk.Label(dept_f, text="Нет данных.").pack(pady=10)

        # Pivot
        pivot_f = ttk.LabelFrame(
            self.tab_lodging,
            text="Общежитие → объекты (уникальные сотрудники по TBN)"
        )
        pivot_f.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        pv = dp.get_dorm_to_objects_people_pivot(top_objects=10, top_dorms=30)
        if pv is None or pv.empty:
            ttk.Label(pivot_f,
                      text="Нет данных (проверьте tbn в табелях и таблице employees)."
                      ).pack(pady=10)
        else:
            cols = ["Общежитие"] + list(pv.columns)
            tree3 = self._create_treeview_with_hscroll(
                pivot_f, columns=cols, height=12
            )
            for c in cols:
                tree3.heading(c, text=c)
                tree3.column(
                    c,
                    width=240 if c == "Общежитие" else 110,
                    anchor="w" if c == "Общежитие" else "e",
                )
            self._insert_rows(tree3, [
                [dorm_name] + [int(row[col]) for col in pv.columns]
                for dorm_name, row in pv.iterrows()
            ])

    # ----------------------------------------------------------
    #  TAB 7: ФОТ
    # ----------------------------------------------------------

    def _build_payroll_tab(self):
        self._clear_tab(self.tab_payroll)
        dp = self.data_provider

        # ── KPI-карточки ──────────────────────────────────────
        kpi_frame = tk.Frame(self.tab_payroll, bg="#F0F2F5")
        kpi_frame.pack(fill="x", padx=10, pady=10)

        kpi = dp.get_payroll_kpi()
        total_acc  = float(kpi.get("total_accrued", 0))
        unique_emp = int(kpi.get("unique_employees", 0))
        avg_acc    = float(kpi.get("avg_accrued", 0))
        cost_per_h = float(kpi.get("cost_per_hour", 0))
        rwv_pct    = float(kpi.get("rwv_share_pct", 0))
        rwv_h      = float(kpi.get("total_rwv_hours", 0))

        cards = [
            ("Всего начислено",
             f"{total_acc:,.0f}".replace(",", " "),
             "₽", PALETTE["payroll"]),
            ("Сотрудников в ФОТ",
             unique_emp, "чел.", PALETTE["neutral"]),
            ("Среднее начисление\n(по подразделению)",
             f"{avg_acc:,.0f}".replace(",", " "),
             "₽/чел.", PALETTE["neutral"]),
            ("Стоимость часа",
             f"{cost_per_h:,.1f}".replace(",", " "),
             "₽/час", PALETTE["success"]),
            ("Часы в выходные (RWV)",
             f"{rwv_h:,.0f}".replace(",", " "),
             "час.", PALETTE["warning"]),
            ("Доля RWV",
             f"{rwv_pct:.1f}", "% от всех часов",
             PALETTE["negative"] if rwv_pct > 25 else PALETTE["neutral"]),
        ]
        for i, (title, value, unit, color) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit, color=color)
            card.grid(row=0, column=i, padx=5, pady=4, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        if total_acc == 0:
            ttk.Label(
                self.tab_payroll,
                text="Нет данных ФОТ за период.\n"
                     "Загрузите payroll_uploads через модуль импорта.",
                font=("Segoe UI", 11), justify="center",
                foreground=PALETTE["text_muted"],
            ).pack(expand=True)
            return

        # ── Основная область: два столбца ─────────────────────
        main = ttk.Frame(self.tab_payroll)
        main.pack(fill="both", expand=True, padx=10, pady=4)

        left = ttk.Frame(main)
        left.pack(side="left", fill="both", expand=True, padx=(0, 5))

        right = ttk.Frame(main)
        right.pack(side="left", fill="both", expand=True, padx=(5, 0))

        # ── ЛЕВАЯ КОЛОНКА ─────────────────────────────────────

        # Тренд ФОТ по месяцам
        trend_f = ttk.LabelFrame(left, text="Динамика ФОТ по месяцам (₽)")
        trend_f.pack(fill="both", expand=True, pady=(0, 5))

        df_trend = dp.get_payroll_trend()
        if not df_trend.empty:
            x = list(range(len(df_trend)))
            fig, ax = self._make_figure(figsize=(5.5, 2.8))
            ax2 = ax.twinx()

            ax.bar(x, df_trend["total_accrued"].tolist(),
                   color=PALETTE["payroll"], alpha=0.75, label="ФОТ, ₽")
            ax2.plot(x, df_trend["employee_count"].tolist(),
                     marker="o", color=PALETTE["primary"],
                     linewidth=2, markersize=5, label="Сотрудников")

            # Подписи сумм над столбцами
            for xi, v in zip(x, df_trend["total_accrued"].tolist()):
                label_txt = (f"{v/1_000_000:.1f}М"
                             if v >= 1_000_000 else f"{v/1000:.0f}к")
                ax.text(xi, v, label_txt,
                        ha="center", va="bottom", fontsize=7,
                        color=PALETTE["payroll"], fontweight="bold")

            ax.set_xticks(x)
            ax.set_xticklabels(df_trend["period"].tolist(),
                               rotation=45, ha="right", fontsize=7)
            ax.yaxis.set_major_formatter(
                mticker.FuncFormatter(
                    lambda v, _: f"{v/1_000_000:.1f}М"
                    if v >= 1_000_000 else f"{v/1000:.0f}к"
                )
            )
            apply_chart_style(ax, ylabel="Начислено, ₽")
            ax2.set_ylabel("Сотрудников", fontsize=8, color=PALETTE["primary"])
            ax2.tick_params(colors=PALETTE["primary"], labelsize=8)
            ax2.spines["right"].set_color(PALETTE["primary"])
            ax2.spines["top"].set_visible(False)

            handles = [
                mpatches.Patch(color=PALETTE["payroll"], label="ФОТ итого, ₽"),
                mpatches.Patch(color=PALETTE["primary"], label="Кол-во сотрудников"),
            ]
            ax.legend(handles=handles, fontsize=7, loc="upper left")
            self._embed_figure(fig, trend_f)
        else:
            ttk.Label(trend_f, text="Нет данных.").pack(pady=10)

        # ФОТ по объектам — горизонтальный бар
        obj_f = ttk.LabelFrame(left, text="ФОТ по объектам (₽)")
        obj_f.pack(fill="both", expand=True, pady=(5, 0))

        df_obj = dp.get_payroll_by_object(limit=12)
        if not df_obj.empty:
            df_plot = df_obj.head(10).sort_values("payroll_amount", ascending=True)
            fig, ax = self._make_figure(figsize=(5.5, 3.2))
            bars = ax.barh(
                df_plot["object_name"].str[:28].tolist(),
                df_plot["payroll_amount"].tolist(),
                color=PALETTE["payroll"], edgecolor="white",
            )
            max_v = float(df_plot["payroll_amount"].max() or 1)
            for bar in bars:
                w = float(bar.get_width() or 0)
                label_txt = (f"{w/1_000_000:.1f}М"
                             if w >= 1_000_000 else f"{w/1000:.0f}к")
                ax.text(w + max_v * 0.01,
                        bar.get_y() + bar.get_height() / 2,
                        label_txt, va="center", fontsize=7,
                        color=PALETTE["neutral"])
            ax.xaxis.set_major_formatter(
                mticker.FuncFormatter(
                    lambda v, _: f"{v/1_000_000:.1f}М"
                    if v >= 1_000_000 else f"{v/1000:.0f}к"
                )
            )
            apply_chart_style(ax, xlabel="₽")
            self._embed_figure(fig, obj_f)
        else:
            ttk.Label(obj_f,
                      text="Нет данных.\nЗагрузите payroll_distribution.",
                      justify="center").pack(pady=20)

        # ── ПРАВАЯ КОЛОНКА ────────────────────────────────────

        # Таблица по подразделениям — АГРЕГИРОВАННАЯ (без персональных данных)
        dept_f = ttk.LabelFrame(
            right,
            text="ФОТ по подразделениям (агрегировано)"
        )
        dept_f.pack(fill="both", expand=True, pady=(0, 5))

        df_dept_stats = dp.get_payroll_stats_summary()
        if not df_dept_stats.empty:
            tree_dept = self._create_treeview(
                dept_f,
                columns=[
                    ("dept",     "Подразделение"),
                    ("emp",      "Сотр."),
                    ("total",    "Итого, ₽"),
                    ("avg",      "Среднее, ₽"),
                    ("median",   "Медиана, ₽"),
                    ("cph",      "₽/час"),
                    ("rwv_emp",  "С RWV, чел."),
                ],
                height=10,
            )
            tree_dept.column("dept",    width=200)
            tree_dept.column("emp",     width=60,  anchor="e")
            tree_dept.column("total",   width=130, anchor="e")
            tree_dept.column("avg",     width=110, anchor="e")
            tree_dept.column("median",  width=110, anchor="e")
            tree_dept.column("cph",     width=80,  anchor="e")
            tree_dept.column("rwv_emp", width=90,  anchor="e")

            self._insert_rows(tree_dept, [
                (
                    r["department_name"],
                    int(r["employee_count"]),
                    f"{r['total_accrued']:,.0f}".replace(",", " "),
                    f"{r['avg_accrued']:,.0f}".replace(",", " "),
                    f"{r['median_accrued']:,.0f}".replace(",", " "),
                    f"{r['cost_per_hour']:.1f}"
                    if r["cost_per_hour"] > 0 else "—",
                    int(r["rwv_employees_cnt"]),
                )
                for _, r in df_dept_stats.iterrows()
            ])
        else:
            ttk.Label(dept_f, text="Нет данных.").pack(pady=10)

        # Таблица по должностям — без персональных данных
        pos_f = ttk.LabelFrame(
            right,
            text="ФОТ по должностям (агрегировано)"
        )
        pos_f.pack(fill="both", expand=True, pady=(5, 0))

        df_pos = dp.get_payroll_by_position()
        if not df_pos.empty:
            tree_pos = self._create_treeview(
                pos_f,
                columns=[
                    ("position", "Должность"),
                    ("emp",      "Сотр."),
                    ("total",    "Итого, ₽"),
                    ("avg",      "Среднее, ₽"),
                    ("cph",      "₽/час"),
                ],
                height=8,
            )
            tree_pos.column("position", width=220)
            tree_pos.column("emp",      width=60,  anchor="e")
            tree_pos.column("total",    width=130, anchor="e")
            tree_pos.column("avg",      width=110, anchor="e")
            tree_pos.column("cph",      width=80,  anchor="e")

            self._insert_rows(tree_pos, [
                (
                    r["position_name"],
                    int(r["employee_count"]),
                    f"{r['total_accrued']:,.0f}".replace(",", " "),
                    f"{r['avg_accrued']:,.0f}".replace(",", " "),
                    f"{r['total_accrued']/r['worked_hours']:.1f}"
                    if r["worked_hours"] > 0 else "—",
                )
                for _, r in df_pos.iterrows()
            ])
        else:
            ttk.Label(pos_f, text="Нет данных.").pack(pady=10)

    @staticmethod
    def _strip_tz(df: pd.DataFrame) -> pd.DataFrame:
        """
        Excel (openpyxl) не поддерживает timezone-aware datetimes.
        Конвертируем все такие колонки в naive UTC.
        """
        if df is None or df.empty:
            return df
        df = df.copy()
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if hasattr(df[col].dt, "tz") and df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
            # Обрабатываем и object-колонки где могут быть Timestamp с tz
            elif df[col].dtype == object:
                try:
                    sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    if sample is not None and hasattr(sample, "tzinfo") and sample.tzinfo is not None:
                        df[col] = pd.to_datetime(df[col], utc=True).dt.tz_localize(None)
                except Exception:
                    pass
        return df

    def _export_to_excel(self):
        if not self.data_provider:
            messagebox.showwarning("Экспорт", "Сначала загрузите данные.")
            return

        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel файл", "*.xlsx")],
            title="Сохранить отчёт",
            initialfile=f"analytics_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        )
        if not path:
            return

        dp  = self.data_provider
        stz = self._strip_tz   # короткий псевдоним

        try:
            with pd.ExcelWriter(path, engine="openpyxl") as writer:

                def safe_write(df, sheet):
                    """Записывает df в лист, предварительно убирая tz."""
                    if df is not None and not df.empty:
                        stz(df).to_excel(writer, sheet_name=sheet, index=False)

                # ── Сводка ────────────────────────────────────
                summary = dp.get_executive_summary()
                safe_write(
                    pd.DataFrame([{"Показатель": k, "Значение": v}
                                  for k, v in summary.items()]),
                    "Сводка"
                )

                # ── Трудозатраты ──────────────────────────────
                safe_write(pd.DataFrame([dp.get_labor_kpi()]),
                           "Труд_KPI")
                safe_write(dp.get_labor_by_department(),
                           "Труд_Подразделения")
                safe_write(dp.get_labor_by_object(),
                           "Труд_Объекты")
                safe_write(dp.get_top_employees_by_hours(50),
                           "Труд_ТОП_сотрудников")
                safe_write(dp.get_labor_trend_by_month(),
                           "Труд_Тренд")

                # ── Транспорт ─────────────────────────────────
                safe_write(pd.DataFrame([dp.get_transport_kpi()]),
                           "Транспорт_KPI")
                safe_write(
                    pd.DataFrame([dp.get_transport_fulfillment_summary()]),
                    "Транспорт_Исполнение"
                )
                safe_write(dp.get_transport_by_tech(),
                           "Транспорт_Техника")
                safe_write(dp.get_transport_by_object(50),
                           "Транспорт_Объекты")
                safe_write(dp.get_transport_trend(),
                           "Транспорт_Тренд")

                # ── Питание ───────────────────────────────────
                kpi_m = dp.get_meals_kpi()
                kpi_m.update(dp.get_meals_cost_kpi())
                safe_write(pd.DataFrame([kpi_m]), "Питание_KPI")
                safe_write(dp.get_meals_by_type(),       "Питание_Типы")
                safe_write(dp.get_meals_by_object(50),   "Питание_Объекты")
                safe_write(dp.get_meals_by_department(), "Питание_Подразделения")
                safe_write(dp.get_meals_trend_by_month(),"Питание_Тренд")

                # ── Объекты ───────────────────────────────────
                safe_write(dp.get_objects_rating(50), "Объекты_Рейтинг")

                # ── Пользователи ──────────────────────────────
                safe_write(dp.get_users_activity(), "Пользователи")

                # ── Проживание ────────────────────────────────
                safe_write(pd.DataFrame([dp.get_lodging_kpi()]),
                           "Проживание_KPI")
                safe_write(dp.get_lodging_by_dorm(50),
                           "Проживание_Общежития")
                safe_write(dp.get_lodging_by_department(),
                           "Проживание_Подразделения")
                safe_write(dp.get_lodging_daily_occupancy(),
                           "Проживание_Занятость")

                pv = dp.get_dorm_to_objects_people_pivot(
                    top_objects=15, top_dorms=50
                )
                if pv is not None and not pv.empty:
                    # pivot может иметь MultiIndex — сбрасываем
                    pv_export = pv.reset_index()
                    stz(pv_export).to_excel(
                        writer, sheet_name="Проживание_Pivot", index=False
                    )

                # ── ФОТ (без персональных данных) ─────────────
                safe_write(pd.DataFrame([dp.get_payroll_kpi()]),
                           "ФОТ_KPI")
                safe_write(dp.get_payroll_trend(),
                           "ФОТ_Тренд")
                safe_write(dp.get_payroll_by_object(50),
                           "ФОТ_Объекты")
                safe_write(dp.get_payroll_by_department(),
                           "ФОТ_Подразделения")
                safe_write(dp.get_payroll_stats_summary(),
                           "ФОТ_Статистика_Подразд")
                safe_write(dp.get_payroll_by_position(),
                           "ФОТ_По_Должностям")

                # ── Авто-ширина колонок ───────────────────────
                try:
                    from openpyxl.utils import get_column_letter
                    for sheet_name in writer.sheets:
                        ws = writer.sheets[sheet_name]
                        for col_cells in ws.columns:
                            max_len    = 0
                            col_letter = get_column_letter(
                                col_cells[0].column
                            )
                            for cell in col_cells:
                                try:
                                    cell_len = (
                                        len(str(cell.value))
                                        if cell.value is not None else 0
                                    )
                                    max_len = max(max_len, cell_len)
                                except Exception:
                                    pass
                            ws.column_dimensions[col_letter].width = min(
                                max_len + 4, 60
                            )
                except Exception as fmt_err:
                    logging.warning(
                        f"Не удалось настроить ширину колонок: {fmt_err}"
                    )

            messagebox.showinfo(
                "Экспорт завершён",
                f"Файл успешно сохранён:\n{path}"
            )

        except Exception as e:
            logging.exception("Ошибка экспорта в Excel")
            messagebox.showerror(
                "Ошибка экспорта",
                f"Не удалось сохранить файл:\n{e}"
            )
