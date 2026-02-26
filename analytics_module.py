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
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

db_connection_pool: Optional[pool.SimpleConnectionPool] = None


def set_db_pool(db_pool: pool.SimpleConnectionPool):
    global db_connection_pool
    db_connection_pool = db_pool
    logging.info("Analytics Module: DB pool set.")


# ============================================================
#  ПАЛИТРА И СТИЛЬ — единый корпоративный стиль
# ============================================================

PALETTE = {
    "primary":    "#1565C0",   # синий — труд
    "success":    "#2E7D32",   # зелёный — транспорт
    "warning":    "#E65100",   # оранжевый — питание
    "accent":     "#6A1B9A",   # фиолетовый — жильё
    "neutral":    "#546E7A",   # серый
    "bg_card":    "#F5F7FA",
    "positive":   "#2E7D32",
    "negative":   "#C62828",
    "text_muted": "#78909C",
}

# Цвета для серии графиков
SERIES_COLORS = [
    PALETTE["primary"], PALETTE["success"],
    PALETTE["warning"], PALETTE["accent"],
    "#00838F", "#AD1457",
]


def apply_chart_style(ax, title: str = "", xlabel: str = "", ylabel: str = ""):
    """Единый стиль для всех осей matplotlib."""
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

    # ----------------------------------------------------------
    #  ВСПОМОГАТЕЛЬНЫЙ МЕТОД: период «предыдущий» для Δ
    # ----------------------------------------------------------

    def _prev_period_dates(self) -> Tuple:
        """Возвращает (prev_start, prev_end) — такой же длины, сдвинутый назад."""
        delta = (self.end_date - self.start_date) + timedelta(days=1)
        prev_end = self.start_date - timedelta(days=1)
        prev_start = prev_end - delta + timedelta(days=1)
        return prev_start, prev_end

    # ----------------------------------------------------------
    #  НОВЫЙ: сводный Executive KPI (все модули разом)
    # ----------------------------------------------------------

    def get_executive_summary(self) -> Dict[str, Any]:
        """
        Один запрос-агрегат по всем модулям для сводной вкладки.
        Также считает те же метрики за предыдущий аналогичный период (для Δ%).
        """
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
                start_p, end_p,          # labor_hours
                start_p, end_p,          # overtime_hours
                start_p, end_p,          # unique_workers
                sd, ed,                  # machine_hours
                sd, ed,                  # transport_orders
                sd, ed,                  # meal_portions
                sd, ed,                  # meal_cost
                ed, ed,                  # active_residents (на конец периода)
            ))
            return rows[0] if rows else {}

        cur  = _fetch(self.start_date, self.end_date)
        prev = _fetch(prev_start, prev_end)

        def _delta(key):
            c = float(cur.get(key, 0) or 0)
            p = float(prev.get(key, 0) or 0)
            if p == 0:
                return None  # нет данных для сравнения
            return (c - p) / p * 100.0

        result = {}
        for k in ("labor_hours", "overtime_hours", "machine_hours",
                  "meal_portions", "meal_cost", "active_residents"):
            result[k] = float(cur.get(k, 0) or 0)
        result["unique_workers"] = int(cur.get("unique_workers", 0) or 0)
        result["transport_orders"] = int(cur.get("transport_orders", 0) or 0)

        # Δ% к предыдущему периоду
        for k in ("labor_hours", "overtime_hours", "machine_hours",
                  "meal_portions", "meal_cost"):
            result[f"delta_{k}"] = _delta(k)

        # Производные
        h = result["labor_hours"]
        w = result["unique_workers"]
        result["hours_per_worker"] = h / w if w > 0 else 0.0
        ot = result["overtime_hours"]
        result["overtime_pct"] = ot / h * 100 if h > 0 else 0.0

        return result

    # ----------------------------------------------------------
    #  НОВЫЙ: тренд всех модулей по месяцам (для сводного графика)
    # ----------------------------------------------------------

    def get_monthly_trend_all(self) -> pd.DataFrame:
        """
        Возвращает DataFrame с колонками:
        period | labor_hours | machine_hours | meal_portions
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
            GROUP BY 1, 2
            ORDER BY 1, 2;
        """, (self.start_date, self.end_date))

        meals = self._execute_query("""
            SELECT EXTRACT(YEAR FROM mo.date)::int  AS year,
                   EXTRACT(MONTH FROM mo.date)::int AS month,
                   COALESCE(SUM(moi.quantity),0)::float AS meal_portions
            FROM meal_orders mo
            JOIN meal_order_items moi ON mo.id = moi.order_id
            WHERE mo.date BETWEEN %s AND %s
            GROUP BY 1, 2
            ORDER BY 1, 2;
        """, (self.start_date, self.end_date))

        def to_df(rows, val_col):
            df = pd.DataFrame(rows)
            if df.empty:
                return df
            df["period"] = df.apply(
                lambda r: f"{int(r['year'])}-{int(r['month']):02d}", axis=1
            )
            return df[["period", val_col]]

        dfl  = to_df(labor,     "labor_hours")
        dft  = to_df(transport, "machine_hours")
        dfm  = to_df(meals,     "meal_portions")

        if dfl.empty:
            return pd.DataFrame()

        df = dfl
        if not dft.empty:
            df = df.merge(dft, on="period", how="left")
        else:
            df["machine_hours"] = 0.0
        if not dfm.empty:
            df = df.merge(dfm, on="period", how="left")
        else:
            df["meal_portions"] = 0.0

        df = df.fillna(0)
        return df

    # ----------------------------------------------------------
    #  НОВЫЙ: сводная стоимость по модулям (для pie-chart затрат)
    # ----------------------------------------------------------

    def get_cost_breakdown(self) -> Dict[str, float]:
        """
        Возвращает оценку затрат по категориям (то, что можно посчитать).
        """
        meal_cost_row = self._execute_query("""
            SELECT COALESCE(SUM(COALESCE(mt.price,0)*COALESCE(moi.quantity,1)),0)::float AS v
            FROM meal_orders mo
            JOIN meal_order_items moi ON mo.id = moi.order_id
            LEFT JOIN meal_types mt ON mt.id = moi.meal_type_id
            WHERE mo.date BETWEEN %s AND %s;
        """, (self.start_date, self.end_date))

        # Стоимость проживания
        lodging_cost_row = self._execute_query("""
            WITH days AS (
                SELECT generate_series(%s::date, %s::date, '1 day'::interval)::date AS d
            ),
            sod AS (
                SELECT dd.d, s.dorm_id, s.room_id
                FROM days dd
                JOIN dorm_stays s ON s.check_in <= dd.d
                  AND (s.check_out IS NULL OR s.check_out > dd.d)
            ),
            dm AS (SELECT id, rate_mode FROM dorms),
            rated AS (
                SELECT
                    CASE WHEN dm.rate_mode='PER_ROOM' THEN (
                        SELECT dr.price_per_day FROM dorm_rates dr
                        WHERE dr.room_id=sod.room_id AND dr.valid_from<=sod.d
                          AND dr.currency='RUB' ORDER BY dr.valid_from DESC LIMIT 1)
                    ELSE (
                        SELECT dr.price_per_day FROM dorm_rates dr
                        WHERE dr.dorm_id=sod.dorm_id AND dr.valid_from<=sod.d
                          AND dr.currency='RUB' ORDER BY dr.valid_from DESC LIMIT 1)
                    END AS price_per_day
                FROM sod JOIN dm ON dm.id=sod.dorm_id
            )
            SELECT COALESCE(SUM(COALESCE(price_per_day,0)),0)::float AS v FROM rated;
        """, (self.start_date, self.end_date))

        meal_cost    = float((meal_cost_row[0] if meal_cost_row else {}).get("v", 0) or 0)
        lodging_cost = float((lodging_cost_row[0] if lodging_cost_row else {}).get("v", 0) or 0)

        return {
            "Питание":    meal_cost,
            "Проживание": lodging_cost,
        }

    # ----------------------------------------------------------
    #  НОВЫЙ: активность по объектам — рейтинговая таблица
    # ----------------------------------------------------------

    def get_objects_rating(self, limit: int = 15) -> pd.DataFrame:
        """
        Расширенная таблица объектов: труд + транспорт + питание + проживание.
        Добавляем: кол-во уникальных сотрудников по табелю.
        """
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year * 100 + self.end_date.month

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
            SELECT
                o.address,
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
            ORDER BY labor_hours DESC
            LIMIT {limit};
        """, (start_p, end_p, self.start_date, self.end_date, self.start_date, self.end_date))

        df = pd.DataFrame(rows)
        if not df.empty:
            for c in ("labor_hours", "machine_hours", "portions", "meal_cost"):
                df[c] = df[c].astype(float)
            for c in ("workers", "transport_cnt"):
                df[c] = df[c].astype(int)
            df["address"] = df["address"].fillna("—")
        return df

    # ----------------------------------------------------------
    #  ВСЕ СТАРЫЕ МЕТОДЫ — оставляем как есть, только добавляем
    #  новые для транспорта
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
        start_period = self.start_date.year * 100 + self.start_date.month
        end_period = self.end_date.year * 100 + self.end_date.month
        params: List[Any] = [start_period, end_period]
        join_clause, filter_clause = "", ""
        if self.object_type_filter:
            join_clause = "LEFT JOIN objects o ON th.object_db_id = o.id"
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        result = self._execute_query(
            base_query.format(join_clause=join_clause, filter_clause=filter_clause),
            tuple(params)
        )
        row = dict(result[0]) if result else {}
        total_hours   = float(row.get("total_hours", 0) or 0)
        total_overtime = float(row.get("total_overtime", 0) or 0)
        uniq          = int(row.get("unique_people_key", 0) or 0)
        row.update({
            "total_hours":       total_hours,
            "total_days":        float(row.get("total_days", 0) or 0),
            "total_overtime":    total_overtime,
            "unique_people":     uniq,
            "hours_per_person":  total_hours / uniq if uniq > 0 else 0.0,
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
            join_clause = "LEFT JOIN objects o ON th.object_db_id = o.id"
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
            join_clause = "LEFT JOIN objects o ON th.object_db_id = o.id"
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
            join_clause = "LEFT JOIN objects o ON th.object_db_id = o.id"
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        data = self._execute_query(
            base_query.format(join_clause=join_clause, filter_clause=filter_clause),
            tuple(params)
        )
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"] = df["total_hours"].astype(float)
            df["people_cnt"]  = df["people_cnt"].astype(int)
            df["department_name"] = df["department_name"].fillna("—")
        return df

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
            "total_machine_hours":  total_hours,
            "total_orders":         total_orders,
            "total_units":          total_units,
            "avg_hours_per_order":  total_hours / total_orders if total_orders > 0 else 0.0,
            "hours_per_unit":       total_hours / total_units  if total_units  > 0 else 0.0,
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
            df["total_hours"]  = df["total_hours"].astype(float)
            df["order_count"]  = df["order_count"].astype(int)
        return df

    # НОВЫЙ: транспорт по объектам
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
        GROUP BY o.address
        HAVING o.address IS NOT NULL
        ORDER BY total_hours DESC
        LIMIT {limit};
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

    # НОВЫЙ: тренд транспорта по месяцам
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
            "total_portions_qty":     total_qty,
            "total_orders":           total_orders,
            "unique_employees":       unique_emp,
            "avg_portions_per_order": total_qty / total_orders if total_orders > 0 else 0.0,
            "avg_portions_per_person": total_qty / unique_emp  if unique_emp   > 0 else 0.0,
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
            df["total_qty"]         = df["total_qty"].astype(float)
            df["unique_employees"]  = df["unique_employees"].astype(int)
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

    def get_objects_overview(self, limit: int = 20) -> pd.DataFrame:
        # Оставляем для совместимости — используем get_objects_rating
        return self.get_objects_rating(limit=limit)

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
            for col in ("timesheets_created","transport_orders_created",
                        "meal_orders_created","dorm_checkins","dorm_checkouts"):
                df[col] = df[col].fillna(0).astype(int)
        return df

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
                ELSE (
                    SELECT dr.price_per_day FROM dorm_rates dr
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
        rows = self._execute_query(query, (self.start_date, self.end_date, self.end_date, self.end_date))
        row = dict(rows[0]) if rows else {}
        for k in ("bed_days","active_on_end","missing_rate_bed_days"):
            row[k] = int(row.get(k, 0) or 0)
        for k in ("amount_rub","avg_price_rub"):
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


# ============================================================
#                   UI HELPERS (общие)
# ============================================================

class DeltaBadge(tk.Frame):
    """
    Маленький виджет «▲ +8.3% к пред. периоду».
    delta_pct: float | None (None = нет данных)
    """
    def __init__(self, parent, delta_pct, **kwargs):
        super().__init__(parent, **kwargs)
        if delta_pct is None:
            tk.Label(self, text="нет данных пред. периода",
                     font=("Segoe UI", 7), fg=PALETTE["text_muted"],
                     bg=self["bg"] if "bg" in kwargs else "white").pack()
            return
        arrow  = "▲" if delta_pct >= 0 else "▼"
        color  = PALETTE["positive"] if delta_pct >= 0 else PALETTE["negative"]
        text   = f"{arrow} {abs(delta_pct):.1f}% к пред. периоду"
        tk.Label(self, text=text, font=("Segoe UI", 8, "bold"),
                 fg=color, bg=self["bg"] if "bg" in kwargs else "white").pack()


# ============================================================
#                   ANALYTICS PAGE
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
    #  HEADER: фильтры + кнопки
    # ----------------------------------------------------------

    def _build_header(self):
        hdr = ttk.Frame(self, padding="8 6 8 6")
        hdr.pack(fill="x", side="top")

        # --- Период ---
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

        # --- Тип объекта ---
        ttk.Label(hdr, text="Тип объекта:").pack(side="left", padx=(12, 4))
        self.object_type_var = tk.StringVar(value="Все типы")
        self.object_type_combo = ttk.Combobox(
            hdr, textvariable=self.object_type_var,
            state="readonly", width=28,
        )
        self.object_type_combo.pack(side="left", padx=4)
        self.object_type_combo.bind("<<ComboboxSelected>>", self.refresh_data)

        # --- Кнопки ---
        ttk.Button(hdr, text="⟳  Обновить", command=self.refresh_data).pack(side="left", padx=10)
        ttk.Button(hdr, text="📥  Экспорт в Excel", command=self._export_to_excel).pack(side="left", padx=4)

        # --- Метка последнего обновления ---
        self.last_update_var = tk.StringVar(value="")
        ttk.Label(hdr, textvariable=self.last_update_var,
                  font=("Segoe UI", 8), foreground=PALETTE["text_muted"]).pack(side="right", padx=8)

    # ----------------------------------------------------------
    #  NOTEBOOK: вкладки
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

        self.notebook.add(self.tab_summary,   text="  📊 Сводка  ")
        self.notebook.add(self.tab_labor,     text="  👷 Трудозатраты  ")
        self.notebook.add(self.tab_transport, text="  🚛 Транспорт  ")
        self.notebook.add(self.tab_meals,     text="  🍽 Питание  ")
        self.notebook.add(self.tab_objects,   text="  🏗 Объекты  ")
        self.notebook.add(self.tab_users,     text="  👤 Пользователи  ")
        self.notebook.add(self.tab_lodging,   text="  🏠 Проживание  ")

        # Lazy loading: строим вкладку только когда на неё переходят
        self._tab_built = {t: False for t in (
            "summary", "labor", "transport", "meals", "objects", "users", "lodging"
        )}
        self._tab_map = {
            0: "summary", 1: "labor", 2: "transport",
            3: "meals",   4: "objects", 5: "users", 6: "lodging",
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

        # Сбрасываем флаги построения — все вкладки нужно перестроить
        for k in self._tab_built:
            self._tab_built[k] = False

        # Очищаем все вкладки
        for tab in (self.tab_summary, self.tab_labor, self.tab_transport,
                    self.tab_meals, self.tab_objects, self.tab_users, self.tab_lodging):
            self._clear_tab(tab)

        # Строим только активную вкладку
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
        """
        KPI-карточка с заголовком, значением, единицей и опциональным Δ%.
        """
        card = tk.Frame(parent, bg="white", bd=1, relief="solid")
        card.configure(highlightbackground="#E0E0E0", highlightthickness=1)

        # Цветная полоска сверху
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
            DeltaBadge(inner, delta_pct, bg="white").pack(pady=(4, 0))

        return card

    def _make_figure(self, figsize=(6, 3.5)) -> Tuple[Figure, Any]:
        """Создаёт Figure с нашим фоном."""
        fig = Figure(figsize=figsize, dpi=100, facecolor="#FAFAFA")
        ax  = fig.add_subplot(111)
        return fig, ax

    def _embed_figure(self, fig: Figure, parent) -> FigureCanvasTkAgg:
        fig.tight_layout(pad=1.8)
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        return canvas

    def _create_treeview(self, parent, columns: List[tuple], height: int = 10) -> ttk.Treeview:
        tree = ttk.Treeview(parent, columns=[c[0] for c in columns],
                            show="headings", height=height)
        for col_id, col_text in columns:
            tree.heading(col_id, text=col_text)
            tree.column(col_id, anchor="w", width=120)

        # Чередующиеся строки
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
        tree = ttk.Treeview(container, columns=columns, show="headings", height=height)
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
        """Вставка строк с чередованием цветов."""
        for i, vals in enumerate(rows):
            tag = "odd" if i % 2 == 0 else "even"
            tree.insert("", "end", values=vals, tags=(tag,))

    # ----------------------------------------------------------
    #  TAB 0: СВОДКА (Executive Summary)
    # ----------------------------------------------------------

    def _build_summary_tab(self):
        self._clear_tab(self.tab_summary)
        dp = self.data_provider

        # ── KPI-карточки ──────────────────────────────────────
        summary = dp.get_executive_summary()

        kpi_frame = tk.Frame(self.tab_summary, bg="#F0F2F5")
        kpi_frame.pack(fill="x", padx=10, pady=10)

        labor_h  = summary.get("labor_hours", 0)
        workers  = summary.get("unique_workers", 0)
        ot_pct   = summary.get("overtime_pct", 0)
        mach_h   = summary.get("machine_hours", 0)
        portions = summary.get("meal_portions", 0)
        meal_c   = summary.get("meal_cost", 0)
        residents= summary.get("active_residents", 0)

        cards_def = [
            # (заголовок, значение, единица, delta_key, цвет)
            ("Человеко-часов",
             f"{labor_h:,.0f}".replace(",", " "),
             "час.",
             summary.get("delta_labor_hours"),
             PALETTE["primary"]),

            ("Уникальных работников",
             workers, "чел.", None, PALETTE["primary"]),

            ("Переработки",
             f"{ot_pct:.1f}", "%  от всех часов",
             summary.get("delta_overtime_hours"),
             PALETTE["negative"] if ot_pct > 20 else PALETTE["neutral"]),

            ("Машино-часов",
             f"{mach_h:,.0f}".replace(",", " "),
             "час.",
             summary.get("delta_machine_hours"),
             PALETTE["success"]),

            ("Порций питания",
             f"{portions:,.0f}".replace(",", " "),
             "шт.",
             summary.get("delta_meal_portions"),
             PALETTE["warning"]),

            ("Стоимость питания",
             f"{meal_c:,.0f}".replace(",", " "),
             "₽ (оценка)",
             summary.get("delta_meal_cost"),
             PALETTE["warning"]),

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

        # ── Нижняя часть: тренд + breakdown затрат ────────────
        bottom = ttk.Frame(self.tab_summary)
        bottom.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        # Тренд по всем модулям
        trend_frame = ttk.LabelFrame(bottom, text="Динамика по месяцам (все модули)")
        trend_frame.pack(side="left", fill="both", expand=True, padx=(0, 6))

        df_trend = dp.get_monthly_trend_all()
        if not df_trend.empty:
            fig, ax = self._make_figure(figsize=(7, 3.5))
            ax2 = ax.twinx()

            x = list(range(len(df_trend)))
            ax.bar(x, df_trend["labor_hours"],   color=PALETTE["primary"],
                   alpha=0.75, label="Чел.-часы", width=0.3,
                   align="center")
            ax.bar([v + 0.31 for v in x], df_trend["machine_hours"],
                   color=PALETTE["success"], alpha=0.75,
                   label="Маш.-часы", width=0.3)
            ax2.plot(x, df_trend["meal_portions"], marker="o",
                     color=PALETTE["warning"], linewidth=2,
                     label="Порции (ось →)")

            ax.set_xticks(x)
            ax.set_xticklabels(df_trend["period"], rotation=45, ha="right", fontsize=8)
            apply_chart_style(ax, ylabel="Часы")
            ax2.set_ylabel("Порции", fontsize=8, color=PALETTE["warning"])
            ax2.tick_params(colors=PALETTE["warning"], labelsize=8)
            ax2.spines["right"].set_color(PALETTE["warning"])

            # Общая легенда
            handles  = [
                mpatches.Patch(color=PALETTE["primary"],  label="Чел.-часы"),
                mpatches.Patch(color=PALETTE["success"],  label="Маш.-часы"),
                mpatches.Patch(color=PALETTE["warning"],  label="Порции"),
            ]
            ax.legend(handles=handles, fontsize=8, loc="upper left")
            self._embed_figure(fig, trend_frame)
        else:
            ttk.Label(trend_frame, text="Нет данных за период.").pack(pady=20)

        # Структура затрат (pie)
        cost_frame = ttk.LabelFrame(bottom, text="Структура затрат (оценка, ₽)")
        cost_frame.pack(side="left", fill="both", expand=False,
                        padx=(6, 0), ipadx=10)

        breakdown = dp.get_cost_breakdown()
        breakdown_nonzero = {k: v for k, v in breakdown.items() if v > 0}

        if breakdown_nonzero:
            fig2, ax2 = self._make_figure(figsize=(3.5, 3.5))
            colors_pie = [PALETTE["warning"], PALETTE["accent"],
                          PALETTE["success"], PALETTE["primary"]]
            wedges, texts, autotexts = ax2.pie(
                list(breakdown_nonzero.values()),
                labels=list(breakdown_nonzero.keys()),
                autopct="%1.1f%%",
                colors=colors_pie[:len(breakdown_nonzero)],
                startangle=90,
                wedgeprops=dict(width=0.55, edgecolor="white"),
                pctdistance=0.78,
            )
            for t in texts:
                t.set_fontsize(9)
            for at in autotexts:
                at.set_fontsize(8)
                at.set_color("white")
                at.set_fontweight("bold")
            ax2.axis("equal")
            total_cost = sum(breakdown_nonzero.values())
            ax2.set_title(
                f"Итого: {total_cost:,.0f} ₽".replace(",", " "),
                fontsize=9, fontweight="bold", pad=10,
            )
            self._embed_figure(fig2, cost_frame)
        else:
            ttk.Label(cost_frame,
                      text="Нет данных\nо стоимости.",
                      justify="center").pack(pady=30, padx=20)

    # ----------------------------------------------------------
    #  TAB 1: ТРУДОЗАТРАТЫ
    # ----------------------------------------------------------

    def _build_labor_tab(self):
        self._clear_tab(self.tab_labor)
        dp = self.data_provider

        # KPI
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
             f"{kpi.get('overtime_share_pct', 0):.1f}",
             "%",
             PALETTE["negative"] if kpi.get("overtime_share_pct", 0) > 20
             else PALETTE["neutral"]),
        ]
        for i, (title, value, unit, color) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit, color=color)
            card.grid(row=0, column=i, padx=5, pady=4, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        # Графики
        charts = ttk.Frame(self.tab_labor)
        charts.pack(fill="both", expand=True, padx=10, pady=4)

        # Левый: ТОП объектов
        left = ttk.LabelFrame(charts, text="ТОП-10 объектов по трудозатратам")
        left.pack(side="left", fill="both", expand=True, padx=(0, 5))

        df_obj = dp.get_labor_by_object()
        if not df_obj.empty:
            df_obj["total_hours"]  = df_obj["total_hours"].fillna(0).astype(float)
            df_obj["object_name"]  = df_obj["object_name"].fillna("—")
            df_plot = df_obj.head(10).sort_values("total_hours", ascending=True).copy()
            df_plot["short"] = df_plot["object_name"].str[:32].str.strip() + \
                               df_plot["object_name"].apply(lambda s: "…" if len(s) > 32 else "")

            fig, ax = self._make_figure(figsize=(5, 4))
            bars = ax.barh(df_plot["short"], df_plot["total_hours"],
                           color=PALETTE["primary"], edgecolor="white", linewidth=0.5)
            max_v = float(df_plot["total_hours"].max() or 1)
            for bar in bars:
                w = float(bar.get_width() or 0)
                ax.text(w + max_v * 0.015, bar.get_y() + bar.get_height() / 2,
                        f"{w:,.0f}".replace(",", " "), va="center", fontsize=7,
                        color=PALETTE["neutral"])
            apply_chart_style(ax, xlabel="Человеко-часы")
            self._embed_figure(fig, left)
        else:
            ttk.Label(left, text="Нет данных.").pack(pady=20)

        # Правый: тренд + ТОП сотрудников
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
            ax.fill_between(df_trend["period"], df_trend["total_hours"],
                            alpha=0.18, color=PALETTE["primary"])
            ax.plot(df_trend["period"], df_trend["total_hours"],
                    marker="o", color=PALETTE["primary"], linewidth=2, markersize=5)
            ax.set_xticks(range(len(df_trend)))
            ax.set_xticklabels(df_trend["period"], rotation=45, ha="right", fontsize=7)
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
            ax.barh(df_plot_e["fio"], normal,
                    color=PALETTE["primary"], label="Норма", edgecolor="white")
            ax.barh(df_plot_e["fio"], df_plot_e["total_overtime"],
                    left=normal, color=PALETTE["negative"],
                    label="Переработка", alpha=0.85, edgecolor="white")
            ax.legend(fontsize=7, loc="lower right")
            apply_chart_style(ax, xlabel="Часы")
            self._embed_figure(fig, emp_f)
        else:
            ttk.Label(emp_f, text="Нет данных.").pack(pady=10)

        # Таблица по подразделениям
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
            rows = []
            for _, row in df_dept.iterrows():
                h = float(row["total_hours"])
                p = int(row["people_cnt"])
                rows.append((
                    row["department_name"],
                    p,
                    f"{h:,.1f}".replace(",", " "),
                    f"{h/p:.1f}" if p > 0 else "—",
                ))
            self._insert_rows(tree, rows)
        else:
            ttk.Label(dept_frame, text="Нет данных.").pack(pady=10)

    # ----------------------------------------------------------
    #  TAB 2: ТРАНСПОРТ
    # ----------------------------------------------------------

    def _build_transport_tab(self):
        self._clear_tab(self.tab_transport)
        dp = self.data_provider

        # KPI
        kpi_frame = tk.Frame(self.tab_transport, bg="#F0F2F5")
        kpi_frame.pack(fill="x", padx=10, pady=10)

        kpi = dp.get_transport_kpi()
        cards = [
            ("Всего маш.-часов",
             f"{kpi.get('total_machine_hours', 0):,.1f}".replace(",", " "),
             "час.", PALETTE["success"]),
            ("Всего заявок",
             int(kpi.get("total_orders", 0) or 0),
             "шт.", PALETTE["neutral"]),
            ("Единиц техники",
             f"{kpi.get('total_units', 0):.0f}",
             "шт.", PALETTE["neutral"]),
            ("Среднее на заявку",
             f"{kpi.get('avg_hours_per_order', 0):.1f}",
             "час/заявку", PALETTE["neutral"]),
            ("Часов на единицу",
             f"{kpi.get('hours_per_unit', 0):.1f}",
             "час/ед.", PALETTE["neutral"]),
        ]
        for i, (title, value, unit, color) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit, color=color)
            card.grid(row=0, column=i, padx=5, pady=4, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        # Графики
        charts = ttk.Frame(self.tab_transport)
        charts.pack(fill="both", expand=True, padx=10, pady=4)

        # Левый: ТОП техники
        left = ttk.LabelFrame(charts, text="ТОП-10 видов техники (маш.-часы)")
        left.pack(side="left", fill="both", expand=True, padx=(0, 5))

        df_tech = dp.get_transport_by_tech()
        if not df_tech.empty:
            df_tech["total_hours"] = df_tech["total_hours"].fillna(0).astype(float)
            df_tech["tech"]        = df_tech["tech"].fillna("—")
            df_plot = df_tech.head(10).sort_values("total_hours", ascending=True)

            fig, ax = self._make_figure(figsize=(5, 4))
            bars = ax.barh(df_plot["tech"], df_plot["total_hours"],
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

        # Правый: тренд + по объектам
        right = ttk.Frame(charts)
        right.pack(side="left", fill="both", expand=True, padx=(5, 0))

        trend_f = ttk.LabelFrame(right, text="Динамика маш.-часов по месяцам")
        trend_f.pack(fill="both", expand=True, pady=(0, 5))

        df_tr = dp.get_transport_trend()
        if not df_tr.empty and "period" in df_tr.columns:
            fig, ax = self._make_figure(figsize=(5, 2.4))
            ax.fill_between(df_tr["period"], df_tr["total_hours"],
                            alpha=0.18, color=PALETTE["success"])
            ax.plot(df_tr["period"], df_tr["total_hours"],
                    marker="o", color=PALETTE["success"], linewidth=2, markersize=5)
            ax.set_xticks(range(len(df_tr)))
            ax.set_xticklabels(df_tr["period"], rotation=45, ha="right", fontsize=7)
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
                    ("object",  "Объект"),
                    ("hours",   "Маш.-часы"),
                    ("orders",  "Заявок"),
                ],
                height=7,
            )
            tree.column("object", width=220)
            tree.column("hours",  width=100, anchor="e")
            tree.column("orders", width=80,  anchor="e")
            rows = [(r["object_name"],
                     f"{r['total_hours']:,.1f}".replace(",", " "),
                     int(r["order_count"]))
                    for _, r in df_obj.iterrows()]
            self._insert_rows(tree, rows)
        else:
            ttk.Label(obj_f, text="Нет данных.").pack(pady=10)

    # ----------------------------------------------------------
    #  TAB 3: ПИТАНИЕ
    # ----------------------------------------------------------

    def _build_meals_tab(self):
        self._clear_tab(self.tab_meals)
        dp = self.data_provider

        # KPI
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
             f"{total_qty:,.0f}".replace(",", " "),
             "шт.", PALETTE["warning"]),
            ("Всего заявок",   total_ord,   "шт.", PALETTE["neutral"]),
            ("Накормлено",     unique_emp,  "чел.", PALETTE["neutral"]),
            ("Порций/заявку",
             f"{kpi.get('avg_portions_per_order', 0):.1f}", "", PALETTE["neutral"]),
            ("Порций/чел.",
             f"{kpi.get('avg_portions_per_person', 0):.1f}", "", PALETTE["neutral"]),
            ("Стоимость (оценка)",
             f"{total_cost:,.0f}".replace(",", " "),
             "₽", PALETTE["warning"]),
        ]
        for i, (title, value, unit, color) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit, color=color)
            card.grid(row=0, column=i, padx=5, pady=4, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        top = ttk.Frame(self.tab_meals)
        top.pack(fill="both", expand=True, padx=10, pady=4)

        # Pie: типы питания
        pie_f = ttk.LabelFrame(top, text="Структура по типам питания")
        pie_f.pack(side="left", fill="both", expand=True, padx=(0, 5))

        df_types = dp.get_meals_by_type()
        if not df_types.empty:
            df_types["total_qty"]      = df_types["total_qty"].fillna(0).astype(float)
            df_types["meal_type_text"] = df_types["meal_type_text"].fillna("—")
            fig, ax = self._make_figure(figsize=(4.5, 3.8))
            colors_pie = SERIES_COLORS * 3
            wedges, texts, autotexts = ax.pie(
                df_types["total_qty"],
                labels=df_types["meal_type_text"],
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

        # Тренд: динамика порций
        trend_f = ttk.LabelFrame(top, text="Динамика порций по месяцам")
        trend_f.pack(side="left", fill="both", expand=True, padx=(5, 0))

        df_trend = dp.get_meals_trend_by_month()
        if not df_trend.empty:
            df_trend["total_qty"] = df_trend["total_qty"].fillna(0).astype(float)
            df_trend["period"]    = pd.to_datetime(df_trend["period"]).dt.strftime("%Y-%m")
            fig, ax = self._make_figure(figsize=(5, 3.8))
            ax.fill_between(df_trend["period"], df_trend["total_qty"],
                            alpha=0.18, color=PALETTE["warning"])
            ax.plot(df_trend["period"], df_trend["total_qty"],
                    marker="o", color=PALETTE["warning"], linewidth=2, markersize=5)
            ax.set_xticks(range(len(df_trend)))
            ax.set_xticklabels(df_trend["period"], rotation=45, ha="right", fontsize=7)
            apply_chart_style(ax, ylabel="Порций")
            self._embed_figure(fig, trend_f)
        else:
            ttk.Label(trend_f, text="Нет данных.").pack(pady=20)

        # Таблицы: объекты + подразделения
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

        for c in ("labor_hours","machine_hours","portions","meal_cost"):
            df[c] = df[c].fillna(0).astype(float)
        df["address"] = df["address"].fillna("—")

        outer = ttk.Frame(self.tab_objects)
        outer.pack(fill="both", expand=True, padx=10, pady=8)

        # Таблица
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

        # График: grouped bar (труд + транспорт)
        chart_f = ttk.LabelFrame(outer, text="Труд vs Транспорт (ТОП-10 объектов)")
        chart_f.pack(side="left", fill="both", expand=True, padx=(6, 0))

        df_top = df.head(10)
        fig, ax = self._make_figure(figsize=(5.5, 4.5))
        x     = list(range(len(df_top)))
        width = 0.38
        ax.bar([v - width/2 for v in x], df_top["labor_hours"],
               width=width, label="Чел.-часы",  color=PALETTE["primary"],  edgecolor="white")
        ax.bar([v + width/2 for v in x], df_top["machine_hours"],
               width=width, label="Маш.-часы",  color=PALETTE["success"],  edgecolor="white")
        ax.set_xticks(x)
        ax.set_xticklabels(
            [a[:14] + "…" if len(a) > 14 else a for a in df_top["address"]],
            rotation=45, ha="right", fontsize=7,
        )
        ax.legend(fontsize=8)
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
                      text="Нет активности пользователей за выбранный период.").pack(pady=20)
            return

        df["username"]  = df["username"].fillna("—")
        df["full_name"] = df["full_name"].fillna("")
        df["total_ops"] = (
            df["timesheets_created"] + df["transport_orders_created"]
            + df["meal_orders_created"] + df["dorm_checkins"] + df["dorm_checkouts"]
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
        for c in ("th","tr","mo","ci","co","total"):
            tree.column(c, width=80, anchor="e")

        self._insert_rows(tree, [
            (
                r["username"], r["full_name"] or "",
                int(r["timesheets_created"]),
                int(r["transport_orders_created"]),
                int(r["meal_orders_created"]),
                int(r["dorm_checkins"]),
                int(r["dorm_checkouts"]),
                int(r["total_ops"]),
            )
            for _, r in df.iterrows()
        ])

        # График активности
        chart_f = ttk.LabelFrame(outer, text="ТОП пользователей (стacked bar)")
        chart_f.pack(side="left", fill="both", expand=True, padx=(6, 0))

        df_top = df.sort_values("total_ops", ascending=False).head(10)
        fig, ax = self._make_figure(figsize=(5, 4.5))
        y     = list(range(len(df_top)))
        cats  = [
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
            left_vals = [l + v for l, v in zip(left_vals, vals)]

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

        # KPI
        kpi_frame = tk.Frame(self.tab_lodging, bg="#F0F2F5")
        kpi_frame.pack(fill="x", padx=10, pady=10)

        kpi = dp.get_lodging_kpi()
        bed_days   = int(kpi.get("bed_days", 0))
        amount_rub = float(kpi.get("amount_rub", 0))
        avg_price  = float(kpi.get("avg_price_rub", 0))
        active_end = int(kpi.get("active_on_end", 0))
        missing    = int(kpi.get("missing_rate_bed_days", 0))

        cards = [
            ("Койко-дней",       bed_days,
             "дней", PALETTE["accent"]),
            ("Начислено",
             f"{amount_rub:,.0f}".replace(",", " "),
             "₽", PALETTE["accent"]),
            ("Средняя цена",
             f"{avg_price:,.0f}".replace(",", " "),
             "₽/день", PALETTE["neutral"]),
            ("Проживает (на конец)", active_end,
             "чел.", PALETTE["primary"]),
            ("Без тарифа",       missing,
             "койко-дн.",
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

        # Верхняя часть: занятость
        top = ttk.Frame(self.tab_lodging)
        top.pack(fill="both", expand=False, padx=10, pady=4)

        occ_f = ttk.LabelFrame(top, text="Динамика занятости мест по дням")
        occ_f.pack(fill="both", expand=True)

        df_occ = dp.get_lodging_daily_occupancy()
        if not df_occ.empty:
            df_occ["d"]     = pd.to_datetime(df_occ["d"])
            df_occ["label"] = df_occ["d"].dt.strftime("%d.%m")
            fig, ax = self._make_figure(figsize=(11, 2.2))
            ax.fill_between(df_occ["label"], df_occ["occupied_beds"],
                            alpha=0.2, color=PALETTE["accent"])
            ax.plot(df_occ["label"], df_occ["occupied_beds"],
                    color=PALETTE["accent"], linewidth=1.5)
            step = max(1, len(df_occ) // 15)
            ax.set_xticks(list(range(0, len(df_occ), step)))
            ax.set_xticklabels(df_occ["label"].iloc[::step], rotation=0,
                               ha="center", fontsize=7)
            apply_chart_style(ax, ylabel="Мест занято")
            self._embed_figure(fig, occ_f)
        else:
            ttk.Label(occ_f, text="Нет данных.").pack(pady=10)

        # Нижняя часть: таблицы
        bottom = ttk.Frame(self.tab_lodging)
        bottom.pack(fill="both", expand=True, padx=10, pady=4)

        dorm_f = ttk.LabelFrame(bottom, text="ТОП общежитий (начислено, ₽)")
        dorm_f.pack(side="left", fill="both", expand=True, padx=(0, 5))

        df_dorm = dp.get_lodging_by_dorm(limit=10)
        if not df_dorm.empty:
            tree = self._create_treeview(
                dorm_f,
                columns=[
                    ("dorm",    "Общежитие"),
                    ("bd",      "Койко-дн."),
                    ("amount",  "Начислено, ₽"),
                    ("avg",     "₽/день"),
                    ("miss",    "Без тарифа"),
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

        dept_f = ttk.LabelFrame(bottom, text="По подразделениям (начислено, ₽)")
        dept_f.pack(side="left", fill="both", expand=True, padx=(5, 0))

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

        # Pivot: общежитие → объекты
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
            tree3 = self._create_treeview_with_hscroll(pivot_f, columns=cols, height=12)
            for c in cols:
                tree3.heading(c, text=c)
                tree3.column(c, width=240 if c == "Общежитие" else 110, anchor="w" if c == "Общежитие" else "e")
            rows_pv = []
            for dorm_name, row in pv.iterrows():
                rows_pv.append([dorm_name] + [int(row[col]) for col in pv.columns])
            self._insert_rows(tree3, rows_pv)

    # ----------------------------------------------------------
    #  ЭКСПОРТ В EXCEL
    # ----------------------------------------------------------

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

        dp = self.data_provider
        try:
            with pd.ExcelWriter(path, engine="openpyxl") as writer:

                # Лист 1: Сводка
                summary = dp.get_executive_summary()
                df_sum = pd.DataFrame([{
                    "Показатель": k, "Значение": v
                } for k, v in summary.items()])
                df_sum.to_excel(writer, sheet_name="Сводка", index=False)

                # Лист 2: Трудозатраты
                kpi_l = dp.get_labor_kpi()
                df_kpi_l = pd.DataFrame([kpi_l])
                df_kpi_l.to_excel(writer, sheet_name="Труд_KPI", index=False)

                df_dept_l = dp.get_labor_by_department()
                if not df_dept_l.empty:
                    df_dept_l.to_excel(writer, sheet_name="Труд_Подразделения", index=False)

                df_obj_l = dp.get_labor_by_object()
                if not df_obj_l.empty:
                    df_obj_l.to_excel(writer, sheet_name="Труд_Объекты", index=False)

                df_emp_l = dp.get_top_employees_by_hours(limit=50)
                if not df_emp_l.empty:
                    df_emp_l.to_excel(writer, sheet_name="Труд_ТОП_сотрудников", index=False)

                df_trend_l = dp.get_labor_trend_by_month()
                if not df_trend_l.empty:
                    df_trend_l.to_excel(writer, sheet_name="Труд_Тренд", index=False)

                # Лист 3: Транспорт
                kpi_t = dp.get_transport_kpi()
                df_kpi_t = pd.DataFrame([kpi_t])
                df_kpi_t.to_excel(writer, sheet_name="Транспорт_KPI", index=False)

                df_tech = dp.get_transport_by_tech()
                if not df_tech.empty:
                    df_tech.to_excel(writer, sheet_name="Транспорт_Техника", index=False)

                df_obj_t = dp.get_transport_by_object(limit=50)
                if not df_obj_t.empty:
                    df_obj_t.to_excel(writer, sheet_name="Транспорт_Объекты", index=False)

                df_trend_t = dp.get_transport_trend()
                if not df_trend_t.empty:
                    df_trend_t.to_excel(writer, sheet_name="Транспорт_Тренд", index=False)

                # Лист 4: Питание
                kpi_m = dp.get_meals_kpi()
                cost_m = dp.get_meals_cost_kpi()
                kpi_m.update(cost_m)
                df_kpi_m = pd.DataFrame([kpi_m])
                df_kpi_m.to_excel(writer, sheet_name="Питание_KPI", index=False)

                df_types_m = dp.get_meals_by_type()
                if not df_types_m.empty:
                    df_types_m.to_excel(writer, sheet_name="Питание_Типы", index=False)

                df_obj_m = dp.get_meals_by_object(limit=50)
                if not df_obj_m.empty:
                    df_obj_m.to_excel(writer, sheet_name="Питание_Объекты", index=False)

                df_dept_m = dp.get_meals_by_department()
                if not df_dept_m.empty:
                    df_dept_m.to_excel(writer, sheet_name="Питание_Подразделения", index=False)

                df_trend_m = dp.get_meals_trend_by_month()
                if not df_trend_m.empty:
                    df_trend_m.to_excel(writer, sheet_name="Питание_Тренд", index=False)

                # Лист 5: Объекты
                df_objects = dp.get_objects_rating(limit=50)
                if not df_objects.empty:
                    df_objects.to_excel(writer, sheet_name="Объекты_Рейтинг", index=False)

                # Лист 6: Пользователи
                df_users = dp.get_users_activity()
                if not df_users.empty:
                    df_users.to_excel(writer, sheet_name="Пользователи", index=False)

                # Лист 7: Проживание
                kpi_ld = dp.get_lodging_kpi()
                df_kpi_ld = pd.DataFrame([kpi_ld])
                df_kpi_ld.to_excel(writer, sheet_name="Проживание_KPI", index=False)

                df_dorm_ld = dp.get_lodging_by_dorm(limit=50)
                if not df_dorm_ld.empty:
                    df_dorm_ld.to_excel(writer, sheet_name="Проживание_Общежития", index=False)

                df_dept_ld = dp.get_lodging_by_department()
                if not df_dept_ld.empty:
                    df_dept_ld.to_excel(writer, sheet_name="Проживание_Подразделения", index=False)

                df_occ_ld = dp.get_lodging_daily_occupancy()
                if not df_occ_ld.empty:
                    df_occ_ld.to_excel(writer, sheet_name="Проживание_Занятость", index=False)

                pv = dp.get_dorm_to_objects_people_pivot(top_objects=15, top_dorms=50)
                if pv is not None and not pv.empty:
                    pv.to_excel(writer, sheet_name="Проживание_Pivot")

                # ── Авто-ширина колонок для всех листов ──────────
                try:
                    from openpyxl.utils import get_column_letter
                    for sheet_name in writer.sheets:
                        ws = writer.sheets[sheet_name]
                        for col_cells in ws.columns:
                            max_len = 0
                            col_letter = get_column_letter(col_cells[0].column)
                            for cell in col_cells:
                                try:
                                    cell_len = len(str(cell.value)) if cell.value else 0
                                    max_len = max(max_len, cell_len)
                                except Exception:
                                    pass
                            ws.column_dimensions[col_letter].width = min(max_len + 4, 60)
                except Exception as fmt_err:
                    logging.warning(f"Не удалось настроить ширину колонок: {fmt_err}")

            messagebox.showinfo(
                "Экспорт завершён",
                f"Файл успешно сохранён:\n{path}"
            )

        except Exception as e:
            logging.exception("Ошибка экспорта в Excel")
            messagebox.showerror("Ошибка экспорта", f"Не удалось сохранить файл:\n{e}")
