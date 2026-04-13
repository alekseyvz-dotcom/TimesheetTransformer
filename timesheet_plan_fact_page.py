import logging
import threading
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Any, Tuple

import tkinter as tk
from tkinter import ttk, messagebox, filedialog

import pandas as pd

from psycopg2 import pool
from psycopg2.extras import RealDictCursor


db_connection_pool: Optional[pool.SimpleConnectionPool] = None


def set_db_pool(db_pool: pool.SimpleConnectionPool):
    global db_connection_pool
    db_connection_pool = db_pool
    logging.info("Timesheet Plan/Fact Module: DB pool set.")


PALETTE = {
    "primary": "#1565C0",
    "success": "#2E7D32",
    "warning": "#E65100",
    "accent": "#6A1B9A",
    "neutral": "#546E7A",
    "bg_card": "#F5F7FA",
    "positive": "#2E7D32",
    "negative": "#C62828",
    "text_muted": "#78909C",
    "offday": "#90A4AE",
    "noschedule": "#8E24AA",
}


class TimesheetPlanFactData:
    def __init__(self, start_date: date, end_date: date, object_type_filter: str = ""):
        self.start_date = start_date
        self.end_date = end_date
        self.object_type_filter = object_type_filter or ""

        self._detail_cache: Optional[pd.DataFrame] = None
        self._daily_cache: Optional[pd.DataFrame] = None
        self._date_cache: Optional[pd.DataFrame] = None
        self._object_cache: Optional[pd.DataFrame] = None
        self._position_cache: Optional[pd.DataFrame] = None
        self._kpi_cache: Optional[Dict[str, Any]] = None

    def _execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        if not db_connection_pool:
            raise ConnectionError("Пул соединений с БД не инициализирован.")

        conn = None
        try:
            conn = db_connection_pool.getconn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                return [dict(r) for r in cur.fetchall()]
        finally:
            if conn:
                db_connection_pool.putconn(conn)

    def get_object_types(self) -> List[str]:
        rows = self._execute_query(
            """
            SELECT DISTINCT short_name
            FROM objects
            WHERE COALESCE(short_name, '') <> ''
            ORDER BY short_name
            """
        )
        return [r["short_name"] for r in rows]

    def _build_base_cte(self) -> Tuple[str, List[Any]]:
        params: List[Any] = [self.end_date, self.start_date]
        object_filter_sql = ""

        if self.object_type_filter:
            object_filter_sql = " AND COALESCE(o.short_name, '') = %s "
            params.append(self.object_type_filter)

        sql = f"""
        WITH employee_match AS (
            SELECT
                tr.id AS row_id,
                e.id AS employee_id,
                e.position AS emp_position,
                e.department_id AS emp_department_id,
                e.work_schedule AS emp_work_schedule,
                ROW_NUMBER() OVER (
                    PARTITION BY tr.id
                    ORDER BY
                        CASE
                            WHEN NULLIF(BTRIM(tr.tbn), '') IS NOT NULL
                                 AND NULLIF(BTRIM(e.tbn), '') = NULLIF(BTRIM(tr.tbn), '')
                            THEN 1
                            WHEN NULLIF(BTRIM(tr.tbn), '') IS NULL
                                 AND LOWER(REGEXP_REPLACE(BTRIM(e.fio), '\\s+', ' ', 'g'))
                                     = LOWER(REGEXP_REPLACE(BTRIM(tr.fio), '\\s+', ' ', 'g'))
                            THEN 2
                            ELSE 100
                        END,
                        e.id
                ) AS rn
            FROM timesheet_rows tr
            LEFT JOIN employees e
              ON (
                    NULLIF(BTRIM(tr.tbn), '') IS NOT NULL
                    AND NULLIF(BTRIM(e.tbn), '') = NULLIF(BTRIM(tr.tbn), '')
                 )
                 OR (
                    NULLIF(BTRIM(tr.tbn), '') IS NULL
                    AND LOWER(REGEXP_REPLACE(BTRIM(e.fio), '\\s+', ' ', 'g'))
                        = LOWER(REGEXP_REPLACE(BTRIM(tr.fio), '\\s+', ' ', 'g'))
                 )
        ),
        src AS (
            SELECT
                th.id AS header_id,
                th.object_id,
                th.object_db_id,
                COALESCE(o.address, th.object_addr, '—') AS object_name,
                COALESCE(o.short_name, '') AS object_type,
                th.year,
                th.month,
                tr.id AS row_id,
                tr.fio,
                tr.tbn,
                tr.hours_raw,
                COALESCE(dep_emp.name, dep_hdr.name, th.department, '—') AS department_name,
                COALESCE(em.emp_position, '—') AS position_name,
                COALESCE(em.emp_work_schedule, '') AS work_schedule_name,
                CASE
                    WHEN NULLIF(BTRIM(tr.tbn), '') IS NOT NULL
                        THEN 'tbn:' || BTRIM(tr.tbn)
                    ELSE 'fio:' || LOWER(REGEXP_REPLACE(BTRIM(tr.fio), '\\s+', ' ', 'g'))
                END AS person_key
            FROM timesheet_headers th
            JOIN timesheet_rows tr
              ON tr.header_id = th.id
            LEFT JOIN objects o
              ON o.id = th.object_db_id
            LEFT JOIN departments dep_hdr
              ON dep_hdr.id = th.department_id
            LEFT JOIN employee_match em
              ON em.row_id = tr.id
             AND em.rn = 1
            LEFT JOIN departments dep_emp
              ON dep_emp.id = em.emp_department_id
            WHERE make_date(th.year, th.month, 1) <= %s
              AND (
                    date_trunc('month', make_date(th.year, th.month, 1))
                    + interval '1 month - 1 day'
                  )::date >= %s
              {object_filter_sql}
        ),
        person_days AS (
            SELECT
                (
                    make_date(src.year, src.month, 1)
                    + (gs.day_num - 1) * interval '1 day'
                )::date AS work_date,
                src.object_id,
                src.object_db_id,
                src.object_name,
                src.object_type,
                src.department_name,
                src.position_name,
                src.fio,
                src.tbn,
                src.person_key,
                src.work_schedule_name,
                src.hours_raw[gs.day_num] AS raw_val
            FROM src
            CROSS JOIN LATERAL generate_series(1, 31) AS gs(day_num)
            WHERE gs.day_num <= EXTRACT(
                      DAY FROM (
                          date_trunc('month', make_date(src.year, src.month, 1))
                          + interval '1 month - 1 day'
                      )
                  )
              AND (
                    make_date(src.year, src.month, 1)
                    + (gs.day_num - 1) * interval '1 day'
                  )::date BETWEEN %s AND %s
        ),
        enriched AS (
            SELECT
                pd.work_date,
                pd.object_id,
                pd.object_db_id,
                pd.object_name,
                pd.object_type,
                pd.department_name,
                pd.position_name,
                pd.fio,
                pd.tbn,
                pd.person_key,
                pd.work_schedule_name,
                pd.raw_val,
                CASE
                    WHEN pd.raw_val IS NULL THEN 0
                    WHEN BTRIM(pd.raw_val) = '' THEN 0
                    WHEN substring(replace(pd.raw_val, ',', '.') FROM '([0-9]+(?:\\.[0-9]+)?)') IS NOT NULL
                    THEN COALESCE(substring(replace(pd.raw_val, ',', '.') FROM '([0-9]+(?:\\.[0-9]+)?)')::numeric, 0)
                    ELSE 0
                END AS hours_val,
                ws.id AS schedule_id,
                wsd.is_workday,
                COALESCE(wsd.planned_hours, 0) AS planned_hours
            FROM person_days pd
            LEFT JOIN work_schedules ws
              ON LOWER(BTRIM(ws.schedule_name)) = LOWER(BTRIM(pd.work_schedule_name))
             AND ws.year = EXTRACT(YEAR FROM pd.work_date)::int
            LEFT JOIN work_schedule_days wsd
              ON wsd.schedule_id = ws.id
             AND wsd.work_date = pd.work_date
        )
        """
        params.extend([self.start_date, self.end_date])
        return sql, params

    def get_daily_detail(self) -> pd.DataFrame:
        if self._detail_cache is not None:
            return self._detail_cache.copy()

        cte, params = self._build_base_cte()
        query = cte + """
        SELECT
            work_date,
            object_id,
            object_db_id,
            object_name,
            object_type,
            department_name,
            position_name,
            fio,
            tbn,
            person_key,
            work_schedule_name,
            raw_val AS day_raw,
            hours_val AS hours,
            CASE WHEN hours_val > 0 THEN 1 ELSE 0 END AS worked_flag,
            CASE WHEN schedule_id IS NULL THEN 1 ELSE 0 END AS no_schedule_flag,
            CASE WHEN schedule_id IS NOT NULL AND is_workday IS TRUE THEN 1 ELSE 0 END AS plan_flag,
            CASE WHEN schedule_id IS NOT NULL AND is_workday IS FALSE THEN 1 ELSE 0 END AS off_flag,
            CASE WHEN schedule_id IS NOT NULL AND is_workday IS TRUE  AND hours_val > 0 THEN 1 ELSE 0 END AS fact_flag,
            CASE WHEN schedule_id IS NOT NULL AND is_workday IS TRUE  AND hours_val <= 0 THEN 1 ELSE 0 END AS absent_flag,
            CASE WHEN schedule_id IS NOT NULL AND is_workday IS FALSE AND hours_val > 0 THEN 1 ELSE 0 END AS worked_on_off_flag,
            is_workday,
            planned_hours
        FROM enriched
        ORDER BY
            work_date,
            object_name,
            department_name,
            position_name,
            fio
        """

        rows = self._execute_query(query, tuple(params))
        df = pd.DataFrame(rows)

        if df.empty:
            self._detail_cache = pd.DataFrame(
                columns=[
                    "work_date", "object_id", "object_db_id", "object_name", "object_type",
                    "department_name", "position_name", "fio", "tbn", "person_key",
                    "work_schedule_name", "day_raw", "hours", "worked_flag",
                    "no_schedule_flag", "plan_flag", "off_flag", "fact_flag",
                    "absent_flag", "worked_on_off_flag", "is_workday", "planned_hours"
                ]
            )
            return self._detail_cache.copy()

        df["work_date"] = pd.to_datetime(df["work_date"])
        int_cols = [
            "worked_flag", "no_schedule_flag", "plan_flag", "off_flag",
            "fact_flag", "absent_flag", "worked_on_off_flag"
        ]
        for col in int_cols:
            df[col] = df[col].fillna(0).astype(int)

        df["hours"] = pd.to_numeric(df["hours"], errors="coerce").fillna(0.0)
        df["planned_hours"] = pd.to_numeric(df["planned_hours"], errors="coerce").fillna(0.0)

        self._detail_cache = df.copy()
        return df

    def get_plan_fact_daily(self) -> pd.DataFrame:
        if self._daily_cache is not None:
            return self._daily_cache.copy()

        cte, params = self._build_base_cte()
        query = cte + """
        SELECT
            work_date,
            object_id,
            object_name,
            department_name,
            position_name,
            SUM(CASE WHEN schedule_id IS NOT NULL AND is_workday IS TRUE THEN 1 ELSE 0 END)::int AS plan_count,
            SUM(CASE WHEN schedule_id IS NOT NULL AND is_workday IS FALSE THEN 1 ELSE 0 END)::int AS off_count,
            SUM(CASE WHEN schedule_id IS NOT NULL AND is_workday IS TRUE  AND hours_val > 0 THEN 1 ELSE 0 END)::int AS fact_count,
            SUM(CASE WHEN schedule_id IS NOT NULL AND is_workday IS TRUE  AND hours_val <= 0 THEN 1 ELSE 0 END)::int AS absent_count,
            SUM(CASE WHEN schedule_id IS NOT NULL AND is_workday IS FALSE AND hours_val > 0 THEN 1 ELSE 0 END)::int AS worked_on_off_count,
            SUM(CASE WHEN schedule_id IS NULL THEN 1 ELSE 0 END)::int AS no_schedule_count,
            CASE
                WHEN SUM(CASE WHEN schedule_id IS NOT NULL AND is_workday IS TRUE THEN 1 ELSE 0 END) > 0
                THEN ROUND(
                    SUM(CASE WHEN schedule_id IS NOT NULL AND is_workday IS TRUE AND hours_val > 0 THEN 1 ELSE 0 END)::numeric
                    / SUM(CASE WHEN schedule_id IS NOT NULL AND is_workday IS TRUE THEN 1 ELSE 0 END)::numeric * 100, 1
                )
                ELSE 0
            END AS attendance_pct
        FROM enriched
        GROUP BY
            work_date,
            object_id,
            object_name,
            department_name,
            position_name
        ORDER BY
            work_date,
            object_name,
            department_name,
            position_name
        """

        rows = self._execute_query(query, tuple(params))
        df = pd.DataFrame(rows)
        if not df.empty:
            df["work_date"] = pd.to_datetime(df["work_date"])
            for col in [
                "plan_count", "off_count", "fact_count", "absent_count",
                "worked_on_off_count", "no_schedule_count"
            ]:
                df[col] = df[col].fillna(0).astype(int)
            df["attendance_pct"] = pd.to_numeric(df["attendance_pct"], errors="coerce").fillna(0.0)

        self._daily_cache = df.copy()
        return df

    def get_plan_fact_by_date(self) -> pd.DataFrame:
        if self._date_cache is not None:
            return self._date_cache.copy()

        df = self.get_plan_fact_daily()
        if df.empty:
            self._date_cache = pd.DataFrame(columns=[
                "work_date", "plan_count", "off_count", "fact_count", "absent_count",
                "worked_on_off_count", "no_schedule_count", "attendance_pct"
            ])
            return self._date_cache.copy()

        grp = (
            df.groupby("work_date", as_index=False)
            .agg({
                "plan_count": "sum",
                "off_count": "sum",
                "fact_count": "sum",
                "absent_count": "sum",
                "worked_on_off_count": "sum",
                "no_schedule_count": "sum",
            })
            .sort_values("work_date")
        )

        grp["attendance_pct"] = grp.apply(
            lambda r: round(r["fact_count"] / r["plan_count"] * 100.0, 1)
            if r["plan_count"] > 0 else 0.0,
            axis=1
        )

        self._date_cache = grp.copy()
        return grp

    def get_plan_fact_by_object(self) -> pd.DataFrame:
        if self._object_cache is not None:
            return self._object_cache.copy()

        df = self.get_plan_fact_daily()
        if df.empty:
            self._object_cache = grp.apply(
            lambda r: round(r["fact_count"] / r["plan_count"] * 100.0, 1)
            if r["plan_count"] > 0 else 0.0,
            axis=1
        )

        self._object_cache = grp.copy()
        return grp

    def get_plan_fact_by_position(self) -> pd.DataFrame:
        if self._position_cache is not None:
            return self._position_cache.copy()

        df = self.get_plan_fact_daily()
        if df.empty:
            self._position_cache = pd.DataFrame(columns=[
                "department_name", "position_name", "plan_count", "off_count",
                "fact_count", "absent_count", "worked_on_off_count",
                "no_schedule_count", "attendance_pct"
            ])
            return self._position_cache.copy()

        grp = (
            df.groupby(["department_name", "position_name"], as_index=False)
            .agg({
                "plan_count": "sum",
                "off_count": "sum",
                "fact_count": "sum",
                "absent_count": "sum",
                "worked_on_off_count": "sum",
                "no_schedule_count": "sum",
            })
            .sort_values(
                ["plan_count", "fact_count", "department_name", "position_name"],
                ascending=[False, False, True, True]
            )
        )

        grp["attendance_pct"] = grp.apply(
            lambda r: round(r["fact_count"] / r["plan_count"] * 100.0, 1)
            if r["plan_count"] > 0 else 0.0,
            axis=1
        )

        self._position_cache = grp.copy()
        return grp

    def get_plan_fact_kpi(self) -> Dict[str, Any]:
        if self._kpi_cache is not None:
            return dict(self._kpi_cache)

        df_daily = self.get_plan_fact_daily()
        df_detail = self.get_daily_detail()

        if df_daily.empty:
            self._kpi_cache = {
                "plan_total": 0,
                "off_total": 0,
                "fact_total": 0,
                "absent_total": 0,
                "worked_on_off_total": 0,
                "no_schedule_total": 0,
                "attendance_pct": 0.0,
                "days_count": 0,
                "objects_count": 0,
                "employees_count": 0,
            }
            return dict(self._kpi_cache)

        plan_total = int(df_daily["plan_count"].sum())
        off_total = int(df_daily["off_count"].sum())
        fact_total = int(df_daily["fact_count"].sum())
        absent_total = int(df_daily["absent_count"].sum())
        worked_on_off_total = int(df_daily["worked_on_off_count"].sum())
        no_schedule_total = int(df_daily["no_schedule_count"].sum())

        self._kpi_cache = {
            "plan_total": plan_total,
            "off_total": off_total,
            "fact_total": fact_total,
            "absent_total": absent_total,
            "worked_on_off_total": worked_on_off_total,
            "no_schedule_total": no_schedule_total,
            "attendance_pct": round(fact_total / plan_total * 100.0, 1) if plan_total > 0 else 0.0,
            "days_count": int(df_daily["work_date"].nunique()),
            "objects_count": int(df_detail["object_name"].nunique()) if not df_detail.empty else 0,
            "employees_count": int(df_detail["person_key"].nunique()) if not df_detail.empty else 0,
        }
        return dict(self._kpi_cache)


class TimesheetPlanFactPage(ttk.Frame):
    def __init__(self, master, app_ref=None):
        super().__init__(master)
        self.app_ref = app_ref
        self.data_provider: Optional[TimesheetPlanFactData] = None
        self._loading = False

        self._build_header()
        self._build_body()
        self.load_filters()

        self.after(100, self.refresh_data)

    def _build_header(self):
        hdr = ttk.Frame(self, padding="8 6 8 6")
        hdr.pack(fill="x", side="top")

        ttk.Label(hdr, text="Период:").pack(side="left", padx=(0, 4))
        self.period_var = tk.StringVar(value="Текущий месяц")
        self.period_cb = ttk.Combobox(
            hdr,
            textvariable=self.period_var,
            values=["Текущий месяц", "Прошлый месяц", "Текущий квартал", "Текущий год"],
            state="readonly",
            width=18,
        )
        self.period_cb.pack(side="left", padx=4)
        self.period_cb.bind("<<ComboboxSelected>>", self.refresh_data)

        ttk.Label(hdr, text="Тип объекта:").pack(side="left", padx=(12, 4))
        self.object_type_var = tk.StringVar(value="Все типы")
        self.object_type_combo = ttk.Combobox(
            hdr,
            textvariable=self.object_type_var,
            state="readonly",
            width=28,
        )
        self.object_type_combo.pack(side="left", padx=4)
        self.object_type_combo.bind("<<ComboboxSelected>>", self.refresh_data)

        self.btn_refresh = ttk.Button(hdr, text="⟳ Обновить", command=self.refresh_data)
        self.btn_refresh.pack(side="left", padx=10)

        self.btn_export = ttk.Button(hdr, text="📥 Экспорт в Excel", command=self.export_to_excel)
        self.btn_export.pack(side="left", padx=4)

        self.last_update_var = tk.StringVar(value="")
        ttk.Label(
            hdr,
            textvariable=self.last_update_var,
            font=("Segoe UI", 8),
            foreground=PALETTE["text_muted"],
        ).pack(side="right", padx=8)

    def _build_body(self):
        self.canvas = tk.Canvas(self, highlightthickness=0, bg="#F5F7FA")
        self.v_scroll = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.inner = ttk.Frame(self.canvas)

        self.inner.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )

        self.canvas_window = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.canvas.configure(yscrollcommand=self.v_scroll.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.v_scroll.pack(side="right", fill="y")

        self.canvas.bind(
            "<Configure>",
            lambda e: self.canvas.itemconfig(self.canvas_window, width=e.width)
        )

    def _set_loading_state(self, loading: bool, status_text: str = ""):
        self._loading = loading

        state = "disabled" if loading else "readonly"
        btn_state = "disabled" if loading else "normal"

        try:
            self.period_cb.configure(state=state)
            self.object_type_combo.configure(state=state)
            self.btn_refresh.configure(state=btn_state)
            self.btn_export.configure(state=btn_state if self.data_provider else "disabled")
        except Exception:
            pass

        if status_text:
            self.last_update_var.set(status_text)

    def _clear_inner(self):
        for w in self.inner.winfo_children():
            w.destroy()

    def _show_loading_placeholder(self, text: str = "Загрузка данных..."):
        self._clear_inner()
        box = ttk.Frame(self.inner)
        box.pack(fill="both", expand=True, padx=20, pady=30)
        ttk.Label(box, text=text, font=("Segoe UI", 11)).pack(pady=20)

    def load_filters(self):
        try:
            types = TimesheetPlanFactData(datetime.now().date(), datetime.now().date(), "").get_object_types()
            self.object_type_combo["values"] = ["Все типы"] + types
        except Exception:
            logging.exception("Не удалось загрузить типы объектов")
            self.object_type_combo["values"] = ["Все типы"]

    def get_dates_from_period(self) -> Tuple[date, date]:
        period = self.period_var.get()
        today = datetime.today()

        if period == "Текущий месяц":
            start = today.replace(day=1)
            end = (start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        elif period == "Прошлый месяц":
            end = today.replace(day=1) - timedelta(days=1)
            start = end.replace(day=1)
        elif period == "Текущий квартал":
            q = (today.month - 1) // 3 + 1
            start = datetime(today.year, 3 * q - 2, 1)
            end = (start + timedelta(days=95)).replace(day=1) - timedelta(days=1)
        elif period == "Текущий год":
            start = datetime(today.year, 1, 1)
            end = datetime(today.year, 12, 31)
        else:
            start = today.replace(day=1)
            end = (start + timedelta(days=32)).replace(day=1) - timedelta(days=1)

        return start.date(), end.date()

    def refresh_data(self, event=None):
        if self._loading:
            return

        start_date, end_date = self.get_dates_from_period()
        obj_filter = self.object_type_var.get()
        if obj_filter == "Все типы":
            obj_filter = ""

        self._set_loading_state(
            True,
            f"Загрузка... {start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')}"
        )
        self._show_loading_placeholder("Загрузка аналитики план/факт...")

        def worker():
            try:
                dp = TimesheetPlanFactData(start_date, end_date, obj_filter)

                dp.get_plan_fact_kpi()
                dp.get_plan_fact_by_object()
                dp.get_plan_fact_by_position()
                dp.get_plan_fact_by_date()
                dp.get_plan_fact_daily()

                self.after(0, lambda: self._on_data_loaded(dp, start_date, end_date))
            except Exception as e:
                logging.exception("Ошибка загрузки план/факт")
                self.after(0, lambda: self._on_data_error(e))

        threading.Thread(target=worker, daemon=True).start()

    def _on_data_loaded(self, dp: TimesheetPlanFactData, start_date: date, end_date: date):
        self.data_provider = dp
        self._render()
        self._set_loading_state(
            False,
            f"Обновлено: {datetime.now().strftime('%H:%M:%S')}  |  "
            f"{start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')}"
        )

    def _on_data_error(self, error: Exception):
        self._set_loading_state(False, "Ошибка загрузки данных")
        self._show_loading_placeholder("Не удалось загрузить данные.")
        messagebox.showerror("План / факт", f"Не удалось загрузить данные:\n{error}")

    def _create_card(self, parent, title: str, value: str, unit: str, color: str):
        card = tk.Frame(parent, bg="white", highlightbackground="#E0E0E0", highlightthickness=1)
        accent = tk.Frame(card, bg=color, height=4)
        accent.pack(fill="x", side="top")

        inner = tk.Frame(card, bg="white", padx=10, pady=10)
        inner.pack(fill="both", expand=True)

        tk.Label(
            inner,
            text=title,
            font=("Segoe UI", 9),
            fg=PALETTE["text_muted"],
            bg="white",
            wraplength=160,
            justify="center"
        ).pack()

        tk.Label(
            inner,
            text=value,
            font=("Segoe UI", 20, "bold"),
            fg=color,
            bg="white"
        ).pack(pady=(4, 0))

        tk.Label(
            inner,
            text=unit,
            font=("Segoe UI", 8),
            fg=PALETTE["text_muted"],
            bg="white"
        ).pack()

        return card

    def _create_treeview(self, parent, columns: List[Tuple[str, str]], height: int = 10) -> ttk.Treeview:
        tree = ttk.Treeview(parent, columns=[c[0] for c in columns], show="headings", height=height)

        for col_id, col_text in columns:
            tree.heading(col_id, text=col_text)
            tree.column(col_id, anchor="w", width=120)

        tree.tag_configure("odd", background="#F5F7FA")
        tree.tag_configure("even", background="white")

        vsb = ttk.Scrollbar(parent, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(parent, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        parent.grid_rowconfigure(0, weight=1)
        parent.grid_columnconfigure(0, weight=1)

        return tree

    def _insert_rows(self, tree: ttk.Treeview, rows: list):
        for i, vals in enumerate(rows):
            tag = "odd" if i % 2 == 0 else "even"
            tree.insert("", "end", values=vals, tags=(tag,))

    def _render(self):
        self._clear_inner()
        dp = self.data_provider
        if not dp:
            return

        kpi = dp.get_plan_fact_kpi()

        kpi_frame = tk.Frame(self.inner, bg="#F0F2F5")
        kpi_frame.pack(fill="x", padx=10, pady=10)

        cards = [
            ("План", f"{kpi.get('plan_total', 0):,}".replace(",", " "), "чел.-дн.", PALETTE["primary"]),
            ("Выходной", f"{kpi.get('off_total', 0):,}".replace(",", " "), "чел.-дн.", PALETTE["offday"]),
            ("Факт", f"{kpi.get('fact_total', 0):,}".replace(",", " "), "чел.-дн.", PALETTE["success"]),
            ("Не вышли", f"{kpi.get('absent_total', 0):,}".replace(",", " "), "чел.-дн.", PALETTE["negative"]),
            ("Вышли в выходной", f"{kpi.get('worked_on_off_total', 0):,}".replace(",", " "), "чел.-дн.", PALETTE["warning"]),
            ("Без графика", f"{kpi.get('no_schedule_total', 0):,}".replace(",", " "), "чел.-дн.", PALETTE["noschedule"]),
            ("Явка", f"{kpi.get('attendance_pct', 0):.1f}", "%", PALETTE["accent"]),
        ]

        for i, (title, value, unit, color) in enumerate(cards):
            card = self._create_card(kpi_frame, title, value, unit, color)
            card.grid(row=0, column=i, padx=5, pady=4, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        top = ttk.Frame(self.inner)
        top.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        left = ttk.LabelFrame(top, text="По объектам")
        left.pack(side="left", fill="both", expand=True, padx=(0, 5))

        right = ttk.LabelFrame(top, text="По подразделениям / должностям")
        right.pack(side="left", fill="both", expand=True, padx=(5, 0))

        df_obj = dp.get_plan_fact_by_object()
        if not df_obj.empty:
            obj_wrap = ttk.Frame(left)
            obj_wrap.pack(fill="both", expand=True, padx=4, pady=4)

            tree_obj = self._create_treeview(
                obj_wrap,
                columns=[
                    ("object", "Объект"),
                    ("plan", "План"),
                    ("off", "Выходной"),
                    ("fact", "Факт"),
                    ("absent", "Не вышли"),
                    ("workoff", "Вышли в вых."),
                    ("noschedule", "Без графика"),
                    ("pct", "% явки"),
                ],
                height=12,
            )
            tree_obj.column("object", width=280)
            tree_obj.column("plan", width=80, anchor="e")
            tree_obj.column("off", width=80, anchor="e")
            tree_obj.column("fact", width=80, anchor="e")
            tree_obj.column("absent", width=90, anchor="e")
            tree_obj.column("workoff", width=100, anchor="e")
            tree_obj.column("noschedule", width=100, anchor="e")
            tree_obj.column("pct", width=80, anchor="e")

            self._insert_rows(tree_obj, [
                (
                    r["object_name"],
                    int(r["plan_count"]),
                    int(r["off_count"]),
                    int(r["fact_count"]),
                    int(r["absent_count"]),
                    int(r["worked_on_off_count"]),
                    int(r["no_schedule_count"]),
                    f"{float(r['attendance_pct']):.1f}",
                )
                for _, r in df_obj.iterrows()
            ])
        else:
            ttk.Label(left, text="Нет данных.").pack(pady=20)

        df_pos = dp.get_plan_fact_by_position()
        if not df_pos.empty:
            pos_wrap = ttk.Frame(right)
            pos_wrap.pack(fill="both", expand=True, padx=4, pady=4)

            tree_pos = self._create_treeview(
                pos_wrap,
                columns=[
                    ("department", "Подразделение"),
                    ("position", "Должность"),
                    ("plan", "План"),
                    ("off", "Выходной"),
                    ("fact", "Факт"),
                    ("absent", "Не вышли"),
                    ("workoff", "Вышли в вых."),
                    ("noschedule", "Без графика"),
                    ("pct", "% явки"),
                ],
                height=12,
            )
            tree_pos.column("department", width=180)
            tree_pos.column("position", width=180)
            tree_pos.column("plan", width=80, anchor="e")
            tree_pos.column("off", width=80, anchor="e")
            tree_pos.column("fact", width=80, anchor="e")
            tree_pos.column("absent", width=90, anchor="e")
            tree_pos.column("workoff", width=100, anchor="e")
            tree_pos.column("noschedule", width=100, anchor="e")
            tree_pos.column("pct", width=80, anchor="e")

            self._insert_rows(tree_pos, [
                (
                    r["department_name"],
                    r["position_name"],
                    int(r["plan_count"]),
                    int(r["off_count"]),
                    int(r["fact_count"]),
                    int(r["absent_count"]),
                    int(r["worked_on_off_count"]),
                    int(r["no_schedule_count"]),
                    f"{float(r['attendance_pct']):.1f}",
                )
                for _, r in df_pos.iterrows()
            ])
        else:
            ttk.Label(right, text="Нет данных.").pack(pady=20)

        mid = ttk.LabelFrame(self.inner, text="По датам")
        mid.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        df_date = dp.get_plan_fact_by_date()
        if not df_date.empty:
            date_wrap = ttk.Frame(mid)
            date_wrap.pack(fill="both", expand=True, padx=4, pady=4)

            tree_date = self._create_treeview(
                date_wrap,
                columns=[
                    ("date", "Дата"),
                    ("plan", "План"),
                    ("off", "Выходной"),
                    ("fact", "Факт"),
                    ("absent", "Не вышли"),
                    ("workoff", "Вышли в вых."),
                    ("noschedule", "Без графика"),
                    ("pct", "% явки"),
                ],
                height=10,
            )
            tree_date.column("date", width=100)
            tree_date.column("plan", width=90, anchor="e")
            tree_date.column("off", width=90, anchor="e")
            tree_date.column("fact", width=90, anchor="e")
            tree_date.column("absent", width=100, anchor="e")
            tree_date.column("workoff", width=110, anchor="e")
            tree_date.column("noschedule", width=110, anchor="e")
            tree_date.column("pct", width=90, anchor="e")

            self._insert_rows(tree_date, [
                (
                    pd.to_datetime(r["work_date"]).strftime("%d.%m.%Y"),
                    int(r["plan_count"]),
                    int(r["off_count"]),
                    int(r["fact_count"]),
                    int(r["absent_count"]),
                    int(r["worked_on_off_count"]),
                    int(r["no_schedule_count"]),
                    f"{float(r['attendance_pct']):.1f}",
                )
                for _, r in df_date.iterrows()
            ])
        else:
            ttk.Label(mid, text="Нет данных.").pack(pady=20)

        bottom = ttk.LabelFrame(self.inner, text="Детализация")
        bottom.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        df_det = dp.get_plan_fact_daily()
        if not df_det.empty:
            det_wrap = ttk.Frame(bottom)
            det_wrap.pack(fill="both", expand=True, padx=4, pady=4)

            tree_det = self._create_treeview(
                det_wrap,
                columns=[
                    ("date", "Дата"),
                    ("object", "Объект"),
                    ("dept", "Подразделение"),
                    ("pos", "Должность"),
                    ("plan", "План"),
                    ("off", "Выходной"),
                    ("fact", "Факт"),
                    ("absent", "Не вышли"),
                    ("workoff", "Вышли в вых."),
                    ("noschedule", "Без графика"),
                    ("pct", "%"),
                ],
                height=14,
            )
            tree_det.column("date", width=100)
            tree_det.column("object", width=240)
            tree_det.column("dept", width=160)
            tree_det.column("pos", width=160)
            tree_det.column("plan", width=70, anchor="e")
            tree_det.column("off", width=70, anchor="e")
            tree_det.column("fact", width=70, anchor="e")
            tree_det.column("absent", width=90, anchor="e")
            tree_det.column("workoff", width=100, anchor="e")
            tree_det.column("noschedule", width=100, anchor="e")
            tree_det.column("pct", width=70, anchor="e")

            self._insert_rows(tree_det, [
                (
                    pd.to_datetime(r["work_date"]).strftime("%d.%m.%Y"),
                    r["object_name"],
                    r["department_name"],
                    r["position_name"],
                    int(r["plan_count"]),
                    int(r["off_count"]),
                    int(r["fact_count"]),
                    int(r["absent_count"]),
                    int(r["worked_on_off_count"]),
                    int(r["no_schedule_count"]),
                    f"{float(r['attendance_pct']):.1f}",
                )
                for _, r in df_det.iterrows()
            ])
        else:
            ttk.Label(bottom, text="Нет данных.").pack(pady=20)

    @staticmethod
    def _strip_tz(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        df = df.copy()
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                try:
                    if hasattr(df[col].dt, "tz") and df[col].dt.tz is not None:
                        df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
                except Exception:
                    pass
        return df

    def export_to_excel(self):
        if not self.data_provider:
            messagebox.showwarning("Экспорт", "Сначала загрузите данные.")
            return

        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel файл", "*.xlsx")],
            title="Сохранить отчёт",
            initialfile=f"timesheet_plan_fact_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        )
        if not path:
            return

        dp = self.data_provider
        stz = self._strip_tz

        try:
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                pd.DataFrame([dp.get_plan_fact_kpi()]).to_excel(writer, sheet_name="KPI", index=False)
                stz(dp.get_plan_fact_by_object()).to_excel(writer, sheet_name="По_объектам", index=False)
                stz(dp.get_plan_fact_by_position()).to_excel(writer, sheet_name="По_должностям", index=False)
                stz(dp.get_plan_fact_by_date()).to_excel(writer, sheet_name="По_датам", index=False)
                stz(dp.get_plan_fact_daily()).to_excel(writer, sheet_name="Детализация", index=False)

                try:
                    from openpyxl.utils import get_column_letter
                    for sheet_name in writer.sheets:
                        ws = writer.sheets[sheet_name]
                        for col_cells in ws.columns:
                            max_len = 0
                            col_letter = get_column_letter(col_cells[0].column)
                            for cell in col_cells:
                                try:
                                    cell_len = len(str(cell.value)) if cell.value is not None else 0
                                    max_len = max(max_len, cell_len)
                                except Exception:
                                    pass
                            ws.column_dimensions[col_letter].width = min(max_len + 4, 60)
                except Exception as fmt_err:
                    logging.warning(f"Не удалось настроить ширину колонок: {fmt_err}")

            messagebox.showinfo("Экспорт завершён", f"Файл успешно сохранён:\n{path}")

        except Exception as e:
            logging.exception("Ошибка экспорта в Excel")
            messagebox.showerror("Ошибка экспорта", f"Не удалось сохранить файл:\n{e}")
