import calendar
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


def _norm_text(v: Any) -> str:
    return str(v or "").strip()


def _norm_fio(v: Any) -> str:
    return " ".join(_norm_text(v).lower().split())


def _person_key(fio: Any, tbn: Any) -> str:
    tbn_val = _norm_text(tbn)
    if tbn_val:
        return f"tbn:{tbn_val}"
    return f"fio:{_norm_fio(fio)}"


def _extract_hours(raw: Any) -> float:
    if raw is None:
        return 0.0

    s = str(raw).strip().replace(",", ".")
    if not s:
        return 0.0

    import re
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return 0.0

    try:
        return float(m.group(1))
    except Exception:
        return 0.0


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
                cur.execute("SET LOCAL statement_timeout = 120000")
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

    def _load_timesheet_rows(self) -> List[Dict[str, Any]]:
        params: List[Any] = [self.end_date, self.start_date]
        object_filter_sql = ""

        if self.object_type_filter:
            object_filter_sql = " AND COALESCE(o.short_name, '') = %s "
            params.append(self.object_type_filter)

        sql = f"""
            SELECT
                th.id AS header_id,
                th.object_id,
                th.object_db_id,
                COALESCE(o.address, th.object_addr, '—') AS object_name,
                COALESCE(o.short_name, '') AS object_type,
                th.year,
                th.month,
                COALESCE(dep_hdr.name, th.department, '—') AS header_department,

                tr.id AS row_id,
                tr.fio,
                tr.tbn,
                tr.hours_raw,
                tr.total_days,
                tr.total_hours,
                tr.night_hours,
                tr.overtime_day,
                tr.overtime_night
            FROM timesheet_headers th
            JOIN timesheet_rows tr
              ON tr.header_id = th.id
            LEFT JOIN objects o
              ON o.id = th.object_db_id
            LEFT JOIN departments dep_hdr
              ON dep_hdr.id = th.department_id
            WHERE make_date(th.year, th.month, 1) <= %s
              AND (
                    date_trunc('month', make_date(th.year, th.month, 1))
                    + interval '1 month - 1 day'
                  )::date >= %s
              {object_filter_sql}
            ORDER BY
                th.year,
                th.month,
                object_name,
                tr.fio
        """
        return self._execute_query(sql, tuple(params))

    def _load_employees(self) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        rows = self._execute_query(
            """
            SELECT
                e.id,
                e.fio,
                e.tbn,
                e.position,
                e.department_id,
                e.work_schedule,
                COALESCE(d.name, '—') AS department_name
            FROM employees e
            LEFT JOIN departments d
              ON d.id = e.department_id
            """
        )

        by_tbn: Dict[str, Dict[str, Any]] = {}
        by_fio: Dict[str, Dict[str, Any]] = {}

        for r in rows:
            item = {
                "employee_id": r.get("id"),
                "fio": r.get("fio") or "",
                "tbn": r.get("tbn") or "",
                "position_name": r.get("position") or "—",
                "department_name": r.get("department_name") or "—",
                "work_schedule_name": _norm_text(r.get("work_schedule")),
            }

            tbn_norm = _norm_text(r.get("tbn"))
            fio_norm = _norm_fio(r.get("fio"))

            if tbn_norm and tbn_norm not in by_tbn:
                by_tbn[tbn_norm] = item

            if fio_norm and fio_norm not in by_fio:
                by_fio[fio_norm] = item

        return by_tbn, by_fio

    def _load_schedule_map(self) -> Dict[Tuple[str, date], Dict[str, Any]]:
        rows = self._execute_query(
            """
            SELECT
                LOWER(BTRIM(ws.schedule_name)) AS schedule_name_norm,
                wsd.work_date,
                wsd.is_workday,
                wsd.planned_hours,
                wsd.raw_value
            FROM work_schedules ws
            JOIN work_schedule_days wsd
              ON wsd.schedule_id = ws.id
            WHERE wsd.work_date BETWEEN %s AND %s
            """,
            (self.start_date, self.end_date)
        )

        schedule_map: Dict[Tuple[str, date], Dict[str, Any]] = {}
        for r in rows:
            schedule_map[(r["schedule_name_norm"], r["work_date"])] = {
                "is_workday": bool(r["is_workday"]),
                "planned_hours": float(r["planned_hours"] or 0),
                "raw_value": r.get("raw_value"),
            }
        return schedule_map

    def _match_employee(
        self,
        fio: Any,
        tbn: Any,
        employees_by_tbn: Dict[str, Dict[str, Any]],
        employees_by_fio: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        tbn_norm = _norm_text(tbn)
        if tbn_norm and tbn_norm in employees_by_tbn:
            return employees_by_tbn[tbn_norm]

        fio_norm = _norm_fio(fio)
        if fio_norm and fio_norm in employees_by_fio:
            return employees_by_fio[fio_norm]

        return None

    def _build_detail(self) -> pd.DataFrame:
        timesheet_rows = self._load_timesheet_rows()
        employees_by_tbn, employees_by_fio = self._load_employees()
        schedule_map = self._load_schedule_map()

        result_rows: List[Dict[str, Any]] = []

        for row in timesheet_rows:
            year = int(row["year"])
            month = int(row["month"])
            days_in_month = calendar.monthrange(year, month)[1]
            hours_raw = row.get("hours_raw") or []

            emp = self._match_employee(
                row.get("fio"),
                row.get("tbn"),
                employees_by_tbn,
                employees_by_fio,
            )

            if emp:
                employee_id = emp.get("employee_id")
                department_name = emp.get("department_name") or row.get("header_department") or "—"
                position_name = emp.get("position_name") or "—"
                work_schedule_name = emp.get("work_schedule_name") or ""
            else:
                employee_id = None
                department_name = row.get("header_department") or "—"
                position_name = "—"
                work_schedule_name = ""

            schedule_name_norm = work_schedule_name.lower()

            for day_num in range(1, days_in_month + 1):
                work_dt = date(year, month, day_num)
                if not (self.start_date <= work_dt <= self.end_date):
                    continue

                raw_val = None
                if isinstance(hours_raw, list) and len(hours_raw) >= day_num:
                    raw_val = hours_raw[day_num - 1]

                hours = _extract_hours(raw_val)
                worked_flag = 1 if hours > 0 else 0

                schedule_info = None
                if schedule_name_norm:
                    schedule_info = schedule_map.get((schedule_name_norm, work_dt))

                no_schedule_flag = 0
                plan_flag = 0
                off_flag = 0
                fact_flag = 0
                absent_flag = 0
                worked_on_off_flag = 0
                is_workday = None
                planned_hours = 0.0

                if schedule_info is None:
                    no_schedule_flag = 1
                else:
                    is_workday = bool(schedule_info["is_workday"])
                    planned_hours = float(schedule_info.get("planned_hours") or 0)

                    if is_workday:
                        plan_flag = 1
                        if worked_flag:
                            fact_flag = 1
                        else:
                            absent_flag = 1
                    else:
                        off_flag = 1
                        if worked_flag:
                            worked_on_off_flag = 1

                result_rows.append(
                    {
                        "work_date": pd.Timestamp(work_dt),
                        "object_id": row.get("object_id") or row.get("object_db_id"),
                        "object_db_id": row.get("object_db_id"),
                        "object_name": row.get("object_name") or "—",
                        "object_type": row.get("object_type") or "",
                        "department_name": department_name,
                        "position_name": position_name,
                        "fio": row.get("fio") or "",
                        "tbn": row.get("tbn") or "",
                        "person_key": _person_key(row.get("fio"), row.get("tbn")),
                        "employee_id": employee_id,
                        "work_schedule_name": work_schedule_name,
                        "day_raw": raw_val,
                        "hours": hours,
                        "worked_flag": worked_flag,
                        "is_workday": is_workday,
                        "planned_hours": planned_hours,
                        "plan_flag": plan_flag,
                        "off_flag": off_flag,
                        "fact_flag": fact_flag,
                        "absent_flag": absent_flag,
                        "worked_on_off_flag": worked_on_off_flag,
                        "no_schedule_flag": no_schedule_flag,
                    }
                )

        df = pd.DataFrame(result_rows)

        if df.empty:
            return pd.DataFrame(columns=[
                "work_date", "object_id", "object_db_id", "object_name", "object_type",
                "department_name", "position_name", "fio", "tbn", "person_key",
                "employee_id", "work_schedule_name", "day_raw", "hours",
                "worked_flag", "is_workday", "planned_hours", "plan_flag",
                "off_flag", "fact_flag", "absent_flag", "worked_on_off_flag",
                "no_schedule_flag",
            ])

        for col in [
            "worked_flag", "plan_flag", "off_flag", "fact_flag",
            "absent_flag", "worked_on_off_flag", "no_schedule_flag",
        ]:
            df[col] = df[col].fillna(0).astype(int)

        df["hours"] = pd.to_numeric(df["hours"], errors="coerce").fillna(0.0)
        df["planned_hours"] = pd.to_numeric(df["planned_hours"], errors="coerce").fillna(0.0)

        return df

    def get_daily_detail(self) -> pd.DataFrame:
        if self._detail_cache is None:
            self._detail_cache = self._build_detail()
        return self._detail_cache.copy()

    def get_plan_fact_daily(self) -> pd.DataFrame:
        if self._daily_cache is not None:
            return self._daily_cache.copy()

        df = self.get_daily_detail()

        if df.empty:
            self._daily_cache = pd.DataFrame(columns=[
                "work_date", "object_id", "object_name", "department_name", "position_name",
                "plan_count", "off_count", "fact_count", "absent_count",
                "worked_on_off_count", "no_schedule_count", "attendance_pct"
            ])
            return self._daily_cache.copy()

        daily_df = (
            df.groupby(
                ["work_date", "object_id", "object_name", "department_name", "position_name"],
                as_index=False
            )
            .agg(
                plan_count=("plan_flag", "sum"),
                off_count=("off_flag", "sum"),
                fact_count=("fact_flag", "sum"),
                absent_count=("absent_flag", "sum"),
                worked_on_off_count=("worked_on_off_flag", "sum"),
                no_schedule_count=("no_schedule_flag", "sum"),
            )
            .sort_values(["work_date", "object_name", "department_name", "position_name"])
        )

        daily_df["attendance_pct"] = daily_df.apply(
            lambda r: round(r["fact_count"] / r["plan_count"] * 100.0, 1)
            if r["plan_count"] > 0 else 0.0,
            axis=1
        )

        self._daily_cache = daily_df.copy()
        return self._daily_cache.copy()

    def get_plan_fact_by_date(self) -> pd.DataFrame:
        if self._date_cache is not None:
            return self._date_cache.copy()

        daily_df = self.get_plan_fact_daily()

        if daily_df.empty:
            self._date_cache = pd.DataFrame(columns=[
                "work_date", "plan_count", "off_count", "fact_count", "absent_count",
                "worked_on_off_count", "no_schedule_count", "attendance_pct"
            ])
            return self._date_cache.copy()

        by_date_df = (
            daily_df.groupby("work_date", as_index=False)
            .agg(
                plan_count=("plan_count", "sum"),
                off_count=("off_count", "sum"),
                fact_count=("fact_count", "sum"),
                absent_count=("absent_count", "sum"),
                worked_on_off_count=("worked_on_off_count", "sum"),
                no_schedule_count=("no_schedule_count", "sum"),
            )
            .sort_values("work_date")
        )

        by_date_df["attendance_pct"] = by_date_df.apply(
            lambda r: round(r["fact_count"] / r["plan_count"] * 100.0, 1)
            if r["plan_count"] > 0 else 0.0,
            axis=1
        )

        self._date_cache = by_date_df.copy()
        return self._date_cache.copy()

    def get_plan_fact_by_object(self) -> pd.DataFrame:
        if self._object_cache is not None:
            return self._object_cache.copy()

        daily_df = self.get_plan_fact_daily()

        if daily_df.empty:
            self._object_cache = pd.DataFrame(columns=[
                "object_name", "plan_count", "off_count", "fact_count",
                "absent_count", "worked_on_off_count", "no_schedule_count", "attendance_pct"
            ])
            return self._object_cache.copy()

        by_object_df = (
            daily_df.groupby("object_name", as_index=False)
            .agg(
                plan_count=("plan_count", "sum"),
                off_count=("off_count", "sum"),
                fact_count=("fact_count", "sum"),
                absent_count=("absent_count", "sum"),
                worked_on_off_count=("worked_on_off_count", "sum"),
                no_schedule_count=("no_schedule_count", "sum"),
            )
            .sort_values(["plan_count", "fact_count", "object_name"], ascending=[False, False, True])
        )

        by_object_df["attendance_pct"] = by_object_df.apply(
            lambda r: round(r["fact_count"] / r["plan_count"] * 100.0, 1)
            if r["plan_count"] > 0 else 0.0,
            axis=1
        )

        self._object_cache = by_object_df.copy()
        return self._object_cache.copy()

    def get_plan_fact_by_position(self) -> pd.DataFrame:
        if self._position_cache is not None:
            return self._position_cache.copy()

        daily_df = self.get_plan_fact_daily()

        if daily_df.empty:
            self._position_cache = pd.DataFrame(columns=[
                "department_name", "position_name", "plan_count", "off_count",
                "fact_count", "absent_count", "worked_on_off_count",
                "no_schedule_count", "attendance_pct"
            ])
            return self._position_cache.copy()

        by_position_df = (
            daily_df.groupby(["department_name", "position_name"], as_index=False)
            .agg(
                plan_count=("plan_count", "sum"),
                off_count=("off_count", "sum"),
                fact_count=("fact_count", "sum"),
                absent_count=("absent_count", "sum"),
                worked_on_off_count=("worked_on_off_count", "sum"),
                no_schedule_count=("no_schedule_count", "sum"),
            )
            .sort_values(
                ["plan_count", "fact_count", "department_name", "position_name"],
                ascending=[False, False, True, True]
            )
        )

        by_position_df["attendance_pct"] = by_position_df.apply(
            lambda r: round(r["fact_count"] / r["plan_count"] * 100.0, 1)
            if r["plan_count"] > 0 else 0.0,
            axis=1
        )

        self._position_cache = by_position_df.copy()
        return self._position_cache.copy()

    def get_plan_fact_kpi(self) -> Dict[str, Any]:
        if self._kpi_cache is not None:
            return dict(self._kpi_cache)

        daily_df = self.get_plan_fact_daily()
        detail_df = self.get_daily_detail()

        if daily_df.empty:
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

        plan_total = int(daily_df["plan_count"].sum())
        off_total = int(daily_df["off_count"].sum())
        fact_total = int(daily_df["fact_count"].sum())
        absent_total = int(daily_df["absent_count"].sum())
        worked_on_off_total = int(daily_df["worked_on_off_count"].sum())
        no_schedule_total = int(daily_df["no_schedule_count"].sum())

        self._kpi_cache = {
            "plan_total": plan_total,
            "off_total": off_total,
            "fact_total": fact_total,
            "absent_total": absent_total,
            "worked_on_off_total": worked_on_off_total,
            "no_schedule_total": no_schedule_total,
            "attendance_pct": round(fact_total / plan_total * 100.0, 1) if plan_total > 0 else 0.0,
            "days_count": int(daily_df["work_date"].nunique()),
            "objects_count": int(detail_df["object_name"].nunique()) if not detail_df.empty else 0,
            "employees_count": int(detail_df["person_key"].nunique()) if not detail_df.empty else 0,
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

        combo_state = "disabled" if loading else "readonly"
        btn_state = "disabled" if loading else "normal"

        self.period_cb.configure(state=combo_state)
        self.object_type_combo.configure(state=combo_state)
        self.btn_refresh.configure(state=btn_state)
        self.btn_export.configure(state=("normal" if (not loading and self.data_provider) else "disabled"))

        if status_text:
            self.last_update_var.set(status_text)

    def _clear_inner(self):
        for w in self.inner.winfo_children():
            w.destroy()

    def _show_loading_placeholder(self, text: str = "Загрузка данных..."):
        self._clear_inner()
        wrap = ttk.Frame(self.inner)
        wrap.pack(fill="both", expand=True, padx=20, pady=30)
        ttk.Label(wrap, text=text, font=("Segoe UI", 11)).pack(pady=20)

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
                logging.exception("Ошибка загрузки аналитики план/факт")
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
