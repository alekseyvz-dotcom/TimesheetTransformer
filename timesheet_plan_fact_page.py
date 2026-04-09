import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Any, Tuple

import logging
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
}


class TimesheetPlanFactData:
    def __init__(self, start_date: date, end_date: date, object_type_filter: str = ""):
        self.start_date = start_date
        self.end_date = end_date
        self.object_type_filter = object_type_filter or ""
        self._daily_cache: Optional[pd.DataFrame] = None
        self._date_cache: Optional[pd.DataFrame] = None
        self._object_cache: Optional[pd.DataFrame] = None
        self._position_cache: Optional[pd.DataFrame] = None
        self._kpi_cache: Optional[Dict[str, Any]] = None

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
            logging.exception("Timesheet Plan/Fact query error")
            messagebox.showerror("Ошибка БД", f"Ошибка получения данных:\n{e}")
            return []
        finally:
            if conn:
                db_connection_pool.putconn(conn)

    def get_object_types(self) -> List[str]:
        rows = self._execute_query("""
            SELECT DISTINCT short_name
            FROM objects
            WHERE short_name IS NOT NULL
              AND short_name <> ''
            ORDER BY short_name;
        """)
        return [r["short_name"] for r in rows]

    def _build_person_day_cte(self) -> str:
        return """
        WITH matched_employees AS (
            SELECT
                NULLIF(btrim(e.tbn), '') AS tbn_norm,
                e.position,
                e.department_id,
                ROW_NUMBER() OVER (
                    PARTITION BY NULLIF(btrim(e.tbn), '')
                    ORDER BY e.id
                ) AS rn
            FROM employees e
        ),

        base_rows AS (
            SELECT
                th.object_db_id,
                th.year,
                th.month,
                COALESCE(o.address, '—') AS object_name,
                COALESCE(
                    dep_emp.name,
                    dep_hdr.name,
                    NULLIF(btrim(th.department), ''),
                    '—'
                ) AS department_name,
                COALESCE(me.position, '—') AS position_name,
                CASE
                    WHEN NULLIF(btrim(tr.tbn), '') IS NOT NULL
                        THEN 'tbn:' || btrim(tr.tbn)
                    ELSE 'fio:' || lower(regexp_replace(btrim(tr.fio), '\\s+', ' ', 'g'))
                END AS person_key,
                tr.hours_raw
            FROM timesheet_headers th
            JOIN timesheet_rows tr
              ON tr.header_id = th.id
            LEFT JOIN objects o
              ON o.id = th.object_db_id
            LEFT JOIN departments dep_hdr
              ON dep_hdr.id = th.department_id
            LEFT JOIN matched_employees me
              ON me.tbn_norm = NULLIF(btrim(tr.tbn), '')
             AND me.rn = 1
            LEFT JOIN departments dep_emp
              ON dep_emp.id = me.department_id
            WHERE make_date(th.year, th.month, 1) <= %s
              AND (
                    date_trunc('month', make_date(th.year, th.month, 1))
                    + interval '1 month - 1 day'
                  )::date >= %s
              {object_filter}
        ),

        daily_person AS (
            SELECT
                (
                    make_date(br.year, br.month, 1)
                    + (gs.day_num - 1) * interval '1 day'
                )::date AS work_date,
                br.object_db_id,
                br.object_name,
                br.department_name,
                br.position_name,
                br.person_key,
                br.hours_raw[gs.day_num] AS raw_val
            FROM base_rows br
            CROSS JOIN LATERAL generate_series(1, 31) AS gs(day_num)
            WHERE gs.day_num <= EXTRACT(
                      DAY FROM (
                          date_trunc('month', make_date(br.year, br.month, 1))
                          + interval '1 month - 1 day'
                      )
                  )
              AND (
                    make_date(br.year, br.month, 1)
                    + (gs.day_num - 1) * interval '1 day'
                  )::date BETWEEN %s AND %s
        ),

        daily_person_flags AS (
            SELECT
                work_date,
                object_db_id,
                object_name,
                department_name,
                position_name,
                person_key,
                MAX(
                    CASE
                        WHEN raw_val IS NULL THEN 0
                        WHEN btrim(raw_val) = '' THEN 0
                        WHEN substring(
                                 replace(raw_val, ',', '.')
                                 FROM '([0-9]+(?:\\.[0-9]+)?)'
                             ) IS NOT NULL
                         AND substring(
                                 replace(raw_val, ',', '.')
                                 FROM '([0-9]+(?:\\.[0-9]+)?)'
                             )::numeric > 0
                        THEN 1
                        ELSE 0
                    END
                ) AS worked_flag
            FROM daily_person
            GROUP BY
                work_date,
                object_db_id,
                object_name,
                department_name,
                position_name,
                person_key
        )
        """

    def _build_params(self) -> Tuple[str, List[Any]]:
        params: List[Any] = [self.end_date, self.start_date]
        object_filter = ""
        if self.object_type_filter:
            object_filter = "AND o.short_name = %s"
            params.append(self.object_type_filter)
        params.extend([self.start_date, self.end_date])
        return object_filter, params

    def get_plan_fact_daily(self) -> pd.DataFrame:
        if self._daily_cache is not None:
            return self._daily_cache.copy()

        object_filter, params = self._build_params()

        query = self._build_person_day_cte() + """
        , fact_daily AS (
            SELECT
                work_date,
                object_db_id,
                object_name,
                department_name,
                position_name,
                COUNT(*)::int AS plan_count,
                COUNT(*) FILTER (WHERE worked_flag = 1)::int AS fact_count
            FROM daily_person_flags
            GROUP BY
                work_date,
                object_db_id,
                object_name,
                department_name,
                position_name
        )
        SELECT
            work_date,
            object_db_id,
            object_name,
            department_name,
            position_name,
            plan_count,
            fact_count,
            GREATEST(plan_count - fact_count, 0)::int AS absent_count,
            CASE
                WHEN plan_count > 0
                THEN ROUND(fact_count::numeric / plan_count::numeric * 100, 1)
                ELSE 0
            END AS attendance_pct
        FROM fact_daily
        ORDER BY
            work_date,
            object_name,
            department_name,
            position_name;
        """

        rows = self._execute_query(query.format(object_filter=object_filter), tuple(params))
        df = pd.DataFrame(rows)
        if not df.empty:
            df["work_date"] = pd.to_datetime(df["work_date"])
            for col in ("plan_count", "fact_count", "absent_count"):
                df[col] = df[col].fillna(0).astype(int)
            df["attendance_pct"] = df["attendance_pct"].fillna(0).astype(float)

        self._daily_cache = df.copy()
        return df

    def get_plan_fact_by_date(self) -> pd.DataFrame:
        if self._date_cache is not None:
            return self._date_cache.copy()

        df = self.get_plan_fact_daily()
        if df.empty:
            return pd.DataFrame(columns=[
                "work_date", "plan_count", "fact_count", "absent_count", "attendance_pct"
            ])

        grp = (
            df.groupby("work_date", as_index=False)
              .agg({
                  "plan_count": "sum",
                  "fact_count": "sum",
                  "absent_count": "sum",
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

    def _get_actual_last_date(self, df: pd.DataFrame) -> Optional[pd.Timestamp]:
        if df is None or df.empty:
            return None

        df = df.copy()
        df["work_date"] = pd.to_datetime(df["work_date"])
        today = pd.Timestamp(datetime.today().date())

        df_actual = df[(df["work_date"] <= today) & (df["fact_count"] > 0)]
        if not df_actual.empty:
            return df_actual["work_date"].max()

        df_past = df[df["work_date"] <= today]
        if not df_past.empty:
            return df_past["work_date"].max()

        return df["work_date"].min()

    def get_plan_fact_kpi(self) -> Dict[str, Any]:
        if self._kpi_cache is not None:
            return dict(self._kpi_cache)

        df_date = self.get_plan_fact_by_date()
        df_det = self.get_plan_fact_daily()

        if df_date.empty:
            return {
                "plan_total": 0,
                "fact_total": 0,
                "absent_total": 0,
                "attendance_pct": 0.0,
                "days_count": 0,
                "objects_count": 0,
                "as_of_date": None,
            }

        actual_date = self._get_actual_last_date(df_date)
        if actual_date is None:
            return {
                "plan_total": 0,
                "fact_total": 0,
                "absent_total": 0,
                "attendance_pct": 0.0,
                "days_count": 0,
                "objects_count": 0,
                "as_of_date": None,
            }

        last_row = df_date[pd.to_datetime(df_date["work_date"]) == actual_date].iloc[-1]

        result = {
            "plan_total": int(last_row["plan_count"]),
            "fact_total": int(last_row["fact_count"]),
            "absent_total": int(last_row["absent_count"]),
            "attendance_pct": float(last_row["attendance_pct"]),
            "days_count": int(df_date["work_date"].nunique()),
            "objects_count": int(df_det["object_name"].nunique()) if not df_det.empty else 0,
            "as_of_date": pd.to_datetime(last_row["work_date"]),
        }
        self._kpi_cache = dict(result)
        return result

    def get_plan_fact_by_object(self) -> pd.DataFrame:
        if self._object_cache is not None:
            return self._object_cache.copy()

        df_daily = self.get_plan_fact_daily()
        actual_date = self._get_actual_last_date(df_daily)
        if actual_date is None:
            return pd.DataFrame(columns=[
                "object_name", "plan_count", "fact_count", "absent_count", "attendance_pct"
            ])

        object_filter, params = self._build_params()
        params.extend([actual_date.date()])

        query = self._build_person_day_cte() + """
        SELECT
            object_name,
            COUNT(DISTINCT person_key)::int AS plan_count,
            COUNT(DISTINCT person_key) FILTER (WHERE worked_flag = 1)::int AS fact_count,
            GREATEST(
                COUNT(DISTINCT person_key)
                - COUNT(DISTINCT person_key) FILTER (WHERE worked_flag = 1),
                0
            )::int AS absent_count,
            CASE
                WHEN COUNT(DISTINCT person_key) > 0
                THEN ROUND(
                    COUNT(DISTINCT person_key) FILTER (WHERE worked_flag = 1)::numeric
                    / COUNT(DISTINCT person_key)::numeric * 100, 1
                )
                ELSE 0
            END AS attendance_pct
        FROM daily_person_flags
        WHERE work_date = %s
        GROUP BY object_name
        ORDER BY plan_count DESC, fact_count DESC, object_name;
        """

        rows = self._execute_query(query.format(object_filter=object_filter), tuple(params))
        df = pd.DataFrame(rows)
        if not df.empty:
            for col in ("plan_count", "fact_count", "absent_count"):
                df[col] = df[col].fillna(0).astype(int)
            df["attendance_pct"] = df["attendance_pct"].fillna(0).astype(float)

        self._object_cache = df.copy()
        return df

    def get_plan_fact_by_position(self) -> pd.DataFrame:
        if self._position_cache is not None:
            return self._position_cache.copy()

        df_daily = self.get_plan_fact_daily()
        actual_date = self._get_actual_last_date(df_daily)
        if actual_date is None:
            return pd.DataFrame(columns=[
                "department_name",
                "position_name",
                "plan_count",
                "fact_count",
                "absent_count",
                "attendance_pct",
            ])

        object_filter, params = self._build_params()
        params.extend([actual_date.date()])

        query = self._build_person_day_cte() + """
        SELECT
            department_name,
            position_name,
            COUNT(DISTINCT person_key)::int AS plan_count,
            COUNT(DISTINCT person_key) FILTER (WHERE worked_flag = 1)::int AS fact_count,
            GREATEST(
                COUNT(DISTINCT person_key)
                - COUNT(DISTINCT person_key) FILTER (WHERE worked_flag = 1),
                0
            )::int AS absent_count,
            CASE
                WHEN COUNT(DISTINCT person_key) > 0
                THEN ROUND(
                    COUNT(DISTINCT person_key) FILTER (WHERE worked_flag = 1)::numeric
                    / COUNT(DISTINCT person_key)::numeric * 100, 1
                )
                ELSE 0
            END AS attendance_pct
        FROM daily_person_flags
        WHERE work_date = %s
        GROUP BY department_name, position_name
        ORDER BY plan_count DESC, fact_count DESC, department_name, position_name;
        """

        rows = self._execute_query(query.format(object_filter=object_filter), tuple(params))
        df = pd.DataFrame(rows)
        if not df.empty:
            for col in ("plan_count", "fact_count", "absent_count"):
                df[col] = df[col].fillna(0).astype(int)
            df["attendance_pct"] = df["attendance_pct"].fillna(0).astype(float)

        self._position_cache = df.copy()
        return df

class TimesheetPlanFactPage(ttk.Frame):
    def __init__(self, master, app_ref=None):
        super().__init__(master)
        self.app_ref = app_ref
        self.data_provider: Optional[TimesheetPlanFactData] = None

        self._build_header()
        self._build_body()
        self.load_filters()
        self.refresh_data()

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

        ttk.Button(hdr, text="⟳ Обновить", command=self.refresh_data).pack(side="left", padx=10)
        ttk.Button(hdr, text="📥 Экспорт в Excel", command=self.export_to_excel).pack(side="left", padx=4)

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

    def _clear_inner(self):
        for w in self.inner.winfo_children():
            w.destroy()

    def load_filters(self):
        try:
            types = TimesheetPlanFactData(datetime.now().date(), datetime.now().date(), "").get_object_types()
            self.object_type_combo["values"] = ["Все типы"] + types
        except Exception as e:
            logging.error(f"Не удалось загрузить типы объектов: {e}")
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
        start_date, end_date = self.get_dates_from_period()
        obj_filter = self.object_type_var.get()
        if obj_filter == "Все типы":
            obj_filter = ""

        self.data_provider = TimesheetPlanFactData(start_date, end_date, obj_filter)
        self._render()

        self.last_update_var.set(
            f"Обновлено: {datetime.now().strftime('%H:%M:%S')}  |  "
            f"{start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')}"
        )

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
            ("План (в табеле)", f"{kpi.get('plan_total', 0):,}".replace(",", " "), "чел.", PALETTE["primary"]),
            ("Факт (с часами)", f"{kpi.get('fact_total', 0):,}".replace(",", " "), "чел.", PALETTE["success"]),
            ("Не вышли", f"{kpi.get('absent_total', 0):,}".replace(",", " "), "чел.", PALETTE["negative"]),
            ("Явка", f"{kpi.get('attendance_pct', 0):.1f}", "%", PALETTE["accent"]),
            ("Дней в выборке", str(int(kpi.get("days_count", 0))), "дн.", PALETTE["neutral"]),
            ("Объектов", str(int(kpi.get("objects_count", 0))), "шт.", PALETTE["neutral"]),
        ]

        for i, (title, value, unit, color) in enumerate(cards):
            card = self._create_card(kpi_frame, title, value, unit, color)
            card.grid(row=0, column=i, padx=5, pady=4, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        top = ttk.Frame(self.inner)
        top.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        left = ttk.LabelFrame(top, text="План / факт по объектам")
        left.pack(side="left", fill="both", expand=True, padx=(0, 5))

        right = ttk.LabelFrame(top, text="План / факт по должностям и подразделениям")
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
                    ("fact", "Факт"),
                    ("absent", "Не вышли"),
                    ("pct", "% явки"),
                ],
                height=12,
            )
            tree_obj.column("object", width=280)
            tree_obj.column("plan", width=80, anchor="e")
            tree_obj.column("fact", width=80, anchor="e")
            tree_obj.column("absent", width=90, anchor="e")
            tree_obj.column("pct", width=80, anchor="e")

            self._insert_rows(tree_obj, [
                (
                    r["object_name"],
                    int(r["plan_count"]),
                    int(r["fact_count"]),
                    int(r["absent_count"]),
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
                    ("fact", "Факт"),
                    ("absent", "Не вышли"),
                    ("pct", "% явки"),
                ],
                height=12,
            )
            tree_pos.column("department", width=180)
            tree_pos.column("position", width=180)
            tree_pos.column("plan", width=80, anchor="e")
            tree_pos.column("fact", width=80, anchor="e")
            tree_pos.column("absent", width=90, anchor="e")
            tree_pos.column("pct", width=80, anchor="e")

            self._insert_rows(tree_pos, [
                (
                    r["department_name"],
                    r["position_name"],
                    int(r["plan_count"]),
                    int(r["fact_count"]),
                    int(r["absent_count"]),
                    f"{float(r['attendance_pct']):.1f}",
                )
                for _, r in df_pos.iterrows()
            ])
        else:
            ttk.Label(right, text="Нет данных.").pack(pady=20)

        mid = ttk.LabelFrame(self.inner, text="Итоги по датам")
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
                    ("fact", "Факт"),
                    ("absent", "Не вышли"),
                    ("pct", "% явки"),
                ],
                height=8,
            )
            tree_date.column("date", width=100)
            tree_date.column("plan", width=90, anchor="e")
            tree_date.column("fact", width=90, anchor="e")
            tree_date.column("absent", width=100, anchor="e")
            tree_date.column("pct", width=90, anchor="e")

            self._insert_rows(tree_date, [
                (
                    pd.to_datetime(r["work_date"]).strftime("%d.%m.%Y"),
                    int(r["plan_count"]),
                    int(r["fact_count"]),
                    int(r["absent_count"]),
                    f"{float(r['attendance_pct']):.1f}",
                )
                for _, r in df_date.iterrows()
            ])
        else:
            ttk.Label(mid, text="Нет данных.").pack(pady=20)

        bottom = ttk.LabelFrame(self.inner, text="Детализация по дням / объектам / должностям")
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
                    ("fact", "Факт"),
                    ("absent", "Не вышли"),
                    ("pct", "%"),
                ],
                height=14,
            )
            tree_det.column("date", width=100)
            tree_det.column("object", width=260)
            tree_det.column("dept", width=160)
            tree_det.column("pos", width=160)
            tree_det.column("plan", width=70, anchor="e")
            tree_det.column("fact", width=70, anchor="e")
            tree_det.column("absent", width=90, anchor="e")
            tree_det.column("pct", width=70, anchor="e")

            self._insert_rows(tree_det, [
                (
                    pd.to_datetime(r["work_date"]).strftime("%d.%m.%Y"),
                    r["object_name"],
                    r["department_name"],
                    r["position_name"],
                    int(r["plan_count"]),
                    int(r["fact_count"]),
                    int(r["absent_count"]),
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
                if hasattr(df[col].dt, "tz") and df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
            elif df[col].dtype == object:
                try:
                    sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    if sample is not None and hasattr(sample, "tzinfo") and sample.tzinfo is not None:
                        df[col] = pd.to_datetime(df[col], utc=True).dt.tz_localize(None)
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
                kpi_df = pd.DataFrame([dp.get_plan_fact_kpi()])
                obj_df = dp.get_plan_fact_by_object()
                pos_df = dp.get_plan_fact_by_position()
                date_df = dp.get_plan_fact_by_date()
                det_df = dp.get_plan_fact_daily()

                stz(kpi_df).to_excel(writer, sheet_name="KPI", index=False)
                stz(obj_df).to_excel(writer, sheet_name="По_объектам", index=False)
                stz(pos_df).to_excel(writer, sheet_name="По_должностям", index=False)
                stz(date_df).to_excel(writer, sheet_name="По_датам", index=False)
                stz(det_df).to_excel(writer, sheet_name="Детализация", index=False)

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
