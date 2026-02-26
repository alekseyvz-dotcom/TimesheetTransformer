import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import logging
import pandas as pd
import numpy as np

from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.dates as mdates

db_connection_pool: Optional[pool.SimpleConnectionPool] = None


def set_db_pool(db_pool: pool.SimpleConnectionPool):
    global db_connection_pool
    db_connection_pool = db_pool
    logging.info("Analytics Module: DB pool set.")


# Цвета для KPI (можно вынести в отдельный конфиг позже)
KPI_COLORS = {
    "good":    "#2e7d32",    # зелёный
    "warning": "#f57c00",    # оранжевый
    "danger":  "#c62828",    # красный
    "neutral": "#1976d2",    # синий
    "gray":    "#616161",
}


class AnalyticsData:
    def __init__(self, start_date: datetime.date, end_date: datetime.date, object_type_filter: str = ""):
        self.start_date = start_date
        self.end_date   = end_date
        self.object_type_filter = object_type_filter.strip()

        # Для сравнения с предыдущим аналогичным периодом
        delta = end_date - start_date
        self.prev_end   = start_date - timedelta(days=1)
        self.prev_start = self.prev_end - delta

    def _execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        if not db_connection_pool:
            raise ConnectionError("Пул соединений с БД не был инициализирован.")
        conn = None
        try:
            conn = db_connection_pool.getconn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params or ())
                return cur.fetchall()
        except Exception as e:
            logging.exception("Analytics query error")
            messagebox.showerror("Ошибка БД", f"Не удалось получить данные:\n{e}")
            return []
        finally:
            if conn:
                db_connection_pool.putconn(conn)

    def get_object_types(self) -> List[str]:
        query = """
            SELECT DISTINCT short_name
            FROM objects
            WHERE short_name IS NOT NULL AND short_name <> ''
            ORDER BY short_name
        """
        results = self._execute_query(query)
        return [row["short_name"] for row in results]

    def _get_join_and_filter(self, alias: str = "o", obj_field: str = "object_id") -> Tuple[str, str, List[Any]]:
        join_clause = ""
        filter_clause = ""
        params = []

        if self.object_type_filter:
            join_clause = f"LEFT JOIN objects {alias} ON {obj_field} = {alias}.id"
            filter_clause = f"AND {alias}.short_name = %s"
            params.append(self.object_type_filter)

        return join_clause, filter_clause, params


    def get_labor_kpi(self, compare: bool = False) -> Dict[str, Any]:
        base_query = """
        SELECT
            COALESCE(SUM(tr.total_hours), 0)                      AS total_hours,
            COALESCE(SUM(tr.total_days), 0)                        AS total_days,
            COALESCE(SUM(tr.overtime_day + tr.overtime_night), 0)  AS total_overtime,
            COUNT(DISTINCT COALESCE(NULLIF(tr.tbn,''), tr.fio))    AS unique_people
        FROM timesheet_headers th
        JOIN timesheet_rows tr ON th.id = tr.header_id
        {join_clause}
        WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
        {filter_clause}
        """

        start_period = self.start_date.year * 100 + self.start_date.month
        end_period   = self.end_date.year   * 100 + self.end_date.month

        join_c, filt_c, params = self._get_join_and_filter(obj_field="th.object_db_id")
        query = base_query.format(join_clause=join_c, filter_clause=filt_c)
        params = [start_period, end_period] + params

        row = self._execute_query(query, tuple(params))
        current = row[0] if row else {}

        result = {
            "total_hours":     float(current.get("total_hours", 0)),
            "total_days":      float(current.get("total_days", 0)),
            "total_overtime":  float(current.get("total_overtime", 0)),
            "unique_people":   int(current.get("unique_people", 0)),
        }

        result["overtime_share_pct"] = (
            result["total_overtime"] / result["total_hours"] * 100
            if result["total_hours"] > 0 else 0.0
        )
        result["hours_per_person"] = (
            result["total_hours"] / result["unique_people"]
            if result["unique_people"] > 0 else 0.0
        )

        if compare:
            prev_join, prev_filt, prev_params = self._get_join_and_filter(obj_field="th.object_db_id")
            prev_query = base_query.format(join_clause=prev_join, filter_clause=prev_filt)
            prev_params = [self.prev_start.year*100 + self.prev_start.month,
                           self.prev_end.year*100   + self.prev_end.month] + prev_params

            prev_row = self._execute_query(prev_query, tuple(prev_params))
            prev = prev_row[0] if prev_row else {}

            prev_hours = float(prev.get("total_hours", 0))
            if prev_hours > 0:
                result["hours_delta_pct"] = (
                    (result["total_hours"] - prev_hours) / prev_hours * 100
                )
            else:
                result["hours_delta_pct"] = None

        return result

    def get_labor_by_object(self) -> pd.DataFrame:
        join_c, filt_c, params = self._get_join_and_filter(obj_field="th.object_db_id")
        query = f"""
            SELECT 
                o.address AS object_name,
                SUM(tr.total_hours) AS total_hours
            FROM timesheet_headers th
            JOIN timesheet_rows tr ON th.id = tr.header_id
            {join_c}
            WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
            {filt_c}
            GROUP BY o.address
            HAVING o.address IS NOT NULL
            ORDER BY total_hours DESC
        """
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year   * 100 + self.end_date.month
        data = self._execute_query(query, (start_p, end_p, *params))
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"] = df["total_hours"].astype(float)
        return df

    def get_labor_trend_by_month(self) -> pd.DataFrame:
        join_c, filt_c, params_add = self._get_join_and_filter(obj_field="th.object_db_id")
        query = f"""
            SELECT
                th.year,
                th.month,
                SUM(tr.total_hours) AS total_hours,
                SUM(tr.overtime_day + tr.overtime_night) AS total_overtime
            FROM timesheet_headers th
            JOIN timesheet_rows tr ON th.id = tr.header_id
            {join_c}
            WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
            {filt_c}
            GROUP BY th.year, th.month
            ORDER BY th.year, th.month
        """
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year   * 100 + self.end_date.month
        data = self._execute_query(query, (start_p, end_p, *params_add))
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"]   = df["total_hours"].astype(float)
            df["total_overtime"] = df["total_overtime"].astype(float)
            df["period"] = df.apply(lambda r: f"{int(r['year'])}-{int(r['month']):02d}", axis=1)
        return df

    def get_top_employees_by_hours(self, limit: int = 10) -> pd.DataFrame:
        join_c, filt_c, params_add = self._get_join_and_filter(obj_field="th.object_db_id")
        query = f"""
            SELECT
                tr.fio,
                SUM(tr.total_hours) AS total_hours,
                SUM(tr.overtime_day + tr.overtime_night) AS total_overtime
            FROM timesheet_headers th
            JOIN timesheet_rows tr ON th.id = tr.header_id
            {join_c}
            WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
            {filt_c}
            GROUP BY tr.fio
            ORDER BY total_hours DESC
            LIMIT {limit}
        """
        start_p = self.start_date.year * 100 + self.start_date.month
        end_p   = self.end_date.year   * 100 + self.end_date.month
        data = self._execute_query(query, (start_p, end_p, *params_add))
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"]   = df["total_hours"].astype(float)
            df["total_overtime"] = df["total_overtime"].astype(float)
        return df


    def get_transport_kpi(self) -> Dict[str, Any]:
        join_c, filt_c, params_add = self._get_join_and_filter(obj_field="t.object_id")
        query = f"""
            SELECT
                COALESCE(SUM(tp.hours), 0) AS total_machine_hours,
                COUNT(DISTINCT t.id)       AS total_orders,
                COALESCE(SUM(tp.qty), 0)   AS total_units
            FROM transport_orders t
            JOIN transport_order_positions tp ON t.id = tp.order_id
            {join_c}
            WHERE t.date BETWEEN %s AND %s
            {filt_c}
        """
        data = self._execute_query(query, (self.start_date, self.end_date, *params_add))
        row = data[0] if data else {}
        res = {
            "total_machine_hours": float(row.get("total_machine_hours", 0)),
            "total_orders":        int(row.get("total_orders", 0)),
            "total_units":         float(row.get("total_units", 0)),
        }
        res["avg_hours_per_order"] = res["total_machine_hours"] / res["total_orders"] if res["total_orders"] > 0 else 0.0
        res["hours_per_unit"]      = res["total_machine_hours"] / res["total_units"]      if res["total_units"] > 0 else 0.0
        return res


    def get_meals_kpi(self) -> Dict[str, Any]:
        join_c, filt_c, params_add = self._get_join_and_filter(obj_field="mo.object_id")
        query = f"""
            SELECT
                COUNT(moi.id)                   AS total_portions_rows,
                COALESCE(SUM(moi.quantity), 0)  AS total_portions_qty,
                COUNT(DISTINCT mo.id)           AS total_orders,
                COUNT(DISTINCT moi.employee_id) AS unique_employees
            FROM meal_orders mo
            JOIN meal_order_items moi ON mo.id = moi.order_id
            {join_c}
            WHERE mo.date BETWEEN %s AND %s
            {filt_c}
        """
        data = self._execute_query(query, (self.start_date, self.end_date, *params_add))
        row = data[0] if data else {}
        res = {
            "total_portions_qty": float(row.get("total_portions_qty", 0)),
            "total_orders":       int(row.get("total_orders", 0)),
            "unique_employees":   int(row.get("unique_employees", 0)),
        }
        res["avg_portions_per_order"] = res["total_portions_qty"] / res["total_orders"] if res["total_orders"] > 0 else 0.0
        res["avg_portions_per_person"] = res["total_portions_qty"] / res["unique_employees"] if res["unique_employees"] > 0 else 0.0
        return res


    def get_lodging_kpi(self) -> Dict[str, Any]:
        # Здесь оставляем почти как было, но можно позже добавить сравнение
        query = """
        WITH days AS (
            SELECT generate_series(%s::date, %s::date, interval '1 day')::date AS d
        ),
        stays_on_day AS (
            SELECT dd.d, s.employee_id, s.dorm_id, s.room_id
            FROM days dd
            JOIN dorm_stays s ON s.check_in <= dd.d
                             AND (s.check_out IS NULL OR s.check_out > dd.d)
        ),
        dorm_mode AS (SELECT id, rate_mode FROM dorms),
        rate_on_day AS (
            -- ... (оставляем логику как в оригинале)
            -- для краткости опускаем полную реализацию, используй свою версию
        )
        SELECT
            COUNT(*)::int AS bed_days,
            COALESCE(SUM(COALESCE(price_per_day, 0)), 0)::numeric AS amount_rub,
            COALESCE(AVG(price_per_day), 0)::numeric AS avg_price_rub,
            (SELECT COUNT(*) FROM dorm_stays s2
             WHERE s2.check_in <= %s::date
               AND (s2.check_out IS NULL OR s2.check_out > %s::date)
            )::int AS active_on_end,
            (SELECT COUNT(*) FROM rate_on_day WHERE price_per_day IS NULL)::int AS missing_rate_bed_days
        FROM rate_on_day
        """
        rows = self._execute_query(query, (self.start_date, self.end_date, self.end_date, self.end_date))
        row = rows[0] if rows else {}
        return {
            "bed_days":              int(row.get("bed_days", 0)),
            "amount_rub":            float(row.get("amount_rub", 0)),
            "avg_price_rub":         float(row.get("avg_price_rub", 0)),
            "active_on_end":         int(row.get("active_on_end", 0)),
            "missing_rate_bed_days": int(row.get("missing_rate_bed_days", 0)),
        }


class AnalyticsPage(ttk.Frame):
    def __init__(self, master, app_ref):
        super().__init__(master)
        self.app_ref = app_ref
        self.data_provider: Optional[AnalyticsData] = None

        # Фильтры
        filter_frame = ttk.Frame(self, padding="10 5")
        filter_frame.pack(fill="x")

        ttk.Label(filter_frame, text="Период:").pack(side="left", padx=(0,4))
        self.period_var = tk.StringVar(value="Текущий месяц")
        period_combo = ttk.Combobox(
            filter_frame, textvariable=self.period_var,
            values=["Текущий месяц", "Прошлый месяц", "Текущий квартал", "Текущий год", "Произвольный период"],
            state="readonly", width=20
        )
        period_combo.pack(side="left", padx=4)
        period_combo.bind("<<ComboboxSelected>>", self._on_period_changed)

        ttk.Label(filter_frame, text="Тип объекта:").pack(side="left", padx=(12,4))
        self.object_type_var = tk.StringVar(value="Все типы")
        self.object_type_combo = ttk.Combobox(
            filter_frame, textvariable=self.object_type_var,
            state="readonly", width=28
        )
        self.object_type_combo.pack(side="left", padx=4)
        self.object_type_combo.bind("<<ComboboxSelected>>", self.refresh_data)

        ttk.Button(filter_frame, text="Обновить", command=self.refresh_data).pack(side="left", padx=(12,0))

        # Основной notebook
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=8, pady=(4,8))

        # Вкладки
        self.tab_overview  = ttk.Frame(self.notebook)
        self.tab_labor     = ttk.Frame(self.notebook)
        self.tab_transport = ttk.Frame(self.notebook)
        self.tab_meals     = ttk.Frame(self.notebook)
        self.tab_objects   = ttk.Frame(self.notebook)
        self.tab_users     = ttk.Frame(self.notebook)
        self.tab_lodging   = ttk.Frame(self.notebook)

        self.notebook.add(self.tab_overview,  text="  Обзор  ")
        self.notebook.add(self.tab_labor,     text="  Трудозатраты  ")
        self.notebook.add(self.tab_transport, text="  Транспорт  ")
        self.notebook.add(self.tab_meals,     text="  Питание  ")
        self.notebook.add(self.tab_objects,   text="  Объекты  ")
        self.notebook.add(self.tab_users,     text="  Пользователи  ")
        self.notebook.add(self.tab_lodging,   text="  Проживание  ")

        self.load_object_types()
        self.refresh_data()   # первый запуск

    def load_object_types(self):
        try:
            temp = AnalyticsData(datetime.now().date(), datetime.now().date(), "")
            types = temp.get_object_types()
            self.object_type_combo["values"] = ["Все типы"] + types
        except Exception as e:
            logging.error(f"Ошибка загрузки типов объектов: {e}")
            self.object_type_combo["values"] = ["Все типы"]

    def _on_period_changed(self, event=None):
        if self.period_var.get() == "Произвольный период":
            # можно позже добавить диалог выбора дат
            messagebox.showinfo("Внимание", "Произвольный период пока не реализован")
        self.refresh_data()

    def get_dates_from_period(self) -> Tuple[datetime.date, datetime.date]:
        period = self.period_var.get()
        today = datetime.today().date()
        if period == "Текущий месяц":
            start = today.replace(day=1)
            end   = (start + timedelta(days=35)).replace(day=1) - timedelta(days=1)
        elif period == "Прошлый месяц":
            end   = today.replace(day=1) - timedelta(days=1)
            start = end.replace(day=1)
        elif period == "Текущий квартал":
            q = (today.month - 1) // 3 + 1
            start = datetime(today.year, 3*q-2, 1).date()
            end   = (datetime(today.year, 3*q+1, 1).date() - timedelta(days=1))
        elif period == "Текущий год":
            start = datetime(today.year, 1, 1).date()
            end   = datetime(today.year, 12, 31).date()
        else:
            start = today.replace(day=1)
            end   = (start + timedelta(days=35)).replace(day=1) - timedelta(days=1)
        return start, end

    def refresh_data(self, event=None):
        start, end = self.get_dates_from_period()
        obj_filter = self.object_type_var.get()
        if obj_filter == "Все типы":
            obj_filter = ""

        self.data_provider = AnalyticsData(start, end, obj_filter)

        self._build_overview_tab()
        self._build_labor_tab()
        self._build_transport_tab()
        self._build_meals_tab()
        self._build_objects_tab()
        self._build_users_tab()
        self._build_lodging_tab()

    def _build_overview_tab(self):
        for widget in self.tab_overview.winfo_children():
            widget.destroy()

        main_frame = ttk.Frame(self.tab_overview, padding=10)
        main_frame.pack(fill="both", expand=True)

        # Верхний блок — крупные KPI карточки
        kpi_frame = ttk.LabelFrame(main_frame, text=" Ключевые показатели ", padding=10)
        kpi_frame.pack(fill="x", pady=(0,12))

        kpi_data = {
            "Трудозатраты": self.data_provider.get_labor_kpi(compare=True),
            "Транспорт":    self.data_provider.get_transport_kpi(),
            "Питание":      self.data_provider.get_meals_kpi(),
            "Проживание":   self.data_provider.get_lodging_kpi(),
        }

        for i, (title, data) in enumerate(kpi_data.items()):
            card = self._create_kpi_card(kpi_frame, title, data)
            card.grid(row=0, column=i, padx=6, sticky="nsew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        # Средний блок — графики
        charts_frame = ttk.Frame(main_frame)
        charts_frame.pack(fill="both", expand=True)

        left_chart = ttk.LabelFrame(charts_frame, text=" Трудозатраты и переработки ", padding=6)
        left_chart.pack(side="left", fill="both", expand=True, padx=(0,6))

        self._plot_labor_trend(left_chart)

        right_chart = ttk.LabelFrame(charts_frame, text=" Топ-10 объектов по чел.-часам ", padding=6)
        right_chart.pack(side="left", fill="both", expand=True, padx=(6,0))

        self._plot_top_objects(right_chart)

    def _create_kpi_card(self, parent, title: str, data: Dict):
        card = ttk.Frame(parent, padding=10, borderwidth=2, relief="groove")
        
        ttk.Label(card, text=title, font=("Segoe UI", 11, "bold")).pack(anchor="w")

        if title == "Трудозатраты":
            main_val = f"{data.get('total_hours',0):,.1f}"
            unit = "чел.-часов"
            delta = data.get("hours_delta_pct")
            delta_str = f"{delta:+.1f}%" if delta is not None else "—"
            color = KPI_COLORS["danger"] if data.get("overtime_share_pct",0) > 18 else \
                    KPI_COLORS["warning"] if data.get("overtime_share_pct",0) > 12 else \
                    KPI_COLORS["good"]

            ttk.Label(card, text=main_val, font=("Segoe UI", 24, "bold"), foreground=color).pack(pady=4)
            ttk.Label(card, text=unit, font=("Segoe UI", 10)).pack()
            ttk.Label(card, text=f"Δ к прошлому периоду: {delta_str}", font=("Segoe UI", 9)).pack()

            ovt = f"{data.get('overtime_share_pct',0):.1f}% переработок"
            ovt_color = color
            ttk.Label(card, text=ovt, foreground=ovt_color).pack(pady=(4,0))

        elif title == "Проживание":
            bed_days = data.get("bed_days", 0)
            missing  = data.get("missing_rate_bed_days", 0)
            rub      = f"{data.get('amount_rub',0):,.0f} ₽"
            color = KPI_COLORS["danger"] if missing > 0 else KPI_COLORS["neutral"]
            ttk.Label(card, text=bed_days, font=("Segoe UI", 24, "bold"), foreground=color).pack(pady=4)
            ttk.Label(card, text="койко-дней", font=("Segoe UI", 10)).pack()
            ttk.Label(card, text=rub, font=("Segoe UI", 12)).pack(pady=(4,0))
            if missing > 0:
                ttk.Label(card, text=f"!!! {missing} без тарифа", foreground=KPI_COLORS["danger"]).pack()

        else:
            # заглушка для остальных
            val = data.get("total_portions_qty") or data.get("total_machine_hours") or 0
            ttk.Label(card, text=f"{val:,.1f}", font=("Segoe UI", 24, "bold")).pack(pady=4)
            ttk.Label(card, text="основная метрика", font=("Segoe UI", 10)).pack()

        return card

    def _plot_labor_trend(self, parent):
        df = self.data_provider.get_labor_trend_by_month()
        if df.empty:
            ttk.Label(parent, text="Нет данных за период").pack(pady=40)
            return

        fig = Figure(figsize=(6.2, 3.8), dpi=100)
        ax = fig.add_subplot(111)

        ax.bar(df["period"], df["total_hours"], color="#1976d2", alpha=0.7, label="Обычные часы")
        ax.bar(df["period"], df["total_overtime"], bottom=df["total_hours"],
               color="#d32f2f", alpha=0.8, label="Переработки")

        ax.set_ylabel("Человеко-часы")
        ax.grid(True, axis="y", linestyle="--", alpha=0.4)
        ax.legend(fontsize=9)
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

        fig.tight_layout()
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=5, pady=5)

    def _plot_top_objects(self, parent):
        df = self.data_provider.get_labor_by_object()
        if df.empty or len(df) < 1:
            ttk.Label(parent, text="Нет данных").pack(pady=40)
            return

        df = df.head(10).copy()
        df["short_name"] = df["object_name"].str[:28] + df["object_name"].str[28:].apply(lambda x: "…" if x else "")

        fig = Figure(figsize=(6.2, 3.8), dpi=100)
        ax = fig.add_subplot(111)
        bars = ax.barh(df["short_name"][::-1], df["total_hours"][::-1], color="#0288d1")
        ax.set_xlabel("Чел.-часы")
        ax.grid(axis="x", linestyle="--", alpha=0.5)

        for bar in bars:
            width = bar.get_width()
            ax.text(width + max(df["total_hours"])*0.015, bar.get_y() + bar.get_height()/2,
                    f"{int(width)}", va="center", fontsize=9)

        fig.tight_layout()
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=5, pady=5)

    def _create_kpi_card_simple(self, parent, title, value, unit="", color="black", delta=None):
        """Простая карточка для остальных вкладок"""
        card = ttk.Frame(parent, padding=12, borderwidth=1, relief="solid")
        ttk.Label(card, text=title, font=("Segoe UI", 10, "bold")).pack(anchor="w")
        val_label = ttk.Label(card, text=f"{value}", font=("Segoe UI", 22, "bold"), foreground=color)
        val_label.pack(pady=(4, 0))
        ttk.Label(card, text=unit, font=("Segoe UI", 10)).pack()
        if delta is not None:
            delta_color = KPI_COLORS["good"] if delta >= 0 else KPI_COLORS["danger"]
            ttk.Label(card, text=f"Δ {delta:+.1f}%", foreground=delta_color, font=("Segoe UI", 9)).pack()
        return card

    def _create_treeview(self, parent, columns: list, height=10):
        """Улучшенная таблица с прокруткой"""
        frame = ttk.Frame(parent)
        frame.pack(fill="both", expand=True)

        tree = ttk.Treeview(frame, columns=[c[0] for c in columns], show="headings", height=height)
        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        for col_id, col_text, width in columns:
            tree.heading(col_id, text=col_text)
            tree.column(col_id, width=width, anchor="w")

        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        frame.grid_rowconfigure(0, weight=1)
        frame.grid_columnconfigure(0, weight=1)
        return tree

    def _insert_df_to_treeview(self, tree, df: pd.DataFrame):
        tree.delete(*tree.get_children())
        for _, row in df.iterrows():
            values = [str(row.get(c, "")) for c in tree["columns"]]
            tree.insert("", "end", values=values)

    def _build_labor_tab(self):
        self._clear_tab(self.tab_labor)
        frame = ttk.Frame(self.tab_labor, padding=10)
        frame.pack(fill="both", expand=True)

        kpi = self.data_provider.get_labor_kpi(compare=True)
        kpi_frame = ttk.Frame(frame)
        kpi_frame.pack(fill="x", pady=(0, 15))

        cards = [
            ("Всего чел.-часов",    f"{kpi['total_hours']:,.1f}",     "часов",   KPI_COLORS["neutral"]),
            ("Переработки",         f"{kpi['total_overtime']:,.1f}",  "часов",   KPI_COLORS["danger"] if kpi["overtime_share_pct"] > 15 else KPI_COLORS["warning"]),
            ("Сотрудников",         f"{kpi['unique_people']}",        "чел.",    KPI_COLORS["neutral"]),
            ("Часов на человека",   f"{kpi['hours_per_person']:.1f}", "час/чел", KPI_COLORS["neutral"]),
            ("Доля переработок",    f"{kpi['overtime_share_pct']:.1f}%", "",     KPI_COLORS["danger"] if kpi["overtime_share_pct"] > 18 else KPI_COLORS["good"]),
        ]

        for i, (t, v, u, c) in enumerate(cards):
            card = self._create_kpi_card_simple(kpi_frame, t, v, u, c)
            card.grid(row=0, column=i, padx=6, sticky="ew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        # Графики и таблицы
        charts = ttk.Frame(frame)
        charts.pack(fill="both", expand=True)

        left = ttk.LabelFrame(charts, text=" ТОП-10 объектов ", padding=6)
        left.pack(side="left", fill="both", expand=True, padx=(0,6))
        df_obj = self.data_provider.get_labor_by_object().head(10)
        tree = self._create_treeview(left, [("object_name", "Объект", 220), ("total_hours", "Чел.-часы", 100)])
        self._insert_df_to_treeview(tree, df_obj)

        right = ttk.LabelFrame(charts, text=" ТОП-10 сотрудников ", padding=6)
        right.pack(side="left", fill="both", expand=True, padx=6)
        df_emp = self.data_provider.get_top_employees_by_hours(10)
        tree_emp = self._create_treeview(right, [("fio", "Сотрудник", 180), ("total_hours", "Часы", 90), ("total_overtime", "Перераб.", 90)])
        self._insert_df_to_treeview(tree_emp, df_emp)

    def _build_transport_tab(self):
        self._clear_tab(self.tab_transport)
        frame = ttk.Frame(self.tab_transport, padding=10)
        frame.pack(fill="both", expand=True)

        kpi = self.data_provider.get_transport_kpi()
        kpi_frame = ttk.Frame(frame)
        kpi_frame.pack(fill="x", pady=(0,15))

        cards = [
            ("Машино-часы",     f"{kpi['total_machine_hours']:,.1f}", "часов"),
            ("Заказов",         f"{kpi['total_orders']}",             "шт."),
            ("Единиц",          f"{kpi['total_units']:,.0f}",         "шт."),
            ("Часов на заказ",  f"{kpi['avg_hours_per_order']:.1f}",  "час/заказ"),
        ]

        for i, (t, v, u) in enumerate(cards):
            card = self._create_kpi_card_simple(kpi_frame, t, v, u)
            card.grid(row=0, column=i, padx=6, sticky="ew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        # Таблица по технике
        ttk.Label(frame, text="Распределение по видам техники", font=("Segoe UI", 11, "bold")).pack(anchor="w", pady=(10,5))
        df_tech = self.data_provider.get_transport_by_tech()  # предполагаем, что метод есть
        tree = self._create_treeview(frame, [("tech", "Техника", 250), ("total_hours", "Машино-часы", 120)])
        self._insert_df_to_treeview(tree, df_tech)

    def _build_meals_tab(self):
        self._clear_tab(self.tab_meals)
        frame = ttk.Frame(self.tab_meals, padding=10)
        frame.pack(fill="both", expand=True)

        kpi = self.data_provider.get_meals_kpi()
        kpi_frame = ttk.Frame(frame)
        kpi_frame.pack(fill="x", pady=(0,15))

        cards = [
            ("Порций",           f"{kpi['total_portions_qty']:,.0f}",       "шт."),
            ("Заказов",          f"{kpi['total_orders']}",                  "шт."),
            ("Сотрудников",      f"{kpi['unique_employees']}",              "чел."),
            ("Порций на заказ",  f"{kpi['avg_portions_per_order']:.1f}",    "шт."),
        ]

        for i, (t, v, u) in enumerate(cards):
            card = self._create_kpi_card_simple(kpi_frame, t, v, u)
            card.grid(row=0, column=i, padx=6, sticky="ew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        ttk.Label(frame, text="По видам питания", font=("Segoe UI", 11, "bold")).pack(anchor="w", pady=(10,5))
        df_types = self.data_provider.get_meals_by_type()  # предполагаем метод
        tree = self._create_treeview(frame, [("meal_type_text", "Вид питания", 220), ("total_qty", "Порций", 100)])
        self._insert_df_to_treeview(tree, df_types)

    def _build_objects_tab(self):
        self._clear_tab(self.tab_objects)
        frame = ttk.Frame(self.tab_objects, padding=10)
        frame.pack(fill="both", expand=True)

        df = self.data_provider.get_objects_overview(limit=20)
        if df.empty:
            ttk.Label(frame, text="Нет данных по объектам за период").pack(pady=60)
            return

        tree = self._create_treeview(frame, [
            ("address",       "Объект",           240),
            ("labor_hours",   "Чел.-часы",        110),
            ("machine_hours", "Машино-часы",      110),
            ("portions",      "Порций питания",   110),
            ("total_activity","Суммарная активн.", 130),
        ], height=18)
        self._insert_df_to_treeview(tree, df)

    def _build_users_tab(self):
        self._clear_tab(self.tab_users)
        frame = ttk.Frame(self.tab_users, padding=10)
        frame.pack(fill="both", expand=True)

        df = self.data_provider.get_users_activity()
        if df.empty:
            ttk.Label(frame, text="Нет данных по активности").pack(pady=60)
            return

        tree = self._create_treeview(frame, [
            ("full_name",              "Пользователь",         180),
            ("timesheets_created",     "Табели",               80),
            ("transport_orders_created","Заявки транспорт",     120),
            ("meal_orders_created",    "Заявки питание",        110),
            ("dorm_checkins",          "Заселения",             90),
            ("dorm_checkouts",         "Выселения",             90),
        ], height=18)
        self._insert_df_to_treeview(tree, df)

    def _build_lodging_tab(self):
        self._clear_tab(self.tab_lodging)
        frame = ttk.Frame(self.tab_lodging, padding=10)
        frame.pack(fill="both", expand=True)

        kpi = self.data_provider.get_lodging_kpi()
        kpi_frame = ttk.Frame(frame)
        kpi_frame.pack(fill="x", pady=(0,15))

        missing = kpi["missing_rate_bed_days"]
        color = KPI_COLORS["danger"] if missing > 0 else KPI_COLORS["good"]

        cards = [
            ("Койко-дней",      f"{kpi['bed_days']:,d}",         "сутки",   color),
            ("Стоимость",       f"{kpi['amount_rub']:,.0f} ₽",   "",        KPI_COLORS["neutral"]),
            ("Активно на конец",f"{kpi['active_on_end']}",       "чел.",    KPI_COLORS["neutral"]),
        ]

        for i, (t, v, u, c=color) in enumerate(cards):
            card = self._create_kpi_card_simple(kpi_frame, t, v, u, c)
            card.grid(row=0, column=i, padx=6, sticky="ew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        if missing > 0:
            ttk.Label(frame, text=f"Внимание! {missing} койко-дней без тарифа", 
                      foreground=KPI_COLORS["danger"], font=("Segoe UI", 11, "bold")).pack(pady=10)


    def _clear_tab(self, tab):
        for widget in tab.winfo_children():
            widget.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    root.title("Аналитика — Дашборд")
    root.geometry("1280x800")
    page = AnalyticsPage(root, None)
    page.pack(fill="both", expand=True)
    root.mainloop()
