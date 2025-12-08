# analytics_module.py

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
import matplotlib.ticker as mticker

# Глобальная переменная для хранения пула соединений
db_connection_pool: Optional[pool.SimpleConnectionPool] = None


def set_db_pool(db_pool: pool.SimpleConnectionPool):
    """Принимает пул соединений от главного приложения."""
    global db_connection_pool
    db_connection_pool = db_pool
    logging.info("Analytics Module: Пул соединений с БД установлен.")


# ============================================================
#                      DATA PROVIDER
# ============================================================

class AnalyticsData:
    """Класс для выполнения SQL-запросов и получения данных для дашбордов."""

    def __init__(self, start_date, end_date, object_type_filter: str):
        self.start_date = start_date
        self.end_date = end_date
        self.object_type_filter = object_type_filter  # short_name из objects

    # ---------- Внутренние утилиты ----------

    def _execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Универсальный метод для выполнения запросов к БД."""
        if not db_connection_pool:
            raise ConnectionError("Пул соединений с БД не был инициализирован.")
        conn = None
        try:
            conn = db_connection_pool.getconn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchall()
        except Exception as e:
            logging.error(f"Ошибка выполнения SQL-запроса в модуле аналитики: {e}")
            messagebox.showerror("Ошибка БД", f"Не удалось получить данные для аналитики:\n{e}")
            return []
        finally:
            if conn:
                db_connection_pool.putconn(conn)

    def get_object_types(self) -> List[str]:
        """Получает уникальные значения 'short_name' из таблицы объектов для фильтра."""
        query = """
        SELECT DISTINCT short_name
        FROM objects
        WHERE short_name IS NOT NULL AND short_name <> ''
        ORDER BY short_name;
        """
        results = self._execute_query(query)
        return [row['short_name'] for row in results]

    def _get_filter_clauses_by_object_id(
        self, table_alias: str, field: str = "object_id"
    ) -> Tuple[str, str, list]:
        """
        Вспомогательная функция: вернёт join_clause, filter_clause, params_list
        для фильтрации по типу объекта (objects.short_name) по полю <alias>.<field>.
        """
        params = []
        join_clause = ""
        filter_clause = ""

        if self.object_type_filter:
            join_clause = f"LEFT JOIN objects o ON {table_alias}.{field} = o.id"
            filter_clause = "AND o.short_name = %s"
            params.append(self.object_type_filter)

        return join_clause, filter_clause, params

    # ============================================================
    #                      1. ТРУДОЗАТРАТЫ
    # ============================================================

    def get_labor_kpi(self) -> Dict[str, Any]:
        """KPI по трудозатратам за период."""
        base_query = """
        SELECT
            COALESCE(SUM(tr.total_hours), 0)                      AS total_hours,
            COALESCE(SUM(tr.total_days), 0)                       AS total_days,
            COALESCE(SUM(tr.overtime_day + tr.overtime_night), 0) AS total_overtime,
            COUNT(DISTINCT tr.fio)                                AS unique_employees
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
        return result[0] if result else {}

    def get_labor_by_object(self) -> pd.DataFrame:
        """Трудозатраты в разрезе объектов."""
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
        return pd.DataFrame(data)

    # 1.1. Динамика трудозатрат по месяцам
    def get_labor_trend_by_month(self) -> pd.DataFrame:
        """
        Возвращает суммарные человеко-часы по месяцам в выбранном периоде.
        """
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
        return pd.DataFrame(data)

    # 1.2. ТОП‑сотрудники по часам
    def get_top_employees_by_hours(self, limit: int = 10) -> pd.DataFrame:
        """
        ТОП сотрудников по суммарным часам за период.
        """
        base_query = f"""
        SELECT
            tr.fio,
            SUM(tr.total_hours) AS total_hours,
            SUM(tr.overtime_day + tr.overtime_night) AS total_overtime
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

        query = base_query.format(join_clause=join_clause, filter_clause=filter_clause)
        data = self._execute_query(query, tuple(params))
        return pd.DataFrame(data)

    # 1.3. Нагрузка по подразделениям
    def get_labor_by_department(self) -> pd.DataFrame:
        """
        Суммарные человеко-часы по подразделениям.
        """
        base_query = """
        SELECT
            d.name AS department_name,
            SUM(tr.total_hours) AS total_hours
        FROM timesheet_headers th
        JOIN timesheet_rows tr ON th.id = tr.header_id
        LEFT JOIN departments d ON th.department_id = d.id
        {join_clause}
        WHERE (th.year * 100 + th.month) BETWEEN %s AND %s
        {filter_clause}
        GROUP BY d.name
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
        return pd.DataFrame(data)

    # ============================================================
    #                      2. ТРАНСПОРТ
    # ============================================================

    def get_transport_kpi(self) -> Dict[str, Any]:
        """KPI по транспорту и технике."""
        join_clause, filter_clause, extra_params = self._get_filter_clauses_by_object_id('t', 'object_id')

        query = f"""
        SELECT
            COALESCE(SUM(tp.hours), 0) AS total_machine_hours,
            COUNT(DISTINCT t.id)      AS total_orders,
            COALESCE(SUM(tp.qty), 0)  AS total_units
        FROM transport_orders t
        JOIN transport_order_positions tp ON t.id = tp.order_id
        {join_clause}
        WHERE t.date BETWEEN %s AND %s
        {filter_clause};
        """
        params: List[Any] = [self.start_date, self.end_date]
        params.extend(extra_params)

        result = self._execute_query(query, tuple(params))
        kpi = result[0] if result else {}
        if kpi.get('total_orders', 0) > 0:
            kpi['avg_hours_per_order'] = kpi.get('total_machine_hours', 0) / kpi['total_orders']
        else:
            kpi['avg_hours_per_order'] = 0
        return kpi

    def get_transport_by_tech(self) -> pd.DataFrame:
        """Машино-часы в разрезе техники."""
        join_clause, filter_clause, extra_params = self._get_filter_clauses_by_object_id('t', 'object_id')

        query = f"""
        SELECT
            tp.tech,
            SUM(tp.hours) AS total_hours
        FROM transport_orders t
        JOIN transport_order_positions tp ON t.id = tp.order_id
        {join_clause}
        WHERE t.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY tp.tech
        ORDER BY total_hours DESC;
        """
        params: List[Any] = [self.start_date, self.end_date]
        params.extend(extra_params)

        data = self._execute_query(query, tuple(params))
        return pd.DataFrame(data)

    # ============================================================
    #                      3. ПИТАНИЕ
    # ============================================================

    def get_meals_kpi(self) -> Dict[str, Any]:
        """KPI по питанию."""
        join_clause, filter_clause, extra_params = self._get_filter_clauses_by_object_id('mo', 'object_id')

        query = f"""
        SELECT
            COUNT(moi.id)                   AS total_portions,
            COUNT(DISTINCT mo.id)           AS total_orders,
            COUNT(DISTINCT moi.employee_id) AS unique_employees
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        {join_clause}
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause};
        """
        params: List[Any] = [self.start_date, self.end_date]
        params.extend(extra_params)

        result = self._execute_query(query, tuple(params))
        return result[0] if result else {}

    def get_meals_by_type(self) -> pd.DataFrame:
        """Количество порций в разрезе типов питания."""
        join_clause, filter_clause, extra_params = self._get_filter_clauses_by_object_id('mo', 'object_id')

        query = f"""
        SELECT
            moi.meal_type_text,
            COUNT(moi.id) AS total_count
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        {join_clause}
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY moi.meal_type_text
        ORDER BY total_count DESC;
        """
        params: List[Any] = [self.start_date, self.end_date]
        params.extend(extra_params)

        data = self._execute_query(query, tuple(params))
        return pd.DataFrame(data)

    # 3.1 Динамика по дням/месяцам (порции)
    def get_meals_trend_by_day(self) -> pd.DataFrame:
        """Количество порций по дням в периоде."""
        join_clause, filter_clause, extra_params = self._get_filter_clauses_by_object_id('mo', 'object_id')

        query = f"""
        SELECT
            mo.date,
            COUNT(moi.id) AS total_portions
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        {join_clause}
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY mo.date
        ORDER BY mo.date;
        """
        params: List[Any] = [self.start_date, self.end_date]
        params.extend(extra_params)

        data = self._execute_query(query, tuple(params))
        return pd.DataFrame(data)

    def get_meals_trend_by_month(self) -> pd.DataFrame:
        """Количество порций по месяцам в периоде."""
        join_clause, filter_clause, extra_params = self._get_filter_clauses_by_object_id('mo', 'object_id')

        query = f"""
        SELECT
            date_trunc('month', mo.date) AS period,
            COUNT(moi.id) AS total_portions
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        {join_clause}
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY period
        ORDER BY period;
        """
        params: List[Any] = [self.start_date, self.end_date]
        params.extend(extra_params)

        data = self._execute_query(query, tuple(params))
        return pd.DataFrame(data)

    # 3.2 Питание по объектам
    def get_meals_by_object(self, limit: int = 10) -> pd.DataFrame:
        """Количество порций и людей по объектам."""
        join_clause, filter_clause, extra_params = self._get_filter_clauses_by_object_id('mo', 'object_id')

        query = f"""
        SELECT
            o.address AS object_name,
            COUNT(moi.id) AS total_portions,
            COUNT(DISTINCT moi.employee_id) AS unique_employees
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        LEFT JOIN objects o ON mo.object_id = o.id
        {join_clause.replace('LEFT JOIN objects o', 'LEFT JOIN objects o2')} -- чтобы не дублировать alias
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause.replace('o.', 'o2.')}
        GROUP BY o.address
        HAVING o.address IS NOT NULL
        ORDER BY total_portions DESC
        LIMIT {limit};
        """
        # ВНИМАНИЕ: выше мы хитро заменили алиасы, чтобы не иметь дважды "objects o"
        params: List[Any] = [self.start_date, self.end_date]
        params.extend(extra_params)

        data = self._execute_query(query, tuple(params))
        return pd.DataFrame(data)

    # 3.3 Питание по подразделениям
    def get_meals_by_department(self) -> pd.DataFrame:
        """Питание по подразделениям: порции и люди."""
        join_clause, filter_clause, extra_params = self._get_filter_clauses_by_object_id('mo', 'object_id')

        query = f"""
        SELECT
            d.name AS department_name,
            COUNT(moi.id) AS total_portions,
            COUNT(DISTINCT moi.employee_id) AS unique_employees
        FROM meal_orders mo
        JOIN meal_order_items moi ON mo.id = moi.order_id
        LEFT JOIN departments d ON mo.department_id = d.id
        {join_clause}
        WHERE mo.date BETWEEN %s AND %s
        {filter_clause}
        GROUP BY d.name
        ORDER BY total_portions DESC;
        """
        params: List[Any] = [self.start_date, self.end_date]
        params.extend(extra_params)

        data = self._execute_query(query, tuple(params))
        return pd.DataFrame(data)

    # ============================================================
    #        4. СКВОЗНАЯ АНАЛИТИКА ПО ОБЪЕКТАМ (TOP-N КАРТОЧКА)
    # ============================================================

    def get_objects_overview(self, limit: int = 20) -> pd.DataFrame:
        """
        Сводная информация по объектам: человеко-часы, машино-часы, порции.
        Ограничивается TOP-N по человеко-часам.
        """
        # Периоды для всех модулей берем один и тот же (по датам).
        start_period = self.start_date.year * 100 + self.start_date.month
        end_period = self.end_date.year * 100 + self.end_date.month

        query = f"""
        WITH labor AS (
            SELECT
                th.object_db, 5))

        table_frame = ttk.LabelFrame(top_frame, text="Активность пользователей")
        table_frame.pack(side="left", fill="both", expand=True, padx=(0, 5))

        tree = self._create_treeview(
            table_frame,
            columns=[
                ("user", "Логин"),
                ("name", "ФИО"),
                ("th", "Табелей"),
                ("tr", "Заявок на транспорт"),
                ("mo", "Заявок на питание"),
            ],
            height=15,
        )
        tree.column("user", width=100)
        tree.column("name", width=180)
        tree.column("th", width=80, anchor="e")
        tree.column("tr", width=120, anchor="e")
        tree.column("mo", width=120, anchor="e")

        df["total_ops"] = (
            df["timesheets_created"] + df["transport_orders_created"] + df["meal_orders_created"]
        )

        for _, row in df.iterrows():
            tree.insert(
                "",
                "end",
                values=(
                    row["username"],
                    row["full_name"] or "",
                    row["timesheets_created"],
                    row["transport_orders_created"],
                    row["meal_orders_created"],
                ),
            )

        # Справа — бар-чарт по общему числу операций
        chart_frame = ttk.LabelFrame(top_frame, text="ТОП пользователей по количеству операций")
        chart_frame.pack(side="left", fill="both", expand=True, padx=(5, 0))

        df_top = df.sort_values("total_ops", ascending=False).head(10)
        fig = Figure(figsize=(5, 4), dpi=100)
        ax = fig.add_subplot(111)
        bars = ax.barh(df_top["username"], df_top["total_ops"], color="#0078D7")
        ax.set_xlabel("Операций (табели + заявки)")
        ax.invert_yaxis()
        ax.grid(axis="x", alpha=0.3, linestyle="--")
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.5, bar.get_y() + bar.get_height() / 2, f"{int(width)}", va="center")
        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
