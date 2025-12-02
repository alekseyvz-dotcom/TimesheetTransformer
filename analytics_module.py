import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import logging
import pandas as pd

# Импорты для графиков
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.ticker as mticker

# Глобальная переменная для хранения пула соединений
db_connection_pool: Optional[pool.SimpleConnectionPool] = None

def set_db_pool(pool: pool.SimpleConnectionPool):
    """Принимает пул соединений от главного приложения."""
    global db_connection_pool
    db_connection_pool = pool
    logging.info("Analytics Module: Пул соединений с БД установлен.")

class AnalyticsData:
    """Класс для выполнения SQL-запросов и получения данных для дашбордов."""

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
    
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
    
    def get_labor_kpi(self) -> Dict[str, Any]:
        """Получает KPI для дашборда 'Трудозатраты'."""
        query = """
        SELECT
            COALESCE(SUM(tr.total_hours), 0) as total_hours,
            COALESCE(SUM(tr.total_days), 0) as total_days,
            COALESCE(SUM(tr.overtime_day + tr.overtime_night), 0) as total_overtime,
            COUNT(DISTINCT tr.fio) as unique_employees
        FROM timesheet_headers th
        JOIN timesheet_rows tr ON th.id = tr.header_id
        WHERE th.year * 100 + th.month BETWEEN %s AND %s;
        """
        # Преобразуем даты в формат YYYYMM для сравнения
        start_period = self.start_date.year * 100 + self.start_date.month
        end_period = self.end_date.year * 100 + self.end_date.month
        
        result = self._execute_query(query, (start_period, end_period))
        return result[0] if result else {}

    def get_labor_by_object(self) -> pd.DataFrame:
        """Получает данные по трудозатратам в разрезе объектов."""
        query = """
        SELECT 
            COALESCE(o.short_name, th.object_addr) as object_name,
            SUM(tr.total_hours) as total_hours
        FROM timesheet_headers th
        JOIN timesheet_rows tr ON th.id = tr.header_id
        LEFT JOIN objects o ON th.object_db_id = o.id
        WHERE th.year * 100 + th.month BETWEEN %s AND %s
        GROUP BY object_name
        ORDER BY total_hours DESC;
        """
        start_period = self.start_date.year * 100 + self.start_date.month
        end_period = self.end_date.year * 100 + self.end_date.month

        data = self._execute_query(query, (start_period, end_period))
        return pd.DataFrame(data)

    def get_transport_kpi(self) -> Dict[str, Any]:
        """Получает KPI для дашборда 'Транспорт'."""
        query = """
        SELECT
            COALESCE(SUM(tp.hours), 0) as total_machine_hours,
            COUNT(DISTINCT t.id) as total_orders,
            COALESCE(SUM(tp.qty), 0) as total_units
        FROM transport_orders t
        JOIN transport_order_positions tp ON t.id = tp.order_id
        WHERE t.date BETWEEN %s AND %s;
        """
        result = self._execute_query(query, (self.start_date, self.end_date))
        kpi = result[0] if result else {}
        if kpi.get('total_orders', 0) > 0:
            kpi['avg_hours_per_order'] = kpi.get('total_machine_hours', 0) / kpi['total_orders']
        else:
            kpi['avg_hours_per_order'] = 0
        return kpi

    def get_transport_by_tech(self) -> pd.DataFrame:
        """Получает данные по машино-часам в разрезе техники."""
        query = """
        SELECT
            tp.tech,
            SUM(tp.hours) as total_hours
        FROM transport_orders t
        JOIN transport_order_positions tp ON t.id = tp.order_id
        WHERE t.date BETWEEN %s AND %s
        GROUP BY tp.tech
        ORDER BY total_hours DESC;
        """
        data = self._execute_query(query, (self.start_date, self.end_date))
        return pd.DataFrame(data)

class AnalyticsPage(ttk.Frame):
    """Главный фрейм для страницы аналитики."""

    def __init__(self, master, app_ref):
        super().__init__(master)
        self.app_ref = app_ref
        
        # --- Панель фильтров ---
        filter_frame = ttk.Frame(self, padding="10")
        filter_frame.pack(fill="x", side="top")
        
        ttk.Label(filter_frame, text="Период:").pack(side="left", padx=(0, 5))
        self.period_var = tk.StringVar(value="Текущий месяц")
        period_combo = ttk.Combobox(filter_frame, textvariable=self.period_var, 
                                    values=["Текущий месяц", "Прошлый месяц", "Текущий квартал", "Текущий год"],
                                    state="readonly")
        period_combo.pack(side="left", padx=5)
        period_combo.bind("<<ComboboxSelected>>", self.refresh_data)

        ttk.Button(filter_frame, text="Обновить", command=self.refresh_data).pack(side="left", padx=10)

        # --- Контейнер для дашбордов ---
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.tab_labor = ttk.Frame(self.notebook)
        self.tab_transport = ttk.Frame(self.notebook)
        
        self.notebook.add(self.tab_labor, text="  Трудозатраты  ")
        self.notebook.add(self.tab_transport, text="  Транспорт и Техника  ")

        # Инициализация и первое обновление
        self.refresh_data()

    def get_dates_from_period(self):
        """Возвращает начальную и конечную дату в зависимости от выбранного периода."""
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
        else: # По умолчанию текущий месяц
            start_date = today.replace(day=1)
            end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        
        return start_date.date(), end_date.date()

    def refresh_data(self, event=None):
        """Обновляет все данные и перерисовывает дашборды."""
        start_date, end_date = self.get_dates_from_period()
        self.data_provider = AnalyticsData(start_date, end_date)
        
        self._build_labor_tab()
        self._build_transport_tab()

    def _clear_tab(self, tab):
        for widget in tab.winfo_children():
            widget.destroy()

    def _create_kpi_card(self, parent, title, value, unit):
        """Создает виджет-карточку для отображения KPI."""
        card = ttk.Frame(parent, borderwidth=2, relief="groove", padding=10)
        ttk.Label(card, text=title, font=("Segoe UI", 10, "bold")).pack()
        ttk.Label(card, text=f"{value}", font=("Segoe UI", 18, "bold"), foreground="#0078D7").pack(pady=(5, 0))
        ttk.Label(card, text=unit, font=("Segoe UI", 9)).pack()
        return card

    def _build_labor_tab(self):
        self._clear_tab(self.tab_labor)
        
        # --- KPI ---
        kpi_frame = ttk.Frame(self.tab_labor)
        kpi_frame.pack(fill="x", pady=10, padx=5)
        
        kpi_data = self.data_provider.get_labor_kpi()

        cards_data = [
            ("Всего чел.-часов", f"{kpi_data.get('total_hours', 0):.1f}", "час."),
            ("Всего чел.-дней", int(kpi_data.get('total_days', 0)), "дн."),
            ("Часы переработок", f"{kpi_data.get('total_overtime', 0):.1f}", "час."),
            ("Сотрудников", kpi_data.get('unique_employees', 0), "чел."),
        ]
        for i, (title, value, unit) in enumerate(cards_data):
            card = self._create_kpi_card(kpi_frame, title, value, unit)
            card.grid(row=0, column=i, padx=5, sticky="ew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        # --- График ---
        df = self.data_provider.get_labor_by_object()
        if not df.empty:
            fig = Figure(figsize=(10, 5), dpi=100)
            ax = fig.add_subplot(111)
            
            df_plot = df.head(10).sort_values('total_hours', ascending=True)
            bars = ax.barh(df_plot['object_name'], df_plot['total_hours'], color='#0078D7')

            ax.set_title("ТОП-10 объектов по трудозатратам", fontsize=12, weight='bold')
            ax.set_xlabel("Человеко-часы")
            ax.grid(axis='x', linestyle='--', alpha=0.7)
            fig.tight_layout()

            # Добавляем значения на бары
            for bar in bars:
                width = bar.get_width()
                ax.text(width + 5, bar.get_y() + bar.get_height()/2, f'{width:.0f}', va='center')

            canvas = FigureCanvasTkAgg(fig, master=self.tab_labor)
            canvas.draw()
            canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True, pady=10)

    def _build_transport_tab(self):
        self._clear_tab(self.tab_transport)
        
        # --- KPI ---
        kpi_frame = ttk.Frame(self.tab_transport)
        kpi_frame.pack(fill="x", pady=10, padx=5)
        
        kpi_data = self.data_provider.get_transport_kpi()
        cards_data = [
            ("Всего маш.-часов", f"{kpi_data.get('total_machine_hours', 0):.1f}", "час."),
            ("Всего заявок", kpi_data.get('total_orders', 0), "шт."),
            ("Единиц техники", kpi_data.get('total_units', 0), "шт."),
            ("Среднее время", f"{kpi_data.get('avg_hours_per_order', 0):.1f}", "час./заявку"),
        ]
        for i, (title, value, unit) in enumerate(cards_data):
            card = self._create_kpi_card(kpi_frame, title, value, unit)
            card.grid(row=0, column=i, padx=5, sticky="ew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        # --- График ---
        df = self.data_provider.get_transport_by_tech()
        if not df.empty:
            fig = Figure(figsize=(10, 5), dpi=100)
            ax = fig.add_subplot(111)

            df_plot = df.head(10).sort_values('total_hours', ascending=False)
            bars = ax.bar(df_plot['tech'], df_plot['total_hours'], color='#5E9A2C')
            
            ax.set_title("ТОП-10 востребованной техники", fontsize=12, weight='bold')
            ax.set_ylabel("Машино-часы")
            ax.tick_params(axis='x', rotation=45, labelsize=9)
            ax.grid(axis='y', linestyle='--', alpha=0.7)
            fig.tight_layout(pad=2.0)
            
            canvas = FigureCanvasTkAgg(fig, master=self.tab_transport)
            canvas.draw()
            canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True, pady=10)
