import os
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Tuple

import tkinter as tk
from tkinter import ttk, messagebox

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from psycopg2.extras import RealDictCursor


# ---------------- DB pool wiring (как в других модулях) ----------------

db_connection_pool = None

def set_db_pool(pool):
    global db_connection_pool
    db_connection_pool = pool

def get_db_connection():
    if db_connection_pool is None:
        raise RuntimeError("Пул соединений не был установлен из главного приложения.")
    return db_connection_pool.getconn()

def release_db_connection(conn):
    if db_connection_pool and conn:
        db_connection_pool.putconn(conn)


# ---------------- Utils ----------------

def parse_date_any(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def exe_dir() -> str:
    # в main_app у тебя есть своя exe_dir, но здесь сделаем локально просто для Excel
    import sys
    from pathlib import Path
    if getattr(sys, "frozen", False):
        return str(Path(sys.executable).resolve().parent)
    return str(Path(__file__).resolve().parent)

def _norm(s: str) -> str:
    # нормализация для сравнения названий типов питания
    return " ".join((s or "").strip().lower().split())


# ---------------- Business constants ----------------

# Соответствие комплексов типам питания (как ты сказал)
COMPLEX_MAP = {
    "Комплекс 1": _norm("одноразоваое питание"),
    "Комплекс 2": _norm("двухразовое питание"),
    "Комплекс 3": _norm("трехразовое питание"),
}

COMPLEX_ORDER = ["Комплекс 1", "Комплекс 2", "Комплекс 3"]


# ---------------- Queries ----------------

def _load_price_map(conn) -> Dict[str, float]:
    """
    Возвращает map: нормализованное имя типа питания -> цена
    """
    with conn.cursor() as cur:
        cur.execute("SELECT name, COALESCE(price, 0) FROM meal_types")
        res = {}
        for name, price in cur.fetchall():
            res[_norm(name)] = float(price or 0)
        return res

def _fetch_meals_items(conn, date_from: date, date_to: date) -> List[Dict[str, Any]]:
    """
    Тянем плоские строки по заказам за период: date, address, meal_type_name, qty.
    meal_type_name берём из meal_types.name, а если meal_type_id NULL — fallback на текст.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                mo.date::date AS service_date,
                COALESCE(mo.fact_address, o.address, '') AS object_address,
                COALESCE(mt.name, moi.meal_type_text, '') AS meal_type_name,
                COALESCE(moi.quantity, 1) AS qty
            FROM meal_orders mo
            JOIN meal_order_items moi ON moi.order_id = mo.id
            LEFT JOIN objects o ON o.id = mo.object_id
            LEFT JOIN meal_types mt ON mt.id = moi.meal_type_id
            WHERE mo.date >= %s AND mo.date <= %s
            """,
            (date_from, date_to),
        )
        return [dict(r) for r in cur.fetchall()]

def build_daily_report(date_from: date, date_to: date) -> List[Dict[str, Any]]:
    """
    Верхняя таблица: строки по (дата, объект).
    Для каждого комплекса: qty, price, amount.
    """
    conn = None
    try:
        conn = get_db_connection()
        price_map = _load_price_map(conn)
        rows = _fetch_meals_items(conn, date_from, date_to)

        # агрегация: (date, address) -> complex -> qty
        agg: Dict[Tuple[date, str], Dict[str, float]] = {}

        for r in rows:
            d = r["service_date"]
            addr = (r.get("object_address") or "").strip()
            mt_norm = _norm(r.get("meal_type_name") or "")
            qty = float(r.get("qty") or 0)

            if not addr:
                addr = "(без адреса)"

            # определяем комплекс
            complex_name = None
            for cx, mt_need in COMPLEX_MAP.items():
                if mt_norm == mt_need:
                    complex_name = cx
                    break
            if complex_name is None:
                continue  # лишние типы питания в этот отчет не включаем

            key = (d, addr)
            m = agg.setdefault(key, {cx: 0.0 for cx in COMPLEX_ORDER})
            m[complex_name] += qty

        # формируем итоговые строки
        out: List[Dict[str, Any]] = []
        for (d, addr), cx_qty in sorted(agg.items(), key=lambda x: (x[0][0], x[0][1])):
            rec: Dict[str, Any] = {
                "date": d,
                "address": addr,
                "complexes": {},
                "row_total_amount": 0.0,
            }
            total_amount = 0.0
            for cx in COMPLEX_ORDER:
                mt_norm = COMPLEX_MAP[cx]
                price = float(price_map.get(mt_norm, 0.0))
                qty = float(cx_qty.get(cx, 0.0))
                amount = price * qty
                rec["complexes"][cx] = {"price": price, "qty": qty, "amount": amount}
                total_amount += amount
            rec["row_total_amount"] = total_amount
            out.append(rec)

        return out
    finally:
        if conn:
            release_db_connection(conn)

def build_monthly_report(month_date: date) -> List[Dict[str, Any]]:
    """
    Нижняя таблица: строки по объекту за месяц month_date.year/month_date.month.
    """
    year = month_date.year
    month = month_date.month
    date_from = date(year, month, 1)
    # конец месяца
    if month == 12:
        date_to = date(year + 1, 1, 1)
    else:
        date_to = date(year, month + 1, 1)
    date_to = date_to.replace(day=1)  # first of next month
    # сделать inclusive end:
    date_to_incl = date_to - datetime.resolution  # не работает для date
    # проще: date_to_exclusive и в SQL "<"
    conn = None
    try:
        conn = get_db_connection()
        price_map = _load_price_map(conn)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(mo.fact_address, o.address, '') AS object_address,
                    COALESCE(mt.name, moi.meal_type_text, '') AS meal_type_name,
                    COALESCE(moi.quantity, 1) AS qty
                FROM meal_orders mo
                JOIN meal_order_items moi ON moi.order_id = mo.id
                LEFT JOIN objects o ON o.id = mo.object_id
                LEFT JOIN meal_types mt ON mt.id = moi.meal_type_id
                WHERE mo.date >= %s AND mo.date < %s
                """,
                (date_from, date_to),
            )
            rows = [dict(r) for r in cur.fetchall()]

        agg: Dict[str, Dict[str, float]] = {}  # address -> complex -> qty
        for r in rows:
            addr = (r.get("object_address") or "").strip() or "(без адреса)"
            mt_norm = _norm(r.get("meal_type_name") or "")
            qty = float(r.get("qty") or 0)

            complex_name = None
            for cx, mt_need in COMPLEX_MAP.items():
                if mt_norm == mt_need:
                    complex_name = cx
                    break
            if complex_name is None:
                continue

            m = agg.setdefault(addr, {cx: 0.0 for cx in COMPLEX_ORDER})
            m[complex_name] += qty

        out: List[Dict[str, Any]] = []
        for addr, cx_qty in sorted(agg.items(), key=lambda x: x[0]):
            rec: Dict[str, Any] = {
                "address": addr,
                "complexes": {},
                "row_total_amount": 0.0,
            }
            total_amount = 0.0
            for cx in COMPLEX_ORDER:
                mt_norm = COMPLEX_MAP[cx]
                price = float(price_map.get(mt_norm, 0.0))
                qty = float(cx_qty.get(cx, 0.0))
                amount = price * qty
                rec["complexes"][cx] = {"price": price, "qty": qty, "amount": amount}
                total_amount += amount
            rec["row_total_amount"] = total_amount
            out.append(rec)

        return out
    finally:
        if conn:
            release_db_connection(conn)


# ---------------- UI Page ----------------

class MealsReportsPage(tk.Frame):
    def __init__(self, master, app_ref=None):
        super().__init__(master, bg="#f7f7f7")
        self.app_ref = app_ref

        self.daily_tree = None
        self.monthly_tree = None

        self._daily_data: List[Dict[str, Any]] = []
        self._monthly_data: List[Dict[str, Any]] = []

        self._build_ui()

    def _build_ui(self):
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(fill="x", padx=10, pady=8)

        tk.Label(top, text="Период:", bg="#f7f7f7").grid(row=0, column=0, sticky="w")

        self.ent_from = ttk.Entry(top, width=12)
        self.ent_to = ttk.Entry(top, width=12)

        today = date.today()
        self.ent_from.insert(0, today.strftime("%Y-%m-%d"))
        self.ent_to.insert(0, today.strftime("%Y-%m-%d"))

        self.ent_from.grid(row=0, column=1, padx=(6, 4), sticky="w")
        tk.Label(top, text="—", bg="#f7f7f7").grid(row=0, column=2, sticky="w")
        self.ent_to.grid(row=0, column=3, padx=(4, 10), sticky="w")

        ttk.Button(top, text="Сформировать", command=self._reload).grid(row=0, column=4, padx=4)
        ttk.Button(top, text="Выгрузить в Excel", command=self._export_excel).grid(row=0, column=5, padx=4)

        # ---- Верхняя таблица (дневной/интервал) ----
        box1 = tk.LabelFrame(self, text="Отчет за день / период (дата + объект)")
        box1.pack(fill="both", expand=True, padx=10, pady=(4, 8))

        daily_cols = self._daily_columns()
        self.daily_tree = ttk.Treeview(box1, columns=daily_cols, show="headings", height=12)
        self._setup_tree_columns(self.daily_tree, daily_cols, daily=True)

        vsb1 = ttk.Scrollbar(box1, orient="vertical", command=self.daily_tree.yview)
        self.daily_tree.configure(yscrollcommand=vsb1.set)
        self.daily_tree.pack(side="left", fill="both", expand=True)
        vsb1.pack(side="right", fill="y")

        # ---- Нижняя таблица (месячный) ----
        box2 = tk.LabelFrame(self, text="Месячный отчет (объект за месяц)")
        box2.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        monthly_cols = self._monthly_columns()
        self.monthly_tree = ttk.Treeview(box2, columns=monthly_cols, show="headings", height=10)
        self._setup_tree_columns(self.monthly_tree, monthly_cols, daily=False)

        vsb2 = ttk.Scrollbar(box2, orient="vertical", command=self.monthly_tree.yview)
        self.monthly_tree.configure(yscrollcommand=vsb2.set)
        self.monthly_tree.pack(side="left", fill="both", expand=True)
        vsb2.pack(side="right", fill="y")

        self._reload()

    def _daily_columns(self) -> List[str]:
        cols = ["service_date", "object"]
        for cx in COMPLEX_ORDER:
            cols += [f"{cx}_price", f"{cx}_qty", f"{cx}_amount"]
        cols.append("total_amount")
        return cols

    def _monthly_columns(self) -> List[str]:
        cols = ["object"]
        for cx in COMPLEX_ORDER:
            cols += [f"{cx}_price", f"{cx}_qty", f"{cx}_amount"]
        cols.append("total_amount")
        return cols

    def _setup_tree_columns(self, tree: ttk.Treeview, cols: List[str], daily: bool):
        # headings
        if daily:
            tree.heading("service_date", text="Дата оказания услуги")
            tree.heading("object", text="Наименование строительного объекта")
        else:
            tree.heading("object", text="Наименование строительного объекта")

        for cx in COMPLEX_ORDER:
            tree.heading(f"{cx}_price", text=f"{cx}: Цена")
            tree.heading(f"{cx}_qty", text=f"{cx}: Кол-во порций")
            tree.heading(f"{cx}_amount", text=f"{cx}: Стоимость")

        tree.heading("total_amount", text="ИТОГО стоимость")

        # widths
        if daily:
            tree.column("service_date", width=120, anchor="center", stretch=False)
            tree.column("object", width=320, anchor="w")
        else:
            tree.column("object", width=420, anchor="w")

        for cx in COMPLEX_ORDER:
            tree.column(f"{cx}_price", width=90, anchor="e", stretch=False)
            tree.column(f"{cx}_qty", width=120, anchor="e", stretch=False)
            tree.column(f"{cx}_amount", width=110, anchor="e", stretch=False)

        tree.column("total_amount", width=130, anchor="e", stretch=False)

    def _read_period(self) -> Tuple[Optional[date], Optional[date]]:
        d_from = parse_date_any(self.ent_from.get())
        d_to = parse_date_any(self.ent_to.get())
        return d_from, d_to

    def _reload(self):
        d_from, d_to = self._read_period()
        if not d_from or not d_to:
            messagebox.showwarning("Отчеты питания", "Укажите период (Дата с/по).", parent=self)
            return
        if d_from > d_to:
            messagebox.showwarning("Отчеты питания", "Дата 'с' больше даты 'по'.", parent=self)
            return

        # daily/period report
        try:
            self._daily_data = build_daily_report(d_from, d_to)
        except Exception as e:
            messagebox.showerror("Отчеты питания", f"Ошибка формирования дневного отчета:\n{e}", parent=self)
            return

        # monthly report: строго 1 месяц (по твоей бухгалтерской логике)
        if (d_from.year, d_from.month) != (d_to.year, d_to.month):
            self._monthly_data = []
            self._populate_tree(self.daily_tree, self._daily_data, daily=True)
            self._populate_tree(self.monthly_tree, self._monthly_data, daily=False)
            messagebox.showwarning(
                "Отчеты питания",
                "Период пересекает два месяца.\n"
                "Месячный отчет формируется только если 'с' и 'по' в одном месяце.",
                parent=self,
            )
            return

        try:
            self._monthly_data = build_monthly_report(d_from)
        except Exception as e:
            messagebox.showerror("Отчеты питания", f"Ошибка формирования месячного отчета:\n{e}", parent=self)
            return

        self._populate_tree(self.daily_tree, self._daily_data, daily=True)
        self._populate_tree(self.monthly_tree, self._monthly_data, daily=False)

    def _populate_tree(self, tree: ttk.Treeview, data: List[Dict[str, Any]], daily: bool):
        tree.delete(*tree.get_children())

        # итоги по колонкам amount
        totals = {cx: 0.0 for cx in COMPLEX_ORDER}
        grand_total = 0.0

        for rec in data:
            values = []
            if daily:
                d: date = rec["date"]
                values.append(d.strftime("%Y-%m-%d"))
            values.append(rec.get("address") or rec.get("object") or rec.get("address") or "")

            for cx in COMPLEX_ORDER:
                c = rec["complexes"].get(cx, {})
                price = float(c.get("price") or 0)
                qty = float(c.get("qty") or 0)
                amount = float(c.get("amount") or 0)
                values += [f"{price:.2f}", f"{qty:.2f}", f"{amount:.2f}"]
                totals[cx] += amount
                grand_total += amount

            values.append(f"{float(rec.get('row_total_amount') or 0):.2f}")
            tree.insert("", "end", values=values)

        # строка ИТОГО
        if data:
            if daily:
                total_label = "ИТОГО"
                values = [total_label, ""]
            else:
                values = ["ИТОГО"]

            for cx in COMPLEX_ORDER:
                # цена в итого бессмысленна — оставим пусто
                values += ["", "", f"{totals[cx]:.2f}"]
            values.append(f"{grand_total:.2f}")

            tree.insert("", "end", values=values)

    def _export_excel(self):
        d_from, d_to = self._read_period()
        if not d_from or not d_to:
            messagebox.showwarning("Excel", "Укажите период (Дата с/по).", parent=self)
            return
        if not self._daily_data:
            messagebox.showwarning("Excel", "Нет данных для выгрузки.", parent=self)
            return

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Отчеты питания"

            # Заголовок
            ws.append(["Сводный отчет по услугам питания"])
            ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=11)

            ws.append([f"Период: {d_from.strftime('%Y-%m-%d')} — {d_to.strftime('%Y-%m-%d')}"])
            ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=11)
            ws.append([])

            # ---- Верхняя таблица ----
            ws.append(["Отчет за день / период"])
            ws.merge_cells(start_row=4, start_column=1, end_row=4, end_column=11)

            header = ["Дата оказания услуги", "Наименование строительного объекта"]
            for cx in COMPLEX_ORDER:
                header += [f"{cx} Цена", f"{cx} Кол-во порций", f"{cx} Стоимость"]
            header += ["ИТОГО стоимость"]
            ws.append(header)

            for rec in self._daily_data:
                row = [rec["date"].strftime("%Y-%m-%d"), rec["address"]]
                for cx in COMPLEX_ORDER:
                    c = rec["complexes"][cx]
                    row += [c["price"], c["qty"], c["amount"]]
                row.append(rec["row_total_amount"])
                ws.append(row)

            ws.append([])

            # ---- Нижняя таблица ----
            ws.append(["Месячный отчет"])
            ws.merge_cells(start_row=ws.max_row, start_column=1, end_row=ws.max_row, end_column=11)

            header2 = ["Наименование строительного объекта"]
            for cx in COMPLEX_ORDER:
                header2 += [f"{cx} Цена", f"{cx} Кол-во порций", f"{cx} Стоимость"]
            header2 += ["ИТОГО стоимость"]
            ws.append(header2)

            for rec in self._monthly_data:
                row = [rec["address"]]
                for cx in COMPLEX_ORDER:
                    c = rec["complexes"][cx]
                    row += [c["price"], c["qty"], c["amount"]]
                row.append(rec["row_total_amount"])
                ws.append(row)

            # widths
            widths = [14, 45] + [10, 14, 14] * 3 + [14]
            for i, w in enumerate(widths, start=1):
                ws.column_dimensions[get_column_letter(i)].width = w

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"Отчет_питание_{d_from.strftime('%Y%m%d')}-{d_to.strftime('%Y%m%d')}_{ts}.xlsx"
            out_dir = os.path.join(exe_dir(), "Заявки_питание")
            os.makedirs(out_dir, exist_ok=True)
            fpath = os.path.join(out_dir, fname)

            wb.save(fpath)

            messagebox.showinfo("Excel", f"Файл сформирован:\n{fpath}", parent=self)
            try:
                os.startfile(fpath)
            except Exception:
                pass

        except Exception as e:
            messagebox.showerror("Excel", f"Ошибка формирования Excel:\n{e}", parent=self)


def create_meals_reports_page(parent, app_ref=None) -> tk.Frame:
    try:
        page = MealsReportsPage(parent, app_ref=app_ref)
        return page
    except Exception:
        import traceback
        messagebox.showerror("Питание — отчеты", traceback.format_exc(), parent=parent)
        return tk.Frame(parent)
