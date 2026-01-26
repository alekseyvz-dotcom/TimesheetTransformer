import os
from datetime import datetime, date
from typing import Optional, Dict, Any, List, Tuple

import tkinter as tk
from tkinter import ttk, messagebox

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from psycopg2.extras import RealDictCursor


# ---------------- DB pool wiring ----------------

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
    import sys
    from pathlib import Path
    if getattr(sys, "frozen", False):
        return str(Path(sys.executable).resolve().parent)
    return str(Path(__file__).resolve().parent)

def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


# ---------------- Business constants ----------------

# Соответствие "Комплекс 1/2/3" -> названия типов питания в БД
COMPLEX_MAP = {
    "Комплекс 1": _norm("одноразовое питание"),
    "Комплекс 2": _norm("двухразовое питание"),
    "Комплекс 3": _norm("трехразовое питание"),
}
COMPLEX_ORDER = ["Комплекс 1", "Комплекс 2", "Комплекс 3"]


# ---------------- Data aggregation ----------------

def _load_price_map(conn) -> Dict[str, float]:
    """map: normalized meal_types.name -> price"""
    with conn.cursor() as cur:
        cur.execute("SELECT name, COALESCE(price, 0) FROM meal_types")
        res: Dict[str, float] = {}
        for name, price in cur.fetchall():
            res[_norm(name)] = float(price or 0)
        return res

def _complex_by_meal_type_name(meal_type_name: str) -> Optional[str]:
    mt = _norm(meal_type_name)
    if "однораз" in mt:
        return "Комплекс 1"
    if "двухраз" in mt:
        return "Комплекс 2"
    if "трехраз" in mt or "трёхраз" in mt:
        return "Комплекс 3"
    return None

def _fetch_items_for_period(conn, date_from: date, date_to: date) -> List[Dict[str, Any]]:
    """
    Возвращает строки: service_date, address, meal_type_name, qty.
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

def build_daily_rows(date_from: date, date_to: date) -> List[Dict[str, Any]]:
    """
    Строки для ежедневного отчета: (date, address) + 3 комплекса.
    """
    conn = None
    try:
        conn = get_db_connection()
        price_map = _load_price_map(conn)
        items = _fetch_items_for_period(conn, date_from, date_to)

        # (date, address) -> complex -> qty
        agg: Dict[Tuple[date, str], Dict[str, float]] = {}

        for it in items:
            d = it["service_date"]
            addr = (it.get("object_address") or "").strip() or "(без адреса)"
            cx = _complex_by_meal_type_name(it.get("meal_type_name") or "")
            if not cx:
                continue
            qty = float(it.get("qty") or 0)

            m = agg.setdefault((d, addr), {c: 0.0 for c in COMPLEX_ORDER})
            m[cx] += qty

        out: List[Dict[str, Any]] = []
        for (d, addr), cx_qty in sorted(agg.items(), key=lambda x: (x[0][0], x[0][1])):
            rec: Dict[str, Any] = {"date": d, "address": addr, "complexes": {}, "total_amount": 0.0}
            total = 0.0
            for cx in COMPLEX_ORDER:
                mt_norm = COMPLEX_MAP[cx]
                price = float(price_map.get(mt_norm, 0.0))
                qty = float(cx_qty.get(cx, 0.0))
                amount = price * qty
                rec["complexes"][cx] = {"price": price, "qty": qty, "amount": amount}
                total += amount
            rec["total_amount"] = total
            out.append(rec)

        return out
    finally:
        if conn:
            release_db_connection(conn)

def build_monthly_rows(year: int, month: int) -> List[Dict[str, Any]]:
    """
    Строки для месячного отчета: (address) + 3 комплекса за месяц.
    """
    if month < 1 or month > 12:
        raise ValueError("month must be 1..12")

    date_from = date(year, month, 1)
    if month == 12:
        date_to_excl = date(year + 1, 1, 1)
    else:
        date_to_excl = date(year, month + 1, 1)

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
                (date_from, date_to_excl),
            )
            items = [dict(r) for r in cur.fetchall()]

        # address -> complex -> qty
        agg: Dict[str, Dict[str, float]] = {}

        for it in items:
            addr = (it.get("object_address") or "").strip() or "(без адреса)"
            cx = _complex_by_meal_type_name(it.get("meal_type_name") or "")
            if not cx:
                continue
            qty = float(it.get("qty") or 0)
            m = agg.setdefault(addr, {c: 0.0 for c in COMPLEX_ORDER})
            m[cx] += qty

        out: List[Dict[str, Any]] = []
        for addr, cx_qty in sorted(agg.items(), key=lambda x: x[0]):
            rec: Dict[str, Any] = {"address": addr, "complexes": {}, "total_amount": 0.0}
            total = 0.0
            for cx in COMPLEX_ORDER:
                mt_norm = COMPLEX_MAP[cx]
                price = float(price_map.get(mt_norm, 0.0))
                qty = float(cx_qty.get(cx, 0.0))
                amount = price * qty
                rec["complexes"][cx] = {"price": price, "qty": qty, "amount": amount}
                total += amount
            rec["total_amount"] = total
            out.append(rec)

        return out
    finally:
        if conn:
            release_db_connection(conn)


# ---------------- Excel export ----------------

def export_daily_excel(date_from: date, date_to: date, out_path: str):
    rows = build_daily_rows(date_from, date_to)

    wb = Workbook()
    ws = wb.active
    ws.title = "Ежедневный отчет"

    ws.append(["Ежедневный отчет по питанию"])
    ws.append([f"Период: {date_from.strftime('%Y-%m-%d')} — {date_to.strftime('%Y-%m-%d')}"])
    ws.append([])

    header = ["Дата оказания услуги", "Наименование строительного объекта"]
    for cx in COMPLEX_ORDER:
        header += [f"{cx} Цена", f"{cx} Кол-во порций", f"{cx} Стоимость"]
    header += ["ИТОГО стоимость"]
    ws.append(header)

    totals = {cx: 0.0 for cx in COMPLEX_ORDER}
    grand_total = 0.0

    for r in rows:
        row = [r["date"].strftime("%Y-%m-%d"), r["address"]]
        for cx in COMPLEX_ORDER:
            c = r["complexes"][cx]
            row += [c["price"], c["qty"], c["amount"]]
            totals[cx] += float(c["amount"] or 0)
            grand_total += float(c["amount"] or 0)
        row.append(r["total_amount"])
        ws.append(row)

    # ИТОГО строка
    if rows:
        total_row = ["ИТОГО", ""]
        for cx in COMPLEX_ORDER:
            total_row += ["", "", totals[cx]]
        total_row += [grand_total]
        ws.append(total_row)

    widths = [14, 45] + [10, 14, 14] * 3 + [14]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w

    wb.save(out_path)

def export_monthly_excel(year: int, month: int, out_path: str):
    rows = build_monthly_rows(year, month)

    wb = Workbook()
    ws = wb.active
    ws.title = "Месячный отчет"

    ws.append(["Месячный отчет по питанию"])
    ws.append([f"Месяц: {year:04d}-{month:02d}"])
    ws.append([])

    header = ["Наименование строительного объекта"]
    for cx in COMPLEX_ORDER:
        header += [f"{cx} Цена", f"{cx} Кол-во порций", f"{cx} Стоимость"]
    header += ["ИТОГО стоимость"]
    ws.append(header)

    totals = {cx: 0.0 for cx in COMPLEX_ORDER}
    grand_total = 0.0

    for r in rows:
        row = [r["address"]]
        for cx in COMPLEX_ORDER:
            c = r["complexes"][cx]
            row += [c["price"], c["qty"], c["amount"]]
            totals[cx] += float(c["amount"] or 0)
            grand_total += float(c["amount"] or 0)
        row.append(r["total_amount"])
        ws.append(row)

    if rows:
        total_row = ["ИТОГО"]
        for cx in COMPLEX_ORDER:
            total_row += ["", "", totals[cx]]
        total_row += [grand_total]
        ws.append(total_row)

    widths = [55] + [10, 14, 14] * 3 + [14]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w

    wb.save(out_path)


# ---------------- UI Page (no tables) ----------------

class MealsReportsPage(tk.Frame):
    def __init__(self, master, app_ref=None):
        super().__init__(master, bg="#f7f7f7")
        self.app_ref = app_ref

        self.var_report_kind = tk.StringVar(value="daily")  # daily | monthly
        self.var_month = tk.StringVar(value=date.today().strftime("%Y-%m"))
        self._build_ui()

    def _build_ui(self):
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(fill="x", padx=12, pady=12)

        tk.Label(top, text="Тип отчета:", bg="#f7f7f7").grid(row=0, column=0, sticky="w")

        rb1 = ttk.Radiobutton(top, text="Ежедневный (за период дат)", value="daily",
                              variable=self.var_report_kind, command=self._update_mode)
        rb2 = ttk.Radiobutton(top, text="Месячный", value="monthly",
                              variable=self.var_report_kind, command=self._update_mode)
        rb1.grid(row=0, column=1, sticky="w", padx=(8, 10))
        rb2.grid(row=0, column=2, sticky="w", padx=(0, 10))

        # ---- daily controls ----
        self.frm_daily = tk.Frame(top, bg="#f7f7f7")
        self.frm_daily.grid(row=1, column=0, columnspan=6, sticky="w", pady=(10, 0))

        tk.Label(self.frm_daily, text="Дата с:", bg="#f7f7f7").grid(row=0, column=0, sticky="w")
        self.ent_from = ttk.Entry(self.frm_daily, width=12)
        self.ent_from.grid(row=0, column=1, sticky="w", padx=(6, 10))
        tk.Label(self.frm_daily, text="по:", bg="#f7f7f7").grid(row=0, column=2, sticky="w")
        self.ent_to = ttk.Entry(self.frm_daily, width=12)
        self.ent_to.grid(row=0, column=3, sticky="w", padx=(6, 10))

        today = date.today()
        self.ent_from.insert(0, today.strftime("%Y-%m-%d"))
        self.ent_to.insert(0, today.strftime("%Y-%m-%d"))

        # ---- monthly controls ----
        self.frm_monthly = tk.Frame(top, bg="#f7f7f7")
        self.frm_monthly.grid(row=2, column=0, columnspan=6, sticky="w", pady=(10, 0))

        tk.Label(self.frm_monthly, text="Месяц (YYYY-MM):", bg="#f7f7f7").grid(row=0, column=0, sticky="w")
        self.ent_month = ttk.Entry(self.frm_monthly, textvariable=self.var_month, width=12)
        self.ent_month.grid(row=0, column=1, sticky="w", padx=(6, 10))

        # кнопки
        btns = tk.Frame(self, bg="#f7f7f7")
        btns.pack(fill="x", padx=12, pady=(0, 12))

        ttk.Button(btns, text="Выгрузить в Excel", command=self._on_export).pack(side="left")

        self._update_mode()

    def _update_mode(self):
        kind = self.var_report_kind.get()
        if kind == "daily":
            self.frm_daily.grid()
            self.frm_monthly.grid_remove()
        else:
            self.frm_monthly.grid()
            self.frm_daily.grid_remove()

    def _on_export(self):
        kind = self.var_report_kind.get()

        out_dir = os.path.join(exe_dir(), "Заявки_питание")
        os.makedirs(out_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        try:
            if kind == "daily":
                d_from = parse_date_any(self.ent_from.get())
                d_to = parse_date_any(self.ent_to.get())
                if not d_from or not d_to:
                    messagebox.showwarning("Отчеты", "Укажите даты 'с' и 'по'.", parent=self)
                    return
                if d_from > d_to:
                    messagebox.showwarning("Отчеты", "Дата 'с' больше даты 'по'.", parent=self)
                    return

                out_path = os.path.join(out_dir, f"Питание_ежедневный_{d_from:%Y%m%d}-{d_to:%Y%m%d}_{ts}.xlsx")
                export_daily_excel(d_from, d_to, out_path)

                messagebox.showinfo("Отчеты", f"Файл сформирован:\n{out_path}", parent=self)
                try:
                    os.startfile(out_path)
                except Exception:
                    pass

            else:
                m = (self.var_month.get() or "").strip()
                # ожидаем YYYY-MM
                try:
                    dt = datetime.strptime(m, "%Y-%m")
                    year, month = dt.year, dt.month
                except Exception:
                    messagebox.showwarning("Отчеты", "Месяц должен быть в формате YYYY-MM, например 2026-01.", parent=self)
                    return

                out_path = os.path.join(out_dir, f"Питание_месячный_{year:04d}{month:02d}_{ts}.xlsx")
                export_monthly_excel(year, month, out_path)

                messagebox.showinfo("Отчеты", f"Файл сформирован:\n{out_path}", parent=self)
                try:
                    os.startfile(out_path)
                except Exception:
                    pass

        except Exception as e:
            messagebox.showerror("Отчеты", f"Не удалось сформировать отчет:\n{e}", parent=self)


def create_meals_reports_page(parent, app_ref=None) -> tk.Frame:
    try:
        return MealsReportsPage(parent, app_ref=app_ref)
    except Exception:
        import traceback
        messagebox.showerror("Питание — отчеты", traceback.format_exc(), parent=parent)
        return tk.Frame(parent)
