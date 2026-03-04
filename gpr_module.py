from __future__ import annotations

import sys
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values


# ------------------------- DB pool wiring -------------------------
db_connection_pool = None

def set_db_pool(pool):
    global db_connection_pool
    db_connection_pool = pool

def get_db_connection():
    if not db_connection_pool:
        raise RuntimeError("Пул соединений не был установлен (gpr_module.set_db_pool).")
    return db_connection_pool.getconn()

def release_db_connection(conn):
    if db_connection_pool and conn:
        db_connection_pool.putconn(conn)


# ------------------------- Utilities -------------------------

def _parse_date_ru(s: str) -> date:
    return datetime.strptime(s.strip(), "%d.%m.%Y").date()

def _format_date_ru(d: date) -> str:
    return d.strftime("%d.%m.%Y")

def _today() -> date:
    return datetime.now().date()

def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)

def _month_end(d: date) -> date:
    # простой способ: следующий месяц - 1 день
    if d.month == 12:
        return date(d.year, 12, 31)
    nm = date(d.year, d.month + 1, 1)
    return nm.fromordinal(nm.toordinal() - 1)


# ------------------------- DB service layer -------------------------

class GprService:
    """
    Минимальный сервис для ГПР v1:
    - текущий план на объект (gpr_plans.is_current=true)
    - задачи плана (gpr_tasks)
    - справочники: work_types, uom
    """

    # ---------- objects ----------
    @staticmethod
    def load_objects_short() -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, COALESCE(short_name,'') AS short_name, address
                    FROM public.objects
                    ORDER BY address
                """)
                return [dict(r) for r in cur.fetchall()]
        finally:
            release_db_connection(conn)

    # ---------- dictionaries ----------
    @staticmethod
    def load_work_types() -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, COALESCE(code,'') AS code, name
                    FROM public.gpr_work_types
                    WHERE is_active = true
                    ORDER BY sort_order, name
                """)
                return [dict(r) for r in cur.fetchall()]
        finally:
            release_db_connection(conn)

    @staticmethod
    def load_uoms() -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT code, name
                    FROM public.gpr_uom
                    ORDER BY code
                """)
                return [dict(r) for r in cur.fetchall()]
        finally:
            release_db_connection(conn)

    # ---------- plans ----------
    @staticmethod
    def get_or_create_current_plan(object_db_id: int, user_id: Optional[int]) -> int:
        conn = None
        try:
            conn = get_db_connection()
            with conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT id
                    FROM public.gpr_plans
                    WHERE object_db_id = %s AND is_current = true
                """, (object_db_id,))
                row = cur.fetchone()
                if row:
                    return int(row[0])

                cur.execute("""
                    INSERT INTO public.gpr_plans(object_db_id, version_no, is_current, is_baseline, created_by)
                    VALUES (%s, 1, true, false, %s)
                    RETURNING id
                """, (object_db_id, user_id))
                return int(cur.fetchone()[0])
        finally:
            release_db_connection(conn)

    # ---------- tasks ----------
    @staticmethod
    def load_plan_tasks(plan_id: int) -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        t.id,
                        t.parent_id,
                        t.work_type_id,
                        wt.name AS work_type_name,
                        t.name,
                        t.uom_code,
                        t.plan_qty,
                        t.plan_start,
                        t.plan_finish,
                        t.status,
                        t.sort_order,
                        t.is_milestone
                    FROM public.gpr_tasks t
                    JOIN public.gpr_work_types wt ON wt.id = t.work_type_id
                    WHERE t.plan_id = %s
                    ORDER BY t.sort_order, wt.sort_order, wt.name, t.name, t.plan_start, t.id
                """, (plan_id,))
                return [dict(r) for r in cur.fetchall()]
        finally:
            release_db_connection(conn)

    @staticmethod
    def replace_plan_tasks(plan_id: int, user_id: Optional[int], tasks: List[Dict[str, Any]]) -> None:
        """
        Полная замена задач плана. Для v1 это проще всего.
        Важно: parent_id пока не восстанавливаем (WBS можно добавить позже аккуратно).
        """
        conn = None
        try:
            conn = get_db_connection()
            with conn, conn.cursor() as cur:
                cur.execute("DELETE FROM public.gpr_tasks WHERE plan_id = %s", (plan_id,))

                if tasks:
                    values = []
                    for i, t in enumerate(tasks):
                        values.append((
                            plan_id,
                            int(t["work_type_id"]),
                            (t.get("name") or "").strip(),
                            t.get("uom_code") or None,
                            t.get("plan_qty"),
                            t["plan_start"],
                            t["plan_finish"],
                            (t.get("status") or "planned"),
                            int(t.get("sort_order") if t.get("sort_order") is not None else i),
                            bool(t.get("is_milestone") or False),
                            user_id,
                        ))

                    execute_values(cur, """
                        INSERT INTO public.gpr_tasks
                            (plan_id, work_type_id, name, uom_code, plan_qty, plan_start, plan_finish,
                             status, sort_order, is_milestone, created_by)
                        VALUES %s
                    """, values)

                cur.execute("UPDATE public.gpr_plans SET updated_at = now() WHERE id = %s", (plan_id,))
        finally:
            release_db_connection(conn)

    # ---------- templates ----------
    @staticmethod
    def load_templates() -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, name
                    FROM public.gpr_templates
                    WHERE is_active = true
                    ORDER BY name
                """)
                return [dict(r) for r in cur.fetchall()]
        finally:
            release_db_connection(conn)

    @staticmethod
    def load_template_tasks(template_id: int) -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        id, parent_id, work_type_id, name, uom_code, default_qty,
                        is_milestone, sort_order
                    FROM public.gpr_template_tasks
                    WHERE template_id = %s
                    ORDER BY sort_order, id
                """, (template_id,))
                return [dict(r) for r in cur.fetchall()]
        finally:
            release_db_connection(conn)


# ------------------------- Dialogs -------------------------

class DateRangeDialog(simpledialog.Dialog):
    def __init__(self, parent, init_from: date, init_to: date):
        self.init_from = init_from
        self.init_to = init_to
        self.result: Optional[Tuple[date, date]] = None
        super().__init__(parent, title="Диапазон дат")

    def body(self, master):
        tk.Label(master, text="С (дд.мм.гггг):").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=4)
        self.ent_from = ttk.Entry(master, width=14)
        self.ent_from.grid(row=0, column=1, sticky="w", pady=4)
        self.ent_from.insert(0, _format_date_ru(self.init_from))

        tk.Label(master, text="По (дд.мм.гггг):").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=4)
        self.ent_to = ttk.Entry(master, width=14)
        self.ent_to.grid(row=1, column=1, sticky="w", pady=4)
        self.ent_to.insert(0, _format_date_ru(self.init_to))

        tk.Label(master, text="Подсказка: формат 01.03.2026", fg="#777").grid(
            row=2, column=0, columnspan=2, sticky="w", pady=(6, 0)
        )
        return self.ent_from

    def validate(self):
        try:
            d1 = _parse_date_ru(self.ent_from.get())
            d2 = _parse_date_ru(self.ent_to.get())
            if d2 < d1:
                raise ValueError("Конечная дата меньше начальной")
            self._d1, self._d2 = d1, d2
            return True
        except Exception as e:
            messagebox.showwarning("Диапазон дат", f"Некорректные даты: {e}", parent=self)
            return False

    def apply(self):
        self.result = (self._d1, self._d2)


class TaskEditDialog(simpledialog.Dialog):
    def __init__(self, parent, work_types: List[Dict[str, Any]], uoms: List[Dict[str, Any]], init: Optional[Dict[str, Any]] = None):
        self.work_types = work_types
        self.uoms = uoms
        self.init = init or {}
        self.result: Optional[Dict[str, Any]] = None
        super().__init__(parent, title="Работа ГПР")

    def body(self, master):
        wt_vals = [f"{w['name']} (id={w['id']})" for w in self.work_types]
        uom_vals = [f"{u['code']} — {u['name']}" for u in self.uoms]

        tk.Label(master, text="Тип работ:").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=4)
        self.cmb_wt = ttk.Combobox(master, state="readonly", width=40, values=wt_vals)
        self.cmb_wt.grid(row=0, column=1, sticky="w", pady=4)

        tk.Label(master, text="Вид работ:").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=4)
        self.ent_name = ttk.Entry(master, width=44)
        self.ent_name.grid(row=1, column=1, sticky="w", pady=4)

        tk.Label(master, text="Ед. изм.:").grid(row=2, column=0, sticky="e", padx=(0, 6), pady=4)
        self.cmb_uom = ttk.Combobox(master, state="readonly", width=40, values=["(нет)"] + uom_vals)
        self.cmb_uom.grid(row=2, column=1, sticky="w", pady=4)

        tk.Label(master, text="Плановый объём:").grid(row=3, column=0, sticky="e", padx=(0, 6), pady=4)
        self.ent_qty = ttk.Entry(master, width=18)
        self.ent_qty.grid(row=3, column=1, sticky="w", pady=4)

        tk.Label(master, text="Начало (дд.мм.гггг):").grid(row=4, column=0, sticky="e", padx=(0, 6), pady=4)
        self.ent_start = ttk.Entry(master, width=14)
        self.ent_start.grid(row=4, column=1, sticky="w", pady=4)

        tk.Label(master, text="Окончание (дд.мм.гггг):").grid(row=5, column=0, sticky="e", padx=(0, 6), pady=4)
        self.ent_finish = ttk.Entry(master, width=14)
        self.ent_finish.grid(row=5, column=1, sticky="w", pady=4)

        self.var_milestone = tk.BooleanVar(value=bool(self.init.get("is_milestone", False)))
        ttk.Checkbutton(master, text="Веха", variable=self.var_milestone).grid(row=6, column=1, sticky="w", pady=(6, 0))

        # init values
        if self.work_types:
            init_wt_id = self.init.get("work_type_id")
            if init_wt_id:
                for i, w in enumerate(self.work_types):
                    if int(w["id"]) == int(init_wt_id):
                        self.cmb_wt.current(i)
                        break
            else:
                self.cmb_wt.current(0)

        self.ent_name.insert(0, self.init.get("name", ""))

        init_uom = self.init.get("uom_code")
        if init_uom:
            # +1 because "(нет)"
            for i, u in enumerate(self.uoms, start=1):
                if u["code"] == init_uom:
                    self.cmb_uom.current(i)
                    break
        else:
            self.cmb_uom.current(0)

        if self.init.get("plan_qty") is not None:
            self.ent_qty.insert(0, str(self.init.get("plan_qty")))

        d1 = self.init.get("plan_start") or _today()
        d2 = self.init.get("plan_finish") or _today()
        if isinstance(d1, str):
            d1 = datetime.fromisoformat(d1).date()
        if isinstance(d2, str):
            d2 = datetime.fromisoformat(d2).date()
        self.ent_start.insert(0, _format_date_ru(d1))
        self.ent_finish.insert(0, _format_date_ru(d2))

        return self.ent_name

    def validate(self):
        try:
            if self.cmb_wt.current() < 0:
                raise ValueError("Выберите тип работ")
            wt_id = int(self.work_types[self.cmb_wt.current()]["id"])

            name = (self.ent_name.get() or "").strip()
            if not name:
                raise ValueError("Введите вид работ")

            uom_code = None
            uom_idx = self.cmb_uom.current()
            if uom_idx > 0:
                uom_code = self.uoms[uom_idx - 1]["code"]

            qty_raw = (self.ent_qty.get() or "").strip().replace(",", ".")
            qty = None
            if qty_raw:
                qty = float(qty_raw)

            ds = _parse_date_ru(self.ent_start.get())
            df = _parse_date_ru(self.ent_finish.get())
            if df < ds:
                raise ValueError("Окончание раньше начала")

            self._out = {
                "work_type_id": wt_id,
                "name": name,
                "uom_code": uom_code,
                "plan_qty": qty,
                "plan_start": ds,
                "plan_finish": df,
                "status": "planned",
                "is_milestone": bool(self.var_milestone.get()),
            }
            return True
        except Exception as e:
            messagebox.showwarning("Работа ГПР", str(e), parent=self)
            return False

    def apply(self):
        self.result = dict(self._out)


# ------------------------- Gantt Canvas -------------------------

class GanttCanvas(tk.Frame):
    """
    Простой Canvas-Гант:
    - слева таблица (Treeview) в родителе
    - здесь: шкала дат + полосы
    - поддерживает горизонтальный/вертикальный скролл
    """
    def __init__(self, master, *, day_px: int = 18, row_h: int = 24):
        super().__init__(master)
        self.day_px = int(day_px)
        self.row_h = int(row_h)

        self.header = tk.Canvas(self, height=28, bg="#e8eaed", highlightthickness=0)
        self.body = tk.Canvas(self, bg="#ffffff", highlightthickness=0)

        self.vsb = ttk.Scrollbar(self, orient="vertical", command=self._yview)
        self.hsb = ttk.Scrollbar(self, orient="horizontal", command=self._xview)

        self.header.grid(row=0, column=0, sticky="ew")
        self.body.grid(row=1, column=0, sticky="nsew")
        self.vsb.grid(row=1, column=1, sticky="ns")
        self.hsb.grid(row=2, column=0, sticky="ew")

        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.body.configure(yscrollcommand=self.vsb.set)
        self.body.configure(xscrollcommand=self._on_body_xscroll)

        self._range: Tuple[date, date] = (_today(), _today())
        self._rows: List[Dict[str, Any]] = []

        self._colors = {
            "planned": "#90caf9",
            "in_progress": "#ffcc80",
            "done": "#a5d6a7",
            "paused": "#ffe082",
            "canceled": "#ef9a9a",
        }

        self.body.bind("<Configure>", lambda e: self.redraw())
        self.body.bind("<MouseWheel>", self._on_wheel)
        self.body.bind("<Shift-MouseWheel>", self._on_shift_wheel)

    def set_range(self, d_from: date, d_to: date):
        self._range = (d_from, d_to)
        self.redraw()

    def set_rows(self, rows: List[Dict[str, Any]]):
        self._rows = rows or []
        self.redraw()

    def sync_yview(self, fraction: float):
        """Синхронизация вертикали с Treeview (если нужно)."""
        try:
            self.body.yview_moveto(fraction)
        except Exception:
            pass

    def _yview(self, *args):
        self.body.yview(*args)

    def _xview(self, *args):
        self.body.xview(*args)

    def _on_body_xscroll(self, f1, f2):
        self.hsb.set(f1, f2)
        self.header.xview_moveto(f1)

    def _on_wheel(self, event):
        delta = -1 * (event.delta // 120) if event.delta else 0
        self.body.yview_scroll(delta, "units")
        return "break"

    def _on_shift_wheel(self, event):
        delta = -1 * (event.delta // 120) if event.delta else 0
        self.body.xview_scroll(delta, "units")
        return "break"

    def redraw(self):
        d0, d1 = self._range
        if d1 < d0:
            return

        days = (d1 - d0).days + 1
        total_w = max(1, days * self.day_px)
        total_h = max(1, len(self._rows) * self.row_h)

        self.header.delete("all")
        self.body.delete("all")

        self.header.configure(scrollregion=(0, 0, total_w, 28))
        self.body.configure(scrollregion=(0, 0, total_w, total_h))

        # header day numbers (простая шкала)
        for i in range(days):
            x0 = i * self.day_px
            x1 = x0 + self.day_px
            cur = d0.fromordinal(d0.toordinal() + i)
            fill = "#f3f4f6" if cur.weekday() < 5 else "#ffecec"
            self.header.create_rectangle(x0, 0, x1, 28, fill=fill, outline="#d0d0d0")
            if self.day_px >= 16:
                self.header.create_text((x0 + x1) / 2, 14, text=str(cur.day), fill="#333", font=("Segoe UI", 8))

        # body grid + bars
        for r, t in enumerate(self._rows):
            y0 = r * self.row_h
            y1 = y0 + self.row_h
            # zebra
            bg = "#ffffff" if r % 2 == 0 else "#fafafa"
            self.body.create_rectangle(0, y0, total_w, y1, fill=bg, outline="")

            # task bar
            ts: date = t.get("plan_start")
            tf: date = t.get("plan_finish")
            if not isinstance(ts, date) or not isinstance(tf, date):
                continue

            # clamp to range
            if tf < d0 or ts > d1:
                continue
            ts2 = max(ts, d0)
            tf2 = min(tf, d1)

            x0 = (ts2 - d0).days * self.day_px
            x1 = ((tf2 - d0).days + 1) * self.day_px

            status = (t.get("status") or "planned").strip()
            col = self._colors.get(status, "#90caf9")

            self.body.create_rectangle(x0 + 1, y0 + 4, x1 - 1, y1 - 4, fill=col, outline="#5f6368")
            # milestone marker
            if bool(t.get("is_milestone")):
                cx = x0 + 6
                cy = (y0 + y1) / 2
                self.body.create_polygon(cx, cy, cx + 8, cy - 6, cx + 16, cy, cx + 8, cy + 6, fill="#1a73e8", outline="")

        # grid vertical lines (редко, чтобы не тормозить)
        step = 7 if self.day_px >= 12 else 14
        for i in range(0, days, step):
            x = i * self.day_px
            self.body.create_line(x, 0, x, total_h, fill="#eeeeee")


# ------------------------- Main Page -------------------------

class GprPage(tk.Frame):
    def __init__(self, master, app_ref):
        super().__init__(master, bg="#f7f7f7")
        self.app_ref = app_ref

        self.objects: List[Dict[str, Any]] = []
        self.work_types: List[Dict[str, Any]] = []
        self.uoms: List[Dict[str, Any]] = []

        self.object_db_id: Optional[int] = None
        self.plan_id: Optional[int] = None

        self.range_from: date = _month_start(_today())
        self.range_to: date = _month_end(_today())

        self.tasks: List[Dict[str, Any]] = []

        self._build_ui()
        self._load_refs()
        self._refresh_objects_ui()

    # ---------- UI ----------
    def _build_ui(self):
        top = tk.Frame(self, bg="#ffffff")
        top.pack(fill="x", padx=10, pady=10)

        # object
        tk.Label(top, text="Объект:", bg="#ffffff").grid(row=0, column=0, sticky="e", padx=(0, 6))
        self.cmb_object = ttk.Combobox(top, state="readonly", width=60, values=[])
        self.cmb_object.grid(row=0, column=1, sticky="w")

        ttk.Button(top, text="Открыть", command=self._open_object).grid(row=0, column=2, padx=(10, 0))

        # range
        tk.Label(top, text="Диапазон:", bg="#ffffff").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=(8, 0))
        self.lbl_range = tk.Label(top, text="", bg="#ffffff", fg="#444")
        self.lbl_range.grid(row=1, column=1, sticky="w", pady=(8, 0))

        ttk.Button(top, text="Изменить…", command=self._change_range).grid(row=1, column=2, padx=(10, 0), pady=(8, 0))

        # toolbar
        bar = tk.Frame(self, bg="#f7f7f7")
        bar.pack(fill="x", padx=10, pady=(0, 6))

        ttk.Button(bar, text="➕ Добавить работу", command=self._add_task).pack(side="left")
        ttk.Button(bar, text="✏️ Редактировать", command=self._edit_selected).pack(side="left", padx=(6, 0))
        ttk.Button(bar, text="🗑 Удалить", command=self._delete_selected).pack(side="left", padx=(6, 0))
        ttk.Button(bar, text="📋 Из шаблона…", command=self._apply_template).pack(side="left", padx=(16, 0))

        ttk.Button(bar, text="💾 Сохранить", command=self._save).pack(side="right")

        # split: left tree + right gantt
        main = tk.PanedWindow(self, orient="horizontal", sashrelief="raised", bg="#f7f7f7")
        main.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        left = tk.Frame(main, bg="#ffffff")
        right = tk.Frame(main, bg="#ffffff")
        main.add(left, minsize=420)
        main.add(right, minsize=420)

        # Treeview tasks
        cols = ("type", "name", "start", "finish", "uom", "qty")
        self.tree = ttk.Treeview(left, columns=cols, show="headings", selectmode="browse")
        self.tree.heading("type", text="Тип работ")
        self.tree.heading("name", text="Вид работ")
        self.tree.heading("start", text="Начало")
        self.tree.heading("finish", text="Конец")
        self.tree.heading("uom", text="Ед.")
        self.tree.heading("qty", text="Объём")

        self.tree.column("type", width=140, anchor="w")
        self.tree.column("name", width=260, anchor="w")
        self.tree.column("start", width=90, anchor="center")
        self.tree.column("finish", width=90, anchor="center")
        self.tree.column("uom", width=60, anchor="center")
        self.tree.column("qty", width=80, anchor="e")

        vsb = ttk.Scrollbar(left, orient="vertical", command=self._on_tree_yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.bind("<Double-1>", lambda e: self._edit_selected())
        self.tree.bind("<Return>", lambda e: self._edit_selected())

        # Gantt
        self.gantt = GanttCanvas(right, day_px=18, row_h=24)
        self.gantt.pack(fill="both", expand=True)

        self._update_range_label()

    def _on_tree_yview(self, *args):
        # Treeview scroll
        self.tree.yview(*args)
        # sync gantt
        try:
            f = self.tree.yview()[0]
            self.gantt.sync_yview(float(f))
        except Exception:
            pass

    # ---------- Data loading ----------
    def _load_refs(self):
        try:
            self.objects = GprService.load_objects_short()
            self.work_types = GprService.load_work_types()
            self.uoms = GprService.load_uoms()
        except Exception as e:
            logging.exception("GPR: error loading refs")
            messagebox.showerror("ГПР", f"Не удалось загрузить справочники:\n{e}", parent=self)

    def _refresh_objects_ui(self):
        vals = []
        for o in self.objects:
            label = f"{o['address']}"
            if o.get("short_name"):
                label = f"{o['short_name']} — {o['address']}"
            vals.append(label)
        self.cmb_object.configure(values=vals)
        if vals:
            self.cmb_object.current(0)

    def _update_range_label(self):
        self.lbl_range.config(text=f"{_format_date_ru(self.range_from)} — {_format_date_ru(self.range_to)}")
        self.gantt.set_range(self.range_from, self.range_to)

    # ---------- Actions ----------
    def _selected_object_db_id(self) -> Optional[int]:
        idx = self.cmb_object.current()
        if idx < 0 or idx >= len(self.objects):
            return None
        return int(self.objects[idx]["id"])

    def _open_object(self):
        object_db_id = self._selected_object_db_id()
        if not object_db_id:
            messagebox.showwarning("ГПР", "Выберите объект.", parent=self)
            return

        self.object_db_id = object_db_id
        user_id = (self.app_ref.current_user or {}).get("id")

        try:
            self.plan_id = GprService.get_or_create_current_plan(object_db_id, user_id)
            self.tasks = GprService.load_plan_tasks(self.plan_id)
        except Exception as e:
            logging.exception("GPR: open object error")
            messagebox.showerror("ГПР", f"Не удалось открыть ГПР:\n{e}", parent=self)
            return

        self._render_tasks()

    def _render_tasks(self):
        self.tree.delete(*self.tree.get_children())

        for t in self.tasks:
            iid = str(t.get("id") or "")
            wt = t.get("work_type_name") or ""
            nm = t.get("name") or ""
            ds = t.get("plan_start")
            df = t.get("plan_finish")
            uom = t.get("uom_code") or ""
            qty = t.get("plan_qty")
            qty_str = ""
            if qty is not None:
                try:
                    qty_str = f"{float(qty):.3f}".rstrip("0").rstrip(".")
                except Exception:
                    qty_str = str(qty)

            self.tree.insert(
                "", "end", iid=iid if iid else None,
                values=(wt, nm, _format_date_ru(ds), _format_date_ru(df), uom, qty_str),
            )

        # gantt uses same order as tree
        self.gantt.set_rows(self.tasks)

    def _change_range(self):
        dlg = DateRangeDialog(self, self.range_from, self.range_to)
        if dlg.result:
            self.range_from, self.range_to = dlg.result
            self._update_range_label()
            self.gantt.set_rows(self.tasks)

    def _add_task(self):
        if not self.plan_id:
            messagebox.showinfo("ГПР", "Сначала выберите объект и нажмите «Открыть».", parent=self)
            return
        dlg = TaskEditDialog(self, self.work_types, self.uoms, init={
            "plan_start": self.range_from,
            "plan_finish": self.range_from,
        })
        if not dlg.result:
            return

        t = dlg.result
        # locally assign id None; will get real id after save
        t["id"] = None
        t["work_type_name"] = next((w["name"] for w in self.work_types if int(w["id"]) == int(t["work_type_id"])), "")
        t["sort_order"] = len(self.tasks) * 10
        self.tasks.append(t)
        self._render_tasks()

    def _selected_task_index(self) -> Optional[int]:
        sel = self.tree.selection()
        if not sel:
            return None
        iid = sel[0]
        # try by id first
        try:
            tid = int(iid)
            for i, t in enumerate(self.tasks):
                if t.get("id") and int(t["id"]) == tid:
                    return i
        except Exception:
            pass
        # fallback: by tree index
        try:
            return self.tree.index(iid)
        except Exception:
            return None

    def _edit_selected(self):
        idx = self._selected_task_index()
        if idx is None:
            return
        t0 = self.tasks[idx]
        dlg = TaskEditDialog(self, self.work_types, self.uoms, init=t0)
        if not dlg.result:
            return
        upd = dlg.result
        # keep id
        upd["id"] = t0.get("id")
        upd["sort_order"] = t0.get("sort_order", idx * 10)
        upd["work_type_name"] = next((w["name"] for w in self.work_types if int(w["id"]) == int(upd["work_type_id"])), "")
        self.tasks[idx] = upd
        self._render_tasks()

    def _delete_selected(self):
        idx = self._selected_task_index()
        if idx is None:
            return
        if not messagebox.askyesno("ГПР", "Удалить выбранную работу?", parent=self):
            return
        self.tasks.pop(idx)
        self._render_tasks()

    def _apply_template(self):
        if not self.plan_id:
            messagebox.showinfo("ГПР", "Сначала выберите объект и нажмите «Открыть».", parent=self)
            return

        try:
            templates = GprService.load_templates()
        except Exception as e:
            messagebox.showerror("ГПР", f"Не удалось загрузить шаблоны:\n{e}", parent=self)
            return

        if not templates:
            messagebox.showinfo("ГПР", "Шаблонов нет (таблица gpr_templates пустая).", parent=self)
            return

        # simple choice
        choices = [f"{t['name']} (id={t['id']})" for t in templates]
        choice = simpledialog.askstring("Шаблон", "Введите id шаблона или выберите из списка:\n\n" + "\n".join(choices[:20]), parent=self)
        if not choice:
            return

        try:
            # extract id
            tid = int("".join(ch for ch in choice if ch.isdigit()))
        except Exception:
            messagebox.showwarning("Шаблон", "Не удалось распознать id шаблона.", parent=self)
            return

        try:
            tmpl_tasks = GprService.load_template_tasks(tid)
        except Exception as e:
            messagebox.showerror("ГПР", f"Не удалось загрузить задачи шаблона:\n{e}", parent=self)
            return

        if not tmpl_tasks:
            messagebox.showinfo("ГПР", "В выбранном шаблоне нет задач.", parent=self)
            return

        if self.tasks and not messagebox.askyesno("ГПР", "Заменить текущие работы на работы из шаблона?", parent=self):
            return

        # convert template tasks to plan tasks with dates inside current range start (simple)
        base = self.range_from
        out = []
        for i, tt in enumerate(tmpl_tasks):
            wt_id = int(tt["work_type_id"])
            wt_name = next((w["name"] for w in self.work_types if int(w["id"]) == wt_id), "")

            out.append({
                "id": None,
                "work_type_id": wt_id,
                "work_type_name": wt_name,
                "name": tt["name"],
                "uom_code": tt.get("uom_code"),
                "plan_qty": tt.get("default_qty"),
                "plan_start": base,
                "plan_finish": base,
                "status": "planned",
                "is_milestone": bool(tt.get("is_milestone") or False),
                "sort_order": int(tt.get("sort_order") if tt.get("sort_order") is not None else i * 10),
            })

        self.tasks = out
        self._render_tasks()

    def _save(self):
        if not self.plan_id:
            messagebox.showinfo("ГПР", "Сначала выберите объект и нажмите «Открыть».", parent=self)
            return

        # quick validation
        for t in self.tasks:
            if not t.get("name"):
                messagebox.showwarning("ГПР", "Есть работа без названия.", parent=self)
                return
            if not t.get("work_type_id"):
                messagebox.showwarning("ГПР", "Есть работа без типа.", parent=self)
                return
            ds = t.get("plan_start")
            df = t.get("plan_finish")
            if not isinstance(ds, date) or not isinstance(df, date) or df < ds:
                messagebox.showwarning("ГПР", f"Некорректные даты у работы: {t.get('name')}", parent=self)
                return

        user_id = (self.app_ref.current_user or {}).get("id")
        try:
            GprService.replace_plan_tasks(self.plan_id, user_id, self.tasks)
            # reload from db to get ids/order
            self.tasks = GprService.load_plan_tasks(self.plan_id)
            self._render_tasks()
            messagebox.showinfo("ГПР", "Сохранено.", parent=self)
        except Exception as e:
            logging.exception("GPR save error")
            messagebox.showerror("ГПР", f"Ошибка сохранения:\n{e}", parent=self)


# ------------------------- API for main_app -------------------------

def create_gpr_page(parent, app_ref) -> GprPage:
    return GprPage(parent, app_ref=app_ref)
