# gpr_module.py  — профессиональный модуль ГПР v2
from __future__ import annotations

import sys
import logging
import calendar
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set
from pathlib import Path

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

try:
    from openpyxl import Workbook
    from openpyxl.utils import get_column_letter
    from openpyxl.styles import Font, PatternFill, Alignment
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False


# ═══════════════════════════════════════════════════════════════
#  COLORS / THEME
# ═══════════════════════════════════════════════════════════════
C = {
    "bg":           "#f0f2f5",
    "panel":        "#ffffff",
    "accent":       "#1565c0",
    "accent_light": "#e3f2fd",
    "success":      "#2e7d32",
    "warning":      "#ed6c02",
    "error":        "#d32f2f",
    "border":       "#dde1e7",
    "text":         "#1a1a2e",
    "text2":        "#555",
    "text3":        "#999",
    "btn_bg":       "#1565c0",
    "btn_fg":       "#ffffff",
}

STATUS_COLORS = {
    "planned":     ("#90caf9", "#1565c0", "Запланировано"),
    "in_progress": ("#ffcc80", "#e65100", "В работе"),
    "done":        ("#a5d6a7", "#1b5e20", "Выполнено"),
    "paused":      ("#fff176", "#f9a825", "Приостановлено"),
    "canceled":    ("#ef9a9a", "#b71c1c", "Отменено"),
}

STATUS_LIST = ["planned", "in_progress", "done", "paused", "canceled"]
STATUS_LABELS = {k: v[2] for k, v in STATUS_COLORS.items()}


# ═══════════════════════════════════════════════════════════════
#  DB POOL
# ═══════════════════════════════════════════════════════════════
db_connection_pool = None

def set_db_pool(pool):
    global db_connection_pool
    db_connection_pool = pool

def _conn():
    if not db_connection_pool:
        raise RuntimeError("DB pool not set (gpr_module.set_db_pool)")
    return db_connection_pool.getconn()

def _release(conn):
    if db_connection_pool and conn:
        db_connection_pool.putconn(conn)


# ═══════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════
def _parse_date(s: str) -> date:
    return datetime.strptime(s.strip(), "%d.%m.%Y").date()

def _fmt_date(d) -> str:
    if isinstance(d, date):
        return d.strftime("%d.%m.%Y")
    return str(d or "")

def _today() -> date:
    return datetime.now().date()

def _quarter_range() -> Tuple[date, date]:
    t = _today()
    q_start_month = ((t.month - 1) // 3) * 3 + 1
    d0 = date(t.year, q_start_month, 1)
    end_month = q_start_month + 2
    d1 = date(t.year, end_month, calendar.monthrange(t.year, end_month)[1])
    return d0, d1

def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return None

def _fmt_qty(v) -> str:
    f = _safe_float(v)
    if f is None:
        return ""
    return f"{f:.3f}".rstrip("0").rstrip(".")


# ═══════════════════════════════════════════════════════════════
#  SERVICE LAYER
# ═══════════════════════════════════════════════════════════════
class GprService:

    # ── objects ──
    @staticmethod
    def load_objects_short() -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id,
                           COALESCE(short_name,'') AS short_name,
                           address,
                           COALESCE(excel_id,'') AS excel_id,
                           COALESCE(status,'') AS status
                    FROM public.objects
                    ORDER BY address, short_name
                """)
                return [dict(r) for r in cur.fetchall()]
        finally:
            _release(conn)

    # ── dictionaries ──
    @staticmethod
    def load_work_types() -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, COALESCE(code,'') AS code, name
                    FROM public.gpr_work_types WHERE is_active=true
                    ORDER BY sort_order, name
                """)
                return [dict(r) for r in cur.fetchall()]
        finally:
            _release(conn)

    @staticmethod
    def load_uoms() -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT code, name FROM public.gpr_uom ORDER BY code")
                return [dict(r) for r in cur.fetchall()]
        finally:
            _release(conn)

    @staticmethod
    def load_statuses() -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT code, name FROM public.gpr_statuses ORDER BY code")
                return [dict(r) for r in cur.fetchall()]
        finally:
            _release(conn)

    # ── plans ──
    @staticmethod
    def get_or_create_current_plan(object_db_id: int, user_id: Optional[int]) -> Dict[str, Any]:
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT p.*, u.full_name AS creator_name
                    FROM public.gpr_plans p
                    LEFT JOIN public.app_users u ON u.id = p.created_by
                    WHERE p.object_db_id=%s AND p.is_current=true
                """, (object_db_id,))
                row = cur.fetchone()
                if row:
                    return dict(row)

                cur.execute("""
                    INSERT INTO public.gpr_plans(object_db_id, version_no, is_current, is_baseline, created_by)
                    VALUES (%s, 1, true, false, %s)
                    RETURNING id
                """, (object_db_id, user_id))
                pid = cur.fetchone()["id"]

                cur.execute("""
                    SELECT p.*, u.full_name AS creator_name
                    FROM public.gpr_plans p
                    LEFT JOIN public.app_users u ON u.id = p.created_by
                    WHERE p.id=%s
                """, (pid,))
                return dict(cur.fetchone())
        finally:
            _release(conn)

    # ── tasks ──
    @staticmethod
    def load_plan_tasks(plan_id: int) -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT t.id, t.parent_id, t.work_type_id,
                           wt.name AS work_type_name,
                           t.name, t.uom_code, t.plan_qty,
                           t.plan_start, t.plan_finish,
                           t.status, t.sort_order, t.is_milestone,
                           t.created_by, t.created_at, t.updated_at
                    FROM public.gpr_tasks t
                    JOIN public.gpr_work_types wt ON wt.id = t.work_type_id
                    WHERE t.plan_id = %s
                    ORDER BY t.sort_order, wt.sort_order, wt.name,
                             t.name, t.plan_start, t.id
                """, (plan_id,))
                return [dict(r) for r in cur.fetchall()]
        finally:
            _release(conn)

    @staticmethod
    def load_task_facts_cumulative(task_ids: List[int]) -> Dict[int, float]:
        if not task_ids:
            return {}
        conn = None
        try:
            conn = _conn()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT task_id, SUM(fact_qty) AS total
                    FROM public.gpr_task_facts
                    WHERE task_id = ANY(%s)
                    GROUP BY task_id
                """, (task_ids,))
                return {r[0]: float(r[1]) for r in cur.fetchall()}
        finally:
            _release(conn)

    @staticmethod
    def replace_plan_tasks(plan_id: int, user_id: Optional[int],
                           tasks: List[Dict[str, Any]]) -> None:
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("DELETE FROM public.gpr_tasks WHERE plan_id=%s",
                            (plan_id,))
                if tasks:
                    vals = []
                    for i, t in enumerate(tasks):
                        vals.append((
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
                        (plan_id,work_type_id,name,uom_code,plan_qty,
                         plan_start,plan_finish,status,sort_order,is_milestone,created_by)
                        VALUES %s
                    """, vals)
                cur.execute(
                    "UPDATE public.gpr_plans SET updated_at=now() WHERE id=%s",
                    (plan_id,))
        finally:
            _release(conn)

    @staticmethod
    def update_task_status(task_id: int, new_status: str) -> None:
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.gpr_tasks SET status=%s, updated_at=now() WHERE id=%s",
                    (new_status, task_id))
        finally:
            _release(conn)

    # ── templates ──
    @staticmethod
    def load_templates() -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, name FROM public.gpr_templates
                    WHERE is_active=true ORDER BY name
                """)
                return [dict(r) for r in cur.fetchall()]
        finally:
            _release(conn)

    @staticmethod
    def load_template_tasks(template_id: int) -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, parent_id, work_type_id, name, uom_code,
                           default_qty, is_milestone, sort_order
                    FROM public.gpr_template_tasks
                    WHERE template_id=%s ORDER BY sort_order, id
                """, (template_id,))
                return [dict(r) for r in cur.fetchall()]
        finally:
            _release(conn)


# ═══════════════════════════════════════════════════════════════
#  AUTOCOMPLETE COMBOBOX
# ═══════════════════════════════════════════════════════════════
class _AutoCombo(ttk.Combobox):
    def __init__(self, master=None, **kw):
        super().__init__(master, **kw)
        self._all: List[str] = []
        self.bind("<KeyRelease>", self._filter)

    def set_values(self, vals: List[str]):
        self._all = list(vals or [])
        self.config(values=self._all)

    def _filter(self, _e=None):
        q = self.get().strip().lower()
        if not q:
            self.config(values=self._all)
            return
        f = [v for v in self._all if q in v.lower()]
        self.config(values=f)


# ═══════════════════════════════════════════════════════════════
#  DIALOGS
# ═══════════════════════════════════════════════════════════════
class DateRangeDialog(simpledialog.Dialog):
    def __init__(self, parent, d0: date, d1: date):
        self._d0, self._d1 = d0, d1
        self.result: Optional[Tuple[date, date]] = None
        super().__init__(parent, title="Диапазон дат отображения")

    def body(self, m):
        tk.Label(m, text="С (дд.мм.гггг):").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=4)
        self.e0 = ttk.Entry(m, width=14); self.e0.grid(row=0, column=1, pady=4)
        self.e0.insert(0, _fmt_date(self._d0))

        tk.Label(m, text="По (дд.мм.гггг):").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=4)
        self.e1 = ttk.Entry(m, width=14); self.e1.grid(row=1, column=1, pady=4)
        self.e1.insert(0, _fmt_date(self._d1))

        ttk.Button(m, text="Текущий квартал", command=self._set_quarter).grid(
            row=2, column=0, columnspan=2, pady=(8, 0))
        return self.e0

    def _set_quarter(self):
        d0, d1 = _quarter_range()
        self.e0.delete(0, "end"); self.e0.insert(0, _fmt_date(d0))
        self.e1.delete(0, "end"); self.e1.insert(0, _fmt_date(d1))

    def validate(self):
        try:
            a, b = _parse_date(self.e0.get()), _parse_date(self.e1.get())
            if b < a: raise ValueError("end < start")
            self._a, self._b = a, b; return True
        except Exception as e:
            messagebox.showwarning("Даты", str(e), parent=self); return False

    def apply(self):
        self.result = (self._a, self._b)


class TaskEditDialog(simpledialog.Dialog):
    def __init__(self, parent, wt, uoms, statuses_db=None, init=None):
        self.wt = wt; self.uoms = uoms; self.init = init or {}
        self._statuses_db = statuses_db or []
        self.result: Optional[Dict[str, Any]] = None
        super().__init__(parent, title="Работа ГПР")

    def body(self, m):
        wt_v = [w["name"] for w in self.wt]
        uom_v = [f"{u['code']} — {u['name']}" for u in self.uoms]
        st_v = [STATUS_LABELS.get(s, s) for s in STATUS_LIST]

        r = 0
        tk.Label(m, text="Тип работ *:").grid(row=r, column=0, sticky="e", padx=(0, 6), pady=3)
        self.cmb_wt = ttk.Combobox(m, state="readonly", width=42, values=wt_v)
        self.cmb_wt.grid(row=r, column=1, pady=3); r += 1

        tk.Label(m, text="Вид работ *:").grid(row=r, column=0, sticky="e", padx=(0, 6), pady=3)
        self.ent_name = ttk.Entry(m, width=46)
        self.ent_name.grid(row=r, column=1, pady=3); r += 1

        tk.Label(m, text="Ед. изм.:").grid(row=r, column=0, sticky="e", padx=(0, 6), pady=3)
        self.cmb_uom = ttk.Combobox(m, state="readonly", width=42, values=["—"] + uom_v)
        self.cmb_uom.grid(row=r, column=1, pady=3); r += 1

        tk.Label(m, text="Объём план:").grid(row=r, column=0, sticky="e", padx=(0, 6), pady=3)
        self.ent_qty = ttk.Entry(m, width=18)
        self.ent_qty.grid(row=r, column=1, sticky="w", pady=3); r += 1

        tk.Label(m, text="Начало *:").grid(row=r, column=0, sticky="e", padx=(0, 6), pady=3)
        self.ent_s = ttk.Entry(m, width=14)
        self.ent_s.grid(row=r, column=1, sticky="w", pady=3); r += 1

        tk.Label(m, text="Окончание *:").grid(row=r, column=0, sticky="e", padx=(0, 6), pady=3)
        self.ent_f = ttk.Entry(m, width=14)
        self.ent_f.grid(row=r, column=1, sticky="w", pady=3); r += 1

        tk.Label(m, text="Статус:").grid(row=r, column=0, sticky="e", padx=(0, 6), pady=3)
        self.cmb_st = ttk.Combobox(m, state="readonly", width=20, values=st_v)
        self.cmb_st.grid(row=r, column=1, sticky="w", pady=3); r += 1

        self.var_ms = tk.BooleanVar(value=bool(self.init.get("is_milestone")))
        ttk.Checkbutton(m, text="Веха (milestone)", variable=self.var_ms).grid(
            row=r, column=1, sticky="w", pady=3)

        # fill init
        iw = self.init.get("work_type_id")
        if iw:
            for i, w in enumerate(self.wt):
                if int(w["id"]) == int(iw): self.cmb_wt.current(i); break
        elif self.wt:
            self.cmb_wt.current(0)

        self.ent_name.insert(0, self.init.get("name", ""))

        iu = self.init.get("uom_code")
        if iu:
            for i, u in enumerate(self.uoms, 1):
                if u["code"] == iu: self.cmb_uom.current(i); break
        else:
            self.cmb_uom.current(0)

        if self.init.get("plan_qty") is not None:
            self.ent_qty.insert(0, _fmt_qty(self.init["plan_qty"]))

        d0 = self.init.get("plan_start") or _today()
        d1 = self.init.get("plan_finish") or _today()
        if isinstance(d0, str): d0 = datetime.fromisoformat(d0).date()
        if isinstance(d1, str): d1 = datetime.fromisoformat(d1).date()
        self.ent_s.insert(0, _fmt_date(d0))
        self.ent_f.insert(0, _fmt_date(d1))

        ist = self.init.get("status", "planned")
        try:
            self.cmb_st.current(STATUS_LIST.index(ist))
        except Exception:
            self.cmb_st.current(0)

        return self.ent_name

    def validate(self):
        try:
            wi = self.cmb_wt.current()
            if wi < 0: raise ValueError("Выберите тип работ")
            wt_id = int(self.wt[wi]["id"])
            nm = (self.ent_name.get() or "").strip()
            if not nm: raise ValueError("Введите вид работ")

            uom = None
            ui = self.cmb_uom.current()
            if ui > 0: uom = self.uoms[ui - 1]["code"]

            qty = _safe_float(self.ent_qty.get())
            ds = _parse_date(self.ent_s.get())
            df = _parse_date(self.ent_f.get())
            if df < ds: raise ValueError("Окончание раньше начала")

            si = self.cmb_st.current()
            st = STATUS_LIST[si] if si >= 0 else "planned"

            self._out = dict(
                work_type_id=wt_id, name=nm, uom_code=uom, plan_qty=qty,
                plan_start=ds, plan_finish=df, status=st,
                is_milestone=bool(self.var_ms.get()),
            )
            return True
        except Exception as e:
            messagebox.showwarning("Работа", str(e), parent=self); return False

    def apply(self):
        self.result = dict(self._out)


class TemplateSelectDialog(simpledialog.Dialog):
    def __init__(self, parent, templates):
        self.templates = templates
        self.result: Optional[int] = None
        super().__init__(parent, title="Выбор шаблона ГПР")

    def body(self, m):
        tk.Label(m, text="Выберите шаблон:").pack(anchor="w", pady=(0, 6))
        self.lb = tk.Listbox(m, width=50, height=min(15, max(4, len(self.templates))))
        for t in self.templates:
            self.lb.insert("end", t["name"])
        self.lb.pack(fill="both", expand=True)
        if self.templates:
            self.lb.selection_set(0)
        return self.lb

    def validate(self):
        sel = self.lb.curselection()
        if not sel:
            messagebox.showwarning("Шаблон", "Выберите шаблон.", parent=self)
            return False
        self._idx = sel[0]
        return True

    def apply(self):
        self.result = int(self.templates[self._idx]["id"])


# ═══════════════════════════════════════════════════════════════
#  GANTT CANVAS (professional)
# ═══════════════════════════════════════════════════════════════
class GanttCanvas(tk.Frame):
    """Гант, синхронизированный с Treeview по позициям строк."""
    MONTH_H = 20
    DAY_H = 22
    HEADER_H = MONTH_H + DAY_H

    def __init__(self, master, *, day_px=20, linked_tree=None):
        super().__init__(master, bg=C["panel"])
        self.day_px = day_px
        self._tree = linked_tree  # ссылка на Treeview

        self.hdr = tk.Canvas(self, height=self.HEADER_H, bg="#e8eaed",
                             highlightthickness=0)
        self.body = tk.Canvas(self, bg="#ffffff", highlightthickness=0)
        self.hsb = ttk.Scrollbar(self, orient="horizontal", command=self._xview)

        self.hdr.grid(row=0, column=0, sticky="ew")
        self.body.grid(row=1, column=0, sticky="nsew")
        self.hsb.grid(row=2, column=0, sticky="ew")
        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.body.configure(xscrollcommand=self._on_xscroll)

        self._range: Tuple[date, date] = _quarter_range()
        self._rows: List[Dict[str, Any]] = []
        self._facts: Dict[int, float] = {}

        self.body.bind("<Configure>", lambda e: self.after_idle(self.redraw))
        self.body.bind("<MouseWheel>", self._wheel)
        self.body.bind("<Shift-MouseWheel>", self._hwheel)

    def set_tree(self, tree):
        self._tree = tree

    def set_range(self, d0, d1):
        self._range = (d0, d1)
        self.redraw()

    def set_data(self, rows, facts=None):
        self._rows = rows or []
        self._facts = facts or {}
        self.after(50, self.redraw)  # даём Treeview время отрисоваться

    def _xview(self, *a):
        self.body.xview(*a)

    def _on_xscroll(self, f0, f1):
        self.hsb.set(f0, f1)
        self.hdr.xview_moveto(f0)

    def _wheel(self, e):
        """Колёсико на Ганте → скроллим Treeview (он главный)."""
        if self._tree:
            d = -1 * (e.delta // 120) if e.delta else 0
            self._tree.yview_scroll(d, "units")
            self.after_idle(self.redraw)
        return "break"

    def _hwheel(self, e):
        d = -1 * (e.delta // 120) if e.delta else 0
        self.body.xview_scroll(d, "units")
        return "break"

    def _get_tree_row_positions(self) -> List[Tuple[int, int]]:
        """
        Возвращает список (y_top, y_bottom) для каждой видимой строки Treeview,
        в координатах Canvas (пиксели от верха body).
        """
        if not self._tree:
            return []

        positions = []
        items = self._tree.get_children()
        tree_top = self._tree.winfo_rooty()
        canvas_top = self.body.winfo_rooty()
        offset = tree_top - canvas_top

        for iid in items:
            try:
                bbox = self._tree.bbox(iid)
                if bbox:
                    # bbox = (x, y, width, height) относительно Treeview
                    y_in_tree = bbox[1]
                    h = bbox[3]
                    y_top = y_in_tree + offset
                    y_bot = y_top + h
                    positions.append((y_top, y_bot))
                else:
                    positions.append(None)  # строка не видна
            except Exception:
                positions.append(None)

        return positions

    def redraw(self):
        d0, d1 = self._range
        if d1 < d0:
            return
        days = (d1 - d0).days + 1
        tw = max(1, days * self.day_px)
        body_h = self.body.winfo_height()
        if body_h < 10:
            body_h = 600

        self.hdr.delete("all")
        self.body.delete("all")
        self.hdr.configure(scrollregion=(0, 0, tw, self.HEADER_H))
        self.body.configure(scrollregion=(0, 0, tw, body_h))

        # ── Заголовок: месяцы ──
        cur = date(d0.year, d0.month, 1)
        while cur <= d1:
            mr = calendar.monthrange(cur.year, cur.month)[1]
            ms = max(cur, d0)
            me = min(date(cur.year, cur.month, mr), d1)
            x0 = (ms - d0).days * self.day_px
            x1 = ((me - d0).days + 1) * self.day_px
            self.hdr.create_rectangle(x0, 0, x1, self.MONTH_H,
                                      fill="#d6dbe0", outline="#bbb")
            if (x1 - x0) > 40:
                self.hdr.create_text((x0 + x1) / 2, self.MONTH_H / 2,
                                     text=cur.strftime('%b %Y'),
                                     font=("Segoe UI", 8, "bold"), fill="#333")
            if cur.month == 12:
                cur = date(cur.year + 1, 1, 1)
            else:
                cur = date(cur.year, cur.month + 1, 1)

        # ── Заголовок: дни ──
        for i in range(days):
            x0 = i * self.day_px
            x1 = x0 + self.day_px
            d = d0 + timedelta(days=i)
            fill = "#ffecec" if d.weekday() >= 5 else "#f3f4f6"
            self.hdr.create_rectangle(x0, self.MONTH_H, x1, self.HEADER_H,
                                      fill=fill, outline="#d0d0d0")
            if self.day_px >= 14:
                self.hdr.create_text((x0 + x1) / 2, self.MONTH_H + self.DAY_H / 2,
                                     text=str(d.day), font=("Segoe UI", 7),
                                     fill="#555")

        # ── Линия «сегодня» ──
        td = _today()
        if d0 <= td <= d1:
            tx = (td - d0).days * self.day_px + self.day_px // 2
            self.hdr.create_line(tx, 0, tx, self.HEADER_H,
                                 fill=C["error"], width=2)
            self.body.create_line(tx, 0, tx, body_h,
                                  fill=C["error"], width=1, dash=(4, 2))

        # ── Получаем позиции строк из Treeview ──
        positions = self._get_tree_row_positions()

        # ── Рисуем бары по реальным позициям ──
        for row_idx, t in enumerate(self._rows):
            if row_idx >= len(positions) or positions[row_idx] is None:
                continue

            y0, y1 = positions[row_idx]

            # Зебра
            bg = "#ffffff" if row_idx % 2 == 0 else "#f8f9fa"
            self.body.create_rectangle(0, y0, tw, y1, fill=bg, outline="")

            ts = t.get("plan_start")
            tf = t.get("plan_finish")
            if not isinstance(ts, date) or not isinstance(tf, date):
                continue
            if tf < d0 or ts > d1:
                continue

            s2 = max(ts, d0)
            f2 = min(tf, d1)
            bx0 = (s2 - d0).days * self.day_px
            bx1 = ((f2 - d0).days + 1) * self.day_px

            st = (t.get("status") or "planned").strip()
            col, _, _ = STATUS_COLORS.get(st, ("#90caf9", "#555", ""))

            # Бар
            by0 = y0 + 4
            by1 = y1 - 4
            self.body.create_rectangle(bx0 + 1, by0, bx1 - 1, by1,
                                       fill=col, outline="#5f6368")

            # Факт (зелёная полоска)
            tid = t.get("id")
            pq = _safe_float(t.get("plan_qty"))
            fq = self._facts.get(tid, 0) if tid else 0
            if pq and pq > 0 and fq > 0:
                pct = min(1.0, fq / pq)
                fw = max(2, int((bx1 - bx0 - 2) * pct))
                self.body.create_rectangle(bx0 + 1, by0, bx0 + 1 + fw, by1,
                                           fill="#388e3c", outline="")

            # Веха
            if t.get("is_milestone"):
                cx = bx0 + 6
                cy = (y0 + y1) / 2
                self.body.create_polygon(
                    cx, cy, cx + 7, cy - 5, cx + 14, cy, cx + 7, cy + 5,
                    fill="#1a73e8", outline="")

            # Название на баре
            bar_w = bx1 - bx0
            if bar_w > 60:
                nm = (t.get("name") or "")[:30]
                self.body.create_text(bx0 + 4, (y0 + y1) / 2, text=nm,
                                      anchor="w", font=("Segoe UI", 7),
                                      fill="#333")

        # Сетка
        step = 7 if self.day_px >= 10 else 14
        for i in range(0, days, step):
            x = i * self.day_px
            self.body.create_line(x, 0, x, body_h, fill="#eeeeee")
# ═══════════════════════════════════════════════════════════════
#  MAIN PAGE
# ═══════════════════════════════════════════════════════════════
class GprPage(tk.Frame):

    def __init__(self, master, app_ref):
        super().__init__(master, bg=C["bg"])
        self.app_ref = app_ref

        self.objects: List[Dict[str, Any]] = []
        self.work_types: List[Dict[str, Any]] = []
        self.uoms: List[Dict[str, Any]] = []

        self.object_db_id: Optional[int] = None
        self.plan_info: Optional[Dict[str, Any]] = None
        self.plan_id: Optional[int] = None

        self.tasks: List[Dict[str, Any]] = []
        self.tasks_filtered: List[Dict[str, Any]] = []
        self.facts: Dict[int, float] = {}

        q = _quarter_range()
        self.range_from: date = q[0]
        self.range_to: date = q[1]

        self._build_ui()
        self._load_refs()

    # ══════════════════════════════════════════════════════════
    #  BUILD UI
    # ══════════════════════════════════════════════════════════
    def _build_ui(self):
        # ── header ──
        hdr = tk.Frame(self, bg=C["accent"], pady=6)
        hdr.pack(fill="x")
        tk.Label(hdr, text="📊  ГПР — График производства работ",
                 font=("Segoe UI", 12, "bold"), bg=C["accent"], fg="white",
                 padx=12).pack(side="left")

        self.lbl_plan_info = tk.Label(
            hdr, text="", font=("Segoe UI", 8), bg=C["accent"], fg="#bbdefb",
            padx=12)
        self.lbl_plan_info.pack(side="right")

        # ── top panel (object + range) ──
        top = tk.LabelFrame(self, text=" 📍 Объект и диапазон ",
                            font=("Segoe UI", 9, "bold"),
                            bg=C["panel"], fg=C["accent"],
                            relief="groove", bd=1, padx=10, pady=8)
        top.pack(fill="x", padx=10, pady=(8, 4))
        top.grid_columnconfigure(1, weight=1)

        tk.Label(top, text="Объект:", bg=C["panel"],
                 font=("Segoe UI", 9)).grid(row=0, column=0, sticky="e", padx=(0, 6))
        self.cmb_obj = _AutoCombo(top, width=60, font=("Segoe UI", 9))
        self.cmb_obj.grid(row=0, column=1, sticky="ew", pady=3)

        btn_f = tk.Frame(top, bg=C["panel"])
        btn_f.grid(row=0, column=2, padx=(8, 0))
        self._accent_btn(btn_f, "▶  Открыть", self._open_object).pack(side="left")

        tk.Label(top, text="Диапазон:", bg=C["panel"],
                 font=("Segoe UI", 9)).grid(row=1, column=0, sticky="e", padx=(0, 6))

        range_f = tk.Frame(top, bg=C["panel"])
        range_f.grid(row=1, column=1, sticky="w", pady=3)
        self.lbl_range = tk.Label(range_f, text="", bg=C["panel"], fg=C["text2"],
                                  font=("Segoe UI", 9))
        self.lbl_range.pack(side="left")
        ttk.Button(range_f, text="Изменить…", command=self._change_range).pack(
            side="left", padx=(12, 0))
        ttk.Button(range_f, text="По работам", command=self._fit_range).pack(
            side="left", padx=(6, 0))

        # ── toolbar ──
        bar = tk.Frame(self, bg=C["accent_light"], pady=5)
        bar.pack(fill="x", padx=10)

        self._tb_btn(bar, "➕ Добавить", self._add_task)
        self._tb_btn(bar, "✏️ Редактировать", self._edit_selected)
        self._tb_btn(bar, "🗑 Удалить", self._delete_selected)
        tk.Frame(bar, bg=C["border"], width=1).pack(side="left", fill="y", padx=8)
        self._tb_btn(bar, "📋 Из шаблона…", self._apply_template)
        self._tb_btn(bar, "📥 Экспорт Excel", self._export_excel)
        tk.Frame(bar, bg=C["border"], width=1).pack(side="left", fill="y", padx=8)
        self._tb_btn(bar, "🔍−", lambda: self._zoom(-2))
        self._tb_btn(bar, "🔍+", lambda: self._zoom(2))

        self._accent_btn(bar, "💾  СОХРАНИТЬ", self._save).pack(side="right", padx=(4, 8))

        # ── filter bar ──
        fbar = tk.Frame(self, bg=C["bg"], pady=4)
        fbar.pack(fill="x", padx=10)

        tk.Label(fbar, text="Фильтр тип:", bg=C["bg"],
                 font=("Segoe UI", 8)).pack(side="left")
        self.cmb_filt_wt = ttk.Combobox(fbar, state="readonly", width=20,
                                         values=["Все"])
        self.cmb_filt_wt.pack(side="left", padx=(4, 12)); self.cmb_filt_wt.current(0)
        self.cmb_filt_wt.bind("<<ComboboxSelected>>", lambda e: self._apply_filter())

        tk.Label(fbar, text="Статус:", bg=C["bg"],
                 font=("Segoe UI", 8)).pack(side="left")
        self.cmb_filt_st = ttk.Combobox(fbar, state="readonly", width=16,
                                         values=["Все"] + [STATUS_LABELS[s] for s in STATUS_LIST])
        self.cmb_filt_st.pack(side="left", padx=(4, 12)); self.cmb_filt_st.current(0)
        self.cmb_filt_st.bind("<<ComboboxSelected>>", lambda e: self._apply_filter())

        tk.Label(fbar, text="Поиск:", bg=C["bg"],
                 font=("Segoe UI", 8)).pack(side="left")
        self.var_search = tk.StringVar()
        ent_s = ttk.Entry(fbar, textvariable=self.var_search, width=24)
        ent_s.pack(side="left", padx=(4, 0))
        ent_s.bind("<KeyRelease>", lambda e: self._apply_filter())

        # ── summary ──
        self.lbl_summary = tk.Label(self, text="", bg=C["bg"],
                                    font=("Segoe UI", 8), fg=C["text2"], anchor="w")
        self.lbl_summary.pack(fill="x", padx=14, pady=(2, 0))

        # ── legend ──
        leg = tk.Frame(self, bg=C["bg"])
        leg.pack(fill="x", padx=14, pady=(0, 2))
        for code in STATUS_LIST:
            col, _, label = STATUS_COLORS[code]
            f = tk.Frame(leg, bg=C["bg"])
            f.pack(side="left", padx=(0, 12))
            tk.Canvas(f, width=12, height=12, bg=col, highlightthickness=1,
                      highlightbackground="#999").pack(side="left", padx=(0, 3))
            tk.Label(f, text=label, bg=C["bg"], font=("Segoe UI", 7),
                     fg=C["text2"]).pack(side="left")

        # ── split: tree + gantt ──
        pw = tk.PanedWindow(self, orient="horizontal", sashrelief="raised",
                            bg=C["bg"])
        pw.pack(fill="both", expand=True, padx=10, pady=(4, 10))

        left = tk.Frame(pw, bg=C["panel"])
        right = tk.Frame(pw, bg=C["panel"])
        pw.add(left, minsize=480)
        pw.add(right, minsize=400)

        # Treeview
        cols = ("type", "name", "start", "finish", "uom", "qty", "status")
        self.tree = ttk.Treeview(left, columns=cols, show="headings",
                                 selectmode="browse")
        heads = {"type": ("Тип работ", 130), "name": ("Вид работ", 220),
                 "start": ("Начало", 85), "finish": ("Конец", 85),
                 "uom": ("Ед.", 50), "qty": ("Объём", 75),
                 "status": ("Статус", 100)}
        for c, (t, w) in heads.items():
            self.tree.heading(c, text=t)
            anc = "center" if c in ("start", "finish", "uom", "status") else (
                "e" if c == "qty" else "w")
            self.tree.column(c, width=w, anchor=anc)

        vsb = ttk.Scrollbar(left, orient="vertical", command=self._on_tree_scroll)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.bind("<Double-1>", lambda e: self._edit_selected())
        self.tree.bind("<Return>", lambda e: self._edit_selected())
        self.tree.bind("<MouseWheel>", self._on_tree_wheel)

        # Gantt (привязан к Treeview)
        self.gantt = GanttCanvas(right, day_px=20, linked_tree=self.tree)
        self.gantt.pack(fill="both", expand=True)
        
    # ── UI helpers ──
    def _accent_btn(self, parent, text, cmd):
        b = tk.Button(parent, text=text, font=("Segoe UI", 9, "bold"),
                      bg=C["btn_bg"], fg=C["btn_fg"],
                      activebackground="#0d47a1", activeforeground="white",
                      relief="flat", cursor="hand2", padx=10, pady=3, command=cmd)
        b.pack(side="left", padx=2)
        b.bind("<Enter>", lambda e: b.config(bg="#0d47a1"))
        b.bind("<Leave>", lambda e: b.config(bg=C["btn_bg"]))
        return b

    def _tb_btn(self, parent, text, cmd):
        ttk.Button(parent, text=text, command=cmd).pack(side="left", padx=2)

    
    def _on_tree_scroll(self, *args):
        """Scrollbar двигает Treeview, потом перерисовываем Гант."""
        self.tree.yview(*args)
        self.gantt.after_idle(self.gantt.redraw)

    def _on_tree_wheel(self, event):
        """Колёсико на Treeview → скролл + перерисовка Ганта."""
        d = -1 * (event.delta // 120) if event.delta else 0
        self.tree.yview_scroll(d, "units")
        self.gantt.after_idle(self.gantt.redraw)
        return "break"

    # ══════════════════════════════════════════════════════════
    #  DATA
    # ══════════════════════════════════════════════════════════
    def _load_refs(self):
        try:
            self.objects = GprService.load_objects_short()
            self.work_types = GprService.load_work_types()
            self.uoms = GprService.load_uoms()
        except Exception as e:
            logging.exception("GPR refs error")
            messagebox.showerror("ГПР", f"Ошибка загрузки справочников:\n{e}", parent=self)
            return
    
        vals = []
        for o in self.objects:
            sn = (o.get("short_name") or "").strip()
            addr = (o.get("address") or "").strip()
            eid = str(o.get("excel_id") or "").strip()
            db_id = str(o.get("id") or "")
    
            # ВСЕГДА показываем short_name + address + идентификатор
            # Это гарантирует уникальность каждой строки
            tag = f"[{eid}]" if eid else f"[id:{db_id}]"
    
            if sn:
                lbl = f"{sn} — {addr} — {tag}"
            else:
                lbl = f"{addr} — {tag}"
    
            vals.append(lbl)
    
        self.cmb_obj.set_values(vals)
    
        wt_names = ["Все"] + [w["name"] for w in self.work_types]
        self.cmb_filt_wt.config(values=wt_names)

    def _update_range_label(self):
        self.lbl_range.config(
            text=f"{_fmt_date(self.range_from)} — {_fmt_date(self.range_to)}")
        self.gantt.set_range(self.range_from, self.range_to)

    def _update_plan_info(self):
        p = self.plan_info
        if not p:
            self.lbl_plan_info.config(text="")
            return
        cr = p.get("creator_name") or "—"
        upd = p.get("updated_at")
        upd_s = upd.strftime("%d.%m.%Y %H:%M") if isinstance(upd, datetime) else str(upd or "")
        v = p.get("version_no", 1)
        self.lbl_plan_info.config(
            text=f"Версия: {v}  |  Создал: {cr}  |  Обновлён: {upd_s}")

    def _update_summary(self):
        total = len(self.tasks)
        by_st = {}
        overdue = 0
        td = _today()
        for t in self.tasks:
            st = t.get("status", "planned")
            by_st[st] = by_st.get(st, 0) + 1
            if st not in ("done", "canceled"):
                pf = t.get("plan_finish")
                if isinstance(pf, date) and pf < td:
                    overdue += 1

        parts = [f"Всего: {total}"]
        for s in STATUS_LIST:
            if by_st.get(s, 0) > 0:
                parts.append(f"{STATUS_LABELS[s]}: {by_st[s]}")
        if overdue > 0:
            parts.append(f"⚠ Просрочено: {overdue}")
        self.lbl_summary.config(text="  |  ".join(parts))

    # ══════════════════════════════════════════════════════════
    #  ACTIONS
    # ══════════════════════════════════════════════════════════
    def _sel_obj_id(self) -> Optional[int]:
        idx = self.cmb_obj.current()
        if idx < 0 or idx >= len(self.objects): return None
        return int(self.objects[idx]["id"])

    def _open_object(self):
        oid = self._sel_obj_id()
        if not oid:
            messagebox.showwarning("ГПР", "Выберите объект из списка.", parent=self)
            return
        self.object_db_id = oid
        uid = (self.app_ref.current_user or {}).get("id")
        try:
            self.plan_info = GprService.get_or_create_current_plan(oid, uid)
            self.plan_id = int(self.plan_info["id"])
            self.tasks = GprService.load_plan_tasks(self.plan_id)
            tids = [t["id"] for t in self.tasks if t.get("id")]
            self.facts = GprService.load_task_facts_cumulative(tids)
        except Exception as e:
            logging.exception("GPR open error")
            messagebox.showerror("ГПР", f"Не удалось открыть ГПР:\n{e}", parent=self)
            return

        self._update_plan_info()
        self._apply_filter()
        self._update_summary()

        obj = next((o for o in self.objects if int(o["id"]) == oid), None)
        addr = obj["address"] if obj else ""
        self.lbl_bottom.config(text=f"Объект: {addr}  |  Работ: {len(self.tasks)}")

    def _apply_filter(self):
        wt_idx = self.cmb_filt_wt.current()
        wt_name = None
        if wt_idx > 0:
            wt_name = self.work_types[wt_idx - 1]["name"]

        st_idx = self.cmb_filt_st.current()
        st_code = None
        if st_idx > 0:
            st_code = STATUS_LIST[st_idx - 1]

        q = (self.var_search.get() or "").strip().lower()

        res = []
        for t in self.tasks:
            if wt_name and (t.get("work_type_name") or "") != wt_name:
                continue
            if st_code and (t.get("status") or "") != st_code:
                continue
            if q:
                nm = (t.get("name") or "").lower()
                wtn = (t.get("work_type_name") or "").lower()
                if q not in nm and q not in wtn:
                    continue
            res.append(t)

        self.tasks_filtered = res
        self._render()

    def _render(self):
        self.tree.delete(*self.tree.get_children())
        for t in self.tasks_filtered:
            iid = str(t.get("id") or "")
            st_label = STATUS_LABELS.get(t.get("status", ""), t.get("status", ""))
            self.tree.insert("", "end", iid=iid if iid else None, values=(
                t.get("work_type_name", ""),
                t.get("name", ""),
                _fmt_date(t.get("plan_start")),
                _fmt_date(t.get("plan_finish")),
                t.get("uom_code") or "",
                _fmt_qty(t.get("plan_qty")),
                st_label,
            ))
        self.gantt.set_data(self.tasks_filtered, self.facts)

    def _change_range(self):
        dlg = DateRangeDialog(self, self.range_from, self.range_to)
        if dlg.result:
            self.range_from, self.range_to = dlg.result
            self._update_range_label()
            self.gantt.set_data(self.tasks_filtered, self.facts)

    def _fit_range(self):
        if not self.tasks:
            return
        d0 = min(t["plan_start"] for t in self.tasks if isinstance(t.get("plan_start"), date))
        d1 = max(t["plan_finish"] for t in self.tasks if isinstance(t.get("plan_finish"), date))
        self.range_from = d0 - timedelta(days=7)
        self.range_to = d1 + timedelta(days=7)
        self._update_range_label()
        self.gantt.set_data(self.tasks_filtered, self.facts)

    def _zoom(self, delta):
        self.gantt.day_px = max(6, min(50, self.gantt.day_px + delta))
        self.gantt.redraw()

    # ── CRUD ──
    def _find_task_idx(self) -> Optional[int]:
        sel = self.tree.selection()
        if not sel: return None
        iid = sel[0]
        try:
            tid = int(iid)
            for i, t in enumerate(self.tasks):
                if t.get("id") and int(t["id"]) == tid: return i
        except: pass
        try: return self.tree.index(iid)
        except: return None

    def _add_task(self):
        if not self.plan_id:
            messagebox.showinfo("ГПР", "Сначала откройте объект.", parent=self)
            return
        uid = (self.app_ref.current_user or {}).get("id")
        from gpr_task_dialog import open_task_dialog
        result = open_task_dialog(self, self.work_types, self.uoms,
                                   init={"plan_start": self.range_from,
                                         "plan_finish": self.range_from},
                                   user_id=uid)
        if not result:
            return
        t = result
        t["id"] = None
        t["work_type_name"] = next(
            (w["name"] for w in self.work_types if int(w["id"]) == int(t["work_type_id"])), "")
        t["sort_order"] = len(self.tasks) * 10
        self.tasks.append(t)
        self._apply_filter()
        self._update_summary()

    def _edit_selected(self):
        from gpr_task_dialog import open_task_dialog, _EmployeeService
        idx = self._find_task_idx()
        if idx is None:
            return
        t0 = self.tasks[idx]
        uid = (self.app_ref.current_user or {}).get("id")
        result = open_task_dialog(self, self.work_types, self.uoms,
                                   init=t0, user_id=uid)
        if not result:
            return
        upd = result
        upd["id"] = t0.get("id")
        upd["sort_order"] = t0.get("sort_order", idx * 10)
        upd["work_type_name"] = next(
            (w["name"] for w in self.work_types if int(w["id"]) == int(upd["work_type_id"])), "")
    
        # Сохраняем назначения если задача уже в БД
        task_id = t0.get("id")
        assignments = upd.pop("_assignments", [])
        if task_id and assignments is not None:
            try:
                _EmployeeService.save_task_assignments(task_id, assignments, uid)
            except Exception as e:
                logging.exception("Save assignments error")
    
        self.tasks[idx] = upd
        self._apply_filter()
        self._update_summary()

    def _delete_selected(self):
        idx = self._find_task_idx()
        if idx is None: return
        if not messagebox.askyesno("ГПР", "Удалить работу?", parent=self): return
        self.tasks.pop(idx)
        self._apply_filter()
        self._update_summary()

    def _apply_template(self):
        if not self.plan_id:
            messagebox.showinfo("ГПР", "Сначала откройте объект.", parent=self); return
        try:
            tpls = GprService.load_templates()
        except Exception as e:
            messagebox.showerror("ГПР", f"Ошибка:\n{e}", parent=self); return
        if not tpls:
            messagebox.showinfo("ГПР", "Шаблонов нет.", parent=self); return

        dlg = TemplateSelectDialog(self, tpls)
        if not dlg.result: return

        try:
            tt = GprService.load_template_tasks(dlg.result)
        except Exception as e:
            messagebox.showerror("ГПР", f"Ошибка:\n{e}", parent=self); return
        if not tt:
            messagebox.showinfo("ГПР", "В шаблоне нет задач.", parent=self); return
        if self.tasks and not messagebox.askyesno(
                "ГПР", "Заменить текущие работы?", parent=self):
            return

        base = self.range_from
        out = []
        for i, x in enumerate(tt):
            wid = int(x["work_type_id"])
            wn = next((w["name"] for w in self.work_types if int(w["id"]) == wid), "")
            out.append(dict(
                id=None, work_type_id=wid, work_type_name=wn,
                name=x["name"], uom_code=x.get("uom_code"),
                plan_qty=x.get("default_qty"),
                plan_start=base, plan_finish=base,
                status="planned",
                is_milestone=bool(x.get("is_milestone")),
                sort_order=int(x.get("sort_order") if x.get("sort_order") is not None else i*10),
            ))
        self.tasks = out
        self._apply_filter()
        self._update_summary()

    def _save(self):
        if not self.plan_id:
            messagebox.showinfo("ГПР", "Сначала откройте объект.", parent=self); return
        for t in self.tasks:
            if not t.get("name"):
                messagebox.showwarning("ГПР", "Есть работа без названия.", parent=self); return
            ds, df = t.get("plan_start"), t.get("plan_finish")
            if not isinstance(ds, date) or not isinstance(df, date) or df < ds:
                messagebox.showwarning("ГПР",
                    f"Некорректные даты: {t.get('name')}", parent=self); return

        uid = (self.app_ref.current_user or {}).get("id")
        try:
            GprService.replace_plan_tasks(self.plan_id, uid, self.tasks)
            self.tasks = GprService.load_plan_tasks(self.plan_id)
            tids = [t["id"] for t in self.tasks if t.get("id")]
            self.facts = GprService.load_task_facts_cumulative(tids)
            self.plan_info = GprService.get_or_create_current_plan(self.object_db_id, uid)
            self._update_plan_info()
            self._apply_filter()
            self._update_summary()
            messagebox.showinfo("ГПР", "Сохранено.", parent=self)
        except Exception as e:
            logging.exception("GPR save err")
            messagebox.showerror("ГПР", f"Ошибка сохранения:\n{e}", parent=self)

    # ══════════════════════════════════════════════════════════
    #  EXPORT TO EXCEL
    # ══════════════════════════════════════════════════════════
    def _export_excel(self):
        if not self.tasks:
            messagebox.showinfo("ГПР", "Нет данных для выгрузки.", parent=self)
            return

        if not HAS_OPENPYXL:
            messagebox.showwarning("ГПР",
                "Для экспорта необходима библиотека openpyxl.", parent=self)
            return

        obj = next((o for o in self.objects
                     if int(o["id"]) == self.object_db_id), None)
        obj_name = (obj["short_name"] or obj["address"]) if obj else "объект"
        default_name = f"ГПР_{obj_name}_{_today().strftime('%Y%m%d')}.xlsx"
        # убираем спецсимволы из имени файла
        default_name = "".join(
            c if c.isalnum() or c in "._- ()" else "_" for c in default_name
        )

        path = filedialog.asksaveasfilename(
            parent=self,
            title="Сохранить ГПР в Excel",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel", "*.xlsx"), ("Все файлы", "*.*")],
        )
        if not path:
            return

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "ГПР"

            # ── заголовки ──
            headers = [
                "№", "Тип работ", "Вид работ", "Ед. изм.",
                "Объём план", "Начало", "Окончание",
                "Длительность (дн.)", "Статус", "Факт (накоп.)", "% выполнения",
            ]
            widths = [6, 22, 36, 8, 14, 14, 14, 14, 16, 14, 14]

            for i, h in enumerate(headers, 1):
                cell = ws.cell(1, i, h)
                cell.font = Font(bold=True, size=10)
                cell.fill = PatternFill("solid", fgColor="D6DCE4")
                cell.alignment = Alignment(horizontal="center", vertical="center",
                                           wrap_text=True)

            for i, w in enumerate(widths, 1):
                ws.column_dimensions[get_column_letter(i)].width = w

            ws.freeze_panes = "A2"

            # ── строки ──
            status_fill = {
                "planned":     PatternFill("solid", fgColor="D6EAFF"),
                "in_progress": PatternFill("solid", fgColor="FFF3CD"),
                "done":        PatternFill("solid", fgColor="D4EDDA"),
                "paused":      PatternFill("solid", fgColor="FFF9C4"),
                "canceled":    PatternFill("solid", fgColor="F8D7DA"),
            }

            for row_num, t in enumerate(self.tasks, start=2):
                ds = t.get("plan_start")
                df = t.get("plan_finish")
                dur = (df - ds).days + 1 if isinstance(ds, date) and isinstance(df, date) else ""

                pq = _safe_float(t.get("plan_qty"))
                tid = t.get("id")
                fq = self.facts.get(tid, 0) if tid else 0
                pct = ""
                if pq and pq > 0:
                    pct = f"{min(100.0, fq / pq * 100):.1f}%"

                st_code = t.get("status", "planned")
                st_label = STATUS_LABELS.get(st_code, st_code)

                values = [
                    row_num - 1,
                    t.get("work_type_name", ""),
                    t.get("name", ""),
                    t.get("uom_code") or "",
                    _fmt_qty(pq) if pq else "",
                    _fmt_date(ds),
                    _fmt_date(df),
                    dur,
                    st_label,
                    _fmt_qty(fq) if fq else "",
                    pct,
                ]

                for col, val in enumerate(values, 1):
                    cell = ws.cell(row_num, col, val)
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                    # подкрашиваем статус
                    if col == 9:
                        fill = status_fill.get(st_code)
                        if fill:
                            cell.fill = fill

            # ── итоговая строка ──
            last_row = len(self.tasks) + 2
            ws.cell(last_row, 1, "").font = Font(bold=True)
            ws.cell(last_row, 2, f"Итого работ: {len(self.tasks)}").font = Font(bold=True)

            done_cnt = sum(1 for t in self.tasks if t.get("status") == "done")
            ws.cell(last_row, 9, f"Выполнено: {done_cnt}").font = Font(bold=True)

            wb.save(path)
            messagebox.showinfo("ГПР", f"Файл сохранён:\n{path}", parent=self)

        except Exception as e:
            logging.exception("GPR excel export error")
            messagebox.showerror("ГПР", f"Ошибка экспорта:\n{e}", parent=self)


# ═══════════════════════════════════════════════════════════════
#  API for main_app
# ═══════════════════════════════════════════════════════════════
def create_gpr_page(parent, app_ref) -> GprPage:
    """Фабричная функция — вызывается из main_app._show_page."""
    return GprPage(parent, app_ref=app_ref)
