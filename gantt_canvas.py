"""
gantt_canvas.py
Canvas диаграммы Ганта для модуля ГПР
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

import tkinter as tk
from tkinter import ttk

from gpr_common import (
    C,
    STATUS_COLORS,
    today,
    safe_float,
)


class GanttCanvas(tk.Frame):
    """
    Диаграмма Ганта синхронизированная с Treeview
    """

    MONTH_H = 20
    DAY_H = 22
    HEADER_H = MONTH_H + DAY_H

    def __init__(self, master, *, day_px=20, linked_tree=None):
        super().__init__(master, bg=C["panel"])

        self.day_px = day_px
        self._tree = linked_tree

        self._range: Tuple[date, date] = (today(), today())

        self._rows: List[Dict[str, Any]] = []
        self._facts: Dict[int, float] = {}

        self._build_ui()

    # ─────────────────────────────────────────────
    # UI
    # ─────────────────────────────────────────────
    def _build_ui(self):

        self.hdr = tk.Canvas(
            self,
            height=self.HEADER_H,
            bg="#e8eaed",
            highlightthickness=0,
        )

        self.body = tk.Canvas(
            self,
            bg="#ffffff",
            highlightthickness=0,
        )

        self.hsb = ttk.Scrollbar(
            self,
            orient="horizontal",
            command=self._xview,
        )

        self.hdr.grid(row=0, column=0, sticky="ew")
        self.body.grid(row=1, column=0, sticky="nsew")
        self.hsb.grid(row=2, column=0, sticky="ew")

        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.body.configure(xscrollcommand=self._on_xscroll)

        self.body.bind("<Configure>", lambda e: self.after_idle(self.redraw))

        self.body.bind("<MouseWheel>", self._wheel)
        self.body.bind("<Shift-MouseWheel>", self._hwheel)

    # ─────────────────────────────────────────────
    # API
    # ─────────────────────────────────────────────
    def set_tree(self, tree):
        self._tree = tree

    def set_range(self, d0: date, d1: date):
        self._range = (d0, d1)
        self.redraw()

    def set_data(self, rows, facts=None):
        self._rows = rows or []
        self._facts = facts or {}
        self.after(50, self.redraw)

    # ─────────────────────────────────────────────
    # SCROLL
    # ─────────────────────────────────────────────
    def _xview(self, *args):
        self.body.xview(*args)

    def _on_xscroll(self, f0, f1):
        self.hsb.set(f0, f1)
        self.hdr.xview_moveto(f0)

    def _wheel(self, event):

        if self._tree:
            delta = -1 * (event.delta // 120)
            self._tree.yview_scroll(delta, "units")
            self.after_idle(self.redraw)

        return "break"

    def _hwheel(self, event):

        delta = -1 * (event.delta // 120)
        self.body.xview_scroll(delta, "units")

        return "break"

    # ─────────────────────────────────────────────
    # TREE ROW POSITIONS
    # ─────────────────────────────────────────────
    def _get_tree_row_positions(self):

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

                    y = bbox[1]
                    h = bbox[3]

                    y0 = y + offset
                    y1 = y0 + h

                    positions.append((y0, y1))

                else:

                    positions.append(None)

            except Exception:

                positions.append(None)

        return positions

    # ─────────────────────────────────────────────
    # REDRAW
    # ─────────────────────────────────────────────
    def redraw(self):

        d0, d1 = self._range

        if d1 < d0:
            return

        days = (d1 - d0).days + 1

        width = max(1, days * self.day_px)

        body_h = self.body.winfo_height()

        if body_h < 10:
            body_h = 600

        self.hdr.delete("all")
        self.body.delete("all")

        self.hdr.configure(scrollregion=(0, 0, width, self.HEADER_H))
        self.body.configure(scrollregion=(0, 0, width, body_h))

        self._draw_months(d0, d1)
        self._draw_days(d0, days)
        self._draw_today_line(d0, d1, body_h)
        self._draw_tasks(d0, d1, width)

    # ─────────────────────────────────────────────
    # HEADER
    # ─────────────────────────────────────────────
    def _draw_months(self, d0, d1):

        cur = date(d0.year, d0.month, 1)

        while cur <= d1:

            mr = calendar.monthrange(cur.year, cur.month)[1]

            ms = max(cur, d0)
            me = min(date(cur.year, cur.month, mr), d1)

            x0 = (ms - d0).days * self.day_px
            x1 = ((me - d0).days + 1) * self.day_px

            self.hdr.create_rectangle(
                x0, 0, x1, self.MONTH_H,
                fill="#d6dbe0",
                outline="#bbb"
            )

            if (x1 - x0) > 40:

                self.hdr.create_text(
                    (x0 + x1) / 2,
                    self.MONTH_H / 2,
                    text=cur.strftime('%b %Y'),
                    font=("Segoe UI", 8, "bold"),
                    fill="#333"
                )

            if cur.month == 12:
                cur = date(cur.year + 1, 1, 1)
            else:
                cur = date(cur.year, cur.month + 1, 1)

    def _draw_days(self, d0, days):

        for i in range(days):

            x0 = i * self.day_px
            x1 = x0 + self.day_px

            d = d0 + timedelta(days=i)

            fill = "#ffecec" if d.weekday() >= 5 else "#f3f4f6"

            self.hdr.create_rectangle(
                x0,
                self.MONTH_H,
                x1,
                self.HEADER_H,
                fill=fill,
                outline="#d0d0d0",
            )

            if self.day_px >= 14:

                self.hdr.create_text(
                    (x0 + x1) / 2,
                    self.MONTH_H + self.DAY_H / 2,
                    text=str(d.day),
                    font=("Segoe UI", 7),
                    fill="#555",
                )

    # ─────────────────────────────────────────────
    # TODAY
    # ─────────────────────────────────────────────
    def _draw_today_line(self, d0, d1, body_h):

        td = today()

        if not (d0 <= td <= d1):
            return

        tx = (td - d0).days * self.day_px + self.day_px // 2

        self.hdr.create_line(
            tx, 0, tx, self.HEADER_H,
            fill=C["error"],
            width=2
        )

        self.body.create_line(
            tx, 0, tx, body_h,
            fill=C["error"],
            width=1,
            dash=(4, 2)
        )

    # ─────────────────────────────────────────────
    # TASKS
    # ─────────────────────────────────────────────
    def _draw_tasks(self, d0, d1, width):

        positions = self._get_tree_row_positions()

        for idx, task in enumerate(self._rows):

            if idx >= len(positions):
                continue

            pos = positions[idx]

            if not pos:
                continue

            y0, y1 = pos

            bg = "#ffffff" if idx % 2 == 0 else "#f8f9fa"

            self.body.create_rectangle(
                0, y0, width, y1,
                fill=bg,
                outline=""
            )

            ts = task.get("plan_start")
            tf = task.get("plan_finish")

            if not ts or not tf:
                continue

            if tf < d0 or ts > d1:
                continue

            s2 = max(ts, d0)
            f2 = min(tf, d1)

            bx0 = (s2 - d0).days * self.day_px
            bx1 = ((f2 - d0).days + 1) * self.day_px

            st = task.get("status", "planned")

            color, _, _ = STATUS_COLORS.get(st, ("#90caf9", "#333", ""))

            by0 = y0 + 4
            by1 = y1 - 4

            self.body.create_rectangle(
                bx0 + 1,
                by0,
                bx1 - 1,
                by1,
                fill=color,
                outline="#5f6368"
            )

            # факт выполнения
            tid = task.get("id")

            pq = safe_float(task.get("plan_qty"))

            fq = self._facts.get(tid, 0) if tid else 0

            if pq and pq > 0 and fq > 0:

                pct = min(1.0, fq / pq)

                fw = max(2, int((bx1 - bx0 - 2) * pct))

                self.body.create_rectangle(
                    bx0 + 1,
                    by0,
                    bx0 + 1 + fw,
                    by1,
                    fill="#388e3c",
                    outline=""
                )

            if task.get("is_milestone"):

                cx = bx0 + 6
                cy = (y0 + y1) / 2

                self.body.create_polygon(
                    cx, cy,
                    cx + 7, cy - 5,
                    cx + 14, cy,
                    cx + 7, cy + 5,
                    fill="#1a73e8",
                    outline=""
                )
