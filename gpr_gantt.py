from __future__ import annotations

import calendar
import tkinter as tk
from datetime import date, timedelta
from tkinter import ttk
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from gpr_common import (
    C,
    GprTask,
    coerce_to_date,
    safe_float,
    status_fill_color,
    today,
)


class GanttCanvas(tk.Frame):
    """
    Диаграмма Ганта, визуально синхронизированная с Treeview по строкам.

    Основные особенности:
    - заголовок месяцев/дней;
    - body canvas для баров;
    - горизонтальный скролл;
    - вертикальная синхронизация через связанный Treeview;
    - debounce перерисовки;
    - поддержка как GprTask, так и dict-подобных задач.
    """

    MONTH_H = 20
    DAY_H = 22
    HEADER_H = MONTH_H + DAY_H

    def __init__(
        self,
        master,
        *,
        day_px: int = 20,
        linked_tree=None,
        row_height_fallback: int = 24,
        redraw_delay_ms: int = 40,
    ):
        super().__init__(master, bg=C["panel"])

        self.day_px = max(6, min(50, int(day_px)))
        self._tree = linked_tree
        self._row_height_fallback = max(18, int(row_height_fallback))
        self._redraw_delay_ms = max(10, int(redraw_delay_ms))

        self._range: Tuple[date, date] = (today(), today())
        self._rows: List[Any] = []
        self._facts: Dict[int, float] = {}

        self._redraw_job = None

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
        self.hsb = ttk.Scrollbar(self, orient="horizontal", command=self._xview)

        self.hdr.grid(row=0, column=0, sticky="ew")
        self.body.grid(row=1, column=0, sticky="nsew")
        self.hsb.grid(row=2, column=0, sticky="ew")

        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.body.configure(xscrollcommand=self._on_xscroll)

        self.body.bind("<Configure>", lambda _e: self.schedule_redraw())
        self.body.bind("<MouseWheel>", self._wheel)
        self.body.bind("<Shift-MouseWheel>", self._hwheel)
        self.body.bind("<Button-1>", self._on_body_click)

        # Linux wheel
        self.body.bind("<Button-4>", self._wheel_linux_up)
        self.body.bind("<Button-5>", self._wheel_linux_down)
        self.body.bind("<Shift-Button-4>", self._hwheel_linux_left)
        self.body.bind("<Shift-Button-5>", self._hwheel_linux_right)

    # =========================================================
    # Lifecycle
    # =========================================================

    def destroy(self):
        if self._redraw_job is not None:
            try:
                self.after_cancel(self._redraw_job)
            except Exception:
                pass
            self._redraw_job = None
        super().destroy()

    # =========================================================
    # Public API
    # =========================================================

    def set_tree(self, tree) -> None:
        self._tree = tree
        self.schedule_redraw()

    def set_range(self, d0: date, d1: date) -> None:
        if not isinstance(d0, date) or not isinstance(d1, date):
            return
        if d1 < d0:
            d0, d1 = d1, d0
        self._range = (d0, d1)
        self.schedule_redraw()

    def set_data(self, rows: Sequence[Any], facts: Optional[Mapping[int, float]] = None) -> None:
        self._rows = list(rows or [])
        self._facts = {int(k): float(v or 0.0) for k, v in (facts or {}).items()}
        self.schedule_redraw()

    def set_day_px(self, day_px: int) -> None:
        self.day_px = max(6, min(50, int(day_px)))
        self.schedule_redraw()

    def schedule_redraw(self) -> None:
        if self._redraw_job is not None:
            try:
                self.after_cancel(self._redraw_job)
            except Exception:
                pass
            self._redraw_job = None
        self._redraw_job = self.after(self._redraw_delay_ms, self.redraw)

    # =========================================================
    # Scroll / input
    # =========================================================

    def _xview(self, *args):
        self.body.xview(*args)

    def _on_xscroll(self, first, last):
        self.hsb.set(first, last)
        try:
            self.hdr.xview_moveto(first)
        except Exception:
            pass

    def _wheel(self, event):
        if self._tree is not None:
            delta = -1 * (event.delta // 120) if event.delta else 0
            if delta != 0:
                self._tree.yview_scroll(delta, "units")
                self.after_idle(self.schedule_redraw)
        return "break"

    def _hwheel(self, event):
        delta = -1 * (event.delta // 120) if event.delta else 0
        if delta != 0:
            self.body.xview_scroll(delta, "units")
        return "break"

    def _wheel_linux_up(self, _event):
        if self._tree is not None:
            self._tree.yview_scroll(-1, "units")
            self.after_idle(self.schedule_redraw)
        return "break"

    def _wheel_linux_down(self, _event):
        if self._tree is not None:
            self._tree.yview_scroll(1, "units")
            self.after_idle(self.schedule_redraw)
        return "break"

    def _hwheel_linux_left(self, _event):
        self.body.xview_scroll(-1, "units")
        return "break"

    def _hwheel_linux_right(self, _event):
        self.body.xview_scroll(1, "units")
        return "break"

    # =========================================================
    # Drawing helpers
    # =========================================================

    def _days_count(self) -> int:
        d0, d1 = self._range
        if d1 < d0:
            return 0
        return (d1 - d0).days + 1

    def _total_width(self) -> int:
        return max(1, self._days_count() * self.day_px)

    def _extract_task_start(self, task: Any) -> Optional[date]:
        if isinstance(task, GprTask):
            return coerce_to_date(task.plan_start)
        if isinstance(task, Mapping):
            return coerce_to_date(task.get("plan_start"))
        return None

    def _extract_task_finish(self, task: Any) -> Optional[date]:
        if isinstance(task, GprTask):
            return coerce_to_date(task.plan_finish)
        if isinstance(task, Mapping):
            return coerce_to_date(task.get("plan_finish"))
        return None

    def _extract_task_status(self, task: Any) -> str:
        if isinstance(task, GprTask):
            return task.status or "planned"
        if isinstance(task, Mapping):
            return str(task.get("status") or "planned")
        return "planned"

    def _extract_task_name(self, task: Any) -> str:
        if isinstance(task, GprTask):
            return task.name or ""
        if isinstance(task, Mapping):
            return str(task.get("name") or "")
        return ""

    def _extract_task_is_milestone(self, task: Any) -> bool:
        if isinstance(task, GprTask):
            return bool(task.is_milestone)
        if isinstance(task, Mapping):
            return bool(task.get("is_milestone"))
        return False

    def _extract_task_plan_qty(self, task: Any) -> Optional[float]:
        if isinstance(task, GprTask):
            return safe_float(task.plan_qty)
        if isinstance(task, Mapping):
            return safe_float(task.get("plan_qty"))
        return None

    def _extract_task_id(self, task: Any) -> Optional[int]:
        if isinstance(task, GprTask):
            return int(task.id) if task.id is not None else None
        if isinstance(task, Mapping):
            task_id = task.get("id")
            try:
                return int(task_id) if task_id is not None else None
            except Exception:
                return None
        return None

    def _extract_task_fact_qty(self, task: Any) -> float:
        task_id = self._extract_task_id(task)
        if task_id is None:
            if isinstance(task, GprTask):
                return float(task.fact_qty_total or 0.0)
            if isinstance(task, Mapping):
                return float(safe_float(task.get("fact_qty_total")) or 0.0)
            return 0.0
        return float(self._facts.get(task_id, 0.0))

    def _draw_header(self, total_width: int):
        d0, d1 = self._range
        self.hdr.delete("all")
        self.hdr.configure(scrollregion=(0, 0, total_width, self.HEADER_H))

        if d1 < d0:
            return

        # Месяцы
        cur = date(d0.year, d0.month, 1)
        while cur <= d1:
            month_days = calendar.monthrange(cur.year, cur.month)[1]
            month_start = max(cur, d0)
            month_end = min(date(cur.year, cur.month, month_days), d1)

            x0 = (month_start - d0).days * self.day_px
            x1 = ((month_end - d0).days + 1) * self.day_px

            self.hdr.create_rectangle(x0, 0, x1, self.MONTH_H, fill="#d6dbe0", outline="#bbb")

            if (x1 - x0) > 40:
                self.hdr.create_text(
                    (x0 + x1) / 2,
                    self.MONTH_H / 2,
                    text=cur.strftime("%b %Y"),
                    font=("Segoe UI", 8, "bold"),
                    fill="#333",
                )

            if cur.month == 12:
                cur = date(cur.year + 1, 1, 1)
            else:
                cur = date(cur.year, cur.month + 1, 1)

        # Дни
        days = self._days_count()
        for i in range(days):
            x0 = i * self.day_px
            x1 = x0 + self.day_px
            d = d0 + timedelta(days=i)
            fill = "#ffecec" if d.weekday() >= 5 else "#f3f4f6"
            self.hdr.create_rectangle(x0, self.MONTH_H, x1, self.HEADER_H, fill=fill, outline="#d0d0d0")

            if self.day_px >= 14:
                self.hdr.create_text(
                    (x0 + x1) / 2,
                    self.MONTH_H + self.DAY_H / 2,
                    text=str(d.day),
                    font=("Segoe UI", 7),
                    fill="#555",
                )

        # Линия "сегодня"
        td = today()
        if d0 <= td <= d1:
            tx = (td - d0).days * self.day_px + self.day_px // 2
            self.hdr.create_line(tx, 0, tx, self.HEADER_H, fill=C["error"], width=2)

    def _get_tree_rows_info(self) -> List[Dict[str, Any]]:
        """
        Возвращает список по порядку tree.get_children():
        [
          {
            "iid": "...",
            "y0": int,
            "y1": int,
            "visible": bool,
            "selected": bool,
          },
          ...
        ]
        """
        if self._tree is None:
            info = []
            y = 0
            for idx, _row in enumerate(self._rows):
                y0 = y
                y1 = y + self._row_height_fallback
                info.append(
                    {
                        "iid": f"row:{idx}",
                        "y0": y0,
                        "y1": y1,
                        "visible": True,
                        "selected": False,
                    }
                )
                y = y1
            return info

        result: List[Dict[str, Any]] = []
        try:
            items = list(self._tree.get_children())
            selected = set(self._tree.selection())
            tree_top = self._tree.winfo_rooty()
            canvas_top = self.body.winfo_rooty()
            offset = tree_top - canvas_top

            for iid in items:
                bbox = self._tree.bbox(iid)
                if bbox:
                    y_in_tree = bbox[1]
                    h = bbox[3]
                    y0 = y_in_tree + offset
                    y1 = y0 + h
                    result.append(
                        {
                            "iid": iid,
                            "y0": y0,
                            "y1": y1,
                            "visible": True,
                            "selected": iid in selected,
                        }
                    )
                else:
                    result.append(
                        {
                            "iid": iid,
                            "y0": 0,
                            "y1": 0,
                            "visible": False,
                            "selected": iid in selected,
                        }
                    )
        except Exception:
            # fallback, если tree ещё не готов
            y = 0
            for idx, _row in enumerate(self._rows):
                y0 = y
                y1 = y + self._row_height_fallback
                result.append(
                    {
                        "iid": f"row:{idx}",
                        "y0": y0,
                        "y1": y1,
                        "visible": True,
                        "selected": False,
                    }
                )
                y = y1

        return result

    def _draw_body_grid(self, total_width: int, body_height: int):
        d0, d1 = self._range
        self.body.delete("all")
        self.body.configure(scrollregion=(0, 0, total_width, body_height))

        # Линия "сегодня"
        td = today()
        if d0 <= td <= d1:
            tx = (td - d0).days * self.day_px + self.day_px // 2
            self.body.create_line(tx, 0, tx, body_height, fill=C["error"], width=1, dash=(4, 2))

        # Вертикальная сетка
        days = self._days_count()
        step = 7 if self.day_px >= 10 else 14
        for i in range(0, days, step):
            x = i * self.day_px
            self.body.create_line(x, 0, x, body_height, fill="#eeeeee")

    def _draw_rows(self, total_width: int, rows_info: List[Dict[str, Any]]):
        d0, d1 = self._range
        if d1 < d0:
            return

        for row_index, task in enumerate(self._rows):
            if row_index >= len(rows_info):
                break

            info = rows_info[row_index]
            if not info["visible"]:
                continue

            y0 = info["y0"]
            y1 = info["y1"]

            # подложка строки
            bg = "#ffffff" if row_index % 2 == 0 else "#f8f9fa"
            self.body.create_rectangle(0, y0, total_width, y1, fill=bg, outline="")

            # выделение выбранной строки
            if info.get("selected"):
                self.body.create_rectangle(0, y0, total_width, y1, outline="#90caf9", width=1)

            start = self._extract_task_start(task)
            finish = self._extract_task_finish(task)

            if not isinstance(start, date) or not isinstance(finish, date):
                continue

            if finish < d0 or start > d1:
                continue

            s2 = max(start, d0)
            f2 = min(finish, d1)

            x0 = (s2 - d0).days * self.day_px
            x1 = ((f2 - d0).days + 1) * self.day_px

            status = self._extract_task_status(task)
            fill = status_fill_color(status)

            by0 = y0 + 4
            by1 = y1 - 4

            # milestone
            if self._extract_task_is_milestone(task):
                cx = x0 + 7
                cy = (y0 + y1) / 2
                self.body.create_polygon(
                    cx,
                    cy,
                    cx + 7,
                    cy - 6,
                    cx + 14,
                    cy,
                    cx + 7,
                    cy + 6,
                    fill="#1a73e8",
                    outline="#0d47a1",
                )
            else:
                self.body.create_rectangle(x0 + 1, by0, x1 - 1, by1, fill=fill, outline="#5f6368")

                # прогресс факта
                plan_qty = self._extract_task_plan_qty(task)
                fact_qty = self._extract_task_fact_qty(task)
                if plan_qty and plan_qty > 0 and fact_qty > 0:
                    pct = min(1.0, fact_qty / plan_qty)
                    fw = max(2, int((x1 - x0 - 2) * pct))
                    self.body.create_rectangle(
                        x0 + 1,
                        by0,
                        x0 + 1 + fw,
                        by1,
                        fill="#388e3c",
                        outline="",
                    )

            # название работы поверх бара
            bar_w = x1 - x0
            if bar_w > 60:
                name = self._extract_task_name(task)[:40]
                self.body.create_text(
                    x0 + 4,
                    (y0 + y1) / 2,
                    text=name,
                    anchor="w",
                    font=("Segoe UI", 7),
                    fill="#333",
                )

    # =========================================================
    # Interaction
    # =========================================================

    def _on_body_click(self, event):
        if self._tree is None:
            return

        rows_info = self._get_tree_rows_info()
        target_iid = None
        for info in rows_info:
            if not info.get("visible"):
                continue
            if info["y0"] <= event.y <= info["y1"]:
                target_iid = info["iid"]
                break

        if target_iid:
            try:
                self._tree.selection_set(target_iid)
                self._tree.focus(target_iid)
                self._tree.see(target_iid)
            except Exception:
                pass
            self.after_idle(self.schedule_redraw)

    # =========================================================
    # Main draw
    # =========================================================

    def redraw(self):
        self._redraw_job = None

        d0, d1 = self._range
        if not isinstance(d0, date) or not isinstance(d1, date):
            return
        if d1 < d0:
            return

        total_width = self._total_width()

        rows_info = self._get_tree_rows_info()
        visible_rows = [r for r in rows_info if r.get("visible")]
        body_height = self.body.winfo_height()

        if visible_rows:
            max_y = max(int(r["y1"]) for r in visible_rows)
            body_height = max(body_height, max_y + 20)
        else:
            body_height = max(body_height, max(200, len(self._rows) * self._row_height_fallback + 20))

        self._draw_header(total_width)
        self._draw_body_grid(total_width, body_height)
        self._draw_rows(total_width, rows_info)


__all__ = ["GanttCanvas"]
