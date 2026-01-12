# virtual_timesheet_grid.py
from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import calendar
from datetime import datetime


def month_days(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


class VirtualTimesheetGrid(tk.Frame):
    """
    Виртуализированный грид табеля на Canvas:
    - рисует только видимые строки
    - не создаёт тысячи Entry/Label
    """

    def __init__(
        self,
        master,
        *,
        get_year_month: Callable[[], Tuple[int, int]],
        on_change: Optional[Callable[[], None]] = None,
        row_height: int = 22,
        colpx: Optional[Dict[str, int]] = None,
    ):
        super().__init__(master)

        self.get_year_month = get_year_month
        self.on_change = on_change
        self.row_height = int(row_height)

        self.COLPX = colpx or {
            "fio": 200,
            "tbn": 100,
            "day": 36,
            "days": 46,
            "hours": 56,
            "btn52": 40,
            "del": 66,
        }

        self.model_rows: List[Dict[str, Any]] = []
        self.selected_indices: Set[int] = set()

        self.HEADER_BG = "#d0d0d0"
        self.ZEBRA_EVEN = "#ffffff"
        self.ZEBRA_ODD = "#f6f8fa"
        self.SELECT_BG = "#c5e1ff"
        self.DISABLED_BG = "#f0f0f0"
        self.WEEK_BG_SAT = "#fff8e1"
        self.WEEK_BG_SUN = "#ffebee"

        # --- UI: header canvas + body canvas + scrollbars ---
        self.header = tk.Canvas(self, height=26, highlightthickness=0)
        self.body = tk.Canvas(self, highlightthickness=0)
        self.vsb = ttk.Scrollbar(self, orient="vertical", command=self._yview)
        self.hsb = ttk.Scrollbar(self, orient="horizontal", command=self._xview)

        self.header.grid(row=0, column=0, sticky="ew")
        self.body.grid(row=1, column=0, sticky="nsew")
        self.vsb.grid(row=1, column=1, sticky="ns")
        self.hsb.grid(row=2, column=0, sticky="ew")

        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.body.configure(yscrollcommand=self.vsb.set, xscrollcommand=self.hsb.set)

        # events
        self.body.bind("<Configure>", lambda e: self._refresh())
        self.body.bind("<Button-1>", self._on_click)
        self.body.bind("<MouseWheel>", self._on_wheel)
        self.body.bind("<Shift-MouseWheel>", self._on_shift_wheel)

        # sync header horizontal scroll with body
        self.body.bind("<Expose>", lambda e: self._sync_header_x())

        self._header_items: List[int] = []
        self._row_items: Dict[int, List[int]] = {}  # row_index -> canvas item ids

        self._build_columns()
        self._draw_header()

    # -------- public API --------

    def set_rows(self, rows: List[Dict[str, Any]]):
        self.model_rows = rows or []
        self.selected_indices.clear()
        self._update_scrollregion()
        self._refresh()

    def set_selected_indices(self, indices: Set[int]):
        self.selected_indices = set(indices or set())
        self._refresh()

    def get_selected_indices(self) -> Set[int]:
        return set(self.selected_indices)

    def refresh(self):
        self._refresh()

    # -------- internals --------

    def _build_columns(self):
        # columns: (kind, x0, x1, extra)
        cols = []
        x = 0
        cols.append(("fio", x, x + self.COLPX["fio"], None)); x += self.COLPX["fio"]
        cols.append(("tbn", x, x + self.COLPX["tbn"], None)); x += self.COLPX["tbn"]
        for di in range(31):
            cols.append(("day", x, x + self.COLPX["day"], di)); x += self.COLPX["day"]
        cols.append(("days", x, x + self.COLPX["days"], None)); x += self.COLPX["days"]
        cols.append(("hours", x, x + self.COLPX["hours"], None)); x += self.COLPX["hours"]
        cols.append(("ot_day", x, x + self.COLPX["hours"], None)); x += self.COLPX["hours"]
        cols.append(("ot_night", x, x + self.COLPX["hours"], None)); x += self.COLPX["hours"]
        cols.append(("btn52", x, x + self.COLPX["btn52"], None)); x += self.COLPX["btn52"]
        cols.append(("del", x, x + self.COLPX["del"], None)); x += self.COLPX["del"]

        self._cols = cols
        self._total_width = x

        self.header.configure(scrollregion=(0, 0, self._total_width, 0))
        self.body.configure(scrollregion=(0, 0, self._total_width, 0))

    def _draw_header(self):
        for item in self._header_items:
            try:
                self.header.delete(item)
            except Exception:
                pass
        self._header_items.clear()

        y0, y1 = 0, 26
        for kind, x0, x1, extra in self._cols:
            text = ""
            if kind == "fio": text = "ФИО"
            elif kind == "tbn": text = "Таб.№"
            elif kind == "day": text = str(extra + 1)
            elif kind == "days": text = "Дней"
            elif kind == "hours": text = "Часы"
            elif kind == "ot_day": text = "Пер.день"
            elif kind == "ot_night": text = "Пер.ночь"
            elif kind == "btn52": text = "5/2"
            elif kind == "del": text = "Удалить"

            r = self.header.create_rectangle(x0, y0, x1, y1, fill=self.HEADER_BG, outline="#b0b0b0")
            t = self.header.create_text((x0 + x1) / 2, (y0 + y1) / 2, text=text, anchor="center")
            self._header_items.extend([r, t])

    def _update_scrollregion(self):
        h = max(1, len(self.model_rows)) * self.row_height
        self.body.configure(scrollregion=(0, 0, self._total_width, h))
        self.header.configure(scrollregion=(0, 0, self._total_width, 0))

    def _yview(self, *args):
        self.body.yview(*args)
        self._refresh()

    def _xview(self, *args):
        self.body.xview(*args)
        self._sync_header_x()

    def _sync_header_x(self):
        try:
            first, last = self.body.xview()
            self.header.xview_moveto(first)
        except Exception:
            pass

    def _on_wheel(self, event):
        # Windows delta=120 step
        self.body.yview_scroll(int(-1 * (event.delta / 120)), "units")
        self._refresh()
        return "break"

    def _on_shift_wheel(self, event):
        self.body.xview_scroll(int(-1 * (event.delta / 120)), "units")
        self._sync_header_x()
        return "break"

    def _hit_test(self, x: int, y: int) -> Tuple[Optional[int], Optional[Tuple[str, Optional[int]]]]:
        cy = int(self.body.canvasy(y))
        cx = int(self.body.canvasx(x))
        row = cy // self.row_height
        if row < 0 or row >= len(self.model_rows):
            return None, None

        for kind, x0, x1, extra in self._cols:
            if x0 <= cx < x1:
                if kind == "day":
                    return row, ("day", int(extra))
                return row, (kind, None)
        return row, None

    def _on_click(self, event):
        row, col = self._hit_test(event.x, event.y)
        if row is None:
            return

        # выделение строки по клику (как у тебя сейчас по ФИО/ТБН)
        if col and col[0] in ("fio", "tbn"):
            if row in self.selected_indices:
                self.selected_indices.remove(row)
            else:
                self.selected_indices.add(row)
            self._refresh()

    def _refresh(self):
        # видимая область
        try:
            y0 = int(self.body.canvasy(0))
            h = int(self.body.winfo_height())
        except Exception:
            return

        first_row = max(0, y0 // self.row_height)
        visible = max(1, h // self.row_height + 2)
        last_row = min(len(self.model_rows), first_row + visible)

        # удалить отрисованные строки вне диапазона
        for r in list(self._row_items.keys()):
            if r < first_row or r >= last_row:
                for item in self._row_items[r]:
                    try:
                        self.body.delete(item)
                    except Exception:
                        pass
                del self._row_items[r]

        # дорисовать нужные строки
        for r in range(first_row, last_row):
            self._draw_row(r)

        self._sync_header_x()

    def _bg_for_day(self, year: int, month: int, day: int) -> str:
        wd = datetime(year, month, day).weekday()
        if wd == 5:
            return self.WEEK_BG_SAT
        if wd == 6:
            return self.WEEK_BG_SUN
        return "white"

    def _draw_row(self, row_index: int):
        # перерисуем строку полностью (пока так; потом оптимизируем точечно)
        if row_index in self._row_items:
            for item in self._row_items[row_index]:
                try:
                    self.body.delete(item)
                except Exception:
                    pass

        rec = self.model_rows[row_index]
        y0 = row_index * self.row_height
        y1 = y0 + self.row_height

        zebra = self.ZEBRA_EVEN if (row_index % 2 == 0) else self.ZEBRA_ODD
        selected = (row_index in self.selected_indices)

        y, m = self.get_year_month()
        dim = month_days(y, m)

        items: List[int] = []

        # values
        fio = (rec.get("fio") or "")
        tbn = (rec.get("tbn") or "")
        hours = rec.get("hours") or [None] * 31
        if len(hours) < 31:
            hours = (hours + [None] * 31)[:31]

        # TODO: totals рисовать из rec["_totals"] когда внедрим инкрементальные итоги
        # пока пусто
        totals_days = rec.get("_totals", {}).get("days") if isinstance(rec.get("_totals"), dict) else ""
        totals_hours = rec.get("_totals", {}).get("hours") if isinstance(rec.get("_totals"), dict) else ""

        for kind, x0, x1, extra in self._cols:
            fill = self.SELECT_BG if selected else zebra

            text = ""
            anchor = "w"
            tx = x0 + 4

            if kind == "fio":
                text = fio
            elif kind == "tbn":
                text = tbn
                anchor = "center"
                tx = (x0 + x1) / 2
            elif kind == "day":
                di = int(extra)
                day_num = di + 1
                if day_num > dim:
                    fill = self.DISABLED_BG
                    text = ""
                else:
                    # фон выходных
                    fill = self.SELECT_BG if selected else self._bg_for_day(y, m, day_num)
                    v = hours[di]
                    text = "" if v is None else str(v)
                    anchor = "center"
                    tx = (x0 + x1) / 2
            elif kind == "days":
                text = "" if totals_days is None else str(totals_days)
                anchor = "e"
                tx = x1 - 4
            elif kind == "hours":
                text = "" if totals_hours is None else str(totals_hours)
                anchor = "e"
                tx = x1 - 4
            elif kind == "btn52":
                text = "5/2"
                anchor = "center"
                tx = (x0 + x1) / 2
            elif kind == "del":
                text = "Удалить"
                anchor = "center"
                tx = (x0 + x1) / 2

            r_id = self.body.create_rectangle(x0, y0, x1, y1, fill=fill, outline="#e0e0e0")
            t_id = self.body.create_text(tx, (y0 + y1) / 2, text=text, anchor=anchor, fill="#111")
            items.extend([r_id, t_id])

        self._row_items[row_index] = items
