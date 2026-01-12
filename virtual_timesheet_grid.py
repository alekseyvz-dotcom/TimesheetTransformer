# virtual_timesheet_grid.py
from __future__ import annotations

import calendar
import tkinter as tk
from datetime import datetime
from tkinter import ttk
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


def month_days(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _fmt_num(v: Optional[float]) -> str:
    if v is None:
        return ""
    s = f"{v:.2f}".rstrip("0").rstrip(".")
    return s


class VirtualTimesheetGrid(tk.Frame):
    """
    Виртуализированный грид табеля на Canvas:
    - рисует только видимые строки
    - не создаёт тысячи Entry/Label
    - редактирование одной ячейки через один Entry поверх Canvas
    - клики по "5/2" и "Удалить" обрабатываются (через колбэки)
    """

    def __init__(
        self,
        master,
        *,
        get_year_month: Callable[[], Tuple[int, int]],
        on_change: Optional[Callable[[], None]] = None,
        on_fill_52: Optional[Callable[[int], None]] = None,
        on_delete_row: Optional[Callable[[int], None]] = None,
        row_height: int = 22,
        colpx: Optional[Dict[str, int]] = None,
        read_only: bool = False,
        allow_row_select: bool = True,
    ):
        super().__init__(master)

        self.get_year_month = get_year_month
        self.on_change = on_change
        self.on_fill_52 = on_fill_52
        self.on_delete_row = on_delete_row

        self.read_only = bool(read_only)
        self.allow_row_select = bool(allow_row_select)

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

        # editor state
        self._editor: Optional[tk.Entry] = None
        self._editor_window_id: Optional[int] = None
        self._editor_var: Optional[tk.StringVar] = None
        self._edit_row: Optional[int] = None
        self._edit_day: Optional[int] = None

        # colors
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

        # wheel (Windows); if you need mac/linux later, can add platform-specific handling
        self.body.bind("<MouseWheel>", self._on_wheel)
        self.body.bind("<Shift-MouseWheel>", self._on_shift_wheel)

        # sync header horizontal scroll with body
        self.body.bind("<Expose>", lambda e: self._sync_header_x())

        self._header_items: List[int] = []
        self._row_items: Dict[int, List[int]] = {}  # row_index -> canvas item ids

        self._cols: List[Tuple[str, int, int, Optional[int]]] = []
        self._total_width: int = 1

        self._build_columns()
        self._draw_header()

    # -------- public API --------

    def set_rows(self, rows: List[Dict[str, Any]]):
        # close editor to avoid committing to wrong indices
        self._end_edit(commit=True)

        self.model_rows = rows or []
        self.selected_indices.clear()

        # clear rendered rows to avoid "ghost" artifacts
        for r, items in list(self._row_items.items()):
            for item in items:
                try:
                    self.body.delete(item)
                except Exception:
                    pass
        self._row_items.clear()

        self._update_scrollregion()
        self._refresh()

    def set_selected_indices(self, indices: Set[int]):
        self.selected_indices = set(indices or set())
        self._refresh()

    def get_selected_indices(self) -> Set[int]:
        return set(self.selected_indices)

    def refresh(self):
        self._refresh()

    def close_editor(self, commit: bool = True):
        self._end_edit(commit=commit)

    # -------- internals --------

    def _build_columns(self):
        # columns: (kind, x0, x1, extra)
        cols: List[Tuple[str, int, int, Optional[int]]] = []
        x = 0

        cols.append(("fio", x, x + self.COLPX["fio"], None))
        x += self.COLPX["fio"]

        cols.append(("tbn", x, x + self.COLPX["tbn"], None))
        x += self.COLPX["tbn"]

        for di in range(31):
            cols.append(("day", x, x + self.COLPX["day"], di))
            x += self.COLPX["day"]

        cols.append(("days", x, x + self.COLPX["days"], None))
        x += self.COLPX["days"]

        cols.append(("hours", x, x + self.COLPX["hours"], None))
        x += self.COLPX["hours"]

        cols.append(("ot_day", x, x + self.COLPX["hours"], None))
        x += self.COLPX["hours"]

        cols.append(("ot_night", x, x + self.COLPX["hours"], None))
        x += self.COLPX["hours"]

        cols.append(("btn52", x, x + self.COLPX["btn52"], None))
        x += self.COLPX["btn52"]

        cols.append(("del", x, x + self.COLPX["del"], None))
        x += self.COLPX["del"]

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
            if kind == "fio":
                text = "ФИО"
            elif kind == "tbn":
                text = "Таб.№"
            elif kind == "day":
                text = str(int(extra) + 1)
            elif kind == "days":
                text = "Дней"
            elif kind == "hours":
                text = "Часы"
            elif kind == "ot_day":
                text = "Пер.день"
            elif kind == "ot_night":
                text = "Пер.ночь"
            elif kind == "btn52":
                text = "5/2"
            elif kind == "del":
                text = "Удалить"
            else:
                text = ""

            r = self.header.create_rectangle(
                x0, y0, x1, y1, fill=self.HEADER_BG, outline="#b0b0b0"
            )
            t = self.header.create_text(
                (x0 + x1) / 2, (y0 + y1) / 2, text=text, anchor="center"
            )
            self._header_items.extend([r, t])

    def _update_scrollregion(self):
        h = max(1, len(self.model_rows)) * self.row_height
        self.body.configure(scrollregion=(0, 0, self._total_width, h))
        self.header.configure(scrollregion=(0, 0, self._total_width, 0))

    def _yview(self, *args):
        self._end_edit(commit=True)
        self.body.yview(*args)
        self._refresh()

    def _xview(self, *args):
        self._end_edit(commit=True)
        self.body.xview(*args)
        self._sync_header_x()

    def _sync_header_x(self):
        try:
            first, _last = self.body.xview()
            self.header.xview_moveto(first)
        except Exception:
            pass

    def _on_wheel(self, event):
        self._end_edit(commit=True)
        self.body.yview_scroll(int(-1 * (event.delta / 120)), "units")
        self._refresh()
        return "break"

    def _on_shift_wheel(self, event):
        self._end_edit(commit=True)
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
            self._end_edit(commit=True)
            return
    
        if not col:
            self._end_edit(commit=True)
            return
    
        kind, extra = col
    
        # Если кликнули по другой ячейке — сначала коммитим текущий редактор
        # (но ниже для day мы ещё откроем новый редактор)
        if self._editor:
            self._end_edit(commit=True)
    
        # "buttons" in cells
        if kind == "btn52":
            if callable(self.on_fill_52) and not self.read_only:
                self.on_fill_52(row)
            return
    
        if kind == "del":
            if callable(self.on_delete_row) and not self.read_only:
                self.on_delete_row(row)
            return
    
        # Редактирование по 1 клику
        if kind == "day" and not self.read_only:
            day_index = int(extra)
            y, m = self.get_year_month()
            if (day_index + 1) <= month_days(y, m):
                self._begin_edit_day(row, day_index)
            return
    
        # row selection by clicking FIO/TBN
        if self.allow_row_select and kind in ("fio", "tbn"):
            if row in self.selected_indices:
                self.selected_indices.remove(row)
            else:
                self.selected_indices.add(row)
            self._refresh()

    def _on_double_click(self, event):
        if self.read_only:
            return

        row, col = self._hit_test(event.x, event.y)
        if row is None or not col:
            return

        kind, extra = col
        if kind != "day":
            return

        day_index = int(extra)
        y, m = self.get_year_month()
        if (day_index + 1) > month_days(y, m):
            return  # disabled day

        self._begin_edit_day(row, day_index)

    def _cell_bbox(self, row_index: int, kind: str, extra: Optional[int]) -> Optional[Tuple[int, int, int, int]]:
        x0 = x1 = None
        for k, cx0, cx1, ex in self._cols:
            if k == kind and (extra is None or ex == extra):
                x0, x1 = cx0, cx1
                break
        if x0 is None:
            return None
        y0 = row_index * self.row_height
        y1 = y0 + self.row_height
        return (x0, y0, x1, y1)

    def _begin_edit_day(self, row_index: int, day_index: int):
        # close previous editor
        self._end_edit(commit=True)

        bbox = self._cell_bbox(row_index, "day", day_index)
        if not bbox:
            return
        x0, y0, x1, y1 = bbox

        rec = self.model_rows[row_index]

        hours = rec.get("hours") or [None] * 31
        if len(hours) < 31:
            hours = (hours + [None] * 31)[:31]
            rec["hours"] = hours

        cur_val = hours[day_index] or ""

        self._edit_row = row_index
        self._edit_day = day_index

        self._editor_var = tk.StringVar(value=str(cur_val))
        self._editor = tk.Entry(self.body, textvariable=self._editor_var, justify="center")

        def _cancel(ev):
            self._end_edit(commit=False)
            return "break"
        
        self._editor.bind("<Return>", lambda e: (self._commit_and_move(dr=1, dc=0)))
        self._editor.bind("<Tab>", lambda e: (self._commit_and_move(dr=0, dc=1)))
        self._editor.bind("<Shift-Tab>", lambda e: (self._commit_and_move(dr=0, dc=-1)))
        
        self._editor.bind("<Left>",  lambda e: (self._commit_and_move(dr=0, dc=-1)))
        self._editor.bind("<Right>", lambda e: (self._commit_and_move(dr=0, dc=1)))
        self._editor.bind("<Up>",    lambda e: (self._commit_and_move(dr=-1, dc=0)))
        self._editor.bind("<Down>",  lambda e: (self._commit_and_move(dr=1, dc=0)))
        
        self._editor.bind("<Escape>", _cancel)
        self._editor.bind("<FocusOut>", lambda e: self._end_edit(commit=True))

        self._editor_window_id = self.body.create_window(
            x0 + 1,
            y0 + 1,
            width=max(4, x1 - x0 - 2),
            height=max(4, y1 - y0 - 2),
            anchor="nw",
            window=self._editor,
        )

        self._editor.focus_set()
        try:
            self._editor.selection_range(0, "end")
        except Exception:
            pass

    def _end_edit(self, commit: bool):
        if not self._editor:
            return

        row_index = self._edit_row
        day_index = self._edit_day

        # read value before destroying
        new_val = ""
        try:
            if self._editor_var is not None:
                new_val = (self._editor_var.get() or "").strip()
        except Exception:
            new_val = ""

        # destroy editor UI
        try:
            if self._editor_window_id is not None:
                self.body.delete(self._editor_window_id)
        except Exception:
            pass
        try:
            self._editor.destroy()
        except Exception:
            pass

        self._editor = None
        self._editor_window_id = None
        self._editor_var = None
        self._edit_row = None
        self._edit_day = None

        if not commit:
            return
        if row_index is None or day_index is None:
            return
        if not (0 <= row_index < len(self.model_rows)):
            return
        if not (0 <= day_index < 31):
            return

        rec = self.model_rows[row_index]
        hours = rec.get("hours") or [None] * 31
        if len(hours) < 31:
            hours = (hours + [None] * 31)[:31]

        hours[day_index] = (new_val if new_val else None)
        rec["hours"] = hours

        # re-draw row
        self._draw_row(row_index)

        # notify
        if callable(self.on_change):
            self.on_change()

    def _refresh(self):
        # visible area
        try:
            y0 = int(self.body.canvasy(0))
            h = int(self.body.winfo_height())
        except Exception:
            return

        first_row = max(0, y0 // self.row_height)
        visible = max(1, h // self.row_height + 2)
        last_row = min(len(self.model_rows), first_row + visible)

        # delete rendered rows out of range
        for r in list(self._row_items.keys()):
            if r < first_row or r >= last_row:
                for item in self._row_items[r]:
                    try:
                        self.body.delete(item)
                    except Exception:
                        pass
                del self._row_items[r]

        # draw rows in range
        for r in range(first_row, last_row):
            self._draw_row(r)

        self._sync_header_x()

    def _commit_and_move(self, dr: int, dc: int):
        """
        Commit текущей ячейки и открыть редактор в соседней (dr/dc).
        dc двигает по дням, dr по строкам.
        """
        row = self._edit_row
        day = self._edit_day
        if row is None or day is None:
            return "break"
    
        # Сохраним значение
        self._end_edit(commit=True)
    
        new_row = row + dr
        new_day = day + dc
    
        if new_row < 0:
            new_row = 0
        if new_row >= len(self.model_rows):
            new_row = len(self.model_rows) - 1 if self.model_rows else 0
    
        if new_day < 0:
            new_day = 0
        if new_day > 30:
            new_day = 30
    
        # не открывать disabled день (после конца месяца) — откатимся назад пока не попадём в валидный
        y, m = self.get_year_month()
        dim = month_days(y, m)
        while new_day + 1 > dim and new_day > 0:
            new_day -= 1
    
        if self.model_rows:
            self._begin_edit_day(new_row, new_day)
    
        return "break"

    def _bg_for_day(self, year: int, month: int, day: int) -> str:
        wd = datetime(year, month, day).weekday()
        if wd == 5:
            return self.WEEK_BG_SAT
        if wd == 6:
            return self.WEEK_BG_SUN
        return "white"

    def _draw_row(self, row_index: int):
        # redraw whole row (ok for visible-only virtualization)
        if row_index in self._row_items:
            for item in self._row_items[row_index]:
                try:
                    self.body.delete(item)
                except Exception:
                    pass

        if not (0 <= row_index < len(self.model_rows)):
            return

        rec = self.model_rows[row_index]

        y0 = row_index * self.row_height
        y1 = y0 + self.row_height

        zebra = self.ZEBRA_EVEN if (row_index % 2 == 0) else self.ZEBRA_ODD
        selected = (row_index in self.selected_indices)

        y, m = self.get_year_month()
        dim = month_days(y, m)

        items: List[int] = []

        fio = (rec.get("fio") or "")
        tbn = (rec.get("tbn") or "")

        hours = rec.get("hours") or [None] * 31
        if len(hours) < 31:
            hours = (hours + [None] * 31)[:31]
            rec["hours"] = hours

        totals = rec.get("_totals") if isinstance(rec.get("_totals"), dict) else {}
        totals_days = totals.get("days")
        totals_hours = totals.get("hours")
        totals_ot_day = totals.get("ot_day")
        totals_ot_night = totals.get("ot_night")

        for kind, x0, x1, extra in self._cols:
            fill = self.SELECT_BG if selected else zebra

            text = ""
            anchor = "w"
            tx: float = x0 + 4

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
            elif kind == "ot_day":
                text = "" if totals_ot_day is None else str(totals_ot_day)
                anchor = "e"
                tx = x1 - 4
            elif kind == "ot_night":
                text = "" if totals_ot_night is None else str(totals_ot_night)
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

            r_id = self.body.create_rectangle(
                x0, y0, x1, y1, fill=fill, outline="#e0e0e0"
            )
            t_id = self.body.create_text(
                tx, (y0 + y1) / 2, text=text, anchor=anchor, fill="#111"
            )
            items.extend([r_id, t_id])

        self._row_items[row_index] = items
