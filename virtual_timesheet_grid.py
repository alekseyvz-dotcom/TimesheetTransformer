from __future__ import annotations

import calendar
import platform
import tkinter as tk
import tkinter.font as tkfont
from datetime import date
from tkinter import ttk
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

OS_NAME = platform.system()


def month_days(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


class VirtualTimesheetGrid(tk.Frame):
    """
    Виртуальный грид табеля.

    Особенности:
    - рисует только видимые строки;
    - работает на Canvas, что быстрее для больших таблиц;
    - поддерживает редактирование ячеек по дням;
    - поддерживает выделение строк;
    - синхронизирует горизонтальный скролл шапки и тела;
    - устойчиво работает внутри ttk.Notebook / вкладок.
    """

    def __init__(
        self,
        master,
        *,
        get_year_month: Callable[[], Tuple[int, int]],
        on_change: Optional[Callable[[int, int], None]] = None,
        on_delete_row: Optional[Callable[[int], None]] = None,
        on_selection_change: Optional[Callable[[Set[int]], None]] = None,
        on_trip_period_click: Optional[Callable[[int], None]] = None,
        row_height: int = 22,
        colpx: Optional[Dict[str, int]] = None,
        read_only: bool = False,
        allow_row_select: bool = True,
        show_trip_period: bool = False,
    ):
        super().__init__(master, bg="#ffffff")

        self.get_year_month = get_year_month
        self.on_change = on_change
        self.on_delete_row = on_delete_row
        self.on_selection_change = on_selection_change
        self.on_trip_period_click = on_trip_period_click

        self.read_only = bool(read_only)
        self.allow_row_select = bool(allow_row_select)
        self.row_height = int(row_height)
        self.show_trip_period = bool(show_trip_period)

        self.COLPX = colpx or {
            "fio": 200,
            "tbn": 100,
            "trip": 74,
            "day": 36,
            "days": 46,
            "hours": 56,
            "ot_day": 60,
            "ot_night": 60,
            "del": 66,
        }

        self.model_rows: List[Dict[str, Any]] = []
        self.selected_indices: Set[int] = set()

        self._cached_ym: Tuple[int, int] = (0, 0)
        self._weekend_map: Dict[int, str] = {}
        self._rendered_rows: Set[int] = set()

        self._editor: Optional[tk.Entry] = None
        self._editor_window_id: Optional[int] = None
        self._editor_var: Optional[tk.StringVar] = None
        self._edit_row: Optional[int] = None
        self._edit_day: Optional[int] = None

        self._tooltip: Optional[tk.Toplevel] = None
        self._tooltip_label: Optional[tk.Label] = None
        self._tooltip_row: Optional[int] = None

        self.show_schedule_highlight: bool = False

        # Цвета в стилистике новой оболочки
        self.BORDER = "#c9d3df"
        self.HEADER_BG = "#e7edf4"
        self.HEADER_TEXT = "#1f2937"
        self.ZEBRA_EVEN = "#ffffff"
        self.ZEBRA_ODD = "#f8fbfd"
        self.SELECT_BG = "#d7e8fb"
        self.DISABLED_BG = "#eef2f6"
        self.WEEK_BG_SAT = "#fff8e8"
        self.WEEK_BG_SUN = "#fff1f1"
        self.TEXT = "#1f2937"
        self.MUTED = "#6b7280"
        self.DELETE_BG = "#fbe9e7"
        self.DELETE_TEXT = "#9a3412"
        self.SCHEDULE_WORK_BG = "#e8f5e9"
        self.SCHEDULE_OFF_BG = "#eef2f7"
        self.SCHEDULE_MISSING_BG = "#fff8db"
        self.SCHEDULE_EXTRA_BG = "#fdeaea"

        self.font_header = tkfont.Font(family="Segoe UI", size=9, weight="bold")
        self.font_cell = tkfont.Font(family="Segoe UI", size=9)
        self.font_small = tkfont.Font(family="Segoe UI", size=8)

        self.header = tk.Canvas(
            self,
            height=28,
            highlightthickness=0,
            bg=self.HEADER_BG,
            bd=0,
            relief="flat",
        )
        self.body = tk.Canvas(
            self,
            highlightthickness=0,
            bg="#ffffff",
            bd=0,
            relief="flat",
        )

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

        self.body.bind("<Configure>", self._on_body_configure)
        self.body.bind("<Button-1>", self._on_click)
        self.body.bind("<Motion>", self._on_mouse_move)
        self.body.bind("<Leave>", self._on_mouse_leave)
        self.body.bind("<Map>", lambda _e: self.after(60, self.refresh))

        if OS_NAME == "Linux":
            self.body.bind("<Button-4>", lambda e: self._scroll_generic(e, -1, "y"))
            self.body.bind("<Button-5>", lambda e: self._scroll_generic(e, 1, "y"))
            self.body.bind("<Shift-Button-4>", lambda e: self._scroll_generic(e, -1, "x"))
            self.body.bind("<Shift-Button-5>", lambda e: self._scroll_generic(e, 1, "x"))
        else:
            self.body.bind("<MouseWheel>", self._on_wheel)
            self.body.bind("<Shift-MouseWheel>", self._on_shift_wheel)

        self._cols: List[Tuple[str, int, int, Optional[int]]] = []
        self._total_width: int = 1

        self._build_columns()
        self._update_weekends_cache()
        self._draw_header()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_rows(self, rows: List[Dict[str, Any]]):
        self._hide_tooltip()
        self._end_edit(commit=True)
        self.model_rows = rows or []
    
        # Оставляем только валидные индексы для текущего набора строк
        self.selected_indices = {i for i in self.selected_indices if 0 <= i < len(self.model_rows)}
    
        self.body.delete("all")
        self._rendered_rows.clear()
    
        self._update_weekends_cache()
        self._update_scrollregion()
        self._draw_header()
        self.after_idle(lambda: self._refresh(force_redraw=True))

    def set_selected_indices(self, indices: Set[int]):
        self.selected_indices = {i for i in (indices or set()) if 0 <= i < len(self.model_rows)}
        self._refresh(force_redraw=True)
        self._notify_selection_change()

    def get_selected_indices(self) -> Set[int]:
        return set(self.selected_indices)

    def set_schedule_highlight_enabled(self, enabled: bool):
        self.show_schedule_highlight = bool(enabled)
        self.refresh()

    def refresh(self):
        self._hide_tooltip()
        self._update_weekends_cache()
        self._update_scrollregion()
        self._draw_header()
        self._refresh(force_redraw=True)

    def close_editor(self, commit: bool = True):
        self._end_edit(commit=commit)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _on_body_configure(self, _event=None):
        self._refresh()

    def _update_weekends_cache(self):
        try:
            y, m = self.get_year_month()
        except Exception:
            return

        if (y, m) == self._cached_ym:
            return

        self._cached_ym = (y, m)
        self._weekend_map.clear()

        dim = month_days(y, m)
        for d in range(1, dim + 1):
            wd = date(y, m, d).weekday()
            if wd == 5:
                self._weekend_map[d - 1] = self.WEEK_BG_SAT
            elif wd == 6:
                self._weekend_map[d - 1] = self.WEEK_BG_SUN

    def _build_columns(self):
        cols: List[Tuple[str, int, int, Optional[int]]] = []
        x = 0

        def add(kind: str, width: int, extra: Optional[int] = None):
            nonlocal x
            cols.append((kind, x, x + width, extra))
            x += width

        add("fio", int(self.COLPX["fio"]))
        add("tbn", int(self.COLPX["tbn"]))
        
        if self.show_trip_period:
            add("trip", int(self.COLPX.get("trip", 74)))
        
        for di in range(31):
            add("day", int(self.COLPX["day"]), di)

        add("days", int(self.COLPX.get("days", 52)))
        add("hours", int(self.COLPX.get("hours", 58)))
        add("ot_day", int(self.COLPX.get("ot_day", self.COLPX.get("hours", 58))))
        add("ot_night", int(self.COLPX.get("ot_night", self.COLPX.get("hours", 58))))
        add("del", int(self.COLPX["del"]))

        self._cols = cols
        self._total_width = x

        self.header.configure(scrollregion=(0, 0, self._total_width, 28))
        body_h = max(1, len(self.model_rows)) * self.row_height
        self.body.configure(scrollregion=(0, 0, self._total_width, body_h))

    def _draw_header(self):
        self.header.delete("all")
        self._update_weekends_cache()

        labels = {
            "fio": "ФИО",
            "tbn": "Таб.№",
            "trip": "КМ",
            "days": "Дней",
            "hours": "Часы",
            "ot_day": "Пер.день",
            "ot_night": "Пер.ночь",
            "del": "Удалить",
        }

        try:
            y, m = self._cached_ym
            dim = month_days(y, m) if y and m else 31
        except Exception:
            dim = 31

        for kind, x0, x1, extra in self._cols:
            bg = self.HEADER_BG
            text = labels.get(kind, "")

            if kind == "day":
                di = int(extra)
                day_num = di + 1
                text = str(day_num)
                if day_num > dim:
                    bg = self.DISABLED_BG
                else:
                    bg = self._weekend_map.get(di, self.HEADER_BG)

            self.header.create_rectangle(
                x0,
                0,
                x1,
                28,
                fill=bg,
                outline=self.BORDER,
            )
            self.header.create_text(
                (x0 + x1) / 2,
                14,
                text=text,
                font=self.font_header if kind != "day" else self.font_cell,
                fill=self.HEADER_TEXT,
                anchor="center",
            )

        try:
            self.header.xview_moveto(self.body.xview()[0])
        except Exception:
            pass

    def _update_scrollregion(self):
        h = max(1, len(self.model_rows)) * self.row_height
        self.body.configure(scrollregion=(0, 0, self._total_width, h))

    def _clip_text(self, text: str, max_px: int, font: tkfont.Font) -> str:
        text = str(text or "")
        if not text:
            return ""
        if font.measure(text) <= max_px:
            return text
        ell = "…"
        max_px = max(0, max_px - font.measure(ell))
        if max_px <= 0:
            return ell
        result = text
        while result and font.measure(result) > max_px:
            result = result[:-1]
        return result + ell

    def _notify_selection_change(self):
        if callable(self.on_selection_change):
            try:
                self.on_selection_change(set(self.selected_indices))
            except Exception:
                pass

    def _get_schedule_cell_bg(
        self,
        rec: Dict[str, Any],
        day_num: int,
        day_index: int,
        selected: bool,
        base_bg: str,
        cell_value: Any,
    ) -> str:
        if selected:
            return self.SELECT_BG

        if not self.show_schedule_highlight:
            return self._weekend_map.get(day_index, base_bg)

        schedule_map = rec.get("schedule_days_map") or {}
        if not isinstance(schedule_map, dict):
            return self._weekend_map.get(day_index, base_bg)

        day_info = schedule_map.get(day_num)
        if not isinstance(day_info, dict):
            return self._weekend_map.get(day_index, base_bg)

        is_workday = bool(day_info.get("is_workday"))
        has_value = cell_value is not None and str(cell_value).strip() != ""

        # Рабочий день по графику, но часов нет в табеле
        if is_workday and not has_value:
            return self.SCHEDULE_MISSING_BG

        # Выходной по графику, но часы в табеле есть
        if (not is_workday) and has_value:
            return self.SCHEDULE_EXTRA_BG

        return self.SCHEDULE_WORK_BG if is_workday else self.SCHEDULE_OFF_BG 

    def _show_tooltip(self, text: str, x_root: int, y_root: int):
        text = str(text or "").strip()
        if not text:
            self._hide_tooltip()
            return

        if self._tooltip is None or not self._tooltip.winfo_exists():
            self._tooltip = tk.Toplevel(self)
            self._tooltip.wm_overrideredirect(True)
            self._tooltip.attributes("-topmost", True)

            self._tooltip_label = tk.Label(
                self._tooltip,
                text=text,
                justify="left",
                bg="#fffedb",
                fg="#1f2937",
                relief="solid",
                bd=1,
                padx=8,
                pady=4,
                font=self.font_small,
            )
            self._tooltip_label.pack()
        else:
            if self._tooltip_label is not None:
                self._tooltip_label.config(text=text)

        self._tooltip.geometry(f"+{x_root + 14}+{y_root + 12}")

    def _hide_tooltip(self):
        self._tooltip_row = None
        if self._tooltip is not None:
            try:
                self._tooltip.destroy()
            except Exception:
                pass
        self._tooltip = None
        self._tooltip_label = None

    def _on_mouse_move(self, event):
        row_index, col_data = self._hit_test(event.x, event.y)
    
        if row_index is None or not col_data:
            self._hide_tooltip()
            return
    
        kind, _extra = col_data
    
        if not (0 <= row_index < len(self.model_rows)):
            self._hide_tooltip()
            return
    
        rec = self.model_rows[row_index]
    
        if kind == "fio":
            schedule = str(rec.get("work_schedule") or "").strip()
            if not schedule:
                schedule = "не указан"
            text = f"График: {schedule}"
            self._tooltip_row = row_index
            self._show_tooltip(text, event.x_root, event.y_root)
            return
    
        if kind == "trip" and self.show_trip_period:
            text = self._format_trip_period_full(rec)
            self._tooltip_row = row_index
            self._show_tooltip(text, event.x_root, event.y_root)
            return
    
        self._hide_tooltip()

    def _on_mouse_leave(self, _event):
        self._hide_tooltip()

    def _format_trip_period_short(self, rec: Dict[str, Any]) -> str:
        periods = rec.get("trip_periods", [])
        
        # Если периодов нет (или остался старый формат без конвертации)
        if not periods:
            # На всякий случай fallback для старых данных, если они просочатся
            if rec.get("trip_date_from") and rec.get("trip_date_to"):
                periods = [{"from": rec["trip_date_from"], "to": rec["trip_date_to"]}]
            else:
                return ""

        try:
            year, month = self.get_year_month()
        except Exception:
            year, month = 0, 0

        # Если командировка всего одна в месяце, выводим как раньше
        if len(periods) == 1:
            d_from = periods[0].get("from")
            d_to = periods[0].get("to")
            
            if d_from and d_to:
                same_month = (
                    d_from.year == year and d_from.month == month and
                    d_to.year == year and d_to.month == month
                )
                if same_month:
                    return f"{d_from.day:02d}-{d_to.day:02d}"
                return f"{d_from.strftime('%d.%m')}-{d_to.strftime('%d.%m')}"

        # Если периодов несколько, пишем их количество
        return f"{len(periods)} пер."
    
    
    def _format_trip_period_full(self, rec: Dict[str, Any]) -> str:
        periods = rec.get("trip_periods", [])
        
        if not periods:
            # Fallback
            if rec.get("trip_date_from") and rec.get("trip_date_to"):
                periods = [{"from": rec["trip_date_from"], "to": rec["trip_date_to"]}]
            else:
                return "Период командировки не задан"

        parts = []
        for p in periods:
            d_from = p.get("from")
            d_to = p.get("to")
            left = d_from.strftime("%d.%m.%Y") if d_from else "—"
            right = d_to.strftime("%d.%m.%Y") if d_to else "—"
            parts.append(f"с {left} по {right}")

        if len(parts) == 1:
            return f"Командировка: {parts[0]}"
        
        return "Командировки:\n" + "\n".join(parts)
    
    def _is_trip_day(self, rec: Dict[str, Any], day_num: int) -> bool:
        periods = rec.get("trip_periods", [])
        
        # Fallback для старых данных
        if not periods and rec.get("trip_date_from") and rec.get("trip_date_to"):
            periods = [{"from": rec["trip_date_from"], "to": rec["trip_date_to"]}]
            
        if not periods:
            return False

        try:
            year, month = self.get_year_month()
            cur = date(year, month, day_num)
        except Exception:
            return False

        # Проверяем вхождение даты во все существующие периоды
        for p in periods:
            d_from = p.get("from")
            d_to = p.get("to")
            if d_from and d_to and (d_from <= cur <= d_to):
                return True

        return False    
    
    
    def _get_trip_cell_bg(
        self,
        rec: Dict[str, Any],
        day_num: int,
        day_index: int,
        selected: bool,
        base_bg: str,
        cell_value: Any,
    ) -> str:
        if selected:
            return self.SELECT_BG
    
        trip_bg = "#e7f1ff"
        in_trip = self._is_trip_day(rec, day_num)
    
        if self.show_schedule_highlight:
            base = self._get_schedule_cell_bg(
                rec=rec,
                day_num=day_num,
                day_index=day_index,
                selected=selected,
                base_bg=base_bg,
                cell_value=cell_value,
            )
            if in_trip:
                if base in (self.SCHEDULE_MISSING_BG, self.SCHEDULE_EXTRA_BG):
                    return base
                return trip_bg
            return base
    
        if in_trip:
            return trip_bg
    
        return self._weekend_map.get(day_index, base_bg)

    # ------------------------------------------------------------------
    # Scrolling
    # ------------------------------------------------------------------

    def _yview(self, *args):
        self._hide_tooltip()
        self._end_edit(commit=True)
        self.body.yview(*args)
        self._refresh()

    def _xview(self, *args):
        self._hide_tooltip()
        self._end_edit(commit=True)
        self.body.xview(*args)

    def _on_body_xscroll(self, f1, f2):
        self.hsb.set(f1, f2)
        self.header.xview_moveto(f1)

    def _scroll_generic(self, _event, units: int, orient: str):
        self._hide_tooltip()
        self._end_edit(commit=True)
        if orient == "y":
            self.body.yview_scroll(units, "units")
            self._refresh()
        else:
            self.body.xview_scroll(units, "units")
        return "break"

    def _on_wheel(self, event):
        delta = -1 * (event.delta // 120) if event.delta else 0
        if delta == 0 and event.delta:
            delta = -1 if event.delta > 0 else 1
        return self._scroll_generic(event, delta, "y")

    def _on_shift_wheel(self, event):
        delta = -1 * (event.delta // 120) if event.delta else 0
        if delta == 0 and event.delta:
            delta = -1 if event.delta > 0 else 1
        return self._scroll_generic(event, delta, "x")

    # ------------------------------------------------------------------
    # Hit test / interaction
    # ------------------------------------------------------------------

    def _hit_test(self, x: int, y: int) -> Tuple[Optional[int], Optional[Tuple[str, Optional[int]]]]:
        cx = int(self.body.canvasx(x))
        cy = int(self.body.canvasy(y))
        row = cy // self.row_height

        if row < 0 or row >= len(self.model_rows):
            return None, None

        for kind, x0, x1, extra in self._cols:
            if x0 <= cx < x1:
                return row, (kind, extra)
        return row, None

    def _on_click(self, event):
        self._hide_tooltip()
        self.body.focus_set()

        row_index, col_data = self._hit_test(event.x, event.y)

        if row_index is None:
            self._end_edit(commit=True)
            return

        kind, extra = col_data or (None, None)

        if self._editor and kind == "day":
            if row_index == self._edit_row and int(extra) == self._edit_day:
                return

        self._end_edit(commit=True)

        if not col_data:
            return

        if kind == "del":
            if callable(self.on_delete_row) and not self.read_only:
                self.on_delete_row(row_index)
            return

        if kind == "trip":
            if not self.read_only and callable(self.on_trip_period_click):
                self.on_trip_period_click(row_index)
            return

        if kind == "day" and not self.read_only:
            day_index = int(extra)
            try:
                y, m = self.get_year_month()
                if (day_index + 1) <= month_days(y, m):
                    self._begin_edit_day(row_index, day_index)
            except Exception:
                pass
            return

        if self.allow_row_select and kind in ("fio", "tbn"):
            if row_index in self.selected_indices:
                self.selected_indices.remove(row_index)
            else:
                self.selected_indices.add(row_index)
            self._draw_row(row_index)
            self._notify_selection_change()

    # ------------------------------------------------------------------
    # Editing
    # ------------------------------------------------------------------

    def _cell_bbox(self, row_index: int, kind: str, extra: Optional[int]) -> Optional[Tuple[int, int, int, int]]:
        for k, x0, x1, ex in self._cols:
            if k == kind and (extra is None or ex == extra):
                return x0, row_index * self.row_height, x1, (row_index + 1) * self.row_height
        return None

    def _begin_edit_day(self, row_index: int, day_index: int):
        self._end_edit(commit=True)

        bbox = self._cell_bbox(row_index, "day", day_index)
        if not bbox:
            return
        x0, y0, x1, y1 = bbox

        rec = self.model_rows[row_index]
        hours = rec.get("hours") or []

        cur_val = ""
        if day_index < len(hours):
            v = hours[day_index]
            cur_val = str(v) if v is not None else ""

        self._edit_row = row_index
        self._edit_day = day_index

        self._editor_var = tk.StringVar(value=cur_val)
        self._editor = tk.Entry(
            self.body,
            textvariable=self._editor_var,
            justify="center",
            relief="solid",
            bd=1,
            font=self.font_cell,
            bg="#ffffff",
            fg=self.TEXT,
        )

        def _cancel(_ev):
            self._end_edit(commit=False)
            return "break"

        self._editor.bind("<Return>", lambda e: self._commit_and_move(dr=1, dc=0))
        self._editor.bind("<KP_Enter>", lambda e: self._commit_and_move(dr=1, dc=0))
        self._editor.bind("<Tab>", lambda e: self._commit_and_move(dr=0, dc=1))
        self._editor.bind("<Shift-Tab>", lambda e: self._commit_and_move(dr=0, dc=-1))
        self._editor.bind("<Up>", lambda e: self._commit_and_move(dr=-1, dc=0))
        self._editor.bind("<Down>", lambda e: self._commit_and_move(dr=1, dc=0))
        self._editor.bind("<Left>", self._on_arrow_left)
        self._editor.bind("<Right>", self._on_arrow_right)
        self._editor.bind("<Escape>", _cancel)
        self._editor.bind("<FocusOut>", lambda _e: self._end_edit(commit=True))

        self._editor_window_id = self.body.create_window(
            x0 + 1,
            y0 + 1,
            width=max(6, x1 - x0 - 2),
            height=max(6, y1 - y0 - 2),
            anchor="nw",
            window=self._editor,
        )

        self._editor.focus_set()
        self._editor.selection_range(0, "end")

    def _on_arrow_left(self, _event):
        if not self._editor:
            return
        cursor_pos = self._editor.index(tk.INSERT)
        if cursor_pos == 0:
            self._commit_and_move(dr=0, dc=-1)
            return "break"
        return None

    def _on_arrow_right(self, _event):
        if not self._editor:
            return
        cursor_pos = self._editor.index(tk.INSERT)
        text_len = len(self._editor.get())
        if cursor_pos == text_len:
            self._commit_and_move(dr=0, dc=1)
            return "break"
        return None

    def _end_edit(self, commit: bool):
        if not self._editor:
            return

        row_index = self._edit_row
        day_index = self._edit_day

        val = ""
        try:
            if self._editor_var is not None:
                val = self._editor_var.get().strip()
        except Exception:
            pass

        if self._editor_window_id:
            try:
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

        self.body.focus_set()

        if not commit or row_index is None or day_index is None:
            return

        if not (0 <= row_index < len(self.model_rows)):
            return

        rec = self.model_rows[row_index]
        hours = rec.get("hours") or []

        if len(hours) <= day_index:
            hours.extend([None] * (day_index - len(hours) + 1))

        new_val = val if val else None

        if hours[day_index] != new_val:
            hours[day_index] = new_val
            rec["hours"] = hours
            self._draw_row(row_index)

            if callable(self.on_change):
                self.on_change(row_index, day_index)

    def _see_row(self, row_index: int):
        if not self.model_rows:
            return

        total_h = max(1, len(self.model_rows) * self.row_height)
        view_h = max(1, self.body.winfo_height())

        row_top = row_index * self.row_height
        row_bottom = row_top + self.row_height

        cur_top = self.body.canvasy(0)
        cur_bottom = cur_top + view_h

        if row_top < cur_top:
            self.body.yview_moveto(row_top / total_h)
            self._refresh()
        elif row_bottom > cur_bottom:
            target = max(0, row_bottom - view_h)
            self.body.yview_moveto(target / total_h)
            self._refresh()

    def _commit_and_move(self, dr: int, dc: int):
        row = self._edit_row
        day = self._edit_day
        self._end_edit(commit=True)

        if row is None or day is None:
            return "break"

        new_row = row + dr
        new_day = day + dc

        if not self.model_rows:
            return "break"

        if new_row < 0:
            new_row = 0
        if new_row >= len(self.model_rows):
            new_row = len(self.model_rows) - 1

        try:
            y, m = self.get_year_month()
            dim = month_days(y, m)
        except Exception:
            dim = 31

        if new_day < 0:
            new_day = 0
        if new_day >= dim:
            new_day = dim - 1

        self._see_row(new_row)
        self._begin_edit_day(new_row, new_day)

        return "break"

    # ------------------------------------------------------------------
    # Virtual rendering
    # ------------------------------------------------------------------

    def _refresh(self, force_redraw: bool = False):
        if not self.winfo_exists():
            return

        if not self.model_rows:
            self.body.delete("all")
            self._rendered_rows.clear()
            self._update_scrollregion()
            return

        try:
            y_top = self.body.canvasy(0)
            view_h = self.body.winfo_height()
            if view_h <= 2:
                self.after(50, self._refresh)
                return
        except Exception:
            return

        first_row = max(0, int(y_top // self.row_height))
        visible_count = int(view_h // self.row_height) + 3
        last_row = min(len(self.model_rows), first_row + visible_count)

        rows_to_display = set(range(first_row, last_row))

        if force_redraw:
            garbage = self._rendered_rows - rows_to_display
            for r in garbage:
                self.body.delete(f"row_{r}")

            for r in rows_to_display:
                self._draw_row(r)

            self._rendered_rows = rows_to_display
            return

        garbage = self._rendered_rows - rows_to_display
        for r in garbage:
            self.body.delete(f"row_{r}")

        missing = rows_to_display - self._rendered_rows
        for r in missing:
            self._draw_row(r)

        self._rendered_rows = rows_to_display

    def _format_total_value(self, val: Any) -> str:
        if val is None or val == "":
            return ""
        if isinstance(val, float):
            if float(val).is_integer():
                return str(int(val))
            return f"{val:.2f}".rstrip("0").rstrip(".")
        return str(val)

    def _draw_row(self, row_index: int):
        tag = f"row_{row_index}"
        self.body.delete(tag)

        if not (0 <= row_index < len(self.model_rows)):
            return

        rec = self.model_rows[row_index]
        y0 = row_index * self.row_height
        y1 = y0 + self.row_height

        selected = row_index in self.selected_indices
        base_bg = self.ZEBRA_EVEN if (row_index % 2 == 0) else self.ZEBRA_ODD

        hours = rec.get("hours") or []
        totals = rec.get("_totals") or {}

        try:
            y, m = self._cached_ym
            dim = month_days(y, m) if y and m else 31
        except Exception:
            dim = 31

        for kind, x0, x1, extra in self._cols:
            bg = self.SELECT_BG if selected else base_bg
            text = ""
            anchor = "w"
            tx = x0 + 4
            font = self.font_cell
            fill = self.TEXT

            if kind == "fio":
                text = self._clip_text(str(rec.get("fio") or ""), max(10, x1 - x0 - 8), self.font_cell)

            elif kind == "tbn":
                text = self._clip_text(str(rec.get("tbn") or ""), max(10, x1 - x0 - 6), self.font_cell)
                anchor = "center"
                tx = (x0 + x1) / 2
            
            elif kind == "trip":
                trip_short = self._format_trip_period_short(rec)
                text = self._clip_text(trip_short, max(10, x1 - x0 - 6), self.font_small)
                anchor = "center"
                tx = (x0 + x1) / 2
                font = self.font_small
                if trip_short:
                    bg = "#eef6ff" if not selected else self.SELECT_BG
                else:
                    bg = self.SELECT_BG if selected else base_bg
                    fill = self.MUTED
            
            elif kind == "day":
                di = int(extra)
                day_num = di + 1
                anchor = "center"
                tx = (x0 + x1) / 2
            
                val = None
            
                if day_num > dim:
                    bg = self.DISABLED_BG if not selected else self.SELECT_BG
                    fill = self.MUTED
                    text = ""
                else:
                    val = hours[di] if di < len(hours) else None
                    text = "" if val is None else str(val)
            
                    if self.show_trip_period:
                        bg = self._get_trip_cell_bg(
                            rec=rec,
                            day_num=day_num,
                            day_index=di,
                            selected=selected,
                            base_bg=base_bg,
                            cell_value=val,
                        )
                    else:
                        bg = self._get_schedule_cell_bg(
                            rec=rec,
                            day_num=day_num,
                            day_index=di,
                            selected=selected,
                            base_bg=base_bg,
                            cell_value=val,
                        )

            elif kind == "del":
                anchor = "center"
                tx = (x0 + x1) / 2
                if self.read_only or not callable(self.on_delete_row):
                    text = ""
                    bg = self.DISABLED_BG if not selected else self.SELECT_BG
                    fill = self.MUTED
                else:
                    text = "Удал."
                    bg = self.DELETE_BG if not selected else self.SELECT_BG
                    fill = self.DELETE_TEXT
                    font = self.font_small

            else:
                text = self._format_total_value(totals.get(kind))
                anchor = "e"
                tx = x1 - 4
                if kind in ("days", "hours", "ot_day", "ot_night"):
                    font = self.font_small

            self.body.create_rectangle(
                x0,
                y0,
                x1,
                y1,
                fill=bg,
                outline=self.BORDER,
                tags=(tag, "bg"),
            )

            if text:
                self.body.create_text(
                    tx,
                    (y0 + y1) / 2,
                    text=text,
                    anchor=anchor,
                    fill=fill,
                    font=font,
                    tags=(tag, "txt"),
                )
