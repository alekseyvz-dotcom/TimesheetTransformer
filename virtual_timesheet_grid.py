# virtual_timesheet_grid.py
from __future__ import annotations

import calendar
import tkinter as tk
import platform
from datetime import date
from tkinter import ttk
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# Определяем ОС для правильной обработки колеса мыши
OS_NAME = platform.system()

def month_days(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


class VirtualTimesheetGrid(tk.Frame):
    """
    Оптимизированный виртуальный грид табеля.
    - Отрисовывает только видимые строки (Virtualization).
    - Использует Canvas Tags для быстрого управления объектами.
    - Кэширует выходные дни.
    - Поддерживает любой ввод в ячейках.
    """

    def __init__(
        self,
        master,
        *,
        get_year_month: Callable[[], Tuple[int, int]],
        on_change: Optional[Callable[[int, int], None]] = None,
        on_delete_row: Optional[Callable[[int], None]] = None,
        row_height: int = 22,
        colpx: Optional[Dict[str, int]] = None,
        read_only: bool = False,
        allow_row_select: bool = True,
    ):
        super().__init__(master, bg="#ffffff")

        self.get_year_month = get_year_month
        self.on_change = on_change
        self.on_delete_row = on_delete_row

        self.read_only = bool(read_only)
        self.allow_row_select = bool(allow_row_select)
        self.row_height = int(row_height)

        self.COLPX = colpx or {
            "fio": 200, "tbn": 100, "day": 36,
            "days": 46, "hours": 56, "del": 66,
        }

        self.model_rows: List[Dict[str, Any]] = []
        self.selected_indices: Set[int] = set()

        # --- Кэширование ---
        # Храним (год, месяц) -> карта {день_индекс: цвет_фона}
        self._cached_ym: Tuple[int, int] = (0, 0)
        self._weekend_map: Dict[int, str] = {} 
        # Множество индексов строк, которые сейчас отрисованы на канвасе
        self._rendered_rows: Set[int] = set()

        # --- Редактор ---
        self._editor: Optional[tk.Entry] = None
        self._editor_window_id: Optional[int] = None
        self._editor_var: Optional[tk.StringVar] = None
        self._edit_row: Optional[int] = None
        self._edit_day: Optional[int] = None

        # --- Цвета ---
        self.HEADER_BG = "#e7edf4"
        self.ZEBRA_EVEN = "#ffffff"
        self.ZEBRA_ODD = "#f8fbfd"
        self.SELECT_BG = "#d7e8fb"
        self.DISABLED_BG = "#eef2f6"
        self.WEEK_BG_SAT = "#fff8e8"
        self.WEEK_BG_SUN = "#fff1f1"

        # --- UI Components ---
        self.header = tk.Canvas(
            self,
            height=26,
            highlightthickness=0,
            bg=self.HEADER_BG,
            bd=0,
        )
        self.body = tk.Canvas(
            self,
            highlightthickness=0,
            bg="#ffffff",
            bd=0,
        )
        
        self.vsb = ttk.Scrollbar(self, orient="vertical", command=self._yview)
        self.hsb = ttk.Scrollbar(self, orient="horizontal", command=self._xview)

        # Layout
        self.header.grid(row=0, column=0, sticky="ew")
        self.body.grid(row=1, column=0, sticky="nsew")
        self.vsb.grid(row=1, column=1, sticky="ns")
        self.hsb.grid(row=2, column=0, sticky="ew")

        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        # Linking Scrollbars
        self.body.configure(yscrollcommand=self.vsb.set)
        # Для горизонтального скролла используем кастомный метод для синхронизации заголовка
        self.body.configure(xscrollcommand=self._on_body_xscroll)

        # Events
        self.body.bind("<Configure>", lambda e: self._refresh())
        self.body.bind("<Button-1>", self._on_click)

        # MouseWheel (Cross-platform)
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
        self._draw_header()

    # -------- Public API --------

    def set_rows(self, rows: List[Dict[str, Any]]):
        """Загружает новые данные и полностью обновляет вид."""
        self._end_edit(commit=True)
        self.model_rows = rows or []
        self.selected_indices.clear()
        
        # Полная очистка канваса (быстрее, чем удалять по одной)
        self.body.delete("all")
        self._rendered_rows.clear()
        
        self._update_weekends_cache()
        self._update_scrollregion()
        self._refresh()

    def set_selected_indices(self, indices: Set[int]):
        """Устанавливает выделенные строки."""
        self.selected_indices = set(indices or set())
        # Принудительно перерисовываем всё видимое, чтобы обновить цвета выделения
        self._refresh(force_redraw=True)

    def get_selected_indices(self) -> Set[int]:
        return set(self.selected_indices)

    def refresh(self):
        """Принудительное обновление (например, при смене месяца извне)."""
        self._update_weekends_cache()
        self._refresh(force_redraw=True)

    def close_editor(self, commit: bool = True):
        self._end_edit(commit=commit)

    # -------- Internals --------

    def _update_weekends_cache(self):
        """Создает карту цветов для выходных дней текущего месяца."""
        y, m = self.get_year_month()
        # Если год/месяц не менялись, ничего не делаем
        if (y, m) == self._cached_ym:
            return
        
        self._cached_ym = (y, m)
        self._weekend_map.clear()
        
        dim = month_days(y, m)
        for d in range(1, dim + 1):
            wd = date(y, m, d).weekday()
            # 5 - Суббота, 6 - Воскресенье
            if wd == 5:
                self._weekend_map[d-1] = self.WEEK_BG_SAT
            elif wd == 6:
                self._weekend_map[d-1] = self.WEEK_BG_SUN

    def _build_columns(self):
        cols = []
        x = 0
        
        def add(kind, width, extra=None):
            nonlocal x
            cols.append((kind, x, x + width, extra))
            x += width

        add("fio", self.COLPX["fio"])
        add("tbn", self.COLPX["tbn"])
        
        for di in range(31):
            add("day", self.COLPX["day"], di)
            
        for k in ["days", "hours", "ot_day", "ot_night"]:
             add(k, self.COLPX.get(k, 50))
             
        add("del", self.COLPX["del"])

        self._cols = cols
        self._total_width = x
        self.header.configure(scrollregion=(0, 0, self._total_width, 0))
        self.body.configure(scrollregion=(0, 0, self._total_width, 0))

    def _draw_header(self):
        self.header.delete("all")
        y0, y1 = 0, 26
        
        labels = {
            "fio": "ФИО", "tbn": "Таб.№", "days": "Дней", 
            "hours": "Часы", "ot_day": "Пер.день", 
            "ot_night": "Пер.ночь", "del": "Удалить"
        }

        for kind, x0, x1, extra in self._cols:
            text = labels.get(kind, "")
            if kind == "day":
                text = str(int(extra) + 1)

            self.header.create_rectangle(x0, y0, x1, y1, fill=self.HEADER_BG, outline="#b0b0b0")
            self.header.create_text((x0 + x1) / 2, (y0 + y1) / 2, text=text, anchor="center")

    def _update_scrollregion(self):
        h = max(1, len(self.model_rows)) * self.row_height
        self.body.configure(scrollregion=(0, 0, self._total_width, h))

    # --- Scrolling ---

    def _yview(self, *args):
        self._end_edit(commit=True)
        self.body.yview(*args)
        self._refresh()

    def _xview(self, *args):
        self._end_edit(commit=True)
        self.body.xview(*args)
        # Header синхронизируется автоматически через callback _on_body_xscroll

    def _on_body_xscroll(self, f1, f2):
        """Вызывается канвасом при прокрутке по X. Синхронизируем Scrollbar и Header."""
        self.hsb.set(f1, f2)
        self.header.xview_moveto(f1)

    def _scroll_generic(self, event, units, orient):
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
            delta = -1 if event.delta > 0 else 1 # Fix for MacOS small delta
        return self._scroll_generic(event, delta, "y")

    def _on_shift_wheel(self, event):
        delta = -1 * (event.delta // 120) if event.delta else 0
        if delta == 0 and event.delta: 
            delta = -1 if event.delta > 0 else 1
        return self._scroll_generic(event, delta, "x")

    # --- Interaction ---

    def _hit_test(self, x: int, y: int) -> Tuple[Optional[int], Optional[Tuple[str, Optional[int]]]]:
        cx = int(self.body.canvasx(x))
        cy = int(self.body.canvasy(y))
        row = cy // self.row_height
        
        if row < 0 or row >= len(self.model_rows):
            return None, None
        
        # Простой перебор колонок (их немного, это быстро)
        for kind, x0, x1, extra in self._cols:
            if x0 <= cx < x1:
                return row, (kind, extra)
        return row, None

    def _on_click(self, event):
        # 1. Возвращаем фокус на канвас (чтобы работали клавиатурные события после закрытия редактора)
        self.body.focus_set()

        row_index, col_data = self._hit_test(event.x, event.y)
        
        # Если клик вне данных - просто коммитим редактор
        if row_index is None:
            self._end_edit(commit=True)
            return

        # Если клик в ту же ячейку, что и редактируется - игнорируем
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

        if kind == "day" and not self.read_only:
            day_index = int(extra)
            y, m = self.get_year_month()
            if (day_index + 1) <= month_days(y, m):
                self._begin_edit_day(row_index, day_index)
            return

        if self.allow_row_select and kind in ("fio", "tbn"):
            if row_index in self.selected_indices:
                self.selected_indices.remove(row_index)
            else:
                self.selected_indices.add(row_index)
            # Перерисовываем только одну строку
            self._draw_row(row_index)

    # --- Editing ---

    def _cell_bbox(self, row_index: int, kind: str, extra: Optional[int]) -> Optional[Tuple[int, int, int, int]]:
        for k, x0, x1, ex in self._cols:
            if k == kind and (extra is None or ex == extra):
                return (x0, row_index * self.row_height, x1, (row_index + 1) * self.row_height)
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
        self._editor = tk.Entry(self.body, textvariable=self._editor_var, justify="center")

        def _cancel(_ev):
            self._end_edit(commit=False)
            return "break"

        # --- НАВИГАЦИЯ ---
        # Enter / Tab / Up / Down - стандартное поведение
        self._editor.bind("<Return>", lambda e: self._commit_and_move(dr=1, dc=0)) # Enter вниз (или вправо, как удобнее)
        self._editor.bind("<KP_Enter>", lambda e: self._commit_and_move(dr=1, dc=0)) # Numpad Enter
        self._editor.bind("<Tab>", lambda e: self._commit_and_move(dr=0, dc=1))
        self._editor.bind("<Up>", lambda e: self._commit_and_move(dr=-1, dc=0))
        self._editor.bind("<Down>", lambda e: self._commit_and_move(dr=1, dc=0))
        
        # --- НОВАЯ ЛОГИКА ДЛЯ ВЛЕВО / ВПРАВО ---
        self._editor.bind("<Left>", self._on_arrow_left)
        self._editor.bind("<Right>", self._on_arrow_right)
        
        self._editor.bind("<Escape>", _cancel)
        self._editor.bind("<FocusOut>", lambda e: self._end_edit(commit=True))

        self._editor_window_id = self.body.create_window(
            x0 + 1, y0 + 1,
            width=max(4, x1 - x0 - 2),
            height=max(4, y1 - y0 - 2),
            anchor="nw",
            window=self._editor,
        )

        self._editor.focus_set()
        # Выделяем весь текст при входе, чтобы сразу можно было перезаписать
        self._editor.selection_range(0, "end")

    def _on_arrow_left(self, event):
        """Переход влево, только если курсор в начале строки."""
        # tk.INSERT - текущая позиция курсора
        cursor_pos = self._editor.index(tk.INSERT)
        
        # Если курсор в самом начале (или текст выделен целиком и курсор в начале)
        if cursor_pos == 0:
            # Сохраняем и идем в предыдущий день
            self._commit_and_move(dr=0, dc=-1)
            return "break" # Предотвращаем стандартное движение курсора
        
        # Иначе просто двигаем курсор внутри текста
        return

    def _on_arrow_right(self, event):
        """Переход вправо, только если курсор в конце строки."""
        cursor_pos = self._editor.index(tk.INSERT)
        text_len = len(self._editor.get())
        
        # Если курсор в самом конце
        if cursor_pos == text_len:
            # Сохраняем и идем в следующий день
            self._commit_and_move(dr=0, dc=1)
            return "break"
            
        return

    def _end_edit(self, commit: bool):
        if not self._editor:
            return

        row_index = self._edit_row
        day_index = self._edit_day
        
        # Получаем значение перед уничтожением виджета
        val = ""
        try:
            if self._editor_var:
                val = self._editor_var.get().strip()
        except Exception:
            pass

        # Cleanup UI
        if self._editor_window_id:
            self.body.delete(self._editor_window_id)
        if self._editor:
            self._editor.destroy()

        self._editor = None
        self._editor_window_id = None
        self._editor_var = None
        self._edit_row = None
        self._edit_day = None
        
        # Возврат фокуса, чтобы скролл клавиатурой работал
        self.body.focus_set()

        if not commit or row_index is None or day_index is None:
            return

        if not (0 <= row_index < len(self.model_rows)):
            return

        # Сохранение в модель
        rec = self.model_rows[row_index]
        hours = rec.get("hours") or []
        
        # Расширяем список, если он короче индекса дня
        if len(hours) <= day_index:
            hours.extend([None] * (day_index - len(hours) + 1))
        
        new_val = val if val else None
        
        # Обновляем только если изменилось
        if hours[day_index] != new_val:
            hours[day_index] = new_val
            rec["hours"] = hours
            
            # Перерисовка одной строки
            self._draw_row(row_index)

            if callable(self.on_change):
                self.on_change(row_index, day_index)

    def _commit_and_move(self, dr: int, dc: int):
        """Сохраняет текущее значение и перемещает редактор."""
        row = self._edit_row
        day = self._edit_day
        self._end_edit(commit=True)

        if row is None or day is None:
            return "break"

        new_row = row + dr
        new_day = day + dc

        # Ограничения строк
        if new_row < 0: new_row = 0
        if new_row >= len(self.model_rows): 
            new_row = len(self.model_rows) - 1

        # Ограничения дней
        y, m = self.get_year_month()
        dim = month_days(y, m)
        
        if new_day < 0: new_day = 0
        if new_day >= dim: new_day = dim - 1

        # Если данные есть, открываем редактор
        if self.model_rows:
            self._begin_edit_day(new_row, new_day)

        return "break"

    # --- Virtual Rendering ---

    def _refresh(self, force_redraw: bool = False):
        """
        Умная перерисовка: рисует только то, чего нет на экране, 
        и удаляет то, что ушло за экран.
        """
        try:
            y_top = self.body.canvasy(0)
            view_h = self.body.winfo_height()
            if view_h <= 1: 
                return
        except Exception:
            return

        first_row = max(0, int(y_top // self.row_height))
        visible_count = int(view_h // self.row_height) + 2
        last_row = min(len(self.model_rows), first_row + visible_count)
        
        rows_to_display = set(range(first_row, last_row))

        if force_redraw:
            # Если принудительно, удаляем всё видимое и рисуем заново
            for r in rows_to_display:
                self._draw_row(r)
            # И чистим мусор за пределами видимости
            garbage = self._rendered_rows - rows_to_display
            for r in garbage:
                self.body.delete(f"row_{r}")
            self._rendered_rows = rows_to_display
            return

        # 1. Удаляем строки, которые ушли за пределы видимости
        garbage = self._rendered_rows - rows_to_display
        for r in garbage:
            self.body.delete(f"row_{r}")
        
        # 2. Рисуем строки, которые появились в области видимости
        missing = rows_to_display - self._rendered_rows
        for r in missing:
            self._draw_row(r)
            
        self._rendered_rows = rows_to_display

    def _draw_row(self, row_index: int):
        # Удаляем старую версию строки по тегу (если она была)
        tag = f"row_{row_index}"
        self.body.delete(tag)

        if not (0 <= row_index < len(self.model_rows)):
            return

        rec = self.model_rows[row_index]
        y0 = row_index * self.row_height
        y1 = y0 + self.row_height

        selected = (row_index in self.selected_indices)
        # Базовый цвет (зебра)
        base_bg = self.ZEBRA_EVEN if (row_index % 2 == 0) else self.ZEBRA_ODD
        
        hours = rec.get("hours") or []
        totals = rec.get("_totals") or {}
        
        y, m = self._cached_ym
        dim = month_days(y, m)

        # Рисуем ячейки
        for kind, x0, x1, extra in self._cols:
            bg = self.SELECT_BG if selected else base_bg
            text = ""
            anchor = "w"
            tx = x0 + 4

            if kind == "fio":
                text = str(rec.get("fio") or "")
            elif kind == "tbn":
                text = str(rec.get("tbn") or "")
                anchor = "center"; tx = (x0 + x1)/2
            elif kind == "day":
                di = int(extra)
                day_num = di + 1
                if day_num > dim:
                    bg = self.DISABLED_BG
                else:
                    # Берем цвет выходного из кэша O(1)
                    if not selected:
                        bg = self._weekend_map.get(di, base_bg)
                    
                    val = hours[di] if di < len(hours) else None
                    text = str(val) if val is not None else ""
                    anchor = "center"; tx = (x0 + x1)/2
            
            elif kind == "del":
                text = "Удалить" # Можно заменить на иконку
                anchor = "center"; tx = (x0 + x1)/2
            
            else:
                # Totals (days, hours, etc)
                val = totals.get(kind)
                text = str(val) if val is not None else ""
                anchor = "e"; tx = x1 - 4

            # Создаем элементы с тегом row_N
            self.body.create_rectangle(
                x0, y0, x1, y1, 
                fill=bg, outline="#e0e0e0", 
                tags=(tag, "bg")
            )
            if text:
                self.body.create_text(
                    tx, (y0 + y1)/2, 
                    text=text, anchor=anchor, fill="#111", 
                    tags=(tag, "txt")
                )
