from __future__ import annotations

import calendar
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

from timesheet_common import (
    calc_row_totals,
    normalize_hours_list,
    normalize_spaces,
    normalize_tbn,
)
from timesheet_db import (
    load_employees_from_db,
    load_objects_short_for_timesheet,
    find_fired_employees_in_timesheet,
)
from trip_timesheet_db import (
    upsert_trip_timesheet_header,
    replace_trip_timesheet_rows,
    load_trip_timesheet_rows_from_db,
    find_trip_timesheet_header_id,
    find_duplicate_employees_for_trip_timesheet,
)

from trip_period_dialog import TripPeriodDialog

# ВАЖНО:
# подстрой импорт под твой реальный модуль, где лежит грид
from virtual_timesheet_grid import VirtualTimesheetGrid

# ВАЖНО:
# подстрой импорт под твой реальный модуль диалога выбора сотрудников
from timesheet_dialogs import SelectEmployeesDialog


MONTH_NAMES = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}


class TripTimesheetPage(ttk.Frame):
    def __init__(self, master, app, *args, **kwargs):
        super().__init__(master, *args, **kwargs)
        self.app = app

        self.current_header_id: Optional[int] = None
        self.rows: List[Dict[str, Any]] = []

        today = date.today()
        self.var_year = tk.IntVar(value=today.year)
        self.var_month = tk.IntVar(value=today.month)

        self.var_object_display = tk.StringVar(value="")
        self.var_status = tk.StringVar(value="Готово.")
        self.var_trip_info = tk.StringVar(value="")

        self._objects_cache: List[Tuple[str, str, str]] = []
        self._object_display_map: Dict[str, Tuple[str, str, str]] = {}

        self._building_ui = False

        self._build_ui()
        self._load_reference_data()
        self._refresh_grid()

    # =========================================================
    # UI
    # =========================================================
    def _build_ui(self) -> None:
        self._building_ui = True

        top = ttk.Frame(self, padding=(10, 10, 10, 6))
        top.pack(fill="x")

        ttk.Label(top, text="Год").pack(side="left")
        self.cmb_year = ttk.Combobox(
            top,
            width=8,
            state="readonly",
            values=self._make_year_values(),
            textvariable=self.var_year,
        )
        self.cmb_year.pack(side="left", padx=(6, 12))
        self.cmb_year.bind("<<ComboboxSelected>>", lambda _e: self._on_period_changed())

        ttk.Label(top, text="Месяц").pack(side="left")
        self.cmb_month = ttk.Combobox(
            top,
            width=12,
            state="readonly",
            values=[f"{m:02d} — {MONTH_NAMES[m]}" for m in range(1, 13)],
        )
        self.cmb_month.pack(side="left", padx=(6, 12))
        self.cmb_month.bind("<<ComboboxSelected>>", lambda _e: self._on_month_combo_changed())
        self.cmb_month.current(max(0, self.var_month.get() - 1))

        ttk.Label(top, text="Объект").pack(side="left")
        self.cmb_object = ttk.Combobox(
            top,
            width=60,
            textvariable=self.var_object_display,
        )
        self.cmb_object.pack(side="left", padx=(6, 8), fill="x", expand=True)
        self.cmb_object.bind("<<ComboboxSelected>>", lambda _e: self._on_object_changed())
        self.cmb_object.bind("<FocusOut>", lambda _e: self._normalize_object_field())

        btns = ttk.Frame(self, padding=(10, 0, 10, 6))
        btns.pack(fill="x")

        ttk.Button(btns, text="Открыть", command=self._open_timesheet).pack(side="left")
        ttk.Button(btns, text="Сохранить", command=self._save_timesheet).pack(side="left", padx=(8, 0))
        ttk.Button(btns, text="Добавить сотрудников", command=self._add_employees).pack(side="left", padx=(16, 0))
        ttk.Button(btns, text="Удалить выбранных", command=self._delete_selected_rows).pack(side="left", padx=(8, 0))
        ttk.Button(btns, text="Очистить часы", command=self._clear_hours_for_selected).pack(side="left", padx=(8, 0))
        ttk.Button(btns, text="Проверить дубли", command=self._check_duplicates).pack(side="left", padx=(16, 0))

        grid_wrap = ttk.Frame(self, padding=(10, 0, 10, 6))
        grid_wrap.pack(fill="both", expand=True)

        self.grid_widget = VirtualTimesheetGrid(
            grid_wrap,
            get_year_month=self._get_year_month,
            on_change=self._on_grid_change,
            on_delete_row=self._on_delete_row,
            on_selection_change=self._on_selection_change,
            on_trip_period_click=self._on_trip_period_click,
            show_trip_period=True,
            allow_row_select=True,
            read_only=False,
        )
        self.grid_widget.pack(fill="both", expand=True)

        bottom = ttk.Frame(self, padding=(10, 0, 10, 10))
        bottom.pack(fill="x")

        trip_lbl = ttk.Label(
            bottom,
            textvariable=self.var_trip_info,
            foreground="#1f4f82",
        )
        trip_lbl.pack(anchor="w")

        status_lbl = ttk.Label(
            bottom,
            textvariable=self.var_status,
            foreground="#555555",
        )
        status_lbl.pack(anchor="w", pady=(4, 0))

        self._building_ui = False

    def _make_year_values(self) -> List[int]:
        current = date.today().year
        return list(range(current - 3, current + 4))

    # =========================================================
    # Справочники
    # =========================================================
    def _load_reference_data(self) -> None:
        self._objects_cache = load_objects_short_for_timesheet()

        display_values: List[str] = []
        self._object_display_map = {}

        for object_id, object_addr, short_name in self._objects_cache:
            display = self._make_object_display(object_id, object_addr, short_name)
            display_values.append(display)
            self._object_display_map[display] = (object_id, object_addr, short_name)

        self.cmb_object["values"] = display_values

    def _make_object_display(self, object_id: str, object_addr: str, short_name: str) -> str:
        object_id = normalize_spaces(object_id or "")
        object_addr = normalize_spaces(object_addr or "")
        short_name = normalize_spaces(short_name or "")

        left = object_addr
        if short_name:
            left = f"{object_addr} ({short_name})"

        if object_id:
            return f"{left} [{object_id}]"
        return left

    def _parse_selected_object(self) -> Tuple[str, str]:
        value = normalize_spaces(self.var_object_display.get())
        if not value:
            return "", ""

        exact = self._object_display_map.get(value)
        if exact:
            object_id, object_addr, _short_name = exact
            return normalize_spaces(object_id), normalize_spaces(object_addr)

        for display, item in self._object_display_map.items():
            if normalize_spaces(display) == value:
                object_id, object_addr, _short_name = item
                return normalize_spaces(object_id), normalize_spaces(object_addr)

        return "", value

    def _normalize_object_field(self) -> None:
        value = normalize_spaces(self.var_object_display.get())
        if not value:
            self.var_object_display.set("")
            return

        exact = self._object_display_map.get(value)
        if exact:
            self.var_object_display.set(self._make_object_display(*exact))
            return

        for display in self._object_display_map:
            if normalize_spaces(display) == value:
                self.var_object_display.set(display)
                return

    # =========================================================
    # Период / объект
    # =========================================================
    def _get_year_month(self) -> Tuple[int, int]:
        return int(self.var_year.get()), int(self.var_month.get())

    def _on_month_combo_changed(self) -> None:
        idx = self.cmb_month.current()
        if idx < 0:
            return
        self.var_month.set(idx + 1)
        self._on_period_changed()

    def _on_period_changed(self) -> None:
        self._refresh_grid()
        self._update_trip_info_from_selection()

    def _on_object_changed(self) -> None:
        self._normalize_object_field()

    # =========================================================
    # Работа со строками
    # =========================================================
    def _empty_row(self, fio: str = "", tbn: str = "") -> Dict[str, Any]:
        year, month = self._get_year_month()
        hours = normalize_hours_list([], year, month)
        totals = calc_row_totals(hours, year, month)

        return {
            "fio": normalize_spaces(fio),
            "tbn": normalize_tbn(tbn),
            "hours": hours,
            "trip_date_from": None,
            "trip_date_to": None,
            "_totals": totals,
            "work_schedule": "",
        }

    def _normalize_trip_row(self, rec: Dict[str, Any]) -> Dict[str, Any]:
        year, month = self._get_year_month()

        fio = normalize_spaces(rec.get("fio") or "")
        tbn = normalize_tbn(rec.get("tbn"))
        hours = normalize_hours_list(rec.get("hours"), year, month)

        trip_date_from = rec.get("trip_date_from")
        trip_date_to = rec.get("trip_date_to")

        if trip_date_from and not isinstance(trip_date_from, date):
            trip_date_from = None
        if trip_date_to and not isinstance(trip_date_to, date):
            trip_date_to = None

        totals = rec.get("_totals")
        if not isinstance(totals, dict):
            totals = calc_row_totals(hours, year, month)

        out = {
            "fio": fio,
            "tbn": tbn,
            "hours": hours,
            "trip_date_from": trip_date_from,
            "trip_date_to": trip_date_to,
            "_totals": totals,
        }

        if "work_schedule" in rec:
            out["work_schedule"] = normalize_spaces(rec.get("work_schedule") or "")
        else:
            out["work_schedule"] = ""

        return out

    def _recalc_all_totals(self) -> None:
        year, month = self._get_year_month()
        for rec in self.rows:
            rec["hours"] = normalize_hours_list(rec.get("hours"), year, month)
            rec["_totals"] = calc_row_totals(rec["hours"], year, month)

    def _refresh_grid(self) -> None:
        self._recalc_all_totals()
        self.grid_widget.set_rows(self.rows)

    def _set_rows(self, rows: Sequence[Dict[str, Any]]) -> None:
        self.rows = [self._normalize_trip_row(dict(r)) for r in rows]
        self._refresh_grid()

    # =========================================================
    # Открытие / загрузка / сохранение
    # =========================================================
    def _open_timesheet(self) -> None:
        object_id, object_addr = self._parse_selected_object()
        year, month = self._get_year_month()

        if not object_addr:
            messagebox.showwarning("Внимание", "Выберите объект.", parent=self)
            return

        try:
            rows = load_trip_timesheet_rows_from_db(
                object_id=object_id or None,
                object_addr=object_addr,
                year=year,
                month=month,
                user_id=int(self.app.current_user["id"]),
            )
            self.current_header_id = find_trip_timesheet_header_id(
                object_id=object_id or None,
                object_addr=object_addr,
                year=year,
                month=month,
                user_id=int(self.app.current_user["id"]),
            )
        except Exception as exc:
            messagebox.showerror("Ошибка", f"Не удалось открыть табель:\n{exc}", parent=self)
            return

        self._set_rows(rows)
        self.var_status.set(f"Открыт командировочный табель: {object_addr}, {month:02d}.{year}.")
        self._update_trip_info_from_selection()

    def _save_timesheet(self) -> None:
        object_id, object_addr = self._parse_selected_object()
        year, month = self._get_year_month()

        if not object_addr:
            messagebox.showwarning("Внимание", "Выберите объект.", parent=self)
            return

        if not self.rows:
            if not messagebox.askyesno(
                "Сохранение",
                "В табеле нет строк. Всё равно создать/сохранить пустой табель?",
                parent=self,
            ):
                return

        errors = self._validate_before_save()
        if errors:
            messagebox.showerror("Ошибка", "\n".join(errors), parent=self)
            return

        try:
            header_id = upsert_trip_timesheet_header(
                object_id=object_id or None,
                object_addr=object_addr,
                year=year,
                month=month,
                user_id=int(self.app.current_user["id"]),
            )

            self._recalc_all_totals()
            replace_trip_timesheet_rows(
                header_id=header_id,
                rows=self.rows,
                year=year,
                month=month,
            )

            self.current_header_id = header_id
        except Exception as exc:
            messagebox.showerror("Ошибка", f"Не удалось сохранить табель:\n{exc}", parent=self)
            return

        self.var_status.set(f"Командировочный табель сохранён. ID: {self.current_header_id}")
        self._update_trip_info_from_selection()

    def _validate_before_save(self) -> List[str]:
        errors: List[str] = []

        for i, rec in enumerate(self.rows, start=1):
            fio = normalize_spaces(rec.get("fio") or "")
            tbn = normalize_tbn(rec.get("tbn"))
            d_from = rec.get("trip_date_from")
            d_to = rec.get("trip_date_to")

            if not fio and not tbn:
                errors.append(f"Строка {i}: не заполнены ФИО и табельный номер.")

            if (d_from is None) != (d_to is None):
                errors.append(f"Строка {i}: период командировки должен быть заполнен полностью или очищен полностью.")

            if d_from and d_to and d_from > d_to:
                errors.append(f"Строка {i}: дата начала командировки позже даты окончания.")

        return errors

    # =========================================================
    # Выбор сотрудников
    # =========================================================
    def _add_employees(self) -> None:
        try:
            employees = load_employees_from_db()
        except Exception as exc:
            messagebox.showerror("Ошибка", f"Не удалось загрузить сотрудников:\n{exc}", parent=self)
            return
    
        existing_keys = {
            (normalize_spaces(r.get("fio") or "").lower(), normalize_tbn(r.get("tbn")))
            for r in self.rows
        }
    
        try:
            dlg = SelectEmployeesDialog(self, employees=employees, current_dep="Все")
            self.wait_window(dlg)
            selected = getattr(dlg, "result", None)
        except Exception as exc:
            messagebox.showerror("Ошибка", f"Не удалось открыть выбор сотрудников:\n{exc}", parent=self)
            return
    
        if not selected:
            return
    
        added = 0
        for item in selected:
            if len(item) >= 2:
                fio = normalize_spaces(item[0] or "")
                tbn = normalize_tbn(item[1])
            else:
                continue
    
            key = (fio.lower(), tbn)
            if key in existing_keys:
                continue
    
            row = self._empty_row(fio=fio, tbn=tbn)
    
            if len(item) >= 5:
                row["work_schedule"] = normalize_spaces(item[4] or "")
    
            self.rows.append(row)
            existing_keys.add(key)
            added += 1
    
        self._refresh_grid()
        self.var_status.set(f"Добавлено сотрудников: {added}")
        self._check_fired_employees_after_add()
        self._check_duplicates(silent_if_empty=True)

    def _check_fired_employees_after_add(self) -> None:
        employees = []
        for rec in self.rows:
            fio = normalize_spaces(rec.get("fio") or "")
            tbn = normalize_tbn(rec.get("tbn"))
            if fio or tbn:
                employees.append((fio, tbn))

        if not employees:
            return

        year, month = self._get_year_month()

        try:
            fired = find_fired_employees_in_timesheet(
                employees=employees,
                year=year,
                month=month,
            )
        except Exception:
            return

        if not fired:
            return

        lines = ["В табеле обнаружены уволенные сотрудники:"]
        for item in fired[:20]:
            fio = item.get("fio") or ""
            tbn = item.get("tbn") or ""
            dismissal_date = item.get("dismissal_date")
            if dismissal_date:
                lines.append(f"• {fio} ({tbn}) — увольнение: {dismissal_date}")
            else:
                lines.append(f"• {fio} ({tbn}) — отмечен как уволенный")

        if len(fired) > 20:
            lines.append(f"... и ещё {len(fired) - 20}")

        messagebox.showwarning("Проверка сотрудников", "\n".join(lines), parent=self)

    # =========================================================
    # Дубли
    # =========================================================
    def _collect_employee_pairs(self) -> List[Tuple[str, str]]:
        out: List[Tuple[str, str]] = []
        for rec in self.rows:
            fio = normalize_spaces(rec.get("fio") or "")
            tbn = normalize_tbn(rec.get("tbn"))
            if fio or tbn:
                out.append((fio, tbn))
        return out

    def _check_duplicates(self, silent_if_empty: bool = False) -> None:
        object_id, object_addr = self._parse_selected_object()
        year, month = self._get_year_month()

        if not object_addr:
            if not silent_if_empty:
                messagebox.showwarning("Внимание", "Сначала выберите объект.", parent=self)
            return

        employees = self._collect_employee_pairs()
        if not employees:
            if not silent_if_empty:
                messagebox.showinfo("Проверка дублей", "В табеле нет сотрудников для проверки.", parent=self)
            return

        try:
            duplicates = find_duplicate_employees_for_trip_timesheet(
                object_id=object_id or None,
                object_addr=object_addr,
                year=year,
                month=month,
                user_id=int(self.app.current_user["id"]),
                employees=employees,
            )
        except Exception as exc:
            if not silent_if_empty:
                messagebox.showerror("Ошибка", f"Не удалось проверить дубли:\n{exc}", parent=self)
            return

        if not duplicates:
            if not silent_if_empty:
                messagebox.showinfo("Проверка дублей", "Дубликаты не найдены.", parent=self)
            return

        lines = ["Сотрудники уже присутствуют в командировочных табелях других пользователей:"]
        for item in duplicates[:30]:
            fio = item.get("fio") or ""
            tbn = item.get("tbn") or ""
            user_name = item.get("full_name") or item.get("username") or f"ID={item.get('user_id')}"
            lines.append(f"• {fio} ({tbn}) — пользователь: {user_name}, header_id={item.get('header_id')}")

        if len(duplicates) > 30:
            lines.append(f"... и ещё {len(duplicates) - 30}")

        messagebox.showwarning("Найдены дубликаты", "\n".join(lines), parent=self)

    # =========================================================
    # События грида
    # =========================================================
    def _on_grid_change(self, row_index: int, col_index: int) -> None:
        if not (0 <= row_index < len(self.rows)):
            return

        year, month = self._get_year_month()

        rec = self.rows[row_index]
        rec["hours"] = normalize_hours_list(rec.get("hours"), year, month)
        rec["_totals"] = calc_row_totals(rec["hours"], year, month)

        self.grid_widget.set_rows(self.rows)
        self._update_trip_info_from_selection()

    def _on_delete_row(self, row_index: int) -> None:
        if not (0 <= row_index < len(self.rows)):
            return

        rec = self.rows[row_index]
        fio = normalize_spaces(rec.get("fio") or "")
        tbn = normalize_tbn(rec.get("tbn"))

        if not messagebox.askyesno(
            "Удаление строки",
            f"Удалить строку сотрудника:\n{fio} ({tbn})?",
            parent=self,
        ):
            return

        del self.rows[row_index]
        self._refresh_grid()
        self._update_trip_info_from_selection()

    def _on_selection_change(self, selected_rows) -> None:
        self._update_trip_info_from_selection()

    def _on_trip_period_click(self, row_index: int) -> None:
        if not (0 <= row_index < len(self.rows)):
            return

        rec = self.rows[row_index]
        year, month = self._get_year_month()

        result = TripPeriodDialog.show(
            self,
            initial_date_from=rec.get("trip_date_from"),
            initial_date_to=rec.get("trip_date_to"),
            year=year,
            month=month,
        )
        if result is None:
            return

        trip_date_from, trip_date_to = result

        rec["trip_date_from"] = trip_date_from
        rec["trip_date_to"] = trip_date_to

        self._refresh_grid()
        self.grid_widget.set_selected_rows({row_index})
        self._update_trip_info_from_selection()

    # =========================================================
    # Действия над выбранными строками
    # =========================================================
    def _get_selected_row_indexes(self) -> List[int]:
        try:
            selected = list(self.grid_widget.get_selected_rows())
        except Exception:
            selected = []
        return sorted(i for i in selected if 0 <= i < len(self.rows))

    def _delete_selected_rows(self) -> None:
        indexes = self._get_selected_row_indexes()
        if not indexes:
            messagebox.showinfo("Удаление", "Не выбраны строки.", parent=self)
            return

        if not messagebox.askyesno(
            "Удаление",
            f"Удалить выбранные строки: {len(indexes)} шт.?",
            parent=self,
        ):
            return

        for idx in reversed(indexes):
            del self.rows[idx]

        self._refresh_grid()
        self._update_trip_info_from_selection()
        self.var_status.set(f"Удалено строк: {len(indexes)}")

    def _clear_hours_for_selected(self) -> None:
        indexes = self._get_selected_row_indexes()
        if not indexes:
            messagebox.showinfo("Очистка часов", "Не выбраны строки.", parent=self)
            return

        year, month = self._get_year_month()

        for idx in indexes:
            self.rows[idx]["hours"] = [None] * 31
            self.rows[idx]["_totals"] = calc_row_totals(self.rows[idx]["hours"], year, month)

        self._refresh_grid()
        self.var_status.set(f"Очищены часы у строк: {len(indexes)}")

    # =========================================================
    # Нижняя информационная строка
    # =========================================================
    def _update_trip_info_from_selection(self) -> None:
        indexes = self._get_selected_row_indexes()
        if len(indexes) != 1:
            self.var_trip_info.set("")
            return

        rec = self.rows[indexes[0]]
        fio = normalize_spaces(rec.get("fio") or "")
        tbn = normalize_tbn(rec.get("tbn"))
        d_from = rec.get("trip_date_from")
        d_to = rec.get("trip_date_to")

        if d_from and d_to:
            period = f"Командировка: с {d_from.strftime('%d.%m.%Y')} по {d_to.strftime('%d.%m.%Y')}"
        else:
            period = "Период командировки не задан"

        suffix = fio
        if tbn:
            suffix = f"{fio} ({tbn})"

        self.var_trip_info.set(f"{suffix}: {period}")

    # =========================================================
    # Внешние helpers, если страница открывается из списка
    # =========================================================
    def open_by_context(
        self,
        *,
        object_id: Optional[str],
        object_addr: str,
        year: int,
        month: int,
    ) -> None:
        self.var_year.set(int(year))
        self.var_month.set(int(month))
        self.cmb_month.current(max(0, int(month) - 1))

        object_id_norm = normalize_spaces(object_id or "")
        object_addr_norm = normalize_spaces(object_addr or "")

        selected_display = ""
        for display, item in self._object_display_map.items():
            item_object_id, item_object_addr, _short_name = item
            if normalize_spaces(item_object_addr) == object_addr_norm and normalize_spaces(item_object_id) == object_id_norm:
                selected_display = display
                break

        if selected_display:
            self.var_object_display.set(selected_display)
        else:
            self.var_object_display.set(object_addr_norm)

        self._open_timesheet()

    # =========================================================
    # Необязательная команда: создать пустую строку вручную
    # =========================================================
    def add_empty_row(self) -> None:
        self.rows.append(self._empty_row())
        self._refresh_grid()
        self.var_status.set("Добавлена пустая строка.")
