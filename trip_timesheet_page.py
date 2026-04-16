from __future__ import annotations

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

from timesheet_common import (
    calc_row_totals,
    calc_rows_summary,
    format_summary_value,
    month_days,
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
from virtual_timesheet_grid import VirtualTimesheetGrid
from timesheet_dialogs import (
    AutoCompleteCombobox,
    SelectEmployeesDialog,
    SelectObjectIdDialog,
)

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

UI = {
    "bg": "#edf1f5",
    "panel": "#f7f9fb",
    "panel2": "#eef3f8",
    "line": "#c9d3df",
    "accent": "#2f74c0",
    "warning": "#c97a20",
    "text": "#1f2937",
    "muted": "#5b6776",
    "white": "#ffffff",
    "btn_save_bg": "#2f74c0",
    "btn_save_fg": "#ffffff",
}


class TripTimesheetPage(tk.Frame):
    def __init__(self, master, app, *args, **kwargs):
        super().__init__(master, bg=UI["bg"], *args, **kwargs)
        self.app = app

        self.current_header_id: Optional[int] = None
        self.rows: List[Dict[str, Any]] = []

        today = date.today()
        self.var_year = tk.IntVar(value=today.year)
        self.var_month = tk.IntVar(value=today.month)

        self.var_status = tk.StringVar(value="Готово.")
        self.var_trip_info = tk.StringVar(value="")
        self.var_filter = tk.StringVar(value="")

        self.objects_full: List[Tuple[str, str, str]] = []
        self.address_options: List[str] = []

        self._building_ui = False

        self._build_ui()
        self._load_reference_data()
        self._refresh_grid()

    # =========================================================
    # UI
    # =========================================================
    def _build_ui(self) -> None:
        self._building_ui = True

        self._build_header()
        self._build_top_form()
        self._build_toolbar()
        self._build_filter_bar()
        self._build_grid()
        self._build_bottom()

        self._building_ui = False

    def _build_header(self) -> None:
        hdr = tk.Frame(self, bg=UI["accent"], pady=4)
        hdr.pack(fill="x")

        tk.Label(
            hdr,
            text="📋 Командировочный табель",
            font=("Segoe UI", 12, "bold"),
            bg=UI["accent"],
            fg="white",
            padx=12,
        ).pack(side="left")

        tk.Label(
            hdr,
            textvariable=self.var_status,
            font=("Segoe UI", 8),
            bg=UI["accent"],
            fg="#dbeafe",
            padx=10,
        ).pack(side="right")

    def _ts_lbl(self, parent, text: str, row: int, col: int = 0, required: bool = False, **grid_kw):
        display = f"{text}{'  *' if required else ''}:"
        fg = UI["warning"] if required else "#333333"
        tk.Label(
            parent,
            text=display,
            font=("Segoe UI", 9),
            bg=UI["panel"],
            fg=fg,
            anchor="e",
        ).grid(row=row, column=col, sticky="e", padx=(0, 6), pady=3, **grid_kw)

    def _build_top_form(self) -> None:
        outer = tk.Frame(self, bg=UI["bg"])
        outer.pack(fill="x", padx=10, pady=(6, 2))
        outer.grid_columnconfigure(0, weight=1)
        outer.grid_columnconfigure(1, weight=2)

        self._build_period_panel(outer)
        self._build_object_panel(outer)

    def _build_period_panel(self, parent) -> None:
        pnl = tk.LabelFrame(
            parent,
            text=" 📅 Период ",
            font=("Segoe UI", 9, "bold"),
            bg=UI["panel"],
            fg=UI["accent"],
            relief="groove",
            bd=1,
            padx=10,
            pady=8,
        )
        pnl.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=2)

        self._ts_lbl(pnl, "Месяц", 0, required=True)
        self.cmb_month = ttk.Combobox(
            pnl,
            width=16,
            state="readonly",
            values=[f"{m:02d} — {MONTH_NAMES[m]}" for m in range(1, 13)],
        )
        self.cmb_month.grid(row=0, column=1, sticky="w", pady=3)
        self.cmb_month.bind("<<ComboboxSelected>>", lambda _e: self._on_month_combo_changed())
        self.cmb_month.current(max(0, self.var_month.get() - 1))

        tk.Label(
            pnl,
            text="Год  *:",
            font=("Segoe UI", 9),
            bg=UI["panel"],
            fg=UI["warning"],
            anchor="e",
        ).grid(row=0, column=2, sticky="e", padx=(12, 6), pady=3)

        self.cmb_year = ttk.Combobox(
            pnl,
            width=8,
            state="readonly",
            values=self._make_year_values(),
            textvariable=self.var_year,
        )
        self.cmb_year.grid(row=0, column=3, sticky="w", pady=3)
        self.cmb_year.bind("<<ComboboxSelected>>", lambda _e: self._on_period_changed())

    def _build_object_panel(self, parent) -> None:
        pnl = tk.LabelFrame(
            parent,
            text=" 📍 Объект ",
            font=("Segoe UI", 9, "bold"),
            bg=UI["panel"],
            fg=UI["accent"],
            relief="groove",
            bd=1,
            padx=10,
            pady=8,
        )
        pnl.grid(row=0, column=1, sticky="nsew", padx=(4, 0), pady=2)
        pnl.grid_columnconfigure(1, weight=1)

        self._ts_lbl(pnl, "Адрес объекта", 0, required=True)
        self.cmb_address = AutoCompleteCombobox(pnl, width=42, font=("Segoe UI", 9))
        self.cmb_address.grid(row=0, column=1, columnspan=3, sticky="ew", pady=3)
        self.cmb_address.bind("<<ComboboxSelected>>", lambda _e: self._on_address_select())
        self.cmb_address.bind("<Return>", lambda _e: self._on_address_select())

        self._ts_lbl(pnl, "ID объекта", 1)
        id_frame = tk.Frame(pnl, bg=UI["panel"])
        id_frame.grid(row=1, column=1, columnspan=3, sticky="ew", pady=3)

        self.cmb_object_id = ttk.Combobox(id_frame, state="readonly", values=[], width=22)
        self.cmb_object_id.pack(side="left")
        self.cmb_object_id.bind("<<ComboboxSelected>>", lambda _e: self._on_object_id_select())

        tk.Label(
            id_frame,
            text="← подставляется автоматически по адресу",
            font=("Segoe UI", 7),
            fg="#888888",
            bg=UI["panel"],
        ).pack(side="left", padx=8)

    def _ts_btn(self, parent, text: str, cmd, side="left", padx=3, pady=0, width=None):
        b = ttk.Button(parent, text=text, command=cmd, width=width)
        b.pack(side=side, padx=padx, pady=pady)
        return b

    def _build_toolbar(self) -> None:
        bar = tk.Frame(self, bg=UI["panel2"], relief="flat")
        bar.pack(fill="x", padx=10, pady=(4, 0))

        left = tk.Frame(bar, bg=UI["panel2"])
        left.pack(side="left", fill="x", expand=True)

        actions = tk.Frame(bar, bg=UI["panel2"])
        actions.pack(side="right", anchor="n", padx=(10, 4), pady=4)

        row1 = tk.Frame(left, bg=UI["panel2"])
        row1.pack(fill="x", pady=(4, 4))

        self._ts_btn(row1, "Открыть", self._open_timesheet, side="left", padx=(4, 3))
        self._ts_btn(row1, "Добавить сотрудников", self._add_employees, side="left", padx=3)
        self._ts_btn(row1, "Удалить выбранных", self._delete_selected_rows, side="left", padx=3)
        self._ts_btn(row1, "Очистить часы", self._clear_hours_for_selected, side="left", padx=3)
        self._ts_btn(row1, "Проверить дубли", self._check_duplicates, side="left", padx=3)

        btn_save = tk.Button(
            actions,
            text="Сохранить",
            font=("Segoe UI", 9, "bold"),
            bg=UI["btn_save_bg"],
            fg=UI["btn_save_fg"],
            activebackground="#0d47a1",
            activeforeground="white",
            relief="flat",
            cursor="hand2",
            padx=14,
            pady=4,
            command=self._save_timesheet,
            width=14,
        )
        btn_save.pack(fill="x", pady=(0, 4))
        btn_save.bind("<Enter>", lambda _e: btn_save.config(bg="#0d47a1"))
        btn_save.bind("<Leave>", lambda _e: btn_save.config(bg=UI["btn_save_bg"]))

    def _build_filter_bar(self) -> None:
        bar = tk.Frame(self, bg=UI["bg"], pady=2)
        bar.pack(fill="x", padx=10, pady=(4, 0))

        tk.Label(
            bar,
            text="🔍 Поиск (ФИО / таб. №):",
            font=("Segoe UI", 9),
            bg=UI["bg"],
        ).pack(side="left")

        ent_filter = ttk.Entry(bar, textvariable=self.var_filter, width=36)
        ent_filter.pack(side="left", padx=(4, 8))
        ent_filter.bind("<KeyRelease>", lambda _e: self._apply_filter())

        ttk.Button(bar, text="Очистить", command=self._clear_filter).pack(side="left")

    def _build_grid(self) -> None:
        grid_wrap = tk.Frame(
            self,
            bg=UI["panel"],
            highlightbackground=UI["line"],
            highlightthickness=1,
            bd=0,
        )
        grid_wrap.pack(fill="both", expand=True, padx=10, pady=(4, 4))

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

    def _build_bottom(self) -> None:
        bottom = tk.Frame(self, bg=UI["panel2"], pady=5)
        bottom.pack(fill="x", padx=10, pady=(0, 8))

        self.lbl_totals = tk.Label(
            bottom,
            text="Сотрудников: 0 | Дней: 0 | Часов: 0",
            font=("Segoe UI", 9, "bold"),
            fg=UI["accent"],
            bg=UI["panel2"],
        )
        self.lbl_totals.pack(side="left", padx=10)

        self.lbl_trip_info = tk.Label(
            bottom,
            textvariable=self.var_trip_info,
            font=("Segoe UI", 9),
            fg=UI["muted"],
            bg=UI["panel2"],
        )
        self.lbl_trip_info.pack(side="right", padx=10)

    def _make_year_values(self) -> List[int]:
        current = date.today().year
        return list(range(current - 3, current + 4))

    # =========================================================
    # Справочники
    # =========================================================
    def _load_reference_data(self) -> None:
        self.objects_full = load_objects_short_for_timesheet()
        self.address_options = sorted(
            {
                normalize_spaces(addr)
                for _object_id, addr, _short_name in self.objects_full
                if normalize_spaces(addr)
            }
        )
        self.cmb_address.set_completion_list(self.address_options)

    def _parse_selected_object(self) -> Tuple[str, str]:
        object_addr = normalize_spaces(self.cmb_address.get() or "")
        object_id = normalize_spaces(self.cmb_object_id.get() or "")
        return object_id, object_addr

    def _sync_object_id_values_silent(self) -> None:
        addr = normalize_spaces(self.cmb_address.get() or "")
        objects_for_addr = [
            (code, a, short_name)
            for (code, a, short_name) in self.objects_full
            if normalize_spaces(a) == addr
        ]

        if not objects_for_addr:
            self.cmb_object_id.config(state="normal", values=[])
            self.cmb_object_id.set("")
            return

        ids = sorted({normalize_spaces(code) for code, _, _ in objects_for_addr if normalize_spaces(code)})
        cur = normalize_spaces(self.cmb_object_id.get() or "")

        self.cmb_object_id.config(state="readonly", values=ids)

        if cur and cur in ids:
            return
        if len(ids) == 1:
            self.cmb_object_id.set(ids[0])
        else:
            if cur not in ids:
                self.cmb_object_id.set("")

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

    def _on_address_select(self) -> None:
        self._sync_object_id_values_silent()

        addr = normalize_spaces(self.cmb_address.get() or "")
        objects_for_addr = [
            (normalize_spaces(code), normalize_spaces(a), normalize_spaces(short_name))
            for (code, a, short_name) in self.objects_full
            if normalize_spaces(a) == addr
        ]

        ids = sorted({code for code, _, _ in objects_for_addr if code})
        if len(ids) <= 1:
            return

        dlg = SelectObjectIdDialog(self, objects_for_addr, addr)
        self.wait_window(dlg)

        selected_id = normalize_spaces(dlg.result or "")
        if selected_id and selected_id in ids:
            self.cmb_object_id.set(selected_id)

    def _on_object_id_select(self) -> None:
        pass

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
            "work_schedule": normalize_spaces(rec.get("work_schedule") or ""),
        }
        return out

    def _recalc_all_totals(self) -> None:
        year, month = self._get_year_month()
        for rec in self.rows:
            rec["hours"] = normalize_hours_list(rec.get("hours"), year, month)
            rec["_totals"] = calc_row_totals(rec["hours"], year, month)

    def _refresh_grid(self) -> None:
        self._recalc_all_totals()
        visible_rows = self._get_filtered_rows()
        self.grid_widget.set_rows(visible_rows)
        self._recalc_bottom_totals()

    def _set_rows(self, rows: Sequence[Dict[str, Any]]) -> None:
        self.rows = [self._normalize_trip_row(dict(r)) for r in rows]
        self._refresh_grid()

    def _get_filtered_rows(self) -> List[Dict[str, Any]]:
        q = normalize_spaces(self.var_filter.get() or "").lower()
        if not q:
            return self.rows

        filtered = []
        for rec in self.rows:
            fio = normalize_spaces(rec.get("fio") or "").lower()
            tbn = normalize_tbn(rec.get("tbn")).lower()
            if q in fio or q in tbn:
                filtered.append(rec)
        return filtered

    def _apply_filter(self) -> None:
        self._refresh_grid()
        self._update_trip_info_from_selection()

    def _clear_filter(self) -> None:
        self.var_filter.set("")
        self._apply_filter()

    def _recalc_bottom_totals(self) -> None:
        year, month = self._get_year_month()
        summary = calc_rows_summary(self.rows, year, month)
        txt = (
            f"Сотрудников: {summary['employees']}  |  "
            f"Дней: {summary['days']}  |  "
            f"Часов: {format_summary_value(summary['hours'])}"
        )
        self.lbl_totals.config(text=txt)

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
    def _get_visible_rows(self) -> List[Dict[str, Any]]:
        return self._get_filtered_rows()

    def _visible_to_real_index(self, visible_index: int) -> Optional[int]:
        visible_rows = self._get_visible_rows()
        if not (0 <= visible_index < len(visible_rows)):
            return None

        rec = visible_rows[visible_index]
        for i, row in enumerate(self.rows):
            if row is rec:
                return i
        return None

    def _on_grid_change(self, row_index: int, col_index: int) -> None:
        real_index = self._visible_to_real_index(row_index)
        if real_index is None:
            return

        year, month = self._get_year_month()
        rec = self.rows[real_index]
        rec["hours"] = normalize_hours_list(rec.get("hours"), year, month)
        rec["_totals"] = calc_row_totals(rec["hours"], year, month)

        self._refresh_grid()
        self._update_trip_info_from_selection()

    def _on_delete_row(self, row_index: int) -> None:
        real_index = self._visible_to_real_index(row_index)
        if real_index is None:
            return

        rec = self.rows[real_index]
        fio = normalize_spaces(rec.get("fio") or "")
        tbn = normalize_tbn(rec.get("tbn"))

        if not messagebox.askyesno(
            "Удаление строки",
            f"Удалить строку сотрудника:\n{fio} ({tbn})?",
            parent=self,
        ):
            return

        del self.rows[real_index]
        self._refresh_grid()
        self._update_trip_info_from_selection()

    def _on_selection_change(self, selected_rows) -> None:
        self._update_trip_info_from_selection()

    def _on_trip_period_click(self, row_index: int) -> None:
        real_index = self._visible_to_real_index(row_index)
        if real_index is None:
            return

        rec = self.rows[real_index]
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
        self._update_trip_info_from_selection()

    # =========================================================
    # Действия над выбранными строками
    # =========================================================
    def _get_selected_row_indexes(self) -> List[int]:
        try:
            selected = list(self.grid_widget.get_selected_rows())
        except Exception:
            selected = []
        return sorted(i for i in selected if 0 <= i < len(self._get_visible_rows()))

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

        real_indexes = [self._visible_to_real_index(i) for i in indexes]
        real_indexes = sorted([i for i in real_indexes if i is not None], reverse=True)

        for idx in real_indexes:
            del self.rows[idx]

        self._refresh_grid()
        self._update_trip_info_from_selection()
        self.var_status.set(f"Удалено строк: {len(real_indexes)}")

    def _clear_hours_for_selected(self) -> None:
        indexes = self._get_selected_row_indexes()
        if not indexes:
            messagebox.showinfo("Очистка часов", "Не выбраны строки.", parent=self)
            return

        year, month = self._get_year_month()
        real_indexes = [self._visible_to_real_index(i) for i in indexes]
        real_indexes = [i for i in real_indexes if i is not None]

        for idx in real_indexes:
            self.rows[idx]["hours"] = [None] * 31
            self.rows[idx]["_totals"] = calc_row_totals(self.rows[idx]["hours"], year, month)

        self._refresh_grid()
        self.var_status.set(f"Очищены часы у строк: {len(real_indexes)}")

    # =========================================================
    # Нижняя информационная строка
    # =========================================================
    def _update_trip_info_from_selection(self) -> None:
        indexes = self._get_selected_row_indexes()
        visible_rows = self._get_visible_rows()

        if len(indexes) != 1:
            self.var_trip_info.set("")
            return

        idx = indexes[0]
        if not (0 <= idx < len(visible_rows)):
            self.var_trip_info.set("")
            return

        rec = visible_rows[idx]
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
    # Внешние helpers
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

        self.cmb_address.set(object_addr_norm)
        self._sync_object_id_values_silent()

        if object_id_norm:
            values = list(self.cmb_object_id.cget("values") or [])
            if object_id_norm not in values:
                values.append(object_id_norm)
                self.cmb_object_id.config(values=values)
            self.cmb_object_id.set(object_id_norm)
        else:
            self.cmb_object_id.set("")

        self._open_timesheet()

    def add_empty_row(self) -> None:
        self.rows.append(self._empty_row())
        self._refresh_grid()
        self.var_status.set("Добавлена пустая строка.")
