from __future__ import annotations

import calendar
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import date
from typing import Optional


class TripPeriodDialog(tk.Toplevel):
    def __init__(
        self,
        master,
        *,
        initial_date_from: Optional[date] = None,
        initial_date_to: Optional[date] = None,
        year: Optional[int] = None,
        month: Optional[int] = None,
    ):
        super().__init__(master)
        self.title("Период командировки")
        self.resizable(False, False)
        self.transient(master)

        self.result: Optional[tuple[Optional[date], Optional[date]]] = None

        today = date.today()
        init_from = initial_date_from
        init_to = initial_date_to

        base_year = year or (init_from.year if init_from else today.year)
        base_month = month or (init_from.month if init_from else today.month)

        self.var_from_day = tk.StringVar(value=str(init_from.day) if init_from else "")
        self.var_from_month = tk.StringVar(value=str(init_from.month) if init_from else str(base_month))
        self.var_from_year = tk.StringVar(value=str(init_from.year) if init_from else str(base_year))

        self.var_to_day = tk.StringVar(value=str(init_to.day) if init_to else "")
        self.var_to_month = tk.StringVar(value=str(init_to.month) if init_to else str(base_month))
        self.var_to_year = tk.StringVar(value=str(init_to.year) if init_to else str(base_year))

        self._build_ui()
        self._center(master)

        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

        self.bind("<Return>", lambda _e: self._on_ok())
        self.bind("<Escape>", lambda _e: self._on_cancel())

        self.grab_set()
        self.focus_set()

    def _build_ui(self) -> None:
        root = ttk.Frame(self, padding=12)
        root.pack(fill="both", expand=True)

        frm_from = ttk.LabelFrame(root, text="Дата начала", padding=10)
        frm_from.pack(fill="x", expand=True)

        self._build_date_row(
            frm_from,
            self.var_from_day,
            self.var_from_month,
            self.var_from_year,
        )

        frm_to = ttk.LabelFrame(root, text="Дата окончания", padding=10)
        frm_to.pack(fill="x", expand=True, pady=(10, 0))

        self._build_date_row(
            frm_to,
            self.var_to_day,
            self.var_to_month,
            self.var_to_year,
        )

        info = ttk.Label(
            root,
            text="Можно задать пустой период через кнопку «Очистить».",
            foreground="#666666",
        )
        info.pack(anchor="w", pady=(10, 0))

        btns = ttk.Frame(root)
        btns.pack(fill="x", pady=(12, 0))

        ttk.Button(btns, text="Очистить", command=self._on_clear).pack(side="left")
        ttk.Button(btns, text="Отмена", command=self._on_cancel).pack(side="right")
        ttk.Button(btns, text="OK", command=self._on_ok).pack(side="right", padx=(0, 8))

    def _build_date_row(
        self,
        master,
        var_day: tk.StringVar,
        var_month: tk.StringVar,
        var_year: tk.StringVar,
    ) -> None:
        row = ttk.Frame(master)
        row.pack(fill="x")

        ttk.Label(row, text="День").pack(side="left")
        e_day = ttk.Entry(row, textvariable=var_day, width=6)
        e_day.pack(side="left", padx=(6, 12))

        ttk.Label(row, text="Месяц").pack(side="left")
        e_month = ttk.Entry(row, textvariable=var_month, width=6)
        e_month.pack(side="left", padx=(6, 12))

        ttk.Label(row, text="Год").pack(side="left")
        e_year = ttk.Entry(row, textvariable=var_year, width=8)
        e_year.pack(side="left", padx=(6, 0))

    def _center(self, master) -> None:
        self.update_idletasks()

        if master is not None:
            try:
                mx = master.winfo_rootx()
                my = master.winfo_rooty()
                mw = master.winfo_width()
                mh = master.winfo_height()
            except Exception:
                mx = my = 100
                mw = 900
                mh = 700
        else:
            mx = my = 100
            mw = 900
            mh = 700

        w = self.winfo_reqwidth()
        h = self.winfo_reqheight()

        x = mx + max(0, (mw - w) // 2)
        y = my + max(0, (mh - h) // 2)

        self.geometry(f"+{x}+{y}")

    def _parse_date(
        self,
        day_var: tk.StringVar,
        month_var: tk.StringVar,
        year_var: tk.StringVar,
        label: str,
    ) -> Optional[date]:
        day_raw = (day_var.get() or "").strip()
        month_raw = (month_var.get() or "").strip()
        year_raw = (year_var.get() or "").strip()

        if not day_raw and not month_raw and not year_raw:
            return None

        if not day_raw or not month_raw or not year_raw:
            raise ValueError(f"{label}: заполните день, месяц и год полностью.")

        try:
            d = int(day_raw)
            m = int(month_raw)
            y = int(year_raw)
        except Exception:
            raise ValueError(f"{label}: день, месяц и год должны быть числами.")

        if y < 2000 or y > 2100:
            raise ValueError(f"{label}: год вне допустимого диапазона.")

        if m < 1 or m > 12:
            raise ValueError(f"{label}: месяц должен быть от 1 до 12.")

        max_day = calendar.monthrange(y, m)[1]
        if d < 1 or d > max_day:
            raise ValueError(f"{label}: день должен быть от 1 до {max_day}.")

        return date(y, m, d)

    def _on_ok(self) -> None:
        try:
            d_from = self._parse_date(
                self.var_from_day,
                self.var_from_month,
                self.var_from_year,
                "Дата начала",
            )
            d_to = self._parse_date(
                self.var_to_day,
                self.var_to_month,
                self.var_to_year,
                "Дата окончания",
            )
        except ValueError as exc:
            messagebox.showerror("Ошибка", str(exc), parent=self)
            return

        if (d_from is None) != (d_to is None):
            messagebox.showerror(
                "Ошибка",
                "Либо заполните обе даты периода, либо очистите обе.",
                parent=self,
            )
            return

        if d_from and d_to and d_from > d_to:
            messagebox.showerror(
                "Ошибка",
                "Дата начала не может быть позже даты окончания.",
                parent=self,
            )
            return

        self.result = (d_from, d_to)
        self.destroy()

    def _on_clear(self) -> None:
        self.result = (None, None)
        self.destroy()

    def _on_cancel(self) -> None:
        self.result = None
        self.destroy()

    @classmethod
    def show(
        cls,
        master,
        *,
        initial_date_from: Optional[date] = None,
        initial_date_to: Optional[date] = None,
        year: Optional[int] = None,
        month: Optional[int] = None,
    ) -> Optional[tuple[Optional[date], Optional[date]]]:
        dlg = cls(
            master,
            initial_date_from=initial_date_from,
            initial_date_to=initial_date_to,
            year=year,
            month=month,
        )
        dlg.wait_window()
        return dlg.result
