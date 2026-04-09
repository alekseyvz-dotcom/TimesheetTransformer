from __future__ import annotations

import tkinter as tk
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from openpyxl import Workbook
from tkinter import filedialog, messagebox, simpledialog, ttk

from timesheet_common import (
    MAX_HOURS_PER_DAY,
    SPECIAL_CODES,
    TS_COLORS,
    is_allowed_timesheet_code,
    month_name_ru,
    normalize_code,
    normalize_spaces,
    parse_hours_value,
)

# ============================================================
# Общие helpers
# ============================================================


def center_toplevel(win: tk.Toplevel, parent: tk.Misc | None = None) -> None:
    try:
        win.update_idletasks()

        if parent is not None and parent.winfo_exists():
            px = parent.winfo_rootx()
            py = parent.winfo_rooty()
            pw = parent.winfo_width()
            ph = parent.winfo_height()
            sw = win.winfo_width()
            sh = win.winfo_height()
            x = px + max(0, (pw - sw) // 2)
            y = py + max(0, (ph - sh) // 2)
            win.geometry(f"+{x}+{y}")
            return

        screen_w = win.winfo_screenwidth()
        screen_h = win.winfo_screenheight()
        sw = win.winfo_width()
        sh = win.winfo_height()
        x = max(0, (screen_w - sw) // 2)
        y = max(0, (screen_h - sh) // 2)
        win.geometry(f"+{x}+{y}")
    except Exception:
        pass


def setup_modal_window(win: tk.Toplevel, parent: tk.Misc | None = None) -> None:
    try:
        if parent is not None:
            win.transient(parent)
    except Exception:
        pass

    try:
        win.grab_set()
    except Exception:
        pass

    try:
        win.focus_set()
    except Exception:
        pass

    center_toplevel(win, parent)


# ============================================================
# Диалог предупреждения о подозрительных часах
# ============================================================


class SuspiciousHoursWarningDialog(tk.Toplevel):
    """
    Показывает список подозрительных значений (> MAX_HOURS_PER_DAY)
    и спрашивает, продолжать ли сохранение.
    """

    def __init__(self, parent, suspicious: List[Dict[str, Any]], context: str = "сохранении"):
        super().__init__(parent)
        self.title(f"⚠️ Подозрительные значения часов (>{MAX_HOURS_PER_DAY} ч.)")
        self.resizable(True, True)
        self.result: Optional[bool] = None

        setup_modal_window(self, parent)

        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.bind("<Escape>", lambda _e: self._on_cancel())

        main = tk.Frame(self, padx=12, pady=12)
        main.pack(fill="both", expand=True)

        tk.Label(
            main,
            text=(
                f"⚠️ Обнаружено {len(suspicious)} подозрительных значений\n"
                f"(более {MAX_HOURS_PER_DAY} часов в сутки).\n\n"
                f"Возможно, пропущена точка/запятая (например, 825 вместо 8.25).\n"
                f"Проверьте значения перед {context}:"
            ),
            justify="left",
            font=("Segoe UI", 10),
            fg=TS_COLORS["warning"],
        ).pack(anchor="w", pady=(0, 8))

        table_frame = tk.Frame(main)
        table_frame.pack(fill="both", expand=True)

        cols = ("fio", "tbn", "day", "raw", "parsed")
        tree = ttk.Treeview(
            table_frame,
            columns=cols,
            show="headings",
            height=min(15, max(5, len(suspicious))),
        )
        tree.heading("fio", text="ФИО")
        tree.heading("tbn", text="Таб.№")
        tree.heading("day", text="День")
        tree.heading("raw", text="Значение в ячейке")
        tree.heading("parsed", text="Распознано (ч.)")

        tree.column("fio", width=250, anchor="w")
        tree.column("tbn", width=100, anchor="center")
        tree.column("day", width=60, anchor="center")
        tree.column("raw", width=150, anchor="center")
        tree.column("parsed", width=130, anchor="center")

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)

        tree.pack(side="left", fill="both", expand=True, pady=(0, 8))
        vsb.pack(side="right", fill="y", pady=(0, 8))

        for item in suspicious[:100]:
            parsed = item.get("parsed")
            parsed_str = f"{parsed:.2f}" if isinstance(parsed, (int, float)) else "?"
            tree.insert(
                "",
                "end",
                values=(
                    item.get("fio", ""),
                    item.get("tbn", ""),
                    item.get("day", ""),
                    item.get("raw", ""),
                    parsed_str,
                ),
            )

        if len(suspicious) > 100:
            tree.insert("", "end", values=("", "", "", f"... и ещё {len(suspicious) - 100}", ""))

        btn_frame = tk.Frame(main)
        btn_frame.pack(fill="x", pady=(8, 0))

        tk.Button(
            btn_frame,
            text="❌ Отмена (исправить значения)",
            font=("Segoe UI", 10, "bold"),
            bg=TS_COLORS["warning"],
            fg="white",
            activebackground="#880000",
            activeforeground="white",
            relief="flat",
            cursor="hand2",
            padx=14,
            pady=6,
            command=self._on_cancel,
        ).pack(side="left", padx=(0, 8))

        tk.Button(
            btn_frame,
            text="⚠️ Всё равно сохранить",
            font=("Segoe UI", 10),
            bg="#FF9800",
            fg="white",
            activebackground="#E65100",
            activeforeground="white",
            relief="flat",
            cursor="hand2",
            padx=14,
            pady=6,
            command=self._on_continue,
        ).pack(side="right")

        center_toplevel(self, parent)

    def _on_cancel(self):
        self.result = False
        self.destroy()

    def _on_continue(self):
        self.result = True
        self.destroy()


# ============================================================
# Выбор даты СКУД
# ============================================================


class SelectDateDialog(simpledialog.Dialog):
    def __init__(self, parent, init_date: date):
        self.init_date = init_date
        self.result: Optional[date] = None
        self._selected_date: Optional[date] = None
        super().__init__(parent, title="Выбор даты (СКУД)")

    def body(self, master):
        tk.Label(master, text="Дата (дд.мм.гггг):").grid(
            row=0, column=0, sticky="e", padx=(0, 6), pady=(4, 4)
        )
        self.ent = ttk.Entry(master, width=16)
        self.ent.grid(row=0, column=1, sticky="w", pady=(4, 4))
        self.ent.insert(0, self.init_date.strftime("%d.%m.%Y"))
        return self.ent

    def validate(self):
        s = normalize_spaces(self.ent.get())
        try:
            self._selected_date = datetime.strptime(s, "%d.%m.%Y").date()
            return True
        except Exception:
            messagebox.showwarning("СКУД", "Введите дату в формате дд.мм.гггг", parent=self)
            return False

    def apply(self):
        self.result = self._selected_date


# ============================================================
# Копирование из месяца
# ============================================================


class CopyFromDialog(simpledialog.Dialog):
    def __init__(self, parent, init_year: int, init_month: int):
        self.init_year = init_year
        self.init_month = init_month
        self.result: Optional[Dict[str, Any]] = None
        super().__init__(parent, title="Копировать сотрудников из месяца")

    def body(self, master):
        tk.Label(master, text="Источник").grid(row=0, column=0, sticky="w", pady=(2, 6), columnspan=4)

        tk.Label(master, text="Месяц:").grid(row=1, column=0, sticky="e")
        self.cmb_month = ttk.Combobox(
            master,
            state="readonly",
            width=18,
            values=[month_name_ru(i) for i in range(1, 13)],
        )
        self.cmb_month.grid(row=1, column=1, sticky="w")
        self.cmb_month.current(max(0, min(11, self.init_month - 1)))

        tk.Label(master, text="Год:").grid(row=1, column=2, sticky="e", padx=(10, 4))
        self.spn_year = tk.Spinbox(master, from_=2000, to=2100, width=6)
        self.spn_year.grid(row=1, column=3, sticky="w")
        self.spn_year.delete(0, "end")
        self.spn_year.insert(0, str(self.init_year))

        self.var_copy_hours = tk.BooleanVar(value=False)
        ttk.Checkbutton(master, text="Копировать часы", variable=self.var_copy_hours).grid(
            row=2, column=1, sticky="w", pady=(8, 2)
        )

        tk.Label(master, text="Режим:").grid(row=3, column=0, sticky="e", pady=(6, 2))
        self.var_mode = tk.StringVar(value="replace")
        frame_mode = tk.Frame(master)
        frame_mode.grid(row=3, column=1, columnspan=3, sticky="w", pady=(6, 2))
        ttk.Radiobutton(frame_mode, text="Заменить текущий список", value="replace", variable=self.var_mode).pack(anchor="w")
        ttk.Radiobutton(frame_mode, text="Объединить (добавить недостающих)", value="merge", variable=self.var_mode).pack(anchor="w")

        return self.cmb_month

    def validate(self):
        try:
            year = int(self.spn_year.get())
            if not (2000 <= year <= 2100):
                raise ValueError
            return True
        except Exception:
            messagebox.showwarning("Копирование", "Введите корректный год (2000–2100).", parent=self)
            return False

    def apply(self):
        self.result = {
            "year": int(self.spn_year.get()),
            "month": self.cmb_month.current() + 1,
            "with_hours": bool(self.var_copy_hours.get()),
            "mode": self.var_mode.get(),
        }


# ============================================================
# Выбор ID объекта
# ============================================================


class SelectObjectIdDialog(tk.Toplevel):
    def __init__(self, parent, objects_for_addr: Sequence[Tuple[str, str, str]], addr: str):
        super().__init__(parent)
        self.title("Выбор ID объекта")
        self.resizable(True, True)
        self.result: Optional[str] = None

        setup_modal_window(self, parent)

        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.bind("<Escape>", lambda _e: self._on_cancel())

        main = tk.Frame(self, padx=10, pady=10)
        main.pack(fill="both", expand=True)

        tk.Label(
            main,
            text=f"По адресу:\n{addr}\nнайдено несколько объектов.\nВыберите нужный ID:",
            justify="left",
        ).pack(anchor="w")

        cols = ("excel_id", "address", "short_name")
        self.tree = ttk.Treeview(main, columns=cols, show="headings", height=8, selectmode="browse")
        self.tree.heading("excel_id", text="ID (excel_id)")
        self.tree.heading("address", text="Адрес")
        self.tree.heading("short_name", text="Краткое имя")

        self.tree.column("excel_id", width=120, anchor="center", stretch=False)
        self.tree.column("address", width=260, anchor="w")
        self.tree.column("short_name", width=200, anchor="w")

        vsb = ttk.Scrollbar(main, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True, pady=(8, 4))
        vsb.pack(side="right", fill="y")

        for code, a, short_name in objects_for_addr:
            self.tree.insert("", "end", values=(code, a, short_name))

        btns = tk.Frame(main)
        btns.pack(fill="x", pady=(6, 0))
        ttk.Button(btns, text="OK", command=self._on_ok).pack(side="right", padx=(4, 0))
        ttk.Button(btns, text="Отмена", command=self._on_cancel).pack(side="right")

        self.tree.bind("<Double-1>", self._on_ok)
        self.tree.bind("<Return>", self._on_ok)

        center_toplevel(self, parent)

    def _on_ok(self, event=None):
        sel = self.tree.selection()
        if not sel:
            messagebox.showwarning("Выбор ID объекта", "Сначала выберите строку.", parent=self)
            return
        vals = self.tree.item(sel[0], "values")
        if not vals:
            return
        self.result = vals[0]
        self.destroy()

    def _on_cancel(self, event=None):
        self.result = None
        self.destroy()


# ============================================================
# Проверка сопоставления СКУД
# ============================================================


class SkudMappingReviewDialog(tk.Toplevel):
    def __init__(self, parent, rows: List[Dict[str, Any]], problems: List[Dict[str, Any]]):
        super().__init__(parent)
        self.parent = parent
        self.title("СКУД — проверка сопоставления перед применением")
        self.resizable(True, True)
        self.result: Optional[Dict[str, Any]] = None
        self._rows = rows
        self._problems = problems
        self._apply_state: Dict[str, bool] = {}

        setup_modal_window(self, parent)

        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.bind("<Escape>", lambda _e: self._on_cancel())

        main = tk.Frame(self, padx=10, pady=10)
        main.pack(fill="both", expand=True)

        tk.Label(
            main,
            text=(
                "Проверьте сопоставления. Снимите галочки с неверных строк.\n"
                "Проблемы (нет входа/выхода) показаны ниже и НЕ применяются."
            ),
            justify="left",
        ).pack(anchor="w")

        table_frame = tk.Frame(main)
        table_frame.pack(fill="both", expand=True, pady=(8, 6))

        cols = ("apply", "skud_fio", "matched_fio", "score", "hours", "interval", "counts")
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=12)

        self.tree.heading("apply", text="Применять")
        self.tree.heading("skud_fio", text="ФИО из СКУД")
        self.tree.heading("matched_fio", text="Сопоставлено")
        self.tree.heading("score", text="Score")
        self.tree.heading("hours", text="Часы")
        self.tree.heading("interval", text="Интервал")
        self.tree.heading("counts", text="Вх/Вых")

        self.tree.column("apply", width=90, anchor="center", stretch=False)
        self.tree.column("skud_fio", width=280, anchor="w")
        self.tree.column("matched_fio", width=280, anchor="w")
        self.tree.column("score", width=70, anchor="center", stretch=False)
        self.tree.column("hours", width=60, anchor="center", stretch=False)
        self.tree.column("interval", width=220, anchor="w")
        self.tree.column("counts", width=70, anchor="center", stretch=False)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.tag_configure("low_score", foreground="#b00020")
        self.tree.tag_configure("normal", foreground="#000000")

        for i, row in enumerate(self._rows):
            iid = f"m_{i}"
            apply_default = bool(row.get("apply", True))
            self._apply_state[iid] = apply_default

            score = row.get("score")
            score_str = f"{score:.2f}" if isinstance(score, (int, float)) else ""
            hours = row.get("hours_rounded")
            hours_str = str(hours) if hours is not None else ""

            first_in = row.get("first_in")
            last_out = row.get("last_out")
            interval = ""
            if isinstance(first_in, datetime) and isinstance(last_out, datetime):
                interval = f"{first_in.strftime('%H:%M:%S')} – {last_out.strftime('%H:%M:%S')}"

            counts = f"{row.get('count_in', 0)}/{row.get('count_out', 0)}"
            apply_mark = "[x]" if apply_default else "[ ]"

            tag = "normal"
            if isinstance(score, (int, float)) and score < 0.90:
                tag = "low_score"

            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    apply_mark,
                    row.get("skud_fio", ""),
                    row.get("matched_fio", ""),
                    score_str,
                    hours_str,
                    interval,
                    counts,
                ),
                tags=(tag,),
            )

        self.tree.bind("<Button-1>", self._on_click)
        self.tree.bind("<Double-1>", self._on_click)

        prob_frame = tk.LabelFrame(
            main,
            text="Проблемы (нет входа/выхода) — НЕ применяются",
            padx=8,
            pady=8,
        )
        prob_frame.pack(fill="both", expand=True, pady=(6, 8))

        if not self._problems:
            tk.Label(prob_frame, text="Проблем не найдено.").pack(anchor="w")
        else:
            txt = tk.Text(prob_frame, height=7, wrap="word")
            txt.pack(fill="both", expand=True)

            txt.insert("end", "Невозможно посчитать часы автоматически:\n\n")
            for p in self._problems:
                skud_fio = p.get("skud_fio", "")
                has_in = bool(p.get("has_in"))
                has_out = bool(p.get("has_out"))

                parts = []
                if has_in and not has_out:
                    parts.append("есть ВХОД, нет ВЫХОДА")
                if has_out and not has_in:
                    parts.append("есть ВЫХОД, нет ВХОДА")
                if not parts:
                    parts.append("аномальная последовательность событий")

                first_in = p.get("first_in")
                last_out = p.get("last_out")
                t_in = first_in.strftime("%H:%M:%S") if isinstance(first_in, datetime) else "-"
                t_out = last_out.strftime("%H:%M:%S") if isinstance(last_out, datetime) else "-"
                cnt_in = p.get("count_in", 0)
                cnt_out = p.get("count_out", 0)

                txt.insert(
                    "end",
                    f"- {skud_fio} ({'/'.join(parts)}) | Вх/Вых {cnt_in}/{cnt_out} | {t_in} – {t_out}\n",
                )

            txt.configure(state="disabled")

        btns = tk.Frame(main)
        btns.pack(fill="x")

        ttk.Button(btns, text="Отметить всех", command=self._select_all).pack(side="left")
        ttk.Button(btns, text="Снять всех", command=self._clear_all).pack(side="left", padx=(6, 0))
        ttk.Button(btns, text="Выгрузить проблемы в Excel", command=self._export_problems_to_excel).pack(side="left", padx=(6, 0))

        ttk.Button(btns, text="Применить", command=self._on_apply).pack(side="right", padx=(6, 0))
        ttk.Button(btns, text="Отмена", command=self._on_cancel).pack(side="right")

        center_toplevel(self, parent)

    def _toggle_iid(self, iid: str):
        current = bool(self._apply_state.get(iid, True))
        new_state = not current
        self._apply_state[iid] = new_state

        vals = list(self.tree.item(iid, "values"))
        if vals:
            vals[0] = "[x]" if new_state else "[ ]"
            self.tree.item(iid, values=tuple(vals))

    def _on_click(self, event=None):
        row_id = self.tree.identify_row(event.y) if event else None
        col = self.tree.identify_column(event.x) if event else None
        if not row_id or col != "#1":
            return
        self._toggle_iid(row_id)

    def _select_all(self):
        for iid in self.tree.get_children():
            self._apply_state[iid] = True
            vals = list(self.tree.item(iid, "values"))
            if vals:
                vals[0] = "[x]"
                self.tree.item(iid, values=tuple(vals))

    def _clear_all(self):
        for iid in self.tree.get_children():
            self._apply_state[iid] = False
            vals = list(self.tree.item(iid, "values"))
            if vals:
                vals[0] = "[ ]"
                self.tree.item(iid, values=tuple(vals))

    def _export_problems_to_excel(self):
        if not self._problems:
            messagebox.showinfo("СКУД", "Проблем нет — выгружать нечего.", parent=self)
            return

        default_name = f"СКУД_проблемы_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        path = filedialog.asksaveasfilename(
            parent=self,
            title="Сохранить проблемы СКУД в Excel",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel", "*.xlsx"), ("Все файлы", "*.*")],
        )
        if not path:
            return

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Проблемы СКУД"

            header = [
                "ФИО (СКУД)",
                "Проблема",
                "Входов",
                "Выходов",
                "Первый вход",
                "Последний выход",
            ]
            ws.append(header)

            ws.column_dimensions["A"].width = 32
            ws.column_dimensions["B"].width = 28
            ws.column_dimensions["C"].width = 10
            ws.column_dimensions["D"].width = 10
            ws.column_dimensions["E"].width = 20
            ws.column_dimensions["F"].width = 20

            for p in self._problems:
                skud_fio = p.get("skud_fio", "") or ""
                has_in = bool(p.get("has_in"))
                has_out = bool(p.get("has_out"))

                if has_in and not has_out:
                    problem = "Есть вход, нет выхода"
                elif has_out and not has_in:
                    problem = "Есть выход, нет входа"
                else:
                    problem = "Аномальная последовательность"

                cnt_in = int(p.get("count_in") or 0)
                cnt_out = int(p.get("count_out") or 0)

                first_in = p.get("first_in")
                last_out = p.get("last_out")

                first_in_str = first_in.strftime("%d.%m.%Y %H:%M:%S") if isinstance(first_in, datetime) else ""
                last_out_str = last_out.strftime("%d.%m.%Y %H:%M:%S") if isinstance(last_out, datetime) else ""

                ws.append([skud_fio, problem, cnt_in, cnt_out, first_in_str, last_out_str])

            wb.save(path)
            messagebox.showinfo("СКУД", f"Проблемы сохранены:\n{path}", parent=self)
        except Exception as e:
            messagebox.showerror("СКУД", f"Ошибка выгрузки проблем в Excel:\n{e}", parent=self)

    def _on_apply(self):
        selected_rows = []
        for i, row in enumerate(self._rows):
            iid = f"m_{i}"
            if self._apply_state.get(iid, False):
                selected_rows.append(row)

        self.result = {"apply": True, "rows": selected_rows}
        self.destroy()

    def _on_cancel(self):
        self.result = {"apply": False, "rows": []}
        self.destroy()


# ============================================================
# Выбор сотрудников
# ============================================================

class SelectEmployeesDialog(tk.Toplevel):
    def __init__(self, parent, employees: Sequence[Tuple], current_dep: str):
        super().__init__(parent)
        self.parent = parent
        self.employees = list(employees)
        self.current_dep = normalize_spaces(current_dep)
        self.result: Optional[List[Tuple]] = None

        self.title("Выбор сотрудников")
        self.resizable(True, True)

        setup_modal_window(self, parent)

        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.bind("<Escape>", lambda _e: self._on_cancel())

        self.var_only_dep = tk.BooleanVar(value=bool(self.current_dep and self.current_dep != "Все"))
        self.var_search = tk.StringVar()

        main = tk.Frame(self, padx=10, pady=10)
        main.pack(fill="both", expand=True)

        top = tk.Frame(main)
        top.pack(fill="x")

        tk.Label(
            top,
            text=f"Подразделение: {self.current_dep or 'Все'}",
            font=("Segoe UI", 10, "bold"),
        ).grid(row=0, column=0, columnspan=2, sticky="w")

        ttk.Checkbutton(
            top,
            text="Показывать только сотрудников этого подразделения",
            variable=self.var_only_dep,
            command=self._refilter,
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 4))

        tk.Label(top, text="Поиск (ФИО / таб.№):").grid(row=2, column=0, sticky="w", pady=(4, 2))
        ent_search = ttk.Entry(top, textvariable=self.var_search, width=40)
        ent_search.grid(row=2, column=1, sticky="w", pady=(4, 2))
        ent_search.bind("<KeyRelease>", lambda _e: self._refilter())

        tbl_frame = tk.Frame(main)
        tbl_frame.pack(fill="both", expand=True, pady=(8, 4))

        columns = ("fio", "tbn", "pos", "dep")
        self.tree = ttk.Treeview(tbl_frame, columns=columns, show="headings", selectmode="none")
        self.tree.heading("fio", text="ФИО")
        self.tree.heading("tbn", text="Таб.№")
        self.tree.heading("pos", text="Должность")
        self.tree.heading("dep", text="Подразделение")

        self.tree.column("fio", width=260, anchor="w")
        self.tree.column("tbn", width=80, anchor="center", stretch=False)
        self.tree.column("pos", width=180, anchor="w")
        self.tree.column("dep", width=140, anchor="w")

        self.tree.tag_configure("checked", font=("Segoe UI", 9, "bold"))
        self.tree.tag_configure("unchecked", font=("Segoe UI", 9))

        vsb = ttk.Scrollbar(tbl_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.bind("<Button-1>", self._on_tree_click)
        self.tree.bind("<Double-1>", self._on_tree_click)

        self._filtered_indices: List[int] = []
        self._selected_indices: set[int] = set()

        sel_frame = tk.Frame(main)
        sel_frame.pack(fill="x")
        ttk.Button(sel_frame, text="Отметить всех", command=self._select_all).pack(side="left", padx=(0, 4))
        ttk.Button(sel_frame, text="Снять все", command=self._clear_all).pack(side="left", padx=4)

        self.lbl_selected = tk.Label(sel_frame, text="Выбрано: 0", bg=sel_frame["bg"])
        self.lbl_selected.pack(side="right")

        btns = tk.Frame(main)
        btns.pack(fill="x", pady=(8, 0))
        ttk.Button(btns, text="OK", command=self._on_ok).pack(side="right", padx=(4, 0))
        ttk.Button(btns, text="Отмена", command=self._on_cancel).pack(side="right")

        self._refilter()
        self._update_selected_count()
        center_toplevel(self, parent)

    def _unpack_employee(self, emp: Tuple) -> Tuple[str, str, str, str]:
        """
        Поддерживает и старый формат:
            (fio, tbn, pos, dep)
        и новый формат:
            (fio, tbn, pos, dep, work_schedule)
        """
        fio = normalize_spaces(emp[0] if len(emp) > 0 else "")
        tbn = normalize_spaces(emp[1] if len(emp) > 1 else "")
        pos = normalize_spaces(emp[2] if len(emp) > 2 else "")
        dep = normalize_spaces(emp[3] if len(emp) > 3 else "")
        return fio, tbn, pos, dep

    def _update_selected_count(self):
        try:
            self.lbl_selected.config(text=f"Выбрано: {len(self._selected_indices)}")
        except Exception:
            pass

    def _refilter(self):
        search = normalize_spaces(self.var_search.get()).lower()
        only_dep = bool(self.var_only_dep.get())
        dep_sel = self.current_dep

        self.tree.delete(*self.tree.get_children())
        self._filtered_indices.clear()

        for idx, emp in enumerate(self.employees):
            fio, tbn, pos, dep = self._unpack_employee(emp)

            if only_dep and dep_sel and dep_sel != "Все":
                if normalize_spaces(dep) != dep_sel:
                    continue

            if search:
                if search not in (fio or "").lower() and search not in (tbn or "").lower():
                    continue

            checked = idx in self._selected_indices
            display_fio = f"[{'x' if checked else ' '}] {fio}"

            iid = f"emp_{idx}"
            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(display_fio, tbn, pos, dep),
                tags=("checked" if checked else "unchecked",),
            )
            self._filtered_indices.append(idx)

        self._update_selected_count()

    def _toggle_index(self, idx: int):
        if idx in self._selected_indices:
            self._selected_indices.remove(idx)
        else:
            self._selected_indices.add(idx)
        self._update_selected_count()

    def _on_tree_click(self, event):
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return

        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return

        try:
            pos_in_view = self.tree.index(row_id)
            emp_index = self._filtered_indices[pos_in_view]
        except Exception:
            return

        self._toggle_index(emp_index)

        fio, tbn, pos, dep = self._unpack_employee(self.employees[emp_index])
        checked = emp_index in self._selected_indices
        display_fio = f"[{'x' if checked else ' '}] {fio}"
        self.tree.item(
            row_id,
            values=(display_fio, tbn, pos, dep),
            tags=("checked" if checked else "unchecked",),
        )

    def _select_all(self):
        for emp_index in self._filtered_indices:
            self._selected_indices.add(emp_index)
        self._refilter()

    def _clear_all(self):
        self._selected_indices.clear()
        self._refilter()

    def _on_ok(self):
        if not self._selected_indices:
            if not messagebox.askyesno(
                "Выбор сотрудников",
                "Не выбрано ни одного сотрудника.\nЗакрыть окно?",
                parent=self,
            ):
                return
            self.result = []
        else:
            self.result = [self.employees[i] for i in sorted(self._selected_indices)]
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()

# ============================================================
# Пакетное добавление
# ============================================================


class BatchAddDialog(tk.Toplevel):
    def __init__(self, parent, total: int, title: str = "Добавление сотрудников"):
        super().__init__(parent)
        self.parent = parent
        self.total = max(1, int(total))
        self.done = 0
        self.cancelled = False

        self.title(title)
        self.resizable(False, False)

        setup_modal_window(self, parent)

        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.bind("<Escape>", lambda _e: self._on_cancel())

        frm = tk.Frame(self, padx=12, pady=12)
        frm.pack(fill="both", expand=True)

        self.lbl = tk.Label(frm, text=f"Добавлено: 0 из {self.total}")
        self.lbl.pack(fill="x")

        self.pb = ttk.Progressbar(frm, mode="determinate", maximum=self.total, length=420)
        self.pb.pack(fill="x", pady=(8, 8))

        self.btn_cancel = ttk.Button(frm, text="Отмена", command=self._on_cancel)
        self.btn_cancel.pack(anchor="e", pady=(6, 0))

        center_toplevel(self, parent)

    def step(self, n: int = 1):
        if self.cancelled:
            return
        self.done += n
        if self.done > self.total:
            self.done = self.total
        self.pb["value"] = self.done
        self.lbl.config(text=f"Добавлено: {self.done} из {self.total}")
        self.update_idletasks()

    def _on_cancel(self):
        self.cancelled = True

    def close(self):
        try:
            self.grab_release()
        except Exception:
            pass
        self.destroy()


# ============================================================
# Проставить часы всем
# ============================================================


class HoursFillDialog(simpledialog.Dialog):
    def __init__(self, parent, max_day: int):
        self.max_day = int(max_day)
        self.result: Optional[Dict[str, Any]] = None
        super().__init__(parent, title="Проставить часы всем")

    def body(self, master):
        tk.Label(master, text=f"В текущем месяце дней: {self.max_day}").grid(
            row=0, column=0, columnspan=3, sticky="w", pady=(2, 6)
        )

        tk.Label(master, text="День:").grid(row=1, column=0, sticky="e")
        self.spn_day = tk.Spinbox(master, from_=1, to=max(31, self.max_day), width=4)
        self.spn_day.grid(row=1, column=1, sticky="w")
        self.spn_day.delete(0, "end")
        self.spn_day.insert(0, "1")

        self.var_clear = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            master,
            text="Очистить день (пусто)",
            variable=self.var_clear,
            command=self._on_toggle_clear,
        ).grid(row=2, column=1, sticky="w", pady=(6, 2))

        tk.Label(master, text="Часы:").grid(row=3, column=0, sticky="e", pady=(6, 0))
        self.ent_hours = ttk.Entry(master, width=12)
        self.ent_hours.grid(row=3, column=1, sticky="w", pady=(6, 0))
        self.ent_hours.insert(0, "8,25")

        tk.Label(master, text="Форматы: 8 | 8,25 | 8:30 | 1/7").grid(
            row=4, column=0, columnspan=3, sticky="w", pady=(6, 2)
        )
        return self.spn_day

    def _on_toggle_clear(self):
        self.ent_hours.configure(state="disabled" if self.var_clear.get() else "normal")

    def validate(self):
        try:
            d = int(self.spn_day.get())
            if not (1 <= d <= self.max_day):
                raise ValueError
        except Exception:
            messagebox.showwarning(
                "Проставить часы",
                f"День должен быть числом от 1 до {self.max_day}.",
                parent=self,
            )
            return False

        if self.var_clear.get():
            self._d = d
            self._h = 0.0
            self._clear = True
            return True

        hv = parse_hours_value(normalize_spaces(self.ent_hours.get()))
        if hv is None or hv < 0:
            messagebox.showwarning(
                "Проставить часы",
                "Введите корректное значение часов (например, 8, 8:30, 1/7).",
                parent=self,
            )
            return False

        self._d = d
        self._h = float(hv)
        self._clear = False
        return True

    def apply(self):
        self.result = {
            "day": self._d,
            "hours": self._h,
            "clear": self._clear,
        }

# ============================================================
# Автодополняемый combobox
# ============================================================


class AutoCompleteCombobox(ttk.Combobox):
    def __init__(self, master=None, **kw):
        super().__init__(master, **kw)
        self._all_values: List[str] = []
        self.bind("<KeyRelease>", self._on_keyrelease)
        self.bind("<Control-BackSpace>", self._clear_all)
        self.bind("<FocusOut>", self._on_focus_out)

    def set_values(self, values: Sequence[str]):
        self._all_values = list(values) if values is not None else []
        self.config(values=self._all_values)

    def set_completion_list(self, values: Sequence[str]):
        self.set_values(values)

    def _on_keyrelease(self, event):
        if event.keysym in ("BackSpace", "Left", "Right", "Up", "Down", "Return", "Tab", "Escape"):
            return

        text = normalize_spaces(self.get())
        if not text:
            self.config(values=self._all_values)
            return

        filtered = [v for v in self._all_values if text.lower() in v.lower()]
        self.config(values=filtered)

    def _clear_all(self, event=None):
        self.delete(0, tk.END)
        self.config(values=self._all_values)
        return "break"

    def _on_focus_out(self, event):
        current = normalize_spaces(self.get())
        if current and current not in self._all_values:
            self.set("")


# ============================================================
# Время для выделенных сотрудников
# ============================================================


class TimeForSelectedDialog(simpledialog.Dialog):
    """
    Диалог для массовой установки часов/кода выбранным сотрудникам.
    Поддерживает:
      - один день
      - диапазон дней
      - часы
      - буквенные коды
      - пустое значение = очистка
    """

    CODE_HINTS = {
        code: str(info.get("description") or code)
        for code, info in SPECIAL_CODES.items()
    }

    def __init__(self, parent, max_day: int):
        self.max_day = int(max_day)
        self.result: Optional[Dict[str, Any]] = None
        super().__init__(parent, title="Время для выделенных сотрудников")

    def body(self, master):
        tk.Label(master, text=f"В текущем месяце дней: {self.max_day}").grid(
            row=0, column=0, columnspan=4, sticky="w", pady=(4, 4)
        )

        self.var_mode = tk.StringVar(value="single")
        ttk.Radiobutton(master, text="Один день", value="single", variable=self.var_mode).grid(
            row=1, column=0, sticky="w", pady=(2, 2), columnspan=2
        )
        ttk.Radiobutton(master, text="Диапазон дней", value="range", variable=self.var_mode).grid(
            row=1, column=2, sticky="w", pady=(2, 2), columnspan=2
        )

        tk.Label(master, text="День:").grid(row=2, column=0, sticky="e")
        self.spn_day = tk.Spinbox(master, from_=1, to=max(31, self.max_day), width=4)
        self.spn_day.grid(row=2, column=1, sticky="w")
        self.spn_day.delete(0, "end")
        self.spn_day.insert(0, "1")

        tk.Label(master, text="С:").grid(row=3, column=0, sticky="e")
        self.spn_from = tk.Spinbox(master, from_=1, to=max(31, self.max_day), width=4)
        self.spn_from.grid(row=3, column=1, sticky="w")
        self.spn_from.delete(0, "end")
        self.spn_from.insert(0, "1")

        tk.Label(master, text="по:").grid(row=3, column=2, sticky="e")
        self.spn_to = tk.Spinbox(master, from_=1, to=max(31, self.max_day), width=4)
        self.spn_to.grid(row=3, column=3, sticky="w")
        self.spn_to.delete(0, "end")
        self.spn_to.insert(0, str(self.max_day))

        tk.Label(master, text="Значение:").grid(row=4, column=0, sticky="e", pady=(6, 0))
        self.ent_value = ttk.Entry(master, width=20)
        self.ent_value.grid(row=4, column=1, sticky="w", pady=(6, 0))
        self.ent_value.insert(0, "8,25")

        tk.Label(master, text="или код:").grid(row=4, column=2, sticky="e", pady=(6, 0))
        self.var_code = tk.StringVar(value="(не выбран)")
        code_values = ["(не выбран)"] + sorted(self.CODE_HINTS.keys())
        self.cmb_code = ttk.Combobox(
            master,
            state="readonly",
            width=18,
            textvariable=self.var_code,
            values=code_values,
        )
        self.cmb_code.grid(row=4, column=3, sticky="w", pady=(6, 0))

        self.cmb_code.bind("<<ComboboxSelected>>", self._on_code_selected)

        tk.Label(
            master,
            text=(
                "Часы: 8 | 8,25 | 8:30 | 1/7 | 8/2(1/1)\n"
                "Коды: НН, НВ, МО, ВМ, ОТ, Б, О, П, В, К, РВ 8, РВ 11\n"
                "Пусто — очистить выбранные дни"
            ),
        ).grid(row=5, column=0, columnspan=4, sticky="w", pady=(6, 0))

        self.lbl_code_help = tk.Label(master, text="", fg="#555")
        self.lbl_code_help.grid(row=6, column=0, columnspan=4, sticky="w", pady=(6, 0))

        self.ent_value.bind("<KeyRelease>", lambda _e: self._update_help())
        self.cmb_code.bind("<<ComboboxSelected>>", lambda _e: self._update_help())
        self._update_help()

        return self.ent_value

    def _on_code_selected(self, _e=None):
        code = normalize_spaces(self.var_code.get())
        if code and code != "(не выбран)":
            self.ent_value.delete(0, "end")
            self.ent_value.insert(0, code)

    def _update_help(self):
        v = normalize_code(self.ent_value.get())
        hint = self.CODE_HINTS.get(v, "")
        self.lbl_code_help.config(text=(f"Код: {hint}" if hint else ""))

    def validate(self):
        mode = self.var_mode.get()

        try:
            d_single = int(self.spn_day.get())
            d_from = int(self.spn_from.get())
            d_to = int(self.spn_to.get())
        except Exception:
            messagebox.showwarning("Время для выделенных", "Дни должны быть целыми числами.", parent=self)
            return False

        if not (1 <= d_single <= self.max_day):
            messagebox.showwarning(
                "Время для выделенных",
                f"Один день должен быть от 1 до {self.max_day}.",
                parent=self,
            )
            return False

        if not (1 <= d_from <= self.max_day) or not (1 <= d_to <= self.max_day):
            messagebox.showwarning(
                "Время для выделенных",
                f"Диапазон дней должен быть в пределах 1–{self.max_day}.",
                parent=self,
            )
            return False

        if mode == "range" and d_from > d_to:
            messagebox.showwarning(
                "Время для выделенных",
                "Начальный день диапазона не может быть больше конечного.",
                parent=self,
            )
            return False

        self._mode = mode
        if mode == "single":
            self._from = self._to = d_single
        else:
            self._from, self._to = d_from, d_to

        val = normalize_spaces(self.ent_value.get())
        if not val:
            self._value = None
            return True

        if is_allowed_timesheet_code(val):
            self._value = normalize_code(val)
            return True

        hv = parse_hours_value(val)
        if hv is None or hv < 0:
            messagebox.showwarning(
                "Время для выделенных",
                "Введите корректное значение часов или код.\n"
                "Примеры часов: 8, 8:30, 1/7, 8/2(1/1)\n"
                "Коды: НН, НВ, МО, ВМ, ОТ, Б, О, П, В, К, РВ 8, РВ 11",
                parent=self,
            )
            return False

        self._value = val
        return True

    def apply(self):
        self.result = {
            "from": self._from,
            "to": self._to,
            "value": self._value,
        }


__all__ = [
    "center_toplevel",
    "setup_modal_window",
    "SuspiciousHoursWarningDialog",
    "SelectDateDialog",
    "CopyFromDialog",
    "SelectObjectIdDialog",
    "SkudMappingReviewDialog",
    "SelectEmployeesDialog",
    "BatchAddDialog",
    "HoursFillDialog",
    "AutoCompleteCombobox",
    "TimeForSelectedDialog",
]
