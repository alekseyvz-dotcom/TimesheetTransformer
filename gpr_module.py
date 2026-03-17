from __future__ import annotations

import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Sequence

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

from gpr_common import (
    C,
    STATUS_COLORS,
    STATUS_LABELS,
    STATUS_LIST,
    GprPlanInfo,
    GprTask,
    build_object_label_map,
    build_summary_text,
    calc_fact_percent,
    clone_task_as_new,
    filter_tasks,
    find_task_index_by_iid,
    fit_range_for_tasks,
    fmt_date,
    fmt_percent,
    fmt_qty,
    normalize_spaces,
    safe_filename,
    task_from_dialog_result,
    task_to_tree_iid,
    today,
    validate_tasks,
)
from gpr_db import GprService, set_db_pool
from gpr_dialogs import AutoCompleteCombobox, DateRangeDialog, TemplateSelectDialog
from gpr_gantt import GanttCanvas
from gpr_task_dialog import open_task_dialog

logger = logging.getLogger(__name__)


class GprPage(tk.Frame):
    def __init__(self, master, app_ref):
        super().__init__(master, bg=C["bg"])
        self.app_ref = app_ref

        self.objects: List[Dict[str, Any]] = []
        self.work_types: List[Dict[str, Any]] = []
        self.uoms: List[Dict[str, Any]] = []
        self.statuses_db: List[Dict[str, Any]] = []

        self.object_label_to_id: Dict[str, int] = {}
        self.object_db_id: Optional[int] = None
        self.plan_info: Optional[GprPlanInfo] = None
        self.plan_id: Optional[int] = None

        self.tasks: List[GprTask] = []
        self.tasks_filtered: List[GprTask] = []
        self.facts: Dict[int, float] = {}

        self.range_from, self.range_to = self._default_range()

        self._dirty = False
        self._loaded_plan_updated_at = None
        self._filter_job = None

        self._build_ui()
        self._load_refs()
        self._update_range_label()
        self._update_summary()
        self._update_bottom_info()

    # ============================================================
    # BASIC HELPERS
    # ============================================================

    def _default_range(self):
        from gpr_common import quarter_range
        return quarter_range()

    def _safe_current_user_id(self) -> Optional[int]:
        try:
            user = getattr(self.app_ref, "current_user", None) or {}
            uid = user.get("id")
            return int(uid) if uid is not None else None
        except Exception:
            return None

    def _set_state_text(self, text: str, fg: str = "#bbdefb"):
        try:
            self.lbl_state.config(text=text, fg=fg)
        except Exception:
            pass

    def _mark_dirty(self):
        self._dirty = True
        self._set_state_text("Есть несохранённые изменения", fg="#ffe082")

    def _mark_saved(self):
        self._dirty = False
        now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        self._set_state_text(f"Сохранено: {now}", fg="#bbdefb")

    def _mark_error(self, text: str):
        self._set_state_text(text, fg="#ffccbc")

    def _confirm_leave_with_unsaved(self) -> bool:
        if not self._dirty:
            return True

        answer = messagebox.askyesnocancel(
            "Несохранённые изменения",
            "Есть несохранённые изменения.\n\nСохранить перед продолжением?",
            parent=self,
        )
        if answer is None:
            return False
        if answer is True:
            return self._save()
        return True

    def _selected_object_id(self) -> Optional[int]:
        raw = normalize_spaces(self.cmb_obj.get() or "")
        if not raw:
            return None

        # точное совпадение
        if raw in self.object_label_to_id:
            return int(self.object_label_to_id[raw])

        # нормализованное совпадение
        raw_norm = normalize_spaces(raw).lower()
        for label, object_id in self.object_label_to_id.items():
            if normalize_spaces(label).lower() == raw_norm:
                return int(object_id)

        return None

    def _selected_task(self) -> Optional[GprTask]:
        sel = self.tree.selection()
        if not sel:
            return None
        iid = sel[0]
        idx = find_task_index_by_iid(self.tasks, iid)
        if idx is None:
            return None
        if 0 <= idx < len(self.tasks):
            return self.tasks[idx]
        return None

    def _selected_task_index(self) -> Optional[int]:
        sel = self.tree.selection()
        if not sel:
            return None
        return find_task_index_by_iid(self.tasks, sel[0])

    def _work_type_name_by_id(self, work_type_id: int) -> str:
        for wt in self.work_types:
            try:
                if int(wt["id"]) == int(work_type_id):
                    return normalize_spaces(wt.get("name") or "")
            except Exception:
                continue
        return ""

    def _status_code_from_filter(self) -> Optional[str]:
        text = normalize_spaces(self.cmb_filt_st.get() or "")
        if not text or text == "Все":
            return None

        for code in STATUS_LIST:
            if STATUS_LABELS.get(code, code) == text:
                return code
        return None

    def _work_type_name_from_filter(self) -> Optional[str]:
        text = normalize_spaces(self.cmb_filt_wt.get() or "")
        return None if not text or text == "Все" else text

    def _renumber_sort_orders(self):
        for i, task in enumerate(self.tasks):
            task.sort_order = i * 10

    # ============================================================
    # UI
    # ============================================================

    def _build_ui(self):
        # Header
        hdr = tk.Frame(self, bg=C["accent"], pady=6)
        hdr.pack(fill="x")

        tk.Label(
            hdr,
            text="📊 ГПР — График производства работ",
            font=("Segoe UI", 12, "bold"),
            bg=C["accent"],
            fg="white",
            padx=12,
        ).pack(side="left")

        self.lbl_state = tk.Label(
            hdr,
            text="Не сохранено",
            font=("Segoe UI", 8),
            bg=C["accent"],
            fg="#bbdefb",
            padx=10,
        )
        self.lbl_state.pack(side="right")

        self.lbl_plan_info = tk.Label(
            hdr,
            text="",
            font=("Segoe UI", 8),
            bg=C["accent"],
            fg="#bbdefb",
            padx=12,
        )
        self.lbl_plan_info.pack(side="right")

        # Top panel
        top = tk.LabelFrame(
            self,
            text=" 📍 Объект и диапазон ",
            font=("Segoe UI", 9, "bold"),
            bg=C["panel"],
            fg=C["accent"],
            relief="groove",
            bd=1,
            padx=10,
            pady=8,
        )
        top.pack(fill="x", padx=10, pady=(8, 4))
        top.grid_columnconfigure(1, weight=1)

        tk.Label(top, text="Объект:", bg=C["panel"], font=("Segoe UI", 9)).grid(
            row=0, column=0, sticky="e", padx=(0, 6)
        )
        self.cmb_obj = AutoFilterCombobox(top, width=60, font=("Segoe UI", 9))
        self.cmb_obj.grid(row=0, column=1, sticky="ew", pady=3)
        self.cmb_obj.bind("<Return>", lambda _e: self._open_object())

        btn_f = tk.Frame(top, bg=C["panel"])
        btn_f.grid(row=0, column=2, padx=(8, 0))
        self._accent_btn(btn_f, "▶ Открыть", self._open_object).pack(side="left")

        tk.Label(top, text="Диапазон:", bg=C["panel"], font=("Segoe UI", 9)).grid(
            row=1, column=0, sticky="e", padx=(0, 6)
        )

        range_f = tk.Frame(top, bg=C["panel"])
        range_f.grid(row=1, column=1, sticky="w", pady=3)

        self.lbl_range = tk.Label(range_f, text="", bg=C["panel"], fg=C["text2"], font=("Segoe UI", 9))
        self.lbl_range.pack(side="left")

        ttk.Button(range_f, text="Изменить…", command=self._change_range).pack(side="left", padx=(12, 0))
        ttk.Button(range_f, text="По работам", command=self._fit_range).pack(side="left", padx=(6, 0))

        # Toolbar
        bar = tk.Frame(self, bg=C["accent_light"], pady=5)
        bar.pack(fill="x", padx=10)

        self._tb_btn(bar, "➕ Добавить", self._add_task)
        self._tb_btn(bar, "✏️ Редактировать", self._edit_selected)
        self._tb_btn(bar, "📄 Дублировать", self._duplicate_selected)
        self._tb_btn(bar, "🗑 Удалить", self._delete_selected)

        tk.Frame(bar, bg=C["border"], width=1).pack(side="left", fill="y", padx=8)

        self._tb_btn(bar, "📋 Из шаблона…", self._apply_template)
        self._tb_btn(bar, "🔄 Обновить", self._reload_plan)
        self._tb_btn(bar, "📥 Экспорт Excel", self._export_excel)

        tk.Frame(bar, bg=C["border"], width=1).pack(side="left", fill="y", padx=8)

        self._tb_btn(bar, "🔍−", lambda: self._zoom(-2))
        self._tb_btn(bar, "🔍+", lambda: self._zoom(2))

        self._accent_btn(bar, "💾 СОХРАНИТЬ", self._save).pack(side="right", padx=(4, 8))

        # Filter bar
        fbar = tk.Frame(self, bg=C["bg"], pady=4)
        fbar.pack(fill="x", padx=10)

        tk.Label(fbar, text="Фильтр тип:", bg=C["bg"], font=("Segoe UI", 8)).pack(side="left")
        self.cmb_filt_wt = ttk.Combobox(fbar, state="readonly", width=24, values=["Все"])
        self.cmb_filt_wt.pack(side="left", padx=(4, 12))
        self.cmb_filt_wt.current(0)
        self.cmb_filt_wt.bind("<<ComboboxSelected>>", lambda _e: self._apply_filter())

        tk.Label(fbar, text="Статус:", bg=C["bg"], font=("Segoe UI", 8)).pack(side="left")
        self.cmb_filt_st = ttk.Combobox(
            fbar,
            state="readonly",
            width=18,
            values=["Все"] + [STATUS_LABELS[s] for s in STATUS_LIST],
        )
        self.cmb_filt_st.pack(side="left", padx=(4, 12))
        self.cmb_filt_st.current(0)
        self.cmb_filt_st.bind("<<ComboboxSelected>>", lambda _e: self._apply_filter())

        tk.Label(fbar, text="Поиск:", bg=C["bg"], font=("Segoe UI", 8)).pack(side="left")
        self.var_search = tk.StringVar()
        ent_s = ttk.Entry(fbar, textvariable=self.var_search, width=28)
        ent_s.pack(side="left", padx=(4, 0))
        ent_s.bind("<KeyRelease>", self._schedule_filter)

        # Summary
        self.lbl_summary = tk.Label(
            self,
            text="",
            bg=C["bg"],
            font=("Segoe UI", 8),
            fg=C["text2"],
            anchor="w",
        )
        self.lbl_summary.pack(fill="x", padx=14, pady=(2, 0))

        # Legend
        leg = tk.Frame(self, bg=C["bg"])
        leg.pack(fill="x", padx=14, pady=(0, 2))
        for code in STATUS_LIST:
            col, _fg, label = STATUS_COLORS[code]
            fr = tk.Frame(leg, bg=C["bg"])
            fr.pack(side="left", padx=(0, 12))
            tk.Canvas(fr, width=12, height=12, bg=col, highlightthickness=1, highlightbackground="#999").pack(
                side="left", padx=(0, 3)
            )
            tk.Label(fr, text=label, bg=C["bg"], font=("Segoe UI", 7), fg=C["text2"]).pack(side="left")

        # Split: tree + gantt
        pw = tk.PanedWindow(self, orient="horizontal", sashrelief="raised", bg=C["bg"])
        pw.pack(fill="both", expand=True, padx=10, pady=(4, 4))

        left = tk.Frame(pw, bg=C["panel"])
        right = tk.Frame(pw, bg=C["panel"])
        pw.add(left, minsize=500)
        pw.add(right, minsize=420)

        cols = ("type", "name", "start", "finish", "uom", "qty", "status")
        self.tree = ttk.Treeview(left, columns=cols, show="headings", selectmode="browse")

        heads = {
            "type": ("Тип работ", 130),
            "name": ("Вид работ", 240),
            "start": ("Начало", 90),
            "finish": ("Конец", 90),
            "uom": ("Ед.", 55),
            "qty": ("Объём", 85),
            "status": ("Статус", 110),
        }
        for c, (text, width) in heads.items():
            self.tree.heading(c, text=text)
            anchor = "center" if c in ("start", "finish", "uom", "status") else ("e" if c == "qty" else "w")
            self.tree.column(c, width=width, anchor=anchor)

        # Tree tags by status
        for code in STATUS_LIST:
            fill = STATUS_COLORS[code][0]
            self.tree.tag_configure(code, background=fill)

        self.vsb = ttk.Scrollbar(left, orient="vertical", command=self._on_tree_scrollbar)
        self.tree.configure(yscrollcommand=self._on_tree_yscroll)

        self.tree.pack(side="left", fill="both", expand=True)
        self.vsb.pack(side="right", fill="y")

        self.tree.bind("<Double-1>", lambda _e: self._edit_selected())
        self.tree.bind("<Return>", lambda _e: self._edit_selected())
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self.gantt.schedule_redraw())
        self.tree.bind("<MouseWheel>", self._on_tree_wheel)
        self.tree.bind("<Button-4>", self._on_tree_wheel_linux_up)
        self.tree.bind("<Button-5>", self._on_tree_wheel_linux_down)

        self.gantt = GanttCanvas(right, day_px=20, linked_tree=self.tree)
        self.gantt.pack(fill="both", expand=True)

        # Bottom
        bottom = tk.Frame(self, bg=C["accent_light"], pady=5)
        bottom.pack(fill="x", padx=10, pady=(0, 10))

        self.lbl_bottom = tk.Label(
            bottom,
            text="Объект не открыт",
            font=("Segoe UI", 9, "bold"),
            fg=C["accent"],
            bg=C["accent_light"],
        )
        self.lbl_bottom.pack(side="left", padx=10)

    def _accent_btn(self, parent, text, cmd):
        b = tk.Button(
            parent,
            text=text,
            font=("Segoe UI", 9, "bold"),
            bg=C["btn_bg"],
            fg=C["btn_fg"],
            activebackground="#0d47a1",
            activeforeground="white",
            relief="flat",
            cursor="hand2",
            padx=10,
            pady=3,
            command=cmd,
        )
        b.bind("<Enter>", lambda _e: b.config(bg="#0d47a1"))
        b.bind("<Leave>", lambda _e: b.config(bg=C["btn_bg"]))
        return b

    def _tb_btn(self, parent, text, cmd):
        ttk.Button(parent, text=text, command=cmd).pack(side="left", padx=2)

    # ============================================================
    # Tree / gantt sync
    # ============================================================

    def _on_tree_yscroll(self, first, last):
        self.vsb.set(first, last)
        self.gantt.schedule_redraw()

    def _on_tree_scrollbar(self, *args):
        self.tree.yview(*args)
        self.gantt.schedule_redraw()

    def _on_tree_wheel(self, event):
        delta = -1 * (event.delta // 120) if event.delta else 0
        if delta:
            self.tree.yview_scroll(delta, "units")
            self.gantt.schedule_redraw()
        return "break"

    def _on_tree_wheel_linux_up(self, _event):
        self.tree.yview_scroll(-1, "units")
        self.gantt.schedule_redraw()
        return "break"

    def _on_tree_wheel_linux_down(self, _event):
        self.tree.yview_scroll(1, "units")
        self.gantt.schedule_redraw()
        return "break"

    # ============================================================
    # LOAD REFS
    # ============================================================

    def _load_refs(self):
        try:
            self.objects = GprService.load_objects_short()
            self.work_types = GprService.load_work_types()
            self.uoms = GprService.load_uoms()

            try:
                self.statuses_db = GprService.load_statuses()
            except Exception:
                logger.exception("Не удалось загрузить статусы GPR из БД")
                self.statuses_db = []
        except Exception as e:
            logger.exception("Ошибка загрузки справочников ГПР")
            messagebox.showerror("ГПР", f"Ошибка загрузки справочников:\n{e}", parent=self)
            return

        labels, self.object_label_to_id = build_object_label_map(self.objects)
        self.cmb_obj.set_values(labels)

        wt_names = ["Все"] + [normalize_spaces(w.get("name") or "") for w in self.work_types]
        self.cmb_filt_wt.configure(values=wt_names)
        self.cmb_filt_wt.current(0)

    # ============================================================
    # PLAN / OBJECT LOAD
    # ============================================================

    def _reload_plan(self):
        if not self.plan_id or not self.object_db_id:
            messagebox.showinfo("ГПР", "Сначала откройте объект.", parent=self)
            return

        if not self._confirm_leave_with_unsaved():
            return

        self._load_plan_for_current_object()

    def _open_object(self):
        oid = self._selected_object_id()
        if not oid:
            messagebox.showwarning(
                "ГПР",
                "Выберите объект из списка.\n"
                "Если объект введён вручную, выберите его из выпадающего списка точно.",
                parent=self,
            )
            return

        if self.object_db_id != oid or self._dirty:
            if not self._confirm_leave_with_unsaved():
                return

        self.object_db_id = oid
        self._load_plan_for_current_object()

    def _load_plan_for_current_object(self):
        if not self.object_db_id:
            return

        uid = self._safe_current_user_id()

        try:
            self.plan_info = GprService.get_or_create_current_plan(self.object_db_id, uid)
            self.plan_id = int(self.plan_info.id)

            self.tasks = GprService.load_plan_tasks(self.plan_id)
            task_ids = [t.id for t in self.tasks if t.id is not None]
            self.facts = GprService.load_task_facts_cumulative(task_ids)

            self._loaded_plan_updated_at = self.plan_info.updated_at
            self._dirty = False

            # авто-диапазон
            fitted = fit_range_for_tasks(self.tasks, padding_days=7)
            if fitted:
                self.range_from, self.range_to = fitted
            else:
                self.range_from, self.range_to = self._default_range()

            self._update_range_label()
            self._update_plan_info()
            self._apply_filter()
            self._update_summary()
            self._update_bottom_info()
            self._mark_saved()

        except Exception as e:
            logger.exception("Ошибка открытия ГПР объекта")
            messagebox.showerror("ГПР", f"Не удалось открыть ГПР:\n{e}", parent=self)

    # ============================================================
    # UI updates
    # ============================================================

    def _update_range_label(self):
        self.lbl_range.config(text=f"{fmt_date(self.range_from)} — {fmt_date(self.range_to)}")
        self.gantt.set_range(self.range_from, self.range_to)

    def _update_plan_info(self):
        if not self.plan_info:
            self.lbl_plan_info.config(text="")
            return

        creator = normalize_spaces(self.plan_info.creator_name or "—")
        upd = self.plan_info.updated_at
        upd_text = upd.strftime("%d.%m.%Y %H:%M") if isinstance(upd, datetime) else str(upd or "")
        version = getattr(self.plan_info, "version_no", 1)

        self.lbl_plan_info.config(
            text=f"Версия: {version}  |  Создал: {creator}  |  Обновлён: {upd_text}"
        )

    def _update_summary(self):
        base = build_summary_text(self.tasks, facts=self.facts)
        if self.tasks_filtered and len(self.tasks_filtered) != len(self.tasks):
            base += f"  |  Показано: {len(self.tasks_filtered)} из {len(self.tasks)}"
        self.lbl_summary.config(text=base or "Нет данных")

    def _update_bottom_info(self):
        if not self.object_db_id:
            self.lbl_bottom.config(text="Объект не открыт")
            return

        obj = next((o for o in self.objects if int(o["id"]) == int(self.object_db_id)), None)
        addr = normalize_spaces(obj.get("address") or "") if obj else ""
        short_name = normalize_spaces(obj.get("short_name") or "") if obj else ""
        title = short_name or addr or f"ID {self.object_db_id}"

        self.lbl_bottom.config(text=f"Объект: {title}  |  Работ: {len(self.tasks)}")

    # ============================================================
    # Filters / render
    # ============================================================

    def _schedule_filter(self, _event=None):
        if self._filter_job is not None:
            try:
                self.after_cancel(self._filter_job)
            except Exception:
                pass
        self._filter_job = self.after(150, self._apply_filter)

    def _apply_filter(self):
        self._filter_job = None

        wt_name = self._work_type_name_from_filter()
        status_code = self._status_code_from_filter()
        query = normalize_spaces(self.var_search.get() or "")

        self.tasks_filtered = filter_tasks(
            self.tasks,
            work_type_name=wt_name,
            status_code=status_code,
            search_text=query,
        )
        self._render()
        self._update_summary()

    def _render(self):
        self.tree.delete(*self.tree.get_children())

        for task in self.tasks_filtered:
            iid = task_to_tree_iid(task)
            status_code = normalize_spaces(task.status or "planned")

            values = (
                task.work_type_name,
                task.name,
                fmt_date(task.plan_start),
                fmt_date(task.plan_finish),
                task.uom_code or "",
                fmt_qty(task.plan_qty),
                STATUS_LABELS.get(status_code, status_code),
            )

            tags = (status_code,) if status_code in STATUS_LIST else ()
            self.tree.insert("", "end", iid=iid, values=values, tags=tags)

        self.gantt.set_data(self.tasks_filtered, self.facts)

    # ============================================================
    # Range / zoom
    # ============================================================

    def _change_range(self):
        dlg = DateRangeDialog(self, self.range_from, self.range_to)
        if dlg.result:
            self.range_from, self.range_to = dlg.result
            self._update_range_label()
            self.gantt.set_data(self.tasks_filtered, self.facts)

    def _fit_range(self):
        if not self.tasks:
            return

        fitted = fit_range_for_tasks(self.tasks, padding_days=7)
        if not fitted:
            messagebox.showinfo("ГПР", "Нет задач с корректными датами для подбора диапазона.", parent=self)
            return

        self.range_from, self.range_to = fitted
        self._update_range_label()
        self.gantt.set_data(self.tasks_filtered, self.facts)

    def _zoom(self, delta: int):
        self.gantt.zoom(delta)

    # ============================================================
    # CRUD
    # ============================================================

    def _ensure_plan_open(self) -> bool:
        if not self.plan_id:
            messagebox.showinfo("ГПР", "Сначала откройте объект.", parent=self)
            return False
        return True

    def _add_task(self):
        if not self._ensure_plan_open():
            return

        uid = self._safe_current_user_id()
        result = open_task_dialog(
            self,
            self.work_types,
            self.uoms,
            init={
                "plan_start": self.range_from,
                "plan_finish": self.range_from,
            },
            user_id=uid,
        )
        if not result:
            return

        work_type_name = result.get("work_type_name") or self._work_type_name_by_id(int(result["work_type_id"]))
        task = task_from_dialog_result(
            result,
            existing_id=None,
            existing_client_id=None,
            sort_order=len(self.tasks) * 10,
            work_type_name=work_type_name,
        )
        task.plan_id = self.plan_id

        self.tasks.append(task)
        self._renumber_sort_orders()
        self._apply_filter()
        self._update_bottom_info()
        self._mark_dirty()

    def _edit_selected(self):
        task = self._selected_task()
        if task is None:
            return

        uid = self._safe_current_user_id()
        result = open_task_dialog(
            self,
            self.work_types,
            self.uoms,
            init=task,
            user_id=uid,
        )
        if not result:
            return

        idx = self._selected_task_index()
        if idx is None:
            return

        work_type_name = result.get("work_type_name") or self._work_type_name_by_id(int(result["work_type_id"]))
        updated = task_from_dialog_result(
            result,
            existing_id=task.id,
            existing_client_id=task.client_id,
            sort_order=task.sort_order,
            work_type_name=work_type_name,
        )
        updated.plan_id = self.plan_id

        self.tasks[idx] = updated
        self._apply_filter()
        self._mark_dirty()

    def _duplicate_selected(self):
        task = self._selected_task()
        if task is None:
            return

        clone = clone_task_as_new(task)
        clone.plan_id = self.plan_id
        clone.sort_order = len(self.tasks) * 10
        self.tasks.append(clone)

        self._renumber_sort_orders()
        self._apply_filter()
        self._update_bottom_info()
        self._mark_dirty()

    def _delete_selected(self):
        task = self._selected_task()
        idx = self._selected_task_index()
        if task is None or idx is None:
            return

        if task.id is not None:
            fact_qty = float(self.facts.get(int(task.id), 0.0))
            if fact_qty > 0:
                messagebox.showwarning(
                    "Удаление задачи",
                    "По этой задаче уже есть фактические объёмы.\n"
                    "Удаление запрещено, пока факты не будут убраны.",
                    parent=self,
                )
                return

        if not messagebox.askyesno(
            "ГПР",
            f"Удалить работу?\n\n{task.name}",
            parent=self,
        ):
            return

        self.tasks.pop(idx)
        self._renumber_sort_orders()
        self._apply_filter()
        self._update_bottom_info()
        self._mark_dirty()

    def _apply_template(self):
        if not self._ensure_plan_open():
            return

        try:
            templates = GprService.load_templates()
        except Exception as e:
            logger.exception("Ошибка загрузки шаблонов")
            messagebox.showerror("ГПР", f"Ошибка загрузки шаблонов:\n{e}", parent=self)
            return

        if not templates:
            messagebox.showinfo("ГПР", "Шаблонов нет.", parent=self)
            return

        dlg = TemplateSelectDialog(self, templates)
        if dlg.result is None:
            return

        try:
            template_rows = GprService.load_template_tasks(dlg.result)
        except Exception as e:
            logger.exception("Ошибка загрузки задач шаблона")
            messagebox.showerror("ГПР", f"Ошибка загрузки задач шаблона:\n{e}", parent=self)
            return

        if not template_rows:
            messagebox.showinfo("ГПР", "В шаблоне нет задач.", parent=self)
            return

        if self.tasks:
            if not messagebox.askyesno(
                "ГПР",
                "Заменить текущие работы задачами из шаблона?",
                parent=self,
            ):
                return

        out: List[GprTask] = []
        base = self.range_from

        for i, row in enumerate(template_rows):
            wid = int(row["work_type_id"])
            wt_name = self._work_type_name_by_id(wid)

            # ВАЖНО:
            # parent_id из шаблона НЕ переносим напрямую,
            # потому что это id шаблонной задачи, а не реальной gpr_tasks.
            task = GprTask(
                id=None,
                work_type_id=wid,
                work_type_name=wt_name,
                name=normalize_spaces(row.get("name") or ""),
                uom_code=normalize_spaces(row.get("uom_code") or "") or None,
                plan_qty=safe_float(row.get("default_qty")),
                plan_start=base,
                plan_finish=base,
                status="planned",
                sort_order=int(row.get("sort_order") or (i * 10)),
                is_milestone=bool(row.get("is_milestone")),
                parent_id=None,
            ).normalized_copy()
            task.plan_id = self.plan_id
            out.append(task)

        self.tasks = out
        self._renumber_sort_orders()
        self._apply_filter()
        self._update_bottom_info()
        self._mark_dirty()

    # ============================================================
    # SAVE
    # ============================================================

    def _save(self) -> bool:
        if not self._ensure_plan_open():
            return False

        errors = validate_tasks(self.tasks)
        if errors:
            preview = "\n".join(f"• {e}" for e in errors[:20])
            if len(errors) > 20:
                preview += f"\n• ... и ещё {len(errors) - 20}"
            messagebox.showwarning("Сохранение ГПР", f"Исправьте ошибки:\n\n{preview}", parent=self)
            return False

        uid = self._safe_current_user_id()

        try:
            result = GprService.save_plan_tasks_diff(
                self.plan_id,
                uid,
                self.tasks,
                expected_plan_updated_at=self._loaded_plan_updated_at,
            )

            # перечитываем актуальное состояние
            self.tasks = GprService.load_plan_tasks(self.plan_id)
            task_ids = [t.id for t in self.tasks if t.id is not None]
            self.facts = GprService.load_task_facts_cumulative(task_ids)

            self.plan_info = GprService.get_plan_by_id(self.plan_id)
            self._loaded_plan_updated_at = self.plan_info.updated_at if self.plan_info else None

            self._apply_filter()
            self._update_plan_info()
            self._update_summary()
            self._update_bottom_info()
            self._mark_saved()

            messagebox.showinfo(
                "ГПР",
                "Сохранено.\n\n"
                f"Новых задач: {result.get('inserted', 0)}\n"
                f"Обновлено: {result.get('updated', 0)}\n"
                f"Удалено: {result.get('deleted', 0)}",
                parent=self,
            )
            return True

        except Exception as e:
            logger.exception("Ошибка сохранения ГПР")
            self._mark_error("Ошибка сохранения")
            messagebox.showerror("ГПР", f"Ошибка сохранения:\n{e}", parent=self)
            return False

    # ============================================================
    # EXPORT EXCEL
    # ============================================================

    def _export_excel(self):
        if not self.tasks:
            messagebox.showinfo("ГПР", "Нет данных для выгрузки.", parent=self)
            return

        if not HAS_OPENPYXL:
            messagebox.showwarning(
                "ГПР",
                "Для экспорта необходима библиотека openpyxl.",
                parent=self,
            )
            return

        export_tasks: Sequence[GprTask] = self.tasks

        is_filtered = len(self.tasks_filtered) != len(self.tasks)
        if is_filtered:
            answer = messagebox.askyesnocancel(
                "Экспорт Excel",
                "Сейчас включены фильтры.\n\n"
                "Да — выгрузить только отфильтрованные задачи\n"
                "Нет — выгрузить все задачи\n"
                "Отмена — не выгружать",
                parent=self,
            )
            if answer is None:
                return
            if answer is True:
                export_tasks = self.tasks_filtered
            else:
                export_tasks = self.tasks

        obj = next((o for o in self.objects if int(o["id"]) == int(self.object_db_id)), None) if self.object_db_id else None
        obj_name = normalize_spaces((obj.get("short_name") or obj.get("address")) if obj else "объект")
        default_name = safe_filename(f"ГПР_{obj_name}_{today().strftime('%Y%m%d')}.xlsx")

        path = filedialog.asksaveasfilename(
            parent=self,
            title="Сохранить ГПР в Excel",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel", "*.xlsx"), ("Все файлы", "*.*")],
        )
        if not path:
            return

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "ГПР"

            object_title = obj_name or "—"
            range_text = f"{fmt_date(self.range_from)} — {fmt_date(self.range_to)}"

            ws["A1"] = "Объект:"
            ws["B1"] = object_title
            ws["A2"] = "Период отображения:"
            ws["B2"] = range_text
            ws["A3"] = "Сформировано:"
            ws["B3"] = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

            for cell in ("A1", "A2", "A3"):
                ws[cell].font = Font(bold=True)

            header_row = 5
            headers = [
                "№",
                "Тип работ",
                "Вид работ",
                "Ед. изм.",
                "Объём план",
                "Начало",
                "Окончание",
                "Длительность (дн.)",
                "Статус",
                "Факт (накоп.)",
                "% выполнения",
                "Назначено работников",
            ]
            widths = [6, 22, 36, 10, 14, 14, 14, 16, 18, 14, 14, 22]

            for col_idx, title in enumerate(headers, start=1):
                cell = ws.cell(header_row, col_idx, title)
                cell.font = Font(bold=True, size=10)
                cell.fill = PatternFill("solid", fgColor="D6DCE4")
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

            for i, width in enumerate(widths, start=1):
                ws.column_dimensions[get_column_letter(i)].width = width

            ws.freeze_panes = f"A{header_row + 1}"

            status_fill = {
                "planned": PatternFill("solid", fgColor="D6EAFF"),
                "in_progress": PatternFill("solid", fgColor="FFF3CD"),
                "done": PatternFill("solid", fgColor="D4EDDA"),
                "paused": PatternFill("solid", fgColor="FFF9C4"),
                "canceled": PatternFill("solid", fgColor="F8D7DA"),
            }

            row_num = header_row + 1
            for n, task in enumerate(export_tasks, start=1):
                ds = task.plan_start
                df = task.plan_finish
                duration = task.duration_days() if task.duration_days() is not None else ""

                fact_qty = float(self.facts.get(int(task.id), 0.0)) if task.id is not None else 0.0
                pct = calc_fact_percent(task.plan_qty, fact_qty)

                ws.cell(row_num, 1, n)
                ws.cell(row_num, 2, task.work_type_name)
                ws.cell(row_num, 3, task.name)
                ws.cell(row_num, 4, task.uom_code or "")
                ws.cell(row_num, 5, fmt_qty(task.plan_qty) if task.plan_qty is not None else "")
                ws.cell(row_num, 6, fmt_date(ds))
                ws.cell(row_num, 7, fmt_date(df))
                ws.cell(row_num, 8, duration)
                ws.cell(row_num, 9, STATUS_LABELS.get(task.status, task.status))
                ws.cell(row_num, 10, fmt_qty(fact_qty))
                ws.cell(row_num, 11, fmt_percent(pct))
                ws.cell(row_num, 12, len(task.assignments or []))

                for c in range(1, 13):
                    ws.cell(row_num, c).alignment = Alignment(horizontal="center", vertical="center")

                ws.cell(row_num, 2).alignment = Alignment(horizontal="left", vertical="center")
                ws.cell(row_num, 3).alignment = Alignment(horizontal="left", vertical="center")

                fill = status_fill.get(task.status)
                if fill:
                    ws.cell(row_num, 9).fill = fill

                row_num += 1

            # итоговая строка
            ws.cell(row_num + 1, 1, "")
            ws.cell(row_num + 1, 2, f"Итого задач: {len(export_tasks)}").font = Font(bold=True)

            done_cnt = sum(1 for t in export_tasks if t.status == "done")
            ws.cell(row_num + 1, 9, f"Выполнено: {done_cnt}").font = Font(bold=True)

            wb.save(path)
            messagebox.showinfo("ГПР", f"Файл сохранён:\n{path}", parent=self)

        except Exception as e:
            logger.exception("Ошибка экспорта ГПР в Excel")
            messagebox.showerror("ГПР", f"Ошибка экспорта:\n{e}", parent=self)

    # ============================================================
    # PUBLIC helper (если main_app когда-нибудь захочет спросить)
    # ============================================================

    def has_unsaved_changes(self) -> bool:
        return bool(self._dirty)


# ============================================================
# API FOR main_app
# ============================================================

def create_gpr_page(parent, app_ref) -> GprPage:
    return GprPage(parent, app_ref=app_ref)


__all__ = [
    "set_db_pool",
    "GprPage",
    "create_gpr_page",
]
