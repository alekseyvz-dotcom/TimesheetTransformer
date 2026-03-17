"""
gpr_page.py
Главная страница ГПР
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Dict, List, Optional

import tkinter as tk
from tkinter import ttk, messagebox

from gpr_common import (
    C,
    STATUS_LIST,
    STATUS_LABELS,
    fmt_date,
    fmt_qty,
    quarter_range,
)

from gpr_service import GprService
from gantt_canvas import GanttCanvas
from task_dialog import open_task_dialog


class GprPage(tk.Frame):

    def __init__(self, master, app_ref=None):
        super().__init__(master, bg=C["bg"])

        self.app_ref = app_ref

        self.objects: List[Dict] = []
        self.work_types: List[Dict] = []
        self.uoms: List[Dict] = []

        self.tasks: List[Dict] = []
        self.tasks_filtered: List[Dict] = []

        self.plan_id: Optional[int] = None
        self.object_db_id: Optional[int] = None

        q = quarter_range()

        self.range_from: date = q[0]
        self.range_to: date = q[1]

        self._build_ui()
        self._load_refs()

    # ----------------------------------------------------
    # UI
    # ----------------------------------------------------
    def _build_ui(self):

        hdr = tk.Frame(self, bg=C["accent"], pady=6)
        hdr.pack(fill="x")

        tk.Label(
            hdr,
            text="📊 ГПР — график производства работ",
            font=("Segoe UI", 12, "bold"),
            bg=C["accent"],
            fg="white",
            padx=12,
        ).pack(side="left")

        top = tk.Frame(self, bg=C["panel"])
        top.pack(fill="x", padx=10, pady=6)

        tk.Label(top, text="Объект:", bg=C["panel"]).pack(side="left")

        self.cmb_obj = ttk.Combobox(top, width=60)
        self.cmb_obj.pack(side="left", padx=6)

        ttk.Button(
            top,
            text="Открыть",
            command=self._open_object,
        ).pack(side="left")

        # toolbar
        bar = tk.Frame(self, bg=C["accent_light"])
        bar.pack(fill="x", padx=10)

        ttk.Button(bar, text="Добавить", command=self._add_task).pack(
            side="left", padx=2
        )

        ttk.Button(bar, text="Редактировать", command=self._edit_task).pack(
            side="left", padx=2
        )

        ttk.Button(bar, text="Удалить", command=self._delete_task).pack(
            side="left", padx=2
        )

        ttk.Button(bar, text="Сохранить", command=self._save).pack(
            side="right", padx=6
        )

        # split
        pw = tk.PanedWindow(self, orient="horizontal")
        pw.pack(fill="both", expand=True, padx=10, pady=8)

        left = tk.Frame(pw)
        right = tk.Frame(pw)

        pw.add(left, minsize=450)
        pw.add(right)

        # tree
        cols = ("type", "name", "start", "finish", "uom", "qty", "status")

        self.tree = ttk.Treeview(
            left,
            columns=cols,
            show="headings",
            selectmode="browse",
        )

        headers = {
            "type": ("Тип", 140),
            "name": ("Работа", 240),
            "start": ("Начало", 90),
            "finish": ("Конец", 90),
            "uom": ("Ед.", 50),
            "qty": ("Объем", 80),
            "status": ("Статус", 100),
        }

        for c, (t, w) in headers.items():

            self.tree.heading(c, text=t)

            self.tree.column(
                c,
                width=w,
                anchor="center" if c in ("start", "finish", "status") else "w",
            )

        vsb = ttk.Scrollbar(left, orient="vertical", command=self.tree.yview)

        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.bind("<Double-1>", lambda e: self._edit_task())

        # gantt
        self.gantt = GanttCanvas(right, linked_tree=self.tree)
        self.gantt.pack(fill="both", expand=True)

    # ----------------------------------------------------
    # LOAD DATA
    # ----------------------------------------------------
    def _load_refs(self):

        try:

            self.objects = GprService.load_objects_short()
            self.work_types = GprService.load_work_types()
            self.uoms = GprService.load_uoms()

        except Exception as e:

            logging.exception("load refs error")

            messagebox.showerror(
                "ГПР",
                f"Ошибка загрузки справочников:\n{e}",
                parent=self,
            )

            return

        vals = []

        for o in self.objects:

            sn = (o.get("short_name") or "").strip()
            addr = (o.get("address") or "").strip()

            if sn:
                vals.append(f"{sn} — {addr}")
            else:
                vals.append(addr)

        self.cmb_obj["values"] = vals

    # ----------------------------------------------------
    # OPEN OBJECT
    # ----------------------------------------------------
    def _open_object(self):

        idx = self.cmb_obj.current()

        if idx < 0:
            messagebox.showwarning(
                "ГПР",
                "Выберите объект",
                parent=self,
            )
            return

        obj = self.objects[idx]

        self.object_db_id = obj["id"]

        uid = (self.app_ref.current_user or {}).get("id")

        try:

            plan = GprService.get_or_create_current_plan(
                self.object_db_id,
                uid,
            )

            self.plan_id = plan["id"]

            self.tasks = GprService.load_plan_tasks(self.plan_id)

        except Exception as e:

            messagebox.showerror(
                "ГПР",
                f"Ошибка:\n{e}",
                parent=self,
            )

            return

        self.tasks_filtered = list(self.tasks)

        self._render()

    # ----------------------------------------------------
    # RENDER
    # ----------------------------------------------------
    def _render(self):

        self.tree.delete(*self.tree.get_children())

        for t in self.tasks_filtered:

            iid = str(t.get("id") or f"tmp_{id(t)}")

            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    t.get("work_type_name"),
                    t.get("name"),
                    fmt_date(t.get("plan_start")),
                    fmt_date(t.get("plan_finish")),
                    t.get("uom_code") or "",
                    fmt_qty(t.get("plan_qty")),
                    STATUS_LABELS.get(t.get("status"), ""),
                ),
            )

        self.gantt.set_range(self.range_from, self.range_to)
        self.gantt.set_data(self.tasks_filtered)

    # ----------------------------------------------------
    # CRUD
    # ----------------------------------------------------
    def _selected_index(self):

        sel = self.tree.selection()

        if not sel:
            return None

        iid = sel[0]

        try:
            tid = int(iid)

            for i, t in enumerate(self.tasks):
                if t.get("id") == tid:
                    return i

        except:
            pass

        return None

    def _add_task(self):

        if not self.plan_id:
            return

        uid = (self.app_ref.current_user or {}).get("id")

        res = open_task_dialog(
            self,
            self.work_types,
            self.uoms,
            init={
                "plan_start": self.range_from,
                "plan_finish": self.range_from,
            },
            user_id=uid,
        )

        if not res:
            return

        res["id"] = None

        res["work_type_name"] = next(
            (w["name"] for w in self.work_types if w["id"] == res["work_type_id"]),
            "",
        )

        self.tasks.append(res)

        self._render()

    def _edit_task(self):

        idx = self._selected_index()

        if idx is None:
            return

        t0 = self.tasks[idx]

        uid = (self.app_ref.current_user or {}).get("id")

        res = open_task_dialog(
            self,
            self.work_types,
            self.uoms,
            init=t0,
            user_id=uid,
        )

        if not res:
            return

        res["id"] = t0.get("id")

        res["work_type_name"] = next(
            (w["name"] for w in self.work_types if w["id"] == res["work_type_id"]),
            "",
        )

        self.tasks[idx] = res

        self._render()

    def _delete_task(self):

        idx = self._selected_index()

        if idx is None:
            return

        if not messagebox.askyesno(
            "ГПР",
            "Удалить задачу?",
            parent=self,
        ):
            return

        self.tasks.pop(idx)

        self._render()

    # ----------------------------------------------------
    # SAVE
    # ----------------------------------------------------
    def _save(self):

        if not self.plan_id:
            return

        uid = (self.app_ref.current_user or {}).get("id")

        try:

            GprService.save_plan_tasks(
                self.plan_id,
                uid,
                self.tasks,
            )

            self.tasks = GprService.load_plan_tasks(self.plan_id)

            self.tasks_filtered = list(self.tasks)

            self._render()

            messagebox.showinfo(
                "ГПР",
                "Сохранено",
                parent=self,
            )

        except Exception as e:

            logging.exception("save error")

            messagebox.showerror(
                "ГПР",
                f"Ошибка сохранения:\n{e}",
                parent=self,
            )


# ----------------------------------------------------
# API
# ----------------------------------------------------
def create_gpr_page(parent, app_ref):

    return GprPage(parent, app_ref)
