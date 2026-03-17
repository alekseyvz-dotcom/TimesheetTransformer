"""
dictionaries_page.py
Справочники ГПР
"""

from __future__ import annotations

from typing import Dict, List, Optional

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from psycopg2.extras import RealDictCursor

from gpr_common import C, fmt_qty
from gpr_db import get_conn, release_conn


class GprDictionariesPage(tk.Frame):

    def __init__(self, master, app_ref=None):
        super().__init__(master, bg=C["bg"])

        self.app_ref = app_ref

        self._wt_data: List[Dict] = []
        self._uom_data: List[Dict] = []

        self._build_ui()

        self._wt_load()
        self._uom_load()

    # ----------------------------------------------------
    # UI
    # ----------------------------------------------------
    def _build_ui(self):

        hdr = tk.Frame(self, bg=C["accent"], pady=6)
        hdr.pack(fill="x")

        tk.Label(
            hdr,
            text="⚙ Справочники ГПР",
            font=("Segoe UI", 12, "bold"),
            bg=C["accent"],
            fg="white",
            padx=12,
        ).pack(side="left")

        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=10, pady=10)

        tab_wt = tk.Frame(nb, bg=C["panel"])
        tab_uom = tk.Frame(nb, bg=C["panel"])

        nb.add(tab_wt, text="Типы работ")
        nb.add(tab_uom, text="Ед. измерения")

        self._build_wt_tab(tab_wt)
        self._build_uom_tab(tab_uom)

    # ----------------------------------------------------
    # WORK TYPES
    # ----------------------------------------------------
    def _build_wt_tab(self, parent):

        bar = tk.Frame(parent, bg=C["panel"])
        bar.pack(fill="x", padx=6, pady=6)

        ttk.Button(bar, text="Добавить", command=self._wt_add).pack(
            side="left", padx=2
        )

        ttk.Button(bar, text="Редактировать", command=self._wt_edit).pack(
            side="left", padx=2
        )

        ttk.Button(bar, text="Вкл/Выкл", command=self._wt_toggle).pack(
            side="left", padx=2
        )

        cols = ("id", "code", "name", "sort", "active")

        self.wt_tree = ttk.Treeview(
            parent,
            columns=cols,
            show="headings",
            height=18,
        )

        headers = {
            "id": ("ID", 50),
            "code": ("Код", 80),
            "name": ("Наименование", 300),
            "sort": ("Сорт.", 70),
            "active": ("Активен", 70),
        }

        for c, (t, w) in headers.items():

            self.wt_tree.heading(c, text=t)

            self.wt_tree.column(c, width=w)

        self.wt_tree.pack(fill="both", expand=True, padx=6, pady=6)

        self.wt_tree.bind("<Double-1>", lambda e: self._wt_edit())

    def _wt_load(self):

        self.wt_tree.delete(*self.wt_tree.get_children())

        conn = None

        try:

            conn = get_conn()

            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                cur.execute("""
                    SELECT id, code, name, sort_order, is_active
                    FROM public.gpr_work_types
                    ORDER BY sort_order, name
                """)

                self._wt_data = [dict(r) for r in cur.fetchall()]

        finally:

            release_conn(conn)

        for w in self._wt_data:

            self.wt_tree.insert(
                "",
                "end",
                values=(
                    w["id"],
                    w["code"],
                    w["name"],
                    w["sort_order"],
                    "Да" if w["is_active"] else "Нет",
                ),
            )

    def _wt_sel(self) -> Optional[Dict]:

        sel = self.wt_tree.selection()

        if not sel:
            return None

        idx = self.wt_tree.index(sel[0])

        return self._wt_data[idx]

    def _wt_add(self):

        name = simpledialog.askstring(
            "Тип работ",
            "Наименование:",
            parent=self,
        )

        if not name:
            return

        conn = None

        try:

            conn = get_conn()

            with conn, conn.cursor() as cur:

                cur.execute("""
                    INSERT INTO public.gpr_work_types
                    (name, sort_order, is_active)
                    VALUES (%s, 100, true)
                """, (name.strip(),))

        finally:

            release_conn(conn)

        self._wt_load()

    def _wt_edit(self):

        w = self._wt_sel()

        if not w:
            return

        name = simpledialog.askstring(
            "Редактировать",
            "Наименование:",
            initialvalue=w["name"],
            parent=self,
        )

        if not name:
            return

        conn = None

        try:

            conn = get_conn()

            with conn, conn.cursor() as cur:

                cur.execute("""
                    UPDATE public.gpr_work_types
                    SET name=%s
                    WHERE id=%s
                """, (name.strip(), w["id"]))

        finally:

            release_conn(conn)

        self._wt_load()

    def _wt_toggle(self):

        w = self._wt_sel()

        if not w:
            return

        conn = None

        try:

            conn = get_conn()

            with conn, conn.cursor() as cur:

                cur.execute("""
                    UPDATE public.gpr_work_types
                    SET is_active = NOT is_active
                    WHERE id=%s
                """, (w["id"],))

        finally:

            release_conn(conn)

        self._wt_load()

    # ----------------------------------------------------
    # UOM
    # ----------------------------------------------------
    def _build_uom_tab(self, parent):

        bar = tk.Frame(parent, bg=C["panel"])
        bar.pack(fill="x", padx=6, pady=6)

        ttk.Button(bar, text="Добавить", command=self._uom_add).pack(
            side="left", padx=2
        )

        ttk.Button(bar, text="Редактировать", command=self._uom_edit).pack(
            side="left", padx=2
        )

        ttk.Button(bar, text="Удалить", command=self._uom_del).pack(
            side="left", padx=2
        )

        cols = ("code", "name")

        self.uom_tree = ttk.Treeview(
            parent,
            columns=cols,
            show="headings",
            height=18,
        )

        self.uom_tree.heading("code", text="Код")
        self.uom_tree.heading("name", text="Наименование")

        self.uom_tree.pack(fill="both", expand=True, padx=6, pady=6)

        self.uom_tree.bind("<Double-1>", lambda e: self._uom_edit())

    def _uom_load(self):

        self.uom_tree.delete(*self.uom_tree.get_children())

        conn = None

        try:

            conn = get_conn()

            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                cur.execute("""
                    SELECT code, name
                    FROM public.gpr_uom
                    ORDER BY code
                """)

                self._uom_data = [dict(r) for r in cur.fetchall()]

        finally:

            release_conn(conn)

        for u in self._uom_data:

            self.uom_tree.insert(
                "",
                "end",
                values=(u["code"], u["name"]),
            )

    def _uom_sel(self):

        sel = self.uom_tree.selection()

        if not sel:
            return None

        idx = self.uom_tree.index(sel[0])

        return self._uom_data[idx]

    def _uom_add(self):

        code = simpledialog.askstring(
            "Ед. измерения",
            "Код:",
            parent=self,
        )

        if not code:
            return

        name = simpledialog.askstring(
            "Ед. измерения",
            "Наименование:",
            parent=self,
        )

        if not name:
            return

        conn = None

        try:

            conn = get_conn()

            with conn, conn.cursor() as cur:

                cur.execute("""
                    INSERT INTO public.gpr_uom(code, name)
                    VALUES (%s,%s)
                """, (code.strip(), name.strip()))

        finally:

            release_conn(conn)

        self._uom_load()

    def _uom_edit(self):

        u = self._uom_sel()

        if not u:
            return

        name = simpledialog.askstring(
            "Редактировать",
            "Наименование:",
            initialvalue=u["name"],
            parent=self,
        )

        if not name:
            return

        conn = None

        try:

            conn = get_conn()

            with conn, conn.cursor() as cur:

                cur.execute("""
                    UPDATE public.gpr_uom
                    SET name=%s
                    WHERE code=%s
                """, (name.strip(), u["code"]))

        finally:

            release_conn(conn)

        self._uom_load()

    def _uom_del(self):

        u = self._uom_sel()

        if not u:
            return

        if not messagebox.askyesno(
            "Удалить",
            f"Удалить '{u['code']}'?",
            parent=self,
        ):
            return

        conn = None

        try:

            conn = get_conn()

            with conn, conn.cursor() as cur:

                cur.execute("""
                    DELETE FROM public.gpr_uom
                    WHERE code=%s
                """, (u["code"],))

        finally:

            release_conn(conn)

        self._uom_load()


# ----------------------------------------------------
# API
# ----------------------------------------------------
def create_gpr_dicts_page(parent, app_ref):

    return GprDictionariesPage(parent, app_ref)
