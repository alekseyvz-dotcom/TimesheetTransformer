# gpr_dictionaries.py — Справочники ГПР (отдельный модуль)
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from psycopg2.extras import RealDictCursor

# Импортируем из gpr_module то, что нужно
from gpr_module import (
    _conn, _release, _today, _fmt_qty, _safe_float,
    C, STATUS_LABELS, STATUS_LIST,
    TaskEditDialog,
)


# ═══════════════════════════════════════════════════════════════
#  Универсальный диалог справочника
# ═══════════════════════════════════════════════════════════════
class _DictEditDialog(simpledialog.Dialog):
    """Диалог добавления/редактирования одной записи справочника."""

    def __init__(self, parent, title_text: str,
                 fields: List[Tuple[str, str, int]],
                 init: Optional[Dict[str, str]] = None):
        self._fields = fields
        self._init = init or {}
        self.result: Optional[Dict[str, str]] = None
        super().__init__(parent, title=title_text)

    def body(self, m):
        self._vars: Dict[str, tk.StringVar] = {}
        for i, (label, key, width) in enumerate(self._fields):
            tk.Label(m, text=label + ":").grid(
                row=i, column=0, sticky="e", padx=(0, 6), pady=3)
            var = tk.StringVar(value=str(self._init.get(key, "")))
            ent = ttk.Entry(m, textvariable=var, width=width)
            ent.grid(row=i, column=1, sticky="w", pady=3)
            self._vars[key] = var
        return None

    def validate(self):
        self._out = {k: v.get().strip() for k, v in self._vars.items()}
        return True

    def apply(self):
        self.result = self._out


# ═══════════════════════════════════════════════════════════════
#  Главная страница справочников
# ═══════════════════════════════════════════════════════════════
class GprDictionariesPage(tk.Frame):
    """
    Управление справочниками ГПР:
      - Типы работ  (gpr_work_types)
      - Ед. измерения (gpr_uom)
      - Шаблоны      (gpr_templates + gpr_template_tasks)
    """

    def __init__(self, master, app_ref=None):
        super().__init__(master, bg=C["bg"])
        self.app_ref = app_ref
        self._build_ui()

    # ══════════════════════════════════════════════════════
    #  UI
    # ══════════════════════════════════════════════════════
    def _build_ui(self):
        # Заголовок
        hdr = tk.Frame(self, bg=C["accent"], pady=6)
        hdr.pack(fill="x")
        tk.Label(hdr, text="⚙  Справочники ГПР",
                 font=("Segoe UI", 12, "bold"),
                 bg=C["accent"], fg="white", padx=12).pack(side="left")

        # Notebook
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=10, pady=10)

        tab_wt = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_wt, text="  Типы работ  ")
        self._build_wt_tab(tab_wt)

        tab_uom = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_uom, text="  Единицы измерения  ")
        self._build_uom_tab(tab_uom)

        tab_tpl = tk.Frame(self.nb, bg=C["panel"])
        self.nb.add(tab_tpl, text="  Шаблоны  ")
        self._build_tpl_tab(tab_tpl)

    # ══════════════════════════════════════════════════════
    #  ТИПЫ РАБОТ
    # ══════════════════════════════════════════════════════
    def _build_wt_tab(self, parent):
        bar = tk.Frame(parent, bg=C["panel"])
        bar.pack(fill="x", padx=8, pady=6)
        ttk.Button(bar, text="➕ Добавить", command=self._wt_add).pack(side="left", padx=2)
        ttk.Button(bar, text="✏️ Редактировать", command=self._wt_edit).pack(side="left", padx=2)
        ttk.Button(bar, text="🔄 Вкл/Выкл", command=self._wt_toggle).pack(side="left", padx=2)
        ttk.Button(bar, text="🔃 Обновить", command=self._wt_load).pack(side="left", padx=8)

        cols = ("id", "code", "name", "sort_order", "is_active")
        self.wt_tree = ttk.Treeview(parent, columns=cols, show="headings",
                                     selectmode="browse", height=18)
        for c, t, w, a in [
            ("id",         "ID",         50,  "center"),
            ("code",       "Код",        80,  "w"),
            ("name",       "Наименование", 300, "w"),
            ("sort_order", "Сортировка", 80,  "center"),
            ("is_active",  "Активен",    70,  "center"),
        ]:
            self.wt_tree.heading(c, text=t)
            self.wt_tree.column(c, width=w, anchor=a)

        vsb = ttk.Scrollbar(parent, orient="vertical", command=self.wt_tree.yview)
        self.wt_tree.configure(yscrollcommand=vsb.set)
        self.wt_tree.pack(side="left", fill="both", expand=True,
                          padx=(8, 0), pady=(0, 8))
        vsb.pack(side="right", fill="y", padx=(0, 8), pady=(0, 8))

        self.wt_tree.tag_configure("inactive", foreground="#bbb")
        self.wt_tree.bind("<Double-1>", lambda e: self._wt_edit())

        self._wt_data: List[Dict] = []
        self._wt_load()

    def _wt_load(self):
        self.wt_tree.delete(*self.wt_tree.get_children())
        self._wt_data.clear()
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, COALESCE(code,'') AS code, name,
                           sort_order, is_active
                    FROM public.gpr_work_types
                    ORDER BY sort_order, name
                """)
                self._wt_data = [dict(r) for r in cur.fetchall()]
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)

        for w in self._wt_data:
            active = "Да" if w["is_active"] else "Нет"
            tags = () if w["is_active"] else ("inactive",)
            self.wt_tree.insert("", "end",
                values=(w["id"], w["code"], w["name"], w["sort_order"], active),
                tags=tags)

    def _wt_sel(self) -> Optional[Dict]:
        sel = self.wt_tree.selection()
        if not sel:
            return None
        idx = self.wt_tree.index(sel[0])
        return self._wt_data[idx] if 0 <= idx < len(self._wt_data) else None

    def _wt_add(self):
        fields = [
            ("Код",              "code",       12),
            ("Наименование",     "name",       40),
            ("Порядок сортировки","sort_order", 8),
        ]
        dlg = _DictEditDialog(self, "Новый тип работ", fields,
                               init={"sort_order": "100"})
        if not dlg.result:
            return
        r = dlg.result
        if not r.get("name"):
            messagebox.showwarning("Справочники", "Наименование обязательно.", parent=self)
            return
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.gpr_work_types(code, name, sort_order, is_active)
                    VALUES (%s, %s, %s, true)
                """, (r["code"] or None, r["name"], int(r["sort_order"] or 100)))
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)
        self._wt_load()

    def _wt_edit(self):
        w = self._wt_sel()
        if not w:
            return
        fields = [
            ("Код",              "code",       12),
            ("Наименование",     "name",       40),
            ("Порядок сортировки","sort_order", 8),
        ]
        dlg = _DictEditDialog(self, "Редактирование типа работ", fields,
                               init={"code": w["code"], "name": w["name"],
                                     "sort_order": str(w["sort_order"])})
        if not dlg.result:
            return
        r = dlg.result
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.gpr_work_types
                    SET code=%s, name=%s, sort_order=%s
                    WHERE id=%s
                """, (r["code"] or None, r["name"],
                      int(r["sort_order"] or 0), w["id"]))
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)
        self._wt_load()

    def _wt_toggle(self):
        w = self._wt_sel()
        if not w:
            return
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.gpr_work_types SET is_active = NOT is_active WHERE id=%s",
                    (w["id"],))
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)
        self._wt_load()

    # ══════════════════════════════════════════════════════
    #  ЕДИНИЦЫ ИЗМЕРЕНИЯ
    # ══════════════════════════════════════════════════════
    def _build_uom_tab(self, parent):
        bar = tk.Frame(parent, bg=C["panel"])
        bar.pack(fill="x", padx=8, pady=6)
        ttk.Button(bar, text="➕ Добавить", command=self._uom_add).pack(side="left", padx=2)
        ttk.Button(bar, text="✏️ Редактировать", command=self._uom_edit).pack(side="left", padx=2)
        ttk.Button(bar, text="🗑 Удалить", command=self._uom_del).pack(side="left", padx=2)
        ttk.Button(bar, text="🔃 Обновить", command=self._uom_load).pack(side="left", padx=8)

        cols = ("code", "name")
        self.uom_tree = ttk.Treeview(parent, columns=cols, show="headings",
                                      selectmode="browse", height=18)
        self.uom_tree.heading("code", text="Код")
        self.uom_tree.heading("name", text="Наименование")
        self.uom_tree.column("code", width=100, anchor="w")
        self.uom_tree.column("name", width=300, anchor="w")

        vsb = ttk.Scrollbar(parent, orient="vertical", command=self.uom_tree.yview)
        self.uom_tree.configure(yscrollcommand=vsb.set)
        self.uom_tree.pack(side="left", fill="both", expand=True,
                           padx=(8, 0), pady=(0, 8))
        vsb.pack(side="right", fill="y", padx=(0, 8), pady=(0, 8))
        self.uom_tree.bind("<Double-1>", lambda e: self._uom_edit())

        self._uom_data: List[Dict] = []
        self._uom_load()

    def _uom_load(self):
        self.uom_tree.delete(*self.uom_tree.get_children())
        self._uom_data.clear()
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT code, name FROM public.gpr_uom ORDER BY code")
                self._uom_data = [dict(r) for r in cur.fetchall()]
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)
        for u in self._uom_data:
            self.uom_tree.insert("", "end", values=(u["code"], u["name"]))

    def _uom_sel(self) -> Optional[Dict]:
        sel = self.uom_tree.selection()
        if not sel:
            return None
        idx = self.uom_tree.index(sel[0])
        return self._uom_data[idx] if 0 <= idx < len(self._uom_data) else None

    def _uom_add(self):
        fields = [("Код", "code", 12), ("Наименование", "name", 30)]
        dlg = _DictEditDialog(self, "Новая единица измерения", fields)
        if not dlg.result:
            return
        r = dlg.result
        if not r["code"]:
            messagebox.showwarning("Справочники", "Код обязателен.", parent=self)
            return
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.gpr_uom(code, name) VALUES(%s,%s)",
                    (r["code"], r["name"]))
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)
        self._uom_load()

    def _uom_edit(self):
        u = self._uom_sel()
        if not u:
            return
        fields = [("Код (только чтение)", "code", 12), ("Наименование", "name", 30)]
        dlg = _DictEditDialog(self, "Редактирование ед. изм.", fields,
                               init={"code": u["code"], "name": u["name"]})
        if not dlg.result:
            return
        r = dlg.result
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("UPDATE public.gpr_uom SET name=%s WHERE code=%s",
                            (r["name"], u["code"]))
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)
        self._uom_load()

    def _uom_del(self):
        u = self._uom_sel()
        if not u:
            return
        if not messagebox.askyesno("Справочники",
                f"Удалить единицу '{u['code']}'?\n"
                f"(Не получится, если она используется в задачах)",
                parent=self):
            return
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("DELETE FROM public.gpr_uom WHERE code=%s",
                            (u["code"],))
        except Exception as e:
            messagebox.showerror("Справочники",
                f"Ошибка (возможно, используется):\n{e}", parent=self)
            return
        finally:
            _release(conn)
        self._uom_load()

    # ══════════════════════════════════════════════════════
    #  ШАБЛОНЫ + ЗАДАЧИ ШАБЛОНОВ
    # ══════════════════════════════════════════════════════
    def _build_tpl_tab(self, parent):
        # --- верх: список шаблонов ---
        top = tk.LabelFrame(parent, text=" Шаблоны ", padx=8, pady=4,
                            bg=C["panel"])
        top.pack(fill="x", padx=8, pady=(6, 2))

        bar = tk.Frame(top, bg=C["panel"])
        bar.pack(fill="x")
        ttk.Button(bar, text="➕ Добавить шаблон",
                   command=self._tpl_add).pack(side="left", padx=2)
        ttk.Button(bar, text="✏️ Переименовать",
                   command=self._tpl_rename).pack(side="left", padx=2)
        ttk.Button(bar, text="🔄 Вкл/Выкл",
                   command=self._tpl_toggle).pack(side="left", padx=2)
        ttk.Button(bar, text="🔃 Обновить",
                   command=self._tpl_load).pack(side="left", padx=8)

        cols_t = ("id", "name", "is_active")
        self.tpl_tree = ttk.Treeview(top, columns=cols_t, show="headings",
                                      selectmode="browse", height=6)
        self.tpl_tree.heading("id", text="ID")
        self.tpl_tree.heading("name", text="Наименование шаблона")
        self.tpl_tree.heading("is_active", text="Активен")
        self.tpl_tree.column("id", width=50, anchor="center")
        self.tpl_tree.column("name", width=400, anchor="w")
        self.tpl_tree.column("is_active", width=70, anchor="center")
        self.tpl_tree.pack(fill="x", pady=(4, 6))
        self.tpl_tree.tag_configure("inactive", foreground="#bbb")
        self.tpl_tree.bind("<<TreeviewSelect>>", lambda e: self._tt_load())

        # --- низ: задачи выбранного шаблона ---
        bot = tk.LabelFrame(parent, text=" Задачи шаблона ", padx=8, pady=4,
                            bg=C["panel"])
        bot.pack(fill="both", expand=True, padx=8, pady=(2, 8))

        bar2 = tk.Frame(bot, bg=C["panel"])
        bar2.pack(fill="x")
        ttk.Button(bar2, text="➕ Добавить задачу",
                   command=self._tt_add).pack(side="left", padx=2)
        ttk.Button(bar2, text="✏️ Редактировать",
                   command=self._tt_edit).pack(side="left", padx=2)
        ttk.Button(bar2, text="🗑 Удалить",
                   command=self._tt_del).pack(side="left", padx=2)

        cols_tt = ("sort", "work_type", "name", "uom", "qty", "milestone")
        self.tt_tree = ttk.Treeview(bot, columns=cols_tt, show="headings",
                                     selectmode="browse", height=14)
        for c, t, w, a in [
            ("sort",      "№",           40,  "center"),
            ("work_type", "Тип работ",  160, "w"),
            ("name",      "Наименование",280, "w"),
            ("uom",       "Ед.",         60,  "center"),
            ("qty",       "Объём",       70,  "e"),
            ("milestone", "Веха",        50,  "center"),
        ]:
            self.tt_tree.heading(c, text=t)
            self.tt_tree.column(c, width=w, anchor=a)

        vsb = ttk.Scrollbar(bot, orient="vertical", command=self.tt_tree.yview)
        self.tt_tree.configure(yscrollcommand=vsb.set)
        self.tt_tree.pack(side="left", fill="both", expand=True, pady=(4, 0))
        vsb.pack(side="right", fill="y", pady=(4, 0))
        self.tt_tree.bind("<Double-1>", lambda e: self._tt_edit())

        self._tpl_data: List[Dict] = []
        self._tt_data: List[Dict] = []
        self._wt_cache: List[Dict] = []
        self._uom_cache: List[Dict] = []
        self._tpl_load()

    # --- шаблоны CRUD ---
    def _tpl_load(self):
        self.tpl_tree.delete(*self.tpl_tree.get_children())
        self._tpl_data.clear()
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, name, is_active
                    FROM public.gpr_templates ORDER BY name
                """)
                self._tpl_data = [dict(r) for r in cur.fetchall()]
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)

        for t in self._tpl_data:
            active = "Да" if t["is_active"] else "Нет"
            tags = () if t["is_active"] else ("inactive",)
            self.tpl_tree.insert("", "end",
                values=(t["id"], t["name"], active), tags=tags)

    def _tpl_sel(self) -> Optional[Dict]:
        sel = self.tpl_tree.selection()
        if not sel:
            return None
        idx = self.tpl_tree.index(sel[0])
        return self._tpl_data[idx] if 0 <= idx < len(self._tpl_data) else None

    def _tpl_add(self):
        name = simpledialog.askstring("Новый шаблон", "Наименование:",
                                       parent=self)
        if not name or not name.strip():
            return
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.gpr_templates(name, is_active) VALUES(%s, true)",
                    (name.strip(),))
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)
        self._tpl_load()

    def _tpl_rename(self):
        t = self._tpl_sel()
        if not t:
            return
        name = simpledialog.askstring("Переименовать", "Новое наименование:",
                                       initialvalue=t["name"], parent=self)
        if not name or not name.strip():
            return
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("UPDATE public.gpr_templates SET name=%s WHERE id=%s",
                            (name.strip(), t["id"]))
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)
        self._tpl_load()

    def _tpl_toggle(self):
        t = self._tpl_sel()
        if not t:
            return
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.gpr_templates SET is_active = NOT is_active WHERE id=%s",
                    (t["id"],))
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)
        self._tpl_load()

    # --- задачи шаблонов ---
    def _tt_load(self):
        self.tt_tree.delete(*self.tt_tree.get_children())
        self._tt_data.clear()
        t = self._tpl_sel()
        if not t:
            return
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT tt.id, tt.work_type_id,
                           wt.name AS wt_name,
                           tt.name, tt.uom_code, tt.default_qty,
                           tt.is_milestone, tt.sort_order
                    FROM public.gpr_template_tasks tt
                    JOIN public.gpr_work_types wt ON wt.id = tt.work_type_id
                    WHERE tt.template_id = %s
                    ORDER BY tt.sort_order, tt.id
                """, (t["id"],))
                self._tt_data = [dict(r) for r in cur.fetchall()]

                # кэш для диалога добавления/редактирования
                cur.execute("""
                    SELECT id, name FROM public.gpr_work_types
                    WHERE is_active = true ORDER BY sort_order, name
                """)
                self._wt_cache = [dict(r) for r in cur.fetchall()]

                cur.execute("SELECT code, name FROM public.gpr_uom ORDER BY code")
                self._uom_cache = [dict(r) for r in cur.fetchall()]
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)

        for tt in self._tt_data:
            ms = "✓" if tt.get("is_milestone") else ""
            self.tt_tree.insert("", "end", values=(
                tt["sort_order"],
                tt["wt_name"],
                tt["name"],
                tt.get("uom_code") or "",
                _fmt_qty(tt.get("default_qty")),
                ms,
            ))

    def _tt_sel(self) -> Optional[Dict]:
        sel = self.tt_tree.selection()
        if not sel:
            return None
        idx = self.tt_tree.index(sel[0])
        return self._tt_data[idx] if 0 <= idx < len(self._tt_data) else None

    def _tt_add(self):
        tpl = self._tpl_sel()
        if not tpl:
            messagebox.showinfo("Справочники", "Выберите шаблон.", parent=self)
            return
        dlg = TaskEditDialog(self, self._wt_cache, self._uom_cache,
                              init={"plan_start": _today(),
                                    "plan_finish": _today()})
        if not dlg.result:
            return
        r = dlg.result
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.gpr_template_tasks
                        (template_id, work_type_id, name, uom_code,
                         default_qty, is_milestone, sort_order)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    tpl["id"],
                    r["work_type_id"],
                    r["name"],
                    r.get("uom_code"),
                    r.get("plan_qty"),
                    r.get("is_milestone", False),
                    len(self._tt_data) * 10 + 10,
                ))
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)
        self._tt_load()

    def _tt_edit(self):
        tt = self._tt_sel()
        if not tt:
            return
        init = {
            "work_type_id": tt["work_type_id"],
            "name":         tt["name"],
            "uom_code":     tt.get("uom_code"),
            "plan_qty":     tt.get("default_qty"),
            "plan_start":   _today(),
            "plan_finish":  _today(),
            "is_milestone": tt.get("is_milestone", False),
        }
        dlg = TaskEditDialog(self, self._wt_cache, self._uom_cache, init=init)
        if not dlg.result:
            return
        r = dlg.result
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.gpr_template_tasks
                    SET work_type_id=%s, name=%s, uom_code=%s,
                        default_qty=%s, is_milestone=%s
                    WHERE id=%s
                """, (
                    r["work_type_id"], r["name"], r.get("uom_code"),
                    r.get("plan_qty"), r.get("is_milestone", False),
                    tt["id"],
                ))
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)
        self._tt_load()

    def _tt_del(self):
        tt = self._tt_sel()
        if not tt:
            return
        if not messagebox.askyesno("Справочники",
                f"Удалить задачу «{tt['name']}»?", parent=self):
            return
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM public.gpr_template_tasks WHERE id=%s",
                    (tt["id"],))
        except Exception as e:
            messagebox.showerror("Справочники", f"Ошибка:\n{e}", parent=self)
            return
        finally:
            _release(conn)
        self._tt_load()


# ═══════════════════════════════════════════════════════════════
#  API для main_app
# ═══════════════════════════════════════════════════════════════
def create_gpr_dicts_page(parent, app_ref) -> GprDictionariesPage:
    """Фабричная функция."""
    return GprDictionariesPage(parent, app_ref=app_ref)
