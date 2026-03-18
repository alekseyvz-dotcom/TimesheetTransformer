# gpr_dictionaries.py — профессиональные справочники ГПР
from __future__ import annotations

import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from psycopg2.extras import RealDictCursor


logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  DB POOL
# ═══════════════════════════════════════════════════════════════
db_connection_pool = None


def set_db_pool(pool):
    global db_connection_pool
    db_connection_pool = pool


def _conn():
    if not db_connection_pool:
        raise RuntimeError("DB pool not set (gpr_dictionaries)")
    return db_connection_pool.getconn()


def _release(conn):
    if db_connection_pool and conn:
        try:
            db_connection_pool.putconn(conn)
        except Exception:
            logger.exception("Error releasing DB connection in gpr_dictionaries")


# ═══════════════════════════════════════════════════════════════
#  CONST
# ═══════════════════════════════════════════════════════════════
C = {
    "bg": "#f0f2f5",
    "panel": "#ffffff",
    "accent": "#1565c0",
    "accent_light": "#e3f2fd",
    "success": "#2e7d32",
    "warning": "#ed6c02",
    "error": "#d32f2f",
    "border": "#dde1e7",
    "text": "#1a1a2e",
    "text2": "#555",
    "text3": "#999",
    "btn_bg": "#1565c0",
    "btn_fg": "#ffffff",
}

STATUS_LABELS = {
    "planned": "Запланировано",
    "in_progress": "В работе",
    "done": "Выполнено",
    "paused": "Приостановлено",
    "canceled": "Отменено",
}
STATUS_LIST = ["planned", "in_progress", "done", "paused", "canceled"]


def _today() -> date:
    return datetime.now().date()


def _safe_float(v):
    if v is None:
        return None
    try:
        s = str(v).strip().replace(",", ".")
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _fmt_qty(v) -> str:
    f = _safe_float(v)
    if f is None:
        return ""
    return f"{f:.3f}".rstrip("0").rstrip(".")


def _fmt_dt(v) -> str:
    if isinstance(v, datetime):
        return v.strftime("%d.%m.%Y %H:%M")
    return str(v or "—")


def _user_id(app_ref) -> Optional[int]:
    try:
        return (getattr(app_ref, "current_user", None) or {}).get("id")
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
#  SERVICES
# ═══════════════════════════════════════════════════════════════
class GprTemplateService:
    @staticmethod
    def load_templates_full(search: str = "") -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                params = []
                where = ""
                if search.strip():
                    where = """
                        WHERE t.name ILIKE %s
                           OR COALESCE(t.category, '') ILIKE %s
                           OR COALESCE(t.description, '') ILIKE %s
                    """
                    q = f"%{search.strip()}%"
                    params.extend([q, q, q])

                cur.execute(f"""
                    SELECT t.id,
                           t.name,
                           t.description,
                           t.category,
                           t.note,
                           t.is_active,
                           t.created_at,
                           t.updated_at,
                           COALESCE(cu.full_name, '') AS creator_name,
                           COALESCE(uu.full_name, '') AS updater_name,
                           COALESCE(cnt.task_count, 0) AS task_count
                    FROM public.gpr_templates t
                    LEFT JOIN public.app_users cu ON cu.id = t.created_by
                    LEFT JOIN public.app_users uu ON uu.id = t.updated_by
                    LEFT JOIN (
                        SELECT template_id, COUNT(*) AS task_count
                        FROM public.gpr_template_tasks
                        GROUP BY template_id
                    ) cnt ON cnt.template_id = t.id
                    {where}
                    ORDER BY t.is_active DESC, t.name
                """, params)
                return [dict(r) for r in cur.fetchall()]
        finally:
            _release(conn)

    @staticmethod
    def load_template_tasks(template_id: int) -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT tt.id,
                           tt.template_id,
                           tt.work_type_id,
                           tt.parent_id,
                           wt.name AS wt_name,
                           tt.name,
                           tt.uom_code,
                           tt.default_qty,
                           tt.is_milestone,
                           tt.sort_order,
                           tt.created_at,
                           tt.updated_at
                    FROM public.gpr_template_tasks tt
                    JOIN public.gpr_work_types wt ON wt.id = tt.work_type_id
                    WHERE tt.template_id = %s
                    ORDER BY tt.sort_order, tt.id
                """, (template_id,))
                return [dict(r) for r in cur.fetchall()]
        finally:
            _release(conn)

    @staticmethod
    def create_template(
        name: str,
        category: Optional[str] = None,
        description: Optional[str] = None,
        note: Optional[str] = None,
        created_by: Optional[int] = None,
    ) -> int:
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.gpr_templates
                        (name, category, description, note, is_active, created_by, updated_by)
                    VALUES (%s, %s, %s, %s, true, %s, %s)
                    RETURNING id
                """, (
                    name.strip(),
                    (category or "").strip() or None,
                    (description or "").strip() or None,
                    (note or "").strip() or None,
                    created_by,
                    created_by,
                ))
                return int(cur.fetchone()[0])
        finally:
            _release(conn)

    @staticmethod
    def update_template(
        template_id: int,
        name: str,
        category: Optional[str] = None,
        description: Optional[str] = None,
        note: Optional[str] = None,
        updated_by: Optional[int] = None,
    ) -> None:
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.gpr_templates
                    SET name=%s,
                        category=%s,
                        description=%s,
                        note=%s,
                        updated_by=%s
                    WHERE id=%s
                """, (
                    name.strip(),
                    (category or "").strip() or None,
                    (description or "").strip() or None,
                    (note or "").strip() or None,
                    updated_by,
                    template_id,
                ))
        finally:
            _release(conn)

    @staticmethod
    def toggle_template(template_id: int, updated_by: Optional[int] = None) -> None:
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.gpr_templates
                    SET is_active = NOT is_active,
                        updated_by=%s
                    WHERE id=%s
                """, (updated_by, template_id))
        finally:
            _release(conn)

    @staticmethod
    def delete_template(template_id: int) -> None:
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("DELETE FROM public.gpr_templates WHERE id=%s", (template_id,))
        finally:
            _release(conn)

    @staticmethod
    def duplicate_template(
        template_id: int,
        new_name: str,
        created_by: Optional[int] = None,
    ) -> int:
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT name, category, description, note
                    FROM public.gpr_templates
                    WHERE id=%s
                """, (template_id,))
                src = cur.fetchone()
                if not src:
                    raise ValueError("Шаблон не найден")

                cur.execute("""
                    INSERT INTO public.gpr_templates
                        (name, category, description, note, is_active, created_by, updated_by)
                    VALUES (%s, %s, %s, %s, true, %s, %s)
                    RETURNING id
                """, (
                    new_name.strip(),
                    src.get("category"),
                    src.get("description"),
                    src.get("note"),
                    created_by,
                    created_by,
                ))
                new_tpl_id = int(cur.fetchone()["id"])

                cur.execute("""
                    SELECT id, work_type_id, parent_id, name, uom_code,
                           default_qty, is_milestone, sort_order
                    FROM public.gpr_template_tasks
                    WHERE template_id=%s
                    ORDER BY sort_order, id
                """, (template_id,))
                old_tasks = [dict(r) for r in cur.fetchall()]

                id_map: Dict[int, int] = {}

                for t in old_tasks:
                    cur.execute("""
                        INSERT INTO public.gpr_template_tasks
                            (template_id, work_type_id, parent_id, name, uom_code,
                             default_qty, is_milestone, sort_order)
                        VALUES (%s, %s, NULL, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        new_tpl_id,
                        t["work_type_id"],
                        t["name"],
                        t.get("uom_code"),
                        t.get("default_qty"),
                        t.get("is_milestone", False),
                        t.get("sort_order", 0),
                    ))
                    new_id = int(cur.fetchone()["id"])
                    id_map[int(t["id"])] = new_id

                for t in old_tasks:
                    old_parent = t.get("parent_id")
                    if old_parent and int(old_parent) in id_map:
                        cur.execute("""
                            UPDATE public.gpr_template_tasks
                            SET parent_id=%s
                            WHERE id=%s
                        """, (id_map[int(old_parent)], id_map[int(t["id"])]))

                return new_tpl_id
        finally:
            _release(conn)


# ═══════════════════════════════════════════════════════════════
#  DIALOGS
# ═══════════════════════════════════════════════════════════════
class _DictEditDialog(simpledialog.Dialog):
    """Диалог добавления/редактирования одной записи справочника."""

    def __init__(
        self,
        parent,
        title_text: str,
        fields: List[Tuple[str, str, int]],
        init: Optional[Dict[str, str]] = None,
        readonly_keys: Optional[List[str]] = None,
    ):
        self._fields = fields
        self._init = init or {}
        self._readonly_keys = set(readonly_keys or [])
        self.result: Optional[Dict[str, str]] = None
        super().__init__(parent, title=title_text)

    def body(self, m):
        self._vars: Dict[str, tk.StringVar] = {}
        self._entries: Dict[str, ttk.Entry] = {}
        for i, (label, key, width) in enumerate(self._fields):
            tk.Label(m, text=label + ":").grid(
                row=i, column=0, sticky="e", padx=(0, 6), pady=4
            )
            var = tk.StringVar(value=str(self._init.get(key, "")))
            ent = ttk.Entry(m, textvariable=var, width=width)
            ent.grid(row=i, column=1, sticky="w", pady=4)
            if key in self._readonly_keys:
                ent.state(["readonly"])
            self._vars[key] = var
            self._entries[key] = ent

        if self._fields:
            first_key = self._fields[0][1]
            return self._entries[first_key]
        return None

    def validate(self):
        self._out = {k: v.get().strip() for k, v in self._vars.items()}
        return True

    def apply(self):
        self.result = self._out


class _TemplateEditDialog(simpledialog.Dialog):
    def __init__(self, parent, init: Optional[Dict[str, Any]] = None):
        self.init = init or {}
        self.result: Optional[Dict[str, Any]] = None
        super().__init__(parent, title="Карточка шаблона")

    def body(self, m):
        m.grid_columnconfigure(1, weight=1)

        tk.Label(m, text="Наименование *:").grid(
            row=0, column=0, sticky="e", padx=(0, 8), pady=5
        )
        self.var_name = tk.StringVar(value=self.init.get("name", ""))
        self.ent_name = ttk.Entry(m, textvariable=self.var_name, width=52)
        self.ent_name.grid(row=0, column=1, sticky="ew", pady=5)

        tk.Label(m, text="Категория:").grid(
            row=1, column=0, sticky="e", padx=(0, 8), pady=5
        )
        self.var_category = tk.StringVar(value=self.init.get("category", ""))
        self.ent_category = ttk.Entry(m, textvariable=self.var_category, width=52)
        self.ent_category.grid(row=1, column=1, sticky="ew", pady=5)

        tk.Label(m, text="Описание:").grid(
            row=2, column=0, sticky="ne", padx=(0, 8), pady=5
        )
        self.txt_desc = tk.Text(m, width=52, height=4, wrap="word")
        self.txt_desc.grid(row=2, column=1, sticky="ew", pady=5)
        self.txt_desc.insert("1.0", self.init.get("description", "") or "")

        tk.Label(m, text="Примечание:").grid(
            row=3, column=0, sticky="ne", padx=(0, 8), pady=5
        )
        self.txt_note = tk.Text(m, width=52, height=4, wrap="word")
        self.txt_note.grid(row=3, column=1, sticky="ew", pady=5)
        self.txt_note.insert("1.0", self.init.get("note", "") or "")

        info = self.init.get("_info")
        if info:
            tk.Label(
                m,
                text=info,
                fg=C["text3"],
                justify="left",
                anchor="w",
            ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(8, 0))

        return self.ent_name

    def validate(self):
        name = self.var_name.get().strip()
        if not name:
            messagebox.showwarning("Шаблон", "Введите наименование шаблона.", parent=self)
            return False

        self.result = {
            "name": name,
            "category": self.var_category.get().strip(),
            "description": self.txt_desc.get("1.0", "end").strip(),
            "note": self.txt_note.get("1.0", "end").strip(),
        }
        return True


class _TemplateTaskDialog(simpledialog.Dialog):
    """Диалог добавления/редактирования задачи шаблона."""

    def __init__(self, parent, work_types, uoms, parents=None, init=None):
        self.work_types = work_types or []
        self.uoms = uoms or []
        self.parents = parents or []
        self.init = init or {}
        self.result = None
        super().__init__(parent, title="Задача шаблона")

    def body(self, master):
        master.grid_columnconfigure(1, weight=1)

        self.var_name = tk.StringVar(value=self.init.get("name", ""))
        self.var_qty = tk.StringVar(value=_fmt_qty(self.init.get("default_qty")))
        self.var_milestone = tk.BooleanVar(value=bool(self.init.get("is_milestone", False)))
        self.var_sort = tk.StringVar(value=str(self.init.get("sort_order", 10)))

        tk.Label(master, text="Тип работ *:").grid(
            row=0, column=0, sticky="e", padx=6, pady=4
        )
        self.cmb_wt = ttk.Combobox(
            master,
            state="readonly",
            width=42,
            values=[w["name"] for w in self.work_types],
        )
        self.cmb_wt.grid(row=0, column=1, sticky="w", padx=6, pady=4)

        tk.Label(master, text="Наименование *:").grid(
            row=1, column=0, sticky="e", padx=6, pady=4
        )
        self.ent_name = ttk.Entry(master, textvariable=self.var_name, width=44)
        self.ent_name.grid(row=1, column=1, sticky="ew", padx=6, pady=4)

        tk.Label(master, text="Ед. изм.:").grid(
            row=2, column=0, sticky="e", padx=6, pady=4
        )
        self.cmb_uom = ttk.Combobox(
            master,
            state="readonly",
            width=42,
            values=["—"] + [f"{u['code']} — {u['name']}" for u in self.uoms],
        )
        self.cmb_uom.grid(row=2, column=1, sticky="w", padx=6, pady=4)

        tk.Label(master, text="Объём по умолчанию:").grid(
            row=3, column=0, sticky="e", padx=6, pady=4
        )
        self.ent_qty = ttk.Entry(master, textvariable=self.var_qty, width=18)
        self.ent_qty.grid(row=3, column=1, sticky="w", padx=6, pady=4)

        tk.Label(master, text="Родительская задача:").grid(
            row=4, column=0, sticky="e", padx=6, pady=4
        )
        self.cmb_parent = ttk.Combobox(master, state="readonly", width=42)
        parent_values = ["— Нет —"] + [p["name"] for p in self.parents]
        self.cmb_parent["values"] = parent_values
        self.cmb_parent.grid(row=4, column=1, sticky="w", padx=6, pady=4)

        tk.Label(master, text="Порядок:").grid(
            row=5, column=0, sticky="e", padx=6, pady=4
        )
        self.ent_sort = ttk.Entry(master, textvariable=self.var_sort, width=10)
        self.ent_sort.grid(row=5, column=1, sticky="w", padx=6, pady=4)

        ttk.Checkbutton(master, text="Веха", variable=self.var_milestone).grid(
            row=6, column=1, sticky="w", padx=6, pady=6
        )

        wt_id = self.init.get("work_type_id")
        if wt_id:
            for i, w in enumerate(self.work_types):
                if int(w["id"]) == int(wt_id):
                    self.cmb_wt.current(i)
                    break
        elif self.work_types:
            self.cmb_wt.current(0)

        uom_code = self.init.get("uom_code")
        if uom_code:
            found = False
            for i, u in enumerate(self.uoms, start=1):
                if u["code"] == uom_code:
                    self.cmb_uom.current(i)
                    found = True
                    break
            if not found:
                self.cmb_uom.current(0)
        else:
            self.cmb_uom.current(0)

        parent_id = self.init.get("parent_id")
        if parent_id:
            found = False
            for i, p in enumerate(self.parents, start=1):
                if int(p["id"]) == int(parent_id):
                    self.cmb_parent.current(i)
                    found = True
                    break
            if not found:
                self.cmb_parent.current(0)
        else:
            self.cmb_parent.current(0)

        return self.ent_name

    def validate(self):
        wi = self.cmb_wt.current()
        if wi < 0:
            messagebox.showwarning("Шаблон", "Выберите тип работ.", parent=self)
            return False

        name = self.var_name.get().strip()
        if not name:
            messagebox.showwarning("Шаблон", "Введите наименование задачи.", parent=self)
            return False

        qty = _safe_float(self.var_qty.get())
        if self.var_qty.get().strip() and qty is None:
            messagebox.showwarning("Шаблон", "Объём должен быть числом.", parent=self)
            return False

        try:
            sort_order = int(self.var_sort.get().strip() or "0")
        except ValueError:
            messagebox.showwarning("Шаблон", "Порядок должен быть целым числом.", parent=self)
            return False

        uom_code = None
        ui = self.cmb_uom.current()
        if ui > 0:
            uom_code = self.uoms[ui - 1]["code"]

        parent_id = None
        pi = self.cmb_parent.current()
        if pi > 0:
            parent_id = self.parents[pi - 1]["id"]

        self.result = {
            "work_type_id": self.work_types[wi]["id"],
            "name": name,
            "uom_code": uom_code,
            "default_qty": qty,
            "is_milestone": bool(self.var_milestone.get()),
            "parent_id": parent_id,
            "sort_order": sort_order,
        }
        return True


# ═══════════════════════════════════════════════════════════════
#  PAGE
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
        hdr = tk.Frame(self, bg=C["accent"], pady=6)
        hdr.pack(fill="x")
        tk.Label(
            hdr,
            text="⚙  Справочники ГПР",
            font=("Segoe UI", 12, "bold"),
            bg=C["accent"],
            fg="white",
            padx=12,
        ).pack(side="left")

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
        self.wt_tree = ttk.Treeview(
            parent, columns=cols, show="headings",
            selectmode="browse", height=18
        )
        for c, t, w, a in [
            ("id", "ID", 50, "center"),
            ("code", "Код", 80, "w"),
            ("name", "Наименование", 300, "w"),
            ("sort_order", "Сортировка", 80, "center"),
            ("is_active", "Активен", 70, "center"),
        ]:
            self.wt_tree.heading(c, text=t)
            self.wt_tree.column(c, width=w, anchor=a)

        vsb = ttk.Scrollbar(parent, orient="vertical", command=self.wt_tree.yview)
        self.wt_tree.configure(yscrollcommand=vsb.set)
        self.wt_tree.pack(side="left", fill="both", expand=True, padx=(8, 0), pady=(0, 8))
        vsb.pack(side="right", fill="y", padx=(0, 8), pady=(0, 8))

        self.wt_tree.tag_configure("inactive", foreground="#bbb")
        self.wt_tree.bind("<Double-1>", lambda _e: self._wt_edit())

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
                    SELECT id, COALESCE(code,'') AS code, name, sort_order, is_active
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
            self.wt_tree.insert(
                "", "end",
                values=(w["id"], w["code"], w["name"], w["sort_order"], active),
                tags=tags
            )

    def _wt_sel(self) -> Optional[Dict]:
        sel = self.wt_tree.selection()
        if not sel:
            return None
        idx = self.wt_tree.index(sel[0])
        return self._wt_data[idx] if 0 <= idx < len(self._wt_data) else None

    def _wt_add(self):
        fields = [
            ("Код", "code", 12),
            ("Наименование", "name", 40),
            ("Порядок сортировки", "sort_order", 8),
        ]
        dlg = _DictEditDialog(self, "Новый тип работ", fields, init={"sort_order": "100"})
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
            ("Код", "code", 12),
            ("Наименование", "name", 40),
            ("Порядок сортировки", "sort_order", 8),
        ]
        dlg = _DictEditDialog(
            self,
            "Редактирование типа работ",
            fields,
            init={
                "code": w["code"],
                "name": w["name"],
                "sort_order": str(w["sort_order"]),
            }
        )
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
                """, (r["code"] or None, r["name"], int(r["sort_order"] or 0), w["id"]))
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
                    (w["id"],)
                )
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
        self.uom_tree = ttk.Treeview(
            parent, columns=cols, show="headings",
            selectmode="browse", height=18
        )
        self.uom_tree.heading("code", text="Код")
        self.uom_tree.heading("name", text="Наименование")
        self.uom_tree.column("code", width=100, anchor="w")
        self.uom_tree.column("name", width=300, anchor="w")

        vsb = ttk.Scrollbar(parent, orient="vertical", command=self.uom_tree.yview)
        self.uom_tree.configure(yscrollcommand=vsb.set)
        self.uom_tree.pack(side="left", fill="both", expand=True, padx=(8, 0), pady=(0, 8))
        vsb.pack(side="right", fill="y", padx=(0, 8), pady=(0, 8))
        self.uom_tree.bind("<Double-1>", lambda _e: self._uom_edit())

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
                    (r["code"], r["name"])
                )
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
        fields = [("Код", "code", 12), ("Наименование", "name", 30)]
        dlg = _DictEditDialog(
            self,
            "Редактирование ед. изм.",
            fields,
            init={"code": u["code"], "name": u["name"]},
            readonly_keys=["code"],
        )
        if not dlg.result:
            return
        r = dlg.result
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.gpr_uom SET name=%s WHERE code=%s",
                    (r["name"], u["code"])
                )
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
        if not messagebox.askyesno(
            "Справочники",
            f"Удалить единицу '{u['code']}'?\n(Не получится, если она используется в задачах)",
            parent=self
        ):
            return
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("DELETE FROM public.gpr_uom WHERE code=%s", (u["code"],))
        except Exception as e:
            messagebox.showerror(
                "Справочники",
                f"Ошибка (возможно, используется):\n{e}",
                parent=self
            )
            return
        finally:
            _release(conn)
        self._uom_load()

    # ══════════════════════════════════════════════════════
    #  ШАБЛОНЫ + ЗАДАЧИ ШАБЛОНОВ (professional)
    # ══════════════════════════════════════════════════════
    def _build_tpl_tab(self, parent):
        self._tpl_data: List[Dict] = []
        self._tt_data: List[Dict] = []
        self._wt_cache: List[Dict] = []
        self._uom_cache: List[Dict] = []

        search_bar = tk.Frame(parent, bg=C["panel"])
        search_bar.pack(fill="x", padx=8, pady=(8, 4))

        tk.Label(search_bar, text="Поиск шаблона:", bg=C["panel"]).pack(side="left")
        self.var_tpl_search = tk.StringVar()
        ent_search = ttk.Entry(search_bar, textvariable=self.var_tpl_search, width=32)
        ent_search.pack(side="left", padx=(6, 8))
        ent_search.bind("<KeyRelease>", lambda _e: self._tpl_load())

        ttk.Button(search_bar, text="🔃 Обновить", command=self._tpl_load).pack(side="left", padx=2)

        pw = tk.PanedWindow(parent, orient="horizontal", sashrelief="raised", bg=C["bg"])
        pw.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        left = tk.Frame(pw, bg=C["panel"])
        right = tk.Frame(pw, bg=C["panel"])
        pw.add(left, minsize=320)
        pw.add(right, minsize=560)

        # Левая панель
        top_left = tk.LabelFrame(left, text=" Шаблоны ", bg=C["panel"], padx=8, pady=6)
        top_left.pack(fill="both", expand=True)

        bar = tk.Frame(top_left, bg=C["panel"])
        bar.pack(fill="x")
        ttk.Button(bar, text="➕ Добавить", command=self._tpl_add).pack(side="left", padx=2)
        ttk.Button(bar, text="✏️ Редактировать", command=self._tpl_rename).pack(side="left", padx=2)
        ttk.Button(bar, text="📄 Дублировать", command=self._tpl_duplicate).pack(side="left", padx=2)
        ttk.Button(bar, text="🔄 Вкл/Выкл", command=self._tpl_toggle).pack(side="left", padx=2)
        ttk.Button(bar, text="🗑 Удалить", command=self._tpl_delete).pack(side="left", padx=2)

        cols_t = ("name", "category", "tasks", "active")
        self.tpl_tree = ttk.Treeview(
            top_left, columns=cols_t, show="headings",
            selectmode="browse", height=16
        )
        self.tpl_tree.heading("name", text="Наименование")
        self.tpl_tree.heading("category", text="Категория")
        self.tpl_tree.heading("tasks", text="Задач")
        self.tpl_tree.heading("active", text="Статус")
        self.tpl_tree.column("name", width=180, anchor="w")
        self.tpl_tree.column("category", width=100, anchor="w")
        self.tpl_tree.column("tasks", width=60, anchor="center")
        self.tpl_tree.column("active", width=70, anchor="center")

        vsb_t = ttk.Scrollbar(top_left, orient="vertical", command=self.tpl_tree.yview)
        self.tpl_tree.configure(yscrollcommand=vsb_t.set)
        self.tpl_tree.pack(side="left", fill="both", expand=True, pady=(6, 0))
        vsb_t.pack(side="right", fill="y", pady=(6, 0))

        self.tpl_tree.tag_configure("inactive", foreground="#bbb")
        self.tpl_tree.bind("<<TreeviewSelect>>", lambda _e: self._tt_load())
        self.tpl_tree.bind("<Double-1>", lambda _e: self._tpl_rename())

        # Правая панель
        info = tk.LabelFrame(right, text=" Карточка шаблона ", bg=C["panel"], padx=10, pady=8)
        info.pack(fill="x", pady=(0, 6))

        self.lbl_tpl_info = tk.Label(
            info,
            text="Выберите шаблон",
            bg=C["panel"],
            fg=C["text2"],
            justify="left",
            anchor="w",
        )
        self.lbl_tpl_info.pack(fill="x")

        tasks_box = tk.LabelFrame(right, text=" Состав шаблона ", bg=C["panel"], padx=8, pady=6)
        tasks_box.pack(fill="both", expand=True)

        bar2 = tk.Frame(tasks_box, bg=C["panel"])
        bar2.pack(fill="x")
        ttk.Button(bar2, text="➕ Добавить задачу", command=self._tt_add).pack(side="left", padx=2)
        ttk.Button(bar2, text="✏️ Редактировать", command=self._tt_edit).pack(side="left", padx=2)
        ttk.Button(bar2, text="📄 Дублировать", command=self._tt_duplicate).pack(side="left", padx=2)
        ttk.Button(bar2, text="⬆ Вверх", command=lambda: self._tt_move(-1)).pack(side="left", padx=2)
        ttk.Button(bar2, text="⬇ Вниз", command=lambda: self._tt_move(1)).pack(side="left", padx=2)
        ttk.Button(bar2, text="↻ Пересчитать порядок", command=self._tt_reindex).pack(side="left", padx=2)
        ttk.Button(bar2, text="🗑 Удалить", command=self._tt_del).pack(side="left", padx=2)

        cols_tt = ("sort", "work_type", "name", "uom", "qty", "milestone")
        self.tt_tree = ttk.Treeview(
            tasks_box, columns=cols_tt, show="tree headings",
            selectmode="browse", height=18
        )
        self.tt_tree.heading("#0", text="Иерархия")
        self.tt_tree.column("#0", width=220, anchor="w")

        for c, t, w, a in [
            ("sort", "№", 50, "center"),
            ("work_type", "Тип работ", 160, "w"),
            ("name", "Название", 220, "w"),
            ("uom", "Ед.", 60, "center"),
            ("qty", "Объём", 80, "e"),
            ("milestone", "Веха", 55, "center"),
        ]:
            self.tt_tree.heading(c, text=t)
            self.tt_tree.column(c, width=w, anchor=a)

        vsb_tt = ttk.Scrollbar(tasks_box, orient="vertical", command=self.tt_tree.yview)
        self.tt_tree.configure(yscrollcommand=vsb_tt.set)
        self.tt_tree.pack(side="left", fill="both", expand=True, pady=(6, 0))
        vsb_tt.pack(side="right", fill="y", pady=(6, 0))

        self.tt_tree.bind("<Double-1>", lambda _e: self._tt_edit())

        self._tpl_load()

    def _tpl_load(self):
        sel_id = None
        sel = self.tpl_tree.selection() if hasattr(self, "tpl_tree") else ()
        if sel:
            cur_tpl = self._tpl_sel()
            sel_id = cur_tpl["id"] if cur_tpl else None

        self.tpl_tree.delete(*self.tpl_tree.get_children())
        self._tpl_data = []

        try:
            self._tpl_data = GprTemplateService.load_templates_full(
                self.var_tpl_search.get() if hasattr(self, "var_tpl_search") else ""
            )
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка загрузки шаблонов:\n{e}", parent=self)
            return

        selected_iid = None
        for i, t in enumerate(self._tpl_data):
            active = "Да" if t.get("is_active") else "Нет"
            tags = () if t.get("is_active") else ("inactive",)
            iid = self.tpl_tree.insert(
                "", "end",
                values=(
                    t["name"],
                    t.get("category") or "",
                    t.get("task_count", 0),
                    active,
                ),
                tags=tags
            )
            if sel_id and int(t["id"]) == int(sel_id):
                selected_iid = iid

        if selected_iid:
            self.tpl_tree.selection_set(selected_iid)
            self.tpl_tree.focus(selected_iid)
            self._tt_load()
        else:
            self._tt_clear_view()

    def _tpl_sel(self) -> Optional[Dict]:
        sel = self.tpl_tree.selection()
        if not sel:
            return None
        idx = self.tpl_tree.index(sel[0])
        return self._tpl_data[idx] if 0 <= idx < len(self._tpl_data) else None

    def _tpl_add(self):
        dlg = _TemplateEditDialog(self)
        if not dlg.result:
            return
        try:
            GprTemplateService.create_template(
                dlg.result["name"],
                category=dlg.result.get("category"),
                description=dlg.result.get("description"),
                note=dlg.result.get("note"),
                created_by=_user_id(self.app_ref),
            )
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка создания шаблона:\n{e}", parent=self)
            return
        self._tpl_load()

    def _tpl_rename(self):
        t = self._tpl_sel()
        if not t:
            messagebox.showinfo("Шаблоны", "Выберите шаблон.", parent=self)
            return

        info = (
            f"Создал: {t.get('creator_name') or '—'}\n"
            f"Обновил: {t.get('updater_name') or '—'}\n"
            f"Задач: {t.get('task_count', 0)}\n"
            f"Создан: {_fmt_dt(t.get('created_at'))}\n"
            f"Обновлён: {_fmt_dt(t.get('updated_at'))}"
        )
        dlg = _TemplateEditDialog(
            self,
            init={
                "name": t["name"],
                "category": t.get("category") or "",
                "description": t.get("description") or "",
                "note": t.get("note") or "",
                "_info": info,
            }
        )
        if not dlg.result:
            return
        try:
            GprTemplateService.update_template(
                t["id"],
                dlg.result["name"],
                category=dlg.result.get("category"),
                description=dlg.result.get("description"),
                note=dlg.result.get("note"),
                updated_by=_user_id(self.app_ref),
            )
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка изменения шаблона:\n{e}", parent=self)
            return
        self._tpl_load()

    def _tpl_toggle(self):
        t = self._tpl_sel()
        if not t:
            messagebox.showinfo("Шаблоны", "Выберите шаблон.", parent=self)
            return
        try:
            GprTemplateService.toggle_template(t["id"], updated_by=_user_id(self.app_ref))
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка изменения статуса:\n{e}", parent=self)
            return
        self._tpl_load()

    def _tpl_delete(self):
        t = self._tpl_sel()
        if not t:
            messagebox.showinfo("Шаблоны", "Выберите шаблон.", parent=self)
            return
        if not messagebox.askyesno(
            "Шаблоны",
            f"Удалить шаблон «{t['name']}»?\n\nБудут удалены и все задачи шаблона.",
            parent=self
        ):
            return
        try:
            GprTemplateService.delete_template(t["id"])
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка удаления шаблона:\n{e}", parent=self)
            return
        self._tpl_load()

    def _tpl_duplicate(self):
        t = self._tpl_sel()
        if not t:
            messagebox.showinfo("Шаблоны", "Выберите шаблон.", parent=self)
            return

        new_name = simpledialog.askstring(
            "Дублирование шаблона",
            "Новое наименование шаблона:",
            initialvalue=f"{t['name']} (копия)",
            parent=self
        )
        if not new_name or not new_name.strip():
            return

        try:
            GprTemplateService.duplicate_template(
                t["id"],
                new_name.strip(),
                created_by=_user_id(self.app_ref),
            )
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка копирования шаблона:\n{e}", parent=self)
            return
        self._tpl_load()

    def _tt_clear_view(self):
        self.tt_tree.delete(*self.tt_tree.get_children())
        self._tt_data = []
        if hasattr(self, "lbl_tpl_info"):
            self.lbl_tpl_info.config(text="Выберите шаблон")

    def _load_wt_uom_cache(self):
        conn = None
        try:
            conn = _conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, name
                    FROM public.gpr_work_types
                    WHERE is_active = true
                    ORDER BY sort_order, name
                """)
                self._wt_cache = [dict(r) for r in cur.fetchall()]

                cur.execute("SELECT code, name FROM public.gpr_uom ORDER BY code")
                self._uom_cache = [dict(r) for r in cur.fetchall()]
        finally:
            _release(conn)

    def _tt_load(self):
        self.tt_tree.delete(*self.tt_tree.get_children())
        self._tt_data = []

        tpl = self._tpl_sel()
        if not tpl:
            self._tt_clear_view()
            return

        try:
            self._tt_data = GprTemplateService.load_template_tasks(tpl["id"])
            self._load_wt_uom_cache()
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка загрузки задач шаблона:\n{e}", parent=self)
            return

        self.lbl_tpl_info.config(
            text=(
                f"Шаблон: {tpl['name']}\n"
                f"Категория: {tpl.get('category') or '—'}\n"
                f"Статус: {'Активен' if tpl.get('is_active') else 'Отключён'}\n"
                f"Задач: {len(self._tt_data)}\n"
                f"Создал: {tpl.get('creator_name') or '—'}\n"
                f"Обновил: {tpl.get('updater_name') or '—'}\n"
                f"Обновлён: {_fmt_dt(tpl.get('updated_at'))}\n"
                f"Описание: {tpl.get('description') or '—'}\n"
                f"Примечание: {tpl.get('note') or '—'}"
            )
        )

        children: Dict[Optional[int], List[Dict]] = {}
        for row in self._tt_data:
            children.setdefault(row.get("parent_id"), []).append(row)

        for key in children:
            children[key].sort(key=lambda x: (x.get("sort_order", 0), x.get("id", 0)))

        def add_node(node: Dict, parent_iid: str = ""):
            ms = "✓" if node.get("is_milestone") else ""
            iid = str(node["id"])
            self.tt_tree.insert(
                parent_iid,
                "end",
                iid=iid,
                text=node["name"],
                values=(
                    node.get("sort_order", 0),
                    node.get("wt_name", ""),
                    node.get("name", ""),
                    node.get("uom_code") or "",
                    _fmt_qty(node.get("default_qty")),
                    ms,
                )
            )
            for child in children.get(node["id"], []):
                add_node(child, iid)

        roots = children.get(None, []) + children.get(0, [])
        used_root_ids = {id(x) for x in roots}
        extra_roots = [x for x in self._tt_data if x.get("parent_id") not in children and id(x) not in used_root_ids]

        for root in roots + extra_roots:
            add_node(root, "")

    def _tt_sel(self) -> Optional[Dict]:
        sel = self.tt_tree.selection()
        if not sel:
            return None
        iid = sel[0]
        for x in self._tt_data:
            if str(x["id"]) == str(iid):
                return x
        return None

    def _tt_add(self):
        tpl = self._tpl_sel()
        if not tpl:
            messagebox.showinfo("Шаблоны", "Выберите шаблон.", parent=self)
            return

        if not self._wt_cache or not self._uom_cache:
            self._load_wt_uom_cache()

        dlg = _TemplateTaskDialog(
            self,
            work_types=self._wt_cache,
            uoms=self._uom_cache,
            parents=self._tt_data,
            init={"sort_order": len(self._tt_data) * 10 + 10},
        )
        if not dlg.result:
            return

        r = dlg.result
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.gpr_template_tasks
                        (template_id, work_type_id, parent_id, name, uom_code,
                         default_qty, is_milestone, sort_order)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    tpl["id"],
                    r["work_type_id"],
                    r.get("parent_id"),
                    r["name"],
                    r.get("uom_code"),
                    r.get("default_qty"),
                    r.get("is_milestone", False),
                    r.get("sort_order", len(self._tt_data) * 10 + 10),
                ))
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка добавления задачи:\n{e}", parent=self)
            return
        finally:
            _release(conn)

        self._tt_load()

    def _tt_edit(self):
        tt = self._tt_sel()
        if not tt:
            messagebox.showinfo("Шаблоны", "Выберите задачу шаблона.", parent=self)
            return

        if not self._wt_cache or not self._uom_cache:
            self._load_wt_uom_cache()

        parents = [p for p in self._tt_data if p["id"] != tt["id"]]

        dlg = _TemplateTaskDialog(
            self,
            work_types=self._wt_cache,
            uoms=self._uom_cache,
            parents=parents,
            init={
                "work_type_id": tt["work_type_id"],
                "name": tt["name"],
                "uom_code": tt.get("uom_code"),
                "default_qty": tt.get("default_qty"),
                "is_milestone": tt.get("is_milestone", False),
                "parent_id": tt.get("parent_id"),
                "sort_order": tt.get("sort_order", 0),
            },
        )
        if not dlg.result:
            return

        r = dlg.result
        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.gpr_template_tasks
                    SET work_type_id=%s,
                        parent_id=%s,
                        name=%s,
                        uom_code=%s,
                        default_qty=%s,
                        is_milestone=%s,
                        sort_order=%s
                    WHERE id=%s
                """, (
                    r["work_type_id"],
                    r.get("parent_id"),
                    r["name"],
                    r.get("uom_code"),
                    r.get("default_qty"),
                    r.get("is_milestone", False),
                    r.get("sort_order", 0),
                    tt["id"],
                ))
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка редактирования задачи:\n{e}", parent=self)
            return
        finally:
            _release(conn)

        self._tt_load()

    def _tt_duplicate(self):
        tt = self._tt_sel()
        tpl = self._tpl_sel()
        if not tt or not tpl:
            messagebox.showinfo("Шаблоны", "Выберите задачу шаблона.", parent=self)
            return

        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.gpr_template_tasks
                        (template_id, work_type_id, parent_id, name, uom_code,
                         default_qty, is_milestone, sort_order)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    tpl["id"],
                    tt["work_type_id"],
                    tt.get("parent_id"),
                    f"{tt['name']} (копия)",
                    tt.get("uom_code"),
                    tt.get("default_qty"),
                    tt.get("is_milestone", False),
                    int(tt.get("sort_order", 0)) + 1,
                ))
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка копирования задачи:\n{e}", parent=self)
            return
        finally:
            _release(conn)

        self._tt_load()

    def _tt_move(self, direction: int):
        tt = self._tt_sel()
        if not tt:
            return

        siblings = [x for x in self._tt_data if x.get("parent_id") == tt.get("parent_id")]
        siblings.sort(key=lambda x: (x.get("sort_order", 0), x.get("id", 0)))

        idx = next((i for i, x in enumerate(siblings) if x["id"] == tt["id"]), None)
        if idx is None:
            return

        new_idx = idx + direction
        if new_idx < 0 or new_idx >= len(siblings):
            return

        other = siblings[new_idx]

        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.gpr_template_tasks SET sort_order=%s WHERE id=%s",
                    (other.get("sort_order", 0), tt["id"])
                )
                cur.execute(
                    "UPDATE public.gpr_template_tasks SET sort_order=%s WHERE id=%s",
                    (tt.get("sort_order", 0), other["id"])
                )
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка перемещения:\n{e}", parent=self)
            return
        finally:
            _release(conn)

        self._tt_load()

    def _tt_reindex(self):
        tpl = self._tpl_sel()
        if not tpl:
            return

        ordered = sorted(self._tt_data, key=lambda x: (x.get("sort_order", 0), x.get("id", 0)))

        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                for i, row in enumerate(ordered, start=1):
                    cur.execute("""
                        UPDATE public.gpr_template_tasks
                        SET sort_order=%s
                        WHERE id=%s
                    """, (i * 10, row["id"]))
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка пересчёта порядка:\n{e}", parent=self)
            return
        finally:
            _release(conn)

        self._tt_load()

    def _tt_del(self):
        tt = self._tt_sel()
        if not tt:
            return
        if not messagebox.askyesno(
            "Шаблоны",
            f"Удалить задачу «{tt['name']}»?\n\nЕсли у неё есть дочерние задачи, у них parent_id станет NULL.",
            parent=self
        ):
            return

        conn = None
        try:
            conn = _conn()
            with conn, conn.cursor() as cur:
                cur.execute("DELETE FROM public.gpr_template_tasks WHERE id=%s", (tt["id"],))
        except Exception as e:
            messagebox.showerror("Шаблоны", f"Ошибка удаления задачи:\n{e}", parent=self)
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
