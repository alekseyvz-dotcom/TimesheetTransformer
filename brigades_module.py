import tkinter as tk
from tkinter import ttk, messagebox
from typing import Optional, Any, Dict, List, Tuple
import logging

try:
    import psycopg2
except Exception:
    psycopg2 = None

# ============================================================
#  Цветовая схема (единая с остальными модулями)
# ============================================================
BR_COLORS = {
    "bg":           "#f0f2f5",
    "panel":        "#ffffff",
    "accent":       "#1565c0",
    "accent_light": "#e3f2fd",
    "success":      "#2e7d32",
    "warning":      "#b00020",
    "border":       "#dde1e7",
    "btn_save_bg":  "#1565c0",
    "btn_save_fg":  "#ffffff",
    "row_even":     "#ffffff",
    "row_odd":      "#f8f9fb",
    "row_brig":     "#e8f5e9",   # зелёный фон строк с бригадиром
}

_db_pool = None


def set_db_pool(pool_obj):
    global _db_pool
    _db_pool = pool_obj


def _db_get_conn():
    if not _db_pool:
        raise RuntimeError("Пул соединений не инициализирован (brigades_module).")
    return _db_pool.getconn()


def _db_put_conn(conn):
    try:
        if _db_pool and conn:
            _db_pool.putconn(conn)
    except Exception:
        pass


# ============================================================
#  DB API  (без изменений)
# ============================================================

def db_load_allowed_departments_for_user(user_id: int) -> List[str]:
    conn = None
    try:
        conn = _db_get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT th.department
                FROM public.timesheet_headers th
                WHERE th.user_id = %s
                  AND COALESCE(th.department, '') <> ''
                ORDER BY th.department
                """,
                (user_id,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        _db_put_conn(conn)


def db_load_employees_for_department(department_name: str) -> List[Dict[str, str]]:
    conn = None
    try:
        conn = _db_get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.fio, e.tbn
                FROM public.employees e
                JOIN public.departments d ON d.id = e.department_id
                WHERE d.name = %s
                  AND e.is_fired = false
                  AND COALESCE(e.tbn,'') <> ''
                ORDER BY e.fio
                """,
                (department_name,),
            )
            return [{"fio": fio, "tbn": tbn} for (fio, tbn) in cur.fetchall()]
    finally:
        _db_put_conn(conn)


def db_load_brigadier_map(department_name: str) -> Dict[str, Optional[str]]:
    conn = None
    try:
        conn = _db_get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT employee_tbn, brigadier_tbn
                FROM public.employee_brigadiers
                WHERE department = %s
                """,
                (department_name,),
            )
            return {emp_tbn: br_tbn for (emp_tbn, br_tbn) in cur.fetchall()}
    finally:
        _db_put_conn(conn)


def db_upsert_assignments(
    department_name: str,
    assignments: List[Tuple[str, Optional[str]]],
    created_by: Optional[int] = None,
) -> None:
    conn = None
    try:
        conn = _db_get_conn()
        with conn:
            with conn.cursor() as cur:
                for employee_tbn, brigadier_tbn in assignments:
                    cur.execute(
                        """
                        INSERT INTO public.employee_brigadiers
                            (department, employee_tbn, brigadier_tbn,
                             created_by, updated_at)
                        VALUES (%s, %s, %s, %s, now())
                        ON CONFLICT (department, employee_tbn)
                        DO UPDATE SET brigadier_tbn = EXCLUDED.brigadier_tbn,
                                      created_by    = EXCLUDED.created_by,
                                      updated_at    = now()
                        """,
                        (department_name, employee_tbn,
                         brigadier_tbn, created_by),
                    )
    finally:
        _db_put_conn(conn)


# ============================================================
#  Диалог выбора бригадира  (улучшенный)
# ============================================================

class _PickBrigadierDialog(tk.Toplevel):
    """
    Модальный диалог выбора бригадира из сотрудников подразделения.
    Результат: self.result = tbn (str) или None.
    """

    def __init__(self, parent, employees: List[Dict[str, str]],
                 current_brig_tbn: Optional[str] = None):
        super().__init__(parent)
        self.employees        = employees
        self.current_brig_tbn = current_brig_tbn
        self.result: Optional[str] = None

        self.title("Выбор бригадира")
        self.resizable(True, True)
        self.configure(bg=BR_COLORS["bg"])
        self.grab_set()

        self._view_items: List[Dict[str, str]] = []
        self._build()
        self._render("")

        # центрирование
        try:
            self.update_idletasks()
            px = parent.winfo_rootx()
            py = parent.winfo_rooty()
            pw = parent.winfo_width()
            ph = parent.winfo_height()
            self.geometry(
                f"560x480"
                f"+{px + (pw - 560) // 2}"
                f"+{py + (ph - 480) // 2}"
            )
        except Exception:
            self.geometry("560x480")

    def _build(self):
        # Заголовок
        hdr = tk.Frame(self, bg=BR_COLORS["accent"], pady=6)
        hdr.pack(fill="x")
        tk.Label(
            hdr, text="👤  Выбор бригадира",
            font=("Segoe UI", 11, "bold"),
            bg=BR_COLORS["accent"], fg="white", padx=12
        ).pack(side="left")

        body = tk.Frame(self, bg=BR_COLORS["bg"], padx=12, pady=10)
        body.pack(fill="both", expand=True)

        # Поиск
        srch = tk.Frame(body, bg=BR_COLORS["bg"])
        srch.pack(fill="x", pady=(0, 8))
        tk.Label(
            srch, text="🔍 Поиск (ФИО / таб. №):",
            font=("Segoe UI", 9), bg=BR_COLORS["bg"]
        ).pack(side="left")
        self._q_var = tk.StringVar()
        ent = ttk.Entry(srch, textvariable=self._q_var, width=36,
                        font=("Segoe UI", 9))
        ent.pack(side="left", padx=(6, 0))
        ent.bind("<KeyRelease>", lambda e: self._render(self._q_var.get()))
        ent.focus_set()

        # Treeview вместо Listbox — выглядит аккуратнее
        tbl_f = tk.Frame(body, bg=BR_COLORS["panel"],
                         relief="groove", bd=1)
        tbl_f.pack(fill="both", expand=True)

        cols = ("fio", "tbn", "mark")
        self._tree = ttk.Treeview(
            tbl_f, columns=cols, show="headings",
            selectmode="browse", height=14
        )
        self._tree.heading("fio",  text="ФИО сотрудника")
        self._tree.heading("tbn",  text="Таб. №")
        self._tree.heading("mark", text="")

        self._tree.column("fio",  width=340, anchor="w")
        self._tree.column("tbn",  width=100, anchor="center", stretch=False)
        self._tree.column("mark", width=60,  anchor="center", stretch=False)

        self._tree.tag_configure(
            "current", background="#fff9c4",
            font=("Segoe UI", 9, "bold")
        )
        self._tree.tag_configure("normal", font=("Segoe UI", 9))

        vsb = ttk.Scrollbar(tbl_f, orient="vertical",
                            command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)
        self._tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self._tree.bind("<Double-1>", lambda e: self._ok())
        self._tree.bind("<Return>",   lambda e: self._ok())

        # Кнопки
        btn_bar = tk.Frame(self, bg=BR_COLORS["bg"], pady=8)
        btn_bar.pack(fill="x", padx=12)

        tk.Button(
            btn_bar, text="✔  Выбрать",
            font=("Segoe UI", 9, "bold"),
            bg=BR_COLORS["btn_save_bg"], fg=BR_COLORS["btn_save_fg"],
            activebackground="#0d47a1", activeforeground="white",
            relief="flat", cursor="hand2", padx=14, pady=4,
            command=self._ok
        ).pack(side="right", padx=(6, 0))

        ttk.Button(
            btn_bar, text="Отмена",
            command=self._cancel
        ).pack(side="right")

    def _render(self, q: str):
        ft = (q or "").strip().lower()
        self._tree.delete(*self._tree.get_children())
        self._view_items = []

        for emp in self.employees:
            fio = emp["fio"]
            tbn = emp["tbn"]
            if ft and ft not in fio.lower() and ft not in tbn.lower():
                continue

            is_cur = (tbn == self.current_brig_tbn)
            mark   = "◀ текущий" if is_cur else ""
            tag    = "current" if is_cur else "normal"

            iid = f"b_{tbn}"
            self._tree.insert(
                "", "end", iid=iid,
                values=(fio, tbn, mark),
                tags=(tag,)
            )
            self._view_items.append(emp)

            # Прокручиваем к текущему бригадиру
            if is_cur:
                self._tree.see(iid)
                self._tree.selection_set(iid)

    def _ok(self, _=None):
        sel = self._tree.selection()
        if not sel:
            messagebox.showwarning(
                "Выбор бригадира", "Не выбрана строка.", parent=self
            )
            return
        iid = sel[0]
        tbn = iid.replace("b_", "", 1)
        self.result = tbn
        self.destroy()

    def _cancel(self):
        self.result = None
        self.destroy()


# ============================================================
#  Основная страница
# ============================================================

class BrigadesPage(tk.Frame):
    """
    Страница «Бригады» в едином стиле с TabSheet / MealOrder.
    """

    def __init__(self, master, app_ref):
        super().__init__(master, bg=BR_COLORS["bg"])
        self.app_ref = app_ref

        self.allowed_departments: List[str] = []
        self.employees:  List[Dict[str, str]]      = []
        self.emp_by_tbn: Dict[str, Dict[str, str]] = {}
        self.brig_map:   Dict[str, Optional[str]]  = {}

        # Кэш: tbn бригадира -> его ФИО (для отображения)
        self._brig_fio_cache: Dict[str, str] = {}

        self._build_ui()
        self._load_departments()

    # ── helpers ──────────────────────────────────────────────

    def _user_id(self) -> Optional[int]:
        try:
            return (self.app_ref.current_user or {}).get("id")
        except Exception:
            return None

    def _brig_display(self, emp_tbn: str) -> str:
        """Возвращает строку 'ФИО (tbn)' или просто 'tbn' для бригадира."""
        br_tbn = self.brig_map.get(emp_tbn)
        if not br_tbn:
            return ""
        fio = self._brig_fio_cache.get(br_tbn, "")
        return f"{fio}  ({br_tbn})" if fio else br_tbn

    def _rebuild_fio_cache(self):
        self._brig_fio_cache = {
            e["tbn"]: e["fio"] for e in self.employees
        }

    # ── UI ───────────────────────────────────────────────────

    def _build_ui(self):
        # ── Заголовок ─────────────────────────────────────────
        hdr = tk.Frame(self, bg=BR_COLORS["accent"], pady=6)
        hdr.pack(fill="x")
        tk.Label(
            hdr, text="👥  Управление бригадами",
            font=("Segoe UI", 12, "bold"),
            bg=BR_COLORS["accent"], fg="white", padx=12
        ).pack(side="left")

        # ── Панель настроек (подразделение) ───────────────────
        cfg_pnl = tk.LabelFrame(
            self, text=" ⚙️ Подразделение ",
            font=("Segoe UI", 9, "bold"),
            bg=BR_COLORS["panel"], fg=BR_COLORS["accent"],
            relief="groove", bd=1, padx=10, pady=8
        )
        cfg_pnl.pack(fill="x", padx=10, pady=(8, 4))

        tk.Label(
            cfg_pnl, text="Подразделение  *:",
            font=("Segoe UI", 9), fg=BR_COLORS["warning"],
            bg=BR_COLORS["panel"]
        ).grid(row=0, column=0, sticky="e", padx=(0, 8), pady=3)

        self.cmb_dep = ttk.Combobox(
            cfg_pnl, state="readonly", width=44, values=[]
        )
        self.cmb_dep.grid(row=0, column=1, sticky="w", pady=3)
        self.cmb_dep.bind(
            "<<ComboboxSelected>>",
            lambda e: self._load_department_data()
        )

        # Счётчик
        self.lbl_counts = tk.Label(
            cfg_pnl, text="",
            font=("Segoe UI", 9), fg=BR_COLORS["accent"],
            bg=BR_COLORS["panel"]
        )
        self.lbl_counts.grid(row=0, column=2, sticky="w", padx=(20, 0))

        # Кнопки управления — справа
        btn_cfg = tk.Frame(cfg_pnl, bg=BR_COLORS["panel"])
        btn_cfg.grid(row=0, column=3, sticky="e", padx=(20, 0))

        tk.Button(
            btn_cfg,
            text="💾  Сохранить",
            font=("Segoe UI", 9, "bold"),
            bg=BR_COLORS["btn_save_bg"], fg=BR_COLORS["btn_save_fg"],
            activebackground="#0d47a1", activeforeground="white",
            relief="flat", cursor="hand2", padx=12, pady=4,
            command=self._save
        ).pack(side="left", padx=(0, 6))

        ttk.Button(
            btn_cfg, text="🔄 Обновить",
            command=self._load_department_data
        ).pack(side="left")

        cfg_pnl.grid_columnconfigure(1, weight=1)

        # ── Тулбар действий ───────────────────────────────────
        tool = tk.Frame(self, bg=BR_COLORS["accent_light"], pady=5)
        tool.pack(fill="x", padx=10, pady=(2, 0))

        self._btn_assign = tk.Button(
            tool,
            text="👤  Назначить бригадира…",
            font=("Segoe UI", 9, "bold"),
            bg=BR_COLORS["btn_save_bg"], fg=BR_COLORS["btn_save_fg"],
            activebackground="#0d47a1", activeforeground="white",
            relief="flat", cursor="hand2", padx=10, pady=3,
            command=self._assign_selected
        )
        self._btn_assign.pack(side="left", padx=(8, 6))

        self._btn_clear = ttk.Button(
            tool, text="✖  Снять бригадира",
            command=self._clear_selected
        )
        self._btn_clear.pack(side="left", padx=(0, 6))

        # разделитель
        tk.Frame(tool, bg=BR_COLORS["border"], width=1).pack(
            side="left", fill="y", padx=8
        )

        # Быстрые действия: снять всех
        ttk.Button(
            tool, text="Снять всех бригадиров",
            command=self._clear_all
        ).pack(side="left", padx=(0, 6))

        # Поиск — справа в тулбаре
        tk.Label(
            tool, text="🔍 Поиск:",
            font=("Segoe UI", 9), bg=BR_COLORS["accent_light"]
        ).pack(side="left", padx=(16, 4))

        self.search_var = tk.StringVar()
        ent_search = ttk.Entry(
            tool, textvariable=self.search_var, width=28
        )
        ent_search.pack(side="left")

        def _on_search_key(_e=None):
            self._refresh_tree(self.search_var.get())

        ent_search.bind("<KeyRelease>", _on_search_key)

        ttk.Button(
            tool, text="×",
            width=2,
            command=lambda: (
                self.search_var.set(""),
                self._refresh_tree("")
            )
        ).pack(side="left", padx=(2, 8))

        # ── Таблица ───────────────────────────────────────────
        tbl_pnl = tk.LabelFrame(
            self, text=" 📋 Список сотрудников ",
            font=("Segoe UI", 9, "bold"),
            bg=BR_COLORS["panel"], fg=BR_COLORS["accent"],
            relief="groove", bd=1
        )
        tbl_pnl.pack(fill="both", expand=True, padx=10, pady=(4, 4))

        # Настройка стилей строк
        style = ttk.Style()
        style.configure("Brigades.Treeview", rowheight=24,
                        font=("Segoe UI", 9))
        style.configure("Brigades.Treeview.Heading",
                        font=("Segoe UI", 9, "bold"))

        cols = ("num", "fio", "tbn", "brigadier")
        self.tree = ttk.Treeview(
            tbl_pnl, columns=cols,
            show="headings", selectmode="browse",
            style="Brigades.Treeview"
        )

        heads = {
            "num":       ("№",              40,  "center"),
            "fio":       ("ФИО сотрудника", 380, "w"),
            "tbn":       ("Таб. №",         110, "center"),
            "brigadier": ("Бригадир",       300, "w"),
        }
        for col, (text, width, anchor) in heads.items():
            self.tree.heading(col, text=text)
            self.tree.column(col, width=width, anchor=anchor,
                             stretch=(col == "fio"))

        # Теги цветов строк
        self.tree.tag_configure(
            "has_brig",
            background=BR_COLORS["row_brig"],
            font=("Segoe UI", 9)
        )
        self.tree.tag_configure(
            "no_brig_even",
            background=BR_COLORS["row_even"],
            font=("Segoe UI", 9)
        )
        self.tree.tag_configure(
            "no_brig_odd",
            background=BR_COLORS["row_odd"],
            font=("Segoe UI", 9)
        )

        vsb = ttk.Scrollbar(tbl_pnl, orient="vertical",
                            command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.bind("<Double-1>", lambda e: self._assign_selected())
        self.tree.bind("<Return>",   lambda e: self._assign_selected())
        self.tree.bind("<Delete>",   lambda e: self._clear_selected())

        # ── Нижняя панель (итоги) ─────────────────────────────
        bottom = tk.Frame(self, bg=BR_COLORS["accent_light"], pady=5)
        bottom.pack(fill="x", padx=10, pady=(0, 8))

        self.lbl_total = tk.Label(
            bottom,
            text="Сотрудников: 0  |  С бригадиром: 0  |  Без бригадира: 0",
            font=("Segoe UI", 9, "bold"),
            fg=BR_COLORS["accent"],
            bg=BR_COLORS["accent_light"]
        )
        self.lbl_total.pack(side="left", padx=10)

        tk.Label(
            bottom,
            text="Двойной щелчок / Enter — назначить бригадира  |  "
                 "Delete — снять бригадира",
            font=("Segoe UI", 9, "italic"), fg="#555",
            bg=BR_COLORS["accent_light"]
        ).pack(side="right", padx=10)

    # ── Загрузка данных ───────────────────────────────────────

    def _load_departments(self):
        uid = self._user_id()
        if not uid:
            messagebox.showerror("Бригады",
                                 "Не удалось определить пользователя.")
            return

        try:
            self.allowed_departments = \
                db_load_allowed_departments_for_user(uid)
        except Exception as e:
            logging.exception("Ошибка загрузки подразделений")
            messagebox.showerror("Бригады",
                                 f"Ошибка загрузки подразделений:\n{e}")
            return

        self.cmb_dep.configure(values=self.allowed_departments)
        if self.allowed_departments:
            self.cmb_dep.set(self.allowed_departments[0])
            self._load_department_data()
        else:
            self.cmb_dep.set("")
            self._clear_tree()
            messagebox.showinfo(
                "Бригады",
                "Нет доступных подразделений.\n"
                "(У пользователя нет сохранённых табелей.)"
            )

    def _load_department_data(self):
        dep = (self.cmb_dep.get() or "").strip()
        if not dep:
            return

        try:
            self.employees  = db_load_employees_for_department(dep)
            self.emp_by_tbn = {e["tbn"]: e for e in self.employees}
            self.brig_map   = db_load_brigadier_map(dep)
        except Exception as e:
            logging.exception("Ошибка загрузки данных бригады")
            messagebox.showerror("Бригады",
                                 f"Ошибка загрузки данных:\n{e}")
            return

        self._rebuild_fio_cache()
        self.search_var.set("")
        self._refresh_tree("")

    # ── Отображение ───────────────────────────────────────────

    def _clear_tree(self):
        self.tree.delete(*self.tree.get_children())

    def _refresh_tree(self, filter_text: str = ""):
        self._clear_tree()
        ft  = filter_text.strip().lower()
        num = 0

        for e in self.employees:
            fio = e["fio"]
            tbn = e["tbn"]

            if ft and ft not in fio.lower() and ft not in tbn.lower():
                continue

            num += 1
            br_display = self._brig_display(tbn)
            has_brig   = bool(self.brig_map.get(tbn))

            if has_brig:
                tag = "has_brig"
            else:
                tag = "no_brig_even" if num % 2 == 0 else "no_brig_odd"

            self.tree.insert(
                "", "end", iid=tbn,
                values=(num, fio, tbn, br_display),
                tags=(tag,)
            )

        self._update_counts()

    def _update_counts(self):
        total    = len(self.employees)
        with_br  = sum(1 for e in self.employees
                       if self.brig_map.get(e["tbn"]))
        without  = total - with_br

        try:
            self.lbl_total.config(
                text=(
                    f"Сотрудников: {total}  |  "
                    f"С бригадиром: {with_br}  |  "
                    f"Без бригадира: {without}"
                )
            )
            self.lbl_counts.config(
                text=f"Всего: {total} чел."
            )
        except Exception:
            pass

    # ── Действия ──────────────────────────────────────────────

    def _selected_employee_tbn(self) -> Optional[str]:
        sel = self.tree.selection()
        return sel[0] if sel else None

    def _assign_selected(self):
        emp_tbn = self._selected_employee_tbn()
        if not emp_tbn:
            messagebox.showinfo("Бригады", "Выберите сотрудника в таблице.")
            return

        current_br = self.brig_map.get(emp_tbn)
        emp_fio    = self.emp_by_tbn.get(emp_tbn, {}).get("fio", emp_tbn)

        dlg = _PickBrigadierDialog(
            self,
            employees=self.employees,
            current_brig_tbn=current_br
        )
        self.wait_window(dlg)

        if dlg.result is None:
            return

        brig_tbn = dlg.result
        self.brig_map[emp_tbn] = brig_tbn

        # Обновляем строку в таблице
        br_display = self._brig_display(emp_tbn)
        try:
            self.tree.set(emp_tbn, "brigadier", br_display)
            self.tree.item(emp_tbn, tags=("has_brig",))
        except Exception:
            pass

        self._update_counts()

    def _clear_selected(self):
        emp_tbn = self._selected_employee_tbn()
        if not emp_tbn:
            return

        self.brig_map[emp_tbn] = None

        try:
            self.tree.set(emp_tbn, "brigadier", "")
            # восстанавливаем зебру
            idx = list(self.tree.get_children()).index(emp_tbn)
            tag = "no_brig_even" if idx % 2 == 0 else "no_brig_odd"
            self.tree.item(emp_tbn, tags=(tag,))
        except Exception:
            pass

        self._update_counts()

    def _clear_all(self):
        if not self.employees:
            return
        if not messagebox.askyesno(
            "Снять всех бригадиров",
            "Вы уверены, что хотите снять бригадира у всех сотрудников?\n\n"
            "Изменения вступят в силу после нажатия «Сохранить».",
            parent=self
        ):
            return

        for e in self.employees:
            self.brig_map[e["tbn"]] = None

        self._refresh_tree(self.search_var.get())

    def _save(self):
        dep = (self.cmb_dep.get() or "").strip()
        if not dep:
            messagebox.showwarning("Бригады",
                                   "Выберите подразделение.")
            return

        uid = self._user_id()

        assignments: List[Tuple[str, Optional[str]]] = [
            (e["tbn"], self.brig_map.get(e["tbn"]))
            for e in self.employees
        ]

        try:
            db_upsert_assignments(dep, assignments, created_by=uid)
            messagebox.showinfo(
                "Бригады",
                f"✅  Назначения сохранены.\n"
                f"Подразделение: {dep}\n"
                f"Записей: {len(assignments)}"
            )
        except Exception as e:
            logging.exception("Ошибка сохранения бригад")
            messagebox.showerror("Бригады",
                                 f"Ошибка сохранения:\n{e}")


# ============================================================
def create_brigades_page(parent, app_ref):
    return BrigadesPage(parent, app_ref)
