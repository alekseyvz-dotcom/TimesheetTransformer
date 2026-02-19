import tkinter as tk
from tkinter import ttk, messagebox
from typing import Optional, Any, Dict, List, Tuple

import logging

try:
    import psycopg2
except Exception:
    psycopg2 = None


_db_pool = None


def set_db_pool(pool_obj):
    global _db_pool
    _db_pool = pool_obj


def _db_get_conn():
    if not _db_pool:
        raise RuntimeError("Пул соединений с БД не инициализирован (brigades_module).")
    return _db_pool.getconn()


def _db_put_conn(conn):
    try:
        if _db_pool and conn:
            _db_pool.putconn(conn)
    except Exception:
        pass


# ----------------------- DB API -----------------------

def db_load_allowed_departments_for_user(user_id: int) -> List[str]:
    """
    Подразделения, доступные пользователю в "Бригады":
    только те, где у него есть timesheet_headers.
    """
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
    """
    Сотрудники выбранного подразделения (из справочника employees).
    Сопоставление подразделения: departments.name == department_name (как в timesheet_headers.department).
    """
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
    """
    Возвращает map: employee_tbn -> brigadier_tbn|None
    """
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
    assignments: List[Tuple[str, Optional[str]]],  # [(employee_tbn, brigadier_tbn_or_None)]
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
                            (department, employee_tbn, brigadier_tbn, created_by, updated_at)
                        VALUES (%s, %s, %s, %s, now())
                        ON CONFLICT (department, employee_tbn)
                        DO UPDATE SET brigadier_tbn = EXCLUDED.brigadier_tbn,
                                      created_by = EXCLUDED.created_by,
                                      updated_at = now()
                        """,
                        (department_name, employee_tbn, brigadier_tbn, created_by),
                    )
    finally:
        _db_put_conn(conn)


# ----------------------- UI -----------------------

class BrigadesPage(tk.Frame):
    """
    Страница "Бригады":
      - выбор подразделения (только где есть табели у пользователя)
      - список сотрудников подразделения
      - назначение бригадира (выбор из сотрудников этого же подразделения)
      - сохранение в БД
    """

    def __init__(self, master, app_ref):
        super().__init__(master, bg="#f7f7f7")
        self.app_ref = app_ref

        self.allowed_departments: List[str] = []
        self.employees: List[Dict[str, str]] = []
        self.emp_by_tbn: Dict[str, Dict[str, str]] = {}
        self.brig_map: Dict[str, Optional[str]] = {}

        self._build_ui()
        self._load_departments()

    def _user_id(self) -> Optional[int]:
        try:
            return (self.app_ref.current_user or {}).get("id")
        except Exception:
            return None

    def _build_ui(self):
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(fill="x", padx=10, pady=10)

        tk.Label(top, text="Подразделение:", bg="#f7f7f7").pack(side="left")
        self.cmb_dep = ttk.Combobox(top, state="readonly", width=60, values=[])
        self.cmb_dep.pack(side="left", padx=(8, 12))
        self.cmb_dep.bind("<<ComboboxSelected>>", lambda e: self._load_department_data())

        ttk.Button(top, text="Сохранить", command=self._save).pack(side="left")
        ttk.Button(top, text="Обновить", command=self._load_department_data).pack(side="left", padx=(8, 0))

        mid = tk.Frame(self, bg="#f7f7f7")
        mid.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        # Treeview + scroll
        columns = ("fio", "tbn", "brigadier")
        self.tree = ttk.Treeview(mid, columns=columns, show="headings", height=18)
        self.tree.heading("fio", text="Сотрудник")
        self.tree.heading("tbn", text="Таб. №")
        self.tree.heading("brigadier", text="Бригадир (таб.№)")

        self.tree.column("fio", width=420, anchor="w")
        self.tree.column("tbn", width=120, anchor="w")
        self.tree.column("brigadier", width=200, anchor="w")

        vsb = ttk.Scrollbar(mid, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        mid.grid_rowconfigure(0, weight=1)
        mid.grid_columnconfigure(0, weight=1)

        bottom = tk.Frame(self, bg="#f7f7f7")
        bottom.pack(fill="x", padx=10, pady=(0, 10))

        ttk.Button(bottom, text="Назначить бригадира…", command=self._assign_selected).pack(side="left")
        ttk.Button(bottom, text="Снять бригадира", command=self._clear_selected).pack(side="left", padx=(8, 0))

        self.lbl_hint = tk.Label(
            bottom,
            text="Назначения сохраняются глобально для подразделения.",
            bg="#f7f7f7",
            fg="#777",
        )
        self.lbl_hint.pack(side="right")

        self.tree.bind("<Double-1>", lambda e: self._assign_selected())

    def _load_departments(self):
        uid = self._user_id()
        if not uid:
            messagebox.showerror("Бригады", "Не удалось определить пользователя.")
            return

        try:
            self.allowed_departments = db_load_allowed_departments_for_user(uid)
        except Exception as e:
            logging.exception("Ошибка загрузки подразделений для Бригад")
            messagebox.showerror("Бригады", f"Ошибка загрузки подразделений:\n{e}")
            return

        self.cmb_dep.configure(values=self.allowed_departments)
        if self.allowed_departments:
            self.cmb_dep.set(self.allowed_departments[0])
            self._load_department_data()
        else:
            self.cmb_dep.set("")
            self._clear_tree()
            messagebox.showinfo("Бригады", "Нет доступных подразделений (у пользователя нет табелей).")

    def _clear_tree(self):
        for iid in self.tree.get_children():
            self.tree.delete(iid)

    def _load_department_data(self):
        dep = (self.cmb_dep.get() or "").strip()
        if not dep:
            return

        try:
            self.employees = db_load_employees_for_department(dep)
            self.emp_by_tbn = {e["tbn"]: e for e in self.employees}
            self.brig_map = db_load_brigadier_map(dep)
        except Exception as e:
            logging.exception("Ошибка загрузки данных для Бригад")
            messagebox.showerror("Бригады", f"Ошибка загрузки данных:\n{e}")
            return

        self._refresh_tree()

    def _refresh_tree(self):
        self._clear_tree()
        for e in self.employees:
            fio = e["fio"]
            tbn = e["tbn"]
            br = self.brig_map.get(tbn)
            self.tree.insert("", "end", iid=tbn, values=(fio, tbn, br or ""))

    def _selected_employee_tbn(self) -> Optional[str]:
        sel = self.tree.selection()
        if not sel:
            return None
        return sel[0]

    def _pick_brigadier_dialog(self, exclude_tbn: str) -> Optional[str]:
        """
        Выбор бригадира из сотрудников подразделения.
        Возвращает brigadier_tbn или None (отмена).
        """
        items = [e for e in self.employees if e["tbn"] != exclude_tbn]
        if not items:
            messagebox.showinfo("Бригады", "В подразделении нет других сотрудников для назначения бригадиром.")
            return None

        dlg = tk.Toplevel(self)
        dlg.title("Выбор бригадира")
        dlg.transient(self)
        dlg.grab_set()

        tk.Label(dlg, text="Выберите бригадира:", anchor="w").pack(fill="x", padx=10, pady=(10, 6))

        q_var = tk.StringVar()
        ent = ttk.Entry(dlg, textvariable=q_var)
        ent.pack(fill="x", padx=10, pady=(0, 8))
        ent.focus_set()

        lb = tk.Listbox(dlg, width=80, height=18)
        lb.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        view = {"items": items[:]}

        def render(filter_text: str):
            ft = (filter_text or "").strip().lower()
            lb.delete(0, "end")
            view_items = []
            for it in items:
                line = f'{it["fio"]}  |  {it["tbn"]}'
                if not ft or ft in it["fio"].lower() or ft in it["tbn"].lower():
                    view_items.append(it)
                    lb.insert("end", line)
            view["items"] = view_items

        def on_key(*_):
            render(q_var.get())

        ent.bind("<KeyRelease>", lambda e: on_key())

        result = {"tbn": None}

        def ok():
            sel = lb.curselection()
            if not sel:
                messagebox.showwarning("Бригады", "Не выбран бригадир.", parent=dlg)
                return
            idx = sel[0]
            result["tbn"] = view["items"][idx]["tbn"]
            dlg.destroy()

        def cancel():
            dlg.destroy()

        btns = tk.Frame(dlg)
        btns.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(btns, text="OK", command=ok).pack(side="right")
        ttk.Button(btns, text="Отмена", command=cancel).pack(side="right", padx=(0, 8))

        render("")
        dlg.wait_window()
        return result["tbn"]

    def _assign_selected(self):
        emp_tbn = self._selected_employee_tbn()
        if not emp_tbn:
            messagebox.showinfo("Бригады", "Выберите сотрудника.")
            return

        brig_tbn = self._pick_brigadier_dialog(exclude_tbn=emp_tbn)
        if brig_tbn is None:
            return

        if brig_tbn == emp_tbn:
            brig_tbn = None

        self.brig_map[emp_tbn] = brig_tbn
        self.tree.set(emp_tbn, "brigadier", brig_tbn or "")

    def _clear_selected(self):
        emp_tbn = self._selected_employee_tbn()
        if not emp_tbn:
            return
        self.brig_map[emp_tbn] = None
        self.tree.set(emp_tbn, "brigadier", "")

    def _save(self):
        dep = (self.cmb_dep.get() or "").strip()
        if not dep:
            return

        uid = self._user_id()

        # сохраняем по всем сотрудникам подразделения
        assignments: List[Tuple[str, Optional[str]]] = []
        for e in self.employees:
            tbn = e["tbn"]
            br = self.brig_map.get(tbn)
            if br == tbn:
                br = None
            assignments.append((tbn, br))

        try:
            db_upsert_assignments(dep, assignments, created_by=uid)
            messagebox.showinfo("Бригады", "Сохранено.")
        except Exception as e:
            logging.exception("Ошибка сохранения Бригад")
            messagebox.showerror("Бригады", f"Ошибка сохранения:\n{e}")


def create_brigades_page(parent, app_ref):
    return BrigadesPage(parent, app_ref)
