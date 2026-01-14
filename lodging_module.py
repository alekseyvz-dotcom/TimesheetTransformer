import logging
from datetime import date, datetime
from typing import Optional, Any, Dict, List, Tuple

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

import psycopg2
from psycopg2.extras import RealDictCursor


# ===================== DB POOL (как в других модулях) =====================

db_connection_pool = None

def set_db_pool(pool):
    global db_connection_pool
    db_connection_pool = pool

def get_db_connection():
    if not db_connection_pool:
        raise RuntimeError("Пул соединений не установлен (set_db_pool не вызывался).")
    return db_connection_pool.getconn()

def release_db_connection(conn):
    if db_connection_pool and conn:
        db_connection_pool.putconn(conn)


# ===================== DB HELPERS =====================

def load_dorms(active_only: bool = True) -> List[Dict[str, Any]]:
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if active_only:
                cur.execute(
                    "SELECT id, name, address, is_active, rate_mode, notes "
                    "FROM dorms WHERE is_active = TRUE "
                    "ORDER BY name, address"
                )
            else:
                cur.execute(
                    "SELECT id, name, address, is_active, rate_mode, notes "
                    "FROM dorms ORDER BY is_active DESC, name, address"
                )
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_db_connection(conn)

def load_rooms(dorm_id: int, active_only: bool = False) -> List[Dict[str, Any]]:
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if active_only:
                cur.execute(
                    "SELECT id, dorm_id, room_no, capacity, is_active, notes "
                    "FROM dorm_rooms WHERE dorm_id = %s AND is_active = TRUE "
                    "ORDER BY room_no",
                    (dorm_id,),
                )
            else:
                cur.execute(
                    "SELECT id, dorm_id, room_no, capacity, is_active, notes "
                    "FROM dorm_rooms WHERE dorm_id = %s "
                    "ORDER BY is_active DESC, room_no",
                    (dorm_id,),
                )
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_db_connection(conn)

def upsert_dorm(dorm_id: Optional[int], name: str, address: str, rate_mode: str, is_active: bool, notes: str) -> int:
    conn = None
    try:
        conn = get_db_connection()
        with conn, conn.cursor() as cur:
            if dorm_id:
                cur.execute(
                    "UPDATE dorms SET name=%s, address=%s, rate_mode=%s, is_active=%s, notes=%s, updated_at=now() "
                    "WHERE id=%s RETURNING id",
                    (name, address, rate_mode, is_active, notes or None, dorm_id),
                )
                return cur.fetchone()[0]
            else:
                cur.execute(
                    "INSERT INTO dorms (name, address, rate_mode, is_active, notes) "
                    "VALUES (%s,%s,%s,%s,%s) RETURNING id",
                    (name, address, rate_mode, is_active, notes or None),
                )
                return cur.fetchone()[0]
    finally:
        release_db_connection(conn)

def upsert_room(room_id: Optional[int], dorm_id: int, room_no: str, capacity: int, is_active: bool, notes: str) -> int:
    conn = None
    try:
        conn = get_db_connection()
        with conn, conn.cursor() as cur:
            if room_id:
                cur.execute(
                    "UPDATE dorm_rooms SET room_no=%s, capacity=%s, is_active=%s, notes=%s, updated_at=now() "
                    "WHERE id=%s RETURNING id",
                    (room_no, capacity, is_active, notes or None, room_id),
                )
                return cur.fetchone()[0]
            else:
                cur.execute(
                    "INSERT INTO dorm_rooms (dorm_id, room_no, capacity, is_active, notes) "
                    "VALUES (%s,%s,%s,%s,%s) RETURNING id",
                    (dorm_id, room_no, capacity, is_active, notes or None),
                )
                return cur.fetchone()[0]
    finally:
        release_db_connection(conn)

def _parse_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def _norm(s: str) -> str:
    return (s or "").strip().lower()

def load_active_occupancy_by_room(dorm_id: int) -> Dict[int, int]:
    """
    Возвращает {room_id: occupied_count} только по активным проживающим.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT room_id, COUNT(*) "
                "FROM dorm_stays "
                "WHERE dorm_id = %s AND status='active' AND check_out IS NULL "
                "GROUP BY room_id",
                (dorm_id,),
            )
            return {int(r[0]): int(r[1]) for r in cur.fetchall()}
    finally:
        release_db_connection(conn)

def load_stays(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Реестр проживаний с join на employees/dorms/rooms.
    filters:
      dorm_id: Optional[int]
      only_active: bool
      q: Optional[str] (поиск по fio/tbn)
    """
    dorm_id = filters.get("dorm_id")
    only_active = bool(filters.get("only_active", True))
    q = (filters.get("q") or "").strip()

    where = ["1=1"]
    params: List[Any] = []

    if dorm_id:
        where.append("s.dorm_id = %s")
        params.append(dorm_id)

    if only_active:
        where.append("s.status='active' AND s.check_out IS NULL")

    if q:
        where.append("(e.fio ILIKE %s OR COALESCE(e.tbn,'') ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])

    where_sql = " AND ".join(where)

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                    s.id AS stay_id,
                    s.check_in, s.check_out, s.status,
                    e.id AS employee_id, e.fio, e.tbn,
                    d.id AS dorm_id, d.name AS dorm_name, d.address AS dorm_address,
                    r.id AS room_id, r.room_no, r.capacity
                FROM dorm_stays s
                JOIN employees e   ON e.id = s.employee_id
                JOIN dorms d       ON d.id = s.dorm_id
                JOIN dorm_rooms r  ON r.id = s.room_id
                WHERE {where_sql}
                ORDER BY d.name, r.room_no, e.fio
                """,
                params,
            )
            return [dict(x) for x in cur.fetchall()]
    finally:
        release_db_connection(conn)

def create_stay(employee_id: int, dorm_id: int, room_id: int, check_in: date, created_by: Optional[int], notes: str = ""):
    """
    Заселение:
      - проверка: свободные места
      - проверка: нет активного проживания у сотрудника (частично гарантируется уникальным индексом)
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn, conn.cursor() as cur:
            # capacity
            cur.execute("SELECT capacity FROM dorm_rooms WHERE id=%s AND dorm_id=%s", (room_id, dorm_id))
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Комната не найдена.")
            capacity = int(row[0] or 0)

            # occupied
            cur.execute(
                "SELECT COUNT(*) FROM dorm_stays "
                "WHERE room_id=%s AND status='active' AND check_out IS NULL",
                (room_id,),
            )
            occupied = int(cur.fetchone()[0] or 0)

            if occupied >= capacity:
                raise RuntimeError("В комнате нет свободных мест.")

            cur.execute(
                "INSERT INTO dorm_stays (employee_id, dorm_id, room_id, check_in, status, created_by, notes) "
                "VALUES (%s,%s,%s,%s,'active',%s,%s)",
                (employee_id, dorm_id, room_id, check_in, created_by, notes or None),
            )
    except psycopg2.errors.UniqueViolation:
        raise RuntimeError("У сотрудника уже есть активное проживание (нельзя заселить повторно).")
    finally:
        release_db_connection(conn)

def close_stay(stay_id: int, check_out: date, closed_by: Optional[int], reason: str = ""):
    conn = None
    try:
        conn = get_db_connection()
        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE dorm_stays "
                "SET check_out=%s, status='closed', closed_by=%s, close_reason=%s, updated_at=now() "
                "WHERE id=%s",
                (check_out, closed_by, reason or None, stay_id),
            )
            if cur.rowcount != 1:
                raise RuntimeError("Запись проживания не найдена.")
    finally:
        release_db_connection(conn)


# ===================== DIALOGS =====================

class SimpleTextDialog(simpledialog.Dialog):
    def __init__(self, parent, title: str, prompt: str, initial: str = ""):
        self.prompt = prompt
        self.initial = initial
        self.value = None
        super().__init__(parent, title=title)

    def body(self, master):
        ttk.Label(master, text=self.prompt).grid(row=0, column=0, sticky="w")
        self.ent = ttk.Entry(master, width=50)
        self.ent.grid(row=1, column=0, sticky="we", pady=(6, 0))
        self.ent.insert(0, self.initial or "")
        return self.ent

    def apply(self):
        self.value = (self.ent.get() or "").strip()

class CheckInDialog(simpledialog.Dialog):
    def __init__(self, parent, employees: List[Tuple[int, str, str]], dorms: List[Dict[str, Any]], title="Заселение"):
        self._all_employees = employees[:]  # (id, fio, tbn)
        self._filtered_employees: List[Tuple[int, str, str]] = employees[:]
        self.dorms = dorms
        self.result = None
        self._rooms: List[Dict[str, Any]] = []
        super().__init__(parent, title=title)

    def body(self, master):
        ttk.Label(master, text="Поиск сотрудника (ФИО/Таб№):").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=4)
        self.var_emp_q = tk.StringVar()
        ent_q = ttk.Entry(master, width=30, textvariable=self.var_emp_q)
        ent_q.grid(row=0, column=1, sticky="w", pady=4)
        ent_q.bind("<KeyRelease>", lambda e: self._reload_employees())

        ttk.Label(master, text="Сотрудник:").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=4)
        self.cmb_emp = ttk.Combobox(master, state="readonly", width=48)
        self.cmb_emp.grid(row=1, column=1, sticky="w", pady=4)

        ttk.Label(master, text="Общежитие:").grid(row=2, column=0, sticky="e", padx=(0, 6), pady=4)
        self.cmb_dorm = ttk.Combobox(master, state="readonly", width=48)
        self.cmb_dorm.grid(row=2, column=1, sticky="w", pady=4)
        self.cmb_dorm["values"] = [f"{d['name']} | {d['address']} | id={d['id']}" for d in self.dorms]
        self.cmb_dorm.bind("<<ComboboxSelected>>", lambda e: self._reload_rooms())

        ttk.Label(master, text="Комната:").grid(row=3, column=0, sticky="e", padx=(0, 6), pady=4)
        self.cmb_room = ttk.Combobox(master, state="readonly", width=48)
        self.cmb_room.grid(row=3, column=1, sticky="w", pady=4)

        ttk.Label(master, text="Дата заезда (дд.мм.гггг):").grid(row=4, column=0, sticky="e", padx=(0, 6), pady=4)
        self.ent_in = ttk.Entry(master, width=20)
        self.ent_in.grid(row=4, column=1, sticky="w", pady=4)
        self.ent_in.insert(0, datetime.now().strftime("%d.%m.%Y"))

        ttk.Label(master, text="Комментарий:").grid(row=5, column=0, sticky="ne", padx=(0, 6), pady=4)
        self.txt_notes = tk.Text(master, width=48, height=3)
        self.txt_notes.grid(row=5, column=1, sticky="we", pady=4)

        # первичная загрузка сотрудников
        self._reload_employees()
        return ent_q  # фокус на поиске

    def _reload_employees(self):
        q = _norm(self.var_emp_q.get())
        if not q:
            self._filtered_employees = self._all_employees[:]
        else:
            def ok(e):
                eid, fio, tbn = e
                hay = f"{fio} {tbn} {eid}".lower()
                return q in hay
            self._filtered_employees = [e for e in self._all_employees if ok(e)]

        self.cmb_emp["values"] = [f"{fio} | {tbn or ''} | id={eid}" for eid, fio, tbn in self._filtered_employees]

        # авто-выбор первого, чтобы не оставалось пусто
        if self._filtered_employees:
            self.cmb_emp.current(0)
        else:
            self.cmb_emp.set("")

    def _reload_rooms(self):
        idx = self.cmb_dorm.current()
        if idx < 0:
            self.cmb_room["values"] = []
            return
        dorm_id = int(self.dorms[idx]["id"])
        rooms = load_rooms(dorm_id, active_only=True)
        occ = load_active_occupancy_by_room(dorm_id)
        self._rooms = rooms

        vals = []
        for r in rooms:
            rid = int(r["id"])
            cap = int(r["capacity"] or 0)
            used = int(occ.get(rid, 0))
            vals.append(f"{r['room_no']} | {used}/{cap} занято | id={rid}")
        self.cmb_room["values"] = vals
        if vals:
            self.cmb_room.current(0)

    def validate(self):
        if self.cmb_emp.current() < 0:
            messagebox.showwarning("Заселение", "Выберите сотрудника.", parent=self)
            return False
        if self.cmb_dorm.current() < 0:
            messagebox.showwarning("Заселение", "Выберите общежитие.", parent=self)
            return False
        if self.cmb_room.current() < 0:
            messagebox.showwarning("Заселение", "Выберите комнату.", parent=self)
            return False

        d = _parse_date(self.ent_in.get())
        if not d:
            messagebox.showwarning("Заселение", "Введите корректную дату заезда (дд.мм.гггг).", parent=self)
            return False

        self._check_in = d
        return True

    def apply(self):
        emp_idx = self.cmb_emp.current()
        dorm_idx = self.cmb_dorm.current()
        room_idx = self.cmb_room.current()

        employee_id = int(self._filtered_employees[emp_idx][0])
        dorm_id = int(self.dorms[dorm_idx]["id"])
        room_id = int(self._rooms[room_idx]["id"])
        notes = (self.txt_notes.get("1.0", "end").strip() or "")

        self.result = {
            "employee_id": employee_id,
            "dorm_id": dorm_id,
            "room_id": room_id,
            "check_in": self._check_in,
            "notes": notes,
        }

# ===================== PAGES =====================

class LodgingRegistryPage(tk.Frame):
    def __init__(self, master, app_ref):
        super().__init__(master)
        self.app_ref = app_ref

        self.var_only_active = tk.BooleanVar(value=True)
        self.var_q = tk.StringVar()
        self.var_dorm = tk.StringVar()

        self._dorms: List[Dict[str, Any]] = []

        self._build_ui()
        self._reload()

    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=8)

        ttk.Checkbutton(top, text="Только активные", variable=self.var_only_active, command=self._reload).pack(side="left")

        ttk.Label(top, text="Общежитие:").pack(side="left", padx=(12, 4))
        self.cmb_dorm = ttk.Combobox(top, state="readonly", width=44, textvariable=self.var_dorm)
        self.cmb_dorm.pack(side="left")
        self.cmb_dorm.bind("<<ComboboxSelected>>", lambda e: self._reload())

        ttk.Label(top, text="Поиск (ФИО/Таб№):").pack(side="left", padx=(12, 4))
        ent = ttk.Entry(top, textvariable=self.var_q, width=30)
        ent.pack(side="left")
        ent.bind("<KeyRelease>", lambda e: self.after(150, self._reload))

        btns = tk.Frame(self)
        btns.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(btns, text="Заселить…", command=self._check_in).pack(side="left", padx=(0, 6))
        ttk.Button(btns, text="Выселить…", command=self._check_out).pack(side="left")

        # table
        frame = tk.Frame(self)
        frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        cols = ("fio", "tbn", "dorm", "room", "cap", "check_in", "check_out", "status")
        self.tree = ttk.Treeview(frame, columns=cols, show="headings", selectmode="browse")
        for c, title, w in [
            ("fio", "ФИО", 240),
            ("tbn", "Таб№", 80),
            ("dorm", "Общежитие", 220),
            ("room", "Комната", 90),
            ("cap", "Мест", 60),
            ("check_in", "Заезд", 90),
            ("check_out", "Выезд", 90),
            ("status", "Статус", 80),
        ]:
            self.tree.heading(c, text=title)
            self.tree.column(c, width=w, anchor="w")

        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)

        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

    def _reload_dorms(self):
        self._dorms = load_dorms(active_only=False)
        values = ["Все"] + [f"{d['name']} | {d['address']} | id={d['id']}" for d in self._dorms]
        self.cmb_dorm["values"] = values
        if not self.cmb_dorm.get():
            self.cmb_dorm.set("Все")

    def _selected_dorm_id(self) -> Optional[int]:
        s = (self.cmb_dorm.get() or "").strip()
        if not s or s == "Все":
            return None
        # пытаемся вытащить "... id=123"
        try:
            if "id=" in s:
                return int(s.rsplit("id=", 1)[1])
        except Exception:
            return None
        return None

    def _reload(self):
        self._reload_dorms()

        dorm_id = self._selected_dorm_id()
        only_active = bool(self.var_only_active.get())
        q = (self.var_q.get() or "").strip()

        rows = load_stays({"dorm_id": dorm_id, "only_active": only_active, "q": q})

        self.tree.delete(*self.tree.get_children())
        for r in rows:
            dorm_disp = f"{r['dorm_name']}"
            room_disp = str(r["room_no"])
            cap = str(r.get("capacity") or "")

            ci = r["check_in"].strftime("%d.%m.%Y") if r.get("check_in") else ""
            co = r["check_out"].strftime("%d.%m.%Y") if r.get("check_out") else ""
            status = r.get("status") or ""

            self.tree.insert(
                "",
                "end",
                iid=str(r["stay_id"]),
                values=(r["fio"], r.get("tbn") or "", dorm_disp, room_disp, cap, ci, co, status),
            )

    def _check_in(self):
        # список сотрудников (id, fio, tbn)
        conn = None
        employees: List[Tuple[int, str, str]] = []
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT id, fio, COALESCE(tbn,'') FROM employees WHERE COALESCE(is_fired,FALSE)=FALSE ORDER BY fio")
                employees = [(int(a), str(b), str(c)) for a, b, c in cur.fetchall()]
        finally:
            release_db_connection(conn)

        dorms = load_dorms(active_only=True)
        if not dorms:
            messagebox.showwarning("Проживание", "Нет активных общежитий. Сначала создайте общежитие.", parent=self)
            return

        dlg = CheckInDialog(self, employees, dorms)
        if not dlg.result:
            return

        user_id = (self.app_ref.current_user or {}).get("id") if hasattr(self.app_ref, "current_user") else None
        try:
            create_stay(
                employee_id=dlg.result["employee_id"],
                dorm_id=dlg.result["dorm_id"],
                room_id=dlg.result["room_id"],
                check_in=dlg.result["check_in"],
                created_by=user_id,
                notes=dlg.result.get("notes") or "",
            )
        except Exception as e:
            messagebox.showerror("Заселение", str(e), parent=self)
            return

        self._reload()

    def _check_out(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Выселение", "Выберите запись проживания в таблице.", parent=self)
            return
        stay_id = int(sel[0])

        sdate = datetime.now().strftime("%d.%m.%Y")
        dlg = SimpleTextDialog(self, "Выселение", "Дата выезда (дд.мм.гггг):", initial=sdate)
        if dlg.value is None:
            return
        d = _parse_date(dlg.value)
        if not d:
            messagebox.showwarning("Выселение", "Некорректная дата.", parent=self)
            return

        reason_dlg = SimpleTextDialog(self, "Выселение", "Причина (необязательно):", initial="")
        reason = (reason_dlg.value or "") if reason_dlg.value is not None else ""

        user_id = (self.app_ref.current_user or {}).get("id") if hasattr(self.app_ref, "current_user") else None
        try:
            close_stay(stay_id, d, user_id, reason)
        except Exception as e:
            messagebox.showerror("Выселение", str(e), parent=self)
            return

        self._reload()


class DormsPage(tk.Frame):
    def __init__(self, master, app_ref):
        super().__init__(master)
        self.app_ref = app_ref

        self._dorms: List[Dict[str, Any]] = []
        self._rooms: List[Dict[str, Any]] = []
        self._selected_dorm_id: Optional[int] = None

        self._build_ui()
        self._reload_dorms()

    def _build_ui(self):
        root = tk.Frame(self)
        root.pack(fill="both", expand=True, padx=8, pady=8)
        root.columnconfigure(0, weight=1)
        root.columnconfigure(1, weight=2)
        root.rowconfigure(0, weight=1)

        # left: dorms
        left = tk.Frame(root)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        ttk.Label(left, text="Общежития").pack(anchor="w")

        cols = ("name", "address", "active", "mode")
        self.tree_dorms = ttk.Treeview(left, columns=cols, show="headings", selectmode="browse", height=18)
        self.tree_dorms.heading("name", text="Название")
        self.tree_dorms.heading("address", text="Адрес")
        self.tree_dorms.heading("active", text="Активно")
        self.tree_dorms.heading("mode", text="Тариф")

        self.tree_dorms.column("name", width=160)
        self.tree_dorms.column("address", width=240)
        self.tree_dorms.column("active", width=60, anchor="center")
        self.tree_dorms.column("mode", width=80, anchor="center")

        self.tree_dorms.pack(fill="both", expand=True, pady=(6, 6))
        self.tree_dorms.bind("<<TreeviewSelect>>", lambda e: self._on_select_dorm())

        btns = tk.Frame(left)
        btns.pack(fill="x")
        ttk.Button(btns, text="Добавить", command=self._add_dorm).pack(side="left", padx=(0, 6))
        ttk.Button(btns, text="Редактировать", command=self._edit_dorm).pack(side="left")

        # right: rooms
        right = tk.Frame(root)
        right.grid(row=0, column=1, sticky="nsew")
        ttk.Label(right, text="Комнаты").pack(anchor="w")

        cols2 = ("room_no", "capacity", "occupied", "free", "active")
        self.tree_rooms = ttk.Treeview(right, columns=cols2, show="headings", selectmode="browse", height=18)
        self.tree_rooms.heading("room_no", text="Комната")
        self.tree_rooms.heading("capacity", text="Мест")
        self.tree_rooms.heading("occupied", text="Занято")
        self.tree_rooms.heading("free", text="Свободно")
        self.tree_rooms.heading("active", text="Активно")
        
        self.tree_rooms.column("room_no", width=120)
        self.tree_rooms.column("capacity", width=60, anchor="center")
        self.tree_rooms.column("occupied", width=70, anchor="center")
        self.tree_rooms.column("free", width=80, anchor="center")
        self.tree_rooms.column("active", width=60, anchor="center")

        self.tree_rooms.pack(fill="both", expand=True, pady=(6, 6))

        btns2 = tk.Frame(right)
        btns2.pack(fill="x")
        ttk.Button(btns2, text="Добавить комнату", command=self._add_room).pack(side="left", padx=(0, 6))
        ttk.Button(btns2, text="Редактировать", command=self._edit_room).pack(side="left")

    def _reload_dorms(self):
        self._dorms = load_dorms(active_only=False)
        self.tree_dorms.delete(*self.tree_dorms.get_children())
        for d in self._dorms:
            mode = d.get("rate_mode") or ""
            active = "Да" if d.get("is_active") else "Нет"
            self.tree_dorms.insert("", "end", iid=str(d["id"]), values=(d["name"], d["address"], active, mode))

        self._selected_dorm_id = None
        self._rooms = []
        self.tree_rooms.delete(*self.tree_rooms.get_children())

    def _on_select_dorm(self):
        sel = self.tree_dorms.selection()
        if not sel:
            return
        self._selected_dorm_id = int(sel[0])
        self._reload_rooms()

    def _reload_rooms(self):
        if not self._selected_dorm_id:
            return
        self._rooms = load_rooms(self._selected_dorm_id, active_only=False)
    
        # занятость только по активным проживающим
        occ = load_active_occupancy_by_room(self._selected_dorm_id)
    
        self.tree_rooms.delete(*self.tree_rooms.get_children())
        for r in self._rooms:
            rid = int(r["id"])
            cap = int(r.get("capacity") or 0)
            used = int(occ.get(rid, 0))
            free = max(0, cap - used)
    
            active = "Да" if r.get("is_active") else "Нет"
            self.tree_rooms.insert(
                "",
                "end",
                iid=str(rid),
                values=(r["room_no"], cap, used, free, active),
            )

    def _add_dorm(self):
        self._open_dorm_editor(None)

    def _edit_dorm(self):
        sel = self.tree_dorms.selection()
        if not sel:
            messagebox.showinfo("Общежития", "Выберите общежитие.", parent=self)
            return
        dorm_id = int(sel[0])
        dorm = next((d for d in self._dorms if int(d["id"]) == dorm_id), None)
        if not dorm:
            return
        self._open_dorm_editor(dorm)

    def _open_dorm_editor(self, dorm: Optional[Dict[str, Any]]):
        win = tk.Toplevel(self)
        win.title("Общежитие")
        win.resizable(False, False)
        win.grab_set()

        frm = tk.Frame(win, padx=10, pady=10)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Название:").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=4)
        ent_name = ttk.Entry(frm, width=40)
        ent_name.grid(row=0, column=1, sticky="w", pady=4)

        ttk.Label(frm, text="Адрес:").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=4)
        ent_addr = ttk.Entry(frm, width=40)
        ent_addr.grid(row=1, column=1, sticky="w", pady=4)

        ttk.Label(frm, text="Режим тарифа:").grid(row=2, column=0, sticky="e", padx=(0, 6), pady=4)
        cmb_mode = ttk.Combobox(frm, state="readonly", width=18, values=["PER_DORM", "PER_ROOM"])
        cmb_mode.grid(row=2, column=1, sticky="w", pady=4)

        var_active = tk.BooleanVar(value=True)
        chk = ttk.Checkbutton(frm, text="Активно", variable=var_active)
        chk.grid(row=3, column=1, sticky="w", pady=4)

        ttk.Label(frm, text="Примечание:").grid(row=4, column=0, sticky="ne", padx=(0, 6), pady=4)
        txt = tk.Text(frm, width=40, height=4)
        txt.grid(row=4, column=1, sticky="w", pady=4)

        if dorm:
            ent_name.insert(0, dorm.get("name") or "")
            ent_addr.insert(0, dorm.get("address") or "")
            cmb_mode.set(dorm.get("rate_mode") or "PER_DORM")
            var_active.set(bool(dorm.get("is_active")))
            txt.insert("1.0", dorm.get("notes") or "")
        else:
            cmb_mode.set("PER_DORM")
            var_active.set(True)

        def on_save():
            name = (ent_name.get() or "").strip()
            addr = (ent_addr.get() or "").strip()
            mode = (cmb_mode.get() or "").strip()
            notes = (txt.get("1.0", "end").strip() or "")

            if not name or not addr:
                messagebox.showwarning("Общежитие", "Заполните название и адрес.", parent=win)
                return
            if mode not in ("PER_DORM", "PER_ROOM"):
                messagebox.showwarning("Общежитие", "Некорректный режим тарифа.", parent=win)
                return

            try:
                upsert_dorm(
                    dorm_id=int(dorm["id"]) if dorm else None,
                    name=name,
                    address=addr,
                    rate_mode=mode,
                    is_active=bool(var_active.get()),
                    notes=notes,
                )
            except Exception as e:
                messagebox.showerror("Общежитие", f"Ошибка сохранения:\n{e}", parent=win)
                return

            win.destroy()
            self._reload_dorms()

        btns = tk.Frame(frm)
        btns.grid(row=5, column=0, columnspan=2, sticky="e", pady=(8, 0))
        ttk.Button(btns, text="Сохранить", command=on_save).pack(side="right", padx=(6, 0))
        ttk.Button(btns, text="Отмена", command=win.destroy).pack(side="right")

    def _add_room(self):
        if not self._selected_dorm_id:
            messagebox.showinfo("Комнаты", "Сначала выберите общежитие.", parent=self)
            return
        self._open_room_editor(None)

    def _edit_room(self):
        if not self._selected_dorm_id:
            messagebox.showinfo("Комнаты", "Сначала выберите общежитие.", parent=self)
            return
        sel = self.tree_rooms.selection()
        if not sel:
            messagebox.showinfo("Комнаты", "Выберите комнату.", parent=self)
            return
        rid = int(sel[0])
        room = next((r for r in self._rooms if int(r["id"]) == rid), None)
        if not room:
            return
        self._open_room_editor(room)

    def _open_room_editor(self, room: Optional[Dict[str, Any]]):
        win = tk.Toplevel(self)
        win.title("Комната")
        win.resizable(False, False)
        win.grab_set()

        frm = tk.Frame(win, padx=10, pady=10)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Комната:").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=4)
        ent_no = ttk.Entry(frm, width=18)
        ent_no.grid(row=0, column=1, sticky="w", pady=4)

        ttk.Label(frm, text="Мест:").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=4)
        spn_cap = tk.Spinbox(frm, from_=0, to=100, width=6)
        spn_cap.grid(row=1, column=1, sticky="w", pady=4)

        var_active = tk.BooleanVar(value=True)
        ttk.Checkbutton(frm, text="Активно", variable=var_active).grid(row=2, column=1, sticky="w", pady=4)

        ttk.Label(frm, text="Примечание:").grid(row=3, column=0, sticky="ne", padx=(0, 6), pady=4)
        txt = tk.Text(frm, width=40, height=4)
        txt.grid(row=3, column=1, sticky="w", pady=4)

        if room:
            ent_no.insert(0, room.get("room_no") or "")
            spn_cap.delete(0, "end")
            spn_cap.insert(0, str(room.get("capacity") or 0))
            var_active.set(bool(room.get("is_active")))
            txt.insert("1.0", room.get("notes") or "")
        else:
            spn_cap.delete(0, "end")
            spn_cap.insert(0, "4")
            var_active.set(True)

        def on_save():
            room_no = (ent_no.get() or "").strip()
            try:
                cap = int(spn_cap.get())
                if cap < 0:
                    raise ValueError
            except Exception:
                messagebox.showwarning("Комната", "Мест должно быть числом >= 0.", parent=win)
                return

            notes = (txt.get("1.0", "end").strip() or "")
            if not room_no:
                messagebox.showwarning("Комната", "Введите номер комнаты.", parent=win)
                return

            try:
                upsert_room(
                    room_id=int(room["id"]) if room else None,
                    dorm_id=int(self._selected_dorm_id),
                    room_no=room_no,
                    capacity=cap,
                    is_active=bool(var_active.get()),
                    notes=notes,
                )
            except Exception as e:
                messagebox.showerror("Комната", f"Ошибка сохранения:\n{e}", parent=win)
                return

            win.destroy()
            self._reload_rooms()

        btns = tk.Frame(frm)
        btns.grid(row=4, column=0, columnspan=2, sticky="e", pady=(8, 0))
        ttk.Button(btns, text="Сохранить", command=on_save).pack(side="right", padx=(6, 0))
        ttk.Button(btns, text="Отмена", command=win.destroy).pack(side="right")


# ===================== API FOR MAIN APP =====================

def create_lodging_registry_page(parent, app_ref):
    return LodgingRegistryPage(parent, app_ref)

def create_dorms_page(parent, app_ref):
    return DormsPage(parent, app_ref)
