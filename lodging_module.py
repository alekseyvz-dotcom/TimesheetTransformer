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
def load_employees_for_checkin(include_fired: bool = False) -> List[Dict[str, Any]]:
    """
    Возвращает сотрудников + признак active_stay.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            where = ["1=1"]
            if not include_fired:
                where.append("COALESCE(e.is_fired,FALSE)=FALSE")

            cur.execute(
                f"""
                SELECT
                    e.id,
                    e.fio,
                    COALESCE(e.tbn,'') AS tbn,
                    EXISTS (
                        SELECT 1
                        FROM dorm_stays s
                        WHERE s.employee_id = e.id
                          AND s.status='active'
                          AND s.check_out IS NULL
                    ) AS has_active_stay
                FROM employees e
                WHERE {" AND ".join(where)}
                ORDER BY e.fio
                """
            )
            return [dict(r) for r in cur.fetchall()]
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

def load_rates(dorm_id: Optional[int] = None, room_id: Optional[int] = None) -> List[Dict[str, Any]]:
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            where = ["1=1"]
            params: List[Any] = []
            if dorm_id is not None:
                where.append("dr.dorm_id = %s")
                params.append(dorm_id)
            if room_id is not None:
                where.append("dr.room_id = %s")
                params.append(room_id)

            cur.execute(
                f"""
                SELECT
                    dr.id,
                    dr.dorm_id, dr.room_id,
                    dr.valid_from,
                    dr.price_per_day,
                    dr.currency,
                    dr.comment,
                    d.name AS dorm_name,
                    r.room_no
                FROM dorm_rates dr
                LEFT JOIN dorms d ON d.id = dr.dorm_id
                LEFT JOIN dorm_rooms r ON r.id = dr.room_id
                WHERE {" AND ".join(where)}
                ORDER BY COALESCE(d.name,''), COALESCE(r.room_no,''), dr.valid_from DESC
                """,
                params,
            )
            return [dict(x) for x in cur.fetchall()]
    finally:
        release_db_connection(conn)


def upsert_rate(rate_id: Optional[int], dorm_id: Optional[int], room_id: Optional[int],
                valid_from: date, price_per_day: float, currency: str, created_by: Optional[int], comment: str = "") -> int:
    conn = None
    try:
        conn = get_db_connection()
        with conn, conn.cursor() as cur:
            if rate_id:
                cur.execute(
                    """
                    UPDATE dorm_rates
                    SET dorm_id=%s, room_id=%s, valid_from=%s, price_per_day=%s, currency=%s, comment=%s
                    WHERE id=%s
                    RETURNING id
                    """,
                    (dorm_id, room_id, valid_from, price_per_day, currency, comment or None, rate_id),
                )
                return int(cur.fetchone()[0])
            else:
                cur.execute(
                    """
                    INSERT INTO dorm_rates (dorm_id, room_id, valid_from, price_per_day, currency, created_by, comment)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                    """,
                    (dorm_id, room_id, valid_from, price_per_day, currency, created_by, comment or None),
                )
                return int(cur.fetchone()[0])
    finally:
        release_db_connection(conn)


def delete_rate(rate_id: int):
    conn = None
    try:
        conn = get_db_connection()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM dorm_rates WHERE id=%s", (rate_id,))
            if cur.rowcount != 1:
                raise RuntimeError("Тариф не найден.")
    finally:
        release_db_connection(conn)


def pick_rate_for_date(dorm_id: int, room_id: int, on_date: date) -> Dict[str, Any]:
    """
    Выбор тарифа на дату:
      - если у общежития PER_ROOM: берём тариф комнаты (room_id), иначе тариф общежития (dorm_id)
      - выбираем последнюю запись с valid_from <= on_date
    Возвращает dict тарифа или бросает исключение.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT rate_mode FROM dorms WHERE id=%s", (dorm_id,))
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Общежитие не найдено.")
            rate_mode = row["rate_mode"]

            if rate_mode == "PER_ROOM":
                cur.execute(
                    """
                    SELECT * FROM dorm_rates
                    WHERE room_id=%s AND valid_from <= %s
                    ORDER BY valid_from DESC
                    LIMIT 1
                    """,
                    (room_id, on_date),
                )
                r = cur.fetchone()
                if not r:
                    raise RuntimeError("Не найден тариф комнаты на выбранную дату.")
                return dict(r)
            else:
                cur.execute(
                    """
                    SELECT * FROM dorm_rates
                    WHERE dorm_id=%s AND valid_from <= %s
                    ORDER BY valid_from DESC
                    LIMIT 1
                    """,
                    (dorm_id, on_date),
                )
                r = cur.fetchone()
                if not r:
                    raise RuntimeError("Не найден тариф общежития на выбранную дату.")
                return dict(r)
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
    def __init__(self, parent, employees: List[Dict[str, Any]], dorms: List[Dict[str, Any]], title="Заселение"):
        self._all_employees = employees[:]   # dict: id,fio,tbn,has_active_stay
        self._filtered_employees: List[Dict[str, Any]] = employees[:]
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

        self.var_show_all = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            master,
            text="Показывать всех (включая уже проживающих)",
            variable=self.var_show_all,
            command=self._reload_employees,
        ).grid(row=1, column=1, sticky="w", pady=2)

        ttk.Label(master, text="Сотрудник:").grid(row=2, column=0, sticky="e", padx=(0, 6), pady=4)
        self.cmb_emp = ttk.Combobox(master, state="readonly", width=56)
        self.cmb_emp.grid(row=2, column=1, sticky="w", pady=4)

        ttk.Label(master, text="Общежитие:").grid(row=3, column=0, sticky="e", padx=(0, 6), pady=4)
        self.cmb_dorm = ttk.Combobox(master, state="readonly", width=56)
        self.cmb_dorm.grid(row=3, column=1, sticky="w", pady=4)
        self.cmb_dorm["values"] = [f"{d['name']} | {d['address']} | id={d['id']}" for d in self.dorms]
        self.cmb_dorm.bind("<<ComboboxSelected>>", lambda e: self._reload_rooms())

        self.var_hide_full = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            master,
            text="Скрыть комнаты без мест",
            variable=self.var_hide_full,
            command=self._reload_rooms,
        ).grid(row=4, column=1, sticky="w", pady=2)

        ttk.Label(master, text="Комната:").grid(row=5, column=0, sticky="e", padx=(0, 6), pady=4)
        self.cmb_room = ttk.Combobox(master, state="readonly", width=56)
        self.cmb_room.grid(row=5, column=1, sticky="w", pady=4)

        ttk.Label(master, text="Дата заезда (дд.мм.гггг):").grid(row=6, column=0, sticky="e", padx=(0, 6), pady=4)
        self.ent_in = ttk.Entry(master, width=20)
        self.ent_in.grid(row=6, column=1, sticky="w", pady=4)
        self.ent_in.insert(0, datetime.now().strftime("%d.%m.%Y"))

        ttk.Label(master, text="Комментарий:").grid(row=7, column=0, sticky="ne", padx=(0, 6), pady=4)
        self.txt_notes = tk.Text(master, width=56, height=3)
        self.txt_notes.grid(row=7, column=1, sticky="we", pady=4)

        self._reload_employees()
        return ent_q

    def _reload_employees(self):
        q = _norm(self.var_emp_q.get())
        show_all = bool(self.var_show_all.get())

        items = self._all_employees
        if not show_all:
            items = [e for e in items if not e.get("has_active_stay")]

        if q:
            def ok(e):
                hay = f"{e.get('fio','')} {e.get('tbn','')} {e.get('id','')}".lower()
                return q in hay
            items = [e for e in items if ok(e)]

        self._filtered_employees = items

        vals = []
        for e in self._filtered_employees:
            mark = " (УЖЕ ПРОЖИВАЕТ)" if e.get("has_active_stay") else ""
            vals.append(f"{e.get('fio','')} | {e.get('tbn','')} | id={e.get('id')}{mark}")

        self.cmb_emp["values"] = vals
        if vals:
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

        # считаем free и сортируем по убыванию свободных мест
        enriched = []
        for r in rooms:
            rid = int(r["id"])
            cap = int(r.get("capacity") or 0)
            used = int(occ.get(rid, 0))
            free = max(0, cap - used)
            enriched.append((free, used, cap, r))

        hide_full = bool(self.var_hide_full.get())
        if hide_full:
            enriched = [x for x in enriched if x[0] > 0]

        enriched.sort(key=lambda x: (-x[0], str(x[3].get("room_no") or "")))

        self._rooms = [x[3] for x in enriched]

        vals = []
        for free, used, cap, r in enriched:
            vals.append(f"{r['room_no']} | {used}/{cap} занято | свободно: {free} | id={r['id']}")

        self.cmb_room["values"] = vals
        if vals:
            self.cmb_room.current(0)
        else:
            self.cmb_room.set("")

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

        # защита: если оператор включил “показывать всех” и выбрал уже проживающего
        emp = self._filtered_employees[self.cmb_emp.current()]
        if emp.get("has_active_stay"):
            messagebox.showwarning("Заселение", "У сотрудника уже есть активное проживание.", parent=self)
            return False

        self._check_in = d
        return True

    def apply(self):
        emp = self._filtered_employees[self.cmb_emp.current()]
        dorm = self.dorms[self.cmb_dorm.current()]
        room = self._rooms[self.cmb_room.current()]

        notes = (self.txt_notes.get("1.0", "end").strip() or "")

        self.result = {
            "employee_id": int(emp["id"]),
            "dorm_id": int(dorm["id"]),
            "room_id": int(room["id"]),
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
        employees = load_employees_for_checkin(include_fired=False)
    
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

class RatesPage(tk.Frame):
    def __init__(self, master, app_ref):
        super().__init__(master)
        self.app_ref = app_ref
        self._dorms: List[Dict[str, Any]] = []
        self._rooms: List[Dict[str, Any]] = []
        self._build_ui()
        self._reload_dorms()

    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", padx=8, pady=8)

        ttk.Label(top, text="Общежитие:").pack(side="left")
        self.cmb_dorm = ttk.Combobox(top, state="readonly", width=50)
        self.cmb_dorm.pack(side="left", padx=(6, 12))
        self.cmb_dorm.bind("<<ComboboxSelected>>", lambda e: self._reload_rooms_and_rates())

        ttk.Label(top, text="Комната (для PER_ROOM):").pack(side="left")
        self.cmb_room = ttk.Combobox(top, state="readonly", width=30)
        self.cmb_room.pack(side="left", padx=(6, 12))
        self.cmb_room.bind("<<ComboboxSelected>>", lambda e: self._reload_rates())

        btns = tk.Frame(self)
        btns.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(btns, text="Добавить…", command=self._add).pack(side="left", padx=(0, 6))
        ttk.Button(btns, text="Редактировать…", command=self._edit).pack(side="left", padx=(0, 6))
        ttk.Button(btns, text="Удалить", command=self._delete).pack(side="left")
        ttk.Button(btns, text="Проверить тариф на дату…", command=self._check_pick).pack(side="right")

        frame = tk.Frame(self)
        frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        cols = ("scope", "valid_from", "price", "currency", "comment")
        self.tree = ttk.Treeview(frame, columns=cols, show="headings", selectmode="browse")
        self.tree.heading("scope", text="Область")
        self.tree.heading("valid_from", text="Действует с")
        self.tree.heading("price", text="Цена/сутки")
        self.tree.heading("currency", text="Валюта")
        self.tree.heading("comment", text="Комментарий")

        self.tree.column("scope", width=260)
        self.tree.column("valid_from", width=110, anchor="center")
        self.tree.column("price", width=110, anchor="e")
        self.tree.column("currency", width=70, anchor="center")
        self.tree.column("comment", width=420)

        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

    def _reload_dorms(self):
        self._dorms = load_dorms(active_only=False)
        self.cmb_dorm["values"] = [f"{d['name']} | {d['address']} | id={d['id']}" for d in self._dorms]
        if self._dorms:
            self.cmb_dorm.current(0)
            self._reload_rooms_and_rates()
        else:
            self.cmb_room["values"] = []
            self.tree.delete(*self.tree.get_children())

    def _selected_dorm(self) -> Optional[Dict[str, Any]]:
        idx = self.cmb_dorm.current()
        return self._dorms[idx] if idx >= 0 else None

    def _reload_rooms_and_rates(self):
        dorm = self._selected_dorm()
        if not dorm:
            return
        self._rooms = load_rooms(int(dorm["id"]), active_only=False)
        self.cmb_room["values"] = ["(не выбрано)"] + [f"{r['room_no']} | id={r['id']}" for r in self._rooms]
        self.cmb_room.current(0)
        self._reload_rates()

    def _reload_rates(self):
        dorm = self._selected_dorm()
        if not dorm:
            return
        dorm_id = int(dorm["id"])

        room_id = None
        if self.cmb_room.current() > 0:
            room_id = int(self._rooms[self.cmb_room.current() - 1]["id"])

        rows = load_rates(dorm_id=None, room_id=None)

        # фильтруем для отображения:
        # - показываем тарифы выбранного общежития (dorm_id)
        # - и тарифы выбранной комнаты (room_id), если выбрана
        filtered = []
        for r in rows:
            if r.get("dorm_id") == dorm_id:
                filtered.append(r)
            elif room_id is not None and r.get("room_id") == room_id:
                filtered.append(r)

        self.tree.delete(*self.tree.get_children())
        for r in filtered:
            scope = ""
            if r.get("room_id"):
                scope = f"Комната {r.get('room_no')}"
            else:
                scope = f"Общежитие {r.get('dorm_name')}"
            vf = r["valid_from"].strftime("%d.%m.%Y") if r.get("valid_from") else ""
            price = str(r.get("price_per_day") or "")
            cur = r.get("currency") or ""
            cmt = r.get("comment") or ""
            self.tree.insert("", "end", iid=str(r["id"]), values=(scope, vf, price, cur, cmt))

    def _selected_rate_id(self) -> Optional[int]:
        sel = self.tree.selection()
        return int(sel[0]) if sel else None

    def _rate_editor(self, rate: Optional[Dict[str, Any]]):
        dorm = self._selected_dorm()
        if not dorm:
            return

        win = tk.Toplevel(self)
        win.title("Тариф")
        win.resizable(False, False)
        win.grab_set()

        frm = tk.Frame(win, padx=10, pady=10)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Тип тарифа:").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=4)
        cmb_type = ttk.Combobox(frm, state="readonly", values=["PER_DORM", "PER_ROOM"], width=12)
        cmb_type.grid(row=0, column=1, sticky="w", pady=4)

        ttk.Label(frm, text="Комната (если PER_ROOM):").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=4)
        cmb_room = ttk.Combobox(frm, state="readonly", width=24)
        cmb_room.grid(row=1, column=1, sticky="w", pady=4)
        cmb_room["values"] = ["(не выбрано)"] + [f"{r['room_no']} | id={r['id']}" for r in self._rooms]

        ttk.Label(frm, text="Действует с (дд.мм.гггг):").grid(row=2, column=0, sticky="e", padx=(0, 6), pady=4)
        ent_vf = ttk.Entry(frm, width=20)
        ent_vf.grid(row=2, column=1, sticky="w", pady=4)

        ttk.Label(frm, text="Цена/сутки:").grid(row=3, column=0, sticky="e", padx=(0, 6), pady=4)
        ent_price = ttk.Entry(frm, width=20)
        ent_price.grid(row=3, column=1, sticky="w", pady=4)

        ttk.Label(frm, text="Валюта:").grid(row=4, column=0, sticky="e", padx=(0, 6), pady=4)
        ent_cur = ttk.Entry(frm, width=10)
        ent_cur.grid(row=4, column=1, sticky="w", pady=4)
        ent_cur.insert(0, "RUB")

        ttk.Label(frm, text="Комментарий:").grid(row=5, column=0, sticky="ne", padx=(0, 6), pady=4)
        txt = tk.Text(frm, width=40, height=4)
        txt.grid(row=5, column=1, sticky="w", pady=4)

        if rate:
            # определяем тип
            if rate.get("room_id"):
                cmb_type.set("PER_ROOM")
            else:
                cmb_type.set("PER_DORM")

            # выбираем комнату
            if rate.get("room_id"):
                rid = int(rate["room_id"])
                idx = 0
                for i, r in enumerate(self._rooms):
                    if int(r["id"]) == rid:
                        idx = i + 1
                        break
                cmb_room.current(idx)
            else:
                cmb_room.current(0)

            ent_vf.insert(0, rate["valid_from"].strftime("%d.%m.%Y"))
            ent_price.insert(0, str(rate.get("price_per_day") or ""))
            ent_cur.delete(0, "end")
            ent_cur.insert(0, rate.get("currency") or "RUB")
            txt.insert("1.0", rate.get("comment") or "")
        else:
            cmb_type.set("PER_DORM")
            cmb_room.current(0)
            ent_vf.insert(0, datetime.now().strftime("%d.%m.%Y"))

        def on_save():
            rtype = (cmb_type.get() or "").strip()
            if rtype not in ("PER_DORM", "PER_ROOM"):
                messagebox.showwarning("Тариф", "Выберите тип тарифа.", parent=win)
                return

            vf = _parse_date(ent_vf.get())
            if not vf:
                messagebox.showwarning("Тариф", "Некорректная дата.", parent=win)
                return

            try:
                price = float((ent_price.get() or "").replace(",", "."))
                if price < 0:
                    raise ValueError
            except Exception:
                messagebox.showwarning("Тариф", "Некорректная цена.", parent=win)
                return

            cur = (ent_cur.get() or "").strip() or "RUB"
            comment = (txt.get("1.0", "end").strip() or "")

            dorm_id = int(dorm["id"])
            room_id = None

            if rtype == "PER_ROOM":
                if cmb_room.current() <= 0:
                    messagebox.showwarning("Тариф", "Для PER_ROOM нужно выбрать комнату.", parent=win)
                    return
                room_id = int(self._rooms[cmb_room.current() - 1]["id"])
                dorm_id_for_save = None
            else:
                dorm_id_for_save = dorm_id
                room_id = None

            user_id = (self.app_ref.current_user or {}).get("id") if hasattr(self.app_ref, "current_user") else None
            try:
                upsert_rate(
                    rate_id=int(rate["id"]) if rate else None,
                    dorm_id=dorm_id_for_save,
                    room_id=room_id,
                    valid_from=vf,
                    price_per_day=price,
                    currency=cur,
                    created_by=user_id,
                    comment=comment,
                )
            except Exception as e:
                messagebox.showerror("Тариф", f"Ошибка сохранения:\n{e}", parent=win)
                return

            win.destroy()
            self._reload_rates()

        btns = tk.Frame(frm)
        btns.grid(row=6, column=0, columnspan=2, sticky="e", pady=(8, 0))
        ttk.Button(btns, text="Сохранить", command=on_save).pack(side="right", padx=(6, 0))
        ttk.Button(btns, text="Отмена", command=win.destroy).pack(side="right")

    def _add(self):
        self._rate_editor(None)

    def _edit(self):
        rid = self._selected_rate_id()
        if not rid:
            messagebox.showinfo("Тарифы", "Выберите тариф в таблице.", parent=self)
            return
        rate = next((r for r in load_rates(None, None) if int(r["id"]) == rid), None)
        if not rate:
            messagebox.showerror("Тарифы", "Не удалось загрузить тариф.", parent=self)
            return
        self._rate_editor(rate)

    def _delete(self):
        rid = self._selected_rate_id()
        if not rid:
            messagebox.showinfo("Тарифы", "Выберите тариф в таблице.", parent=self)
            return
        if not messagebox.askyesno("Удаление", "Удалить тариф?", parent=self):
            return
        try:
            delete_rate(rid)
        except Exception as e:
            messagebox.showerror("Тарифы", str(e), parent=self)
            return
        self._reload_rates()

    def _check_pick(self):
        dorm = self._selected_dorm()
        if not dorm:
            return
        dorm_id = int(dorm["id"])

        # нужна комната, чтобы корректно проверять PER_ROOM
        if self.cmb_room.current() <= 0:
            messagebox.showinfo("Проверка тарифа", "Выберите комнату (для проверки тарифа на дату).", parent=self)
            return
        room_id = int(self._rooms[self.cmb_room.current() - 1]["id"])

        dlg = SimpleTextDialog(self, "Проверка тарифа", "Дата (дд.мм.гггг):", initial=datetime.now().strftime("%d.%m.%Y"))
        if dlg.value is None:
            return
        d = _parse_date(dlg.value)
        if not d:
            messagebox.showwarning("Проверка тарифа", "Некорректная дата.", parent=self)
            return

        try:
            r = pick_rate_for_date(dorm_id, room_id, d)
        except Exception as e:
            messagebox.showerror("Проверка тарифа", str(e), parent=self)
            return

        messagebox.showinfo(
            "Проверка тарифа",
            f"Тариф найден:\n"
            f"valid_from: {r.get('valid_from')}\n"
            f"price_per_day: {r.get('price_per_day')} {r.get('currency')}\n"
            f"id: {r.get('id')}",
            parent=self,
        )

# ===================== API FOR MAIN APP =====================

def create_lodging_registry_page(parent, app_ref):
    return LodgingRegistryPage(parent, app_ref)

def create_dorms_page(parent, app_ref):
    return DormsPage(parent, app_ref)

def create_rates_page(parent, app_ref):
    return RatesPage(parent, app_ref)
