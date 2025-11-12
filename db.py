# db.py
import os
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional

try:
    import settings_manager as Settings
except Exception:
    Settings = None

# Попытаться импортировать Postgres драйвер (опционально)
_pg_available = False
try:
    import psycopg2
    import psycopg2.extras
    _pg_available = True
except Exception:
    _pg_available = False

def _db_kind() -> str:
    return Settings.get_db_provider().strip().lower() if Settings else "sqlite"

def _sqlite_path() -> Path:
    if Settings:
        return Path(Settings.get_sqlite_path())
    return Path.cwd() / "app_data.sqlite3"

def _pg_dsn() -> Optional[str]:
    if not Settings:
        return None
    url = (Settings.get_database_url() or "").strip()
    if not url:
        return None
    # если sslmode не указан в URL — добавим из настроек
    if "sslmode=" not in url and Settings.get_db_sslmode():
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode={Settings.get_db_sslmode()}"
    return url

# ————— соединения —————

def get_conn():
    """
    Возвращает соединение SQLite или Postgres.
    Для Postgres требует psycopg2-binary установленным.
    """
    kind = _db_kind()
    if kind == "postgres":
        if not _pg_available:
            raise RuntimeError("Для Postgres требуется пакет psycopg2-binary. Установите: pip install psycopg2-binary")
        dsn = _pg_dsn()
        if not dsn:
            raise RuntimeError("DATABASE_URL пуст. Укажите строку подключения в настройках БД.")
        conn = psycopg2.connect(dsn)
        return conn
    # sqlite по умолчанию
    p = _sqlite_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def _exec_script(conn, sql: str):
    cur = conn.cursor()
    cur.execute(sql) if isinstance(sql, str) and sql.count(";") <= 1 else cur.executescript(sql)
    cur.close()

# ————— схема —————

_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS departments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS employees (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fio TEXT NOT NULL,
  tbn TEXT,
  pos TEXT,
  department_id INTEGER REFERENCES departments(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS ix_emp_dep ON employees(department_id);

CREATE TABLE IF NOT EXISTS objects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT,
  address TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_objects_code_addr ON objects(COALESCE(code,''), address);

CREATE TABLE IF NOT EXISTS timesheets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  object_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
  department_id INTEGER REFERENCES departments(id) ON DELETE SET NULL,
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(object_id, COALESCE(department_id,-1), year, month)
);

CREATE TABLE IF NOT EXISTS timesheet_rows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  timesheet_id INTEGER NOT NULL REFERENCES timesheets(id) ON DELETE CASCADE,
  employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE RESTRICT,
  UNIQUE(timesheet_id, employee_id)
);

CREATE TABLE IF NOT EXISTS timesheet_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  row_id INTEGER NOT NULL REFERENCES timesheet_rows(id) ON DELETE CASCADE,
  day INTEGER NOT NULL,
  base_hours REAL,
  ot_day REAL,
  ot_night REAL,
  UNIQUE(row_id, day)
);
"""

_PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS departments (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS employees (
  id SERIAL PRIMARY KEY,
  fio TEXT NOT NULL,
  tbn TEXT,
  pos TEXT,
  department_id INTEGER REFERENCES departments(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS ix_emp_dep ON employees(department_id);

CREATE TABLE IF NOT EXISTS objects (
  id SERIAL PRIMARY KEY,
  code TEXT,
  address TEXT NOT NULL
);
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ux_objects_code_addr'
  ) THEN
    CREATE UNIQUE INDEX ux_objects_code_addr ON objects(COALESCE(code,''), address);
  END IF;
END$$;

CREATE TABLE IF NOT EXISTS timesheets (
  id SERIAL PRIMARY KEY,
  object_id INTEGER NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
  department_id INTEGER REFERENCES departments(id) ON DELETE SET NULL,
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'ux_ts_obj_dep_period'
  ) THEN
    CREATE UNIQUE INDEX ux_ts_obj_dep_period
      ON timesheets(object_id, COALESCE(department_id, -1), year, month);
  END IF;
END$$;

CREATE TABLE IF NOT EXISTS timesheet_rows (
  id SERIAL PRIMARY KEY,
  timesheet_id INTEGER NOT NULL REFERENCES timesheets(id) ON DELETE CASCADE,
  employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE RESTRICT,
  UNIQUE(timesheet_id, employee_id)
);

CREATE TABLE IF NOT EXISTS timesheet_entries (
  id SERIAL PRIMARY KEY,
  row_id INTEGER NOT NULL REFERENCES timesheet_rows(id) ON DELETE CASCADE,
  day INTEGER NOT NULL CHECK(day BETWEEN 1 AND 31),
  base_hours DOUBLE PRECISION,
  ot_day DOUBLE PRECISION,
  ot_night DOUBLE PRECISION,
  UNIQUE(row_id, day)
);
"""

def init_db():
    conn = get_conn()
    try:
        if _db_kind() == "postgres":
            _exec_script(conn, _PG_SCHEMA)
        else:
            _exec_script(conn, _SQLITE_SCHEMA)
        conn.commit()
    finally:
        conn.close()

# ————— простые операции —————

def upsert_department(name: Optional[str]) -> Optional[int]:
    if not name:
        return None
    conn = get_conn()
    try:
        if _db_kind() == "postgres":
            cur = conn.cursor()
            cur.execute("INSERT INTO departments(name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (name,))
            cur.execute("SELECT id FROM departments WHERE name=%s", (name,))
            dep_id = cur.fetchone()[0]
            conn.commit()
            return dep_id
        else:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO departments(name) VALUES (?)", (name,))
            cur.execute("SELECT id FROM departments WHERE name=?", (name,))
            dep_id = cur.fetchone()[0]
            conn.commit()
            return dep_id
    finally:
        conn.close()

def upsert_employee(fio: str, tbn: str, pos: str, dep_name: Optional[str]) -> int:
    dep_id = upsert_department(dep_name)
    conn = get_conn()
    try:
        cur = conn.cursor()
        if _db_kind() == "postgres":
            cur.execute("SELECT id FROM employees WHERE fio=%s AND COALESCE(tbn,'')=COALESCE(%s,'')", (fio, tbn))
            row = cur.fetchone()
            if row:
                cur.execute("UPDATE employees SET pos=%s, department_id=%s WHERE id=%s", (pos, dep_id, row[0]))
                emp_id = row[0]
            else:
                cur.execute("INSERT INTO employees(fio, tbn, pos, department_id) VALUES (%s,%s,%s,%s) RETURNING id",
                            (fio, tbn, pos, dep_id))
                emp_id = cur.fetchone()[0]
        else:
            cur.execute("SELECT id FROM employees WHERE fio=? AND COALESCE(tbn,'')=COALESCE(?, '')", (fio, tbn))
            row = cur.fetchone()
            if row:
                cur.execute("UPDATE employees SET pos=?, department_id=? WHERE id=?", (pos, dep_id, row[0]))
                emp_id = row[0]
            else:
                cur.execute("INSERT INTO employees(fio, tbn, pos, department_id) VALUES (?,?,?,?)",
                            (fio, tbn, pos, dep_id))
                emp_id = cur.lastrowid
        conn.commit()
        return int(emp_id)
    finally:
        conn.close()

def find_object(code: Optional[str], address: str) -> int:
    conn = get_conn()
    try:
        cur = conn.cursor()
        if _db_kind() == "postgres":
            cur.execute("SELECT id FROM objects WHERE COALESCE(code,'')=COALESCE(%s,'') AND address=%s", (code, address))
            row = cur.fetchone()
            if row:
                obj_id = row[0]
            else:
                cur.execute("INSERT INTO objects(code, address) VALUES (%s,%s) RETURNING id", (code, address))
                obj_id = cur.fetchone()[0]
        else:
            cur.execute("SELECT id FROM objects WHERE COALESCE(code,'')=COALESCE(?, '') AND address=?", (code, address))
            row = cur.fetchone()
            if row:
                obj_id = row[0]
            else:
                cur.execute("INSERT INTO objects(code, address) VALUES (?,?)", (code, address))
                obj_id = cur.lastrowid
        conn.commit()
        return int(obj_id)
    finally:
        conn.close()

def get_employees_by_department(dep_name: Optional[str]) -> List[Dict[str, Any]]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        if _db_kind() == "postgres":
            if not dep_name or dep_name == "Все":
                cur.execute("""SELECT e.id, e.fio, e.tbn, e.pos, d.name as dep
                               FROM employees e LEFT JOIN departments d ON d.id=e.department_id
                               ORDER BY fio""")
            else:
                cur.execute("""SELECT e.id, e.fio, e.tbn, e.pos, d.name as dep
                               FROM employees e JOIN departments d ON d.id=e.department_id
                               WHERE d.name=%s ORDER BY fio""", (dep_name,))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, r)) for r in rows]
        else:
            if not dep_name or dep_name == "Все":
                cur.execute("""SELECT e.id, e.fio, e.tbn, e.pos, d.name as dep
                               FROM employees e LEFT JOIN departments d ON d.id=e.department_id
                               ORDER BY fio""")
            else:
                cur.execute("""SELECT e.id, e.fio, e.tbn, e.pos, d.name as dep
                               FROM employees e JOIN departments d ON d.id=e.department_id
                               WHERE d.name=? ORDER BY fio""", (dep_name,))
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()

def get_or_create_timesheet(object_id: int, dep_id: Optional[int], year: int, month: int) -> int:
    conn = get_conn()
    try:
        cur = conn.cursor()
        if _db_kind() == "postgres":
            cur.execute("""SELECT id FROM timesheets
                           WHERE object_id=%s AND COALESCE(department_id,-1)=COALESCE(%s,-1)
                             AND year=%s AND month=%s""", (object_id, dep_id, year, month))
            row = cur.fetchone()
            if row:
                ts_id = row[0]
            else:
                cur.execute("""INSERT INTO timesheets(object_id, department_id, year, month)
                               VALUES (%s,%s,%s,%s) RETURNING id""", (object_id, dep_id, year, month))
                ts_id = cur.fetchone()[0]
        else:
            cur.execute("""SELECT id FROM timesheets
                           WHERE object_id=? AND COALESCE(department_id,-1)=COALESCE(?, -1)
                             AND year=? AND month=?""", (object_id, dep_id, year, month))
            row = cur.fetchone()
            if row:
                ts_id = row[0]
            else:
                cur.execute("""INSERT INTO timesheets(object_id, department_id, year, month)
                               VALUES (?,?,?,?)""", (object_id, dep_id, year, month))
                ts_id = cur.lastrowid
        conn.commit()
        return int(ts_id)
    finally:
        conn.close()

def load_timesheet(object_code: Optional[str], object_address: str, dep_name: Optional[str],
                   year: int, month: int) -> List[Dict[str, Any]]:
    obj_id = find_object(object_code, object_address)
    dep_id = upsert_department(dep_name) if dep_name and dep_name != "Все" else None
    conn = get_conn()
    try:
        cur = conn.cursor()
        # найти timesheet
        if _db_kind() == "postgres":
            cur.execute("""SELECT id FROM timesheets
                           WHERE object_id=%s AND COALESCE(department_id,-1)=COALESCE(%s,-1)
                             AND year=%s AND month=%s""", (obj_id, dep_id, year, month))
        else:
            cur.execute("""SELECT id FROM timesheets
                           WHERE object_id=? AND COALESCE(department_id,-1)=COALESCE(?, -1)
                             AND year=? AND month=?""", (obj_id, dep_id, year, month))
        row = cur.fetchone()
        if not row:
            return []
        ts_id = row[0]
        # строки
        if _db_kind() == "postgres":
            cur.execute("""SELECT tr.id as row_id, e.fio, e.tbn
                           FROM timesheet_rows tr JOIN employees e ON e.id=tr.employee_id
                           WHERE tr.timesheet_id=%s ORDER BY e.fio""", (ts_id,))
        else:
            cur.execute("""SELECT tr.id as row_id, e.fio, e.tbn
                           FROM timesheet_rows tr JOIN employees e ON e.id=tr.employee_id
                           WHERE tr.timesheet_id=? ORDER BY e.fio""", (ts_id,))
        rows = cur.fetchall()
        result = []
        for r in rows:
            row_id, fio, tbn = r if _db_kind() == "postgres" else (r["row_id"], r["fio"], r["tbn"])
            if _db_kind() == "postgres":
                cur.execute("""SELECT day, base_hours, ot_day, ot_night
                               FROM timesheet_entries WHERE row_id=%s ORDER BY day""", (row_id,))
                ents = cur.fetchall()
                hours = [None]*31
                for d, base, od, on in ents:
                    s = None
                    if base is not None:
                        base_s = f"{float(base):.2f}".rstrip("0").rstrip(".").replace(".", ",")
                        if (od or on):
                            s = f"{base_s}({int(od or 0)}/{int(on or 0)})"
                        else:
                            s = base_s
                    hours[int(d)-1] = s
            else:
                cur2 = conn.cursor()
                cur2.execute("""SELECT day, base_hours, ot_day, ot_night
                                FROM timesheet_entries WHERE row_id=? ORDER BY day""", (row_id,))
                ents = cur2.fetchall()
                hours = [None]*31
                for e in ents:
                    d, base, od, on = e["day"], e["base_hours"], e["ot_day"], e["ot_night"]
                    s = None
                    if base is not None:
                        base_s = f"{float(base):.2f}".rstrip("0").rstrip(".").replace(".", ",")
                        if (od or on):
                            s = f"{base_s}({int(od or 0)}/{int(on or 0)})"
                        else:
                            s = base_s
                    hours[int(d)-1] = s
            result.append({"fio": fio, "tbn": tbn, "hours": hours})
        return result
    finally:
        conn.close()

def save_timesheet(object_code: Optional[str], object_address: str, dep_name: Optional[str],
                   year: int, month: int, model_rows: List[Dict[str, Any]]):
    obj_id = find_object(object_code, object_address)
    dep_id = upsert_department(dep_name) if dep_name and dep_name != "Все" else None
    ts_id = get_or_create_timesheet(obj_id, dep_id, year, month)
    conn = get_conn()
    try:
        cur = conn.cursor()
        # очистка старых данных табеля
        if _db_kind() == "postgres":
            cur.execute("DELETE FROM timesheet_entries WHERE row_id IN (SELECT id FROM timesheet_rows WHERE timesheet_id=%s)", (ts_id,))
            cur.execute("DELETE FROM timesheet_rows WHERE timesheet_id=%s", (ts_id,))
        else:
            cur.execute("DELETE FROM timesheet_entries WHERE row_id IN (SELECT id FROM timesheet_rows WHERE timesheet_id=?)", (ts_id,))
            cur.execute("DELETE FROM timesheet_rows WHERE timesheet_id=?", (ts_id,))
        # запись новых строк
        for rec in model_rows:
            emp_id = upsert_employee(rec["fio"], rec.get("tbn",""), "", dep_name or None)
            if _db_kind() == "postgres":
                cur.execute("INSERT INTO timesheet_rows(timesheet_id, employee_id) VALUES (%s,%s) RETURNING id", (ts_id, emp_id))
                row_id = cur.fetchone()[0]
            else:
                cur.execute("INSERT INTO timesheet_rows(timesheet_id, employee_id) VALUES (?,?)", (ts_id, emp_id))
                row_id = cur.lastrowid
            for idx, raw in enumerate(rec.get("hours") or []):
                if not raw:
                    continue
                base = None
                ot_day = None
                ot_night = None
                try:
                    base = float(str(raw).split("(")[0].replace(",", "."))
                except Exception:
                    base = None
                if "(" in str(raw) and ")" in str(raw):
                    ins = str(raw)[str(raw).find("(")+1:str(raw).find(")")]
                    parts = ins.split("/")
                    try:
                        ot_day = float(parts[0].replace(",", ".")) if parts and parts[0].strip() else None
                    except Exception:
                        ot_day = None
                    try:
                        ot_night = float(parts[1].replace(",", ".")) if len(parts)>1 and parts[1].strip() else None
                    except Exception:
                        ot_night = None
                if _db_kind() == "postgres":
                    cur.execute("""INSERT INTO timesheet_entries(row_id, day, base_hours, ot_day, ot_night)
                                   VALUES (%s,%s,%s,%s,%s)""", (row_id, idx+1, base, ot_day, ot_night))
                else:
                    cur.execute("""INSERT INTO timesheet_entries(row_id, day, base_hours, ot_day, ot_night)
                                   VALUES (?,?,?,?,?)""", (row_id, idx+1, base, ot_day, ot_night))
        conn.commit()
    finally:
        conn.close()
