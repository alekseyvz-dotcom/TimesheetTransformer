import os
import re
import sys
import csv
import json
import configparser
import urllib.request
import urllib.error
import urllib.parse
from io import BytesIO
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
from datetime import datetime, date, timedelta

# ========================= БАЗОВЫЕ КОНСТАНТЫ =========================

APP_TITLE = "Заказ питания"

# Конфигурация
CONFIG_FILE = "tabel_config.ini"
CONFIG_SECTION_PATHS = "Paths"
CONFIG_SECTION_UI = "UI"
CONFIG_SECTION_INTEGR = "Integrations"
CONFIG_SECTION_REMOTE = "Remote"

KEY_SPR = "spravochnik_path"
KEY_SELECTED_DEP = "selected_department"

KEY_MEALS_MODE = "meals_mode"
KEY_MEALS_WEBHOOK_URL = "meals_webhook_url"
KEY_MEALS_WEBHOOK_TOKEN = "meals_webhook_token"
KEY_MEALS_PLANNING_ENABLED = "meals_planning_enabled"
KEY_MEALS_PLANNING_PASSWORD = "meals_planning_password"

KEY_REMOTE_USE = "use_remote"
KEY_YA_PUBLIC_LINK = "yadisk_public_link"
KEY_YA_PUBLIC_PATH = "yadisk_public_path"

SPRAVOCHNIK_FILE = "Справочник.xlsx"
ORDERS_DIR = "Заявки_питание"


def exe_dir() -> Path:
    """Каталог, откуда запущена программа/скрипт."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def config_path() -> Path:
    """Путь к ini‑конфигу для fallback‑режима (без settings_manager)."""
    return exe_dir() / CONFIG_FILE


# ========================= РАБОТА С НАСТРОЙКАМИ =========================

# Мягкий импорт менеджера настроек
try:
    import settings_manager as Settings
except Exception:
    Settings = None

if Settings:
    # Используем централизованный менеджер настроек
    ensure_config = Settings.ensure_config
    read_config = Settings.read_config
    write_config = Settings.write_config

    def get_spr_path() -> Path:
        return Settings.get_spr_path_from_config()

    def get_saved_dep() -> str:
        return Settings.get_selected_department_from_config()

    def set_saved_dep(dep: str):
        return Settings.set_selected_department_in_config(dep)

    def get_meals_planning_password() -> str:
        cfg = read_config()
        return cfg.get(CONFIG_SECTION_INTEGR, KEY_MEALS_PLANNING_PASSWORD, fallback="2025").strip()

else:
    # Локальный (старый) способ хранения настроек в INI

    def ensure_config():
        cp = config_path()
        if cp.exists():
            cfg = configparser.ConfigParser()
            cfg.read(cp, encoding="utf-8")
            changed = False

            if not cfg.has_section(CONFIG_SECTION_PATHS):
                cfg[CONFIG_SECTION_PATHS] = {}
                changed = True
            if KEY_SPR not in cfg[CONFIG_SECTION_PATHS]:
                cfg[CONFIG_SECTION_PATHS][KEY_SPR] = str(exe_dir() / SPRAVOCHNIK_FILE)
                changed = True

            if not cfg.has_section(CONFIG_SECTION_UI):
                cfg[CONFIG_SECTION_UI] = {}
                changed = True
            if KEY_SELECTED_DEP not in cfg[CONFIG_SECTION_UI]:
                cfg[CONFIG_SECTION_UI][KEY_SELECTED_DEP] = "Все"
                changed = True

            if not cfg.has_section(CONFIG_SECTION_INTEGR):
                cfg[CONFIG_SECTION_INTEGR] = {}
                changed = True
            if KEY_MEALS_MODE not in cfg[CONFIG_SECTION_INTEGR]:
                cfg[CONFIG_SECTION_INTEGR][KEY_MEALS_MODE] = "webhook"
                changed = True
            if KEY_MEALS_WEBHOOK_URL not in cfg[CONFIG_SECTION_INTEGR]:
                cfg[CONFIG_SECTION_INTEGR][KEY_MEALS_WEBHOOK_URL] = ""
                changed = True
            if KEY_MEALS_WEBHOOK_TOKEN not in cfg[CONFIG_SECTION_INTEGR]:
                cfg[CONFIG_SECTION_INTEGR][KEY_MEALS_WEBHOOK_TOKEN] = ""
                changed = True
            if KEY_MEALS_PLANNING_ENABLED not in cfg[CONFIG_SECTION_INTEGR]:
                cfg[CONFIG_SECTION_INTEGR][KEY_MEALS_PLANNING_ENABLED] = "true"
                changed = True
            if KEY_MEALS_PLANNING_PASSWORD not in cfg[CONFIG_SECTION_INTEGR]:
                cfg[CONFIG_SECTION_INTEGR][KEY_MEALS_PLANNING_PASSWORD] = "2025"
                changed = True

            if not cfg.has_section(CONFIG_SECTION_REMOTE):
                cfg[CONFIG_SECTION_REMOTE] = {}
                changed = True
            if KEY_REMOTE_USE not in cfg[CONFIG_SECTION_REMOTE]:
                cfg[CONFIG_SECTION_REMOTE][KEY_REMOTE_USE] = "false"
                changed = True
            if KEY_YA_PUBLIC_LINK not in cfg[CONFIG_SECTION_REMOTE]:
                cfg[CONFIG_SECTION_REMOTE][KEY_YA_PUBLIC_LINK] = ""
                changed = True
            if KEY_YA_PUBLIC_PATH not in cfg[CONFIG_SECTION_REMOTE]:
                cfg[CONFIG_SECTION_REMOTE][KEY_YA_PUBLIC_PATH] = ""
                changed = True

            if changed:
                with open(cp, "w", encoding="utf-8") as f:
                    cfg.write(f)
            return

        # если ini отсутствует — создаём дефолтный
        cfg = configparser.ConfigParser()
        cfg[CONFIG_SECTION_PATHS] = {KEY_SPR: str(exe_dir() / SPRAVOCHNIK_FILE)}
        cfg[CONFIG_SECTION_UI] = {KEY_SELECTED_DEP: "Все"}
        cfg[CONFIG_SECTION_INTEGR] = {
            KEY_MEALS_MODE: "webhook",
            KEY_MEALS_WEBHOOK_URL: "",
            KEY_MEALS_WEBHOOK_TOKEN: "",
            KEY_MEALS_PLANNING_ENABLED: "true",
            KEY_MEALS_PLANNING_PASSWORD: "2025",
        }
        cfg[CONFIG_SECTION_REMOTE] = {
            KEY_REMOTE_USE: "false",
            KEY_YA_PUBLIC_LINK: "",
            KEY_YA_PUBLIC_PATH: "",
        }
        with open(cp, "w", encoding="utf-8") as f:
            cfg.write(f)

    def read_config() -> configparser.ConfigParser:
        ensure_config()
        cfg = configparser.ConfigParser()
        cfg.read(config_path(), encoding="utf-8")
        return cfg

    def write_config(cfg: configparser.ConfigParser):
        with open(config_path(), "w", encoding="utf-8") as f:
            cfg.write(f)

    def get_spr_path() -> Path:
        cfg = read_config()
        raw = cfg.get(
            CONFIG_SECTION_PATHS,
            KEY_SPR,
            fallback=str(exe_dir() / SPRAVOCHNIK_FILE),
        )
        return Path(os.path.expandvars(raw))

    def get_saved_dep() -> str:
        cfg = read_config()
        return cfg.get(CONFIG_SECTION_UI, KEY_SELECTED_DEP, fallback="Все")

    def set_saved_dep(dep: str):
        cfg = read_config()
        if not cfg.has_section(CONFIG_SECTION_UI):
            cfg[CONFIG_SECTION_UI] = {}
        cfg[CONFIG_SECTION_UI][KEY_SELECTED_DEP] = dep or "Все"
        write_config(cfg)

    def get_meals_planning_password() -> str:
        cfg = read_config()
        return cfg.get(CONFIG_SECTION_INTEGR, KEY_MEALS_PLANNING_PASSWORD, fallback="2025").strip()


def get_meals_planning_enabled() -> bool:
    cfg = read_config()
    v = cfg.get(CONFIG_SECTION_INTEGR, KEY_MEALS_PLANNING_ENABLED, fallback="true").strip().lower()
    return v in ("1", "true", "yes", "on")


def get_meals_mode() -> str:
    cfg = read_config()
    return cfg.get(CONFIG_SECTION_INTEGR, KEY_MEALS_MODE, fallback="webhook").strip().lower()


def get_meals_webhook_url() -> str:
    cfg = read_config()
    return cfg.get(CONFIG_SECTION_INTEGR, KEY_MEALS_WEBHOOK_URL, fallback="").strip()


def get_meals_webhook_token() -> str:
    cfg = read_config()
    return cfg.get(CONFIG_SECTION_INTEGR, KEY_MEALS_WEBHOOK_TOKEN, fallback="").strip()

# ========================= ЗАГРУЗКА СПРАВОЧНИКА =========================

def ensure_spravochnik(path: Path):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    if path.exists():
        return
    wb = Workbook()
    ws1 = wb.active
    ws1.title = "Сотрудники"
    ws1.append(["ФИО", "Табельный №", "Должность", "Подразделение"])
    ws1.append(["Иванов И. И.", "ST00-00001", "Слесарь", "Монтаж"])
    ws2 = wb.create_sheet("Объекты")
    ws2.append(["ID объекта", "Адрес"])
    ws2.append(["OBJ-001", "ул. Пушкина, д. 1"])
    ws3 = wb.create_sheet("Типы питания")
    ws3.append(["Тип питания"])
    ws3.append(["Одноразовое"])
    ws3.append(["Двухразовое"])
    ws3.append(["Трехразовое"])
    wb.save(path)

def fetch_yadisk_public_bytes(public_link: str, public_path: str = "") -> bytes:
    if not public_link:
        raise RuntimeError("Не задана публичная ссылка Я.Диска")
    api = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
    params = {"public_key": public_link}
    if public_path:
        params["path"] = public_path
    url = api + "?" + urllib.parse.urlencode(params, safe="/")
    with urllib.request.urlopen(url, timeout=15) as r:
        meta = json.loads(r.read().decode("utf-8", errors="replace"))
    href = meta.get("href")
    if not href:
        raise RuntimeError(f"Я.Диск не вернул href: {meta}")
    with urllib.request.urlopen(href, timeout=60) as f:
        return f.read()

def _s(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    return str(v).strip()

def load_spravochnik_from_wb(wb) -> Tuple[List[Tuple[str, str, str, str]], List[Tuple[str, str]], List[str]]:
    employees: List[Tuple[str, str, str, str]] = []
    objects: List[Tuple[str, str]] = []
    meal_types: List[str] = []

    if "Сотрудники" in wb.sheetnames:
        ws = wb["Сотрудники"]
        hdr = [_s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        have_pos = ("должность" in hdr) or (len(hdr) >= 3)
        have_dep = ("подразделение" in hdr) or (len(hdr) >= 4)
        for r in ws.iter_rows(min_row=2, values_only=True):
            fio = _s(r[0] if r and len(r) > 0 else "")
            tbn = _s(r[1] if r and len(r) > 1 else "")
            pos = _s(r[2] if have_pos and r and len(r) > 2 else "")
            dep = _s(r[3] if have_dep and r and len(r) > 3 else "")
            if fio:
                employees.append((fio, tbn, pos, dep))

    if "Объекты" in wb.sheetnames:
        ws = wb["Объекты"]
        hdr = [_s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        have_two = ("id объекта" in hdr) or (len(hdr) >= 2)
        for r in ws.iter_rows(min_row=2, values_only=True):
            if have_two:
                oid = _s(r[0] if r and len(r) > 0 else "")
                addr = _s(r[1] if r and len(r) > 1 else "")
            else:
                oid = ""
                addr = _s(r[0] if r and len(r) > 0 else "")
            if oid or addr:
                objects.append((oid, addr))

    if "Типы питания" in wb.sheetnames:
        ws = wb["Типы питания"]
        for r in ws.iter_rows(min_row=2, values_only=True):
            meal_type = _s(r[0] if r and len(r) > 0 else "")
            if meal_type:
                meal_types.append(meal_type)

    return employees, objects, meal_types

def load_spravochnik_remote_or_local(local_path: Path):
    cfg = read_config()
    use_remote = cfg.get(CONFIG_SECTION_REMOTE, KEY_REMOTE_USE, fallback="false").strip().lower() in ("1", "true", "yes", "on")
    if use_remote:
        try:
            public_link = cfg.get(CONFIG_SECTION_REMOTE, KEY_YA_PUBLIC_LINK, fallback="").strip()
            public_path = cfg.get(CONFIG_SECTION_REMOTE, KEY_YA_PUBLIC_PATH, fallback="").strip()
            raw = fetch_yadisk_public_bytes(public_link, public_path)
            wb = load_workbook(BytesIO(raw), read_only=True, data_only=True)
            return load_spravochnik_from_wb(wb)
        except Exception as e:
            print(f"[Remote YaDisk] ошибка: {e}")
            if local_path.exists():
                wb = load_workbook(local_path, read_only=True, data_only=True)
                return load_spravochnik_from_wb(wb)
            return [], [], []
    ensure_spravochnik(local_path)
    wb = load_workbook(local_path, read_only=True, data_only=True)
    return load_spravochnik_from_wb(wb)

# ========================= УТИЛИТЫ =========================

def parse_date_any(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    return None

def post_json(url: str, payload: dict, token: str = '') -> Tuple[bool, str]:
    try:
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        if token:
            sep = '&' if ('?' in url) else '?'
            url = f"{url}{sep}token={urllib.parse.quote(token)}"
        req = urllib.request.Request(url, data=body, headers={'Content-Type': 'application/json; charset=utf-8'}, method='POST')
        with urllib.request.urlopen(req, timeout=12) as resp:
            code = resp.getcode()
            text = resp.read().decode('utf-8', errors='replace')
            return (200 <= code < 300, f"{code}: {text}")
    except urllib.error.HTTPError as e:
        try:
            txt = e.read().decode('utf-8', errors='replace')
        except Exception:
            txt = str(e)
        return (False, f"HTTPError {e.code}: {txt}")
    except Exception as e:
        return (False, f"Error: {e}")

def safe_filename(s: str, maxlen: int = 60) -> str:
    if not s:
        return "NOID"
    s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", str(s)).strip()
    s = re.sub(r"_+", "_", s)
    return s[:maxlen] if len(s) > maxlen else s

# ========================= ВИДЖЕТЫ =========================

class AutoCompleteCombobox(ttk.Combobox):
    def __init__(self, master=None, **kw):
        super().__init__(master, **kw)
        self._all_values: List[str] = []
        self.bind("<KeyRelease>", self._on_keyrelease)
        self.bind("<Control-BackSpace>", self._clear_all)

    def set_completion_list(self, values: List[str]):
        self._all_values = list(values)
        self['values'] = self._all_values

    def _clear_all(self, _=None):
        self.delete(0, tk.END)
        self['values'] = self._all_values

    def _on_keyrelease(self, event):
        if event.keysym in ("Up", "Down", "Left", "Right", "Home", "End", "Return", "Escape", "Tab"):
            return
        typed = self.get().strip()
        if not typed:
            self['values'] = self._all_values
            return
        self['values'] = [x for x in self._all_values if typed.lower() in x.lower()]

# ========================= СТРОКА СОТРУДНИКА =========================

class EmployeeRow:
    ERR_BG = "#ffccbc"
    ZEBRA_EVEN = "#ffffff"
    ZEBRA_ODD = "#f6f8fa"

    def __init__(self, parent, idx: int, emp_names: List[str], meal_types: List[str], on_delete):
        self.parent = parent
        self.idx = idx
        self.on_delete = on_delete
        self.emp_names = emp_names
        self.meal_types = meal_types
        self.frame = tk.Frame(parent)

        self.fio_var = tk.StringVar()
        self.cmb_fio = AutoCompleteCombobox(self.frame, textvariable=self.fio_var, width=38)
        self.cmb_fio.set_completion_list(emp_names)
        self.cmb_fio.grid(row=0, column=0, padx=2, pady=1, sticky="w")

        self.cmb_meal_type = ttk.Combobox(self.frame, values=meal_types, state="readonly", width=18)
        if meal_types:
            self.cmb_meal_type.set(meal_types[0])
        self.cmb_meal_type.grid(row=0, column=1, padx=2)

        self.ent_comment = ttk.Entry(self.frame, width=40)
        self.ent_comment.grid(row=0, column=2, padx=2, sticky="w")

        self.btn_del = ttk.Button(self.frame, text="Удалить", width=9, command=self._delete)
        self.btn_del.grid(row=0, column=3, padx=2)

        for i in range(4):
            self.frame.grid_columnconfigure(i, minsize=[320, 160, 340, 80][i])

    def grid(self, row: int):
        self.frame.grid(row=row, column=0, sticky="w")

    def destroy(self):
        self.frame.destroy()

    def apply_zebra(self, row0: int):
        bg = self.ZEBRA_ODD if (row0 % 2 == 1) else self.ZEBRA_EVEN
        for w in (self.cmb_fio, self.cmb_meal_type, self.ent_comment):
            try:
                w.configure(background=bg)
            except Exception:
                pass

    def _delete(self):
        self.on_delete(self)

    def validate(self) -> bool:
        ok = True
        fio = (self.cmb_fio.get() or "").strip()
        if not fio:
            self._mark_err(self.cmb_fio)
            ok = False
        else:
            self._clear_err(self.cmb_fio)
        meal_type = (self.cmb_meal_type.get() or "").strip()
        if not meal_type:
            self._mark_err(self.cmb_meal_type)
            ok = False
        else:
            self._clear_err(self.cmb_meal_type)
        return ok

    def _mark_err(self, widget):
        try:
            widget.configure(background=self.ERR_BG)
        except Exception:
            pass

    def _clear_err(self, widget):
        try:
            widget.configure(background="white")
        except Exception:
            pass

    def get_dict(self) -> Dict:
        return {
            "fio": (self.cmb_fio.get() or "").strip(),
            "meal_type": (self.cmb_meal_type.get() or "").strip(),
            "comment": (self.ent_comment.get() or "").strip(),
        }

# ========================= СТРАНИЦА СОЗДАНИЯ ЗАЯВКИ =========================

class MealOrderPage(tk.Frame):
    """Страница для создания заявок на питание"""

    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        ensure_config()
        self.base_dir = exe_dir()
        self.spr_path = get_spr_path()
        self.orders_dir = self.base_dir / ORDERS_DIR
        self.orders_dir.mkdir(parents=True, exist_ok=True)
        self._load_spr()
        self._build_ui()

    def _load_spr(self):
        employees, objects, meal_types = load_spravochnik_remote_or_local(self.spr_path)
        self.emps = [{'fio': fio, 'tbn': tbn, 'pos': pos, 'dep': dep} for (fio, tbn, pos, dep) in employees]
        self.objects = objects
        self.meal_types = meal_types if meal_types else ["Одноразовое", "Двухразовое", "Трехразовое"]
        self.deps = ["Все"] + sorted({(r['dep'] or "").strip() for r in self.emps if (r['dep'] or "").strip()})
        self.emp_names_all = [r['fio'] for r in self.emps]
        self.addr_to_ids = {}
        for oid, addr in self.objects:
            if not addr:
                continue
            self.addr_to_ids.setdefault(addr, [])
            if oid and oid not in self.addr_to_ids[addr]:
                self.addr_to_ids[addr].append(oid)
        addresses_set = set(self.addr_to_ids.keys())
        addresses_set.update(addr for _, addr in self.objects if addr)
        self.addresses = sorted(addresses_set)

    def _build_ui(self):
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(fill="x", padx=10, pady=8)

        # Дата
        tk.Label(top, text="Дата заказа*:", bg="#f7f7f7").grid(row=0, column=0, sticky="w")
        self.ent_date = ttk.Entry(top, width=12)
        self.ent_date.grid(row=0, column=1, sticky="w", padx=(4, 12))
        self.ent_date.insert(0, (date.today() + timedelta(days=1)).strftime("%Y-%m-%d"))
        self.ent_date.bind("<KeyRelease>", lambda e: self._update_date_hint())
        self.ent_date.bind("<FocusOut>", lambda e: self._update_date_hint())

        # Подразделение
        tk.Label(top, text="Подразделение*:", bg="#f7f7f7").grid(row=0, column=2, sticky="w")
        self.cmb_dep = ttk.Combobox(top, state="readonly", values=self.deps, width=30)
        saved_dep = get_saved_dep()
        self.cmb_dep.set(saved_dep if saved_dep in self.deps else self.deps[0])
        self.cmb_dep.grid(row=0, column=3, sticky="w", padx=(4, 12))
        self.cmb_dep.bind(
            "<<ComboboxSelected>>",
            lambda e: (set_saved_dep(self.cmb_dep.get()), self._update_emp_list())
        )

        # Наименование бригады
        tk.Label(top, text="Наименование бригады:", bg="#f7f7f7").grid(row=0, column=4, sticky="w", padx=(12, 4))
        self.ent_team = ttk.Entry(top, width=30)
        self.ent_team.grid(row=0, column=5, sticky="we", padx=(0, 4))

        # Адрес объекта
        tk.Label(top, text="Адрес объекта*:", bg="#f7f7f7").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.cmb_address = AutoCompleteCombobox(top, width=56)
        self.cmb_address.set_completion_list(self.addresses)
        self.cmb_address.grid(row=1, column=1, columnspan=2, sticky="we", padx=(4, 12), pady=(8, 0))
        self.cmb_address.bind("<<ComboboxSelected>>", lambda e: self._sync_ids_by_address())
        self.cmb_address.bind("<FocusOut>", lambda e: self._sync_ids_by_address())
        self.cmb_address.bind("<Return>", lambda e: self._sync_ids_by_address())

        # ID объекта
        tk.Label(top, text="ID объекта:", bg="#f7f7f7").grid(
            row=1, column=3, sticky="e", pady=(8, 0), padx=(0, 4)
        )
        self.cmb_object_id = ttk.Combobox(top, state="readonly", values=[], width=20)
        self.cmb_object_id.grid(row=1, column=4, sticky="w", padx=(4, 0), pady=(8, 0))

        # Подсказка по дате
        self.lbl_date_hint = tk.Label(top, text="", fg="#555", bg="#f7f7f7")
        self.lbl_date_hint.grid(row=1, column=5, sticky="w", padx=(12, 0))

        # ------- блок сотрудников -------
        emp_wrap = tk.LabelFrame(self, text="Сотрудники")
        emp_wrap.pack(fill="both", expand=True, padx=10, pady=(6, 8))

        hdr = tk.Frame(emp_wrap)
        hdr.pack(fill="x")
        tk.Label(hdr, text="ФИО сотрудника*", width=42, anchor="w").grid(row=0, column=0, padx=2)
        tk.Label(hdr, text="Тип питания*", width=20, anchor="w").grid(row=0, column=1, padx=2)
        tk.Label(hdr, text="Комментарий", width=44, anchor="w").grid(row=0, column=2, padx=2)
        tk.Label(hdr, text="Действие", width=10, anchor="center").grid(row=0, column=3, padx=2)

        wrap = tk.Frame(emp_wrap)
        wrap.pack(fill="both", expand=True)
        self.cv = tk.Canvas(wrap, borderwidth=0, highlightthickness=0)
        self.rows_holder = tk.Frame(self.cv)
        self.cv.create_window((0, 0), window=self.rows_holder, anchor="nw")
        self.cv.pack(side="left", fill="both", expand=True)
        self.vscroll = ttk.Scrollbar(wrap, orient="vertical", command=self.cv.yview)
        self.vscroll.pack(side="right", fill="y")
        self.cv.configure(yscrollcommand=self.vscroll.set)
        self.rows_holder.bind("<Configure>", lambda e: self.cv.configure(scrollregion=self.cv.bbox("all")))
        self.cv.bind("<MouseWheel>", lambda e: (self.cv.yview_scroll(int(-1 * (e.delta / 120)), "units"), "break"))

        self.emp_rows: List[EmployeeRow] = []
        btns = tk.Frame(emp_wrap)
        btns.pack(fill="x")
        ttk.Button(btns, text="Добавить сотрудника", command=self.add_employee).pack(side="left", padx=2, pady=4)
        ttk.Button(btns, text="Добавить подразделение", command=self.add_department).pack(side="left", padx=4, pady=4)

        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(bottom, text="Сохранить заявку", command=self.save_order).pack(side="left", padx=4)
        ttk.Button(bottom, text="Очистить форму", command=self.clear_form).pack(side="left", padx=4)
        ttk.Button(bottom, text="Открыть папку заявок", command=self.open_orders_dir).pack(side="left", padx=4)

        for c in range(6):
            top.grid_columnconfigure(c, weight=0)
        top.grid_columnconfigure(1, weight=1)  # адрес
        top.grid_columnconfigure(5, weight=1)  # бригада

        self._update_emp_list()
        self._update_date_hint()
        self.add_employee()

    def _update_emp_list(self):
        dep = (self.cmb_dep.get() or "Все").strip()
        if dep == "Все":
            names = [r['fio'] for r in self.emps]
        else:
            names = [r['fio'] for r in self.emps if (r['dep'] or "") == dep]
        seen, filtered = set(), []
        for n in names:
            if n not in seen:
                seen.add(n)
                filtered.append(n)
        if not filtered and dep != "Все":
            filtered = [r['fio'] for r in self.emps]
        for row in self.emp_rows:
            row.cmb_fio.set_completion_list(filtered)

    def _update_date_hint(self):
        try:
            req = parse_date_any(self.ent_date.get())
            today = date.today()
            if req is None:
                self.lbl_date_hint.config(
                    text="Укажите дату в формате YYYY-MM-DD или DD.MM.YYYY",
                    fg="#b00020"
                )
            elif req < today:
                self.lbl_date_hint.config(
                    text="Дата не может быть в прошлом",
                    fg="#b00020"
                )
            else:
                self.lbl_date_hint.config(
                    text="Ок: заявка на выбранную дату",
                    fg="#2e7d32"
                )
        except Exception:
            self.lbl_date_hint.config(text="", fg="#555")

    def _sync_ids_by_address(self):
        addr = (self.cmb_address.get() or "").strip()
        ids = sorted(self.addr_to_ids.get(addr, []))
        if ids:
            self.cmb_object_id.config(state="readonly", values=ids)
            if self.cmb_object_id.get() not in ids:
                self.cmb_object_id.set(ids[0])
        else:
            self.cmb_object_id.config(state="normal", values=[])
            self.cmb_object_id.set("")

    def add_employee(self):
        dep = (self.cmb_dep.get() or "Все").strip()
        if dep == "Все":
            names = [r['fio'] for r in self.emps]
        else:
            names = [r['fio'] for r in self.emps if (r['dep'] or "") == dep]
        seen, filtered = set(), []
        for n in names:
            if n not in seen:
                seen.add(n)
                filtered.append(n)
        row = EmployeeRow(self.rows_holder, len(self.emp_rows) + 1, filtered, self.meal_types, self.delete_employee)
        row.grid(len(self.emp_rows))
        row.apply_zebra(len(self.emp_rows))
        self.emp_rows.append(row)

    def delete_employee(self, emp_row: EmployeeRow):
        try:
            self.emp_rows.remove(emp_row)
        except Exception:
            pass
        emp_row.destroy()
        for i, r in enumerate(self.emp_rows, start=0):
            r.grid(i)
            r.apply_zebra(i)

    def _validate_form(self) -> bool:
        req = parse_date_any(self.ent_date.get())
        today = date.today()
        if req is None or req < today:
            messagebox.showwarning("Заявка", "Дата должна быть сегодня или позже.")
            return False
        if not (self.cmb_dep.get() or "").strip():
            messagebox.showwarning("Заявка", "Выберите Подразделение.")
            return False
        addr = (self.cmb_address.get() or "").strip()
        if not addr:
            messagebox.showwarning("Заявка", "Укажите Адрес объекта.")
            return False
        if not self.emp_rows:
            messagebox.showwarning("Заявка", "Добавьте хотя бы одного сотрудника.")
            return False
        all_ok = True
        for r in self.emp_rows:
            all_ok = r.validate() and all_ok
        if not all_ok:
            messagebox.showwarning("Заявка", "Исправьте подсвеченные поля (ФИО и Тип питания обязательны).")
            return False
        return True

    def _build_order_dict(self) -> Dict:
        created_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        req_date = parse_date_any(self.ent_date.get()) or date.today()
        addr = (self.cmb_address.get() or "").strip()
        oid = (self.cmb_object_id.get() or "").strip()
        employees = [r.get_dict() for r in self.emp_rows]
        return {
            "created_at": created_at,
            "date": req_date.strftime("%Y-%m-%d"),
            "department": (self.cmb_dep.get() or "").strip(),
            "team_name": (self.ent_team.get() or "").strip(),
            "object": {"id": oid, "address": addr},
            "employees": employees,
        }

    def save_order(self):
        if not self._validate_form():
            return
        data = self._build_order_dict()
        ts = datetime.now().strftime("%H%M%S")
        id_part = data["object"]["id"] or safe_filename(data["object"]["address"])
        fname = f"Заявка_питание_{data['date']}_{ts}_{id_part or 'NOID'}.xlsx"
        fpath = self.orders_dir / fname

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Заявка"
            ws.append(["Создано", data["created_at"]])
            ws.append(["Дата", data["date"]])
            ws.append(["Подразделение", data["department"]])
            ws.append(["Наименование бригады", data.get("team_name", "")])
            ws.append(["ID объекта", data["object"]["id"]])
            ws.append(["Адрес", data["object"]["address"]])
            ws.append([])
            hdr = ["#", "ФИО", "Тип питания", "Комментарий"]
            ws.append(hdr)
            for i, emp in enumerate(data["employees"], start=1):
                ws.append([i, emp["fio"], emp["meal_type"], emp["comment"]])
            for col, w in enumerate([4, 40, 20, 40], start=1):
                ws.column_dimensions[get_column_letter(col)].width = w
            ws.freeze_panes = "A8"
            wb.save(fpath)
        except Exception as e:
            messagebox.showerror("Сохранение", f"Не удалось сохранить XLSX:\n{e}")
            return

        csv_path = self.orders_dir / f"Свод_питание_{data['date'][:7].replace('-', '_')}.csv"
        try:
            new = not csv_path.exists()
            with open(csv_path, "a", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f, delimiter=";")
                if new:
                    w.writerow([
                        "Создано", "Дата", "Подразделение", "Наименование бригады",
                        "ID объекта", "Адрес", "ФИО", "Тип питания", "Комментарий"
                    ])
                for emp in data["employees"]:
                    w.writerow([
                        data["created_at"], data["date"], data["department"], data.get("team_name", ""),
                        data["object"]["id"], data["object"]["address"],
                        emp["fio"], emp["meal_type"], emp["comment"]
                    ])
        except Exception as e:
            messagebox.showwarning("Сводный CSV", f"XLSX сохранён, но не удалось добавить в CSV:\n{e}")

        try:
            mode = get_meals_mode()
            if mode == 'webhook':
                url = get_meals_webhook_url()
                token = get_meals_webhook_token()
                if url:
                    ok, info = post_json(url, data, token)
                    if ok:
                        messagebox.showinfo(
                            "Сохранение/Отправка",
                            f"Заявка сохранена и отправлена онлайн.\n\nXLSX:\n{fpath}\nCSV:\n{csv_path}\n\nОтвет сервера:\n{info}"
                        )
                    else:
                        messagebox.showwarning(
                            "Сохранение/Отправка",
                            f"Локально сохранено, но онлайн-отправка не удалась.\n\nXLSX:\n{fpath}\nCSV:\n{csv_path}\n\n{info}"
                        )
                    return
                else:
                    messagebox.showinfo(
                        "Сохранение",
                        f"Заявка сохранена:\n{fpath}\n\nСводный CSV:\n{csv_path}\n(Онлайн-отправка не настроена)"
                    )
                    return
            else:
                messagebox.showinfo(
                    "Сохранение",
                    f"Заявка сохранена:\n{fpath}\n\nСводный CSV:\n{csv_path}"
                )
                return
        except Exception as e:
            messagebox.showwarning(
                "Сохранение/Отправка",
                f"Локально сохранено, но онлайн-отправка упала с ошибкой:\n{e}\n\nXLSX:\n{fpath}\nCSV:\n{csv_path}"
            )
            return

    def clear_form(self):
        self.ent_date.delete(0, "end")
        self.ent_date.insert(0, (date.today() + timedelta(days=1)).strftime("%Y-%m-%d"))
        self.cmb_address.set("")
        self.cmb_object_id.config(values=[])
        self.cmb_object_id.set("")
        self.ent_team.delete(0, "end")
        for r in self.emp_rows:
            r.destroy()
        self.emp_rows.clear()
        self.add_employee()
        self._update_date_hint()

    def add_department(self):
        """Добавить всех сотрудников выбранного подразделения в заявку"""
        dep = (self.cmb_dep.get() or "Все").strip()

        if dep == "Все":
            candidates = self.emps[:]  # все сотрудники
        else:
            candidates = [e for e in self.emps if (e['dep'] or "") == dep]

        if not candidates:
            messagebox.showinfo("Питание", f"В подразделении «{dep}» нет сотрудников.")
            return

        existing_fio = {row.cmb_fio.get().strip() for row in self.emp_rows if row.cmb_fio.get().strip()}
        added = 0

        for e in candidates:
            fio = e['fio']
            if fio in existing_fio:
                continue
            row = EmployeeRow(self.rows_holder, len(self.emp_rows) + 1, [], self.meal_types, self.delete_employee)
            row.grid(len(self.emp_rows))
            row.apply_zebra(len(self.emp_rows))
            row.fio_var.set(fio)
            self.emp_rows.append(row)
            existing_fio.add(fio)
            added += 1

        self._update_emp_list()
        messagebox.showinfo("Питание", f"Добавлено сотрудников: {added}")

    def open_orders_dir(self):
        try:
            os.startfile(self.orders_dir)
        except Exception as e:
            messagebox.showerror("Папка", f"Не удалось открыть папку:\n{e}")

# ========================= СТРАНИЦА ПЛАНИРОВАНИЯ ПИТАНИЯ =========================

class MealPlanningPage(tk.Frame):
    """Страница планирования питания"""

    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        self.spr_path = get_spr_path()
        self.authenticated = False
        self.row_meta: Dict[str, Dict[str, Any]] = {}

        if not self._check_password():
            self._show_access_denied()
            return

        self.authenticated = True
        self._load_spr()
        self._build_ui()

    def _check_password(self) -> bool:
        required_password = get_meals_planning_password()
        if not required_password:
            return True
        pwd = simpledialog.askstring("Планирование питания", "Введите пароль для доступа:", show="*", parent=self)
        if pwd is None:
            return False
        if pwd != required_password:
            messagebox.showerror("Доступ запрещён", "Неверный пароль.", parent=self)
            return False
        return True

    def _show_access_denied(self):
        container = tk.Frame(self, bg="#f7f7f7")
        container.place(relx=0.5, rely=0.5, anchor="center")
        tk.Label(container, text="Доступ запрещён", font=("Segoe UI", 18, "bold"),
                 bg="#f7f7f7", fg="#666").pack(pady=(0, 10))
        tk.Label(container, text="Для просмотра этого раздела требуется пароль",
                 font=("Segoe UI", 10), bg="#f7f7f7", fg="#888").pack()

    def _load_spr(self):
        employees, objects, meal_types = load_spravochnik_remote_or_local(self.spr_path)
        self.emps = [{'fio': fio, 'tbn': tbn, 'pos': pos, 'dep': dep}
                     for (fio, tbn, pos, dep) in employees]
        self.objects = objects
        self.meal_types = meal_types if meal_types else ["Одноразовое", "Двухразовое", "Трехразовое"]
        self.departments = ["Все"] + sorted({dep for _, _, _, dep in employees if dep})

    def _build_ui(self):
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(fill="x", padx=10, pady=8)

        # Дата
        tk.Label(top, text="Дата:", bg="#f7f7f7").grid(row=0, column=0, sticky="w")
        self.ent_filter_date = ttk.Entry(top, width=12)
        self.ent_filter_date.grid(row=0, column=1, padx=4)
        self.ent_filter_date.insert(0, date.today().strftime("%Y-%m-%d"))

        # Подразделение
        tk.Label(top, text="Подразделение:", bg="#f7f7f7").grid(row=0, column=2, sticky="w", padx=(12, 0))
        self.cmb_filter_dep = ttk.Combobox(top, state="readonly",
                                           values=self.departments, width=20)
        self.cmb_filter_dep.grid(row=0, column=3, padx=4)
        self.cmb_filter_dep.set("Все")

        # Адрес
        tk.Label(top, text="Адрес:", bg="#f7f7f7").grid(row=0, column=4, sticky="w", padx=(12, 0))
        self.ent_filter_address = ttk.Entry(top, width=30)
        self.ent_filter_address.grid(row=0, column=5, padx=4)

        ttk.Button(top, text="🔄 Загрузить реестр", command=self.load_registry)\
            .grid(row=0, column=6, padx=12)
        ttk.Button(top, text="📊 Сформировать Excel", command=self.export_to_excel)\
            .grid(row=0, column=7, padx=4)

        table_frame = tk.LabelFrame(self, text="Реестр заказа питания по объектам")
        table_frame.pack(fill="both", expand=True, padx=10, pady=8)

        columns = ("date", "address", "total_count", "details")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=15)

        headers = {
            "date": "Дата",
            "address": "Адрес объекта",
            "total_count": "Всего заявок",
            "details": "Детали (двойной клик)"
        }
        widths = {"date": 100, "address": 400, "total_count": 120, "details": 300}

        for col in columns:
            self.tree.heading(col, text=headers.get(col, col))
            self.tree.column(col, width=widths.get(col, 100))

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)

        self.tree.bind("<Double-1>", self.on_row_double_click)

    def load_registry(self):
        try:
            url = get_meals_webhook_url()
            if not url:
                messagebox.showwarning("Загрузка", "Не настроен webhook URL в конфигурации")
                return

            token = get_meals_webhook_token()
            filter_date = self.ent_filter_date.get().strip()
            filter_address = self.ent_filter_address.get().strip()
            filter_dep = self.cmb_filter_dep.get().strip()

            params = {'action': 'get_registry'}
            if filter_date:
                params['date'] = filter_date
            if filter_address:
                params['address'] = filter_address
            if filter_dep and filter_dep != "Все":
                params['department'] = filter_dep
            if token:
                params['token'] = token

            query = urllib.parse.urlencode(params)
            full_url = f"{url}?{query}"

            with urllib.request.urlopen(full_url, timeout=15) as resp:
                result = json.loads(resp.read().decode('utf-8'))

            if not result.get('ok'):
                messagebox.showerror("Ошибка", f"Сервер вернул ошибку:\n{result.get('error', 'Unknown')}")
                return

            registry = result.get('registry', [])
            self._populate_tree(registry)
            messagebox.showinfo("Загрузка", f"Загружено объектов: {len(registry)}")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить реестр:\n{e}")

    def _populate_tree(self, registry: List[Dict]):
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.row_meta = {}

        for entry in registry:
            req_date = entry.get('date', '')
            address = entry.get('address', '')
            total = entry.get('total_count', 0)
            details_text = self._format_details(entry.get('by_department', {}))
            item_id = self.tree.insert("", "end", values=(req_date, address, total, details_text))
            self.row_meta[item_id] = entry

    def _format_details(self, by_dept: Dict) -> str:
        if not by_dept:
            return "Нет данных"
        parts = []
        for dept, data in by_dept.items():
            total = data.get('total', 0)
            parts.append(f"{dept}: {total} чел.")
        return " | ".join(parts[:3]) + (" ..." if len(parts) > 3 else "")

    def on_row_double_click(self, event):
        selection = self.tree.selection()
        if not selection:
            return
        item_id = selection[0]
        entry = self.row_meta.get(item_id)
        if not entry:
            return
        self._show_details_dialog(entry)

    def _show_details_dialog(self, entry: Dict):
        dialog = tk.Toplevel(self)
        dialog.title("Детальная информация")
        dialog.geometry("800x600")
        dialog.resizable(True, True)
        dialog.transient(self)
        dialog.grab_set()

        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (800 // 2)
        y = (dialog.winfo_screenheight() // 2) - (600 // 2)
        dialog.geometry(f"800x600+{x}+{y}")

        header = tk.Frame(dialog, bg="#e3f2fd", relief="solid", borderwidth=1)
        header.pack(fill="x", padx=0, pady=0)
        tk.Label(
            header,
            text=f"📅 Дата: {entry.get('date', '')} | 📍 {entry.get('address', '')}",
            font=("Arial", 12, "bold"),
            bg="#e3f2fd",
            fg="#0066cc",
            padx=15,
            pady=12
        ).pack(anchor="w")

        info_frame = tk.Frame(dialog, bg="#f7f7f7")
        info_frame.pack(fill="x", padx=15, pady=10)
        tk.Label(info_frame,
                 text=f"Всего заявок: {entry.get('total_count', 0)} человек",
                 font=("Arial", 11, "bold"),
                 bg="#f7f7f7").pack(anchor="w")

        table_frame = tk.LabelFrame(dialog, text="Детализация по подразделениям и типам питания",
                                    padx=10, pady=10)
        table_frame.pack(fill="both", expand=True, padx=15, pady=(0, 10))

        columns = ("department", "meal_type", "count")
        tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=15)
        tree.heading("department", text="Подразделение")
        tree.heading("meal_type", text="Тип питания")
        tree.heading("count", text="Количество")
        tree.column("department", width=300)
        tree.column("meal_type", width=200)
        tree.column("count", width=100)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        by_dept = entry.get('by_department', {})
        totals_by_type = {}

        for dept, data in sorted(by_dept.items()):
            by_type = data.get('by_meal_type', {})
            for meal_type, count in sorted(by_type.items()):
                tree.insert("", "end", values=(dept, meal_type, count))
                totals_by_type[meal_type] = totals_by_type.get(meal_type, 0) + count

        if totals_by_type:
            tree.insert("", "end", values=("", "", ""), tags=('separator',))
            tree.tag_configure('separator', background='#e0e0e0')
            for meal_type, total in sorted(totals_by_type.items()):
                tree.insert("", "end", values=("ИТОГО", meal_type, total), tags=('total',))
            tree.tag_configure('total', background='#fff3cd', font=('Arial', 9, 'bold'))

        ttk.Button(dialog, text="Закрыть", command=dialog.destroy, width=20).pack(pady=15)

    def export_to_excel(self):
        try:
            url = get_meals_webhook_url()
            if not url:
                messagebox.showwarning("Экспорт", "Не настроен webhook URL в конфигурации")
                return

            token = get_meals_webhook_token()
            filter_date = self.ent_filter_date.get().strip()
            filter_address = self.ent_filter_address.get().strip()
            filter_dep = self.cmb_filter_dep.get().strip()

            params = {'action': 'get_details'}
            if filter_date:
                params['date'] = filter_date
            if filter_address:
                params['address'] = filter_address
            if filter_dep and filter_dep != "Все":
                params['department'] = filter_dep
            if token:
                params['token'] = token

            query = urllib.parse.urlencode(params)
            full_url = f"{url}?{query}"

            with urllib.request.urlopen(full_url, timeout=15) as resp:
                result = json.loads(resp.read().decode('utf-8'))

            if not result.get('ok'):
                messagebox.showerror("Ошибка", f"Сервер вернул ошибку:\n{result.get('error', 'Unknown')}")
                return

            orders = result.get('orders', [])
            if not orders:
                messagebox.showinfo("Экспорт", "Нет данных для экспорта")
                return

            wb = Workbook()
            ws = wb.active
            ws.title = "Реестр питания"

            # свод
            summary: Dict[str, Dict[str, int]] = {}
            for o in orders:
                addr = o.get('address', '') or ''
                mt = o.get('meal_type', '') or ''
                if not addr or not mt:
                    continue
                summary.setdefault(addr, {})
                summary[addr][mt] = summary[addr].get(mt, 0) + 1

            ws.append(["Свод по объектам и типам питания"])
            ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=3)
            ws.append(["Адрес", "Тип питания", "Общее количество"])

            for addr, by_type in summary.items():
                for mt, cnt in by_type.items():
                    ws.append([addr, mt, cnt])

            ws.append([])

            # детали
            headers = [
                "Дата", "Адрес", "ID объекта", "Подразделение", "Наименование бригады",
                "ФИО", "Табельный №", "Должность", "Тип питания", "Комментарий"
            ]
            ws.append(headers)

            for order in orders:
                ws.append([
                    order.get('date', ''),
                    order.get('address', ''),
                    order.get('object_id', ''),
                    order.get('department', ''),
                    order.get('team_name', ''),
                    order.get('fio', ''),
                    order.get('tbn', ''),
                    order.get('position', ''),
                    order.get('meal_type', ''),
                    order.get('comment', '')
                ])

            for col, width in enumerate([12, 40, 15, 25, 25, 30, 15, 25, 18, 40], start=1):
                ws.column_dimensions[get_column_letter(col)].width = width

            ws.freeze_panes = "A4"

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"Реестр_питания_{filter_date or 'все'}_{ts}.xlsx"
            fpath = exe_dir() / ORDERS_DIR / fname
            fpath.parent.mkdir(parents=True, exist_ok=True)

            wb.save(fpath)
            messagebox.showinfo(
                "Экспорт",
                f"Реестр успешно сформирован:\n{fpath}\n\nЗаписей: {len(orders)}"
            )

            try:
                os.startfile(fpath)
            except Exception:
                pass
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось сформировать реестр:\n{e}")

# ========================= STANDALONE ОКНО =========================

class MealsApp(tk.Tk):
    """Standalone приложение для модуля питания"""

    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1000x720")
        self.resizable(True, True)

        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True)

        order_page = MealOrderPage(notebook)
        notebook.add(order_page, text="Создать заявку")

        if get_meals_planning_enabled():
            planning_page = MealPlanningPage(notebook)
            notebook.add(planning_page, text="Планирование питания")

# ========================= API ДЛЯ ВСТРАИВАНИЯ =========================

def create_meals_order_page(parent) -> tk.Frame:
    ensure_config()
    try:
        return MealOrderPage(parent)
    except Exception:
        import traceback
        messagebox.showerror("Питание — ошибка", traceback.format_exc(), parent=parent)
        return tk.Frame(parent)

def create_meals_planning_page(parent) -> tk.Frame:
    ensure_config()
    try:
        return MealPlanningPage(parent)
    except Exception:
        import traceback
        messagebox.showerror("Планирование питания — ошибка", traceback.format_exc(), parent=parent)
        return tk.Frame(parent)

def open_meals_module(parent=None):
    if parent is None:
        app = MealsApp()
        app.mainloop()
        return app

    win = tk.Toplevel(parent)
    win.title(APP_TITLE)
    win.geometry("1000x720")

    notebook = ttk.Notebook(win)
    notebook.pack(fill="both", expand=True)

    order_page = MealOrderPage(notebook)
    notebook.add(order_page, text="Создать заявку")

    if get_meals_planning_enabled():
        planning_page = MealPlanningPage(notebook)
        notebook.add(planning_page, text="Планирование питания")

    return win

if __name__ == "__main__":
    ensure_config()
    app = MealsApp()
    app.mainloop()
