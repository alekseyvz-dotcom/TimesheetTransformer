import os
import re
import sys
import csv
import calendar
import configparser
import json
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, date
from pathlib import Path
from typing import List, Tuple, Optional, Any, Dict

import tkinter as tk
from tkinter import ttk, messagebox

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

APP_TITLE = "Заказ спецтехники"

# Конфиг и файлы
CONFIG_FILE = "tabel_config.ini"
CONFIG_SECTION_PATHS = "Paths"
CONFIG_SECTION_UI = "UI"
KEY_SPR = "spravochnik_path"
KEY_SELECTED_DEP = "selected_department"

SPRAVOCHNIK_FILE = "Справочник.xlsx"
ORDERS_DIR = "Заявки_спецтехники"

# ------------------------- Утилиты -------------------------

def exe_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

def config_path() -> Path:
    return exe_dir() / CONFIG_FILE

def ensure_config():
    cp = config_path()
    if cp.exists():
        # допишем недостающие секции/ключи
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
        if changed:
            with open(cp, "w", encoding="utf-8") as f:
                cfg.write(f)
        return
    # создаём
    cfg = configparser.ConfigParser()
    cfg[CONFIG_SECTION_PATHS] = {
        KEY_SPR: str(exe_dir() / SPRAVOCHNIK_FILE)
    }
    cfg[CONFIG_SECTION_UI] = {
        KEY_SELECTED_DEP: "Все"
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
    raw = cfg.get(CONFIG_SECTION_PATHS, KEY_SPR, fallback=str(exe_dir() / SPRAVOCHNIK_FILE))
    return Path(os.path.expandvars(raw))

def get_saved_dep() -> str:
    cfg = read_config()
    return cfg.get(CONFIG_SECTION_UI, KEY_SELECTED_DEP, fallback="Все")

def get_orders_mode() -> str:
    cfg = read_config()
    return cfg.get('Integrations', 'orders_mode', fallback='none').strip().lower()

def get_orders_webhook_url() -> str:
    cfg = read_config()
    return cfg.get('Integrations', 'orders_webhook_url', fallback='').strip()

def get_orders_webhook_token() -> str:
    cfg = read_config()
    return cfg.get('Integrations', 'orders_webhook_token', fallback='').strip()

def set_saved_dep(dep: str):
    cfg = read_config()
    if not cfg.has_section(CONFIG_SECTION_UI):
        cfg[CONFIG_SECTION_UI] = {}
    cfg[CONFIG_SECTION_UI][KEY_SELECTED_DEP] = dep or "Все"
    write_config(cfg)

def month_days(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]

def safe_filename(s: str, maxlen: int = 80) -> str:
    if not s:
        return "NOID"
    s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", str(s)).strip()
    s = re.sub(r"_+", "_", s)
    return s[:maxlen] if len(s) > maxlen else s

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
    ws1.append(["Петров П. П.", "ST00-00002", "Электромонтер", "Электрика"])
    ws1.append(["Сидорова А. А.", "ST00-00003", "Инженер", "ИТ"])
    ws2 = wb.create_sheet("Объекты")
    ws2.append(["ID объекта", "Адрес"])
    ws2.append(["OBJ-001", "ул. Пушкина, д. 1"])
    ws2.append(["OBJ-002", "пр. Строителей, 25"])
    # Техника
    ws3 = wb.create_sheet("Техника")
    ws3.append(["Тип", "Наименование", "Гос№", "Подразделение", "Примечание"])
    ws3.append(["Автокран", "КС-45717", "А123ВС77", "", "25 т."])
    ws3.append(["Манипулятор", "Isuzu Giga", "М456ОР77", "", "Борт 7 т."])
    ws3.append(["Экскаватор", "JCB 3CX", "Е789КУ77", "", ""])
    wb.save(path)

def ensure_tech_sheet(path: Path):
    # если листа «Техника» нет — создадим
    ensure_spravochnik(path)
    try:
        wb = load_workbook(path)
        if "Техника" not in wb.sheetnames:
            ws = wb.create_sheet("Техника")
            ws.append(["Тип", "Наименование", "Гос№", "Подразделение", "Примечание"])
            ws.append(["Автокран", "КС-45717", "А123ВС77", "", "25 т."])
            ws.append(["Манипулятор", "Isuzu Giga", "М456ОР77", "", "Борт 7 т."])
            ws.append(["Экскаватор", "JCB 3CX", "Е789КУ77", "", ""])
            wb.save(path)
    except Exception:
        pass

def load_spravochnik(path: Path) -> Tuple[List[Dict], List[Tuple[str,str]], List[Dict]]:
    """
    employees: [{'fio','tbn','pos','dep'}]
    objects:   [(id, addr)]
    techs:     [{'type','name','plate','dep','note','disp'}] disp — строка для отображения
    """
    def s(v) -> str:
        if v is None:
            return ""
        if isinstance(v, float) and v.is_integer():
            v = int(v)
        return str(v).strip()

    ensure_spravochnik(path)
    ensure_tech_sheet(path)
    wb = load_workbook(path, read_only=True, data_only=True)

    employees: List[Dict] = []
    objects: List[Tuple[str,str]] = []
    techs: List[Dict] = []

    if "Сотрудники" in wb.sheetnames:
        ws = wb["Сотрудники"]
        hdr = [s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        have_pos = ("должность" in hdr) or (len(hdr) >= 3)
        have_dep = ("подразделение" in hdr) or (len(hdr) >= 4)
        for r in ws.iter_rows(min_row=2, values_only=True):
            fio = s(r[0] if len(r) > 0 else None)
            tbn = s(r[1] if len(r) > 1 else None)
            pos = s(r[2] if have_pos and len(r) > 2 else None)
            dep = s(r[3] if have_dep and len(r) > 3 else None)
            if fio:
                employees.append({'fio': fio, 'tbn': tbn, 'pos': pos, 'dep': dep})

    if "Объекты" in wb.sheetnames:
        ws = wb["Объекты"]
        hdr = [s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        have_two = ("id объекта" in hdr) or (len(hdr) >= 2)
        for r in ws.iter_rows(min_row=2, values_only=True):
            if have_two:
                oid = s(r[0] if len(r) > 0 else None)
                addr = s(r[1] if len(r) > 1 else None)
            else:
                oid = ""
                addr = s(r[0] if len(r) > 0 else None)
            if oid or addr:
                objects.append((oid, addr))

    if "Техника" in wb.sheetnames:
        ws = wb["Техника"]
        hdr = [s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        # Ожидаем: Тип, Наименование, Гос№, Подразделение, Примечание
        for r in ws.iter_rows(min_row=2, values_only=True):
            tp  = s(r[0] if len(r) > 0 else None)
            nm  = s(r[1] if len(r) > 1 else None)
            pl  = s(r[2] if len(r) > 2 else None)
            dep = s(r[3] if len(r) > 3 else None)
            note= s(r[4] if len(r) > 4 else None)
            if (tp or nm or pl):
                disp = " | ".join(x for x in [tp, nm, pl] if x)
                techs.append({'type': tp, 'name': nm, 'plate': pl, 'dep': dep, 'note': note, 'disp': disp})

    return employees, objects, techs

def parse_hours_value(v: Any) -> Optional[float]:
    s = str(v or "").strip()
    if not s:
        return None
    if "/" in s:
        total = 0.0
        any_part = False
        for part in s.split("/"):
            n = parse_hours_value(part)
            if isinstance(n, (int, float)):
                total += float(n); any_part = True
        return total if any_part else None
    if ":" in s:
        p = s.split(":")
        try:
            hh = float(p[0].replace(",", "."))
            mm = float((p[1] if len(p)>1 else "0").replace(",", "."))
            ss = float((p[2] if len(p)>2 else "0").replace(",", "."))
            return hh + mm/60.0 + ss/3600.0
        except:
            pass
    s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return None

def parse_time_str(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return f"{hh:02d}:{mm:02d}"

def parse_date_any(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
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
    req = urllib.request.Request(
    url,
    data=body,
    headers={'Content-Type': 'application/json; charset=utf-8'},
    method='POST'
    )
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
# ------------------------- Виджеты -------------------------

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

# ------------------------- Строка позиции -------------------------

class PositionRow:
    ERR_BG = "#ffccbc"
    ZEBRA_EVEN = "#ffffff"
    ZEBRA_ODD  = "#f6f8fa"

    def __init__(self, parent, idx: int, tech_values: List[str], on_delete):
        self.parent = parent
        self.idx = idx
        self.on_delete = on_delete
        self.tech_values = tech_values

        self.frame = tk.Frame(parent)

        self.cmb_tech = ttk.Combobox(self.frame, values=tech_values, width=46)
        self.cmb_tech.grid(row=0, column=0, padx=2, pady=1, sticky="w")

        self.ent_qty = ttk.Entry(self.frame, width=6, justify="center")
        self.ent_qty.grid(row=0, column=1, padx=2)
        self.ent_qty.insert(0, "1")

        self.ent_time = ttk.Entry(self.frame, width=8, justify="center")
        self.ent_time.grid(row=0, column=2, padx=2)
        self.ent_time.insert(0, "")  # необязательное

        self.ent_hours = ttk.Entry(self.frame, width=8, justify="center")
        self.ent_hours.grid(row=0, column=3, padx=2)
        self.ent_hours.insert(0, "4")

        self.ent_note = ttk.Entry(self.frame, width=34)
        self.ent_note.grid(row=0, column=4, padx=2, sticky="w")

        self.btn_del = ttk.Button(self.frame, text="Удалить", width=9, command=self._delete)
        self.btn_del.grid(row=0, column=5, padx=2)

        for i in range(6):
            self.frame.grid_columnconfigure(i, minsize=[380, 50, 70, 70, 280, 80][i])

    def grid(self, row: int):
        self.frame.grid(row=row, column=0, sticky="w")

    def destroy(self):
        self.frame.destroy()

    def apply_zebra(self, row0: int):
        bg = self.ZEBRA_ODD if (row0 % 2 == 1) else self.ZEBRA_EVEN
        for w in (self.cmb_tech, self.ent_qty, self.ent_time, self.ent_hours, self.ent_note):
            try:
                w.configure(background=bg)
            except Exception:
                try:
                    w.configure(style="")  # ttk
                except Exception:
                    pass

    def _delete(self):
        self.on_delete(self)

    def validate(self) -> bool:
        ok = True
        # техника
        val = (self.cmb_tech.get() or "").strip()
        if not val:
            self._mark_err(self.cmb_tech); ok = False
        else:
            self._clear_err(self.cmb_tech)
        # qty
        try:
            qty = int((self.ent_qty.get() or "0").strip())
            if qty <= 0: raise ValueError
            self._clear_err(self.ent_qty)
        except Exception:
            self._mark_err(self.ent_qty); ok = False
        # time (optional)
        tstr = (self.ent_time.get() or "").strip()
        if tstr:
            if parse_time_str(tstr) is None:
                self._mark_err(self.ent_time); ok = False
            else:
                self._clear_err(self.ent_time)
        else:
            self._clear_err(self.ent_time)
        # hours
        hv = parse_hours_value(self.ent_hours.get())
        if hv is None or hv < 0:
            self._mark_err(self.ent_hours); ok = False
        else:
            self._clear_err(self.ent_hours)
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
            "tech": (self.cmb_tech.get() or "").strip(),
            "qty": int((self.ent_qty.get() or "0").strip() or 0),
            "time": (parse_time_str(self.ent_time.get()) or ""),
            "hours": float(parse_hours_value(self.ent_hours.get()) or 0.0),
            "note": (self.ent_note.get() or "").strip(),
        }

# ------------------------- Окно заявок -------------------------

class SpecialOrdersApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1180x720")
        self.resizable(True, True)

        self.base_dir = exe_dir()
        self.spr_path = get_spr_path()
        self.orders_dir = self.base_dir / ORDERS_DIR
        self.orders_dir.mkdir(parents=True, exist_ok=True)

        self._load_spr()
        self._build_ui()

    def _load_spr(self):
        self.emps, self.objects, self.techs = load_spravochnik(self.spr_path)
        self.deps = ["Все"] + sorted({(r['dep'] or "").strip() for r in self.emps if (r['dep'] or "").strip()})
        self.emp_names_all = [r['fio'] for r in self.emps]
        self.addr_to_ids = {}
        for oid, addr in self.objects:
            if not addr:
                continue
            self.addr_to_ids.setdefault(addr, [])
            if oid and oid not in self.addr_to_ids[addr]:
                self.addr_to_ids[addr].append(oid)
        self.addresses = sorted(self.addr_to_ids.keys() | {addr for _, addr in self.objects if addr})
        self.tech_values = [t['disp'] for t in self.techs]

    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", padx=10, pady=8)

        # Ряд 1: Подразделение, ФИО, Телефон, Дата
        tk.Label(top, text="Подразделение:").grid(row=0, column=0, sticky="w")
        self.cmb_dep = ttk.Combobox(top, state="readonly", values=self.deps, width=48)
        saved_dep = get_saved_dep()
        self.cmb_dep.set(saved_dep if saved_dep in self.deps else self.deps[0])
        self.cmb_dep.grid(row=0, column=1, sticky="w", padx=(4, 12))
        self.cmb_dep.bind("<<ComboboxSelected>>", lambda e: (set_saved_dep(self.cmb_dep.get()), self._update_fio_list()))

        tk.Label(top, text="ФИО:").grid(row=0, column=2, sticky="w")
        self.fio_var = tk.StringVar()
        self.cmb_fio = AutoCompleteCombobox(top, textvariable=self.fio_var, width=36)
        self.cmb_fio.grid(row=0, column=3, sticky="w", padx=(4, 12))

        tk.Label(top, text="Телефон:").grid(row=0, column=4, sticky="w")
        self.ent_phone = ttk.Entry(top, width=18)
        self.ent_phone.grid(row=0, column=5, sticky="w", padx=(4, 12))

        tk.Label(top, text="Дата:").grid(row=0, column=6, sticky="w")
        self.ent_date = ttk.Entry(top, width=12)
        self.ent_date.grid(row=0, column=7, sticky="w", padx=(4, 0))
        self.ent_date.insert(0, date.today().strftime("%Y-%m-%d"))

        # Ряд 2: Объект (Адрес / ID)
        tk.Label(top, text="Адрес:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.cmb_address = AutoCompleteCombobox(top, width=56)
        self.cmb_address.set_completion_list(self.addresses)
        self.cmb_address.grid(row=1, column=1, columnspan=3, sticky="w", padx=(4, 12), pady=(8, 0))
        self.cmb_address.bind("<<ComboboxSelected>>", lambda e: self._sync_ids_by_address())
        self.cmb_address.bind("<FocusOut>", lambda e: self._sync_ids_by_address())
        self.cmb_address.bind("<Return>", lambda e: self._sync_ids_by_address())

        tk.Label(top, text="ID объекта:").grid(row=1, column=4, sticky="w", pady=(8, 0))
        self.cmb_object_id = ttk.Combobox(top, state="readonly", values=[], width=20)
        self.cmb_object_id.grid(row=1, column=5, sticky="w", padx=(4, 12), pady=(8, 0))

        # Ряд 3: Общий комментарий
        tk.Label(top, text="Комментарий:").grid(row=2, column=0, sticky="nw", pady=(8, 0))
        self.txt_comment = tk.Text(top, height=3, width=96)
        self.txt_comment.grid(row=2, column=1, columnspan=7, sticky="we", padx=(4, 0), pady=(8, 0))

        # Рамка позиций
        pos_wrap = tk.LabelFrame(self, text="Позиции")
        pos_wrap.pack(fill="both", expand=True, padx=10, pady=(6, 8))

        # Шапка позиций
        hdr = tk.Frame(pos_wrap)
        hdr.pack(fill="x")
        tk.Label(hdr, text="Техника", width=52, anchor="w").grid(row=0, column=0, padx=2)
        tk.Label(hdr, text="Кол-во", width=6, anchor="center").grid(row=0, column=1, padx=2)
        tk.Label(hdr, text="Подача (чч:мм)", width=12, anchor="center").grid(row=0, column=2, padx=2)
        tk.Label(hdr, text="Часы", width=10, anchor="center").grid(row=0, column=3, padx=2)
        tk.Label(hdr, text="Примечание", width=38, anchor="w").grid(row=0, column=4, padx=2)
        tk.Label(hdr, text="Действие", width=10, anchor="center").grid(row=0, column=5, padx=2)

        # Позиции — холдер со скроллом
        wrap = tk.Frame(pos_wrap)
        wrap.pack(fill="both", expand=True)
        self.cv = tk.Canvas(wrap, borderwidth=0, highlightthickness=0)
        self.rows_holder = tk.Frame(self.cv)
        self.cv.create_window((0, 0), window=self.rows_holder, anchor="nw")
        self.cv.pack(side="left", fill="both", expand=True)
        self.vscroll = ttk.Scrollbar(wrap, orient="vertical", command=self.cv.yview)
        self.vscroll.pack(side="right", fill="y")
        self.cv.configure(yscrollcommand=self.vscroll.set)
        self.rows_holder.bind("<Configure>", lambda e: self.cv.configure(scrollregion=self.cv.bbox("all")))
        self.cv.bind("<MouseWheel>", lambda e: (self.cv.yview_scroll(int(-1*(e.delta/120)), "units"), "break"))

        self.pos_rows: List[PositionRow] = []
        btns = tk.Frame(pos_wrap)
        btns.pack(fill="x")
        ttk.Button(btns, text="Добавить позицию", command=self.add_position).pack(side="left", padx=2, pady=4)

        # Нижние кнопки
        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(bottom, text="Сохранить заявку", command=self.save_order).pack(side="left", padx=4)
        ttk.Button(bottom, text="Очистить форму", command=self.clear_form).pack(side="left", padx=4)
        ttk.Button(bottom, text="Открыть папку заявок", command=self.open_orders_dir).pack(side="left", padx=4)

        # Первичная инициализация
        self._update_fio_list()
        # Стартовая одна строка
        self.add_position()

        # Колонки top — растяжение
        for c in range(8):
            top.grid_columnconfigure(c, weight=0)
        top.grid_columnconfigure(1, weight=1)
        top.grid_columnconfigure(5, weight=0)

    def _update_fio_list(self):
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
            # ничего в выбранном департаменте — fallback на всех
            filtered = [r['fio'] for r in self.emps]
        self.cmb_fio.set_completion_list(filtered)

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

    def add_position(self):
        row = PositionRow(self.rows_holder, len(self.pos_rows) + 1, self.tech_values, self.delete_position)
        row.grid(len(self.pos_rows))
        row.apply_zebra(len(self.pos_rows))
        self.pos_rows.append(row)

    def delete_position(self, prow: PositionRow):
        try:
            self.pos_rows.remove(prow)
        except Exception:
            pass
        prow.destroy()
        # перегрид для зебры
        for i, r in enumerate(self.pos_rows, start=0):
            r.grid(i)
            r.apply_zebra(i)

    def _validate_form(self) -> bool:
        ok = True
        # департамент
        if not (self.cmb_dep.get() or "").strip():
            ok = False
        # ФИО
        if not (self.cmb_fio.get() or "").strip():
            ok = False
        # дата
        d = parse_date_any(self.ent_date.get())
        if d is None:
            ok = False
            messagebox.showwarning("Заявка", "Введите корректную дату (YYYY-MM-DD или DD.MM.YYYY).")
            return False
        # адрес/ID — хотя бы что‑то
        addr = (self.cmb_address.get() or "").strip()
        oid = (self.cmb_object_id.get() or "").strip()
        if not addr and not oid:
            ok = False
            messagebox.showwarning("Заявка", "Укажите Адрес и/или ID объекта.")
            return False
        # позиции
        if not self.pos_rows:
            messagebox.showwarning("Заявка", "Добавьте хотя бы одну позицию.")
            return False
        for r in self.pos_rows:
            ok = r.validate() and ok
        if not ok:
            messagebox.showwarning("Заявка", "Исправьте подсвеченные поля в позициях.")
        return ok

    def _build_order_dict(self) -> Dict:
        created_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        req_date = parse_date_any(self.ent_date.get()) or date.today()
        addr = (self.cmb_address.get() or "").strip()
        oid = (self.cmb_object_id.get() or "").strip()
        comment = self.txt_comment.get("1.0", "end").strip()
        positions = [r.get_dict() for r in self.pos_rows]
        return {
            "created_at": created_at,
            "date": req_date.strftime("%Y-%m-%d"),
            "department": (self.cmb_dep.get() or "").strip(),
            "requester_fio": (self.cmb_fio.get() or "").strip(),
            "requester_phone": (self.ent_phone.get() or "").strip(),
            "object": {"id": oid, "address": addr},
            "comment": comment,
            "positions": positions,
        }

    def save_order(self):
        if not self._validate_form():
            return
        data = self._build_order_dict()
        # XLSX
        ts = datetime.now().strftime("%H%M%S")
        id_part = data["object"]["id"] or safe_filename(data["object"]["address"])
        fname = f"Заявка_спецтехники_{data['date']}_{ts}_{id_part or 'NOID'}.xlsx"
        fpath = self.orders_dir / fname

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Заявка"
            # Шапка
            ws.append(["Создано", data["created_at"]])
            ws.append(["Дата", data["date"]])
            ws.append(["Подразделение", data["department"]])
            ws.append(["Заявитель (ФИО)", data["requester_fio"]])
            ws.append(["Телефон", data["requester_phone"]])
            ws.append(["ID объекта", data["object"]["id"]])
            ws.append(["Адрес", data["object"]["address"]])
            ws.append(["Комментарий", data["comment"]])
            ws.append([])

            # Позиции
            hdr = ["#", "Техника", "Кол-во", "Подача (чч:мм)", "Часы", "Примечание"]
            ws.append(hdr)
            for i, p in enumerate(data["positions"], start=1):
                ws.append([
                    i,
                    p["tech"],
                    p["qty"],
                    (p["time"] or None),
                    p["hours"],
                    p["note"]
                ])
            for col, w in enumerate([4, 48, 8, 14, 10, 36], start=1):
                ws.column_dimensions[get_column_letter(col)].width = w
            ws.freeze_panes = "A12"
            wb.save(fpath)
        except Exception as e:
            messagebox.showerror("Сохранение", f"Не удалось сохранить XLSX:\n{e}")
            return

        # CSV (свод за месяц) — по 1 строке на позицию
        csv_path = self.orders_dir / f"Свод_заявок_{data['date'][:7].replace('-', '_')}.csv"
        try:
            new = not csv_path.exists()
            with open(csv_path, "a", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f, delimiter=";")
                if new:
                    w.writerow([
                        "Создано","Дата","Подразделение","ФИО","Телефон","ID объекта","Адрес",
                        "Техника","Кол-во","Подача","Часы","Примечание","Комментарий заявки"
                    ])
                for p in data["positions"]:
                    w.writerow([
                        data["created_at"], data["date"], data["department"], data["requester_fio"],
                        data["requester_phone"], data["object"]["id"], data["object"]["address"],
                        p["tech"], p["qty"], p["time"], p["hours"], p["note"], data["comment"]
                    ])
        except Exception as e:
            messagebox.showwarning("Сводный CSV", f"XLSX сохранён, но не удалось добавить в CSV:\n{e}")

        messagebox.showinfo("Сохранение", f"Заявка сохранена:\n{fpath}\n\nСводный CSV:\n{csv_path}")
         # Попытка онлайн-отправки (webhook)
     try:
         mode = get_orders_mode()
         if mode == 'webhook':
             url = get_orders_webhook_url()
             token = get_orders_webhook_token()
             if url:
                 ok, info = post_json(url, data, token)
                 if ok:
                     messagebox.showinfo(
                         "Сохранение/Отправка",
                         f"Заявка сохранена локально и отправлена онлайн.\n\n"
                         f"XLSX:\n{fpath}\nCSV:\n{csv_path}\n\nОтвет сервера:\n{info}"
                     )
                 else:
                     messagebox.showwarning(
                         "Сохранение/Отправка",
                         f"Локально сохранено, но онлайн-отправка не удалась.\n\n"
                         f"XLSX:\n{fpath}\nCSV:\n{csv_path}\n\n{info}"
                     )
                 return
             else:
                 messagebox.showinfo(
                     "Сохранение",
                     f"Заявка сохранена:\n{fpath}\n\nСводный CSV:\n{csv_path}\n(Онлайн-отправка не настроена)"
                 )
                 return
         else:
             messagebox.showinfo("Сохранение", f"Заявка сохранена:\n{fpath}\n\nСводный CSV:\n{csv_path}")
             return
     except Exception as e:
         messagebox.showwarning(
             "Сохранение/Отправка",
             f"Локально сохранено, но онлайн-отправка упала с ошибкой:\n{e}\n\n"
             f"XLSX:\n{fpath}\nCSV:\n{csv_path}"
         )
         return

    def clear_form(self):
        # не меняем подразделение
        self.fio_var.set("")
        self.ent_phone.delete(0, "end")
        self.ent_date.delete(0, "end"); self.ent_date.insert(0, date.today().strftime("%Y-%m-%d"))
        self.cmb_address.set("")
        self.cmb_object_id.config(values=[]); self.cmb_object_id.set("")
        self.txt_comment.delete("1.0", "end")
        for r in self.pos_rows:
            r.destroy()
        self.pos_rows.clear()
        self.add_position()

    def open_orders_dir(self):
        try:
            os.startfile(self.orders_dir)
        except Exception as e:
            messagebox.showerror("Папка", f"Не удалось открыть папку:\n{e}")


if __name__ == "__main__":
    ensure_config()
    app = SpecialOrdersApp()
    app.mainloop()
