# python
import os
import re
import sys
import csv
import json
import calendar
import configparser
import urllib.request
import urllib.error
import urllib.parse
from io import BytesIO
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
CONFIG_SECTION_PATHS   = "Paths"
CONFIG_SECTION_UI      = "UI"
CONFIG_SECTION_INTEGR  = "Integrations"
CONFIG_SECTION_ORDERS  = "Orders"
CONFIG_SECTION_REMOTE  = "Remote"   # удалённый справочник (Яндекс Диск — публичная ссылка)

KEY_SPR                 = "spravochnik_path"
KEY_SELECTED_DEP        = "selected_department"

KEY_ORDERS_MODE         = "orders_mode"               # none | webhook
KEY_ORDERS_WEBHOOK_URL  = "orders_webhook_url"        # https://script.google.com/macros/s/.../exec
KEY_ORDERS_WEBHOOK_TOKEN= "orders_webhook_token"
KEY_PLANNING_ENABLED = "planning_enabled"             # true|false

# Настройки отсечки подачи заявок
KEY_CUTOFF_ENABLED      = "cutoff_enabled"            # true|false
KEY_CUTOFF_HOUR         = "cutoff_hour"               # 0..23
KEY_DRIVER_DEPARTMENTS = "driver_departments"

# Удалённый справочник (Я.Диск)
KEY_REMOTE_USE          = "use_remote"                # true|false
KEY_YA_PUBLIC_LINK      = "yadisk_public_link"        # публичная ссылка (public_key)
KEY_YA_PUBLIC_PATH      = "yadisk_public_path"        # если опубликована папка — путь к файлу внутри неё

SPRAVOCHNIK_FILE = "Справочник.xlsx"
ORDERS_DIR = "Заявки_спецтехники"


# ------------------------- Утилиты конфигурации -------------------------

def exe_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

def config_path() -> Path:
    return exe_dir() / CONFIG_FILE

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
        if KEY_ORDERS_MODE not in cfg[CONFIG_SECTION_INTEGR]:
            cfg[CONFIG_SECTION_INTEGR][KEY_ORDERS_MODE] = "none"
            changed = True
        if KEY_ORDERS_WEBHOOK_URL not in cfg[CONFIG_SECTION_INTEGR]:
            cfg[CONFIG_SECTION_INTEGR][KEY_ORDERS_WEBHOOK_URL] = ""
            changed = True
        if KEY_ORDERS_WEBHOOK_TOKEN not in cfg[CONFIG_SECTION_INTEGR]:
            cfg[CONFIG_SECTION_INTEGR][KEY_ORDERS_WEBHOOK_TOKEN] = ""
            changed = True

        if not cfg.has_section(CONFIG_SECTION_ORDERS):
            cfg[CONFIG_SECTION_ORDERS] = {}
            changed = True
        if KEY_CUTOFF_ENABLED not in cfg[CONFIG_SECTION_ORDERS]:
            cfg[CONFIG_SECTION_ORDERS][KEY_CUTOFF_ENABLED] = "true"
            changed = True
        if KEY_CUTOFF_HOUR not in cfg[CONFIG_SECTION_ORDERS]:
            cfg[CONFIG_SECTION_ORDERS][KEY_CUTOFF_HOUR] = "13"
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
        if KEY_PLANNING_ENABLED not in cfg[CONFIG_SECTION_INTEGR]:
            cfg[CONFIG_SECTION_INTEGR][KEY_PLANNING_ENABLED] = "false"
            changed = True
        if KEY_DRIVER_DEPARTMENTS not in cfg[CONFIG_SECTION_INTEGR]:
            cfg[CONFIG_SECTION_INTEGR][KEY_DRIVER_DEPARTMENTS] = "Служба гаража, Автопарк, Транспортный цех"
            changed = True

        if changed:
            with open(cp, "w", encoding="utf-8") as f:
                cfg.write(f)
        return

    # создаём с нуля
    cfg = configparser.ConfigParser()
    cfg[CONFIG_SECTION_PATHS] = {
        KEY_SPR: str(exe_dir() / SPRAVOCHNIK_FILE)
    }
    cfg[CONFIG_SECTION_UI] = {
        KEY_SELECTED_DEP: "Все"
    }
    cfg[CONFIG_SECTION_INTEGR] = {
        KEY_ORDERS_MODE: "none",
        KEY_ORDERS_WEBHOOK_URL: "",
        KEY_ORDERS_WEBHOOK_TOKEN: ""
    }
    cfg[CONFIG_SECTION_ORDERS] = {
        KEY_CUTOFF_ENABLED: "true",
        KEY_CUTOFF_HOUR: "13"
    }
    cfg[CONFIG_SECTION_REMOTE] = {
        KEY_REMOTE_USE: "false",
        KEY_YA_PUBLIC_LINK: "",
        KEY_YA_PUBLIC_PATH: ""
    }
    with open(cp, "w", encoding="utf-8") as f:
        cfg.write(f)
        
def get_planning_enabled() -> bool:
    cfg = read_config()
    v = cfg.get(CONFIG_SECTION_INTEGR, KEY_PLANNING_ENABLED, fallback="false").strip().lower()
    return v in ("1", "true", "yes", "on")

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

def set_saved_dep(dep: str):
    cfg = read_config()
    if not cfg.has_section(CONFIG_SECTION_UI):
        cfg[CONFIG_SECTION_UI] = {}
    cfg[CONFIG_SECTION_UI][KEY_SELECTED_DEP] = dep or "Все"
    write_config(cfg)

def get_orders_mode() -> str:
    cfg = read_config()
    return cfg.get(CONFIG_SECTION_INTEGR, KEY_ORDERS_MODE, fallback="none").strip().lower()

def get_orders_webhook_url() -> str:
    cfg = read_config()
    return cfg.get(CONFIG_SECTION_INTEGR, KEY_ORDERS_WEBHOOK_URL, fallback="").strip()

def get_orders_webhook_token() -> str:
    cfg = read_config()
    return cfg.get(CONFIG_SECTION_INTEGR, KEY_ORDERS_WEBHOOK_TOKEN, fallback="").strip()

# Настройки отсечки приёма заявок
def get_cutoff_enabled() -> bool:
    cfg = read_config()
    v = cfg.get(CONFIG_SECTION_ORDERS, KEY_CUTOFF_ENABLED, fallback="true").strip().lower()
    return v in ("1", "true", "yes", "on")

def get_cutoff_hour() -> int:
    cfg = read_config()
    try:
        h = int(cfg.get(CONFIG_SECTION_ORDERS, KEY_CUTOFF_HOUR, fallback="13").strip())
        return min(23, max(0, h))
    except Exception:
        return 13

def is_past_cutoff_for_date(req_date: date, cutoff_hour: int) -> bool:
    now = datetime.now()
    if req_date != now.date():
        return False
    cutoff = now.replace(hour=cutoff_hour, minute=0, second=0, microsecond=0)
    return now >= cutoff


# ------------------------- Справочник: локально/Я.Диск -------------------------

def ensure_spravochnik(path: Path):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    if path.exists():
        return
    wb = Workbook()
    # Сотрудники
    ws1 = wb.active
    ws1.title = "Сотрудники"
    ws1.append(["ФИО", "Табельный №", "Должность", "Подразделение"])
    ws1.append(["Иванов И. И.", "ST00-00001", "Слесарь", "Монтаж"])
    ws1.append(["Петров П. П.", "ST00-00002", "Электромонтер", "Электрика"])
    ws1.append(["Сидорова А. А.", "ST00-00003", "Инженер", "ИТ"])
    # Объекты
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

def load_spravochnik_from_wb(wb) -> Tuple[
    List[Tuple[str,str,str,str]],
    List[Tuple[str,str]],
    List[Tuple[str,str,str,str,str]]
]:
    employees: List[Tuple[str,str,str,str]] = []
    objects:   List[Tuple[str,str]] = []
    tech:      List[Tuple[str,str,str,str,str]] = []

    if "Сотрудники" in wb.sheetnames:
        ws = wb["Сотрудники"]
        hdr = [_s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        have_pos = ("должность" in hdr) or (len(hdr) >= 3)
        have_dep = ("подразделение" in hdr) or (len(hdr) >= 4)
        for r in ws.iter_rows(min_row=2, values_only=True):
            fio = _s(r[0] if r and len(r)>0 else "")
            tbn = _s(r[1] if r and len(r)>1 else "")
            pos = _s(r[2] if have_pos and r and len(r)>2 else "")
            dep = _s(r[3] if have_dep and r and len(r)>3 else "")
            if fio:
                employees.append((fio, tbn, pos, dep))

    if "Объекты" in wb.sheetnames:
        ws = wb["Объекты"]
        hdr = [_s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        have_two = ("id объекта" in hdr) or (len(hdr) >= 2)
        for r in ws.iter_rows(min_row=2, values_only=True):
            if have_two:
                oid = _s(r[0] if r and len(r)>0 else "")
                addr = _s(r[1] if r and len(r)>1 else "")
            else:
                oid = ""
                addr = _s(r[0] if r and len(r)>0 else "")
            if oid or addr:
                objects.append((oid, addr))

    if "Техника" in wb.sheetnames:
        ws = wb["Техника"]
        hdr = [_s(c).lower() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        for r in ws.iter_rows(min_row=2, values_only=True):
            tp  = _s(r[0] if r and len(r)>0 else "")
            nm  = _s(r[1] if r and len(r)>1 else "")
            pl  = _s(r[2] if r and len(r)>2 else "")
            dep = _s(r[3] if r and len(r)>3 else "")
            note= _s(r[4] if r and len(r)>4 else "")
            if tp or nm or pl:
                tech.append((tp, nm, pl, dep, note))

    return employees, objects, tech

def load_spravochnik_remote_or_local(local_path: Path):
    cfg = read_config()
    use_remote = cfg.get(CONFIG_SECTION_REMOTE, KEY_REMOTE_USE, fallback="false").strip().lower() in ("1","true","yes","on")
    if use_remote:
        try:
            public_link = cfg.get(CONFIG_SECTION_REMOTE, KEY_YA_PUBLIC_LINK, fallback="").strip()
            public_path = cfg.get(CONFIG_SECTION_REMOTE, KEY_YA_PUBLIC_PATH, fallback="").strip()
            raw = fetch_yadisk_public_bytes(public_link, public_path)
            wb = load_workbook(BytesIO(raw), read_only=True, data_only=True)
            return load_spravochnik_from_wb(wb)
        except Exception as e:
            print(f"[Remote YaDisk] ошибка: {e} — используется локальный файл")

    ensure_spravochnik(local_path)
    wb = load_workbook(local_path, read_only=True, data_only=True)
    return load_spravochnik_from_wb(wb)


# ------------------------- Парсинг значений -------------------------

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
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    return None


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

        # ===== ИЗМЕНЕНИЯ ДЛЯ АВТОФОРМАТИРОВАНИЯ ВРЕМЕНИ =====
        self.time_var = tk.StringVar()
        self.time_var.trace_add("write", self._on_time_changed)
        self._formatting_time = False
        self._format_timer = None  # Таймер для отложенного форматирования
        
        self.ent_time = ttk.Entry(self.frame, width=8, justify="center", textvariable=self.time_var)
        self.ent_time.grid(row=0, column=2, padx=2)
        
        # Форматирование при потере фокуса (мгновенно)
        self.ent_time.bind("<FocusOut>", self._format_immediately)
        self.ent_time.bind("<Return>", self._format_immediately)
        # ====================================================

        self.ent_hours = ttk.Entry(self.frame, width=8, justify="center")
        self.ent_hours.grid(row=0, column=3, padx=2)
        self.ent_hours.insert(0, "4")

        self.ent_note = ttk.Entry(self.frame, width=34)
        self.ent_note.grid(row=0, column=4, padx=2, sticky="w")

        self.btn_del = ttk.Button(self.frame, text="Удалить", width=9, command=self._delete)
        self.btn_del.grid(row=0, column=5, padx=2)

        for i in range(6):
            self.frame.grid_columnconfigure(i, minsize=[380, 50, 70, 70, 280, 80][i])

    # ===== НОВЫЕ МЕТОДЫ ДЛЯ АВТОФОРМАТИРОВАНИЯ =====
    def _on_time_changed(self, *args):
        """Вызывается при каждом изменении - запускает отложенное форматирование"""
        if self._formatting_time:
            return
        
        # Отменяем предыдущий таймер
        if self._format_timer:
            self.ent_time.after_cancel(self._format_timer)
        
        # Запускаем новый таймер на 500мс
        self._format_timer = self.ent_time.after(500, self._do_format)
    
    def _format_immediately(self, event=None):
        """Форматирует немедленно (при FocusOut или Enter)"""
        if self._format_timer:
            self.ent_time.after_cancel(self._format_timer)
            self._format_timer = None
        self._do_format()
        return None
    
    def _do_format(self):
        """Выполняет форматирование"""
        if self._formatting_time:
            return
        
        current = self.time_var.get()
        formatted = self._auto_format_time_input(current)
        
        if formatted != current:
            self._formatting_time = True
            try:
                cursor_pos = self.ent_time.index(tk.INSERT)
                self.time_var.set(formatted)
                # Ставим курсор в конец
                self.ent_time.icursor(tk.END)
            finally:
                self._formatting_time = False
    
    def _auto_format_time_input(self, raw: str) -> str:
        """
        Автоматически форматирует ввод времени в формат ЧЧ:ММ
        Примеры:
        - '8' → '08:00'
        - '13' → '13:00'
        - '130' → '01:30'
        - '1300' → '13:00'
        - '13.00' → '13:00'
        - '9.45' → '09:45'
        """
        if not raw:
            return ""
        
        # Удаляем все кроме цифр
        digits = ''.join(c for c in raw if c.isdigit())
        
        if not digits:
            return ""
        
        # Форматируем в зависимости от количества цифр
        if len(digits) == 1:
            # '8' → '08:00'
            hh = int(digits)
            return f"{hh:02d}:00"
        elif len(digits) == 2:
            # '13' → '13:00'
            hh = min(int(digits), 23)
            return f"{hh:02d}:00"
        elif len(digits) == 3:
            # '130' → '01:30' (интерпретируем как Ч:ММ)
            hh = int(digits[0])
            mm = min(int(digits[1:3]), 59)
            return f"{hh:02d}:{mm:02d}"
        else:  # 4 или больше цифр
            # '1300' → '13:00' (интерпретируем как ЧЧММ)
            hh = min(int(digits[:2]), 23)
            mm = min(int(digits[2:4]), 59)
            return f"{hh:02d}:{mm:02d}"
    # ===============================================

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
                pass

    def _delete(self):
        self.on_delete(self)

    def validate(self) -> bool:
        ok = True
        val = (self.cmb_tech.get() or "").strip()
        if not val:
            self._mark_err(self.cmb_tech); ok = False
        else:
            self._clear_err(self.cmb_tech)
        try:
            qty = int((self.ent_qty.get() or "0").strip())
            if qty <= 0: raise ValueError
            self._clear_err(self.ent_qty)
        except Exception:
            self._mark_err(self.ent_qty); ok = False
        tstr = (self.ent_time.get() or "").strip()
        if tstr:
            if parse_time_str(tstr) is None:
                self._mark_err(self.ent_time); ok = False
            else:
                self._clear_err(self.ent_time)
        else:
            self._clear_err(self.ent_time)
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
        
# ------------------------- HTTP -------------------------

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


# ------------------------- Встраиваемая страница -------------------------

class SpecialOrdersPage(tk.Frame):
    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        ensure_config()  # на всякий случай
        self.base_dir = exe_dir()
        self.spr_path = get_spr_path()
        self.orders_dir = self.base_dir / ORDERS_DIR
        self.orders_dir.mkdir(parents=True, exist_ok=True)

        self._load_spr()
        self._build_ui()

    # Ниже — те же методы, что использует standalone-окно, но работают в рамках Frame
    def _load_spr(self):
        employees, objects, tech = load_spravochnik_remote_or_local(self.spr_path)
        self.emps = [{'fio': fio, 'tbn': tbn, 'pos': pos, 'dep': dep} for (fio, tbn, pos, dep) in employees]
        self.objects = objects

        # ========== ТЕХНИКА: ТОЛЬКО УНИКАЛЬНЫЕ ТИПЫ ДЛЯ ЗАЯВКИ ==========
        self.techs = []
        tech_types = set()  # Собираем уникальные типы
    
        for tp, nm, pl, dep, note in tech:
            if tp:  # Добавляем только если тип указан
                tech_types.add(tp)
        
            # ВАЖНО: сохраняем полную информацию для справки
            self.techs.append({
                'type': tp, 
                'name': nm, 
                'plate': pl, 
                'dep': dep, 
                'note': note
            })
    
        # Для выпадающего списка в заявке - только типы (отсортированные)
        self.tech_values = sorted(list(tech_types))
        # ================================================================

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

    def _build_ui(self):
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(fill="x", padx=10, pady=8)

        tk.Label(top, text="Подразделение:", bg="#f7f7f7").grid(row=0, column=0, sticky="w")
        self.cmb_dep = ttk.Combobox(top, state="readonly", values=self.deps, width=48)
        saved_dep = get_saved_dep()
        self.cmb_dep.set(saved_dep if saved_dep in self.deps else self.deps[0])
        self.cmb_dep.grid(row=0, column=1, sticky="w", padx=(4, 12))
        self.cmb_dep.bind("<<ComboboxSelected>>",
                          lambda e: (set_saved_dep(self.cmb_dep.get()), self._update_fio_list(), self._update_cutoff_hint()))

        tk.Label(top, text="ФИО:", bg="#f7f7f7").grid(row=0, column=2, sticky="w")
        self.fio_var = tk.StringVar()
        self.cmb_fio = AutoCompleteCombobox(top, textvariable=self.fio_var, width=36)
        self.cmb_fio.grid(row=0, column=3, sticky="w", padx=(4, 12))

        tk.Label(top, text="Телефон:", bg="#f7f7f7").grid(row=0, column=4, sticky="w")
        self.ent_phone = ttk.Entry(top, width=18)
        self.ent_phone.grid(row=0, column=5, sticky="w", padx=(4, 12))

        tk.Label(top, text="Дата:", bg="#f7f7f7").grid(row=0, column=6, sticky="w")
        self.ent_date = ttk.Entry(top, width=12)
        self.ent_date.grid(row=0, column=7, sticky="w", padx=(4, 0))
        self.ent_date.insert(0, date.today().strftime("%Y-%m-%d"))
        self.ent_date.bind("<KeyRelease>", lambda e: self._update_cutoff_hint())
        self.ent_date.bind("<FocusOut>", lambda e: self._update_cutoff_hint())

        tk.Label(top, text="Адрес:", bg="#f7f7f7").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.cmb_address = AutoCompleteCombobox(top, width=56)
        self.cmb_address.set_completion_list(self.addresses)
        self.cmb_address.grid(row=1, column=1, columnspan=3, sticky="w", padx=(4, 12), pady=(8, 0))
        self.cmb_address.bind("<<ComboboxSelected>>", lambda e: self._sync_ids_by_address())
        self.cmb_address.bind("<FocusOut>", lambda e: self._sync_ids_by_address())
        self.cmb_address.bind("<Return>", lambda e: self._sync_ids_by_address())

        tk.Label(top, text="ID объекта:", bg="#f7f7f7").grid(row=1, column=4, sticky="w", pady=(8, 0))
        self.cmb_object_id = ttk.Combobox(top, state="readonly", values=[], width=20)
        self.cmb_object_id.grid(row=1, column=5, sticky="w", padx=(4, 12), pady=(8, 0))

        self.lbl_cutoff_hint = tk.Label(top, text="", fg="#555", bg="#f7f7f7")
        self.lbl_cutoff_hint.grid(row=1, column=6, columnspan=2, sticky="w", pady=(8, 0))

        tk.Label(top, text="Комментарий:", bg="#f7f7f7").grid(row=2, column=0, sticky="nw", pady=(8, 0))
        self.txt_comment = tk.Text(top, height=3, width=96)
        self.txt_comment.grid(row=2, column=1, columnspan=7, sticky="we", padx=(4, 0), pady=(8, 0))

        pos_wrap = tk.LabelFrame(self, text="Позиции")
        pos_wrap.pack(fill="both", expand=True, padx=10, pady=(6, 8))

        hdr = tk.Frame(pos_wrap)
        hdr.pack(fill="x")
        tk.Label(hdr, text="Техника", width=52, anchor="w").grid(row=0, column=0, padx=2)
        tk.Label(hdr, text="Кол-во", width=6, anchor="center").grid(row=0, column=1, padx=2)
        tk.Label(hdr, text="Подача (чч:мм)", width=12, anchor="center").grid(row=0, column=2, padx=2)
        tk.Label(hdr, text="Часы", width=10, anchor="center").grid(row=0, column=3, padx=2)
        tk.Label(hdr, text="Примечание", width=38, anchor="w").grid(row=0, column=4, padx=2)
        tk.Label(hdr, text="Действие", width=10, anchor="center").grid(row=0, column=5, padx=2)

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

        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(bottom, text="Сохранить заявку", command=self.save_order).pack(side="left", padx=4)
        ttk.Button(bottom, text="Очистить форму", command=self.clear_form).pack(side="left", padx=4)
        ttk.Button(bottom, text="Открыть папку заявок", command=self.open_orders_dir).pack(side="left", padx=4)

        self._update_fio_list()
        self._update_cutoff_hint()
        self.add_position()

        for c in range(8):
            top.grid_columnconfigure(c, weight=0)
        top.grid_columnconfigure(1, weight=1)
        top.grid_columnconfigure(5, weight=0)

    # Методы логики/валидации/сохранения — те же, что и в standalone
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
            filtered = [r['fio'] for r in self.emps]
        self.cmb_fio.set_completion_list(filtered)

    def _update_cutoff_hint(self):
        if not get_cutoff_enabled():
            self.lbl_cutoff_hint.config(text="", fg="#555")
            return
        ch = get_cutoff_hour()
        hint_base = f"Приём заявок до {ch:02d}:00 (на текущую дату)"
        req = parse_date_any(self.ent_date.get())
        today = date.today()
        if req is None:
            self.lbl_cutoff_hint.config(text=hint_base, fg="#555")
            return
        if req < today:
            self.lbl_cutoff_hint.config(text="Выбрана прошедшая дата — заявки недоступны", fg="#b00020")
        elif req == today and is_past_cutoff_for_date(today, ch):
            self.lbl_cutoff_hint.config(text=f"Сегодня приём закрыт после {ch:02d}:00", fg="#b00020")
        else:
            self.lbl_cutoff_hint.config(text=hint_base, fg="#555")

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
        for i, r in enumerate(self.pos_rows, start=0):
            r.grid(i)
            r.apply_zebra(i)

    def _validate_form(self) -> bool:
        ok = True
        if not (self.cmb_dep.get() or "").strip():
            ok = False
        if not (self.cmb_fio.get() or "").strip():
            ok = False
        d = parse_date_any(self.ent_date.get())
        if d is None:
            messagebox.showwarning("Заявка", "Введите корректную дату (YYYY-MM-DD или DD.MM.YYYY).")
            return False
        addr = (self.cmb_address.get() or "").strip()
        oid = (self.cmb_object_id.get() or "").strip()
        if not addr and not oid:
            messagebox.showwarning("Заявка", "Укажите Адрес и/или ID объекта.")
            return False
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

        try:
            req_date = parse_date_any(self.ent_date.get()) or date.today()
            if req_date < date.today():
                messagebox.showwarning("Заявка",
                                       "Заявки на прошедшую дату не принимаются.\nВыберите сегодняшнюю или будущую дату.")
                return
        except Exception:
            pass

        try:
            req_date = parse_date_any(self.ent_date.get()) or date.today()
            if get_cutoff_enabled() and is_past_cutoff_for_date(req_date, get_cutoff_hour()):
                ch = get_cutoff_hour()
                messagebox.showwarning("Заявка",
                                       f"Приём заявок на текущую дату закрыт после {ch:02d}:00.\n"
                                       f"Выберите завтрашнюю дату и повторите.")
                return
        except Exception:
            pass

        data = self._build_order_dict()

        ts = datetime.now().strftime("%H%M%S")
        id_part = data["object"]["id"] or safe_filename(data["object"]["address"])
        fname = f"Заявка_спецтехники_{data['date']}_{ts}_{id_part or 'NOID'}.xlsx"
        fpath = self.orders_dir / fname

        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Заявка"
            ws.append(["Создано", data["created_at"]])
            ws.append(["Дата", data["date"]])
            ws.append(["Подразделение", data["department"]])
            ws.append(["Заявитель (ФИО)", data["requester_fio"]])
            ws.append(["Телефон", data["requester_phone"]])
            ws.append(["ID объекта", data["object"]["id"]])
            ws.append(["Адрес", data["object"]["address"]])
            ws.append(["Комментарий", data["comment"]])
            ws.append([])
            hdr = ["#", "Техника", "Кол-во", "Подача (чч:мм)", "Часы", "Примечание"]
            ws.append(hdr)
            for i, p in enumerate(data["positions"], start=1):
                ws.append([i, p["tech"], p["qty"], (p["time"] or None), p["hours"], p["note"]])
            for col, w in enumerate([4, 48, 8, 14, 10, 36], start=1):
                ws.column_dimensions[get_column_letter(col)].width = w
            ws.freeze_panes = "A12"
            wb.save(fpath)
        except Exception as e:
            messagebox.showerror("Сохранение", f"Не удалось сохранить XLSX:\n{e}")
            return

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

        try:
            mode = get_orders_mode()
            if mode == 'webhook':
                url = get_orders_webhook_url()
                token = get_orders_webhook_token()
                if url:
                    ok, info = post_json(url, data, token)
                    if ok:
                        messagebox.showinfo("Сохранение/Отправка",
                                            f"Заявка сохранена и отправлена онлайн.\n\n"
                                            f"XLSX:\n{fpath}\nCSV:\n{csv_path}\n\nОтвет сервера:\n{info}")
                    else:
                        messagebox.showwarning("Сохранение/Отправка",
                                               f"Локально сохранено, но онлайн-отправка не удалась.\n\n"
                                               f"XLSX:\n{fpath}\nCSV:\n{csv_path}\n\n{info}")
                    return
                else:
                    messagebox.showinfo("Сохранение",
                                        f"Заявка сохранена:\n{fpath}\n\nСводный CSV:\n{csv_path}\n"
                                        f"(Онлайн-отправка не настроена)")
                    return
            else:
                messagebox.showinfo("Сохранение", f"Заявка сохранена:\n{fpath}\n\nСводный CSV:\n{csv_path}")
                return
        except Exception as e:
            messagebox.showwarning("Сохранение/Отправка",
                                   f"Локально сохранено, но онлайн-отправка упала с ошибкой:\n{e}\n\n"
                                   f"XLSX:\n{fpath}\nCSV:\n{csv_path}")
            return

    def clear_form(self):
        self.fio_var.set("")
        self.ent_phone.delete(0, "end")
        self.ent_date.delete(0, "end")
        self.ent_date.insert(0, date.today().strftime("%Y-%m-%d"))
        self.cmb_address.set("")
        self.cmb_object_id.config(values=[])
        self.cmb_object_id.set("")
        self.txt_comment.delete("1.0", "end")
        for r in self.pos_rows:
            r.destroy()
        self.pos_rows.clear()
        self.add_position()
        self._update_cutoff_hint()

    def open_orders_dir(self):
        try:
            os.startfile(self.orders_dir)
        except Exception as e:
            messagebox.showerror("Папка", f"Не удалось открыть папку:\n{e}")

# ------------------------- Планирование транспорта -------------------------

class TransportPlanningPage(tk.Frame):
    """Вкладка планирования транспорта"""
    
    def __init__(self, master):
        super().__init__(master, bg="#f7f7f7")
        self.spr_path = get_spr_path()
        self._load_spr()
        self._build_ui()
        
    def _load_spr(self):
        """Загрузка справочника"""
        employees, objects, tech = load_spravochnik_remote_or_local(self.spr_path)
    
        # ========== ТРАНСПОРТ: полная структура для каскадных списков ==========
        self.vehicles = []
        self.vehicle_types = set()
    
        for tp, nm, pl, dep, note in tech:
            self.vehicles.append({
                'type': tp, 
                'name': nm, 
                'plate': pl, 
                'dep': dep, 
                'note': note
            })
            if tp:
                self.vehicle_types.add(tp)
    
        # Сортируем типы
        self.vehicle_types = sorted(list(self.vehicle_types))
        # ======================================================================
    
        # Водители
        cfg = read_config()
        driver_depts_str = cfg.get(
            CONFIG_SECTION_INTEGR, 
            KEY_DRIVER_DEPARTMENTS, 
            fallback="Служба гаража"
        )
        DRIVER_DEPARTMENTS = [d.strip() for d in driver_depts_str.split(",") if d.strip()]
    
        self.drivers = []
        for fio, tbn, pos, dep in employees:
            is_driver_dept = dep in DRIVER_DEPARTMENTS
            is_driver_pos = 'водитель' in pos.lower()
        
            if is_driver_dept or is_driver_pos:
                self.drivers.append({
                    'fio': fio, 
                    'tbn': tbn, 
                    'pos': pos,
                    'dep': dep
                })
    
        self.drivers.sort(key=lambda x: x['fio'])
        self.departments = ["Все"] + sorted({dep for _, _, _, dep in employees if dep})
        
    def _build_ui(self):
        """Построение интерфейса"""
        # Верхняя панель с фильтрами
        top = tk.Frame(self, bg="#f7f7f7")
        top.pack(fill="x", padx=10, pady=8)
        
        tk.Label(top, text="Дата:", bg="#f7f7f7").grid(row=0, column=0, sticky="w")
        self.ent_filter_date = ttk.Entry(top, width=12)
        self.ent_filter_date.grid(row=0, column=1, padx=4)
        self.ent_filter_date.insert(0, date.today().strftime("%Y-%m-%d"))
        
        tk.Label(top, text="Подразделение:", bg="#f7f7f7").grid(row=0, column=2, sticky="w", padx=(12,0))
        self.cmb_filter_dep = ttk.Combobox(top, state="readonly", values=self.departments, width=20)
        self.cmb_filter_dep.set("Все")
        self.cmb_filter_dep.grid(row=0, column=3, padx=4)
        
        tk.Label(top, text="Статус:", bg="#f7f7f7").grid(row=0, column=4, sticky="w", padx=(12,0))
        self.cmb_filter_status = ttk.Combobox(
            top, state="readonly", 
            values=["Все", "Новая", "Назначена", "В работе", "Выполнена"], 
            width=15
        )
        self.cmb_filter_status.set("Все")
        self.cmb_filter_status.grid(row=0, column=5, padx=4)
        
        ttk.Button(top, text="🔄 Обновить", command=self.load_orders).grid(row=0, column=6, padx=12)
        ttk.Button(top, text="💾 Сохранить назначения", command=self.save_assignments).grid(row=0, column=7, padx=4)
        
        # Таблица заявок
        table_frame = tk.Frame(self)
        table_frame.pack(fill="both", expand=True, padx=10, pady=8)
        
        # Создаем Treeview с колонками
        columns = (
            "id", "created", "date", "dept", "requester", 
            "object", "tech", "qty", "time", "hours", 
            "assigned_vehicle", "driver", "status"
        )
        
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=20)
        
        # Заголовки
        headers = {
            "id": "ID", "created": "Создано", "date": "Дата", 
            "dept": "Подразделение", "requester": "Заявитель",
            "object": "Объект/Адрес", "tech": "Техника", "qty": "Кол-во",
            "time": "Подача", "hours": "Часы", 
            "assigned_vehicle": "Назначен авто", "driver": "Водитель", 
            "status": "Статус"
        }
        
        widths = {
            "id": 80, "created": 130, "date": 90, "dept": 120, 
            "requester": 150, "object": 200, "tech": 180, 
            "qty": 50, "time": 60, "hours": 50, 
            "assigned_vehicle": 180, "driver": 150, "status": 100
        }
        
        for col in columns:
            self.tree.heading(col, text=headers.get(col, col))
            self.tree.column(col, width=widths.get(col, 100))
        
        # Скроллбары
        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)
        
        # Двойной клик для редактирования
        self.tree.bind("<Double-1>", self.on_row_double_click)
        
        # Цветовое выделение по статусам
        self.tree.tag_configure('Новая', background='#fff3cd')
        self.tree.tag_configure('Назначена', background='#d1ecf1')
        self.tree.tag_configure('В работе', background='#d4edda')
        self.tree.tag_configure('Выполнена', background='#e2e3e5')
        
    def load_orders(self):
        """Загрузка заявок из Google Таблиц"""
        try:
            url = get_orders_webhook_url()
            
            if not url:
                messagebox.showwarning("Загрузка", "Не настроен webhook URL в конфигурации")
                return
            
            token = get_orders_webhook_token()
            filter_date = self.ent_filter_date.get().strip()
            filter_dept = self.cmb_filter_dep.get()
            filter_status = self.cmb_filter_status.get()
            
            # GET запрос
            params = {}
            if filter_date:
                params['date'] = filter_date
            if filter_dept and filter_dept != "Все":
                params['department'] = filter_dept
            if filter_status and filter_status != "Все":
                params['status'] = filter_status
            if token:
                params['token'] = token
                
            query = urllib.parse.urlencode(params)
            full_url = f"{url}?{query}" if query else url
            
            with urllib.request.urlopen(full_url, timeout=15) as resp:
                result = json.loads(resp.read().decode('utf-8'))
            
            if not result.get('ok'):
                messagebox.showerror("Ошибка", f"Сервер вернул ошибку:\n{result.get('error', 'Unknown')}")
                return
            
            orders = result.get('orders', [])
            self._populate_tree(orders)
            messagebox.showinfo("Загрузка", f"Загружено заявок: {len(orders)}")
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить заявки:\n{e}")

    def _check_vehicle_conflict(self, vehicle_full: str, req_date: str, req_time: str, current_id: str) -> List[Dict]:
        """
        Проверяет, не назначен ли этот автомобиль на другую заявку в это же время
    
        vehicle_full: "Автокран | КС-45717 | А123ВС77"
        """
        if not vehicle_full or not req_date:
            return []
    
        conflicts = []
    
        for item_id in self.tree.get_children():
            values = self.tree.item(item_id)['values']
        
            # Пропускаем текущую заявку
            if values[0] == current_id:
                continue
        
            other_date = values[2]          # Дата
            other_vehicle = values[10]      # Назначенный авто
            other_time = values[8]          # Подача
            other_requester = values[4]     # Заявитель
            other_object = values[5]        # Объект
            other_status = values[12]       # Статус
        
            # Проверяем совпадение
            if (other_vehicle == vehicle_full and 
                other_date == req_date and
                other_status not in ['Выполнена', 'Отменена']):
            
                # Если время не указано - считаем потенциальным конфликтом
                if not req_time or not other_time:
                    conflicts.append({
                        'time': other_time or 'не указано',
                        'requester': other_requester,
                        'object': other_object,
                        'status': other_status
                    })
                # Если время указано - проверяем пересечение
                elif req_time == other_time:
                    conflicts.append({
                        'time': other_time,
                        'requester': other_requester,
                        'object': other_object,
                        'status': other_status
                    })
    
        return conflicts
    
    def _populate_tree(self, orders: List[Dict]):
        """Заполнение таблицы заявками"""
        # Очищаем таблицу
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        for order in orders:
            obj_display = order.get('object_address', '') or order.get('object_id', '')
            status = order.get('status', 'Новая')
            
            item_id = self.tree.insert("", "end", values=(
                order.get('id', ''),
                order.get('created_at', ''),
                order.get('date', ''),
                order.get('department', ''),
                order.get('requester_fio', ''),
                obj_display,
                order.get('tech', ''),
                order.get('qty', ''),
                order.get('time', ''),
                order.get('hours', ''),
                order.get('assigned_vehicle', ''),
                order.get('driver', ''),
                status
            ), tags=(status,))
    
    def on_row_double_click(self, event):
        """Открытие окна редактирования назначения"""
        selection = self.tree.selection()
        if not selection:
            return
        
        item = self.tree.item(selection[0])
        values = item['values']
        
        # Открываем диалог для назначения транспорта
        self._show_assignment_dialog(selection[0], values)

    def _show_assignment_dialog(self, item_id, values):
        """Диалог назначения транспорта и водителя"""
        dialog = tk.Toplevel(self)
        dialog.title("Назначение транспорта")
        dialog.geometry("640x700")
        dialog.resizable(True, True)
        dialog.transient(self)
        dialog.grab_set()

        # Центрируем
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (640 // 2)
        y = (dialog.winfo_screenheight() // 2) - (700 // 2)
        dialog.geometry(f"640x700+{x}+{y}")

        # ========== КОНТЕЙНЕР СО СКРОЛЛОМ ==========
        scroll_container = tk.Frame(dialog)
        scroll_container.pack(fill="both", expand=True, padx=0, pady=0)

        canvas = tk.Canvas(scroll_container, highlightthickness=0)
        scrollbar = ttk.Scrollbar(scroll_container, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas)

        def update_scroll_region(event=None):
            canvas.configure(scrollregion=canvas.bbox("all"))

            scrollable_frame.bind("<Configure>", update_scroll_region)

        canvas_window = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        # Адаптация ширины
        def on_canvas_configure(event):
            canvas.itemconfig(canvas_window, width=event.width)
        canvas.bind("<Configure>", on_canvas_configure)

        # Прокрутка колесиком
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")

        def bind_mousewheel(event=None):
            canvas.bind_all("<MouseWheel>", _on_mousewheel)

        def unbind_mousewheel(event=None):
            canvas.unbind_all("<MouseWheel>")

        canvas.bind("<Enter>", bind_mousewheel)
        canvas.bind("<Leave>", unbind_mousewheel)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # ========== СОДЕРЖИМОЕ ==========

        # Информация о заявке
        info_frame = tk.LabelFrame(scrollable_frame, text="📋 Информация о заявке", padx=12, pady=10)
        info_frame.pack(fill="x", padx=15, pady=10)

        info_data = [
            ("Дата:", values[2]),
            ("Время подачи:", values[8] or 'не указано'),
            ("Заявитель:", values[4]),
            ("Объект:", values[5]),
        ]

        for label, value in info_data:
            row = tk.Frame(info_frame)
            row.pack(fill="x", pady=2)
            tk.Label(row, text=label, font=("Arial", 9), width=15, anchor="w").pack(side="left")
            tk.Label(row, text=value, font=("Arial", 9), anchor="w").pack(side="left", fill="x", expand=True)

        # Техника
        tech_frame = tk.Frame(info_frame, bg="#e3f2fd", relief="solid", borderwidth=1)
        tech_frame.pack(fill="x", pady=(8, 2), padx=5)
        tk.Label(
            tech_frame, 
            text=f"🚛 Техника: {values[6]} x {values[7]} ({values[9]} ч.)", 
            font=("Arial", 10, "bold"), 
            fg="#0066cc",
            bg="#e3f2fd",
            padx=8,
            pady=8
        ).pack(anchor="w")

        # Предупреждение о конфликтах
        warning_frame = tk.Frame(scrollable_frame, bg="#fff3cd", relief="solid", borderwidth=1)
        warning_label = tk.Label(
            warning_frame, 
            text="", 
            font=("Arial", 9), 
            bg="#fff3cd",
            fg="#856404",
            wraplength=580,
            justify="left"
        )
        warning_label.pack(padx=10, pady=8)

        # Назначение транспорта
        assign_frame = tk.LabelFrame(scrollable_frame, text="🚗 Назначение транспорта", padx=15, pady=15)
        assign_frame.pack(fill="both", expand=True, padx=15, pady=5)

        # ========== ПАРСИМ ТЕКУЩЕЕ НАЗНАЧЕНИЕ ==========
        current_assignment = values[10]
        current_type = ""
        current_name = ""
        current_plate = ""

        if current_assignment and " | " in current_assignment:
            parts = current_assignment.split(" | ")
            current_type = parts[0].strip() if len(parts) > 0 else ""
            current_name = parts[1].strip() if len(parts) > 1 else ""
            current_plate = parts[2].strip() if len(parts) > 2 else ""
        elif current_assignment:
            current_type = current_assignment.strip()

        # ========== 1. ТИП ТЕХНИКИ ==========
        tk.Label(assign_frame, text="1️⃣ Тип техники:", font=("Arial", 9, "bold")).grid(
            row=0, column=0, sticky="w", pady=(5, 2)
        )
        vehicle_type_var = tk.StringVar(value=current_type)
        cmb_vehicle_type = ttk.Combobox(
            assign_frame, 
            textvariable=vehicle_type_var,
            values=self.vehicle_types,
            state="readonly",
            width=55,
            font=("Arial", 9)
        )
        cmb_vehicle_type.grid(row=1, column=0, pady=(0, 12), sticky="we")

        # ========== 2. НАИМЕНОВАНИЕ ==========
        tk.Label(assign_frame, text="2️⃣ Наименование:", font=("Arial", 9, "bold")).grid(
            row=2, column=0, sticky="w", pady=(5, 2)
        )
        vehicle_name_var = tk.StringVar(value="")
        cmb_vehicle_name = ttk.Combobox(
            assign_frame, 
            textvariable=vehicle_name_var,
            values=[],
            state="readonly",
            width=55,
            font=("Arial", 9)
        )
        cmb_vehicle_name.grid(row=3, column=0, pady=(0, 12), sticky="we")

        # ========== 3. ГОС. НОМЕР ==========
        tk.Label(assign_frame, text="3️⃣ Гос. номер:", font=("Arial", 9, "bold")).grid(
            row=4, column=0, sticky="w", pady=(5, 2)
        )
        vehicle_plate_var = tk.StringVar(value="")
        cmb_vehicle_plate = ttk.Combobox(
            assign_frame, 
            textvariable=vehicle_plate_var,
            values=[],
            state="readonly",
            width=55,
            font=("Arial", 9)
        )
        cmb_vehicle_plate.grid(row=5, column=0, pady=(0, 12), sticky="we")

        # Информация о выборе
        selection_info = tk.Label(
            assign_frame,
            text="💡 Выберите сначала тип, затем наименование и гос. номер",
            font=("Arial", 8),
            fg="#666"
        )
        selection_info.grid(row=6, column=0, sticky="w", pady=(0, 10))

        # ========== ЛОГИКА КАСКАДНЫХ СПИСКОВ ==========

        def update_names(*args):
            selected_type = vehicle_type_var.get()
            vehicle_name_var.set("")
            vehicle_plate_var.set("")
    
            if not selected_type:
                cmb_vehicle_name['values'] = []
                cmb_vehicle_plate['values'] = []
                cmb_vehicle_name.state(['disabled'])
                cmb_vehicle_plate.state(['disabled'])
                selection_info.config(text="💡 Выберите тип техники", fg="#666")
                return
    
            names = sorted(set(
                v['name'] for v in self.vehicles 
                if v['type'] == selected_type and v['name']
            ))
    
            cmb_vehicle_name['values'] = names
            cmb_vehicle_name.state(['!disabled'])
            cmb_vehicle_plate['values'] = []
            cmb_vehicle_plate.state(['disabled'])
    
            if len(names) == 0:
                selection_info.config(text="⚠️ Нет доступных наименований для этого типа", fg="#dc3545")
            elif len(names) == 1:
                vehicle_name_var.set(names[0])
                # Не вызываем update_plates() здесь, он сработает по trace
            else:
                selection_info.config(text=f"💡 Доступно наименований: {len(names)}", fg="#666")

        def update_plates(*args):
            selected_type = vehicle_type_var.get()
            selected_name = vehicle_name_var.get()
            vehicle_plate_var.set("")
    
            if not selected_type or not selected_name:
                cmb_vehicle_plate['values'] = []
                cmb_vehicle_plate.state(['disabled'])
                return
    
            plates = sorted(set(
                v['plate'] for v in self.vehicles 
                if v['type'] == selected_type 
                and v['name'] == selected_name 
                and v['plate']
            ))
    
            cmb_vehicle_plate['values'] = plates
            cmb_vehicle_plate.state(['!disabled'])
    
            if len(plates) == 0:
                selection_info.config(text="⚠️ Нет доступных гос. номеров", fg="#dc3545")
            elif len(plates) == 1:
                vehicle_plate_var.set(plates[0])
                # check_conflicts() вызовется по trace
                selection_info.config(text=f"✓ Назначен: {get_full_vehicle_string()}", fg="#28a745")
            else:
                selection_info.config(text=f"💡 Доступно гос. номеров: {len(plates)}", fg="#666")

        def get_full_vehicle_string() -> str:
            parts = []
            if vehicle_type_var.get():
                parts.append(vehicle_type_var.get())
            if vehicle_name_var.get():
                parts.append(vehicle_name_var.get())
            if vehicle_plate_var.get():
                parts.append(vehicle_plate_var.get())
            return " | ".join(parts) if parts else ""

        # Привязываем обработчики
        vehicle_type_var.trace_add("write", update_names)
        vehicle_name_var.trace_add("write", update_plates)
        vehicle_plate_var.trace_add("write", lambda *args: check_conflicts())

        # Разделитель
        ttk.Separator(assign_frame, orient='horizontal').grid(
            row=7, column=0, sticky='ew', pady=15
        )

        # Водитель
        tk.Label(assign_frame, text="👨‍✈️ Водитель:", font=("Arial", 9, "bold")).grid(
            row=8, column=0, sticky="w", pady=(5, 2)
        )

        driver_count_label = tk.Label(
            assign_frame, 
            text=f"(доступно: {len(self.drivers)} чел.)",
            font=("Arial", 8),
            fg="#666"
        )
        driver_count_label.grid(row=8, column=0, sticky="e", pady=(5, 2))

        driver_var = tk.StringVar(value=values[11])

        driver_display_list = []
        for d in self.drivers:
            display = f"{d['fio']}"
            if d.get('dep'):
                display += f" ({d['dep']})"
            driver_display_list.append(display)

        cmb_driver = ttk.Combobox(
            assign_frame,
            textvariable=driver_var,
            values=driver_display_list,
            width=55,
            font=("Arial", 9)
        )
        cmb_driver.grid(row=9, column=0, pady=(0, 12), sticky="we")

        # Статус
        tk.Label(assign_frame, text="📊 Статус:", font=("Arial", 9, "bold")).grid(
            row=10, column=0, sticky="w", pady=(5, 2)
        )
        status_var = tk.StringVar(value=values[12])
        cmb_status = ttk.Combobox(
            assign_frame,
            textvariable=status_var,
            values=["Новая", "Назначена", "В работе", "Выполнена"],
            state="readonly",
            width=55,
            font=("Arial", 9)
        )
        cmb_status.grid(row=11, column=0, pady=(0, 15), sticky="we")

        assign_frame.grid_columnconfigure(0, weight=1)

        # ========== ПРОВЕРКА КОНФЛИКТОВ ==========
        def check_conflicts(*args):
            selected_vehicle = get_full_vehicle_string()
            if not selected_vehicle:
                warning_frame.pack_forget()
                return
    
            req_date = values[2]
            req_time = values[8]
            current_id = values[0]
    
            conflicts = self._check_vehicle_conflict(selected_vehicle, req_date, req_time, current_id)
    
            if conflicts:
                warning_text = f"⚠️ ВНИМАНИЕ! Автомобиль '{selected_vehicle}' уже назначен на {len(conflicts)} заявку(-и) в этот день:\n\n"
                for i, conf in enumerate(conflicts, 1):
                    warning_text += f"{i}. {conf['time']} — {conf['requester']} ({conf['object']}) [{conf['status']}]\n"
                warning_text += "\nПроверьте возможность выполнения заявок!"
        
                warning_label.config(text=warning_text)
                warning_frame.pack(fill="x", padx=15, pady=(0, 5))
            else:
                warning_frame.pack_forget()

        # Автоматическое изменение статуса
        def on_vehicle_or_driver_change(*args):
            if get_full_vehicle_string() and driver_var.get():
                if status_var.get() == "Новая":
                    status_var.set("Назначена")

        vehicle_plate_var.trace_add("write", on_vehicle_or_driver_change)
        driver_var.trace_add("write", on_vehicle_or_driver_change)

        # ========== КНОПКИ (ФИКСИРОВАННЫЕ ВНИЗУ) ==========
        button_container = tk.Frame(dialog, bg="#f0f0f0", relief="raised", borderwidth=1)
        button_container.pack(fill="x", side="bottom", padx=0, pady=0)

        def save_and_close():
            if not get_full_vehicle_string():
                messagebox.showwarning("Назначение", "Выберите транспорт!", parent=dialog)
                return
    
            driver_name = driver_var.get()
            if " (" in driver_name:
                driver_name = driver_name.split(" (")[0].strip()
    
            new_values = list(values)
            new_values[10] = get_full_vehicle_string()
            new_values[11] = driver_name
            new_values[12] = status_var.get()
            self.tree.item(item_id, values=new_values, tags=(new_values[12],))
    
            unbind_mousewheel()
            dialog.destroy()

        def cancel_and_close():
            unbind_mousewheel()
            dialog.destroy()

        ttk.Button(
            button_container, 
            text="✓ Сохранить", 
            command=save_and_close, 
            width=20
        ).pack(side="left", padx=15, pady=12)

        ttk.Button(
            button_container, 
            text="✗ Отмена", 
            command=cancel_and_close, 
            width=20
        ).pack(side="left", padx=5, pady=12)

        # ========== КРИТИЧЕСКИ ВАЖНО: ПРИНУДИТЕЛЬНОЕ ОБНОВЛЕНИЕ ==========
        # Обновляем геометрию ДО инициализации значений
        dialog.update_idletasks()
        scrollable_frame.update_idletasks()
        canvas.update_idletasks()
    
        # Теперь инициализируем значения (это должно вызвать trace и отрисовать виджеты)
        if current_type:
            vehicle_type_var.set(current_type)
            dialog.update_idletasks()  # Даём время на обработку
        
            if current_name:
                vehicle_name_var.set(current_name)
                dialog.update_idletasks()
            
                if current_plate:
                    vehicle_plate_var.set(current_plate)
                    dialog.update_idletasks()

        # Финальное обновление области прокрутки
        canvas.configure(scrollregion=canvas.bbox("all"))
        canvas.yview_moveto(0)
    
        # Ещё одно обновление для гарантии
        dialog.update()
        # ================================================================

        cmb_vehicle_type.focus_set()
        dialog.bind("<Return>", lambda e: save_and_close())
        dialog.bind("<Escape>", lambda e: cancel_and_close())

        # Проверяем конфликты при открытии
        check_conflicts()

    def save_assignments(self):
        """Сохранение назначений в Google Таблицы"""
        try:
            # Собираем все назначения
            assignments = []
            for item in self.tree.get_children():
                values = self.tree.item(item)['values']
                assignments.append({
                    'id': values[0],
                    'assigned_vehicle': values[10],
                    'driver': values[11],
                    'status': values[12]
                })
            
            if not assignments:
                messagebox.showwarning("Сохранение", "Нет данных для сохранения")
                return
            
            # Отправляем на сервер
            url = get_orders_webhook_url()
            token = get_orders_webhook_token()
            
            payload = {
                'action': 'update_assignments',
                'assignments': assignments
            }
            
            ok, info = post_json(url, payload, token)
            
            if ok:
                messagebox.showinfo("Сохранение", f"Назначения успешно сохранены!\n\nОбновлено записей: {len(assignments)}")
            else:
                messagebox.showerror("Ошибка", f"Не удалось сохранить:\n{info}")
                
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка сохранения:\n{e}")


# Функция для создания страницы планирования
def create_planning_page(parent) -> tk.Frame:
    """Создаёт страницу планирования транспорта"""
    ensure_config()
    page = TransportPlanningPage(parent)
    page.pack(fill="both", expand=True)
    return page

# ------------------------- Вариант standalone-окна -------------------------

class SpecialOrdersApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1180x720")
        self.resizable(True, True)
        # Встроенная страница как корневой виджет
        page = SpecialOrdersPage(self)
        page.pack(fill="both", expand=True)


# ------------------------- API для встраивания -------------------------

def create_page(parent) -> tk.Frame:
    """
    Создаёт страницу "Заявка на автотранспорт" внутри переданного родителя.
    Возвращает tk.Frame (уже со построенным UI).
    """
    ensure_config()
    page = SpecialOrdersPage(parent)
    # Не пакуем здесь - родитель сам запакует
    return page

def create_planning_page(parent) -> tk.Frame:
    """
    Создаёт страницу "Планирование транспорта" внутри переданного родителя.
    Возвращает tk.Frame (уже со построенным UI).
    """
    ensure_config()
    page = TransportPlanningPage(parent)
    # Не пакуем здесь - родитель сам запакует
    return page

def open_special_orders(parent=None):
    """
    Совместимость: если parent задан — открываем Toplevel с встраиваемой страницей.
    Если не задан — отдельное окно как раньше.
    """
    if parent is None:
        app = SpecialOrdersApp()
        app.mainloop()
        return app
    # Toplevel, но UI — тот же встраиваемый
    win = tk.Toplevel(parent)
    win.title(APP_TITLE)
    win.geometry("1180x720")
    page = SpecialOrdersPage(win)
    page.pack(fill="both", expand=True)
    return win

# ------------------------- Утилиты -------------------------

def safe_filename(s: str, maxlen: int = 60) -> str:
    if not s:
        return "NOID"
    s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", str(s)).strip()
    s = re.sub(r"_+", "_", s)
    return s[:maxlen] if len(s) > maxlen else s


if __name__ == "__main__":
    ensure_config()
    app = SpecialOrdersApp()
    app.mainloop()
