# settings_manager.py
import os
import json
import hmac
import base64
import hashlib
import configparser
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
from pathlib import Path
from typing import Any, Dict, Optional

# Константы (совместимые с main_app)
CONFIG_FILE = "tabel_config.ini"
CONFIG_SECTION_PATHS = "Paths"
CONFIG_SECTION_UI = "UI"
CONFIG_SECTION_INTEGR = "Integrations"
CONFIG_SECTION_REMOTE = "Remote"
KEY_SPR = "spravochnik_path"
KEY_OUTPUT_DIR = "output_dir"
KEY_EXPORT_PWD = "export_password"
KEY_PLANNING_PASSWORD = "planning_password"
KEY_SELECTED_DEP = "selected_department"
KEY_REMOTE_USE = "use_remote"
KEY_YA_PUBLIC_LINK = "yadisk_public_link"
KEY_YA_PUBLIC_PATH = "yadisk_public_path"

SETTINGS_FILENAME = "settings.dat"  # зашифрованное хранилище

# ПАРОЛЬ ДОСТУПА К ОКНУ «НАСТРОЙКИ»
# поменяйте по своему усмотрению
SETTINGS_ACCESS_PASSWORD = "2025"

def exe_dir() -> Path:
    if getattr(__import__("sys"), "frozen", False):
        return Path(__import__("sys").executable).resolve().parent
    return Path(__file__).resolve().parent

SETTINGS_PATH = exe_dir() / SETTINGS_FILENAME
INI_PATH = exe_dir() / CONFIG_FILE

# ---------------- ШИФРОВАНИЕ ----------------

def _is_windows() -> bool:
    import platform
    return platform.system().lower().startswith("win")

def _dpapi_protect(data: bytes) -> bytes:
    import ctypes
    from ctypes import wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]

    CryptProtectData = ctypes.windll.crypt32.CryptProtectData
    CryptProtectData.argtypes = [ctypes.POINTER(DATA_BLOB), wintypes.LPWSTR,
                                 ctypes.POINTER(DATA_BLOB), ctypes.c_void_p,
                                 ctypes.c_void_p, wintypes.DWORD,
                                 ctypes.POINTER(DATA_BLOB)]
    CryptProtectData.restype = wintypes.BOOL

    in_blob = DATA_BLOB()
    in_blob.cbData = len(data)
    in_blob.pbData = ctypes.cast(ctypes.create_string_buffer(data), ctypes.POINTER(ctypes.c_char))

    out_blob = DATA_BLOB()
    if not CryptProtectData(ctypes.byref(in_blob), None, None, None, None, 0, ctypes.byref(out_blob)):
        raise RuntimeError("DPAPI protect failed")

    try:
        out = ctypes.string_at(out_blob.pbData, out_blob.cbData)
        return out
    finally:
        ctypes.windll.kernel32.LocalFree(out_blob.pbData)

def _dpapi_unprotect(data: bytes) -> bytes:
    import ctypes
    from ctypes import wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]

    CryptUnprotectData = ctypes.windll.crypt32.CryptUnprotectData
    CryptUnprotectData.argtypes = [ctypes.POINTER(DATA_BLOB), ctypes.POINTER(wintypes.LPWSTR),
                                   ctypes.POINTER(DATA_BLOB), ctypes.c_void_p,
                                   ctypes.c_void_p, wintypes.DWORD,
                                   ctypes.POINTER(DATA_BLOB)]
    CryptUnprotectData.restype = wintypes.BOOL

    in_blob = DATA_BLOB()
    in_blob.cbData = len(data)
    in_blob.pbData = ctypes.cast(ctypes.create_string_buffer(data), ctypes.POINTER(ctypes.c_char))

    out_blob = DATA_BLOB()
    if not CryptUnprotectData(ctypes.byref(in_blob), None, None, None, None, 0, ctypes.byref(out_blob)):
        raise RuntimeError("DPAPI unprotect failed")

    try:
        out = ctypes.string_at(out_blob.pbData, out_blob.cbData)
        return out
    finally:
        ctypes.windll.kernel32.LocalFree(out_blob.pbData)

def _fallback_key() -> bytes:
    user = os.environ.get("USERNAME") or os.environ.get("USER") or "user"
    root = str(exe_dir())
    payload = (user + "|" + root).encode("utf-8")
    return hashlib.sha256(payload).digest()

def _fallback_encrypt(data: bytes) -> bytes:
    key = _fallback_key()
    mac = hmac.new(key, data, hashlib.sha256).digest()
    return base64.b64encode(mac + data)

def _fallback_decrypt(packed: bytes) -> bytes:
    raw = base64.b64decode(packed)
    key = _fallback_key()
    mac = raw[:32]
    data = raw[32:]
    if not hmac.compare_digest(mac, hmac.new(key, data, hashlib.sha256).digest()):
        raise RuntimeError("Settings integrity check failed")
    return data

def _encrypt_dict(d: Dict[str, Any]) -> bytes:
    data = json.dumps(d, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if _is_windows():
        try:
            enc = _dpapi_protect(data)
            return b"WDP1" + enc
        except Exception:
            pass
    return b"FBK1" + _fallback_encrypt(data)

def _decrypt_dict(blob: bytes) -> Dict[str, Any]:
    if not blob:
        return {}
    if blob.startswith(b"WDP1"):
        data = _dpapi_unprotect(blob[4:])
        return json.loads(data.decode("utf-8"))
    if blob.startswith(b"FBK1"):
        data = _fallback_decrypt(blob[4:])
        return json.loads(data.decode("utf-8"))
    return json.loads(blob.decode("utf-8", errors="replace"))

# ---------------- ХРАНИЛИЩЕ НАСТРОЕК ----------------

_defaults: Dict[str, Dict[str, Any]] = {
    "Paths": {
        KEY_SPR: str(exe_dir() / "Справочник.xlsx"),
        KEY_OUTPUT_DIR: str(exe_dir() / "Объектные_табели"),
    },
    "UI": {
        KEY_SELECTED_DEP: "Все",
    },
    "Integrations": {
        KEY_EXPORT_PWD: "2025",
        KEY_PLANNING_PASSWORD: "2025",
        "orders_mode": "webhook",
        "orders_webhook_url": "",
        "planning_enabled": "false",
        "driver_departments": "",
    },
    "Remote": {
        KEY_REMOTE_USE: "false",
        KEY_YA_PUBLIC_LINK: "",
        KEY_YA_PUBLIC_PATH: "",
    },
    # Раздел Orders удален из UI и defaults
}

_store: Dict[str, Dict[str, Any]] = {}

def _ensure_sections():
    for sec, vals in _defaults.items():
        _store.setdefault(sec, {})
        for k, v in vals.items():
            if k not in _store[sec]:
                _store[sec][k] = v

def load_settings():
    global _store
    if SETTINGS_PATH.exists():
        try:
            raw = SETTINGS_PATH.read_bytes()
            _store = _decrypt_dict(raw)
            _ensure_sections()
            return
        except Exception:
            pass
    migrate_from_ini_or_create()

def save_settings():
    _ensure_sections()
    blob = _encrypt_dict(_store)
    SETTINGS_PATH.write_bytes(blob)

def migrate_from_ini_or_create():
    global _store
    cfg = configparser.ConfigParser()
    if INI_PATH.exists():
        try:
            cfg.read(INI_PATH, encoding="utf-8")
        except Exception:
            pass
    _store = json.loads(json.dumps(_defaults))
    for sec in _store.keys():
        if cfg.has_section(sec):
            for key in _store[sec].keys():
                val = cfg.get(sec, key, fallback=_store[sec][key])
                _store[sec][key] = val
    save_settings()

# Публичные API

def ensure_config():
    load_settings()

class _ProxyConfig:
    def get(self, section: str, key: str, fallback: Optional[str] = None) -> str:
        try:
            v = _store.get(section, {}).get(key, None)
            if v is None or v == "":
                return fallback if fallback is not None else ""
            return str(v)
        except Exception:
            return fallback if fallback is not None else ""

def read_config() -> _ProxyConfig:
    ensure_config()
    return _ProxyConfig()

def write_config(_cfg=None):
    save_settings()

def get_spr_path_from_config() -> Path:
    ensure_config()
    raw = _store["Paths"].get(KEY_SPR) or _defaults["Paths"][KEY_SPR]
    return Path(os.path.expandvars(str(raw)))

def get_output_dir_from_config() -> Path:
    ensure_config()
    raw = _store["Paths"].get(KEY_OUTPUT_DIR) or _defaults["Paths"][KEY_OUTPUT_DIR]
    return Path(os.path.expandvars(str(raw)))

def get_export_password_from_config() -> str:
    ensure_config()
    return str(_store["Integrations"].get(KEY_EXPORT_PWD, _defaults["Integrations"][KEY_EXPORT_PWD]))

def get_selected_department_from_config() -> str:
    ensure_config()
    return str(_store["UI"].get(KEY_SELECTED_DEP, _defaults["UI"][KEY_SELECTED_DEP]))

def set_selected_department_in_config(dep: str):
    ensure_config()
    _store["UI"][KEY_SELECTED_DEP] = dep or "Все"
    save_settings()

def get_remote_use() -> bool:
    ensure_config()
    v = str(_store["Remote"].get(KEY_REMOTE_USE, "false")).strip().lower()
    return v in ("1", "true", "yes", "on")

def get_yadisk_public_link() -> str:
    ensure_config()
    return str(_store["Remote"].get(KEY_YA_PUBLIC_LINK, ""))

def get_yadisk_public_path() -> str:
    ensure_config()
    return str(_store["Remote"].get(KEY_YA_PUBLIC_PATH, ""))

# ---------------- UI ОКНО НАСТРОЕК ----------------

# Храним Var-переменные для сохранения
_vars: Dict[str, Dict[str, Any]] = {}

def open_settings_window(parent: tk.Tk):
    ensure_config()

    # Запрос пароля
    pwd = simpledialog.askstring("Доступ к настройкам", "Введите пароль:", show="*", parent=parent)
    if pwd is None:
        return
    if pwd != SETTINGS_ACCESS_PASSWORD:
        messagebox.showerror("Доступ", "Неверный пароль.")
        return

    win = tk.Toplevel(parent)
    win.title("Настройки")
    win.resizable(False, False)
    nb = ttk.Notebook(win)
    nb.pack(fill="both", expand=True, padx=10, pady=10)

    # Настройки папок (Paths)
    tab_paths = ttk.Frame(nb)
    nb.add(tab_paths, text="Настройки папок")
    _mk_entry_with_btn(tab_paths, "Справочник (xlsx):", "Paths", KEY_SPR, is_dir=False, row=0)
    _mk_entry_with_btn(tab_paths, "Папка табелей:", "Paths", KEY_OUTPUT_DIR, is_dir=True, row=1)

    # Основное (UI)
    tab_ui = ttk.Frame(nb)
    nb.add(tab_ui, text="Основное")
    _mk_entry(tab_ui, "Подразделение по умолчанию:", "UI", KEY_SELECTED_DEP, row=0, width=40)

    # Интеграции (Integrations)
    tab_int = ttk.Frame(nb)
    nb.add(tab_int, text="Интеграции")
    _mk_entry(tab_int, "Экспорт (пароль):", "Integrations", KEY_EXPORT_PWD, row=0, width=20, show="*")
    _mk_entry(tab_int, "Планирование (пароль):", "Integrations", KEY_PLANNING_PASSWORD, row=1, width=20, show="*")
    _mk_entry(tab_int, "Настройка реестра:", "Integrations", "orders_mode", row=2, width=20)
    _mk_entry(tab_int, "URL скрипта реестра:", "Integrations", "orders_webhook_url", row=3, width=64)
    _mk_check(tab_int, "planning_enabled:", "Integrations", "planning_enabled", row=4)
    _mk_entry(tab_int, "Подразделения водителей:", "Integrations", "driver_departments", row=5, width=64)

    # Удаленный справочник (Remote)
    tab_rem = ttk.Frame(nb)
    nb.add(tab_rem, text="Удаленный справочник")
    _mk_check(tab_rem, "Включить удаленный справочник:", "Remote", KEY_REMOTE_USE, row=0)
    _mk_entry(tab_rem, "Публичная ссылка Я.Диска:", "Remote", KEY_YA_PUBLIC_LINK, row=1, width=64)
    _mk_entry(tab_rem, "Путь внутри публичной папки:", "Remote", KEY_YA_PUBLIC_PATH, row=2, width=40)

    # Кнопки
    btns = ttk.Frame(win)
    btns.pack(fill="x", padx=10, pady=(0, 10))
    ttk.Button(btns, text="Сохранить", command=lambda: (_save_from_vars(win), messagebox.showinfo("Настройки", "Сохранено")) ).pack(side="right", padx=4)
    ttk.Button(btns, text="Отмена", command=win.destroy).pack(side="right")

    # Центрирование окна
    try:
        win.update_idletasks()
        px, py = parent.winfo_rootx(), parent.winfo_rooty()
        pw, ph = parent.winfo_width(), parent.winfo_height()
        sw, sh = win.winfo_width(), win.winfo_height()
        win.geometry(f"+{px + (pw - sw)//2}+{py + (ph - sh)//2}")
    except Exception:
        pass

def _mk_entry(parent, label, section, key, row, width=30, show=None):
    ttk.Label(parent, text=label).grid(row=row, column=0, sticky="e", padx=(6, 6), pady=4)
    v = tk.StringVar(value=str(_store.get(section, {}).get(key, _defaults[section][key])))
    ent = ttk.Entry(parent, textvariable=v, width=width, show=show)
    ent.grid(row=row, column=1, sticky="w", padx=(0, 6), pady=4)
    _vars.setdefault(section, {})[key] = v

def _mk_check(parent, label, section, key, row):
    ttk.Label(parent, text=label).grid(row=row, column=0, sticky="e", padx=(6, 6), pady=4)
    cur = str(_store.get(section, {}).get(key, _defaults[section][key])).strip().lower() in ("1","true","yes","on")
    v = tk.BooleanVar(value=cur)
    cb = ttk.Checkbutton(parent, variable=v)
    cb.grid(row=row, column=1, sticky="w", padx=(0, 6), pady=4)
    _vars.setdefault(section, {})[key] = v

def _mk_entry_with_btn(parent, label, section, key, is_dir, row):
    ttk.Label(parent, text=label).grid(row=row, column=0, sticky="e", padx=(6, 6), pady=4)
    v = tk.StringVar(value=str(_store.get(section, {}).get(key, _defaults[section][key])))
    ent = ttk.Entry(parent, textvariable=v, width=60)
    ent.grid(row=row, column=1, sticky="w", padx=(0, 6), pady=4)
    def browse():
        if is_dir:
            d = filedialog.askdirectory(title="Выбор папки")
            if d:
                v.set(d)
        else:
            p = filedialog.askopenfilename(title="Выбор файла", filetypes=[("Excel", "*.xlsx;*.xls")])
            if p:
                v.set(p)
    ttk.Button(parent, text="...", width=3, command=browse).grid(row=row, column=2, sticky="w")
    _vars.setdefault(section, {})[key] = v

def _save_from_vars(win):
    for sec, kv in _vars.items():
        for k, var in kv.items():
            if isinstance(var, tk.BooleanVar):
                _store[sec][k] = "true" if var.get() else "false"
            else:
                _store[sec][k] = str(var.get())
    save_settings()
    try:
        win.destroy()
    except Exception:
        pass
