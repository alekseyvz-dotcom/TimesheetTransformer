import argparse
import ctypes
import os
import re
import sys
from datetime import time, datetime
from pathlib import Path
from typing import List, Optional, Tuple, Any

from openpyxl import load_workbook, Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.utils import column_index_from_string, get_column_letter

# ===== Настройки =====
START_ROW = 21
HOURS_OFFSET = 2  # на сколько строк ниже в AO лежат ИТОГО часов относительно дней
RESULT_SHEET_NAME = "Результат"

# Полу-«ломаные» колонки дней
DAY_COLS_HALF1_LETTERS = ["I", "K", "M", "N", "P", "R", "T", "V", "X", "Z", "AB", "AD", "AF", "AH", "AK"]          # 1..15
DAY_COLS_HALF2_LETTERS = ["I", "K", "M", "N", "P", "R", "T", "V", "X", "Z", "AB", "AD", "AF", "AH", "AK", "AL"]     # 16..31
AO_COL_LETTER = "AO"

NON_WORKING_CODES = {
    "В", "НН", "ОТ", "ОД", "У", "УД", "Б", "ДО", "К", "ПР", "ОЖ", "ОЗ", "НС", "Н", "НВ"
}

# ===== MessageBox и лог =====

def exe_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path.cwd()

LOG_PATH = exe_dir() / "TimesheetTransformer.log"

def log(msg: str):
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(msg.rstrip() + "\n")
    except Exception:
        pass

def msg_info(title: str, text: str):
    ctypes.windll.user32.MessageBoxW(0, text, title, 0x40)  # MB_ICONINFORMATION

def msg_error(title: str, text: str):
    ctypes.windll.user32.MessageBoxW(0, text, title, 0x10)  # MB_ICONERROR

# ===== Утилиты =====

def only_digits(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())

def has_letters(s: str) -> bool:
    return re.search(r"[A-Za-zА-Яа-яЁё]", str(s) or "") is not None

def clean_spaces(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    for cp in (0x00A0, 0x202F, 0x2009, 0x200A, 0x200B, 0x2060, 0xFEFF, 0x200E, 0x200F):
        s = s.replace(chr(cp), " ")
    s = s.replace("\t", " ").replace("\r\n", "\n").replace("\r", "\n")
    return s.strip()

def get_first_paren_content(s: str) -> str:
    m = re.search(r"\((.*?)\)", s)
    return m.group(1) if m else ""

def split_fio_and_title(raw: Any) -> Tuple[str, str]:
    s = clean_spaces(raw).replace("\r\n", "\n").replace("\r", "\n")
    parts = [p.strip() for p in s.split("\n") if p.strip()]
    fio = parts[0] if parts else ""
    title = ""
    if len(parts) > 1:
        inside = get_first_paren_content(parts[1])
        title = inside.strip() if inside else parts[1].strip()
    return fio, title

def extract_code_token(s: Any) -> str:
    txt = str(s) if s is not None else ""
    m = re.search(r"([A-Za-zА-Яа-яЁё]+)", txt)
    return m.group(1).upper() if m else ""

def is_non_working_code(code: str) -> bool:
    return code.upper().strip() in NON_WORKING_CODES

def token_to_number(t: str) -> Optional[float]:
    t = clean_spaces(t)
    if not t:
        return None
    # Время "h:mm(:ss)"
    if ":" in t:
        parts = t.split(":")
        if len(parts) >= 2:
            try:
                hh = float(re.sub(r"[^\d.+-]", "", parts[0]) or 0)
                mm = float(re.sub(r"[^\d.+-]", "", parts[1]) or 0)
                ss = float(re.sub(r"[^\d.+-]", "", parts[2])) if len(parts) >= 3 else 0.0
                return hh + mm / 60.0 + ss / 3600.0
            except Exception:
                pass
    # Нормализуем разделители
    t = (t.replace("\uFF0C", ",").replace("\uFF0E", ".").replace("\u201A", ",").replace(" ", ""))
    while t and t[-1] in ",.":
        t = t[:-1]
    t = t.replace(",", ".")
    m = re.search(r"[-+]?\d+(?:\.\d+)?", t)
    if m:
        try:
            return float(m.group(0))
        except Exception:
            return None
    try:
        return float(t)
    except Exception:
        return None

def sum_slash_parts(s: str) -> Optional[float]:
    if "/" not in (s or ""):
        return None
    total = 0.0
    cnt = 0
    for part in s.split("/"):
        n = token_to_number(part)
        if n is not None:
            total += n
            cnt += 1
    if cnt >= 1:
        return total
    return None

def to_number_cell(cell) -> Optional[float]:
    """Число часов/дней: число, время, текст, 'a/b' — сумма частей."""
    if cell is None:
        return None
    v = cell.value
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        # Если это excel-время (доля суток < 1) — преобразуем в часы
        try:
            if float(v) < 1.0:
                return float(v) * 24.0
        except Exception:
            pass
        return float(v)
    if isinstance(v, time):
        return v.hour + v.minute / 60.0 + v.second / 3600.0
    if isinstance(v, datetime):
        return v.hour + v.minute / 60.0 + v.second / 3600.0
    s = str(v)
    sumv = sum_slash_parts(s)
    if sumv is not None:
        return float(sumv)
    n = token_to_number(s)
    return float(n) if n is not None else None

def day_hours_from_cells(code_cell, hours_cell) -> Optional[float]:
    n = to_number_cell(hours_cell)
    if n is not None:
        return n
    code = extract_code_token(code_cell.value if code_cell else "")
    if not code:
        return None
    if is_non_working_code(code):
        return 0.0
    return None

def pick_candidate_sheet(wb) -> Optional[Any]:
    for ws in wb.worksheets:
        if "табел" in ws.title.lower():
            return ws
    for ws in wb.worksheets:
        if ws.sheet_state == "visible":
            return ws
    return wb.worksheets[0] if wb.worksheets else None

def latest_file_in_folder(folder: str) -> Optional[str]:
    folder = Path(folder)
    if not folder.exists():
        return None
    cand = []
    for ext in ("*.xlsx", "*.xlsm"):
        cand.extend(folder.glob(ext))
    if not cand:
        return None
    cand.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return str(cand[0])

def guess_last_row(ws, start_row: int = START_ROW, bcol: int = 2,
                   max_scan_rows: int = 20000, empty_break: int = 20) -> int:
    """
    Быстрое определение конца данных по столбцу B:
    - сканируем сверху вниз максимум max_scan_rows строк,
    - после появления данных завершаем при empty_break подряд пустых строк.
    """
    r_max = min(ws.max_row or (start_row + max_scan_rows), start_row + max_scan_rows)
    last = start_row - 1
    seen = False
    empty_run = 0
    for r in range(start_row, r_max + 1):
        v = ws.cell(r, bcol).value
        if v is None or str(v).strip() == "":
            if seen:
                empty_run += 1
                if empty_run >= empty_break:
                    break
        else:
            seen = True
            empty_run = 0
            last = r
    return max(last, start_row)

# ===== Трансформация =====

def transform_sheet(ws) -> Tuple[List[str], List[List[Any]]]:
    day_cols_h1 = [column_index_from_string(x) for x in DAY_COLS_HALF1_LETTERS]  # 1..15
    day_cols_h2 = [column_index_from_string(x) for x in DAY_COLS_HALF2_LETTERS]  # 16..31
    ao_col = column_index_from_string(AO_COL_LETTER)

    header = ["№", "ФИО", "Должность", "Табельный №", "ID объекта"] + [str(i) for i in range(1, 32)] + ["Отработано дней", "Отработано часов"]

    last_row = guess_last_row(ws, START_ROW, 2)
    log(f"guess_last_row -> {last_row}")

    rows: 
