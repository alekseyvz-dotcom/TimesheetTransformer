import argparse
import ctypes
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
HOURS_OFFSET = 2
RESULT_SHEET_NAME = "Результат"

# Ограничители сканирования и логирования прогресса
MAX_SCAN_ROWS = 4000       # максимум строк, которые смотрим от START_ROW при поиске конца
NO_GOOD_BREAK = 150        # если столько подряд "неосмысленных" строк — считаем, что данные закончились
PROGRESS_EVERY = 200       # писать прогресс в лог каждые N строк

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

# ——— Парсинг чисел/времени и «a/b» ———

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
        try:
            # Excel-время как доля суток (<1) — воспринимаем как часы
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

# ===== Поиск конца данных (ускорённый и «осмысленный») =====

def is_good_row(ws, r: int, ao_col: int) -> bool:
    """
    Быстрая проверка «строка сотрудника»: в C есть буквы (ФИО) и либо:
    - в E (таб.№) что-то есть, либо
    - в AO есть цифры/число.
    Без тяжёлых вычислений.
    """
    c_val = ws.cell(r, 3).value
    if not has_letters(c_val):
        return False
    e_val = ws.cell(r, 5).value
    if str(e_val or "").strip():
        return True
    ao = ws.cell(r, ao_col).value
    if ao is None:
        return False
    if isinstance(ao, (int, float)):
        return True
    return bool(re.search(r"\d", str(ao)))

def find_last_data_row(ws, start_row: int = START_ROW) -> int:
    ao_col = column_index_from_string(AO_COL_LETTER)
    limit = min(ws.max_row or (start_row + MAX_SCAN_ROWS), start_row + MAX_SCAN_ROWS)
    last_good = start_row - 1
    no_good = 0
    for r in range(start_row, limit + 1):
        if is_good_row(ws, r, ao_col):
            last_good = r
            no_good = 0
        else:
            no_good += 1
        if r % PROGRESS_EVERY == 0:
            log(f"scan r={r}, last_good={last_good}, no_good={no_good}")
        if last_good >= start_row and no_good >= NO_GOOD_BREAK:
            break
    if last_good < start_row:
        last_good = start_row
    log(f"find_last_data_row -> {last_good}")
    return last_good

# ===== Трансформация =====

def extract_code_token(s: Any) -> str:
    txt = str(s) if s is not None else ""
    m = re.search(r"([A-Za-zА-Яа-яЁё]+)", txt)
    return m.group(1).upper() if m else ""

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

def transform_sheet(ws) -> Tuple[List[str], List[List[Any]]]:
    day_cols_h1 = [column_index_from_string(x) for x in DAY_COLS_HALF1_LETTERS]  # 1..15
    day_cols_h2 = [column_index_from_string(x) for x in DAY_COLS_HALF2_LETTERS]  # 16..31
    ao_col = column_index_from_string(AO_COL_LETTER)

    header = ["№", "ФИО", "Должность", "Табельный №", "ID объекта"] + [str(i) for i in range(1, 32)] + ["Отработано дней", "Отработано часов"]

    last_row = find_last_data_row(ws, START_ROW)
    rows: List[List[Any]] = []

    for r in range(START_ROW, last_row + 1):
        if (r - START_ROW) % PROGRESS_EVERY == 0:
            log(f"proc r={r}/{last_row}")

        raw_num = only_digits(ws.cell(r, 2).value or "")
        if not raw_num:
            continue

        fio_raw = ws.cell(r, 3).value
        fio, title = split_fio_and_title(fio_raw)
        tbn = clean_spaces(ws.cell(r, 5).value or "")

        days_num = to_number_cell(ws.cell(r, ao_col))
        hrs_num = None
        if r + HOURS_OFFSET <= ws.max_row:
            hrs_num = to_number_cell(ws.cell(r + HOURS_OFFSET, ao_col))
        if hrs_num is None and r + 1 <= ws.max_row:
            hrs_num = to_number_cell(ws.cell(r + 1, ao_col))

        if not (has_letters(fio) and (len(tbn) > 0 or isinstance(days_num, (int, float)))):
            continue

        out = [int(raw_num), fio, title, tbn, ""]  # ID объекта пусто

        # Дни 1..15: коды r, часы r+1
        for col in day_cols_h1:
            code_cell = ws.cell(r, col)
            hours_cell = ws.cell(r + 1, col) if r + 1 <= ws.max_row else None
            daily = day_hours_from_cells(code_cell, hours_cell)
            out.append(daily if daily is not None else "")

        # Дни 16..31: коды r+2, часы r+3
        for col in day_cols_h2:
            code_cell = ws.cell(r + 2, col) if r + 2 <= ws.max_row else None
            hours_cell = ws.cell(r + 3, col) if r + 3 <= ws.max_row else None
            daily = day_hours_from_cells(code_cell, hours_cell)
            out.append(daily if daily is not None else "")

        out.append(days_num if days_num is not None else "")
        out.append(hrs_num if hrs_num is not None else "")

        rows.append(out)

    return header, rows

def save_result(header: List[str], rows: List[List[Any]], out_path: str):
    wb_out = Workbook()
    ws_out = wb_out.active
    ws_out.title = RESULT_SHEET_NAME

    ws_out.append(header)
    for cell in ws_out[1]:
        cell.font = Font(bold=True)

    for row in rows:
        ws_out.append(row)

    day_start_col = 6  # после: №, ФИО, Должность, Табельный №, ID объекта
    total_days_col = day_start_col + 31
    total_hours_col = total_days_col + 1

    for col_idx in range(1, 6):
        ws_out.column_dimensions[get_column_letter(col_idx)].width = 16 if col_idx in (2, 3) else 12
    for col_idx in range(day_start_col, day_start_col + 31):
        ws_out.column_dimensions[get_column_letter(col_idx)].width = 4.25
    ws_out.column_dimensions[get_column_letter(total_days_col)].width = 12
    ws_out.column_dimensions[get_column_letter(total_hours_col)].width = 14

    wrap = Alignment(wrap_text=True)
    for row_idx in range(2, ws_out.max_row + 1):
        ws_out.cell(row_idx, 2).alignment = wrap
        ws_out.cell(row_idx, 3).alignment = wrap

    center = Alignment(horizontal="center", vertical="center")
    for col_idx in range(day_start_col, day_start_col + 31):
        ws_out.cell(1, col_idx).alignment = center
        for row_idx in range(2, ws_out.max_row + 1):
            ws_out.cell(row_idx, col_idx).alignment = center

    for col_idx in (total_days_col, total_hours_col):
        ws_out.cell(1, col_idx).alignment = center
        for row_idx in range(2, ws_out.max_row + 1):
            ws_out.cell(row_idx, col_idx).alignment = center

    for col_idx in range(day_start_col, day_start_col + 31):
        for row_idx in range(2, ws_out.max_row + 1):
            ws_out.cell(row_idx, col_idx).number_format = "General"
    for row_idx in range(2, ws_out.max_row + 1):
        ws_out.cell(row_idx, total_days_col).number_format = "0"
        ws_out.cell(row_idx, total_hours_col).number_format = "General"

    ws_out.freeze_panes = "A2"

    out_path = str(out_path)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    wb_out.save(out_path)

def safe_save_result(header, rows, primary_out: Path) -> Path:
    try:
        save_result(header, rows, str(primary_out))
        return primary_out
    except Exception as e:
        log(f"Primary save failed: {e}")
        alt_dir = Path.home() / "Desktop" / "TimesheetTransformer_Results"
        alt_dir.mkdir(parents=True, exist_ok=True)
        alt_path = alt_dir / primary_out.name
        save_result(header, rows, str(alt_path))
        return alt_path

# ===== CLI =====

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

def pick_candidate_sheet(wb) -> Optional[Any]:
    for ws in wb.worksheets:
        if "табел" in ws.title.lower():
            return ws
    for ws in wb.worksheets:
        if ws.sheet_state == "visible":
            return ws
    return wb.worksheets[0] if wb.worksheets else None

def transform_file(file_path: str, out_path: Optional[str] = None):
    try:
        p = Path(file_path)
        ext = p.suffix.lower()
        if ext not in (".xlsx", ".xlsm"):
            msg_error("Неподдерживаемый формат",
                      f"Выбран файл: {p.name}\nПоддерживаются только .xlsx и .xlsm.\nСохраните исходник как .xlsx.")
            return

        log(f"Open workbook: {file_path}")
        wb = load_workbook(file_path, data_only=True, read_only=True)
        ws = pick_candidate_sheet(wb)
        if ws is None:
            msg_error("Ошибка", "Не найден лист для обработки.")
            return
        log(f"Sheet: {ws.title}")

        header, rows = transform_sheet(ws)
        out_path = out_path or str(p.with_name(p.stem + "_result.xlsx"))

        saved_to = safe_save_result(header, rows, Path(out_path))
        msg_info("Готово", f"Результат сохранён:\n{saved_to}\n\nЛог: {LOG_PATH}")
        log("Done.")

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        log(tb)
        msg_error("Критическая ошибка", f"{e}\n\nПодробности в логе:\n{LOG_PATH}")

def pick_file_dialog() -> Optional[str]:
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        fp = filedialog.askopenfilename(
            title="Выберите файл табеля",
            filetypes=[("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")]
        )
        root.destroy()
        return fp or None
    except Exception as e:
        log(f"File dialog failed: {e}")
        return None

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Преобразование табеля (1С ЗУП) в читаемую таблицу")
    g = parser.add_mutually_exclusive_group(required=False)
    g.add_argument("--file", help="Путь к файлу табеля (xlsx/xlsm)")
    g.add_argument("--pick", action="store_true", help="Выбрать файл через диалог")
    g.add_argument("--latest", help="Взять самый свежий файл из указанной папки")
    parser.add_argument("--out", help="Путь для сохранения результата (xlsx)")
    args = parser.parse_args()

    if not any([args.file, args.pick, args.latest]):
        args.pick = True

    if args.file:
        transform_file(args.file, args.out)
    elif args.pick:
        fp = pick_file_dialog()
        if not fp:
            msg_info("Отмена", "Файл не выбран.")
            return
        transform_file(fp, args.out)
    elif args.latest:
        fp = latest_file_in_folder(args.latest)
        if not fp:
            msg_error("Не найден файл", "В папке не найден подходящий файл (*.xlsx, *.xlsm).")
            return
        transform_file(fp, args.out)

if __name__ == "__main__":
    main()
