import argparse
import os
import re
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

# ===== Утилиты =====

def only_digits(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())

def has_letters(s: str) -> bool:
    return re.search(r"[A-Za-zА-Яа-яЁё]", str(s) or "") is not None

def clean_spaces(s: str) -> str:
    if s is None:
        return ""
    # NBSP/NNBSP/тонкие/скрытые пробелы и пр.
    s = str(s)
    for cp in (0x00A0, 0x202F, 0x2009, 0x200A, 0x200B, 0x2060, 0xFEFF, 0x200E, 0x200F):
        s = s.replace(chr(cp), " ")
    s = s.replace("\t", " ").replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\n")
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

def token_to_number(s: str) -> Optional[float]:
    t = clean_spaces(s)
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
    t = (t
         .replace("\uFF0C", ",")  # fullwidth comma
         .replace("\uFF0E", ".")  # fullwidth dot
         .replace("\u201A", ",")  # low-9 comma
         .replace(" ", ""))

    # Срежем хвостовые , .
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
        # Фолбек
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
    """Преобразует ячейку в число часов/дней: число, время, текст, 'a/b' — сумма частей."""
    if cell is None:
        return None
    v = cell.value
    if v is None or v == "":
        return None

    # Уже число?
    if isinstance(v, (int, float)):
        # Если ячейка с форматом времени -> часы
        nf = (cell.number_format or "").lower()
        if ("h" in nf or "ч" in nf or "[h]" in nf) and float(v) < 2:  # защита от дат
            return float(v) * 24.0
        return float(v)

    # Время как Python time/datetime
    if isinstance(v, time):
        return v.hour + v.minute / 60.0 + v.second / 3600.0
    if isinstance(v, datetime):
        return v.hour + v.minute / 60.0 + v.second / 3600.0

    s = str(v)
    # "a/b[/c]" → сумма
    sumv = sum_slash_parts(s)
    if sumv is not None:
        return float(sumv)
    # Обычный токен
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
    # 1) лист с "табел" в названии
    for ws in wb.worksheets:
        if "табел" in ws.title.lower():
            return ws
    # 2) первый видимый лист
    for ws in wb.worksheets:
        if ws.sheet_state == "visible":
            return ws
    return wb.worksheets[0] if wb.worksheets else None

def latest_file_in_folder(folder: str) -> Optional[str]:
    folder = Path(folder)
    if not folder.exists():
        return None
    cand = []
    # openpyxl: поддержка .xlsx/.xlsm (без .xlsb/.xls)
    for ext in ("*.xlsx", "*.xlsm"):
        cand.extend(folder.glob(ext))
    if not cand:
        return None
    cand.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return str(cand[0])

# ===== Трансформация =====

def transform_sheet(ws) -> Tuple[List[str], List[List[Any]]]:
    day_cols_h1 = [column_index_from_string(x) for x in DAY_COLS_HALF1_LETTERS]  # 1..15
    day_cols_h2 = [column_index_from_string(x) for x in DAY_COLS_HALF2_LETTERS]  # 16..31
    ao_col = column_index_from_string(AO_COL_LETTER)

    # Заголовки
    header = ["№", "ФИО", "Должность", "Табельный №", "ID объекта"] + [str(i) for i in range(1, 32)] + ["Отработано дней", "Отработано часов"]

    # Последняя строка по столбцу B
    last_row = START_ROW
    for r in range(ws.max_row, START_ROW - 1, -1):
        b = ws.cell(r, 2).value
        if b is not None and str(b).strip() != "":
            last_row = r
            break

    rows: List[List[Any]] = []
    for r in range(START_ROW, last_row + 1):
        raw_num = only_digits(ws.cell(r, 2).value or "")
        if not raw_num:
            continue

        fio_raw = ws.cell(r, 3).value
        fio, title = split_fio_and_title(fio_raw)
        tbn = clean_spaces(ws.cell(r, 5).value or "")

        # ИТОГО из AO
        days_num = to_number_cell(ws.cell(r, ao_col))
        hrs_num = None
        if r + HOURS_OFFSET <= ws.max_row:
            hrs_num = to_number_cell(ws.cell(r + HOURS_OFFSET, ao_col))
        if hrs_num is None and r + 1 <= ws.max_row:
            hrs_num = to_number_cell(ws.cell(r + 1, ao_col))

        # Фильтр: нужны буквы в ФИО и (таб.№ есть или дни число)
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
        for i2, col in enumerate(day_cols_h2):
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

    # Заголовок
    ws_out.append(header)
    for cell in ws_out[1]:
        cell.font = Font(bold=True)

    # Данные
    for row in rows:
        ws_out.append(row)

    # Оформление
    day_start_col = 6  # после: №, ФИО, Должность, Табельный №, ID объекта
    total_days_col = day_start_col + 31
    total_hours_col = total_days_col + 1

    # Ширины
    for col_idx in range(1, 6):
        ws_out.column_dimensions[get_column_letter(col_idx)].width = 16 if col_idx in (2, 3) else 12
    for col_idx in range(day_start_col, day_start_col + 31):
        ws_out.column_dimensions[get_column_letter(col_idx)].width = 4.25
    ws_out.column_dimensions[get_column_letter(total_days_col)].width = 12
    ws_out.column_dimensions[get_column_letter(total_hours_col)].width = 14

    # Перенос строк в ФИО/Должность
    wrap = Alignment(wrap_text=True)
    for row_idx in range(2, ws_out.max_row + 1):
        ws_out.cell(row_idx, 2).alignment = wrap
        ws_out.cell(row_idx, 3).alignment = wrap

    # Центрирование: дни + итоги (заголовки и данные)
    center = Alignment(horizontal="center", vertical="center")
    for col_idx in range(day_start_col, day_start_col + 31):
        ws_out.cell(1, col_idx).alignment = center
        for row_idx in range(2, ws_out.max_row + 1):
            ws_out.cell(row_idx, col_idx).alignment = center

    for col_idx in (total_days_col, total_hours_col):
        ws_out.cell(1, col_idx).alignment = center
        for row_idx in range(2, ws_out.max_row + 1):
            ws_out.cell(row_idx, col_idx).alignment = center

    # Форматы
    for col_idx in range(day_start_col, day_start_col + 31):
        for row_idx in range(2, ws_out.max_row + 1):
            ws_out.cell(row_idx, col_idx).number_format = "General"
    for row_idx in range(2, ws_out.max_row + 1):
        ws_out.cell(row_idx, total_days_col).number_format = "0"         # дни целые
        ws_out.cell(row_idx, total_hours_col).number_format = "General"  # часы без хвостов

    # Заморозка шапки
    ws_out.freeze_panes = "A2"

    # Сохранение
    out_path = str(out_path)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    wb_out.save(out_path)

# ===== CLI и запуск =====

def transform_file(file_path: str, out_path: Optional[str] = None):
    print(f"Открываю файл: {file_path}")
    wb = load_workbook(file_path, data_only=True, read_only=False)
    ws = pick_candidate_sheet(wb)
    if ws is None:
        raise RuntimeError("Не найден лист для обработки")
    print(f"Обрабатываю лист: {ws.title}")

    header, rows = transform_sheet(ws)
    if not out_path:
        p = Path(file_path)
        out_path = str(p.with_name(p.stem + "_result.xlsx"))
    print(f"Сохраняю результат: {out_path}")
    save_result(header, rows, out_path)
    print("Готово.")

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
    except Exception:
        return None

def main():
    parser = argparse.ArgumentParser(description="Преобразование табеля (1С ЗУП) в читаемую таблицу")
    g = parser.add_mutually_exclusive_group(required=False)
    g.add_argument("--file", help="Путь к файлу табеля (xlsx/xlsm)")
    g.add_argument("--pick", action="store_true", help="Выбрать файл через диалог")
    g.add_argument("--latest", help="Взять самый свежий файл из указанной папки")
    parser.add_argument("--out", help="Путь для сохранения результата (xlsx)")
    args = parser.parse_args()

    # Если ключи не переданы (двойной клик по exe) — открываем диалог
    if not any([args.file, args.pick, args.latest]):
        args.pick = True

    if args.file:
        transform_file(args.file, args.out)
    elif args.pick:
        fp = pick_file_dialog()
        if not fp:
            print("Файл не выбран.")
            return
        transform_file(fp, args.out)
    elif args.latest:
        fp = latest_file_in_folder(args.latest)
        if not fp:
            print("В папке не найден подходящий файл (*.xlsx, *.xlsm)")
            return
        transform_file(fp, args.out)

if __name__ == "__main__":
    main()