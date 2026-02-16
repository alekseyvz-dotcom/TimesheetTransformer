"""
payroll_module.py — Модуль «Затраты (ФОТ)»
Загрузка Excel-файла с начислениями ЗП, распределение по объектам
пропорционально часам из объектного табеля, аналитика.
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from decimal import Decimal, ROUND_HALF_UP
import logging
import re
import os

import pandas as pd
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# ============================================================
#  DB pool — устанавливается из main_app при старте
# ============================================================

db_connection_pool: Optional[pool.SimpleConnectionPool] = None


def set_db_pool(db_pool: pool.SimpleConnectionPool):
    global db_connection_pool
    db_connection_pool = db_pool
    logging.info("Payroll Module: DB pool set.")


# ============================================================
#  EXCEL PARSER
# ============================================================

class PayrollExcelParser:
    """
    Парсер Excel с начислениями ЗП.
    Структура (из реального файла):
      - Строки 1-6: шапка (организация, период и т.д.)
      - Строка 7 (idx 6): заголовки колонок
      - Строка 8 (idx 7): подзаголовки (Дней/Часов)
      - С строки 9 (idx 8): данные
      - Колонка A (0): Табельный номер
      - Колонка C (2): ФИО
      - Колонка E (4): Подразделение
      - Колонка G (6): Должность
      - Колонка K (10): Отработано дней
      - Колонка L (11): Отработано часов
      - Последняя колонка с «Всего» в заголовке: Всего начислено
    """

    MONTH_MAP = {
        'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4,
        'май': 5, 'июнь': 6, 'июль': 7, 'август': 8,
        'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12,
    }

    @staticmethod
    def parse(file_path: str) -> Dict[str, Any]:
        import openpyxl
        wb = openpyxl.load_workbook(file_path, data_only=True)
        ws = wb.active

        all_rows = []
        for row in ws.iter_rows(values_only=True):
            all_rows.append(list(row))

        if len(all_rows) < 9:
            wb.close()
            raise ValueError("Файл слишком короткий — ожидается минимум 9 строк.")

        # --- Период и организация ---
        organization = ""
        period_label = ""
        year = None
        month = None

        for idx in range(min(7, len(all_rows))):
            for cell_val in all_rows[idx]:
                if cell_val and isinstance(cell_val, str):
                    cell_lower = cell_val.strip().lower()
                    for m_name, m_num in PayrollExcelParser.MONTH_MAP.items():
                        if m_name in cell_lower:
                            month = m_num
                            year_match = re.search(r'(\d{4})', cell_val)
                            if year_match:
                                year = int(year_match.group(1))
                            period_label = cell_val.strip()
                            break
                    if ('организация' in cell_lower or
                            'ано' in cell_lower or
                            'ооо' in cell_lower):
                        if not organization:
                            organization = cell_val.strip()

        now = datetime.now()
        year = year or now.year
        month = month or now.month
        if not period_label:
            period_label = f"{month:02d}.{year}"

        # --- Определяем колонку «Всего начислено» ---
        header_row_idx = 6
        headers = all_rows[header_row_idx] if len(all_rows) > header_row_idx else []
        total_col_idx = None

        for ci, hv in enumerate(headers):
            if hv and isinstance(hv, str) and 'всего' in hv.lower():
                total_col_idx = ci  # берём последнее «Всего»

        if total_col_idx is None:
            # fallback — последняя колонка
            for ci in range(len(headers) - 1, -1, -1):
                if headers[ci] is not None:
                    total_col_idx = ci
                    break
            if total_col_idx is None:
                total_col_idx = len(headers) - 1

        # --- Основные индексы колонок ---
        COL_TBN = 0
        COL_FIO = 2
        COL_DEPT = 4
        COL_POS = 6
        COL_DAYS = 10
        COL_HOURS = 11

        # --- Парсим данные ---
        def safe_float(v):
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return float(v)
            try:
                return float(str(v).replace(',', '.').replace(' ', '').strip())
            except (ValueError, TypeError):
                return None

        def safe_int(v):
            f = safe_float(v)
            return int(f) if f is not None else None

        def cell(row_data, idx):
            return row_data[idx] if idx < len(row_data) else None

        parsed_rows = []
        data_start_idx = 8

        for ri in range(data_start_idx, len(all_rows)):
            rd = all_rows[ri]
            if not rd or len(rd) < 3:
                continue

            tbn_raw = cell(rd, COL_TBN)
            fio_raw = cell(rd, COL_FIO)

            tbn_str = str(tbn_raw).strip() if tbn_raw else ""
            fio_str = str(fio_raw).strip() if fio_raw else ""

            # Пропускаем пустые и итоговые строки
            skip_words = ('итого', 'всего', 'none', '')
            if tbn_str.lower() in skip_words and fio_str.lower() in skip_words:
                continue
            if not tbn_str and not fio_str:
                continue
            # Если тбн — «Итого» — пропускаем
            if tbn_str.lower() in ('итого', 'всего', 'итого:', 'всего:'):
                continue

            total_accrued = safe_float(cell(rd, total_col_idx))

            parsed_rows.append({
                "tbn": tbn_str,
                "fio": fio_str,
                "department_raw": str(cell(rd, COL_DEPT) or "").strip(),
                "position_raw": str(cell(rd, COL_POS) or "").strip(),
                "worked_days": safe_int(cell(rd, COL_DAYS)),
                "worked_hours": safe_float(cell(rd, COL_HOURS)),
                "total_accrued": total_accrued,
            })

        wb.close()

        return {
            "organization": organization,
            "period_label": period_label,
            "year": year,
            "month": month,
            "rows": parsed_rows,
        }


# ============================================================
#  DATA LAYER
# ============================================================

class PayrollDataManager:
    """Работа с БД: сохранение загрузки, распределение, выборки."""

    @staticmethod
    def _get_conn():
        if not db_connection_pool:
            raise ConnectionError("Пул соединений не инициализирован.")
        return db_connection_pool.getconn()

    @staticmethod
    def _put_conn(conn):
        if conn and db_connection_pool:
            db_connection_pool.putconn(conn)

    @staticmethod
    def _query(sql: str, params: tuple = None) -> List[Dict]:
        conn = PayrollDataManager._get_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]
        finally:
            PayrollDataManager._put_conn(conn)

    # ---- Загрузки ----

    @staticmethod
    def get_uploads() -> List[Dict]:
        return PayrollDataManager._query("""
            SELECT pu.id, pu.organization, pu.period_label,
                   pu.year, pu.month, pu.file_name,
                   pu.uploaded_at, pu.note,
                   au.full_name AS uploaded_by_name,
                   (SELECT COUNT(*) FROM payroll_rows pr WHERE pr.upload_id = pu.id) AS row_count,
                   (SELECT COALESCE(SUM(pr.total_accrued),0) FROM payroll_rows pr WHERE pr.upload_id = pu.id) AS total_sum,
                   (SELECT COUNT(*) FROM payroll_distribution pd
                    JOIN payroll_rows pr2 ON pd.payroll_row_id = pr2.id
                    WHERE pr2.upload_id = pu.id) AS dist_count
            FROM payroll_uploads pu
            LEFT JOIN app_users au ON pu.uploaded_by = au.id
            ORDER BY pu.year DESC, pu.month DESC, pu.uploaded_at DESC
        """)

    @staticmethod
    def save_upload(parsed: Dict, file_name: str, user_id: int) -> int:
        """Сохраняет загрузку + строки, возвращает upload_id."""
        conn = PayrollDataManager._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO payroll_uploads
                        (organization, period_label, year, month, file_name, uploaded_by)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    parsed["organization"],
                    parsed["period_label"],
                    parsed["year"],
                    parsed["month"],
                    file_name,
                    user_id,
                ))
                upload_id = cur.fetchone()[0]

                for r in parsed["rows"]:
                    # Пытаемся найти employee_id по tbn
                    employee_id = None
                    if r["tbn"]:
                        cur.execute(
                            "SELECT id FROM employees WHERE tbn = %s LIMIT 1",
                            (r["tbn"],))
                        emp = cur.fetchone()
                        if emp:
                            employee_id = emp[0]

                    cur.execute("""
                        INSERT INTO payroll_rows
                            (upload_id, tbn, fio, department_raw, position_raw,
                             worked_days, worked_hours, total_accrued, employee_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        upload_id,
                        r["tbn"] or None,
                        r["fio"] or None,
                        r["department_raw"] or None,
                        r["position_raw"] or None,
                        r["worked_days"],
                        r["worked_hours"],
                        r["total_accrued"],
                        employee_id,
                    ))
            conn.commit()
            return upload_id
        except Exception:
            conn.rollback()
            raise
        finally:
            PayrollDataManager._put_conn(conn)

    @staticmethod
    def delete_upload(upload_id: int):
        """Удаляет загрузку каскадно (rows + distribution)."""
        conn = PayrollDataManager._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM payroll_uploads WHERE id = %s", (upload_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            PayrollDataManager._put_conn(conn)

    # ---- Распределение ----

    @staticmethod
    def distribute(upload_id: int) -> Dict[str, int]:
        """
        Распределяет ФОТ по объектам для загрузки upload_id.
        Алгоритм:
          1. Для каждой payroll_row находим все timesheet_rows
             с тем же tbn за тот же year/month.
          2. Считаем total_hours на каждом объекте.
          3. fraction = hours_on_obj / sum(hours_all_objects).
          4. amount = total_accrued * fraction.
        Возвращает { "distributed": N, "not_found": M, "zero_hours": K }
        """
        conn = PayrollDataManager._get_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Получаем параметры загрузки
                cur.execute(
                    "SELECT year, month FROM payroll_uploads WHERE id = %s",
                    (upload_id,))
                upl = cur.fetchone()
                if not upl:
                    raise ValueError(f"Загрузка {upload_id} не найдена")
                y, m = upl["year"], upl["month"]

                # Удаляем старое распределение
                cur.execute("""
                    DELETE FROM payroll_distribution
                    WHERE payroll_row_id IN (
                        SELECT id FROM payroll_rows WHERE upload_id = %s
                    )
                """, (upload_id,))

                # Получаем строки
                cur.execute("""
                    SELECT id, tbn, total_accrued
                    FROM payroll_rows
                    WHERE upload_id = %s AND tbn IS NOT NULL AND tbn <> ''
                """, (upload_id,))
                rows = cur.fetchall()

                stats = {"distributed": 0, "not_found": 0, "zero_hours": 0}

                for pr in rows:
                    pr_id = pr["id"]
                    tbn = pr["tbn"]
                    total_accrued = float(pr["total_accrued"] or 0)

                    if total_accrued == 0:
                        stats["zero_hours"] += 1
                        continue

                    # Находим часы по объектам из табеля
                    cur.execute("""
                        SELECT
                            th.object_db_id AS object_id,
                            th.id AS header_id,
                            COALESCE(tr.total_hours, 0) AS hours
                        FROM timesheet_rows tr
                        JOIN timesheet_headers th ON th.id = tr.header_id
                        WHERE tr.tbn = %s
                          AND th.year = %s
                          AND th.month = %s
                          AND COALESCE(tr.total_hours, 0) > 0
                    """, (tbn, y, m))
                    ts_rows = cur.fetchall()

                    if not ts_rows:
                        stats["not_found"] += 1
                        continue

                    # Суммарные часы по всем объектам
                    total_ts_hours = sum(float(r["hours"]) for r in ts_rows)
                    if total_ts_hours <= 0:
                        stats["not_found"] += 1
                        continue

                    # Группируем по объекту (сотрудник может быть несколько
                    # раз на одном объекте в разных строках)
                    obj_hours: Dict[int, Tuple[float, int]] = {}
                    for tsr in ts_rows:
                        oid = tsr["object_id"]
                        h = float(tsr["hours"])
                        hid = tsr["header_id"]
                        if oid in obj_hours:
                            old_h, old_hid = obj_hours[oid]
                            obj_hours[oid] = (old_h + h, old_hid)
                        else:
                            obj_hours[oid] = (h, hid)

                    # Распределяем
                    distributed_sum = Decimal("0")
                    items = list(obj_hours.items())

                    for i, (oid, (h_on_obj, hdr_id)) in enumerate(items):
                        fraction = Decimal(str(h_on_obj)) / Decimal(str(total_ts_hours))

                        if i == len(items) - 1:
                            # Последний объект — остаток (чтобы сумма = total_accrued)
                            amount = Decimal(str(total_accrued)) - distributed_sum
                        else:
                            amount = (Decimal(str(total_accrued)) * fraction).quantize(
                                Decimal("0.01"), rounding=ROUND_HALF_UP)
                            distributed_sum += amount

                        cur.execute("""
                            INSERT INTO payroll_distribution
                                (payroll_row_id, object_id, timesheet_header_id,
                                 hours_on_object, total_hours_all_objects,
                                 fraction, amount)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            pr_id, oid, hdr_id,
                            round(h_on_obj, 2),
                            round(total_ts_hours, 2),
                            round(float(fraction), 6),
                            float(amount),
                        ))

                    stats["distributed"] += 1

            conn.commit()
            return stats
        except Exception:
            conn.rollback()
            raise
        finally:
            PayrollDataManager._put_conn(conn)

    # ---- Аналитические выборки ----

    @staticmethod
    def get_distribution_by_object(upload_id: int) -> pd.DataFrame:
        data = PayrollDataManager._query("""
            SELECT
                o.address AS object_name,
                o.short_name AS object_type,
                COUNT(DISTINCT pr.tbn) AS people_cnt,
                SUM(pd.hours_on_object) AS total_hours,
                SUM(pd.amount) AS total_amount
            FROM payroll_distribution pd
            JOIN payroll_rows pr ON pr.id = pd.payroll_row_id
            JOIN objects o ON o.id = pd.object_id
            WHERE pr.upload_id = %s
            GROUP BY o.id, o.address, o.short_name
            ORDER BY total_amount DESC
        """, (upload_id,))
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_hours"] = df["total_hours"].astype(float)
            df["total_amount"] = df["total_amount"].astype(float)
            df["people_cnt"] = df["people_cnt"].astype(int)
        return df

    @staticmethod
    def get_distribution_by_department(upload_id: int) -> pd.DataFrame:
        data = PayrollDataManager._query("""
            SELECT
                COALESCE(pr.department_raw, '—') AS department_name,
                COUNT(DISTINCT pr.tbn) AS people_cnt,
                SUM(pr.total_accrued) AS total_accrued,
                SUM(pd_sum.distributed) AS total_distributed
            FROM payroll_rows pr
            LEFT JOIN (
                SELECT payroll_row_id, SUM(amount) AS distributed
                FROM payroll_distribution
                GROUP BY payroll_row_id
            ) pd_sum ON pd_sum.payroll_row_id = pr.id
            WHERE pr.upload_id = %s
            GROUP BY COALESCE(pr.department_raw, '—')
            ORDER BY total_accrued DESC
        """, (upload_id,))
        df = pd.DataFrame(data)
        if not df.empty:
            df["total_accrued"] = df["total_accrued"].fillna(0).astype(float)
            df["total_distributed"] = df["total_distributed"].fillna(0).astype(float)
            df["people_cnt"] = df["people_cnt"].astype(int)
        return df

    @staticmethod
    def get_undistributed_rows(upload_id: int) -> pd.DataFrame:
        """Сотрудники, которых не удалось распределить."""
        data = PayrollDataManager._query("""
            SELECT
                pr.tbn, pr.fio, pr.department_raw,
                pr.position_raw, pr.total_accrued
            FROM payroll_rows pr
            WHERE pr.upload_id = %s
              AND pr.id NOT IN (
                  SELECT DISTINCT payroll_row_id FROM payroll_distribution
              )
              AND COALESCE(pr.total_accrued, 0) > 0
            ORDER BY pr.total_accrued DESC
        """, (upload_id,))
        return pd.DataFrame(data)

    @staticmethod
    def get_upload_summary(upload_id: int) -> Dict[str, Any]:
        rows = PayrollDataManager._query("""
            SELECT
                (SELECT COUNT(*) FROM payroll_rows WHERE upload_id = %s) AS total_rows,
                (SELECT COALESCE(SUM(total_accrued), 0) FROM payroll_rows WHERE upload_id = %s) AS total_accrued,
                (SELECT COALESCE(SUM(pd.amount), 0)
                 FROM payroll_distribution pd
                 JOIN payroll_rows pr ON pd.payroll_row_id = pr.id
                 WHERE pr.upload_id = %s) AS total_distributed,
                (SELECT COUNT(DISTINCT pr2.id)
                 FROM payroll_rows pr2
                 WHERE pr2.upload_id = %s
                   AND pr2.id IN (SELECT DISTINCT payroll_row_id FROM payroll_distribution)
                ) AS rows_distributed,
                (SELECT COUNT(DISTINCT pr3.id)
                 FROM payroll_rows pr3
                 WHERE pr3.upload_id = %s
                   AND COALESCE(pr3.total_accrued, 0) > 0
                   AND pr3.id NOT IN (SELECT DISTINCT payroll_row_id FROM payroll_distribution)
                ) AS rows_not_distributed,
                (SELECT COUNT(DISTINCT pd2.object_id)
                 FROM payroll_distribution pd2
                 JOIN payroll_rows pr4 ON pd2.payroll_row_id = pr4.id
                 WHERE pr4.upload_id = %s) AS objects_count
        """, (upload_id, upload_id, upload_id, upload_id, upload_id, upload_id))
        r = rows[0] if rows else {}
        for k in ("total_accrued", "total_distributed"):
            r[k] = float(r.get(k, 0) or 0)
        for k in ("total_rows", "rows_distributed", "rows_not_distributed", "objects_count"):
            r[k] = int(r.get(k, 0) or 0)
        r["undistributed_amount"] = r["total_accrued"] - r["total_distributed"]
        return r


# ============================================================
#  UI: Главная страница модуля
# ============================================================

class PayrollPage(ttk.Frame):
    """Страница «Затраты (ФОТ)»."""

    def __init__(self, master, app_ref):
        super().__init__(master)
        self.app_ref = app_ref

        # ---- Верхняя панель кнопок ----
        toolbar = ttk.Frame(self, padding="8")
        toolbar.pack(fill="x", side="top")

        ttk.Button(toolbar, text="📂 Загрузить Excel",
                    command=self._on_upload).pack(side="left", padx=4)
        ttk.Button(toolbar, text="🔄 Распределить ФОТ",
                    command=self._on_distribute).pack(side="left", padx=4)
        ttk.Button(toolbar, text="🗑 Удалить загрузку",
                    command=self._on_delete).pack(side="left", padx=4)
        ttk.Button(toolbar, text="↻ Обновить",
                    command=self._refresh).pack(side="left", padx=4)

        # ---- Основная область: PanedWindow ----
        pw = ttk.PanedWindow(self, orient="horizontal")
        pw.pack(fill="both", expand=True, padx=5, pady=5)

        # Левая панель — список загрузок
        left = ttk.LabelFrame(pw, text="Загрузки")
        pw.add(left, weight=1)

        self.tree_uploads = ttk.Treeview(left, columns=(
            "id", "period", "file", "rows", "sum", "dist", "date"
        ), show="headings", height=12, selectmode="browse")

        cols_cfg = [
            ("id", "ID", 40),
            ("period", "Период", 110),
            ("file", "Файл", 180),
            ("rows", "Строк", 55),
            ("sum", "Сумма ФОТ", 110),
            ("dist", "Распр.", 55),
            ("date", "Загружено", 130),
        ]
        for cid, text, w in cols_cfg:
            self.tree_uploads.heading(cid, text=text)
            self.tree_uploads.column(cid, width=w, anchor="e" if cid in ("rows", "sum", "dist") else "w")

        vsb = ttk.Scrollbar(left, orient="vertical", command=self.tree_uploads.yview)
        self.tree_uploads.configure(yscrollcommand=vsb.set)
        self.tree_uploads.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree_uploads.bind("<<TreeviewSelect>>", self._on_upload_selected)

        # Правая панель — детали выбранной загрузки
        right = ttk.Frame(pw)
        pw.add(right, weight=3)

        self.detail_notebook = ttk.Notebook(right)
        self.detail_notebook.pack(fill="both", expand=True)

        self.tab_summary = ttk.Frame(self.detail_notebook)
        self.tab_by_object = ttk.Frame(self.detail_notebook)
        self.tab_by_dept = ttk.Frame(self.detail_notebook)
        self.tab_unmatched = ttk.Frame(self.detail_notebook)

        self.detail_notebook.add(self.tab_summary, text="  Сводка  ")
        self.detail_notebook.add(self.tab_by_object, text="  По объектам  ")
        self.detail_notebook.add(self.tab_by_dept, text="  По подразделениям  ")
        self.detail_notebook.add(self.tab_unmatched, text="  Не распределено  ")

        self._selected_upload_id: Optional[int] = None

        self._refresh()

    # ---- Actions ----

    def _on_upload(self):
        file_path = filedialog.askopenfilename(
            title="Выберите Excel-файл с начислениями",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if not file_path:
            return

        try:
            parsed = PayrollExcelParser.parse(file_path)
        except Exception as e:
            logging.exception("Ошибка парсинга Excel")
            messagebox.showerror("Ошибка парсинга",
                                 f"Не удалось прочитать файл:\n{e}")
            return

        row_count = len(parsed["rows"])
        total = sum(r["total_accrued"] or 0 for r in parsed["rows"])

        msg = (
            f"Файл: {os.path.basename(file_path)}\n"
            f"Период: {parsed['period_label']}\n"
            f"Организация: {parsed['organization']}\n"
            f"Строк данных: {row_count}\n"
            f"Сумма «Всего начислено»: {total:,.2f} ₽\n\n"
            f"Загрузить?"
        )
        if not messagebox.askyesno("Подтверждение загрузки", msg):
            return

        try:
            user_id = self.app_ref.current_user.get("id")
            upload_id = PayrollDataManager.save_upload(
                parsed, os.path.basename(file_path), user_id)
            messagebox.showinfo("Успех",
                                f"Загрузка #{upload_id} сохранена.\n"
                                f"{row_count} строк.\n\n"
                                f"Теперь нажмите «Распределить ФОТ».")
            self._refresh()
        except Exception as e:
            logging.exception("Ошибка сохранения загрузки")
            messagebox.showerror("Ошибка", f"Не удалось сохранить:\n{e}")

    def _on_distribute(self):
        if not self._selected_upload_id:
            messagebox.showwarning("Внимание", "Выберите загрузку в списке слева.")
            return
        uid = self._selected_upload_id

        if not messagebox.askyesno(
                "Распределение",
                f"Распределить ФОТ загрузки #{uid} по объектам?\n"
                f"(старое распределение будет пересчитано)"):
            return

        try:
            stats = PayrollDataManager.distribute(uid)
            messagebox.showinfo(
                "Результат распределения",
                f"Распределено сотрудников: {stats['distributed']}\n"
                f"Не найдено в табелях: {stats['not_found']}\n"
                f"Нулевое начисление: {stats['zero_hours']}")
            self._refresh()
            self._show_upload_details(uid)
        except Exception as e:
            logging.exception("Ошибка распределения")
            messagebox.showerror("Ошибка", f"Не удалось распределить:\n{e}")

    def _on_delete(self):
        if not self._selected_upload_id:
            messagebox.showwarning("Внимание", "Выберите загрузку в списке слева.")
            return
        uid = self._selected_upload_id
        if not messagebox.askyesno(
                "Удаление",
                f"Удалить загрузку #{uid} и все связанные данные?\n"
                f"Это действие нельзя отменить."):
            return
        try:
            PayrollDataManager.delete_upload(uid)
            self._selected_upload_id = None
            self._refresh()
            self._clear_details()
            messagebox.showinfo("Готово", f"Загрузка #{uid} удалена.")
        except Exception as e:
            logging.exception("Ошибка удаления загрузки")
            messagebox.showerror("Ошибка", f"Не удалось удалить:\n{e}")

    # ---- Refresh / Select ----

    def _refresh(self):
        for item in self.tree_uploads.get_children():
            self.tree_uploads.delete(item)
        try:
            uploads = PayrollDataManager.get_uploads()
        except Exception as e:
            logging.exception("Ошибка загрузки списка payroll_uploads")
            return
        for u in uploads:
            dt = u.get("uploaded_at")
            dt_str = dt.strftime("%d.%m.%Y %H:%M") if dt else ""
            total_sum = float(u.get("total_sum", 0) or 0)
            self.tree_uploads.insert("", "end", iid=str(u["id"]), values=(
                u["id"],
                u.get("period_label") or f"{u['month']:02d}.{u['year']}",
                u.get("file_name") or "",
                u.get("row_count", 0),
                f"{total_sum:,.2f}".replace(",", " "),
                u.get("dist_count", 0),
                dt_str,
            ))
        # Восстанавливаем выделение
        if self._selected_upload_id:
            iid = str(self._selected_upload_id)
            if self.tree_uploads.exists(iid):
                self.tree_uploads.selection_set(iid)
                self.tree_uploads.focus(iid)

    def _on_upload_selected(self, event=None):
        sel = self.tree_uploads.selection()
        if not sel:
            return
        uid = int(sel[0])
        self._selected_upload_id = uid
        self._show_upload_details(uid)

    # ---- Details ----

    def _clear_tab(self, tab):
        for w in tab.winfo_children():
            w.destroy()

    def _clear_details(self):
        self._clear_tab(self.tab_summary)
        self._clear_tab(self.tab_by_object)
        self._clear_tab(self.tab_by_dept)
        self._clear_tab(self.tab_unmatched)

    def _show_upload_details(self, upload_id: int):
        self._clear_details()
        try:
            self._build_summary_tab(upload_id)
            self._build_by_object_tab(upload_id)
            self._build_by_dept_tab(upload_id)
            self._build_unmatched_tab(upload_id)
        except Exception as e:
            logging.exception("Ошибка построения деталей загрузки")
            ttk.Label(self.tab_summary, text=f"Ошибка: {e}").pack(padx=10, pady=10)

    # ---- Tab: Сводка ----

    def _create_kpi_card(self, parent, title, value, unit):
        card = ttk.Frame(parent, borderwidth=2, relief="groove", padding=10)
        ttk.Label(card, text=title, font=("Segoe UI", 9, "bold")).pack()
        ttk.Label(card, text=f"{value}",
                  font=("Segoe UI", 16, "bold"),
                  foreground="#0078D7").pack(pady=(4, 0))
        ttk.Label(card, text=unit, font=("Segoe UI", 8)).pack()
        return card

    def _build_summary_tab(self, upload_id: int):
        tab = self.tab_summary
        s = PayrollDataManager.get_upload_summary(upload_id)

        kpi_frame = ttk.Frame(tab)
        kpi_frame.pack(fill="x", pady=10, padx=10)

        cards = [
            ("Всего строк", s["total_rows"], "чел."),
            ("Сумма ФОТ",
             f"{s['total_accrued']:,.0f}".replace(",", " "), "₽"),
            ("Распределено",
             f"{s['total_distributed']:,.0f}".replace(",", " "), "₽"),
            ("Не распределено",
             f"{s['undistributed_amount']:,.0f}".replace(",", " "), "₽"),
            ("Сотр. распред.", s["rows_distributed"], "чел."),
            ("Сотр. без объекта", s["rows_not_distributed"], "чел."),
            ("Объектов", s["objects_count"], "шт."),
        ]
        for i, (title, value, unit) in enumerate(cards):
            card = self._create_kpi_card(kpi_frame, title, value, unit)
            card.grid(row=0, column=i, padx=4, sticky="ew")
            kpi_frame.grid_columnconfigure(i, weight=1)

        # Процент распределения
        pct = 0.0
        if s["total_accrued"] > 0:
            pct = s["total_distributed"] / s["total_accrued"] * 100
        pct_frame = ttk.Frame(tab)
        pct_frame.pack(fill="x", padx=10, pady=(0, 10))

        bar_bg = ttk.Frame(pct_frame, relief="sunken", borderwidth=1)
        bar_bg.pack(fill="x", pady=4)
        bar_fill = tk.Frame(bar_bg, bg="#0078D7", height=20)
        bar_fill.pack(side="left", fill="y")
        # Обновим ширину после отрисовки
        def _update_bar(event=None):
            total_w = bar_bg.winfo_width()
            fill_w = max(1, int(total_w * pct / 100))
            bar_fill.configure(width=fill_w)
        bar_bg.bind("<Configure>", _update_bar)

        ttk.Label(pct_frame,
                  text=f"Распределено {pct:.1f}% от общей суммы ФОТ",
                  font=("Segoe UI", 9)).pack(anchor="w")

        # Предупреждение если не всё распределено
        if s["rows_not_distributed"] > 0:
            warn_frame = ttk.Frame(tab)
            warn_frame.pack(fill="x", padx=10, pady=5)
            ttk.Label(
                warn_frame,
                text=f"⚠ {s['rows_not_distributed']} сотрудник(ов) не найдены в табелях "
                     f"за этот период. Их ФОТ ({s['undistributed_amount']:,.0f} ₽) "
                     f"не распределён по объектам. См. вкладку «Не распределено».",
                foreground="#B00020",
                wraplength=700,
                justify="left",
            ).pack(anchor="w")

    # ---- Tab: По объектам ----
    # ---- Tab: По объектам ----

    def _build_by_object_tab(self, upload_id: int):
        tab = self.tab_by_object
        df = PayrollDataManager.get_distribution_by_object(upload_id)

        if df.empty:
            ttk.Label(tab,
                      text="Нет данных. Нажмите «Распределить ФОТ».",
                      font=("Segoe UI", 10)).pack(padx=20, pady=20)
            return

        # Кнопка экспорта
        btn_frame = ttk.Frame(tab)
        btn_frame.pack(fill="x", padx=5, pady=(5, 0))
        ttk.Button(btn_frame, text="📥 Выгрузить в Excel",
                   command=lambda: self._export_by_object(upload_id)).pack(side="right", padx=5)

        # Таблица на всю ширину
        table_frame = ttk.Frame(tab)
        table_frame.pack(fill="both", expand=True, padx=5, pady=5)

        tree = ttk.Treeview(table_frame, columns=(
            "num", "object", "type", "people", "hours", "amount", "share"
        ), show="headings", height=22)

        cols = [
            ("num", "№", 40, "center"),
            ("object", "Объект", 400, "w"),
            ("type", "Тип", 100, "w"),
            ("people", "Людей", 65, "e"),
            ("hours", "Часов", 90, "e"),
            ("amount", "Сумма, ₽", 130, "e"),
            ("share", "Доля %", 70, "e"),
        ]
        for cid, text, w, anchor in cols:
            tree.heading(cid, text=text)
            tree.column(cid, width=w, anchor=anchor, minwidth=40)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)

        grand_total = df["total_amount"].sum()
        for idx, (_, row) in enumerate(df.iterrows(), 1):
            share = (row["total_amount"] / grand_total * 100) if grand_total > 0 else 0
            tree.insert("", "end", values=(
                idx,
                row.get("object_name", "—"),
                row.get("object_type", ""),
                int(row["people_cnt"]),
                f"{row['total_hours']:,.1f}".replace(",", " "),
                f"{row['total_amount']:,.2f}".replace(",", " "),
                f"{share:.1f}",
            ))

        # Итого
        tree.insert("", "end", values=(
            "", "ИТОГО", "",
            int(df["people_cnt"].sum()),
            f"{df['total_hours'].sum():,.1f}".replace(",", " "),
            f"{grand_total:,.2f}".replace(",", " "),
            "100.0",
        ), tags=("total",))
        tree.tag_configure("total", font=("Segoe UI", 9, "bold"))

    # ---- Tab: По подразделениям ----

    def _build_by_dept_tab(self, upload_id: int):
        tab = self.tab_by_dept
        df = PayrollDataManager.get_distribution_by_department(upload_id)

        if df.empty:
            ttk.Label(tab, text="Нет данных.").pack(padx=20, pady=20)
            return

        # Кнопка экспорта
        btn_frame = ttk.Frame(tab)
        btn_frame.pack(fill="x", padx=5, pady=(5, 0))
        ttk.Button(btn_frame, text="📥 Выгрузить в Excel",
                   command=lambda: self._export_by_dept(upload_id)).pack(side="right", padx=5)

        # Таблица на всю ширину
        table_frame = ttk.Frame(tab)
        table_frame.pack(fill="both", expand=True, padx=5, pady=5)

        tree = ttk.Treeview(table_frame, columns=(
            "num", "dept", "people", "accrued", "distributed", "diff", "pct"
        ), show="headings", height=22)

        for cid, text, w, anc in [
            ("num", "№", 40, "center"),
            ("dept", "Подразделение", 300, "w"),
            ("people", "Людей", 65, "e"),
            ("accrued", "Начислено, ₽", 130, "e"),
            ("distributed", "Распределено, ₽", 130, "e"),
            ("diff", "Остаток, ₽", 120, "e"),
            ("pct", "Распр. %", 75, "e"),
        ]:
            tree.heading(cid, text=text)
            tree.column(cid, width=w, anchor=anc, minwidth=40)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)

        for idx, (_, row) in enumerate(df.iterrows(), 1):
            diff = row["total_accrued"] - row["total_distributed"]
            pct = (row["total_distributed"] / row["total_accrued"] * 100) if row["total_accrued"] > 0 else 0
            tree.insert("", "end", values=(
                idx,
                row["department_name"],
                int(row["people_cnt"]),
                f"{row['total_accrued']:,.2f}".replace(",", " "),
                f"{row['total_distributed']:,.2f}".replace(",", " "),
                f"{diff:,.2f}".replace(",", " "),
                f"{pct:.1f}",
            ))

        # Итого
        total_accrued = df["total_accrued"].sum()
        total_distributed = df["total_distributed"].sum()
        total_diff = total_accrued - total_distributed
        total_pct = (total_distributed / total_accrued * 100) if total_accrued > 0 else 0
        tree.insert("", "end", values=(
            "", "ИТОГО",
            int(df["people_cnt"].sum()),
            f"{total_accrued:,.2f}".replace(",", " "),
            f"{total_distributed:,.2f}".replace(",", " "),
            f"{total_diff:,.2f}".replace(",", " "),
            f"{total_pct:.1f}",
        ), tags=("total",))
        tree.tag_configure("total", font=("Segoe UI", 9, "bold"))

    # ---- Tab: Не распределено (тоже добавим экспорт) ----

    def _build_unmatched_tab(self, upload_id: int):
        tab = self.tab_unmatched
        df = PayrollDataManager.get_undistributed_rows(upload_id)

        if df.empty:
            ttk.Label(tab,
                      text="✅ Все сотрудники успешно распределены по объектам!",
                      font=("Segoe UI", 11),
                      foreground="#16A34A").pack(padx=20, pady=30)
            return

        total_lost = df["total_accrued"].fillna(0).astype(float).sum()

        info_frame = ttk.Frame(tab)
        info_frame.pack(fill="x", padx=10, pady=8)

        ttk.Label(
            info_frame,
            text=f"⚠ {len(df)} сотрудник(ов) не найдены в объектном табеле "
                 f"за данный месяц.\n"
                 f"Нераспределённая сумма: {total_lost:,.2f} ₽\n\n"
                 f"Возможные причины:\n"
                 f"  • Табельный номер в Excel не совпадает с tbn в табеле\n"
                 f"  • Сотрудник не внесён в объектный табель за этот месяц\n"
                 f"  • Административный/офисный персонал без объекта",
            foreground="#B00020",
            wraplength=700,
            justify="left",
        ).pack(side="left", anchor="w")

        ttk.Button(
            info_frame, text="📥 Выгрузить в Excel",
            command=lambda: self._export_unmatched(upload_id),
        ).pack(side="right", padx=5)

        tree_frame = ttk.Frame(tab)
        tree_frame.pack(fill="both", expand=True, padx=10, pady=5)

        tree = ttk.Treeview(tree_frame, columns=(
            "num", "tbn", "fio", "dept", "pos", "accrued"
        ), show="headings", height=20)

        for cid, text, w, anc in [
            ("num", "№", 40, "center"),
            ("tbn", "Таб. номер", 100, "w"),
            ("fio", "ФИО", 250, "w"),
            ("dept", "Подразделение", 200, "w"),
            ("pos", "Должность", 200, "w"),
            ("accrued", "Начислено, ₽", 120, "e"),
        ]:
            tree.heading(cid, text=text)
            tree.column(cid, width=w, anchor=anc, minwidth=40)

        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        for idx, (_, row) in enumerate(df.iterrows(), 1):
            accrued = float(row.get("total_accrued", 0) or 0)
            tree.insert("", "end", values=(
                idx,
                row.get("tbn", ""),
                row.get("fio", ""),
                row.get("department_raw", ""),
                row.get("position_raw", ""),
                f"{accrued:,.2f}".replace(",", " "),
            ))

        tree.insert("", "end", values=(
            "", "", "ИТОГО", "", "",
            f"{total_lost:,.2f}".replace(",", " "),
        ), tags=("total",))
        tree.tag_configure("total", font=("Segoe UI", 9, "bold"))

    # ============================================================
    #  ЭКСПОРТ В EXCEL
    # ============================================================

    def _ask_save_path(self, default_name: str) -> Optional[str]:
        path = filedialog.asksaveasfilename(
            title="Сохранить как",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
        )
        return path if path else None

    def _export_by_object(self, upload_id: int):
        df = PayrollDataManager.get_distribution_by_object(upload_id)
        if df.empty:
            messagebox.showinfo("Экспорт", "Нет данных для выгрузки.")
            return

        path = self._ask_save_path(f"ФОТ_по_объектам_{upload_id}.xlsx")
        if not path:
            return

        try:
            grand_total = df["total_amount"].sum()
            df_export = df.copy()
            df_export["share_pct"] = df_export["total_amount"].apply(
                lambda x: round(x / grand_total * 100, 1) if grand_total > 0 else 0
            )
            df_export = df_export.rename(columns={
                "object_name": "Объект",
                "object_type": "Тип объекта",
                "people_cnt": "Кол-во сотрудников",
                "total_hours": "Часов на объекте",
                "total_amount": "Сумма ФОТ, ₽",
                "share_pct": "Доля, %",
            })

            # Добавляем строку итого
            totals = pd.DataFrame([{
                "Объект": "ИТОГО",
                "Тип объекта": "",
                "Кол-во сотрудников": int(df_export["Кол-во сотрудников"].sum()),
                "Часов на объекте": round(df_export["Часов на объекте"].sum(), 1),
                "Сумма ФОТ, ₽": round(df_export["Сумма ФОТ, ₽"].sum(), 2),
                "Доля, %": 100.0,
            }])
            df_export = pd.concat([df_export, totals], ignore_index=True)

            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                df_export.to_excel(writer, index=False, sheet_name="По объектам")
                self._autofit_columns(writer, "По объектам", df_export)

            messagebox.showinfo("Экспорт", f"Файл сохранён:\n{path}")
        except Exception as e:
            logging.exception("Ошибка экспорта по объектам")
            messagebox.showerror("Ошибка", f"Не удалось сохранить файл:\n{e}")

    def _export_by_dept(self, upload_id: int):
        df = PayrollDataManager.get_distribution_by_department(upload_id)
        if df.empty:
            messagebox.showinfo("Экспорт", "Нет данных для выгрузки.")
            return

        path = self._ask_save_path(f"ФОТ_по_подразделениям_{upload_id}.xlsx")
        if not path:
            return

        try:
            df_export = df.copy()
            df_export["diff"] = df_export["total_accrued"] - df_export["total_distributed"]
            df_export["pct"] = df_export.apply(
                lambda r: round(r["total_distributed"] / r["total_accrued"] * 100, 1)
                if r["total_accrued"] > 0 else 0, axis=1
            )
            df_export = df_export.rename(columns={
                "department_name": "Подразделение",
                "people_cnt": "Кол-во сотрудников",
                "total_accrued": "Начислено, ₽",
                "total_distributed": "Распределено, ₽",
                "diff": "Остаток, ₽",
                "pct": "Распределено, %",
            })

            totals = pd.DataFrame([{
                "Подразделение": "ИТОГО",
                "Кол-во сотрудников": int(df_export["Кол-во сотрудников"].sum()),
                "Начислено, ₽": round(df_export["Начислено, ₽"].sum(), 2),
                "Распределено, ₽": round(df_export["Распределено, ₽"].sum(), 2),
                "Остаток, ₽": round(df_export["Остаток, ₽"].sum(), 2),
                "Распределено, %": "",
            }])
            df_export = pd.concat([df_export, totals], ignore_index=True)

            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                df_export.to_excel(writer, index=False, sheet_name="По подразделениям")
                self._autofit_columns(writer, "По подразделениям", df_export)

            messagebox.showinfo("Экспорт", f"Файл сохранён:\n{path}")
        except Exception as e:
            logging.exception("Ошибка экспорта по подразделениям")
            messagebox.showerror("Ошибка", f"Не удалось сохранить файл:\n{e}")

    def _export_unmatched(self, upload_id: int):
        df = PayrollDataManager.get_undistributed_rows(upload_id)
        if df.empty:
            messagebox.showinfo("Экспорт", "Нет нераспределённых сотрудников.")
            return

        path = self._ask_save_path(f"ФОТ_нераспределено_{upload_id}.xlsx")
        if not path:
            return

        try:
            df_export = df.copy()
            df_export = df_export.rename(columns={
                "tbn": "Таб. номер",
                "fio": "ФИО",
                "department_raw": "Подразделение",
                "position_raw": "Должность",
                "total_accrued": "Начислено, ₽",
            })

            total_lost = df_export["Начислено, ₽"].fillna(0).astype(float).sum()
            totals = pd.DataFrame([{
                "Таб. номер": "",
                "ФИО": "ИТОГО",
                "Подразделение": "",
                "Должность": "",
                "Начислено, ₽": round(total_lost, 2),
            }])
            df_export = pd.concat([df_export, totals], ignore_index=True)

            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                df_export.to_excel(writer, index=False, sheet_name="Не распределено")
                self._autofit_columns(writer, "Не распределено", df_export)

            messagebox.showinfo("Экспорт", f"Файл сохранён:\n{path}")
        except Exception as e:
            logging.exception("Ошибка экспорта нераспределённых")
            messagebox.showerror("Ошибка", f"Не удалось сохранить файл:\n{e}")

    def _autofit_columns(self, writer, sheet_name: str, df: pd.DataFrame):
        """Автоподбор ширины колонок в Excel."""
        try:
            ws = writer.sheets[sheet_name]
            for i, col in enumerate(df.columns):
                max_len = max(
                    len(str(col)),
                    df[col].astype(str).str.len().max() if len(df) > 0 else 0
                )
                ws.column_dimensions[chr(65 + i) if i < 26
                                     else chr(64 + i // 26) + chr(65 + i % 26)
                                     ].width = min(max_len + 3, 50)
        except Exception:
            pass  # не критично если автоширина не сработает

# ============================================================
#  Функция-фабрика для main_app
# ============================================================

def create_payroll_page(parent, app_ref) -> PayrollPage:
    """Фабрика для вызова из main_app._show_page."""
    return PayrollPage(parent, app_ref)
