from __future__ import annotations

import calendar
from datetime import datetime, date
from typing import Optional, Tuple

C = {
    "bg": "#f0f2f5",
    "panel": "#ffffff",
    "accent": "#1565c0",
    "accent_light": "#e3f2fd",
    "success": "#2e7d32",
    "warning": "#ed6c02",
    "error": "#d32f2f",
    "border": "#dde1e7",
    "text": "#1a1a2e",
    "text2": "#555",
    "text3": "#999",
    "btn_bg": "#1565c0",
    "btn_fg": "#ffffff",
}

STATUS_COLORS = {
    "planned": ("#90caf9", "#1565c0", "Запланировано"),
    "in_progress": ("#ffcc80", "#e65100", "В работе"),
    "done": ("#a5d6a7", "#1b5e20", "Выполнено"),
    "paused": ("#fff176", "#f9a825", "Приостановлено"),
    "canceled": ("#ef9a9a", "#b71c1c", "Отменено"),
}

STATUS_LIST = ["planned", "in_progress", "done", "paused", "canceled"]
STATUS_LABELS = {k: v[2] for k, v in STATUS_COLORS.items()}


def parse_date(s: str) -> date:
    return datetime.strptime(s.strip(), "%d.%m.%Y").date()


def fmt_date(d) -> str:
    if isinstance(d, date):
        return d.strftime("%d.%m.%Y")
    return str(d or "")


def today() -> date:
    return datetime.now().date()


def quarter_range() -> Tuple[date, date]:
    t = today()
    q_start_month = ((t.month - 1) // 3) * 3 + 1
    d0 = date(t.year, q_start_month, 1)
    end_month = q_start_month + 2
    d1 = date(t.year, end_month, calendar.monthrange(t.year, end_month)[1])
    return d0, d1


def safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return None


def fmt_qty(v) -> str:
    f = safe_float(v)
    if f is None:
        return ""
    return f"{f:.3f}".rstrip("0").rstrip(".")
