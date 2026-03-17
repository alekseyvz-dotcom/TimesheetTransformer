from __future__ import annotations

import calendar
import logging
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  COLORS / THEME
# ═══════════════════════════════════════════════════════════════
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
STATUS_LABEL_TO_CODE = {v[2]: k for k, v in STATUS_COLORS.items()}

TASK_ROLES = {
    "executor": "Исполнитель",
    "foreman": "Бригадир",
    "inspector": "Контролёр",
}
TASK_ROLE_LIST = list(TASK_ROLES.keys())
TASK_ROLE_LABELS = list(TASK_ROLES.values())
TASK_ROLE_LABEL_TO_CODE = {v: k for k, v in TASK_ROLES.items()}


# ═══════════════════════════════════════════════════════════════
#  MODELS
# ═══════════════════════════════════════════════════════════════
@dataclass
class GprAssignment:
    employee_id: Optional[int]
    fio: str
    tbn: str = ""
    position: str = ""
    department: str = ""
    role_in_task: str = "executor"
    note: Optional[str] = None
    assignment_id: Optional[int] = None

    def normalized_copy(self) -> "GprAssignment":
        return GprAssignment(
            employee_id=int(self.employee_id) if self.employee_id not in (None, "") else None,
            fio=normalize_spaces(self.fio),
            tbn=normalize_spaces(self.tbn),
            position=normalize_spaces(self.position),
            department=normalize_spaces(self.department),
            role_in_task=normalize_task_role(self.role_in_task),
            note=normalize_spaces(self.note or "") or None,
            assignment_id=int(self.assignment_id) if self.assignment_id not in (None, "") else None,
        )

    def fingerprint(self) -> tuple[Any, ...]:
        n = self.normalized_copy()
        return (
            n.employee_id,
            n.fio.lower(),
            n.tbn,
            n.position,
            n.department,
            n.role_in_task,
            n.note or "",
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self.normalized_copy())


@dataclass
class GprTask:
    id: Optional[int] = None
    client_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    plan_id: Optional[int] = None
    parent_id: Optional[int] = None

    work_type_id: Optional[int] = None
    work_type_name: str = ""

    name: str = ""
    uom_code: Optional[str] = None
    plan_qty: Optional[float] = None

    plan_start: Optional[date] = None
    plan_finish: Optional[date] = None

    status: str = "planned"
    sort_order: int = 0
    is_milestone: bool = False

    created_by: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    assignments: List[GprAssignment] = field(default_factory=list)

    def normalized_copy(self) -> "GprTask":
        start = coerce_to_date(self.plan_start)
        finish = coerce_to_date(self.plan_finish)

        return GprTask(
            id=int(self.id) if self.id not in (None, "") else None,
            client_id=self.client_id or uuid.uuid4().hex,
            plan_id=int(self.plan_id) if self.plan_id not in (None, "") else None,
            parent_id=int(self.parent_id) if self.parent_id not in (None, "") else None,
            work_type_id=int(self.work_type_id) if self.work_type_id not in (None, "") else None,
            work_type_name=normalize_spaces(self.work_type_name),
            name=normalize_spaces(self.name),
            uom_code=normalize_spaces(self.uom_code or "") or None,
            plan_qty=safe_float(self.plan_qty),
            plan_start=start,
            plan_finish=finish,
            status=normalize_status(self.status),
            sort_order=int(self.sort_order or 0),
            is_milestone=bool(self.is_milestone),
            created_by=int(self.created_by) if self.created_by not in (None, "") else None,
            created_at=self.created_at,
            updated_at=self.updated_at,
            assignments=[a.normalized_copy() for a in self.assignments],
        )

    def duration_days(self) -> Optional[int]:
        if isinstance(self.plan_start, date) and isinstance(self.plan_finish, date):
            return (self.plan_finish - self.plan_start).days + 1
        return None

    def tree_iid(self) -> str:
        return task_to_tree_iid(self)

    def fingerprint(self) -> tuple[Any, ...]:
        t = self.normalized_copy()
        return (
            t.id,
            t.client_id,
            t.plan_id,
            t.parent_id,
            t.work_type_id,
            t.work_type_name,
            t.name,
            t.uom_code or "",
            t.plan_qty,
            t.plan_start.isoformat() if isinstance(t.plan_start, date) else None,
            t.plan_finish.isoformat() if isinstance(t.plan_finish, date) else None,
            t.status,
            t.sort_order,
            t.is_milestone,
            tuple(a.fingerprint() for a in t.assignments),
        )

    def to_dict(self) -> Dict[str, Any]:
        t = self.normalized_copy()
        return {
            "id": t.id,
            "client_id": t.client_id,
            "plan_id": t.plan_id,
            "parent_id": t.parent_id,
            "work_type_id": t.work_type_id,
            "work_type_name": t.work_type_name,
            "name": t.name,
            "uom_code": t.uom_code,
            "plan_qty": t.plan_qty,
            "plan_start": t.plan_start,
            "plan_finish": t.plan_finish,
            "status": t.status,
            "sort_order": t.sort_order,
            "is_milestone": t.is_milestone,
            "created_by": t.created_by,
            "created_at": t.created_at,
            "updated_at": t.updated_at,
            "assignments": [a.to_dict() for a in t.assignments],
        }


@dataclass
class GprPlanInfo:
    id: int
    object_db_id: int
    version_no: int = 1
    is_current: bool = True
    is_baseline: bool = False
    created_by: Optional[int] = None
    creator_name: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ═══════════════════════════════════════════════════════════════
#  BASIC HELPERS
# ═══════════════════════════════════════════════════════════════
def normalize_spaces(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def safe_filename(value: str, maxlen: int = 120) -> str:
    s = normalize_spaces(value)
    s = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", s)
    s = re.sub(r"_+", "_", s)
    return s[:maxlen] if maxlen > 0 else s


def parse_date(value: str) -> date:
    return datetime.strptime(normalize_spaces(value), "%d.%m.%Y").date()


def fmt_date(value: Any) -> str:
    if isinstance(value, date):
        return value.strftime("%d.%m.%Y")
    return str(value or "")


def today() -> date:
    return datetime.now().date()


def quarter_range(base_date: Optional[date] = None) -> Tuple[date, date]:
    t = base_date or today()
    q_start_month = ((t.month - 1) // 3) * 3 + 1
    d0 = date(t.year, q_start_month, 1)
    end_month = q_start_month + 2
    d1 = date(t.year, end_month, calendar.monthrange(t.year, end_month)[1])
    return d0, d1


def safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "."))
    except Exception:
        return None


def fmt_qty(value: Any) -> str:
    f = safe_float(value)
    if f is None:
        return ""
    return f"{f:.3f}".rstrip("0").rstrip(".")


def coerce_to_date(value: Any) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        s = normalize_spaces(value)
        if not s:
            return None

        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                continue

        try:
            return datetime.fromisoformat(s).date()
        except Exception:
            return None

    return None


def coerce_to_datetime(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        s = normalize_spaces(value)
        if not s:
            return None
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None
    return None


def normalize_status(value: str) -> str:
    s = normalize_spaces(value)
    if s in STATUS_LIST:
        return s
    if s in STATUS_LABEL_TO_CODE:
        return STATUS_LABEL_TO_CODE[s]
    return "planned"


def normalize_task_role(value: str) -> str:
    s = normalize_spaces(value)
    if s in TASK_ROLE_LIST:
        return s
    if s in TASK_ROLE_LABEL_TO_CODE:
        return TASK_ROLE_LABEL_TO_CODE[s]
    return "executor"


def status_label(status_code: str) -> str:
    return STATUS_LABELS.get(normalize_status(status_code), normalize_status(status_code))


def status_fill_color(status_code: str) -> str:
    return STATUS_COLORS.get(normalize_status(status_code), ("#cccccc", "#333333", ""))[0]


def role_label(role_code: str) -> str:
    return TASK_ROLES.get(normalize_task_role(role_code), normalize_task_role(role_code))


# ═══════════════════════════════════════════════════════════════
#  TASK HELPERS
# ═══════════════════════════════════════════════════════════════
def task_to_tree_iid(task: GprTask | Dict[str, Any]) -> str:
    if isinstance(task, GprTask):
        task_id = task.id
        client_id = task.client_id
    else:
        task_id = task.get("id")
        client_id = task.get("client_id")

    if task_id not in (None, ""):
        return f"db:{int(task_id)}"

    if not client_id:
        client_id = uuid.uuid4().hex
        if isinstance(task, dict):
            task["client_id"] = client_id

    return f"tmp:{client_id}"


def ensure_task_client_id(task: GprTask | Dict[str, Any]) -> str:
    if isinstance(task, GprTask):
        if not task.client_id:
            task.client_id = uuid.uuid4().hex
        return task.client_id

    client_id = normalize_spaces(task.get("client_id") or "")
    if not client_id:
        client_id = uuid.uuid4().hex
        task["client_id"] = client_id
    return client_id


def assignment_from_db_row(row: Mapping[str, Any]) -> GprAssignment:
    return GprAssignment(
        assignment_id=row.get("assignment_id"),
        employee_id=row.get("employee_id"),
        fio=row.get("fio") or "",
        tbn=row.get("tbn") or "",
        position=row.get("position") or "",
        department=row.get("department") or "",
        role_in_task=row.get("role_in_task") or "executor",
        note=row.get("note"),
    ).normalized_copy()


def task_from_db_row(
    row: Mapping[str, Any],
    assignments: Optional[Sequence[GprAssignment | Mapping[str, Any]]] = None,
) -> GprTask:
    normalized_assignments: List[GprAssignment] = []
    for item in assignments or []:
        if isinstance(item, GprAssignment):
            normalized_assignments.append(item.normalized_copy())
        else:
            normalized_assignments.append(assignment_from_db_row(item))

    return GprTask(
        id=row.get("id"),
        client_id=uuid.uuid4().hex,
        plan_id=row.get("plan_id"),
        parent_id=row.get("parent_id"),
        work_type_id=row.get("work_type_id"),
        work_type_name=row.get("work_type_name") or "",
        name=row.get("name") or "",
        uom_code=row.get("uom_code"),
        plan_qty=safe_float(row.get("plan_qty")),
        plan_start=coerce_to_date(row.get("plan_start")),
        plan_finish=coerce_to_date(row.get("plan_finish")),
        status=row.get("status") or "planned",
        sort_order=int(row.get("sort_order") or 0),
        is_milestone=bool(row.get("is_milestone")),
        created_by=row.get("created_by"),
        created_at=coerce_to_datetime(row.get("created_at")),
        updated_at=coerce_to_datetime(row.get("updated_at")),
        assignments=normalized_assignments,
    ).normalized_copy()


def task_from_dialog_result(
    result: Mapping[str, Any],
    *,
    existing_id: Optional[int] = None,
    existing_client_id: Optional[str] = None,
    sort_order: int = 0,
    work_type_name: str = "",
) -> GprTask:
    assignments = []
    for item in result.get("_assignments") or []:
        assignments.append(
            GprAssignment(
                assignment_id=item.get("assignment_id"),
                employee_id=item.get("employee_id"),
                fio=item.get("fio") or "",
                tbn=item.get("tbn") or "",
                position=item.get("position") or "",
                department=item.get("department") or "",
                role_in_task=item.get("role_in_task") or "executor",
                note=item.get("note"),
            )
        )

    return GprTask(
        id=existing_id,
        client_id=existing_client_id or uuid.uuid4().hex,
        work_type_id=result.get("work_type_id"),
        work_type_name=work_type_name,
        name=result.get("name") or "",
        uom_code=result.get("uom_code"),
        plan_qty=safe_float(result.get("plan_qty")),
        plan_start=coerce_to_date(result.get("plan_start")),
        plan_finish=coerce_to_date(result.get("plan_finish")),
        status=result.get("status") or "planned",
        sort_order=sort_order,
        is_milestone=bool(result.get("is_milestone")),
        assignments=assignments,
    ).normalized_copy()


def task_to_dialog_init(task: GprTask | Mapping[str, Any]) -> Dict[str, Any]:
    if isinstance(task, GprTask):
        t = task.normalized_copy()
        return {
            "id": t.id,
            "client_id": t.client_id,
            "work_type_id": t.work_type_id,
            "work_type_name": t.work_type_name,
            "name": t.name,
            "uom_code": t.uom_code,
            "plan_qty": t.plan_qty,
            "plan_start": t.plan_start,
            "plan_finish": t.plan_finish,
            "status": t.status,
            "sort_order": t.sort_order,
            "is_milestone": t.is_milestone,
            "_assignments": [a.to_dict() for a in t.assignments],
        }

    ensure_task_client_id(task)
    return {
        "id": task.get("id"),
        "client_id": task.get("client_id"),
        "work_type_id": task.get("work_type_id"),
        "work_type_name": task.get("work_type_name") or "",
        "name": task.get("name") or "",
        "uom_code": task.get("uom_code"),
        "plan_qty": safe_float(task.get("plan_qty")),
        "plan_start": coerce_to_date(task.get("plan_start")),
        "plan_finish": coerce_to_date(task.get("plan_finish")),
        "status": normalize_status(task.get("status") or "planned"),
        "sort_order": int(task.get("sort_order") or 0),
        "is_milestone": bool(task.get("is_milestone")),
        "_assignments": list(task.get("assignments") or []),
    }


def clone_task_as_new(task: GprTask) -> GprTask:
    t = task.normalized_copy()
    return GprTask(
        id=None,
        client_id=uuid.uuid4().hex,
        plan_id=t.plan_id,
        parent_id=t.parent_id,
        work_type_id=t.work_type_id,
        work_type_name=t.work_type_name,
        name=t.name,
        uom_code=t.uom_code,
        plan_qty=t.plan_qty,
        plan_start=t.plan_start,
        plan_finish=t.plan_finish,
        status="planned" if t.status == "done" else t.status,
        sort_order=t.sort_order,
        is_milestone=t.is_milestone,
        assignments=[a.normalized_copy() for a in t.assignments],
    ).normalized_copy()


def normalize_task(task: GprTask | Mapping[str, Any]) -> GprTask:
    if isinstance(task, GprTask):
        return task.normalized_copy()

    ensure_task_client_id(task)
    assignments: List[GprAssignment] = []
    for item in task.get("assignments") or task.get("_assignments") or []:
        if isinstance(item, GprAssignment):
            assignments.append(item.normalized_copy())
        else:
            assignments.append(
                GprAssignment(
                    assignment_id=item.get("assignment_id"),
                    employee_id=item.get("employee_id"),
                    fio=item.get("fio") or "",
                    tbn=item.get("tbn") or "",
                    position=item.get("position") or "",
                    department=item.get("department") or "",
                    role_in_task=item.get("role_in_task") or "executor",
                    note=item.get("note"),
                ).normalized_copy()
            )

    return GprTask(
        id=task.get("id"),
        client_id=task.get("client_id"),
        plan_id=task.get("plan_id"),
        parent_id=task.get("parent_id"),
        work_type_id=task.get("work_type_id"),
        work_type_name=task.get("work_type_name") or "",
        name=task.get("name") or "",
        uom_code=task.get("uom_code"),
        plan_qty=safe_float(task.get("plan_qty")),
        plan_start=coerce_to_date(task.get("plan_start")),
        plan_finish=coerce_to_date(task.get("plan_finish")),
        status=task.get("status") or "planned",
        sort_order=int(task.get("sort_order") or 0),
        is_milestone=bool(task.get("is_milestone")),
        created_by=task.get("created_by"),
        created_at=coerce_to_datetime(task.get("created_at")),
        updated_at=coerce_to_datetime(task.get("updated_at")),
        assignments=assignments,
    ).normalized_copy()


def normalize_tasks(tasks: Sequence[GprTask | Mapping[str, Any]]) -> List[GprTask]:
    out = [normalize_task(t) for t in tasks]
    # нормализуем порядок по текущему индексу, если sort_order повторяется или пустой
    for i, t in enumerate(out):
        if t.sort_order is None:
            t.sort_order = i * 10
    return out


def find_task_index_by_iid(tasks: Sequence[GprTask], iid: str) -> Optional[int]:
    iid_norm = normalize_spaces(iid)
    if not iid_norm:
        return None

    if iid_norm.startswith("db:"):
        try:
            db_id = int(iid_norm.split(":", 1)[1])
        except Exception:
            return None
        for idx, task in enumerate(tasks):
            if task.id is not None and int(task.id) == db_id:
                return idx
        return None

    if iid_norm.startswith("tmp:"):
        cid = iid_norm.split(":", 1)[1].strip()
        for idx, task in enumerate(tasks):
            if task.client_id == cid:
                return idx
        return None

    return None


def tasks_signature(tasks: Sequence[GprTask]) -> tuple[Any, ...]:
    return tuple(t.fingerprint() for t in normalize_tasks(tasks))


# ═══════════════════════════════════════════════════════════════
#  OBJECT LABEL HELPERS
# ═══════════════════════════════════════════════════════════════
def build_object_display_label(obj: Mapping[str, Any]) -> str:
    short_name = normalize_spaces(obj.get("short_name") or "")
    address = normalize_spaces(obj.get("address") or "")
    excel_id = normalize_spaces(obj.get("excel_id") or "")
    db_id = obj.get("id")

    tag = f"[{excel_id}]" if excel_id else f"[id:{db_id}]"
    if short_name:
        return f"{short_name} — {address} — {tag}"
    return f"{address} — {tag}"


def build_object_label_map(objects: Sequence[Mapping[str, Any]]) -> tuple[list[str], dict[str, int]]:
    labels: List[str] = []
    label_to_id: dict[str, int] = {}

    for obj in objects:
        try:
            object_id = int(obj["id"])
        except Exception:
            continue
        label = build_object_display_label(obj)
        labels.append(label)
        label_to_id[label] = object_id

    return labels, label_to_id


# ═══════════════════════════════════════════════════════════════
#  VALIDATION
# ═══════════════════════════════════════════════════════════════
def validate_assignment(assignment: GprAssignment) -> List[str]:
    errors: List[str] = []
    a = assignment.normalized_copy()

    if a.employee_id is None and not a.fio:
        errors.append("Назначение без employee_id и без ФИО.")
    if a.role_in_task not in TASK_ROLE_LIST:
        errors.append(f"Недопустимая роль на задаче: {a.role_in_task}")

    return errors


def validate_task(task: GprTask) -> List[str]:
    errors: List[str] = []
    t = task.normalized_copy()

    if t.work_type_id in (None, ""):
        errors.append("Не выбран тип работ.")

    if not t.name:
        errors.append("Не заполнен вид работ.")

    if t.status not in STATUS_LIST:
        errors.append(f"Недопустимый статус: {t.status}")

    if t.plan_qty is not None and t.plan_qty < 0:
        errors.append("Объём не может быть отрицательным.")

    if not isinstance(t.plan_start, date):
        errors.append("Не заполнена дата начала.")

    if not isinstance(t.plan_finish, date):
        errors.append("Не заполнена дата окончания.")

    if isinstance(t.plan_start, date) and isinstance(t.plan_finish, date):
        if t.plan_finish < t.plan_start:
            errors.append("Дата окончания раньше даты начала.")

    seen_employees: set[tuple[Any, ...]] = set()
    for a in t.assignments:
        for msg in validate_assignment(a):
            errors.append(f"Назначение: {msg}")

        key = (a.employee_id, a.fio.lower(), a.tbn, a.role_in_task)
        if key in seen_employees:
            errors.append(f"Дублирующее назначение сотрудника: {a.fio} ({a.tbn})")
        seen_employees.add(key)

    return errors


def validate_tasks(tasks: Sequence[GprTask]) -> List[str]:
    errors: List[str] = []

    for idx, task in enumerate(normalize_tasks(tasks), start=1):
        task_errors = validate_task(task)
        for msg in task_errors:
            errors.append(f"Строка {idx} ({task.name or 'без названия'}): {msg}")

    return errors


# ═══════════════════════════════════════════════════════════════
#  SUMMARY / FILTER / RANGE
# ═══════════════════════════════════════════════════════════════
def summarize_tasks(tasks: Sequence[GprTask], facts: Optional[Dict[int, float]] = None) -> Dict[str, Any]:
    facts = facts or {}
    total = len(tasks)
    overdue = 0
    done = 0
    canceled = 0
    in_progress = 0
    planned = 0
    paused = 0

    today_date = today()

    for t in tasks:
        status = normalize_status(t.status)
        if status == "done":
            done += 1
        elif status == "canceled":
            canceled += 1
        elif status == "in_progress":
            in_progress += 1
        elif status == "paused":
            paused += 1
        else:
            planned += 1

        if status not in ("done", "canceled") and isinstance(t.plan_finish, date) and t.plan_finish < today_date:
            overdue += 1

    return {
        "total": total,
        "planned": planned,
        "in_progress": in_progress,
        "done": done,
        "paused": paused,
        "canceled": canceled,
        "overdue": overdue,
        "facts_total": float(sum(float(v or 0) for v in facts.values())),
    }


def build_summary_text(tasks: Sequence[GprTask], facts: Optional[Dict[int, float]] = None) -> str:
    s = summarize_tasks(tasks, facts=facts)
    parts = [f"Всего: {s['total']}"]
    for status in STATUS_LIST:
        cnt = s.get(status, 0)
        if cnt:
            parts.append(f"{STATUS_LABELS[status]}: {cnt}")
    if s["overdue"]:
        parts.append(f"⚠ Просрочено: {s['overdue']}")
    return "  |  ".join(parts)


def fit_range_for_tasks(tasks: Sequence[GprTask], padding_days: int = 7) -> Optional[Tuple[date, date]]:
    starts = [t.plan_start for t in tasks if isinstance(t.plan_start, date)]
    finishes = [t.plan_finish for t in tasks if isinstance(t.plan_finish, date)]
    if not starts or not finishes:
        return None
    return min(starts) - timedelta(days=padding_days), max(finishes) + timedelta(days=padding_days)


def filter_tasks(
    tasks: Sequence[GprTask],
    *,
    work_type_name: Optional[str] = None,
    status_code: Optional[str] = None,
    search_text: str = "",
) -> List[GprTask]:
    wt = normalize_spaces(work_type_name or "")
    st = normalize_status(status_code) if status_code else ""
    q = normalize_spaces(search_text).lower()

    out: List[GprTask] = []
    for t in tasks:
        if wt and normalize_spaces(t.work_type_name) != wt:
            continue
        if st and normalize_status(t.status) != st:
            continue
        if q:
            hay = " | ".join(
                [
                    normalize_spaces(t.work_type_name).lower(),
                    normalize_spaces(t.name).lower(),
                    normalize_spaces(t.uom_code or "").lower(),
                ]
            )
            if q not in hay:
                continue
        out.append(t)
    return out


# ═══════════════════════════════════════════════════════════════
#  EXPORT HELPERS
# ═══════════════════════════════════════════════════════════════
def calc_fact_percent(plan_qty: Optional[float], fact_qty: Optional[float]) -> Optional[float]:
    pq = safe_float(plan_qty)
    fq = safe_float(fact_qty)
    if pq is None or pq <= 0 or fq is None:
        return None
    return min(100.0, (fq / pq) * 100.0)


def fmt_percent(value: Optional[float]) -> str:
    if value is None:
        return ""
    return f"{value:.1f}%"


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOTS / DIFF HELPERS
# ═══════════════════════════════════════════════════════════════
def split_tasks_for_save(
    existing_db_tasks: Sequence[GprTask],
    current_tasks: Sequence[GprTask],
) -> Dict[str, List[GprTask]]:
    """
    Подготавливает удобную структуру для diff-сохранения.

    Возвращает:
      {
        "insert": [...],
        "update": [...],
        "delete": [...],
      }
    """
    db_by_id = {t.id: t for t in existing_db_tasks if t.id is not None}
    cur_by_id = {t.id: t for t in current_tasks if t.id is not None}

    to_insert = [t for t in current_tasks if t.id is None]
    to_update = [t for t in current_tasks if t.id is not None and t.id in db_by_id]
    to_delete = [t for t in existing_db_tasks if t.id is not None and t.id not in cur_by_id]

    return {
        "insert": normalize_tasks(to_insert),
        "update": normalize_tasks(to_update),
        "delete": normalize_tasks(to_delete),
    }


__all__ = [
    "C",
    "STATUS_COLORS",
    "STATUS_LIST",
    "STATUS_LABELS",
    "STATUS_LABEL_TO_CODE",
    "TASK_ROLES",
    "TASK_ROLE_LIST",
    "TASK_ROLE_LABELS",
    "TASK_ROLE_LABEL_TO_CODE",
    "GprAssignment",
    "GprTask",
    "GprPlanInfo",
    "normalize_spaces",
    "safe_filename",
    "parse_date",
    "fmt_date",
    "today",
    "quarter_range",
    "safe_float",
    "fmt_qty",
    "coerce_to_date",
    "coerce_to_datetime",
    "normalize_status",
    "normalize_task_role",
    "status_label",
    "status_fill_color",
    "role_label",
    "task_to_tree_iid",
    "ensure_task_client_id",
    "assignment_from_db_row",
    "task_from_db_row",
    "task_from_dialog_result",
    "task_to_dialog_init",
    "clone_task_as_new",
    "normalize_task",
    "normalize_tasks",
    "find_task_index_by_iid",
    "tasks_signature",
    "build_object_display_label",
    "build_object_label_map",
    "validate_assignment",
    "validate_task",
    "validate_tasks",
    "summarize_tasks",
    "build_summary_text",
    "fit_range_for_tasks",
    "filter_tasks",
    "calc_fact_percent",
    "fmt_percent",
    "split_tasks_for_save",
]
