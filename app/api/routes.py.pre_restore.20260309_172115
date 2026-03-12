from __future__ import annotations

import csv
import hashlib
import hmac
import io
import json
import re
from math import ceil
from typing import Optional
from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, Depends, File, Form, HTTPException, Response, UploadFile
from openpyxl import Workbook
from sqlalchemy import delete, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from app.api.schemas import (
    AdminEmployeeRow,
    AdminEmployeeUpdateRequest,
    AdminEmployeeUpdateResponse,
    AdminEventQueryResponse,
    AdminEventQueryEmailRequest,
    AdminOverviewResponse,
    AdminPinChangeRequest,
    AdminPinChangeResponse,
    AdminPinVerifyRequest,
    AdminPinVerifyResponse,
    AdminQueryDay,
    AdminQueryEvent,
    AdminQuerySummary,
    AdminRecentEvent,
    AdminResetRequest,
    AdminResetResponse,
    EmailCsvRequest,
    EmployeeBulkImportResponse,
    EmployeeCreate,
    EmployeeOut,
    EventCreate,
    EventOut,
    FaceRegisterRequest,
    FaceRegisterResponse,
    IdentifyRequest,
    IdentifyResponse,
    OfflineFaceCacheEmployee,
    OfflineFaceCacheResponse,
    AdminPayrollAutoEmailConfigRequest,
    AdminPayrollAutoEmailConfigResponse,
    AdminPayrollAutoEmailStatusResponse,
)
from app.core.config import settings
from app.core.tz import EASTERN_TZ, eastern_date_range_to_utc_naive, eastern_today_utc_naive_range
from app.db.models import AuditLog, Employee, EventMethod, EventType, FaceTemplate, TimeEvent, TimeSegment
from app.db.session import get_db
from app.services.aggregation import summarize_employee_events
from app.services.csv_export import build_pay_period_adp_attachment, build_pay_period_csv_bytes, build_pay_period_xlsx
from app.services.employee_bulk import export_employees_xlsx, import_employees_xlsx
from app.services.face_matching import best_match, parse_embedding, euclidean_distance, top2_matches
from app.services.mailer import MailerError, send_email_with_attachments
from app.services.admin_auth import change_admin_pin as _change_admin_pin
from app.services.admin_auth import verify_admin_pin as _verify_admin_pin
from app.services.state_machine import StateError, allowed_events_for_status, apply_event, infer_state

router = APIRouter(prefix="/api")

# Hardcoded reset password (plaintext not stored). PBKDF2-HMAC-SHA256 with a fixed salt.
_RESET_SALT = bytes.fromhex("0b3c8c3f99f0e1df7e6a9dfb6b9d9a1a")
_RESET_DK_HEX = "bb88922917ab77586ebdfe4708ecf7ec4a6fdfe8cc63d059212a1cc3a0cec514"
_RESET_ITERATIONS = 210_000
_RETENTION_DAYS = 365

_K_PAYROLL_AUTO_ENABLED = "payroll_auto_email_enabled"
_K_PAYROLL_AUTO_TO = "payroll_auto_email_to"
_K_PAYROLL_AUTO_LAST_END = "payroll_auto_email_last_end"
_K_PAYROLL_AUTO_LAST_ERROR = "payroll_auto_email_last_error"
_K_PAYROLL_AUTO_CATCHUP_ONCE = "payroll_auto_email_catchup_once"
_K_PAYROLL_AUTO_LAST_KEY = "payroll_auto_email_last_key"
_K_PAYROLL_AUTO_VERSION = "payroll_auto_email_schedule_version"
_K_PAYROLL_AUTO_CYCLE_WEEKS = "payroll_auto_email_cycle_weeks"
_K_PAYROLL_AUTO_ANCHOR_WEEKDAY = "payroll_auto_email_anchor_weekday"
_K_PAYROLL_AUTO_ANCHOR_START = "payroll_auto_email_anchor_start_date"
_K_PAYROLL_AUTO_PENDING_CYCLE_WEEKS = "payroll_auto_email_pending_cycle_weeks"
_K_PAYROLL_AUTO_PENDING_ANCHOR_WEEKDAY = "payroll_auto_email_pending_anchor_weekday"
_K_PAYROLL_AUTO_PENDING_ANCHOR_START = "payroll_auto_email_pending_anchor_start_date"
_K_PAYROLL_AUTO_PENDING_EFFECTIVE_START = "payroll_auto_email_pending_effective_start"
_PAYROLL_AUTO_VERSION_V2 = "v2"
_PAYROLL_AUTO_SCHEDULE = "WEEKLY_OR_BIWEEKLY_ANCHORED_ET_0130"
_OFFLINE_FACE_THRESHOLD = 0.53


def _cfg_get(db: Session, key: str) -> Optional[str]:
    return db.execute(text("SELECT value FROM app_config WHERE key=:k"), {"k": key}).scalar_one_or_none()


def _cfg_set(db: Session, key: str, value: str) -> None:
    db.execute(
        text(
            "INSERT INTO app_config(key, value) VALUES(:k, :v) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
        ),
        {"k": key, "v": value},
    )


def _cfg_set_date(db: Session, key: str, value: Optional[date]) -> None:
    _cfg_set(db, key, value.isoformat() if value else "")


def _cfg_get_date(db: Session, key: str) -> Optional[date]:
    raw = (_cfg_get(db, key) or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw).date()
    except Exception:
        return None


def _cfg_get_int(db: Session, key: str, default: int, *, min_value: int, max_value: int) -> int:
    raw = (_cfg_get(db, key) or "").strip()
    try:
        value = int(raw)
    except Exception:
        value = default
    if value < min_value or value > max_value:
        return default
    return value


def _default_anchor_start(today_et: date, anchor_weekday: int) -> date:
    # 0=Mon ... 6=Sun
    delta = (today_et.weekday() - anchor_weekday) % 7
    return today_et - timedelta(days=delta)


def _period_days(cycle_weeks: int) -> int:
    return 7 * (1 if cycle_weeks == 1 else 2)


def _next_period_start_date(
    *,
    now_et: date,
    last_end: Optional[date],
    anchor_start: Optional[date],
    anchor_weekday: int,
) -> date:
    if last_end is not None:
        return last_end + timedelta(days=1)
    if anchor_start is not None:
        return anchor_start
    return _default_anchor_start(now_et, anchor_weekday)


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}


@router.post("/admin/verify-pin", response_model=AdminPinVerifyResponse)
def verify_admin_pin(payload: AdminPinVerifyRequest, db: Session = Depends(get_db)) -> AdminPinVerifyResponse:
    if not _verify_admin_pin(db, (payload.admin_pin or "").strip(), verify_master=_verify_reset_password):
        raise HTTPException(status_code=403, detail="invalid admin PIN")
    return AdminPinVerifyResponse(ok=True)


def _verify_reset_password(candidate: str) -> bool:
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        candidate.encode("utf-8"),
        _RESET_SALT,
        _RESET_ITERATIONS,
    ).hex()
    return hmac.compare_digest(dk, _RESET_DK_HEX)


def require_admin(db: Session, admin_pin: Optional[str]) -> None:
    if not _verify_admin_pin(db, (admin_pin or "").strip(), verify_master=_verify_reset_password):
        raise HTTPException(status_code=403, detail="invalid admin PIN")


_EMPLOYEE_CODE_PATTERN = re.compile(r"^([A-Z])(\d{4})$")


def _next_prefix_letter(prefix: str) -> Optional[str]:
    if not prefix or len(prefix) != 1 or not prefix.isalpha():
        return "E"
    c = ord(prefix.upper())
    if c >= ord("Z"):
        return None
    return chr(c + 1)


def _allocate_next_employee_code(db: Session) -> str:
    max_by_prefix = {}
    rows = db.execute(select(Employee.employee_code)).scalars().all()
    for raw in rows:
        code = (raw or "").strip().upper()
        m = _EMPLOYEE_CODE_PATTERN.match(code)
        if not m:
            continue
        prefix = m.group(1)
        number = int(m.group(2))
        prev = max_by_prefix.get(prefix, 0)
        if number > prev:
            max_by_prefix[prefix] = number

    prefix = "E"
    while prefix is not None:
        cur = max_by_prefix.get(prefix, 0)
        if cur < 9999:
            return f"{prefix}{cur + 1:04d}"
        prefix = _next_prefix_letter(prefix)

    raise HTTPException(status_code=409, detail="employee_code sequence exhausted")


@router.post("/admin/reset", response_model=AdminResetResponse)
def admin_reset(payload: AdminResetRequest, db: Session = Depends(get_db)) -> AdminResetResponse:
    require_admin(db, payload.admin_pin)
    if not _verify_reset_password(payload.reset_password):
        raise HTTPException(status_code=403, detail="invalid reset password")

    # Delete in a safe order; keep it simple (MVP SQLite).
    def _count(table: str) -> int:
        return int(db.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one())

    def _delete(table: str) -> None:
        db.execute(text(f"DELETE FROM {table}"))

    deleted_audit_logs = _count("audit_logs")
    deleted_time_segments = _count("time_segments")
    deleted_time_events = _count("time_events")
    deleted_face_templates = _count("face_templates")
    deleted_employees = _count("employees")

    _delete("audit_logs")
    _delete("time_segments")
    _delete("time_events")
    _delete("face_templates")
    _delete("employees")
    db.commit()

    return AdminResetResponse(
        ok=True,
        deleted_employees=deleted_employees,
        deleted_face_templates=deleted_face_templates,
        deleted_time_events=deleted_time_events,
        deleted_time_segments=deleted_time_segments,
        deleted_audit_logs=deleted_audit_logs,
    )


@router.post("/admin/pin/change", response_model=AdminPinChangeResponse)
def admin_pin_change(payload: AdminPinChangeRequest, db: Session = Depends(get_db)) -> AdminPinChangeResponse:
    # Master password (reset password) also works here because require_admin accepts it.
    require_admin(db, payload.admin_pin)
    new_pin = (payload.new_admin_pin or "").strip()
    if len(new_pin) < 4:
        raise HTTPException(status_code=400, detail="new_admin_pin must be at least 4 chars")
    _change_admin_pin(db, actor="admin", new_pin=new_pin)
    return AdminPinChangeResponse(ok=True)


@router.get("/admin/payroll-auto-email/status", response_model=AdminPayrollAutoEmailStatusResponse)
def payroll_auto_email_status(admin_pin: str, db: Session = Depends(get_db)) -> AdminPayrollAutoEmailStatusResponse:
    require_admin(db, admin_pin)
    enabled = (_cfg_get(db, _K_PAYROLL_AUTO_ENABLED) or "").lower() == "true"
    to_email = (_cfg_get(db, _K_PAYROLL_AUTO_TO) or "").strip() or None
    catchup_once = (_cfg_get(db, _K_PAYROLL_AUTO_CATCHUP_ONCE) or "true").lower() == "true"
    last_end = _cfg_get_date(db, _K_PAYROLL_AUTO_LAST_END)
    last_err = (_cfg_get(db, _K_PAYROLL_AUTO_LAST_ERROR) or "").strip() or None

    cycle_weeks = _cfg_get_int(db, _K_PAYROLL_AUTO_CYCLE_WEEKS, 2, min_value=1, max_value=2)
    anchor_weekday = _cfg_get_int(db, _K_PAYROLL_AUTO_ANCHOR_WEEKDAY, 0, min_value=0, max_value=6)
    anchor_start = _cfg_get_date(db, _K_PAYROLL_AUTO_ANCHOR_START)

    pending_cycle = _cfg_get_int(db, _K_PAYROLL_AUTO_PENDING_CYCLE_WEEKS, 0, min_value=0, max_value=2)
    pending_cycle_weeks = pending_cycle if pending_cycle in (1, 2) else None
    pending_anchor_weekday_raw = (_cfg_get(db, _K_PAYROLL_AUTO_PENDING_ANCHOR_WEEKDAY) or "").strip()
    pending_anchor_weekday: Optional[int] = None
    if pending_anchor_weekday_raw:
        try:
            candidate = int(pending_anchor_weekday_raw)
            if 0 <= candidate <= 6:
                pending_anchor_weekday = candidate
        except Exception:
            pending_anchor_weekday = None
    pending_anchor_start = _cfg_get_date(db, _K_PAYROLL_AUTO_PENDING_ANCHOR_START)
    pending_effective_start = _cfg_get_date(db, _K_PAYROLL_AUTO_PENDING_EFFECTIVE_START)

    now_et = datetime.now(timezone.utc).astimezone(EASTERN_TZ).date()
    if anchor_start is None:
        if last_end is not None:
            anchor_start = last_end + timedelta(days=1)
        else:
            anchor_start = _default_anchor_start(now_et, anchor_weekday)
    effective_cycle = cycle_weeks
    effective_weekday = anchor_weekday
    effective_anchor_start = anchor_start
    next_start = _next_period_start_date(
        now_et=now_et,
        last_end=last_end,
        anchor_start=effective_anchor_start,
        anchor_weekday=effective_weekday,
    )
    if (
        pending_cycle_weeks in (1, 2)
        and pending_anchor_weekday is not None
        and pending_effective_start is not None
        and next_start >= pending_effective_start
    ):
        effective_cycle = pending_cycle_weeks
        effective_weekday = pending_anchor_weekday
        effective_anchor_start = pending_anchor_start
        next_start = _next_period_start_date(
            now_et=now_et,
            last_end=last_end,
            anchor_start=effective_anchor_start,
            anchor_weekday=effective_weekday,
        )

    next_end = next_start + timedelta(days=_period_days(effective_cycle) - 1)
    send_day = next_end + timedelta(days=1)
    next_send_at_et = datetime(send_day.year, send_day.month, send_day.day, 1, 30, tzinfo=EASTERN_TZ)

    return AdminPayrollAutoEmailStatusResponse(
        enabled=enabled,
        to_email=to_email,
        schedule=_PAYROLL_AUTO_SCHEDULE,
        cycle_weeks=cycle_weeks,
        anchor_weekday=anchor_weekday,
        anchor_start_date=anchor_start,
        catchup_once=catchup_once,
        pending_cycle_weeks=pending_cycle_weeks,
        pending_anchor_weekday=pending_anchor_weekday,
        pending_anchor_start_date=pending_anchor_start,
        pending_effective_start=pending_effective_start,
        next_start_date=next_start,
        next_end_date=next_end,
        next_send_at_et=next_send_at_et,
        last_sent_end=last_end,
        last_error=last_err,
    )


@router.post("/admin/payroll-auto-email/config", response_model=AdminPayrollAutoEmailConfigResponse)
def payroll_auto_email_config(
    payload: AdminPayrollAutoEmailConfigRequest, db: Session = Depends(get_db)
) -> AdminPayrollAutoEmailConfigResponse:
    require_admin(db, payload.admin_pin)
    _cfg_set(db, _K_PAYROLL_AUTO_ENABLED, "true" if payload.enabled else "false")
    if payload.to_email:
        _cfg_set(db, _K_PAYROLL_AUTO_TO, str(payload.to_email))
    else:
        _cfg_set(db, _K_PAYROLL_AUTO_TO, "")
    _cfg_set(db, _K_PAYROLL_AUTO_CATCHUP_ONCE, "true" if payload.catchup_once else "false")

    now_et = datetime.now(timezone.utc).astimezone(EASTERN_TZ).date()
    last_end = _cfg_get_date(db, _K_PAYROLL_AUTO_LAST_END)
    version = (_cfg_get(db, _K_PAYROLL_AUTO_VERSION) or "").strip()
    legacy_mode = version != _PAYROLL_AUTO_VERSION_V2

    current_cycle = _cfg_get_int(db, _K_PAYROLL_AUTO_CYCLE_WEEKS, 2, min_value=1, max_value=2)
    current_weekday = _cfg_get_int(db, _K_PAYROLL_AUTO_ANCHOR_WEEKDAY, 0, min_value=0, max_value=6)
    current_anchor = _cfg_get_date(db, _K_PAYROLL_AUTO_ANCHOR_START)
    if current_anchor is None and last_end is not None:
        current_anchor = last_end + timedelta(days=1)
    if current_anchor is None:
        current_anchor = _default_anchor_start(now_et, current_weekday)

    requested_cycle = 1 if payload.cycle_weeks == 1 else 2
    requested_weekday = int(payload.anchor_weekday)
    if requested_weekday < 0 or requested_weekday > 6:
        requested_weekday = 0
    requested_anchor = payload.anchor_start_date
    if requested_anchor is None:
        if last_end is not None:
            requested_anchor = last_end + timedelta(days=1)
        else:
            requested_anchor = _default_anchor_start(now_et, requested_weekday)

    schedule_changed = (
        requested_cycle != current_cycle
        or requested_weekday != current_weekday
        or requested_anchor != current_anchor
    )

    pending_cycle_weeks = _cfg_get_int(db, _K_PAYROLL_AUTO_PENDING_CYCLE_WEEKS, 0, min_value=0, max_value=2)
    pending_exists = pending_cycle_weeks in (1, 2)

    if schedule_changed:
        if legacy_mode or last_end is None:
            # First migration from semi-monthly OR no prior send history: apply immediately.
            _cfg_set(db, _K_PAYROLL_AUTO_CYCLE_WEEKS, str(requested_cycle))
            _cfg_set(db, _K_PAYROLL_AUTO_ANCHOR_WEEKDAY, str(requested_weekday))
            _cfg_set_date(db, _K_PAYROLL_AUTO_ANCHOR_START, requested_anchor)
            _cfg_set(db, _K_PAYROLL_AUTO_PENDING_CYCLE_WEEKS, "")
            _cfg_set(db, _K_PAYROLL_AUTO_PENDING_ANCHOR_WEEKDAY, "")
            _cfg_set(db, _K_PAYROLL_AUTO_PENDING_ANCHOR_START, "")
            _cfg_set(db, _K_PAYROLL_AUTO_PENDING_EFFECTIVE_START, "")
        else:
            # Existing cycle is in-flight: defer new cadence to the next cycle boundary.
            next_start_current = last_end + timedelta(days=1)
            effective_start = next_start_current + timedelta(days=_period_days(current_cycle))
            _cfg_set(db, _K_PAYROLL_AUTO_PENDING_CYCLE_WEEKS, str(requested_cycle))
            _cfg_set(db, _K_PAYROLL_AUTO_PENDING_ANCHOR_WEEKDAY, str(requested_weekday))
            _cfg_set_date(db, _K_PAYROLL_AUTO_PENDING_ANCHOR_START, requested_anchor)
            _cfg_set_date(db, _K_PAYROLL_AUTO_PENDING_EFFECTIVE_START, effective_start)
    elif pending_exists:
        # Same as current active config: treat as cancel pending update.
        _cfg_set(db, _K_PAYROLL_AUTO_PENDING_CYCLE_WEEKS, "")
        _cfg_set(db, _K_PAYROLL_AUTO_PENDING_ANCHOR_WEEKDAY, "")
        _cfg_set(db, _K_PAYROLL_AUTO_PENDING_ANCHOR_START, "")
        _cfg_set(db, _K_PAYROLL_AUTO_PENDING_EFFECTIVE_START, "")

    if not pending_exists and not schedule_changed:
        _cfg_set(db, _K_PAYROLL_AUTO_CYCLE_WEEKS, str(current_cycle))
        _cfg_set(db, _K_PAYROLL_AUTO_ANCHOR_WEEKDAY, str(current_weekday))
        _cfg_set_date(db, _K_PAYROLL_AUTO_ANCHOR_START, current_anchor)

    _cfg_set(db, _K_PAYROLL_AUTO_VERSION, _PAYROLL_AUTO_VERSION_V2)
    # Keep last_end; clear last_error on config change.
    _cfg_set(db, _K_PAYROLL_AUTO_LAST_ERROR, "")
    db.commit()
    return AdminPayrollAutoEmailConfigResponse(ok=True)

@router.get("/employees", response_model=list[EmployeeOut])
def list_employees(db: Session = Depends(get_db)) -> list[Employee]:
    return db.execute(select(Employee).order_by(Employee.employee_code)).scalars().all()


@router.post("/employees", response_model=EmployeeOut)
def create_employee(payload: EmployeeCreate, db: Session = Depends(get_db)) -> Employee:
    require_admin(db, payload.admin_pin)

    code = _allocate_next_employee_code(db)

    if payload.termination_date is not None and payload.termination_date < payload.hire_date:
        raise HTTPException(status_code=400, detail="termination_date must be >= hire_date")

    employee = Employee(
        employee_code=code,
        name=payload.name,
        hire_date=payload.hire_date,
        termination_date=payload.termination_date,
        title=payload.title,
        work_group=payload.work_group,
        is_active=False if payload.termination_date is not None else True,
    )
    db.add(employee)
    db.flush()
    db.add(
        AuditLog(
            who="admin",
            action="CREATE_EMPLOYEE",
            target_type="employee",
            target_id=employee.id,
            before_json=None,
            after_json=json.dumps(
                {
                    "employee_code": employee.employee_code,
                    "name": employee.name,
                    "hire_date": employee.hire_date.isoformat() if employee.hire_date else None,
                    "termination_date": employee.termination_date.isoformat() if employee.termination_date else None,
                }
            ),
            reason="admin create",
        )
    )
    db.commit()
    db.refresh(employee)
    return employee


@router.post("/employees/{employee_id}/deactivate")
def deactivate_employee(employee_id: str, admin_pin: str, db: Session = Depends(get_db)) -> dict[str, bool]:
    require_admin(db, admin_pin)

    employee = db.get(Employee, employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail="employee not found")

    before = {"is_active": employee.is_active}
    employee.is_active = False
    db.add(
        AuditLog(
            who="admin",
            action="DEACTIVATE_EMPLOYEE",
            target_type="employee",
            target_id=employee.id,
            before_json=json.dumps(before),
            after_json=json.dumps({"is_active": False}),
            reason="manual deactivation",
        )
    )
    db.commit()
    return {"ok": True}

@router.put("/admin/employees/{employee_id}", response_model=AdminEmployeeUpdateResponse)
def admin_update_employee(
    employee_id: str, payload: AdminEmployeeUpdateRequest, db: Session = Depends(get_db)
) -> AdminEmployeeUpdateResponse:
    require_admin(db, payload.admin_pin)

    employee = db.get(Employee, employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail="employee not found")

    if payload.termination_date is not None and payload.termination_date < payload.hire_date:
        raise HTTPException(status_code=400, detail="termination_date must be >= hire_date")

    before = {
        "name": employee.name,
        "hire_date": employee.hire_date.isoformat() if getattr(employee, "hire_date", None) else None,
        "termination_date": employee.termination_date.isoformat() if getattr(employee, "termination_date", None) else None,
        "title": employee.title,
        "work_group": getattr(employee, "work_group", None),
        "is_active": employee.is_active,
    }

    employee.name = payload.name
    employee.hire_date = payload.hire_date
    employee.termination_date = payload.termination_date
    employee.title = payload.title
    employee.work_group = payload.work_group
    # Termination date means terminated (not allowed to clock in/out anymore).
    employee.is_active = False if payload.termination_date is not None else payload.is_active

    db.add(
        AuditLog(
            who="admin",
            action="UPDATE_EMPLOYEE",
            target_type="employee",
            target_id=employee.id,
            before_json=json.dumps(before),
            after_json=json.dumps(
                {
                    "name": employee.name,
                    "hire_date": employee.hire_date.isoformat() if getattr(employee, "hire_date", None) else None,
                    "termination_date": employee.termination_date.isoformat() if getattr(employee, "termination_date", None) else None,
                    "title": employee.title,
                    "work_group": employee.work_group,
                    "is_active": employee.is_active,
                }
            ),
            reason="admin edit",
        )
    )
    db.commit()
    return AdminEmployeeUpdateResponse(ok=True)


@router.get("/admin/employees", response_model=list[AdminEmployeeRow])
def admin_employees(admin_pin: str, db: Session = Depends(get_db)) -> list[AdminEmployeeRow]:
    require_admin(db, admin_pin)

    employees = db.execute(select(Employee).order_by(Employee.employee_code)).scalars().all()
    counts = dict(
        db.execute(select(FaceTemplate.employee_id, func.count(FaceTemplate.id)).group_by(FaceTemplate.employee_id)).all()
    )

    rows = []
    for emp in employees:
        rows.append(
            AdminEmployeeRow(
                id=emp.id,
                employee_code=emp.employee_code,
                name=emp.name,
                hire_date=emp.hire_date,
                termination_date=emp.termination_date,
                title=emp.title or "STAFF",
                work_group=emp.work_group or "FRONT",
                is_active=emp.is_active,
                face_template_count=int(counts.get(emp.id, 0)),
            )
        )
    return rows


@router.get("/admin/employees/export.xlsx")
def admin_employees_export_xlsx(admin_pin: str, db: Session = Depends(get_db)) -> Response:
    require_admin(db, admin_pin)
    file_bytes = export_employees_xlsx(db)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"employees_bulk_{ts}.xlsx"
    return Response(
        content=file_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/admin/employees/import", response_model=EmployeeBulkImportResponse)
async def admin_employees_import_xlsx(
    admin_pin: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
) -> EmployeeBulkImportResponse:
    require_admin(db, admin_pin)

    filename = (file.filename or "").lower()
    if not filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    try:
        result = import_employees_xlsx(db, content, actor="admin")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to import Excel: {exc}") from exc

    return EmployeeBulkImportResponse(
        created=result.created,
        updated=result.updated,
        skipped=result.skipped,
        errors=result.errors,
    )


@router.get("/admin/overview", response_model=AdminOverviewResponse)
def admin_overview(admin_pin: str, db: Session = Depends(get_db)) -> AdminOverviewResponse:
    require_admin(db, admin_pin)

    active_employees = db.execute(select(func.count(Employee.id)).where(Employee.is_active.is_(True))).scalar_one()

    registered_face_employees = db.execute(
        select(func.count(func.distinct(FaceTemplate.employee_id)))
        .join(Employee, FaceTemplate.employee_id == Employee.id)
        .where(Employee.is_active.is_(True))
    ).scalar_one()

    total_face_templates = db.execute(select(func.count(FaceTemplate.id))).scalar_one()

    today_start, today_end = eastern_today_utc_naive_range()

    today_events = db.execute(
        select(func.count(TimeEvent.id)).where(TimeEvent.ts_utc >= today_start, TimeEvent.ts_utc < today_end)
    ).scalar_one()

    recent_rows = db.execute(
        select(TimeEvent, Employee)
        .join(Employee, TimeEvent.employee_id == Employee.id)
        .order_by(TimeEvent.ts_utc.desc())
        .limit(20)
    ).all()

    recent_events = [
        AdminRecentEvent(
            employee_code=emp.employee_code,
            employee_name=emp.name,
            event_type=ev.event_type.value,
            method=ev.method.value,
            ts_utc=ev.ts_utc,
        )
        for ev, emp in recent_rows
    ]

    return AdminOverviewResponse(
        active_employees=int(active_employees),
        registered_face_employees=int(registered_face_employees),
        total_face_templates=int(total_face_templates),
        today_events=int(today_events),
        recent_events=recent_events,
    )


_EVENT_SCOPE_ALL = "ALL"
_EVENT_SCOPE_CODE = "EMPLOYEE_CODE"
_EVENT_SCOPE_NAME = "EMPLOYEE_NAME"
_EVENT_FILTER_ALL = "ALL"
_EVENT_FILTER_CLOCK = "CLOCK"
_EVENT_FILTER_BREAK = "BREAK"
_EVENT_PAGE_SIZE_DEFAULT = 30
_EVENT_PAGE_SIZE_MAX = 500


def _parse_query_input_date(raw: str, field_name: str) -> date:
    value = (raw or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise HTTPException(status_code=400, detail=f"Invalid {field_name}. Use YYYY-MM-DD or MM/DD/YYYY")


def _normalize_event_scope(scope: str) -> str:
    value = (scope or "").strip().upper()
    if value in {_EVENT_SCOPE_ALL, _EVENT_SCOPE_CODE, _EVENT_SCOPE_NAME}:
        return value
    return _EVENT_SCOPE_ALL


def _normalize_event_filter(event_filter: str) -> str:
    value = (event_filter or "").strip().upper()
    if value in {_EVENT_FILTER_ALL, _EVENT_FILTER_CLOCK, _EVENT_FILTER_BREAK}:
        return value
    return _EVENT_FILTER_ALL


def _normalize_page_size(page_size: int) -> int:
    if page_size <= 0:
        return _EVENT_PAGE_SIZE_DEFAULT
    return min(page_size, _EVENT_PAGE_SIZE_MAX)


def _normalize_page(page: int) -> int:
    return page if page > 0 else 1


def _event_types_for_filter(event_filter: str) -> Optional[tuple[EventType, ...]]:
    if event_filter == _EVENT_FILTER_CLOCK:
        return (EventType.CLOCK_IN, EventType.CLOCK_OUT)
    if event_filter == _EVENT_FILTER_BREAK:
        return (EventType.BREAK_START, EventType.BREAK_END)
    return None


def _ts_utc_to_et_text(ts_utc: datetime) -> str:
    if ts_utc.tzinfo is None:
        aware_utc = ts_utc.replace(tzinfo=timezone.utc)
    else:
        aware_utc = ts_utc.astimezone(timezone.utc)
    return aware_utc.astimezone(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _minutes_to_hours_decimal(minutes: int) -> float:
    return round(max(int(minutes or 0), 0) / 60.0, 2)


def _event_window_text_et(start_date: date, end_date: date) -> str:
    return f"{start_date.isoformat()} 00:00:00 ET ~ {end_date.isoformat()} 23:59:59 ET"


def _run_admin_event_query(
    db: Session,
    *,
    start_date: date,
    end_date: date,
    q: str,
    scope: str,
    event_filter: str,
    page: int,
    page_size: int,
    include_all_events_for_export: bool = False,
) -> tuple[AdminEventQueryResponse, list[AdminQueryEvent]]:
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")

    q_clean = (q or "").strip()
    scope_norm = _normalize_event_scope(scope)
    event_filter_norm = _normalize_event_filter(event_filter)
    page_norm = _normalize_page(page)
    page_size_norm = _normalize_page_size(page_size)

    start_dt, end_dt = eastern_date_range_to_utc_naive(start_date, end_date)
    where_clauses = [TimeEvent.ts_utc >= start_dt, TimeEvent.ts_utc < end_dt]

    if scope_norm == _EVENT_SCOPE_CODE:
        if not q_clean:
            raise HTTPException(status_code=400, detail="q is required when scope=EMPLOYEE_CODE")
        ql = q_clean.lower()
        where_clauses.append(
            or_(
                func.lower(Employee.employee_code) == ql,
                func.lower(Employee.employee_code).like(f"%{ql}%"),
            )
        )
    elif scope_norm == _EVENT_SCOPE_NAME:
        if not q_clean:
            raise HTTPException(status_code=400, detail="q is required when scope=EMPLOYEE_NAME")
        ql = q_clean.lower()
        where_clauses.append(func.lower(Employee.name).like(f"%{ql}%"))

    event_types = _event_types_for_filter(event_filter_norm)
    if event_types is not None:
        where_clauses.append(TimeEvent.event_type.in_(event_types))

    total_events = int(
        db.execute(
            select(func.count(TimeEvent.id))
            .select_from(TimeEvent)
            .join(Employee, TimeEvent.employee_id == Employee.id)
            .where(*where_clauses)
        ).scalar_one()
        or 0
    )

    matched_employees = int(
        db.execute(
            select(func.count(func.distinct(Employee.id)))
            .select_from(TimeEvent)
            .join(Employee, TimeEvent.employee_id == Employee.id)
            .where(*where_clauses)
        ).scalar_one()
        or 0
    )

    total_pages = int(ceil(total_events / page_size_norm)) if total_events > 0 else 0
    if total_pages > 0:
        page_norm = max(1, min(page_norm, total_pages))
    else:
        page_norm = 1

    events: list[AdminQueryEvent] = []
    if total_events > 0:
        offset = (page_norm - 1) * page_size_norm
        event_rows = (
            db.execute(
                select(TimeEvent, Employee)
                .join(Employee, TimeEvent.employee_id == Employee.id)
                .where(*where_clauses)
                .order_by(TimeEvent.ts_utc.desc())
                .offset(offset)
                .limit(page_size_norm)
            )
            .all()
        )
        for ev, emp in event_rows:
            events.append(
                AdminQueryEvent(
                    ts_utc=ev.ts_utc,
                    ts_et=_ts_utc_to_et_text(ev.ts_utc),
                    employee_code=emp.employee_code,
                    employee_name=emp.name,
                    title=emp.title or "STAFF",
                    event_type=ev.event_type.value,
                    method=ev.method.value,
                )
            )

    summaries: list[AdminQuerySummary] = []
    if matched_employees > 0:
        employee_rows = db.execute(
            select(Employee.id, Employee.employee_code, Employee.name, Employee.title)
            .join(TimeEvent, TimeEvent.employee_id == Employee.id)
            .where(*where_clauses)
            .group_by(Employee.id, Employee.employee_code, Employee.name, Employee.title)
            .order_by(func.lower(Employee.name), Employee.employee_code)
        ).all()

        employee_ids = [str(row[0]) for row in employee_rows]
        employee_info = {
            str(row[0]): {
                "employee_code": str(row[1]),
                "employee_name": str(row[2]),
                "title": str(row[3] or "STAFF"),
            }
            for row in employee_rows
        }

        summary_rows = (
            db.execute(
                select(TimeEvent, Employee)
                .join(Employee, TimeEvent.employee_id == Employee.id)
                .where(
                    TimeEvent.employee_id.in_(employee_ids),
                    TimeEvent.ts_utc >= start_dt,
                    TimeEvent.ts_utc < end_dt,
                )
                .order_by(TimeEvent.employee_id.asc(), TimeEvent.ts_utc.asc())
            )
            .all()
        )

        events_by_emp: dict[str, list[TimeEvent]] = {emp_id: [] for emp_id in employee_ids}
        for ev, emp in summary_rows:
            events_by_emp.setdefault(emp.id, []).append(ev)

        for emp_id in employee_ids:
            info = employee_info.get(emp_id)
            if not info:
                continue
            day_summaries = summarize_employee_events(emp_id, events_by_emp.get(emp_id, []))
            days: list[AdminQueryDay] = []
            total_work = 0
            total_break = 0
            for s in day_summaries:
                if s.work_date < start_date or s.work_date > end_date:
                    continue
                total_work += int(s.total_work_minutes or 0)
                total_break += int(s.break_minutes or 0)
                days.append(
                    AdminQueryDay(
                        work_date=s.work_date,
                        work_minutes=int(s.total_work_minutes or 0),
                        break_minutes=int(s.break_minutes or 0),
                        flags=s.flags,
                    )
                )
            days.sort(key=lambda d: d.work_date)
            summaries.append(
                AdminQuerySummary(
                    employee_code=info["employee_code"],
                    employee_name=info["employee_name"],
                    title=info["title"],
                    total_work_minutes=total_work,
                    total_break_minutes=total_break,
                    days=days,
                )
            )

    summaries.sort(key=lambda s: (s.employee_name.lower(), s.employee_code))

    all_events_for_export: list[AdminQueryEvent] = []
    if include_all_events_for_export and total_events > 0:
        export_rows = (
            db.execute(
                select(TimeEvent, Employee)
                .join(Employee, TimeEvent.employee_id == Employee.id)
                .where(*where_clauses)
                .order_by(TimeEvent.ts_utc.asc())
            )
            .all()
        )
        for ev, emp in export_rows:
            all_events_for_export.append(
                AdminQueryEvent(
                    ts_utc=ev.ts_utc,
                    ts_et=_ts_utc_to_et_text(ev.ts_utc),
                    employee_code=emp.employee_code,
                    employee_name=emp.name,
                    title=emp.title or "STAFF",
                    event_type=ev.event_type.value,
                    method=ev.method.value,
                )
            )

    response = AdminEventQueryResponse(
        start_date=start_date,
        end_date=end_date,
        scope=scope_norm,
        event_filter=event_filter_norm,
        q=q_clean,
        page=page_norm,
        page_size=page_size_norm,
        total_events=total_events,
        total_pages=total_pages,
        matched_employees=matched_employees,
        summaries=summaries,
        events=events,
    )
    return response, all_events_for_export


@router.get("/admin/events/query", response_model=AdminEventQueryResponse)
def admin_events_query(
    admin_pin: str,
    start_date: str,
    end_date: str,
    q: str = "",
    scope: str = _EVENT_SCOPE_ALL,
    event_filter: str = _EVENT_FILTER_ALL,
    page: int = 1,
    page_size: int = _EVENT_PAGE_SIZE_DEFAULT,
    db: Session = Depends(get_db),
) -> AdminEventQueryResponse:
    require_admin(db, admin_pin)
    start_d = _parse_query_input_date(start_date, "start_date")
    end_d = _parse_query_input_date(end_date, "end_date")
    response, _ = _run_admin_event_query(
        db,
        start_date=start_d,
        end_date=end_d,
        q=q,
        scope=scope,
        event_filter=event_filter,
        page=page,
        page_size=page_size,
        include_all_events_for_export=False,
    )
    try:
        db.add(
            AuditLog(
                who="admin",
                action="ADMIN_EVENT_QUERY",
                target_type="report",
                target_id=end_d.isoformat(),
                before_json=None,
                after_json=json.dumps(
                    {
                        "start_date": response.start_date.isoformat(),
                        "end_date": response.end_date.isoformat(),
                        "scope": response.scope,
                        "event_filter": response.event_filter,
                        "q": response.q,
                        "page": response.page,
                        "page_size": response.page_size,
                        "matched_employees": response.matched_employees,
                        "total_events": response.total_events,
                    }
                ),
                reason=(
                    f"{response.scope}/{response.event_filter} "
                    f"{_event_window_text_et(response.start_date, response.end_date)} "
                    f"events={response.total_events}"
                )[:255],
            )
        )
        db.commit()
    except Exception:
        db.rollback()
    return response


@router.post("/admin/events/query/email")
def admin_events_query_email(payload: AdminEventQueryEmailRequest, db: Session = Depends(get_db)) -> dict[str, object]:
    require_admin(db, payload.admin_pin)
    start_d = _parse_query_input_date(payload.start_date, "start_date")
    end_d = _parse_query_input_date(payload.end_date, "end_date")
    response, export_events = _run_admin_event_query(
        db,
        start_date=start_d,
        end_date=end_d,
        q=payload.q,
        scope=payload.scope,
        event_filter=payload.event_filter,
        page=1,
        page_size=_EVENT_PAGE_SIZE_MAX,
        include_all_events_for_export=True,
    )
    if response.total_events <= 0 or not export_events:
        raise HTTPException(status_code=400, detail="No events to email for selected filter")

    to_email = (_cfg_get(db, _K_PAYROLL_AUTO_TO) or "").strip()
    if not to_email:
        raise HTTPException(status_code=400, detail="Payroll default email is not configured")

    csv_output = io.StringIO()
    csv_writer = csv.DictWriter(
        csv_output,
        fieldnames=["TimestampET", "EmployeeCode", "EmployeeName", "Title", "EventType", "Method"],
        delimiter=",",
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\r\n",
    )
    csv_writer.writeheader()
    for row in export_events:
        csv_writer.writerow(
            {
                "TimestampET": row.ts_et,
                "EmployeeCode": row.employee_code,
                "EmployeeName": row.employee_name,
                "Title": row.title,
                "EventType": row.event_type,
                "Method": row.method,
            }
        )
    csv_content = csv_output.getvalue()

    wb = Workbook()
    ws_summary = wb.active
    ws_summary.title = "Summary"
    ws_summary.append(["Period Start", response.start_date.isoformat()])
    ws_summary.append(["Period End", response.end_date.isoformat()])
    ws_summary.append(["Scope", response.scope])
    ws_summary.append(["Event Filter", response.event_filter])
    ws_summary.append(["Keyword", response.q or ""])
    ws_summary.append(["Matched Employees", response.matched_employees])
    ws_summary.append(["Total Events", response.total_events])
    ws_summary.append([])
    ws_summary.append(["EmployeeCode", "EmployeeName", "Title", "TotalWorkHours", "TotalBreakHours"])
    total_work = 0
    total_break = 0
    for row in response.summaries:
        total_work += int(row.total_work_minutes or 0)
        total_break += int(row.total_break_minutes or 0)
        ws_summary.append(
            [
                row.employee_code,
                row.employee_name,
                row.title,
                _minutes_to_hours_decimal(row.total_work_minutes),
                _minutes_to_hours_decimal(row.total_break_minutes),
            ]
        )
    ws_summary.append([])
    ws_summary.append(
        [
            "TOTAL",
            "",
            "",
            _minutes_to_hours_decimal(total_work),
            _minutes_to_hours_decimal(total_break),
        ]
    )

    ws_events = wb.create_sheet("Events")
    ws_events.append(["TimestampET", "EmployeeCode", "EmployeeName", "Title", "EventType", "Method"])
    for row in export_events:
        ws_events.append([row.ts_et, row.employee_code, row.employee_name, row.title, row.event_type, row.method])

    for row_no in range(10, ws_summary.max_row + 1):
        ws_summary[f"D{row_no}"].number_format = "0.00"
        ws_summary[f"E{row_no}"].number_format = "0.00"

    xlsx_buf = io.BytesIO()
    wb.save(xlsx_buf)
    xlsx_buf.seek(0)
    xlsx_content = xlsx_buf.read()

    filename_base = f"event_query_{response.start_date}_{response.end_date}"
    sent_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        send_email_with_attachments(
            to_email=to_email,
            subject=f"Event Query Report {response.start_date} ~ {response.end_date} ({sent_at})",
            body=(
                "Attached are event query files (CSV + XLSX).\n"
                f"ET window: {_event_window_text_et(response.start_date, response.end_date)}\n"
                f"Scope: {response.scope}\n"
                f"Event filter: {response.event_filter}\n"
                f"Keyword: {response.q or '-'}\n"
                f"Matched employees: {response.matched_employees}\n"
                f"Total events: {response.total_events}\n"
                f"Sent at: {sent_at}\n"
            ),
            attachments=[
                (f"{filename_base}.csv", csv_content.encode("utf-8"), "text", "csv"),
                (
                    f"{filename_base}.xlsx",
                    xlsx_content,
                    "application",
                    "vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ),
            ],
        )
    except MailerError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"SMTP send failed: {exc}") from exc

    try:
        db.add(
            AuditLog(
                who="admin",
                action="ADMIN_EVENT_QUERY_EMAIL",
                target_type="report",
                target_id=end_d.isoformat(),
                before_json=None,
                after_json=json.dumps(
                    {
                        "start_date": response.start_date.isoformat(),
                        "end_date": response.end_date.isoformat(),
                        "scope": response.scope,
                        "event_filter": response.event_filter,
                        "q": response.q,
                        "matched_employees": response.matched_employees,
                        "total_events": response.total_events,
                        "to_email": to_email,
                    }
                ),
                reason=(f"{response.start_date}~{response.end_date} to {to_email}")[:255],
            )
        )
        db.commit()
    except Exception:
        db.rollback()

    return {"ok": True, "to_email": to_email, "total_events": response.total_events}


@router.post("/faces/register", response_model=FaceRegisterResponse)
def register_face_templates(payload: FaceRegisterRequest, db: Session = Depends(get_db)) -> FaceRegisterResponse:
    require_admin(db, payload.admin_pin)

    employee = db.execute(select(Employee).where(Employee.employee_code == payload.employee_code)).scalar_one_or_none()
    if not employee:
        raise HTTPException(status_code=404, detail="employee not found")

    if len(payload.embeddings) == 0:
        raise HTTPException(status_code=400, detail="at least one embedding is required")

    valid_embeddings = [vec for vec in payload.embeddings if len(vec) >= 32]
    if len(valid_embeddings) == 0:
        raise HTTPException(status_code=400, detail="invalid embedding payload")

    # Duplicate-face prevention: check whether these embeddings match another employee's stored template.
    # If matched, require explicit client confirmation (payload.force=True) to proceed.
    DUP_THRESHOLD = 0.45
    if not payload.force:
        rows = db.execute(
            select(
                FaceTemplate.embedding_vector,
                Employee.employee_code,
                Employee.name,
                Employee.is_active,
                Employee.termination_date,
            )
            .join(Employee, FaceTemplate.employee_id == Employee.id)
            .where(FaceTemplate.employee_id != employee.id)
        ).all()

        candidates = []
        for embedding_vector, employee_code, employee_name, is_active, termination_date in rows:
            try:
                parsed = parse_embedding(embedding_vector)
                candidates.append((employee_code, employee_name, parsed, bool(is_active), termination_date))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue

        best = None  # (distance, code, name, active, termination_date)
        for vec in valid_embeddings:
            for code, name, cand_vec, active, term_date in candidates:
                try:
                    dist = euclidean_distance(vec, cand_vec)
                except ValueError:
                    continue
                if best is None or dist < best[0]:
                    best = (dist, code, name, active, term_date)

        if best is not None and best[0] <= DUP_THRESHOLD:
            dist, dup_code, dup_name, dup_active, dup_term = best
            raise HTTPException(
                status_code=409,
                detail={
                    "type": "FACE_DUPLICATE",
                    "employee_code": employee.employee_code,
                    "duplicate_employee_code": dup_code,
                    "duplicate_employee_name": dup_name,
                    "duplicate_is_active": dup_active,
                    "duplicate_termination_date": dup_term.isoformat() if dup_term else None,
                    "distance": round(float(dist), 4),
                    "threshold": DUP_THRESHOLD,
                },
            )

    if payload.replace_existing:
        db.query(FaceTemplate).filter(FaceTemplate.employee_id == employee.id).delete()

    for vec in valid_embeddings:
        db.add(
            FaceTemplate(
                employee_id=employee.id,
                embedding_vector=json.dumps(vec),
                quality_score=1.0,
            )
        )

    was_active = bool(employee.is_active)
    if employee.termination_date is None:
        employee.is_active = True
    auto_activated = (not was_active) and bool(employee.is_active)

    db.add(
        AuditLog(
            who="admin",
            action="REGISTER_FACE_FORCE" if payload.force else "REGISTER_FACE",
            target_type="employee",
            target_id=employee.id,
            before_json=None,
            after_json=json.dumps(
                {
                    "template_count": len(valid_embeddings),
                    "force": bool(payload.force),
                    "is_active": bool(employee.is_active),
                    "auto_activated": auto_activated,
                }
            ),
            reason="face registration",
        )
    )

    db.commit()

    template_count = db.execute(
        select(func.count(FaceTemplate.id)).where(FaceTemplate.employee_id == employee.id)
    ).scalar_one()

    return FaceRegisterResponse(ok=True, employee_code=employee.employee_code, template_count=int(template_count))


@router.post("/identify", response_model=IdentifyResponse)
def identify(payload: IdentifyRequest, db: Session = Depends(get_db)) -> IdentifyResponse:
    if len(payload.embedding) < 32:
        return IdentifyResponse(
            matched=False,
            employee_code=None,
            employee_name=None,
            confidence=0.0,
            distance=None,
            reason="NO_FACE",
        )

    rows = db.execute(
        select(FaceTemplate.embedding_vector, Employee.employee_code, Employee.name)
        .join(Employee, FaceTemplate.employee_id == Employee.id)
        .where(Employee.is_active.is_(True), Employee.termination_date.is_(None))
    ).all()

    candidates = []
    for embedding_vector, employee_code, employee_name in rows:
        try:
            parsed = parse_embedding(embedding_vector)
            candidates.append((employee_code, employee_name, parsed))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue

    # Safer identification: block ambiguous matches (top1 vs top2 too close).
    # This reduces the chance that A is recognized as B in edge cases.
    AMBIGUOUS_MARGIN = 0.02
    best, second = top2_matches(payload.embedding, candidates)
    if best is None:
        return IdentifyResponse(
            matched=False,
            employee_code=None,
            employee_name=None,
            confidence=0.0,
            distance=None,
            reason="NO_MATCH",
        )

    best_code, best_name, best_dist = best
    second_dist = second[2] if second is not None else None

    # Respect the client threshold for "match", but block if the second-best is too close.
    if best_dist > float(payload.threshold):
        conf = max(0.0, 1.0 - best_dist)
        return IdentifyResponse(
            matched=False,
            employee_code=None,
            employee_name=None,
            confidence=round(conf, 4),
            distance=round(best_dist, 4),
            reason="NO_MATCH",
            second_distance=None if second_dist is None else round(float(second_dist), 4),
        )

    if second_dist is not None and (float(second_dist) - float(best_dist)) < AMBIGUOUS_MARGIN:
        # Too close: force a re-scan rather than guessing.
        return IdentifyResponse(
            matched=False,
            employee_code=None,
            employee_name=None,
            confidence=0.0,
            distance=round(best_dist, 4),
            reason="AMBIGUOUS",
            second_distance=round(float(second_dist), 4),
        )

    matched, employee_code, employee_name, confidence, distance = best_match(
        embedding=payload.embedding,
        candidates=candidates,
        threshold=payload.threshold,
    )

    status = None
    allowed: Optional[list[str]] = None
    if matched and employee_code:
        emp = db.execute(
            select(Employee).where(Employee.employee_code == employee_code, Employee.is_active.is_(True), Employee.termination_date.is_(None))
        ).scalar_one_or_none()
        if emp is None:
            matched = False
            employee_code = None
            employee_name = None
            confidence = 0.0
            distance = None
        else:
            last_ev = (
                db.execute(select(TimeEvent).where(TimeEvent.employee_id == emp.id).order_by(TimeEvent.ts_utc.desc()).limit(1))
                .scalars()
                .first()
            )
            state = infer_state([last_ev] if last_ev is not None else [])
            status = state.status
            allowed = [e.value for e in allowed_events_for_status(state.status)]

    return IdentifyResponse(
        matched=matched,
        employee_code=employee_code,
        employee_name=employee_name,
        confidence=confidence,
        distance=distance,
        status=status,
        allowed_events=allowed,
        reason=None if matched else "NO_MATCH",
    )


@router.get("/offline/face-cache", response_model=OfflineFaceCacheResponse)
def offline_face_cache(db: Session = Depends(get_db)) -> OfflineFaceCacheResponse:
    rows = db.execute(
        select(Employee.employee_code, Employee.name, FaceTemplate.embedding_vector)
        .join(FaceTemplate, FaceTemplate.employee_id == Employee.id)
        .where(Employee.is_active.is_(True), Employee.termination_date.is_(None))
        .order_by(Employee.employee_code.asc(), FaceTemplate.created_at.asc())
    ).all()

    by_employee: dict[str, dict[str, object]] = {}
    total_templates = 0
    for employee_code, employee_name, embedding_vector in rows:
        try:
            parsed = parse_embedding(embedding_vector)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if len(parsed) < 32:
            continue
        bucket = by_employee.setdefault(
            str(employee_code),
            {"employee_name": str(employee_name), "embeddings": []},
        )
        bucket["embeddings"].append(parsed)
        total_templates += 1

    employees: list[OfflineFaceCacheEmployee] = []
    for employee_code in sorted(by_employee.keys()):
        bucket = by_employee[employee_code]
        embeddings = bucket.get("embeddings") or []
        if not embeddings:
            continue
        employees.append(
            OfflineFaceCacheEmployee(
                employee_code=employee_code,
                employee_name=str(bucket.get("employee_name") or employee_code),
                embeddings=embeddings,
            )
        )

    return OfflineFaceCacheResponse(
        generated_at=datetime.now(timezone.utc),
        offline_threshold=_OFFLINE_FACE_THRESHOLD,
        total_employees=len(employees),
        total_templates=total_templates,
        employees=employees,
    )


@router.post("/events", response_model=EventOut)
def create_event(payload: EventCreate, db: Session = Depends(get_db)) -> TimeEvent:
    event_uuid = (payload.event_uuid or "").strip() or None
    if event_uuid:
        duplicate = db.execute(select(TimeEvent).where(TimeEvent.event_uuid == event_uuid)).scalar_one_or_none()
        if duplicate is not None:
            return duplicate

    employee = db.execute(
        select(Employee).where(
            Employee.employee_code == payload.employee_code,
            Employee.is_active.is_(True),
            Employee.termination_date.is_(None),
        )
    ).scalar_one_or_none()
    if not employee:
        raise HTTPException(status_code=404, detail="employee not found or inactive")

    method = EventMethod(payload.method)
    if method in {EventMethod.MANUAL, EventMethod.ADMIN_EDIT}:
        require_admin(db, payload.admin_pin)

    ts = payload.ts_utc or datetime.now(timezone.utc).replace(tzinfo=None)
    event_type = EventType(payload.event_type)

    existing_events = (
        db.execute(select(TimeEvent).where(TimeEvent.employee_id == employee.id).order_by(TimeEvent.ts_utc)).scalars().all()
    )

    try:
        apply_event(existing_events, event_type, ts)
    except StateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    event = TimeEvent(
        employee_id=employee.id,
        event_type=event_type,
        ts_utc=ts,
        event_uuid=event_uuid,
        device_id=payload.device_id,
        method=method,
        confidence=payload.confidence,
        note=payload.note,
    )
    db.add(event)
    # Rolling retention: keep only the most recent 365 days (UTC).
    cutoff = datetime.utcnow() - timedelta(days=_RETENTION_DAYS)
    db.execute(delete(TimeEvent).where(TimeEvent.ts_utc < cutoff))
    db.execute(delete(TimeSegment).where(TimeSegment.work_date < cutoff.date()))
    db.execute(delete(AuditLog).where(AuditLog.created_at < cutoff))
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        if event_uuid:
            duplicate = db.execute(select(TimeEvent).where(TimeEvent.event_uuid == event_uuid)).scalar_one_or_none()
            if duplicate is not None:
                return duplicate
        raise HTTPException(status_code=409, detail="duplicate event_uuid") from exc
    db.refresh(event)
    return event


@router.get("/events", response_model=list[EventOut])
def list_events(employee_code: Optional[str] = None, db: Session = Depends(get_db)) -> list[TimeEvent]:
    query = select(TimeEvent).order_by(TimeEvent.ts_utc.desc())
    if employee_code:
        employee = db.execute(select(Employee).where(Employee.employee_code == employee_code)).scalar_one_or_none()
        if employee is None:
            return []
        query = query.where(TimeEvent.employee_id == employee.id)
    return db.execute(query.limit(500)).scalars().all()


@router.get("/reports/pay-period.csv")
def pay_period_csv(start_date: str, end_date: str, db: Session = Depends(get_db)) -> Response:
    try:
        start = datetime.fromisoformat(start_date).date()
        end = datetime.fromisoformat(end_date).date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD") from exc

    csv_bytes = build_pay_period_csv_bytes(db, start, end)
    file_name = f"ADP_pay_period_{start}_{end}.csv"

    headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
    return Response(content=csv_bytes, media_type="text/csv; charset=utf-8", headers=headers)


@router.post("/reports/pay-period/email")
def pay_period_email(payload: EmailCsvRequest, db: Session = Depends(get_db)) -> dict[str, bool]:
    adp_attachment = build_pay_period_adp_attachment(db, payload.start_date, payload.end_date)
    xlsx_bytes = build_pay_period_xlsx(db, payload.start_date, payload.end_date)
    excel_filename = f"excel_pay_period_{payload.start_date}_{payload.end_date}.xlsx"
    sent_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    try:
        send_email_with_attachments(
            to_email=str(payload.to_email),
            subject=f"Payroll Files {payload.start_date} ~ {payload.end_date} ({sent_at})",
            body=f"Attached are ADP CSV and Excel payroll files generated by Timeclock MVP.\nSent at: {sent_at}",
            attachments=[
                adp_attachment,
                (
                    excel_filename,
                    xlsx_bytes,
                    "application",
                    "vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            ],
        )
    except MailerError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"SMTP send failed: {exc}") from exc

    return {"ok": True}
