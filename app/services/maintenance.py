from __future__ import annotations

import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import delete
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from app.core.tz import EASTERN_TZ
from app.db.models import AuditLog, TimeEvent, TimeSegment
from app.db.session import SessionLocal, engine
from app.services.csv_export import build_pay_period_adp_attachment, build_pay_period_xlsx
from app.services.mailer import MailerError, send_email_with_attachments


@dataclass(frozen=True)
class MaintenanceConfig:
    retention_days: int = 365
    backup_hour: int = 1
    backup_minute: int = 30
    # Overwritten daily. Can be set to an absolute path in containers (ex: /data/backup).
    backup_filename: str = os.getenv("BACKUP_FILENAME", "backup")
    payroll_auto_schedule: str = "WEEKLY_OR_BIWEEKLY_ANCHORED_ET_0130"


_thread_started = False

_K_PAYROLL_AUTO_ENABLED = "payroll_auto_email_enabled"
_K_PAYROLL_AUTO_TO = "payroll_auto_email_to"
_K_PAYROLL_AUTO_LAST_END = "payroll_auto_email_last_end"
_K_PAYROLL_AUTO_LAST_ERROR = "payroll_auto_email_last_error"
_K_PAYROLL_AUTO_LAST_KEY = "payroll_auto_email_last_key"
_K_PAYROLL_AUTO_CATCHUP_ONCE = "payroll_auto_email_catchup_once"
_K_PAYROLL_AUTO_VERSION = "payroll_auto_email_schedule_version"
_K_PAYROLL_AUTO_CYCLE_WEEKS = "payroll_auto_email_cycle_weeks"
_K_PAYROLL_AUTO_ANCHOR_WEEKDAY = "payroll_auto_email_anchor_weekday"
_K_PAYROLL_AUTO_ANCHOR_START = "payroll_auto_email_anchor_start_date"
_K_PAYROLL_AUTO_PENDING_CYCLE_WEEKS = "payroll_auto_email_pending_cycle_weeks"
_K_PAYROLL_AUTO_PENDING_ANCHOR_WEEKDAY = "payroll_auto_email_pending_anchor_weekday"
_K_PAYROLL_AUTO_PENDING_ANCHOR_START = "payroll_auto_email_pending_anchor_start_date"
_K_PAYROLL_AUTO_PENDING_EFFECTIVE_START = "payroll_auto_email_pending_effective_start"
_PAYROLL_AUTO_VERSION_V2 = "v2"


def _project_root() -> Path:
    # app/services/maintenance.py -> app/services -> app -> project root
    return Path(__file__).resolve().parents[2]


def _data_dir() -> Path:
    """
    Data directory for runtime artifacts (backup file, lock file).
    In containers, set DATA_DIR=/data and mount it as a volume.
    """
    raw = (os.getenv("DATA_DIR") or "").strip()
    if raw:
        try:
            return Path(raw).resolve()
        except Exception:
            pass
    return _project_root()


def _sqlite_db_path() -> Optional[Path]:
    if engine.url.get_backend_name() != "sqlite":
        return None
    db = engine.url.database or ""
    if not db:
        return None
    p = Path(db)
    if p.is_absolute():
        return p
    # DATABASE_URL uses sqlite:///./timeclock.db by default; resolve relative to project root.
    return (_project_root() / p).resolve()


def _backup_path(cfg: MaintenanceConfig) -> Path:
    p = Path(cfg.backup_filename)
    if p.is_absolute():
        return p
    return (_data_dir() / cfg.backup_filename).resolve()


def _next_run_utc(cfg: MaintenanceConfig) -> datetime:
    # Scheduler is fixed to US Eastern time (DST-aware).
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(EASTERN_TZ)
    target_et = now_et.replace(hour=cfg.backup_hour, minute=cfg.backup_minute, second=0, microsecond=0)
    if now_et >= target_et:
        target_et = target_et + timedelta(days=1)
    return target_et.astimezone(timezone.utc)


def _run_retention(db: Session, *, cutoff_dt_utc: datetime) -> dict[str, int]:
    # Timestamps are stored as naive UTC in this MVP.
    cutoff_date = cutoff_dt_utc.date()

    deleted_events = db.execute(delete(TimeEvent).where(TimeEvent.ts_utc < cutoff_dt_utc)).rowcount or 0
    deleted_segments = db.execute(delete(TimeSegment).where(TimeSegment.work_date < cutoff_date)).rowcount or 0
    deleted_audits = db.execute(delete(AuditLog).where(AuditLog.created_at < cutoff_dt_utc)).rowcount or 0
    db.commit()
    return {
        "time_events": int(deleted_events),
        "time_segments": int(deleted_segments),
        "audit_logs": int(deleted_audits),
    }


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


def _parse_pending_weekday(db: Session) -> Optional[int]:
    raw = (_cfg_get(db, _K_PAYROLL_AUTO_PENDING_ANCHOR_WEEKDAY) or "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except Exception:
        return None
    return value if 0 <= value <= 6 else None


def _active_schedule(db: Session, *, now_et: date, last_end: Optional[date]) -> tuple[int, int, date]:
    cycle_weeks = _cfg_get_int(db, _K_PAYROLL_AUTO_CYCLE_WEEKS, 2, min_value=1, max_value=2)
    anchor_weekday = _cfg_get_int(db, _K_PAYROLL_AUTO_ANCHOR_WEEKDAY, 0, min_value=0, max_value=6)
    anchor_start = _cfg_get_date(db, _K_PAYROLL_AUTO_ANCHOR_START)

    if anchor_start is None:
        if last_end is not None:
            anchor_start = last_end + timedelta(days=1)
        else:
            anchor_start = _default_anchor_start(now_et, anchor_weekday)

    pending_cycle = _cfg_get_int(db, _K_PAYROLL_AUTO_PENDING_CYCLE_WEEKS, 0, min_value=0, max_value=2)
    pending_weekday = _parse_pending_weekday(db)
    pending_anchor_start = _cfg_get_date(db, _K_PAYROLL_AUTO_PENDING_ANCHOR_START)
    pending_effective_start = _cfg_get_date(db, _K_PAYROLL_AUTO_PENDING_EFFECTIVE_START)

    should_promote = (
        pending_cycle in (1, 2)
        and pending_weekday is not None
        and pending_effective_start is not None
        and last_end is not None
        and (last_end + timedelta(days=1)) >= pending_effective_start
    )

    if should_promote:
        cycle_weeks = pending_cycle
        anchor_weekday = pending_weekday
        anchor_start = pending_anchor_start or pending_effective_start
        _cfg_set(db, _K_PAYROLL_AUTO_CYCLE_WEEKS, str(cycle_weeks))
        _cfg_set(db, _K_PAYROLL_AUTO_ANCHOR_WEEKDAY, str(anchor_weekday))
        _cfg_set_date(db, _K_PAYROLL_AUTO_ANCHOR_START, anchor_start)
        _cfg_set(db, _K_PAYROLL_AUTO_PENDING_CYCLE_WEEKS, "")
        _cfg_set(db, _K_PAYROLL_AUTO_PENDING_ANCHOR_WEEKDAY, "")
        _cfg_set(db, _K_PAYROLL_AUTO_PENDING_ANCHOR_START, "")
        _cfg_set(db, _K_PAYROLL_AUTO_PENDING_EFFECTIVE_START, "")
        _cfg_set(db, _K_PAYROLL_AUTO_VERSION, _PAYROLL_AUTO_VERSION_V2)
        db.commit()
    else:
        # Ensure active defaults persist for future status reads.
        changed = False
        if (_cfg_get(db, _K_PAYROLL_AUTO_CYCLE_WEEKS) or "").strip() == "":
            _cfg_set(db, _K_PAYROLL_AUTO_CYCLE_WEEKS, str(cycle_weeks))
            changed = True
        if (_cfg_get(db, _K_PAYROLL_AUTO_ANCHOR_WEEKDAY) or "").strip() == "":
            _cfg_set(db, _K_PAYROLL_AUTO_ANCHOR_WEEKDAY, str(anchor_weekday))
            changed = True
        if (_cfg_get(db, _K_PAYROLL_AUTO_ANCHOR_START) or "").strip() == "":
            _cfg_set_date(db, _K_PAYROLL_AUTO_ANCHOR_START, anchor_start)
            changed = True
        if (_cfg_get(db, _K_PAYROLL_AUTO_VERSION) or "").strip() == "":
            _cfg_set(db, _K_PAYROLL_AUTO_VERSION, _PAYROLL_AUTO_VERSION_V2)
            changed = True
        if changed:
            db.commit()

    return cycle_weeks, anchor_weekday, anchor_start


def _latest_due_period(
    now_et: datetime,
    *,
    anchor_start: date,
    cycle_weeks: int,
) -> Optional[tuple[date, date, datetime]]:
    days = _period_days(cycle_weeks)
    days_since_anchor = (now_et.date() - anchor_start).days
    if days_since_anchor < days:
        return None
    # Candidate index where send_date is on/before today.
    idx = max(0, (days_since_anchor // days) - 1)
    for period_idx in range(idx, -1, -1):
        start_d = anchor_start + timedelta(days=period_idx * days)
        end_d = start_d + timedelta(days=days - 1)
        send_day = end_d + timedelta(days=1)
        send_at = datetime(send_day.year, send_day.month, send_day.day, 1, 30, tzinfo=EASTERN_TZ)
        if now_et >= send_at:
            return start_d, end_d, send_at
    return None


def _maybe_send_payroll_auto_email(db: Session, *, cfg: MaintenanceConfig, now_et: datetime) -> None:
    enabled = (_cfg_get(db, _K_PAYROLL_AUTO_ENABLED) or "").lower() == "true"
    if not enabled:
        return
    to_email = (_cfg_get(db, _K_PAYROLL_AUTO_TO) or "").strip()
    if not to_email:
        return
    catchup_once = (_cfg_get(db, _K_PAYROLL_AUTO_CATCHUP_ONCE) or "true").lower() == "true"

    last_end = _cfg_get_date(db, _K_PAYROLL_AUTO_LAST_END)
    cycle_weeks, anchor_weekday, anchor_start = _active_schedule(db, now_et=now_et.date(), last_end=last_end)
    due = _latest_due_period(now_et, anchor_start=anchor_start, cycle_weeks=cycle_weeks)
    if due is None:
        return
    start_d, end_d, send_at = due

    if not catchup_once and send_at.date() != now_et.date():
        return

    period_key = f"{start_d.isoformat()}|{end_d.isoformat()}"
    last_key = (_cfg_get(db, _K_PAYROLL_AUTO_LAST_KEY) or "").strip()
    if last_key == period_key:
        return
    if last_end is not None and end_d <= last_end:
        return

    try:
        adp_attachment = build_pay_period_adp_attachment(db, start_d, end_d)
        xlsx_bytes = build_pay_period_xlsx(db, start_d, end_d)
        excel_filename = f"excel_pay_period_{start_d}_{end_d}.xlsx"
        sent_at = now_et.strftime("%Y-%m-%d %H:%M:%S ET")
        send_email_with_attachments(
            to_email=to_email,
            subject=f"Payroll Files {start_d} ~ {end_d} (AUTO) ({sent_at})",
            body=(
                "Attached are ADP CSV and Excel payroll files generated by Timeclock.\n"
                f"Schedule: {cfg.payroll_auto_schedule}\n"
                f"Cadence: {cycle_weeks} week(s), weekday={anchor_weekday}\n"
                f"Period: {start_d} ~ {end_d}\n"
                f"Sent at: {sent_at}\n"
            ),
            attachments=[
                adp_attachment,
                (
                    excel_filename,
                    xlsx_bytes,
                    "application",
                    "vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ),
            ],
        )
        _cfg_set_date(db, _K_PAYROLL_AUTO_LAST_END, end_d)
        _cfg_set(db, _K_PAYROLL_AUTO_LAST_KEY, period_key)
        _cfg_set(db, _K_PAYROLL_AUTO_LAST_ERROR, "")
        db.add(
            AuditLog(
                who="system",
                action="AUTO_EMAIL_PAY_PERIOD",
                target_type="report",
                target_id=end_d.isoformat(),
                before_json=None,
                after_json=None,
                reason=f"{start_d}~{end_d} to {to_email}",
            )
        )
        db.commit()
    except MailerError as exc:
        _cfg_set(db, _K_PAYROLL_AUTO_LAST_ERROR, str(exc)[:400])
        db.commit()
    except Exception as exc:
        _cfg_set(db, _K_PAYROLL_AUTO_LAST_ERROR, f"{exc}"[:400])
        db.commit()


def _run_sqlite_backup(src_db: Path, dest_file: Path) -> None:
    dest_file.parent.mkdir(parents=True, exist_ok=True)
    if dest_file.exists():
        # Ensure we truly overwrite a single "backup" file as requested.
        dest_file.unlink()
    # Use SQLite's online backup API for a consistent snapshot.
    with sqlite3.connect(str(src_db)) as src:
        with sqlite3.connect(str(dest_file)) as dst:
            src.backup(dst)
            dst.execute("PRAGMA wal_checkpoint(FULL)")


def _lock_file_path() -> Path:
    return (_data_dir() / ".maintenance.lock").resolve()


def _acquire_lock() -> Optional[object]:
    """
    Best-effort single-instance guard (useful with uvicorn --reload).
    Returns the open file handle if lock acquired.
    """
    try:
        import fcntl  # macOS/Linux only
    except Exception:
        return None

    lock_path = _lock_file_path()
    fh = open(lock_path, "a+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return None
    return fh


def _maintenance_worker(cfg: MaintenanceConfig) -> None:
    lock_fh = _acquire_lock()
    if lock_fh is None and os.name != "nt":
        # Another process is already running maintenance.
        return

    # One-time cleanup + one-time catch-up check at startup.
    try:
        with SessionLocal() as db:
            cutoff = datetime.utcnow() - timedelta(days=cfg.retention_days)
            _run_retention(db, cutoff_dt_utc=cutoff)
            _maybe_send_payroll_auto_email(db, cfg=cfg, now_et=datetime.now(timezone.utc).astimezone(EASTERN_TZ))
    except Exception:
        # Keep the worker alive; next run may succeed.
        pass

    while True:
        next_run_utc = _next_run_utc(cfg)
        sleep_s = max(1.0, (next_run_utc - datetime.now(timezone.utc)).total_seconds())
        time.sleep(sleep_s)

        try:
            with SessionLocal() as db:
                cutoff = datetime.utcnow() - timedelta(days=cfg.retention_days)
                _run_retention(db, cutoff_dt_utc=cutoff)
                _maybe_send_payroll_auto_email(db, cfg=cfg, now_et=datetime.now(timezone.utc).astimezone(EASTERN_TZ))

            src = _sqlite_db_path()
            if src is not None and src.exists():
                _run_sqlite_backup(src, _backup_path(cfg))
        except Exception:
            # Swallow errors; try again next day.
            continue


def start_maintenance(cfg: Optional[MaintenanceConfig] = None) -> None:
    global _thread_started
    if _thread_started:
        return
    _thread_started = True

    _cfg = cfg or MaintenanceConfig()
    t = threading.Thread(target=_maintenance_worker, args=(_cfg,), daemon=True, name="maintenance")
    t.start()
