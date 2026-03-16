from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zipfile import ZIP_DEFLATED, ZipFile

from sqlalchemy.sql import text

from app.core.tz import EASTERN_TZ, eastern_date_range_to_utc_naive
from app.db.session import engine

_K_PAYROLL_AUTO_CYCLE_WEEKS = "payroll_auto_email_cycle_weeks"
_K_PAYROLL_AUTO_ANCHOR_WEEKDAY = "payroll_auto_email_anchor_weekday"
_K_PAYROLL_AUTO_ANCHOR_START = "payroll_auto_email_anchor_start_date"


@dataclass(frozen=True)
class BackupItem:
    backup_id: str
    kind: str
    filename: str
    backup_year: int
    covered_start: str
    covered_end: str
    created_at: str
    size_bytes: int
    path: Path


@dataclass(frozen=True)
class BackupRunResult:
    annual: BackupItem
    recovery: BackupItem


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _sqlite_db_path() -> Path:
    if engine.url.get_backend_name() != "sqlite":
        raise RuntimeError("System backup currently supports sqlite only")
    raw = engine.url.database or ""
    if not raw:
        raise RuntimeError("SQLite database path is not configured")
    path = Path(raw)
    if path.is_absolute():
        return path
    return (_project_root() / path).resolve()


def _db_parent() -> Path:
    return _sqlite_db_path().resolve().parent


def backup_root() -> Path:
    return (_db_parent() / "system_backups").resolve()


def annual_backup_dir() -> Path:
    return (backup_root() / "annual").resolve()


def recovery_backup_dir() -> Path:
    return (backup_root() / "recovery").resolve()


def _restore_sentinel_path() -> Path:
    return (backup_root() / ".restore_in_progress").resolve()


def _tmp_dir() -> Path:
    return (backup_root() / ".tmp").resolve()


def ensure_backup_dirs() -> None:
    annual_backup_dir().mkdir(parents=True, exist_ok=True)
    recovery_backup_dir().mkdir(parents=True, exist_ok=True)
    _tmp_dir().mkdir(parents=True, exist_ok=True)


def restore_in_progress() -> bool:
    return _restore_sentinel_path().exists()


@contextmanager
def _restore_guard():
    ensure_backup_dirs()
    sentinel = _restore_sentinel_path()
    try:
        fd = os.open(sentinel, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise RuntimeError("system restore already in progress") from exc
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(datetime.now(timezone.utc).isoformat())
        yield
    finally:
        sentinel.unlink(missing_ok=True)


def _cfg_get_raw(key: str) -> Optional[str]:
    with engine.begin() as conn:
        return conn.execute(text("SELECT value FROM app_config WHERE key=:k"), {"k": key}).scalar_one_or_none()


def _cfg_get_int(key: str, default: int, *, min_value: int, max_value: int) -> int:
    raw = (_cfg_get_raw(key) or "").strip()
    try:
        value = int(raw)
    except Exception:
        value = default
    if value < min_value or value > max_value:
        return default
    return value


def _cfg_get_date(key: str) -> Optional[date]:
    raw = (_cfg_get_raw(key) or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw).date()
    except Exception:
        return None


def _default_anchor_start(today_et: date, anchor_weekday: int) -> date:
    delta = (today_et.weekday() - anchor_weekday) % 7
    return today_et - timedelta(days=delta)


def _period_days(cycle_weeks: int) -> int:
    return 7 * (1 if cycle_weeks == 1 else 2)


def _payroll_config(reference_day: Optional[date] = None) -> tuple[int, int, date]:
    today_et = reference_day or datetime.now(EASTERN_TZ).date()
    cycle_weeks = _cfg_get_int(_K_PAYROLL_AUTO_CYCLE_WEEKS, 2, min_value=1, max_value=2)
    anchor_weekday = _cfg_get_int(_K_PAYROLL_AUTO_ANCHOR_WEEKDAY, 0, min_value=0, max_value=6)
    anchor_start = _cfg_get_date(_K_PAYROLL_AUTO_ANCHOR_START)
    if anchor_start is None:
        anchor_start = _default_anchor_start(today_et, anchor_weekday)
    return cycle_weeks, anchor_weekday, anchor_start


def _pay_period_containing(target_day: date) -> date:
    cycle_weeks, _, anchor_start = _payroll_config(target_day)
    span_days = _period_days(cycle_weeks)
    offset_days = (target_day - anchor_start).days
    period_index = offset_days // span_days
    return anchor_start + timedelta(days=period_index * span_days)


def _backup_window_for_year(backup_year: int, *, today_et: Optional[date] = None) -> tuple[date, date]:
    today = today_et or datetime.now(EASTERN_TZ).date()
    start = _pay_period_containing(date(backup_year, 1, 1))
    if backup_year < today.year:
        end = date(backup_year, 12, 31)
    else:
        end = today
    return start, end


def _manifest(
    *,
    kind: str,
    backup_year: int,
    covered_start: date,
    covered_end: date,
    db_filename: str,
) -> dict[str, Any]:
    now_et = datetime.now(EASTERN_TZ)
    return {
        "kind": kind,
        "backup_year": backup_year,
        "covered_start": covered_start.isoformat(),
        "covered_end": covered_end.isoformat(),
        "created_at": now_et.isoformat(),
        "db_filename": db_filename,
    }


def _safe_backup_id(path: Path) -> str:
    return path.relative_to(backup_root()).as_posix()


def _sqlite_backup(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(src) as src_conn, sqlite3.connect(dest) as dest_conn:
        src_conn.backup(dest_conn)


def _annual_archive_name(backup_year: int) -> str:
    return f"{backup_year}_timeclock data backup.zip"


def _pre_restore_archive_name() -> str:
    return "pre_restore_latest.zip"


def _recovery_archive_name(covered_start: date, covered_end: date) -> str:
    start_part = f"{covered_start.isoformat().replace('-', '')}(00:00:00)"
    end_part = f"{covered_end.isoformat().replace('-', '')}(23:59:59)"
    return f"{start_part}-{end_part}.zip"


def _write_zip(zip_path: Path, *, sqlite_file: Path, manifest: dict[str, Any]) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_zip = zip_path.with_suffix(zip_path.suffix + ".tmp")
    if tmp_zip.exists():
        tmp_zip.unlink()
    with ZipFile(tmp_zip, "w", compression=ZIP_DEFLATED) as zf:
        zf.write(sqlite_file, arcname=manifest["db_filename"])
        zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=True, indent=2))
    tmp_zip.replace(zip_path)


def _has_time_data_within(snapshot_db: Path, *, covered_start: date, covered_end: date) -> bool:
    utc_start, utc_end = eastern_date_range_to_utc_naive(covered_start, covered_end)
    with sqlite3.connect(snapshot_db) as conn:
        event_row = conn.execute(
            "SELECT 1 FROM time_events WHERE ts_utc >= ? AND ts_utc < ? LIMIT 1",
            (utc_start.isoformat(sep=" "), utc_end.isoformat(sep=" ")),
        ).fetchone()
        if event_row is not None:
            return True
        segment_row = conn.execute(
            "SELECT 1 FROM time_segments WHERE work_date >= ? AND work_date <= ? LIMIT 1",
            (covered_start.isoformat(), covered_end.isoformat()),
        ).fetchone()
        return segment_row is not None


def _load_manifest(zip_path: Path) -> dict[str, Any]:
    with ZipFile(zip_path, "r") as zf:
        with zf.open("manifest.json") as fh:
            return json.load(fh)


def _validate_backup_archive(zip_path: Path) -> dict[str, Any]:
    with ZipFile(zip_path, "r") as zf:
        try:
            with zf.open("manifest.json") as fh:
                manifest = json.load(fh)
        except KeyError as exc:
            raise RuntimeError("backup archive missing manifest.json") from exc
        db_filename = str(manifest.get("db_filename") or "").strip()
        if not db_filename:
            raise RuntimeError("backup manifest missing db filename")
        names = set(zf.namelist())
        if db_filename not in names:
            raise RuntimeError("backup payload missing sqlite database")
    kind = str(manifest.get("kind") or "").strip().upper()
    if kind not in {"ANNUAL", "RECOVERY"}:
        raise RuntimeError("unsupported backup kind")
    try:
        int(manifest.get("backup_year") or 0)
    except Exception as exc:
        raise RuntimeError("backup manifest missing backup year") from exc
    covered_start = str(manifest.get("covered_start") or "").strip()
    covered_end = str(manifest.get("covered_end") or "").strip()
    if not covered_start or not covered_end:
        raise RuntimeError("backup manifest missing covered period")
    return manifest


def _prune_snapshot_for_annual(snapshot_db: Path, *, covered_start: date, covered_end: date) -> None:
    utc_start, utc_end = eastern_date_range_to_utc_naive(covered_start, covered_end)
    with sqlite3.connect(snapshot_db) as conn:
        conn.execute("DELETE FROM time_events WHERE ts_utc < ? OR ts_utc >= ?", (utc_start.isoformat(sep=" "), utc_end.isoformat(sep=" ")))
        conn.execute("DELETE FROM time_segments WHERE work_date < ? OR work_date > ?", (covered_start.isoformat(), covered_end.isoformat()))
        conn.execute("DELETE FROM audit_logs WHERE created_at < ? OR created_at >= ?", (utc_start.isoformat(sep=" "), utc_end.isoformat(sep=" ")))
        conn.commit()


def _build_backup_item(path: Path) -> BackupItem:
    manifest = _load_manifest(path)
    stat = path.stat()
    return BackupItem(
        backup_id=_safe_backup_id(path),
        kind=str(manifest.get("kind") or "UNKNOWN"),
        filename=path.name,
        backup_year=int(manifest.get("backup_year") or 0),
        covered_start=str(manifest.get("covered_start") or ""),
        covered_end=str(manifest.get("covered_end") or ""),
        created_at=str(manifest.get("created_at") or datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()),
        size_bytes=stat.st_size,
        path=path,
    )


def _replace_recovery_archive(item: BackupItem) -> None:
    for existing in recovery_backup_dir().glob("*.zip"):
        if existing.resolve() != item.path.resolve():
            existing.unlink(missing_ok=True)


def _prune_old_archives(*, current_year: int) -> None:
    oldest_allowed_year = current_year - 2
    for candidate in annual_backup_dir().glob("*.zip"):
        try:
            backup_year = int(candidate.name.split("_", 1)[0])
        except Exception:
            continue
        if backup_year < oldest_allowed_year:
            candidate.unlink(missing_ok=True)


def list_backups() -> list[BackupItem]:
    ensure_backup_dirs()
    items: list[BackupItem] = []
    for folder in (annual_backup_dir(), recovery_backup_dir()):
        for zip_path in folder.glob("*.zip"):
            try:
                items.append(_build_backup_item(zip_path))
            except Exception:
                continue
    items.sort(key=lambda item: (item.created_at, item.filename), reverse=True)
    return items


def create_backup_set(*, now_et: Optional[datetime] = None, annual_years: Optional[list[int]] = None) -> BackupRunResult:
    ensure_backup_dirs()
    live_db = _sqlite_db_path()
    now_local = now_et.astimezone(EASTERN_TZ) if now_et else datetime.now(EASTERN_TZ)
    backup_year = now_local.year
    years_to_build = list(dict.fromkeys(annual_years or [backup_year]))
    annual_item: Optional[BackupItem] = None

    with tempfile.TemporaryDirectory(dir=_tmp_dir()) as tmp_root:
        tmp_dir = Path(tmp_root)
        full_snapshot = (tmp_dir / "timeclock_full.sqlite3").resolve()

        _sqlite_backup(live_db, full_snapshot)

        for year in years_to_build:
            covered_start, covered_end = _backup_window_for_year(year, today_et=now_local.date())
            if year != backup_year and not _has_time_data_within(
                full_snapshot,
                covered_start=covered_start,
                covered_end=covered_end,
            ):
                continue
            annual_snapshot = (tmp_dir / f"timeclock_annual_{year}.sqlite3").resolve()
            shutil.copy2(full_snapshot, annual_snapshot)
            _prune_snapshot_for_annual(annual_snapshot, covered_start=covered_start, covered_end=covered_end)

            annual_manifest = _manifest(
                kind="ANNUAL",
                backup_year=year,
                covered_start=covered_start,
                covered_end=covered_end,
                db_filename=annual_snapshot.name,
            )
            annual_path = (annual_backup_dir() / _annual_archive_name(year)).resolve()
            _write_zip(annual_path, sqlite_file=annual_snapshot, manifest=annual_manifest)
            built_item = _build_backup_item(annual_path)
            if year == backup_year or annual_item is None:
                annual_item = built_item

        covered_start, covered_end = _backup_window_for_year(backup_year, today_et=now_local.date())
        recovery_manifest = _manifest(
            kind="RECOVERY",
            backup_year=backup_year,
            covered_start=covered_start,
            covered_end=now_local.date(),
            db_filename="timeclock_recovery.sqlite3",
        )
        recovery_path = (recovery_backup_dir() / _recovery_archive_name(covered_start, now_local.date())).resolve()
        _write_zip(recovery_path, sqlite_file=full_snapshot, manifest=recovery_manifest)
        recovery_item = _build_backup_item(recovery_path)

    if annual_item is None:
        raise RuntimeError("failed to build annual backup")
    _replace_recovery_archive(recovery_item)
    _prune_old_archives(current_year=backup_year)
    return BackupRunResult(annual=annual_item, recovery=recovery_item)


def _resolve_backup_path(backup_id: str) -> Path:
    candidate = (backup_root() / backup_id).resolve()
    if backup_root() not in candidate.parents and candidate != backup_root():
        raise ValueError("invalid backup id")
    if not candidate.exists() or candidate.suffix.lower() != ".zip":
        raise FileNotFoundError("backup not found")
    return candidate


def restore_backup(backup_id: str) -> BackupItem:
    zip_path = _resolve_backup_path(backup_id)
    manifest = _load_manifest(zip_path)
    db_filename = str(manifest.get("db_filename") or "").strip()
    if not db_filename:
        raise RuntimeError("backup manifest missing db filename")

    live_db = _sqlite_db_path()
    ensure_backup_dirs()

    with _restore_guard():
        engine.dispose()
        with tempfile.TemporaryDirectory(dir=_tmp_dir()) as tmp_root:
            tmp_dir = Path(tmp_root)
            with ZipFile(zip_path, "r") as zf:
                zf.extract(db_filename, path=tmp_dir)
            extracted_db = (tmp_dir / db_filename).resolve()
            if not extracted_db.exists():
                raise RuntimeError("backup payload missing sqlite database")

            pre_restore_manifest = _manifest(
                kind="RECOVERY",
                backup_year=datetime.now(EASTERN_TZ).year,
                covered_start=datetime.now(EASTERN_TZ).date(),
                covered_end=datetime.now(EASTERN_TZ).date(),
                db_filename="timeclock_pre_restore.sqlite3",
            )
            pre_restore_path = (recovery_backup_dir() / _pre_restore_archive_name()).resolve()
            pre_restore_snapshot = (tmp_dir / "timeclock_pre_restore.sqlite3").resolve()
            _sqlite_backup(live_db, pre_restore_snapshot)
            _write_zip(pre_restore_path, sqlite_file=pre_restore_snapshot, manifest=pre_restore_manifest)

            _sqlite_backup(extracted_db, live_db)
        engine.dispose()
    return _build_backup_item(zip_path)


def import_backup_archive(src_zip: Path) -> BackupItem:
    ensure_backup_dirs()
    manifest = _validate_backup_archive(src_zip)
    kind = str(manifest.get("kind") or "").strip().upper()
    backup_year = int(manifest.get("backup_year") or 0)
    covered_start = date.fromisoformat(str(manifest.get("covered_start")))
    covered_end = date.fromisoformat(str(manifest.get("covered_end")))

    if kind == "ANNUAL":
        dest_path = (annual_backup_dir() / _annual_archive_name(backup_year)).resolve()
    else:
        if src_zip.name == _pre_restore_archive_name():
            dest_path = (recovery_backup_dir() / _pre_restore_archive_name()).resolve()
        else:
            dest_path = (recovery_backup_dir() / _recovery_archive_name(covered_start, covered_end)).resolve()

    tmp_dest = dest_path.with_suffix(dest_path.suffix + ".upload")
    if tmp_dest.exists():
        tmp_dest.unlink()
    shutil.copy2(src_zip, tmp_dest)
    tmp_dest.replace(dest_path)

    item = _build_backup_item(dest_path)
    if kind == "RECOVERY":
        _replace_recovery_archive(item)
    else:
        _prune_old_archives(current_year=datetime.now(EASTERN_TZ).year)
    return item
