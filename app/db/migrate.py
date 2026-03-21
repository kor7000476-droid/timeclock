from __future__ import annotations

import os
import secrets

import hashlib
from sqlalchemy import text
from sqlalchemy.engine import Engine


def _sqlite_columns(engine: Engine, table: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    # row: (cid, name, type, notnull, dflt_value, pk)
    return {str(r[1]) for r in rows}


def run_migrations(engine: Engine) -> None:
    # Minimal migrations for the SQLite MVP (no Alembic).
    if engine.url.get_backend_name() != "sqlite":
        return

    # App config table (stores admin PIN hash, etc.)
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS app_config ("
                "  key TEXT PRIMARY KEY,"
                "  value TEXT NOT NULL"
                ")"
            )
        )

    cols = _sqlite_columns(engine, "employees")
    if "title" not in cols:
        with engine.begin() as conn:
            try:
                conn.execute(text("ALTER TABLE employees ADD COLUMN title TEXT NOT NULL DEFAULT 'STAFF'"))
            except Exception:
                # Fallback for older SQLite builds (add nullable then backfill).
                conn.execute(text("ALTER TABLE employees ADD COLUMN title TEXT"))
                conn.execute(text("UPDATE employees SET title='STAFF' WHERE title IS NULL OR title=''"))

    cols = _sqlite_columns(engine, "employees")
    if "work_group" not in cols:
        with engine.begin() as conn:
            try:
                conn.execute(text("ALTER TABLE employees ADD COLUMN work_group TEXT NOT NULL DEFAULT 'FRONT'"))
            except Exception:
                conn.execute(text("ALTER TABLE employees ADD COLUMN work_group TEXT"))
                conn.execute(text("UPDATE employees SET work_group='FRONT' WHERE work_group IS NULL OR work_group=''"))

    cols = _sqlite_columns(engine, "employees")
    if "fixed_start_time" not in cols:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE employees ADD COLUMN fixed_start_time TEXT"))

    cols = _sqlite_columns(engine, "employees")
    if "hire_date" not in cols:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE employees ADD COLUMN hire_date TEXT"))
            # Backfill from created_at if possible; otherwise default to today.
            conn.execute(
                text(
                    "UPDATE employees "
                    "SET hire_date = COALESCE(NULLIF(substr(created_at, 1, 10), ''), date('now')) "
                    "WHERE hire_date IS NULL OR hire_date=''"
                )
            )

    cols = _sqlite_columns(engine, "employees")
    if "termination_date" not in cols:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE employees ADD COLUMN termination_date TEXT"))

    # Drop deprecated columns by rebuilding the table.
    # This ensures backups/restores no longer carry unused fields.
    cols = _sqlite_columns(engine, "employees")
    if "department" in cols or "store_id" in cols or "adp_mapping_code" in cols:
        with engine.begin() as conn:
            conn.execute(text("PRAGMA foreign_keys=OFF"))
            conn.execute(text("DROP TABLE IF EXISTS employees_new"))
            conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS employees_new ("
                    "  id TEXT PRIMARY KEY,"
                    "  employee_code TEXT NOT NULL UNIQUE,"
                    "  name TEXT NOT NULL,"
                    "  fixed_start_time TEXT,"
                    "  hire_date TEXT,"
                    "  termination_date TEXT,"
                    "  title TEXT NOT NULL DEFAULT 'STAFF',"
                    "  work_group TEXT NOT NULL DEFAULT 'FRONT',"
                    "  is_active BOOLEAN NOT NULL DEFAULT 1,"
                    "  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                    "  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
                    ")"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO employees_new("
                    "  id, employee_code, name, fixed_start_time, hire_date, termination_date, title, work_group, is_active, created_at, updated_at"
                    ") "
                    "SELECT "
                    "  id, employee_code, name, "
                    "  fixed_start_time, "
                    "  hire_date, termination_date, "
                    "  COALESCE(NULLIF(title,''), 'STAFF') AS title, "
                    "  COALESCE(NULLIF(work_group,''), 'FRONT') AS work_group, "
                    "  COALESCE(is_active, 1) AS is_active, "
                    "  COALESCE(created_at, CURRENT_TIMESTAMP) AS created_at, "
                    "  COALESCE(updated_at, CURRENT_TIMESTAMP) AS updated_at "
                    "FROM employees"
                )
            )
            conn.execute(text("DROP TABLE employees"))
            conn.execute(text("ALTER TABLE employees_new RENAME TO employees"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_employees_employee_code ON employees(employee_code)"))
            conn.execute(text("PRAGMA foreign_keys=ON"))

    # Initialize admin PIN hash only if not set (avoid storing plaintext).
    with engine.begin() as conn:
        dk = conn.execute(text("SELECT value FROM app_config WHERE key='admin_pin_dk_hex'")).scalar()
        salt = conn.execute(text("SELECT value FROM app_config WHERE key='admin_pin_salt_hex'")).scalar()
        iters = conn.execute(text("SELECT value FROM app_config WHERE key='admin_pin_iterations'")).scalar()
        if not (dk and salt and iters):
            pin = os.getenv("ADMIN_PIN", "1234")
            salt_bytes = secrets.token_bytes(16)
            dk_hex = hashlib.pbkdf2_hmac("sha256", pin.encode("utf-8"), salt_bytes, 210_000).hex()
            conn.execute(
                text(
                    "INSERT INTO app_config(key, value) VALUES(:k, :v) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
                ),
                {"k": "admin_pin_salt_hex", "v": salt_bytes.hex()},
            )
            conn.execute(
                text(
                    "INSERT INTO app_config(key, value) VALUES(:k, :v) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
                ),
                {"k": "admin_pin_dk_hex", "v": dk_hex},
            )
            conn.execute(
                text(
                    "INSERT INTO app_config(key, value) VALUES(:k, :v) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
                ),
                {"k": "admin_pin_iterations", "v": "210000"},
            )

        # Data retention is fixed at 365 days (see maintenance + event write path).

    cols = _sqlite_columns(engine, "time_events")
    if "effective_ts_utc" not in cols:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE time_events ADD COLUMN effective_ts_utc TIMESTAMP"))
            conn.execute(text("UPDATE time_events SET effective_ts_utc = ts_utc WHERE effective_ts_utc IS NULL"))

    cols = _sqlite_columns(engine, "time_events")
    if "event_uuid" not in cols:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE time_events ADD COLUMN event_uuid TEXT"))

    # Idempotency key for kiosk offline replay.
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_time_events_event_uuid "
                "ON time_events(event_uuid)"
            )
        )

    # Performance index for admin event query filtering/pagination.
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_time_events_ts_utc_employee_event "
                "ON time_events(ts_utc, employee_id, event_type)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_time_events_effective_ts_utc_employee_event "
                "ON time_events(effective_ts_utc, employee_id, event_type)"
            )
        )
