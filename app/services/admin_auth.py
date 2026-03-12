from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from app.db.models import AuditLog


_CFG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS app_config (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
)
"""

_K_PIN_SALT = "admin_pin_salt_hex"
_K_PIN_DK = "admin_pin_dk_hex"
_K_PIN_ITERS = "admin_pin_iterations"
_DEFAULT_ITERS = 210_000


def _ensure_table(db: Session) -> None:
    db.execute(text(_CFG_TABLE_SQL))


def _cfg_get(db: Session, key: str) -> Optional[str]:
    return db.execute(text("SELECT value FROM app_config WHERE key=:k"), {"k": key}).scalar_one_or_none()


def _cfg_set(db: Session, key: str, value: str) -> None:
    # SQLite upsert (3.24+). If unavailable, this will error loudly in dev.
    db.execute(
        text(
            "INSERT INTO app_config(key, value) VALUES(:k, :v) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
        ),
        {"k": key, "v": value},
    )


def _hash_pin(pin: str, *, salt: Optional[bytes] = None, iterations: int = _DEFAULT_ITERS) -> tuple[str, str, int]:
    salt_bytes = salt or secrets.token_bytes(16)
    dk_hex = hashlib.pbkdf2_hmac("sha256", pin.encode("utf-8"), salt_bytes, iterations).hex()
    return salt_bytes.hex(), dk_hex, iterations


def ensure_admin_pin_initialized(db: Session) -> None:
    """Initialize stored admin PIN hash from env only if DB is empty."""
    _ensure_table(db)
    dk = _cfg_get(db, _K_PIN_DK)
    salt = _cfg_get(db, _K_PIN_SALT)
    iters = _cfg_get(db, _K_PIN_ITERS)
    if dk and salt and iters:
        return

    # First-run initialization: hash ADMIN_PIN from env (never store plaintext).
    env_pin = os.getenv("ADMIN_PIN", "1234")
    salt_hex, dk_hex, iterations = _hash_pin(env_pin)
    _cfg_set(db, _K_PIN_SALT, salt_hex)
    _cfg_set(db, _K_PIN_DK, dk_hex)
    _cfg_set(db, _K_PIN_ITERS, str(iterations))
    db.commit()


def verify_admin_pin(db: Session, candidate: str, *, verify_master: callable[[str], bool]) -> bool:
    """Returns True if candidate matches the stored admin PIN OR the master password."""
    if not candidate:
        return False
    if verify_master(candidate):
        return True

    ensure_admin_pin_initialized(db)
    salt_hex = _cfg_get(db, _K_PIN_SALT)
    dk_hex = _cfg_get(db, _K_PIN_DK)
    iters_raw = _cfg_get(db, _K_PIN_ITERS)
    if not salt_hex or not dk_hex:
        return False
    try:
        iters = int(iters_raw or _DEFAULT_ITERS)
    except ValueError:
        iters = _DEFAULT_ITERS

    salt = bytes.fromhex(salt_hex)
    cand_dk = hashlib.pbkdf2_hmac("sha256", candidate.encode("utf-8"), salt, iters).hex()
    return hmac.compare_digest(cand_dk, dk_hex)


def change_admin_pin(
    db: Session,
    *,
    actor: str,
    new_pin: str,
) -> None:
    ensure_admin_pin_initialized(db)
    before = {
        "admin_pin": "set",
        "changed_at": datetime.utcnow().isoformat(),
    }

    salt_hex, dk_hex, iterations = _hash_pin(new_pin)
    _cfg_set(db, _K_PIN_SALT, salt_hex)
    _cfg_set(db, _K_PIN_DK, dk_hex)
    _cfg_set(db, _K_PIN_ITERS, str(iterations))

    db.add(
        AuditLog(
            who=actor,
            action="CHANGE_ADMIN_PIN",
            target_type="system",
            target_id="admin_pin",
            before_json=json.dumps(before),
            after_json=json.dumps({"admin_pin": "updated"}),
            reason="admin pin change",
        )
    )
    db.commit()
