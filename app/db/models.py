from __future__ import annotations

import enum
import uuid
from datetime import date, datetime
from typing import Optional

from sqlalchemy import Boolean, Date, DateTime, Enum, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


def uuid4() -> str:
    return str(uuid.uuid4())


class EventType(str, enum.Enum):
    CLOCK_IN = "CLOCK_IN"
    CLOCK_OUT = "CLOCK_OUT"
    BREAK_START = "BREAK_START"
    BREAK_END = "BREAK_END"


class EventMethod(str, enum.Enum):
    FACE = "FACE"
    MANUAL = "MANUAL"
    ADMIN_EDIT = "ADMIN_EDIT"


class Employee(Base):
    __tablename__ = "employees"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid4)
    employee_code: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(120))
    hire_date: Mapped[date] = mapped_column(Date, nullable=False)
    termination_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    title: Mapped[str] = mapped_column(String(16), default="STAFF")
    work_group: Mapped[str] = mapped_column(String(16), default="FRONT")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    face_templates: Mapped[list[FaceTemplate]] = relationship(back_populates="employee", cascade="all, delete-orphan")
    time_events: Mapped[list[TimeEvent]] = relationship(back_populates="employee", cascade="all, delete-orphan")


class FaceTemplate(Base):
    __tablename__ = "face_templates"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid4)
    employee_id: Mapped[str] = mapped_column(String(36), ForeignKey("employees.id", ondelete="CASCADE"), index=True)
    embedding_vector: Mapped[str] = mapped_column(Text)
    quality_score: Mapped[float] = mapped_column(Float, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    employee: Mapped[Employee] = relationship(back_populates="face_templates")


class TimeEvent(Base):
    __tablename__ = "time_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid4)
    employee_id: Mapped[str] = mapped_column(String(36), ForeignKey("employees.id", ondelete="CASCADE"), index=True)
    event_type: Mapped[EventType] = mapped_column(Enum(EventType), index=True)
    ts_utc: Mapped[datetime] = mapped_column(DateTime, index=True)
    event_uuid: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    device_id: Mapped[str] = mapped_column(String(64), index=True)
    method: Mapped[EventMethod] = mapped_column(Enum(EventMethod), default=EventMethod.FACE)
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    note: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    employee: Mapped[Employee] = relationship(back_populates="time_events")


class TimeSegment(Base):
    __tablename__ = "time_segments"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid4)
    employee_id: Mapped[str] = mapped_column(String(36), ForeignKey("employees.id", ondelete="CASCADE"), index=True)
    work_date: Mapped[datetime] = mapped_column(Date, index=True)
    start_ts: Mapped[datetime] = mapped_column(DateTime)
    end_ts: Mapped[datetime] = mapped_column(DateTime)
    break_minutes: Mapped[int] = mapped_column(Integer, default=0)
    total_work_minutes: Mapped[int] = mapped_column(Integer)
    pay_period_id: Mapped[str] = mapped_column(String(32), index=True)
    flags: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=uuid4)
    who: Mapped[str] = mapped_column(String(120), index=True)
    action: Mapped[str] = mapped_column(String(120), index=True)
    target_type: Mapped[str] = mapped_column(String(60))
    target_id: Mapped[str] = mapped_column(String(36), index=True)
    before_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    after_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
