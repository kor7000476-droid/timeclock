from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from app.db.models import EventType, TimeEvent


class StateError(ValueError):
    pass


@dataclass
class EmployeeState:
    status: str
    last_event: Optional[EventType]


def infer_state(events: list[TimeEvent]) -> EmployeeState:
    if not events:
        return EmployeeState(status="OFF", last_event=None)

    latest = events[-1]
    if latest.event_type == EventType.CLOCK_IN:
        return EmployeeState(status="WORKING", last_event=latest.event_type)
    if latest.event_type == EventType.BREAK_START:
        return EmployeeState(status="BREAK", last_event=latest.event_type)
    if latest.event_type == EventType.BREAK_END:
        return EmployeeState(status="WORKING", last_event=latest.event_type)
    return EmployeeState(status="OFF", last_event=latest.event_type)


def validate_transition(current_status: str, event_type: EventType) -> None:
    allowed = {
        "OFF": {EventType.CLOCK_IN},
        "WORKING": {EventType.BREAK_START, EventType.CLOCK_OUT},
        "BREAK": {EventType.BREAK_END},
    }
    if event_type not in allowed.get(current_status, set()):
        raise StateError(f"Invalid transition: {current_status} -> {event_type.value}")


def allowed_events_for_status(status: str) -> list[EventType]:
    allowed = {
        "OFF": [EventType.CLOCK_IN],
        "WORKING": [EventType.CLOCK_OUT, EventType.BREAK_START],
        "BREAK": [EventType.BREAK_END],
    }
    return allowed.get(status, [])


def apply_event(existing_events: list[TimeEvent], event_type: EventType, ts_utc: datetime) -> None:
    state = infer_state(existing_events)
    validate_transition(state.status, event_type)

    if existing_events and ts_utc < existing_events[-1].ts_utc:
        raise StateError("Event timestamp cannot be earlier than the previous event")
