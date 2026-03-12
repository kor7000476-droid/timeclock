from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime

from app.db.models import EventType, TimeEvent
from app.core.tz import utc_naive_to_eastern


@dataclass
class DaySummary:
    employee_id: str
    work_date: date
    total_work_minutes: int
    break_minutes: int
    flags: list[str]


def _minutes_between(start: datetime, end: datetime) -> int:
    return int((end - start).total_seconds() // 60)


def summarize_employee_events(employee_id: str, events: list[TimeEvent]) -> list[DaySummary]:
    grouped: dict[date, list[TimeEvent]] = defaultdict(list)
    for ev in sorted(events, key=lambda e: e.ts_utc):
        grouped[utc_naive_to_eastern(ev.ts_utc).date()].append(ev)

    summaries: list[DaySummary] = []
    for work_date, day_events in grouped.items():
        flags: list[str] = []
        total_work = 0
        total_break = 0

        clock_in_at = None
        break_started_at = None

        for ev in day_events:
            if ev.event_type == EventType.CLOCK_IN:
                clock_in_at = ev.ts_utc
            elif ev.event_type == EventType.BREAK_START:
                if clock_in_at is None:
                    flags.append("break_start_without_clock_in")
                    continue
                break_started_at = ev.ts_utc
                total_work += _minutes_between(clock_in_at, ev.ts_utc)
            elif ev.event_type == EventType.BREAK_END:
                if break_started_at is None:
                    flags.append("break_end_without_break_start")
                    continue
                total_break += _minutes_between(break_started_at, ev.ts_utc)
                clock_in_at = ev.ts_utc
                break_started_at = None
            elif ev.event_type == EventType.CLOCK_OUT:
                if clock_in_at is None:
                    flags.append("clock_out_without_clock_in")
                    continue
                total_work += _minutes_between(clock_in_at, ev.ts_utc)
                clock_in_at = None

        if clock_in_at is not None:
            flags.append("missing_clock_out")
        if break_started_at is not None:
            flags.append("missing_break_end")

        summaries.append(
            DaySummary(
                employee_id=employee_id,
                work_date=work_date,
                total_work_minutes=max(total_work, 0),
                break_minutes=max(total_break, 0),
                flags=flags,
            )
        )

    return summaries
