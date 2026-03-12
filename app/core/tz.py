from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Optional
try:
    from zoneinfo import ZoneInfo
except ModuleNotFoundError:
    from backports.zoneinfo import ZoneInfo


# Timeclock is defined to operate on US Eastern time for date boundaries.
EASTERN_TZ = ZoneInfo("America/New_York")


def utc_naive_to_eastern(dt_utc_naive: datetime) -> datetime:
    """Interpret naive datetime as UTC and convert to US Eastern (tz-aware)."""
    return dt_utc_naive.replace(tzinfo=timezone.utc).astimezone(EASTERN_TZ)


def eastern_date_range_to_utc_naive(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    """
    Convert an inclusive [start_date, end_date] in US Eastern into a UTC-naive
    half-open range [utc_start, utc_end) for DB filtering.
    """
    local_start = datetime.combine(start_date, time.min).replace(tzinfo=EASTERN_TZ)
    local_end = datetime.combine(end_date + timedelta(days=1), time.min).replace(tzinfo=EASTERN_TZ)
    utc_start = local_start.astimezone(timezone.utc).replace(tzinfo=None)
    utc_end = local_end.astimezone(timezone.utc).replace(tzinfo=None)
    return utc_start, utc_end


def eastern_today_utc_naive_range(now_utc: Optional[datetime] = None) -> tuple[datetime, datetime]:
    """UTC-naive [start, end) range representing 'today' in US Eastern."""
    now = now_utc or datetime.now(timezone.utc).replace(tzinfo=None)
    local_now = utc_naive_to_eastern(now)
    start_local = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    return (
        start_local.astimezone(timezone.utc).replace(tzinfo=None),
        end_local.astimezone(timezone.utc).replace(tzinfo=None),
    )
