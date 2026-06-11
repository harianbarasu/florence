"""Timezone-aware time helpers.

The model resolves natural language ("Friday after school") into concrete
local datetimes because it always knows the current local time. These
functions only validate, convert, format, and roll recurrences forward.
"""

from __future__ import annotations

import calendar
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

RECURRENCES = ("daily", "weekdays", "weekly", "biweekly", "monthly", "yearly")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def resolve_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name.strip())
    except (ZoneInfoNotFoundError, ValueError, KeyError) as exc:
        raise ValueError(f"unknown timezone: {name!r} (use an IANA name like America/Chicago)") from exc


def parse_local(value: str, tz_name: str) -> datetime:
    """Parse 'YYYY-MM-DD HH:MM' in the household timezone; return aware UTC."""
    zone = resolve_timezone(tz_name)
    raw = value.strip().replace("T", " ")
    try:
        local = datetime.strptime(raw, "%Y-%m-%d %H:%M")
    except ValueError:
        try:
            local = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise ValueError(
                f"could not parse {value!r}; use 'YYYY-MM-DD HH:MM' (24-hour, local time)"
            ) from exc
    return local.replace(tzinfo=zone).astimezone(timezone.utc)


def to_local(value: datetime, tz_name: str) -> datetime:
    return ensure_utc(value).astimezone(resolve_timezone(tz_name))


def format_local(value: datetime, tz_name: str) -> str:
    local = to_local(value, tz_name)
    hour = local.strftime("%I:%M %p").lstrip("0")
    return f"{local.strftime('%a %b')} {local.day}, {local.year} {hour}"


def format_now_verbose(tz_name: str, at: datetime | None = None) -> str:
    local = to_local(at or now_utc(), tz_name)
    hour = local.strftime("%I:%M %p").lstrip("0")
    return f"{local.strftime('%A, %B')} {local.day}, {local.year}, {hour}"


def next_occurrence(due_at_utc: datetime, recurrence: str, tz_name: str) -> datetime:
    """Next occurrence after the previous due time, keeping the local wall-clock
    time stable across DST changes."""
    zone = resolve_timezone(tz_name)
    naive = ensure_utc(due_at_utc).astimezone(zone).replace(tzinfo=None)
    rec = recurrence.strip().lower()
    if rec == "daily":
        naive += timedelta(days=1)
    elif rec == "weekdays":
        naive += timedelta(days=1)
        while naive.weekday() >= 5:
            naive += timedelta(days=1)
    elif rec == "weekly":
        naive += timedelta(weeks=1)
    elif rec == "biweekly":
        naive += timedelta(weeks=2)
    elif rec == "monthly":
        month = naive.month % 12 + 1
        year = naive.year + (1 if month == 1 else 0)
        naive = naive.replace(year=year, month=month, day=min(naive.day, calendar.monthrange(year, month)[1]))
    elif rec == "yearly":
        year = naive.year + 1
        naive = naive.replace(year=year, day=min(naive.day, calendar.monthrange(year, naive.month)[1]))
    else:
        raise ValueError(f"unknown recurrence: {recurrence!r} (use one of {', '.join(RECURRENCES)})")
    return naive.replace(tzinfo=zone).astimezone(timezone.utc)


def next_local_time(hour: int, minute: int, tz_name: str, *, after: datetime | None = None) -> datetime:
    """Next occurrence of a local wall-clock time strictly after `after` (UTC)."""
    zone = resolve_timezone(tz_name)
    base = ensure_utc(after or now_utc()).astimezone(zone)
    candidate = base.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= base:
        candidate = (candidate + timedelta(days=1)).replace(hour=hour, minute=minute)
    return candidate.astimezone(timezone.utc)
