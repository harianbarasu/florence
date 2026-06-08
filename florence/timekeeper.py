"""Timezone-aware parsing and formatting for parent reminders."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


WEEKDAYS = {
    "monday": 0,
    "mon": 0,
    "tuesday": 1,
    "tue": 1,
    "tues": 1,
    "wednesday": 2,
    "wed": 2,
    "thursday": 3,
    "thu": 3,
    "thurs": 3,
    "friday": 4,
    "fri": 4,
    "saturday": 5,
    "sat": 5,
    "sunday": 6,
    "sun": 6,
}

MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


@dataclass(frozen=True, slots=True)
class ParsedDueTime:
    due_at_utc: datetime | None
    needs_clarification: bool
    reason: str
    matched_text: str | None = None


def resolve_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown timezone: {name}") from exc


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_due_time(text: str, timezone_name: str, *, now_utc: datetime | None = None) -> ParsedDueTime:
    """Parse common parent reminder phrases without silently returning past times."""

    zone = resolve_timezone(timezone_name)
    now = ensure_utc(now_utc or utc_now())
    local_now = now.astimezone(zone)
    raw = " ".join(text.strip().split())
    lower = raw.lower()

    ambiguous_clock = _ambiguous_clock_time(lower)
    if ambiguous_clock is not None:
        return ParsedDueTime(
            due_at_utc=None,
            needs_clarification=True,
            reason="ambiguous_clock_time",
            matched_text=ambiguous_clock,
        )

    relative = _parse_relative(lower, local_now)
    if relative is not None:
        return _future_result(relative, now, lower)

    explicit = _parse_iso(raw, zone) or _parse_month_day(lower, local_now, zone)
    if explicit is not None:
        return _future_result(explicit, now, lower)

    candidate_date = None
    if "day after tomorrow" in lower:
        candidate_date = local_now.date() + timedelta(days=2)
    elif "tomorrow" in lower:
        candidate_date = local_now.date() + timedelta(days=1)
    elif "today" in lower:
        candidate_date = local_now.date()
    else:
        weekday = _find_weekday(lower)
        if weekday is not None:
            days = (weekday - local_now.weekday()) % 7
            if days == 0 or f"next {_weekday_name(weekday)}" in lower:
                days += 7
            candidate_date = local_now.date() + timedelta(days=days)

    parsed_time = _parse_clock_time(lower) or _parse_daypart_time(lower)
    if candidate_date is None and parsed_time is not None:
        candidate_date = local_now.date()

    if candidate_date is None:
        return ParsedDueTime(
            due_at_utc=None,
            needs_clarification=True,
            reason="no_due_time_found",
        )

    local_time = parsed_time or time(9, 0)
    local_due = datetime.combine(candidate_date, local_time, tzinfo=zone)
    return _future_result(local_due, now, lower)


def format_local(dt_utc: datetime, timezone_name: str) -> str:
    zone = resolve_timezone(timezone_name)
    local = ensure_utc(dt_utc).astimezone(zone)
    return local.strftime("%a, %b %-d at %-I:%M %p")


def _parse_relative(lower: str, local_now: datetime) -> datetime | None:
    match = re.search(r"\bin\s+(\d+)\s+(minute|minutes|hour|hours|day|days|week|weeks)\b", lower)
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2)
    if unit.startswith("minute"):
        return local_now + timedelta(minutes=amount)
    if unit.startswith("hour"):
        return local_now + timedelta(hours=amount)
    if unit.startswith("day"):
        return local_now + timedelta(days=amount)
    return local_now + timedelta(weeks=amount)


def _parse_iso(raw: str, zone: ZoneInfo) -> datetime | None:
    match = re.search(r"\b(\d{4}-\d{2}-\d{2})(?:[ t](\d{1,2}:\d{2})(?::\d{2})?)?\b", raw)
    if not match:
        return None
    date_part = match.group(1)
    time_part = match.group(2) or "09:00"
    try:
        return datetime.fromisoformat(f"{date_part}T{time_part}").replace(tzinfo=zone)
    except ValueError:
        return None


def _parse_month_day(lower: str, local_now: datetime, zone: ZoneInfo) -> datetime | None:
    month_names = "|".join(MONTHS)
    match = re.search(
        rf"\b({month_names})\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,\s*(\d{{4}}))?\b",
        lower,
    )
    if not match:
        return None
    month = MONTHS[match.group(1).rstrip(".")]
    day = int(match.group(2))
    year = int(match.group(3) or local_now.year)
    parsed_time = _parse_clock_time(lower) or time(9, 0)
    try:
        local_due = datetime(year, month, day, parsed_time.hour, parsed_time.minute, tzinfo=zone)
    except ValueError:
        return None
    if local_due <= local_now and match.group(3) is None:
        try:
            local_due = local_due.replace(year=year + 1)
        except ValueError:
            return None
    return local_due


def _parse_clock_time(lower: str) -> time | None:
    if re.search(r"\bnoon\b", lower):
        return time(12, 0)
    if re.search(r"\bmidnight\b", lower):
        return time(0, 0)

    match = re.search(r"\b(?:at|by|around)?\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", lower)
    if not match:
        match = re.search(r"\b(\d{1,2}):(\d{2})\b", lower)
        if not match:
            match_24h = re.search(
                r"\b(?:at|by|around|before|after)\s+(1[3-9]|2[0-3])\b",
                lower,
            )
            if match_24h:
                return time(int(match_24h.group(1)), 0)
            return None
        hour = int(match.group(1))
        minute = int(match.group(2))
        if hour > 23 or minute > 59:
            return None
        return time(hour, minute)

    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    meridiem = match.group(3)
    if hour < 1 or hour > 12 or minute > 59:
        return None
    if meridiem == "pm" and hour != 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    return time(hour, minute)


def _ambiguous_clock_time(lower: str) -> str | None:
    for match in re.finditer(
        r"\b(?:at|by|around|before|after)\s+(\d{1,2})(?::(\d{2}))?(?!:)",
        lower,
    ):
        tail = lower[match.end() :]
        if re.match(r"\s*(?:am|pm)\b", tail):
            continue
        hour = int(match.group(1))
        minute = int(match.group(2) or "0")
        if 1 <= hour <= 12 and 0 <= minute <= 59:
            return match.group(0)
    return None


def _parse_daypart_time(lower: str) -> time | None:
    if re.search(r"\bmorning\b", lower):
        return time(8, 0)
    if re.search(r"\bafter\s+school\b", lower):
        return time(15, 0)
    if re.search(r"\bafternoon\b", lower):
        return time(15, 0)
    if re.search(r"\bevening\b", lower):
        return time(18, 0)
    if re.search(r"\b(?:tonight|night)\b", lower):
        return time(19, 0)
    return None


def _find_weekday(lower: str) -> int | None:
    for name, value in WEEKDAYS.items():
        if re.search(rf"\b{name}\b", lower):
            return value
    return None


def _weekday_name(index: int) -> str:
    for name, value in WEEKDAYS.items():
        if value == index and len(name) > 3:
            return name
    return ""


def _future_result(local_due: datetime, now_utc: datetime, matched_text: str) -> ParsedDueTime:
    due_utc = ensure_utc(local_due)
    if due_utc <= now_utc + timedelta(minutes=1):
        return ParsedDueTime(
            due_at_utc=None,
            needs_clarification=True,
            reason="due_time_is_in_the_past",
            matched_text=matched_text,
        )
    return ParsedDueTime(
        due_at_utc=due_utc,
        needs_clarification=False,
        reason="ok",
        matched_text=matched_text,
    )
