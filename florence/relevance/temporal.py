"""Small temporal parsing helpers for Florence candidate scoring."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


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

WEEKDAYS = {
    "sunday": 6,
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
}


@dataclass(slots=True)
class ParsedExplicitDate:
    match: str
    value: date


@dataclass(slots=True)
class ParsedTime:
    hours: int
    minutes: int


@dataclass(slots=True)
class ParsedTimeRange:
    start: ParsedTime
    end: ParsedTime


def add_days(value: date, days: int) -> date:
    return value + timedelta(days=days)


def zoned_datetime_to_utc(value: date, hours: int, minutes: int, time_zone: str) -> datetime:
    local = datetime.combine(value, time(hour=hours, minute=minutes), tzinfo=ZoneInfo(time_zone))
    return local.astimezone(timezone.utc)


def _safe_date(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def parse_explicit_date(source: str, time_zone: str, now: datetime | None = None) -> ParsedExplicitDate | None:
    current = now.astimezone(ZoneInfo(time_zone)) if now else datetime.now(ZoneInfo(time_zone))
    today = current.date()

    relative = re.search(r"\b(today|tomorrow)\b", source, flags=re.IGNORECASE)
    if relative:
        label = relative.group(1).lower()
        return ParsedExplicitDate(relative.group(0), add_days(today, 1) if label == "tomorrow" else today)

    weekday = re.search(
        r"\b(sunday|monday|tuesday|wednesday|thursday|friday|saturday)\b",
        source,
        flags=re.IGNORECASE,
    )
    if weekday:
        target = WEEKDAYS[weekday.group(1).lower()]
        delta = (target - today.weekday()) % 7
        delta = delta or 7
        return ParsedExplicitDate(weekday.group(0), add_days(today, delta))

    iso = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", source)
    if iso:
        parsed = _safe_date(int(iso.group(1)), int(iso.group(2)), int(iso.group(3)))
        if parsed is not None:
            return ParsedExplicitDate(iso.group(0), parsed)

    slash = re.search(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b", source)
    if slash:
        year = int(slash.group(3)) if slash.group(3) else today.year
        if year < 100:
            year += 2000
        parsed = _safe_date(year, int(slash.group(1)), int(slash.group(2)))
        if parsed is not None:
            return ParsedExplicitDate(slash.group(0), parsed)

    month_name = re.search(r"\b([A-Za-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?\b", source)
    if month_name:
        month = MONTHS.get(month_name.group(1).lower())
        if month:
            year = int(month_name.group(3)) if month_name.group(3) else today.year
            parsed = _safe_date(year, month, int(month_name.group(2)))
            if parsed is not None:
                return ParsedExplicitDate(month_name.group(0), parsed)

    return None


def _normalize_hours(hours: int, meridiem: str | None) -> int:
    lowered = (meridiem or "").lower()
    if lowered == "pm" and hours < 12:
        return hours + 12
    if lowered == "am" and hours == 12:
        return 0
    return hours


def _time_total_minutes(hours: int, minutes: int) -> int:
    return hours * 60 + minutes


def _normalize_time_range_pair(
    *,
    start_hour: int,
    start_minute: int,
    start_meridiem: str | None,
    end_hour: int,
    end_minute: int,
    end_meridiem: str | None,
) -> ParsedTimeRange:
    inferred_start = (start_meridiem or end_meridiem or "").lower() or None
    inferred_end = (end_meridiem or start_meridiem or "").lower() or None
    normalized_start = ParsedTime(_normalize_hours(start_hour, inferred_start), start_minute)
    normalized_end = ParsedTime(_normalize_hours(end_hour, inferred_end), end_minute)

    # When only one side carried am/pm and the naive propagation yields a backwards range,
    # try flipping the missing side before giving up. This handles common strings like
    # "11:30-1 pm" or "3:30 PM-4:15".
    if _time_total_minutes(normalized_end.hours, normalized_end.minutes) <= _time_total_minutes(normalized_start.hours, normalized_start.minutes):
        if start_meridiem and not end_meridiem:
            alternate_end_meridiem = "am" if inferred_start == "pm" else "pm"
            alternate_end = ParsedTime(_normalize_hours(end_hour, alternate_end_meridiem), end_minute)
            if _time_total_minutes(alternate_end.hours, alternate_end.minutes) > _time_total_minutes(normalized_start.hours, normalized_start.minutes):
                normalized_end = alternate_end
        elif end_meridiem and not start_meridiem:
            alternate_start_meridiem = "am" if inferred_end == "pm" else "pm"
            alternate_start = ParsedTime(_normalize_hours(start_hour, alternate_start_meridiem), start_minute)
            if _time_total_minutes(normalized_end.hours, normalized_end.minutes) > _time_total_minutes(alternate_start.hours, alternate_start.minutes):
                normalized_start = alternate_start

    return ParsedTimeRange(start=normalized_start, end=normalized_end)


def parse_single_time(source: str) -> ParsedTime | None:
    match = re.search(
        r"\b(?:at|starts?\s+at|begins?\s+at|arrive(?:s|)\s+at)\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b",
        source,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    return ParsedTime(
        hours=_normalize_hours(int(match.group(1)), match.group(3)),
        minutes=int(match.group(2) or 0),
    )


def parse_time_range(source: str) -> ParsedTimeRange | None:
    explicit = re.search(
        r"\b(?:from\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*(?:-|–|to|until)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b",
        source,
        flags=re.IGNORECASE,
    )
    if explicit and (explicit.group(3) or explicit.group(6)):
        return _normalize_time_range_pair(
            start_hour=int(explicit.group(1)),
            start_minute=int(explicit.group(2) or 0),
            start_meridiem=explicit.group(3),
            end_hour=int(explicit.group(4)),
            end_minute=int(explicit.group(5) or 0),
            end_meridiem=explicit.group(6),
        )

    starts_ends = re.search(
        r"\b(?:starts?|begins?|arrive(?:s|))\s+at\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b[\s\S]{0,80}\b(?:ends?|until)\s+at?\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b",
        source,
        flags=re.IGNORECASE,
    )
    if not starts_ends:
        return None

    return ParsedTimeRange(
        start=ParsedTime(_normalize_hours(int(starts_ends.group(1)), starts_ends.group(3)), int(starts_ends.group(2) or 0)),
        end=ParsedTime(_normalize_hours(int(starts_ends.group(4)), starts_ends.group(6)), int(starts_ends.group(5) or 0)),
    )
