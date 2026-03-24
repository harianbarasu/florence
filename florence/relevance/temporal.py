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
        r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*(?:-|to|until)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b",
        source,
        flags=re.IGNORECASE,
    )
    if explicit:
        return ParsedTimeRange(
            start=ParsedTime(_normalize_hours(int(explicit.group(1)), explicit.group(3)), int(explicit.group(2) or 0)),
            end=ParsedTime(_normalize_hours(int(explicit.group(4)), explicit.group(6)), int(explicit.group(5) or 0)),
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
