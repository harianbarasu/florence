"""Minimal ICS calendar-feed parsing for Florence schedule ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


@dataclass(slots=True)
class CalendarFeedEvent:
    uid: str | None
    recurrence_id: str | None
    summary: str | None
    description: str | None
    location: str | None
    starts_at: str | None
    ends_at: str | None
    timezone: str | None
    all_day: bool


def _unfold_ics_lines(raw: str) -> list[str]:
    lines: list[str] = []
    for line in raw.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        if not lines:
            lines.append(line)
            continue
        if line.startswith((" ", "\t")):
            lines[-1] += line[1:]
        else:
            lines.append(line)
    return [line for line in lines if line]


def _unescape_ics_text(value: str) -> str:
    return (
        value.replace("\\n", "\n")
        .replace("\\N", "\n")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\\\", "\\")
    ).strip()


def _parse_content_line(line: str) -> tuple[str, dict[str, str], str] | None:
    if ":" not in line:
        return None
    left, raw_value = line.split(":", 1)
    parts = left.split(";")
    name = parts[0].strip().upper()
    params: dict[str, str] = {}
    for part in parts[1:]:
        if "=" not in part:
            params[part.strip().upper()] = ""
            continue
        key, value = part.split("=", 1)
        params[key.strip().upper()] = value.strip()
    return name, params, raw_value


def _parse_ics_datetime(
    value: str,
    *,
    params: dict[str, str],
    default_timezone: str,
) -> tuple[str | None, bool, str | None]:
    normalized = value.strip()
    if not normalized:
        return None, False, None

    value_kind = params.get("VALUE", "").upper()
    tz_name = params.get("TZID", "").strip() or default_timezone or "UTC"
    tzinfo = ZoneInfo(tz_name)

    if value_kind == "DATE" or (len(normalized) == 8 and normalized.isdigit()):
        date_value = datetime.strptime(normalized, "%Y%m%d")
        localized = date_value.replace(tzinfo=tzinfo)
        return localized.isoformat(), True, tz_name

    if normalized.endswith("Z"):
        parsed = datetime.strptime(normalized, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        return parsed.isoformat(), False, "UTC"

    time_formats = ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M")
    for time_format in time_formats:
        try:
            parsed = datetime.strptime(normalized, time_format)
            localized = parsed.replace(tzinfo=tzinfo)
            return localized.isoformat(), False, tz_name
        except ValueError:
            continue
    return None, False, None


def parse_calendar_feed(
    raw_text: str,
    *,
    default_timezone: str = "UTC",
) -> tuple[str | None, list[CalendarFeedEvent]]:
    """Parse an ICS feed into a calendar label plus normalized events.

    This intentionally handles the common subset Florence needs from public or
    shareable schedule feeds: VCALENDAR metadata plus VEVENT fields like UID,
    DTSTART, DTEND, SUMMARY, DESCRIPTION, and LOCATION.
    """

    calendar_summary: str | None = None
    current_event: dict[str, object] | None = None
    parsed_events: list[CalendarFeedEvent] = []

    for line in _unfold_ics_lines(raw_text):
        parsed = _parse_content_line(line)
        if parsed is None:
            continue
        name, params, raw_value = parsed
        if name == "X-WR-CALNAME" and not calendar_summary:
            calendar_summary = _unescape_ics_text(raw_value)
            continue
        if name == "BEGIN" and raw_value.strip().upper() == "VEVENT":
            current_event = {}
            continue
        if name == "END" and raw_value.strip().upper() == "VEVENT":
            if current_event is None:
                continue
            starts_at = current_event.get("starts_at")
            ends_at = current_event.get("ends_at")
            all_day = bool(current_event.get("all_day"))
            timezone_name = current_event.get("timezone")
            if starts_at and not ends_at:
                start_dt = datetime.fromisoformat(str(starts_at).replace("Z", "+00:00"))
                if all_day:
                    ends_at = (start_dt + timedelta(days=1)).isoformat()
                else:
                    ends_at = start_dt.isoformat()
            parsed_events.append(
                CalendarFeedEvent(
                    uid=str(current_event.get("uid") or "").strip() or None,
                    recurrence_id=str(current_event.get("recurrence_id") or "").strip() or None,
                    summary=str(current_event.get("summary") or "").strip() or None,
                    description=str(current_event.get("description") or "").strip() or None,
                    location=str(current_event.get("location") or "").strip() or None,
                    starts_at=str(starts_at).strip() if starts_at else None,
                    ends_at=str(ends_at).strip() if ends_at else None,
                    timezone=str(timezone_name).strip() if timezone_name else None,
                    all_day=all_day,
                )
            )
            current_event = None
            continue
        if current_event is None:
            continue
        if name == "UID":
            current_event["uid"] = _unescape_ics_text(raw_value)
        elif name == "RECURRENCE-ID":
            recurrence_id, _, _ = _parse_ics_datetime(
                raw_value,
                params=params,
                default_timezone=default_timezone,
            )
            current_event["recurrence_id"] = recurrence_id or _unescape_ics_text(raw_value)
        elif name == "SUMMARY":
            current_event["summary"] = _unescape_ics_text(raw_value)
        elif name == "DESCRIPTION":
            current_event["description"] = _unescape_ics_text(raw_value)
        elif name == "LOCATION":
            current_event["location"] = _unescape_ics_text(raw_value)
        elif name == "DTSTART":
            starts_at, all_day, timezone_name = _parse_ics_datetime(
                raw_value,
                params=params,
                default_timezone=default_timezone,
            )
            current_event["starts_at"] = starts_at
            current_event["all_day"] = all_day
            current_event["timezone"] = timezone_name
        elif name == "DTEND":
            ends_at, _, timezone_name = _parse_ics_datetime(
                raw_value,
                params=params,
                default_timezone=default_timezone,
            )
            current_event["ends_at"] = ends_at
            if timezone_name and not current_event.get("timezone"):
                current_event["timezone"] = timezone_name

    return calendar_summary, parsed_events
