"""Google Gmail and Calendar fetch helpers for Florence."""

from __future__ import annotations

import base64
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import httpx

from florence.google.types import GmailSyncItem, GoogleCalendarMetadata, ParentCalendarSyncItem


def _base64url_decode_utf8(value: str | None) -> str:
    if not value:
        return ""
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding).decode("utf-8", errors="ignore")


def _strip_html_tags(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()


def _normalize_maybe_html_text(value: str, mime_type: str | None) -> str:
    if not value:
        return ""
    normalized_mime = (mime_type or "").lower()
    if "html" in normalized_mime or re.search(r"</?[a-z][\s\S]*>", value, re.IGNORECASE):
        return _strip_html_tags(value)
    return value.replace("\r\n", "\n").strip()


def _compact_text(raw: str, max_length: int = 8_000) -> str:
    normalized = " ".join(raw.split())
    if len(normalized) <= max_length:
        return normalized
    return f"{normalized[: max_length - 1].rstrip()}..."


def _read_gmail_header(headers: list[dict[str, Any]] | None, name: str) -> str:
    lowered_name = name.lower()
    for header in headers or []:
        if str(header.get("name") or "").lower() == lowered_name:
            return str(header.get("value") or "")
    return ""


def _collect_message_body_parts(
    part: dict[str, Any] | None,
    *,
    plain_text: list[str],
    html_text: list[str],
) -> None:
    if not part:
        return

    mime_type = str(part.get("mimeType") or "").lower()
    body = part.get("body") or {}
    body_text = _base64url_decode_utf8(body.get("data"))
    if body_text:
        if mime_type.startswith("text/plain"):
            plain_text.append(_normalize_maybe_html_text(body_text, mime_type))
        elif mime_type.startswith("text/html"):
            html_text.append(_normalize_maybe_html_text(body_text, mime_type))

    for child in part.get("parts") or []:
        if isinstance(child, dict):
            _collect_message_body_parts(child, plain_text=plain_text, html_text=html_text)


def _count_attachment_parts(part: dict[str, Any] | None) -> int:
    if not part:
        return 0

    body = part.get("body") or {}
    has_attachment = bool(part.get("filename")) or bool(body.get("attachmentId"))
    child_total = 0
    for child in part.get("parts") or []:
        if isinstance(child, dict):
            child_total += _count_attachment_parts(child)
    return (1 if has_attachment else 0) + child_total


def _extract_message_body_text(payload: dict[str, Any] | None) -> str | None:
    if not payload:
        return None

    plain_text: list[str] = []
    html_text: list[str] = []
    _collect_message_body_parts(payload, plain_text=plain_text, html_text=html_text)
    preferred = "\n\n".join(part for part in plain_text if part).strip()
    fallback = "\n\n".join(part for part in html_text if part).strip()
    if not preferred and not fallback:
        return None
    return _compact_text(preferred or fallback)


def build_gmail_sync_item(message: dict[str, Any]) -> GmailSyncItem:
    payload = message.get("payload") if isinstance(message.get("payload"), dict) else None
    internal_date = message.get("internalDate")
    received_at = None
    if internal_date is not None:
        try:
            received_at = datetime.fromtimestamp(int(str(internal_date)) / 1000, tz=timezone.utc)
        except (TypeError, ValueError):
            received_at = None

    return GmailSyncItem(
        gmail_message_id=str(message.get("id") or ""),
        thread_id=str(message.get("threadId")) if message.get("threadId") is not None else None,
        from_address=_read_gmail_header(payload.get("headers") if payload else None, "From"),
        subject=_read_gmail_header(payload.get("headers") if payload else None, "Subject") or "Untitled Gmail message",
        snippet=str(message.get("snippet")) if message.get("snippet") is not None else None,
        body_text=_extract_message_body_text(payload),
        attachment_text=None,
        attachment_count=_count_attachment_parts(payload),
        received_at=received_at,
    )


def list_recent_gmail_sync_items(
    *,
    access_token: str,
    max_results: int | None = None,
    timeout_seconds: float = 30.0,
) -> list[GmailSyncItem]:
    message_refs: list[dict[str, Any]] = []
    page_token: str | None = None
    page_size = min(100, max_results) if max_results else 100

    while True:
        list_url = "https://gmail.googleapis.com/gmail/v1/users/me/messages"
        params = {
            "maxResults": str(page_size),
            "q": "newer_than:90d -in:trash -in:spam",
        }
        if page_token:
            params["pageToken"] = page_token
        response = httpx.get(
            list_url,
            params=params,
            headers={"authorization": f"Bearer {access_token}"},
            timeout=timeout_seconds,
        )
        payload = response.json()
        response.raise_for_status()

        message_refs.extend(payload.get("messages") or [])
        page_token = payload.get("nextPageToken")
        if not page_token:
            break
        if max_results and len(message_refs) >= max_results:
            break

    if max_results:
        message_refs = message_refs[:max_results]

    results: list[GmailSyncItem] = []
    for ref in message_refs:
        message_id = ref.get("id")
        if not message_id:
            continue

        detail_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}"
        response = httpx.get(
            detail_url,
            params={"format": "full"},
            headers={"authorization": f"Bearer {access_token}"},
            timeout=timeout_seconds,
        )
        payload = response.json()
        response.raise_for_status()
        if isinstance(payload, dict):
            results.append(build_gmail_sync_item(payload))

    return results


def _parse_google_calendar_event_datetime(
    value: dict[str, Any] | None,
    fallback_timezone: str,
) -> tuple[datetime, str, bool] | None:
    if not value:
        return None

    date_time = value.get("dateTime")
    if isinstance(date_time, str):
        try:
            return datetime.fromisoformat(date_time.replace("Z", "+00:00")), str(value.get("timeZone") or fallback_timezone), False
        except ValueError:
            return None

    date_only = value.get("date")
    if isinstance(date_only, str):
        try:
            parsed = datetime.fromisoformat(f"{date_only}T00:00:00+00:00")
        except ValueError:
            return None
        return parsed, str(value.get("timeZone") or fallback_timezone), True

    return None


def build_parent_calendar_sync_item(
    event: dict[str, Any],
    *,
    calendar: GoogleCalendarMetadata,
    family_member_names: list[str],
) -> ParentCalendarSyncItem | None:
    event_id = event.get("id")
    if not event_id or event.get("status") == "cancelled":
        return None

    start = _parse_google_calendar_event_datetime(event.get("start"), calendar.timezone)
    end = _parse_google_calendar_event_datetime(event.get("end"), start[1] if start else calendar.timezone)
    if not start or not end:
        return None

    updated_at = None
    raw_updated = event.get("updated")
    if isinstance(raw_updated, str):
        try:
            updated_at = datetime.fromisoformat(raw_updated.replace("Z", "+00:00"))
        except ValueError:
            updated_at = None

    return ParentCalendarSyncItem(
        google_event_id=str(event_id),
        title=str(event.get("summary") or "Untitled calendar event").strip(),
        description=str(event.get("description")).strip() if event.get("description") is not None else None,
        location=str(event.get("location")).strip() if event.get("location") is not None else None,
        html_link=str(event.get("htmlLink")) if event.get("htmlLink") is not None else None,
        starts_at=start[0],
        ends_at=end[0],
        timezone=start[1],
        all_day=start[2],
        updated_at=updated_at,
        calendar_summary=calendar.summary,
        family_member_names=[name for name in family_member_names if name.strip()],
    )


def list_recent_parent_calendar_sync_items(
    *,
    access_token: str,
    calendar: GoogleCalendarMetadata,
    family_member_names: list[str],
    max_results: int = 20,
    window_days: int = 30,
    timeout_seconds: float = 30.0,
    now: datetime | None = None,
) -> list[ParentCalendarSyncItem]:
    start_time = now or datetime.now(timezone.utc)
    end_time = start_time + timedelta(days=window_days)
    events_url = f"https://www.googleapis.com/calendar/v3/calendars/{quote(calendar.id, safe='')}/events"
    response = httpx.get(
        events_url,
        params={
            "singleEvents": "true",
            "orderBy": "startTime",
            "maxResults": str(max_results),
            "timeMin": start_time.isoformat(),
            "timeMax": end_time.isoformat(),
        },
        headers={"authorization": f"Bearer {access_token}"},
        timeout=timeout_seconds,
    )
    payload = response.json()
    response.raise_for_status()

    results: list[ParentCalendarSyncItem] = []
    for event in payload.get("items") or []:
        if not isinstance(event, dict):
            continue
        sync_item = build_parent_calendar_sync_item(
            event,
            calendar=calendar,
            family_member_names=family_member_names,
        )
        if sync_item is not None:
            results.append(sync_item)

    return results
