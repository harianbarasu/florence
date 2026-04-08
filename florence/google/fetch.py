"""Google Gmail and Calendar fetch helpers for Florence."""

from __future__ import annotations

import base64
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import unescape
from typing import Any
from urllib.parse import quote

import httpx

from florence.contracts import HouseholdEvent
from florence.media.openai_extract import (
    extract_image_text_with_openai,
    extract_pdf_text_with_openai,
)
from florence.google.types import GmailSyncItem, GoogleCalendarMetadata, ParentCalendarSyncItem

logger = logging.getLogger(__name__)

_GMAIL_ATTACHMENT_PARSE_LIMIT = 5
_GMAIL_TEXT_ATTACHMENT_MAX_BYTES = 256_000
_GMAIL_PDF_ATTACHMENT_MAX_BYTES = 8_000_000
_GMAIL_IMAGE_ATTACHMENT_MAX_BYTES = 8_000_000
_GMAIL_PDF_EXTRACTION_MAX_CHARS = 5_000
_GMAIL_IMAGE_EXTRACTION_MAX_CHARS = 5_000
_GMAIL_ATTACHMENT_TEXT_MAX_CHARS = 8_000


@dataclass(slots=True)
class _GmailAttachmentPart:
    mime_type: str
    filename: str | None
    attachment_id: str | None
    inline_data: str | None


def _base64url_decode_utf8(value: str | None) -> str:
    if not value:
        return ""
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding).decode("utf-8", errors="ignore")


def _base64url_decode_bytes(value: str | None) -> bytes:
    if not value:
        return b""
    padding = "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode(value + padding)
    except Exception:
        return b""


def _strip_html_tags(html: str) -> str:
    with_breaks = re.sub(r"(?i)<\s*br\s*/?>", "\n", html)
    with_breaks = re.sub(r"(?i)</\s*(p|div|li|tr|td|th|h[1-6]|ul|ol|table)\s*>", "\n", with_breaks)
    stripped = re.sub(r"<[^>]+>", " ", with_breaks)
    return _normalize_preserving_line_structure(unescape(stripped))


def _normalize_maybe_html_text(value: str, mime_type: str | None) -> str:
    if not value:
        return ""
    normalized_mime = (mime_type or "").lower()
    if "html" in normalized_mime or re.search(r"</?[a-z][\s\S]*>", value, re.IGNORECASE):
        return _strip_html_tags(value)
    return _normalize_preserving_line_structure(value)


def _compact_text(raw: str, max_length: int = 8_000) -> str:
    normalized = _normalize_preserving_line_structure(raw)
    if len(normalized) <= max_length:
        return normalized
    return f"{normalized[: max_length - 1].rstrip()}..."


def _normalize_preserving_line_structure(raw: str) -> str:
    text = raw.replace("\r\n", "\n").replace("\r", "\n")
    normalized_lines: list[str] = []
    blank_pending = False
    for line in text.split("\n"):
        cleaned = " ".join(line.split())
        if not cleaned:
            if normalized_lines and not blank_pending:
                normalized_lines.append("")
            blank_pending = True
            continue
        blank_pending = False
        normalized_lines.append(cleaned)

    while normalized_lines and not normalized_lines[-1]:
        normalized_lines.pop()
    return "\n".join(normalized_lines).strip()


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


def _collect_attachment_parts(
    part: dict[str, Any] | None,
    *,
    attachments: list[_GmailAttachmentPart],
) -> None:
    if not part:
        return

    body = part.get("body") or {}
    filename = str(part.get("filename")).strip() if part.get("filename") else None
    attachment_id = str(body.get("attachmentId")).strip() if body.get("attachmentId") else None
    inline_data = str(body.get("data")) if body.get("data") is not None else None

    if filename or attachment_id:
        attachments.append(
            _GmailAttachmentPart(
                mime_type=str(part.get("mimeType") or "").lower(),
                filename=filename or None,
                attachment_id=attachment_id or None,
                inline_data=inline_data or None,
            )
        )

    for child in part.get("parts") or []:
        if isinstance(child, dict):
            _collect_attachment_parts(child, attachments=attachments)


def _extract_pdf_attachment_text_with_gpt(
    *,
    pdf_bytes: bytes,
    filename: str | None,
) -> str | None:
    return extract_pdf_text_with_openai(
        pdf_bytes=pdf_bytes,
        filename=filename,
        api_key_env_names=("FLORENCE_GMAIL_PDF_OPENAI_API_KEY", "OPENAI_API_KEY"),
        base_url_env_name="FLORENCE_GMAIL_PDF_OPENAI_BASE_URL",
        model_env_name="FLORENCE_GMAIL_PDF_MODEL",
        log_label="OpenAI Gmail PDF attachment extraction",
        system_text=(
            "Extract plain text details from this PDF attachment. Preserve dates, times, names, "
            "locations, fees, deadlines, and required items when present. Keep it concise."
        ),
        max_output_chars=_GMAIL_PDF_EXTRACTION_MAX_CHARS,
    )


def _extract_image_attachment_text_with_gpt(
    *,
    image_bytes: bytes,
    mime_type: str,
    filename: str | None,
) -> str | None:
    return extract_image_text_with_openai(
        image_bytes=image_bytes,
        mime_type=mime_type,
        filename=filename,
        api_key_env_names=("FLORENCE_GMAIL_PDF_OPENAI_API_KEY", "OPENAI_API_KEY"),
        base_url_env_name="FLORENCE_GMAIL_PDF_OPENAI_BASE_URL",
        model_env_name="FLORENCE_GMAIL_PDF_MODEL",
        log_label="OpenAI Gmail image attachment extraction",
        max_output_chars=_GMAIL_IMAGE_EXTRACTION_MAX_CHARS,
    )


def _download_gmail_attachment_bytes(
    *,
    access_token: str,
    message_id: str,
    attachment_id: str,
    timeout_seconds: float,
) -> bytes:
    attachment_url = (
        f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}/attachments/{attachment_id}"
    )
    response = httpx.get(
        attachment_url,
        headers={"authorization": f"Bearer {access_token}"},
        timeout=timeout_seconds,
    )
    payload = response.json()
    response.raise_for_status()
    if not isinstance(payload, dict):
        return b""
    return _base64url_decode_bytes(payload.get("data"))


def _load_gmail_attachment_bytes(
    part: _GmailAttachmentPart,
    *,
    access_token: str,
    message_id: str,
    timeout_seconds: float,
) -> bytes:
    inline = _base64url_decode_bytes(part.inline_data)
    if inline:
        return inline
    if not part.attachment_id:
        return b""
    return _download_gmail_attachment_bytes(
        access_token=access_token,
        message_id=message_id,
        attachment_id=part.attachment_id,
        timeout_seconds=timeout_seconds,
    )


def _extract_attachment_part_text(part: _GmailAttachmentPart, *, content: bytes) -> str | None:
    if not content:
        return None

    filename = (part.filename or "").lower()
    mime_type = part.mime_type
    if (
        mime_type.startswith("text/plain")
        or mime_type.startswith("text/html")
        or filename.endswith(".txt")
        or filename.endswith(".html")
        or filename.endswith(".htm")
    ):
        decoded = content[:_GMAIL_TEXT_ATTACHMENT_MAX_BYTES].decode("utf-8", errors="ignore")
        normalized = _normalize_maybe_html_text(decoded, mime_type)
        return _compact_text(normalized, max_length=2_500) if normalized else None

    if mime_type == "application/pdf" or filename.endswith(".pdf"):
        if len(content) > _GMAIL_PDF_ATTACHMENT_MAX_BYTES:
            logger.warning(
                "Skipping oversized Gmail PDF attachment %s (%d bytes)",
                part.filename or "attachment.pdf",
                len(content),
            )
            return None
        return _extract_pdf_attachment_text_with_gpt(pdf_bytes=content, filename=part.filename)

    if mime_type.startswith("image/") or filename.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".heic", ".heif")):
        if len(content) > _GMAIL_IMAGE_ATTACHMENT_MAX_BYTES:
            logger.warning(
                "Skipping oversized Gmail image attachment %s (%d bytes)",
                part.filename or "image",
                len(content),
            )
            return None
        final_mime = mime_type if mime_type.startswith("image/") else "image/jpeg"
        return _extract_image_attachment_text_with_gpt(
            image_bytes=content,
            mime_type=final_mime,
            filename=part.filename,
        )

    return None


def _extract_message_attachment_text(
    payload: dict[str, Any] | None,
    *,
    access_token: str,
    message_id: str,
    timeout_seconds: float,
) -> str | None:
    if not payload:
        return None

    attachments: list[_GmailAttachmentPart] = []
    _collect_attachment_parts(payload, attachments=attachments)
    if not attachments:
        return None

    snippets: list[str] = []
    for part in attachments[:_GMAIL_ATTACHMENT_PARSE_LIMIT]:
        content = _load_gmail_attachment_bytes(
            part,
            access_token=access_token,
            message_id=message_id,
            timeout_seconds=timeout_seconds,
        )
        text = _extract_attachment_part_text(part, content=content)
        if not text:
            continue
        label = part.filename or part.mime_type or "attachment"
        snippets.append(f"{label}: {text}")

    if not snippets:
        return None
    return _compact_text("\n\n".join(snippets), max_length=_GMAIL_ATTACHMENT_TEXT_MAX_CHARS)


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


def build_gmail_sync_item(
    message: dict[str, Any],
    *,
    attachment_text: str | None = None,
    attachment_count: int | None = None,
) -> GmailSyncItem:
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
        attachment_text=attachment_text,
        attachment_count=_count_attachment_parts(payload) if attachment_count is None else attachment_count,
        received_at=received_at,
    )


def list_recent_gmail_sync_items(
    *,
    access_token: str,
    max_results: int | None = None,
    gmail_query: str | None = None,
    timeout_seconds: float = 30.0,
) -> list[GmailSyncItem]:
    message_refs: list[dict[str, Any]] = []
    page_token: str | None = None
    page_size = min(100, max_results) if max_results else 100

    while True:
        list_url = "https://gmail.googleapis.com/gmail/v1/users/me/messages"
        normalized_query = " ".join(str(gmail_query or "").split()).strip()
        if normalized_query:
            query = normalized_query
            lowered_query = normalized_query.lower()
            if "-in:trash" not in lowered_query:
                query = f"{query} -in:trash"
            if "-in:spam" not in lowered_query:
                query = f"{query} -in:spam"
        else:
            query = "newer_than:90d -in:trash -in:spam"
        params = {
            "maxResults": str(page_size),
            "q": query,
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
            message_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else None
            count = _count_attachment_parts(message_payload)
            attachment_text = (
                _extract_message_attachment_text(
                    message_payload,
                    access_token=access_token,
                    message_id=str(message_id),
                    timeout_seconds=timeout_seconds,
                )
                if count > 0
                else None
            )
            results.append(
                build_gmail_sync_item(
                    payload,
                    attachment_text=attachment_text,
                    attachment_count=count,
                )
            )

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
        calendar_id=calendar.id,
        calendar_primary=calendar.primary,
    )


def list_recent_parent_calendar_sync_items(
    *,
    access_token: str,
    calendar: GoogleCalendarMetadata,
    family_member_names: list[str],
    max_results: int = 20,
    window_days: int | None = None,
    past_window_days: int | None = None,
    future_window_days: int | None = None,
    timeout_seconds: float = 30.0,
    now: datetime | None = None,
) -> list[ParentCalendarSyncItem]:
    anchor_time = now or datetime.now(timezone.utc)
    resolved_past_window_days = max(0, past_window_days or 0)
    resolved_future_window_days = max(0, future_window_days if future_window_days is not None else (window_days or 30))
    start_time = anchor_time - timedelta(days=resolved_past_window_days)
    end_time = anchor_time + timedelta(days=resolved_future_window_days)
    events_url = f"https://www.googleapis.com/calendar/v3/calendars/{quote(calendar.id, safe='')}/events"
    results: list[ParentCalendarSyncItem] = []
    page_token: str | None = None
    while len(results) < max_results:
        response = httpx.get(
            events_url,
            params={
                "singleEvents": "true",
                "orderBy": "startTime",
                "maxResults": str(min(250, max_results - len(results))),
                "timeMin": start_time.isoformat(),
                "timeMax": end_time.isoformat(),
                **({"pageToken": page_token} if page_token else {}),
            },
            headers={"authorization": f"Bearer {access_token}"},
            timeout=timeout_seconds,
        )
        payload = response.json()
        response.raise_for_status()
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
            if len(results) >= max_results:
                break
        page_token = payload.get("nextPageToken")
        if not page_token:
            break

    return results


def create_google_calendar(
    *,
    access_token: str,
    summary: str,
    timezone: str,
    timeout_seconds: float = 30.0,
) -> GoogleCalendarMetadata:
    response = httpx.post(
        "https://www.googleapis.com/calendar/v3/calendars",
        headers={"authorization": f"Bearer {access_token}"},
        json={"summary": summary, "timeZone": timezone},
        timeout=timeout_seconds,
    )
    payload = response.json()
    response.raise_for_status()
    return GoogleCalendarMetadata(
        id=str(payload.get("id") or ""),
        summary=str(payload.get("summary") or summary),
        timezone=str(payload.get("timeZone") or timezone),
        access_role=str(payload.get("accessRole") or "") or None,
        primary=bool(payload.get("primary")),
        selected=bool(payload.get("selected", True)),
        hidden=bool(payload.get("hidden", False)),
    )


def build_google_calendar_web_url(*, calendar_id: str) -> str:
    """Return the Google Calendar web UI URL for an existing calendar id."""

    return f"https://calendar.google.com/calendar/u/0/r?cid={quote(calendar_id, safe='')}"


def share_google_calendar_with_user(
    *,
    access_token: str,
    calendar_id: str,
    user_email: str,
    role: str = "reader",
    send_notifications: bool = False,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    response = httpx.post(
        f"https://www.googleapis.com/calendar/v3/calendars/{quote(calendar_id, safe='')}/acl",
        headers={"authorization": f"Bearer {access_token}"},
        params={"sendNotifications": "true" if send_notifications else "false"},
        json={
            "role": role,
            "scope": {
                "type": "user",
                "value": user_email,
            },
        },
        timeout=timeout_seconds,
    )
    if response.status_code == 409:
        return {
            "calendar_id": calendar_id,
            "user_email": user_email,
            "role": role,
            "already_exists": True,
        }
    payload = response.json()
    response.raise_for_status()
    return payload if isinstance(payload, dict) else {}


def add_google_calendar_to_calendar_list(
    *,
    access_token: str,
    calendar_id: str,
    selected: bool = True,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    response = httpx.post(
        "https://www.googleapis.com/calendar/v3/users/me/calendarList",
        headers={"authorization": f"Bearer {access_token}"},
        json={
            "id": calendar_id,
            "selected": selected,
        },
        timeout=timeout_seconds,
    )
    if response.status_code == 409:
        return {
            "id": calendar_id,
            "selected": selected,
            "already_exists": True,
        }
    payload = response.json()
    response.raise_for_status()
    return payload if isinstance(payload, dict) else {}


def _google_calendar_event_payload(event: HouseholdEvent) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "summary": event.title,
        "location": event.location,
        "description": event.description,
    }
    if event.all_day and event.starts_at and event.ends_at:
        payload["start"] = {"date": str(event.starts_at)[:10]}
        payload["end"] = {"date": str(event.ends_at)[:10]}
        return payload
    payload["start"] = {"dateTime": event.starts_at, "timeZone": event.timezone}
    payload["end"] = {"dateTime": event.ends_at, "timeZone": event.timezone}
    return payload


def upsert_google_calendar_event(
    *,
    access_token: str,
    calendar_id: str,
    event: HouseholdEvent,
    google_event_id: str | None = None,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    encoded_calendar_id = quote(calendar_id, safe="")
    payload = _google_calendar_event_payload(event)
    if google_event_id:
        response = httpx.put(
            f"https://www.googleapis.com/calendar/v3/calendars/{encoded_calendar_id}/events/{quote(google_event_id, safe='')}",
            headers={"authorization": f"Bearer {access_token}"},
            json=payload,
            timeout=timeout_seconds,
        )
    else:
        response = httpx.post(
            f"https://www.googleapis.com/calendar/v3/calendars/{encoded_calendar_id}/events",
            headers={"authorization": f"Bearer {access_token}"},
            json=payload,
            timeout=timeout_seconds,
        )
    result = response.json()
    response.raise_for_status()
    return dict(result) if isinstance(result, dict) else {}


def delete_google_calendar_event(
    *,
    access_token: str,
    calendar_id: str,
    google_event_id: str,
    timeout_seconds: float = 30.0,
) -> None:
    response = httpx.delete(
        f"https://www.googleapis.com/calendar/v3/calendars/{quote(calendar_id, safe='')}/events/{quote(google_event_id, safe='')}",
        headers={"authorization": f"Bearer {access_token}"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
