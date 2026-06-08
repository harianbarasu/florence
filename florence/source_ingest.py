"""Typed connected-source normalization.

The connected account boundary should produce small, typed candidates. Florence
then applies the same Need-to-Know policy to every source instead of letting raw
email/calendar content leak directly into chat.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime

from florence.models import SourceItem
from florence.timekeeper import ensure_utc


MAX_SOURCE_TITLE_CHARS = 160
MAX_SOURCE_BODY_CHARS = 2000
MAX_SOURCE_SENDER_CHARS = 240


@dataclass(frozen=True, slots=True)
class EmailCandidate:
    household_id: str
    subject: str
    body: str
    sender: str
    received_at_utc: datetime
    external_id: str | None = None
    event_at_utc: datetime | None = None
    connected_account_id: str | None = None


@dataclass(frozen=True, slots=True)
class CalendarEventCandidate:
    household_id: str
    title: str
    starts_at_utc: datetime
    ends_at_utc: datetime | None = None
    location: str | None = None
    description: str | None = None
    calendar_name: str | None = None
    external_id: str | None = None
    observed_at_utc: datetime | None = None
    connected_account_id: str | None = None


def email_to_source_item(candidate: EmailCandidate) -> SourceItem:
    subject = normalize_source_title(candidate.subject) or "(No subject)"
    body = normalize_source_body(candidate.body)
    sender = normalize_source_sender(candidate.sender)
    external_id = _account_scoped_external_id(
        candidate.connected_account_id,
        candidate.external_id,
    ) or _stable_id(
        "email",
        candidate.household_id,
        candidate.connected_account_id or "",
        subject,
        sender,
        ensure_utc(candidate.received_at_utc).isoformat(),
        body[:500],
    )
    return SourceItem(
        id=_stable_id("source", candidate.household_id, "email", external_id),
        household_id=candidate.household_id,
        source_type="email",
        title=subject,
        body=body,
        sender=sender,
        external_id=external_id,
        connected_account_id=candidate.connected_account_id,
        observed_at_utc=ensure_utc(candidate.received_at_utc),
        event_at_utc=ensure_utc(candidate.event_at_utc) if candidate.event_at_utc else None,
    )


def calendar_event_to_source_item(candidate: CalendarEventCandidate) -> SourceItem:
    title = normalize_source_title(candidate.title) or "Calendar event"
    details = []
    if candidate.location:
        details.append(f"Location: {_compact(candidate.location, limit=300)}")
    if candidate.calendar_name:
        details.append(f"Calendar: {normalize_source_sender(candidate.calendar_name)}")
    if candidate.description:
        details.append(_compact(candidate.description, limit=MAX_SOURCE_BODY_CHARS))
    body = normalize_source_body("\n".join(part for part in details if part))
    starts_at = ensure_utc(candidate.starts_at_utc)
    external_id = _account_scoped_external_id(
        candidate.connected_account_id,
        candidate.external_id,
    ) or _stable_id(
        "calendar",
        candidate.household_id,
        candidate.connected_account_id or "",
        title,
        starts_at.isoformat(),
        body[:500],
    )
    return SourceItem(
        id=_stable_id("source", candidate.household_id, "calendar", external_id),
        household_id=candidate.household_id,
        source_type="calendar",
        title=title,
        body=body,
        sender=candidate.calendar_name,
        external_id=external_id,
        connected_account_id=candidate.connected_account_id,
        observed_at_utc=ensure_utc(candidate.observed_at_utc or starts_at),
        event_at_utc=starts_at,
    )


def _stable_id(*parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return digest[:32]


def normalize_source_title(value: str | None) -> str:
    return _compact(value, limit=MAX_SOURCE_TITLE_CHARS)


def normalize_source_body(value: str | None) -> str:
    return _compact(value, limit=MAX_SOURCE_BODY_CHARS)


def normalize_source_sender(value: str | None) -> str:
    return _compact(value, limit=MAX_SOURCE_SENDER_CHARS)


def _compact(value: str | None, *, limit: int | None = None) -> str:
    if not value:
        return ""
    compacted = " ".join(value.split())
    if limit is None or len(compacted) <= limit:
        return compacted
    return compacted[:limit].rstrip()


def _account_scoped_external_id(connected_account_id: str | None, external_id: str | None) -> str | None:
    if not external_id:
        return None
    if not connected_account_id:
        return external_id
    return f"{connected_account_id}:{external_id}"
