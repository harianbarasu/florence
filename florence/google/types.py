"""Typed Google integration models for Florence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class FlorenceGoogleOauthState:
    household_id: str
    member_id: str
    thread_id: str | None
    nonce: str
    issued_at_ms: int


@dataclass(slots=True)
class GoogleTokenResponse:
    access_token: str
    refresh_token: str | None = None
    expires_in: int | None = None
    scope: str | None = None
    token_type: str | None = None


@dataclass(slots=True)
class GoogleCalendarMetadata:
    id: str
    summary: str
    timezone: str
    access_role: str | None = None


@dataclass(slots=True)
class GmailSyncItem:
    gmail_message_id: str
    thread_id: str | None
    from_address: str
    subject: str
    snippet: str | None
    body_text: str | None
    attachment_text: str | None
    attachment_count: int
    received_at: datetime | None


@dataclass(slots=True)
class ParentCalendarSyncItem:
    google_event_id: str
    title: str
    description: str | None
    location: str | None
    html_link: str | None
    starts_at: datetime
    ends_at: datetime
    timezone: str
    all_day: bool
    updated_at: datetime | None
    calendar_summary: str | None
    family_member_names: list[str]
