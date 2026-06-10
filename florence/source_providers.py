"""Provider adapters for connected source sync."""

from __future__ import annotations

import base64
import html
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from florence.config import Settings
from florence.models import ConnectedAccount, OutboundMessage
from florence.oauth import GOOGLE_TOKEN_ENDPOINT, TokenVault, TokenVaultError
from florence.timekeeper import ensure_utc

if TYPE_CHECKING:
    from florence.service import FlorenceService


@dataclass(frozen=True, slots=True)
class ProviderBatch:
    emails: list[dict[str, object]]
    calendar_events: list[dict[str, object]]
    cursor: str | None = None


class SourceProvider(Protocol):
    provider: str

    def fetch(self, account: ConnectedAccount, *, now_utc: datetime) -> ProviderBatch:
        ...


@dataclass(frozen=True, slots=True)
class SourceSyncRunResult:
    checked: int
    synced: int
    imported: int
    surfaced: int
    skipped: int
    failed: int
    delivery_attempted: int = 0
    delivery_sent: int = 0
    delivery_failed: int = 0
    messages: tuple[OutboundMessage, ...] = ()


class GoogleSourceProvider:
    """Google Gmail/Calendar fetcher backed by encrypted account tokens."""

    provider = "google"

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        store: object | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self._client = http_client or httpx.Client(timeout=20.0)

    def fetch(self, account: ConnectedAccount, *, now_utc: datetime) -> ProviderBatch:
        if self.settings is None or self.store is None or not self.settings.token_encryption_key:
            return ProviderBatch(emails=[], calendar_events=[], cursor=account.cursor)
        token_record = self.store.get_connected_account_token(account.id)
        if token_record is None:
            raise RuntimeError("missing google oauth token")
        vault = TokenVault.from_settings(self.settings)
        token_payload = vault.decrypt(token_record.token_ciphertext)
        access_token, expires_at = self._access_token(
            account=account,
            token_payload=token_payload,
            vault=vault,
            now_utc=ensure_utc(now_utc),
        )
        emails = self._fetch_gmail(account=account, access_token=access_token, now_utc=now_utc)
        calendar_timezone = self._calendar_timezone(account)
        calendar_events = self._fetch_calendar(
            access_token=access_token,
            now_utc=now_utc,
            timezone_name=calendar_timezone,
        )
        cursor = json.dumps({"synced_at_utc": ensure_utc(now_utc).isoformat()}, sort_keys=True)
        if expires_at is None:
            expires_at = token_record.expires_at_utc
        return ProviderBatch(emails=emails, calendar_events=calendar_events, cursor=cursor)

    def search_gmail(
        self,
        account: ConnectedAccount,
        *,
        query: str,
        now_utc: datetime,
        max_results: int = 5,
    ) -> list[dict[str, object]]:
        if self.settings is None or self.store is None or not self.settings.token_encryption_key:
            return []
        token_record = self.store.get_connected_account_token(account.id)
        if token_record is None:
            return []
        vault = TokenVault.from_settings(self.settings)
        token_payload = vault.decrypt(token_record.token_ciphertext)
        access_token, _expires_at = self._access_token(
            account=account,
            token_payload=token_payload,
            vault=vault,
            now_utc=ensure_utc(now_utc),
        )
        return self._search_gmail(
            account=account,
            access_token=access_token,
            query=query,
            now_utc=now_utc,
            max_results=max_results,
        )

    def _access_token(
        self,
        *,
        account: ConnectedAccount,
        token_payload: dict[str, object],
        vault: TokenVault,
        now_utc: datetime,
    ) -> tuple[str, datetime | None]:
        access_token = _payload_str(token_payload, "access_token")
        expires_at = _parse_dt(token_payload.get("expires_at_utc"))
        if expires_at is None or expires_at - timedelta(seconds=60) > now_utc:
            return access_token, expires_at
        refresh_token = _payload_str(token_payload, "refresh_token")
        refreshed = self._refresh_token(refresh_token)
        access_token = _payload_str(refreshed, "access_token")
        expires_at = _expires_at(refreshed, now_utc)
        token_payload = {
            **token_payload,
            "access_token": access_token,
            "token_type": refreshed.get("token_type") or token_payload.get("token_type"),
            "scope": refreshed.get("scope") or token_payload.get("scope"),
            "expires_at_utc": expires_at.isoformat() if expires_at else None,
        }
        self.store.upsert_connected_account_token(
            connected_account_id=account.id,
            provider="google",
            token_ciphertext=vault.encrypt(token_payload),
            scopes=tuple(str(token_payload.get("scope") or "").split()),
            expires_at_utc=expires_at,
            now_utc=now_utc,
        )
        return access_token, expires_at

    def _refresh_token(self, refresh_token: str) -> dict[str, Any]:
        if not self.settings or not self.settings.google_client_id:
            raise TokenVaultError("Google OAuth settings are incomplete")
        response = self._client.post(
            GOOGLE_TOKEN_ENDPOINT,
            data={
                "client_id": self.settings.google_client_id,
                "client_secret": self.settings.google_client_secret or "",
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError("google refresh returned a non-object")
        return data

    def _fetch_gmail(
        self,
        *,
        account: ConnectedAccount,
        access_token: str,
        now_utc: datetime,
    ) -> list[dict[str, object]]:
        settings = self.settings
        if settings is None:
            return []
        since = account.last_synced_at_utc or ensure_utc(now_utc) - timedelta(
            days=max(1, settings.google_fetch_since_days)
        )
        response = self._client.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages",
            params={
                "maxResults": max(1, min(500, settings.google_fetch_max_emails)),
                "labelIds": "INBOX",
            },
            headers={"Authorization": f"Bearer {access_token}"},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return []
        messages = payload.get("messages")
        if not isinstance(messages, list):
            return []
        emails: list[dict[str, object]] = []
        for message in messages:
            if not isinstance(message, dict) or not message.get("id"):
                continue
            detail = self._gmail_message(str(message["id"]), access_token)
            received_at = _gmail_internal_date(detail) or ensure_utc(now_utc)
            if received_at < since:
                continue
            headers = _gmail_headers(detail)
            emails.append(
                {
                    "external_id": str(detail.get("id") or message["id"]),
                    "subject": headers.get("subject") or "(No subject)",
                    "sender": headers.get("from") or "",
                    "body": str(detail.get("snippet") or ""),
                    "received_at_utc": received_at.isoformat(),
                }
            )
        return emails

    def _gmail_message(self, message_id: str, access_token: str) -> dict[str, Any]:
        response = self._client.get(
            f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}",
            params=[
                ("format", "metadata"),
                ("metadataHeaders", "Subject"),
                ("metadataHeaders", "From"),
                ("metadataHeaders", "Date"),
            ],
            headers={"Authorization": f"Bearer {access_token}"},
        )
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else {}

    def _search_gmail(
        self,
        *,
        account: ConnectedAccount,
        access_token: str,
        query: str,
        now_utc: datetime,
        max_results: int,
    ) -> list[dict[str, object]]:
        response = self._client.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages",
            params={
                "maxResults": max(1, min(10, max_results)),
                "q": query,
            },
            headers={"Authorization": f"Bearer {access_token}"},
        )
        response.raise_for_status()
        payload = response.json()
        messages = payload.get("messages") if isinstance(payload, dict) else None
        if not isinstance(messages, list):
            return []
        emails: list[dict[str, object]] = []
        for message in messages:
            if not isinstance(message, dict) or not message.get("id"):
                continue
            detail = self._gmail_full_message(str(message["id"]), access_token)
            received_at = _gmail_internal_date(detail) or ensure_utc(now_utc)
            headers = _gmail_headers(detail)
            body = _gmail_body(detail) or str(detail.get("snippet") or "")
            emails.append(
                {
                    "external_id": f"search:{detail.get('id') or message['id']}",
                    "subject": headers.get("subject") or "(No subject)",
                    "sender": headers.get("from") or "",
                    "body": body,
                    "received_at_utc": received_at.isoformat(),
                    "connected_account_id": account.id,
                }
            )
        return emails

    def _gmail_full_message(self, message_id: str, access_token: str) -> dict[str, Any]:
        response = self._client.get(
            f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}",
            params=[("format", "full")],
            headers={"Authorization": f"Bearer {access_token}"},
        )
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else {}

    def _calendar_timezone(self, account: ConnectedAccount) -> str:
        if self.store is None:
            return "UTC"
        household = self.store.get_household_by_id(account.household_id)
        if household is None:
            return "UTC"
        return household.timezone

    def _fetch_calendar(
        self,
        *,
        access_token: str,
        now_utc: datetime,
        timezone_name: str,
    ) -> list[dict[str, object]]:
        settings = self.settings
        if settings is None:
            return []
        try:
            calendar_zone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            calendar_zone = ZoneInfo("UTC")
        now = ensure_utc(now_utc)
        time_max = now + timedelta(days=max(1, settings.google_fetch_calendar_days))
        response = self._client.get(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events",
            params={
                "timeMin": _rfc3339_z(now),
                "timeMax": _rfc3339_z(time_max),
                "singleEvents": "true",
                "orderBy": "startTime",
                "maxResults": max(1, min(2500, settings.google_fetch_max_calendar_events)),
            },
            headers={"Authorization": f"Bearer {access_token}"},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return []
        items = payload.get("items")
        if not isinstance(items, list):
            return []
        events: list[dict[str, object]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            starts_at = _calendar_start(item, calendar_zone)
            if starts_at is None:
                continue
            events.append(
                {
                    "external_id": _optional_str(item.get("id")),
                    "title": str(item.get("summary") or "Calendar event"),
                    "starts_at_utc": starts_at.isoformat(),
                    "ends_at_utc": _calendar_end(item, calendar_zone),
                    "location": _optional_str(item.get("location")),
                    "description": _optional_str(item.get("description")),
                    "calendar_name": "Google Calendar",
                    "observed_at_utc": ensure_utc(now_utc).isoformat(),
                }
            )
        return events


def default_source_providers(
    *,
    settings: Settings | None = None,
    store: object | None = None,
) -> dict[str, SourceProvider]:
    google = GoogleSourceProvider(settings=settings, store=store)
    return {google.provider: google}


def run_connected_source_sync(
    *,
    service: FlorenceService,
    providers: dict[str, SourceProvider] | None = None,
    now_utc: datetime | None = None,
    limit: int = 100,
    mark_surfaced: bool = True,
) -> SourceSyncRunResult:
    now = ensure_utc(now_utc or datetime.now(timezone.utc))
    resolved_providers = (
        default_source_providers(settings=service.settings, store=service.store)
        if providers is None
        else providers
    )
    checked = 0
    synced = 0
    imported = 0
    surfaced = 0
    skipped = 0
    failed = 0
    messages: list[OutboundMessage] = []
    for account in service.store.list_active_connected_accounts(now_utc=now, limit=limit):
        checked += 1
        provider = resolved_providers.get(account.provider)
        household = service.store.get_household_by_id(account.household_id)
        if provider is None or household is None:
            skipped += 1
            continue
        try:
            batch = provider.fetch(account, now_utc=now)
            result = service.sync_connected_sources(
                chat_id=household.chat_id,
                provider=account.provider,
                external_account_id=account.external_account_id,
                account_label=account.account_label,
                emails=batch.emails,
                calendar_events=batch.calendar_events,
                cursor=batch.cursor,
                now_utc=now,
                mark_surfaced=mark_surfaced,
            )
        except Exception as exc:
            service.store.record_connected_account_sync_failure(
                account_id=account.id,
                error=str(exc),
                failed_at_utc=now,
            )
            failed += 1
            continue
        synced += 1
        imported += result.imported
        surfaced += result.surfaced
        messages.extend(result.messages)
    return SourceSyncRunResult(
        checked=checked,
        synced=synced,
        imported=imported,
        surfaced=surfaced,
        skipped=skipped,
        failed=failed,
        messages=tuple(messages),
    )


def _payload_str(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"missing google {key}")
    return value


def _parse_dt(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _expires_at(payload: dict[str, Any], now: datetime) -> datetime | None:
    raw = payload.get("expires_in")
    if raw in (None, ""):
        return None
    try:
        seconds = int(raw)
    except (TypeError, ValueError):
        return None
    return ensure_utc(now) + timedelta(seconds=seconds)


def _gmail_internal_date(message: dict[str, Any]) -> datetime | None:
    raw = message.get("internalDate")
    if raw is None:
        return None
    try:
        millis = int(raw)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)


def _gmail_headers(message: dict[str, Any]) -> dict[str, str]:
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return {}
    headers = payload.get("headers")
    if not isinstance(headers, list):
        return {}
    result: dict[str, str] = {}
    for item in headers:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip().lower()
        value = str(item.get("value") or "").strip()
        if name and value:
            result[name] = value
    return result


def _gmail_body(message: dict[str, Any]) -> str:
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return ""
    parts = _gmail_text_parts(payload)
    plain = [text for mime_type, text in parts if mime_type == "text/plain" and text.strip()]
    html_parts = [text for mime_type, text in parts if mime_type == "text/html" and text.strip()]
    if plain:
        return _clean_email_text("\n".join(plain))
    if html_parts:
        return _clean_email_text("\n".join(_strip_html(part) for part in html_parts))
    return _clean_email_text(_decode_gmail_body(payload.get("body")))


def _gmail_text_parts(payload: dict[str, Any]) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    mime_type = str(payload.get("mimeType") or "")
    decoded = _decode_gmail_body(payload.get("body"))
    if decoded:
        results.append((mime_type, decoded))
    parts = payload.get("parts")
    if isinstance(parts, list):
        for part in parts:
            if isinstance(part, dict):
                results.extend(_gmail_text_parts(part))
    return results


def _decode_gmail_body(body: object) -> str:
    if not isinstance(body, dict):
        return ""
    data = body.get("data")
    if not isinstance(data, str) or not data:
        return ""
    padded = data + ("=" * (-len(data) % 4))
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8", errors="replace")
    except (ValueError, TypeError):
        return ""


def _strip_html(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", value)
    return html.unescape(without_tags)


def _clean_email_text(value: str) -> str:
    compact = " ".join(html.unescape(value).split())
    return compact[:2000]


def _calendar_start(item: dict[str, Any], calendar_zone: ZoneInfo) -> datetime | None:
    start = item.get("start")
    if not isinstance(start, dict):
        return None
    return _parse_dt(start.get("dateTime")) or _parse_all_day(start.get("date"), calendar_zone)


def _calendar_end(item: dict[str, Any], calendar_zone: ZoneInfo) -> str | None:
    end = item.get("end")
    if not isinstance(end, dict):
        return None
    value = _parse_dt(end.get("dateTime")) or _parse_all_day(end.get("date"), calendar_zone)
    return value.isoformat() if value else None


def _parse_all_day(value: object, calendar_zone: ZoneInfo) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        local_midnight = datetime.fromisoformat(value).replace(tzinfo=calendar_zone)
        return ensure_utc(local_midnight)
    except ValueError:
        return None


def _optional_str(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    compact = " ".join(value.split())
    return compact or None


def _rfc3339_z(value: datetime) -> str:
    return ensure_utc(value).isoformat().replace("+00:00", "Z")
