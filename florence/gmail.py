"""Google OAuth (read-only Gmail + Calendar) and fetch/search helpers.

Token custody: OAuth token payloads are Fernet-encrypted at rest
(FLORENCE_TOKEN_ENCRYPTION_KEY) and refreshed transparently on use.
"""

from __future__ import annotations

import base64
import html
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

import httpx
from cryptography.fernet import Fernet, InvalidToken

from florence.config import Settings
from florence.store import GmailAccount, Store
from florence.timeutil import ensure_utc, now_utc

log = logging.getLogger("florence.gmail")

GOOGLE_AUTHORIZATION_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_ENDPOINT = "https://openidconnect.googleapis.com/v1/userinfo"
GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"
CALENDAR_API = "https://www.googleapis.com/calendar/v3/calendars/primary/events"

OAUTH_SCOPES = (
    "openid",
    "email",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/calendar.readonly",
)


class GoogleError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class EmailSummary:
    account_email: str
    gmail_id: str
    sender: str
    subject: str
    snippet: str
    received_at: datetime


@dataclass(frozen=True, slots=True)
class CalendarEvent:
    account_email: str
    title: str
    starts_at: datetime
    ends_at: datetime | None
    location: str | None
    all_day: bool


class TokenVault:
    def __init__(self, encryption_key: str) -> None:
        try:
            self._fernet = Fernet(encryption_key.encode())
        except (ValueError, TypeError) as exc:
            raise GoogleError("invalid FLORENCE_TOKEN_ENCRYPTION_KEY") from exc

    def encrypt(self, payload: dict[str, Any]) -> str:
        return self._fernet.encrypt(json.dumps(payload, sort_keys=True).encode()).decode()

    def decrypt(self, ciphertext: str) -> dict[str, Any]:
        try:
            raw = self._fernet.decrypt(ciphertext.encode())
        except InvalidToken as exc:
            raise GoogleError("stored token cannot be decrypted") from exc
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise GoogleError("stored token payload is not an object")
        return data


def authorization_url(settings: Settings, state: str) -> str:
    if not settings.google_client_id or not settings.google_redirect_uri:
        raise GoogleError("Google OAuth is not configured")
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": settings.google_redirect_uri,
        "response_type": "code",
        "scope": " ".join(OAUTH_SCOPES),
        "state": state,
        "access_type": "offline",
        "include_granted_scopes": "true",
        "prompt": "consent",
    }
    return f"{GOOGLE_AUTHORIZATION_ENDPOINT}?{urlencode(params)}"


class GoogleService:
    def __init__(
        self, settings: Settings, store: Store, *, http_client: httpx.AsyncClient | None = None
    ) -> None:
        self.settings = settings
        self.store = store
        self._client = http_client or httpx.AsyncClient(timeout=25.0)
        self._vault: TokenVault | None = (
            TokenVault(settings.token_encryption_key) if settings.token_encryption_key else None
        )

    @property
    def configured(self) -> bool:
        return bool(
            self.settings.google_client_id
            and self.settings.google_client_secret
            and self.settings.google_redirect_uri
            and self._vault
        )

    # -- OAuth -----------------------------------------------------------------

    async def exchange_code(self, code: str) -> dict[str, Any]:
        """Exchange an authorization code; returns {email, google_sub, ciphertext, scopes}."""
        if not self.configured:
            raise GoogleError("Google OAuth is not configured")
        token_data = await self._post_token(
            {
                "client_id": self.settings.google_client_id,
                "client_secret": self.settings.google_client_secret or "",
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": self.settings.google_redirect_uri,
            }
        )
        access_token = _require_str(token_data, "access_token")
        userinfo = await self._get_json(
            GOOGLE_USERINFO_ENDPOINT, access_token, error="google userinfo fetch failed"
        )
        sub = _require_str(userinfo, "sub")
        email = str(userinfo.get("email") or "").strip() or f"google-{sub}"
        expires_at = _expires_at(token_data, now_utc())
        payload = {
            "access_token": access_token,
            "refresh_token": token_data.get("refresh_token"),
            "expires_at_utc": expires_at.isoformat() if expires_at else None,
        }
        assert self._vault is not None
        return {
            "email": email,
            "google_sub": sub,
            "ciphertext": self._vault.encrypt(payload),
            "scopes": str(token_data.get("scope") or " ".join(OAUTH_SCOPES)),
        }

    async def _post_token(self, payload: dict[str, str]) -> dict[str, Any]:
        try:
            response = await self._client.post(GOOGLE_TOKEN_ENDPOINT, data=payload)
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise GoogleError(f"google token exchange failed: {exc}") from exc
        if not isinstance(data, dict):
            raise GoogleError("google token exchange returned a non-object")
        return data

    async def _access_token(self, account: GmailAccount) -> str:
        if self._vault is None:
            raise GoogleError("token encryption key is not configured")
        payload = self._vault.decrypt(account.token_ciphertext)
        access_token = str(payload.get("access_token") or "")
        expires_at = _parse_dt(payload.get("expires_at_utc"))
        if access_token and expires_at and expires_at - timedelta(seconds=90) > now_utc():
            return access_token
        refresh_token = str(payload.get("refresh_token") or "")
        if not refresh_token:
            raise GoogleError(f"no refresh token for {account.email}; reconnect needed")
        refreshed = await self._post_token(
            {
                "client_id": self.settings.google_client_id or "",
                "client_secret": self.settings.google_client_secret or "",
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            }
        )
        access_token = _require_str(refreshed, "access_token")
        expires_at = _expires_at(refreshed, now_utc())
        payload.update(
            access_token=access_token,
            expires_at_utc=expires_at.isoformat() if expires_at else None,
        )
        await self.store.update_gmail_token(account.id, self._vault.encrypt(payload))
        return access_token

    async def _get_json(self, url: str, access_token: str, *, error: str, params: Any = None) -> dict[str, Any]:
        try:
            response = await self._client.get(
                url, params=params, headers={"Authorization": f"Bearer {access_token}"}
            )
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise GoogleError(f"{error}: {exc}") from exc
        return data if isinstance(data, dict) else {}

    # -- Gmail -----------------------------------------------------------------

    async def new_message_ids(self, account: GmailAccount, *, since: datetime, cap: int) -> list[str]:
        token = await self._access_token(account)
        query = f"in:inbox -category:promotions -category:social after:{int(ensure_utc(since).timestamp())}"
        data = await self._get_json(
            f"{GMAIL_API}/messages",
            token,
            params={"maxResults": max(1, min(100, cap)), "q": query},
            error="gmail list failed",
        )
        messages = data.get("messages")
        if not isinstance(messages, list):
            return []
        return [str(m["id"]) for m in messages if isinstance(m, dict) and m.get("id")]

    async def message_summary(self, account: GmailAccount, gmail_id: str) -> EmailSummary:
        token = await self._access_token(account)
        detail = await self._get_json(
            f"{GMAIL_API}/messages/{gmail_id}",
            token,
            params=[
                ("format", "metadata"),
                ("metadataHeaders", "Subject"),
                ("metadataHeaders", "From"),
            ],
            error="gmail message fetch failed",
        )
        headers = _headers_map(detail)
        return EmailSummary(
            account_email=account.email,
            gmail_id=gmail_id,
            sender=headers.get("from", ""),
            subject=headers.get("subject", "(no subject)"),
            snippet=html.unescape(str(detail.get("snippet") or ""))[:300],
            received_at=_internal_date(detail) or now_utc(),
        )

    async def search(self, account: GmailAccount, query: str, *, cap: int = 5) -> list[EmailSummary]:
        token = await self._access_token(account)
        data = await self._get_json(
            f"{GMAIL_API}/messages",
            token,
            params={"maxResults": max(1, min(10, cap)), "q": query},
            error="gmail search failed",
        )
        messages = data.get("messages")
        if not isinstance(messages, list):
            return []
        out: list[EmailSummary] = []
        for m in messages[:cap]:
            if isinstance(m, dict) and m.get("id"):
                out.append(await self.message_summary(account, str(m["id"])))
        return out

    async def read_full(self, account: GmailAccount, gmail_id: str) -> dict[str, Any]:
        token = await self._access_token(account)
        detail = await self._get_json(
            f"{GMAIL_API}/messages/{gmail_id}", token, params=[("format", "full")], error="gmail read failed"
        )
        headers = _headers_map(detail)
        received = _internal_date(detail)
        return {
            "from": headers.get("from", ""),
            "to": headers.get("to", ""),
            "subject": headers.get("subject", "(no subject)"),
            "date": received.isoformat() if received else None,
            "body": _body_text(detail) or html.unescape(str(detail.get("snippet") or "")),
        }

    # -- Calendar ----------------------------------------------------------------

    async def calendar_events(
        self, account: GmailAccount, *, days_ahead: int, tz_name: str
    ) -> list[CalendarEvent]:
        token = await self._access_token(account)
        now = now_utc()
        data = await self._get_json(
            CALENDAR_API,
            token,
            params={
                "timeMin": _rfc3339(now),
                "timeMax": _rfc3339(now + timedelta(days=max(1, days_ahead))),
                "singleEvents": "true",
                "orderBy": "startTime",
                "maxResults": 50,
            },
            error="calendar fetch failed",
        )
        items = data.get("items")
        if not isinstance(items, list):
            return []
        from florence.timeutil import resolve_timezone

        zone = resolve_timezone(tz_name)
        events: list[CalendarEvent] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            start_raw = item.get("start") if isinstance(item.get("start"), dict) else {}
            end_raw = item.get("end") if isinstance(item.get("end"), dict) else {}
            all_day = "date" in start_raw and "dateTime" not in start_raw
            starts = _parse_dt(start_raw.get("dateTime")) or _parse_all_day(start_raw.get("date"), zone)
            if starts is None:
                continue
            events.append(
                CalendarEvent(
                    account_email=account.email,
                    title=str(item.get("summary") or "(untitled)"),
                    starts_at=starts,
                    ends_at=_parse_dt(end_raw.get("dateTime")) or _parse_all_day(end_raw.get("date"), zone),
                    location=(str(item.get("location")).strip() or None) if item.get("location") else None,
                    all_day=all_day,
                )
            )
        return events


# -- parsing helpers ----------------------------------------------------------


def _require_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise GoogleError(f"google response missing {key}")
    return value


def _expires_at(payload: dict[str, Any], now: datetime) -> datetime | None:
    raw = payload.get("expires_in")
    try:
        seconds = int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return now + timedelta(seconds=seconds) if seconds > 0 else None


def _parse_dt(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _parse_all_day(value: Any, zone: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return ensure_utc(datetime.fromisoformat(value).replace(tzinfo=zone))
    except ValueError:
        return None


def _internal_date(message: dict[str, Any]) -> datetime | None:
    raw = message.get("internalDate")
    try:
        millis = int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)


def _headers_map(message: dict[str, Any]) -> dict[str, str]:
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return {}
    headers = payload.get("headers")
    if not isinstance(headers, list):
        return {}
    out: dict[str, str] = {}
    for item in headers:
        if isinstance(item, dict) and item.get("name") and item.get("value"):
            out[str(item["name"]).strip().lower()] = str(item["value"]).strip()
    return out


def _body_text(message: dict[str, Any]) -> str:
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return ""
    parts = _text_parts(payload)
    plain = [t for mime, t in parts if mime == "text/plain" and t.strip()]
    if plain:
        return _clean("\n".join(plain))
    htmls = [t for mime, t in parts if mime == "text/html" and t.strip()]
    if htmls:
        return _clean("\n".join(re.sub(r"<[^>]+>", " ", h) for h in htmls))
    return _clean(_decode_body(payload.get("body")))


def _text_parts(payload: dict[str, Any]) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    decoded = _decode_body(payload.get("body"))
    if decoded:
        results.append((str(payload.get("mimeType") or ""), decoded))
    parts = payload.get("parts")
    if isinstance(parts, list):
        for part in parts:
            if isinstance(part, dict):
                results.extend(_text_parts(part))
    return results


def _decode_body(body: Any) -> str:
    if not isinstance(body, dict):
        return ""
    data = body.get("data")
    if not isinstance(data, str) or not data:
        return ""
    padded = data + ("=" * (-len(data) % 4))
    try:
        return base64.urlsafe_b64decode(padded.encode()).decode("utf-8", errors="replace")
    except (ValueError, TypeError):
        return ""


def _clean(value: str) -> str:
    return " ".join(html.unescape(value).split())[:4000]


def _rfc3339(value: datetime) -> str:
    return ensure_utc(value).isoformat().replace("+00:00", "Z")
