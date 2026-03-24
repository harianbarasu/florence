"""Google OAuth primitives for Florence.

These helpers intentionally avoid persistence concerns. They provide the core
state signing, connect URL construction, token exchange, token refresh, and
basic Google identity/calendar metadata fetches that Florence will need once
the product storage layer is added.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import asdict
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

import httpx

from florence.google.types import (
    FlorenceGoogleOauthState,
    GoogleCalendarMetadata,
    GoogleTokenResponse,
)

FLORENCE_GOOGLE_OAUTH_SCOPES = (
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
)


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("utf-8").rstrip("=")


def _base64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _sign_state(encoded_payload: str, state_secret: str) -> str:
    digest = hmac.new(
        state_secret.encode("utf-8"),
        encoded_payload.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return _base64url_encode(digest)


def encode_google_oauth_state(payload: FlorenceGoogleOauthState, state_secret: str) -> str:
    encoded_payload = _base64url_encode(json.dumps(asdict(payload), separators=(",", ":")).encode("utf-8"))
    return f"{encoded_payload}.{_sign_state(encoded_payload, state_secret)}"


def decode_google_oauth_state(
    raw_state: str,
    state_secret: str,
    *,
    max_age_seconds: int = 15 * 60,
    now_ms: int | None = None,
) -> FlorenceGoogleOauthState:
    encoded_payload, _, signature = raw_state.partition(".")
    if not encoded_payload or not signature:
        raise ValueError("invalid_google_oauth_state")

    expected_signature = _sign_state(encoded_payload, state_secret)
    if not hmac.compare_digest(expected_signature, signature):
        raise ValueError("invalid_google_oauth_state_signature")

    payload = json.loads(_base64url_decode(encoded_payload).decode("utf-8"))
    issued_at_ms = int(payload.get("issued_at_ms") or 0)
    current_ms = now_ms if now_ms is not None else int(datetime.now().timestamp() * 1000)
    if current_ms - issued_at_ms > max_age_seconds * 1000:
        raise ValueError("google_oauth_state_expired")

    try:
        return FlorenceGoogleOauthState(
            household_id=str(payload["household_id"]),
            member_id=str(payload["member_id"]),
            thread_id=str(payload["thread_id"]) if payload.get("thread_id") is not None else None,
            nonce=str(payload["nonce"]),
            issued_at_ms=issued_at_ms,
        )
    except KeyError as exc:
        raise ValueError("invalid_google_oauth_state_payload") from exc


def build_google_oauth_connect_url(
    *,
    client_id: str,
    redirect_uri: str,
    state_payload: FlorenceGoogleOauthState,
    state_secret: str,
    scopes: tuple[str, ...] = FLORENCE_GOOGLE_OAUTH_SCOPES,
) -> str:
    state = encode_google_oauth_state(state_payload, state_secret)
    query = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "access_type": "offline",
            "prompt": "consent",
            "scope": " ".join(scopes),
            "state": state,
        }
    )
    return f"https://accounts.google.com/o/oauth2/v2/auth?{query}"


def _parse_token_response(payload: dict[str, Any]) -> GoogleTokenResponse:
    return GoogleTokenResponse(
        access_token=str(payload.get("access_token") or ""),
        refresh_token=str(payload.get("refresh_token")) if payload.get("refresh_token") is not None else None,
        expires_in=int(payload["expires_in"]) if payload.get("expires_in") is not None else None,
        scope=str(payload.get("scope")) if payload.get("scope") is not None else None,
        token_type=str(payload.get("token_type")) if payload.get("token_type") is not None else None,
    )


def exchange_google_code_for_tokens(
    *,
    code: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    timeout_seconds: float = 30.0,
) -> GoogleTokenResponse:
    response = httpx.post(
        "https://oauth2.googleapis.com/token",
        headers={"content-type": "application/x-www-form-urlencoded"},
        data={
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
        timeout=timeout_seconds,
    )
    payload = response.json()
    response.raise_for_status()
    return _parse_token_response(payload)


def refresh_google_access_token(
    *,
    refresh_token: str,
    client_id: str,
    client_secret: str,
    timeout_seconds: float = 30.0,
) -> GoogleTokenResponse:
    response = httpx.post(
        "https://oauth2.googleapis.com/token",
        headers={"content-type": "application/x-www-form-urlencoded"},
        data={
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
        },
        timeout=timeout_seconds,
    )
    payload = response.json()
    response.raise_for_status()
    return _parse_token_response(payload)


def fetch_google_user_email(*, access_token: str, timeout_seconds: float = 30.0) -> str:
    response = httpx.get(
        "https://www.googleapis.com/oauth2/v2/userinfo",
        headers={"authorization": f"Bearer {access_token}"},
        timeout=timeout_seconds,
    )
    payload = response.json()
    response.raise_for_status()
    email = payload.get("email")
    if not isinstance(email, str) or not email.strip():
        raise ValueError("google_userinfo_missing_email")
    return email.strip()


def fetch_primary_google_calendar(
    *,
    access_token: str,
    fallback_timezone: str = "America/Los_Angeles",
    timeout_seconds: float = 30.0,
) -> GoogleCalendarMetadata:
    response = httpx.get(
        "https://www.googleapis.com/calendar/v3/users/me/calendarList",
        headers={"authorization": f"Bearer {access_token}"},
        timeout=timeout_seconds,
    )
    payload = response.json()
    response.raise_for_status()
    items = payload.get("items") or []
    primary = next((item for item in items if item.get("primary")), None) or (items[0] if items else None)
    if not isinstance(primary, dict) or not primary.get("id"):
        raise ValueError("google_primary_calendar_missing")
    return GoogleCalendarMetadata(
        id=str(primary["id"]),
        summary=str(primary.get("summary") or "Primary calendar"),
        timezone=str(primary.get("timeZone") or fallback_timezone),
        access_role=str(primary.get("accessRole")) if primary.get("accessRole") is not None else None,
    )
