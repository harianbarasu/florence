"""OAuth and encrypted token custody for connected accounts."""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

import httpx
from cryptography.fernet import Fernet, InvalidToken

from florence.config import Settings
from florence.timekeeper import ensure_utc


GOOGLE_AUTHORIZATION_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_ENDPOINT = "https://openidconnect.googleapis.com/v1/userinfo"


class OAuthConfigurationError(RuntimeError):
    pass


class OAuthExchangeError(RuntimeError):
    pass


class TokenVaultError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class OAuthStart:
    authorization_url: str
    state: str
    expires_at_utc: datetime
    scopes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class GoogleOAuthResult:
    external_account_id: str
    account_label: str | None
    scopes: tuple[str, ...]
    token_payload: dict[str, object]
    expires_at_utc: datetime | None


def missing_google_oauth_settings(settings: Settings) -> list[str]:
    missing: list[str] = []
    if not settings.google_client_id:
        missing.append("GOOGLE_CLIENT_ID")
    if not settings.google_client_secret:
        missing.append("GOOGLE_CLIENT_SECRET")
    if not settings.google_redirect_uri:
        missing.append("GOOGLE_REDIRECT_URI")
    if not settings.token_encryption_key:
        missing.append("FLORENCE_TOKEN_ENCRYPTION_KEY")
    return missing


def require_google_oauth_settings(settings: Settings) -> None:
    missing = missing_google_oauth_settings(settings)
    if missing:
        raise OAuthConfigurationError(", ".join(missing))


def build_google_oauth_start(
    settings: Settings,
    *,
    now_utc: datetime | None = None,
) -> OAuthStart:
    require_google_oauth_settings(settings)
    now = ensure_utc(now_utc or datetime.now(timezone.utc))
    state = secrets.token_urlsafe(32)
    expires_at = now + timedelta(minutes=max(1, settings.google_oauth_state_ttl_minutes))
    scopes = settings.google_oauth_scopes
    authorization_url = google_authorization_url(settings=settings, state=state, scopes=scopes)
    return OAuthStart(
        authorization_url=authorization_url,
        state=state,
        expires_at_utc=expires_at,
        scopes=scopes,
    )


def google_authorization_url(
    *,
    settings: Settings,
    state: str,
    scopes: tuple[str, ...] | None = None,
) -> str:
    if not settings.google_client_id or not settings.google_redirect_uri:
        raise OAuthConfigurationError("GOOGLE_CLIENT_ID and GOOGLE_REDIRECT_URI are required")
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": settings.google_redirect_uri,
        "response_type": "code",
        "scope": " ".join(scopes or settings.google_oauth_scopes),
        "state": state,
        "access_type": "offline",
        "include_granted_scopes": "true",
        "prompt": "consent",
    }
    return f"{GOOGLE_AUTHORIZATION_ENDPOINT}?{urlencode(params)}"


class TokenVault:
    def __init__(self, encryption_key: str) -> None:
        try:
            self._fernet = Fernet(encryption_key.encode("utf-8"))
        except (ValueError, TypeError) as exc:
            raise TokenVaultError("invalid token encryption key") from exc

    @classmethod
    def from_settings(cls, settings: Settings) -> "TokenVault":
        if not settings.token_encryption_key:
            raise TokenVaultError("FLORENCE_TOKEN_ENCRYPTION_KEY is required")
        return cls(settings.token_encryption_key)

    @staticmethod
    def generate_key() -> str:
        return Fernet.generate_key().decode("utf-8")

    def encrypt(self, payload: dict[str, object]) -> str:
        plaintext = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return self._fernet.encrypt(plaintext).decode("utf-8")

    def decrypt(self, ciphertext: str) -> dict[str, object]:
        try:
            raw = self._fernet.decrypt(ciphertext.encode("utf-8"))
        except InvalidToken as exc:
            raise TokenVaultError("token ciphertext cannot be decrypted") from exc
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise TokenVaultError("token payload is not an object")
        return data


class GoogleOAuthClient:
    def __init__(
        self,
        settings: Settings,
        *,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings
        self._client = http_client or httpx.Client(timeout=20.0)

    def exchange_code(
        self,
        *,
        code: str,
        now_utc: datetime | None = None,
    ) -> GoogleOAuthResult:
        if not self.settings.google_client_id or not self.settings.google_redirect_uri:
            raise OAuthConfigurationError("Google OAuth settings are incomplete")
        now = ensure_utc(now_utc or datetime.now(timezone.utc))
        token_data = self._post_token(
            {
                "client_id": self.settings.google_client_id,
                "client_secret": self.settings.google_client_secret or "",
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": self.settings.google_redirect_uri,
            }
        )
        access_token = _required_str(token_data, "access_token")
        expires_at = _expires_at(token_data, now)
        userinfo = self._get_userinfo(access_token)
        external_account_id = _required_str(userinfo, "sub")
        account_label = _optional_str(userinfo.get("email"))
        scopes = _scopes_from_token(token_data, self.settings.google_oauth_scopes)
        token_payload = {
            "provider": "google",
            "access_token": access_token,
            "refresh_token": _optional_str(token_data.get("refresh_token")),
            "token_type": _optional_str(token_data.get("token_type")),
            "scope": " ".join(scopes),
            "expires_at_utc": expires_at.isoformat() if expires_at else None,
            "google_sub": external_account_id,
            "email": account_label,
        }
        return GoogleOAuthResult(
            external_account_id=external_account_id,
            account_label=account_label,
            scopes=scopes,
            token_payload=token_payload,
            expires_at_utc=expires_at,
        )

    def _post_token(self, payload: dict[str, str]) -> dict[str, Any]:
        try:
            response = self._client.post(GOOGLE_TOKEN_ENDPOINT, data=payload)
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise OAuthExchangeError("google token exchange failed") from exc
        if not isinstance(data, dict):
            raise OAuthExchangeError("google token exchange returned a non-object")
        return data

    def _get_userinfo(self, access_token: str) -> dict[str, Any]:
        try:
            response = self._client.get(
                GOOGLE_USERINFO_ENDPOINT,
                headers={"Authorization": f"Bearer {access_token}"},
            )
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise OAuthExchangeError("google userinfo fetch failed") from exc
        if not isinstance(data, dict):
            raise OAuthExchangeError("google userinfo returned a non-object")
        return data


def _expires_at(payload: dict[str, Any], now: datetime) -> datetime | None:
    raw = payload.get("expires_in")
    if raw in (None, ""):
        return None
    try:
        seconds = int(raw)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    return now + timedelta(seconds=seconds)


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise OAuthExchangeError(f"google response missing {key}")
    return value


def _optional_str(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    compact = value.strip()
    return compact or None


def _scopes_from_token(payload: dict[str, Any], fallback: tuple[str, ...]) -> tuple[str, ...]:
    raw = payload.get("scope")
    if isinstance(raw, str) and raw.strip():
        return tuple(item for item in raw.split() if item)
    return fallback
