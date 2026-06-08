"""Signed web onboarding links for Florence households."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timezone

from florence.timekeeper import ensure_utc


VALID_ONBOARDING_ROLES = frozenset({"primary", "partner"})


@dataclass(frozen=True, slots=True)
class OnboardingTokenClaims:
    chat_id: str
    member_id: str
    role: str
    expires_at_utc: datetime


class OnboardingTokenError(ValueError):
    """Raised when an onboarding token is malformed, expired, or unsigned."""


def create_onboarding_token(
    *,
    secret: str,
    chat_id: str,
    member_id: str,
    role: str,
    expires_at_utc: datetime,
) -> str:
    if role not in VALID_ONBOARDING_ROLES:
        raise ValueError("unsupported onboarding role")
    if not secret:
        raise ValueError("onboarding token secret is required")
    payload = {
        "v": 1,
        "chat_id": chat_id,
        "member_id": member_id,
        "role": role,
        "exp": int(ensure_utc(expires_at_utc).timestamp()),
    }
    encoded_payload = _b64url(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = _sign(secret=secret, encoded_payload=encoded_payload)
    return f"{encoded_payload}.{signature}"


def verify_onboarding_token(
    *,
    secret: str,
    token: str,
    now_utc: datetime,
) -> OnboardingTokenClaims:
    if not secret:
        raise OnboardingTokenError("missing_secret")
    payload_part, separator, signature_part = token.partition(".")
    if not separator or not payload_part or not signature_part:
        raise OnboardingTokenError("malformed_token")
    expected = _sign(secret=secret, encoded_payload=payload_part)
    if not hmac.compare_digest(signature_part, expected):
        raise OnboardingTokenError("invalid_signature")
    try:
        payload = json.loads(_unb64url(payload_part))
    except (ValueError, TypeError) as exc:
        raise OnboardingTokenError("malformed_payload") from exc
    if not isinstance(payload, dict) or payload.get("v") != 1:
        raise OnboardingTokenError("unsupported_token")
    chat_id = str(payload.get("chat_id") or "")
    member_id = str(payload.get("member_id") or "")
    role = str(payload.get("role") or "")
    try:
        expires_at = datetime.fromtimestamp(int(payload.get("exp")), tz=timezone.utc)
    except (TypeError, ValueError, OSError) as exc:
        raise OnboardingTokenError("malformed_expiry") from exc
    if not chat_id or not member_id or role not in VALID_ONBOARDING_ROLES:
        raise OnboardingTokenError("malformed_claims")
    if expires_at <= ensure_utc(now_utc):
        raise OnboardingTokenError("expired_token")
    return OnboardingTokenClaims(
        chat_id=chat_id,
        member_id=member_id,
        role=role,
        expires_at_utc=expires_at,
    )


def _sign(*, secret: str, encoded_payload: str) -> str:
    digest = hmac.new(
        secret.encode("utf-8"),
        encoded_payload.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return _b64url(digest)


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _unb64url(value: str) -> bytes:
    padded = value + "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(padded.encode("ascii"))
