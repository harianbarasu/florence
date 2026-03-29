"""Signed Florence onboarding link payloads."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import asdict, dataclass
from datetime import datetime


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("utf-8").rstrip("=")


def _base64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _sign_token(encoded_payload: str, state_secret: str) -> str:
    digest = hmac.new(
        state_secret.encode("utf-8"),
        encoded_payload.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return _base64url_encode(digest)


@dataclass(slots=True)
class FlorenceOnboardingLinkState:
    household_id: str
    member_id: str
    thread_id: str
    issued_at_ms: int


def encode_onboarding_link_state(payload: FlorenceOnboardingLinkState, state_secret: str) -> str:
    encoded_payload = _base64url_encode(json.dumps(asdict(payload), separators=(",", ":")).encode("utf-8"))
    return f"{encoded_payload}.{_sign_token(encoded_payload, state_secret)}"


def decode_onboarding_link_state(
    raw_token: str,
    state_secret: str,
    *,
    max_age_seconds: int = 30 * 24 * 60 * 60,
    now_ms: int | None = None,
) -> FlorenceOnboardingLinkState:
    encoded_payload, _, signature = raw_token.partition(".")
    if not encoded_payload or not signature:
        raise ValueError("invalid_onboarding_link_state")

    expected_signature = _sign_token(encoded_payload, state_secret)
    if not hmac.compare_digest(expected_signature, signature):
        raise ValueError("invalid_onboarding_link_state_signature")

    payload = json.loads(_base64url_decode(encoded_payload).decode("utf-8"))
    issued_at_ms = int(payload.get("issued_at_ms") or 0)
    current_ms = now_ms if now_ms is not None else int(datetime.now().timestamp() * 1000)
    if current_ms - issued_at_ms > max_age_seconds * 1000:
        raise ValueError("onboarding_link_state_expired")

    try:
        return FlorenceOnboardingLinkState(
            household_id=str(payload["household_id"]),
            member_id=str(payload["member_id"]),
            thread_id=str(payload["thread_id"]),
            issued_at_ms=issued_at_ms,
        )
    except KeyError as exc:
        raise ValueError("invalid_onboarding_link_state_payload") from exc
