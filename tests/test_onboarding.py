from datetime import datetime, timedelta, timezone

import pytest

from florence.onboarding import (
    OnboardingTokenError,
    create_onboarding_token,
    verify_onboarding_token,
)


def test_onboarding_token_round_trips_claims():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    token = create_onboarding_token(
        secret="setup-secret",
        chat_id="chat-123",
        member_id="member-123",
        role="primary",
        expires_at_utc=now + timedelta(hours=1),
    )

    claims = verify_onboarding_token(
        secret="setup-secret",
        token=token,
        now_utc=now,
    )

    assert claims.chat_id == "chat-123"
    assert claims.member_id == "member-123"
    assert claims.role == "primary"


def test_onboarding_token_rejects_tampering_and_expiry():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    token = create_onboarding_token(
        secret="setup-secret",
        chat_id="chat-123",
        member_id="member-123",
        role="partner",
        expires_at_utc=now + timedelta(hours=1),
    )

    tampered = f"{token[:-1]}{'A' if token[-1] != 'A' else 'B'}"
    with pytest.raises(OnboardingTokenError):
        verify_onboarding_token(
            secret="setup-secret",
            token=tampered,
            now_utc=now,
        )

    with pytest.raises(OnboardingTokenError):
        verify_onboarding_token(
            secret="setup-secret",
            token=token,
            now_utc=now + timedelta(hours=2),
        )
