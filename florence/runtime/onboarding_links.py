"""Helpers for signed Florence onboarding URLs."""

from __future__ import annotations

import time
from dataclasses import dataclass

from florence.onboarding import (
    FlorenceOnboardingLinkState,
    decode_onboarding_link_state,
    encode_onboarding_link_state,
)


@dataclass(slots=True)
class FlorenceOnboardingLink:
    url: str
    state: FlorenceOnboardingLinkState


class FlorenceOnboardingLinkService:
    def __init__(
        self,
        *,
        public_base_url: str,
        state_secret: str,
        path: str = "/v1/florence/onboarding",
    ):
        self.public_base_url = public_base_url.rstrip("/")
        self.state_secret = state_secret
        normalized_path = path.strip() or "/v1/florence/onboarding"
        self.path = normalized_path if normalized_path.startswith("/") else f"/{normalized_path}"

    def build_link(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        now_ms: int | None = None,
    ) -> FlorenceOnboardingLink:
        state = FlorenceOnboardingLinkState(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            issued_at_ms=now_ms if now_ms is not None else int(time.time() * 1000),
        )
        token = encode_onboarding_link_state(state, self.state_secret)
        return FlorenceOnboardingLink(
            url=f"{self.public_base_url}{self.path}?token={token}",
            state=state,
        )

    def decode_token(self, raw_token: str) -> FlorenceOnboardingLinkState:
        return decode_onboarding_link_state(raw_token, self.state_secret)
