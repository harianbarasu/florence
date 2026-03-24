import time
from urllib.parse import parse_qs, urlparse

from florence.contracts import GoogleSourceKind
from florence.google import GoogleCalendarMetadata, GoogleTokenResponse
from florence.runtime import FlorenceCandidateReviewService, FlorenceGoogleAccountLinkService, FlorenceOnboardingSessionService
from florence.state import FlorenceStateDB


def test_google_account_link_callback_persists_connection_and_marks_onboarding_connected(tmp_path, monkeypatch):
    store = FlorenceStateDB(tmp_path / "florence.db")
    onboarding_service = FlorenceOnboardingSessionService(
        store,
        candidate_review_service=FlorenceCandidateReviewService(store),
    )
    onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    service = FlorenceGoogleAccountLinkService(
        store,
        onboarding_service,
        client_id="client-id",
        client_secret="client-secret",
        redirect_uri="https://example.com/callback",
        state_secret="state-secret",
    )
    link = service.build_connect_link(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        now_ms=int(time.time() * 1000),
        nonce="nonce-123",
    )

    monkeypatch.setattr(
        "florence.runtime.services.exchange_google_code_for_tokens",
        lambda **_: GoogleTokenResponse(
            access_token="access-token",
            refresh_token="refresh-token",
            expires_in=3600,
        ),
    )
    monkeypatch.setattr("florence.runtime.services.fetch_google_user_email", lambda **_: "parent@example.com")
    monkeypatch.setattr(
        "florence.runtime.services.fetch_primary_google_calendar",
        lambda **_: GoogleCalendarMetadata(
            id="primary",
            summary="Family",
            timezone="America/Los_Angeles",
            access_role="owner",
        ),
    )

    raw_state = parse_qs(urlparse(link.url).query)["state"][0]
    result = service.handle_callback(code="auth-code", raw_state=raw_state)

    assert result.connection.email == "parent@example.com"
    assert result.connection.connected_scopes == (GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR)
    assert result.onboarding_transition.state.google_connected is True
    saved = store.list_google_connections(household_id="hh_123", member_id="mem_123")
    assert len(saved) == 1
    store.close()
