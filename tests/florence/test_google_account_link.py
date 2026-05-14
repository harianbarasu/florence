import time
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

from florence.contracts import GoogleConnection, GoogleSourceKind
from florence.google import GoogleCalendarMetadata, GoogleTokenResponse
from florence.runtime import (
    FlorenceCandidateReviewService,
    FlorenceGoogleAccountLinkService,
    FlorenceGoogleSyncWorkerService,
    FlorenceOnboardingSessionService,
)
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
        "florence.runtime.google_services.exchange_google_code_for_tokens",
        lambda **_: GoogleTokenResponse(
            access_token="access-token",
            refresh_token="refresh-token",
            expires_in=3600,
        ),
    )
    monkeypatch.setattr("florence.runtime.google_services.fetch_google_user_email", lambda **_: "parent@example.com")
    monkeypatch.setattr(
        "florence.runtime.google_services.list_google_calendars",
        lambda **_: [
            GoogleCalendarMetadata(
                id="primary",
                summary="Family",
                timezone="America/Los_Angeles",
                access_role="owner",
                primary=True,
            )
        ],
    )

    raw_state = parse_qs(urlparse(link.url).query)["state"][0]
    result = service.handle_callback(code="auth-code", raw_state=raw_state)

    assert result.connection.email == "parent@example.com"
    assert result.connection.connected_scopes == (GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR)
    assert result.onboarding_transition.state.google_connected is True
    saved = store.list_google_connections(household_id="hh_123", member_id="mem_123")
    assert len(saved) == 1
    assert saved[0].metadata["available_calendars"][0]["id"] == "primary"
    store.close()


def test_google_account_link_callback_keeps_multiple_accounts_for_same_member(tmp_path, monkeypatch):
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

    def exchange_google_code_for_tokens(**kwargs):
        code = kwargs["code"]
        return GoogleTokenResponse(
            access_token=f"access-token-{code}",
            refresh_token=f"refresh-token-{code}",
            expires_in=3600,
        )

    def fetch_google_user_email(*, access_token, **_):
        return {
            "access-token-personal": "jackson.personal@example.com",
            "access-token-family": "jackson.family@example.com",
        }[access_token]

    def list_google_calendars(*, access_token, **_):
        email = fetch_google_user_email(access_token=access_token)
        return [
            GoogleCalendarMetadata(
                id=f"primary-{email}",
                summary=f"Primary {email}",
                timezone="America/Los_Angeles",
                access_role="owner",
                primary=True,
            )
        ]

    monkeypatch.setattr("florence.runtime.google_services.exchange_google_code_for_tokens", exchange_google_code_for_tokens)
    monkeypatch.setattr("florence.runtime.google_services.fetch_google_user_email", fetch_google_user_email)
    monkeypatch.setattr("florence.runtime.google_services.list_google_calendars", list_google_calendars)

    personal_link = service.build_connect_link(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        now_ms=int(time.time() * 1000),
        nonce="nonce-personal",
    )
    family_link = service.build_connect_link(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        now_ms=int(time.time() * 1000) + 1,
        nonce="nonce-family",
    )

    personal_state = parse_qs(urlparse(personal_link.url).query)["state"][0]
    family_state = parse_qs(urlparse(family_link.url).query)["state"][0]
    personal_result = service.handle_callback(code="personal", raw_state=personal_state)
    family_result = service.handle_callback(code="family", raw_state=family_state)

    saved = store.list_google_connections(household_id="hh_123", member_id="mem_123")
    saved_by_email = {connection.email: connection for connection in saved}
    assert personal_result.connection.id != family_result.connection.id
    assert set(saved_by_email) == {"jackson.personal@example.com", "jackson.family@example.com"}
    assert all(connection.active for connection in saved)
    assert saved_by_email["jackson.personal@example.com"].refresh_token == "refresh-token-personal"
    assert saved_by_email["jackson.family@example.com"].metadata["primary_calendar_id"] == "primary-jackson.family@example.com"
    assert onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    ).google_connected is True
    store.close()


def test_google_sync_household_runs_every_active_google_account(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    connections = [
        GoogleConnection(
            id="gconn_personal",
            household_id="hh_123",
            member_id="mem_123",
            email="jackson.personal@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            active=True,
        ),
        GoogleConnection(
            id="gconn_family",
            household_id="hh_123",
            member_id="mem_123",
            email="jackson.family@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            active=True,
        ),
        GoogleConnection(
            id="gconn_inactive",
            household_id="hh_123",
            member_id="mem_123",
            email="old@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            active=False,
        ),
    ]
    for connection in connections:
        store.upsert_google_connection(connection)
    worker = FlorenceGoogleSyncWorkerService(store, google_sync_service=SimpleNamespace())
    calls: list[str] = []

    def fake_sync_connection(connection_id, **_):
        calls.append(connection_id)
        return SimpleNamespace(
            connection=store.get_google_connection(connection_id),
            sync_result=SimpleNamespace(candidates=[]),
        )

    worker.sync_connection = fake_sync_connection

    results = worker.sync_household(household_id="hh_123")

    assert {result.connection.id for result in results} == {"gconn_personal", "gconn_family"}
    assert set(calls) == {"gconn_personal", "gconn_family"}
    assert "gconn_inactive" not in calls
    store.close()
