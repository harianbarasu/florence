from datetime import datetime, timezone

import httpx

from florence.contracts import CandidateState, ChildProfile, GoogleConnection, GoogleSourceKind, HouseholdProfileItem, HouseholdProfileKind
from florence.google import GmailSyncItem, GmailSyncPage, GoogleTokenResponse, ParentCalendarSyncItem
from florence.onboarding import OnboardingState
from florence.runtime import FlorenceGoogleSyncPersistenceService, FlorenceGoogleSyncWorkerService
from florence.runtime.google_services import _calendar_sync_targets
from florence.state import FlorenceStateDB


def test_google_sync_worker_fetches_and_persists_candidates(tmp_path, monkeypatch):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="access-token",
            metadata={
                "primary_calendar_id": "primary",
                "primary_calendar_summary": "Family calendar",
                "primary_calendar_timezone": "America/Los_Angeles",
            },
        )
    )
    store.upsert_onboarding_session(
        OnboardingState(
            household_id="hh_123",
            member_id="mem_123",
            thread_id="dm_thread_123",
            google_connected=True,
            child_names=["Ava"],
            metadata={
                "child_profiles": [
                    {
                        "name": "Ava",
                        "school": "Roosevelt Elementary",
                        "activities": ["Soccer"],
                    }
                ]
            },
        )
    )
    store.replace_child_profiles(
        household_id="hh_123",
        children=[ChildProfile(id="child_ava", household_id="hh_123", full_name="Ava")],
    )
    store.replace_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.SCHOOL,
        items=[
            HouseholdProfileItem(
                id="school_roosevelt",
                household_id="hh_123",
                kind=HouseholdProfileKind.SCHOOL,
                label="Roosevelt Elementary",
                member_id="mem_123",
            )
        ],
    )
    store.replace_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.ACTIVITY,
        items=[
            HouseholdProfileItem(
                id="activity_soccer",
                household_id="hh_123",
                kind=HouseholdProfileKind.ACTIVITY,
                label="Soccer",
                member_id="mem_123",
            )
        ],
    )

    monkeypatch.setattr(
        "florence.runtime.google_services.list_recent_gmail_sync_page",
        lambda **_: GmailSyncPage(
            items=[
                GmailSyncItem(
                    gmail_message_id="gmail_123",
                    thread_id="thread_123",
                    from_address="teacher@school.edu",
                    subject="Soccer practice update",
                    snippet="Practice moves to Thursday 4pm to 5pm",
                    body_text="Ava soccer practice is on September 18 from 4pm to 5pm.",
                    attachment_text=None,
                    attachment_count=0,
                    received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
                )
            ],
            next_page_token=None,
        ),
    )
    monkeypatch.setattr(
        "florence.runtime.google_services.list_recent_parent_calendar_sync_items",
        lambda **_: [
            ParentCalendarSyncItem(
                google_event_id="event_123",
                title="Ava soccer practice",
                description="Weekly team practice",
                location="North field",
                html_link=None,
                starts_at=datetime(2026, 9, 18, 23, 0, tzinfo=timezone.utc),
                ends_at=datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc),
                timezone="America/Los_Angeles",
                all_day=False,
                updated_at=None,
                calendar_summary="Family calendar",
                family_member_names=["Ava"],
            )
        ],
    )

    worker = FlorenceGoogleSyncWorkerService(store, FlorenceGoogleSyncPersistenceService(store))
    result = worker.sync_connection("gconn_123")

    assert result.connection.id == "gconn_123"
    mirrored_messages = store.search_google_gmail_messages(
        household_id="hh_123",
        connection_ids=["gconn_123"],
        newer_than_days=400,
        limit=5,
    )
    assert len(mirrored_messages) == 1
    assert mirrored_messages[0].subject == "Soccer practice update"
    mirrored_events = store.list_google_calendar_events(household_id="hh_123", limit=5)
    assert len(mirrored_events) == 1
    assert mirrored_events[0].title == "Ava soccer practice"
    pending = store.list_imported_candidates(
        household_id="hh_123",
        member_id="mem_123",
        state=CandidateState.PENDING_REVIEW,
    )
    assert len(pending) == 2
    store.close()


def test_google_sync_worker_uses_deep_bootstrap_scan_on_first_sync(tmp_path, monkeypatch):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_bootstrap",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="access-token",
            metadata={
                "primary_calendar_id": "family",
                "primary_calendar_summary": "Family calendar",
                "primary_calendar_timezone": "America/Los_Angeles",
            },
        )
    )
    observed: dict[str, object] = {}
    observed_calendar: dict[str, object] = {}

    def _fake_gmail(**kwargs):
        observed.update(kwargs)
        return GmailSyncPage(items=[], next_page_token=None)

    def _fake_calendar(**kwargs):
        observed_calendar.update(kwargs)
        return []

    monkeypatch.setenv("FLORENCE_GMAIL_BOOTSTRAP_MAX_RESULTS", "321")
    monkeypatch.setenv("FLORENCE_CALENDAR_BOOTSTRAP_MAX_RESULTS", "654")
    monkeypatch.setattr("florence.runtime.google_services.list_recent_gmail_sync_page", _fake_gmail)
    monkeypatch.setattr("florence.runtime.google_services.list_recent_parent_calendar_sync_items", _fake_calendar)

    worker = FlorenceGoogleSyncWorkerService(store, FlorenceGoogleSyncPersistenceService(store))
    result = worker.sync_connection("gconn_bootstrap", now=datetime(2026, 3, 25, 18, 0, tzinfo=timezone.utc))

    assert observed["max_results"] == 321
    assert observed["gmail_query"] == "newer_than:365d"
    assert observed_calendar["max_results"] == 654
    assert observed_calendar["past_window_days"] == 365
    assert observed_calendar["future_window_days"] == 365
    assert result.connection.metadata["gmail_bootstrap_completed_at"] == "2026-03-25T18:00:00+00:00"
    assert result.connection.metadata["gmail_last_query"] == "newer_than:365d"
    assert result.connection.metadata["calendar_bootstrap_completed_at"] == "2026-03-25T18:00:00+00:00"
    store.close()


def test_google_sync_worker_uses_incremental_window_after_bootstrap(tmp_path, monkeypatch):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_incremental",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="access-token",
            metadata={
                "primary_calendar_id": "family",
                "primary_calendar_summary": "Family calendar",
                "primary_calendar_timezone": "America/Los_Angeles",
                "gmail_bootstrap_completed_at": "2026-03-20T18:00:00+00:00",
                "gmail_last_synced_at": "2026-03-22T18:00:00+00:00",
                "calendar_bootstrap_completed_at": "2026-03-20T18:00:00+00:00",
                "calendar_last_synced_at": "2026-03-22T18:00:00+00:00",
            },
        )
    )
    observed: dict[str, object] = {}
    observed_calendar: dict[str, object] = {}

    def _fake_gmail(**kwargs):
        observed.update(kwargs)
        return []

    def _fake_calendar(**kwargs):
        observed_calendar.update(kwargs)
        return []

    monkeypatch.setenv("FLORENCE_GMAIL_INCREMENTAL_MAX_RESULTS", "111")
    monkeypatch.setenv("FLORENCE_CALENDAR_INCREMENTAL_MAX_RESULTS", "222")
    monkeypatch.setattr("florence.runtime.google_services.list_recent_gmail_sync_items", _fake_gmail)
    monkeypatch.setattr("florence.runtime.google_services.list_recent_parent_calendar_sync_items", _fake_calendar)

    worker = FlorenceGoogleSyncWorkerService(store, FlorenceGoogleSyncPersistenceService(store))
    result = worker.sync_connection("gconn_incremental", now=datetime(2026, 3, 25, 18, 0, tzinfo=timezone.utc))

    assert observed["max_results"] == 111
    assert observed["gmail_query"] == "newer_than:5d"
    assert observed_calendar["max_results"] == 222
    assert observed_calendar["past_window_days"] == 30
    assert observed_calendar["future_window_days"] == 120
    assert result.connection.metadata["gmail_last_synced_at"] == "2026-03-25T18:00:00+00:00"
    assert result.connection.metadata["gmail_last_max_results"] == 111
    assert result.connection.metadata["calendar_last_max_results"] == 222
    store.close()


def test_google_sync_worker_bootstrap_walks_multiple_gmail_pages_and_persists_rows(tmp_path, monkeypatch):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_multipage",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="access-token",
            metadata={
                "primary_calendar_id": "family",
                "primary_calendar_summary": "Family calendar",
                "primary_calendar_timezone": "America/Los_Angeles",
            },
        )
    )
    observed_page_tokens: list[str | None] = []

    def _fake_gmail_page(**kwargs):
        observed_page_tokens.append(kwargs.get("page_token"))
        if kwargs.get("page_token") == "page-2":
            return GmailSyncPage(
                items=[
                    GmailSyncItem(
                        gmail_message_id="gmail_2",
                        thread_id="thread_2",
                        from_address="coach@example.com",
                        subject="Practice moved",
                        snippet="Practice moved to Thursday.",
                        body_text="Practice moved to Thursday.",
                        attachment_text=None,
                        attachment_count=0,
                        received_at=datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc),
                    )
                ],
                next_page_token=None,
            )
        return GmailSyncPage(
            items=[
                GmailSyncItem(
                    gmail_message_id="gmail_1",
                    thread_id="thread_1",
                    from_address="school@example.com",
                    subject="Picture day reminder",
                    snippet="Picture day is Friday.",
                    body_text="Picture day is Friday.",
                    attachment_text=None,
                    attachment_count=0,
                    received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
                )
            ],
            next_page_token="page-2",
        )

    monkeypatch.setattr("florence.runtime.google_services.list_recent_gmail_sync_page", _fake_gmail_page)
    monkeypatch.setattr("florence.runtime.google_services.list_recent_parent_calendar_sync_items", lambda **_: [])

    worker = FlorenceGoogleSyncWorkerService(store, FlorenceGoogleSyncPersistenceService(store))
    result = worker.sync_connection("gconn_multipage")

    assert observed_page_tokens == [None, "page-2"]
    mirrored_messages = store.search_google_gmail_messages(
        household_id="hh_123",
        connection_ids=["gconn_multipage"],
        newer_than_days=400,
        limit=10,
    )
    assert [item.gmail_message_id for item in mirrored_messages] == ["gmail_2", "gmail_1"]
    assert result.connection.metadata["gmail_bootstrap_completed_at"] is not None
    store.close()


def test_google_sync_worker_honors_calendar_preferences_and_keeps_non_primary_ids_distinct(tmp_path, monkeypatch):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_multi",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GOOGLE_CALENDAR,),
            access_token="access-token",
            metadata={
                "primary_calendar_id": "family",
                "primary_calendar_summary": "Family calendar",
                "primary_calendar_timezone": "America/Los_Angeles",
                "available_calendars": [
                    {
                        "id": "family",
                        "summary": "Family calendar",
                        "timezone": "America/Los_Angeles",
                        "primary": True,
                    },
                    {
                        "id": "work",
                        "summary": "Work calendar",
                        "timezone": "America/Los_Angeles",
                    },
                    {
                        "id": "shared_secondary",
                        "summary": "Shared secondary calendar",
                        "timezone": "America/Los_Angeles",
                    },
                    {
                        "id": "ignored",
                        "summary": "Ignored calendar",
                        "timezone": "America/Los_Angeles",
                    },
                ],
                "calendar_preferences": {
                    "family": {
                        "usage_mode": "planning_and_conflicts",
                        "detail_visibility": "full_details",
                    },
                    "work": {
                        "usage_mode": "conflicts_only",
                        "detail_visibility": "busy_only",
                    },
                    "shared_secondary": {
                        "usage_mode": "planning_and_conflicts",
                        "detail_visibility": "full_details",
                    },
                    "ignored": {
                        "usage_mode": "ignore",
                    },
                },
            },
        )
    )
    store.upsert_onboarding_session(
        OnboardingState(
            household_id="hh_123",
            member_id="mem_123",
            thread_id="dm_thread_123",
            google_connected=True,
            child_names=["Ava"],
            metadata={
                "child_profiles": [
                    {
                        "name": "Ava",
                        "school": "Roosevelt Elementary",
                        "activities": ["Soccer"],
                    }
                ]
            },
        )
    )
    store.replace_child_profiles(
        household_id="hh_123",
        children=[ChildProfile(id="child_ava", household_id="hh_123", full_name="Ava")],
    )
    observed_calendar_ids: list[str] = []

    monkeypatch.setattr(
        "florence.runtime.google_services.list_recent_gmail_sync_page",
        lambda **_: GmailSyncPage(items=[], next_page_token=None),
    )

    def _fake_calendar_fetch(*, calendar, **kwargs):
        observed_calendar_ids.append(calendar.id)
        return [
            ParentCalendarSyncItem(
                google_event_id="event_shared",
                title="Ava soccer practice",
                description="Weekly team practice",
                location="North field",
                html_link=None,
                starts_at=datetime(2026, 9, 18, 23, 0, tzinfo=timezone.utc),
                ends_at=datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc),
                timezone="America/Los_Angeles",
                all_day=False,
                updated_at=None,
                calendar_summary=calendar.summary,
                family_member_names=["Ava"],
                calendar_id=calendar.id,
                calendar_primary=calendar.primary,
            )
        ]

    monkeypatch.setattr("florence.runtime.google_services.list_recent_parent_calendar_sync_items", _fake_calendar_fetch)

    worker = FlorenceGoogleSyncWorkerService(store, FlorenceGoogleSyncPersistenceService(store))
    result = worker.sync_connection("gconn_multi")

    assert observed_calendar_ids == ["family", "work", "shared_secondary"]
    candidates = store.list_imported_candidates(household_id="hh_123", member_id="mem_123")
    assert len(candidates) == 2
    assert {candidate.source_identifier for candidate in candidates} == {
        "google_calendar:event_shared",
        "google_calendar:shared_secondary:event_shared",
    }
    assert {candidate.metadata["calendar_id"] for candidate in candidates} == {"family", "shared_secondary"}
    assert result.connection.metadata["last_calendar_item_count"] == 3
    store.close()


def test_google_sync_targets_skip_florence_managed_shared_calendars_from_any_connection():
    targets = _calendar_sync_targets(
        {
            "available_calendars": [
                {
                    "id": "family",
                    "summary": "Family calendar",
                    "timezone": "America/Los_Angeles",
                    "primary": True,
                },
                {
                    "id": "florence_shared",
                    "summary": "Florence shared household calendar",
                    "timezone": "America/Los_Angeles",
                },
            ],
            "calendar_preferences": {
                "family": {
                    "usage_mode": "planning_and_conflicts",
                    "detail_visibility": "full_details",
                },
                "florence_shared": {
                    "usage_mode": "planning_and_conflicts",
                    "detail_visibility": "full_details",
                },
            },
            "florence_managed_calendar_ids": ["florence_shared"],
        }
    )

    assert [target.calendar.id for target in targets] == ["family"]


def test_google_sync_worker_refreshes_when_expiry_metadata_is_missing(tmp_path, monkeypatch):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_missing_expiry",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="stale-token",
            refresh_token="refresh-token",
            access_token_expires_at=None,
            metadata={
                "primary_calendar_id": "family",
                "primary_calendar_summary": "Family calendar",
                "primary_calendar_timezone": "America/Los_Angeles",
            },
        )
    )

    observed_tokens: list[str] = []

    def _fake_refresh(**_kwargs):
        return GoogleTokenResponse(
            access_token="fresh-token",
            refresh_token="refresh-token",
            expires_in=3600,
        )

    def _fake_gmail(**kwargs):
        observed_tokens.append(kwargs["access_token"])
        return GmailSyncPage(items=[], next_page_token=None)

    monkeypatch.setattr("florence.runtime.google_services.refresh_google_access_token", _fake_refresh)
    monkeypatch.setattr("florence.runtime.google_services.list_recent_gmail_sync_page", _fake_gmail)
    monkeypatch.setattr("florence.runtime.google_services.list_recent_parent_calendar_sync_items", lambda **_: [])

    worker = FlorenceGoogleSyncWorkerService(store, FlorenceGoogleSyncPersistenceService(store))
    result = worker.sync_connection(
        "gconn_missing_expiry",
        now=datetime(2026, 4, 8, 7, 0, tzinfo=timezone.utc),
        client_id="google-client",
        client_secret="google-secret",
    )

    assert observed_tokens == ["fresh-token"]
    assert result.connection.access_token == "fresh-token"
    assert result.connection.access_token_expires_at == "2026-04-08T08:00:00+00:00"
    store.close()


def test_google_sync_worker_retries_gmail_fetch_once_after_google_401(tmp_path, monkeypatch):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_retry",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="stale-token",
            refresh_token="refresh-token",
            access_token_expires_at="2026-04-08T12:00:00+00:00",
            metadata={
                "primary_calendar_id": "family",
                "primary_calendar_summary": "Family calendar",
                "primary_calendar_timezone": "America/Los_Angeles",
            },
        )
    )

    observed_tokens: list[str] = []

    def _fake_refresh(**_kwargs):
        return GoogleTokenResponse(
            access_token="fresh-token",
            refresh_token="refresh-token",
            expires_in=3600,
        )

    def _fake_gmail(**kwargs):
        observed_tokens.append(kwargs["access_token"])
        if kwargs["access_token"] == "stale-token":
            request = httpx.Request("GET", "https://gmail.googleapis.com/gmail/v1/users/me/messages")
            response = httpx.Response(401, request=request)
            raise httpx.HTTPStatusError("401 Unauthorized", request=request, response=response)
        return GmailSyncPage(items=[], next_page_token=None)

    monkeypatch.setattr("florence.runtime.google_services.refresh_google_access_token", _fake_refresh)
    monkeypatch.setattr("florence.runtime.google_services.list_recent_gmail_sync_page", _fake_gmail)
    monkeypatch.setattr("florence.runtime.google_services.list_recent_parent_calendar_sync_items", lambda **_: [])

    worker = FlorenceGoogleSyncWorkerService(store, FlorenceGoogleSyncPersistenceService(store))
    result = worker.sync_connection(
        "gconn_retry",
        now=datetime(2026, 4, 8, 7, 0, tzinfo=timezone.utc),
        client_id="google-client",
        client_secret="google-secret",
    )

    assert observed_tokens == ["stale-token", "fresh-token"]
    assert result.connection.access_token == "fresh-token"
    assert result.connection.access_token_expires_at == "2026-04-08T08:00:00+00:00"
    store.close()
