from datetime import datetime, timezone

from florence.contracts import CandidateState, ChildProfile, GoogleConnection, GoogleSourceKind, HouseholdProfileItem, HouseholdProfileKind
from florence.google import GmailSyncItem, ParentCalendarSyncItem
from florence.onboarding import OnboardingState
from florence.runtime import FlorenceGoogleSyncPersistenceService, FlorenceGoogleSyncWorkerService
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
            school_labels=["Roosevelt Elementary"],
            activity_labels=["Soccer"],
            school_basics_collected=True,
            activity_basics_collected=True,
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
        "florence.runtime.services.list_recent_gmail_sync_items",
        lambda **_: [
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
    )
    monkeypatch.setattr(
        "florence.runtime.services.list_recent_parent_calendar_sync_items",
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
    pending = store.list_imported_candidates(
        household_id="hh_123",
        member_id="mem_123",
        state=CandidateState.PENDING_REVIEW,
    )
    assert len(pending) == 2
    store.close()
