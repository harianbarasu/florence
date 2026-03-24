from datetime import datetime, timezone

from florence.contracts import CandidateState, GoogleConnection, GoogleSourceKind, HouseholdContext
from florence.google import FlorenceGoogleSyncBatch, GmailSyncItem
from florence.runtime import (
    FlorenceCandidateReviewService,
    FlorenceGoogleSyncPersistenceService,
    FlorenceOnboardingSessionService,
)
from florence.state import FlorenceStateDB


def test_google_sync_persistence_service_stores_connection_and_candidates(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    google_service = FlorenceGoogleSyncPersistenceService(store)
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    google_service.save_google_connection(connection)

    result = google_service.persist_sync_batch(
        FlorenceGoogleSyncBatch(
            connection=connection,
            context=HouseholdContext(
                household_id="hh_123",
                actor_member_id="mem_123",
                channel_id="chan_dm_123",
                visible_child_names=["Ava"],
                school_labels=[],
                activity_labels=[],
            ),
            gmail_items=[
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
    )

    assert store.get_google_connection("gconn_123") == connection
    assert len(result.candidates) == 1
    assert result.candidates[0].state == CandidateState.QUARANTINED
    assert len(store.list_imported_candidates(household_id="hh_123", member_id="mem_123")) == 1

    store.close()


def test_onboarding_service_releases_quarantined_candidates_once_grounded(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )
    google_service = FlorenceGoogleSyncPersistenceService(store)
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL,),
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    google_service.save_google_connection(connection)
    google_service.persist_sync_batch(
        FlorenceGoogleSyncBatch(
            connection=connection,
            context=HouseholdContext(
                household_id="hh_123",
                actor_member_id="mem_123",
                channel_id="chan_dm_123",
                visible_child_names=["Ava"],
                school_labels=[],
                activity_labels=[],
            ),
            gmail_items=[
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
    )

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        display_name="Maya",
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )
    onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        child_names=["Ava"],
    )
    candidates_before = store.list_imported_candidates(
        household_id="hh_123",
        member_id="mem_123",
        state=CandidateState.QUARANTINED,
    )
    assert len(candidates_before) == 1

    onboarding_service.record_school_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        school_labels=["Roosevelt Elementary"],
    )
    onboarding_service.record_activity_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        activity_labels=["Soccer"],
    )

    pending = store.list_imported_candidates(
        household_id="hh_123",
        member_id="mem_123",
        state=CandidateState.PENDING_REVIEW,
    )
    assert len(pending) == 1
    assert pending[0].source_identifier == "gmail:gmail_123"

    store.close()
