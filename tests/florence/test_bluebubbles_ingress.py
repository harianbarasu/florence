from florence.bluebubbles import (
    BlueBubblesInboundMessage,
    FlorenceBlueBubblesIngressService,
    FlorenceResolvedBlueBubblesMessage,
)
from florence.contracts import CandidateState, GoogleConnection, GoogleSourceKind, HouseholdContext, HouseholdEvent
from florence.google import FlorenceGoogleSyncBatch, GmailSyncItem
from florence.runtime import (
    FlorenceCandidateReviewService,
    FlorenceGoogleSyncPersistenceService,
    FlorenceHouseholdQueryService,
    FlorenceOnboardingSessionService,
)
from florence.state import FlorenceStateDB


def _build_ingress(store: FlorenceStateDB) -> FlorenceBlueBubblesIngressService:
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )
    google_sync_service = FlorenceGoogleSyncPersistenceService(store)
    google_sync_service.save_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="parent@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            metadata={"primary_calendar_timezone": "America/Los_Angeles"},
        )
    )
    return FlorenceBlueBubblesIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )


def test_dm_onboarding_advances_from_parent_name_to_google_prompt(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    ingress = _build_ingress(store)

    result = ingress.handle_message(
        FlorenceResolvedBlueBubblesMessage(
            household_id="hh_123",
            member_id="mem_123",
            thread_id="dm_thread_123",
            message=BlueBubblesInboundMessage(
                source_message_id="msg_1",
                chat_guid="chat:dm",
                body="Maya",
                sender_handle="+15555550123",
                is_from_me=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text is not None
    assert "Google" in result.reply_text
    store.close()


def test_dm_review_yes_confirms_pending_candidate_and_creates_group_announcement(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    ingress = _build_ingress(store)
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )
    google_sync_service = FlorenceGoogleSyncPersistenceService(store)
    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        display_name="Maya",
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        child_names=["Ava"],
    )
    onboarding_service.record_school_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        school_labels=["Roosevelt Elementary"],
    )
    onboarding_service.record_activity_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        activity_labels=["Soccer"],
    )
    google_sync_service.persist_sync_batch(
        FlorenceGoogleSyncBatch(
            connection=store.get_google_connection("gconn_123"),
            context=HouseholdContext(
                household_id="hh_123",
                actor_member_id="mem_123",
                channel_id="dm_thread_123",
                visible_child_names=["Ava"],
                school_labels=["Roosevelt Elementary"],
                activity_labels=["Soccer"],
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
                    received_at=None,
                )
            ],
        )
    )

    review_prompt = review_service.build_next_review_prompt(household_id="hh_123", member_id="mem_123")
    assert review_prompt is not None

    result = ingress.handle_message(
        FlorenceResolvedBlueBubblesMessage(
            household_id="hh_123",
            member_id="mem_123",
            thread_id="dm_thread_123",
            message=BlueBubblesInboundMessage(
                source_message_id="msg_2",
                chat_guid="chat:dm",
                body="yes",
                sender_handle="+15555550123",
                is_from_me=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text is not None
    assert "Confirmed" in result.reply_text
    assert result.group_announcement is not None
    confirmed = store.list_imported_candidates(
        household_id="hh_123",
        member_id="mem_123",
        state=CandidateState.CONFIRMED,
    )
    assert len(confirmed) == 1
    assert len(store.list_household_events(household_id="hh_123")) == 1
    store.close()


def test_group_message_can_activate_group_and_answer_schedule_question(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )
    query_service = FlorenceHouseholdQueryService(store)
    ingress = FlorenceBlueBubblesIngressService(
        store,
        onboarding_service,
        review_service,
        query_service,
    )
    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        display_name="Maya",
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        child_names=["Ava"],
    )
    onboarding_service.record_school_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        school_labels=["Roosevelt Elementary"],
    )
    onboarding_service.record_activity_basics(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        activity_labels=["Soccer"],
    )

    activation = ingress.handle_message(
        FlorenceResolvedBlueBubblesMessage(
            household_id="hh_123",
            member_id="mem_123",
            thread_id="group_thread_123",
            message=BlueBubblesInboundMessage(
                source_message_id="msg_3",
                chat_guid="chat;group",
                body="Hi Florence",
                sender_handle="+15555550123",
                is_from_me=False,
                participants=["+15555550123", "+15555550124"],
            ),
        )
    )
    assert activation.consumed is True
    assert activation.reply_text is not None

    store.upsert_household_event(
        HouseholdEvent(
            id="evt_123",
            household_id="hh_123",
            title="Ava soccer practice",
            starts_at="2026-03-18T23:00:00+00:00",
            ends_at="2026-03-19T00:00:00+00:00",
            timezone="America/Los_Angeles",
        )
    )
    summary = ingress.handle_message(
        FlorenceResolvedBlueBubblesMessage(
            household_id="hh_123",
            member_id="mem_123",
            thread_id="group_thread_123",
            message=BlueBubblesInboundMessage(
                source_message_id="msg_4",
                chat_guid="chat;group",
                body="What is happening this week?",
                sender_handle="+15555550123",
                is_from_me=False,
                participants=["+15555550123", "+15555550124"],
            ),
        )
    )
    assert summary.consumed is True
    assert summary.reply_text is not None
    assert "family plan" in summary.reply_text
    store.close()
