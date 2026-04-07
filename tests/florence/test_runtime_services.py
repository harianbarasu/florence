from datetime import datetime, timedelta, timezone

from florence.contracts import (
    CandidateState,
    Channel,
    ChannelMessage,
    ChannelMessageRole,
    ChannelType,
    GoogleConnection,
    GoogleSourceKind,
    Household,
    HouseholdContext,
    HouseholdNudge,
    HouseholdNudgeStatus,
    HouseholdNudgeTargetKind,
    HouseholdProfileItem,
    HouseholdProfileKind,
    HouseholdSourceVisibility,
    HouseholdRoutineStatus,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
    ImportedCandidate,
    Member,
    MemberRole,
)
from florence.google import FlorenceGoogleSyncBatch, GmailSyncItem, ParentCalendarSyncItem
from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.protocol_types import CANDIDATE_REVIEW_PROMPT_KIND
from florence.onboarding import OnboardingStage
from florence.runtime import (
    FlorenceCandidateReviewService,
    FlorenceGoogleSyncPersistenceService,
    FlorenceGroupShareService,
    FlorenceHouseholdManagerService,
    FlorenceOnboardingSessionService,
)
from florence.runtime.delivery import FlorenceChannelDeliveryService
from florence.runtime.operations import FlorenceHouseholdOperationsService
from florence.state import FlorenceStateDB


class _StubGroupShareChatService:
    def __init__(self, *, promotion_text: str | None = None) -> None:
        self.promotion_text = promotion_text
        self.promotion_calls: list[dict[str, object]] = []

    def compose_operator_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        kind: str,
        payload=None,
        conversation_history=None,  # noqa: ARG002
    ) -> str | None:
        assert kind == "group_promotion"
        payload = dict(payload or {})
        self.promotion_calls.append(
            {
                "household_id": household_id,
                "channel_id": channel_id,
                "actor_member_id": actor_member_id,
                "source_text": payload.get("source_text"),
            }
        )
        return self.promotion_text


class _FakeLinqClient:
    def __init__(self) -> None:
        self.sent: list[dict[str, str]] = []

    def send_text(self, *, chat_id: str, message: str) -> None:
        self.sent.append({"chat_id": chat_id, "message": message})


class _StubReviewPromptChatService:
    def compose_operator_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        kind: str,
        payload=None,
        conversation_history=None,  # noqa: ARG002
    ) -> str:
        assert kind == "review_prompt"
        payload = dict(payload or {})
        candidate = dict(payload.get("candidate") or {})
        source_prompt = payload.get("source_prompt")
        title = str(candidate.get("title") or "").strip() or "This looks worth double-checking."
        lines = [title]
        if source_prompt:
            lines.append(str(source_prompt).strip())
        lines.append("Reply yes if I should add it, no if it's wrong, or skip for later.")
        return " ".join(lines)


def test_google_sync_persistence_service_stores_connection_and_candidates(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    google_service = FlorenceGoogleSyncPersistenceService(store)
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    store.upsert_google_connection(connection)

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
                    from_address="Ms. Kim <teacher@roosevelt.k12.ca.us>",
                    subject="Roosevelt Elementary soccer practice update",
                    snippet="ParentSquare reminder",
                    body_text="Ava soccer practice is on September 18 from 4pm to 5pm.",
                    attachment_text=None,
                    attachment_count=0,
                    received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
                )
            ],
            calendar_items=[
                ParentCalendarSyncItem(
                    google_event_id="event_123",
                    title="Soccer practice",
                    description="Weekly team practice on the north field",
                    location="North Field",
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
    )

    assert store.get_google_connection("gconn_123") == connection
    assert len(result.candidates) == 2
    assert result.candidates[0].state == CandidateState.QUARANTINED
    assert result.candidates[0].metadata["source_visibility"] == "needs_classification"
    assert len(store.list_imported_candidates(household_id="hh_123", member_id="mem_123")) == 2
    source_rules = store.list_household_source_rules(household_id="hh_123", source_kind=GoogleSourceKind.GMAIL)
    assert source_rules == []
    household = store.get_household("hh_123")
    assert household is not None
    grounding_hints = household.settings["grounding_hints"]
    assert grounding_hints["schools"][0]["label"] == "Roosevelt Elementary"
    assert grounding_hints["schools"][0]["domains"] == ["roosevelt.k12.ca.us"]
    assert grounding_hints["schools"][0]["platforms"] == ["ParentSquare"]
    assert grounding_hints["activities"][0]["label"] == "Soccer"
    assert grounding_hints["activities"][0]["locations"] == ["North Field"]

    store.close()


def test_candidate_review_prompt_asks_once_for_unknown_source_classification(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_123",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:gmail_123",
            title="Weekend class info",
            summary="Linda <linda@musicalbeginnings.com> - Musical Beginnings schedule update.",
            state=CandidateState.PENDING_REVIEW,
            metadata={
                "from_address": "Linda <linda@musicalbeginnings.com>",
                "confirmation_question": "Should I add Weekend class info to your household plan?",
            },
        )
    )

    prompt = review_service.build_next_review_prompt(household_id="hh_123", member_id="mem_123")

    assert prompt is not None
    assert "Reply share to treat future items from this source as household-shared" in prompt.text
    store.close()


def test_candidate_review_prompt_returns_latest_pending_candidate(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_steady_123",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:gmail_steady_123",
            title="Steady-state item",
            summary="Should be reviewed later.",
            state=CandidateState.PENDING_REVIEW,
            metadata={
                "confirmation_question": "Should I add this later item to your household plan?",
            },
        )
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_bootstrap_123",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:gmail_bootstrap_123",
            title="Bootstrap item",
            summary="Should be reviewed first.",
            state=CandidateState.PENDING_REVIEW,
            metadata={
                "confirmation_question": "Should I add this first item to your household plan?",
            },
        )
    )

    prompt = review_service.build_next_dm_review_prompt(household_id="hh_123", member_id="mem_123")

    assert prompt is not None
    assert prompt.candidate.id == "cand_bootstrap_123"
    store.close()


def test_google_sync_persistence_service_marks_grounded_candidates_as_steady_state_review(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    google_service = FlorenceGoogleSyncPersistenceService(store)
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL,),
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    store.upsert_google_connection(connection)

    result = google_service.persist_sync_batch(
        FlorenceGoogleSyncBatch(
            connection=connection,
            context=HouseholdContext(
                household_id="hh_123",
                actor_member_id="mem_123",
                channel_id="chan_dm_123",
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
                    received_at=datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
                )
            ],
        )
    )

    assert result.candidates[0].state == CandidateState.PENDING_REVIEW
    assert result.candidates[0].metadata["source_visibility"] == "needs_classification"
    store.close()


def test_google_sync_persistence_service_preserves_review_metadata_and_terminal_state(tmp_path, monkeypatch):
    store = FlorenceStateDB(tmp_path / "florence.db")
    google_service = FlorenceGoogleSyncPersistenceService(store)
    connection = GoogleConnection(
        id="gconn_123",
        household_id="hh_123",
        member_id="mem_123",
        email="parent@example.com",
        connected_scopes=(GoogleSourceKind.GMAIL,),
        metadata={"primary_calendar_timezone": "America/Los_Angeles"},
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_123",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:gmail_123",
            title="Young Minds invoice",
            summary="Existing candidate already reviewed.",
            state=CandidateState.CONFIRMED,
            metadata={
                "review_nudged_at": "2026-03-30T03:30:00Z",
                "confirmed_event_id": "evt_123",
            },
        )
    )

    monkeypatch.setattr(
        "florence.runtime.google_services.build_google_import_candidates",
        lambda _batch: type(
            "_FakeSyncResult",
            (),
            {
                "candidates": [
                    ImportedCandidate(
                        id="cand_123",
                        household_id="hh_123",
                        member_id="mem_123",
                        source_kind=GoogleSourceKind.GMAIL,
                        source_identifier="gmail:gmail_123",
                        title="Young Minds invoice",
                        summary="Re-imported from Gmail.",
                        state=CandidateState.PENDING_REVIEW,
                        metadata={"confirmation_question": "Should I add this invoice to your household plan?"},
                    )
                ],
                "skipped_count": 0,
            },
        )(),
    )

    result = google_service.persist_sync_batch(
        FlorenceGoogleSyncBatch(
            connection=connection,
            context=HouseholdContext(
                household_id="hh_123",
                actor_member_id="mem_123",
                channel_id="chan_dm_123",
            ),
        )
    )

    persisted = result.candidates[0]
    stored = store.get_imported_candidate("cand_123")

    assert persisted.state == CandidateState.CONFIRMED
    assert persisted.metadata["review_nudged_at"] == "2026-03-30T03:30:00Z"
    assert persisted.metadata["confirmed_event_id"] == "evt_123"
    assert persisted.metadata["confirmation_question"] == "Should I add this invoice to your household plan?"
    assert stored == persisted
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
    store.upsert_google_connection(connection)
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

    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        text="7",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        text="Roosevelt Elementary",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        text="Soccer",
    )

    pending = store.list_imported_candidates(
        household_id="hh_123",
        member_id="mem_123",
        state=CandidateState.PENDING_REVIEW,
    )
    assert len(pending) == 1
    assert pending[0].source_identifier == "gmail:gmail_123"
    assert [child.full_name for child in store.list_child_profiles(household_id="hh_123")] == ["Ava"]
    assert [
        item.label
        for item in store.list_household_profile_items(
            household_id="hh_123",
            kind=HouseholdProfileKind.SCHOOL,
        )
    ] == ["Roosevelt Elementary"]
    assert [
        item.label
        for item in store.list_household_profile_items(
            household_id="hh_123",
            kind=HouseholdProfileKind.ACTIVITY,
        )
    ] == ["Soccer"]

    store.close()


def test_second_parent_early_onboarding_does_not_clear_existing_household_grounding(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    onboarding_service = FlorenceOnboardingSessionService(store)

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_primary",
        thread_id="dm_primary",
        display_name="Maya",
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_primary",
        thread_id="dm_primary",
    )
    onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_primary",
        thread_id="dm_primary",
        child_names=["Ava"],
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_primary",
        thread_id="dm_primary",
        text="7",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_primary",
        thread_id="dm_primary",
        text="Roosevelt Elementary",
    )

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_second",
        thread_id="dm_second",
        display_name="Chris",
    )

    assert [child.full_name for child in store.list_child_profiles(household_id="hh_123")] == ["Ava"]
    assert [
        item.label
        for item in store.list_household_profile_items(
            household_id="hh_123",
            kind=HouseholdProfileKind.SCHOOL,
        )
    ] == ["Roosevelt Elementary"]
    store.close()


def test_onboarding_service_ignores_generic_planning_message_during_child_detail_collection(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    onboarding_service = FlorenceOnboardingSessionService(store)

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

    transition = onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        text="I need help figuring out Friday pickup while this is syncing",
    )

    session = onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )
    assert transition.changed is False
    assert session.stage == OnboardingStage.COLLECT_CHILD_AGE
    assert session.child_profiles == [{"name": "Ava"}]
    store.close()


def test_onboarding_prompt_surfaces_google_grounding_suggestions(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
            settings={
                "grounding_hints": {
                    "schools": [
                        {
                            "label": "Roosevelt Elementary",
                            "domains": ["roosevelt.k12.ca.us"],
                            "platforms": ["ParentSquare"],
                            "contacts": ["Ms. Kim"],
                        }
                    ],
                    "activities": [
                        {
                            "label": "Soccer",
                            "locations": ["North Field"],
                            "contacts": ["Coach Ben"],
                        }
                    ],
                    "contacts": ["Ms. Kim", "Coach Ben"],
                    "locations": ["North Field"],
                }
            },
        )
    )
    onboarding_service = FlorenceOnboardingSessionService(store)

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
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        text="7",
    )
    school_prompt = onboarding_service.get_prompt(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )

    assert school_prompt is not None
    assert "Google already surfaced a few likely school signals:" in school_prompt.text
    assert "Roosevelt Elementary" in school_prompt.text
    assert "ParentSquare" in school_prompt.text

    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        text="Roosevelt Elementary",
    )
    activity_prompt = onboarding_service.get_prompt(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )

    assert activity_prompt is not None
    assert "Google also found likely activity signals:" in activity_prompt.text
    assert "Soccer" in activity_prompt.text
    assert "North Field" in activity_prompt.text
    store.close()


def test_onboarding_service_renders_transition_and_repeat_messages(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    onboarding_service = FlorenceOnboardingSessionService(store)

    transition = onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        display_name="Maya",
    )

    transition_messages = onboarding_service.get_transition_messages(
        transition,
        previous_stage=OnboardingStage.COLLECT_PARENT_NAME,
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        link_url="https://example.com/google/connect",
    )
    repeat_messages = onboarding_service.get_prompt_messages(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        link_url="https://example.com/google/connect",
    )

    assert transition_messages == (
        "Hi, I'm Florence.",
        "I help run the household with you by keeping logistics organized, surfacing reminders, and staying on top of school and calendar noise.",
        "Connect your Google account so I can pull up to the last year of family email and calendar in the background while we keep going here.",
        "https://example.com/google/connect",
        "Once Google says you're connected, come right back here. You can also keep answering my questions while it runs.",
        "What are your kids' names? Send all of them in one message, one per line or comma-separated.",
    )
    assert repeat_messages == (
        "What are your kids' names? Send all of them in one message, one per line or comma-separated.",
    )
    store.close()


def test_onboarding_service_renders_google_connect_retry_messages(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    onboarding_service = FlorenceOnboardingSessionService(store)
    onboarding_service.set_link_url_builder(
        lambda household_id, member_id, thread_id: "https://example.com/google/connect"
    )

    messages = onboarding_service.get_google_connect_retry_messages(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )

    assert messages == (
        "I still don’t see your Google account connected yet.",
        "https://example.com/google/connect",
        "Once Google says you're connected, come back here and text done.",
    )
    store.close()


def test_onboarding_service_handles_google_done_followup_with_chat_continuation(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    onboarding_service = FlorenceOnboardingSessionService(store)
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="maya@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="access-token",
        )
    )
    calls: list[str] = []

    result = onboarding_service.handle_google_done_followup(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        continue_with_household_chat=lambda message_text: (
            calls.append(message_text) or ("I found the email and pulled the dates.", ("I found the email and pulled the dates.",))
        ),
    )

    assert calls == ["My Google account is connected now. Continue with the inbox or calendar lookup you just offered."]
    assert result.reply_text == "I found the email and pulled the dates."
    assert result.reply_messages == ("I found the email and pulled the dates.",)
    store.close()


def test_onboarding_sync_merges_grounding_hints_into_profile_metadata(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya's household",
            timezone="America/Los_Angeles",
            settings={
                "grounding_hints": {
                    "schools": [
                        {
                            "label": "Roosevelt Elementary",
                            "domains": ["roosevelt.k12.ca.us"],
                            "platforms": ["ParentSquare"],
                            "contacts": ["Ms. Kim"],
                        }
                    ],
                    "activities": [
                        {
                            "label": "Soccer",
                            "locations": ["North Field"],
                            "contacts": ["Coach Ben"],
                        }
                    ],
                }
            },
        )
    )
    onboarding_service = FlorenceOnboardingSessionService(store)

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
        child_names=["Ava Johnson"],
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        text="7",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        text="Roosevelt Elementary",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        text="Soccer",
    )

    child = store.list_child_profiles(household_id="hh_123")[0]
    school = store.list_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.SCHOOL,
    )[0]
    activity = store.list_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.ACTIVITY,
    )[0]

    assert child.metadata["aliases"] == ["Ava"]
    assert school.metadata["domains"] == ["roosevelt.k12.ca.us"]
    assert school.metadata["platforms"] == ["ParentSquare"]
    assert school.metadata["contacts"] == ["Ms. Kim"]
    assert activity.metadata["locations"] == ["North Field"]
    assert activity.metadata["contacts"] == ["Coach Ben"]
    store.close()


def test_onboarding_sync_persists_structured_household_profiles_without_manager_sidecar(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    onboarding_service = FlorenceOnboardingSessionService(store)

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        display_name="Maya",
    )
    onboarding_service.record_child_names(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        child_names=["Ava"],
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        text="7",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        text="Roosevelt Elementary",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        text="Soccer",
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )

    household = store.get_household("hh_123")
    assert household is not None
    assert "manager_profile" not in household.settings
    session = store.get_onboarding_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )
    assert session is not None
    assert session.parent_display_name == "Maya"
    child = store.list_child_profiles(household_id="hh_123")[0]
    assert child.full_name == "Ava"
    school = store.list_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.SCHOOL,
    )[0]
    assert school.label == "Roosevelt Elementary"
    activity = store.list_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.ACTIVITY,
    )[0]
    assert activity.label == "Soccer"
    store.close()


def test_household_manager_service_schedules_due_nudge_with_default_dm_context(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(
        Member(
            id="mem_123",
            household_id="hh_123",
            display_name="Maya",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    onboarding_service = FlorenceOnboardingSessionService(store)
    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        display_name="Maya",
    )

    manager = FlorenceHouseholdManagerService(store)
    nudge = manager.schedule_nudge(
        household_id="hh_123",
        message="Remember to order groceries for taco night.",
        scheduled_for="2026-03-24T12:00:00+00:00",
        target_kind=HouseholdNudgeTargetKind.GENERAL,
    )

    due = manager.list_due_nudges(
        household_id="hh_123",
        now=datetime(2026, 3, 24, 12, 30, tzinfo=timezone.utc),
    )
    assert due == [nudge]
    assert nudge.recipient_member_id == "mem_123"
    assert nudge.channel_id == "chan_dm_123"

    sent = manager.mark_nudge_sent(
        nudge_id=nudge.id,
        sent_at=datetime(2026, 3, 24, 12, 31, tzinfo=timezone.utc),
    )
    assert sent is not None
    assert sent.sent_at == "2026-03-24T12:31:00+00:00"
    store.close()


def test_household_manager_service_completes_actionable_nudge_and_work_item(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    manager = FlorenceHouseholdManagerService(store)
    store.upsert_household_work_item(
        HouseholdWorkItem(
            id="work_123",
            household_id="hh_123",
            title="Book pediatrician visit",
            status=HouseholdWorkItemStatus.OPEN,
        )
    )
    store.upsert_household_nudge(
        HouseholdNudge(
            id="nudge_123",
            household_id="hh_123",
            target_kind=HouseholdNudgeTargetKind.WORK_ITEM,
            target_id="work_123",
            message="Reminder: book the pediatrician visit.",
            status=HouseholdNudgeStatus.SENT,
            recipient_member_id="mem_123",
            channel_id="chan_dm_123",
            scheduled_for="2026-03-24T12:00:00+00:00",
            sent_at="2026-03-24T12:05:00+00:00",
        )
    )

    result = manager.complete_actionable_nudge(
        household_id="hh_123",
        member_id="mem_123",
        channel_id="chan_dm_123",
        now=datetime(2026, 3, 24, 12, 30, tzinfo=timezone.utc),
    )

    updated_nudge = store.get_household_nudge("nudge_123")
    updated_work_item = store.get_household_work_item("work_123")
    events = store.list_pilot_events(household_id="hh_123", event_type="reminder_done")
    assert result is not None
    assert "marked" in result.reply_text.lower()
    assert updated_nudge is not None
    assert updated_nudge.status == HouseholdNudgeStatus.ACKNOWLEDGED
    assert updated_work_item is not None
    assert updated_work_item.status == HouseholdWorkItemStatus.DONE
    assert len(events) == 1
    assert events[0].metadata["nudge_id"] == "nudge_123"
    assert events[0].metadata["marked_work_item_done"] is True
    store.close()


def test_household_manager_service_snoozes_actionable_nudge(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    manager = FlorenceHouseholdManagerService(store)
    now = datetime(2026, 3, 24, 12, 30, tzinfo=timezone.utc)
    store.upsert_household_nudge(
        HouseholdNudge(
            id="nudge_124",
            household_id="hh_123",
            target_kind=HouseholdNudgeTargetKind.GENERAL,
            message="Reminder: pack baseball gear.",
            status=HouseholdNudgeStatus.SENT,
            recipient_member_id="mem_123",
            channel_id="chan_dm_123",
            scheduled_for=(now - timedelta(minutes=10)).isoformat(),
            sent_at=(now - timedelta(minutes=8)).isoformat(),
        )
    )

    result = manager.snooze_actionable_nudge(
        household_id="hh_123",
        member_id="mem_123",
        channel_id="chan_dm_123",
        scheduled_for=now + timedelta(hours=3),
        now=now,
    )

    updated_nudge = store.get_household_nudge("nudge_124")
    events = store.list_pilot_events(household_id="hh_123", event_type="reminder_snoozed")
    assert result is not None
    assert "snoozed" in result.reply_text.lower()
    assert updated_nudge is not None
    assert updated_nudge.status == HouseholdNudgeStatus.SCHEDULED
    assert updated_nudge.sent_at is None
    assert updated_nudge.acknowledged_at is None
    assert updated_nudge.scheduled_for is not None
    assert len(events) == 1
    assert events[0].metadata["nudge_id"] == "nudge_124"
    store.close()


def test_household_manager_service_briefing_routines_due_and_advance(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(
        Member(
            id="mem_123",
            household_id="hh_123",
            display_name="Maya",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    onboarding_service = FlorenceOnboardingSessionService(store)
    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        display_name="Maya",
    )
    store.replace_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.PREFERENCE,
        items=[
            HouseholdProfileItem(
                id="pref_briefing_123",
                household_id="hh_123",
                kind=HouseholdProfileKind.PREFERENCE,
                label="Briefing cadence",
                metadata={
                    "category": "operating_rule",
                    "value": "Weekday morning brief at 6:45 and evening check-in on school nights at 8:30pm.",
                },
            )
        ],
    )

    manager = FlorenceHouseholdManagerService(store)
    routines = manager.ensure_briefing_routines(
        household_id="hh_123",
        now=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
    )
    assert len(routines) == 3
    assert all(routine.status == HouseholdRoutineStatus.ACTIVE for routine in routines)

    morning = next(routine for routine in routines if routine.metadata.get("brief_kind") == "morning")
    weekly = next(routine for routine in routines if routine.metadata.get("brief_kind") == "weekly")
    assert weekly.title == "Weekly preview"
    assert weekly.metadata["days"] == [6]
    assert weekly.metadata["local_time"] == "17:30"
    due = manager.list_due_briefing_routines(
        household_id="hh_123",
        now=datetime(2026, 3, 24, 14, 0, tzinfo=timezone.utc),
    )
    assert morning in due

    updated = manager.mark_briefing_routine_sent(
        routine_id=morning.id,
        sent_at=datetime(2026, 3, 24, 14, 1, tzinfo=timezone.utc),
    )
    assert updated is not None
    assert updated.last_completed_at == "2026-03-24T14:01:00+00:00"
    assert updated.next_due_at is not None
    assert updated.next_due_at > "2026-03-24T14:01:00+00:00"
    store.close()


def test_household_manager_service_records_reminder_feedback_and_event(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    manager = FlorenceHouseholdManagerService(store)
    profile = manager.record_reminder_feedback(
        household_id="hh_123",
        feedback_text="Too many reminders too early. Morning-of is enough for practice.",
        member_id="mem_123",
        channel_id="chan_dm_123",
        now=datetime(2026, 3, 24, 18, 30, tzinfo=timezone.utc),
    )

    assert profile is not None
    assert profile.label == "Reminder style"
    assert profile.metadata["category"] == "reminder_style"
    assert profile.metadata["value"] == "Too many reminders too early. Morning-of is enough for practice."
    assert profile.metadata["recorded_by_member_id"] == "mem_123"
    events = store.list_pilot_events(household_id="hh_123", event_type="reminder_feedback_received")
    assert len(events) == 1
    assert events[0].metadata["text"] == "Too many reminders too early. Morning-of is enough for practice."
    store.close()


def test_group_share_service_promotes_existing_shareable_message(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(
        Member(
            id="mem_123",
            household_id="hh_123",
            display_name="Maya",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_group_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="group-thread-123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Parent group",
        )
    )
    store.append_channel_message(
        ChannelMessage(
            id="msg_shareable",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.ASSISTANT,
            body="I pulled together the key household items.",
            metadata={
                "promotable_group_message": "Florence pulled together a quick household update:\n- Science fair Friday",
            },
            created_at=datetime.now(tz=timezone.utc).timestamp(),
        )
    )
    service = FlorenceGroupShareService(
        store,
        channel_log=FlorenceChannelLog(store),
        household_chat_service=_StubGroupShareChatService(),
        review_confirmation_suffix="Reply yes if I should add it, no if it's wrong, or skip for later.",
    )

    result = service.handle_explicit_share_request(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        current_provider="linq",
        current_message_id="msg_share_now",
    )

    updated = store.get_channel_message("msg_shareable")
    assert result is not None
    assert result.reply_text == "Shared a short version with the parent group."
    assert result.group_announcement == "Florence pulled together a quick household update:\n- Science fair Friday"
    assert updated is not None
    assert updated.metadata["promoted_group_channel_id"] == "chan_group_123"
    assert updated.metadata["promoted_to_group_at"]
    store.close()


def test_group_share_service_ignores_share_request_when_latest_message_is_review_prompt(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(
        Member(
            id="mem_123",
            household_id="hh_123",
            display_name="Maya",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    chat_service = _StubGroupShareChatService(promotion_text="Should not be called.")
    service = FlorenceGroupShareService(
        store,
        channel_log=FlorenceChannelLog(store),
        household_chat_service=chat_service,
        review_confirmation_suffix="Reply yes if I should add it, no if it's wrong, or skip for later.",
    )
    store.append_channel_message(
        ChannelMessage(
            id="msg_review_prompt",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.ASSISTANT,
            body="Summer camp invoice. Should I add it?",
            metadata={"protocol_kind": CANDIDATE_REVIEW_PROMPT_KIND},
            created_at=datetime.now(tz=timezone.utc).timestamp(),
        )
    )

    result = service.handle_explicit_share_request(
        household_id="hh_123",
        channel_id="chan_dm_123",
        actor_member_id="mem_123",
        current_provider="linq",
        current_message_id="msg_share_now",
    )

    assert result is None
    assert chat_service.promotion_calls == []
    store.close()


def test_operations_review_nudge_records_candidate_review_prompt_metadata(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    store.upsert_member(
        Member(
            id="mem_123",
            household_id="hh_123",
            display_name="Maya",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    onboarding_service = FlorenceOnboardingSessionService(store)
    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm-thread-123",
        display_name="Maya",
    )
    candidate = store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_999",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:gmail_999",
            title="Young Minds invoice",
            summary="Needs confirmation.",
            state=CandidateState.PENDING_REVIEW,
            requires_confirmation=True,
            metadata={"confirmation_question": "Should I add this?"},
        )
    )
    linq = _FakeLinqClient()
    delivery = FlorenceChannelDeliveryService(
        store,
        linq_client_getter=lambda: linq,
        sendblue_client_getter=lambda: None,
    )
    operations = FlorenceHouseholdOperationsService(
        store,
        delivery_service=delivery,
        household_chat_service_getter=lambda: _StubReviewPromptChatService(),
    )

    nudged = operations.nudge_for_new_pending_candidates(
        household_id="hh_123",
        member_id="mem_123",
        candidates=[candidate],
    )

    latest = FlorenceChannelLog(store).latest_assistant_message(channel_id="chan_dm_123")
    persisted = store.get_imported_candidate("cand_999")
    assert nudged is True
    assert linq.sent
    assert latest is not None
    assert latest.metadata["protocol_kind"] == CANDIDATE_REVIEW_PROMPT_KIND
    assert persisted is not None
    assert persisted.metadata["review_nudged_at"]
    store.close()
