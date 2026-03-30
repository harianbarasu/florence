from florence.messaging import (
    FlorenceInboundMessage,
    FlorenceMessagingIngressService,
    FlorenceResolvedInboundMessage,
)
from datetime import datetime, timedelta, timezone

from florence.contracts import (
    CandidateState,
    GoogleConnection,
    GoogleSourceKind,
    Household,
    HouseholdSourceVisibility,
    HouseholdNudge,
    HouseholdNudgeStatus,
    HouseholdNudgeTargetKind,
    ImportedCandidate,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
)
from florence.onboarding import OnboardingVariant
from florence.runtime import (
    FlorenceCandidateReviewService,
    FlorenceHouseholdQueryService,
    FlorenceIdentityResolver,
    FlorenceOnboardingSessionService,
)
from florence.state import FlorenceStateDB


class _StubGoogleAccountLinkService:
    def build_connect_link(self, *, household_id: str, member_id: str, thread_id: str):
        class _Link:
            url = "https://example.com/google/connect"

        return _Link()


class _StubOnboardingLinkService:
    def build_link(self, *, household_id: str, member_id: str, thread_id: str):
        class _Link:
            url = "https://florence.example.com/v1/florence/onboarding?token=test-token"

        return _Link()


class _StubHouseholdChatService:
    def __init__(self, reply_text: str):
        self.reply_text = reply_text
        self.calls = []

    def respond(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        message_text: str,
        conversation_history=None,
    ):
        self.calls.append(
            {
                "household_id": household_id,
                "channel_id": channel_id,
                "actor_member_id": actor_member_id,
                "message_text": message_text,
                "conversation_history": conversation_history or [],
            }
        )

        class _Reply:
            text = self.reply_text

        return _Reply()


def _build_hybrid_onboarding_service(store, review_service):
    return FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
        variant_selector=lambda _household_id, _member_id: OnboardingVariant.HYBRID,
    )


def _complete_hybrid_onboarding(onboarding_service):
    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        display_name="Maya",
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
    onboarding_service.record_household_operations(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        household_operations=["school forms", "pickup planning"],
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    onboarding_service.record_nudge_preferences(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        nudge_preferences="Day before and morning of, and follow up until I reply for anything school-related.",
    )
    onboarding_service.record_operating_preferences(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        operating_preferences="Weekday morning brief at 6:45, no texts after 9pm, ask before spending money.",
    )


def test_dm_parent_name_reply_includes_friendly_google_link(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
        google_account_link_service=_StubGoogleAccountLinkService(),
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_123",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Maya",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text is not None
    assert result.reply_text == "Hi, I'm Florence."
    assert result.reply_messages == (
        "Hi, I'm Florence.",
        "I help run the household with you by learning the family map first, then keeping up with reminders, logistics, school noise, and schedule changes.",
        "Start with the kids I should know about: first name plus grade or age if helpful. One per line or comma-separated is fine.",
    )
    store.close()


def test_dm_onboarding_prefers_web_handoff_when_link_service_is_available(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
        google_account_link_service=_StubGoogleAccountLinkService(),
        onboarding_link_service=_StubOnboardingLinkService(),
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_web_123",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Maya",
                is_group_chat=False,
            ),
        )
    )

    session = onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    assert result.reply_messages == (
        "Hi, I'm Florence.",
        "I’m easiest to set up on a computer. Finish setup there so I can learn your household, connect Google, and start acting like your house manager.",
        "https://florence.example.com/v1/florence/onboarding?token=test-token",
        "Once setup is done, I’ll text you here when I’m ready and when the first Gmail and Calendar pass finishes.",
    )
    assert session.parent_display_name is None
    assert session.stage == "collect_parent_name"
    store.close()


def test_dm_status_question_after_google_connect_returns_sync_progress_sequence(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
        google_account_link_service=_StubGoogleAccountLinkService(),
        onboarding_link_service=_StubOnboardingLinkService(),
    )

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        display_name="Maya",
    )
    onboarding_service.record_household_operations(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        household_operations=["school forms", "pickup planning"],
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_sync_progress",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="What's the sync status?",
                is_group_chat=False,
            ),
        )
    )

    assert result.reply_messages == (
        "Google connected.",
        "I’m syncing your recent email and calendar in the background now.",
        "If you want to track setup progress on your computer, use this link:",
        "https://florence.example.com/v1/florence/onboarding?token=test-token",
        "I’ll text you here when the first pass is ready.",
    )
    store.close()


def test_dm_acknowledgement_during_sync_does_not_loop_setup_messages(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
        google_account_link_service=_StubGoogleAccountLinkService(),
        onboarding_link_service=_StubOnboardingLinkService(),
    )

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        display_name="Maya",
    )
    onboarding_service.record_household_operations(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        household_operations=["school forms", "pickup planning"],
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_sync_ack",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Sounds good",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text is None
    assert result.reply_messages == ()
    store.close()


def test_dm_substantive_message_during_sync_uses_household_chat_service(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I can help you think through Friday pickup while the sync finishes.")
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
        google_account_link_service=_StubGoogleAccountLinkService(),
        onboarding_link_service=_StubOnboardingLinkService(),
        household_chat_service=chat_service,
    )

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        display_name="Maya",
    )
    onboarding_service.record_household_operations(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        household_operations=["school forms", "pickup planning"],
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_sync_substantive",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Can you help me think through Friday pickup while this is syncing?",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "I can help you think through Friday pickup while the sync finishes."
    assert "first Gmail and Calendar sync is still running" in chat_service.calls[0]["message_text"]
    store.close()


def test_complete_dm_routes_freeform_chat_through_household_chat_service(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I can keep planning with you here.")
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
        household_chat_service=chat_service,
    )

    _complete_hybrid_onboarding(onboarding_service)

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_201",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Can you help me plan pickup for Friday?",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "I can keep planning with you here."
    assert chat_service.calls[0]["channel_id"] == "chan_dm_123"
    assert chat_service.calls[0]["actor_member_id"] == "mem_123"
    store.close()


def test_pending_candidate_does_not_hijack_generic_yes_without_review_prompt_context(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I can pull exact Giants and A's dates now.")
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
        household_chat_service=chat_service,
    )
    _complete_hybrid_onboarding(onboarding_service)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_123",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:haircuts",
            title="Fireflies Haircuts for Kids accepted your appointment",
            summary="Haircut appointment for Friday at 3:30 PM.",
            state=CandidateState.PENDING_REVIEW,
        )
    )

    first = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_301",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Can you pull the baseball dates for next week?",
                is_group_chat=False,
            ),
        )
    )
    assert first.reply_text == "I can pull exact Giants and A's dates now."

    second = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_302",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="yes please",
                is_group_chat=False,
            ),
        )
    )
    assert second.reply_text == "I can pull exact Giants and A's dates now."
    candidate = store.get_imported_candidate("cand_123")
    assert candidate is not None
    assert candidate.state == CandidateState.PENDING_REVIEW
    assert store.list_household_events(household_id="hh_123") == []
    store.close()


def test_review_prompt_then_yes_confirms_pending_candidate(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )
    _complete_hybrid_onboarding(onboarding_service)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_124",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:fireflies-2",
            title="Fireflies Haircuts for Kids accepted your appointment",
            summary="Haircut appointment for Friday at 3:30 PM.",
            state=CandidateState.PENDING_REVIEW,
        )
    )

    review = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_303",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="review imports",
                is_group_chat=False,
            ),
        )
    )
    assert review.reply_text is not None
    assert "Imported item:" in review.reply_text

    confirmation = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_304",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="yes",
                is_group_chat=False,
            ),
        )
    )
    assert confirmation.reply_text is not None
    assert confirmation.reply_text.startswith("Confirmed.")
    candidate = store.get_imported_candidate("cand_124")
    assert candidate is not None
    assert candidate.state == CandidateState.CONFIRMED
    events = store.list_household_events(household_id="hh_123")
    assert len(events) == 1
    assert "Fireflies Haircuts for Kids" in events[0].title
    store.close()


def test_review_prompt_then_share_persists_source_rule_for_future_items(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )
    _complete_hybrid_onboarding(onboarding_service)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_125",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:linda-1",
            title="Violet music class update",
            summary="Linda <linda@musicalbeginnings.com> - no class April 8.",
            state=CandidateState.PENDING_REVIEW,
            metadata={
                "from_address": "Linda <linda@musicalbeginnings.com>",
                "confirmation_question": "Should I add Violet music class update to your household plan?",
            },
        )
    )

    review = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_305",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="review imports",
                is_group_chat=False,
            ),
        )
    )
    assert review.reply_text is not None
    assert "Reply share to treat future items from this source as household-shared" in review.reply_text

    classification = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_306",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="share",
                is_group_chat=False,
            ),
        )
    )

    assert classification.reply_text is not None
    assert "shared household context" in classification.reply_text
    rules = store.list_household_source_rules(
        household_id="hh_123",
        source_kind=GoogleSourceKind.GMAIL,
        visibility=HouseholdSourceVisibility.SHARED,
    )
    assert any(rule.matcher_value == "musicalbeginnings.com" for rule in rules)
    store.close()


def test_activity_basics_advances_to_household_operations_before_unlocking_agent(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        display_name="Maya",
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

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_202",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Soccer",
                is_group_chat=False,
            ),
        )
    )

    session = onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    assert result.consumed is True
    assert result.reply_messages == (
        "What recurring logistics or reminders should I help manage first? A short list is fine: lunches, forms, returns, bills, sports, appointments, birthdays, and so on.",
    )
    assert session.is_complete is False
    assert session.stage == "collect_household_operations"
    assert session.group_channel_id is None
    store.close()


def test_child_name_parsing_from_freeform_sentence_keeps_only_names(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        display_name="Maya",
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_child_parse_1",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body=(
                    "Theo is 7 he's in first grade, Violet is about to turn 4 in May, "
                    "she's in her last year of pre school before starting TK in the fall"
                ),
                is_group_chat=False,
            ),
        )
    )

    session = onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    assert session.child_names == ["Theo", "Violet"]
    assert result.reply_text is not None
    assert "Theo, Violet" in result.reply_text
    store.close()


def test_nudge_preferences_advance_to_operating_policy_step(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        display_name="Maya",
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
    onboarding_service.record_household_operations(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        household_operations=["school forms", "returns"],
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_203",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Day before and morning of, and keep nudging if it is school-related.",
                is_group_chat=False,
            ),
        )
    )

    session = onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    assert result.consumed is True
    assert result.reply_text is not None
    assert "you're ready" in result.reply_text.lower()
    assert session.is_complete is True
    assert session.stage == "complete"
    assert session.group_channel_id is None
    store.close()


def test_operating_preferences_completion_unlocks_agent_without_requiring_group(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )

    onboarding_service.record_parent_name(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        display_name="Maya",
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
    onboarding_service.record_household_operations(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        household_operations=["school forms", "returns"],
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    onboarding_service.record_nudge_preferences(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        nudge_preferences="Day before and morning of, and keep nudging if it is school-related.",
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_204",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Weekday morning brief at 6:45, no texts after 9pm, ask before spending money.",
                is_group_chat=False,
            ),
        )
    )

    session = onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    assert result.consumed is True
    assert result.reply_messages == (
        "You're ready. Florence is set up as your house manager now.",
        "Start with a real task like: what's on the kids' schedule next week, check my email for a school or camp update, remind me about picture day, or plan dinners and groceries for next week.",
    )
    assert session.is_complete is True
    assert session.group_channel_id is None
    store.close()


def test_first_group_message_after_context_collection_records_group_channel(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )

    _complete_hybrid_onboarding(onboarding_service)

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_group_123",
            thread_id="group_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_group_1",
                thread_id="group_thread_123",
                sender_handle="+15555550123",
                body="Hey Florence",
                is_group_chat=True,
            ),
        )
    )

    session = onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    assert result.consumed is True
    assert result.reply_text is not None
    assert "I’m in." in result.reply_text
    assert session.group_channel_id == "group_thread_123"
    store.close()


def test_complete_dm_schedule_question_routes_through_household_chat_service_before_state_shortcuts(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I can check Musical Beginnings and pull the spring break dates.")
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
        household_chat_service=chat_service,
    )
    _complete_hybrid_onboarding(onboarding_service)

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_schedule_1",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Do you know the spring break schedule for the kids music class?",
                is_group_chat=False,
            ),
        )
    )

    assert result.reply_text == "I can check Musical Beginnings and pull the spring break dates."
    assert chat_service.calls[0]["message_text"] == "Do you know the spring break schedule for the kids music class?"
    store.close()


def test_done_after_google_connect_prompt_routes_back_to_agent_not_reminder_ack(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I found the Musical Beginnings spring break email and pulled the dates.")
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
        household_chat_service=chat_service,
    )
    _complete_hybrid_onboarding(onboarding_service)
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
    ingress.append_assistant_message(
        household_id="hh_123",
        channel_id="chan_dm_123",
        body="If you already finished the link I sent you earlier, reply done and I'll look for emails from Linda at Musical Beginnings.",
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_done_google_1",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="done",
                is_group_chat=False,
            ),
        )
    )

    assert result.reply_text == "I found the Musical Beginnings spring break email and pulled the dates."
    assert chat_service.calls[0]["message_text"] == "My Google account is connected now. Continue with the inbox or calendar lookup you just offered."
    store.close()


def test_complete_dm_can_answer_tracking_visibility_request(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )
    _complete_hybrid_onboarding(onboarding_service)

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_205",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="What are you tracking for us right now?",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text is not None
    assert "actively tracking" in result.reply_text.lower()
    store.close()


def test_complete_dm_reminder_feedback_updates_manager_profile_and_logs_event(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )
    _complete_hybrid_onboarding(onboarding_service)

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_206",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Too many reminders too early. Morning-of is better for practices.",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text is not None
    assert "updated your reminder style" in result.reply_text.lower()

    household = store.get_household("hh_123")
    assert household is not None
    manager_profile = household.settings["manager_profile"]
    assert manager_profile["nudge_preferences_override"] == "Too many reminders too early. Morning-of is better for practices."
    events = store.list_pilot_events(household_id="hh_123", event_type="reminder_feedback_received")
    assert len(events) == 1
    store.close()


def test_complete_dm_done_acknowledges_sent_nudge_and_marks_work_item_done(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )
    _complete_hybrid_onboarding(onboarding_service)

    now = datetime.now(timezone.utc)
    work_item = HouseholdWorkItem(
        id="work_123",
        household_id="hh_123",
        title="Upload field trip form",
        status=HouseholdWorkItemStatus.OPEN,
    )
    store.upsert_household_work_item(work_item)
    nudge = HouseholdNudge(
        id="nudge_123",
        household_id="hh_123",
        target_kind=HouseholdNudgeTargetKind.WORK_ITEM,
        target_id=work_item.id,
        message="Reminder: upload the field trip form tonight.",
        status=HouseholdNudgeStatus.SENT,
        recipient_member_id="mem_123",
        channel_id="chan_dm_123",
        scheduled_for=(now - timedelta(minutes=20)).isoformat(),
        sent_at=(now - timedelta(minutes=15)).isoformat(),
    )
    store.upsert_household_nudge(nudge)

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_207",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="done",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text is not None
    assert "marked" in result.reply_text.lower()

    updated_nudge = store.get_household_nudge("nudge_123")
    assert updated_nudge is not None
    assert updated_nudge.status == HouseholdNudgeStatus.ACKNOWLEDGED
    assert updated_nudge.acknowledged_at is not None

    updated_work_item = store.get_household_work_item("work_123")
    assert updated_work_item is not None
    assert updated_work_item.status == HouseholdWorkItemStatus.DONE
    assert updated_work_item.completed_at is not None

    events = store.list_pilot_events(household_id="hh_123", event_type="reminder_done")
    assert len(events) == 1
    assert events[0].metadata["nudge_id"] == "nudge_123"
    assert events[0].metadata["marked_work_item_done"] is True
    store.close()


def test_complete_dm_snooze_reschedules_sent_nudge_and_logs_event(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )
    _complete_hybrid_onboarding(onboarding_service)

    now = datetime.now(timezone.utc)
    nudge = HouseholdNudge(
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
    store.upsert_household_nudge(nudge)

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_208",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="snooze 3h",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text is not None
    assert "snoozed" in result.reply_text.lower()

    updated_nudge = store.get_household_nudge("nudge_124")
    assert updated_nudge is not None
    assert updated_nudge.status == HouseholdNudgeStatus.SCHEDULED
    assert updated_nudge.sent_at is None
    assert updated_nudge.acknowledged_at is None
    assert updated_nudge.scheduled_for is not None
    scheduled_for = datetime.fromisoformat(updated_nudge.scheduled_for.replace("Z", "+00:00"))
    assert scheduled_for > now + timedelta(hours=2)

    events = store.list_pilot_events(household_id="hh_123", event_type="reminder_snoozed")
    assert len(events) == 1
    assert events[0].metadata["nudge_id"] == "nudge_124"
    store.close()


def test_group_non_household_question_does_not_fall_back_to_schedule_summary(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_hybrid_onboarding_service(store, review_service)
    resolver = FlorenceIdentityResolver(store, provider="linq")
    direct = resolver.resolve_direct_message(
        sender_handle="+15555550123",
        thread_external_id="dm_thread_123",
    )
    group = resolver.resolve_group_message(
        sender_handle="+15555550123",
        participant_handles=["+15555550123", "+15555550124"],
        thread_external_id="group_thread_123",
    )
    assert group is not None

    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id=direct.household.id,
            member_id=direct.member.id,
            channel_id=group.channel.id,
            thread_id="group_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_aquarium_123",
                thread_id="group_thread_123",
                sender_handle="+15555550123",
                body="What are the Monterey Bay Aquarium hours today and when is the best time to go?",
                is_group_chat=True,
            ),
        )
    )

    assert result.consumed is False
    assert result.reply_text is None
    store.close()
