from florence.messaging import (
    FlorenceInboundMessage,
    FlorenceMessagingIngressService,
    FlorenceResolvedInboundMessage,
)
from florence.runtime import (
    FlorenceCandidateReviewService,
    FlorenceHouseholdQueryService,
    FlorenceOnboardingSessionService,
)
from florence.state import FlorenceStateDB


class _StubGoogleAccountLinkService:
    def build_connect_link(self, *, household_id: str, member_id: str, thread_id: str):
        class _Link:
            url = "https://example.com/google/connect"

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


def test_dm_parent_name_reply_includes_friendly_google_link(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )
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
        "I help keep your household organized by keeping up with school emails, calendar invites, and schedule changes.",
        "First step: connect your Google account so I can start syncing Gmail and Calendar.",
        "https://example.com/google/connect",
        "When you're done, reply done here and I'll keep going.",
    )
    store.close()


def test_complete_dm_routes_freeform_chat_through_household_chat_service(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )
    chat_service = _StubHouseholdChatService("I can keep planning with you here.")
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
        household_chat_service=chat_service,
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


def test_activity_basics_completion_unlocks_agent_without_requiring_group(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )
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
        "Perfect. I have enough household context to help now.",
        "You can keep asking me here, and you can add me to the family group later if you want shared help there too.",
    )
    assert session.is_complete is True
    assert session.group_channel_id is None
    store.close()


def test_first_group_message_after_context_collection_records_group_channel(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )
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
