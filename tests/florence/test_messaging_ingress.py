from florence.messaging import (
    FlorenceInboundMessage,
    FlorenceMessagingIngressService,
    FlorenceResolvedInboundMessage,
)
from florence.messaging.protocol_types import CANDIDATE_REVIEW_PROMPT_KIND
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
    HouseholdSourceVisibility,
    HouseholdNudge,
    HouseholdNudgeStatus,
    HouseholdNudgeTargetKind,
    HouseholdProfileKind,
    ImportedCandidate,
    Member,
    MemberRole,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
)
from florence.runtime import (
    FlorenceCandidateReviewService,
    FlorenceIdentityResolver,
    FlorenceOnboardingSessionService,
)
from florence.state import FlorenceStateDB


class _StubGoogleAccountLinkService:
    def build_connect_link(self, *, household_id: str, member_id: str, thread_id: str):
        class _Link:
            url = "https://example.com/google/connect"

        return _Link()


class _StubHouseholdChatService:
    def __init__(
        self,
        reply_text: str,
        *,
        promotion_text: str | None = None,
        review_prompt_text: str | None = None,
        sync_waiting_text: str | None = None,
    ):
        self.reply_text = reply_text
        self.promotion_text = promotion_text
        self.review_prompt_text = review_prompt_text
        self.sync_waiting_text = sync_waiting_text or (
            "Google is connected. I’m syncing up to the last year of your email and calendar in the background now, and I’ll text you here when the first pass is ready."
        )
        self.calls = []
        self.promotion_calls = []
        self.review_prompt_calls = []
        self.sync_waiting_calls = []

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

    def compose_operator_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        kind: str,
        payload=None,
        conversation_history=None,
    ) -> str | None:
        payload = dict(payload or {})
        if kind == "group_promotion":
            self.promotion_calls.append(
                {
                    "household_id": household_id,
                    "channel_id": channel_id,
                    "actor_member_id": actor_member_id,
                    "source_text": payload.get("source_text"),
                }
            )
            return self.promotion_text
        if kind == "review_prompt":
            candidate = payload.get("candidate") or {}
            source_prompt = payload.get("source_prompt")
            self.review_prompt_calls.append(
                {
                    "household_id": household_id,
                    "channel_id": channel_id,
                    "actor_member_id": actor_member_id,
                    "candidate_id": candidate.get("id"),
                    "candidate_title": candidate.get("title"),
                    "source_prompt": source_prompt,
                }
            )
            if self.review_prompt_text is not None:
                return self.review_prompt_text
            title = " ".join(str(candidate.get("title") or "").split()).strip()
            lines = [title or "This looks worth double-checking."]
            if source_prompt:
                lines.append(str(source_prompt).strip())
            lines.append("Reply yes if I should add it, no if it's wrong, or skip for later.")
            return " ".join(line for line in lines if line)
        if kind in {"sync_waiting", "sync_started"}:
            self.sync_waiting_calls.append(
                {
                    "household_id": household_id,
                    "channel_id": channel_id,
                    "actor_member_id": actor_member_id,
                    "kind": kind,
                    "user_message": payload.get("user_message"),
                    "conversation_history": conversation_history or [],
                    "data_dependent": bool(payload.get("data_dependent")),
                }
            )
            return self.sync_waiting_text
        raise AssertionError(f"Unexpected compose_operator_message kind: {kind}")


def _build_onboarding_service(store, review_service):
    return FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )


def _build_ingress(
    store,
    onboarding_service,
    review_service,
    **kwargs,
):
    google_account_link_service = kwargs.pop("google_account_link_service", None)
    onboarding_service.set_link_url_builder(
        None
        if google_account_link_service is None
        else lambda household_id, member_id, thread_id: google_account_link_service.build_connect_link(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        ).url
    )
    kwargs.setdefault(
        "household_chat_service",
        _StubHouseholdChatService("I can keep planning with you here."),
    )
    return FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        **kwargs,
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
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="7",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="Roosevelt Elementary",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="Soccer",
    )
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )


def test_dm_parent_name_reply_includes_friendly_google_link(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
        "I help run the household with you by keeping logistics organized, surfacing reminders, and staying on top of school and calendar noise.",
        "Connect your Google account so I can pull up to the last year of family email and calendar in the background while we keep going here.",
        "https://example.com/google/connect",
        "Once Google says you're connected, come right back here. You can also keep answering my questions while it runs.",
        "What are your kids' names? Send all of them in one message, one per line or comma-separated.",
    )
    store.close()


def test_dm_onboarding_replies_immediately_to_child_name_message(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        google_account_link_service=_StubGoogleAccountLinkService(),
    )

    ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_name",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Maya",
                is_group_chat=False,
            ),
        )
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_child_1",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Ava",
                is_group_chat=False,
            ),
        )
    )

    session = onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    assert result.reply_messages == ("Great, let's learn more about each kid one at a time. How old is Ava?",)
    assert session.child_names == ["Ava"]
    store.close()


def test_dm_onboarding_absorbs_fragmented_second_child_name_during_child_detail_collection(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        google_account_link_service=_StubGoogleAccountLinkService(),
    )

    ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_name_single",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Maya",
                is_group_chat=False,
            ),
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
                message_id="msg_child_single",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Ava",
                is_group_chat=False,
            ),
        )
    )
    second = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_child_fragmented",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Ben",
                is_group_chat=False,
            ),
        )
    )

    session = onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    assert first.reply_messages == ("Great, let's learn more about each kid one at a time. How old is Ava?",)
    assert second.reply_messages == ("Great, let's learn more about each kid one at a time. How old is Ava?",)
    assert session.child_names == ["Ava", "Ben"]
    store.close()


def test_dm_onboarding_stays_in_messages_even_when_link_service_is_available(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
        "I help run the household with you by keeping logistics organized, surfacing reminders, and staying on top of school and calendar noise.",
        "Connect your Google account so I can pull up to the last year of family email and calendar in the background while we keep going here.",
        "https://example.com/google/connect",
        "Once Google says you're connected, come right back here. You can also keep answering my questions while it runs.",
        "What are your kids' names? Send all of them in one message, one per line or comma-separated.",
    )
    assert session.parent_display_name == "Maya"
    assert session.stage == "collect_child_names"
    store.close()


def test_dm_status_question_after_google_connect_falls_back_to_stock_sync_update_when_chat_is_empty(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService(
        "",
        sync_waiting_text="I’m still syncing in the background, but I’ll text you here when the first pass is ready.",
    )
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
        google_account_link_service=_StubGoogleAccountLinkService(),
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

    assert result.reply_text == (
        "Google is connected. I’m syncing up to the last year of your email and calendar in the background now, and I’ll text you here when the first pass is ready."
    )
    assert chat_service.calls
    assert chat_service.sync_waiting_calls == []
    store.close()


def test_dm_status_question_after_google_connect_uses_household_chat_context(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService(
        "I’m still syncing in the background, but I’ll text you here when the first pass is ready.",
        sync_waiting_text="I’m still syncing in the background, but I’ll text you here when the first pass is ready.",
    )
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_sync_progress_chat",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="What's the sync status?",
                is_group_chat=False,
            ),
        )
    )

    assert result.reply_text == "I’m still syncing in the background, but I’ll text you here when the first pass is ready."
    assert chat_service.calls
    assert "first Gmail and Calendar sync is still running" in chat_service.calls[0]["message_text"]
    assert "What's the sync status?" in chat_service.calls[0]["message_text"]
    assert chat_service.sync_waiting_calls == []
    store.close()


def test_dm_data_dependent_question_during_initial_sync_sets_data_dependent_flag(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService(
        "I’m still syncing, so I can’t answer from your calendar confidently yet.",
        sync_waiting_text="I’m still syncing, so I can’t answer from your calendar confidently yet.",
    )
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_sync_calendar_question",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Can you check tomorrow's calendar?",
                is_group_chat=False,
            ),
        )
    )

    assert result.reply_text == "I’m still syncing, so I can’t answer from your calendar confidently yet."
    assert chat_service.sync_waiting_calls
    assert chat_service.sync_waiting_calls[0]["user_message"] == "Can you check tomorrow's calendar?"
    assert chat_service.sync_waiting_calls[0]["data_dependent"] is True
    store.close()


def test_dm_share_reply_promotes_latest_brief_to_group(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
    )
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
            provider_channel_id="dm_thread_123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_group_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="group_thread_123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Parent group",
        )
    )
    store.append_channel_message(
        ChannelMessage(
            id="msg_asst_shareable",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.ASSISTANT,
            body="I went through your recent email and calendar activity.",
            metadata={
                "promotable_group_message": "Florence pulled together a quick household update:\n- Science fair Friday\n- Soccer photos Monday",
            },
            created_at=datetime.now(tz=timezone.utc).timestamp(),
        )
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_share_brief",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="share that with the group",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "Shared a short version with the parent group."
    assert result.group_announcement == (
        "Florence pulled together a quick household update:\n- Science fair Friday\n- Soccer photos Monday"
    )
    updated = store.get_channel_message("msg_asst_shareable")
    assert updated is not None
    assert updated.metadata["promoted_group_channel_id"] == "chan_group_123"
    assert updated.metadata["promoted_to_group_at"]
    store.close()


def test_dm_share_reply_can_compose_group_safe_summary_from_recent_dm(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService(
        "I can keep planning with you here.",
        promotion_text="Household update: science fair is Friday and dinner is covered tonight.",
    )
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )
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
            provider_channel_id="dm_thread_123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_group_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="group_thread_123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Parent group",
        )
    )
    store.append_channel_message(
        ChannelMessage(
            id="msg_user_prev",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.USER,
            sender_member_id="mem_123",
            body="Science fair is Friday and I already planned tacos for dinner.",
            created_at=datetime.now(tz=timezone.utc).timestamp() - 10,
        )
    )
    store.append_channel_message(
        ChannelMessage(
            id="msg_asst_prev",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.ASSISTANT,
            body="I can remind you about the science fair and keep taco night in the plan.",
            created_at=datetime.now(tz=timezone.utc).timestamp() - 5,
        )
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_share_generic",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="share that with the group",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "Shared a short version with the parent group."
    assert result.group_announcement == "Household update: science fair is Friday and dinner is covered tonight."
    assert len(chat_service.promotion_calls) == 1
    assert "Science fair is Friday" in chat_service.promotion_calls[0]["source_text"]
    assert "share that with the group" not in chat_service.promotion_calls[0]["source_text"]
    store.close()


def test_completed_dm_meal_request_routes_through_household_chat(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    _complete_hybrid_onboarding(onboarding_service)
    chat_service = _StubHouseholdChatService(
        "I can keep planning with you here.",
    )
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )
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
            provider_channel_id="dm_thread_123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_meal_capture",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Can you plan dinners for this week and make the grocery list too?",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "I can keep planning with you here."
    assert chat_service.calls[0]["message_text"] == "Can you plan dinners for this week and make the grocery list too?"
    store.close()


def test_completed_group_media_message_routes_through_household_chat(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    _complete_hybrid_onboarding(onboarding_service)
    chat_service = _StubHouseholdChatService(
        "I can keep planning with you here.",
    )
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )
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
            id="chan_group_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="group_thread_123",
            channel_type=ChannelType.HOUSEHOLD_GROUP,
            title="Parent group",
        )
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_group_123",
            thread_id="group_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_group_media",
                thread_id="group_thread_123",
                sender_handle="+15555550123",
                body=(
                    "Can you add this please?\n\n"
                    "Media context extracted from attachments:\n"
                    "- school-flyer.png: Science fair is Friday at 6 PM. Wear blue. PTA meeting Monday at 7 PM."
                ),
                is_group_chat=True,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "I can keep planning with you here."
    assert "Media context extracted from attachments" in chat_service.calls[0]["message_text"]
    store.close()


def test_dm_acknowledgement_during_sync_does_not_loop_setup_messages(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        google_account_link_service=_StubGoogleAccountLinkService(),
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
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I can help you think through Friday pickup while the sync finishes.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        google_account_link_service=_StubGoogleAccountLinkService(),
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


def test_dm_statement_during_sync_without_question_marker_still_falls_through_to_household_chat(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I can help you sort out Friday pickup while the sync finishes.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        google_account_link_service=_StubGoogleAccountLinkService(),
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

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_sync_statement",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="I need help figuring out Friday pickup while this is syncing",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "I can help you sort out Friday pickup while the sync finishes."
    assert "first Gmail and Calendar sync is still running" in chat_service.calls[0]["message_text"]
    store.close()


def test_complete_dm_routes_freeform_chat_through_household_chat_service(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I can keep planning with you here.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I can pull exact Giants and A's dates now.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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


def test_pending_candidate_does_not_hijack_calendar_question_without_explicit_review_request(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I can check the Roosevelt calendar for Friday now.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )
    _complete_hybrid_onboarding(onboarding_service)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_123b",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:school-updates",
            title="Roosevelt Friday schedule update",
            summary="Friday dismissal is 12:30 PM.",
            state=CandidateState.PENDING_REVIEW,
        )
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_302b",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Can you check the Roosevelt calendar for Friday?",
                is_group_chat=False,
            ),
        )
    )

    assert result.reply_text == "I can check the Roosevelt calendar for Friday now."
    assert chat_service.calls[0]["message_text"] == "Can you check the Roosevelt calendar for Friday?"
    candidate = store.get_imported_candidate("cand_123b")
    assert candidate is not None
    assert candidate.state == CandidateState.PENDING_REVIEW
    store.close()


def test_review_prompt_then_yes_confirms_pending_candidate(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
    assert "Fireflies Haircuts for Kids" in review.reply_text
    assert "Reply yes if I should add it, no if it's wrong, or skip for later." in review.reply_text
    review_messages = store.list_channel_messages(channel_id="chan_dm_123", limit=8)
    latest_review_message = next(
        message
        for message in reversed(review_messages)
        if message.sender_role == ChannelMessageRole.ASSISTANT
    )
    assert latest_review_message.metadata["protocol_kind"] == CANDIDATE_REVIEW_PROMPT_KIND

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
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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


def test_child_activity_answer_advances_to_google_connect_before_unlocking_agent(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="7",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="Roosevelt Elementary",
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
        "Connect your Google account so I can pull up to the last year of family email and calendar in the background while we keep going here.",
        "Once Google says you're connected, come right back here. You can also keep answering my questions while it runs.",
    )
    assert session.is_complete is False
    assert session.stage == "connect_google"
    store.close()


def test_child_name_parsing_from_freeform_sentence_keeps_only_names(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
    assert "how old is theo" in result.reply_text.lower()
    store.close()


def test_google_done_after_child_details_completes_onboarding(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="7",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="Roosevelt Elementary",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="Soccer",
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
                body="done",
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
    store.close()


def test_google_callback_copy_does_not_require_group_to_unlock_agent(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
    onboarding_service.record_google_connected(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
    )
    onboarding_service.record_user_reply(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="7",
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
                body="Roosevelt Elementary",
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
    assert result.reply_messages == ("What activities does Ava do? If none right now, say none.",)
    assert session.is_complete is False
    store.close()


def test_child_age_reply_advances_immediately_after_google_connect(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
        child_names=["Lexie"],
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
                message_id="msg_lexie_age",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="she's 7",
                is_group_chat=False,
            ),
        )
    )

    assert result.reply_messages == ("What school does Lexie go to? If not in school yet, say not yet.",)
    store.close()


def test_activity_completion_after_google_records_onboarding_completion(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
                message_id="msg_205",
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
    events = store.list_pilot_events(household_id="hh_123", event_type="onboarding_complete")
    assert result.consumed is True
    assert result.reply_messages == ("What school does Ava go to? If not in school yet, say not yet.",)
    assert session.is_complete is False
    assert session.stage == "collect_child_school"
    assert len(events) == 0
    store.close()


def test_first_group_message_after_context_collection_records_group_channel(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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


def test_known_parent_new_dm_thread_does_not_restart_onboarding(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I remember you. What do you need?")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )

    _complete_hybrid_onboarding(onboarding_service)

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_new",
            thread_id="dm_thread_new",
            message=FlorenceInboundMessage(
                provider="sendblue",
                message_id="msg_new_thread_1",
                thread_id="dm_thread_new",
                sender_handle="+15555550123",
                body="Hi",
                is_group_chat=False,
            ),
        )
    )

    resumed = onboarding_service.get_or_create_session(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_new",
    )
    assert resumed.is_complete is True
    assert result.consumed is True
    assert result.reply_text == "I remember you. What do you need?"
    assert chat_service.calls[0]["message_text"] == "Hi"
    store.close()


def test_complete_dm_schedule_question_routes_through_household_chat_service_before_state_shortcuts(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I can check Musical Beginnings and pull the spring break dates.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I found the Musical Beginnings spring break email and pulled the dates.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService(
        "Here’s what I’m tracking right now: school reminders, upcoming events, and groceries that still need a plan."
    )
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
    assert "tracking right now" in result.reply_text.lower()
    assert chat_service.calls[0]["message_text"] == "What are you tracking for us right now?"
    store.close()


def test_complete_dm_reminder_feedback_records_preference_and_logs_event(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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

    preferences = store.list_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.PREFERENCE,
    )
    assert len(preferences) == 1
    assert preferences[0].label == "Reminder style"
    assert preferences[0].metadata["category"] == "reminder_style"
    assert preferences[0].metadata["value"] == "Too many reminders too early. Morning-of is better for practices."
    events = store.list_pilot_events(household_id="hh_123", event_type="reminder_feedback_received")
    assert len(events) == 1
    store.close()


def test_complete_dm_done_without_active_reminder_falls_through_to_household_chat(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("Tell me which reminder or task you want to update.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
                message_id="msg_206b",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="done",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "Tell me which reminder or task you want to update."
    assert chat_service.calls[0]["message_text"] == "done"
    store.close()


def test_complete_dm_done_acknowledges_sent_nudge_and_marks_work_item_done(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
    onboarding_service = _build_onboarding_service(store, review_service)
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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
    onboarding_service = _build_onboarding_service(store, review_service)
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

    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
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

    assert result.consumed is True
    assert result.reply_text == "I can keep planning with you here."
    store.close()
