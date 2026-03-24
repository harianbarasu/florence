from florence.contracts import AppChatMessageRole, AppChatScope
from florence.runtime.app_chat import FlorenceAppChatService
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime.services import (
    FlorenceCandidateReviewService,
    FlorenceHouseholdQueryService,
    FlorenceOnboardingSessionService,
)
from florence.state import FlorenceStateDB


class _FakeAgent:
    last_run = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def run_conversation(self, user_message, system_message, conversation_history=None):
        _FakeAgent.last_run = {
            "user_message": user_message,
            "system_message": system_message,
            "conversation_history": conversation_history or [],
        }
        return {"final_response": f"Shared reply for: {user_message}"}


def _build_service(store: FlorenceStateDB) -> FlorenceAppChatService:
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )
    household_chat_service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        agent_factory=_FakeAgent,
    )
    return FlorenceAppChatService(
        store,
        onboarding_service=onboarding_service,
        candidate_review_service=review_service,
        query_service=FlorenceHouseholdQueryService(store),
        household_chat_service=household_chat_service,
        google_account_link_service=None,
    )


def test_app_chat_bootstrap_creates_shared_and_private_threads(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    service = _build_service(store)

    result = service.bootstrap_parent(parent_name="Maya")

    assert result.household.name == "Maya's household"
    assert result.private_thread.scope == AppChatScope.PRIVATE
    assert result.shared_thread.scope == AppChatScope.SHARED
    assert result.assistant_message is not None
    assert "Google" in result.assistant_message.body
    private_messages = service.list_messages(channel_id=result.private_thread.channel.id)
    assert len(private_messages) == 1
    assert private_messages[0].sender_role == AppChatMessageRole.ASSISTANT
    store.close()


def test_app_chat_private_onboarding_completes_and_shared_chat_persists_history(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    service = _build_service(store)
    bootstrap = service.bootstrap_parent(parent_name="Maya")

    service.send_message(
        household_id=bootstrap.household.id,
        member_id=bootstrap.member.id,
        scope=AppChatScope.PRIVATE,
        text="done",
    )
    service.send_message(
        household_id=bootstrap.household.id,
        member_id=bootstrap.member.id,
        scope=AppChatScope.PRIVATE,
        text="Ava",
    )
    service.send_message(
        household_id=bootstrap.household.id,
        member_id=bootstrap.member.id,
        scope=AppChatScope.PRIVATE,
        text="Roosevelt Elementary",
    )
    completed = service.send_message(
        household_id=bootstrap.household.id,
        member_id=bootstrap.member.id,
        scope=AppChatScope.PRIVATE,
        text="Soccer",
    )

    assert completed.assistant_message is not None
    assert "shared household chat" in completed.assistant_message.body.lower()

    first_shared = service.send_message(
        household_id=bootstrap.household.id,
        member_id=bootstrap.member.id,
        scope=AppChatScope.SHARED,
        text="What should we handle this week?",
    )
    assert first_shared.assistant_message is not None
    assert "Shared reply" in first_shared.assistant_message.body

    second_shared = service.send_message(
        household_id=bootstrap.household.id,
        member_id=bootstrap.member.id,
        scope=AppChatScope.SHARED,
        text="Anything else?",
    )
    assert second_shared.assistant_message is not None
    history = _FakeAgent.last_run["conversation_history"]
    assert len(history) == 2
    assert history[0]["role"] == "user"
    assert "What should we handle this week?" in history[0]["content"]
    assert history[1]["role"] == "assistant"
    assert "Shared reply for: What should we handle this week?" in history[1]["content"]

    shared_messages = service.list_messages(channel_id=bootstrap.shared_thread.channel.id)
    assert len(shared_messages) == 4
    assert shared_messages[0].sender_role == AppChatMessageRole.USER
    assert shared_messages[1].sender_role == AppChatMessageRole.ASSISTANT
    store.close()
