import json
import re
from dataclasses import replace
from florence.messaging import (
    FlorenceInboundAttachment,
    FlorenceInboundMessage,
    FlorenceMessagingIngressService,
    FlorenceResolvedInboundMessage,
)
from florence.messaging.protocol_types import (
    CANDIDATE_REVIEW_PROMPT_KIND,
    build_google_connect_prompt_metadata,
)
from datetime import datetime, timedelta, timezone
from florence.onboarding import OnboardingStage

from florence.contracts import (
    CandidateState,
    Channel,
    ChannelMessage,
    ChannelMessageRole,
    ChannelType,
    ChildProfile,
    GoogleConnection,
    GoogleSourceKind,
    Household,
    HouseholdSourceVisibility,
    HouseholdNudge,
    HouseholdNudgeStatus,
    HouseholdNudgeTargetKind,
    HouseholdProfileKind,
    IdentityKind,
    ImportedCandidate,
    Member,
    MemberIdentity,
    MemberRole,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
)
from florence.runtime import (
    FlorenceCandidateReviewService,
    FlorenceHouseholdManagerService,
    FlorenceHouseholdLinkService,
    FlorenceIdentityResolver,
    FlorenceOnboardingSessionService,
)
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.state import FlorenceStateDB
from hermes_state import SessionDB
from model_tools import handle_function_call


_TEST_CHILD_AGE_PATTERN = re.compile(
    r"\b(?P<name>[A-Z][A-Za-z'’-]*)"
    r"(?:\s+(?:is|turns|will\s+be|will\s+turn|is\s+about\s+to\s+turn|about\s+to\s+turn)\s+|\s+)"
    r"(?P<age>\d{1,2})\b"
)


class _StubGoogleAccountLinkService:
    def build_connect_link(self, *, household_id: str, member_id: str, thread_id: str):
        class _Link:
            url = "https://example.com/google/connect"

        return _Link()


def _split_test_child_entries(text: str) -> list[str]:
    normalized = re.sub(r"\b(?:and|&)\b", ",", text, flags=re.IGNORECASE)
    normalized = normalized.replace("\n", ",")
    return [part.strip(" .,!?:;") for part in normalized.split(",") if part.strip(" .,!?:;")]


def _extract_test_child_names_and_updates(text: str) -> tuple[list[str], list[dict[str, str]]]:
    updates: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in _TEST_CHILD_AGE_PATTERN.finditer(text):
        name = str(match.group("name") or "").strip(" .,!?:;")
        age = str(match.group("age") or "").strip()
        if not name or not age or name.lower() in seen:
            continue
        seen.add(name.lower())
        updates.append({"name": name, "age": age})
    if updates:
        return ([update["name"] for update in updates], updates)
    child_names: list[str] = []
    for entry in _split_test_child_entries(text):
        token_match = re.match(r"([A-Za-z][A-Za-z'’-]*)", entry)
        if token_match is None:
            continue
        name = token_match.group(1).strip()
        if name.lower() in seen:
            continue
        seen.add(name.lower())
        child_names.append(name)
    return (child_names, [])


def _record_test_onboarding_reply(
    onboarding_service,
    *,
    household_id: str,
    member_id: str,
    thread_id: str,
    text: str,
):
    session = onboarding_service.get_or_create_session(
        household_id=household_id,
        member_id=member_id,
        thread_id=thread_id,
    )
    if session.stage == OnboardingStage.COLLECT_CHILD_AGE:
        return onboarding_service.apply_explicit_update(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            age=text,
        )
    if session.stage == OnboardingStage.COLLECT_CHILD_SCHOOL:
        return onboarding_service.apply_explicit_update(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            school=text,
        )
    if session.stage == OnboardingStage.COLLECT_CHILD_ACTIVITIES:
        return onboarding_service.apply_explicit_update(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
            activities=[] if text.strip().lower().startswith("none") else [text],
        )
    raise AssertionError(f"Unexpected test onboarding reply stage: {session.stage}")


class _StubHouseholdChatService:
    def __init__(
        self,
        reply_text: str,
        *,
        onboarding_turn_handler=None,
        onboarding_turn_text: str | None = None,
        promotion_text: str | None = None,
        review_prompt_text: str | None = None,
        sync_waiting_text: str | None = None,
    ):
        self.reply_text = reply_text
        self.onboarding_turn_handler = onboarding_turn_handler
        self.onboarding_turn_text = onboarding_turn_text
        self.promotion_text = promotion_text
        self.review_prompt_text = review_prompt_text
        self.sync_waiting_text = sync_waiting_text or (
            "Google is connected. I’m syncing up to the last year of your email and calendar in the background now, and I’ll text you here when the first pass is ready."
        )
        self.calls = []
        self.onboarding_turn_calls = []
        self.review_queue_turn_calls = []
        self.group_share_turn_calls = []
        self.group_intro_turn_calls = []
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
        message_attachments=(),
        conversation_history=None,
    ):
        self.calls.append(
            {
                "household_id": household_id,
                "channel_id": channel_id,
                "actor_member_id": actor_member_id,
                "message_text": message_text,
                "message_attachments": message_attachments,
                "conversation_history": conversation_history or [],
            }
        )

        class _Reply:
            text = self.reply_text

        return _Reply()

    def compose_onboarding_turn(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        payload=None,
        conversation_history=None,
    ) -> tuple[str, ...] | None:
        payload = dict(payload or {})
        self.onboarding_turn_calls.append(
            {
                "household_id": household_id,
                "channel_id": channel_id,
                "actor_member_id": actor_member_id,
                "payload": payload,
                "conversation_history": conversation_history or [],
            }
        )
        if self.onboarding_turn_handler is not None:
            messages = self.onboarding_turn_handler(
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=actor_member_id,
                payload=payload,
            )
            if messages is None:
                return None
            return tuple(message for message in messages if message)
        if self.onboarding_turn_text is None:
            return None
        return (self.onboarding_turn_text,)

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
        if kind == "onboarding_turn":
            messages = self.compose_onboarding_turn(
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=actor_member_id,
                payload=payload,
                conversation_history=conversation_history,
            )
            return messages[0] if messages else None
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
        if kind == "review_queue_turn":
            self.review_queue_turn_calls.append(
                {
                    "household_id": household_id,
                    "channel_id": channel_id,
                    "actor_member_id": actor_member_id,
                    "user_message": payload.get("user_message"),
                    "prompt_armed": bool(payload.get("prompt_armed")),
                }
            )
            normalized = " ".join(str(payload.get("user_message") or "").split()).strip().lower()
            if not payload.get("prompt_armed") and any(
                phrase in normalized
                for phrase in (
                    "review imports",
                    "review queue",
                    "pending imports",
                    "pending candidates",
                    "anything to review",
                    "show imports",
                    "show queue",
                    "what's pending",
                    "what is pending",
                )
            ):
                return "SHOW_CURRENT_REVIEW_PROMPT"
            return "NO_REVIEW_PROTOCOL_ACTION"
        if kind == "group_share_turn":
            self.group_share_turn_calls.append(
                {
                    "household_id": household_id,
                    "channel_id": channel_id,
                    "actor_member_id": actor_member_id,
                    "user_message": payload.get("user_message"),
                    "latest_assistant_protocol_kind": payload.get("latest_assistant_protocol_kind"),
                }
            )
            normalized = " ".join(str(payload.get("user_message") or "").split()).strip().lower()
            latest_protocol_kind = str(payload.get("latest_assistant_protocol_kind") or "").strip()
            if latest_protocol_kind == CANDIDATE_REVIEW_PROMPT_KIND and normalized in {"share", "private"}:
                return "NO_GROUP_SHARE_PROTOCOL_ACTION"
            if (
                ("group" in normalized or "family" in normalized or "everyone" in normalized)
                and any(word in normalized for word in ("share", "send", "post"))
            ) or normalized in {"share that", "share it", "send it", "post it"}:
                return "EXECUTE_GROUP_SHARE"
            return "NO_GROUP_SHARE_PROTOCOL_ACTION"
        if kind == "group_intro_turn":
            self.group_intro_turn_calls.append(
                {
                    "household_id": household_id,
                    "channel_id": channel_id,
                    "actor_member_id": actor_member_id,
                    "user_message": payload.get("user_message"),
                }
            )
            normalized = " ".join(str(payload.get("user_message") or "").split()).strip().lower()
            if normalized in {
                "hi",
                "hey",
                "hello",
                "hi florence",
                "hey florence",
                "hello florence",
                "hey there florence",
                "hello there florence",
            }:
                return "SHOW_GROUP_INTRO"
            return "NO_GROUP_INTRO_PROTOCOL_ACTION"
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


class _OnboardingToolAgent:
    created = []
    last_run = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.session_id = kwargs.get("session_id")
        _OnboardingToolAgent.created.append(kwargs)

    def run_conversation(self, user_message, system_message, conversation_history=None, task_id=None):
        payload = json.loads(user_message)
        _OnboardingToolAgent.last_run = {
            "user_message": user_message,
            "system_message": system_message,
            "conversation_history": conversation_history or [],
            "task_id": task_id,
        }
        if payload.get("task") == "group_share_turn_decision":
            return {"final_response": "NO_GROUP_SHARE_PROTOCOL_ACTION"}
        if payload.get("task") == "review_queue_turn_decision":
            return {"final_response": "NO_REVIEW_PROTOCOL_ACTION"}
        assert payload["task"] == "handle_onboarding_turn"
        result = json.loads(
            handle_function_call(
                "household_apply_onboarding_update",
                {
                    "child_updates": [
                        {"name": "Theo", "school": "Roosevelt Elementary"},
                        {"name": "Violet", "school": "Little Sprouts Preschool"},
                    ]
                },
                task_id=task_id,
            )
        )
        reply_messages = result.get("result", {}).get("reply_messages") or []
        return {
            "final_response": reply_messages[0] if reply_messages else "What activities does Theo do right now? If none, just say none."
        }


class _OnboardingHandoffAgent:
    created = []
    runs = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.session_id = kwargs.get("session_id")
        _OnboardingHandoffAgent.created.append(kwargs)

    def run_conversation(self, user_message, system_message, conversation_history=None, task_id=None):
        payload = json.loads(user_message)
        _OnboardingHandoffAgent.runs.append(
            {
                "task": payload.get("task"),
                "user_message": user_message,
                "system_message": system_message,
                "conversation_history": conversation_history or [],
                "task_id": task_id,
            }
        )
        if payload.get("task") == "group_share_turn_decision":
            return {"final_response": "NO_GROUP_SHARE_PROTOCOL_ACTION"}
        if payload.get("task") == "review_queue_turn_decision":
            return {"final_response": "NO_REVIEW_PROTOCOL_ACTION"}
        if payload.get("task") == "handle_onboarding_turn":
            return {"final_response": "HANDOFF_TO_SYNC_WAITING"}
        if payload.get("task") == "compose_sync_waiting_reply":
            return {"final_response": "Still syncing, so I can’t answer from your calendar confidently yet."}
        return {"final_response": "I can keep helping here."}


class _ReminderToolAgent:
    created = []
    runs = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.session_id = kwargs.get("session_id")
        _ReminderToolAgent.created.append(kwargs)

    def run_conversation(self, user_message, system_message, conversation_history=None, task_id=None):
        if user_message.lstrip().startswith("{"):
            payload = json.loads(user_message)
            task = payload.get("task")
            if task == "group_share_turn_decision":
                return {"final_response": "NO_GROUP_SHARE_PROTOCOL_ACTION"}
            if task == "review_queue_turn_decision":
                return {"final_response": "NO_REVIEW_PROTOCOL_ACTION"}
        _ReminderToolAgent.runs.append(
            {
                "user_message": user_message,
                "system_message": system_message,
                "conversation_history": conversation_history or [],
                "task_id": task_id,
            }
        )
        nudge_match = re.search(r'nudge_id="([^"]+)"', user_message)
        reply_match = re.search(r"User reply:\s*(.+)\s*$", user_message, re.DOTALL)
        nudge_id = nudge_match.group(1) if nudge_match else ""
        reply_text = reply_match.group(1).strip() if reply_match else ""
        if not nudge_id:
            return {"final_response": "Which reminder do you want to update?"}
        lowered = " ".join(reply_text.lower().split())
        if lowered == "done":
            result = json.loads(
                handle_function_call(
                    "household_apply_nudge_action",
                    {"nudge_id": nudge_id, "action": "done"},
                    task_id=task_id,
                )
            )
            return {"final_response": result.get("result", {}).get("reply_text") or "Done."}
        if lowered.startswith("snooze"):
            hours_match = re.search(r"(\d+)\s*(?:h|hr|hrs|hour|hours)\b", lowered)
            minutes_match = re.search(r"(\d+)\s*(?:m|min|mins|minute|minutes)\b", lowered)
            days_match = re.search(r"(\d+)\s*(?:d|day|days)\b", lowered)
            delay = timedelta(hours=2)
            if days_match:
                delay = timedelta(days=max(1, int(days_match.group(1))))
            elif hours_match:
                delay = timedelta(hours=max(1, int(hours_match.group(1))))
            elif minutes_match:
                delay = timedelta(minutes=max(1, int(minutes_match.group(1))))
            scheduled_for = (datetime.now(timezone.utc) + delay).isoformat()
            result = json.loads(
                handle_function_call(
                    "household_apply_nudge_action",
                    {
                        "nudge_id": nudge_id,
                        "action": "snooze",
                        "scheduled_for": scheduled_for,
                    },
                    task_id=task_id,
                )
            )
            return {"final_response": result.get("result", {}).get("reply_text") or "Snoozed."}
        return {"final_response": "Tell me which reminder you want to update."}


def _build_onboarding_service(store, review_service):
    return FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )


def _simulate_hermes_onboarding_turn(
    *,
    onboarding_service: FlorenceOnboardingSessionService,
    household_id: str,
    channel_id: str,
    actor_member_id: str | None,
    payload: dict[str, object],
) -> tuple[str, ...] | None:
    if actor_member_id is None:
        return None
    user_message = str(payload.get("user_message") or "")
    normalized = " ".join(user_message.strip().lower().split())
    acknowledgement = normalized in {
        "ok",
        "okay",
        "sounds good",
        "sgtm",
        "got it",
        "cool",
        "nice",
        "great",
        "perfect",
        "thanks",
        "thank you",
        "awesome",
        "works for me",
        "understood",
        "roger",
        "👍",
        "🙏",
    }
    thread_id = str(payload.get("thread_id") or "").strip()
    channel = onboarding_service.store.get_channel(channel_id)
    if not thread_id and channel is not None and channel.provider_channel_id:
        thread_id = channel.provider_channel_id
    if not thread_id:
        return None
    previous_session = onboarding_service.get_or_create_session(
        household_id=household_id,
        member_id=actor_member_id,
        thread_id=thread_id,
    )
    if previous_session.google_connected:
        if acknowledgement:
            return ("NO_SETUP_REPLY",)
        if re.search(
            r"\b(?:calendar|email|emails|inbox|gmail)\b|\b(?:check|show|find|pull|search|look\s+up|look\s+in|what(?:'s| is)\s+(?:on|in))\b.*\b(?:schedule|scheduled)\b",
            user_message,
            re.IGNORECASE,
        ):
            return ("HANDOFF_TO_SYNC_WAITING",)
        if (
            ("?" in user_message and len(user_message.split()) > 1)
            or re.search(
                r"\b(?:help figuring out|help with|can you help|could you help|i need help|while this is syncing)\b",
                user_message,
                re.IGNORECASE,
            )
            or user_message.strip().lower().startswith(
                ("can ", "could ", "would ", "what ", "what's ", "when ", "where ", "who ", "why ", "how ", "show ", "check ", "find ", "plan ", "help ", "remind ", "review ", "list ", "share ", "send ", "post ")
            )
        ):
            return ("HANDOFF_TO_CONTEXTUAL_CHAT",)
    elif acknowledgement:
        return None
    if previous_session.stage == OnboardingStage.COLLECT_PARENT_NAME:
        transition = onboarding_service.record_parent_name(
            household_id=household_id,
            member_id=actor_member_id,
            thread_id=thread_id,
            display_name=user_message,
        )
    elif previous_session.stage == OnboardingStage.COLLECT_CHILD_NAMES:
        child_names, child_updates = _extract_test_child_names_and_updates(user_message)
        if not child_names:
            return onboarding_service.get_prompt_messages(
                household_id=household_id,
                member_id=actor_member_id,
                thread_id=thread_id,
            )
        transition = onboarding_service.record_child_names(
            household_id=household_id,
            member_id=actor_member_id,
            thread_id=thread_id,
            child_names=child_names,
        )
        for child_update in child_updates:
            transition = onboarding_service.apply_explicit_update(
                household_id=household_id,
                member_id=actor_member_id,
                thread_id=thread_id,
                child_name=child_update["name"],
                age=child_update["age"],
            )
    else:
        transition = _record_test_onboarding_reply(
            onboarding_service,
            household_id=household_id,
            member_id=actor_member_id,
            thread_id=thread_id,
            text=user_message,
        )
    if transition.changed:
        if transition.state.is_complete:
            FlorenceHouseholdManagerService(onboarding_service.store).finalize_onboarding_completion(
                household_id=household_id,
                member_id=actor_member_id,
                channel_id=channel_id,
            )
        return onboarding_service.get_transition_messages(
            transition,
            previous_stage=previous_session.stage,
            household_id=household_id,
            member_id=actor_member_id,
            thread_id=thread_id,
        )
    return onboarding_service.get_prompt_messages(
        household_id=household_id,
        member_id=actor_member_id,
        thread_id=thread_id,
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
    onboarding_turn_handler = lambda household_id, channel_id, actor_member_id, payload: _simulate_hermes_onboarding_turn(
        onboarding_service=onboarding_service,
        household_id=household_id,
        channel_id=channel_id,
        actor_member_id=actor_member_id,
        payload=payload,
    )
    household_chat_service = kwargs.get("household_chat_service")
    if household_chat_service is None:
        kwargs["household_chat_service"] = _StubHouseholdChatService(
            "I can keep planning with you here.",
            onboarding_turn_handler=onboarding_turn_handler,
        )
    elif isinstance(household_chat_service, _StubHouseholdChatService):
        if household_chat_service.onboarding_turn_handler is None and household_chat_service.onboarding_turn_text is None:
            household_chat_service.onboarding_turn_handler = onboarding_turn_handler
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
    _record_test_onboarding_reply(onboarding_service, 
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="7",
    )
    _record_test_onboarding_reply(onboarding_service, 
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="Roosevelt Elementary",
    )
    _record_test_onboarding_reply(onboarding_service, 
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


def test_dm_pending_household_link_request_prompts_and_auto_merges_lightweight_household(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    store.upsert_household(Household(id="hh_jackson", name="Jackson household", timezone="America/Los_Angeles"))
    store.upsert_household(Household(id="hh_kendall", name="Kendall household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_jackson", household_id="hh_jackson", display_name="Jackson", role=MemberRole.ADMIN))
    store.upsert_member(Member(id="mem_kendall", household_id="hh_kendall", display_name="Kendall", role=MemberRole.ADMIN))
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_kendall",
            member_id="mem_kendall",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0124",
            normalized_value="+15555550124",
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_kendall_dm",
            household_id="hh_kendall",
            provider="sendblue",
            provider_channel_id="thread-kendall",
            channel_type=ChannelType.PARENT_DM,
            title="Kendall",
            metadata={"sender_handle": "+15555550124"},
        )
    )
    FlorenceHouseholdLinkService(store).create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )
    ingress = _build_ingress(store, onboarding_service, review_service)

    prompt = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_kendall",
            member_id="mem_kendall",
            channel_id="chan_kendall_dm",
            thread_id="thread-kendall",
            message=FlorenceInboundMessage(
                provider="sendblue",
                message_id="msg-kendall-1",
                thread_id="thread-kendall",
                sender_handle="+1 (555) 555-0124",
                body="hey",
                is_group_chat=False,
            ),
        )
    )

    assert prompt.consumed is True
    assert "Jackson wants to connect you to the same household here." in prompt.reply_text

    merged = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_kendall",
            member_id="mem_kendall",
            channel_id="chan_kendall_dm",
            thread_id="thread-kendall",
            message=FlorenceInboundMessage(
                provider="sendblue",
                message_id="msg-kendall-2",
                thread_id="thread-kendall",
                sender_handle="+1 (555) 555-0124",
                body="yes",
                is_group_chat=False,
            ),
        )
    )

    assert merged.consumed is True
    assert "linked into the same household now" in (merged.reply_text or "").lower()
    assert store.get_member("mem_kendall").household_id == "hh_jackson"
    assert store.get_channel("chan_kendall_dm").household_id == "hh_jackson"
    assert store.list_channel_messages(channel_id="chan_kendall_dm")[-1].household_id == "hh_jackson"
    store.close()


def test_dm_pending_household_link_accepts_first_yes_after_outbound_invite(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    store.upsert_household(Household(id="hh_jackson", name="Jackson household", timezone="America/Los_Angeles"))
    store.upsert_household(Household(id="hh_kendall", name="Kendall household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_jackson", household_id="hh_jackson", display_name="Jackson", role=MemberRole.ADMIN))
    store.upsert_member(Member(id="mem_kendall", household_id="hh_kendall", display_name="Kendall", role=MemberRole.ADMIN))
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_kendall",
            member_id="mem_kendall",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0124",
            normalized_value="+15555550124",
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_kendall_dm",
            household_id="hh_kendall",
            provider="sendblue",
            provider_channel_id="thread-kendall",
            channel_type=ChannelType.PARENT_DM,
            title="Kendall",
            metadata={"sender_handle": "+15555550124"},
        )
    )
    service = FlorenceHouseholdLinkService(store)
    request = service.create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )
    store.upsert_household_link_request(
        replace(
            request,
            metadata={
                **dict(request.metadata),
                "invited_message_sent_at": "2026-04-08T21:00:00+00:00",
            },
        )
    )
    ingress = _build_ingress(store, onboarding_service, review_service)

    merged = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_kendall",
            member_id="mem_kendall",
            channel_id="chan_kendall_dm",
            thread_id="thread-kendall",
            message=FlorenceInboundMessage(
                provider="sendblue",
                message_id="msg-kendall-yes",
                thread_id="thread-kendall",
                sender_handle="+1 (555) 555-0124",
                body="yes",
                is_group_chat=False,
            ),
        )
    )

    assert merged.consumed is True
    assert "linked into the same household now" in (merged.reply_text or "").lower()
    assert store.get_member("mem_kendall").household_id == "hh_jackson"
    store.close()


def test_dm_mature_household_link_merges_after_invited_yes(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    store.upsert_household(Household(id="hh_jackson", name="Jackson household", timezone="America/Los_Angeles"))
    store.upsert_household(Household(id="hh_kendall", name="Kendall household", timezone="America/Los_Angeles"))
    store.upsert_member(Member(id="mem_jackson", household_id="hh_jackson", display_name="Jackson", role=MemberRole.ADMIN))
    store.upsert_member(Member(id="mem_kendall", household_id="hh_kendall", display_name="Kendall", role=MemberRole.ADMIN))
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_jackson",
            member_id="mem_jackson",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0199",
            normalized_value="+15555550199",
        )
    )
    store.upsert_member_identity(
        MemberIdentity(
            id="ident_kendall",
            member_id="mem_kendall",
            kind=IdentityKind.PHONE,
            value="+1 (555) 555-0124",
            normalized_value="+15555550124",
        )
    )
    store.replace_child_profiles(
        household_id="hh_kendall",
        children=[ChildProfile(id="child_theo", household_id="hh_kendall", full_name="Theo Williams")],
    )
    store.upsert_channel(
        Channel(
            id="chan_jackson_dm",
            household_id="hh_jackson",
            provider="sendblue",
            provider_channel_id="thread-jackson",
            channel_type=ChannelType.PARENT_DM,
            title="Jackson",
            metadata={"sender_handle": "+15555550199"},
        )
    )
    store.upsert_channel(
        Channel(
            id="chan_kendall_dm",
            household_id="hh_kendall",
            provider="sendblue",
            provider_channel_id="thread-kendall",
            channel_type=ChannelType.PARENT_DM,
            title="Kendall",
            metadata={"sender_handle": "+15555550124"},
        )
    )
    FlorenceHouseholdLinkService(store).create_phone_link_request(
        household_id="hh_jackson",
        inviting_member_id="mem_jackson",
        invited_phone="+1 (555) 555-0124",
        invited_display_name="Kendall",
    )
    ingress = _build_ingress(store, onboarding_service, review_service)

    ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_kendall",
            member_id="mem_kendall",
            channel_id="chan_kendall_dm",
            thread_id="thread-kendall",
            message=FlorenceInboundMessage(
                provider="sendblue",
                message_id="msg-kendall-1",
                thread_id="thread-kendall",
                sender_handle="+1 (555) 555-0124",
                body="hi",
                is_group_chat=False,
            ),
        )
    )
    invited_yes = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_kendall",
            member_id="mem_kendall",
            channel_id="chan_kendall_dm",
            thread_id="thread-kendall",
            message=FlorenceInboundMessage(
                provider="sendblue",
                message_id="msg-kendall-2",
                thread_id="thread-kendall",
                sender_handle="+1 (555) 555-0124",
                body="yes",
                is_group_chat=False,
            ),
        )
    )

    assert "linked into the same household now" in (invited_yes.reply_text or "").lower()
    assert store.get_member("mem_kendall").household_id == "hh_jackson"
    assert store.get_household("hh_kendall") is None
    store.close()


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
        "What are your kids' names? You can send them all in one message, one per line or comma-separated.",
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
    assert result.reply_messages == ("Great, let's do one kid at a time. How old is Ava?",)
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
        "What are your kids' names? You can send them all in one message, one per line or comma-separated.",
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
    assert ingress.household_chat_service.group_share_turn_calls[-1]["user_message"] == "share that with the group"
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
    assert chat_service.group_share_turn_calls[-1]["user_message"] == "share that with the group"
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
    assert chat_service.calls[0]["message_attachments"] == ()
    store.close()


def test_completed_dm_passes_image_attachments_into_household_chat(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    _complete_hybrid_onboarding(onboarding_service)
    chat_service = _StubHouseholdChatService("I can read the attachment now.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_media_live",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Can you pull the exact closure dates from this image?",
                is_group_chat=False,
                attachments=(
                    FlorenceInboundAttachment(
                        kind="image",
                        mime_type="image/png",
                        filename="studio-closures.png",
                        data_url="data:image/png;base64,QUFBQQ==",
                    ),
                ),
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "I can read the attachment now."
    assert len(chat_service.calls[0]["message_attachments"]) == 1
    assert chat_service.calls[0]["message_attachments"][0].filename == "studio-closures.png"
    store.close()


def test_completed_dm_suppresses_protocol_sentinel_if_chat_model_leaks_one(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    _complete_hybrid_onboarding(onboarding_service)
    chat_service = _StubHouseholdChatService("NO_GROUP_SHARE_PROTOCOL_ACTION")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_protocol_leak",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="You should see it in my email from today.",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == (
        "I saw your message, but I hit a reply problem on my side. "
        "Send it again and I'll answer directly."
    )
    assert chat_service.calls[0]["message_text"] == "You should see it in my email from today."
    store.close()


def test_completed_dm_webcal_link_bypasses_group_share_protocol_and_routes_to_chat(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    _complete_hybrid_onboarding(onboarding_service)
    chat_service = _StubHouseholdChatService("I can inspect Theo's DRALL calendar feed.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_webcal",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="webcal://api.team-manager.gc.com/ics-calendar-documents/user/example.ics?teamId=abc",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "I can inspect Theo's DRALL calendar feed."
    assert chat_service.group_share_turn_calls == []
    assert chat_service.calls[0]["message_text"] == "webcal://api.team-manager.gc.com/ics-calendar-documents/user/example.ics?teamId=abc"
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


def test_review_prompt_then_yes_routes_single_item_context_to_chat(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("Confirmed. I’m adding that review item now.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
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
    assert chat_service.review_queue_turn_calls[-1]["user_message"] == "review imports"
    review_messages = store.list_channel_messages(channel_id="chan_dm_123", limit=8)
    latest_review_message = next(
        message
        for message in reversed(review_messages)
        if message.sender_role == ChannelMessageRole.ASSISTANT
    )
    assert latest_review_message.metadata["protocol_kind"] == CANDIDATE_REVIEW_PROMPT_KIND
    assert latest_review_message.metadata["pending_action_type"] == "candidate_review"
    assert latest_review_message.metadata["pending_action_target_kind"] == "imported_candidate"
    assert latest_review_message.metadata["pending_action_target_id"] == "cand_124"

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
    assert confirmation.reply_text == "Confirmed. I’m adding that review item now."
    assert chat_service.calls
    contextual_message = chat_service.calls[-1]["message_text"]
    assert '"candidate_id": "cand_124"' in contextual_message
    assert "Interpret the whole message yourself, including short yes/no/share/private replies" in contextual_message
    assert "User reply: yes" in contextual_message
    candidate = store.get_imported_candidate("cand_124")
    assert candidate is not None
    assert candidate.state == CandidateState.PENDING_REVIEW
    assert store.list_household_events(household_id="hh_123") == []
    store.close()


def test_review_prompt_yes_stays_bound_to_visible_candidate_even_if_newer_pending_item_arrives(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("Confirmed. I’m adding that review item now.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )
    _complete_hybrid_onboarding(onboarding_service)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_visible",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:one-medical",
            title="One Medical receipt",
            summary="Payment of $37.43 on April 8 for Kendall.",
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
                message_id="msg_visible",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="review imports",
                is_group_chat=False,
            ),
        )
    )
    assert review.reply_text is not None
    assert "One Medical receipt" in review.reply_text

    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_hidden",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:violet-dance",
            title="Violet Dance",
            summary="Dance schedule update.",
            state=CandidateState.PENDING_REVIEW,
        )
    )

    confirmation = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_yes",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="yes",
                is_group_chat=False,
            ),
        )
    )
    assert confirmation.reply_text == "Confirmed. I’m adding that review item now."
    contextual_message = chat_service.calls[-1]["message_text"]
    assert '"candidate_id": "cand_visible"' in contextual_message
    assert '"candidate_id": "cand_hidden"' not in contextual_message
    store.close()


def test_review_batch_reply_context_includes_numbered_active_items(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("Okay — which number do you mean?")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )
    _complete_hybrid_onboarding(onboarding_service)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_batch_1",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:batch-1",
            title="Charlie Cooper birthday",
            summary="Saturday, April 11 at 10:00 AM.",
            state=CandidateState.PENDING_REVIEW,
        )
    )
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_batch_2",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:batch-2",
            title="One Medical receipt",
            summary="$37.43 on April 8 for Kendall.",
            state=CandidateState.PENDING_REVIEW,
            metadata={"candidate_scope": "private_parent"},
        )
    )
    store.append_channel_message(
        ChannelMessage(
            id="msg_review_batch",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.ASSISTANT,
            body="📬 I found a few things to review:\n1. Charlie Cooper birthday\n2. One Medical receipt\nReply with 1 yes, 2 no, 3 skip, or ask me about one of them.",
            metadata={
                "protocol_kind": CANDIDATE_REVIEW_PROMPT_KIND,
                "pending_action_type": "candidate_review",
                "pending_action_target_kind": "imported_candidate",
                "pending_action_target_id": "cand_batch_1",
                "pending_action_target_ids": ["cand_batch_1", "cand_batch_2"],
            },
            created_at=datetime.now(timezone.utc).timestamp(),
        )
    )

    confirmation = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_batch_reply",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="2 no",
                is_group_chat=False,
            ),
        )
    )

    assert confirmation.reply_text == "Okay — which number do you mean?"
    contextual_message = chat_service.calls[-1]["message_text"]
    assert '"index": 1' in contextual_message
    assert '"candidate_id": "cand_batch_1"' in contextual_message
    assert '"index": 2' in contextual_message
    assert '"candidate_id": "cand_batch_2"' in contextual_message
    assert "If the user replies with numbered decisions like 1 yes, 2 no, 3 skip" in contextual_message
    assert "clearly refers to exactly one listed item by title, sender, place, child, or date" in contextual_message
    assert "Zimmi is correct but it's at 7 PM EST" in contextual_message
    assert "User reply: 2 no" in contextual_message
    store.close()


def test_review_prompt_then_corrective_yes_routes_single_item_context_to_chat(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("Got it — I’ll fix the time before I add it.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )
    _complete_hybrid_onboarding(onboarding_service)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_124b",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:music-1",
            title="Theo music class",
            summary="Theo has a music class on June 10 at 4:15 PM.",
            state=CandidateState.PENDING_REVIEW,
            metadata={
                "confirmation_question": "Should I add Theo music class to the household plan?",
                "proposed_fields": {
                    "title": "Theo music class",
                    "starts_at": "2026-06-10T16:15:00-07:00",
                    "ends_at": "2026-06-10T17:00:00-07:00",
                    "timezone": "America/Los_Angeles",
                },
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
                message_id="msg_304a",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="review imports",
                is_group_chat=False,
            ),
        )
    )
    assert review.reply_text is not None

    correction = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_304b",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Yes, add it but the time is 3:30 PM to 4:15 PM.",
                is_group_chat=False,
            ),
        )
    )

    assert correction.reply_text == "Got it — I’ll fix the time before I add it."
    assert chat_service.calls
    contextual_message = chat_service.calls[-1]["message_text"]
    assert "household_apply_candidate_review" in contextual_message
    assert '"candidate_id": "cand_124b"' in contextual_message
    assert "Treat only this item as review-actionable right now." in contextual_message
    assert "User reply: Yes, add it but the time is 3:30 PM to 4:15 PM." in contextual_message
    candidate = store.get_imported_candidate("cand_124b")
    assert candidate is not None
    assert candidate.state == CandidateState.PENDING_REVIEW
    assert store.list_household_events(household_id="hh_123") == []
    store.close()


def test_review_prompt_then_corrective_no_does_not_reject_candidate(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("Got it — the event might be right, but the details need correction.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )
    _complete_hybrid_onboarding(onboarding_service)
    store.upsert_imported_candidate(
        ImportedCandidate(
            id="cand_124c",
            household_id="hh_123",
            member_id="mem_123",
            source_kind=GoogleSourceKind.GMAIL,
            source_identifier="gmail:music-2",
            title="Theo music class",
            summary="Theo has a music class on June 10 at 4:15 PM.",
            state=CandidateState.PENDING_REVIEW,
            metadata={
                "confirmation_question": "Should I add Theo music class to the household plan?",
            },
        )
    )

    ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_304c",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="review imports",
                is_group_chat=False,
            ),
        )
    )

    correction = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_304d",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="No, it is at 3:30 and lasts until 4:15.",
                is_group_chat=False,
            ),
        )
    )

    assert correction.reply_text == "Got it — the event might be right, but the details need correction."
    candidate = store.get_imported_candidate("cand_124c")
    assert candidate is not None
    assert candidate.state == CandidateState.PENDING_REVIEW
    assert store.list_household_events(household_id="hh_123") == []
    assert "Interpret the whole message yourself, including short yes/no/share/private replies" in chat_service.calls[-1]["message_text"]
    store.close()


def test_review_prompt_then_share_routes_to_chat_for_explicit_source_decision(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("Got it — I’ll treat this source as shared household context.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
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
    assert "Reply yes if I should add it, no if it's wrong, or skip for later." in review.reply_text

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
    assert classification.reply_text == "Got it — I’ll treat this source as shared household context."
    assert chat_service.calls
    contextual_message = chat_service.calls[-1]["message_text"]
    assert '"candidate_id": "cand_125"' in contextual_message
    assert "set source_visibility" in contextual_message
    assert "User reply: share" in contextual_message
    rules = store.list_household_source_rules(
        household_id="hh_123",
        source_kind=GoogleSourceKind.GMAIL,
        visibility=HouseholdSourceVisibility.SHARED,
    )
    assert rules == []
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
    _record_test_onboarding_reply(onboarding_service, 
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="7",
    )
    _record_test_onboarding_reply(onboarding_service, 
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


def test_child_name_parsing_from_freeform_sentence_carries_inline_ages_forward(tmp_path):
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
                body="Theo is 7 Violet will be 4 next month",
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
    assert session.child_profiles[0]["age"] == "7"
    assert session.child_profiles[1]["age"] == "4"
    assert result.reply_text is not None
    assert "what school does theo go to" in result.reply_text.lower()
    store.close()


def test_child_name_parsing_from_compact_list_carries_inline_ages_forward(tmp_path):
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
                message_id="msg_child_parse_2",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Theo 7, Violet 4",
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
    assert session.child_profiles[0]["age"] == "7"
    assert session.child_profiles[1]["age"] == "4"
    assert result.reply_text is not None
    assert "what school does theo go to" in result.reply_text.lower()
    store.close()


def test_incomplete_dm_can_route_multi_child_school_updates_through_hermes_onboarding_turn(tmp_path):
    _OnboardingToolAgent.created.clear()
    _OnboardingToolAgent.last_run = None
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
            provider_channel_id="dm_thread_123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
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
        child_names=["Theo", "Violet"],
    )
    onboarding_service.apply_explicit_update(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        child_name="Theo",
        age="7",
    )
    onboarding_service.apply_explicit_update(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        child_name="Violet",
        age="4",
    )
    chat_service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        provider="anthropic",
        agent_factory=_OnboardingToolAgent,
        session_db=SessionDB(tmp_path / "hermes_state.db"),
    )
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_child_schools_1",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Theo goes to Roosevelt Elementary and Violet goes to Little Sprouts Preschool",
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
    assert "what activities does theo do" in result.reply_text.lower()
    assert session.child_profiles[0]["school"] == "Roosevelt Elementary"
    assert session.child_profiles[1]["school"] == "Little Sprouts Preschool"
    assert session.stage == "collect_child_activities"
    assert [created["enabled_toolsets"] for created in _OnboardingToolAgent.created[:2]] == [
        ["florence_briefing"],
        ["florence_onboarding"],
    ]
    assert "Use household_apply_onboarding_update to store only explicit setup facts" in _OnboardingToolAgent.last_run["system_message"]
    payload = json.loads(_OnboardingToolAgent.last_run["user_message"])
    assert payload["task"] == "handle_onboarding_turn"
    assert payload["stage"] == "collect_child_school"
    assert payload["current_child_name"] == "Theo"
    store.close()


def test_incomplete_dm_real_hermes_onboarding_handoff_can_escape_to_sync_waiting(tmp_path):
    _OnboardingHandoffAgent.created.clear()
    _OnboardingHandoffAgent.runs.clear()
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
            provider_channel_id="dm_thread_123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
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
    chat_service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        provider="anthropic",
        agent_factory=_OnboardingHandoffAgent,
        session_db=SessionDB(tmp_path / "hermes_state.db"),
    )
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_sync_handoff_1",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Can you check tomorrow's calendar?",
                is_group_chat=False,
            ),
        )
    )

    assert result.reply_text == "Still syncing, so I can’t answer from your calendar confidently yet."
    assert [run["task"] for run in _OnboardingHandoffAgent.runs] == [
        "group_share_turn_decision",
        "handle_onboarding_turn",
        "compose_sync_waiting_reply",
    ]
    assert "reply exactly HANDOFF_TO_SYNC_WAITING" in _OnboardingHandoffAgent.runs[1]["system_message"]
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
    _record_test_onboarding_reply(onboarding_service, 
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="7",
    )
    _record_test_onboarding_reply(onboarding_service, 
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="Roosevelt Elementary",
    )
    _record_test_onboarding_reply(onboarding_service, 
        household_id="hh_123",
        member_id="mem_123",
        thread_id="dm_thread_123",
        text="Soccer",
    )
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_done_123",
            household_id="hh_123",
            member_id="mem_123",
            email="maya@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL,),
            access_token="access-token",
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
    _record_test_onboarding_reply(onboarding_service, 
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
    assert result.reply_messages == ("What activities does Ava do right now? If none, just say none.",)
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

    assert result.reply_messages == ("What school does Lexie go to? If not yet, just say not yet.",)
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
    assert result.reply_messages == ("What school does Ava go to? If not yet, just say not yet.",)
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
    assert ingress.household_chat_service.group_intro_turn_calls[-1]["user_message"] == "Hey Florence"


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


def test_completed_dm_followup_after_sync_update_brief_routes_back_through_household_chat(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("The dentist appointment moved and pickup timing needs a tweak.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )
    _complete_hybrid_onboarding(onboarding_service)
    ingress.append_assistant_message(
        household_id="hh_123",
        channel_id="chan_dm_123",
        body="I finished another sync pass. The main thing I want you to check is Dentist appointment moved.",
        metadata={"promotion_kind": "sync_update_brief"},
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_sync_update_followup",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="What changed",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "The dentist appointment moved and pickup timing needs a tweak."
    assert chat_service.calls[0]["message_text"] == "What changed"
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
        metadata=build_google_connect_prompt_metadata(),
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


def test_incomplete_setup_does_not_swallow_real_request_that_starts_with_great(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService(
        "Still syncing, but I can keep helping once the first pass lands.",
        sync_waiting_text="Still syncing, but I can keep helping once the first pass lands.",
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
                message_id="msg_setup_sync_1",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Great can you check email for school updates while that sync runs?",
                is_group_chat=False,
            ),
        )
    )

    assert result.reply_text == "Still syncing, but I can keep helping once the first pass lands."
    assert chat_service.sync_waiting_calls[0]["data_dependent"] is True
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


def test_complete_dm_reminder_feedback_routes_through_household_chat(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("I’ll remember that you want fewer reminders and less proactive nudging.")
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
                message_id="msg_206",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Too many reminders too early. Morning-of is better for practices.",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "I’ll remember that you want fewer reminders and less proactive nudging."
    assert chat_service.calls[0]["message_text"] == "Too many reminders too early. Morning-of is better for practices."
    preferences = store.list_household_profile_items(
        household_id="hh_123",
        kind=HouseholdProfileKind.PREFERENCE,
    )
    assert preferences == []
    events = store.list_pilot_events(household_id="hh_123", event_type="reminder_feedback_received")
    assert events == []
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


def test_complete_dm_done_acknowledges_sent_nudge_and_marks_work_item_done_via_hermes(tmp_path):
    _ReminderToolAgent.created.clear()
    _ReminderToolAgent.runs.clear()
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        provider="anthropic",
        agent_factory=_ReminderToolAgent,
        session_db=SessionDB(tmp_path / "hermes_state.db"),
    )
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
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
    store.append_channel_message(
        ChannelMessage(
            id="msg_nudge_prompt_123",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.ASSISTANT,
            body="Reminder: upload the field trip form tonight.",
            metadata={
                "pending_action_type": "household_nudge",
                "pending_action_target_kind": "household_nudge",
                "pending_action_target_id": "nudge_123",
            },
            created_at=datetime.now(timezone.utc).timestamp(),
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
    assert "household_apply_nudge_action" in _ReminderToolAgent.runs[0]["system_message"]
    store.close()


def test_complete_dm_snooze_reschedules_sent_nudge_and_logs_event_via_hermes(tmp_path):
    _ReminderToolAgent.created.clear()
    _ReminderToolAgent.runs.clear()
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = FlorenceHouseholdChatService(
        store,
        model="anthropic/claude-opus-4.6",
        max_iterations=4,
        provider="anthropic",
        agent_factory=_ReminderToolAgent,
        session_db=SessionDB(tmp_path / "hermes_state.db"),
    )
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
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
    store.append_channel_message(
        ChannelMessage(
            id="msg_nudge_prompt_124",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.ASSISTANT,
            body="Reminder: pack baseball gear.",
            metadata={
                "pending_action_type": "household_nudge",
                "pending_action_target_kind": "household_nudge",
                "pending_action_target_id": "nudge_124",
            },
            created_at=datetime.now(timezone.utc).timestamp(),
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
    assert "household_apply_nudge_action" in _ReminderToolAgent.runs[0]["system_message"]
    store.close()


def test_complete_dm_got_it_does_not_acknowledge_sent_nudge(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("Tell me which reminder you want to update.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )
    _complete_hybrid_onboarding(onboarding_service)

    now = datetime.now(timezone.utc)
    nudge = HouseholdNudge(
        id="nudge_125",
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
    store.append_channel_message(
        ChannelMessage(
            id="msg_nudge_prompt_125",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.ASSISTANT,
            body="Reminder: pack baseball gear.",
            metadata={
                "pending_action_type": "household_nudge",
                "pending_action_target_kind": "household_nudge",
                "pending_action_target_id": "nudge_125",
            },
            created_at=datetime.now(timezone.utc).timestamp(),
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
                message_id="msg_209",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="got it",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "Tell me which reminder you want to update."
    updated_nudge = store.get_household_nudge("nudge_125")
    assert updated_nudge is not None
    assert updated_nudge.status == HouseholdNudgeStatus.SENT
    assert store.list_pilot_events(household_id="hh_123", event_type="reminder_done") == []
    store.close()


def test_complete_dm_finished_does_not_acknowledge_sent_nudge(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya's household", timezone="America/Los_Angeles"))
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = _build_onboarding_service(store, review_service)
    chat_service = _StubHouseholdChatService("Tell me which reminder you want to update.")
    ingress = _build_ingress(
        store,
        onboarding_service,
        review_service,
        household_chat_service=chat_service,
    )
    _complete_hybrid_onboarding(onboarding_service)

    now = datetime.now(timezone.utc)
    nudge = HouseholdNudge(
        id="nudge_126",
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
    store.append_channel_message(
        ChannelMessage(
            id="msg_nudge_prompt_126",
            household_id="hh_123",
            channel_id="chan_dm_123",
            sender_role=ChannelMessageRole.ASSISTANT,
            body="Reminder: pack baseball gear.",
            metadata={
                "pending_action_type": "household_nudge",
                "pending_action_target_kind": "household_nudge",
                "pending_action_target_id": "nudge_126",
            },
            created_at=datetime.now(timezone.utc).timestamp(),
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
                message_id="msg_210",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="finished",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text == "Tell me which reminder you want to update."
    updated_nudge = store.get_household_nudge("nudge_126")
    assert updated_nudge is not None
    assert updated_nudge.status == HouseholdNudgeStatus.SENT
    assert store.list_pilot_events(household_id="hh_123", event_type="reminder_done") == []
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
