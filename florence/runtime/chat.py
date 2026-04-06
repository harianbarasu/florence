"""Hermes-backed household chat orchestration for Florence."""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, replace
from typing import Any, Callable

from florence.contracts import (
    ChannelMessage,
    ChannelMessageRole,
    ChannelType,
    HouseholdBriefingKind,
    HouseholdMealStatus,
    HouseholdNudgeStatus,
    HouseholdProfileKind,
    HouseholdRoutineStatus,
    HouseholdShoppingItemStatus,
    HouseholdWorkItemStatus,
)
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FlorenceHouseholdChatReply:
    text: str


@dataclass(slots=True)
class FlorenceHouseholdActivationBrief:
    text: str
    group_text: str | None = None


class FlorenceHouseholdChatService:
    """Wrap Hermes core for Florence household chat after onboarding."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        model: str,
        max_iterations: int = 6,
        provider: str = "auto",
        enabled_toolsets: list[str] | tuple[str, ...] | None = None,
        disabled_toolsets: list[str] | tuple[str, ...] | None = None,
        fallback_model: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
        tool_use_enforcement: str | bool | list[str] | tuple[str, ...] = "auto",
        enable_honcho: bool = True,
        honcho_scope: str = "member",
        agent_factory: Callable[..., Any] | None = None,
        session_db: Any | None = None,
    ):
        self.store = store
        self.model = model
        self.max_iterations = max_iterations
        self.provider = provider.strip() if isinstance(provider, str) and provider.strip() else "auto"
        self.enabled_toolsets = list(enabled_toolsets) if enabled_toolsets is not None else ["florence_chat"]
        self.disabled_toolsets = list(disabled_toolsets or [])
        self.fallback_model = [dict(item) for item in (fallback_model or ()) if isinstance(item, dict)]
        self.tool_use_enforcement = list(tool_use_enforcement) if isinstance(tool_use_enforcement, tuple) else tool_use_enforcement
        self.enable_honcho = bool(enable_honcho)
        self.honcho_scope = str(honcho_scope or "member").strip().lower() or "member"
        self.agent_factory = agent_factory
        self.session_db = session_db or self._build_session_db()

    def respond(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        message_text: str,
        conversation_history: list[ChannelMessage] | None = None,
    ) -> FlorenceHouseholdChatReply | None:
        system_message = self._build_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not system_message:
            return None
        history, session_id = self._load_conversation_history(
            channel_id=channel_id,
            fallback_messages=conversation_history or [],
        )
        result = self._run_agent_conversation(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=message_text,
            system_message=system_message,
            conversation_history=history,
            session_id=session_id,
        )
        final_response = str(result.get("final_response") or "").strip()
        if not final_response:
            logger.warning(
                "Florence household chat produced an empty final_response for household_id=%s channel_id=%s",
                household_id,
                channel_id,
            )
            return None
        return FlorenceHouseholdChatReply(text=final_response)

    def handle_capture_request(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        message_text: str,
        capture_kind: str,
        conversation_history: list[ChannelMessage] | None = None,
    ) -> FlorenceHouseholdChatReply | None:
        system_message = self._build_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not system_message:
            return None
        capture_label = " ".join(str(capture_kind or "capture").split()).strip().lower() or "capture"
        system_message = "\n".join(
            [
                system_message,
                "You are handling a capture -> handled request.",
                "Before creating duplicates, look at household state, prior Florence context, and connected inbox context when they help.",
                "Default to turning the input into durable household state when the user is asking Florence to remember, plan, track, or manage something.",
                "For meal and grocery requests, prefer creating or updating household meals and shopping items instead of leaving the plan only in chat.",
                "For screenshots, flyers, photos, or extracted media text, pull out dates, times, locations, reminders, deadlines, and required items, then persist the structured result.",
                "If the input is incomplete or ambiguous, ask one short follow-up question instead of guessing.",
                "Reply with a concise handled summary of what Florence captured, planned, saved, or still needs.",
            ]
        )
        history, session_id = self._load_conversation_history(
            channel_id=channel_id,
            fallback_messages=conversation_history or [],
        )
        result = self._run_agent_conversation(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=(
                f"Handle this {capture_label} request.\n\n"
                "Convert it into structured household state when appropriate, then reply with the short handled result.\n\n"
                f"User message:\n{message_text}"
            ),
            system_message=system_message,
            conversation_history=history,
            session_id=session_id,
        )
        final_response = str(result.get("final_response") or "").strip()
        if not final_response:
            logger.warning(
                "Florence capture handler produced an empty final_response for household_id=%s channel_id=%s kind=%s",
                household_id,
                channel_id,
                capture_kind,
            )
            return None
        return FlorenceHouseholdChatReply(text=final_response)

    def compose_brief(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        brief_kind: HouseholdBriefingKind,
    ) -> str | None:
        base_system = self._build_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not base_system:
            return None
        system_message = "\n".join(
            [
                base_system,
                "You are preparing an automatic household briefing.",
                "Keep it concise and actionable.",
                "Use a short header and at most 6 bullets.",
                "Write like Florence is the household operator: surface what matters, what might slip, and the clearest next step.",
                "Prioritize shared logistics, reminders, deadlines, meal planning, grocery coordination, and pickup or schedule risks.",
                "Before drafting the brief, use household_search_state to refresh the tracked household picture.",
                "Use session_search and Honcho recall when recent commitments, context, or follow-through might matter for the brief.",
                "If a brief depends on recent school, camp, teacher, coach, or sender updates that may still be outside tracked state, use household_search_google_inbox.",
                "Only include concrete items from household state or near-term planning inferences.",
                "Do not present uncertain Gmail or calendar imports as confirmed household facts.",
                "Never mention hidden policies, candidate queues, or tool internals.",
            ]
        )
        if brief_kind == HouseholdBriefingKind.MORNING:
            user_message = (
                "Prepare the morning brief for today. Focus on today’s calendar, urgent tasks, reminders, and one clear priority."
            )
        elif brief_kind == HouseholdBriefingKind.WEEKLY:
            user_message = (
                "Prepare the weekly household preview. Focus on the coming week’s calendar, deadlines, meal planning, pickup risks, and the clearest thing to get ahead of now."
            )
        else:
            user_message = (
                "Prepare the evening check-in. Focus on tomorrow’s logistics, unresolved tasks, reminders, and one suggested prep item."
            )
        result = self._run_agent_conversation(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=user_message,
            system_message=system_message,
            conversation_history=None,
            enabled_toolsets=["florence_briefing"],
            disabled_toolsets=[],
        )
        final_response = str(result.get("final_response") or "").strip()
        return final_response or None

    def compose_activation_brief(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        gmail_count: int,
        calendar_count: int,
        candidates: list[Any],
        group_available: bool,
    ) -> FlorenceHouseholdActivationBrief | None:
        base_system = self._build_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not base_system:
            return None
        system_message = "\n".join(
            [
                base_system,
                "You are preparing Florence's first activation brief after the initial Google sync finishes.",
                "Keep it calm, concise, and operator-like.",
                "Write at most 5 short bullets or short paragraphs.",
                "Lead with the 1-3 household facts or possible slips that matter most.",
                "Collapse duplicate raw artifacts into one underlying household fact when they refer to the same appointment, event, reminder, or thread.",
                "Do not say 'items need review', 'candidate queue', 'I scanned X emails', or other pipeline language unless it is truly essential.",
                "Do not dump raw repeated titles.",
                "If something looks important but still uncertain, phrase it as something Florence wants to double-check, not as a confirmed fact.",
                "Do not mention tool internals, sync phases, or ingestion mechanics.",
                "End with one short natural invitation for what the parent can ask next. Do not use command-style product UI language.",
            ]
        )
        user_message = json.dumps(
            {
                "task": "compose_initial_sync_activation_brief",
                "gmail_count": gmail_count,
                "calendar_count": calendar_count,
                "group_available": group_available,
                "candidates": [
                    {
                        "title": str(getattr(candidate, "title", "") or "").strip(),
                        "summary": str(getattr(candidate, "summary", "") or "").strip(),
                        "state": str(getattr(candidate, "state", "") or "").strip(),
                        "confirmation_question": str(getattr(candidate, "metadata", {}).get("confirmation_question") or "").strip(),
                    }
                    for candidate in candidates
                ],
            },
            ensure_ascii=True,
        )
        result = self._run_agent_conversation(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=user_message,
            system_message=system_message,
            conversation_history=None,
            enabled_toolsets=["florence_briefing"],
            disabled_toolsets=[],
        )
        final_response = str(result.get("final_response") or "").strip()
        if not final_response:
            return None
        group_text = None
        if group_available:
            group_text = self.compose_group_promotion(
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=actor_member_id,
                source_text=final_response,
            )
        return FlorenceHouseholdActivationBrief(text=final_response, group_text=group_text)

    def compose_review_prompt(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        candidate: Any,
        source_prompt: str | None = None,
    ) -> str | None:
        base_system = self._build_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not base_system:
            return None
        system_message = "\n".join(
            [
                base_system,
                "You are preparing a short Florence review prompt for one possible household item.",
                "Keep it calm, concise, and plainspoken.",
                "Do not say 'Imported item', 'candidate', 'queue', or other pipeline language.",
                "Summarize the underlying household fact or question in normal language.",
                "If the item is uncertain, frame it as something Florence wants to double-check before adding.",
                "If there is source-sharing guidance, include it naturally in one short line.",
                "End exactly with: Reply yes if I should add it, no if it's wrong, or skip for later.",
            ]
        )
        user_message = json.dumps(
            {
                "task": "compose_review_prompt",
                "candidate": {
                    "title": str(getattr(candidate, "title", "") or "").strip(),
                    "summary": str(getattr(candidate, "summary", "") or "").strip(),
                    "state": str(getattr(candidate, "state", "") or "").strip(),
                    "confirmation_question": str(getattr(candidate, "metadata", {}).get("confirmation_question") or "").strip(),
                },
                "source_prompt": str(source_prompt or "").strip(),
            },
            ensure_ascii=True,
        )
        result = self._run_agent_conversation(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=user_message,
            system_message=system_message,
            conversation_history=None,
            enabled_toolsets=["florence_briefing"],
            disabled_toolsets=[],
        )
        final_response = str(result.get("final_response") or "").strip()
        return final_response or None

    def compose_sync_waiting_reply(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        user_message: str | None = None,
        conversation_history: list[ChannelMessage] | None = None,
        data_dependent: bool = False,
        just_connected: bool = False,
    ) -> str | None:
        base_system = self._build_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not base_system:
            return None
        system_message = "\n".join(
            [
                base_system,
                "You are writing a short Florence message while the first Gmail and Calendar sync is still running.",
                "Keep it calm, concise, and reassuring.",
                "Do not replay the active onboarding question unless the user explicitly asks for it.",
                "Do not mention candidate queues, scan counts, pipeline phases, or tool internals.",
                "Do not sound like a background job log or admin console.",
                "Keep it to one short text-sized reply.",
            ]
        )
        if just_connected:
            system_message = "\n".join(
                [
                    system_message,
                    "Google just connected successfully. Florence is sending a quiet background-status update into the DM thread.",
                    "Acknowledge that sync is running in the background and say Florence will text when the first pass is ready.",
                ]
            )
            prompt_payload: dict[str, Any] = {
                "task": "compose_google_sync_started_update",
            }
        else:
            guidance = (
                "The user is asking for inbox or calendar-dependent information, so explain that Florence is still syncing before it can answer confidently from that data."
                if data_dependent
                else "The user mainly wants sync status or wants to know what Florence can do while the sync runs."
            )
            system_message = "\n".join([system_message, guidance])
            prompt_payload = {
                "task": "compose_sync_waiting_reply",
                "user_message": str(user_message or "").strip(),
                "data_dependent": bool(data_dependent),
            }
        result = self._run_agent_conversation(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=json.dumps(prompt_payload, ensure_ascii=True),
            system_message=system_message,
            conversation_history=(
                self._build_conversation_history(conversation_history or [])
                if conversation_history is not None
                else None
            ),
            enabled_toolsets=["florence_briefing"],
            disabled_toolsets=[],
        )
        final_response = str(result.get("final_response") or "").strip()
        return final_response or None

    def compose_group_promotion(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        source_text: str,
    ) -> str | None:
        base_system = self._build_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not base_system:
            return None
        system_message = "\n".join(
            [
                base_system,
                "You are preparing a short group-safe household update from a private parent DM.",
                "Only include concrete shared logistics, tasks, reminders, meals, grocery items, calendar changes, or plans the other parent should know.",
                "Do not include raw feelings, therapy-like language, health-sensitive details, vulnerable wording, or anything that belongs only in a private DM.",
                "If there is nothing appropriate to share with the parent group, reply exactly NO_GROUP_SHARE.",
                "Keep the result concise and ready to send as a standalone household update.",
            ]
        )
        result = self._run_agent_conversation(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=(
                "Turn this recent private DM exchange into a short parent-group update if appropriate:\n\n"
                f"{source_text}"
            ),
            system_message=system_message,
            conversation_history=None,
            enabled_toolsets=["florence_briefing"],
            disabled_toolsets=[],
        )
        final_response = str(result.get("final_response") or "").strip()
        if not final_response or final_response == "NO_GROUP_SHARE":
            return None
        return final_response

    def _run_agent_conversation(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        user_message: str,
        system_message: str,
        conversation_history: list[dict[str, str]] | None,
        session_id: str | None = None,
        enabled_toolsets: list[str] | None = None,
        disabled_toolsets: list[str] | None = None,
    ) -> dict[str, Any]:
        task_id = f"florence-household-{uuid.uuid4()}"

        agent_factory = self.agent_factory
        if agent_factory is None:
            from run_agent import AIAgent

            agent_factory = AIAgent
        from tools.florence_household_tool import (
            clear_household_tool_context,
            set_household_tool_context,
        )
        set_household_tool_context(
            task_id,
            store=self.store,
            household_id=household_id,
            actor_member_id=actor_member_id,
            channel_id=channel_id,
        )
        try:
            agent = agent_factory(
                model=self.model,
                max_iterations=self.max_iterations,
                provider=self.provider,
                enabled_toolsets=enabled_toolsets if enabled_toolsets is not None else self.enabled_toolsets,
                disabled_toolsets=(
                    disabled_toolsets
                    if disabled_toolsets is not None
                    else (self.disabled_toolsets or None)
                ),
                quiet_mode=True,
                skip_memory=not self.enable_honcho,
                skip_local_memory=True,
                platform="florence",
                session_id=session_id,
                session_db=self.session_db,
                honcho_session_key=(
                    self._build_honcho_session_key(
                        household_id=household_id,
                        channel_id=channel_id,
                        actor_member_id=actor_member_id,
                    )
                    if self.enable_honcho
                    else None
                ),
                fallback_model=self.fallback_model or None,
                tool_use_enforcement=self.tool_use_enforcement,
                session_search_kwargs=self._build_session_search_kwargs(
                    household_id=household_id,
                    channel_id=channel_id,
                ),
            )
            result = agent.run_conversation(
                user_message=user_message,
                system_message=system_message,
                conversation_history=conversation_history,
                task_id=task_id,
            )
            self._persist_channel_session_id(
                channel_id=channel_id,
                session_id=str(getattr(agent, "session_id", "") or "").strip(),
            )
            return result
        finally:
            clear_household_tool_context(task_id)

    @staticmethod
    def _build_session_db() -> Any | None:
        try:
            from hermes_state import SessionDB

            return SessionDB()
        except Exception:
            return None

    def _default_session_id(self, channel_id: str) -> str:
        return f"florence-channel-{channel_id}"

    def _current_channel_session_id(self, channel_id: str) -> str:
        channel = self.store.get_channel(channel_id)
        if channel is None:
            return self._default_session_id(channel_id)
        metadata = dict(channel.metadata) if isinstance(channel.metadata, dict) else {}
        stored = str(metadata.get("hermes_session_id") or "").strip()
        return stored or self._default_session_id(channel_id)

    def _resolve_session_root(self, session_id: str | None) -> str | None:
        cleaned = str(session_id or "").strip()
        if not cleaned:
            return None
        if self.session_db is None:
            return cleaned
        visited: set[str] = set()
        resolved = cleaned
        current = cleaned
        while current and current not in visited:
            visited.add(current)
            resolved = current
            try:
                session = self.session_db.get_session(current)
            except Exception:
                logger.debug("Florence SessionDB lineage lookup failed for session_id=%s", current, exc_info=True)
                break
            if not session:
                break
            parent = str(session.get("parent_session_id") or "").strip()
            if not parent:
                break
            current = parent
        return resolved

    def _build_session_search_kwargs(self, *, household_id: str, channel_id: str) -> dict[str, Any]:
        allowed_session_ids: set[str] = set()
        for channel in self.store.list_channels(household_id=household_id):
            root = self._resolve_session_root(self._current_channel_session_id(channel.id))
            if root:
                allowed_session_ids.add(root)
        if not allowed_session_ids:
            root = self._resolve_session_root(self._current_channel_session_id(channel_id))
            if root:
                allowed_session_ids.add(root)
        return {
            "source_filter": ["florence"],
            "allowed_session_ids": sorted(allowed_session_ids),
        }

    def _build_honcho_session_key(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
    ) -> str:
        channel = self.store.get_channel(channel_id)
        channel_type = channel.channel_type if channel is not None else None
        scope = self.honcho_scope
        if scope == "household":
            return f"florence:household:{household_id}"
        if scope == "channel":
            return f"florence:channel:{household_id}:{channel_id}"
        if channel_type == ChannelType.HOUSEHOLD_GROUP:
            return f"florence:household:{household_id}"
        if actor_member_id:
            return f"florence:member:{household_id}:{actor_member_id}"
        return f"florence:channel:{household_id}:{channel_id}"

    def _persist_channel_session_id(self, *, channel_id: str, session_id: str | None) -> None:
        cleaned = str(session_id or "").strip()
        if not cleaned:
            return
        channel = self.store.get_channel(channel_id)
        if channel is None:
            return
        metadata = dict(channel.metadata) if isinstance(channel.metadata, dict) else {}
        if str(metadata.get("hermes_session_id") or "").strip() == cleaned:
            return
        metadata["hermes_session_id"] = cleaned
        self.store.upsert_channel(replace(channel, metadata=metadata))

    def _load_conversation_history(
        self,
        *,
        channel_id: str,
        fallback_messages: list[ChannelMessage],
    ) -> tuple[list[dict[str, str]], str]:
        session_id = self._current_channel_session_id(channel_id)
        if self.session_db is not None:
            try:
                transcript = self.session_db.get_messages_as_conversation(session_id)
                if transcript:
                    return transcript, session_id
            except Exception:
                logger.debug("Florence SessionDB load failed for channel_id=%s", channel_id, exc_info=True)
        return self._build_conversation_history(fallback_messages), session_id

    @staticmethod
    def _build_conversation_history(messages: list[ChannelMessage]) -> list[dict[str, str]]:
        history: list[dict[str, str]] = []
        for message in messages:
            if not message.body.strip():
                continue
            if message.sender_role == ChannelMessageRole.USER:
                history.append({"role": "user", "content": message.body})
            elif message.sender_role == ChannelMessageRole.ASSISTANT:
                history.append({"role": "assistant", "content": message.body})
        return history

    def _build_system_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
    ) -> str:
        household = self.store.get_household(household_id)
        if household is None:
            return ""
        channel = self.store.get_channel(channel_id)
        manager_profile = household.settings.get("manager_profile") if isinstance(household.settings, dict) else None
        manager_profile = dict(manager_profile) if isinstance(manager_profile, dict) else {}

        actor_name = None
        if actor_member_id:
            member = self.store.get_member(actor_member_id)
            if member is not None:
                actor_name = member.display_name

        members = self.store.list_members(household_id)
        events = self.store.list_household_events(household_id=household_id)
        work_items = [
            item
            for item in self.store.list_household_work_items(household_id=household_id)
            if item.status in {
                HouseholdWorkItemStatus.OPEN,
                HouseholdWorkItemStatus.IN_PROGRESS,
                HouseholdWorkItemStatus.BLOCKED,
            }
        ]
        routines = self.store.list_household_routines(
            household_id=household_id,
            status=HouseholdRoutineStatus.ACTIVE,
        )
        nudges = [
            nudge
            for nudge in self.store.list_household_nudges(household_id=household_id)
            if nudge.status in {
                HouseholdNudgeStatus.SCHEDULED,
                HouseholdNudgeStatus.SENT,
            }
        ]
        meals = self.store.list_household_meals(
            household_id=household_id,
            status=HouseholdMealStatus.PLANNED,
        )
        shopping_items = self.store.list_household_shopping_items(
            household_id=household_id,
            list_name="groceries",
            status=HouseholdShoppingItemStatus.NEEDED,
        )
        preference_items = self.store.list_household_profile_items(
            household_id=household_id,
            kind=HouseholdProfileKind.PREFERENCE,
        )
        child_names = [child.full_name for child in self.store.list_child_profiles(household_id=household_id)]
        child_name_by_id = {
            child.id: child.full_name
            for child in self.store.list_child_profiles(household_id=household_id)
        }
        member_name_by_id = {
            member.id: member.display_name
            for member in members
        }
        school_labels = [
            item.label
            for item in self.store.list_household_profile_items(
                household_id=household_id,
                kind=HouseholdProfileKind.SCHOOL,
            )
        ]
        activity_labels = [
            item.label
            for item in self.store.list_household_profile_items(
                household_id=household_id,
                kind=HouseholdProfileKind.ACTIVITY,
            )
        ]

        lines = [
            "You are Florence, the Hermes-powered household agent for this iMessage thread.",
            "You are running on Hermes core, but the backend household state is the source of truth.",
            "You are a general household agent: help with planning, research, logistics, shopping, writing, reminders, and coordination when useful.",
            "Your core product loops are inbox -> plan, capture -> handled, and briefs -> stay ahead.",
            "Treat almost any household input as something you can structure and handle: school email, screenshots, flyers, photos, mental dumps, meals, groceries, reminders, and schedule questions.",
            "You have Hermes non-coding tools available for research, browsing websites, messaging, reminders, and media tasks.",
            "Your memory stack is: authoritative Florence household state, Florence session history, and Florence-scoped Honcho memory.",
            "You also have Florence household-state tools. Use them to persist durable household state when the user wants Florence to remember or manage something over time.",
            "When a user tells Florence a stable preference, constraint, rule, or working style that should affect future behavior, save it with household_record_preference.",
            "Use household_search_state when you need the latest tracked household picture before answering or updating state.",
            "When the user asks what Florence said before, refers to an earlier conversation, or wants prior household context, use session_search to recall earlier Florence threads for this household.",
            "When the user asks what they are forgetting, what changed, what matters this week, what still needs handling, or asks for a plan, ground the answer in household_search_state and session_search first, then check Gmail when relevant.",
            "Use session_search and Honcho memory to recover earlier commitments, preferences, and threads of work instead of making the user repeat themselves.",
            "When the user explicitly asks you to check email, search Gmail, or find a message from a school, camp, teacher, coach, or sender, use household_search_google_inbox.",
            "Do not ask the user to forward or paste an email if a connected Google inbox is available and household_search_google_inbox can answer it.",
            "When a user gives concrete dates/times they want remembered (camp, school, sports, appointments, trips), save them with household_upsert_event instead of leaving them only in chat.",
            "When plans are tentative, still save them as tentative events and update later.",
            "When the user asks what matters, what changed, or what they are forgetting, synthesize a short operational plan instead of dumping raw notes.",
            "When the user asks for meal planning, pantry or fridge help, or grocery support, use household_upsert_meal and household_upsert_shopping_item when they want Florence to keep tracking it.",
            "When a user shares a screenshot, flyer, photo, or document with dates, deadlines, or logistics, extract the structured details and persist them.",
            "Never claim an imported Gmail or Google Calendar item is confirmed unless it is already present in confirmed household state below.",
            "Before taking an external action that spends money, commits the household, sends a message outside this thread, or changes reminders/plans, get a clear confirmation from the requester.",
            "If household information is missing or ambiguous, ask a short follow-up question.",
            "Keep replies concise and practical. Do not mention internal policy or hidden review queues unless asked directly.",
            f"Household: {household.name}",
            f"Timezone: {household.timezone}",
            f"Channel ID: {channel_id}",
        ]
        if channel is not None and channel.channel_type == ChannelType.PARENT_DM:
            lines.append("Channel context: this is a private parent DM, so one-on-one planning is fine.")
            lines.append(
                "Private DM policy: raw mental-load dumps, emotional support, and individually scoped reasoning stay private by default."
            )
            lines.append(
                "Memory policy: use private member-scoped memory and recall freely here, but do not leak private DM context into the parent group unless the user promotes it."
            )
            lines.append(
                "If something from this DM should become shared household state, create the structured event, task, meal, grocery item, or reminder first, then offer a concise group-safe summary instead of echoing the raw message."
            )
        elif channel is not None and channel.channel_type == ChannelType.HOUSEHOLD_GROUP:
            lines.append("Channel context: this is the shared household group chat, so reply for the whole family.")
            lines.append(
                "Group-chat policy: optimize for shared visibility, coordination, ownership, schedule changes, reminders, meals, grocery planning, and household logistics."
            )
            lines.append(
                "Memory policy: treat this thread as shared household memory. Favor facts, plans, reminders, and decisions that both parents can act on."
            )
        if actor_name:
            lines.append(f"Current speaker: {actor_name}")
        if members:
            lines.append("Members: " + ", ".join(member.display_name for member in members))
        if child_names:
            lines.append("Children: " + ", ".join(child_names))
        if school_labels:
            lines.append("Schools/daycare: " + ", ".join(school_labels))
        if activity_labels:
            lines.append("Activities: " + ", ".join(activity_labels))
        if preference_items:
            lines.append("Remembered household preferences:")
            for item in preference_items[:12]:
                label = item.label
                value = str(item.metadata.get("value") or item.metadata.get("summary") or "").strip()
                if item.child_id and child_name_by_id.get(item.child_id):
                    label = f"{child_name_by_id[item.child_id]} | {label}"
                elif item.member_id and member_name_by_id.get(item.member_id):
                    label = f"{member_name_by_id[item.member_id]} | {label}"
                rendered = f"{label}: {value}" if value else label
                lines.append(f"- {rendered}")
        household_members = manager_profile.get("household_members")
        if isinstance(household_members, list) and household_members:
            lines.append("Family unit details: " + " | ".join(str(item) for item in household_members[:6]))
        child_details = manager_profile.get("child_details")
        if isinstance(child_details, list) and child_details:
            lines.append("Child notes: " + " | ".join(str(item) for item in child_details[:6]))
        household_operations = manager_profile.get("household_operations")
        if isinstance(household_operations, list) and household_operations:
            lines.append("Household operations to help manage: " + ", ".join(str(item) for item in household_operations[:10]))
        nudge_preferences = manager_profile.get("nudge_preferences_override") or manager_profile.get("nudge_preferences")
        if isinstance(nudge_preferences, str) and nudge_preferences.strip():
            lines.append(f"Reminder and nudge style: {nudge_preferences.strip()}")
        operating_preferences = manager_profile.get("operating_preferences")
        if isinstance(operating_preferences, str) and operating_preferences.strip():
            lines.append(f"Household operating policy: {operating_preferences.strip()}")

        if events:
            lines.append("Confirmed household events:")
            for event in events[:20]:
                label = event.title
                if event.starts_at:
                    label = f"{label} | starts {event.starts_at}"
                if event.ends_at:
                    label = f"{label} | ends {event.ends_at}"
                if event.location:
                    label = f"{label} | location {event.location}"
                if event.status.value != "confirmed":
                    label = f"{label} | status {event.status.value}"
                lines.append(f"- {label}")
        else:
            lines.append("Confirmed household events: none yet.")

        if work_items:
            lines.append("Open household work items:")
            for item in work_items[:12]:
                label = item.title
                if item.due_at:
                    label = f"{label} | due {item.due_at}"
                if item.status != HouseholdWorkItemStatus.OPEN:
                    label = f"{label} | status {item.status.value}"
                lines.append(f"- {label}")

        if routines:
            lines.append("Active household routines:")
            for routine in routines[:12]:
                label = f"{routine.title} | cadence {routine.cadence}"
                if routine.next_due_at:
                    label = f"{label} | next due {routine.next_due_at}"
                lines.append(f"- {label}")

        if nudges:
            lines.append("Pending household nudges:")
            for nudge in nudges[:12]:
                label = nudge.message
                if nudge.scheduled_for:
                    label = f"{label} | scheduled {nudge.scheduled_for}"
                if nudge.target_id:
                    label = f"{label} | target {nudge.target_kind.value}:{nudge.target_id}"
                lines.append(f"- {label}")

        if meals:
            lines.append("Upcoming meal plan:")
            for meal in meals[:12]:
                label = f"{meal.title} | {meal.meal_type} | {meal.scheduled_for}"
                lines.append(f"- {label}")

        if shopping_items:
            lines.append("Open grocery list:")
            for item in shopping_items[:20]:
                label = item.title
                if item.quantity:
                    label = f"{label} | qty {item.quantity}"
                if item.unit:
                    label = f"{label} {item.unit}"
                if item.needed_by:
                    label = f"{label} | needed by {item.needed_by}"
                lines.append(f"- {label}")

        lines.append("Use the household state below as authoritative context, then use Hermes tools when they help.")
        return "\n".join(lines)
