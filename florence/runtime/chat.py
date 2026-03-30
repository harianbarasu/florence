"""Hermes-backed household chat orchestration for Florence."""

from __future__ import annotations

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
        agent_factory: Callable[..., Any] | None = None,
        session_db: Any | None = None,
    ):
        self.store = store
        self.model = model
        self.max_iterations = max_iterations
        self.provider = provider.strip() if isinstance(provider, str) and provider.strip() else "auto"
        self.enabled_toolsets = list(enabled_toolsets) if enabled_toolsets is not None else ["florence_chat"]
        self.disabled_toolsets = list(disabled_toolsets or [])
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
                "Only include concrete items from household state or near-term planning inferences.",
                "Never mention hidden policies, candidate queues, or tool internals.",
            ]
        )
        user_message = (
            "Prepare the morning brief for today. Focus on today’s calendar, urgent tasks, reminders, and one clear priority."
            if brief_kind == HouseholdBriefingKind.MORNING
            else "Prepare the evening check-in. Focus on tomorrow’s logistics, unresolved tasks, reminders, and one suggested prep item."
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
                skip_memory=True,
                platform="florence",
                session_id=session_id,
                session_db=self.session_db,
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
        child_names = [child.full_name for child in self.store.list_child_profiles(household_id=household_id)]
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
            "You have Hermes non-coding tools available for research, browsing websites, messaging, reminders, and media tasks.",
            "You also have Florence household-state tools. Use them to persist durable household state when the user wants Florence to remember or manage something over time.",
            "When the user explicitly asks you to check email, search Gmail, or find a message from a school, camp, teacher, coach, or sender, use household_search_google_inbox.",
            "Do not ask the user to forward or paste an email if a connected Google inbox is available and household_search_google_inbox can answer it.",
            "When a user gives concrete dates/times they want remembered (camp, school, sports, appointments, trips), save them with household_upsert_event instead of leaving them only in chat.",
            "When plans are tentative, still save them as tentative events and update later.",
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
        elif channel is not None and channel.channel_type == ChannelType.HOUSEHOLD_GROUP:
            lines.append("Channel context: this is the shared household group chat, so reply for the whole family.")
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
