"""Hermes-backed household chat orchestration for Florence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from florence.contracts import AppChatMessage, AppChatMessageRole
from florence.state import FlorenceStateDB


@dataclass(slots=True)
class FlorenceHouseholdChatReply:
    text: str


class FlorenceHouseholdChatService:
    """Wrap Hermes core for household group chat after Florence onboarding."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        model: str,
        max_iterations: int = 6,
        agent_factory: Callable[..., Any] | None = None,
    ):
        self.store = store
        self.model = model
        self.max_iterations = max_iterations
        self.agent_factory = agent_factory

    def respond(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        message_text: str,
        conversation_history: list[AppChatMessage] | None = None,
    ) -> FlorenceHouseholdChatReply | None:
        system_message = self._build_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not system_message:
            return None

        agent_factory = self.agent_factory
        if agent_factory is None:
            from run_agent import AIAgent

            agent_factory = AIAgent

        agent = agent_factory(
            model=self.model,
            max_iterations=self.max_iterations,
            enabled_toolsets=["florence_chat"],
            quiet_mode=True,
            skip_memory=True,
            platform="florence",
        )
        history = self._build_conversation_history(conversation_history or [])
        result = agent.run_conversation(
            user_message=message_text,
            system_message=system_message,
            conversation_history=history,
        )
        final_response = str(result.get("final_response") or "").strip()
        return FlorenceHouseholdChatReply(text=final_response) if final_response else None

    @staticmethod
    def _build_conversation_history(messages: list[AppChatMessage]) -> list[dict[str, str]]:
        history: list[dict[str, str]] = []
        for message in messages:
            if not message.body.strip():
                continue
            if message.sender_role == AppChatMessageRole.USER:
                history.append({"role": "user", "content": message.body})
            elif message.sender_role == AppChatMessageRole.ASSISTANT:
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

        actor_name = None
        if actor_member_id:
            member = self.store.get_member(actor_member_id)
            if member is not None:
                actor_name = member.display_name

        members = self.store.list_members(household_id)
        onboarding_sessions = self.store.list_onboarding_sessions(household_id)
        events = self.store.list_household_events(household_id=household_id)

        child_names: list[str] = []
        school_labels: list[str] = []
        activity_labels: list[str] = []
        seen_children: set[str] = set()
        seen_schools: set[str] = set()
        seen_activities: set[str] = set()
        for session in onboarding_sessions:
            for child_name in session.child_names:
                normalized = child_name.strip()
                if normalized and normalized not in seen_children:
                    seen_children.add(normalized)
                    child_names.append(normalized)
            for school in session.school_labels:
                normalized = school.strip()
                if normalized and normalized not in seen_schools:
                    seen_schools.add(normalized)
                    school_labels.append(normalized)
            for activity in session.activity_labels:
                normalized = activity.strip()
                if normalized and normalized not in seen_activities:
                    seen_activities.add(normalized)
                    activity_labels.append(normalized)

        lines = [
            "You are Florence, a household chief-of-staff assistant for a family group chat.",
            "You are running on Hermes core, but the backend household state is the source of truth.",
            "Never claim an imported Gmail or Google Calendar item is confirmed unless it is already present in confirmed household state below.",
            "If household information is missing or ambiguous, ask a short follow-up question.",
            "Keep replies concise and practical. Do not mention internal policy or hidden review queues unless asked directly.",
            f"Household: {household.name}",
            f"Timezone: {household.timezone}",
            f"Channel ID: {channel_id}",
        ]
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

        lines.append(
            "You may answer questions about the household plan, summarize what is happening, and help with household coordination."
        )
        return "\n".join(lines)
