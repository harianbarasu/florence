"""Hermes-backed household chat orchestration for Florence."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

from florence.contracts import ChannelMessage, ChannelMessageRole, ChannelType, HouseholdProfileKind
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
    ):
        self.store = store
        self.model = model
        self.max_iterations = max_iterations
        self.provider = provider.strip() if isinstance(provider, str) and provider.strip() else "auto"
        self.enabled_toolsets = list(enabled_toolsets) if enabled_toolsets is not None else ["florence_chat"]
        self.disabled_toolsets = list(disabled_toolsets or [])
        self.agent_factory = agent_factory

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

        agent_factory = self.agent_factory
        if agent_factory is None:
            from run_agent import AIAgent

            agent_factory = AIAgent

        agent = agent_factory(
            model=self.model,
            max_iterations=self.max_iterations,
            provider=self.provider,
            enabled_toolsets=self.enabled_toolsets,
            disabled_toolsets=self.disabled_toolsets or None,
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
        if not final_response:
            logger.warning(
                "Florence household chat produced an empty final_response for household_id=%s channel_id=%s",
                household_id,
                channel_id,
            )
            return None
        return FlorenceHouseholdChatReply(text=final_response)

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

        actor_name = None
        if actor_member_id:
            member = self.store.get_member(actor_member_id)
            if member is not None:
                actor_name = member.display_name

        members = self.store.list_members(household_id)
        events = self.store.list_household_events(household_id=household_id)
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

        lines.append("Use the household state below as authoritative context, then use Hermes tools when they help.")
        return "\n".join(lines)
