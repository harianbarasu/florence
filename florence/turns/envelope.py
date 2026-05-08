"""Canonical Florence turn envelope and outcome types."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from florence.contracts import ChannelMessage, ChannelType
from florence.messaging.ingress_types import FlorenceResolvedInboundMessage
from florence.messaging.types import FlorenceInboundAttachment
from florence.runtime.visibility import FlorenceConversationScope, resolve_conversation_scope
from florence.state import FlorenceStateDB


class FlorenceTurnTrigger(StrEnum):
    INBOUND_GROUP = "inbound_group"
    INBOUND_DM = "inbound_dm"
    SCHEDULED_BRIEF = "scheduled_brief"
    SCHEDULED_NUDGE = "scheduled_nudge"
    SYNC_BRIEF = "sync_brief"
    REVIEW = "review"
    ONBOARDING = "onboarding"
    SYSTEM = "system"


class FlorenceTurnDisposition(StrEnum):
    REPLY = "reply"
    REPLY_MULTIPLE = "reply_multiple"
    NO_REPLY = "no_reply"
    ASK_CLARIFICATION = "ask_clarification"
    PERSISTED_SILENTLY = "persisted_silently"
    GROUP_ANNOUNCEMENT = "group_announcement"
    DELIVERY_ONLY = "delivery_only"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class FlorenceToolScope:
    turn_id: str
    household_id: str
    actor_member_id: str | None
    channel_id: str
    channel_type: ChannelType | None
    visibility_scope: FlorenceConversationScope
    allowed_source_scopes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class FlorenceTurnEnvelope:
    turn_id: str
    trigger_kind: FlorenceTurnTrigger
    household_id: str
    actor_member_id: str | None
    channel_id: str
    channel_type: ChannelType | None
    visibility_scope: FlorenceConversationScope
    provider: str | None = None
    provider_thread_id: str | None = None
    provider_message_id: str | None = None
    message_text: str = ""
    attachments: tuple[FlorenceInboundAttachment, ...] = ()
    recent_history: tuple[ChannelMessage, ...] = ()
    received_at: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_group(self) -> bool:
        return self.channel_type == ChannelType.HOUSEHOLD_GROUP

    @property
    def is_private_dm(self) -> bool:
        return self.channel_type == ChannelType.PARENT_DM

    @property
    def tool_scope(self) -> FlorenceToolScope:
        return FlorenceToolScope(
            turn_id=self.turn_id,
            household_id=self.household_id,
            actor_member_id=self.actor_member_id,
            channel_id=self.channel_id,
            channel_type=self.channel_type,
            visibility_scope=self.visibility_scope,
            allowed_source_scopes=_allowed_source_scopes(self.visibility_scope),
        )


@dataclass(frozen=True, slots=True)
class FlorenceTurnOutcome:
    disposition: FlorenceTurnDisposition
    reply_messages: tuple[str, ...] = ()
    group_announcement: str | None = None
    state_change_ids: tuple[str, ...] = ()
    scheduled_work_ids: tuple[str, ...] = ()
    no_reply_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def consumed(self) -> bool:
        return self.disposition != FlorenceTurnDisposition.NO_REPLY or bool(
            self.reply_messages or self.group_announcement
        )


def build_inbound_turn_envelope(
    store: FlorenceStateDB,
    resolved: FlorenceResolvedInboundMessage,
    *,
    recent_history: tuple[ChannelMessage, ...] = (),
    turn_id: str | None = None,
) -> FlorenceTurnEnvelope:
    scope = resolve_conversation_scope(
        store,
        channel_id=resolved.channel_id,
        actor_member_id=resolved.member_id,
    )
    if scope.channel_type is None:
        inferred_channel_type = (
            ChannelType.HOUSEHOLD_GROUP if resolved.is_group else ChannelType.PARENT_DM
        )
        scope = FlorenceConversationScope(
            channel_type=inferred_channel_type,
            scope=(
                "shared_household_group"
                if inferred_channel_type == ChannelType.HOUSEHOLD_GROUP
                else "private_parent_dm"
            ),
            actor_member_id=resolved.member_id,
        )
    trigger_kind = (
        FlorenceTurnTrigger.INBOUND_GROUP
        if scope.channel_type == ChannelType.HOUSEHOLD_GROUP or resolved.is_group
        else FlorenceTurnTrigger.INBOUND_DM
    )
    return FlorenceTurnEnvelope(
        turn_id=turn_id or f"florence-turn-{uuid.uuid4()}",
        trigger_kind=trigger_kind,
        household_id=resolved.household_id,
        actor_member_id=resolved.member_id,
        channel_id=resolved.channel_id,
        channel_type=scope.channel_type,
        visibility_scope=scope,
        provider=resolved.message.provider,
        provider_thread_id=resolved.thread_id,
        provider_message_id=resolved.message.message_id,
        message_text=resolved.message.body,
        attachments=resolved.message.attachments,
        recent_history=recent_history,
        received_at=resolved.message.sent_at,
        metadata={
            "sender_handle": resolved.message.sender_handle,
            "is_group_chat": resolved.message.is_group_chat,
            "reply_to_message_id": resolved.message.reply_to_message_id,
            "event_type": resolved.message.event_type,
            "participant_handles": tuple(resolved.message.participant_handles),
            "transport_metadata": dict(resolved.message.metadata or {}),
        },
    )


def build_system_turn_envelope(
    store: FlorenceStateDB,
    *,
    trigger_kind: FlorenceTurnTrigger,
    household_id: str,
    channel_id: str,
    actor_member_id: str | None = None,
    message_text: str = "",
    metadata: dict[str, Any] | None = None,
    recent_history_limit: int = 24,
    turn_id: str | None = None,
) -> FlorenceTurnEnvelope:
    scope = resolve_conversation_scope(
        store,
        channel_id=channel_id,
        actor_member_id=actor_member_id,
    )
    channel = store.get_channel(channel_id)
    recent_history: tuple[ChannelMessage, ...] = ()
    if recent_history_limit > 0:
        recent_history = tuple(store.list_channel_messages(channel_id=channel_id, limit=recent_history_limit))
    return FlorenceTurnEnvelope(
        turn_id=turn_id or f"florence-turn-{uuid.uuid4()}",
        trigger_kind=trigger_kind,
        household_id=household_id,
        actor_member_id=actor_member_id,
        channel_id=channel_id,
        channel_type=scope.channel_type,
        visibility_scope=scope,
        provider=channel.provider if channel is not None else None,
        provider_thread_id=channel.provider_channel_id if channel is not None else None,
        message_text=message_text,
        recent_history=recent_history,
        metadata=dict(metadata or {}),
    )


def _allowed_source_scopes(scope: FlorenceConversationScope) -> tuple[str, ...]:
    if scope.is_shared_household_group:
        return ("shared_household", "shared_group_memory")
    if scope.is_private_parent_dm:
        return ("shared_household", "shared_group_memory", "private_parent")
    return ("system",)
