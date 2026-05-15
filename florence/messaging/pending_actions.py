"""Helpers for reading the latest assistant pending action in a channel."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from florence.contracts import ChannelMessage
from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.protocol_types import (
    PendingAction,
    pending_action_from_metadata,
    pending_action_is_expired,
)


@dataclass(frozen=True, slots=True)
class ActivePendingAction:
    message: ChannelMessage
    action: PendingAction
    metadata: dict[str, object]

    @property
    def target_ids(self) -> tuple[str, ...]:
        if self.action.target_ids:
            return self.action.target_ids
        return (self.action.target_id,) if self.action.target_id else ()


def latest_assistant_protocol_message(
    channel_log: FlorenceChannelLog,
    *,
    channel_id: str,
    protocol_kind: str | None = None,
    limit: int = 8,
) -> ChannelMessage | None:
    for message in reversed(channel_log.recent_messages(channel_id=channel_id, limit=limit)):
        sender_role = getattr(message, "sender_role", None)
        role_value = getattr(sender_role, "value", sender_role)
        if role_value is not None and str(role_value) != "assistant":
            continue
        metadata = message.metadata if isinstance(message.metadata, dict) else {}
        if protocol_kind is not None and metadata.get("protocol_kind") != protocol_kind:
            continue
        return message
    return None


def latest_pending_action(
    channel_log: FlorenceChannelLog,
    *,
    channel_id: str,
    protocol_kind: str | None = None,
    action_type: str | None = None,
    target_kind: str | None = None,
    role: str | None = None,
    limit: int = 8,
    now: datetime | None = None,
    include_expired: bool = False,
) -> ActivePendingAction | None:
    message = latest_assistant_protocol_message(
        channel_log,
        channel_id=channel_id,
        protocol_kind=protocol_kind,
        limit=limit,
    )
    if message is None:
        return None
    metadata = message.metadata if isinstance(message.metadata, dict) else {}
    action = pending_action_from_metadata(
        metadata,
        message_id=message.id,
        message_body=message.body,
    )
    if action is None:
        return None
    if pending_action_is_expired(action, now=now) and not include_expired:
        return None
    if action_type is not None and action.action_type != action_type:
        return None
    if target_kind is not None and action.target_kind != target_kind:
        return None
    if role is not None and action.role != role:
        return None
    return ActivePendingAction(message=message, action=action, metadata=dict(metadata))


def active_pending_target_ids(active: ActivePendingAction | None) -> list[str]:
    if active is None:
        return []
    return [target_id for target_id in active.target_ids if str(target_id).strip()]
