"""Channel message persistence and history helpers for Florence messaging."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import time

from florence.contracts import ChannelMessage, ChannelMessageRole
from florence.messaging.types import FlorenceInboundMessage
from florence.state import FlorenceStateDB


@dataclass(slots=True)
class _PromotedGroupMessage:
    group_announcement: str
    already_promoted: bool = False


def _stable_transport_message_id(provider: str, message_id: str) -> str:
    digest = hashlib.sha256(f"{provider}:{message_id}".encode("utf-8")).hexdigest()[:20]
    return f"chatmsg_{digest}"


def _assistant_message_id(channel_id: str, body: str) -> str:
    digest = hashlib.sha256(f"{channel_id}:{body}:{time.time_ns()}".encode("utf-8")).hexdigest()[:20]
    return f"assistant_{digest}"


class FlorenceChannelLog:
    """Persist and load channel message history for ingress and chat."""

    def __init__(self, store: FlorenceStateDB) -> None:
        self.store = store

    def has_inbound_message(self, *, provider: str, message_id: str) -> bool:
        return self.store.get_channel_message(_stable_transport_message_id(provider, message_id)) is not None

    def append_inbound_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        member_id: str | None,
        thread_id: str,
        message: FlorenceInboundMessage,
    ) -> None:
        body = message.body.strip()
        if not body:
            return
        self.store.append_channel_message(
            ChannelMessage(
                id=_stable_transport_message_id(message.provider, message.message_id),
                household_id=household_id,
                channel_id=channel_id,
                sender_role=ChannelMessageRole.USER,
                sender_member_id=member_id,
                body=body,
                metadata={
                    "provider": message.provider,
                    "event_type": message.event_type,
                    "transport_thread_id": thread_id,
                    "transport_message_id": message.message_id,
                    "reply_to_message_id": message.reply_to_message_id,
                    "sent_at": message.sent_at,
                    **message.metadata,
                },
                created_at=time.time(),
            )
        )

    def append_assistant_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        body: str,
        metadata: dict[str, object] | None = None,
    ) -> ChannelMessage:
        return self.store.append_channel_message(
            ChannelMessage(
                id=_assistant_message_id(channel_id, body),
                household_id=household_id,
                channel_id=channel_id,
                sender_role=ChannelMessageRole.ASSISTANT,
                body=body,
                metadata=metadata or {},
                created_at=time.time(),
            )
        )

    def append_transport_reply_messages(
        self,
        *,
        household_id: str,
        channel_id: str,
        provider: str,
        thread_id: str,
        reply_to_message_id: str,
        messages: tuple[str, ...],
        metadata: dict[str, object] | None = None,
    ) -> None:
        for body in messages:
            self.append_assistant_message(
                household_id=household_id,
                channel_id=channel_id,
                body=body,
                metadata={
                    "provider": provider,
                    "transport_thread_id": thread_id,
                    "transport_reply_to": reply_to_message_id,
                    **(metadata or {}),
                },
            )

    def conversation_history(
        self,
        *,
        channel_id: str,
        limit: int = 24,
    ) -> list[ChannelMessage] | None:
        history = self.store.list_channel_messages(channel_id=channel_id, limit=limit)
        return history[:-1] if history else None

    def recent_messages(
        self,
        *,
        channel_id: str,
        limit: int,
    ) -> list[ChannelMessage]:
        return self.store.list_channel_messages(channel_id=channel_id, limit=limit)

    def latest_assistant_message(
        self,
        *,
        channel_id: str,
        limit: int = 8,
    ) -> ChannelMessage | None:
        history = self.recent_messages(channel_id=channel_id, limit=limit)
        return next(
            (
                message
                for message in reversed(history)
                if message.sender_role == ChannelMessageRole.ASSISTANT
            ),
            None,
        )

    def latest_assistant_message_body(
        self,
        *,
        channel_id: str,
        limit: int = 8,
    ) -> str | None:
        latest = self.latest_assistant_message(channel_id=channel_id, limit=limit)
        if latest is None:
            return None
        body = latest.body.strip()
        return body or None

    def latest_promotable_group_message(
        self,
        *,
        channel_id: str,
        limit: int = 12,
    ) -> ChannelMessage | None:
        history = self.recent_messages(channel_id=channel_id, limit=limit)
        for message in reversed(history):
            if message.sender_role != ChannelMessageRole.ASSISTANT:
                continue
            promotable = " ".join(str(message.metadata.get("promotable_group_message") or "").split()).strip()
            if promotable:
                return message
        return None

    def promote_latest_group_message(
        self,
        *,
        channel_id: str,
        promoted_group_channel_id: str,
        limit: int = 12,
    ) -> _PromotedGroupMessage | None:
        promotable_message = self.latest_promotable_group_message(channel_id=channel_id, limit=limit)
        if promotable_message is None:
            return None
        promotable_group_message = str(promotable_message.metadata.get("promotable_group_message") or "").strip()
        if not promotable_group_message:
            return None
        if promotable_message.metadata.get("promoted_to_group_at"):
            return _PromotedGroupMessage(
                group_announcement=promotable_group_message,
                already_promoted=True,
            )
        self.store.append_channel_message(
            replace(
                promotable_message,
                metadata={
                    **dict(promotable_message.metadata),
                    "promoted_to_group_at": datetime.now(timezone.utc).isoformat(),
                    "promoted_group_channel_id": promoted_group_channel_id,
                },
            )
        )
        return _PromotedGroupMessage(group_announcement=promotable_group_message)

    def recent_exchange_for_group_promotion(
        self,
        *,
        channel_id: str,
        current_provider: str,
        current_message_id: str,
        limit: int = 8,
    ) -> str | None:
        current_inbound_id = _stable_transport_message_id(current_provider, current_message_id)
        history = [
            message
            for message in self.recent_messages(channel_id=channel_id, limit=limit)
            if message.id != current_inbound_id
        ]
        if not history:
            return None
        rendered: list[str] = []
        for message in history[-4:]:
            speaker = "Florence" if message.sender_role == ChannelMessageRole.ASSISTANT else "Parent"
            body = " ".join(message.body.split()).strip()
            if body:
                rendered.append(f"{speaker}: {body}")
        if not rendered:
            return None
        return "\n".join(rendered)
