"""Shared types for Florence messaging ingress and route handlers."""

from __future__ import annotations

from dataclasses import dataclass, field

from florence.messaging.types import FlorenceInboundMessage


@dataclass(slots=True)
class FlorenceResolvedInboundMessage:
    household_id: str
    member_id: str | None
    channel_id: str
    thread_id: str
    message: FlorenceInboundMessage

    @property
    def is_group(self) -> bool:
        return self.message.is_group_chat


@dataclass(slots=True)
class FlorenceMessagingIngressResult:
    reply_text: str | None = None
    reply_messages: tuple[str, ...] = field(default_factory=tuple)
    reply_metadata: dict[str, object] = field(default_factory=dict)
    group_announcement: str | None = None
    consumed: bool = False
