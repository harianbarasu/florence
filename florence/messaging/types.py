"""Transport-normalized message types for Florence."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class FlorenceInboundMessage:
    provider: str
    message_id: str
    thread_id: str
    sender_handle: str
    body: str
    is_group_chat: bool
    is_from_me: bool = False
    event_type: str | None = None
    participant_handles: tuple[str, ...] = ()
    reply_to_message_id: str | None = None
    sent_at: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)
