"""Transport-normalized message types for Florence."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


FLORENCE_MEDIA_ATTACHMENTS_METADATA_KEY = "_florence_media_attachments"


def _read_string(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


@dataclass(slots=True)
class FlorenceInboundAttachment:
    kind: str
    mime_type: str | None = None
    filename: str | None = None
    url: str | None = None
    data_url: str | None = None
    extracted_text: str | None = None


def parse_inbound_attachments(value: object) -> tuple[FlorenceInboundAttachment, ...]:
    if not isinstance(value, list):
        return ()
    attachments: list[FlorenceInboundAttachment] = []
    for raw_attachment in value:
        if not isinstance(raw_attachment, dict):
            continue
        kind = _read_string(raw_attachment.get("kind"))
        if not kind:
            continue
        attachments.append(
            FlorenceInboundAttachment(
                kind=kind,
                mime_type=_read_string(raw_attachment.get("mime_type")),
                filename=_read_string(raw_attachment.get("filename")),
                url=_read_string(raw_attachment.get("url")),
                data_url=_read_string(raw_attachment.get("data_url")),
                extracted_text=_read_string(raw_attachment.get("extracted_text")),
            )
        )
    return tuple(attachments)


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
    attachments: tuple[FlorenceInboundAttachment, ...] = field(default_factory=tuple)
