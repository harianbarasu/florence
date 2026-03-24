"""BlueBubbles webhook normalization for Florence."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _read_string(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _read_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _read_object(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _read_array(value: Any) -> list[Any] | None:
    return value if isinstance(value, list) else None


@dataclass(slots=True)
class BlueBubblesInboundMessage:
    source_message_id: str
    chat_guid: str
    body: str
    sender_handle: str
    is_from_me: bool
    event_type: str | None = None
    is_group_hint: bool | None = None
    participants: list[str] = field(default_factory=list)
    raw_payload: dict[str, Any] = field(default_factory=dict)

    @property
    def is_group_chat(self) -> bool:
        if len(self.participants) > 1:
            return True
        if self.is_group_hint is not None:
            return self.is_group_hint
        # BlueBubbles commonly uses ";-;" group GUIDs when explicit chat metadata is absent.
        return ";-;" in self.chat_guid


def _extract_chat_guid(chat: dict[str, Any] | None, message: dict[str, Any]) -> str | None:
    candidates = [
        chat.get("chatGuid") if chat else None,
        chat.get("guid") if chat else None,
        chat.get("identifier") if chat else None,
        chat.get("chatIdentifier") if chat else None,
        chat.get("chat_identifier") if chat else None,
        message.get("chatGuid"),
        message.get("guid"),
        message.get("chatGuid") if isinstance(message.get("chat"), dict) else None,
    ]
    for candidate in candidates:
        value = _read_string(candidate)
        if value:
            return value
    return None


def _extract_participants(chat: dict[str, Any] | None) -> list[str]:
    if not chat:
        return []
    raw = (
        _read_array(chat.get("participants"))
        or _read_array(chat.get("handles"))
        or _read_array(chat.get("participantHandles"))
        or []
    )
    participants: list[str] = []
    for entry in raw:
        if isinstance(entry, str) and entry.strip():
            participants.append(entry.strip())
            continue
        if isinstance(entry, dict):
            candidate = (
                _read_string(entry.get("address"))
                or _read_string(entry.get("handle"))
                or _read_string(entry.get("id"))
                or _read_string(entry.get("identifier"))
            )
            if candidate:
                participants.append(candidate)
    return participants


def _extract_group_hint(chat: dict[str, Any] | None, message: dict[str, Any], data: dict[str, Any]) -> bool | None:
    chat_like = (
        chat,
        _read_object(message.get("chat")),
        _read_object(data.get("chat")),
    )
    for candidate in chat_like:
        if not candidate:
            continue
        for key in ("isGroup", "hasParticipants", "isGroupChat"):
            value = _read_bool(candidate.get(key))
            if value is not None:
                return value
    return None


def parse_bluebubbles_payload(payload: dict[str, Any]) -> BlueBubblesInboundMessage:
    data = _read_object(payload.get("data")) or payload
    message = _read_object(data.get("message")) or data
    chat = _read_object(data.get("chat")) or _read_object(payload.get("chat"))
    sender = _read_object(data.get("sender")) or _read_object(message.get("handle"))

    chat_guid = _extract_chat_guid(chat, message)
    if not chat_guid:
        raise ValueError("bluebubbles_chat_guid_required")

    body = (
        _read_string(message.get("text"))
        or _read_string(message.get("body"))
        or _read_string(data.get("body"))
        or ""
    )
    message_id = (
        _read_string(message.get("guid"))
        or _read_string(message.get("id"))
        or _read_string(data.get("guid"))
        or _read_string(payload.get("guid"))
        or ""
    )
    sender_handle = (
        _read_string(sender.get("address")) if sender else None
    ) or (
        _read_string(message.get("handleId"))
        or _read_string(message.get("from"))
        or _read_string(data.get("from"))
        or ""
    )

    if not message_id:
        raise ValueError("bluebubbles_message_id_required")
    if not sender_handle:
        raise ValueError("bluebubbles_sender_handle_required")

    return BlueBubblesInboundMessage(
        source_message_id=message_id,
        chat_guid=chat_guid,
        body=body,
        sender_handle=sender_handle,
        is_from_me=bool(message.get("isFromMe") or data.get("isFromMe") or payload.get("isFromMe")),
        event_type=_read_string(payload.get("type")) or _read_string(data.get("eventType")),
        is_group_hint=_extract_group_hint(chat, message, data),
        participants=_extract_participants(chat),
        raw_payload=payload,
    )
