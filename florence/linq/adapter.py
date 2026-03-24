"""Linq webhook normalization for Florence."""

from __future__ import annotations

from typing import Any

from florence.messaging import FlorenceInboundMessage

IGNORED_LINQ_EVENT_TYPES = {
    "message.sent",
    "message.delivered",
    "message.read",
    "message.failed",
    "reaction.added",
    "reaction.removed",
    "chat.typing_indicator.started",
    "chat.typing_indicator.stopped",
}


def _read_string(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _read_object(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _read_array(value: Any) -> list[Any] | None:
    return value if isinstance(value, list) else None


def _flatten_text(parts: list[Any]) -> str:
    values: list[str] = []
    for part in parts:
        if not isinstance(part, dict):
            continue
        if _read_string(part.get("type")) != "text":
            continue
        value = _read_string(part.get("value"))
        if value:
            values.append(value)
    return "\n".join(values).strip()


def parse_linq_payload(payload: dict[str, Any]) -> FlorenceInboundMessage:
    event_type = _read_string(payload.get("event_type")) or _read_string(payload.get("type"))
    data = _read_object(payload.get("data")) or payload
    webhook_version = _read_string(payload.get("webhook_version")) or "2026-02-03"

    if webhook_version >= "2026-02-03":
        chat = _read_object(data.get("chat")) or {}
        sender_handle = _read_object(data.get("sender_handle")) or {}
        parts = _read_array(data.get("parts")) or []
        chat_id = _read_string(chat.get("id"))
        message_id = _read_string(data.get("id"))
        sender = _read_string(sender_handle.get("handle"))
        if not chat_id:
            raise ValueError("linq_chat_id_required")
        if not message_id:
            raise ValueError("linq_message_id_required")
        if not sender:
            raise ValueError("linq_sender_handle_required")
        body = _flatten_text(parts)
        return FlorenceInboundMessage(
            provider="linq",
            message_id=message_id,
            thread_id=chat_id,
            sender_handle=sender,
            body=body,
            is_group_chat=bool(chat.get("is_group")),
            is_from_me=_read_string(data.get("direction")) == "outbound" or bool(sender_handle.get("is_me")),
            event_type=event_type,
            participant_handles=(),
            reply_to_message_id=_read_string((_read_object(data.get("reply_to")) or {}).get("id")),
            sent_at=_read_string(data.get("sent_at")),
            metadata={
                "service": _read_string(data.get("service")),
                "webhook_version": webhook_version,
                "event_id": _read_string(payload.get("event_id")),
                "trace_id": _read_string(payload.get("trace_id")),
            },
        )

    message = _read_object(data.get("message")) or {}
    chat_id = _read_string(data.get("chat_id")) or _read_string((_read_object(data.get("chat")) or {}).get("id"))
    message_id = _read_string(message.get("id")) or _read_string(data.get("id"))
    sender = _read_string(data.get("from_handle")) or _read_string((_read_object(message.get("sender_handle")) or {}).get("handle"))
    if not chat_id:
        raise ValueError("linq_chat_id_required")
    if not message_id:
        raise ValueError("linq_message_id_required")
    if not sender:
        raise ValueError("linq_sender_handle_required")
    parts = _read_array(message.get("parts")) or []
    body = _flatten_text(parts)
    return FlorenceInboundMessage(
        provider="linq",
        message_id=message_id,
        thread_id=chat_id,
        sender_handle=sender,
        body=body,
        is_group_chat=bool(data.get("is_group")),
        is_from_me=bool(data.get("is_from_me")),
        event_type=event_type,
        participant_handles=(),
        reply_to_message_id=_read_string((_read_object(message.get("reply_to")) or {}).get("id")),
        sent_at=_read_string(message.get("sent_at")),
        metadata={
            "service": _read_string(message.get("service")),
            "webhook_version": webhook_version,
            "event_id": _read_string(payload.get("event_id")),
            "trace_id": _read_string(payload.get("trace_id")),
        },
    )
