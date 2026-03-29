"""Sendblue webhook normalization for Florence."""

from __future__ import annotations

from typing import Any

from florence.messaging import FlorenceInboundMessage


def _read_string(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _read_array(value: Any) -> list[Any] | None:
    return value if isinstance(value, list) else None


def build_sendblue_thread_id(*, sendblue_number: str, contact_number: str) -> str:
    line = _read_string(sendblue_number)
    contact = _read_string(contact_number)
    if not line or not contact:
        raise ValueError("sendblue_thread_id_required")
    return f"{line}|{contact}"


def parse_sendblue_payload(payload: dict[str, Any]) -> FlorenceInboundMessage:
    message_id = _read_string(payload.get("message_handle"))
    if not message_id:
        raise ValueError("sendblue_message_id_required")

    is_outbound = bool(payload.get("is_outbound"))
    status = _read_string(payload.get("status"))
    service = _read_string(payload.get("service"))
    from_number = _read_string(payload.get("from_number"))
    to_number = _read_string(payload.get("to_number"))
    sendblue_number = _read_string(payload.get("sendblue_number"))
    number = _read_string(payload.get("number"))
    line_handle = sendblue_number or (from_number if is_outbound else to_number)
    contact_handle = number or (to_number if is_outbound else from_number)
    if not line_handle or not contact_handle:
        raise ValueError("sendblue_thread_id_required")
    sender_handle = to_number if is_outbound else from_number
    if not sender_handle:
        raise ValueError("sendblue_sender_handle_required")

    group_id = _read_string(payload.get("group_id"))
    participant_handles = tuple(
        participant
        for participant in (_read_string(value) for value in (_read_array(payload.get("participants")) or []))
        if participant
    )
    return FlorenceInboundMessage(
        provider="sendblue",
        message_id=message_id,
        thread_id=build_sendblue_thread_id(sendblue_number=line_handle, contact_number=contact_handle),
        sender_handle=sender_handle,
        body=_read_string(payload.get("content")) or "",
        is_group_chat=bool(group_id),
        is_from_me=is_outbound,
        event_type=status,
        participant_handles=participant_handles,
        sent_at=_read_string(payload.get("date_sent")),
        metadata={
            "service": service,
            "status": status,
            "from_number": from_number,
            "to_number": to_number,
            "sendblue_number": sendblue_number,
            "group_id": group_id,
            "message_type": _read_string(payload.get("message_type")),
        },
    )
