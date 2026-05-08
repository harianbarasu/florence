"""Turn replay/audit record helpers."""

from __future__ import annotations

from typing import Any

from florence.messaging.ingress_types import FlorenceMessagingIngressResult
from florence.state import FlorenceStateDB
from florence.turns import FlorenceTurnDisposition, FlorenceTurnEnvelope

FLORENCE_PROMPT_VERSION = "group-first-v1"


def record_turn(
    store: FlorenceStateDB,
    *,
    envelope: FlorenceTurnEnvelope,
    result: FlorenceMessagingIngressResult,
    hermes_version: str | None = None,
    prompt_version: str = FLORENCE_PROMPT_VERSION,
) -> None:
    store.upsert_turn_record(
        turn_id=envelope.turn_id,
        household_id=envelope.household_id,
        channel_id=envelope.channel_id,
        actor_member_id=envelope.actor_member_id,
        trigger_kind=envelope.trigger_kind.value,
        disposition=_result_disposition(result),
        envelope=_serialize_envelope(envelope),
        outcome=_serialize_result(result),
        hermes_version=hermes_version,
        prompt_version=prompt_version,
    )


def record_turn_outcome(
    store: FlorenceStateDB,
    *,
    envelope: FlorenceTurnEnvelope,
    disposition: FlorenceTurnDisposition | str,
    reply_messages: tuple[str, ...] | list[str] = (),
    group_announcement: str | None = None,
    state_change_ids: tuple[str, ...] | list[str] = (),
    scheduled_work_ids: tuple[str, ...] | list[str] = (),
    no_reply_reason: str | None = None,
    metadata: dict[str, Any] | None = None,
    consumed: bool | None = None,
    hermes_version: str | None = None,
    prompt_version: str = FLORENCE_PROMPT_VERSION,
) -> None:
    disposition_value = disposition.value if isinstance(disposition, FlorenceTurnDisposition) else str(disposition)
    reply_values = tuple(str(message) for message in reply_messages if str(message or "").strip())
    if consumed is None:
        consumed = disposition_value != FlorenceTurnDisposition.NO_REPLY.value or bool(
            reply_values or group_announcement
        )
    outcome = {
        "reply_text": reply_values[0] if reply_values else None,
        "reply_messages": list(reply_values),
        "group_announcement": group_announcement,
        "state_change_ids": list(state_change_ids),
        "scheduled_work_ids": list(scheduled_work_ids),
        "no_reply_reason": no_reply_reason,
        "metadata": _jsonable(metadata or {}),
        "consumed": consumed,
    }
    store.upsert_turn_record(
        turn_id=envelope.turn_id,
        household_id=envelope.household_id,
        channel_id=envelope.channel_id,
        actor_member_id=envelope.actor_member_id,
        trigger_kind=envelope.trigger_kind.value,
        disposition=disposition_value,
        envelope=_serialize_envelope(envelope),
        outcome=outcome,
        hermes_version=hermes_version,
        prompt_version=prompt_version,
    )


def _serialize_envelope(envelope: FlorenceTurnEnvelope) -> dict[str, object]:
    tool_scope = envelope.tool_scope
    return {
        "turn_id": envelope.turn_id,
        "trigger_kind": envelope.trigger_kind.value,
        "household_id": envelope.household_id,
        "actor_member_id": envelope.actor_member_id,
        "channel_id": envelope.channel_id,
        "channel_type": envelope.channel_type.value if envelope.channel_type is not None else None,
        "visibility_scope": envelope.visibility_scope.scope,
        "tool_scope": {
            "turn_id": tool_scope.turn_id,
            "household_id": tool_scope.household_id,
            "actor_member_id": tool_scope.actor_member_id,
            "channel_id": tool_scope.channel_id,
            "channel_type": tool_scope.channel_type.value if tool_scope.channel_type is not None else None,
            "visibility_scope": tool_scope.visibility_scope.scope,
            "allowed_source_scopes": list(tool_scope.allowed_source_scopes),
        },
        "delivery_target": {
            "channel_id": envelope.channel_id,
            "channel_type": envelope.channel_type.value if envelope.channel_type is not None else None,
            "provider": envelope.provider,
            "provider_thread_id": envelope.provider_thread_id,
        },
        "provider": envelope.provider,
        "provider_thread_id": envelope.provider_thread_id,
        "provider_message_id": envelope.provider_message_id,
        "message_text": envelope.message_text,
        "attachment_count": len(envelope.attachments),
        "recent_history_count": len(envelope.recent_history),
        "received_at": envelope.received_at,
        "metadata": _jsonable(envelope.metadata),
    }


def _serialize_result(result: FlorenceMessagingIngressResult) -> dict[str, object]:
    return {
        "reply_text": result.reply_text,
        "reply_messages": list(result.reply_messages),
        "reply_metadata": _jsonable(result.reply_metadata),
        "group_announcement": result.group_announcement,
        "consumed": result.consumed,
    }


def _result_disposition(result: FlorenceMessagingIngressResult) -> str:
    if result.group_announcement and not (result.reply_text or result.reply_messages):
        return "group_announcement"
    if result.reply_messages:
        return "reply_multiple" if len(result.reply_messages) > 1 else "reply"
    if result.reply_text:
        return "reply"
    if result.consumed:
        return "delivery_only"
    return "no_reply"


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
