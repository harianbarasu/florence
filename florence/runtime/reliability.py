"""Durable reliability breadcrumbs for Florence transport and turn handling."""

from __future__ import annotations

import contextlib
import time
from contextvars import ContextVar
from enum import StrEnum
from typing import Any, Iterator

from florence.contracts import PilotEvent
from florence.state import FlorenceStateDB


UNKNOWN_HOUSEHOLD_ID = "__unresolved_household__"


class FlorenceReliabilityEvent(StrEnum):
    INBOUND_RECEIVED = "inbound_received"
    INBOUND_DEDUPLICATED = "inbound_deduplicated"
    IDENTITY_RESOLVED = "identity_resolved"
    CHANNEL_RESOLVED = "channel_resolved"
    ROUTE_SELECTED = "route_selected"
    HERMES_TURN_STARTED = "hermes_turn_started"
    HERMES_TURN_COMPLETED = "hermes_turn_completed"
    REPLY_GENERATED = "reply_generated"
    OUTBOUND_ATTEMPTED = "outbound_attempted"
    OUTBOUND_SENT = "outbound_sent"
    OUTBOUND_SKIPPED = "outbound_skipped"
    OUTBOUND_FAILED = "outbound_failed"
    CHANNEL_DISABLED = "channel_disabled"


_RELIABILITY_CONTEXT: ContextVar[dict[str, Any]] = ContextVar(
    "florence_reliability_context",
    default={},
)


@contextlib.contextmanager
def reliability_context(**metadata: Any) -> Iterator[None]:
    current = dict(_RELIABILITY_CONTEXT.get() or {})
    current.update(_compact_metadata(metadata))
    token = _RELIABILITY_CONTEXT.set(current)
    try:
        yield
    finally:
        _RELIABILITY_CONTEXT.reset(token)


def current_reliability_context() -> dict[str, Any]:
    return dict(_RELIABILITY_CONTEXT.get() or {})


def record_reliability_event(
    store: FlorenceStateDB,
    event_type: FlorenceReliabilityEvent | str,
    *,
    household_id: str | None = None,
    member_id: str | None = None,
    channel_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    created_at: float | None = None,
) -> PilotEvent:
    event_value = event_type.value if isinstance(event_type, FlorenceReliabilityEvent) else str(event_type)
    context_metadata = current_reliability_context()
    payload = {
        "reliability_event": True,
        "event_type": event_value,
        **context_metadata,
        **dict(metadata or {}),
    }
    payload = _compact_metadata(payload)
    event = PilotEvent(
        id=f"pilot_{event_value}_{time.time_ns()}",
        household_id=household_id or str(payload.get("household_id") or "").strip() or UNKNOWN_HOUSEHOLD_ID,
        event_type=event_value,
        member_id=member_id or _optional_string(payload.get("member_id")),
        channel_id=channel_id or _optional_string(payload.get("channel_id")),
        metadata=payload,
        created_at=created_at if created_at is not None else time.time(),
    )
    return store.upsert_pilot_event(event)


def transport_event_metadata(
    *,
    provider: str,
    provider_channel_id: str | None = None,
    message_id: str | None = None,
    turn_id: str | None = None,
    correlation_id: str | None = None,
    delivery_kind: str | None = None,
    failure_reason: str | None = None,
    skipped_reason: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "provider": provider,
        "provider_channel_id": provider_channel_id,
        "message_id": message_id,
        "provider_message_id": message_id,
        "turn_id": turn_id,
        "correlation_id": correlation_id or _correlation_id(provider, message_id, provider_channel_id),
        "delivery_kind": delivery_kind,
        "failure_reason": failure_reason,
        "skipped_reason": skipped_reason,
        **extra,
    }
    return _compact_metadata(payload)


def _correlation_id(provider: str, message_id: str | None, provider_channel_id: str | None) -> str | None:
    if message_id:
        return f"{provider}:{message_id}"
    if provider_channel_id:
        return f"{provider}:{provider_channel_id}"
    return provider or None


def _optional_string(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _compact_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    compacted: dict[str, Any] = {}
    for key, value in metadata.items():
        if value is None:
            continue
        if isinstance(value, dict):
            compacted[str(key)] = _compact_metadata(value)
        elif isinstance(value, (list, tuple)):
            compacted[str(key)] = [_jsonable(item) for item in value if item is not None]
        else:
            compacted[str(key)] = _jsonable(value)
    return compacted


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return _compact_metadata(value)
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, StrEnum):
        return value.value
    return str(value)
