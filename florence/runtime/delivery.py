"""Channel delivery and channel-resolution helpers for Florence runtime."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import replace
from typing import Any, Callable

from florence.contracts import ChannelMessage, ChannelMessageRole, ChannelType, PilotEvent
from florence.runtime.entrypoints import FlorenceEntrypointResult
from florence.runtime.reliability import (
    FlorenceReliabilityEvent,
    record_reliability_event,
    transport_event_metadata,
)
from florence.sendblue import FlorenceSendbluePermanentOptOutError
from florence.state import FlorenceStateDB
from florence.text_safety import scrub_internal_ids

logger = logging.getLogger(__name__)
_TRANSPORT_DISABLED_KEY = "transport_disabled"
_TRANSPORT_DISABLED_REASON_KEY = "transport_disabled_reason"
_TRANSPORT_DISABLED_AT_KEY = "transport_disabled_at"
_SENDBLUE_OPT_OUT_REASON = "sendblue_opted_out"


def _plain_text_transport_message(message: str) -> str:
    text = message
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
    text = re.sub(r"```(?:[a-zA-Z0-9_+-]+)?\n?", "", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"(?<!\*)\*\*([^\n*][\s\S]*?[^\n*])\*\*(?!\*)", r"\1", text)
    text = re.sub(r"(?<!_)__([^\n_][\s\S]*?[^\n_])__(?!_)", r"\1", text)
    text = re.sub(r"(?<!\S)\*([^\n*]+)\*(?!\S)", r"\1", text)
    text = re.sub(r"(?<!\S)_([^\n_]+)_(?!\S)", r"\1", text)
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"\n{3,}", "\n\n", text)
    # Backstop: malformed model emphasis can leave raw markers behind even after
    # the paired-markdown cleanup above. Strip any leftovers before SMS delivery.
    text = text.replace("**", "").replace("__", "")
    return scrub_internal_ids(text.strip())


class FlorenceChannelDeliveryService:
    """Transport delivery plus household-channel lookup."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        linq_client_getter: Callable[[], Any],
        sendblue_client_getter: Callable[[], Any],
    ) -> None:
        self.store = store
        self._linq_client_getter = linq_client_getter
        self._sendblue_client_getter = sendblue_client_getter

    def deliver_ingress_result(
        self,
        *,
        result: FlorenceEntrypointResult,
        provider: str,
    ) -> None:
        reply_metadata = dict(getattr(result, "reply_metadata", {}) or {})
        reply_messages = result.reply_messages or ((result.reply_text,) if result.reply_text else ())
        if reply_messages and result.channel_id:
            channel = self.store.get_channel(result.channel_id)
            if channel is not None:
                for message in reply_messages:
                    self.send_channel_message(
                        channel=channel,
                        message=message,
                        record_message=False,
                        message_metadata={
                            **reply_metadata,
                            "delivery_kind": "reply",
                        },
                    )

        if result.group_announcement and result.household_id:
            group_channel = self.find_group_channel(result.household_id, provider=provider)
            if group_channel is not None:
                self.send_channel_message(
                    channel=group_channel,
                    message=result.group_announcement,
                    message_metadata={
                        **reply_metadata,
                        "delivery_kind": "group_announcement",
                        "source_channel_id": result.channel_id,
                    },
                )

    def find_group_channel(
        self,
        household_id: str,
        *,
        provider: str,
        store: FlorenceStateDB | None = None,
    ) -> Any | None:
        target_store = store or self.store
        channels = target_store.list_channels(household_id=household_id, channel_type=ChannelType.HOUSEHOLD_GROUP)
        for channel in channels:
            if channel.provider == provider:
                return channel
        return None

    def preferred_household_channel(
        self,
        *,
        household_id: str,
        fallback_channel: Any,
        store: FlorenceStateDB | None = None,
    ) -> Any:
        group_channel = self.find_group_channel(
            household_id,
            provider=fallback_channel.provider,
            store=store,
        )
        return group_channel or fallback_channel

    def find_channel_by_provider_id(
        self,
        provider_channel_id: str,
        *,
        store: FlorenceStateDB | None = None,
    ) -> Any | None:
        target_store = store or self.store
        for household in target_store.list_households():
            for channel in target_store.list_channels(household_id=household.id):
                if channel.provider_channel_id == provider_channel_id:
                    return channel
        return None

    def send_channel_message(
        self,
        *,
        channel: Any,
        message: str,
        record_message: bool = True,
        store: FlorenceStateDB | None = None,
        message_metadata: dict[str, Any] | None = None,
    ) -> bool:
        target_store = store or self.store
        outgoing_message = _plain_text_transport_message(message)
        transport_metadata = self._transport_metadata_for_channel(channel)
        blocked_reason = self.channel_delivery_blocked_reason(channel)
        if blocked_reason:
            logger.info(
                "Skipping Florence transport delivery for blocked channel %s reason=%s",
                channel.provider_channel_id,
                blocked_reason,
            )
            self._record_outbound_reliability_event(
                store=target_store,
                channel=channel,
                outgoing_message=outgoing_message,
                record_message=record_message,
                message_metadata=message_metadata,
                transport_metadata=transport_metadata,
                event_type=FlorenceReliabilityEvent.OUTBOUND_SKIPPED,
                skipped_reason=blocked_reason,
            )
            self._record_outbound_audit_event(
                store=target_store,
                channel=channel,
                outgoing_message=outgoing_message,
                record_message=record_message,
                message_metadata=message_metadata,
                transport_metadata=transport_metadata,
                failed=True,
                failure_reason=blocked_reason,
            )
            return False
        try:
            self._record_outbound_reliability_event(
                store=target_store,
                channel=channel,
                outgoing_message=outgoing_message,
                record_message=record_message,
                message_metadata=message_metadata,
                transport_metadata=transport_metadata,
                event_type=FlorenceReliabilityEvent.OUTBOUND_ATTEMPTED,
            )
            if channel.provider == "linq":
                self._linq_client_getter().send_text(chat_id=channel.provider_channel_id, message=outgoing_message)
            elif channel.provider == "sendblue":
                group_id = transport_metadata.get("group_id")
                numbers = transport_metadata.get("numbers")
                self._sendblue_client_getter().send_text(
                    thread_id=channel.provider_channel_id,
                    message=outgoing_message,
                    group_id=group_id,
                    numbers=numbers,
                )
            else:
                self._record_outbound_reliability_event(
                    store=target_store,
                    channel=channel,
                    outgoing_message=outgoing_message,
                    record_message=record_message,
                    message_metadata=message_metadata,
                    transport_metadata=transport_metadata,
                    event_type=FlorenceReliabilityEvent.OUTBOUND_SKIPPED,
                    skipped_reason="unsupported_provider",
                )
                return False
            if record_message:
                target_store.append_channel_message(
                    ChannelMessage(
                        id=self._assistant_message_id(channel.id),
                        household_id=channel.household_id,
                        channel_id=channel.id,
                        sender_role=ChannelMessageRole.ASSISTANT,
                        body=outgoing_message,
                        metadata={
                            "provider": channel.provider,
                            "transport_thread_id": channel.provider_channel_id,
                            **(message_metadata or {}),
                        },
                        created_at=time.time(),
                    )
                )
            self._record_outbound_reliability_event(
                store=target_store,
                channel=channel,
                outgoing_message=outgoing_message,
                record_message=record_message,
                message_metadata=message_metadata,
                transport_metadata=transport_metadata,
                event_type=FlorenceReliabilityEvent.OUTBOUND_SENT,
            )
            self._record_outbound_audit_event(
                store=target_store,
                channel=channel,
                outgoing_message=outgoing_message,
                record_message=record_message,
                message_metadata=message_metadata,
                transport_metadata=transport_metadata,
            )
            return True
        except FlorenceSendbluePermanentOptOutError:
            logger.warning(
                "Florence Sendblue recipient opted out; disabling channel %s",
                channel.provider_channel_id,
            )
            self._disable_channel_delivery(
                store=target_store,
                channel=channel,
                reason=_SENDBLUE_OPT_OUT_REASON,
            )
            self._record_outbound_reliability_event(
                store=target_store,
                channel=channel,
                outgoing_message=outgoing_message,
                record_message=record_message,
                message_metadata=message_metadata,
                transport_metadata=transport_metadata,
                event_type=FlorenceReliabilityEvent.OUTBOUND_FAILED,
                failure_reason=_SENDBLUE_OPT_OUT_REASON,
            )
            self._record_outbound_audit_event(
                store=target_store,
                channel=channel,
                outgoing_message=outgoing_message,
                record_message=record_message,
                message_metadata=message_metadata,
                transport_metadata=transport_metadata,
                failed=True,
                failure_reason=_SENDBLUE_OPT_OUT_REASON,
            )
            return False
        except Exception:
            logger.exception("Florence transport delivery failed for channel %s", channel.provider_channel_id)
            self._record_outbound_reliability_event(
                store=target_store,
                channel=channel,
                outgoing_message=outgoing_message,
                record_message=record_message,
                message_metadata=message_metadata,
                transport_metadata=transport_metadata,
                event_type=FlorenceReliabilityEvent.OUTBOUND_FAILED,
                failure_reason="transport_error",
            )
            self._record_outbound_audit_event(
                store=target_store,
                channel=channel,
                outgoing_message=outgoing_message,
                record_message=record_message,
                message_metadata=message_metadata,
                transport_metadata=transport_metadata,
                failed=True,
                failure_reason="transport_error",
            )
            return False

    @staticmethod
    def channel_delivery_blocked_reason(channel: Any) -> str | None:
        metadata = dict(getattr(channel, "metadata", {}) or {})
        if metadata.get(_TRANSPORT_DISABLED_KEY) is True:
            reason = str(metadata.get(_TRANSPORT_DISABLED_REASON_KEY) or "").strip()
            return reason or "transport_disabled"
        return None

    @staticmethod
    def _disable_channel_delivery(
        *,
        store: FlorenceStateDB,
        channel: Any,
        reason: str,
    ) -> None:
        metadata = dict(getattr(channel, "metadata", {}) or {})
        metadata[_TRANSPORT_DISABLED_KEY] = True
        metadata[_TRANSPORT_DISABLED_REASON_KEY] = reason
        metadata[_TRANSPORT_DISABLED_AT_KEY] = time.time()
        try:
            store.upsert_channel(replace(channel, metadata=metadata))
            record_reliability_event(
                store,
                FlorenceReliabilityEvent.CHANNEL_DISABLED,
                household_id=channel.household_id,
                channel_id=channel.id,
                metadata=transport_event_metadata(
                    provider=channel.provider,
                    provider_channel_id=channel.provider_channel_id,
                    failure_reason=reason,
                    channel_type=(
                        channel.channel_type.value
                        if hasattr(channel.channel_type, "value")
                        else str(channel.channel_type)
                    ),
                ),
            )
        except Exception:
            logger.exception(
                "Florence failed to persist disabled transport metadata for channel %s",
                channel.provider_channel_id,
            )

    @staticmethod
    def _assistant_message_id(channel_id: str) -> str:
        return f"msg_asst_{channel_id}_{time.time_ns()}"

    @staticmethod
    def _transport_metadata_for_channel(channel: Any) -> dict[str, Any]:
        metadata = dict(getattr(channel, "metadata", {}) or {})
        group_id = str(metadata.get("group_id") or "").strip() or None
        numbers = None
        sendblue_number = str(metadata.get("sendblue_number") or "").strip()
        if group_id is None and channel.channel_type == ChannelType.HOUSEHOLD_GROUP:
            participant_handles = metadata.get("participant_handles")
            if isinstance(participant_handles, list):
                numbers = [
                    str(handle).strip()
                    for handle in participant_handles
                    if isinstance(handle, str)
                    and str(handle).strip()
                    and str(handle).strip() != sendblue_number
                ]
                numbers = list(dict.fromkeys(numbers)) or None
        return {
            "group_id": group_id,
            "numbers": numbers,
            "sendblue_number": sendblue_number or None,
        }

    def _record_outbound_reliability_event(
        self,
        *,
        store: FlorenceStateDB,
        channel: Any,
        outgoing_message: str,
        record_message: bool,
        message_metadata: dict[str, Any] | None,
        transport_metadata: dict[str, Any],
        event_type: FlorenceReliabilityEvent,
        failure_reason: str | None = None,
        skipped_reason: str | None = None,
    ) -> None:
        raw_message_metadata = dict(message_metadata or {})
        turn_id = str(raw_message_metadata.get("florence_turn_id") or raw_message_metadata.get("turn_id") or "").strip()
        delivery_kind = str(raw_message_metadata.get("delivery_kind") or "").strip()
        provider_message_id = str(
            raw_message_metadata.get("provider_message_id")
            or raw_message_metadata.get("message_id")
            or raw_message_metadata.get("transport_reply_to")
            or ""
        ).strip()
        metadata = transport_event_metadata(
            provider=channel.provider,
            provider_channel_id=channel.provider_channel_id,
            message_id=provider_message_id or None,
            turn_id=turn_id or None,
            delivery_kind=delivery_kind or None,
            failure_reason=failure_reason,
            skipped_reason=skipped_reason,
            channel_type=(
                channel.channel_type.value
                if hasattr(channel.channel_type, "value")
                else str(channel.channel_type)
            ),
            channel_title=getattr(channel, "title", None),
            record_message=record_message,
            message_length=len(outgoing_message),
            message_metadata=raw_message_metadata,
            **transport_metadata,
        )
        try:
            record_reliability_event(
                store,
                event_type,
                household_id=channel.household_id,
                channel_id=channel.id,
                metadata=metadata,
            )
        except Exception:
            logger.exception(
                "Florence outbound reliability logging failed for channel %s",
                channel.provider_channel_id,
            )

    def _record_outbound_audit_event(
        self,
        *,
        store: FlorenceStateDB,
        channel: Any,
        outgoing_message: str,
        record_message: bool,
        message_metadata: dict[str, Any] | None,
        transport_metadata: dict[str, Any],
        failed: bool = False,
        failure_reason: str | None = None,
    ) -> None:
        event_type = "outbound_message_failed" if failed else "outbound_message_sent"
        metadata: dict[str, Any] = {
            "provider": channel.provider,
            "provider_channel_id": channel.provider_channel_id,
            "channel_type": channel.channel_type.value if hasattr(channel.channel_type, "value") else str(channel.channel_type),
            "channel_title": getattr(channel, "title", None),
            "message": outgoing_message,
            "record_message": record_message,
            "message_metadata": dict(message_metadata or {}),
            **transport_metadata,
        }
        if failure_reason:
            metadata["failure_reason"] = failure_reason
        try:
            store.upsert_pilot_event(
                PilotEvent(
                    id=f"pilot_{event_type}_{channel.id}_{time.time_ns()}",
                    household_id=channel.household_id,
                    event_type=event_type,
                    channel_id=channel.id,
                    metadata=metadata,
                    created_at=time.time(),
                )
            )
        except Exception:
            logger.exception(
                "Florence outbound audit logging failed for channel %s",
                channel.provider_channel_id,
            )
