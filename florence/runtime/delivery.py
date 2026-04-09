"""Channel delivery and channel-resolution helpers for Florence runtime."""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Callable

from florence.contracts import ChannelMessage, ChannelMessageRole, ChannelType, PilotEvent
from florence.runtime.entrypoints import FlorenceEntrypointResult
from florence.state import FlorenceStateDB
from florence.text_safety import scrub_internal_ids

logger = logging.getLogger(__name__)


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
        reply_messages = result.reply_messages or ((result.reply_text,) if result.reply_text else ())
        if reply_messages and result.channel_id:
            channel = self.store.get_channel(result.channel_id)
            if channel is not None:
                for message in reply_messages:
                    self.send_channel_message(channel=channel, message=message, record_message=False)

        if result.group_announcement and result.household_id:
            group_channel = self.find_group_channel(result.household_id, provider=provider)
            if group_channel is not None:
                self.send_channel_message(channel=group_channel, message=result.group_announcement)

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
        try:
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
            self._record_outbound_audit_event(
                store=target_store,
                channel=channel,
                outgoing_message=outgoing_message,
                record_message=record_message,
                message_metadata=message_metadata,
                transport_metadata=transport_metadata,
            )
            return True
        except Exception:
            logger.exception("Florence transport delivery failed for channel %s", channel.provider_channel_id)
            self._record_outbound_audit_event(
                store=target_store,
                channel=channel,
                outgoing_message=outgoing_message,
                record_message=record_message,
                message_metadata=message_metadata,
                transport_metadata=transport_metadata,
                failed=True,
            )
            return False

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
