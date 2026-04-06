"""Channel delivery and channel-resolution helpers for Florence runtime."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable

from florence.contracts import ChannelMessage, ChannelMessageRole, ChannelType
from florence.runtime.entrypoints import FlorenceEntrypointResult
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)


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
        try:
            target_store = store or self.store
            if channel.provider == "linq":
                self._linq_client_getter().send_text(chat_id=channel.provider_channel_id, message=message)
            elif channel.provider == "sendblue":
                metadata = dict(getattr(channel, "metadata", {}) or {})
                group_id = str(metadata.get("group_id") or "").strip() or None
                numbers = None
                if group_id is None and channel.channel_type == ChannelType.HOUSEHOLD_GROUP:
                    participant_handles = metadata.get("participant_handles")
                    sendblue_number = str(metadata.get("sendblue_number") or "").strip()
                    if isinstance(participant_handles, list):
                        numbers = [
                            str(handle).strip()
                            for handle in participant_handles
                            if isinstance(handle, str)
                            and str(handle).strip()
                            and str(handle).strip() != sendblue_number
                        ]
                        numbers = list(dict.fromkeys(numbers)) or None
                self._sendblue_client_getter().send_text(
                    thread_id=channel.provider_channel_id,
                    message=message,
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
                        body=message,
                        metadata={
                            "provider": channel.provider,
                            "transport_thread_id": channel.provider_channel_id,
                            **(message_metadata or {}),
                        },
                        created_at=time.time(),
                    )
                )
            return True
        except Exception:
            logger.exception("Florence transport delivery failed for channel %s", channel.provider_channel_id)
            return False

    @staticmethod
    def _assistant_message_id(channel_id: str) -> str:
        return f"msg_asst_{channel_id}_{time.time_ns()}"
