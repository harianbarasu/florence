"""DM-to-group promotion service for Florence's private/share boundary."""

from __future__ import annotations

from dataclasses import dataclass

from florence.contracts import ChannelType
from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.protocol_types import CANDIDATE_REVIEW_PROMPT_KIND
from florence.state import FlorenceStateDB


@dataclass(slots=True)
class FlorenceGroupShareResult:
    reply_text: str
    group_announcement: str | None = None


class FlorenceGroupShareService:
    """Own the deterministic DM-to-group promotion decision and fallback flow."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        channel_log: FlorenceChannelLog,
        household_chat_service,
        review_confirmation_suffix: str,
    ) -> None:
        self.store = store
        self.channel_log = channel_log
        self.household_chat_service = household_chat_service
        self.review_confirmation_suffix = review_confirmation_suffix

    def handle_explicit_share_request(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        current_provider: str,
        current_message_id: str,
    ) -> FlorenceGroupShareResult | None:
        latest_assistant = self.channel_log.latest_assistant_message(channel_id=channel_id)
        latest_body = latest_assistant.body.strip() if latest_assistant is not None else None
        if latest_assistant is not None and latest_assistant.metadata.get("protocol_kind") == CANDIDATE_REVIEW_PROMPT_KIND:
            return None
        if latest_body is not None and self.review_confirmation_suffix in latest_body:
            return None

        provider = self._provider_for_channel(
            channel_id=channel_id,
            current_provider=current_provider,
        )
        group_channel = self._household_group_channel(
            household_id=household_id,
            provider=provider,
        )
        if group_channel is None:
            return FlorenceGroupShareResult(
                reply_text="I can share that once the parent group is active.",
            )

        promoted_message = self.channel_log.promote_latest_group_message(
            channel_id=channel_id,
            promoted_group_channel_id=group_channel.id,
        )
        if promoted_message is not None:
            if promoted_message.already_promoted:
                return FlorenceGroupShareResult(
                    reply_text="I already shared that with the parent group.",
                )
            return FlorenceGroupShareResult(
                reply_text="Shared a short version with the parent group.",
                group_announcement=promoted_message.group_announcement,
            )

        source_text = self.channel_log.recent_exchange_for_group_promotion(
            channel_id=channel_id,
            current_provider=current_provider,
            current_message_id=current_message_id,
        )
        if source_text is None:
            return FlorenceGroupShareResult(
                reply_text="There is not a clean update to share from that DM yet.",
            )
        shared_summary = self.household_chat_service.compose_operator_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            kind="group_promotion",
            payload={"source_text": source_text},
        )
        if not shared_summary or not shared_summary.strip():
            return FlorenceGroupShareResult(
                reply_text="There is not a clean update to share from that DM yet.",
            )
        return FlorenceGroupShareResult(
            reply_text="Shared a short version with the parent group.",
            group_announcement=shared_summary.strip(),
        )

    def _provider_for_channel(self, *, channel_id: str, current_provider: str) -> str:
        current_channel = self.store.get_channel(channel_id)
        if current_channel is not None and current_channel.provider:
            return current_channel.provider
        return current_provider

    def _household_group_channel(self, *, household_id: str, provider: str):
        return next(
            (
                channel
                for channel in self.store.list_channels(
                    household_id=household_id,
                    channel_type=ChannelType.HOUSEHOLD_GROUP,
                )
                if channel.provider == provider
            ),
            None,
        )
