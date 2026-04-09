"""Transport-agnostic Florence DM and household-group ingress."""

from __future__ import annotations

import logging

from florence.messaging.chat_bridge import FlorenceHouseholdChatBridge
from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.dm_router import FlorenceDmRouter
from florence.messaging.group_share_protocol import FlorenceGroupShareProtocol
from florence.messaging.group_router import FlorenceGroupRouter
from florence.messaging.household_link_protocol import FlorenceHouseholdLinkProtocol
from florence.messaging.ingress_types import (
    FlorenceMessagingIngressResult,
    FlorenceResolvedInboundMessage,
)
from florence.messaging.reminder_protocol import FlorenceReminderProtocol
from florence.messaging.review_protocol import FlorenceCandidateReviewProtocol
from florence.messaging.setup_protocol import FlorenceSetupProtocol
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime.candidate_review import FlorenceCandidateReviewService
from florence.runtime.group_share import FlorenceGroupShareService
from florence.runtime.household_manager import FlorenceHouseholdManagerService
from florence.runtime.household_link import FlorenceHouseholdLinkService
from florence.runtime.onboarding_service import FlorenceOnboardingSessionService
from florence.state import FlorenceStateDB
from florence.text_safety import scrub_internal_ids

logger = logging.getLogger(__name__)

class FlorenceMessagingIngressService:
    """Routes normalized transport messages into Florence onboarding and chat flows."""

    def __init__(
        self,
        store: FlorenceStateDB,
        onboarding_service: FlorenceOnboardingSessionService,
        candidate_review_service: FlorenceCandidateReviewService,
        *,
        household_chat_service: FlorenceHouseholdChatService,
        household_manager_service: FlorenceHouseholdManagerService | None = None,
    ):
        self.store = store
        self.channel_log = FlorenceChannelLog(store)
        self.onboarding_service = onboarding_service
        self.candidate_review_service = candidate_review_service
        self.household_chat_service = household_chat_service
        self.chat_bridge = FlorenceHouseholdChatBridge(
            household_chat_service=self.household_chat_service,
            channel_log=self.channel_log,
        )
        self.household_manager_service = household_manager_service or FlorenceHouseholdManagerService(store)
        self.household_link_service = FlorenceHouseholdLinkService(store)
        self.review_protocol = FlorenceCandidateReviewProtocol(
            channel_log=self.channel_log,
            candidate_review_service=self.candidate_review_service,
            household_chat_service=self.household_chat_service,
        )
        self.group_share_service = FlorenceGroupShareService(
            store,
            channel_log=self.channel_log,
            household_chat_service=self.household_chat_service,
        )
        self.setup_protocol = FlorenceSetupProtocol(
            onboarding_service=self.onboarding_service,
            on_complete=lambda household_id, member_id, channel_id: self._record_onboarding_completion(
                household_id=household_id,
                member_id=member_id,
                channel_id=channel_id,
            ),
            handle_onboarding_turn=lambda household_id, channel_id, member_id, payload: self.chat_bridge.handle_setup_onboarding_turn(
                household_id=household_id,
                channel_id=channel_id,
                member_id=member_id,
                payload=payload,
            ),
            handle_sync_waiting_turn=lambda household_id, channel_id, member_id, user_message, data_dependent: self.chat_bridge.handle_setup_sync_waiting_turn(
                household_id=household_id,
                channel_id=channel_id,
                member_id=member_id,
                user_message=user_message,
                data_dependent=data_dependent,
            ),
        )
        self.group_share_protocol = FlorenceGroupShareProtocol(
            group_share_service=self.group_share_service,
        )
        self.household_link_protocol = FlorenceHouseholdLinkProtocol(
            channel_log=self.channel_log,
            household_link_service=self.household_link_service,
        )
        self.reminder_protocol = FlorenceReminderProtocol(
            channel_log=self.channel_log,
            household_manager_service=self.household_manager_service,
            onboarding_service=self.onboarding_service,
        )
        self.dm_router = FlorenceDmRouter(
            onboarding_service=self.onboarding_service,
            group_share_protocol=self.group_share_protocol,
            household_link_protocol=self.household_link_protocol,
            review_protocol=self.review_protocol,
            setup_protocol=self.setup_protocol,
            reminder_protocol=self.reminder_protocol,
            chat_bridge=self.chat_bridge,
        )
        self.group_router = FlorenceGroupRouter(
            store=self.store,
            channel_log=self.channel_log,
            chat_bridge=self.chat_bridge,
        )

    def handle_message(self, resolved: FlorenceResolvedInboundMessage) -> FlorenceMessagingIngressResult:
        if resolved.message.is_from_me:
            return FlorenceMessagingIngressResult(consumed=False)

        if self.channel_log.has_inbound_message(
            provider=resolved.message.provider,
            message_id=resolved.message.message_id,
        ):
            logger.info(
                "Ignoring duplicate inbound message provider=%s message_id=%s channel_id=%s",
                resolved.message.provider,
                resolved.message.message_id,
                resolved.channel_id,
            )
            return FlorenceMessagingIngressResult(consumed=True)

        self.channel_log.append_inbound_message(
            household_id=resolved.household_id,
            channel_id=resolved.channel_id,
            member_id=resolved.member_id,
            thread_id=resolved.thread_id,
            message=resolved.message,
        )

        result = self.group_router.handle_message(resolved) if resolved.is_group else self.dm_router.handle_message(resolved)

        raw_reply_messages = result.reply_messages or ((result.reply_text,) if result.reply_text else ())
        reply_messages = tuple(scrub_internal_ids(message) for message in raw_reply_messages if message)
        persisted_channel = self.store.get_channel(resolved.channel_id)
        persisted_household_id = (
            persisted_channel.household_id
            if persisted_channel is not None
            else resolved.household_id
        )
        self.channel_log.append_transport_reply_messages(
            household_id=persisted_household_id,
            channel_id=resolved.channel_id,
            provider=resolved.message.provider,
            thread_id=resolved.thread_id,
            reply_to_message_id=resolved.message.message_id,
            messages=reply_messages,
            metadata=result.reply_metadata,
        )

        return result

    def append_assistant_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        body: str,
        metadata: dict[str, object] | None = None,
    ) -> ChannelMessage:
        return self.channel_log.append_assistant_message(
            household_id=household_id,
            channel_id=channel_id,
            body=body,
            metadata=metadata,
        )

    def _record_onboarding_completion(self, *, household_id: str, member_id: str, channel_id: str) -> None:
        try:
            self.household_manager_service.finalize_onboarding_completion(
                household_id=household_id,
                member_id=member_id,
                channel_id=channel_id,
            )
        except Exception:
            logger.exception("Failed to finalize onboarding completion hooks for household_id=%s", household_id)
