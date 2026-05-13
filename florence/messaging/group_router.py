"""Household-group lane routing for Florence messaging ingress."""

from __future__ import annotations

from florence.contracts import ChannelMessageRole
from florence.messaging.chat_bridge import FlorenceHouseholdChatBridge
from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.ingress_types import (
    FlorenceMessagingIngressResult,
    FlorenceResolvedInboundMessage,
)
from florence.messaging.protocol_types import FlorenceProtocolReply
from florence.runtime.group_policy import FlorenceGroupTurnPolicy
from florence.runtime.household_manager import FlorenceHouseholdManagerService
from florence.runtime.review_feedback import ParsedReviewFeedback, ReviewFeedbackKind, parse_review_feedback
from florence.state import FlorenceStateDB
from florence.turns import (
    FlorenceTurnDisposition,
    FlorenceTurnEnvelope,
    FlorenceTurnOutcome,
    build_inbound_turn_envelope,
)


class FlorenceGroupRouter:
    """Route household group turns through a Hermes intro decision and normal chat."""

    def __init__(
        self,
        *,
        store: FlorenceStateDB,
        channel_log: FlorenceChannelLog,
        chat_bridge: FlorenceHouseholdChatBridge,
    ) -> None:
        self.store = store
        self.channel_log = channel_log
        self.chat_bridge = chat_bridge
        self.group_policy = FlorenceGroupTurnPolicy()

    def handle_message(self, resolved: FlorenceResolvedInboundMessage) -> FlorenceMessagingIngressResult:
        return self.handle_turn(
            envelope=build_inbound_turn_envelope(
                self.store,
                resolved,
                recent_history=tuple(self.channel_log.recent_messages(channel_id=resolved.channel_id, limit=24)),
            ),
            resolved=resolved,
        )

    def handle_turn(
        self,
        *,
        envelope: FlorenceTurnEnvelope,
        resolved: FlorenceResolvedInboundMessage,
    ) -> FlorenceMessagingIngressResult:
        if resolved.member_id is None:
            return self._result_from_turn_outcome(self.group_policy.no_actor_outcome(envelope))
        chatter_outcome = self.group_policy.obvious_chatter_outcome(envelope)
        if chatter_outcome is not None:
            return self._result_from_turn_outcome(chatter_outcome)

        shared_feedback = self._handle_shared_feedback(resolved)
        if shared_feedback is not None:
            return self._result_from_turn_outcome(
                self._outcome_from_protocol_reply(envelope, shared_feedback)
            )

        onboarding_sessions = self.store.list_member_onboarding_sessions(
            household_id=resolved.household_id,
            member_id=resolved.member_id,
        )
        latest = onboarding_sessions[0] if onboarding_sessions else None
        history = self.channel_log.recent_messages(channel_id=resolved.channel_id, limit=8)
        prior_assistant_messages = [message for message in history[:-1] if message.role == ChannelMessageRole.ASSISTANT]
        if (
            latest is not None
            and latest.is_complete
            and not prior_assistant_messages
        ):
            intro_result = self.chat_bridge.handle_group_intro_turn(
                household_id=resolved.household_id,
                channel_id=resolved.channel_id,
                member_id=resolved.member_id,
                user_message=resolved.message.body,
            )
            if intro_result is not None:
                return self._result_from_turn_outcome(
                    self._outcome_from_protocol_reply(envelope, intro_result)
                )

        chat_result = self.chat_bridge.respond_as_protocol(
            household_id=resolved.household_id,
            channel_id=resolved.channel_id,
            actor_member_id=resolved.member_id,
            message_text=resolved.message.body,
            message_attachments=resolved.message.attachments,
        )
        if chat_result is not None:
            return self._result_from_turn_outcome(
                self._outcome_from_protocol_reply(envelope, chat_result)
            )
        return self._result_from_turn_outcome(
            FlorenceTurnOutcome(
                disposition=FlorenceTurnDisposition.NO_REPLY,
                no_reply_reason="group_chat_no_result",
                metadata={"florence_turn_id": envelope.turn_id},
            )
        )

    def _outcome_from_protocol_reply(
        self,
        envelope: FlorenceTurnEnvelope,
        reply: FlorenceProtocolReply,
    ) -> FlorenceTurnOutcome:
        return self.group_policy.reply_outcome(
            envelope,
            reply_text=reply.reply_text,
            reply_messages=reply.reply_messages,
            group_announcement=reply.group_announcement,
            metadata=reply.reply_metadata,
        )

    @staticmethod
    def _result_from_turn_outcome(outcome: FlorenceTurnOutcome) -> FlorenceMessagingIngressResult:
        metadata = dict(outcome.metadata)
        if outcome.no_reply_reason:
            metadata.setdefault("no_reply_reason", outcome.no_reply_reason)
        return FlorenceMessagingIngressResult(
            reply_text=outcome.reply_messages[0] if len(outcome.reply_messages) == 1 else None,
            reply_messages=outcome.reply_messages if len(outcome.reply_messages) != 1 else (),
            reply_metadata=metadata,
            group_announcement=outcome.group_announcement,
            consumed=outcome.consumed,
        )

    def _handle_shared_feedback(self, resolved: FlorenceResolvedInboundMessage) -> FlorenceProtocolReply | None:
        feedback = parse_review_feedback(resolved.message.body)
        if feedback is None or feedback.kind not in {
            ReviewFeedbackKind.LESS_PROACTIVE,
            ReviewFeedbackKind.MORE_PROACTIVE,
            ReviewFeedbackKind.DISABLE_MODULE,
        }:
            return None
        preference = self._record_shared_feedback_preference(resolved=resolved, feedback=feedback)
        reply = (
            "Got it. I’ll keep shared Florence updates shorter and more action-focused."
            if feedback.kind == ReviewFeedbackKind.LESS_PROACTIVE
            else "Got it. I’ll be more proactive in the family chat when something is clearly useful."
            if feedback.kind == ReviewFeedbackKind.MORE_PROACTIVE
            else "Got it. I’ll keep that module off for shared household prompts."
        )
        return FlorenceProtocolReply(
            reply_text=reply,
            reply_metadata={
                "review_feedback_kind": feedback.kind.value,
                "recorded_preference_id": preference.id,
                "feedback_scope": "shared_household",
            },
            consumed=True,
        )

    def _record_shared_feedback_preference(
        self,
        *,
        resolved: FlorenceResolvedInboundMessage,
        feedback: ParsedReviewFeedback,
    ):
        if feedback.kind == ReviewFeedbackKind.LESS_PROACTIVE:
            label = "Florence shared update style"
            value = "Keep shared household updates shorter and action-focused."
            category = "operating_preference"
        elif feedback.kind == ReviewFeedbackKind.MORE_PROACTIVE:
            label = "Florence shared proactivity"
            value = "Be more proactive in the family group when an item is clearly useful, timely, and safe."
            category = "operating_preference"
        else:
            module = feedback.module_hint or "requested module"
            label = f"Disabled shared module: {module}"
            value = f"Do not proactively run or suggest the {module} module in shared household prompts unless a parent asks."
            category = "automation_boundary"
        return FlorenceHouseholdManagerService(self.store).record_preference(
            household_id=resolved.household_id,
            label=label,
            value=value,
            category=category,
            member_id=None,
            recorded_by_member_id=resolved.member_id,
            channel_id=resolved.channel_id,
            metadata={
                "review_feedback_kind": feedback.kind.value,
                "raw_feedback_text": feedback.raw_text,
                "module_hint": feedback.module_hint,
                "feedback_scope": "shared_household",
            },
        )
