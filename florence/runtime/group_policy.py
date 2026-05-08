"""Group-chat turn policy for Florence."""

from __future__ import annotations

from florence.turns import FlorenceTurnDisposition, FlorenceTurnEnvelope, FlorenceTurnOutcome


class FlorenceGroupTurnPolicy:
    """Classify group-chat outcomes before transport delivery."""

    _LOW_SIGNAL_MESSAGES = {
        "ok",
        "okay",
        "k",
        "kk",
        "yes",
        "yep",
        "yeah",
        "no",
        "nope",
        "thanks",
        "thank you",
        "ty",
        "got it",
        "sounds good",
        "👍",
    }

    def obvious_chatter_outcome(self, envelope: FlorenceTurnEnvelope) -> FlorenceTurnOutcome | None:
        normalized = " ".join(envelope.message_text.lower().split()).strip()
        if not normalized:
            return FlorenceTurnOutcome(
                disposition=FlorenceTurnDisposition.NO_REPLY,
                no_reply_reason="group_empty_message",
                metadata={"florence_turn_id": envelope.turn_id},
            )
        if normalized in self._LOW_SIGNAL_MESSAGES:
            return FlorenceTurnOutcome(
                disposition=FlorenceTurnDisposition.NO_REPLY,
                no_reply_reason="group_low_signal_chatter",
                metadata={"florence_turn_id": envelope.turn_id},
            )
        return None

    def no_actor_outcome(self, envelope: FlorenceTurnEnvelope) -> FlorenceTurnOutcome:
        return FlorenceTurnOutcome(
            disposition=FlorenceTurnDisposition.NO_REPLY,
            no_reply_reason="group_actor_unresolved",
            metadata={"florence_turn_id": envelope.turn_id},
        )

    def reply_outcome(
        self,
        envelope: FlorenceTurnEnvelope,
        *,
        reply_text: str | None = None,
        reply_messages: tuple[str, ...] = (),
        group_announcement: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> FlorenceTurnOutcome:
        messages = reply_messages or ((reply_text,) if reply_text else ())
        disposition = (
            FlorenceTurnDisposition.GROUP_ANNOUNCEMENT
            if group_announcement and not messages
            else FlorenceTurnDisposition.REPLY_MULTIPLE
            if len(messages) > 1
            else FlorenceTurnDisposition.REPLY
            if messages
            else FlorenceTurnDisposition.NO_REPLY
        )
        return FlorenceTurnOutcome(
            disposition=disposition,
            reply_messages=messages,
            group_announcement=group_announcement,
            no_reply_reason=None if messages or group_announcement else "group_no_action",
            metadata={
                "florence_turn_id": envelope.turn_id,
                **(metadata or {}),
            },
        )
