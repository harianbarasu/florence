"""Onboarding and first-sync protocol for Florence DM ingress."""

from __future__ import annotations

from typing import Any, Callable

from florence.onboarding import OnboardingStage

from florence.messaging.protocol_types import FlorenceProtocolReply


class FlorenceSetupProtocol:
    """Onboarding and first-sync protocol handling before normal Hermes-first chat."""

    def __init__(
        self,
        *,
        onboarding_service,
        on_complete: Callable[[str, str, str], None],
        handle_onboarding_turn: Callable[[str, str, str, dict[str, object]], FlorenceProtocolReply | None],
        handle_sync_waiting_turn: Callable[[str, str, str, str, bool], FlorenceProtocolReply],
    ) -> None:
        self.onboarding_service = onboarding_service
        self.on_complete = on_complete
        self.handle_onboarding_turn = handle_onboarding_turn
        self.handle_sync_waiting_turn = handle_sync_waiting_turn

    def handle_incomplete_turn(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        thread_id: str,
        session,
        text: str,
    ) -> FlorenceProtocolReply:
        connect_done_result = self._handle_google_connect_done_if_applicable(
            household_id=household_id,
            member_id=member_id,
            channel_id=channel_id,
            thread_id=thread_id,
            session=session,
            text=text,
        )
        if connect_done_result is not None:
            return connect_done_result

        onboarding_chat_result = self.handle_onboarding_turn(
            household_id,
            channel_id,
            member_id,
            self._build_onboarding_payload(session=session, text=text),
        )
        if onboarding_chat_result is not None:
            return onboarding_chat_result

        if session.google_connected:
            return self.handle_sync_waiting_turn(
                household_id,
                channel_id,
                member_id,
                text,
                False,
            )
        return self._repeat_onboarding_prompt(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )

    def _handle_google_connect_done_if_applicable(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        thread_id: str,
        session,
        text: str,
    ) -> FlorenceProtocolReply | None:
        normalized = " ".join(text.strip().lower().split())
        if session.stage != OnboardingStage.CONNECT_GOOGLE or normalized != "done":
            return None
        member_connections = self.onboarding_service.store.list_google_connections(
            household_id=household_id,
            member_id=member_id,
        )
        if not member_connections:
            return self._messages_reply(
                self.onboarding_service.get_google_connect_retry_messages(
                    household_id=household_id,
                    member_id=member_id,
                    thread_id=thread_id,
                )
            )
        transition = self.onboarding_service.record_google_connected(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )
        if transition.state.is_complete:
            self.on_complete(household_id, member_id, channel_id)
        return self._messages_reply(
            self.onboarding_service.get_transition_messages(
                transition,
                previous_stage=session.stage,
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
            )
        )

    def _build_onboarding_payload(self, *, session, text: str) -> dict[str, Any]:
        prompt = self.onboarding_service.get_prompt(
            household_id=session.household_id,
            member_id=session.member_id,
            thread_id=session.thread_id,
        )
        return {
            "user_message": text,
            "stage": session.stage.value,
            "thread_id": session.thread_id,
            "google_connected": bool(session.google_connected),
            "parent_display_name": session.parent_display_name,
            "child_names": list(session.child_names),
            "child_profiles": [dict(profile) for profile in session.child_profiles],
            "current_child_name": session.current_child_name,
            "next_prompt": prompt.text if prompt is not None else None,
        }

    def _repeat_onboarding_prompt(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
    ) -> FlorenceProtocolReply:
        return self._messages_reply(self.onboarding_service.get_prompt_messages(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        ))

    @staticmethod
    def _messages_reply(messages: tuple[str, ...]) -> FlorenceProtocolReply:
        return FlorenceProtocolReply(
            reply_text=messages[0] if messages else None,
            reply_messages=messages,
            consumed=True,
        )
