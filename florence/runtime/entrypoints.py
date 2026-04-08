"""Florence transport and OAuth entrypoints."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

from florence.config import FlorenceGoogleRuntimeConfig
from florence.linq import parse_linq_payload
from florence.messaging import (
    FlorenceInboundMessage,
    FlorenceMessagingIngressService,
    FlorenceResolvedInboundMessage,
)
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime.candidate_review import FlorenceCandidateReviewService
from florence.runtime.google_services import FlorenceGoogleAccountLinkService
from florence.runtime.household_manager import FlorenceHouseholdManagerService
from florence.runtime.household_merge import FlorenceHouseholdMergeService
from florence.runtime.onboarding_service import FlorenceOnboardingSessionService
from florence.runtime.resolver import FlorenceIdentityResolver
from florence.sendblue import parse_sendblue_payload
from florence.state import FlorenceStateDB

IGNORED_LINQ_EVENT_TYPES = {
    "message.sent",
    "message.delivered",
    "message.read",
    "message.failed",
}

IGNORABLE_LINQ_PARSE_ERRORS = {
    "linq_chat_id_required",
    "linq_message_id_required",
    "linq_sender_handle_required",
}

IGNORABLE_SENDBLUE_PARSE_ERRORS = {
    "sendblue_thread_id_required",
    "sendblue_message_id_required",
    "sendblue_sender_handle_required",
}

@dataclass(slots=True)
class FlorenceEntrypointResult:
    reply_text: str | None = None
    reply_messages: tuple[str, ...] = field(default_factory=tuple)
    group_announcement: str | None = None
    consumed: bool = False
    household_id: str | None = None
    member_id: str | None = None
    channel_id: str | None = None
    error: str | None = None


class FlorenceEntrypointService:
    """Composable Florence entrypoints around persisted Florence services."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        google_oauth: FlorenceGoogleRuntimeConfig | None = None,
        household_chat_service: FlorenceHouseholdChatService,
        household_manager_service: FlorenceHouseholdManagerService | None = None,
        household_merge_service: FlorenceHouseholdMergeService | None = None,
    ):
        self.store = store
        self.candidate_review_service = FlorenceCandidateReviewService(store)
        self.onboarding_service = FlorenceOnboardingSessionService(
            store,
            candidate_review_service=self.candidate_review_service,
        )
        household_merge_service = household_merge_service or FlorenceHouseholdMergeService(store)
        self.identity_resolvers = {
            "linq": FlorenceIdentityResolver(
                store,
                provider="linq",
                household_merge_service=household_merge_service,
            ),
            "sendblue": FlorenceIdentityResolver(
                store,
                provider="sendblue",
                household_merge_service=household_merge_service,
            ),
        }
        self.household_chat_service = household_chat_service
        self.household_manager_service = household_manager_service or FlorenceHouseholdManagerService(store)
        self.google_account_link_service = (
            FlorenceGoogleAccountLinkService(
                store,
                self.onboarding_service,
                client_id=google_oauth.client_id,
                client_secret=google_oauth.client_secret,
                redirect_uri=google_oauth.redirect_uri,
                state_secret=google_oauth.state_secret,
            )
            if google_oauth is not None
            else None
        )
        self.onboarding_service.set_link_url_builder(
            None
            if self.google_account_link_service is None
            else lambda household_id, member_id, thread_id: self.google_account_link_service.build_connect_link(
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
            ).url
        )
        self.ingress = FlorenceMessagingIngressService(
            store,
            self.onboarding_service,
            self.candidate_review_service,
            household_chat_service=self.household_chat_service,
            household_manager_service=self.household_manager_service,
        )

    def handle_linq_payload(self, payload: dict[str, object]) -> FlorenceEntrypointResult:
        try:
            inbound = parse_linq_payload(payload)
        except ValueError as exc:
            if str(exc) in IGNORABLE_LINQ_PARSE_ERRORS:
                return FlorenceEntrypointResult(consumed=False, error=str(exc))
            raise
        if inbound.event_type and inbound.event_type.strip().lower() in IGNORED_LINQ_EVENT_TYPES:
            return FlorenceEntrypointResult(consumed=False)
        if inbound.metadata.get("service") and str(inbound.metadata["service"]).lower() != "imessage":
            return FlorenceEntrypointResult(consumed=False, error="linq_non_imessage_ignored")
        return self._handle_transport_message(
            provider="linq",
            thread_id=inbound.thread_id,
            sender_handle=inbound.sender_handle,
            is_group_chat=inbound.is_group_chat,
            participant_handles=list(inbound.participant_handles),
            inbound_message=inbound,
        )

    def handle_sendblue_payload(self, payload: dict[str, object]) -> FlorenceEntrypointResult:
        try:
            inbound = parse_sendblue_payload(payload)
        except ValueError as exc:
            if str(exc) in IGNORABLE_SENDBLUE_PARSE_ERRORS:
                return FlorenceEntrypointResult(consumed=False, error=str(exc))
            raise
        if inbound.is_from_me:
            return FlorenceEntrypointResult(consumed=False)
        return self._handle_transport_message(
            provider="sendblue",
            thread_id=inbound.thread_id,
            sender_handle=inbound.sender_handle,
            is_group_chat=inbound.is_group_chat,
            participant_handles=list(inbound.participant_handles),
            inbound_message=inbound,
        )

    def _handle_transport_message(
        self,
        *,
        provider: str,
        thread_id: str,
        sender_handle: str,
        is_group_chat: bool,
        participant_handles: list[str],
        inbound_message,
    ) -> FlorenceEntrypointResult:
        resolver = self.identity_resolvers[provider]
        effective_participant_handles = list(participant_handles)
        if provider == "sendblue":
            sendblue_number = str(inbound_message.metadata.get("sendblue_number") or "").strip()
            if sendblue_number:
                effective_participant_handles = [
                    handle
                    for handle in effective_participant_handles
                    if str(handle).strip() and str(handle).strip() != sendblue_number
                ]
        if is_group_chat:
            resolved = resolver.resolve_group_message(
                sender_handle=sender_handle,
                participant_handles=effective_participant_handles,
                thread_external_id=thread_id,
            )
            if resolved is None:
                return FlorenceEntrypointResult(
                    reply_text=(
                        "Hi, I’m Florence. Before I jump into a family group, one parent should message me directly first so I can set up the household."
                    ),
                    consumed=True,
                    error="unresolved_group_household",
                )
            if provider == "sendblue":
                metadata = dict(resolved.channel.metadata)
                group_id = str(inbound_message.metadata.get("group_id") or "").strip()
                sendblue_number = str(inbound_message.metadata.get("sendblue_number") or "").strip()
                if group_id:
                    metadata["group_id"] = group_id
                if sendblue_number:
                    metadata["sendblue_number"] = sendblue_number
                if effective_participant_handles:
                    metadata["participant_handles"] = list(
                        dict.fromkeys(handle for handle in effective_participant_handles if handle)
                    )
                resolved = replace(
                    resolved,
                    channel=self.store.upsert_channel(replace(resolved.channel, metadata=metadata)),
                )
        else:
            try:
                resolved = resolver.resolve_direct_message(
                    sender_handle=sender_handle,
                    thread_external_id=thread_id,
                )
            except ValueError as exc:
                if str(exc) == "ambiguous_existing_household_for_identity":
                    return FlorenceEntrypointResult(
                        reply_text=(
                            "I found more than one possible household for this number, so I didn't create a new one. "
                            "Message me in the right family group first or have the other parent invite me there so I can link this DM correctly."
                        ),
                        consumed=True,
                        error=str(exc),
                    )
                raise

        member_id = resolved.member.id if resolved.member is not None else None
        result = self.ingress.handle_message(
            FlorenceResolvedInboundMessage(
                household_id=resolved.household.id,
                member_id=member_id,
                channel_id=resolved.channel.id,
                thread_id=resolved.channel.provider_channel_id,
                message=inbound_message,
            )
        )
        reply_text = result.reply_text
        reply_messages = result.reply_messages
        consumed = result.consumed
        if resolved.private_dm_link_intro:
            existing_messages = reply_messages or ((reply_text,) if reply_text else ())
            reply_messages = (resolved.private_dm_link_intro, *existing_messages)
            reply_text = None
            consumed = True
        group_announcement = result.group_announcement
        if resolved.group_private_dm_hint:
            group_announcement = (
                f"{group_announcement}\n\n{resolved.group_private_dm_hint}"
                if group_announcement
                else resolved.group_private_dm_hint
            )
        current_channel = self.store.get_channel(resolved.channel.id) or resolved.channel
        current_member = self.store.get_member(member_id) if member_id is not None else None
        return FlorenceEntrypointResult(
            reply_text=reply_text,
            reply_messages=reply_messages,
            group_announcement=group_announcement,
            consumed=consumed or bool(group_announcement),
            household_id=current_channel.household_id,
            member_id=current_member.id if current_member is not None else member_id,
            channel_id=current_channel.id,
        )
