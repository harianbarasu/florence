"""Florence transport and OAuth entrypoints."""

from __future__ import annotations

from dataclasses import dataclass, field

from florence.linq import parse_linq_payload
from florence.messaging import (
    FlorenceInboundMessage,
    FlorenceMessagingIngressService,
    FlorenceResolvedInboundMessage,
)
from florence.runtime.chat import FlorenceHouseholdChatService
from florence.runtime.resolver import FlorenceIdentityResolver
from florence.runtime.services import (
    FlorenceCandidateReviewService,
    FlorenceGoogleAccountLinkService,
    FlorenceHouseholdQueryService,
    FlorenceOnboardingSessionService,
)
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


@dataclass(slots=True)
class FlorenceGoogleOauthConfig:
    client_id: str
    client_secret: str
    redirect_uri: str
    state_secret: str


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
        google_oauth: FlorenceGoogleOauthConfig | None = None,
        household_chat_model: str | None = None,
        household_chat_max_iterations: int = 6,
        household_chat_provider: str = "auto",
        household_chat_enabled_toolsets: list[str] | tuple[str, ...] | None = None,
        household_chat_disabled_toolsets: list[str] | tuple[str, ...] | None = None,
    ):
        self.store = store
        self.candidate_review_service = FlorenceCandidateReviewService(store)
        self.onboarding_service = FlorenceOnboardingSessionService(
            store,
            candidate_review_service=self.candidate_review_service,
        )
        self.query_service = FlorenceHouseholdQueryService(store)
        self.identity_resolvers = {
            "linq": FlorenceIdentityResolver(store, provider="linq"),
        }
        self.household_chat_service = (
            FlorenceHouseholdChatService(
                store,
                model=household_chat_model,
                max_iterations=household_chat_max_iterations,
                provider=household_chat_provider,
                enabled_toolsets=household_chat_enabled_toolsets,
                disabled_toolsets=household_chat_disabled_toolsets,
            )
            if household_chat_model
            else None
        )
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
        self.ingress = FlorenceMessagingIngressService(
            store,
            self.onboarding_service,
            self.candidate_review_service,
            self.query_service,
            google_account_link_service=self.google_account_link_service,
            household_chat_service=self.household_chat_service,
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
        if is_group_chat:
            resolved = resolver.resolve_group_message(
                sender_handle=sender_handle,
                participant_handles=participant_handles,
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
        else:
            resolved = resolver.resolve_direct_message(
                sender_handle=sender_handle,
                thread_external_id=thread_id,
            )

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
        return FlorenceEntrypointResult(
            reply_text=result.reply_text,
            reply_messages=result.reply_messages,
            group_announcement=result.group_announcement,
            consumed=result.consumed,
            household_id=resolved.household.id,
            member_id=member_id,
            channel_id=resolved.channel.id,
        )

    def handle_google_oauth_callback(self, *, code: str, state: str) -> FlorenceEntrypointResult:
        if self.google_account_link_service is None:
            return FlorenceEntrypointResult(error="google_oauth_not_configured")

        callback = self.google_account_link_service.handle_callback(code=code, raw_state=state)
        review_prompt = self.candidate_review_service.build_next_review_prompt(
            household_id=callback.connection.household_id,
            member_id=callback.connection.member_id,
        )
        reply = callback.onboarding_transition.prompt.text if callback.onboarding_transition.prompt else "Google connected."
        if review_prompt is not None:
            reply = f"{reply}\n\n{review_prompt.text}" if reply else review_prompt.text

        return FlorenceEntrypointResult(
            reply_text=reply,
            consumed=True,
            household_id=callback.connection.household_id,
            member_id=callback.connection.member_id,
        )
