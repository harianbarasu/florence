"""Shared household visibility and scope helpers for Florence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from florence.contracts import (
    ChannelType,
    GoogleSourceKind,
    HouseholdSourceVisibility,
)
from florence.source_rules import request_matches_shared_gmail_rule
from florence.state import FlorenceStateDB


@dataclass(slots=True)
class FlorenceConversationScope:
    channel_type: ChannelType | None
    scope: str
    actor_member_id: str | None

    @property
    def is_private_parent_dm(self) -> bool:
        return self.channel_type == ChannelType.PARENT_DM

    @property
    def is_shared_household_group(self) -> bool:
        return self.channel_type == ChannelType.HOUSEHOLD_GROUP

    @property
    def private_review_available(self) -> bool:
        return bool(self.is_private_parent_dm and self.actor_member_id)


@dataclass(slots=True)
class FlorenceGoogleInboxScope:
    search_scope: str
    scope_reason: str
    connections: list[Any]
    error: str | None = None


def resolve_conversation_scope(
    store: FlorenceStateDB,
    *,
    channel_id: str,
    actor_member_id: str | None,
) -> FlorenceConversationScope:
    channel = store.get_channel(channel_id)
    channel_type = channel.channel_type if channel is not None else None
    scope = (
        "private_parent_dm"
        if channel_type == ChannelType.PARENT_DM
        else "shared_household_group"
        if channel_type == ChannelType.HOUSEHOLD_GROUP
        else "system"
    )
    return FlorenceConversationScope(
        channel_type=channel_type,
        scope=scope,
        actor_member_id=actor_member_id,
    )


def build_scope_model_lines(*, scope: FlorenceConversationScope) -> list[str]:
    lines = [
        "Household scope model:",
        "Shared household scope: facts, plans, reminders, meals, grocery items, routines, and events that both parents can act on together.",
        "Private parent scope: DM-only context, mental-load triage, emotional processing, individually scoped concerns, and anything the parent has not chosen to share.",
        "Tentative scope: provisional plans or not-yet-confirmed details that Florence may track, but must label clearly and must not present as settled shared fact.",
        "Unreviewed imports or unresolved source classifications are not shared household facts yet.",
    ]
    if scope.is_private_parent_dm:
        lines.append(
            "Current scope: private parent DM. Florence may use shared household context plus this parent's private context here."
        )
        lines.append(
            "If a private DM contains logistics that should become shared, convert it into structured household state or a group-safe summary instead of repeating the raw DM."
        )
    elif scope.is_shared_household_group:
        lines.append(
            "Current scope: shared household group. Florence may use shared household context here, but must not reveal private DM-only context unless it was explicitly promoted or already persisted as shared household state."
        )
        lines.append(
            "In the group, tentative planning is allowed when clearly labeled, but private reasoning and private source details stay out."
        )
    return lines


def resolve_google_inbox_scope(
    store: FlorenceStateDB,
    *,
    household_id: str,
    channel_id: str,
    actor_member_id: str | None,
    query: str | None,
    sender: str | None,
    subject: str | None,
) -> FlorenceGoogleInboxScope:
    household_connections = [
        connection
        for connection in store.list_google_connections(household_id=household_id)
        if GoogleSourceKind.GMAIL in connection.connected_scopes
    ]
    shared_rules = store.list_household_source_rules(
        household_id=household_id,
        source_kind=GoogleSourceKind.GMAIL,
        visibility=HouseholdSourceVisibility.SHARED,
    )
    scope = resolve_conversation_scope(
        store,
        channel_id=channel_id,
        actor_member_id=actor_member_id,
    )
    search_shared_household = request_matches_shared_gmail_rule(
        shared_rules,
        sender=sender,
        query=query,
        subject=subject,
    )

    if scope.is_shared_household_group:
        if not search_shared_household:
            return FlorenceGoogleInboxScope(
                search_scope="group_requires_shared_scope",
                scope_reason="group_chat_disallows_private_inbox_search",
                connections=[],
                error=(
                    "Inbox search from the family group only uses shared household scope. "
                    "Use a private DM for private inbox lookup, or ask here about a shared school, camp, teacher, or sender."
                ),
            )
        return FlorenceGoogleInboxScope(
            search_scope="shared_household",
            scope_reason="matched_shared_source_rule",
            connections=household_connections,
        )

    member_connections: list[Any] = []
    if actor_member_id:
        member_connections = [
            connection
            for connection in store.list_google_connections(
                household_id=household_id,
                member_id=actor_member_id,
            )
            if GoogleSourceKind.GMAIL in connection.connected_scopes
        ]
    if search_shared_household:
        return FlorenceGoogleInboxScope(
            search_scope="shared_household",
            scope_reason="matched_shared_source_rule",
            connections=household_connections,
        )
    if not member_connections and (actor_member_id is None or len(household_connections) == 1):
        return FlorenceGoogleInboxScope(
            search_scope="available_connected_inbox",
            scope_reason="single_connected_inbox_fallback",
            connections=household_connections,
        )
    return FlorenceGoogleInboxScope(
        search_scope="private_parent",
        scope_reason="current_parent_dm",
        connections=member_connections,
    )
