"""Identity and channel resolution for Florence transports."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from florence.contracts import (
    Channel,
    ChannelType,
    Household,
    HouseholdStatus,
    IdentityKind,
    Member,
    MemberIdentity,
    MemberRole,
)
from florence.onboarding import OnboardingStage
from florence.state import FlorenceStateDB


def normalize_identity_value(kind: IdentityKind, value: str) -> str:
    raw = value.strip()
    if kind == IdentityKind.IMESSAGE_EMAIL:
        return raw.lower()

    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return raw.lower()
    if len(digits) == 10:
        return f"+1{digits}"
    if raw.startswith("+"):
        return f"+{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return f"+{digits}"


def infer_identity_kind(handle: str) -> IdentityKind:
    return IdentityKind.IMESSAGE_EMAIL if "@" in handle else IdentityKind.PHONE


def display_name_from_handle(handle: str) -> str:
    normalized = handle.split("@")[0] if "@" in handle else handle.lstrip("+")
    digits = re.sub(r"\D+", "", normalized)
    if digits and not re.search(r"[A-Za-z]", normalized):
        return digits

    cleaned = re.sub(r"[^A-Za-z0-9]+", " ", normalized).strip()
    if not cleaned:
        return "Parent"
    return " ".join(part.capitalize() for part in cleaned.split()[:3])


def household_name_from_display_name(display_name: str) -> str:
    first = display_name.split()[0] if display_name.strip() else "Family"
    return f"{first}'s household"


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256(":".join(parts).encode("utf-8")).hexdigest()[:20]
    return f"{prefix}_{digest}"


@dataclass(slots=True)
class FlorenceResolvedTransportContext:
    household: Household
    member: Member | None
    channel: Channel


class FlorenceIdentityResolver:
    """Creates and resolves Florence households, members, identities, and channels."""

    def __init__(self, store: FlorenceStateDB, *, provider: str = "bluebubbles"):
        self.store = store
        self.provider = provider

    def resolve_direct_message(self, *, sender_handle: str, thread_external_id: str) -> FlorenceResolvedTransportContext:
        kind = infer_identity_kind(sender_handle)
        normalized = normalize_identity_value(kind, sender_handle)
        member = self.store.find_member_by_identity(kind=kind, normalized_value=normalized)
        if member is None:
            member = self._create_household_for_new_sender(sender_handle, normalized, kind)

        household = self.store.get_household(member.household_id)
        if household is None:
            raise ValueError("member_household_missing")

        channel = self.store.get_channel_by_provider_id(
            provider=self.provider,
            provider_channel_id=thread_external_id,
        )
        if channel is None:
            channel = self.store.upsert_channel(
                Channel(
                    id=_stable_id("chan", household.id, self.provider, thread_external_id),
                    household_id=household.id,
                    provider=self.provider,
                    provider_channel_id=thread_external_id,
                    channel_type=ChannelType.PARENT_DM,
                    title=member.display_name,
                    metadata={"sender_handle": normalized},
                )
            )
        return FlorenceResolvedTransportContext(household=household, member=member, channel=channel)

    def resolve_group_message(
        self,
        *,
        sender_handle: str,
        participant_handles: list[str] | None,
        thread_external_id: str,
    ) -> FlorenceResolvedTransportContext | None:
        existing_channel = self.store.get_channel_by_provider_id(
            provider=self.provider,
            provider_channel_id=thread_external_id,
        )
        if existing_channel is not None:
            household = self.store.get_household(existing_channel.household_id)
            if household is None:
                return None
            member = self._find_member_in_household_by_handle(household.id, sender_handle)
            return FlorenceResolvedTransportContext(household=household, member=member, channel=existing_channel)

        matching_households: dict[str, list[Member]] = {}
        handles = [sender_handle, *(participant_handles or [])]
        for handle in handles:
            kind = infer_identity_kind(handle)
            normalized = normalize_identity_value(kind, handle)
            member = self.store.find_member_by_identity(kind=kind, normalized_value=normalized)
            if member is None:
                continue
            matching_households.setdefault(member.household_id, []).append(member)

        if len(matching_households) != 1:
            return None

        household_id, matched_members = next(iter(matching_households.items()))
        household = self.store.get_household(household_id)
        if household is None:
            return None

        sender_member = self._find_member_in_household_by_handle(household_id, sender_handle)
        if sender_member is None:
            sender_member = self._find_pending_group_activation_member(household_id)

        title_handles = [
            display_name_from_handle(handle)
            for handle in [sender_handle, *participant_handles]
            if handle.strip()
        ]
        title = ", ".join(dict.fromkeys(title_handles)) or "Family group"
        channel = self.store.upsert_channel(
            Channel(
                id=_stable_id("chan", household.id, self.provider, thread_external_id),
                household_id=household.id,
                provider=self.provider,
                provider_channel_id=thread_external_id,
                channel_type=ChannelType.HOUSEHOLD_GROUP,
                title=title,
                metadata={
                    "participant_handles": [
                        normalize_identity_value(infer_identity_kind(handle), handle)
                        for handle in (participant_handles or [])
                    ],
                },
            )
        )
        return FlorenceResolvedTransportContext(household=household, member=sender_member, channel=channel)

    def _create_household_for_new_sender(
        self,
        sender_handle: str,
        normalized_handle: str,
        kind: IdentityKind,
    ) -> Member:
        display_name = display_name_from_handle(sender_handle)
        household = self.store.upsert_household(
            Household(
                id=_stable_id("hh", normalized_handle),
                name=household_name_from_display_name(display_name),
                timezone="America/Los_Angeles",
                status=HouseholdStatus.ACTIVE,
            )
        )
        member = self.store.upsert_member(
            Member(
                id=_stable_id("mem", household.id, normalized_handle),
                household_id=household.id,
                display_name=display_name,
                role=MemberRole.ADMIN,
            )
        )
        self.store.upsert_member_identity(
            MemberIdentity(
                id=_stable_id("ident", kind.value, normalized_handle),
                member_id=member.id,
                kind=kind,
                value=sender_handle.strip(),
                normalized_value=normalized_handle,
            )
        )
        return self.store.get_member(member.id) or member

    def _find_member_in_household_by_handle(self, household_id: str, handle: str) -> Member | None:
        kind = infer_identity_kind(handle)
        normalized = normalize_identity_value(kind, handle)
        member = self.store.find_member_by_identity(kind=kind, normalized_value=normalized)
        if member is None or member.household_id != household_id:
            return None
        return member

    def _find_pending_group_activation_member(self, household_id: str) -> Member | None:
        for session in self.store.list_onboarding_sessions(household_id):
            if session.group_channel_id:
                continue
            if session.stage != OnboardingStage.ACTIVATE_GROUP:
                continue
            member = self.store.get_member(session.member_id)
            if member is not None:
                return member
        return None
