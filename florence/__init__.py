"""Florence product layer.

This package holds the household-specific product contracts and services that
sit around Hermes core. Hermes remains the agent/runtime engine; Florence owns
household identity, onboarding, Google integrations, messaging transports like
Linq, and shared state.
"""

from florence.contracts import (
    CandidateState,
    ChannelMessage,
    ChannelMessageRole,
    Channel,
    ChannelType,
    ChildProfile,
    GoogleConnection,
    GoogleSourceKind,
    Household,
    HouseholdContext,
    HouseholdEvent,
    HouseholdEventStatus,
    HouseholdProfileItem,
    HouseholdProfileKind,
    HouseholdStatus,
    IdentityKind,
    ImportedCandidate,
    Member,
    MemberIdentity,
    MemberRole,
)

__all__ = [
    "CandidateState",
    "ChannelMessage",
    "ChannelMessageRole",
    "Channel",
    "ChannelType",
    "ChildProfile",
    "GoogleConnection",
    "GoogleSourceKind",
    "Household",
    "HouseholdContext",
    "HouseholdEvent",
    "HouseholdEventStatus",
    "HouseholdProfileItem",
    "HouseholdProfileKind",
    "HouseholdStatus",
    "IdentityKind",
    "ImportedCandidate",
    "Member",
    "MemberIdentity",
    "MemberRole",
]
