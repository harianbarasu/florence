"""Florence product layer.

This package holds the household-specific product contracts and services that
sit around Hermes core. Hermes remains the agent/runtime engine; Florence owns
household identity, onboarding, Google integrations, messaging transports like
Linq, and shared state.
"""

from florence.contracts import (
    AppChatMessage,
    AppChatMessageRole,
    AppChatScope,
    AppChatThread,
    CandidateState,
    Channel,
    ChannelType,
    ChildProfile,
    GoogleConnection,
    GoogleSourceKind,
    Household,
    HouseholdContext,
    HouseholdEvent,
    HouseholdEventStatus,
    HouseholdStatus,
    IdentityKind,
    ImportedCandidate,
    Member,
    MemberIdentity,
    MemberRole,
)

__all__ = [
    "AppChatMessage",
    "AppChatMessageRole",
    "AppChatScope",
    "AppChatThread",
    "CandidateState",
    "Channel",
    "ChannelType",
    "ChildProfile",
    "GoogleConnection",
    "GoogleSourceKind",
    "Household",
    "HouseholdContext",
    "HouseholdEvent",
    "HouseholdEventStatus",
    "HouseholdStatus",
    "IdentityKind",
    "ImportedCandidate",
    "Member",
    "MemberIdentity",
    "MemberRole",
]
