"""Core Florence domain contracts.

These contracts intentionally avoid any storage or transport implementation.
They define the basic product-level entities Florence needs in order to wrap
Hermes core with household-aware behavior.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class HouseholdStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"


class MemberRole(StrEnum):
    ADMIN = "admin"
    PARENT = "parent"
    CAREGIVER = "caregiver"
    GRANDPARENT = "grandparent"
    CHILD_LIMITED = "child_limited"


class ChannelType(StrEnum):
    HOUSEHOLD_GROUP = "household_group"
    PARENT_DM = "parent_dm"
    SYSTEM_NOTIFICATIONS = "system_notifications"


class IdentityKind(StrEnum):
    PHONE = "phone"
    IMESSAGE_EMAIL = "imessage_email"


class GoogleSourceKind(StrEnum):
    GMAIL = "gmail"
    GOOGLE_CALENDAR = "google_calendar"


class HouseholdSourceVisibility(StrEnum):
    SHARED = "shared"
    PRIVATE = "private"


class HouseholdSourceMatcherKind(StrEnum):
    GMAIL_FROM_ADDRESS = "gmail_from_address"
    GMAIL_FROM_DOMAIN = "gmail_from_domain"
    GMAIL_SENDER_NAME = "gmail_sender_name"
    GOOGLE_CALENDAR_SUMMARY = "google_calendar_summary"


class CandidateState(StrEnum):
    QUARANTINED = "quarantined"
    PENDING_REVIEW = "pending_review"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"


class HouseholdEventStatus(StrEnum):
    CONFIRMED = "confirmed"
    TENTATIVE = "tentative"
    CANCELLED = "cancelled"


class HouseholdProfileKind(StrEnum):
    SCHOOL = "school"
    ACTIVITY = "activity"
    CONTACT = "contact"
    PLACE = "place"
    PROVIDER = "provider"
    ASSET = "asset"
    PREFERENCE = "preference"


class ChannelMessageRole(StrEnum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"


class HouseholdWorkItemStatus(StrEnum):
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    BLOCKED = "blocked"
    DONE = "done"
    CANCELLED = "cancelled"


class HouseholdRoutineStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"


class HouseholdNudgeStatus(StrEnum):
    SCHEDULED = "scheduled"
    SENT = "sent"
    ACKNOWLEDGED = "acknowledged"
    CANCELLED = "cancelled"


class HouseholdNudgeTargetKind(StrEnum):
    EVENT = "event"
    WORK_ITEM = "work_item"
    ROUTINE = "routine"
    GENERAL = "general"


class HouseholdMealStatus(StrEnum):
    PLANNED = "planned"
    DONE = "done"
    SKIPPED = "skipped"


class HouseholdShoppingItemStatus(StrEnum):
    NEEDED = "needed"
    ORDERED = "ordered"
    PURCHASED = "purchased"
    SKIPPED = "skipped"


class HouseholdBriefingKind(StrEnum):
    MORNING = "morning"
    EVENING = "evening"


@dataclass(slots=True)
class Household:
    id: str
    name: str
    timezone: str
    status: HouseholdStatus = HouseholdStatus.ACTIVE
    settings: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class Member:
    id: str
    household_id: str
    display_name: str
    role: MemberRole
    status: str = "active"
    external_identities: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ChildProfile:
    id: str
    household_id: str
    full_name: str
    birthdate: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class HouseholdProfileItem:
    id: str
    household_id: str
    kind: HouseholdProfileKind
    label: str
    member_id: str | None = None
    child_id: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class Channel:
    id: str
    household_id: str
    provider: str
    provider_channel_id: str
    channel_type: ChannelType
    title: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class MemberIdentity:
    id: str
    member_id: str
    kind: IdentityKind
    value: str
    normalized_value: str


@dataclass(slots=True)
class HouseholdContext:
    household_id: str
    actor_member_id: str | None
    channel_id: str
    visible_child_names: list[str] = field(default_factory=list)
    child_aliases: list[str] = field(default_factory=list)
    school_labels: list[str] = field(default_factory=list)
    school_domains: list[str] = field(default_factory=list)
    school_platforms: list[str] = field(default_factory=list)
    activity_labels: list[str] = field(default_factory=list)
    contact_names: list[str] = field(default_factory=list)
    location_labels: list[str] = field(default_factory=list)

    @property
    def is_grounded_for_google_matching(self) -> bool:
        """Return True once Google-derived relevance signals are usable."""
        return bool(self.visible_child_names and (self.school_labels or self.activity_labels))


@dataclass(slots=True)
class GoogleConnection:
    id: str
    household_id: str
    member_id: str
    email: str
    connected_scopes: tuple[GoogleSourceKind, ...]
    active: bool = True
    access_token: str | None = None
    refresh_token: str | None = None
    access_token_expires_at: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class HouseholdSourceRule:
    id: str
    household_id: str
    source_kind: GoogleSourceKind
    matcher_kind: HouseholdSourceMatcherKind
    matcher_value: str
    visibility: HouseholdSourceVisibility
    label: str | None = None
    created_by_member_id: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class ImportedCandidate:
    id: str
    household_id: str
    member_id: str
    source_kind: GoogleSourceKind
    source_identifier: str
    title: str
    summary: str
    state: CandidateState = CandidateState.QUARANTINED
    confidence_bps: int | None = None
    requires_confirmation: bool = True
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class HouseholdEvent:
    id: str
    household_id: str
    title: str
    starts_at: str | None = None
    ends_at: str | None = None
    timezone: str | None = None
    all_day: bool = False
    location: str | None = None
    description: str | None = None
    source_candidate_id: str | None = None
    status: HouseholdEventStatus = HouseholdEventStatus.CONFIRMED
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class HouseholdWorkItem:
    id: str
    household_id: str
    title: str
    description: str | None = None
    status: HouseholdWorkItemStatus = HouseholdWorkItemStatus.OPEN
    owner_member_id: str | None = None
    child_id: str | None = None
    due_at: str | None = None
    starts_at: str | None = None
    completed_at: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class HouseholdRoutine:
    id: str
    household_id: str
    title: str
    cadence: str
    description: str | None = None
    status: HouseholdRoutineStatus = HouseholdRoutineStatus.ACTIVE
    owner_member_id: str | None = None
    child_id: str | None = None
    next_due_at: str | None = None
    last_completed_at: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class HouseholdNudge:
    id: str
    household_id: str
    target_kind: HouseholdNudgeTargetKind
    target_id: str | None = None
    message: str = ""
    status: HouseholdNudgeStatus = HouseholdNudgeStatus.SCHEDULED
    recipient_member_id: str | None = None
    channel_id: str | None = None
    scheduled_for: str | None = None
    sent_at: str | None = None
    acknowledged_at: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class HouseholdMeal:
    id: str
    household_id: str
    title: str
    meal_type: str
    scheduled_for: str
    description: str | None = None
    status: HouseholdMealStatus = HouseholdMealStatus.PLANNED
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class HouseholdShoppingItem:
    id: str
    household_id: str
    title: str
    list_name: str = "groceries"
    status: HouseholdShoppingItemStatus = HouseholdShoppingItemStatus.NEEDED
    quantity: str | None = None
    unit: str | None = None
    notes: str | None = None
    meal_id: str | None = None
    needed_by: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class ChannelMessage:
    id: str
    household_id: str
    channel_id: str
    sender_role: ChannelMessageRole
    body: str
    sender_member_id: str | None = None
    created_at: float = 0.0
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class PilotEvent:
    id: str
    household_id: str
    event_type: str
    member_id: str | None = None
    channel_id: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)
    created_at: float = 0.0
