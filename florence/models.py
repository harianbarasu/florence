"""Dataclasses shared by Florence modules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class MessageDirection(StrEnum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"


class OutboundDeliveryStatus(StrEnum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    CANCELED = "canceled"


class ReminderStatus(StrEnum):
    PENDING = "pending"
    COMPLETED = "completed"
    SENT = "sent"
    CANCELED = "canceled"
    EXPIRED = "expired"


class SourceDecision(StrEnum):
    SURFACE = "surface"
    STORE_ONLY = "store_only"
    SUPPRESS = "suppress"


class SourcePreferenceKind(StrEnum):
    ALWAYS_SURFACE = "always_surface"
    MUTE = "mute"


class SourceFeedbackKind(StrEnum):
    USEFUL = "useful"
    NOT_USEFUL = "not_useful"


class ConnectedAccountStatus(StrEnum):
    ACTIVE = "active"
    DISABLED = "disabled"


class MemoryKind(StrEnum):
    FACT = "fact"
    PREFERENCE = "preference"
    ROUTINE = "routine"
    CONSTRAINT = "constraint"


class MemberRole(StrEnum):
    PARENT = "parent"
    HELPER = "helper"


class PendingActionStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    CANCELED = "canceled"
    EXPIRED = "expired"
    EXECUTED = "executed"
    FAILED = "failed"


class ActionExecutionStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"


class PrivacyMode(StrEnum):
    MAXIMUM = "maximum"


@dataclass(frozen=True, slots=True)
class MessageAttachment:
    kind: str
    url: str | None = None
    content_type: str | None = None
    filename: str | None = None
    extracted_text: str | None = None
    external_id: str | None = None
    size_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class IncomingMessage:
    chat_id: str
    message_id: str
    sender: str
    text: str
    received_at: datetime
    attachments: tuple[MessageAttachment, ...] = ()


@dataclass(frozen=True, slots=True)
class OutboundMessage:
    chat_id: str
    text: str
    idempotency_key: str
    new_chat_from: str | None = None
    new_chat_to: tuple[str, ...] = ()
    migrate_household_id: str | None = None
    invited_partner_phone: str | None = None
    routine_household_id: str | None = None
    routine_name: str | None = None
    routine_local_date: str | None = None
    briefed_source_item_ids: tuple[str, ...] = ()
    delivery_household_id: str | None = None
    delivery_source_message_id: str | None = None


@dataclass(frozen=True, slots=True)
class Household:
    id: str
    chat_id: str
    timezone: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class HouseholdMember:
    id: str
    household_id: str
    phone: str
    role: MemberRole
    display_name: str | None
    created_at_utc: datetime
    last_seen_at_utc: datetime


@dataclass(frozen=True, slots=True)
class Reminder:
    id: str
    household_id: str
    chat_id: str
    title: str
    due_at_utc: datetime
    created_at_utc: datetime
    status: ReminderStatus
    assignee_member_id: str | None = None
    assignee_label: str | None = None


@dataclass(frozen=True, slots=True)
class SourceItem:
    id: str
    household_id: str
    source_type: str
    title: str
    body: str
    observed_at_utc: datetime
    event_at_utc: datetime | None = None
    sender: str | None = None
    external_id: str | None = None
    connected_account_id: str | None = None


@dataclass(frozen=True, slots=True)
class SourceTriage:
    decision: SourceDecision
    reason: str
    priority: int
    suggested_title: str | None = None
    suggested_due_at_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class BriefingSourceItem:
    id: str
    title: str
    source_type: str
    reason: str | None
    priority: int
    event_at_utc: datetime | None


@dataclass(frozen=True, slots=True)
class SourceReviewSnapshot:
    household_id: str
    total: int
    surfaced: int
    connected_total: int
    connected_surfaced: int
    token_backed_google_total: int
    token_backed_google_surfaced: int
    latest_token_backed_google_synced_at_utc: datetime | None
    stored_only: int
    suppressed: int
    by_reason: dict[str, int]
    recent_surfaced: list[BriefingSourceItem]
    recent_stored: list[BriefingSourceItem]


@dataclass(frozen=True, slots=True)
class SourcePreference:
    id: str
    household_id: str
    phrase: str
    preference: SourcePreferenceKind
    created_at_utc: datetime
    updated_at_utc: datetime
    created_by_member_id: str | None = None


@dataclass(frozen=True, slots=True)
class ConnectedAccount:
    id: str
    household_id: str
    provider: str
    external_account_id: str
    account_label: str | None
    status: ConnectedAccountStatus
    cursor: str | None
    created_at_utc: datetime
    updated_at_utc: datetime
    last_synced_at_utc: datetime | None = None
    sync_failure_count: int = 0
    last_sync_error: str | None = None
    retry_after_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class OAuthState:
    state: str
    provider: str
    chat_id: str
    created_at_utc: datetime
    expires_at_utc: datetime
    account_label: str | None = None
    used_at_utc: datetime | None = None
    return_path: str | None = None


@dataclass(frozen=True, slots=True)
class ConnectedAccountToken:
    connected_account_id: str
    provider: str
    token_ciphertext: str
    scopes: tuple[str, ...]
    created_at_utc: datetime
    updated_at_utc: datetime
    expires_at_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class MemoryRecord:
    id: str
    household_id: str
    kind: MemoryKind
    text: str
    confidence: float
    created_at_utc: datetime
    updated_at_utc: datetime
    subject: str | None = None
    asserted_by_member_id: str | None = None
    source_message_id: str | None = None
    expires_at_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class MemoryExportItem:
    id: str
    kind: MemoryKind
    text: str
    confidence: float
    created_at_utc: datetime
    updated_at_utc: datetime
    subject: str | None
    asserted_by_member_id: str | None
    asserted_by_label: str | None
    source_message_id: str | None
    expires_at_utc: datetime | None


@dataclass(frozen=True, slots=True)
class MemorySnapshot:
    household_id: str
    memories: list[MemoryExportItem]


@dataclass(frozen=True, slots=True)
class HouseholdPrivacy:
    household_id: str
    mode: PrivacyMode
    memory_enabled: bool
    product_analytics_opt_in: bool
    updated_at_utc: datetime


@dataclass(frozen=True, slots=True)
class HouseholdDataSummary:
    household_id: str
    timezone: str
    stopped: bool
    memory_enabled: bool
    product_analytics_opt_in: bool
    member_count: int
    parent_count: int
    helper_count: int
    message_count: int
    active_reminder_count: int
    source_item_count: int
    surfaced_source_item_count: int
    stored_source_item_count: int
    suppressed_source_item_count: int
    connected_account_count: int
    source_preference_count: int
    active_memory_count: int
    pending_action_count: int


@dataclass(frozen=True, slots=True)
class HouseholdReadiness:
    household_id: str
    timezone: str
    parent_count: int
    named_parent_count: int
    child_count: int
    connected_account_count: int
    source_preference_count: int
    memory_count: int
    ready: bool
    missing: list[str]


@dataclass(frozen=True, slots=True)
class PendingAction:
    id: str
    household_id: str
    chat_id: str
    action_type: str
    summary: str
    payload: dict[str, object]
    created_at_utc: datetime
    expires_at_utc: datetime
    status: PendingActionStatus
    created_by_member_id: str | None = None
    resolved_by_member_id: str | None = None
    resolved_at_utc: datetime | None = None


@dataclass(frozen=True, slots=True)
class ActionExecution:
    id: str
    action_id: str
    household_id: str
    status: ActionExecutionStatus
    attempted_at_utc: datetime
    result: dict[str, object]
    error: str | None = None
