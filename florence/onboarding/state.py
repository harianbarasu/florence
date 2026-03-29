"""Onboarding state contracts for Florence."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class OnboardingVariant(StrEnum):
    CONCIERGE = "concierge"
    HYBRID = "hybrid"


class OnboardingStage(StrEnum):
    COLLECT_PARENT_NAME = "collect_parent_name"
    COLLECT_HOUSEHOLD_MEMBERS = "collect_household_members"
    CONNECT_GOOGLE = "connect_google"
    COLLECT_CHILD_NAMES = "collect_child_names"
    COLLECT_SCHOOL_BASICS = "collect_school_basics"
    COLLECT_ACTIVITY_BASICS = "collect_activity_basics"
    COLLECT_HOUSEHOLD_OPERATIONS = "collect_household_operations"
    COLLECT_NUDGE_PREFERENCES = "collect_nudge_preferences"
    COLLECT_OPERATING_PREFERENCES = "collect_operating_preferences"
    ACTIVATE_GROUP = "activate_group"
    COMPLETE = "complete"


def _metadata_list(metadata: dict[str, object], key: str) -> list[str]:
    raw = metadata.get(key)
    if not isinstance(raw, list):
        return []
    values: list[str] = []
    for item in raw:
        cleaned = " ".join(str(item).split()).strip()
        if cleaned:
            values.append(cleaned)
    return values


def _metadata_text(metadata: dict[str, object], key: str) -> str | None:
    raw = metadata.get(key)
    if raw is None:
        return None
    cleaned = " ".join(str(raw).split()).strip()
    return cleaned or None


@dataclass(slots=True)
class OnboardingState:
    """Minimal onboarding snapshot for Florence V1."""

    household_id: str
    member_id: str
    thread_id: str
    stage: OnboardingStage = OnboardingStage.COLLECT_PARENT_NAME
    parent_display_name: str | None = None
    google_connected: bool = False
    child_names: list[str] = field(default_factory=list)
    school_labels: list[str] = field(default_factory=list)
    activity_labels: list[str] = field(default_factory=list)
    school_basics_collected: bool = False
    activity_basics_collected: bool = False
    group_channel_id: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    @property
    def variant(self) -> OnboardingVariant:
        raw = str(self.metadata.get("variant") or "").strip().lower()
        try:
            return OnboardingVariant(raw)
        except ValueError:
            return OnboardingVariant.HYBRID

    @property
    def household_members(self) -> list[str]:
        return _metadata_list(self.metadata, "household_members")

    @property
    def child_details(self) -> list[str]:
        return _metadata_list(self.metadata, "child_details")

    @property
    def household_operations(self) -> list[str]:
        return _metadata_list(self.metadata, "household_operations")

    @property
    def nudge_preferences(self) -> str | None:
        return _metadata_text(self.metadata, "nudge_preferences")

    @property
    def operating_preferences(self) -> str | None:
        return _metadata_text(self.metadata, "operating_preferences")

    @property
    def is_grounded_for_google_matching(self) -> bool:
        """Return True when Google candidate relevance can be trusted."""
        return bool(self.child_names and (self.school_labels or self.activity_labels))

    @property
    def is_complete(self) -> bool:
        basics_ready = bool(
            self.google_connected
            and self.child_names
            and self.school_basics_collected
            and self.activity_basics_collected
        )
        if not basics_ready:
            return False
        if self.variant == OnboardingVariant.CONCIERGE and not self.household_members:
            return False
        return True
