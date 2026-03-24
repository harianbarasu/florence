"""Onboarding state contracts for Florence."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class OnboardingStage(StrEnum):
    COLLECT_PARENT_NAME = "collect_parent_name"
    CONNECT_GOOGLE = "connect_google"
    COLLECT_CHILD_NAMES = "collect_child_names"
    COLLECT_SCHOOL_BASICS = "collect_school_basics"
    COLLECT_ACTIVITY_BASICS = "collect_activity_basics"
    ACTIVATE_GROUP = "activate_group"
    COMPLETE = "complete"


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

    @property
    def is_grounded_for_google_matching(self) -> bool:
        """Return True when Google candidate relevance can be trusted."""
        return bool(self.child_names and (self.school_labels or self.activity_labels))

    @property
    def is_complete(self) -> bool:
        return self.stage == OnboardingStage.COMPLETE
