"""Onboarding state contracts for Florence."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class OnboardingVariant(StrEnum):
    CONCIERGE = "concierge"
    HYBRID = "hybrid"


class OnboardingStage(StrEnum):
    COLLECT_PARENT_NAME = "collect_parent_name"
    COLLECT_HOUSEHOLD_MEMBERS = "collect_household_members"
    COLLECT_CHILD_NAMES = "collect_child_names"
    COLLECT_CHILD_AGE = "collect_child_age"
    COLLECT_CHILD_SCHOOL = "collect_child_school"
    COLLECT_CHILD_ACTIVITIES = "collect_child_activities"
    CONNECT_GOOGLE = "connect_google"
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


def _metadata_int(metadata: dict[str, object], key: str) -> int | None:
    raw = metadata.get(key)
    if raw is None:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(str(value).split()).strip()
    return cleaned or None


def _clean_text_list(value: object) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        return None
    values: list[str] = []
    for item in value:
        cleaned = _clean_text(item)
        if cleaned:
            values.append(cleaned)
    return values


def _metadata_child_profiles(metadata: dict[str, object]) -> list[dict[str, Any]]:
    raw = metadata.get("child_profiles")
    if not isinstance(raw, list):
        return []
    profiles: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = _clean_text(item.get("name"))
        if not name:
            continue
        profile: dict[str, Any] = {"name": name}
        age = _clean_text(item.get("age"))
        school = _clean_text(item.get("school"))
        activities = _clean_text_list(item.get("activities"))
        if age is not None:
            profile["age"] = age
        if school is not None:
            profile["school"] = school
        if activities is not None:
            profile["activities"] = activities
        profiles.append(profile)
    return profiles


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
        explicit = _metadata_list(self.metadata, "child_details")
        if explicit:
            return explicit
        details: list[str] = []
        for profile in self.child_profiles:
            parts = [str(profile["name"])]
            age = _clean_text(profile.get("age"))
            school = _clean_text(profile.get("school"))
            activities = _clean_text_list(profile.get("activities"))
            if age:
                parts.append(f"age {age}")
            if school:
                parts.append(school)
            if activities is not None and activities:
                parts.append(", ".join(activities))
            details.append(" - ".join(parts))
        return details

    @property
    def child_profiles(self) -> list[dict[str, Any]]:
        return _metadata_child_profiles(self.metadata)

    @property
    def current_child_index(self) -> int:
        index = _metadata_int(self.metadata, "current_child_index")
        if index is None or index < 0:
            return 0
        if not self.child_profiles:
            return 0
        return min(index, max(0, len(self.child_profiles) - 1))

    @property
    def current_child_profile(self) -> dict[str, Any] | None:
        profiles = self.child_profiles
        if not profiles:
            return None
        return profiles[self.current_child_index]

    @property
    def current_child_name(self) -> str | None:
        profile = self.current_child_profile
        if profile is None:
            return None
        return _clean_text(profile.get("name"))

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
        profiles = self.child_profiles
        if not self.google_connected or not profiles:
            return False
        for profile in profiles:
            if not _clean_text(profile.get("age")):
                return False
            if not _clean_text(profile.get("school")):
                return False
            if not isinstance(profile.get("activities"), list):
                return False
        return True
