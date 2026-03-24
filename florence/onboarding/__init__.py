"""Deterministic DM-first onboarding state and flows for Florence."""

from florence.onboarding.flow import (
    OnboardingPrompt,
    OnboardingTransition,
    apply_activity_basics,
    apply_child_names,
    apply_parent_name,
    apply_school_basics,
    build_onboarding_prompt,
    mark_google_connected,
    mark_group_activated,
    sync_onboarding_stage,
)
from florence.onboarding.state import OnboardingStage, OnboardingState

__all__ = [
    "OnboardingPrompt",
    "OnboardingStage",
    "OnboardingState",
    "OnboardingTransition",
    "apply_activity_basics",
    "apply_child_names",
    "apply_parent_name",
    "apply_school_basics",
    "build_onboarding_prompt",
    "mark_google_connected",
    "mark_group_activated",
    "sync_onboarding_stage",
]
