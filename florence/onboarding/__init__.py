"""Minimal iMessage-first onboarding state and flows for Florence."""

from florence.onboarding.flow import (
    OnboardingPrompt,
    OnboardingTransition,
    apply_child_names,
    apply_child_profile_updates,
    apply_parent_name,
    build_google_connected_syncing_message_sequence,
    build_google_connect_message,
    build_google_connect_message_sequence,
    build_onboarding_prompt,
    build_onboarding_prompt_message_sequence,
    build_onboarding_transition_message_sequence,
    build_onboarding_ready_message_sequence,
    mark_google_connected,
    sync_onboarding_stage,
)
from florence.onboarding.parsing import extract_child_names, split_entries, split_labels, split_names
from florence.onboarding.state import OnboardingStage, OnboardingState, OnboardingVariant

__all__ = [
    "OnboardingPrompt",
    "OnboardingStage",
    "OnboardingState",
    "OnboardingVariant",
    "OnboardingTransition",
    "apply_child_names",
    "apply_child_profile_updates",
    "apply_parent_name",
    "build_google_connected_syncing_message_sequence",
    "build_google_connect_message",
    "build_google_connect_message_sequence",
    "build_onboarding_prompt",
    "build_onboarding_prompt_message_sequence",
    "build_onboarding_transition_message_sequence",
    "build_onboarding_ready_message_sequence",
    "extract_child_names",
    "mark_google_connected",
    "split_entries",
    "split_labels",
    "split_names",
    "sync_onboarding_stage",
]
