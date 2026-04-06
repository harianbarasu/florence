"""Deterministic onboarding flow service for Florence."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from florence.onboarding.state import OnboardingStage, OnboardingState


@dataclass(slots=True)
class OnboardingPrompt:
    stage: OnboardingStage
    text: str
    requires_external_action: bool = False


@dataclass(slots=True)
class OnboardingTransition:
    state: OnboardingState
    prompt: OnboardingPrompt | None
    changed: bool = False


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(str(value).split()).strip()
    return cleaned or None


def _clean_list(values: list[str]) -> list[str]:
    return [item for item in (_clean_text(value) for value in values) if item]


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(value)
    return ordered


def _normalize_child_profiles(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in profiles:
        if not isinstance(item, dict):
            continue
        name = _clean_text(item.get("name"))
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        profile: dict[str, Any] = {"name": name}
        age = _clean_text(item.get("age"))
        school = _clean_text(item.get("school"))
        activities_raw = item.get("activities")
        activities = _clean_list(activities_raw) if isinstance(activities_raw, list) else None
        if age is not None:
            profile["age"] = age
        if school is not None:
            profile["school"] = school
        if activities is not None:
            profile["activities"] = activities
        normalized.append(profile)
    return normalized


def _seed_child_profiles(state: OnboardingState) -> list[dict[str, Any]]:
    if state.child_profiles:
        return [dict(profile) for profile in state.child_profiles]
    return [{"name": name} for name in _clean_list(state.child_names)]


def _child_detail_lines(profiles: list[dict[str, Any]]) -> list[str]:
    details: list[str] = []
    for profile in profiles:
        parts = [str(profile["name"])]
        age = _clean_text(profile.get("age"))
        school = _clean_text(profile.get("school"))
        activities = profile.get("activities")
        if age:
            parts.append(f"age {age}")
        if school:
            parts.append(school)
        if isinstance(activities, list) and activities:
            parts.append(", ".join(_clean_list(activities)))
        details.append(" - ".join(parts))
    return details


def _refresh_child_state(state: OnboardingState, profiles: list[dict[str, Any]]) -> OnboardingState:
    normalized = _normalize_child_profiles(profiles)
    metadata = dict(state.metadata)
    metadata["child_profiles"] = normalized
    metadata["child_details"] = _child_detail_lines(normalized)
    names = [str(profile["name"]) for profile in normalized]
    school_labels = _dedupe(
        [
            school
            for school in (_clean_text(profile.get("school")) for profile in normalized)
            if school and school.lower() not in {"none yet", "not yet", "unknown"}
        ]
    )
    activity_labels = _dedupe(
        [
            label
            for profile in normalized
            for label in (_clean_list(profile.get("activities")) if isinstance(profile.get("activities"), list) else [])
        ]
    )
    school_collected = bool(normalized) and all(_clean_text(profile.get("school")) is not None for profile in normalized)
    activity_collected = bool(normalized) and all(isinstance(profile.get("activities"), list) for profile in normalized)
    return replace(
        state,
        metadata=metadata,
        child_names=names,
        school_labels=school_labels,
        activity_labels=activity_labels,
        school_basics_collected=school_collected,
        activity_basics_collected=activity_collected,
    )


def _set_current_child_index(state: OnboardingState, index: int) -> OnboardingState:
    metadata = dict(state.metadata)
    metadata["current_child_index"] = max(0, index)
    return replace(state, metadata=metadata)


def _replace_metadata_list(state: OnboardingState, key: str, values: list[str]) -> OnboardingState:
    metadata = dict(state.metadata)
    metadata[key] = _clean_list(values)
    return replace(state, metadata=metadata)


def _replace_metadata_text(state: OnboardingState, key: str, value: str) -> OnboardingState:
    metadata = dict(state.metadata)
    metadata[key] = _clean_text(value)
    return replace(state, metadata=metadata)


def _next_missing_child_field(state: OnboardingState) -> tuple[int | None, OnboardingStage | None]:
    profiles = state.child_profiles
    for index, profile in enumerate(profiles):
        if _clean_text(profile.get("age")) is None:
            return index, OnboardingStage.COLLECT_CHILD_AGE
        if _clean_text(profile.get("school")) is None:
            return index, OnboardingStage.COLLECT_CHILD_SCHOOL
        if not isinstance(profile.get("activities"), list):
            return index, OnboardingStage.COLLECT_CHILD_ACTIVITIES
    return None, None


def sync_onboarding_stage(state: OnboardingState) -> OnboardingState:
    """Return a copy of the state with the canonical next stage applied."""
    current = state
    if current.group_channel_id:
        return replace(current, stage=OnboardingStage.COMPLETE)
    if current.is_complete:
        return replace(current, stage=OnboardingStage.COMPLETE)
    if not current.parent_display_name:
        return replace(current, stage=OnboardingStage.COLLECT_PARENT_NAME)
    if not current.child_profiles:
        return replace(current, stage=OnboardingStage.COLLECT_CHILD_NAMES)
    child_index, child_stage = _next_missing_child_field(current)
    if child_stage is not None and child_index is not None:
        current = _set_current_child_index(current, child_index)
        return replace(current, stage=child_stage)
    if not current.google_connected:
        return replace(current, stage=OnboardingStage.CONNECT_GOOGLE)
    return replace(current, stage=OnboardingStage.COMPLETE)


def build_google_connect_message(link_url: str | None = None) -> str:
    return "\n\n".join(build_google_connect_message_sequence(link_url))


def build_google_connect_message_sequence(
    link_url: str | None = None,
    *,
    include_intro: bool = False,
) -> tuple[str, ...]:
    messages: list[str] = []
    if include_intro:
        messages.extend(
            [
                "Hi, I'm Florence.",
                "I help run the household with you by keeping logistics organized, surfacing reminders, and staying on top of school and calendar noise.",
            ]
        )
    messages.append("Connect your Google account so I can pull the last 30 days of family email and calendar in the background while we keep going here.")
    if link_url:
        messages.append(link_url)
    messages.append("Once Google says you're connected, come right back here. You can also keep answering my questions while it runs.")
    return tuple(messages)


def build_onboarding_ready_message_sequence() -> tuple[str, ...]:
    return (
        "You're ready. Florence is set up as your house manager now.",
        (
            "Start with a real task like: what's on the kids' schedule next week, check my email for a school or camp update, "
            "remind me about picture day, or plan dinners and groceries for next week."
        ),
    )


def build_google_connected_syncing_message_sequence(link_url: str | None = None) -> tuple[str, ...]:
    messages = [
        "Google is connected. I’m syncing the last 30 days of your email and calendar in the background now, and I’ll text you here when the first pass is ready.",
    ]
    if link_url:
        messages.extend(
            [
                "If you want to track setup progress on your computer, use this link:",
                link_url,
            ]
        )
    return tuple(messages)


def build_onboarding_prompt(state: OnboardingState) -> OnboardingPrompt | None:
    current = sync_onboarding_stage(state)
    if current.stage == OnboardingStage.COLLECT_PARENT_NAME:
        return OnboardingPrompt(stage=current.stage, text="What's your name?")

    if current.stage == OnboardingStage.COLLECT_CHILD_NAMES:
        return OnboardingPrompt(
            stage=current.stage,
            text="What are your kids' names? One per line or comma-separated is fine.",
        )

    child_name = current.current_child_name or "your child"
    if current.stage == OnboardingStage.COLLECT_CHILD_AGE:
        intro = "Great, let's learn more about each kid one at a time."
        if current.current_child_index > 0:
            intro = "Okay, next one."
        return OnboardingPrompt(
            stage=current.stage,
            text=f"{intro} How old is {child_name}?",
        )

    if current.stage == OnboardingStage.COLLECT_CHILD_SCHOOL:
        return OnboardingPrompt(
            stage=current.stage,
            text=f"What school does {child_name} go to? If not in school yet, say not yet.",
        )

    if current.stage == OnboardingStage.COLLECT_CHILD_ACTIVITIES:
        return OnboardingPrompt(
            stage=current.stage,
            text=f"What activities does {child_name} do? If none right now, say none.",
        )

    if current.stage == OnboardingStage.CONNECT_GOOGLE:
        return OnboardingPrompt(
            stage=current.stage,
            text=build_google_connect_message(),
            requires_external_action=True,
        )

    if current.stage == OnboardingStage.COMPLETE:
        return None

    return OnboardingPrompt(
        stage=current.stage,
        text="Tell me what I should know next.",
    )


def apply_parent_name(state: OnboardingState, display_name: str) -> OnboardingTransition:
    cleaned = _clean_text(display_name)
    next_state = sync_onboarding_stage(replace(state, parent_display_name=cleaned))
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=cleaned != (state.parent_display_name or None),
    )


def mark_google_connected(state: OnboardingState) -> OnboardingTransition:
    next_state = sync_onboarding_stage(replace(state, google_connected=True))
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=state.google_connected is False,
    )


def apply_child_names(
    state: OnboardingState,
    child_names: list[str],
    *,
    child_details: list[str] | None = None,
) -> OnboardingTransition:
    existing = {str(profile["name"]).lower(): dict(profile) for profile in state.child_profiles}
    ordered: list[dict[str, Any]] = list(state.child_profiles)
    for name in _clean_list(child_names):
        key = name.lower()
        if key in existing:
            continue
        profile = {"name": name}
        ordered.append(profile)
        existing[key] = profile
    next_state = _refresh_child_state(state, ordered)
    if child_details is not None:
        next_state = _replace_metadata_list(next_state, "child_details", child_details)
    next_state = sync_onboarding_stage(next_state)
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=next_state.child_profiles != state.child_profiles,
    )


def apply_child_profile_updates(state: OnboardingState, child_updates: list[dict[str, Any]]) -> OnboardingTransition:
    profiles = _seed_child_profiles(state)
    index_by_name = {str(profile["name"]).lower(): idx for idx, profile in enumerate(profiles)}
    changed = False
    for raw in child_updates:
        if not isinstance(raw, dict):
            continue
        name = _clean_text(raw.get("name")) or state.current_child_name
        if not name:
            continue
        key = name.lower()
        if key not in index_by_name:
            profiles.append({"name": name})
            index_by_name[key] = len(profiles) - 1
            changed = True
        profile = dict(profiles[index_by_name[key]])
        age = _clean_text(raw.get("age"))
        school = _clean_text(raw.get("school"))
        activities_raw = raw.get("activities")
        activities = _clean_list(activities_raw) if isinstance(activities_raw, list) else activities_raw
        if age is not None and age != profile.get("age"):
            profile["age"] = age
            changed = True
        if school is not None and school != profile.get("school"):
            profile["school"] = school
            changed = True
        if isinstance(activities, list) and activities != profile.get("activities"):
            profile["activities"] = activities
            changed = True
        profiles[index_by_name[key]] = profile
    next_state = _refresh_child_state(state, profiles)
    next_state = sync_onboarding_stage(next_state)
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=changed,
    )


def apply_school_basics(state: OnboardingState, school_labels: list[str]) -> OnboardingTransition:
    profiles = _seed_child_profiles(state)
    labels = _clean_list(school_labels)
    unresolved = [idx for idx, profile in enumerate(profiles) if _clean_text(profile.get("school")) is None]
    for idx, label in zip(unresolved, labels):
        profiles[idx]["school"] = label
    return apply_child_profile_updates(
        _refresh_child_state(state, profiles),
        [],
    )


def apply_activity_basics(state: OnboardingState, activity_labels: list[str]) -> OnboardingTransition:
    profiles = _seed_child_profiles(state)
    unresolved = [idx for idx, profile in enumerate(profiles) if not isinstance(profile.get("activities"), list)]
    if unresolved:
        target_index = unresolved[0]
        profiles[target_index]["activities"] = _clean_list(activity_labels)
    return apply_child_profile_updates(
        _refresh_child_state(state, profiles),
        [],
    )


def apply_household_members(state: OnboardingState, household_members: list[str]) -> OnboardingTransition:
    next_state = sync_onboarding_stage(_replace_metadata_list(state, "household_members", household_members))
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=next_state.household_members != state.household_members,
    )


def apply_household_operations(state: OnboardingState, household_operations: list[str]) -> OnboardingTransition:
    next_state = sync_onboarding_stage(_replace_metadata_list(state, "household_operations", household_operations))
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=next_state.household_operations != state.household_operations,
    )


def apply_nudge_preferences(state: OnboardingState, nudge_preferences: str) -> OnboardingTransition:
    next_state = sync_onboarding_stage(_replace_metadata_text(state, "nudge_preferences", nudge_preferences))
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=next_state.nudge_preferences != state.nudge_preferences,
    )


def apply_operating_preferences(state: OnboardingState, operating_preferences: str) -> OnboardingTransition:
    next_state = sync_onboarding_stage(_replace_metadata_text(state, "operating_preferences", operating_preferences))
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=next_state.operating_preferences != state.operating_preferences,
    )


def mark_group_activated(state: OnboardingState, group_channel_id: str) -> OnboardingTransition:
    cleaned = _clean_text(group_channel_id)
    next_state = sync_onboarding_stage(replace(state, group_channel_id=cleaned))
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=cleaned != (state.group_channel_id or None),
    )
