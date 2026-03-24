"""Deterministic onboarding flow service for Florence."""

from __future__ import annotations

from dataclasses import dataclass, replace

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


def sync_onboarding_stage(state: OnboardingState) -> OnboardingState:
    """Return a copy of the state with the canonical next stage applied."""
    if state.group_channel_id:
        return replace(state, stage=OnboardingStage.COMPLETE)
    if state.activity_basics_collected:
        return replace(state, stage=OnboardingStage.ACTIVATE_GROUP)
    if state.school_basics_collected:
        return replace(state, stage=OnboardingStage.COLLECT_ACTIVITY_BASICS)
    if state.child_names:
        return replace(state, stage=OnboardingStage.COLLECT_SCHOOL_BASICS)
    if state.google_connected:
        return replace(state, stage=OnboardingStage.COLLECT_CHILD_NAMES)
    if state.parent_display_name:
        return replace(state, stage=OnboardingStage.CONNECT_GOOGLE)
    return replace(state, stage=OnboardingStage.COLLECT_PARENT_NAME)


def build_google_connect_message(link_url: str | None = None) -> str:
    """Return the friendly Google-connect step copy, with an optional OAuth link."""
    return "\n\n".join(build_google_connect_message_sequence(link_url))


def build_google_connect_message_sequence(
    link_url: str | None = None,
    *,
    include_intro: bool = False,
) -> tuple[str, ...]:
    """Return the Google-connect step as short separate agent-style messages."""
    messages: list[str] = []
    if include_intro:
        messages.extend(
            [
                "Hi, I'm Florence.",
                "I help keep your household organized by keeping up with school emails, calendar invites, and schedule changes.",
            ]
        )
    messages.append("First step: connect your Google account so I can start syncing Gmail and Calendar.")
    if link_url:
        messages.append(link_url)
    messages.append("When you're done, reply done here and I'll keep going.")
    return tuple(messages)


def build_onboarding_prompt(state: OnboardingState) -> OnboardingPrompt | None:
    """Return the next deterministic prompt for the current onboarding stage."""
    current = sync_onboarding_stage(state)
    if current.stage == OnboardingStage.COLLECT_PARENT_NAME:
        return OnboardingPrompt(
            stage=current.stage,
            text="What should I call you?",
        )

    if current.stage == OnboardingStage.CONNECT_GOOGLE:
        return OnboardingPrompt(
            stage=current.stage,
            text=build_google_connect_message(),
            requires_external_action=True,
        )

    if current.stage == OnboardingStage.COLLECT_CHILD_NAMES:
        return OnboardingPrompt(
            stage=current.stage,
            text="What are your children's first names?",
        )

    if current.stage == OnboardingStage.COLLECT_SCHOOL_BASICS:
        child_list = ", ".join(current.child_names)
        if len(current.child_names) == 1:
            prompt = f"Which school, daycare, or preschool does {child_list} attend?"
        else:
            prompt = f"Which schools, daycares, or preschools should I know for {child_list}?"
        return OnboardingPrompt(stage=current.stage, text=prompt)

    if current.stage == OnboardingStage.COLLECT_ACTIVITY_BASICS:
        child_list = ", ".join(current.child_names)
        if len(current.child_names) == 1:
            prompt = f"What recurring activities should I know about for {child_list}? If none yet, say none."
        else:
            prompt = f"What recurring activities should I know about for {child_list}? If none yet, say none."
        return OnboardingPrompt(stage=current.stage, text=prompt)

    if current.stage == OnboardingStage.ACTIVATE_GROUP:
        return OnboardingPrompt(
            stage=current.stage,
            text="Add me to your family iMessage group, then tell me here once I'm in.",
            requires_external_action=True,
        )

    return None


def apply_parent_name(state: OnboardingState, display_name: str) -> OnboardingTransition:
    cleaned = " ".join(display_name.split()).strip()
    next_state = sync_onboarding_stage(replace(state, parent_display_name=cleaned or None))
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=cleaned != (state.parent_display_name or ""),
    )


def mark_google_connected(state: OnboardingState) -> OnboardingTransition:
    next_state = sync_onboarding_stage(replace(state, google_connected=True))
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=state.google_connected is False,
    )


def apply_child_names(state: OnboardingState, child_names: list[str]) -> OnboardingTransition:
    cleaned = [name for name in (" ".join(raw.split()).strip() for raw in child_names) if name]
    next_state = sync_onboarding_stage(replace(state, child_names=cleaned))
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=cleaned != state.child_names,
    )


def apply_school_basics(state: OnboardingState, school_labels: list[str]) -> OnboardingTransition:
    cleaned = [label for label in (" ".join(raw.split()).strip() for raw in school_labels) if label]
    next_state = sync_onboarding_stage(
        replace(
            state,
            school_labels=cleaned,
            school_basics_collected=True,
        )
    )
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=cleaned != state.school_labels or not state.school_basics_collected,
    )


def apply_activity_basics(state: OnboardingState, activity_labels: list[str]) -> OnboardingTransition:
    cleaned = [label for label in (" ".join(raw.split()).strip() for raw in activity_labels) if label]
    next_state = sync_onboarding_stage(
        replace(
            state,
            activity_labels=cleaned,
            activity_basics_collected=True,
        )
    )
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=cleaned != state.activity_labels or not state.activity_basics_collected,
    )


def mark_group_activated(state: OnboardingState, group_channel_id: str) -> OnboardingTransition:
    cleaned = group_channel_id.strip()
    next_state = sync_onboarding_stage(replace(state, group_channel_id=cleaned or None))
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=cleaned != (state.group_channel_id or ""),
    )
