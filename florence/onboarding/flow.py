"""Deterministic onboarding flow service for Florence."""

from __future__ import annotations

from dataclasses import dataclass, replace

from florence.onboarding.state import OnboardingStage, OnboardingState, OnboardingVariant


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
    if state.is_complete:
        return replace(state, stage=OnboardingStage.COMPLETE)
    if state.variant == OnboardingVariant.CONCIERGE and not state.household_members:
        return replace(state, stage=OnboardingStage.COLLECT_HOUSEHOLD_MEMBERS)
    if state.household_operations and not state.google_connected:
        return replace(state, stage=OnboardingStage.CONNECT_GOOGLE)
    if state.operating_preferences:
        return replace(state, stage=OnboardingStage.COMPLETE)
    if state.nudge_preferences:
        return replace(state, stage=OnboardingStage.COLLECT_OPERATING_PREFERENCES)
    if state.household_operations:
        return replace(state, stage=OnboardingStage.COLLECT_NUDGE_PREFERENCES)
    if state.school_basics_collected:
        if state.activity_basics_collected:
            return replace(state, stage=OnboardingStage.COLLECT_HOUSEHOLD_OPERATIONS)
        return replace(state, stage=OnboardingStage.COLLECT_ACTIVITY_BASICS)
    if state.child_names:
        return replace(state, stage=OnboardingStage.COLLECT_SCHOOL_BASICS)
    if state.parent_display_name:
        return replace(state, stage=OnboardingStage.COLLECT_CHILD_NAMES)
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
                "I help run the household with you by keeping logistics organized, surfacing reminders, and staying on top of school and calendar noise.",
            ]
        )
    messages.append("You're almost ready. Connect your Google account so I can compare Gmail and Calendar against the household context you just gave me.")
    if link_url:
        messages.append(link_url)
    messages.append("Once Google says you're connected, come back here and text done.")
    return tuple(messages)


def build_onboarding_ready_message_sequence() -> tuple[str, ...]:
    """Return the activation copy once Florence is ready for real use."""
    return (
        "You're ready. Florence is set up as your house manager now.",
        (
            "Start with a real task like: what's on the kids' schedule next week, check my email for a school or camp update, "
            "remind me about picture day, or plan dinners and groceries for next week."
        ),
    )


def build_web_onboarding_handoff_sequence(
    link_url: str | None,
    *,
    include_intro: bool = False,
) -> tuple[str, ...]:
    """Return the web-first onboarding handoff copy."""
    messages: list[str] = []
    if include_intro:
        messages.extend(
            [
                "Hi, I'm Florence.",
                "I’m easiest to set up on a computer. Finish setup there so I can learn your household, connect Google, and start acting like your house manager.",
            ]
        )
    else:
        messages.append("Finish setup on your computer here when you're ready.")
    if link_url:
        messages.append(link_url)
    messages.append("Once setup is done, I’ll text you here when I’m ready and when the first Gmail and Calendar pass finishes.")
    return tuple(messages)


def _replace_metadata_list(state: OnboardingState, key: str, values: list[str]) -> OnboardingState:
    metadata = dict(state.metadata)
    cleaned = [value for value in (" ".join(raw.split()).strip() for raw in values) if value]
    metadata[key] = cleaned
    return replace(state, metadata=metadata)


def _replace_metadata_text(state: OnboardingState, key: str, value: str) -> OnboardingState:
    metadata = dict(state.metadata)
    cleaned = " ".join(value.split()).strip()
    metadata[key] = cleaned
    return replace(state, metadata=metadata)


def build_onboarding_prompt(state: OnboardingState) -> OnboardingPrompt | None:
    """Return the next deterministic prompt for the current onboarding stage."""
    current = sync_onboarding_stage(state)
    if current.stage == OnboardingStage.COLLECT_PARENT_NAME:
        return OnboardingPrompt(
            stage=current.stage,
            text="What should I call you?",
        )

    if current.stage == OnboardingStage.COLLECT_HOUSEHOLD_MEMBERS:
        return OnboardingPrompt(
            stage=current.stage,
            text=(
                "Let's build the household map first. Who is in your family unit? "
                "Reply one per line or comma-separated, like Maya - mom, Ben - dad, Ava - daughter."
            ),
        )

    if current.stage == OnboardingStage.CONNECT_GOOGLE:
        return OnboardingPrompt(
            stage=current.stage,
            text=build_google_connect_message(),
            requires_external_action=True,
        )

    if current.stage == OnboardingStage.COLLECT_CHILD_NAMES:
        if current.variant == OnboardingVariant.CONCIERGE:
            text = (
                "Tell me about each child I should know about: first name plus nickname, grade, or age if helpful. "
                "One per line works well, like Ava - goes by Aves - 3rd grade."
            )
        else:
            text = (
                "Start with the kids I should know about: first name plus grade or age if helpful. "
                "One per line or comma-separated is fine."
            )
        return OnboardingPrompt(
            stage=current.stage,
            text=text,
        )

    if current.stage == OnboardingStage.COLLECT_SCHOOL_BASICS:
        child_list = ", ".join(current.child_names)
        if current.variant == OnboardingVariant.CONCIERGE:
            if len(current.child_names) == 1:
                prompt = f"Which school, daycare, preschool, or camp should I know for {child_list}? Include who it belongs to if there is any ambiguity."
            else:
                prompt = f"Which schools, daycares, preschools, or camps should I know for {child_list}? Include which child goes with which place."
        else:
            if len(current.child_names) == 1:
                prompt = f"Which school, daycare, or preschool does {child_list} attend?"
            else:
                prompt = f"Which schools, daycares, or preschools should I know for {child_list}?"
        return OnboardingPrompt(stage=current.stage, text=prompt)

    if current.stage == OnboardingStage.COLLECT_ACTIVITY_BASICS:
        child_list = ", ".join(current.child_names)
        if current.variant == OnboardingVariant.CONCIERGE:
            prompt = (
                f"What recurring activities, teams, lessons, or clubs should I know for {child_list}? "
                "If helpful, include the child, like Ava soccer or Noah piano. If none yet, say none."
            )
        else:
            prompt = f"What recurring activities should I know about for {child_list}? If none yet, say none."
        return OnboardingPrompt(stage=current.stage, text=prompt)

    if current.stage == OnboardingStage.COLLECT_HOUSEHOLD_OPERATIONS:
        if current.variant == OnboardingVariant.CONCIERGE:
            text = (
                "What recurring household logistics do you want me to help manage like a house manager? "
                "Think lunches, school forms, practice logistics, birthday gifts, bills, returns, camps, appointments, or anything else you keep in your head today."
            )
        else:
            text = (
                "What recurring logistics or reminders should I help manage first? "
                "A short list is fine: lunches, forms, returns, bills, sports, appointments, birthdays, and so on."
            )
        return OnboardingPrompt(stage=current.stage, text=text)

    if current.stage == OnboardingStage.COLLECT_NUDGE_PREFERENCES:
        if current.variant == OnboardingVariant.CONCIERGE:
            text = (
                "Reminder style: I default to day before + morning of for important family logistics. "
                "If you want a different default, say same-day only or keep nudging until you acknowledge."
            )
        else:
            text = (
                "Reminder style: I default to day before + morning of for important family logistics. "
                "If you want a different default, say same-day only or keep nudging until acknowledged."
            )
        return OnboardingPrompt(stage=current.stage, text=text)

    if current.stage == OnboardingStage.COLLECT_OPERATING_PREFERENCES:
        if current.variant == OnboardingVariant.CONCIERGE:
            text = (
                "Any house rules for how I should operate as your house manager? "
                "Share your defaults in one line, like quiet hours, brief timing, and when I should ask before taking action."
            )
        else:
            text = (
                "Any house rules for how I should operate? "
                "A short one-liner is enough, like no texts after 9pm, morning brief time, and ask before spending money."
            )
        return OnboardingPrompt(stage=current.stage, text=text)

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


def apply_child_names(
    state: OnboardingState,
    child_names: list[str],
    *,
    child_details: list[str] | None = None,
) -> OnboardingTransition:
    cleaned = [name for name in (" ".join(raw.split()).strip() for raw in child_names) if name]
    next_state = replace(state, child_names=cleaned)
    if child_details is not None:
        next_state = _replace_metadata_list(next_state, "child_details", child_details)
    next_state = sync_onboarding_stage(next_state)
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=cleaned != state.child_names or (child_details is not None and next_state.child_details != state.child_details),
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
    cleaned = group_channel_id.strip()
    next_state = sync_onboarding_stage(replace(state, group_channel_id=cleaned or None))
    return OnboardingTransition(
        state=next_state,
        prompt=build_onboarding_prompt(next_state),
        changed=cleaned != (state.group_channel_id or ""),
    )
