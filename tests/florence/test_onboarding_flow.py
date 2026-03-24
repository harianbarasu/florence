from florence.onboarding import (
    OnboardingStage,
    OnboardingState,
    OnboardingVariant,
    apply_activity_basics,
    apply_child_names,
    apply_household_members,
    apply_household_operations,
    apply_nudge_preferences,
    apply_operating_preferences,
    apply_parent_name,
    apply_school_basics,
    build_onboarding_prompt,
    mark_google_connected,
    mark_group_activated,
)


def test_onboarding_flow_advances_through_required_v1_steps():
    state = OnboardingState(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        metadata={"variant": OnboardingVariant.HYBRID.value},
    )

    prompt = build_onboarding_prompt(state)
    assert prompt is not None
    assert prompt.stage == OnboardingStage.COLLECT_PARENT_NAME

    transition = apply_parent_name(state, "  Maya   ")
    assert transition.state.parent_display_name == "Maya"
    assert transition.state.stage == OnboardingStage.COLLECT_CHILD_NAMES
    assert transition.prompt is not None
    assert "kids" in transition.prompt.text.lower() or "child" in transition.prompt.text.lower()

    transition = apply_child_names(transition.state, ["Ava", "Noah"])
    assert transition.state.stage == OnboardingStage.COLLECT_SCHOOL_BASICS

    transition = apply_school_basics(
        transition.state,
        ["Roosevelt Elementary", "Little Oaks Preschool"],
    )
    assert transition.state.school_basics_collected is True
    assert transition.state.stage == OnboardingStage.COLLECT_ACTIVITY_BASICS

    transition = apply_activity_basics(transition.state, ["Soccer", "Piano"])
    assert transition.state.activity_basics_collected is True
    assert transition.state.stage == OnboardingStage.COLLECT_HOUSEHOLD_OPERATIONS

    transition = apply_household_operations(
        transition.state,
        ["school forms", "returns", "soccer logistics"],
    )
    assert transition.state.stage == OnboardingStage.CONNECT_GOOGLE
    assert transition.prompt is not None
    assert transition.prompt.requires_external_action is True

    transition = mark_google_connected(transition.state)
    assert transition.state.stage == OnboardingStage.COLLECT_NUDGE_PREFERENCES

    transition = apply_nudge_preferences(
        transition.state,
        "Day before and morning of for anything time-sensitive. Keep nudging until I reply for school forms.",
    )
    assert transition.state.stage == OnboardingStage.COLLECT_OPERATING_PREFERENCES
    assert transition.prompt is not None

    transition = apply_operating_preferences(
        transition.state,
        "Weekday morning brief at 6:45, evening check-in on school nights, no texts after 9pm, always ask before spending money.",
    )
    assert transition.state.stage == OnboardingStage.COMPLETE
    assert transition.state.is_complete is True
    assert transition.prompt is None

    transition = mark_group_activated(transition.state, "bb_thread_group_123")
    assert transition.state.group_channel_id == "bb_thread_group_123"
    assert transition.state.stage == OnboardingStage.COMPLETE
    assert transition.state.is_complete is True
    assert transition.prompt is None


def test_onboarding_allows_empty_activity_list_once_answer_is_collected():
    state = OnboardingState(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        parent_display_name="Maya",
        child_names=["Ava"],
        school_labels=["Roosevelt Elementary"],
        school_basics_collected=True,
        stage=OnboardingStage.COLLECT_ACTIVITY_BASICS,
        metadata={"variant": OnboardingVariant.HYBRID.value},
    )

    transition = apply_activity_basics(state, [])

    assert transition.state.activity_labels == []
    assert transition.state.activity_basics_collected is True
    assert transition.state.stage == OnboardingStage.COLLECT_HOUSEHOLD_OPERATIONS
    assert transition.state.is_complete is False
    assert transition.prompt is not None


def test_concierge_variant_collects_family_unit_before_child_details():
    state = OnboardingState(
        household_id="hh_123",
        member_id="mem_456",
        thread_id="thread_dm_456",
        metadata={"variant": OnboardingVariant.CONCIERGE.value},
    )

    transition = apply_parent_name(state, "Maya")

    assert transition.state.stage == OnboardingStage.COLLECT_HOUSEHOLD_MEMBERS
    assert transition.prompt is not None
    assert "family unit" in transition.prompt.text.lower()

    transition = apply_household_members(
        transition.state,
        ["Maya - mom", "Ben - dad", "Ava - daughter"],
    )

    assert transition.state.stage == OnboardingStage.COLLECT_CHILD_NAMES
