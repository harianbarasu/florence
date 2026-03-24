from florence.onboarding import (
    OnboardingStage,
    OnboardingState,
    apply_activity_basics,
    apply_child_names,
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
    )

    prompt = build_onboarding_prompt(state)
    assert prompt is not None
    assert prompt.stage == OnboardingStage.COLLECT_PARENT_NAME

    transition = apply_parent_name(state, "  Maya   ")
    assert transition.state.parent_display_name == "Maya"
    assert transition.state.stage == OnboardingStage.CONNECT_GOOGLE
    assert transition.prompt is not None
    assert transition.prompt.requires_external_action is True

    transition = mark_google_connected(transition.state)
    assert transition.state.stage == OnboardingStage.COLLECT_CHILD_NAMES

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
    assert transition.state.stage == OnboardingStage.ACTIVATE_GROUP

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
        google_connected=True,
        child_names=["Ava"],
        school_labels=["Roosevelt Elementary"],
        school_basics_collected=True,
        stage=OnboardingStage.COLLECT_ACTIVITY_BASICS,
    )

    transition = apply_activity_basics(state, [])

    assert transition.state.activity_labels == []
    assert transition.state.activity_basics_collected is True
    assert transition.state.stage == OnboardingStage.ACTIVATE_GROUP
    assert transition.prompt is not None
    assert transition.prompt.requires_external_action is True
