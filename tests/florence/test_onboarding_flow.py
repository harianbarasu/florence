from florence.onboarding import (
    OnboardingStage,
    OnboardingState,
    apply_child_names,
    apply_child_profile_updates,
    apply_parent_name,
    build_onboarding_prompt,
    build_onboarding_transition_message_sequence,
    mark_google_connected,
)


def test_onboarding_flow_advances_through_minimal_imessage_steps():
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
    assert transition.state.stage == OnboardingStage.COLLECT_CHILD_NAMES

    transition = apply_child_names(transition.state, ["Ava", "Noah"])
    assert transition.state.stage == OnboardingStage.COLLECT_CHILD_AGE

    transition = apply_child_profile_updates(
        transition.state,
        [{"name": "Ava", "age": "7", "school": "Roosevelt Elementary", "activities": ["Soccer"]}],
    )
    assert transition.state.stage == OnboardingStage.COLLECT_CHILD_AGE

    transition = apply_child_profile_updates(
        transition.state,
        [{"name": "Noah", "age": "4", "school": "Little Oaks Preschool", "activities": []}],
    )
    assert transition.state.stage == OnboardingStage.CONNECT_GOOGLE
    assert transition.prompt is not None
    assert transition.prompt.requires_external_action is True

    transition = mark_google_connected(transition.state)
    assert transition.state.stage == OnboardingStage.COMPLETE
    assert transition.state.is_complete is True
    assert transition.prompt is None


def test_activity_answer_can_be_empty_and_still_complete_child_profile():
    state = OnboardingState(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        parent_display_name="Maya",
        stage=OnboardingStage.COLLECT_CHILD_ACTIVITIES,
        metadata={
            "child_profiles": [{"name": "Ava", "age": "7", "school": "Roosevelt Elementary"}],
            "current_child_index": 0,
        },
    )

    transition = apply_child_profile_updates(
        state,
        [{"name": "Ava", "activities": []}],
    )

    assert transition.state.activity_labels == []
    assert transition.state.stage == OnboardingStage.CONNECT_GOOGLE
    assert transition.state.is_complete is False
    assert transition.prompt is not None


def test_onboarding_goes_straight_to_kids_after_parent_name():
    state = OnboardingState(
        household_id="hh_123",
        member_id="mem_456",
        thread_id="thread_dm_456",
    )

    transition = apply_parent_name(state, "Maya")

    assert transition.state.stage == OnboardingStage.COLLECT_CHILD_NAMES
    assert transition.prompt is not None
    assert "kids" in transition.prompt.text.lower()


def test_onboarding_prompt_uses_short_name_for_full_child_name():
    state = OnboardingState(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
        parent_display_name="Maya",
        metadata={
            "child_profiles": [{"name": "Theo Williams", "age": "7"}],
            "current_child_index": 0,
        },
    )

    prompt = build_onboarding_prompt(state)

    assert prompt is not None
    assert prompt.stage == OnboardingStage.COLLECT_CHILD_SCHOOL
    assert prompt.text == "What school does Theo go to? If not yet, just say not yet."


def test_transition_messages_after_parent_name_include_intro_and_google_connect():
    state = OnboardingState(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_dm_123",
    )

    transition = apply_parent_name(state, "Maya")
    messages = build_onboarding_transition_message_sequence(
        transition,
        previous_stage=OnboardingStage.COLLECT_PARENT_NAME,
        link_url="https://example.com/google/connect",
    )

    assert messages == (
        "Hi, I'm Florence.",
        "I help run the household with you by keeping logistics organized, surfacing reminders, and staying on top of school and calendar noise.",
        "Connect your Google account so I can pull up to the last year of family email and calendar in the background while we keep going here.",
        "https://example.com/google/connect",
        "Once Google says you're connected, come right back here. You can also keep answering my questions while it runs.",
        "What are your kids' names? You can send them all in one message, one per line or comma-separated.",
    )
