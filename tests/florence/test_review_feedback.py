from florence.runtime.review_feedback import ReviewFeedbackKind, parse_review_feedback


def test_parse_review_feedback_covers_tradclaw_style_corrections() -> None:
    examples = {
        "ignore this sender": ReviewFeedbackKind.IGNORE_SOURCE,
        "already handled": ReviewFeedbackKind.ALREADY_HANDLED,
        "too late": ReviewFeedbackKind.STALE,
        "wrong date": ReviewFeedbackKind.WRONG_DETAILS,
        "ignore this type": ReviewFeedbackKind.IGNORE_ITEM_TYPE,
        "duplicate": ReviewFeedbackKind.DUPLICATE,
        "too noisy": ReviewFeedbackKind.TOO_NOISY,
        "tell me sooner": ReviewFeedbackKind.WRONG_TIMING,
        "private only": ReviewFeedbackKind.PRIVATE_ONLY,
        "always share this source": ReviewFeedbackKind.ALWAYS_SHARE,
        "this matters": ReviewFeedbackKind.ALWAYS_SURFACE,
        "less proactive please": ReviewFeedbackKind.LESS_PROACTIVE,
        "too wordy": ReviewFeedbackKind.LESS_PROACTIVE,
        "more proactive on these": ReviewFeedbackKind.MORE_PROACTIVE,
        "disable meal planning": ReviewFeedbackKind.DISABLE_MODULE,
    }

    for text, expected_kind in examples.items():
        parsed = parse_review_feedback(text)
        assert parsed is not None, text
        assert parsed.kind == expected_kind


def test_parse_review_feedback_extracts_numbered_target() -> None:
    parsed = parse_review_feedback("2 already handled")

    assert parsed is not None
    assert parsed.kind == ReviewFeedbackKind.ALREADY_HANDLED
    assert parsed.target_index == 2


def test_parse_review_feedback_leaves_ambiguous_corrections_for_hermes() -> None:
    assert parse_review_feedback("yes, but the time is 3:30 PM") is None
    assert parse_review_feedback("what is this?") is None
