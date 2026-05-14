from florence.media.openai_extract import DEFAULT_IMAGE_EXTRACTION_PROMPT


def test_image_extraction_prompt_preserves_calendar_date_source_truth():
    prompt = DEFAULT_IMAGE_EXTRACTION_PROMPT

    assert "act as OCR rather than a calendar interpreter" in prompt
    assert "Do not shift, normalize, or convert dates from grid position or weekday math." in prompt
    assert "Do not invent derived sections such as 'month-by-month logistics', 'important milestones', or date-conversion summaries" in prompt
    assert "If a date/event relationship is unclear" in prompt
    assert "faithful OCR-style extraction" in prompt
