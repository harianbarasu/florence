"""Florence-owned Hermes toolset registration."""

from __future__ import annotations


FLORENCE_HOUSEHOLD_TOOLS = [
    "web_search", "web_extract",
    "browser_navigate", "browser_snapshot", "browser_click",
    "browser_type", "browser_scroll", "browser_back",
    "browser_press", "browser_close", "browser_get_images",
    "browser_vision", "browser_console",
    "vision_analyze", "image_generate", "text_to_speech",
    "todo", "session_search", "clarify",
    "cronjob",
    "delegate_task",
    "send_message",
    "honcho_context", "honcho_profile", "honcho_search", "honcho_conclude",
    "household_search_state",
    "household_search_google_inbox",
    "household_search_google_calendar",
    "household_import_calendar_feed",
    "household_apply_candidate_review",
    "household_apply_nudge_action",
    "household_apply_onboarding_update",
    "household_request_parent_link",
    "household_resolve_merge_followup",
    "household_upsert_event",
    "household_upsert_work_item",
    "household_upsert_routine",
    "household_schedule_nudge",
    "household_upsert_meal",
    "household_upsert_shopping_item",
    "household_record_preference",
]

FLORENCE_ONBOARDING_TOOLS = [
    "household_apply_onboarding_update",
    "household_search_google_inbox",
    "household_search_google_calendar",
]

FLORENCE_BRIEFING_TOOLS = [
    "household_search_state",
    "household_search_google_inbox",
    "household_search_google_calendar",
    "session_search",
    "honcho_context",
    "honcho_search",
]


def register_florence_toolsets() -> None:
    """Register Florence product toolsets with Hermes' runtime toolset map."""
    from florence.agent.hermes_runtime import create_hermes_custom_toolset

    # Importing the household module registers Florence's tool handlers with
    # whichever Hermes registry the runtime boundary has selected.
    import florence.tools.household  # noqa: F401

    create_hermes_custom_toolset(
        "florence_chat",
        "Florence household-general Hermes profile without coding or admin tools",
        tools=FLORENCE_HOUSEHOLD_TOOLS,
    )
    create_hermes_custom_toolset(
        "florence_onboarding",
        "Florence onboarding profile for fast structured setup updates",
        tools=FLORENCE_ONBOARDING_TOOLS,
    )
    create_hermes_custom_toolset(
        "florence_briefing",
        "Florence briefing profile (read-only household state lookup)",
        tools=FLORENCE_BRIEFING_TOOLS,
    )
