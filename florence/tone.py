"""Centralized user-facing tone for Florence."""

from __future__ import annotations

from datetime import datetime

from florence.models import (
    BriefingSourceItem,
    HouseholdDataSummary,
    HouseholdReadiness,
    HouseholdPrivacy,
    MemorySnapshot,
    PendingAction,
    Reminder,
    SourceFeedbackKind,
    SourcePreference,
    SourcePreferenceKind,
    SourceReviewSnapshot,
)
from florence.timekeeper import format_local


def reminder_created(
    title: str,
    due_at_utc: datetime,
    timezone_name: str,
    *,
    assignee_label: str | None = None,
) -> str:
    if assignee_label:
        return f"Done. I will remind {assignee_label} about {title} ({format_local(due_at_utc, timezone_name)})."
    return f"Done. I will remind you about {title} ({format_local(due_at_utc, timezone_name)})."


def reminder_needs_time() -> str:
    return "I can do that. What day and time should I use? I will not schedule it in the past."


def reminder_needs_ampm(matched_text: str | None = None) -> str:
    when = f" for '{matched_text.strip()}'" if matched_text else ""
    return f"I can do that. Should I use AM or PM{when}?"


def due_reminder(title: str, *, assignee_label: str | None = None) -> str:
    if assignee_label:
        return f"Quick reminder for {assignee_label}: {title}"
    return f"Quick reminder: {title}"


def reminder_completed(title: str) -> str:
    return f"Done. I marked {title} as handled."


def reminder_canceled(title: str) -> str:
    return f"Done. I canceled the reminder for {title}."


def reminder_resolution_parent_only() -> str:
    return "I need one of the parents in this household to change reminders."


def reminder_resolution_needs_text() -> str:
    return "Which reminder should I update? Send it like 'done pack lunch' or 'cancel reminder pack lunch'."


def reminder_resolution_not_found(query: str) -> str:
    return f"I could not find an active reminder matching '{query}'."


def reminder_resolution_ambiguous(matches: list[Reminder]) -> str:
    lines = ["I found more than one matching reminder. Be a little more specific:"]
    for reminder in matches[:3]:
        lines.append(f"- {reminder.title}")
    return "\n".join(lines)


def source_surface(
    title: str,
    due_at_utc: datetime | None,
    timezone_name: str,
    *,
    suggested_action: PendingAction | None = None,
) -> str:
    if due_at_utc is None:
        text = f"This looks worth your attention: {title}"
    else:
        text = f"This looks worth your attention: {title} ({format_local(due_at_utc, timezone_name)})."
    if suggested_action is None:
        return text
    code = suggested_action.id[:8]
    return (
        f"{text}\n"
        f"I can also add a reminder for this. Reply 'approve {code}' or 'cancel {code}'."
    )


def attachment_saved() -> str:
    return "Got it. I will keep that with the household context and surface it only if it needs action."


def attachment_needs_context(count: int) -> str:
    noun = "attachment" if count == 1 else "attachments"
    return (
        f"I got the {noun}. If the key details are only in the image or PDF, send one line like "
        "'permission slip due Friday' and I will track it."
    )


def no_agenda() -> str:
    return "Nothing due today is on my list right now."


def help_text(topic: str | None = None) -> str:
    normalized = " ".join((topic or "").strip(" ?").lower().split())
    if normalized in {"setup", "onboarding", "household"}:
        return (
            "Setup help: text 'setup' to see what is missing, 'my name is Sam', "
            "'invite partner +15555550101', 'our kids are Maya and Leo', or "
            "'connect google'."
        )
    if normalized in {"source", "sources", "email", "connected sources"}:
        return (
            "Source help: text 'source review' to see what I texted or kept quiet, "
            "'always tell me about permission slips', 'mute newsletters', "
            "'mute this sender', 'source preferences', or 'disconnect google'."
        )
    if normalized in {"calendar", "calendars", "calendar events", "event", "events", "household calendar"}:
        return (
            "Calendar help: text 'add soccer practice tomorrow at 5pm to calendar'. "
            "I will add it to the household calendar for agenda, prep, and briefings. "
            "This does not write to Google Calendar."
        )
    if normalized in {"memory", "memories"}:
        return (
            "Memory help: the household book is the context Florence keeps. Parents can text "
            "'remember that Maya likes pasta', "
            "'show household book', 'what do you remember?', 'forget Maya likes pasta', "
            "or 'pause memory'."
        )
    if normalized in {"privacy", "controls"}:
        return (
            "Privacy help: parents can text 'privacy status', 'pause memory', "
            "'resume memory', 'opt out of product analytics', 'data summary', "
            "'delete my data', 'stop', or 'start'."
        )
    if normalized in {"reminder", "reminders", "task", "tasks"}:
        return (
            "Reminder help: text 'remind us tomorrow at 8am to pack lunch', "
            "'remind Alex tomorrow at 8am to pack cleats', 'done pack lunch', "
            "'cancel reminder pack lunch', or 'tomorrow prep'."
        )
    if normalized in {"approval", "approvals", "action", "actions"}:
        return (
            "Approval help: I ask before risky actions. A parent can reply "
            "'approve abc12345', 'cancel abc12345', or text 'handoff' to see open items."
        )
    if normalized in {"support", "human"}:
        return "Support help: text 'support' or 'talk to a human' and I will show the configured support contact."
    return (
        "Text me things like: 'remind us tomorrow at 8am to pack the permission slip', "
        "'done pack lunch', 'what is on deck today?', 'tomorrow prep', 'handoff', 'my name is Sam', "
        "'add soccer practice tomorrow at 5pm to calendar', "
        "'invite partner +15555550101', "
        "'confirm partner +15555550101', 'stop', or 'start', "
        "'support' for human help, or forward school details. "
        "For more detail, text 'help setup', 'help sources', 'help calendar', "
        "'help memory', 'help privacy', or 'help reminders'. "
        "I will keep the noisy stuff quiet."
    )


def support_contact(contact: str | None) -> str:
    if contact:
        return (
            f"You can reach Florence support here: {contact}. "
            "If this is urgent, contact your usual support or emergency contacts directly."
        )
    return (
        "Florence support is not configured for this environment yet. "
        "If this is urgent, contact your usual support or emergency contacts directly."
    )


def stopped() -> str:
    return "Understood. I will stop replying in this thread."


def resumed() -> str:
    return "I am back on for this household."


def stop_parent_only() -> str:
    return "I need one of the parents in this household to stop or restart Florence."


def data_deletion_parent_only() -> str:
    return "I need one of the parents in this household to delete household data."


def data_deletion_confirm_prompt() -> str:
    return (
        "I can delete this household's Florence data. To avoid mistakes, text "
        "'confirm delete household data'. This removes messages, reminders, connected sources, "
        "source rules, approvals, and durable memory for this household."
    )


def data_deletion_confirmation_missing() -> str:
    return (
        "Text 'delete my data' first, then confirm with "
        "'confirm delete household data' within the confirmation window."
    )


def data_deletion_complete() -> str:
    return "Done. I deleted this household's Florence data from this environment. Send a new message if you want to start fresh."


def data_summary_parent_only() -> str:
    return "I need one of the parents in this household to view the household data summary."


def household_data_summary(summary: HouseholdDataSummary) -> str:
    paused = "paused" if summary.stopped else "active"
    memory = "on" if summary.memory_enabled else "paused"
    analytics = "on" if summary.product_analytics_opt_in else "off"
    return (
        "Here is the household data I have, without raw message or email bodies:\n"
        f"- Household: {summary.parent_count} parents, {summary.helper_count} helpers, timezone {summary.timezone}\n"
        f"- Messages stored: {summary.message_count}\n"
        f"- Active reminders: {summary.active_reminder_count}\n"
        f"- Connected sources: {summary.connected_account_count} accounts, {summary.source_item_count} items "
        f"({summary.surfaced_source_item_count} texted, {summary.stored_source_item_count} kept quiet, "
        f"{summary.suppressed_source_item_count} suppressed)\n"
        f"- Source rules: {summary.source_preference_count}\n"
        f"- Durable memories: {summary.active_memory_count}\n"
        f"- Pending approvals: {summary.pending_action_count}\n"
        f"- Florence status: {paused}; memory {memory}; analytics {analytics}"
    )


def fallback_reply() -> str:
    return (
        "Got it. I am having trouble thinking that through right now, "
        "so I did not make any household changes. If it is urgent, text 'support' for human help."
    )


def member_named(name: str, next_prompt: str | None = None) -> str:
    text = f"Nice to meet you, {name}. I will use that in this household."
    if next_prompt:
        return f"{text} {next_prompt}"
    return text


def timezone_updated(timezone_name: str) -> str:
    return f"Done. I will use {timezone_name} for this household."


def children_recorded(child_names: list[str]) -> str:
    if len(child_names) == 1:
        return f"Got it. I will remember {child_names[0]} as a child in this household."
    names = ", ".join(child_names[:-1]) + f" and {child_names[-1]}"
    return f"Got it. I will remember {names} as children in this household."


def calendar_event_created(title: str, starts_at_utc: datetime, timezone_name: str) -> str:
    return f"Done. I added {title} to the household calendar ({format_local(starts_at_utc, timezone_name)})."


def calendar_event_needs_time() -> str:
    return "I can add that to the household calendar. What day and time should I use? I will not schedule it in the past."


def calendar_event_needs_ampm(matched_text: str | None = None) -> str:
    when = f" for '{matched_text.strip()}'" if matched_text else ""
    return f"I can add that to the household calendar. Should I use AM or PM{when}?"


def calendar_event_parent_only() -> str:
    return "I need one of the parents in this household to add calendar events."


def household_setup_parent_only() -> str:
    return "I need one of the parents in this household to change household setup details."


def partner_invite_parent_only() -> str:
    return "I need one of the parents in this household to invite a partner."


def partner_invite_already_ready() -> str:
    return "I already see two parents in this household."


def partner_invite_needs_phone() -> str:
    return "I can do that. Send your partner's phone number, including country code if you know it."


def partner_invite_self() -> str:
    return "That looks like your number. Send your partner's phone number instead."


def partner_invite_not_configured() -> str:
    return "I am not able to start the shared thread yet. Linq sending still needs to be configured."


def partner_invite_started(phone: str) -> str:
    return f"I will start a shared household thread with {phone} now."


def partner_confirmed(phone: str) -> str:
    return f"Done. I confirmed {phone} as the second parent in this household."


def partner_group_intro() -> str:
    return (
        "Hi, I’m Florence. I help parents coordinate the household from this shared thread. "
        "I’ll keep reminders, school details, calendar items, and noisy email organized. "
        "To finish setup, each parent can tell me what they want to be called from their own phone."
    )


def first_greeting() -> str:
    return (
        "Hi, I'm Florence. I can help keep the household moving here: reminders, "
        "school details, calendar notes, and the useful bits from email. "
        "You can just text me normally. What should I call you?"
    )


def onboarding_next_prompt(readiness: HouseholdReadiness) -> str | None:
    if readiness.ready:
        return "I have enough setup to start helping. Send me anything household-related."
    if readiness.parent_count < 2:
        if readiness.named_parent_count == 0:
            return "What should I call you?"
        return (
            "Next, who should I coordinate with you? Send your partner's phone number "
            "and I will help start the shared household thread."
        )
    if readiness.named_parent_count < 2:
        return "Next, ask your partner to send their name from their phone."
    if readiness.child_count == 0:
        return "Next, who are the kids I should know about?"
    if readiness.connected_account_count == 0:
        return (
            "Next, connect Google when you are ready so I can watch calendar and email "
            "for timely household things."
        )
    if readiness.source_preference_count == 0:
        return (
            "Last setup step: tell me what is always worth a text, like "
            "permission slips."
        )
    return "Tell me the next household detail you want me to remember."


def onboarding_greeting_resume(
    *,
    name: str | None = None,
    setup_url: str | None = None,
    role: str = "primary",
    next_prompt: str | None = None,
) -> str:
    greeting = f"Hi {name}." if name else "Hi."
    if setup_url:
        if role == "partner":
            return (
                f"{greeting} I can keep going from here. Use this link to connect Google, "
                f"confirm the household details, and set your tone preference: {setup_url}"
            )
        return (
            f"{greeting} I can keep going from here. Use this link for partner, kids, "
            f"location, caretakers, tone, and Google: {setup_url}"
        )
    if next_prompt:
        return f"{greeting} I can keep going from here. {next_prompt}"
    return f"{greeting} I can keep going from here. Tell me the next household detail you want me to know."


def setup_next_action(readiness: HouseholdReadiness) -> str | None:
    if readiness.ready:
        return None
    if readiness.parent_count < 2:
        if readiness.named_parent_count == 0:
            return "Tell me what I should call you."
        return "Send your partner's phone number so I can bring them into the household."
    if readiness.named_parent_count < 2:
        return "Ask the other parent to tell me their name from their phone."
    if readiness.child_count == 0:
        return "Tell me your child or children's names."
    if readiness.connected_account_count == 0:
        return "Say you want to connect Google Calendar and Gmail."
    if readiness.source_preference_count == 0:
        return "Tell me one thing that is always worth a text, like permission slips."
    return "Tell me the next household detail you want me to remember."


def readiness_status(readiness: HouseholdReadiness) -> str:
    lines = [
        "Household setup:",
        f"- Parents seen: {readiness.parent_count}/2",
        f"- Parents named: {readiness.named_parent_count}/2",
        f"- Children: {readiness.child_count}",
        f"- Connected sources: {readiness.connected_account_count}",
        f"- Source rules: {readiness.source_preference_count}",
        f"- Timezone: {readiness.timezone}",
    ]
    if readiness.ready:
        lines.append("Ready for a pilot. I have enough household context to start helping.")
    else:
        lines.append("Next steps:")
        for item in readiness.missing[:4]:
            lines.append(f"- {item}")
        action = setup_next_action(readiness)
        if action is not None:
            lines.append(f"Next action: {action}")
    return "\n".join(lines)


def name_needed() -> str:
    return "What name should I use for you in this household?"


def name_too_long() -> str:
    return "Use a shorter name for now, and I will keep it tidy here."


def timezone_unknown() -> str:
    return "I do not recognize that timezone. Try something like America/Los_Angeles."


def household_status(
    *,
    timezone_name: str,
    people: list[tuple[str, str]],
    parent_count: int,
    ready: bool,
    next_action: str | None,
) -> str:
    lines = [f"Household timezone: {timezone_name}", "People I have seen here:"]
    for label, role in people:
        lines.append(f"- {label} ({role})")
    if parent_count < 2:
        lines.append("When you are ready, add your partner to this iMessage thread.")
    lines.append("Setup: ready for a pilot." if ready else f"Setup next action: {next_action}")
    return "\n".join(lines)


def daily_briefing(
    *,
    reminders: list[Reminder],
    sources: list[BriefingSourceItem],
    timezone_name: str,
) -> str:
    if not reminders and not sources:
        return "Good morning. Nothing urgent is on my list for today."

    lines = ["Good morning. Here is what needs attention:"]
    for reminder in reminders[:5]:
        lines.append(f"- {_reminder_label(reminder)} ({format_local(reminder.due_at_utc, timezone_name)})")
    for source in sources[:3]:
        if source.event_at_utc is None:
            lines.append(f"- {source.title}")
        else:
            lines.append(f"- {source.title} ({format_local(source.event_at_utc, timezone_name)})")
    return "\n".join(lines)


def agenda_today(
    *,
    reminders: list[Reminder],
    sources: list[BriefingSourceItem],
    timezone_name: str,
) -> str:
    if not reminders and not sources:
        return no_agenda()

    lines = ["Here is what I have for today:"]
    for reminder in reminders[:5]:
        lines.append(f"- {_reminder_label(reminder)} ({format_local(reminder.due_at_utc, timezone_name)})")
    for source in sources[:3]:
        if source.event_at_utc is None:
            lines.append(f"- {source.title}")
        else:
            lines.append(f"- {source.title} ({format_local(source.event_at_utc, timezone_name)})")
    return "\n".join(lines)


def tomorrow_prep(
    *,
    reminders: list[Reminder],
    sources: list[BriefingSourceItem],
    timezone_name: str,
) -> str:
    if not reminders and not sources:
        return "I do not see anything specific to prep for tomorrow."

    lines = ["Tomorrow prep:"]
    for reminder in reminders[:6]:
        lines.append(f"- {_reminder_label(reminder)} ({format_local(reminder.due_at_utc, timezone_name)})")
    for source in sources[:4]:
        lines.append(f"- {source.title} ({format_local(source.event_at_utc, timezone_name)})")
    lines.append("When something is handled, text me 'done' and the item.")
    return "\n".join(lines)


def memory_snapshot(snapshot: MemorySnapshot) -> str:
    if not snapshot.memories:
        return (
            "The household book is empty right now. Text 'remember that ...' "
            "when there is something you want me to keep."
        )

    lines = ["Here is what I remember for this household book:"]
    for memory in snapshot.memories[:10]:
        source = f" (from {memory.asserted_by_label})" if memory.asserted_by_label else ""
        lines.append(f"- {memory.text}{source}")
    if len(snapshot.memories) > 10:
        lines.append(f"- Plus {len(snapshot.memories) - 10} more.")
    lines.append("To remove one, text me 'forget' and the detail.")
    return "\n".join(lines)


def memory_paused() -> str:
    return "Household memory is paused. I will not add to the household book until a parent turns it back on."


def memory_resumed() -> str:
    return "Household memory is back on. I will use the household book only for this household."


def memory_disabled() -> str:
    return "Household memory is paused, so I did not save that to the household book."


def memory_parent_only() -> str:
    return "I need one of the parents in this household to change the household book."


def memory_view_parent_only() -> str:
    return "I need one of the parents in this household to view the household book."


def memory_needs_text() -> str:
    return "What should I add to the household book?"


def memory_too_long(limit: int) -> str:
    return f"That is a bit long for the household book. Send the short version in {limit} characters or fewer."


def forget_needs_text() -> str:
    return "What should I remove from the household book?"


def memory_saved(text: str) -> str:
    return f"Got it. I added this to the household book: {text}."


def memory_removed() -> str:
    return "Done. I removed that from the household book."


def memory_cleared(count: int) -> str:
    if count == 1:
        return "Done. I cleared 1 household memory from the household book."
    return f"Done. I cleared {count} household memories from the household book."


def memory_clear_empty() -> str:
    return "There are no active household book items to clear."


def memory_not_found() -> str:
    return "I could not find that in the household book."


def privacy_parent_only() -> str:
    return "I need one of the parents in this household to change privacy settings."


def analytics_opted_in() -> str:
    return "Done. Product analytics are on for this household, using only de-identified aggregate signals."


def analytics_opted_out() -> str:
    return "Done. Product analytics are off for this household."


def privacy_status(privacy: HouseholdPrivacy) -> str:
    memory = "on" if privacy.memory_enabled else "paused"
    analytics = "on" if privacy.product_analytics_opt_in else "off"
    return (
        "Privacy for this household:\n"
        f"- Mode: {privacy.mode.value}\n"
        f"- Household book memory: {memory}\n"
        f"- Product analytics: {analytics}\n"
        "- Cross-family memory sharing: off"
    )


def google_connection_parent_only() -> str:
    return "I need one of the parents in this household to connect Google."


def google_disconnect_parent_only() -> str:
    return "I need one of the parents in this household to disconnect Google."


def google_connection_not_configured() -> str:
    return "I am not able to make the Google connection link yet. Google OAuth and token encryption still need to be configured."


def google_connection_link(
    authorization_url: str,
    expires_at_utc: datetime,
    timezone_name: str,
) -> str:
    return (
        "Use this link to connect Google Calendar and Gmail for this household:\n"
        f"{authorization_url}\n"
        f"It expires at {format_local(expires_at_utc, timezone_name)}. "
        "After that, I will only surface items that look timely and actionable."
    )


def google_connected(account_label: str | None) -> str:
    label = f" for {account_label}" if account_label else ""
    return (
        f"Google is connected{label}. I will keep noisy email and calendar items quiet "
        "unless they look timely and actionable. Text 'source review' anytime to see the summary."
    )


def google_disconnected(count: int) -> str:
    account = "account" if count == 1 else "accounts"
    return (
        f"Google is disconnected for this household. I removed the stored Google token for "
        f"{count} {account} and will stop polling Gmail and Calendar."
    )


def google_disconnect_empty() -> str:
    return "There are no active Google accounts connected for this household."


def pending_action_request(action: PendingAction) -> str:
    code = action.id[:8]
    return (
        f"I can do this, but I need a parent to approve first: {action.summary}\n"
        f"Reply 'approve {code}' or 'cancel {code}'."
    )


def agent_reply_needs_approval_guard() -> str:
    return "I can help with that."


def agent_reply_no_state_change_guard() -> str:
    return "I hear you. I did not make any household changes from that."


def approval_parent_only() -> str:
    return "I need one of the parents in this household to approve that."


def approval_not_found() -> str:
    return "I could not find an active approval with that code. It may already be handled or expired."


def approval_approved(action: PendingAction) -> str:
    return f"Approved: {action.summary}"


def approval_canceled(action: PendingAction) -> str:
    return f"Canceled: {action.summary}"


def handoff_parent_only() -> str:
    return "I need one of the parents in this household to view the household handoff."


def household_handoff(
    *,
    approvals: list[PendingAction],
    reminders: list[Reminder],
    timezone_name: str,
) -> str:
    if not approvals and not reminders:
        return "Household handoff is clear. I do not see pending approvals or upcoming reminders."

    lines = ["Household handoff:"]
    if approvals:
        lines.append("Needs parent approval:")
        for action in approvals[:5]:
            lines.append(f"- {action.summary} (approve {action.id[:8]} or cancel {action.id[:8]})")
    if reminders:
        lines.append("Coming up:")
        for reminder in reminders[:6]:
            lines.append(f"- {_reminder_label(reminder)} ({format_local(reminder.due_at_utc, timezone_name)})")
    return "\n".join(lines)


def source_preference_saved(preference: SourcePreference) -> str:
    if preference.preference == SourcePreferenceKind.ALWAYS_SURFACE:
        return f"Got it. I will make a point to tell you about {preference.phrase}."
    return f"Got it. I will keep {preference.phrase} quiet unless it becomes clearly urgent."


def source_preference_needs_phrase(preference: SourcePreferenceKind) -> str:
    if preference == SourcePreferenceKind.ALWAYS_SURFACE:
        return "What should I always tell you about?"
    return "What should I keep quiet about?"


def source_preference_parent_only() -> str:
    return "I need one of the parents in this household to view or change source rules."


def source_preferences(preferences: list[SourcePreference]) -> str:
    if not preferences:
        return "No source preferences yet. A parent can text something like 'always tell me about permission slips'."
    lines = ["Source preferences for this household:"]
    for preference in preferences[:10]:
        if preference.preference == SourcePreferenceKind.ALWAYS_SURFACE:
            lines.append(f"- Tell me about: {preference.phrase}")
        else:
            lines.append(f"- Keep quiet about: {preference.phrase}")
    if len(preferences) > 10:
        lines.append(f"- Plus {len(preferences) - 10} more.")
    return "\n".join(lines)


def source_review(snapshot: SourceReviewSnapshot, timezone_name: str) -> str:
    if snapshot.total == 0:
        return (
            "I have not reviewed any connected email or calendar items yet. "
            "After a source sync, I will show counts here without dumping the inbox."
        )
    lines = [
        "Source review:",
        f"- Texted: {snapshot.surfaced}",
        f"- Kept quiet: {snapshot.stored_only}",
        f"- Suppressed as stale/past: {snapshot.suppressed}",
    ]
    if snapshot.recent_surfaced:
        lines.append("Recently texted:")
        for item in snapshot.recent_surfaced[:3]:
            lines.append(f"- {_source_review_item(item, timezone_name)}")
    if snapshot.recent_stored:
        lines.append("Recently kept quiet:")
        for item in snapshot.recent_stored[:3]:
            reason = _source_reason(item.reason)
            lines.append(f"- {_source_review_item(item, timezone_name)} ({reason})")
    lines.append("To tune this, text 'always tell me about ...' or 'mute ...'.")
    return "\n".join(lines)


def source_review_parent_only() -> str:
    return "I need one of the parents in this household to review connected-source items."


def source_feedback_without_recent_item() -> str:
    return "I do not have a recent source item to tune from yet. I will use feedback after I surface something."


def source_reference_missing(reference_kind: str) -> str:
    if reference_kind == "domain":
        return "I do not have a recent source item with an email domain to tune from."
    return "I do not have a recent source item with a sender to tune from."


def source_feedback_saved(feedback: SourceFeedbackKind, preference: SourcePreference) -> str:
    if feedback == SourceFeedbackKind.NOT_USEFUL:
        return f"Got it. I will keep {preference.phrase} quieter."
    return f"Got it. I will watch more closely for {preference.phrase}."


def _source_review_item(item: BriefingSourceItem, timezone_name: str) -> str:
    if item.event_at_utc is None:
        return item.title
    return f"{item.title} ({format_local(item.event_at_utc, timezone_name)})"


def _reminder_label(reminder: Reminder) -> str:
    if reminder.assignee_label:
        return f"{reminder.assignee_label}: {reminder.title}"
    return reminder.title


def _source_reason(reason: str | None) -> str:
    labels = {
        "actionable_without_known_due_time": "actionable, no clear due time",
        "automated_background_notice": "automated background notice",
        "event_is_in_the_past": "past item",
        "household_muted_source": "muted by your rule",
        "household_requested_source": "requested by your rule",
        "initial_sync_backfill": "first sync backfill",
        "low_signal_source": "low signal",
        "no_due_time_and_not_actionable": "no clear action",
        "upcoming_actionable_source": "upcoming action",
        "urgent_actionable_source": "urgent action",
        "useful_context_not_interrupt_worthy": "useful, not interrupt-worthy",
    }
    return labels.get(reason or "", "not interrupt-worthy")
