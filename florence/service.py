"""Florence application service."""

from __future__ import annotations

import html
import re
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from email.utils import parseaddr

from florence.agent_protocol import AgentActionProposal, AgentProposalBundle, extract_agent_proposals
from florence.config import Settings
from florence.hermes import AgentBackend, HermesBackend
from florence.models import (
    ActionExecution,
    ConnectedAccount,
    ConnectedAccountStatus,
    Household,
    HouseholdReadiness,
    HouseholdPrivacy,
    HouseholdMember,
    IncomingMessage,
    MemoryKind,
    MemorySnapshot,
    MemberRole,
    MessageAttachment,
    MessageDirection,
    OutboundMessage,
    PendingAction,
    PendingActionStatus,
    Reminder,
    ReminderStatus,
    SourceDecision,
    SourceFeedbackKind,
    SourcePreference,
    SourcePreferenceKind,
    SourceReviewSnapshot,
    SourceItem,
)
from florence.onboarding import create_onboarding_token
from florence.oauth import OAuthConfigurationError, OAuthStart, build_google_oauth_start
from florence.policy import NeedToKnowPolicy
from florence.source_ingest import (
    CalendarEventCandidate,
    EmailCandidate,
    calendar_event_to_source_item,
    email_to_source_item,
    normalize_source_body,
    normalize_source_sender,
    normalize_source_title,
)
from florence.source_providers import GoogleSourceProvider
from florence.store import Store
from florence.timekeeper import ensure_utc, format_local, parse_due_time, utc_now
from florence.timekeeper import resolve_timezone
from florence import tone


REMINDER_PREFIX = re.compile(
    r"\b(?:remind(?: me| us)?(?: to)?|don't let me forget(?: to)?)\b",
    re.IGNORECASE,
)
CALENDAR_EVENT_COLON = re.compile(r"^(?:calendar(?: event)?|event)\s*:\s*(.+)$", re.IGNORECASE)
CALENDAR_EVENT_ADD_TO = re.compile(
    r"^(?:add|put|save|create|schedule)\s+(?:this\s+)?(?:to|on)\s+(?:the\s+)?"
    r"(?:calendar|schedule)\s*:?\s*(.+)$",
    re.IGNORECASE,
)
CALENDAR_EVENT_WITH_OBJECT = re.compile(
    r"^(?:add|put|save|create|schedule)\s+(.+?)\s+(?:to|on|in)\s+(?:the\s+)?"
    r"(?:calendar|schedule)\b(.*)$",
    re.IGNORECASE,
)
CALENDAR_EVENT_AS_EVENT = re.compile(
    r"^(?:add|put|save|create|schedule)\s+(.+?)\s+as\s+(?:a\s+)?(?:calendar\s+)?event\b(.*)$",
    re.IGNORECASE,
)
REMINDER_COMPLETE = re.compile(
    r"^(?:done(?: with)?|complete|completed|handled|finished)\s+(?:the\s+)?(?:reminder\s+)?(.+)$",
    re.IGNORECASE,
)
REMINDER_CANCEL = re.compile(
    r"^(?:cancel|delete|remove)\s+(?:the\s+)?reminder(?:\s+for)?\s+(.+)$|^forget\s+reminder\s+(.+)$",
    re.IGNORECASE,
)
APPROVAL_REPLY = re.compile(r"^(approve|cancel)\s+([a-f0-9-]{4,})\b", re.IGNORECASE)
SOURCE_ALWAYS = re.compile(
    r"^(?:always tell me about|always surface|always show me)\s+(.+)",
    re.IGNORECASE,
)
SOURCE_MUTE = re.compile(
    r"^(?:mute|do not tell me about|don't tell me about|keep quiet about)\s+(.+)",
    re.IGNORECASE,
)
SOURCE_SENDER_REFERENCES = {
    "sender",
    "the sender",
    "this sender",
    "that sender",
    "email sender",
    "the email sender",
    "this email sender",
    "that email sender",
}
SOURCE_DOMAIN_REFERENCES = {
    "domain",
    "the domain",
    "this domain",
    "that domain",
    "sender domain",
    "the sender domain",
    "this sender domain",
    "that sender domain",
    "email domain",
    "the email domain",
    "this email domain",
    "that email domain",
}
INITIAL_SYNC_SURFACE_REASONS = {"urgent_actionable_source", "high_signal_without_known_due_time"}
CONNECTED_SOURCE_DUPLICATE_WINDOW = timedelta(days=1)
EMAIL_SEARCH_MAX_RESULTS = 5
EMAIL_SEARCH_AGENT_FOCUS = (
    "Answer only the current email-search request. Do not continue unrelated prior topics "
    "from conversation history unless the parent asks."
)
MAX_EXPLICIT_MEMORY_CHARS = 240
EMAIL_LIKE = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
CHILD_IS = re.compile(
    r"^(?:our child is|my child is|kid is|child is|our son is|my son is|our daughter is|my daughter is)\s+(.+)",
    re.IGNORECASE,
)
CHILDREN_ARE = re.compile(
    r"^(?:our children are|my children are|our kids are|my kids are|kids are|children are)\s+(.+)",
    re.IGNORECASE,
)
CHILDREN_NATURAL = re.compile(
    r"^(?:we have|we've got|we got|i have|i've got|our family has)\s+"
    r"(?:(?:one|two|three|four|five|six|\d+)\s+)?"
    r"(?:kids?|children|child)\b\s*[:,-]?\s*(?:named\s+|called\s+)?(.+)$",
    re.IGNORECASE,
)
CHILDREN_MEMORY = re.compile(r"^(?:.+?['’]s\s+)?(?:children|kids)\s+are\s+(.+)$", re.IGNORECASE)
CHILD_ENTRY = re.compile(
    r"(?P<name>[A-Z][A-Za-z' -]*?)(?:,?\s*age\s+(?P<age>\d{1,2}|[A-Za-z]+))?"
    r"(?=(?:,\s*(?:and\s+)?[A-Z])|(?:\s+and\s+[A-Z])|$)"
)
PARTNER_INVITE = re.compile(
    r"^(?:invite|add|bring in)\s+(?:my\s+|our\s+)?(?:partner|coparent|co-parent|spouse)\s*(.*)$"
    r"|^(?:my|our)\s+(?:partner|coparent|co-parent|spouse)\s+(?:is|number is)\s+(.+)$",
    re.IGNORECASE,
)
PARTNER_CONFIRM = re.compile(
    r"^(?:confirm|mark|make)\s+(?:my\s+|our\s+)?(?:partner|coparent|co-parent|spouse)\s*(.*)$",
    re.IGNORECASE,
)
PHONE_LIKE = re.compile(r"\+?\d[\d\s().-]{6,}\d")
ACTION_DONE_CLAIM = re.compile(
    r"\b(?:"
    r"(?:i\s*(?:have|'ve|will|'ll)?\s*)?"
    r"(?:set|added|created|scheduled|saved|put|made)\b[^.!?\n]{0,80}"
    r"\b(?:reminder|task|alarm|calendar|event)"
    r"|(?:reminder|task|alarm|calendar|event)\b[^.!?\n]{0,80}"
    r"\b(?:set|added|created|scheduled|saved)"
    r"|(?:i\s*(?:will|'ll)\s*)?remind\s+(?:you|y'all|ya|the household|everyone|us)\b"
    r")",
    re.IGNORECASE,
)
MEMORY_DONE_CLAIM = re.compile(
    r"\b(?:"
    r"(?:i\s*(?:have|'ve|will|'ll)?\s*)?"
    r"(?:remembered|remember|saved|stored|noted|kept)\b[^.!?\n]{0,80}"
    r"\b(?:that|this|it|memory|preference|fact|detail)"
    r"|(?:i\s*(?:have|'ve)?\s*)?(?:remembered|saved|stored|noted)\b"
    r"|keep(?:ing)?\s+(?:that|this|it)\s+in\s+mind"
    r")",
    re.IGNORECASE,
)
SOURCE_RULE_DONE_CLAIM = re.compile(
    r"\b(?:"
    r"watch(?:ing)?\s+for"
    r"|keep(?:ing)?\s+(?:an\s+)?eye\s+(?:out\s+)?for"
    r"|make\s+(?:a\s+)?point\s+to\s+tell"
    r"|tell\s+you\s+about"
    r"|(?:mute|muted|keep(?:ing)?\s+[^.!?\n]{0,50}\s+quiet)"
    r")",
    re.IGNORECASE,
)
GOOGLE_CONNECT_COMMANDS = {
    "connect google",
    "connect my google",
    "connect our google",
    "connect google account",
    "connect my google account",
    "connect our google account",
    "connect gmail",
    "connect my gmail",
    "connect our gmail",
    "connect calendar",
    "connect my calendar",
    "connect our calendar",
    "connect email",
    "connect my email",
    "connect our email",
    "connect sources",
    "connect my sources",
    "connect our sources",
    "link google",
    "link my google",
    "link our google",
    "link google account",
    "link my google account",
    "link our google account",
    "link gmail",
    "link my gmail",
    "link our gmail",
    "link calendar",
    "link my calendar",
    "link our calendar",
    "link email",
    "link my email",
    "link our email",
    "link sources",
    "link my sources",
    "link our sources",
}
GOOGLE_CONNECT_NATURAL = re.compile(
    r"\b(?:connect|link|authorize)\s+(?:my\s+|our\s+)?"
    r"(?:google(?:\s+account)?|gmail|calendar|email|sources?)\b",
    re.IGNORECASE,
)
GOOGLE_CONNECT_ACCOUNT_NATURAL = re.compile(
    r"\b(?:add|set\s+up|setup)\s+(?:my\s+|our\s+)"
    r"(?:google(?:\s+account)?|gmail|calendar|email|sources?)\b",
    re.IGNORECASE,
)
GOOGLE_DISCONNECT_COMMANDS = {
    "disconnect google",
    "disconnect gmail",
    "disconnect calendar",
    "disconnect email",
    "disconnect sources",
    "unlink google",
    "unlink gmail",
    "unlink calendar",
    "unlink email",
    "unlink sources",
    "remove google",
    "remove gmail",
    "remove calendar",
    "remove email",
    "remove sources",
    "turn off google",
    "turn off gmail",
    "turn off calendar",
    "turn off email",
    "turn off sources",
    "turn off connected sources",
}
MEMORY_CLEAR_COMMANDS = {
    "clear household memory",
    "clear all household memory",
    "clear household book",
    "clear all household book",
    "delete household memory",
    "delete all household memory",
    "delete household book",
    "delete all household book",
    "forget all household memory",
    "forget all household book",
}
SUPPORT_COMMANDS = {
    "support",
    "human",
    "contact support",
    "customer support",
    "talk to a human",
    "talk with a human",
    "help me contact support",
}
DATA_DELETE_REQUEST_COMMANDS = {
    "delete my data",
    "delete our data",
    "delete household data",
    "delete all household data",
    "erase my data",
    "erase our data",
    "erase household data",
    "erase all household data",
    "close account",
    "delete account",
}
DATA_DELETE_CONFIRM_COMMANDS = {
    "confirm delete household data",
    "confirm delete my data",
    "confirm delete our data",
}
HOUSEHOLD_DELETION_TOMBSTONE_TTL = timedelta(hours=24)
DATA_SUMMARY_COMMANDS = {
    "data summary",
    "household data summary",
    "what data do you have",
    "what data do you have?",
    "what do you know about us",
    "what do you know about us?",
    "show household data",
    "show my data",
    "show our data",
}
SIMPLE_GREETING_COMMANDS = {
    "hi",
    "hi florence",
    "hi there",
    "hello",
    "hello florence",
    "hello there",
    "hey",
    "hey florence",
    "hey there",
}
NATURAL_ONBOARDING_CONTINUATIONS = {
    "ok",
    "okay",
    "k",
    "sure",
    "yes",
    "yep",
    "yeah",
    "sounds good",
    "got it",
    "cool",
    "great",
    "ready",
    "i'm ready",
    "im ready",
    "i am ready",
    "let's do it",
    "lets do it",
    "let's start",
    "lets start",
    "what now",
    "what next",
    "what do you need",
    "what should i do",
    "how do i set this up",
    "how do we set this up",
    "how does this work",
    "help me set this up",
    "continue",
    "continue setup",
    "keep going",
    "let's keep going",
    "lets keep going",
    "send me the link",
    "send the link",
    "where is the link",
    "where's the link",
    "setup link",
    "onboarding link",
    "can we keep going",
    "can i keep going",
    "can we set this up",
    "can i set this up",
}


@dataclass(frozen=True, slots=True)
class SourceSyncResult:
    account: ConnectedAccount
    imported: int
    surfaced: int
    messages: list[OutboundMessage]


@dataclass(frozen=True, slots=True)
class WebOnboardingResult:
    household_id: str
    actor_member_id: str
    role: str
    saved_memory_count: int
    saved_source_preference_count: int
    partner_phone: str | None = None
    partner_member_id: str | None = None
    partner_onboarding_url: str | None = None
    invite_text: str | None = None


class FlorenceService:
    def __init__(
        self,
        *,
        settings: Settings,
        store: Store | None = None,
        agent: AgentBackend | None = None,
        policy: NeedToKnowPolicy | None = None,
    ) -> None:
        self.settings = settings
        self.store = store or Store(settings.database_dsn)
        self.agent = agent or HermesBackend(settings)
        self.policy = policy or NeedToKnowPolicy()

    def agent_smoke_check(
        self,
        *,
        chat_id: str,
        now_utc: datetime | None = None,
    ) -> str:
        now = ensure_utc(now_utc or utc_now())
        household = self.store.get_household_by_chat(chat_id)
        if household is None:
            raise ValueError("household_not_found")
        members = self.store.list_members(household.id)
        if not members:
            raise ValueError("household_has_no_members")
        actor = next((member for member in members if member.role == MemberRole.PARENT), None)
        if actor is None:
            raise ValueError("household_has_no_parent")
        upcoming_reminders = self._with_reminder_assignee_labels(
            self.store.upcoming_reminders(
                household_id=household.id,
                now_utc=now,
                limit=8,
            )
        )
        privacy = self.store.household_privacy(household.id)
        memories = (
            self.store.list_memories(household_id=household.id, now_utc=now)
            if privacy.memory_enabled and actor.role == MemberRole.PARENT
            else []
        )
        return self.agent.complete(
            household=household,
            user_text=(
                "Pilot smoke check. Reply with one short sentence confirming Florence can reason "
                "through this household context. Do not propose reminders, memory, source rules, "
                "or external actions."
            ),
            conversation_history=[],
            upcoming=[
                (_reminder_context_title(reminder), reminder.due_at_utc)
                for reminder in upcoming_reminders
            ],
            memories=memories,
            members=members,
            actor=actor,
            now_utc=now,
            source_preferences=self.store.list_source_preferences(household.id),
            privacy=privacy,
            readiness=self._readiness(household.id, now),
        )

    def handle_incoming(
        self,
        incoming: IncomingMessage,
        *,
        now_utc: datetime | None = None,
    ) -> list[OutboundMessage]:
        now = ensure_utc(now_utc or utc_now())
        if self.store.has_household_deletion_tombstone(
            chat_id=incoming.chat_id,
            message_id=incoming.message_id,
            now_utc=now,
        ):
            return []
        existing_household = self.store.get_household_by_chat(incoming.chat_id)
        if existing_household is None:
            known_household = self.store.get_unique_household_by_member_phone(incoming.sender)
            if known_household is not None and self._should_attach_unknown_chat(
                known_household,
                incoming.text,
            ):
                existing_household = self.store.migrate_household_chat(
                    household_id=known_household.id,
                    new_chat_id=incoming.chat_id,
                    now_utc=now,
                )
        household = existing_household or self.store.get_or_create_household(
            chat_id=incoming.chat_id,
            timezone_name=self.settings.default_timezone,
            now_utc=now,
        )
        actor = self.store.get_or_create_member(
            household_id=household.id,
            phone=incoming.sender,
            now_utc=now,
        )
        inserted = self.store.save_message(
            household_id=household.id,
            chat_id=incoming.chat_id,
            direction=MessageDirection.INBOUND,
            sender=incoming.sender,
            body=_incoming_message_body(incoming),
            created_at_utc=incoming.received_at,
            message_id=incoming.message_id,
            actor_member_id=actor.id,
        )
        if not inserted:
            return []

        lower = " ".join(incoming.text.strip().lower().split())
        if existing_household is None and _simple_greeting(lower):
            return [self._out(household.id, incoming.chat_id, tone.first_greeting(), now)]
        if lower in {"help", "?"}:
            return [self._out(household.id, incoming.chat_id, tone.help_text(), now)]
        if lower in SUPPORT_COMMANDS:
            return [
                self._out(
                    household.id,
                    incoming.chat_id,
                    tone.support_contact(self.settings.support_contact),
                    now,
                )
            ]
        if lower in DATA_DELETE_CONFIRM_COMMANDS:
            if actor.role != MemberRole.PARENT:
                return [self._out(household.id, incoming.chat_id, tone.data_deletion_parent_only(), now)]
            confirmation_since = now - timedelta(
                minutes=max(1, self.settings.data_deletion_confirmation_ttl_minutes)
            )
            if not self.store.has_recent_data_deletion_request(
                household_id=household.id,
                actor_member_id=actor.id,
                request_commands=DATA_DELETE_REQUEST_COMMANDS,
                since_utc=confirmation_since,
                before_utc=now,
            ):
                return [
                    self._out(
                        household.id,
                        incoming.chat_id,
                        tone.data_deletion_confirmation_missing(),
                        now,
                    )
                ]
            outbound = self._out(household.id, incoming.chat_id, tone.data_deletion_complete(), now)
            self.store.record_household_deletion_message_tombstones(
                household_id=household.id,
                deleted_at_utc=now,
                expires_at_utc=now + HOUSEHOLD_DELETION_TOMBSTONE_TTL,
            )
            self.store.delete_household(household.id)
            return [outbound]
        if lower in DATA_DELETE_REQUEST_COMMANDS:
            if actor.role != MemberRole.PARENT:
                return [self._out(household.id, incoming.chat_id, tone.data_deletion_parent_only(), now)]
            return [self._out(household.id, incoming.chat_id, tone.data_deletion_confirm_prompt(), now)]
        if lower in DATA_SUMMARY_COMMANDS:
            if actor.role != MemberRole.PARENT:
                return [self._out(household.id, incoming.chat_id, tone.data_summary_parent_only(), now)]
            summary = self.store.household_data_summary(household_id=household.id, now_utc=now)
            return [self._out(household.id, incoming.chat_id, tone.household_data_summary(summary), now)]
        help_topic = _help_topic(lower)
        if help_topic is not None:
            return [self._out(household.id, incoming.chat_id, tone.help_text(help_topic), now)]
        if lower in {"stop", "stop household", "pause florence"}:
            if actor.role != MemberRole.PARENT:
                return [self._out(household.id, incoming.chat_id, tone.stop_parent_only(), now)]
            self.store.set_stopped(household.id, True)
            return [self._out(household.id, incoming.chat_id, tone.stopped(), now)]
        if lower in {"start", "restart", "resume", "resume florence"}:
            if actor.role != MemberRole.PARENT:
                return [self._out(household.id, incoming.chat_id, tone.stop_parent_only(), now)]
            self.store.set_stopped(household.id, False)
            return [self._out(household.id, incoming.chat_id, tone.resumed(), now)]
        source_disconnect = self._maybe_disconnect_source_account(
            household.id,
            actor,
            incoming.text,
            now,
        )
        if source_disconnect is not None:
            return [self._out(household.id, incoming.chat_id, source_disconnect, now)]
        if self.store.is_stopped(household.id):
            return []

        onboarding_continuation = self._maybe_resume_onboarding_naturally(
            household.id,
            actor,
            incoming.text,
            lower,
            now,
        )
        if onboarding_continuation is not None:
            return [self._out(household.id, incoming.chat_id, onboarding_continuation, now)]

        partner_invite = self._maybe_handle_partner_invite(household, actor, incoming.text, now)
        if partner_invite is not None:
            return partner_invite

        prompted_partner_invite = self._maybe_handle_prompted_partner_phone(
            household,
            actor,
            incoming.text,
            now,
        )
        if prompted_partner_invite is not None:
            return prompted_partner_invite

        setup_reply = self._maybe_handle_household_setup(household.id, actor, incoming.text, now)
        if setup_reply is not None:
            return [self._out(household.id, incoming.chat_id, setup_reply, now)]

        approval_reply = self._maybe_handle_pending_action(household.id, actor, incoming.text, now)
        if approval_reply is not None:
            return [self._out(household.id, incoming.chat_id, approval_reply, now)]

        handoff_reply = self._maybe_show_handoff(household, actor, lower, now)
        if handoff_reply is not None:
            return [self._out(household.id, incoming.chat_id, handoff_reply, now)]

        privacy_reply = self._maybe_handle_privacy(household.id, actor, incoming.text, now)
        if privacy_reply is not None:
            return [self._out(household.id, incoming.chat_id, privacy_reply, now)]

        connection_reply = self._maybe_handle_source_connection(
            household.id,
            incoming.chat_id,
            actor,
            incoming.text,
            now,
        )
        if connection_reply is not None:
            return [self._out(household.id, incoming.chat_id, connection_reply, now)]

        reminder_resolution_reply = self._maybe_resolve_reminder(
            household.id,
            actor,
            incoming.text,
            now,
        )
        if reminder_resolution_reply is not None:
            return [self._out(household.id, incoming.chat_id, reminder_resolution_reply, now)]

        source_feedback_reply = self._maybe_handle_source_feedback(
            household.id,
            actor,
            incoming.text,
            now,
        )
        if source_feedback_reply is not None:
            return [self._out(household.id, incoming.chat_id, source_feedback_reply, now)]

        email_search_reply = self._maybe_search_connected_email(household, actor, incoming, now)
        if email_search_reply is not None:
            return email_search_reply

        if self._agent_should_lead_active_conversation(
            household.id,
            actor,
            incoming,
            lower,
        ):
            attachment_reply = self._maybe_handle_attachments(household.id, incoming, now)
            if attachment_reply is not None:
                return attachment_reply
            memory_reply = self._maybe_update_memory(
                household.id,
                actor,
                incoming,
                now,
                acknowledge_success=False,
            )
            if memory_reply not in {None, ""}:
                return [self._out(household.id, incoming.chat_id, memory_reply, now)]
            source_preference_reply = self._maybe_update_source_preference(
                household.id,
                actor,
                incoming.text,
                now,
                acknowledge_success=False,
            )
            if source_preference_reply not in {None, ""}:
                return [self._out(household.id, incoming.chat_id, source_preference_reply, now)]
            return self._agent_turn(
                household=household,
                actor=actor,
                incoming=incoming,
                now=now,
            )

        memory_reply = self._maybe_update_memory(household.id, actor, incoming, now)
        if memory_reply is not None:
            return [self._out(household.id, incoming.chat_id, memory_reply, now)]

        memory_status = self._maybe_show_memory(household.id, actor, lower, now)
        if memory_status is not None:
            return [self._out(household.id, incoming.chat_id, memory_status, now)]

        source_preference_reply = self._maybe_update_source_preference(
            household.id,
            actor,
            incoming.text,
            now,
        )
        if source_preference_reply is not None:
            return [self._out(household.id, incoming.chat_id, source_preference_reply, now)]

        source_review_reply = self._maybe_show_source_review(household, actor, lower)
        if source_review_reply is not None:
            return [self._out(household.id, incoming.chat_id, source_review_reply, now)]

        source_preference_status = self._maybe_show_source_preferences(household.id, actor, lower)
        if source_preference_status is not None:
            return [self._out(household.id, incoming.chat_id, source_preference_status, now)]

        calendar_event_reply = self._maybe_add_calendar_event(household, actor, incoming, now)
        if calendar_event_reply is not None:
            return [self._out(household.id, incoming.chat_id, calendar_event_reply, now)]

        reminder_reply = self._maybe_create_reminder(household, incoming.text, now)
        if reminder_reply is not None:
            return [self._out(household.id, incoming.chat_id, reminder_reply, now)]

        attachment_reply = self._maybe_handle_attachments(household.id, incoming, now)
        if attachment_reply is not None:
            return attachment_reply

        if self._is_agenda_request(lower):
            return [self._out(household.id, incoming.chat_id, self._agenda(household.id, now), now)]

        if self._is_tomorrow_prep_request(lower):
            return [self._out(household.id, incoming.chat_id, self._tomorrow_prep(household.id, now), now)]

        bare_name_reply = self._maybe_handle_bare_name_reply(household.id, actor, incoming.text, now)
        if bare_name_reply is not None:
            return [self._out(household.id, incoming.chat_id, bare_name_reply, now)]

        return self._agent_turn(
            household=household,
            actor=actor,
            incoming=incoming,
            now=now,
        )

    def _should_attach_unknown_chat(self, household: Household, text: str) -> bool:
        members = self.store.list_members(household.id)
        parent_count = sum(1 for member in members if member.role == MemberRole.PARENT)
        if parent_count >= 2:
            return True
        lower = " ".join(text.strip().lower().split())
        return _manual_household_thread_handoff(lower)

    def _agent_should_lead_active_conversation(
        self,
        household_id: str,
        actor: HouseholdMember,
        incoming: IncomingMessage,
        lower: str,
    ) -> bool:
        if actor.role != MemberRole.PARENT:
            return False
        if _active_conversation_rail_command(incoming.text, lower):
            return False
        history = self.store.recent_messages(
            household_id,
            limit=6,
            exclude_message_id=incoming.message_id,
        )
        last_assistant = next(
            (
                message["content"]
                for message in reversed(history)
                if message["role"] == "assistant"
            ),
            "",
        )
        if not _assistant_prompt_expects_reply(last_assistant):
            return False
        if incoming.attachments and incoming.text.strip():
            return True
        return _looks_like_active_conversation_reply(incoming.text)

    def _agent_turn(
        self,
        *,
        household: Household,
        actor: HouseholdMember,
        incoming: IncomingMessage,
        now: datetime,
    ) -> list[OutboundMessage]:
        helper_context_since = (
            min(ensure_utc(actor.created_at_utc), ensure_utc(incoming.received_at))
            if actor.role != MemberRole.PARENT
            else None
        )
        upcoming_reminders = self._with_reminder_assignee_labels(
            self.store.upcoming_reminders(
                household_id=household.id,
                now_utc=now,
                created_since_utc=helper_context_since,
                limit=8,
            )
        )
        upcoming = [
            (_reminder_context_title(reminder), reminder.due_at_utc)
            for reminder in upcoming_reminders
        ]
        privacy = self.store.household_privacy(household.id)
        memories = (
            self.store.list_memories(household_id=household.id, now_utc=now)
            if privacy.memory_enabled and actor.role == MemberRole.PARENT
            else []
        )
        members = self.store.list_members(household.id)
        source_preferences = (
            self.store.list_source_preferences(household.id)
            if actor.role == MemberRole.PARENT
            else []
        )
        readiness = self._readiness(household.id, now)
        reply = self.agent.complete(
            household=household,
            user_text=_incoming_agent_text(incoming),
            conversation_history=self.store.recent_messages(
                household.id,
                since_utc=helper_context_since,
                exclude_message_id=incoming.message_id,
            ),
            upcoming=upcoming,
            memories=memories,
            members=members,
            actor=actor,
            now_utc=now,
            source_preferences=source_preferences,
            privacy=privacy,
            readiness=readiness,
        )
        return self._agent_outbound(
            household_id=household.id,
            chat_id=incoming.chat_id,
            actor=actor,
            source_message_id=incoming.message_id,
            reply=reply,
            now=now,
        )

    def ingest_source_item(
        self,
        *,
        chat_id: str,
        source_type: str,
        title: str,
        body: str,
        observed_at_utc: datetime | None = None,
        event_at_utc: datetime | None = None,
        sender: str | None = None,
        external_id: str | None = None,
        now_utc: datetime | None = None,
        mark_surfaced: bool = True,
    ) -> list[OutboundMessage]:
        now = ensure_utc(now_utc or utc_now())
        household = self.store.get_or_create_household(
            chat_id=chat_id,
            timezone_name=self.settings.default_timezone,
            now_utc=now,
        )
        item = SourceItem(
            id=_source_item_id(
                household_id=household.id,
                source_type=source_type,
                external_id=external_id,
            ),
            household_id=household.id,
            source_type=source_type,
            title=normalize_source_title(title) or "Household item",
            body=normalize_source_body(body),
            sender=normalize_source_sender(sender),
            external_id=external_id,
            observed_at_utc=ensure_utc(observed_at_utc or now),
            event_at_utc=ensure_utc(event_at_utc) if event_at_utc else None,
        )
        _, outbound = self._ingest_normalized_source_item(
            household_id=household.id,
            chat_id=chat_id,
            item=item,
            now=now,
            mark_surfaced=mark_surfaced,
        )
        return outbound

    def ingest_email(
        self,
        *,
        chat_id: str,
        subject: str,
        body: str,
        sender: str,
        received_at_utc: datetime,
        external_id: str | None = None,
        event_at_utc: datetime | None = None,
        now_utc: datetime | None = None,
        mark_surfaced: bool = True,
    ) -> list[OutboundMessage]:
        now = ensure_utc(now_utc or utc_now())
        household = self.store.get_or_create_household(
            chat_id=chat_id,
            timezone_name=self.settings.default_timezone,
            now_utc=now,
        )
        item = email_to_source_item(
            EmailCandidate(
                household_id=household.id,
                subject=subject,
                body=body,
                sender=sender,
                received_at_utc=received_at_utc,
                external_id=external_id,
                event_at_utc=event_at_utc,
            )
        )
        _inserted, outbound = self._ingest_normalized_source_item(
            household_id=household.id,
            chat_id=chat_id,
            item=item,
            now=now,
            mark_surfaced=mark_surfaced,
        )
        return outbound

    def ingest_calendar_event(
        self,
        *,
        chat_id: str,
        title: str,
        starts_at_utc: datetime,
        ends_at_utc: datetime | None = None,
        location: str | None = None,
        description: str | None = None,
        calendar_name: str | None = None,
        external_id: str | None = None,
        observed_at_utc: datetime | None = None,
        now_utc: datetime | None = None,
        mark_surfaced: bool = True,
    ) -> list[OutboundMessage]:
        now = ensure_utc(now_utc or utc_now())
        household = self.store.get_or_create_household(
            chat_id=chat_id,
            timezone_name=self.settings.default_timezone,
            now_utc=now,
        )
        item = calendar_event_to_source_item(
            CalendarEventCandidate(
                household_id=household.id,
                title=title,
                starts_at_utc=starts_at_utc,
                ends_at_utc=ends_at_utc,
                location=location,
                description=description,
                calendar_name=calendar_name,
                external_id=external_id,
                observed_at_utc=observed_at_utc or now,
            )
        )
        _inserted, outbound = self._ingest_normalized_source_item(
            household_id=household.id,
            chat_id=chat_id,
            item=item,
            now=now,
            mark_surfaced=mark_surfaced,
        )
        return outbound

    def sync_connected_sources(
        self,
        *,
        chat_id: str,
        provider: str,
        external_account_id: str,
        account_label: str | None = None,
        emails: list[dict[str, object]] | None = None,
        calendar_events: list[dict[str, object]] | None = None,
        cursor: str | None = None,
        now_utc: datetime | None = None,
        mark_surfaced: bool = True,
    ) -> SourceSyncResult:
        now = ensure_utc(now_utc or utc_now())
        household = self.store.get_or_create_household(
            chat_id=chat_id,
            timezone_name=self.settings.default_timezone,
            now_utc=now,
        )
        existing_account = self.store.get_connected_account(
            household_id=household.id,
            provider=provider,
            external_account_id=external_account_id,
        )
        if existing_account is not None and existing_account.status == ConnectedAccountStatus.DISABLED:
            return SourceSyncResult(
                account=existing_account,
                imported=0,
                surfaced=0,
                messages=[],
            )
        account = self.store.upsert_connected_account(
            household_id=household.id,
            provider=provider,
            external_account_id=external_account_id,
            account_label=account_label,
            now_utc=now,
        )
        initial_sync = account.last_synced_at_utc is None
        imported = 0
        messages: list[OutboundMessage] = []

        for email in emails or []:
            item = email_to_source_item(
                EmailCandidate(
                    household_id=household.id,
                    subject=str(email.get("subject") or ""),
                    body=str(email.get("body") or ""),
                    sender=str(email.get("sender") or ""),
                    received_at_utc=_coerce_dt(email.get("received_at_utc"), now),
                    external_id=_optional_str(email.get("external_id")),
                    event_at_utc=_coerce_optional_dt(email.get("event_at_utc")),
                    connected_account_id=account.id,
                )
            )
            inserted, outbound = self._ingest_normalized_source_item(
                household_id=household.id,
                chat_id=chat_id,
                item=item,
                now=now,
                initial_sync=initial_sync,
                mark_surfaced=mark_surfaced,
            )
            imported += 1 if inserted else 0
            messages.extend(outbound)

        for event in calendar_events or []:
            starts_at = _coerce_optional_dt(event.get("starts_at_utc"))
            if starts_at is None:
                continue
            item = calendar_event_to_source_item(
                CalendarEventCandidate(
                    household_id=household.id,
                    title=str(event.get("title") or ""),
                    starts_at_utc=starts_at,
                    ends_at_utc=_coerce_optional_dt(event.get("ends_at_utc")),
                    location=_optional_str(event.get("location")),
                    description=_optional_str(event.get("description")),
                    calendar_name=_optional_str(event.get("calendar_name")),
                    external_id=_optional_str(event.get("external_id")),
                    observed_at_utc=_coerce_optional_dt(event.get("observed_at_utc")),
                    connected_account_id=account.id,
                )
            )
            inserted, outbound = self._ingest_normalized_source_item(
                household_id=household.id,
                chat_id=chat_id,
                item=item,
                now=now,
                initial_sync=initial_sync,
                mark_surfaced=mark_surfaced,
            )
            imported += 1 if inserted else 0
            messages.extend(outbound)

        account = self.store.update_connected_account_sync(
            account_id=account.id,
            cursor=cursor,
            synced_at_utc=now,
        )
        return SourceSyncResult(
            account=account,
            imported=imported,
            surfaced=len(messages),
            messages=messages,
        )

    def connected_accounts(self, *, chat_id: str) -> list[ConnectedAccount]:
        household = self.store.get_household_by_chat(chat_id)
        if household is None:
            raise ValueError("household_not_found")
        return self.store.list_connected_accounts(household.id)

    def start_google_oauth(
        self,
        *,
        chat_id: str,
        account_label: str | None = None,
        return_path: str | None = None,
        now_utc: datetime | None = None,
    ) -> OAuthStart:
        now = ensure_utc(now_utc or utc_now())
        self.store.get_or_create_household(
            chat_id=chat_id,
            timezone_name=self.settings.default_timezone,
            now_utc=now,
        )
        start = build_google_oauth_start(self.settings, now_utc=now)
        self.store.create_oauth_state(
            state=start.state,
            provider="google",
            chat_id=chat_id,
            account_label=account_label,
            return_path=return_path,
            expires_at_utc=start.expires_at_utc,
            now_utc=now,
        )
        return start

    def onboarding_url(
        self,
        *,
        chat_id: str,
        member_id: str,
        role: str,
        now_utc: datetime | None = None,
    ) -> str | None:
        base_url = _web_base_url(self.settings.web_base_url)
        secret = self.settings.onboarding_state_secret
        if not base_url or not secret:
            return None
        now = ensure_utc(now_utc or utc_now())
        expires_at = now + timedelta(hours=max(1, self.settings.onboarding_token_ttl_hours))
        token = create_onboarding_token(
            secret=secret,
            chat_id=chat_id,
            member_id=member_id,
            role=role,
            expires_at_utc=expires_at,
        )
        return f"{base_url}/onboarding/{token}"

    def apply_web_onboarding(
        self,
        *,
        chat_id: str,
        actor_member_id: str,
        role: str,
        data: dict[str, object],
        now_utc: datetime | None = None,
    ) -> WebOnboardingResult:
        now = ensure_utc(now_utc or utc_now())
        household = self.store.get_household_by_chat(chat_id)
        if household is None:
            raise ValueError("household_not_found")
        actor = _member_by_id(self.store.list_members(household.id), actor_member_id)
        if actor is None:
            raise ValueError("member_not_found")
        if actor.role != MemberRole.PARENT:
            raise ValueError("parent_required")

        parent_name = _web_text(data.get("parent_name"), max_chars=60)
        if parent_name:
            actor = self.store.set_member_name(actor.id, parent_name, now_utc=now)

        household_profile_source = "web-onboarding:household-profile"
        self.store.delete_memories_by_source(
            household_id=household.id,
            source_message_id=household_profile_source,
            now_utc=now,
        )
        saved_memory_count = 0
        for kind, subject, text, confidence in _web_household_memories(data):
            self.store.upsert_memory(
                household_id=household.id,
                kind=kind,
                subject=subject,
                text=text,
                confidence=confidence,
                asserted_by_member_id=actor.id,
                source_message_id=household_profile_source,
                now_utc=now,
            )
            saved_memory_count += 1

        tone_source = f"web-onboarding:tone:{actor.id}"
        self.store.delete_memories_by_source(
            household_id=household.id,
            source_message_id=tone_source,
            now_utc=now,
        )
        tone_preference = _web_text(data.get("tone_preference"), max_chars=160)
        if tone_preference:
            self.store.upsert_memory(
                household_id=household.id,
                kind=MemoryKind.PREFERENCE,
                subject=actor.display_name or actor.phone,
                text=f"Tone preference for {actor.display_name or actor.phone}: {tone_preference}.",
                confidence=0.9,
                asserted_by_member_id=actor.id,
                source_message_id=tone_source,
                now_utc=now,
            )
            saved_memory_count += 1

        saved_source_preference_count = 0
        for raw_rule in _web_list(data.get("source_rule"), max_items=5, max_chars=120):
            raw_rule = _web_source_rule_phrase(raw_rule)
            phrase, error = self._source_preference_phrase(household.id, raw_rule)
            if error or not phrase:
                continue
            self.store.upsert_source_preference(
                household_id=household.id,
                phrase=phrase,
                preference=SourcePreferenceKind.ALWAYS_SURFACE,
                created_by_member_id=actor.id,
                now_utc=now,
            )
            saved_source_preference_count += 1

        partner_phone = _phone_from_text(_web_text(data.get("partner_phone"), max_chars=80))
        partner_member_id = None
        partner_url = None
        invite_text = None
        if role == "primary" and partner_phone and partner_phone != actor.phone:
            partner_member = self.store.ensure_parent_member(
                household_id=household.id,
                phone=partner_phone,
                now_utc=now,
            )
            if partner_member is not None:
                partner_member_id = partner_member.id
                partner_url = self.onboarding_url(
                    chat_id=chat_id,
                    member_id=partner_member.id,
                    role="partner",
                    now_utc=now,
                )
                invite_text = _partner_web_invite_text(partner_url)

        return WebOnboardingResult(
            household_id=household.id,
            actor_member_id=actor.id,
            role=role,
            saved_memory_count=saved_memory_count,
            saved_source_preference_count=saved_source_preference_count,
            partner_phone=partner_phone,
            partner_member_id=partner_member_id,
            partner_onboarding_url=partner_url,
            invite_text=invite_text,
        )

    def google_connected_confirmation(
        self,
        *,
        chat_id: str,
        account_label: str | None,
        oauth_state: str | None = None,
        now_utc: datetime | None = None,
    ) -> OutboundMessage:
        now = ensure_utc(now_utc or utc_now())
        household = self.store.get_household_by_chat(chat_id)
        if household is None:
            raise ValueError("household_not_found")
        delivery_source_message_id = f"oauth:google:{oauth_state}" if oauth_state else None
        return self._out(
            household.id,
            chat_id,
            tone.google_connected(account_label),
            now,
            idempotency_key=delivery_source_message_id,
            delivery_household_id=household.id if delivery_source_message_id else None,
            delivery_source_message_id=delivery_source_message_id,
        )

    def source_review_snapshot(self, *, chat_id: str, now_utc: datetime | None = None) -> SourceReviewSnapshot:
        household = self.store.get_household_by_chat(chat_id)
        if household is None:
            raise ValueError("household_not_found")
        return self.store.source_review_snapshot(household_id=household.id)

    def source_preferences(self, *, chat_id: str) -> list[SourcePreference]:
        household = self.store.get_household_by_chat(chat_id)
        if household is None:
            raise ValueError("household_not_found")
        return self.store.list_source_preferences(household.id)

    def memory_snapshot(self, *, chat_id: str, now_utc: datetime | None = None) -> MemorySnapshot:
        now = ensure_utc(now_utc or utc_now())
        household = self.store.get_household_by_chat(chat_id)
        if household is None:
            raise ValueError("household_not_found")
        return self.store.memory_snapshot(household_id=household.id, now_utc=now)

    def delete_memory(
        self,
        *,
        chat_id: str,
        memory_id: str,
        now_utc: datetime | None = None,
    ) -> bool:
        now = ensure_utc(now_utc or utc_now())
        household = self.store.get_household_by_chat(chat_id)
        if household is None:
            raise ValueError("household_not_found")
        return self.store.delete_memory(
            household_id=household.id,
            memory_id=memory_id,
            now_utc=now,
        )

    def privacy_snapshot(
        self,
        *,
        chat_id: str,
        now_utc: datetime | None = None,
    ) -> HouseholdPrivacy:
        household = self.store.get_household_by_chat(chat_id)
        if household is None:
            raise ValueError("household_not_found")
        return self.store.household_privacy(household.id)

    def readiness_snapshot(
        self,
        *,
        chat_id: str,
        now_utc: datetime | None = None,
    ) -> HouseholdReadiness:
        now = ensure_utc(now_utc or utc_now())
        household = self.store.get_household_by_chat(chat_id)
        if household is None:
            raise ValueError("household_not_found")
        return self._readiness(household.id, now)

    def create_pending_action(
        self,
        *,
        chat_id: str,
        action_type: str,
        summary: str,
        payload: dict[str, object] | None = None,
        sender: str | None = None,
        now_utc: datetime | None = None,
        expires_at_utc: datetime | None = None,
    ) -> list[OutboundMessage]:
        now = ensure_utc(now_utc or utc_now())
        household = self.store.get_or_create_household(
            chat_id=chat_id,
            timezone_name=self.settings.default_timezone,
            now_utc=now,
        )
        created_by_member_id = None
        if sender:
            created_by_member_id = self.store.get_or_create_member(
                household_id=household.id,
                phone=sender,
                now_utc=now,
            ).id
        expires_at = ensure_utc(expires_at_utc) if expires_at_utc else now + timedelta(
            minutes=self.settings.pending_action_ttl_minutes,
        )
        action = self.store.create_pending_action(
            household_id=household.id,
            chat_id=chat_id,
            action_type=action_type,
            summary=summary,
            payload=payload or {},
            created_by_member_id=created_by_member_id,
            created_at_utc=now,
            expires_at_utc=expires_at,
        )
        return [self._out(household.id, chat_id, tone.pending_action_request(action), now)]

    def pending_actions(
        self,
        *,
        chat_id: str,
        now_utc: datetime | None = None,
    ) -> list[PendingAction]:
        now = ensure_utc(now_utc or utc_now())
        household = self.store.get_household_by_chat(chat_id)
        if household is None:
            raise ValueError("household_not_found")
        return self.store.list_pending_actions(household_id=household.id, now_utc=now)

    def action_executions(self, *, chat_id: str, limit: int = 50) -> list[ActionExecution]:
        household = self.store.get_household_by_chat(chat_id)
        if household is None:
            raise ValueError("household_not_found")
        return self.store.list_action_executions(household_id=household.id, limit=limit)

    def due_reminder_messages(
        self,
        *,
        now_utc: datetime | None = None,
        mark_sent: bool = True,
    ) -> list[OutboundMessage]:
        now = ensure_utc(now_utc or utc_now())
        not_before = now - timedelta(minutes=self.settings.reminder_delivery_grace_minutes)
        self.store.expire_stale_reminders(before_utc=not_before)
        reminders = self._with_reminder_assignee_labels(
            self.store.due_reminders(now_utc=now, not_before_utc=not_before)
        )
        outbound = [
            OutboundMessage(
                chat_id=reminder.chat_id,
                text=tone.due_reminder(reminder.title, assignee_label=reminder.assignee_label),
                idempotency_key=f"reminder:{reminder.id}",
            )
            for reminder in reminders
        ]
        if mark_sent:
            self.store.mark_reminders_sent(reminder.id for reminder in reminders)
            for message in outbound:
                household = self.store.get_household_by_chat(message.chat_id)
                if household:
                    self.store.save_message(
                        household_id=household.id,
                        chat_id=message.chat_id,
                        direction=MessageDirection.OUTBOUND,
                        body=message.text,
                        created_at_utc=now,
                        message_id=message.idempotency_key,
                    )
        return outbound

    def mark_outbound_delivered(
        self,
        message: OutboundMessage,
        *,
        now_utc: datetime | None = None,
    ) -> None:
        now = ensure_utc(now_utc or utc_now())
        if message.delivery_source_message_id:
            self.store.mark_outbound_delivery_sent(
                idempotency_key=message.idempotency_key,
                now_utc=now,
            )
            if (
                message.delivery_household_id
                and message.delivery_source_message_id.startswith("source:")
            ):
                self.store.mark_source_items_surfaced(
                    household_id=message.delivery_household_id,
                    source_item_ids=[message.delivery_source_message_id.removeprefix("source:")],
                    surfaced_at_utc=now,
                )
        if message.idempotency_key.startswith("reminder:"):
            reminder_id = message.idempotency_key.removeprefix("reminder:")
            self.store.mark_reminders_sent([reminder_id])
        if (
            message.routine_household_id
            and message.routine_name
            and message.routine_local_date
        ):
            self.store.claim_routine_run(
                household_id=message.routine_household_id,
                routine_name=message.routine_name,
                local_date=message.routine_local_date,
                ran_at_utc=now,
            )
            self.store.mark_source_items_briefed(
                household_id=message.routine_household_id,
                source_item_ids=message.briefed_source_item_ids,
                briefed_at_utc=now,
            )
        household = self.store.get_household_by_chat(message.chat_id)
        if household:
            self.store.save_message(
                household_id=household.id,
                chat_id=message.chat_id,
                direction=MessageDirection.OUTBOUND,
                body=message.text,
                created_at_utc=now,
                message_id=message.idempotency_key,
            )

    def prepare_outbound_delivery(
        self,
        message: OutboundMessage,
        *,
        now_utc: datetime | None = None,
    ) -> None:
        if not message.delivery_household_id or not message.delivery_source_message_id:
            return
        self.store.record_outbound_deliveries_for_source(
            household_id=message.delivery_household_id,
            source_message_id=message.delivery_source_message_id,
            messages=[message],
            now_utc=ensure_utc(now_utc or utc_now()),
        )

    def mark_outbound_delivery_failed(
        self,
        message: OutboundMessage,
        *,
        error: str,
        now_utc: datetime | None = None,
    ) -> None:
        if not message.delivery_source_message_id:
            return
        self.store.mark_outbound_delivery_failed(
            idempotency_key=message.idempotency_key,
            error=error,
            now_utc=ensure_utc(now_utc or utc_now()),
        )

    def _ingest_normalized_source_item(
        self,
        *,
        household_id: str,
        chat_id: str,
        item: SourceItem,
        now: datetime,
        initial_sync: bool = False,
        mark_surfaced: bool = True,
        parent_submitted: bool = False,
        suppress_parent_submitted_surface: bool = False,
    ) -> tuple[bool, list[OutboundMessage]]:
        triage = self._classify_source_item(household_id=household_id, item=item, now=now)
        should_surface = triage.decision == SourceDecision.SURFACE
        decision = triage.decision
        reason = triage.reason
        if (
            initial_sync
            and triage.decision == SourceDecision.SURFACE
            and triage.reason not in INITIAL_SYNC_SURFACE_REASONS
        ):
            should_surface = False
            decision = SourceDecision.STORE_ONLY
            reason = "initial_sync_backfill"
        if self.store.is_stopped(household_id):
            should_surface = False
            if decision == SourceDecision.SURFACE:
                decision = SourceDecision.STORE_ONLY
                reason = "household_stopped"
        if parent_submitted and decision == SourceDecision.SURFACE and (
            suppress_parent_submitted_surface or reason == "household_requested_source"
        ):
            should_surface = False
            decision = SourceDecision.STORE_ONLY
            reason = "parent_submitted_context"
        if should_surface and self._is_recent_duplicate_connected_source(item, now):
            should_surface = False
            decision = SourceDecision.STORE_ONLY
            reason = "duplicate_connected_source"
        surfaced_at = now if should_surface and mark_surfaced else None
        inserted = self.store.add_source_item(
            item,
            decision=decision.value,
            reason=reason,
            priority=triage.priority,
            surfaced_at_utc=surfaced_at,
        )
        if not inserted:
            return inserted, self.store.retryable_source_outbound_deliveries_for_source(
                household_id=household_id,
                source_message_id=f"source:{item.id}",
            )
        if not should_surface:
            return inserted, []
        suggested_action = self._maybe_create_source_reminder_action(
            household_id=household_id,
            chat_id=chat_id,
            title=triage.suggested_title or item.title,
            due_at_utc=triage.suggested_due_at_utc,
            reason=triage.reason,
            now=now,
        )
        title = triage.suggested_title or item.title
        timezone_name = self._household_by_id(household_id).timezone
        if parent_submitted:
            text = tone.attachment_actionable_saved(
                title,
                triage.suggested_due_at_utc,
                timezone_name,
                suggested_action=suggested_action,
            )
        else:
            text = tone.source_surface(
                title,
                triage.suggested_due_at_utc,
                timezone_name,
                sender=_source_sender_label(item.sender),
                context=_source_surface_context(item, title),
                suggested_action=suggested_action,
            )
        return inserted, [
            self._out(
                household_id,
                chat_id,
                text,
                now,
                delivery_household_id=household_id,
                delivery_source_message_id=f"source:{item.id}",
                save_message=mark_surfaced,
            )
        ]

    def _is_recent_duplicate_connected_source(self, item: SourceItem, now: datetime) -> bool:
        if item.connected_account_id is None:
            return False
        return self.store.has_recent_similar_source_item(
            household_id=item.household_id,
            source_type=item.source_type,
            sender=item.sender,
            title=item.title,
            since_utc=ensure_utc(now) - CONNECTED_SOURCE_DUPLICATE_WINDOW,
            exclude_id=item.id,
        )

    def daily_briefing_messages(
        self,
        *,
        now_utc: datetime | None = None,
        mark_sent: bool = True,
    ) -> list[OutboundMessage]:
        now = ensure_utc(now_utc or utc_now())
        outbound: list[OutboundMessage] = []
        for household in self.store.list_households():
            local_now = now.astimezone(resolve_timezone(household.timezone))
            scheduled = local_now.replace(
                hour=self.settings.daily_briefing_hour,
                minute=self.settings.daily_briefing_minute,
                second=0,
                microsecond=0,
            )
            if local_now < scheduled:
                continue
            briefing_window = timedelta(
                minutes=max(0, self.settings.daily_briefing_delivery_grace_minutes)
            )
            if local_now > scheduled + briefing_window:
                continue
            local_date = local_now.date().isoformat()
            horizon = now + timedelta(hours=24)
            reminders = self.store.upcoming_reminders(
                household_id=household.id,
                now_utc=now,
                limit=5,
            )
            reminders = self._with_reminder_assignee_labels(
                [reminder for reminder in reminders if reminder.due_at_utc <= horizon]
            )
            sources = self.store.briefing_source_items(
                household_id=household.id,
                now_utc=now,
                horizon_utc=horizon,
                limit=3,
            )
            if not reminders and not sources:
                continue
            if mark_sent:
                claimed = self.store.claim_routine_run(
                    household_id=household.id,
                    routine_name="daily_briefing",
                    local_date=local_date,
                    ran_at_utc=now,
                )
            else:
                claimed = not self.store.routine_run_exists(
                    household_id=household.id,
                    routine_name="daily_briefing",
                    local_date=local_date,
                )
            if not claimed:
                continue
            text = tone.daily_briefing(
                reminders=reminders,
                sources=sources,
                timezone_name=household.timezone,
            )
            source_ids = tuple(source.id for source in sources)
            outbound.append(
                self._out(
                    household.id,
                    household.chat_id,
                    text,
                    now,
                    routine_household_id=household.id,
                    routine_name="daily_briefing",
                    routine_local_date=local_date,
                    briefed_source_item_ids=source_ids,
                    idempotency_key=f"routine:daily_briefing:{household.id}:{local_date}",
                    save_message=mark_sent,
                )
            )
            if mark_sent:
                self.store.mark_source_items_briefed(
                    household_id=household.id,
                    source_item_ids=source_ids,
                    briefed_at_utc=now,
                )
        return outbound

    def _classify_source_item(self, *, household_id: str, item: SourceItem, now: datetime):
        preferences = self.store.list_source_preferences(household_id)
        return self.policy.classify(item, now_utc=now, preferences=preferences)

    def _maybe_handle_attachments(
        self,
        household_id: str,
        incoming: IncomingMessage,
        now: datetime,
    ) -> list[OutboundMessage] | None:
        if not incoming.attachments:
            return None
        outbound: list[OutboundMessage] = []
        inserted_count = 0
        household = self._household_by_id(household_id)
        continue_agent_turn = self._attachment_answers_recent_assistant_prompt(
            household_id,
            incoming,
        )
        for index, attachment in enumerate(incoming.attachments, start=1):
            item = _attachment_source_item(
                household_id=household_id,
                incoming=incoming,
                attachment=attachment,
                index=index,
                timezone_name=household.timezone,
                now=now,
            )
            inserted, surfaced = self._ingest_normalized_source_item(
                household_id=household_id,
                chat_id=incoming.chat_id,
                item=item,
                now=now,
                parent_submitted=True,
                suppress_parent_submitted_surface=continue_agent_turn,
            )
            if inserted:
                inserted_count += 1
            if surfaced and not continue_agent_turn and not outbound:
                outbound.extend(surfaced)
        if continue_agent_turn:
            return None
        if outbound:
            return outbound
        if incoming.text.strip() or any(attachment.extracted_text for attachment in incoming.attachments):
            return [self._out(household_id, incoming.chat_id, tone.attachment_saved(), now)]
        if inserted_count:
            return [
                self._out(
                    household_id,
                    incoming.chat_id,
                    tone.attachment_needs_context(len(incoming.attachments)),
                    now,
                )
            ]
        return []

    def _attachment_answers_recent_assistant_prompt(
        self,
        household_id: str,
        incoming: IncomingMessage,
    ) -> bool:
        if not incoming.attachments or not incoming.text.strip():
            return False
        history = self.store.recent_messages(
            household_id,
            limit=6,
            exclude_message_id=incoming.message_id,
        )
        last_assistant = next(
            (
                message["content"]
                for message in reversed(history)
                if message["role"] == "assistant"
            ),
            "",
        )
        return _assistant_prompt_invited_attachment_reply(last_assistant)

    def _with_reminder_assignee_labels(self, reminders: list[Reminder]) -> list[Reminder]:
        member_cache: dict[str, dict[str, HouseholdMember]] = {}
        label_cache: dict[str, dict[str, str]] = {}
        labeled = []
        for reminder in reminders:
            if not reminder.assignee_member_id:
                labeled.append(reminder)
                continue
            members_by_id = member_cache.setdefault(
                reminder.household_id,
                {member.id: member for member in self.store.list_members(reminder.household_id)},
            )
            member = members_by_id.get(reminder.assignee_member_id)
            if member is None:
                labeled.append(reminder)
                continue
            labels_by_id = label_cache.setdefault(
                reminder.household_id,
                _household_member_labels(list(members_by_id.values())),
            )
            labeled.append(
                replace(
                    reminder,
                    assignee_label=labels_by_id[member.id],
                )
            )
        return labeled

    def _agent_outbound(
        self,
        *,
        household_id: str,
        chat_id: str,
        actor: HouseholdMember,
        source_message_id: str,
        reply: str,
        now: datetime,
    ) -> list[OutboundMessage]:
        bundle = extract_agent_proposals(reply)
        privacy = self.store.household_privacy(household_id)
        saved_memory_count = 0
        if privacy.memory_enabled and actor.role == MemberRole.PARENT:
            for memory in bundle.memories:
                self.store.upsert_memory(
                    household_id=household_id,
                    kind=memory.kind,
                    text=memory.text,
                    subject=memory.subject,
                    confidence=memory.confidence,
                    asserted_by_member_id=actor.id,
                    source_message_id=source_message_id,
                    now_utc=now,
                )
                saved_memory_count += 1

        saved_source_preference_count = 0
        if actor.role == MemberRole.PARENT:
            for preference in bundle.source_preferences:
                self.store.upsert_source_preference(
                    household_id=household_id,
                    phrase=preference.phrase,
                    preference=preference.preference,
                    now_utc=now,
                    created_by_member_id=actor.id,
                )
                saved_source_preference_count += 1

        pending_actions: list[PendingAction] = []
        for proposal in bundle.actions:
            action = self._pending_action_from_agent_proposal(
                household_id=household_id,
                chat_id=chat_id,
                actor_member_id=actor.id,
                proposal=proposal,
                now=now,
            )
            if action is not None:
                pending_actions.append(action)

        reply_text = _guard_agent_reply_text(
            bundle,
            created_action_count=len(pending_actions),
            saved_memory_count=saved_memory_count,
            saved_source_preference_count=saved_source_preference_count,
        )

        outbound: list[OutboundMessage] = []
        if reply_text:
            outbound.append(self._out(household_id, chat_id, reply_text, now))

        for action in pending_actions:
            outbound.append(self._out(household_id, chat_id, tone.pending_action_request(action), now))

        if not outbound:
            outbound.append(self._out(household_id, chat_id, tone.fallback_reply(), now))
        return outbound

    def _pending_action_from_agent_proposal(
        self,
        *,
        household_id: str,
        chat_id: str,
        actor_member_id: str,
        proposal: AgentActionProposal,
        now: datetime,
    ) -> PendingAction | None:
        if proposal.action_type != "create_reminder":
            return None
        title = proposal.payload.get("title")
        due_at_raw = proposal.payload.get("due_at_utc")
        if not isinstance(title, str) or not title.strip():
            return None
        if _contains_contact_detail_or_redaction(title):
            return None
        if not isinstance(due_at_raw, str) or not due_at_raw.strip():
            return None
        try:
            due_at = ensure_utc(datetime.fromisoformat(due_at_raw.replace("Z", "+00:00")))
        except ValueError:
            return None
        if due_at <= now:
            return None
        title = " ".join(title.strip().split())[:160]
        summary = proposal.summary or f"Add reminder: {title}"
        return self.store.create_pending_action(
            household_id=household_id,
            chat_id=chat_id,
            action_type="create_reminder",
            summary=summary[:240],
            payload={
                "title": title,
                "due_at_utc": due_at.isoformat(),
            },
            created_by_member_id=actor_member_id,
            created_at_utc=now,
            expires_at_utc=min(
                due_at,
                now + timedelta(minutes=self.settings.pending_action_ttl_minutes),
            ),
        )

    def _maybe_create_source_reminder_action(
        self,
        *,
        household_id: str,
        chat_id: str,
        title: str,
        due_at_utc: datetime | None,
        reason: str,
        now: datetime,
    ) -> PendingAction | None:
        if due_at_utc is None:
            return None
        if reason not in {"urgent_actionable_source", "upcoming_actionable_source"}:
            return None
        due_at = ensure_utc(due_at_utc)
        if due_at <= now:
            return None
        return self.store.create_pending_action(
            household_id=household_id,
            chat_id=chat_id,
            action_type="create_reminder",
            summary=f"Add reminder: {title}",
            payload={
                "title": title,
                "due_at_utc": due_at.isoformat(),
            },
            created_at_utc=now,
            expires_at_utc=min(
                due_at,
                now + timedelta(minutes=self.settings.pending_action_ttl_minutes),
            ),
        )

    def _maybe_add_calendar_event(
        self,
        household,
        actor: HouseholdMember,
        incoming: IncomingMessage,
        now: datetime,
    ) -> str | None:
        event_text = _calendar_event_text(incoming.text)
        if event_text is None:
            return None
        if actor.role != MemberRole.PARENT:
            return tone.calendar_event_parent_only()
        parsed = parse_due_time(event_text, household.timezone, now_utc=now)
        if parsed.needs_clarification or parsed.due_at_utc is None:
            if parsed.reason == "ambiguous_clock_time":
                return tone.calendar_event_needs_ampm(parsed.matched_text)
            return tone.calendar_event_needs_time()
        title = _calendar_event_title(event_text)
        item = SourceItem(
            id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{household.id}:calendar:{incoming.message_id}")),
            household_id=household.id,
            source_type="calendar",
            title=title,
            body="Added from the household iMessage thread.",
            sender=incoming.sender,
            external_id=f"manual-calendar:{incoming.message_id}",
            observed_at_utc=now,
            event_at_utc=parsed.due_at_utc,
        )
        self.store.add_source_item(
            item,
            decision=SourceDecision.SURFACE.value,
            reason="manual_calendar_event",
            priority=85,
            surfaced_at_utc=now,
        )
        return tone.calendar_event_created(title, parsed.due_at_utc, household.timezone)

    def _maybe_create_reminder(self, household, text: str, now: datetime) -> str | None:
        if not REMINDER_PREFIX.search(text):
            return None
        parsed = parse_due_time(text, household.timezone, now_utc=now)
        if parsed.needs_clarification or parsed.due_at_utc is None:
            if parsed.reason == "ambiguous_clock_time":
                return tone.reminder_needs_ampm(parsed.matched_text)
            return tone.reminder_needs_time()
        title, assignee_member_id = _reminder_title_and_assignee(
            _reminder_title(text),
            self.store.list_members(household.id),
        )
        reminder = self.store.create_reminder(
            household_id=household.id,
            chat_id=household.chat_id,
            title=title,
            due_at_utc=parsed.due_at_utc,
            created_at_utc=now,
            assignee_member_id=assignee_member_id,
        )
        [reminder] = self._with_reminder_assignee_labels([reminder])
        return tone.reminder_created(
            reminder.title,
            reminder.due_at_utc,
            household.timezone,
            assignee_label=reminder.assignee_label,
        )

    def _maybe_resolve_reminder(
        self,
        household_id: str,
        actor: HouseholdMember,
        text: str,
        now: datetime,
    ) -> str | None:
        parsed = _reminder_resolution(text)
        if parsed is None:
            return None
        status, query = parsed
        if actor.role != MemberRole.PARENT:
            return tone.reminder_resolution_parent_only()
        if not query:
            if status == ReminderStatus.COMPLETED:
                matches = self.store.recent_sent_reminders(
                    household_id=household_id,
                    now_utc=now,
                    since_utc=now - timedelta(minutes=self.settings.reminder_delivery_grace_minutes),
                    limit=4,
                )
                if len(matches) == 1:
                    reminder = self.store.mark_sent_reminder_completed(
                        household_id=household_id,
                        reminder_id=matches[0].id,
                    )
                    if reminder is not None:
                        return tone.reminder_completed(reminder.title)
                if len(matches) > 1:
                    return tone.reminder_resolution_ambiguous(matches)
            return tone.reminder_resolution_needs_text()
        matches = self.store.find_pending_reminders_by_query(
            household_id=household_id,
            query=query,
            now_utc=now,
            limit=4,
        )
        if not matches:
            return tone.reminder_resolution_not_found(query)
        if len(matches) > 1:
            return tone.reminder_resolution_ambiguous(matches)
        reminder = self.store.update_reminder_status(
            household_id=household_id,
            reminder_id=matches[0].id,
            status=status,
        )
        if reminder is None:
            return tone.reminder_resolution_not_found(query)
        if status == ReminderStatus.COMPLETED:
            return tone.reminder_completed(reminder.title)
        return tone.reminder_canceled(reminder.title)

    def _maybe_handle_partner_invite(
        self,
        household,
        actor: HouseholdMember,
        text: str,
        now: datetime,
    ) -> list[OutboundMessage] | None:
        invite_match = PARTNER_INVITE.match(text.strip())
        confirm_match = PARTNER_CONFIRM.match(text.strip())
        if not invite_match and not confirm_match:
            return None
        match = invite_match or confirm_match
        raw_target = next((group for group in match.groups() if group is not None), "")
        phone = _phone_from_text(raw_target)
        if phone is None:
            return [self._out(household.id, household.chat_id, tone.partner_invite_needs_phone(), now)]
        return self._partner_invite_messages(
            household,
            actor,
            phone=phone,
            confirm=confirm_match is not None,
            now=now,
        )

    def _maybe_handle_prompted_partner_phone(
        self,
        household,
        actor: HouseholdMember,
        text: str,
        now: datetime,
    ) -> list[OutboundMessage] | None:
        if not self._recently_asked_for_partner_phone(household.id):
            return None
        phone = _phone_from_text(text)
        if phone is None:
            return None
        return self._partner_invite_messages(
            household,
            actor,
            phone=phone,
            confirm=False,
            now=now,
        )

    def _partner_invite_messages(
        self,
        household,
        actor: HouseholdMember,
        *,
        phone: str,
        confirm: bool,
        now: datetime,
    ) -> list[OutboundMessage]:
        if actor.role != MemberRole.PARENT:
            return [self._out(household.id, household.chat_id, tone.partner_invite_parent_only(), now)]
        members = self.store.list_members(household.id)
        parent_count = sum(1 for member in members if member.role == MemberRole.PARENT)
        if parent_count >= 2:
            return [self._out(household.id, household.chat_id, tone.partner_invite_already_ready(), now)]
        if phone == actor.phone:
            return [self._out(household.id, household.chat_id, tone.partner_invite_self(), now)]
        if confirm:
            member = self.store.ensure_parent_member(
                household_id=household.id,
                phone=phone,
                now_utc=now,
            )
            if member is None:
                return [self._out(household.id, household.chat_id, tone.partner_invite_already_ready(), now)]
            return [self._out(household.id, household.chat_id, tone.partner_confirmed(phone), now)]
        if not self.settings.linq_api_key or not self.settings.linq_from_phone:
            return [self._out(household.id, household.chat_id, tone.partner_invite_not_configured(), now)]
        intro = tone.partner_group_intro()
        group_message = OutboundMessage(
            chat_id=household.chat_id,
            text=intro,
            idempotency_key=f"group:{uuid.uuid4()}",
            new_chat_from=self.settings.linq_from_phone,
            new_chat_to=(actor.phone, phone),
            migrate_household_id=household.id,
            invited_partner_phone=phone,
        )
        return [
            self._out(household.id, household.chat_id, tone.partner_invite_started(phone), now),
            group_message,
        ]

    def complete_partner_group_created(
        self,
        *,
        household_id: str,
        new_chat_id: str,
        partner_phone: str,
        intro_text: str,
        now_utc: datetime | None = None,
    ) -> None:
        now = ensure_utc(now_utc or utc_now())
        self.store.ensure_parent_member(
            household_id=household_id,
            phone=partner_phone,
            now_utc=now,
        )
        self.store.migrate_household_chat(
            household_id=household_id,
            new_chat_id=new_chat_id,
            now_utc=now,
        )
        self.store.save_message(
            household_id=household_id,
            chat_id=new_chat_id,
            direction=MessageDirection.OUTBOUND,
            body=intro_text,
            created_at_utc=now,
            message_id=f"group-intro:{uuid.uuid4()}",
        )

    def _maybe_handle_household_setup(
        self,
        household_id: str,
        actor: HouseholdMember,
        text: str,
        now: datetime,
    ) -> str | None:
        if _setup_command(text):
            reply = tone.readiness_status(self._readiness(household_id, now))
            url = self._onboarding_url_for_actor(household_id, actor, now)
            if url:
                return f"{reply}\n\nSetup link: {url}"
            return reply

        name_match = re.match(r"(?:my name is|i am|i'm)\s+(.+)", text.strip(), flags=re.IGNORECASE)
        if name_match:
            name = " ".join(name_match.group(1).strip(" .").split())
            if not name:
                return tone.name_needed()
            if len(name) > 60:
                return tone.name_too_long()
            self.store.set_member_name(actor.id, name, now_utc=now)
            return tone.member_named(
                name,
                next_prompt=self._onboarding_next_prompt(household_id, actor, now),
            )

        child_names = _child_names_from_text(text)
        if not child_names and self._recently_asked_for_children(household_id):
            child_names = _bare_child_names_from_text(text)
        if child_names:
            if actor.role != MemberRole.PARENT:
                return tone.household_setup_parent_only()
            for child_name in child_names:
                self.store.upsert_memory(
                    household_id=household_id,
                    kind=MemoryKind.FACT,
                    subject=child_name,
                    text=f"Child profile: {child_name}.",
                    confidence=0.95,
                    asserted_by_member_id=actor.id,
                    now_utc=now,
                )
            return tone.children_recorded(child_names)

        tz_match = re.match(
            r"(?:set timezone|use timezone|timezone is)\s+([A-Za-z0-9_./+-]+)",
            text.strip(),
            flags=re.IGNORECASE,
        )
        if tz_match:
            if actor.role != MemberRole.PARENT:
                return tone.household_setup_parent_only()
            timezone_name = tz_match.group(1)
            try:
                resolve_timezone(timezone_name)
            except ValueError:
                return tone.timezone_unknown()
            self.store.update_household_timezone(household_id, timezone_name)
            return tone.timezone_updated(timezone_name)

        if text.strip().lower() in {"household setup", "household status", "who is in this household", "who's in this household"}:
            household = self._household_by_id(household_id)
            members = self.store.list_members(household_id)
            labels_by_id = _household_member_labels(members)
            people = [
                (labels_by_id[member.id], member.role.value)
                for member in members
            ]
            parent_count = sum(1 for member in members if member.role.value == "parent")
            readiness = self._readiness(household_id, now)
            return tone.household_status(
                timezone_name=household.timezone,
                people=people,
                parent_count=parent_count,
                ready=readiness.ready,
                next_action=tone.setup_next_action(readiness),
            )
        return None

    def _recently_asked_actor_for_name(self, household_id: str) -> bool:
        history = self.store.recent_messages(household_id, limit=4)
        return any(
            message["role"] == "assistant" and "What should I call you?" in message["content"]
            for message in history
        )

    def _maybe_handle_bare_name_reply(
        self,
        household_id: str,
        actor: HouseholdMember,
        text: str,
        now: datetime,
    ) -> str | None:
        bare_name = _bare_name_reply(text)
        if (
            actor.display_name is None
            and bare_name is not None
            and self._recently_asked_actor_for_name(household_id)
        ):
            self.store.set_member_name(actor.id, bare_name, now_utc=now)
            return tone.member_named(
                bare_name,
                next_prompt=self._onboarding_next_prompt(household_id, actor, now),
            )
        return None

    def _recently_asked_for_partner_phone(self, household_id: str) -> bool:
        history = self.store.recent_messages(household_id, limit=4)
        return any(
            message["role"] == "assistant"
            and "Send your partner's phone number" in message["content"]
            for message in history
        )

    def _recently_asked_for_children(self, household_id: str) -> bool:
        history = self.store.recent_messages(household_id, limit=4)
        return any(
            message["role"] == "assistant"
            and (
                "who are the kids i should know" in message["content"].lower()
                or "child or children's names" in message["content"].lower()
            )
            for message in history
        )

    def _recently_asked_for_source_preference(self, household_id: str) -> bool:
        history = self.store.recent_messages(household_id, limit=4)
        return any(
            message["role"] == "assistant"
            and (
                "always worth a text" in message["content"].lower()
                or "what deserves a text" in message["content"].lower()
            )
            for message in history
        )

    def _maybe_resume_onboarding_naturally(
        self,
        household_id: str,
        actor: HouseholdMember,
        text: str,
        lower: str,
        now: datetime,
    ) -> str | None:
        if actor.role != MemberRole.PARENT or not _natural_onboarding_continuation(text, lower):
            return None
        readiness = self._readiness(household_id, now)
        if readiness.ready:
            return None
        role = self._onboarding_role_for_actor(household_id, actor)
        setup_url = self._onboarding_url_for_actor(household_id, actor, now)
        if setup_url is None:
            return None
        return tone.onboarding_greeting_resume(
            name=actor.display_name,
            setup_url=setup_url,
            role=role,
        )

    def _onboarding_next_prompt(
        self,
        household_id: str,
        actor: HouseholdMember,
        now: datetime,
    ) -> str | None:
        readiness = self._readiness(household_id, now)
        if not readiness.ready and actor.role == MemberRole.PARENT:
            role = self._onboarding_role_for_actor(household_id, actor)
            url = self._onboarding_url_for_actor(household_id, actor, now)
            if url:
                if role == "partner":
                    return (
                        "Next, use this setup link to connect your Google account, "
                        f"confirm the household details, and set your tone preference: {url}"
                    )
                return (
                    "Next, use this setup link for partner, kids, location, caretakers, tone, "
                    f"and Google: {url} If you want to stay in text, send your partner's phone number."
                )
        return tone.onboarding_next_prompt(readiness)

    def _onboarding_url_for_actor(
        self,
        household_id: str,
        actor: HouseholdMember,
        now: datetime,
    ) -> str | None:
        if actor.role != MemberRole.PARENT:
            return None
        household = self._household_by_id(household_id)
        return self.onboarding_url(
            chat_id=household.chat_id,
            member_id=actor.id,
            role=self._onboarding_role_for_actor(household_id, actor),
            now_utc=now,
        )

    def _onboarding_role_for_actor(self, household_id: str, actor: HouseholdMember) -> str:
        parents = [
            member
            for member in self.store.list_members(household_id)
            if member.role == MemberRole.PARENT
        ]
        if parents and parents[0].id == actor.id:
            return "primary"
        return "partner"

    def _readiness(self, household_id: str, now: datetime) -> HouseholdReadiness:
        household = self._household_by_id(household_id)
        members = self.store.list_members(household_id)
        parents = [member for member in members if member.role == MemberRole.PARENT]
        memories = self.store.memory_snapshot(household_id=household_id, now_utc=now, limit=100)
        children = _child_names_from_memories(memories.memories)
        connected_accounts = self.store.list_connected_accounts(household_id)
        source_preferences = self.store.list_source_preferences(household_id)

        missing: list[str] = []
        named_parent_count = sum(1 for parent in parents if parent.display_name)
        if named_parent_count == 0:
            missing.append("Tell me what each parent wants to be called.")
        elif named_parent_count < 2:
            missing.append("Ask the second parent to tell me their name from their phone.")
        if len(parents) < 2:
            missing.append("Invite or confirm your partner as the second parent.")
        if not children:
            missing.append("Tell me your child or children's names.")
        if not connected_accounts:
            missing.append("Connect or import at least one calendar or email source.")
        if not source_preferences:
            missing.append("Tell me one thing that is always worth a text.")

        return HouseholdReadiness(
            household_id=household_id,
            timezone=household.timezone,
            parent_count=len(parents),
            named_parent_count=named_parent_count,
            child_count=len(children),
            connected_account_count=len(connected_accounts),
            source_preference_count=len(source_preferences),
            memory_count=len(memories.memories),
            ready=not missing,
            missing=missing,
        )

    def _maybe_handle_privacy(
        self,
        household_id: str,
        actor: HouseholdMember,
        text: str,
        now: datetime,
    ) -> str | None:
        lower = " ".join(text.strip().lower().split())
        if lower in {"privacy status", "privacy settings", "memory controls"}:
            return tone.privacy_status(self.store.household_privacy(household_id))

        if lower in {"pause memory", "turn off memory", "disable memory"}:
            if actor.role != MemberRole.PARENT:
                return tone.privacy_parent_only()
            self.store.update_memory_enabled(
                household_id=household_id,
                enabled=False,
                now_utc=now,
            )
            return tone.memory_paused()

        if lower in {"resume memory", "turn on memory", "enable memory"}:
            if actor.role != MemberRole.PARENT:
                return tone.privacy_parent_only()
            self.store.update_memory_enabled(
                household_id=household_id,
                enabled=True,
                now_utc=now,
            )
            return tone.memory_resumed()

        if lower in {"opt in to product analytics", "turn on product analytics"}:
            if actor.role != MemberRole.PARENT:
                return tone.privacy_parent_only()
            self.store.update_product_analytics_opt_in(
                household_id=household_id,
                opted_in=True,
                now_utc=now,
            )
            return tone.analytics_opted_in()

        if lower in {"opt out of product analytics", "turn off product analytics"}:
            if actor.role != MemberRole.PARENT:
                return tone.privacy_parent_only()
            self.store.update_product_analytics_opt_in(
                household_id=household_id,
                opted_in=False,
                now_utc=now,
            )
            return tone.analytics_opted_out()

        return None

    def _maybe_handle_source_connection(
        self,
        household_id: str,
        chat_id: str,
        actor: HouseholdMember,
        text: str,
        now: datetime,
    ) -> str | None:
        if not _google_connection_request(text):
            return None
        if actor.role != MemberRole.PARENT:
            return tone.google_connection_parent_only()
        household = self._household_by_id(household_id)
        try:
            start = self.start_google_oauth(chat_id=chat_id, now_utc=now)
        except OAuthConfigurationError:
            return tone.google_connection_not_configured()
        return tone.google_connection_link(
            start.authorization_url,
            start.expires_at_utc,
            household.timezone,
        )

    def _maybe_disconnect_source_account(
        self,
        household_id: str,
        actor: HouseholdMember,
        text: str,
        now: datetime,
    ) -> str | None:
        lower = " ".join(text.strip().lower().split())
        if lower not in GOOGLE_DISCONNECT_COMMANDS:
            return None
        if actor.role != MemberRole.PARENT:
            return tone.google_disconnect_parent_only()
        disconnected = self.store.disconnect_connected_accounts(
            household_id=household_id,
            provider="google",
            now_utc=now,
        )
        if disconnected == 0:
            return tone.google_disconnect_empty()
        return tone.google_disconnected(disconnected)

    def _maybe_update_memory(
        self,
        household_id: str,
        actor: HouseholdMember,
        incoming: IncomingMessage,
        now: datetime,
        *,
        acknowledge_success: bool = True,
    ) -> str | None:
        text = incoming.text.strip()
        lower = text.lower()
        if " ".join(lower.split()) in MEMORY_CLEAR_COMMANDS:
            if actor.role != MemberRole.PARENT:
                return tone.memory_parent_only()
            count = self.store.clear_memories(household_id=household_id, now_utc=now)
            if count == 0:
                return tone.memory_clear_empty()
            return tone.memory_cleared(count)
        remember_prefixes = (
            "remember that ",
            "please remember that ",
            "remember ",
            "please remember ",
        )
        for prefix in remember_prefixes:
            if lower.startswith(prefix):
                if actor.role != MemberRole.PARENT:
                    return tone.memory_parent_only()
                fact = text[len(prefix) :].strip(" .")
                if not fact:
                    return tone.memory_needs_text()
                if len(fact) > MAX_EXPLICIT_MEMORY_CHARS:
                    return tone.memory_too_long(MAX_EXPLICIT_MEMORY_CHARS)
                if not self.store.household_privacy(household_id).memory_enabled:
                    return tone.memory_disabled()
                kind = _memory_kind(fact)
                self.store.upsert_memory(
                    household_id=household_id,
                    kind=kind,
                    text=fact,
                    now_utc=now,
                    asserted_by_member_id=actor.id,
                    source_message_id=incoming.message_id,
                    confidence=0.9,
                )
                if not acknowledge_success:
                    return ""
                return tone.memory_saved(fact)

        forget_match = re.match(r"forget (?:that )?(.*)", text, flags=re.IGNORECASE)
        if forget_match:
            if actor.role != MemberRole.PARENT:
                return tone.memory_parent_only()
            query = forget_match.group(1).strip(" .")
            if not query:
                return tone.forget_needs_text()
            count = self.store.forget_memories(
                household_id=household_id,
                query=query,
                now_utc=now,
            )
            if count:
                return tone.memory_removed()
            return tone.memory_not_found()
        return None

    def _maybe_show_memory(
        self,
        household_id: str,
        actor: HouseholdMember,
        lower: str,
        now: datetime,
    ) -> str | None:
        if lower.strip(" ?") not in {
            "what do you remember",
            "what is in the household book",
            "what's in the household book",
            "household book",
            "household book status",
            "book status",
            "memory status",
            "list memory",
            "list memories",
            "list household book",
            "show memory",
            "show memories",
            "show household book",
        }:
            return None
        if actor.role != MemberRole.PARENT:
            return tone.memory_view_parent_only()
        snapshot = self.store.memory_snapshot(household_id=household_id, now_utc=now, limit=10)
        return tone.memory_snapshot(snapshot)

    def _maybe_update_source_preference(
        self,
        household_id: str,
        actor: HouseholdMember,
        text: str,
        now: datetime,
        *,
        acknowledge_success: bool = True,
    ) -> str | None:
        always_match = SOURCE_ALWAYS.match(text.strip())
        if always_match:
            if actor.role != MemberRole.PARENT:
                return tone.source_preference_parent_only()
            phrase, error = self._source_preference_phrase(household_id, always_match.group(1))
            if error is not None:
                return error
            if not phrase:
                return tone.source_preference_needs_phrase(SourcePreferenceKind.ALWAYS_SURFACE)
            preference = self.store.upsert_source_preference(
                household_id=household_id,
                phrase=phrase,
                preference=SourcePreferenceKind.ALWAYS_SURFACE,
                now_utc=now,
                created_by_member_id=actor.id,
            )
            if not acknowledge_success:
                return ""
            return tone.source_preference_saved(preference)

        mute_match = SOURCE_MUTE.match(text.strip())
        if mute_match:
            if actor.role != MemberRole.PARENT:
                return tone.source_preference_parent_only()
            phrase, error = self._source_preference_phrase(household_id, mute_match.group(1))
            if error is not None:
                return error
            if not phrase:
                return tone.source_preference_needs_phrase(SourcePreferenceKind.MUTE)
            preference = self.store.upsert_source_preference(
                household_id=household_id,
                phrase=phrase,
                preference=SourcePreferenceKind.MUTE,
                now_utc=now,
                created_by_member_id=actor.id,
            )
            if not acknowledge_success:
                return ""
            return tone.source_preference_saved(preference)

        prompted_phrase = _bare_source_preference_phrase(text)
        if prompted_phrase and self._recently_asked_for_source_preference(household_id):
            if actor.role != MemberRole.PARENT:
                return tone.source_preference_parent_only()
            phrase, error = self._source_preference_phrase(household_id, prompted_phrase)
            if error is not None:
                return error
            if not phrase:
                return tone.source_preference_needs_phrase(SourcePreferenceKind.ALWAYS_SURFACE)
            preference = self.store.upsert_source_preference(
                household_id=household_id,
                phrase=phrase,
                preference=SourcePreferenceKind.ALWAYS_SURFACE,
                now_utc=now,
                created_by_member_id=actor.id,
            )
            if not acknowledge_success:
                return ""
            return tone.source_preference_saved(preference)
        return None

    def _source_preference_phrase(self, household_id: str, raw_phrase: str) -> tuple[str, str | None]:
        reference_kind = _source_reference_kind(raw_phrase)
        if reference_kind is None:
            return _preference_phrase(raw_phrase), None

        item = self.store.last_surfaced_source_item(household_id)
        if item is None:
            return "", tone.source_feedback_without_recent_item()

        phrase = _source_reference_phrase(item, reference_kind)
        if not phrase:
            return "", tone.source_reference_missing(reference_kind)
        return phrase, None

    def _maybe_handle_source_feedback(
        self,
        household_id: str,
        actor: HouseholdMember,
        text: str,
        now: datetime,
    ) -> str | None:
        feedback = _source_feedback_kind(text)
        if feedback is None:
            return None
        if actor.role != MemberRole.PARENT:
            return tone.source_preference_parent_only()
        explicit_phrases = _explicit_source_feedback_phrases(text, feedback)
        if explicit_phrases:
            preference_kind = (
                SourcePreferenceKind.MUTE
                if feedback == SourceFeedbackKind.NOT_USEFUL
                else SourcePreferenceKind.ALWAYS_SURFACE
            )
            preferences = [
                self.store.upsert_source_preference(
                    household_id=household_id,
                    phrase=phrase,
                    preference=preference_kind,
                    now_utc=now,
                    created_by_member_id=actor.id,
                )
                for phrase in explicit_phrases
            ]
            return tone.source_feedback_saved_many(feedback, preferences)
        item = self.store.last_surfaced_source_item(household_id)
        if item is None:
            return tone.source_feedback_without_recent_item()
        phrase = _feedback_phrase(item, feedback)
        preference_kind = (
            SourcePreferenceKind.MUTE
            if feedback == SourceFeedbackKind.NOT_USEFUL
            else SourcePreferenceKind.ALWAYS_SURFACE
        )
        preference = self.store.upsert_source_preference(
            household_id=household_id,
            phrase=phrase,
            preference=preference_kind,
            now_utc=now,
            created_by_member_id=actor.id,
        )
        self.store.record_source_feedback(
            household_id=household_id,
            source_item_id=item.id,
            feedback=feedback,
            phrase=phrase,
            created_by_member_id=actor.id,
            created_at_utc=now,
        )
        return tone.source_feedback_saved(feedback, preference)

    def _maybe_search_connected_email(
        self,
        household: Household,
        actor: HouseholdMember,
        incoming: IncomingMessage,
        now: datetime,
    ) -> list[OutboundMessage] | None:
        if not _email_search_requested(incoming.text):
            return None
        if actor.role != MemberRole.PARENT:
            return [self._out(household.id, incoming.chat_id, tone.email_search_parent_only(), now)]
        accounts = [
            account
            for account in self.store.list_connected_accounts(household.id)
            if account.provider == "google"
            and account.status == ConnectedAccountStatus.ACTIVE
            and self.store.get_connected_account_token(account.id) is not None
        ]
        if not accounts:
            return [self._out(household.id, incoming.chat_id, tone.email_search_no_connected_email(), now)]
        query_source = self._email_search_context(household.id, incoming)
        queries = _gmail_search_queries(query_source)
        if not queries:
            return [self._out(household.id, incoming.chat_id, tone.email_search_no_results("that"), now)]
        provider = GoogleSourceProvider(settings=self.settings, store=self.store)
        results: list[dict[str, object]] = []
        seen: set[tuple[str, str]] = set()
        failed = 0
        for account in accounts:
            for query in queries:
                if len(results) >= EMAIL_SEARCH_MAX_RESULTS:
                    break
                try:
                    matches = provider.search_gmail(
                        account,
                        query=query,
                        now_utc=now,
                        max_results=EMAIL_SEARCH_MAX_RESULTS - len(results),
                    )
                except Exception:
                    failed += 1
                    continue
                for match in matches:
                    key = (
                        str(match.get("sender") or "").lower(),
                        str(match.get("subject") or "").lower(),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    results.append(match)
                    if len(results) >= EMAIL_SEARCH_MAX_RESULTS:
                        break
            if len(results) >= EMAIL_SEARCH_MAX_RESULTS:
                break
        if not results:
            search_note = (
                f"Florence searched connected Gmail for '{queries[0]}' but the provider failed."
                if failed
                else f"Florence searched connected Gmail for '{queries[0]}' and found no clear matches."
            )
            return self._agent_turn(
                household=household,
                actor=actor,
                incoming=replace(incoming, text=f"{incoming.text}\n\n{search_note}\n{EMAIL_SEARCH_AGENT_FOCUS}"),
                now=now,
            )
        items = self._store_email_search_results(
            household=household,
            results=results,
            now=now,
        )
        search_context = _email_search_context_for_agent(
            query=queries[0],
            items=items,
            timezone_name=household.timezone,
        )
        return self._agent_turn(
            household=household,
            actor=actor,
            incoming=replace(incoming, text=f"{incoming.text}\n\n{search_context}"),
            now=now,
        )

    def _email_search_context(self, household_id: str, incoming: IncomingMessage) -> str:
        if len(_email_search_keywords(incoming.text)) >= 2:
            return incoming.text
        history = self.store.recent_messages(
            household_id,
            limit=6,
            exclude_message_id=incoming.message_id,
        )
        for message in reversed(history):
            content = message.get("content") or ""
            if message.get("role") == "user" and not _email_search_requested(content):
                return content
        return incoming.text

    def _store_email_search_results(
        self,
        *,
        household: Household,
        results: list[dict[str, object]],
        now: datetime,
    ) -> list[SourceItem]:
        items: list[SourceItem] = []
        for result in results:
            received_at = _coerce_dt(result.get("received_at_utc"), now)
            item = email_to_source_item(
                EmailCandidate(
                    household_id=household.id,
                    subject=str(result.get("subject") or ""),
                    body=str(result.get("body") or ""),
                    sender=str(result.get("sender") or ""),
                    received_at_utc=received_at,
                    external_id=_optional_str(result.get("external_id")),
                    connected_account_id=_optional_str(result.get("connected_account_id")),
                )
            )
            self.store.add_source_item(
                item,
                decision=SourceDecision.STORE_ONLY.value,
                reason="parent_requested_email_search",
                priority=30,
                surfaced_at_utc=None,
            )
            items.append(item)
        return items

    def _maybe_show_source_preferences(
        self,
        household_id: str,
        actor: HouseholdMember,
        lower: str,
    ) -> str | None:
        if lower.strip(" ?") not in {
            "source preferences",
            "source rules",
            "email preferences",
            "email rules",
        }:
            return None
        if actor.role != MemberRole.PARENT:
            return tone.source_preference_parent_only()
        preferences = self.store.list_source_preferences(household_id)
        return tone.source_preferences(preferences)

    def _maybe_show_source_review(
        self,
        household,
        actor: HouseholdMember,
        lower: str,
    ) -> str | None:
        if lower.strip(" ?") not in {
            "source review",
            "source summary",
            "email review",
            "email summary",
            "what did you keep quiet",
            "what have you kept quiet",
        }:
            return None
        if actor.role != MemberRole.PARENT:
            return tone.source_review_parent_only()
        snapshot = self.store.source_review_snapshot(household_id=household.id, limit=3)
        return tone.source_review(snapshot, household.timezone)

    def _maybe_handle_pending_action(
        self,
        household_id: str,
        actor: HouseholdMember,
        text: str,
        now: datetime,
    ) -> str | None:
        match = APPROVAL_REPLY.match(text.strip())
        if match is None:
            return None
        if actor.role != MemberRole.PARENT:
            return tone.approval_parent_only()
        action = self.store.find_pending_action_by_code(
            household_id=household_id,
            code=match.group(2),
            now_utc=now,
        )
        if action is None:
            return tone.approval_not_found()
        status = (
            PendingActionStatus.APPROVED
            if match.group(1).lower() == "approve"
            else PendingActionStatus.CANCELED
        )
        resolved = self.store.resolve_pending_action(
            household_id=household_id,
            action_id=action.id,
            status=status,
            resolved_by_member_id=actor.id,
            now_utc=now,
        )
        if resolved is None:
            return tone.approval_not_found()
        if status == PendingActionStatus.APPROVED:
            return tone.approval_approved(resolved)
        return tone.approval_canceled(resolved)

    def _maybe_show_handoff(
        self,
        household,
        actor: HouseholdMember,
        lower: str,
        now: datetime,
    ) -> str | None:
        if lower.strip(" ?") not in {
            "handoff",
            "household handoff",
            "what is open",
            "what's open",
            "open items",
            "pending items",
            "what needs approval",
            "pending approvals",
        }:
            return None
        if actor.role != MemberRole.PARENT:
            return tone.handoff_parent_only()
        horizon = now + timedelta(days=7)
        approvals = self.store.list_pending_actions(
            household_id=household.id,
            now_utc=now,
            limit=5,
        )
        reminders = self.store.upcoming_reminders(
            household_id=household.id,
            now_utc=now,
            horizon_utc=horizon,
            limit=6,
        )
        reminders = self._with_reminder_assignee_labels(reminders)
        return tone.household_handoff(
            approvals=approvals,
            reminders=reminders,
            timezone_name=household.timezone,
        )

    def _agenda(self, household_id: str, now: datetime) -> str:
        household = self._household_by_id(household_id)
        local_now = now.astimezone(resolve_timezone(household.timezone))
        local_tomorrow = (local_now + timedelta(days=1)).date()
        horizon = datetime.combine(
            local_tomorrow,
            datetime.min.time(),
            tzinfo=local_now.tzinfo,
        ).astimezone(timezone.utc)
        reminders = self.store.upcoming_reminders(
            household_id=household_id,
            now_utc=now,
            horizon_utc=horizon,
            limit=5,
        )
        reminders = self._with_reminder_assignee_labels(reminders)
        sources = self.store.agenda_source_items(
            household_id=household_id,
            now_utc=now,
            horizon_utc=horizon,
            limit=3,
        )
        return tone.agenda_today(
            reminders=reminders,
            sources=sources,
            timezone_name=household.timezone,
        )

    def _tomorrow_prep(self, household_id: str, now: datetime) -> str:
        household = self._household_by_id(household_id)
        local_now = now.astimezone(resolve_timezone(household.timezone))
        local_tomorrow = (local_now + timedelta(days=1)).date()
        start = datetime.combine(
            local_tomorrow,
            datetime.min.time(),
            tzinfo=local_now.tzinfo,
        ).astimezone(timezone.utc)
        horizon = start + timedelta(days=1)
        reminders = self.store.upcoming_reminders(
            household_id=household_id,
            now_utc=start - timedelta(microseconds=1),
            horizon_utc=horizon,
            limit=6,
        )
        reminders = self._with_reminder_assignee_labels(reminders)
        sources = [
            source
            for source in self.store.agenda_source_items(
                household_id=household_id,
                now_utc=start,
                horizon_utc=horizon,
                limit=6,
            )
            if source.event_at_utc is not None
        ][:4]
        return tone.tomorrow_prep(
            reminders=reminders,
            sources=sources,
            timezone_name=household.timezone,
        )

    def _household_by_id(self, household_id: str):
        # The store only needs this internally for agenda formatting, so keep it simple.
        with self.store.connect() as conn:
            row = conn.execute("SELECT chat_id FROM households WHERE id = ?", (household_id,)).fetchone()
        if row is None:
            raise ValueError("unknown household")
        household = self.store.get_household_by_chat(row["chat_id"])
        if household is None:
            raise ValueError("unknown household")
        return household

    def _out(
        self,
        household_id: str,
        chat_id: str,
        text: str,
        now: datetime,
        *,
        routine_household_id: str | None = None,
        routine_name: str | None = None,
        routine_local_date: str | None = None,
        briefed_source_item_ids: tuple[str, ...] = (),
        idempotency_key: str | None = None,
        save_message: bool = True,
        delivery_household_id: str | None = None,
        delivery_source_message_id: str | None = None,
    ) -> OutboundMessage:
        key = idempotency_key or f"out:{uuid.uuid4()}"
        if save_message:
            self.store.save_message(
                household_id=household_id,
                chat_id=chat_id,
                direction=MessageDirection.OUTBOUND,
                body=text,
                created_at_utc=now,
                message_id=key,
            )
        return OutboundMessage(
            chat_id=chat_id,
            text=text,
            idempotency_key=key,
            routine_household_id=routine_household_id,
            routine_name=routine_name,
            routine_local_date=routine_local_date,
            briefed_source_item_ids=briefed_source_item_ids,
            delivery_household_id=delivery_household_id,
            delivery_source_message_id=delivery_source_message_id,
        )

    @staticmethod
    def _is_agenda_request(lower: str) -> bool:
        return any(
            phrase in lower
            for phrase in (
                "what's on deck",
                "what is on deck",
                "what's today",
                "what is today",
                "today?",
                "briefing",
            )
        )

    @staticmethod
    def _is_tomorrow_prep_request(lower: str) -> bool:
        return any(
            phrase in lower
            for phrase in (
                "prep for tomorrow",
                "prepare for tomorrow",
                "tomorrow prep",
                "tomorrow's prep",
                "what should we prep tomorrow",
                "what should we prepare tomorrow",
                "what should we prep for tomorrow",
                "what should we prepare for tomorrow",
            )
        )


def _guard_agent_reply_text(
    bundle: AgentProposalBundle,
    *,
    created_action_count: int,
    saved_memory_count: int,
    saved_source_preference_count: int,
) -> str:
    reply_text = bundle.reply_text
    if not reply_text:
        return ""

    if created_action_count and ACTION_DONE_CLAIM.search(reply_text):
        return tone.agent_reply_needs_approval_guard()

    dropped_proposal = (
        len(bundle.actions) > created_action_count
        or len(bundle.memories) > saved_memory_count
        or len(bundle.source_preferences) > saved_source_preference_count
        or bundle.rejected_proposal_count > 0
    )
    if dropped_proposal and _agent_reply_claims_state_change(reply_text):
        return tone.agent_reply_no_state_change_guard()

    return reply_text


def _agent_reply_claims_state_change(reply_text: str) -> bool:
    return any(
        pattern.search(reply_text)
        for pattern in (
            ACTION_DONE_CLAIM,
            MEMORY_DONE_CLAIM,
            SOURCE_RULE_DONE_CLAIM,
        )
    )


def _contains_contact_detail_or_redaction(text: str) -> bool:
    lowered = text.lower()
    return (
        "[phone number]" in lowered
        or PHONE_LIKE.search(text) is not None
        or EMAIL_LIKE.search(text) is not None
    )


def _household_member_labels(members: list[HouseholdMember]) -> dict[str, str]:
    labels: dict[str, str] = {}
    unnamed_role_counts: dict[str, int] = {}
    for member in members:
        if member.display_name:
            labels[member.id] = member.display_name
            continue
        unnamed_role_counts[member.role.value] = unnamed_role_counts.get(member.role.value, 0) + 1
        count = unnamed_role_counts[member.role.value]
        base = f"unnamed {member.role.value}"
        labels[member.id] = base if count == 1 else f"{base} {count}"
    return labels


def _reminder_title(text: str) -> str:
    cleaned = REMINDER_PREFIX.sub("", text, count=1).strip(" .:-")
    cleaned = _strip_time_expressions(cleaned)
    cleaned = " ".join(cleaned.split()).strip(" .:-")
    cleaned = re.sub(r"^to\s+", "", cleaned, flags=re.IGNORECASE)
    return cleaned or text.strip()


def _calendar_event_text(text: str) -> str | None:
    stripped = " ".join(text.strip().split())
    if not stripped:
        return None
    for pattern in (CALENDAR_EVENT_COLON, CALENDAR_EVENT_ADD_TO):
        match = pattern.match(stripped)
        if match is not None:
            return _clean_calendar_event_text(match.group(1))
    for pattern in (CALENDAR_EVENT_WITH_OBJECT, CALENDAR_EVENT_AS_EVENT):
        match = pattern.match(stripped)
        if match is None:
            continue
        event_text = _clean_calendar_event_text(f"{match.group(1)} {match.group(2)}")
        if event_text:
            return event_text
    return None


def _calendar_event_title(event_text: str) -> str:
    cleaned = _strip_time_expressions(event_text)
    cleaned = re.sub(r"\b(?:calendar|schedule|event)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = " ".join(cleaned.split()).strip(" .:-")
    return (cleaned or event_text.strip())[:160]


def _clean_calendar_event_text(text: str) -> str | None:
    cleaned = " ".join(text.strip(" .:-").split())
    return cleaned if cleaned else None


def _strip_time_expressions(text: str) -> str:
    return re.sub(
        r"\b("
        r"today|tomorrow|tonight|day after tomorrow|"
        r"this morning|this afternoon|this evening|tomorrow morning|tomorrow afternoon|tomorrow evening|"
        r"morning|afternoon|evening|night|after school|noon|midnight|"
        r"next \w+|in \d+ \w+|"
        r"(?:at|by|around)?\s*\d{1,2}(:\d{2})?\s*(am|pm)|"
        r"(?:at|by|around)?\s*\d{1,2}:\d{2}|"
        r"(?:at|by|around)\s+(1[3-9]|2[0-3])"
        r")\b",
        "",
        text,
        flags=re.IGNORECASE,
    )


def _reminder_title_and_assignee(
    title: str,
    members: list[HouseholdMember],
) -> tuple[str, str | None]:
    for member in members:
        if not member.display_name:
            continue
        name = re.escape(member.display_name.strip())
        patterns = (
            rf"^{name}\s+to\s+(.+)$",
            rf"^for\s+{name}\s+to\s+(.+)$",
            rf"^for\s+{name}\s*[:,-]\s*(.+)$",
            rf"^{name}\s*[:,-]\s*(.+)$",
        )
        for pattern in patterns:
            match = re.match(pattern, title, flags=re.IGNORECASE)
            if match is None:
                continue
            cleaned = " ".join(match.group(1).strip(" .:-").split())
            if cleaned:
                return cleaned, member.id
    return title, None


def _reminder_context_title(reminder: Reminder) -> str:
    if reminder.assignee_label:
        return f"{reminder.assignee_label}: {reminder.title}"
    return reminder.title


def _reminder_resolution(text: str) -> tuple[ReminderStatus, str] | None:
    stripped = " ".join(text.strip().split())
    if re.fullmatch(r"done[.!]?", stripped, flags=re.IGNORECASE):
        return (ReminderStatus.COMPLETED, "")
    complete_match = REMINDER_COMPLETE.match(stripped)
    if complete_match:
        return (ReminderStatus.COMPLETED, complete_match.group(1).strip(" .:-"))
    cancel_match = REMINDER_CANCEL.match(stripped)
    if cancel_match:
        query = next((group for group in cancel_match.groups() if group), "")
        return (ReminderStatus.CANCELED, query.strip(" .:-"))
    return None


def _memory_kind(text: str) -> MemoryKind:
    lower = text.lower()
    if any(word in lower for word in ("likes", "prefers", "favorite", "hates", "doesn't like")):
        return MemoryKind.PREFERENCE
    if any(word in lower for word in ("every ", "usually", "routine", "bedtime", "pickup")):
        return MemoryKind.ROUTINE
    if any(word in lower for word in ("never", "must", "cannot", "allergy", "allergic")):
        return MemoryKind.CONSTRAINT
    return MemoryKind.FACT


def _setup_command(text: str) -> bool:
    return " ".join(text.strip().lower().split()) in {
        "setup",
        "start setup",
        "setup status",
        "onboarding",
        "onboarding status",
        "readiness",
        "readiness status",
    }


def _google_connection_request(text: str) -> bool:
    normalized = " ".join(text.strip(" .!?").lower().split())
    if normalized in GOOGLE_CONNECT_COMMANDS:
        return True
    return bool(
        GOOGLE_CONNECT_NATURAL.search(normalized)
        or GOOGLE_CONNECT_ACCOUNT_NATURAL.search(normalized)
    )


def _simple_greeting(text: str) -> bool:
    normalized = " ".join(text.strip(" .!?").lower().split())
    return normalized in SIMPLE_GREETING_COMMANDS


def _manual_household_thread_handoff(lower: str) -> bool:
    if not lower:
        return False
    thread_terms = ("group", "thread", "chat", "text")
    household_terms = (
        "partner",
        "coparent",
        "co-parent",
        "spouse",
        "wife",
        "husband",
        "family",
        "household",
        "everyone",
        "both",
        "together",
    )
    if any(term in lower for term in thread_terms) and any(term in lower for term in household_terms):
        return True
    if any(name in lower for name in ("florence", "flornece")) and any(
        term in lower for term in household_terms
    ):
        return True
    return False


def _natural_onboarding_continuation(text: str, lower: str) -> bool:
    normalized = " ".join(text.strip(" .!?").lower().split())
    if not normalized or _setup_command(normalized) or _looks_like_household_content(text, lower):
        return False
    if _simple_greeting(normalized) or normalized in NATURAL_ONBOARDING_CONTINUATIONS:
        return True
    words = normalized.split()
    if len(words) > 14:
        return False
    if any(word in words for word in ("setup", "onboarding")):
        return True
    if normalized.startswith(("what ", "how ")) and any(
        phrase in normalized
        for phrase in (
            "next",
            "need",
            "do i",
            "do we",
            "should i",
            "should we",
            "does this work",
        )
    ):
        return True
    return False


def _looks_like_household_content(text: str, lower: str) -> bool:
    if PHONE_LIKE.search(text) or EMAIL_LIKE.search(text):
        return True
    if (
        REMINDER_PREFIX.search(text)
        or SOURCE_ALWAYS.match(text.strip())
        or SOURCE_MUTE.match(text.strip())
        or CHILD_IS.match(text.strip())
        or CHILDREN_ARE.match(text.strip())
        or CHILDREN_NATURAL.match(text.strip())
        or PARTNER_INVITE.match(text.strip())
        or PARTNER_CONFIRM.match(text.strip())
    ):
        return True
    content_markers = (
        "remind",
        "calendar",
        "event",
        "kid",
        "child",
        "school",
        "pickup",
        "dropoff",
        "permission",
        "remember",
        "forget",
        "mute",
        "connect google",
        "disconnect google",
        "source",
        "privacy",
        "support",
        "prep",
        "dinner",
        "lunch",
        "tomorrow",
    )
    return lower.startswith("my name is") or any(marker in lower for marker in content_markers)


def _bare_name_reply(text: str) -> str | None:
    name = " ".join(text.strip(" .!?").split())
    if not name or len(name) > 60:
        return None
    if len(name.split()) > 3:
        return None
    normalized = name.lower()
    if normalized in {
        "hi",
        "hello",
        "hey",
        "help",
        "no",
        "nope",
        "ok",
        "okay",
        "setup",
        "status",
        "sure",
        "test",
        "thanks",
        "thank you",
        "yes",
        "yep",
    }:
        return None
    if not re.fullmatch(r"[A-Za-z][A-Za-z' -]{0,59}", name):
        return None
    return name


def _bare_child_names_from_text(text: str) -> list[str]:
    candidate = " ".join(text.strip(" .!?").split())
    normalized = candidate.lower()
    if not _prompted_bare_detail_candidate(candidate, normalized, max_words=8):
        return []
    names = _split_names(candidate)
    if not names:
        return []
    return [name for name in names if re.fullmatch(r"[A-Za-z][A-Za-z' -]{0,59}", name)]


def _bare_source_preference_phrase(text: str) -> str:
    candidate = _preference_phrase(text)
    if not _prompted_bare_detail_candidate(candidate, candidate, max_words=8):
        return ""
    return candidate


def _prompted_bare_detail_candidate(candidate: str, normalized: str, *, max_words: int) -> bool:
    if not candidate or len(candidate) > 100 or len(normalized.split()) > max_words:
        return False
    if PHONE_LIKE.search(candidate) or EMAIL_LIKE.search(candidate):
        return False
    if normalized in SIMPLE_GREETING_COMMANDS or normalized in NATURAL_ONBOARDING_CONTINUATIONS:
        return False
    if normalized in {
        "help",
        "no",
        "nope",
        "setup",
        "status",
        "stop",
        "support",
        "thanks",
        "thank you",
        "yes",
        "yep",
    }:
        return False
    if (
        _setup_command(normalized)
        or REMINDER_PREFIX.search(candidate)
        or SOURCE_ALWAYS.match(candidate)
        or SOURCE_MUTE.match(candidate)
        or PARTNER_INVITE.match(candidate)
        or PARTNER_CONFIRM.match(candidate)
        or _google_connection_request(candidate)
    ):
        return False
    return True


def _help_topic(lower: str) -> str | None:
    if lower.startswith("help "):
        topic = lower.removeprefix("help ").strip(" ?")
        return topic or None
    if lower.endswith(" help"):
        topic = lower.removesuffix(" help").strip(" ?")
        return topic or None
    return None


def _source_feedback_kind(text: str) -> SourceFeedbackKind | None:
    normalized = " ".join(text.strip(" .!?").lower().split())
    if normalized in {
        "can be ignored",
        "definitely ignore this",
        "ignore emails like this",
        "ignore this",
        "ignore these",
        "not important",
        "not useful",
        "spammy",
        "too noisy",
        "dont show this",
        "don't show this",
        "keep this quiet",
        "mute this",
        "these can be ignored",
        "this can be ignored",
    }:
        return SourceFeedbackKind.NOT_USEFUL
    if (
        ("ignore" in normalized or "ignored" in normalized)
        and any(marker in normalized for marker in ("email", "emails", "this", "these", "like this"))
    ):
        return SourceFeedbackKind.NOT_USEFUL
    if any(marker in normalized for marker in ("too promotional", "marketing email", "marketing emails")):
        return SourceFeedbackKind.NOT_USEFUL
    if normalized in {
        "important",
        "this is important",
        "useful",
        "that was useful",
        "good catch",
        "more like this",
        "show me more like this",
    }:
        return SourceFeedbackKind.USEFUL
    return None


def _explicit_source_feedback_phrases(text: str, feedback: SourceFeedbackKind) -> list[str]:
    if feedback != SourceFeedbackKind.NOT_USEFUL:
        return []
    normalized = text.replace("’", "'").replace("‘", "'")
    phrases: list[str] = []
    for pattern in (
        r"\b(?:we|i)\s+(?:are\s+not|aren't|arent|won't|wont|will\s+not|not)\s+"
        r"(?:doing|using|attending|going\s+to|signed\s+up\s+for)\s+"
        r"(?P<phrase>[^.!?;\n,]+?)(?:\s+so\b|\s+because\b|$)",
        r"\b(?:any|all|future|the)?\s*(?P<phrase>[A-Za-z0-9\"'“”][^.!?;\n]{1,80}?)\s+"
        r"(?:emails?|messages?|notices?|alerts?)\s+(?:can|could|should)\s+be\s+ignored\b",
        r"\bignore\s+(?:any|all|future|the)?\s*(?P<phrase>[^.!?;\n]{2,80}?)"
        r"(?:\s+emails?|\s+messages?|\s+notices?|\s+alerts?|$)",
    ):
        for match in re.finditer(pattern, normalized, flags=re.IGNORECASE):
            phrase = _clean_explicit_source_feedback_phrase(match.group("phrase"))
            if phrase and phrase not in phrases:
                phrases.append(phrase)
    return phrases[:4]


def _clean_explicit_source_feedback_phrase(raw_phrase: str) -> str:
    phrase = html.unescape(raw_phrase)
    phrase = phrase.replace("“", '"').replace("”", '"').replace("’", "'").replace("‘", "'")
    phrase = re.sub(r"\s+", " ", phrase).strip(" \"'.,;:!?()[]{}")
    phrase = re.sub(r"^(?:any|all|future|the|these|those|this|that)\s+", "", phrase, flags=re.IGNORECASE)
    phrase = re.sub(
        r"\s+(?:emails?|messages?|notices?|alerts?|as well|too|please|for now)$",
        "",
        phrase,
        flags=re.IGNORECASE,
    ).strip(" \"'.,;:!?()[]{}")
    normalized = _preference_phrase(phrase)
    if normalized in {
        "it",
        "that",
        "this",
        "these",
        "those",
        "email",
        "emails",
        "message",
        "messages",
        "stuff",
        "things",
        "anything",
        "everything",
    }:
        return ""
    if not normalized or len(normalized) < 3 or len(normalized.split()) > 8:
        return ""
    if PHONE_LIKE.search(phrase) or EMAIL_LIKE.search(phrase):
        return ""
    return normalized


def _feedback_phrase(item: SourceItem, feedback: SourceFeedbackKind | None = None) -> str:
    if feedback == SourceFeedbackKind.NOT_USEFUL:
        noisy_phrase = _noisy_feedback_phrase(item)
        if noisy_phrase:
            return noisy_phrase
    title = " ".join(item.title.strip(" .").lower().split())
    return title[:120] or item.source_type


def _noisy_feedback_phrase(item: SourceItem) -> str | None:
    text = f"{item.title}\n{item.body}".lower()
    for phrase in (
        "final few spots",
        "few spots",
        "limited space",
        "spots left",
        "register now",
        "enroll now",
        "enrollment open",
        "starts next week",
        "summer camp starts",
        "gift ideas",
        "view online",
    ):
        if phrase in text:
            return phrase
    return None


def _email_search_requested(text: str) -> bool:
    normalized = " ".join(text.strip(" .!?").lower().split())
    if not normalized:
        return False
    if any(
        marker in normalized
        for marker in (
            "search my email",
            "search our email",
            "search email",
            "search gmail",
            "look in my email",
            "look through my email",
            "check my email",
            "check gmail",
            "find in my email",
            "find it in my email",
        )
    ):
        return True
    return "in my email" in normalized and normalized.startswith(("do you see", "can you find", "find"))


def _gmail_search_queries(text: str) -> list[str]:
    keywords = _email_search_keywords(text)
    if not keywords:
        return []
    queries: list[str] = []
    airline = next((word for word in keywords if word in {"american", "delta", "united", "jetblue", "southwest"}), "")
    if airline:
        queries.append(f"{airline} flight")
        queries.append(airline)
    if len(keywords) >= 2:
        queries.append(" ".join(keywords[:5]))
    queries.append(keywords[0])
    deduped: list[str] = []
    for query in queries:
        if query and query not in deduped:
            deduped.append(query)
    return deduped[:4]


def _email_search_keywords(text: str) -> list[str]:
    stopwords = {
        "add",
        "also",
        "and",
        "are",
        "back",
        "can",
        "check",
        "connected",
        "details",
        "email",
        "find",
        "flight",
        "flights",
        "from",
        "have",
        "here",
        "into",
        "look",
        "mail",
        "my",
        "our",
        "search",
        "see",
        "the",
        "then",
        "there",
        "these",
        "they",
        "those",
        "through",
        "to",
        "trip",
        "with",
        "you",
    }
    keywords: list[str] = []
    for raw in re.findall(r"[A-Za-z][A-Za-z0-9']*", text):
        word = raw.lower().strip("'")
        if len(word) <= 2 and word not in {"la"}:
            continue
        if word in stopwords:
            continue
        if word not in keywords:
            keywords.append(word)
    return keywords[:8]


def _email_search_context_for_agent(
    *,
    query: str,
    items: list[SourceItem],
    timezone_name: str,
) -> str:
    lines = [
        f"Florence searched connected Gmail for '{query}' and found {len(items)} likely matches.",
        "Use these results to answer the parent naturally. Do not ask them to forward emails that are already shown here.",
        EMAIL_SEARCH_AGENT_FOCUS,
    ]
    for index, item in enumerate(items[:EMAIL_SEARCH_MAX_RESULTS], start=1):
        sender = _source_sender_label(item.sender) or "unknown sender"
        observed = format_local(item.observed_at_utc, timezone_name)
        snippet = _email_search_snippet(item.body)
        lines.append(f"{index}. {item.title} from {sender} ({observed})")
        if snippet:
            lines.append(f"   Snippet: {snippet}")
    return "\n".join(lines)


def _email_search_snippet(body: str) -> str:
    compact = " ".join(html.unescape(body).split())
    if len(compact) <= 500:
        return compact
    return compact[:501].rsplit(" ", 1)[0].rstrip(" ,;:-") + "..."


def _source_sender_label(sender: str | None) -> str | None:
    normalized = normalize_source_sender(sender)
    if not normalized:
        return None
    name, address = parseaddr(normalized)
    label = name or (address if "@" in address else normalized)
    return normalize_source_sender(label.strip().strip('"')) or None


def _source_surface_context(item: SourceItem, title: str) -> str | None:
    body = normalize_source_body(html.unescape(item.body))
    if not body:
        return None
    title_key = _source_context_key(title)
    for candidate in _source_context_candidates(body):
        cleaned = _clean_source_context(candidate)
        if _source_context_is_useful(cleaned, title_key):
            return f"Context: {cleaned}"
    fallback = _clean_source_context(body)
    if _source_context_is_useful(fallback, title_key):
        return f"Context: {fallback}"
    return None


def _source_context_candidates(body: str) -> list[str]:
    return [
        part
        for part in re.split(r"(?<=[.!?])\s+|\s+[\u2022|]\s+|\s+-\s+", body)
        if part.strip()
    ]


def _clean_source_context(candidate: str) -> str:
    compact = " ".join(candidate.strip(" -").split())
    compact = re.sub(r"https?://\S+", "", compact).strip()
    compact = re.sub(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b", "", compact).strip()
    compact = " ".join(compact.split())
    return _truncate_source_context(compact)


def _truncate_source_context(value: str, *, limit: int = 180) -> str:
    if len(value) <= limit:
        return value
    truncated = value[: limit + 1].rsplit(" ", 1)[0].rstrip(" ,;:-")
    return f"{truncated}..."


def _source_context_is_useful(candidate: str, title_key: str) -> bool:
    if len(candidate) < 12:
        return False
    candidate_key = _source_context_key(candidate)
    if not candidate_key:
        return False
    if candidate_key == title_key:
        return False
    if title_key and title_key in candidate_key and len(candidate_key) <= len(title_key) + 24:
        return False
    boilerplate = (
        "all-access",
        "click here",
        "content coming your way",
        "facebook",
        "follow us",
        "instagram",
        "manage preferences",
        "privacy policy",
        "snapchat",
        "stay tuned",
        "tiktok",
        "unsubscribe",
        "view in browser",
        "view online",
        "youtube",
    )
    return not any(marker in candidate_key for marker in boilerplate)


def _source_context_key(value: str) -> str:
    normalized = html.unescape(value).lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return " ".join(normalized.split())


def _source_item_id(*, household_id: str, source_type: str, external_id: str | None) -> str:
    if external_id:
        key = f"florence:source-item:{household_id}:{source_type}:{external_id}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, key))
    return str(uuid.uuid4())


def _attachment_source_item(
    *,
    household_id: str,
    incoming: IncomingMessage,
    attachment: MessageAttachment,
    index: int,
    timezone_name: str,
    now: datetime,
) -> SourceItem:
    external_id = f"linq:{incoming.message_id}:attachment:{index}"
    title = (
        _attachment_summary_title(incoming.text)
        or _attachment_summary_title(attachment.extracted_text)
        or normalize_source_title(attachment.filename)
        or _attachment_title(attachment)
    )
    return SourceItem(
        id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{household_id}:{external_id}")),
        household_id=household_id,
        source_type=_attachment_source_type(attachment),
        title=title,
        body=_attachment_source_body(incoming.text, attachment),
        sender=incoming.sender,
        external_id=external_id,
        observed_at_utc=incoming.received_at,
        event_at_utc=_attachment_event_at(incoming.text, attachment, timezone_name, now),
    )


def _attachment_source_type(attachment: MessageAttachment) -> str:
    label = f"{attachment.kind} {attachment.content_type or ''}".lower()
    if "image" in label or "photo" in label:
        return "flyer"
    return "document"


def _attachment_title(attachment: MessageAttachment) -> str:
    label = _attachment_label(attachment)
    return normalize_source_title(f"Shared {label}") or "Shared attachment"


def _attachment_summary_title(value: str | None) -> str:
    text = normalize_source_title(value)
    match = re.match(r"(.+?[.!?])(?:\s|$)", text)
    if match and len(match.group(1)) >= 8:
        return match.group(1)
    return text


def _attachment_source_body(caption: str, attachment: MessageAttachment) -> str:
    details = []
    normalized_caption = normalize_source_body(caption)
    if normalized_caption:
        details.append(normalized_caption)
    extracted_text = normalize_source_body(attachment.extracted_text)
    if extracted_text:
        details.append(f"Extracted text: {extracted_text}")
    details.append(f"Attachment: {_attachment_label(attachment)}.")
    if not extracted_text:
        details.append("Text extraction is not enabled for this attachment yet.")
    return normalize_source_body("\n".join(details))


def _attachment_event_at(
    caption: str,
    attachment: MessageAttachment,
    timezone_name: str,
    now: datetime,
) -> datetime | None:
    text = "\n".join(
        part
        for part in (
            caption.strip(),
            attachment.extracted_text or "",
        )
        if part
    )
    if not text:
        return None
    parsed = parse_due_time(text, timezone_name, now_utc=now)
    if parsed.needs_clarification:
        return None
    return parsed.due_at_utc


def _attachment_label(attachment: MessageAttachment) -> str:
    if attachment.filename:
        return attachment.filename
    if attachment.content_type:
        return attachment.content_type
    return attachment.kind or "attachment"


def _assistant_prompt_invited_attachment_reply(text: str) -> bool:
    normalized = " ".join(text.strip().lower().split())
    if not normalized:
        return False
    requested_media_terms = (
        "send me",
        "send over",
        "share",
        "screenshots",
        "screenshot",
        "pdfs",
        "pdf",
        "emails",
        "email",
        "camp names",
        "camp schedules",
        "schedule",
    )
    follow_up_terms = (
        "first,",
        "are we",
        "which kid",
        "which child",
        "which kids",
        "theo",
        "violet",
        "both",
        "locking down",
    )
    if any(term in normalized for term in requested_media_terms):
        return True
    return normalized.endswith("?") and any(term in normalized for term in follow_up_terms)


def _assistant_prompt_expects_reply(text: str) -> bool:
    normalized = " ".join(text.strip().lower().split())
    if not normalized:
        return False
    if _assistant_prompt_requires_deterministic_reply(normalized):
        return False
    if _assistant_prompt_invited_attachment_reply(text):
        return True
    if "?" in normalized:
        return True
    reply_markers = (
        "tell me",
        "send me",
        "send over",
        "reply",
        "what should",
        "what do you",
        "who should",
        "which",
        "when should",
        "where should",
        "first,",
        "next,",
    )
    return any(marker in normalized for marker in reply_markers)


def _assistant_prompt_requires_deterministic_reply(normalized: str) -> bool:
    deterministic_markers = (
        "what should i call you",
        "send your partner's phone number",
        "ask your partner to send their name",
        "next setup step",
        "next useful setup step",
        "next helpful step",
        "last setup step",
        "setup next action",
        "setup link:",
        "use this setup link",
        "to finish getting set up",
        "to finish setup",
        "this looks worth your attention",
        "reply 'approve",
        "reply approve",
        "i can also add a reminder for this",
    )
    if any(marker in normalized for marker in deterministic_markers):
        return True
    if normalized.startswith(("nice to meet you", "got it", "thanks")) and "next" in normalized:
        return True
    return False


def _active_conversation_rail_command(text: str, lower: str) -> bool:
    normalized = " ".join(text.strip(" .!?").lower().split())
    if not normalized:
        return False
    if (
        normalized in SIMPLE_GREETING_COMMANDS
        or normalized in SUPPORT_COMMANDS
        or normalized in DATA_DELETE_CONFIRM_COMMANDS
        or normalized in DATA_DELETE_REQUEST_COMMANDS
        or normalized in DATA_SUMMARY_COMMANDS
        or normalized in GOOGLE_DISCONNECT_COMMANDS
        or normalized in MEMORY_CLEAR_COMMANDS
        or normalized in {
            "help",
            "?",
            "stop",
            "stop household",
            "pause florence",
            "start",
            "restart",
            "resume",
            "resume florence",
            "privacy status",
            "privacy settings",
            "memory controls",
            "pause memory",
            "turn off memory",
            "disable memory",
            "resume memory",
            "turn on memory",
            "enable memory",
            "opt in to product analytics",
            "turn on product analytics",
            "opt out of product analytics",
            "turn off product analytics",
            "source review",
            "source summary",
            "email review",
            "email summary",
            "source preferences",
            "source rules",
            "email preferences",
            "email rules",
            "handoff",
            "household handoff",
            "open items",
            "pending items",
            "pending approvals",
        }
    ):
        return True
    if (
        _setup_command(normalized)
        or _google_connection_request(text)
        or APPROVAL_REPLY.match(text.strip())
        or PARTNER_INVITE.match(text.strip())
        or PARTNER_CONFIRM.match(text.strip())
        or REMINDER_PREFIX.search(text)
        or _calendar_event_text(text) is not None
        or _reminder_resolution(text) is not None
        or _source_feedback_kind(text) is not None
        or _email_search_requested(text)
    ):
        return True
    return lower.strip(" ?") in {
        "what did you keep quiet",
        "what have you kept quiet",
        "what is open",
        "what's open",
        "what needs approval",
    }


def _looks_like_active_conversation_reply(text: str) -> bool:
    normalized = " ".join(text.strip(" .!").lower().split())
    if not normalized or normalized.endswith("?"):
        return False
    if SOURCE_ALWAYS.match(text.strip()) or SOURCE_MUTE.match(text.strip()):
        return True
    if normalized.startswith(("remember that ", "please remember that ", "remember ", "please remember ")):
        return True
    words = normalized.split()
    if len(words) <= 10:
        return True
    if len(words) <= 20 and any(
        word in words
        for word in (
            "yes",
            "yep",
            "yeah",
            "no",
            "nope",
            "both",
            "either",
            "same",
            "this",
            "that",
            "there",
            "tomorrow",
            "today",
        )
    ):
        return True
    return False


def _incoming_agent_text(incoming: IncomingMessage) -> str:
    if not incoming.attachments:
        return incoming.text
    text = incoming.text.strip()
    attachment_lines = []
    for index, attachment in enumerate(incoming.attachments[:3], start=1):
        label = _attachment_label(attachment)
        extracted = normalize_source_body(attachment.extracted_text)
        if extracted:
            attachment_lines.append(f"Attachment {index}: {label}. Extracted text: {extracted}")
        else:
            attachment_lines.append(f"Attachment {index}: {label}. Text extraction is not available.")
    if len(incoming.attachments) > 3:
        attachment_lines.append(f"{len(incoming.attachments) - 3} more attachments were included.")
    attachment_context = "\n".join(attachment_lines)
    if not text:
        return attachment_context
    return f"{text}\n\n{attachment_context}"


def _incoming_message_body(incoming: IncomingMessage) -> str:
    if incoming.text:
        return incoming.text
    if not incoming.attachments:
        return ""
    noun = "attachment" if len(incoming.attachments) == 1 else "attachments"
    labels = ", ".join(_attachment_label(attachment) for attachment in incoming.attachments[:3])
    return f"Shared {len(incoming.attachments)} {noun}: {labels}"


def _source_reference_kind(text: str) -> str | None:
    normalized = _preference_phrase(text)
    if normalized in SOURCE_SENDER_REFERENCES:
        return "sender"
    if normalized in SOURCE_DOMAIN_REFERENCES:
        return "domain"
    return None


def _source_reference_phrase(item: SourceItem, reference_kind: str) -> str:
    sender = item.sender or ""
    if reference_kind == "domain":
        return _sender_domain_phrase(sender)
    return _sender_phrase(sender)


def _sender_phrase(sender: str) -> str:
    _name, address = parseaddr(sender)
    return _preference_phrase(address or sender)


def _sender_domain_phrase(sender: str) -> str:
    sender_phrase = _sender_phrase(sender)
    if "@" not in sender_phrase:
        return ""
    return _preference_phrase(sender_phrase.rsplit("@", 1)[1])


def _child_names_from_text(text: str) -> list[str]:
    stripped = text.strip()
    match = CHILDREN_ARE.match(stripped) or CHILD_IS.match(stripped) or CHILDREN_NATURAL.match(stripped)
    if match is None:
        return []
    return _split_names(match.group(1).strip(" :,-"))


def _member_by_id(members: list[HouseholdMember], member_id: str) -> HouseholdMember | None:
    return next((member for member in members if member.id == member_id), None)


def _web_base_url(raw: str | None) -> str | None:
    if not raw:
        return None
    normalized = raw.strip().rstrip("/")
    if not normalized.startswith(("https://", "http://")):
        return None
    return normalized


def _web_text(value: object, *, max_chars: int = 240) -> str:
    text = " ".join(str(value or "").replace("\r", "\n").split())
    return text[:max_chars].strip(" .")


def _web_list(value: object, *, max_items: int = 12, max_chars: int = 240) -> list[str]:
    raw = str(value or "").replace("\r", "\n")
    parts: list[str] = []
    for line in raw.splitlines():
        item = _web_text(line, max_chars=max_chars)
        if item:
            parts.append(item)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in parts:
        key = item.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped[:max_items]


def _web_household_memories(data: dict[str, object]) -> list[tuple[MemoryKind, str | None, str, float]]:
    memories: list[tuple[MemoryKind, str | None, str, float]] = []
    household_name = _web_text(data.get("household_name"), max_chars=80)
    if household_name:
        memories.append((MemoryKind.FACT, "household", f"Household name: {household_name}.", 0.9))
    location = _web_text(data.get("location"), max_chars=160)
    if location:
        memories.append((MemoryKind.FACT, "household", f"Household location: {location}.", 0.9))
    for child_memory in _web_child_memories(data):
        memories.append(child_memory)
    for pet in _web_list(data.get("pets"), max_items=6, max_chars=160):
        memories.append((MemoryKind.FACT, "household", f"Pet: {pet}.", 0.85))
    for caretaker in _web_list(data.get("caretakers"), max_items=8, max_chars=180):
        memories.append((MemoryKind.FACT, "household", f"Caretaker: {caretaker}.", 0.85))
    extra_context = _web_text(data.get("family_context"), max_chars=240)
    if extra_context:
        memories.append((MemoryKind.FACT, "household", f"Household context: {extra_context}.", 0.8))
    return memories


def _web_source_rule_phrase(raw_rule: str) -> str:
    return re.sub(
        r"^(?:always tell me about|always surface|always show me)\s+",
        "",
        raw_rule,
        flags=re.IGNORECASE,
    ).strip()


def _web_child_memories(data: dict[str, object]) -> list[tuple[MemoryKind, str | None, str, float]]:
    memories: list[tuple[MemoryKind, str | None, str, float]] = []
    for index in range(1, 5):
        name = _web_text(data.get(f"child_{index}_name"), max_chars=60)
        if not name:
            continue
        details = [f"Child profile: {name}"]
        age = _web_text(data.get(f"child_{index}_age"), max_chars=24)
        grade = _web_text(data.get(f"child_{index}_grade"), max_chars=40)
        school = _web_text(data.get(f"child_{index}_school"), max_chars=100)
        activities = _web_text(data.get(f"child_{index}_activities"), max_chars=160)
        location = _web_text(data.get(f"child_{index}_location"), max_chars=160)
        if age:
            details.append(f"age {age}")
        if grade:
            details.append(f"grade {grade}")
        if school:
            details.append(f"school {school}")
        if activities:
            details.append(f"activities {activities}")
        if location:
            details.append(f"school/activity location {location}")
        memories.append((MemoryKind.FACT, name, "; ".join(details) + ".", 0.95))
    return memories


def _partner_web_invite_text(partner_url: str | None) -> str:
    if partner_url:
        return (
            "Hi, Florence is helping us coordinate the household. "
            f"Can you connect your Google account and confirm the setup here? {partner_url}"
        )
    return (
        "Hi, Florence is helping us coordinate the household. "
        "Can you text Florence your name and connect your Google account when you are in the shared thread?"
    )


def _split_names(value: str) -> list[str]:
    normalized = re.sub(r"\band\b", ",", value.strip(" ."), flags=re.IGNORECASE)
    names: list[str] = []
    seen: set[str] = set()
    for raw_name in normalized.split(","):
        name = " ".join(raw_name.strip(" .").split())
        if not name or len(name) > 60:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
    return names[:8]


def _phone_from_text(text: str) -> str | None:
    match = PHONE_LIKE.search(text)
    if match is None:
        return None
    raw = match.group(0).strip()
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if raw.startswith("+") and 8 <= len(digits) <= 15:
        return "+" + digits
    return None


def _child_names_from_memories(memories) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for memory in memories:
        for name in _child_names_from_memory(memory):
            key = name.casefold()
            if not name or key in seen:
                continue
            seen.add(key)
            names.append(name)
    return names


def _child_names_from_memory(memory) -> list[str]:
    text = str(getattr(memory, "text", "") or "")
    if text.startswith("Child profile:"):
        raw = str(getattr(memory, "subject", "") or "")
        if not raw:
            raw = text.removeprefix("Child profile:").split(";", 1)[0].strip(" .")
        return [raw] if raw else []
    match = CHILDREN_MEMORY.match(text.strip(" ."))
    if match is None:
        return []
    return [name for name, _age in _child_entries(match.group(1))]


def _child_entries(text: str) -> list[tuple[str, str | None]]:
    entries: list[tuple[str, str | None]] = []
    for match in CHILD_ENTRY.finditer(text.strip(" .")):
        name = " ".join(match.group("name").strip(" ,").split())
        if name:
            entries.append((name, match.group("age")))
    return entries


def _preference_phrase(text: str) -> str:
    return " ".join(text.strip(" .").lower().split())[:120]


def _coerce_dt(value: object, default: datetime) -> datetime:
    parsed = _coerce_optional_dt(value)
    return parsed or default


def _coerce_optional_dt(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return ensure_utc(value)
    if value in (None, ""):
        return None
    try:
        return ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _optional_str(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
