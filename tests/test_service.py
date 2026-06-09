import threading
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import pytest

from florence.actions import run_approved_actions
from florence.config import Settings
from florence.models import (
    ActionExecutionStatus,
    IncomingMessage,
    MessageAttachment,
    MemoryKind,
    OutboundMessage,
    PendingActionStatus,
    ReminderStatus,
    ConnectedAccountStatus,
    SourceFeedbackKind,
    SourcePreferenceKind,
)
from florence.oauth import TokenVault
from florence.service import FlorenceService
from florence.source_ingest import MAX_SOURCE_BODY_CHARS
from florence.store import Store
from florence.worker import run_routine_tick


class FakeAgent:
    def __init__(self, response="Fake agent reply."):
        self.calls = []
        self.response = response

    def complete(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


def _service(tmp_path):
    settings = Settings(db_path=str(tmp_path / "florence.sqlite"))
    agent = FakeAgent()
    return FlorenceService(settings=settings, agent=agent), agent


def _google_service(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
        token_encryption_key=TokenVault.generate_key(),
    )
    agent = FakeAgent()
    return FlorenceService(settings=settings, agent=agent), agent


def _linq_service(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        linq_api_key="linq-api-key",
        linq_from_phone="+15555550000",
    )
    agent = FakeAgent()
    return FlorenceService(settings=settings, agent=agent), agent


def _support_service(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        support_contact="support@example.com",
    )
    agent = FakeAgent()
    return FlorenceService(settings=settings, agent=agent), agent


class FakeSender:
    def __init__(self):
        self.sent = []

    def send_text(self, *, chat_id: str, text: str, idempotency_key: str):
        self.sent.append(
            {
                "chat_id": chat_id,
                "text": text,
                "idempotency_key": idempotency_key,
            }
        )


class FlakySender(FakeSender):
    def __init__(self, *, fail_send: int = 0):
        super().__init__()
        self.fail_send = fail_send

    def send_text(self, *, chat_id: str, text: str, idempotency_key: str):
        if self.fail_send:
            self.fail_send -= 1
            raise RuntimeError("linq send failed")
        return super().send_text(chat_id=chat_id, text=text, idempotency_key=idempotency_key)


def _incoming(
    text,
    *,
    chat_id="chat-1",
    message_id="message-1",
    sender="+15555550100",
    received_at: datetime | None = None,
):
    return IncomingMessage(
        chat_id=chat_id,
        message_id=message_id,
        sender=sender,
        text=text,
        received_at=received_at or datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )


def test_household_creation_is_idempotent_under_first_message_race(tmp_path):
    class RacingStore(Store):
        def __init__(self, path: str, barrier: threading.Barrier):
            super().__init__(path)
            self.barrier = barrier
            self.remaining_race_reads = 2
            self.race_lock = threading.Lock()

        def get_household_by_chat(self, chat_id: str):
            should_race = False
            if chat_id == "race-chat":
                with self.race_lock:
                    if self.remaining_race_reads > 0:
                        self.remaining_race_reads -= 1
                        should_race = True
            if should_race:
                self.barrier.wait(timeout=5)
                return None
            return super().get_household_by_chat(chat_id)

    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    store = RacingStore(str(tmp_path / "florence.sqlite"), threading.Barrier(2))
    results = []
    errors = []

    def create_household() -> None:
        try:
            results.append(
                store.get_or_create_household(
                    chat_id="race-chat",
                    timezone_name="America/Los_Angeles",
                    now_utc=now,
                )
            )
        except Exception as exc:  # pragma: no cover - assertion reports the error.
            errors.append(exc)

    threads = [threading.Thread(target=create_household) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert errors == []
    assert len(results) == 2
    assert results[0].id == results[1].id
    assert store.get_household_by_chat("race-chat").id == results[0].id
    assert len([household for household in store.list_households() if household.chat_id == "race-chat"]) == 1


def test_manual_group_chat_from_known_parent_reuses_household(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household = service.store.get_or_create_household(
        chat_id="direct-chat",
        timezone_name=service.settings.default_timezone,
        now_utc=now,
    )
    jackson = service.store.get_or_create_member(
        household_id=household.id,
        phone="+15555550100",
        now_utc=now,
    )
    service.store.set_member_name(jackson.id, "Jackson", now_utc=now)
    kendall = service.store.ensure_parent_member(
        household_id=household.id,
        phone="+15555550101",
        now_utc=now,
    )
    assert kendall is not None
    service.store.set_member_name(kendall.id, "Kendall", now_utc=now)
    service.store.upsert_memory(
        household_id=household.id,
        kind=MemoryKind.FACT,
        subject="children",
        text="Jackson's children are Theo, age 7, and Violet, age 4.",
        now_utc=now,
    )
    service.store.upsert_connected_account(
        household_id=household.id,
        provider="google",
        external_account_id="google-account",
        account_label="Jackson",
        now_utc=now,
    )
    service.store.upsert_source_preference(
        household_id=household.id,
        phrase="school logistics",
        preference=SourcePreferenceKind.ALWAYS_SURFACE,
        created_by_member_id=jackson.id,
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "Ok here is the new group with Florence and Kendall",
            chat_id="manual-group-chat",
            message_id="manual-group-message",
            sender="+15555550100",
            received_at=now + timedelta(minutes=1),
        ),
        now_utc=now + timedelta(minutes=1),
    )

    group_household = service.store.get_household_by_chat("manual-group-chat")
    direct_household = service.store.get_household_by_chat("direct-chat")
    assert group_household is not None
    assert direct_household is not None
    assert group_household.id == household.id
    assert direct_household.id == household.id
    assert group_household.chat_id == "manual-group-chat"
    assert len(service.store.list_households()) == 1
    assert len(agent.calls) == 1
    assert outbound[0].text == "Fake agent reply."
    assert "what should I call each parent" not in outbound[0].text.lower()


def test_manual_group_chat_does_not_guess_when_phone_is_in_multiple_households(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    first = service.store.get_or_create_household(
        chat_id="first-chat",
        timezone_name=service.settings.default_timezone,
        now_utc=now,
    )
    second = service.store.get_or_create_household(
        chat_id="second-chat",
        timezone_name=service.settings.default_timezone,
        now_utc=now,
    )
    service.store.get_or_create_member(
        household_id=first.id,
        phone="+15555550100",
        now_utc=now,
    )
    service.store.get_or_create_member(
        household_id=second.id,
        phone="+15555550100",
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "Hi",
            chat_id="ambiguous-manual-group",
            message_id="ambiguous-manual-message",
            sender="+15555550100",
            received_at=now + timedelta(minutes=1),
        ),
        now_utc=now + timedelta(minutes=1),
    )

    new_household = service.store.get_household_by_chat("ambiguous-manual-group")
    assert new_household is not None
    assert new_household.id not in {first.id, second.id}
    assert len(agent.calls) == 0
    assert outbound[0].text.startswith("Hi, I'm Florence.")


def test_unknown_chat_from_known_single_parent_does_not_reuse_household_without_handoff(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    existing = service.store.get_or_create_household(
        chat_id="existing-chat",
        timezone_name=service.settings.default_timezone,
        now_utc=now,
    )
    service.store.get_or_create_member(
        household_id=existing.id,
        phone="+15555550100",
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "remember that Maya likes noodles",
            chat_id="ordinary-new-chat",
            message_id="ordinary-new-message",
            sender="+15555550100",
            received_at=now + timedelta(minutes=1),
        ),
        now_utc=now + timedelta(minutes=1),
    )

    new_household = service.store.get_household_by_chat("ordinary-new-chat")
    assert new_household is not None
    assert new_household.id != existing.id
    assert service.store.get_household_by_chat("existing-chat").id == existing.id
    assert len(agent.calls) == 0
    assert "Got it" in outbound[0].text


def _store_google_connected_account(service, *, chat_id: str, now: datetime):
    household = service.store.get_or_create_household(
        chat_id=chat_id,
        timezone_name=service.settings.default_timezone,
        now_utc=now,
    )
    account = service.store.upsert_connected_account(
        household_id=household.id,
        provider="google",
        external_account_id="google-sub-123",
        account_label="Parent Google",
        now_utc=now,
    )
    vault = TokenVault.from_settings(service.settings)
    service.store.upsert_connected_account_token(
        connected_account_id=account.id,
        provider="google",
        token_ciphertext=vault.encrypt(
            {
                "provider": "google",
                "access_token": "access-token",
                "refresh_token": "refresh-token",
            }
        ),
        scopes=("https://www.googleapis.com/auth/calendar.readonly",),
        now_utc=now,
    )
    return household, account


def test_service_refuses_past_reminder(tmp_path):
    service, _agent = _service(tmp_path)

    outbound = service.handle_incoming(
        _incoming("remind us today at 8am to call school"),
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )

    assert len(outbound) == 1
    assert "will not schedule it in the past" in outbound[0].text


def test_service_asks_for_clarification_on_bare_hour_reminder(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("remind us tomorrow at 8 to pack lunch", chat_id="ambiguous-time"),
        now_utc=now,
    )
    household = service.store.get_household_by_chat("ambiguous-time")

    assert len(outbound) == 1
    assert "Should I use AM or PM" in outbound[0].text
    assert "'at 8'" in outbound[0].text
    assert household is not None
    assert service.store.upcoming_reminders(household_id=household.id, now_utc=now) == []


def test_service_creates_and_sends_due_reminder(tmp_path):
    service, _agent = _service(tmp_path)

    created = service.handle_incoming(
        _incoming("remind us tomorrow at 8am to pack the permission slip"),
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    assert "Done." in created[0].text

    due = service.due_reminder_messages(
        now_utc=datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc),
    )

    assert len(due) == 1
    assert due[0].text == "Quick reminder: pack the permission slip"


def test_daypart_reminder_confirmation_uses_clean_task_title(tmp_path):
    service, _agent = _service(tmp_path)

    created = service.handle_incoming(
        _incoming("remind us tomorrow morning to pack lunch"),
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    due = service.due_reminder_messages(
        now_utc=datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc),
    )

    assert "pack lunch" in created[0].text
    assert "morning to pack lunch" not in created[0].text
    assert due[0].text == "Quick reminder: pack lunch"


def test_named_household_member_reminder_keeps_owner_label(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "member-reminder"

    service.handle_incoming(_incoming("my name is Sam", chat_id=chat_id, message_id="parent-name"), now_utc=now)
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id=chat_id,
            message_id="alex-name",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    created = service.handle_incoming(
        _incoming(
            "remind Alex today at 5pm to pack cleats",
            chat_id=chat_id,
            message_id="create-alex-reminder",
        ),
        now_utc=now,
    )
    agenda = service.handle_incoming(
        _incoming("what's on deck today?", chat_id=chat_id, message_id="agenda"),
        now_utc=now + timedelta(minutes=1),
    )
    due_at = datetime(2026, 6, 6, 0, 0, tzinfo=timezone.utc)
    due = service.due_reminder_messages(now_utc=due_at)

    household = service.store.get_household_by_chat(chat_id)
    assert household is not None
    reminder = service.store.recent_sent_reminders(
        household_id=household.id,
        now_utc=due_at,
        since_utc=now,
    )[0]

    assert "remind Alex about pack cleats" in created[0].text
    assert "Alex: pack cleats" in agenda[0].text
    assert due[0].text == "Quick reminder for Alex: pack cleats"
    assert reminder.title == "pack cleats"
    assert reminder.assignee_member_id is not None


def test_unnamed_reminder_assignee_uses_role_label_not_phone(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    due_at = now + timedelta(hours=1)
    household = service.store.get_or_create_household(
        chat_id="unnamed-assignee-reminder",
        timezone_name="America/Los_Angeles",
        now_utc=now,
    )
    member = service.store.get_or_create_member(
        household_id=household.id,
        phone="+15555550999",
        now_utc=now,
    )
    service.store.create_reminder(
        household_id=household.id,
        chat_id=household.chat_id,
        title="pack lunch",
        due_at_utc=due_at,
        created_at_utc=now,
        assignee_member_id=member.id,
    )

    due = service.due_reminder_messages(now_utc=due_at)

    assert due[0].text == "Quick reminder for unnamed parent: pack lunch"
    assert "+15555550999" not in due[0].text


def test_reminder_mentions_member_without_assigning_when_name_is_not_target(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "member-reminder-no-target"

    service.handle_incoming(_incoming("my name is Sam", chat_id=chat_id, message_id="parent-name"), now_utc=now)
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id=chat_id,
            message_id="alex-name",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    created = service.handle_incoming(
        _incoming(
            "remind us tomorrow at 8am to ask Alex about lunch",
            chat_id=chat_id,
            message_id="create-reminder",
        ),
        now_utc=now,
    )
    household = service.store.get_household_by_chat(chat_id)
    assert household is not None
    reminder = service.store.upcoming_reminders(
        household_id=household.id,
        now_utc=now,
    )[0]

    assert "remind you about ask Alex about lunch" in created[0].text
    assert "remind Alex about" not in created[0].text
    assert reminder.title == "ask Alex about lunch"
    assert reminder.assignee_member_id is None


def test_parent_can_mark_reminder_done_before_it_sends(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("remind us tomorrow at 8am to pack lunch", chat_id="reminder-done", message_id="create"),
        now_utc=now,
    )
    done = service.handle_incoming(
        _incoming("done pack lunch", chat_id="reminder-done", message_id="done"),
        now_utc=now + timedelta(minutes=5),
    )
    due = service.due_reminder_messages(now_utc=datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc))

    assert "marked pack lunch as handled" in done[0].text
    assert due == []


def test_parent_can_reply_done_after_due_reminder_sends(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    due_at = datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("remind us tomorrow at 8am to pack lunch", chat_id="reminder-done-after-send", message_id="create"),
        now_utc=now,
    )
    sent = service.due_reminder_messages(now_utc=due_at)
    done = service.handle_incoming(
        _incoming("done", chat_id="reminder-done-after-send", message_id="done"),
        now_utc=due_at + timedelta(minutes=5),
    )
    household = service.store.get_household_by_chat("reminder-done-after-send")
    assert household is not None
    with service.store.connect() as conn:
        row = conn.execute(
            "SELECT status FROM reminders WHERE household_id = ?",
            (household.id,),
        ).fetchone()

    assert sent[0].text == "Quick reminder: pack lunch"
    assert "marked pack lunch as handled" in done[0].text
    assert row["status"] == ReminderStatus.COMPLETED.value


def test_done_after_multiple_sent_reminders_asks_for_specificity(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    due_at = datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("remind us tomorrow at 8am to pack lunch", chat_id="reminder-done-ambiguous", message_id="one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("remind us tomorrow at 8am to pack cleats", chat_id="reminder-done-ambiguous", message_id="two"),
        now_utc=now,
    )
    service.due_reminder_messages(now_utc=due_at)

    done = service.handle_incoming(
        _incoming("done", chat_id="reminder-done-ambiguous", message_id="done"),
        now_utc=due_at + timedelta(minutes=5),
    )
    household = service.store.get_household_by_chat("reminder-done-ambiguous")
    assert household is not None
    recent = service.store.recent_sent_reminders(
        household_id=household.id,
        now_utc=due_at + timedelta(minutes=5),
        since_utc=due_at - timedelta(hours=1),
    )

    assert "more than one matching reminder" in done[0].text
    assert len(recent) == 2


def test_parent_can_cancel_reminder_before_it_sends(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming(
            "remind us tomorrow at 8am to bring permission slip",
            chat_id="reminder-cancel",
            message_id="create",
        ),
        now_utc=now,
    )
    canceled = service.handle_incoming(
        _incoming("cancel reminder permission slip", chat_id="reminder-cancel", message_id="cancel"),
        now_utc=now + timedelta(minutes=5),
    )
    due = service.due_reminder_messages(now_utc=datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc))

    assert "canceled the reminder for bring permission slip" in canceled[0].text
    assert due == []


def test_helper_cannot_complete_household_reminder(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("remind us tomorrow at 8am to pack lunch", chat_id="reminder-helper", message_id="create"),
        now_utc=now,
    )
    helper = service.handle_incoming(
        _incoming(
            "done pack lunch",
            chat_id="reminder-helper",
            message_id="helper-done",
            sender="+15555550101",
        ),
        now_utc=now + timedelta(minutes=5),
    )
    due = service.due_reminder_messages(now_utc=datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc))

    assert "need one of the parents" in helper[0].text
    assert len(due) == 1


def test_ambiguous_reminder_completion_keeps_reminders_active(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("remind us tomorrow at 8am to pack lunch", chat_id="reminder-ambiguous", message_id="one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "remind us tomorrow at 9am to pack cleats",
            chat_id="reminder-ambiguous",
            message_id="two",
        ),
        now_utc=now,
    )
    reply = service.handle_incoming(
        _incoming("done pack", chat_id="reminder-ambiguous", message_id="done"),
        now_utc=now + timedelta(minutes=5),
    )
    household = service.store.get_household_by_chat("reminder-ambiguous")
    assert household is not None
    upcoming = service.store.upcoming_reminders(household_id=household.id, now_utc=now)

    assert "more than one matching reminder" in reply[0].text
    assert len(upcoming) == 2


def test_agenda_request_is_limited_to_household_local_day(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("remind us today at 5pm to pick up cleats", chat_id="agenda-today", message_id="today"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "remind us tomorrow at 8am to pack lunch",
            chat_id="agenda-today",
            message_id="tomorrow",
        ),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming("what's on deck today?", chat_id="agenda-today", message_id="agenda"),
        now_utc=now,
    )

    assert "Here is what I have for today:" in outbound[0].text
    assert "pick up cleats" in outbound[0].text
    assert "pack lunch" not in outbound[0].text


def test_agenda_request_includes_today_source_items_without_email_body(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.ingest_source_item(
        chat_id="agenda-source",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for today's field trip. Full email body stays out.",
        event_at_utc=now + timedelta(hours=3),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming("what is on deck today?", chat_id="agenda-source", message_id="agenda"),
        now_utc=now + timedelta(minutes=1),
    )

    assert "Permission slip due" in outbound[0].text
    assert "Full email body" not in outbound[0].text


def test_agenda_request_keeps_source_items_visible_after_daily_briefing(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        daily_briefing_hour=7,
        daily_briefing_minute=15,
    )
    service = FlorenceService(settings=settings, agent=FakeAgent())
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.ingest_source_item(
        chat_id="agenda-after-briefing",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for today's field trip.",
        event_at_utc=now + timedelta(hours=3),
        now_utc=now,
    )
    briefing = service.daily_briefing_messages(now_utc=now + timedelta(minutes=1))

    outbound = service.handle_incoming(
        _incoming("what's on deck today?", chat_id="agenda-after-briefing", message_id="agenda"),
        now_utc=now + timedelta(minutes=2),
    )

    assert "Permission slip due" in briefing[0].text
    assert "Permission slip due" in outbound[0].text


def test_tomorrow_prep_uses_next_local_day_without_source_body(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("remind us today at 5pm to pick up cleats", chat_id="prep-tomorrow", message_id="today"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "remind us tomorrow at 8am to pack lunch",
            chat_id="prep-tomorrow",
            message_id="tomorrow",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "remind us day after tomorrow at 8am to bring library books",
            chat_id="prep-tomorrow",
            message_id="day-after",
        ),
        now_utc=now,
    )
    service.ingest_source_item(
        chat_id="prep-tomorrow",
        source_type="email",
        title="Field trip permission slip due",
        body="Please sign and bring the permission slip. Full email body stays out.",
        event_at_utc=datetime(2026, 6, 6, 18, 0, tzinfo=timezone.utc),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming("what should we prep for tomorrow?", chat_id="prep-tomorrow", message_id="prep"),
        now_utc=now,
    )

    assert "Tomorrow prep:" in outbound[0].text
    assert "pack lunch" in outbound[0].text
    assert "Field trip permission slip due" in outbound[0].text
    assert "pick up cleats" not in outbound[0].text
    assert "library books" not in outbound[0].text
    assert "Full email body" not in outbound[0].text


def test_tomorrow_prep_is_clear_when_empty(tmp_path):
    service, _agent = _service(tmp_path)

    outbound = service.handle_incoming(
        _incoming("tomorrow prep", chat_id="prep-empty", message_id="prep-empty"),
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )

    assert outbound[0].text == "I do not see anything specific to prep for tomorrow."


def test_duplicate_inbound_message_is_ignored_without_side_effects(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    incoming = _incoming(
        "remind us tomorrow at 8am to pack the permission slip",
        chat_id="duplicate-inbound",
        message_id="duplicate-message",
    )

    first = service.handle_incoming(incoming, now_utc=now)
    second = service.handle_incoming(incoming, now_utc=now + timedelta(minutes=1))
    household = service.store.get_household_by_chat("duplicate-inbound")
    assert household is not None
    reminders = service.store.upcoming_reminders(household_id=household.id, now_utc=now)

    assert len(first) == 1
    assert second == []
    assert len(reminders) == 1
    assert agent.calls == []


def test_duplicate_inbound_agent_turn_is_not_reprocessed(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    incoming = _incoming(
        "What should we do next?",
        chat_id="duplicate-agent",
        message_id="duplicate-agent-message",
    )

    first = service.handle_incoming(incoming, now_utc=now)
    second = service.handle_incoming(incoming, now_utc=now + timedelta(minutes=1))

    assert [message.text for message in first] == ["Fake agent reply."]
    assert second == []
    assert len(agent.calls) == 1


def test_first_greeting_is_natural_without_setup_command_or_hermes(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("hi!", chat_id="natural-hi", message_id="natural-hi"),
        now_utc=now,
    )
    household = service.store.get_household_by_chat("natural-hi")

    assert len(outbound) == 1
    assert "I'm Florence" in outbound[0].text
    assert "You can just text me normally" in outbound[0].text
    assert "What should I call you?" in outbound[0].text
    assert "setup status" not in outbound[0].text.lower()
    assert agent.calls == []
    assert household is not None
    assert service.store.list_members(household.id)[0].role.value == "parent"


def test_ready_household_greeting_still_uses_agent_path(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("my name is Sam", chat_id="existing-hi", message_id="parent-name"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "confirm partner +15555550101",
            chat_id="existing-hi",
            message_id="partner",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id="existing-hi",
            message_id="partner-name",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("our child is Maya", chat_id="existing-hi", message_id="child"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "always tell me about permission slips",
            chat_id="existing-hi",
            message_id="source-rule",
        ),
        now_utc=now,
    )
    service.sync_connected_sources(
        chat_id="existing-hi",
        provider="google",
        external_account_id="parent@example.com",
        account_label="Parent Gmail",
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming("hello", chat_id="existing-hi", message_id="second-hi"),
        now_utc=now + timedelta(minutes=1),
    )

    assert outbound[0].text == "Fake agent reply."
    assert len(agent.calls) == 1
    assert agent.calls[0]["user_text"] == "hello"


def test_bare_name_reply_after_first_greeting_updates_member_name(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("hi", chat_id="bare-name", message_id="bare-name-hi"),
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming("Hari", chat_id="bare-name", message_id="bare-name-reply"),
        now_utc=now + timedelta(minutes=1),
    )
    household = service.store.get_household_by_chat("bare-name")
    assert household is not None
    members = service.store.list_members(household.id)

    assert "Nice to meet you, Hari. I will use that in this household." in outbound[0].text
    assert "Send your partner's phone number" in outbound[0].text
    assert members[0].display_name == "Hari"
    assert agent.calls == []


def test_bare_name_reply_includes_web_onboarding_link_when_configured(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        web_base_url="https://florence.example.com",
        onboarding_state_secret="setup-secret",
    )
    agent = FakeAgent()
    service = FlorenceService(settings=settings, agent=agent)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("hi", chat_id="bare-name-web-link", message_id="bare-name-web-link-hi"),
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming("Hari", chat_id="bare-name-web-link", message_id="bare-name-web-link-reply"),
        now_utc=now + timedelta(minutes=1),
    )

    assert "https://florence.example.com/onboarding/" in outbound[0].text
    assert "partner, kids, location, caretakers, tone, and Google" in outbound[0].text
    assert agent.calls == []


def test_setup_status_includes_web_onboarding_link_when_configured(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        web_base_url="https://florence.example.com",
        onboarding_state_secret="setup-secret",
    )
    agent = FakeAgent()
    service = FlorenceService(settings=settings, agent=agent)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("my name is Hari", chat_id="setup-web-link", message_id="setup-web-link-name"),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming("setup status", chat_id="setup-web-link", message_id="setup-web-link-status"),
        now_utc=now + timedelta(minutes=1),
    )

    assert "Household setup:" in outbound[0].text
    assert "Setup link: https://florence.example.com/onboarding/" in outbound[0].text


def test_existing_incomplete_household_greeting_resumes_onboarding_naturally(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        web_base_url="https://florence.example.com",
        onboarding_state_secret="setup-secret",
    )
    agent = FakeAgent()
    service = FlorenceService(settings=settings, agent=agent)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("my name is Hari", chat_id="natural-onboarding", message_id="natural-name"),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming("hi", chat_id="natural-onboarding", message_id="natural-hi-again"),
        now_utc=now + timedelta(minutes=1),
    )

    assert "Hi Hari. I can keep going from here." in outbound[0].text
    assert "https://florence.example.com/onboarding/" in outbound[0].text
    assert "setup status" not in outbound[0].text.lower()
    assert agent.calls == []


def test_existing_incomplete_household_low_content_reply_resumes_onboarding_naturally(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        web_base_url="https://florence.example.com",
        onboarding_state_secret="setup-secret",
    )
    agent = FakeAgent()
    service = FlorenceService(settings=settings, agent=agent)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("my name is Hari", chat_id="natural-onboarding-ok", message_id="natural-name"),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming("sounds good", chat_id="natural-onboarding-ok", message_id="natural-sounds-good"),
        now_utc=now + timedelta(minutes=1),
    )

    assert "I can keep going from here" in outbound[0].text
    assert "https://florence.example.com/onboarding/" in outbound[0].text
    assert "setup status" not in outbound[0].text.lower()
    assert agent.calls == []


def test_existing_incomplete_household_link_request_resumes_onboarding_naturally(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        web_base_url="https://florence.example.com",
        onboarding_state_secret="setup-secret",
    )
    agent = FakeAgent()
    service = FlorenceService(settings=settings, agent=agent)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("my name is Hari", chat_id="natural-onboarding-link", message_id="natural-name"),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming("send me the link", chat_id="natural-onboarding-link", message_id="natural-link"),
        now_utc=now + timedelta(minutes=1),
    )

    assert "I can keep going from here" in outbound[0].text
    assert "https://florence.example.com/onboarding/" in outbound[0].text
    assert "setup status" not in outbound[0].text.lower()
    assert agent.calls == []


def test_incomplete_household_real_task_is_not_swallowed_by_onboarding(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        web_base_url="https://florence.example.com",
        onboarding_state_secret="setup-secret",
    )
    agent = FakeAgent()
    service = FlorenceService(settings=settings, agent=agent)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("my name is Hari", chat_id="natural-onboarding-task", message_id="natural-name"),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "remind us tomorrow at 8am to pack lunch",
            chat_id="natural-onboarding-task",
            message_id="natural-reminder",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert "Done. I will remind you about pack lunch" in outbound[0].text
    assert "onboarding" not in outbound[0].text.lower()
    assert agent.calls == []


def test_incomplete_household_natural_child_fact_is_not_swallowed_by_onboarding(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        web_base_url="https://florence.example.com",
        onboarding_state_secret="setup-secret",
    )
    agent = FakeAgent()
    service = FlorenceService(settings=settings, agent=agent)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("my name is Hari", chat_id="natural-onboarding-kids", message_id="natural-name"),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming("we have two kids, Maya and Leo", chat_id="natural-onboarding-kids", message_id="natural-kids"),
        now_utc=now + timedelta(minutes=1),
    )
    memory = service.memory_snapshot(chat_id="natural-onboarding-kids", now_utc=now)

    assert "Maya and Leo" in outbound[0].text
    assert "onboarding" not in outbound[0].text.lower()
    assert {item.subject for item in memory.memories} == {"Maya", "Leo"}
    assert agent.calls == []


def test_bare_name_reply_only_applies_after_name_prompt(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("Hari", chat_id="bare-name-no-prompt", message_id="bare-name-no-prompt"),
        now_utc=now,
    )
    household = service.store.get_household_by_chat("bare-name-no-prompt")
    assert household is not None
    members = service.store.list_members(household.id)

    assert outbound[0].text == "Fake agent reply."
    assert members[0].display_name is None
    assert len(agent.calls) == 1


def test_prompted_partner_phone_starts_invite_flow(tmp_path):
    service, agent = _linq_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("hi", chat_id="prompted-phone", message_id="prompted-phone-hi"),
        now_utc=now,
    )
    name_reply = service.handle_incoming(
        _incoming("Hari", chat_id="prompted-phone", message_id="prompted-phone-name"),
        now_utc=now + timedelta(minutes=1),
    )
    outbound = service.handle_incoming(
        _incoming(
            "+1 (555) 555-0101",
            chat_id="prompted-phone",
            message_id="prompted-phone-partner",
        ),
        now_utc=now + timedelta(minutes=2),
    )

    assert "Send your partner's phone number" in name_reply[0].text
    assert len(outbound) == 2
    assert "start a shared household thread with +15555550101" in outbound[0].text
    assert outbound[1].new_chat_to == ("+15555550100", "+15555550101")
    assert agent.calls == []


def test_natural_partner_phone_starts_invite_flow(tmp_path):
    service, agent = _linq_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("my name is Hari", chat_id="natural-partner-phone", message_id="parent-name"),
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming(
            "my partner is +1 (555) 555-0101",
            chat_id="natural-partner-phone",
            message_id="partner-phone",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert len(outbound) == 2
    assert "start a shared household thread with +15555550101" in outbound[0].text
    assert outbound[1].new_chat_to == ("+15555550100", "+15555550101")
    assert agent.calls == []


def test_support_contact_is_reachable_without_hermes(tmp_path):
    service, agent = _support_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("talk to a human", chat_id="support-chat", message_id="support"),
        now_utc=now,
    )

    assert "support@example.com" in outbound[0].text
    assert "emergency contacts" in outbound[0].text
    assert agent.calls == []


def test_support_contact_has_clear_unconfigured_fallback(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("support", chat_id="support-empty", message_id="support-empty"),
        now_utc=now,
    )

    assert "support is not configured" in outbound[0].text
    assert "emergency contacts" in outbound[0].text
    assert agent.calls == []


def test_topic_help_is_deterministic_and_does_not_call_hermes(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    sources = service.handle_incoming(
        _incoming("help sources", chat_id="help-topic", message_id="help-sources"),
        now_utc=now,
    )
    memory = service.handle_incoming(
        _incoming("memory help", chat_id="help-topic", message_id="help-memory"),
        now_utc=now + timedelta(minutes=1),
    )
    reminders = service.handle_incoming(
        _incoming("help reminders", chat_id="help-topic", message_id="help-reminders"),
        now_utc=now + timedelta(minutes=2),
    )

    assert "Source help:" in sources[0].text
    assert "source review" in sources[0].text
    assert "Memory help:" in memory[0].text
    assert "what do you remember" in memory[0].text
    assert "Reminder help:" in reminders[0].text
    assert "remind Alex" in reminders[0].text
    assert agent.calls == []


def test_support_request_takes_precedence_over_help_topic(tmp_path):
    service, agent = _support_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("help me contact support", chat_id="help-support", message_id="help-support"),
        now_utc=now,
    )

    assert "support@example.com" in outbound[0].text
    assert "Support help:" not in outbound[0].text
    assert agent.calls == []


def test_parent_can_confirm_household_data_deletion(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "delete-household"

    service.handle_incoming(
        _incoming("my name is Sam", chat_id=chat_id, message_id="delete-name"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "remember that Maya likes pasta",
            chat_id=chat_id,
            message_id="delete-memory",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    service.handle_incoming(
        _incoming(
            "always tell me about permission slips",
            chat_id=chat_id,
            message_id="delete-source-rule",
        ),
        now_utc=now + timedelta(minutes=2),
    )
    service.sync_connected_sources(
        chat_id=chat_id,
        provider="google",
        external_account_id="parent@example.com",
        emails=[
            {
                "external_id": "delete-email",
                "subject": "Weekly newsletter",
                "body": "Weekly recap and spirit wear sale.",
                "sender": "school@example.com",
                "received_at_utc": now.isoformat(),
            },
        ],
        now_utc=now + timedelta(minutes=3),
    )
    service.create_pending_action(
        chat_id=chat_id,
        action_type="send_message",
        summary="Send a note",
        payload={"text": "hello"},
        now_utc=now + timedelta(minutes=4),
    )
    household = service.store.get_household_by_chat(chat_id)
    assert household is not None
    account = service.store.list_connected_accounts(household.id)[0]
    service.store.upsert_connected_account_token(
        connected_account_id=account.id,
        provider="google",
        token_ciphertext="encrypted-token-payload",
        scopes=("email", "calendar"),
        expires_at_utc=now + timedelta(hours=1),
        now_utc=now + timedelta(minutes=4),
    )
    service.store.create_oauth_state(
        state="delete-oauth-state",
        provider="google",
        chat_id=chat_id,
        expires_at_utc=now + timedelta(minutes=30),
        now_utc=now + timedelta(minutes=4),
        account_label="Parent Gmail",
    )
    service.store.record_outbound_deliveries_for_source(
        household_id=household.id,
        source_message_id="delete-source-message",
        messages=[
            OutboundMessage(
                chat_id=chat_id,
                text="Delivery record to delete",
                idempotency_key="out:delete-delivery",
            )
        ],
        now_utc=now + timedelta(minutes=4),
    )
    source_item_id = None
    with service.store.connect() as conn:
        source_item = conn.execute(
            "SELECT id FROM source_items WHERE household_id = ?",
            (household.id,),
        ).fetchone()
        assert source_item is not None
        source_item_id = source_item["id"]
    service.store.record_source_feedback(
        household_id=household.id,
        source_item_id=source_item_id,
        feedback=SourceFeedbackKind.NOT_USEFUL,
        phrase="school@example.com",
        created_by_member_id=None,
        created_at_utc=now + timedelta(minutes=4),
    )
    action = service.pending_actions(chat_id=chat_id, now_utc=now + timedelta(minutes=4))[0]
    service.store.record_action_execution(
        action=action,
        status=ActionExecutionStatus.FAILED,
        attempted_at_utc=now + timedelta(minutes=4),
        error="test execution row to delete",
    )

    prompt = service.handle_incoming(
        _incoming("delete my data", chat_id=chat_id, message_id="delete-request"),
        now_utc=now + timedelta(minutes=5),
    )

    confirmed = service.handle_incoming(
        _incoming(
            "confirm delete household data",
            chat_id=chat_id,
            message_id="delete-confirm",
        ),
        now_utc=now + timedelta(minutes=6),
    )

    assert "confirm delete household data" in prompt[0].text
    assert "deleted this household" in confirmed[0].text
    assert service.store.get_household_by_chat(chat_id) is None
    with service.store.connect() as conn:
        for table_name in (
            "messages",
            "reminders",
            "source_items",
            "connected_accounts",
            "outbound_deliveries",
            "source_preferences",
            "source_feedback",
            "memories",
            "pending_actions",
            "action_executions",
            "routine_runs",
            "household_members",
            "household_chat_aliases",
        ):
            count = conn.execute(
                f"SELECT COUNT(*) AS count FROM {table_name} WHERE household_id = ?",
                (household.id,),
            ).fetchone()["count"]
            assert count == 0, table_name
        token_count = conn.execute(
            "SELECT COUNT(*) AS count FROM connected_account_tokens"
        ).fetchone()["count"]
        oauth_state_count = conn.execute(
            "SELECT COUNT(*) AS count FROM oauth_states WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()["count"]
        assert token_count == 0
        assert oauth_state_count == 0
    assert agent.calls == []


def test_parent_cannot_confirm_household_data_deletion_without_recent_request(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "delete-without-request"

    service.handle_incoming(
        _incoming("my name is Sam", chat_id=chat_id, message_id="delete-no-request-name"),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "confirm delete household data",
            chat_id=chat_id,
            message_id="delete-no-request-confirm",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert "Text 'delete my data' first" in outbound[0].text
    assert service.store.get_household_by_chat(chat_id) is not None
    assert agent.calls == []


def test_service_read_exports_do_not_create_missing_household(tmp_path):
    service, _ = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "missing-read-export-chat"

    operations = (
        lambda: service.connected_accounts(chat_id=chat_id),
        lambda: service.source_review_snapshot(chat_id=chat_id, now_utc=now),
        lambda: service.source_preferences(chat_id=chat_id),
        lambda: service.memory_snapshot(chat_id=chat_id, now_utc=now),
        lambda: service.privacy_snapshot(chat_id=chat_id, now_utc=now),
        lambda: service.readiness_snapshot(chat_id=chat_id, now_utc=now),
        lambda: service.pending_actions(chat_id=chat_id, now_utc=now),
        lambda: service.action_executions(chat_id=chat_id),
        lambda: service.delete_memory(chat_id=chat_id, memory_id="memory-1", now_utc=now),
    )

    for operation in operations:
        with pytest.raises(ValueError, match="household_not_found"):
            operation()

    assert service.store.get_household_by_chat(chat_id) is None


def test_household_data_deletion_request_spacing_is_normalized(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "delete-spaced-request"

    service.handle_incoming(
        _incoming("my name is Sam", chat_id=chat_id, message_id="delete-spaced-name"),
        now_utc=now,
    )
    prompt = service.handle_incoming(
        _incoming("  delete    my   data  ", chat_id=chat_id, message_id="delete-spaced-request"),
        now_utc=now + timedelta(minutes=1),
    )

    confirmed = service.handle_incoming(
        _incoming(
            "confirm delete household data",
            chat_id=chat_id,
            message_id="delete-spaced-confirm",
        ),
        now_utc=now + timedelta(minutes=2),
    )

    assert "confirm delete household data" in prompt[0].text
    assert "deleted this household" in confirmed[0].text
    assert service.store.get_household_by_chat(chat_id) is None
    assert agent.calls == []


def test_household_data_deletion_confirmation_expires(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        data_deletion_confirmation_ttl_minutes=5,
    )
    agent = FakeAgent()
    service = FlorenceService(settings=settings, agent=agent)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "delete-expired-request"

    service.handle_incoming(
        _incoming("my name is Sam", chat_id=chat_id, message_id="delete-expired-name"),
        now_utc=now,
    )
    prompt = service.handle_incoming(
        _incoming("delete my data", chat_id=chat_id, message_id="delete-expired-request"),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "confirm delete household data",
            chat_id=chat_id,
            message_id="delete-expired-confirm",
        ),
        now_utc=now + timedelta(minutes=6),
    )

    assert "confirm delete household data" in prompt[0].text
    assert "Text 'delete my data' first" in outbound[0].text
    assert service.store.get_household_by_chat(chat_id) is not None
    assert agent.calls == []


def test_helper_cannot_delete_household_data(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "delete-helper"

    service.handle_incoming(
        _incoming("my name is Sam", chat_id=chat_id, message_id="delete-parent"),
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming(
            "confirm delete household data",
            chat_id=chat_id,
            message_id="delete-helper-confirm",
            sender="+15555550101",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert "one of the parents" in outbound[0].text
    assert service.store.get_household_by_chat(chat_id) is not None


def test_parent_can_view_household_data_summary_without_raw_content(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "data-summary"

    service.handle_incoming(
        _incoming("my name is Sam", chat_id=chat_id, message_id="summary-name"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "remember that Maya likes pasta",
            chat_id=chat_id,
            message_id="summary-memory",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    service.handle_incoming(
        _incoming(
            "always tell me about permission slips",
            chat_id=chat_id,
            message_id="summary-source-rule",
        ),
        now_utc=now + timedelta(minutes=2),
    )
    service.sync_connected_sources(
        chat_id=chat_id,
        provider="google",
        external_account_id="parent@example.com",
        emails=[
            {
                "external_id": "summary-email",
                "subject": "Weekly newsletter",
                "body": "Weekly recap and spirit wear sale.",
                "sender": "school@example.com",
                "received_at_utc": now.isoformat(),
            },
        ],
        now_utc=now + timedelta(minutes=3),
    )
    service.create_pending_action(
        chat_id=chat_id,
        action_type="send_message",
        summary="Send a note",
        payload={"text": "hello"},
        now_utc=now + timedelta(minutes=4),
    )
    service.handle_incoming(
        _incoming("stop", chat_id=chat_id, message_id="summary-stop"),
        now_utc=now + timedelta(minutes=5),
    )

    outbound = service.handle_incoming(
        _incoming("data summary", chat_id=chat_id, message_id="summary-command"),
        now_utc=now + timedelta(minutes=6),
    )

    text = outbound[0].text
    assert "without raw message or email bodies" in text
    assert "1 parents, 0 helpers" in text
    assert "Connected sources: 1 accounts, 1 items" in text
    assert "Source rules: 1" in text
    assert "Durable memories: 1" in text
    assert "Pending approvals: 1" in text
    assert "Florence status: paused" in text
    assert "Maya likes pasta" not in text
    assert "Weekly recap" not in text
    assert agent.calls == []


def test_helper_cannot_view_household_data_summary(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "data-summary-helper"

    service.handle_incoming(
        _incoming("my name is Sam", chat_id=chat_id, message_id="summary-parent"),
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming(
            "data summary",
            chat_id=chat_id,
            message_id="summary-helper",
            sender="+15555550101",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert "one of the parents" in outbound[0].text


def test_helper_cannot_stop_household_thread(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "stop-helper"

    service.handle_incoming(
        _incoming("hello", chat_id=chat_id, message_id="parent-seen"),
        now_utc=now,
    )
    helper_stop = service.handle_incoming(
        _incoming(
            "stop",
            chat_id=chat_id,
            message_id="helper-stop",
            sender="+15555550101",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    parent_ask = service.handle_incoming(
        _incoming("what should we do next?", chat_id=chat_id, message_id="parent-ask"),
        now_utc=now + timedelta(minutes=2),
    )

    assert "need one of the parents" in helper_stop[0].text
    assert parent_ask[0].text == "Fake agent reply."
    assert len(agent.calls) == 1


def test_parent_stop_suppresses_thread_until_parent_restarts(tmp_path):
    service, agent = _support_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "stop-parent"

    stopped = service.handle_incoming(
        _incoming("stop", chat_id=chat_id, message_id="stop"),
        now_utc=now,
    )
    suppressed = service.handle_incoming(
        _incoming("what should we do next?", chat_id=chat_id, message_id="suppressed"),
        now_utc=now + timedelta(minutes=1),
    )
    help_reply = service.handle_incoming(
        _incoming("help", chat_id=chat_id, message_id="help"),
        now_utc=now + timedelta(minutes=2),
    )
    support_reply = service.handle_incoming(
        _incoming("support", chat_id=chat_id, message_id="support"),
        now_utc=now + timedelta(minutes=3),
    )
    privacy_help = service.handle_incoming(
        _incoming("help privacy", chat_id=chat_id, message_id="privacy-help"),
        now_utc=now + timedelta(minutes=4),
    )
    resumed = service.handle_incoming(
        _incoming("start", chat_id=chat_id, message_id="start"),
        now_utc=now + timedelta(minutes=5),
    )
    after_resume = service.handle_incoming(
        _incoming("what should we do next?", chat_id=chat_id, message_id="after-resume"),
        now_utc=now + timedelta(minutes=6),
    )

    assert "stop replying" in stopped[0].text
    assert suppressed == []
    assert "Text me things like" in help_reply[0].text
    assert "support@example.com" in support_reply[0].text
    assert "Privacy help:" in privacy_help[0].text
    assert "back on" in resumed[0].text
    assert after_resume[0].text == "Fake agent reply."
    assert len(agent.calls) == 1


def test_helper_cannot_restart_stopped_household(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "stop-helper-restart"

    service.handle_incoming(
        _incoming("stop", chat_id=chat_id, message_id="stop"),
        now_utc=now,
    )
    helper_start = service.handle_incoming(
        _incoming(
            "start",
            chat_id=chat_id,
            message_id="helper-start",
            sender="+15555550101",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    suppressed = service.handle_incoming(
        _incoming("what should we do next?", chat_id=chat_id, message_id="still-stopped"),
        now_utc=now + timedelta(minutes=2),
    )

    assert "need one of the parents" in helper_start[0].text
    assert suppressed == []
    assert agent.calls == []


def test_worker_pauses_due_reminders_while_household_stopped(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        daily_briefing_hour=23,
    )
    service = FlorenceService(settings=settings, agent=FakeAgent())
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    due_at = datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc)
    chat_id = "stop-worker-reminder"

    service.handle_incoming(
        _incoming(
            "remind us tomorrow at 8am to pack lunch",
            chat_id=chat_id,
            message_id="create",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("stop", chat_id=chat_id, message_id="stop"),
        now_utc=now + timedelta(minutes=1),
    )
    sender = FakeSender()

    stopped_tick = run_routine_tick(service, sender, now_utc=due_at)

    assert stopped_tick.sent == 0
    assert stopped_tick.reminder_messages == 0
    assert sender.sent == []

    service.handle_incoming(
        _incoming("start", chat_id=chat_id, message_id="start"),
        now_utc=due_at + timedelta(minutes=1),
    )
    resumed_tick = run_routine_tick(service, sender, now_utc=due_at + timedelta(minutes=2))

    assert resumed_tick.sent == 1
    assert resumed_tick.reminder_messages == 1
    assert len(sender.sent) == 1
    assert sender.sent[0]["chat_id"] == chat_id
    assert sender.sent[0]["text"] == "Quick reminder: pack lunch"
    assert sender.sent[0]["idempotency_key"].startswith("reminder:")


def test_worker_expires_due_reminder_after_stopped_past_grace(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        daily_briefing_hour=23,
        reminder_delivery_grace_minutes=60,
    )
    service = FlorenceService(settings=settings, agent=FakeAgent())
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    due_at = datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc)
    chat_id = "stop-worker-reminder-stale"

    service.handle_incoming(
        _incoming(
            "remind us tomorrow at 8am to pack lunch",
            chat_id=chat_id,
            message_id="create",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("stop", chat_id=chat_id, message_id="stop"),
        now_utc=now + timedelta(minutes=1),
    )
    sender = FakeSender()

    stopped_tick = run_routine_tick(service, sender, now_utc=due_at + timedelta(minutes=30))

    assert stopped_tick.sent == 0
    assert stopped_tick.reminder_messages == 0

    service.handle_incoming(
        _incoming("start", chat_id=chat_id, message_id="start"),
        now_utc=due_at + timedelta(minutes=61),
    )
    resumed_tick = run_routine_tick(service, sender, now_utc=due_at + timedelta(minutes=62))

    assert resumed_tick.sent == 0
    assert resumed_tick.reminder_messages == 0
    assert sender.sent == []
    assert service.store.due_reminders(now_utc=due_at + timedelta(minutes=62)) == []


def test_stale_reminder_is_expired_not_sent(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        reminder_delivery_grace_minutes=60,
    )
    service = FlorenceService(settings=settings, agent=FakeAgent())
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household = service.store.get_or_create_household(
        chat_id="chat-stale",
        timezone_name="America/Los_Angeles",
        now_utc=now,
    )
    service.store.create_reminder(
        household_id=household.id,
        chat_id=household.chat_id,
        title="old permission slip",
        due_at_utc=now - timedelta(days=14),
        created_at_utc=now - timedelta(days=15),
    )

    due = service.due_reminder_messages(now_utc=now)

    assert due == []
    assert service.store.due_reminders(now_utc=now) == []


def test_source_junk_is_not_surfaced(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.ingest_source_item(
        chat_id="chat-1",
        source_type="email",
        title="Weekly school newsletter",
        body="Weekly recap and spirit wear sale.",
        event_at_utc=now + timedelta(hours=12),
        now_utc=now,
    )

    assert outbound == []


def test_source_policy_only_scans_bounded_source_summary(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    quiet_prefix = " ".join(["general family newsletter"] * 220)
    body = f"{quiet_prefix} permission slip due bring pack field trip"

    outbound = service.ingest_source_item(
        chat_id="source-large-summary",
        source_type="email",
        title="Weekly newsletter",
        body=body,
        event_at_utc=now + timedelta(hours=12),
        now_utc=now,
    )
    household = service.store.get_household_by_chat("source-large-summary")
    assert household is not None
    with service.store.connect() as conn:
        row = conn.execute(
            "SELECT body, decision, reason FROM source_items WHERE household_id = ?",
            (household.id,),
        ).fetchone()

    assert outbound == []
    assert row["decision"] == "store_only"
    assert row["reason"] == "low_signal_source"
    assert len(row["body"]) == MAX_SOURCE_BODY_CHARS
    assert "permission slip" not in row["body"]


def test_high_signal_source_without_time_surfaces_without_reminder_action(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.ingest_source_item(
        chat_id="source-no-time-school-change",
        source_type="email",
        title="No school tomorrow",
        body="Campus will be closed for a teacher work day.",
        event_at_utc=None,
        now_utc=now,
    )

    assert len(outbound) == 1
    assert "No school tomorrow" in outbound[0].text
    assert "add a reminder" not in outbound[0].text
    assert service.pending_actions(chat_id="source-no-time-school-change", now_utc=now) == []


def test_media_only_message_is_acknowledged_and_stored_as_source(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        IncomingMessage(
            chat_id="media-only",
            message_id="media-message",
            sender="+15555550100",
            text="",
            received_at=now,
            attachments=(
                MessageAttachment(
                    kind="image",
                    content_type="image/png",
                    filename="school-flyer.png",
                    url="https://example.com/private/flyer.png",
                ),
            ),
        ),
        now_utc=now,
    )
    snapshot = service.source_review_snapshot(chat_id="media-only")

    assert len(outbound) == 1
    assert "I got the attachment" in outbound[0].text
    assert "permission slip due Friday" in outbound[0].text
    assert snapshot.total == 1
    assert snapshot.stored_only == 1
    assert agent.calls == []
    household = service.store.get_household_by_chat("media-only")
    assert household is not None
    with service.store.connect() as conn:
        row = conn.execute(
            "SELECT title, body, source_type, external_id FROM source_items WHERE household_id = ?",
            (household.id,),
        ).fetchone()
        message = conn.execute(
            "SELECT body FROM messages WHERE household_id = ? AND id = ?",
            (household.id, "media-message"),
        ).fetchone()

    assert row["title"] == "school-flyer.png"
    assert row["source_type"] == "flyer"
    assert row["external_id"] == "linq:media-message:attachment:1"
    assert "https://example.com" not in row["body"]
    assert message["body"] == "Shared 1 attachment: school-flyer.png"


def test_captioned_attachment_uses_need_to_know_policy(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        IncomingMessage(
            chat_id="captioned-media",
            message_id="captioned-media-message",
            sender="+15555550100",
            text="Field trip permission slip due tomorrow",
            received_at=now,
            attachments=(MessageAttachment(kind="image", content_type="image/jpeg"),),
        ),
        now_utc=now,
    )
    snapshot = service.source_review_snapshot(chat_id="captioned-media")

    assert len(outbound) == 1
    assert "This looks worth your attention: Field trip permission slip due tomorrow" in outbound[0].text
    assert snapshot.total == 1
    assert snapshot.surfaced == 1
    assert agent.calls == []


def test_extracted_attachment_text_surfaces_and_suggests_reminder(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        IncomingMessage(
            chat_id="extracted-media",
            message_id="extracted-media-message",
            sender="+15555550100",
            text="",
            received_at=now,
            attachments=(
                MessageAttachment(
                    kind="image",
                    content_type="image/png",
                    filename="field-trip.png",
                    url="https://example.com/private/field-trip.png",
                    extracted_text="Field trip permission slip due tomorrow at 5pm. Bring $5.",
                ),
            ),
        ),
        now_utc=now,
    )
    snapshot = service.source_review_snapshot(chat_id="extracted-media", now_utc=now)
    actions = service.pending_actions(chat_id="extracted-media", now_utc=now)

    assert len(outbound) == 1
    assert "This looks worth your attention: Field trip permission slip due tomorrow at 5pm." in outbound[0].text
    assert "Sat, Jun 6 at 5:00 PM" in outbound[0].text
    assert "approve" in outbound[0].text
    assert snapshot.total == 1
    assert snapshot.surfaced == 1
    assert len(actions) == 1
    assert actions[0].payload["title"] == "Field trip permission slip due tomorrow at 5pm."
    assert agent.calls == []

    household = service.store.get_household_by_chat("extracted-media")
    assert household is not None
    with service.store.connect() as conn:
        row = conn.execute(
            "SELECT body, event_at_utc FROM source_items WHERE household_id = ?",
            (household.id,),
        ).fetchone()
    assert "Extracted text: Field trip permission slip due tomorrow at 5pm. Bring $5." in row["body"]
    assert "https://example.com" not in row["body"]
    assert row["event_at_utc"] == "2026-06-07T00:00:00+00:00"


def test_email_import_dedupes_and_keeps_newsletter_quiet(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    first = service.ingest_email(
        chat_id="email-chat",
        subject="Weekly school newsletter",
        body="Weekly recap and spirit wear sale.",
        sender="school@example.com",
        received_at_utc=now,
        external_id="email-1",
        event_at_utc=now + timedelta(hours=12),
        now_utc=now,
    )
    second = service.ingest_email(
        chat_id="email-chat",
        subject="Weekly school newsletter",
        body="Weekly recap and spirit wear sale.",
        sender="school@example.com",
        received_at_utc=now,
        external_id="email-1",
        event_at_utc=now + timedelta(hours=12),
        now_utc=now,
    )
    snapshot = service.source_review_snapshot(chat_id="email-chat", now_utc=now)

    assert first == []
    assert second == []
    assert snapshot.total == 1
    assert snapshot.stored_only == 1


def test_actionable_source_is_surfaced(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.ingest_source_item(
        chat_id="chat-1",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        event_at_utc=now + timedelta(hours=8),
        now_utc=now,
    )

    assert len(outbound) == 1
    assert "worth your attention" in outbound[0].text
    assert "add a reminder" in outbound[0].text


def test_actionable_source_can_create_approved_reminder(tmp_path):
    service, _agent = _service(tmp_path)
    sender = FakeSender()
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    due_at = now + timedelta(hours=8)

    service.ingest_source_item(
        chat_id="source-reminder",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        event_at_utc=due_at,
        now_utc=now,
    )
    action = service.pending_actions(chat_id="source-reminder", now_utc=now)[0]
    approval = service.handle_incoming(
        _incoming(
            f"approve {action.id[:8]}",
            chat_id="source-reminder",
            message_id="source-reminder-approve",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    run = run_approved_actions(
        store=service.store,
        sender=sender,
        now_utc=now + timedelta(minutes=2),
    )
    household = service.store.get_household_by_chat("source-reminder")
    assert household is not None
    upcoming = service.store.upcoming_reminders(
        household_id=household.id,
        now_utc=now + timedelta(minutes=2),
    )

    assert action.action_type == "create_reminder"
    assert "Approved: Add reminder: Permission slip due" in approval[0].text
    assert run.succeeded == 1
    assert sender.sent == []
    assert upcoming[0].title == "Permission slip due"
    assert upcoming[0].due_at_utc == due_at


def test_source_reminder_action_expires_at_due_time(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    due_at = now + timedelta(minutes=30)

    service.ingest_source_item(
        chat_id="source-reminder-late-approval",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        event_at_utc=due_at,
        now_utc=now,
    )
    action = service.pending_actions(chat_id="source-reminder-late-approval", now_utc=now)[0]

    approval = service.handle_incoming(
        _incoming(
            f"approve {action.id[:8]}",
            chat_id="source-reminder-late-approval",
            message_id="late-source-reminder-approve",
        ),
        now_utc=due_at + timedelta(minutes=1),
    )
    household = service.store.get_household_by_chat("source-reminder-late-approval")
    assert household is not None
    resolved = service.store.list_pending_actions(
        household_id=household.id,
        now_utc=due_at + timedelta(minutes=1),
        include_resolved=True,
    )[0]

    assert "could not find an active approval" in approval[0].text
    assert resolved.status == PendingActionStatus.EXPIRED
    assert service.store.upcoming_reminders(
        household_id=household.id,
        now_utc=due_at + timedelta(minutes=1),
    ) == []


def test_duplicate_actionable_source_does_not_duplicate_reminder_action(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    first = service.ingest_source_item(
        chat_id="source-dedupe-action",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        external_id="permission-slip-1",
        event_at_utc=now + timedelta(hours=8),
        now_utc=now,
    )
    second = service.ingest_source_item(
        chat_id="source-dedupe-action",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        external_id="permission-slip-1",
        event_at_utc=now + timedelta(hours=8),
        now_utc=now,
    )

    assert len(first) == 1
    assert second == []
    assert len(service.pending_actions(chat_id="source-dedupe-action", now_utc=now)) == 1


def test_connected_source_sync_tracks_account_cursor_and_counts_imports(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    result = service.sync_connected_sources(
        chat_id="sync-chat",
        provider="google",
        external_account_id="parent@example.com",
        account_label="Parent Gmail",
        cursor="cursor-1",
        now_utc=now,
        emails=[
            {
                "external_id": "email-noise",
                "subject": "Weekly school newsletter",
                "body": "Weekly recap and spirit wear sale.",
                "sender": "school@example.com",
                "received_at_utc": now.isoformat(),
            },
            {
                "external_id": "email-action",
                "subject": "Permission slip due",
                "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                "sender": "teacher@example.com",
                "received_at_utc": now.isoformat(),
                "event_at_utc": (now + timedelta(hours=8)).isoformat(),
            },
        ],
    )
    accounts = service.connected_accounts(chat_id="sync-chat")
    snapshot = service.source_review_snapshot(chat_id="sync-chat", now_utc=now)

    assert result.imported == 2
    assert result.surfaced == 1
    assert result.account.cursor == "cursor-1"
    assert result.account.last_synced_at_utc == now
    assert accounts[0].account_label == "Parent Gmail"
    assert snapshot.total == 2
    assert snapshot.surfaced == 1
    assert snapshot.stored_only == 1


def test_source_review_token_backed_counts_follow_same_active_google_account(tmp_path):
    service, _agent = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.sync_connected_sources(
        chat_id="token-backed-source",
        provider="google",
        external_account_id="parent@example.com",
        account_label="Parent Gmail",
        cursor="cursor-1",
        now_utc=now,
        emails=[
            {
                "external_id": "email-action",
                "subject": "Permission slip due",
                "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                "sender": "teacher@example.com",
                "received_at_utc": now.isoformat(),
                "event_at_utc": (now + timedelta(hours=8)).isoformat(),
            }
        ],
    )
    household = service.store.get_household_by_chat("token-backed-source")
    assert household is not None
    account = service.connected_accounts(chat_id="token-backed-source")[0]
    before_token = service.source_review_snapshot(chat_id="token-backed-source", now_utc=now)

    assert before_token.connected_total == 1
    assert before_token.connected_surfaced == 1
    assert before_token.token_backed_google_total == 0
    assert before_token.token_backed_google_surfaced == 0

    expires_at = now + timedelta(hours=1)
    service.store.upsert_connected_account_token(
        connected_account_id=account.id,
        provider="google",
        token_ciphertext=TokenVault.from_settings(service.settings).encrypt(
            {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "expires_at_utc": expires_at.isoformat(),
            }
        ),
        scopes=("openid", "email"),
        expires_at_utc=expires_at,
        now_utc=now,
    )
    after_token = service.source_review_snapshot(chat_id="token-backed-source", now_utc=now)

    assert after_token.token_backed_google_total == 1
    assert after_token.token_backed_google_surfaced == 1
    assert after_token.latest_token_backed_google_synced_at_utc == now

    service.store.disconnect_connected_accounts(
        household_id=household.id,
        provider="google",
        now_utc=now + timedelta(minutes=1),
    )
    after_disconnect = service.source_review_snapshot(chat_id="token-backed-source", now_utc=now)

    assert after_disconnect.connected_total == 1
    assert after_disconnect.connected_surfaced == 1
    assert after_disconnect.token_backed_google_total == 0
    assert after_disconnect.token_backed_google_surfaced == 0


def test_initial_connected_source_sync_suppresses_non_urgent_backfill(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    result = service.sync_connected_sources(
        chat_id="sync-initial-quiet",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
        emails=[
            {
                "external_id": "email-future-rsvp",
                "subject": "RSVP due for field trip",
                "body": "Please RSVP for next week's field trip.",
                "sender": "teacher@example.com",
                "received_at_utc": now.isoformat(),
                "event_at_utc": (now + timedelta(days=3)).isoformat(),
            },
        ],
    )
    snapshot = service.source_review_snapshot(chat_id="sync-initial-quiet", now_utc=now)

    assert result.imported == 1
    assert result.surfaced == 0
    assert result.messages == []
    assert service.pending_actions(chat_id="sync-initial-quiet", now_utc=now) == []
    assert snapshot.stored_only == 1
    assert snapshot.by_reason["initial_sync_backfill"] == 1


def test_initial_connected_source_sync_surfaces_recent_schedule_change_without_time(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    result = service.sync_connected_sources(
        chat_id="sync-initial-schedule-change",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
        emails=[
            {
                "external_id": "email-no-school",
                "subject": "No school tomorrow",
                "body": "Campus will be closed for a teacher work day.",
                "sender": "school@example.com",
                "received_at_utc": now.isoformat(),
            },
        ],
    )
    snapshot = service.source_review_snapshot(chat_id="sync-initial-schedule-change", now_utc=now)

    assert result.imported == 1
    assert result.surfaced == 1
    assert len(result.messages) == 1
    assert "No school tomorrow" in result.messages[0].text
    assert "add a reminder" not in result.messages[0].text
    assert service.pending_actions(chat_id="sync-initial-schedule-change", now_utc=now) == []
    assert snapshot.surfaced == 1
    assert snapshot.by_reason["high_signal_without_known_due_time"] == 1


def test_initial_backfill_source_appears_once_in_daily_briefing(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    result = service.sync_connected_sources(
        chat_id="sync-initial-briefing",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
        emails=[
            {
                "external_id": "email-future-rsvp",
                "subject": "RSVP due for field trip",
                "body": "Please RSVP for next week's field trip.",
                "sender": "teacher@example.com",
                "received_at_utc": now.isoformat(),
                "event_at_utc": (now + timedelta(days=3)).isoformat(),
            },
        ],
    )

    first_briefing = service.daily_briefing_messages(
        now_utc=datetime(2026, 6, 6, 14, 30, tzinfo=timezone.utc),
    )
    second_briefing = service.daily_briefing_messages(
        now_utc=datetime(2026, 6, 7, 14, 30, tzinfo=timezone.utc),
    )

    assert result.messages == []
    assert "RSVP due for field trip" in first_briefing[0].text
    assert second_briefing == []


def test_later_connected_source_sync_surfaces_non_urgent_actionable_items(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.sync_connected_sources(
        chat_id="sync-later-actionable",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
    )
    result = service.sync_connected_sources(
        chat_id="sync-later-actionable",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-2",
        now_utc=now + timedelta(minutes=5),
        emails=[
            {
                "external_id": "email-future-rsvp",
                "subject": "RSVP due for field trip",
                "body": "Please RSVP for next week's field trip.",
                "sender": "teacher@example.com",
                "received_at_utc": (now + timedelta(minutes=5)).isoformat(),
                "event_at_utc": (now + timedelta(days=3)).isoformat(),
            },
        ],
    )
    snapshot = service.source_review_snapshot(chat_id="sync-later-actionable", now_utc=now)

    assert result.imported == 1
    assert result.surfaced == 1
    assert len(result.messages) == 1
    assert "RSVP due for field trip" in result.messages[0].text
    assert snapshot.surfaced == 1
    assert snapshot.by_reason["upcoming_actionable_source"] == 1


def test_initial_connected_source_sync_suppresses_requested_low_signal_items(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("always tell me about spirit wear", chat_id="sync-initial-preference", message_id="rule"),
        now_utc=now,
    )
    result = service.sync_connected_sources(
        chat_id="sync-initial-preference",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
        emails=[
            {
                "external_id": "email-spirit-wear",
                "subject": "Spirit wear sale",
                "body": "Order spirit wear this week.",
                "sender": "school@example.com",
                "received_at_utc": now.isoformat(),
                "event_at_utc": (now + timedelta(hours=12)).isoformat(),
            },
        ],
    )
    snapshot = service.source_review_snapshot(chat_id="sync-initial-preference", now_utc=now)

    assert result.imported == 1
    assert result.surfaced == 0
    assert snapshot.stored_only == 1
    assert snapshot.by_reason["initial_sync_backfill"] == 1


def test_connected_source_sync_dedupes_and_advances_cursor(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    payload = {
        "external_id": "email-1",
        "subject": "Weekly school newsletter",
        "body": "Weekly recap.",
        "sender": "school@example.com",
        "received_at_utc": now.isoformat(),
    }

    first = service.sync_connected_sources(
        chat_id="sync-dedupe",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
        emails=[payload],
    )
    second = service.sync_connected_sources(
        chat_id="sync-dedupe",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-2",
        now_utc=now + timedelta(minutes=5),
        emails=[payload],
    )
    account = service.connected_accounts(chat_id="sync-dedupe")[0]
    snapshot = service.source_review_snapshot(chat_id="sync-dedupe", now_utc=now)

    assert first.imported == 1
    assert second.imported == 0
    assert second.surfaced == 0
    assert account.cursor == "cursor-2"
    assert account.last_synced_at_utc == now + timedelta(minutes=5)
    assert snapshot.total == 1


def test_connected_source_sync_scopes_external_ids_by_account(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    payload = {
        "external_id": "same-provider-id",
        "subject": "Weekly school newsletter",
        "body": "Weekly recap.",
        "sender": "school@example.com",
        "received_at_utc": now.isoformat(),
    }

    one = service.sync_connected_sources(
        chat_id="sync-two-accounts",
        provider="google",
        external_account_id="one@example.com",
        now_utc=now,
        emails=[payload],
    )
    two = service.sync_connected_sources(
        chat_id="sync-two-accounts",
        provider="google",
        external_account_id="two@example.com",
        now_utc=now,
        emails=[payload],
    )
    accounts = service.connected_accounts(chat_id="sync-two-accounts")
    snapshot = service.source_review_snapshot(chat_id="sync-two-accounts", now_utc=now)

    assert one.imported == 1
    assert two.imported == 1
    assert len(accounts) == 2
    assert snapshot.total == 2


def test_actionable_email_import_surfaces_once(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    first = service.ingest_email(
        chat_id="email-action",
        subject="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        sender="teacher@example.com",
        received_at_utc=now,
        external_id="school-action-1",
        event_at_utc=now + timedelta(hours=8),
        now_utc=now,
    )
    second = service.ingest_email(
        chat_id="email-action",
        subject="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        sender="teacher@example.com",
        received_at_utc=now,
        external_id="school-action-1",
        event_at_utc=now + timedelta(hours=8),
        now_utc=now,
    )
    snapshot = service.source_review_snapshot(chat_id="email-action", now_utc=now)

    assert len(first) == 1
    assert "Permission slip due" in first[0].text
    assert second == []
    assert snapshot.total == 1
    assert snapshot.surfaced == 1
    assert snapshot.by_reason["urgent_actionable_source"] == 1


def test_calendar_import_surfaces_upcoming_parent_relevant_event(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.ingest_calendar_event(
        chat_id="calendar-chat",
        title="Maya dentist appointment",
        starts_at_utc=now + timedelta(hours=4),
        location="Pediatric Dentist",
        calendar_name="Family",
        external_id="calendar-1",
        now_utc=now,
    )

    assert len(outbound) == 1
    assert "Maya dentist appointment" in outbound[0].text


def test_calendar_import_suppresses_past_event(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.ingest_calendar_event(
        chat_id="calendar-past",
        title="Soccer practice",
        starts_at_utc=now - timedelta(days=2),
        calendar_name="Family",
        external_id="calendar-past-1",
        now_utc=now,
    )
    snapshot = service.source_review_snapshot(chat_id="calendar-past", now_utc=now)

    assert outbound == []
    assert snapshot.total == 1
    assert snapshot.suppressed == 1
    assert snapshot.by_reason["event_is_in_the_past"] == 1


def test_parent_can_add_household_calendar_event_from_text(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    created = service.handle_incoming(
        _incoming(
            "add soccer practice tomorrow at 5pm to calendar",
            chat_id="manual-calendar",
            message_id="manual-calendar-add",
        ),
        now_utc=now,
    )
    prep = service.handle_incoming(
        _incoming("tomorrow prep", chat_id="manual-calendar", message_id="manual-calendar-prep"),
        now_utc=now + timedelta(minutes=1),
    )
    household = service.store.get_household_by_chat("manual-calendar")
    assert household is not None
    snapshot = service.source_review_snapshot(chat_id="manual-calendar", now_utc=now)
    with service.store.connect() as conn:
        row = conn.execute(
            "SELECT title, source_type, reason, event_at_utc FROM source_items WHERE household_id = ?",
            (household.id,),
        ).fetchone()

    assert "Done. I added soccer practice to the household calendar" in created[0].text
    assert "Sat, Jun 6 at 5:00 PM" in created[0].text
    assert "soccer practice" in prep[0].text
    assert snapshot.surfaced == 1
    assert row["title"] == "soccer practice"
    assert row["source_type"] == "calendar"
    assert row["reason"] == "manual_calendar_event"
    assert row["event_at_utc"] == "2026-06-07T00:00:00+00:00"
    assert agent.calls == []


def test_calendar_event_command_asks_for_ampm_on_bare_hour(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming(
            "add soccer practice tomorrow at 5 to calendar",
            chat_id="manual-calendar-ambiguous",
            message_id="manual-calendar-ambiguous-add",
        ),
        now_utc=now,
    )
    snapshot = service.source_review_snapshot(chat_id="manual-calendar-ambiguous", now_utc=now)

    assert "Should I use AM or PM for 'at 5'?" in outbound[0].text
    assert snapshot.total == 0
    assert agent.calls == []


def test_helper_cannot_add_household_calendar_event(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("hello", chat_id="manual-calendar-helper", message_id="parent-one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id="manual-calendar-helper",
            message_id="parent-two",
            sender="+15555550101",
        ),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "calendar: soccer practice tomorrow at 5pm",
            chat_id="manual-calendar-helper",
            message_id="helper-calendar",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    snapshot = service.source_review_snapshot(chat_id="manual-calendar-helper", now_utc=now)

    assert "need one of the parents" in outbound[0].text
    assert snapshot.total == 0


def test_household_can_mute_actionable_source_pattern(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    reply = service.handle_incoming(
        _incoming("mute permission slips", chat_id="source-mute", message_id="source-mute-command"),
        now_utc=now,
    )
    outbound = service.ingest_source_item(
        chat_id="source-mute",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        event_at_utc=now + timedelta(hours=8),
        now_utc=now,
    )
    snapshot = service.source_review_snapshot(chat_id="source-mute", now_utc=now)

    assert "keep permission slips quiet" in reply[0].text
    assert outbound == []
    assert snapshot.total == 1
    assert snapshot.stored_only == 1
    assert snapshot.by_reason["household_muted_source"] == 1


def test_source_preference_handles_singular_plural_parent_language(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    mute_reply = service.handle_incoming(
        _incoming("mute newsletter", chat_id="source-plural-language", message_id="mute"),
        now_utc=now,
    )
    muted = service.ingest_source_item(
        chat_id="source-plural-language",
        source_type="email",
        title="Weekly newsletters",
        body="Classroom news and a spirit wear sale.",
        event_at_utc=now + timedelta(hours=12),
        now_utc=now + timedelta(minutes=1),
    )
    always_reply = service.handle_incoming(
        _incoming(
            "always tell me about permission slip",
            chat_id="source-plural-language",
            message_id="always",
        ),
        now_utc=now + timedelta(minutes=2),
    )
    surfaced = service.ingest_source_item(
        chat_id="source-plural-language",
        source_type="email",
        title="Permission slips for field trip",
        body="Please return permission slips by next week.",
        event_at_utc=now + timedelta(days=5),
        now_utc=now + timedelta(minutes=3),
    )
    snapshot = service.source_review_snapshot(chat_id="source-plural-language", now_utc=now)

    assert "keep newsletter quiet" in mute_reply[0].text
    assert muted == []
    assert "make a point to tell you about permission slip" in always_reply[0].text
    assert len(surfaced) == 1
    assert "Permission slips for field trip" in surfaced[0].text
    assert snapshot.surfaced == 1
    assert snapshot.stored_only == 1


def test_household_can_request_low_signal_source_pattern(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    reply = service.handle_incoming(
        _incoming(
            "always tell me about spirit wear",
            chat_id="source-always",
            message_id="source-always-command",
        ),
        now_utc=now,
    )
    outbound = service.ingest_source_item(
        chat_id="source-always",
        source_type="email",
        title="Weekly school newsletter",
        body="Weekly recap and spirit wear sale.",
        event_at_utc=now + timedelta(hours=12),
        now_utc=now,
    )
    snapshot = service.source_review_snapshot(chat_id="source-always", now_utc=now)

    assert "make a point to tell you about spirit wear" in reply[0].text
    assert len(outbound) == 1
    assert "Weekly school newsletter" in outbound[0].text
    assert "add a reminder" not in outbound[0].text
    assert service.pending_actions(chat_id="source-always", now_utc=now) == []
    assert snapshot.surfaced == 1
    assert snapshot.by_reason["household_requested_source"] == 1


def test_source_preferences_are_household_scoped(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("always tell me about spirit wear", chat_id="family-a", message_id="family-a-rule"),
        now_utc=now,
    )
    family_a = service.ingest_source_item(
        chat_id="family-a",
        source_type="email",
        title="Weekly school newsletter",
        body="Weekly recap and spirit wear sale.",
        event_at_utc=now + timedelta(hours=12),
        now_utc=now,
    )
    family_b = service.ingest_source_item(
        chat_id="family-b",
        source_type="email",
        title="Weekly school newsletter",
        body="Weekly recap and spirit wear sale.",
        event_at_utc=now + timedelta(hours=12),
        now_utc=now,
    )

    assert len(family_a) == 1
    assert family_b == []


def test_source_preferences_status_lists_rules(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("always tell me about dentist", chat_id="source-status", message_id="always-rule"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("mute newsletters", chat_id="source-status", message_id="mute-rule"),
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming("source preferences", chat_id="source-status", message_id="source-status"),
        now_utc=now,
    )

    assert "Tell me about: dentist" in outbound[0].text
    assert "Keep quiet about: newsletters" in outbound[0].text


def test_source_review_command_summarizes_texted_and_quiet_items(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.ingest_source_item(
        chat_id="source-review-command",
        source_type="email",
        title="Maya dentist appointment",
        body="Pickup reminder due today for Maya dentist appointment.",
        event_at_utc=now + timedelta(hours=3),
        now_utc=now,
    )
    service.ingest_source_item(
        chat_id="source-review-command",
        source_type="email",
        title="Weekly school newsletter",
        body="Weekly update with spirit wear sale and fundraiser recap.",
        event_at_utc=now + timedelta(hours=12),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming("source review", chat_id="source-review-command", message_id="source-review"),
        now_utc=now + timedelta(minutes=1),
    )

    assert "Source review:" in outbound[0].text
    assert "Texted: 1" in outbound[0].text
    assert "Kept quiet: 1" in outbound[0].text
    assert "Maya dentist appointment" in outbound[0].text
    assert "Weekly school newsletter" in outbound[0].text
    assert "low signal" in outbound[0].text
    assert "spirit wear sale" not in outbound[0].text


def test_source_review_command_is_parent_only(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "source-review-parent-only"

    service.handle_incoming(
        _incoming("hello", chat_id=chat_id, message_id="parent-seen"),
        now_utc=now,
    )
    service.ingest_source_item(
        chat_id=chat_id,
        source_type="email",
        title="Maya therapy appointment",
        body="Appointment reminder for Maya.",
        event_at_utc=now + timedelta(hours=3),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "source review",
            chat_id=chat_id,
            message_id="helper-source-review",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert "need one of the parents" in outbound[0].text
    assert "Maya therapy appointment" not in outbound[0].text


def test_source_preferences_are_parent_only(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("my name is Sam", chat_id="source-parent-only", message_id="parent-one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id="source-parent-only",
            message_id="parent-two",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "always tell me about permission slips",
            chat_id="source-parent-only",
            message_id="parent-rule",
        ),
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming(
            "always tell me about field trips",
            chat_id="source-parent-only",
            message_id="helper-rule",
            sender="+15555550102",
        ),
        now_utc=now,
    )
    status = service.handle_incoming(
        _incoming(
            "source preferences",
            chat_id="source-parent-only",
            message_id="helper-view-rules",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert "need one of the parents" in outbound[0].text
    assert "need one of the parents" in status[0].text
    assert "permission slips" not in status[0].text
    preferences = service.source_preferences(chat_id="source-parent-only")
    assert [preference.phrase for preference in preferences] == ["permission slips"]


def test_source_feedback_without_recent_item_is_clear(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("not useful", chat_id="source-feedback-empty", message_id="empty-feedback"),
        now_utc=now,
    )

    assert "do not have a recent source item" in outbound[0].text


def test_mute_this_sender_requires_recent_source_item(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("mute this sender", chat_id="source-sender-empty", message_id="empty-sender-feedback"),
        now_utc=now,
    )

    assert "recent source item" in outbound[0].text
    assert service.source_preferences(chat_id="source-sender-empty") == []


def test_parent_can_mute_last_surfaced_sender(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    first = service.ingest_source_item(
        chat_id="source-feedback-sender",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        sender="School PTA <updates@school.example>",
        event_at_utc=now + timedelta(hours=8),
        now_utc=now,
    )
    feedback = service.handle_incoming(
        _incoming(
            "mute this sender",
            chat_id="source-feedback-sender",
            message_id="mute-sender-feedback",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    second = service.ingest_source_item(
        chat_id="source-feedback-sender",
        source_type="email",
        title="RSVP due",
        body="Please RSVP for the class celebration by tomorrow.",
        sender="updates@school.example",
        event_at_utc=now + timedelta(hours=10),
        now_utc=now + timedelta(minutes=2),
    )
    preferences = service.source_preferences(chat_id="source-feedback-sender")
    snapshot = service.source_review_snapshot(chat_id="source-feedback-sender", now_utc=now)

    assert len(first) == 1
    assert "keep updates@school.example quiet" in feedback[0].text
    assert preferences[0].phrase == "updates@school.example"
    assert preferences[0].preference == SourcePreferenceKind.MUTE
    assert second == []
    assert snapshot.surfaced == 1
    assert snapshot.stored_only == 1
    assert snapshot.by_reason["household_muted_source"] == 1


def test_parent_can_mute_last_surfaced_sender_domain(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    first = service.ingest_source_item(
        chat_id="source-feedback-domain",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        sender="teacher@school.example",
        event_at_utc=now + timedelta(hours=8),
        now_utc=now,
    )
    feedback = service.handle_incoming(
        _incoming(
            "mute this domain",
            chat_id="source-feedback-domain",
            message_id="mute-domain-feedback",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    second = service.ingest_source_item(
        chat_id="source-feedback-domain",
        source_type="email",
        title="Class celebration RSVP due",
        body="Please RSVP for the class celebration by tomorrow.",
        sender="principal@school.example",
        event_at_utc=now + timedelta(hours=10),
        now_utc=now + timedelta(minutes=2),
    )
    preferences = service.source_preferences(chat_id="source-feedback-domain")

    assert len(first) == 1
    assert "keep school.example quiet" in feedback[0].text
    assert preferences[0].phrase == "school.example"
    assert second == []


def test_negative_source_feedback_mutes_future_similar_items(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming(
            "always tell me about spirit wear",
            chat_id="source-feedback-negative",
            message_id="always-spirit",
        ),
        now_utc=now,
    )
    first = service.ingest_source_item(
        chat_id="source-feedback-negative",
        source_type="email",
        title="Weekly school newsletter",
        body="Weekly recap and spirit wear sale.",
        event_at_utc=now + timedelta(hours=12),
        now_utc=now,
    )
    feedback = service.handle_incoming(
        _incoming(
            "not useful",
            chat_id="source-feedback-negative",
            message_id="negative-feedback",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    second = service.ingest_source_item(
        chat_id="source-feedback-negative",
        source_type="email",
        title="Weekly school newsletter",
        body="Weekly recap and spirit wear sale.",
        event_at_utc=now + timedelta(hours=13),
        now_utc=now + timedelta(minutes=2),
    )
    snapshot = service.source_review_snapshot(chat_id="source-feedback-negative", now_utc=now)

    assert len(first) == 1
    assert "keep weekly school newsletter quieter" in feedback[0].text
    assert second == []
    assert snapshot.surfaced == 1
    assert snapshot.stored_only == 1
    assert snapshot.by_reason["household_muted_source"] == 1


def test_positive_source_feedback_surfaces_future_similar_items(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    first = service.ingest_source_item(
        chat_id="source-feedback-positive",
        source_type="calendar",
        title="Maya dentist appointment",
        body="Pediatric Dentist",
        event_at_utc=now + timedelta(hours=6),
        now_utc=now,
    )
    feedback = service.handle_incoming(
        _incoming(
            "more like this",
            chat_id="source-feedback-positive",
            message_id="positive-feedback",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    second = service.ingest_source_item(
        chat_id="source-feedback-positive",
        source_type="calendar",
        title="Maya dentist appointment",
        body="Six-month cleaning.",
        event_at_utc=now + timedelta(days=30),
        now_utc=now + timedelta(minutes=2),
    )
    snapshot = service.source_review_snapshot(chat_id="source-feedback-positive", now_utc=now)

    assert len(first) == 1
    assert "watch more closely for maya dentist appointment" in feedback[0].text
    assert len(second) == 1
    assert snapshot.surfaced == 2
    assert snapshot.by_reason["household_requested_source"] == 1


def test_source_feedback_is_parent_only(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming(
            "always tell me about spirit wear",
            chat_id="source-feedback-parent-only",
            message_id="parent-rule",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id="source-feedback-parent-only",
            message_id="parent-two",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.ingest_source_item(
        chat_id="source-feedback-parent-only",
        source_type="email",
        title="Weekly school newsletter",
        body="Weekly recap and spirit wear sale.",
        event_at_utc=now + timedelta(hours=12),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "not useful",
            chat_id="source-feedback-parent-only",
            message_id="helper-feedback",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    preferences = service.source_preferences(chat_id="source-feedback-parent-only")

    assert "need one of the parents" in outbound[0].text
    assert [preference.phrase for preference in preferences] == ["spirit wear"]


def test_daily_briefing_runs_once_per_household_local_day(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        daily_briefing_hour=7,
        daily_briefing_minute=15,
    )
    service = FlorenceService(settings=settings, agent=FakeAgent())
    setup_now = datetime(2026, 6, 5, 13, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming(
            "remind us today at 8am to pack lunch",
            chat_id="briefing-chat",
            message_id="briefing-reminder",
        ),
        now_utc=setup_now,
    )
    service.ingest_source_item(
        chat_id="briefing-chat",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        event_at_utc=datetime(2026, 6, 5, 18, 0, tzinfo=timezone.utc),
        now_utc=setup_now,
    )

    briefing_now = datetime(2026, 6, 5, 14, 30, tzinfo=timezone.utc)
    first = service.daily_briefing_messages(now_utc=briefing_now)
    second = service.daily_briefing_messages(now_utc=briefing_now + timedelta(minutes=5))

    assert len(first) == 1
    assert "Good morning" in first[0].text
    assert "pack lunch" in first[0].text
    assert "Permission slip due" in first[0].text
    assert second == []


def test_daily_briefing_skips_after_delivery_window_when_household_restarts(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        daily_briefing_hour=7,
        daily_briefing_minute=15,
        daily_briefing_delivery_grace_minutes=60,
    )
    service = FlorenceService(settings=settings, agent=FakeAgent())
    setup_now = datetime(2026, 6, 5, 13, 0, tzinfo=timezone.utc)
    chat_id = "briefing-stale-restart"
    service.handle_incoming(
        _incoming("my name is Sam", chat_id=chat_id, message_id="name"),
        now_utc=setup_now,
    )
    service.ingest_source_item(
        chat_id=chat_id,
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        event_at_utc=datetime(2026, 6, 5, 18, 0, tzinfo=timezone.utc),
        now_utc=setup_now + timedelta(minutes=1),
    )
    household = service.store.get_household_by_chat(chat_id)
    assert household is not None

    service.handle_incoming(
        _incoming("stop", chat_id=chat_id, message_id="stop"),
        now_utc=setup_now + timedelta(minutes=2),
    )
    stopped_tick = run_routine_tick(
        service,
        FakeSender(),
        now_utc=datetime(2026, 6, 5, 14, 30, tzinfo=timezone.utc),
    )

    assert stopped_tick.briefing_messages == 0

    service.handle_incoming(
        _incoming("start", chat_id=chat_id, message_id="start"),
        now_utc=datetime(2026, 6, 5, 15, 16, tzinfo=timezone.utc),
    )
    late_briefing = service.daily_briefing_messages(
        now_utc=datetime(2026, 6, 5, 15, 16, tzinfo=timezone.utc)
    )
    with service.store.connect() as conn:
        routine_claims = conn.execute(
            "SELECT COUNT(*) AS count FROM routine_runs WHERE household_id = ?",
            (household.id,),
        ).fetchone()["count"]
        source = conn.execute(
            """
            SELECT briefed_at_utc FROM source_items
            WHERE household_id = ? AND title = ?
            """,
            (household.id, "Permission slip due"),
        ).fetchone()

    assert late_briefing == []
    assert routine_claims == 0
    assert source["briefed_at_utc"] is None


def test_empty_daily_briefing_does_not_text_or_claim_day(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        daily_briefing_hour=7,
        daily_briefing_minute=15,
    )
    service = FlorenceService(settings=settings, agent=FakeAgent())
    now = datetime(2026, 6, 5, 14, 30, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("my name is Sam", chat_id="empty-briefing", message_id="empty-briefing-name"),
        now_utc=now - timedelta(hours=1),
    )

    empty = service.daily_briefing_messages(now_utc=now)
    service.ingest_source_item(
        chat_id="empty-briefing",
        source_type="email",
        title="RSVP due for field trip",
        body="Please RSVP for next week's field trip.",
        event_at_utc=now + timedelta(hours=8),
        now_utc=now + timedelta(minutes=5),
    )
    later = service.daily_briefing_messages(now_utc=now + timedelta(minutes=6))
    duplicate = service.daily_briefing_messages(now_utc=now + timedelta(minutes=7))

    assert empty == []
    assert len(later) == 1
    assert "RSVP due for field trip" in later[0].text
    assert duplicate == []


def test_routine_tick_sends_reminders_and_briefings(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        daily_briefing_hour=7,
        daily_briefing_minute=15,
    )
    service = FlorenceService(settings=settings, agent=FakeAgent())
    sender = FakeSender()
    setup_now = datetime(2026, 6, 5, 13, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming(
            "remind us today at 7:20am to leave for school",
            chat_id="worker-chat",
            message_id="worker-reminder",
        ),
        now_utc=setup_now,
    )
    service.handle_incoming(
        _incoming(
            "remind us today at 8am to bring jackets",
            chat_id="worker-chat",
            message_id="worker-briefing-reminder",
        ),
        now_utc=setup_now + timedelta(minutes=1),
    )

    result = run_routine_tick(
        service,
        sender,
        now_utc=datetime(2026, 6, 5, 14, 25, tzinfo=timezone.utc),
    )

    assert result.sent == 2
    assert result.reminder_messages == 1
    assert result.briefing_messages == 1
    assert any("Quick reminder: leave for school" in message["text"] for message in sender.sent)
    assert any("Good morning" in message["text"] for message in sender.sent)
    assert any("bring jackets" in message["text"] for message in sender.sent)


def test_routine_tick_keeps_reminder_pending_when_delivery_fails(tmp_path):
    service, _agent = _service(tmp_path)
    setup_now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    due_at = datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc)
    chat_id = "worker-retry-chat"
    service.handle_incoming(
        _incoming(
            "remind us tomorrow at 8am to pack lunch",
            chat_id=chat_id,
            message_id="worker-retry-reminder",
        ),
        now_utc=setup_now,
    )
    household = service.store.get_household_by_chat(chat_id)
    assert household is not None

    failed_result = run_routine_tick(service, FlakySender(fail_send=1), now_utc=due_at)
    still_due = service.store.due_reminders(
        now_utc=due_at + timedelta(minutes=1),
        not_before_utc=due_at - timedelta(minutes=5),
    )
    retry_sender = FakeSender()
    retry_result = run_routine_tick(service, retry_sender, now_utc=due_at + timedelta(minutes=1))
    sent = service.store.recent_sent_reminders(
        household_id=household.id,
        now_utc=due_at + timedelta(minutes=1),
        since_utc=due_at - timedelta(minutes=1),
    )

    assert failed_result.sent == 0
    assert failed_result.delivery_failed == 1
    assert failed_result.reminder_messages == 1
    assert len(still_due) == 1
    assert still_due[0].status == ReminderStatus.PENDING
    assert retry_result.reminder_messages == 1
    assert retry_sender.sent[0]["idempotency_key"].startswith("reminder:")
    assert len(sent) == 1
    assert sent[0].status == ReminderStatus.SENT


def test_routine_tick_continues_after_one_household_delivery_fails(tmp_path):
    service, _agent = _service(tmp_path)
    setup_now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    due_at = datetime(2026, 6, 6, 15, 0, tzinfo=timezone.utc)
    first_chat = "worker-partial-failure-one"
    second_chat = "worker-partial-failure-two"
    service.handle_incoming(
        _incoming(
            "remind us tomorrow at 8am to pack lunch",
            chat_id=first_chat,
            message_id="worker-partial-failure-reminder-one",
        ),
        now_utc=setup_now,
    )
    service.handle_incoming(
        _incoming(
            "remind us tomorrow at 8am to bring jackets",
            chat_id=second_chat,
            message_id="worker-partial-failure-reminder-two",
        ),
        now_utc=setup_now,
    )
    first_household = service.store.get_household_by_chat(first_chat)
    second_household = service.store.get_household_by_chat(second_chat)
    assert first_household is not None
    assert second_household is not None
    sender = FlakySender(fail_send=1)

    result = run_routine_tick(service, sender, now_utc=due_at)
    first_due = service.store.due_reminders(
        now_utc=due_at + timedelta(minutes=1),
        not_before_utc=due_at - timedelta(minutes=5),
    )
    second_sent = service.store.recent_sent_reminders(
        household_id=second_household.id,
        now_utc=due_at + timedelta(minutes=1),
        since_utc=due_at - timedelta(minutes=1),
    )

    assert result.sent == 1
    assert result.delivery_failed == 1
    assert result.reminder_messages == 2
    assert [message["chat_id"] for message in sender.sent] == [second_chat]
    assert len(first_due) == 1
    assert first_due[0].household_id == first_household.id
    assert first_due[0].status == ReminderStatus.PENDING
    assert len(second_sent) == 1
    assert second_sent[0].status == ReminderStatus.SENT


def test_routine_tick_keeps_daily_briefing_unclaimed_when_delivery_fails(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        daily_briefing_hour=7,
        daily_briefing_minute=15,
    )
    service = FlorenceService(settings=settings, agent=FakeAgent())
    setup_now = datetime(2026, 6, 5, 13, 0, tzinfo=timezone.utc)
    briefing_now = datetime(2026, 6, 5, 14, 30, tzinfo=timezone.utc)
    chat_id = "briefing-retry-chat"
    service.handle_incoming(
        _incoming("my name is Sam", chat_id=chat_id, message_id="briefing-retry-name"),
        now_utc=setup_now,
    )
    service.sync_connected_sources(
        chat_id=chat_id,
        provider="google",
        external_account_id="parent@example.com",
        emails=[
            {
                "external_id": "briefing-retry-source",
                "subject": "RSVP due for class picnic",
                "body": "Please RSVP for next week's class picnic.",
                "sender": "teacher@example.com",
                "received_at_utc": setup_now.isoformat(),
                "event_at_utc": (briefing_now + timedelta(hours=8)).isoformat(),
            }
        ],
        now_utc=setup_now + timedelta(minutes=1),
    )
    household = service.store.get_household_by_chat(chat_id)
    assert household is not None

    failed_result = run_routine_tick(service, FlakySender(fail_send=1), now_utc=briefing_now)
    with service.store.connect() as conn:
        failed_claims = conn.execute(
            "SELECT COUNT(*) AS count FROM routine_runs WHERE household_id = ?",
            (household.id,),
        ).fetchone()["count"]
        failed_messages = conn.execute(
            "SELECT COUNT(*) AS count FROM messages WHERE household_id = ? AND id LIKE ?",
            (household.id, "routine:daily_briefing:%"),
        ).fetchone()["count"]
        failed_source = conn.execute(
            """
            SELECT briefed_at_utc FROM source_items
            WHERE household_id = ? AND title = ?
            """,
            (household.id, "RSVP due for class picnic"),
        ).fetchone()

    retry_sender = FakeSender()
    retry_result = run_routine_tick(
        service,
        retry_sender,
        now_utc=briefing_now + timedelta(minutes=1),
    )
    duplicate = run_routine_tick(
        service,
        FakeSender(),
        now_utc=briefing_now + timedelta(minutes=2),
    )
    with service.store.connect() as conn:
        successful_claims = conn.execute(
            "SELECT COUNT(*) AS count FROM routine_runs WHERE household_id = ?",
            (household.id,),
        ).fetchone()["count"]
        successful_messages = conn.execute(
            "SELECT COUNT(*) AS count FROM messages WHERE household_id = ? AND id LIKE ?",
            (household.id, "routine:daily_briefing:%"),
        ).fetchone()["count"]
        successful_source = conn.execute(
            """
            SELECT briefed_at_utc FROM source_items
            WHERE household_id = ? AND title = ?
            """,
            (household.id, "RSVP due for class picnic"),
        ).fetchone()

    assert failed_result.sent == 0
    assert failed_result.delivery_failed == 1
    assert failed_result.briefing_messages == 1
    assert failed_claims == 0
    assert failed_messages == 0
    assert failed_source["briefed_at_utc"] is None
    assert retry_result.briefing_messages == 1
    assert retry_sender.sent[0]["idempotency_key"].startswith("routine:daily_briefing:")
    assert "RSVP due for class picnic" in retry_sender.sent[0]["text"]
    assert successful_claims == 1
    assert successful_messages == 1
    assert successful_source["briefed_at_utc"] is not None
    assert duplicate.briefing_messages == 0


def test_memory_is_household_scoped(tmp_path):
    store = Store(str(tmp_path / "florence.sqlite"))
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    one = store.get_or_create_household(chat_id="chat-1", timezone_name="UTC", now_utc=now)
    two = store.get_or_create_household(chat_id="chat-2", timezone_name="UTC", now_utc=now)

    store.upsert_memory(
        household_id=one.id,
        kind=MemoryKind.PREFERENCE,
        text="Maya likes pasta.",
        now_utc=now,
    )

    assert [m.text for m in store.list_memories(household_id=one.id, now_utc=now)] == [
        "Maya likes pasta."
    ]
    assert store.list_memories(household_id=two.id, now_utc=now) == []


def test_memory_provenance_member_must_belong_to_household(tmp_path):
    store = Store(str(tmp_path / "florence.sqlite"))
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    one = store.get_or_create_household(chat_id="chat-1", timezone_name="UTC", now_utc=now)
    two = store.get_or_create_household(chat_id="chat-2", timezone_name="UTC", now_utc=now)
    family_two_member = store.get_or_create_member(
        household_id=two.id,
        phone="+15555550102",
        now_utc=now,
    )

    with pytest.raises(ValueError, match="must belong to the household"):
        store.upsert_memory(
            household_id=one.id,
            kind=MemoryKind.PREFERENCE,
            text="Maya likes pasta.",
            now_utc=now,
            asserted_by_member_id=family_two_member.id,
        )

    assert store.list_memories(household_id=one.id, now_utc=now) == []


def test_reminder_assignee_member_must_belong_to_household(tmp_path):
    store = Store(str(tmp_path / "florence.sqlite"))
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    one = store.get_or_create_household(chat_id="chat-1", timezone_name="UTC", now_utc=now)
    two = store.get_or_create_household(chat_id="chat-2", timezone_name="UTC", now_utc=now)
    family_two_member = store.get_or_create_member(
        household_id=two.id,
        phone="+15555550102",
        now_utc=now,
    )

    with pytest.raises(ValueError, match="must belong to the household"):
        store.create_reminder(
            household_id=one.id,
            chat_id=one.chat_id,
            title="pack lunch",
            due_at_utc=now + timedelta(hours=1),
            created_at_utc=now,
            assignee_member_id=family_two_member.id,
        )

    assert store.upcoming_reminders(household_id=one.id, now_utc=now) == []


def test_memory_delete_is_household_scoped(tmp_path):
    store = Store(str(tmp_path / "florence.sqlite"))
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    one = store.get_or_create_household(chat_id="chat-1", timezone_name="UTC", now_utc=now)
    two = store.get_or_create_household(chat_id="chat-2", timezone_name="UTC", now_utc=now)
    memory = store.upsert_memory(
        household_id=one.id,
        kind=MemoryKind.PREFERENCE,
        text="Maya likes pasta.",
        now_utc=now,
    )

    assert not store.delete_memory(household_id=two.id, memory_id=memory.id, now_utc=now)
    assert [m.id for m in store.list_memories(household_id=one.id, now_utc=now)] == [memory.id]

    assert store.delete_memory(household_id=one.id, memory_id=memory.id, now_utc=now)
    assert store.list_memories(household_id=one.id, now_utc=now) == []


def test_explicit_memory_reaches_agent_context(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("remember that Maya likes pasta", message_id="message-remember"),
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming("what should we make for dinner?", message_id="message-ask"),
        now_utc=now,
    )

    assert outbound[0].text == "Fake agent reply."
    assert agent.calls[-1]["memories"][0].text == "Maya likes pasta"
    assert agent.calls[-1]["memories"][0].asserted_by_member_id == agent.calls[-1]["actor"].id


def test_memory_status_shows_household_memory_with_provenance(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(_incoming("my name is Sam", message_id="memory-name"), now_utc=now)
    service.handle_incoming(
        _incoming("remember that Maya likes pasta", message_id="memory-remember"),
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming("what do you remember?", message_id="memory-status"),
        now_utc=now,
    )

    assert "Here is what I remember for this household" in outbound[0].text
    assert "Maya likes pasta (from Sam)" in outbound[0].text


def test_household_book_alias_shows_curated_memory(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("remember that Maya likes pasta", message_id="book-remember"),
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming("show household book", message_id="book-show"),
        now_utc=now,
    )

    assert "household book" in outbound[0].text.lower()
    assert "Maya likes pasta" in outbound[0].text


def test_duplicate_explicit_memory_updates_existing_record(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("remember that Maya likes pasta", message_id="memory-1"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("remember that Maya likes pasta", message_id="memory-2"),
        now_utc=now + timedelta(minutes=1),
    )
    snapshot = service.memory_snapshot(chat_id="chat-1", now_utc=now + timedelta(minutes=1))

    assert [memory.text for memory in snapshot.memories] == ["Maya likes pasta"]


def test_long_explicit_memory_is_not_stored(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    long_fact = " ".join(["Maya likes detailed school lunch notes"] * 12)

    outbound = service.handle_incoming(
        _incoming(f"remember that {long_fact}", message_id="memory-too-long"),
        now_utc=now,
    )
    snapshot = service.memory_snapshot(chat_id="chat-1", now_utc=now)

    assert "short version in 240 characters or fewer" in outbound[0].text
    assert snapshot.memories == []


def test_parent_can_clear_all_household_memory(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("remember that Maya likes pasta", message_id="memory-clear-1"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("remember that Leo likes soccer", message_id="memory-clear-2"),
        now_utc=now + timedelta(minutes=1),
    )

    reply = service.handle_incoming(
        _incoming("clear household memory", message_id="memory-clear-all"),
        now_utc=now + timedelta(minutes=2),
    )
    snapshot = service.memory_snapshot(chat_id="chat-1", now_utc=now + timedelta(minutes=3))

    assert "cleared 2 household memories" in reply[0].text
    assert snapshot.memories == []


def test_helper_cannot_view_write_or_delete_durable_memory(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("my name is Sam", chat_id="memory-parent-only", message_id="parent-one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id="memory-parent-only",
            message_id="parent-two",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "remember that Maya likes pasta",
            chat_id="memory-parent-only",
            message_id="parent-memory",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    helper_write = service.handle_incoming(
        _incoming(
            "remember that Maya hates soccer",
            chat_id="memory-parent-only",
            message_id="helper-memory",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=2),
    )
    helper_forget = service.handle_incoming(
        _incoming(
            "forget Maya likes pasta",
            chat_id="memory-parent-only",
            message_id="helper-forget",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=3),
    )
    helper_clear = service.handle_incoming(
        _incoming(
            "clear household memory",
            chat_id="memory-parent-only",
            message_id="helper-clear-memory",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=4),
    )
    helper_view = service.handle_incoming(
        _incoming(
            "what do you remember?",
            chat_id="memory-parent-only",
            message_id="helper-view-memory",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=5),
    )
    snapshot = service.memory_snapshot(chat_id="memory-parent-only", now_utc=now + timedelta(minutes=5))

    assert "need one of the parents" in helper_write[0].text
    assert "need one of the parents" in helper_forget[0].text
    assert "need one of the parents" in helper_clear[0].text
    assert "need one of the parents" in helper_view[0].text
    assert "Maya likes pasta" not in helper_view[0].text
    assert [memory.text for memory in snapshot.memories] == ["Maya likes pasta"]


def test_only_first_sender_is_automatic_parent_and_later_senders_are_helpers(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(_incoming("my name is Alex", chat_id="chat-parents", message_id="m1"), now_utc=now)
    service.handle_incoming(
        IncomingMessage(
            chat_id="chat-parents",
            message_id="m2",
            sender="+15555550101",
            text="my name is Jordan",
            received_at=now,
        ),
        now_utc=now,
    )
    service.handle_incoming(
        IncomingMessage(
            chat_id="chat-parents",
            message_id="m3",
            sender="+15555550102",
            text="hello",
            received_at=now,
        ),
        now_utc=now,
    )

    household = service.store.get_household_by_chat("chat-parents")
    assert household is not None
    members = service.store.list_members(household.id)
    assert [member.role.value for member in members] == ["parent", "helper", "helper"]
    assert [member.display_name for member in members] == ["Alex", "Jordan", None]


def test_parent_can_confirm_partner_as_second_parent(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "confirm-partner"

    service.handle_incoming(
        _incoming("my name is Sam", chat_id=chat_id, message_id="parent-one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id=chat_id,
            message_id="partner-before-confirm",
            sender="+15555550101",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    reply = service.handle_incoming(
        _incoming("confirm partner +15555550101", chat_id=chat_id, message_id="confirm"),
        now_utc=now + timedelta(minutes=2),
    )
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id=chat_id,
            message_id="partner-name",
            sender="+15555550101",
        ),
        now_utc=now + timedelta(minutes=3),
    )

    household = service.store.get_household_by_chat(chat_id)
    assert household is not None
    members = service.store.list_members(household.id)

    assert "confirmed +15555550101 as the second parent" in reply[0].text
    assert [member.role.value for member in members] == ["parent", "parent"]
    assert [member.display_name for member in members] == ["Sam", "Alex"]


def test_helper_cannot_change_household_setup_details(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "setup-parent-only"
    service.handle_incoming(
        _incoming("my name is Alex", chat_id=chat_id, message_id="parent-one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "my name is Jordan",
            chat_id=chat_id,
            message_id="parent-two",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id=chat_id,
            message_id="helper-seen",
            sender="+15555550102",
        ),
        now_utc=now,
    )

    child_reply = service.handle_incoming(
        _incoming(
            "our kids are Maya and Leo",
            chat_id=chat_id,
            message_id="helper-kids",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    timezone_reply = service.handle_incoming(
        _incoming(
            "set timezone America/New_York",
            chat_id=chat_id,
            message_id="helper-timezone",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=2),
    )
    readiness = service.readiness_snapshot(chat_id=chat_id, now_utc=now + timedelta(minutes=3))
    household = service.store.get_household_by_chat(chat_id)

    assert household is not None
    assert "need one of the parents" in child_reply[0].text
    assert "need one of the parents" in timezone_reply[0].text
    assert readiness.child_count == 0
    assert household.timezone == "America/Los_Angeles"


def test_household_status_and_timezone_commands(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    name_reply = service.handle_incoming(_incoming("my name is Sam", message_id="name"), now_utc=now)
    tz_reply = service.handle_incoming(
        _incoming("set timezone America/New_York", message_id="tz"),
        now_utc=now,
    )
    status_reply = service.handle_incoming(
        _incoming("household status", message_id="status"),
        now_utc=now,
    )

    assert "Nice to meet you, Sam" in name_reply[0].text
    assert "America/New_York" in tz_reply[0].text
    assert "Household timezone: America/New_York" in status_reply[0].text
    assert "Sam (parent)" in status_reply[0].text


def test_household_status_uses_role_label_for_unnamed_member(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("hello", chat_id="status-unnamed", message_id="hello", sender="+15555550998"),
        now_utc=now,
    )
    status_reply = service.handle_incoming(
        _incoming(
            "household status",
            chat_id="status-unnamed",
            message_id="status",
            sender="+15555550998",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert "unnamed parent (parent)" in status_reply[0].text
    assert "+15555550998" not in status_reply[0].text


def test_parent_invite_creates_group_chat_delivery_instruction(tmp_path):
    service, _agent = _linq_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("invite partner (555) 555-0101", chat_id="invite-chat", message_id="invite"),
        now_utc=now,
    )

    assert len(outbound) == 2
    assert "start a shared household thread with +15555550101" in outbound[0].text
    assert outbound[1].new_chat_from == "+15555550000"
    assert outbound[1].new_chat_to == ("+15555550100", "+15555550101")
    assert outbound[1].migrate_household_id is not None
    assert outbound[1].invited_partner_phone == "+15555550101"
    assert "https://" not in outbound[1].text


def test_partner_invite_requires_parent_and_config(tmp_path):
    service, _agent = _linq_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("hello", chat_id="invite-parent-only", message_id="parent-one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id="invite-parent-only",
            message_id="parent-two",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id="invite-parent-only",
            message_id="helper-seen",
            sender="+15555550102",
        ),
        now_utc=now,
    )

    helper_reply = service.handle_incoming(
        _incoming(
            "invite partner +15555550103",
            chat_id="invite-parent-only",
            message_id="helper-invite",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    unconfigured, _agent = _service(tmp_path)
    unconfigured_reply = unconfigured.handle_incoming(
        _incoming("invite partner +15555550101", chat_id="invite-unconfigured"),
        now_utc=now,
    )

    assert "need one of the parents" in helper_reply[0].text
    assert "Linq sending still needs to be configured" in unconfigured_reply[0].text


def test_complete_partner_group_created_migrates_household_and_preserves_old_chat_alias(tmp_path):
    service, _agent = _linq_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    outbound = service.handle_incoming(
        _incoming("invite partner +15555550101", chat_id="old-chat", message_id="invite"),
        now_utc=now,
    )
    household_id = outbound[1].migrate_household_id
    assert household_id is not None

    service.complete_partner_group_created(
        household_id=household_id,
        new_chat_id="new-group-chat",
        partner_phone="+15555550101",
        intro_text=outbound[1].text,
        now_utc=now + timedelta(seconds=1),
    )
    old_household = service.store.get_household_by_chat("old-chat")
    new_household = service.store.get_household_by_chat("new-group-chat")
    members = service.store.list_members(household_id)
    readiness = service.readiness_snapshot(chat_id="new-group-chat", now_utc=now)

    assert old_household is not None
    assert new_household is not None
    assert old_household.id == new_household.id == household_id
    assert new_household.chat_id == "new-group-chat"
    assert [member.phone for member in members[:2]] == ["+15555550100", "+15555550101"]
    assert readiness.parent_count == 2


def test_partner_group_migration_retargets_retryable_source_delivery(tmp_path):
    service, _agent = _linq_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="old-source-chat",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
    )
    source_result = service.sync_connected_sources(
        chat_id="old-source-chat",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-2",
        now_utc=now + timedelta(minutes=1),
        mark_surfaced=False,
        emails=[
            {
                "external_id": "permission-slip",
                "subject": "Permission slip due",
                "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                "sender": "teacher@example.com",
                "received_at_utc": (now + timedelta(minutes=1)).isoformat(),
                "event_at_utc": (now + timedelta(hours=8)).isoformat(),
            }
        ],
    )
    service.prepare_outbound_delivery(source_result.messages[0], now_utc=now + timedelta(minutes=1))
    service.mark_outbound_delivery_failed(
        source_result.messages[0],
        error="RuntimeError: linq send unavailable",
        now_utc=now + timedelta(minutes=2),
    )
    invite = service.handle_incoming(
        _incoming("invite partner +15555550101", chat_id="old-source-chat", message_id="invite"),
        now_utc=now + timedelta(minutes=3),
    )
    household_id = invite[1].migrate_household_id
    assert household_id is not None

    service.complete_partner_group_created(
        household_id=household_id,
        new_chat_id="new-source-group",
        partner_phone="+15555550101",
        intro_text=invite[1].text,
        now_utc=now + timedelta(minutes=4),
    )
    retry = service.store.retryable_source_outbound_deliveries()
    duplicate_retry = service.sync_connected_sources(
        chat_id="new-source-group",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-3",
        now_utc=now + timedelta(minutes=5),
        mark_surfaced=False,
        emails=[
            {
                "external_id": "permission-slip",
                "subject": "Permission slip due",
                "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                "sender": "teacher@example.com",
                "received_at_utc": (now + timedelta(minutes=5)).isoformat(),
                "event_at_utc": (now + timedelta(hours=8)).isoformat(),
            }
        ],
    )

    assert retry[0].chat_id == "new-source-group"
    assert duplicate_retry.messages[0].chat_id == "new-source-group"


def test_agent_receives_actor_and_member_context(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(_incoming("my name is Sam", message_id="name"), now_utc=now)
    service.handle_incoming(_incoming("what should we do next?", message_id="ask"), now_utc=now)

    assert agent.calls[-1]["actor"].display_name == "Sam"
    assert agent.calls[-1]["members"][0].display_name == "Sam"


def test_agent_receives_household_policy_context(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("always tell me about permission slips", chat_id="agent-policy", message_id="source-rule"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("pause memory", chat_id="agent-policy", message_id="pause-memory"),
        now_utc=now + timedelta(minutes=1),
    )
    service.handle_incoming(
        _incoming("what should we do next?", chat_id="agent-policy", message_id="agent-ask"),
        now_utc=now + timedelta(minutes=2),
    )
    call = agent.calls[-1]

    assert call["privacy"].memory_enabled is False
    assert call["readiness"].ready is False
    assert any("Invite or confirm your partner" in item for item in call["readiness"].missing)
    assert call["source_preferences"][0].phrase == "permission slips"
    assert call["source_preferences"][0].preference == SourcePreferenceKind.ALWAYS_SURFACE


def test_agent_context_does_not_duplicate_current_parent_turn(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "agent-no-duplicate-current"

    service.handle_incoming(
        _incoming(
            "Could you think through pickup?",
            chat_id=chat_id,
            message_id="first-question",
            received_at=now,
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "What about dinner?",
            chat_id=chat_id,
            message_id="second-question",
            received_at=now + timedelta(minutes=1),
        ),
        now_utc=now + timedelta(minutes=1),
    )
    call = agent.calls[-1]

    assert call["user_text"] == "What about dinner?"
    assert {"role": "user", "content": "What about dinner?"} not in call["conversation_history"]
    assert {"role": "user", "content": "Could you think through pickup?"} in call["conversation_history"]


def test_helper_turn_excludes_source_preferences_from_agent_context(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming(
            "always tell me about permission slips",
            chat_id="helper-source-policy",
            message_id="source-rule",
            sender="+15555550100",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "Should I worry about school messages?",
            chat_id="helper-source-policy",
            message_id="helper-question",
            sender="+15555550199",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    call = agent.calls[-1]

    assert call["actor"].role.value == "helper"
    assert call["source_preferences"] == []


def test_helper_turn_history_starts_when_helper_first_seen(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "helper-history"

    service.handle_incoming(
        _incoming(
            "Maya has a private appointment at 4.",
            chat_id=chat_id,
            message_id="parent-private",
            sender="+15555550100",
            received_at=now,
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "What should I know for pickup?",
            chat_id=chat_id,
            message_id="helper-question",
            sender="+15555550199",
            received_at=now + timedelta(minutes=1),
        ),
        now_utc=now + timedelta(minutes=1),
    )
    call = agent.calls[-1]
    history_blob = "\n".join(message["content"] for message in call["conversation_history"])

    assert call["actor"].role.value == "helper"
    assert "private appointment" not in history_blob
    assert call["user_text"] == "What should I know for pickup?"
    assert call["conversation_history"] == []


def test_helper_turn_excludes_preexisting_upcoming_reminders_from_agent_context(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "helper-upcoming"

    service.handle_incoming(
        _incoming(
            "remind me tomorrow at 9am about private therapy appointment",
            chat_id=chat_id,
            message_id="parent-reminder",
            sender="+15555550100",
            received_at=now,
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "What should I know for pickup?",
            chat_id=chat_id,
            message_id="helper-question",
            sender="+15555550199",
            received_at=now + timedelta(minutes=1),
        ),
        now_utc=now + timedelta(minutes=1),
    )
    call = agent.calls[-1]

    assert call["actor"].role.value == "helper"
    assert call["upcoming"] == []


def test_parent_can_approve_pending_action(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("my name is Sam", chat_id="approval-chat", message_id="approval-name"),
        now_utc=now,
    )
    service.create_pending_action(
        chat_id="approval-chat",
        action_type="send_message",
        summary="Email the teacher about pickup.",
        payload={"to": "teacher@example.com"},
        now_utc=now,
    )
    action = service.pending_actions(chat_id="approval-chat", now_utc=now)[0]

    outbound = service.handle_incoming(
        _incoming(
            f"approve {action.id[:8]}",
            chat_id="approval-chat",
            message_id="approval-approve",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    household = service.store.get_household_by_chat("approval-chat")
    assert household is not None
    resolved = service.store.list_pending_actions(
        household_id=household.id,
        now_utc=now + timedelta(minutes=1),
        include_resolved=True,
    )[0]

    assert "Approved: Email the teacher about pickup." in outbound[0].text
    assert resolved.status == PendingActionStatus.APPROVED
    assert resolved.resolved_by_member_id is not None


def test_household_handoff_lists_pending_approvals_and_upcoming_reminders(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.create_pending_action(
        chat_id="handoff-chat",
        action_type="send_message",
        summary="Email the teacher about pickup.",
        payload={"to": "teacher@example.com"},
        now_utc=now,
    )
    action = service.pending_actions(chat_id="handoff-chat", now_utc=now)[0]
    service.handle_incoming(
        _incoming(
            "remind us tomorrow at 8am to pack lunch",
            chat_id="handoff-chat",
            message_id="handoff-reminder",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "remind us June 20 at 8am to pay camp deposit",
            chat_id="handoff-chat",
            message_id="handoff-future",
        ),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming("what's open?", chat_id="handoff-chat", message_id="handoff"),
        now_utc=now + timedelta(minutes=1),
    )

    assert "Household handoff:" in outbound[0].text
    assert "Needs parent approval:" in outbound[0].text
    assert f"approve {action.id[:8]}" in outbound[0].text
    assert "Email the teacher about pickup." in outbound[0].text
    assert "Coming up:" in outbound[0].text
    assert "pack lunch" in outbound[0].text
    assert "camp deposit" not in outbound[0].text
    assert agent.calls == []


def test_household_handoff_is_clear_when_empty(tmp_path):
    service, agent = _service(tmp_path)

    outbound = service.handle_incoming(
        _incoming("handoff", chat_id="handoff-empty", message_id="handoff-empty"),
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )

    assert outbound[0].text == (
        "Household handoff is clear. I do not see pending approvals or upcoming reminders."
    )
    assert agent.calls == []


def test_helper_cannot_view_household_handoff(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("hello", chat_id="handoff-helper", message_id="parent-one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id="handoff-helper",
            message_id="parent-two",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id="handoff-helper",
            message_id="helper-seen",
            sender="+15555550102",
        ),
        now_utc=now,
    )
    service.create_pending_action(
        chat_id="handoff-helper",
        action_type="send_message",
        summary="Email the teacher about pickup.",
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "handoff",
            chat_id="handoff-helper",
            message_id="helper-handoff",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert "need one of the parents" in outbound[0].text
    assert "Email the teacher" not in outbound[0].text


def test_helper_cannot_approve_pending_action(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("hello", chat_id="helper-approval", message_id="parent-1"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id="helper-approval",
            message_id="parent-2",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id="helper-approval",
            message_id="helper-1",
            sender="+15555550102",
        ),
        now_utc=now,
    )
    service.create_pending_action(
        chat_id="helper-approval",
        action_type="send_message",
        summary="Text the babysitter.",
        now_utc=now,
    )
    action = service.pending_actions(chat_id="helper-approval", now_utc=now)[0]

    outbound = service.handle_incoming(
        _incoming(
            f"approve {action.id[:8]}",
            chat_id="helper-approval",
            message_id="helper-approve",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert "need one of the parents" in outbound[0].text
    assert service.pending_actions(chat_id="helper-approval", now_utc=now + timedelta(minutes=1))[0].id == action.id


def test_pending_action_approval_is_household_scoped(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.create_pending_action(
        chat_id="approval-a",
        action_type="send_message",
        summary="Email the coach.",
        now_utc=now,
    )
    action = service.pending_actions(chat_id="approval-a", now_utc=now)[0]

    outbound = service.handle_incoming(
        _incoming(
            f"approve {action.id[:8]}",
            chat_id="approval-b",
            message_id="wrong-household-approve",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert "could not find an active approval" in outbound[0].text
    assert service.pending_actions(chat_id="approval-a", now_utc=now + timedelta(minutes=1))[0].id == action.id


def test_expired_pending_action_cannot_be_approved(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.create_pending_action(
        chat_id="expired-approval",
        action_type="send_message",
        summary="Text the tutor.",
        now_utc=now,
        expires_at_utc=now + timedelta(minutes=1),
    )
    action = service.pending_actions(chat_id="expired-approval", now_utc=now)[0]

    outbound = service.handle_incoming(
        _incoming(
            f"approve {action.id[:8]}",
            chat_id="expired-approval",
            message_id="expired-approve",
        ),
        now_utc=now + timedelta(minutes=2),
    )
    household = service.store.get_household_by_chat("expired-approval")
    assert household is not None
    resolved = service.store.list_pending_actions(
        household_id=household.id,
        now_utc=now + timedelta(minutes=2),
        include_resolved=True,
    )[0]

    assert "could not find an active approval" in outbound[0].text
    assert resolved.status == PendingActionStatus.EXPIRED


def test_unapproved_pending_action_does_not_execute(tmp_path):
    service, _agent = _service(tmp_path)
    sender = FakeSender()
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.create_pending_action(
        chat_id="execute-pending",
        action_type="send_message",
        summary="Text the household.",
        payload={"text": "Pickup is at 4."},
        now_utc=now,
    )

    result = run_approved_actions(
        store=service.store,
        sender=sender,
        now_utc=now + timedelta(minutes=1),
    )

    assert result.attempted == 0
    assert sender.sent == []
    assert service.store.list_action_executions() == []


def test_approved_send_message_action_executes_once_and_audits(tmp_path):
    service, _agent = _service(tmp_path)
    sender = FakeSender()
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("my name is Sam", chat_id="execute-approved", message_id="execute-name"),
        now_utc=now,
    )
    service.create_pending_action(
        chat_id="execute-approved",
        action_type="send_message",
        summary="Text the household.",
        payload={"text": "Pickup is at 4."},
        now_utc=now,
    )
    action = service.pending_actions(chat_id="execute-approved", now_utc=now)[0]
    service.handle_incoming(
        _incoming(
            f"approve {action.id[:8]}",
            chat_id="execute-approved",
            message_id="execute-approve",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    first = run_approved_actions(
        store=service.store,
        sender=sender,
        now_utc=now + timedelta(minutes=2),
    )
    second = run_approved_actions(
        store=service.store,
        sender=sender,
        now_utc=now + timedelta(minutes=3),
    )
    household = service.store.get_household_by_chat("execute-approved")
    assert household is not None
    execution = service.store.list_action_executions(household_id=household.id)[0]
    resolved = service.store.list_pending_actions(
        household_id=household.id,
        now_utc=now + timedelta(minutes=3),
        include_resolved=True,
    )[0]

    assert first.succeeded == 1
    assert second.attempted == 0
    assert sender.sent == [
        {
            "chat_id": "execute-approved",
            "text": "Pickup is at 4.",
            "idempotency_key": f"action:{action.id}",
        }
    ]
    assert execution.status == ActionExecutionStatus.SUCCESS
    assert execution.action_id == action.id
    assert resolved.status == PendingActionStatus.EXECUTED


def test_approved_action_waits_while_household_is_stopped(tmp_path):
    service, _agent = _service(tmp_path)
    sender = FakeSender()
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.create_pending_action(
        chat_id="execute-stopped",
        action_type="send_message",
        summary="Text the household.",
        payload={"text": "Pickup is at 4."},
        now_utc=now,
        expires_at_utc=now + timedelta(hours=1),
    )
    action = service.pending_actions(chat_id="execute-stopped", now_utc=now)[0]
    service.handle_incoming(
        _incoming(
            f"approve {action.id[:8]}",
            chat_id="execute-stopped",
            message_id="execute-approve",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    service.handle_incoming(
        _incoming("stop", chat_id="execute-stopped", message_id="execute-stop"),
        now_utc=now + timedelta(minutes=2),
    )

    stopped = run_approved_actions(
        store=service.store,
        sender=sender,
        now_utc=now + timedelta(minutes=3),
    )
    service.handle_incoming(
        _incoming("start", chat_id="execute-stopped", message_id="execute-start"),
        now_utc=now + timedelta(minutes=4),
    )
    restarted = run_approved_actions(
        store=service.store,
        sender=sender,
        now_utc=now + timedelta(minutes=5),
    )
    household = service.store.get_household_by_chat("execute-stopped")
    assert household is not None
    resolved = service.store.list_pending_actions(
        household_id=household.id,
        now_utc=now + timedelta(minutes=5),
        include_resolved=True,
    )[0]

    assert stopped.attempted == 0
    assert restarted.succeeded == 1
    assert sender.sent == [
        {
            "chat_id": "execute-stopped",
            "text": "Pickup is at 4.",
            "idempotency_key": f"action:{action.id}",
        }
    ]
    assert resolved.status == PendingActionStatus.EXECUTED


def test_unsupported_approved_action_fails_closed_and_audits(tmp_path):
    service, _agent = _service(tmp_path)
    sender = FakeSender()
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.create_pending_action(
        chat_id="execute-unsupported",
        action_type="email_teacher",
        summary="Email the teacher.",
        payload={"body": "We will be late."},
        now_utc=now,
    )
    action = service.pending_actions(chat_id="execute-unsupported", now_utc=now)[0]
    service.handle_incoming(
        _incoming(
            f"approve {action.id[:8]}",
            chat_id="execute-unsupported",
            message_id="unsupported-approve",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    result = run_approved_actions(
        store=service.store,
        sender=sender,
        now_utc=now + timedelta(minutes=2),
    )
    household = service.store.get_household_by_chat("execute-unsupported")
    assert household is not None
    execution = service.store.list_action_executions(household_id=household.id)[0]
    resolved = service.store.list_pending_actions(
        household_id=household.id,
        now_utc=now + timedelta(minutes=2),
        include_resolved=True,
    )[0]

    assert result.failed == 1
    assert sender.sent == []
    assert execution.status == ActionExecutionStatus.FAILED
    assert "unsupported action type" in (execution.error or "")
    assert resolved.status == PendingActionStatus.FAILED


def test_approved_stale_reminder_action_fails_without_creating_reminder(tmp_path):
    service, _agent = _service(tmp_path)
    sender = FakeSender()
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    stale_due_at = now - timedelta(minutes=5)
    service.create_pending_action(
        chat_id="execute-stale-reminder",
        action_type="create_reminder",
        summary="Add reminder: stale reminder",
        payload={
            "title": "stale reminder",
            "due_at_utc": stale_due_at.isoformat(),
        },
        now_utc=now,
        expires_at_utc=now + timedelta(hours=1),
    )
    action = service.pending_actions(chat_id="execute-stale-reminder", now_utc=now)[0]
    service.handle_incoming(
        _incoming(
            f"approve {action.id[:8]}",
            chat_id="execute-stale-reminder",
            message_id="stale-reminder-approve",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    result = run_approved_actions(
        store=service.store,
        sender=sender,
        now_utc=now + timedelta(minutes=2),
    )
    household = service.store.get_household_by_chat("execute-stale-reminder")
    assert household is not None
    execution = service.store.list_action_executions(household_id=household.id)[0]
    resolved = service.store.list_pending_actions(
        household_id=household.id,
        now_utc=now + timedelta(minutes=2),
        include_resolved=True,
    )[0]

    assert result.failed == 1
    assert sender.sent == []
    assert execution.status == ActionExecutionStatus.FAILED
    assert "due_at_utc is in the past" in (execution.error or "")
    assert resolved.status == PendingActionStatus.FAILED
    assert service.store.upcoming_reminders(
        household_id=household.id,
        now_utc=now + timedelta(minutes=2),
    ) == []


def test_memory_pause_blocks_new_durable_memory(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    paused = service.handle_incoming(
        _incoming("pause memory", chat_id="privacy-memory", message_id="pause-memory"),
        now_utc=now,
    )
    blocked = service.handle_incoming(
        _incoming(
            "remember that Maya likes noodles",
            chat_id="privacy-memory",
            message_id="remember-blocked",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    snapshot = service.memory_snapshot(
        chat_id="privacy-memory",
        now_utc=now + timedelta(minutes=1),
    )

    assert "Household memory is paused" in paused[0].text
    assert "did not save" in blocked[0].text
    assert snapshot.memories == []


def test_paused_memory_is_excluded_from_agent_context(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming(
            "remember that Maya likes noodles",
            chat_id="privacy-agent",
            message_id="remember-agent",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("pause memory", chat_id="privacy-agent", message_id="pause-agent"),
        now_utc=now + timedelta(minutes=1),
    )
    service.handle_incoming(
        _incoming(
            "What should we make for dinner?",
            chat_id="privacy-agent",
            message_id="agent-question",
        ),
        now_utc=now + timedelta(minutes=2),
    )

    assert agent.calls[-1]["memories"] == []


def test_helper_turn_excludes_durable_memory_from_agent_context(tmp_path):
    service, agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming(
            "remember that Maya likes noodles",
            chat_id="helper-memory-context",
            message_id="remember",
            sender="+15555550100",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "What should we make for dinner?",
            chat_id="helper-memory-context",
            message_id="helper-question",
            sender="+15555550199",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    call = agent.calls[-1]

    assert call["actor"].role.value == "helper"
    assert call["memories"] == []


def test_privacy_controls_are_parent_only(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("hello", chat_id="privacy-parent-only", message_id="parent-one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id="privacy-parent-only",
            message_id="parent-two",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id="privacy-parent-only",
            message_id="helper-seen",
            sender="+15555550102",
        ),
        now_utc=now,
    )

    reply = service.handle_incoming(
        _incoming(
            "pause memory",
            chat_id="privacy-parent-only",
            message_id="helper-pause",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    privacy = service.privacy_snapshot(
        chat_id="privacy-parent-only",
        now_utc=now + timedelta(minutes=1),
    )

    assert "need one of the parents" in reply[0].text
    assert privacy.memory_enabled is True


def test_privacy_settings_are_household_scoped(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("pause memory", chat_id="privacy-family-a", message_id="family-a-pause"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "remember that Maya likes noodles",
            chat_id="privacy-family-b",
            message_id="family-b-remember",
        ),
        now_utc=now,
    )

    family_a = service.privacy_snapshot(chat_id="privacy-family-a", now_utc=now)
    family_b = service.privacy_snapshot(chat_id="privacy-family-b", now_utc=now)
    family_b_memory = service.memory_snapshot(chat_id="privacy-family-b", now_utc=now)

    assert family_a.memory_enabled is False
    assert family_b.memory_enabled is True
    assert [memory.text for memory in family_b_memory.memories] == ["Maya likes noodles"]


def test_privacy_status_and_analytics_opt_in_are_household_scoped(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming(
            "opt in to product analytics",
            chat_id="privacy-analytics",
            message_id="analytics-on",
        ),
        now_utc=now,
    )
    status = service.handle_incoming(
        _incoming("privacy status", chat_id="privacy-analytics", message_id="privacy-status"),
        now_utc=now + timedelta(minutes=1),
    )
    service.handle_incoming(
        _incoming("my name is Alex", chat_id="privacy-other", message_id="privacy-other-name"),
        now_utc=now,
    )
    other = service.privacy_snapshot(chat_id="privacy-other", now_utc=now)

    assert "Product analytics: on" in status[0].text
    assert "Cross-family memory sharing: off" in status[0].text
    assert other.product_analytics_opt_in is False


def test_setup_status_lists_missing_household_readiness_steps(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("setup", chat_id="setup-new", message_id="setup-new"),
        now_utc=now,
    )

    assert "Household setup:" in outbound[0].text
    assert "Parents seen: 1/2" in outbound[0].text
    assert "Tell me what each parent wants to be called." in outbound[0].text
    assert "Invite or confirm your partner as the second parent." in outbound[0].text
    assert "Next action: Tell me what I should call you." in outbound[0].text


def test_setup_status_suggests_inviting_partner_after_first_parent_is_named(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("my name is Sam", chat_id="setup-invite-next", message_id="name"),
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming("setup status", chat_id="setup-invite-next", message_id="setup"),
        now_utc=now + timedelta(minutes=1),
    )

    assert "Next action: Send your partner's phone number" in outbound[0].text


def test_setup_status_suggests_google_after_family_context_is_present(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("my name is Sam", chat_id="setup-google-next", message_id="parent-one-name"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "confirm partner +15555550101",
            chat_id="setup-google-next",
            message_id="confirm-partner",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id="setup-google-next",
            message_id="parent-two-name",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("our child is Maya", chat_id="setup-google-next", message_id="child"),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming("setup status", chat_id="setup-google-next", message_id="setup"),
        now_utc=now + timedelta(minutes=1),
    )

    assert "Next action: Say you want to connect Google Calendar and Gmail." in outbound[0].text


def test_setup_status_suggests_source_rule_after_connected_source(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("my name is Sam", chat_id="setup-rule-next", message_id="parent-one-name"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "confirm partner +15555550101",
            chat_id="setup-rule-next",
            message_id="confirm-partner",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id="setup-rule-next",
            message_id="parent-two-name",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("our child is Maya", chat_id="setup-rule-next", message_id="child"),
        now_utc=now,
    )
    service.sync_connected_sources(
        chat_id="setup-rule-next",
        provider="google",
        external_account_id="parent@example.com",
        account_label="Parent Gmail",
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming("household status", chat_id="setup-rule-next", message_id="status"),
        now_utc=now + timedelta(minutes=1),
    )

    assert (
        "Setup next action: Tell me one thing that is always worth a text, "
        "like permission slips."
    ) in outbound[0].text


def test_prompted_bare_source_rule_updates_preferences(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("my name is Sam", chat_id="prompted-source-rule", message_id="parent-one-name"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "confirm partner +15555550101",
            chat_id="prompted-source-rule",
            message_id="confirm-partner",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id="prompted-source-rule",
            message_id="parent-two-name",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("our child is Maya", chat_id="prompted-source-rule", message_id="child"),
        now_utc=now,
    )
    service.sync_connected_sources(
        chat_id="prompted-source-rule",
        provider="google",
        external_account_id="parent@example.com",
        account_label="Parent Gmail",
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("household status", chat_id="prompted-source-rule", message_id="status"),
        now_utc=now + timedelta(minutes=1),
    )

    outbound = service.handle_incoming(
        _incoming("permission slips", chat_id="prompted-source-rule", message_id="rule"),
        now_utc=now + timedelta(minutes=2),
    )
    preferences = service.source_preferences(chat_id="prompted-source-rule")

    assert "permission slips" in outbound[0].text
    assert [(item.preference.value, item.phrase) for item in preferences] == [
        ("always_surface", "permission slips")
    ]


def test_child_names_update_readiness_and_memory(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    reply = service.handle_incoming(
        _incoming("our kids are Maya and Leo", chat_id="setup-kids", message_id="kids"),
        now_utc=now,
    )
    readiness = service.readiness_snapshot(chat_id="setup-kids", now_utc=now)
    memory = service.memory_snapshot(chat_id="setup-kids", now_utc=now)

    assert "Maya and Leo" in reply[0].text
    assert readiness.child_count == 2
    assert {item.subject for item in memory.memories} == {"Maya", "Leo"}


def test_natural_child_names_update_readiness_and_memory(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    reply = service.handle_incoming(
        _incoming("we have two kids, Maya and Leo", chat_id="natural-kids", message_id="kids"),
        now_utc=now,
    )
    readiness = service.readiness_snapshot(chat_id="natural-kids", now_utc=now)
    memory = service.memory_snapshot(chat_id="natural-kids", now_utc=now)

    assert "Maya and Leo" in reply[0].text
    assert readiness.child_count == 2
    assert {item.subject for item in memory.memories} == {"Maya", "Leo"}


def test_prompted_bare_child_names_update_readiness_and_memory(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("my name is Sam", chat_id="prompted-kids", message_id="parent-one-name"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("confirm partner +15555550101", chat_id="prompted-kids", message_id="confirm-partner"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("my name is Alex", chat_id="prompted-kids", message_id="parent-two-name", sender="+15555550101"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("setup status", chat_id="prompted-kids", message_id="setup"),
        now_utc=now + timedelta(minutes=1),
    )

    reply = service.handle_incoming(
        _incoming("Maya and Leo", chat_id="prompted-kids", message_id="kids"),
        now_utc=now + timedelta(minutes=2),
    )
    readiness = service.readiness_snapshot(chat_id="prompted-kids", now_utc=now)
    memory = service.memory_snapshot(chat_id="prompted-kids", now_utc=now)

    assert "Maya and Leo" in reply[0].text
    assert readiness.child_count == 2
    assert {item.subject for item in memory.memories} == {"Maya", "Leo"}


def test_parent_can_start_google_connection_from_imessage(tmp_path):
    service, _agent = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("connect google", chat_id="connect-google", message_id="connect-google"),
        now_utc=now,
    )
    parsed = urlparse(outbound[0].text.splitlines()[1])
    query = parse_qs(parsed.query)
    state = query["state"][0]
    consumed = service.store.consume_oauth_state(
        state=state,
        provider="google",
        now_utc=now + timedelta(minutes=1),
    )

    assert "Use this link to connect Google Calendar and Gmail" in outbound[0].text
    assert parsed.netloc == "accounts.google.com"
    assert query["client_id"] == ["google-client-id"]
    assert consumed is not None
    assert consumed.chat_id == "connect-google"


def test_parent_can_start_google_connection_with_natural_text(tmp_path):
    service, _agent = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("can I connect my calendar?", chat_id="connect-google-natural", message_id="connect-google"),
        now_utc=now,
    )

    assert "Use this link to connect Google Calendar and Gmail" in outbound[0].text
    assert "accounts.google.com" in outbound[0].text


def test_google_connection_command_is_parent_only(tmp_path):
    service, _agent = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("hello", chat_id="connect-parent-only", message_id="parent-one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id="connect-parent-only",
            message_id="parent-two",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "hello",
            chat_id="connect-parent-only",
            message_id="helper-seen",
            sender="+15555550102",
        ),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "connect google",
            chat_id="connect-parent-only",
            message_id="helper-connect",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert "need one of the parents" in outbound[0].text


def test_google_connection_command_explains_missing_operator_config(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("connect google", chat_id="connect-missing-config", message_id="connect-missing"),
        now_utc=now,
    )

    assert "not able to make the Google connection link yet" in outbound[0].text
    assert "still need to be configured" in outbound[0].text


def test_parent_can_disconnect_google_from_imessage(tmp_path):
    service, _agent = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household, account = _store_google_connected_account(
        service,
        chat_id="disconnect-google",
        now=now,
    )

    outbound = service.handle_incoming(
        _incoming("disconnect google", chat_id="disconnect-google", message_id="disconnect-google"),
        now_utc=now + timedelta(minutes=1),
    )
    active_accounts = service.store.list_connected_accounts(household.id)
    all_accounts = service.store.list_connected_accounts(household.id, include_disabled=True)

    assert "Google is disconnected" in outbound[0].text
    assert "removed the stored Google token" in outbound[0].text
    assert active_accounts == []
    assert all_accounts[0].status == ConnectedAccountStatus.DISABLED
    assert service.store.get_connected_account_token(account.id) is None


def test_helper_cannot_disconnect_google(tmp_path):
    service, _agent = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.handle_incoming(
        _incoming("hello", chat_id="disconnect-parent-only", message_id="parent-one"),
        now_utc=now,
    )
    household, account = _store_google_connected_account(
        service,
        chat_id="disconnect-parent-only",
        now=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "disconnect google",
            chat_id="disconnect-parent-only",
            message_id="helper-disconnect",
            sender="+15555550101",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert "need one of the parents" in outbound[0].text
    assert service.store.list_connected_accounts(household.id)[0].status == ConnectedAccountStatus.ACTIVE
    assert service.store.get_connected_account_token(account.id) is not None


def test_parent_can_disconnect_google_while_household_is_stopped(tmp_path):
    service, _agent = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household, account = _store_google_connected_account(
        service,
        chat_id="disconnect-stopped",
        now=now,
    )
    service.handle_incoming(
        _incoming("stop", chat_id="disconnect-stopped", message_id="stop"),
        now_utc=now + timedelta(minutes=1),
    )

    outbound = service.handle_incoming(
        _incoming("disconnect google", chat_id="disconnect-stopped", message_id="disconnect"),
        now_utc=now + timedelta(minutes=2),
    )

    assert "Google is disconnected" in outbound[0].text
    assert service.store.list_connected_accounts(household.id) == []
    assert service.store.get_connected_account_token(account.id) is None


def test_google_disconnect_reports_when_no_active_account(tmp_path):
    service, _agent = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    outbound = service.handle_incoming(
        _incoming("disconnect google", chat_id="disconnect-empty", message_id="disconnect-empty"),
        now_utc=now,
    )

    assert "no active Google accounts" in outbound[0].text


def test_connected_source_sync_does_not_reactivate_disconnected_google_account(tmp_path):
    service, _agent = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household, account = _store_google_connected_account(
        service,
        chat_id="sync-after-disconnect",
        now=now,
    )
    service.store.disconnect_connected_accounts(
        household_id=household.id,
        provider="google",
        now_utc=now + timedelta(minutes=1),
    )

    result = service.sync_connected_sources(
        chat_id="sync-after-disconnect",
        provider="google",
        external_account_id="google-sub-123",
        account_label="Parent Google",
        cursor="cursor-after-disconnect",
        now_utc=now + timedelta(minutes=2),
        emails=[
            {
                "external_id": "email-after-disconnect",
                "subject": "Permission slip due",
                "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                "sender": "teacher@example.com",
                "received_at_utc": now.isoformat(),
                "event_at_utc": (now + timedelta(hours=8)).isoformat(),
            }
        ],
    )
    active_accounts = service.store.list_connected_accounts(household.id)
    all_accounts = service.store.list_connected_accounts(household.id, include_disabled=True)
    snapshot = service.source_review_snapshot(chat_id="sync-after-disconnect", now_utc=now)

    assert result.account.id == account.id
    assert result.account.status == ConnectedAccountStatus.DISABLED
    assert result.imported == 0
    assert result.surfaced == 0
    assert result.messages == []
    assert active_accounts == []
    assert all_accounts[0].status == ConnectedAccountStatus.DISABLED
    assert service.store.get_connected_account_token(account.id) is None
    assert snapshot.total == 0


def test_duplicate_source_retry_respects_stopped_household(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    first = service.ingest_source_item(
        chat_id="source-retry-stopped",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        external_id="permission-slip",
        observed_at_utc=now,
        event_at_utc=now + timedelta(hours=8),
        now_utc=now,
        mark_surfaced=False,
    )
    service.prepare_outbound_delivery(first[0], now_utc=now)
    service.mark_outbound_delivery_failed(
        first[0],
        error="RuntimeError: linq send unavailable",
        now_utc=now + timedelta(minutes=1),
    )
    service.handle_incoming(
        _incoming("stop", chat_id="source-retry-stopped", message_id="stop"),
        now_utc=now + timedelta(minutes=2),
    )

    duplicate = service.ingest_source_item(
        chat_id="source-retry-stopped",
        source_type="email",
        title="Permission slip due",
        body="Please sign and bring the permission slip for tomorrow's field trip.",
        external_id="permission-slip",
        observed_at_utc=now + timedelta(minutes=3),
        event_at_utc=now + timedelta(hours=8),
        now_utc=now + timedelta(minutes=3),
        mark_surfaced=False,
    )

    assert duplicate == []


def test_household_readiness_becomes_ready_when_core_setup_is_done(tmp_path):
    service, _agent = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    service.handle_incoming(
        _incoming("my name is Sam", chat_id="setup-ready", message_id="parent-one-name"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "confirm partner +15555550101",
            chat_id="setup-ready",
            message_id="confirm-partner",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id="setup-ready",
            message_id="parent-two-name",
            sender="+15555550101",
        ),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming("our child is Maya", chat_id="setup-ready", message_id="child"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "always tell me about permission slips",
            chat_id="setup-ready",
            message_id="source-rule",
        ),
        now_utc=now,
    )
    service.sync_connected_sources(
        chat_id="setup-ready",
        provider="google",
        external_account_id="parent@example.com",
        account_label="Parent Gmail",
        now_utc=now,
    )

    readiness = service.readiness_snapshot(chat_id="setup-ready", now_utc=now)
    status = service.handle_incoming(
        _incoming("setup status", chat_id="setup-ready", message_id="setup-status"),
        now_utc=now,
    )

    assert readiness.ready is True
    assert readiness.parent_count == 2
    assert readiness.named_parent_count == 2
    assert readiness.child_count == 1
    assert readiness.connected_account_count == 1
    assert readiness.source_preference_count == 1
    assert "Ready for a pilot" in status[0].text


def test_agent_reminder_proposal_becomes_pending_action(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    due_at = now + timedelta(hours=3)
    agent = FakeAgent(
        "I can keep this moving.\n"
        "```florence\n"
        '{"actions":[{"type":"create_reminder","summary":"Add reminder: Pack cleats",'
        f'"payload":{{"title":"Pack cleats","due_at_utc":"{due_at.isoformat()}"}}}}]}}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )

    outbound = service.handle_incoming(
        _incoming("Can you help us remember soccer prep?", chat_id="agent-action"),
        now_utc=now,
    )
    action = service.pending_actions(chat_id="agent-action", now_utc=now)[0]

    assert len(outbound) == 2
    assert outbound[0].text == "I can keep this moving."
    assert "```" not in outbound[0].text
    assert "approve" in outbound[1].text
    assert action.action_type == "create_reminder"
    assert action.summary == "Add reminder: Pack cleats"
    assert action.payload["title"] == "Pack cleats"
    assert action.payload["due_at_utc"] == due_at.isoformat()
    assert agent.calls[0]["now_utc"] == now


def test_agent_reminder_overclaim_is_replaced_before_parent_approval(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    due_at = now + timedelta(hours=3)
    agent = FakeAgent(
        "I set a reminder for cleats.\n"
        "```florence\n"
        '{"actions":[{"type":"create_reminder","summary":"Add reminder: Pack cleats",'
        f'"payload":{{"title":"Pack cleats","due_at_utc":"{due_at.isoformat()}"}}}}]}}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )

    outbound = service.handle_incoming(
        _incoming("Can you help us remember soccer prep?", chat_id="agent-action-overclaim"),
        now_utc=now,
    )

    assert len(outbound) == 2
    assert outbound[0].text == "I can help with that."
    assert "set a reminder" not in outbound[0].text.lower()
    assert "approve" in outbound[1].text
    assert len(service.pending_actions(chat_id="agent-action-overclaim", now_utc=now)) == 1


def test_agent_past_reminder_proposal_is_ignored(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    past = now - timedelta(hours=3)
    agent = FakeAgent(
        "I will keep an eye on it.\n"
        "```florence\n"
        '{"actions":[{"type":"create_reminder","payload":'
        f'{{"title":"Past task","due_at_utc":"{past.isoformat()}"}}}}]}}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )

    outbound = service.handle_incoming(
        _incoming("Could you track this?", chat_id="agent-past-action"),
        now_utc=now,
    )

    assert [message.text for message in outbound] == ["I will keep an eye on it."]
    assert service.pending_actions(chat_id="agent-past-action", now_utc=now) == []


def test_agent_contact_detail_reminder_proposal_is_ignored_and_guarded(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    due_at = now + timedelta(hours=3)
    agent = FakeAgent(
        "I set a reminder to call [phone number].\n"
        "```florence\n"
        '{"actions":[{"type":"create_reminder","payload":'
        f'{{"title":"Call [phone number]","due_at_utc":"{due_at.isoformat()}"}}}}]}}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )

    outbound = service.handle_incoming(
        _incoming("Could you help with my backup contact?", chat_id="agent-contact-action"),
        now_utc=now,
    )

    assert [message.text for message in outbound] == [
        "I hear you. I did not make any household changes from that."
    ]
    assert service.pending_actions(chat_id="agent-contact-action", now_utc=now) == []


def test_agent_memory_proposal_is_saved_when_enabled(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    agent = FakeAgent(
        "Got it.\n"
        "```florence\n"
        '{"memories":[{"kind":"preference","subject":"Maya",'
        '"text":"Maya likes noodles.","confidence":0.7}]}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )

    outbound = service.handle_incoming(
        _incoming("Maya likes noodles.", chat_id="agent-memory", message_id="memory-source"),
        now_utc=now,
    )
    snapshot = service.memory_snapshot(chat_id="agent-memory", now_utc=now)

    assert [message.text for message in outbound] == ["Got it."]
    assert len(snapshot.memories) == 1
    assert snapshot.memories[0].text == "Maya likes noodles."
    assert snapshot.memories[0].source_message_id == "memory-source"


def test_agent_oversized_memory_proposal_is_ignored(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    long_memory = "Maya " + ("has a very detailed temporary preference " * 8)
    agent = FakeAgent(
        "Got it.\n"
        "```florence\n"
        f'{{"memories":[{{"kind":"preference","text":"{long_memory}"}}]}}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )

    outbound = service.handle_incoming(
        _incoming("Maya has a lot of temporary lunch details.", chat_id="agent-memory-long"),
        now_utc=now,
    )
    snapshot = service.memory_snapshot(chat_id="agent-memory-long", now_utc=now)

    assert [message.text for message in outbound] == ["Got it."]
    assert snapshot.memories == []


def test_rejected_agent_memory_overclaim_is_replaced(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    agent = FakeAgent(
        "I remembered Grandma's backup phone.\n"
        "```florence\n"
        '{"memories":[{"kind":"fact","text":"Grandma backup phone is +15555550123."}]}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )

    outbound = service.handle_incoming(
        _incoming("Grandma can help with pickup.", chat_id="agent-memory-unsafe-overclaim"),
        now_utc=now,
    )
    snapshot = service.memory_snapshot(chat_id="agent-memory-unsafe-overclaim", now_utc=now)

    assert [message.text for message in outbound] == [
        "I hear you. I did not make any household changes from that."
    ]
    assert snapshot.memories == []


def test_agent_memory_proposal_respects_paused_memory(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    agent = FakeAgent(
        "Understood.\n"
        "```florence\n"
        '{"memories":[{"kind":"preference","text":"Maya likes noodles."}]}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )

    service.handle_incoming(
        _incoming("pause memory", chat_id="agent-memory-paused", message_id="pause"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "Maya likes noodles.",
            chat_id="agent-memory-paused",
            message_id="memory-paused-source",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    snapshot = service.memory_snapshot(
        chat_id="agent-memory-paused",
        now_utc=now + timedelta(minutes=1),
    )

    assert snapshot.memories == []


def test_agent_paused_memory_overclaim_is_replaced(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    agent = FakeAgent(
        "I remembered that.\n"
        "```florence\n"
        '{"memories":[{"kind":"preference","text":"Maya likes noodles."}]}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )

    service.handle_incoming(
        _incoming("pause memory", chat_id="agent-memory-paused-overclaim", message_id="pause"),
        now_utc=now,
    )
    outbound = service.handle_incoming(
        _incoming(
            "Maya likes noodles.",
            chat_id="agent-memory-paused-overclaim",
            message_id="memory-paused-overclaim-source",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    snapshot = service.memory_snapshot(
        chat_id="agent-memory-paused-overclaim",
        now_utc=now + timedelta(minutes=1),
    )

    assert [message.text for message in outbound] == [
        "I hear you. I did not make any household changes from that."
    ]
    assert snapshot.memories == []


def test_agent_memory_proposal_from_helper_is_ignored(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    agent = FakeAgent(
        "Understood.\n"
        "```florence\n"
        '{"memories":[{"kind":"preference","text":"Maya likes noodles."}]}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )
    service.handle_incoming(
        _incoming("my name is Sam", chat_id="agent-helper-memory", message_id="parent-one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id="agent-helper-memory",
            message_id="parent-two",
            sender="+15555550101",
        ),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "Maya likes noodles.",
            chat_id="agent-helper-memory",
            message_id="helper-memory-source",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=1),
    )
    snapshot = service.memory_snapshot(
        chat_id="agent-helper-memory",
        now_utc=now + timedelta(minutes=1),
    )

    assert [message.text for message in outbound] == ["Understood."]
    assert snapshot.memories == []


def test_agent_source_preference_proposal_is_saved(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    agent = FakeAgent(
        "I will watch for those.\n"
        "```florence\n"
        '{"source_preferences":[{"preference":"always_surface","phrase":"Permission slips"}]}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )

    outbound = service.handle_incoming(
        _incoming(
            "Please keep an eye out for permission slips.",
            chat_id="agent-source-pref",
            message_id="agent-source-pref-message",
        ),
        now_utc=now,
    )
    preferences = service.source_preferences(chat_id="agent-source-pref")

    assert [message.text for message in outbound] == ["I will watch for those."]
    assert len(preferences) == 1
    assert preferences[0].phrase == "permission slips"
    assert preferences[0].preference == SourcePreferenceKind.ALWAYS_SURFACE
    assert agent.calls[0]["now_utc"] == now


def test_agent_source_preference_proposal_from_helper_is_ignored(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    agent = FakeAgent(
        "I will keep it in mind.\n"
        "```florence\n"
        '{"source_preferences":[{"preference":"mute","phrase":"newsletters"}]}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )
    service.handle_incoming(
        _incoming("my name is Sam", chat_id="agent-helper-source-pref", message_id="parent-one"),
        now_utc=now,
    )
    service.handle_incoming(
        _incoming(
            "my name is Alex",
            chat_id="agent-helper-source-pref",
            message_id="parent-two",
            sender="+15555550101",
        ),
        now_utc=now,
    )

    outbound = service.handle_incoming(
        _incoming(
            "Please mute newsletters.",
            chat_id="agent-helper-source-pref",
            message_id="helper-agent-pref",
            sender="+15555550102",
        ),
        now_utc=now + timedelta(minutes=1),
    )

    assert [message.text for message in outbound] == [
        "I hear you. I did not make any household changes from that."
    ]
    assert service.source_preferences(chat_id="agent-helper-source-pref") == []


def test_invalid_agent_source_preference_proposal_is_ignored(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    agent = FakeAgent(
        "I will stay selective.\n"
        "```florence\n"
        '{"source_preferences":[{"preference":"sometimes","phrase":"cafeteria menu"}]}\n'
        "```"
    )
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "florence.sqlite")),
        agent=agent,
    )

    outbound = service.handle_incoming(
        _incoming("Please be selective.", chat_id="agent-source-pref-invalid"),
        now_utc=now,
    )

    assert [message.text for message in outbound] == ["I will stay selective."]
    assert service.source_preferences(chat_id="agent-source-pref-invalid") == []
