import base64
import sqlite3
from datetime import datetime, timedelta, timezone

import httpx

from florence.config import Settings
from florence.linq import LinqClient
from florence.models import (
    ConnectedAccountStatus,
    IncomingMessage,
    OutboundDeliveryStatus,
    OutboundMessage,
)
from florence.oauth import TokenVault
from florence.service import FlorenceService
from florence.source_providers import (
    GoogleSourceProvider,
    ProviderBatch,
    SourceSyncRunResult,
    run_connected_source_sync,
)
from florence.store import Store
from florence.worker import run_source_sync_tick


class FakeAgent:
    def complete(self, **kwargs):
        return "Fake agent reply."


class FakeProvider:
    provider = "google"

    def __init__(self, batch: ProviderBatch):
        self.batch = batch
        self.seen_accounts = []

    def fetch(self, account, *, now_utc: datetime) -> ProviderBatch:
        self.seen_accounts.append(account)
        return self.batch


class FailingProvider:
    provider = "google"

    def fetch(self, account, *, now_utc: datetime) -> ProviderBatch:
        raise RuntimeError("provider unavailable")


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


class ProductionLikeStore(Store):
    def __init__(self, path: str):
        super().__init__(path)
        self.backend = "postgres"

    def connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn


def _service(tmp_path):
    settings = Settings(db_path=str(tmp_path / "florence.sqlite"))
    return FlorenceService(settings=settings, agent=FakeAgent())


def _google_service(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
        token_encryption_key=TokenVault.generate_key(),
    )
    return FlorenceService(settings=settings, agent=FakeAgent())


def _postgres_like_service(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "florence.sqlite"),
        database_url="postgresql://florence:secret@db:5432/florence",
        linq_api_key="linq-api-key",
    )
    store = ProductionLikeStore(settings.db_path)
    return FlorenceService(settings=settings, store=store, agent=FakeAgent())


def _store_google_token(service, *, chat_id: str, now: datetime, payload: dict[str, object]):
    household = service.store.get_or_create_household(
        chat_id=chat_id,
        timezone_name=service.settings.default_timezone,
        now_utc=now,
    )
    account = service.store.upsert_connected_account(
        household_id=household.id,
        provider="google",
        external_account_id="google-sub-123",
        account_label="parent@example.com",
        now_utc=now,
    )
    vault = TokenVault.from_settings(service.settings)
    service.store.upsert_connected_account_token(
        connected_account_id=account.id,
        provider="google",
        token_ciphertext=vault.encrypt(payload),
        scopes=("openid", "email"),
        expires_at_utc=datetime.fromisoformat(str(payload["expires_at_utc"])),
        now_utc=now,
    )
    return account


def test_provider_runner_syncs_active_connected_account(tmp_path):
    service = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="provider-sync",
        provider="google",
        external_account_id="parent@example.com",
        account_label="Parent Gmail",
        cursor="cursor-1",
        now_utc=now,
    )
    provider = FakeProvider(
        ProviderBatch(
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
            calendar_events=[],
            cursor="cursor-2",
        )
    )

    result = run_connected_source_sync(
        service=service,
        providers={"google": provider},
        now_utc=now + timedelta(minutes=5),
    )
    account = service.connected_accounts(chat_id="provider-sync")[0]
    snapshot = service.source_review_snapshot(chat_id="provider-sync", now_utc=now)

    assert result.checked == 1
    assert result.synced == 1
    assert result.imported == 1
    assert result.surfaced == 1
    assert provider.seen_accounts[0].cursor == "cursor-1"
    assert account.cursor == "cursor-2"
    assert account.last_synced_at_utc == now + timedelta(minutes=5)
    assert snapshot.surfaced == 1
    assert snapshot.connected_total == 1
    assert snapshot.connected_surfaced == 1


def test_provider_runner_skips_import_if_account_is_disconnected_during_fetch(tmp_path):
    service = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="provider-disconnect-race",
        provider="google",
        external_account_id="parent@example.com",
        account_label="Parent Gmail",
        cursor="cursor-1",
        now_utc=now,
    )

    class DisconnectingProvider:
        provider = "google"

        def fetch(self, account, *, now_utc: datetime) -> ProviderBatch:
            service.store.disconnect_connected_accounts(
                household_id=account.household_id,
                provider=account.provider,
                now_utc=now_utc,
            )
            return ProviderBatch(
                emails=[
                    {
                        "external_id": "email-after-disconnect",
                        "subject": "Permission slip due",
                        "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                        "sender": "teacher@example.com",
                        "received_at_utc": now_utc.isoformat(),
                        "event_at_utc": (now_utc + timedelta(hours=8)).isoformat(),
                    }
                ],
                calendar_events=[],
                cursor="cursor-2",
            )

    result = run_connected_source_sync(
        service=service,
        providers={"google": DisconnectingProvider()},
        now_utc=now + timedelta(minutes=5),
    )
    household = service.store.get_household_by_chat("provider-disconnect-race")
    assert household is not None
    active_accounts = service.store.list_connected_accounts(household.id)
    all_accounts = service.store.list_connected_accounts(household.id, include_disabled=True)
    snapshot = service.source_review_snapshot(chat_id="provider-disconnect-race", now_utc=now)

    assert result.checked == 1
    assert result.synced == 1
    assert result.imported == 0
    assert result.surfaced == 0
    assert not result.messages
    assert active_accounts == []
    assert all_accounts[0].status == ConnectedAccountStatus.DISABLED
    assert all_accounts[0].cursor == "cursor-1"
    assert snapshot.total == 0


def test_worker_source_sync_tick_sends_surfaced_messages(tmp_path):
    service = _service(tmp_path)
    sender = FakeSender()
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="provider-send",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
    )
    provider = FakeProvider(
        ProviderBatch(
            emails=[
                {
                    "external_id": "email-action",
                    "subject": "Permission slip due",
                    "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                    "sender": "teacher@example.com",
                    "received_at_utc": (now + timedelta(minutes=5)).isoformat(),
                    "event_at_utc": (now + timedelta(hours=8)).isoformat(),
                }
            ],
            calendar_events=[],
            cursor="cursor-2",
        )
    )

    result = run_source_sync_tick(
        service,
        providers={"google": provider},
        sender=sender,
        now_utc=now + timedelta(minutes=5),
    )

    assert result.surfaced == 1
    assert len(result.messages) == 1
    assert sender.sent[0]["chat_id"] == "provider-send"
    assert "Permission slip due" in sender.sent[0]["text"]
    assert sender.sent[0]["idempotency_key"] == result.messages[0].idempotency_key


def test_worker_records_google_live_verification_after_default_sync_and_real_linq_delivery(
    tmp_path,
    monkeypatch,
):
    service = _postgres_like_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.store.get_or_create_household(
        chat_id="google-live-chat",
        timezone_name="America/Los_Angeles",
        now_utc=now,
    )
    message = OutboundMessage(
        chat_id="google-live-chat",
        text="Permission slip due tomorrow. Reply approve ABC123.",
        idempotency_key="source:google-live-item",
    )

    def fake_sync(**kwargs):
        assert kwargs["providers"] is None
        assert kwargs["mark_surfaced"] is False
        return SourceSyncRunResult(
            checked=1,
            synced=1,
            imported=1,
            surfaced=1,
            skipped=0,
            failed=0,
            messages=(message,),
        )

    monkeypatch.setattr("florence.worker.run_connected_source_sync", fake_sync)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"message": {"id": "google-live-outbound"}})

    sender = LinqClient(
        service.settings,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = run_source_sync_tick(service, sender=sender, now_utc=now)

    assert result.delivery_sent == 1
    assert requests[0].url.path == "/api/partner/v3/chats/google-live-chat/messages"
    assert service.store.list_live_verifications()["google"] == {
        "name": "google",
        "verified_at_utc": "2026-06-05T16:00:00+00:00",
        "proof": "Google OAuth source sync imported and surfaced a token-backed item",
        "source": "source_sync_worker",
        "updated_at_utc": "2026-06-05T16:00:00+00:00",
    }


def test_worker_does_not_record_google_live_verification_for_fake_provider_path(tmp_path):
    service = _postgres_like_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="google-fake-provider-chat",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
    )
    provider = FakeProvider(
        ProviderBatch(
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
            calendar_events=[],
            cursor="cursor-2",
        )
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"message": {"id": "fake-provider-outbound"}})

    sender = LinqClient(
        service.settings,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = run_source_sync_tick(
        service,
        providers={"google": provider},
        sender=sender,
        now_utc=now + timedelta(minutes=5),
    )

    assert result.delivery_sent == 1
    assert requests
    assert service.store.list_live_verifications() == {}


def test_worker_source_sync_retries_failed_surface_after_cursor_advances(tmp_path):
    service = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="provider-send-retry",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
    )
    provider = FakeProvider(
        ProviderBatch(
            emails=[
                {
                    "external_id": "email-action",
                    "subject": "Permission slip due",
                    "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                    "sender": "teacher@example.com",
                    "received_at_utc": (now + timedelta(minutes=5)).isoformat(),
                    "event_at_utc": (now + timedelta(hours=8)).isoformat(),
                }
            ],
            calendar_events=[],
            cursor="cursor-2",
        )
    )

    failed_result = run_source_sync_tick(
        service,
        providers={"google": provider},
        sender=FlakySender(fail_send=1),
        now_utc=now + timedelta(minutes=5),
    )
    account_after_failure = service.connected_accounts(chat_id="provider-send-retry")[0]
    with service.store.connect() as conn:
        failed_delivery = conn.execute(
            """
            SELECT idempotency_key, delivery_status, attempts
            FROM outbound_deliveries
            WHERE source_message_id LIKE 'source:%'
            """,
        ).fetchone()
        source_count_after_failure = conn.execute(
            """
            SELECT COUNT(*) AS count FROM source_items
            WHERE household_id = ?
            """,
            (account_after_failure.household_id,),
        ).fetchone()["count"]
        failed_source = conn.execute(
            """
            SELECT id, surfaced_at_utc FROM source_items
            WHERE household_id = ?
            """,
            (account_after_failure.household_id,),
        ).fetchone()
    feedback_before_retry = service.handle_incoming(
        IncomingMessage(
            chat_id="provider-send-retry",
            message_id="feedback-before-retry",
            sender="+15555550100",
            text="not useful",
            received_at=now + timedelta(minutes=5, seconds=30),
        ),
        now_utc=now + timedelta(minutes=5, seconds=30),
    )

    retry_sender = FakeSender()
    retry_result = run_source_sync_tick(
        service,
        providers={},
        sender=retry_sender,
        now_utc=now + timedelta(minutes=6),
    )
    with service.store.connect() as conn:
        sent_delivery = conn.execute(
            """
            SELECT delivery_status, attempts
            FROM outbound_deliveries
            WHERE idempotency_key = ?
            """,
            (failed_delivery["idempotency_key"],),
        ).fetchone()
        source_count_after_retry = conn.execute(
            """
            SELECT COUNT(*) AS count FROM source_items
            WHERE household_id = ?
            """,
            (account_after_failure.household_id,),
        ).fetchone()["count"]
        delivered_source = conn.execute(
            """
            SELECT surfaced_at_utc FROM source_items
            WHERE id = ?
            """,
            (failed_source["id"],),
        ).fetchone()

    assert account_after_failure.cursor == "cursor-2"
    assert failed_result.delivery_attempted == 1
    assert failed_result.delivery_sent == 0
    assert failed_result.delivery_failed == 1
    assert failed_delivery["delivery_status"] == OutboundDeliveryStatus.FAILED.value
    assert failed_delivery["attempts"] == 1
    assert source_count_after_failure == 1
    assert failed_source["surfaced_at_utc"] is None
    assert feedback_before_retry[0].text == "Fake agent reply."
    assert retry_result.surfaced == 0
    assert retry_sender.sent[0]["idempotency_key"] == failed_delivery["idempotency_key"]
    assert "Permission slip due" in retry_sender.sent[0]["text"]
    assert sent_delivery["delivery_status"] == OutboundDeliveryStatus.SENT.value
    assert sent_delivery["attempts"] == 2
    assert source_count_after_retry == 1
    assert delivered_source["surfaced_at_utc"] is not None


def test_worker_source_sync_continues_after_one_delivery_fails(tmp_path):
    service = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="provider-send-partial",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
    )
    provider = FakeProvider(
        ProviderBatch(
            emails=[
                {
                    "external_id": "first-email-action",
                    "subject": "First permission slip due",
                    "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                    "sender": "teacher@example.com",
                    "received_at_utc": (now + timedelta(minutes=5)).isoformat(),
                    "event_at_utc": (now + timedelta(hours=8)).isoformat(),
                },
                {
                    "external_id": "second-email-action",
                    "subject": "Second permission slip due",
                    "body": "Please sign and bring the permission slip for Monday's field trip.",
                    "sender": "teacher@example.com",
                    "received_at_utc": (now + timedelta(minutes=6)).isoformat(),
                    "event_at_utc": (now + timedelta(hours=9)).isoformat(),
                },
            ],
            calendar_events=[],
            cursor="cursor-2",
        )
    )
    sender = FlakySender(fail_send=1)

    result = run_source_sync_tick(
        service,
        providers={"google": provider},
        sender=sender,
        now_utc=now + timedelta(minutes=5),
    )
    account = service.connected_accounts(chat_id="provider-send-partial")[0]
    with service.store.connect() as conn:
        delivery_rows = conn.execute(
            """
            SELECT idempotency_key, delivery_status, attempts
            FROM outbound_deliveries
            WHERE household_id = ?
            """,
            (account.household_id,),
        ).fetchall()
        source_rows = conn.execute(
            """
            SELECT title, surfaced_at_utc
            FROM source_items
            WHERE household_id = ?
            ORDER BY observed_at_utc ASC
            """,
            (account.household_id,),
        ).fetchall()

    assert result.delivery_attempted == 2
    assert result.delivery_sent == 1
    assert result.delivery_failed == 1
    assert len(sender.sent) == 1
    assert "Second permission slip due" in sender.sent[0]["text"]
    assert "First permission slip due" not in sender.sent[0]["text"]
    delivery_by_key = {row["idempotency_key"]: row for row in delivery_rows}
    sent_delivery = delivery_by_key.pop(sender.sent[0]["idempotency_key"])
    failed_delivery = next(iter(delivery_by_key.values()))
    assert sent_delivery["delivery_status"] == OutboundDeliveryStatus.SENT.value
    assert sent_delivery["attempts"] == 1
    assert failed_delivery["delivery_status"] == OutboundDeliveryStatus.FAILED.value
    assert failed_delivery["attempts"] == 1
    assert [row["title"] for row in source_rows] == [
        "First permission slip due",
        "Second permission slip due",
    ]
    assert source_rows[0]["surfaced_at_utc"] is None
    assert source_rows[1]["surfaced_at_utc"] is not None


def test_worker_does_not_retry_source_delivery_after_google_disconnect(tmp_path):
    service = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="provider-send-disconnect",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
    )
    provider = FakeProvider(
        ProviderBatch(
            emails=[
                {
                    "external_id": "email-action",
                    "subject": "Permission slip due",
                    "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                    "sender": "teacher@example.com",
                    "received_at_utc": (now + timedelta(minutes=5)).isoformat(),
                    "event_at_utc": (now + timedelta(hours=8)).isoformat(),
                }
            ],
            calendar_events=[],
            cursor="cursor-2",
        )
    )

    failed_result = run_source_sync_tick(
        service,
        providers={"google": provider},
        sender=FlakySender(fail_send=1),
        now_utc=now + timedelta(minutes=5),
    )
    account = service.connected_accounts(chat_id="provider-send-disconnect")[0]
    service.store.disconnect_connected_accounts(
        household_id=account.household_id,
        provider="google",
        now_utc=now + timedelta(minutes=6),
    )
    retry_sender = FakeSender()

    run_source_sync_tick(
        service,
        providers={},
        sender=retry_sender,
        now_utc=now + timedelta(minutes=7),
    )
    with service.store.connect() as conn:
        delivery = conn.execute(
            """
            SELECT delivery_status, attempts, last_error
            FROM outbound_deliveries
            WHERE source_message_id LIKE 'source:%'
            """,
        ).fetchone()

    assert failed_result.delivery_failed == 1
    assert retry_sender.sent == []
    assert delivery["delivery_status"] == OutboundDeliveryStatus.CANCELED.value
    assert delivery["attempts"] == 1
    assert delivery["last_error"] == "canceled after google disconnect"


def test_worker_source_sync_prepares_new_source_messages_before_retry_send_failure(tmp_path):
    service = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="provider-send-prepare",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
    )
    first_provider = FakeProvider(
        ProviderBatch(
            emails=[
                {
                    "external_id": "old-action",
                    "subject": "Old permission slip due",
                    "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                    "sender": "teacher@example.com",
                    "received_at_utc": (now + timedelta(minutes=5)).isoformat(),
                    "event_at_utc": (now + timedelta(hours=8)).isoformat(),
                }
            ],
            calendar_events=[],
            cursor="cursor-2",
        )
    )
    first_failed_result = run_source_sync_tick(
        service,
        providers={"google": first_provider},
        sender=FlakySender(fail_send=1),
        now_utc=now + timedelta(minutes=5),
    )
    second_provider = FakeProvider(
        ProviderBatch(
            emails=[
                {
                    "external_id": "new-action",
                    "subject": "New permission slip due",
                    "body": "Please sign and bring the permission slip for Monday's field trip.",
                    "sender": "teacher@example.com",
                    "received_at_utc": (now + timedelta(minutes=6)).isoformat(),
                    "event_at_utc": (now + timedelta(hours=9)).isoformat(),
                }
            ],
            calendar_events=[],
            cursor="cursor-3",
        )
    )

    second_failed_result = run_source_sync_tick(
        service,
        providers={"google": second_provider},
        sender=FlakySender(fail_send=2),
        now_utc=now + timedelta(minutes=6),
    )
    account = service.connected_accounts(chat_id="provider-send-prepare")[0]
    with service.store.connect() as conn:
        delivery_rows = conn.execute(
            """
            SELECT delivery_status, attempts
            FROM outbound_deliveries
            WHERE household_id = ?
            ORDER BY created_at_utc ASC
            """,
            (account.household_id,),
        ).fetchall()
        source_rows = conn.execute(
            """
            SELECT title, surfaced_at_utc
            FROM source_items
            WHERE household_id = ?
            ORDER BY observed_at_utc ASC
            """,
            (account.household_id,),
        ).fetchall()

    assert account.cursor == "cursor-3"
    assert first_failed_result.delivery_failed == 1
    assert second_failed_result.delivery_attempted == 2
    assert second_failed_result.delivery_sent == 0
    assert second_failed_result.delivery_failed == 2
    assert len(source_rows) == 2
    assert [row["title"] for row in source_rows] == [
        "Old permission slip due",
        "New permission slip due",
    ]
    assert [row["surfaced_at_utc"] for row in source_rows] == [None, None]
    assert len(delivery_rows) == 2
    assert [row["delivery_status"] for row in delivery_rows] == [
        OutboundDeliveryStatus.FAILED.value,
        OutboundDeliveryStatus.FAILED.value,
    ]
    assert [row["attempts"] for row in delivery_rows] == [2, 1]


def test_provider_runner_skips_accounts_without_provider(tmp_path):
    service = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="provider-missing",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
    )

    result = run_connected_source_sync(
        service=service,
        providers={},
        now_utc=now + timedelta(minutes=5),
    )
    account = service.connected_accounts(chat_id="provider-missing")[0]

    assert result.checked == 1
    assert result.synced == 0
    assert result.skipped == 1
    assert account.cursor == "cursor-1"
    assert account.last_synced_at_utc == now


def test_provider_runner_records_failures_without_advancing_cursor(tmp_path):
    service = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="provider-failure",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
    )

    result = run_connected_source_sync(
        service=service,
        providers={"google": FailingProvider()},
        now_utc=now + timedelta(minutes=5),
    )
    account = service.connected_accounts(chat_id="provider-failure")[0]

    assert result.checked == 1
    assert result.failed == 1
    assert result.synced == 0
    assert account.cursor == "cursor-1"
    assert account.last_synced_at_utc == now
    assert account.sync_failure_count == 1
    assert account.last_sync_error == "provider unavailable"
    assert account.retry_after_utc == now + timedelta(minutes=10)


def test_provider_runner_respects_backoff_after_failure(tmp_path):
    service = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="provider-backoff",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
    )

    first = run_connected_source_sync(
        service=service,
        providers={"google": FailingProvider()},
        now_utc=now + timedelta(minutes=5),
    )
    second = run_connected_source_sync(
        service=service,
        providers={"google": FailingProvider()},
        now_utc=now + timedelta(minutes=9),
    )
    account = service.connected_accounts(chat_id="provider-backoff")[0]

    assert first.checked == 1
    assert first.failed == 1
    assert second.checked == 0
    assert second.failed == 0
    assert account.sync_failure_count == 1
    assert account.cursor == "cursor-1"


def test_provider_runner_success_after_backoff_clears_failure_state(tmp_path):
    service = _service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    service.sync_connected_sources(
        chat_id="provider-recovery",
        provider="google",
        external_account_id="parent@example.com",
        cursor="cursor-1",
        now_utc=now,
    )
    run_connected_source_sync(
        service=service,
        providers={"google": FailingProvider()},
        now_utc=now + timedelta(minutes=5),
    )
    provider = FakeProvider(ProviderBatch(emails=[], calendar_events=[], cursor="cursor-2"))

    result = run_connected_source_sync(
        service=service,
        providers={"google": provider},
        now_utc=now + timedelta(minutes=11),
    )
    account = service.connected_accounts(chat_id="provider-recovery")[0]

    assert result.checked == 1
    assert result.synced == 1
    assert account.cursor == "cursor-2"
    assert account.sync_failure_count == 0
    assert account.last_sync_error is None
    assert account.retry_after_utc is None


def test_google_provider_fetches_recent_gmail_and_calendar_items(tmp_path):
    service = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    account = _store_google_token(
        service,
        chat_id="google-family",
        now=now,
        payload={
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "scope": "openid email",
            "expires_at_utc": (now + timedelta(hours=1)).isoformat(),
        },
    )
    message_time = int(now.timestamp() * 1000)
    event_start = now + timedelta(hours=3)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer access-token"
        if request.url.host == "gmail.googleapis.com" and request.url.path.endswith("/messages"):
            return httpx.Response(200, json={"messages": [{"id": "message-1"}]})
        if request.url.host == "gmail.googleapis.com" and request.url.path.endswith("/message-1"):
            return httpx.Response(
                200,
                json={
                    "id": "message-1",
                    "internalDate": str(message_time),
                    "snippet": "Please sign the permission slip.",
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Permission slip due"},
                            {"name": "From", "value": "teacher@example.com"},
                        ]
                    },
                },
            )
        if request.url.host == "www.googleapis.com" and request.url.path.endswith("/events"):
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": "event-1",
                            "summary": "Pediatrician",
                            "start": {"dateTime": event_start.isoformat()},
                            "end": {"dateTime": (event_start + timedelta(minutes=30)).isoformat()},
                            "location": "Clinic",
                        }
                    ]
                },
            )
        raise AssertionError(f"unexpected request: {request.url}")

    provider = GoogleSourceProvider(
        settings=service.settings,
        store=service.store,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    batch = provider.fetch(account, now_utc=now)

    assert batch.cursor is not None
    assert batch.emails == [
        {
            "external_id": "message-1",
            "subject": "Permission slip due",
            "sender": "teacher@example.com",
            "body": "Please sign the permission slip.",
            "received_at_utc": now.isoformat(),
        }
    ]
    assert batch.calendar_events[0]["external_id"] == "event-1"
    assert batch.calendar_events[0]["title"] == "Pediatrician"
    assert batch.calendar_events[0]["starts_at_utc"] == event_start.isoformat()
    assert batch.calendar_events[0]["location"] == "Clinic"


def test_google_provider_searches_gmail_with_full_body(tmp_path):
    service = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    account = _store_google_token(
        service,
        chat_id="google-search-family",
        now=now,
        payload={
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "scope": "openid email",
            "expires_at_utc": (now + timedelta(hours=1)).isoformat(),
        },
    )
    message_time = int(now.timestamp() * 1000)
    body = base64.urlsafe_b64encode(b"LAX to Cleveland. Cleveland to Paris.").decode()
    queries = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer access-token"
        if request.url.host == "gmail.googleapis.com" and request.url.path.endswith("/messages"):
            queries.append(request.url.params.get("q"))
            return httpx.Response(200, json={"messages": [{"id": "message-1"}]})
        if request.url.host == "gmail.googleapis.com" and request.url.path.endswith("/message-1"):
            return httpx.Response(
                200,
                json={
                    "id": "message-1",
                    "internalDate": str(message_time),
                    "payload": {
                        "mimeType": "text/plain",
                        "body": {"data": body},
                        "headers": [
                            {"name": "Subject", "value": "American Airlines trip confirmation"},
                            {"name": "From", "value": "American Airlines <no-reply@aa.com>"},
                        ],
                    },
                },
            )
        raise AssertionError(f"unexpected request: {request.url}")

    provider = GoogleSourceProvider(
        settings=service.settings,
        store=service.store,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    results = provider.search_gmail(account, query="american flight", now_utc=now)

    assert queries == ["american flight"]
    assert results == [
        {
            "external_id": "search:message-1",
            "subject": "American Airlines trip confirmation",
            "sender": "American Airlines <no-reply@aa.com>",
            "body": "LAX to Cleveland. Cleveland to Paris.",
            "received_at_utc": now.isoformat(),
            "connected_account_id": account.id,
        }
    ]


def test_google_provider_interprets_all_day_events_in_household_timezone(tmp_path):
    service = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    account = _store_google_token(
        service,
        chat_id="google-all-day-family",
        now=now,
        payload={
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "scope": "openid email",
            "expires_at_utc": (now + timedelta(hours=1)).isoformat(),
        },
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer access-token"
        if request.url.host == "gmail.googleapis.com":
            return httpx.Response(200, json={"messages": []})
        if request.url.host == "www.googleapis.com" and request.url.path.endswith("/events"):
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": "event-all-day",
                            "summary": "No school",
                            "start": {"date": "2026-06-06"},
                            "end": {"date": "2026-06-07"},
                        }
                    ]
                },
            )
        raise AssertionError(f"unexpected request: {request.url}")

    provider = GoogleSourceProvider(
        settings=service.settings,
        store=service.store,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    batch = provider.fetch(account, now_utc=now)

    assert batch.calendar_events[0]["title"] == "No school"
    assert batch.calendar_events[0]["starts_at_utc"] == "2026-06-06T07:00:00+00:00"
    assert batch.calendar_events[0]["ends_at_utc"] == "2026-06-07T07:00:00+00:00"


def test_google_provider_refreshes_expired_access_token(tmp_path):
    service = _google_service(tmp_path)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    account = _store_google_token(
        service,
        chat_id="google-refresh-family",
        now=now,
        payload={
            "access_token": "expired-access-token",
            "refresh_token": "refresh-token",
            "scope": "openid email",
            "expires_at_utc": (now - timedelta(minutes=5)).isoformat(),
        },
    )
    seen_authorizations: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "oauth2.googleapis.com":
            return httpx.Response(
                200,
                json={
                    "access_token": "new-access-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                    "scope": "openid email",
                },
            )
        seen_authorizations.append(request.headers["authorization"])
        if request.url.host == "gmail.googleapis.com":
            return httpx.Response(200, json={"messages": []})
        if request.url.host == "www.googleapis.com":
            return httpx.Response(200, json={"items": []})
        raise AssertionError(f"unexpected request: {request.url}")

    provider = GoogleSourceProvider(
        settings=service.settings,
        store=service.store,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    provider.fetch(account, now_utc=now)

    token_record = service.store.get_connected_account_token(account.id)
    assert token_record is not None
    decrypted = TokenVault.from_settings(service.settings).decrypt(token_record.token_ciphertext)
    assert seen_authorizations == ["Bearer new-access-token", "Bearer new-access-token"]
    assert decrypted["access_token"] == "new-access-token"
    assert decrypted["refresh_token"] == "refresh-token"
