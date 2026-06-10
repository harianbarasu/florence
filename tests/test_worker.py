from __future__ import annotations

import logging
from datetime import datetime, timezone

from florence import worker
from florence.config import Settings
from florence.linq import LinqClient
from florence.models import IncomingMessage
from florence.service import FlorenceService


def test_run_forever_continues_after_tick_failure(tmp_path, monkeypatch, caplog):
    calls: list[bool] = []

    def fake_run_worker_tick(service, sender, *, run_sources):
        calls.append(run_sources)
        if len(calls) == 1:
            raise RuntimeError("linq unavailable")
        return None

    monkeypatch.setattr(worker, "run_worker_tick", fake_run_worker_tick)
    monkeypatch.setattr(worker.time, "sleep", lambda _: None)
    caplog.set_level(logging.ERROR, logger="florence.worker")

    worker.run_forever(
        settings=Settings(db_path=str(tmp_path / "worker.db")),
        interval_seconds=0,
        source_sync_interval_seconds=300,
        max_iterations=2,
    )

    assert calls == [True, True]
    assert "Florence worker tick failed; continuing" in caplog.text


class FakeLinqReconciler:
    def __init__(self, incoming: list[IncomingMessage]):
        self.incoming = incoming
        self.sent: list[dict[str, str]] = []

    def recent_incoming_messages(
        self,
        *,
        now_utc: datetime,
        chat_limit: int = 25,
        messages_per_chat: int = 20,
        since_utc: datetime | None = None,
    ) -> list[IncomingMessage]:
        return self.incoming

    def send_text(self, *, chat_id: str, text: str, idempotency_key: str) -> object:
        self.sent.append(
            {
                "chat_id": chat_id,
                "text": text,
                "idempotency_key": idempotency_key,
            }
        )
        return {"message": {"id": idempotency_key}}


class FakeAgent:
    def __init__(self, response: str):
        self.response = response

    def complete(self, **kwargs):
        return self.response


def test_linq_reconciliation_replies_to_missed_inbound_once(tmp_path):
    now = datetime(2026, 6, 8, 15, 0, tzinfo=timezone.utc)
    service = FlorenceService(
        settings=Settings(db_path=str(tmp_path / "worker.db")),
        agent=FakeAgent("Hi, I'm Florence."),
    )
    linq = FakeLinqReconciler(
        [
            IncomingMessage(
                chat_id="linq-chat",
                message_id="missed-hi",
                sender="+15555550100",
                text="hi",
                received_at=now,
            )
        ]
    )

    first = worker.run_linq_reconciliation_tick(service, linq, now_utc=now)
    second = worker.run_linq_reconciliation_tick(service, linq, now_utc=now)

    household = service.store.get_household_by_chat("linq-chat")
    assert household is not None
    assert first.checked == 1
    assert first.inbound == 1
    assert first.sent == 1
    assert first.delivery_failed == 0
    assert second.checked == 1
    assert second.inbound == 0
    assert second.sent == 0
    assert len(linq.sent) == 1
    assert "I'm Florence" in linq.sent[0]["text"]
    assert service.store.message_transport_summary(household_id=household.id)["inbound"] == 1
    assert service.store.message_transport_summary(household_id=household.id)["outbound"] == 1
    assert service.store.outbound_delivery_summary(household_id=household.id)["sent"] == 1


def test_worker_linq_reconciliation_only_scans_recent_window(tmp_path, monkeypatch):
    now = datetime(2026, 6, 8, 15, 0, tzinfo=timezone.utc)
    service = FlorenceService(settings=Settings(db_path=str(tmp_path / "worker.db")))
    linq = LinqClient(Settings(db_path=str(tmp_path / "worker.db")))
    seen_since: list[datetime | None] = []

    def fake_recent_incoming_messages(
        self,
        *,
        now_utc: datetime,
        chat_limit: int = 25,
        messages_per_chat: int = 20,
        since_utc: datetime | None = None,
    ) -> list[IncomingMessage]:
        seen_since.append(since_utc)
        return []

    monkeypatch.setattr(LinqClient, "recent_incoming_messages", fake_recent_incoming_messages)

    result = worker.run_worker_tick(service, linq, now_utc=now, run_sources=False)

    assert result.linq_reconciliation is not None
    assert result.linq_reconciliation.checked == 0
    assert seen_since == [now - worker.LINQ_RECONCILIATION_LOOKBACK]
