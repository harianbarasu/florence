"""Small scheduler loop for Florence routines."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Protocol

from florence.actions import run_approved_actions
from florence.config import Settings
from florence.linq import LinqClient
from florence.models import OutboundMessage
from florence.source_providers import (
    SourceProvider,
    SourceSyncRunResult,
    run_connected_source_sync,
)
from florence.service import FlorenceService


logger = logging.getLogger(__name__)


class Sender(Protocol):
    def send_text(self, *, chat_id: str, text: str, idempotency_key: str) -> object:
        ...


@dataclass(slots=True)
class RoutineTickResult:
    sent: int
    delivery_failed: int
    reminder_messages: int
    briefing_messages: int
    action_attempts: int
    action_succeeded: int
    action_failed: int


@dataclass(slots=True)
class SendBatchResult:
    attempted: int
    sent: int
    failed: int


@dataclass(slots=True)
class WorkerTickResult:
    routine: RoutineTickResult
    source_sync: SourceSyncRunResult | None
    source_messages: int
    source_delivery_failed: int


def run_routine_tick(
    service: FlorenceService,
    sender: Sender,
    *,
    now_utc: datetime | None = None,
) -> RoutineTickResult:
    now = now_utc or datetime.now(timezone.utc)
    reminders = service.due_reminder_messages(now_utc=now, mark_sent=False)
    briefings = service.daily_briefing_messages(now_utc=now, mark_sent=False)
    outbound = [*reminders, *briefings]
    send_result = _send_all(
        sender,
        outbound,
        on_sent=lambda message: service.mark_outbound_delivered(message, now_utc=now),
    )
    actions = run_approved_actions(store=service.store, sender=sender, now_utc=now)
    return RoutineTickResult(
        sent=send_result.sent + actions.succeeded,
        delivery_failed=send_result.failed,
        reminder_messages=len(reminders),
        briefing_messages=len(briefings),
        action_attempts=actions.attempted,
        action_succeeded=actions.succeeded,
        action_failed=actions.failed,
    )


def run_source_sync_tick(
    service: FlorenceService,
    *,
    providers: dict[str, SourceProvider] | None = None,
    sender: Sender | None = None,
    now_utc: datetime | None = None,
    limit: int = 100,
) -> SourceSyncRunResult:
    now = now_utc or datetime.now(timezone.utc)
    retryable_source = service.store.retryable_source_outbound_deliveries()
    retryable_oauth = service.store.retryable_outbound_deliveries_by_source_prefix(
        source_message_prefix="oauth:",
    )
    result = run_connected_source_sync(
        service=service,
        providers=providers,
        now_utc=now,
        limit=limit,
        mark_surfaced=sender is None,
    )
    if sender is not None:
        send_result = _send_all(
            sender,
            [*retryable_oauth, *retryable_source, *result.messages],
            on_before_send=lambda message: service.prepare_outbound_delivery(message, now_utc=now),
            on_sent=lambda message: service.mark_outbound_delivered(message, now_utc=now),
            on_failed=lambda message, error: service.mark_outbound_delivery_failed(
                message,
                error=error,
                now_utc=now,
            ),
        )
        result = replace(
            result,
            delivery_attempted=send_result.attempted,
            delivery_sent=send_result.sent,
            delivery_failed=send_result.failed,
        )
        _maybe_record_google_live_verification(
            service=service,
            sender=sender,
            result=result,
            providers=providers,
            now_utc=now,
        )
    return result


def run_worker_tick(
    service: FlorenceService,
    sender: Sender,
    *,
    providers: dict[str, SourceProvider] | None = None,
    now_utc: datetime | None = None,
    run_sources: bool = True,
    source_limit: int = 100,
) -> WorkerTickResult:
    now = now_utc or datetime.now(timezone.utc)
    routine = run_routine_tick(service, sender, now_utc=now)
    source_sync = None
    source_messages = 0
    source_delivery_failed = 0
    if run_sources:
        source_sync = run_source_sync_tick(
            service,
            providers=providers,
            sender=sender,
            now_utc=now,
            limit=source_limit,
        )
        source_messages = len(source_sync.messages)
        source_delivery_failed = source_sync.delivery_failed
    return WorkerTickResult(
        routine=routine,
        source_sync=source_sync,
        source_messages=source_messages,
        source_delivery_failed=source_delivery_failed,
    )


def run_forever(
    *,
    settings: Settings | None = None,
    interval_seconds: int = 60,
    source_sync_interval_seconds: int | None = None,
    max_iterations: int | None = None,
) -> None:
    resolved = settings or Settings.from_env()
    service = FlorenceService(settings=resolved)
    sender = LinqClient(resolved)
    source_interval = (
        resolved.source_sync_interval_seconds
        if source_sync_interval_seconds is None
        else source_sync_interval_seconds
    )
    logger.info(
        "Florence worker started with routine interval %ss and source interval %ss",
        interval_seconds,
        source_interval,
    )
    next_source_sync = 0.0
    iterations = 0
    while max_iterations is None or iterations < max_iterations:
        now_monotonic = time.monotonic()
        run_sources = source_interval > 0 and now_monotonic >= next_source_sync
        try:
            run_worker_tick(service, sender, run_sources=run_sources)
        except Exception:
            logger.exception("Florence worker tick failed; continuing")
        else:
            if run_sources:
                next_source_sync = now_monotonic + source_interval
        iterations += 1
        if max_iterations is not None and iterations >= max_iterations:
            break
        time.sleep(interval_seconds)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run_forever()


def _send_all(
    sender: Sender,
    outbound: list[OutboundMessage],
    *,
    on_sent: Callable[[OutboundMessage], None] | None = None,
    on_before_send: Callable[[OutboundMessage], None] | None = None,
    on_failed: Callable[[OutboundMessage, str], None] | None = None,
) -> SendBatchResult:
    if on_before_send is not None:
        for message in outbound:
            on_before_send(message)
    sent = 0
    failed = 0
    for message in outbound:
        try:
            sender.send_text(
                chat_id=message.chat_id,
                text=message.text,
                idempotency_key=message.idempotency_key,
            )
        except Exception as exc:
            failed += 1
            error = f"{type(exc).__name__}: {exc}"
            if on_failed is not None:
                on_failed(message, error)
            logger.warning(
                "Florence worker send failed for chat %s with idempotency key %s: %s",
                message.chat_id,
                message.idempotency_key,
                error,
            )
            continue
        if on_sent is not None:
            on_sent(message)
        sent += 1
    return SendBatchResult(attempted=len(outbound), sent=sent, failed=failed)


def _maybe_record_google_live_verification(
    *,
    service: FlorenceService,
    sender: Sender | None,
    result: SourceSyncRunResult,
    providers: dict[str, SourceProvider] | None,
    now_utc: datetime,
) -> None:
    if providers is not None:
        return
    if service.settings.database_backend != "postgres" or service.store.backend != "postgres":
        return
    if not _sender_can_prove_live_linq_delivery(sender):
        return
    if (
        result.checked <= 0
        or result.synced <= 0
        or result.imported <= 0
        or result.surfaced <= 0
        or result.delivery_sent <= 0
        or result.delivery_failed != 0
    ):
        return
    try:
        service.store.record_live_verification(
            name="google",
            verified_at_utc=now_utc,
            proof="Google OAuth source sync imported and surfaced a token-backed item",
            source="source_sync_worker",
            now_utc=now_utc,
        )
    except Exception:
        logger.warning("Florence could not record Google live verification proof", exc_info=True)


def _sender_can_prove_live_linq_delivery(sender: Sender | None) -> bool:
    return isinstance(sender, LinqClient) and bool(sender.settings.linq_api_key)
