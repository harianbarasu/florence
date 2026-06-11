"""Background runtime: inbound debouncing, the scheduler, and Gmail sync.

Proactivity is just more agent turns — a due reminder or a batch of new email
wakes the same loop with a directive instead of a parent's message.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import replace
from datetime import timedelta
from typing import Any

from florence.agent import Deps, run_turn
from florence.attachments import ingest_attachments
from florence.gmail import EmailSummary
from florence import prompts
from florence.store import TaskItem
from florence.timeutil import next_local_time, next_occurrence, now_utc
from florence.triage import gate_email

log = logging.getLogger("florence.runtime")

MORNING_BRIEF_TITLE = "Morning brief"
MAX_REMINDER_ATTEMPTS = 3
EMAIL_ITEMS_PER_TURN = 12


class Runtime:
    """Owns the background tasks. One instance per process (single replica)."""

    def __init__(self, deps: Deps) -> None:
        self.deps = deps
        self._debounce: dict[str, asyncio.Task] = {}
        self._turn_semaphore = asyncio.Semaphore(4)
        self._background: list[asyncio.Task] = []
        self._chat_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def start(self) -> None:
        self._background = [
            asyncio.create_task(self._scheduler_loop(), name="scheduler"),
            asyncio.create_task(self._gmail_loop(), name="gmail-sync"),
        ]

    async def stop(self) -> None:
        for task in [*self._background, *self._debounce.values()]:
            task.cancel()
        for task in [*self._background, *self._debounce.values()]:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass

    # -- inbound ---------------------------------------------------------------

    def enqueue_inbound(self, chat_id: str) -> None:
        """Debounce rapid-fire texts: the turn runs once typing pauses."""
        existing = self._debounce.pop(chat_id, None)
        if existing is not None:
            existing.cancel()
        self._debounce[chat_id] = asyncio.create_task(self._debounced_turn(chat_id))

    async def _debounced_turn(self, chat_id: str) -> None:
        try:
            await asyncio.sleep(self.deps.settings.debounce_seconds)
        except asyncio.CancelledError:
            return
        self._debounce.pop(chat_id, None)
        try:
            await self.run_inbound_turn(chat_id)
        except Exception:  # noqa: BLE001
            log.exception("inbound turn crashed for chat %s", chat_id)

    async def run_inbound_turn(self, chat_id: str) -> None:
        async with self._chat_locks[chat_id]:
            async with self._turn_semaphore:
                resolved = await self.deps.store.household_for_chat(chat_id)
                if resolved is None:
                    return
                household, chat = resolved
                if household.stopped:
                    return
                image_parts = await self._ingest_recent_attachments(chat_id)
                recent = await self.deps.store.recent_messages(chat_id, limit=1)
                member = None
                if recent and recent[-1].sender_phone:
                    member = await self.deps.store.member_by_phone(recent[-1].sender_phone)
                await run_turn(
                    self.deps,
                    household=household,
                    chat=chat,
                    member=member,
                    image_parts=image_parts,
                )

    async def _ingest_recent_attachments(self, chat_id: str) -> list[dict[str, Any]]:
        """Extract text/images from any not-yet-ingested attachments on recent
        inbound messages; persist extracted text, return vision parts for this turn."""
        image_parts: list[dict[str, Any]] = []
        cutoff = now_utc() - timedelta(minutes=10)
        for message in await self.deps.store.recent_messages(chat_id, limit=8):
            if message.direction != "inbound" or not message.attachments:
                continue
            if any(not a.get("ingested") for a in message.attachments):
                updated, images = await ingest_attachments(message.attachments, self.deps.linq)
                await self.deps.store.update_message_attachments(message.id, updated)
                if message.created_at >= cutoff:
                    image_parts.extend(images)
        return image_parts[:4]

    # -- scheduler ----------------------------------------------------------------

    async def _scheduler_loop(self) -> None:
        while True:
            try:
                await self._scheduler_tick()
            except Exception:  # noqa: BLE001
                log.exception("scheduler tick failed")
            await asyncio.sleep(self.deps.settings.scheduler_interval_seconds)

    async def _scheduler_tick(self) -> None:
        await self.deps.store.recover_stuck_tasks()
        due = await self.deps.store.claim_due_tasks(limit=10)
        for task in due:
            asyncio.create_task(self._fire_task(task), name=f"task-{task.id}")

    async def _fire_task(self, task: TaskItem) -> None:
        async with self._turn_semaphore:
            household = await self.deps.store.household_by_id(task.household_id)
            if household is None or household.stopped:
                await self.deps.store.finish_task(task.id, next_due_at=self._next_due(task, household))
                return
            chat = None
            chat_id = task.chat_id or household.primary_chat_id
            if chat_id:
                resolved = await self.deps.store.household_for_chat(chat_id)
                if resolved is not None:
                    chat = resolved[1]
            tz = household.timezone
            directive = (
                prompts.routine_directive(task)
                if task.kind == "routine"
                else prompts.reminder_directive(task, tz)
            )
            try:
                result = await run_turn(self.deps, household=household, chat=chat, directive=directive)
            except Exception:  # noqa: BLE001
                log.exception("task turn crashed for %s", task.id)
                result = None
            if result is None or result.error is not None:
                if task.attempts < MAX_REMINDER_ATTEMPTS:
                    await self.deps.store.retry_task(task.id, delay_minutes=5)
                else:
                    await self.deps.store.finish_task(task.id, next_due_at=self._next_due(task, household))
                return
            await self.deps.store.finish_task(task.id, next_due_at=self._next_due(task, household))

    def _next_due(self, task: TaskItem, household) -> Any:
        if not task.recurrence or household is None:
            return None
        next_due = next_occurrence(task.due_at, task.recurrence, household.timezone)
        # If the task fired very late (downtime), roll forward past now.
        while next_due <= now_utc():
            next_due = next_occurrence(next_due, task.recurrence, household.timezone)
        return next_due

    async def ensure_default_routines(self, household_id: str) -> None:
        household = await self.deps.store.household_by_id(household_id)
        if household is None:
            return
        if await self.deps.store.household_has_routine(household_id, MORNING_BRIEF_TITLE):
            return
        first = next_local_time(
            self.deps.settings.brief_hour, self.deps.settings.brief_minute, household.timezone
        )
        await self.deps.store.create_task(
            household_id=household_id,
            chat_id=household.primary_chat_id,
            kind="routine",
            title=MORNING_BRIEF_TITLE,
            due_at=first,
            notes=prompts.MORNING_BRIEF_NOTES,
            recurrence="daily",
            created_by="florence",
        )
        # One-time follow-up so Florence drives setup even if the first
        # conversation fizzles before email is connected.
        checkin = next_local_time(17, 0, household.timezone)
        if checkin - now_utc() < timedelta(hours=14):
            checkin = next_occurrence(checkin, "daily", household.timezone)
        await self.deps.store.create_task(
            household_id=household_id,
            chat_id=household.primary_chat_id,
            kind="routine",
            title="Setup check-in",
            due_at=checkin,
            notes=prompts.SETUP_CHECKIN_NOTES,
            recurrence=None,
            created_by="florence",
        )

    # -- gmail sync ------------------------------------------------------------------

    async def _gmail_loop(self) -> None:
        while True:
            try:
                await self._gmail_tick()
            except Exception:  # noqa: BLE001
                log.exception("gmail tick failed")
            await asyncio.sleep(self.deps.settings.gmail_sync_interval_seconds)

    async def _gmail_tick(self) -> None:
        if not self.deps.gmail.configured:
            return
        interval = timedelta(seconds=self.deps.settings.gmail_sync_interval_seconds)
        by_household: dict[str, list[EmailSummary]] = defaultdict(list)
        for account in await self.deps.store.gmail_accounts():
            if account.last_synced_at and now_utc() - account.last_synced_at < interval:
                continue
            since = account.last_synced_at or now_utc() - timedelta(hours=1)
            try:
                ids = await self.deps.gmail.new_message_ids(
                    account, since=since, cap=self.deps.settings.gmail_max_per_sync
                )
                fresh = await self.deps.store.filter_unseen_emails(account.id, ids)
                summaries = [await self.deps.gmail.message_summary(account, gid) for gid in fresh]
                await self.deps.store.mark_emails_seen(account.id, fresh)
                await self.deps.store.mark_gmail_synced(account.id)
                by_household[account.household_id].extend(summaries)
            except Exception as exc:  # noqa: BLE001
                log.warning("gmail sync failed for %s: %s", account.email, exc)
                await self.deps.store.record_gmail_failure(account.id)
        for household_id, items in by_household.items():
            if items:
                await self._email_turn(household_id, items)

    async def _email_turn(self, household_id: str, items: list[EmailSummary]) -> None:
        household = await self.deps.store.household_by_id(household_id)
        if household is None or household.stopped or not household.primary_chat_id:
            return
        items = sorted(items, key=lambda e: e.received_at)[-EMAIL_ITEMS_PER_TURN:]
        survivors = await self._gate_and_enrich(household_id, items)
        if not survivors:
            return
        resolved = await self.deps.store.household_for_chat(household.primary_chat_id)
        chat = resolved[1] if resolved else None
        async with self._turn_semaphore:
            try:
                await run_turn(
                    self.deps,
                    household=household,
                    chat=chat,
                    directive=prompts.email_directive(survivors, household.timezone),
                )
            except Exception:  # noqa: BLE001
                log.exception("email turn crashed for household %s", household_id)

    async def _gate_and_enrich(
        self, household_id: str, items: list[EmailSummary]
    ) -> list[EmailSummary]:
        """Run each email past the gatekeeper; fetch full bodies for survivors
        so the family turn acts on substance, not snippets."""
        memories = await self.deps.store.list_memories(household_id)
        accounts = {a.email: a for a in await self.deps.store.gmail_accounts(household_id)}
        survivors: list[EmailSummary] = []
        skipped: list[dict[str, str]] = []
        for item in items:
            decision = await gate_email(
                self.deps.llm,
                item=item,
                memories=memories,
                triage_model=self.deps.settings.triage_model,
            )
            if not decision.notify:
                skipped.append({"subject": item.subject, "why": decision.justification})
                continue
            body = decision.summary
            account = accounts.get(item.account_email)
            if account is not None:
                try:
                    full = await self.deps.gmail.read_full(account, item.gmail_id)
                    body = str(full.get("body") or "")[:1500] or body
                except Exception as exc:  # noqa: BLE001
                    log.warning("read_full failed for %s: %s", item.gmail_id, exc)
            survivors.append(replace(item, body=body))
        await self.deps.store.log_event(
            "email_triage",
            household_id=household_id,
            payload={"total": len(items), "passed": len(survivors), "skipped": skipped},
        )
        return survivors

    async def gmail_connected_turn(
        self, household_id: str, chat_id: str | None, email: str, by_name: str | None
    ) -> None:
        household = await self.deps.store.household_by_id(household_id)
        if household is None:
            return
        chat = None
        target = chat_id or household.primary_chat_id
        if target:
            resolved = await self.deps.store.household_for_chat(target)
            chat = resolved[1] if resolved else None
        await run_turn(
            self.deps,
            household=household,
            chat=chat,
            directive=prompts.gmail_connected_directive(email, by_name),
        )
