"""Persistence. Dumb typed queries only — no business logic, no canned text."""

from __future__ import annotations

import json
import secrets
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from florence.timeutil import ensure_utc, now_utc


@dataclass(frozen=True, slots=True)
class Household:
    id: str
    name: str | None
    timezone: str
    primary_chat_id: str | None
    stopped: bool


@dataclass(frozen=True, slots=True)
class Member:
    id: str
    household_id: str
    phone: str
    name: str | None
    role: str


@dataclass(frozen=True, slots=True)
class Chat:
    chat_id: str
    household_id: str
    kind: str


@dataclass(frozen=True, slots=True)
class StoredMessage:
    id: int
    chat_id: str
    direction: str
    sender_phone: str | None
    sender_name: str | None
    body: str
    attachments: list[dict[str, Any]]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class Memory:
    id: str
    content: str
    category: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class TaskItem:
    id: str
    household_id: str
    chat_id: str | None
    kind: str
    title: str
    notes: str | None
    due_at: datetime
    recurrence: str | None
    status: str
    attempts: int


@dataclass(frozen=True, slots=True)
class GmailAccount:
    id: str
    household_id: str
    member_phone: str | None
    email: str
    token_ciphertext: str
    status: str
    last_synced_at: datetime | None
    failure_count: int


@dataclass(frozen=True, slots=True)
class OAuthStateRow:
    state: str
    household_id: str
    member_phone: str | None
    chat_id: str | None


def _new_id() -> str:
    return uuid.uuid4().hex


def _household(row: dict[str, Any]) -> Household:
    return Household(
        id=row["id"],
        name=row["name"],
        timezone=row["timezone"],
        primary_chat_id=row["primary_chat_id"],
        stopped=row["stopped"],
    )


def _member(row: dict[str, Any]) -> Member:
    return Member(
        id=row["id"],
        household_id=row["household_id"],
        phone=row["phone"],
        name=row["name"],
        role=row["role"],
    )


def _chat(row: dict[str, Any]) -> Chat:
    return Chat(chat_id=row["chat_id"], household_id=row["household_id"], kind=row["kind"])


def _message(row: dict[str, Any]) -> StoredMessage:
    attachments = row["attachments"]
    if isinstance(attachments, str):
        attachments = json.loads(attachments)
    return StoredMessage(
        id=row["id"],
        chat_id=row["chat_id"],
        direction=row["direction"],
        sender_phone=row["sender_phone"],
        sender_name=row["sender_name"],
        body=row["body"],
        attachments=attachments or [],
        created_at=ensure_utc(row["created_at"]),
    )


def _task(row: dict[str, Any]) -> TaskItem:
    return TaskItem(
        id=row["id"],
        household_id=row["household_id"],
        chat_id=row["chat_id"],
        kind=row["kind"],
        title=row["title"],
        notes=row["notes"],
        due_at=ensure_utc(row["due_at"]),
        recurrence=row["recurrence"],
        status=row["status"],
        attempts=row["attempts"],
    )


def _gmail(row: dict[str, Any]) -> GmailAccount:
    return GmailAccount(
        id=row["id"],
        household_id=row["household_id"],
        member_phone=row["member_phone"],
        email=row["email"],
        token_ciphertext=row["token_ciphertext"],
        status=row["status"],
        last_synced_at=ensure_utc(row["last_synced_at"]) if row["last_synced_at"] else None,
        failure_count=row["failure_count"],
    )


class Store:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self.pool = pool

    # -- households / chats / members -------------------------------------

    async def household_for_chat(self, chat_id: str) -> tuple[Household, Chat] | None:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """SELECT h.*, c.chat_id AS c_chat_id, c.kind AS c_kind
                   FROM chats c JOIN households h ON h.id = c.household_id
                   WHERE c.chat_id = %s""",
                (chat_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        household = _household(row)
        chat = Chat(chat_id=row["c_chat_id"], household_id=household.id, kind=row["c_kind"])
        return household, chat

    async def household_by_id(self, household_id: str) -> Household | None:
        async with self.pool.connection() as conn:
            cur = await conn.execute("SELECT * FROM households WHERE id = %s", (household_id,))
            row = await cur.fetchone()
        return _household(row) if row else None

    async def member_by_phone(self, phone: str) -> Member | None:
        async with self.pool.connection() as conn:
            cur = await conn.execute("SELECT * FROM members WHERE phone = %s", (phone,))
            row = await cur.fetchone()
        return _member(row) if row else None

    async def members_of(self, household_id: str) -> list[Member]:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                "SELECT * FROM members WHERE household_id = %s ORDER BY created_at", (household_id,)
            )
            rows = await cur.fetchall()
        return [_member(r) for r in rows]

    async def chats_of(self, household_id: str) -> list[Chat]:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                "SELECT * FROM chats WHERE household_id = %s ORDER BY created_at", (household_id,)
            )
            rows = await cur.fetchall()
        return [_chat(r) for r in rows]

    async def attach_inbound(
        self,
        *,
        chat_id: str,
        chat_kind: str,
        sender_phone: str,
        default_timezone: str,
    ) -> tuple[Household, Chat, Member, bool, bool]:
        """Resolve (household, chat, member) for an inbound message, creating
        whatever is missing. A known phone in a new chat attaches the new chat
        to that member's existing household (the 1:1 <-> group-chat bridge).
        Returns (household, chat, member, household_created, chat_created)."""
        now = now_utc()
        async with self.pool.connection() as conn:
            async with conn.transaction():
                cur = await conn.execute("SELECT * FROM chats WHERE chat_id = %s", (chat_id,))
                chat_row = await cur.fetchone()
                household_created = False
                chat_created = chat_row is None
                if chat_row is not None:
                    household_id = chat_row["household_id"]
                    if chat_kind != "unknown" and chat_row["kind"] != chat_kind:
                        await conn.execute(
                            "UPDATE chats SET kind = %s WHERE chat_id = %s", (chat_kind, chat_id)
                        )
                else:
                    cur = await conn.execute(
                        "SELECT * FROM members WHERE phone = %s", (sender_phone,)
                    )
                    member_row = await cur.fetchone()
                    if member_row is not None:
                        household_id = member_row["household_id"]
                    else:
                        household_id = _new_id()
                        household_created = True
                        await conn.execute(
                            """INSERT INTO households (id, timezone, primary_chat_id, created_at)
                               VALUES (%s, %s, %s, %s)""",
                            (household_id, default_timezone, chat_id, now),
                        )
                    await conn.execute(
                        """INSERT INTO chats (chat_id, household_id, kind, created_at)
                           VALUES (%s, %s, %s, %s)""",
                        (chat_id, household_id, chat_kind, now),
                    )
                    # A group chat becomes the family's primary delivery channel.
                    if chat_kind == "group":
                        await conn.execute(
                            "UPDATE households SET primary_chat_id = %s WHERE id = %s",
                            (chat_id, household_id),
                        )
                cur = await conn.execute("SELECT * FROM members WHERE phone = %s", (sender_phone,))
                member_row = await cur.fetchone()
                if member_row is None:
                    member_id = _new_id()
                    await conn.execute(
                        """INSERT INTO members (id, household_id, phone, role, created_at)
                           VALUES (%s, %s, %s, 'parent', %s)""",
                        (member_id, household_id, sender_phone, now),
                    )
                await conn.execute(
                    "UPDATE chats SET last_inbound_at = %s WHERE chat_id = %s", (now, chat_id)
                )
                cur = await conn.execute("SELECT * FROM households WHERE id = %s", (household_id,))
                household = _household(await cur.fetchone())
                cur = await conn.execute("SELECT * FROM chats WHERE chat_id = %s", (chat_id,))
                chat = _chat(await cur.fetchone())
                cur = await conn.execute("SELECT * FROM members WHERE phone = %s", (sender_phone,))
                member = _member(await cur.fetchone())
        return household, chat, member, household_created, chat_created

    async def add_known_member(self, household_id: str, phone: str) -> bool:
        """Add a member discovered from chat participant lists. Returns True if new."""
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """INSERT INTO members (id, household_id, phone, role, created_at)
                   VALUES (%s, %s, %s, 'parent', %s)
                   ON CONFLICT (phone) DO NOTHING""",
                (_new_id(), household_id, phone, now_utc()),
            )
            return cur.rowcount > 0

    async def update_member(
        self, household_id: str, phone: str, *, name: str | None = None, role: str | None = None
    ) -> bool:
        sets, params = [], []
        if name is not None:
            sets.append("name = %s")
            params.append(name)
        if role is not None:
            sets.append("role = %s")
            params.append(role)
        if not sets:
            return False
        params.extend([household_id, phone])
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                f"UPDATE members SET {', '.join(sets)} WHERE household_id = %s AND phone = %s",
                params,
            )
            return cur.rowcount > 0

    async def set_household(
        self,
        household_id: str,
        *,
        name: str | None = None,
        timezone_name: str | None = None,
        stopped: bool | None = None,
        primary_chat_id: str | None = None,
    ) -> None:
        sets, params = [], []
        if name is not None:
            sets.append("name = %s")
            params.append(name)
        if timezone_name is not None:
            sets.append("timezone = %s")
            params.append(timezone_name)
        if stopped is not None:
            sets.append("stopped = %s")
            params.append(stopped)
        if primary_chat_id is not None:
            sets.append("primary_chat_id = %s")
            params.append(primary_chat_id)
        if not sets:
            return
        params.append(household_id)
        async with self.pool.connection() as conn:
            await conn.execute(f"UPDATE households SET {', '.join(sets)} WHERE id = %s", params)

    # -- messages ----------------------------------------------------------

    async def record_message(
        self,
        *,
        household_id: str,
        chat_id: str,
        direction: str,
        body: str,
        sender_phone: str | None = None,
        sender_name: str | None = None,
        attachments: list[dict[str, Any]] | None = None,
        external_id: str | None = None,
        created_at: datetime | None = None,
    ) -> int | None:
        """Insert a message; returns its id, or None if external_id was a duplicate."""
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """INSERT INTO messages
                   (household_id, chat_id, external_id, direction, sender_phone, sender_name,
                    body, attachments, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (external_id) DO NOTHING
                   RETURNING id""",
                (
                    household_id,
                    chat_id,
                    external_id,
                    direction,
                    sender_phone,
                    sender_name,
                    body,
                    Jsonb(attachments) if attachments else None,
                    ensure_utc(created_at) if created_at else now_utc(),
                ),
            )
            row = await cur.fetchone()
        return row["id"] if row else None

    async def update_message_attachments(
        self, message_id: int, attachments: list[dict[str, Any]]
    ) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                "UPDATE messages SET attachments = %s WHERE id = %s",
                (Jsonb(attachments), message_id),
            )

    async def recent_messages(self, chat_id: str, limit: int = 40) -> list[StoredMessage]:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                "SELECT * FROM messages WHERE chat_id = %s ORDER BY id DESC LIMIT %s",
                (chat_id, limit),
            )
            rows = await cur.fetchall()
        return [_message(r) for r in reversed(rows)]

    async def recent_other_chat_messages(
        self, household_id: str, exclude_chat_id: str, limit: int = 8
    ) -> list[StoredMessage]:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """SELECT * FROM messages
                   WHERE household_id = %s AND chat_id != %s
                   ORDER BY id DESC LIMIT %s""",
                (household_id, exclude_chat_id, limit),
            )
            rows = await cur.fetchall()
        return [_message(r) for r in reversed(rows)]

    # -- memories ----------------------------------------------------------

    async def add_memory(
        self, household_id: str, content: str, category: str | None, created_by: str | None
    ) -> str:
        memory_id = _new_id()
        async with self.pool.connection() as conn:
            await conn.execute(
                """INSERT INTO memories (id, household_id, content, category, created_by, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (memory_id, household_id, content, category, created_by, now_utc()),
            )
        return memory_id

    async def archive_memory(self, household_id: str, memory_id: str) -> bool:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """UPDATE memories SET archived_at = %s
                   WHERE id = %s AND household_id = %s AND archived_at IS NULL""",
                (now_utc(), memory_id, household_id),
            )
            return cur.rowcount > 0

    async def list_memories(self, household_id: str, limit: int = 80) -> list[Memory]:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """SELECT * FROM memories
                   WHERE household_id = %s AND archived_at IS NULL
                   ORDER BY created_at DESC LIMIT %s""",
                (household_id, limit),
            )
            rows = await cur.fetchall()
        return [
            Memory(
                id=r["id"],
                content=r["content"],
                category=r["category"],
                created_at=ensure_utc(r["created_at"]),
            )
            for r in reversed(rows)
        ]

    # -- tasks (reminders & routines) ---------------------------------------

    async def create_task(
        self,
        *,
        household_id: str,
        chat_id: str | None,
        kind: str,
        title: str,
        due_at: datetime,
        notes: str | None = None,
        recurrence: str | None = None,
        created_by: str | None = None,
    ) -> str:
        task_id = _new_id()
        async with self.pool.connection() as conn:
            await conn.execute(
                """INSERT INTO tasks
                   (id, household_id, chat_id, kind, title, notes, due_at, recurrence,
                    status, created_by, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s)""",
                (
                    task_id,
                    household_id,
                    chat_id,
                    kind,
                    title,
                    notes,
                    ensure_utc(due_at),
                    recurrence,
                    created_by,
                    now_utc(),
                ),
            )
        return task_id

    async def get_task(self, household_id: str, task_id: str) -> TaskItem | None:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                "SELECT * FROM tasks WHERE id = %s AND household_id = %s", (task_id, household_id)
            )
            row = await cur.fetchone()
        return _task(row) if row else None

    async def update_task(
        self,
        household_id: str,
        task_id: str,
        *,
        title: str | None = None,
        notes: str | None = None,
        due_at: datetime | None = None,
        recurrence: str | None = None,
        status: str | None = None,
    ) -> bool:
        sets, params = [], []
        if title is not None:
            sets.append("title = %s")
            params.append(title)
        if notes is not None:
            sets.append("notes = %s")
            params.append(notes)
        if due_at is not None:
            sets.append("due_at = %s")
            params.append(ensure_utc(due_at))
        if recurrence is not None:
            sets.append("recurrence = %s")
            params.append(None if recurrence == "none" else recurrence)
        if status is not None:
            sets.append("status = %s")
            params.append(status)
        if not sets:
            return False
        params.extend([task_id, household_id])
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                f"UPDATE tasks SET {', '.join(sets)} WHERE id = %s AND household_id = %s", params
            )
            return cur.rowcount > 0

    async def upcoming_tasks(
        self, household_id: str, *, within_days: int = 14, limit: int = 20
    ) -> list[TaskItem]:
        horizon = now_utc() + timedelta(days=within_days)
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """SELECT * FROM tasks
                   WHERE household_id = %s AND status IN ('pending', 'firing') AND due_at <= %s
                   ORDER BY due_at LIMIT %s""",
                (household_id, horizon, limit),
            )
            rows = await cur.fetchall()
        return [_task(r) for r in rows]

    async def list_tasks(
        self, household_id: str, *, include_inactive: bool = False, limit: int = 50
    ) -> list[TaskItem]:
        statuses = ("pending", "firing", "done", "cancelled") if include_inactive else ("pending", "firing")
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """SELECT * FROM tasks
                   WHERE household_id = %s AND status = ANY(%s)
                   ORDER BY due_at LIMIT %s""",
                (household_id, list(statuses), limit),
            )
            rows = await cur.fetchall()
        return [_task(r) for r in rows]

    async def household_has_routine(self, household_id: str, title: str) -> bool:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """SELECT 1 FROM tasks
                   WHERE household_id = %s AND kind = 'routine' AND title = %s
                     AND status IN ('pending', 'firing') LIMIT 1""",
                (household_id, title),
            )
            return await cur.fetchone() is not None

    async def claim_due_tasks(self, *, limit: int = 10) -> list[TaskItem]:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """UPDATE tasks SET status = 'firing', last_fired_at = %s, attempts = attempts + 1
                   WHERE id IN (
                     SELECT id FROM tasks WHERE status = 'pending' AND due_at <= %s
                     ORDER BY due_at LIMIT %s FOR UPDATE SKIP LOCKED
                   )
                   RETURNING *""",
                (now_utc(), now_utc(), limit),
            )
            rows = await cur.fetchall()
        return [_task(r) for r in rows]

    async def finish_task(self, task_id: str, *, next_due_at: datetime | None) -> None:
        async with self.pool.connection() as conn:
            if next_due_at is not None:
                await conn.execute(
                    "UPDATE tasks SET status = 'pending', due_at = %s, attempts = 0 WHERE id = %s",
                    (ensure_utc(next_due_at), task_id),
                )
            else:
                await conn.execute("UPDATE tasks SET status = 'done' WHERE id = %s", (task_id,))

    async def retry_task(self, task_id: str, *, delay_minutes: int = 5) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                "UPDATE tasks SET status = 'pending', due_at = %s WHERE id = %s",
                (now_utc() + timedelta(minutes=delay_minutes), task_id),
            )

    async def recover_stuck_tasks(self, *, older_than_minutes: int = 10) -> int:
        cutoff = now_utc() - timedelta(minutes=older_than_minutes)
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """UPDATE tasks SET status = 'pending'
                   WHERE status = 'firing' AND last_fired_at < %s""",
                (cutoff,),
            )
            return cur.rowcount

    # -- gmail accounts ------------------------------------------------------

    async def upsert_gmail_account(
        self,
        *,
        household_id: str,
        member_phone: str | None,
        email: str,
        google_sub: str,
        token_ciphertext: str,
        scopes: str | None,
    ) -> str:
        account_id = _new_id()
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """INSERT INTO gmail_accounts
                   (id, household_id, member_phone, email, google_sub, token_ciphertext,
                    scopes, status, last_synced_at, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, 'active', %s, %s)
                   ON CONFLICT (household_id, google_sub) DO UPDATE SET
                     token_ciphertext = EXCLUDED.token_ciphertext,
                     email = EXCLUDED.email,
                     member_phone = COALESCE(EXCLUDED.member_phone, gmail_accounts.member_phone),
                     scopes = EXCLUDED.scopes,
                     status = 'active',
                     failure_count = 0
                   RETURNING id""",
                (
                    account_id,
                    household_id,
                    member_phone,
                    email,
                    google_sub,
                    token_ciphertext,
                    scopes,
                    now_utc(),
                    now_utc(),
                ),
            )
            row = await cur.fetchone()
        return row["id"]

    async def gmail_accounts(self, household_id: str | None = None) -> list[GmailAccount]:
        async with self.pool.connection() as conn:
            if household_id is None:
                cur = await conn.execute("SELECT * FROM gmail_accounts WHERE status = 'active'")
            else:
                cur = await conn.execute(
                    "SELECT * FROM gmail_accounts WHERE status = 'active' AND household_id = %s",
                    (household_id,),
                )
            rows = await cur.fetchall()
        return [_gmail(r) for r in rows]

    async def update_gmail_token(self, account_id: str, token_ciphertext: str) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                "UPDATE gmail_accounts SET token_ciphertext = %s WHERE id = %s",
                (token_ciphertext, account_id),
            )

    async def mark_gmail_synced(self, account_id: str) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                "UPDATE gmail_accounts SET last_synced_at = %s, failure_count = 0 WHERE id = %s",
                (now_utc(), account_id),
            )

    async def record_gmail_failure(self, account_id: str, *, disable_after: int = 10) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                """UPDATE gmail_accounts
                   SET failure_count = failure_count + 1,
                       status = CASE WHEN failure_count + 1 >= %s THEN 'error' ELSE status END
                   WHERE id = %s""",
                (disable_after, account_id),
            )

    async def filter_unseen_emails(self, account_id: str, gmail_ids: list[str]) -> list[str]:
        if not gmail_ids:
            return []
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                "SELECT gmail_id FROM seen_emails WHERE account_id = %s AND gmail_id = ANY(%s)",
                (account_id, gmail_ids),
            )
            seen = {r["gmail_id"] for r in await cur.fetchall()}
        return [g for g in gmail_ids if g not in seen]

    async def mark_emails_seen(self, account_id: str, gmail_ids: list[str]) -> None:
        if not gmail_ids:
            return
        async with self.pool.connection() as conn:
            await conn.executemany(
                """INSERT INTO seen_emails (account_id, gmail_id) VALUES (%s, %s)
                   ON CONFLICT DO NOTHING""",
                [(account_id, g) for g in gmail_ids],
            )

    # -- oauth states ----------------------------------------------------------

    async def create_oauth_state(
        self,
        *,
        household_id: str,
        member_phone: str | None,
        chat_id: str | None,
        ttl_minutes: int = 60,
    ) -> str:
        state = secrets.token_urlsafe(32)
        now = now_utc()
        async with self.pool.connection() as conn:
            await conn.execute(
                """INSERT INTO oauth_states
                   (state, household_id, member_phone, chat_id, created_at, expires_at)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (state, household_id, member_phone, chat_id, now, now + timedelta(minutes=ttl_minutes)),
            )
        return state

    async def peek_oauth_state(self, state: str) -> OAuthStateRow | None:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """SELECT * FROM oauth_states
                   WHERE state = %s AND used_at IS NULL AND expires_at > %s""",
                (state, now_utc()),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return OAuthStateRow(
            state=row["state"],
            household_id=row["household_id"],
            member_phone=row["member_phone"],
            chat_id=row["chat_id"],
        )

    async def consume_oauth_state(self, state: str) -> OAuthStateRow | None:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                """UPDATE oauth_states SET used_at = %s
                   WHERE state = %s AND used_at IS NULL AND expires_at > %s
                   RETURNING *""",
                (now_utc(), state, now_utc()),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return OAuthStateRow(
            state=row["state"],
            household_id=row["household_id"],
            member_phone=row["member_phone"],
            chat_id=row["chat_id"],
        )

    # -- events ------------------------------------------------------------

    async def log_event(
        self, kind: str, *, household_id: str | None = None, payload: dict[str, Any] | None = None
    ) -> None:
        try:
            async with self.pool.connection() as conn:
                await conn.execute(
                    "INSERT INTO events (household_id, kind, payload, created_at) VALUES (%s, %s, %s, %s)",
                    (household_id, kind, Jsonb(payload) if payload else None, now_utc()),
                )
        except Exception:  # noqa: BLE001 - logging must never break the turn
            pass

    async def recent_events(self, limit: int = 30) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            cur = await conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT %s", (limit,))
            rows = await cur.fetchall()
        for r in rows:
            r["created_at"] = ensure_utc(r["created_at"]).isoformat()
        return rows

    async def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        async with self.pool.connection() as conn:
            for table in ("households", "members", "chats", "messages", "memories", "tasks", "gmail_accounts"):
                cur = await conn.execute(f"SELECT COUNT(*) AS n FROM {table}")  # noqa: S608
                out[table] = (await cur.fetchone())["n"]
        return out
