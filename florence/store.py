"""Persistence layer for Florence.

SQLite is kept for local development and tests. Deployed SaaS environments should
use PostgreSQL via ``FLORENCE_DATABASE_URL``.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from florence.models import (
    ActionExecution,
    ActionExecutionStatus,
    BriefingSourceItem,
    ConnectedAccount,
    ConnectedAccountStatus,
    ConnectedAccountToken,
    Household,
    HouseholdDataSummary,
    HouseholdMember,
    HouseholdPrivacy,
    MemoryExportItem,
    MemoryKind,
    MemoryRecord,
    MemorySnapshot,
    MemberRole,
    MessageDirection,
    OAuthState,
    OutboundDeliveryStatus,
    OutboundMessage,
    PendingAction,
    PendingActionStatus,
    PrivacyMode,
    Reminder,
    ReminderStatus,
    SourceDecision,
    SourceFeedbackKind,
    SourceItem,
    SourcePreference,
    SourcePreferenceKind,
    SourceReviewSnapshot,
)
from florence.timekeeper import ensure_utc


def _iso(value: datetime) -> str:
    return ensure_utc(value).isoformat()


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _is_postgres_dsn(value: str) -> bool:
    return value.strip().lower().startswith(("postgres://", "postgresql://"))


def _unsupported_database_url_scheme(value: str) -> str | None:
    normalized = value.strip()
    if "://" not in normalized or _is_postgres_dsn(normalized):
        return None
    return normalized.split("://", 1)[0].lower() or "unknown"


_INSERT_OR_IGNORE_RE = re.compile(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", re.IGNORECASE)
_PRAGMA_TABLE_INFO_RE = re.compile(
    r"^\s*PRAGMA\s+table_info\((?P<table>[A-Za-z_][A-Za-z0-9_]*)\)\s*$",
    re.IGNORECASE,
)

CURRENT_SCHEMA_VERSION = 1

_SCHEMA_REQUIRED_COLUMNS: dict[str, set[str]] = {
    "households": {
        "id",
        "chat_id",
        "timezone",
        "stopped",
        "memory_enabled",
        "product_analytics_opt_in",
        "privacy_updated_at_utc",
        "created_at_utc",
    },
    "household_members": {
        "id",
        "household_id",
        "phone",
        "role",
        "display_name",
        "created_at_utc",
        "last_seen_at_utc",
    },
    "connected_accounts": {
        "id",
        "household_id",
        "provider",
        "external_account_id",
        "account_label",
        "status",
        "cursor",
        "created_at_utc",
        "updated_at_utc",
        "last_synced_at_utc",
        "sync_failure_count",
        "last_sync_error",
        "retry_after_utc",
    },
    "messages": {
        "id",
        "household_id",
        "actor_member_id",
        "chat_id",
        "direction",
        "sender",
        "body",
        "created_at_utc",
    },
    "outbound_deliveries": {
        "idempotency_key",
        "household_id",
        "source_message_id",
        "chat_id",
        "text",
        "payload_json",
        "delivery_status",
        "attempts",
        "delivered_at_utc",
        "last_error",
        "created_at_utc",
        "updated_at_utc",
    },
    "reminders": {
        "id",
        "household_id",
        "chat_id",
        "title",
        "assignee_member_id",
        "due_at_utc",
        "created_at_utc",
        "status",
    },
    "household_chat_aliases": {"chat_id", "household_id", "created_at_utc"},
    "source_items": {
        "id",
        "household_id",
        "connected_account_id",
        "source_type",
        "external_id",
        "sender",
        "title",
        "body",
        "observed_at_utc",
        "event_at_utc",
        "decision",
        "reason",
        "priority",
        "surfaced_at_utc",
        "briefed_at_utc",
    },
    "connected_account_tokens": {
        "connected_account_id",
        "provider",
        "token_ciphertext",
        "scopes_json",
        "expires_at_utc",
        "created_at_utc",
        "updated_at_utc",
    },
    "oauth_states": {
        "state",
        "provider",
        "chat_id",
        "account_label",
        "return_path",
        "created_at_utc",
        "expires_at_utc",
        "used_at_utc",
    },
    "household_deletion_tombstones": {
        "tombstone_key",
        "deleted_at_utc",
        "expires_at_utc",
    },
    "source_preferences": {
        "id",
        "household_id",
        "phrase",
        "preference",
        "created_by_member_id",
        "created_at_utc",
        "updated_at_utc",
    },
    "source_feedback": {
        "id",
        "household_id",
        "source_item_id",
        "feedback",
        "phrase",
        "created_by_member_id",
        "created_at_utc",
    },
    "memories": {
        "id",
        "household_id",
        "kind",
        "subject",
        "text",
        "confidence",
        "asserted_by_member_id",
        "source_message_id",
        "created_at_utc",
        "updated_at_utc",
        "expires_at_utc",
        "deleted_at_utc",
    },
    "routine_runs": {
        "id",
        "household_id",
        "routine_name",
        "local_date",
        "ran_at_utc",
    },
    "pending_actions": {
        "id",
        "household_id",
        "chat_id",
        "action_type",
        "summary",
        "payload_json",
        "created_by_member_id",
        "created_at_utc",
        "expires_at_utc",
        "status",
        "resolved_by_member_id",
        "resolved_at_utc",
    },
    "action_executions": {
        "id",
        "action_id",
        "household_id",
        "status",
        "attempted_at_utc",
        "result_json",
        "error",
    },
    "live_verifications": {
        "name",
        "verified_at_utc",
        "proof",
        "source",
        "updated_at_utc",
    },
}


class DatabaseSchemaError(RuntimeError):
    """Raised when an existing database is not compatible with this build."""


def _postgres_sql(sql: str) -> str:
    converted = sql.replace(
        "ABS(strftime('%s', due_at_utc) - strftime('%s', ?))",
        "ABS(EXTRACT(EPOCH FROM due_at_utc::timestamptz) - "
        "EXTRACT(EPOCH FROM ?::timestamptz))",
    )
    had_insert_or_ignore = bool(_INSERT_OR_IGNORE_RE.search(converted))
    converted = _INSERT_OR_IGNORE_RE.sub("INSERT INTO", converted)
    converted = converted.replace("?", "%s")
    if had_insert_or_ignore:
        suffix = ";" if converted.rstrip().endswith(";") else ""
        converted = converted.rstrip().removesuffix(";").rstrip()
        converted = f"{converted} ON CONFLICT DO NOTHING{suffix}"
    return converted


class _PostgresConnection:
    def __init__(self, dsn: str) -> None:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - exercised only in postgres deploys
            raise RuntimeError(
                "PostgreSQL database URLs require the psycopg package. "
                "Install Florence with its production dependencies."
            ) from exc
        self._conn = psycopg.connect(dsn, row_factory=dict_row)

    def __enter__(self) -> "_PostgresConnection":
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> object:
        return self._conn.__exit__(exc_type, exc, traceback)

    def execute(
        self,
        sql: str,
        parameters: Iterable[Any] | None = None,
    ):
        pragma = _PRAGMA_TABLE_INFO_RE.match(sql)
        if pragma:
            return self._conn.execute(
                """
                SELECT column_name AS name
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = %s
                """,
                (pragma.group("table"),),
            )
        return self._conn.execute(_postgres_sql(sql), tuple(parameters or ()))

    def executemany(self, sql: str, seq_of_parameters: Iterable[Iterable[Any]]):
        cursor = self._conn.cursor()
        cursor.executemany(_postgres_sql(sql), [tuple(params) for params in seq_of_parameters])
        return cursor

    def executescript(self, sql_script: str) -> None:
        for statement in sql_script.split(";"):
            if statement.strip():
                self.execute(statement)


class Store:
    def __init__(self, path: str) -> None:
        unsupported_scheme = _unsupported_database_url_scheme(path)
        if unsupported_scheme is not None:
            raise ValueError(
                "Unsupported Florence database URL scheme "
                f"{unsupported_scheme!r}. Use a direct SQLite file path for local "
                "development or a postgres:// / postgresql:// URL for deploys."
            )
        self.path = path
        self.backend = "postgres" if _is_postgres_dsn(path) else "sqlite"
        if self.backend == "sqlite" and path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.ensure_schema()

    def connect(self) -> sqlite3.Connection | "_PostgresConnection":
        if self.backend == "postgres":
            return _PostgresConnection(self.path)
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def ping(self) -> None:
        with self.connect() as conn:
            conn.execute("SELECT 1").fetchone()

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS households (
                    id TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL UNIQUE,
                    timezone TEXT NOT NULL,
                    stopped INTEGER NOT NULL DEFAULT 0,
                    memory_enabled INTEGER NOT NULL DEFAULT 1,
                    product_analytics_opt_in INTEGER NOT NULL DEFAULT 0,
                    privacy_updated_at_utc TEXT,
                    created_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS household_members (
                    id TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL REFERENCES households(id),
                    phone TEXT NOT NULL,
                    role TEXT NOT NULL,
                    display_name TEXT,
                    created_at_utc TEXT NOT NULL,
                    last_seen_at_utc TEXT NOT NULL,
                    UNIQUE(household_id, phone)
                );

                CREATE TABLE IF NOT EXISTS connected_accounts (
                    id TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL REFERENCES households(id),
                    provider TEXT NOT NULL,
                    external_account_id TEXT NOT NULL,
                    account_label TEXT,
                    status TEXT NOT NULL,
                    cursor TEXT,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    last_synced_at_utc TEXT,
                    sync_failure_count INTEGER NOT NULL DEFAULT 0,
                    last_sync_error TEXT,
                    retry_after_utc TEXT,
                    UNIQUE(household_id, provider, external_account_id)
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL REFERENCES households(id),
                    actor_member_id TEXT REFERENCES household_members(id),
                    chat_id TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    sender TEXT,
                    body TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS outbound_deliveries (
                    idempotency_key TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL REFERENCES households(id),
                    source_message_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    text TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    delivery_status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    delivered_at_utc TEXT,
                    last_error TEXT,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_outbound_deliveries_source_status
                ON outbound_deliveries(household_id, source_message_id, delivery_status);

                CREATE TABLE IF NOT EXISTS reminders (
                    id TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL REFERENCES households(id),
                    chat_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    assignee_member_id TEXT REFERENCES household_members(id),
                    due_at_utc TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    status TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS household_chat_aliases (
                    chat_id TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL REFERENCES households(id),
                    created_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS source_items (
                    id TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL REFERENCES households(id),
                    connected_account_id TEXT REFERENCES connected_accounts(id),
                    source_type TEXT NOT NULL,
                    external_id TEXT,
                    sender TEXT,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL,
                    observed_at_utc TEXT NOT NULL,
                    event_at_utc TEXT,
                    decision TEXT,
                    reason TEXT,
                    priority INTEGER NOT NULL DEFAULT 0,
                    surfaced_at_utc TEXT,
                    briefed_at_utc TEXT,
                    UNIQUE(household_id, source_type, external_id)
                );

                CREATE TABLE IF NOT EXISTS connected_account_tokens (
                    connected_account_id TEXT PRIMARY KEY REFERENCES connected_accounts(id) ON DELETE CASCADE,
                    provider TEXT NOT NULL,
                    token_ciphertext TEXT NOT NULL,
                    scopes_json TEXT NOT NULL,
                    expires_at_utc TEXT,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS oauth_states (
                    state TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    account_label TEXT,
                    return_path TEXT,
                    created_at_utc TEXT NOT NULL,
                    expires_at_utc TEXT NOT NULL,
                    used_at_utc TEXT
                );

                CREATE TABLE IF NOT EXISTS household_deletion_tombstones (
                    tombstone_key TEXT PRIMARY KEY,
                    deleted_at_utc TEXT NOT NULL,
                    expires_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS source_preferences (
                    id TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL REFERENCES households(id),
                    phrase TEXT NOT NULL,
                    preference TEXT NOT NULL,
                    created_by_member_id TEXT REFERENCES household_members(id),
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    UNIQUE(household_id, phrase)
                );

                CREATE TABLE IF NOT EXISTS source_feedback (
                    id TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL REFERENCES households(id),
                    source_item_id TEXT NOT NULL REFERENCES source_items(id),
                    feedback TEXT NOT NULL,
                    phrase TEXT NOT NULL,
                    created_by_member_id TEXT REFERENCES household_members(id),
                    created_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS memories (
                    id TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL REFERENCES households(id),
                    kind TEXT NOT NULL,
                    subject TEXT,
                    text TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    asserted_by_member_id TEXT REFERENCES household_members(id),
                    source_message_id TEXT,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    expires_at_utc TEXT,
                    deleted_at_utc TEXT,
                    UNIQUE(household_id, kind, subject, text)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_memories_unique_subject_coalesced
                ON memories(
                    household_id,
                    kind,
                    COALESCE(subject, '__florence_null_subject__'),
                    text
                );

                CREATE TABLE IF NOT EXISTS routine_runs (
                    id TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL REFERENCES households(id),
                    routine_name TEXT NOT NULL,
                    local_date TEXT NOT NULL,
                    ran_at_utc TEXT NOT NULL,
                    UNIQUE(household_id, routine_name, local_date)
                );

                CREATE TABLE IF NOT EXISTS pending_actions (
                    id TEXT PRIMARY KEY,
                    household_id TEXT NOT NULL REFERENCES households(id),
                    chat_id TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_by_member_id TEXT REFERENCES household_members(id),
                    created_at_utc TEXT NOT NULL,
                    expires_at_utc TEXT NOT NULL,
                    status TEXT NOT NULL,
                    resolved_by_member_id TEXT REFERENCES household_members(id),
                    resolved_at_utc TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_pending_actions_household_status
                ON pending_actions(household_id, status, expires_at_utc);

                CREATE TABLE IF NOT EXISTS action_executions (
                    id TEXT PRIMARY KEY,
                    action_id TEXT NOT NULL REFERENCES pending_actions(id),
                    household_id TEXT NOT NULL REFERENCES households(id),
                    status TEXT NOT NULL,
                    attempted_at_utc TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    error TEXT,
                    UNIQUE(action_id)
                );

                CREATE TABLE IF NOT EXISTS live_verifications (
                    name TEXT PRIMARY KEY,
                    verified_at_utc TEXT NOT NULL,
                    proof TEXT NOT NULL,
                    source TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                );
                """
            )
            self._ensure_column(conn, "households", "memory_enabled", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column(
                conn,
                "households",
                "product_analytics_opt_in",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(conn, "households", "privacy_updated_at_utc", "TEXT")
            self._ensure_column(
                conn,
                "connected_accounts",
                "sync_failure_count",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(conn, "connected_accounts", "last_sync_error", "TEXT")
            self._ensure_column(conn, "connected_accounts", "retry_after_utc", "TEXT")
            self._ensure_column(conn, "memories", "asserted_by_member_id", "TEXT")
            self._ensure_column(conn, "source_items", "connected_account_id", "TEXT")
            self._ensure_column(conn, "source_items", "briefed_at_utc", "TEXT")
            self._ensure_column(conn, "reminders", "assignee_member_id", "TEXT")
            self._ensure_column(conn, "oauth_states", "return_path", "TEXT")
            self._raise_for_schema_status(self._schema_status(conn))

    def schema_status(self) -> dict[str, Any]:
        with self.connect() as conn:
            return self._schema_status(conn)

    def record_live_verification(
        self,
        *,
        name: str,
        verified_at_utc: datetime,
        proof: str,
        source: str,
        now_utc: datetime,
    ) -> dict[str, str]:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO live_verifications
                    (name, verified_at_utc, proof, source, updated_at_utc)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(name)
                DO UPDATE SET
                    verified_at_utc = excluded.verified_at_utc,
                    proof = excluded.proof,
                    source = excluded.source,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (
                    name,
                    _iso(verified_at_utc),
                    proof,
                    source,
                    _iso(now_utc),
                ),
            )
            row = conn.execute(
                "SELECT * FROM live_verifications WHERE name = ?",
                (name,),
            ).fetchone()
        return _live_verification_record(row)

    def list_live_verifications(self) -> dict[str, dict[str, str]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM live_verifications").fetchall()
        return {row["name"]: _live_verification_record(row) for row in rows}

    @staticmethod
    def _schema_status(conn: sqlite3.Connection | _PostgresConnection) -> dict[str, Any]:
        missing_tables: list[str] = []
        missing_columns: dict[str, list[str]] = {}
        for table_name, required_columns in _SCHEMA_REQUIRED_COLUMNS.items():
            rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            columns = {str(row["name"]) for row in rows}
            if not columns:
                missing_tables.append(table_name)
                continue
            missing = sorted(required_columns - columns)
            if missing:
                missing_columns[table_name] = missing
        return {
            "ready": not missing_tables and not missing_columns,
            "current_version": CURRENT_SCHEMA_VERSION,
            "checked_tables": sorted(_SCHEMA_REQUIRED_COLUMNS),
            "missing_tables": missing_tables,
            "missing_columns": missing_columns,
        }

    @staticmethod
    def _raise_for_schema_status(status: dict[str, Any]) -> None:
        if status["ready"]:
            return
        problems: list[str] = []
        missing_tables = list(status.get("missing_tables") or [])
        missing_columns = dict(status.get("missing_columns") or {})
        if missing_tables:
            problems.append("missing tables: " + ", ".join(missing_tables))
        for table_name, columns in missing_columns.items():
            problems.append(f"{table_name} missing columns: {', '.join(columns)}")
        raise DatabaseSchemaError(
            "Florence database schema is incompatible with this build; "
            + "; ".join(problems)
            + ". Use a fresh Florence database for the pilot or run a deliberate migration "
            "before pointing FLORENCE_DATABASE_URL at this database."
        )

    @staticmethod
    def _ensure_column(
        conn: sqlite3.Connection,
        table_name: str,
        column_name: str,
        column_ddl: str,
    ) -> None:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        if column_name not in {row["name"] for row in rows}:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_ddl}")

    def get_or_create_household(
        self,
        *,
        chat_id: str,
        timezone_name: str,
        now_utc: datetime,
    ) -> Household:
        household = self.get_household_by_chat(chat_id)
        if household is not None:
            return household
        with self.connect() as conn:
            household_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT OR IGNORE INTO households (id, chat_id, timezone, created_at_utc)
                VALUES (?, ?, ?, ?)
                """,
                (household_id, chat_id, timezone_name, _iso(now_utc)),
            )
            row = conn.execute("SELECT * FROM households WHERE chat_id = ?", (chat_id,)).fetchone()
        return _household(row)

    def get_household_by_chat(self, chat_id: str) -> Household | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM households WHERE chat_id = ?", (chat_id,)).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    SELECT households.*
                    FROM household_chat_aliases
                    JOIN households ON households.id = household_chat_aliases.household_id
                    WHERE household_chat_aliases.chat_id = ?
                    """,
                    (chat_id,),
                ).fetchone()
        return _household(row) if row else None

    def get_unique_household_by_member_phone(self, phone: str) -> Household | None:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM households
                WHERE id IN (
                    SELECT DISTINCT household_id
                    FROM household_members
                    WHERE phone = ?
                )
                ORDER BY created_at_utc ASC
                LIMIT 2
                """,
                (phone,),
            ).fetchall()
        if len(rows) != 1:
            return None
        return _household(rows[0])

    def get_household_by_id(self, household_id: str) -> Household | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM households WHERE id = ?", (household_id,)).fetchone()
        return _household(row) if row else None

    def list_households(self) -> list[Household]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM households
                WHERE stopped = 0
                ORDER BY created_at_utc ASC
                """
            ).fetchall()
        return [_household(row) for row in rows]

    def delete_household(self, household_id: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT chat_id FROM households WHERE id = ?",
                (household_id,),
            ).fetchone()
            if row is None:
                return False
            chat_ids = [row["chat_id"]]
            alias_rows = conn.execute(
                "SELECT chat_id FROM household_chat_aliases WHERE household_id = ?",
                (household_id,),
            ).fetchall()
            chat_ids.extend(alias["chat_id"] for alias in alias_rows)
            placeholders = ",".join("?" for _ in chat_ids)
            conn.execute(
                f"DELETE FROM oauth_states WHERE chat_id IN ({placeholders})",
                tuple(chat_ids),
            )
            conn.execute("DELETE FROM action_executions WHERE household_id = ?", (household_id,))
            conn.execute("DELETE FROM pending_actions WHERE household_id = ?", (household_id,))
            conn.execute("DELETE FROM routine_runs WHERE household_id = ?", (household_id,))
            conn.execute("DELETE FROM source_feedback WHERE household_id = ?", (household_id,))
            conn.execute("DELETE FROM source_preferences WHERE household_id = ?", (household_id,))
            conn.execute("DELETE FROM source_items WHERE household_id = ?", (household_id,))
            conn.execute(
                """
                DELETE FROM connected_account_tokens
                WHERE connected_account_id IN (
                    SELECT id FROM connected_accounts WHERE household_id = ?
                )
                """,
                (household_id,),
            )
            conn.execute("DELETE FROM connected_accounts WHERE household_id = ?", (household_id,))
            conn.execute("DELETE FROM outbound_deliveries WHERE household_id = ?", (household_id,))
            conn.execute("DELETE FROM reminders WHERE household_id = ?", (household_id,))
            conn.execute("DELETE FROM messages WHERE household_id = ?", (household_id,))
            conn.execute("DELETE FROM memories WHERE household_id = ?", (household_id,))
            conn.execute("DELETE FROM household_chat_aliases WHERE household_id = ?", (household_id,))
            conn.execute("DELETE FROM household_members WHERE household_id = ?", (household_id,))
            cur = conn.execute("DELETE FROM households WHERE id = ?", (household_id,))
        return cur.rowcount > 0

    def record_household_deletion_message_tombstones(
        self,
        *,
        household_id: str,
        deleted_at_utc: datetime,
        expires_at_utc: datetime,
    ) -> int:
        with self.connect() as conn:
            conn.execute(
                "DELETE FROM household_deletion_tombstones WHERE expires_at_utc <= ?",
                (_iso(deleted_at_utc),),
            )
            rows = conn.execute(
                """
                SELECT chat_id, id
                FROM messages
                WHERE household_id = ?
                  AND direction = ?
                """,
                (household_id, MessageDirection.INBOUND.value),
            ).fetchall()
            tombstones = [
                (
                    _household_deletion_tombstone_key(
                        chat_id=str(row["chat_id"]),
                        message_id=str(row["id"]),
                    ),
                    _iso(deleted_at_utc),
                    _iso(expires_at_utc),
                )
                for row in rows
            ]
            if tombstones:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO household_deletion_tombstones
                        (tombstone_key, deleted_at_utc, expires_at_utc)
                    VALUES (?, ?, ?)
                    """,
                    tombstones,
                )
        return len(tombstones)

    def has_household_deletion_tombstone(
        self,
        *,
        chat_id: str,
        message_id: str,
        now_utc: datetime,
    ) -> bool:
        tombstone_key = _household_deletion_tombstone_key(chat_id=chat_id, message_id=message_id)
        now = ensure_utc(now_utc)
        with self.connect() as conn:
            conn.execute(
                "DELETE FROM household_deletion_tombstones WHERE expires_at_utc <= ?",
                (_iso(now),),
            )
            row = conn.execute(
                """
                SELECT 1 FROM household_deletion_tombstones
                WHERE tombstone_key = ?
                  AND expires_at_utc > ?
                """,
                (tombstone_key, _iso(now)),
            ).fetchone()
        return row is not None

    def update_household_timezone(self, household_id: str, timezone_name: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE households SET timezone = ? WHERE id = ?",
                (timezone_name, household_id),
            )

    def migrate_household_chat(
        self,
        *,
        household_id: str,
        new_chat_id: str,
        now_utc: datetime,
    ) -> Household:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM households WHERE id = ?", (household_id,)).fetchone()
            if row is None:
                raise ValueError("unknown household")
            old_chat_id = row["chat_id"]
            if old_chat_id == new_chat_id:
                return _household(row)
            conn.execute(
                """
                INSERT OR IGNORE INTO household_chat_aliases
                    (chat_id, household_id, created_at_utc)
                VALUES (?, ?, ?)
                """,
                (old_chat_id, household_id, _iso(now_utc)),
            )
            conn.execute(
                "UPDATE households SET chat_id = ? WHERE id = ?",
                (new_chat_id, household_id),
            )
            conn.execute(
                """
                UPDATE reminders
                SET chat_id = ?
                WHERE household_id = ? AND status = ?
                """,
                (new_chat_id, household_id, ReminderStatus.PENDING.value),
            )
            conn.execute(
                """
                UPDATE pending_actions
                SET chat_id = ?
                WHERE household_id = ? AND status = ?
                """,
                (new_chat_id, household_id, PendingActionStatus.PENDING.value),
            )
            conn.execute(
                """
                UPDATE oauth_states
                SET chat_id = ?
                WHERE chat_id = ? AND used_at_utc IS NULL
                """,
                (new_chat_id, old_chat_id),
            )
            delivery_rows = conn.execute(
                """
                SELECT idempotency_key, payload_json
                FROM outbound_deliveries
                WHERE household_id = ?
                  AND chat_id = ?
                  AND delivery_status IN (?, ?)
                """,
                (
                    household_id,
                    old_chat_id,
                    OutboundDeliveryStatus.PENDING.value,
                    OutboundDeliveryStatus.FAILED.value,
                ),
            ).fetchall()
            for delivery in delivery_rows:
                conn.execute(
                    """
                    UPDATE outbound_deliveries
                    SET chat_id = ?,
                        payload_json = ?,
                        updated_at_utc = ?
                    WHERE idempotency_key = ?
                    """,
                    (
                        new_chat_id,
                        _retarget_outbound_payload_chat(
                            delivery["payload_json"],
                            old_chat_id,
                            new_chat_id,
                        ),
                        _iso(now_utc),
                        delivery["idempotency_key"],
                    ),
                )
            row = conn.execute("SELECT * FROM households WHERE id = ?", (household_id,)).fetchone()
        return _household(row)

    def get_or_create_member(
        self,
        *,
        household_id: str,
        phone: str,
        now_utc: datetime,
    ) -> HouseholdMember:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM household_members
                WHERE household_id = ? AND phone = ?
                """,
                (household_id, phone),
            ).fetchone()
            if row is None:
                member_count = conn.execute(
                    """
                    SELECT COUNT(*) AS count FROM household_members
                    WHERE household_id = ?
                    """,
                    (household_id,),
                ).fetchone()["count"]
                role = MemberRole.PARENT if member_count == 0 else MemberRole.HELPER
                member_id = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO household_members
                    (id, household_id, phone, role, created_at_utc, last_seen_at_utc)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (member_id, household_id, phone, role.value, _iso(now_utc), _iso(now_utc)),
                )
                row = conn.execute(
                    "SELECT * FROM household_members WHERE id = ?",
                    (member_id,),
                ).fetchone()
            else:
                conn.execute(
                    """
                    UPDATE household_members SET last_seen_at_utc = ?
                    WHERE id = ?
                    """,
                    (_iso(now_utc), row["id"]),
                )
                row = conn.execute(
                    "SELECT * FROM household_members WHERE id = ?",
                    (row["id"],),
                ).fetchone()
        return _member(row)

    def ensure_parent_member(
        self,
        *,
        household_id: str,
        phone: str,
        now_utc: datetime,
    ) -> HouseholdMember | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM household_members
                WHERE household_id = ? AND phone = ?
                """,
                (household_id, phone),
            ).fetchone()
            if row is not None and row["role"] == MemberRole.PARENT.value:
                conn.execute(
                    """
                    UPDATE household_members SET last_seen_at_utc = ?
                    WHERE id = ?
                    """,
                    (_iso(now_utc), row["id"]),
                )
                row = conn.execute(
                    "SELECT * FROM household_members WHERE id = ?",
                    (row["id"],),
                ).fetchone()
                return _member(row)

            parent_count = conn.execute(
                """
                SELECT COUNT(*) AS count FROM household_members
                WHERE household_id = ? AND role = ?
                """,
                (household_id, MemberRole.PARENT.value),
            ).fetchone()["count"]
            if parent_count >= 2:
                return None

            if row is None:
                member_id = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO household_members
                    (id, household_id, phone, role, created_at_utc, last_seen_at_utc)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        member_id,
                        household_id,
                        phone,
                        MemberRole.PARENT.value,
                        _iso(now_utc),
                        _iso(now_utc),
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM household_members WHERE id = ?",
                    (member_id,),
                ).fetchone()
            else:
                conn.execute(
                    """
                    UPDATE household_members
                    SET role = ?, last_seen_at_utc = ?
                    WHERE id = ?
                    """,
                    (MemberRole.PARENT.value, _iso(now_utc), row["id"]),
                )
                row = conn.execute(
                    "SELECT * FROM household_members WHERE id = ?",
                    (row["id"],),
                ).fetchone()
        return _member(row)

    def set_member_name(self, member_id: str, display_name: str, *, now_utc: datetime) -> HouseholdMember:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE household_members
                SET display_name = ?, last_seen_at_utc = ?
                WHERE id = ?
                """,
                (display_name, _iso(now_utc), member_id),
            )
            row = conn.execute("SELECT * FROM household_members WHERE id = ?", (member_id,)).fetchone()
        return _member(row)

    def list_members(self, household_id: str) -> list[HouseholdMember]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM household_members
                WHERE household_id = ?
                ORDER BY
                  CASE role WHEN 'parent' THEN 0 ELSE 1 END,
                  created_at_utc ASC
                """,
                (household_id,),
            ).fetchall()
        return [_member(row) for row in rows]

    def set_stopped(self, household_id: str, stopped: bool) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE households SET stopped = ? WHERE id = ?",
                (1 if stopped else 0, household_id),
            )

    def is_stopped(self, household_id: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT stopped FROM households WHERE id = ?",
                (household_id,),
            ).fetchone()
        return bool(row and row["stopped"])

    def household_privacy(self, household_id: str) -> HouseholdPrivacy:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    id,
                    memory_enabled,
                    product_analytics_opt_in,
                    COALESCE(privacy_updated_at_utc, created_at_utc) AS privacy_updated_at_utc
                FROM households
                WHERE id = ?
                """,
                (household_id,),
            ).fetchone()
        if row is None:
            raise ValueError("unknown household")
        return HouseholdPrivacy(
            household_id=row["id"],
            mode=PrivacyMode.MAXIMUM,
            memory_enabled=bool(row["memory_enabled"]),
            product_analytics_opt_in=bool(row["product_analytics_opt_in"]),
            updated_at_utc=_dt(row["privacy_updated_at_utc"]),
        )

    def household_data_summary(self, *, household_id: str, now_utc: datetime) -> HouseholdDataSummary:
        now_iso = _iso(now_utc)
        with self.connect() as conn:
            household = conn.execute(
                """
                SELECT id, timezone, stopped, memory_enabled, product_analytics_opt_in
                FROM households
                WHERE id = ?
                """,
                (household_id,),
            ).fetchone()
            if household is None:
                raise ValueError("unknown household")
            member_counts = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN role = ? THEN 1 ELSE 0 END) AS parents,
                    SUM(CASE WHEN role = ? THEN 1 ELSE 0 END) AS helpers
                FROM household_members
                WHERE household_id = ?
                """,
                (MemberRole.PARENT.value, MemberRole.HELPER.value, household_id),
            ).fetchone()
            source_counts = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN decision = ? AND surfaced_at_utc IS NOT NULL THEN 1 ELSE 0 END) AS surfaced,
                    SUM(CASE WHEN decision = ? THEN 1 ELSE 0 END) AS stored,
                    SUM(CASE WHEN decision = ? THEN 1 ELSE 0 END) AS suppressed
                FROM source_items
                WHERE household_id = ?
                """,
                (
                    SourceDecision.SURFACE.value,
                    SourceDecision.STORE_ONLY.value,
                    SourceDecision.SUPPRESS.value,
                    household_id,
                ),
            ).fetchone()
            message_count = conn.execute(
                "SELECT COUNT(*) AS count FROM messages WHERE household_id = ?",
                (household_id,),
            ).fetchone()["count"]
            active_reminders = conn.execute(
                """
                SELECT COUNT(*) AS count FROM reminders
                WHERE household_id = ? AND status IN (?, ?)
                """,
                (household_id, ReminderStatus.PENDING.value, ReminderStatus.SENT.value),
            ).fetchone()["count"]
            connected_accounts = conn.execute(
                """
                SELECT COUNT(*) AS count FROM connected_accounts
                WHERE household_id = ? AND status = ?
                """,
                (household_id, ConnectedAccountStatus.ACTIVE.value),
            ).fetchone()["count"]
            source_preferences = conn.execute(
                "SELECT COUNT(*) AS count FROM source_preferences WHERE household_id = ?",
                (household_id,),
            ).fetchone()["count"]
            active_memories = conn.execute(
                """
                SELECT COUNT(*) AS count FROM memories
                WHERE household_id = ?
                  AND deleted_at_utc IS NULL
                  AND (expires_at_utc IS NULL OR expires_at_utc > ?)
                """,
                (household_id, now_iso),
            ).fetchone()["count"]
            pending_actions = conn.execute(
                """
                SELECT COUNT(*) AS count FROM pending_actions
                WHERE household_id = ? AND status = ? AND expires_at_utc > ?
                """,
                (household_id, PendingActionStatus.PENDING.value, now_iso),
            ).fetchone()["count"]
        return HouseholdDataSummary(
            household_id=household["id"],
            timezone=household["timezone"],
            stopped=bool(household["stopped"]),
            memory_enabled=bool(household["memory_enabled"]),
            product_analytics_opt_in=bool(household["product_analytics_opt_in"]),
            member_count=int(member_counts["total"] or 0),
            parent_count=int(member_counts["parents"] or 0),
            helper_count=int(member_counts["helpers"] or 0),
            message_count=int(message_count or 0),
            active_reminder_count=int(active_reminders or 0),
            source_item_count=int(source_counts["total"] or 0),
            surfaced_source_item_count=int(source_counts["surfaced"] or 0),
            stored_source_item_count=int(source_counts["stored"] or 0),
            suppressed_source_item_count=int(source_counts["suppressed"] or 0),
            connected_account_count=int(connected_accounts or 0),
            source_preference_count=int(source_preferences or 0),
            active_memory_count=int(active_memories or 0),
            pending_action_count=int(pending_actions or 0),
        )

    def has_recent_data_deletion_request(
        self,
        *,
        household_id: str,
        actor_member_id: str,
        request_commands: Iterable[str],
        since_utc: datetime,
        before_utc: datetime,
    ) -> bool:
        normalized_commands = tuple(_normalize_command(command) for command in request_commands if command.strip())
        if not normalized_commands:
            return False
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT body
                FROM messages
                WHERE household_id = ?
                  AND actor_member_id = ?
                  AND direction = ?
                  AND created_at_utc >= ?
                  AND created_at_utc <= ?
                ORDER BY created_at_utc DESC
                LIMIT 20
                """,
                (
                    household_id,
                    actor_member_id,
                    MessageDirection.INBOUND.value,
                    _iso(since_utc),
                    _iso(before_utc),
                ),
            ).fetchall()
        return any(_normalize_command(str(row["body"])) in normalized_commands for row in rows)

    def update_memory_enabled(
        self,
        *,
        household_id: str,
        enabled: bool,
        now_utc: datetime,
    ) -> HouseholdPrivacy:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE households
                SET memory_enabled = ?,
                    privacy_updated_at_utc = ?
                WHERE id = ?
                """,
                (1 if enabled else 0, _iso(now_utc), household_id),
            )
        return self.household_privacy(household_id)

    def update_product_analytics_opt_in(
        self,
        *,
        household_id: str,
        opted_in: bool,
        now_utc: datetime,
    ) -> HouseholdPrivacy:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE households
                SET product_analytics_opt_in = ?,
                    privacy_updated_at_utc = ?
                WHERE id = ?
                """,
                (1 if opted_in else 0, _iso(now_utc), household_id),
            )
        return self.household_privacy(household_id)

    def save_message(
        self,
        *,
        household_id: str,
        chat_id: str,
        direction: MessageDirection,
        body: str,
        created_at_utc: datetime,
        sender: str | None = None,
        message_id: str | None = None,
        actor_member_id: str | None = None,
    ) -> bool:
        row_id = message_id or str(uuid.uuid4())
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO messages
                (id, household_id, actor_member_id, chat_id, direction, sender, body, created_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row_id,
                    household_id,
                    actor_member_id,
                    chat_id,
                    direction.value,
                    sender,
                    body,
                    _iso(created_at_utc),
                ),
            )
        return cur.rowcount > 0

    def record_outbound_deliveries_for_source(
        self,
        *,
        household_id: str,
        source_message_id: str,
        messages: Iterable[OutboundMessage],
        now_utc: datetime,
    ) -> None:
        rows = [
            (
                message.idempotency_key,
                household_id,
                source_message_id,
                message.chat_id,
                message.text,
                json.dumps(_outbound_payload(message), sort_keys=True),
                OutboundDeliveryStatus.PENDING.value,
                _iso(now_utc),
                _iso(now_utc),
            )
            for message in messages
        ]
        if not rows:
            return
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO outbound_deliveries
                (
                    idempotency_key, household_id, source_message_id, chat_id,
                    text, payload_json, delivery_status, created_at_utc, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def retryable_outbound_deliveries_for_source(
        self,
        *,
        household_id: str,
        source_message_id: str,
    ) -> list[OutboundMessage]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM outbound_deliveries
                WHERE household_id = ?
                  AND source_message_id = ?
                  AND delivery_status IN (?, ?)
                ORDER BY created_at_utc ASC, idempotency_key ASC
                """,
                (
                    household_id,
                    source_message_id,
                    OutboundDeliveryStatus.PENDING.value,
                    OutboundDeliveryStatus.FAILED.value,
                ),
            ).fetchall()
        return [_outbound_delivery(row) for row in rows]

    def retryable_outbound_deliveries_by_source_prefix(
        self,
        *,
        source_message_prefix: str,
        limit: int = 50,
    ) -> list[OutboundMessage]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT outbound_deliveries.*
                FROM outbound_deliveries
                JOIN households ON households.id = outbound_deliveries.household_id
                WHERE outbound_deliveries.source_message_id LIKE ?
                  AND outbound_deliveries.delivery_status IN (?, ?)
                  AND households.stopped = 0
                ORDER BY outbound_deliveries.created_at_utc ASC,
                         outbound_deliveries.idempotency_key ASC
                LIMIT ?
                """,
                (
                    f"{source_message_prefix}%",
                    OutboundDeliveryStatus.PENDING.value,
                    OutboundDeliveryStatus.FAILED.value,
                    limit,
                ),
            ).fetchall()
        return [_outbound_delivery(row) for row in rows]

    def retryable_source_outbound_deliveries_for_source(
        self,
        *,
        household_id: str,
        source_message_id: str,
    ) -> list[OutboundMessage]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT outbound_deliveries.*
                FROM outbound_deliveries
                JOIN households ON households.id = outbound_deliveries.household_id
                JOIN source_items
                  ON source_items.household_id = outbound_deliveries.household_id
                 AND outbound_deliveries.source_message_id = ('source:' || source_items.id)
                LEFT JOIN connected_accounts
                  ON connected_accounts.id = source_items.connected_account_id
                WHERE outbound_deliveries.household_id = ?
                  AND outbound_deliveries.source_message_id = ?
                  AND outbound_deliveries.delivery_status IN (?, ?)
                  AND households.stopped = 0
                  AND (
                    source_items.connected_account_id IS NULL
                    OR connected_accounts.status = ?
                  )
                ORDER BY outbound_deliveries.created_at_utc ASC,
                         outbound_deliveries.idempotency_key ASC
                """,
                (
                    household_id,
                    source_message_id,
                    OutboundDeliveryStatus.PENDING.value,
                    OutboundDeliveryStatus.FAILED.value,
                    ConnectedAccountStatus.ACTIVE.value,
                ),
            ).fetchall()
        return [_outbound_delivery(row) for row in rows]

    def retryable_source_outbound_deliveries(self, *, limit: int = 50) -> list[OutboundMessage]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT outbound_deliveries.*
                FROM outbound_deliveries
                JOIN households ON households.id = outbound_deliveries.household_id
                JOIN source_items
                  ON source_items.household_id = outbound_deliveries.household_id
                 AND outbound_deliveries.source_message_id = ('source:' || source_items.id)
                LEFT JOIN connected_accounts
                  ON connected_accounts.id = source_items.connected_account_id
                WHERE outbound_deliveries.source_message_id LIKE ?
                  AND outbound_deliveries.delivery_status IN (?, ?)
                  AND households.stopped = 0
                  AND (
                    source_items.connected_account_id IS NULL
                    OR connected_accounts.status = ?
                  )
                ORDER BY outbound_deliveries.created_at_utc ASC,
                         outbound_deliveries.idempotency_key ASC
                LIMIT ?
                """,
                (
                    "source:%",
                    OutboundDeliveryStatus.PENDING.value,
                    OutboundDeliveryStatus.FAILED.value,
                    ConnectedAccountStatus.ACTIVE.value,
                    limit,
                ),
            ).fetchall()
        return [_outbound_delivery(row) for row in rows]

    def mark_outbound_delivery_sent(self, *, idempotency_key: str, now_utc: datetime) -> bool:
        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE outbound_deliveries
                SET delivery_status = ?,
                    attempts = attempts + 1,
                    delivered_at_utc = ?,
                    last_error = NULL,
                    updated_at_utc = ?
                WHERE idempotency_key = ?
                  AND delivery_status != ?
                """,
                (
                    OutboundDeliveryStatus.SENT.value,
                    _iso(now_utc),
                    _iso(now_utc),
                    idempotency_key,
                    OutboundDeliveryStatus.SENT.value,
                ),
            )
        return cur.rowcount > 0

    def mark_outbound_delivery_failed(
        self,
        *,
        idempotency_key: str,
        error: str,
        now_utc: datetime,
    ) -> bool:
        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE outbound_deliveries
                SET delivery_status = ?,
                    attempts = attempts + 1,
                    last_error = ?,
                    updated_at_utc = ?
                WHERE idempotency_key = ?
                  AND delivery_status != ?
                """,
                (
                    OutboundDeliveryStatus.FAILED.value,
                    error[:1000],
                    _iso(now_utc),
                    idempotency_key,
                    OutboundDeliveryStatus.SENT.value,
                ),
            )
        return cur.rowcount > 0

    def outbound_delivery_summary(
        self,
        *,
        household_id: str,
        issue_limit: int = 10,
    ) -> dict[str, object]:
        with self.connect() as conn:
            status_rows = conn.execute(
                """
                SELECT delivery_status, COUNT(*) AS count
                FROM outbound_deliveries
                WHERE household_id = ?
                GROUP BY delivery_status
                """,
                (household_id,),
            ).fetchall()
            issue_rows = conn.execute(
                """
                SELECT
                    idempotency_key,
                    source_message_id,
                    delivery_status,
                    attempts,
                    last_error,
                    created_at_utc,
                    updated_at_utc
                FROM outbound_deliveries
                WHERE household_id = ?
                  AND delivery_status IN (?, ?)
                ORDER BY updated_at_utc ASC, created_at_utc ASC
                LIMIT ?
                """,
                (
                    household_id,
                    OutboundDeliveryStatus.PENDING.value,
                    OutboundDeliveryStatus.FAILED.value,
                    issue_limit,
                ),
            ).fetchall()
        counts = {status.value: 0 for status in OutboundDeliveryStatus}
        for row in status_rows:
            counts[str(row["delivery_status"])] = int(row["count"] or 0)
        issues = [
            {
                "idempotency_key": row["idempotency_key"],
                "source_message_id": row["source_message_id"],
                "status": row["delivery_status"],
                "attempts": int(row["attempts"] or 0),
                "last_error": row["last_error"],
                "created_at_utc": row["created_at_utc"],
                "updated_at_utc": row["updated_at_utc"],
            }
            for row in issue_rows
        ]
        retryable = counts[OutboundDeliveryStatus.PENDING.value] + counts[OutboundDeliveryStatus.FAILED.value]
        return {
            "ready": retryable == 0,
            "pending": counts[OutboundDeliveryStatus.PENDING.value],
            "failed": counts[OutboundDeliveryStatus.FAILED.value],
            "sent": counts[OutboundDeliveryStatus.SENT.value],
            "canceled": counts[OutboundDeliveryStatus.CANCELED.value],
            "retryable": retryable,
            "issues": issues,
        }

    def message_transport_summary(self, *, household_id: str) -> dict[str, object]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN direction = ? THEN 1 ELSE 0 END) AS inbound,
                    SUM(CASE WHEN direction = ? THEN 1 ELSE 0 END) AS outbound,
                    MAX(CASE WHEN direction = ? THEN created_at_utc ELSE NULL END) AS latest_inbound_at_utc,
                    MAX(CASE WHEN direction = ? THEN created_at_utc ELSE NULL END) AS latest_outbound_at_utc
                FROM messages
                WHERE household_id = ?
                """,
                (
                    MessageDirection.INBOUND.value,
                    MessageDirection.OUTBOUND.value,
                    MessageDirection.INBOUND.value,
                    MessageDirection.OUTBOUND.value,
                    household_id,
                ),
            ).fetchone()
        inbound = int(row["inbound"] or 0) if row else 0
        outbound = int(row["outbound"] or 0) if row else 0
        missing = []
        if inbound == 0:
            missing.append("At least one inbound iMessage received by Florence.")
        if outbound == 0:
            missing.append("At least one successfully recorded outbound iMessage.")
        return {
            "ready": not missing,
            "inbound": inbound,
            "outbound": outbound,
            "latest_inbound_at_utc": row["latest_inbound_at_utc"] if row else None,
            "latest_outbound_at_utc": row["latest_outbound_at_utc"] if row else None,
            "missing": missing,
        }

    def recent_messages(
        self,
        household_id: str,
        *,
        limit: int = 12,
        since_utc: datetime | None = None,
        exclude_message_id: str | None = None,
    ) -> list[dict[str, str]]:
        filters = [
            "messages.household_id = ?",
            """
                  (
                    messages.direction != ?
                    OR outbound_deliveries.idempotency_key IS NULL
                    OR outbound_deliveries.delivery_status = ?
                  )
            """,
        ]
        params: list[object] = [
            MessageDirection.OUTBOUND.value,
            household_id,
            MessageDirection.OUTBOUND.value,
            OutboundDeliveryStatus.SENT.value,
        ]
        if since_utc is not None:
            filters.append("messages.created_at_utc >= ?")
            params.append(_iso(since_utc))
        if exclude_message_id is not None:
            filters.append("messages.id != ?")
            params.append(exclude_message_id)
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT messages.direction, messages.body
                FROM messages
                LEFT JOIN outbound_deliveries
                  ON outbound_deliveries.idempotency_key = messages.id
                 AND messages.direction = ?
                WHERE {' AND '.join(filters)}
                ORDER BY messages.created_at_utc DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        history = []
        for row in reversed(rows):
            role = "assistant" if row["direction"] == MessageDirection.OUTBOUND.value else "user"
            history.append({"role": role, "content": row["body"]})
        return history

    def create_reminder(
        self,
        *,
        household_id: str,
        chat_id: str,
        title: str,
        due_at_utc: datetime,
        created_at_utc: datetime,
        assignee_member_id: str | None = None,
    ) -> Reminder:
        reminder = Reminder(
            id=str(uuid.uuid4()),
            household_id=household_id,
            chat_id=chat_id,
            title=title,
            due_at_utc=ensure_utc(due_at_utc),
            created_at_utc=ensure_utc(created_at_utc),
            status=ReminderStatus.PENDING,
            assignee_member_id=assignee_member_id,
        )
        with self.connect() as conn:
            if assignee_member_id is not None:
                member = conn.execute(
                    """
                    SELECT id FROM household_members
                    WHERE id = ? AND household_id = ?
                    """,
                    (assignee_member_id, household_id),
                ).fetchone()
                if member is None:
                    raise ValueError("reminder assignee_member_id must belong to the household")

            conn.execute(
                """
                INSERT INTO reminders
                (
                    id, household_id, chat_id, title, assignee_member_id,
                    due_at_utc, created_at_utc, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    reminder.id,
                    reminder.household_id,
                    reminder.chat_id,
                    reminder.title,
                    reminder.assignee_member_id,
                    _iso(reminder.due_at_utc),
                    _iso(reminder.created_at_utc),
                    reminder.status.value,
                ),
            )
        return reminder

    def due_reminders(
        self,
        *,
        now_utc: datetime,
        not_before_utc: datetime | None = None,
        limit: int = 50,
    ) -> list[Reminder]:
        params: list[object] = [ReminderStatus.PENDING.value, _iso(now_utc)]
        not_before_sql = ""
        if not_before_utc is not None:
            not_before_sql = "AND reminders.due_at_utc >= ?"
            params.append(_iso(not_before_utc))
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT reminders.*
                FROM reminders
                JOIN households ON households.id = reminders.household_id
                WHERE reminders.status = ?
                  AND reminders.due_at_utc <= ?
                  AND households.stopped = 0
                {not_before_sql}
                ORDER BY reminders.due_at_utc ASC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [_reminder(row) for row in rows]

    def expire_stale_reminders(self, *, before_utc: datetime) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE reminders
                SET status = ?
                WHERE status = ?
                  AND due_at_utc < ?
                  AND EXISTS (
                    SELECT 1
                    FROM households
                    WHERE households.id = reminders.household_id
                      AND households.stopped = 0
                  )
                """,
                (ReminderStatus.EXPIRED.value, ReminderStatus.PENDING.value, _iso(before_utc)),
            )
        return cur.rowcount

    def mark_reminders_sent(self, reminder_ids: Iterable[str]) -> None:
        ids = list(reminder_ids)
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        with self.connect() as conn:
            conn.execute(
                f"UPDATE reminders SET status = ? WHERE id IN ({placeholders})",
                [ReminderStatus.SENT.value, *ids],
            )

    def find_pending_reminders_by_query(
        self,
        *,
        household_id: str,
        query: str,
        now_utc: datetime,
        limit: int = 5,
    ) -> list[Reminder]:
        normalized = " ".join(query.strip().lower().split())
        if not normalized:
            return []
        pattern = f"%{normalized}%"
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM reminders
                WHERE household_id = ?
                  AND status = ?
                  AND lower(title) LIKE ?
                ORDER BY ABS(strftime('%s', due_at_utc) - strftime('%s', ?)) ASC,
                         due_at_utc ASC
                LIMIT ?
                """,
                (
                    household_id,
                    ReminderStatus.PENDING.value,
                    pattern,
                    _iso(now_utc),
                    limit,
                ),
            ).fetchall()
        return [_reminder(row) for row in rows]

    def recent_sent_reminders(
        self,
        *,
        household_id: str,
        now_utc: datetime,
        since_utc: datetime,
        limit: int = 4,
    ) -> list[Reminder]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM reminders
                WHERE household_id = ?
                  AND status = ?
                  AND due_at_utc <= ?
                  AND due_at_utc >= ?
                ORDER BY due_at_utc DESC
                LIMIT ?
                """,
                (
                    household_id,
                    ReminderStatus.SENT.value,
                    _iso(now_utc),
                    _iso(since_utc),
                    limit,
                ),
            ).fetchall()
        return [_reminder(row) for row in rows]

    def update_reminder_status(
        self,
        *,
        household_id: str,
        reminder_id: str,
        status: ReminderStatus,
    ) -> Reminder | None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE reminders
                SET status = ?
                WHERE household_id = ?
                  AND id = ?
                  AND status = ?
                """,
                (
                    status.value,
                    household_id,
                    reminder_id,
                    ReminderStatus.PENDING.value,
                ),
            )
            row = conn.execute(
                "SELECT * FROM reminders WHERE household_id = ? AND id = ?",
                (household_id, reminder_id),
            ).fetchone()
        if row is None or row["status"] != status.value:
            return None
        return _reminder(row)

    def mark_sent_reminder_completed(
        self,
        *,
        household_id: str,
        reminder_id: str,
    ) -> Reminder | None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE reminders
                SET status = ?
                WHERE household_id = ?
                  AND id = ?
                  AND status = ?
                """,
                (
                    ReminderStatus.COMPLETED.value,
                    household_id,
                    reminder_id,
                    ReminderStatus.SENT.value,
                ),
            )
            row = conn.execute(
                "SELECT * FROM reminders WHERE household_id = ? AND id = ?",
                (household_id, reminder_id),
            ).fetchone()
        if row is None or row["status"] != ReminderStatus.COMPLETED.value:
            return None
        return _reminder(row)

    def claim_routine_run(
        self,
        *,
        household_id: str,
        routine_name: str,
        local_date: str,
        ran_at_utc: datetime,
    ) -> bool:
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO routine_runs
                (id, household_id, routine_name, local_date, ran_at_utc)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(uuid.uuid4()), household_id, routine_name, local_date, _iso(ran_at_utc)),
            )
        return cur.rowcount > 0

    def routine_run_exists(
        self,
        *,
        household_id: str,
        routine_name: str,
        local_date: str,
    ) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM routine_runs
                WHERE household_id = ?
                  AND routine_name = ?
                  AND local_date = ?
                """,
                (household_id, routine_name, local_date),
            ).fetchone()
        return row is not None

    def last_surfaced_source_item(self, household_id: str) -> SourceItem | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM source_items
                WHERE household_id = ?
                  AND decision = ?
                  AND surfaced_at_utc IS NOT NULL
                ORDER BY surfaced_at_utc DESC, observed_at_utc DESC
                LIMIT 1
                """,
                (household_id, SourceDecision.SURFACE.value),
            ).fetchone()
        return _source_item(row) if row else None

    def record_source_feedback(
        self,
        *,
        household_id: str,
        source_item_id: str,
        feedback: SourceFeedbackKind,
        phrase: str,
        created_at_utc: datetime,
        created_by_member_id: str | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO source_feedback
                (
                    id, household_id, source_item_id, feedback, phrase,
                    created_by_member_id, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    household_id,
                    source_item_id,
                    feedback.value,
                    phrase,
                    created_by_member_id,
                    _iso(created_at_utc),
                ),
            )

    def add_source_item(
        self,
        item: SourceItem,
        *,
        decision: str,
        reason: str,
        priority: int,
        surfaced_at_utc: datetime | None = None,
    ) -> bool:
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO source_items
                (
                    id, household_id, connected_account_id, source_type, external_id,
                    sender, title, body, observed_at_utc, event_at_utc, decision,
                    reason, priority, surfaced_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.id,
                    item.household_id,
                    item.connected_account_id,
                    item.source_type,
                    item.external_id,
                    item.sender,
                    item.title,
                    item.body,
                    _iso(item.observed_at_utc),
                    _iso(item.event_at_utc) if item.event_at_utc else None,
                    decision,
                    reason,
                    priority,
                    _iso(surfaced_at_utc) if surfaced_at_utc else None,
                ),
            )
        return cur.rowcount > 0

    def has_recent_similar_source_item(
        self,
        *,
        household_id: str,
        source_type: str,
        sender: str | None,
        title: str,
        since_utc: datetime,
        exclude_id: str | None = None,
    ) -> bool:
        filters = [
            "household_id = ?",
            "source_type = ?",
            "LOWER(title) = LOWER(?)",
            "LOWER(COALESCE(sender, '')) = LOWER(?)",
            "observed_at_utc >= ?",
        ]
        params: list[object] = [
            household_id,
            source_type,
            title,
            sender or "",
            _iso(since_utc),
        ]
        if exclude_id is not None:
            filters.append("id != ?")
            params.append(exclude_id)
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT 1
                FROM source_items
                WHERE {' AND '.join(filters)}
                LIMIT 1
                """,
                params,
            ).fetchone()
        return row is not None

    def upsert_connected_account(
        self,
        *,
        household_id: str,
        provider: str,
        external_account_id: str,
        now_utc: datetime,
        account_label: str | None = None,
        cursor: str | None = None,
    ) -> ConnectedAccount:
        provider = _normalize_key(provider)
        external_account_id = _normalize_key(external_account_id)
        account_id = str(uuid.uuid4())
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO connected_accounts
                (
                    id, household_id, provider, external_account_id, account_label,
                    status, cursor, created_at_utc, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(household_id, provider, external_account_id)
                DO UPDATE SET
                    account_label = COALESCE(excluded.account_label, connected_accounts.account_label),
                    status = ?,
                    cursor = COALESCE(excluded.cursor, connected_accounts.cursor),
                    updated_at_utc = excluded.updated_at_utc
                """,
                (
                    account_id,
                    household_id,
                    provider,
                    external_account_id,
                    account_label,
                    ConnectedAccountStatus.ACTIVE.value,
                    cursor,
                    _iso(now_utc),
                    _iso(now_utc),
                    ConnectedAccountStatus.ACTIVE.value,
                ),
            )
            row = conn.execute(
                """
                SELECT * FROM connected_accounts
                WHERE household_id = ? AND provider = ? AND external_account_id = ?
                """,
                (household_id, provider, external_account_id),
            ).fetchone()
        return _connected_account(row)

    def get_connected_account(
        self,
        *,
        household_id: str,
        provider: str,
        external_account_id: str,
    ) -> ConnectedAccount | None:
        provider = _normalize_key(provider)
        external_account_id = _normalize_key(external_account_id)
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM connected_accounts
                WHERE household_id = ? AND provider = ? AND external_account_id = ?
                """,
                (household_id, provider, external_account_id),
            ).fetchone()
        return _connected_account(row) if row else None

    def update_connected_account_sync(
        self,
        *,
        account_id: str,
        cursor: str | None,
        synced_at_utc: datetime,
    ) -> ConnectedAccount:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE connected_accounts
                SET cursor = ?,
                    last_synced_at_utc = ?,
                    updated_at_utc = ?,
                    sync_failure_count = 0,
                    last_sync_error = NULL,
                    retry_after_utc = NULL
                WHERE id = ?
                """,
                (_empty_to_none(cursor), _iso(synced_at_utc), _iso(synced_at_utc), account_id),
            )
            row = conn.execute("SELECT * FROM connected_accounts WHERE id = ?", (account_id,)).fetchone()
        return _connected_account(row)

    def record_connected_account_sync_failure(
        self,
        *,
        account_id: str,
        error: str,
        failed_at_utc: datetime,
    ) -> ConnectedAccount:
        compact_error = " ".join(error.split())[:300] or "source provider failed"
        with self.connect() as conn:
            row = conn.execute(
                "SELECT sync_failure_count FROM connected_accounts WHERE id = ?",
                (account_id,),
            ).fetchone()
            if row is None:
                raise ValueError("unknown connected account")
            failure_count = int(row["sync_failure_count"]) + 1
            retry_after = ensure_utc(failed_at_utc) + _source_sync_backoff(failure_count)
            conn.execute(
                """
                UPDATE connected_accounts
                SET sync_failure_count = ?,
                    last_sync_error = ?,
                    retry_after_utc = ?,
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (
                    failure_count,
                    compact_error,
                    _iso(retry_after),
                    _iso(failed_at_utc),
                    account_id,
                ),
            )
            row = conn.execute("SELECT * FROM connected_accounts WHERE id = ?", (account_id,)).fetchone()
        return _connected_account(row)

    def create_oauth_state(
        self,
        *,
        state: str,
        provider: str,
        chat_id: str,
        expires_at_utc: datetime,
        now_utc: datetime,
        account_label: str | None = None,
        return_path: str | None = None,
    ) -> OAuthState:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO oauth_states
                    (state, provider, chat_id, account_label, return_path, created_at_utc, expires_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    state,
                    _normalize_key(provider),
                    chat_id,
                    _empty_to_none(account_label),
                    _empty_to_none(return_path),
                    _iso(now_utc),
                    _iso(expires_at_utc),
                ),
            )
            row = conn.execute("SELECT * FROM oauth_states WHERE state = ?", (state,)).fetchone()
        return _oauth_state(row)

    def consume_oauth_state(
        self,
        *,
        state: str,
        provider: str,
        now_utc: datetime,
    ) -> OAuthState | None:
        now = ensure_utc(now_utc)
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM oauth_states
                WHERE state = ? AND provider = ?
                """,
                (state, _normalize_key(provider)),
            ).fetchone()
            if row is None or row["used_at_utc"] is not None:
                return None
            record = _oauth_state(row)
            if record.expires_at_utc < now:
                return None
            conn.execute(
                "UPDATE oauth_states SET used_at_utc = ? WHERE state = ?",
                (_iso(now), state),
            )
            row = conn.execute("SELECT * FROM oauth_states WHERE state = ?", (state,)).fetchone()
        return _oauth_state(row)

    def upsert_connected_account_token(
        self,
        *,
        connected_account_id: str,
        provider: str,
        token_ciphertext: str,
        scopes: Iterable[str],
        now_utc: datetime,
        expires_at_utc: datetime | None = None,
    ) -> ConnectedAccountToken:
        scopes_json = json.dumps([scope for scope in scopes if scope], sort_keys=True)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO connected_account_tokens
                (
                    connected_account_id, provider, token_ciphertext, scopes_json,
                    expires_at_utc, created_at_utc, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(connected_account_id)
                DO UPDATE SET
                    provider = excluded.provider,
                    token_ciphertext = excluded.token_ciphertext,
                    scopes_json = excluded.scopes_json,
                    expires_at_utc = excluded.expires_at_utc,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (
                    connected_account_id,
                    _normalize_key(provider),
                    token_ciphertext,
                    scopes_json,
                    _iso(expires_at_utc) if expires_at_utc else None,
                    _iso(now_utc),
                    _iso(now_utc),
                ),
            )
            row = conn.execute(
                "SELECT * FROM connected_account_tokens WHERE connected_account_id = ?",
                (connected_account_id,),
            ).fetchone()
        return _connected_account_token(row)

    def get_connected_account_token(
        self,
        connected_account_id: str,
    ) -> ConnectedAccountToken | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM connected_account_tokens WHERE connected_account_id = ?",
                (connected_account_id,),
            ).fetchone()
        return _connected_account_token(row) if row else None

    def disconnect_connected_accounts(
        self,
        *,
        household_id: str,
        provider: str,
        now_utc: datetime,
    ) -> int:
        provider = _normalize_key(provider)
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, status FROM connected_accounts
                WHERE household_id = ? AND provider = ?
                """,
                (household_id, provider),
            ).fetchall()
            account_ids = [str(row["id"]) for row in rows]
            active_count = sum(1 for row in rows if row["status"] == ConnectedAccountStatus.ACTIVE.value)
            if not account_ids:
                return 0
            placeholders = ", ".join("?" for _ in account_ids)
            conn.execute(
                f"""
                DELETE FROM connected_account_tokens
                WHERE connected_account_id IN ({placeholders})
                """,
                account_ids,
            )
            conn.execute(
                f"""
                UPDATE connected_accounts
                SET status = ?,
                    updated_at_utc = ?,
                    sync_failure_count = 0,
                    last_sync_error = NULL,
                    retry_after_utc = NULL
                WHERE id IN ({placeholders})
                """,
                (
                    ConnectedAccountStatus.DISABLED.value,
                    _iso(now_utc),
                    *account_ids,
                ),
            )
            conn.execute(
                f"""
                UPDATE outbound_deliveries
                SET delivery_status = ?,
                    last_error = ?,
                    updated_at_utc = ?
                WHERE household_id = ?
                  AND delivery_status IN (?, ?)
                  AND (
                    source_message_id LIKE ?
                    OR source_message_id IN (
                        SELECT 'source:' || id
                        FROM source_items
                        WHERE connected_account_id IN ({placeholders})
                    )
                  )
                """,
                (
                    OutboundDeliveryStatus.CANCELED.value,
                    f"canceled after {provider} disconnect",
                    _iso(now_utc),
                    household_id,
                    OutboundDeliveryStatus.PENDING.value,
                    OutboundDeliveryStatus.FAILED.value,
                    f"oauth:{provider}:%",
                    *account_ids,
                ),
            )
        return active_count

    def list_connected_accounts(
        self,
        household_id: str,
        *,
        include_disabled: bool = False,
    ) -> list[ConnectedAccount]:
        status_clause = "" if include_disabled else "AND status = ?"
        params: list[object] = [household_id]
        if not include_disabled:
            params.append(ConnectedAccountStatus.ACTIVE.value)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM connected_accounts
                WHERE household_id = ?
                  {status_clause}
                ORDER BY updated_at_utc DESC
                """,
                params,
            ).fetchall()
        return [_connected_account(row) for row in rows]

    def connected_account_auth_summary(self, *, household_id: str) -> dict[str, int]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(DISTINCT connected_accounts.id) AS active_total,
                    COUNT(
                        DISTINCT CASE
                            WHEN connected_accounts.provider = 'google'
                            THEN connected_accounts.id
                        END
                    ) AS active_google,
                    COUNT(
                        DISTINCT CASE
                            WHEN connected_accounts.provider = 'google'
                             AND connected_account_tokens.connected_account_id IS NOT NULL
                            THEN connected_accounts.id
                        END
                    ) AS token_backed_google
                FROM connected_accounts
                LEFT JOIN connected_account_tokens
                  ON connected_account_tokens.connected_account_id = connected_accounts.id
                WHERE connected_accounts.household_id = ?
                  AND connected_accounts.status = ?
                """,
                (household_id, ConnectedAccountStatus.ACTIVE.value),
            ).fetchone()
        return {
            "active_total": int(row["active_total"] or 0) if row else 0,
            "active_google": int(row["active_google"] or 0) if row else 0,
            "token_backed_google": int(row["token_backed_google"] or 0) if row else 0,
        }

    def list_active_connected_accounts(
        self,
        *,
        now_utc: datetime | None = None,
        limit: int = 100,
    ) -> list[ConnectedAccount]:
        retry_clause = ""
        params: list[object] = [ConnectedAccountStatus.ACTIVE.value]
        if now_utc is not None:
            retry_clause = """
                  AND (
                    connected_accounts.retry_after_utc IS NULL
                    OR connected_accounts.retry_after_utc <= ?
                  )
            """
            params.append(_iso(now_utc))
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT connected_accounts.*
                FROM connected_accounts
                JOIN households ON households.id = connected_accounts.household_id
                WHERE connected_accounts.status = ?
                  AND households.stopped = 0
                  {retry_clause}
                ORDER BY
                  connected_accounts.last_synced_at_utc IS NOT NULL ASC,
                  connected_accounts.last_synced_at_utc ASC,
                  connected_accounts.created_at_utc ASC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [_connected_account(row) for row in rows]

    def source_review_snapshot(self, *, household_id: str, limit: int = 5) -> SourceReviewSnapshot:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    decision,
                    reason,
                    COUNT(*) AS count,
                    SUM(CASE WHEN surfaced_at_utc IS NOT NULL THEN 1 ELSE 0 END) AS delivered_count
                FROM source_items
                WHERE household_id = ?
                GROUP BY decision, reason
                """,
                (household_id,),
            ).fetchall()
            connected_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(
                        CASE
                            WHEN decision = ? AND surfaced_at_utc IS NOT NULL
                            THEN 1 ELSE 0
                        END
                    ) AS surfaced
                FROM source_items
                WHERE household_id = ?
                  AND connected_account_id IS NOT NULL
                """,
                (SourceDecision.SURFACE.value, household_id),
            ).fetchone()
            token_backed_google_row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(
                        CASE
                            WHEN source_items.decision = ?
                             AND source_items.surfaced_at_utc IS NOT NULL
                            THEN 1 ELSE 0
                        END
                    ) AS surfaced,
                    MAX(connected_accounts.last_synced_at_utc) AS latest_synced_at_utc
                FROM source_items
                JOIN connected_accounts
                  ON connected_accounts.id = source_items.connected_account_id
                 AND connected_accounts.status = ?
                 AND connected_accounts.provider = 'google'
                JOIN connected_account_tokens
                  ON connected_account_tokens.connected_account_id = connected_accounts.id
                WHERE source_items.household_id = ?
                """,
                (
                    SourceDecision.SURFACE.value,
                    ConnectedAccountStatus.ACTIVE.value,
                    household_id,
                ),
            ).fetchone()
            recent_surfaced_rows = conn.execute(
                """
                SELECT id, title, source_type, reason, priority, event_at_utc
                FROM source_items
                WHERE household_id = ?
                  AND decision = ?
                  AND surfaced_at_utc IS NOT NULL
                ORDER BY surfaced_at_utc DESC, observed_at_utc DESC
                LIMIT ?
                """,
                (household_id, SourceDecision.SURFACE.value, limit),
            ).fetchall()
            recent_stored_rows = conn.execute(
                """
                SELECT id, title, source_type, reason, priority, event_at_utc
                FROM source_items
                WHERE household_id = ? AND decision = ?
                ORDER BY observed_at_utc DESC
                LIMIT ?
                """,
                (household_id, SourceDecision.STORE_ONLY.value, limit),
            ).fetchall()

        total = 0
        surfaced = 0
        stored_only = 0
        suppressed = 0
        by_reason: dict[str, int] = {}
        for row in rows:
            count = int(row["count"])
            total += count
            decision = row["decision"]
            reason = row["reason"] or "unknown"
            by_reason[reason] = by_reason.get(reason, 0) + count
            if decision == SourceDecision.SURFACE.value:
                surfaced += int(row["delivered_count"] or 0)
            elif decision == SourceDecision.STORE_ONLY.value:
                stored_only += count
            elif decision == SourceDecision.SUPPRESS.value:
                suppressed += count

        return SourceReviewSnapshot(
            household_id=household_id,
            total=total,
            surfaced=surfaced,
            connected_total=int(connected_row["total"] or 0) if connected_row else 0,
            connected_surfaced=int(connected_row["surfaced"] or 0) if connected_row else 0,
            token_backed_google_total=(
                int(token_backed_google_row["total"] or 0) if token_backed_google_row else 0
            ),
            token_backed_google_surfaced=(
                int(token_backed_google_row["surfaced"] or 0) if token_backed_google_row else 0
            ),
            latest_token_backed_google_synced_at_utc=(
                _dt(token_backed_google_row["latest_synced_at_utc"])
                if token_backed_google_row and token_backed_google_row["latest_synced_at_utc"]
                else None
            ),
            stored_only=stored_only,
            suppressed=suppressed,
            by_reason=by_reason,
            recent_surfaced=[
                BriefingSourceItem(
                    id=row["id"],
                    title=row["title"],
                    source_type=row["source_type"],
                    reason=row["reason"],
                    priority=int(row["priority"]),
                    event_at_utc=_dt(row["event_at_utc"]) if row["event_at_utc"] else None,
                )
                for row in recent_surfaced_rows
            ],
            recent_stored=[
                BriefingSourceItem(
                    id=row["id"],
                    title=row["title"],
                    source_type=row["source_type"],
                    reason=row["reason"],
                    priority=int(row["priority"]),
                    event_at_utc=_dt(row["event_at_utc"]) if row["event_at_utc"] else None,
                )
                for row in recent_stored_rows
            ],
        )

    def upsert_source_preference(
        self,
        *,
        household_id: str,
        phrase: str,
        preference: SourcePreferenceKind,
        now_utc: datetime,
        created_by_member_id: str | None = None,
    ) -> SourcePreference:
        normalized = _normalize_phrase(phrase)
        preference_id = str(uuid.uuid4())
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO source_preferences
                (
                    id, household_id, phrase, preference, created_by_member_id,
                    created_at_utc, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(household_id, phrase)
                DO UPDATE SET
                    preference = excluded.preference,
                    created_by_member_id = COALESCE(
                        excluded.created_by_member_id,
                        source_preferences.created_by_member_id
                    ),
                    updated_at_utc = excluded.updated_at_utc
                """,
                (
                    preference_id,
                    household_id,
                    normalized,
                    preference.value,
                    created_by_member_id,
                    _iso(now_utc),
                    _iso(now_utc),
                ),
            )
            row = conn.execute(
                """
                SELECT * FROM source_preferences
                WHERE household_id = ? AND phrase = ?
                """,
                (household_id, normalized),
            ).fetchone()
        return _source_preference(row)

    def list_source_preferences(self, household_id: str, *, limit: int = 50) -> list[SourcePreference]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM source_preferences
                WHERE household_id = ?
                ORDER BY updated_at_utc DESC
                LIMIT ?
                """,
                (household_id, limit),
            ).fetchall()
        return [_source_preference(row) for row in rows]

    def upcoming_reminders(
        self,
        *,
        household_id: str,
        now_utc: datetime,
        horizon_utc: datetime | None = None,
        created_since_utc: datetime | None = None,
        limit: int = 10,
    ) -> list[Reminder]:
        horizon_clause = ""
        created_since_clause = ""
        params: list[object] = [
            household_id,
            ReminderStatus.PENDING.value,
            _iso(now_utc),
        ]
        if horizon_utc is not None:
            horizon_clause = "AND due_at_utc <= ?"
            params.append(_iso(horizon_utc))
        if created_since_utc is not None:
            created_since_clause = "AND created_at_utc >= ?"
            params.append(_iso(created_since_utc))
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM reminders
                WHERE household_id = ? AND status = ? AND due_at_utc > ?
                  {horizon_clause}
                  {created_since_clause}
                ORDER BY due_at_utc ASC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [_reminder(row) for row in rows]

    def briefing_source_items(
        self,
        *,
        household_id: str,
        now_utc: datetime,
        horizon_utc: datetime,
        limit: int = 3,
    ) -> list[BriefingSourceItem]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, title, source_type, reason, priority, event_at_utc
                FROM source_items
                WHERE household_id = ?
                  AND briefed_at_utc IS NULL
                  AND (
                    (
                      decision = 'surface'
                      AND (
                        event_at_utc IS NULL
                        OR (event_at_utc >= ? AND event_at_utc <= ?)
                      )
                    )
                    OR (
                      decision = 'store_only'
                      AND reason = 'initial_sync_backfill'
                      AND (event_at_utc IS NULL OR event_at_utc >= ?)
                    )
                  )
                ORDER BY priority DESC, COALESCE(event_at_utc, observed_at_utc) ASC
                LIMIT ?
                """,
                (
                    household_id,
                    _iso(now_utc),
                    _iso(horizon_utc),
                    _iso(now_utc),
                    limit,
                ),
            ).fetchall()
        return [
            BriefingSourceItem(
                id=row["id"],
                title=row["title"],
                source_type=row["source_type"],
                reason=row["reason"],
                priority=int(row["priority"]),
                event_at_utc=_dt(row["event_at_utc"]) if row["event_at_utc"] else None,
            )
            for row in rows
        ]

    def agenda_source_items(
        self,
        *,
        household_id: str,
        now_utc: datetime,
        horizon_utc: datetime,
        limit: int = 3,
    ) -> list[BriefingSourceItem]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, title, source_type, reason, priority, event_at_utc
                FROM source_items
                WHERE household_id = ?
                  AND (
                    (
                      decision = 'surface'
                      AND (
                        event_at_utc IS NULL
                        OR (event_at_utc >= ? AND event_at_utc <= ?)
                      )
                    )
                    OR (
                      decision = 'store_only'
                      AND reason = 'initial_sync_backfill'
                      AND (event_at_utc IS NULL OR event_at_utc >= ?)
                    )
                  )
                ORDER BY priority DESC, COALESCE(event_at_utc, observed_at_utc) ASC
                LIMIT ?
                """,
                (
                    household_id,
                    _iso(now_utc),
                    _iso(horizon_utc),
                    _iso(now_utc),
                    limit,
                ),
            ).fetchall()
        return [
            BriefingSourceItem(
                id=row["id"],
                title=row["title"],
                source_type=row["source_type"],
                reason=row["reason"],
                priority=int(row["priority"]),
                event_at_utc=_dt(row["event_at_utc"]) if row["event_at_utc"] else None,
            )
            for row in rows
        ]

    def mark_source_items_briefed(
        self,
        *,
        household_id: str,
        source_item_ids: Iterable[str],
        briefed_at_utc: datetime,
    ) -> int:
        ids = [item_id for item_id in source_item_ids if item_id]
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        with self.connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE source_items
                SET briefed_at_utc = ?
                WHERE household_id = ?
                  AND id IN ({placeholders})
                  AND briefed_at_utc IS NULL
                """,
                (_iso(briefed_at_utc), household_id, *ids),
            )
        return cur.rowcount

    def mark_source_items_surfaced(
        self,
        *,
        household_id: str,
        source_item_ids: Iterable[str],
        surfaced_at_utc: datetime,
    ) -> int:
        ids = [item_id for item_id in source_item_ids if item_id]
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        with self.connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE source_items
                SET surfaced_at_utc = COALESCE(surfaced_at_utc, ?)
                WHERE household_id = ?
                  AND id IN ({placeholders})
                  AND decision = ?
                """,
                (_iso(surfaced_at_utc), household_id, *ids, SourceDecision.SURFACE.value),
            )
        return cur.rowcount

    def upsert_memory(
        self,
        *,
        household_id: str,
        kind: MemoryKind,
        text: str,
        now_utc: datetime,
        subject: str | None = None,
        asserted_by_member_id: str | None = None,
        source_message_id: str | None = None,
        confidence: float = 0.8,
        expires_at_utc: datetime | None = None,
    ) -> MemoryRecord:
        with self.connect() as conn:
            if asserted_by_member_id is not None:
                member = conn.execute(
                    """
                    SELECT 1 FROM household_members
                    WHERE id = ? AND household_id = ?
                    """,
                    (asserted_by_member_id, household_id),
                ).fetchone()
                if member is None:
                    raise ValueError("memory asserted_by_member_id must belong to the household")
            row = conn.execute(
                """
                SELECT * FROM memories
                WHERE household_id = ?
                  AND kind = ?
                  AND COALESCE(subject, '__florence_null_subject__')
                      = COALESCE(?, '__florence_null_subject__')
                  AND text = ?
                ORDER BY deleted_at_utc IS NULL DESC, updated_at_utc DESC
                LIMIT 1
                """,
                (household_id, kind.value, subject, text),
            ).fetchone()
            if row is None:
                memory_id = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT OR IGNORE INTO memories
                    (
                        id, household_id, kind, subject, text, confidence,
                        asserted_by_member_id, source_message_id, created_at_utc,
                        updated_at_utc, expires_at_utc
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        memory_id,
                        household_id,
                        kind.value,
                        subject,
                        text,
                        confidence,
                        asserted_by_member_id,
                        source_message_id,
                        _iso(now_utc),
                        _iso(now_utc),
                        _iso(expires_at_utc) if expires_at_utc else None,
                    ),
                )
                row = conn.execute(
                    """
                    SELECT * FROM memories
                    WHERE household_id = ?
                      AND kind = ?
                      AND COALESCE(subject, '__florence_null_subject__')
                          = COALESCE(?, '__florence_null_subject__')
                      AND text = ?
                    ORDER BY deleted_at_utc IS NULL DESC, updated_at_utc DESC
                    LIMIT 1
                    """,
                    (household_id, kind.value, subject, text),
                ).fetchone()
                if row is None:
                    raise RuntimeError("memory insert did not create or find a durable record")
            memory_id = row["id"]
            conn.execute(
                """
                UPDATE memories
                SET confidence = ?,
                    asserted_by_member_id = COALESCE(?, asserted_by_member_id),
                    source_message_id = COALESCE(?, source_message_id),
                    updated_at_utc = ?,
                    expires_at_utc = ?,
                    deleted_at_utc = NULL
                WHERE id = ?
                """,
                (
                    confidence,
                    asserted_by_member_id,
                    source_message_id,
                    _iso(now_utc),
                    _iso(expires_at_utc) if expires_at_utc else None,
                    memory_id,
                ),
            )
            row = conn.execute("SELECT * FROM memories WHERE id = ?", (memory_id,)).fetchone()
        return _memory(row)

    def list_memories(
        self,
        *,
        household_id: str,
        now_utc: datetime,
        limit: int = 30,
    ) -> list[MemoryRecord]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM memories
                WHERE household_id = ?
                  AND deleted_at_utc IS NULL
                  AND (expires_at_utc IS NULL OR expires_at_utc > ?)
                ORDER BY confidence DESC, updated_at_utc DESC
                LIMIT ?
                """,
                (household_id, _iso(now_utc), limit),
            ).fetchall()
        return [_memory(row) for row in rows]

    def forget_memories(self, *, household_id: str, query: str, now_utc: datetime) -> int:
        pattern = f"%{query.strip()}%"
        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE memories
                SET deleted_at_utc = ?, updated_at_utc = ?
                WHERE household_id = ?
                  AND deleted_at_utc IS NULL
                  AND text LIKE ?
                """,
                (_iso(now_utc), _iso(now_utc), household_id, pattern),
            )
        return cur.rowcount

    def clear_memories(self, *, household_id: str, now_utc: datetime) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE memories
                SET deleted_at_utc = ?, updated_at_utc = ?
                WHERE household_id = ?
                  AND deleted_at_utc IS NULL
                """,
                (_iso(now_utc), _iso(now_utc), household_id),
            )
        return cur.rowcount

    def delete_memory(self, *, household_id: str, memory_id: str, now_utc: datetime) -> bool:
        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE memories
                SET deleted_at_utc = ?, updated_at_utc = ?
                WHERE household_id = ?
                  AND id = ?
                  AND deleted_at_utc IS NULL
                """,
                (_iso(now_utc), _iso(now_utc), household_id, memory_id),
            )
        return cur.rowcount > 0

    def delete_memories_by_source(
        self,
        *,
        household_id: str,
        source_message_id: str,
        now_utc: datetime,
    ) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE memories
                SET deleted_at_utc = ?, updated_at_utc = ?
                WHERE household_id = ?
                  AND source_message_id = ?
                  AND deleted_at_utc IS NULL
                """,
                (_iso(now_utc), _iso(now_utc), household_id, source_message_id),
            )
        return cur.rowcount

    def memory_snapshot(
        self,
        *,
        household_id: str,
        now_utc: datetime,
        limit: int = 100,
    ) -> MemorySnapshot:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    memories.*,
                    COALESCE(household_members.display_name, household_members.phone)
                        AS asserted_by_label
                FROM memories
                LEFT JOIN household_members
                    ON household_members.id = memories.asserted_by_member_id
                   AND household_members.household_id = memories.household_id
                WHERE memories.household_id = ?
                  AND memories.deleted_at_utc IS NULL
                  AND (memories.expires_at_utc IS NULL OR memories.expires_at_utc > ?)
                ORDER BY memories.kind ASC, memories.updated_at_utc DESC
                LIMIT ?
                """,
                (household_id, _iso(now_utc), limit),
            ).fetchall()
        return MemorySnapshot(
            household_id=household_id,
            memories=[_memory_export(row) for row in rows],
        )

    def create_pending_action(
        self,
        *,
        household_id: str,
        chat_id: str,
        action_type: str,
        summary: str,
        payload: dict[str, object],
        created_at_utc: datetime,
        expires_at_utc: datetime,
        created_by_member_id: str | None = None,
    ) -> PendingAction:
        action_id = str(uuid.uuid4())
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO pending_actions
                (
                    id, household_id, chat_id, action_type, summary, payload_json,
                    created_by_member_id, created_at_utc, expires_at_utc, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    action_id,
                    household_id,
                    chat_id,
                    action_type,
                    summary,
                    json.dumps(payload, sort_keys=True),
                    created_by_member_id,
                    _iso(created_at_utc),
                    _iso(expires_at_utc),
                    PendingActionStatus.PENDING.value,
                ),
            )
            row = conn.execute("SELECT * FROM pending_actions WHERE id = ?", (action_id,)).fetchone()
        return _pending_action(row)

    def expire_pending_actions(self, *, now_utc: datetime, household_id: str | None = None) -> int:
        params: list[object] = [
            PendingActionStatus.EXPIRED.value,
            PendingActionStatus.PENDING.value,
            PendingActionStatus.APPROVED.value,
            _iso(now_utc),
        ]
        household_clause = ""
        if household_id is not None:
            household_clause = "AND household_id = ?"
            params.append(household_id)
        with self.connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE pending_actions
                SET status = ?
                WHERE status IN (?, ?)
                  AND expires_at_utc <= ?
                  {household_clause}
                """,
                params,
            )
        return cur.rowcount

    def list_pending_actions(
        self,
        *,
        household_id: str,
        now_utc: datetime,
        include_resolved: bool = False,
        limit: int = 50,
    ) -> list[PendingAction]:
        self.expire_pending_actions(now_utc=now_utc, household_id=household_id)
        status_clause = "" if include_resolved else "AND status = ?"
        params: list[object] = [household_id]
        if not include_resolved:
            params.append(PendingActionStatus.PENDING.value)
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM pending_actions
                WHERE household_id = ?
                  {status_clause}
                ORDER BY created_at_utc DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [_pending_action(row) for row in rows]

    def executable_actions(self, *, now_utc: datetime, limit: int = 50) -> list[PendingAction]:
        self.expire_pending_actions(now_utc=now_utc)
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT pending_actions.*
                FROM pending_actions
                JOIN households ON households.id = pending_actions.household_id
                LEFT JOIN action_executions
                    ON action_executions.action_id = pending_actions.id
                WHERE pending_actions.status = ?
                  AND pending_actions.expires_at_utc > ?
                  AND action_executions.id IS NULL
                  AND households.stopped = 0
                ORDER BY pending_actions.resolved_at_utc ASC, pending_actions.created_at_utc ASC
                LIMIT ?
                """,
                (PendingActionStatus.APPROVED.value, _iso(now_utc), limit),
            ).fetchall()
        return [_pending_action(row) for row in rows]

    def find_pending_action_by_code(
        self,
        *,
        household_id: str,
        code: str,
        now_utc: datetime,
    ) -> PendingAction | None:
        normalized = code.strip().lower()
        if len(normalized) < 4:
            return None
        self.expire_pending_actions(now_utc=now_utc, household_id=household_id)
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM pending_actions
                WHERE household_id = ?
                  AND status = ?
                  AND expires_at_utc > ?
                  AND lower(id) LIKE ?
                ORDER BY created_at_utc DESC
                LIMIT 2
                """,
                (
                    household_id,
                    PendingActionStatus.PENDING.value,
                    _iso(now_utc),
                    f"{normalized}%",
                ),
            ).fetchall()
        if len(rows) != 1:
            return None
        return _pending_action(rows[0])

    def resolve_pending_action(
        self,
        *,
        household_id: str,
        action_id: str,
        status: PendingActionStatus,
        resolved_by_member_id: str,
        now_utc: datetime,
    ) -> PendingAction | None:
        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE pending_actions
                SET status = ?,
                    resolved_by_member_id = ?,
                    resolved_at_utc = ?
                WHERE household_id = ?
                  AND id = ?
                  AND status = ?
                  AND expires_at_utc > ?
                """,
                (
                    status.value,
                    resolved_by_member_id,
                    _iso(now_utc),
                    household_id,
                    action_id,
                    PendingActionStatus.PENDING.value,
                    _iso(now_utc),
                ),
            )
            if cur.rowcount == 0:
                return None
            row = conn.execute("SELECT * FROM pending_actions WHERE id = ?", (action_id,)).fetchone()
        return _pending_action(row)

    def record_action_execution(
        self,
        *,
        action: PendingAction,
        status: ActionExecutionStatus,
        attempted_at_utc: datetime,
        result: dict[str, object] | None = None,
        error: str | None = None,
    ) -> ActionExecution:
        execution_id = str(uuid.uuid4())
        action_status = (
            PendingActionStatus.EXECUTED
            if status == ActionExecutionStatus.SUCCESS
            else PendingActionStatus.FAILED
        )
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO action_executions
                (id, action_id, household_id, status, attempted_at_utc, result_json, error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    execution_id,
                    action.id,
                    action.household_id,
                    status.value,
                    _iso(attempted_at_utc),
                    json.dumps(result or {}, sort_keys=True),
                    error,
                ),
            )
            if cur.rowcount > 0:
                conn.execute(
                    """
                    UPDATE pending_actions
                    SET status = ?
                    WHERE id = ?
                      AND household_id = ?
                    """,
                    (action_status.value, action.id, action.household_id),
                )
            row = conn.execute(
                "SELECT * FROM action_executions WHERE action_id = ?",
                (action.id,),
            ).fetchone()
        return _action_execution(row)

    def list_action_executions(
        self,
        *,
        household_id: str | None = None,
        limit: int = 50,
    ) -> list[ActionExecution]:
        params: list[object] = []
        household_clause = ""
        if household_id is not None:
            household_clause = "WHERE household_id = ?"
            params.append(household_id)
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM action_executions
                {household_clause}
                ORDER BY attempted_at_utc DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [_action_execution(row) for row in rows]

    def action_execution_summary(
        self,
        *,
        household_id: str,
        now_utc: datetime,
        issue_limit: int = 10,
    ) -> dict[str, object]:
        self.expire_pending_actions(now_utc=now_utc, household_id=household_id)
        with self.connect() as conn:
            execution_rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM action_executions
                WHERE household_id = ?
                GROUP BY status
                """,
                (household_id,),
            ).fetchall()
            approved_row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM pending_actions
                WHERE household_id = ?
                  AND status = ?
                  AND expires_at_utc > ?
                """,
                (
                    household_id,
                    PendingActionStatus.APPROVED.value,
                    _iso(now_utc),
                ),
            ).fetchone()
            failed_issue_rows = conn.execute(
                """
                SELECT action_id, status, attempted_at_utc, error
                FROM action_executions
                WHERE household_id = ?
                  AND status = ?
                ORDER BY attempted_at_utc DESC
                LIMIT ?
                """,
                (
                    household_id,
                    ActionExecutionStatus.FAILED.value,
                    issue_limit,
                ),
            ).fetchall()
            remaining_issue_limit = max(0, issue_limit - len(failed_issue_rows))
            approved_issue_rows = conn.execute(
                """
                SELECT id, action_type, created_at_utc, resolved_at_utc
                FROM pending_actions
                WHERE household_id = ?
                  AND status = ?
                  AND expires_at_utc > ?
                ORDER BY resolved_at_utc ASC, created_at_utc ASC
                LIMIT ?
                """,
                (
                    household_id,
                    PendingActionStatus.APPROVED.value,
                    _iso(now_utc),
                    remaining_issue_limit,
                ),
            ).fetchall()

        execution_counts = {status.value: 0 for status in ActionExecutionStatus}
        for row in execution_rows:
            execution_counts[str(row["status"])] = int(row["count"] or 0)
        approved = int(approved_row["count"] or 0) if approved_row else 0
        failed = execution_counts[ActionExecutionStatus.FAILED.value]
        issues: list[dict[str, object]] = [
            {
                "action_id": row["action_id"],
                "status": row["status"],
                "attempted_at_utc": row["attempted_at_utc"],
                "error": row["error"],
            }
            for row in failed_issue_rows
        ]
        issues.extend(
            {
                "action_id": row["id"],
                "action_type": row["action_type"],
                "status": PendingActionStatus.APPROVED.value,
                "created_at_utc": row["created_at_utc"],
                "resolved_at_utc": row["resolved_at_utc"],
            }
            for row in approved_issue_rows
        )
        return {
            "ready": approved == 0 and failed == 0,
            "approved": approved,
            "failed": failed,
            "succeeded": execution_counts[ActionExecutionStatus.SUCCESS.value],
            "issues": issues,
        }


def _household(row: sqlite3.Row) -> Household:
    return Household(
        id=row["id"],
        chat_id=row["chat_id"],
        timezone=row["timezone"],
        created_at=_dt(row["created_at_utc"]),
    )


def _connected_account(row: sqlite3.Row) -> ConnectedAccount:
    return ConnectedAccount(
        id=row["id"],
        household_id=row["household_id"],
        provider=row["provider"],
        external_account_id=row["external_account_id"],
        account_label=row["account_label"],
        status=ConnectedAccountStatus(row["status"]),
        cursor=row["cursor"],
        created_at_utc=_dt(row["created_at_utc"]),
        updated_at_utc=_dt(row["updated_at_utc"]),
        last_synced_at_utc=_dt(row["last_synced_at_utc"]) if row["last_synced_at_utc"] else None,
        sync_failure_count=int(row["sync_failure_count"]),
        last_sync_error=row["last_sync_error"],
        retry_after_utc=_dt(row["retry_after_utc"]) if row["retry_after_utc"] else None,
    )


def _oauth_state(row: sqlite3.Row) -> OAuthState:
    return OAuthState(
        state=row["state"],
        provider=row["provider"],
        chat_id=row["chat_id"],
        account_label=row["account_label"],
        created_at_utc=_dt(row["created_at_utc"]),
        expires_at_utc=_dt(row["expires_at_utc"]),
        used_at_utc=_dt(row["used_at_utc"]) if row["used_at_utc"] else None,
        return_path=row["return_path"],
    )


def _connected_account_token(row: sqlite3.Row) -> ConnectedAccountToken:
    try:
        scopes = tuple(str(item) for item in json.loads(row["scopes_json"]) if str(item))
    except (TypeError, ValueError):
        scopes = ()
    return ConnectedAccountToken(
        connected_account_id=row["connected_account_id"],
        provider=row["provider"],
        token_ciphertext=row["token_ciphertext"],
        scopes=scopes,
        expires_at_utc=_dt(row["expires_at_utc"]) if row["expires_at_utc"] else None,
        created_at_utc=_dt(row["created_at_utc"]),
        updated_at_utc=_dt(row["updated_at_utc"]),
    )


def _outbound_payload(message: OutboundMessage) -> dict[str, object]:
    return {
        "chat_id": message.chat_id,
        "text": message.text,
        "idempotency_key": message.idempotency_key,
        "new_chat_from": message.new_chat_from,
        "new_chat_to": list(message.new_chat_to),
        "migrate_household_id": message.migrate_household_id,
        "invited_partner_phone": message.invited_partner_phone,
        "routine_household_id": message.routine_household_id,
        "routine_name": message.routine_name,
        "routine_local_date": message.routine_local_date,
        "briefed_source_item_ids": list(message.briefed_source_item_ids),
        "delivery_household_id": message.delivery_household_id,
        "delivery_source_message_id": message.delivery_source_message_id,
    }


def _retarget_outbound_payload_chat(payload_json: str, old_chat_id: str, new_chat_id: str) -> str:
    try:
        payload = json.loads(payload_json)
    except (TypeError, ValueError):
        return payload_json
    if not isinstance(payload, dict):
        return payload_json
    if payload.get("chat_id") == old_chat_id:
        payload["chat_id"] = new_chat_id
    return json.dumps(payload, sort_keys=True)


def _outbound_delivery(row: sqlite3.Row) -> OutboundMessage:
    try:
        payload = json.loads(row["payload_json"])
    except (TypeError, ValueError):
        payload = {}
    new_chat_to = payload.get("new_chat_to")
    return OutboundMessage(
        chat_id=str(payload.get("chat_id") or row["chat_id"]),
        text=str(payload.get("text") or row["text"]),
        idempotency_key=str(payload.get("idempotency_key") or row["idempotency_key"]),
        new_chat_from=payload.get("new_chat_from") if isinstance(payload.get("new_chat_from"), str) else None,
        new_chat_to=tuple(str(item) for item in new_chat_to) if isinstance(new_chat_to, list) else (),
        migrate_household_id=(
            payload.get("migrate_household_id")
            if isinstance(payload.get("migrate_household_id"), str)
            else None
        ),
        invited_partner_phone=(
            payload.get("invited_partner_phone")
            if isinstance(payload.get("invited_partner_phone"), str)
            else None
        ),
        routine_household_id=(
            payload.get("routine_household_id")
            if isinstance(payload.get("routine_household_id"), str)
            else None
        ),
        routine_name=payload.get("routine_name") if isinstance(payload.get("routine_name"), str) else None,
        routine_local_date=(
            payload.get("routine_local_date")
            if isinstance(payload.get("routine_local_date"), str)
            else None
        ),
        briefed_source_item_ids=(
            tuple(str(item) for item in payload.get("briefed_source_item_ids"))
            if isinstance(payload.get("briefed_source_item_ids"), list)
            else ()
        ),
        delivery_household_id=(
            payload.get("delivery_household_id")
            if isinstance(payload.get("delivery_household_id"), str)
            else None
        ),
        delivery_source_message_id=(
            payload.get("delivery_source_message_id")
            if isinstance(payload.get("delivery_source_message_id"), str)
            else None
        ),
    )


def _live_verification_record(row: sqlite3.Row) -> dict[str, str]:
    return {
        "name": row["name"],
        "verified_at_utc": row["verified_at_utc"],
        "proof": row["proof"],
        "source": row["source"],
        "updated_at_utc": row["updated_at_utc"],
    }


def _member(row: sqlite3.Row) -> HouseholdMember:
    return HouseholdMember(
        id=row["id"],
        household_id=row["household_id"],
        phone=row["phone"],
        role=MemberRole(row["role"]),
        display_name=row["display_name"],
        created_at_utc=_dt(row["created_at_utc"]),
        last_seen_at_utc=_dt(row["last_seen_at_utc"]),
    )


def _reminder(row: sqlite3.Row) -> Reminder:
    return Reminder(
        id=row["id"],
        household_id=row["household_id"],
        chat_id=row["chat_id"],
        title=row["title"],
        due_at_utc=_dt(row["due_at_utc"]),
        created_at_utc=_dt(row["created_at_utc"]),
        status=ReminderStatus(row["status"]),
        assignee_member_id=row["assignee_member_id"],
    )


def _source_item(row: sqlite3.Row) -> SourceItem:
    return SourceItem(
        id=row["id"],
        household_id=row["household_id"],
        connected_account_id=row["connected_account_id"],
        source_type=row["source_type"],
        external_id=row["external_id"],
        sender=row["sender"],
        title=row["title"],
        body=row["body"],
        observed_at_utc=_dt(row["observed_at_utc"]),
        event_at_utc=_dt(row["event_at_utc"]) if row["event_at_utc"] else None,
    )


def _memory(row: sqlite3.Row) -> MemoryRecord:
    return MemoryRecord(
        id=row["id"],
        household_id=row["household_id"],
        kind=MemoryKind(row["kind"]),
        subject=row["subject"],
        text=row["text"],
        confidence=float(row["confidence"]),
        asserted_by_member_id=row["asserted_by_member_id"],
        source_message_id=row["source_message_id"],
        created_at_utc=_dt(row["created_at_utc"]),
        updated_at_utc=_dt(row["updated_at_utc"]),
        expires_at_utc=_dt(row["expires_at_utc"]) if row["expires_at_utc"] else None,
    )


def _source_preference(row: sqlite3.Row) -> SourcePreference:
    return SourcePreference(
        id=row["id"],
        household_id=row["household_id"],
        phrase=row["phrase"],
        preference=SourcePreferenceKind(row["preference"]),
        created_by_member_id=row["created_by_member_id"],
        created_at_utc=_dt(row["created_at_utc"]),
        updated_at_utc=_dt(row["updated_at_utc"]),
    )


def _memory_export(row: sqlite3.Row) -> MemoryExportItem:
    return MemoryExportItem(
        id=row["id"],
        kind=MemoryKind(row["kind"]),
        subject=row["subject"],
        text=row["text"],
        confidence=float(row["confidence"]),
        asserted_by_member_id=row["asserted_by_member_id"],
        asserted_by_label=row["asserted_by_label"],
        source_message_id=row["source_message_id"],
        created_at_utc=_dt(row["created_at_utc"]),
        updated_at_utc=_dt(row["updated_at_utc"]),
        expires_at_utc=_dt(row["expires_at_utc"]) if row["expires_at_utc"] else None,
    )


def _normalize_phrase(value: str) -> str:
    return " ".join(value.strip(" .").lower().split())


def _normalize_command(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _normalize_key(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _household_deletion_tombstone_key(*, chat_id: str, message_id: str) -> str:
    payload = f"{chat_id.strip()}\0{message_id.strip()}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _empty_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    compact = value.strip()
    return compact or None


def _source_sync_backoff(failure_count: int) -> timedelta:
    minutes = min(12 * 60, 5 * (2 ** max(0, failure_count - 1)))
    return timedelta(minutes=minutes)


def _pending_action(row: sqlite3.Row) -> PendingAction:
    payload = json.loads(row["payload_json"])
    if not isinstance(payload, dict):
        payload = {}
    return PendingAction(
        id=row["id"],
        household_id=row["household_id"],
        chat_id=row["chat_id"],
        action_type=row["action_type"],
        summary=row["summary"],
        payload=payload,
        created_by_member_id=row["created_by_member_id"],
        created_at_utc=_dt(row["created_at_utc"]),
        expires_at_utc=_dt(row["expires_at_utc"]),
        status=PendingActionStatus(row["status"]),
        resolved_by_member_id=row["resolved_by_member_id"],
        resolved_at_utc=_dt(row["resolved_at_utc"]) if row["resolved_at_utc"] else None,
    )


def _action_execution(row: sqlite3.Row) -> ActionExecution:
    result = json.loads(row["result_json"])
    if not isinstance(result, dict):
        result = {}
    return ActionExecution(
        id=row["id"],
        action_id=row["action_id"],
        household_id=row["household_id"],
        status=ActionExecutionStatus(row["status"]),
        attempted_at_utc=_dt(row["attempted_at_utc"]),
        result=result,
        error=row["error"],
    )
