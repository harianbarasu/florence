"""Database-backed Florence product state store."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from florence.contracts import (
    AppChatMessage,
    AppChatMessageRole,
    CandidateState,
    Channel,
    ChannelType,
    GoogleConnection,
    GoogleSourceKind,
    HouseholdEvent,
    HouseholdEventStatus,
    Household,
    HouseholdStatus,
    IdentityKind,
    ImportedCandidate,
    Member,
    MemberIdentity,
    MemberRole,
)
from florence.onboarding import OnboardingStage, OnboardingState
from florence.state.db import RowLike, connect_florence_db

FLORENCE_DB_PATH = Path(os.getenv("HERMES_HOME", Path.home() / ".hermes")) / "florence.db"
FLORENCE_SCHEMA_VERSION = 2

FLORENCE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS florence_schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS onboarding_sessions (
    session_key TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    parent_display_name TEXT,
    google_connected INTEGER NOT NULL DEFAULT 0,
    child_names_json TEXT NOT NULL,
    school_labels_json TEXT NOT NULL,
    activity_labels_json TEXT NOT NULL,
    school_basics_collected INTEGER NOT NULL DEFAULT 0,
    activity_basics_collected INTEGER NOT NULL DEFAULT 0,
    group_channel_id TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_onboarding_household_member_thread
ON onboarding_sessions(household_id, member_id, thread_id);

CREATE TABLE IF NOT EXISTS households (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    timezone TEXT NOT NULL,
    status TEXT NOT NULL,
    settings_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS members (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_members_household
ON members(household_id, role, status);

CREATE TABLE IF NOT EXISTS member_identities (
    id TEXT PRIMARY KEY,
    member_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    value TEXT NOT NULL,
    normalized_value TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_member_identities_unique
ON member_identities(kind, normalized_value);

CREATE INDEX IF NOT EXISTS idx_member_identities_member
ON member_identities(member_id);

CREATE TABLE IF NOT EXISTS channels (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    provider_channel_id TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    title TEXT,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_channels_provider_unique
ON channels(household_id, provider, provider_channel_id);

CREATE INDEX IF NOT EXISTS idx_channels_household_type
ON channels(household_id, channel_type);

CREATE TABLE IF NOT EXISTS app_chat_messages (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    sender_role TEXT NOT NULL,
    sender_member_id TEXT,
    body TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_app_chat_messages_channel
ON app_chat_messages(channel_id, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_app_chat_messages_household
ON app_chat_messages(household_id, created_at DESC);

CREATE TABLE IF NOT EXISTS google_connections (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    email TEXT NOT NULL,
    connected_scopes_json TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    access_token TEXT,
    refresh_token TEXT,
    access_token_expires_at TEXT,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_google_connections_household
ON google_connections(household_id, member_id, active);

CREATE TABLE IF NOT EXISTS imported_candidates (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_identifier TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    state TEXT NOT NULL,
    confidence_bps INTEGER,
    requires_confirmation INTEGER NOT NULL DEFAULT 1,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_imported_candidates_source
ON imported_candidates(household_id, member_id, source_kind, source_identifier);

CREATE INDEX IF NOT EXISTS idx_imported_candidates_household_state
ON imported_candidates(household_id, member_id, state);

CREATE TABLE IF NOT EXISTS household_events (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    title TEXT NOT NULL,
    starts_at TEXT,
    ends_at TEXT,
    timezone TEXT,
    all_day INTEGER NOT NULL DEFAULT 0,
    location TEXT,
    description TEXT,
    source_candidate_id TEXT,
    status TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_household_events_household_time
ON household_events(household_id, starts_at, status);
"""


def _json_dumps(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _json_loads(raw: str | None, *, default: object) -> object:
    if not raw:
        return default
    return json.loads(raw)


class FlorenceStateDB:
    """Florence persistence backed by SQLite or Postgres."""

    def __init__(self, db_path: Path | str | None = None):
        self.database = db_path or os.getenv("FLORENCE_DATABASE_URL") or os.getenv("DATABASE_URL") or FLORENCE_DB_PATH
        self.db_path = self.database if isinstance(self.database, Path) else None
        self._conn = self._connect(self.database)
        self._init_schema()

    @staticmethod
    def _connect(database: Path | str):
        return connect_florence_db(database)

    def _init_schema(self) -> None:
        cursor = self._conn.cursor()
        cursor.executescript(FLORENCE_SCHEMA_SQL)
        cursor.execute("SELECT version FROM florence_schema_version LIMIT 1")
        row = cursor.fetchone()
        if row is None:
            cursor.execute("INSERT INTO florence_schema_version (version) VALUES (?)", (FLORENCE_SCHEMA_VERSION,))
        elif int(row["version"]) != FLORENCE_SCHEMA_VERSION:
            cursor.execute("UPDATE florence_schema_version SET version = ?", (FLORENCE_SCHEMA_VERSION,))
        self._conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @staticmethod
    def build_onboarding_session_key(household_id: str, member_id: str, thread_id: str) -> str:
        return f"{household_id}:{member_id}:{thread_id}"

    def get_onboarding_session(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
    ) -> OnboardingState | None:
        session_key = self.build_onboarding_session_key(household_id, member_id, thread_id)
        row = self._conn.execute(
            "SELECT * FROM onboarding_sessions WHERE session_key = ?",
            (session_key,),
        ).fetchone()
        return self._row_to_onboarding_state(row) if row else None

    def upsert_onboarding_session(self, state: OnboardingState) -> OnboardingState:
        now = time.time()
        session_key = self.build_onboarding_session_key(state.household_id, state.member_id, state.thread_id)
        existing = self._conn.execute(
            "SELECT created_at FROM onboarding_sessions WHERE session_key = ?",
            (session_key,),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO onboarding_sessions (
                session_key,
                household_id,
                member_id,
                thread_id,
                stage,
                parent_display_name,
                google_connected,
                child_names_json,
                school_labels_json,
                activity_labels_json,
                school_basics_collected,
                activity_basics_collected,
                group_channel_id,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_key) DO UPDATE SET
                stage = excluded.stage,
                parent_display_name = excluded.parent_display_name,
                google_connected = excluded.google_connected,
                child_names_json = excluded.child_names_json,
                school_labels_json = excluded.school_labels_json,
                activity_labels_json = excluded.activity_labels_json,
                school_basics_collected = excluded.school_basics_collected,
                activity_basics_collected = excluded.activity_basics_collected,
                group_channel_id = excluded.group_channel_id,
                updated_at = excluded.updated_at
            """,
            (
                session_key,
                state.household_id,
                state.member_id,
                state.thread_id,
                state.stage.value,
                state.parent_display_name,
                1 if state.google_connected else 0,
                _json_dumps(state.child_names),
                _json_dumps(state.school_labels),
                _json_dumps(state.activity_labels),
                1 if state.school_basics_collected else 0,
                1 if state.activity_basics_collected else 0,
                state.group_channel_id,
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return state

    def list_onboarding_sessions(self, household_id: str) -> list[OnboardingState]:
        rows = self._conn.execute(
            "SELECT * FROM onboarding_sessions WHERE household_id = ? ORDER BY updated_at DESC",
            (household_id,),
        ).fetchall()
        return [self._row_to_onboarding_state(row) for row in rows]

    def list_member_onboarding_sessions(self, *, household_id: str, member_id: str) -> list[OnboardingState]:
        rows = self._conn.execute(
            """
            SELECT * FROM onboarding_sessions
            WHERE household_id = ? AND member_id = ?
            ORDER BY updated_at DESC
            """,
            (household_id, member_id),
        ).fetchall()
        return [self._row_to_onboarding_state(row) for row in rows]

    def get_household(self, household_id: str) -> Household | None:
        row = self._conn.execute(
            "SELECT * FROM households WHERE id = ?",
            (household_id,),
        ).fetchone()
        return self._row_to_household(row) if row else None

    def upsert_household(self, household: Household) -> Household:
        now = time.time()
        existing = self._conn.execute(
            "SELECT created_at FROM households WHERE id = ?",
            (household.id,),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO households (
                id, name, timezone, status, settings_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                timezone = excluded.timezone,
                status = excluded.status,
                settings_json = excluded.settings_json,
                updated_at = excluded.updated_at
            """,
            (
                household.id,
                household.name,
                household.timezone,
                household.status.value,
                _json_dumps(household.settings),
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return household

    def list_households(self) -> list[Household]:
        rows = self._conn.execute(
            "SELECT * FROM households ORDER BY updated_at DESC"
        ).fetchall()
        return [self._row_to_household(row) for row in rows]

    def get_member(self, member_id: str) -> Member | None:
        row = self._conn.execute(
            "SELECT * FROM members WHERE id = ?",
            (member_id,),
        ).fetchone()
        return self._row_to_member(row) if row else None

    def upsert_member(self, member: Member) -> Member:
        now = time.time()
        existing = self._conn.execute(
            "SELECT created_at FROM members WHERE id = ?",
            (member.id,),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO members (
                id, household_id, display_name, role, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                household_id = excluded.household_id,
                display_name = excluded.display_name,
                role = excluded.role,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (
                member.id,
                member.household_id,
                member.display_name,
                member.role.value,
                member.status,
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return member

    def list_members(self, household_id: str) -> list[Member]:
        rows = self._conn.execute(
            "SELECT * FROM members WHERE household_id = ? ORDER BY updated_at DESC",
            (household_id,),
        ).fetchall()
        return [self._row_to_member(row) for row in rows]

    def upsert_member_identity(self, identity: MemberIdentity) -> MemberIdentity:
        now = time.time()
        existing = self._conn.execute(
            """
            SELECT created_at FROM member_identities
            WHERE kind = ? AND normalized_value = ?
            """,
            (identity.kind.value, identity.normalized_value),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO member_identities (
                id, member_id, kind, value, normalized_value, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(kind, normalized_value) DO UPDATE SET
                member_id = excluded.member_id,
                kind = excluded.kind,
                value = excluded.value,
                normalized_value = excluded.normalized_value,
                updated_at = excluded.updated_at
            """,
            (
                identity.id,
                identity.member_id,
                identity.kind.value,
                identity.value,
                identity.normalized_value,
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return identity

    def list_member_identities(self, member_id: str) -> list[MemberIdentity]:
        rows = self._conn.execute(
            "SELECT * FROM member_identities WHERE member_id = ? ORDER BY updated_at DESC",
            (member_id,),
        ).fetchall()
        return [self._row_to_member_identity(row) for row in rows]

    def find_member_by_identity(self, *, kind: IdentityKind, normalized_value: str) -> Member | None:
        row = self._conn.execute(
            """
            SELECT members.* FROM member_identities
            INNER JOIN members ON members.id = member_identities.member_id
            WHERE member_identities.kind = ? AND member_identities.normalized_value = ?
            LIMIT 1
            """,
            (kind.value, normalized_value),
        ).fetchone()
        return self._row_to_member(row) if row else None

    def find_identity(self, *, kind: IdentityKind, normalized_value: str) -> MemberIdentity | None:
        row = self._conn.execute(
            """
            SELECT * FROM member_identities
            WHERE kind = ? AND normalized_value = ?
            LIMIT 1
            """,
            (kind.value, normalized_value),
        ).fetchone()
        return self._row_to_member_identity(row) if row else None

    def get_channel(self, channel_id: str) -> Channel | None:
        row = self._conn.execute(
            "SELECT * FROM channels WHERE id = ?",
            (channel_id,),
        ).fetchone()
        return self._row_to_channel(row) if row else None

    def get_channel_by_provider_id(self, *, provider: str, provider_channel_id: str) -> Channel | None:
        row = self._conn.execute(
            """
            SELECT * FROM channels
            WHERE provider = ? AND provider_channel_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (provider, provider_channel_id),
        ).fetchone()
        return self._row_to_channel(row) if row else None

    def upsert_channel(self, channel: Channel) -> Channel:
        now = time.time()
        existing = self._conn.execute(
            """
            SELECT created_at FROM channels
            WHERE household_id = ? AND provider = ? AND provider_channel_id = ?
            """,
            (channel.household_id, channel.provider, channel.provider_channel_id),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO channels (
                id, household_id, provider, provider_channel_id, channel_type, title, metadata_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(household_id, provider, provider_channel_id) DO UPDATE SET
                household_id = excluded.household_id,
                provider = excluded.provider,
                provider_channel_id = excluded.provider_channel_id,
                channel_type = excluded.channel_type,
                title = excluded.title,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                channel.id,
                channel.household_id,
                channel.provider,
                channel.provider_channel_id,
                channel.channel_type.value,
                channel.title,
                _json_dumps(channel.metadata),
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return channel

    def list_channels(
        self,
        *,
        household_id: str,
        channel_type: ChannelType | None = None,
    ) -> list[Channel]:
        params: list[object] = [household_id]
        query = "SELECT * FROM channels WHERE household_id = ?"
        if channel_type is not None:
            query += " AND channel_type = ?"
            params.append(channel_type.value)
        query += " ORDER BY updated_at DESC"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_channel(row) for row in rows]

    def append_app_chat_message(self, message: AppChatMessage) -> AppChatMessage:
        self._conn.execute(
            """
            INSERT INTO app_chat_messages (
                id, household_id, channel_id, sender_role, sender_member_id, body, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                household_id = excluded.household_id,
                channel_id = excluded.channel_id,
                sender_role = excluded.sender_role,
                sender_member_id = excluded.sender_member_id,
                body = excluded.body,
                metadata_json = excluded.metadata_json,
                created_at = excluded.created_at
            """,
            (
                message.id,
                message.household_id,
                message.channel_id,
                message.sender_role.value,
                message.sender_member_id,
                message.body,
                _json_dumps(message.metadata),
                message.created_at,
            ),
        )
        self._conn.commit()
        return message

    def list_app_chat_messages(
        self,
        *,
        channel_id: str,
        limit: int = 50,
    ) -> list[AppChatMessage]:
        rows = self._conn.execute(
            """
            SELECT * FROM app_chat_messages
            WHERE channel_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (channel_id, max(1, limit)),
        ).fetchall()
        return [self._row_to_app_chat_message(row) for row in reversed(rows)]

    def get_app_chat_message(self, message_id: str) -> AppChatMessage | None:
        row = self._conn.execute(
            "SELECT * FROM app_chat_messages WHERE id = ?",
            (message_id,),
        ).fetchone()
        return self._row_to_app_chat_message(row) if row else None

    def get_google_connection(self, connection_id: str) -> GoogleConnection | None:
        row = self._conn.execute(
            "SELECT * FROM google_connections WHERE id = ?",
            (connection_id,),
        ).fetchone()
        return self._row_to_google_connection(row) if row else None

    def upsert_google_connection(self, connection: GoogleConnection) -> GoogleConnection:
        now = time.time()
        existing = self._conn.execute(
            "SELECT created_at FROM google_connections WHERE id = ?",
            (connection.id,),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO google_connections (
                id,
                household_id,
                member_id,
                email,
                connected_scopes_json,
                active,
                access_token,
                refresh_token,
                access_token_expires_at,
                metadata_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                household_id = excluded.household_id,
                member_id = excluded.member_id,
                email = excluded.email,
                connected_scopes_json = excluded.connected_scopes_json,
                active = excluded.active,
                access_token = excluded.access_token,
                refresh_token = excluded.refresh_token,
                access_token_expires_at = excluded.access_token_expires_at,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                connection.id,
                connection.household_id,
                connection.member_id,
                connection.email,
                _json_dumps([scope.value for scope in connection.connected_scopes]),
                1 if connection.active else 0,
                connection.access_token,
                connection.refresh_token,
                connection.access_token_expires_at,
                _json_dumps(connection.metadata),
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return connection

    def list_google_connections(
        self,
        *,
        household_id: str,
        member_id: str | None = None,
        active_only: bool = True,
    ) -> list[GoogleConnection]:
        params: list[object] = [household_id]
        query = "SELECT * FROM google_connections WHERE household_id = ?"
        if member_id is not None:
            query += " AND member_id = ?"
            params.append(member_id)
        if active_only:
            query += " AND active = 1"
        query += " ORDER BY updated_at DESC"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_google_connection(row) for row in rows]

    def get_imported_candidate_by_source(
        self,
        *,
        household_id: str,
        member_id: str,
        source_kind: GoogleSourceKind,
        source_identifier: str,
    ) -> ImportedCandidate | None:
        row = self._conn.execute(
            """
            SELECT * FROM imported_candidates
            WHERE household_id = ? AND member_id = ? AND source_kind = ? AND source_identifier = ?
            """,
            (household_id, member_id, source_kind.value, source_identifier),
        ).fetchone()
        return self._row_to_imported_candidate(row) if row else None

    def upsert_imported_candidate(self, candidate: ImportedCandidate) -> ImportedCandidate:
        now = time.time()
        existing = self._conn.execute(
            "SELECT created_at FROM imported_candidates WHERE id = ?",
            (candidate.id,),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO imported_candidates (
                id,
                household_id,
                member_id,
                source_kind,
                source_identifier,
                title,
                summary,
                state,
                confidence_bps,
                requires_confirmation,
                metadata_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                household_id = excluded.household_id,
                member_id = excluded.member_id,
                source_kind = excluded.source_kind,
                source_identifier = excluded.source_identifier,
                title = excluded.title,
                summary = excluded.summary,
                state = excluded.state,
                confidence_bps = excluded.confidence_bps,
                requires_confirmation = excluded.requires_confirmation,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                candidate.id,
                candidate.household_id,
                candidate.member_id,
                candidate.source_kind.value,
                candidate.source_identifier,
                candidate.title,
                candidate.summary,
                candidate.state.value,
                candidate.confidence_bps,
                1 if candidate.requires_confirmation else 0,
                _json_dumps(candidate.metadata),
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return candidate

    def set_imported_candidate_state(self, candidate_id: str, state: CandidateState) -> None:
        self._conn.execute(
            "UPDATE imported_candidates SET state = ?, updated_at = ? WHERE id = ?",
            (state.value, time.time(), candidate_id),
        )
        self._conn.commit()

    def list_imported_candidates(
        self,
        *,
        household_id: str,
        member_id: str | None = None,
        state: CandidateState | None = None,
    ) -> list[ImportedCandidate]:
        params: list[object] = [household_id]
        query = "SELECT * FROM imported_candidates WHERE household_id = ?"
        if member_id is not None:
            query += " AND member_id = ?"
            params.append(member_id)
        if state is not None:
            query += " AND state = ?"
            params.append(state.value)
        query += " ORDER BY updated_at DESC"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_imported_candidate(row) for row in rows]

    def get_imported_candidate(self, candidate_id: str) -> ImportedCandidate | None:
        row = self._conn.execute(
            "SELECT * FROM imported_candidates WHERE id = ?",
            (candidate_id,),
        ).fetchone()
        return self._row_to_imported_candidate(row) if row else None

    def upsert_household_event(self, event: HouseholdEvent) -> HouseholdEvent:
        now = time.time()
        existing = self._conn.execute(
            "SELECT created_at FROM household_events WHERE id = ?",
            (event.id,),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO household_events (
                id,
                household_id,
                title,
                starts_at,
                ends_at,
                timezone,
                all_day,
                location,
                description,
                source_candidate_id,
                status,
                metadata_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                household_id = excluded.household_id,
                title = excluded.title,
                starts_at = excluded.starts_at,
                ends_at = excluded.ends_at,
                timezone = excluded.timezone,
                all_day = excluded.all_day,
                location = excluded.location,
                description = excluded.description,
                source_candidate_id = excluded.source_candidate_id,
                status = excluded.status,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                event.id,
                event.household_id,
                event.title,
                event.starts_at,
                event.ends_at,
                event.timezone,
                1 if event.all_day else 0,
                event.location,
                event.description,
                event.source_candidate_id,
                event.status.value,
                _json_dumps(event.metadata),
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return event

    def list_household_events(
        self,
        *,
        household_id: str,
        status: HouseholdEventStatus | None = None,
    ) -> list[HouseholdEvent]:
        params: list[object] = [household_id]
        query = "SELECT * FROM household_events WHERE household_id = ?"
        if status is not None:
            query += " AND status = ?"
            params.append(status.value)
        query += " ORDER BY COALESCE(starts_at, '') ASC, updated_at DESC"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_household_event(row) for row in rows]

    @staticmethod
    def _row_to_onboarding_state(row: RowLike) -> OnboardingState:
        return OnboardingState(
            household_id=str(row["household_id"]),
            member_id=str(row["member_id"]),
            thread_id=str(row["thread_id"]),
            stage=OnboardingStage(str(row["stage"])),
            parent_display_name=str(row["parent_display_name"]) if row["parent_display_name"] is not None else None,
            google_connected=bool(row["google_connected"]),
            child_names=list(_json_loads(row["child_names_json"], default=[])),
            school_labels=list(_json_loads(row["school_labels_json"], default=[])),
            activity_labels=list(_json_loads(row["activity_labels_json"], default=[])),
            school_basics_collected=bool(row["school_basics_collected"]),
            activity_basics_collected=bool(row["activity_basics_collected"]),
            group_channel_id=str(row["group_channel_id"]) if row["group_channel_id"] is not None else None,
        )

    @staticmethod
    def _row_to_household(row: RowLike) -> Household:
        return Household(
            id=str(row["id"]),
            name=str(row["name"]),
            timezone=str(row["timezone"]),
            status=HouseholdStatus(str(row["status"])),
            settings=dict(_json_loads(row["settings_json"], default={})),
        )

    def _row_to_member(self, row: RowLike) -> Member:
        member_id = str(row["id"])
        identities = self.list_member_identities(member_id)
        return Member(
            id=member_id,
            household_id=str(row["household_id"]),
            display_name=str(row["display_name"]),
            role=MemberRole(str(row["role"])),
            status=str(row["status"]),
            external_identities={identity.kind.value: identity.value for identity in identities},
        )

    @staticmethod
    def _row_to_member_identity(row: RowLike) -> MemberIdentity:
        return MemberIdentity(
            id=str(row["id"]),
            member_id=str(row["member_id"]),
            kind=IdentityKind(str(row["kind"])),
            value=str(row["value"]),
            normalized_value=str(row["normalized_value"]),
        )

    @staticmethod
    def _row_to_channel(row: RowLike) -> Channel:
        return Channel(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            provider=str(row["provider"]),
            provider_channel_id=str(row["provider_channel_id"]),
            channel_type=ChannelType(str(row["channel_type"])),
            title=str(row["title"]) if row["title"] is not None else None,
            metadata=dict(_json_loads(row["metadata_json"], default={})),
        )

    @staticmethod
    def _row_to_app_chat_message(row: RowLike) -> AppChatMessage:
        return AppChatMessage(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            channel_id=str(row["channel_id"]),
            sender_role=AppChatMessageRole(str(row["sender_role"])),
            sender_member_id=str(row["sender_member_id"]) if row["sender_member_id"] is not None else None,
            body=str(row["body"]),
            created_at=float(row["created_at"]),
            metadata=dict(_json_loads(row["metadata_json"], default={})),
        )

    @staticmethod
    def _row_to_google_connection(row: RowLike) -> GoogleConnection:
        scopes_raw = _json_loads(row["connected_scopes_json"], default=[])
        scopes = tuple(GoogleSourceKind(str(scope)) for scope in scopes_raw)
        return GoogleConnection(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            member_id=str(row["member_id"]),
            email=str(row["email"]),
            connected_scopes=scopes,
            active=bool(row["active"]),
            access_token=str(row["access_token"]) if row["access_token"] is not None else None,
            refresh_token=str(row["refresh_token"]) if row["refresh_token"] is not None else None,
            access_token_expires_at=(
                str(row["access_token_expires_at"]) if row["access_token_expires_at"] is not None else None
            ),
            metadata=dict(_json_loads(row["metadata_json"], default={})),
        )

    @staticmethod
    def _row_to_imported_candidate(row: RowLike) -> ImportedCandidate:
        return ImportedCandidate(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            member_id=str(row["member_id"]),
            source_kind=GoogleSourceKind(str(row["source_kind"])),
            source_identifier=str(row["source_identifier"]),
            title=str(row["title"]),
            summary=str(row["summary"]),
            state=CandidateState(str(row["state"])),
            confidence_bps=int(row["confidence_bps"]) if row["confidence_bps"] is not None else None,
            requires_confirmation=bool(row["requires_confirmation"]),
            metadata=dict(_json_loads(row["metadata_json"], default={})),
        )

    @staticmethod
    def _row_to_household_event(row: RowLike) -> HouseholdEvent:
        return HouseholdEvent(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            title=str(row["title"]),
            starts_at=str(row["starts_at"]) if row["starts_at"] is not None else None,
            ends_at=str(row["ends_at"]) if row["ends_at"] is not None else None,
            timezone=str(row["timezone"]) if row["timezone"] is not None else None,
            all_day=bool(row["all_day"]),
            location=str(row["location"]) if row["location"] is not None else None,
            description=str(row["description"]) if row["description"] is not None else None,
            source_candidate_id=str(row["source_candidate_id"]) if row["source_candidate_id"] is not None else None,
            status=HouseholdEventStatus(str(row["status"])),
            metadata=dict(_json_loads(row["metadata_json"], default={})),
        )
