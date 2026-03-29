"""Database-backed Florence product state store."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from florence.contracts import (
    CandidateState,
    ChannelMessage,
    ChannelMessageRole,
    Channel,
    ChannelType,
    ChildProfile,
    GoogleConnection,
    GoogleSourceKind,
    HouseholdEvent,
    HouseholdEventStatus,
    HouseholdMeal,
    HouseholdMealStatus,
    HouseholdNudge,
    HouseholdNudgeStatus,
    HouseholdNudgeTargetKind,
    Household,
    HouseholdSourceMatcherKind,
    HouseholdProfileItem,
    HouseholdProfileKind,
    HouseholdSourceRule,
    HouseholdSourceVisibility,
    HouseholdRoutine,
    HouseholdRoutineStatus,
    HouseholdShoppingItem,
    HouseholdShoppingItemStatus,
    HouseholdStatus,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
    IdentityKind,
    ImportedCandidate,
    Member,
    MemberIdentity,
    MemberRole,
    PilotEvent,
)
from florence.onboarding import OnboardingStage, OnboardingState
from florence.state.db import RowLike, connect_florence_db

FLORENCE_DB_PATH = Path(os.getenv("HERMES_HOME", Path.home() / ".hermes")) / "florence.db"
FLORENCE_SCHEMA_VERSION = 9

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
    metadata_json TEXT NOT NULL DEFAULT '{}',
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

CREATE TABLE IF NOT EXISTS child_profiles (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    full_name TEXT NOT NULL,
    birthdate TEXT,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_child_profiles_household
ON child_profiles(household_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS household_profile_items (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    label TEXT NOT NULL,
    member_id TEXT,
    child_id TEXT,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_household_profile_items_household_kind
ON household_profile_items(household_id, kind, updated_at DESC);

CREATE TABLE IF NOT EXISTS channel_messages (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    sender_role TEXT NOT NULL,
    sender_member_id TEXT,
    body TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_channel_messages_channel
ON channel_messages(channel_id, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_channel_messages_household
ON channel_messages(household_id, created_at DESC);

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

CREATE TABLE IF NOT EXISTS household_source_rules (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    matcher_kind TEXT NOT NULL,
    matcher_value TEXT NOT NULL,
    visibility TEXT NOT NULL,
    label TEXT,
    created_by_member_id TEXT,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_household_source_rules_unique
ON household_source_rules(household_id, source_kind, matcher_kind, matcher_value);

CREATE INDEX IF NOT EXISTS idx_household_source_rules_visibility
ON household_source_rules(household_id, visibility, source_kind, updated_at DESC);

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

CREATE TABLE IF NOT EXISTS household_work_items (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL,
    owner_member_id TEXT,
    child_id TEXT,
    due_at TEXT,
    starts_at TEXT,
    completed_at TEXT,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_household_work_items_household_status
ON household_work_items(household_id, status, due_at, updated_at DESC);

CREATE TABLE IF NOT EXISTS household_routines (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    title TEXT NOT NULL,
    cadence TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL,
    owner_member_id TEXT,
    child_id TEXT,
    next_due_at TEXT,
    last_completed_at TEXT,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_household_routines_household_status
ON household_routines(household_id, status, next_due_at, updated_at DESC);

CREATE TABLE IF NOT EXISTS household_nudges (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    target_id TEXT,
    message TEXT NOT NULL,
    status TEXT NOT NULL,
    recipient_member_id TEXT,
    channel_id TEXT,
    scheduled_for TEXT,
    sent_at TEXT,
    acknowledged_at TEXT,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_household_nudges_household_status
ON household_nudges(household_id, status, scheduled_for, updated_at DESC);

CREATE TABLE IF NOT EXISTS household_meals (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    title TEXT NOT NULL,
    meal_type TEXT NOT NULL,
    scheduled_for TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_household_meals_household_time
ON household_meals(household_id, scheduled_for, status);

CREATE TABLE IF NOT EXISTS household_shopping_items (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    title TEXT NOT NULL,
    list_name TEXT NOT NULL,
    status TEXT NOT NULL,
    quantity TEXT,
    unit TEXT,
    notes TEXT,
    meal_id TEXT,
    needed_by TEXT,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_household_shopping_items_household_status
ON household_shopping_items(household_id, list_name, status, needed_by, updated_at DESC);

CREATE TABLE IF NOT EXISTS pilot_events (
    id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    member_id TEXT,
    channel_id TEXT,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pilot_events_household_created
ON pilot_events(household_id, created_at DESC);
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
        self._ensure_onboarding_session_columns()
        cursor.execute("SELECT version FROM florence_schema_version LIMIT 1")
        row = cursor.fetchone()
        if row is None:
            cursor.execute("INSERT INTO florence_schema_version (version) VALUES (?)", (FLORENCE_SCHEMA_VERSION,))
        elif int(row["version"]) != FLORENCE_SCHEMA_VERSION:
            cursor.execute("UPDATE florence_schema_version SET version = ?", (FLORENCE_SCHEMA_VERSION,))
        self._conn.commit()

    def _ensure_onboarding_session_columns(self) -> None:
        columns = self._table_columns("onboarding_sessions")
        if "metadata_json" not in columns:
            self._conn.execute("ALTER TABLE onboarding_sessions ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}'")

    def _table_columns(self, table_name: str) -> set[str]:
        database_str = str(self.database).strip().lower()
        if database_str.startswith(("postgres://", "postgresql://")):
            rows = self._conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = ? AND table_schema = current_schema()
                """,
                (table_name,),
            ).fetchall()
            return {str(row["column_name"]) for row in rows}
        rows = self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row["name"]) for row in rows}

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
                metadata_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                metadata_json = excluded.metadata_json,
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
                _json_dumps(state.metadata),
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

    def replace_child_profiles(self, *, household_id: str, children: list[ChildProfile]) -> list[ChildProfile]:
        self._conn.execute("DELETE FROM child_profiles WHERE household_id = ?", (household_id,))
        now = time.time()
        for child in children:
            self._conn.execute(
                """
                INSERT INTO child_profiles (
                    id, household_id, full_name, birthdate, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    child.id,
                    child.household_id,
                    child.full_name,
                    child.birthdate,
                    _json_dumps(child.metadata),
                    now,
                    now,
                ),
            )
        self._conn.commit()
        return children

    def list_child_profiles(self, *, household_id: str) -> list[ChildProfile]:
        rows = self._conn.execute(
            "SELECT * FROM child_profiles WHERE household_id = ? ORDER BY updated_at DESC, full_name ASC",
            (household_id,),
        ).fetchall()
        return [self._row_to_child_profile(row) for row in rows]

    def replace_household_profile_items(
        self,
        *,
        household_id: str,
        kind: HouseholdProfileKind,
        items: list[HouseholdProfileItem],
        member_id: str | None = None,
    ) -> list[HouseholdProfileItem]:
        if member_id is None:
            self._conn.execute(
                "DELETE FROM household_profile_items WHERE household_id = ? AND kind = ?",
                (household_id, kind.value),
            )
        else:
            self._conn.execute(
                """
                DELETE FROM household_profile_items
                WHERE household_id = ? AND kind = ? AND member_id = ?
                """,
                (household_id, kind.value, member_id),
            )
        now = time.time()
        for item in items:
            self._conn.execute(
                """
                INSERT INTO household_profile_items (
                    id, household_id, kind, label, member_id, child_id, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.id,
                    item.household_id,
                    item.kind.value,
                    item.label,
                    item.member_id,
                    item.child_id,
                    _json_dumps(item.metadata),
                    now,
                    now,
                ),
            )
        self._conn.commit()
        return items

    def list_household_profile_items(
        self,
        *,
        household_id: str,
        kind: HouseholdProfileKind | None = None,
        member_id: str | None = None,
    ) -> list[HouseholdProfileItem]:
        params: list[object] = [household_id]
        query = "SELECT * FROM household_profile_items WHERE household_id = ?"
        if kind is not None:
            query += " AND kind = ?"
            params.append(kind.value)
        if member_id is not None:
            query += " AND member_id = ?"
            params.append(member_id)
        query += " ORDER BY updated_at DESC, label ASC"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_household_profile_item(row) for row in rows]

    def append_channel_message(self, message: ChannelMessage) -> ChannelMessage:
        self._conn.execute(
            """
            INSERT INTO channel_messages (
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

    def list_channel_messages(
        self,
        *,
        channel_id: str,
        limit: int = 50,
    ) -> list[ChannelMessage]:
        rows = self._conn.execute(
            """
            SELECT * FROM channel_messages
            WHERE channel_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (channel_id, max(1, limit)),
        ).fetchall()
        return [self._row_to_channel_message(row) for row in reversed(rows)]

    def get_channel_message(self, message_id: str) -> ChannelMessage | None:
        row = self._conn.execute(
            "SELECT * FROM channel_messages WHERE id = ?",
            (message_id,),
        ).fetchone()
        return self._row_to_channel_message(row) if row else None

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

    def find_google_connection_by_email(
        self,
        *,
        email: str,
        active_only: bool = True,
    ) -> GoogleConnection | None:
        normalized = " ".join(email.split()).strip().lower()
        if not normalized:
            return None
        params: list[object] = [normalized]
        query = "SELECT * FROM google_connections WHERE LOWER(email) = ?"
        if active_only:
            query += " AND active = 1"
        query += " ORDER BY updated_at DESC LIMIT 1"
        row = self._conn.execute(query, tuple(params)).fetchone()
        return self._row_to_google_connection(row) if row else None

    def upsert_household_source_rule(self, rule: HouseholdSourceRule) -> HouseholdSourceRule:
        now = time.time()
        existing = self._conn.execute(
            """
            SELECT created_at FROM household_source_rules
            WHERE household_id = ? AND source_kind = ? AND matcher_kind = ? AND matcher_value = ?
            """,
            (
                rule.household_id,
                rule.source_kind.value,
                rule.matcher_kind.value,
                rule.matcher_value,
            ),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO household_source_rules (
                id,
                household_id,
                source_kind,
                matcher_kind,
                matcher_value,
                visibility,
                label,
                created_by_member_id,
                metadata_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(household_id, source_kind, matcher_kind, matcher_value) DO UPDATE SET
                visibility = excluded.visibility,
                label = excluded.label,
                created_by_member_id = excluded.created_by_member_id,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                rule.id,
                rule.household_id,
                rule.source_kind.value,
                rule.matcher_kind.value,
                rule.matcher_value,
                rule.visibility.value,
                rule.label,
                rule.created_by_member_id,
                _json_dumps(rule.metadata),
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return rule

    def list_household_source_rules(
        self,
        *,
        household_id: str,
        source_kind: GoogleSourceKind | None = None,
        visibility: HouseholdSourceVisibility | None = None,
    ) -> list[HouseholdSourceRule]:
        params: list[object] = [household_id]
        query = "SELECT * FROM household_source_rules WHERE household_id = ?"
        if source_kind is not None:
            query += " AND source_kind = ?"
            params.append(source_kind.value)
        if visibility is not None:
            query += " AND visibility = ?"
            params.append(visibility.value)
        query += " ORDER BY updated_at DESC, matcher_value ASC"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_household_source_rule(row) for row in rows]

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

    def get_household_work_item(self, work_item_id: str) -> HouseholdWorkItem | None:
        row = self._conn.execute(
            "SELECT * FROM household_work_items WHERE id = ?",
            (work_item_id,),
        ).fetchone()
        return self._row_to_household_work_item(row) if row else None

    def upsert_household_work_item(self, work_item: HouseholdWorkItem) -> HouseholdWorkItem:
        now = time.time()
        existing = self._conn.execute(
            "SELECT created_at FROM household_work_items WHERE id = ?",
            (work_item.id,),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO household_work_items (
                id,
                household_id,
                title,
                description,
                status,
                owner_member_id,
                child_id,
                due_at,
                starts_at,
                completed_at,
                metadata_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                household_id = excluded.household_id,
                title = excluded.title,
                description = excluded.description,
                status = excluded.status,
                owner_member_id = excluded.owner_member_id,
                child_id = excluded.child_id,
                due_at = excluded.due_at,
                starts_at = excluded.starts_at,
                completed_at = excluded.completed_at,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                work_item.id,
                work_item.household_id,
                work_item.title,
                work_item.description,
                work_item.status.value,
                work_item.owner_member_id,
                work_item.child_id,
                work_item.due_at,
                work_item.starts_at,
                work_item.completed_at,
                _json_dumps(work_item.metadata),
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return work_item

    def list_household_work_items(
        self,
        *,
        household_id: str,
        status: HouseholdWorkItemStatus | None = None,
        owner_member_id: str | None = None,
    ) -> list[HouseholdWorkItem]:
        params: list[object] = [household_id]
        query = "SELECT * FROM household_work_items WHERE household_id = ?"
        if status is not None:
            query += " AND status = ?"
            params.append(status.value)
        if owner_member_id is not None:
            query += " AND owner_member_id = ?"
            params.append(owner_member_id)
        query += " ORDER BY COALESCE(due_at, '') ASC, updated_at DESC, title ASC"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_household_work_item(row) for row in rows]

    def get_household_routine(self, routine_id: str) -> HouseholdRoutine | None:
        row = self._conn.execute(
            "SELECT * FROM household_routines WHERE id = ?",
            (routine_id,),
        ).fetchone()
        return self._row_to_household_routine(row) if row else None

    def upsert_household_routine(self, routine: HouseholdRoutine) -> HouseholdRoutine:
        now = time.time()
        existing = self._conn.execute(
            "SELECT created_at FROM household_routines WHERE id = ?",
            (routine.id,),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO household_routines (
                id,
                household_id,
                title,
                cadence,
                description,
                status,
                owner_member_id,
                child_id,
                next_due_at,
                last_completed_at,
                metadata_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                household_id = excluded.household_id,
                title = excluded.title,
                cadence = excluded.cadence,
                description = excluded.description,
                status = excluded.status,
                owner_member_id = excluded.owner_member_id,
                child_id = excluded.child_id,
                next_due_at = excluded.next_due_at,
                last_completed_at = excluded.last_completed_at,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                routine.id,
                routine.household_id,
                routine.title,
                routine.cadence,
                routine.description,
                routine.status.value,
                routine.owner_member_id,
                routine.child_id,
                routine.next_due_at,
                routine.last_completed_at,
                _json_dumps(routine.metadata),
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return routine

    def list_household_routines(
        self,
        *,
        household_id: str,
        status: HouseholdRoutineStatus | None = None,
        owner_member_id: str | None = None,
    ) -> list[HouseholdRoutine]:
        params: list[object] = [household_id]
        query = "SELECT * FROM household_routines WHERE household_id = ?"
        if status is not None:
            query += " AND status = ?"
            params.append(status.value)
        if owner_member_id is not None:
            query += " AND owner_member_id = ?"
            params.append(owner_member_id)
        query += " ORDER BY COALESCE(next_due_at, '') ASC, updated_at DESC, title ASC"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_household_routine(row) for row in rows]

    def get_household_nudge(self, nudge_id: str) -> HouseholdNudge | None:
        row = self._conn.execute(
            "SELECT * FROM household_nudges WHERE id = ?",
            (nudge_id,),
        ).fetchone()
        return self._row_to_household_nudge(row) if row else None

    def upsert_household_nudge(self, nudge: HouseholdNudge) -> HouseholdNudge:
        now = time.time()
        existing = self._conn.execute(
            "SELECT created_at FROM household_nudges WHERE id = ?",
            (nudge.id,),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO household_nudges (
                id,
                household_id,
                target_kind,
                target_id,
                message,
                status,
                recipient_member_id,
                channel_id,
                scheduled_for,
                sent_at,
                acknowledged_at,
                metadata_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                household_id = excluded.household_id,
                target_kind = excluded.target_kind,
                target_id = excluded.target_id,
                message = excluded.message,
                status = excluded.status,
                recipient_member_id = excluded.recipient_member_id,
                channel_id = excluded.channel_id,
                scheduled_for = excluded.scheduled_for,
                sent_at = excluded.sent_at,
                acknowledged_at = excluded.acknowledged_at,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                nudge.id,
                nudge.household_id,
                nudge.target_kind.value,
                nudge.target_id,
                nudge.message,
                nudge.status.value,
                nudge.recipient_member_id,
                nudge.channel_id,
                nudge.scheduled_for,
                nudge.sent_at,
                nudge.acknowledged_at,
                _json_dumps(nudge.metadata),
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return nudge

    def list_household_nudges(
        self,
        *,
        household_id: str,
        status: HouseholdNudgeStatus | None = None,
        recipient_member_id: str | None = None,
    ) -> list[HouseholdNudge]:
        params: list[object] = [household_id]
        query = "SELECT * FROM household_nudges WHERE household_id = ?"
        if status is not None:
            query += " AND status = ?"
            params.append(status.value)
        if recipient_member_id is not None:
            query += " AND recipient_member_id = ?"
            params.append(recipient_member_id)
        query += " ORDER BY COALESCE(scheduled_for, '') ASC, updated_at DESC"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_household_nudge(row) for row in rows]

    def get_household_meal(self, meal_id: str) -> HouseholdMeal | None:
        row = self._conn.execute(
            "SELECT * FROM household_meals WHERE id = ?",
            (meal_id,),
        ).fetchone()
        return self._row_to_household_meal(row) if row else None

    def upsert_household_meal(self, meal: HouseholdMeal) -> HouseholdMeal:
        now = time.time()
        existing = self._conn.execute(
            "SELECT created_at FROM household_meals WHERE id = ?",
            (meal.id,),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO household_meals (
                id,
                household_id,
                title,
                meal_type,
                scheduled_for,
                description,
                status,
                metadata_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                household_id = excluded.household_id,
                title = excluded.title,
                meal_type = excluded.meal_type,
                scheduled_for = excluded.scheduled_for,
                description = excluded.description,
                status = excluded.status,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                meal.id,
                meal.household_id,
                meal.title,
                meal.meal_type,
                meal.scheduled_for,
                meal.description,
                meal.status.value,
                _json_dumps(meal.metadata),
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return meal

    def list_household_meals(
        self,
        *,
        household_id: str,
        status: HouseholdMealStatus | None = None,
    ) -> list[HouseholdMeal]:
        params: list[object] = [household_id]
        query = "SELECT * FROM household_meals WHERE household_id = ?"
        if status is not None:
            query += " AND status = ?"
            params.append(status.value)
        query += " ORDER BY scheduled_for ASC, updated_at DESC"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_household_meal(row) for row in rows]

    def get_household_shopping_item(self, item_id: str) -> HouseholdShoppingItem | None:
        row = self._conn.execute(
            "SELECT * FROM household_shopping_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        return self._row_to_household_shopping_item(row) if row else None

    def upsert_household_shopping_item(self, item: HouseholdShoppingItem) -> HouseholdShoppingItem:
        now = time.time()
        existing = self._conn.execute(
            "SELECT created_at FROM household_shopping_items WHERE id = ?",
            (item.id,),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        self._conn.execute(
            """
            INSERT INTO household_shopping_items (
                id,
                household_id,
                title,
                list_name,
                status,
                quantity,
                unit,
                notes,
                meal_id,
                needed_by,
                metadata_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                household_id = excluded.household_id,
                title = excluded.title,
                list_name = excluded.list_name,
                status = excluded.status,
                quantity = excluded.quantity,
                unit = excluded.unit,
                notes = excluded.notes,
                meal_id = excluded.meal_id,
                needed_by = excluded.needed_by,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                item.id,
                item.household_id,
                item.title,
                item.list_name,
                item.status.value,
                item.quantity,
                item.unit,
                item.notes,
                item.meal_id,
                item.needed_by,
                _json_dumps(item.metadata),
                created_at,
                now,
            ),
        )
        self._conn.commit()
        return item

    def list_household_shopping_items(
        self,
        *,
        household_id: str,
        list_name: str | None = None,
        status: HouseholdShoppingItemStatus | None = None,
    ) -> list[HouseholdShoppingItem]:
        params: list[object] = [household_id]
        query = "SELECT * FROM household_shopping_items WHERE household_id = ?"
        if list_name is not None:
            query += " AND list_name = ?"
            params.append(list_name)
        if status is not None:
            query += " AND status = ?"
            params.append(status.value)
        query += " ORDER BY COALESCE(needed_by, '') ASC, updated_at DESC, title ASC"
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_household_shopping_item(row) for row in rows]

    def upsert_pilot_event(self, event: PilotEvent) -> PilotEvent:
        self._conn.execute(
            """
            INSERT INTO pilot_events (
                id,
                household_id,
                event_type,
                member_id,
                channel_id,
                metadata_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                household_id = excluded.household_id,
                event_type = excluded.event_type,
                member_id = excluded.member_id,
                channel_id = excluded.channel_id,
                metadata_json = excluded.metadata_json,
                created_at = excluded.created_at
            """,
            (
                event.id,
                event.household_id,
                event.event_type,
                event.member_id,
                event.channel_id,
                _json_dumps(event.metadata),
                event.created_at,
            ),
        )
        self._conn.commit()
        return event

    def list_pilot_events(
        self,
        *,
        household_id: str,
        event_type: str | None = None,
        limit: int = 50,
    ) -> list[PilotEvent]:
        params: list[object] = [household_id]
        query = "SELECT * FROM pilot_events WHERE household_id = ?"
        if event_type is not None:
            query += " AND event_type = ?"
            params.append(event_type)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(max(1, limit))
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_pilot_event(row) for row in rows]

    @staticmethod
    def _row_to_onboarding_state(row: RowLike) -> OnboardingState:
        try:
            metadata_raw = row["metadata_json"]
        except (KeyError, IndexError, TypeError):
            metadata_raw = None
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
            metadata=dict(_json_loads(metadata_raw, default={})),
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
    def _row_to_child_profile(row: RowLike) -> ChildProfile:
        return ChildProfile(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            full_name=str(row["full_name"]),
            birthdate=str(row["birthdate"]) if row["birthdate"] is not None else None,
            metadata=dict(_json_loads(row["metadata_json"], default={})),
        )

    @staticmethod
    def _row_to_household_profile_item(row: RowLike) -> HouseholdProfileItem:
        return HouseholdProfileItem(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            kind=HouseholdProfileKind(str(row["kind"])),
            label=str(row["label"]),
            member_id=str(row["member_id"]) if row["member_id"] is not None else None,
            child_id=str(row["child_id"]) if row["child_id"] is not None else None,
            metadata=dict(_json_loads(row["metadata_json"], default={})),
        )

    @staticmethod
    def _row_to_channel_message(row: RowLike) -> ChannelMessage:
        return ChannelMessage(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            channel_id=str(row["channel_id"]),
            sender_role=ChannelMessageRole(str(row["sender_role"])),
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
    def _row_to_household_source_rule(row: RowLike) -> HouseholdSourceRule:
        return HouseholdSourceRule(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            source_kind=GoogleSourceKind(str(row["source_kind"])),
            matcher_kind=HouseholdSourceMatcherKind(str(row["matcher_kind"])),
            matcher_value=str(row["matcher_value"]),
            visibility=HouseholdSourceVisibility(str(row["visibility"])),
            label=str(row["label"]) if row["label"] is not None else None,
            created_by_member_id=str(row["created_by_member_id"]) if row["created_by_member_id"] is not None else None,
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

    @staticmethod
    def _row_to_household_work_item(row: RowLike) -> HouseholdWorkItem:
        return HouseholdWorkItem(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            title=str(row["title"]),
            description=str(row["description"]) if row["description"] is not None else None,
            status=HouseholdWorkItemStatus(str(row["status"])),
            owner_member_id=str(row["owner_member_id"]) if row["owner_member_id"] is not None else None,
            child_id=str(row["child_id"]) if row["child_id"] is not None else None,
            due_at=str(row["due_at"]) if row["due_at"] is not None else None,
            starts_at=str(row["starts_at"]) if row["starts_at"] is not None else None,
            completed_at=str(row["completed_at"]) if row["completed_at"] is not None else None,
            metadata=dict(_json_loads(row["metadata_json"], default={})),
        )

    @staticmethod
    def _row_to_household_routine(row: RowLike) -> HouseholdRoutine:
        return HouseholdRoutine(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            title=str(row["title"]),
            cadence=str(row["cadence"]),
            description=str(row["description"]) if row["description"] is not None else None,
            status=HouseholdRoutineStatus(str(row["status"])),
            owner_member_id=str(row["owner_member_id"]) if row["owner_member_id"] is not None else None,
            child_id=str(row["child_id"]) if row["child_id"] is not None else None,
            next_due_at=str(row["next_due_at"]) if row["next_due_at"] is not None else None,
            last_completed_at=str(row["last_completed_at"]) if row["last_completed_at"] is not None else None,
            metadata=dict(_json_loads(row["metadata_json"], default={})),
        )

    @staticmethod
    def _row_to_household_nudge(row: RowLike) -> HouseholdNudge:
        return HouseholdNudge(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            target_kind=HouseholdNudgeTargetKind(str(row["target_kind"])),
            target_id=str(row["target_id"]) if row["target_id"] is not None else None,
            message=str(row["message"]),
            status=HouseholdNudgeStatus(str(row["status"])),
            recipient_member_id=str(row["recipient_member_id"]) if row["recipient_member_id"] is not None else None,
            channel_id=str(row["channel_id"]) if row["channel_id"] is not None else None,
            scheduled_for=str(row["scheduled_for"]) if row["scheduled_for"] is not None else None,
            sent_at=str(row["sent_at"]) if row["sent_at"] is not None else None,
            acknowledged_at=str(row["acknowledged_at"]) if row["acknowledged_at"] is not None else None,
            metadata=dict(_json_loads(row["metadata_json"], default={})),
        )

    @staticmethod
    def _row_to_household_meal(row: RowLike) -> HouseholdMeal:
        return HouseholdMeal(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            title=str(row["title"]),
            meal_type=str(row["meal_type"]),
            scheduled_for=str(row["scheduled_for"]),
            description=str(row["description"]) if row["description"] is not None else None,
            status=HouseholdMealStatus(str(row["status"])),
            metadata=dict(_json_loads(row["metadata_json"], default={})),
        )

    @staticmethod
    def _row_to_household_shopping_item(row: RowLike) -> HouseholdShoppingItem:
        return HouseholdShoppingItem(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            title=str(row["title"]),
            list_name=str(row["list_name"]),
            status=HouseholdShoppingItemStatus(str(row["status"])),
            quantity=str(row["quantity"]) if row["quantity"] is not None else None,
            unit=str(row["unit"]) if row["unit"] is not None else None,
            notes=str(row["notes"]) if row["notes"] is not None else None,
            meal_id=str(row["meal_id"]) if row["meal_id"] is not None else None,
            needed_by=str(row["needed_by"]) if row["needed_by"] is not None else None,
            metadata=dict(_json_loads(row["metadata_json"], default={})),
        )

    @staticmethod
    def _row_to_pilot_event(row: RowLike) -> PilotEvent:
        return PilotEvent(
            id=str(row["id"]),
            household_id=str(row["household_id"]),
            event_type=str(row["event_type"]),
            member_id=str(row["member_id"]) if row["member_id"] is not None else None,
            channel_id=str(row["channel_id"]) if row["channel_id"] is not None else None,
            metadata=dict(_json_loads(row["metadata_json"], default={})),
            created_at=float(row["created_at"]),
        )
