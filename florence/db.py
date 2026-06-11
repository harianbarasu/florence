"""Postgres pool and schema. The schema is created idempotently on boot."""

from __future__ import annotations

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS households (
  id TEXT PRIMARY KEY,
  name TEXT,
  timezone TEXT NOT NULL,
  primary_chat_id TEXT,
  stopped BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS members (
  id TEXT PRIMARY KEY,
  household_id TEXT NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  phone TEXT NOT NULL UNIQUE,
  name TEXT,
  role TEXT NOT NULL DEFAULT 'parent',
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS chats (
  chat_id TEXT PRIMARY KEY,
  household_id TEXT NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  kind TEXT NOT NULL DEFAULT 'unknown',
  created_at TIMESTAMPTZ NOT NULL,
  last_inbound_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS messages (
  id BIGSERIAL PRIMARY KEY,
  household_id TEXT NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  chat_id TEXT NOT NULL,
  external_id TEXT UNIQUE,
  direction TEXT NOT NULL,
  sender_phone TEXT,
  sender_name TEXT,
  body TEXT NOT NULL,
  attachments JSONB,
  created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages (chat_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_messages_household ON messages (household_id, id DESC);

CREATE TABLE IF NOT EXISTS memories (
  id TEXT PRIMARY KEY,
  household_id TEXT NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  content TEXT NOT NULL,
  category TEXT,
  created_by TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  archived_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_memories_household ON memories (household_id, created_at DESC);

CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  household_id TEXT NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  chat_id TEXT,
  kind TEXT NOT NULL DEFAULT 'reminder',
  title TEXT NOT NULL,
  notes TEXT,
  due_at TIMESTAMPTZ NOT NULL,
  recurrence TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  created_by TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  last_fired_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks (status, due_at);
CREATE INDEX IF NOT EXISTS idx_tasks_household ON tasks (household_id, due_at);

CREATE TABLE IF NOT EXISTS gmail_accounts (
  id TEXT PRIMARY KEY,
  household_id TEXT NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  member_phone TEXT,
  email TEXT NOT NULL,
  google_sub TEXT NOT NULL,
  token_ciphertext TEXT NOT NULL,
  scopes TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  last_synced_at TIMESTAMPTZ,
  failure_count INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (household_id, google_sub)
);

CREATE TABLE IF NOT EXISTS seen_emails (
  account_id TEXT NOT NULL REFERENCES gmail_accounts(id) ON DELETE CASCADE,
  gmail_id TEXT NOT NULL,
  PRIMARY KEY (account_id, gmail_id)
);

CREATE TABLE IF NOT EXISTS oauth_states (
  state TEXT PRIMARY KEY,
  household_id TEXT NOT NULL,
  member_phone TEXT,
  chat_id TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  used_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL PRIMARY KEY,
  household_id TEXT,
  kind TEXT NOT NULL,
  payload JSONB,
  created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_recent ON events (id DESC);
"""


async def open_pool(database_url: str, schema: str) -> AsyncConnectionPool:
    async def _configure(conn) -> None:
        await conn.execute(f'SET search_path TO "{schema}"')

    pool = AsyncConnectionPool(
        conninfo=database_url,
        min_size=1,
        max_size=8,
        open=False,
        configure=_configure,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open(wait=True, timeout=30)
    async with pool.connection() as conn:
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        await conn.execute(f'SET search_path TO "{schema}"')
        await conn.execute(SCHEMA_SQL)
    return pool
