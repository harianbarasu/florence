import os
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from florence.config import Settings
from florence.models import IncomingMessage, MemoryKind
from florence.service import FlorenceService
from florence.store import DatabaseSchemaError, Store, _postgres_sql


def test_postgres_sql_translates_sqlite_placeholders_and_insert_ignore():
    sql = "INSERT OR IGNORE INTO messages (id, body) VALUES (?, ?)"

    assert _postgres_sql(sql) == (
        "INSERT INTO messages (id, body) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    )


def test_postgres_sql_translates_reminder_epoch_ordering():
    sql = "ORDER BY ABS(strftime('%s', due_at_utc) - strftime('%s', ?)) ASC LIMIT ?"

    assert _postgres_sql(sql) == (
        "ORDER BY ABS(EXTRACT(EPOCH FROM due_at_utc::timestamptz) - "
        "EXTRACT(EPOCH FROM %s::timestamptz)) ASC LIMIT %s"
    )


def test_postgres_sql_keeps_memory_subject_comparison_parameterized():
    sql = (
        "AND COALESCE(subject, '__florence_null_subject__') "
        "= COALESCE(?, '__florence_null_subject__') AND text = ?"
    )

    assert _postgres_sql(sql) == (
        "AND COALESCE(subject, '__florence_null_subject__') "
        "= COALESCE(%s, '__florence_null_subject__') AND text = %s"
    )


def test_store_memory_unique_index_coalesces_null_subjects(tmp_path):
    store = Store(str(tmp_path / "florence.sqlite"))
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household = store.get_or_create_household(
        chat_id="memory-null-subject-chat",
        timezone_name="America/Los_Angeles",
        now_utc=now,
    )

    first = store.upsert_memory(
        household_id=household.id,
        kind=MemoryKind.PREFERENCE,
        text="Maya likes pasta.",
        subject=None,
        now_utc=now,
        confidence=0.6,
    )
    second = store.upsert_memory(
        household_id=household.id,
        kind=MemoryKind.PREFERENCE,
        text="Maya likes pasta.",
        subject=None,
        now_utc=now + timedelta(minutes=1),
        confidence=0.9,
    )
    with store.connect() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO memories
                (id, household_id, kind, subject, text, confidence, created_at_utc, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "direct-duplicate-memory",
                household.id,
                MemoryKind.PREFERENCE.value,
                None,
                "Maya likes pasta.",
                0.1,
                now.isoformat(),
                now.isoformat(),
            ),
        )
        duplicate_count = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM memories
            WHERE household_id = ?
              AND kind = ?
              AND COALESCE(subject, '__florence_null_subject__')
                  = COALESCE(?, '__florence_null_subject__')
              AND text = ?
            """,
            (household.id, MemoryKind.PREFERENCE.value, None, "Maya likes pasta."),
        ).fetchone()["count"]
        indexes = conn.execute("PRAGMA index_list(memories)").fetchall()
    memories = store.list_memories(household_id=household.id, now_utc=now + timedelta(minutes=1))

    assert first.id == second.id
    assert duplicate_count == 1
    assert [memory.text for memory in memories] == ["Maya likes pasta."]
    assert memories[0].confidence == 0.9
    assert "idx_memories_unique_subject_coalesced" in {row["name"] for row in indexes}


def test_store_rejects_reused_incompatible_schema(tmp_path):
    db_path = tmp_path / "legacy-florence.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE messages (
                id TEXT PRIMARY KEY,
                family_id TEXT NOT NULL,
                member_id TEXT,
                channel TEXT NOT NULL,
                body TEXT NOT NULL,
                occurred_at TEXT NOT NULL
            )
            """
        )

    with pytest.raises(DatabaseSchemaError) as exc_info:
        Store(str(db_path))

    error = str(exc_info.value)
    assert "Florence database schema is incompatible with this build" in error
    assert "messages missing columns" in error
    assert "household_id" in error
    assert "chat_id" in error


def test_postgres_sql_translates_data_deletion_request_window_lookup():
    sql = (
        "SELECT body FROM messages WHERE household_id = ? AND actor_member_id = ? "
        "AND direction = ? AND created_at_utc >= ? AND created_at_utc <= ? LIMIT ?"
    )

    assert _postgres_sql(sql) == (
        "SELECT body FROM messages WHERE household_id = %s AND actor_member_id = %s "
        "AND direction = %s AND created_at_utc >= %s AND created_at_utc <= %s LIMIT %s"
    )


def test_postgres_sql_translates_dynamic_in_clause_for_deletion():
    sql = "DELETE FROM oauth_states WHERE chat_id IN (?,?,?)"

    assert _postgres_sql(sql) == "DELETE FROM oauth_states WHERE chat_id IN (%s,%s,%s)"


def test_postgres_sql_keeps_source_retry_concat_and_translates_params():
    sql = """
        SELECT outbound_deliveries.*
        FROM outbound_deliveries
        JOIN source_items
          ON source_items.household_id = outbound_deliveries.household_id
         AND outbound_deliveries.source_message_id = ('source:' || source_items.id)
        WHERE outbound_deliveries.source_message_id LIKE ?
          AND outbound_deliveries.delivery_status IN (?, ?)
        LIMIT ?
    """

    converted = _postgres_sql(sql)

    assert "('source:' || source_items.id)" in converted
    assert "outbound_deliveries.source_message_id LIKE %s" in converted
    assert "outbound_deliveries.delivery_status IN (%s, %s)" in converted
    assert "LIMIT %s" in converted


def test_postgres_sql_preserves_existing_on_conflict_upsert():
    sql = """
        INSERT INTO connected_accounts
            (id, household_id, provider, external_account_id, status)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(household_id, provider, external_account_id)
        DO UPDATE SET status = ?
    """

    converted = _postgres_sql(sql)

    assert "VALUES (%s, %s, %s, %s, %s)" in converted
    assert "ON CONFLICT(household_id, provider, external_account_id)" in converted
    assert "DO UPDATE SET status = %s" in converted


def test_postgres_sql_translates_source_briefing_dynamic_ids():
    sql = """
        UPDATE source_items
        SET briefed_at_utc = ?
        WHERE household_id = ?
          AND id IN (?,?)
          AND briefed_at_utc IS NULL
    """

    converted = _postgres_sql(sql)

    assert "SET briefed_at_utc = %s" in converted
    assert "WHERE household_id = %s" in converted
    assert "AND id IN (%s,%s)" in converted


def test_real_postgres_household_memory_and_approval_isolation():
    dsn = os.getenv("FLORENCE_POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("set FLORENCE_POSTGRES_TEST_DSN to run the live Postgres store smoke")

    settings = Settings(database_url=dsn)
    service = FlorenceService(settings=settings)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    suffix = uuid.uuid4().hex
    chat_a = f"pg-family-a-{suffix}"
    chat_b = f"pg-family-b-{suffix}"
    cleanup_chat_ids = [chat_a, chat_b]

    def send(chat_id: str, message_id: str, text: str, sender: str) -> None:
        service.handle_incoming(
            IncomingMessage(
                chat_id=chat_id,
                message_id=message_id,
                sender=sender,
                text=text,
                received_at=now,
            ),
            now_utc=now,
        )

    try:
        send(chat_a, f"{suffix}-a-name", "my name is Sam", "+15555550100")
        send(chat_a, f"{suffix}-a-partner", "confirm partner +15555550101", "+15555550100")
        send(chat_a, f"{suffix}-a-partner-name", "my name is Alex", "+15555550101")
        send(chat_a, f"{suffix}-a-child", "our child is Maya", "+15555550100")
        send(chat_a, f"{suffix}-a-memory", "remember that Maya likes pasta", "+15555550100")
        send(
            chat_a,
            f"{suffix}-a-source-rule",
            "always tell me about permission slips",
            "+15555550100",
        )

        send(chat_b, f"{suffix}-b-name", "my name is Jordan", "+15555550200")
        send(chat_b, f"{suffix}-b-memory", "remember that Leo likes rice", "+15555550200")

        memory_a = service.memory_snapshot(chat_id=chat_a, now_utc=now).memories
        memory_b = service.memory_snapshot(chat_id=chat_b, now_utc=now).memories
        assert any("Maya likes pasta" in item.text for item in memory_a)
        assert all("Leo likes rice" not in item.text for item in memory_a)
        assert any("Leo likes rice" in item.text for item in memory_b)
        assert all("Maya likes pasta" not in item.text for item in memory_b)

        service.create_pending_action(
            chat_id=chat_a,
            action_type="create_reminder",
            summary="Add reminder: Pack lunch",
            payload={
                "title": "Pack lunch",
                "due_at_utc": (now + timedelta(days=1)).isoformat(),
            },
            sender="+15555550100",
            now_utc=now,
        )
        action = service.pending_actions(chat_id=chat_a, now_utc=now)[0]
        cross_family_reply = service.handle_incoming(
            IncomingMessage(
                chat_id=chat_b,
                message_id=f"{suffix}-b-approve-cross-family",
                sender="+15555550200",
                text=f"approve {action.id[:8]}",
                received_at=now,
            ),
            now_utc=now,
        )
        assert "I could not find an active approval with that code" in cross_family_reply[0].text
        assert service.pending_actions(chat_id=chat_a, now_utc=now)[0].status.value == "pending"

        same_family_reply = service.handle_incoming(
            IncomingMessage(
                chat_id=chat_a,
                message_id=f"{suffix}-a-approve",
                sender="+15555550100",
                text=f"approve {action.id[:8]}",
                received_at=now,
            ),
            now_utc=now,
        )
        assert "Approved" in same_family_reply[0].text
        assert service.pending_actions(chat_id=chat_a, now_utc=now) == []
    finally:
        for chat_id in cleanup_chat_ids:
            household = service.store.get_household_by_chat(chat_id)
            if household is not None:
                service.store.delete_household(household.id)
