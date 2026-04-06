from pathlib import Path

import pytest

from florence.state.db import (
    PostgresConnectionAdapter,
    SQLiteConnectionAdapter,
    _rewrite_placeholders,
    _split_sql_script,
    connect_florence_db,
)


def test_connect_florence_db_uses_sqlite_adapter_for_path(tmp_path):
    conn = connect_florence_db(tmp_path / "florence.db")
    assert isinstance(conn, SQLiteConnectionAdapter)
    cursor = conn.execute("SELECT 1 AS value")
    row = cursor.fetchone()
    assert row is not None
    assert row["value"] == 1
    conn.close()


def test_rewrite_placeholders_converts_sqlite_style_params():
    assert _rewrite_placeholders("SELECT * FROM households WHERE id = ? AND status = ?") == (
        "SELECT * FROM households WHERE id = %s AND status = %s"
    )


def test_split_sql_script_skips_empty_statements():
    statements = list(_split_sql_script("SELECT 1;  ;\nSELECT 2;"))
    assert statements == ["SELECT 1", "SELECT 2"]


class _FakePsycopgCursor:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, params: tuple[object, ...] = ()):
        self.executed.append((query, params))
        if self.fail:
            raise RuntimeError("boom")


class _FakePsycopgConnection:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.rollback_calls = 0
        self.cursor_calls = 0
        self.last_cursor: _FakePsycopgCursor | None = None

    def cursor(self):
        self.cursor_calls += 1
        self.last_cursor = _FakePsycopgCursor(fail=self.fail)
        return self.last_cursor

    def rollback(self):
        self.rollback_calls += 1

    def commit(self):
        return None

    def close(self):
        return None


def test_postgres_execute_rolls_back_after_statement_failure():
    adapter = PostgresConnectionAdapter.__new__(PostgresConnectionAdapter)
    fake = _FakePsycopgConnection(fail=True)
    adapter._conn = fake

    with pytest.raises(RuntimeError, match="boom"):
        adapter.execute("SELECT * FROM households WHERE id = ?", ("hh_123",))

    assert fake.rollback_calls == 1
    assert fake.last_cursor is not None
    assert fake.last_cursor.executed == [("SELECT * FROM households WHERE id = %s", ("hh_123",))]
