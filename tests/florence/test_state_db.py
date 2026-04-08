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
    def __init__(self, *, fail: bool = False, error: Exception | None = None):
        self.fail = fail
        self.error = error
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, params: tuple[object, ...] = ()):
        self.executed.append((query, params))
        if self.error is not None:
            raise self.error
        if self.fail:
            raise RuntimeError("boom")


class _FakePsycopgConnection:
    def __init__(
        self,
        *,
        fail: bool = False,
        cursor_error: Exception | None = None,
        execute_error: Exception | None = None,
        closed: bool = False,
    ):
        self.fail = fail
        self.cursor_error = cursor_error
        self.execute_error = execute_error
        self.closed = closed
        self.close_calls = 0
        self.rollback_calls = 0
        self.cursor_calls = 0
        self.last_cursor: _FakePsycopgCursor | None = None

    def cursor(self):
        self.cursor_calls += 1
        if self.cursor_error is not None:
            raise self.cursor_error
        self.last_cursor = _FakePsycopgCursor(fail=self.fail, error=self.execute_error)
        return self.last_cursor

    def rollback(self):
        self.rollback_calls += 1

    def commit(self):
        return None

    def close(self):
        self.close_calls += 1
        self.closed = True
        return None


class _FakePsycopgOperationalError(RuntimeError):
    pass


class _FakePsycopgModule:
    OperationalError = _FakePsycopgOperationalError

    def __init__(self, *connections: _FakePsycopgConnection):
        self.connections = list(connections)
        self.connect_calls: list[tuple[str, object]] = []

    def connect(self, dsn: str, *, row_factory: object):
        self.connect_calls.append((dsn, row_factory))
        if not self.connections:
            raise AssertionError("unexpected_connect_call")
        return self.connections.pop(0)


def test_postgres_execute_rolls_back_after_statement_failure():
    adapter = PostgresConnectionAdapter.__new__(PostgresConnectionAdapter)
    fake = _FakePsycopgConnection(fail=True)
    adapter._conn = fake

    with pytest.raises(RuntimeError, match="boom"):
        adapter.execute("SELECT * FROM households WHERE id = ?", ("hh_123",))

    assert fake.rollback_calls == 1
    assert fake.last_cursor is not None
    assert fake.last_cursor.executed == [("SELECT * FROM households WHERE id = %s", ("hh_123",))]


def test_postgres_execute_reconnects_after_closed_connection_error(monkeypatch):
    stale = _FakePsycopgConnection(
        cursor_error=_FakePsycopgOperationalError("the connection is closed"),
        closed=True,
    )
    fresh = _FakePsycopgConnection()
    fake_psycopg = _FakePsycopgModule(fresh)
    dict_row_sentinel = object()
    monkeypatch.setattr("florence.state.db.psycopg", fake_psycopg)
    monkeypatch.setattr("florence.state.db.dict_row", dict_row_sentinel)

    adapter = PostgresConnectionAdapter.__new__(PostgresConnectionAdapter)
    adapter._dsn = "postgresql://example/florence"
    adapter._conn = stale

    row_cursor = adapter.execute("SELECT * FROM households WHERE id = ?", ("hh_123",))

    assert row_cursor is not None
    assert stale.close_calls == 1
    assert fresh.cursor_calls == 1
    assert fresh.last_cursor is not None
    assert fresh.last_cursor.executed == [("SELECT * FROM households WHERE id = %s", ("hh_123",))]
    assert fake_psycopg.connect_calls == [("postgresql://example/florence", dict_row_sentinel)]
