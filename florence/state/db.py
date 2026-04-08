"""Database adapters for Florence state storage."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Protocol

try:  # pragma: no cover - optional until Postgres is configured
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional dependency path
    psycopg = None
    dict_row = None

RowLike = Mapping[str, Any]


class FlorenceCursor(Protocol):
    def execute(self, query: str, params: tuple[Any, ...] = ()) -> "FlorenceCursor": ...

    def executescript(self, script: str) -> None: ...

    def fetchone(self) -> RowLike | None: ...

    def fetchall(self) -> list[RowLike]: ...


class FlorenceConnection(Protocol):
    def execute(self, query: str, params: tuple[Any, ...] = ()) -> FlorenceCursor: ...

    def cursor(self) -> FlorenceCursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SQLiteCursorAdapter:
    def __init__(self, cursor: sqlite3.Cursor):
        self._cursor = cursor

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> "SQLiteCursorAdapter":
        self._cursor.execute(query, params)
        return self

    def executescript(self, script: str) -> None:
        self._cursor.executescript(script)

    def fetchone(self) -> RowLike | None:
        return self._cursor.fetchone()

    def fetchall(self) -> list[RowLike]:
        return self._cursor.fetchall()


class SQLiteConnectionAdapter:
    def __init__(self, database_path: Path):
        database_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(database_path), check_same_thread=False, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        self._conn = conn

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> SQLiteCursorAdapter:
        return SQLiteCursorAdapter(self._conn.execute(query, params))

    def cursor(self) -> SQLiteCursorAdapter:
        return SQLiteCursorAdapter(self._conn.cursor())

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


class PostgresCursorAdapter:
    def __init__(self, cursor: Any, connection: "PostgresConnectionAdapter"):
        self._cursor = cursor
        self._connection = connection

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> "PostgresCursorAdapter":
        try:
            self._cursor.execute(_rewrite_placeholders(query), params)
        except Exception:
            self._connection._rollback_if_possible()
            raise
        return self

    def executescript(self, script: str) -> None:
        try:
            for statement in _split_sql_script(script):
                self._cursor.execute(statement)
        except Exception:
            self._connection._rollback_if_possible()
            raise

    def fetchone(self) -> RowLike | None:
        return self._cursor.fetchone()

    def fetchall(self) -> list[RowLike]:
        return self._cursor.fetchall()


class PostgresConnectionAdapter:
    def __init__(self, dsn: str):
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg_required_for_postgres")
        self._dsn = dsn
        self._conn = self._open_connection()

    def _open_connection(self) -> Any:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _connection_is_closed(self) -> bool:
        conn = self._conn
        if conn is None:
            return True
        if bool(getattr(conn, "broken", False)):
            return True
        closed = getattr(conn, "closed", False)
        if isinstance(closed, bool):
            return closed
        if isinstance(closed, int):
            return closed != 0
        return False

    def _is_recoverable_connection_error(self, exc: Exception) -> bool:
        operational_error = getattr(psycopg, "OperationalError", None) if psycopg is not None else None
        if operational_error is not None and isinstance(exc, operational_error):
            message = str(exc).lower()
            if (
                "connection is closed" in message
                or "server closed the connection unexpectedly" in message
                or "terminating connection" in message
            ):
                return True
        return self._connection_is_closed()

    def _reconnect(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        self._conn = self._open_connection()

    def _rollback_if_possible(self) -> None:
        try:
            if self._conn is not None and not self._connection_is_closed():
                self._conn.rollback()
        except Exception:
            pass

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> PostgresCursorAdapter:
        try:
            cursor = self.cursor()
            cursor.execute(query, params)
            return cursor
        except Exception as exc:
            if not self._is_recoverable_connection_error(exc):
                raise
        self._reconnect()
        cursor = self.cursor()
        cursor.execute(query, params)
        return cursor

    def cursor(self) -> PostgresCursorAdapter:
        if self._connection_is_closed():
            self._reconnect()
        try:
            return PostgresCursorAdapter(self._conn.cursor(), self)
        except Exception as exc:
            if not self._is_recoverable_connection_error(exc):
                raise
        self._reconnect()
        return PostgresCursorAdapter(self._conn.cursor(), self)

    def commit(self) -> None:
        try:
            self._conn.commit()
        except Exception as exc:
            if self._is_recoverable_connection_error(exc):
                self._reconnect()
            raise

    def rollback(self) -> None:
        try:
            self._conn.rollback()
        except Exception as exc:
            if self._is_recoverable_connection_error(exc):
                self._reconnect()
            raise

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def connect_florence_db(database: Path | str) -> FlorenceConnection:
    if isinstance(database, Path):
        return SQLiteConnectionAdapter(database)

    database_str = str(database).strip()
    if database_str.startswith(("postgres://", "postgresql://")):
        return PostgresConnectionAdapter(database_str)

    return SQLiteConnectionAdapter(Path(database_str).expanduser())


def _rewrite_placeholders(query: str) -> str:
    return query.replace("?", "%s")


def _split_sql_script(script: str) -> Iterable[str]:
    for statement in script.split(";"):
        sql = statement.strip()
        if sql:
            yield sql
