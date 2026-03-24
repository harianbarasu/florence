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

    def close(self) -> None:
        self._conn.close()


class PostgresCursorAdapter:
    def __init__(self, cursor: Any):
        self._cursor = cursor

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> "PostgresCursorAdapter":
        self._cursor.execute(_rewrite_placeholders(query), params)
        return self

    def executescript(self, script: str) -> None:
        for statement in _split_sql_script(script):
            self._cursor.execute(statement)

    def fetchone(self) -> RowLike | None:
        return self._cursor.fetchone()

    def fetchall(self) -> list[RowLike]:
        return self._cursor.fetchall()


class PostgresConnectionAdapter:
    def __init__(self, dsn: str):
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg_required_for_postgres")
        self._conn = psycopg.connect(dsn, row_factory=dict_row)

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> PostgresCursorAdapter:
        cursor = self.cursor()
        cursor.execute(query, params)
        return cursor

    def cursor(self) -> PostgresCursorAdapter:
        return PostgresCursorAdapter(self._conn.cursor())

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


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
