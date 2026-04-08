#!/usr/bin/env python3
"""Reset Florence state for testing or user data deletion."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from florence.contracts import IdentityKind
from florence.runtime.resolver import normalize_identity_value
from florence.state import FlorenceStateDB

@dataclass(slots=True)
class ResetSummary:
    mode: str
    household_id: str | None
    phone: str | None
    normalized_phone: str | None
    deleted_counts: dict[str, int]
    remaining_counts: dict[str, int]


def _augment_postgres_dsn(database_url: str) -> str:
    if not database_url.startswith(("postgres://", "postgresql://")):
        return database_url
    parsed = urlsplit(database_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.setdefault("connect_timeout", "5")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))


def _is_postgres_database(database_url: str) -> bool:
    lowered = database_url.strip().lower()
    return lowered.startswith(("postgres://", "postgresql://"))


def _set_timeouts(store: FlorenceStateDB) -> None:
    if not _is_postgres_database(str(store.database)):
        return
    store._conn.execute("SET lock_timeout = '3s'")
    store._conn.execute("SET statement_timeout = '15s'")
    store._conn.commit()


def _terminate_other_postgres_sessions(store: FlorenceStateDB) -> int:
    if not _is_postgres_database(str(store.database)):
        return 0
    rows = store._conn.execute(
        """
        SELECT pg_terminate_backend(pid) AS terminated
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
        """
    ).fetchall()
    store._conn.commit()
    return sum(1 for row in rows if bool(row.get("terminated")))


def _resolve_household_id_for_phone(store: FlorenceStateDB, phone: str) -> tuple[str | None, str]:
    normalized_phone = normalize_identity_value(IdentityKind.PHONE, phone)
    member = store.find_member_by_identity(kind=IdentityKind.PHONE, normalized_value=normalized_phone)
    if member is None:
        return None, normalized_phone
    return member.household_id, normalized_phone


def wipe_all_data(store: FlorenceStateDB) -> ResetSummary:
    deleted_counts = store.wipe_all_state()
    return ResetSummary(
        mode="all",
        household_id=None,
        phone=None,
        normalized_phone=None,
        deleted_counts=deleted_counts,
        remaining_counts=store.count_all_state_rows(),
    )


def delete_household_by_phone(store: FlorenceStateDB, phone: str) -> ResetSummary:
    household_id, normalized_phone = _resolve_household_id_for_phone(store, phone)
    if household_id is None:
        return ResetSummary(
            mode="phone",
            household_id=None,
            phone=phone,
            normalized_phone=normalized_phone,
            deleted_counts={},
            remaining_counts={},
        )

    deleted_counts = store.delete_household_state(household_id)

    return ResetSummary(
        mode="phone",
        household_id=household_id,
        phone=phone,
        normalized_phone=normalized_phone,
        deleted_counts=deleted_counts,
        remaining_counts=store.count_household_state_rows(household_id),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset Florence state for testing or user deletion requests.")
    parser.add_argument("--database-url", help="Postgres or SQLite database URL/path. Defaults to FLORENCE_DATABASE_URL or DATABASE_URL.")
    parser.add_argument("--phone", help="Delete the Florence household associated with this phone number.")
    parser.add_argument("--all", action="store_true", help="Wipe all Florence app data.")
    parser.add_argument(
        "--terminate-other-sessions",
        action="store_true",
        help="Terminate other Postgres sessions first to avoid lock hangs.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if bool(args.all) == bool(args.phone):
        raise SystemExit("Pass exactly one of --all or --phone.")

    database = args.database_url or ""
    store = FlorenceStateDB(_augment_postgres_dsn(database) if database else None)
    try:
        _set_timeouts(store)
        terminated_sessions = 0
        if args.terminate_other_sessions:
            terminated_sessions = _terminate_other_postgres_sessions(store)
            _set_timeouts(store)
        summary = wipe_all_data(store) if args.all else delete_household_by_phone(store, args.phone)
        payload: dict[str, Any] = {
            "mode": summary.mode,
            "household_id": summary.household_id,
            "phone": summary.phone,
            "normalized_phone": summary.normalized_phone,
            "terminated_other_sessions": terminated_sessions,
            "deleted_counts": summary.deleted_counts,
            "remaining_counts": summary.remaining_counts,
        }
        print(json.dumps(payload, sort_keys=True))
        return 0
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
