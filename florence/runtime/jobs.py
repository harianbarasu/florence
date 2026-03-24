"""Florence worker entrypoints."""

from __future__ import annotations

from datetime import datetime

from florence.runtime.services import FlorenceGoogleSyncPersistenceService, FlorenceGoogleSyncWorkerService
from florence.state import FlorenceStateDB


def run_florence_google_sync(
    *,
    store: FlorenceStateDB,
    household_id: str | None = None,
    connection_id: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
    now: datetime | None = None,
) -> object:
    """Run one Florence Google sync pass for a connection or a household."""
    worker = FlorenceGoogleSyncWorkerService(
        store,
        FlorenceGoogleSyncPersistenceService(store),
    )
    if connection_id is not None:
        return worker.sync_connection(
            connection_id,
            now=now,
            client_id=client_id,
            client_secret=client_secret,
        )
    if household_id is None:
        raise ValueError("household_id_or_connection_id_required")
    return worker.sync_household(
        household_id=household_id,
        now=now,
        client_id=client_id,
        client_secret=client_secret,
    )
