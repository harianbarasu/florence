"""Google mirror freshness helpers for Florence trust boundaries."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

GOOGLE_MIRROR_STALE_AFTER_HOURS = 24


def parse_google_sync_datetime(value: object) -> datetime | None:
    normalized = " ".join(str(value or "").split()).strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_google_mirror_connection_freshness(
    *,
    connection: Any,
    metadata: dict[str, Any],
    last_synced_key: str,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    current = now_utc or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)

    raw_last_synced = " ".join(str(metadata.get(last_synced_key) or "").split()).strip() or None
    parsed_last_synced = parse_google_sync_datetime(raw_last_synced)
    initial_sync_state = " ".join(str(metadata.get("initial_sync_state") or "").split()).strip() or None
    last_sync_status = " ".join(str(metadata.get("last_sync_status") or "").split()).strip() or None
    last_sync_error = " ".join(str(metadata.get("last_sync_error") or "").split()).strip() or None
    sync_phase = " ".join(str(metadata.get("sync_phase") or "").split()).strip() or None

    age_hours: float | None = None
    if parsed_last_synced is not None:
        age_hours = max(0.0, (current - parsed_last_synced).total_seconds() / 3600)

    if initial_sync_state == "running" or last_sync_status == "running":
        status = "running"
        fresh_enough = False
        guidance = "Mirror sync is still running. Do not claim missing results mean the source is absent."
    elif last_sync_status == "error" or last_sync_error:
        status = "error"
        fresh_enough = False
        guidance = "The latest mirror sync failed. Use returned matches as historical evidence, not a complete current view."
    elif parsed_last_synced is None:
        status = "never_synced"
        fresh_enough = False
        guidance = "This mirror has not completed a sync yet. Do not claim completeness or absence from this source."
    elif age_hours is not None and age_hours > GOOGLE_MIRROR_STALE_AFTER_HOURS:
        status = "stale"
        fresh_enough = False
        guidance = "The mirror is stale. Use returned matches as evidence, but do not claim this is the latest complete source view."
    else:
        status = "fresh"
        fresh_enough = True
        guidance = "The mirror is fresh enough for ordinary source-backed answers from returned matches."

    return {
        "connection_id": getattr(connection, "id", None),
        "email": getattr(connection, "email", None),
        "status": status,
        "fresh_enough_for_latest_claims": fresh_enough,
        "last_synced_at": raw_last_synced,
        "age_hours": round(age_hours, 2) if age_hours is not None else None,
        "stale_after_hours": GOOGLE_MIRROR_STALE_AFTER_HOURS,
        "initial_sync_state": initial_sync_state,
        "last_sync_status": last_sync_status,
        "last_sync_error": last_sync_error,
        "sync_phase": sync_phase,
        "guidance": guidance,
    }


def build_google_mirror_freshness_summary(
    *,
    connection_freshness: list[dict[str, Any]],
    source_kind: str,
) -> dict[str, Any]:
    statuses = {str(item.get("status") or "") for item in connection_freshness}
    if not connection_freshness:
        status = "no_connections"
        fresh_enough = False
        guidance = f"No active Google {source_kind} connection is available in the current scope."
    elif "running" in statuses:
        status = "running"
        fresh_enough = False
        guidance = "A mirror sync is still running. Do not treat empty results as proof that nothing exists."
    elif "error" in statuses:
        status = "error"
        fresh_enough = False
        guidance = "At least one mirror sync failed. Do not claim complete/latest coverage from these results."
    elif "never_synced" in statuses:
        status = "never_synced"
        fresh_enough = False
        guidance = "At least one mirror has never completed sync. Do not claim complete coverage from these results."
    elif "stale" in statuses:
        status = "stale"
        fresh_enough = False
        guidance = "At least one mirror is stale. Use returned matches as source evidence, but avoid latest/complete claims."
    else:
        status = "fresh"
        fresh_enough = True
        guidance = "All selected mirrors are fresh enough for ordinary source-backed answers from returned matches."
    return {
        "source_kind": source_kind,
        "status": status,
        "fresh_enough_for_latest_claims": fresh_enough,
        "stale_after_hours": GOOGLE_MIRROR_STALE_AFTER_HOURS,
        "connections": connection_freshness,
        "guidance": guidance,
    }
