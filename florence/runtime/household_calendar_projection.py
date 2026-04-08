"""Google Calendar projection for shared Florence household events."""

from __future__ import annotations

import logging
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

from florence.contracts import GoogleConnection, HouseholdEventStatus, MemberRole
from florence.google import refresh_google_access_token
from florence.google.fetch import (
    add_google_calendar_to_calendar_list,
    build_google_calendar_web_url,
    create_google_calendar,
    delete_google_calendar_event,
    share_google_calendar_with_user,
    upsert_google_calendar_event,
)
from florence.state import FlorenceStateDB

HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY = "shared_google_calendar_projection"
HOUSEHOLD_CALENDAR_PROJECTION_EVENT_ID_KEY = "shared_google_calendar_event_id"
HOUSEHOLD_CALENDAR_MANAGED_IDS_METADATA_KEY = "florence_managed_calendar_ids"

logger = logging.getLogger(__name__)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _configured_google_client_id() -> str | None:
    raw = (
        os.getenv("FLORENCE_GOOGLE_CLIENT_ID", "").strip()
        or os.getenv("GOOGLE_CLIENT_ID", "").strip()
    )
    return raw or None


def _configured_google_client_secret() -> str | None:
    raw = (
        os.getenv("FLORENCE_GOOGLE_CLIENT_SECRET", "").strip()
        or os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
    )
    return raw or None


class FlorenceHouseholdCalendarProjectionService:
    """Project shared confirmed Florence events into one household Google Calendar."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> None:
        self.store = store
        self.client_id = client_id or _configured_google_client_id()
        self.client_secret = client_secret or _configured_google_client_secret()

    def get_projection_config(self, *, household_id: str) -> dict[str, Any] | None:
        household = self.store.get_household(household_id)
        if household is None:
            return None
        raw = household.settings.get(HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY)
        return dict(raw) if isinstance(raw, dict) else None

    def ensure_projection(
        self,
        *,
        household_id: str,
        preferred_connection_id: str | None = None,
    ) -> dict[str, Any] | None:
        household = self.store.get_household(household_id)
        if household is None:
            return None
        existing = self.get_projection_config(household_id=household_id)
        if existing and str(existing.get("calendar_id") or "").strip():
            return self._ensure_projection_access(
                household_id=household_id,
                config=existing,
            )

        connection = self._resolve_host_connection(
            household_id=household_id,
            preferred_connection_id=preferred_connection_id,
        )
        if connection is None:
            return None
        connection = self._ensure_fresh_access_token(connection)
        if not connection.access_token:
            return None

        calendar_summary = f"Florence - {household.name}"
        created = create_google_calendar(
            access_token=connection.access_token,
            summary=calendar_summary,
            timezone=household.timezone,
        )
        config = {
            "host_connection_id": connection.id,
            "host_email": connection.email,
            "calendar_id": created.id,
            "calendar_summary": created.summary,
            "calendar_web_url": build_google_calendar_web_url(calendar_id=created.id),
            "timezone": created.timezone,
            "status": "active",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "shared_with_connection_ids": [],
            "shared_with_emails": [],
        }
        settings = dict(household.settings)
        settings[HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY] = config
        self.store.upsert_household(replace(household, settings=settings))

        self._mark_connection_managed_calendar(
            connection=connection,
            calendar_id=created.id,
            is_host=True,
        )
        return self._ensure_projection_access(
            household_id=household_id,
            config=config,
        )

    def sync_household(
        self,
        *,
        household_id: str,
        preferred_connection_id: str | None = None,
    ) -> dict[str, Any] | None:
        config = self.ensure_projection(
            household_id=household_id,
            preferred_connection_id=preferred_connection_id,
        )
        if config is None:
            return None
        household = self.store.get_household(household_id)
        if household is None:
            return None
        connection_id = str(config.get("host_connection_id") or "").strip()
        if not connection_id:
            return None
        connection = self.store.get_google_connection(connection_id)
        if connection is None:
            return None
        connection = self._ensure_fresh_access_token(connection)
        if not connection.access_token:
            return None

        calendar_id = str(config.get("calendar_id") or "").strip()
        if not calendar_id:
            return None

        projected_count = 0
        for event in self.store.list_household_events(household_id=household_id):
            metadata = dict(event.metadata)
            projected_event_id = str(metadata.get(HOUSEHOLD_CALENDAR_PROJECTION_EVENT_ID_KEY) or "").strip() or None
            should_project = (
                event.status == HouseholdEventStatus.CONFIRMED
                and bool(event.starts_at)
                and bool(event.ends_at)
            )
            if should_project:
                payload = upsert_google_calendar_event(
                    access_token=connection.access_token,
                    calendar_id=calendar_id,
                    event=event,
                    google_event_id=projected_event_id,
                )
                metadata[HOUSEHOLD_CALENDAR_PROJECTION_EVENT_ID_KEY] = str(payload.get("id") or projected_event_id or "")
                metadata["shared_google_calendar_synced_at"] = datetime.now(timezone.utc).isoformat()
                self.store.upsert_household_event(replace(event, metadata=metadata))
                projected_count += 1
                continue
            if projected_event_id:
                delete_google_calendar_event(
                    access_token=connection.access_token,
                    calendar_id=calendar_id,
                    google_event_id=projected_event_id,
                )
                metadata.pop(HOUSEHOLD_CALENDAR_PROJECTION_EVENT_ID_KEY, None)
                metadata["shared_google_calendar_synced_at"] = datetime.now(timezone.utc).isoformat()
                self.store.upsert_household_event(replace(event, metadata=metadata))

        settings = dict(household.settings)
        updated_config = dict(config)
        updated_config["last_synced_at"] = datetime.now(timezone.utc).isoformat()
        updated_config["last_projected_event_count"] = projected_count
        settings[HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY] = updated_config
        self.store.upsert_household(replace(household, settings=settings))
        return updated_config

    def _ensure_projection_access(
        self,
        *,
        household_id: str,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        calendar_id = str(config.get("calendar_id") or "").strip()
        host_connection_id = str(config.get("host_connection_id") or "").strip()
        if not calendar_id or not host_connection_id:
            return config

        host_connection = self.store.get_google_connection(host_connection_id)
        if host_connection is None:
            return config
        host_connection = self._ensure_fresh_access_token(host_connection)
        self._mark_connection_managed_calendar(
            connection=host_connection,
            calendar_id=calendar_id,
            is_host=True,
        )

        shared_with_connection_ids = {
            str(item).strip()
            for item in (
                config.get("shared_with_connection_ids")
                if isinstance(config.get("shared_with_connection_ids"), list)
                else []
            )
            if str(item).strip()
        }
        shared_with_emails = {
            str(item).strip()
            for item in (
                config.get("shared_with_emails")
                if isinstance(config.get("shared_with_emails"), list)
                else []
            )
            if str(item).strip()
        }

        if host_connection.access_token:
            for viewer in self._resolve_viewer_connections(
                household_id=household_id,
                host_connection_id=host_connection.id,
            ):
                viewer_email = viewer.email.strip()
                if not viewer_email:
                    continue
                try:
                    share_google_calendar_with_user(
                        access_token=host_connection.access_token,
                        calendar_id=calendar_id,
                        user_email=viewer_email,
                        role="reader",
                        send_notifications=False,
                    )
                    viewer = self._ensure_fresh_access_token(viewer)
                    if viewer.access_token:
                        add_google_calendar_to_calendar_list(
                            access_token=viewer.access_token,
                            calendar_id=calendar_id,
                            selected=True,
                        )
                    self._mark_connection_managed_calendar(
                        connection=viewer,
                        calendar_id=calendar_id,
                        is_host=False,
                    )
                    shared_with_connection_ids.add(viewer.id)
                    shared_with_emails.add(viewer_email)
                except Exception:
                    logger.exception(
                        "Florence household calendar projection share failed household_id=%s calendar_id=%s viewer_connection_id=%s",
                        household_id,
                        calendar_id,
                        viewer.id,
                    )

        updated_config = dict(config)
        updated_config["host_email"] = host_connection.email
        updated_config["calendar_web_url"] = build_google_calendar_web_url(calendar_id=calendar_id)
        updated_config["shared_with_connection_ids"] = sorted(shared_with_connection_ids)
        updated_config["shared_with_emails"] = sorted(shared_with_emails)
        updated_config["last_shared_at"] = datetime.now(timezone.utc).isoformat()

        household = self.store.get_household(household_id)
        if household is not None:
            settings = dict(household.settings)
            settings[HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY] = updated_config
            self.store.upsert_household(replace(household, settings=settings))
        return updated_config

    def _resolve_host_connection(
        self,
        *,
        household_id: str,
        preferred_connection_id: str | None = None,
    ) -> GoogleConnection | None:
        if preferred_connection_id:
            connection = self.store.get_google_connection(preferred_connection_id)
            if connection is not None and connection.household_id == household_id and connection.active:
                return connection
        connections = [
            connection
            for connection in self.store.list_google_connections(household_id=household_id)
            if connection.active
        ]
        return connections[0] if connections else None

    def _resolve_viewer_connections(
        self,
        *,
        household_id: str,
        host_connection_id: str,
    ) -> list[GoogleConnection]:
        viewers: list[GoogleConnection] = []
        seen_emails: set[str] = set()
        for connection in self.store.list_google_connections(household_id=household_id):
            if not connection.active or connection.id == host_connection_id:
                continue
            if not connection.email.strip():
                continue
            member = self.store.get_member(connection.member_id)
            if member is not None and member.role not in {MemberRole.ADMIN, MemberRole.PARENT}:
                continue
            normalized_email = connection.email.strip().lower()
            if normalized_email in seen_emails:
                continue
            seen_emails.add(normalized_email)
            viewers.append(connection)
        return viewers

    def _mark_connection_managed_calendar(
        self,
        *,
        connection: GoogleConnection,
        calendar_id: str,
        is_host: bool,
    ) -> GoogleConnection:
        metadata = dict(connection.metadata)
        managed_ids = {
            str(item).strip()
            for item in (
                metadata.get(HOUSEHOLD_CALENDAR_MANAGED_IDS_METADATA_KEY)
                if isinstance(metadata.get(HOUSEHOLD_CALENDAR_MANAGED_IDS_METADATA_KEY), list)
                else []
            )
            if str(item).strip()
        }
        legacy_projection_id = str(metadata.get("florence_projection_calendar_id") or "").strip()
        if legacy_projection_id:
            managed_ids.add(legacy_projection_id)
        managed_ids.add(calendar_id)
        metadata[HOUSEHOLD_CALENDAR_MANAGED_IDS_METADATA_KEY] = sorted(managed_ids)
        if is_host:
            metadata["florence_projection_calendar_id"] = calendar_id
        updated = replace(connection, metadata=metadata)
        self.store.upsert_google_connection(updated)
        return updated

    def _ensure_fresh_access_token(self, connection: GoogleConnection) -> GoogleConnection:
        expiry = _parse_iso_datetime(connection.access_token_expires_at)
        refresh_needed = connection.access_token is None or (
            expiry is not None and expiry <= datetime.now(timezone.utc) + timedelta(minutes=5)
        )
        if not refresh_needed:
            return connection
        if not connection.refresh_token or not self.client_id or not self.client_secret:
            return connection
        refreshed = refresh_google_access_token(
            refresh_token=connection.refresh_token,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        updated = replace(
            connection,
            access_token=refreshed.access_token or connection.access_token,
            refresh_token=refreshed.refresh_token or connection.refresh_token,
            access_token_expires_at=(
                datetime.now(timezone.utc) + timedelta(seconds=refreshed.expires_in or 3600)
            ).isoformat(),
        )
        self.store.upsert_google_connection(updated)
        return updated
