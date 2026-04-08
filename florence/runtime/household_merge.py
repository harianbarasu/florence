"""Deterministic household merge helpers for Florence identity resolution."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import Any

from florence.contracts import ChannelType, Household, Member, MemberRole
from florence.google import merge_google_grounding_hints
from florence.google.fetch import delete_google_calendar
from florence.runtime.household_calendar_projection import (
    HOUSEHOLD_CALENDAR_MANAGED_IDS_METADATA_KEY,
    HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY,
    FlorenceHouseholdCalendarProjectionService,
)
from florence.state import FlorenceStateDB

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _ProjectionMergeDecision:
    kept_config: dict[str, Any] | None
    retired_config: dict[str, Any] | None = None


class FlorenceHouseholdMergeService:
    """Merge deterministic duplicate households once a shared group proves they are one family."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        household_calendar_projection_service: FlorenceHouseholdCalendarProjectionService | None = None,
    ) -> None:
        self.store = store
        self.household_calendar_projection_service = household_calendar_projection_service

    def merge_group_households(
        self,
        *,
        target_household_id: str,
        matching_households: dict[str, list[Member]],
    ) -> str | None:
        source_household_ids = [
            household_id
            for household_id in matching_households
            if household_id != target_household_id
        ]
        if not source_household_ids:
            return target_household_id
        if any(
            not self._is_mergeable_source_household(
                household_id=household_id,
                matched_members=matching_households.get(household_id, []),
            )
            for household_id in source_household_ids
        ):
            return None

        for source_household_id in source_household_ids:
            self._merge_one_into_target(
                target_household_id=target_household_id,
                source_household_id=source_household_id,
            )
        return target_household_id

    def merge_households(
        self,
        *,
        target_household_id: str,
        source_household_id: str,
    ) -> str:
        """Explicitly merge one known household into another."""
        if target_household_id == source_household_id:
            return target_household_id
        self._merge_one_into_target(
            target_household_id=target_household_id,
            source_household_id=source_household_id,
        )
        return target_household_id

    def _is_mergeable_source_household(
        self,
        *,
        household_id: str,
        matched_members: list[Member],
    ) -> bool:
        if self.store.get_household(household_id) is None:
            return False
        if self.store.list_channels(
            household_id=household_id,
            channel_type=ChannelType.HOUSEHOLD_GROUP,
        ):
            return False

        active_parent_members = [
            member
            for member in self.store.list_members(household_id)
            if member.status == "active" and member.role in {MemberRole.ADMIN, MemberRole.PARENT}
        ]
        if len(active_parent_members) != 1:
            return False

        matched_parent_ids = {
            member.id
            for member in matched_members
            if member.role in {MemberRole.ADMIN, MemberRole.PARENT}
        }
        if len(matched_parent_ids) != 1:
            return False
        return active_parent_members[0].id in matched_parent_ids

    def _merge_one_into_target(
        self,
        *,
        target_household_id: str,
        source_household_id: str,
    ) -> None:
        target_household = self.store.get_household(target_household_id)
        source_household = self.store.get_household(source_household_id)
        if target_household is None or source_household is None:
            raise ValueError("household_missing_for_merge")

        source_connections = self.store.list_google_connections(household_id=source_household_id)
        projection_decision = self._projection_merge_decision(
            target_household=target_household,
            source_household=source_household,
        )
        merged_settings = self._merged_household_settings(
            target_household=target_household,
            source_household=source_household,
            kept_projection_config=projection_decision.kept_config,
        )

        self.store.merge_households(
            target_household_id=target_household_id,
            source_household_id=source_household_id,
            merged_settings=merged_settings,
        )

        retired_calendar_id = self._retired_calendar_id(projection_decision.retired_config)
        if retired_calendar_id:
            for connection in source_connections:
                self._remember_managed_calendar_id(
                    connection_id=connection.id,
                    calendar_id=retired_calendar_id,
                    clear_legacy_projection_id=True,
                )

        self._retire_source_projection_calendar(projection_decision.retired_config)

        if self.household_calendar_projection_service is not None and projection_decision.kept_config is not None:
            self.household_calendar_projection_service.ensure_projection(
                household_id=target_household_id,
                preferred_connection_id=str(projection_decision.kept_config.get("host_connection_id") or "").strip() or None,
            )

    def _merged_household_settings(
        self,
        *,
        target_household: Household,
        source_household: Household,
        kept_projection_config: dict[str, Any] | None,
    ) -> dict[str, object]:
        merged = dict(source_household.settings)
        merged.update(target_household.settings)

        merged["grounding_hints"] = merge_google_grounding_hints(
            source_household.settings.get("grounding_hints")
            if isinstance(source_household.settings.get("grounding_hints"), dict)
            else None,
            target_household.settings.get("grounding_hints")
            if isinstance(target_household.settings.get("grounding_hints"), dict)
            else {},
        )

        if kept_projection_config is not None:
            merged[HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY] = kept_projection_config
        else:
            merged.pop(HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY, None)
        return merged

    def _projection_merge_decision(
        self,
        *,
        target_household: Household,
        source_household: Household,
    ) -> _ProjectionMergeDecision:
        target_config = self._projection_config_from_household(target_household)
        source_config = self._projection_config_from_household(source_household)
        if target_config is None:
            return _ProjectionMergeDecision(kept_config=source_config)
        if source_config is None:
            return _ProjectionMergeDecision(kept_config=target_config)

        kept = dict(target_config)
        kept["shared_with_connection_ids"] = self._sorted_union_list(
            target_config.get("shared_with_connection_ids"),
            source_config.get("shared_with_connection_ids"),
        )
        kept["shared_with_emails"] = self._sorted_union_list(
            target_config.get("shared_with_emails"),
            source_config.get("shared_with_emails"),
        )
        kept["calendar_link_shared_member_ids"] = self._sorted_union_list(
            target_config.get("calendar_link_shared_member_ids"),
            source_config.get("calendar_link_shared_member_ids"),
        )
        kept.setdefault("calendar_web_url", source_config.get("calendar_web_url"))
        kept.setdefault("host_email", source_config.get("host_email"))
        kept.setdefault("created_at", source_config.get("created_at"))

        target_calendar_id = str(target_config.get("calendar_id") or "").strip()
        source_calendar_id = str(source_config.get("calendar_id") or "").strip()
        retired = None
        if target_calendar_id and source_calendar_id and target_calendar_id != source_calendar_id:
            retired = dict(source_config)
        return _ProjectionMergeDecision(kept_config=kept, retired_config=retired)

    @staticmethod
    def _projection_config_from_household(household: Household) -> dict[str, Any] | None:
        raw = household.settings.get(HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY)
        return dict(raw) if isinstance(raw, dict) else None

    @staticmethod
    def _sorted_union_list(*raw_values: object) -> list[str]:
        merged: set[str] = set()
        for raw in raw_values:
            if not isinstance(raw, list):
                continue
            for item in raw:
                cleaned = str(item).strip()
                if cleaned:
                    merged.add(cleaned)
        return sorted(merged)

    @staticmethod
    def _retired_calendar_id(config: dict[str, Any] | None) -> str | None:
        calendar_id = str((config or {}).get("calendar_id") or "").strip()
        return calendar_id or None

    def _remember_managed_calendar_id(
        self,
        *,
        connection_id: str,
        calendar_id: str,
        clear_legacy_projection_id: bool,
    ) -> None:
        connection = self.store.get_google_connection(connection_id)
        if connection is None:
            return
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
        managed_ids.add(calendar_id)
        metadata[HOUSEHOLD_CALENDAR_MANAGED_IDS_METADATA_KEY] = sorted(managed_ids)
        if clear_legacy_projection_id and str(metadata.get("florence_projection_calendar_id") or "").strip() == calendar_id:
            metadata.pop("florence_projection_calendar_id", None)
        self.store.upsert_google_connection(replace(connection, metadata=metadata))

    def _retire_source_projection_calendar(self, config: dict[str, Any] | None) -> None:
        if config is None or self.household_calendar_projection_service is None:
            return
        calendar_id = str(config.get("calendar_id") or "").strip()
        host_connection_id = str(config.get("host_connection_id") or "").strip()
        if not calendar_id or not host_connection_id:
            return

        connection = self.store.get_google_connection(host_connection_id)
        if connection is None:
            return
        try:
            connection = self.household_calendar_projection_service._ensure_fresh_access_token(connection)
            if not connection.access_token:
                return
            delete_google_calendar(
                access_token=connection.access_token,
                calendar_id=calendar_id,
            )
        except Exception:
            logger.exception(
                "Florence household merge failed to retire source projection calendar household_connection_id=%s calendar_id=%s",
                host_connection_id,
                calendar_id,
            )
