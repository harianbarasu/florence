"""Deterministic source-policy handling for parent DM instructions."""

from __future__ import annotations

import re
from dataclasses import replace
from typing import Any

from florence.contracts import (
    CandidateState,
    GoogleConnection,
    GoogleSourceKind,
    HouseholdSourceVisibility,
)
from florence.messaging.protocol_types import FlorenceProtocolReply
from florence.source_rules import build_account_source_rule
from florence.state import FlorenceStateDB

_CONSUMER_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "icloud.com",
    "me.com",
    "mac.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "live.com",
    "msn.com",
    "aol.com",
}


class FlorenceSourcePolicyProtocol:
    """Persist explicit account-level Google privacy/calendar instructions."""

    def __init__(self, *, store: FlorenceStateDB) -> None:
        self.store = store

    def handle_turn(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        text: str,
    ) -> FlorenceProtocolReply | None:
        normalized = _normalize_policy_text(text)
        if not normalized:
            return None
        email_private = _wants_email_private(normalized)
        calendar_blockers = _wants_calendar_blockers(normalized)
        if not email_private and not calendar_blockers:
            return None

        connections = [
            connection
            for connection in self.store.list_google_connections(
                household_id=household_id,
                member_id=member_id,
            )
            if connection.active
        ]
        matched_connections = [
            connection
            for connection in connections
            if _connection_matches_text(connection, normalized)
        ]
        if not matched_connections and "work account" in normalized:
            matched_connections = [
                connection
                for connection in connections
                if _email_domain(connection.email) not in _CONSUMER_DOMAINS
            ]
        if not matched_connections:
            return None

        private_email_accounts: list[str] = []
        blocker_calendar_accounts: list[str] = []
        for connection in matched_connections:
            if email_private and GoogleSourceKind.GMAIL in connection.connected_scopes:
                self._record_account_rule(
                    household_id=household_id,
                    member_id=member_id,
                    channel_id=channel_id,
                    connection=connection,
                    source_kind=GoogleSourceKind.GMAIL,
                    visibility=HouseholdSourceVisibility.PRIVATE,
                )
                self._apply_existing_candidate_account_policy(
                    connection=connection,
                    source_kind=GoogleSourceKind.GMAIL,
                    visibility=HouseholdSourceVisibility.PRIVATE,
                )
                private_email_accounts.append(connection.email)
            if calendar_blockers and GoogleSourceKind.GOOGLE_CALENDAR in connection.connected_scopes:
                self._record_account_rule(
                    household_id=household_id,
                    member_id=member_id,
                    channel_id=channel_id,
                    connection=connection,
                    source_kind=GoogleSourceKind.GOOGLE_CALENDAR,
                    visibility=HouseholdSourceVisibility.PRIVATE,
                )
                self._set_calendar_blocker_preferences(connection)
                self._suppress_existing_calendar_candidates(connection=connection)
                blocker_calendar_accounts.append(connection.email)

        if not private_email_accounts and not blocker_calendar_accounts:
            return None
        reply_bits = []
        if private_email_accounts:
            reply_bits.append(f"email from {_account_list(private_email_accounts)} private to this Florence thread")
        if blocker_calendar_accounts:
            reply_bits.append(f"calendar events from {_account_list(blocker_calendar_accounts)} as busy blockers only")
        return FlorenceProtocolReply(
            reply_text=f"Got it. I’ll keep {' and '.join(reply_bits)}.",
            reply_metadata={
                "source_policy_kind": "google_account_policy",
                "private_email_accounts": private_email_accounts,
                "blocker_calendar_accounts": blocker_calendar_accounts,
            },
            consumed=True,
        )

    def _record_account_rule(
        self,
        *,
        household_id: str,
        member_id: str,
        channel_id: str,
        connection: GoogleConnection,
        source_kind: GoogleSourceKind,
        visibility: HouseholdSourceVisibility,
    ) -> None:
        rule = build_account_source_rule(
            household_id=household_id,
            source_kind=source_kind,
            email=connection.email,
            visibility=visibility,
            created_by_member_id=member_id,
            label=connection.email,
            metadata={
                "google_connection_id": connection.id,
                "channel_id": channel_id,
                "created_from": "explicit_parent_account_policy",
            },
        )
        if rule is not None:
            self.store.upsert_household_source_rule(rule)

    def _apply_existing_candidate_account_policy(
        self,
        *,
        connection: GoogleConnection,
        source_kind: GoogleSourceKind,
        visibility: HouseholdSourceVisibility,
    ) -> None:
        rule = build_account_source_rule(
            household_id=connection.household_id,
            source_kind=source_kind,
            email=connection.email,
            visibility=visibility,
            label=connection.email,
        )
        for candidate in self.store.list_imported_candidates(
            household_id=connection.household_id,
            member_id=connection.member_id,
            state=CandidateState.PENDING_REVIEW,
        ):
            metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
            if candidate.source_kind != source_kind:
                continue
            if not _candidate_from_connection(candidate_metadata=metadata, connection=connection):
                continue
            metadata["source_visibility"] = visibility.value
            if rule is not None:
                metadata["source_rule_id"] = rule.id
                metadata["source_rule_label"] = rule.label
            self.store.upsert_imported_candidate(replace(candidate, metadata=metadata))

    def _set_calendar_blocker_preferences(self, connection: GoogleConnection) -> None:
        metadata = dict(connection.metadata) if isinstance(connection.metadata, dict) else {}
        preferences = dict(metadata.get("calendar_preferences") or {})
        calendar_ids = _calendar_ids_for_connection(metadata)
        for calendar_id in calendar_ids:
            preferences[calendar_id] = {
                "usage_mode": "conflicts_only",
                "detail_visibility": "busy_only",
            }
        if preferences:
            metadata["calendar_preferences"] = preferences
            self.store.upsert_google_connection(replace(connection, metadata=metadata))

    def _suppress_existing_calendar_candidates(self, *, connection: GoogleConnection) -> None:
        for candidate in self.store.list_imported_candidates(
            household_id=connection.household_id,
            member_id=connection.member_id,
            state=CandidateState.PENDING_REVIEW,
        ):
            metadata = dict(candidate.metadata) if isinstance(candidate.metadata, dict) else {}
            if candidate.source_kind != GoogleSourceKind.GOOGLE_CALENDAR:
                continue
            if not _candidate_from_connection(candidate_metadata=metadata, connection=connection):
                continue
            metadata["source_visibility"] = HouseholdSourceVisibility.PRIVATE.value
            metadata["suppressed_reason"] = "calendar_account_conflicts_only"
            self.store.upsert_imported_candidate(
                replace(candidate, state=CandidateState.REJECTED, metadata=metadata)
            )


def _normalize_policy_text(text: str | None) -> str:
    normalized = str(text or "").lower().replace("’", "'")
    normalized = normalized.replace("don't", "dont")
    normalized = re.sub(r"[^a-z0-9@._+\-\s]", " ", normalized)
    return " ".join(normalized.split()).strip()


def _wants_email_private(text: str) -> bool:
    return (
        ("email" in text or "mail" in text or "inbox" in text or "account" in text)
        and ("private" in text or "dont share" in text or "do not share" in text)
    )


def _wants_calendar_blockers(text: str) -> bool:
    if "calendar" not in text:
        return False
    return any(
        phrase in text
        for phrase in (
            "blocker",
            "blockers",
            "busy only",
            "busy blocker",
            "conflict",
            "conflicts only",
            "no details",
            "no need to share",
            "dont share",
            "do not share",
        )
    )


def _email_domain(email: str | None) -> str:
    cleaned = str(email or "").strip().lower()
    return cleaned.split("@", 1)[1] if "@" in cleaned else ""


def _domain_tokens(email: str | None) -> set[str]:
    domain = _email_domain(email)
    if not domain:
        return set()
    base = domain.rsplit(".", 1)[0]
    tokens = {domain, base}
    tokens.update(part for part in re.split(r"[^a-z0-9]+", base) if part)
    return {token for token in tokens if token and len(token) >= 3}


def _connection_matches_text(connection: GoogleConnection, text: str) -> bool:
    email = str(connection.email or "").strip().lower()
    if email and email in text:
        return True
    return any(token in text for token in _domain_tokens(email))


def _calendar_ids_for_connection(metadata: dict[str, Any]) -> list[str]:
    managed_ids = {
        str(item).strip()
        for item in list(metadata.get("florence_managed_calendar_ids") or [])
        if str(item).strip()
    }
    calendar_ids: list[str] = []
    raw_catalog = metadata.get("available_calendars")
    if isinstance(raw_catalog, list):
        for raw_item in raw_catalog:
            if not isinstance(raw_item, dict):
                continue
            calendar_id = str(raw_item.get("id") or "").strip()
            if calendar_id and calendar_id not in managed_ids:
                calendar_ids.append(calendar_id)
    if not calendar_ids:
        primary_calendar_id = str(metadata.get("primary_calendar_id") or "").strip()
        if primary_calendar_id and primary_calendar_id not in managed_ids:
            calendar_ids.append(primary_calendar_id)
    return calendar_ids


def _candidate_from_connection(*, candidate_metadata: dict[str, Any], connection: GoogleConnection) -> bool:
    if str(candidate_metadata.get("google_connection_id") or "").strip() == connection.id:
        return True
    connected_email = str(candidate_metadata.get("connected_email") or "").strip().lower()
    return bool(connected_email and connected_email == str(connection.email or "").strip().lower())


def _account_list(accounts: list[str]) -> str:
    cleaned = [" ".join(account.split()).strip() for account in accounts if " ".join(account.split()).strip()]
    if len(cleaned) <= 1:
        return cleaned[0] if cleaned else "that account"
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"
