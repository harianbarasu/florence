"""Household linking helpers for connecting multiple parents into one Florence household."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Callable

from florence.contracts import (
    ChannelType,
    ChildProfile,
    HouseholdEventStatus,
    HouseholdLinkRequest,
    HouseholdLinkRequestStatus,
    HouseholdNudge,
    HouseholdNudgeStatus,
    HouseholdNudgeTargetKind,
    HouseholdProfileItem,
    HouseholdProfileKind,
    HouseholdRoutineStatus,
    HouseholdWorkItem,
    HouseholdWorkItemStatus,
    IdentityKind,
    MemberRole,
)
from florence.messaging.protocol_types import build_household_link_prompt_metadata
from florence.runtime.household_merge import FlorenceHouseholdMergeService
from florence.runtime.household_manager import FlorenceHouseholdManagerService
from florence.runtime.resolver import normalize_identity_value
from florence.state import FlorenceStateDB


_MEANINGFUL_HOUSEHOLD_STATE_KEYS = (
    "child_profiles",
    "household_events",
    "household_meals",
    "household_nudges",
    "household_profile_items",
    "household_routines",
    "household_shopping_items",
    "household_source_rules",
    "household_work_items",
)
_MERGE_CLEANUP_WORK_ITEM_KIND = "merge_cleanup"
_INVITING_CONFIRM_NUDGE_KIND = "household_link_confirmation"


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256(":".join(parts).encode("utf-8")).hexdigest()[:20]
    return f"{prefix}_{digest}"


@dataclass(slots=True)
class HouseholdLinkAssessment:
    household_id: str
    active_parent_count: int
    has_group_channel: bool
    meaningful_state_counts: dict[str, int]

    @property
    def is_lightweight(self) -> bool:
        return self.active_parent_count <= 1 and not self.has_group_channel and not self.meaningful_state_counts

    @property
    def is_mature(self) -> bool:
        return not self.is_lightweight


@dataclass(slots=True)
class HouseholdLinkActionResult:
    request: HouseholdLinkRequest
    reply_text: str


@dataclass(slots=True)
class _MergeCleanupSummary:
    count: int
    preview_lines: list[str]


@dataclass(slots=True)
class MergeFollowupResolutionResult:
    work_item: HouseholdWorkItem
    child: ChildProfile | None = None
    remaining_conflicts: list[str] | None = None


class FlorenceHouseholdLinkService:
    """Create and inspect pending parent-link requests without exposing transport/private details."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        household_merge_service: FlorenceHouseholdMergeService | None = None,
        now_getter: Callable[[], datetime] | None = None,
        request_expiry_days: int = 7,
    ) -> None:
        self.store = store
        self.household_merge_service = household_merge_service or FlorenceHouseholdMergeService(store)
        self.now_getter = now_getter or (lambda: datetime.now(timezone.utc))
        self.request_expiry_days = max(1, int(request_expiry_days))

    def create_phone_link_request(
        self,
        *,
        household_id: str,
        inviting_member_id: str,
        invited_phone: str,
        invited_display_name: str | None = None,
    ) -> HouseholdLinkRequest:
        normalized_phone = normalize_identity_value(IdentityKind.PHONE, invited_phone)
        existing_request = self._find_reusable_request(
            household_id=household_id,
            invited_identity_kind=IdentityKind.PHONE,
            invited_identity_normalized_value=normalized_phone,
        )
        if existing_request is not None:
            if invited_display_name and invited_display_name != existing_request.invited_display_name:
                return self._save_request(
                    replace(
                        existing_request,
                        invited_display_name=invited_display_name,
                    )
                )
            return existing_request
        existing_member = self.store.find_member_by_identity(
            kind=IdentityKind.PHONE,
            normalized_value=normalized_phone,
        )
        now = self.now_getter()
        status = HouseholdLinkRequestStatus.PENDING
        invited_member_id = existing_member.id if existing_member is not None else None
        source_household_id = None
        requires_merge_confirmation = False
        metadata: dict[str, object] = {}

        if existing_member is not None:
            if existing_member.household_id == household_id:
                status = HouseholdLinkRequestStatus.MERGED
                metadata["already_linked"] = True
            else:
                source_household_id = existing_member.household_id
                assessment = self.assess_household(existing_member.household_id)
                metadata["source_household_maturity"] = "mature" if assessment.is_mature else "lightweight"
                metadata["source_household_meaningful_counts"] = dict(assessment.meaningful_state_counts)
                requires_merge_confirmation = assessment.is_mature

        request = HouseholdLinkRequest(
            id=_stable_id(
                "linkreq",
                household_id,
                inviting_member_id,
                normalized_phone,
                str(int(now.timestamp() * 1000)),
            ),
            household_id=household_id,
            inviting_member_id=inviting_member_id,
            invited_identity_kind=IdentityKind.PHONE,
            invited_identity_normalized_value=normalized_phone,
            invited_identity_value=invited_phone,
            invited_display_name=invited_display_name,
            invited_member_id=invited_member_id,
            source_household_id=source_household_id,
            requires_merge_confirmation=requires_merge_confirmation,
            status=status,
            expires_at=(now + timedelta(days=self.request_expiry_days)).isoformat(),
            metadata=metadata,
        )
        return self._save_request(request)

    def find_active_phone_link_request(self, *, invited_phone: str) -> HouseholdLinkRequest | None:
        return self.store.find_active_household_link_request(
            invited_identity_kind=IdentityKind.PHONE,
            invited_identity_normalized_value=normalize_identity_value(IdentityKind.PHONE, invited_phone),
            now_utc=self.now_getter(),
        )

    def find_pending_request_for_inviting_member(
        self,
        *,
        household_id: str,
        inviting_member_id: str,
    ) -> HouseholdLinkRequest | None:
        for request in self.store.list_household_link_requests(
            household_id=household_id,
            statuses=(HouseholdLinkRequestStatus.ACCEPTED,),
        ):
            if request.inviting_member_id != inviting_member_id:
                continue
            if not request.requires_merge_confirmation:
                continue
            metadata = dict(request.metadata) if isinstance(request.metadata, dict) else {}
            if metadata.get("awaiting_inviting_confirmation") is True:
                return request
        return None

    def build_inviting_request_reply(self, request: HouseholdLinkRequest) -> str:
        target_name = self._preferred_name(request)
        metadata = dict(request.metadata) if isinstance(request.metadata, dict) else {}
        if request.status == HouseholdLinkRequestStatus.MERGED or metadata.get("already_linked") is True:
            return f"{target_name} is already linked into this household."
        if request.status == HouseholdLinkRequestStatus.ACCEPTED and request.requires_merge_confirmation:
            return (
                f"{target_name} already said yes. Because this will combine information from both sides, "
                "reply yes when you're ready for me to finish linking everything into one household."
            )
        return (
            f"I can link {target_name} into this household once they confirm from their side. "
            "Their 1:1 thread with me will stay private."
        )

    def build_invited_confirmation_prompt(self, request: HouseholdLinkRequest, *, inviting_member_name: str | None) -> str:
        inviter_name = (inviting_member_name or "The other parent").strip() or "The other parent"
        return (
            f"{inviter_name} wants to connect you to the same household here. "
            "Your own thread with me will still stay private. Reply yes if you want me to link you in."
        )

    def build_inviting_confirmation_prompt(self, request: HouseholdLinkRequest) -> str:
        target_name = self._preferred_name(request)
        return (
            f"{target_name} said yes. Because this will combine information from both sides, "
            "reply yes if you want me to finish linking everything into one household."
        )

    def decline_from_invited(
        self,
        *,
        request_id: str,
        invited_member_id: str,
    ) -> HouseholdLinkActionResult:
        request = self._require_request(request_id)
        request = self._refresh_request_for_invited_member(request=request, invited_member_id=invited_member_id)
        updated = self._save_request(
            replace(
                request,
                status=HouseholdLinkRequestStatus.DECLINED,
                metadata={
                    **dict(request.metadata),
                    "invited_declined_at": self.now_getter().isoformat(),
                },
            )
        )
        self._clear_inviting_confirmation_prompt(request=updated)
        return HouseholdLinkActionResult(
            request=updated,
            reply_text="Okay. I won't link you into the same household here.",
        )

    def cancel_from_inviting_member(
        self,
        *,
        request_id: str,
        inviting_member_id: str,
    ) -> HouseholdLinkActionResult:
        request = self._require_request(request_id)
        if request.inviting_member_id != inviting_member_id:
            raise ValueError("household_link_request_not_owned_by_member")
        updated = self._save_request(
            replace(
                request,
                status=HouseholdLinkRequestStatus.CANCELLED,
                metadata={
                    **dict(request.metadata),
                    "inviting_cancelled_at": self.now_getter().isoformat(),
                },
            )
        )
        self._clear_inviting_confirmation_prompt(request=updated)
        return HouseholdLinkActionResult(
            request=updated,
            reply_text="Okay. I won't finish linking everything into one household.",
        )

    def accept_from_invited(
        self,
        *,
        request_id: str,
        invited_member_id: str,
    ) -> HouseholdLinkActionResult:
        request = self._require_request(request_id)
        request = self._refresh_request_for_invited_member(request=request, invited_member_id=invited_member_id)
        if request.status not in {HouseholdLinkRequestStatus.PENDING, HouseholdLinkRequestStatus.ACCEPTED}:
            raise ValueError("household_link_request_not_pending")

        if request.source_household_id is None or request.source_household_id == request.household_id:
            merged = self._save_request(
                replace(
                    request,
                    status=HouseholdLinkRequestStatus.MERGED,
                    source_household_id=request.household_id,
                    metadata={
                        **dict(request.metadata),
                        "invited_confirmed_at": self.now_getter().isoformat(),
                    },
                )
            )
            return HouseholdLinkActionResult(
                request=merged,
                reply_text="You're already linked into this household here. Your 1:1 thread will stay private.",
            )

        if request.requires_merge_confirmation:
            accepted = self._save_request(
                replace(
                    request,
                    status=HouseholdLinkRequestStatus.ACCEPTED,
                    metadata={
                        **dict(request.metadata),
                        "invited_confirmed_at": self.now_getter().isoformat(),
                        "awaiting_inviting_confirmation": True,
                    },
                )
            )
            self._schedule_inviting_confirmation_prompt(request=accepted)
            return HouseholdLinkActionResult(
                request=accepted,
                reply_text=(
                    "Thanks — I’ve got your yes. Because this will combine information from both sides, "
                    "I’ll wait for Jackson to confirm before I merge everything."
                ),
            )

        preferred_child_ids = {
            child.id for child in self.store.list_child_profiles(household_id=request.household_id)
        }
        self.household_merge_service.merge_households(
            target_household_id=request.household_id,
            source_household_id=request.source_household_id,
        )
        merged_request = self._require_request(request.id)
        child_profile_conflicts = self._merge_child_profiles(
            household_id=merged_request.household_id,
            preferred_child_ids=preferred_child_ids,
        )
        cleanup = self._create_merge_cleanup_items(
            request=merged_request,
            owner_member_id=request.inviting_member_id,
            child_profile_conflicts=child_profile_conflicts,
        )
        merged = self._save_request(
            replace(
                merged_request,
                status=HouseholdLinkRequestStatus.MERGED,
                source_household_id=merged_request.household_id,
                metadata={
                    **dict(merged_request.metadata),
                    "invited_confirmed_at": self.now_getter().isoformat(),
                    "merged_at": self.now_getter().isoformat(),
                },
            )
        )
        self._clear_inviting_confirmation_prompt(request=merged)
        cleanup_suffix = self._cleanup_reply_suffix(
            count=cleanup.count,
            preview_lines=cleanup.preview_lines,
            lead_in="I also found",
        )
        return HouseholdLinkActionResult(
            request=merged,
            reply_text=f"You're linked into the same household now. Your 1:1 thread will stay private.{cleanup_suffix}",
        )

    def accept_from_inviting_member(
        self,
        *,
        request_id: str,
        inviting_member_id: str,
    ) -> HouseholdLinkActionResult:
        request = self._require_request(request_id)
        if request.inviting_member_id != inviting_member_id:
            raise ValueError("household_link_request_not_owned_by_member")
        if request.status != HouseholdLinkRequestStatus.ACCEPTED or not request.requires_merge_confirmation:
            raise ValueError("household_link_request_not_waiting_for_inviting_confirmation")
        if not request.source_household_id or request.source_household_id == request.household_id:
            merged = self._save_request(
                replace(
                    request,
                    status=HouseholdLinkRequestStatus.MERGED,
                    metadata={
                        **dict(request.metadata),
                        "inviting_confirmed_at": self.now_getter().isoformat(),
                    },
                )
            )
            return HouseholdLinkActionResult(
                request=merged,
                reply_text=f"{self._preferred_name(request)} is already linked into this household.",
            )

        preferred_child_ids = {
            child.id for child in self.store.list_child_profiles(household_id=request.household_id)
        }
        self.household_merge_service.merge_households(
            target_household_id=request.household_id,
            source_household_id=request.source_household_id,
        )
        merged_request = self._require_request(request.id)
        child_profile_conflicts = self._merge_child_profiles(
            household_id=merged_request.household_id,
            preferred_child_ids=preferred_child_ids,
        )
        cleanup = self._create_merge_cleanup_items(
            request=merged_request,
            owner_member_id=inviting_member_id,
            child_profile_conflicts=child_profile_conflicts,
        )
        merged = self._save_request(
            replace(
                merged_request,
                status=HouseholdLinkRequestStatus.MERGED,
                source_household_id=merged_request.household_id,
                metadata={
                    **dict(merged_request.metadata),
                    "awaiting_inviting_confirmation": False,
                    "inviting_confirmed_at": self.now_getter().isoformat(),
                    "merged_at": self.now_getter().isoformat(),
                },
            )
        )
        self._clear_inviting_confirmation_prompt(request=merged)
        cleanup_suffix = self._cleanup_reply_suffix(
            count=cleanup.count,
            preview_lines=cleanup.preview_lines,
            lead_in="I found",
        )
        return HouseholdLinkActionResult(
            request=merged,
            reply_text=f"Done. {self._preferred_name(request)} is linked into the same household now.{cleanup_suffix}",
        )

    def assess_household(self, household_id: str) -> HouseholdLinkAssessment:
        counts = self.store.count_household_state_rows(household_id)
        active_parent_count = len(
            [
                member
                for member in self.store.list_members(household_id)
                if member.status == "active" and member.role in {MemberRole.ADMIN, MemberRole.PARENT}
            ]
        )
        has_group_channel = bool(
            self.store.list_channels(household_id=household_id, channel_type=ChannelType.HOUSEHOLD_GROUP)
        )
        meaningful_state_counts = {
            key: int(counts.get(key, 0))
            for key in _MEANINGFUL_HOUSEHOLD_STATE_KEYS
            if int(counts.get(key, 0)) > 0
        }
        return HouseholdLinkAssessment(
            household_id=household_id,
            active_parent_count=active_parent_count,
            has_group_channel=has_group_channel,
            meaningful_state_counts=meaningful_state_counts,
        )

    def resolve_merge_followup(
        self,
        *,
        household_id: str,
        work_item_id: str,
        actor_member_id: str | None = None,
        birthdate: str | None = None,
        school: str | None = None,
        resolution_note: str | None = None,
        group_index: int = 0,
    ) -> MergeFollowupResolutionResult:
        work_item = self.store.get_household_work_item(work_item_id)
        if work_item is None or work_item.household_id != household_id:
            raise ValueError("unknown_household_work_item_id")
        metadata = dict(work_item.metadata) if isinstance(work_item.metadata, dict) else {}
        if metadata.get("category") != _MERGE_CLEANUP_WORK_ITEM_KIND:
            raise ValueError("household_work_item_not_merge_followup")
        cleanup_kind = str(metadata.get("cleanup_kind") or "").strip()
        if cleanup_kind != "child_profiles":
            raise ValueError("merge_followup_cleanup_kind_unsupported")

        duplicate_groups = list(metadata.get("duplicate_groups") or [])
        if not duplicate_groups:
            raise ValueError("merge_followup_has_no_remaining_groups")
        if group_index < 0 or group_index >= len(duplicate_groups):
            raise ValueError("merge_followup_group_index_out_of_range")
        group = dict(duplicate_groups[group_index]) if isinstance(duplicate_groups[group_index], dict) else {}
        resolved_child = self._resolve_child_merge_conflict(
            household_id=household_id,
            group=group,
            birthdate=birthdate,
            school=school,
            resolution_note=resolution_note,
        )
        updated_group = self._updated_child_conflict_group(
            group=group,
            birthdate=birthdate,
            school=school,
        )
        if updated_group is None:
            duplicate_groups.pop(group_index)
        else:
            duplicate_groups[group_index] = updated_group

        updated_metadata = dict(metadata)
        updated_metadata["duplicate_groups"] = duplicate_groups
        preview_lines = [
            self._group_preview_line(cleanup_kind=cleanup_kind, group=dict(item))
            for item in duplicate_groups
            if isinstance(item, dict)
        ]
        updated_metadata["preview_lines"] = preview_lines[:2]
        if resolution_note:
            updated_metadata["last_resolution_note"] = resolution_note.strip()
        current = self.now_getter().isoformat()
        if duplicate_groups:
            _, description, _ = self._cleanup_work_item_copy(
                cleanup_kind=cleanup_kind,
                first_group=dict(duplicate_groups[0]),
                group_count=len(duplicate_groups),
            )
            updated_work_item = replace(
                work_item,
                description=description,
                metadata=updated_metadata,
            )
        else:
            updated_work_item = replace(
                work_item,
                status=HouseholdWorkItemStatus.DONE,
                completed_at=current,
                metadata=updated_metadata,
            )
        updated_work_item = self.store.upsert_household_work_item(updated_work_item)
        FlorenceHouseholdManagerService(self.store).record_pilot_event(
            household_id=household_id,
            event_type="merge_followup_resolved",
            member_id=actor_member_id,
            metadata={
                "work_item_id": work_item_id,
                "cleanup_kind": cleanup_kind,
                "remaining_groups": len(duplicate_groups),
                "resolved_birthdate": birthdate,
                "resolved_school": school,
            },
            created_at=self.now_getter(),
        )
        remaining_conflicts = (
            list(updated_group.get("diff_lines") or [])
            if isinstance(updated_group, dict)
            else []
        )
        return MergeFollowupResolutionResult(
            work_item=updated_work_item,
            child=resolved_child,
            remaining_conflicts=remaining_conflicts,
        )

    def _find_reusable_request(
        self,
        *,
        household_id: str,
        invited_identity_kind: IdentityKind,
        invited_identity_normalized_value: str,
    ) -> HouseholdLinkRequest | None:
        statuses = (
            HouseholdLinkRequestStatus.PENDING,
            HouseholdLinkRequestStatus.ACCEPTED,
            HouseholdLinkRequestStatus.MERGED,
        )
        for request in self.store.list_household_link_requests(
            household_id=household_id,
            statuses=statuses,
        ):
            if request.invited_identity_kind != invited_identity_kind:
                continue
            if request.invited_identity_normalized_value != invited_identity_normalized_value:
                continue
            if request.status == HouseholdLinkRequestStatus.PENDING and request.expires_at:
                expires_at = datetime.fromisoformat(request.expires_at)
                if expires_at <= self.now_getter():
                    self._save_request(replace(request, status=HouseholdLinkRequestStatus.EXPIRED))
                    continue
            return request
        return None

    def _refresh_request_for_invited_member(
        self,
        *,
        request: HouseholdLinkRequest,
        invited_member_id: str,
    ) -> HouseholdLinkRequest:
        member = self.store.get_member(invited_member_id)
        if member is None:
            raise ValueError("household_link_invited_member_missing")

        metadata = dict(request.metadata) if isinstance(request.metadata, dict) else {}
        source_household_id = request.source_household_id
        requires_merge_confirmation = request.requires_merge_confirmation
        if member.household_id != request.household_id:
            source_household_id = member.household_id
            assessment = self.assess_household(member.household_id)
            metadata["source_household_maturity"] = "mature" if assessment.is_mature else "lightweight"
            metadata["source_household_meaningful_counts"] = dict(assessment.meaningful_state_counts)
            requires_merge_confirmation = assessment.is_mature
        refreshed = replace(
            request,
            invited_member_id=member.id,
            source_household_id=source_household_id,
            requires_merge_confirmation=requires_merge_confirmation,
            metadata=metadata,
        )
        return self._save_request(refreshed)

    def _preferred_name(self, request: HouseholdLinkRequest) -> str:
        name = str(request.invited_display_name or "").strip()
        if name:
            return name
        if request.invited_member_id:
            member = self.store.get_member(request.invited_member_id)
            if member is not None and member.display_name.strip():
                return member.display_name.strip()
        return "the other parent"

    def _require_request(self, request_id: str) -> HouseholdLinkRequest:
        request = self.store.get_household_link_request(request_id)
        if request is None:
            raise ValueError("household_link_request_missing")
        return request

    def _save_request(self, request: HouseholdLinkRequest) -> HouseholdLinkRequest:
        return self.store.upsert_household_link_request(
            replace(
                request,
                updated_at=self.now_getter().timestamp(),
            )
        )

    def _schedule_inviting_confirmation_prompt(self, *, request: HouseholdLinkRequest) -> None:
        manager = FlorenceHouseholdManagerService(self.store)
        channel_id = manager.default_dm_channel_id(
            household_id=request.household_id,
            member_id=request.inviting_member_id,
        )
        if not channel_id:
            return
        nudge = HouseholdNudge(
            id=_stable_id("nudge", request.household_id, request.id, _INVITING_CONFIRM_NUDGE_KIND),
            household_id=request.household_id,
            target_kind=HouseholdNudgeTargetKind.GENERAL,
            target_id=request.id,
            message=self.build_inviting_confirmation_prompt(request),
            status=HouseholdNudgeStatus.SCHEDULED,
            recipient_member_id=request.inviting_member_id,
            channel_id=channel_id,
            scheduled_for=self.now_getter().isoformat(),
            metadata={
                "kind": _INVITING_CONFIRM_NUDGE_KIND,
                "delivery_message_metadata": build_household_link_prompt_metadata(request.id, role="inviting"),
            },
        )
        self.store.upsert_household_nudge(nudge)

    def _clear_inviting_confirmation_prompt(self, *, request: HouseholdLinkRequest) -> None:
        nudge_id = _stable_id("nudge", request.household_id, request.id, _INVITING_CONFIRM_NUDGE_KIND)
        nudge = self.store.get_household_nudge(nudge_id)
        if nudge is None:
            return
        self.store.upsert_household_nudge(
            replace(
                nudge,
                status=HouseholdNudgeStatus.CANCELLED,
                acknowledged_at=self.now_getter().isoformat(),
            )
        )

    def _create_merge_cleanup_items(
        self,
        *,
        request: HouseholdLinkRequest,
        owner_member_id: str | None,
        child_profile_conflicts: list[dict[str, object]] | None = None,
    ) -> _MergeCleanupSummary:
        household_id = request.household_id
        self._auto_dedupe_exact_merge_duplicates(household_id=household_id)
        duplicate_groups = {
            "child_profiles": list(child_profile_conflicts or []),
            "events": self._duplicate_event_groups(household_id=household_id),
            "preferences": self._duplicate_preference_groups(household_id=household_id),
            "routines": self._duplicate_routine_groups(household_id=household_id),
        }
        created = 0
        preview_lines: list[str] = []
        manager = FlorenceHouseholdManagerService(self.store)
        for cleanup_kind, groups in duplicate_groups.items():
            if not groups:
                continue
            first_group = groups[0]
            title, description, item_preview_lines = self._cleanup_work_item_copy(
                cleanup_kind=cleanup_kind,
                first_group=first_group,
                group_count=len(groups),
            )
            work_item = HouseholdWorkItem(
                id=_stable_id("work", household_id, request.id, cleanup_kind, _MERGE_CLEANUP_WORK_ITEM_KIND),
                household_id=household_id,
                title=title,
                description=description,
                status=HouseholdWorkItemStatus.OPEN,
                owner_member_id=owner_member_id,
                metadata={
                    "category": _MERGE_CLEANUP_WORK_ITEM_KIND,
                    "cleanup_kind": cleanup_kind,
                    "merge_request_id": request.id,
                    "duplicate_groups": groups,
                    "preview_lines": item_preview_lines,
                },
            )
            manager.upsert_work_item(work_item)
            created += 1
            preview_lines.extend(item_preview_lines[:1])
        if created:
            manager.record_pilot_event(
                household_id=household_id,
                event_type="household_link_cleanup_created",
                member_id=owner_member_id,
                metadata={
                    "merge_request_id": request.id,
                    "cleanup_counts": {key: len(value) for key, value in duplicate_groups.items() if value},
                },
                created_at=self.now_getter(),
            )
        return _MergeCleanupSummary(count=created, preview_lines=preview_lines[:2])

    def _merge_child_profiles(
        self,
        *,
        household_id: str,
        preferred_child_ids: set[str],
    ) -> list[dict[str, object]]:
        children = self.store.list_child_profiles(household_id=household_id)
        grouped: dict[str, list[ChildProfile]] = {}
        for child in children:
            normalized = self._normalize_label(child.full_name)
            if not normalized:
                grouped.setdefault(child.id, []).append(child)
                continue
            grouped.setdefault(normalized, []).append(child)
        if all(len(group) <= 1 for group in grouped.values()):
            return []

        child_conflicts: list[dict[str, object]] = []
        merged_children: list[ChildProfile] = []
        child_id_map: dict[str, str] = {}
        profile_items = self.store.list_household_profile_items(household_id=household_id)
        school_items = [
            item
            for item in profile_items
            if item.kind == HouseholdProfileKind.SCHOOL
        ]
        for group in grouped.values():
            if len(group) <= 1:
                merged_children.append(group[0])
                continue
            canonical = self._choose_canonical_child(group=group, preferred_child_ids=preferred_child_ids)
            merged_metadata = dict(canonical.metadata) if isinstance(canonical.metadata, dict) else {}
            for child in group:
                if child.id == canonical.id:
                    continue
                child_id_map[child.id] = canonical.id
                for key, value in dict(child.metadata).items():
                    if key not in merged_metadata:
                        merged_metadata[key] = value
            birthdates = sorted({str(child.birthdate or "").strip() for child in group if str(child.birthdate or "").strip()})
            canonical_birthdate = canonical.birthdate
            if not canonical_birthdate and len(birthdates) == 1:
                canonical_birthdate = birthdates[0]
            merged_children.append(
                replace(
                    canonical,
                    birthdate=canonical_birthdate,
                    metadata=merged_metadata,
                )
            )
            conflict_lines: list[str] = []
            if len(birthdates) > 1:
                conflict_lines.append(f"Birthdate differs: {', '.join(birthdates)}.")
            school_labels = sorted(
                {
                    str(item.label or "").strip()
                    for item in school_items
                    if item.child_id in {child.id for child in group} and str(item.label or "").strip()
                }
            )
            if len(school_labels) > 1:
                conflict_lines.append(f"School differs: {', '.join(school_labels)}.")
            if conflict_lines:
                child_conflicts.append(
                    {
                        "child_id": canonical.id,
                        "items": [{"id": canonical.id, "label": canonical.full_name}],
                        "birthdates": birthdates,
                        "school_labels": school_labels,
                        "diff_lines": conflict_lines,
                    }
                )

        if child_id_map:
            self._rewrite_child_linked_profile_items(
                household_id=household_id,
                child_id_map=child_id_map,
            )
            self._rewrite_child_linked_work_items(
                household_id=household_id,
                child_id_map=child_id_map,
            )
            self._rewrite_child_linked_routines(
                household_id=household_id,
                child_id_map=child_id_map,
            )
            self._rewrite_child_linked_event_metadata(
                household_id=household_id,
                child_id_map=child_id_map,
                canonical_children={child.id: child for child in merged_children},
            )
        self.store.replace_child_profiles(
            household_id=household_id,
            children=sorted(merged_children, key=lambda child: (child.full_name.lower(), child.id)),
        )
        return child_conflicts

    def _resolve_child_merge_conflict(
        self,
        *,
        household_id: str,
        group: dict[str, object],
        birthdate: str | None,
        school: str | None,
        resolution_note: str | None,
    ) -> ChildProfile:
        child_id = str(group.get("child_id") or "").strip()
        if not child_id:
            items = list(group.get("items") or [])
            if items:
                child_id = str((items[0] or {}).get("id") or "").strip()
        child = next(
            (
                item
                for item in self.store.list_child_profiles(household_id=household_id)
                if item.id == child_id
            ),
            None,
        )
        if child is None:
            raise ValueError("merge_followup_child_missing")
        updated_metadata = dict(child.metadata) if isinstance(child.metadata, dict) else {}
        if resolution_note:
            updated_metadata["merge_resolution_note"] = resolution_note.strip()
        updated_child = replace(
            child,
            birthdate=(birthdate.strip() if birthdate and birthdate.strip() else child.birthdate),
            metadata=updated_metadata,
        )
        self.store.replace_child_profiles(
            household_id=household_id,
            children=[
                updated_child if item.id == updated_child.id else item
                for item in self.store.list_child_profiles(household_id=household_id)
            ],
        )
        if school and school.strip():
            self._apply_child_school_resolution(
                household_id=household_id,
                child=updated_child,
                school_label=school.strip(),
            )
        return updated_child

    def _apply_child_school_resolution(
        self,
        *,
        household_id: str,
        child: ChildProfile,
        school_label: str,
    ) -> None:
        all_school_items = self.store.list_household_profile_items(
            household_id=household_id,
            kind=HouseholdProfileKind.SCHOOL,
        )
        child_items = [item for item in all_school_items if item.child_id == child.id]
        other_items = [item for item in all_school_items if item.child_id != child.id]
        chosen = next(
            (
                item
                for item in child_items
                if self._normalize_label(item.label) == self._normalize_label(school_label)
            ),
            child_items[0] if child_items else None,
        )
        merged_metadata: dict[str, object] = {}
        for item in child_items:
            item_metadata = dict(item.metadata) if isinstance(item.metadata, dict) else {}
            for key, value in item_metadata.items():
                if isinstance(value, list):
                    existing = merged_metadata.get(key) if isinstance(merged_metadata.get(key), list) else []
                    merged_metadata[key] = sorted({*(str(v).strip() for v in existing if str(v).strip()), *(str(v).strip() for v in value if str(v).strip())})
                elif key not in merged_metadata and value not in (None, "", [], {}):
                    merged_metadata[key] = value
        if chosen is None:
            chosen = HouseholdProfileItem(
                id=_stable_id("school", household_id, child.id, self._normalize_label(school_label)),
                household_id=household_id,
                kind=HouseholdProfileKind.SCHOOL,
                label=school_label,
                child_id=child.id,
                metadata=merged_metadata,
            )
        else:
            chosen = replace(
                chosen,
                label=school_label,
                child_id=child.id,
                metadata={**(dict(chosen.metadata) if isinstance(chosen.metadata, dict) else {}), **merged_metadata},
            )
        self.store.replace_household_profile_items(
            household_id=household_id,
            kind=HouseholdProfileKind.SCHOOL,
            items=[*other_items, chosen],
        )

    def _updated_child_conflict_group(
        self,
        *,
        group: dict[str, object],
        birthdate: str | None,
        school: str | None,
    ) -> dict[str, object] | None:
        updated = dict(group)
        diff_lines: list[str] = []
        birthdates = [
            str(value).strip()
            for value in list(group.get("birthdates") or [])
            if str(value).strip()
        ]
        if len(sorted(set(birthdates))) > 1:
            if birthdate and birthdate.strip():
                updated["birthdates"] = [birthdate.strip()]
            else:
                diff_lines.append(f"Birthdate differs: {', '.join(sorted(set(birthdates)))}.")
        school_labels = [
            str(value).strip()
            for value in list(group.get("school_labels") or [])
            if str(value).strip()
        ]
        if len(sorted(set(school_labels))) > 1:
            if school and school.strip():
                updated["school_labels"] = [school.strip()]
            else:
                diff_lines.append(f"School differs: {', '.join(sorted(set(school_labels)))}.")
        if not diff_lines:
            return None
        updated["diff_lines"] = diff_lines
        return updated

    @staticmethod
    def _choose_canonical_child(
        *,
        group: list[ChildProfile],
        preferred_child_ids: set[str],
    ) -> ChildProfile:
        def sort_key(child: ChildProfile) -> tuple[int, int, int, str]:
            metadata = dict(child.metadata) if isinstance(child.metadata, dict) else {}
            completeness = int(bool(str(child.birthdate or "").strip())) + sum(
                1 for value in metadata.values() if value not in (None, "", [], {})
            )
            return (
                0 if child.id in preferred_child_ids else 1,
                -completeness,
                len(child.full_name),
                child.id,
            )

        return sorted(group, key=sort_key)[0]

    def _rewrite_child_linked_profile_items(
        self,
        *,
        household_id: str,
        child_id_map: dict[str, str],
    ) -> None:
        all_items = self.store.list_household_profile_items(household_id=household_id)
        by_kind: dict[HouseholdProfileKind, list[object]] = {}
        seen_keys: dict[tuple[str, str, str, str, str], str] = {}
        for item in all_items:
            updated = item
            if item.child_id in child_id_map:
                updated = replace(item, child_id=child_id_map[item.child_id])
            dedupe_key = (
                updated.kind.value,
                self._normalize_label(updated.label),
                str(updated.member_id or "").strip(),
                str(updated.child_id or "").strip(),
                json.dumps(updated.metadata, sort_keys=True),
            )
            if dedupe_key in seen_keys:
                continue
            seen_keys[dedupe_key] = updated.id
            by_kind.setdefault(updated.kind, []).append(updated)
        for kind, items in by_kind.items():
            self.store.replace_household_profile_items(
                household_id=household_id,
                kind=kind,
                items=items,
            )

    def _rewrite_child_linked_work_items(
        self,
        *,
        household_id: str,
        child_id_map: dict[str, str],
    ) -> None:
        for work_item in self.store.list_household_work_items(household_id=household_id):
            if work_item.child_id not in child_id_map:
                continue
            self.store.upsert_household_work_item(
                replace(work_item, child_id=child_id_map[work_item.child_id])
            )

    def _rewrite_child_linked_routines(
        self,
        *,
        household_id: str,
        child_id_map: dict[str, str],
    ) -> None:
        for routine in self.store.list_household_routines(household_id=household_id):
            if routine.child_id not in child_id_map:
                continue
            self.store.upsert_household_routine(
                replace(routine, child_id=child_id_map[routine.child_id])
            )

    def _rewrite_child_linked_event_metadata(
        self,
        *,
        household_id: str,
        child_id_map: dict[str, str],
        canonical_children: dict[str, ChildProfile],
    ) -> None:
        event_changed = False
        for event in self.store.list_household_events(household_id=household_id):
            metadata = dict(event.metadata) if isinstance(event.metadata, dict) else {}
            current_child_id = str(metadata.get("child_id") or "").strip()
            if current_child_id not in child_id_map:
                continue
            canonical_child_id = child_id_map[current_child_id]
            metadata["child_id"] = canonical_child_id
            canonical_child = canonical_children.get(canonical_child_id)
            if canonical_child is not None:
                metadata["child_name"] = canonical_child.full_name
            self.store.upsert_household_event(replace(event, metadata=metadata))
            event_changed = True
        if event_changed:
            from florence.runtime.household_calendar_projection import FlorenceHouseholdCalendarProjectionService

            FlorenceHouseholdCalendarProjectionService(self.store).sync_household(household_id=household_id)

    def _auto_dedupe_exact_merge_duplicates(self, *, household_id: str) -> None:
        self._auto_dedupe_exact_preferences(household_id=household_id)
        self._auto_dedupe_exact_routines(household_id=household_id)
        if self._auto_dedupe_exact_events(household_id=household_id):
            from florence.runtime.household_calendar_projection import FlorenceHouseholdCalendarProjectionService

            FlorenceHouseholdCalendarProjectionService(self.store).sync_household(household_id=household_id)

    def _auto_dedupe_exact_preferences(self, *, household_id: str) -> None:
        items = self.store.list_household_profile_items(
            household_id=household_id,
            kind=HouseholdProfileKind.PREFERENCE,
        )
        grouped: dict[tuple[str, str, str, str, str], list[object]] = {}
        for item in items:
            grouped.setdefault(
                (
                    self._normalize_label(item.label),
                    str(item.metadata.get("category") or "").strip().lower(),
                    str(item.member_id or "").strip(),
                    str(item.child_id or "").strip(),
                    self._normalize_label(str(item.metadata.get("value") or item.metadata.get("summary") or "")),
                ),
                [],
            ).append(item)
        kept: list[object] = []
        for group in grouped.values():
            ordered = sorted(group, key=lambda item: item.id)
            kept.append(ordered[0])
        self.store.replace_household_profile_items(
            household_id=household_id,
            kind=HouseholdProfileKind.PREFERENCE,
            items=kept,
        )

    def _auto_dedupe_exact_routines(self, *, household_id: str) -> None:
        for group in self._routine_groups(household_id=household_id).values():
            ordered = sorted(group, key=lambda routine: routine.id)
            if len(ordered) <= 1:
                continue
            if self._routine_diff_lines(ordered):
                continue
            for routine in ordered[1:]:
                if routine.status == HouseholdRoutineStatus.ARCHIVED:
                    continue
                self.store.upsert_household_routine(
                    replace(
                        routine,
                        status=HouseholdRoutineStatus.ARCHIVED,
                        metadata={
                            **dict(routine.metadata),
                            "merge_auto_deduped": True,
                        },
                    )
                )

    def _auto_dedupe_exact_events(self, *, household_id: str) -> bool:
        changed = False
        for group in self._event_groups(household_id=household_id).values():
            ordered = sorted(group, key=lambda event: event.id)
            if len(ordered) <= 1:
                continue
            if self._event_diff_lines(ordered):
                continue
            for event in ordered[1:]:
                if event.status == HouseholdEventStatus.CANCELLED:
                    continue
                changed = True
                self.store.upsert_household_event(
                    replace(
                        event,
                        status=HouseholdEventStatus.CANCELLED,
                        metadata={
                            **dict(event.metadata),
                            "merge_auto_deduped": True,
                        },
                    )
                )
        return changed

    @staticmethod
    def _normalize_label(value: str) -> str:
        return " ".join(
            token
            for token in "".join(ch.lower() if ch.isalnum() else " " for ch in value).split()
            if token
        )

    def _event_groups(self, *, household_id: str) -> dict[tuple[str, str, str], list[object]]:
        grouped: dict[tuple[str, str, str], list[object]] = {}
        for event in self.store.list_household_events(household_id=household_id):
            if event.status == HouseholdEventStatus.CANCELLED:
                continue
            normalized_title = self._normalize_label(event.title)
            if not event.starts_at or not normalized_title:
                continue
            child_id = str((event.metadata or {}).get("child_id") or "").strip()
            key = (event.starts_at, normalized_title, child_id)
            grouped.setdefault(key, []).append(event)
        return grouped

    def _duplicate_event_groups(self, *, household_id: str) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        for events in self._event_groups(household_id=household_id).values():
            if len(events) <= 1:
                continue
            diff_lines = self._event_diff_lines(events)
            if not diff_lines:
                continue
            results.append(
                {
                    "items": [{"id": event.id, "label": event.title} for event in events],
                    "diff_lines": diff_lines,
                }
            )
        return results

    def _duplicate_preference_groups(self, *, household_id: str) -> list[dict[str, object]]:
        grouped: dict[tuple[str, str, str, str], list[HouseholdWorkItem | object]] = {}
        for item in self.store.list_household_profile_items(
            household_id=household_id,
            kind=HouseholdProfileKind.PREFERENCE,
        ):
            normalized_label = self._normalize_label(item.label)
            category = str(item.metadata.get("category") or "").strip().lower()
            if not normalized_label:
                continue
            key = (
                normalized_label,
                category,
                str(item.member_id or "").strip(),
                str(item.child_id or "").strip(),
            )
            grouped.setdefault(key, []).append(item)
        results: list[dict[str, object]] = []
        for items in grouped.values():
            if len(items) <= 1:
                continue
            diff_lines = self._preference_diff_lines(items)
            if not diff_lines:
                continue
            results.append(
                {
                    "items": [{"id": item.id, "label": item.label} for item in items],
                    "diff_lines": diff_lines,
                }
            )
        return results

    def _routine_groups(self, *, household_id: str) -> dict[tuple[str, str, str], list[object]]:
        grouped: dict[tuple[str, str, str], list[object]] = {}
        for routine in self.store.list_household_routines(household_id=household_id):
            if routine.status == HouseholdRoutineStatus.ARCHIVED:
                continue
            normalized_title = self._normalize_label(routine.title)
            if not normalized_title:
                continue
            key = (
                normalized_title,
                str(routine.cadence or "").strip().lower(),
                str(routine.child_id or "").strip(),
            )
            grouped.setdefault(key, []).append(routine)
        return grouped

    def _duplicate_routine_groups(self, *, household_id: str) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        for routines in self._routine_groups(household_id=household_id).values():
            if len(routines) <= 1:
                continue
            diff_lines = self._routine_diff_lines(routines)
            if not diff_lines:
                continue
            results.append(
                {
                    "items": [{"id": routine.id, "label": routine.title} for routine in routines],
                    "diff_lines": diff_lines,
                }
            )
        return results

    def _event_diff_lines(self, events: list[object]) -> list[str]:
        return self._describe_field_differences(
            {
                "End time": [str(getattr(event, "ends_at", "") or "").strip() for event in events],
                "Timezone": [str(getattr(event, "timezone", "") or "").strip() for event in events],
                "Location": [str(getattr(event, "location", "") or "").strip() for event in events],
                "Description": [str(getattr(event, "description", "") or "").strip() for event in events],
            }
        )

    def _preference_diff_lines(self, items: list[object]) -> list[str]:
        return self._describe_field_differences(
            {
                "Saved value": [
                    str(getattr(item, "metadata", {}).get("value") or getattr(item, "metadata", {}).get("summary") or "").strip()
                    for item in items
                ],
            }
        )

    def _routine_diff_lines(self, routines: list[object]) -> list[str]:
        return self._describe_field_differences(
            {
                "Owner": [str(getattr(routine, "owner_member_id", "") or "").strip() for routine in routines],
                "Description": [str(getattr(routine, "description", "") or "").strip() for routine in routines],
                "Next due": [str(getattr(routine, "next_due_at", "") or "").strip() for routine in routines],
                "Status": [str(getattr(routine, "status", "") or "").strip() for routine in routines],
            }
        )

    @classmethod
    def _describe_field_differences(cls, field_values: dict[str, list[str]]) -> list[str]:
        diff_lines: list[str] = []
        for label, raw_values in field_values.items():
            values = [value if value else "<none>" for value in raw_values]
            unique_values = sorted({value for value in values})
            if len(unique_values) <= 1:
                continue
            rendered = " vs ".join(
                json.dumps("" if value == "<none>" else value, ensure_ascii=True)
                for value in unique_values[:3]
            )
            if len(unique_values) > 3:
                rendered = f"{rendered} and {len(unique_values) - 3} more"
            diff_lines.append(f"{label} differs: {rendered}.")
        return diff_lines

    @staticmethod
    def _cleanup_work_item_copy(
        cleanup_kind: str,
        *,
        first_group: dict[str, object],
        group_count: int,
    ) -> tuple[str, str, list[str]]:
        items = list(first_group.get("items") or [])
        labels = ", ".join(str(item.get("label") or "").strip() for item in items[:3] if str(item.get("label") or "").strip())
        diff_lines = [
            str(line).strip()
            for line in (first_group.get("diff_lines") or [])
            if str(line).strip()
        ]
        if cleanup_kind == "child_profiles":
            title = "Review child details after linking"
            description = f"I found {group_count} overlapping child profile group(s), starting with {labels}. {diff_lines[0] if diff_lines else ''}".strip()
            preview_lines = [f"{labels}: {diff_lines[0]}" if labels and diff_lines else labels or "Child details need review"]
        elif cleanup_kind == "events":
            title = "Review duplicate household events"
            description = f"I found {group_count} overlapping event group(s), starting with {labels}. {diff_lines[0] if diff_lines else ''}".strip()
            preview_lines = [f"{labels}: {diff_lines[0]}" if labels and diff_lines else labels or "Overlapping event"]
        elif cleanup_kind == "preferences":
            title = "Review overlapping preferences"
            description = f"I found {group_count} overlapping preference group(s), starting with {labels}. {diff_lines[0] if diff_lines else ''}".strip()
            preview_lines = [f"{labels}: {diff_lines[0]}" if labels and diff_lines else labels or "Overlapping preference"]
        else:
            title = "Review overlapping routines"
            description = f"I found {group_count} overlapping routine group(s), starting with {labels}. {diff_lines[0] if diff_lines else ''}".strip()
            preview_lines = [f"{labels}: {diff_lines[0]}" if labels and diff_lines else labels or "Overlapping routine"]
        return title, description, preview_lines

    def _group_preview_line(self, *, cleanup_kind: str, group: dict[str, object]) -> str:
        _, _, preview_lines = self._cleanup_work_item_copy(
            cleanup_kind=cleanup_kind,
            first_group=group,
            group_count=1,
        )
        return preview_lines[0] if preview_lines else ""

    @staticmethod
    def _cleanup_reply_suffix(*, count: int, preview_lines: list[str], lead_in: str) -> str:
        if count <= 0:
            return ""
        if preview_lines:
            preview = "; ".join(line.rstrip(".") for line in preview_lines[:2])
            return (
                f" {lead_in} {count} follow-up item{'s' if count != 1 else ''} to review: "
                f"{preview}."
            )
        return f" {lead_in} {count} cleanup item{'s' if count != 1 else ''} to review next."
