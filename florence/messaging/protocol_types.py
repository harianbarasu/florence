"""Shared reply types for Florence messaging protocols."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
from typing import Any


CANDIDATE_REVIEW_PROMPT_KIND = "candidate_review_prompt"
HOUSEHOLD_NUDGE_PROMPT_KIND = "household_nudge_prompt"
GOOGLE_CONNECT_PROMPT_KIND = "google_connect_prompt"
HOUSEHOLD_LINK_PROMPT_KIND = "household_link_prompt"
HOUSEHOLD_LINK_PROMPT_ROLE_KEY = "household_link_prompt_role"
PENDING_ACTION_TYPE_KEY = "pending_action_type"
PENDING_ACTION_TARGET_ID_KEY = "pending_action_target_id"
PENDING_ACTION_TARGET_IDS_KEY = "pending_action_target_ids"
PENDING_ACTION_TARGET_KIND_KEY = "pending_action_target_kind"
PENDING_ACTION_ID_KEY = "pending_action_id"
PENDING_ACTION_CREATED_AT_KEY = "pending_action_created_at"
PENDING_ACTION_EXPIRES_AT_KEY = "pending_action_expires_at"
PENDING_ACTION_PREVIEW_KEY = "pending_action_preview"

_DEFAULT_PENDING_ACTION_TTL_SECONDS = 7 * 24 * 60 * 60


@dataclass(frozen=True, slots=True)
class PendingAction:
    id: str
    action_type: str
    target_kind: str
    target_id: str | None = None
    target_ids: tuple[str, ...] = ()
    role: str | None = None
    preview: str | None = None
    created_at: str | None = None
    expires_at: str | None = None
    message_id: str | None = None


def _normalize_metadata_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _stable_pending_action_id(
    *,
    action_type: str,
    target_kind: str,
    target_id: str | None = None,
    target_ids: list[str] | tuple[str, ...] | None = None,
    role: str | None = None,
) -> str:
    normalized_ids = [
        _normalize_metadata_text(item)
        for item in list(target_ids or [])
        if _normalize_metadata_text(item)
    ]
    if not normalized_ids and _normalize_metadata_text(target_id):
        normalized_ids = [_normalize_metadata_text(target_id)]
    raw = "\n".join(
        [
            _normalize_metadata_text(action_type),
            _normalize_metadata_text(target_kind),
            _normalize_metadata_text(role),
            *normalized_ids,
        ]
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
    return f"pact_{digest}"


def build_pending_action_metadata(
    *,
    action_type: str,
    target_kind: str,
    target_id: str | None = None,
    target_ids: list[str] | tuple[str, ...] | None = None,
    role: str | None = None,
    preview: str | None = None,
    ttl_seconds: int = _DEFAULT_PENDING_ACTION_TTL_SECONDS,
) -> dict[str, object]:
    normalized_target_id = _normalize_metadata_text(target_id) or None
    normalized_ids = [
        _normalize_metadata_text(item)
        for item in list(target_ids or [])
        if _normalize_metadata_text(item)
    ]
    if normalized_target_id and normalized_target_id not in normalized_ids:
        normalized_ids.insert(0, normalized_target_id)
    created_at = datetime.now(timezone.utc)
    metadata: dict[str, object] = {
        PENDING_ACTION_ID_KEY: _stable_pending_action_id(
            action_type=action_type,
            target_kind=target_kind,
            target_id=normalized_target_id,
            target_ids=normalized_ids,
            role=role,
        ),
        PENDING_ACTION_TYPE_KEY: action_type,
        PENDING_ACTION_TARGET_KIND_KEY: target_kind,
        PENDING_ACTION_CREATED_AT_KEY: created_at.isoformat(),
        PENDING_ACTION_EXPIRES_AT_KEY: (created_at + timedelta(seconds=max(1, int(ttl_seconds)))).isoformat(),
    }
    if normalized_target_id:
        metadata[PENDING_ACTION_TARGET_ID_KEY] = normalized_target_id
    if normalized_ids:
        metadata[PENDING_ACTION_TARGET_IDS_KEY] = normalized_ids
    normalized_role = _normalize_metadata_text(role)
    if normalized_role:
        metadata[HOUSEHOLD_LINK_PROMPT_ROLE_KEY] = normalized_role
    normalized_preview = _normalize_metadata_text(preview)
    if normalized_preview:
        metadata[PENDING_ACTION_PREVIEW_KEY] = normalized_preview
    return metadata


def pending_action_from_metadata(
    metadata: dict[str, object] | None,
    *,
    message_id: str | None = None,
    message_body: str | None = None,
) -> PendingAction | None:
    if not isinstance(metadata, dict):
        return None
    action_type = _normalize_metadata_text(metadata.get(PENDING_ACTION_TYPE_KEY))
    target_kind = _normalize_metadata_text(metadata.get(PENDING_ACTION_TARGET_KIND_KEY))
    if not action_type or not target_kind:
        return None
    raw_target_ids = metadata.get(PENDING_ACTION_TARGET_IDS_KEY)
    if not isinstance(raw_target_ids, (list, tuple)):
        raw_target_ids = []
    target_ids = tuple(
        item
        for item in (
            _normalize_metadata_text(raw)
            for raw in list(raw_target_ids)
        )
        if item
    )
    target_id = _normalize_metadata_text(metadata.get(PENDING_ACTION_TARGET_ID_KEY)) or (target_ids[0] if target_ids else None)
    action_id = _normalize_metadata_text(metadata.get(PENDING_ACTION_ID_KEY)) or _stable_pending_action_id(
        action_type=action_type,
        target_kind=target_kind,
        target_id=target_id,
        target_ids=target_ids,
        role=_normalize_metadata_text(metadata.get(HOUSEHOLD_LINK_PROMPT_ROLE_KEY)) or None,
    )
    return PendingAction(
        id=action_id,
        action_type=action_type,
        target_kind=target_kind,
        target_id=target_id,
        target_ids=target_ids or ((target_id,) if target_id else ()),
        role=_normalize_metadata_text(metadata.get(HOUSEHOLD_LINK_PROMPT_ROLE_KEY)) or None,
        preview=_normalize_metadata_text(metadata.get(PENDING_ACTION_PREVIEW_KEY))
        or _normalize_metadata_text(message_body)
        or None,
        created_at=_normalize_metadata_text(metadata.get(PENDING_ACTION_CREATED_AT_KEY)) or None,
        expires_at=_normalize_metadata_text(metadata.get(PENDING_ACTION_EXPIRES_AT_KEY)) or None,
        message_id=message_id,
    )


def pending_action_is_expired(action: PendingAction, *, now: datetime | None = None) -> bool:
    if not action.expires_at:
        return False
    try:
        expires_at = datetime.fromisoformat(action.expires_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return expires_at <= current.astimezone(timezone.utc)


def latest_active_pending_action(
    messages: list[Any] | tuple[Any, ...],
    *,
    now: datetime | None = None,
) -> PendingAction | None:
    for message in reversed(list(messages or [])):
        sender_role = getattr(message, "sender_role", None)
        role_value = getattr(sender_role, "value", sender_role)
        if role_value is not None and str(role_value) != "assistant":
            continue
        metadata = getattr(message, "metadata", None)
        action = pending_action_from_metadata(
            metadata if isinstance(metadata, dict) else None,
            message_id=str(getattr(message, "id", "") or "").strip() or None,
            message_body=str(getattr(message, "body", "") or ""),
        )
        if action is None:
            continue
        if pending_action_is_expired(action, now=now):
            continue
        return action
    return None


def pending_action_to_model_context(action: PendingAction) -> dict[str, object]:
    return {
        "pending_action_id": action.id,
        "action_type": action.action_type,
        "target_kind": action.target_kind,
        "target_id": action.target_id,
        "target_ids": list(action.target_ids),
        "role": action.role,
        "preview": action.preview,
        "expires_at": action.expires_at,
    }


def build_candidate_review_prompt_metadata(
    candidate_id: str,
    *,
    candidate_ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, object]:
    normalized_ids = [
        str(item).strip()
        for item in list(candidate_ids or [])
        if str(item).strip()
    ]
    metadata: dict[str, object] = {
        "protocol_kind": CANDIDATE_REVIEW_PROMPT_KIND,
        **build_pending_action_metadata(
            action_type="candidate_review",
            target_kind="imported_candidate",
            target_id=candidate_id,
            target_ids=normalized_ids,
        ),
    }
    return metadata


def build_household_nudge_metadata(nudge_id: str) -> dict[str, object]:
    return {
        "protocol_kind": HOUSEHOLD_NUDGE_PROMPT_KIND,
        **build_pending_action_metadata(
            action_type="household_nudge",
            target_kind="household_nudge",
            target_id=nudge_id,
        ),
    }


def build_google_connect_prompt_metadata() -> dict[str, object]:
    return {
        "protocol_kind": GOOGLE_CONNECT_PROMPT_KIND,
        **build_pending_action_metadata(
            action_type="google_connect",
            target_kind="google_connection",
        ),
    }


def build_household_link_prompt_metadata(request_id: str, *, role: str) -> dict[str, object]:
    return {
        "protocol_kind": HOUSEHOLD_LINK_PROMPT_KIND,
        **build_pending_action_metadata(
            action_type="household_link_request",
            target_kind="household_link_request",
            target_id=request_id,
            role=role,
        ),
    }


@dataclass(slots=True)
class FlorenceProtocolReply:
    reply_text: str | None = None
    reply_messages: tuple[str, ...] = ()
    reply_metadata: dict[str, object] = field(default_factory=dict)
    group_announcement: str | None = None
    consumed: bool = False
