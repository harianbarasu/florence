"""Local terminal smoke harness for Florence."""

from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path
from typing import Any

from florence.config import FlorenceSettings
from florence.contracts import IdentityKind
from florence.runtime.entrypoints import FlorenceAppService, FlorenceGoogleOauthConfig
from florence.runtime.resolver import infer_identity_kind, normalize_identity_value
from florence.state import FlorenceStateDB


def _build_payload(
    *,
    sender_handle: str,
    thread_id: str,
    text: str,
    is_group: bool,
    participants: list[str],
) -> dict[str, Any]:
    return {
        "type": "new-message",
        "data": {
            "message": {
                "guid": f"msg_{uuid.uuid4().hex[:12]}",
                "text": text,
                "isFromMe": False,
            },
            "chat": {
                "chatGuid": thread_id,
                "isGroup": is_group,
                "participantHandles": participants,
            },
            "sender": {
                "address": sender_handle,
            },
        },
    }


def _build_app(store: FlorenceStateDB, *, with_hermes_chat: bool) -> FlorenceAppService:
    settings = FlorenceSettings.from_env()
    google_oauth = (
        FlorenceGoogleOauthConfig(
            client_id=settings.google.client_id or "",
            client_secret=settings.google.client_secret or "",
            redirect_uri=settings.google.redirect_uri or "",
            state_secret=settings.google.state_secret or "",
        )
        if settings.google.configured
        else None
    )
    return FlorenceAppService(
        store,
        google_oauth=google_oauth,
        household_chat_model=settings.hermes.model if with_hermes_chat else None,
        household_chat_max_iterations=settings.hermes.max_iterations,
    )


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _enum_value(value: Any) -> Any:
    return getattr(value, "value", value)


def _detect_household_id(store: FlorenceStateDB, sender_handle: str) -> str | None:
    kind = infer_identity_kind(sender_handle)
    normalized = normalize_identity_value(kind, sender_handle)
    member = store.find_member_by_identity(kind=kind, normalized_value=normalized)
    return member.household_id if member is not None else None


def _serialize_state(store: FlorenceStateDB, *, household_id: str | None) -> dict[str, Any]:
    households = [store.get_household(household_id)] if household_id else store.list_households()
    serialized_households: list[dict[str, Any]] = []
    for household in households:
        if household is None:
            continue
        members = store.list_members(household.id)
        serialized_households.append(
            {
                "household": {
                    "id": household.id,
                    "name": household.name,
                    "timezone": household.timezone,
                    "status": _enum_value(household.status),
                    "settings": household.settings,
                },
                "members": [
                    {
                        "id": member.id,
                        "display_name": member.display_name,
                        "role": _enum_value(member.role),
                        "status": _enum_value(member.status),
                        "identities": [
                            {
                                "kind": _enum_value(identity.kind),
                                "value": identity.value,
                                "normalized_value": identity.normalized_value,
                            }
                            for identity in store.list_member_identities(member.id)
                        ],
                        "onboarding_sessions": [
                            {
                                "thread_id": session.thread_id,
                                "stage": session.stage.value,
                                "parent_display_name": session.parent_display_name,
                                "google_connected": session.google_connected,
                                "child_names": session.child_names,
                                "school_labels": session.school_labels,
                                "activity_labels": session.activity_labels,
                                "group_channel_id": session.group_channel_id,
                            }
                            for session in store.list_member_onboarding_sessions(
                                household_id=household.id,
                                member_id=member.id,
                            )
                        ],
                        "google_connections": [
                            {
                                "id": connection.id,
                                "email": connection.email,
                                "connected_scopes": [_enum_value(scope) for scope in connection.connected_scopes],
                                "active": connection.active,
                                "metadata": connection.metadata,
                            }
                            for connection in store.list_google_connections(
                                household_id=household.id,
                                member_id=member.id,
                            )
                        ],
                        "candidates": [
                            {
                                "id": candidate.id,
                                "title": candidate.title,
                                "summary": candidate.summary,
                                "state": _enum_value(candidate.state),
                                "source_kind": _enum_value(candidate.source_kind),
                                "confidence_bps": candidate.confidence_bps,
                                "requires_confirmation": candidate.requires_confirmation,
                                "metadata": candidate.metadata,
                            }
                            for candidate in store.list_imported_candidates(
                                household_id=household.id,
                                member_id=member.id,
                            )
                        ],
                    }
                    for member in members
                ],
                "channels": [
                    {
                        "id": channel.id,
                        "provider": channel.provider,
                        "provider_channel_id": channel.provider_channel_id,
                        "channel_type": _enum_value(channel.channel_type),
                        "title": channel.title,
                        "metadata": channel.metadata,
                    }
                    for channel in store.list_channels(household_id=household.id)
                ],
                "events": [
                    {
                        "id": event.id,
                        "title": event.title,
                        "starts_at": event.starts_at,
                        "ends_at": event.ends_at,
                        "timezone": event.timezone,
                        "status": _enum_value(event.status),
                        "source_candidate_id": event.source_candidate_id,
                        "metadata": event.metadata,
                    }
                    for event in store.list_household_events(household_id=household.id)
                ],
            }
        )
    return {"households": serialized_households}


def main() -> None:
    parser = argparse.ArgumentParser(description="Local Florence terminal smoke harness")
    parser.add_argument("--db-path", default=".tmp/florence-smoke.db", help="SQLite db path for the smoke session")
    parser.add_argument(
        "--with-hermes-chat",
        action="store_true",
        help="Enable Hermes-backed group chat replies for non-deterministic group turns",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    message_parser = subparsers.add_parser("message", help="Send a synthetic BlueBubbles message into Florence")
    message_parser.add_argument("--from", dest="sender_handle", required=True, help="Sender phone/email handle")
    message_parser.add_argument("--thread", required=True, help="BlueBubbles chat GUID or local synthetic thread id")
    message_parser.add_argument("--text", required=True, help="Message body")
    message_parser.add_argument("--group", action="store_true", help="Treat the message as a group chat message")
    message_parser.add_argument(
        "--participant",
        dest="participants",
        action="append",
        default=[],
        help="Additional group participant handle; repeatable",
    )

    state_parser = subparsers.add_parser("state", help="Inspect the current Florence smoke state")
    state_parser.add_argument("--household-id", default=None, help="Optional household id filter")
    state_parser.add_argument("--for-handle", default=None, help="Resolve household id from a known sender handle")

    args = parser.parse_args()
    db_path = Path(args.db_path).expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    store = FlorenceStateDB(db_path)
    try:
        if args.command == "message":
            app = _build_app(store, with_hermes_chat=args.with_hermes_chat)
            participants = list(dict.fromkeys([args.sender_handle, *args.participants])) if args.group else []
            payload = _build_payload(
                sender_handle=args.sender_handle,
                thread_id=args.thread,
                text=args.text,
                is_group=args.group,
                participants=participants,
            )
            result = app.handle_bluebubbles_payload(payload)
            _print_json(
                {
                    "consumed": result.consumed,
                    "reply_text": result.reply_text,
                    "group_announcement": result.group_announcement,
                    "household_id": result.household_id,
                    "member_id": result.member_id,
                    "channel_id": result.channel_id,
                    "error": result.error,
                }
            )
            return

        household_id = args.household_id
        if household_id is None and args.for_handle:
            household_id = _detect_household_id(store, args.for_handle)
        _print_json(_serialize_state(store, household_id=household_id))
    finally:
        store.close()


if __name__ == "__main__":
    main()
