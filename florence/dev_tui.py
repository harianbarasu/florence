"""Interactive terminal harness for Florence app chat."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from florence.config import FlorenceSettings
from florence.contracts import AppChatScope
from florence.runtime.entrypoints import FlorenceAppService, FlorenceGoogleOauthConfig
from florence.state import FlorenceStateDB


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


def _select_existing_identity(store: FlorenceStateDB) -> tuple[str, str] | None:
    households = store.list_households()
    if len(households) != 1:
        return None
    members = store.list_members(households[0].id)
    if len(members) != 1:
        return None
    return households[0].id, members[0].id


def _print_assistant(text: str | None) -> None:
    if text and text.strip():
        print(f"\nFlorence> {text}\n")


def _print_thread_header(*, household_name: str, scope: AppChatScope) -> None:
    label = "shared household chat" if scope == AppChatScope.SHARED else "private parent chat"
    print(f"\n[{household_name}] active scope: {label}")


def _print_help() -> None:
    print(
        "\nCommands:\n"
        "/help                 show commands\n"
        "/scope shared         switch to shared household chat\n"
        "/scope private        switch to private parent chat\n"
        "/threads              show available app chat threads\n"
        "/messages [n]         show recent messages for the active thread\n"
        "/state                show household/member ids\n"
        "/quit                 exit\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Interactive Florence terminal harness")
    parser.add_argument("--db-path", default=".tmp/florence-tui.db", help="SQLite db path for the TUI session")
    parser.add_argument("--with-hermes-chat", action="store_true", help="Enable Hermes-backed shared chat replies")
    parser.add_argument("--parent-name", default=None, help="Bootstrap parent name if no local household exists")
    parser.add_argument("--household-name", default=None, help="Optional household name during bootstrap")
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = FlorenceStateDB(db_path)
    try:
        app = _build_app(store, with_hermes_chat=args.with_hermes_chat)
        existing = _select_existing_identity(store)
        if existing is None:
            parent_name = (args.parent_name or "").strip()
            if not parent_name:
                parent_name = input("Parent name: ").strip()
            bootstrap = app.bootstrap_app_parent(
                parent_name=parent_name,
                household_name=args.household_name,
            )
            household = bootstrap.household
            member = bootstrap.member
            _print_assistant(bootstrap.assistant_message.body if bootstrap.assistant_message else None)
        else:
            household_id, member_id = existing
            household = store.get_household(household_id)
            member = store.get_member(member_id)
            assert household is not None
            assert member is not None

        current_scope = AppChatScope.SHARED
        sessions = store.list_member_onboarding_sessions(household_id=household.id, member_id=member.id)
        if sessions and not sessions[0].is_complete:
            current_scope = AppChatScope.PRIVATE
        _print_thread_header(household_name=household.name, scope=current_scope)
        _print_help()

        while True:
            try:
                raw = input("you> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not raw:
                continue
            if raw == "/quit":
                break
            if raw == "/help":
                _print_help()
                continue
            if raw.startswith("/scope "):
                _, _, scope_name = raw.partition(" ")
                try:
                    current_scope = AppChatScope(scope_name.strip().lower())
                except ValueError:
                    print("Unknown scope. Use /scope shared or /scope private.")
                    continue
                _print_thread_header(household_name=household.name, scope=current_scope)
                continue
            if raw == "/threads":
                threads = app.list_app_threads(household_id=household.id, member_id=member.id)
                print()
                for thread in threads:
                    print(f"- {thread.scope.value}: {thread.channel.id} ({thread.channel.title})")
                print()
                continue
            if raw.startswith("/messages"):
                parts = raw.split()
                limit = 12
                if len(parts) > 1 and parts[1].isdigit():
                    limit = max(1, int(parts[1]))
                threads = app.list_app_threads(household_id=household.id, member_id=member.id)
                thread = next(thread for thread in threads if thread.scope == current_scope)
                messages = app.list_app_messages(channel_id=thread.channel.id, limit=limit)
                print()
                for message in messages:
                    role = message.sender_role.value
                    print(f"[{role}] {message.body}")
                print()
                continue
            if raw == "/state":
                payload = {
                    "household_id": household.id,
                    "household_name": household.name,
                    "member_id": member.id,
                    "member_name": member.display_name,
                    "active_scope": current_scope.value,
                }
                print(json.dumps(payload, indent=2, sort_keys=True))
                continue

            result = app.send_app_message(
                household_id=household.id,
                member_id=member.id,
                scope=current_scope,
                text=raw,
            )
            _print_assistant(result.assistant_message.body if result.assistant_message else None)
    finally:
        store.close()


if __name__ == "__main__":
    main()
