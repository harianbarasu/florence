"""Approved action execution for Florence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from florence.models import ActionExecutionStatus, PendingAction
from florence.store import Store
from florence.timekeeper import ensure_utc


class ActionSender(Protocol):
    def send_text(self, *, chat_id: str, text: str, idempotency_key: str) -> object:
        ...


@dataclass(frozen=True, slots=True)
class ActionRunResult:
    attempted: int
    succeeded: int
    failed: int


def run_approved_actions(
    *,
    store: Store,
    sender: ActionSender,
    now_utc: datetime | None = None,
    limit: int = 50,
) -> ActionRunResult:
    now = ensure_utc(now_utc or datetime.now(timezone.utc))
    attempted = 0
    succeeded = 0
    failed = 0
    for action in store.executable_actions(now_utc=now, limit=limit):
        attempted += 1
        try:
            result = _execute_action(action, sender, store, now)
        except Exception as exc:
            failed += 1
            store.record_action_execution(
                action=action,
                status=ActionExecutionStatus.FAILED,
                attempted_at_utc=now,
                error=str(exc),
            )
        else:
            succeeded += 1
            store.record_action_execution(
                action=action,
                status=ActionExecutionStatus.SUCCESS,
                attempted_at_utc=now,
                result=result,
            )
    return ActionRunResult(attempted=attempted, succeeded=succeeded, failed=failed)


def _execute_action(
    action: PendingAction,
    sender: ActionSender,
    store: Store,
    now_utc: datetime,
) -> dict[str, object]:
    if action.action_type == "send_message":
        return _execute_send_message(action, sender)
    if action.action_type == "create_reminder":
        return _execute_create_reminder(action, store, now_utc)
    raise ValueError(f"unsupported action type: {action.action_type}")


def _execute_send_message(action: PendingAction, sender: ActionSender) -> dict[str, object]:
    text = action.payload.get("text")
    if not isinstance(text, str) or not text.strip():
        raise ValueError("send_message action requires payload.text")
    result = sender.send_text(
        chat_id=action.chat_id,
        text=text.strip(),
        idempotency_key=f"action:{action.id}",
    )
    if isinstance(result, dict):
        return result
    return {"result": str(result)}


def _execute_create_reminder(
    action: PendingAction,
    store: Store,
    now_utc: datetime,
) -> dict[str, object]:
    title = action.payload.get("title")
    due_at_raw = action.payload.get("due_at_utc")
    if not isinstance(title, str) or not title.strip():
        raise ValueError("create_reminder action requires payload.title")
    if not isinstance(due_at_raw, str) or not due_at_raw.strip():
        raise ValueError("create_reminder action requires payload.due_at_utc")
    due_at = ensure_utc(datetime.fromisoformat(due_at_raw.replace("Z", "+00:00")))
    if due_at <= ensure_utc(now_utc):
        raise ValueError("create_reminder action due_at_utc is in the past")
    reminder = store.create_reminder(
        household_id=action.household_id,
        chat_id=action.chat_id,
        title=title.strip(),
        due_at_utc=due_at,
        created_at_utc=now_utc,
    )
    return {
        "reminder_id": reminder.id,
        "title": reminder.title,
        "due_at_utc": reminder.due_at_utc.isoformat(),
    }
