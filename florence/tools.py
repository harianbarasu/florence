"""The agent's tools: schemas the model sees and the dispatcher that runs them.

Every tool returns a JSON-serializable dict. Errors come back as
{"error": "..."} so the model can correct itself mid-turn instead of failing.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Awaitable, Callable

from florence.config import Settings
from florence.gmail import GoogleService
from florence.store import Chat, Household, Member, Store
from florence.timeutil import (
    RECURRENCES,
    format_local,
    next_local_time,
    now_utc,
    parse_local,
    resolve_timezone,
)

log = logging.getLogger("florence.tools")

SendFn = Callable[[str, str], Awaitable[dict[str, Any]]]


@dataclass
class ToolContext:
    settings: Settings
    store: Store
    gmail: GoogleService
    household: Household
    chat: Chat | None
    member: Member | None
    household_chats: list[Chat]
    send: SendFn
    trace: list[dict[str, Any]] = field(default_factory=list)


TOOL_SCHEMAS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "remember",
            "description": (
                "Save a durable family fact you'll want weeks from now: names, school, teachers, "
                "activities, sizes, allergies, routines, who handles what, preferences. "
                "Save proactively when you learn something durable. Don't save one-off chatter."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {"type": "string", "description": "The fact, written so it makes sense out of context. E.g. 'Maya (age 7) is in Ms. Alvarez's 2nd-grade class at Lincoln Elementary.'"},
                    "category": {
                        "type": "string",
                        "enum": ["kids", "school", "activities", "health", "logistics", "dates", "preferences", "other"],
                    },
                },
                "required": ["content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "forget",
            "description": "Delete a saved memory that is wrong or outdated (see ids in your context). To correct a fact, forget the old one and remember the new one.",
            "parameters": {
                "type": "object",
                "properties": {"memory_id": {"type": "string"}},
                "required": ["memory_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "schedule_reminder",
            "description": (
                "Schedule a reminder Florence will deliver as a text at the right moment. "
                "Compute when_local from the current local time in your context. "
                "Give reminders sensible lead time (a Friday-morning deadline means a Thursday-evening reminder)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string", "description": "What to remind about, e.g. 'Sign Maya's permission slip (due tomorrow)'"},
                    "when_local": {"type": "string", "description": "'YYYY-MM-DD HH:MM' 24-hour, in the household's timezone"},
                    "repeat": {"type": "string", "enum": ["none", *RECURRENCES]},
                    "notes": {"type": "string", "description": "Context to include when delivering it"},
                    "deliver_to_chat_id": {"type": "string", "description": "Defaults to the current chat"},
                },
                "required": ["title", "when_local"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_reminder",
            "description": "Reschedule, rename, complete, or cancel an existing reminder/routine by id (see Schedule in your context). Also use this to change the morning brief time.",
            "parameters": {
                "type": "object",
                "properties": {
                    "reminder_id": {"type": "string"},
                    "when_local": {"type": "string", "description": "'YYYY-MM-DD HH:MM' new time, local"},
                    "title": {"type": "string"},
                    "notes": {"type": "string"},
                    "status": {"type": "string", "enum": ["done", "cancelled"]},
                },
                "required": ["reminder_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_reminders",
            "description": "List this family's reminders and routines beyond the upcoming ones already in your context.",
            "parameters": {
                "type": "object",
                "properties": {"include_inactive": {"type": "boolean"}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_email",
            "description": (
                "Search the family's connected Gmail accounts. Uses Gmail query syntax: "
                "from:, to:, subject:, after:YYYY/MM/DD, before:, has:attachment, etc."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "account_email": {"type": "string", "description": "Limit to one connected account; default searches all"},
                    "max_results": {"type": "integer", "minimum": 1, "maximum": 10},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_email",
            "description": "Read the full body of one email found via search_email or listed in an email alert.",
            "parameters": {
                "type": "object",
                "properties": {
                    "email_id": {"type": "string"},
                    "account_email": {"type": "string"},
                },
                "required": ["email_id", "account_email"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_calendar",
            "description": "Fetch upcoming Google Calendar events from the family's connected accounts.",
            "parameters": {
                "type": "object",
                "properties": {"days_ahead": {"type": "integer", "minimum": 1, "maximum": 30}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_calendar_event",
            "description": (
                "Create an event on the family's Google Calendar — camps, appointments, games, school "
                "events. Use this when something has real dates the family should see on their calendar "
                "(a reminder is for nudging; a calendar event is for the schedule — often you want both). "
                "Requires a connected Google account."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "start_local": {"type": "string", "description": "'YYYY-MM-DD HH:MM' local time, or 'YYYY-MM-DD' for all-day events"},
                    "end_local": {"type": "string", "description": "Same format. For multi-day all-day events (like camps), the LAST day. Defaults to 1 hour after start."},
                    "all_day": {"type": "boolean"},
                    "location": {"type": "string"},
                    "notes": {"type": "string"},
                    "account_email": {"type": "string", "description": "Which connected calendar; defaults to the requester's"},
                },
                "required": ["title", "start_local"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "gmail_connect_link",
            "description": (
                "Create a private link a parent can tap to connect their Gmail (read-only: email + calendar). "
                "Share the returned URL in your reply. Each parent connects their own."
            ),
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "message_chat",
            "description": (
                "Send a text to a DIFFERENT chat of this family right now (your normal reply already goes "
                "to the current chat). Use e.g. to post something in the family group from a 1:1 thread when asked."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "chat_id": {"type": "string"},
                    "message": {"type": "string"},
                },
                "required": ["chat_id", "message"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "set_household",
            "description": "Update household settings: display name and/or IANA timezone (e.g. America/New_York). Set the timezone as soon as you learn where the family lives.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "e.g. 'The Patel family'"},
                    "timezone": {"type": "string", "description": "IANA timezone name"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_member",
            "description": "Set a family member's name or role once you learn it (phones are in your context). Do this as soon as someone introduces themselves or each other.",
            "parameters": {
                "type": "object",
                "properties": {
                    "phone": {"type": "string"},
                    "name": {"type": "string"},
                    "role": {"type": "string", "enum": ["parent", "helper", "kid", "other"]},
                },
                "required": ["phone"],
            },
        },
    },
]


async def dispatch(ctx: ToolContext, name: str, raw_args: str | dict[str, Any]) -> str:
    if isinstance(raw_args, str):
        try:
            args = json.loads(raw_args) if raw_args.strip() else {}
        except ValueError:
            return json.dumps({"error": f"arguments were not valid JSON: {raw_args[:200]}"})
    else:
        args = raw_args or {}
    handler = _HANDLERS.get(name)
    if handler is None:
        return json.dumps({"error": f"unknown tool: {name}"})
    try:
        result = await handler(ctx, args)
    except ValueError as exc:
        result = {"error": str(exc)}
    except TypeError as exc:
        result = {"error": f"bad arguments for {name}: {exc}"}
    except Exception as exc:  # noqa: BLE001 - tool failures go back to the model
        log.exception("tool %s failed", name)
        result = {"error": f"{name} failed: {exc}"}
    ctx.trace.append({"tool": name, "args": args, "result": _truncate(result)})
    return json.dumps(result, default=str)


def _truncate(result: dict[str, Any]) -> dict[str, Any]:
    text = json.dumps(result, default=str)
    if len(text) <= 600:
        return result
    return {"truncated": text[:600]}


# -- handlers -------------------------------------------------------------------


async def _remember(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    content = str(args.get("content") or "").strip()
    if not content:
        raise ValueError("content is required")
    memory_id = await ctx.store.add_memory(
        ctx.household.id,
        content,
        args.get("category"),
        ctx.member.phone if ctx.member else "florence",
    )
    return {"saved": True, "memory_id": memory_id}


async def _forget(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    ok = await ctx.store.archive_memory(ctx.household.id, str(args.get("memory_id") or ""))
    return {"forgotten": ok} if ok else {"error": "no such memory"}


def _default_chat_id(ctx: ToolContext) -> str | None:
    if ctx.chat is not None:
        return ctx.chat.chat_id
    return ctx.household.primary_chat_id


async def _schedule_reminder(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    title = str(args.get("title") or "").strip()
    when_raw = str(args.get("when_local") or "").strip()
    if not title or not when_raw:
        raise ValueError("title and when_local are required")
    tz = ctx.household.timezone
    due_at = parse_local(when_raw, tz)
    now = now_utc()
    if due_at < now:
        raise ValueError(
            f"{when_raw} is in the past — it is currently {format_local(now, tz)} for this family. "
            "Recompute and try again."
        )
    repeat = str(args.get("repeat") or "none").strip().lower()
    if repeat not in ("none", *RECURRENCES):
        raise ValueError(f"repeat must be one of none, {', '.join(RECURRENCES)}")
    deliver_to = str(args.get("deliver_to_chat_id") or "").strip() or _default_chat_id(ctx)
    if deliver_to and deliver_to not in {c.chat_id for c in ctx.household_chats}:
        raise ValueError("deliver_to_chat_id is not one of this family's chats")
    task_id = await ctx.store.create_task(
        household_id=ctx.household.id,
        chat_id=deliver_to,
        kind="reminder",
        title=title,
        due_at=due_at,
        notes=str(args.get("notes") or "").strip() or None,
        recurrence=None if repeat == "none" else repeat,
        created_by=ctx.member.phone if ctx.member else "florence",
    )
    return {
        "reminder_id": task_id,
        "will_fire": format_local(due_at, tz),
        "repeat": repeat,
    }


async def _update_reminder(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    task_id = str(args.get("reminder_id") or "").strip()
    task = await ctx.store.get_task(ctx.household.id, task_id)
    if task is None:
        raise ValueError("no reminder with that id for this family")
    tz = ctx.household.timezone
    due_at = None
    if args.get("when_local"):
        due_at = parse_local(str(args["when_local"]), tz)
        if due_at < now_utc():
            raise ValueError(
                f"that time is in the past — it is currently {format_local(now_utc(), tz)}"
            )
    status = args.get("status")
    if status is not None and status not in ("done", "cancelled"):
        raise ValueError("status must be done or cancelled")
    ok = await ctx.store.update_task(
        ctx.household.id,
        task_id,
        title=str(args["title"]).strip() if args.get("title") else None,
        notes=str(args["notes"]).strip() if args.get("notes") else None,
        due_at=due_at,
        status=status,
    )
    out: dict[str, Any] = {"updated": ok}
    if due_at is not None:
        out["will_fire"] = format_local(due_at, tz)
    return out


async def _list_reminders(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    tasks = await ctx.store.list_tasks(
        ctx.household.id, include_inactive=bool(args.get("include_inactive"))
    )
    tz = ctx.household.timezone
    return {
        "reminders": [
            {
                "id": t.id,
                "title": t.title,
                "when": format_local(t.due_at, tz),
                "repeat": t.recurrence or "none",
                "status": t.status,
                "kind": t.kind,
            }
            for t in tasks
        ]
    }


async def _accounts_for(ctx: ToolContext, account_email: str | None) -> list:
    accounts = await ctx.store.gmail_accounts(ctx.household.id)
    if account_email:
        accounts = [a for a in accounts if a.email.lower() == account_email.lower().strip()]
        if not accounts:
            raise ValueError(f"no connected account named {account_email}")
    if not accounts:
        raise ValueError(
            "no Gmail connected for this family yet — offer gmail_connect_link to set one up"
        )
    return accounts


async def _search_email(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    query = str(args.get("query") or "").strip()
    if not query:
        raise ValueError("query is required")
    cap = min(int(args.get("max_results") or 5), 10)
    accounts = await _accounts_for(ctx, args.get("account_email"))
    results = []
    for account in accounts:
        for s in await ctx.gmail.search(account, query, cap=cap):
            results.append(
                {
                    "email_id": s.gmail_id,
                    "account_email": s.account_email,
                    "from": s.sender,
                    "subject": s.subject,
                    "date": format_local(s.received_at, ctx.household.timezone),
                    "snippet": s.snippet,
                }
            )
    return {"results": results[: cap * len(accounts)], "hint": "use read_email for full contents"}


async def _read_email(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    accounts = await _accounts_for(ctx, str(args.get("account_email") or "").strip() or None)
    return await ctx.gmail.read_full(accounts[0], str(args.get("email_id") or "").strip())


async def _get_calendar(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    days = min(int(args.get("days_ahead") or 7), 30)
    accounts = await _accounts_for(ctx, None)
    tz = ctx.household.timezone
    events = []
    for account in accounts:
        for e in await ctx.gmail.calendar_events(account, days_ahead=days, tz_name=tz):
            events.append(
                {
                    "title": e.title,
                    "when": format_local(e.starts_at, tz) + (" (all day)" if e.all_day else ""),
                    "location": e.location,
                    "calendar": e.account_email,
                    "_sort": e.starts_at,
                }
            )
    events.sort(key=lambda e: e.pop("_sort"))
    return {"events": events[:40]}


async def _add_calendar_event(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    title = str(args.get("title") or "").strip()
    start_raw = str(args.get("start_local") or "").strip()
    if not title or not start_raw:
        raise ValueError("title and start_local are required")
    accounts = await _accounts_for(ctx, str(args.get("account_email") or "").strip() or None)
    account = accounts[0]
    if ctx.member and not args.get("account_email"):
        for a in accounts:
            if a.member_phone == ctx.member.phone:
                account = a
                break
    tz = ctx.household.timezone
    end_raw = str(args.get("end_local") or "").strip() or None
    all_day = bool(args.get("all_day")) or (len(start_raw) == 10 and "-" in start_raw)
    if all_day:
        try:
            date.fromisoformat(start_raw[:10])
        except ValueError as exc:
            raise ValueError("all-day start_local must be 'YYYY-MM-DD'") from exc
        data = await ctx.gmail.create_event(
            account,
            title=title,
            start=None,
            end=None,
            tz_name=tz,
            location=str(args.get("location") or "").strip() or None,
            description=str(args.get("notes") or "").strip() or None,
            all_day_start=start_raw[:10],
            all_day_end=end_raw[:10] if end_raw else None,
        )
        when = start_raw[:10] + (f" through {end_raw[:10]}" if end_raw else "") + " (all day)"
    else:
        start = parse_local(start_raw, tz)
        end = parse_local(end_raw, tz) if end_raw else None
        data = await ctx.gmail.create_event(
            account,
            title=title,
            start=start,
            end=end,
            tz_name=tz,
            location=str(args.get("location") or "").strip() or None,
            description=str(args.get("notes") or "").strip() or None,
        )
        when = format_local(start, tz)
    return {"created": True, "calendar": account.email, "when": when, "link": data.get("htmlLink")}


async def _gmail_connect_link(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    if not ctx.gmail.configured:
        raise ValueError("Gmail connection isn't configured on the server yet")
    state = await ctx.store.create_oauth_state(
        household_id=ctx.household.id,
        member_phone=ctx.member.phone if ctx.member else None,
        chat_id=_default_chat_id(ctx),
        ttl_minutes=60,
    )
    return {
        "url": f"{ctx.settings.web_base_url}/connect/google?s={state}",
        "expires_in_minutes": 60,
        "note": "Read-only access to Gmail and Calendar. Florence confirms in chat once connected.",
    }


async def _message_chat(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    chat_id = str(args.get("chat_id") or "").strip()
    message = str(args.get("message") or "").strip()
    if not chat_id or not message:
        raise ValueError("chat_id and message are required")
    if chat_id not in {c.chat_id for c in ctx.household_chats}:
        raise ValueError("that chat_id does not belong to this family")
    if ctx.chat is not None and chat_id == ctx.chat.chat_id:
        raise ValueError("that's the current chat — just reply normally instead")
    return await ctx.send(chat_id, message)


async def _set_household(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    name = str(args.get("name") or "").strip() or None
    tz = str(args.get("timezone") or "").strip() or None
    if tz is not None:
        resolve_timezone(tz)
    if name is None and tz is None:
        raise ValueError("nothing to update")
    await ctx.store.set_household(ctx.household.id, name=name, timezone_name=tz)
    if tz is not None:
        for routine in await ctx.store.list_tasks(ctx.household.id):
            if routine.kind == "routine":
                local = routine.due_at.astimezone(resolve_timezone(ctx.household.timezone))
                await ctx.store.update_task(
                    ctx.household.id,
                    routine.id,
                    due_at=next_local_time(local.hour, local.minute, tz),
                )
    return {"updated": True, "name": name, "timezone": tz}


async def _update_member(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
    phone = str(args.get("phone") or "").strip()
    if not phone:
        raise ValueError("phone is required")
    name = str(args.get("name") or "").strip() or None
    role = str(args.get("role") or "").strip() or None
    if role is not None and role not in ("parent", "helper", "kid", "other"):
        raise ValueError("role must be parent, helper, kid, or other")
    ok = await ctx.store.update_member(ctx.household.id, phone, name=name, role=role)
    if not ok:
        raise ValueError("no member with that phone in this family")
    return {"updated": True}


_HANDLERS: dict[str, Callable[[ToolContext, dict[str, Any]], Awaitable[dict[str, Any]]]] = {
    "remember": _remember,
    "forget": _forget,
    "schedule_reminder": _schedule_reminder,
    "update_reminder": _update_reminder,
    "list_reminders": _list_reminders,
    "search_email": _search_email,
    "read_email": _read_email,
    "get_calendar": _get_calendar,
    "add_calendar_event": _add_calendar_event,
    "gmail_connect_link": _gmail_connect_link,
    "message_chat": _message_chat,
    "set_household": _set_household,
    "update_member": _update_member,
}
