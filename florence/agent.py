"""The agent turn: one loop, real tools, no gates.

Every trigger — inbound text, due reminder, morning routine, new email —
builds full household context and runs the same loop. The model's final text
is delivered to the contextual chat; empty text means deliberate silence.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from florence.config import Settings
from florence.gmail import GoogleService
from florence.linq import LinqClient
from florence.llm import LLMClient, LLMError
from florence.store import Chat, Household, Member, Store
from florence.tools import TOOL_SCHEMAS, ToolContext, dispatch
from florence import prompts

log = logging.getLogger("florence.agent")

SANDBOX_PREFIX = "sandbox-"
MAX_BUBBLES = 3
MAX_BUBBLE_CHARS = 600
MAX_MESSAGE_CHARS = 3500
FALLBACK_TEXT = "Ugh — I hit a snag on my end just now. Give me a minute and try me again."


@dataclass
class Deps:
    settings: Settings
    store: Store
    linq: LinqClient
    gmail: GoogleService
    llm: LLMClient


@dataclass
class TurnResult:
    sent: list[dict[str, str]] = field(default_factory=list)
    trace: list[dict[str, Any]] = field(default_factory=list)
    steps: int = 0
    error: str | None = None


def split_bubbles(text: str) -> list[str]:
    """Blank lines become separate iMessage bubbles when that reads naturally."""
    text = text.strip()
    if len(text) > MAX_MESSAGE_CHARS:
        text = text[:MAX_MESSAGE_CHARS].rstrip() + "…"
    chunks = [c.strip() for c in text.split("\n\n") if c.strip()]
    if 1 < len(chunks) <= MAX_BUBBLES and all(len(c) <= MAX_BUBBLE_CHARS for c in chunks):
        return chunks
    return [text]


async def run_turn(
    deps: Deps,
    *,
    household: Household,
    chat: Chat | None,
    member: Member | None = None,
    directive: str | None = None,
    image_parts: list[dict[str, Any]] | None = None,
) -> TurnResult:
    started = time.monotonic()
    result = TurnResult()
    target_chat_id = chat.chat_id if chat else household.primary_chat_id

    async def send(chat_id: str, text: str) -> dict[str, Any]:
        if len(result.sent) >= deps.settings.max_sends_per_turn:
            return {"error": "send limit reached for this turn"}
        fresh = await deps.store.household_by_id(household.id)
        if fresh is not None and fresh.stopped:
            return {"error": "this family has paused Florence (STOP)"}
        if not chat_id.startswith(SANDBOX_PREFIX):
            response = await deps.linq.send_text(
                chat_id=chat_id, text=text, idempotency_key=uuid.uuid4().hex
            )
            external_id = None
            if isinstance(response.get("message"), dict):
                external_id = response["message"].get("id")
        else:
            external_id = None
        await deps.store.record_message(
            household_id=household.id,
            chat_id=chat_id,
            direction="outbound",
            body=text,
            sender_name="Florence",
            external_id=str(external_id) if external_id else None,
        )
        result.sent.append({"chat_id": chat_id, "text": text})
        return {"sent": True}

    household_chats = await deps.store.chats_of(household.id)
    members = await deps.store.members_of(household.id)
    ctx = ToolContext(
        settings=deps.settings,
        store=deps.store,
        gmail=deps.gmail,
        household=household,
        chat=chat,
        member=member,
        household_chats=household_chats,
        send=send,
    )
    result.trace = ctx.trace

    system_prompt = prompts.build_system_prompt(
        household=household,
        members=members,
        chats=household_chats,
        memories=await deps.store.list_memories(household.id),
        tasks=await deps.store.upcoming_tasks(household.id),
        accounts=await deps.store.gmail_accounts(household.id),
        chat=chat,
        support_contact=deps.settings.support_contact,
    )
    messages: list[dict[str, Any]] = [{"role": "system", "content": system_prompt}]
    if chat is not None:
        chat_history = await deps.store.recent_messages(chat.chat_id, prompts.HISTORY_LIMIT)
        sibling = await deps.store.recent_other_chat_messages(
            household.id, chat.chat_id, prompts.SIBLING_HISTORY_LIMIT
        )
        messages.extend(
            prompts.history_messages(
                chat_messages=chat_history,
                sibling_messages=sibling,
                members_by_phone={m.phone: m for m in members},
                image_parts=image_parts or [],
            )
        )
    if directive:
        messages.append({"role": "user", "content": directive})

    if chat is not None and not chat.chat_id.startswith(SANDBOX_PREFIX) and directive is None:
        await deps.linq.start_typing(chat.chat_id)

    final_text = ""
    try:
        for step in range(deps.settings.max_turn_steps):
            result.steps = step + 1
            reply = await deps.llm.chat(messages, tools=TOOL_SCHEMAS)
            if reply.tool_calls:
                messages.append(
                    {
                        "role": "assistant",
                        "content": reply.content or None,
                        "tool_calls": reply.tool_calls,
                    }
                )
                for call in reply.tool_calls:
                    function = call.get("function") or {}
                    tool_result = await dispatch(
                        ctx, str(function.get("name") or ""), function.get("arguments") or "{}"
                    )
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": str(call.get("id") or ""),
                            "content": tool_result,
                        }
                    )
                continue
            final_text = reply.content.strip()
            break
        else:
            log.warning("turn hit max steps (%s) without final text", deps.settings.max_turn_steps)
    except LLMError as exc:
        result.error = str(exc)
        log.error("turn failed for household %s: %s", household.id, exc)

    if result.error is None and final_text and target_chat_id:
        for bubble in split_bubbles(final_text)[:MAX_BUBBLES]:
            await send(target_chat_id, bubble)
    elif result.error is not None and directive is None and target_chat_id:
        # A parent texted and the model is down: a short honest note beats silence.
        try:
            await send(target_chat_id, FALLBACK_TEXT)
        except Exception:  # noqa: BLE001
            pass

    if chat is not None and not result.sent and not chat.chat_id.startswith(SANDBOX_PREFIX):
        await deps.linq.stop_typing(chat.chat_id)

    await deps.store.log_event(
        "agent_turn",
        household_id=household.id,
        payload={
            "chat_id": target_chat_id,
            "trigger": (directive or "inbound")[:140],
            "steps": result.steps,
            "tools": [t["tool"] for t in result.trace],
            "sent": len(result.sent),
            "ms": int((time.monotonic() - started) * 1000),
            "error": result.error,
        },
    )
    return result
