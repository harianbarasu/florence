"""Hermes-backed household chat orchestration for Florence."""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any, Callable
from zoneinfo import ZoneInfo

from florence.contracts import (
    Channel,
    ChannelMessage,
    ChannelMessageRole,
    ChannelType,
    HouseholdBriefingKind,
    HouseholdLinkRequestStatus,
    HouseholdMealStatus,
    HouseholdNudgeStatus,
    HouseholdProfileKind,
    HouseholdRoutineStatus,
    HouseholdShoppingItemStatus,
    HouseholdWorkItemStatus,
)
from florence.messaging.types import FlorenceInboundAttachment
from florence.state import FlorenceStateDB
from florence.runtime.visibility import (
    build_scope_model_lines,
    resolve_conversation_scope,
    resolve_google_calendar_scope,
    resolve_google_inbox_scope,
)

logger = logging.getLogger(__name__)


_ONBOARDING_SYNC_WAITING_SENTINEL = "HANDOFF_TO_SYNC_WAITING"
_ONBOARDING_CONTEXTUAL_CHAT_SENTINEL = "HANDOFF_TO_CONTEXTUAL_CHAT"
_ONBOARDING_NO_REPLY_SENTINEL = "NO_SETUP_REPLY"
_REVIEW_SHOW_PROMPT_SENTINEL = "SHOW_CURRENT_REVIEW_PROMPT"
_REVIEW_NO_ACTION_SENTINEL = "NO_REVIEW_PROTOCOL_ACTION"
_GROUP_SHARE_EXECUTE_SENTINEL = "EXECUTE_GROUP_SHARE"
_GROUP_SHARE_NO_ACTION_SENTINEL = "NO_GROUP_SHARE_PROTOCOL_ACTION"
_GROUP_INTRO_SHOW_SENTINEL = "SHOW_GROUP_INTRO"
_GROUP_INTRO_NO_ACTION_SENTINEL = "NO_GROUP_INTRO_PROTOCOL_ACTION"
_SLOW_HERMES_TURN_MS = 3_000
_PROTOCOL_SENTINELS = frozenset(
    {
        _ONBOARDING_SYNC_WAITING_SENTINEL,
        _ONBOARDING_CONTEXTUAL_CHAT_SENTINEL,
        _ONBOARDING_NO_REPLY_SENTINEL,
        _REVIEW_SHOW_PROMPT_SENTINEL,
        _REVIEW_NO_ACTION_SENTINEL,
        _GROUP_SHARE_EXECUTE_SENTINEL,
        _GROUP_SHARE_NO_ACTION_SENTINEL,
        _GROUP_INTRO_SHOW_SENTINEL,
        _GROUP_INTRO_NO_ACTION_SENTINEL,
    }
)


@dataclass(slots=True)
class FlorenceHouseholdChatReply:
    text: str


class FlorenceHouseholdChatService:
    """Wrap Hermes core for Florence household chat after onboarding."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        model: str,
        max_iterations: int = 6,
        provider: str = "auto",
        briefing_style: str = "plain",
        briefing_emoji_mode: str = "none",
        agent_factory: Callable[..., Any] | None = None,
        session_db: Any | None = None,
        now_getter: Callable[[], datetime] | None = None,
    ):
        self.store = store
        self.model = model
        self.max_iterations = max_iterations
        self.provider = provider.strip() if isinstance(provider, str) and provider.strip() else "auto"
        self.briefing_style = briefing_style.strip().lower() if isinstance(briefing_style, str) and briefing_style.strip() else "plain"
        self.briefing_emoji_mode = (
            briefing_emoji_mode.strip().lower()
            if isinstance(briefing_emoji_mode, str) and briefing_emoji_mode.strip()
            else "none"
        )
        self.agent_factory = agent_factory
        self.session_db = session_db or self._build_session_db()
        self.now_getter = now_getter or (lambda: datetime.now(timezone.utc))

    def respond(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        message_text: str,
        message_attachments: tuple[FlorenceInboundAttachment, ...] = (),
        conversation_history: list[ChannelMessage] | None = None,
    ) -> FlorenceHouseholdChatReply | None:
        system_message = self._build_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not system_message:
            return None
        history, session_id = self._load_conversation_history(
            channel_id=channel_id,
            fallback_messages=conversation_history or [],
        )
        result = self._run_agent_conversation(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=self._build_live_user_message(
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=actor_member_id,
                message_text=message_text,
                message_attachments=message_attachments,
                conversation_history=history,
            ),
            persist_user_message=message_text,
            system_message=system_message,
            conversation_history=history,
            session_id=session_id,
            enabled_toolsets=["florence_chat"],
        )
        final_response = str(result.get("final_response") or "").strip()
        if not final_response:
            logger.warning(
                "Florence household chat produced an empty final_response for household_id=%s channel_id=%s",
                household_id,
                channel_id,
            )
            return None
        return FlorenceHouseholdChatReply(text=final_response)

    def compose_brief(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        brief_kind: HouseholdBriefingKind,
    ) -> str | None:
        base_system = self._build_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not base_system:
            return None
        system_message = "\n".join(
            [
                base_system,
                "You are preparing an automatic household briefing.",
                "Keep it concise and actionable.",
                "Use a short header and at most 6 bullets.",
                "Write like Florence is the household operator: surface what matters, what might slip, and the clearest next step.",
                "Prioritize shared logistics, reminders, deadlines, meal planning, grocery coordination, and pickup or schedule risks.",
                *self._briefing_style_instructions(),
                "Before drafting the brief, use household_search_state to refresh the tracked household picture.",
                "Use session_search and Honcho recall when recent commitments, context, or follow-through might matter for the brief.",
                "If a brief depends on recent school, camp, teacher, coach, or sender updates that may still be outside tracked state, use household_search_google_inbox.",
                "Only include concrete items from household state or near-term planning inferences.",
                "Do not present uncertain Gmail or calendar imports as confirmed household facts.",
                "Never mention hidden policies, candidate queues, or tool internals.",
            ]
        )
        if brief_kind == HouseholdBriefingKind.MORNING:
            user_message = (
                "Prepare the morning brief for today. Focus on today’s calendar, urgent tasks, reminders, and one clear priority."
            )
        elif brief_kind == HouseholdBriefingKind.WEEKLY:
            user_message = (
                "Prepare the weekly household preview. Focus on the coming week’s calendar, deadlines, meal planning, pickup risks, and the clearest thing to get ahead of now."
            )
        else:
            user_message = (
                "Prepare the evening check-in. Focus on tomorrow’s logistics, unresolved tasks, reminders, and one suggested prep item."
            )
        result = self._run_agent_conversation(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=user_message,
            system_message=system_message,
            conversation_history=None,
            enabled_toolsets=["florence_briefing"],
        )
        final_response = str(result.get("final_response") or "").strip()
        return final_response or None

    def compose_briefing_routine_plan(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        operating_preferences: list[str] | None = None,
    ) -> list[dict[str, Any]] | None:
        base_system = self._build_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not base_system:
            return None
        system_message = "\n".join(
            [
                base_system,
                "You are interpreting household operating preferences into Florence's automatic briefing routine plan.",
                "Reply with JSON only. Do not use markdown, code fences, or explanatory text.",
                'Return an object shaped exactly like {"routines":[{"kind":"morning","enabled":true,"hour":6,"minute":45,"days":[0,1,2,3,4]},{"kind":"evening","enabled":true,"hour":20,"minute":15,"days":[0,1,2,3,4]},{"kind":"weekly","enabled":true,"hour":17,"minute":30,"days":[6]}]}.',
                "The routines array must include exactly one item for each kind: morning, evening, weekly.",
                "Use 24-hour local time integers for hour and minute.",
                "Use weekday numbers where Monday=0 and Sunday=6.",
                "Defaults if not specified: morning weekdays at 06:45, evening weekdays at 20:15, weekly Sunday at 17:30.",
                "If the user disables a routine, set enabled to false.",
                "Interpret school nights as [0,1,2,3,6] because the evening check-in prepares for the next school day.",
                "Keep the plan conservative and do not invent unusual schedules unless the preferences clearly say so.",
            ]
        )
        user_message = json.dumps(
            {
                "task": "plan_briefing_routines",
                "operating_preferences": [statement for statement in (operating_preferences or []) if str(statement).strip()],
            },
            ensure_ascii=True,
        )
        result = self._run_agent_conversation(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=user_message,
            system_message=system_message,
            conversation_history=None,
            enabled_toolsets=["florence_briefing"],
        )
        final_response = str(result.get("final_response") or "").strip()
        return self._parse_briefing_routine_plan(final_response)

    def _briefing_style_instructions(self) -> list[str]:
        lines = [
            "Write for iMessage/SMS in plain text. Do not rely on markdown, bold markers, or other rich-text formatting.",
            "Use everyday parent-facing language, not PM, admin, or ops jargon.",
            "Avoid words like 'underspecified', 'surface', 'risk posture', 'optimize', or 'unresolved items'.",
        ]
        if self.briefing_style == "warm":
            lines.append(
                "Keep the tone warm, calm, and competent. Sound like a helpful co-parenting operator, not a dashboard or life coach."
            )
        elif self.briefing_style == "neutral":
            lines.append(
                "Keep the tone calm, neutral, and matter-of-fact without sounding stiff."
            )
        else:
            lines.append(
                "Keep the tone calm, direct, and natural. Prefer plainspoken text a parent would actually send."
            )
        if self.briefing_emoji_mode == "minimal":
            lines.append("Use at most one emoji in the header if it genuinely helps. Do not use emojis in bullets.")
        else:
            lines.append("Do not use emojis.")
        return lines

    def compose_operator_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        kind: str,
        payload: dict[str, Any] | None = None,
        conversation_history: list[ChannelMessage] | None = None,
    ) -> str | None:
        base_system = self._build_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not base_system:
            return None
        payload = dict(payload or {})
        conversation_context = None
        if conversation_history is not None:
            conversation_context = self._build_conversation_history(conversation_history)
        enabled_toolsets = ["florence_briefing"]
        max_iterations = self.max_iterations

        if kind == "activation_brief":
            system_message = "\n".join(
                [
                    base_system,
                    "You are preparing Florence's first activation brief after the initial Google sync finishes.",
                    "Keep it calm, concise, and operator-like.",
                    "Write at most 5 short bullets or short paragraphs.",
                    "Lead with the 1-3 household facts or possible slips that matter most.",
                    "Collapse duplicate raw artifacts into one underlying household fact when they refer to the same appointment, event, reminder, or thread.",
                    "Do not say 'items need review', 'candidate queue', 'I scanned X emails', or other pipeline language unless it is truly essential.",
                    "Do not dump raw repeated titles.",
                    "If something looks important but still uncertain, phrase it as something Florence wants to double-check, not as a confirmed fact.",
                    "Do not mention tool internals, sync phases, or ingestion mechanics.",
                    "End with one short natural invitation for what the parent can ask next. Do not use command-style product UI language.",
                ]
            )
            user_message = json.dumps(
                {
                    "task": "compose_initial_sync_activation_brief",
                    "gmail_count": int(payload.get("gmail_count") or 0),
                    "calendar_count": int(payload.get("calendar_count") or 0),
                    "candidates": list(payload.get("candidates") or []),
                },
                ensure_ascii=True,
            )
        elif kind == "sync_update_brief":
            system_message = "\n".join(
                [
                    base_system,
                    "You are preparing a short Florence household update after a later Gmail and Calendar sync pass finishes.",
                    "Summarize what changed since the prior notified sync snapshot when previous_sync is provided.",
                    "If previous_sync is sparse, summarize what this sync pass surfaced without pretending you know an exact delta.",
                    "Keep it calm, concise, and operator-like.",
                    "Write at most 4 short bullets or short paragraphs.",
                    "Lead with the 1-2 changes or possible slips that matter most.",
                    "If Florence surfaced something to review or double-check, describe the underlying household item in natural language.",
                    "Do not say 'candidate queue', 'scan counts', 'pipeline', or other tool internals unless truly essential.",
                    "Do not claim exact numeric change deltas unless the payload clearly supports them.",
                    "End with one short natural invitation for what the parent can ask next.",
                ]
            )
            user_message = json.dumps(
                {
                    "task": "compose_sync_update_brief",
                    "previous_sync": dict(payload.get("previous_sync") or {}),
                    "current_sync": dict(payload.get("current_sync") or {}),
                },
                ensure_ascii=True,
            )
        elif kind == "onboarding_turn":
            enabled_toolsets = ["florence_onboarding"]
            max_iterations = self._onboarding_max_iterations(payload)
            system_message = self._build_onboarding_turn_system_message(
                self._build_onboarding_system_message(
                    household_id=household_id,
                    channel_id=channel_id,
                    actor_member_id=actor_member_id,
                )
            )
            user_message = self._build_onboarding_turn_user_message(
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=actor_member_id,
                payload=payload,
            )
        elif kind == "review_prompt":
            system_message = "\n".join(
                [
                    base_system,
                    "You are preparing a short Florence review prompt for one possible household item.",
                    "Keep it calm, concise, and plainspoken.",
                    "Do not say 'Imported item', 'candidate', 'queue', or other pipeline language.",
                    "Summarize the underlying household fact or question in normal language.",
                    "If trigger is scheduled_review_sweep and pending_review_count is greater than 1, briefly note that Florence still has a few items waiting after this one.",
                    "If the item is uncertain, frame it as something Florence wants to double-check before adding.",
                    "If there is source-sharing guidance, include it naturally in one short line.",
                    "End exactly with: Reply yes if I should add it, no if it's wrong, or skip for later.",
                ]
            )
            user_message = json.dumps(
                {
                    "task": "compose_review_prompt",
                    "candidate": dict(payload.get("candidate") or {}),
                    "source_prompt": str(payload.get("source_prompt") or "").strip(),
                    "pending_review_count": int(payload.get("pending_review_count") or 0),
                    "trigger": str(payload.get("trigger") or "").strip(),
                },
                ensure_ascii=True,
            )
        elif kind == "review_queue_turn":
            system_message = "\n".join(
                [
                    base_system,
                    "You are deciding whether Florence should surface the one currently available private review item in this DM.",
                    f"If the user is explicitly asking to review pending imports, the review queue, or what needs review now, reply exactly {_REVIEW_SHOW_PROMPT_SENTINEL}.",
                    f"Otherwise reply exactly {_REVIEW_NO_ACTION_SENTINEL}.",
                    "Do not answer the underlying household question here.",
                    "If prompt_armed is true, the review item is already surfaced, so do not re-surface it from this decision step.",
                ]
            )
            user_message = json.dumps(
                {
                    "task": "review_queue_turn_decision",
                    "user_message": str(payload.get("user_message") or "").strip(),
                    "prompt_armed": bool(payload.get("prompt_armed")),
                    "rendered_prompt_text": str(payload.get("rendered_prompt_text") or "").strip(),
                    "candidate": dict(payload.get("candidate") or {}),
                },
                ensure_ascii=True,
            )
        elif kind == "sync_waiting":
            data_dependent = bool(payload.get("data_dependent"))
            system_message = "\n".join(
                [
                    base_system,
                    "You are writing a short Florence message while the first Gmail and Calendar sync is still running.",
                    "Keep it calm, concise, and reassuring.",
                    "Do not replay the active onboarding question unless the user explicitly asks for it.",
                    "Do not mention candidate queues, scan counts, pipeline phases, or tool internals.",
                    "Do not sound like a background job log or admin console.",
                    "Keep it to one short text-sized reply.",
                    (
                        "The user is asking for inbox or calendar-dependent information, so explain that Florence is still syncing before it can answer confidently from that data."
                        if data_dependent
                        else "The user mainly wants sync status or wants to know what Florence can do while the sync runs."
                    ),
                ]
            )
            user_message = json.dumps(
                {
                    "task": "compose_sync_waiting_reply",
                    "user_message": str(payload.get("user_message") or "").strip(),
                    "data_dependent": data_dependent,
                },
                ensure_ascii=True,
            )
        elif kind == "sync_started":
            system_message = "\n".join(
                [
                    base_system,
                    "You are writing a short Florence message while the first Gmail and Calendar sync is still running.",
                    "Keep it calm, concise, and reassuring.",
                    "Do not replay the active onboarding question unless the user explicitly asks for it.",
                    "Do not mention candidate queues, scan counts, pipeline phases, or tool internals.",
                    "Do not sound like a background job log or admin console.",
                    "Keep it to one short text-sized reply.",
                    "Google just connected successfully. Florence is sending a quiet background-status update into the DM thread.",
                    "Acknowledge that sync is running in the background and say Florence will text when the first pass is ready.",
                ]
            )
            user_message = json.dumps(
                {
                    "task": "compose_google_sync_started_update",
                },
                ensure_ascii=True,
            )
        elif kind == "group_share_turn":
            system_message = "\n".join(
                [
                    base_system,
                    "You are deciding whether a private parent-DM message is an explicit request to share recent DM context into the household parent group.",
                    f"If the user is clearly asking Florence to share, send, or post the recent DM update to the parent group, reply exactly {_GROUP_SHARE_EXECUTE_SENTINEL}.",
                    f"Otherwise reply exactly {_GROUP_SHARE_NO_ACTION_SENTINEL}.",
                    "Bare links, screenshots, attachments, schedule feeds, webcal:// links, and .ics calendar URLs are not group-share requests by themselves.",
                    "Follow-ups like 'here is the calendar link', 'here is the schedule', or 'use this feed for Theo' are normal household-task turns unless they explicitly mention sharing to the group.",
                    "Do not confuse this with source-visibility choices inside a review-item prompt.",
                    "If the latest assistant protocol kind is candidate_review_prompt, plain replies like share/private usually belong to review handling, not group promotion.",
                ]
            )
            user_message = json.dumps(
                {
                    "task": "group_share_turn_decision",
                    "user_message": str(payload.get("user_message") or "").strip(),
                    "latest_assistant_protocol_kind": str(payload.get("latest_assistant_protocol_kind") or "").strip(),
                },
                ensure_ascii=True,
            )
        elif kind == "group_intro_turn":
            system_message = "\n".join(
                [
                    base_system,
                    "You are deciding whether Florence should send the lightweight first-time intro for the household parent group.",
                    f"If this message is just an opening greeting or simple hello into the newly active parent group, reply exactly {_GROUP_INTRO_SHOW_SENTINEL}.",
                    f"Otherwise reply exactly {_GROUP_INTRO_NO_ACTION_SENTINEL}.",
                    "Do not answer the underlying household question here.",
                    "If the message already contains a substantive request, planning question, or task, do not use the intro.",
                ]
            )
            user_message = json.dumps(
                {
                    "task": "group_intro_turn_decision",
                    "user_message": str(payload.get("user_message") or "").strip(),
                },
                ensure_ascii=True,
            )
        elif kind == "group_promotion":
            system_message = "\n".join(
                [
                    base_system,
                    "You are preparing a short group-safe household update from a private parent DM.",
                    "Only include concrete shared logistics, tasks, reminders, meals, grocery items, calendar changes, or plans the other parent should know.",
                    "Do not include raw feelings, therapy-like language, health-sensitive details, vulnerable wording, or anything that belongs only in a private DM.",
                    "If there is nothing appropriate to share with the parent group, reply exactly NO_GROUP_SHARE.",
                    "Keep the result concise and ready to send as a standalone household update.",
                ]
            )
            user_message = (
                "Turn this recent private DM exchange into a short parent-group update if appropriate:\n\n"
                f"{str(payload.get('source_text') or '').strip()}"
            )
        else:
            raise ValueError(f"unsupported_operator_message_kind: {kind}")

        result = self._run_agent_conversation(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=user_message,
            system_message=system_message,
            conversation_history=conversation_context,
            enabled_toolsets=enabled_toolsets,
            max_iterations=max_iterations,
            turn_kind=kind,
            internal_turn=True,
        )
        final_response = str(result.get("final_response") or "").strip()
        if kind == "group_promotion" and final_response == "NO_GROUP_SHARE":
            return None
        return final_response

    @staticmethod
    def _parse_briefing_routine_plan(response_text: str) -> list[dict[str, Any]] | None:
        defaults = {
            "morning": {"kind": "morning", "enabled": True, "hour": 6, "minute": 45, "days": [0, 1, 2, 3, 4]},
            "evening": {"kind": "evening", "enabled": True, "hour": 20, "minute": 15, "days": [0, 1, 2, 3, 4]},
            "weekly": {"kind": "weekly", "enabled": True, "hour": 17, "minute": 30, "days": [6]},
        }
        payload = FlorenceHouseholdChatService._load_json_object(response_text)
        if not isinstance(payload, dict):
            return None
        raw_routines = payload.get("routines")
        if not isinstance(raw_routines, list):
            return None

        parsed: dict[str, dict[str, Any]] = {}
        for item in raw_routines:
            if not isinstance(item, dict):
                continue
            kind = str(item.get("kind") or "").strip().lower()
            default = defaults.get(kind)
            if default is None:
                continue
            enabled = bool(item.get("enabled", default["enabled"]))
            try:
                hour = int(item.get("hour", default["hour"]))
                minute = int(item.get("minute", default["minute"]))
            except (TypeError, ValueError):
                hour = int(default["hour"])
                minute = int(default["minute"])
            if not (0 <= hour <= 23):
                hour = int(default["hour"])
            if not (0 <= minute <= 59):
                minute = int(default["minute"])
            days: list[int] = []
            for raw_day in item.get("days", default["days"]) if isinstance(item.get("days", default["days"]), list) else default["days"]:
                try:
                    day = int(raw_day)
                except (TypeError, ValueError):
                    continue
                if 0 <= day <= 6 and day not in days:
                    days.append(day)
            if not days:
                days = list(default["days"])
            parsed[kind] = {
                "kind": kind,
                "enabled": enabled,
                "hour": hour,
                "minute": minute,
                "days": days,
            }

        if not parsed:
            return None
        return [dict(parsed.get(kind, defaults[kind])) for kind in ("morning", "evening", "weekly")]

    @staticmethod
    def _load_json_object(response_text: str) -> Any | None:
        cleaned = str(response_text or "").strip()
        if not cleaned:
            return None
        candidates = [cleaned]
        if cleaned.startswith("```"):
            stripped = cleaned.strip("`").strip()
            first_newline = stripped.find("\n")
            if first_newline >= 0:
                candidates.append(stripped[first_newline + 1 :].strip())
        for candidate in candidates:
            try:
                return json.loads(candidate)
            except Exception:
                continue
        return None

    def compose_onboarding_turn(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        payload: dict[str, Any] | None = None,
        conversation_history: list[ChannelMessage] | None = None,
    ) -> tuple[str, ...] | None:
        base_system = self._build_onboarding_system_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not base_system:
            return None
        payload = dict(payload or {})
        conversation_context = None
        if conversation_history is not None:
            conversation_context = self._build_conversation_history(conversation_history)

        system_message = self._build_onboarding_turn_system_message(base_system)
        user_message = self._build_onboarding_turn_user_message(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            payload=payload,
        )
        result = self._run_agent_conversation(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            user_message=user_message,
            system_message=system_message,
            conversation_history=conversation_context,
            enabled_toolsets=["florence_onboarding"],
            max_iterations=self._onboarding_max_iterations(payload),
            turn_kind="onboarding_turn",
            internal_turn=True,
        )
        tool_reply_messages = self._extract_onboarding_reply_messages(result)
        if tool_reply_messages:
            return tool_reply_messages
        final_response = str(result.get("final_response") or "").strip()
        if not final_response:
            return None
        return (final_response,)

    @staticmethod
    def _build_onboarding_turn_system_message(base_system: str) -> str:
        return "\n".join(
            [
                base_system,
                "You are handling one parent-DM onboarding turn before Florence transitions into normal household chat.",
                "Treat the payload as the authoritative onboarding context for this DM thread.",
                "Use household_apply_onboarding_update to store only explicit setup facts the parent actually provided in this message.",
                "Do not infer unstated names, ages, schools, activities, or Google connection status.",
                "Do not use unrelated Florence write tools in this turn.",
                "When Google is connected and the current onboarding question is about school or activities, you may use household_search_google_inbox or household_search_google_calendar to recover newly synced context for that exact missing field.",
                "If recent_google_context already contains a likely answer, use it to ask a short confirmation or to continue the turn more intelligently instead of asking the user to restate everything from scratch.",
                "If the parent says Florence should already have that answer from email or calendar, treat that as permission to check the connected Google context for the current onboarding question.",
                (
                    f"If Google is already connected and the message is asking for inbox or calendar dependent information that still requires the first sync to finish, "
                    f"reply exactly {_ONBOARDING_SYNC_WAITING_SENTINEL}."
                ),
                (
                    f"If Google is already connected and the message is a sync-status question or a general household question that can continue while sync runs, "
                    f"reply exactly {_ONBOARDING_CONTEXTUAL_CHAT_SENTINEL}."
                ),
                (
                    f"If Google is already connected and the message is only a brief acknowledgement like ok, sounds good, or thanks with no substantive request, "
                    f"reply exactly {_ONBOARDING_NO_REPLY_SENTINEL}."
                ),
                "If the user did not provide a concrete storable onboarding fact, do not call a write tool. Ask one short follow-up or restate the next onboarding question naturally.",
                "If household_apply_onboarding_update returns reply_messages, base your reply on that result so Florence stays aligned with the deterministic onboarding state machine.",
                "Keep the reply short, plainspoken, and specific to the next onboarding step.",
                "Do not mention tools, hidden state, or backend mechanics.",
            ]
        )

    def _build_onboarding_system_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
    ) -> str:
        household = self.store.get_household(household_id)
        if household is None:
            return ""
        actor_name = None
        if actor_member_id:
            member = self.store.get_member(actor_member_id)
            if member is not None:
                actor_name = member.display_name
        scope = resolve_conversation_scope(
            self.store,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        lines = [
            "You are Florence, the Hermes-powered household onboarding agent for this private parent DM.",
            "This lane is only for setup. Florence household state is the source of truth.",
            "Keep this turn narrow: interpret the user's setup reply, store explicit facts, and move to the next onboarding step.",
            "Do not browse, research, or use general household planning behavior in this lane.",
            "Use household_apply_onboarding_update for onboarding writes.",
            "Only use household_search_google_inbox or household_search_google_calendar when Google is connected and they are directly helping with the one current missing onboarding field.",
            f"Household: {household.name}",
            f"Timezone: {household.timezone}",
        ]
        lines.extend(build_scope_model_lines(scope=scope))
        lines.append("Channel context: this is an active private parent-DM onboarding thread.")
        if actor_name:
            lines.append(f"Current speaker: {actor_name}")
        return "\n".join(lines)

    def _build_onboarding_turn_user_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        payload: dict[str, Any],
    ) -> str:
        return json.dumps(
            {
                "task": "handle_onboarding_turn",
                "user_message": str(payload.get("user_message") or "").strip(),
                "stage": str(payload.get("stage") or "").strip(),
                "google_connected": bool(payload.get("google_connected")),
                "parent_display_name": payload.get("parent_display_name"),
                "child_names": list(payload.get("child_names") or []),
                "child_profiles": list(payload.get("child_profiles") or []),
                "current_child_name": payload.get("current_child_name"),
                "next_prompt": payload.get("next_prompt"),
                "recent_google_context": self._build_onboarding_recent_google_context(
                    household_id=household_id,
                    channel_id=channel_id,
                    actor_member_id=actor_member_id,
                    payload=payload,
                ),
            },
            ensure_ascii=True,
        )

    @staticmethod
    def _onboarding_google_lookup_stage(payload: dict[str, Any]) -> bool:
        stage = str(payload.get("stage") or "").strip()
        return bool(payload.get("google_connected")) and stage in {"collect_child_school", "collect_child_activities"}

    def _onboarding_max_iterations(self, payload: dict[str, Any]) -> int:
        return min(self.max_iterations, 3 if self._onboarding_google_lookup_stage(payload) else 2)

    def _build_onboarding_recent_google_context(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not self._onboarding_google_lookup_stage(payload):
            return None
        focus_child_name = str(payload.get("current_child_name") or "").strip()
        if not focus_child_name:
            child_names = [str(name).strip() for name in payload.get("child_names") or [] if str(name).strip()]
            focus_child_name = child_names[0] if len(child_names) == 1 else ""
        if not focus_child_name:
            return None

        inbox_scope = resolve_google_inbox_scope(
            self.store,
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            query=focus_child_name,
            sender=None,
            subject=None,
        )
        calendar_scope = resolve_google_calendar_scope(
            self.store,
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            query=focus_child_name,
            calendar_summary=None,
        )

        gmail_matches: list[dict[str, Any]] = []
        if not inbox_scope.error and inbox_scope.connections:
            for item in self.store.search_google_gmail_messages(
                household_id=household_id,
                connection_ids=[connection.id for connection in inbox_scope.connections],
                query=focus_child_name,
                newer_than_days=365,
                limit=3,
            ):
                gmail_matches.append(
                    {
                        "from_address": item.from_address,
                        "subject": item.subject,
                        "snippet": item.snippet,
                        "received_at": item.received_at.isoformat() if item.received_at is not None else None,
                    }
                )

        calendar_matches: list[dict[str, Any]] = []
        if not calendar_scope.error and calendar_scope.connections:
            for item in self.store.search_google_calendar_events(
                household_id=household_id,
                connection_ids=[connection.id for connection in calendar_scope.connections],
                query=focus_child_name,
                newer_than_days=365,
                limit=3,
            ):
                calendar_matches.append(
                    {
                        "title": item.title,
                        "calendar_summary": item.calendar_summary,
                        "starts_at": item.starts_at.isoformat() if item.starts_at is not None else None,
                        "ends_at": item.ends_at.isoformat() if item.ends_at is not None else None,
                    }
                )

        connection_statuses = []
        for connection in {connection.id: connection for connection in [*(inbox_scope.connections or []), *(calendar_scope.connections or [])]}.values():
            metadata = dict(connection.metadata) if isinstance(connection.metadata, dict) else {}
            connection_statuses.append(
                {
                    "email": connection.email,
                    "initial_sync_state": metadata.get("initial_sync_state"),
                    "last_sync_status": metadata.get("last_sync_status"),
                    "sync_phase": metadata.get("sync_phase"),
                }
            )

        mirror_sync_running = any(
            status.get("initial_sync_state") == "running" or status.get("last_sync_status") == "running"
            for status in connection_statuses
        )
        if not gmail_matches and not calendar_matches and not mirror_sync_running:
            return None
        return {
            "focus_child_name": focus_child_name,
            "gmail_matches": gmail_matches,
            "calendar_matches": calendar_matches,
            "mirror_sync_running": mirror_sync_running,
            "connection_statuses": connection_statuses,
        }

    def _run_agent_conversation(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        user_message: Any,
        persist_user_message: str | None = None,
        system_message: str,
        conversation_history: list[dict[str, str]] | None,
        session_id: str | None = None,
        enabled_toolsets: list[str],
        max_iterations: int | None = None,
        turn_kind: str = "chat",
        internal_turn: bool = False,
    ) -> dict[str, Any]:
        task_id = f"florence-household-{uuid.uuid4()}"
        result: dict[str, Any] | None = None
        error: Exception | None = None
        turn_started = perf_counter()
        resolved_max_iterations = max_iterations if max_iterations is not None else self.max_iterations
        resolved_session_id = session_id
        resolved_skip_memory = False
        resolved_honcho_session_key = self._build_honcho_session_key(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if internal_turn:
            resolved_session_id = session_id or (
                f"{self._default_session_id(channel_id)}-internal-{turn_kind}-{uuid.uuid4().hex[:8]}"
            )
            resolved_skip_memory = True
            resolved_honcho_session_key = None

        agent_factory = self.agent_factory
        if agent_factory is None:
            from run_agent import AIAgent

            agent_factory = AIAgent
        from tools.florence_household_tool import (
            clear_household_tool_context,
            set_household_tool_context,
        )
        set_household_tool_context(
            task_id,
            store=self.store,
            household_id=household_id,
            actor_member_id=actor_member_id,
            channel_id=channel_id,
            household_chat_service=self,
        )
        try:
            agent = agent_factory(
                model=self.model,
                max_iterations=resolved_max_iterations,
                provider=self.provider,
                enabled_toolsets=enabled_toolsets,
                quiet_mode=True,
                skip_memory=resolved_skip_memory,
                skip_local_memory=True,
                skip_context_files=False,
                platform="florence",
                session_id=resolved_session_id,
                session_db=self.session_db,
                honcho_session_key=resolved_honcho_session_key,
                session_search_kwargs=self._build_session_search_kwargs(
                    household_id=household_id,
                    channel_id=channel_id,
                    actor_member_id=actor_member_id,
                ),
            )
            run_kwargs = {
                "user_message": user_message,
                "system_message": system_message,
                "conversation_history": conversation_history,
                "task_id": task_id,
            }
            if persist_user_message is not None:
                run_kwargs["persist_user_message"] = persist_user_message
            try:
                result = agent.run_conversation(**run_kwargs)
            except TypeError as exc:
                if "persist_user_message" not in str(exc):
                    raise
                run_kwargs.pop("persist_user_message", None)
                result = agent.run_conversation(**run_kwargs)
            if not internal_turn:
                self._persist_channel_session_id(
                    channel_id=channel_id,
                    session_id=str(getattr(agent, "session_id", "") or "").strip(),
                )
            return result
        except Exception as exc:
            error = exc
            raise
        finally:
            self._log_agent_turn(
                turn_kind=turn_kind,
                household_id=household_id,
                channel_id=channel_id,
                actor_member_id=actor_member_id,
                enabled_toolsets=enabled_toolsets,
                max_iterations=resolved_max_iterations,
                conversation_history=conversation_history,
                duration_ms=int((perf_counter() - turn_started) * 1000),
                result=result,
                error=error,
            )
            clear_household_tool_context(task_id)

    def _log_agent_turn(
        self,
        *,
        turn_kind: str,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        enabled_toolsets: list[str],
        max_iterations: int,
        conversation_history: list[dict[str, str]] | None,
        duration_ms: int,
        result: dict[str, Any] | None,
        error: Exception | None,
    ) -> None:
        tool_calls = self._extract_tool_call_names(result)
        onboarding_reply_messages = (
            len(self._extract_onboarding_reply_messages(result))
            if turn_kind == "onboarding_turn"
            else 0
        )
        final_response_preview = self._preview_text(
            result.get("final_response") if isinstance(result, dict) else None
        )
        log_level = logging.WARNING if error is not None or duration_ms >= _SLOW_HERMES_TURN_MS else logging.INFO
        logger.log(
            log_level,
            "Florence Hermes turn kind=%s household_id=%s channel_id=%s actor_member_id=%s duration_ms=%s history_messages=%s toolsets=%s max_iterations=%s tool_calls=%s onboarding_reply_messages=%s final_response=%s error=%s",
            turn_kind,
            household_id,
            channel_id,
            actor_member_id,
            duration_ms,
            len(conversation_history or []),
            ",".join(enabled_toolsets),
            max_iterations,
            ",".join(tool_calls) if tool_calls else "-",
            onboarding_reply_messages,
            final_response_preview or "-",
            str(error) if error is not None else "-",
        )
        if self._verbose_turn_logging_enabled():
            logger.info(
                "Florence Hermes turn detail kind=%s assistant_messages=%s last_reasoning=%s",
                turn_kind,
                json.dumps(self._assistant_message_summaries(result), ensure_ascii=False),
                self._preview_text(
                    result.get("last_reasoning") if isinstance(result, dict) else None,
                    limit=400,
                )
                or "-",
            )

    @staticmethod
    def looks_like_protocol_sentinel(reply_text: str | None) -> bool:
        normalized = str(reply_text or "").strip()
        return normalized in _PROTOCOL_SENTINELS

    def _build_live_user_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        message_text: str,
        message_attachments: tuple[FlorenceInboundAttachment, ...],
        conversation_history: list[dict[str, str]] | None,
    ) -> Any:
        recent_google_context = self._build_recent_google_context(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            message_text=message_text,
            conversation_history=conversation_history,
        )
        image_parts = []
        for attachment in message_attachments:
            source = str(attachment.data_url or attachment.url or "").strip()
            mime_type = str(attachment.mime_type or "").strip().lower()
            if not source:
                continue
            if attachment.kind != "image" and not mime_type.startswith("image/"):
                continue
            image_parts.append(
                {
                    "type": "image_url",
                    "image_url": {"url": source},
                }
            )
        if not image_parts and recent_google_context is None:
            return message_text
        if not image_parts:
            return json.dumps(
                {
                    "task": "handle_live_household_turn",
                    "user_message": message_text,
                    "recent_google_context": recent_google_context,
                },
                ensure_ascii=True,
            )
        return [
            {
                "type": "text",
                "text": json.dumps(
                    {
                        "task": "handle_live_household_turn",
                        "user_message": message_text,
                        "recent_google_context": recent_google_context,
                    },
                    ensure_ascii=True,
                ),
            },
            *image_parts,
        ]

    def _build_recent_google_context(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        message_text: str,
        conversation_history: list[dict[str, str]] | None,
    ) -> dict[str, Any] | None:
        scope = resolve_conversation_scope(
            self.store,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if not scope.is_private_parent_dm:
            return None
        query = self._build_recent_google_query(message_text=message_text, conversation_history=conversation_history)
        if not query:
            return None

        inbox_scope = resolve_google_inbox_scope(
            self.store,
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            query=query,
            sender=None,
            subject=None,
        )
        calendar_scope = resolve_google_calendar_scope(
            self.store,
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
            query=query,
            calendar_summary=None,
        )

        gmail_matches: list[dict[str, Any]] = []
        if not inbox_scope.error and inbox_scope.connections:
            for item in self.store.search_google_gmail_messages(
                household_id=household_id,
                connection_ids=[connection.id for connection in inbox_scope.connections],
                query=query,
                newer_than_days=365,
                limit=3,
            ):
                gmail_matches.append(
                    {
                        "from_address": item.from_address,
                        "subject": item.subject,
                        "snippet": item.snippet,
                        "received_at": item.received_at.isoformat() if item.received_at is not None else None,
                    }
                )

        calendar_matches: list[dict[str, Any]] = []
        if not calendar_scope.error and calendar_scope.connections:
            for item in self.store.search_google_calendar_events(
                household_id=household_id,
                connection_ids=[connection.id for connection in calendar_scope.connections],
                query=query,
                newer_than_days=365,
                limit=3,
            ):
                calendar_matches.append(
                    {
                        "title": item.title,
                        "calendar_summary": item.calendar_summary,
                        "starts_at": item.starts_at.isoformat() if item.starts_at is not None else None,
                        "ends_at": item.ends_at.isoformat() if item.ends_at is not None else None,
                    }
                )

        connection_statuses = []
        combined_connections = {
            connection.id: connection
            for connection in [*(inbox_scope.connections or []), *(calendar_scope.connections or [])]
        }
        for connection in combined_connections.values():
            metadata = dict(connection.metadata) if isinstance(connection.metadata, dict) else {}
            connection_statuses.append(
                {
                    "email": connection.email,
                    "initial_sync_state": metadata.get("initial_sync_state"),
                    "last_sync_status": metadata.get("last_sync_status"),
                    "sync_phase": metadata.get("sync_phase"),
                }
            )

        mirror_sync_running = any(
            status.get("initial_sync_state") == "running" or status.get("last_sync_status") == "running"
            for status in connection_statuses
        )
        if not gmail_matches and not calendar_matches and not mirror_sync_running:
            return None
        return {
            "query_basis": query,
            "gmail_matches": gmail_matches,
            "calendar_matches": calendar_matches,
            "mirror_sync_running": mirror_sync_running,
            "connection_statuses": connection_statuses,
        }

    @staticmethod
    def _build_recent_google_query(
        *,
        message_text: str,
        conversation_history: list[dict[str, str]] | None,
    ) -> str:
        parts: list[str] = []
        current = " ".join(str(message_text or "").split()).strip()
        if current:
            parts.append(current)
        if conversation_history:
            recent = conversation_history[-4:]
            for item in recent:
                role = str(item.get("role") or "").strip().lower()
                if role not in {"user", "assistant"}:
                    continue
                content = " ".join(str(item.get("content") or "").split()).strip()
                if content:
                    parts.append(content)
        combined = " ".join(parts).strip()
        if len(combined) > 500:
            combined = combined[:500]
        return combined

    @staticmethod
    def _verbose_turn_logging_enabled() -> bool:
        return str(os.getenv("FLORENCE_HERMES_VERBOSE_LOGGING") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    @staticmethod
    def _preview_text(value: Any, *, limit: int = 200) -> str:
        text = " ".join(str(value or "").split())
        if not text:
            return ""
        if len(text) <= limit:
            return text
        return f"{text[: limit - 3]}..."

    @classmethod
    def _assistant_message_summaries(cls, result: dict[str, Any] | None) -> list[dict[str, Any]]:
        if not isinstance(result, dict):
            return []
        summaries: list[dict[str, Any]] = []
        for message in result.get("messages") or []:
            if not isinstance(message, dict) or message.get("role") != "assistant":
                continue
            tool_calls = []
            for tool_call in message.get("tool_calls") or []:
                if not isinstance(tool_call, dict):
                    continue
                function = tool_call.get("function")
                if not isinstance(function, dict):
                    continue
                name = str(function.get("name") or "").strip()
                if name:
                    tool_calls.append(name)
            summaries.append(
                {
                    "tool_calls": tool_calls,
                    "content": cls._preview_text(message.get("content")),
                }
            )
        return summaries

    @staticmethod
    def _extract_tool_call_names(result: dict[str, Any] | None) -> tuple[str, ...]:
        if not isinstance(result, dict):
            return ()
        names: list[str] = []
        seen: set[str] = set()
        for message in result.get("messages") or []:
            if not isinstance(message, dict) or message.get("role") != "assistant":
                continue
            for tool_call in message.get("tool_calls") or []:
                if not isinstance(tool_call, dict):
                    continue
                function = tool_call.get("function")
                if not isinstance(function, dict):
                    continue
                name = str(function.get("name") or "").strip()
                if name and name not in seen:
                    seen.add(name)
                    names.append(name)
        return tuple(names)

    @staticmethod
    def _extract_onboarding_reply_messages(result: dict[str, Any]) -> tuple[str, ...]:
        raw_messages = result.get("messages")
        if not isinstance(raw_messages, list):
            return ()
        for message in reversed(raw_messages):
            if not isinstance(message, dict) or message.get("role") != "tool":
                continue
            content = message.get("content")
            if not isinstance(content, str):
                continue
            try:
                payload = json.loads(content)
            except Exception:
                continue
            raw_reply_messages = ((payload.get("result") or {}) if isinstance(payload, dict) else {}).get("reply_messages")
            if not isinstance(raw_reply_messages, list):
                continue
            reply_messages = tuple(
                item.strip()
                for item in (str(value) for value in raw_reply_messages)
                if item.strip()
            )
            if reply_messages:
                return reply_messages
        return ()

    @staticmethod
    def _build_session_db() -> Any | None:
        try:
            from hermes_state import SessionDB

            return SessionDB()
        except Exception:
            return None

    def _default_session_id(self, channel_id: str) -> str:
        return f"florence-channel-{channel_id}"

    def _current_channel_session_id(self, channel_id: str) -> str:
        channel = self.store.get_channel(channel_id)
        if channel is None:
            return self._default_session_id(channel_id)
        metadata = dict(channel.metadata) if isinstance(channel.metadata, dict) else {}
        stored = str(metadata.get("hermes_session_id") or "").strip()
        return stored or self._default_session_id(channel_id)

    def _resolve_session_root(self, session_id: str | None) -> str | None:
        cleaned = str(session_id or "").strip()
        if not cleaned:
            return None
        if self.session_db is None:
            return cleaned
        visited: set[str] = set()
        resolved = cleaned
        current = cleaned
        while current and current not in visited:
            visited.add(current)
            resolved = current
            try:
                session = self.session_db.get_session(current)
            except Exception:
                logger.debug("Florence SessionDB lineage lookup failed for session_id=%s", current, exc_info=True)
                break
            if not session:
                break
            parent = str(session.get("parent_session_id") or "").strip()
            if not parent:
                break
            current = parent
        return resolved

    def _build_session_search_kwargs(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
    ) -> dict[str, Any]:
        allowed_session_ids: set[str] = set()
        for channel in self._session_search_channels(
            household_id=household_id,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        ):
            root = self._resolve_session_root(self._current_channel_session_id(channel.id))
            if root:
                allowed_session_ids.add(root)
        if not allowed_session_ids:
            root = self._resolve_session_root(self._current_channel_session_id(channel_id))
            if root:
                allowed_session_ids.add(root)
        return {
            "source_filter": ["florence"],
            "allowed_session_ids": sorted(allowed_session_ids),
        }

    def _session_search_channels(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
    ) -> list[Channel]:
        current_channel = self.store.get_channel(channel_id)
        if current_channel is None:
            return []
        household_channels = self.store.list_channels(household_id=household_id)
        scope = resolve_conversation_scope(
            self.store,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        if scope.is_shared_household_group:
            shared_channels = [
                channel
                for channel in household_channels
                if channel.channel_type == ChannelType.HOUSEHOLD_GROUP
            ]
            return shared_channels or [current_channel]

        if scope.is_private_parent_dm:
            member_handles = self._member_identity_handles(actor_member_id=actor_member_id)
            visible_channels: list[Channel] = []
            for channel in household_channels:
                if channel.channel_type == ChannelType.HOUSEHOLD_GROUP:
                    visible_channels.append(channel)
                    continue
                if channel.id == channel_id:
                    visible_channels.append(channel)
                    continue
                if channel.channel_type != ChannelType.PARENT_DM:
                    continue
                sender_handle = self._channel_sender_handle(channel)
                if sender_handle and sender_handle in member_handles:
                    visible_channels.append(channel)
            return visible_channels or [current_channel]

        return [current_channel]

    def _member_identity_handles(self, *, actor_member_id: str | None) -> set[str]:
        if not actor_member_id:
            return set()
        return {
            str(identity.normalized_value).strip()
            for identity in self.store.list_member_identities(actor_member_id)
            if str(identity.normalized_value).strip()
        }

    @staticmethod
    def _channel_sender_handle(channel: Any) -> str | None:
        metadata = dict(channel.metadata) if isinstance(channel.metadata, dict) else {}
        sender_handle = str(metadata.get("sender_handle") or "").strip()
        return sender_handle or None

    def _build_honcho_session_key(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
    ) -> str:
        channel = self.store.get_channel(channel_id)
        scope = resolve_conversation_scope(
            self.store,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        channel_type = scope.channel_type
        if channel_type == ChannelType.HOUSEHOLD_GROUP:
            return f"florence:household:{household_id}"
        if actor_member_id:
            return f"florence:member:{household_id}:{actor_member_id}"
        return f"florence:channel:{household_id}:{channel_id}"

    def _persist_channel_session_id(self, *, channel_id: str, session_id: str | None) -> None:
        cleaned = str(session_id or "").strip()
        if not cleaned:
            return
        channel = self.store.get_channel(channel_id)
        if channel is None:
            return
        metadata = dict(channel.metadata) if isinstance(channel.metadata, dict) else {}
        if str(metadata.get("hermes_session_id") or "").strip() == cleaned:
            return
        metadata["hermes_session_id"] = cleaned
        self.store.upsert_channel(replace(channel, metadata=metadata))

    def _load_conversation_history(
        self,
        *,
        channel_id: str,
        fallback_messages: list[ChannelMessage],
    ) -> tuple[list[dict[str, str]], str]:
        session_id = self._current_channel_session_id(channel_id)
        if self.session_db is not None:
            try:
                transcript = self.session_db.get_messages_as_conversation(session_id)
                if transcript:
                    return transcript, session_id
            except Exception:
                logger.debug("Florence SessionDB load failed for channel_id=%s", channel_id, exc_info=True)
        return self._build_conversation_history(fallback_messages), session_id

    @staticmethod
    def _build_conversation_history(messages: list[ChannelMessage]) -> list[dict[str, str]]:
        history: list[dict[str, str]] = []
        for message in messages:
            if not message.body.strip():
                continue
            if message.sender_role == ChannelMessageRole.USER:
                history.append({"role": "user", "content": message.body})
            elif message.sender_role == ChannelMessageRole.ASSISTANT:
                history.append({"role": "assistant", "content": message.body})
        return history

    def _build_system_message(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
    ) -> str:
        household = self.store.get_household(household_id)
        if household is None:
            return ""
        channel = self.store.get_channel(channel_id)

        actor_name = None
        if actor_member_id:
            member = self.store.get_member(actor_member_id)
            if member is not None:
                actor_name = member.display_name

        members = self.store.list_members(household_id)
        now_utc = self._coerce_aware_datetime(self.now_getter())
        local_now = self._household_local_now(now_utc=now_utc, timezone_name=household.timezone)
        events = self._relevant_events_snapshot(
            self.store.list_household_events(household_id=household_id),
            now_utc=now_utc,
        )
        work_items = [
            item
            for item in self.store.list_household_work_items(household_id=household_id)
            if item.status in {
                HouseholdWorkItemStatus.OPEN,
                HouseholdWorkItemStatus.IN_PROGRESS,
                HouseholdWorkItemStatus.BLOCKED,
            }
        ]
        routines = self.store.list_household_routines(
            household_id=household_id,
            status=HouseholdRoutineStatus.ACTIVE,
        )
        nudges = [
            nudge
            for nudge in self.store.list_household_nudges(household_id=household_id)
            if nudge.status in {
                HouseholdNudgeStatus.SCHEDULED,
                HouseholdNudgeStatus.SENT,
            }
        ]
        meals = self.store.list_household_meals(
            household_id=household_id,
            status=HouseholdMealStatus.PLANNED,
        )
        shopping_items = self.store.list_household_shopping_items(
            household_id=household_id,
            list_name="groceries",
            status=HouseholdShoppingItemStatus.NEEDED,
        )
        preference_items = self.store.list_household_profile_items(
            household_id=household_id,
            kind=HouseholdProfileKind.PREFERENCE,
        )
        child_names = [child.full_name for child in self.store.list_child_profiles(household_id=household_id)]
        child_name_by_id = {
            child.id: child.full_name
            for child in self.store.list_child_profiles(household_id=household_id)
        }
        member_name_by_id = {
            member.id: member.display_name
            for member in members
        }
        school_labels = [
            item.label
            for item in self.store.list_household_profile_items(
                household_id=household_id,
                kind=HouseholdProfileKind.SCHOOL,
            )
        ]
        activity_labels = [
            item.label
            for item in self.store.list_household_profile_items(
                household_id=household_id,
                kind=HouseholdProfileKind.ACTIVITY,
            )
        ]
        scope = resolve_conversation_scope(
            self.store,
            channel_id=channel_id,
            actor_member_id=actor_member_id,
        )
        channel_type = scope.channel_type

        lines = [
            "You are Florence, the Hermes-powered household agent for this household conversation.",
            "You are running on Hermes core, but the backend household state is the source of truth.",
            "The family group chat is the primary operating surface for shared household work. Parent DMs are the private side channel.",
            "You are a general household agent: help with planning, research, logistics, shopping, writing, reminders, and coordination when useful.",
            "Your core product loops are inbox -> plan, capture -> handled, and briefs -> stay ahead.",
            "Treat almost any household input as something you can structure and handle: school email, screenshots, flyers, photos, mental dumps, meals, groceries, reminders, and schedule questions.",
            "You have Hermes non-coding tools available for research, browsing websites, messaging, reminders, and media tasks.",
            "Talk like a capable household assistant, not an internal ops dashboard.",
            "In ordinary parent-facing replies, do not mention backend wording like 'household state', 'calendar projection', 'tentative anchor', 'protocol', 'candidate', 'source classification', or similar internal mechanics unless the user is explicitly asking Florence to debug itself.",
            "If something is missing, say the plain missing fact directly, for example: 'I don't have Theo's school hours saved yet.'",
            "Do not explain storage layers, sync pipelines, projection mechanics, or visibility models in ordinary replies.",
            "Your memory stack is: authoritative Florence household state, Florence session history, and Florence-scoped Honcho memory.",
            "You also have Florence household-state tools. Use them to persist durable household state when the user wants Florence to remember or manage something over time.",
            "If a parent wants Florence to connect another parent into the same household, use household_request_parent_link with their phone number instead of telling them to wait for the family group chat.",
            "When using household_request_parent_link, keep the reply privacy-safe. Do not reveal whether Florence already knew that number or had an existing thread with that person.",
            "If there is an open parent-link request for this parent and they reply with yes, text her, send it, or do it, use household_request_parent_link again with request_id and send_invite_now=true instead of making them repeat the phone number.",
            "Before creating duplicate tasks, events, meals, grocery items, or reminders, check household state, recent Florence context, and connected inbox context when they help.",
            "When the user is asking Florence to capture, track, plan, or manage something, prefer updating durable household state and reply with a concise handled summary of what Florence saved, planned, or still needs.",
            "For meal and grocery requests, prefer creating or updating household meals and shopping items instead of leaving the plan only in chat.",
            "When a user tells Florence a stable preference, constraint, rule, or working style that should affect future behavior, save it with household_record_preference.",
            "Use household_search_state when you need the latest tracked household picture before answering or updating state.",
            "household_search_state now returns scope context too: current visibility scope and tentative tracked state. Private review details are hidden unless you explicitly request them.",
            "When the user asks what Florence said before, refers to an earlier conversation, or wants prior household context, use session_search to recall earlier Florence threads for this household.",
            "When the user asks what they are forgetting, what changed, what matters this week, what still needs handling, or asks for a plan, ground the answer in household_search_state and session_search first, then check Gmail when relevant.",
            "Use session_search and Honcho memory to recover earlier commitments, preferences, and threads of work instead of making the user repeat themselves.",
            "Use household_search_google_inbox whenever connected inbox context is likely to be the fastest grounded way to answer or act on the request.",
            "Use household_search_google_calendar whenever the user is asking about a class, practice, game, appointment, or schedule detail that may already be on their mirrored Google Calendar.",
            "In a private parent DM, be willing to search the connected inbox before asking the parent to restate details that likely already live in an email, invite, or forwarded message.",
            "Treat webcal:// links and .ics URLs as calendar feeds or schedule exports. Convert webcal:// to https:// before fetching or extracting them.",
            "If a parent pastes a schedule link or calendar feed into a private DM, assume they want Florence to inspect or ingest that schedule, not share it to the group.",
            "When a parent pastes a shareable schedule feed they want Florence to remember, use household_import_calendar_feed instead of only summarizing the feed in chat.",
            "If the user points Florence toward their inbox as the source of truth, treat that as a strong cue to search the connected inbox instead of bouncing the question back.",
            "household_search_google_inbox respects scope: in a parent DM it defaults to that parent's inbox, while in the family group it only uses shared-household inbox scope.",
            "household_search_google_calendar respects the same privacy boundary: in a parent DM it defaults to that parent's mirrored calendar, while in the family group it only uses shared-household calendar scope.",
            "If household_search_google_inbox returns no matches but reports mirror_sync_running=true, explain that Florence is still syncing that inbox instead of implying the email does not exist.",
            "If household_search_google_calendar returns no matches but reports mirror_sync_running=true, explain that Florence is still syncing that calendar instead of implying the schedule is absent.",
            "If household_search_google_inbox returns no matches and the user is pointing to a very recent forwarded invite or message, ask for one or two grounding details rather than claiming certainty that nothing is there.",
            "If the live turn payload includes recent_google_context, treat it as fresh mirrored inbox or calendar evidence for this active DM thread.",
            "Use recent_google_context proactively when it likely answers the parent's question or resolves a vague reference like that invite, that schedule, or those school emails.",
            "Do not make the parent restate where something came from if recent_google_context already contains the relevant synced evidence.",
            "If the user thinks something was added twice or duplicated on the calendar, start with household_search_state for events. Use event_insights.likely_duplicate_groups when present, then fix the extra event by id instead of narrating internal uncertainty.",
            "When the user needs current information from the public web such as school calendars, camp policies, activity schedules, vendor details, or comparisons, use web_search and web_extract instead of guessing.",
            "When the task requires interacting with a website or portal, following multi-step navigation, checking dynamic page state, or inspecting console/browser output, use the browser tools instead of pretending you already know the result.",
            "If external research or website investigation branches into a bounded sidecar task, use delegate_task to gather evidence in parallel, then return to the main Florence turn to synthesize the result and make any final household-state updates yourself.",
            "When the user is replying to one currently surfaced imported-item review prompt, treat only that item as actionable. Use household_apply_candidate_review to confirm, reject, skip, set source_visibility, or confirm with corrected fields.",
            "When the user is replying to one currently surfaced reminder/nudge prompt, treat only that one nudge as actionable. Use household_apply_nudge_action for done or snooze changes.",
            "If Florence surfaced a merge follow-up after linking households, focus only on the remaining differences Florence still needs help reconciling, not a dump of both sides.",
            "For merge follow-up items about shared child facts, use household_resolve_merge_followup to apply the chosen shared fact and close or shrink the follow-up in one step.",
            "For other merge follow-up items, summarize the diff plainly, ask for one short keep/merge choice when needed, and mark the work item done with household_upsert_work_item once the overlap is resolved.",
            "When the user gives feedback about reminder style, timing, or Florence being too proactive or not proactive enough, save it with household_record_preference instead of treating it as a deterministic Florence protocol.",
            "When a parent DM is still in onboarding and the message context says onboarding is active, use household_apply_onboarding_update to store only the specific missing setup facts the user actually provided.",
            "For imported Gmail review items, use source_provenance as the primary evidence. Florence may preserve light proposed_fields, but do not trust Gmail-derived times or dates unless they are clearly supported by the raw source.",
            "Do not reach into unrelated hidden review items during ordinary household chat.",
            "Do not ask the user to forward or paste an email if a connected Google inbox is available and household_search_google_inbox can answer it.",
            "When a user gives concrete dates/times they want remembered (camp, school, sports, appointments, trips), save them with household_upsert_event instead of leaving them only in chat.",
            "When plans are tentative, still save them as tentative events and update later.",
            "When the user asks what matters, what changed, or what they are forgetting, synthesize a short operational plan instead of dumping raw notes.",
            "When the user asks for meal planning, pantry or fridge help, or grocery support, use household_upsert_meal and household_upsert_shopping_item when they want Florence to keep tracking it.",
            "When a user shares a screenshot, flyer, photo, document, or extracted media text with dates, deadlines, or logistics, extract the structured details and persist them.",
            "Never claim an imported Gmail or Google Calendar item is confirmed unless it is already present in confirmed household state below.",
            "Before taking an external action that spends money, commits the household, sends a message outside this thread, or changes reminders/plans, get a clear confirmation from the requester.",
            "If household information is missing or ambiguous, ask a short follow-up question.",
            "Keep replies concise and practical. Do not mention internal policy or hidden review queues unless asked directly.",
            "When the user asks a direct operational question, answer that question plainly in the first sentence before any extra context.",
            "When Florence saves or updates something, say it plainly. Prefer 'I added Violet's Wednesday music class' over internal phrases like 'grounded parts', 'baseline cleanup', 'durable fact', or 'private context'.",
            f"Household: {household.name}",
            f"Timezone: {household.timezone}",
            f"Current household-local date/time: {local_now.isoformat()}",
            f"Channel ID: {channel_id}",
        ]
        lines.extend(build_scope_model_lines(scope=scope))
        if scope.is_private_parent_dm:
            lines.append("Channel context: this is a private parent DM, which acts as Florence's private side channel.")
            lines.append(
                "Private DM policy: raw mental-load dumps, emotional support, and individually scoped reasoning stay private by default."
            )
            lines.append(
                "Memory policy: use private member-scoped memory and recall freely here, but do not leak private DM context into the parent group unless the user promotes it."
            )
            lines.append(
                "Recall policy: session_search in this DM can use this parent's private Florence threads plus shared household group threads, but not another parent's private DM history."
            )
            lines.append(
                "If something from this DM should become shared household state, create the structured event, task, meal, grocery item, or reminder first, then offer a concise group-safe summary instead of echoing the raw message."
            )
        elif scope.is_shared_household_group:
            lines.append("Channel context: this is the shared household group chat, which is Florence's primary operating surface for the family.")
            lines.append(
                "Group-chat policy: optimize for shared visibility, coordination, ownership, schedule changes, reminders, meals, grocery planning, and household logistics."
            )
            lines.append(
                "Memory policy: treat this thread as shared household memory. Favor facts, plans, reminders, and decisions that both parents can act on."
            )
            lines.append(
                "Recall policy: session_search in the family group is limited to shared household group threads, not private parent DMs."
            )
        if actor_name:
            lines.append(f"Current speaker: {actor_name}")
        if members:
            lines.append("Members: " + ", ".join(member.display_name for member in members))
        if child_names:
            lines.append("Children: " + ", ".join(child_names))
        if school_labels:
            lines.append("Schools/daycare: " + ", ".join(school_labels))
        if activity_labels:
            lines.append("Activities: " + ", ".join(activity_labels))
        if preference_items:
            lines.append("Remembered household preferences:")
            for item in preference_items[:12]:
                label = item.label
                value = str(item.metadata.get("value") or item.metadata.get("summary") or "").strip()
                if item.child_id and child_name_by_id.get(item.child_id):
                    label = f"{child_name_by_id[item.child_id]} | {label}"
                elif item.member_id and member_name_by_id.get(item.member_id):
                    label = f"{member_name_by_id[item.member_id]} | {label}"
                rendered = f"{label}: {value}" if value else label
                lines.append(f"- {rendered}")
        if actor_member_id:
            pending_parent_links = [
                request
                for request in self.store.list_household_link_requests(
                    household_id=household_id,
                    statuses=(
                        HouseholdLinkRequestStatus.PENDING,
                        HouseholdLinkRequestStatus.ACCEPTED,
                    ),
                )
                if request.inviting_member_id == actor_member_id
            ]
            if pending_parent_links:
                lines.append("Open parent-link requests for this parent:")
                for request in pending_parent_links[:6]:
                    request_metadata = dict(request.metadata) if isinstance(request.metadata, dict) else {}
                    target_name = str(request.invited_display_name or "").strip()
                    if not target_name and request.invited_member_id:
                        invited_member = self.store.get_member(request.invited_member_id)
                        if invited_member is not None and invited_member.display_name.strip():
                            target_name = invited_member.display_name.strip()
                    if not target_name:
                        target_name = "the other parent"
                    invite_sent = "yes" if str(request_metadata.get("invited_message_sent_at") or "").strip() else "no"
                    awaiting_merge_yes = (
                        "yes"
                        if request.status == HouseholdLinkRequestStatus.ACCEPTED
                        and bool(request_metadata.get("awaiting_inviting_confirmation"))
                        else "no"
                    )
                    lines.append(
                        f"- request_id {request.id} | target {target_name} | invite_sent {invite_sent} | "
                        f"awaiting_final_yes {awaiting_merge_yes} | status {request.status.value}"
                    )
        lines.extend(self._build_event_snapshot_lines(events))

        if work_items:
            lines.append("Open household work items:")
            for item in work_items[:12]:
                label = item.title
                if item.due_at:
                    label = f"{label} | due {item.due_at}"
                if item.status != HouseholdWorkItemStatus.OPEN:
                    label = f"{label} | status {item.status.value}"
                lines.append(f"- {label}")
                metadata = dict(item.metadata) if isinstance(item.metadata, dict) else {}
                if metadata.get("category") == "merge_cleanup":
                    lines.append(f"  merge_followup_id: {item.id}")
                    for preview_line in list(metadata.get("preview_lines") or [])[:2]:
                        rendered_preview = str(preview_line).strip()
                        if rendered_preview:
                            lines.append(f"  diff: {rendered_preview}")

        if routines:
            lines.append("Active household routines:")
            for routine in routines[:12]:
                label = f"{routine.title} | cadence {routine.cadence}"
                if routine.next_due_at:
                    label = f"{label} | next due {routine.next_due_at}"
                lines.append(f"- {label}")

        if nudges:
            lines.append("Pending household nudges:")
            for nudge in nudges[:12]:
                label = nudge.message
                if nudge.scheduled_for:
                    label = f"{label} | scheduled {nudge.scheduled_for}"
                if nudge.target_id:
                    label = f"{label} | target {nudge.target_kind.value}:{nudge.target_id}"
                lines.append(f"- {label}")

        if meals:
            lines.append("Upcoming meal plan:")
            for meal in meals[:12]:
                label = f"{meal.title} | {meal.meal_type} | {meal.scheduled_for}"
                lines.append(f"- {label}")

        if shopping_items:
            lines.append("Open grocery list:")
            for item in shopping_items[:20]:
                label = item.title
                if item.quantity:
                    label = f"{label} | qty {item.quantity}"
                if item.unit:
                    label = f"{label} {item.unit}"
                if item.needed_by:
                    label = f"{label} | needed by {item.needed_by}"
                lines.append(f"- {label}")

        lines.append("Use the household state below as authoritative context, then use Hermes tools when they help.")
        return "\n".join(lines)

    @staticmethod
    def _build_event_snapshot_lines(events: list[Any]) -> list[str]:
        confirmed_events = [event for event in events if event.status.value == "confirmed"]
        tentative_events = [event for event in events if event.status.value != "confirmed"]
        lines: list[str] = []

        if confirmed_events:
            lines.append("Confirmed household events:")
            for event in confirmed_events[:20]:
                lines.append(f"- {FlorenceHouseholdChatService._format_event_snapshot_line(event)}")
        else:
            lines.append("Confirmed household events: none yet.")

        if tentative_events:
            lines.append("Tentative tracked events:")
            for event in tentative_events[:12]:
                lines.append(f"- {FlorenceHouseholdChatService._format_event_snapshot_line(event, include_status=True)}")

        return lines

    @staticmethod
    def _coerce_aware_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _parse_event_datetime(raw: str | None) -> datetime | None:
        if not raw:
            return None
        text = raw.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _household_local_now(*, now_utc: datetime, timezone_name: str | None) -> datetime:
        zone_name = timezone_name or "America/Los_Angeles"
        try:
            return now_utc.astimezone(ZoneInfo(zone_name))
        except Exception:
            return now_utc

    @classmethod
    def _event_is_relevant(cls, event: Any, *, now_utc: datetime) -> bool:
        start = cls._parse_event_datetime(getattr(event, "starts_at", None))
        end = cls._parse_event_datetime(getattr(event, "ends_at", None)) or start
        lookback = timedelta(hours=12)

        if start is None and end is None:
            return event.status.value != "confirmed"
        if end is not None:
            return end >= now_utc - lookback
        return False

    @classmethod
    def _event_sort_key(cls, event: Any, *, now_utc: datetime) -> tuple[int, datetime, str]:
        start = cls._parse_event_datetime(getattr(event, "starts_at", None))
        end = cls._parse_event_datetime(getattr(event, "ends_at", None)) or start
        if start is None and end is None:
            return (2, datetime.max.replace(tzinfo=timezone.utc), getattr(event, "title", ""))
        if end is not None and end < now_utc:
            return (1, end, getattr(event, "title", ""))
        return (0, start or end or datetime.max.replace(tzinfo=timezone.utc), getattr(event, "title", ""))

    @classmethod
    def _relevant_events_snapshot(cls, events: list[Any], *, now_utc: datetime) -> list[Any]:
        relevant = [event for event in events if cls._event_is_relevant(event, now_utc=now_utc)]
        return sorted(relevant, key=lambda event: cls._event_sort_key(event, now_utc=now_utc))

    @staticmethod
    def _format_event_snapshot_line(event: Any, *, include_status: bool = False) -> str:
        label = event.title
        if event.starts_at:
            label = f"{label} | starts {event.starts_at}"
        if event.ends_at:
            label = f"{label} | ends {event.ends_at}"
        if event.location:
            label = f"{label} | location {event.location}"
        if include_status:
            label = f"{label} | status {event.status.value}"
        return label
