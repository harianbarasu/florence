"""Adapter between Florence household turns and Hermes core."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Callable

from florence.agent.hermes_runtime import build_hermes_agent
from florence.state import FlorenceStateDB


@dataclass(slots=True)
class FlorenceHermesRunResult:
    result: dict[str, Any]
    session_id: str
    task_id: str


class FlorenceHermesAdapter:
    """Own the direct Hermes `AIAgent` construction boundary for Florence."""

    def __init__(
        self,
        *,
        store: FlorenceStateDB,
        model: str,
        provider: str,
        session_db: Any,
        agent_factory: Callable[..., Any] | None = None,
        household_chat_service: Any | None = None,
    ) -> None:
        self.store = store
        self.model = model
        self.provider = provider
        self.session_db = session_db
        self.agent_factory = agent_factory
        self.household_chat_service = household_chat_service

    def run_conversation(
        self,
        *,
        household_id: str,
        channel_id: str,
        actor_member_id: str | None,
        user_message: Any,
        system_message: str,
        conversation_history: list[dict[str, str]] | None,
        enabled_toolsets: list[str],
        max_iterations: int,
        session_id: str | None,
        skip_memory: bool,
        honcho_session_key: str | None,
        session_search_kwargs: dict[str, Any],
        persist_user_message: str | None = None,
    ) -> FlorenceHermesRunResult:
        task_id = f"florence-household-{uuid.uuid4()}"
        turn_id = task_id
        agent_factory = self.agent_factory
        if agent_factory is None:
            agent_factory = build_hermes_agent

        from florence.tools.household import (
            clear_household_tool_context,
            set_household_tool_context,
        )

        set_household_tool_context(
            task_id,
            store=self.store,
            household_id=household_id,
            actor_member_id=actor_member_id,
            channel_id=channel_id,
            turn_id=turn_id,
            household_chat_service=self.household_chat_service,
        )
        try:
            agent = agent_factory(
                model=self.model,
                max_iterations=max_iterations,
                provider=self.provider,
                enabled_toolsets=enabled_toolsets,
                quiet_mode=True,
                skip_memory=skip_memory,
                skip_local_memory=True,
                skip_context_files=False,
                platform="florence",
                session_id=session_id,
                session_db=self.session_db,
                honcho_session_key=honcho_session_key,
                session_search_kwargs=session_search_kwargs,
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
            return FlorenceHermesRunResult(
                result=result,
                session_id=str(getattr(agent, "session_id", "") or "").strip(),
                task_id=task_id,
            )
        finally:
            clear_household_tool_context(task_id)
