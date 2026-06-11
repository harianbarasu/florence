"""Thin client for any OpenAI-compatible chat-completions endpoint.

No SDK: one POST, explicit retries, provider-agnostic. Swapping between
OpenAI, Nous Portal, OpenRouter, or a local server is config, not code.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

from florence.config import Settings

log = logging.getLogger("florence.llm")

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL)
_RETRYABLE = {429, 500, 502, 503, 504}


class LLMError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class LLMReply:
    content: str
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    usage: dict[str, Any] | None = None


class LLMClient:
    def __init__(self, settings: Settings, *, http_client: httpx.AsyncClient | None = None) -> None:
        self.settings = settings
        self._client = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(180.0, connect=15.0)
        )

    async def chat(
        self,
        messages: list[dict[str, Any]],
        *,
        tools: list[dict[str, Any]] | None = None,
        model: str | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> LLMReply:
        if not self.settings.model_api_key:
            raise LLMError("no model API key configured (FLORENCE_MODEL_API_KEY / OPENAI_API_KEY)")
        payload: dict[str, Any] = {"model": model or self.settings.model, "messages": messages}
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"
        if response_format:
            payload["response_format"] = response_format
        if self.settings.reasoning_effort and model is None:
            payload["reasoning_effort"] = self.settings.reasoning_effort

        last_error = "unknown error"
        for attempt in range(3):
            try:
                response = await self._client.post(
                    f"{self.settings.model_base_url}/chat/completions",
                    headers={"Authorization": f"Bearer {self.settings.model_api_key}"},
                    json=payload,
                )
            except httpx.HTTPError as exc:
                last_error = f"network error: {exc}"
                await asyncio.sleep(2 * (attempt + 1))
                continue
            if response.status_code in _RETRYABLE and attempt < 2:
                last_error = f"{response.status_code}: {response.text[:200]}"
                await asyncio.sleep(2 * (attempt + 1))
                continue
            if response.status_code != 200:
                raise LLMError(f"model call failed ({response.status_code}): {response.text[:400]}")
            data = response.json()
            try:
                message = data["choices"][0]["message"]
            except (KeyError, IndexError, TypeError) as exc:
                raise LLMError(f"unexpected model response shape: {str(data)[:300]}") from exc
            return LLMReply(
                content=_content_text(message.get("content")),
                tool_calls=message.get("tool_calls") or [],
                usage=data.get("usage"),
            )
        raise LLMError(f"model call failed after retries: {last_error}")


def _content_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        text = content
    elif isinstance(content, list):
        text = "".join(
            part.get("text", "") for part in content if isinstance(part, dict) and part.get("type") == "text"
        )
    else:
        text = str(content)
    return _THINK_RE.sub("", text).strip()
