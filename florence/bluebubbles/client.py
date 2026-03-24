"""Outbound BlueBubbles client for Florence production delivery."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import httpx

from florence.config import FlorenceBlueBubblesRuntimeConfig


@dataclass(slots=True)
class BlueBubblesSendResult:
    status_code: int
    body: dict[str, Any] | str | None


class FlorenceBlueBubblesClient:
    """Thin client around the BlueBubbles REST API."""

    def __init__(
        self,
        config: FlorenceBlueBubblesRuntimeConfig,
        *,
        timeout_seconds: float = 30.0,
    ):
        self.config = config
        self.timeout_seconds = timeout_seconds

    def is_configured(self) -> bool:
        return self.config.configured

    def verify_webhook_secret(self, value: str | None) -> bool:
        if not self.config.webhook_secret:
            return True
        return value == self.config.webhook_secret

    def send_text(
        self,
        *,
        chat_guid: str,
        message: str,
        reply_to_guid: str | None = None,
    ) -> BlueBubblesSendResult:
        if not self.is_configured():
            raise ValueError("bluebubbles_not_configured")
        if not chat_guid.strip():
            raise ValueError("bluebubbles_chat_guid_required")
        if not message.strip():
            raise ValueError("bluebubbles_message_required")

        url = self._build_url("/api/v1/message/text")
        payload: dict[str, Any] = {
            "chatGuid": chat_guid,
            "tempGuid": str(uuid.uuid4()),
            "message": message,
        }
        if reply_to_guid:
            payload["replyToGuid"] = reply_to_guid

        try:
            response = httpx.post(
                url,
                headers={"content-type": "application/json"},
                json=payload,
                timeout=self.timeout_seconds,
            )
        except Exception as exc:  # pragma: no cover - network exception path
            raise RuntimeError(f"bluebubbles_send_failed:network:{exc}") from exc

        body = self._decode_body(response)
        if not response.is_success:
            raise RuntimeError(f"bluebubbles_send_failed:{response.status_code}:{body}")
        return BlueBubblesSendResult(status_code=response.status_code, body=body)

    def _build_url(self, path: str) -> str:
        assert self.config.base_url is not None
        assert self.config.password is not None
        base = self.config.base_url.rstrip("/") + "/"
        url = httpx.URL(urljoin(base, path.lstrip("/"))).copy_add_param("password", self.config.password)
        return str(url)

    @staticmethod
    def _decode_body(response: httpx.Response) -> dict[str, Any] | str | None:
        if not response.text:
            return None
        try:
            parsed = response.json()
            return parsed if isinstance(parsed, (dict, list)) else response.text
        except Exception:
            return response.text
