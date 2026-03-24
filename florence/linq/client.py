"""Linq API client for Florence messaging."""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from time import time
from typing import Any
from urllib.parse import urljoin

import httpx

from florence.config import FlorenceLinqRuntimeConfig


@dataclass(slots=True)
class FlorenceLinqSendResult:
    status_code: int
    body: dict[str, Any] | list[Any] | str | None


class FlorenceLinqClient:
    """Thin Linq client for sending messages and verifying webhooks."""

    def __init__(self, config: FlorenceLinqRuntimeConfig, *, timeout_seconds: float = 30.0):
        self.config = config
        self.timeout_seconds = timeout_seconds

    def is_configured(self) -> bool:
        return self.config.configured

    def verify_webhook_signature(self, *, raw_body: bytes, timestamp: str | None, signature: str | None) -> bool:
        if not self.config.webhook_secret:
            return True
        if not timestamp or not signature:
            return False
        try:
            ts = int(timestamp)
        except Exception:
            return False
        if abs(int(time()) - ts) > 300:
            return False
        signed = timestamp.encode("utf-8") + b"." + raw_body
        expected = hmac.new(
            self.config.webhook_secret.encode("utf-8"),
            signed,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)

    def send_text(self, *, chat_id: str, message: str) -> FlorenceLinqSendResult:
        if not self.is_configured():
            raise ValueError("linq_not_configured")
        if not chat_id.strip():
            raise ValueError("linq_chat_id_required")
        if not message.strip():
            raise ValueError("linq_message_required")

        payload = {
            "message": {
                "parts": [
                    {
                        "type": "text",
                        "value": message,
                    }
                ]
            }
        }
        response = self._request("POST", f"/chats/{chat_id}/messages", payload)
        if response.status_code not in {200, 201, 202}:
            raise RuntimeError(f"linq_send_failed:{response.status_code}:{response.text}")
        return FlorenceLinqSendResult(status_code=response.status_code, body=self._decode_body(response))

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> httpx.Response:
        if not self.config.api_key:
            raise ValueError("linq_api_key_required")
        url = urljoin(self.config.base_url.rstrip("/") + "/", path.lstrip("/"))
        headers = {
            "authorization": f"Bearer {self.config.api_key}",
            "content-type": "application/json",
        }
        return httpx.request(
            method,
            url,
            headers=headers,
            content=json.dumps(payload).encode("utf-8") if payload is not None else None,
            timeout=self.timeout_seconds,
        )

    @staticmethod
    def _decode_body(response: httpx.Response) -> dict[str, Any] | list[Any] | str | None:
        if not response.text:
            return None
        try:
            parsed = response.json()
            return parsed if isinstance(parsed, (dict, list)) else response.text
        except Exception:
            return response.text
