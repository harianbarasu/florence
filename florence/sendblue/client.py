"""Sendblue API client for Florence messaging."""

from __future__ import annotations

import hmac
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import httpx

from florence.config import FlorenceSendblueRuntimeConfig


@dataclass(slots=True)
class FlorenceSendblueSendResult:
    status_code: int
    body: dict[str, Any] | list[Any] | str | None


class FlorenceSendblueClient:
    """Thin Sendblue client for sending messages and verifying webhooks."""

    def __init__(self, config: FlorenceSendblueRuntimeConfig, *, timeout_seconds: float = 30.0):
        self.config = config
        self.timeout_seconds = timeout_seconds

    def is_configured(self) -> bool:
        return self.config.configured

    def verify_webhook_signature(self, *, secret_header: str | None) -> bool:
        if not self.config.webhook_secret:
            return True
        if not secret_header:
            return False
        return hmac.compare_digest(secret_header.strip(), self.config.webhook_secret)

    def send_text(self, *, thread_id: str, message: str) -> FlorenceSendblueSendResult:
        if not self.is_configured():
            raise ValueError("sendblue_not_configured")
        if not message.strip():
            raise ValueError("sendblue_message_required")
        from_number, to_number = self._parse_thread_id(thread_id)
        payload = {
            "number": to_number,
            "from_number": from_number,
            "content": message,
        }
        response = self._request("POST", "/send-message", payload)
        if response.status_code not in {200, 201, 202}:
            raise RuntimeError(f"sendblue_send_failed:{response.status_code}:{response.text}")
        return FlorenceSendblueSendResult(status_code=response.status_code, body=self._decode_body(response))

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> httpx.Response:
        if not self.config.api_key_id or not self.config.api_secret_key:
            raise ValueError("sendblue_api_credentials_required")
        url = urljoin(self.config.base_url.rstrip("/") + "/", path.lstrip("/"))
        headers = {
            "sb-api-key-id": self.config.api_key_id,
            "sb-api-secret-key": self.config.api_secret_key,
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
    def _parse_thread_id(thread_id: str) -> tuple[str, str]:
        raw = thread_id.strip()
        if "|" not in raw:
            raise ValueError("sendblue_thread_id_required")
        from_number, to_number = raw.split("|", 1)
        if not from_number.strip() or not to_number.strip():
            raise ValueError("sendblue_thread_id_required")
        return from_number.strip(), to_number.strip()

    @staticmethod
    def _decode_body(response: httpx.Response) -> dict[str, Any] | list[Any] | str | None:
        if not response.text:
            return None
        try:
            parsed = response.json()
            return parsed if isinstance(parsed, (dict, list)) else response.text
        except Exception:
            return response.text
