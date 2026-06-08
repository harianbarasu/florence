"""Linq webhook parsing and outbound client."""

from __future__ import annotations

import hashlib
import hmac
import json
import base64
import binascii
from datetime import datetime
from time import time
from typing import Any, Mapping
from urllib.parse import urljoin

import httpx

from florence.config import Settings
from florence.models import IncomingMessage, MessageAttachment
from florence.timekeeper import ensure_utc


MESSAGE_RECEIVED_EVENTS = {"message.received", "message.inbound", "chat.message.received"}
MAX_ATTACHMENT_EXTRACTED_TEXT_CHARS = 2000


def verify_linq_signature(
    *,
    secret: str | None,
    raw_body: bytes,
    timestamp: str | None,
    signature: str | None,
    webhook_id: str | None = None,
    webhook_timestamp: str | None = None,
    webhook_signature: str | None = None,
    now_epoch: int | None = None,
) -> bool:
    if not secret:
        return True
    if webhook_id and webhook_timestamp and webhook_signature:
        if _verify_standard_webhook_signature(
            secret=secret,
            raw_body=raw_body,
            webhook_id=webhook_id,
            timestamp=webhook_timestamp,
            signature=webhook_signature,
            now_epoch=now_epoch,
        ):
            return True
    if not timestamp or not signature:
        return False
    return _verify_legacy_webhook_signature(
        secret=secret,
        raw_body=raw_body,
        timestamp=timestamp,
        signature=signature,
        now_epoch=now_epoch,
    )


def _verify_standard_webhook_signature(
    *,
    secret: str,
    raw_body: bytes,
    webhook_id: str,
    timestamp: str,
    signature: str,
    now_epoch: int | None,
) -> bool:
    if not _timestamp_is_fresh(timestamp, now_epoch=now_epoch):
        return False
    signed_content = webhook_id.encode("utf-8") + b"." + timestamp.encode("utf-8") + b"." + raw_body
    for key in _webhook_secret_keys(secret):
        expected = base64.b64encode(hmac.new(key, signed_content, hashlib.sha256).digest()).decode(
            "ascii"
        )
        for provided in signature.split():
            if provided.startswith("v1,") and hmac.compare_digest(expected, provided[3:]):
                return True
    return False


def _verify_legacy_webhook_signature(
    *,
    secret: str,
    raw_body: bytes,
    timestamp: str,
    signature: str,
    now_epoch: int | None,
) -> bool:
    if not _timestamp_is_fresh(timestamp, now_epoch=now_epoch):
        return False
    signed_content = timestamp.encode("utf-8") + b"." + raw_body
    provided = _normalize_signature(signature)
    for key in _webhook_secret_keys(secret):
        expected = hmac.new(key, signed_content, hashlib.sha256).hexdigest()
        if hmac.compare_digest(expected, provided):
            return True
    return False


def _timestamp_is_fresh(timestamp: str, *, now_epoch: int | None) -> bool:
    try:
        ts = int(timestamp)
    except ValueError:
        return False
    now = int(time() if now_epoch is None else now_epoch)
    return abs(now - ts) <= 300


def _webhook_secret_keys(secret: str) -> tuple[bytes, ...]:
    keys: list[bytes] = []
    if secret.startswith("whsec_"):
        try:
            keys.append(base64.b64decode(secret.removeprefix("whsec_"), validate=True))
        except (binascii.Error, ValueError):
            pass
    raw = secret.encode("utf-8")
    if raw not in keys:
        keys.append(raw)
    return tuple(keys)


def parse_linq_event(
    payload: Mapping[str, Any],
    headers: Mapping[str, str],
    *,
    now_utc: datetime,
) -> IncomingMessage | None:
    event_type = _first(
        _header(headers, "x-webhook-event"),
        payload.get("event"),
        payload.get("type"),
        payload.get("event_type"),
    )
    if event_type and str(event_type) not in MESSAGE_RECEIVED_EVENTS:
        return None

    data = payload.get("data") if isinstance(payload.get("data"), Mapping) else payload
    chat = data.get("chat") if isinstance(data.get("chat"), Mapping) else {}
    message = data.get("message") if isinstance(data.get("message"), Mapping) else data

    chat_id = _text_value(
        _first(
            data.get("chat_id"),
            chat.get("id") if isinstance(chat, Mapping) else None,
            message.get("chat_id") if isinstance(message, Mapping) else None,
        )
    )
    message_id = _text_value(
        _first(
            message.get("id"),
            message.get("message_id"),
            data.get("message_id"),
        )
    )
    sender = _sender_value(
        message.get("from"),
        message.get("sender"),
        data.get("from"),
        data.get("sender"),
    )
    text = _message_text(message).strip()
    attachments = _message_attachments(message)
    if not chat_id or not message_id or not sender or (not text and not attachments):
        return None

    received_at = _parse_dt(
        _first(message.get("received_at"), message.get("sent_at"), data.get("created_at")),
        default=now_utc,
    )
    return IncomingMessage(
        chat_id=chat_id,
        message_id=message_id,
        sender=sender,
        text=text,
        received_at=received_at,
        attachments=tuple(attachments),
    )


class LinqClient:
    def __init__(
        self,
        settings: Settings,
        *,
        timeout_seconds: float = 20.0,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings
        self.timeout_seconds = timeout_seconds
        self._client = http_client or httpx.Client(timeout=timeout_seconds)

    def send_text(self, *, chat_id: str, text: str, idempotency_key: str) -> dict[str, Any]:
        if not self.settings.linq_api_key:
            return {"dry_run": True, "chat_id": chat_id, "text": text}
        payload = {
            "message": {
                "parts": [{"type": "text", "value": text}],
                "idempotency_key": idempotency_key,
            }
        }
        url = urljoin(
            self.settings.linq_base_url.rstrip("/") + "/",
            f"chats/{chat_id}/messages",
        )
        response = self._client.post(
            url,
            headers={
                "Authorization": f"Bearer {self.settings.linq_api_key}",
                "Content-Type": "application/json",
            },
            content=json.dumps(payload).encode("utf-8"),
        )
        response.raise_for_status()
        if not response.text:
            return {}
        try:
            parsed = response.json()
            return parsed if isinstance(parsed, dict) else {"response": parsed}
        except ValueError:
            return {"response": response.text}

    def create_chat(
        self,
        *,
        from_phone: str,
        to: tuple[str, ...],
        text: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        recipients = [recipient for recipient in to if recipient]
        if not recipients:
            raise ValueError("create_chat requires at least one recipient")
        if not self.settings.linq_api_key:
            return {
                "dry_run": True,
                "from": from_phone,
                "to": recipients,
                "text": text,
            }
        payload = {
            "from": from_phone,
            "to": recipients,
            "message": {
                "parts": [{"type": "text", "value": text}],
                "idempotency_key": idempotency_key,
                "preferred_service": "iMessage",
            },
        }
        url = urljoin(self.settings.linq_base_url.rstrip("/") + "/", "chats")
        response = self._client.post(
            url,
            headers={
                "Authorization": f"Bearer {self.settings.linq_api_key}",
                "Content-Type": "application/json",
            },
            content=json.dumps(payload).encode("utf-8"),
        )
        response.raise_for_status()
        if not response.text:
            return {}
        try:
            parsed = response.json()
            return parsed if isinstance(parsed, dict) else {"response": parsed}
        except ValueError:
            return {"response": response.text}


def _message_text(message: Mapping[str, Any]) -> str:
    if isinstance(message.get("text"), str):
        return message["text"]
    if isinstance(message.get("body"), str):
        return message["body"]
    parts = message.get("parts")
    if not isinstance(parts, list):
        nested = message.get("message")
        if isinstance(nested, Mapping):
            parts = nested.get("parts")
    values = []
    if isinstance(parts, list):
        for part in parts:
            if isinstance(part, Mapping) and part.get("type") == "text":
                values.append(str(part.get("value") or ""))
    return "\n".join(value for value in values if value)


def _message_attachments(message: Mapping[str, Any]) -> list[MessageAttachment]:
    parts = message.get("parts")
    if not isinstance(parts, list):
        nested = message.get("message")
        if isinstance(nested, Mapping):
            parts = nested.get("parts")
    if not isinstance(parts, list):
        return []
    attachments: list[MessageAttachment] = []
    for part in parts:
        if not isinstance(part, Mapping):
            continue
        kind = _text_value(part.get("type")) or "attachment"
        if kind.lower() == "text":
            continue
        url = _text_value(
            _first(
                part.get("url"),
                part.get("media_url"),
                part.get("content_url"),
                part.get("download_url"),
                part.get("href"),
            )
        )
        external_id = _text_value(
            _first(
                part.get("id"),
                part.get("media_id"),
                part.get("attachment_id"),
                url,
            )
        )
        content_type = _text_value(
            _first(
                part.get("content_type"),
                part.get("mime_type"),
                part.get("mime"),
            )
        )
        filename = _text_value(
            _first(
                part.get("filename"),
                part.get("file_name"),
                part.get("name"),
            )
        )
        attachments.append(
            MessageAttachment(
                kind=kind,
                url=url,
                content_type=content_type,
                filename=filename,
                extracted_text=_attachment_extracted_text(part),
                external_id=external_id,
                size_bytes=_int_value(_first(part.get("size_bytes"), part.get("size"))),
            )
        )
    return attachments


def _sender_value(*values: object) -> str | None:
    for value in values:
        if isinstance(value, Mapping):
            candidate = _first(
                value.get("phone_number"),
                value.get("phone"),
                value.get("e164"),
                value.get("address"),
                value.get("id"),
            )
        else:
            candidate = value
        text = _text_value(candidate)
        if text:
            return text
    return None


def _text_value(value: object) -> str | None:
    if value in (None, "") or isinstance(value, Mapping):
        return None
    text = str(value).strip()
    return text or None


def _attachment_extracted_text(part: Mapping[str, Any]) -> str | None:
    values = []
    for key in (
        "extracted_text",
        "ocr_text",
        "text",
        "description",
        "summary",
        "transcript",
        "alt_text",
        "caption",
    ):
        value = _text_value(part.get(key))
        if value and value not in values:
            values.append(value)
    if not values:
        return None
    return _compact_text("\n".join(values), limit=MAX_ATTACHMENT_EXTRACTED_TEXT_CHARS)


def _compact_text(value: str, *, limit: int) -> str:
    compacted = " ".join(value.split())
    if len(compacted) <= limit:
        return compacted
    return compacted[:limit].rstrip()


def _int_value(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _parse_dt(value: object, *, default: datetime) -> datetime:
    if not value:
        return ensure_utc(default)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return ensure_utc(parsed)
    except ValueError:
        return ensure_utc(default)


def _header(headers: Mapping[str, str], name: str) -> str | None:
    lower = name.lower()
    for key, value in headers.items():
        if key.lower() == lower:
            return value
    return None


def _first(*values: object) -> object | None:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _normalize_signature(signature: str) -> str:
    compact = signature.strip()
    if compact.lower().startswith("sha256="):
        return compact.split("=", 1)[1].strip()
    return compact
