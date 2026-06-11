"""Linq iMessage transport: webhook verification/parsing and the outbound client.

The parsing and signature code is carried over from the previous Florence and
tolerates both the standard (svix-style) and legacy webhook header formats,
plus several payload shapes Linq has used over time.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import logging
from dataclasses import dataclass, field
from datetime import datetime
from time import time
from typing import Any, Mapping

import httpx

from florence.config import Settings
from florence.timeutil import ensure_utc

log = logging.getLogger("florence.linq")

MESSAGE_RECEIVED_EVENTS = {"message.received", "message.inbound", "chat.message.received"}
MAX_MEDIA_BYTES = 12 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class MessageAttachment:
    kind: str
    url: str | None = None
    content_type: str | None = None
    filename: str | None = None
    extracted_text: str | None = None
    external_id: str | None = None
    size_bytes: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "url": self.url,
            "content_type": self.content_type,
            "filename": self.filename,
            "extracted_text": self.extracted_text,
            "external_id": self.external_id,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class IncomingMessage:
    chat_id: str
    message_id: str
    sender: str
    text: str
    received_at: datetime
    attachments: tuple[MessageAttachment, ...] = ()
    is_group: bool | None = None
    chat_handles: tuple[str, ...] = field(default=())


# -- signature verification ----------------------------------------------------


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
        if _verify_standard(
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
    return _verify_legacy(
        secret=secret, raw_body=raw_body, timestamp=timestamp, signature=signature, now_epoch=now_epoch
    )


def _verify_standard(
    *, secret: str, raw_body: bytes, webhook_id: str, timestamp: str, signature: str, now_epoch: int | None
) -> bool:
    if not _timestamp_is_fresh(timestamp, now_epoch=now_epoch):
        return False
    signed = webhook_id.encode() + b"." + timestamp.encode() + b"." + raw_body
    for key in _secret_keys(secret):
        expected = base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode("ascii")
        for provided in signature.split():
            if provided.startswith("v1,") and hmac.compare_digest(expected, provided[3:]):
                return True
    return False


def _verify_legacy(
    *, secret: str, raw_body: bytes, timestamp: str, signature: str, now_epoch: int | None
) -> bool:
    if not _timestamp_is_fresh(timestamp, now_epoch=now_epoch):
        return False
    signed = timestamp.encode() + b"." + raw_body
    provided = signature.strip()
    if provided.lower().startswith("sha256="):
        provided = provided.split("=", 1)[1].strip()
    for key in _secret_keys(secret):
        expected = hmac.new(key, signed, hashlib.sha256).hexdigest()
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


def _secret_keys(secret: str) -> tuple[bytes, ...]:
    keys: list[bytes] = []
    if secret.startswith("whsec_"):
        try:
            keys.append(base64.b64decode(secret.removeprefix("whsec_"), validate=True))
        except (binascii.Error, ValueError):
            pass
    raw = secret.encode()
    if raw not in keys:
        keys.append(raw)
    return tuple(keys)


# -- webhook parsing -------------------------------------------------------------


def parse_linq_event(
    payload: Mapping[str, Any], headers: Mapping[str, str], *, now_utc: datetime
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

    chat_id = _text(
        _first(
            data.get("chat_id"),
            chat.get("id") if isinstance(chat, Mapping) else None,
            message.get("chat_id") if isinstance(message, Mapping) else None,
        )
    )
    message_id = _text(_first(message.get("id"), message.get("message_id"), data.get("message_id")))
    sender = _sender_value(
        message.get("from"),
        message.get("sender"),
        message.get("sender_handle"),
        message.get("from_handle"),
        data.get("from"),
        data.get("sender"),
        data.get("sender_handle"),
        data.get("from_handle"),
    )
    text = _message_text(message).strip()
    attachments = _message_attachments(message)
    if not chat_id or not message_id or not sender or (not text and not attachments):
        return None

    received_at = _parse_dt(
        _first(message.get("received_at"), message.get("sent_at"), data.get("created_at")),
        default=now_utc,
    )
    is_group: bool | None = None
    if isinstance(chat, Mapping) and isinstance(chat.get("is_group"), bool):
        is_group = chat["is_group"]
    handles = _chat_handles(chat)
    if is_group is None and handles:
        is_group = len(handles) > 2
    return IncomingMessage(
        chat_id=chat_id,
        message_id=message_id,
        sender=sender,
        text=text,
        received_at=received_at,
        attachments=tuple(attachments),
        is_group=is_group,
        chat_handles=handles,
    )


def _chat_handles(chat: Mapping[str, Any] | Any) -> tuple[str, ...]:
    if not isinstance(chat, Mapping):
        return ()
    raw = chat.get("handles") or chat.get("participants")
    if not isinstance(raw, list):
        return ()
    out: list[str] = []
    for item in raw:
        if isinstance(item, Mapping):
            value = _text(_first(item.get("handle"), item.get("phone_number"), item.get("address"), item.get("id")))
        else:
            value = _text(item)
        if value and value not in out:
            out.append(value)
    return tuple(out)


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
                values.append(str(_first(part.get("value"), part.get("body")) or ""))
    return "\n".join(v for v in values if v)


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
        kind = (_text(part.get("type")) or "attachment").lower()
        if kind == "text":
            continue
        url = _text(
            _first(
                part.get("url"),
                part.get("media_url"),
                part.get("content_url"),
                part.get("download_url"),
                part.get("href"),
            )
        )
        attachments.append(
            MessageAttachment(
                kind=kind,
                url=url,
                content_type=_text(_first(part.get("content_type"), part.get("mime_type"), part.get("mime"))),
                filename=_text(_first(part.get("filename"), part.get("file_name"), part.get("name"))),
                extracted_text=_extracted_text(part),
                external_id=_text(_first(part.get("id"), part.get("media_id"), part.get("attachment_id"), url)),
                size_bytes=_int(_first(part.get("size_bytes"), part.get("size"))),
            )
        )
    return attachments


def _sender_value(*values: object) -> str | None:
    for value in values:
        if isinstance(value, Mapping):
            candidate = _first(
                value.get("phone_number"),
                value.get("phone"),
                value.get("handle"),
                value.get("e164"),
                value.get("address"),
                value.get("id"),
            )
        else:
            candidate = value
        text = _text(candidate)
        if text:
            return text
    return None


def normalize_phone(value: str | None) -> str:
    if not value:
        return ""
    digits = "".join(ch for ch in value if ch.isdigit())
    return digits or value.strip().lower()


def _extracted_text(part: Mapping[str, Any]) -> str | None:
    values: list[str] = []
    for key in ("extracted_text", "ocr_text", "text", "description", "summary", "transcript", "alt_text", "caption"):
        value = _text(part.get(key))
        if value and value not in values:
            values.append(value)
    if not values:
        return None
    compacted = " ".join("\n".join(values).split())
    return compacted[:2000]


def _text(value: object) -> str | None:
    if value in (None, "") or isinstance(value, Mapping):
        return None
    text = str(value).strip()
    return text or None


def _int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _parse_dt(value: object, *, default: datetime) -> datetime:
    if not value:
        return ensure_utc(default)
    try:
        return ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
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


# -- outbound client -------------------------------------------------------------


class LinqClient:
    def __init__(self, settings: Settings, *, http_client: httpx.AsyncClient | None = None) -> None:
        self.settings = settings
        self._client = http_client or httpx.AsyncClient(timeout=20.0)

    @property
    def live(self) -> bool:
        return bool(self.settings.linq_api_key)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.settings.linq_api_key}",
            "Content-Type": "application/json",
        }

    def _url(self, path: str) -> str:
        return self.settings.linq_base_url.rstrip("/") + "/" + path.lstrip("/")

    async def send_text(self, *, chat_id: str, text: str, idempotency_key: str) -> dict[str, Any]:
        if not self.live:
            log.info("DRY-RUN send to %s: %s", chat_id, text)
            return {"dry_run": True, "chat_id": chat_id, "text": text}
        # Current docs spell the text part {"type": "text", "body": ...}; older
        # deployments used "value". If the primary shape is rejected, retry once
        # with the legacy key so a docs drift can't take messaging down.
        for attempt, part_key in enumerate(("body", "value")):
            payload = {
                "message": {
                    "parts": [{"type": "text", part_key: text}],
                    "idempotency_key": idempotency_key,
                }
            }
            response = await self._client.post(
                self._url(f"chats/{chat_id}/messages"), headers=self._headers(), json=payload
            )
            if attempt == 0 and response.status_code in (400, 422):
                log.warning(
                    "send_text rejected with %s using part key %r; retrying with legacy key: %s",
                    response.status_code,
                    part_key,
                    response.text[:200],
                )
                continue
            response.raise_for_status()
            return _json_or_empty(response)
        response.raise_for_status()
        return _json_or_empty(response)

    async def create_chat(self, *, to: list[str], text: str, idempotency_key: str) -> dict[str, Any]:
        if not self.settings.linq_from_phone:
            raise RuntimeError("LINQ_FROM_PHONE is not configured")
        if not self.live:
            return {"dry_run": True, "to": to, "text": text}
        payload = {
            "from": self.settings.linq_from_phone,
            "to": to,
            "message": {
                "parts": [{"type": "text", "body": text}],
                "idempotency_key": idempotency_key,
                "preferred_service": "iMessage",
            },
        }
        response = await self._client.post(self._url("chats"), headers=self._headers(), json=payload)
        response.raise_for_status()
        return _json_or_empty(response)

    async def get_chat(self, chat_id: str) -> dict[str, Any]:
        if not self.live:
            return {}
        response = await self._client.get(self._url(f"chats/{chat_id}"), headers=self._headers())
        response.raise_for_status()
        return _json_or_empty(response)

    async def start_typing(self, chat_id: str) -> None:
        if not self.live:
            return
        try:
            await self._client.post(self._url(f"chats/{chat_id}/typing"), headers=self._headers())
        except httpx.HTTPError:
            pass

    async def stop_typing(self, chat_id: str) -> None:
        if not self.live:
            return
        try:
            await self._client.delete(self._url(f"chats/{chat_id}/typing"), headers=self._headers())
        except httpx.HTTPError:
            pass

    async def download_media(self, url: str) -> tuple[bytes, str | None]:
        """Fetch inbound media. CDN URLs are usually presigned; fall back to API auth."""
        response = await self._client.get(url, follow_redirects=True)
        if response.status_code in (401, 403) and self.live:
            response = await self._client.get(
                url, headers={"Authorization": f"Bearer {self.settings.linq_api_key}"}, follow_redirects=True
            )
        response.raise_for_status()
        content = response.content
        if len(content) > MAX_MEDIA_BYTES:
            raise ValueError(f"attachment too large ({len(content)} bytes)")
        return content, response.headers.get("content-type")


def _json_or_empty(response: httpx.Response) -> dict[str, Any]:
    if not response.text:
        return {}
    try:
        parsed = response.json()
        return parsed if isinstance(parsed, dict) else {"response": parsed}
    except ValueError:
        return {"response": response.text}


def chat_handles_from_info(info: Mapping[str, Any]) -> tuple[str, ...]:
    chat = info.get("chat") if isinstance(info.get("chat"), Mapping) else info
    return _chat_handles(chat)


def chat_is_group_from_info(info: Mapping[str, Any]) -> bool | None:
    chat = info.get("chat") if isinstance(info.get("chat"), Mapping) else info
    if isinstance(chat, Mapping) and isinstance(chat.get("is_group"), bool):
        return chat["is_group"]
    handles = _chat_handles(chat)
    if handles:
        return len(handles) > 2
    return None
