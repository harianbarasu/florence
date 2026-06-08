import base64
import hashlib
import hmac
import json
from datetime import datetime, timezone

import httpx

from florence.config import Settings
from florence.linq import LinqClient, parse_linq_event, verify_linq_signature


def test_linq_signature_verification():
    secret = "secret"
    timestamp = "1780689600"
    payload = b'{"ok":true}'
    signature = hmac.new(
        secret.encode(),
        timestamp.encode() + b"." + payload,
        hashlib.sha256,
    ).hexdigest()

    assert verify_linq_signature(
        secret=secret,
        raw_body=payload,
        timestamp=timestamp,
        signature=signature,
        now_epoch=1780689600,
    )

    assert verify_linq_signature(
        secret=secret,
        raw_body=payload,
        timestamp=timestamp,
        signature=f"sha256={signature}",
        now_epoch=1780689600,
    )


def test_linq_standard_webhook_signature_verification():
    key = b"raw-webhook-key"
    secret = "whsec_" + base64.b64encode(key).decode("ascii")
    webhook_id = "evt_123"
    timestamp = "1780689600"
    payload = b'{"ok":true}'
    signature = base64.b64encode(
        hmac.new(
            key,
            webhook_id.encode() + b"." + timestamp.encode() + b"." + payload,
            hashlib.sha256,
        ).digest()
    ).decode("ascii")

    assert verify_linq_signature(
        secret=secret,
        raw_body=payload,
        timestamp=None,
        signature=None,
        webhook_id=webhook_id,
        webhook_timestamp=timestamp,
        webhook_signature=f"v1,{signature}",
        now_epoch=1780689600,
    )


def test_linq_signature_rejects_stale_timestamp():
    secret = "secret"
    timestamp = "1780689000"
    payload = b'{"ok":true}'
    signature = hmac.new(
        secret.encode(),
        timestamp.encode() + b"." + payload,
        hashlib.sha256,
    ).hexdigest()

    assert not verify_linq_signature(
        secret=secret,
        raw_body=payload,
        timestamp=timestamp,
        signature=signature,
        now_epoch=1780689600,
    )


def test_parse_linq_message_received_event():
    incoming = parse_linq_event(
        {
            "event": "message.received",
            "data": {
                "chat": {"id": "chat-1"},
                "message": {
                    "id": "message-1",
                    "from": "+15555550100",
                    "parts": [{"type": "text", "value": "hello"}],
                    "sent_at": "2026-06-05T16:00:00Z",
                },
            },
        },
        {},
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )

    assert incoming is not None
    assert incoming.chat_id == "chat-1"
    assert incoming.text == "hello"


def test_parse_linq_combines_text_parts_and_sender_objects():
    incoming = parse_linq_event(
        {
            "event": "message.received",
            "data": {
                "chat": {"id": "chat-1"},
                "message": {
                    "id": "message-1",
                    "sender": {"phone_number": "+15555550100"},
                    "parts": [
                        {"type": "text", "value": "Permission slip"},
                        {"type": "image", "url": "https://example.com/slip.png"},
                        {"type": "text", "value": "due tomorrow"},
                    ],
                },
            },
        },
        {},
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )

    assert incoming is not None
    assert incoming.sender == "+15555550100"
    assert incoming.text == "Permission slip\ndue tomorrow"
    assert len(incoming.attachments) == 1
    assert incoming.attachments[0].kind == "image"
    assert incoming.attachments[0].url == "https://example.com/slip.png"


def test_parse_linq_media_only_message():
    incoming = parse_linq_event(
        {
            "event": "message.received",
            "data": {
                "chat": {"id": "chat-1"},
                "message": {
                    "id": "message-1",
                    "from": "+15555550100",
                    "parts": [
                        {
                            "type": "image",
                            "url": "https://example.com/flyer.png",
                            "mime_type": "image/png",
                            "filename": "flyer.png",
                            "ocr_text": "Permission slip due tomorrow.",
                            "size": "1234",
                        }
                    ],
                },
            },
        },
        {},
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )

    assert incoming is not None
    assert incoming.text == ""
    assert len(incoming.attachments) == 1
    assert incoming.attachments[0].content_type == "image/png"
    assert incoming.attachments[0].filename == "flyer.png"
    assert incoming.attachments[0].extracted_text == "Permission slip due tomorrow."
    assert incoming.attachments[0].size_bytes == 1234


def test_parse_linq_requires_stable_message_id():
    incoming = parse_linq_event(
        {
            "event": "message.received",
            "data": {
                "chat": {"id": "chat-1"},
                "message": {
                    "from": "+15555550100",
                    "parts": [{"type": "text", "value": "hello"}],
                },
            },
        },
        {},
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )

    assert incoming is None


def test_parse_linq_requires_sender():
    incoming = parse_linq_event(
        {
            "event": "message.received",
            "data": {
                "chat": {"id": "chat-1"},
                "message": {
                    "id": "message-1",
                    "parts": [{"type": "text", "value": "hello"}],
                },
            },
        },
        {},
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )

    assert incoming is None


def test_parse_linq_ignores_delivery_events():
    incoming = parse_linq_event(
        {"event": "message.delivered", "data": {"chat": {"id": "chat-1"}}},
        {},
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )

    assert incoming is None


def test_linq_send_text_posts_existing_chat_message():
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"message": {"id": "outbound-1"}})

    client = LinqClient(
        Settings(linq_api_key="linq-api-key"),
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = client.send_text(
        chat_id="chat-1",
        text="Reminder: pack lunch",
        idempotency_key="reminder:abc",
    )

    payload = json.loads(requests[0].content.decode("utf-8"))
    assert result["message"]["id"] == "outbound-1"
    assert requests[0].url.path == "/api/partner/v3/chats/chat-1/messages"
    assert requests[0].headers["authorization"] == "Bearer linq-api-key"
    assert payload["message"]["parts"] == [{"type": "text", "value": "Reminder: pack lunch"}]
    assert payload["message"]["idempotency_key"] == "reminder:abc"


def test_linq_create_chat_posts_group_initial_message():
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"chat": {"id": "group-chat"}})

    client = LinqClient(
        Settings(linq_api_key="linq-api-key"),
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    result = client.create_chat(
        from_phone="+15555550000",
        to=("+15555550100", "+15555550101"),
        text="Welcome to Florence.",
        idempotency_key="group:invite",
    )

    payload = json.loads(requests[0].content.decode("utf-8"))
    assert result["chat"]["id"] == "group-chat"
    assert requests[0].url.path == "/api/partner/v3/chats"
    assert requests[0].headers["authorization"] == "Bearer linq-api-key"
    assert payload["from"] == "+15555550000"
    assert payload["to"] == ["+15555550100", "+15555550101"]
    assert payload["message"]["parts"] == [{"type": "text", "value": "Welcome to Florence."}]
    assert payload["message"]["idempotency_key"] == "group:invite"
    assert payload["message"]["preferred_service"] == "iMessage"
