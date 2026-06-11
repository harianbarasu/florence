import base64
import hashlib
import hmac
from datetime import datetime, timezone

from florence.linq import parse_linq_event, verify_linq_signature

NOW = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)


def test_legacy_signature_verification():
    secret = "secret"
    timestamp = "1780689600"
    payload = b'{"ok":true}'
    signature = hmac.new(secret.encode(), timestamp.encode() + b"." + payload, hashlib.sha256).hexdigest()
    assert verify_linq_signature(
        secret=secret, raw_body=payload, timestamp=timestamp, signature=signature, now_epoch=1780689600
    )
    assert verify_linq_signature(
        secret=secret,
        raw_body=payload,
        timestamp=timestamp,
        signature=f"sha256={signature}",
        now_epoch=1780689600,
    )


def test_standard_signature_verification():
    key = b"raw-webhook-key"
    secret = "whsec_" + base64.b64encode(key).decode("ascii")
    webhook_id = "evt_123"
    timestamp = "1780689600"
    payload = b'{"ok":true}'
    signature = base64.b64encode(
        hmac.new(key, webhook_id.encode() + b"." + timestamp.encode() + b"." + payload, hashlib.sha256).digest()
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


def test_stale_timestamp_rejected():
    secret = "secret"
    timestamp = "1780689000"
    payload = b'{"ok":true}'
    signature = hmac.new(secret.encode(), timestamp.encode() + b"." + payload, hashlib.sha256).hexdigest()
    assert not verify_linq_signature(
        secret=secret, raw_body=payload, timestamp=timestamp, signature=signature, now_epoch=1780689600
    )


def test_parse_classic_message_received():
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
        now_utc=NOW,
    )
    assert incoming is not None
    assert incoming.chat_id == "chat-1"
    assert incoming.sender == "+15555550100"
    assert incoming.text == "hello"


def test_parse_2026_sender_handle_payload_with_group():
    incoming = parse_linq_event(
        {
            "api_version": "v3",
            "webhook_version": "2026-02-03",
            "event_type": "message.received",
            "data": {
                "chat": {
                    "id": "chat-2026",
                    "is_group": True,
                    "handles": [
                        {"handle": "+16462350806"},
                        {"handle": "+15555550100"},
                        {"handle": "+15555550101"},
                    ],
                },
                "id": "message-2026",
                "direction": "inbound",
                "sender_handle": {"handle": "+15555550100", "is_me": False},
                "parts": [{"type": "text", "body": "soccer at 4 today"}],
                "received_at": "2026-06-05T15:59:00Z",
            },
        },
        {},
        now_utc=NOW,
    )
    assert incoming is not None
    assert incoming.chat_id == "chat-2026"
    assert incoming.sender == "+15555550100"
    assert incoming.text == "soccer at 4 today"
    assert incoming.is_group is True
    assert incoming.chat_handles == ("+16462350806", "+15555550100", "+15555550101")


def test_parse_attachment_only_message():
    incoming = parse_linq_event(
        {
            "event": "message.received",
            "data": {
                "chat": {"id": "chat-1"},
                "message": {
                    "id": "m2",
                    "from": "+15555550100",
                    "parts": [
                        {
                            "type": "media",
                            "url": "https://cdn.linqapp.com/x.pdf",
                            "content_type": "application/pdf",
                            "filename": "camp-form.pdf",
                        }
                    ],
                },
            },
        },
        {},
        now_utc=NOW,
    )
    assert incoming is not None
    assert incoming.text == ""
    assert len(incoming.attachments) == 1
    att = incoming.attachments[0]
    assert att.content_type == "application/pdf"
    assert att.filename == "camp-form.pdf"


def test_non_message_events_ignored():
    assert (
        parse_linq_event({"event": "message.delivered", "data": {"chat": {"id": "c"}}}, {}, now_utc=NOW)
        is None
    )
