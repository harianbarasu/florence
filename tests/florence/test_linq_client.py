import hashlib
import hmac
import time

import httpx

from florence.config import FlorenceLinqRuntimeConfig
from florence.linq import FlorenceLinqClient


def test_linq_client_sends_text(monkeypatch):
    captured = {}

    def fake_request(method, url, *, headers, content, timeout):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = headers
        captured["content"] = content.decode("utf-8")
        captured["timeout"] = timeout
        return httpx.Response(202, json={"id": "msg_123"})

    monkeypatch.setattr("httpx.request", fake_request)

    client = FlorenceLinqClient(
        FlorenceLinqRuntimeConfig(
            api_key="linq-api-key",
            webhook_secret="webhook-secret",
        )
    )
    result = client.send_text(chat_id="chat_123", message="Hello from Florence")

    assert result.status_code == 202
    assert captured["method"] == "POST"
    assert captured["url"] == "https://api.linqapp.com/api/partner/v3/chats/chat_123/messages"
    assert captured["headers"]["authorization"] == "Bearer linq-api-key"
    assert "Hello from Florence" in captured["content"]


def test_linq_client_verifies_webhook_signature():
    client = FlorenceLinqClient(
        FlorenceLinqRuntimeConfig(
            api_key="linq-api-key",
            webhook_secret="webhook-secret",
        )
    )
    raw_body = b'{"ok":true}'
    timestamp = str(int(time.time()))
    signature = hmac.new(
        b"webhook-secret",
        timestamp.encode("utf-8") + b"." + raw_body,
        hashlib.sha256,
    ).hexdigest()

    assert client.verify_webhook_signature(
        raw_body=raw_body,
        timestamp=timestamp,
        signature=signature,
    ) is True
