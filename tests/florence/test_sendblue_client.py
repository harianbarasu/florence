import httpx

from florence.config import FlorenceSendblueRuntimeConfig
from florence.sendblue import FlorenceSendblueClient


def test_sendblue_client_sends_text(monkeypatch):
    captured = {}

    def fake_request(method, url, *, headers, content, timeout):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = headers
        captured["content"] = content.decode("utf-8")
        captured["timeout"] = timeout
        return httpx.Response(200, json={"status": "OK", "message_handle": "msg_123"})

    monkeypatch.setattr("httpx.request", fake_request)

    client = FlorenceSendblueClient(
        FlorenceSendblueRuntimeConfig(
            api_key_id="sb-key-id",
            api_secret_key="sb-secret",
            from_number="+15122164639",
            webhook_secret="webhook-secret",
        )
    )
    result = client.send_text(thread_id="+15122164639|+15555550123", message="Hello from Florence")

    assert result.status_code == 200
    assert captured["method"] == "POST"
    assert captured["url"] == "https://api.sendblue.co/api/send-message"
    assert captured["headers"]["sb-api-key-id"] == "sb-key-id"
    assert captured["headers"]["sb-api-secret-key"] == "sb-secret"
    assert "\"number\": \"+15555550123\"" in captured["content"]
    assert "\"from_number\": \"+15122164639\"" in captured["content"]


def test_sendblue_client_verifies_webhook_secret():
    client = FlorenceSendblueClient(
        FlorenceSendblueRuntimeConfig(
            api_key_id="sb-key-id",
            api_secret_key="sb-secret",
            from_number="+15122164639",
            webhook_secret="webhook-secret",
        )
    )

    assert client.verify_webhook_signature(secret_header="webhook-secret") is True
    assert client.verify_webhook_signature(secret_header="wrong-secret") is False


def test_sendblue_client_sends_group_text(monkeypatch):
    captured = {}

    def fake_request(method, url, *, headers, content, timeout):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = headers
        captured["content"] = content.decode("utf-8")
        captured["timeout"] = timeout
        return httpx.Response(200, json={"status": "OK", "message_handle": "msg_group_123"})

    monkeypatch.setattr("httpx.request", fake_request)

    client = FlorenceSendblueClient(
        FlorenceSendblueRuntimeConfig(
            api_key_id="sb-key-id",
            api_secret_key="sb-secret",
            from_number="+15122164639",
            webhook_secret="webhook-secret",
        )
    )
    result = client.send_text(
        thread_id="+15122164639|group:group_123456",
        message="Hello, everyone!",
        group_id="group_123456",
    )

    assert result.status_code == 200
    assert captured["url"] == "https://api.sendblue.co/api/send-group-message"
    assert "\"group_id\": \"group_123456\"" in captured["content"]
    assert "\"from_number\": \"+15122164639\"" in captured["content"]


def test_sendblue_client_adds_group_recipient(monkeypatch):
    captured = {}

    def fake_request(method, url, *, headers, content, timeout):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = headers
        captured["content"] = content.decode("utf-8")
        captured["timeout"] = timeout
        return httpx.Response(200, json={"status": "OK", "message": "added"})

    monkeypatch.setattr("httpx.request", fake_request)

    client = FlorenceSendblueClient(
        FlorenceSendblueRuntimeConfig(
            api_key_id="sb-key-id",
            api_secret_key="sb-secret",
            from_number="+15122164639",
            webhook_secret="webhook-secret",
        )
    )
    result = client.add_group_recipient(group_id="group_123456", number="+18887776666")

    assert result.status_code == 200
    assert captured["url"] == "https://api.sendblue.co/api/modify-group"
    assert "\"group_id\": \"group_123456\"" in captured["content"]
    assert "\"modify_type\": \"add_recipient\"" in captured["content"]
    assert "\"number\": \"+18887776666\"" in captured["content"]
