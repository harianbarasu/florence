import httpx

from florence.bluebubbles import FlorenceBlueBubblesClient
from florence.config import FlorenceBlueBubblesRuntimeConfig


def test_bluebubbles_client_sends_text_with_password_query(monkeypatch):
    captured = {}

    def fake_post(url, *, headers, json, timeout):
        captured["url"] = str(url)
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr("httpx.post", fake_post)

    client = FlorenceBlueBubblesClient(
        FlorenceBlueBubblesRuntimeConfig(
            base_url="https://bb.example.com",
            password="secret-password",
            webhook_secret="webhook-secret",
        )
    )
    result = client.send_text(chat_guid="iMessage;-;chat123", message="Hello from Florence")

    assert result.status_code == 200
    assert captured["url"] == "https://bb.example.com/api/v1/message/text?password=secret-password"
    assert captured["json"]["chatGuid"] == "iMessage;-;chat123"
    assert captured["json"]["message"] == "Hello from Florence"
    assert client.verify_webhook_secret("webhook-secret") is True
