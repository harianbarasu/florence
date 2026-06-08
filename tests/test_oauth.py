from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

from fastapi.testclient import TestClient

from florence.app import create_app
from florence.config import Settings
from florence.models import ConnectedAccountStatus
from florence.oauth import (
    GoogleOAuthResult,
    OAuthExchangeError,
    TokenVault,
    google_authorization_url,
)
from florence.service import FlorenceService
from florence.store import Store
from florence.worker import run_source_sync_tick


def _settings(tmp_path, **overrides):
    values = {
        "db_path": str(tmp_path / "florence.sqlite"),
        "google_client_id": "google-client-id",
        "google_client_secret": "google-client-secret",
        "google_redirect_uri": "https://florence.example.com/oauth/google/callback",
        "token_encryption_key": TokenVault.generate_key(),
    }
    values.update(overrides)
    return Settings(**values)


class FakeGoogleOAuthClient:
    def __init__(self) -> None:
        self.codes: list[str] = []

    def exchange_code(self, *, code: str, now_utc: datetime) -> GoogleOAuthResult:
        self.codes.append(code)
        expires_at = now_utc + timedelta(hours=1)
        return GoogleOAuthResult(
            external_account_id="google-sub-123",
            account_label="parent@example.com",
            scopes=(
                "openid",
                "email",
                "https://www.googleapis.com/auth/gmail.readonly",
                "https://www.googleapis.com/auth/calendar.readonly",
            ),
            token_payload={
                "provider": "google",
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "scope": "openid email",
                "expires_at_utc": expires_at.isoformat(),
            },
            expires_at_utc=expires_at,
        )


class FailingGoogleOAuthClient:
    def exchange_code(self, *, code: str, now_utc: datetime) -> GoogleOAuthResult:
        raise OAuthExchangeError(
            "google token exchange failed with Bearer abcdefghijklmnop "
            "for parent@example.com and +15555550123 using google-client-secret"
        )


class FailingLinqClient:
    def send_text(self, *, chat_id: str, text: str, idempotency_key: str):
        raise RuntimeError("linq confirmation unavailable")

    def create_chat(self, *, from_phone: str, to: tuple[str, ...], text: str, idempotency_key: str):
        raise AssertionError("oauth callback should not create a new chat")


class CapturingLinqClient:
    def __init__(self) -> None:
        self.sent: list[dict[str, str]] = []

    def send_text(self, *, chat_id: str, text: str, idempotency_key: str):
        self.sent.append(
            {
                "chat_id": chat_id,
                "text": text,
                "idempotency_key": idempotency_key,
            }
        )

    def create_chat(self, *, from_phone: str, to: tuple[str, ...], text: str, idempotency_key: str):
        raise AssertionError("oauth confirmation retry should not create a new chat")


def test_google_authorization_url_uses_state_and_readonly_scopes(tmp_path):
    settings = _settings(tmp_path)

    url = google_authorization_url(settings=settings, state="state-123")
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    assert parsed.scheme == "https"
    assert parsed.netloc == "accounts.google.com"
    assert query["client_id"] == ["google-client-id"]
    assert query["redirect_uri"] == ["https://florence.example.com/oauth/google/callback"]
    assert query["response_type"] == ["code"]
    assert query["state"] == ["state-123"]
    assert query["access_type"] == ["offline"]
    assert query["include_granted_scopes"] == ["true"]
    assert query["prompt"] == ["consent"]
    assert "https://www.googleapis.com/auth/gmail.readonly" in query["scope"][0]
    assert "https://www.googleapis.com/auth/calendar.readonly" in query["scope"][0]


def test_token_vault_encrypts_without_plaintext_and_round_trips(tmp_path):
    settings = _settings(tmp_path)
    vault = TokenVault.from_settings(settings)

    ciphertext = vault.encrypt({"access_token": "secret-access-token"})

    assert "secret-access-token" not in ciphertext
    assert vault.decrypt(ciphertext) == {"access_token": "secret-access-token"}


def test_google_oauth_callback_stores_encrypted_token_and_connected_account(tmp_path):
    settings = _settings(tmp_path)
    fake_oauth = FakeGoogleOAuthClient()
    client = TestClient(create_app(settings, google_oauth_client=fake_oauth))

    start = client.post(
        "/dev/oauth/google/start",
        json={"chat_id": "family-chat", "account_label": "Mom Gmail"},
    )
    state = start.json()["state"]
    callback = client.get(f"/oauth/google/callback?state={state}&code=auth-code")
    replay = client.get(f"/oauth/google/callback?state={state}&code=auth-code")

    store = Store(settings.db_path)
    household = store.get_household_by_chat("family-chat")
    assert start.status_code == 200
    assert callback.status_code == 200
    assert replay.status_code == 400
    assert fake_oauth.codes == ["auth-code"]
    assert household is not None
    accounts = store.list_connected_accounts(household.id)
    history = store.recent_messages(household.id)
    assert len(accounts) == 1
    assert accounts[0].provider == "google"
    assert accounts[0].external_account_id == "google-sub-123"
    assert accounts[0].account_label == "Mom Gmail"
    assert history[-1]["role"] == "assistant"
    assert "Google is connected for Mom Gmail" in history[-1]["content"]
    token = store.get_connected_account_token(accounts[0].id)
    assert token is not None
    assert "access-token" not in token.token_ciphertext
    decrypted = TokenVault.from_settings(settings).decrypt(token.token_ciphertext)
    assert decrypted["access_token"] == "access-token"
    assert decrypted["refresh_token"] == "refresh-token"


def test_google_oauth_callback_does_not_recreate_deleted_household(tmp_path):
    settings = _settings(tmp_path)
    fake_oauth = FakeGoogleOAuthClient()
    client = TestClient(create_app(settings, google_oauth_client=fake_oauth))

    start = client.post(
        "/dev/oauth/google/start",
        json={"chat_id": "deleted-family-chat", "account_label": "Mom Gmail"},
    )
    state = start.json()["state"]
    store = Store(settings.db_path)
    household = store.get_household_by_chat("deleted-family-chat")
    assert household is not None
    store.delete_household(household.id)

    callback = client.get(f"/oauth/google/callback?state={state}&code=auth-code")

    assert callback.status_code == 400
    assert callback.json()["detail"] == "invalid_or_expired_oauth_state"
    assert fake_oauth.codes == []
    assert store.get_household_by_chat("deleted-family-chat") is None


def test_google_oauth_callback_does_not_echo_google_error_query(tmp_path):
    settings = _settings(tmp_path)
    client = TestClient(create_app(settings))

    response = client.get(
        "/oauth/google/callback?error=access_denied+Bearer+abcdefghijklmnop+"
        "parent@example.com+%2B15555550123"
    )
    body = response.text

    assert response.status_code == 400
    assert response.json()["detail"] == "google_oauth_error"
    assert "abcdefghijklmnop" not in body
    assert "parent@example.com" not in body
    assert "+15555550123" not in body


def test_google_oauth_callback_uses_generic_error_for_exchange_failure(tmp_path):
    settings = _settings(tmp_path)
    client = TestClient(create_app(settings, google_oauth_client=FailingGoogleOAuthClient()))
    start = client.post(
        "/dev/oauth/google/start",
        json={"chat_id": "family-chat", "account_label": "Mom Gmail"},
    )
    state = start.json()["state"]

    response = client.get(f"/oauth/google/callback?state={state}&code=auth-code")
    body = response.text
    store = Store(settings.db_path)
    household = store.get_household_by_chat("family-chat")

    assert response.status_code == 502
    assert response.json()["detail"] == "google_oauth_callback_failed"
    assert household is not None
    assert store.list_connected_accounts(household.id) == []
    assert "abcdefghijklmnop" not in body
    assert "parent@example.com" not in body
    assert "+15555550123" not in body
    assert "google-client-secret" not in body
    assert "google token exchange failed" not in body


def test_google_oauth_callback_keeps_connection_when_confirmation_send_fails(tmp_path):
    settings = _settings(tmp_path)
    fake_oauth = FakeGoogleOAuthClient()
    client = TestClient(
        create_app(
            settings,
            google_oauth_client=fake_oauth,
            linq_client=FailingLinqClient(),
        )
    )

    start = client.post("/dev/oauth/google/start", json={"chat_id": "family-chat"})
    state = start.json()["state"]
    callback = client.get(f"/oauth/google/callback?state={state}&code=auth-code")

    store = Store(settings.db_path)
    household = store.get_household_by_chat("family-chat")
    assert callback.status_code == 200
    assert "Google is connected." in callback.text
    assert "could not send the iMessage confirmation yet" in callback.text
    assert fake_oauth.codes == ["auth-code"]
    assert household is not None
    accounts = store.list_connected_accounts(household.id)
    assert len(accounts) == 1
    token = store.get_connected_account_token(accounts[0].id)
    assert token is not None
    delivery = store.outbound_delivery_summary(household_id=household.id)
    assert delivery["ready"] is False
    assert delivery["failed"] == 1
    assert delivery["issues"][0]["idempotency_key"] == f"oauth:google:{state}"
    assert delivery["issues"][0]["source_message_id"] == f"oauth:google:{state}"
    assert delivery["issues"][0]["last_error"] == "RuntimeError: linq confirmation unavailable"


def test_google_oauth_callback_does_not_text_stopped_household(tmp_path):
    settings = _settings(tmp_path)
    fake_oauth = FakeGoogleOAuthClient()
    fake_linq = CapturingLinqClient()
    client = TestClient(
        create_app(
            settings,
            google_oauth_client=fake_oauth,
            linq_client=fake_linq,
        )
    )

    start = client.post("/dev/oauth/google/start", json={"chat_id": "family-chat"})
    state = start.json()["state"]
    store = Store(settings.db_path)
    household = store.get_household_by_chat("family-chat")
    assert household is not None
    store.set_stopped(household.id, True)

    callback = client.get(f"/oauth/google/callback?state={state}&code=auth-code")

    accounts = store.list_connected_accounts(household.id)
    history = store.recent_messages(household.id)
    delivery = store.outbound_delivery_summary(household_id=household.id)
    assert callback.status_code == 200
    assert "household is paused" in callback.text
    assert fake_oauth.codes == ["auth-code"]
    assert fake_linq.sent == []
    assert len(accounts) == 1
    assert store.get_connected_account_token(accounts[0].id) is not None
    assert not any("Google is connected" in message["content"] for message in history)
    assert delivery["ready"] is True
    assert delivery["retryable"] == 0
    assert delivery["sent"] == 0


def test_google_oauth_callback_reconnects_disabled_google_account(tmp_path):
    settings = _settings(tmp_path)
    fake_oauth = FakeGoogleOAuthClient()
    client = TestClient(create_app(settings, google_oauth_client=fake_oauth))

    first_start = client.post("/dev/oauth/google/start", json={"chat_id": "family-chat"})
    first_state = first_start.json()["state"]
    first_callback = client.get(f"/oauth/google/callback?state={first_state}&code=auth-code-1")
    store = Store(settings.db_path)
    household = store.get_household_by_chat("family-chat")
    assert household is not None
    first_account = store.list_connected_accounts(household.id)[0]
    store.disconnect_connected_accounts(
        household_id=household.id,
        provider="google",
        now_utc=datetime(2026, 6, 5, 16, 5, tzinfo=timezone.utc),
    )

    second_start = client.post("/dev/oauth/google/start", json={"chat_id": "family-chat"})
    second_state = second_start.json()["state"]
    second_callback = client.get(f"/oauth/google/callback?state={second_state}&code=auth-code-2")
    store = Store(settings.db_path)
    active_accounts = store.list_connected_accounts(household.id)
    all_accounts = store.list_connected_accounts(household.id, include_disabled=True)

    assert first_callback.status_code == 200
    assert second_callback.status_code == 200
    assert fake_oauth.codes == ["auth-code-1", "auth-code-2"]
    assert len(active_accounts) == 1
    assert len(all_accounts) == 1
    assert active_accounts[0].id == first_account.id
    assert active_accounts[0].status == ConnectedAccountStatus.ACTIVE
    assert store.get_connected_account_token(active_accounts[0].id) is not None


def test_worker_retries_failed_google_oauth_confirmation(tmp_path):
    settings = _settings(tmp_path)
    fake_oauth = FakeGoogleOAuthClient()
    client = TestClient(
        create_app(
            settings,
            google_oauth_client=fake_oauth,
            linq_client=FailingLinqClient(),
        )
    )
    start = client.post("/dev/oauth/google/start", json={"chat_id": "family-chat"})
    state = start.json()["state"]
    callback = client.get(f"/oauth/google/callback?state={state}&code=auth-code")
    retry_sender = CapturingLinqClient()

    result = run_source_sync_tick(
        FlorenceService(settings=settings),
        providers={},
        sender=retry_sender,
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )

    store = Store(settings.db_path)
    household = store.get_household_by_chat("family-chat")
    assert callback.status_code == 200
    assert household is not None
    assert result.checked == 1
    assert result.skipped == 1
    assert len(retry_sender.sent) == 1
    assert retry_sender.sent[0]["idempotency_key"] == f"oauth:google:{state}"
    delivery = store.outbound_delivery_summary(household_id=household.id)
    assert delivery["ready"] is True
    assert delivery["sent"] == 1
    assert delivery["failed"] == 0


def test_google_disconnect_cancels_failed_oauth_confirmation_retry(tmp_path):
    settings = _settings(tmp_path)
    fake_oauth = FakeGoogleOAuthClient()
    client = TestClient(
        create_app(
            settings,
            google_oauth_client=fake_oauth,
            linq_client=FailingLinqClient(),
        )
    )
    start = client.post("/dev/oauth/google/start", json={"chat_id": "family-chat"})
    state = start.json()["state"]
    callback = client.get(f"/oauth/google/callback?state={state}&code=auth-code")
    store = Store(settings.db_path)
    household = store.get_household_by_chat("family-chat")
    assert household is not None
    assert callback.status_code == 200
    assert store.outbound_delivery_summary(household_id=household.id)["failed"] == 1

    store.disconnect_connected_accounts(
        household_id=household.id,
        provider="google",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    retry_sender = CapturingLinqClient()
    result = run_source_sync_tick(
        FlorenceService(settings=settings),
        providers={},
        sender=retry_sender,
        now_utc=datetime(2026, 6, 5, 16, 1, tzinfo=timezone.utc),
    )
    delivery = Store(settings.db_path).outbound_delivery_summary(household_id=household.id)

    assert result.checked == 0
    assert retry_sender.sent == []
    assert delivery["ready"] is True
    assert delivery["failed"] == 0
    assert delivery["canceled"] == 1


def test_google_oauth_start_requires_encryption_key(tmp_path):
    settings = _settings(tmp_path, token_encryption_key=None)
    client = TestClient(create_app(settings))

    response = client.post("/dev/oauth/google/start", json={"chat_id": "family-chat"})

    assert response.status_code == 503
    assert "FLORENCE_TOKEN_ENCRYPTION_KEY" in response.json()["detail"]["missing"]


def test_oauth_state_expires_and_is_single_use(tmp_path):
    store = Store(str(tmp_path / "florence.sqlite"))
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    store.create_oauth_state(
        state="state-123",
        provider="google",
        chat_id="family-chat",
        account_label=None,
        expires_at_utc=now + timedelta(minutes=15),
        now_utc=now,
    )

    consumed = store.consume_oauth_state(
        state="state-123",
        provider="google",
        now_utc=now + timedelta(minutes=1),
    )
    replay = store.consume_oauth_state(
        state="state-123",
        provider="google",
        now_utc=now + timedelta(minutes=2),
    )
    store.create_oauth_state(
        state="expired-state",
        provider="google",
        chat_id="family-chat",
        account_label=None,
        expires_at_utc=now - timedelta(minutes=1),
        now_utc=now - timedelta(minutes=2),
    )
    expired = store.consume_oauth_state(
        state="expired-state",
        provider="google",
        now_utc=now,
    )

    assert consumed is not None
    assert consumed.used_at_utc == now + timedelta(minutes=1)
    assert replay is None
    assert expired is None
