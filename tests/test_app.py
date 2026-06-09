import base64
import hashlib
import hmac
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
from fastapi.testclient import TestClient

from florence.app import create_app
from florence import tone
from florence.config import Settings
from florence.linq import LinqClient
from florence.models import (
    ActionExecutionStatus,
    MemoryKind,
    MemberRole,
    MessageDirection,
    OutboundMessage,
    SourceDecision,
    SourceItem,
    SourcePreferenceKind,
)
from florence.oauth import TokenVault
from florence.service import FlorenceService
from florence.source_ingest import MAX_SOURCE_BODY_CHARS
from florence.source_providers import ProviderBatch
from florence.store import Store
from florence.worker import run_source_sync_tick


PINNED_HERMES_REF = "0123456789abcdef0123456789abcdef01234567"
LIVE_VERIFICATION_EVIDENCE = {
    "linq_live_verified_at": "2026-06-05T16:00:00Z",
    "linq_live_verification_proof": "Linq webhook received message and outbound iMessage arrived",
    "google_live_verified_at": "2026-06-05T16:05:00Z",
    "google_live_verification_proof": "Google OAuth completed and source sync imported a test item",
    "hermes_live_verified_at": "2026-06-05T16:10:00Z",
    "hermes_live_verification_proof": "Hermes smoke endpoint returned live_hermes_verified=true",
}


def _settings(tmp_path, **overrides):
    values = {"db_path": str(tmp_path / "florence.sqlite")}
    values.update(overrides)
    database_url = str(values.get("database_url") or "")
    if database_url.startswith(("postgres://", "postgresql://")) and "hermes_strict" not in overrides:
        values["hermes_strict"] = True
    return Settings(**values)


class FakeLinqClient:
    def __init__(self):
        self.sent = []
        self.created = []

    def send_text(self, *, chat_id: str, text: str, idempotency_key: str):
        self.sent.append(
            {
                "chat_id": chat_id,
                "text": text,
                "idempotency_key": idempotency_key,
            }
        )
        return {"ok": True}

    def create_chat(self, *, from_phone: str, to: tuple[str, ...], text: str, idempotency_key: str):
        self.created.append(
            {
                "from_phone": from_phone,
                "to": to,
                "text": text,
                "idempotency_key": idempotency_key,
            }
        )
        return {"chat": {"id": "group-chat"}}


class FlakyLinqClient(FakeLinqClient):
    def __init__(self, *, fail_send: int = 0, fail_create: int = 0):
        super().__init__()
        self.fail_send = fail_send
        self.fail_create = fail_create

    def send_text(self, *, chat_id: str, text: str, idempotency_key: str):
        if self.fail_send:
            self.fail_send -= 1
            raise RuntimeError("linq send unavailable")
        return super().send_text(chat_id=chat_id, text=text, idempotency_key=idempotency_key)

    def create_chat(self, *, from_phone: str, to: tuple[str, ...], text: str, idempotency_key: str):
        if self.fail_create:
            self.fail_create -= 1
            raise RuntimeError("linq chat unavailable")
        return super().create_chat(
            from_phone=from_phone,
            to=to,
            text=text,
            idempotency_key=idempotency_key,
        )


class FakeAgent:
    def __init__(self, response: str):
        self.response = response
        self.calls = []

    def complete(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


class FakeConnectedSourceProvider:
    provider = "google"

    def __init__(self, batch: ProviderBatch):
        self.batch = batch
        self.seen_accounts = []

    def fetch(self, account, *, now_utc: datetime):
        self.seen_accounts.append(account)
        return self.batch


class ProductionLikeStore(Store):
    def __init__(self, path: str):
        super().__init__(path)
        self.backend = "postgres"

    def connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn


def _production_like_store(settings: Settings) -> Store:
    return ProductionLikeStore(settings.db_path)


def _attach_google_oauth_token(
    store: Store,
    *,
    settings: Settings,
    chat_id: str,
    now: datetime,
) -> None:
    household = store.get_household_by_chat(chat_id)
    assert household is not None
    accounts = store.list_connected_accounts(household.id)
    assert accounts
    vault = TokenVault.from_settings(settings)
    expires_at = now + timedelta(hours=1)
    store.upsert_connected_account_token(
        connected_account_id=accounts[0].id,
        provider="google",
        token_ciphertext=vault.encrypt(
            {
                "provider": "google",
                "access_token": "pilot-access-token",
                "refresh_token": "pilot-refresh-token",
                "expires_at_utc": expires_at.isoformat(),
            }
        ),
        scopes=("openid", "email", "https://www.googleapis.com/auth/gmail.readonly"),
        expires_at_utc=expires_at,
        now_utc=now,
    )


def _write_compatible_hermes_agent(hermes_path):
    hermes_path.mkdir()
    (hermes_path / ".florence-hermes-ref").write_text(PINNED_HERMES_REF)
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        return {'final_response': 'ok'}\n"
    )


def _hermes_preflight_deployment(tmp_path, hermes_path, chat_id: str) -> dict:
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    Store(settings.db_path).get_or_create_household(
        chat_id=chat_id,
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings))
    response = client.get(
        f"/dev/pilot-check/{chat_id}",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    assert response.status_code == 200
    return response.json()["deployment"]


def _signed_linq_headers(secret: str, raw_body: bytes) -> dict[str, str]:
    timestamp = str(int(time.time()))
    signature = hmac.new(
        secret.encode("utf-8"),
        timestamp.encode("utf-8") + b"." + raw_body,
        hashlib.sha256,
    ).hexdigest()
    return {
        "content-type": "application/json",
        "x-webhook-timestamp": timestamp,
        "x-webhook-signature": signature,
    }


def _standard_signed_linq_headers(secret: str, raw_body: bytes) -> dict[str, str]:
    webhook_id = "evt_standard_test"
    timestamp = str(int(time.time()))
    key = base64.b64decode(secret.removeprefix("whsec_"))
    signature = base64.b64encode(
        hmac.new(
            key,
            webhook_id.encode("utf-8") + b"." + timestamp.encode("utf-8") + b"." + raw_body,
            hashlib.sha256,
        ).digest()
    ).decode("ascii")
    return {
        "content-type": "application/json",
        "webhook-id": webhook_id,
        "webhook-timestamp": timestamp,
        "webhook-signature": f"v1,{signature}",
    }


def _linq_payload(
    *,
    chat_id: str,
    message_id: str,
    sender: str,
    text: str,
    sent_at: datetime,
) -> dict[str, object]:
    return {
        "event": "message.received",
        "data": {
            "chat": {"id": chat_id},
            "message": {
                "id": message_id,
                "from": sender,
                "parts": [{"type": "text", "value": text}],
                "sent_at": sent_at.isoformat(),
            },
        },
    }


def _post_signed_linq(
    client: TestClient,
    *,
    secret: str,
    chat_id: str,
    message_id: str,
    sender: str = "+15555550100",
    text: str,
    sent_at: datetime,
):
    payload = _linq_payload(
        chat_id=chat_id,
        message_id=message_id,
        sender=sender,
        text=text,
        sent_at=sent_at,
    )
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return client.post(
        "/webhooks/linq",
        content=raw,
        headers=_signed_linq_headers(secret, raw),
    )


def test_linq_webhook_allows_unsigned_local_sqlite_smoke(tmp_path):
    settings = _settings(tmp_path)
    client = TestClient(create_app(settings, linq_client=FakeLinqClient()))

    response = client.post(
        "/webhooks/linq",
        json=_linq_payload(
            chat_id="unsigned-local-chat",
            message_id="unsigned-local-message",
            sender="+15555550100",
            text="my name is Sam",
            sent_at=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
        ),
    )

    assert response.status_code == 200
    assert response.json()["sent"] == 1


def test_linq_webhook_accepts_standard_webhook_signature_headers(tmp_path):
    secret = "whsec_" + base64.b64encode(b"standard-webhook-key").decode("ascii")
    settings = _settings(tmp_path, linq_webhook_secret=secret)
    client = TestClient(create_app(settings, linq_client=FakeLinqClient()))
    payload = _linq_payload(
        chat_id="standard-signed-chat",
        message_id="standard-signed-message",
        sender="+15555550100",
        text="hi",
        sent_at=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

    response = client.post(
        "/webhooks/linq",
        content=raw,
        headers=_standard_signed_linq_headers(secret, raw),
    )

    assert response.status_code == 200
    assert response.json()["sent"] == 1


def test_web_onboarding_saves_primary_profile_and_partner_invite(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    settings = _settings(
        tmp_path,
        web_base_url="https://florence.example.com",
        onboarding_state_secret="setup-secret",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
        token_encryption_key=TokenVault.generate_key(),
    )
    store = Store(settings.db_path)
    household = store.get_or_create_household(
        chat_id="onboarding-chat",
        timezone_name="America/Los_Angeles",
        now_utc=now,
    )
    actor = store.get_or_create_member(
        household_id=household.id,
        phone="+15555550100",
        now_utc=now,
    )
    service = FlorenceService(settings=settings, store=store, agent=FakeAgent("unused"))
    token = service.onboarding_url(
        chat_id=household.chat_id,
        member_id=actor.id,
        role="primary",
        now_utc=now,
    ).rsplit("/", 1)[1]
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient(), now_fn=lambda: now))

    form = client.get(f"/onboarding/{token}")
    response = client.post(
        f"/onboarding/{token}",
        data={
            "parent_name": "Hari",
            "partner_phone": "+1 (555) 555-0101",
            "household_name": "Barasu household",
            "location": "Cambridge",
            "child_1_name": "Maya",
            "child_1_age": "6",
            "child_1_grade": "1",
            "child_1_school": "Lincoln",
            "child_1_activities": "soccer",
            "child_1_location": "Lincoln field",
            "pets": "Rio the dog",
            "caretakers": "Nina, nanny, Tuesdays",
            "tone_preference": "Warm and concise",
            "source_rule": "always tell me about permission slips",
            "family_context": "Both parents travel for work sometimes",
        },
    )
    edited_form = client.get(f"/onboarding/{token}")
    readiness = service.readiness_snapshot(chat_id=household.chat_id, now_utc=now)
    memory = service.memory_snapshot(chat_id=household.chat_id, now_utc=now)
    preferences = service.source_preferences(chat_id=household.chat_id)
    members = store.list_members(household.id)

    assert form.status_code == 200
    assert "Connect Google Calendar and Gmail" in form.text
    assert 'data-initial-step="sources"' in form.text
    assert 'data-step="household" hidden' in form.text
    assert response.status_code == 200
    assert "Partner Invite" in response.text
    assert "Saved Details" in response.text
    assert "Child profile: Maya; age 6" in response.text
    assert "permission slips" in response.text
    assert "https://florence.example.com/onboarding/" in response.text
    assert edited_form.status_code == 200
    assert 'value="Hari"' in edited_form.text
    assert 'value="Barasu household"' in edited_form.text
    assert 'value="Cambridge"' in edited_form.text
    assert 'value="+15555550101"' in edited_form.text
    assert 'value="Maya"' in edited_form.text
    assert 'value="Lincoln"' in edited_form.text
    assert "Rio the dog" in edited_form.text
    assert "Nina, nanny, Tuesdays" in edited_form.text
    assert "Both parents travel for work sometimes" in edited_form.text
    assert "permission slips" in edited_form.text
    assert readiness.parent_count == 2
    assert readiness.child_count == 1
    assert readiness.source_preference_count == 1
    assert [member.display_name for member in members][0] == "Hari"
    assert "+15555550101" in [member.phone for member in members]
    assert any(item.text.startswith("Child profile: Maya; age 6") for item in memory.memories)
    assert any(item.text == "Household location: Cambridge." for item in memory.memories)
    assert preferences[0].phrase == "permission slips"


def test_web_onboarding_google_route_starts_oauth_state(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    settings = _settings(
        tmp_path,
        web_base_url="https://florence.example.com",
        onboarding_state_secret="setup-secret",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
        token_encryption_key=TokenVault.generate_key(),
    )
    store = Store(settings.db_path)
    household = store.get_or_create_household(
        chat_id="onboarding-google",
        timezone_name="America/Los_Angeles",
        now_utc=now,
    )
    actor = store.get_or_create_member(
        household_id=household.id,
        phone="+15555550100",
        now_utc=now,
    )
    service = FlorenceService(settings=settings, store=store, agent=FakeAgent("unused"))
    token = service.onboarding_url(
        chat_id=household.chat_id,
        member_id=actor.id,
        role="primary",
        now_utc=now,
    ).rsplit("/", 1)[1]
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient(), now_fn=lambda: now))

    response = client.get(f"/onboarding/{token}/google", follow_redirects=False)
    parsed = urlparse(response.headers["location"])
    state = parse_qs(parsed.query)["state"][0]
    oauth_state = store.consume_oauth_state(
        state=state,
        provider="google",
        now_utc=now + timedelta(minutes=1),
    )

    assert response.status_code == 303
    assert parsed.netloc == "accounts.google.com"
    assert oauth_state is not None
    assert oauth_state.chat_id == "onboarding-google"
    assert oauth_state.return_path == f"/onboarding/{token}?google=connected"


def test_postgres_linq_webhook_requires_configured_secret(tmp_path):
    settings = _settings(
        tmp_path,
        database_url="postgresql://florence:secret@db:5432/florence",
    )
    store = Store(settings.db_path)
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))

    response = client.post(
        "/webhooks/linq",
        json=_linq_payload(
            chat_id="unsigned-postgres-chat",
            message_id="unsigned-postgres-message",
            sender="+15555550100",
            text="my name is Sam",
            sent_at=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
        ),
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "linq_webhook_secret_required"
    assert store.get_household_by_chat("unsigned-postgres-chat") is None


def test_linq_webhook_records_live_verification_after_real_linq_send(tmp_path):
    secret = "linq-webhook-secret"
    settings = _settings(
        tmp_path,
        database_url="postgresql://florence:secret@db:5432/florence",
        linq_api_key="linq-api-key",
        linq_webhook_secret=secret,
        linq_from_phone="+15555550000",
    )
    store = _production_like_store(settings)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"message": {"id": "outbound-live-1"}})

    linq = LinqClient(
        settings,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    client = TestClient(create_app(settings, store=store, linq_client=linq))
    sent_at = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    response = _post_signed_linq(
        client,
        secret=secret,
        chat_id="live-linq-chat",
        message_id="live-linq-message",
        text="my name is Sam",
        sent_at=sent_at,
    )

    assert response.status_code == 200
    assert response.json()["sent"] == 1
    assert requests
    assert requests[0].url.path == "/api/partner/v3/chats/live-linq-chat/messages"
    assert store.list_live_verifications()["linq"] == {
        "name": "linq",
        "verified_at_utc": "2026-06-05T16:00:00+00:00",
        "proof": "Linq webhook received inbound and outbound iMessage send succeeded",
        "source": "linq_webhook",
        "updated_at_utc": "2026-06-05T16:00:00+00:00",
    }


def test_linq_webhook_does_not_record_live_verification_from_fake_sender(tmp_path):
    secret = "linq-webhook-secret"
    settings = _settings(
        tmp_path,
        database_url="postgresql://florence:secret@db:5432/florence",
        linq_api_key="linq-api-key",
        linq_webhook_secret=secret,
        linq_from_phone="+15555550000",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))

    response = _post_signed_linq(
        client,
        secret=secret,
        chat_id="fake-linq-chat",
        message_id="fake-linq-message",
        text="my name is Sam",
        sent_at=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )

    assert response.status_code == 200
    assert response.json()["sent"] == 1
    assert store.list_live_verifications() == {}


def test_dev_endpoints_are_open_for_local_smoke_by_default(tmp_path):
    settings = _settings(tmp_path)
    Store(settings.db_path).get_or_create_household(
        chat_id="local-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings))

    response = client.get("/dev/privacy/local-chat")

    assert response.status_code == 200
    assert response.json()["privacy"]["memory_enabled"] is True


def test_postgres_runtime_requires_admin_key_for_dev_endpoints(tmp_path):
    settings = _settings(
        tmp_path,
        database_url="postgresql://florence:secret@db:5432/florence",
    )
    store = Store(settings.db_path)
    store.get_or_create_household(
        chat_id="postgres-dev-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings, store=store))

    response = client.get("/dev/privacy/postgres-dev-chat")

    assert response.status_code == 401
    assert response.json()["detail"] == "admin_api_key_required"


def test_postgres_dev_write_endpoints_require_explicit_chat_id(tmp_path):
    settings = _settings(
        tmp_path,
        database_url="postgresql://florence:secret@db:5432/florence",
        admin_api_key="secret-admin-key",
        token_encryption_key=TokenVault.generate_key(),
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = Store(settings.db_path)
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))
    headers = {"x-florence-admin-key": "secret-admin-key"}
    starts_at = datetime(2026, 6, 6, 16, 0, tzinfo=timezone.utc).isoformat()

    cases = [
        ("/dev/messages", {"message_id": "missing-chat-message", "text": "my name is Sam"}),
        ("/dev/source-items", {"title": "Permission slip", "external_id": "source-1"}),
        (
            "/dev/import/email",
            {"subject": "Permission slip", "body": "Due Friday", "sender": "school@example.com"},
        ),
        ("/dev/import/calendar", {"title": "Soccer", "starts_at_utc": starts_at}),
        ("/dev/sync-sources", {"external_account_id": "parent@example.com"}),
        ("/dev/oauth/google/start", {}),
        (
            "/dev/actions",
            {"action_type": "send_message", "summary": "Send note", "payload": {"text": "hi"}},
        ),
    ]
    for path, payload in cases:
        response = client.post(path, json=payload, headers=headers)
        assert response.status_code == 400, path
        assert response.json()["detail"] == "chat_id_required"

    assert store.get_household_by_chat("dev-chat") is None


def test_admin_api_key_guards_dev_endpoints(tmp_path):
    settings = _settings(tmp_path, admin_api_key="secret-admin-key")
    Store(settings.db_path).get_or_create_household(
        chat_id="admin-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings))

    missing = client.get("/dev/privacy/admin-chat")
    wrong = client.get(
        "/dev/privacy/admin-chat",
        headers={"authorization": "Bearer wrong"},
    )
    authorized = client.get(
        "/dev/privacy/admin-chat",
        headers={"authorization": "Bearer secret-admin-key"},
    )

    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert authorized.status_code == 200


def test_admin_api_key_accepts_explicit_header(tmp_path):
    settings = _settings(tmp_path, admin_api_key="secret-admin-key")
    Store(settings.db_path).get_or_create_household(
        chat_id="admin-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings))

    response = client.get(
        "/dev/privacy/admin-chat",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )

    assert response.status_code == 200


def test_readiness_endpoint_uses_admin_guard(tmp_path):
    settings = _settings(tmp_path, admin_api_key="secret-admin-key")
    Store(settings.db_path).get_or_create_household(
        chat_id="admin-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings))

    blocked = client.get("/dev/readiness/admin-chat")
    allowed = client.get(
        "/dev/readiness/admin-chat",
        headers={"authorization": "Bearer secret-admin-key"},
    )

    assert blocked.status_code == 401
    assert allowed.status_code == 200
    assert allowed.json()["readiness"]["ready"] is False


def test_pilot_check_reports_household_and_deployment_gaps(tmp_path):
    settings = _settings(tmp_path, admin_api_key="secret-admin-key")
    Store(settings.db_path).get_or_create_household(
        chat_id="pilot-gaps-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings))

    blocked = client.get("/dev/pilot-check/pilot-gaps-chat")
    allowed = client.get(
        "/dev/pilot-check/pilot-gaps-chat",
        headers={"authorization": "Bearer secret-admin-key"},
    )
    payload = allowed.json()

    assert blocked.status_code == 401
    assert allowed.status_code == 200
    assert payload["pilot_ready"] is False
    assert payload["household"]["ready"] is False
    assert "Invite or confirm your partner as the second parent." in payload["household"]["missing"]
    assert payload["deployment"]["ready"] is False
    assert "LINQ_API_KEY" in payload["deployment"]["missing_required"]
    assert "FLORENCE_DATABASE_URL" in payload["deployment"]["missing_required"]
    assert "FLORENCE_HERMES_AGENT_PATH" in payload["deployment"]["missing_required"]
    assert "HERMES_AGENT_REF" in payload["deployment"]["missing_required"]
    assert "FLORENCE_HERMES_PROVIDER" in payload["deployment"]["missing_required"]
    assert "FLORENCE_HERMES_MODEL" in payload["deployment"]["missing_required"]
    assert payload["deployment"]["invalid"] == []
    live = payload["deployment"]["live_verification"]
    assert live["ready"] is False
    assert "Public typed source ingest, bounded Need-to-Know triage, and no raw inbox dump" in live["locally_verified"]
    assert (
        "Connected-source worker sync, cursor advancement, Need-to-Know surfacing, and source delivery retry"
        in live["locally_verified"]
    )
    assert (
        "Hermes proposal boundary, checkout contract preflight, runtime toolset guard, and smoke endpoint"
        in live["locally_verified"]
    )
    assert "Warm deterministic help, support, and fallback tone without Hermes" in live["locally_verified"]
    assert "LINQ_API_KEY" in live["external_credentials_needed"]
    assert "GOOGLE_CLIENT_ID" in live["external_credentials_needed"]
    assert live["unverified"] == []
    assert live["blocked"] == ["External credentials/configuration required"]
    live_steps = {step["id"]: step for step in live["next_steps"]}
    assert live_steps["linq"]["missing_env"] == [
        "LINQ_WEBHOOK_SECRET",
        "LINQ_API_KEY",
        "LINQ_FROM_PHONE",
    ]
    assert live_steps["google"]["missing_env"] == [
        "GOOGLE_CLIENT_ID",
        "GOOGLE_CLIENT_SECRET",
        "GOOGLE_REDIRECT_URI",
        "FLORENCE_TOKEN_ENCRYPTION_KEY",
    ]
    assert live_steps["hermes"]["required_env"] == [
        "FLORENCE_HERMES_AGENT_PATH",
        "HERMES_AGENT_REF",
        "FLORENCE_HERMES_PROVIDER",
        "FLORENCE_HERMES_MODEL",
    ]
    deployment_steps = {step["id"]: step for step in payload["deployment"]["operator_next_steps"]}
    assert deployment_steps["production_environment"]["status"] == "missing_env"
    assert "FLORENCE_DATABASE_URL" in deployment_steps["production_environment"]["missing_env"]
    assert deployment_steps["live_linq"]["proof_env"] == [
        "FLORENCE_LINQ_LIVE_VERIFIED_AT",
        "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF",
    ]
    pilot_steps = {step["id"]: step for step in payload["operator_next_steps"]}
    assert pilot_steps["two_parent_household_setup"]["status"] == "pilot_smoke_blocked"


def test_deployment_check_reports_global_gaps_without_household(tmp_path):
    settings = _settings(tmp_path, admin_api_key="secret-admin-key")
    store = Store(settings.db_path)
    client = TestClient(create_app(settings, store=store))

    blocked = client.get("/dev/deployment-check")
    allowed = client.get(
        "/dev/deployment-check",
        headers={"authorization": "Bearer secret-admin-key"},
    )
    payload = allowed.json()["deployment"]

    assert blocked.status_code == 401
    assert allowed.status_code == 200
    assert store.list_households() == []
    assert payload["ready"] is False
    assert "LINQ_API_KEY" in payload["missing_required"]
    assert "FLORENCE_DATABASE_URL" in payload["missing_required"]
    assert "FLORENCE_HERMES_AGENT_PATH" in payload["missing_required"]
    assert "HERMES_AGENT_REF" in payload["missing_required"]
    assert payload["database"]["configured_backend"] == "sqlite"
    assert payload["database"]["store_backend"] == "sqlite"
    assert payload["database"]["backend_matches"] is True
    assert payload["database"]["reachable"] is True
    assert payload["database"]["schema_ready"] is True
    assert payload["database"]["schema"]["ready"] is True
    live = payload["live_verification"]
    assert live["ready"] is False
    assert "LINQ_API_KEY" in live["external_credentials_needed"]
    assert live["unverified"] == []
    assert live["blocked"] == ["External credentials/configuration required"]
    operator_steps = {step["id"]: step for step in payload["operator_next_steps"]}
    assert operator_steps["production_environment"]["status"] == "missing_env"
    assert "FLORENCE_DATABASE_URL" in operator_steps["production_environment"]["missing_env"]
    assert operator_steps["live_hermes"]["required_env"] == [
        "FLORENCE_HERMES_AGENT_PATH",
        "HERMES_AGENT_REF",
        "FLORENCE_HERMES_PROVIDER",
        "FLORENCE_HERMES_MODEL",
    ]


def test_deployment_check_passes_when_global_settings_and_live_checks_are_ready(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        pending_action_ttl_minutes=7 * 24 * 60,
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        "/dev/deployment-check",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["deployment"]

    assert response.status_code == 200
    assert store.list_households() == []
    assert payload["ready"] is True
    assert payload["missing_required"] == []
    assert payload["invalid"] == []
    assert payload["database_backend"] == "postgres"
    assert payload["database"]["configured_backend"] == "postgres"
    assert payload["database"]["store_backend"] == "postgres"
    assert payload["database"]["backend_matches"] is True
    assert payload["database"]["reachable"] is True
    assert payload["database"]["schema_ready"] is True
    assert payload["database"]["schema"]["ready"] is True
    assert payload["hermes_toolsets"] == []
    assert payload["hermes_agent_ref"] == PINNED_HERMES_REF
    assert payload["hermes_checkout_ref"] == PINNED_HERMES_REF
    assert payload["hermes_ref_matches"] is True
    assert payload["live_verification"]["ready"] is True
    assert payload["live_verification"]["evidence_gaps"] == []
    assert payload["live_verification"]["next_steps"] == []
    assert payload["operator_next_steps"] == []
    assert payload["live_verification"]["evidence"]["linq"] == {
        "verified": True,
        "verified_at_utc": "2026-06-05T16:00:00+00:00",
        "proof": "Linq webhook received message and outbound iMessage arrived",
        "missing": [],
    }
    assert payload["live_verification"]["evidence"]["google"] == {
        "verified": True,
        "verified_at_utc": "2026-06-05T16:05:00+00:00",
        "proof": "Google OAuth completed and source sync imported a test item",
        "missing": [],
    }
    assert payload["live_verification"]["evidence"]["hermes"] == {
        "verified": True,
        "verified_at_utc": "2026-06-05T16:10:00+00:00",
        "proof": "Hermes smoke endpoint returned live_hermes_verified=true",
        "missing": [],
    }


def test_deployment_check_accepts_stored_live_verification_records(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        pending_action_ttl_minutes=7 * 24 * 60,
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store))
    headers = {"x-florence-admin-key": "secret-admin-key"}

    for name, payload in {
        "linq": {
            "verified_at_utc": "2026-06-05T16:00:00Z",
            "proof": "Linq webhook received message and outbound iMessage arrived",
        },
        "google": {
            "verified_at_utc": "2026-06-05T16:05:00Z",
            "proof": "Google OAuth completed and source sync imported a test item",
        },
        "hermes": {
            "verified_at_utc": "2026-06-05T16:10:00Z",
            "proof": "Hermes smoke endpoint returned live_hermes_verified true",
        },
    }.items():
        response = client.post(
            f"/dev/live-verifications/{name}",
            json=payload,
            headers=headers,
        )
        assert response.status_code == 200
        assert response.json()["verification"]["source"] == "operator"

    deployment = client.get("/dev/deployment-check", headers=headers).json()["deployment"]
    verifications = client.get("/dev/live-verifications", headers=headers).json()[
        "verifications"
    ]

    assert deployment["ready"] is True
    assert deployment["live_verification"]["verified"] == {
        "linq": True,
        "google": True,
        "hermes": True,
    }
    assert deployment["live_verification"]["evidence_gaps"] == []
    assert deployment["live_verification"]["unverified"] == []
    assert deployment["live_verification"]["evidence"]["linq"] == {
        "verified": True,
        "verified_at_utc": "2026-06-05T16:00:00+00:00",
        "proof": "Linq webhook received message and outbound iMessage arrived",
        "missing": [],
    }
    assert verifications["linq"]["proof"] == (
        "Linq webhook received message and outbound iMessage arrived"
    )
    assert verifications["hermes"]["source"] == "operator"


def test_live_verification_endpoint_rejects_unsafe_proof_without_echoing_it(tmp_path):
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        linq_api_key="secret-linq-key",
    )
    store = Store(settings.db_path)
    client = TestClient(create_app(settings, store=store))
    unsafe_proof = "Linq smoke sent +15555550123 with secret-linq-key"

    response = client.post(
        "/dev/live-verifications/linq",
        json={
            "verified_at_utc": "2026-06-05T16:00:00Z",
            "proof": unsafe_proof,
        },
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    blob = json.dumps(response.json())

    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "unsafe_live_verification_proof"
    assert "+15555550123" not in blob
    assert "secret-linq-key" not in blob
    assert store.list_live_verifications() == {}


def test_deployment_check_blocks_postgres_when_hermes_lock_is_thread_only(
    tmp_path, monkeypatch
):
    lock_error = "POSIX fcntl file locking is required for deployed SaaS Hermes traffic"
    monkeypatch.setattr("florence.app.hermes_runtime_lock_error", lambda: lock_error)
    monkeypatch.setattr(
        "florence.app.hermes_runtime_lock_mode",
        lambda: "thread_lock_only_no_interprocess_lock",
    )
    monkeypatch.setattr(
        "florence.app.hermes_runtime_concurrency_mode",
        lambda: "serialized_by_thread_lock_only",
    )
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        pending_action_ttl_minutes=7 * 24 * 60,
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        "/dev/deployment-check",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["deployment"]

    assert response.status_code == 200
    assert payload["ready"] is False
    assert payload["database_backend"] == "postgres"
    assert payload["hermes_runtime_concurrency"] == "serialized_by_thread_lock_only"
    assert payload["hermes_runtime_lock"] == "thread_lock_only_no_interprocess_lock"
    assert payload["invalid"] == [lock_error]
    assert payload["live_verification"]["ready"] is False
    assert payload["live_verification"]["blocked"] == ["Pilot deployment safety preflight"]
    operator_steps = {step["id"]: step for step in payload["operator_next_steps"]}
    assert operator_steps["deployment_preflight"]["blocked_by"] == [lock_error]


def test_deployment_check_blocks_nonstrict_hermes_for_postgres_pilot(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        pending_action_ttl_minutes=7 * 24 * 60,
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        hermes_strict=False,
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        "/dev/deployment-check",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["deployment"]

    assert response.status_code == 200
    assert payload["ready"] is False
    assert payload["hermes_strict"] is False
    assert payload["invalid"] == [
        "FLORENCE_HERMES_STRICT must be true for multi-family SaaS pilots; "
        "Hermes contract violations must not fall back silently"
    ]
    assert payload["live_verification"]["blocked"] == ["Pilot deployment safety preflight"]
    assert payload["operator_next_steps"][0]["id"] == "deployment_preflight"


def test_deployment_check_blocks_live_flags_without_proof_metadata(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        pending_action_ttl_minutes=7 * 24 * 60,
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        "/dev/deployment-check",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    live = response.json()["deployment"]["live_verification"]

    assert response.status_code == 200
    assert live["ready"] is False
    assert live["unverified"] == []
    assert live["verified"] == {"linq": True, "google": True, "hermes": True}
    assert live["evidence_gaps"] == [
        "FLORENCE_LINQ_LIVE_VERIFIED_AT",
        "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF",
        "FLORENCE_GOOGLE_LIVE_VERIFIED_AT",
        "FLORENCE_GOOGLE_LIVE_VERIFICATION_PROOF",
        "FLORENCE_HERMES_LIVE_VERIFIED_AT",
        "FLORENCE_HERMES_LIVE_VERIFICATION_PROOF",
    ]
    assert live["blocked"] == ["Live verification proof metadata required"]


def test_deployment_check_blocks_unsafe_live_verification_proof_notes(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        pending_action_ttl_minutes=7 * 24 * 60,
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        linq_live_verified_at="2026-06-05T16:00:00Z",
        linq_live_verification_proof="Linq smoke included +15555550123",
        google_live_verified_at="2026-06-05T16:05:00Z",
        google_live_verification_proof="Google smoke Bearer abcdefghijklmnop",
        hermes_live_verified_at="2026-06-05T16:10:00Z",
        hermes_live_verification_proof='{"live_hermes_verified": true}\nraw response',
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        "/dev/deployment-check",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["deployment"]
    live = payload["live_verification"]
    response_blob = json.dumps(response.json())

    assert response.status_code == 200
    assert payload["ready"] is False
    assert live["blocked"] == ["Live verification proof metadata required"]
    assert live["evidence"]["linq"]["proof"] is None
    assert live["evidence"]["google"]["proof"] is None
    assert live["evidence"]["hermes"]["proof"] is None
    assert live["evidence_gaps"] == [
        "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF must be a short proof note without secrets, tokens, PII, raw payloads, or line breaks",
        "FLORENCE_GOOGLE_LIVE_VERIFICATION_PROOF must be a short proof note without secrets, tokens, PII, raw payloads, or line breaks",
        "FLORENCE_HERMES_LIVE_VERIFICATION_PROOF must be a short proof note without secrets, tokens, PII, raw payloads, or line breaks",
    ]
    assert "+15555550123" not in response_blob
    assert "abcdefghijklmnop" not in response_blob
    assert "raw response" not in response_blob


def test_deployment_check_blocks_invalid_token_encryption_key_without_echoing_it(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    invalid_key = "not-a-valid-fernet-key"
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=invalid_key,
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        "/dev/deployment-check",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["deployment"]
    response_blob = json.dumps(response.json())

    assert response.status_code == 200
    assert payload["ready"] is False
    assert payload["invalid"] == ["FLORENCE_TOKEN_ENCRYPTION_KEY must be a valid Fernet key"]
    assert payload["live_verification"]["blocked"] == ["Pilot deployment safety preflight"]
    assert payload["operator_next_steps"][0]["id"] == "deployment_preflight"
    assert payload["operator_next_steps"][0]["blocked_by"] == [
        "FLORENCE_TOKEN_ENCRYPTION_KEY must be a valid Fernet key"
    ]
    assert invalid_key not in response_blob


def test_deployment_check_blocks_malformed_live_verification_timestamp(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **{
            **LIVE_VERIFICATION_EVIDENCE,
            "linq_live_verified_at": "2026-06-05 16:00:00",
        },
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        "/dev/deployment-check",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    live = response.json()["deployment"]["live_verification"]

    assert response.status_code == 200
    assert live["ready"] is False
    assert live["evidence"]["linq"]["verified_at_utc"] is None
    assert live["evidence_gaps"] == [
        "FLORENCE_LINQ_LIVE_VERIFIED_AT must be an ISO-8601 timestamp with timezone"
    ]
    assert live["blocked"] == ["Live verification proof metadata required"]


def test_deployment_check_blocks_future_live_verification_timestamp(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    future = datetime.now(timezone.utc) + timedelta(days=3)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **{
            **LIVE_VERIFICATION_EVIDENCE,
            "linq_live_verified_at": future.isoformat(),
        },
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        "/dev/deployment-check",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    live = response.json()["deployment"]["live_verification"]

    assert response.status_code == 200
    assert live["ready"] is False
    assert live["evidence"]["linq"]["verified_at_utc"] is None
    assert live["evidence_gaps"] == [
        "FLORENCE_LINQ_LIVE_VERIFIED_AT must not be in the future"
    ]
    assert live["blocked"] == ["Live verification proof metadata required"]


def test_deployment_check_blocks_relative_hermes_runtime_home(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        hermes_runtime_home="relative-hermes-home",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        "/dev/deployment-check",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    deployment = response.json()["deployment"]

    assert response.status_code == 200
    assert deployment["ready"] is False
    assert deployment["hermes_runtime_home"] == "relative-hermes-home"
    assert deployment["invalid"] == [
        "FLORENCE_HERMES_RUNTIME_HOME must be an absolute path"
    ]
    assert deployment["live_verification"]["blocked"] == [
        "Pilot deployment safety preflight"
    ]


def test_deployment_check_blocks_unreachable_database_without_leaking_dsn_secret(tmp_path):
    class UnreachableStore(Store):
        def ping(self) -> None:
            raise RuntimeError(
                "could not connect to postgresql://florence:secret-password@db:5432/florence"
            )

    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    unreachable_store = UnreachableStore(settings.db_path)
    unreachable_store.backend = "postgres"
    client = TestClient(create_app(settings, store=unreachable_store))

    response = client.get(
        "/dev/deployment-check",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["deployment"]

    assert response.status_code == 200
    assert payload["ready"] is False
    assert "Florence database is not reachable from this process" in payload["invalid"]
    assert payload["database"]["reachable"] is False
    assert payload["database"]["backend_matches"] is True
    assert "secret-password" not in payload["database"]["error"]
    assert "postgresql://[redacted]@db:5432/florence" in payload["database"]["error"]


def test_deployment_check_blocks_configured_postgres_with_sqlite_store_override(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    client = TestClient(create_app(settings, store=Store(settings.db_path)))

    response = client.get(
        "/dev/deployment-check",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["deployment"]

    assert response.status_code == 200
    assert payload["ready"] is False
    assert payload["database"]["configured_backend"] == "postgres"
    assert payload["database"]["store_backend"] == "sqlite"
    assert payload["database"]["backend_matches"] is False
    assert payload["database"]["reachable"] is True
    assert "Florence database backend does not match FLORENCE_DATABASE_URL" in payload["invalid"]


def test_deployment_check_blocks_incompatible_database_schema(tmp_path):
    class IncompatibleSchemaStore(ProductionLikeStore):
        def schema_status(self):
            status = super().schema_status()
            status["ready"] = False
            status["missing_columns"] = {"messages": ["household_id", "chat_id"]}
            return status

    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    client = TestClient(create_app(settings, store=IncompatibleSchemaStore(settings.db_path)))

    response = client.get(
        "/dev/deployment-check",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["deployment"]
    database = payload["database"]

    assert response.status_code == 200
    assert payload["ready"] is False
    assert "Florence database schema is not compatible with this build" in payload["invalid"]
    assert database["reachable"] is True
    assert database["backend_matches"] is True
    assert database["schema_ready"] is False
    assert database["schema"]["missing_columns"] == {
        "messages": ["household_id", "chat_id"]
    }
    assert "messages missing columns: household_id, chat_id" in database["schema_error"]
    operator_steps = {step["id"]: step for step in payload["operator_next_steps"]}
    assert "messages missing columns: household_id, chat_id" in operator_steps[
        "postgres_database"
    ]["blocked_by"]


def test_pilot_check_passes_when_household_and_deployment_are_ready(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        pending_action_ttl_minutes=7 * 24 * 60,
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "pilot-ready-chat"
    admin_headers = {"x-florence-admin-key": "secret-admin-key"}

    client.post(
        "/dev/messages",
        json={"chat_id": chat_id, "message_id": "parent-name", "text": "my name is Sam", "now_utc": now.isoformat()},
        headers=admin_headers,
    )
    client.post(
        "/dev/messages",
        json={"chat_id": chat_id, "message_id": "partner", "text": "confirm partner +15555550101", "now_utc": now.isoformat()},
        headers=admin_headers,
    )
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "partner-name",
            "sender": "+15555550101",
            "text": "my name is Alex",
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    client.post(
        "/dev/messages",
        json={"chat_id": chat_id, "message_id": "child", "text": "our child is Maya", "now_utc": now.isoformat()},
        headers=admin_headers,
    )
    client.post(
        "/dev/sync-sources",
        json={
            "chat_id": chat_id,
            "provider": "google",
            "external_account_id": "parent@example.com",
            "account_label": "Parent Gmail",
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "source-rule",
            "text": "always tell me about permission slips",
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    source_response = client.post(
        "/dev/sync-sources",
        json={
            "chat_id": chat_id,
            "provider": "google",
            "external_account_id": "parent@example.com",
            "account_label": "Parent Gmail",
            "emails": [
                {
                    "subject": "Permission slip due tomorrow",
                    "body": "Please sign and return the permission slip for tomorrow's field trip.",
                    "sender": "school@example.com",
                    "external_id": "pilot-ready-source",
                    "received_at_utc": now.isoformat(),
                    "event_at_utc": (now + timedelta(days=1)).isoformat(),
                }
            ],
            "cursor": "pilot-ready-cursor",
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    assert source_response.status_code == 200
    _attach_google_oauth_token(store, settings=settings, chat_id=chat_id, now=now)
    client.post(
        "/dev/actions",
        json={
            "chat_id": chat_id,
            "action_type": "create_reminder",
            "summary": "Add reminder: sign the permission slip.",
            "payload": {
                "title": "Sign the permission slip",
                "due_at_utc": (now + timedelta(days=1)).isoformat(),
            },
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    action = client.get(f"/dev/actions/{chat_id}", headers=admin_headers).json()["actions"][0]
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "approve-source-reminder",
            "text": f"approve {action['id'][:8]}",
            "now_utc": (now + timedelta(minutes=1)).isoformat(),
        },
        headers=admin_headers,
    )
    action_tick = client.post(
        "/dev/actions/tick",
        json={"now_utc": (now + timedelta(minutes=2)).isoformat()},
        headers=admin_headers,
    )
    assert action_tick.json() == {"ok": True, "attempted": 1, "succeeded": 1, "failed": 0}

    response = client.get(f"/dev/pilot-check/{chat_id}", headers=admin_headers)
    payload = response.json()

    assert response.status_code == 200
    assert payload["pilot_ready"] is True
    assert payload["household"]["ready"] is True
    assert payload["deployment"]["ready"] is True
    assert payload["deployment"]["missing_required"] == []
    assert payload["deployment"]["invalid"] == []
    assert payload["deployment"]["database_backend"] == "postgres"
    assert payload["deployment"]["hermes_toolsets"] == []
    assert payload["deployment"]["hermes_agent_ref"] == PINNED_HERMES_REF
    assert payload["deployment"]["hermes_checkout_ref"] == PINNED_HERMES_REF
    assert payload["deployment"]["hermes_ref_matches"] is True
    assert payload["deployment"]["hermes_provider"] == "openrouter"
    assert payload["deployment"]["hermes_model"] == "nousresearch/hermes-test"
    assert payload["deployment"]["hermes_runtime_scope"] == "per_turn_under_runtime_home"
    assert payload["deployment"]["hermes_failure_cleanup"] == (
        "runtime_home_restored_and_checkout_modules_cleared_on_error"
    )
    assert payload["deployment"]["hermes_runtime_cleanup"] == "enabled"
    assert payload["deployment"]["hermes_runtime_concurrency"] == "serialized_by_thread_and_file_lock"
    assert payload["deployment"]["hermes_runtime_lock"] == "thread_lock_plus_interprocess_file_lock"
    assert payload["deployment"]["hermes_preflight_scope"] == "ephemeral_per_check_under_runtime_home"
    assert payload["deployment"]["hermes_python_path_scope"] == "temporary_during_hermes_call"
    assert payload["deployment"]["hermes_module_cache_scope"] == "shadowed_and_cleared_during_hermes_import_or_call"
    live = payload["deployment"]["live_verification"]
    assert live["ready"] is True
    assert live["external_credentials_needed"] == []
    assert live["unverified"] == []
    assert live["verified"] == {"linq": True, "google": True, "hermes": True}
    assert live["blocked"] == []
    assert payload["message_transport"]["ready"] is True
    assert payload["message_transport"]["inbound"] >= 1
    assert payload["message_transport"]["outbound"] >= 1
    assert payload["message_transport"]["missing"] == []
    assert payload["source_review"]["total"] == 1
    assert payload["source_review"]["surfaced"] == 1
    assert payload["source_review"]["connected_total"] == 1
    assert payload["source_review"]["connected_surfaced"] == 1
    assert payload["source_review"]["token_backed_google_total"] == 1
    assert payload["source_review"]["token_backed_google_surfaced"] == 1
    assert payload["source_review"]["latest_token_backed_google_synced_at_utc"] == "2026-06-05T16:00:00Z"
    assert payload["connected_accounts"]["active_google"] == 1
    assert payload["connected_accounts"]["token_backed_google"] == 1
    assert payload["delivery"]["ready"] is True
    assert payload["delivery"]["retryable"] == 0
    assert payload["actions"]["ready"] is True
    assert payload["actions"]["approved"] == 0
    assert payload["actions"]["failed"] == 0
    assert payload["actions"]["succeeded"] == 1
    assert payload["smoke_checklist"]["ready"] is True
    assert payload["smoke_checklist"]["blocked"] == []
    checklist = {step["id"]: step for step in payload["smoke_checklist"]["steps"]}
    assert checklist["deployment_preflight"]["ready"] is True
    assert checklist["two_parent_household_setup"]["ready"] is True
    assert checklist["linq_message_transport"]["ready"] is True
    assert checklist["linq_message_transport"]["evidence"]["linq_verified_at_utc"] == (
        "2026-06-05T16:00:00+00:00"
    )
    assert checklist["linq_live_round_trip"]["evidence"]["proof"] == (
        "Linq webhook received message and outbound iMessage arrived"
    )
    assert checklist["connected_source_account"]["ready"] is True
    assert checklist["connected_source_account"]["evidence"]["token_backed_google"] == 1
    assert checklist["google_live_source_sync"]["ready"] is True
    assert checklist["source_rule_and_need_to_know"]["ready"] is True
    assert checklist["source_rule_and_need_to_know"]["evidence"]["connected_total"] == 1
    assert checklist["source_rule_and_need_to_know"]["evidence"]["connected_surfaced"] == 1
    assert checklist["source_rule_and_need_to_know"]["evidence"]["token_backed_google_total"] == 1
    assert checklist["source_rule_and_need_to_know"]["evidence"]["token_backed_google_surfaced"] == 1
    assert checklist["source_rule_and_need_to_know"]["evidence"]["latest_token_backed_google_synced_at_utc"] == (
        now.isoformat()
    )
    assert checklist["hermes_saas_boundary"]["ready"] is True
    assert checklist["hermes_saas_boundary"]["evidence"]["hermes_failure_cleanup"] == (
        "runtime_home_restored_and_checkout_modules_cleared_on_error"
    )
    assert checklist["hermes_live_response"]["ready"] is True
    assert checklist["outbound_delivery_queue"]["ready"] is True
    assert checklist["approval_worker_queue"]["ready"] is True
    assert checklist["approval_worker_queue"]["evidence"]["succeeded"] == 1

    proof_response = client.get(f"/dev/pilot-proof/{chat_id}", headers=admin_headers)
    proof = proof_response.json()["proof"]
    proof_blob = json.dumps(proof)

    assert proof_response.status_code == 200
    assert proof["pilot_ready"] is True
    assert proof["sanitization"] == {
        "message_bodies": "excluded",
        "source_bodies": "excluded",
        "source_titles": "excluded",
        "source_event_times": "presence_only",
        "oauth_tokens": "excluded",
        "memory_text": "excluded",
        "action_errors": "presence_only",
        "diagnostic_strings": "redacted",
    }
    assert proof["pilot_check"]["smoke_checklist"]["ready"] is True
    assert proof["deployment"]["live_verification"]["ready"] is True
    assert proof["deployment"]["hermes_runtime_concurrency"] == "serialized_by_thread_and_file_lock"
    assert proof["deployment"]["hermes_runtime_lock"] == "thread_lock_plus_interprocess_file_lock"
    assert proof["source_review"]["token_backed_google_total"] == 1
    surfaced_proof_item = proof["source_review"]["recent_surfaced"][0]
    assert isinstance(surfaced_proof_item["id"], str) and surfaced_proof_item["id"]
    assert surfaced_proof_item == {
        "id": surfaced_proof_item["id"],
        "source_type": "email",
        "reason": "urgent_actionable_source",
        "priority": 93,
        "event_at_present": True,
    }
    assert proof["privacy"]["memory_enabled"] is True
    assert proof["action_executions"][0]["status"] == "success"
    assert proof["action_executions"][0]["error_present"] is False
    assert "error" not in proof["action_executions"][0]
    assert "Please sign and return the permission slip" not in proof_blob
    assert "Permission slip due tomorrow" not in proof_blob
    assert "pilot-access-token" not in proof_blob
    assert "refresh_token" not in proof_blob


def test_pilot_check_hermes_boundary_requires_postgres_saas_backend(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        hermes_strict=True,
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = Store(settings.db_path)
    chat_id = "pilot-local-hermes-boundary-chat"
    store.get_or_create_household(
        chat_id=chat_id,
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        f"/dev/pilot-check/{chat_id}",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()
    checklist = {step["id"]: step for step in payload["smoke_checklist"]["steps"]}

    assert response.status_code == 200
    assert payload["deployment"]["database_backend"] == "sqlite"
    assert checklist["hermes_saas_boundary"]["ready"] is False
    assert checklist["hermes_saas_boundary"]["evidence"]["database_backend"] == "sqlite"
    assert checklist["hermes_saas_boundary"]["evidence"]["hermes_failure_cleanup"] == (
        "runtime_home_restored_and_checkout_modules_cleared_on_error"
    )
    assert checklist["hermes_saas_boundary"]["blocked_by"] == [
        "FLORENCE_DATABASE_URL must point to Postgres for Hermes SaaS pilot status."
    ]


def test_pilot_proof_redacts_diagnostic_strings(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / ".florence-hermes-ref").write_text(PINNED_HERMES_REF)
    (hermes_path / "run_agent.py").write_text(
        "raise RuntimeError("
        "'preflight failed with Bearer abcdefghijklmnop and secret-admin-key "
        "plus source-key-secret and google-client-secret at "
        "postgresql://florence:db-secret-password@db:5432/florence "
        "for +15555550123 and parent@example.com'"
        ")\n"
    )
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key-secret",
        linq_api_key="linq-key-secret",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
        database_url="postgresql://florence:db-secret-password@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        **LIVE_VERIFICATION_EVIDENCE,
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
    )
    store = _production_like_store(settings)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household = store.get_or_create_household(
        chat_id="diagnostic-proof-chat",
        timezone_name="America/Los_Angeles",
        now_utc=now,
    )
    parent = store.get_or_create_member(
        household_id=household.id,
        phone="+15555550100",
        now_utc=now,
    )
    store.set_member_name(parent.id, "Sam", now_utc=now)
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))

    response = client.get(
        "/dev/pilot-proof/diagnostic-proof-chat",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    proof = response.json()["proof"]
    proof_blob = json.dumps(proof, sort_keys=True)

    assert response.status_code == 200
    assert proof["sanitization"]["diagnostic_strings"] == "redacted"
    assert proof["pilot_ready"] is False
    assert proof["deployment"]["invalid"]
    assert proof["deployment"]["operator_next_steps"][0]["status"] == "invalid_configuration"
    assert "FLORENCE_HERMES_AGENT_PATH could not import run_agent.AIAgent" in proof_blob
    assert "preflight failed" in proof_blob
    assert "postgresql://[redacted]@db:5432/florence" in proof_blob
    assert "[redacted]" in proof_blob
    assert "[redacted secret]" in proof_blob
    assert "[phone number]" in proof_blob
    assert "[email address]" in proof_blob
    assert "abcdefghijklmnop" not in proof_blob
    assert "secret-admin-key" not in proof_blob
    assert "source-key-secret" not in proof_blob
    assert "google-client-secret" not in proof_blob
    assert "db-secret-password" not in proof_blob
    assert "+15555550123" not in proof_blob
    assert "parent@example.com" not in proof_blob


def test_pilot_proof_preserves_missing_environment_variable_names(tmp_path):
    settings = _settings(tmp_path, admin_api_key="secret-admin-key")
    Store(settings.db_path).get_or_create_household(
        chat_id="missing-env-proof-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings))

    response = client.get(
        "/dev/pilot-proof/missing-env-proof-chat",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    proof = response.json()["proof"]
    proof_blob = json.dumps(proof, sort_keys=True)

    assert response.status_code == 200
    assert proof["deployment"]["missing_required"] == [
        "LINQ_WEBHOOK_SECRET",
        "LINQ_API_KEY",
        "LINQ_FROM_PHONE",
        "FLORENCE_SOURCE_INGEST_API_KEY",
        "FLORENCE_TOKEN_ENCRYPTION_KEY",
        "FLORENCE_SUPPORT_CONTACT",
        "FLORENCE_DATABASE_URL",
        "FLORENCE_HERMES_AGENT_PATH",
        "HERMES_AGENT_REF",
        "FLORENCE_HERMES_PROVIDER",
        "FLORENCE_HERMES_MODEL",
    ]
    assert "GOOGLE_CLIENT_SECRET" in proof_blob
    assert "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF" in proof_blob
    assert "LINQ_[redacted secret]" not in proof_blob
    assert "GOOGLE_[redacted secret]" not in proof_blob
    assert "FLORENCE_SOURCE_INGEST_[redacted secret]" not in proof_blob


def test_pilot_proof_is_scoped_to_requested_household(tmp_path):
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
    )
    store = Store(settings.db_path)
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    admin_headers = {"x-florence-admin-key": "secret-admin-key"}

    household_a = store.get_or_create_household(
        chat_id="proof-family-a",
        timezone_name="America/Los_Angeles",
        now_utc=now,
    )
    household_b = store.get_or_create_household(
        chat_id="proof-family-b",
        timezone_name="America/Los_Angeles",
        now_utc=now,
    )
    parent_a = store.get_or_create_member(
        household_id=household_a.id,
        phone="+15555550100",
        now_utc=now,
    )
    parent_b = store.get_or_create_member(
        household_id=household_b.id,
        phone="+15555550200",
        now_utc=now,
    )
    store.set_member_name(parent_a.id, "Sam", now_utc=now)
    store.set_member_name(parent_b.id, "Riley", now_utc=now)
    store.add_source_item(
        SourceItem(
            id="proof-family-a-source",
            household_id=household_a.id,
            connected_account_id=None,
            source_type="email",
            title="Family A permission slip",
            body="Family A private source body.",
            observed_at_utc=now,
            event_at_utc=now + timedelta(days=1),
            sender="school-a@example.com",
            external_id="proof-family-a-source",
        ),
        decision=SourceDecision.SURFACE.value,
        reason="proof_test",
        priority=80,
        surfaced_at_utc=now,
    )
    store.add_source_item(
        SourceItem(
            id="proof-family-b-source",
            household_id=household_b.id,
            connected_account_id=None,
            source_type="email",
            title="Other family school contract",
            body="Other family private source body.",
            observed_at_utc=now,
            event_at_utc=now + timedelta(days=2),
            sender="school-b@example.com",
            external_id="proof-family-b-source",
        ),
        decision=SourceDecision.SURFACE.value,
        reason="proof_test",
        priority=90,
        surfaced_at_utc=now,
    )
    store.upsert_memory(
        household_id=household_b.id,
        kind=MemoryKind.FACT,
        subject="Leo",
        text="Leo has a confidential allergy note.",
        confidence=0.95,
        asserted_by_member_id=parent_b.id,
        now_utc=now,
    )
    other_action = store.create_pending_action(
        household_id=household_b.id,
        chat_id="proof-family-b",
        action_type="reminder.create",
        summary="Other family action secret.",
        payload={"title": "Other family action secret"},
        created_at_utc=now,
        expires_at_utc=now + timedelta(days=1),
        created_by_member_id=parent_b.id,
    )
    other_execution = store.record_action_execution(
        action=other_action,
        status=ActionExecutionStatus.SUCCESS,
        attempted_at_utc=now + timedelta(minutes=1),
        result={"secret": "Other family execution secret"},
    )

    proof_response = client.get(
        "/dev/pilot-proof/proof-family-a",
        headers=admin_headers,
    )
    proof = proof_response.json()["proof"]
    proof_blob = json.dumps(proof, sort_keys=True)

    assert proof_response.status_code == 200
    assert proof["chat_id"] == "proof-family-a"
    assert proof["source_review"]["total"] == 1
    assert proof["source_review"]["surfaced"] == 1
    assert proof["source_review"]["recent_surfaced"][0] == {
        "id": "proof-family-a-source",
        "source_type": "email",
        "reason": "proof_test",
        "priority": 80,
        "event_at_present": True,
    }
    assert "Family A permission slip" not in proof_blob
    assert proof["action_executions"] == []
    assert "Family A private source body" not in proof_blob
    assert household_b.id not in proof_blob
    assert "proof-family-b" not in proof_blob
    assert "Other family school contract" not in proof_blob
    assert "Other family private source body" not in proof_blob
    assert "Leo has a confidential allergy note" not in proof_blob
    assert "Other family action secret" not in proof_blob
    assert "Other family execution secret" not in proof_blob
    assert other_action.id not in proof_blob
    assert other_execution.id not in proof_blob


def test_pilot_proof_omits_raw_action_execution_errors(tmp_path):
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
    )
    store = Store(settings.db_path)
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household = store.get_or_create_household(
        chat_id="proof-action-error-chat",
        timezone_name="America/Los_Angeles",
        now_utc=now,
    )
    parent = store.get_or_create_member(
        household_id=household.id,
        phone="+15555550100",
        now_utc=now,
    )
    store.set_member_name(parent.id, "Sam", now_utc=now)
    action = store.create_pending_action(
        household_id=household.id,
        chat_id=household.chat_id,
        action_type="send_message",
        summary="Send private update.",
        payload={"text": "Private action payload."},
        created_at_utc=now,
        expires_at_utc=now + timedelta(days=1),
        created_by_member_id=parent.id,
    )
    store.record_action_execution(
        action=action,
        status=ActionExecutionStatus.FAILED,
        attempted_at_utc=now + timedelta(minutes=1),
        error=(
            "RuntimeError: provider failed with Bearer abcdefghijklmnop "
            "for postgresql://florence:secret-password@db:5432/florence"
        ),
    )

    response = client.get(
        "/dev/pilot-proof/proof-action-error-chat",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    proof = response.json()["proof"]
    proof_blob = json.dumps(proof)

    assert response.status_code == 200
    assert proof["sanitization"]["action_errors"] == "presence_only"
    assert proof["action_executions"] == [
        {
            "id": proof["action_executions"][0]["id"],
            "action_id": action.id,
            "status": "failed",
            "attempted_at_utc": "2026-06-05T16:01:00+00:00",
            "error_present": True,
        }
    ]
    assert proof["pilot_check"]["actions"]["issues"] == [
        {
            "action_id": action.id,
            "status": "failed",
            "attempted_at_utc": "2026-06-05T16:01:00+00:00",
            "error_present": True,
        }
    ]
    assert "RuntimeError" not in proof_blob
    assert "abcdefghijklmnop" not in proof_blob
    assert "secret-password" not in proof_blob
    assert "Private action payload" not in proof_blob


def test_pilot_check_requires_recent_linq_message_transport(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    old_message_time = datetime(2026, 5, 20, 16, 0, tzinfo=timezone.utc)
    fresh_source_time = datetime(2026, 6, 5, 16, 5, tzinfo=timezone.utc)
    chat_id = "pilot-stale-linq-transport-chat"
    household = store.get_or_create_household(
        chat_id=chat_id,
        timezone_name=settings.default_timezone,
        now_utc=old_message_time,
    )
    parent_one = store.get_or_create_member(
        household_id=household.id,
        phone="+15555550100",
        now_utc=old_message_time,
    )
    parent_two = store.ensure_parent_member(
        household_id=household.id,
        phone="+15555550101",
        now_utc=old_message_time,
    )
    assert parent_two is not None
    store.set_member_name(parent_one.id, "Sam", now_utc=old_message_time)
    store.set_member_name(parent_two.id, "Alex", now_utc=old_message_time)
    store.save_message(
        household_id=household.id,
        chat_id=chat_id,
        direction=MessageDirection.INBOUND,
        sender=parent_one.phone,
        body="my name is Sam",
        created_at_utc=old_message_time,
        message_id="stale-inbound",
        actor_member_id=parent_one.id,
    )
    store.save_message(
        household_id=household.id,
        chat_id=chat_id,
        direction=MessageDirection.OUTBOUND,
        body="Nice to meet you, Sam.",
        created_at_utc=old_message_time,
        message_id="stale-outbound",
    )
    store.upsert_memory(
        household_id=household.id,
        kind=MemoryKind.FACT,
        subject="Maya",
        text="Child profile: Maya.",
        confidence=0.95,
        asserted_by_member_id=parent_one.id,
        now_utc=old_message_time,
    )
    store.upsert_source_preference(
        household_id=household.id,
        phrase="permission slips",
        preference=SourcePreferenceKind.ALWAYS_SURFACE,
        created_by_member_id=parent_one.id,
        now_utc=old_message_time,
    )
    account = store.upsert_connected_account(
        household_id=household.id,
        provider="google",
        external_account_id="parent@example.com",
        account_label="Parent Gmail",
        now_utc=fresh_source_time,
    )
    _attach_google_oauth_token(store, settings=settings, chat_id=chat_id, now=fresh_source_time)
    store.update_connected_account_sync(
        account_id=account.id,
        cursor="fresh-source-cursor",
        synced_at_utc=fresh_source_time,
    )
    store.add_source_item(
        SourceItem(
            id="fresh-token-backed-source",
            household_id=household.id,
            connected_account_id=account.id,
            source_type="email",
            title="Permission slip due tomorrow",
            body="Please sign and return the permission slip for tomorrow's field trip.",
            observed_at_utc=fresh_source_time,
            event_at_utc=fresh_source_time + timedelta(days=1),
            sender="school@example.com",
            external_id="fresh-token-backed-source",
        ),
        decision=SourceDecision.SURFACE.value,
        reason="Matches household source rule",
        priority=90,
        surfaced_at_utc=fresh_source_time,
    )
    action = store.create_pending_action(
        household_id=household.id,
        chat_id=chat_id,
        action_type="reminder.create",
        summary="Remind Sam about the permission slip.",
        payload={"title": "Sign permission slip"},
        created_at_utc=fresh_source_time,
        expires_at_utc=fresh_source_time + timedelta(days=1),
        created_by_member_id=parent_one.id,
    )
    store.record_action_execution(
        action=action,
        status=ActionExecutionStatus.SUCCESS,
        attempted_at_utc=fresh_source_time + timedelta(minutes=1),
        result={"ok": True},
    )
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))

    response = client.get(
        f"/dev/pilot-check/{chat_id}",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()
    checklist = {step["id"]: step for step in payload["smoke_checklist"]["steps"]}

    assert response.status_code == 200
    assert payload["pilot_ready"] is False
    assert payload["household"]["ready"] is True
    assert payload["deployment"]["ready"] is True
    assert payload["message_transport"]["ready"] is True
    assert payload["message_transport"]["latest_inbound_at_utc"] == "2026-05-20T16:00:00+00:00"
    assert payload["message_transport"]["latest_outbound_at_utc"] == "2026-05-20T16:00:00+00:00"
    assert payload["source_review"]["token_backed_google_total"] == 1
    assert payload["source_review"]["token_backed_google_surfaced"] == 1
    assert payload["actions"]["ready"] is True
    assert checklist["linq_message_transport"]["ready"] is False
    assert checklist["linq_message_transport"]["blocked_by"] == [
        "Send and receive a fresh Linq iMessage round trip for the smoke window."
    ]
    assert checklist["linq_message_transport"]["evidence"]["linq_verified_at_utc"] == (
        "2026-06-05T16:00:00+00:00"
    )
    assert checklist["source_rule_and_need_to_know"]["ready"] is True
    assert checklist["approval_worker_queue"]["ready"] is True
    assert payload["smoke_checklist"]["blocked"] == ["linq_message_transport"]


def test_pilot_check_requires_surfaced_connected_source_item(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        pending_action_ttl_minutes=7 * 24 * 60,
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "pilot-manual-source-chat"
    admin_headers = {"x-florence-admin-key": "secret-admin-key"}

    for payload in (
        {"message_id": "parent-name", "text": "my name is Sam"},
        {"message_id": "partner", "text": "confirm partner +15555550101"},
        {"message_id": "child", "text": "our child is Maya"},
        {"message_id": "source-rule", "text": "always tell me about permission slips"},
    ):
        client.post(
            "/dev/messages",
            json={"chat_id": chat_id, "now_utc": now.isoformat(), **payload},
            headers=admin_headers,
        )
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "partner-name",
            "sender": "+15555550101",
            "text": "my name is Alex",
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    client.post(
        "/dev/sync-sources",
        json={
            "chat_id": chat_id,
            "provider": "google",
            "external_account_id": "parent@example.com",
            "account_label": "Parent Gmail",
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    _attach_google_oauth_token(store, settings=settings, chat_id=chat_id, now=now)
    source_response = client.post(
        "/dev/source-items",
        json={
            "chat_id": chat_id,
            "source_type": "email",
            "title": "Permission slip due tomorrow",
            "body": "Please sign and return the permission slip for tomorrow's field trip.",
            "sender": "school@example.com",
            "external_id": "manual-source-only",
            "event_at_utc": (now + timedelta(days=1)).isoformat(),
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    assert source_response.status_code == 200
    client.post(
        "/dev/actions",
        json={
            "chat_id": chat_id,
            "action_type": "create_reminder",
            "summary": "Add reminder: sign the permission slip.",
            "payload": {
                "title": "Sign the permission slip",
                "due_at_utc": (now + timedelta(days=1)).isoformat(),
            },
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    action = client.get(f"/dev/actions/{chat_id}", headers=admin_headers).json()["actions"][0]
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "approve-manual-source-reminder",
            "text": f"approve {action['id'][:8]}",
            "now_utc": (now + timedelta(minutes=1)).isoformat(),
        },
        headers=admin_headers,
    )
    client.post(
        "/dev/actions/tick",
        json={"now_utc": (now + timedelta(minutes=2)).isoformat()},
        headers=admin_headers,
    )

    response = client.get(f"/dev/pilot-check/{chat_id}", headers=admin_headers)
    payload = response.json()
    checklist = {step["id"]: step for step in payload["smoke_checklist"]["steps"]}

    assert response.status_code == 200
    assert payload["pilot_ready"] is False
    assert payload["source_review"]["total"] == 1
    assert payload["source_review"]["surfaced"] == 1
    assert payload["source_review"]["connected_total"] == 0
    assert payload["source_review"]["connected_surfaced"] == 0
    assert payload["source_review"]["token_backed_google_total"] == 0
    assert payload["source_review"]["token_backed_google_surfaced"] == 0
    assert payload["connected_accounts"]["active_google"] == 1
    assert payload["connected_accounts"]["token_backed_google"] == 1
    assert payload["actions"]["succeeded"] == 1
    assert checklist["connected_source_account"]["ready"] is True
    assert checklist["source_rule_and_need_to_know"]["ready"] is False
    assert checklist["source_rule_and_need_to_know"]["blocked_by"] == [
        "Run a controlled Google OAuth-backed source sync item."
    ]


def test_pilot_check_requires_recent_google_oauth_backed_source_sync(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))
    old_sync = datetime(2026, 5, 20, 16, 0, tzinfo=timezone.utc)
    chat_id = "pilot-stale-source-sync-chat"
    admin_headers = {"x-florence-admin-key": "secret-admin-key"}

    for payload in (
        {"message_id": "parent-name", "text": "my name is Sam"},
        {"message_id": "partner", "text": "confirm partner +15555550101"},
        {"message_id": "child", "text": "our child is Maya"},
        {"message_id": "source-rule", "text": "always tell me about permission slips"},
    ):
        client.post(
            "/dev/messages",
            json={"chat_id": chat_id, "now_utc": old_sync.isoformat(), **payload},
            headers=admin_headers,
        )
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "partner-name",
            "sender": "+15555550101",
            "text": "my name is Alex",
            "now_utc": old_sync.isoformat(),
        },
        headers=admin_headers,
    )
    source_response = client.post(
        "/dev/sync-sources",
        json={
            "chat_id": chat_id,
            "provider": "google",
            "external_account_id": "parent@example.com",
            "account_label": "Parent Gmail",
            "emails": [
                {
                    "subject": "Permission slip due tomorrow",
                    "body": "Please sign and return the permission slip for tomorrow's field trip.",
                    "sender": "school@example.com",
                    "external_id": "stale-source-sync",
                    "received_at_utc": old_sync.isoformat(),
                    "event_at_utc": (old_sync + timedelta(days=1)).isoformat(),
                }
            ],
            "cursor": "stale-cursor",
            "now_utc": old_sync.isoformat(),
        },
        headers=admin_headers,
    )
    assert source_response.status_code == 200
    _attach_google_oauth_token(store, settings=settings, chat_id=chat_id, now=old_sync)

    response = client.get(f"/dev/pilot-check/{chat_id}", headers=admin_headers)
    payload = response.json()
    checklist = {step["id"]: step for step in payload["smoke_checklist"]["steps"]}

    assert response.status_code == 200
    assert payload["pilot_ready"] is False
    assert payload["source_review"]["token_backed_google_total"] == 1
    assert payload["source_review"]["token_backed_google_surfaced"] == 1
    assert payload["source_review"]["latest_token_backed_google_synced_at_utc"] == "2026-05-20T16:00:00Z"
    assert checklist["source_rule_and_need_to_know"]["ready"] is False
    assert checklist["source_rule_and_need_to_know"]["blocked_by"] == [
        "Run a fresh Google OAuth-backed source sync for the smoke window."
    ]
    assert checklist["source_rule_and_need_to_know"]["evidence"]["google_verified_at_utc"] == (
        "2026-06-05T16:05:00+00:00"
    )


def test_dev_hermes_status_reports_local_checkout_without_claiming_saas_ready(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    runtime_home = tmp_path / "hermes-runtime-home"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        hermes_runtime_home=str(runtime_home),
    )
    store = Store(settings.db_path)
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        "/dev/hermes-status",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["hermes"]

    assert response.status_code == 200
    assert payload["mode"] == "configured_checkout"
    assert payload["agent_path"] == str(hermes_path.resolve())
    assert payload["run_agent_path"] == str((hermes_path / "run_agent.py").resolve())
    assert payload["contract_ok"] is True
    assert payload["ready_for_saas_pilot"] is False
    assert payload["database_backend"] == "sqlite"
    assert payload["invalid"] == [
        "FLORENCE_DATABASE_URL must point to Postgres for Hermes SaaS pilot status"
    ]
    assert payload["pinned_ref"] is True
    assert payload["hermes_checkout_ref"] == PINNED_HERMES_REF
    assert payload["hermes_ref_matches"] is True
    assert payload["toolsets_disabled"] is True
    assert payload["runtime_home"] == str(runtime_home.resolve())
    preflight_runtime_home = Path(payload["preflight_runtime_home"])
    assert preflight_runtime_home.parent == runtime_home.resolve()
    assert preflight_runtime_home.name.startswith("florence-preflight-")
    assert payload["preflight_runtime_home_scope"] == "ephemeral_per_check_under_runtime_home"
    assert payload["turn_runtime_home_scope"] == "per_turn_under_runtime_home"
    assert payload["turn_runtime_cleanup"] == "enabled"
    assert payload["turn_failure_cleanup"] == (
        "runtime_home_restored_and_checkout_modules_cleared_on_error"
    )
    assert payload["turn_runtime_concurrency"] == "serialized_by_thread_and_file_lock"
    assert payload["runtime_lock"] == "thread_lock_plus_interprocess_file_lock"
    assert payload["runtime_env_var"] == "HERMES_HOME"
    assert payload["runtime_home_writable"] is True
    assert payload["python_path_scope"] == "temporary_during_hermes_call"
    assert payload["module_cache_scope"] == "shadowed_and_cleared_during_hermes_import_or_call"
    assert not preflight_runtime_home.exists()
    assert payload["memory_owner"] == "florence"
    assert payload["session_scope"] == "ephemeral_per_turn"
    assert payload["durable_hermes_memory"] == "disabled"
    assert store.get_household_by_chat("dev-chat") is None


def test_dev_hermes_status_reports_postgres_checkout_ready_without_household(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    runtime_home = tmp_path / "hermes-runtime-home"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        hermes_runtime_home=str(runtime_home),
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        "/dev/hermes-status",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["hermes"]

    assert response.status_code == 200
    assert payload["contract_ok"] is True
    assert payload["ready_for_saas_pilot"] is True
    assert payload["database_backend"] == "postgres"
    assert payload["invalid"] == []
    assert payload["hermes_ref_matches"] is True
    assert payload["strict_mode"] is True
    assert payload["turn_failure_cleanup"] == (
        "runtime_home_restored_and_checkout_modules_cleared_on_error"
    )


def test_dev_hermes_status_blocks_postgres_when_hermes_lock_is_thread_only(
    tmp_path, monkeypatch
):
    lock_error = "POSIX fcntl file locking is required for deployed SaaS Hermes traffic"
    monkeypatch.setattr("florence.app.hermes_runtime_lock_error", lambda: lock_error)
    monkeypatch.setattr(
        "florence.app.hermes_runtime_lock_mode",
        lambda: "thread_lock_only_no_interprocess_lock",
    )
    monkeypatch.setattr(
        "florence.app.hermes_runtime_concurrency_mode",
        lambda: "serialized_by_thread_lock_only",
    )
    hermes_path = tmp_path / "hermes-agent"
    runtime_home = tmp_path / "hermes-runtime-home"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        hermes_runtime_home=str(runtime_home),
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store))

    response = client.get(
        "/dev/hermes-status",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["hermes"]

    assert response.status_code == 200
    assert payload["contract_ok"] is True
    assert payload["ready_for_saas_pilot"] is False
    assert payload["database_backend"] == "postgres"
    assert payload["turn_runtime_concurrency"] == "serialized_by_thread_lock_only"
    assert payload["runtime_lock"] == "thread_lock_only_no_interprocess_lock"
    assert payload["invalid"] == [lock_error]


def test_dev_hermes_status_clears_checkout_modules_after_preflight(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    runtime_home = tmp_path / "hermes-runtime-home"
    hermes_path.mkdir()
    module_name = "preflight_hermes_helper"
    sys.modules.pop(module_name, None)
    (hermes_path / ".florence-hermes-ref").write_text(PINNED_HERMES_REF)
    (hermes_path / f"{module_name}.py").write_text("HELPER_VALUE = 'loaded from checkout'\n")
    (hermes_path / "run_agent.py").write_text(
        f"import {module_name}\n"
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        f"        return {{'final_response': {module_name}.HELPER_VALUE}}\n"
    )
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        hermes_runtime_home=str(runtime_home),
    )
    client = TestClient(create_app(settings, store=_production_like_store(settings)))

    response = client.get(
        "/dev/hermes-status",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    try:
        payload = response.json()["hermes"]

        assert response.status_code == 200
        assert payload["ready_for_saas_pilot"] is True
        assert payload["module_cache_scope"] == "shadowed_and_cleared_during_hermes_import_or_call"
        assert payload["turn_failure_cleanup"] == (
            "runtime_home_restored_and_checkout_modules_cleared_on_error"
        )
        assert module_name not in sys.modules
    finally:
        sys.modules.pop(module_name, None)


def test_dev_hermes_status_blocks_relative_runtime_home_for_pilot(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        hermes_runtime_home="relative-hermes-home",
    )
    client = TestClient(create_app(settings))

    response = client.get(
        "/dev/hermes-status",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["hermes"]

    assert response.status_code == 200
    assert payload["contract_ok"] is False
    assert payload["ready_for_saas_pilot"] is False
    assert payload["runtime_home"] == "relative-hermes-home"
    assert payload["runtime_home_writable"] is False
    assert "FLORENCE_HERMES_RUNTIME_HOME must be an absolute path" in payload["invalid"]


def test_dev_hermes_status_blocks_missing_path_as_ambient_import_for_pilot(tmp_path):
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
    )
    client = TestClient(create_app(settings))

    response = client.get(
        "/dev/hermes-status",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()["hermes"]

    assert response.status_code == 200
    assert payload["mode"] == "ambient_python_module"
    assert payload["agent_path"] is None
    assert payload["run_agent_path"] is None
    assert payload["contract_ok"] is False
    assert payload["ready_for_saas_pilot"] is False
    assert "FLORENCE_HERMES_AGENT_PATH is not set" in payload["invalid"]


def test_dev_hermes_smoke_uses_agent_without_persisting_turn(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
    )
    store = Store(settings.db_path)
    agent = FakeAgent("Hermes smoke OK. Maya has a dentist appointment.")
    client = TestClient(create_app(settings, store=store, agent=agent, linq_client=FakeLinqClient()))
    chat_id = "hermes-smoke-chat"
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    headers = {"x-florence-admin-key": "secret-admin-key"}
    setup = client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "smoke-name",
            "text": "my name is Sam",
            "now_utc": now.isoformat(),
        },
        headers=headers,
    )
    household = store.get_household_by_chat(chat_id)
    assert setup.status_code == 200
    assert household is not None
    before_messages = store.recent_messages(household.id)

    response = client.post(
        f"/dev/hermes-smoke/{chat_id}",
        json={"now_utc": now.isoformat()},
        headers=headers,
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["response"] is None
    assert payload["response_present"] is True
    assert payload["response_chars"] == len("Hermes smoke OK. Maya has a dentist appointment.")
    assert payload["sanitization"] == {"response": "excluded"}
    assert payload["used_fallback"] is False
    assert payload["live_hermes_verified"] is False
    assert "Maya" not in json.dumps(payload)
    assert payload["hermes"]["ready_for_saas_pilot"] is False
    assert payload["hermes"]["database_backend"] == "sqlite"
    assert payload["hermes"]["invalid"] == [
        "FLORENCE_DATABASE_URL must point to Postgres for Hermes SaaS pilot status"
    ]
    assert payload["hermes"]["hermes_ref_matches"] is True
    assert len(agent.calls) == 1
    assert agent.calls[0]["household"].id == household.id
    assert agent.calls[0]["actor"].display_name == "Sam"
    assert agent.calls[0]["conversation_history"] == []
    assert "Pilot smoke check" in agent.calls[0]["user_text"]
    assert store.recent_messages(household.id) == before_messages


def test_dev_hermes_smoke_reports_live_verified_only_for_postgres_saas(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
    )
    store = _production_like_store(settings)
    agent = FakeAgent("Hermes smoke OK.")
    client = TestClient(create_app(settings, store=store, agent=agent, linq_client=FakeLinqClient()))
    chat_id = "hermes-postgres-smoke-chat"
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    headers = {"x-florence-admin-key": "secret-admin-key"}
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "smoke-name",
            "text": "my name is Sam",
            "now_utc": now.isoformat(),
        },
        headers=headers,
    )

    response = client.post(
        f"/dev/hermes-smoke/{chat_id}",
        json={"now_utc": now.isoformat()},
        headers=headers,
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["response"] is None
    assert payload["response_present"] is True
    assert payload["response_chars"] == len("Hermes smoke OK.")
    assert payload["sanitization"] == {"response": "excluded"}
    assert payload["used_fallback"] is False
    assert payload["live_hermes_verified"] is True
    assert payload["stored_live_verification"] == {
        "name": "hermes",
        "verified_at_utc": "2026-06-05T16:00:00+00:00",
        "proof": "Hermes smoke endpoint returned live_hermes_verified true without fallback",
        "source": "hermes_smoke",
        "updated_at_utc": "2026-06-05T16:00:00+00:00",
    }
    assert payload["hermes"]["database_backend"] == "postgres"
    assert payload["hermes"]["ready_for_saas_pilot"] is True
    assert store.list_live_verifications()["hermes"] == payload["stored_live_verification"]


def test_dev_hermes_smoke_requires_ready_hermes_status_for_live_verification(tmp_path):
    settings = _settings(tmp_path, admin_api_key="secret-admin-key")
    store = Store(settings.db_path)
    agent = FakeAgent("Hermes smoke OK.")
    client = TestClient(create_app(settings, store=store, agent=agent, linq_client=FakeLinqClient()))
    chat_id = "hermes-smoke-misconfigured-chat"
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    headers = {"x-florence-admin-key": "secret-admin-key"}
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "smoke-name",
            "text": "my name is Sam",
            "now_utc": now.isoformat(),
        },
        headers=headers,
    )

    response = client.post(
        f"/dev/hermes-smoke/{chat_id}",
        json={"now_utc": now.isoformat()},
        headers=headers,
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["response"] is None
    assert payload["response_present"] is True
    assert payload["response_chars"] == len("Hermes smoke OK.")
    assert payload["sanitization"] == {"response": "excluded"}
    assert payload["used_fallback"] is False
    assert payload["live_hermes_verified"] is False
    assert payload["hermes"]["ready_for_saas_pilot"] is False
    assert "FLORENCE_HERMES_AGENT_PATH is not set" in payload["hermes"]["invalid"]


def test_dev_hermes_smoke_requires_parent_context(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
    )
    store = Store(settings.db_path)
    agent = FakeAgent("Hermes smoke OK.")
    client = TestClient(create_app(settings, store=store, agent=agent, linq_client=FakeLinqClient()))
    chat_id = "hermes-smoke-helper-only-chat"
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household = store.get_or_create_household(
        chat_id=chat_id,
        timezone_name="America/Los_Angeles",
        now_utc=now,
    )
    store.get_or_create_member(
        household_id=household.id,
        phone="+15555550199",
        now_utc=now,
    )
    with store.connect() as conn:
        conn.execute(
            "UPDATE household_members SET role = ? WHERE household_id = ?",
            (MemberRole.HELPER.value, household.id),
        )

    response = client.post(
        f"/dev/hermes-smoke/{chat_id}",
        json={"now_utc": now.isoformat()},
        headers={"x-florence-admin-key": "secret-admin-key"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "household_has_no_parent"
    assert agent.calls == []


def test_dev_hermes_smoke_reports_fallback_as_unverified(tmp_path):
    settings = _settings(tmp_path, admin_api_key="secret-admin-key")
    store = Store(settings.db_path)
    agent = FakeAgent(tone.fallback_reply())
    client = TestClient(create_app(settings, store=store, agent=agent, linq_client=FakeLinqClient()))
    chat_id = "hermes-fallback-smoke-chat"
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    headers = {"x-florence-admin-key": "secret-admin-key"}
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "smoke-name",
            "text": "my name is Sam",
            "now_utc": now.isoformat(),
        },
        headers=headers,
    )

    response = client.post(
        f"/dev/hermes-smoke/{chat_id}",
        json={"now_utc": now.isoformat()},
        headers=headers,
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["response"] is None
    assert payload["response_present"] is True
    assert payload["response_chars"] == len(tone.fallback_reply())
    assert payload["sanitization"] == {"response": "excluded"}
    assert payload["used_fallback"] is True
    assert payload["live_hermes_verified"] is False
    assert payload["hermes"]["ready_for_saas_pilot"] is False


def test_dev_hermes_smoke_reports_contract_error_without_server_error(tmp_path):
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))
    chat_id = "hermes-contract-smoke-chat"
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    headers = {"x-florence-admin-key": "secret-admin-key"}
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "smoke-name",
            "text": "my name is Sam",
            "now_utc": now.isoformat(),
        },
        headers=headers,
    )

    response = client.post(
        f"/dev/hermes-smoke/{chat_id}",
        json={"now_utc": now.isoformat()},
        headers=headers,
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["ok"] is False
    assert payload["response"] is None
    assert payload["response_present"] is False
    assert payload["response_chars"] == 0
    assert payload["sanitization"] == {"response": "excluded"}
    assert payload["used_fallback"] is False
    assert payload["live_hermes_verified"] is False
    assert payload["error"] == (
        "HermesSaaSContractError: FLORENCE_HERMES_AGENT_PATH is required "
        "for deployed SaaS traffic"
    )
    assert payload["hermes"]["ready_for_saas_pilot"] is False
    assert "FLORENCE_HERMES_AGENT_PATH is not set" in payload["hermes"]["invalid"]


def test_dev_hermes_smoke_uses_generic_error_for_provider_failure(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / ".florence-hermes-ref").write_text(PINNED_HERMES_REF)
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        raise RuntimeError('provider failed with Bearer abcdefghijklmnop and secret-admin-key "
        "at postgresql://florence:secret-password@db:5432/florence')\n"
    )
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
    )
    store = _production_like_store(settings)
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))
    chat_id = "hermes-provider-failure-smoke-chat"
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    headers = {"x-florence-admin-key": "secret-admin-key"}
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "smoke-name",
            "text": "my name is Sam",
            "now_utc": now.isoformat(),
        },
        headers=headers,
    )

    response = client.post(
        f"/dev/hermes-smoke/{chat_id}",
        json={"now_utc": now.isoformat()},
        headers=headers,
    )
    payload = response.json()
    blob = json.dumps(payload)

    assert response.status_code == 200
    assert payload["ok"] is False
    assert payload["response"] is None
    assert payload["response_present"] is False
    assert payload["response_chars"] == 0
    assert payload["sanitization"] == {"response": "excluded"}
    assert payload["live_hermes_verified"] is False
    assert payload["error"] == (
        "RuntimeError: Hermes smoke failed; check provider configuration and server logs"
    )
    assert payload["hermes"]["ready_for_saas_pilot"] is True
    assert "abcdefghijklmnop" not in blob
    assert "secret-admin-key" not in blob
    assert "secret-password" not in blob


def test_pilot_check_blocks_configured_but_unverified_external_services(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    client = TestClient(
        create_app(settings, store=_production_like_store(settings), linq_client=FakeLinqClient())
    )
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "pilot-unverified-chat"
    admin_headers = {"x-florence-admin-key": "secret-admin-key"}
    for payload in (
        {"message_id": "parent-name", "text": "my name is Sam"},
        {"message_id": "partner", "text": "confirm partner +15555550101"},
        {"message_id": "child", "text": "our child is Maya"},
        {"message_id": "source-rule", "text": "always tell me about permission slips"},
    ):
        client.post(
            "/dev/messages",
            json={"chat_id": chat_id, "now_utc": now.isoformat(), **payload},
            headers=admin_headers,
        )
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "partner-name",
            "sender": "+15555550101",
            "text": "my name is Alex",
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    client.post(
        "/dev/sync-sources",
        json={
            "chat_id": chat_id,
            "provider": "google",
            "external_account_id": "parent@example.com",
            "account_label": "Parent Gmail",
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )

    response = client.get(f"/dev/pilot-check/{chat_id}", headers=admin_headers)
    payload = response.json()
    live = payload["deployment"]["live_verification"]

    assert response.status_code == 200
    assert payload["household"]["ready"] is True
    assert payload["pilot_ready"] is False
    assert payload["deployment"]["ready"] is False
    assert payload["deployment"]["missing_required"] == []
    assert payload["deployment"]["invalid"] == []
    assert live["ready"] is False
    assert live["external_credentials_needed"] == []
    assert live["unverified"] == [
        "Live Linq iMessage send and webhook round-trip",
        "Live Google OAuth connection and source sync",
        "Live Hermes Agent response through Florence adapter",
    ]
    assert live["verified"] == {"linq": False, "google": False, "hermes": False}
    assert live["blocked"] == ["Live Linq/Google/Hermes smoke checks not marked verified"]
    assert payload["smoke_checklist"]["ready"] is False
    assert "deployment_preflight" in payload["smoke_checklist"]["blocked"]
    assert "linq_live_round_trip" in payload["smoke_checklist"]["blocked"]
    assert "connected_source_account" in payload["smoke_checklist"]["blocked"]
    assert "google_live_source_sync" in payload["smoke_checklist"]["blocked"]
    assert "hermes_live_response" in payload["smoke_checklist"]["blocked"]
    assert "source_rule_and_need_to_know" in payload["smoke_checklist"]["blocked"]
    assert "approval_worker_queue" in payload["smoke_checklist"]["blocked"]
    checklist = {step["id"]: step for step in payload["smoke_checklist"]["steps"]}
    assert checklist["source_rule_and_need_to_know"]["blocked_by"] == [
        "Run a controlled Google OAuth-backed source sync item."
    ]
    assert checklist["approval_worker_queue"]["blocked_by"] == [
        "Run at least one parent-approved action through the worker."
    ]
    assert checklist["connected_source_account"]["blocked_by"] == [
        "Complete Google OAuth so Florence has an encrypted token for source sync."
    ]
    assert checklist["linq_live_round_trip"]["blocked_by"] == [
        "Live Linq iMessage send and webhook round-trip"
    ]


def test_pilot_check_blocks_household_without_successful_outbound_message(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    store = _production_like_store(settings)
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "pilot-no-outbound-chat"
    household = store.get_or_create_household(
        chat_id=chat_id,
        timezone_name=settings.default_timezone,
        now_utc=now,
    )
    parent_one = store.get_or_create_member(
        household_id=household.id,
        phone="+15555550100",
        now_utc=now,
    )
    parent_two = store.ensure_parent_member(
        household_id=household.id,
        phone="+15555550101",
        now_utc=now,
    )
    assert parent_two is not None
    store.set_member_name(parent_one.id, "Sam", now_utc=now)
    store.set_member_name(parent_two.id, "Alex", now_utc=now)
    store.save_message(
        household_id=household.id,
        chat_id=chat_id,
        direction=MessageDirection.INBOUND,
        sender=parent_one.phone,
        body="my name is Sam",
        created_at_utc=now,
        message_id="only-inbound",
        actor_member_id=parent_one.id,
    )
    store.upsert_memory(
        household_id=household.id,
        kind=MemoryKind.FACT,
        subject="Maya",
        text="Child profile: Maya.",
        confidence=0.95,
        asserted_by_member_id=parent_one.id,
        now_utc=now,
    )
    store.upsert_connected_account(
        household_id=household.id,
        provider="google",
        external_account_id="parent@example.com",
        account_label="Parent Gmail",
        now_utc=now,
    )
    store.upsert_source_preference(
        household_id=household.id,
        phrase="permission slips",
        preference=SourcePreferenceKind.ALWAYS_SURFACE,
        created_by_member_id=parent_one.id,
        now_utc=now,
    )
    client = TestClient(create_app(settings, store=store, linq_client=FakeLinqClient()))

    response = client.get(
        f"/dev/pilot-check/{chat_id}",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["household"]["ready"] is True
    assert payload["deployment"]["ready"] is True
    assert payload["message_transport"]["ready"] is False
    assert payload["message_transport"]["inbound"] == 1
    assert payload["message_transport"]["outbound"] == 0
    assert payload["message_transport"]["missing"] == [
        "At least one successfully recorded outbound iMessage."
    ]
    assert payload["pilot_ready"] is False
    assert "my name is Sam" not in json.dumps(payload)


def test_pilot_check_blocks_pending_or_failed_delivery_work(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    client = TestClient(
        create_app(settings, store=_production_like_store(settings), linq_client=FakeLinqClient())
    )
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "pilot-delivery-chat"
    admin_headers = {"x-florence-admin-key": "secret-admin-key"}
    for payload in (
        {"message_id": "parent-name", "text": "my name is Sam"},
        {"message_id": "partner", "text": "confirm partner +15555550101"},
        {"message_id": "child", "text": "our child is Maya"},
        {"message_id": "source-rule", "text": "always tell me about permission slips"},
    ):
        client.post(
            "/dev/messages",
            json={"chat_id": chat_id, "now_utc": now.isoformat(), **payload},
            headers=admin_headers,
        )
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "partner-name",
            "sender": "+15555550101",
            "text": "my name is Alex",
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    client.post(
        "/dev/sync-sources",
        json={
            "chat_id": chat_id,
            "provider": "google",
            "external_account_id": "parent@example.com",
            "account_label": "Parent Gmail",
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    store = Store(settings.db_path)
    household = store.get_household_by_chat(chat_id)
    assert household is not None
    store.record_outbound_deliveries_for_source(
        household_id=household.id,
        source_message_id="source:stuck",
        messages=[
            OutboundMessage(
                chat_id=chat_id,
                text="Do not expose this text in pilot check.",
                idempotency_key="source:stuck-delivery",
            )
        ],
        now_utc=now,
    )
    store.mark_outbound_delivery_failed(
        idempotency_key="source:stuck-delivery",
        error="RuntimeError: Linq unavailable",
        now_utc=now + timedelta(minutes=1),
    )

    response = client.get(f"/dev/pilot-check/{chat_id}", headers=admin_headers)
    payload = response.json()

    assert response.status_code == 200
    assert payload["household"]["ready"] is True
    assert payload["deployment"]["ready"] is True
    assert payload["pilot_ready"] is False
    assert payload["delivery"]["ready"] is False
    assert payload["delivery"]["failed"] == 1
    assert payload["delivery"]["retryable"] == 1
    assert payload["delivery"]["issues"][0]["idempotency_key"] == "source:stuck-delivery"
    assert payload["delivery"]["issues"][0]["source_message_id"] == "source:stuck"
    assert payload["delivery"]["issues"][0]["last_error"] == "RuntimeError: Linq unavailable"
    assert "Do not expose" not in json.dumps(payload)


def test_pilot_check_blocks_approved_or_failed_action_work(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        pending_action_ttl_minutes=7 * 24 * 60,
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        linq_live_verified=True,
        google_live_verified=True,
        hermes_live_verified=True,
        **LIVE_VERIFICATION_EVIDENCE,
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    client = TestClient(
        create_app(settings, store=_production_like_store(settings), linq_client=FakeLinqClient())
    )
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    chat_id = "pilot-action-chat"
    admin_headers = {"x-florence-admin-key": "secret-admin-key"}
    for payload in (
        {"message_id": "parent-name", "text": "my name is Sam"},
        {"message_id": "partner", "text": "confirm partner +15555550101"},
        {"message_id": "child", "text": "our child is Maya"},
        {"message_id": "source-rule", "text": "always tell me about permission slips"},
    ):
        client.post(
            "/dev/messages",
            json={"chat_id": chat_id, "now_utc": now.isoformat(), **payload},
            headers=admin_headers,
        )
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "partner-name",
            "sender": "+15555550101",
            "text": "my name is Alex",
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    client.post(
        "/dev/sync-sources",
        json={
            "chat_id": chat_id,
            "provider": "google",
            "external_account_id": "parent@example.com",
            "account_label": "Parent Gmail",
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    client.post(
        "/dev/actions",
        json={
            "chat_id": chat_id,
            "action_type": "email_teacher",
            "summary": "Email the teacher.",
            "payload": {"body": "Do not expose this action payload."},
            "now_utc": now.isoformat(),
        },
        headers=admin_headers,
    )
    action = client.get(f"/dev/actions/{chat_id}", headers=admin_headers).json()["actions"][0]
    client.post(
        "/dev/messages",
        json={
            "chat_id": chat_id,
            "message_id": "approve-action",
            "text": f"approve {action['id'][:8]}",
            "now_utc": (now + timedelta(minutes=1)).isoformat(),
        },
        headers=admin_headers,
    )

    approved_response = client.get(f"/dev/pilot-check/{chat_id}", headers=admin_headers)
    action_tick = client.post(
        "/dev/actions/tick",
        json={"now_utc": (now + timedelta(minutes=2)).isoformat()},
        headers=admin_headers,
    )
    failed_response = client.get(f"/dev/pilot-check/{chat_id}", headers=admin_headers)
    approved_payload = approved_response.json()
    failed_payload = failed_response.json()

    assert approved_response.status_code == 200
    assert approved_payload["household"]["ready"] is True
    assert approved_payload["deployment"]["ready"] is True
    assert approved_payload["delivery"]["ready"] is True
    assert approved_payload["pilot_ready"] is False
    assert approved_payload["actions"]["ready"] is False
    assert approved_payload["actions"]["approved"] == 1
    assert approved_payload["actions"]["failed"] == 0
    assert approved_payload["actions"]["issues"][0]["action_id"] == action["id"]
    assert approved_payload["actions"]["issues"][0]["action_type"] == "email_teacher"
    assert action_tick.json() == {"ok": True, "attempted": 1, "succeeded": 0, "failed": 1}
    assert failed_payload["pilot_ready"] is False
    assert failed_payload["actions"]["ready"] is False
    assert failed_payload["actions"]["approved"] == 0
    assert failed_payload["actions"]["failed"] == 1
    assert failed_payload["actions"]["issues"][0]["action_id"] == action["id"]
    assert failed_payload["actions"]["issues"][0]["status"] == "failed"
    assert "unsupported action type" in failed_payload["actions"]["issues"][0]["error"]
    assert "Do not expose this action payload" not in json.dumps(failed_payload)


def test_pilot_check_blocks_any_hermes_toolset_for_saas_pilot(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        hermes_enabled_toolsets=("web",),
    )
    Store(settings.db_path).get_or_create_household(
        chat_id="pilot-unsafe-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings))

    response = client.get(
        "/dev/pilot-check/pilot-unsafe-chat",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    deployment = response.json()["deployment"]

    assert response.status_code == 200
    assert deployment["ready"] is False
    assert "FLORENCE_HERMES_TOOLSETS must be empty" in deployment["invalid"][0]


def test_pilot_check_blocks_unpinned_hermes_agent_ref(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref="main",
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    _production_like_store(settings).get_or_create_household(
        chat_id="pilot-unpinned-hermes-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings, store=_production_like_store(settings)))

    response = client.get(
        "/dev/pilot-check/pilot-unpinned-hermes-chat",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    deployment = response.json()["deployment"]

    assert response.status_code == 200
    assert deployment["ready"] is False
    assert deployment["missing_required"] == []
    assert deployment["invalid"] == [
        "HERMES_AGENT_REF must be a full pinned Git commit SHA "
        "(40 or 64 hex characters), not a branch, tag, short SHA, or floating ref"
    ]
    assert deployment["live_verification"]["blocked"] == [
        "Live Linq/Google/Hermes smoke checks not marked verified",
        "Pilot deployment safety preflight",
    ]


def test_pilot_check_blocks_abbreviated_hermes_agent_ref(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF[:12],
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    _production_like_store(settings).get_or_create_household(
        chat_id="pilot-short-hermes-ref-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings, store=_production_like_store(settings)))

    response = client.get(
        "/dev/pilot-check/pilot-short-hermes-ref-chat",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    deployment = response.json()["deployment"]

    assert response.status_code == 200
    assert deployment["ready"] is False
    assert deployment["hermes_agent_ref"] == PINNED_HERMES_REF[:12]
    assert deployment["hermes_ref_matches"] is None
    assert deployment["invalid"] == [
        "HERMES_AGENT_REF must be a full pinned Git commit SHA "
        "(40 or 64 hex characters), not a branch, tag, short SHA, or floating ref"
    ]


def test_pilot_check_blocks_mismatched_hermes_checkout_ref(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    (hermes_path / ".florence-hermes-ref").write_text(
        "ffffffffffffffffffffffffffffffffffffffff"
    )
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    _production_like_store(settings).get_or_create_household(
        chat_id="pilot-mismatched-hermes-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings, store=_production_like_store(settings)))

    response = client.get(
        "/dev/pilot-check/pilot-mismatched-hermes-chat",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    deployment = response.json()["deployment"]

    assert response.status_code == 200
    assert deployment["ready"] is False
    assert deployment["hermes_agent_ref"] == PINNED_HERMES_REF
    assert deployment["hermes_checkout_ref"] == "ffffffffffffffffffffffffffffffffffffffff"
    assert deployment["hermes_ref_matches"] is False
    assert (
        "FLORENCE_HERMES_AGENT_PATH checkout ref does not match HERMES_AGENT_REF"
        in deployment["invalid"]
    )


def test_pilot_check_blocks_abbreviated_hermes_checkout_ref(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    (hermes_path / ".florence-hermes-ref").write_text(PINNED_HERMES_REF[:12])
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="postgresql://florence:secret@db:5432/florence",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    _production_like_store(settings).get_or_create_household(
        chat_id="pilot-short-checkout-ref-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings, store=_production_like_store(settings)))

    response = client.get(
        "/dev/pilot-check/pilot-short-checkout-ref-chat",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    deployment = response.json()["deployment"]

    assert response.status_code == 200
    assert deployment["ready"] is False
    assert deployment["hermes_agent_ref"] == PINNED_HERMES_REF
    assert deployment["hermes_checkout_ref"] == PINNED_HERMES_REF[:12]
    assert deployment["hermes_ref_matches"] is False
    assert (
        "FLORENCE_HERMES_AGENT_PATH checkout ref does not match HERMES_AGENT_REF"
        in deployment["invalid"]
    )


def test_pilot_check_blocks_unsupported_database_url(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    _write_compatible_hermes_agent(hermes_path)
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        database_url="sqlite:///data/florence.sqlite",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    Store(settings.db_path).get_or_create_household(
        chat_id="pilot-unsupported-db-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings, store=Store(settings.db_path)))

    response = client.get(
        "/dev/pilot-check/pilot-unsupported-db-chat",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    deployment = response.json()["deployment"]

    assert response.status_code == 200
    assert deployment["ready"] is False
    assert deployment["database_backend"] == "unsupported"
    assert deployment["invalid"] == [
        "FLORENCE_DATABASE_URL must start with postgres:// or postgresql://"
    ]


def test_app_startup_rejects_unsupported_database_url_without_store_override(tmp_path):
    settings = _settings(
        tmp_path,
        database_url=f"sqlite:///{tmp_path / 'florence.sqlite'}",
    )

    try:
        create_app(settings)
    except ValueError as exc:
        error = str(exc)
    else:
        raise AssertionError("create_app should reject unsupported database URLs")

    assert "Unsupported Florence database URL scheme 'sqlite'" in error


def test_pilot_check_blocks_hermes_path_without_aiagent(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / "run_agent.py").write_text("VALUE = 1\n")
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    Store(settings.db_path).get_or_create_household(
        chat_id="pilot-missing-aiagent-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings))

    response = client.get(
        "/dev/pilot-check/pilot-missing-aiagent-chat",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    deployment = response.json()["deployment"]

    assert response.status_code == 200
    assert deployment["ready"] is False
    assert deployment["invalid"] == [
        "FLORENCE_HERMES_AGENT_PATH run_agent.py does not expose AIAgent"
    ]


def test_pilot_check_blocks_hermes_path_with_incompatible_aiagent_constructor(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    def __init__(self):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        return {'final_response': 'ok'}\n"
    )

    deployment = _hermes_preflight_deployment(
        tmp_path,
        hermes_path,
        "pilot-incompatible-hermes-constructor-chat",
    )

    assert deployment["ready"] is False
    assert deployment["invalid"] == [
        "FLORENCE_HERMES_AGENT_PATH AIAgent constructor is not compatible with Florence adapter"
    ]


def test_pilot_check_blocks_hermes_path_without_skip_memory_kwarg(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    def __init__(self, provider=None, model='', enabled_toolsets=None, quiet_mode=False,\n"
        "                 save_trajectories=False, skip_context_files=False, platform=None,\n"
        "                 session_id=None):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        return {'final_response': 'ok'}\n"
    )

    deployment = _hermes_preflight_deployment(
        tmp_path,
        hermes_path,
        "pilot-hermes-missing-skip-memory-chat",
    )

    assert deployment["ready"] is False
    assert deployment["invalid"] == [
        "FLORENCE_HERMES_AGENT_PATH AIAgent constructor is not compatible with Florence adapter"
    ]


def test_pilot_check_blocks_hermes_path_without_save_trajectories_kwarg(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    def __init__(self, provider=None, model='', enabled_toolsets=None, quiet_mode=False,\n"
        "                 skip_context_files=False, skip_memory=False, platform=None,\n"
        "                 session_id=None):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        return {'final_response': 'ok'}\n"
    )

    deployment = _hermes_preflight_deployment(
        tmp_path,
        hermes_path,
        "pilot-hermes-missing-save-trajectories-chat",
    )

    assert deployment["ready"] is False
    assert deployment["invalid"] == [
        "FLORENCE_HERMES_AGENT_PATH AIAgent constructor is not compatible with Florence adapter"
    ]


def test_pilot_check_blocks_hermes_path_without_run_conversation(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
    )

    deployment = _hermes_preflight_deployment(
        tmp_path,
        hermes_path,
        "pilot-missing-hermes-run-chat",
    )

    assert deployment["ready"] is False
    assert deployment["invalid"] == [
        "FLORENCE_HERMES_AGENT_PATH AIAgent.run_conversation is missing"
    ]


def test_pilot_check_blocks_hermes_path_with_incompatible_run_conversation(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self):\n"
        "        return {'final_response': 'ok'}\n"
    )

    deployment = _hermes_preflight_deployment(
        tmp_path,
        hermes_path,
        "pilot-incompatible-hermes-run-chat",
    )

    assert deployment["ready"] is False
    assert deployment["invalid"] == [
        "FLORENCE_HERMES_AGENT_PATH AIAgent.run_conversation is not compatible with Florence adapter"
    ]


def test_pilot_check_blocks_unimportable_hermes_path(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / "run_agent.py").write_text(
        "import definitely_missing_florence_hermes_dependency\nclass AIAgent: pass\n"
    )
    settings = _settings(
        tmp_path,
        admin_api_key="secret-admin-key",
        source_ingest_api_key="source-key",
        linq_api_key="linq-key",
        linq_webhook_secret="linq-webhook-secret",
        linq_from_phone="+15555550000",
        token_encryption_key=TokenVault.generate_key(),
        support_contact="support@example.com",
        hermes_agent_path=str(hermes_path),
        hermes_agent_ref=PINNED_HERMES_REF,
        hermes_provider="openrouter",
        hermes_model="nousresearch/hermes-test",
        google_client_id="google-client-id",
        google_client_secret="google-client-secret",
        google_redirect_uri="https://florence.example.com/oauth/google/callback",
    )
    Store(settings.db_path).get_or_create_household(
        chat_id="pilot-unimportable-hermes-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings))

    response = client.get(
        "/dev/pilot-check/pilot-unimportable-hermes-chat",
        headers={"x-florence-admin-key": "secret-admin-key"},
    )
    deployment = response.json()["deployment"]

    assert response.status_code == 200
    assert deployment["ready"] is False
    assert len(deployment["invalid"]) == 1
    assert "FLORENCE_HERMES_AGENT_PATH could not import run_agent.AIAgent" in deployment["invalid"][0]
    assert "ModuleNotFoundError" in deployment["invalid"][0]


def test_dev_control_plane_requires_existing_household(tmp_path):
    settings = _settings(tmp_path)
    client = TestClient(create_app(settings))

    privacy = client.get("/dev/privacy/missing-chat")
    memory = client.get("/dev/memory/missing-chat")
    readiness = client.get("/dev/readiness/missing-chat")
    connected_accounts = client.get("/dev/connected-accounts/missing-chat")
    source_review = client.get("/dev/source-review/missing-chat")
    source_preferences = client.get("/dev/source-preferences/missing-chat")
    pilot_proof = client.get("/dev/pilot-proof/missing-chat")
    pending_actions = client.get("/dev/actions/missing-chat")
    action_executions = client.get("/dev/actions/missing-chat/executions")
    delete = client.delete("/dev/memory/missing-chat/memory-1")

    assert privacy.status_code == 404
    assert memory.status_code == 404
    assert readiness.status_code == 404
    assert connected_accounts.status_code == 404
    assert source_review.status_code == 404
    assert source_preferences.status_code == 404
    assert pilot_proof.status_code == 404
    assert pending_actions.status_code == 404
    assert action_executions.status_code == 404
    assert delete.status_code == 404
    assert privacy.json()["detail"] == "household_not_found"
    assert Store(settings.db_path).get_household_by_chat("missing-chat") is None


def test_dev_endpoints_can_be_disabled_without_hiding_health(tmp_path):
    client = TestClient(
        create_app(
            _settings(
                tmp_path,
                admin_api_key="secret-admin-key",
                dev_endpoints_enabled=False,
            )
        )
    )

    health = client.get("/health")
    dev = client.get(
        "/dev/privacy/admin-chat",
        headers={"authorization": "Bearer secret-admin-key"},
    )

    assert health.status_code == 200
    assert dev.status_code == 404


def test_public_source_ingest_requires_its_own_key(tmp_path):
    settings = _settings(tmp_path, source_ingest_api_key="source-key")
    disabled = TestClient(create_app(_settings(tmp_path)))
    client = TestClient(create_app(settings))
    Store(settings.db_path).get_or_create_household(
        chat_id="api-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    payload = {
        "chat_id": "api-chat",
        "source_type": "email",
        "title": "Permission slip due",
        "external_id": "email-1",
    }

    not_configured = disabled.post("/api/source-items", json=payload)
    missing = client.post("/api/source-items", json=payload)
    wrong = client.post(
        "/api/source-items",
        json=payload,
        headers={"authorization": "Bearer wrong"},
    )
    allowed = client.post(
        "/api/source-items",
        json=payload,
        headers={"authorization": "Bearer source-key"},
    )

    assert not_configured.status_code == 404
    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert allowed.status_code == 200


def test_public_source_ingest_requires_existing_household_chat(tmp_path):
    settings = _settings(tmp_path, source_ingest_api_key="source-key")
    fake_linq = FakeLinqClient()
    client = TestClient(create_app(settings, linq_client=fake_linq))

    response = client.post(
        "/api/source-items",
        json={
            "chat_id": "unknown-chat",
            "source_type": "email",
            "title": "Permission slip due",
            "external_id": "unknown-email-1",
        },
        headers={"x-florence-source-key": "source-key"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "household_not_found"
    assert fake_linq.sent == []
    assert Store(settings.db_path).get_household_by_chat("unknown-chat") is None


def test_public_source_ingest_requires_external_id_for_idempotency(tmp_path):
    client = TestClient(create_app(_settings(tmp_path, source_ingest_api_key="source-key")))

    response = client.post(
        "/api/source-items",
        json={
            "chat_id": "api-chat",
            "source_type": "email",
            "title": "Permission slip due",
        },
        headers={"x-florence-source-key": "source-key"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "external_id_required"


def test_public_source_ingest_rejects_oversized_body(tmp_path):
    settings = _settings(tmp_path, source_ingest_api_key="source-key")
    fake_linq = FakeLinqClient()
    client = TestClient(create_app(settings, linq_client=fake_linq))
    Store(settings.db_path).get_or_create_household(
        chat_id="api-large-source",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )

    response = client.post(
        "/api/source-items",
        json={
            "chat_id": "api-large-source",
            "source_type": "email",
            "title": "Raw newsletter dump",
            "body": "x" * (MAX_SOURCE_BODY_CHARS + 1),
            "external_id": "large-email-1",
        },
        headers={"x-florence-source-key": "source-key"},
    )
    store = Store(settings.db_path)
    household = store.get_household_by_chat("api-large-source")
    assert household is not None
    snapshot = store.source_review_snapshot(household_id=household.id)

    assert response.status_code == 413
    assert response.json()["detail"] == "body_too_large"
    assert fake_linq.sent == []
    assert snapshot.total == 0


def test_public_source_ingest_dedupes_and_surfaces_through_need_to_know(tmp_path):
    settings = _settings(tmp_path, source_ingest_api_key="source-key")
    fake_linq = FakeLinqClient()
    client = TestClient(create_app(settings, linq_client=fake_linq))
    Store(settings.db_path).get_or_create_household(
        chat_id="api-source-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    payload = {
        "chat_id": "api-source-chat",
        "source_type": "email",
        "title": "Field trip permission slip due",
        "body": "Please sign and bring the permission slip for tomorrow's field trip.",
        "sender": "school@example.com",
        "external_id": "school-email-1",
        "event_at_utc": "2026-06-06T15:00:00Z",
        "now_utc": "2026-06-05T16:00:00Z",
    }
    headers = {"x-florence-source-key": "source-key"}

    first = client.post("/api/source-items", json=payload, headers=headers)
    duplicate = client.post("/api/source-items", json=payload, headers=headers)
    store = Store(settings.db_path)
    household = store.get_household_by_chat("api-source-chat")
    assert household is not None
    snapshot = store.source_review_snapshot(household_id=household.id)
    with store.connect() as conn:
        source_row = conn.execute(
            """
            SELECT surfaced_at_utc FROM source_items
            WHERE household_id = ?
            """,
            (household.id,),
        ).fetchone()

    assert first.status_code == 200
    assert duplicate.status_code == 200
    assert first.json()["sent"] == 1
    assert duplicate.json()["sent"] == 0
    assert len(fake_linq.sent) == 1
    assert fake_linq.sent[0]["chat_id"] == "api-source-chat"
    assert "Field trip permission slip due" in fake_linq.sent[0]["text"]
    assert snapshot.total == 1
    assert snapshot.surfaced == 1
    assert source_row["surfaced_at_utc"] is not None


def test_public_source_ingest_failed_send_records_one_retryable_attempt(tmp_path):
    settings = _settings(tmp_path, source_ingest_api_key="source-key")
    fake_linq = FlakyLinqClient(fail_send=1)
    client = TestClient(create_app(settings, linq_client=fake_linq), raise_server_exceptions=False)
    Store(settings.db_path).get_or_create_household(
        chat_id="api-source-failure-chat",
        timezone_name="America/Los_Angeles",
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    payload = {
        "chat_id": "api-source-failure-chat",
        "source_type": "email",
        "title": "Field trip permission slip due",
        "body": "Please sign and bring the permission slip for tomorrow's field trip.",
        "sender": "school@example.com",
        "external_id": "school-email-failure",
        "event_at_utc": "2026-06-06T15:00:00Z",
        "now_utc": "2026-06-05T16:00:00Z",
    }

    response = client.post(
        "/api/source-items",
        json=payload,
        headers={"x-florence-source-key": "source-key"},
    )
    store = Store(settings.db_path)
    household = store.get_household_by_chat("api-source-failure-chat")
    assert household is not None
    snapshot = store.source_review_snapshot(household_id=household.id)
    with store.connect() as conn:
        source_row = conn.execute(
            """
            SELECT surfaced_at_utc FROM source_items
            WHERE household_id = ?
            """,
            (household.id,),
        ).fetchone()
        delivery = conn.execute(
            """
            SELECT delivery_status, attempts, last_error
            FROM outbound_deliveries
            WHERE household_id = ?
            """,
            (household.id,),
        ).fetchone()

    assert response.status_code == 500
    assert fake_linq.sent == []
    assert snapshot.total == 1
    assert snapshot.surfaced == 0
    assert source_row["surfaced_at_utc"] is None
    assert delivery["delivery_status"] == "failed"
    assert delivery["attempts"] == 1
    assert delivery["last_error"] == "RuntimeError: linq send unavailable"

    retry = client.post(
        "/api/source-items",
        json=payload,
        headers={"x-florence-source-key": "source-key"},
    )
    retry_snapshot = store.source_review_snapshot(household_id=household.id)
    with store.connect() as conn:
        source_row = conn.execute(
            """
            SELECT surfaced_at_utc FROM source_items
            WHERE household_id = ?
            """,
            (household.id,),
        ).fetchone()
        source_count = conn.execute(
            """
            SELECT COUNT(*) AS count FROM source_items
            WHERE household_id = ?
            """,
            (household.id,),
        ).fetchone()
        action_count = conn.execute(
            """
            SELECT COUNT(*) AS count FROM pending_actions
            WHERE household_id = ?
            """,
            (household.id,),
        ).fetchone()
        delivery = conn.execute(
            """
            SELECT delivery_status, attempts, last_error
            FROM outbound_deliveries
            WHERE household_id = ?
            """,
            (household.id,),
        ).fetchone()

    assert retry.status_code == 200
    assert retry.json()["sent"] == 1
    assert len(fake_linq.sent) == 1
    assert fake_linq.sent[0]["chat_id"] == "api-source-failure-chat"
    assert retry_snapshot.total == 1
    assert retry_snapshot.surfaced == 1
    assert source_row["surfaced_at_utc"] is not None
    assert source_count["count"] == 1
    assert action_count["count"] == 1
    assert delivery["delivery_status"] == "sent"
    assert delivery["attempts"] == 2
    assert delivery["last_error"] is None


def test_public_source_ingest_rejects_agent_instruction_payloads(tmp_path):
    settings = _settings(tmp_path, source_ingest_api_key="source-key")
    fake_linq = FakeLinqClient()
    client = TestClient(create_app(settings, linq_client=fake_linq))

    response = client.post(
        "/api/source-items",
        json={
            "chat_id": "api-instruction-chat",
            "source_type": "automation",
            "title": "Do something broad",
            "external_id": "automation-1",
            "message": "Please email the school and update our calendar.",
        },
        headers={"x-florence-source-key": "source-key"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "source_item_not_agent_instruction"
    assert fake_linq.sent == []


def test_public_source_ingest_stores_without_texting_stopped_households(tmp_path):
    settings = _settings(tmp_path, source_ingest_api_key="source-key")
    fake_linq = FakeLinqClient()
    client = TestClient(create_app(settings, linq_client=fake_linq))
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    store = Store(settings.db_path)
    household = store.get_or_create_household(
        chat_id="api-stopped-chat",
        timezone_name="America/Los_Angeles",
        now_utc=now,
    )
    store.set_stopped(household.id, True)

    response = client.post(
        "/api/source-items",
        json={
            "chat_id": "api-stopped-chat",
            "source_type": "email",
            "title": "Permission slip due tomorrow",
            "body": "Please sign and bring the permission slip.",
            "external_id": "stopped-email-1",
            "event_at_utc": "2026-06-06T15:00:00Z",
            "now_utc": "2026-06-05T16:00:00Z",
        },
        headers={"x-florence-source-key": "source-key"},
    )
    snapshot = store.source_review_snapshot(household_id=household.id)

    assert response.status_code == 200
    assert response.json()["sent"] == 0
    assert fake_linq.sent == []
    assert snapshot.total == 1
    assert snapshot.stored_only == 1
    assert snapshot.by_reason["household_stopped"] == 1


def test_pilot_smoke_path_from_linq_to_source_approval_worker_and_reminder(tmp_path):
    secret = "linq-webhook-secret"
    settings = _settings(
        tmp_path,
        admin_api_key="admin-key",
        source_ingest_api_key="source-key",
        linq_webhook_secret=secret,
    )
    fake_linq = FakeLinqClient()
    client = TestClient(create_app(settings, linq_client=fake_linq))
    now = datetime.now(timezone.utc).replace(microsecond=0)
    due_at = now + timedelta(hours=8)
    chat_id = "pilot-smoke-chat"
    admin_headers = {"x-florence-admin-key": "admin-key"}
    source_headers = {"x-florence-source-key": "source-key"}

    name = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="pilot-name-parent-one",
        text="my name is Sam",
        sent_at=now,
    )
    confirm_partner = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="pilot-confirm-partner",
        text="confirm partner +15555550101",
        sent_at=now + timedelta(seconds=1),
    )
    partner_name = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="pilot-name-parent-two",
        sender="+15555550101",
        text="my name is Alex",
        sent_at=now + timedelta(seconds=2),
    )
    child = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="pilot-child",
        text="our child is Maya",
        sent_at=now + timedelta(seconds=3),
    )
    source = client.post(
        "/api/source-items",
        json={
            "chat_id": chat_id,
            "source_type": "email",
            "title": "Field trip permission slip due",
            "body": "Please sign and bring the permission slip for tomorrow's field trip.",
            "sender": "teacher@example.edu",
            "external_id": "pilot-source-permission-slip",
            "event_at_utc": due_at.isoformat(),
            "now_utc": now.isoformat(),
        },
        headers=source_headers,
    )
    source_text = source.json()["messages"][0]["text"]
    actions = client.get(f"/dev/actions/{chat_id}", headers=admin_headers)
    action_id = actions.json()["actions"][0]["id"]
    approval = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="pilot-approve-source-reminder",
        text=f"approve {action_id[:8]}",
        sent_at=now + timedelta(seconds=4),
    )
    approval_text = fake_linq.sent[-1]["text"]
    action_tick = client.post(
        "/dev/actions/tick",
        json={"now_utc": (now + timedelta(minutes=1)).isoformat()},
        headers=admin_headers,
    )
    reminder_tick = client.post(
        "/dev/reminders/tick",
        json={"now_utc": due_at.isoformat()},
        headers=admin_headers,
    )
    source_review = client.get(f"/dev/source-review/{chat_id}", headers=admin_headers)
    executions = client.get(f"/dev/actions/{chat_id}/executions", headers=admin_headers)
    pilot_check = client.get(f"/dev/pilot-check/{chat_id}", headers=admin_headers)

    assert name.status_code == 200
    assert confirm_partner.status_code == 200
    assert partner_name.status_code == 200
    assert child.status_code == 200
    assert source.status_code == 200
    assert source.json()["sent"] == 1
    assert "This looks worth your attention: Field trip permission slip due" in source_text
    assert "approve" in source_text
    assert actions.status_code == 200
    assert actions.json()["actions"][0]["action_type"] == "create_reminder"
    assert approval.status_code == 200
    assert "Approved: Add reminder: Field trip permission slip due" in approval_text
    assert action_tick.json() == {"ok": True, "attempted": 1, "succeeded": 1, "failed": 0}
    assert reminder_tick.status_code == 200
    assert reminder_tick.json()["messages"][0]["text"] == "Quick reminder: Field trip permission slip due"
    assert fake_linq.sent[-1]["text"] == "Quick reminder: Field trip permission slip due"
    assert source_review.json()["snapshot"]["surfaced"] == 1
    assert source_review.json()["snapshot"]["by_reason"]["urgent_actionable_source"] == 1
    assert executions.json()["executions"][0]["status"] == "success"
    assert pilot_check.status_code == 200
    assert pilot_check.json()["delivery"]["ready"] is True
    assert pilot_check.json()["actions"]["ready"] is True
    assert pilot_check.json()["actions"]["failed"] == 0


def test_pilot_smoke_path_from_linq_to_connected_source_worker_and_reminder(tmp_path):
    secret = "linq-webhook-secret"
    settings = _settings(
        tmp_path,
        admin_api_key="admin-key",
        linq_webhook_secret=secret,
    )
    store = Store(settings.db_path)
    agent = FakeAgent("Fake agent reply.")
    fake_linq = FakeLinqClient()
    service = FlorenceService(settings=settings, store=store, agent=agent)
    client = TestClient(create_app(settings, store=store, agent=agent, linq_client=fake_linq))
    now = datetime.now(timezone.utc).replace(microsecond=0)
    sync_now = now + timedelta(minutes=5)
    due_at = sync_now + timedelta(hours=8)
    chat_id = "pilot-connected-source-smoke-chat"
    admin_headers = {"x-florence-admin-key": "admin-key"}

    _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="connected-source-parent-one-name",
        text="my name is Sam",
        sent_at=now,
    )
    _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="connected-source-confirm-partner",
        text="confirm partner +15555550101",
        sent_at=now + timedelta(seconds=1),
    )
    _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="connected-source-parent-two-name",
        sender="+15555550101",
        text="my name is Alex",
        sent_at=now + timedelta(seconds=2),
    )
    _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="connected-source-child",
        text="our child is Maya",
        sent_at=now + timedelta(seconds=3),
    )
    _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="connected-source-rule",
        text="always tell me about permission slips",
        sent_at=now + timedelta(seconds=4),
    )
    service.sync_connected_sources(
        chat_id=chat_id,
        provider="google",
        external_account_id="google-sub-123",
        account_label="Parent Gmail",
        cursor="cursor-1",
        now_utc=now + timedelta(seconds=5),
    )
    provider = FakeConnectedSourceProvider(
        ProviderBatch(
            emails=[
                {
                    "external_id": "gmail-permission-slip-1",
                    "subject": "Field trip permission slip due",
                    "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                    "sender": "teacher@example.edu",
                    "received_at_utc": sync_now.isoformat(),
                    "event_at_utc": due_at.isoformat(),
                }
            ],
            calendar_events=[],
            cursor="cursor-2",
        )
    )

    sync_result = run_source_sync_tick(
        service,
        providers={"google": provider},
        sender=fake_linq,
        now_utc=sync_now,
    )
    source_text = fake_linq.sent[-1]["text"]
    account = service.connected_accounts(chat_id=chat_id)[0]
    actions = client.get(f"/dev/actions/{chat_id}", headers=admin_headers)
    action = actions.json()["actions"][0]
    approval = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="connected-source-approval",
        text=f"approve {action['id'][:8]}",
        sent_at=sync_now + timedelta(seconds=1),
    )
    approval_text = fake_linq.sent[-1]["text"]
    action_tick = client.post(
        "/dev/actions/tick",
        json={"now_utc": (sync_now + timedelta(minutes=1)).isoformat()},
        headers=admin_headers,
    )
    reminder_tick = client.post(
        "/dev/reminders/tick",
        json={"now_utc": due_at.isoformat()},
        headers=admin_headers,
    )
    source_review = client.get(f"/dev/source-review/{chat_id}", headers=admin_headers)
    readiness = client.get(f"/dev/readiness/{chat_id}", headers=admin_headers)

    assert sync_result.checked == 1
    assert sync_result.synced == 1
    assert sync_result.imported == 1
    assert sync_result.surfaced == 1
    assert len(sync_result.messages) == 1
    assert provider.seen_accounts[0].cursor == "cursor-1"
    assert account.provider == "google"
    assert account.cursor == "cursor-2"
    assert source_text.startswith("This looks worth your attention: Field trip permission slip due")
    assert "approve" in source_text
    assert "Please sign and bring the permission slip" not in source_text
    assert actions.status_code == 200
    assert action["action_type"] == "create_reminder"
    assert action["payload"]["title"] == "Field trip permission slip due"
    assert approval.status_code == 200
    assert "Approved: Add reminder: Field trip permission slip due" in approval_text
    assert action_tick.json() == {"ok": True, "attempted": 1, "succeeded": 1, "failed": 0}
    assert reminder_tick.json()["messages"][0]["text"] == "Quick reminder: Field trip permission slip due"
    assert source_review.json()["snapshot"]["surfaced"] == 1
    assert source_review.json()["snapshot"]["by_reason"]["urgent_actionable_source"] == 1
    assert readiness.json()["readiness"]["ready"] is True


def test_linq_webhook_agent_proposal_stays_bounded_by_approval_and_worker(tmp_path):
    secret = "linq-webhook-secret"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    due_at = now + timedelta(hours=6)
    agent = FakeAgent(
        "I can help track that.\n\n"
        "```florence\n"
        "{"
        '"actions":[{'
        '"type":"create_reminder",'
        '"summary":"Add reminder: Pack cleats",'
        '"payload":{'
        '"title":"Pack cleats",'
        f'"due_at_utc":"{due_at.isoformat()}"'
        "}"
        "}]"
        "}\n"
        "```"
    )
    settings = _settings(
        tmp_path,
        admin_api_key="admin-key",
        linq_webhook_secret=secret,
    )
    fake_linq = FakeLinqClient()
    client = TestClient(create_app(settings, agent=agent, linq_client=fake_linq))
    chat_id = "agent-boundary-chat"
    admin_headers = {"x-florence-admin-key": "admin-key"}

    question = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="agent-boundary-question",
        text="Can you make sure we remember cleats for practice?",
        sent_at=now,
    )
    actions = client.get(f"/dev/actions/{chat_id}", headers=admin_headers)
    action = actions.json()["actions"][0]
    approval = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="agent-boundary-approval",
        text=f"approve {action['id'][:8]}",
        sent_at=now + timedelta(seconds=1),
    )
    approval_text = fake_linq.sent[-1]["text"]
    action_tick = client.post(
        "/dev/actions/tick",
        json={"now_utc": (now + timedelta(minutes=1)).isoformat()},
        headers=admin_headers,
    )
    reminder_tick = client.post(
        "/dev/reminders/tick",
        json={"now_utc": due_at.isoformat()},
        headers=admin_headers,
    )
    executions = client.get(f"/dev/actions/{chat_id}/executions", headers=admin_headers)

    assert question.status_code == 200
    assert question.json()["sent"] == 2
    assert len(agent.calls) == 1
    assert agent.calls[0]["user_text"] == "Can you make sure we remember cleats for practice?"
    assert fake_linq.sent[0]["text"] == "I can help track that."
    assert "```florence" not in fake_linq.sent[0]["text"]
    assert "I can do this, but I need a parent to approve first: Add reminder: Pack cleats" in fake_linq.sent[1]["text"]
    assert action["action_type"] == "create_reminder"
    assert action["payload"]["title"] == "Pack cleats"
    assert approval.status_code == 200
    assert "Approved: Add reminder: Pack cleats" in approval_text
    assert action_tick.json() == {"ok": True, "attempted": 1, "succeeded": 1, "failed": 0}
    assert reminder_tick.json()["messages"][0]["text"] == "Quick reminder: Pack cleats"
    assert executions.json()["executions"][0]["status"] == "success"


def test_linq_webhook_partner_invite_creates_group_and_migrates_household(tmp_path):
    settings = _settings(
        tmp_path,
        linq_api_key="linq-api-key",
        linq_from_phone="+15555550000",
    )
    fake_linq = FakeLinqClient()
    client = TestClient(create_app(settings, linq_client=fake_linq))

    response = client.post(
        "/webhooks/linq",
        json={
            "event": "message.received",
            "data": {
                "chat": {"id": "old-chat"},
                "message": {
                    "id": "invite-message",
                    "from": "+15555550100",
                    "parts": [{"type": "text", "value": "invite partner +15555550101"}],
                    "sent_at": "2026-06-05T16:00:00Z",
                },
            },
        },
    )

    store = Store(settings.db_path)
    old_household = store.get_household_by_chat("old-chat")
    new_household = store.get_household_by_chat("group-chat")
    assert response.status_code == 200
    assert response.json()["sent"] == 2
    assert fake_linq.sent[0]["chat_id"] == "old-chat"
    assert fake_linq.created[0]["from_phone"] == "+15555550000"
    assert fake_linq.created[0]["to"] == ("+15555550100", "+15555550101")
    assert old_household is not None
    assert new_household is not None
    assert old_household.id == new_household.id
    assert new_household.chat_id == "group-chat"


def test_linq_webhook_retry_creates_partner_group_without_repeating_ack(tmp_path):
    settings = _settings(
        tmp_path,
        linq_api_key="linq-api-key",
        linq_from_phone="+15555550000",
    )
    fake_linq = FlakyLinqClient(fail_create=1)
    client = TestClient(create_app(settings, linq_client=fake_linq), raise_server_exceptions=False)
    payload = {
        "event": "message.received",
        "data": {
            "chat": {"id": "retry-invite-chat"},
            "message": {
                "id": "retry-invite-message",
                "from": "+15555550100",
                "parts": [{"type": "text", "value": "invite partner +15555550101"}],
                "sent_at": "2026-06-05T16:00:00Z",
            },
        },
    }

    first = client.post("/webhooks/linq", json=payload)
    second = client.post("/webhooks/linq", json=payload)
    third = client.post("/webhooks/linq", json=payload)

    store = Store(settings.db_path)
    old_household = store.get_household_by_chat("retry-invite-chat")
    new_household = store.get_household_by_chat("group-chat")
    assert first.status_code == 500
    assert second.status_code == 200
    assert third.status_code == 200
    assert second.json()["sent"] == 1
    assert third.json()["sent"] == 0
    assert len(fake_linq.sent) == 1
    assert len(fake_linq.created) == 1
    assert old_household is not None
    assert new_household is not None
    assert old_household.id == new_household.id


def test_linq_webhook_duplicate_message_does_not_resend(tmp_path):
    settings = _settings(tmp_path)
    fake_linq = FakeLinqClient()
    client = TestClient(create_app(settings, linq_client=fake_linq))
    payload = {
        "event": "message.received",
        "data": {
            "chat": {"id": "duplicate-chat"},
            "message": {
                "id": "duplicate-message",
                "from": "+15555550100",
                "parts": [{"type": "text", "value": "remind us tomorrow at 8am to pack lunch"}],
                "sent_at": "2026-06-05T16:00:00Z",
            },
        },
    }

    first = client.post("/webhooks/linq", json=payload)
    second = client.post("/webhooks/linq", json=payload)
    store = Store(settings.db_path)
    household = store.get_household_by_chat("duplicate-chat")
    assert household is not None

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["sent"] == 1
    assert second.json()["sent"] == 0
    assert len(fake_linq.sent) == 1
    assert len(store.upcoming_reminders(household_id=household.id, now_utc=household.created_at)) == 1


def test_linq_retry_of_data_deletion_confirmation_does_not_recreate_household(tmp_path):
    secret = "linq-webhook-secret"
    settings = _settings(tmp_path, linq_webhook_secret=secret)
    fake_linq = FakeLinqClient()
    client = TestClient(create_app(settings, linq_client=fake_linq))
    chat_id = "delete-retry-chat"
    sender = "+15555550100"
    now = datetime.now(timezone.utc) - timedelta(minutes=10)

    name = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="delete-name",
        sender=sender,
        text="my name is Sam",
        sent_at=now,
    )
    request = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="delete-request",
        sender=sender,
        text="delete my data",
        sent_at=now + timedelta(minutes=1),
    )
    confirm = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="delete-confirm",
        sender=sender,
        text="confirm delete household data",
        sent_at=now + timedelta(minutes=2),
    )
    replay = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="delete-confirm",
        sender=sender,
        text="confirm delete household data",
        sent_at=now + timedelta(minutes=2),
    )
    request_replay = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="delete-request",
        sender=sender,
        text="delete my data",
        sent_at=now + timedelta(minutes=1),
    )
    name_replay = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="delete-name",
        sender=sender,
        text="my name is Sam",
        sent_at=now,
    )
    store = Store(settings.db_path)

    assert name.status_code == 200
    assert request.status_code == 200
    assert confirm.status_code == 200
    assert replay.status_code == 200
    assert request_replay.status_code == 200
    assert name_replay.status_code == 200
    assert confirm.json()["sent"] == 1
    assert replay.json()["sent"] == 0
    assert request_replay.json()["sent"] == 0
    assert name_replay.json()["sent"] == 0
    assert "deleted this household" in fake_linq.sent[-1]["text"]
    assert store.get_household_by_chat(chat_id) is None

    fresh = _post_signed_linq(
        client,
        secret=secret,
        chat_id=chat_id,
        message_id="fresh-after-delete",
        sender=sender,
        text="my name is Sam",
        sent_at=now + timedelta(minutes=3),
    )

    assert fresh.status_code == 200
    assert fresh.json()["sent"] == 1
    assert store.get_household_by_chat(chat_id) is not None


def test_linq_webhook_retry_sends_failed_outbound_without_duplicate_side_effects(tmp_path):
    settings = _settings(tmp_path)
    fake_linq = FlakyLinqClient(fail_send=1)
    client = TestClient(create_app(settings, linq_client=fake_linq), raise_server_exceptions=False)
    payload = {
        "event": "message.received",
        "data": {
            "chat": {"id": "retry-chat"},
            "message": {
                "id": "retry-message",
                "from": "+15555550100",
                "parts": [{"type": "text", "value": "remind us tomorrow at 8am to pack lunch"}],
                "sent_at": "2026-06-05T16:00:00Z",
            },
        },
    }

    first = client.post("/webhooks/linq", json=payload)
    second = client.post("/webhooks/linq", json=payload)
    third = client.post("/webhooks/linq", json=payload)
    store = Store(settings.db_path)
    household = store.get_household_by_chat("retry-chat")
    assert household is not None

    assert first.status_code == 500
    assert second.status_code == 200
    assert third.status_code == 200
    assert second.json()["sent"] == 1
    assert third.json()["sent"] == 0
    assert len(fake_linq.sent) == 1
    assert len(store.upcoming_reminders(household_id=household.id, now_utc=household.created_at)) == 1


def test_linq_failed_outbound_is_hidden_from_next_agent_history_until_sent(tmp_path):
    settings = _settings(tmp_path)
    agent = FakeAgent("First reply")
    fake_linq = FlakyLinqClient(fail_send=1)
    client = TestClient(
        create_app(settings, agent=agent, linq_client=fake_linq),
        raise_server_exceptions=False,
    )
    first_payload = {
        "event": "message.received",
        "data": {
            "chat": {"id": "history-failure-chat"},
            "message": {
                "id": "history-first-message",
                "from": "+15555550100",
                "parts": [{"type": "text", "value": "Can you help us decide what to prep?"}],
                "sent_at": "2026-06-05T16:00:00Z",
            },
        },
    }
    second_payload = {
        "event": "message.received",
        "data": {
            "chat": {"id": "history-failure-chat"},
            "message": {
                "id": "history-second-message",
                "from": "+15555550100",
                "parts": [{"type": "text", "value": "Anything else?"}],
                "sent_at": "2026-06-05T16:01:00Z",
            },
        },
    }

    first = client.post("/webhooks/linq", json=first_payload)
    second = client.post("/webhooks/linq", json=second_payload)
    store = Store(settings.db_path)
    household = store.get_household_by_chat("history-failure-chat")
    assert household is not None
    history_after_second = store.recent_messages(household.id)

    assert first.status_code == 500
    assert second.status_code == 200
    assert len(agent.calls) == 2
    assert "First reply" not in [
        message["content"] for message in agent.calls[1]["conversation_history"]
    ]
    assert "First reply" in [message["content"] for message in history_after_second]


def test_linq_webhook_missing_message_id_is_ignored_without_state(tmp_path):
    settings = _settings(tmp_path)
    fake_linq = FakeLinqClient()
    client = TestClient(create_app(settings, linq_client=fake_linq))

    response = client.post(
        "/webhooks/linq",
        json={
            "event": "message.received",
            "data": {
                "chat": {"id": "malformed-chat"},
                "message": {
                    "from": "+15555550100",
                    "parts": [{"type": "text", "value": "remind us tomorrow at 8am to pack lunch"}],
                    "sent_at": "2026-06-05T16:00:00Z",
                },
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True, "ignored": True}
    assert fake_linq.sent == []
    assert Store(settings.db_path).get_household_by_chat("malformed-chat") is None


def test_linq_webhook_media_only_message_gets_acknowledged(tmp_path):
    settings = _settings(tmp_path)
    fake_linq = FakeLinqClient()
    client = TestClient(create_app(settings, linq_client=fake_linq))

    response = client.post(
        "/webhooks/linq",
        json={
            "event": "message.received",
            "data": {
                "chat": {"id": "media-chat"},
                "message": {
                    "id": "media-message",
                    "from": "+15555550100",
                    "parts": [
                        {
                            "type": "image",
                            "url": "https://example.com/private/flyer.png",
                            "mime_type": "image/png",
                            "filename": "school-flyer.png",
                        }
                    ],
                    "sent_at": "2026-06-05T16:00:00Z",
                },
            },
        },
    )
    store = Store(settings.db_path)
    household = store.get_household_by_chat("media-chat")
    assert household is not None
    snapshot = store.source_review_snapshot(household_id=household.id)

    assert response.status_code == 200
    assert response.json()["sent"] == 1
    assert len(fake_linq.sent) == 1
    assert "I got the attachment" in fake_linq.sent[0]["text"]
    assert snapshot.total == 1
