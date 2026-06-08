import pytest

from florence.config import Settings
from florence.store import Store


def test_hermes_toolsets_are_empty_by_default(monkeypatch):
    monkeypatch.delenv("FLORENCE_HERMES_TOOLSETS", raising=False)

    assert Settings().hermes_enabled_toolsets == ()
    assert Settings.from_env().hermes_enabled_toolsets == ()


def test_hermes_toolsets_can_be_configured_from_env(monkeypatch):
    monkeypatch.setenv("FLORENCE_HERMES_TOOLSETS", "web,filesystem")

    assert Settings.from_env().hermes_enabled_toolsets == ("web", "filesystem")


def test_source_sync_interval_can_be_configured(monkeypatch):
    monkeypatch.setenv("FLORENCE_SOURCE_SYNC_INTERVAL_SECONDS", "900")

    assert Settings.from_env().source_sync_interval_seconds == 900


def test_hermes_agent_ref_can_be_configured(monkeypatch):
    monkeypatch.setenv("HERMES_AGENT_REF", "0123456789abcdef0123456789abcdef01234567")

    assert Settings.from_env().hermes_agent_ref == "0123456789abcdef0123456789abcdef01234567"


def test_hermes_runtime_home_can_be_configured(monkeypatch):
    monkeypatch.delenv("FLORENCE_HERMES_RUNTIME_HOME", raising=False)

    assert Settings.from_env().hermes_runtime_home == "/tmp/florence-hermes-home"

    monkeypatch.setenv("FLORENCE_HERMES_RUNTIME_HOME", "/tmp/florence-test-hermes-home")

    assert Settings.from_env().hermes_runtime_home == "/tmp/florence-test-hermes-home"


def test_hermes_credentials_use_florence_env_with_openai_fallback(monkeypatch):
    monkeypatch.delenv("FLORENCE_HERMES_API_KEY", raising=False)
    monkeypatch.delenv("FLORENCE_HERMES_BASE_URL", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "openai-key")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://llm.example/v1")

    fallback = Settings.from_env()
    assert fallback.hermes_api_key == "openai-key"
    assert fallback.hermes_base_url == "https://llm.example/v1"

    monkeypatch.setenv("FLORENCE_HERMES_API_KEY", "florence-hermes-key")
    monkeypatch.setenv("FLORENCE_HERMES_BASE_URL", "https://hermes.example/v1")

    configured = Settings.from_env()
    assert configured.hermes_api_key == "florence-hermes-key"
    assert configured.hermes_base_url == "https://hermes.example/v1"


def test_daily_briefing_delivery_grace_can_be_configured(monkeypatch):
    monkeypatch.setenv("FLORENCE_DAILY_BRIEFING_DELIVERY_GRACE_MINUTES", "120")

    assert Settings.from_env().daily_briefing_delivery_grace_minutes == 120


def test_data_deletion_confirmation_ttl_can_be_configured(monkeypatch):
    monkeypatch.setenv("FLORENCE_DATA_DELETION_CONFIRMATION_TTL_MINUTES", "10")

    assert Settings.from_env().data_deletion_confirmation_ttl_minutes == 10


def test_support_contact_can_be_configured(monkeypatch):
    monkeypatch.setenv("FLORENCE_SUPPORT_CONTACT", "support@example.com")

    assert Settings.from_env().support_contact == "support@example.com"


def test_database_url_selects_postgres_backend(monkeypatch):
    monkeypatch.delenv("FLORENCE_DATABASE_URL", raising=False)

    local = Settings.from_env()
    assert local.database_dsn == local.db_path
    assert local.database_backend == "sqlite"

    monkeypatch.setenv("FLORENCE_DATABASE_URL", "postgresql://florence:secret@db/florence")

    deployed = Settings.from_env()
    assert deployed.database_dsn == "postgresql://florence:secret@db/florence"
    assert deployed.database_backend == "postgres"


def test_store_rejects_url_style_database_strings_that_are_not_postgres(tmp_path):
    sqlite_url = f"sqlite:///{tmp_path / 'florence.sqlite'}"

    with pytest.raises(ValueError, match="Unsupported Florence database URL scheme 'sqlite'"):
        Store(sqlite_url)


def test_live_verification_flags_default_off_and_parse_from_env(monkeypatch):
    monkeypatch.delenv("FLORENCE_LINQ_LIVE_VERIFIED", raising=False)
    monkeypatch.delenv("FLORENCE_LINQ_LIVE_VERIFIED_AT", raising=False)
    monkeypatch.delenv("FLORENCE_LINQ_LIVE_VERIFICATION_PROOF", raising=False)
    monkeypatch.delenv("FLORENCE_GOOGLE_LIVE_VERIFIED", raising=False)
    monkeypatch.delenv("FLORENCE_GOOGLE_LIVE_VERIFIED_AT", raising=False)
    monkeypatch.delenv("FLORENCE_GOOGLE_LIVE_VERIFICATION_PROOF", raising=False)
    monkeypatch.delenv("FLORENCE_HERMES_LIVE_VERIFIED", raising=False)
    monkeypatch.delenv("FLORENCE_HERMES_LIVE_VERIFIED_AT", raising=False)
    monkeypatch.delenv("FLORENCE_HERMES_LIVE_VERIFICATION_PROOF", raising=False)

    defaults = Settings.from_env()
    assert defaults.linq_live_verified is False
    assert defaults.linq_live_verified_at is None
    assert defaults.linq_live_verification_proof is None
    assert defaults.google_live_verified is False
    assert defaults.google_live_verified_at is None
    assert defaults.google_live_verification_proof is None
    assert defaults.hermes_live_verified is False
    assert defaults.hermes_live_verified_at is None
    assert defaults.hermes_live_verification_proof is None

    monkeypatch.setenv("FLORENCE_LINQ_LIVE_VERIFIED", "1")
    monkeypatch.setenv("FLORENCE_LINQ_LIVE_VERIFIED_AT", "2026-06-05T16:00:00Z")
    monkeypatch.setenv("FLORENCE_LINQ_LIVE_VERIFICATION_PROOF", "Linq chat smoke run 2026-06-05")
    monkeypatch.setenv("FLORENCE_GOOGLE_LIVE_VERIFIED", "true")
    monkeypatch.setenv("FLORENCE_GOOGLE_LIVE_VERIFIED_AT", "2026-06-05T16:05:00Z")
    monkeypatch.setenv("FLORENCE_GOOGLE_LIVE_VERIFICATION_PROOF", "Google OAuth/source sync smoke run 2026-06-05")
    monkeypatch.setenv("FLORENCE_HERMES_LIVE_VERIFIED", "yes")
    monkeypatch.setenv("FLORENCE_HERMES_LIVE_VERIFIED_AT", "2026-06-05T16:10:00Z")
    monkeypatch.setenv("FLORENCE_HERMES_LIVE_VERIFICATION_PROOF", "Hermes smoke endpoint returned live_hermes_verified")

    configured = Settings.from_env()
    assert configured.linq_live_verified is True
    assert configured.linq_live_verified_at == "2026-06-05T16:00:00Z"
    assert configured.linq_live_verification_proof == "Linq chat smoke run 2026-06-05"
    assert configured.google_live_verified is True
    assert configured.google_live_verified_at == "2026-06-05T16:05:00Z"
    assert configured.google_live_verification_proof == "Google OAuth/source sync smoke run 2026-06-05"
    assert configured.hermes_live_verified is True
    assert configured.hermes_live_verified_at == "2026-06-05T16:10:00Z"
    assert configured.hermes_live_verification_proof == "Hermes smoke endpoint returned live_hermes_verified"
