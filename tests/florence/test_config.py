from florence.config import FlorenceSettings


def test_florence_settings_reads_env_and_derives_google_redirect_uri(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("FLORENCE_PUBLIC_BASE_URL", "https://florence.example.com")
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "google-client")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "google-secret")
    monkeypatch.setenv("GOOGLE_OAUTH_STATE_SECRET", "state-secret")
    monkeypatch.setenv("LINQ_API_KEY", "linq-api-key")
    monkeypatch.setenv("FLORENCE_HTTP_PORT", "9090")

    settings = FlorenceSettings.from_env()

    assert settings.server.port == 9090
    assert settings.server.public_base_url == "https://florence.example.com"
    assert settings.server.web_base_url is None
    assert settings.google.redirect_uri == "https://florence.example.com/v1/florence/google/callback"
    assert settings.google.configured is True
    assert settings.linq.configured is True


def test_florence_settings_supports_railway_public_domain_and_port(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.delenv("FLORENCE_PUBLIC_BASE_URL", raising=False)
    monkeypatch.delenv("PUBLIC_API_BASE_URL", raising=False)
    monkeypatch.setenv("RAILWAY_PUBLIC_DOMAIN", "florence-production.up.railway.app")
    monkeypatch.setenv("PORT", "8080")

    settings = FlorenceSettings.from_env()

    assert settings.server.port == 8080
    assert settings.server.public_base_url == "https://florence-production.up.railway.app"
    assert settings.google.redirect_uri == "https://florence-production.up.railway.app/v1/florence/google/callback"


def test_florence_settings_reads_web_base_url_when_configured(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("FLORENCE_PUBLIC_BASE_URL", "https://api.florence.example.com")
    monkeypatch.setenv("FLORENCE_WEB_BASE_URL", "https://app.florence.example.com")

    settings = FlorenceSettings.from_env()

    assert settings.server.public_base_url == "https://api.florence.example.com"
    assert settings.server.web_base_url == "https://app.florence.example.com"


def test_florence_settings_prefers_database_url_when_present(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("DATABASE_URL", "postgresql://postgres:secret@db.example.com:5432/florence")

    settings = FlorenceSettings.from_env()

    assert settings.server.database_url == "postgresql://postgres:secret@db.example.com:5432/florence"
    assert settings.server.db_path is None


def test_florence_settings_default_and_override_household_toolsets(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    default_settings = FlorenceSettings.from_env()
    assert default_settings.hermes.provider == "auto"
    assert default_settings.hermes.enabled_toolsets == ("florence_chat",)
    assert default_settings.hermes.disabled_toolsets == ()

    monkeypatch.setenv("FLORENCE_HERMES_PROVIDER", "custom")
    monkeypatch.setenv("FLORENCE_HERMES_ENABLED_TOOLSETS", "web,browser,clarify")
    monkeypatch.setenv("FLORENCE_HERMES_DISABLED_TOOLSETS", "memory,session_search")

    overridden_settings = FlorenceSettings.from_env()
    assert overridden_settings.hermes.provider == "custom"
    assert overridden_settings.hermes.enabled_toolsets == ("web", "browser", "clarify")
    assert overridden_settings.hermes.disabled_toolsets == ("memory", "session_search")
