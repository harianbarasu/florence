from florence.config import FlorenceSettings


def test_florence_settings_reads_env_and_derives_google_redirect_uri(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("FLORENCE_PUBLIC_BASE_URL", "https://florence.example.com")
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "google-client")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "google-secret")
    monkeypatch.setenv("GOOGLE_OAUTH_STATE_SECRET", "state-secret")
    monkeypatch.setenv("BLUEBUBBLES_BASE_URL", "https://bb.example.com")
    monkeypatch.setenv("BLUEBUBBLES_PASSWORD", "bb-password")
    monkeypatch.setenv("LINQ_API_KEY", "linq-api-key")
    monkeypatch.setenv("FLORENCE_HTTP_PORT", "9090")

    settings = FlorenceSettings.from_env()

    assert settings.server.port == 9090
    assert settings.server.public_base_url == "https://florence.example.com"
    assert settings.google.redirect_uri == "https://florence.example.com/v1/florence/google/callback"
    assert settings.google.configured is True
    assert settings.bluebubbles.configured is True
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


def test_florence_settings_prefers_database_url_when_present(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("DATABASE_URL", "postgresql://postgres:secret@db.example.com:5432/florence")

    settings = FlorenceSettings.from_env()

    assert settings.server.database_url == "postgresql://postgres:secret@db.example.com:5432/florence"
    assert settings.server.db_path is None
