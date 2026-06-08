"""Runtime configuration for Florence."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _csv_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.getenv(name)
    if not raw:
        return default
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


DEFAULT_GOOGLE_OAUTH_SCOPES = (
    "openid",
    "email",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/calendar.readonly",
)
DEFAULT_HERMES_TOOLSETS: tuple[str, ...] = ()


@dataclass(frozen=True)
class Settings:
    db_path: str = "./florence.db"
    database_url: str | None = None
    default_timezone: str = "America/Los_Angeles"

    linq_api_key: str | None = None
    linq_base_url: str = "https://api.linqapp.com/api/partner/v3"
    linq_webhook_secret: str | None = None
    linq_from_phone: str | None = None

    dev_endpoints_enabled: bool = True
    admin_api_key: str | None = None
    source_ingest_api_key: str | None = None
    support_contact: str | None = None
    web_base_url: str | None = None
    onboarding_state_secret: str | None = None
    onboarding_token_ttl_hours: int = 14 * 24

    google_client_id: str | None = None
    google_client_secret: str | None = None
    google_redirect_uri: str | None = None
    google_oauth_scopes: tuple[str, ...] = DEFAULT_GOOGLE_OAUTH_SCOPES
    google_oauth_state_ttl_minutes: int = 15
    google_fetch_since_days: int = 7
    google_fetch_calendar_days: int = 14
    google_fetch_max_emails: int = 25
    google_fetch_max_calendar_events: int = 50
    source_sync_interval_seconds: int = 5 * 60

    token_encryption_key: str | None = None

    linq_live_verified: bool = False
    linq_live_verified_at: str | None = None
    linq_live_verification_proof: str | None = None
    google_live_verified: bool = False
    google_live_verified_at: str | None = None
    google_live_verification_proof: str | None = None
    hermes_live_verified: bool = False
    hermes_live_verified_at: str | None = None
    hermes_live_verification_proof: str | None = None

    hermes_agent_path: str | None = None
    hermes_agent_ref: str | None = None
    hermes_provider: str | None = None
    hermes_model: str = ""
    hermes_api_key: str | None = None
    hermes_base_url: str | None = None
    hermes_enabled_toolsets: tuple[str, ...] = DEFAULT_HERMES_TOOLSETS
    hermes_runtime_home: str = "/tmp/florence-hermes-home"
    hermes_strict: bool = False

    daily_briefing_hour: int = 7
    daily_briefing_minute: int = 15
    daily_briefing_delivery_grace_minutes: int = 4 * 60
    reminder_delivery_grace_minutes: int = 24 * 60
    pending_action_ttl_minutes: int = 24 * 60
    data_deletion_confirmation_ttl_minutes: int = 30

    @property
    def database_dsn(self) -> str:
        return self.database_url or self.db_path

    @property
    def database_backend(self) -> str:
        if not self.database_url:
            return "sqlite"
        normalized = self.database_url.strip().lower()
        if normalized.startswith(("postgres://", "postgresql://")):
            return "postgres"
        return "unsupported"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=os.getenv("FLORENCE_DB_PATH", "./florence.db"),
            database_url=os.getenv("FLORENCE_DATABASE_URL"),
            default_timezone=os.getenv("FLORENCE_DEFAULT_TIMEZONE", "America/Los_Angeles"),
            linq_api_key=os.getenv("LINQ_API_KEY"),
            linq_base_url=os.getenv(
                "LINQ_BASE_URL",
                "https://api.linqapp.com/api/partner/v3",
            ),
            linq_webhook_secret=os.getenv("LINQ_WEBHOOK_SECRET"),
            linq_from_phone=os.getenv("LINQ_FROM_PHONE"),
            dev_endpoints_enabled=_bool_env("FLORENCE_DEV_ENDPOINTS_ENABLED", True),
            admin_api_key=os.getenv("FLORENCE_ADMIN_API_KEY"),
            source_ingest_api_key=os.getenv("FLORENCE_SOURCE_INGEST_API_KEY"),
            support_contact=os.getenv("FLORENCE_SUPPORT_CONTACT"),
            web_base_url=os.getenv("FLORENCE_WEB_BASE_URL"),
            onboarding_state_secret=os.getenv("FLORENCE_ONBOARDING_STATE_SECRET"),
            onboarding_token_ttl_hours=_int_env("FLORENCE_ONBOARDING_TOKEN_TTL_HOURS", 14 * 24),
            google_client_id=os.getenv("GOOGLE_CLIENT_ID"),
            google_client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
            google_redirect_uri=os.getenv("GOOGLE_REDIRECT_URI"),
            google_oauth_scopes=_csv_env("GOOGLE_OAUTH_SCOPES", DEFAULT_GOOGLE_OAUTH_SCOPES),
            google_oauth_state_ttl_minutes=_int_env("GOOGLE_OAUTH_STATE_TTL_MINUTES", 15),
            google_fetch_since_days=_int_env("GOOGLE_FETCH_SINCE_DAYS", 7),
            google_fetch_calendar_days=_int_env("GOOGLE_FETCH_CALENDAR_DAYS", 14),
            google_fetch_max_emails=_int_env("GOOGLE_FETCH_MAX_EMAILS", 25),
            google_fetch_max_calendar_events=_int_env("GOOGLE_FETCH_MAX_CALENDAR_EVENTS", 50),
            source_sync_interval_seconds=_int_env("FLORENCE_SOURCE_SYNC_INTERVAL_SECONDS", 5 * 60),
            token_encryption_key=os.getenv("FLORENCE_TOKEN_ENCRYPTION_KEY"),
            linq_live_verified=_bool_env("FLORENCE_LINQ_LIVE_VERIFIED", False),
            linq_live_verified_at=os.getenv("FLORENCE_LINQ_LIVE_VERIFIED_AT"),
            linq_live_verification_proof=os.getenv("FLORENCE_LINQ_LIVE_VERIFICATION_PROOF"),
            google_live_verified=_bool_env("FLORENCE_GOOGLE_LIVE_VERIFIED", False),
            google_live_verified_at=os.getenv("FLORENCE_GOOGLE_LIVE_VERIFIED_AT"),
            google_live_verification_proof=os.getenv("FLORENCE_GOOGLE_LIVE_VERIFICATION_PROOF"),
            hermes_live_verified=_bool_env("FLORENCE_HERMES_LIVE_VERIFIED", False),
            hermes_live_verified_at=os.getenv("FLORENCE_HERMES_LIVE_VERIFIED_AT"),
            hermes_live_verification_proof=os.getenv("FLORENCE_HERMES_LIVE_VERIFICATION_PROOF"),
            hermes_agent_path=os.getenv("FLORENCE_HERMES_AGENT_PATH"),
            hermes_agent_ref=os.getenv("HERMES_AGENT_REF") or os.getenv("FLORENCE_HERMES_AGENT_REF"),
            hermes_provider=os.getenv("FLORENCE_HERMES_PROVIDER"),
            hermes_model=os.getenv("FLORENCE_HERMES_MODEL", ""),
            hermes_api_key=os.getenv("FLORENCE_HERMES_API_KEY") or os.getenv("OPENAI_API_KEY"),
            hermes_base_url=os.getenv("FLORENCE_HERMES_BASE_URL") or os.getenv("OPENAI_BASE_URL"),
            hermes_enabled_toolsets=_csv_env(
                "FLORENCE_HERMES_TOOLSETS",
                DEFAULT_HERMES_TOOLSETS,
            ),
            hermes_runtime_home=os.getenv(
                "FLORENCE_HERMES_RUNTIME_HOME",
                "/tmp/florence-hermes-home",
            ),
            hermes_strict=_bool_env("FLORENCE_HERMES_STRICT", False),
            daily_briefing_hour=_int_env("FLORENCE_DAILY_BRIEFING_HOUR", 7),
            daily_briefing_minute=_int_env("FLORENCE_DAILY_BRIEFING_MINUTE", 15),
            daily_briefing_delivery_grace_minutes=_int_env(
                "FLORENCE_DAILY_BRIEFING_DELIVERY_GRACE_MINUTES",
                4 * 60,
            ),
            reminder_delivery_grace_minutes=_int_env(
                "FLORENCE_REMINDER_DELIVERY_GRACE_MINUTES",
                24 * 60,
            ),
            pending_action_ttl_minutes=_int_env("FLORENCE_PENDING_ACTION_TTL_MINUTES", 24 * 60),
            data_deletion_confirmation_ttl_minutes=_int_env(
                "FLORENCE_DATA_DELETION_CONFIRMATION_TTL_MINUTES",
                30,
            ),
        )
