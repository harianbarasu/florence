"""Runtime configuration. One flat Settings object, everything from env."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name) or "").strip() or default)
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float((os.getenv(name) or "").strip() or default)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    # Storage
    database_url: str = "postgresql://florence:florence@localhost:5433/florence"
    db_schema: str = "florence_v3"

    # Identity / web
    web_base_url: str = "http://localhost:8000"
    admin_api_key: str | None = None
    support_contact: str | None = None
    default_timezone: str = "America/Los_Angeles"

    # Linq (iMessage transport)
    linq_api_key: str | None = None
    linq_base_url: str = "https://api.linqapp.com/api/partner/v3"
    linq_webhook_secret: str | None = None
    linq_from_phone: str | None = None

    # Google OAuth (Gmail + Calendar, read-only)
    google_client_id: str | None = None
    google_client_secret: str | None = None
    google_redirect_uri: str | None = None
    token_encryption_key: str | None = None

    # Model: any OpenAI-compatible chat-completions endpoint.
    # Swap providers (OpenAI, Nous Portal, OpenRouter, ...) with env vars only.
    model: str = "gpt-5.5"
    model_base_url: str = "https://api.openai.com/v1"
    model_api_key: str | None = None
    reasoning_effort: str | None = None
    max_turn_steps: int = 10
    max_sends_per_turn: int = 5

    # Rhythms
    debounce_seconds: float = 2.5
    scheduler_interval_seconds: int = 20
    gmail_sync_interval_seconds: int = 300
    gmail_max_per_sync: int = 25
    brief_hour: int = 7
    brief_minute: int = 15

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.getenv("FLORENCE_DATABASE_URL")
            or "postgresql://florence:florence@localhost:5433/florence",
            db_schema=os.getenv("FLORENCE_DB_SCHEMA", "florence_v3"),
            web_base_url=(os.getenv("FLORENCE_WEB_BASE_URL") or "http://localhost:8000").rstrip(
                "/"
            ),
            admin_api_key=os.getenv("FLORENCE_ADMIN_API_KEY") or None,
            support_contact=os.getenv("FLORENCE_SUPPORT_CONTACT") or None,
            default_timezone=os.getenv("FLORENCE_DEFAULT_TIMEZONE", "America/Los_Angeles"),
            linq_api_key=os.getenv("LINQ_API_KEY") or None,
            linq_base_url=os.getenv("LINQ_BASE_URL", "https://api.linqapp.com/api/partner/v3"),
            linq_webhook_secret=os.getenv("LINQ_WEBHOOK_SECRET") or None,
            linq_from_phone=os.getenv("LINQ_FROM_PHONE") or None,
            google_client_id=os.getenv("GOOGLE_CLIENT_ID") or None,
            google_client_secret=os.getenv("GOOGLE_CLIENT_SECRET") or None,
            google_redirect_uri=os.getenv("GOOGLE_REDIRECT_URI") or None,
            token_encryption_key=os.getenv("FLORENCE_TOKEN_ENCRYPTION_KEY") or None,
            model=os.getenv("FLORENCE_MODEL", "gpt-5.5"),
            model_base_url=(
                os.getenv("FLORENCE_MODEL_BASE_URL")
                or os.getenv("OPENAI_BASE_URL")
                or "https://api.openai.com/v1"
            ).rstrip("/"),
            model_api_key=os.getenv("FLORENCE_MODEL_API_KEY") or os.getenv("OPENAI_API_KEY") or None,
            reasoning_effort=os.getenv("FLORENCE_REASONING_EFFORT") or None,
            max_turn_steps=_int("FLORENCE_MAX_TURN_STEPS", 10),
            max_sends_per_turn=_int("FLORENCE_MAX_SENDS_PER_TURN", 5),
            debounce_seconds=_float("FLORENCE_DEBOUNCE_SECONDS", 2.5),
            scheduler_interval_seconds=_int("FLORENCE_SCHEDULER_INTERVAL_SECONDS", 20),
            gmail_sync_interval_seconds=_int("FLORENCE_GMAIL_SYNC_INTERVAL_SECONDS", 300),
            gmail_max_per_sync=_int("FLORENCE_GMAIL_MAX_PER_SYNC", 25),
            brief_hour=_int("FLORENCE_BRIEF_HOUR", 7),
            brief_minute=_int("FLORENCE_BRIEF_MINUTE", 15),
        )
