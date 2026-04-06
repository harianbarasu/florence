"""Runtime configuration for Florence production surfaces."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

try:
    import yaml
except Exception:  # pragma: no cover - optional at import time
    yaml = None


def _hermes_home() -> Path:
    return Path(os.getenv("HERMES_HOME", Path.home() / ".hermes"))


def load_florence_environment() -> None:
    """Load Florence/Hermes env vars from HERMES_HOME and local .env files."""
    hermes_home = _hermes_home()
    env_path = hermes_home / ".env"
    if env_path.exists():
        try:
            load_dotenv(env_path, override=False, encoding="utf-8")
        except UnicodeDecodeError:
            load_dotenv(env_path, override=False, encoding="latin-1")
    load_dotenv(override=False)


def _load_config_yaml() -> dict[str, Any]:
    config_path = _hermes_home() / "config.yaml"
    if not config_path.exists() or yaml is None:
        return {}
    try:
        with open(config_path, encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def _env_or_config(env_names: tuple[str, ...], config: dict[str, Any], *path: str, default: Any = None) -> Any:
    for env_name in env_names:
        value = os.getenv(env_name)
        if value is not None and str(value).strip():
            return value

    cursor: Any = config
    for key in path:
        if not isinstance(cursor, dict) or key not in cursor:
            return default
        cursor = cursor[key]
    return cursor if cursor is not None else default


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _as_str_list(value: Any, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    if value is None:
        return default
    if isinstance(value, str):
        return tuple(part.strip() for part in value.split(",") if part.strip())
    if isinstance(value, (list, tuple, set)):
        normalized = [str(part).strip() for part in value if str(part).strip()]
        return tuple(normalized)
    return default


def _normalize_public_base_url(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().rstrip("/")
    if not normalized:
        return None
    if "://" not in normalized:
        normalized = f"https://{normalized}"
    return normalized


def _normalize_fallback_entry(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    provider = str(value.get("provider") or "").strip()
    model = str(value.get("model") or "").strip()
    if not provider or not model:
        return None
    normalized: dict[str, Any] = {
        "provider": provider,
        "model": model,
    }
    for optional_key in ("base_url", "api_key_env"):
        optional_value = value.get(optional_key)
        if optional_value is not None and str(optional_value).strip():
            normalized[optional_key] = str(optional_value).strip()
    return normalized


def _as_fallback_model_chain(value: Any) -> tuple[dict[str, Any], ...]:
    if value is None:
        return ()
    parsed = value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return ()
        try:
            parsed = json.loads(raw)
        except Exception:
            return ()
    if isinstance(parsed, dict):
        entry = _normalize_fallback_entry(parsed)
        return (entry,) if entry else ()
    if isinstance(parsed, (list, tuple)):
        normalized = []
        for item in parsed:
            entry = _normalize_fallback_entry(item)
            if entry is not None:
                normalized.append(entry)
        return tuple(normalized)
    return ()


def _as_tool_use_enforcement(value: Any, default: Any = "auto") -> Any:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (list, tuple, set)):
        normalized = [str(item).strip() for item in value if str(item).strip()]
        return tuple(normalized) if normalized else default
    normalized = str(value).strip()
    if not normalized:
        return default
    try:
        parsed = json.loads(normalized)
    except Exception:
        parsed = None
    if isinstance(parsed, bool):
        return parsed
    if isinstance(parsed, list):
        items = [str(item).strip() for item in parsed if str(item).strip()]
        return tuple(items) if items else default
    if "," in normalized:
        items = [part.strip() for part in normalized.split(",") if part.strip()]
        return tuple(items) if items else default
    lowered = normalized.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    return normalized


def _normalize_honcho_scope(value: Any, default: str = "member") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"member", "channel", "household"}:
        return normalized
    return default


@dataclass(slots=True)
class FlorenceGoogleRuntimeConfig:
    client_id: str | None
    client_secret: str | None
    redirect_uri: str | None
    state_secret: str | None

    @property
    def configured(self) -> bool:
        return bool(self.client_id and self.client_secret and self.redirect_uri and self.state_secret)


@dataclass(slots=True)
class FlorenceLinqRuntimeConfig:
    api_key: str | None
    webhook_secret: str | None
    base_url: str = "https://api.linqapp.com/api/partner/v3"

    @property
    def configured(self) -> bool:
        return bool(self.api_key)


@dataclass(slots=True)
class FlorenceSendblueRuntimeConfig:
    api_key_id: str | None = None
    api_secret_key: str | None = None
    from_number: str | None = None
    webhook_secret: str | None = None
    base_url: str = "https://api.sendblue.co/api"

    @property
    def configured(self) -> bool:
        return bool(self.api_key_id and self.api_secret_key and self.from_number)


@dataclass(slots=True)
class FlorenceHermesRuntimeConfig:
    model: str
    max_iterations: int
    provider: str = "auto"
    enabled_toolsets: tuple[str, ...] = ("florence_chat",)
    disabled_toolsets: tuple[str, ...] = ()
    fallback_model: tuple[dict[str, Any], ...] = ()
    tool_use_enforcement: str | bool | tuple[str, ...] = "auto"
    enable_honcho: bool = True
    honcho_scope: str = "member"


@dataclass(slots=True)
class FlorenceRedisRuntimeConfig:
    url: str | None
    google_sync_queue_name: str = "florence-google-sync"
    google_sync_queue_processing_name: str = "florence-google-sync-processing"
    google_sync_queue_block_seconds: int = 5
    google_sync_job_dedupe_ttl_seconds: int = 1800
    google_sync_max_attempts: int = 3

    @property
    def configured(self) -> bool:
        return bool(self.url)


@dataclass(slots=True)
class FlorenceServerRuntimeConfig:
    host: str
    port: int
    public_base_url: str | None
    onboarding_state_secret: str | None
    sync_interval_seconds: float
    web_base_url: str | None = None
    db_path: Path | None = None
    database_url: str | None = None


@dataclass(slots=True)
class FlorenceSettings:
    server: FlorenceServerRuntimeConfig
    google: FlorenceGoogleRuntimeConfig
    linq: FlorenceLinqRuntimeConfig
    hermes: FlorenceHermesRuntimeConfig
    redis: FlorenceRedisRuntimeConfig
    sendblue: FlorenceSendblueRuntimeConfig = field(default_factory=FlorenceSendblueRuntimeConfig)

    @classmethod
    def from_env(cls) -> "FlorenceSettings":
        load_florence_environment()
        config = _load_config_yaml()
        florence_cfg = config.get("florence", {}) if isinstance(config.get("florence"), dict) else {}

        public_base_url = _normalize_public_base_url(
            _env_or_config(
                ("FLORENCE_PUBLIC_BASE_URL", "PUBLIC_API_BASE_URL", "RAILWAY_PUBLIC_DOMAIN"),
                florence_cfg,
                "public_base_url",
                default=None,
            )
        )
        web_base_url = _normalize_public_base_url(
            _env_or_config(
                ("FLORENCE_WEB_BASE_URL", "PUBLIC_WEB_BASE_URL"),
                florence_cfg,
                "web_base_url",
                default=None,
            )
        )
        google_redirect_uri = _normalize_public_base_url(
            _env_or_config(
                ("FLORENCE_GOOGLE_REDIRECT_URI",),
                florence_cfg,
                "google",
                "redirect_uri",
                default=None,
            )
        )
        if not google_redirect_uri and public_base_url:
            google_redirect_uri = f"{public_base_url}/v1/florence/google/callback"

        db_path_raw = _env_or_config(
            ("FLORENCE_DB_PATH",),
            florence_cfg,
            "db_path",
            default=str(_hermes_home() / "florence.db"),
        )
        database_url = _env_or_config(
            ("FLORENCE_DATABASE_URL", "DATABASE_URL"),
            florence_cfg,
            "database_url",
            default=None,
        )

        return cls(
            server=FlorenceServerRuntimeConfig(
                host=str(_env_or_config(("FLORENCE_HTTP_HOST",), florence_cfg, "http_host", default="0.0.0.0")),
                port=_as_int(_env_or_config(("FLORENCE_HTTP_PORT", "PORT"), florence_cfg, "http_port", default=8081), 8081),
                public_base_url=public_base_url,
                web_base_url=web_base_url,
                onboarding_state_secret=_env_or_config(
                    (
                        "FLORENCE_ONBOARDING_STATE_SECRET",
                        "FLORENCE_GOOGLE_OAUTH_STATE_SECRET",
                        "GOOGLE_OAUTH_STATE_SECRET",
                    ),
                    florence_cfg,
                    "onboarding",
                    "state_secret",
                    default=None,
                ),
                sync_interval_seconds=_as_float(
                    _env_or_config(
                        ("FLORENCE_SYNC_INTERVAL_SECONDS",),
                        florence_cfg,
                        "sync_interval_seconds",
                        default=300,
                    ),
                    300.0,
                ),
                db_path=None if database_url else Path(str(db_path_raw)).expanduser(),
                database_url=str(database_url).strip() if database_url else None,
            ),
            google=FlorenceGoogleRuntimeConfig(
                client_id=_env_or_config(
                    ("FLORENCE_GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_ID"),
                    florence_cfg,
                    "google",
                    "client_id",
                    default=None,
                ),
                client_secret=_env_or_config(
                    ("FLORENCE_GOOGLE_CLIENT_SECRET", "GOOGLE_CLIENT_SECRET"),
                    florence_cfg,
                    "google",
                    "client_secret",
                    default=None,
                ),
                redirect_uri=str(google_redirect_uri).strip() if google_redirect_uri else None,
                state_secret=_env_or_config(
                    ("FLORENCE_GOOGLE_OAUTH_STATE_SECRET", "GOOGLE_OAUTH_STATE_SECRET"),
                    florence_cfg,
                    "google",
                    "state_secret",
                    default=None,
                ),
            ),
            linq=FlorenceLinqRuntimeConfig(
                api_key=_env_or_config(
                    ("FLORENCE_LINQ_API_KEY", "LINQ_API_KEY"),
                    florence_cfg,
                    "linq",
                    "api_key",
                    default=None,
                ),
                webhook_secret=_env_or_config(
                    ("FLORENCE_LINQ_WEBHOOK_SECRET", "LINQ_WEBHOOK_SECRET"),
                    florence_cfg,
                    "linq",
                    "webhook_secret",
                    default=None,
                ),
                base_url=str(
                    _env_or_config(
                        ("FLORENCE_LINQ_BASE_URL", "LINQ_BASE_URL"),
                        florence_cfg,
                        "linq",
                        "base_url",
                        default="https://api.linqapp.com/api/partner/v3",
                    )
                ).rstrip("/"),
            ),
            sendblue=FlorenceSendblueRuntimeConfig(
                api_key_id=_env_or_config(
                    ("FLORENCE_SENDBLUE_API_KEY_ID", "SENDBLUE_API_KEY_ID", "SENDBLUE_API_KEY"),
                    florence_cfg,
                    "sendblue",
                    "api_key_id",
                    default=None,
                ),
                api_secret_key=_env_or_config(
                    ("FLORENCE_SENDBLUE_API_SECRET_KEY", "SENDBLUE_API_SECRET_KEY", "SENDBLUE_API_SECRET"),
                    florence_cfg,
                    "sendblue",
                    "api_secret_key",
                    default=None,
                ),
                from_number=_env_or_config(
                    ("FLORENCE_SENDBLUE_FROM_NUMBER", "SENDBLUE_FROM_NUMBER"),
                    florence_cfg,
                    "sendblue",
                    "from_number",
                    default=None,
                ),
                webhook_secret=_env_or_config(
                    ("FLORENCE_SENDBLUE_WEBHOOK_SECRET", "SENDBLUE_WEBHOOK_SECRET"),
                    florence_cfg,
                    "sendblue",
                    "webhook_secret",
                    default=None,
                ),
                base_url=str(
                    _env_or_config(
                        ("FLORENCE_SENDBLUE_BASE_URL", "SENDBLUE_BASE_URL"),
                        florence_cfg,
                        "sendblue",
                        "base_url",
                        default="https://api.sendblue.co/api",
                    )
                ).rstrip("/"),
            ),
            hermes=FlorenceHermesRuntimeConfig(
                model=str(
                    _env_or_config(
                        ("FLORENCE_HERMES_MODEL", "HERMES_MODEL"),
                        florence_cfg,
                        "hermes",
                        "model",
                        default="anthropic/claude-opus-4.6",
                    )
                ),
                max_iterations=_as_int(
                    _env_or_config(
                        ("FLORENCE_HERMES_MAX_ITERATIONS",),
                        florence_cfg,
                        "hermes",
                        "max_iterations",
                        default=6,
                    ),
                    6,
                ),
                provider=str(
                    _env_or_config(
                        ("FLORENCE_HERMES_PROVIDER", "HERMES_PROVIDER"),
                        florence_cfg,
                        "hermes",
                        "provider",
                        default="auto",
                    )
                ).strip()
                or "auto",
                enabled_toolsets=_as_str_list(
                    _env_or_config(
                        ("FLORENCE_HERMES_ENABLED_TOOLSETS",),
                        florence_cfg,
                        "hermes",
                        "enabled_toolsets",
                        default=("florence_chat",),
                    ),
                    ("florence_chat",),
                ),
                disabled_toolsets=_as_str_list(
                    _env_or_config(
                        ("FLORENCE_HERMES_DISABLED_TOOLSETS",),
                        florence_cfg,
                        "hermes",
                        "disabled_toolsets",
                        default=(),
                    ),
                    (),
                ),
                fallback_model=_as_fallback_model_chain(
                    _env_or_config(
                        ("FLORENCE_HERMES_FALLBACK_MODEL",),
                        florence_cfg,
                        "hermes",
                        "fallback_model",
                        default=(),
                    )
                ),
                tool_use_enforcement=_as_tool_use_enforcement(
                    _env_or_config(
                        ("FLORENCE_HERMES_TOOL_USE_ENFORCEMENT",),
                        florence_cfg,
                        "hermes",
                        "tool_use_enforcement",
                        default="auto",
                    ),
                    "auto",
                ),
                enable_honcho=_as_bool(
                    _env_or_config(
                        ("FLORENCE_HERMES_ENABLE_HONCHO",),
                        florence_cfg,
                        "hermes",
                        "enable_honcho",
                        default=True,
                    ),
                    True,
                ),
                honcho_scope=_normalize_honcho_scope(
                    _env_or_config(
                        ("FLORENCE_HERMES_HONCHO_SCOPE",),
                        florence_cfg,
                        "hermes",
                        "honcho_scope",
                        default="member",
                    ),
                    "member",
                ),
            ),
            redis=FlorenceRedisRuntimeConfig(
                url=_env_or_config(
                    ("FLORENCE_REDIS_URL", "REDIS_URL"),
                    florence_cfg,
                    "redis",
                    "url",
                    default=None,
                ),
                google_sync_queue_name=str(
                    _env_or_config(
                        ("FLORENCE_GOOGLE_SYNC_QUEUE_NAME",),
                        florence_cfg,
                        "redis",
                        "google_sync_queue_name",
                        default="florence-google-sync",
                    )
                ).strip()
                or "florence-google-sync",
                google_sync_queue_processing_name=str(
                    _env_or_config(
                        ("FLORENCE_GOOGLE_SYNC_QUEUE_PROCESSING_NAME",),
                        florence_cfg,
                        "redis",
                        "google_sync_queue_processing_name",
                        default="florence-google-sync-processing",
                    )
                ).strip()
                or "florence-google-sync-processing",
                google_sync_queue_block_seconds=_as_int(
                    _env_or_config(
                        ("FLORENCE_GOOGLE_SYNC_QUEUE_BLOCK_SECONDS",),
                        florence_cfg,
                        "redis",
                        "google_sync_queue_block_seconds",
                        default=5,
                    ),
                    5,
                ),
                google_sync_job_dedupe_ttl_seconds=_as_int(
                    _env_or_config(
                        ("FLORENCE_GOOGLE_SYNC_JOB_DEDUPE_TTL_SECONDS",),
                        florence_cfg,
                        "redis",
                        "google_sync_job_dedupe_ttl_seconds",
                        default=1800,
                    ),
                    1800,
                ),
                google_sync_max_attempts=_as_int(
                    _env_or_config(
                        ("FLORENCE_GOOGLE_SYNC_MAX_ATTEMPTS",),
                        florence_cfg,
                        "redis",
                        "google_sync_max_attempts",
                        default=3,
                    ),
                    3,
                ),
            ),
        )
