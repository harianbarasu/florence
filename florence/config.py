"""Runtime configuration for Florence production surfaces."""

from __future__ import annotations

import os
from dataclasses import dataclass
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
class FlorenceHermesRuntimeConfig:
    model: str
    max_iterations: int
    provider: str = "auto"
    enabled_toolsets: tuple[str, ...] = ("florence_chat",)
    disabled_toolsets: tuple[str, ...] = ()


@dataclass(slots=True)
class FlorenceServerRuntimeConfig:
    host: str
    port: int
    public_base_url: str | None
    sync_interval_seconds: float
    db_path: Path | None = None
    database_url: str | None = None


@dataclass(slots=True)
class FlorenceSettings:
    server: FlorenceServerRuntimeConfig
    google: FlorenceGoogleRuntimeConfig
    linq: FlorenceLinqRuntimeConfig
    hermes: FlorenceHermesRuntimeConfig

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
            ),
        )
