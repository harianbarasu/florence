"""Deployment metadata helpers for hosted Florence runtimes."""

from __future__ import annotations

import os
from collections.abc import Mapping

_RAILWAY_ENV_KEYS: dict[str, tuple[str, ...]] = {
    "project_id": ("RAILWAY_PROJECT_ID",),
    "environment_id": ("RAILWAY_ENVIRONMENT_ID",),
    "service_id": ("RAILWAY_SERVICE_ID",),
    "deployment_id": ("RAILWAY_DEPLOYMENT_ID", "RAILWAY_DEPLOY_ID"),
    "replica_id": ("RAILWAY_REPLICA_ID",),
    "branch": ("RAILWAY_GIT_BRANCH",),
    "commit_sha": ("RAILWAY_GIT_COMMIT_SHA", "RAILWAY_GIT_COMMIT_HASH"),
    "commit_message": ("RAILWAY_GIT_COMMIT_MESSAGE",),
}

_DISPLAY_ORDER = (
    "project_id",
    "environment_id",
    "service_id",
    "deployment_id",
    "replica_id",
    "branch",
    "commit_sha",
    "commit_message",
)


def _first_non_empty(env: Mapping[str, str], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = str(env.get(key) or "").strip()
        if value:
            return value
    return None


def get_railway_deploy_metadata(env: Mapping[str, str] | None = None) -> dict[str, str | None]:
    source = env or os.environ
    metadata: dict[str, str | None] = {}
    for field, keys in _RAILWAY_ENV_KEYS.items():
        value = _first_non_empty(source, keys)
        if field == "commit_sha" and value:
            value = value[:12]
        metadata[field] = value
    return metadata


def format_railway_deploy_metadata(env: Mapping[str, str] | None = None) -> str:
    metadata = get_railway_deploy_metadata(env)
    parts = [
        f"{field}={value}"
        for field in _DISPLAY_ORDER
        if (value := metadata.get(field))
    ]
    return ", ".join(parts) if parts else "Railway deployment metadata unavailable"
