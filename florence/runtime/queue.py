"""Redis-backed Florence background job queue primitives."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace
from typing import Any, Callable

from florence.config import FlorenceRedisRuntimeConfig

try:  # pragma: no cover - dependency is optional in local test envs
    from redis import Redis
except Exception:  # pragma: no cover - exercised via fallback tests
    Redis = None


@dataclass(slots=True)
class FlorenceGoogleSyncJob:
    connection_id: str
    thread_id: str | None
    notify_when_finished: bool
    attempt: int = 1


@dataclass(slots=True)
class FlorenceClaimedGoogleSyncJob:
    job: FlorenceGoogleSyncJob
    raw_payload: str


class FlorenceRedisGoogleSyncQueue:
    """Simple Redis list queue with inflight recovery for Google sync jobs."""

    def __init__(
        self,
        config: FlorenceRedisRuntimeConfig,
        *,
        client_factory: Callable[[], Any] | None = None,
    ):
        self.config = config
        self._client_factory = client_factory
        self._client: Any | None = None

    @property
    def configured(self) -> bool:
        return self.config.configured

    def enqueue(self, job: FlorenceGoogleSyncJob) -> bool:
        client = self._redis()
        if not client.set(self._guard_key(job.connection_id), "1", nx=True, ex=self.config.google_sync_job_dedupe_ttl_seconds):
            return False
        client.rpush(self.config.google_sync_queue_name, self._serialize(job))
        return True

    def claim(self, *, timeout_seconds: int | None = None) -> FlorenceClaimedGoogleSyncJob | None:
        client = self._redis()
        raw = client.brpoplpush(
            self.config.google_sync_queue_name,
            self.config.google_sync_queue_processing_name,
            timeout=timeout_seconds if timeout_seconds is not None else self.config.google_sync_queue_block_seconds,
        )
        if raw is None:
            return None
        raw_text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        job = self._deserialize(raw_text)
        return FlorenceClaimedGoogleSyncJob(job=job, raw_payload=raw_text)

    def acknowledge(self, claimed: FlorenceClaimedGoogleSyncJob) -> None:
        client = self._redis()
        client.lrem(self.config.google_sync_queue_processing_name, 1, claimed.raw_payload)
        client.delete(self._guard_key(claimed.job.connection_id))

    def retry(self, claimed: FlorenceClaimedGoogleSyncJob) -> None:
        client = self._redis()
        next_job = replace(claimed.job, attempt=claimed.job.attempt + 1)
        client.lrem(self.config.google_sync_queue_processing_name, 1, claimed.raw_payload)
        client.rpush(self.config.google_sync_queue_name, self._serialize(next_job))

    def requeue_inflight_jobs(self) -> int:
        client = self._redis()
        moved = 0
        while True:
            raw = client.rpoplpush(
                self.config.google_sync_queue_processing_name,
                self.config.google_sync_queue_name,
            )
            if raw is None:
                break
            moved += 1
        return moved

    def _redis(self) -> Any:
        if not self.config.configured:
            raise RuntimeError("florence_redis_queue_not_configured")
        if self._client is not None:
            return self._client
        if self._client_factory is not None:
            self._client = self._client_factory()
            return self._client
        if Redis is None:  # pragma: no cover - depends on optional package
            raise RuntimeError("redis_package_not_installed")
        self._client = Redis.from_url(self.config.url)
        return self._client

    @staticmethod
    def _serialize(job: FlorenceGoogleSyncJob) -> str:
        return json.dumps(asdict(job), separators=(",", ":"))

    @staticmethod
    def _deserialize(raw_payload: str) -> FlorenceGoogleSyncJob:
        payload = json.loads(raw_payload)
        return FlorenceGoogleSyncJob(
            connection_id=str(payload["connection_id"]),
            thread_id=str(payload["thread_id"]) if payload.get("thread_id") is not None else None,
            notify_when_finished=bool(payload.get("notify_when_finished")),
            attempt=int(payload.get("attempt") or 1),
        )

    @staticmethod
    def _guard_key(connection_id: str) -> str:
        return f"florence:google-sync:guard:{connection_id}"
