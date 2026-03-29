from florence.config import FlorenceRedisRuntimeConfig
from florence.runtime.queue import FlorenceGoogleSyncJob, FlorenceRedisGoogleSyncQueue


class _FakeRedis:
    def __init__(self):
        self.lists: dict[str, list[str]] = {}
        self.values: dict[str, str] = {}

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    def brpoplpush(self, source, destination, timeout=0):
        return self.rpoplpush(source, destination)

    def rpoplpush(self, source, destination):
        bucket = self.lists.setdefault(source, [])
        if not bucket:
            return None
        value = bucket.pop()
        self.lists.setdefault(destination, []).insert(0, value)
        return value

    def lrem(self, key, count, value):
        bucket = self.lists.setdefault(key, [])
        removed = 0
        updated: list[str] = []
        for item in bucket:
            if removed < count and item == value:
                removed += 1
                continue
            updated.append(item)
        self.lists[key] = updated
        return removed

    def delete(self, key):
        self.values.pop(key, None)
        return 1


def _build_queue(fake_redis: _FakeRedis) -> FlorenceRedisGoogleSyncQueue:
    return FlorenceRedisGoogleSyncQueue(
        FlorenceRedisRuntimeConfig(
            url="redis://localhost:6379/0",
            google_sync_queue_name="florence-sync",
            google_sync_queue_processing_name="florence-sync-processing",
        ),
        client_factory=lambda: fake_redis,
    )


def test_redis_google_sync_queue_round_trips_jobs_and_retries():
    fake_redis = _FakeRedis()
    queue = _build_queue(fake_redis)

    assert queue.enqueue(
        FlorenceGoogleSyncJob(
            connection_id="gconn_123",
            thread_id="dm-thread-123",
            notify_when_finished=True,
        )
    )

    claimed = queue.claim(timeout_seconds=0)
    assert claimed is not None
    assert claimed.job.connection_id == "gconn_123"
    assert claimed.job.attempt == 1

    queue.retry(claimed)
    claimed_retry = queue.claim(timeout_seconds=0)
    assert claimed_retry is not None
    assert claimed_retry.job.attempt == 2

    queue.acknowledge(claimed_retry)
    assert fake_redis.lists["florence-sync"] == []
    assert fake_redis.lists["florence-sync-processing"] == []
    assert fake_redis.values == {}


def test_redis_google_sync_queue_requeues_inflight_jobs_on_startup():
    fake_redis = _FakeRedis()
    queue = _build_queue(fake_redis)
    queue.enqueue(
        FlorenceGoogleSyncJob(
            connection_id="gconn_456",
            thread_id=None,
            notify_when_finished=False,
        )
    )
    claimed = queue.claim(timeout_seconds=0)
    assert claimed is not None

    moved = queue.requeue_inflight_jobs()

    assert moved == 1
    reclaimed = queue.claim(timeout_seconds=0)
    assert reclaimed is not None
    assert reclaimed.job.connection_id == "gconn_456"
