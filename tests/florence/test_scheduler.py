from florence.runtime.scheduler import FlorenceSyncScheduler


class _FakeProductionService:
    def __init__(self):
        self.calls = 0
        self.store = None

    def run_sync_pass(self):
        self.calls += 1
        return {"households": 1, "connections": 1, "candidates": 2, "nudges": 1}


def test_sync_scheduler_run_once_delegates_to_production_service():
    service = _FakeProductionService()
    scheduler = FlorenceSyncScheduler(service, interval_seconds=300)

    result = scheduler.run_once()

    assert result["candidates"] == 2
    assert service.calls == 1


class _RollbackStore:
    def __init__(self):
        self.rollback_calls = 0

    def rollback(self):
        self.rollback_calls += 1


class _FailingProductionService:
    def __init__(self):
        self.store = _RollbackStore()
        self.calls = 0

    def run_sync_pass(self):
        self.calls += 1
        raise RuntimeError("boom")


class _SinglePassStopEvent:
    def __init__(self):
        self._done = False

    def is_set(self):
        return self._done

    def wait(self, _timeout):
        self._done = True
        return True


def test_sync_scheduler_rolls_back_store_after_failed_pass():
    service = _FailingProductionService()
    scheduler = FlorenceSyncScheduler(service, interval_seconds=300)
    scheduler._stop_event = _SinglePassStopEvent()

    scheduler._run_loop()

    assert service.calls == 1
    assert service.store.rollback_calls == 1
