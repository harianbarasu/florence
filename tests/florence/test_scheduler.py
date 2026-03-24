from florence.runtime.scheduler import FlorenceSyncScheduler


class _FakeProductionService:
    def __init__(self):
        self.calls = 0

    def run_sync_pass(self):
        self.calls += 1
        return {"households": 1, "connections": 1, "candidates": 2, "nudges": 1}


def test_sync_scheduler_run_once_delegates_to_production_service():
    service = _FakeProductionService()
    scheduler = FlorenceSyncScheduler(service, interval_seconds=300)

    result = scheduler.run_once()

    assert result["candidates"] == 2
    assert service.calls == 1
