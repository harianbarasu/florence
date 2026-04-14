import time

from florence.runtime.scheduler import FlorenceSyncScheduler


class _FakeSchedulerService:
    def __init__(self):
        self.sync_calls: list[bool] = []
        self.automation_calls = 0

    def run_sync_pass(self):
        raise AssertionError("scheduler should use run_sync_pass_with_options in the background sync loop")

    def run_sync_pass_with_options(self, *, include_automation: bool):
        self.sync_calls.append(include_automation)
        return {"sync": len(self.sync_calls)}

    def run_automation_pass(self):
        self.automation_calls += 1
        return {"automation": self.automation_calls}


def test_sync_scheduler_runs_automation_loop_separately_from_sync_loop():
    service = _FakeSchedulerService()
    scheduler = FlorenceSyncScheduler(
        service,
        interval_seconds=0.12,
        automation_interval_seconds=0.05,
    )

    scheduler.start()
    try:
        time.sleep(0.24)
    finally:
        scheduler.stop(timeout=1.0)

    assert service.sync_calls
    assert all(include_automation is False for include_automation in service.sync_calls)
    assert service.automation_calls > len(service.sync_calls)
