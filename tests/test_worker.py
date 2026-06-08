from __future__ import annotations

import logging

from florence import worker
from florence.config import Settings


def test_run_forever_continues_after_tick_failure(tmp_path, monkeypatch, caplog):
    calls: list[bool] = []

    def fake_run_worker_tick(service, sender, *, run_sources):
        calls.append(run_sources)
        if len(calls) == 1:
            raise RuntimeError("linq unavailable")
        return None

    monkeypatch.setattr(worker, "run_worker_tick", fake_run_worker_tick)
    monkeypatch.setattr(worker.time, "sleep", lambda _: None)
    caplog.set_level(logging.ERROR, logger="florence.worker")

    worker.run_forever(
        settings=Settings(db_path=str(tmp_path / "worker.db")),
        interval_seconds=0,
        source_sync_interval_seconds=300,
        max_iterations=2,
    )

    assert calls == [True, True]
    assert "Florence worker tick failed; continuing" in caplog.text
