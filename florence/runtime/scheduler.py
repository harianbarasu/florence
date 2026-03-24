"""Background sync scheduler for Florence production."""

from __future__ import annotations

import logging
import threading
import time

from florence.runtime.production import FlorenceProductionService

logger = logging.getLogger(__name__)


class FlorenceSyncScheduler:
    """Runs periodic Google sync for Florence in a background thread."""

    def __init__(
        self,
        service: FlorenceProductionService,
        *,
        interval_seconds: float,
    ):
        self.service = service
        self.interval_seconds = max(interval_seconds, 30.0)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="florence-sync", daemon=True)
        self._thread.start()

    def stop(self, *, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self._thread = None

    def run_once(self) -> dict[str, int]:
        return self.service.run_sync_pass()

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                result = self.run_once()
                logger.info("Florence sync pass complete: %s", result)
            except Exception:
                logger.exception("Florence sync pass failed")
            self._stop_event.wait(self.interval_seconds)
