"""Background sync scheduler for Florence production."""

from __future__ import annotations

import logging
import threading
import time

from florence.runtime.production import FlorenceProductionService

logger = logging.getLogger(__name__)


class FlorenceSyncScheduler:
    """Runs periodic Florence automation and Google sync in background threads."""

    def __init__(
        self,
        service: FlorenceProductionService,
        *,
        interval_seconds: float,
        automation_interval_seconds: float = 30.0,
    ):
        self.service = service
        self.interval_seconds = max(interval_seconds, 0.1)
        self.automation_interval_seconds = max(
            min(automation_interval_seconds, self.interval_seconds),
            0.05,
        )
        self._stop_event = threading.Event()
        self._sync_thread: threading.Thread | None = None
        self._automation_thread: threading.Thread | None = None

    def start(self) -> None:
        if self._sync_thread and self._sync_thread.is_alive():
            return
        self._stop_event.clear()
        self._automation_thread = threading.Thread(
            target=self._run_automation_loop,
            name="florence-automation",
            daemon=True,
        )
        self._sync_thread = threading.Thread(
            target=self._run_sync_loop,
            name="florence-sync",
            daemon=True,
        )
        self._automation_thread.start()
        self._sync_thread.start()

    def stop(self, *, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._automation_thread and self._automation_thread.is_alive():
            self._automation_thread.join(timeout=timeout)
        if self._sync_thread and self._sync_thread.is_alive():
            self._sync_thread.join(timeout=timeout)
        self._automation_thread = None
        self._sync_thread = None

    def run_once(self) -> dict[str, int]:
        return self.service.run_sync_pass()

    def run_automation_once(self) -> dict[str, int]:
        return self.service.run_automation_pass()

    def _run_sync_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                result = self.service.run_sync_pass_with_options(include_automation=False)
                if any(value for value in result.values()):
                    logger.info("Florence sync pass complete: %s", result)
                else:
                    logger.debug("Florence sync pass complete: %s", result)
            except Exception:
                try:
                    rollback = getattr(getattr(self.service, "store", None), "rollback", None)
                    if callable(rollback):
                        rollback()
                except Exception:
                    logger.exception("Florence sync pass rollback failed")
                logger.exception("Florence sync pass failed")
            self._stop_event.wait(self.interval_seconds)

    def _run_automation_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                result = self.run_automation_once()
                if any(value for value in result.values()):
                    logger.info("Florence automation pass complete: %s", result)
                else:
                    logger.debug("Florence automation pass complete: %s", result)
            except Exception:
                try:
                    rollback = getattr(getattr(self.service, "store", None), "rollback", None)
                    if callable(rollback):
                        rollback()
                except Exception:
                    logger.exception("Florence automation pass rollback failed")
                logger.exception("Florence automation pass failed")
            self._stop_event.wait(self.automation_interval_seconds)
