"""Standalone Florence background worker."""

from __future__ import annotations

import argparse
import logging
import time

from florence.config import FlorenceSettings
from florence.runtime.production import FlorenceProductionService
from florence.runtime.scheduler import FlorenceSyncScheduler

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Florence background worker")
    parser.add_argument("--once", action="store_true", help="Run a single sync pass and exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = FlorenceSettings.from_env()
    service = FlorenceProductionService(settings)

    try:
        if args.once:
            result = service.run_sync_pass()
            logger.info("Florence sync pass complete: %s", result)
            return

        scheduler = FlorenceSyncScheduler(
            service,
            interval_seconds=settings.server.sync_interval_seconds,
        )
        scheduler.start()
        logger.info("Florence worker started with interval %ss", settings.server.sync_interval_seconds)
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            logger.info("Shutting down Florence worker")
        finally:
            scheduler.stop()
    finally:
        service.close()


if __name__ == "__main__":
    main()
