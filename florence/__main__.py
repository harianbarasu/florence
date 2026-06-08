"""CLI helper for running Florence locally."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime

import uvicorn

from florence.smoke import run_local_pilot_smoke, run_staging_pilot_verification


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="florence")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("serve", help="Run the Florence web server.")
    smoke = subparsers.add_parser(
        "local-smoke",
        help="Run a local-only pilot rehearsal with fake Linq and fake connected sources.",
    )
    smoke.add_argument("--db-path", help="Optional SQLite path to keep the smoke database.")
    smoke.add_argument(
        "--now-utc",
        help="Optional ISO-8601 UTC timestamp for deterministic smoke output.",
    )
    staging = subparsers.add_parser(
        "staging-check",
        help="Verify a deployed staging household after live Linq/Google/Hermes smoke.",
    )
    staging.add_argument("--base-url", required=True, help="Base URL for the deployed Florence app.")
    staging.add_argument("--chat-id", required=True, help="Linq chat id for the staging household.")
    staging.add_argument(
        "--admin-key",
        default=os.getenv("FLORENCE_ADMIN_API_KEY"),
        help="Admin API key. Defaults to FLORENCE_ADMIN_API_KEY.",
    )
    staging.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="HTTP timeout per staging request.",
    )
    staging.add_argument(
        "--skip-hermes-smoke",
        action="store_true",
        help="Skip POST /dev/hermes-smoke; the result will remain not pilot-ready.",
    )
    args = parser.parse_args(argv)
    if args.command == "local-smoke":
        now = datetime.fromisoformat(args.now_utc.replace("Z", "+00:00")) if args.now_utc else None
        result = run_local_pilot_smoke(db_path=args.db_path, now_utc=now)
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        raise SystemExit(0 if result["ok"] else 1)
    if args.command == "staging-check":
        if not args.admin_key:
            parser.error("--admin-key is required when FLORENCE_ADMIN_API_KEY is unset")
        result = run_staging_pilot_verification(
            base_url=args.base_url,
            admin_key=args.admin_key,
            chat_id=args.chat_id,
            timeout_seconds=args.timeout_seconds,
            run_hermes_smoke=not args.skip_hermes_smoke,
        )
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        raise SystemExit(0 if result["ok"] else 1)
    uvicorn.run(
        "florence.app:create_app",
        factory=True,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
    )


if __name__ == "__main__":
    main()
