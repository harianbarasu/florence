"""Entry point: a single process serves HTTP and runs the background loops."""

from __future__ import annotations

import os

import uvicorn


def main() -> None:
    uvicorn.run(
        "florence.app:create_app",
        factory=True,
        host="0.0.0.0",  # noqa: S104 - container entrypoint
        port=int(os.getenv("PORT", "8000")),
        workers=1,  # the in-process scheduler must be a singleton
    )


if __name__ == "__main__":
    main()
