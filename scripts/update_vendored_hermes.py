#!/usr/bin/env python3
"""Refresh Florence's vendored Hermes snapshot from an upstream git ref."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import NamedTuple


REPO_ROOT = Path(__file__).resolve().parents[1]
VENDOR_DIR = REPO_ROOT / "vendor" / "hermes-agent"
VENDOR_MARKER = ".florence-vendor-source"


class VendorSource(NamedTuple):
    remote: str
    ref: str
    commit: str


def run_git(args: list[str], *, capture: bool = False) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
    )
    return result.stdout.strip() if capture else ""


def resolve_remote_ref(remote: str, ref: str) -> str:
    if ref.startswith("refs/") or ref.startswith(f"{remote}/"):
        return ref
    return f"{remote}/{ref}"


def read_vendor_source(*, vendor_dir: Path = VENDOR_DIR) -> VendorSource | None:
    marker = vendor_dir / VENDOR_MARKER
    if not marker.exists():
        return None
    values: dict[str, str] = {}
    for line in marker.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key.strip()] = value.strip()
    remote = values.get("remote")
    ref = values.get("ref")
    commit = values.get("commit")
    if not remote or not ref or not commit:
        return None
    return VendorSource(remote=remote, ref=ref, commit=commit)


def write_vendor_source(
    *,
    vendor_dir: Path,
    remote: str,
    ref: str,
    commit: str,
) -> None:
    marker = vendor_dir / VENDOR_MARKER
    marker.write_text(
        f"remote={remote}\nref={ref}\ncommit={commit}\n",
        encoding="utf-8",
    )


def resolve_commit(*, remote: str, ref: str, fetch: bool) -> str:
    if fetch:
        run_git(["fetch", remote, ref])
    return run_git(["rev-parse", resolve_remote_ref(remote, ref)], capture=True)


def check_vendored_hermes(
    *,
    remote: str,
    ref: str,
    fetch: bool,
    vendor_dir: Path = VENDOR_DIR,
) -> VendorSource:
    expected_commit = resolve_commit(remote=remote, ref=ref, fetch=fetch)
    source = read_vendor_source(vendor_dir=vendor_dir)
    if source is None:
        raise RuntimeError(f"vendored_hermes_marker_missing:{vendor_dir / VENDOR_MARKER}")
    if not (vendor_dir / "run_agent.py").exists():
        raise RuntimeError(f"vendored_hermes_run_agent_missing:{vendor_dir / 'run_agent.py'}")
    expected = VendorSource(remote=remote, ref=ref, commit=expected_commit)
    if source != expected:
        raise RuntimeError(
            "vendored_hermes_out_of_date:"
            f" expected remote={expected.remote} ref={expected.ref} commit={expected.commit}"
            f" found remote={source.remote} ref={source.ref} commit={source.commit}"
        )
    return source


def refresh_vendored_hermes(
    *,
    remote: str,
    ref: str,
    fetch: bool,
    vendor_dir: Path = VENDOR_DIR,
) -> VendorSource:
    commit = resolve_commit(remote=remote, ref=ref, fetch=fetch)

    with tempfile.TemporaryDirectory() as tmp:
        archive_path = Path(tmp) / "hermes.tar"
        with archive_path.open("wb") as archive_file:
            subprocess.run(
                ["git", "archive", "--format=tar", commit],
                cwd=REPO_ROOT,
                check=True,
                stdout=archive_file,
            )

        if vendor_dir.exists():
            shutil.rmtree(vendor_dir)
        vendor_dir.mkdir(parents=True)

        with tarfile.open(archive_path) as archive:
            archive.extractall(vendor_dir)

    write_vendor_source(vendor_dir=vendor_dir, remote=remote, ref=ref, commit=commit)
    return VendorSource(remote=remote, ref=ref, commit=commit)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh vendor/hermes-agent from a clean upstream Hermes ref.",
    )
    parser.add_argument(
        "--remote",
        default="upstream",
        help="Git remote that points at NousResearch/hermes-agent.",
    )
    parser.add_argument(
        "--ref",
        default="main",
        help="Remote branch, tag, or ref to vendor. Defaults to main.",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Skip git fetch before exporting the requested ref.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify vendor/hermes-agent already matches the requested ref without rewriting it.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fetch = not args.no_fetch
    if args.check:
        source = check_vendored_hermes(remote=args.remote, ref=args.ref, fetch=fetch)
        print(f"Vendored Hermes is current: {source.commit}")
        return

    source = refresh_vendored_hermes(remote=args.remote, ref=args.ref, fetch=fetch)
    print(f"Vendored Hermes {source.commit} into {VENDOR_DIR.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
