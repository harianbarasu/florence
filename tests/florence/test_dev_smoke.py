import json
import subprocess
import sys


def _run_smoke(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "florence.dev_smoke", *args],
        check=True,
        capture_output=True,
        text=True,
    )


def test_dev_smoke_message_command_creates_household_and_returns_prompt(tmp_path):
    db_path = tmp_path / "florence-smoke.db"

    result = _run_smoke(
        "--db-path",
        str(db_path),
        "message",
        "--from",
        "+15555550123",
        "--thread",
        "dm-thread-1",
        "--text",
        "hi",
    )

    payload = json.loads(result.stdout)
    assert payload["consumed"] is True
    assert "Google" in payload["reply_text"]
    assert payload["household_id"] is not None


def test_dev_smoke_state_command_can_resolve_household_from_handle(tmp_path):
    db_path = tmp_path / "florence-smoke.db"
    _run_smoke(
        "--db-path",
        str(db_path),
        "message",
        "--from",
        "+15555550123",
        "--thread",
        "dm-thread-1",
        "--text",
        "hi",
    )

    result = _run_smoke(
        "--db-path",
        str(db_path),
        "state",
        "--for-handle",
        "+15555550123",
    )

    payload = json.loads(result.stdout)
    assert len(payload["households"]) == 1
    assert payload["households"][0]["members"][0]["identities"][0]["normalized_value"] == "+15555550123"
