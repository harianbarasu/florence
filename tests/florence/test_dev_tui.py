import subprocess
import sys


def test_dev_tui_can_bootstrap_and_exit(tmp_path):
    db_path = tmp_path / "florence-tui.db"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "florence.dev_tui",
            "--db-path",
            str(db_path),
            "--parent-name",
            "Maya",
        ],
        input="/quit\n",
        capture_output=True,
        text=True,
        check=True,
    )

    assert "Florence>" in result.stdout
    assert "active scope" in result.stdout
