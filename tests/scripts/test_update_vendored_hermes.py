import importlib.util
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "update_vendored_hermes.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("update_vendored_hermes_under_test", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout.strip()


def _commit_file(repo: Path, relative_path: str, text: str) -> str:
    path = repo / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    _git(repo, "add", relative_path)
    _git(repo, "commit", "-m", f"update {relative_path}")
    return _git(repo, "rev-parse", "HEAD")


def _build_local_upstream_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "local-hermes"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "checkout", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test User")
    commit = _commit_file(repo, "run_agent.py", "class AIAgent:\n    pass\n")
    _git(repo, "remote", "add", "upstream", str(repo))
    _git(repo, "update-ref", "refs/remotes/upstream/main", commit)
    return repo, commit


def test_update_vendored_hermes_refreshes_and_checks_local_upstream(tmp_path, monkeypatch):
    module = _load_script_module()
    repo, commit = _build_local_upstream_repo(tmp_path)
    vendor_dir = repo / "vendor" / "hermes-agent"
    monkeypatch.setattr(module, "REPO_ROOT", repo)

    source = module.refresh_vendored_hermes(
        remote="upstream",
        ref="main",
        fetch=False,
        vendor_dir=vendor_dir,
    )
    checked = module.check_vendored_hermes(
        remote="upstream",
        ref="main",
        fetch=False,
        vendor_dir=vendor_dir,
    )

    assert source.commit == commit
    assert checked == source
    assert (vendor_dir / "run_agent.py").read_text(encoding="utf-8") == "class AIAgent:\n    pass\n"
    assert (vendor_dir / ".florence-vendor-source").read_text(encoding="utf-8") == (
        f"remote=upstream\nref=main\ncommit={commit}\n"
    )


def test_update_vendored_hermes_check_fails_when_marker_is_stale(tmp_path, monkeypatch):
    module = _load_script_module()
    repo, _commit = _build_local_upstream_repo(tmp_path)
    vendor_dir = repo / "vendor" / "hermes-agent"
    monkeypatch.setattr(module, "REPO_ROOT", repo)
    module.refresh_vendored_hermes(
        remote="upstream",
        ref="main",
        fetch=False,
        vendor_dir=vendor_dir,
    )
    next_commit = _commit_file(repo, "run_agent.py", "class AIAgent:\n    version = 2\n")
    _git(repo, "update-ref", "refs/remotes/upstream/main", next_commit)

    with pytest.raises(RuntimeError, match="vendored_hermes_out_of_date"):
        module.check_vendored_hermes(
            remote="upstream",
            ref="main",
            fetch=False,
            vendor_dir=vendor_dir,
        )


def test_update_vendored_hermes_resolves_branch_names_with_slashes():
    module = _load_script_module()

    assert module.resolve_remote_ref("upstream", "feature/agent-loop") == "upstream/feature/agent-loop"
    assert module.resolve_remote_ref("upstream", "refs/tags/v1") == "refs/tags/v1"
    assert module.resolve_remote_ref("upstream", "upstream/main") == "upstream/main"
