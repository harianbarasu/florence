from pathlib import Path


def _dockerignore_patterns() -> set[str]:
    dockerignore = Path(__file__).resolve().parents[2] / ".dockerignore"
    return {
        line.strip()
        for line in dockerignore.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


def test_dockerignore_does_not_exclude_florence_runtime_package() -> None:
    patterns = _dockerignore_patterns()

    assert "runtime/" not in patterns
    assert "/runtime/" in patterns


def test_dockerignore_excludes_stale_setuptools_metadata() -> None:
    assert "*.egg-info" in _dockerignore_patterns()
