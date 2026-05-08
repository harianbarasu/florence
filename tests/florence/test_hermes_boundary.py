import ast
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
FLORENCE_ROOT = REPO_ROOT / "florence"
VENDORED_HERMES = REPO_ROOT / "vendor" / "hermes-agent"
DISALLOWED_DIRECT_HERMES_IMPORTS = {
    "cli",
    "hermes_state",
    "model_tools",
    "run_agent",
    "toolsets",
}


def _python_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.py") if "__pycache__" not in path.parts)


def test_florence_product_code_uses_agent_boundary_for_hermes_imports():
    violations: list[str] = []
    for path in _python_files(FLORENCE_ROOT):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root_name = alias.name.split(".", 1)[0]
                    if root_name in DISALLOWED_DIRECT_HERMES_IMPORTS:
                        violations.append(f"{path.relative_to(REPO_ROOT)} imports {alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                root_name = node.module.split(".", 1)[0]
                if root_name in DISALLOWED_DIRECT_HERMES_IMPORTS:
                    violations.append(f"{path.relative_to(REPO_ROOT)} imports from {node.module}")

    assert violations == []


def test_importing_hermes_core_does_not_import_florence_product_code():
    code = """
import run_agent
import sys

loaded = sorted(
    name for name in sys.modules
    if name == "florence" or name.startswith("florence.")
)
assert not loaded, loaded[:20]
"""
    subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )


def test_importing_hermes_tool_orchestration_does_not_import_florence_product_code():
    code = """
import model_tools
import sys

loaded = sorted(
    name for name in sys.modules
    if name == "florence" or name.startswith("florence.")
)
assert not loaded, loaded[:20]
"""
    subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )


def test_vendored_hermes_runtime_switch_resolves_clean_snapshot():
    env = os.environ.copy()
    env["FLORENCE_USE_VENDORED_HERMES"] = "1"
    env["FLORENCE_HERMES_VENDOR_PATH"] = str(VENDORED_HERMES)
    code = """
import florence.agent
from florence.agent.hermes_runtime import import_hermes_module

run_agent = import_hermes_module("run_agent")
model_tools = import_hermes_module("model_tools")

tools = model_tools.get_tool_definitions(
    enabled_toolsets=["florence_chat"],
    disabled_toolsets=["delegation", "messaging", "clarify", "code_execution"],
    quiet_mode=True,
)
names = {tool["function"]["name"] for tool in tools}

assert "vendor/hermes-agent/run_agent.py" in run_agent.__file__, run_agent.__file__
assert "delegate_task" not in names
assert "send_message" not in names
assert "clarify" not in names
"""
    subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        text=True,
        capture_output=True,
    )
