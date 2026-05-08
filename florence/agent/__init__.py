"""Florence agent-runtime integration."""

from florence.agent.hermes_runtime import (
    build_hermes_agent,
    build_hermes_session_db,
    ensure_hermes_import_path,
    hermes_vendor_path,
)

ensure_hermes_import_path()

from florence.agent.hermes_adapter import FlorenceHermesAdapter, FlorenceHermesRunResult
from florence.agent.tool_registry import register_florence_toolsets

register_florence_toolsets()

__all__ = [
    "FlorenceHermesAdapter",
    "FlorenceHermesRunResult",
    "build_hermes_agent",
    "build_hermes_session_db",
    "ensure_hermes_import_path",
    "hermes_vendor_path",
]
