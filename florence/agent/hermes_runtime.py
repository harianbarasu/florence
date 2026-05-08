"""Hermes runtime resolution for Florence.

Florence tracks Hermes as an external upstream engine. During the transition
we keep the historical root-level Hermes files available, but the product
runtime should resolve Hermes through this module so the eventual physical
split is one boundary instead of many scattered imports.
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import Any


def hermes_vendor_path() -> Path:
    configured = os.getenv("FLORENCE_HERMES_VENDOR_PATH", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parents[2] / "vendor" / "hermes-agent"


def use_vendored_hermes() -> bool:
    raw = os.getenv("FLORENCE_USE_VENDORED_HERMES", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def ensure_hermes_import_path() -> Path | None:
    """Put vendored Hermes on sys.path when explicitly enabled."""
    if not use_vendored_hermes():
        return None
    vendor_path = hermes_vendor_path()
    if not vendor_path.exists():
        raise RuntimeError(f"vendored_hermes_missing:{vendor_path}")
    vendor_str = str(vendor_path)
    if vendor_str not in sys.path:
        sys.path.insert(0, vendor_str)
    return vendor_path


def import_hermes_module(module_name: str):
    ensure_hermes_import_path()
    module = importlib.import_module(module_name)
    apply_hermes_compat_patches()
    return module


def apply_hermes_compat_patches() -> None:
    """Apply Florence's small local Hermes compatibility patch layer.

    The vendored tree stays a clean upstream snapshot. Runtime-only patches
    live here until upstream independently grows an equivalent extension point.
    """
    model_tools = sys.modules.get("model_tools")
    if model_tools is not None:
        _patch_model_tools_disabled_after_enabled(model_tools)


def _patch_model_tools_disabled_after_enabled(model_tools: Any) -> None:
    if getattr(model_tools, "_florence_disabled_after_enabled_patch", False):
        return
    original = getattr(model_tools, "get_tool_definitions", None)
    if not callable(original):
        return

    def get_tool_definitions(
        enabled_toolsets: list[str] | None = None,
        disabled_toolsets: list[str] | None = None,
        quiet_mode: bool = False,
    ) -> list[dict[str, Any]]:
        tools = original(
            enabled_toolsets=enabled_toolsets,
            disabled_toolsets=disabled_toolsets,
            quiet_mode=quiet_mode,
        )
        if enabled_toolsets is None or not disabled_toolsets:
            return tools
        disabled_tool_names: set[str] = set()
        for toolset_name in disabled_toolsets:
            if model_tools.validate_toolset(toolset_name):
                disabled_tool_names.update(model_tools.resolve_toolset(toolset_name))
            elif toolset_name in model_tools._LEGACY_TOOLSET_MAP:
                disabled_tool_names.update(model_tools._LEGACY_TOOLSET_MAP[toolset_name])
        if not disabled_tool_names:
            return tools
        return [
            tool
            for tool in tools
            if tool.get("function", {}).get("name") not in disabled_tool_names
        ]

    model_tools.get_tool_definitions = get_tool_definitions
    model_tools._florence_disabled_after_enabled_patch = True


def build_hermes_agent(**kwargs: Any):
    module = import_hermes_module("run_agent")
    return module.AIAgent(**kwargs)


def build_hermes_session_db() -> Any | None:
    try:
        module = import_hermes_module("hermes_state")
        return module.SessionDB()
    except Exception:
        return None


def create_hermes_custom_toolset(
    name: str,
    description: str,
    *,
    tools: list[str],
    category: str = "custom",
) -> None:
    module = import_hermes_module("toolsets")
    try:
        module.create_custom_toolset(name, description, tools=tools, category=category)
    except TypeError as exc:
        if "category" not in str(exc):
            raise
        module.create_custom_toolset(name, description, tools=tools)
