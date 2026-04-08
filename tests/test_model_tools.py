"""Tests for model_tools.py — function call dispatch, agent-loop interception, legacy toolsets."""

import json
import pytest
import tools.browser_tool as browser_tool_module

from model_tools import (
    handle_function_call,
    get_all_tool_names,
    get_tool_definitions,
    get_toolset_for_tool,
    _AGENT_LOOP_TOOLS,
    _LEGACY_TOOLSET_MAP,
    TOOL_TO_TOOLSET_MAP,
)


# =========================================================================
# handle_function_call
# =========================================================================

class TestHandleFunctionCall:
    def test_agent_loop_tool_returns_error(self):
        for tool_name in _AGENT_LOOP_TOOLS:
            result = json.loads(handle_function_call(tool_name, {}))
            assert "error" in result
            assert "agent loop" in result["error"].lower()

    def test_unknown_tool_returns_error(self):
        result = json.loads(handle_function_call("totally_fake_tool_xyz", {}))
        assert "error" in result
        assert "totally_fake_tool_xyz" in result["error"]

    def test_exception_returns_json_error(self):
        # Even if something goes wrong, should return valid JSON
        result = handle_function_call("web_search", None)  # None args may cause issues
        parsed = json.loads(result)
        assert isinstance(parsed, dict)
        assert "error" in parsed
        assert len(parsed["error"]) > 0
        assert "error" in parsed["error"].lower() or "failed" in parsed["error"].lower()


# =========================================================================
# Agent loop tools
# =========================================================================

class TestAgentLoopTools:
    def test_expected_tools_in_set(self):
        assert "todo" in _AGENT_LOOP_TOOLS
        assert "memory" in _AGENT_LOOP_TOOLS
        assert "session_search" in _AGENT_LOOP_TOOLS
        assert "delegate_task" in _AGENT_LOOP_TOOLS

    def test_no_regular_tools_in_set(self):
        assert "web_search" not in _AGENT_LOOP_TOOLS
        assert "terminal" not in _AGENT_LOOP_TOOLS


# =========================================================================
# Legacy toolset map
# =========================================================================

class TestLegacyToolsetMap:
    def test_expected_legacy_names(self):
        expected = [
            "web_tools", "terminal_tools", "vision_tools", "moa_tools",
            "image_tools", "skills_tools", "browser_tools", "cronjob_tools",
            "rl_tools", "file_tools", "tts_tools",
        ]
        for name in expected:
            assert name in _LEGACY_TOOLSET_MAP, f"Missing legacy toolset: {name}"

    def test_values_are_lists_of_strings(self):
        for name, tools in _LEGACY_TOOLSET_MAP.items():
            assert isinstance(tools, list), f"{name} is not a list"
            for tool in tools:
                assert isinstance(tool, str), f"{name} contains non-string: {tool}"


# =========================================================================
# Backward-compat wrappers
# =========================================================================

class TestBackwardCompat:
    def test_get_all_tool_names_returns_list(self):
        names = get_all_tool_names()
        assert isinstance(names, list)
        assert len(names) > 0
        # Should contain well-known tools
        assert "web_search" in names
        assert "terminal" in names

    def test_get_toolset_for_tool(self):
        result = get_toolset_for_tool("web_search")
        assert result is not None
        assert isinstance(result, str)

    def test_get_toolset_for_unknown_tool(self):
        result = get_toolset_for_tool("totally_nonexistent_tool")
        assert result is None

    def test_tool_to_toolset_map(self):
        assert isinstance(TOOL_TO_TOOLSET_MAP, dict)
        assert len(TOOL_TO_TOOLSET_MAP) > 0


class TestFlorenceToolDefinitions:
    def test_florence_chat_exposes_runtime_cronjob_tool(self, monkeypatch):
        monkeypatch.setenv("HERMES_EXEC_ASK", "1")
        tools = get_tool_definitions(enabled_toolsets=["florence_chat"], quiet_mode=True)
        names = {tool["function"]["name"] for tool in tools}
        assert "cronjob" in names
        assert "delegate_task" in names
        assert "schedule_cronjob" not in names
        assert "list_cronjobs" not in names
        assert "remove_cronjob" not in names

    def test_florence_chat_exposes_browser_and_research_tools_when_available(self, monkeypatch):
        monkeypatch.setenv("PARALLEL_API_KEY", "test-key")
        monkeypatch.setattr(browser_tool_module, "_find_agent_browser", lambda: "/tmp/agent-browser")
        monkeypatch.setattr(browser_tool_module, "_get_cloud_provider", lambda: None)
        tools = get_tool_definitions(enabled_toolsets=["florence_chat"], quiet_mode=True)
        names = {tool["function"]["name"] for tool in tools}
        assert {"web_search", "web_extract", "browser_navigate", "browser_snapshot", "browser_console"}.issubset(names)

    def test_disabled_toolsets_apply_after_enabled_toolsets(self, monkeypatch):
        monkeypatch.setenv("HERMES_EXEC_ASK", "1")
        tools = get_tool_definitions(
            enabled_toolsets=["florence_chat"],
            disabled_toolsets=["delegation", "messaging", "clarify", "code_execution"],
            quiet_mode=True,
        )
        names = {tool["function"]["name"] for tool in tools}
        assert "delegate_task" not in names
        assert "send_message" not in names
        assert "clarify" not in names
        assert "cronjob" in names
