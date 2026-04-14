import logging

import pytest

import florence.server as server_module
from florence.config import (
    FlorenceGoogleRuntimeConfig,
    FlorenceHermesRuntimeConfig,
    FlorenceLinqRuntimeConfig,
    FlorenceRedisRuntimeConfig,
    FlorenceServerRuntimeConfig,
    FlorenceSettings,
)
from florence.deploy_metadata import format_railway_deploy_metadata, get_railway_deploy_metadata
from florence.server import _run_hermes_preflight, _start_hermes_preflight


def _build_settings(tmp_path):
    return FlorenceSettings(
        server=FlorenceServerRuntimeConfig(
            host="127.0.0.1",
            port=8081,
            public_base_url="https://florence.example.com",
            sync_interval_seconds=300.0,
            db_path=tmp_path / "florence.db",
        ),
        google=FlorenceGoogleRuntimeConfig(
            client_id="google-client",
            client_secret="google-secret",
            redirect_uri="https://florence.example.com/v1/florence/google/callback",
            state_secret="state-secret",
        ),
        linq=FlorenceLinqRuntimeConfig(
            api_key="linq-api-key",
            webhook_secret="linq-webhook-secret",
        ),
        hermes=FlorenceHermesRuntimeConfig(
            model="openai/gpt-4o-mini",
            max_iterations=2,
            provider="custom",
        ),
        redis=FlorenceRedisRuntimeConfig(url=None),
    )


class _PassingAgent:
    def run_conversation(self, **kwargs):
        return {"final_response": "preflight_ok"}


class _FailingAgent:
    def __init__(self, exc: Exception):
        self.exc = exc

    def run_conversation(self, **kwargs):
        raise self.exc


def test_hermes_preflight_off_mode_skips_agent_startup(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    monkeypatch.setenv("FLORENCE_HERMES_PREFLIGHT", "off")
    called = {"value": False}

    def _factory(**kwargs):
        called["value"] = True
        return _PassingAgent()

    _run_hermes_preflight(settings, agent_factory=_factory)
    assert called["value"] is False


def test_hermes_preflight_warn_mode_logs_and_continues(tmp_path, monkeypatch, caplog):
    settings = _build_settings(tmp_path)
    monkeypatch.setenv("FLORENCE_HERMES_PREFLIGHT", "warn")
    caplog.set_level(logging.WARNING)

    _run_hermes_preflight(
        settings,
        agent_factory=lambda **kwargs: _FailingAgent(RuntimeError("boom")),
    )

    assert "Florence Hermes preflight failed" in caplog.text


def test_hermes_preflight_strict_mode_raises_with_reasoning_hint(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    monkeypatch.setenv("FLORENCE_HERMES_PREFLIGHT", "strict")

    with pytest.raises(RuntimeError, match="does not accept Hermes reasoning fields"):
        _run_hermes_preflight(
            settings,
            agent_factory=lambda **kwargs: _FailingAgent(RuntimeError("Unknown parameter: 'reasoning'.")),
        )


def test_hermes_preflight_strict_mode_passes_on_non_empty_reply(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    monkeypatch.setenv("FLORENCE_HERMES_PREFLIGHT", "strict")

    _run_hermes_preflight(
        settings,
        agent_factory=lambda **kwargs: _PassingAgent(),
    )


def test_start_hermes_preflight_runs_in_background_by_default(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    monkeypatch.delenv("FLORENCE_HERMES_PREFLIGHT_BLOCKING", raising=False)
    monkeypatch.setenv("FLORENCE_HERMES_PREFLIGHT", "warn")

    started = {"value": False}

    class _FakeThread:
        def __init__(self, *, target, name, daemon):
            assert callable(target)
            assert name == "florence-hermes-preflight"
            assert daemon is True
            self._target = target

        def start(self):
            started["value"] = True
            self._target()

    monkeypatch.setattr(server_module.threading, "Thread", _FakeThread)

    _start_hermes_preflight(
        settings,
        agent_factory=lambda **kwargs: _PassingAgent(),
    )

    assert started["value"] is True


def test_start_hermes_preflight_blocking_mode_runs_inline(tmp_path, monkeypatch):
    settings = _build_settings(tmp_path)
    monkeypatch.setenv("FLORENCE_HERMES_PREFLIGHT_BLOCKING", "true")

    calls = {"value": 0}

    def _factory(**kwargs):
        calls["value"] += 1
        return _PassingAgent()

    _start_hermes_preflight(settings, agent_factory=_factory)

    assert calls["value"] == 1


def test_get_railway_deploy_metadata_trims_commit_sha():
    metadata = get_railway_deploy_metadata(
        {
            "RAILWAY_PROJECT_ID": "proj_123",
            "RAILWAY_ENVIRONMENT_ID": "env_456",
            "RAILWAY_SERVICE_ID": "svc_789",
            "RAILWAY_DEPLOYMENT_ID": "dep_abc",
            "RAILWAY_GIT_BRANCH": "main",
            "RAILWAY_GIT_COMMIT_SHA": "213183a297d074e426756dce783a2eedd9b6557f",
            "RAILWAY_GIT_COMMIT_MESSAGE": "Slim Florence Docker builds",
        }
    )

    assert metadata == {
        "project_id": "proj_123",
        "environment_id": "env_456",
        "service_id": "svc_789",
        "deployment_id": "dep_abc",
        "replica_id": None,
        "branch": "main",
        "commit_sha": "213183a297d0",
        "commit_message": "Slim Florence Docker builds",
    }


def test_format_railway_deploy_metadata_returns_human_readable_summary():
    summary = format_railway_deploy_metadata(
        {
            "RAILWAY_SERVICE_ID": "svc_789",
            "RAILWAY_DEPLOYMENT_ID": "dep_abc",
            "RAILWAY_GIT_COMMIT_SHA": "213183a297d074e426756dce783a2eedd9b6557f",
            "RAILWAY_GIT_COMMIT_MESSAGE": "Slim Florence Docker builds",
        }
    )

    assert summary == (
        "service_id=svc_789, deployment_id=dep_abc, "
        "commit_sha=213183a297d0, commit_message=Slim Florence Docker builds"
    )


def test_format_railway_deploy_metadata_handles_missing_values():
    assert format_railway_deploy_metadata({}) == "Railway deployment metadata unavailable"
