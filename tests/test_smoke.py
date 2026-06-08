from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx
import pytest

import florence.__main__ as florence_cli
from florence.__main__ import main
from florence.smoke import run_local_pilot_smoke, run_staging_pilot_verification


SANITIZATION = {
    "message_bodies": "excluded",
    "source_bodies": "excluded",
    "source_titles": "excluded",
    "source_event_times": "presence_only",
    "oauth_tokens": "excluded",
    "memory_text": "excluded",
    "action_errors": "presence_only",
    "diagnostic_strings": "redacted",
}


def test_local_pilot_smoke_rehearses_household_source_approval_and_reminder(tmp_path):
    result = run_local_pilot_smoke(
        db_path=str(tmp_path / "local-smoke.sqlite"),
        now_utc=datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc),
    )
    steps = {step["id"]: step for step in result["steps"]}
    checklist = {
        step["id"]: step for step in result["pilot_check"]["smoke_checklist"]["steps"]
    }
    staging_checklist = {step["id"]: step for step in result["staging_verification_checklist"]}

    assert result["ok"] is True
    assert result["mode"] == "local_only"
    assert result["live_verification_performed"] is False
    assert result["pilot_check"]["pilot_ready"] is False
    assert result["remaining_live_verification"] == [
        "real Linq iMessage send/webhook round trip",
        "real Google OAuth connection and source sync",
        "real Hermes response through the deployed Florence adapter",
        "managed Postgres staging database",
    ]
    assert steps["connected_source_worker"]["ready"] is True
    assert steps["connected_source_worker"]["delivery_sent"] == 1
    assert steps["parent_approval"]["action_type"] == "create_reminder"
    assert steps["action_worker"]["result"] == {
        "ok": True,
        "attempted": 1,
        "succeeded": 1,
        "failed": 0,
    }
    assert steps["reminder_delivery"]["messages"][0]["text"] == (
        "Quick reminder: Field trip permission slip due"
    )
    assert steps["household_readiness"]["readiness"]["ready"] is True
    assert steps["pilot_proof_record"]["ready"] is True
    assert result["source_review"]["token_backed_google_total"] == 1
    assert result["source_review"]["token_backed_google_surfaced"] == 1
    assert result["pilot_proof"]["chat_id"] == result["chat_id"]
    assert result["pilot_proof"]["pilot_ready"] is False
    assert result["pilot_proof"]["generated_at_utc"] == "2026-06-05T16:00:00+00:00"
    assert result["pilot_proof"]["privacy"]["updated_at_utc"] == "2026-06-05T16:00:00Z"
    assert result["pilot_proof"]["pilot_check"]["message_transport"][
        "latest_outbound_at_utc"
    ] == "2026-06-06T00:00:00+00:00"
    assert result["pilot_proof"]["sanitization"] == SANITIZATION
    proof_source_item = result["pilot_proof"]["source_review"]["recent_surfaced"][0]
    assert set(proof_source_item) == {
        "id",
        "source_type",
        "reason",
        "priority",
        "event_at_present",
    }
    assert proof_source_item["source_type"] == "email"
    assert proof_source_item["event_at_present"] is True
    assert result["pilot_proof"]["action_executions"][0]["status"] == "success"
    assert result["pilot_proof"]["action_executions"][0]["error_present"] is False
    assert "error" not in result["pilot_proof"]["action_executions"][0]
    assert checklist["two_parent_household_setup"]["ready"] is True
    assert checklist["linq_message_transport"]["ready"] is True
    assert checklist["connected_source_account"]["ready"] is True
    assert checklist["source_rule_and_need_to_know"]["ready"] is True
    assert checklist["outbound_delivery_queue"]["ready"] is True
    assert checklist["approval_worker_queue"]["ready"] is True
    assert "deployment_preflight" in result["pilot_check"]["smoke_checklist"]["blocked"]
    assert "real Linq iMessage send/webhook round trip" in result["remaining_live_verification"]
    assert staging_checklist["configuration_preflight"]["proof"] == "GET /dev/deployment-check"
    assert staging_checklist["configuration_preflight"]["expect"] == [
        "deployment.missing_required == []",
        "deployment.invalid == []",
        "deployment.live_verification.external_credentials_needed == []",
    ]
    assert "FLORENCE_ADMIN_API_KEY" in staging_checklist["configuration_preflight"]["runtime_env"]
    assert "FLORENCE_SOURCE_INGEST_API_KEY" in (
        staging_checklist["configuration_preflight"]["runtime_env"]
    )
    assert "FLORENCE_TOKEN_ENCRYPTION_KEY" in (
        staging_checklist["configuration_preflight"]["runtime_env"]
    )
    assert staging_checklist["configuration_preflight"]["live_verification_records"] == [
        "live_verifications.linq",
        "live_verifications.google",
        "live_verifications.hermes",
    ]
    assert staging_checklist["configuration_preflight"]["fallback_env"] == [
        "FLORENCE_LINQ_LIVE_VERIFIED",
        "FLORENCE_LINQ_LIVE_VERIFIED_AT",
        "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF",
        "FLORENCE_GOOGLE_LIVE_VERIFIED",
        "FLORENCE_GOOGLE_LIVE_VERIFIED_AT",
        "FLORENCE_GOOGLE_LIVE_VERIFICATION_PROOF",
        "FLORENCE_HERMES_LIVE_VERIFIED",
        "FLORENCE_HERMES_LIVE_VERIFIED_AT",
        "FLORENCE_HERMES_LIVE_VERIFICATION_PROOF",
    ]
    assert staging_checklist["managed_postgres"]["proof"] == "GET /dev/deployment-check"
    assert "FLORENCE_DATABASE_URL" in staging_checklist["managed_postgres"]["runtime_env"]
    assert staging_checklist["linq_live_round_trip"]["proof"] == (
        "GET /dev/pilot-check/{staging_chat_id}"
    )
    assert staging_checklist["linq_live_round_trip"]["local_rehearsal_chat_id"] == (
        result["chat_id"]
    )
    assert staging_checklist["linq_live_round_trip"]["staging_chat_id_placeholder"] == (
        "{staging_chat_id}"
    )
    assert "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF" not in (
        staging_checklist["linq_live_round_trip"]["runtime_env"]
    )
    assert staging_checklist["linq_live_round_trip"]["credential_env"] == [
        "LINQ_WEBHOOK_SECRET",
        "LINQ_API_KEY",
        "LINQ_FROM_PHONE",
    ]
    assert staging_checklist["linq_live_round_trip"]["proof_record"] == (
        "live_verifications.linq"
    )
    assert staging_checklist["linq_live_round_trip"]["automatic_proof_source"] == "linq_webhook"
    assert "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF" not in (
        staging_checklist["linq_live_round_trip"]["runtime_env"]
    )
    assert staging_checklist["linq_live_round_trip"]["fallback_env"] == [
        "FLORENCE_LINQ_LIVE_VERIFIED",
        "FLORENCE_LINQ_LIVE_VERIFIED_AT",
        "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF",
    ]
    assert staging_checklist["google_oauth_source_sync"]["expect"] == [
        "connected_accounts.token_backed_google > 0",
        "source_review.token_backed_google_surfaced > 0",
        "smoke_checklist.steps.google_live_source_sync.ready == true",
        "deployment.live_verification.evidence.google.verified == true",
    ]
    assert "FLORENCE_TOKEN_ENCRYPTION_KEY" in (
        staging_checklist["google_oauth_source_sync"]["runtime_env"]
    )
    assert staging_checklist["google_oauth_source_sync"]["credential_env"] == [
        "GOOGLE_CLIENT_ID",
        "GOOGLE_CLIENT_SECRET",
        "GOOGLE_REDIRECT_URI",
        "FLORENCE_TOKEN_ENCRYPTION_KEY",
    ]
    assert staging_checklist["google_oauth_source_sync"]["proof_record"] == (
        "live_verifications.google"
    )
    assert staging_checklist["google_oauth_source_sync"]["automatic_proof_source"] == (
        "source_sync_worker"
    )
    assert staging_checklist["google_oauth_source_sync"]["fallback_env"] == [
        "FLORENCE_GOOGLE_LIVE_VERIFIED",
        "FLORENCE_GOOGLE_LIVE_VERIFIED_AT",
        "FLORENCE_GOOGLE_LIVE_VERIFICATION_PROOF",
    ]
    assert staging_checklist["hermes_live_response"]["proof"] == (
        "POST /dev/hermes-smoke/{staging_chat_id}"
    )
    assert staging_checklist["hermes_live_response"]["expect"] == [
        "live_hermes_verified == true",
        "response == null",
        "response_present == true",
        "sanitization.response == excluded",
        "used_fallback == false",
        "stored_live_verification.name == hermes",
        "hermes.database_backend == postgres",
        "hermes.ready_for_saas_pilot == true",
        "hermes.strict_mode == true",
        "hermes.turn_runtime_concurrency == serialized_by_thread_and_file_lock",
        "hermes.runtime_lock == thread_lock_plus_interprocess_file_lock",
        "hermes.turn_failure_cleanup == runtime_home_restored_and_checkout_modules_cleared_on_error",
    ]
    assert staging_checklist["hermes_live_response"]["build_args"] == [
        "INSTALL_HERMES_AGENT",
        "HERMES_AGENT_REF",
        "HERMES_AGENT_REPO",
    ]
    assert "FLORENCE_HERMES_AGENT_PATH" in (
        staging_checklist["hermes_live_response"]["runtime_env"]
    )
    assert "HERMES_AGENT_REF" in staging_checklist["hermes_live_response"]["runtime_env"]
    assert "FLORENCE_HERMES_RUNTIME_HOME" in (
        staging_checklist["hermes_live_response"]["runtime_env"]
    )
    assert staging_checklist["hermes_live_response"]["credential_env"] == [
        "FLORENCE_HERMES_AGENT_PATH",
        "HERMES_AGENT_REF",
        "FLORENCE_HERMES_PROVIDER",
        "FLORENCE_HERMES_MODEL",
        "FLORENCE_HERMES_STRICT",
    ]
    assert staging_checklist["hermes_live_response"]["proof_record"] == (
        "live_verifications.hermes"
    )
    assert staging_checklist["hermes_live_response"]["automatic_proof_source"] == "hermes_smoke"
    assert staging_checklist["hermes_live_response"]["fallback_env"] == [
        "FLORENCE_HERMES_LIVE_VERIFIED",
        "FLORENCE_HERMES_LIVE_VERIFIED_AT",
        "FLORENCE_HERMES_LIVE_VERIFICATION_PROOF",
    ]
    assert staging_checklist["deployment_ready"]["proof"] == "GET /dev/deployment-check"
    assert staging_checklist["deployment_ready"]["expect"] == [
        "deployment.ready == true",
        "deployment.live_verification.ready == true",
        "deployment.live_verification.blocked == []",
    ]
    assert staging_checklist["deployment_ready"]["live_verification_records"] == (
        staging_checklist["configuration_preflight"]["live_verification_records"]
    )
    assert staging_checklist["deployment_ready"]["fallback_env"] == (
        staging_checklist["configuration_preflight"]["fallback_env"]
    )
    assert staging_checklist["pilot_ready_proof_record"]["expect"] == [
        "proof.pilot_ready == true",
        "proof.sanitization.message_bodies == excluded",
        "proof.sanitization.source_bodies == excluded",
        "proof.sanitization.source_titles == excluded",
        "proof.sanitization.source_event_times == presence_only",
        "proof.sanitization.oauth_tokens == excluded",
        "proof.sanitization.memory_text == excluded",
        "proof.sanitization.action_errors == presence_only",
        "proof.sanitization.diagnostic_strings == redacted",
    ]
    assert staging_checklist["pilot_ready_proof_record"]["proof"] == (
        "GET /dev/pilot-proof/{staging_chat_id}"
    )
    assert "Please sign and bring the permission slip" not in json.dumps(result)
    assert "local-smoke-access-token" not in json.dumps(result)
    assert "refresh_token" not in json.dumps(result)


def test_local_smoke_cli_prints_json_and_exits_success(tmp_path, capsys):
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "local-smoke",
                "--db-path",
                str(tmp_path / "cli-local-smoke.sqlite"),
                "--now-utc",
                "2026-06-05T16:00:00+00:00",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["ok"] is True
    assert payload["mode"] == "local_only"
    assert payload["pilot_check"]["pilot_ready"] is False
    assert payload["pilot_proof"]["generated_at_utc"] == "2026-06-05T16:00:00+00:00"
    assert payload["pilot_proof"]["sanitization"]["oauth_tokens"] == "excluded"
    assert payload["staging_verification_checklist"][0]["id"] == "configuration_preflight"


def test_staging_pilot_verification_summarizes_live_endpoints_without_raw_source_content():
    requests: list[httpx.Request] = []
    hermes_status = {
        "mode": "configured_checkout",
        "contract_ok": True,
        "ready_for_saas_pilot": True,
        "database_backend": "postgres",
        "strict_mode": True,
        "pinned_ref": True,
        "hermes_ref_matches": True,
        "toolsets_disabled": True,
        "memory_owner": "florence",
        "session_scope": "ephemeral_per_turn",
        "durable_hermes_memory": "disabled",
        "turn_runtime_cleanup": "enabled",
        "turn_failure_cleanup": "runtime_home_restored_and_checkout_modules_cleared_on_error",
        "turn_runtime_concurrency": "serialized_by_thread_and_file_lock",
        "runtime_lock": "thread_lock_plus_interprocess_file_lock",
        "python_path_scope": "temporary_during_hermes_call",
        "module_cache_scope": "shadowed_and_cleared_during_hermes_import_or_call",
        "invalid": [],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        path = request.url.path
        if path == "/health":
            assert "x-florence-admin-key" not in request.headers
            return httpx.Response(200, json={"ok": True})
        assert request.headers["x-florence-admin-key"] == "admin-key"
        if path == "/dev/deployment-check":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "deployment": {
                        "ready": True,
                        "missing_required": [],
                        "invalid": [],
                        "database": {
                            "configured_backend": "postgres",
                            "store_backend": "postgres",
                            "backend_matches": True,
                            "reachable": True,
                            "schema_ready": True,
                        },
                        "live_verification": {
                            "ready": True,
                            "external_credentials_needed": [],
                            "unverified": [],
                            "evidence_gaps": [],
                            "blocked": [],
                            "verified": {"linq": True, "google": True, "hermes": True},
                        },
                        "operator_next_steps": [],
                    },
                },
            )
        if path == "/dev/hermes-status":
            return httpx.Response(200, json={"ok": True, "hermes": hermes_status})
        if path == "/dev/hermes-smoke/staging-chat":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "live_hermes_verified": True,
                    "response": None,
                    "response_present": True,
                    "response_chars": 17,
                    "used_fallback": False,
                    "sanitization": {"response": "excluded"},
                    "stored_live_verification": {
                        "name": "hermes",
                        "verified_at_utc": "2026-06-05T16:00:00+00:00",
                        "proof": "Hermes smoke endpoint returned live_hermes_verified true without fallback",
                        "source": "hermes_smoke",
                        "updated_at_utc": "2026-06-05T16:00:00+00:00",
                    },
                    "error": None,
                    "hermes": hermes_status,
                },
            )
        if path == "/dev/pilot-check/staging-chat":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "pilot_ready": True,
                    "household": {"ready": True},
                    "message_transport": {"ready": True},
                    "connected_accounts": {"token_backed_google": 1},
                    "source_review": {
                        "total": 1,
                        "surfaced": 1,
                        "token_backed_google_total": 1,
                        "token_backed_google_surfaced": 1,
                        "latest_token_backed_google_synced_at_utc": (
                            "2026-06-05T16:00:00+00:00"
                        ),
                        "recent_surfaced": [
                            {
                                "id": "source-1",
                                "title": "Secret permission slip title",
                                "source_type": "email",
                            }
                        ],
                    },
                    "delivery": {"ready": True},
                    "actions": {"ready": True},
                    "smoke_checklist": {
                        "ready": True,
                        "blocked": [],
                        "steps": [{"id": "linq_live_round_trip", "ready": True}],
                    },
                    "operator_next_steps": [],
                },
            )
        if path == "/dev/pilot-proof/staging-chat":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "proof": {
                        "generated_at_utc": "2026-06-05T16:00:00+00:00",
                        "chat_id": "staging-chat",
                        "pilot_ready": True,
                        "sanitization": SANITIZATION,
                    },
                },
            )
        return httpx.Response(404, json={"detail": "not found"})

    client = httpx.Client(
        base_url="https://florence.example.test",
        transport=httpx.MockTransport(handler),
    )

    result = run_staging_pilot_verification(
        base_url="https://florence.example.test",
        admin_key="admin-key",
        chat_id="staging-chat",
        http_client=client,
    )

    assert result["ok"] is True
    assert result["mode"] == "staging_live_verification"
    assert result["live_verification_performed"] is True
    assert result["health"] == {"ok": True}
    assert result["remaining"] == []
    assert result["next_actions"] == []
    assert [request.url.path for request in requests] == [
        "/health",
        "/dev/hermes-status",
        "/dev/hermes-smoke/staging-chat",
        "/dev/deployment-check",
        "/dev/pilot-check/staging-chat",
        "/dev/pilot-proof/staging-chat",
    ]
    assert {check["id"]: check["ready"] for check in result["checks"]} == {
        "health": True,
        "deployment_check": True,
        "hermes_status": True,
        "hermes_live_response": True,
        "pilot_check": True,
        "pilot_ready_proof_record": True,
    }
    assert result["hermes_smoke"]["response_present"] is True
    assert result["hermes_smoke"]["response_chars"] == 17
    assert result["hermes_smoke"]["sanitization"] == {"response": "excluded"}
    assert result["hermes_smoke"]["stored_live_verification"]["source"] == "hermes_smoke"
    assert result["pilot_check"]["source_review"]["token_backed_google_surfaced"] == 1
    assert "Secret permission slip title" not in json.dumps(result)


def test_staging_pilot_verification_rejects_unsanitized_hermes_smoke_response():
    hermes_status = {
        "mode": "configured_checkout",
        "contract_ok": True,
        "ready_for_saas_pilot": True,
        "database_backend": "postgres",
        "strict_mode": True,
        "pinned_ref": True,
        "hermes_ref_matches": True,
        "toolsets_disabled": True,
        "memory_owner": "florence",
        "session_scope": "ephemeral_per_turn",
        "durable_hermes_memory": "disabled",
        "turn_runtime_cleanup": "enabled",
        "turn_failure_cleanup": (
            "runtime_home_restored_and_checkout_modules_cleared_on_error"
        ),
        "turn_runtime_concurrency": "serialized_by_thread_and_file_lock",
        "runtime_lock": "thread_lock_plus_interprocess_file_lock",
        "python_path_scope": "temporary_during_hermes_call",
        "module_cache_scope": "shadowed_and_cleared_during_hermes_import_or_call",
        "invalid": [],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/health":
            return httpx.Response(200, json={"ok": True})
        if path == "/dev/deployment-check":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "deployment": {
                        "ready": True,
                        "missing_required": [],
                        "invalid": [],
                        "database": {
                            "configured_backend": "postgres",
                            "store_backend": "postgres",
                            "backend_matches": True,
                            "reachable": True,
                            "schema_ready": True,
                        },
                        "live_verification": {
                            "ready": True,
                            "external_credentials_needed": [],
                            "unverified": [],
                            "evidence_gaps": [],
                            "blocked": [],
                            "verified": {"linq": True, "google": True, "hermes": True},
                        },
                        "operator_next_steps": [],
                    },
                },
            )
        if path == "/dev/hermes-status":
            return httpx.Response(200, json={"ok": True, "hermes": hermes_status})
        if path == "/dev/hermes-smoke/staging-chat":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "live_hermes_verified": True,
                    "response": "Private family answer about Maya",
                    "response_present": True,
                    "response_chars": 32,
                    "used_fallback": False,
                    "sanitization": {"response": "included"},
                    "stored_live_verification": {
                        "name": "hermes",
                        "verified_at_utc": "2026-06-05T16:00:00+00:00",
                        "proof": "Hermes smoke endpoint returned live_hermes_verified true without fallback",
                        "source": "hermes_smoke",
                        "updated_at_utc": "2026-06-05T16:00:00+00:00",
                    },
                    "error": None,
                    "hermes": hermes_status,
                },
            )
        if path == "/dev/pilot-check/staging-chat":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "pilot_ready": True,
                    "household": {"ready": True},
                    "message_transport": {"ready": True},
                    "connected_accounts": {"token_backed_google": 1},
                    "source_review": {
                        "total": 1,
                        "surfaced": 1,
                        "token_backed_google_total": 1,
                        "token_backed_google_surfaced": 1,
                    },
                    "delivery": {"ready": True},
                    "actions": {"ready": True},
                    "smoke_checklist": {"ready": True, "blocked": [], "steps": []},
                    "operator_next_steps": [],
                },
            )
        if path == "/dev/pilot-proof/staging-chat":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "proof": {
                        "generated_at_utc": "2026-06-05T16:00:00+00:00",
                        "chat_id": "staging-chat",
                        "pilot_ready": True,
                        "sanitization": SANITIZATION,
                    },
                },
            )
        return httpx.Response(404, json={"detail": "not found"})

    client = httpx.Client(
        base_url="https://florence.example.test",
        transport=httpx.MockTransport(handler),
    )

    result = run_staging_pilot_verification(
        base_url="https://florence.example.test",
        admin_key="admin-key",
        chat_id="staging-chat",
        http_client=client,
    )
    checks = {check["id"]: check for check in result["checks"]}
    blob = json.dumps(result)

    assert result["ok"] is False
    assert checks["hermes_live_response"]["ready"] is False
    assert "hermes_live_response" in result["remaining"]
    assert checks["hermes_live_response"]["blocked_by"] == [
        "Hermes smoke response body must be excluded from operator output",
        "Hermes smoke sanitization must mark response as excluded",
    ]
    assert "Private family answer about Maya" not in blob
    assert result["hermes_smoke"]["sanitization"] == {"response": "included"}


def test_staging_pilot_verification_reports_next_actions_when_live_proof_is_missing():
    hermes_status = {
        "mode": "configured_checkout",
        "contract_ok": True,
        "ready_for_saas_pilot": True,
        "database_backend": "postgres",
        "strict_mode": True,
        "pinned_ref": True,
        "hermes_ref_matches": True,
        "toolsets_disabled": True,
        "memory_owner": "florence",
        "session_scope": "ephemeral_per_turn",
        "durable_hermes_memory": "disabled",
        "invalid": [],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/health":
            return httpx.Response(200, json={"ok": True})
        if path == "/dev/deployment-check":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "deployment": {
                        "ready": False,
                        "missing_required": [],
                        "invalid": [],
                        "database": {
                            "configured_backend": "postgres",
                            "store_backend": "postgres",
                            "backend_matches": True,
                            "reachable": True,
                            "schema_ready": True,
                        },
                        "live_verification": {
                            "ready": False,
                            "external_credentials_needed": [],
                            "unverified": ["Live Linq iMessage send and webhook round-trip"],
                            "evidence_gaps": ["FLORENCE_LINQ_LIVE_VERIFIED_AT"],
                            "blocked": [
                                "Live Linq/Google/Hermes smoke checks not marked verified",
                                "Live verification proof metadata required",
                            ],
                            "verified": {"linq": False, "google": True, "hermes": True},
                        },
                        "operator_next_steps": [
                            {
                                "id": "live_linq",
                                "status": "needs_live_smoke",
                                "missing_env": [],
                                "missing_proof": ["FLORENCE_LINQ_LIVE_VERIFIED_AT"],
                                "required_env": [
                                    "LINQ_WEBHOOK_SECRET",
                                    "LINQ_API_KEY",
                                    "LINQ_FROM_PHONE",
                                ],
                                "proof_env": [
                                    "FLORENCE_LINQ_LIVE_VERIFIED_AT",
                                    "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF",
                                ],
                                "blocked_by": [],
                                "action": "Run a real Linq iMessage send/webhook round trip.",
                            }
                        ],
                    },
                },
            )
        if path == "/dev/hermes-status":
            return httpx.Response(200, json={"ok": True, "hermes": hermes_status})
        if path == "/dev/hermes-smoke/staging-chat":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "live_hermes_verified": True,
                    "response": None,
                    "response_present": True,
                    "response_chars": 17,
                    "used_fallback": False,
                    "sanitization": {"response": "excluded"},
                    "stored_live_verification": {
                        "name": "hermes",
                        "verified_at_utc": "2026-06-05T16:00:00+00:00",
                        "proof": "Hermes smoke endpoint returned live_hermes_verified true without fallback",
                        "source": "hermes_smoke",
                        "updated_at_utc": "2026-06-05T16:00:00+00:00",
                    },
                    "error": None,
                    "hermes": hermes_status,
                },
            )
        if path == "/dev/pilot-check/staging-chat":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "pilot_ready": False,
                    "household": {"ready": True},
                    "message_transport": {"ready": False},
                    "connected_accounts": {"token_backed_google": 1},
                    "source_review": {
                        "total": 1,
                        "surfaced": 1,
                        "token_backed_google_total": 1,
                        "token_backed_google_surfaced": 1,
                        "recent_surfaced": [{"title": "Sensitive live source title"}],
                    },
                    "delivery": {"ready": True},
                    "actions": {"ready": True},
                    "smoke_checklist": {
                        "ready": False,
                        "blocked": ["deployment_preflight", "linq_live_round_trip"],
                        "steps": [
                            {
                                "id": "linq_live_round_trip",
                                "ready": False,
                                "blocked_by": [
                                    "Live Linq iMessage send and webhook round-trip"
                                ],
                            }
                        ],
                    },
                    "operator_next_steps": [
                        {
                            "id": "linq_live_round_trip",
                            "status": "pilot_smoke_blocked",
                            "blocked_by": ["Live Linq iMessage send and webhook round-trip"],
                            "action": "A real Linq iMessage send/webhook round trip is verified.",
                        }
                    ],
                },
            )
        if path == "/dev/pilot-proof/staging-chat":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "proof": {
                        "generated_at_utc": "2026-06-05T16:00:00+00:00",
                        "chat_id": "staging-chat",
                        "pilot_ready": False,
                        "sanitization": SANITIZATION,
                    },
                },
            )
        return httpx.Response(404, json={"detail": "not found"})

    client = httpx.Client(
        base_url="https://florence.example.test",
        transport=httpx.MockTransport(handler),
    )

    result = run_staging_pilot_verification(
        base_url="https://florence.example.test",
        admin_key="admin-key",
        chat_id="staging-chat",
        http_client=client,
    )
    action_sources = {(action["source"], action["id"]) for action in result["next_actions"]}

    assert result["ok"] is False
    assert "deployment_check" in result["remaining"]
    assert "Live Linq/Google/Hermes smoke checks not marked verified" in result["remaining"]
    assert ("staging_check", "deployment_check") in action_sources
    assert ("deployment", "live_linq") in action_sources
    assert ("pilot_check", "linq_live_round_trip") in action_sources
    assert result["next_actions"][0]["status"] == "not_ready"
    assert any(
        action.get("missing_proof") == ["FLORENCE_LINQ_LIVE_VERIFIED_AT"]
        for action in result["next_actions"]
    )
    assert "Sensitive live source title" not in json.dumps(result)


def test_staging_smoke_cli_prints_json_and_uses_env_admin_key(monkeypatch, capsys):
    calls = []

    def fake_staging_check(**kwargs):
        calls.append(kwargs)
        return {
            "ok": True,
            "mode": "staging_live_verification",
            "checks": [],
            "remaining": [],
        }

    monkeypatch.setenv("FLORENCE_ADMIN_API_KEY", "env-admin-key")
    monkeypatch.setattr(florence_cli, "run_staging_pilot_verification", fake_staging_check)

    with pytest.raises(SystemExit) as exc:
        main(
            [
                "staging-check",
                "--base-url",
                "https://florence.example.test",
                "--chat-id",
                "staging-chat",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["ok"] is True
    assert payload["mode"] == "staging_live_verification"
    assert calls == [
        {
            "base_url": "https://florence.example.test",
            "admin_key": "env-admin-key",
            "chat_id": "staging-chat",
            "timeout_seconds": 30.0,
            "run_hermes_smoke": True,
        }
    ]
