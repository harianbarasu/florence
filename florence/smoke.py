"""Local operator smoke path for Florence."""

from __future__ import annotations

import hashlib
import hmac
import json
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi.testclient import TestClient

from florence.app import create_app
from florence.config import Settings
from florence.oauth import TokenVault
from florence.service import FlorenceService
from florence.source_providers import ProviderBatch
from florence.store import Store
from florence.worker import run_source_sync_tick


class LocalSmokeLinqClient:
    def __init__(self) -> None:
        self.sent: list[dict[str, str]] = []
        self.created: list[dict[str, object]] = []

    def send_text(self, *, chat_id: str, text: str, idempotency_key: str) -> dict[str, object]:
        self.sent.append(
            {
                "chat_id": chat_id,
                "text": text,
                "idempotency_key": idempotency_key,
            }
        )
        return {"ok": True}

    def create_chat(
        self,
        *,
        from_phone: str,
        to: tuple[str, ...],
        text: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        self.created.append(
            {
                "from_phone": from_phone,
                "to": list(to),
                "text": text,
                "idempotency_key": idempotency_key,
            }
        )
        return {"chat": {"id": "local-smoke-chat"}}


class LocalSmokeConnectedSourceProvider:
    provider = "google"

    def __init__(self, batch: ProviderBatch) -> None:
        self.batch = batch
        self.seen_accounts: list[object] = []

    def fetch(self, account: object, *, now_utc: datetime) -> ProviderBatch:
        self.seen_accounts.append(account)
        return self.batch


def run_local_pilot_smoke(
    *,
    db_path: str | None = None,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Run a local-only household smoke without live external credentials."""

    now = (now_utc or datetime.now(timezone.utc)).replace(microsecond=0)
    due_at = now + timedelta(hours=8)
    chat_id = f"local-smoke-{uuid.uuid4().hex[:8]}"
    secret = "local-smoke-linq-secret"
    admin_key = "local-smoke-admin-key"
    cleanup_dir: tempfile.TemporaryDirectory[str] | None = None
    if db_path is None:
        cleanup_dir = tempfile.TemporaryDirectory(prefix="florence-local-smoke-")
        db_path = str(Path(cleanup_dir.name) / "florence.sqlite")
    settings = Settings(
        db_path=db_path,
        admin_api_key=admin_key,
        linq_webhook_secret=secret,
        token_encryption_key=TokenVault.generate_key(),
    )
    store = Store(settings.db_path)
    service = FlorenceService(settings=settings, store=store)
    linq = LocalSmokeLinqClient()
    client = TestClient(create_app(settings, store=store, linq_client=linq, now_fn=lambda: now))
    admin_headers = {"x-florence-admin-key": admin_key}

    try:
        responses = {
            "parent_one_name": _post_signed_linq(
                client,
                secret=secret,
                chat_id=chat_id,
                message_id="local-smoke-parent-one-name",
                text="my name is Sam",
                sent_at=now,
            ),
            "confirm_partner": _post_signed_linq(
                client,
                secret=secret,
                chat_id=chat_id,
                message_id="local-smoke-confirm-partner",
                text="confirm partner +15555550101",
                sent_at=now + timedelta(seconds=1),
            ),
            "parent_two_name": _post_signed_linq(
                client,
                secret=secret,
                chat_id=chat_id,
                message_id="local-smoke-parent-two-name",
                sender="+15555550101",
                text="my name is Alex",
                sent_at=now + timedelta(seconds=2),
            ),
            "child": _post_signed_linq(
                client,
                secret=secret,
                chat_id=chat_id,
                message_id="local-smoke-child",
                text="our child is Maya",
                sent_at=now + timedelta(seconds=3),
            ),
            "source_rule": _post_signed_linq(
                client,
                secret=secret,
                chat_id=chat_id,
                message_id="local-smoke-source-rule",
                text="always tell me about permission slips",
                sent_at=now + timedelta(seconds=4),
            ),
        }
        service.sync_connected_sources(
            chat_id=chat_id,
            provider="google",
            external_account_id="local-google-parent@example.com",
            account_label="Local Google smoke",
            cursor="local-smoke-cursor-1",
            now_utc=now + timedelta(seconds=5),
        )
        _attach_local_google_token(store=store, settings=settings, chat_id=chat_id, now=now)
        provider = LocalSmokeConnectedSourceProvider(
            ProviderBatch(
                emails=[
                    {
                        "external_id": "local-smoke-permission-slip-1",
                        "subject": "Field trip permission slip due",
                        "body": "Please sign and bring the permission slip for tomorrow's field trip.",
                        "sender": "teacher@example.edu",
                        "received_at_utc": (now + timedelta(minutes=5)).isoformat(),
                        "event_at_utc": due_at.isoformat(),
                    }
                ],
                calendar_events=[],
                cursor="local-smoke-cursor-2",
            )
        )
        source_sync = run_source_sync_tick(
            service,
            providers={"google": provider},
            sender=linq,
            now_utc=now + timedelta(minutes=5),
        )
        actions = client.get(f"/dev/actions/{chat_id}", headers=admin_headers)
        action_items = actions.json().get("actions", []) if actions.status_code == 200 else []
        action = action_items[0] if action_items else {}
        approval = _post_signed_linq(
            client,
            secret=secret,
            chat_id=chat_id,
            message_id="local-smoke-approval",
            text=f"approve {str(action.get('id', ''))[:8]}",
            sent_at=now + timedelta(minutes=6),
        )
        action_tick = client.post(
            "/dev/actions/tick",
            json={"now_utc": (now + timedelta(minutes=7)).isoformat()},
            headers=admin_headers,
        )
        reminder_tick = client.post(
            "/dev/reminders/tick",
            json={"now_utc": due_at.isoformat()},
            headers=admin_headers,
        )
        readiness = client.get(f"/dev/readiness/{chat_id}", headers=admin_headers)
        source_review = client.get(f"/dev/source-review/{chat_id}", headers=admin_headers)
        executions = client.get(f"/dev/actions/{chat_id}/executions", headers=admin_headers)
        pilot_check = client.get(f"/dev/pilot-check/{chat_id}", headers=admin_headers)
        pilot_proof = client.get(f"/dev/pilot-proof/{chat_id}", headers=admin_headers)

        response_statuses = {name: response.status_code for name, response in responses.items()}
        action_tick_payload = action_tick.json()
        reminder_tick_payload = reminder_tick.json()
        pilot_payload = pilot_check.json()
        proof_payload = pilot_proof.json()
        proof = proof_payload.get("proof") if pilot_proof.status_code == 200 else None
        local_flow_ready = (
            all(status == 200 for status in response_statuses.values())
            and source_sync.checked == 1
            and source_sync.synced == 1
            and source_sync.imported == 1
            and source_sync.surfaced == 1
            and actions.status_code == 200
            and bool(action_items)
            and approval.status_code == 200
            and action_tick_payload == {"ok": True, "attempted": 1, "succeeded": 1, "failed": 0}
            and reminder_tick.status_code == 200
            and bool(reminder_tick_payload.get("messages"))
            and readiness.status_code == 200
            and readiness.json()["readiness"]["ready"] is True
            and source_review.status_code == 200
            and executions.status_code == 200
            and pilot_payload["delivery"]["ready"] is True
            and pilot_payload["actions"]["ready"] is True
            and pilot_proof.status_code == 200
            and _pilot_proof_is_sanitized(proof)
        )
        return {
            "ok": local_flow_ready,
            "mode": "local_only",
            "live_verification_performed": False,
            "chat_id": chat_id,
            "db_path": str(db_path),
            "steps": [
                {"id": name, "ready": status == 200, "status_code": status}
                for name, status in response_statuses.items()
            ]
            + [
                {
                    "id": "connected_source_worker",
                    "ready": source_sync.checked == 1
                    and source_sync.synced == 1
                    and source_sync.imported == 1
                    and source_sync.surfaced == 1,
                    "checked": source_sync.checked,
                    "synced": source_sync.synced,
                    "imported": source_sync.imported,
                    "surfaced": source_sync.surfaced,
                    "delivery_sent": source_sync.delivery_sent,
                    "delivery_failed": source_sync.delivery_failed,
                },
                {
                    "id": "parent_approval",
                    "ready": approval.status_code == 200 and bool(action),
                    "action_type": action.get("action_type"),
                },
                {
                    "id": "action_worker",
                    "ready": action_tick_payload
                    == {"ok": True, "attempted": 1, "succeeded": 1, "failed": 0},
                    "result": action_tick_payload,
                },
                {
                    "id": "reminder_delivery",
                    "ready": reminder_tick.status_code == 200
                    and bool(reminder_tick_payload.get("messages")),
                    "messages": reminder_tick_payload.get("messages", []),
                },
                {
                    "id": "household_readiness",
                    "ready": readiness.status_code == 200
                    and readiness.json()["readiness"]["ready"] is True,
                    "readiness": readiness.json().get("readiness") if readiness.status_code == 200 else None,
                },
                {
                    "id": "pilot_proof_record",
                    "ready": pilot_proof.status_code == 200 and _pilot_proof_is_sanitized(proof),
                    "status_code": pilot_proof.status_code,
                },
            ],
            "source_review": source_review.json().get("snapshot")
            if source_review.status_code == 200
            else None,
            "action_executions": executions.json().get("executions")
            if executions.status_code == 200
            else [],
            "pilot_check": {
                "pilot_ready": pilot_payload.get("pilot_ready"),
                "smoke_checklist": pilot_payload.get("smoke_checklist"),
                "operator_next_steps": pilot_payload.get("operator_next_steps"),
                "locally_verified": (pilot_payload.get("deployment") or {})
                .get("live_verification", {})
                .get("locally_verified", []),
                "external_credentials_needed": (pilot_payload.get("deployment") or {})
                .get("live_verification", {})
                .get("external_credentials_needed", []),
            },
            "pilot_proof": proof,
            "remaining_live_verification": [
                "real Linq iMessage send/webhook round trip",
                "real Google OAuth connection and source sync",
                "real Hermes response through the deployed Florence adapter",
                "managed Postgres staging database",
            ],
            "staging_verification_checklist": _staging_verification_checklist(chat_id=chat_id),
        }
    finally:
        if cleanup_dir is not None:
            cleanup_dir.cleanup()


def run_staging_pilot_verification(
    *,
    base_url: str,
    admin_key: str,
    chat_id: str,
    timeout_seconds: float = 30.0,
    run_hermes_smoke: bool = True,
    http_client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Verify a live staging deployment after the operator has run the smoke flow."""

    headers = {"x-florence-admin-key": admin_key}
    close_client = http_client is None
    client = http_client or httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout_seconds)
    try:
        health = _live_request_json(
            client,
            "GET",
            "/health",
            headers={},
        )
        hermes_status = _live_request_json(
            client,
            "GET",
            "/dev/hermes-status",
            headers=headers,
        )
        if run_hermes_smoke:
            hermes_smoke = _live_request_json(
                client,
                "POST",
                f"/dev/hermes-smoke/{chat_id}",
                headers=headers,
            )
        else:
            hermes_smoke = {
                "status_code": None,
                "payload": None,
                "request_ok": False,
                "skipped": True,
                "error": "Hermes smoke was skipped; live Hermes proof is not verified.",
            }
        deployment = _live_request_json(
            client,
            "GET",
            "/dev/deployment-check",
            headers=headers,
        )
        pilot_check = _live_request_json(
            client,
            "GET",
            f"/dev/pilot-check/{chat_id}",
            headers=headers,
        )
        pilot_proof = _live_request_json(
            client,
            "GET",
            f"/dev/pilot-proof/{chat_id}",
            headers=headers,
        )
    finally:
        if close_client:
            client.close()

    checks = [
        _health_live_check(health),
        _deployment_live_check(deployment),
        _hermes_status_live_check(hermes_status),
        _hermes_smoke_live_check(hermes_smoke),
        _pilot_check_live_check(pilot_check),
        _pilot_proof_live_check(pilot_proof),
    ]
    deployment_summary = _summarize_deployment_check(deployment.get("payload"))
    hermes_status_summary = _summarize_hermes_status(
        (hermes_status.get("payload") or {}).get("hermes")
    )
    hermes_smoke_summary = _summarize_hermes_smoke(hermes_smoke.get("payload"))
    pilot_check_summary = _summarize_pilot_check(pilot_check.get("payload"))
    pilot_proof_summary = _summarize_pilot_proof(pilot_proof.get("payload"))
    return {
        "ok": all(check["ready"] for check in checks),
        "mode": "staging_live_verification",
        "live_verification_performed": True,
        "base_url": base_url.rstrip("/"),
        "chat_id": chat_id,
        "checks": checks,
        "health": _summarize_health_check(health.get("payload")),
        "deployment": deployment_summary,
        "hermes_status": hermes_status_summary,
        "hermes_smoke": hermes_smoke_summary,
        "pilot_check": pilot_check_summary,
        "pilot_proof": pilot_proof_summary,
        "next_actions": _staging_next_actions(
            checks=checks,
            deployment=deployment_summary,
            pilot_check=pilot_check_summary,
        ),
        "remaining": _remaining_live_blockers(checks),
    }


def _live_request_json(
    client: httpx.Client,
    method: str,
    path: str,
    *,
    headers: dict[str, str],
) -> dict[str, Any]:
    try:
        response = client.request(method, path, headers=headers)
        try:
            payload = response.json()
        except ValueError:
            payload = None
        return {
            "status_code": response.status_code,
            "payload": payload,
            "request_ok": 200 <= response.status_code < 300,
        }
    except httpx.HTTPError as exc:
        return {
            "status_code": None,
            "payload": None,
            "request_ok": False,
            "error": f"{type(exc).__name__}: staging request failed",
        }


def _health_live_check(result: dict[str, Any]) -> dict[str, Any]:
    payload = result.get("payload") or {}
    blocked_by = []
    if not result.get("request_ok"):
        blocked_by.append(result.get("error") or "GET /health failed")
    if payload.get("ok") is not True:
        blocked_by.append("GET /health did not return ok: true")
    return {
        "id": "health",
        "ready": bool(result.get("request_ok") and payload.get("ok") is True),
        "status_code": result.get("status_code"),
        "blocked_by": _dedupe_strings(blocked_by),
    }


def _deployment_live_check(result: dict[str, Any]) -> dict[str, Any]:
    deployment = (result.get("payload") or {}).get("deployment") or {}
    blocked_by = []
    if not result.get("request_ok"):
        blocked_by.append(result.get("error") or "GET /dev/deployment-check failed")
    if not deployment.get("ready"):
        blocked_by.extend(deployment.get("invalid") or [])
        blocked_by.extend(deployment.get("missing_required") or [])
        blocked_by.extend((deployment.get("live_verification") or {}).get("blocked") or [])
    return {
        "id": "deployment_check",
        "ready": bool(result.get("request_ok") and deployment.get("ready")),
        "status_code": result.get("status_code"),
        "blocked_by": _dedupe_strings(blocked_by),
    }


def _hermes_status_live_check(result: dict[str, Any]) -> dict[str, Any]:
    hermes = (result.get("payload") or {}).get("hermes") or {}
    blocked_by = []
    if not result.get("request_ok"):
        blocked_by.append(result.get("error") or "GET /dev/hermes-status failed")
    if not hermes.get("ready_for_saas_pilot"):
        blocked_by.extend(hermes.get("invalid") or [])
    return {
        "id": "hermes_status",
        "ready": bool(result.get("request_ok") and hermes.get("ready_for_saas_pilot")),
        "status_code": result.get("status_code"),
        "blocked_by": _dedupe_strings(blocked_by),
    }


def _hermes_smoke_live_check(result: dict[str, Any]) -> dict[str, Any]:
    payload = result.get("payload") or {}
    blocked_by = []
    if result.get("skipped"):
        blocked_by.append(str(result.get("error")))
    elif not result.get("request_ok"):
        blocked_by.append(result.get("error") or "POST /dev/hermes-smoke/{chat_id} failed")
    if not payload.get("live_hermes_verified"):
        error = payload.get("error")
        if error:
            blocked_by.append(str(error))
        hermes_invalid = ((payload.get("hermes") or {}).get("invalid") or [])
        blocked_by.extend(hermes_invalid)
    if payload.get("response") is not None:
        blocked_by.append("Hermes smoke response body must be excluded from operator output")
    if not payload.get("response_present"):
        blocked_by.append("Hermes smoke did not prove a non-empty live response")
    response_chars = payload.get("response_chars")
    if not isinstance(response_chars, int) or response_chars <= 0:
        blocked_by.append("Hermes smoke response length proof is missing")
    if (payload.get("sanitization") or {}).get("response") != "excluded":
        blocked_by.append("Hermes smoke sanitization must mark response as excluded")
    if payload.get("used_fallback"):
        blocked_by.append("Hermes smoke used fallback instead of live Hermes")
    stored = payload.get("stored_live_verification")
    stored_ready = (
        isinstance(stored, dict)
        and stored.get("name") == "hermes"
        and bool(stored.get("verified_at_utc"))
        and bool(stored.get("proof"))
    )
    if not stored_ready:
        blocked_by.append("Hermes smoke did not record stored live verification proof")
    return {
        "id": "hermes_live_response",
        "ready": bool(
            result.get("request_ok")
            and payload.get("live_hermes_verified")
            and payload.get("response") is None
            and payload.get("response_present")
            and isinstance(payload.get("response_chars"), int)
            and payload.get("response_chars") > 0
            and (payload.get("sanitization") or {}).get("response") == "excluded"
            and not payload.get("used_fallback")
            and stored_ready
        ),
        "status_code": result.get("status_code"),
        "blocked_by": _dedupe_strings(blocked_by),
    }


def _pilot_check_live_check(result: dict[str, Any]) -> dict[str, Any]:
    payload = result.get("payload") or {}
    smoke = payload.get("smoke_checklist") or {}
    blocked_by = []
    if not result.get("request_ok"):
        blocked_by.append(result.get("error") or "GET /dev/pilot-check/{chat_id} failed")
    if not payload.get("pilot_ready"):
        blocked_by.extend(smoke.get("blocked") or [])
        for step in payload.get("operator_next_steps") or []:
            blocked_by.extend(step.get("blocked_by") or [])
            blocked_by.extend(step.get("missing_env") or [])
            blocked_by.extend(step.get("missing_proof") or [])
    return {
        "id": "pilot_check",
        "ready": bool(result.get("request_ok") and payload.get("pilot_ready") and smoke.get("ready")),
        "status_code": result.get("status_code"),
        "blocked_by": _dedupe_strings(blocked_by),
    }


def _pilot_proof_live_check(result: dict[str, Any]) -> dict[str, Any]:
    proof = (result.get("payload") or {}).get("proof")
    blocked_by = []
    if not result.get("request_ok"):
        blocked_by.append(result.get("error") or "GET /dev/pilot-proof/{chat_id} failed")
    if not isinstance(proof, dict):
        blocked_by.append("Pilot proof response did not include a proof object")
    elif not proof.get("pilot_ready"):
        blocked_by.append("Pilot proof is not marked pilot_ready")
    if not _pilot_proof_is_sanitized(proof):
        blocked_by.append("Pilot proof sanitization contract failed")
    return {
        "id": "pilot_ready_proof_record",
        "ready": bool(
            result.get("request_ok")
            and isinstance(proof, dict)
            and proof.get("pilot_ready")
            and _pilot_proof_is_sanitized(proof)
        ),
        "status_code": result.get("status_code"),
        "blocked_by": _dedupe_strings(blocked_by),
    }


def _summarize_health_check(payload: object) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    return {"ok": payload.get("ok")}


def _summarize_deployment_check(payload: object) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    deployment = payload.get("deployment")
    if not isinstance(deployment, dict):
        return None
    database = deployment.get("database") or {}
    live = deployment.get("live_verification") or {}
    return {
        "ready": deployment.get("ready"),
        "missing_required": list(deployment.get("missing_required") or []),
        "invalid": list(deployment.get("invalid") or []),
        "database": {
            "configured_backend": database.get("configured_backend"),
            "store_backend": database.get("store_backend"),
            "backend_matches": database.get("backend_matches"),
            "reachable": database.get("reachable"),
            "schema_ready": database.get("schema_ready"),
        },
        "live_verification": {
            "ready": live.get("ready"),
            "external_credentials_needed": list(live.get("external_credentials_needed") or []),
            "unverified": list(live.get("unverified") or []),
            "evidence_gaps": list(live.get("evidence_gaps") or []),
            "blocked": list(live.get("blocked") or []),
            "verified": dict(live.get("verified") or {}),
        },
        "operator_next_steps": _summarize_operator_steps(deployment.get("operator_next_steps")),
    }


def _summarize_hermes_status(hermes: object) -> dict[str, Any] | None:
    if not isinstance(hermes, dict):
        return None
    keys = [
        "mode",
        "contract_ok",
        "ready_for_saas_pilot",
        "database_backend",
        "strict_mode",
        "pinned_ref",
        "hermes_ref_matches",
        "toolsets_disabled",
        "memory_owner",
        "session_scope",
        "durable_hermes_memory",
        "turn_runtime_cleanup",
        "turn_failure_cleanup",
        "turn_runtime_concurrency",
        "runtime_lock",
        "python_path_scope",
        "module_cache_scope",
    ]
    summary = {key: hermes.get(key) for key in keys if key in hermes}
    summary["invalid"] = list(hermes.get("invalid") or [])
    return summary


def _summarize_hermes_smoke(payload: object) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    return {
        "ok": payload.get("ok"),
        "live_hermes_verified": payload.get("live_hermes_verified"),
        "response_present": payload.get("response_present"),
        "response_chars": payload.get("response_chars"),
        "used_fallback": payload.get("used_fallback"),
        "sanitization": payload.get("sanitization"),
        "stored_live_verification": payload.get("stored_live_verification"),
        "error": payload.get("error"),
        "hermes": _summarize_hermes_status(payload.get("hermes")),
    }


def _summarize_pilot_check(payload: object) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    source_review = payload.get("source_review") or {}
    smoke = payload.get("smoke_checklist") or {}
    return {
        "pilot_ready": payload.get("pilot_ready"),
        "household_ready": (payload.get("household") or {}).get("ready"),
        "message_transport_ready": (payload.get("message_transport") or {}).get("ready"),
        "connected_accounts": {
            "token_backed_google": (payload.get("connected_accounts") or {}).get(
                "token_backed_google"
            ),
        },
        "source_review": {
            "total": source_review.get("total"),
            "surfaced": source_review.get("surfaced"),
            "token_backed_google_total": source_review.get("token_backed_google_total"),
            "token_backed_google_surfaced": source_review.get("token_backed_google_surfaced"),
            "latest_token_backed_google_synced_at_utc": source_review.get(
                "latest_token_backed_google_synced_at_utc"
            ),
        },
        "delivery_ready": (payload.get("delivery") or {}).get("ready"),
        "actions_ready": (payload.get("actions") or {}).get("ready"),
        "smoke_checklist": {
            "ready": smoke.get("ready"),
            "blocked": list(smoke.get("blocked") or []),
            "steps": [
                {
                    "id": step.get("id"),
                    "ready": step.get("ready"),
                    "blocked_by": list(step.get("blocked_by") or []),
                }
                for step in smoke.get("steps") or []
                if isinstance(step, dict)
            ],
        },
        "operator_next_steps": _summarize_operator_steps(payload.get("operator_next_steps")),
    }


def _summarize_pilot_proof(payload: object) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    proof = payload.get("proof")
    if not isinstance(proof, dict):
        return None
    if not _pilot_proof_is_sanitized(proof):
        return {
            "pilot_ready": proof.get("pilot_ready"),
            "sanitized": False,
        }
    return proof


def _summarize_operator_steps(steps: object) -> list[dict[str, Any]]:
    if not isinstance(steps, list):
        return []
    summarized = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        summarized.append(
            {
                "id": step.get("id"),
                "status": step.get("status"),
                "missing_env": list(step.get("missing_env") or []),
                "missing_proof": list(step.get("missing_proof") or []),
                "blocked_by": list(step.get("blocked_by") or []),
                "required_env": list(step.get("required_env") or []),
                "proof_env": list(step.get("proof_env") or []),
                "action": step.get("action"),
            }
        )
    return summarized


def _staging_next_actions(
    *,
    checks: list[dict[str, Any]],
    deployment: dict[str, Any] | None,
    pilot_check: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for check in checks:
        if check.get("ready"):
            continue
        actions.append(
            {
                "source": "staging_check",
                "id": check.get("id"),
                "status": "not_ready",
                "blocked_by": list(check.get("blocked_by") or []),
            }
        )
    for step in (deployment or {}).get("operator_next_steps") or []:
        actions.append({"source": "deployment", **step})
    for step in (pilot_check or {}).get("operator_next_steps") or []:
        actions.append({"source": "pilot_check", **step})
    return _dedupe_action_dicts(actions)


def _dedupe_action_dicts(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    seen = set()
    for action in actions:
        key = json.dumps(action, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        result.append(action)
    return result


def _remaining_live_blockers(checks: list[dict[str, Any]]) -> list[str]:
    blockers = []
    for check in checks:
        if check.get("ready"):
            continue
        blockers.append(str(check.get("id")))
        blockers.extend(str(item) for item in check.get("blocked_by") or [])
    return _dedupe_strings(blockers)


def _dedupe_strings(items: list[object]) -> list[str]:
    result = []
    seen = set()
    for item in items:
        text = str(item)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _pilot_proof_is_sanitized(proof: object) -> bool:
    if not isinstance(proof, dict):
        return False
    return proof.get("sanitization") == {
        "message_bodies": "excluded",
        "source_bodies": "excluded",
        "source_titles": "excluded",
        "source_event_times": "presence_only",
        "oauth_tokens": "excluded",
        "memory_text": "excluded",
        "action_errors": "presence_only",
        "diagnostic_strings": "redacted",
    }


def _staging_verification_checklist(*, chat_id: str) -> list[dict[str, object]]:
    return [
        {
            "id": "configuration_preflight",
            "status": "not_verified_by_local_smoke",
            "proof": "GET /dev/deployment-check",
            "expect": [
                "deployment.missing_required == []",
                "deployment.invalid == []",
                "deployment.live_verification.external_credentials_needed == []",
            ],
            "runtime_env": [
                "LINQ_WEBHOOK_SECRET",
                "LINQ_API_KEY",
                "LINQ_FROM_PHONE",
                "FLORENCE_ADMIN_API_KEY",
                "FLORENCE_SOURCE_INGEST_API_KEY",
                "FLORENCE_TOKEN_ENCRYPTION_KEY",
                "FLORENCE_SUPPORT_CONTACT",
                "FLORENCE_DATABASE_URL",
                "FLORENCE_HERMES_AGENT_PATH",
                "HERMES_AGENT_REF",
                "FLORENCE_HERMES_PROVIDER",
                "FLORENCE_HERMES_MODEL",
                "FLORENCE_HERMES_STRICT",
            ],
            "live_verification_records": [
                "live_verifications.linq",
                "live_verifications.google",
                "live_verifications.hermes",
            ],
            "fallback_env": [
                "FLORENCE_LINQ_LIVE_VERIFIED",
                "FLORENCE_LINQ_LIVE_VERIFIED_AT",
                "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF",
                "FLORENCE_GOOGLE_LIVE_VERIFIED",
                "FLORENCE_GOOGLE_LIVE_VERIFIED_AT",
                "FLORENCE_GOOGLE_LIVE_VERIFICATION_PROOF",
                "FLORENCE_HERMES_LIVE_VERIFIED",
                "FLORENCE_HERMES_LIVE_VERIFIED_AT",
                "FLORENCE_HERMES_LIVE_VERIFICATION_PROOF",
            ],
        },
        {
            "id": "managed_postgres",
            "status": "not_verified_by_local_smoke",
            "proof": "GET /dev/deployment-check",
            "expect": [
                "deployment.database.configured_backend == postgres",
                "deployment.database.store_backend == postgres",
                "deployment.database.reachable == true",
            ],
            "runtime_env": ["FLORENCE_DATABASE_URL"],
        },
        {
            "id": "linq_live_round_trip",
            "status": "not_verified_by_local_smoke",
            "proof": "GET /dev/pilot-check/{staging_chat_id}",
            "staging_chat_id_placeholder": "{staging_chat_id}",
            "local_rehearsal_chat_id": chat_id,
            "expect": [
                "message_transport.ready == true",
                "smoke_checklist.steps.linq_live_round_trip.ready == true",
                "deployment.live_verification.evidence.linq.verified == true",
            ],
            "runtime_env": [
                "LINQ_API_KEY",
                "LINQ_WEBHOOK_SECRET",
                "LINQ_FROM_PHONE",
            ],
            "credential_env": ["LINQ_WEBHOOK_SECRET", "LINQ_API_KEY", "LINQ_FROM_PHONE"],
            "proof_record": "live_verifications.linq",
            "automatic_proof_source": "linq_webhook",
            "fallback_env": [
                "FLORENCE_LINQ_LIVE_VERIFIED",
                "FLORENCE_LINQ_LIVE_VERIFIED_AT",
                "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF",
            ],
        },
        {
            "id": "google_oauth_source_sync",
            "status": "not_verified_by_local_smoke",
            "proof": "GET /dev/pilot-check/{staging_chat_id}",
            "staging_chat_id_placeholder": "{staging_chat_id}",
            "local_rehearsal_chat_id": chat_id,
            "expect": [
                "connected_accounts.token_backed_google > 0",
                "source_review.token_backed_google_surfaced > 0",
                "smoke_checklist.steps.google_live_source_sync.ready == true",
                "deployment.live_verification.evidence.google.verified == true",
            ],
            "runtime_env": [
                "GOOGLE_CLIENT_ID",
                "GOOGLE_CLIENT_SECRET",
                "GOOGLE_REDIRECT_URI",
                "FLORENCE_TOKEN_ENCRYPTION_KEY",
            ],
            "credential_env": [
                "GOOGLE_CLIENT_ID",
                "GOOGLE_CLIENT_SECRET",
                "GOOGLE_REDIRECT_URI",
                "FLORENCE_TOKEN_ENCRYPTION_KEY",
            ],
            "proof_record": "live_verifications.google",
            "automatic_proof_source": "source_sync_worker",
            "fallback_env": [
                "FLORENCE_GOOGLE_LIVE_VERIFIED",
                "FLORENCE_GOOGLE_LIVE_VERIFIED_AT",
                "FLORENCE_GOOGLE_LIVE_VERIFICATION_PROOF",
            ],
        },
        {
            "id": "hermes_live_response",
            "status": "not_verified_by_local_smoke",
            "proof": "POST /dev/hermes-smoke/{staging_chat_id}",
            "staging_chat_id_placeholder": "{staging_chat_id}",
            "local_rehearsal_chat_id": chat_id,
            "expect": [
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
            ],
            "build_args": [
                "INSTALL_HERMES_AGENT",
                "HERMES_AGENT_REF",
                "HERMES_AGENT_REPO",
            ],
            "runtime_env": [
                "FLORENCE_HERMES_AGENT_PATH",
                "HERMES_AGENT_REF",
                "FLORENCE_HERMES_PROVIDER",
                "FLORENCE_HERMES_MODEL",
                "FLORENCE_HERMES_TOOLSETS",
                "FLORENCE_HERMES_RUNTIME_HOME",
                "FLORENCE_HERMES_STRICT",
            ],
            "credential_env": [
                "FLORENCE_HERMES_AGENT_PATH",
                "HERMES_AGENT_REF",
                "FLORENCE_HERMES_PROVIDER",
                "FLORENCE_HERMES_MODEL",
                "FLORENCE_HERMES_STRICT",
            ],
            "proof_record": "live_verifications.hermes",
            "automatic_proof_source": "hermes_smoke",
            "fallback_env": [
                "FLORENCE_HERMES_LIVE_VERIFIED",
                "FLORENCE_HERMES_LIVE_VERIFIED_AT",
                "FLORENCE_HERMES_LIVE_VERIFICATION_PROOF",
            ],
        },
        {
            "id": "deployment_ready",
            "status": "not_verified_by_local_smoke",
            "proof": "GET /dev/deployment-check",
            "expect": [
                "deployment.ready == true",
                "deployment.live_verification.ready == true",
                "deployment.live_verification.blocked == []",
            ],
            "live_verification_records": [
                "live_verifications.linq",
                "live_verifications.google",
                "live_verifications.hermes",
            ],
            "fallback_env": [
                "FLORENCE_LINQ_LIVE_VERIFIED",
                "FLORENCE_LINQ_LIVE_VERIFIED_AT",
                "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF",
                "FLORENCE_GOOGLE_LIVE_VERIFIED",
                "FLORENCE_GOOGLE_LIVE_VERIFIED_AT",
                "FLORENCE_GOOGLE_LIVE_VERIFICATION_PROOF",
                "FLORENCE_HERMES_LIVE_VERIFIED",
                "FLORENCE_HERMES_LIVE_VERIFIED_AT",
                "FLORENCE_HERMES_LIVE_VERIFICATION_PROOF",
            ],
        },
        {
            "id": "pilot_ready_proof_record",
            "status": "not_verified_by_local_smoke",
            "proof": "GET /dev/pilot-proof/{staging_chat_id}",
            "staging_chat_id_placeholder": "{staging_chat_id}",
            "local_rehearsal_chat_id": chat_id,
            "expect": [
                "proof.pilot_ready == true",
                "proof.sanitization.message_bodies == excluded",
                "proof.sanitization.source_bodies == excluded",
                "proof.sanitization.source_titles == excluded",
                "proof.sanitization.source_event_times == presence_only",
                "proof.sanitization.oauth_tokens == excluded",
                "proof.sanitization.memory_text == excluded",
                "proof.sanitization.action_errors == presence_only",
                "proof.sanitization.diagnostic_strings == redacted",
            ],
            "runtime_env": ["FLORENCE_ADMIN_API_KEY"],
        },
    ]


def _attach_local_google_token(
    *,
    store: Store,
    settings: Settings,
    chat_id: str,
    now: datetime,
) -> None:
    household = store.get_household_by_chat(chat_id)
    if household is None:
        raise RuntimeError("local smoke household was not created")
    accounts = store.list_connected_accounts(household.id)
    if not accounts:
        raise RuntimeError("local smoke connected account was not created")
    expires_at = now + timedelta(hours=1)
    vault = TokenVault.from_settings(settings)
    store.upsert_connected_account_token(
        connected_account_id=accounts[0].id,
        provider="google",
        token_ciphertext=vault.encrypt(
            {
                "provider": "google",
                "access_token": "local-smoke-access-token",
                "refresh_token": "local-smoke-refresh-token",
                "expires_at_utc": expires_at.isoformat(),
            }
        ),
        scopes=("openid", "email", "https://www.googleapis.com/auth/gmail.readonly"),
        expires_at_utc=expires_at,
        now_utc=now,
    )


def _signed_linq_headers(secret: str, raw_body: bytes) -> dict[str, str]:
    timestamp = str(int(time.time()))
    signature = hmac.new(
        secret.encode("utf-8"),
        timestamp.encode("utf-8") + b"." + raw_body,
        hashlib.sha256,
    ).hexdigest()
    return {
        "content-type": "application/json",
        "x-webhook-timestamp": timestamp,
        "x-webhook-signature": signature,
    }


def _linq_payload(
    *,
    chat_id: str,
    message_id: str,
    sender: str,
    text: str,
    sent_at: datetime,
) -> dict[str, object]:
    return {
        "event": "message.received",
        "data": {
            "chat": {"id": chat_id},
            "message": {
                "id": message_id,
                "from": sender,
                "parts": [{"type": "text", "value": text}],
                "sent_at": sent_at.isoformat(),
            },
        },
    }


def _post_signed_linq(
    client: TestClient,
    *,
    secret: str,
    chat_id: str,
    message_id: str,
    text: str,
    sent_at: datetime,
    sender: str = "+15555550100",
):
    payload = _linq_payload(
        chat_id=chat_id,
        message_id=message_id,
        sender=sender,
        text=text,
        sent_at=sent_at,
    )
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return client.post(
        "/webhooks/linq",
        content=raw,
        headers=_signed_linq_headers(secret, raw),
    )
