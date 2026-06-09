"""FastAPI entrypoint for Florence."""

from __future__ import annotations

import html
import hmac
import importlib.util
import inspect
import logging
import re
import sys
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from florence.actions import run_approved_actions
from florence import tone
from florence.config import Settings
from florence.hermes import (
    AgentBackend,
    HERMES_INTERPROCESS_LOCK_MODE,
    HermesSaaSContractError,
    hermes_checkout_ref as read_hermes_checkout_ref,
    hermes_checkout_module_context,
    hermes_preflight_scope,
    hermes_runtime_concurrency_mode,
    hermes_runtime_lock_error,
    hermes_runtime_lock_mode,
    hermes_runtime_home_context,
    hermes_runtime_home_error,
    scoped_hermes_runtime_home,
)
from florence.linq import LinqClient, parse_linq_event, verify_linq_signature
from florence.models import Household, HouseholdMember, IncomingMessage, MemberRole, OutboundMessage
from florence.onboarding import OnboardingTokenClaims, OnboardingTokenError, verify_onboarding_token
from florence.oauth import (
    GoogleOAuthClient,
    OAuthConfigurationError,
    OAuthExchangeError,
    TokenVault,
    TokenVaultError,
    missing_google_oauth_settings,
)
from florence.service import FlorenceService
from florence.source_ingest import (
    MAX_SOURCE_BODY_CHARS,
    MAX_SOURCE_SENDER_CHARS,
    MAX_SOURCE_TITLE_CHARS,
)
from florence.store import DatabaseSchemaError, Store
from florence.timekeeper import ensure_utc
from florence.worker import run_linq_reconciliation_tick, run_routine_tick


logger = logging.getLogger(__name__)

PUBLIC_SOURCE_TYPES = {"email", "calendar", "document", "flyer", "school", "automation", "webhook"}
PILOT_REQUIRED_SETTINGS = {
    "LINQ_WEBHOOK_SECRET": "linq_webhook_secret",
    "LINQ_API_KEY": "linq_api_key",
    "LINQ_FROM_PHONE": "linq_from_phone",
    "FLORENCE_ADMIN_API_KEY": "admin_api_key",
    "FLORENCE_SOURCE_INGEST_API_KEY": "source_ingest_api_key",
    "FLORENCE_TOKEN_ENCRYPTION_KEY": "token_encryption_key",
    "FLORENCE_SUPPORT_CONTACT": "support_contact",
    "FLORENCE_DATABASE_URL": "database_url",
    "FLORENCE_HERMES_AGENT_PATH": "hermes_agent_path",
    "HERMES_AGENT_REF": "hermes_agent_ref",
    "FLORENCE_HERMES_PROVIDER": "hermes_provider",
    "FLORENCE_HERMES_MODEL": "hermes_model",
}
PINNED_GIT_REF = re.compile(r"^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$")
HERMES_AGENT_INIT_KWARGS = (
    "provider",
    "model",
    "enabled_toolsets",
    "quiet_mode",
    "save_trajectories",
    "skip_context_files",
    "skip_memory",
    "platform",
    "session_id",
)
HERMES_RUN_CONVERSATION_KWARGS = (
    "user_message",
    "system_message",
    "conversation_history",
)
HERMES_FAILURE_CLEANUP = "runtime_home_restored_and_checkout_modules_cleared_on_error"
LIVE_VERIFICATION_MAX_FUTURE_SKEW = timedelta(hours=24)
LIVE_VERIFICATION_PROOF_MAX_CHARS = 240
PILOT_MESSAGE_TRANSPORT_MAX_PROOF_AGE = timedelta(days=7)
PILOT_SOURCE_SYNC_MAX_PROOF_AGE = timedelta(days=7)
LIVE_PROOF_EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
LIVE_PROOF_PHONE_RE = re.compile(
    r"(?<!\w)(?:"
    r"\+\d[\d\s().-]{7,}\d"
    r"|(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}"
    r")(?!\w)"
)
LIVE_PROOF_SECRET_RE = re.compile(
    r"(?i)("
    r"bearer\s+[A-Za-z0-9._~+/=-]{8,}"
    r"|access[_ -]?token"
    r"|refresh[_ -]?token"
    r"|client[_ -]?secret"
    r"|api[_ -]?key"
    r"|webhook[_ -]?secret"
    r"|token_ciphertext"
    r"|postgres(?:ql)?://[^@\s]+@"
    r")"
)
LIVE_PROOF_RAW_PAYLOAD_RE = re.compile(r"[{}\n\r]")
LIVE_VERIFICATION_PROOF_SAFETY_ERROR = (
    "must be a short proof note without secrets, tokens, PII, raw payloads, or line breaks"
)
LIVE_VERIFICATION_SPECS = {
    "linq": {
        "label": "Live Linq iMessage round trip",
        "required_env": ("LINQ_WEBHOOK_SECRET", "LINQ_API_KEY", "LINQ_FROM_PHONE"),
        "verified_env": "FLORENCE_LINQ_LIVE_VERIFIED",
        "verified_attr": "linq_live_verified",
        "verified_at_attr": "linq_live_verified_at",
        "proof_attr": "linq_live_verification_proof",
        "verified_at_env": "FLORENCE_LINQ_LIVE_VERIFIED_AT",
        "proof_env": "FLORENCE_LINQ_LIVE_VERIFICATION_PROOF",
        "action": "Send and receive a real iMessage through Linq, then record the smoke timestamp and proof note.",
    },
    "google": {
        "label": "Live Google OAuth source sync",
        "required_env": (
            "GOOGLE_CLIENT_ID",
            "GOOGLE_CLIENT_SECRET",
            "GOOGLE_REDIRECT_URI",
            "FLORENCE_TOKEN_ENCRYPTION_KEY",
        ),
        "verified_env": "FLORENCE_GOOGLE_LIVE_VERIFIED",
        "verified_attr": "google_live_verified",
        "verified_at_attr": "google_live_verified_at",
        "proof_attr": "google_live_verification_proof",
        "verified_at_env": "FLORENCE_GOOGLE_LIVE_VERIFIED_AT",
        "proof_env": "FLORENCE_GOOGLE_LIVE_VERIFICATION_PROOF",
        "action": "Connect a parent Google account through OAuth, sync one controlled item, then record the timestamp and proof note.",
    },
    "hermes": {
        "label": "Live Hermes adapter response",
        "required_env": (
            "FLORENCE_HERMES_AGENT_PATH",
            "HERMES_AGENT_REF",
            "FLORENCE_HERMES_PROVIDER",
            "FLORENCE_HERMES_MODEL",
        ),
        "verified_env": "FLORENCE_HERMES_LIVE_VERIFIED",
        "verified_attr": "hermes_live_verified",
        "verified_at_attr": "hermes_live_verified_at",
        "proof_attr": "hermes_live_verification_proof",
        "verified_at_env": "FLORENCE_HERMES_LIVE_VERIFIED_AT",
        "proof_env": "FLORENCE_HERMES_LIVE_VERIFICATION_PROOF",
        "action": "Run /dev/hermes-smoke/{chat_id}, confirm live_hermes_verified=true without fallback, then record the timestamp and proof note.",
    },
}
OPERATOR_SAFE_ENV_NAMES = frozenset(
    {
        *PILOT_REQUIRED_SETTINGS.keys(),
        "FLORENCE_DB_PATH",
        "FLORENCE_DEV_ENDPOINTS_ENABLED",
        "FLORENCE_DEFAULT_TIMEZONE",
        "FLORENCE_WEB_BASE_URL",
        "FLORENCE_ONBOARDING_STATE_SECRET",
        "FLORENCE_ONBOARDING_TOKEN_TTL_HOURS",
        "GOOGLE_CLIENT_ID",
        "GOOGLE_CLIENT_SECRET",
        "GOOGLE_REDIRECT_URI",
        "GOOGLE_OAUTH_SCOPES",
        "INSTALL_HERMES_AGENT",
        "HERMES_AGENT_REPO",
        "FLORENCE_HERMES_TOOLSETS",
        "FLORENCE_HERMES_RUNTIME_HOME",
        "FLORENCE_HERMES_STRICT",
        "FLORENCE_DATABASE_URL",
        "FLORENCE_LINQ_LIVE_VERIFIED",
        "FLORENCE_GOOGLE_LIVE_VERIFIED",
        "FLORENCE_HERMES_LIVE_VERIFIED",
        *(
            env_name
            for spec in LIVE_VERIFICATION_SPECS.values()
            for env_name in (
                *spec["required_env"],
                spec["verified_env"],
                spec["verified_at_env"],
                spec["proof_env"],
            )
        ),
    }
)


def create_app(
    settings: Settings | None = None,
    *,
    agent: AgentBackend | None = None,
    store: Store | None = None,
    google_oauth_client: GoogleOAuthClient | None = None,
    linq_client: LinqClient | None = None,
    now_fn: Callable[[], datetime] | None = None,
) -> FastAPI:
    resolved = settings or Settings.from_env()
    service = FlorenceService(settings=resolved, store=store, agent=agent)
    linq = linq_client or LinqClient(resolved)
    google_oauth = google_oauth_client or GoogleOAuthClient(resolved)
    app = FastAPI(title="Florence", version="0.1.0")

    def app_now() -> datetime:
        return ensure_utc(now_fn() if now_fn is not None else _now())

    def dev_guard(request: Request) -> None:
        _require_dev_access(resolved, request)

    def source_ingest_guard(request: Request) -> None:
        _require_source_ingest_access(resolved, request)

    def require_household(chat_id: str):
        household = service.store.get_household_by_chat(chat_id)
        if household is None:
            raise HTTPException(status_code=404, detail="household_not_found")
        return household

    def require_onboarding_access(token: str) -> tuple[OnboardingTokenClaims, Household, HouseholdMember]:
        if not resolved.onboarding_state_secret:
            raise HTTPException(status_code=503, detail="onboarding_not_configured")
        try:
            claims = verify_onboarding_token(
                secret=resolved.onboarding_state_secret,
                token=token,
                now_utc=app_now(),
            )
        except OnboardingTokenError as exc:
            raise HTTPException(status_code=400, detail="invalid_or_expired_onboarding_link") from exc
        household = service.store.get_household_by_chat(claims.chat_id)
        if household is None:
            raise HTTPException(status_code=404, detail="household_not_found")
        actor = next(
            (
                member
                for member in service.store.list_members(household.id)
                if member.id == claims.member_id
            ),
            None,
        )
        if actor is None or actor.role != MemberRole.PARENT:
            raise HTTPException(status_code=403, detail="onboarding_parent_required")
        return claims, household, actor

    @app.get("/health")
    def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/onboarding/{token}")
    async def web_onboarding(token: str, request: Request) -> HTMLResponse:
        claims, household, actor = require_onboarding_access(token)
        now = app_now()
        readiness = service.readiness_snapshot(chat_id=claims.chat_id, now_utc=now)
        memory = service.memory_snapshot(chat_id=claims.chat_id, now_utc=now)
        source_preferences = service.source_preferences(chat_id=claims.chat_id)
        connected_accounts = service.connected_accounts(chat_id=claims.chat_id)
        google_missing = missing_google_oauth_settings(resolved)
        return HTMLResponse(
            _render_onboarding_form(
                token=token,
                claims=claims,
                household=household,
                actor=actor,
                readiness=readiness,
                memory_texts=[item.text for item in memory.memories[:10]],
                form_values=_onboarding_form_values(
                    actor=actor,
                    members=service.store.list_members(household.id),
                    memories=memory.memories,
                    source_preferences=source_preferences,
                ),
                connected_account_count=len(connected_accounts),
                google_configured=not google_missing,
                google_missing=google_missing,
                google_status=request.query_params.get("google"),
            )
        )

    @app.post("/onboarding/{token}")
    async def web_onboarding_submit(token: str, request: Request) -> HTMLResponse:
        claims, household, actor = require_onboarding_access(token)
        now = app_now()
        data = await _request_data(request)
        try:
            result = service.apply_web_onboarding(
                chat_id=claims.chat_id,
                actor_member_id=actor.id,
                role=claims.role,
                data=data,
                now_utc=now,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        readiness = service.readiness_snapshot(chat_id=claims.chat_id, now_utc=now)
        memory = service.memory_snapshot(chat_id=claims.chat_id, now_utc=now)
        source_preferences = service.source_preferences(chat_id=claims.chat_id)
        google_missing = missing_google_oauth_settings(resolved)
        return HTMLResponse(
            _render_onboarding_done(
                token=token,
                claims=claims,
                household=household,
                actor=actor,
                result=asdict(result),
                readiness=readiness,
                memory_texts=[item.text for item in memory.memories[:10]],
                source_preferences=[item.phrase for item in source_preferences],
                google_configured=not google_missing,
            )
        )

    @app.get("/onboarding/{token}/google")
    async def web_onboarding_google(token: str):
        claims, _household, actor = require_onboarding_access(token)
        missing = missing_google_oauth_settings(resolved)
        if missing:
            return HTMLResponse(_render_onboarding_google_missing(missing), status_code=503)
        try:
            start = service.start_google_oauth(
                chat_id=claims.chat_id,
                account_label=actor.display_name or actor.phone,
                return_path=f"/onboarding/{token}?google=connected",
                now_utc=app_now(),
            )
        except OAuthConfigurationError:
            return HTMLResponse(_render_onboarding_google_missing(missing), status_code=503)
        return RedirectResponse(start.authorization_url, status_code=303)

    @app.post("/webhooks/linq")
    async def linq_webhook(request: Request) -> dict[str, Any]:
        raw = await request.body()
        if resolved.database_backend == "postgres" and not resolved.linq_webhook_secret:
            raise HTTPException(status_code=401, detail="linq_webhook_secret_required")
        if not verify_linq_signature(
            secret=resolved.linq_webhook_secret,
            raw_body=raw,
            timestamp=request.headers.get("x-webhook-timestamp"),
            signature=request.headers.get("x-webhook-signature"),
            webhook_id=request.headers.get("webhook-id"),
            webhook_timestamp=request.headers.get("webhook-timestamp"),
            webhook_signature=request.headers.get("webhook-signature"),
        ):
            raise HTTPException(status_code=401, detail="invalid_signature")
        payload = await request.json()
        incoming = parse_linq_event(payload, request.headers, now_utc=app_now())
        if incoming is None:
            return {"ok": True, "ignored": True}
        now = incoming.received_at
        outbound = service.handle_incoming(incoming, now_utc=now)
        household = service.store.get_household_by_chat(incoming.chat_id)
        if household is not None:
            if outbound:
                service.store.record_outbound_deliveries_for_source(
                    household_id=household.id,
                    source_message_id=incoming.message_id,
                    messages=outbound,
                    now_utc=now,
                )
            else:
                outbound = service.store.retryable_outbound_deliveries_for_source(
                    household_id=household.id,
                    source_message_id=incoming.message_id,
                )
        send_results = _send_all(linq, outbound, service=service, now_utc=now)
        _maybe_record_linq_live_verification(
            service=service,
            settings=resolved,
            client=linq,
            send_results=send_results,
            verified_at_utc=now,
        )
        return {"ok": True, "sent": len(outbound)}

    @app.post("/api/source-items", dependencies=[Depends(source_ingest_guard)])
    async def api_source_item(payload: dict[str, Any]) -> dict[str, Any]:
        if "instruction" in payload or "message" in payload:
            raise HTTPException(status_code=400, detail="source_item_not_agent_instruction")
        chat_id = _required_text(payload, "chat_id")
        source_type = _required_text(payload, "source_type").lower()
        if source_type not in PUBLIC_SOURCE_TYPES:
            raise HTTPException(status_code=400, detail="unsupported_source_type")
        title = _required_text(payload, "title")
        _reject_too_long("title", title, MAX_SOURCE_TITLE_CHARS)
        external_id = _required_text(payload, "external_id")
        body = str(payload.get("body") or "")
        sender = payload.get("sender")
        _reject_too_long("body", body, MAX_SOURCE_BODY_CHARS)
        if sender is not None:
            _reject_too_long("sender", str(sender), MAX_SOURCE_SENDER_CHARS)
        if service.store.get_household_by_chat(chat_id) is None:
            raise HTTPException(status_code=404, detail="household_not_found")
        now = _parse_optional_dt(payload, "now_utc") or app_now()
        outbound = service.ingest_source_item(
            chat_id=chat_id,
            source_type=source_type,
            title=title,
            body=body,
            sender=sender,
            external_id=external_id,
            observed_at_utc=_parse_optional_dt(payload, "observed_at_utc"),
            event_at_utc=_parse_optional_dt(payload, "event_at_utc"),
            now_utc=now,
            mark_surfaced=False,
        )
        _send_all(linq, outbound, service=service, now_utc=now)
        return {"ok": True, "sent": len(outbound), "messages": [_as_response(message) for message in outbound]}

    @app.post("/dev/messages", dependencies=[Depends(dev_guard)])
    async def dev_message(payload: dict[str, Any]) -> dict[str, Any]:
        now = _parse_dt(payload.get("now_utc")) or app_now()
        incoming = IncomingMessage(
            chat_id=_dev_chat_id(payload, resolved),
            message_id=str(payload.get("message_id") or f"dev-{now.timestamp()}"),
            sender=str(payload.get("sender") or "+15555550100"),
            text=str(payload.get("text") or ""),
            received_at=_parse_dt(payload.get("received_at")) or now,
        )
        outbound = service.handle_incoming(incoming, now_utc=now)
        _send_all(linq, outbound, service=service, now_utc=now)
        return {"ok": True, "messages": [_as_response(message) for message in outbound]}

    @app.post("/dev/source-items", dependencies=[Depends(dev_guard)])
    async def dev_source_item(payload: dict[str, Any]) -> dict[str, Any]:
        now = _parse_dt(payload.get("now_utc")) or app_now()
        outbound = service.ingest_source_item(
            chat_id=_dev_chat_id(payload, resolved),
            source_type=str(payload.get("source_type") or "email"),
            title=str(payload.get("title") or ""),
            body=str(payload.get("body") or ""),
            sender=payload.get("sender"),
            external_id=payload.get("external_id"),
            observed_at_utc=_parse_dt(payload.get("observed_at_utc")),
            event_at_utc=_parse_dt(payload.get("event_at_utc")),
            now_utc=now,
        )
        _send_all(linq, outbound, service=service, now_utc=now)
        return {"ok": True, "messages": [_as_response(message) for message in outbound]}

    @app.post("/dev/import/email", dependencies=[Depends(dev_guard)])
    async def dev_import_email(payload: dict[str, Any]) -> dict[str, Any]:
        now = _parse_dt(payload.get("now_utc")) or app_now()
        received_at = _parse_dt(payload.get("received_at_utc")) or now
        outbound = service.ingest_email(
            chat_id=_dev_chat_id(payload, resolved),
            subject=str(payload.get("subject") or ""),
            body=str(payload.get("body") or ""),
            sender=str(payload.get("sender") or ""),
            received_at_utc=received_at,
            external_id=payload.get("external_id"),
            event_at_utc=_parse_dt(payload.get("event_at_utc")),
            now_utc=now,
            mark_surfaced=False,
        )
        _send_all(linq, outbound, service=service, now_utc=now)
        return {"ok": True, "messages": [_as_response(message) for message in outbound]}

    @app.post("/dev/import/calendar", dependencies=[Depends(dev_guard)])
    async def dev_import_calendar(payload: dict[str, Any]) -> dict[str, Any]:
        starts_at = _parse_dt(payload.get("starts_at_utc"))
        if starts_at is None:
            raise HTTPException(status_code=400, detail="starts_at_utc_required")
        now = _parse_dt(payload.get("now_utc")) or app_now()
        outbound = service.ingest_calendar_event(
            chat_id=_dev_chat_id(payload, resolved),
            title=str(payload.get("title") or ""),
            starts_at_utc=starts_at,
            ends_at_utc=_parse_dt(payload.get("ends_at_utc")),
            location=payload.get("location"),
            description=payload.get("description"),
            calendar_name=payload.get("calendar_name"),
            external_id=payload.get("external_id"),
            observed_at_utc=_parse_dt(payload.get("observed_at_utc")),
            now_utc=now,
            mark_surfaced=False,
        )
        _send_all(linq, outbound, service=service, now_utc=now)
        return {"ok": True, "messages": [_as_response(message) for message in outbound]}

    @app.post("/dev/sync-sources", dependencies=[Depends(dev_guard)])
    async def dev_sync_sources(payload: dict[str, Any]) -> dict[str, Any]:
        emails = payload.get("emails")
        calendar_events = payload.get("calendar_events")
        now = _parse_dt(payload.get("now_utc")) or app_now()
        result = service.sync_connected_sources(
            chat_id=_dev_chat_id(payload, resolved),
            provider=str(payload.get("provider") or "google"),
            external_account_id=str(payload.get("external_account_id") or "dev-account"),
            account_label=payload.get("account_label"),
            emails=emails if isinstance(emails, list) else [],
            calendar_events=calendar_events if isinstance(calendar_events, list) else [],
            cursor=payload.get("cursor"),
            now_utc=now,
            mark_surfaced=False,
        )
        _send_all(linq, result.messages, service=service, now_utc=now)
        return {
            "ok": True,
            "account": asdict(result.account),
            "imported": result.imported,
            "surfaced": result.surfaced,
            "messages": [_as_response(message) for message in result.messages],
        }

    @app.post("/dev/oauth/google/start", dependencies=[Depends(dev_guard)])
    async def dev_google_oauth_start(payload: dict[str, Any]) -> dict[str, Any]:
        missing = missing_google_oauth_settings(resolved)
        if missing:
            raise HTTPException(
                status_code=503,
                detail={"error": "google_oauth_not_configured", "missing": missing},
            )
        now = _parse_dt(payload.get("now_utc")) or app_now()
        try:
            start = service.start_google_oauth(
                chat_id=_dev_chat_id(payload, resolved),
                account_label=payload.get("account_label"),
                now_utc=now,
            )
        except OAuthConfigurationError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return {
            "ok": True,
            "authorization_url": start.authorization_url,
            "state": start.state,
            "expires_at_utc": start.expires_at_utc.isoformat(),
            "scopes": list(start.scopes),
        }

    @app.get("/oauth/google/callback")
    async def google_oauth_callback(request: Request):
        error = request.query_params.get("error")
        if error:
            raise HTTPException(status_code=400, detail="google_oauth_error")
        state = request.query_params.get("state")
        code = request.query_params.get("code")
        if not state or not code:
            raise HTTPException(status_code=400, detail="state_and_code_required")
        missing = missing_google_oauth_settings(resolved)
        if missing:
            raise HTTPException(status_code=503, detail="google_oauth_not_configured")
        now = app_now()
        oauth_state = service.store.consume_oauth_state(
            state=state,
            provider="google",
            now_utc=now,
        )
        if oauth_state is None:
            raise HTTPException(status_code=400, detail="invalid_or_expired_oauth_state")
        household = service.store.get_household_by_chat(oauth_state.chat_id)
        if household is None:
            raise HTTPException(status_code=400, detail="invalid_or_expired_oauth_state")
        confirmation_failed = False
        confirmation_skipped_stopped = service.store.is_stopped(household.id)
        confirmation: OutboundMessage | None = None
        try:
            vault = TokenVault.from_settings(resolved)
            result = google_oauth.exchange_code(code=code, now_utc=now)
            account = service.store.upsert_connected_account(
                household_id=household.id,
                provider="google",
                external_account_id=result.external_account_id,
                account_label=oauth_state.account_label or result.account_label,
                now_utc=now,
            )
            service.store.upsert_connected_account_token(
                connected_account_id=account.id,
                provider="google",
                token_ciphertext=vault.encrypt(result.token_payload),
                scopes=result.scopes,
                expires_at_utc=result.expires_at_utc,
                now_utc=now,
            )
            if not confirmation_skipped_stopped:
                confirmation = service.google_connected_confirmation(
                    chat_id=oauth_state.chat_id,
                    account_label=account.account_label,
                    oauth_state=oauth_state.state,
                    now_utc=now,
                )
        except (OAuthConfigurationError, OAuthExchangeError, TokenVaultError) as exc:
            raise HTTPException(status_code=502, detail="google_oauth_callback_failed") from exc
        except ValueError as exc:
            if str(exc) == "household_not_found":
                raise HTTPException(status_code=400, detail="invalid_or_expired_oauth_state") from exc
            raise
        if confirmation is not None:
            try:
                _send_all(linq, [confirmation], service=service, now_utc=now)
            except Exception:
                confirmation_failed = True
        return_path = _safe_return_path(oauth_state.return_path)
        if return_path is not None:
            return RedirectResponse(return_path, status_code=303)
        return HTMLResponse(
            f"""
            <!doctype html>
            <html>
              <head><title>Florence connected Google</title></head>
              <body>
                <h1>Google is connected.</h1>
                <p>You can close this tab and return to iMessage.</p>
                {
                    "<p>Florence could not send the iMessage confirmation yet, "
                    "but the connection was saved and delivery will show in the "
                    "pilot check.</p>"
                    if confirmation_failed
                    else ""
                }
                {
                    "<p>The household is paused, so Florence saved the connection "
                    "without texting the iMessage thread.</p>"
                    if confirmation_skipped_stopped
                    else ""
                }
              </body>
            </html>
            """
        )

    @app.get("/dev/connected-accounts/{chat_id}", dependencies=[Depends(dev_guard)])
    async def dev_connected_accounts(chat_id: str) -> dict[str, Any]:
        require_household(chat_id)
        return {"ok": True, "accounts": [asdict(account) for account in service.connected_accounts(chat_id=chat_id)]}

    @app.get("/dev/source-review/{chat_id}", dependencies=[Depends(dev_guard)])
    async def dev_source_review(chat_id: str) -> dict[str, Any]:
        require_household(chat_id)
        return {"ok": True, "snapshot": asdict(service.source_review_snapshot(chat_id=chat_id))}

    @app.get("/dev/source-preferences/{chat_id}", dependencies=[Depends(dev_guard)])
    async def dev_source_preferences(chat_id: str) -> dict[str, Any]:
        require_household(chat_id)
        return {"ok": True, "preferences": [asdict(item) for item in service.source_preferences(chat_id=chat_id)]}

    @app.get("/dev/memory/{chat_id}", dependencies=[Depends(dev_guard)])
    async def dev_memory(chat_id: str) -> dict[str, Any]:
        require_household(chat_id)
        return {"ok": True, "snapshot": asdict(service.memory_snapshot(chat_id=chat_id))}

    @app.get("/dev/privacy/{chat_id}", dependencies=[Depends(dev_guard)])
    async def dev_privacy(chat_id: str) -> dict[str, Any]:
        require_household(chat_id)
        return {"ok": True, "privacy": asdict(service.privacy_snapshot(chat_id=chat_id))}

    @app.get("/dev/readiness/{chat_id}", dependencies=[Depends(dev_guard)])
    async def dev_readiness(chat_id: str) -> dict[str, Any]:
        require_household(chat_id)
        return {"ok": True, "readiness": asdict(service.readiness_snapshot(chat_id=chat_id))}

    @app.get("/dev/deployment-check", dependencies=[Depends(dev_guard)])
    async def dev_deployment_check() -> dict[str, Any]:
        return {
            "ok": True,
            "deployment": _deployment_check(
                settings=resolved,
                store=service.store,
            ),
        }

    @app.get("/dev/live-verifications", dependencies=[Depends(dev_guard)])
    async def dev_live_verifications() -> dict[str, Any]:
        return {
            "ok": True,
            "verifications": _proof_safe_value(
                service.store.list_live_verifications(),
                settings=resolved,
            ),
        }

    @app.post("/dev/live-verifications/{name}", dependencies=[Depends(dev_guard)])
    async def dev_record_live_verification(
        name: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        now = _parse_dt(payload.get("now_utc")) or app_now()
        return {
            "ok": True,
            "verification": _record_live_verification(
                store=service.store,
                settings=resolved,
                name=name,
                proof=_required_text(payload, "proof"),
                verified_at_utc=_parse_optional_dt(payload, "verified_at_utc") or now,
                source="operator",
                now_utc=now,
            ),
        }

    def build_pilot_check_payload(chat_id: str) -> dict[str, Any]:
        household = require_household(chat_id)
        now = app_now()
        readiness = service.readiness_snapshot(chat_id=chat_id)
        deployment = _deployment_check(settings=resolved, store=service.store)
        message_transport = service.store.message_transport_summary(household_id=household.id)
        source_review = service.source_review_snapshot(chat_id=chat_id)
        connected_accounts = service.store.connected_account_auth_summary(household_id=household.id)
        delivery = service.store.outbound_delivery_summary(household_id=household.id)
        actions = service.store.action_execution_summary(
            household_id=household.id,
            now_utc=now,
        )
        smoke_checklist = _pilot_smoke_checklist(
            readiness=readiness,
            deployment=deployment,
            message_transport=message_transport,
            source_review=source_review,
            connected_accounts=connected_accounts,
            delivery=delivery,
            actions=actions,
        )
        operator_next_steps = _pilot_operator_next_steps(
            deployment=deployment,
            smoke_checklist=smoke_checklist,
        )
        return {
            "ok": True,
            "pilot_ready": (
                readiness.ready
                and deployment["ready"]
                and bool(message_transport["ready"])
                and _message_transport_proof_recent(message_transport=message_transport, deployment=deployment)
                and source_review.total > 0
                and source_review.surfaced > 0
                and source_review.token_backed_google_total > 0
                and source_review.token_backed_google_surfaced > 0
                and connected_accounts["token_backed_google"] > 0
                and bool(delivery["ready"])
                and bool(actions["ready"])
                and smoke_checklist["ready"]
            ),
            "household": asdict(readiness),
            "deployment": deployment,
            "message_transport": message_transport,
            "source_review": asdict(source_review),
            "connected_accounts": connected_accounts,
            "delivery": delivery,
            "actions": actions,
            "smoke_checklist": smoke_checklist,
            "operator_next_steps": operator_next_steps,
        }

    @app.get("/dev/pilot-check/{chat_id}", dependencies=[Depends(dev_guard)])
    async def dev_pilot_check(chat_id: str) -> dict[str, Any]:
        return build_pilot_check_payload(chat_id)

    @app.get("/dev/pilot-proof/{chat_id}", dependencies=[Depends(dev_guard)])
    async def dev_pilot_proof(chat_id: str) -> dict[str, Any]:
        pilot_check = build_pilot_check_payload(chat_id)
        source_review = pilot_check["source_review"]
        action_executions = service.action_executions(chat_id=chat_id, limit=10)
        return {
            "ok": True,
            "proof": {
                "generated_at_utc": app_now().isoformat(),
                "chat_id": chat_id,
                "pilot_ready": pilot_check["pilot_ready"],
                "sanitization": {
                    "message_bodies": "excluded",
                    "source_bodies": "excluded",
                    "source_titles": "excluded",
                    "source_event_times": "presence_only",
                    "oauth_tokens": "excluded",
                    "memory_text": "excluded",
                    "action_errors": "presence_only",
                    "diagnostic_strings": "redacted",
                },
                "pilot_check": {
                    "pilot_ready": pilot_check["pilot_ready"],
                    "household": _proof_safe_value(pilot_check["household"], settings=resolved),
                    "message_transport": _proof_safe_value(
                        pilot_check["message_transport"],
                        settings=resolved,
                    ),
                    "connected_accounts": _proof_safe_value(
                        pilot_check["connected_accounts"],
                        settings=resolved,
                    ),
                    "delivery": _proof_safe_value(pilot_check["delivery"], settings=resolved),
                    "actions": _proof_action_summary(pilot_check["actions"]),
                    "smoke_checklist": _proof_safe_value(
                        pilot_check["smoke_checklist"],
                        settings=resolved,
                    ),
                    "operator_next_steps": _proof_safe_value(
                        pilot_check["operator_next_steps"],
                        settings=resolved,
                    ),
                },
                "deployment": {
                    "ready": pilot_check["deployment"]["ready"],
                    "missing_required": _proof_safe_value(
                        pilot_check["deployment"]["missing_required"],
                        settings=resolved,
                    ),
                    "invalid": _proof_safe_value(pilot_check["deployment"]["invalid"], settings=resolved),
                    "database": _proof_safe_value(
                        pilot_check["deployment"]["database"],
                        settings=resolved,
                    ),
                    "database_backend": _proof_safe_value(
                        pilot_check["deployment"]["database_backend"],
                        settings=resolved,
                    ),
                    "hermes_ref_matches": _proof_safe_value(
                        pilot_check["deployment"]["hermes_ref_matches"],
                        settings=resolved,
                    ),
                    "hermes_toolsets": _proof_safe_value(
                        pilot_check["deployment"]["hermes_toolsets"],
                        settings=resolved,
                    ),
                    "hermes_strict": pilot_check["deployment"]["hermes_strict"],
                    "hermes_runtime_scope": _proof_safe_value(
                        pilot_check["deployment"]["hermes_runtime_scope"],
                        settings=resolved,
                    ),
                    "hermes_failure_cleanup": _proof_safe_value(
                        pilot_check["deployment"]["hermes_failure_cleanup"],
                        settings=resolved,
                    ),
                    "hermes_runtime_cleanup": _proof_safe_value(
                        pilot_check["deployment"]["hermes_runtime_cleanup"],
                        settings=resolved,
                    ),
                    "hermes_runtime_concurrency": _proof_safe_value(
                        pilot_check["deployment"]["hermes_runtime_concurrency"],
                        settings=resolved,
                    ),
                    "hermes_runtime_lock": _proof_safe_value(
                        pilot_check["deployment"]["hermes_runtime_lock"],
                        settings=resolved,
                    ),
                    "hermes_module_cache_scope": _proof_safe_value(
                        pilot_check["deployment"]["hermes_module_cache_scope"],
                        settings=resolved,
                    ),
                    "live_verification": _proof_safe_value(
                        pilot_check["deployment"]["live_verification"],
                        settings=resolved,
                    ),
                    "operator_next_steps": _proof_safe_value(
                        pilot_check["deployment"]["operator_next_steps"],
                        settings=resolved,
                    ),
                },
                "source_review": {
                    "total": source_review["total"],
                    "surfaced": source_review["surfaced"],
                    "connected_total": source_review["connected_total"],
                    "connected_surfaced": source_review["connected_surfaced"],
                    "token_backed_google_total": source_review["token_backed_google_total"],
                    "token_backed_google_surfaced": source_review[
                        "token_backed_google_surfaced"
                    ],
                    "latest_token_backed_google_synced_at_utc": source_review[
                        "latest_token_backed_google_synced_at_utc"
                    ],
                    "stored_only": source_review["stored_only"],
                    "suppressed": source_review["suppressed"],
                    "by_reason": source_review["by_reason"],
                    "recent_surfaced": _proof_source_items(source_review["recent_surfaced"]),
                    "recent_stored": _proof_source_items(source_review["recent_stored"]),
                },
                "privacy": asdict(service.privacy_snapshot(chat_id=chat_id)),
                "action_executions": [
                    {
                        "id": item.id,
                        "action_id": item.action_id,
                        "status": item.status.value,
                        "attempted_at_utc": item.attempted_at_utc.isoformat(),
                        "error_present": bool(item.error),
                    }
                    for item in action_executions
                ],
            },
        }

    @app.post("/dev/hermes-smoke/{chat_id}", dependencies=[Depends(dev_guard)])
    async def dev_hermes_smoke(
        chat_id: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        require_household(chat_id)
        payload = payload or {}
        now = _parse_dt(payload.get("now_utc")) or app_now()
        hermes_status = _hermes_runtime_status(resolved)
        try:
            response = service.agent_smoke_check(
                chat_id=chat_id,
                now_utc=now,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            return {
                "ok": False,
                "response": None,
                "response_present": False,
                "response_chars": 0,
                "used_fallback": False,
                "live_hermes_verified": False,
                "sanitization": {"response": "excluded"},
                "error": _hermes_smoke_error(exc, settings=resolved),
                "hermes": hermes_status,
            }
        used_fallback = response.strip() == tone.fallback_reply()
        live_hermes_verified = (
            resolved.database_backend == "postgres"
            and not used_fallback
            and bool(hermes_status["ready_for_saas_pilot"])
        )
        stored_live_verification = None
        if live_hermes_verified:
            stored_live_verification = _record_live_verification(
                store=service.store,
                settings=resolved,
                name="hermes",
                proof="Hermes smoke endpoint returned live_hermes_verified true without fallback",
                verified_at_utc=now,
                source="hermes_smoke",
                now_utc=now,
            )
        return {
            "ok": True,
            "response": None,
            "response_present": bool(response.strip()),
            "response_chars": len(response),
            "used_fallback": used_fallback,
            "live_hermes_verified": live_hermes_verified,
            "stored_live_verification": stored_live_verification,
            "sanitization": {"response": "excluded"},
            "hermes": hermes_status,
        }

    @app.get("/dev/hermes-status", dependencies=[Depends(dev_guard)])
    async def dev_hermes_status() -> dict[str, Any]:
        return {"ok": True, "hermes": _hermes_runtime_status(resolved)}

    @app.delete("/dev/memory/{chat_id}/{memory_id}", dependencies=[Depends(dev_guard)])
    async def dev_delete_memory(chat_id: str, memory_id: str) -> dict[str, Any]:
        require_household(chat_id)
        deleted = service.delete_memory(chat_id=chat_id, memory_id=memory_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="memory_not_found")
        return {"ok": True}

    @app.post("/dev/actions", dependencies=[Depends(dev_guard)])
    async def dev_create_action(payload: dict[str, Any]) -> dict[str, Any]:
        action_payload = payload.get("payload")
        now = _parse_dt(payload.get("now_utc")) or app_now()
        outbound = service.create_pending_action(
            chat_id=_dev_chat_id(payload, resolved),
            action_type=str(payload.get("action_type") or "external_action"),
            summary=str(payload.get("summary") or ""),
            payload=action_payload if isinstance(action_payload, dict) else {},
            sender=payload.get("sender"),
            now_utc=now,
            expires_at_utc=_parse_dt(payload.get("expires_at_utc")),
        )
        _send_all(linq, outbound, service=service, now_utc=now)
        return {"ok": True, "messages": [_as_response(message) for message in outbound]}

    @app.post("/dev/actions/tick", dependencies=[Depends(dev_guard)])
    async def dev_actions_tick(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        now = _parse_dt(payload.get("now_utc")) or app_now()
        result = run_approved_actions(
            store=service.store,
            sender=linq,
            now_utc=now,
        )
        return {
            "ok": True,
            "attempted": result.attempted,
            "succeeded": result.succeeded,
            "failed": result.failed,
        }

    @app.get("/dev/actions/{chat_id}", dependencies=[Depends(dev_guard)])
    async def dev_pending_actions(chat_id: str) -> dict[str, Any]:
        require_household(chat_id)
        return {
            "ok": True,
            "actions": [
                asdict(action)
                for action in service.pending_actions(
                    chat_id=chat_id,
                    now_utc=app_now(),
                )
            ],
        }

    @app.get("/dev/actions/{chat_id}/executions", dependencies=[Depends(dev_guard)])
    async def dev_action_executions(chat_id: str) -> dict[str, Any]:
        require_household(chat_id)
        return {"ok": True, "executions": [asdict(item) for item in service.action_executions(chat_id=chat_id)]}

    @app.post("/dev/reminders/tick", dependencies=[Depends(dev_guard)])
    async def dev_reminders_tick(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        now = _parse_dt(payload.get("now_utc")) or app_now()
        outbound = service.due_reminder_messages(
            now_utc=now,
            mark_sent=False,
        )
        _send_all(linq, outbound, service=service, now_utc=now)
        return {"ok": True, "messages": [_as_response(message) for message in outbound]}

    @app.post("/dev/routines/tick", dependencies=[Depends(dev_guard)])
    async def dev_routines_tick(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        now = _parse_dt(payload.get("now_utc")) or app_now()
        result = run_routine_tick(
            service,
            linq,
            now_utc=now,
        )
        return {
            "ok": True,
            "sent": result.sent,
            "delivery_failed": result.delivery_failed,
            "reminder_messages": result.reminder_messages,
            "briefing_messages": result.briefing_messages,
            "action_attempts": result.action_attempts,
            "action_succeeded": result.action_succeeded,
            "action_failed": result.action_failed,
        }

    @app.post("/dev/linq/reconcile", dependencies=[Depends(dev_guard)])
    async def dev_linq_reconcile(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        now = _parse_dt(payload.get("now_utc")) or app_now()
        result = run_linq_reconciliation_tick(
            service,
            linq,
            now_utc=now,
            chat_limit=_bounded_int(payload.get("chat_limit"), default=25, minimum=1, maximum=100),
            messages_per_chat=_bounded_int(
                payload.get("messages_per_chat"),
                default=20,
                minimum=1,
                maximum=100,
            ),
            since_utc=_parse_dt(payload.get("since_utc")),
        )
        return {"ok": True, "result": asdict(result)}

    return app


async def _request_data(request: Request) -> dict[str, object]:
    content_type = request.headers.get("content-type", "").lower()
    if "application/json" in content_type:
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="object_payload_required")
        return payload
    raw = await request.body()
    parsed = parse_qs(raw.decode("utf-8"), keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


def _bounded_int(value: object, *, default: int, minimum: int, maximum: int) -> int:
    if value in (None, ""):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="invalid_integer") from None
    return max(minimum, min(maximum, parsed))


def _render_onboarding_form(
    *,
    token: str,
    claims: OnboardingTokenClaims,
    household: Household,
    actor: HouseholdMember,
    readiness,
    memory_texts: list[str],
    form_values: dict[str, object],
    connected_account_count: int,
    google_configured: bool,
    google_missing: list[str],
    google_status: str | None,
) -> str:
    title = "Partner Setup" if claims.role == "partner" else "Household Setup"
    initial_step = _onboarding_initial_step(
        connected_account_count=connected_account_count,
        google_status=google_status,
    )
    partner_field = ""
    if claims.role == "primary":
        partner_field = """
          <label>
            Partner phone
            <input name="partner_phone" autocomplete="tel" placeholder="+1 555 555 0101" value="{partner_phone}">
          </label>
        """.format(partner_phone=_e(_form_text(form_values, "partner_phone")))
    current_context = _render_current_context(memory_texts)
    google_block = _render_google_block(
        token=token,
        google_configured=google_configured,
        google_missing=google_missing,
        connected_account_count=connected_account_count,
    )
    child_values = _form_children(form_values)
    child_rows = "\n".join(_render_child_row(index, child_values[index - 1]) for index in range(1, 5))
    tone_value = _form_text(form_values, "tone_preference")
    tone_options = _render_tone_options(tone_value)
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
	    <title>Florence {title}</title>
	    {_onboarding_css()}
	  </head>
	  <body>
	    <main data-initial-step="{_e(initial_step)}">
	      <header>
	        <p class="eyebrow">Florence</p>
	        <h1>{_e(title)}</h1>
	        <p class="lede">{_e(_onboarding_lede(claims.role))}</p>
	      </header>
	      {_render_google_connected_notice(google_status)}
	      <nav class="stepper" aria-label="Setup progress">
	        <button type="button" data-step-target="sources">Sources</button>
	        <button type="button" data-step-target="household">Household</button>
	        <button type="button" data-step-target="kids">Kids</button>
	        <button type="button" data-step-target="care">Care</button>
	        <button type="button" data-step-target="preferences">Preferences</button>
	        <button type="button" data-step-target="review">Review</button>
	      </nav>
	      <form method="post" action="/onboarding/{_e(token)}">
	        <section {_step_attrs("sources", initial_step)}>
	          <p class="step-count">Step 1 of 6</p>
	          <h2>Connect sources</h2>
	          {google_block}
	          <div class="actions">
	            <button type="button" data-next>Continue</button>
	          </div>
	        </section>
	        <section {_step_attrs("household", initial_step)}>
	          <p class="step-count">Step 2 of 6</p>
	          <h2>Household</h2>
	          <div class="grid">
	            <label>
	              Your name
	              <input name="parent_name" autocomplete="name" value="{_e(_form_text(form_values, "parent_name") or actor.display_name or "")}">
            </label>
            <label>
              Family or household name
              <input name="household_name" placeholder="Barasu household" value="{_e(_form_text(form_values, "household_name"))}">
            </label>
            {partner_field}
            <label>
              Location
              <input name="location" placeholder="City, neighborhood, or school area" value="{_e(_form_text(form_values, "location"))}">
	            </label>
	          </div>
	          <div class="actions">
	            <button type="button" class="secondary" data-back>Back</button>
	            <button type="button" data-next>Continue</button>
	          </div>
	        </section>
	        <section {_step_attrs("kids", initial_step)}>
	          <p class="step-count">Step 3 of 6</p>
	          <h2>Kids</h2>
	          {child_rows}
	          <div class="actions">
	            <button type="button" class="secondary" data-back>Back</button>
	            <button type="button" data-next>Continue</button>
	          </div>
	        </section>
	        <section {_step_attrs("care", initial_step)}>
	          <p class="step-count">Step 4 of 6</p>
	          <h2>Support system</h2>
	          <div class="grid">
	            <label>
	              Pets
	              <textarea name="pets" rows="4" placeholder="One per line">{_e(_form_text(form_values, "pets"))}</textarea>
            </label>
            <label>
              Other caretakers
              <textarea name="caretakers" rows="4" placeholder="Nanny, grandparent, neighbor, etc. One per line.">{_e(_form_text(form_values, "caretakers"))}</textarea>
	            </label>
	          </div>
	          <div class="actions">
	            <button type="button" class="secondary" data-back>Back</button>
	            <button type="button" data-next>Continue</button>
	          </div>
	        </section>
	        <section {_step_attrs("preferences", initial_step)}>
	          <p class="step-count">Step 5 of 6</p>
	          <h2>Florence preferences</h2>
	          <div class="grid">
	            <label>
	              Tone
	              <select name="tone_preference">
                {tone_options}
              </select>
            </label>
            <label>
              Always worth a text
              <textarea name="source_rule" rows="4" placeholder="Permission slips&#10;Schedule changes&#10;Medicine forms">{_e(_form_text(form_values, "source_rule"))}</textarea>
            </label>
          </div>
          <label>
	              Household book notes
	            <textarea name="family_context" rows="4" placeholder="Stable context Florence should keep visible for this household.">{_e(_form_text(form_values, "family_context"))}</textarea>
	          </label>
	          <div class="actions">
	            <button type="button" class="secondary" data-back>Back</button>
	            <button type="button" data-next>Continue</button>
	          </div>
	        </section>
	        <section {_step_attrs("review", initial_step)}>
	          <p class="step-count">Step 6 of 6</p>
	          <h2>Review</h2>
	          <p>Household {readiness.parent_count}/2 parents, {readiness.child_count} children, {connected_account_count} connected sources.</p>
	          {current_context}
	          <div class="actions">
	            <button type="button" class="secondary" data-back>Back</button>
	            <button type="submit">Save Setup</button>
	          </div>
	        </section>
	      </form>
	      <footer>
	        <p>Household {readiness.parent_count}/2 parents, {readiness.child_count} children, {connected_account_count} connected sources.</p>
	      </footer>
	    </main>
	    {_onboarding_script()}
	  </body>
	</html>"""


def _render_onboarding_done(
    *,
    token: str,
    claims: OnboardingTokenClaims,
    household: Household,
    actor: HouseholdMember,
    result: dict[str, object],
    readiness,
    memory_texts: list[str],
    source_preferences: list[str],
    google_configured: bool,
) -> str:
    invite = str(result.get("invite_text") or "")
    invite_block = ""
    if invite:
        invite_block = f"""
        <section class="band">
          <h2>Partner Invite</h2>
          <textarea readonly rows="4">{_e(invite)}</textarea>
        </section>
        """
    google_block = ""
    if google_configured:
        google_block = f"""<a class="secondary" href="/onboarding/{_e(token)}/google">Connect Google</a>"""
    missing = "" if readiness.ready else "".join(f"<li>{_e(item)}</li>" for item in readiness.missing[:4])
    missing_block = "" if readiness.ready else f"<ul>{missing}</ul>"
    saved_context = _render_current_context(memory_texts)
    saved_rules = _render_source_preference_review(source_preferences)
    saved_details = ""
    if saved_context or saved_rules:
        saved_details = f"""
        <section class="band">
          <h2>Saved Details</h2>
          {saved_context}
          {saved_rules}
        </section>
        """
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Florence Setup Saved</title>
    {_onboarding_css()}
  </head>
  <body>
    <main>
      <header>
        <p class="eyebrow">Florence</p>
        <h1>Setup Saved</h1>
        <p class="lede">I saved this household setup for {_e(actor.display_name or actor.phone)}.</p>
      </header>
      <section class="band">
        <h2>Status</h2>
        <p>{_e(str(result.get("saved_memory_count", 0)))} household book details saved. {_e(str(result.get("saved_source_preference_count", 0)))} source rules saved.</p>
        <p>Parents: {readiness.parent_count}/2. Children: {readiness.child_count}. Connected sources: {readiness.connected_account_count}.</p>
        {missing_block}
      </section>
      {saved_details}
      {invite_block}
      <div class="actions">
        {google_block}
        <a class="secondary" href="/onboarding/{_e(token)}">Edit setup</a>
      </div>
    </main>
  </body>
</html>"""


def _render_onboarding_google_missing(missing: list[str]) -> str:
    items = "".join(f"<li>{_e(item)}</li>" for item in missing)
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Florence Google Setup</title>
    {_onboarding_css()}
  </head>
  <body>
    <main>
      <header>
        <p class="eyebrow">Florence</p>
        <h1>Google Is Not Configured</h1>
      </header>
      <section class="band">
        <p>This deployment still needs Google OAuth settings.</p>
        <ul>{items}</ul>
      </section>
    </main>
  </body>
</html>"""


def _render_google_block(
    *,
    token: str,
    google_configured: bool,
    google_missing: list[str],
    connected_account_count: int,
) -> str:
    if google_configured:
        status = (
            f"{connected_account_count} Google account(s) connected for this household."
            if connected_account_count
            else "No Google accounts connected yet."
        )
        return (
            f"<p>{_e(status)}</p>"
            f'<a class="primary" href="/onboarding/{_e(token)}/google">Connect Google Calendar and Gmail</a>'
        )
    missing = ", ".join(google_missing)
    return f"<p>Google connection is not configured for this deployment yet. Missing: {_e(missing)}.</p>"


def _onboarding_form_values(
    *,
    actor: HouseholdMember,
    members: list[HouseholdMember],
    memories: list[Any],
    source_preferences: list[Any],
) -> dict[str, object]:
    values: dict[str, object] = {
        "parent_name": actor.display_name or "",
        "partner_phone": _partner_phone_for_form(actor=actor, members=members),
        "household_name": "",
        "location": "",
        "pets": "",
        "caretakers": "",
        "tone_preference": "",
        "source_rule": "\n".join(str(item.phrase) for item in source_preferences if str(item.phrase).strip()),
        "family_context": "",
        "children": [],
    }
    pets: list[str] = []
    caretakers: list[str] = []
    children: list[dict[str, str]] = []
    for memory in memories:
        text = str(getattr(memory, "text", "") or "")
        if text.startswith("Household name:"):
            values["household_name"] = _strip_prefixed_sentence(text, "Household name:")
        elif text.startswith("Household location:"):
            values["location"] = _strip_prefixed_sentence(text, "Household location:")
        elif text.startswith("Pet:"):
            pets.append(_strip_prefixed_sentence(text, "Pet:"))
        elif text.startswith("Caretaker:"):
            caretakers.append(_strip_prefixed_sentence(text, "Caretaker:"))
        elif text.startswith("Household context:"):
            values["family_context"] = _strip_prefixed_sentence(text, "Household context:")
        elif text.startswith("Tone preference for "):
            values["tone_preference"] = _strip_prefixed_sentence(text.partition(":")[2], "")
        elif text.startswith("Child profile:"):
            children.append(_parse_child_memory(text=text, subject=getattr(memory, "subject", None)))
        else:
            children.extend(_parse_natural_children_memory(text))
    values["pets"] = "\n".join(item for item in pets if item)
    values["caretakers"] = "\n".join(item for item in caretakers if item)
    values["children"] = children[:4]
    return values


def _partner_phone_for_form(*, actor: HouseholdMember, members: list[HouseholdMember]) -> str:
    for member in members:
        if member.id != actor.id and member.role == MemberRole.PARENT:
            return member.phone
    return ""


def _strip_prefixed_sentence(text: str, prefix: str) -> str:
    stripped = text.removeprefix(prefix).strip()
    return stripped[:-1].strip() if stripped.endswith(".") else stripped


def _parse_child_memory(*, text: str, subject: object) -> dict[str, str]:
    child = {
        "name": str(subject or "").strip(),
        "age": "",
        "grade": "",
        "school": "",
        "activities": "",
        "location": "",
    }
    body = _strip_prefixed_sentence(text, "Child profile:")
    parts = [part.strip() for part in body.split(";") if part.strip()]
    if parts and not child["name"]:
        child["name"] = parts[0]
    for part in parts[1:]:
        if part.startswith("school/activity location "):
            child["location"] = part.removeprefix("school/activity location ").strip()
        elif part.startswith("activities "):
            child["activities"] = part.removeprefix("activities ").strip()
        elif part.startswith("school "):
            child["school"] = part.removeprefix("school ").strip()
        elif part.startswith("grade "):
            child["grade"] = part.removeprefix("grade ").strip()
        elif part.startswith("age "):
            child["age"] = part.removeprefix("age ").strip()
    return child


def _parse_natural_children_memory(text: str) -> list[dict[str, str]]:
    match = re.match(r"^(?:.+?['’]s\s+)?(?:children|kids)\s+are\s+(.+)$", text.strip(" ."), flags=re.IGNORECASE)
    if match is None:
        return []
    children: list[dict[str, str]] = []
    for entry in re.finditer(
        r"(?P<name>[A-Z][A-Za-z' -]*?)(?:,?\s*age\s+(?P<age>\d{1,2}|[A-Za-z]+))?"
        r"(?=(?:,\s*(?:and\s+)?[A-Z])|(?:\s+and\s+[A-Z])|$)",
        match.group(1).strip(" ."),
    ):
        name = " ".join(entry.group("name").strip(" ,").split())
        if not name:
            continue
        children.append(
            {
                "name": name,
                "age": entry.group("age") or "",
                "grade": "",
                "school": "",
                "activities": "",
                "location": "",
            }
        )
    return children


def _form_text(values: dict[str, object], key: str) -> str:
    value = values.get(key)
    return value if isinstance(value, str) else ""


def _form_children(values: dict[str, object]) -> list[dict[str, str]]:
    raw = values.get("children")
    children = raw if isinstance(raw, list) else []
    normalized: list[dict[str, str]] = []
    for child in children[:4]:
        if not isinstance(child, dict):
            continue
        normalized.append({key: str(child.get(key) or "") for key in ("name", "age", "grade", "school", "activities", "location")})
    while len(normalized) < 4:
        normalized.append({"name": "", "age": "", "grade": "", "school": "", "activities": "", "location": ""})
    return normalized


def _render_tone_options(current: str) -> str:
    options = (
        ("Warm, concise, and practical", "Warm, concise, practical"),
        ("Very brief and direct", "Very brief"),
        ("Extra warm and reassuring", "Extra warm"),
        ("Detailed when something affects schedule or logistics", "More detailed for logistics"),
    )
    rendered: list[str] = []
    matched = False
    for value, label in options:
        selected = " selected" if current == value else ""
        matched = matched or bool(selected)
        rendered.append(f'<option value="{_e(value)}"{selected}>{_e(label)}</option>')
    if current and not matched:
        rendered.insert(0, f'<option value="{_e(current)}" selected>{_e(current)}</option>')
    return "\n".join(rendered)


def _render_source_preference_review(source_preferences: list[str]) -> str:
    phrases = [phrase for phrase in source_preferences if phrase.strip()]
    if not phrases:
        return ""
    items = "".join(f"<li>{_e(phrase)}</li>" for phrase in phrases)
    return f"""
      <div class="review-block">
        <h3>Always Worth A Text</h3>
        <ul>{items}</ul>
      </div>
    """


def _render_google_connected_notice(google_status: str | None) -> str:
    if google_status != "connected":
        return ""
    return '<p class="notice">Google is connected. Continue with the household details.</p>'


def _onboarding_initial_step(*, connected_account_count: int, google_status: str | None) -> str:
    if google_status == "connected":
        return "household"
    if connected_account_count <= 0:
        return "sources"
    return "household"


def _step_attrs(step: str, initial_step: str) -> str:
    hidden = "" if step == initial_step else " hidden"
    return f'class="band step" data-step="{_e(step)}"{hidden}'


def _safe_return_path(path: str | None) -> str | None:
    if path is None:
        return None
    stripped = path.strip()
    if not stripped or "\r" in stripped or "\n" in stripped:
        return None
    if not stripped.startswith("/") or stripped.startswith("//"):
        return None
    return stripped


def _render_child_row(index: int, child: dict[str, str]) -> str:
    return f"""
      <div class="child-row">
        <label>
          Child {index} name
          <input name="child_{index}_name" value="{_e(child.get("name", ""))}">
        </label>
        <label>
          Age
          <input name="child_{index}_age" value="{_e(child.get("age", ""))}">
        </label>
        <label>
          Grade
          <input name="child_{index}_grade" value="{_e(child.get("grade", ""))}">
        </label>
        <label>
          School
          <input name="child_{index}_school" value="{_e(child.get("school", ""))}">
        </label>
        <label>
          Activities
          <input name="child_{index}_activities" value="{_e(child.get("activities", ""))}">
        </label>
        <label>
          Locations
          <input name="child_{index}_location" value="{_e(child.get("location", ""))}">
        </label>
      </div>
    """


def _render_current_context(memory_texts: list[str]) -> str:
    if not memory_texts:
        return ""
    items = "".join(f"<li>{_e(text)}</li>" for text in memory_texts)
    return f"""
      <div class="review-block">
        <h3>Household Book</h3>
        <ul>{items}</ul>
      </div>
    """


def _onboarding_lede(role: str) -> str:
    if role == "partner":
        return "Connect your sources, confirm the household book, and set how Florence should talk to you."
    return "Build the household book once so Florence can be useful without dumping noisy email or calendar backfill into iMessage."


def _onboarding_css() -> str:
    return """
    <style>
      :root { color-scheme: light; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
      body { margin: 0; background: #f7f8fb; color: #172033; }
      main { max-width: 920px; margin: 0 auto; padding: 32px 20px 48px; }
      header { padding: 24px 0 16px; }
      h1 { font-size: 2.7rem; line-height: 1.04; margin: 0; letter-spacing: 0; }
      h2 { font-size: 1rem; margin: 0 0 16px; letter-spacing: 0; }
      h3 { font-size: .95rem; margin: 18px 0 8px; letter-spacing: 0; }
      p { line-height: 1.5; }
      .eyebrow { text-transform: uppercase; font-size: .78rem; letter-spacing: .08em; color: #6b7280; margin: 0 0 8px; }
      .lede { max-width: 720px; color: #4b5563; font-size: 1.05rem; }
      .notice { border-left: 4px solid #168a56; background: #eefaf3; border-radius: 6px; padding: 12px 14px; margin: 0 0 18px; color: #0f5132; font-weight: 650; }
      .stepper { display: flex; gap: 8px; overflow-x: auto; padding: 8px 0 18px; margin-bottom: 4px; }
      .stepper button { flex: 0 0 auto; border: 1px solid #cfd7e3; background: #fff; color: #475569; border-radius: 6px; padding: 8px 10px; font: inherit; font-size: .88rem; font-weight: 750; }
      .stepper button.is-active { background: #111827; border-color: #111827; color: #fff; }
      .band { border-top: 1px solid #d8dee8; padding: 24px 0; }
      .step[hidden] { display: none; }
      .step-count { color: #64748b; font-size: .82rem; font-weight: 750; margin: 0 0 8px; }
      .grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }
      .child-row { display: grid; grid-template-columns: 1.2fr .6fr .7fr 1.1fr 1.2fr 1.2fr; gap: 12px; margin-bottom: 12px; }
      label { display: grid; gap: 7px; color: #374151; font-size: .92rem; font-weight: 650; }
      input, textarea, select { width: 100%; box-sizing: border-box; border: 1px solid #cfd7e3; border-radius: 6px; padding: 11px 12px; background: #fff; color: #111827; font: inherit; font-weight: 450; }
      input:focus, textarea:focus, select:focus { outline: 2px solid #94a3b8; outline-offset: 1px; }
      textarea { resize: vertical; }
      ul { padding-left: 20px; color: #4b5563; }
      .actions { display: flex; gap: 12px; flex-wrap: wrap; padding-top: 18px; }
      .actions button, .primary, .secondary { border-radius: 6px; padding: 11px 16px; font: inherit; font-weight: 750; text-decoration: none; border: 1px solid #111827; display: inline-flex; align-items: center; justify-content: center; min-height: 44px; }
      .actions button, .primary { background: #111827; color: #fff; }
      .secondary { background: transparent; color: #111827; }
      .review-block { margin-top: 18px; }
      footer { color: #6b7280; font-size: .9rem; padding-top: 24px; }
      @media (max-width: 760px) {
        main { padding: 22px 14px 36px; }
        h1 { font-size: 2rem; }
        .grid, .child-row { grid-template-columns: 1fr; }
        .actions button, .primary, .secondary { width: 100%; }
      }
    </style>
    """


def _onboarding_script() -> str:
    return """
    <script>
      (() => {
        const root = document.querySelector("main[data-initial-step]");
        if (!root) return;
        const steps = Array.from(document.querySelectorAll("[data-step]"));
        const tabs = Array.from(document.querySelectorAll("[data-step-target]"));
        const order = steps.map((step) => step.dataset.step);

        const show = (stepId, shouldScroll = true) => {
          if (!order.includes(stepId)) stepId = order[0];
          steps.forEach((step) => {
            step.hidden = step.dataset.step !== stepId;
          });
          tabs.forEach((tab) => {
            const active = tab.dataset.stepTarget === stepId;
            tab.classList.toggle("is-active", active);
            tab.setAttribute("aria-current", active ? "step" : "false");
          });
          root.dataset.currentStep = stepId;
          if (shouldScroll) window.scrollTo({ top: 0, behavior: "smooth" });
        };

        document.addEventListener("click", (event) => {
          const target = event.target.closest("[data-next], [data-back], [data-step-target]");
          if (!target) return;
          if (target.dataset.stepTarget) {
            show(target.dataset.stepTarget);
            return;
          }
          const index = order.indexOf(root.dataset.currentStep);
          if (target.hasAttribute("data-next")) show(order[Math.min(index + 1, order.length - 1)]);
          if (target.hasAttribute("data-back")) show(order[Math.max(index - 1, 0)]);
        });

        document.addEventListener("submit", (event) => {
          if (root.dataset.currentStep === "review") return;
          event.preventDefault();
          const index = order.indexOf(root.dataset.currentStep);
          show(order[Math.min(index + 1, order.length - 1)]);
        });

        show(root.dataset.initialStep, false);
      })();
    </script>
    """


def _e(value: object) -> str:
    return html.escape(str(value), quote=True)


def _require_dev_access(settings: Settings, request: Request) -> None:
    if not settings.dev_endpoints_enabled:
        raise HTTPException(status_code=404, detail="not_found")
    if not settings.admin_api_key:
        if settings.database_backend == "postgres":
            raise HTTPException(status_code=401, detail="admin_api_key_required")
        return
    provided = _admin_key_from_request(request)
    if not provided or not hmac.compare_digest(provided, settings.admin_api_key):
        raise HTTPException(status_code=401, detail="admin_api_key_required")


def _require_source_ingest_access(settings: Settings, request: Request) -> None:
    if not settings.source_ingest_api_key:
        raise HTTPException(status_code=404, detail="not_found")
    provided = _source_ingest_key_from_request(request)
    if not provided or not hmac.compare_digest(provided, settings.source_ingest_api_key):
        raise HTTPException(status_code=401, detail="source_ingest_api_key_required")


def _source_ingest_key_from_request(request: Request) -> str | None:
    header_key = request.headers.get("x-florence-source-key")
    if header_key:
        return header_key.strip()
    return _bearer_token_from_request(request)


def _admin_key_from_request(request: Request) -> str | None:
    header_key = request.headers.get("x-florence-admin-key")
    if header_key:
        return header_key.strip()
    return _bearer_token_from_request(request)


def _bearer_token_from_request(request: Request) -> str | None:
    authorization = request.headers.get("authorization")
    if authorization is None:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token.strip()


def _send_all(
    client: LinqClient,
    outbound: list[OutboundMessage],
    *,
    service: FlorenceService,
    now_utc: datetime | None = None,
) -> list[dict[str, Any]]:
    now = ensure_utc(now_utc or _now())
    results: list[dict[str, Any]] = []
    for message in outbound:
        service.prepare_outbound_delivery(message, now_utc=now)
        try:
            if message.new_chat_to:
                if not message.new_chat_from:
                    raise RuntimeError("new chat message is missing sender phone")
                result = client.create_chat(
                    from_phone=message.new_chat_from,
                    to=message.new_chat_to,
                    text=message.text,
                    idempotency_key=message.idempotency_key,
                )
                new_chat_id = _created_chat_id(result)
                if (
                    new_chat_id
                    and not result.get("dry_run")
                    and message.migrate_household_id
                    and message.invited_partner_phone
                ):
                    service.complete_partner_group_created(
                        household_id=message.migrate_household_id,
                        new_chat_id=new_chat_id,
                        partner_phone=message.invited_partner_phone,
                        intro_text=message.text,
                    )
            else:
                result = client.send_text(
                    chat_id=message.chat_id,
                    text=message.text,
                    idempotency_key=message.idempotency_key,
                )
            results.append(
                result if isinstance(result, dict) else {"response_type": type(result).__name__}
            )
            service.store.mark_outbound_delivery_sent(
                idempotency_key=message.idempotency_key,
                now_utc=now,
            )
            service.mark_outbound_delivered(message, now_utc=now)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            if message.delivery_source_message_id:
                service.mark_outbound_delivery_failed(
                    message,
                    error=error,
                    now_utc=now,
                )
            else:
                service.store.mark_outbound_delivery_failed(
                    idempotency_key=message.idempotency_key,
                    error=error,
                    now_utc=now,
                )
            raise
    return results


def _maybe_record_linq_live_verification(
    *,
    service: FlorenceService,
    settings: Settings,
    client: object,
    send_results: list[dict[str, Any]],
    verified_at_utc: datetime,
) -> None:
    if settings.database_backend != "postgres" or service.store.backend != "postgres":
        return
    if not _linq_client_can_prove_live_send(client):
        return
    if not any(not result.get("dry_run") for result in send_results):
        return
    try:
        _record_live_verification(
            store=service.store,
            settings=settings,
            name="linq",
            proof="Linq webhook received inbound and outbound iMessage send succeeded",
            source="linq_webhook",
            verified_at_utc=verified_at_utc,
            now_utc=verified_at_utc,
        )
    except Exception:
        logger.warning("Florence could not record Linq live verification proof", exc_info=True)


def _linq_client_can_prove_live_send(client: object) -> bool:
    return isinstance(client, LinqClient) and bool(client.settings.linq_api_key)


def _created_chat_id(result: dict[str, Any]) -> str | None:
    chat = result.get("chat")
    if isinstance(chat, dict) and isinstance(chat.get("id"), str):
        return chat["id"]
    if isinstance(result.get("chat_id"), str):
        return result["chat_id"]
    if isinstance(result.get("id"), str):
        return result["id"]
    return None


def _as_response(message: OutboundMessage) -> dict[str, Any]:
    return asdict(message)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        return ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _parse_optional_dt(payload: dict[str, Any], field: str) -> datetime | None:
    if field not in payload or payload[field] is None or payload[field] == "":
        return None
    parsed = _parse_dt(payload[field])
    if parsed is None:
        raise HTTPException(status_code=400, detail=f"{field}_invalid")
    return parsed


def _required_text(payload: dict[str, Any], field: str) -> str:
    value = str(payload.get(field) or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail=f"{field}_required")
    return value


def _dev_chat_id(payload: dict[str, Any], settings: Settings) -> str:
    value = str(payload.get("chat_id") or "").strip()
    if value:
        return value
    if settings.database_backend == "postgres":
        raise HTTPException(status_code=400, detail="chat_id_required")
    return "dev-chat"


def _reject_too_long(field: str, value: str, limit: int) -> None:
    if len(value) > limit:
        raise HTTPException(status_code=413, detail=f"{field}_too_large")


def _pilot_smoke_checklist(
    *,
    readiness: Any,
    deployment: dict[str, Any],
    message_transport: dict[str, object],
    source_review: Any,
    connected_accounts: dict[str, int],
    delivery: dict[str, object],
    actions: dict[str, object],
) -> dict[str, Any]:
    live = deployment.get("live_verification") or {}
    evidence = live.get("evidence") or {}
    steps = [
        _smoke_step(
            "deployment_preflight",
            "Production configuration, Postgres, and Hermes preflight are ready.",
            bool(deployment.get("ready")),
            [
                *[f"missing:{item}" for item in deployment.get("missing_required", [])],
                *list(deployment.get("invalid", [])),
                *list(live.get("blocked", [])),
            ],
        ),
        _smoke_step(
            "two_parent_household_setup",
            "Two parents are named and at least one child is recorded.",
            bool(readiness.ready),
            list(readiness.missing),
            {
                "parent_count": readiness.parent_count,
                "named_parent_count": readiness.named_parent_count,
                "child_count": readiness.child_count,
                "timezone": readiness.timezone,
            },
        ),
        _smoke_step(
            "linq_message_transport",
            "The shared iMessage thread has fresh inbound and outbound Florence traffic.",
            bool(message_transport.get("ready"))
            and _message_transport_proof_recent(message_transport=message_transport, deployment=deployment),
            _message_transport_smoke_blockers(message_transport=message_transport, deployment=deployment),
            {
                "inbound": message_transport.get("inbound"),
                "outbound": message_transport.get("outbound"),
                "latest_inbound_at_utc": message_transport.get("latest_inbound_at_utc"),
                "latest_outbound_at_utc": message_transport.get("latest_outbound_at_utc"),
                "linq_verified_at_utc": (
                    _linq_live_verified_at(deployment).isoformat()
                    if _linq_live_verified_at(deployment)
                    else None
                ),
            },
        ),
        _live_smoke_step(
            "linq_live_round_trip",
            "A real Linq iMessage send/webhook round trip is verified.",
            evidence.get("linq") or {},
            fallback_blocker="Live Linq iMessage send and webhook round-trip",
        ),
        _smoke_step(
            "connected_source_account",
            "At least one Google email/calendar source is connected with an OAuth token.",
            connected_accounts.get("token_backed_google", 0) > 0,
            _connected_account_smoke_blockers(connected_accounts),
            {
                "connected_account_count": readiness.connected_account_count,
                "active_total": connected_accounts.get("active_total", 0),
                "active_google": connected_accounts.get("active_google", 0),
                "token_backed_google": connected_accounts.get("token_backed_google", 0),
            },
        ),
        _live_smoke_step(
            "google_live_source_sync",
            "A real Google OAuth connection and source sync are verified.",
            evidence.get("google") or {},
            fallback_blocker="Live Google OAuth connection and source sync",
        ),
        _smoke_step(
            "source_rule_and_need_to_know",
            "Need-to-Know triage has stored and surfaced at least one connected-source item.",
            (
                readiness.source_preference_count > 0
                and source_review.token_backed_google_total > 0
                and source_review.token_backed_google_surfaced > 0
                and _source_sync_proof_recent(source_review=source_review, deployment=deployment)
            ),
            _source_smoke_blockers(readiness=readiness, source_review=source_review, deployment=deployment),
            {
                "source_preference_count": readiness.source_preference_count,
                "total": source_review.total,
                "surfaced": source_review.surfaced,
                "connected_total": source_review.connected_total,
                "connected_surfaced": source_review.connected_surfaced,
                "token_backed_google_total": source_review.token_backed_google_total,
                "token_backed_google_surfaced": source_review.token_backed_google_surfaced,
                "latest_token_backed_google_synced_at_utc": (
                    source_review.latest_token_backed_google_synced_at_utc.isoformat()
                    if source_review.latest_token_backed_google_synced_at_utc
                    else None
                ),
                "google_verified_at_utc": (
                    _google_live_verified_at(deployment).isoformat()
                    if _google_live_verified_at(deployment)
                    else None
                ),
                "stored_only": source_review.stored_only,
                "suppressed": source_review.suppressed,
            },
        ),
        _smoke_step(
            "hermes_saas_boundary",
            "Hermes is pinned, tool-less, and scoped for SaaS turns.",
            _hermes_smoke_boundary_ready(deployment),
            _hermes_smoke_boundary_blockers(deployment),
            {
                "database_backend": deployment.get("database_backend"),
                "hermes_ref_matches": deployment.get("hermes_ref_matches"),
                "hermes_toolsets": deployment.get("hermes_toolsets"),
                "hermes_runtime_scope": deployment.get("hermes_runtime_scope"),
                "hermes_module_cache_scope": deployment.get("hermes_module_cache_scope"),
                "hermes_failure_cleanup": deployment.get("hermes_failure_cleanup"),
            },
        ),
        _live_smoke_step(
            "hermes_live_response",
            "A real Hermes response through Florence's adapter is verified.",
            evidence.get("hermes") or {},
            fallback_blocker="Live Hermes Agent response through Florence adapter",
        ),
        _smoke_step(
            "outbound_delivery_queue",
            "No pending or failed source/OAuth deliveries remain.",
            bool(delivery.get("ready")),
            [f"{delivery.get('retryable', 0)} retryable outbound deliveries remain"]
            if not bool(delivery.get("ready"))
            else [],
            {
                "pending": delivery.get("pending"),
                "failed": delivery.get("failed"),
                "sent": delivery.get("sent"),
                "retryable": delivery.get("retryable"),
            },
        ),
        _smoke_step(
            "approval_worker_queue",
            "At least one parent-approved action has executed and no action work is stuck.",
            bool(actions.get("ready")) and int(actions.get("succeeded") or 0) > 0,
            _action_smoke_blockers(actions),
            {
                "approved": actions.get("approved"),
                "failed": actions.get("failed"),
                "succeeded": actions.get("succeeded"),
            },
        ),
    ]
    blocked = [step["id"] for step in steps if not step["ready"]]
    return {"ready": not blocked, "blocked": blocked, "steps": steps}


def _pilot_operator_next_steps(
    *,
    deployment: dict[str, Any],
    smoke_checklist: dict[str, Any],
) -> list[dict[str, Any]]:
    deployment_steps = list(deployment.get("operator_next_steps") or [])
    next_steps: list[dict[str, Any]] = [*deployment_steps]
    deployment_step_ids = {str(step.get("id")) for step in deployment_steps}
    for step in smoke_checklist.get("steps", []):
        step_id = str(step.get("id") or "")
        if not step_id or bool(step.get("ready")):
            continue
        if step_id == "deployment_preflight" and deployment_step_ids:
            continue
        next_steps.append(
            {
                "id": step_id,
                "status": "pilot_smoke_blocked",
                "blocked_by": list(step.get("blocked_by") or []),
                "action": step.get("label"),
                "evidence": step.get("evidence") or {},
            }
        )
    return next_steps


def _smoke_step(
    step_id: str,
    label: str,
    ready: bool,
    blocked_by: list[str],
    evidence: dict[str, object] | None = None,
) -> dict[str, Any]:
    return {
        "id": step_id,
        "label": label,
        "ready": bool(ready),
        "blocked_by": [item for item in blocked_by if item],
        "evidence": evidence or {},
    }


def _live_smoke_step(
    step_id: str,
    label: str,
    evidence: dict[str, object],
    *,
    fallback_blocker: str,
) -> dict[str, Any]:
    missing = list(evidence.get("missing") or [])
    verified = bool(evidence.get("verified")) and not missing
    return _smoke_step(
        step_id,
        label,
        verified,
        missing if missing else ([] if verified else [fallback_blocker]),
        {
            "verified": bool(evidence.get("verified")),
            "verified_at_utc": evidence.get("verified_at_utc"),
            "proof": evidence.get("proof"),
        },
    )


def _message_transport_smoke_blockers(
    *,
    message_transport: dict[str, object],
    deployment: dict[str, Any],
) -> list[str]:
    blockers = list(message_transport.get("missing") or [])
    if not _message_transport_proof_recent(message_transport=message_transport, deployment=deployment):
        blockers.append("Send and receive a fresh Linq iMessage round trip for the smoke window.")
    return blockers


def _message_transport_proof_recent(
    *,
    message_transport: dict[str, object],
    deployment: dict[str, Any],
) -> bool:
    if not bool(message_transport.get("ready")):
        return True
    linq_verified_at = _linq_live_verified_at(deployment)
    if linq_verified_at is None:
        return True
    latest_inbound = _parse_dt(message_transport.get("latest_inbound_at_utc"))
    latest_outbound = _parse_dt(message_transport.get("latest_outbound_at_utc"))
    if latest_inbound is None or latest_outbound is None:
        return False
    earliest = linq_verified_at - PILOT_MESSAGE_TRANSPORT_MAX_PROOF_AGE
    latest = linq_verified_at + LIVE_VERIFICATION_MAX_FUTURE_SKEW
    return earliest <= latest_inbound <= latest and earliest <= latest_outbound <= latest


def _source_smoke_blockers(*, readiness: Any, source_review: Any, deployment: dict[str, Any]) -> list[str]:
    blockers = []
    if readiness.source_preference_count == 0:
        blockers.append("Set one source rule, like 'always tell me about permission slips'.")
    if source_review.token_backed_google_total == 0:
        blockers.append("Run a controlled Google OAuth-backed source sync item.")
    if source_review.token_backed_google_total > 0 and source_review.token_backed_google_surfaced == 0:
        blockers.append("Surface at least one Need-to-Know Google OAuth-backed source item to the household.")
    if not _source_sync_proof_recent(source_review=source_review, deployment=deployment):
        blockers.append("Run a fresh Google OAuth-backed source sync for the smoke window.")
    return blockers


def _source_sync_proof_recent(*, source_review: Any, deployment: dict[str, Any]) -> bool:
    if source_review.token_backed_google_total <= 0 or source_review.token_backed_google_surfaced <= 0:
        return True
    google_verified_at = _google_live_verified_at(deployment)
    if google_verified_at is None:
        return True
    synced_at = source_review.latest_token_backed_google_synced_at_utc
    if synced_at is None:
        return False
    synced_at = ensure_utc(synced_at)
    earliest = google_verified_at - PILOT_SOURCE_SYNC_MAX_PROOF_AGE
    latest = google_verified_at + LIVE_VERIFICATION_MAX_FUTURE_SKEW
    return earliest <= synced_at <= latest


def _google_live_verified_at(deployment: dict[str, Any]) -> datetime | None:
    return _live_verified_at(deployment, "google")


def _linq_live_verified_at(deployment: dict[str, Any]) -> datetime | None:
    return _live_verified_at(deployment, "linq")


def _live_verified_at(deployment: dict[str, Any], name: str) -> datetime | None:
    live = deployment.get("live_verification") or {}
    evidence = live.get("evidence") or {}
    item = evidence.get(name) or {}
    return _parse_dt(item.get("verified_at_utc"))


def _connected_account_smoke_blockers(connected_accounts: dict[str, int]) -> list[str]:
    if connected_accounts.get("active_google", 0) == 0:
        return ["Connect Google OAuth for the household."]
    if connected_accounts.get("token_backed_google", 0) == 0:
        return ["Complete Google OAuth so Florence has an encrypted token for source sync."]
    return []


def _hermes_smoke_boundary_ready(deployment: dict[str, Any]) -> bool:
    return (
        not any(str(item).startswith("FLORENCE_HERMES") for item in deployment.get("missing_required", []))
        and not _has_hermes_path_error(list(deployment.get("invalid", [])))
        and deployment.get("database_backend") == "postgres"
        and not deployment.get("hermes_toolsets")
        and bool(deployment.get("hermes_ref_matches"))
        and deployment.get("hermes_strict") is True
        and deployment.get("hermes_runtime_scope") == "per_turn_under_runtime_home"
        and deployment.get("hermes_runtime_lock") == HERMES_INTERPROCESS_LOCK_MODE
        and deployment.get("hermes_module_cache_scope") == "shadowed_and_cleared_during_hermes_import_or_call"
        and deployment.get("hermes_failure_cleanup") == HERMES_FAILURE_CLEANUP
    )


def _hermes_smoke_boundary_blockers(deployment: dict[str, Any]) -> list[str]:
    blockers = []
    blockers.extend(
        str(item)
        for item in deployment.get("missing_required", [])
        if str(item).startswith("FLORENCE_HERMES") or str(item) == "HERMES_AGENT_REF"
    )
    blockers.extend(
        str(item)
        for item in deployment.get("invalid", [])
        if "HERMES" in str(item) or str(item).startswith("FLORENCE_HERMES")
    )
    if deployment.get("hermes_toolsets"):
        blockers.append("FLORENCE_HERMES_TOOLSETS must be empty.")
    if deployment.get("database_backend") != "postgres":
        blockers.append("FLORENCE_DATABASE_URL must point to Postgres for Hermes SaaS pilot status.")
    if deployment.get("hermes_strict") is not True:
        blockers.append("FLORENCE_HERMES_STRICT must be true for SaaS pilots.")
    if deployment.get("hermes_runtime_lock") != HERMES_INTERPROCESS_LOCK_MODE:
        blockers.append("Hermes runtime must use an interprocess file lock for SaaS pilots.")
    if not deployment.get("hermes_ref_matches"):
        blockers.append("Hermes checkout ref must match HERMES_AGENT_REF.")
    if deployment.get("hermes_failure_cleanup") != HERMES_FAILURE_CLEANUP:
        blockers.append("Hermes failure cleanup must restore runtime home and clear checkout modules.")
    return blockers


def _action_smoke_blockers(actions: dict[str, object]) -> list[str]:
    blockers = []
    approved = int(actions.get("approved") or 0)
    failed = int(actions.get("failed") or 0)
    succeeded = int(actions.get("succeeded") or 0)
    if succeeded == 0:
        blockers.append("Run at least one parent-approved action through the worker.")
    if approved:
        blockers.append(f"{approved} approved actions still need worker execution.")
    if failed:
        blockers.append(f"{failed} action executions failed.")
    return blockers


def _deployment_check(*, settings: Settings, store: Store) -> dict[str, Any]:
    database = _database_check(settings=settings, store=store)
    stored_live_verifications: dict[str, dict[str, str]] = {}
    if database.get("reachable") and database.get("schema_ready"):
        stored_live_verifications = store.list_live_verifications()
    deployment = _pilot_deployment_check(
        settings,
        stored_live_verifications=stored_live_verifications,
    )
    if not database["reachable"]:
        deployment["ready"] = False
        deployment["invalid"] = [
            *deployment["invalid"],
            "Florence database is not reachable from this process",
        ]
    if not database["backend_matches"]:
        deployment["ready"] = False
        deployment["invalid"] = [
            *deployment["invalid"],
            "Florence database backend does not match FLORENCE_DATABASE_URL",
        ]
    if database.get("reachable") and not database.get("schema_ready", False):
        deployment["ready"] = False
        deployment["invalid"] = [
            *deployment["invalid"],
            "Florence database schema is not compatible with this build",
        ]
    deployment["database"] = database
    deployment["operator_next_steps"] = _deployment_operator_next_steps(deployment)
    return deployment


def _deployment_operator_next_steps(deployment: dict[str, Any]) -> list[dict[str, Any]]:
    next_steps: list[dict[str, Any]] = []
    missing_required = list(deployment.get("missing_required") or [])
    if missing_required:
        next_steps.append(
            {
                "id": "production_environment",
                "status": "missing_env",
                "missing_env": missing_required,
                "action": "Set these production environment variables before running a SaaS pilot.",
            }
        )

    invalid = list(deployment.get("invalid") or [])
    if invalid:
        next_steps.append(
            {
                "id": "deployment_preflight",
                "status": "invalid_configuration",
                "blocked_by": invalid,
                "action": "Fix the deployment safety preflight blockers before testing with a household.",
            }
        )

    database = deployment.get("database") or {}
    database_blockers = []
    if database and not bool(database.get("reachable")):
        database_blockers.append(database.get("error") or "Database is not reachable")
    if database and not bool(database.get("backend_matches")):
        database_blockers.append("Running store backend does not match FLORENCE_DATABASE_URL")
    if database and bool(database.get("reachable")) and not bool(database.get("schema_ready", False)):
        database_blockers.append(database.get("schema_error") or "Database schema is incompatible")
    if database_blockers:
        next_steps.append(
            {
                "id": "postgres_database",
                "status": "database_not_ready",
                "blocked_by": database_blockers,
                "action": "Configure a reachable Postgres FLORENCE_DATABASE_URL for the deployed app.",
            }
        )

    live = deployment.get("live_verification") or {}
    for step in live.get("next_steps") or []:
        next_steps.append(
            {
                "id": f"live_{step.get('id')}",
                "status": step.get("status"),
                "missing_env": list(step.get("missing_env") or []),
                "required_env": list(step.get("required_env") or []),
                "proof_env": list(step.get("proof_env") or []),
                "missing_proof": list(step.get("missing_proof") or []),
                "blocked_by": list(step.get("blocked_by") or []),
                "action": step.get("action"),
            }
        )
    return next_steps


def _pilot_deployment_check(
    settings: Settings,
    *,
    stored_live_verifications: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    missing_required = [
        env_name
        for env_name, attr in PILOT_REQUIRED_SETTINGS.items()
        if not getattr(settings, attr)
    ]
    invalid: list[str] = []
    warnings: list[str] = []

    runtime_home_error = hermes_runtime_home_error(settings.hermes_runtime_home)
    if runtime_home_error:
        invalid.append(runtime_home_error)
    runtime_lock_error = hermes_runtime_lock_error()
    if settings.database_backend == "postgres" and runtime_lock_error:
        invalid.append(runtime_lock_error)
    token_key_error = _token_encryption_key_error(settings)
    if token_key_error:
        invalid.append(token_key_error)

    hermes_path = settings.hermes_agent_path
    hermes_checkout_ref = None
    hermes_ref_matches = None
    if hermes_path and runtime_home_error is None:
        with hermes_runtime_home_context(settings, scope=hermes_preflight_scope(), cleanup=True):
            with hermes_checkout_module_context(settings):
                hermes_error = _hermes_agent_path_error(hermes_path)
        if hermes_error:
            invalid.append(hermes_error)
        elif settings.hermes_agent_ref and PINNED_GIT_REF.fullmatch(settings.hermes_agent_ref.strip()):
            hermes_checkout_ref = read_hermes_checkout_ref(Path(hermes_path).expanduser())
            hermes_ref_matches = _refs_match(settings.hermes_agent_ref, hermes_checkout_ref)
            if not hermes_ref_matches:
                invalid.append(
                    "FLORENCE_HERMES_AGENT_PATH checkout ref does not match HERMES_AGENT_REF"
                )
    if settings.hermes_agent_ref and not PINNED_GIT_REF.fullmatch(settings.hermes_agent_ref.strip()):
        invalid.append(
            "HERMES_AGENT_REF must be a full pinned Git commit SHA "
            "(40 or 64 hex characters), not a branch, tag, short SHA, or floating ref"
        )

    enabled_toolsets = [toolset for toolset in settings.hermes_enabled_toolsets if toolset.strip()]
    if enabled_toolsets:
        invalid.append(
            "FLORENCE_HERMES_TOOLSETS must be empty for multi-family SaaS pilots; "
            "Florence owns external tools and integrations"
        )
    if settings.database_backend == "postgres" and not settings.hermes_strict:
        invalid.append(
            "FLORENCE_HERMES_STRICT must be true for multi-family SaaS pilots; "
            "Hermes contract violations must not fall back silently"
        )
    if (settings.hermes_provider or "").strip().lower() == "custom":
        if not settings.hermes_api_key:
            invalid.append(
                "FLORENCE_HERMES_API_KEY or OPENAI_API_KEY must be set when "
                "FLORENCE_HERMES_PROVIDER=custom"
            )
        if not settings.hermes_base_url:
            invalid.append(
                "FLORENCE_HERMES_BASE_URL or OPENAI_BASE_URL must be set when "
                "FLORENCE_HERMES_PROVIDER=custom"
            )

    if settings.database_url and settings.database_backend != "postgres":
        invalid.append("FLORENCE_DATABASE_URL must start with postgres:// or postgresql://")

    google_oauth_missing = missing_google_oauth_settings(settings)
    if google_oauth_missing:
        warnings.append(
            "Google OAuth is not live until these are set: "
            + ", ".join(google_oauth_missing)
        )

    live_verification = _pilot_live_verification_report(
        settings=settings,
        missing_required=missing_required,
        invalid=invalid,
        google_oauth_missing=google_oauth_missing,
        stored_live_verifications=stored_live_verifications,
    )

    return {
        "ready": not missing_required and not invalid and bool(live_verification["ready"]),
        "missing_required": missing_required,
        "invalid": invalid,
        "warnings": warnings,
        "google_oauth_missing": google_oauth_missing,
        "live_verification": live_verification,
        "hermes_toolsets": list(settings.hermes_enabled_toolsets),
        "hermes_agent_ref": settings.hermes_agent_ref,
        "hermes_checkout_ref": hermes_checkout_ref,
        "hermes_ref_matches": hermes_ref_matches,
        "hermes_provider": settings.hermes_provider,
        "hermes_model": settings.hermes_model,
        "hermes_strict": settings.hermes_strict,
        "hermes_runtime_home": str(Path(settings.hermes_runtime_home).expanduser()),
        "hermes_runtime_scope": "per_turn_under_runtime_home",
        "hermes_runtime_cleanup": "enabled",
        "hermes_runtime_concurrency": hermes_runtime_concurrency_mode(),
        "hermes_runtime_lock": hermes_runtime_lock_mode(),
        "hermes_preflight_scope": "ephemeral_per_check_under_runtime_home",
        "hermes_python_path_scope": "temporary_during_hermes_call",
        "hermes_module_cache_scope": "shadowed_and_cleared_during_hermes_import_or_call",
        "hermes_failure_cleanup": HERMES_FAILURE_CLEANUP,
        "database_backend": settings.database_backend,
    }


def _database_check(*, settings: Settings, store: Store) -> dict[str, Any]:
    store_backend = str(getattr(store, "backend", "unknown"))
    backend_matches = (
        settings.database_backend == "unsupported"
        or store_backend == settings.database_backend
    )
    base = {
        "configured_backend": settings.database_backend,
        "store_backend": store_backend,
        "backend_matches": backend_matches,
    }
    try:
        store.ping()
    except Exception as exc:
        return {
            **base,
            "reachable": False,
            "schema_ready": False,
            "schema": None,
            "error": f"{type(exc).__name__}: {_redact_database_error(str(exc))}",
        }
    try:
        schema = store.schema_status()
    except DatabaseSchemaError as exc:
        return {
            **base,
            "reachable": True,
            "schema_ready": False,
            "schema": None,
            "schema_error": _operator_safe_database_schema_error(str(exc)),
        }
    except Exception as exc:
        return {
            **base,
            "reachable": True,
            "schema_ready": False,
            "schema": None,
            "schema_error": f"{type(exc).__name__}: database schema check failed",
        }
    schema_ready = bool(schema.get("ready"))
    result = {
        **base,
        "reachable": True,
        "schema_ready": schema_ready,
        "schema": schema,
    }
    if not schema_ready:
        result["schema_error"] = _operator_safe_database_schema_error(
            _database_schema_status_error(schema)
        )
    return result


def _token_encryption_key_error(settings: Settings) -> str | None:
    if not settings.token_encryption_key:
        return None
    try:
        TokenVault.from_settings(settings)
    except TokenVaultError:
        return "FLORENCE_TOKEN_ENCRYPTION_KEY must be a valid Fernet key"
    return None


def _record_live_verification(
    *,
    store: Store,
    settings: Settings,
    name: str,
    proof: str,
    verified_at_utc: datetime,
    source: str,
    now_utc: datetime,
) -> dict[str, Any]:
    normalized_name = name.strip().lower()
    if normalized_name not in LIVE_VERIFICATION_SPECS:
        raise HTTPException(status_code=400, detail="unsupported_live_verification")
    verified_at = ensure_utc(verified_at_utc)
    now = ensure_utc(now_utc)
    if verified_at > now + LIVE_VERIFICATION_MAX_FUTURE_SKEW:
        raise HTTPException(status_code=400, detail="live_verification_timestamp_in_future")
    proof_error = _live_verification_proof_error(proof, settings=settings)
    if proof_error is not None:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsafe_live_verification_proof",
                "detail": proof_error,
            },
        )
    record = store.record_live_verification(
        name=normalized_name,
        verified_at_utc=verified_at,
        proof=proof.strip(),
        source=source,
        now_utc=now,
    )
    safe_record = _proof_safe_value(record, settings=settings)
    return safe_record if isinstance(safe_record, dict) else {}


def _database_schema_status_error(schema: dict[str, Any]) -> str:
    problems: list[str] = []
    missing_tables = list(schema.get("missing_tables") or [])
    missing_columns = dict(schema.get("missing_columns") or {})
    if missing_tables:
        problems.append("missing tables: " + ", ".join(str(table) for table in missing_tables))
    for table_name, columns in missing_columns.items():
        problems.append(f"{table_name} missing columns: {', '.join(str(column) for column in columns)}")
    return "; ".join(problems) or "database schema is incompatible"


def _operator_safe_database_schema_error(error: str) -> str:
    return _redact_database_error(error).replace("\n", " ")[:500]


def _redact_database_error(error: str) -> str:
    return re.sub(
        r"(postgres(?:ql)?://)[^@\s]+@",
        r"\1[redacted]@",
        error,
        flags=re.IGNORECASE,
    )


def _proof_source_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "id": item.get("id"),
            "source_type": item.get("source_type"),
            "reason": item.get("reason"),
            "priority": item.get("priority"),
            "event_at_present": bool(item.get("event_at_utc")),
        }
        for item in items
    ]


def _proof_action_summary(actions: dict[str, object]) -> dict[str, object]:
    return {
        "ready": actions.get("ready"),
        "approved": actions.get("approved"),
        "failed": actions.get("failed"),
        "succeeded": actions.get("succeeded"),
        "issues": [_proof_action_issue(issue) for issue in list(actions.get("issues") or [])],
    }


def _proof_action_issue(issue: object) -> dict[str, object]:
    if not isinstance(issue, dict):
        return {}
    proof_issue = {key: value for key, value in issue.items() if key != "error"}
    if "error" in issue:
        proof_issue["error_present"] = bool(issue.get("error"))
    return proof_issue


def _proof_safe_value(value: object, *, settings: Settings) -> object:
    if isinstance(value, dict):
        return {str(key): _proof_safe_value(item, settings=settings) for key, item in value.items()}
    if isinstance(value, list):
        return [_proof_safe_value(item, settings=settings) for item in value]
    if isinstance(value, tuple):
        return [_proof_safe_value(item, settings=settings) for item in value]
    if isinstance(value, str):
        return _operator_safe_text(value, settings=settings)
    return value


def _hermes_smoke_error(exc: Exception, *, settings: Settings) -> str:
    if isinstance(exc, HermesSaaSContractError):
        return _operator_safe_error(exc, settings=settings)
    return f"{type(exc).__name__}: Hermes smoke failed; check provider configuration and server logs"


def _operator_safe_error(exc: Exception, *, settings: Settings) -> str:
    text = _operator_safe_text(f"{type(exc).__name__}: {exc}", settings=settings)
    if len(text) > LIVE_VERIFICATION_PROOF_MAX_CHARS:
        return text[: LIVE_VERIFICATION_PROOF_MAX_CHARS - 1].rstrip() + "..."
    return text


def _operator_safe_text(text: str, *, settings: Settings) -> str:
    text = _redact_database_error(text)
    for secret_value in _configured_secret_values(settings):
        text = text.replace(secret_value, "[redacted]")
    text, protected_env_names = _protect_operator_env_names(text)
    text = LIVE_PROOF_PHONE_RE.sub("[phone number]", text)
    text = LIVE_PROOF_EMAIL_RE.sub("[email address]", text)
    text = LIVE_PROOF_SECRET_RE.sub(_redact_operator_secret_match, text)
    for token, env_name in protected_env_names.items():
        text = text.replace(token, env_name)
    return text


def _protect_operator_env_names(text: str) -> tuple[str, dict[str, str]]:
    protected: dict[str, str] = {}
    for index, env_name in enumerate(sorted(OPERATOR_SAFE_ENV_NAMES, key=len, reverse=True)):
        token = f"__florence_env_{index}__"
        pattern = re.compile(rf"(?<![A-Z0-9_]){re.escape(env_name)}(?![A-Z0-9_])")
        if pattern.search(text):
            text = pattern.sub(token, text)
            protected[token] = env_name
    return text, protected


def _redact_operator_secret_match(match: re.Match[str]) -> str:
    value = match.group(0)
    if re.match(r"(?i)postgres(?:ql)?://\[redacted\]@", value):
        return value
    return "[redacted secret]"


def _hermes_runtime_status(settings: Settings) -> dict[str, Any]:
    configured_path = settings.hermes_agent_path
    resolved_path = str(Path(configured_path).expanduser().resolve()) if configured_path else None
    run_agent_path = str(Path(resolved_path) / "run_agent.py") if resolved_path else None
    runtime_home_path = Path(settings.hermes_runtime_home).expanduser()
    runtime_home = str(runtime_home_path.resolve()) if runtime_home_path.is_absolute() else str(runtime_home_path)
    runtime_home_error = hermes_runtime_home_error(settings.hermes_runtime_home)
    preflight_home = None
    path_error = None
    if configured_path and runtime_home_error is None:
        preflight_scope = hermes_preflight_scope()
        preflight_home = scoped_hermes_runtime_home(settings.hermes_runtime_home, scope=preflight_scope)
        with hermes_runtime_home_context(settings, scope=preflight_scope, cleanup=True):
            with hermes_checkout_module_context(settings):
                path_error = _hermes_agent_path_error(configured_path)
    checkout_ref = (
        read_hermes_checkout_ref(Path(configured_path).expanduser())
        if configured_path and path_error is None
        else None
    )
    pinned_ref = bool(
        settings.hermes_agent_ref and PINNED_GIT_REF.fullmatch(settings.hermes_agent_ref.strip())
    )
    ref_matches = _refs_match(settings.hermes_agent_ref, checkout_ref) if pinned_ref else None
    toolsets = [toolset for toolset in settings.hermes_enabled_toolsets if toolset.strip()]
    invalid = []
    if not configured_path:
        invalid.append("FLORENCE_HERMES_AGENT_PATH is not set")
    if runtime_home_error:
        invalid.append(runtime_home_error)
    if path_error:
        invalid.append(path_error)
    if settings.hermes_agent_ref and not pinned_ref:
        invalid.append(
            "HERMES_AGENT_REF must be a full pinned Git commit SHA "
            "(40 or 64 hex characters), not a branch, tag, short SHA, or floating ref"
        )
    elif not settings.hermes_agent_ref:
        invalid.append("HERMES_AGENT_REF is not set")
    if (
        settings.hermes_agent_ref
        and pinned_ref
        and configured_path
        and path_error is None
        and not ref_matches
    ):
        invalid.append("FLORENCE_HERMES_AGENT_PATH checkout ref does not match HERMES_AGENT_REF")
    if toolsets:
        invalid.append(
            "FLORENCE_HERMES_TOOLSETS must be empty because Florence owns SaaS integrations"
        )
    if settings.database_backend != "postgres":
        invalid.append("FLORENCE_DATABASE_URL must point to Postgres for Hermes SaaS pilot status")
    if settings.database_backend == "postgres" and not settings.hermes_strict:
        invalid.append(
            "FLORENCE_HERMES_STRICT must be true for multi-family SaaS pilots"
        )
    runtime_lock_error = hermes_runtime_lock_error()
    if settings.database_backend == "postgres" and runtime_lock_error:
        invalid.append(runtime_lock_error)
    if not settings.hermes_provider:
        invalid.append("FLORENCE_HERMES_PROVIDER is not set")
    if not settings.hermes_model:
        invalid.append("FLORENCE_HERMES_MODEL is not set")
    if (settings.hermes_provider or "").strip().lower() == "custom":
        if not settings.hermes_api_key:
            invalid.append(
                "FLORENCE_HERMES_API_KEY or OPENAI_API_KEY must be set when "
                "FLORENCE_HERMES_PROVIDER=custom"
            )
        if not settings.hermes_base_url:
            invalid.append(
                "FLORENCE_HERMES_BASE_URL or OPENAI_BASE_URL must be set when "
                "FLORENCE_HERMES_PROVIDER=custom"
            )
    return {
        "mode": "configured_checkout" if configured_path else "ambient_python_module",
        "agent_path": resolved_path,
        "run_agent_path": run_agent_path,
        "contract_ok": bool(configured_path) and path_error is None and runtime_home_error is None,
        "ready_for_saas_pilot": not invalid,
        "invalid": invalid,
        "hermes_agent_ref": settings.hermes_agent_ref,
        "hermes_checkout_ref": checkout_ref,
        "hermes_ref_matches": ref_matches,
        "pinned_ref": pinned_ref,
        "provider": settings.hermes_provider,
        "model": settings.hermes_model,
        "api_key_configured": bool(settings.hermes_api_key),
        "base_url_configured": bool(settings.hermes_base_url),
        "strict_mode": settings.hermes_strict,
        "database_backend": settings.database_backend,
        "toolsets": list(settings.hermes_enabled_toolsets),
        "toolsets_disabled": not toolsets,
        "runtime_home": runtime_home,
        "preflight_runtime_home": preflight_home,
        "preflight_runtime_home_scope": "ephemeral_per_check_under_runtime_home",
        "turn_runtime_home_scope": "per_turn_under_runtime_home",
        "turn_runtime_cleanup": "enabled",
        "turn_runtime_concurrency": hermes_runtime_concurrency_mode(),
        "runtime_lock": hermes_runtime_lock_mode(),
        "runtime_env_var": "HERMES_HOME",
        "runtime_home_writable": runtime_home_error is None,
        "python_path_scope": "temporary_during_hermes_call",
        "module_cache_scope": "shadowed_and_cleared_during_hermes_import_or_call",
        "turn_failure_cleanup": HERMES_FAILURE_CLEANUP,
        "memory_owner": "florence",
        "session_scope": "ephemeral_per_turn",
        "durable_hermes_memory": "disabled",
    }


def _hermes_agent_path_error(hermes_path: str) -> str | None:
    path = Path(hermes_path).expanduser()
    if not path.exists():
        return "FLORENCE_HERMES_AGENT_PATH does not exist"
    run_agent_path = path / "run_agent.py"
    if not run_agent_path.exists():
        return "FLORENCE_HERMES_AGENT_PATH must point to a Hermes checkout with run_agent.py"
    spec = importlib.util.spec_from_file_location(
        f"_florence_hermes_preflight_{abs(hash(str(run_agent_path.resolve())))}",
        run_agent_path,
    )
    if spec is None or spec.loader is None:
        return "FLORENCE_HERMES_AGENT_PATH run_agent.py is not importable"
    module = importlib.util.module_from_spec(spec)
    path_text = str(path.resolve())
    inserted_path = path_text not in sys.path
    if inserted_path:
        sys.path.insert(0, path_text)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        return f"FLORENCE_HERMES_AGENT_PATH could not import run_agent.AIAgent: {type(exc).__name__}: {exc}"
    finally:
        if inserted_path:
            try:
                sys.path.remove(path_text)
            except ValueError:
                pass
    if not hasattr(module, "AIAgent"):
        return "FLORENCE_HERMES_AGENT_PATH run_agent.py does not expose AIAgent"
    AIAgent = module.AIAgent
    if not callable(AIAgent):
        return "FLORENCE_HERMES_AGENT_PATH run_agent.py AIAgent is not callable"
    if not _accepts_required_kwargs(AIAgent, HERMES_AGENT_INIT_KWARGS):
        return (
            "FLORENCE_HERMES_AGENT_PATH AIAgent constructor is not compatible "
            "with Florence adapter"
        )
    run_conversation = getattr(AIAgent, "run_conversation", None)
    if not callable(run_conversation):
        return "FLORENCE_HERMES_AGENT_PATH AIAgent.run_conversation is missing"
    if not _accepts_required_kwargs(run_conversation, HERMES_RUN_CONVERSATION_KWARGS):
        return (
            "FLORENCE_HERMES_AGENT_PATH AIAgent.run_conversation is not compatible "
            "with Florence adapter"
        )
    return None


def _refs_match(expected: str | None, actual: str | None) -> bool | None:
    if not expected or not actual:
        return None
    expected_clean = expected.strip().lower()
    actual_clean = actual.strip().lower()
    if not expected_clean or not actual_clean:
        return None
    return actual_clean == expected_clean


def _accepts_required_kwargs(callable_obj: Any, names: tuple[str, ...]) -> bool:
    try:
        signature = inspect.signature(callable_obj)
    except (TypeError, ValueError):
        return False
    parameters = signature.parameters.values()
    explicit = set()
    for parameter in parameters:
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            return True
        if parameter.kind in {
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        }:
            explicit.add(parameter.name)
    return all(name in explicit for name in names)


def _pilot_live_verification_report(
    *,
    settings: Settings,
    missing_required: list[str],
    invalid: list[str],
    google_oauth_missing: list[str],
    stored_live_verifications: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    locally_verified = [
        "Two-parent setup/readiness, helper defaults, and parent-only controls",
        "Linq webhook signature parsing, message idempotency, and retry delivery through fake sender",
        "Household-level inbound/outbound text transport evidence without exposing message bodies",
        "Public typed source ingest, bounded Need-to-Know triage, and no raw inbox dump",
        "Connected-source worker sync, cursor advancement, Need-to-Know surfacing, and source delivery retry",
        "Timezone-correct reminders, stale reminder expiry, and daily briefing delivery window",
        "Parent approval rail, worker action execution, and due reminder delivery",
        "Hermes proposal boundary, checkout contract preflight, runtime toolset guard, and smoke endpoint",
        "Household-scoped memory, privacy controls, deletion, and SaaS isolation",
        "Warm deterministic help, support, and fallback tone without Hermes",
    ]
    external_credentials_needed = []
    if missing_required:
        external_credentials_needed.extend(missing_required)
    if google_oauth_missing:
        external_credentials_needed.extend(
            item for item in google_oauth_missing if item not in external_credentials_needed
        )
    evidence = _live_verification_evidence(
        settings,
        stored_live_verifications=stored_live_verifications,
    )
    unverified = []
    if _pilot_check_has_settings(
        missing_required,
        "LINQ_WEBHOOK_SECRET",
        "LINQ_API_KEY",
        "LINQ_FROM_PHONE",
    ) and not _live_verification_is_marked(evidence, "linq"):
        unverified.append("Live Linq iMessage send and webhook round-trip")
    if not google_oauth_missing and not _live_verification_is_marked(evidence, "google"):
        unverified.append("Live Google OAuth connection and source sync")
    if _pilot_check_has_settings(
        missing_required,
        "FLORENCE_HERMES_AGENT_PATH",
    ) and not _has_hermes_path_error(invalid) and not _live_verification_is_marked(
        evidence,
        "hermes",
    ):
        unverified.append("Live Hermes Agent response through Florence adapter")
    evidence_gaps = [
        gap
        for item in evidence.values()
        for gap in item["missing"]
    ]
    next_steps = _live_verification_next_steps(
        settings=settings,
        missing_required=missing_required,
        invalid=invalid,
        google_oauth_missing=google_oauth_missing,
        evidence=evidence,
    )
    blocked = []
    if external_credentials_needed:
        blocked.append("External credentials/configuration required")
    if unverified:
        blocked.append("Live Linq/Google/Hermes smoke checks not marked verified")
    if evidence_gaps:
        blocked.append("Live verification proof metadata required")
    if invalid:
        blocked.append("Pilot deployment safety preflight")
    return {
        "ready": (
            not external_credentials_needed
            and not unverified
            and not evidence_gaps
            and not invalid
        ),
        "locally_verified": locally_verified,
        "external_credentials_needed": external_credentials_needed,
        "unverified": unverified,
        "evidence_gaps": evidence_gaps,
        "next_steps": next_steps,
        "verified": {
            "linq": _live_verification_is_marked(evidence, "linq"),
            "google": _live_verification_is_marked(evidence, "google"),
            "hermes": _live_verification_is_marked(evidence, "hermes"),
        },
        "evidence": evidence,
        "blocked": blocked,
    }


def _live_verification_next_steps(
    *,
    settings: Settings,
    missing_required: list[str],
    invalid: list[str],
    google_oauth_missing: list[str],
    evidence: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    missing = set(missing_required)
    google_missing = set(google_oauth_missing)
    next_steps = []
    for name, spec in LIVE_VERIFICATION_SPECS.items():
        evidence_item = evidence.get(name) or {}
        missing_env = [
            env_name
            for env_name in spec["required_env"]
            if env_name in missing or env_name in google_missing
        ]
        missing_proof = list(evidence_item.get("missing") or [])
        invalid_blockers = _live_verification_invalid_blockers(name=name, invalid=invalid)
        verified_flag = bool(evidence_item.get("verified"))
        verified_with_proof = bool(evidence_item.get("verified")) and not missing_proof
        if verified_with_proof and not missing_env and not invalid_blockers:
            continue
        if missing_env:
            status = "missing_env"
        elif invalid_blockers:
            status = "invalid_configuration"
        elif not verified_flag:
            status = "needs_live_smoke"
        else:
            status = "needs_proof_metadata"
        next_steps.append(
            {
                "id": name,
                "label": spec["label"],
                "status": status,
                "missing_env": missing_env,
                "required_env": list(spec["required_env"]),
                "verified_env": spec["verified_env"],
                "proof_env": [spec["verified_at_env"], spec["proof_env"]],
                "missing_proof": missing_proof,
                "blocked_by": invalid_blockers,
                "action": spec["action"],
            }
        )
    return next_steps


def _live_verification_invalid_blockers(*, name: str, invalid: list[str]) -> list[str]:
    if name != "hermes":
        return []
    return [
        item
        for item in invalid
        if "HERMES" in item or item.startswith("FLORENCE_HERMES")
    ]


def _pilot_check_has_settings(missing_required: list[str], *names: str) -> bool:
    missing = set(missing_required)
    return all(name not in missing for name in names)


def _has_hermes_path_error(invalid: list[str]) -> bool:
    return any(item.startswith("FLORENCE_HERMES_AGENT_PATH") for item in invalid)


def _live_verification_is_marked(
    evidence: dict[str, dict[str, Any]],
    name: str,
) -> bool:
    return bool((evidence.get(name) or {}).get("verified"))


def _live_verification_evidence(
    settings: Settings,
    *,
    stored_live_verifications: dict[str, dict[str, str]] | None = None,
) -> dict[str, dict[str, Any]]:
    evidence: dict[str, dict[str, Any]] = {}
    stored_live_verifications = stored_live_verifications or {}
    for name, spec in LIVE_VERIFICATION_SPECS.items():
        stored_item = stored_live_verifications.get(name) or {}
        env_verified = bool(getattr(settings, spec["verified_attr"]))
        stored_verified = bool(stored_item)
        verified = env_verified or stored_verified
        verified_at = _optional_str(getattr(settings, spec["verified_at_attr"]))
        proof = _optional_str(getattr(settings, spec["proof_attr"]))
        verified_at_label = spec["verified_at_env"]
        proof_label = spec["proof_env"]
        if verified_at is None and stored_verified:
            verified_at = _optional_str(stored_item.get("verified_at_utc"))
            verified_at_label = f"stored_live_verifications.{name}.verified_at_utc"
        if proof is None and stored_verified:
            proof = _optional_str(stored_item.get("proof"))
            proof_label = f"stored_live_verifications.{name}.proof"
        missing = []
        normalized_at = None
        safe_proof = None
        if verified:
            if verified_at is None:
                missing.append(verified_at_label)
            else:
                parsed_at = _parse_live_verified_at(verified_at)
                if parsed_at is None:
                    missing.append(
                        f"{verified_at_label} must be an ISO-8601 timestamp with timezone"
                    )
                else:
                    normalized = ensure_utc(parsed_at)
                    if normalized > datetime.now(timezone.utc) + LIVE_VERIFICATION_MAX_FUTURE_SKEW:
                        missing.append(f"{verified_at_label} must not be in the future")
                    else:
                        normalized_at = normalized.isoformat()
            if proof is None:
                missing.append(proof_label)
            else:
                proof_error = _live_verification_proof_error(proof, settings=settings)
                if proof_error is None:
                    safe_proof = proof
                else:
                    missing.append(f"{proof_label} {proof_error}")
        evidence[name] = {
            "verified": verified,
            "verified_at_utc": normalized_at,
            "proof": safe_proof if verified else None,
            "missing": missing,
        }
    return evidence


def _live_verification_proof_error(proof: str, *, settings: Settings) -> str | None:
    if len(proof) > LIVE_VERIFICATION_PROOF_MAX_CHARS:
        return LIVE_VERIFICATION_PROOF_SAFETY_ERROR
    if LIVE_PROOF_RAW_PAYLOAD_RE.search(proof):
        return LIVE_VERIFICATION_PROOF_SAFETY_ERROR
    if LIVE_PROOF_EMAIL_RE.search(proof) or LIVE_PROOF_PHONE_RE.search(proof):
        return LIVE_VERIFICATION_PROOF_SAFETY_ERROR
    if LIVE_PROOF_SECRET_RE.search(proof):
        return LIVE_VERIFICATION_PROOF_SAFETY_ERROR
    for secret_value in _configured_secret_values(settings):
        if secret_value in proof:
            return LIVE_VERIFICATION_PROOF_SAFETY_ERROR
    return None


def _configured_secret_values(settings: Settings) -> list[str]:
    values = [
        settings.linq_api_key,
        settings.linq_webhook_secret,
        settings.admin_api_key,
        settings.source_ingest_api_key,
        settings.google_client_secret,
        settings.token_encryption_key,
    ]
    return [
        str(value).strip()
        for value in values
        if value is not None and len(str(value).strip()) >= 8
    ]


def _parse_live_verified_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def _normalize_live_verified_at(value: str | None) -> str | None:
    parsed = _parse_live_verified_at(value)
    if parsed is None:
        return None
    normalized = ensure_utc(parsed)
    if normalized > datetime.now(timezone.utc) + LIVE_VERIFICATION_MAX_FUTURE_SKEW:
        return None
    return normalized.isoformat()


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
