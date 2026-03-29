"""Standalone HTTP server for Florence prod dogfooding."""

from __future__ import annotations

import argparse
import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from florence.config import FlorenceSettings
from florence.runtime.production import FlorenceHTTPResult, FlorenceProductionService

logger = logging.getLogger(__name__)


def _resolve_preflight_mode() -> str:
    raw = os.getenv("FLORENCE_HERMES_PREFLIGHT", "strict").strip().lower()
    if raw in {"off", "false", "0", "disabled", "no"}:
        return "off"
    if raw in {"warn", "warning"}:
        return "warn"
    return "strict"


def _preflight_error_hint(exc: Exception) -> str:
    detail = str(exc).lower()
    if "unknown parameter: 'reasoning'" in detail or 'unknown parameter: "reasoning"' in detail:
        return (
            "Your model/provider pair does not accept Hermes reasoning fields. "
            "Use a compatible provider/model pair or disable reasoning for that backend."
        )
    if "no cookie auth credentials found" in detail:
        return (
            "The configured endpoint expects Codex cookie/OAuth auth. "
            "Use Codex OAuth credentials, or point Florence to an API-key endpoint."
        )
    if "401" in detail or "authentication" in detail:
        return "Authentication failed. Check provider credentials and endpoint configuration."
    return "Check FLORENCE_HERMES_PROVIDER, FLORENCE_HERMES_MODEL, and provider API credentials."


def _default_preflight_agent_factory(**kwargs):
    from run_agent import AIAgent

    return AIAgent(**kwargs)


def _run_hermes_preflight(
    settings: FlorenceSettings,
    *,
    agent_factory: Callable[..., Any] | None = None,
) -> None:
    mode = _resolve_preflight_mode()
    if mode == "off":
        logger.info("Florence Hermes preflight skipped (FLORENCE_HERMES_PREFLIGHT=off)")
        return

    factory = agent_factory or _default_preflight_agent_factory
    try:
        agent = factory(
            model=settings.hermes.model,
            max_iterations=1,
            provider=settings.hermes.provider,
            enabled_toolsets=[],
            disabled_toolsets=[],
            quiet_mode=True,
            skip_memory=True,
            platform="florence-preflight",
        )
        result = agent.run_conversation(
            user_message="Reply with exactly: preflight_ok",
            system_message="Return exactly preflight_ok. No punctuation.",
        )
        final_response = str(result.get("final_response") or "").strip()
        if not final_response:
            raise RuntimeError("empty_final_response")
    except Exception as exc:  # pragma: no cover - exercised via monkeypatched tests
        message = f"Florence Hermes preflight failed: {exc}. {_preflight_error_hint(exc)}"
        if mode == "warn":
            logger.warning(message)
            return
        raise RuntimeError(message) from exc

    logger.info("Florence Hermes preflight passed")


def _log_runtime_configuration(settings: FlorenceSettings) -> None:
    database_backend = "postgres" if settings.server.database_url else "sqlite"
    logger.info("Florence database backend: %s", database_backend)
    if settings.server.database_url:
        logger.info("Florence database url configured via DATABASE_URL/FLORENCE_DATABASE_URL")
    elif settings.server.db_path is not None:
        logger.warning("Florence is using SQLite at %s", settings.server.db_path)

    if settings.linq.configured:
        logger.info("Florence Linq transport is configured")
    else:
        logger.warning("Florence Linq transport is not configured")
    if settings.sendblue.configured:
        logger.info("Florence Sendblue transport is configured")
    else:
        logger.warning("Florence Sendblue transport is not configured")

    if settings.google.configured:
        logger.info("Florence Google OAuth is configured")
    else:
        logger.warning("Florence Google OAuth is not configured")
    if settings.redis.configured:
        logger.info("Florence Redis queue is configured")
    else:
        logger.warning("Florence Redis queue is not configured; falling back to in-process background sync")
    hermes_base_url = (
        os.getenv("OPENAI_BASE_URL", "").strip()
        or os.getenv("OPENROUTER_BASE_URL", "").strip()
        or "(provider default)"
    )
    logger.info(
        "Florence Hermes runtime: provider=%s model=%s base_url=%s",
        settings.hermes.provider,
        settings.hermes.model,
        hermes_base_url,
    )


class _FlorenceRequestHandler(BaseHTTPRequestHandler):
    service: FlorenceProductionService | None = None

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in {"/health", "/florence/health", "/v1/florence/health"}:
            self._write_response(
                FlorenceHTTPResult(
                    status_code=200,
                    content_type="application/json; charset=utf-8",
                    body=json.dumps({"ok": True}),
                )
            )
            return
        if parsed.path in {"/florence/google/callback", "/v1/florence/google/callback"}:
            query = parse_qs(parsed.query)
            result = self._service().handle_google_callback(
                code=self._query_value(query, "code"),
                state=self._query_value(query, "state"),
                error=self._query_value(query, "error"),
            )
            self._write_response(result)
            return
        if parsed.path in {"/florence/onboarding", "/v1/florence/onboarding"}:
            query = parse_qs(parsed.query)
            result = self._service().handle_onboarding_page(
                token=self._query_value(query, "token"),
                status_message=self._query_value(query, "status"),
            )
            self._write_response(result)
            return
        if parsed.path == "/v1/web/session":
            query = parse_qs(parsed.query)
            result = self._service().handle_web_session(
                token=self._query_value(query, "token"),
                auth_email=self._query_value(query, "email") or self.headers.get("x-florence-auth-email"),
            )
            self._write_response(result)
            return
        if parsed.path == "/v1/web/setup":
            query = parse_qs(parsed.query)
            result = self._service().handle_web_setup(
                token=self._query_value(query, "token"),
                auth_email=self._query_value(query, "email") or self.headers.get("x-florence-auth-email"),
            )
            self._write_response(result)
            return
        if parsed.path == "/v1/web/google/connections":
            query = parse_qs(parsed.query)
            result = self._service().handle_web_google_connections(
                token=self._query_value(query, "token"),
                auth_email=self._query_value(query, "email") or self.headers.get("x-florence-auth-email"),
            )
            self._write_response(result)
            return
        if parsed.path == "/v1/web/settings":
            query = parse_qs(parsed.query)
            result = self._service().handle_web_settings(
                token=self._query_value(query, "token"),
                auth_email=self._query_value(query, "email") or self.headers.get("x-florence-auth-email"),
            )
            self._write_response(result)
            return
        self.send_error(404, "not_found")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in {"/florence/linq/webhook", "/v1/channels/linq/webhook"}:
            try:
                raw = self._read_raw_body()
                payload = self._parse_json_body(raw)
            except ValueError as exc:
                self._write_response(
                    FlorenceHTTPResult(
                        status_code=400,
                        content_type="application/json; charset=utf-8",
                        body=json.dumps({"ok": False, "error": str(exc)}),
                    )
                )
                return
            result = self._service().handle_linq_webhook(
                payload=payload,
                raw_body=raw,
                webhook_signature=self.headers.get("x-webhook-signature"),
                webhook_timestamp=self.headers.get("x-webhook-timestamp"),
            )
            self._write_response(result)
            return
        if parsed.path in {"/florence/sendblue/webhook", "/v1/channels/sendblue/webhook"}:
            try:
                raw = self._read_raw_body()
                payload = self._parse_json_body(raw)
            except ValueError as exc:
                self._write_response(
                    FlorenceHTTPResult(
                        status_code=400,
                        content_type="application/json; charset=utf-8",
                        body=json.dumps({"ok": False, "error": str(exc)}),
                    )
                )
                return
            result = self._service().handle_sendblue_webhook(
                payload=payload,
                webhook_secret=(
                    self.headers.get("x-sendblue-secret")
                    or self.headers.get("sendblue-secret")
                    or self.headers.get("x-webhook-secret")
                    or self.headers.get("webhook-secret")
                ),
            )
            self._write_response(result)
            return
        if parsed.path in {"/florence/onboarding", "/v1/florence/onboarding"}:
            form = self._parse_form_body(self._read_raw_body())
            result = self._service().handle_onboarding_submission(
                token=form.get("token"),
                form_data=form,
            )
            self._write_response(result)
            return
        if parsed.path == "/v1/web/setup/profile":
            self._handle_json_post(
                lambda payload: self._service().handle_web_setup_profile(
                    payload=payload,
                    token=str(payload.get("token") or "") or None,
                    auth_email=str(payload.get("authEmail") or self.headers.get("x-florence-auth-email") or "") or None,
                )
            )
            return
        if parsed.path == "/v1/web/google/start":
            self._handle_json_post(
                lambda payload: self._service().handle_web_google_start(
                    token=str(payload.get("token") or "") or None,
                    auth_email=str(payload.get("authEmail") or self.headers.get("x-florence-auth-email") or "") or None,
                ),
                allow_empty=True,
            )
            return
        if parsed.path == "/v1/web/google/add-account":
            self._handle_json_post(
                lambda payload: self._service().handle_web_google_add_account(
                    token=str(payload.get("token") or "") or None,
                    auth_email=str(payload.get("authEmail") or self.headers.get("x-florence-auth-email") or "") or None,
                ),
                allow_empty=True,
            )
            return
        if parsed.path == "/v1/web/google/disconnect":
            self._handle_json_post(
                lambda payload: self._service().handle_web_google_disconnect(
                    payload=payload,
                    token=str(payload.get("token") or "") or None,
                    auth_email=str(payload.get("authEmail") or self.headers.get("x-florence-auth-email") or "") or None,
                )
            )
            return
        if parsed.path == "/v1/web/settings":
            self._handle_json_post(
                lambda payload: self._service().handle_web_settings_update(
                    payload=payload,
                    token=str(payload.get("token") or "") or None,
                    auth_email=str(payload.get("authEmail") or self.headers.get("x-florence-auth-email") or "") or None,
                )
            )
            return
        self.send_error(404, "not_found")

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        logger.info("%s - %s", self.address_string(), format % args)

    def _read_raw_body(self) -> bytes:
        content_length = int(self.headers.get("content-length") or "0")
        return self.rfile.read(content_length) if content_length > 0 else b"{}"

    def _read_json_body(self) -> dict:
        return self._parse_json_body(self._read_raw_body())

    def _handle_json_post(
        self,
        handler: Callable[[dict], FlorenceHTTPResult],
        *,
        allow_empty: bool = False,
    ) -> None:
        try:
            payload = self._read_json_body() if not allow_empty else self._parse_optional_json_body(self._read_raw_body())
        except ValueError as exc:
            self._write_response(
                FlorenceHTTPResult(
                    status_code=400,
                    content_type="application/json; charset=utf-8",
                    body=json.dumps({"ok": False, "error": str(exc)}),
                )
            )
            return
        self._write_response(handler(payload))

    @staticmethod
    def _parse_json_body(raw: bytes) -> dict:
        try:
            parsed = json.loads(raw.decode("utf-8") or "{}")
        except Exception as exc:
            raise ValueError("invalid_json_body") from exc
        if not isinstance(parsed, dict):
            raise ValueError("json_body_must_be_object")
        return parsed

    @staticmethod
    def _parse_optional_json_body(raw: bytes) -> dict:
        if not raw.strip():
            return {}
        return _FlorenceRequestHandler._parse_json_body(raw)

    @staticmethod
    def _parse_form_body(raw: bytes) -> dict[str, str]:
        parsed = parse_qs(raw.decode("utf-8"), keep_blank_values=True)
        return {
            key: values[0] if values else ""
            for key, values in parsed.items()
        }

    def _write_response(self, result: FlorenceHTTPResult) -> None:
        body_bytes = result.body.encode("utf-8")
        self.send_response(result.status_code)
        self.send_header("content-type", result.content_type)
        self.send_header("content-length", str(len(body_bytes)))
        self.end_headers()
        self.wfile.write(body_bytes)

    @classmethod
    def _service(cls) -> FlorenceProductionService:
        if cls.service is None:
            raise RuntimeError("florence_service_not_initialized")
        return cls.service

    @staticmethod
    def _query_value(query: dict[str, list[str]], key: str) -> str | None:
        values = query.get(key)
        if not values:
            return None
        value = values[0].strip()
        return value or None


def build_http_server(
    service: FlorenceProductionService,
    *,
    host: str,
    port: int,
) -> ThreadingHTTPServer:
    handler_cls = type("FlorenceRequestHandler", (_FlorenceRequestHandler,), {})
    handler_cls.service = service
    return ThreadingHTTPServer((host, port), handler_cls)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Florence HTTP service")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    settings = FlorenceSettings.from_env()
    _log_runtime_configuration(settings)
    host = args.host or settings.server.host
    port = args.port or settings.server.port

    _run_hermes_preflight(settings)

    service = FlorenceProductionService(settings)
    server = build_http_server(service, host=host, port=port)

    logger.info("Florence HTTP server listening on %s:%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Florence server")
    finally:
        server.shutdown()
        service.close()


if __name__ == "__main__":
    main()
