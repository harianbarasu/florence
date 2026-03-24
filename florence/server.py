"""Standalone HTTP server for Florence prod dogfooding."""

from __future__ import annotations

import argparse
import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from florence.config import FlorenceSettings
from florence.runtime.production import FlorenceHTTPResult, FlorenceProductionService
from florence.runtime.scheduler import FlorenceSyncScheduler

logger = logging.getLogger(__name__)


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

    if settings.google.configured:
        logger.info("Florence Google OAuth is configured")
    else:
        logger.warning("Florence Google OAuth is not configured")


def _read_str(payload: dict[str, object], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


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
        if parsed.path in {"/florence/app/chats", "/v1/app/chats"}:
            query = parse_qs(parsed.query)
            result = self._service().handle_app_threads(
                household_id=self._query_value(query, "householdId") or self._query_value(query, "household_id"),
                member_id=self._query_value(query, "memberId") or self._query_value(query, "member_id"),
            )
            self._write_response(result)
            return
        if parsed.path in {"/florence/app/chats/messages", "/v1/app/chats/messages"}:
            query = parse_qs(parsed.query)
            limit_raw = self._query_value(query, "limit")
            result = self._service().handle_app_messages(
                channel_id=self._query_value(query, "channelId") or self._query_value(query, "channel_id"),
                limit=int(limit_raw) if limit_raw and limit_raw.isdigit() else 50,
            )
            self._write_response(result)
            return
        self.send_error(404, "not_found")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in {"/florence/app/bootstrap", "/v1/app/bootstrap"}:
            try:
                payload = self._read_json_body()
            except ValueError as exc:
                self._write_response(
                    FlorenceHTTPResult(
                        status_code=400,
                        content_type="application/json; charset=utf-8",
                        body=json.dumps({"ok": False, "error": str(exc)}),
                    )
                )
                return
            result = self._service().handle_app_bootstrap(
                parent_name=_read_str(payload, "parentName", "parent_name"),
                household_name=_read_str(payload, "householdName", "household_name"),
                timezone=_read_str(payload, "timezone"),
            )
            self._write_response(result)
            return
        if parsed.path in {"/florence/app/chats/send", "/v1/app/chats/send"}:
            try:
                payload = self._read_json_body()
            except ValueError as exc:
                self._write_response(
                    FlorenceHTTPResult(
                        status_code=400,
                        content_type="application/json; charset=utf-8",
                        body=json.dumps({"ok": False, "error": str(exc)}),
                    )
                )
                return
            result = self._service().handle_app_send_message(
                household_id=_read_str(payload, "householdId", "household_id"),
                member_id=_read_str(payload, "memberId", "member_id"),
                scope=_read_str(payload, "scope"),
                text=_read_str(payload, "text"),
            )
            self._write_response(result)
            return
        if parsed.path in {"/florence/bluebubbles/webhook", "/v1/channels/bluebubbles/webhook"}:
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
            query = parse_qs(parsed.query)
            result = self._service().handle_bluebubbles_webhook(
                payload=payload,
                webhook_secret=(
                    self.headers.get("x-florence-bluebubbles-secret")
                    or self._query_value(query, "secret")
                    or self._query_value(query, "webhookSecret")
                ),
            )
            self._write_response(result)
            return
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
        self.send_error(404, "not_found")

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        logger.info("%s - %s", self.address_string(), format % args)

    def _read_raw_body(self) -> bytes:
        content_length = int(self.headers.get("content-length") or "0")
        return self.rfile.read(content_length) if content_length > 0 else b"{}"

    def _read_json_body(self) -> dict:
        return self._parse_json_body(self._read_raw_body())

    @staticmethod
    def _parse_json_body(raw: bytes) -> dict:
        try:
            parsed = json.loads(raw.decode("utf-8") or "{}")
        except Exception as exc:
            raise ValueError("invalid_json_body") from exc
        if not isinstance(parsed, dict):
            raise ValueError("json_body_must_be_object")
        return parsed

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

    service = FlorenceProductionService(settings)
    scheduler = FlorenceSyncScheduler(
        service,
        interval_seconds=settings.server.sync_interval_seconds,
    )
    scheduler.start()
    server = build_http_server(service, host=host, port=port)

    logger.info("Florence HTTP server listening on %s:%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Florence server")
    finally:
        server.shutdown()
        scheduler.stop()
        service.close()


if __name__ == "__main__":
    main()
