"""HTTP surface: the Linq webhook, Google OAuth, health, and a small admin API.

The webhook does transport work only — verify, parse, persist, debounce.
Everything conversational happens inside the agent turn.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from florence import db, prompts
from florence.agent import Deps, run_turn
from florence.config import Settings
from florence.gmail import EmailSummary, GoogleService, authorization_url
from florence.linq import (
    LinqClient,
    chat_handles_from_info,
    chat_is_group_from_info,
    normalize_phone,
    parse_linq_event,
    verify_linq_signature,
)
from florence.llm import LLMClient
from florence.runtime import Runtime
from florence.store import Store
from florence.timeutil import now_utc
from florence.triage import gate_email

log = logging.getLogger("florence.app")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

STOP_WORDS = {"stop", "unsubscribe"}
START_WORDS = {"start", "resume", "unstop"}
STOP_CONFIRMATION = (
    "Florence is paused — no more texts from me. Text START anytime to pick back up."
)

_CSS = """
:root{--bg:#faf7f2;--ink:#2b2725;--muted:#6f6660;--accent:#b9542c}
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,system-ui,'Segoe UI',sans-serif;background:var(--bg);
color:var(--ink);margin:0;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:1.5rem}
.card{max-width:26rem;width:100%;text-align:center;padding:2.6rem 2rem;background:#fff;
border-radius:18px;box-shadow:0 2px 24px rgba(43,39,37,.07)}
.wordmark{font-family:Georgia,'Times New Roman',serif;font-size:2rem;margin:0 0 .3rem}
h1{font-size:1.2rem;margin:.3rem 0 .6rem}
p{line-height:1.55;color:var(--muted);margin:.4rem 0}
ul{text-align:left;color:var(--muted);line-height:1.7;padding-left:1.2rem;margin:.9rem 0}
.btn{display:inline-block;margin-top:1.1rem;background:var(--accent);color:#fff;text-decoration:none;
padding:.85rem 1.7rem;border-radius:12px;font-weight:600}
.fine{font-size:.8rem;color:var(--muted);margin-top:1.4rem}
"""


def _page(
    title: str,
    *,
    heading: str | None = None,
    body_html: str = "",
    button: tuple[str, str] | None = None,
    fine: str | None = None,
) -> str:
    button_html = f'<a class="btn" href="{button[1]}">{button[0]}</a>' if button else ""
    heading_html = f"<h1>{heading}</h1>" if heading else ""
    fine_html = f'<p class="fine">{fine}</p>' if fine else ""
    return (
        '<!doctype html><html><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{title}</title><style>{_CSS}</style></head>"
        f'<body><div class="card"><div class="wordmark">Florence</div>'
        f"{heading_html}{body_html}{button_html}{fine_html}</div></body></html>"
    )


def _pretty_phone(e164: str | None) -> str:
    if not e164 or not e164.startswith("+1") or len(e164) != 12:
        return e164 or ""
    return f"({e164[2:5]}) {e164[5:8]}-{e164[8:]}"


def create_app() -> FastAPI:
    settings = Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = await db.open_pool(settings.database_url, settings.db_schema)
        store = Store(pool)
        deps = Deps(
            settings=settings,
            store=store,
            linq=LinqClient(settings),
            gmail=GoogleService(settings, store),
            llm=LLMClient(settings),
        )
        runtime = Runtime(deps)
        runtime.start()
        app.state.deps = deps
        app.state.runtime = runtime
        log.info(
            "florence up: model=%s linq=%s gmail=%s schema=%s",
            settings.model,
            "live" if deps.linq.live else "dry-run",
            "configured" if deps.gmail.configured else "off",
            settings.db_schema,
        )
        try:
            yield
        finally:
            await runtime.stop()
            await pool.close()

    app = FastAPI(title="Florence", lifespan=lifespan, docs_url=None, redoc_url=None)

    def deps() -> Deps:
        return app.state.deps

    def runtime() -> Runtime:
        return app.state.runtime

    # -- health ------------------------------------------------------------------

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        async with deps().store.pool.connection() as conn:
            await conn.execute("SELECT 1")
        return {"ok": True}

    @app.get("/")
    async def index() -> HTMLResponse:
        phone = settings.linq_from_phone
        button = (f"Text Florence · {_pretty_phone(phone)}", f"sms:{phone}") if phone else None
        return HTMLResponse(
            _page(
                "Florence — the family assistant in your texts",
                body_html=(
                    "<p>The family assistant that lives in your texts. Florence carries the "
                    "mental load — she catches the school emails, remembers everything, keeps "
                    "the calendar honest, and nudges the right parent at the right time.</p>"
                    "<ul><li>Works in your family group chat, both parents in sync</li>"
                    "<li>Watches your inbox so permission slips never slip</li>"
                    "<li>Texts you like a person, not an app</li></ul>"
                ),
                button=button,
                fine="Read-only email access. Your family's data stays yours. Text STOP anytime."
                + (f" Questions: {settings.support_contact}" if settings.support_contact else ""),
            )
        )

    # -- Linq webhook ---------------------------------------------------------------

    @app.post("/webhooks/linq")
    async def linq_webhook(request: Request) -> dict[str, Any]:
        raw = await request.body()
        headers = request.headers
        if settings.linq_webhook_secret and not verify_linq_signature(
            secret=settings.linq_webhook_secret,
            raw_body=raw,
            timestamp=headers.get("x-webhook-timestamp"),
            signature=headers.get("x-webhook-signature"),
            webhook_id=headers.get("webhook-id"),
            webhook_timestamp=headers.get("webhook-timestamp"),
            webhook_signature=headers.get("webhook-signature"),
        ):
            raise HTTPException(status_code=401, detail="invalid signature")
        try:
            payload = await request.json()
        except ValueError:
            return {"ok": True, "ignored": "not json"}
        incoming = parse_linq_event(payload, headers, now_utc=now_utc())
        if incoming is None:
            return {"ok": True, "ignored": "not a message"}
        if normalize_phone(incoming.sender) == normalize_phone(settings.linq_from_phone):
            return {"ok": True, "ignored": "own message"}

        d = deps()
        chat_kind = "group" if incoming.is_group else ("direct" if incoming.is_group is False else "unknown")
        household, chat, member, household_created, chat_created = await d.store.attach_inbound(
            chat_id=incoming.chat_id,
            chat_kind=chat_kind,
            sender_phone=incoming.sender,
            default_timezone=settings.default_timezone,
        )

        # Learn the other participants of a new/updated chat (best effort).
        handles = list(incoming.chat_handles)
        if chat_created and not handles:
            try:
                info = await d.linq.get_chat(incoming.chat_id)
                handles = list(chat_handles_from_info(info))
                if chat.kind == "unknown" and chat_is_group_from_info(info) is True:
                    await d.store.attach_inbound(
                        chat_id=incoming.chat_id,
                        chat_kind="group",
                        sender_phone=incoming.sender,
                        default_timezone=settings.default_timezone,
                    )
            except Exception:  # noqa: BLE001
                log.warning("could not fetch chat info for %s", incoming.chat_id)
        our_phone = normalize_phone(settings.linq_from_phone)
        for handle in handles:
            if normalize_phone(handle) != our_phone:
                await d.store.add_known_member(household.id, handle)

        message_id = await d.store.record_message(
            household_id=household.id,
            chat_id=incoming.chat_id,
            direction="inbound",
            body=incoming.text,
            sender_phone=incoming.sender,
            attachments=[a.as_dict() for a in incoming.attachments] or None,
            external_id=incoming.message_id,
            created_at=incoming.received_at,
        )
        if message_id is None:
            return {"ok": True, "duplicate": True}

        if household_created:
            await runtime().ensure_default_routines(household.id)

        normalized = incoming.text.strip().lower()
        if normalized in STOP_WORDS:
            await d.store.set_household(household.id, stopped=True)
            await d.linq.send_text(
                chat_id=incoming.chat_id, text=STOP_CONFIRMATION, idempotency_key=f"stop-{incoming.message_id}"
            )
            await d.store.record_message(
                household_id=household.id,
                chat_id=incoming.chat_id,
                direction="outbound",
                body=STOP_CONFIRMATION,
                sender_name="Florence",
            )
            await d.store.log_event("stopped", household_id=household.id)
            return {"ok": True, "stopped": True}
        if household.stopped:
            if normalized in START_WORDS:
                await d.store.set_household(household.id, stopped=False)
                await d.store.log_event("resumed", household_id=household.id)
                runtime().enqueue_inbound(incoming.chat_id)
            return {"ok": True}

        runtime().enqueue_inbound(incoming.chat_id)
        return {"ok": True}

    # -- Google OAuth -----------------------------------------------------------------

    def _expired_page() -> HTMLResponse:
        return HTMLResponse(
            _page(
                "Link expired",
                heading="This link has expired",
                body_html="<p>No worries — just text Florence and ask for a fresh connect link.</p>",
            ),
            status_code=400,
        )

    @app.get("/connect/google")
    async def connect_google(s: str = "") -> HTMLResponse:
        row = await deps().store.peek_oauth_state(s)
        if row is None:
            return _expired_page()
        return HTMLResponse(
            _page(
                "Connect Google to Florence",
                heading="Connect your Google account",
                body_html=(
                    "<p>Here's exactly what Florence gets:</p>"
                    "<ul><li><b>Reads</b> your email to catch school and activity stuff — she can "
                    "never send, delete, or touch anything</li>"
                    "<li>Sees your calendar and can add events you ask for</li>"
                    "<li>Disconnect anytime — just text Florence</li></ul>"
                ),
                button=("Continue with Google", f"/connect/google/go?s={s}"),
                fine="Your email is never used to train anything and never leaves Florence.",
            )
        )

    @app.get("/connect/google/go")
    async def connect_google_go(s: str = "") -> RedirectResponse:
        row = await deps().store.peek_oauth_state(s)
        if row is None:
            return _expired_page()  # type: ignore[return-value]
        return RedirectResponse(authorization_url(settings, s), status_code=302)

    @app.get("/oauth/google/callback")
    async def google_callback(
        state: str = "", code: str = "", error: str = ""
    ) -> HTMLResponse:
        d = deps()
        if error or not code:
            return HTMLResponse(
                _page(
                    "Connection cancelled",
                    heading="No problem",
                    body_html="<p>Nothing was connected. Text Florence whenever you'd like to try again.</p>",
                ),
                status_code=400,
            )
        row = await d.store.consume_oauth_state(state)
        if row is None:
            return _expired_page()
        try:
            result = await d.gmail.exchange_code(code)
        except Exception as exc:  # noqa: BLE001
            log.exception("google oauth exchange failed")
            await d.store.log_event("gmail_connect_failed", household_id=row.household_id, payload={"error": str(exc)})
            return HTMLResponse(
                _page(
                    "Something went wrong",
                    heading="That didn't go through",
                    body_html="<p>Google didn't complete the connection. Text Florence and she'll send a fresh link.</p>",
                ),
                status_code=500,
            )
        await d.store.upsert_gmail_account(
            household_id=row.household_id,
            member_phone=row.member_phone,
            email=result["email"],
            google_sub=result["google_sub"],
            token_ciphertext=result["ciphertext"],
            scopes=result["scopes"],
        )
        await d.store.log_event(
            "gmail_connected", household_id=row.household_id, payload={"email": result["email"]}
        )
        by_name = None
        if row.member_phone:
            member = await d.store.member_by_phone(row.member_phone)
            by_name = member.name if member else None
        asyncio.create_task(
            runtime().gmail_connected_turn(row.household_id, row.chat_id, result["email"], by_name)
        )
        return HTMLResponse(
            _page(
                "Connected",
                heading="✓ You're connected",
                body_html=(
                    f"<p>Florence is now watching <b>{result['email']}</b> — read-only, always.</p>"
                    "<p>Close this and check your texts; she'll confirm in the chat.</p>"
                ),
            )
        )

    # -- admin -------------------------------------------------------------------------

    def _require_admin(request: Request) -> None:
        expected = settings.admin_api_key
        provided = (request.headers.get("authorization") or "").removeprefix("Bearer ").strip()
        if not expected or provided != expected:
            raise HTTPException(status_code=401, detail="unauthorized")

    @app.get("/admin/overview")
    async def admin_overview(request: Request) -> dict[str, Any]:
        _require_admin(request)
        d = deps()
        return {
            "counts": await d.store.counts(),
            "events": await d.store.recent_events(30),
            "model": settings.model,
            "linq_live": d.linq.live,
            "gmail_configured": d.gmail.configured,
        }

    @app.post("/admin/test-turn")
    async def admin_test_turn(request: Request) -> JSONResponse:
        """Run a full agent turn in a sandbox household (no real texts sent)."""
        _require_admin(request)
        body = await request.json()
        message = str(body.get("message") or "").strip()
        if not message:
            raise HTTPException(status_code=400, detail="message is required")
        suffix = str(body.get("sandbox") or "admin")
        chat_id = f"sandbox-{suffix}"
        phone = str(body.get("phone") or "+19999999999")
        d = deps()
        household, chat, member, household_created, _ = await d.store.attach_inbound(
            chat_id=chat_id,
            chat_kind="direct",
            sender_phone=phone,
            default_timezone=settings.default_timezone,
        )
        await d.store.record_message(
            household_id=household.id,
            chat_id=chat_id,
            direction="inbound",
            body=message,
            sender_phone=phone,
        )
        result = await run_turn(d, household=household, chat=chat, member=member)
        return JSONResponse(
            {
                "replies": [s["text"] for s in result.sent],
                "trace": result.trace,
                "steps": result.steps,
                "error": result.error,
                "sandbox_household": household.id,
            }
        )

    @app.post("/admin/gmail-sync")
    async def admin_gmail_sync(request: Request) -> dict[str, Any]:
        _require_admin(request)
        await runtime()._gmail_tick()  # noqa: SLF001 - deliberate admin poke
        return {"ok": True}

    @app.post("/admin/test-triage")
    async def admin_test_triage(request: Request) -> JSONResponse:
        """Run emails through the gatekeeper only (no agent turn, no texts)."""
        _require_admin(request)
        body = await request.json()
        raw_items = body.get("emails")
        if not isinstance(raw_items, list) or not raw_items:
            raise HTTPException(status_code=400, detail="emails: [{from, subject, snippet}] required")
        d = deps()
        memories = []
        suffix = body.get("sandbox")
        if suffix:
            resolved = await d.store.household_for_chat(f"sandbox-{suffix}")
            if resolved is not None:
                memories = await d.store.list_memories(resolved[0].id)
        decisions = []
        for n, i in enumerate(raw_items):
            if not isinstance(i, dict):
                continue
            item = EmailSummary(
                account_email=str(i.get("account") or "parent@gmail.com"),
                gmail_id=str(i.get("id") or f"test-{n}"),
                sender=str(i.get("from") or "unknown"),
                subject=str(i.get("subject") or "(no subject)"),
                snippet=str(i.get("snippet") or ""),
                received_at=now_utc(),
            )
            decision = await gate_email(
                d.llm, item=item, memories=memories, triage_model=settings.triage_model
            )
            decisions.append(
                {
                    "subject": item.subject,
                    "notify": decision.notify,
                    "justification": decision.justification,
                    "summary": decision.summary,
                }
            )
        return JSONResponse({"decisions": decisions, "triage_model": settings.triage_model})

    @app.post("/admin/test-email")
    async def admin_test_email(request: Request) -> JSONResponse:
        """Replay an email-triage turn in a sandbox household (no real texts)."""
        _require_admin(request)
        body = await request.json()
        raw_items = body.get("emails")
        if not isinstance(raw_items, list) or not raw_items:
            raise HTTPException(status_code=400, detail="emails: [{from, subject, snippet, account}] required")
        suffix = str(body.get("sandbox") or "admin")
        d = deps()
        household, chat, _, _, _ = await d.store.attach_inbound(
            chat_id=f"sandbox-{suffix}",
            chat_kind="direct",
            sender_phone=str(body.get("phone") or "+19999999999"),
            default_timezone=settings.default_timezone,
        )
        items = [
            EmailSummary(
                account_email=str(i.get("account") or "parent@gmail.com"),
                gmail_id=str(i.get("id") or f"test-{n}"),
                sender=str(i.get("from") or "unknown"),
                subject=str(i.get("subject") or "(no subject)"),
                snippet=str(i.get("snippet") or ""),
                received_at=now_utc(),
            )
            for n, i in enumerate(raw_items)
            if isinstance(i, dict)
        ]
        result = await run_turn(
            d,
            household=household,
            chat=chat,
            directive=prompts.email_directive(items, household.timezone),
        )
        return JSONResponse(
            {
                "replies": [s["text"] for s in result.sent],
                "trace": result.trace,
                "steps": result.steps,
                "error": result.error,
            }
        )

    return app
