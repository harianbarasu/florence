# Deployment Notes

For the plain-English multi-family topology, read
[SaaS Deployment Runbook](saas-deployment.md) first. This file is the detailed
settings and smoke-test reference.

Florence has three public surfaces:

- `POST /webhooks/linq` for Linq webhooks.
- `POST /api/source-items` for trusted external source automation.
- `/dev/*` for local smoke tests and operator control-plane checks.

## Required Production Settings

Set these before exposing Florence beyond a local machine:

```bash
LINQ_WEBHOOK_SECRET=...
LINQ_API_KEY=...
LINQ_FROM_PHONE=...
FLORENCE_ADMIN_API_KEY=...
FLORENCE_SOURCE_INGEST_API_KEY=...
FLORENCE_TOKEN_ENCRYPTION_KEY=...
FLORENCE_SUPPORT_CONTACT=support@example.com
FLORENCE_DATABASE_URL=postgresql://user:password@host:5432/florence
INSTALL_HERMES_AGENT=1
HERMES_AGENT_REPO=https://github.com/NousResearch/hermes-agent.git
HERMES_AGENT_REF=<full pinned commit sha>
FLORENCE_HERMES_AGENT_PATH=/opt/hermes-agent
FLORENCE_HERMES_PROVIDER=openrouter
FLORENCE_HERMES_MODEL=<model id>
FLORENCE_HERMES_TOOLSETS=
FLORENCE_HERMES_RUNTIME_HOME=/tmp/florence-hermes-home
FLORENCE_HERMES_STRICT=1
FLORENCE_LINQ_LIVE_VERIFIED=0
FLORENCE_LINQ_LIVE_VERIFIED_AT=
FLORENCE_LINQ_LIVE_VERIFICATION_PROOF=
FLORENCE_GOOGLE_LIVE_VERIFIED=0
FLORENCE_GOOGLE_LIVE_VERIFIED_AT=
FLORENCE_GOOGLE_LIVE_VERIFICATION_PROOF=
FLORENCE_HERMES_LIVE_VERIFIED=0
FLORENCE_HERMES_LIVE_VERIFIED_AT=
FLORENCE_HERMES_LIVE_VERIFICATION_PROOF=
```

Optional runtime safety knobs:

```bash
FLORENCE_DATA_DELETION_CONFIRMATION_TTL_MINUTES=30
```

`/webhooks/linq` is authenticated with Linq's HMAC signature. Local SQLite smoke
runs may omit `LINQ_WEBHOOK_SECRET`, but Postgres-backed runtimes reject Linq
webhooks until the secret is configured.
`/api/source-items` requires `FLORENCE_SOURCE_INGEST_API_KEY`. `/health` is
unauthenticated by design.

`/api/source-items` is intentionally a bounded summary API. It rejects oversized
source fields with `413`; upstream automations should summarize raw email,
flyers, PDFs, or webhook payloads before sending them to Florence.

`LINQ_FROM_PHONE` must be one of the phone numbers assigned to the Linq partner
account. Florence uses it when a parent texts `invite partner +...` to create
the shared iMessage household thread.

`FLORENCE_SUPPORT_CONTACT` is returned when someone texts `support`, `human`, or
`talk to a human`. Configure it before any external pilot so the iMessage
assistant is not a closed loop.

Generate `FLORENCE_TOKEN_ENCRYPTION_KEY` with:

```bash
python -c "from florence.oauth import TokenVault; print(TokenVault.generate_key())"
```

Treat it like a production secret. If the key is lost, stored connected-source
tokens cannot be decrypted. `/dev/deployment-check` validates that the value is
a real Fernet key before a Postgres pilot can be marked ready.

`FLORENCE_DATABASE_URL` must point to Postgres for deployed pilots. The
application still supports `FLORENCE_DB_PATH` for local SQLite development and
unit tests, but `/dev/pilot-check/{chat_id}` requires a Postgres URL before it
reports a household as pilot-ready. SQLite is configured as a direct file path;
URL-style values such as `sqlite:///...` are rejected at startup so a deploy
cannot silently boot the app and worker against a local file database.
Postgres-backed Hermes traffic also requires POSIX `fcntl` interprocess file
locking. If `/dev/hermes-status` or `/dev/deployment-check` reports
`thread_lock_only_no_interprocess_lock`, pilot readiness stays blocked because
Hermes uses process-global `HERMES_HOME` runtime state.
Use a fresh Florence database for pilots. At startup, Florence validates that any
pre-existing tables with Florence table names have the current required columns;
an older database with reused names, such as an old `messages` table keyed by
`family_id` instead of `household_id`, fails fast with `DatabaseSchemaError`
rather than failing later during parent traffic. `/dev/deployment-check` also
reports `database.schema_ready` and blocks pilot readiness when a store override
or managed database does not match the current schema. Treat reused legacy data
as a deliberate migration project, not as a staging shortcut.

Postgres-backed `/dev/*` write routes are still operator-only, but they should
not silently create a placeholder tenant. Routes such as `/dev/messages`,
`/dev/source-items`, `/dev/import/email`, `/dev/import/calendar`,
`/dev/sync-sources`, `/dev/oauth/google/start`, and `/dev/actions` require an
explicit `chat_id` when `FLORENCE_DATABASE_URL` is Postgres. The `dev-chat`
fallback is reserved for local SQLite smoke tests.

`FLORENCE_HERMES_TOOLSETS` must be empty for multi-family SaaS pilots. Do not
enable Hermes toolsets until each tool has an audited household-level tenant
boundary, export path, deletion path, provenance model, and no path to send raw
household context to external services. Florence owns Google, source, web, and
external-action integrations. The Hermes adapter also fails closed at runtime
before loading Hermes if any toolset is configured, so a misconfigured deploy
cannot silently grant Hermes external tools.

There is no hosted Hermes API to call. In Docker deploys, set
`INSTALL_HERMES_AGENT=1` and pin `HERMES_AGENT_REF` to the exact upstream Hermes
commit you want baked into the image. Use the full 40-character SHA-1 or
64-character SHA-256 object id, not an abbreviated hash. The Dockerfile clones
`HERMES_AGENT_REPO`, installs it into the same Python environment as Florence,
places the checkout at `/opt/hermes-agent`, and writes the installed commit to
`/opt/hermes-agent/.florence-hermes-ref`. The image build fails before cloning if
`INSTALL_HERMES_AGENT=1` and `HERMES_AGENT_REF` is blank, abbreviated, or not a
full commit id. `FLORENCE_HERMES_AGENT_PATH` must point to that checkout,
or to another mounted checkout where
`run_agent.AIAgent` can be imported by the Florence process and exposes the
constructor plus `run_conversation(...)` shape used by Florence's adapter.
Florence calls that constructor with `skip_memory=True`,
`save_trajectories=False`, and a fresh `florence-turn-*` session id so durable
SaaS memory and transcript state stay in Florence/Postgres instead of Hermes
native session memory. Florence also sets `HERMES_HOME` to a fresh per-turn
directory under `FLORENCE_HERMES_RUNTIME_HOME` before importing Hermes, then
removes that directory after the call. Use a deployment-owned absolute scratch
base, such as `/tmp/florence-hermes-home`, so Hermes default logs or session
files do not land in a shared `~/.hermes` profile or a cross-family scratch
bucket. Because `HERMES_HOME` is process-global, Florence serializes every Hermes
runtime context with an in-process thread lock plus an interprocess file lock
under `FLORENCE_HERMES_RUNTIME_HOME`, including operator preflight/status
imports.
Those preflight/status imports use fresh `florence-preflight-*` runtime
directories under the same base, so multiple app workers do not share one fixed
preflight scratch home.
Florence also adds the configured Hermes checkout to Python's import path only
while importing or running Hermes, then removes it afterward. Same-named
pre-existing modules from the checkout root are temporarily shadowed during the
call, and newly imported Python modules loaded from that checkout are cleared
from `sys.modules` after each preflight or parent turn. Module globals
initialized under one runtime home cannot leak into another family turn.
`/dev/pilot-check/{chat_id}` also requires `HERMES_AGENT_REF` to be a full pinned
Git commit SHA, not a branch such as `main` or an abbreviated hash, so a pilot
can be reproduced against the same Hermes code later. The deployment preflight
also compares the configured ref exactly against the checkout marker or git
`HEAD`, and blocks readiness if the running Hermes checkout does not match the
pinned ref. It also requires
explicit `FLORENCE_HERMES_PROVIDER` and `FLORENCE_HERMES_MODEL` values so the
deployed adapter does not depend on a hidden local Hermes default. Ambient
Python `run_agent` imports are local SQLite development only; Postgres-backed
SaaS traffic requires `FLORENCE_HERMES_AGENT_PATH` and will not use an ambient
module for parent turns. The runtime adapter enforces the same Postgres contract
before importing Hermes, so a bypassed preflight cannot accidentally run a
floating checkout, implicit model default, or shared local Hermes profile.

The `FLORENCE_*_LIVE_VERIFIED` flags are not credentials. Leave them at `0`
until the staging smoke checklist has exercised a real Linq send/webhook
round-trip, a real Google OAuth/source sync, and a real Hermes response through
the Florence adapter. The preferred SaaS path is to persist live proof in the
shared Florence database automatically: the signed Linq webhook records `linq`
proof after a real non-dry-run outbound Linq send, the worker records `google`
proof after the default Google OAuth source-sync path imports, surfaces, and
delivers a token-backed item through real Linq, and `/dev/hermes-smoke/{chat_id}`
records `hermes` proof when it returns `live_hermes_verified: true` without
fallback. `POST /dev/live-verifications/{name}` remains available as an
operator fallback; use `name` values `linq`, `google`, or `hermes`.
`GET /dev/live-verifications` shows the stored proof records.
`/dev/pilot-check/{chat_id}` will not report `pilot_ready: true` while any of
these live checks are still unverified. Env flags still work as a deploy-time
fallback; when a flag is set to `1`, set the matching `*_LIVE_VERIFIED_AT` to an
ISO-8601 timestamp with timezone and `*_LIVE_VERIFICATION_PROOF` to a short
proof note. The proof note can be the smoke chat id, a saved
`/dev/hermes-smoke` response reference, a Google source-sync run id, or a
link/reference in the operator runbook. The Hermes smoke response intentionally
excludes the model response body. Proof notes must not contain secrets, tokens,
phone numbers, email addresses, raw JSON, line breaks, or raw family/source
content.

## Google Connected Sources

Set these before enabling Google account connection:

```bash
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
GOOGLE_REDIRECT_URI=https://your-domain.example.com/oauth/google/callback
GOOGLE_OAUTH_SCOPES=openid,email,https://www.googleapis.com/auth/gmail.readonly,https://www.googleapis.com/auth/calendar.readonly
```

The Google OAuth client must list the exact redirect URI. Gmail and Calendar
read-only scopes may require Google OAuth app verification before broad public
launch. For early pilots, keep access limited to configured test users until the
consent screen and verification state are ready.

Once configured, a parent starts connection from iMessage with `connect google`.
A parent can later text `disconnect google` to disable the household Google
account, remove the stored OAuth token, and stop Gmail/Calendar polling. That
disconnect command is still accepted after the household has texted `stop` so
source access can be revoked without first restarting Florence.

The `/dev/oauth/google/start` endpoint is still available for smoke tests, but
it should not be the primary household setup path.

## Dev/Control-Plane Endpoints

When `FLORENCE_ADMIN_API_KEY` is set, every `/dev/*` route requires one of:

```http
Authorization: Bearer <key>
X-Florence-Admin-Key: <key>
```

When `FLORENCE_DATABASE_URL` points to Postgres, `/dev/*` routes require
`FLORENCE_ADMIN_API_KEY`; they do not fall back to open local-smoke behavior.

For stricter deployments, disable the routes entirely:

```bash
FLORENCE_DEV_ENDPOINTS_ENABLED=0
```

Use disabled `/dev/*` routes when a separate authenticated admin UI or job runner
exists. Use the API-key guard when deploying early pilots where the endpoints are
still needed for smoke tests, source review, action ticks, and memory inspection.
Read-only control-plane routes for connected accounts, privacy, readiness,
source review, memory, pending actions, and action executions require an
existing household chat and return `household_not_found` instead of creating a
household during inspection.

## Source Ingest API

Trusted automations can post typed household source items to
`POST /api/source-items` with one of:

```http
Authorization: Bearer <FLORENCE_SOURCE_INGEST_API_KEY>
X-Florence-Source-Key: <FLORENCE_SOURCE_INGEST_API_KEY>
```

The endpoint requires `chat_id`, `source_type`, `title`, and `external_id`.
`chat_id` must already resolve to a known Florence household; this API does not
create households. `external_id` is the idempotency key for that source system.
Optional fields are `body`, `sender`, `observed_at_utc`, `event_at_utc`, and
`now_utc` for tests or backfills. The endpoint rejects broad `message` or
`instruction` payloads; it stores the typed source item, runs Need-to-Know, and
only texts the household when the item is timely and actionable. Stopped
households are stored quietly. If the Linq send fails after a surface-worthy
item is stored, the source system can safely repost the same `chat_id`,
`source_type`, and `external_id`; Florence will retry the existing outbound
without duplicating the source item or reminder approval.

## Worker

Run the worker separately from the web process:

```bash
florence-worker
```

The worker executes due reminders, non-empty daily briefings, and approved
actions. Stopped households are skipped for reminder delivery, briefings,
approved-action execution, and connected-source polling. The worker also polls
connected sources every `FLORENCE_SOURCE_SYNC_INTERVAL_SECONDS` seconds,
defaulting to 300. Set the
interval to `0` if a separate scheduler owns source polling and calls
`florence.worker.run_source_sync_tick(...)` directly.
Worker send loops continue after individual Linq delivery failures and report
delivery-failure counts from routine/source ticks. A transient Linq, source, or
action failure should leave the affected work retryable without stopping
processing for every other household in the shared SaaS worker. The long-running
worker loop also logs unexpected tick-level failures and continues.

Due reminders are marked sent only after the outbound sender returns
successfully. If Linq fails during a worker tick, the failed reminder remains
pending inside the delivery grace window and can be retried with the same Linq
idempotency key on the next tick.

Daily briefing routine claims are also committed only after the outbound sender
succeeds. A failed briefing send does not consume that household's daily
briefing slot or mark included source items as briefed, and the retry uses a
deterministic household/date idempotency key. Briefings are also bounded by
`FLORENCE_DAILY_BRIEFING_DELIVERY_GRACE_MINUTES`, defaulting to four hours, so
a missed morning briefing is skipped instead of arriving later as stale context.

Immediate connected-source surfacing is also backed by outbound delivery state.
If a worker imports a source item, advances the provider cursor, and then Linq
delivery fails, Florence keeps the exact source-surface payload and retries it
on a later source-sync tick without re-importing the source item or creating a
second reminder approval. During source-sync ticks, Florence prepares all
retryable and newly imported source-surface payloads before starting network
sends, so an older retry failure cannot strand a newer imported source item
after the provider cursor has advanced.

Google OAuth confirmation texts use the same outbound delivery table. If the
OAuth exchange succeeds but Linq is temporarily unavailable, Florence keeps the
connection, returns a successful browser callback page, records the failed
`oauth:` confirmation, and retries it from the worker with the same idempotency
key. If a parent disconnects Google before a retry succeeds, Florence cancels
the stale OAuth/source deliveries so the pilot check is not blocked by work the
household has revoked.

## Pilot Smoke Checklist

For a local rehearsal before touching live credentials, run:

```bash
florence local-smoke
```

Pass `--now-utc 2026-06-05T16:00:00+00:00` when you want a reproducible JSON
artifact for comparison or review.

The command uses signed fake Linq webhooks, a fake Linq sender, an encrypted fake
Google token, and a fake connected Google source provider. It exercises
two-parent setup, source sync, Need-to-Know surfacing, parent approval, worker
execution, reminder delivery, `/dev/pilot-check/{chat_id}`, and the sanitized
`/dev/pilot-proof/{chat_id}` artifact in one process. It is intentionally marked
`mode: local_only` with
`live_verification_performed: false`; it does not replace the live Linq,
Google OAuth, Hermes, or managed Postgres checks below. Use the returned
`staging_verification_checklist` as the copyable live-smoke sequence: it names
the endpoints, expected fields, Docker build args, runtime env vars, and proof
records to satisfy in staging. The live service steps separate `credential_env`
from `proof_record`: set credentials first, then let the real Linq webhook,
source-sync worker, and Hermes smoke endpoint write `live_verifications` rows.
`fallback_env` lists the deploy-time flag fallback only.
`configuration_preflight` is the early config check; `deployment_ready` is the
final check after live verification records exist. Replace `{staging_chat_id}`
with the real Linq chat id from the staging iMessage thread; the local smoke
`chat_id` is fake rehearsal evidence only.

After the live staging steps have been run, verify the deployed endpoints with:

```bash
FLORENCE_ADMIN_API_KEY=... \
  florence staging-check \
  --base-url https://your-staging-domain.example.com \
  --chat-id <real-linq-staging-chat-id>
```

The command calls `/health`, `/dev/hermes-status`, `/dev/hermes-smoke/{chat_id}`,
`/dev/deployment-check`, `/dev/pilot-check/{chat_id}`, and
`/dev/pilot-proof/{chat_id}`. Deployment check runs after Hermes smoke so a
successful smoke can persist the `hermes` proof record before readiness is read.
It exits non-zero until the deployment has live Linq, Google, Hermes, managed
Postgres, and sanitized pilot-proof evidence. Its JSON output is meant to be
shareable inside the operator runbook: it summarizes counts and blockers but
does not copy raw source titles, email bodies, message bodies, or OAuth tokens.
On failure, read the top-level `next_actions` list first; it combines
deployment-check and pilot-check blockers into one operator work queue.

Before a real family pilot, verify one complete household loop in staging:

1. Set `LINQ_WEBHOOK_SECRET`, `LINQ_API_KEY`, `LINQ_FROM_PHONE`,
   `FLORENCE_SOURCE_INGEST_API_KEY`, `FLORENCE_ADMIN_API_KEY`,
   `FLORENCE_TOKEN_ENCRYPTION_KEY`, `FLORENCE_SUPPORT_CONTACT`,
   `FLORENCE_DATABASE_URL`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`,
   `GOOGLE_REDIRECT_URI`, `INSTALL_HERMES_AGENT`, `HERMES_AGENT_REF`,
   `FLORENCE_HERMES_AGENT_PATH`, `FLORENCE_HERMES_PROVIDER`, and
   `FLORENCE_HERMES_MODEL`. Keep `FLORENCE_HERMES_STRICT=1` for Postgres
   pilots so Hermes contract or runtime failures do not fall back silently.
2. Start the web process and worker against the same Postgres database.
3. From the Linq/iMessage thread, have the first parent text `my name is Sam`,
   then `confirm partner +15555550101`; have the second parent text
   `my name is Alex`; then add one child with `our child is Maya`.
4. Have a parent text `always tell me about permission slips`, then
   `connect google`; complete the Google OAuth redirect in a browser and confirm
   `/dev/connected-accounts/{chat_id}` lists the active Google account.
5. Put a controlled future permission-slip-style email or calendar item in that
   Google test account, run the source-sync worker, and confirm Florence texts
   only the short Need-to-Know summary and approval code, not a raw email dump.
   `POST /api/source-items` remains useful for a separate trusted-automation
   smoke, but it is not a substitute for the Google live-verification step.
6. Reply from a parent with `approve <code>`, run the action worker tick, then
   run the reminder tick at the source due time and confirm the household gets
   the reminder.
7. Run `GET /dev/hermes-status` with the admin key. Confirm
   `mode: configured_checkout`, `contract_ok: true`, `ready_for_saas_pilot: true`,
   the expected `agent_path` and `run_agent_path`, `hermes_ref_matches: true`, an
   empty `toolsets` list, `database_backend: postgres`, and
   `memory_owner: florence`. This checks the configured Hermes checkout and SaaS
   boundary without requiring an existing household or texting a family. It also
   reports `runtime_home`,
   `preflight_runtime_home_scope`, `turn_runtime_home_scope`,
   `turn_runtime_cleanup`, `turn_failure_cleanup`, `turn_runtime_concurrency`, `runtime_lock`,
   `runtime_env_var`,
   `runtime_home_writable`, `python_path_scope`, and `module_cache_scope`; these
   must show writable per-check/per-turn scratch isolation with thread and
   interprocess file-lock serialization around `HERMES_HOME` (`runtime_lock:
   thread_lock_plus_interprocess_file_lock`), cleanup after failed strict Hermes
   turns, temporary checkout import path scoping, and shadowed/cleared checkout
   module caches.
8. Run `GET /dev/deployment-check` with the admin key. This global preflight
   checks production credentials, Postgres mode, Hermes checkout compatibility,
   an active database reachability probe, and live-verification evidence without
   requiring or creating a household. Its `database` object compares the
   configured backend to the store backend used by this running process. Before
   live smoke completion, expect it to list the Linq, Google, and Hermes live
   checks as unverified.
9. Run `POST /dev/hermes-smoke/{chat_id}` with the admin key and
   `FLORENCE_HERMES_AGENT_PATH` configured after the household has at least one
   parent member. The endpoint calls the same agent boundary without writing a
   transcript message or texting the family. Confirm `live_hermes_verified:
   true`, `response: null`, `response_present: true`,
   `sanitization.response: excluded`, `used_fallback: false`, embedded
   `hermes.database_backend: postgres`, `hermes.ready_for_saas_pilot: true`,
   `hermes.strict_mode: true`, and
   `hermes.turn_failure_cleanup` as
   `runtime_home_restored_and_checkout_modules_cleared_on_error`, and
   `stored_live_verification.name: hermes`. Linq proof should already be stored
   when the real signed webhook produced a real Linq outbound send; Google proof
   should already be stored when the worker's default Google source sync
   imported, surfaced, and delivered a token-backed item through real Linq. If an
   operator must record proof manually, use `POST /dev/live-verifications/linq`
   or `POST /dev/live-verifications/google` with `verified_at_utc` and a short
   `proof` note. Timestamps must be ISO-8601 values with a timezone and must not
   be future-dated. Proof notes must be short references, not raw endpoint
   payloads or family/source content.
   If the endpoint returns `ok: false`, use its sanitized `error` and embedded
   `hermes.invalid` fields to fix the checkout/provider configuration before
   recording live Hermes proof.
10. Run `GET /dev/deployment-check` again and require `deployment.ready: true`.
    If it is false, read `deployment.operator_next_steps` first. It lists the
    missing production env vars, database reachability blockers, invalid Hermes
    preflight failures, and the exact live-verification proof records or
    fallback fields still needed for Linq, Google, or Hermes.
11. Check `/dev/pilot-check/{chat_id}` with the admin key. It combines household
   readiness with deployment preflight and reports missing production
   credentials separately from household setup gaps. Its `live_verification`
   section lists what the local regression suite covers, which external
   credentials are still needed, and which live checks remain blocked. Its
   top-level `operator_next_steps` section flattens the remaining deployment and
   pilot smoke blockers into the next actions an operator should take. Its
   `message_transport` section must show at least one inbound iMessage and at
   least one successfully recorded outbound iMessage within seven days of
   `deployment.live_verification.evidence.linq.verified_at_utc`, without
   exposing message bodies. Its `connected_accounts` section must show at least one active Google
   account backed by an encrypted OAuth token. Its `source_review` section must
   show at least one stored and surfaced Need-to-Know item from that active
   OAuth-token-backed Google account, and the account's latest source sync must
   be within seven days of the recorded Google live-verification timestamp. That
   keeps the smoke from being just account connection, manual source ingest, or a
   stale disconnected account. Its
   `delivery` section must be ready with zero
   pending/failed outbound deliveries before calling the household pilot-ready.
   Its `actions` section must also show at least one successful parent-approved
   action execution, with zero approved-but-unrun actions and zero failed
   executions. Its `smoke_checklist` section provides stable
   step ids and blocker strings for the operator proof record. Its `deployment`
   section also verifies that
   `run_agent.AIAgent` imports from the configured Hermes checkout, that the
   checkout is pinned by `HERMES_AGENT_REF`, that provider/model routing is
   explicit, and that it matches Florence's adapter contract.
12. Save `GET /dev/pilot-proof/{chat_id}` as the smoke proof record. It combines
   the pilot checklist, live-verification state, privacy state, source-review
   counts, and action execution statuses while excluding message bodies, source
   titles/bodies/event times, OAuth tokens, and household memory text. The proof
   reports whether an action execution had an error without including the raw
   error text, and it redacts configured secrets, DSNs, bearer tokens, phone
   numbers, and email addresses from deployment/checklist diagnostic strings.
   The proof is scoped to the requested Linq `chat_id`; it is not a
   cross-household report.
13. Check `/dev/source-review/{chat_id}`, `/dev/actions/{chat_id}/executions`,
   `/dev/privacy/{chat_id}`, and `/dev/readiness/{chat_id}` with the admin key
   when you need the underlying details.

The automated regressions for this surface are:

- `tests/test_app.py::test_pilot_smoke_path_from_linq_to_source_approval_worker_and_reminder`
  uses a signed Linq webhook, public source ingest, parent approval over Linq,
  the action tick, and the reminder tick with a fake Linq sender.
- `tests/test_smoke.py::test_local_pilot_smoke_rehearses_household_source_approval_and_reminder`
  verifies the packaged `florence local-smoke` rehearsal crosses signed Linq,
  a token-backed connected-source worker, approval, action execution, reminder
  delivery, and the pilot checklist while clearly reporting that live
  verification was not performed.
- `tests/test_app.py::test_pilot_smoke_path_from_linq_to_connected_source_worker_and_reminder`
  uses signed Linq setup, a connected Google source account, the source-sync
  worker, parent approval over Linq, the action tick, and the reminder tick with
  a fake provider and fake Linq sender.
- `tests/test_app.py::test_linq_webhook_agent_proposal_stays_bounded_by_approval_and_worker`
  uses a fake agent backend through the same app factory boundary as Hermes,
  then verifies Florence strips hidden proposal JSON, creates a pending action,
  requires parent approval, and executes the reminder through the worker.
- `tests/test_app.py::test_linq_webhook_retry_sends_failed_outbound_without_duplicate_side_effects`
  verifies a retried Linq webhook resends a failed outbound reply without
  creating a duplicate reminder.
- `tests/test_app.py::test_linq_webhook_retry_creates_partner_group_without_repeating_ack`
  verifies a failed partner group creation can be retried without repeating the
  first acknowledgement text.
- `tests/test_app.py::test_pilot_check_passes_when_household_and_deployment_are_ready`
  verifies the pilot preflight only passes when the household is ready and the
  required Linq, source-ingest, support, token, Postgres, Hermes, and
  live-verification settings are present, with household-level inbound/outbound
  text transport evidence and a surfaced Need-to-Know source item in the
  machine-readable smoke checklist. It also verifies `/dev/pilot-proof/{chat_id}`
  returns a saveable sanitized proof record without source titles/bodies/event
  times, OAuth token payloads, raw action execution errors, or unsafe diagnostic
  strings.
- `tests/test_app.py::test_pilot_check_blocks_household_without_successful_outbound_message`
  verifies the pilot preflight blocks a household that has setup data but no
  successfully recorded outbound iMessage.
- `tests/test_app.py::test_deployment_check_reports_global_gaps_without_household`
  verifies the operator deployment check reports global missing credentials and
  live-verification blockers without requiring or creating a household.
- `tests/test_app.py::test_deployment_check_passes_when_global_settings_and_live_checks_are_ready`
  verifies the global deployment preflight passes when production settings,
  Postgres mode, Hermes compatibility, and live-verification flags are present.
- `tests/test_app.py::test_dev_hermes_smoke_uses_agent_without_persisting_turn`
  verifies the operator Hermes smoke endpoint calls the configured agent
  boundary without adding transcript messages.
- `tests/test_app.py::test_dev_hermes_smoke_reports_fallback_as_unverified`
  verifies the Hermes smoke endpoint does not treat Florence's deterministic
  fallback as a live Hermes response.
- `tests/test_app.py::test_dev_hermes_smoke_requires_ready_hermes_status_for_live_verification`
  verifies a non-fallback response does not count as live Hermes proof unless
  the configured checkout, pinned ref, provider/model, and SaaS boundary checks
  are ready.
- `tests/test_app.py::test_dev_hermes_status_reports_local_checkout_without_claiming_saas_ready`
  verifies a local SQLite Hermes checkout can be inspected without being marked
  SaaS-ready.
- `tests/test_app.py::test_dev_hermes_status_reports_postgres_checkout_ready_without_household`
  verifies the operator Hermes status endpoint reports the configured checkout,
  pinned ref, provider/model, empty toolsets, Postgres backend, and
  Florence-owned memory/session boundary without creating a household.
- `tests/test_app.py::test_dev_hermes_status_blocks_missing_path_as_ambient_import_for_pilot`
  verifies an ambient Python `run_agent` import is not treated as a pilot-ready
  pinned Hermes checkout.
- `tests/test_app.py::test_pilot_check_blocks_configured_but_unverified_external_services`
  verifies fake credential-shaped values do not make a household pilot-ready
  until live Linq, Google, and Hermes checks are marked verified.
- `tests/test_app.py::test_pilot_check_blocks_pending_or_failed_delivery_work`
  verifies the pilot preflight fails when scoped outbound delivery work is still
  pending or failed, without exposing message text.
- `tests/test_app.py::test_pilot_check_blocks_approved_or_failed_action_work`
  verifies the pilot preflight fails when parent-approved action work is still
  unrun or has failed, without exposing action payload text.
- `tests/test_app.py::test_postgres_dev_write_endpoints_require_explicit_chat_id`
  verifies operator write routes cannot create an accidental `dev-chat`
  household in deployed/Postgres mode.
- `tests/test_app.py::test_pilot_check_blocks_any_hermes_toolset_for_saas_pilot`
  verifies a multi-family pilot cannot be marked ready when any Hermes toolset
  is enabled before a tenant-boundary audit.
- `tests/test_app.py::test_pilot_check_blocks_unpinned_hermes_agent_ref`
  verifies a pilot cannot be marked ready when Hermes is configured from a
  floating branch or ref instead of a pinned commit SHA.
- `tests/test_app.py::test_pilot_check_blocks_unimportable_hermes_path`
  verifies a pilot cannot be marked ready when the configured Hermes checkout
  cannot import `run_agent.AIAgent`.
- `tests/test_app.py::test_pilot_check_blocks_hermes_path_with_incompatible_aiagent_constructor`,
  `tests/test_app.py::test_pilot_check_blocks_hermes_path_without_skip_memory_kwarg`,
  `tests/test_app.py::test_pilot_check_blocks_hermes_path_without_save_trajectories_kwarg`,
  `tests/test_app.py::test_pilot_check_blocks_hermes_path_without_run_conversation`,
  and
  `tests/test_app.py::test_pilot_check_blocks_hermes_path_with_incompatible_run_conversation`
  verify a pilot cannot be marked ready when the configured Hermes checkout does
  not match Florence's adapter contract.
- `tests/test_app.py::test_pilot_check_reports_household_and_deployment_gaps`
  verifies missing household setup, deployment credentials, and live-verification
  blockers are reported separately.
- `tests/test_store_postgres.py::test_store_memory_unique_index_coalesces_null_subjects`
  verifies no-subject durable memories use a coalesced unique index, matching
  Florence's SaaS memory isolation rules under Postgres null semantics.
- `tests/test_service.py::test_worker_expires_due_reminder_after_stopped_past_grace`
  verifies a reminder missed while stopped expires after the configured reminder
  grace window instead of texting parents late after restart.
- `tests/test_service.py::test_daily_briefing_skips_after_delivery_window_when_household_restarts`
  verifies a morning briefing missed while stopped is skipped after the
  configured briefing window instead of arriving later as stale context.
- `tests/test_store_postgres.py::test_real_postgres_household_memory_and_approval_isolation`
  is an optional live Postgres regression. Set `FLORENCE_POSTGRES_TEST_DSN` to a
  disposable Postgres database URL before running it; it creates two households,
  verifies durable memory does not cross household boundaries, and verifies an
  approval code from one household cannot be approved from another.

## Data

The deployment target is Postgres, shared by the web process and worker through
`FLORENCE_DATABASE_URL`. SQLite remains available through `FLORENCE_DB_PATH` for
local development and fast tests only.
The target Postgres database should be empty or already migrated to this build's
schema before the app and worker start. Florence creates the pilot schema for a
fresh database, but it intentionally does not auto-migrate unrelated older app
schemas that happen to reuse table names.
For deploy-path proof, run the optional live store smoke against a disposable
database:

```bash
FLORENCE_POSTGRES_TEST_DSN=postgresql://user:password@host:5432/florence \
  pytest tests/test_store_postgres.py::test_real_postgres_household_memory_and_approval_isolation -q
```

Before broad production use, add:

- Managed backups with restore drills.
- Migration discipline.
- Disk encryption from the database host or platform.
- A database-level operational runbook before multi-region or high-scale use.
