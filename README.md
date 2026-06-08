# Florence

Florence is a parents-focused AI household assistant for two-parent homes. It is
designed to operate through iMessage using Linq, with Hermes Agent as the
reasoning backend whenever Hermes is available.

The product shape is intentionally small:

- Linq webhook in, Linq reply out.
- Postgres state for deployed pilots, with SQLite retained only for local
  development and tests.
- Household members, with the first sender treated as the founding parent,
  invited or confirmed partners treated as parents, and later unconfirmed
  senders treated as helpers.
- Household-scoped memory with member provenance, text-native review, and scoped
  deletion/export controls for SaaS isolation.
- Per-household privacy controls: maximum privacy by default, parent-only memory
  pause/resume, and analytics opt-in off by default.
- A parent-approval rail for risky external actions, scoped to the household and
  expiring by default, with audited execution for approved actions.
- A strict timekeeper so reminders are anchored to the household timezone and
  never silently schedule into the past.
- A need-to-know policy so connected email/calendar data is stored but only
  surfaced when it is timely and actionable.
- A narrow source-ingest API for trusted automations that validates typed source
  items and bounded summaries instead of forwarding arbitrary JSON or raw dumps
  into Hermes.
- First connected-source sync is treated as quiet backfill, with immediate texts
  only for urgent actionable items and useful held-back items eligible for a
  one-time daily briefing.
- Source-driven reminder suggestions: actionable future source items can ask for
  parent approval before adding a reminder.
- Parent-added household calendar events from iMessage, stored as calendar
  context for agenda, prep, and briefings without claiming an external calendar
  write.
- Parent-controlled household source preferences so parents can say what to
  always surface or keep quiet after connecting email/calendar.
- A warm but concise tone policy that avoids nagging or guilt.
- A Hermes adapter boundary so production can use Hermes Agent directly while
  tests use deterministic fakes.
- A Hermes proposal protocol so Hermes can suggest reminders or household memory
  while Florence still validates and owns all state changes.

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
uvicorn florence.app:create_app --factory --reload
```

Run the worker separately when you want reminders, briefings, and approved
actions to execute:

```bash
florence-worker
```

Run a local-only pilot rehearsal without external credentials:

```bash
florence local-smoke
```

Pass `--now-utc 2026-06-05T16:00:00+00:00` when you want a reproducible JSON
artifact for comparison or review.

This uses fake Linq delivery and a fake connected Google provider to exercise
two-parent setup, source sync, Need-to-Know surfacing, approval, worker
execution, reminder delivery, the pilot checklist, and the sanitized pilot proof
artifact. It prints JSON and keeps `live_verification_performed: false`; real
Linq, Google OAuth, Hermes, and managed Postgres proof are still required before
inviting an outside family.
Use the returned `staging_verification_checklist` as the copyable list of live
checks, endpoints, expected fields, Docker build args, runtime env vars, and
proof records to satisfy in staging. The live service steps separate
`credential_env` from `proof_record`: set credentials first, then let the real
Linq webhook, source-sync worker, and Hermes smoke endpoint write
`live_verifications` rows. `fallback_env` lists the deploy-time flag fallback
only. `configuration_preflight` is the early config check; `deployment_ready`
is the final check after live verification records exist. Replace
`{staging_chat_id}` in that checklist with the real Linq chat id from the
staging iMessage thread; the local smoke `chat_id` is fake rehearsal evidence
only.
After the live staging steps have been run, use one command to verify the
deployed endpoints and produce a sanitized operator artifact:

```bash
FLORENCE_ADMIN_API_KEY=... \
  florence staging-check \
  --base-url https://your-staging-domain.example.com \
  --chat-id <real-linq-staging-chat-id>
```

This calls `/health`, `/dev/deployment-check`, `/dev/hermes-status`,
`/dev/hermes-smoke/{chat_id}`, `/dev/pilot-check/{chat_id}`, and
`/dev/pilot-proof/{chat_id}`. It exits non-zero until staging has live Linq,
Google, Hermes, and pilot-proof evidence, and its JSON output summarizes counts
and blockers without copying raw source titles, email bodies, message bodies, or
OAuth tokens. On failure, read the top-level `next_actions` list first; it
combines deployment-check and pilot-check blockers into one operator work queue.

Container run:

```bash
cp .env.example .env
# Edit .env first: set HERMES_AGENT_REF to a full pinned Hermes commit SHA,
# choose FLORENCE_HERMES_MODEL, and fill the Linq/Google/admin secrets. The Docker
# build intentionally fails while INSTALL_HERMES_AGENT=1 and HERMES_AGENT_REF is
# blank, so a pilot image cannot bake a floating Hermes checkout.
docker compose up --build
```

## Environment

```bash
FLORENCE_DATABASE_URL=postgresql://florence:florence@db:5432/florence
# Local-only SQLite fallback when FLORENCE_DATABASE_URL is unset:
FLORENCE_DB_PATH=./florence.db
FLORENCE_DEFAULT_TIMEZONE=America/Los_Angeles
FLORENCE_DEV_ENDPOINTS_ENABLED=1
FLORENCE_ADMIN_API_KEY=...
FLORENCE_SOURCE_INGEST_API_KEY=...
FLORENCE_TOKEN_ENCRYPTION_KEY=...
FLORENCE_SUPPORT_CONTACT=support@example.com

LINQ_API_KEY=...
LINQ_WEBHOOK_SECRET=...
LINQ_BASE_URL=https://api.linqapp.com/api/partner/v3
LINQ_FROM_PHONE=+15555550000

GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
GOOGLE_REDIRECT_URI=https://your-domain.example.com/oauth/google/callback
GOOGLE_OAUTH_SCOPES=openid,email,https://www.googleapis.com/auth/gmail.readonly,https://www.googleapis.com/auth/calendar.readonly
FLORENCE_SOURCE_SYNC_INTERVAL_SECONDS=300

# Docker image build can install Hermes at /opt/hermes-agent.
INSTALL_HERMES_AGENT=1
HERMES_AGENT_REPO=https://github.com/NousResearch/hermes-agent.git
HERMES_AGENT_REF=<full pinned commit sha>
FLORENCE_HERMES_AGENT_PATH=/opt/hermes-agent
FLORENCE_HERMES_PROVIDER=openrouter
FLORENCE_HERMES_MODEL=...
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

FLORENCE_PENDING_ACTION_TTL_MINUTES=1440
FLORENCE_DATA_DELETION_CONFIRMATION_TTL_MINUTES=30
```

Generate `FLORENCE_TOKEN_ENCRYPTION_KEY` with:

```bash
python -c "from florence.oauth import TokenVault; print(TokenVault.generate_key())"
```

`/dev/deployment-check` validates that it is a real Fernet key before a
Postgres pilot can be marked ready.

Use `FLORENCE_DATABASE_URL` for deployed pilots so the web process and worker
share one Postgres database. `FLORENCE_DB_PATH` is retained for local SQLite
development and tests only. SQLite must be a direct file path, not a
`sqlite://` URL; URL-style database strings are accepted only for Postgres.
Use a fresh Postgres database for pilots, or one deliberately migrated to this
build's schema. Florence creates the current schema in an empty database and
validates required table columns at startup and in `/dev/deployment-check`; it
does not silently reuse older app schemas with conflicting table names.

If `FLORENCE_HERMES_AGENT_PATH` is set, Florence imports Hermes Agent's
`run_agent.AIAgent` from that checkout. The pilot preflight also verifies the
constructor and `run_conversation(...)` shape that the adapter calls. It also
requires `HERMES_AGENT_REF` to be a full pinned Git commit SHA, 40 hex
characters for SHA-1 or 64 for SHA-256, before a deployment can be marked
pilot-ready, along with explicit `FLORENCE_HERMES_PROVIDER` and
`FLORENCE_HERMES_MODEL` values. Local SQLite development can run with strict mode
off; if Hermes is unavailable, Florence replies with a deterministic fallback so
the transport and policy layers still work. Postgres-backed SaaS traffic requires
`FLORENCE_HERMES_STRICT=1` at preflight and runtime, so Hermes contract failures
or provider/runtime failures do not turn into silent family-facing fallback
texts. Postgres SaaS deployments also require POSIX `fcntl` interprocess file
locking around Hermes runtime state. `/dev/hermes-status` and
`/dev/deployment-check` must report
`thread_lock_plus_interprocess_file_lock`; `thread_lock_only_no_interprocess_lock`
is blocked because `HERMES_HOME` is process-global and unsafe without an
interprocess lock.

For Docker deploys, set `INSTALL_HERMES_AGENT=1`, pin `HERMES_AGENT_REF` to a
known full Hermes commit SHA, and keep
`FLORENCE_HERMES_AGENT_PATH=/opt/hermes-agent`.
The Docker image build fails fast if Hermes installation is requested without a
full `HERMES_AGENT_REF`, so a pilot image cannot silently bake a floating
upstream branch or ambiguous short SHA. The build also records the installed
Hermes checkout commit in
`/opt/hermes-agent/.florence-hermes-ref`; `/dev/hermes-status` and
`/dev/deployment-check` require that value to match `HERMES_AGENT_REF`.
For local non-Docker development, use an absolute path to your local Hermes clone,
for example `/Users/you/Projects/hermes-agent`.
Use `GET /dev/hermes-status` with the admin key in staging to confirm the exact
checkout path, `run_agent.py` path, pinned ref, matching checkout ref,
provider/model, empty toolsets, and Florence-owned memory/session boundary before
running a family smoke. The status output also reports `turn_failure_cleanup`
as `runtime_home_restored_and_checkout_modules_cleared_on_error`, which means
failed strict Hermes turns still restore `HERMES_HOME`, remove per-turn scratch
files, and clear checkout-loaded Python modules. For Postgres SaaS it must also
report `runtime_lock: thread_lock_plus_interprocess_file_lock`; otherwise the
endpoint and deployment check block pilot readiness.
Ambient Python `run_agent` imports are allowed only for local SQLite development;
Postgres-backed SaaS traffic requires `FLORENCE_HERMES_AGENT_PATH` so deployed
turns cannot bypass the pinned checkout contract. The runtime adapter enforces
that contract before importing Hermes: Postgres-backed calls require
`FLORENCE_HERMES_STRICT=1`, a pinned `HERMES_AGENT_REF`, a matching checkout
marker or git `HEAD`, and explicit `FLORENCE_HERMES_PROVIDER` /
`FLORENCE_HERMES_MODEL` values. Local SQLite development can still use the
deterministic fallback when strict mode is off.

There is no separate hosted "SaaS Hermes" call in this app. Florence is the SaaS
wrapper: it resolves the family, scopes memory and source context to that
household, calls the local Hermes Agent runtime with no Hermes toolsets, Hermes
native memory disabled, trajectory saving off, and an ephemeral turn session,
then validates any proposed state changes before writing data or texting the
family. Florence also sets `HERMES_HOME` to a fresh per-turn directory under
`FLORENCE_HERMES_RUNTIME_HOME` before importing Hermes, then removes that
directory after the call. That keeps Hermes default logs/session files out of a
shared user `~/.hermes` profile and out of a cross-family scratch bucket. For
Docker pilots, keep the base on non-durable container storage such as
`/tmp/florence-hermes-home`. Because `HERMES_HOME` is process-global, Florence
serializes every Hermes runtime context with an in-process thread lock plus an
interprocess file lock under the configured runtime base. That lock covers web
workers, background workers, operator preflight/status checks, and live parent
turns that share the same filesystem. Those preflight/status checks use fresh
`florence-preflight-*` runtime directories under the same base, so multiple app
workers do not share one fixed preflight scratch home. Florence also adds the
configured Hermes checkout to Python's import path only while importing or
running Hermes, then removes it afterward. Same-named pre-existing modules from
the checkout root are temporarily shadowed during the call, and newly imported
Python modules loaded from that checkout are cleared from `sys.modules` after
each preflight or turn, so one family/check cannot inherit Hermes module globals
from another.

Leave the `FLORENCE_*_LIVE_VERIFIED` flags at `0` until staging has exercised a
real Linq send/webhook round-trip, a real Google OAuth/source sync, and a real
Hermes response through Florence. The preferred SaaS path is sanitized proof in
the shared Florence database: the signed Linq webhook records `linq` proof after
a real non-dry-run outbound Linq send, the worker records `google` proof after a
default Google OAuth source sync imports, surfaces, and delivers a token-backed
item through real Linq, and `/dev/hermes-smoke/{chat_id}` records `hermes` proof
when it returns `live_hermes_verified: true` without fallback.
`POST /dev/live-verifications/{name}` remains available as an operator fallback;
`GET /dev/live-verifications` shows the stored proof records. Env flags still
work as a deploy-time fallback, but when
setting a flag to `1`, also set its `*_LIVE_VERIFIED_AT` value to an ISO-8601
timestamp with timezone and its `*_LIVE_VERIFICATION_PROOF` value to a short
proof note, such as the smoke chat, endpoint response reference, or runbook log
reference. The Hermes smoke endpoint excludes the model response body from
operator output. Proof notes must not contain secrets, tokens, phone numbers,
email addresses, raw JSON, line breaks, or raw family/source content; unsafe
notes are not echoed and keep deployment readiness blocked. Future-dated
verification timestamps are rejected by the deployment preflight.
`/dev/pilot-check/{chat_id}` will stay blocked while external checks are
unverified or missing proof metadata.
It also blocks until the household has at least one inbound and one successfully
recorded outbound iMessage within seven days of the recorded Linq
live-verification timestamp, while keeping message bodies out of the operator
response. It also blocks until at least one source item has been stored and at
least one Need-to-Know source item has been surfaced from the household's active
OAuth-token-backed Google account, with the account's latest source sync within
seven days of the recorded Google live-verification timestamp. The response
includes a machine-readable `smoke_checklist` with stable step ids and blocker
strings for the operator proof record. It also blocks on pending or failed outbound
deliveries, approved-but-unrun or failed action executions, and deployments that
have not yet run at least one parent-approved action through the worker.
When a deployment or pilot check is not ready, read `operator_next_steps` first:
it flattens the missing env vars, live Linq/Google/Hermes proof records or
fallback fields, database blockers, and household smoke blockers into the
remaining actions.

## HTTP

- `GET /health`
- `POST /webhooks/linq`
- `POST /api/source-items`
- `POST /dev/messages`
- `POST /dev/source-items`
- `POST /dev/import/email`
- `POST /dev/import/calendar`
- `POST /dev/sync-sources`
- `POST /dev/oauth/google/start`
- `GET /oauth/google/callback`
- `GET /dev/connected-accounts/{chat_id}`
- `GET /dev/source-review/{chat_id}`
- `GET /dev/source-preferences/{chat_id}`
- `GET /dev/memory/{chat_id}`
- `GET /dev/privacy/{chat_id}`
- `GET /dev/readiness/{chat_id}`
- `GET /dev/deployment-check`
- `GET /dev/live-verifications`
- `POST /dev/live-verifications/{name}`
- `GET /dev/hermes-status`
- `GET /dev/pilot-check/{chat_id}`
- `GET /dev/pilot-proof/{chat_id}`
- `POST /dev/hermes-smoke/{chat_id}`
- `DELETE /dev/memory/{chat_id}/{memory_id}`
- `POST /dev/actions`
- `POST /dev/actions/tick`
- `GET /dev/actions/{chat_id}`
- `GET /dev/actions/{chat_id}/executions`
- `POST /dev/reminders/tick`
- `POST /dev/routines/tick`

The `/dev/*` endpoints are for local smoke testing and operator control-plane
checks. In deployed environments, set `FLORENCE_ADMIN_API_KEY` so they require
`Authorization: Bearer <key>` or `X-Florence-Admin-Key: <key>`. Postgres-backed
runtimes require this key for `/dev/*` access; local SQLite smoke runs may omit
it. Postgres-backed dev write routes that can create household state also
require an explicit `chat_id`; only local SQLite smoke routes fall back to
`dev-chat`. Set `FLORENCE_DEV_ENDPOINTS_ENABLED=0` to remove the routes entirely.

## Worker

Run one routine tick from Python with `florence.worker.run_routine_tick(...)`, or
run a simple loop:

```python
from florence.worker import run_forever

run_forever(interval_seconds=60)
```

The worker sends due reminders inside a bounded grace window and skips stale
reminders instead of texting parents weeks late. A reminder is marked sent only
after the outbound sender succeeds; if Linq is temporarily unavailable, the
reminder stays pending and can be retried on the next tick with the same
idempotency key. The worker sends at most one daily household briefing after the
configured local briefing time and inside the configured briefing delivery
window, and claims that briefing only after the outbound sender succeeds; failed
briefing sends can retry with the same daily idempotency key while the window is
open. It skips the text entirely when there are no reminders or relevant source
items, and it skips missed morning briefings after the window closes instead of
sending stale context later. Households that texted `stop` are skipped for
reminders, briefings, approved-action execution, and connected-source polling
until a parent texts `start`. Approved external actions are executed from the
same worker tick and recorded in the action audit log.
Connected-source polling runs from the same worker loop every
`FLORENCE_SOURCE_SYNC_INTERVAL_SECONDS` seconds by default; set it to `0` to
disable polling and run
`florence.worker.run_source_sync_tick(...)` from a separate job.
Worker send loops continue after individual Linq delivery failures, report
delivery-failure counts from routine/source ticks, and leave the affected work
retryable without stopping processing for other households. The long-running
worker loop also logs unexpected tick-level failures and continues.

`florence.worker.run_source_sync_tick(...)` is still available for one-shot
operator jobs and tests. If a connected-source item is classified as important
but Linq delivery fails after the source cursor advances, Florence keeps the
exact source-surface outbound payload in its delivery table and retries it on a
later source-sync tick with the same idempotency key.

## Connected Sources

Email and calendar integrations should normalize into typed candidates before
they reach Need-to-Know:

- Email: subject, body/snippet, sender, received time, optional event time.
- Calendar: title, start/end, location, description, calendar name.

Florence stores every candidate with source provenance and a triage decision.
Only `surface` decisions text the household. Source summaries are bounded before
policy scoring, so raw inbox or document dumps cannot dominate Need-to-Know.
High-signal school schedule changes, such as no-school days or early dismissal,
can still surface when the source importer did not extract a reliable time, but
only when the source itself was observed recently. Florence will not offer a
reminder approval unless a real due time is known.

Google OAuth uses a web-server authorization flow. A parent can text
`connect google` in the household thread; Florence replies with a short-lived
authorization URL. Google redirects back to `/oauth/google/callback`, then
Florence sends a connection confirmation to the same iMessage thread. Florence
stores Google tokens encrypted with `FLORENCE_TOKEN_ENCRYPTION_KEY` and keeps
them scoped to the connected household account. Public callback failures use
generic browser errors and do not echo provider error text, tokens, email
addresses, phone numbers, or configured secrets. A parent can text
`disconnect google` to disable the household's Google account, remove the stored
OAuth token, and stop Gmail/Calendar polling.

## Text Commands

- `my name is Sam` records the current sender's household name.
- `help`, `help setup`, `help sources`, `help calendar`, `help memory`,
  `help privacy`, and `help reminders` show short command-specific help without
  invoking Hermes.
- `what's on deck today?` shows today's household reminders and relevant source
  items through the next local midnight.
- `what should we prep for tomorrow?` shows tomorrow's reminders and relevant
  source items using the household timezone, without email bodies.
- `add soccer practice tomorrow at 5pm to calendar` adds an internal household
  calendar item for agenda/prep/briefings; it does not write to Google Calendar.
- `handoff` or `what's open?` shows pending approvals and upcoming reminders for
  the next week; parent-only.
- `done` marks the most recent sent reminder as handled when there is only one
  clear match; `done pack lunch` marks a matching active reminder; parent-only.
- `cancel reminder pack lunch` cancels a matching active reminder; parent-only.
- `remind Alex tomorrow at 8am to pack cleats` assigns the reminder to a known
  household member named Alex and labels it in the shared thread.
- `setup` or `setup status` shows the household readiness checklist plus one
  concrete next text action.
- `invite partner +15555550101` starts a shared iMessage household thread; parent-only.
- `confirm partner +15555550101` marks an already-seen sender as the second parent; parent-only.
- `set timezone America/New_York` changes the household timezone; parent-only.
- `household status` lists the timezone and known household participants.
- `our kids are Maya and Leo` records child profiles for household context; parent-only.
- `connect google` sends a short-lived Google Calendar/Gmail authorization link; parent-only.
- `disconnect google` removes the stored Google token and stops Gmail/Calendar
  polling; parent-only and still available after `stop`.
- `remember that Maya likes pasta` writes a household-scoped memory; parent-only.
- `forget Maya likes pasta` deletes matching household memories; parent-only.
- `clear household memory` deletes all active household memories; parent-only.
- `what do you remember?` lists active household memory and provenance; parent-only.
- `privacy status` shows household memory and analytics settings.
- `data summary` shows a parent-only count summary of stored household data
  without raw message or email bodies.
- `pause memory` stops new durable memories and excludes existing memories from
  Hermes context; parent-only.
- `resume memory` turns household memory back on; parent-only.
- `opt in to product analytics` or `opt out of product analytics` changes
  aggregate analytics consent for the household; parent-only.
- `delete my data` starts a parent-only two-step household data deletion flow;
  confirm with `confirm delete household data` before the confirmation window expires.
- `stop` pauses Florence replies, reminders, briefings, approved actions, and
  source polling for the household, and `start` turns them back on; parent-only.
  `help` still works while stopped.
- `support`, `human`, or `talk to a human` returns the configured Florence
  support contact and still works while stopped.
- `always tell me about permission slips` surfaces matching future source items;
  parent-only.
- `mute newsletters` stores matching source items quietly; parent-only.
- `mute this sender` or `mute this domain` uses the most recently surfaced
  source item's sender metadata to quiet future matches; parent-only.
- `source review` shows source counts and a short title-only sample of what was
  texted or kept quiet; parent-only.
- `not useful` or `more like this` tunes future source surfacing from the most
  recently surfaced source item; parent-only.
- `source preferences` lists household source rules; parent-only.
- `approve abc12345` or `cancel abc12345` resolves a pending action; helpers
  cannot approve actions.

## Product Docs

- [Product research](docs/research.md)
- [Tone contract](docs/tone.md)
- [Timekeeping](docs/timekeeping.md)
- [Source policy](docs/source-policy.md)
- [SaaS memory model](docs/memory.md)
- [Hermes boundary](docs/hermes-boundary.md)
- [SaaS deployment runbook](docs/saas-deployment.md)
- [Deployment and pilot smoke checklist](docs/deployment.md)
