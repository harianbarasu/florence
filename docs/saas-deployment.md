# SaaS Deployment Runbook

Florence is deployed once and serves many households. You do not deploy one
Florence per family, and you do not call a hosted Hermes SaaS API.

## Mental Model

```text
Parents in iMessage
        |
        v
Linq phone number and webhook
        |
        v
Florence web app  ----->  Postgres
        |                    ^
        v                    |
Florence worker -------------+
        |
        v
Linq outbound sends
```

One deployed Florence instance handles all families. Linq sends every incoming
iMessage event to `POST /webhooks/linq`. Florence reads the Linq `chat_id`,
finds or creates the matching household row in Postgres, and scopes every
message, memory, source item, reminder, approval, and deletion request to that
household.

The worker is a second process using the same Docker image and the same
`FLORENCE_DATABASE_URL`. It sends due reminders, daily briefings, approved
actions, Google source sync results, and retryable outbound messages. Worker
send loops continue after individual Linq delivery failures, report failure
counts, and leave the affected work retryable so one transient household/source
failure does not stop processing for the rest of the shared SaaS deployment.
The long-running loop also logs unexpected tick-level failures and continues.

## What Runs In Production

Run exactly these components:

- A public HTTPS web process running `florence`.
- A private background worker process running `florence-worker`.
- A managed Postgres database.
- A Linq partner account with an iMessage-capable sending number.
- A Google OAuth app if connected Gmail/Calendar sources are enabled.
- A model provider account for Hermes Agent routing.

The web process must be reachable from Linq and Google:

- `https://your-domain.example.com/webhooks/linq`
- `https://your-domain.example.com/oauth/google/callback`

The worker does not need a public URL.

## Hermes In This Deployment

Hermes Agent is code that Florence imports in-process. It is not a remote SaaS
endpoint, and Florence does not require a separate Hermes Docker image or a
second Hermes container.

For Docker deploys, the image build clones Hermes into `/opt/hermes-agent`:

```bash
INSTALL_HERMES_AGENT=1
HERMES_AGENT_REPO=https://github.com/NousResearch/hermes-agent.git
HERMES_AGENT_REF=<full pinned commit sha>
FLORENCE_HERMES_AGENT_PATH=/opt/hermes-agent
FLORENCE_HERMES_RUNTIME_HOME=/tmp/florence-hermes-home
FLORENCE_HERMES_STRICT=1
```

At runtime Florence imports:

```text
/opt/hermes-agent/run_agent.py
```

and calls `run_agent.AIAgent.run_conversation(...)`.

So the Docker image you deploy is the Florence image. Hermes is baked into that
image when `INSTALL_HERMES_AGENT=1` and `HERMES_AGENT_REF` is a full commit id. The build
records the installed commit in `/opt/hermes-agent/.florence-hermes-ref`, and
operator preflight compares that checkout ref against `HERMES_AGENT_REF` so
staging cannot silently run a different Hermes revision. Ambient Python
`run_agent` imports are local SQLite development only; Postgres-backed SaaS
traffic requires `FLORENCE_HERMES_AGENT_PATH`.

Florence remains the SaaS wrapper:

- Postgres is the durable source of truth.
- Household memory is stored by Florence, not Hermes.
- Hermes native memory is disabled with `skip_memory=True`.
- Hermes trajectory saving is disabled with `save_trajectories=False`.
- Each Hermes call gets a fresh `florence-turn-*` session id.
- Florence sets `HERMES_HOME` to a fresh per-turn directory under
  `FLORENCE_HERMES_RUNTIME_HOME`, then removes that directory after the call,
  including failed strict Hermes turns. The configured base should be a writable
  non-durable path, not a durable shared `~/.hermes` profile.
- Operator preflight/status imports also get fresh ephemeral
  `florence-preflight-*` runtime directories under the same base, so multiple
  deployed app workers do not share or delete one fixed preflight scratch home.
- Hermes calls are serialized for the pilot because `HERMES_HOME` is
  process-global. Florence uses an in-process thread lock plus an interprocess
  file lock under `FLORENCE_HERMES_RUNTIME_HOME`; this includes operator
  preflight/status imports, not only parent turns.
- The configured Hermes checkout is added to Python's import path only while
  Florence is importing or running Hermes, then removed afterward.
- Same-named pre-existing modules from the checkout root are temporarily
  shadowed during the call, and newly imported Python modules loaded from the
  configured Hermes checkout are cleared from `sys.modules` after each preflight
  or parent turn, even when Hermes raises. Module globals initialized under one
  runtime home cannot leak into the next check or family turn.
- Hermes toolsets must stay empty with `FLORENCE_HERMES_TOOLSETS=`.
- `FLORENCE_HERMES_STRICT` must be `1` for Postgres-backed pilots. Strict mode
  being disabled, a missing checkout, floating ref, mismatched checkout, or
  enabled Hermes toolsets are raised instead of hidden behind the fallback reply.
- Florence validates every Hermes proposal before writing state or texting a
  family.

Live pilot proof is also Florence-owned. The signed Linq webhook records `linq`
proof after a real non-dry-run outbound Linq send, the worker records `google`
proof after the default Google OAuth source-sync path imports, surfaces, and
delivers a token-backed item through real Linq, and
`POST /dev/hermes-smoke/{chat_id}` records the `hermes` proof automatically when
the deployed adapter returns a live non-fallback response. Manual
`POST /dev/live-verifications/{name}` calls remain available as an operator
fallback. These proof records live in the shared Florence database, so the web
process and worker see the same SaaS readiness state without redeploying just to
flip verification env flags.

Use a fresh managed Postgres database for the pilot, or one that has been
deliberately migrated to this build's schema. Florence can create its current
tables in an empty database, but it validates required table columns at startup
and in `/dev/deployment-check`; an older database with reused table names is
rejected instead of failing later in a parent conversation.

Postgres-backed SaaS traffic must run on a POSIX runtime where `fcntl`
interprocess file locking is available. Local SQLite development can run with
thread-only locking, but a deployed Postgres pilot cannot because Hermes uses
process-global `HERMES_HOME` runtime state.

Run `GET /dev/hermes-status` with the admin key after deploy. It should report:

```json
{
  "mode": "configured_checkout",
  "contract_ok": true,
  "ready_for_saas_pilot": true,
  "agent_path": "/opt/hermes-agent",
  "toolsets": [],
  "strict_mode": true,
  "database_backend": "postgres",
  "runtime_home": "/tmp/florence-hermes-home",
  "preflight_runtime_home_scope": "ephemeral_per_check_under_runtime_home",
  "turn_runtime_home_scope": "per_turn_under_runtime_home",
  "turn_runtime_cleanup": "enabled",
  "turn_failure_cleanup": "runtime_home_restored_and_checkout_modules_cleared_on_error",
  "turn_runtime_concurrency": "serialized_by_thread_and_file_lock",
  "runtime_lock": "thread_lock_plus_interprocess_file_lock",
  "runtime_env_var": "HERMES_HOME",
  "runtime_home_writable": true,
  "python_path_scope": "temporary_during_hermes_call",
  "module_cache_scope": "shadowed_and_cleared_during_hermes_import_or_call",
  "memory_owner": "florence",
  "session_scope": "ephemeral_per_turn"
}
```

If `runtime_lock` is `thread_lock_only_no_interprocess_lock`, the endpoint and
`/dev/deployment-check` block pilot readiness until the deployment runtime
provides POSIX file locks.

Run `GET /dev/deployment-check` with the admin key before inviting a family. It
checks global configuration, Hermes checkout compatibility, Postgres readiness,
an active database `SELECT 1` from the running process, and whether the live
Linq/Google/Hermes smoke flags have been set. It does not require or create a
household. The `database` object reports both `configured_backend` and
`store_backend`; they must match before the deployment can be marked ready.

## Required Environment

Set these on both the web process and worker:

```bash
FLORENCE_DATABASE_URL=postgresql://user:password@host:5432/florence
FLORENCE_DEFAULT_TIMEZONE=America/Los_Angeles
FLORENCE_ADMIN_API_KEY=<random secret>
FLORENCE_SOURCE_INGEST_API_KEY=<random secret>
FLORENCE_TOKEN_ENCRYPTION_KEY=<generated Fernet key>
FLORENCE_SUPPORT_CONTACT=support@example.com

LINQ_API_KEY=<from Linq>
LINQ_WEBHOOK_SECRET=<from Linq webhook config>
LINQ_FROM_PHONE=<your Linq sending number>
LINQ_BASE_URL=https://api.linqapp.com/api/partner/v3

GOOGLE_CLIENT_ID=<google oauth client id>
GOOGLE_CLIENT_SECRET=<google oauth client secret>
GOOGLE_REDIRECT_URI=https://your-domain.example.com/oauth/google/callback

INSTALL_HERMES_AGENT=1
HERMES_AGENT_REPO=https://github.com/NousResearch/hermes-agent.git
HERMES_AGENT_REF=<full pinned commit sha>
FLORENCE_HERMES_AGENT_PATH=/opt/hermes-agent
FLORENCE_HERMES_PROVIDER=<provider name>
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

Generate `FLORENCE_TOKEN_ENCRYPTION_KEY` with:

```bash
python -c "from florence.oauth import TokenVault; print(TokenVault.generate_key())"
```

Treat it like a production secret. If the key is lost, stored connected-source
tokens cannot be decrypted. `/dev/deployment-check` validates that the value is
a real Fernet key before a Postgres pilot can be marked ready.

Prefer database proof from the live staging path over verification env flags.
Automatic or manual `live_verifications` rows must use ISO-8601 timestamps with
timezone and short concrete proof notes, such as a Linq smoke run, Google
source-sync run, or saved `/dev/hermes-smoke/{chat_id}` response reference. The
Hermes smoke response intentionally excludes the model response body. Env flags
remain a deploy-time fallback; every `*_LIVE_VERIFIED=1` flag must have a
matching `*_LIVE_VERIFIED_AT` timestamp and `*_LIVE_VERIFICATION_PROOF` note.
Proof notes must not contain secrets, tokens, phone numbers, email addresses,
raw JSON, line breaks, or raw family/source content. Unsafe proof notes are not
echoed in operator output and keep `/dev/deployment-check` blocked. Future-dated
proof timestamps are rejected by `/dev/deployment-check`.

## How Families Join

Families do not create accounts in this MVP. They interact through iMessage.

1. A parent texts the Linq-powered Florence number.
2. Linq sends the message to `/webhooks/linq`.
3. Florence creates a household for that Linq `chat_id`.
4. The parent texts `my name is Sam`.
5. The parent texts `invite partner +15555550101` or
   `confirm partner +15555550101`.
6. The second parent joins the shared iMessage thread and texts
   `my name is Alex`.
7. A parent adds children, source preferences, and optional Google connection.

Every later message in that Linq chat maps back to the same household row.
Helpers can be present in the thread, but parent-only controls remain blocked
for helpers.

## External Setup

Configure Linq:

- Sending number: `LINQ_FROM_PHONE`.
- Webhook URL: `https://your-domain.example.com/webhooks/linq`.
- Webhook secret: `LINQ_WEBHOOK_SECRET`.

Configure Google:

- Redirect URI: `https://your-domain.example.com/oauth/google/callback`.
- Scopes: `openid`, `email`, Gmail readonly, Calendar readonly.
- Keep early pilots limited to approved test users until Google consent screen
  verification is ready.

Configure DNS/TLS:

- The web app must have HTTPS.
- Linq and Google must be able to reach the public domain.

## Staging Smoke

Before using live credentials, run `florence local-smoke` once. It rehearses the
signed Linq, connected-source worker, Need-to-Know, approval, action worker,
reminder, pilot-check, and sanitized pilot-proof path locally with fake external
services, and it prints `live_verification_performed: false` so it cannot be
mistaken for the live Linq/Google/Hermes/Postgres proof below. Pass `--now-utc`
when you want a reproducible JSON artifact for comparison or review. Use the
returned `staging_verification_checklist` as the copyable sequence of live
endpoints, expected fields, Docker build args, runtime env vars, and proof
records to satisfy in staging. The live service steps separate `credential_env`
from `proof_record`: set credentials first, then let the real Linq webhook,
source-sync worker, and Hermes smoke endpoint write `live_verifications` rows.
`fallback_env` lists the deploy-time flag fallback only. `configuration_preflight`
is the early config check; `deployment_ready` is the final check after live
verification records exist. Replace `{staging_chat_id}`
with the real Linq chat id from the staging iMessage thread; the local smoke
`chat_id` is fake rehearsal evidence only.

After the live staging steps have been run, verify the deployed endpoints with:

```bash
FLORENCE_ADMIN_API_KEY=... \
  florence staging-check \
  --base-url https://your-staging-domain.example.com \
  --chat-id <real-linq-staging-chat-id>
```

The command calls `/health`, `/dev/deployment-check`, `/dev/hermes-status`,
`/dev/hermes-smoke/{chat_id}`, `/dev/pilot-check/{chat_id}`, and
`/dev/pilot-proof/{chat_id}`. It exits non-zero until the deployment has live
Linq, Google, Hermes, managed Postgres, and sanitized pilot-proof evidence. Its
JSON output summarizes counts and blockers without copying raw source titles,
email bodies, message bodies, or OAuth tokens. On failure, read the top-level
`next_actions` list first; it combines deployment-check and pilot-check blockers
into one operator work queue.

Before inviting any outside family:

1. Deploy the web app and worker against the same Postgres database.
2. Run `GET /health`.
3. Run `GET /dev/deployment-check` with `X-Florence-Admin-Key`; expect it to
   list missing live checks until the rest of this staging smoke is complete.
4. Run `GET /dev/hermes-status` with `X-Florence-Admin-Key`.
5. Send a real iMessage through Linq and confirm it reaches
   `/webhooks/linq`.
6. Complete two-parent setup in the shared iMessage thread.
7. Text `connect google`, complete Google OAuth, and run the worker source sync.
8. Create a controlled future permission-slip style item in the Google test
   account.
9. Confirm Florence texts only the short Need-to-Know summary and approval code.
10. Approve the reminder, run the worker, and confirm the reminder arrives at the
   correct local time.
11. Run `POST /dev/hermes-smoke/{chat_id}` after the household has at least one
    parent member, and confirm it reports `live_hermes_verified: true`,
    `response: null`, `response_present: true`,
    `sanitization.response: excluded`, `used_fallback: false`,
    `hermes.database_backend: postgres`, `hermes.ready_for_saas_pilot: true`,
    and `hermes.strict_mode: true`.
    If it returns `ok: false`, treat the smoke as failed; the response includes
    a sanitized `error` and the same `hermes.invalid` preflight fields without
    echoing raw provider errors or secrets.
12. Confirm `GET /dev/live-verifications` includes `linq`, `google`, and
    `hermes`. If automatic proof was not recorded even though the live check
    passed, use `POST /dev/live-verifications/{name}` with an ISO-8601
    `verified_at_utc` timestamp and a short `proof` note. Env flags
    `FLORENCE_LINQ_LIVE_VERIFIED`, `FLORENCE_GOOGLE_LIVE_VERIFIED`, and
    `FLORENCE_HERMES_LIVE_VERIFIED` remain a deploy-time fallback only; when a
    flag is set to `1`, set the matching `*_LIVE_VERIFIED_AT` timestamp and
    `*_LIVE_VERIFICATION_PROOF` note. The timestamp must include a timezone and
    must not be future-dated. The proof note must be a short reference, not a raw
    endpoint payload or family/source content.
13. Run `GET /dev/deployment-check` again; require `deployment.ready: true`.
14. Run `GET /dev/pilot-check/{chat_id}` and require `pilot_ready: true`.
    Confirm `message_transport.ready: true`, with at least one inbound and one
    outbound message count within seven days of
    `deployment.live_verification.evidence.linq.verified_at_utc`, so the
    household has fresh text-flow evidence without exposing message bodies in the
    operator response. Confirm
    `connected_accounts.token_backed_google > 0`,
    `source_review.token_backed_google_total > 0`,
    `source_review.token_backed_google_surfaced > 0`, and
    `source_review.latest_token_backed_google_synced_at_utc` within seven days
    of `deployment.live_verification.evidence.google.verified_at_utc`. Confirm
    `smoke_checklist.steps.approval_worker_queue.ready: true` after one approved
    action has executed through the worker. Save the `smoke_checklist` object
    with the proof record; it has stable step ids and blocker strings for any
    remaining gap.
15. Save `GET /dev/pilot-proof/{chat_id}`. This is the sanitized operator proof
    bundle: it includes the pilot checklist, live-verification state, privacy
    state, source-review counts, and action execution statuses, but excludes raw
    message bodies, raw source titles/bodies/event times, OAuth tokens, and
    household memory text. Action execution errors are represented only as
    presence/absence, not raw exception strings, and deployment/checklist
    diagnostic strings redact configured secrets, DSNs, bearer tokens, phone
    numbers, and email addresses.
    The proof is scoped to the requested Linq `chat_id`; do not treat it as a
    cross-household or database-wide report.

If the pilot check is not true, do not invite an outside family yet.

## Scale Boundaries For The MVP

This MVP is ready for controlled pilots, not broad self-serve signup.

Acceptable pilot shape:

- A small number of families.
- Operator-created access through the Linq number.
- Admin-key protected control-plane endpoints.
- Manual smoke checks before marking live verification flags.
- One region, one Postgres database, one web process or small web process group,
  and one worker.

Do not scale to broad public launch until there is:

- Account/team admin UI.
- Formal migrations and backup restore drills.
- Rate limits per Linq chat and per household.
- Secrets rotation runbooks.
- Better observability and alerting.
- A reviewed policy for enabling any Hermes toolset.
