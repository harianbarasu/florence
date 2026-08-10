# Florence production runbook

Florence deploys as two persistent Railway services backed by one canonical PostgreSQL service. Both
application services use the same immutable Docker image and environment contract; only their start command
and health model differ.

## Release gates

Do not deploy a release until all of these are true:

- `pnpm check` and a clean `docker build .` pass on Node 24.19.0.
- Every enqueued job kind, including due-timer work, has a worker dispatcher.
- Web handles `SIGTERM` by closing Fastify and PostgreSQL connections before exit.
- A replacement worker waits or retries during the singleton-lease handoff instead of crashing while the old
  deployment drains.
- Worker freshness is externally observable. Railway only checks the web readiness endpoint during deployment;
  it does not continuously monitor either service.
- Web and worker logs use stable, allowlisted error codes. They must not serialize arbitrary provider errors or
  persist raw exception messages.
- A valid Linq Standard Webhooks signing secret is available and a current database backup exists.
- Both Railway services resolve `NODE_ENV=production` and `FLORENCE_POSTGRES_SCHEMA=florence_v4`; never apply
  the rebuild migration to the legacy `florence` schema.

## Railway topology

| Railway service | Source and config path | Public network | Replicas | Start and readiness |
| --- | --- | --- | --- | --- |
| `Postgres` | Railway PostgreSQL | No application-facing domain | Managed | Canonical durable state |
| `api` (web role) | Canonical GitHub repo, `/railway.json` | One canonical HTTPS domain | 1 initially | `node dist/server.js`; Railway checks `/readyz` |
| `florence-worker-v2` | Same commit, `/railway.worker.json` | None | Exactly 1 | `node dist/worker.js`; process liveness plus an external heartbeat |

Set each service's Config-as-Code path explicitly in **Settings → Build**. Railway otherwise discovers only
`/railway.json`; the worker will not use `/railway.worker.json` automatically. Do not attach a volume to either
application service. Their filesystems are scratch space and PostgreSQL is canonical.

### Pilot reset state

As verified on 2026-08-09, `api` and `florence-worker-v2` are intentionally scaled to zero while the reusable
Google-login release is finalized. Both services reference the clean sibling PostgreSQL service created for the
pilot reset; that database has no people or customer/runtime rows. The former full PostgreSQL service remains
offline and unchanged as rollback, and its encrypted backup was independently restored and fingerprinted.

Deploy the same verified commit to both application services, let pre-deploy migrate the clean database, start
the worker first, and start the API only after the worker lease is healthy. Do not delete the old PostgreSQL
service until Jackson/Kendall acceptance succeeds and a separate retirement decision is recorded.

The operational HTTPS origin is currently `https://florence-production-b9af.up.railway.app`. Keep Google OAuth,
Linq webhooks, and `FLORENCE_WEB_BASE_URL` on that origin until `harianbarasu.com` has working public DNS and all
three are changed together. Merely attaching a custom domain in Railway is not a completed DNS cutover.

Both service configs run `node dist/ops/predeploy-production.js` as their only pre-deploy command. It fails
before touching PostgreSQL unless `NODE_ENV=production`, TLS is required, the full runtime contract is valid,
and the target schema is exactly `florence_v4`; it then runs the migration with secret-safe output. Railway
deploys services from a GitHub push independently, so either service must be able to prepare the schema before
starting. Migrations are serialized by a PostgreSQL advisory lock and verify the digest of every previously
applied file.

## Exact environment contract

Make the runtime variables shared by `api` and `florence-worker-v2`. Set
`FLORENCE_DATABASE_URL=${{<active-postgres-service>.DATABASE_URL}}` as a reference variable on both services;
for the 2026-08-09 pilot reset this is the clean sibling database, not the retained rollback service. Do not define
`PORT`; Railway injects it. The Docker image sets `NODE_ENV=production`.

Required for every production process:

| Variable | Contract |
| --- | --- |
| `FLORENCE_DATABASE_URL` | `postgres:` or `postgresql:` URL; use the private Railway reference above |
| `FLORENCE_WEB_BASE_URL` | Clean HTTPS origin with no path, query, fragment, or credentials |
| `FLORENCE_TOKEN_ENCRYPTION_KEY` | At least 32 characters; used for opaque-token and CSRF digests |
| `FLORENCE_DATA_ACTIVE_KEY_ID` | Active key ID, for example `prod-v1` |
| `FLORENCE_DATA_KEYRING_JSON` | JSON object of key IDs to canonical base64- or base64url-encoded 32-byte AES keys; must contain the active ID |
| `LINQ_API_KEY` | Linq partner API credential |
| `LINQ_FROM_PHONE` | Florence's Linq number in E.164 form, such as `+14155550100` |
| `LINQ_WEBHOOK_SECRET` | Linq endpoint signing secret in `whsec_...` Standard Webhooks format |
| `GOOGLE_CLIENT_ID` | Google OAuth web client ID |
| `GOOGLE_CLIENT_SECRET` | Google OAuth web client secret |
| `MODEL_PROVIDER` | `openai`, `anthropic`, or `open_weight` |

Provider-conditional variables:

| Provider | Required | Optional/defaulted |
| --- | --- | --- |
| `openai` | `OPENAI_API_KEY` | `OPENAI_BASE_URL=https://api.openai.com/v1`, `OPENAI_MODEL=gpt-5-mini` |
| `anthropic` | `ANTHROPIC_API_KEY` | `ANTHROPIC_MODEL=claude-sonnet-4-5` |
| `open_weight` | `OPEN_WEIGHT_BASE_URL`, `OPEN_WEIGHT_MODEL` | `OPEN_WEIGHT_API_KEY` may be blank when the endpoint needs no key |

Optional variables and defaults:

| Variable | Default |
| --- | --- |
| `LOG_LEVEL` | `info` |
| `FLORENCE_POSTGRES_SCHEMA` | `florence_v4` |
| `FLORENCE_DEFAULT_TIMEZONE` | `America/Los_Angeles` (must be an IANA time zone) |
| `LINQ_BASE_URL` | `https://api.linqapp.com/api/partner/v3` |
| `WORKER_POLL_INTERVAL_MS` | `1000` |
| `GMAIL_POLL_INTERVAL_MS` | `60000` |
| `CALENDAR_POLL_INTERVAL_MS` | `120000` |
| `RAW_SOURCE_RETENTION_DAYS` | `30` (maximum 30) |
| `WORKER_SCRATCH_RETENTION_DAYS` | `7` (maximum 7) |

There are no separately configured Google redirect variables. Florence derives two exact callbacks
from `FLORENCE_WEB_BASE_URL`:

- `<FLORENCE_WEB_BASE_URL>/auth/google/callback` for identity-only browser login; and
- `<FLORENCE_WEB_BASE_URL>/oauth/google/callback` for optional Gmail and Calendar source access.

Register both exact URIs in Google Cloud before deployment. Keeping the flows separate prevents a
normal sign-in from silently asking for private-source access. If the canonical domain changes,
update both Google callbacks and `FLORENCE_WEB_BASE_URL` together.

Generate independent secrets locally, then paste them directly into sealed Railway variables:

```bash
openssl rand -base64 48
openssl rand -base64 32
```

Use the first output for `FLORENCE_TOKEN_ENCRYPTION_KEY`. Put the second in a keyring such as
`{"prod-v1":"<base64 output>"}`. Never remove an old data key while rows encrypted with it may remain. A token
key rotation invalidates existing sessions and private handoff links.

## Connector configuration

After the web service has its canonical domain:

1. In Linq, set the webhook endpoint to `<origin>/webhooks/linq`, select payload version `2026-02-03`, and copy
   that endpoint's signing secret into `LINQ_WEBHOOK_SECRET`. The API key is not the webhook signing secret.
2. In Google Cloud, register both `<origin>/auth/google/callback` and
   `<origin>/oauth/google/callback` on the OAuth web client and add the intended test users while
   the consent screen remains in testing. The login flow requests only OpenID identity claims and
   stores no Google access or refresh token. Enable both the Gmail API and Google Calendar API for
   the separate source-connection flow. Florence offers two least-privilege connection profiles:
   personal/family requests read-only Mail and Calendar, while work starts with read-only Calendar
   and can explicitly include read-only Gmail. Each connection uses Google's account chooser so one person can attach multiple accounts
   deliberately.
3. Send only synthetic messages during the first connector smoke. Confirm webhook authentication failures do
   not create provider-event rows and that a duplicate delivery does not create duplicate work or output.

For the first auth smoke, consume one private iMessage bootstrap link, explicitly link a Google
login, sign out, and sign back in from the stable public sign-in page. Confirm an unlinked Google
account cannot create or merge a Florence person. Then connect a personal test account from Sources.
Confirm that the card reaches “Keeping
up with new mail,” the primary calendar appears, and recent mail begins moving before older history completes.
Then connect a different work account and confirm its card contains Calendar but no Mail status. Provider auth
failures must change the connection to `reauth_required`; reconnecting the same account starts a new integration
authority epoch so stale queued work cannot continue.

Linq references: [webhook events](https://docs.linqapp.com/guides/webhooks/events/),
[message sending](https://docs.linqapp.com/api/resources/chats/subresources/messages/methods/send/).

## Build and deploy

From a clean checkout of the exact commit:

```bash
corepack enable
pnpm install --frozen-lockfile
pnpm check
docker build --tag florence:release .
docker run --rm --entrypoint node florence:release --version
```

### Governed skill promotion

Skill IDs and versions are immutable production identities. The worker registers a declaration's canonical
definition digest over its purpose, instructions, input/output schemas, and capabilities. The exact seven-skill
suite that predates this gate is the only bootstrap baseline. Bootstrap may create its first production release
on an empty database, but it will never replace, restore, or reactivate release history.

For any new skill or version, startup registers a `candidate` and then exits closed until an operator or an
application-owned release workflow completes all of these in one auditable promotion:

1. Create the named evaluation release with its real protected-suite digest and leave it `candidate`; attach it
   to the candidate `skill_versions` row.
2. Execute that suite and persist at least one exact, passed `evaluation_runs` row for both that skill-version ID
   and evaluation-release ID. A release name or manually changed status is not evaluation evidence.
3. In one transaction, mark the evaluation release `active`, mark the skill version `approved`, retire the prior
   version, deactivate the prior production event, and insert the new active `skill_release_events` `promoted`
   row referencing those exact IDs. Record the responsible person when an application identity exists.
4. Restart the worker and confirm it remains live. It verifies the immutable definition digest, exact evaluation
   release, active production event, and passed evaluation again before every run.

Never edit a definition under an existing ID/version, overwrite a stored definition digest, or edit the frozen
bootstrap digest map. Create a new version and use the evaluated promotion path. There is intentionally no
general-purpose skill administration UI in the first release.

### Rebuild cutover recovery reference

For disaster recovery or an intentional pilot reset, cut over to a sibling PostgreSQL service. Do not mass-delete,
truncate with `CASCADE`, wipe, or rebuild in place on a nearly full volume. A sibling service avoids WAL pressure
on the endangered volume and leaves an immediate rollback target.

1. Stop `api` and `florence-worker-v2`. Take an encrypted PostgreSQL backup, restore it into an isolated database,
   and verify a full metadata fingerprint before touching service references.
2. Preserve exactly `schema_migrations`, `skills`, `skill_versions`, `evaluation_releases`, `evaluation_runs`, and
   `skill_release_events`. Assert that every preserved `skill_release_events.actor_person_id` is null. Do not copy
   customer/runtime tables or `worker_attempts`.
3. Provision a sibling Railway PostgreSQL service. Using the immutable release that matches the backup, migrate
   only through the backup's last applied migration (001–021 for the 2026-08-09 reset). In one transaction,
   empty the fresh baseline rows in FK-safe order—`skill_release_events`, `evaluation_runs`, `worker_attempts`,
   `skill_versions`, `skills`, `evaluation_releases`, then `schema_migrations`—and restore the six preserved
   governance tables exactly, including the 001–021 migration digests and timestamps. `worker_attempts` is
   deliberately emptied but never restored. Rerun that baseline migrator and require a no-op.
4. Verify the baseline table set, exact governance fingerprints, and zero rows in every customer/runtime table.
   Confirm PostgreSQL is healthy and not in recovery.
5. While both application services remain stopped, point both `FLORENCE_DATABASE_URL` reference variables at the
   sibling database with `--skip-deploys`. Never render resolved secrets in terminal output.
6. Commit and push the release only after lint, typecheck, the full PostgreSQL-backed test suite, build, and the
   pinned Docker build pass. Confirm the worker uses `/railway.worker.json` and the API uses `/railway.json`.
7. Deploy and start `florence-worker-v2` first. Its pre-deploy must migrate the sibling database successfully.
   For this release, pre-deploy applies only 022–023. Run the current migrator a second time and require a no-op;
   recheck that the five governed-skill tables are unchanged, the preserved 001–021 migration rows still match
   exactly, and 022–023 match the current committed digests. Then wait for the worker lease to be live and fresh:

   ```sql
   select release_id, now() - last_seen_at as heartbeat_age, stopped_at
   from florence_v4.worker_leases
   where lease_name = 'florence-worker-singleton';
   ```

   `stopped_at` must be null and `heartbeat_age` must remain below 30 seconds.
8. Deploy and start `api` from the same commit. Railway must not switch traffic until `/readyz` returns 200
   against the sibling `florence_v4` schema.
9. Run the automated smoke below, then the Jackson/Kendall onboarding acceptance. Keep the former PostgreSQL
   service and encrypted backup intact until a separate, explicit retirement decision.

Before the first new inbound event, rollback is simply: stop both application services, restore both database
references to the retained service, and redeploy the matching prior image. After new state reaches the clean
database, do not blindly repoint to the old service; restore or repair the new database so accepted state is not
lost.

Review Railway's staged changes before applying them. Confirm the web service shows the `/railway.json` file
icon and the worker shows `/railway.worker.json` in deployment details. The production pre-deploy must finish
before either start command runs. Never bypass it by starting against the old schema manually.

After Railway marks both services active:

```bash
pnpm smoke:production -- https://<canonical-host>
```

The smoke command validates liveness, database/migration readiness, security headers, the built React bundle,
privacy, and terms without using a customer session or printing credentials. Then perform one private-DM
onboarding flow, one consented-group ingest, and one coverage loop through acknowledged ownership.

## Health and operations

- `/healthz` proves only that the web process can answer HTTP.
- `/readyz` proves PostgreSQL is reachable and at least one migration is installed. Railway uses it only while
  activating a deployment, so configure a separate continuous HTTPS monitor.
- Worker freshness must be checked independently. Alert when its heartbeat is stale, when the oldest available
  `pending`/`retry` job exceeds the product SLO, or when any job/outbox row becomes `dead` or `ambiguous`.
- Keep `LOG_LEVEL=info` in production. Never enable request-body logging or copy provider exceptions into logs.
- Keep the worker at one replica until its singleton lease and work/effect ownership model deliberately support
  more. The web service can be scaled after multi-replica ingress is exercised against the same database.

## Migrations, rollback, and secrets

Never edit an applied migration. Add a new numbered migration and take a database backup before destructive or
incompatible schema changes. Application rollback is safe only when the previous image understands the current
schema. Otherwise restore a coordinated database backup and matching application image.

After the first successful private smoke, rotate credentials that have appeared anywhere outside sealed Railway
variables. Rotate one connector at a time, deploy both services, and repeat the production smoke plus that
connector's live check before rotating the next secret.

Railway references: [Config as Code](https://docs.railway.com/config-as-code),
[pre-deploy commands](https://docs.railway.com/deployments/pre-deploy-command),
[healthchecks](https://docs.railway.com/deployments/healthchecks), and
[service variables](https://docs.railway.com/variables).
