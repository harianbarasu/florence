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

The existing production project already has all three service records. Do not create or replace PostgreSQL: its
volume contains pre-rebuild data. Connect `florence-worker-v2` to the same `harianbarasu/florence` GitHub source
as `api`, select `/railway.worker.json`, and add its missing data-keyring variables before its first deployment.
Keep the current Railway domain for the first private smoke unless the Google and Linq callback registrations are
changed in the same release.

Before the rebuild deploy, replace the `api` service's current `NODE_ENV=development` override with
`NODE_ENV=production`, set `FLORENCE_POSTGRES_SCHEMA=florence_v4` on both services, and make their database,
token-key, data-keyring, Linq, Google, and active-model values exact shared/reference variables. The existing
worker values are not one coherent runtime contract and must not be deployed as-is.

Both service configs run `node dist/ops/predeploy-production.js` as their only pre-deploy command. It fails
before touching PostgreSQL unless `NODE_ENV=production`, TLS is required, the full runtime contract is valid,
and the target schema is exactly `florence_v4`; it then runs the migration with secret-safe output. Railway
deploys services from a GitHub push independently, so either service must be able to prepare the schema before
starting. Migrations are serialized by a PostgreSQL advisory lock and verify the digest of every previously
applied file.

## Exact environment contract

Make the runtime variables shared by `api` and `florence-worker-v2`. Set
`FLORENCE_DATABASE_URL=${{Postgres.DATABASE_URL}}` as a reference variable on both services. Do not define
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

There is no separately configured Google redirect URI. Florence derives it as
`<FLORENCE_WEB_BASE_URL>/oauth/google/callback`, which prevents the browser origin, cookie boundary, and OAuth
callback from drifting apart. Register that exact URI in Google Cloud before deployment. If the canonical
domain changes, update Google and `FLORENCE_WEB_BASE_URL` together.

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
2. In Google Cloud, register `<origin>/oauth/google/callback` on the OAuth web client and add the intended test
   users while the consent screen remains in testing. Enable both the Gmail API and Google Calendar API for
   the project. Florence offers two least-privilege connection profiles: personal/family requests read-only
   Mail and Calendar, while work requests read-only Calendar only. Each connection forces Google's account
   chooser so one person can attach multiple Google accounts deliberately.
3. Send only synthetic messages during the first connector smoke. Confirm webhook authentication failures do
   not create provider-event rows and that a duplicate delivery does not create duplicate work or output.

For the first Google smoke, connect a personal test account from Sources. Confirm that the card reaches “Keeping
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

### First rebuild cutover

Use this sequence for the first `florence_v4` deployment. It keeps the current API and legacy schema untouched
until the replacement worker is healthy:

1. Take and verify a Railway PostgreSQL backup. Do not delete, rename, reset, or restore over the existing
   database service or legacy `florence` schema.
2. Temporarily disable GitHub autodeploy on `api`. Connect `florence-worker-v2` to the same repository and keep
   its autodeploy disabled until the cutover finishes.
3. Commit and push the exact release after `pnpm check` and the Docker build pass. This makes one auditable source
   commit available without changing the live API.
4. Update variables without triggering the old image: set `NODE_ENV=production` and
   `FLORENCE_POSTGRES_SCHEMA=florence_v4`; use the same private Postgres reference, token key, complete data
   keyring, Linq account/key/number/webhook secret, Google client, and active model route on both services. Use
   Railway shared/reference variables rather than copying rendered secret values through a terminal.

   Railway's sealed-variable UI is preferred for secrets. If an operator uses the CLI, send one secret through
   standard input so it never appears in shell history, and suppress the automatic deployment while the full
   contract is being assembled:

   ```bash
   railway variable set SECRET_VARIABLE --stdin --skip-deploys --service api --environment production
   ```

   Set the non-secret release guards explicitly on each service without starting a deployment:

   ```bash
   railway variable set NODE_ENV=production FLORENCE_POSTGRES_SCHEMA=florence_v4 --skip-deploys --service api --environment production
   railway variable set NODE_ENV=production FLORENCE_POSTGRES_SCHEMA=florence_v4 --skip-deploys --service florence-worker-v2 --environment production
   ```

   Do not use `railway variable --json`, `--kv`, or any command that renders the resolved environment during
   release preparation. Review variable names and reference links in Railway's UI, then apply all staged changes
   together.
5. Confirm the worker's Config-as-Code path is `/railway.worker.json`, then deploy that exact commit to
   `florence-worker-v2`. Its pre-deploy creates or verifies only `florence_v4`. Wait for the worker lease to be
   live and fresh:

   ```sql
   select release_id, now() - last_seen_at as heartbeat_age, stopped_at
   from florence_v4.worker_leases
   where lease_name = 'florence-worker-singleton';
   ```

   `stopped_at` must be null and `heartbeat_age` must remain below 30 seconds.
6. Confirm `api` uses `/railway.json`, then deploy the same commit. Railway must not switch traffic until the new
   `/readyz` returns 200 against `florence_v4`. If pre-deploy or readiness fails, stop: the previous API remains
   live and no legacy schema was modified.
7. Run the automated smoke below, then one synthetic Linq delivery, one private onboarding, one consented-group
   ingest, and one coverage loop through explicit acknowledgement. Inspect only stable error codes in logs.
8. Re-enable autodeploy only after both services show the same commit/release, coherent shared variables, and a
   fresh worker heartbeat. Keep the legacy schema and backup until a separate, explicit retirement decision.

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
