# Florence production runbook

Florence runs as two Railway services from one immutable Docker image:

| Role | Config | Start command | Public |
| --- | --- | --- | --- |
| API/web | `/railway.json` | `node apps/api/dist/server.js` | Yes, one HTTPS origin |
| Worker | `/railway.worker.json` | `node apps/worker/dist/server.js` | No |

Both use the same PostgreSQL service, commit, migration set, and shared variables. Neither application
service has durable local storage. PostgreSQL is canonical, including encrypted image artifacts.

## Stop conditions

Do not deploy when any of these is true:

- `pnpm check` or the Docker build is red;
- API and worker do not reference the same commit;
- the exact PostgreSQL service reference is uncertain;
- no verified backup exists;
- migration `007_household_signal_core.sql` has not been tested from `001` on a disposable database;
- production data in the former tables is still needed but has no explicit migration into the new
  household event spine;
- Linq or Google callback registrations point at another origin;
- either adult lacks a separate pilot credential/session;
- the worker singleton heartbeat is absent or stale;
- a live connector rehearsal would require real family data instead of synthetic data.

This branch does not automatically translate the former Florence data model. Migrations `001–006`
remain immutable for ledger safety. Migration `007` adds the new product spine alongside it. Before
cutover, explicitly decide whether existing production rows are disposable pilot data or require a
separate, reviewed data migration. Never infer that decision from schema compatibility.

## Railway topology

Project: `Florence`.

- API Config-as-Code path: `/railway.json`.
- Worker Config-as-Code path: `/railway.worker.json`.
- API health path: `/readyz`.
- API binds `0.0.0.0:$PORT`; Railway owns `PORT`.
- Worker stays at exactly one replica and owns PostgreSQL advisory lock `4607346623`.
- API overlap is 5 seconds; worker overlap is zero.
- Both predeploy with `node packages/database/dist/predeploy.js`.

Two PostgreSQL service records currently exist in Railway. Network evidence previously showed the
applications using the service named `Postgres-8tFu`. Confirm reference links and backups in the
Railway UI. Do not delete, replace, or rewire either database record during an application release.

## Environment contract

Required on API and worker:

| Variable | Contract |
| --- | --- |
| `NODE_ENV` | `production` |
| `FLORENCE_DATABASE_URL` | Private Railway PostgreSQL reference |
| `FLORENCE_POSTGRES_SCHEMA` | Exactly `florence_v4` |
| `FLORENCE_WEB_BASE_URL` | Clean canonical HTTPS origin |
| `FLORENCE_IMAGE_VAULT_KEY` | Canonical base64 32-byte key; same on both roles |
| `LINQ_API_KEY` | Linq partner API key |

Required on API:

| Variable | Contract |
| --- | --- |
| `FLORENCE_PILOT_CREDENTIALS` | JSON array of exactly two separate adult credentials |
| `FLORENCE_SESSION_SECRET` | At least 32 random bytes |
| `FLORENCE_ENROLLMENT_SECRET` | At least 32 random bytes |
| `LINQ_WEBHOOK_SECRET` | Linq Standard Webhooks signing secret |
| `LINQ_PARTNER_ID` | Exact expected partner ID |

Required on worker:

| Variable | Contract |
| --- | --- |
| `OPENAI_API_KEY` | Model credential |
| `OPENAI_MODEL` | Pinned configured model |

Google is optional, but these four variables are all-or-none on both roles:

- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_CREDENTIAL_KEY` — canonical base64 32-byte key
- `FLORENCE_WEB_BASE_URL` — callback is derived as `/oauth/google/callback`

Optional bounded settings:

- `LOG_LEVEL=info`
- `FLORENCE_IMAGE_RETENTION_DAYS=30` (1–365)
- `FLORENCE_MODEL_TIMEOUT_MS=30000`
- `FLORENCE_MODEL_MAX_OUTPUT_TOKENS=4000`

Never print resolved Railway variables or put them in shell arguments, source control, fixtures, or
model context. Use Railway sealed variables/reference variables.

## Connector registration

Register exactly:

- Linq webhook: `<origin>/webhooks/linq`, payload version `2026-02-03`.
- Google OAuth redirect: `<origin>/oauth/google/callback`.

The Linq API credential is not the webhook signing secret. The API verifies the raw signature before
business parsing, then re-reads live chat participants before accepting content. Test signature
failure, duplicate delivery, participant drift, and one image before enabling real family traffic.

Google uses `openid email gmail.readonly calendar.events.owned`. Gmail read access is restricted and
may require Google verification/security review for public launch. An External consent screen in
Testing may expire refresh tokens after seven days; that is acceptable only for a deliberate pilot
reconnect cadence.

## Build gates

From a clean checkout on Node 24.19:

```bash
corepack enable
pnpm install --frozen-lockfile
pnpm check
docker build --tag florence:release .
docker run --rm --entrypoint node florence:release --version
```

On a disposable PostgreSQL database, set `FLORENCE_DATABASE_URL` and
`FLORENCE_POSTGRES_SCHEMA=florence_v4`, then run `pnpm db:migrate` twice. Both must pass; the ledger
must contain seven entries ending in `007_household_signal_core.sql`.

## Cutover sequence

1. Disable GitHub autodeploy for API and worker.
2. Verify the exact database reference and take a restorable backup.
3. Resolve the old-data decision explicitly; do not continue with an unanswered migration need.
4. Configure the shared variables and connector callbacks without triggering a deployment.
5. Deploy the worker from the exact release commit using `/railway.worker.json`.
6. Confirm its singleton lease remains current for at least 30 seconds:

   ```sql
   select worker_id, release_id, now() - last_seen_at as heartbeat_age, stopped_at
   from florence_v4.worker_leases
   where lease_name = 'florence-worker-singleton';
   ```

   `stopped_at` must be null and `heartbeat_age` under 30 seconds.
7. Deploy API from the same commit using `/railway.json`.
8. Wait for `/readyz` to return 200, then run:

   ```bash
   pnpm smoke:production -- https://<canonical-host>
   ```

9. Using only synthetic pilot data, rehearse two browser sessions, two private Linq enrollments, the
   exact two-adult group bootstrap, one episode through ownership/reminder/completion, one Gmail
   private candidate/promotion, and one Calendar approval/reread receipt.
10. Re-enable autodeploy only after both roles show the same commit and the worker heartbeat remains
    fresh.

No step in this runbook authorizes deleting old tables, dropping a schema, changing domains, or
restoring a backup over production.

## Health, rollback, and incident response

- `/healthz` proves only that the API process answers HTTP.
- `/readyz` proves PostgreSQL is reachable and migration `007` is present.
- Continuously monitor the worker lease, oldest pending signal/effect, and dead signals.
- Keep request bodies, cookies, authorization headers, provider payloads, and exceptions out of logs.
- Roll back application code only when the prior image understands the current schema. Otherwise
  stop both roles and coordinate a matching database restore.
- Never edit an applied migration. Add the next numbered migration after taking a backup.
- Rotate one connector credential at a time, deploy both roles, and repeat that connector's synthetic
  smoke before rotating another.

Production deployment remains an explicit operator action. Passing local gates does not grant
permission to deploy or mutate production data.
