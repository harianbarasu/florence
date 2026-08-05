# Florence deployment and operations runbook

Last reviewed: 2026-08-05

This runbook is the production procedure for the founding-family Florence pilot. It covers local
setup, Railway's application/PostgreSQL topology, Linq, Google OAuth and Gmail Pub/Sub,
`harianbarasu.com`, credential rotation, and the morning acceptance test. It contains no live
credential values.

## Release topology

One Git commit and one Docker image support two production topologies. Use exactly one of them:

| Mode | Service | Public | Config path | `FLORENCE_PROCESS_ROLE` | Entrypoint | Responsibility |
| --- | --- | --- | --- | --- | --- | --- |
| Combined | `florence` | Yes | `/railway.json` | `all` | `node dist/server.js` | HTTP interface and all durable background loops |
| Split | `florence-web` | Yes | `/railway.json` | `web` | `node dist/server.js` | Webhooks, OAuth, legal pages, health, metrics, operator API |
| Split | `florence-worker` | No | `/railway.worker.json` | `worker` | `node dist/worker.js` | Durable inboxes, Gmail sync/backfill, timers, agent work, outbox effects |
| Both | `Postgres` | No | Railway-managed | n/a | Railway-managed | Authoritative domain, queue, source, audit, and connector state |

Start the founding-family pilot in combined mode unless there is a concrete need for separate
failure or scaling boundaries. Follow the web-first sequence in section 4 when moving to split
mode. Never leave a process in `all` while a worker is running: both would claim background work.
The runtime rejects mismatched entrypoints and roles.

Every application service runs `node dist/cli/migrate.js` as a Railway pre-deploy command. The
migrator serializes concurrent deploys with a PostgreSQL advisory lock and applies pending files in
one transaction. Railway runs pre-deploy commands after the image is built, with service variables
and private networking available; a non-zero result prevents the deployment from proceeding.
[Railway pre-deploy commands](https://docs.railway.com/deployments/pre-deploy-command)

Only the combined or web service gets public networking. A split worker and PostgreSQL stay on
Railway's private network. Never create a worker domain or expose PostgreSQL through a TCP proxy.

## 1. Release gate

Use Node 24 explicitly on machines that have multiple Node versions.

```bash
corepack enable
pnpm install --frozen-lockfile
pnpm check
docker build --tag florence:release .
git status --short
```

The final command must show no unexpected generated or secret-bearing file. The build context
excludes `.env*`, `.npmrc`, private-key formats, logs, databases, dependencies, and build output.
Do not pass secrets as Docker build arguments: Florence reads them only at runtime.

The image defaults to the server entrypoint and runs as the unprivileged `node` user. The default
role is combined (`all`); Railway overrides the image command with the service-specific start
commands above.

## 2. Local setup

Requirements:

- Node 24.19 and pnpm 10.10;
- PostgreSQL 18, or Docker Desktop for the included Compose database;
- a non-production Linq subscription and Google OAuth client if testing real connectors locally.

```bash
cp .env.example .env
docker compose up -d postgres
pnpm install --frozen-lockfile
```

Generate independent random values for `FLORENCE_TOKEN_ENCRYPTION_KEY`,
`FLORENCE_ADMIN_API_KEY`, `GOOGLE_OAUTH_STATE_SECRET`, and
`GOOGLE_PUBSUB_VERIFICATION_TOKEN`. This command emits one 32-byte base64url value; run it once per
secret and paste the results only into `.env` or Railway's variable UI:

```bash
node -e 'process.stdout.write(require("node:crypto").randomBytes(32).toString("base64url") + "\n")'
```

Fill the required core application values, then migrate. Provider values may remain blank until
connector testing. The migrator loads `.env` and fails closed if a required core value is blank:

```bash
pnpm db:migrate
```

For combined local operation, leave `FLORENCE_PROCESS_ROLE=all` and run:

```bash
pnpm dev
```

For a split-topology rehearsal, run these in separate terminals:

```bash
FLORENCE_PROCESS_ROLE=web pnpm dev
FLORENCE_PROCESS_ROLE=worker pnpm dev:worker
```

Do not run the combined command at the same time as the split worker.

Local health checks:

```bash
curl --fail-with-body http://localhost:3000/healthz
curl --fail-with-body http://localhost:3000/readyz
```

Do not repoint the production Linq or Pub/Sub subscription at a laptop. Use a separate development
subscription and a deliberate HTTPS tunnel, or test connector parsing with the repository fixtures.

## 3. Runtime variables

Use Railway service variables, not committed files. A Railway reference variable can expose the
private PostgreSQL URL to each application service without copying the value; Railway documents
cross-service references with the service namespace. [Railway variable
references](https://docs.railway.com/variables/reference)

### Required on every application service

| Variable | Production value or rule |
| --- | --- |
| `NODE_ENV` | `production` (also set in the image) |
| `FLORENCE_PROCESS_ROLE` | `all` for one service, or exactly one `web` plus one `worker`; never mix the modes |
| `LOG_LEVEL` | `info` initially; never use `trace` with real family data |
| `FLORENCE_DATABASE_URL` | Railway reference to the private PostgreSQL `DATABASE_URL` |
| `FLORENCE_DB_SCHEMA` | `florence` |
| `FLORENCE_WEB_BASE_URL` | `https://harianbarasu.com` after domain cutover |
| `FLORENCE_TOKEN_ENCRYPTION_KEY` | One 32-byte base64url/hex key, identical on every application service |
| `FLORENCE_ADMIN_API_KEY` | Independent random operator bearer token |
| `FLORENCE_DEFAULT_TIMEZONE` | `America/Los_Angeles` for the founding family |
| `GOOGLE_CLIENT_ID` | Web OAuth client ID |
| `GOOGLE_CLIENT_SECRET` | Web OAuth client secret |
| `GOOGLE_REDIRECT_URI` | `https://harianbarasu.com/oauth/google/callback` after cutover |

The config loader requires the core Florence values in every process. Duplicate secrets as
service-scoped variables when Railway cannot reference a shared value; do not place the values in
GitHub Actions or build-time variables.

### Provider variables

The v1 composition eagerly constructs shared deterministic adapters, even in split mode. Put the
complete enabled-provider configuration on every application service so startup validation and
readiness are identical. Provider credentials are used only inside those connector adapters; they
are not included in prompts or ephemeral-agent context. This is intentional for a small, reliable
first production system, not permission for models to access raw credentials.

| Variable | Rule |
| --- | --- |
| `LINQ_WEBHOOK_SECRET` | Exact `whsec_...` secret returned for the production subscription |
| `GOOGLE_OAUTH_STATE_SECRET` | Independent random value used only for short-lived handoffs |
| `GOOGLE_PUBSUB_VERIFICATION_TOKEN` | Independent random query token used by the Gmail push route |
| `LINQ_API_KEY` | Linq v3 bearer credential |
| `LINQ_BASE_URL` | `https://api.linqapp.com/api/partner/v3` |
| `LINQ_FROM_PHONE` | Florence's assigned E.164 phone number |
| `GOOGLE_GMAIL_TOPIC_NAME` | `projects/<project-id>/topics/<topic-name>` |
| `GOOGLE_GMAIL_PUBSUB_SUBSCRIPTION` | Full subscription name from Pub/Sub |
| `MODEL_PROVIDER` | `openai`, `anthropic`, or `open-weight` |
| provider-specific model variables | Only the key/base URL/model selected by `MODEL_PROVIDER` |
| `WORKER_POLL_INTERVAL_MS` | Start with `1000` |
| `WORKER_LEASE_SECONDS` | Start with `60` |
| `DAILY_BRIEF_LOCAL_TIME` | Start with `06:30` |

An integration is enabled only when its complete variable set is present. Do not configure half of
an integration. If composition validation reports a missing variable, correct the named integration
on every application service before exposing it to a family.

`PORT` is supplied by Railway to the web service. Do not set a public port or domain on the worker.

## 4. Create the Railway services

Railway supports a service-specific config-as-code path; values in that file override the dashboard
for that deployment. [Railway config as code](https://docs.railway.com/config-as-code/reference)

### Combined founding-family deployment

1. Open the Florence Railway project and add a PostgreSQL service.
2. Add `florence` from `harianbarasu/florence`, branch `main`, root directory `/`.
3. Set its config file path to `/railway.json` and `FLORENCE_PROCESS_ROLE=all`.
4. Add every variable from the previous section.
5. Give this service a temporary Railway domain and deploy.
6. Confirm it reaches `GET /readyz` before Railway's 180-second deadline and that its background
   loops remain healthy.

### Split deployment

1. Use the same PostgreSQL service.
2. Configure `florence-web` from `harianbarasu/florence`, branch `main`, root `/`, config file
   `/railway.json`, and `FLORENCE_PROCESS_ROLE=web`.
3. Configure `florence-worker` from the same repository and branch, root `/`, config file
   `/railway.worker.json`, and `FLORENCE_PROCESS_ROLE=worker`.
4. Add every variable from the previous section to both services.
5. Give only `florence-web` a temporary Railway domain.
6. Deploy web and worker together. Both pre-deploy steps may run concurrently; the migrator lock
   makes this safe.
7. Confirm web reaches `GET /readyz` before Railway's 180-second deadline.
8. Confirm worker remains running. V1 has no worker HTTP health surface; prove liveness with the
   canary commitment in the morning smoke test, not by assuming an idle process is healthy.

To move from combined to split mode, first change and redeploy the existing service from `all` to
`web`. Its ingress remains available while background jobs pause. Verify `/readyz` and confirm
`/operator/status` now reports the local worker as unavailable; only then create or deploy the
`worker` service. This sequencing preserves exactly one background owner throughout the cutover.

Do not override the start or pre-deploy commands in the dashboard. If a deployment shows a
dashboard override, remove it so the checked-in JSON remains authoritative. Railway's deployment
details indicate which values came from config as code.

## 5. Configure `harianbarasu.com`

Attach the apex domain to the combined or web service, never the worker. Railway's current domain
flow returns the DNS records needed for routing and ownership verification. Add the exact CNAME and
TXT records it shows; do not invent a target or omit the verification record. Railway notes that
requests can 404 until ownership is verified and DNS propagation can take time. [Railway custom-domain
command](https://docs.railway.com/cli/domain)

Dashboard flow:

1. Combined/web service → Settings → Networking → Custom Domain.
2. Enter `harianbarasu.com` and select the web process port.
3. Add exactly the records Railway provides at the DNS host for `harianbarasu.com`.
4. Wait for Railway to show the domain and TLS certificate as active.
5. Verify before changing any provider callback:

```bash
curl --fail-with-body https://harianbarasu.com/healthz
curl --fail-with-body https://harianbarasu.com/readyz
curl --fail-with-body https://harianbarasu.com/privacy
curl --fail-with-body https://harianbarasu.com/terms
```

Cut over in this order so OAuth and webhooks do not point at an unverified host:

1. Add `https://harianbarasu.com/oauth/google/callback` to Google as an additional authorized
   redirect URI.
2. Set `FLORENCE_WEB_BASE_URL=https://harianbarasu.com` and set `GOOGLE_REDIRECT_URI` to the exact
   callback on every application service; redeploy.
3. Update the Linq subscription URL.
4. Update the Pub/Sub push endpoint.
5. Complete the end-to-end smoke test.
6. Keep the Railway domain available for infrastructure diagnosis, but never use it in links sent to
   families after cutover.

Google requires a web application's redirect URI to exactly match a configured URI, including
scheme, case, path, and trailing slash. Production redirects must use HTTPS. [Google web-server OAuth
guide](https://developers.google.com/identity/protocols/oauth2/web-server)

## 6. Configure Linq

Florence implements Linq partner API v3 and pins webhook payload version `2026-02-03`.

1. Confirm the production API key and assigned Florence phone number in Linq.
2. Create a webhook subscription for:
   - `message.received`
   - `reaction.added`
   - `reaction.removed`
3. Set the endpoint to:

   ```text
   https://harianbarasu.com/webhooks/linq?version=2026-02-03
   ```

4. Put the returned signing secret, API key, and E.164 phone number on every application service.
5. Send a direct inbound iMessage to the Florence number. Inbound-first initiation is part of the
   consent and phone-reputation strategy.

Linq recommends pinning a dated webhook version. Its webhooks use Standard Webhooks signatures over
the raw body, deliver at least once, and retry eligible failures; Florence verifies the signature,
stores the delivery durably, and acknowledges before background processing. [Linq webhook
guide](https://docs.linqapp.com/guides/webhooks/)

Never paste the signing secret into a test request, issue, or log. A request without a valid
signature should fail; that negative check is included in the smoke test.

## 7. Configure Google OAuth and Gmail Pub/Sub

Use one Google Cloud project whose ID matches the project segment in the Gmail Pub/Sub topic.

### OAuth

1. Enable Gmail API, Google Calendar API, and Cloud Pub/Sub API.
2. During first-night testing, keep the OAuth app in `Testing` and add Hari and the
   co-founder/partner accounts as test users. Because Florence requests Gmail/Calendar scopes,
   Google expires authorizations and refresh tokens from an External/Testing app after seven days;
   reconnect weekly until the app has an appropriate production publishing/verification state.
3. Set the application home page to `https://harianbarasu.com`, privacy policy to
   `https://harianbarasu.com/privacy`, and terms to `https://harianbarasu.com/terms`.
4. Verify ownership of `harianbarasu.com` in Search Console and add it as an authorized domain.
5. Create a Web application OAuth client.
6. Add the exact redirect URI:

   ```text
   https://harianbarasu.com/oauth/google/callback
   ```

7. Set the full Google variable set on every Florence application service.

Florence requests OpenID identity plus Gmail read-only and Google Calendar scopes. Gmail message
access is a restricted scope; a broader SaaS launch requires the appropriate Google OAuth
verification and may require a security assessment before unapproved users connect. Google also
requires production OAuth branding, owned domains, a public home page, HTTPS, and narrow handling
of callback credentials. [Google OAuth production-readiness
policy](https://developers.google.com/identity/protocols/oauth2/production-readiness/policy-compliance),
[Google refresh-token expiration](https://developers.google.com/identity/protocols/oauth2),
[Gmail scope classifications](https://developers.google.com/workspace/gmail/api/auth/scopes)

### Gmail push

1. Create a topic, for example `florence-gmail`, in the same project as the OAuth client.
2. Grant Pub/Sub Publisher on that topic to:

   ```text
   gmail-api-push@system.gserviceaccount.com
   ```

3. Create a push subscription on that topic.
4. Set its HTTPS endpoint to the following, substituting the secret value only in Google Cloud and
   Railway:

   ```text
   https://harianbarasu.com/webhooks/google/gmail?token=<GOOGLE_PUBSUB_VERIFICATION_TOKEN>
   ```

5. Set `GOOGLE_GMAIL_TOPIC_NAME`, the full `GOOGLE_GMAIL_PUBSUB_SUBSCRIPTION`, and the same random
   verification token on every application service.
6. Leave payload wrapping enabled; Florence expects the standard Pub/Sub push envelope.

Google's Gmail push guide requires granting its service account publish access, describes the
base64url notification envelope, and says mailbox watches must be renewed at least every seven days
(daily is recommended). Florence stores watch expiration and reconciles renewal/background history
work durably. [Gmail push notifications](https://developers.google.com/workspace/gmail/api/guides/push)

The endpoint returns `202` only after the notification is durably accepted. Pub/Sub treats `202` as
an acknowledgement and retries non-success responses. [Pub/Sub push delivery](https://docs.cloud.google.com/pubsub/docs/push)

Do not manually call Gmail `watch`; Florence starts it after a successful, adult-owned OAuth
connection. Each Google account remains bound to that adult, and email content stays personal until
Florence proposes and receives authorization for the minimum household meaning.

## 8. Deploy and observe

For a GitHub-connected service, merge the verified commit to `main` and watch both deployments. For
a deliberate CLI deployment, Railway's application command is `railway up`, not `railway deploy`.
[Railway CLI deployment](https://docs.railway.com/cli/deploying)

Expected sequence for each service:

1. Docker image builds.
2. Pre-deploy migrator reports either applied migrations or `Database is current.`
3. Combined starts HTTP and its background loops, or web passes `/readyz` while worker starts its
   durable polling loops.
4. No credential, OAuth token, message body, email content, raw query, or webhook body appears in
   logs.

If `/healthz` is `200` but `/readyz` is `503`, the process is alive but a required dependency or
integration is unavailable. Inspect the redacted startup error and variable names; never print
variable values. Do not bypass readiness to make the deployment green.

## 9. Morning smoke test

Run this after every credential cutover and before inviting another adult.

### HTTP and operator checks

```bash
export FLORENCE_URL=https://harianbarasu.com
curl --fail-with-body "$FLORENCE_URL/healthz"
curl --fail-with-body "$FLORENCE_URL/readyz"
curl --fail-with-body "$FLORENCE_URL/"
curl --fail-with-body "$FLORENCE_URL/privacy"
curl --fail-with-body "$FLORENCE_URL/terms"
```

Read the operator key without echoing it and inspect the dependency status:

```bash
read -s FLORENCE_OPERATOR_TOKEN
curl --fail-with-body \
  -H "Authorization: Bearer $FLORENCE_OPERATOR_TOKEN" \
  "$FLORENCE_URL/operator/status"
unset FLORENCE_OPERATOR_TOKEN
```

In combined mode, every reported check should be `ok`. In split mode, the web process truthfully
reports `worker: unavailable` and aggregate `degraded` because v1 has no cross-process heartbeat;
this does not fail `/readyz`. Confirm the worker is independently running, then confirm
unauthenticated provider calls fail closed:

```bash
curl --output /dev/null --write-out '%{http_code}\n' \
  -X POST -H 'content-type: application/json' --data '{}' \
  "$FLORENCE_URL/webhooks/linq"
curl --output /dev/null --write-out '%{http_code}\n' \
  -X POST -H 'content-type: application/json' --data '{}' \
  "$FLORENCE_URL/webhooks/google/gmail"
```

The Linq request should be rejected for invalid signature; the Gmail request should be rejected for
missing verification token. Neither should create provider-inbox work.

### Real family loop

1. From Hari's phone, send a new direct iMessage to Florence. Confirm one response and no duplicate.
2. Complete the in-message consent flow and confirm private onboarding remains private.
3. Ask Florence to invite the second adult. Have that adult accept in a private DM.
4. Create the family group with both adults and Florence; register it through onboarding.
5. Send one group commitment with an owner and time. Confirm the owner must acknowledge and the
   reminder is group-visible without blame language.
6. Ask privately to connect Google. Open the short-lived browser handoff, select the intended test
   account, and confirm the flow returns to the connected page.
7. Send that Gmail account a harmless synthetic school-style message. Confirm:
   - it is imported once;
   - any review or promotion request goes only to the owning adult's DM;
   - no raw subject, sender, body, or sensitive detail appears in the household group;
   - approving a minimum household meaning shares only that meaning.
8. Ask the group for one bounded family research request. Confirm the project result returns with
   evidence and does not execute an external action.
9. Ask for a meal plan and grocery list. Confirm it runs only because someone asked.
10. Restart the background owner (the combined service or `florence-worker`) during queued work.
    Confirm the lease is recovered and there is one logical result, not two messages.

### Log and durability check

- Search every application-service log for obvious secret prefixes and the synthetic email text;
  there must be no match.
- Confirm the background owner is still running after the test and `/operator/status` reports the
  expected combined or split-topology state described above.
- Confirm Railway PostgreSQL backup protection is enabled before importing real family history.
- Record failures by provider event/job/action ID only, never by copying message or email content.

## 10. Credential rotation checklist

Credentials used during early development or shared through a conversational channel must be
treated as exposed and rotated before real family data is connected.

General rotation sequence:

1. Create a replacement at the provider or with a cryptographically secure generator.
2. Put it into the minimum Railway service scope that needs it.
3. Redeploy and run the relevant health/negative/live smoke checks.
4. Revoke the old value at the provider.
5. Search Git history, build output, Railway logs, and local untracked files for the old value without
   printing it to shared output.
6. Record only the credential name, owner, creation date, and next rotation date—not its value.

Per-credential notes:

| Credential | Rotation procedure |
| --- | --- |
| `LINQ_API_KEY` | Create replacement, update all application services, test one idempotent reply, then revoke old key |
| `LINQ_WEBHOOK_SECRET` | Linq cannot mutate this secret. In a maintenance window, recreate the subscription, immediately update every application service, verify signed delivery, then remove any superseded subscription |
| `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` / open-weight key | Update all application services, run a bounded no-action request, revoke old key |
| `GOOGLE_CLIENT_SECRET` | Replace on all application services, test OAuth and refresh, then retire the old secret |
| `GOOGLE_OAUTH_STATE_SECRET` | Update all application services; outstanding short-lived handoff links become invalid and must be reissued |
| `GOOGLE_PUBSUB_VERIFICATION_TOKEN` | Update Railway and the push endpoint as one maintenance action; Pub/Sub retries failures during a brief mismatch |
| `FLORENCE_ADMIN_API_KEY` | Update all application services, verify old token is rejected and new token succeeds |
| PostgreSQL password | Rotate through Railway, update all references/connections, redeploy, and check migration/readiness |

### Encryption-key warning

`FLORENCE_TOKEN_ENCRYPTION_KEY` encrypts durable OAuth grants and private source content. Florence
does not yet have an online key-ring/rewrap command, so blindly replacing this value makes existing
ciphertext unreadable. For the pre-pilot deployment, rotate it **before the first real OAuth
connection or email import**. If encrypted data already exists, stop and implement/test a versioned
rewrap migration or deliberately reset the non-production data and reconnect accounts. Never rotate
this key by simply changing the Railway variable on a live household.

## 11. Failure, rollback, and recovery

- **Failed pre-deploy:** do not start the new image. Fix the migration/configuration and redeploy.
- **Web unhealthy:** keep providers pointed at the stable prior domain/deployment; Linq and Pub/Sub
  will retry eligible failures.
- **Background owner unhealthy:** in split mode, leave web ingress running so events are durably
  accepted, then restore the worker. In combined mode, provider retries bridge the outage. Lease
  expiry permits safe recovery in either mode.
- **Expired Gmail cursor:** Florence falls back to a recent scan and continues the durable backfill.
- **Lost Linq delivery:** reconcile known chats or request provider replay; never manufacture an
  event ID.
- **Schema change:** Florence uses forward-only migrations and does not maintain backward
  compatibility layers. Prefer a forward fix. Redeploying an older image is safe only when it is
  known to understand the current schema.
- **Provider compromise:** disable/revoke the affected connection first, preserve non-secret audit
  identifiers, rotate credentials, and reconnect explicitly.

Before real family data, enable and test the Railway PostgreSQL backup/restore option appropriate to
the account. A backup is not proven until it has been restored into an isolated database and the
release gate passes against it. [Railway PostgreSQL backups](https://docs.railway.com/volumes/backups)

## Primary references

- [Railway config as code](https://docs.railway.com/config-as-code/reference)
- [Railway pre-deploy commands](https://docs.railway.com/deployments/pre-deploy-command)
- [Railway custom domains](https://docs.railway.com/cli/domain)
- [Linq webhooks](https://docs.linqapp.com/guides/webhooks/)
- [Linq sending messages](https://docs.linqapp.com/guides/messaging/sending-messages/)
- [Google web-server OAuth](https://developers.google.com/identity/protocols/oauth2/web-server)
- [Google OAuth production readiness](https://developers.google.com/identity/protocols/oauth2/production-readiness/policy-compliance)
- [Gmail push notifications](https://developers.google.com/workspace/gmail/api/guides/push)
- [Cloud Pub/Sub push delivery](https://docs.cloud.google.com/pubsub/docs/push)
