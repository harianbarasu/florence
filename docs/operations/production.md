# Florence production runbook

Florence runs as one Railway API/web service from `railway.json`, plus one fresh PostgreSQL service. The API process owns the direct conversation loop; there is no worker service, singleton lease, generic queue, or legacy migration chain.

## Stop conditions

Do not deploy when any of these is true:

- `pnpm check`, the three live-PostgreSQL scenarios, or the Docker build is red;
- the exact PostgreSQL service or restorable backup is uncertain;
- the database is not an explicitly approved fresh pilot database;
- Linq or Google callback registrations point at another origin;
- the pilot database already contains a household before the intended founder
  starts onboarding, or the Florence number has been distributed beyond the
  intended pilot adults;
- the API has no persistent directory for encrypted image retry data;
- a connector rehearsal would require real family data before synthetic proof passes.

The clean baseline intentionally does not translate the former Florence schema. Never point it at an old database and hope table names are compatible.

## Topology

- Config-as-code: `/railway.json`.
- Start: `node apps/api/dist/server.js`.
- Predeploy: `node packages/database/dist/predeploy.js`.
- Health: `GET /api/health`.
- One replica for the pilot.
- One attached persistent directory for `FLORENCE_IMAGE_VAULT_DIRECTORY`.
- PostgreSQL is durable product truth. The image vault holds encrypted short-lived attachment bytes only.

Delete or permanently scale down the former worker service before enabling autodeploy. It has no artifact or start command in this product.

## Environment contract

Required:

| Variable | Contract |
| --- | --- |
| `NODE_ENV` | `production` |
| `FLORENCE_DATABASE_URL` | Private URL for the approved fresh PostgreSQL service |
| `FLORENCE_SESSION_SECRET` | At least 32 random bytes |
| `FLORENCE_ENROLLMENT_SECRET` | At least 32 random bytes |
| `FLORENCE_IMAGE_VAULT_DIRECTORY` | Absolute path on the attached persistent volume |
| `FLORENCE_IMAGE_VAULT_KEY` | Canonical base64 encoding of exactly 32 random bytes |
| `FLORENCE_MESSAGES_URL` | Canonical link that returns the adult to Florence's iMessage thread |
| `LINQ_API_KEY` | Linq partner API key |
| `LINQ_WEBHOOK_SECRET` | Current Standard Webhooks signing secret |
| `LINQ_PARTNER_ID` | Exact expected Linq partner UUID |
| `LINQ_FROM_PHONE` | Florence's exact E.164 iMessage sender number |
| `OPENAI_API_KEY` | Model credential |
| `FLORENCE_OPENAI_MODEL` | Pinned supported model ID |
| `GOOGLE_OAUTH_CLIENT_ID` | Google OAuth web client ID |
| `GOOGLE_OAUTH_CLIENT_SECRET` | Google OAuth web client secret |
| `GOOGLE_OAUTH_REDIRECT_URI` | Exact `<origin>/oauth/google/callback`; its origin is also used for setup links |
| `GOOGLE_CREDENTIAL_KEY` | Canonical base64 encoding of exactly 32 random bytes |

Optional bounded settings:

- `FLORENCE_MODEL_TIMEOUT_MS=30000`
- `FLORENCE_MODEL_MAX_OUTPUT_TOKENS=4000`
- `LOG_LEVEL=info`

Railway owns `PORT`. Never print resolved variables or place them in shell arguments, fixtures, logs, or model context.

## Provider registration

Register exactly:

- Linq webhook: `<origin>/api/v1/webhooks/linq?version=2026-02-03`.
- Google OAuth redirect: `<origin>/oauth/google/callback`.

The Linq API key and webhook signing secret are different credentials. Florence verifies the raw webhook signature, pins the payload version, re-reads the live chat, and requires iMessage plus the exact current participant set before retaining content.

Google currently uses `openid email gmail.readonly calendar.events.owned`. Gmail remains private to its owning adult. Calendar writes require a current exact instruction or approval and provider reread proof.

## Build gates

From a clean checkout on Node 24:

```bash
corepack enable
pnpm install --frozen-lockfile
pnpm check
TEST_DATABASE_URL=postgres://... pnpm --filter @florence/api test
docker build --tag florence:release .
docker run --rm --entrypoint node florence:release --version
```

On a disposable empty database, run `pnpm db:migrate` twice. Both runs must pass, and `florence_schema_migrations` must contain the exact digest for `001_florence`.

## Clean pilot cutover

1. Disable Railway autodeploy.
2. Record the exact old API deployment, database, and volume IDs; take and verify a restorable backup.
3. Stop the old API and worker and confirm no running instance can write.
4. Create or explicitly empty the approved pilot PostgreSQL database. Verify it
   contains no household or adult. Do not mix the new baseline with the old
   schema.
5. Configure the variables and callbacks without starting a deployment.
6. Attach the persistent image-vault directory to the API service.
7. Delete or leave permanently stopped the obsolete worker service.
8. Deploy the exact release commit through `/railway.json`.
9. Wait for `/api/health`, then run `pnpm smoke:production -- https://<canonical-host>`.
10. Before sharing the number more broadly, have the intended founding adult
    text Florence, complete the fragment-link mobile setup first, connect
    Google, add the optional partner and child/school/activity basics one screen
    at a time, and return to the exact private thread. No identity is configured
    ahead of that message, no household name is requested, and the first valid
    setup redemption wins. Then
    rehearse the signed PDF conversation through the Phase 1 stop. After that
    experience is approved, rehearse the second adult's independent onboarding,
    exact family group, sourced memory, useful follow-up, one approved Calendar
    write, provider proof, correction, and Vault deletion.
11. Enable autodeploy only after the full synthetic journey and the two-phone experience pass.

## Recovery

- Keep the pre-reset database backup until the pilot has completed the full journey and the user explicitly authorizes disposal.
- Roll back code only when it understands the current baseline; otherwise stop the API and restore the matching database backup.
- Treat an uncertain Calendar write as pending reconciliation, never as success.
- Rotate one connector credential at a time and repeat that connector's synthetic rehearsal.
- Keep request bodies, cookies, authorization headers, provider payloads, raw family content, and exception objects out of logs.

Production mutation remains an explicit operator action. Passing local gates does not authorize a deploy or database reset.
