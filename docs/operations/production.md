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
- Start: `node apps/api/dist/start.js`.
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
| `FLORENCE_RUNTIME_MODE` | `api` normally; `production_reset_maintenance` only during the guarded reset procedure below |
| `FLORENCE_PRODUCTION_RESET_EXPECTED_RAILWAY_PROJECT_ID` | Exact Railway project ID recorded by the operator; required by both guarded reset modes |
| `FLORENCE_PRODUCTION_RESET_EXPECTED_RAILWAY_ENVIRONMENT_ID` | Exact Railway production-environment ID recorded by the operator; required by both guarded reset modes |
| `FLORENCE_PRODUCTION_RESET_EXPECTED_RAILWAY_SERVICE_ID` | Exact Railway API-service ID recorded by the operator; required by both guarded reset modes |
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

Optional browser provider:

| Variable | Contract |
| --- | --- |
| `BROWSERBASE_API_KEY` | Enables Florence's authenticated browser work when non-empty; omit to start without browser tooling |
| `BROWSERBASE_PROJECT_ID` | Optional Browserbase project ID used when creating and releasing sessions |

Optional phone providers:

| Variable | Contract |
| --- | --- |
| `BLAND_API_KEY` | Enables conversational outbound task calls when non-empty |
| `BLAND_DEFAULT_VOICE` | Optional Bland voice ID; defaults to `mason` |
| `TWILIO_ACCOUNT_SID` | Enables SMS/MMS, inbox polling, and one-way calls when all three Twilio values are present |
| `TWILIO_AUTH_TOKEN` | Twilio account credential |
| `TWILIO_PHONE_NUMBER` | Florence's exact E.164 Twilio sender/caller number |

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

Google currently uses `openid`, `email`, `gmail.modify`, `drive`, `tasks`,
`contacts`, `calendar.events.owned`, `calendar.events.readonly`,
`calendar.app.created`, `calendar.acls`, and `calendar.calendarlist`.
Ordinary Gmail conversation remains private to its owning adult; explicit durable
work started in the family group uses the initiating parent's Google connection.
Calendar writes require a current exact instruction or approval and provider
readback before Florence confirms the change.

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

## Guarded production reset

Never empty the production database with raw SQL. A database-only reset strands the
shared Calendars Florence created in the adults' Google accounts and destroys the
credentials needed to remove them safely. It also leaves encrypted image artifacts on
the attached persistent volume.

The reset must run inside a maintenance deployment of the existing Railway `api`
service. `railway run` and `railway shell` execute locally: they do not mount `/data`
or join Railway's private network. A pre-deploy command has the private network but no
volume. Do not expose PostgreSQL publicly or copy the vault locally as a workaround.

First disable autodeploy and record the exact clean release commit plus Railway project,
production environment, API service, deployment, database, and volume IDs. Configure
the three `FLORENCE_PRODUCTION_RESET_EXPECTED_RAILWAY_*_ID` variables from those
recorded project, environment, and service IDs before starting maintenance. Do not
derive the expected values from the runtime `RAILWAY_*` variables inside the reset
command. If the service has a connected source, disconnect it for the maintenance
window and record the source so it can be restored afterward:

```bash
railway service source disconnect --service api --environment production
git status --short
git rev-parse HEAD
```

`git status --short` must print nothing. Scale the API to zero and inspect the result.
Do not proceed while any deployment instance is `RUNNING`:

```bash
railway scale --service api --environment production us-west2=0
railway service status --service api --environment production --json
```

Stage maintenance mode without allowing the variable change to deploy the old API,
then deploy the exact same clean release. `/railway.json` restores the one replica. The
maintenance runtime starts only an inert `/api/health` endpoint; every product route,
webhook, timer, and Florence dependency remains unavailable.

```bash
railway variable set --service api --environment production --skip-deploys \
  FLORENCE_RUNTIME_MODE=production_reset_maintenance
railway up --service api --environment production --detach --json \
  --message "Enter Florence production reset maintenance"
railway service status --service api --environment production --json
curl --fail --silent --show-error --include \
  https://<canonical-host>/api/health
```

Wait for exactly one `RUNNING` maintenance instance. The health response must include
`x-florence-runtime-mode: production_reset_maintenance` and this exact JSON shape:

```json
{"status":"maintenance","service":"florence-production-reset-maintenance","mode":"production_reset_maintenance"}
```

Run the dry run over Railway SSH so the process has both the private PostgreSQL URL and
the existing mounted volume:

```bash
railway ssh --service api --environment production -- \
  node apps/api/dist/reset-production.js --dry-run
```

The dry run emits only aggregate counts and a 64-character `snapshotGuard`; it never
prints a Calendar ID, artifact filename, credential, email address, or household
content. The guard covers both the exact database snapshot and the exact inventory of
canonical encrypted `.fiv` image artifacts plus exact Florence atomic-write temporary
envelopes named `.<asset UUID>.<write UUID>.tmp` in the asset's two-hex shard. The reset
ignores shard directories, arbitrary `.tmp` files, noncanonical filenames, and
unrelated volume contents. `encryptedImageArtifacts` and
`encryptedImageTemporaryArtifacts` report those two guarded inventories separately. It
uses the founding adult's still-active Google credential to read every database-linked
Family Calendar. It also includes every household where Calendar creation was durably
attempted but the returned Calendar ID was never stored. For those ambiguous creates,
it walks the complete Google Calendar list. Exactly one secondary Calendar with the
deterministic Florence household marker is recovered; zero exact matches is the
idempotent absent result for a create that never reached Google or a Calendar already
removed manually. Multiple matches, malformed results, incomplete pagination, a
missing creator credential, missing retained pre-attempt Google-account lineage,
marker mismatch, primary Calendar, arbitrary Calendar, or unconfirmed provider read
blocks the reset. A same-account reconnect is valid only when the active founding
credential's subject digest matches a retained founder connection created no later
than the Calendar creation attempt. Deleting Google-derived data erases that proof, so
a later account connection cannot authorize Calendar absence or deletion.
The founding account must be Google's single `dataOwner`; an `owner` sharing role is
not sufficient deletion authority.

Review the counts and keep the service in maintenance mode. Run the destructive
command over SSH with the exact guard returned by the dry run:

```bash
railway ssh --service api --environment production -- \
  node apps/api/dist/reset-production.js \
  --confirm-production-reset "RESET FLORENCE PRODUCTION" \
  --snapshot <snapshotGuard> \
  --api-stopped
```

The CLI independently requires Railway project, environment, service, deployment,
and replica identity variables to be present and UUID-shaped. In both dry-run and
execute modes, it also compares the runtime project, environment, and service IDs
exactly against the three operator-configured
`FLORENCE_PRODUCTION_RESET_EXPECTED_RAILWAY_*_ID` values. Missing, malformed, or
mismatched identities fail closed in one aggregate identity-check error without
printing either the actual or expected IDs. The CLI also requires an existing
configured vault directory inside `RAILWAY_VOLUME_MOUNT_PATH` and the exact localhost
maintenance health signature. `--api-stopped` is an explicit operator confirmation,
not the only exclusion control.

The command repeats the provider preflight. Before deleting an exactly discovered
ambiguous Calendar, it locks and rechecks the complete inspected database snapshot and
durably records that provider ID. That makes a partially completed reset safe to rerun.
It then permanently removes each marked Calendar with Google `Calendars.delete`,
accepts `404`/`410` as an idempotent already-absent result, and confirms every Calendar
is absent. If Google's Calendar metadata cache temporarily survives the deletion, the
exact Florence marker and current data-owner identity plus absence from the data owner's
CalendarList is also conclusive because Google does not allow a data owner to remove an
active owned Calendar from that list. It rechecks the inspected image inventory and unlinks only those canonical
`.fiv` files and exact Florence atomic-write temporary envelopes, then confirms neither
kind of canonical image artifact remains. The completion event reports separate
`encryptedImageArtifactsDeleted` and `encryptedImageTemporaryArtifactsDeleted` counts.
Finally it prepares revocation of each active Google credential, locks and rechecks the
resulting exact database snapshot, truncates only household product data, and attempts
the revocations. Migration history, the installed baseline, vault directories,
arbitrary or noncanonical temporary files, and unrelated volume contents remain
intact. An unconfirmed provider revocation is reported as an aggregate count; the
destroyed credential envelopes still remove all Florence access.

If any step fails, do not manually empty the database or restart the API. The command
may already have removed some Calendars or image artifacts, and a fresh dry run plus
rerun reconciles both as absent. A snapshot mismatch means database or canonical vault
state changed after inspection: stop that writer, take a new dry-run snapshot, and
review it again. Calendars orphaned by older database-only resets have no trustworthy
database provenance and require one-time manual review; this command deliberately will
not discover or guess at them. Never substitute `CalendarList.delete`, which only
unsubscribes the current account rather than permanently deleting the Calendar.

Only after a successful reset, stage normal API mode without deploying it separately,
deploy the same recorded clean commit, wait for the normal health payload, and run the
production smoke test:

```bash
railway variable set --service api --environment production --skip-deploys \
  FLORENCE_RUNTIME_MODE=api
git status --short
git rev-parse HEAD
railway up --service api --environment production --detach --json \
  --message "Leave Florence production reset maintenance"
railway service status --service api --environment production --json
pnpm smoke:production -- https://<canonical-host>
```

Again, `git status --short` must be empty and `git rev-parse HEAD` must equal the commit
recorded before maintenance. Restore the recorded source/autodeploy configuration only
after the smoke test passes. If the reset failed, leave maintenance mode running and
follow the reconciliation instructions above instead of starting the API.

## Clean pilot cutover

1. Disable Railway autodeploy.
2. Record the exact old API deployment, database, and volume IDs; take and verify a restorable backup.
3. Stop the API and confirm no running instance can write.
4. For a new pilot, create the approved PostgreSQL database. For an existing pilot,
   use the guarded production reset above while Google credentials still exist.
   Verify the result contains no household or adult. Do not mix the new baseline
   with the old schema.
5. Configure the variables and callbacks without starting a deployment.
6. Attach the persistent image-vault directory to the API service.
7. Deploy the exact release commit through `/railway.json`.
8. Wait for `/api/health`, then run `pnpm smoke:production -- https://<canonical-host>`.
9. Before sharing the number more broadly, have the intended founding adult
    text Florence, complete the fragment-link mobile setup, connect Google, add
    the partner and child age/grade/school/activity basics one screen at a time, and
    return to the exact private thread. No identity is configured ahead of that
    message, no household name is requested, and the first valid redemption for
    that Messages identity creates its household. Rehearse Florence asking once
    before texting the partner, the second adult's independent onboarding and
    Google connection, automatic exact family group and shared Calendar, the
    combined briefing, a real forwarded message/image/PDF/voice note, sourced
    memory, one finite monitor, one relevant interest recommendation, one
    family-Calendar create/update/delete, correction, Vault deletion, the
    read-only web Calendar, and a Google disconnect/delete/reconnect that
    suppresses queued Google-derived output while preserving sent Messages and
    provider-created family Calendar events.
10. Enable autodeploy only after the full synthetic journey and the two-phone experience pass.

## Recovery

- Keep the pre-reset database backup until the pilot has completed the full journey and the user explicitly authorizes disposal.
- Roll back code only when it understands the current baseline; otherwise stop the API and restore the matching database backup.
- Treat an uncertain Calendar write as pending reconciliation, never as success.
- Rotate one connector credential at a time and repeat that connector's synthetic rehearsal.
- Keep request bodies, cookies, authorization headers, provider payloads, raw family content, and exception objects out of logs.

Production mutation remains an explicit operator action. Passing local gates does not authorize a deploy or database reset.
