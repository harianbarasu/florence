# Florence on Railway

This repo now includes first-party Railway deployment support for the Florence
server and worker.

## Runtime Shape

- Railway runs the Docker image from `Dockerfile`
- The web service runs `florence-server`
- A separate worker service should run `florence-worker`
- Health check: `/health`
- Google callback: `/v1/florence/google/callback`
- Linq webhook: `/v1/channels/linq/webhook`

Florence can now run on Postgres. On Railway, Postgres is the preferred setup.

## Railway Setup

1. Create a new Railway web service from this repo.
2. Duplicate it as a worker service using the same repo/image.
3. Set the worker start command to `florence-worker`.
4. Add a Railway Postgres service.
5. Set `DATABASE_URL` or `FLORENCE_DATABASE_URL` on both Florence services from that Postgres instance.
6. Expose the generated Railway domain or attach your own custom domain on the web service.

`railway.json` already tells Railway to use the Dockerfile and health check
`/health`.

## Required Environment

Set these in the Railway service variables:

```bash
# Florence storage
HERMES_HOME=/data/hermes-home
DATABASE_URL=postgresql://...

# Google OAuth
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
GOOGLE_OAUTH_STATE_SECRET=...

# Linq
LINQ_API_KEY=...
LINQ_WEBHOOK_SECRET=choose-a-long-random-secret

# Florence runtime
FLORENCE_SYNC_INTERVAL_SECONDS=300
FLORENCE_HERMES_MODEL=anthropic/claude-opus-4.6

# Hermes model provider credentials
OPENROUTER_API_KEY=...
# Or set the equivalent key for whichever Hermes-supported provider you use.
```

Notes:

- Railway injects `PORT` automatically. Florence now honors `PORT` directly.
- If you use the Railway public domain, Florence now derives its public base URL
  automatically from `RAILWAY_PUBLIC_DOMAIN`.
- If you use a custom domain, set `FLORENCE_PUBLIC_BASE_URL` explicitly.

## Google OAuth

In your Google OAuth app, add this redirect URI:

```text
https://<your-domain>/v1/florence/google/callback
```

Examples:

```text
https://florence-production.up.railway.app/v1/florence/google/callback
https://florence.yourdomain.com/v1/florence/google/callback
```

## Linq Webhook

Configure Linq to post webhooks to:

```text
https://<your-domain>/v1/channels/linq/webhook?version=2026-02-03
```

Example:

```text
https://florence-production.up.railway.app/v1/channels/linq/webhook?version=2026-02-03
```

Linq signs webhooks with `X-Webhook-Signature` and `X-Webhook-Timestamp`.
Florence now verifies those using `LINQ_WEBHOOK_SECRET`.

## Linq Reachability

Railway only needs to accept incoming Linq webhooks. Florence sends outbound
messages directly to the Linq API over HTTPS using `LINQ_API_KEY`.

## Deploy and Verify

After the first deploy:

1. Open `https://<your-domain>/health` and confirm `{"ok": true}`.
2. Send a Linq test webhook.
3. Start a Florence DM onboarding thread.
4. Complete Google connect and confirm the callback returns a success page.
5. Confirm both the web service and worker can read and write the Railway Postgres database.

## Operational Notes

- The worker owns background Google sync. Keep only one worker replica for now.
- Postgres removes the main SQLite single-replica limitation, but for the demo
  I would still keep Florence at one web replica and one worker replica until
  the Linq flow is stable.
