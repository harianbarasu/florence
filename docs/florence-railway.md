# Florence on Railway

This repo now includes first-party Railway deployment support for the Florence
server and worker.

## Runtime Shape

- Railway runs the Docker image from `Dockerfile`
- The web service runs `florence-server`
- A separate worker service should run `florence-worker`
- An optional separate Next web app can run from `web/` for onboarding/control-plane
- Health check: `/health`
- Web onboarding: `/v1/florence/onboarding`
- Google callback: `/v1/florence/google/callback`
- Linq webhook: `/v1/channels/linq/webhook`
- Add a Redis service for the durable Google sync queue

Florence can now run on Postgres. On Railway, Postgres is the preferred setup.

## Railway Setup

1. Create a new Railway web service from this repo.
2. Duplicate it as a worker service using the same repo/image.
3. Set the worker start command to `florence-worker`.
4. Add a Railway Postgres service.
5. Add a Railway Redis service.
6. Set `DATABASE_URL` or `FLORENCE_DATABASE_URL` on both Florence services from Postgres.
7. Set `REDIS_URL` or `FLORENCE_REDIS_URL` on both Florence services from Redis.
8. Expose the generated Railway domain or attach your own custom domain on the web service.

`railway.json` already tells Railway to use the Dockerfile. Set service-specific
healthchecks in Railway itself:

- `florence-api`: `/health`
- `florence-worker`: no healthcheck path

## Required Environment

Set these in the Railway service variables:

```bash
# Florence storage
HERMES_HOME=/data/hermes-home
DATABASE_URL=postgresql://...
REDIS_URL=redis://...

# Google OAuth
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
GOOGLE_OAUTH_STATE_SECRET=...

# Linq
LINQ_API_KEY=...
LINQ_WEBHOOK_SECRET=choose-a-long-random-secret

# Florence runtime
FLORENCE_SYNC_INTERVAL_SECONDS=300
FLORENCE_HERMES_PROVIDER=custom
FLORENCE_HERMES_MODEL=gpt-5.4
FLORENCE_ONBOARDING_STATE_SECRET=...
FLORENCE_WEB_BASE_URL=https://florence-web.up.railway.app

# Hermes model provider credentials for direct OpenAI
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_API_KEY=...
```

Notes:

- Railway injects `PORT` automatically. Florence now honors `PORT` directly.
- If you use the Railway public domain, Florence now derives its public base URL
  automatically from `RAILWAY_PUBLIC_DOMAIN`.
- If you use a custom domain, set `FLORENCE_PUBLIC_BASE_URL` explicitly.
- If you deploy the separate Next onboarding app, set `FLORENCE_WEB_BASE_URL` on the
  Python service so DM onboarding links and post-Google redirects land in the Next
  `Setup` flow instead of the legacy server-rendered page.
- `FLORENCE_ONBOARDING_STATE_SECRET` can reuse the same long random secret as
  `GOOGLE_OAUTH_STATE_SECRET`, but keeping it explicit is cleaner.
- If you want Claude instead, set `FLORENCE_HERMES_PROVIDER=anthropic`,
  `FLORENCE_HERMES_MODEL=anthropic/claude-opus-4.6`, and `ANTHROPIC_API_KEY`.
- If you want OpenRouter instead, set `FLORENCE_HERMES_PROVIDER=openrouter`,
  use an OpenRouter model slug, and provide `OPENROUTER_API_KEY`.

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

1. Set the `florence-api` service healthcheck path to `/health`.
2. Leave the `florence-worker` service with no healthcheck path because it is a long-running background process, not an HTTP server.
3. Open `https://<your-api-domain>/health` and confirm `{"ok": true}`.
4. Send a Linq test webhook.
5. Start a Florence DM onboarding thread and confirm Florence replies with the desktop onboarding URL.
6. Open the onboarding URL, complete setup, and confirm Google connect returns back into the onboarding page.
7. Confirm both the web service and worker can read and write Postgres and Redis.

## Operational Notes

- The worker owns scheduled sync plus the Redis-backed Google sync queue.
- Keep only one worker replica for now.
- Postgres removes the main SQLite single-replica limitation, but for the demo
  I would still keep Florence at one web replica and one worker replica until
  the Linq flow is stable.
