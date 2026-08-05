# Florence

Florence is an iMessage-first family Chief of Staff. It turns family messages, school email,
calendar changes, and explicit requests into privately governed, owned, timed, and completed
household work.

The production shape is deliberately small:

- a Fastify interface for Linq and Gmail webhooks, Google OAuth, legal pages, health, and the
  authenticated operator surface;
- durable background loops for inbox processing, Gmail synchronization, timers, agent work, and
  outbox effects, either colocated for the first private pilot or isolated in a worker service;
- one PostgreSQL database as the authoritative system of record.

The accepted product and architecture are in [PLAN.md](./PLAN.md), the completion contract is in
[GOAL.md](./GOAL.md), and the production setup and morning test are in
[docs/DEPLOYMENT.md](./docs/DEPLOYMENT.md).

## Local development

Requirements: Node 24.19, pnpm 10.10, and PostgreSQL 18 (or Docker).

```bash
corepack enable
pnpm install --frozen-lockfile
cp .env.example .env
docker compose up -d postgres
```

Fill `.env` with development credentials. Generate each local application secret independently;
for example:

```bash
node -e 'process.stdout.write(require("node:crypto").randomBytes(32).toString("base64url") + "\n")'
```

After every required value is present, migrate the local database:

```bash
pnpm db:migrate
```

For the simplest local setup, run the combined process:

```bash
pnpm dev
```

To exercise the split topology, set `FLORENCE_PROCESS_ROLE=web` for `pnpm dev` and
`FLORENCE_PROCESS_ROLE=worker` for `pnpm dev:worker` in separate terminals. Exactly one running
process may own background work.

Run the release gate before every deploy:

```bash
pnpm check
docker build --tag florence:local .
```

Never commit `.env`, provider credentials, family content, OAuth grants, or production payloads.
Florence intentionally redacts request bodies and credentials from logs.

## Deployment

The first private-pilot Railway deployment can use one service:

- combined web and background process: `/railway.json` with `FLORENCE_PROCESS_ROLE=all`.

When separate failure and scaling boundaries are useful, use the same image for two services:

- web: `/railway.json` with `FLORENCE_PROCESS_ROLE=web`
- worker: `/railway.worker.json` with `FLORENCE_PROCESS_ROLE=worker`

Never run an `all` service beside a worker service; that would create two background owners. Both
configurations execute the transaction- and advisory-lock-protected database migrator as a
pre-deploy command. Only a service running the web interface receives a public domain. Follow the
full [deployment runbook](./docs/DEPLOYMENT.md); do not deploy from this short overview alone.
