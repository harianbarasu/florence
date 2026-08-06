# Florence

Florence is an iMessage-first family Chief of Staff. It watches authorized family sources, opens
coverage loops, and coordinates explicit ownership in the group chats families already use.

The canonical product and engineering contract is [`PLAN.md`](PLAN.md). The research transfer is in
[`docs/research/florence-architecture-transfer-from-session-019fcdde-2026-08-05.md`](docs/research/florence-architecture-transfer-from-session-019fcdde-2026-08-05.md).

## Local development

```bash
corepack enable
pnpm install
cp .env.example .env
docker compose up -d postgres
pnpm db:migrate
pnpm dev:server
```

Fill the required connector, model, and encryption values in the ignored `.env` before migrating. Run
`pnpm dev:web` and `pnpm dev:worker` in separate terminals.

## Verification

```bash
pnpm lint
pnpm typecheck
pnpm test
pnpm build
```

Secrets belong only in `.env` or Railway variables. Never commit them.

## Production

The exact two-service Railway topology, environment contract, connector setup, deployment checks, and recovery
procedure are in [`docs/operations/production.md`](docs/operations/production.md). Do not deploy while any
release gate in that runbook is open.
