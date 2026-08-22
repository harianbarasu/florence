# Florence

Florence is a warm family chief of staff who lives primarily in iMessage. The pilot is designed for roughly the first 100 independent households. The default product has two independently verified adults, private conversations with each adult, one exact family group, connected Google accounts, a Florence-created shared family calendar, and a small web workspace for trusted setup and inspectable family knowledge. A quiet solo escape hatch is intentionally limited until a partner joins.

The product benchmark is Instinct's calm, sparse web experience plus Poke's native conversational feel. Florence should be useful, selective, and natural—not a workflow system presented as a chat interface.

The implementation deliberately runs as one API process over one direct PostgreSQL product store. Linq, Google, OpenAI, and encrypted attachment storage remain boundaries because they protect provider authority, irreversible actions, or family data; there is no generic workflow engine, worker service, connector framework, or shadow inbox.

## Product contract

- [Pilot plan](./PLAN.md)

## Run locally

Use Node.js 24 and pnpm 10. Copy `.env.example` to `.env`, provide the credentials needed for the providers you are exercising, and point `FLORENCE_DATABASE_URL` at a development PostgreSQL database.

```bash
pnpm install
pnpm db:migrate
pnpm dev
```

The API defaults to `http://127.0.0.1:8787`; the web app defaults to [http://127.0.0.1:5173](http://127.0.0.1:5173).

## Verify

```bash
pnpm check
```

The real release gate is the complete two-adult household rehearsal in [PLAN.md](./PLAN.md), using real phones, Google accounts, providers, and a production-shaped deployment.
