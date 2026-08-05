# Florence

Florence is an iMessage-first family Chief of Staff. It turns family messages, school email, screenshots, PDFs, and calendar changes into privately governed, owned, timed, and completed household work.

The accepted product and architecture are documented in [PLAN.md](./PLAN.md). The completion contract is in [GOAL.md](./GOAL.md).

## Development

Requires Node 24.19 and pnpm 10.10.

```bash
pnpm install
cp .env.example .env
pnpm db:migrate
pnpm dev
pnpm dev:worker
```

Run the release gate with:

```bash
pnpm check
```

Never commit `.env` or place family content or credentials in logs.
