# Florence

Florence is an iMessage-first family Chief of Staff. It learns the family context adults explicitly
authorize, notices concrete open outcomes, asks for ownership, follows up neutrally, and closes the loop
in the conversations the family already uses.

The mobile web app is Florence's setup and authority surface: family members, children, schools and
grades, Linq enrollment, Google connections, privacy, and exceptions. It is not a second chat or a
generic task dashboard.

## Repository shape

- `apps/api`: authenticated dashboard API, Linq webhook ingress, Google OAuth, and the built web app.
- `apps/worker`: bounded model deliberation, timers, Gmail observation, Linq delivery, and Calendar writes.
- `apps/web`: mobile onboarding and family settings.
- `packages/control-plane`: the single authoritative `HouseholdChiefOfStaff.accept()` seam.
- `packages/database`: PostgreSQL event spine, effects, timers, encrypted artifacts, and migrations.
- `packages/{runtime,linq,google,artifacts}`: narrow provider boundaries.

PostgreSQL is canonical. Model calls are ephemeral, provider retries are idempotent, private Gmail/image
meaning stays private until an exact promotion, and Calendar writes require an exact private approval plus
a provider reread proof.

## Local development

Requires Node 24 and pnpm 10.10.

```bash
corepack enable
pnpm install --frozen-lockfile
cp .env.example .env
docker compose up -d postgres
pnpm db:migrate
pnpm dev
```

The API serves at `http://localhost:3000`; Vite runs the web app at `http://localhost:5173` in development.
Secrets belong only in ignored local configuration or Railway variables.

## Verification

```bash
pnpm lint
pnpm typecheck
pnpm test
pnpm build
docker build --tag florence:local .
```

PostgreSQL integration tests are opt-in through `FLORENCE_TEST_DATABASE_URL` and must target a disposable
database/schema. Production deployment and data cutover remain separate, explicit operations; see
[`docs/operations/production.md`](docs/operations/production.md).
