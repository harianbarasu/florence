# Florence

**The family assistant that lives in your texts.**

Florence carries the invisible load of running a family. She sits in the
parents' iMessage thread (a real phone number, via [Linq](https://linqapp.com)),
reads the school-email firehose (Gmail, read-only), remembers everything,
and nudges the right person at the right time — permission slips Thursday
evening, snack duty the night before, the morning brief at 7:15.

Parents don't install anything. They text Florence like a person, alone or in
the family group chat.

## How it works

One small service. One rule: **everything routes through the agent.**

```
iMessage (Linq webhook) ─┐
reminder comes due ──────┤
morning brief time ──────┼──▶  agent turn: full household context + tools  ──▶ texts (or silence)
new email arrives ───────┤        remember/forget · schedule/update reminders
gmail gets connected ────┘        search/read email · calendar · connect link
                                  message other family chats · update members
```

Every trigger builds the same context — family members, saved memories, the
schedule, connected inboxes, and the **exact current local time** — and runs
the same tool loop ([agent.py](florence/agent.py)). The model's final text goes
to the chat; empty output is deliberate silence (crucial in group chats).
There is no intent routing and no canned copy; the only hardcoded reply in the
codebase is the carrier-mandated STOP confirmation.

Proactivity is the same loop: the scheduler fires a due reminder as a turn
("compose this reminder as a natural text, add context"), so even nudges sound
like a person who knows the family, not a cron job.

- **Model-agnostic** — any OpenAI-compatible endpoint
  ([llm.py](florence/llm.py)): OpenAI today, Nous Portal / OpenRouter /
  self-hosted by changing two env vars.
- **Group-chat native** — chats map to households; a known parent texting from
  a new thread (1:1 or group) lands in the same family. Senders are attributed
  by name in context, so Florence knows who said what.
- **PDFs & photos** — iMessage attachments are ingested (PDF text extraction,
  images passed to vision) so "here's the camp form 📎" just works.
- **Memory** — agent-curated durable facts, injected into every turn.
- **Timezone-strict** — all scheduling happens in the household's timezone;
  the agent is always told what time it is *right now*.

## Run it locally

```bash
docker compose up db                 # postgres on :5433
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env                 # add FLORENCE_MODEL_API_KEY at minimum
set -a; source .env; set +a
florence                             # serves :8000, dry-run texting without LINQ_API_KEY
```

Talk to the agent without a phone:

```bash
curl -s localhost:8000/admin/test-turn \
  -H "Authorization: Bearer $FLORENCE_ADMIN_API_KEY" \
  -H 'content-type: application/json' \
  -d '{"message": "hi! im Sarah. remind me tmrw 8am to pack Maya'\''s cleats"}' | jq
```

Tests: `pytest` · Lint: `ruff check florence tests`

## Deploy (Railway)

The Railway project **Florence** runs service `api` (this repo, Dockerfile) +
`Postgres`. The Linq webhook subscription and the Google OAuth redirect both
point at the service's domain, routes `/webhooks/linq` and
`/oauth/google/callback`.

```bash
railway up --service api
```

Required env: `FLORENCE_DATABASE_URL` (with `florence_v3` search_path),
`FLORENCE_WEB_BASE_URL`, `FLORENCE_ADMIN_API_KEY`, `FLORENCE_TOKEN_ENCRYPTION_KEY`,
`LINQ_API_KEY` / `LINQ_WEBHOOK_SECRET` / `LINQ_FROM_PHONE`,
`GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` / `GOOGLE_REDIRECT_URI`,
`FLORENCE_MODEL` + `FLORENCE_MODEL_API_KEY` (+ `FLORENCE_MODEL_BASE_URL` for
non-OpenAI). See [.env.example](.env.example).

## Repo map

| File | What it is |
|---|---|
| [florence/agent.py](florence/agent.py) | The turn loop — the product |
| [florence/prompts.py](florence/prompts.py) | Florence's personality + proactive directives |
| [florence/tools.py](florence/tools.py) | Tool schemas and handlers |
| [florence/runtime.py](florence/runtime.py) | Debounce, scheduler, Gmail sync |
| [florence/app.py](florence/app.py) | Webhook, OAuth, admin (transport only) |
| [florence/linq.py](florence/linq.py) · [gmail.py](florence/gmail.py) · [llm.py](florence/llm.py) | Integrations |
| [florence/store.py](florence/store.py) · [db.py](florence/db.py) | Postgres |

Read [AGENTS.md](AGENTS.md) before changing message handling — it explains the
one architectural rule and why the previous version died without it.
