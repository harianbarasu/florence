# Florence Agent Guide

Florence is a family assistant that lives in the family's iMessage threads
(via Linq). The architecture has exactly one load-bearing rule:

**Everything routes through the agent.** Every inbound text, due reminder,
morning brief, and new-email batch becomes an agent turn (`agent.run_turn`)
with full household context and real tools. There is no intent classification,
no keyword routing, and no canned reply text anywhere except the legally
required STOP/START handling in `app.py`.

The previous version of this product died by a thousand regex gates in front
of the model. Do not reintroduce them:

- Never add a keyword/regex branch that answers a user before the agent runs.
- Never add templated reply strings. If Florence should say something new,
  give the agent context (prompts.py) or a tool (tools.py), not a string.
- New capability = new tool in `tools.py` + a line in the system prompt if
  needed. New proactive behavior = a new directive in `prompts.py` fired from
  `runtime.py`.

Module map:
- `app.py` — HTTP only: webhook verify/parse/persist, OAuth, admin. No logic.
- `agent.py` — the turn loop. Final model text is sent; empty text = silence.
- `tools.py` — tool schemas + handlers. Tools return `{"error": ...}` to the
  model instead of raising, so it can self-correct.
- `prompts.py` — Florence's personality and all proactive-trigger directives.
- `runtime.py` — debouncer, scheduler, Gmail sync. Proactivity = more turns.
- `store.py` / `db.py` — typed queries / schema. No business logic.
- `linq.py`, `gmail.py`, `llm.py` — transports. The LLM client is
  provider-agnostic (any OpenAI-compatible endpoint).

Run tests with `pytest`. Lint with `ruff check florence tests`.
Local stack: `docker compose up db` then `florence` (or `docker compose up`).
Deploy: `railway up` (service `api` in the Florence project).
