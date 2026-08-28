Label: wayfinder:map

# Make Florence a general household operator

## Product target

Florence should feel like Instinct for a household: a parent asks naturally, Florence uses the relevant tools, and the work ends in a useful result, one genuinely blocking question, or an honest failure. Search, Gmail, and Calendar are only the first slice.

Use the first-party product benchmark in `docs/research/instinct-product-benchmark.md`, the current Instinct/Poke capability audit in `research/instinct-capability-audit.md`, and the Pi/Hermes source inventory in `docs/research/pi-hermes-assistant-tool-gap-2026-08-27.md`.

Direct upstream reuse remains required where it produces a real Florence capability:

- Pi is pinned at `4e494929998d6bc4fccf75e0a233f727db4b70ee` in `/Users/harianbarasu/Projects/florence-upstreams/pi`.
- Hermes Agent is pinned at `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882` in `/Users/harianbarasu/Projects/florence-upstreams/hermes-agent`.
- Record the exact upstream file, commit, and reuse mode in each resolved implementation ticket.
- Do not import coding, shell, terminal, repository, or arbitrary-filesystem tools.

## Immediate frontier

After conversational Google reads:

1. dedicated maps, places, routes, and time-zone tools;
2. live weather plus flight route, status, and alternative tools;
3. full conversational reminder control;
4. linked public-page and PDF reading;
5. one durable multi-step family task using those tools, including natural acknowledgement, steering, cancellation, meaningful progress, and terminal delivery;
6. a prioritized household docket;
7. Gmail communication and the full useful Google Workspace surface: Drive, Docs, Sheets, Slides, Tasks, and Contacts;
8. authenticated browser/computer use, inbound live voice, outbound calls and texts, bookings, and real errands—not another search result handoff.

Each ticket must name the family behavior it unlocks. Existing household boundaries constrain concrete capabilities; generalized safety/privacy frameworks are not roadmap deliverables. Do not add another registry, runtime, scheduler, queue, process, or policy layer when Florence's existing reasoner, PostgreSQL due-work seam, provider adapters, and Linq delivery can own the behavior.

## Product constraints

- Ordinary parent language is enough. No URL-only gate, phrase allowlist, category-specific planner, or silence for an ordinary message.
- The first review accounts for every retained received Gmail message from the prior 90 days and every readable personal Calendar in the defined review window. It does not dump all of that into chat.
- The parental unit is the knowledge unit for validated family facts. A date found only on one adult's personal Calendar remains private and is offered to that owner before Florence copies or names it in the family Calendar; once intentionally added there, it is household truth.
- Reminders and longer work must keep going after the initial reply and report what actually happened. Reactions and progress should feel natural and correspond to real work.
- Keep the small Pi/Hermes-derived tool execution kernel. Remove speculative registry versions, universal policy records, generic evidence/delivery frameworks, and other infrastructure with no current family behavior.
- Add only focused verification for the concrete capability or regression being changed.

## Decisions so far

- [Make broad assistant work the product contract](issues/01-make-broad-assistant-work-the-product-contract.md) — retained only for the broad operator goal, ordinary-language behavior, direct Pi/Hermes reuse, 90-day review, reminders, durable work, and concrete household boundaries.
- [Port the Pi tool lifecycle into Florence](issues/04-port-the-pi-tool-lifecycle.md) — retain a small typed execution kernel and real lifecycle cues; remove the fake facade, speculative generations, universal policy/evidence envelopes, and framework-only tests.
- [Complete Google reads in conversation](issues/05-complete-google-reads-in-conversation.md) — private Gmail attachment reads, complete Calendar catalog/window reads, and uncapped background 90-day accounting now use Florence's existing Google path without generalized delivery or evidence machinery.
- [Port maps, places, routes, and time zones](issues/07-select-maps-weather-and-travel-providers.md) — eight Hermes-derived tools now use a concrete Nominatim, Overpass, Valhalla, and TimeAPI client through Florence's existing reasoner lifecycle, with no provider framework or new infrastructure.
- [Add live weather and flight disruption help](issues/08-port-maps-weather-and-travel.md) — NOAA/NWS forecasts, observations, and alerts plus Hermes-derived live Kiwi alternatives now turn ordinary weather and flight-number disruption messages into work without a new provider framework.
- [Port complete reminder control](issues/09-port-complete-reminder-control.md) — durable private and household reminders now support ordinary-language create/list/change/pause/resume/run/cancel and local-time recurrence through the existing due-work/outbox path.
- [Keep multi-step family work going](issues/10-port-durable-family-work.md) — one Pi/Hermes-adapted PostgreSQL checkpoint loop now acknowledges, resumes, steers, cancels, reports progress, rejects stale workers, and delivers one receipt-confirmed result without a second runtime or progress framework.
- [Read linked public pages and PDFs](issues/06-port-safe-public-page-reading.md) — one Hermes-adapted concrete reader now follows parent or research links, extracts HTML and PDFs locally, and works in foreground or durable tasks without a browser runtime, provider framework, or generic safety subsystem.

## Fog

- Instinct does not publish its tool inventory or execution architecture, so Florence competes on demonstrated user outcomes rather than guessed internals.
- Provider selection for weather and flights belongs inside those concrete capability tickets, not in a standalone architecture phase.
- Purchases, bookings, calls, and long-tail browser work should be added one real family workflow at a time after durable work is functioning; they are core product breadth, not an optional connector catalog.
