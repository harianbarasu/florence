# Instinct and Poke capability audit for Florence

**Researched:** 2026-08-28
**Evidence standard:** First-party public material only: Instinct's official site; Poke's official site, documentation, recipe directory, npm package, and verified GitHub organization. Poke's directory mixes Poke Team and community recipes, so the report calls an item “core” only when Poke's own product/docs claim it and treats other directory entries as ecosystem evidence.
**Out of scope:** Generic safety/privacy architecture and coding-agent features. This is a user-visible family-assistant capability benchmark.

## Executive conclusion

**Observed:** Instinct describes an assistant that understands the user's work and priorities, connects to email, messaging, screen, audio, location, and other applications/devices, can be texted or called, and is trained to use a phone and computer. Its concrete examples are following up on dropped threads, proactively calling or texting, arranging an airport ride, and booking a handyman. [Instinct](https://instinct.co/)

**Observed:** Poke turns a similar aspiration into a shipping product surface: Apple Messages, Telegram, WhatsApp, and RCS; native email/calendar/reminders/web search; voice input; proactive background automations; memory-backed suggestions; reusable recipes; custom MCP tools; programmatic event ingress; and a human operator for real-world tasks. [Poke overview](https://poke.com/) · [Poke docs](https://poke.com/docs) · [Poke API](https://poke.com/docs/api) · [Poke Human](https://poke.com/faq#poke-human)

**Inference for Florence:** Competing here does not mean “add Google Search.” It means Florence accepts an outcome, finds and uses the appropriate sources/tools, keeps the work alive across time, communicates that it is working, and returns a verified result or a specific blocker. That inference follows from Instinct's phone/computer action and dropped-thread follow-through, plus Poke's cross-tool, background, and real-world execution model. [Instinct](https://instinct.co/) · [Poke overview](https://poke.com/) · [Poke API](https://poke.com/docs/api)

Florence's opportunity to exceed both is the **parental unit**: neither public benchmark establishes a first-class, two-adult household model. Florence should reconcile both parents' sources into one household action picture while preserving which person owns or can see each item. This is a product inference from the benchmarks' explicitly personal/account-scoped models, not a claimed Instinct/Poke feature. [Instinct](https://instinct.co/) · [Poke account merging](https://poke.com/docs/merging-accounts)

## What the benchmarks visibly do

| Capability | Observed first-party evidence | Competitive implication for Florence |
| --- | --- | --- |
| No-new-app interface | Instinct says there are “no new interfaces” and accepts texts or calls; Poke lives in Apple Messages, Telegram, WhatsApp, and RCS. [Instinct](https://instinct.co/) · [Poke docs](https://poke.com/docs) | Florence should feel like a capable contact inside the family thread and private threads, not a dashboard that happens to send alerts. |
| Broad ambient context | Instinct explicitly names email, messaging, screen, audio, and location, and says its model understands what the user is working on and what matters. [Instinct](https://instinct.co/) | Email/calendar context is only the starting point; screen/audio/location and connected-service context are part of the competitive bar. |
| Core life-admin actions | Poke can read/search/draft email, schedule meetings/check availability, manage reminders, and search the web. Outlook support adds real-time inbox sync, attachments, calendar CRUD, and contact search. [Poke docs](https://poke.com/docs) · [Poke release notes](https://poke.com/docs/release-notes#december-12-2025) | Florence needs both retrieval and mutation: answer, draft/send, create/update, mark handled, and verify the result. |
| Cross-source briefing | Poke Team's Morning Briefing combines calendar, top emails, and priorities; Add Deadlines converts email obligations into calendar items. [Morning Briefing](https://poke.com/recipes/morning-briefing) · [Add Deadlines](https://poke.com/recipes/add-deadlines-to-calendar) | “What's on the docket?” should be a ranked action view across sources, not a calendar recital or raw scan dump. |
| Proactive/background work | Poke says it uses integrated services and memory automatically “right on time”; Pro includes real-time automations in the background. [Poke overview](https://poke.com/) · [Poke pricing](https://poke.com/pricing) | Durable work, monitors, and follow-through are first-class product behavior, not exceptional flows. |
| Open-ended tool use | Poke discovers and calls tools from any connected MCP server; its recipes bundle onboarding context, initial behavior, and required integrations. [MCP docs](https://poke.com/docs/mcp-servers) · [Recipe docs](https://poke.com/docs/creating-recipes) | Tool selection should be based on the task, not hard-coded trigger phrases such as requiring a pasted URL before research. |
| Event-driven delegation | Poke's API accepts messages from webhooks, scripts, browser/desktop tools, monitoring, and scheduled systems, then gives the assistant full access to email, calendar, reminders, and integrations. [Poke API](https://poke.com/docs/api) | Florence should accept inbound events and turn them into the same durable work objects as a parent's text. |
| Real-world completion | Instinct cites rides and handyman bookings; Poke Human handles phone calls, reservations, orders, and rides through a human operator. [Instinct](https://instinct.co/) · [Poke Human FAQ](https://poke.com/faq#poke-human) | Some household tasks end outside an API. A future phone/computer or human handoff is part of an Instinct-class ceiling. |
| Multimodal work | Poke accepts text or voice, can extract/visualize data from images such as spreadsheets, and can preview/update image and PDF attachments before sending. [Poke overview](https://poke.com/) · [Poke release notes](https://poke.com/docs/release-notes#february-2-2026) · [Poke release notes](https://poke.com/docs/release-notes#december-1-2025) | Family inputs include forwarded PDFs, screenshots, photos, voice notes, and links; all should become actionable context rather than dead attachments. |
| Conversational continuity | Poke recognizes iMessage inline replies so a later message remains attached to the right earlier message; account merging preserves one selected account's Memory, Recipes, and Integrations. [Poke release notes](https://poke.com/docs/release-notes#november-17-2025) · [Poke account merging](https://poke.com/docs/merging-accounts) | References like “that,” delayed replies, and corrections must resolve against the right task/event instead of falling into generic clarification. |
| Human messaging cues | Poke supports WhatsApp read receipts, typing indicators, and inline replies, and markets a personality “as real as a friend.” [Poke release notes](https://poke.com/docs/release-notes#december-22-2025) · [Poke overview](https://poke.com/) | Presence cues and lightweight reactions can make Florence feel alive, but they cannot replace a substantive answer or work acknowledgement. |
| Never silently stops | Poke explicitly says it continues responding in a lighter mode after Free or Pro usage limits; demanding work may wait, but the assistant does not stop responding. [Usage and resets](https://poke.com/docs/usage-and-resets) | Silence is not an acceptable model decision. Florence should always answer, acknowledge work, report a blocker, or say what happens next. |

## The tool portfolio is much broader than Google Search

The benchmark suggests a capability plane, not a single search endpoint.

| Tool family Florence needs | User-visible jobs | Benchmark evidence |
| --- | --- | --- |
| General web research and page reading | Search broadly without requiring a pasted URL; fetch/read a specific URL; compare sources; return current, linked evidence. | Poke includes web search, says it verifies web sources when available, and offers a Poke Team integration that searches and extracts full webpage content with ranked results. [Poke docs](https://poke.com/docs) · [Release notes](https://poke.com/docs/release-notes#december-1-2025) · [Parallel Web Systems](https://poke.com/recipes/parallel-web-systems) |
| Maps, directions, and local services | Resolve routes/ETAs, nearby options, and practical local errands instead of returning generic search prose. | Poke documents improved maps/directions search; Instinct cites arranging a ride and booking a handyman. [Poke release notes](https://poke.com/docs/release-notes#january-12-2026) · [Instinct](https://instinct.co/) |
| Weather and environmental conditions | Forecast-aware reminders and practical advice tied to time and place. | Poke Team's Bad Weather Alerts covers rain, snow, storms, extreme heat, and practical advice; Poke's official MCP examples include a weather-data integration. [Bad Weather Alerts](https://poke.com/recipes/bad-weather-alerts) · [Official MCP examples](https://github.com/InteractionCo/poke-mcp-examples) |
| Flights and travel | Resolve a flight number to route/status, gates/delays, traveler, booking code, time zones, alternatives, itinerary, and follow-up monitoring. | Poke Team recipes extract route and booking details into calendars and sync travel bookings to TripIt; the official travel directory also exposes live-flight and fare-monitoring recipes as ecosystem capabilities. [Add Flights to Calendar](https://poke.com/r/add-flights-to-calendar) · [Forward Travel Plans to TripIt](https://poke.com/r/forward-travel-plans-to-tripit) · [Travel recipes](https://poke.com/recipes?tag=Travel) |
| Email, calendar, and contacts | Search complete threads, manage inbox state, draft/send/reply/forward with attachments, read every relevant calendar, create/update/RSVP/recurrence, and resolve people. | Poke's Gmail/Outlook and calendar documentation covers these read/write operations and contact sync. [Poke docs](https://poke.com/docs) · [Outlook release](https://poke.com/docs/release-notes#december-12-2025) · [Outlook Calendar](https://poke.com/recipes/outlook-calendar) · [Outlook Mail](https://poke.com/recipes/outlook-mail) |
| Tasks, reminders, and monitors | Create, update, complete, snooze, recur, and follow up; monitor a condition without repeating unchanged conclusions. | Poke natively manages reminders, supports background automations, can review recurring automations that were not acted on, and integrates Todoist progress tracking. [Poke docs](https://poke.com/docs) · [Poke overview](https://poke.com/) · [Release notes](https://poke.com/docs/release-notes#december-1-2025) · [Todoist](https://poke.com/recipes/todoist) |
| Documents and media | Read PDFs/images/screenshots/spreadsheets/voice notes; extract owners/deadlines; edit/preview attachments; make a compact chart/table when useful. | Poke documents voice input, image/spreadsheet extraction and visualization, PDF/image attachment preview/editing, and PDF generation/editing. [Poke overview](https://poke.com/) · [Poke release notes](https://poke.com/docs/release-notes) |
| Home and device services | Control household devices and use device/situational context when that directly completes a family request. | Instinct claims phone/computer use plus screen/audio/location context; Poke Team lists Sonos and Philips Hue integrations. [Instinct](https://instinct.co/) · [Home recipes](https://poke.com/recipes?tag=Home) |
| Shopping, money, and recurring household admin | Compare products/prices, watch a target, track subscriptions/renewals, route receipts, and act on a chosen option. | Poke's official directory includes Shopping/Finance/Home categories; Poke Team's monthly subscription audit flags duplicate subscriptions, price increases, and expiring trials. [Recipe directory](https://poke.com/recipes) · [Monthly Subscription Audit](https://poke.com/r/monthly-subscription-audit) |
| Real-world phone/human work | Call a business, reserve, order, book a ride, or complete an errand that lacks a reliable API. | Instinct cites proactive calls and real-world booking; Poke Human dispatches an operator for calls, reservations, orders, and rides. [Instinct](https://instinct.co/) · [Poke Human FAQ](https://poke.com/faq#poke-human) |
| Extensible integrations and event ingress | Add a new service without changing the core reasoner; receive webhooks/events; compose multiple tools in one task. | Poke supports arbitrary MCP servers and an API for event-driven workflows; the official SDK exposes `sendMessage`, `createWebhook`, and `sendWebhook`. [MCP docs](https://poke.com/docs/mcp-servers) · [Poke API](https://poke.com/docs/api) · [Official `poke` package](https://www.npmjs.com/package/poke) |

This does **not** imply Florence should ship every Poke developer integration. For a family assistant, maps/local, weather, travel, products/shopping, household documents, tasks, and real-world coordination are the high-value non-Google-Search tools; code repositories, deployments, and developer observability are not required to meet the stated product goal. That prioritization is an inference from the family jobs above and Poke's much wider recipe catalog. [Poke recipe directory](https://poke.com/recipes)

## The work model Florence should match

### Observed benchmark behavior

1. A request may trigger more than one action. Poke's own API examples chain research, email, calendar, Notion, and future reminders from one inbound event. [Poke API](https://poke.com/docs/api)
2. Work can continue off-turn. Poke markets real-time background automations and “get a head start on tasks”; Instinct markets proactive follow-up on dropped threads. [Poke pricing](https://poke.com/pricing) · [Poke overview](https://poke.com/) · [Instinct](https://instinct.co/)
3. Proactivity is grounded in connected sources and memory, not merely a timer. Poke says it automatically uses integrated services and memory “right on time,” and its first-party Morning Briefing combines the day's schedule, attention-worthy mail, and priorities. [Poke overview](https://poke.com/) · [Morning Briefing](https://poke.com/recipes/morning-briefing)
4. The assistant is expected to take action and then respond. Poke's API describes the sequence as evaluate the request, choose tools, take action, and respond. [Poke API](https://poke.com/docs/api#what-happens-next)
5. Hard-to-automate work can be escalated to an operator rather than silently abandoned. [Poke Human FAQ](https://poke.com/faq#poke-human)

### Inferred Florence contract

For every request that requires work, Florence should maintain a durable work record with: the requested outcome, current status, next action, tool/source being used, evidence/result, blocker (if any), and completion state. This is the minimum internal shape needed to reproduce the observed background, multi-tool, follow-through behavior. [Poke API](https://poke.com/docs/api) · [Instinct](https://instinct.co/)

The user-visible contract should be:

- **Immediate acknowledgement:** within the same inbound handling cycle, say what Florence understood and what it is doing. Native typing/read-state cues can supplement this acknowledgement. [Poke release notes](https://poke.com/docs/release-notes#december-22-2025)
- **Progress only when it matters:** update on a meaningful state change, a decision needed from a parent, or a materially long-running step—not with repetitive “still working” noise. This cadence is an inference from Poke's background-automation and proactive-update positioning. [Poke pricing](https://poke.com/pricing)
- **Finish visibly:** return the answer or action taken, cite the relevant source, and say what changed. If blocked, identify the exact missing input and retain the task so the next reply resumes it. This is an inference from Poke's “takes action and responds” contract and Instinct's dropped-thread follow-up. [Poke API](https://poke.com/docs/api#what-happens-next) · [Instinct](https://instinct.co/)
- **Never choose silence:** even a degraded path should answer with a lighter response or a concrete next step. Poke makes this an explicit product promise at usage limits. [Poke usage docs](https://poke.com/docs/usage-and-resets)

### Recommended Florence timing contract

**Observed:** Poke calls its paid automations “real-time,” exposes read/typing state in supported messaging, promises to keep responding after usage limits, and has treated delayed message responses as a product incident. Poke does not publish a numerical response SLA. [Poke pricing](https://poke.com/pricing) · [Poke release notes](https://poke.com/docs/release-notes#december-22-2025) · [Poke usage docs](https://poke.com/docs/usage-and-resets) · [Poke delayed-response incident](https://status.poke.com/incidents/01KQSSXSPD3QFA3W69PVAPVQJT)

The following is therefore a proposed Florence product SLO, not an observed competitor promise:

| Moment | Proposed user-visible deadline | Required behavior |
| --- | --- | --- |
| Message received | Within 5 seconds | Show typing/presence and send a short acknowledgement when the request will require tools or background work. |
| Ordinary answer | Within 20 seconds | Answer in the same conversational turn; never create a background task for something already answerable. |
| Tool-backed work | First meaningful result or progress within 60 seconds | Say which concrete step is underway and report the first finding, action, or blocker. |
| Longer active work | At each meaningful phase; never more than 3 minutes with no visible state while work is actively running | Report changed state, not repeated “still working” filler. |
| Completion or blocker | Immediately when known | Push the verified result/action, or ask for the one missing decision while keeping the work resumable. |

## Human-feeling messaging without sacrificing usefulness

**Observed:** Poke's documented human-feeling mechanics are native-channel presence and context: read receipts, typing indicators, inline replies, concise text, and voice messages. Its November 2025 notes also say it deliberately reduced emoji use. [Poke release notes](https://poke.com/docs/release-notes#december-22-2025) · [Poke release notes](https://poke.com/docs/release-notes#november-17-2025) · [Poke overview](https://poke.com/)

**Evidence gap:** No first-party Poke or Instinct page reviewed here promises that the assistant sends iMessage tapbacks/reactions. Reactions are therefore a reasonable Florence product choice, not a benchmark fact. The public benchmark clearly supports inline replies, typing/read state, voice, concise tone, and proactive calls/texts. [Poke release notes](https://poke.com/docs/release-notes) · [Instinct](https://instinct.co/)

**Inference for Florence:** Use a reaction for genuinely social acknowledgements (“got it,” celebration, sympathy) or as an immediate presence cue, but never as the only response to a question or delegated task. For work, a reaction should be followed by a short acknowledgement and eventual result. This preserves the friend-like surface Poke markets while meeting Poke's explicit non-silence behavior. [Poke overview](https://poke.com/) · [Poke usage docs](https://poke.com/docs/usage-and-resets)

## Memory and household context

**Observed:** Poke says proactive help uses both connected integrations and memory. Its account-merging flow preserves the selected account's Memory, Recipes, and Integrations, and its journal product recalls past entries and adapts prompts to the user. [Poke overview](https://poke.com/) · [Poke account merging](https://poke.com/docs/merging-accounts) · [Poke Journal](https://poke.com/journal)

**Observed:** Instinct's entire public position is a core model trained around “deeply personal nuances” and an understanding of what the person is doing and what matters, using applications and device signals. [Instinct](https://instinct.co/)

**Inference for Florence:** The primary unit should be the household/parental unit, with adult-private provenance and visibility on each source-derived item. The useful memory is not a pile of summaries; it is a reconciled model of people, commitments, recurring logistics, preferences, unresolved work, decisions, and what has already been communicated. This is how Florence can turn two personal information streams into a capability neither public benchmark describes. [Instinct](https://instinct.co/) · [Poke overview](https://poke.com/)

## Priority bar for Florence

### P0 — required to feel like an assistant that works

1. **Durable multi-step work loop:** acknowledge → investigate/use tools → act → verify → report or retain a specific blocker. [Poke API](https://poke.com/docs/api)
2. **No-silence response rule plus native presence:** every ordinary message gets a response; work gets an acknowledgement; use typing/inline reply/reaction as additive cues. [Poke usage docs](https://poke.com/docs/usage-and-resets) · [Poke release notes](https://poke.com/docs/release-notes)
3. **Complete household context:** both adults' email/calendar/contacts, complete scan accounting, retained relevant history, ownership/visibility, and one reconciled household docket. This is Florence's family-specific inference from the benchmarks' connected-source model. [Poke overview](https://poke.com/) · [Instinct](https://instinct.co/)
4. **General research toolchain:** unrestricted query planning, source search, arbitrary URL reading, source verification/citations, and result synthesis. [Poke docs](https://poke.com/docs) · [Parallel Web Systems](https://poke.com/recipes/parallel-web-systems) · [Poke release notes](https://poke.com/docs/release-notes#december-1-2025)
5. **Family-life tools beyond search:** maps/local, weather, live flight/travel, product/shopping, tasks/reminders, PDFs/images/voice, and email/calendar mutation. [Poke recipes](https://poke.com/recipes) · [Instinct](https://instinct.co/)
6. **Cross-tool composition:** for example, resolve `DL 747`, obtain route/live status, find viable alternatives, check household constraints, then report or update the itinerary—without asking for information the tools can derive. This example is an inference from Poke's flight/calendar recipes and multi-action API. [Add Flights to Calendar](https://poke.com/r/add-flights-to-calendar) · [Poke API](https://poke.com/docs/api)
7. **State-aware proactivity and deduplication:** surface changed/actionable conclusions; do not repeatedly announce the same unchanged event; mark items completed/reopened/snoozed based on evidence and parent replies. This is an inference from Poke's proactive-memory model and its feature for reviewing recurring automations that were not acted upon. [Poke overview](https://poke.com/) · [Poke release notes](https://poke.com/docs/release-notes#december-1-2025)

### P1 — platform breadth after the work loop is reliable

1. Plain-language one-shot and recurring automations/monitors with reusable family recipes. [Poke recipe docs](https://poke.com/docs/creating-recipes)
2. Curated extensibility through a task-appropriate tool protocol, with tools discovered by capability rather than prompt keywords. [Poke MCP docs](https://poke.com/docs/mcp-servers)
3. Event/webhook ingress so schools, travel services, home systems, and other apps can delegate work into the same Florence thread. [Poke API](https://poke.com/docs/api)
4. Phone/computer operation or human handoff for reservations, calls, orders, rides, and businesses without APIs. [Instinct](https://instinct.co/) · [Poke Human FAQ](https://poke.com/faq#poke-human)
5. Voice calls and richer ambient inputs (screen/audio/location) where they directly improve household assistance. [Instinct](https://instinct.co/)

## What can actually be taken from Poke's public code

**Observed:** The official public Poke developer assets expose the extension boundary, not the core assistant engine. The official `poke` npm package provides message and webhook APIs; the verified GitHub organization publishes MCP templates/examples for unauthenticated, API-key, OAuth-proxy, and DCR-backed tools, including weather and health examples. [Official `poke` package](https://www.npmjs.com/package/poke) · [InteractionCo GitHub](https://github.com/InteractionCo) · [Official MCP examples](https://github.com/InteractionCo/poke-mcp-examples)

**Observed limitation:** In the official public organization reviewed for this audit, there is no published Poke planner, memory engine, background work runner, or messaging reasoner to transplant. The reusable first-party code is integration scaffolding and the inbound API/SDK contract. [InteractionCo GitHub](https://github.com/InteractionCo)

**Inference:** Florence can directly borrow Poke's public integration patterns where they fit, but Pi/Hermes or Florence's own existing work system must supply the core task loop, context reconciliation, and follow-through. The user-visible benchmark above should determine what gets adapted; random developer infrastructure should not. [Official MCP examples](https://github.com/InteractionCo/poke-mcp-examples) · [Poke API](https://poke.com/docs/api)

## Public evidence gaps to avoid overstating

- Instinct's first-party public detail is one landing page; it does not publish a tool catalog, execution protocol, latency target, or source code. [Instinct](https://instinct.co/)
- Poke promises “real-time” background automations and continuous responses, but its public docs do not state a task-level latency SLA or a durable progress-event schema. [Poke pricing](https://poke.com/pricing) · [Poke usage docs](https://poke.com/docs/usage-and-resets)
- Poke documents memory as a product capability but does not publicly specify its retention, ranking, or retrieval architecture. [Poke overview](https://poke.com/) · [Poke account merging](https://poke.com/docs/merging-accounts)
- Neither public benchmark describes a native two-parent family authority/context model. Florence should treat this as its product advantage, not assume that copying a personal assistant's memory model is sufficient. [Instinct](https://instinct.co/) · [Poke account merging](https://poke.com/docs/merging-accounts)
- Poke's official recipe directory includes both Poke Team and community entries; community recipes prove ecosystem reach, not guaranteed first-party quality. [Managing integrations](https://poke.com/docs/managing-integrations) · [Recipe directory](https://poke.com/recipes)

## Bottom line

The competitive unit is **completed life-admin work**, not number of tools and not amount of surfaced context. Florence should first make delegation dependable and visible, then give that work loop a broad family-life tool plane: research/page reading, maps/local, weather, live travel, shopping, documents/media, tasks, communications, and eventually device/human execution. Instinct supplies the ceiling—an assistant that sees context, uses phones/computers, calls, and follows through—while Poke supplies the concrete shipping pattern: native messaging, memory-backed proactivity, background automations, an extensible tool ecosystem, event ingress, and real-world handoff. [Instinct](https://instinct.co/) · [Poke overview](https://poke.com/) · [Poke API](https://poke.com/docs/api) · [Poke MCP docs](https://poke.com/docs/mcp-servers) · [Poke Human FAQ](https://poke.com/faq#poke-human)
