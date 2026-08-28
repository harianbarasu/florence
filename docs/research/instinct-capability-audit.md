# Instinct capability audit for Florence

Research date: 2026-08-28

Source access date: 2026-08-28

## Bottom line

Instinct's public product is not differentiated by a documented search engine. It is differentiated by an **outcome contract**: accept an ordinary objective over text or a call, recover context from the user's applications and devices, use a phone or computer, take actions across connected services, and follow up until something useful happens. Instinct's own examples end with a ride arranged or a handyman booked, not a list of links. ([Product page](https://instinct.co/), [Privacy Policy](https://instinct.co/privacy-policy), [Terms of Service](https://instinct.co/terms))

Florence already has the right household-specific foundation and considerably more implemented Google depth than a search-only assistant: two-parent Messages participation, reactions, 90-day source ingestion, household memory, maps, weather, flights, public HTML/PDF reading, reminders, durable work, and broad Google Workspace actions. Its largest lived-product gap was **operating the non-Google surface where family errands finish**.

The smallest implementation tranche that most closes that gap is:

> **The family can now ask Florence to open an authenticated site, keep the browser session alive across work turns, navigate and fill one real household flow, let an adult take over for sign-in when needed, and continue operating from the resulting page state.**

That means one browser/computer-use capability in Florence's existing durable work loop—not another search provider, connector framework, task runtime, or generic action/safety layer. The focused proof stops at a camp-portal review page so the browser journey has a deterministic assertion; the runtime action set itself does not impose a review-only boundary.

## Evidence rules

This audit uses only first-party public sources for claims about Instinct:

- Instinct's current [product page](https://instinct.co/);
- Instinct's [Privacy Policy](https://instinct.co/privacy-policy), revised August 26, 2026;
- Instinct's [Terms of Service](https://instinct.co/terms), revised August 26, 2026; and
- founder Noah Shinn's [launch post](https://x.com/noahrshinn/status/2092691344456351744), with the access caveat below.

The policy and terms are used here only where they materially define a shipped or contemplated product capability. This is not a privacy, legal, or safety review.

Instinct remains in private access. Its public pages establish a product promise and supported data/action surfaces, but they do not expose a tool catalog, runtime design, provider matrix, or reliability data. Every statement below distinguishes a public claim from an implementation inference.

## What Instinct publicly says it does

| Product surface | First-party evidence | What the evidence supports | What it does **not** establish |
| --- | --- | --- | --- |
| Natural interface | Users can text or call; the product says there are “no new interfaces.” ([Product page](https://instinct.co/)) | Ordinary conversational delegation rather than a task-builder UI | iMessage versus SMS behavior, WhatsApp support, group chats, reactions, typing indicators, or response latency |
| Ambient context | Instinct names email, messaging, screen, audio, location, applications, and devices. ([Product page](https://instinct.co/)) | The assistant is expected to start from available personal context | Exact connectors, indexing horizon, retrieval quality, or family sharing |
| Planning and autonomous work | The assistant “thinks, plans, and acts”; Instinct says it understands requests and plans and completes tasks, including by engaging third parties. ([Privacy Policy](https://instinct.co/privacy-policy)) | An objective may require several steps and external work | Durable jobs, retries, recovery after restart, steering, cancellation, or parallel work |
| Phone and computer use | Instinct says it is trained to use a phone and computer the way people do. ([Product page](https://instinct.co/)) | General interface operation is central to the product claim | Browser vendor, accessibility/vision mechanism, site coverage, or whether every advertised outcome uses computer control |
| Connected-service actions | The service may access, index, exchange data with, and take actions on connected third-party websites, applications, and services. ([Terms, “Connected services”](https://instinct.co/terms)) | The product contract extends beyond read-only retrieval and beyond Google | A complete provider list or per-provider operation list |
| Google Workspace | Gmail, Calendar, Drive, Docs, Sheets, Slides, and Tasks are explicitly named, including taking actions and personalizing the service. ([Privacy Policy, “Google Workspace”](https://instinct.co/privacy-policy)) | Instinct's named Google surface is the full productivity suite | Editing depth, Contacts support, search quality, or demonstrated end-to-end workflows for each app |
| Authenticated work | Instinct says an adult may provide credentials so the assistant can sign in to third-party accounts. ([Privacy Policy](https://instinct.co/privacy-policy)) | Authenticated portals and account chores are in scope | Credential storage design, assisted-login UX, MFA handling, session persistence, or adult takeover |
| Purchases and commitments | The terms cover purchases, sharing a payment method with a connected service, and entering agreements, commitments, or transactions on the user's behalf. ([Terms, “Actions”](https://instinct.co/terms)) | Transactional completion—not just product research—is part of the product contract | Merchant coverage, checkout success rate, confirmation UX, or default review behavior |
| External coordination | The assistant may engage third parties; current examples include arranging an airport ride, booking a handyman, and booking a medical appointment. ([Product page](https://instinct.co/), [Privacy Policy](https://instinct.co/privacy-policy)) | Instinct aims to coordinate with people and services outside the user's inbox | Whether it calls, texts, emails, uses a marketplace, or drives a website for each example |
| Proactive follow-up | Instinct says it follows up on dropped threads and can proactively call or text the user. ([Product page](https://instinct.co/)) | It claims responsibility for unfinished work, not only reactive answers | Trigger logic, deduplication, progress cadence, or a response-time guarantee |
| Status surfaces | The app contract names push, local, text, picture, alert, and email messages used for status updates. ([Terms, “Use of the app”](https://instinct.co/terms)) | Multiple outbound surfaces exist for keeping a user informed | Which channels the assistant itself uses, what it sends, or how quickly |
| Memory-like context | Instinct describes personalization from current context and prior experience, indexing connected-service data, and a named Vault. ([Privacy Policy](https://instinct.co/privacy-policy), [Terms, “Materials”](https://instinct.co/terms)) | Persistent context is part of the product surface | Memory schema, source provenance, contradiction handling, active-thread representation, or shared-family knowledge |
| Voice, screen, and location | The product names audio, calls, screen, and location; the policy names voice and optional precise geolocation. ([Product page](https://instinct.co/), [Privacy Policy](https://instinct.co/privacy-policy)) | Instinct targets more than a text-and-email assistant | Live conversational quality, screen-control architecture, or location-trigger behavior |

### Public outcome examples

Instinct's live product page gives three concrete behaviors: recover a dropped thread and proactively contact the user, arrange a ride to the airport, and book a handyman. ([Product page](https://instinct.co/))

The founder's launch post reports early users planning cross-country road trips, buying weekly groceries and concert tickets, cancelling subscriptions, and planning a wedding. ([Founder launch post](https://x.com/noahrshinn/status/2092691344456351744)) These are founder-reported examples, not public demonstrations or a guarantee of general support. The direct X page returned HTTP 403 to the research crawler on 2026-08-28, so the post URL is retained as the primary citation but its contents could not be freshly re-opened in this pass.

The important commonality is not any one merchant or provider. Each example requires context gathering plus action across one or more ordinary services.

## What Instinct does not document publicly

No first-party public source reviewed establishes:

- a dedicated Google Search tool or any complete web/search provider inventory;
- dedicated maps, weather, flight-status, hotel, restaurant, grocery, ticketing, healthcare, or home-service integrations;
- whether long-tail work uses a browser, native app control, APIs, people, or a mixture;
- a public browser action set such as navigation, snapshots, click, type, upload, download, or screenshots;
- a human-operator fallback or assisted-login/takeover flow;
- task persistence, retries, timeouts, steering, cancellation, or restart recovery;
- progress-message cadence or an acknowledgement/completion SLA;
- reactions, Tapbacks, inline replies, typing indicators, or read receipts;
- group or household accounts, shared family memory, or multi-adult coordination; or
- measured reliability for the advertised and founder-reported outcomes.

These are meaningful evidence boundaries. For Florence, reactions and immediate feedback remain good product requirements because of the two-phone testing experience—not because Instinct publicly demonstrates them. Likewise, browser/computer use is the strongest implementation hypothesis for closing Florence's action gap, but Instinct does not publish its mechanism.

## Florence versus the public Instinct bar

This is implemented repository behavior as of the research date, not planned work.

| Lived capability | Florence today | Instinct public bar | Material Florence gap |
| --- | --- | --- | --- |
| Parent interaction | Private-parent and family-group Messages, text, voice-note transcription, images/PDFs, inline context, reactions, typing, delivery state, and immediate work cues. ([Linq adapter](../../packages/linq/src/index.ts), [turn orchestration](../../apps/api/src/florence.ts)) | Text or call, plus proactive call/text | No live inbound/outbound call and no general messaging channel outside the connected family threads |
| Household context | Two-adult household model, child profiles, source-linked memory, retained facts/interests, 90-day Gmail/Calendar ingestion, and a reconciled docket. ([store](../../packages/database/src/store.ts), [turn orchestration](../../apps/api/src/florence.ts)) | Personal context from apps/devices and prior experience | Retrieval/ranking still needs to turn broad history into the right next action without repetition; Instinct does not publicly offer Florence's shared-family model |
| Public research and browser work | Current web search, exact public HTML/PDF reading, maps/places/routes/time zones, U.S. weather/alerts, live flight alternatives, and a durable authenticated browser with live owner takeover. ([reasoner](../../apps/api/src/reasoner.ts), [browser](../../apps/api/src/browser.ts), [page reader](../../apps/api/src/public-page.ts), [maps](../../apps/api/src/maps.ts), [weather](../../apps/api/src/weather.ts), [flights](../../apps/api/src/flights.ts)) | Broad context plus phone/computer operation | The browser closes the general interactive-site reach gap; provider-specific execution and confirmation remain to be proven one real workflow at a time |
| Google work | Gmail read/search/send/reply/labels; Contacts search/create/update; Calendar read/shared-calendar actions; Drive, Docs, Sheets, Slides, and Tasks read/write operations. ([Google adapter](../../packages/google/src/index.ts), [Workspace adapter](../../packages/google/src/workspace.ts), [reasoner tools](../../apps/api/src/reasoner.ts)) | Gmail, Calendar, Drive, Docs, Sheets, Slides, and Tasks actions | Useful depth gaps remain, but another Google action closes less of the lived-product gap than non-Google execution |
| Reminders and proactive work | Natural reminder create/list/change/cancel/pause/resume/run/recurrence, finite monitors, scheduled due work, and deduplicated delivery paths. ([reasoner](../../apps/api/src/reasoner.ts), [turn orchestration](../../apps/api/src/florence.ts)) | Dropped-thread follow-up and proactive call/text | Evidence ranking and active-thread follow-through need product tuning; new reminder infrastructure is not the missing capability |
| Long-running objectives | Durable private or household work with checkpoints, retries, steering, cancellation, progress, a terminal result, and a browser session that survives those checkpoints. ([reasoner](../../apps/api/src/reasoner.ts), [browser](../../apps/api/src/browser.ts), [store](../../packages/database/src/store.ts)) | Think, plan, act, and complete tasks | The work loop now has a general external action tool; broad outcome reliability depends on proving and reconciling concrete workflows |
| External outcomes | Google actions and family-calendar/reminder outcomes can complete; durable work can now operate interactive or authenticated sites rather than returning only links. | Rides, bookings, purchases, connected-service actions, and commitments | Provider-confirmed bookings, purchases, cancellations, third-party contact, and receipt capture still need end-to-end product journeys |

Florence was therefore not missing “agent infrastructure.” It was missing the **last-mile tool** that lets its existing durable worker turn research into a real-world state change. The browser tranche below supplies that reach; the remaining work is provider-confirmed execution across concrete household journeys.

## Smallest next tranche: authenticated browser work

### User-visible capability

Start with one named family errand:

> A parent asks Florence to handle a school/camp or appointment portal. Florence opens the site, keeps the session across work turns, lets the parent take over for sign-in if required, navigates by the site's current controls, fills the known household details, and returns the exact review state before the final submit/book/pay step.

This is deliberately narrower than “general computer use” but exercises the part of Instinct's public bar Florence cannot currently reach.

### Concrete operation surface

The implemented first tranche includes the operations required by that journey:

- open/navigate and go back;
- extract a bounded accessibility snapshot with fresh element references;
- click, type/fill, select, check/uncheck, press a key, scroll, and wait for page state;
- capture a bounded screenshot when the accessibility view is insufficient;
- keep one task-bound authenticated session alive across durable work passes;
- provide an adult-facing takeover link when sign-in or MFA needs the parent; and
- close the session on terminal completion or cancellation.

The model should receive page state and action results through the existing typed durable-tool seam. Provider session IDs, credentials, cleanup, and reconnection remain application-owned rather than model-selected.

### Why this tranche comes first

1. **It closes the widest gap with one tool.** The same operation set reaches school portals, camp registration, appointments, restaurant availability, account changes, subscriptions, travel sites, and the long tail of local services.
2. **It complements rather than duplicates Google Workspace.** Gmail/Calendar/Drive can supply the context; the browser carries that context into the external workflow.
3. **It uses Florence's existing work loop.** Checkpointing, retry, steering, cancellation, progress, and terminal delivery already exist. No second task runtime or generalized action framework is justified.
4. **It matches Instinct's defining public claim.** Phone/computer operation and authenticated connected-service actions are the clearest capability differences in Instinct's own materials.
5. **It yields an honest, testable household result.** The pilot family can see the portal state and review what Florence filled, rather than evaluating an internal tool count.

### Explicitly outside this tranche

- a general connector/plugin marketplace;
- a new credential store, scheduler, queue, memory system, or work runtime;
- coding, terminal, shell, repository, or arbitrary-filesystem tools;
- a generic approval, policy, evidence, safety, or privacy layer;
- human call-center operations;
- every browser action or every site;
- provider-specific payment handling or a guarantee that every binding submission can be reconciled; and
- random provider matrices or framework tests.

Provider-confirmed submit/book/pay/cancel outcomes and external email/SMS/call completion are the next action tranche. The browser can already press the site's controls; what remains is proving concrete journeys and reporting the provider-confirmed outcome without a search-result handoff.

## Next capabilities after browser reach

Once the named browser journey works end to end, the next gaps should be closed in lived-product order:

1. **Commit and verify external actions** — submit/book/cancel when requested, then report the provider-confirmed result or exact failure.
2. **External communication** — Gmail draft/forward/attachments first, then provider email/SMS/calls and reply tracking.
3. **Artifacts and forms** — download, understand, generate/edit, upload, and retain the submitted receipt.
4. **Travel state and execution** — itinerary extraction, flight status/gates, disruption monitoring, hotels/ground transport, and carrying a selected option into booking.
5. **Live voice/location work** — conversational calls and location-aware errands after the browser and communication paths can actually complete the underlying jobs.

## Source register and uncertainty

All live pages below are first-party Instinct sources accessed 2026-08-28.

| Source | Exact evidence used | Uncertainty |
| --- | --- | --- |
| [Instinct product page](https://instinct.co/) | Email/messaging/screen/audio/location, phone/computer use, text/call interface, dropped-thread follow-up, proactive call/text, airport ride, handyman, private-access status | Marketing claims; no provider list, mechanism, demonstration, or reliability data |
| [Instinct Privacy Policy](https://instinct.co/privacy-policy) | Autonomous assistant that thinks/plans/acts, third-party engagement, credentials/sign-in, rides, medical appointments, voice/location, Gmail/Calendar/Drive/Docs/Sheets/Slides/Tasks | Establishes intended/service surfaces, not operation depth or successful execution rates |
| [Instinct Terms of Service](https://instinct.co/terms) | Connected-service indexing and actions, purchases, commitments/transactions, Vault, Mac/mobile apps, push/text/picture/alert/email status surfaces | Contract breadth does not prove every action is enabled for every user or provider |
| [Founder launch post](https://x.com/noahrshinn/status/2092691344456351744) | Founder-reported road trips, groceries, concert tickets, subscription cancellation, and wedding planning | Direct X fetch returned 403 in this research pass; examples are reports, not public demos |

No secondary reports, competitor teardowns, or inferred closed-product internals are used as evidence for Instinct capabilities in this audit.
