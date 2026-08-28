# Instinct/Poke capability audit for Florence

Research date: 2026-08-28
Source access date: 2026-08-28 unless noted otherwise

## Bottom line

The competitive bar is not “better search.” It is **accepting an ordinary household objective, gathering missing public context, and finishing the work across the services where that work lives**.

Instinct's first-party product page describes the broadest version of that promise: an assistant connected to email, messaging, screen, audio, and location; trained to use a phone and computer; reachable by text or call; and able to follow up on dropped threads, arrange an airport ride, or book a handyman. Its founder additionally reports early users planning road trips, buying groceries and concert tickets, cancelling subscriptions, and planning a wedding. ([Instinct product page](https://instinct.co/), [founder launch post](https://x.com/noahrshinn/status/2092691344456351744))

Poke documents the concrete execution surface behind a similar promise: actionable email and calendar connections, reminders, background automations, first-party and custom integrations, browser-backed reservations, and optional human completion for calls, orders, rides, and reservations. ([Poke product page](https://poke.com/), [Poke docs](https://poke.com/docs), [Poke integrations](https://poke.com/docs/managing-integrations), [Poke release notes](https://poke.com/docs/release-notes))

Florence already has the household thread, durable work, proactive scheduling, family memory, public research, maps, weather, flights, and substantial Google Workspace work. Its largest remaining gap is **execution beyond Google and public page reading**: authenticated browsing, forms, reservations, purchases, external communication, trip execution, and cross-service follow-through.

## Evidence boundaries

The comparison below separates what the companies themselves publicly claim from product inference.

- **Instinct is still private access.** Its official site and founder's launch post are the available first-party capability evidence. They establish the product ambition and examples, but do not document supported connectors, booking providers, confirmation flows, memory behavior, reaction support, or response-time guarantees. Treat those examples as first-party claims, not a verified exhaustive tool list.
- **Poke is documented in substantially more detail.** Its official docs, release notes, and recipes name integrations and end-user actions. Poke Team recipes are treated as first-party examples. Community recipes are useful evidence that the platform can be extended, but not evidence that every installation has that behavior by default.
- **No first-party source reviewed publishes a response-time SLA or claims Tapback/reaction support.** “Real-time automations,” typing indicators, read receipts, and inline replies are documented; exact acknowledgement latency and reactions are not. Florence's immediate acknowledgement and situational-reaction goals are therefore product requirements, not copied competitor claims.

## Florence's current execution surface

This table describes implemented repository behavior, including the Google Workspace work in this branch. It is not a roadmap.

| Area | Implemented now | Material limit |
| --- | --- | --- |
| Family messaging | Private-parent and family-group participation, inline reply context, inbound/outbound reactions, immediate work cues, typing, paced bubbles, delivery state, text, voice-note transcription, images, and PDFs. ([Linq adapter](../../packages/linq/src/index.ts), [turn orchestration](../../apps/api/src/florence.ts)) | No live phone call, outbound call, or general contact channel outside the connected parent threads. |
| Household context | Source-linked family memory, two-adult household model, child profiles, retained facts/interests, 90-day Gmail and Calendar ingestion, and a reconciled household docket. ([store](../../packages/database/src/store.ts), [turn orchestration](../../apps/api/src/florence.ts)) | Source selection and proactive ranking still need product tuning; retaining evidence is useful only if Florence retrieves the right thing at the right time. |
| Public information | Current web search plus exact public HTML/PDF reading. ([reasoner](../../apps/api/src/reasoner.ts), [page reader](../../apps/api/src/public-page.ts)) | Cannot operate an authenticated site, navigate a multi-page flow, fill/upload/submit a form, or return a transaction receipt. |
| Places, routes, and weather | Place and nearby search, bounded-area search, reverse geocoding, real route distance/directions, time zones, and U.S. forecasts/alerts. ([maps](../../apps/api/src/maps.ts), [weather](../../apps/api/src/weather.ts)) | Cannot check provider-specific live availability, contact a business, reserve, or book transport. |
| Travel discovery | Live flight alternatives with prices, segments, constraints, and booking links; public research can resolve identifiers. ([flights](../../apps/api/src/flights.ts), [reasoner](../../apps/api/src/reasoner.ts)) | No dedicated flight-number status/gate primitive, itinerary state, confirmation-email sync, disruption monitor, rebooking, hotel, or ground-transport completion. |
| Calendar and reminders | Broad personal-calendar reads; shared family-calendar provision/create/update/delete; private-derived family-date offers; one-shot and recurring reminders; finite monitors; pause/resume/run/cancel. ([Google adapter](../../packages/google/src/index.ts), [reasoner](../../apps/api/src/reasoner.ts)) | No general personal-calendar invitation/RSVP workflow, meeting negotiation, or external scheduler. |
| Gmail and Contacts | Gmail search/get/labels plus durable send/reply/label changes; Google Contacts search plus durable create/update. ([Workspace adapter](../../packages/google/src/workspace.ts), [reasoner tools](../../apps/api/src/reasoner.ts)) | No Gmail draft or forward operation, attachment send/reply, mail rules, or non-Google address book. |
| Drive, Docs, Sheets, Slides, Tasks | Drive search/metadata plus durable folder/share/trash; Docs read/create/append; Sheets read/create/update/append; Slides read/create/add text slide; Google Tasks list/create/update/complete. ([Workspace adapter](../../packages/google/src/workspace.ts), [reasoner tools](../../apps/api/src/reasoner.ts)) | No arbitrary Drive file download/upload, PDF/image artifact pipeline, rich document editing, form submission, or non-Google document/task systems. |
| Long-running work | Durable private or household work with steering, cancellation, checkpointed retries, bounded progress, and one terminal delivery. ([reasoner](../../apps/api/src/reasoner.ts), [store](../../packages/database/src/store.ts)) | The loop can only finish work for which Florence has a real tool. It cannot compensate for missing browser, commerce, booking, or communication actions. |

## Competitor capability comparison

| End-user job | Observed first-party Instinct claim | Observed first-party Poke capability | What it means for Florence |
| --- | --- | --- | --- |
| Hand off an objective and get an outcome | Uses a phone and computer “the same way that humans do”; examples include rides and handyman booking. ([Instinct](https://instinct.co/)) | API-triggered objectives can use email, calendar, reminders, and integrations; Poke Human completes calls, reservations, orders, and rides. ([Poke API](https://poke.com/docs/api), [Poke Human](https://poke.com/faq)) | A search result or “I'll look” is not completion. Every supported objective needs either an external result, a precise blocker, or an honest failure. |
| Communicate on the user's behalf | Connects to email and messaging and can proactively call or text. ([Instinct](https://instinct.co/)) | Gmail searches, drafts, sends, labels, and organizes. Outlook supports compose/reply with attachments, search, folders, flags, rules, contacts, and calendars. ([Poke](https://poke.com/), [Outlook Mail](https://poke.com/recipes/outlook-mail), [release notes](https://poke.com/docs/release-notes)) | Finish Gmail drafts/forwarding/attachments, then add outbound provider communication and calls. |
| Own calendar coordination | No detailed scheduling surface is publicly documented. | Google Calendar creates events, schedules meetings, checks availability across calendars, manages reminders, and coordinates scheduling; Outlook adds recurring events and RSVP tracking. ([Google Calendar](https://poke.com/r/google-calendar), [Outlook Calendar](https://poke.com/r/outlook-calendar)) | Extend family-calendar strength into meeting invitations, negotiation, RSVP handling, and external appointment workflows. |
| Handle reminders and task systems | Following up on dropped threads is a core example. ([Instinct](https://instinct.co/)) | Native reminders plus Todoist/Asana task management and real-time background automations. ([Poke docs](https://poke.com/docs), [Todoist](https://poke.com/recipes/todoist), [Poke pricing](https://poke.com/pricing)) | Florence's native reminders and Google Tasks are a good base. The important gap is turning an open household objective into continued work, not adding another reminder representation. |
| Browse, fill, and transact | Broad phone/computer use and reported purchases/cancellations imply general interface operation, but the mechanism and supported sites are not public. ([Instinct](https://instinct.co/), [founder launch post](https://x.com/noahrshinn/status/2092691344456351744)) | A first-party recipe finds restaurants and autonomously reserves through Browserbase; Poke Human handles the unsupported long tail. ([restaurant reservations](https://poke.com/r/restaurant-reservations), [release notes](https://poke.com/docs/release-notes)) | Authenticated browser completion is the highest-impact missing surface: school portals, forms, reservations, appointments, checkout, and cancellations. |
| Complete travel work | First-party examples include airport rides and cross-country road-trip planning. ([Instinct](https://instinct.co/), [founder launch post](https://x.com/noahrshinn/status/2092691344456351744)) | First-party recipes detect flight confirmations, add timezone-correct calendar events, forward trips to TripIt/Flighty, and route receipts; the recipe directory also exposes flight-status and fare-monitor workflows. ([travel recipes](https://poke.com/recipes?tag=Travel), [release notes](https://poke.com/docs/release-notes)) | Treat a flight number as enough to begin. Add flight status and itinerary state, then disruption monitoring, rebooking options, hotels, and ground transport. |
| Move information across services | Connects broadly to applications/devices, without a public integration inventory. ([Instinct](https://instinct.co/)) | Poke's API examples chain email, calendar, Notion, and reminders; Recipes bundle behavior with integrations; arbitrary MCP servers expose discoverable tools. ([Poke API](https://poke.com/docs/api), [Recipes docs](https://poke.com/docs/creating-recipes), [MCP docs](https://poke.com/docs/mcp-servers)) | Build a few high-value household connectors and complete cross-app workflows before investing in a general marketplace. |
| Monitor and surface what matters | Understands what is important and follows up on dropped threads. ([Instinct](https://instinct.co/)) | Uses connected services and memory proactively; first-party recipes produce calendar/email morning briefings, flag unreplied sent mail, and prevent duplicate receipt forwarding. ([Poke](https://poke.com/), [morning briefing](https://poke.com/recipes/morning-briefing), [follow-up reminders](https://poke.com/recipes/follow-up-reminders), [receipt forwarding](https://poke.com/recipes/forward-receipts-to-mercury)) | Florence should read broad history but surface selectively. Monitoring needs evidence-change detection and deduplication so it does not repeat the same reminder. |
| Remember useful context | Says it understands what the user is working on and what is important; no public memory model is documented. ([Instinct](https://instinct.co/)) | Markets integrated-service use plus memory to help at the right time. ([Poke](https://poke.com/)) | Keep the household/parental unit as Florence's differentiator: remember family facts once, retain provenance, and retrieve across both parents where the product has made the fact shared. |
| Feel like a person in Messages | Text and calls are the stated interface. No official reaction behavior is documented. ([Instinct](https://instinct.co/)) | Lives in Apple Messages, WhatsApp, Telegram, and RCS; accepts voice messages; markets a friend-like personality; supports inline replies in Apple Messages and typing/read receipts/inline replies in WhatsApp. No official Tapback claim was found. ([Poke docs](https://poke.com/docs), [Poke](https://poke.com/), [release notes](https://poke.com/docs/release-notes)) | Use reactions situationally as immediate social acknowledgement, followed by actual work. Never use a reaction as the only answer to a substantive request, and never choose silence for a normal message. |
| Make progress legible | No public progress protocol or response-time guarantee was found. | Markets “real-time” background automations and says API objectives take action and respond, but publishes no acknowledgement or completion SLA. ([Poke pricing](https://poke.com/pricing), [Poke API](https://poke.com/docs/api)) | Florence's UX should set its own stronger bar: acknowledge immediately, identify the work started, send only meaningful progress, and finish with the outcome or blocker. |

## Prioritized capability gaps

### P0: Work Florence should be able to finish next

1. **Authenticated browser completion** — open and navigate sites, maintain a task session, click, type, select, upload, download, submit, wait for page state, and return the confirmation. Initial family jobs: school/camp forms, appointment booking, restaurant reservations, account changes, and subscription cancellation.
2. **External communication** — Gmail drafts, forwards, and attachments; contact lookup across connected accounts; outbound messages or calls to schools, camps, doctors, restaurants, and local providers; reply tracking until the household has an answer.
3. **Travel state and disruption work** — resolve route/status from carrier + flight number + date without asking for data the public web provides; extract itineraries from email; track delays/gates; compare replacement flights, hotels, and ground transport; carry the selected option into booking.
4. **Artifacts and forms** — download Drive or portal files, extract/OCR useful content, edit or generate the needed PDF/image/document, upload it, and retain the submitted confirmation. This is the bridge between “Florence found the form” and “the form is done.”

### P1: Turn isolated tools into household outcomes

5. **Reservations, appointments, and local services** — inspect live availability, reserve or book, contact providers for missing information, track replies/quotes, reschedule/cancel, and put the confirmed result on the family calendar.
6. **Purchases and account chores** — compare the constrained options, check live price/availability, add to cart/order where supported, cancel subscriptions, and capture receipts or cancellation confirmations. Browser completion can cover the long tail; dedicated provider tools are worthwhile only for repeated family jobs.
7. **Cross-app workflows** — email-to-calendar, email-to-task, form-to-Drive, receipt-to-record, trip-to-calendar, and provider-reply-to-docket flows that continue until the objective is done. The durable work record already exists; the missing piece is the external action chain.
8. **Proactive monitoring that earns attention** — monitor changed evidence, unreplied messages, deadlines, trip disruptions, price/availability, and missing confirmations. Deduplicate by the underlying item and suppress unchanged results; broad 90-day ingestion should improve retrieval, not produce a flood of summaries.

### P2: Expand reach after the core workflows work

9. **A small set of household connectors** — likely Todoist/Notion, TripIt/Flighty, school/camp systems, and high-frequency booking or health-administration services. Choose them from actual family usage rather than cloning Poke's developer-oriented catalog.
10. **Live calls** — parent-to-Florence voice plus outbound calls for providers whose workflows still require the phone. The result belongs back in the family thread and docket.
11. **Human completion fallback** — only after the automated path has enough coverage to identify the jobs it genuinely cannot finish. Poke demonstrates that a person can close the phone/order/reservation long tail; it does not imply Florence needs that operating layer before browser and communication tools work.

## Messaging and latency acceptance bar

These are Florence product requirements derived from the observed testing failures, not competitor claims:

- A normal message always receives a useful response. Silence is reserved for non-substantive reactions or system noise, never an ordinary question or request.
- A task that will continue after the turn gets an immediate, natural acknowledgement. A situational Tapback can make the interaction feel human, but it accompanies—not replaces—the acknowledgement.
- Florence starts with available context. Identifiers such as a flight number, event link, business name, or email thread should trigger lookup before a clarifying question.
- Progress messages report a meaningful new step or blocker. They do not repeat the request, narrate internal machinery, or resend the same reminder in different words.
- The terminal message states what happened and includes the useful external evidence: reservation time, sent-email recipient, event link, order/confirmation number, submitted-form receipt, or the exact unresolved blocker.
- Cross-app work remains one user objective. Parents should not have to manually relay outputs between Florence's own tools.

## Source register

All sources below are first-party and were accessed 2026-08-28.

| Source | What it supports |
| --- | --- |
| [Instinct product page](https://instinct.co/) | Inputs/devices, phone/computer use, text/call interface, proactive follow-up, ride and handyman examples, private-access status. |
| [Instinct founder launch post](https://x.com/noahrshinn/status/2092691344456351744) | Founder-reported early-user examples: road trips, groceries, concert tickets, subscription cancellation, wedding planning. |
| [Poke product page](https://poke.com/) and [Poke docs](https://poke.com/docs) | Messaging channels, email/calendar/reminders/web, memory/proactivity marketing, voice input, integrations, background automation positioning. |
| [Poke release notes](https://poke.com/docs/release-notes) and [FAQ](https://poke.com/faq) | Apple Messages availability, inline replies, WhatsApp presence behaviors, Outlook actions, attachments, Poke Human jobs. |
| [Poke integrations](https://poke.com/docs/managing-integrations), [Recipes docs](https://poke.com/docs/creating-recipes), and [MCP docs](https://poke.com/docs/mcp-servers) | First/third-party integration model, recipe behavior bundles, custom discoverable tools. |
| [Poke API](https://poke.com/docs/api) | Programmatic objectives and documented cross-app action examples. |
| [Restaurant Reservations](https://poke.com/r/restaurant-reservations) | Poke Team's Browserbase-backed autonomous reservation example. |
| [Google Calendar](https://poke.com/r/google-calendar) and [Outlook Calendar](https://poke.com/r/outlook-calendar) | Calendar creation, availability, meeting coordination, recurrence, reminders, and RSVPs. |
| [Outlook Mail](https://poke.com/recipes/outlook-mail) | Search, drafts/sends, folders, flags, rules, and email workflows. |
| [Morning Briefing](https://poke.com/recipes/morning-briefing), [Follow-up Reminders](https://poke.com/recipes/follow-up-reminders), and [receipt forwarding](https://poke.com/recipes/forward-receipts-to-mercury) | Proactive cross-source selection, unanswered-message follow-up, and duplicate-aware automation. |
| [Travel recipes](https://poke.com/recipes?tag=Travel) | Flight status/fares, itinerary forwarding, receipt routing, and flight-to-calendar workflows. |
| [Poke pricing](https://poke.com/pricing) | “Real-time” background automation positioning; no published latency SLA. |
