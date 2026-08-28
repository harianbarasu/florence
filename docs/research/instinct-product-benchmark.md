# Instinct product benchmark for Florence

Research date: 2026-08-27

## Scope and evidence standard

This is a public-source product benchmark, not a hands-on review. It uses only material published by Instinct itself:

- the [official product page](https://instinct.co/);
- the [Privacy Policy](https://instinct.co/privacy-policy), revised August 26, 2026;
- the [Terms of Service](https://instinct.co/terms), revised August 26, 2026;
- the [Acceptable Use Policy](https://instinct.co/acceptable-use-policy); and
- Instinct's [SMS terms](https://instinct.co/legal/sms-terms.html) and [SMS privacy statement](https://instinct.co/legal/sms-privacy.html).

No authenticated or closed-beta product was used. Instinct publishes no public help center, API documentation, integration catalog, release notes, product demo, or technical task-lifecycle documentation that could be located on its official domains. Its Terms also expressly restrict using the service for benchmarking or developing a competing product and prohibit disclosure of closed-beta material. Accordingly, this note does not inspect the private app, reproduce beta output, reverse engineer client bundles, or treat third-party anecdotes as verified product behavior. The result is high-confidence on the public product promise and low-confidence on private implementation details.

Throughout this document:

- **Verified** means Instinct states it directly in a first-party source.
- **Inference** means the stated product promise strongly suggests the behavior, but Instinct does not document the mechanism.
- **Unknown** means the public sources do not establish the behavior.

## Executive conclusion

Instinct's public product contract is not “a chatbot with search.” It is an **always-available personal operator**:

1. A person reaches it through familiar channels—text or phone—rather than opening a task-specific interface.
2. It has broad, ongoing context from communications, productivity systems, devices, and prior interactions.
3. It decides how to perform multi-step work, including using connected services and interacting with third parties.
4. It follows up proactively instead of waiting for every next prompt.
5. It can carry work through to an external outcome: a booked ride, scheduled service, purchase, communication, or other transaction.

That is the competitive bar for Florence. Google and public search are important inputs, but they are not the product. Florence needs a general work engine, a much larger assistant-oriented tool surface, contextual proactivity, and conversational progress/completion behavior. The family focus should determine **which work is most valuable**, not confine Florence to reading Gmail and Calendar.

## Verified product model

### 1. The assistant is the interface

Instinct says users can text or call it and that it is trained to use a phone and computer as a person would. It explicitly positions the product as requiring no new interface. The legal materials additionally identify related macOS and mobile applications, SMS sign-in, an assigned Instinct number for a welcome text, and push/local/text/email messages for service and delivery updates. ([Product page](https://instinct.co/), [Privacy Policy](https://instinct.co/privacy-policy), [SMS terms](https://instinct.co/legal/sms-terms.html), [Terms §6](https://instinct.co/terms))

**Product meaning for Florence:** the primary experience is a relationship in Messages, not a dashboard or a collection of commands. A web app may configure sources and history, but the work begins, progresses, and ends conversationally.

### 2. The assistant is expected to act, not merely advise

Instinct calls the product an autonomous personal assistant that thinks, plans, and acts to complete everyday tasks. Its policy says it uses information to understand requests, plan and complete tasks, engage third parties, generate responses, and provide personalized suggestions. Its Terms authorize actions on connected services, including purchases and entering agreements or commitments on the user's behalf. ([Privacy Policy §§ collection/use and assistant data](https://instinct.co/privacy-policy), [Terms §3](https://instinct.co/terms))

The public examples are deliberately outcome-shaped: arrange a ride to the airport and book a handyman, rather than merely find options or explain how to do those things. ([Product page](https://instinct.co/))

**Product meaning for Florence:** “I can help” followed by no work is a product failure. For an actionable request, Florence should either start the work, ask for the one genuinely missing input, or state that the capability is unavailable. It should not silently collapse a task into conversational advice.

### 3. Broad context is part of the capability

Instinct says it connects to email, messaging, screen, audio, location, and other applications or devices. Its Google Workspace disclosure specifically names Gmail, Calendar, Drive, Docs, Sheets, Slides, and Tasks, including file/message metadata, event details, and content. It may also receive documents, private communications, audio/voice, precise location, account credentials, payment information, and health-related material when a user grants access. ([Product page](https://instinct.co/), [Privacy Policy §§ assistant data, Google Workspace, automatic collection](https://instinct.co/privacy-policy))

Its Terms describe a generalized connected-service model: Instinct may access, copy, collect, index, and exchange connected data and take actions in the connected service. ([Terms §3, “Connected services”](https://instinct.co/terms))

**Product meaning for Florence:** the tool catalog needs to cover the ordinary digital surfaces where personal work actually lives. Search, Gmail, and Calendar are only the first slice.

### 4. It is proactive and personalized

Instinct explicitly promises to notice threads a user has dropped and proactively call or text. The policy says it personalizes suggestions using both the context of current interactions and prior experience with the service. ([Product page](https://instinct.co/), [Privacy Policy § collection/use](https://instinct.co/privacy-policy))

This establishes outbound initiative, not merely scheduled reminders. It does not establish how Instinct selects a thread, how often it checks, or whether a human can inspect the trigger logic.

**Product meaning for Florence:** Florence needs a context-to-opportunity loop that continuously turns source changes and unfinished household work into a small number of timely, useful interventions. Repeating calendar facts is not proactivity; selecting the next consequential action is.

### 5. It maintains persistent context

Instinct says suggestions can use prior experience. Its Terms describe indexing connected-service inputs, and distinguish ordinary material from a product feature called the Vault. The privacy controls refer to deleting indexed external-source data from a Workspace surface. ([Privacy Policy §§ collection/use and external sources](https://instinct.co/privacy-policy), [Terms §3, “Connected services” and “Materials”](https://instinct.co/terms))

**Verified:** persistent indexed context and a Vault-like product surface exist in the published contract.

**Unknown:** the Vault schema, retrieval behavior, entity model, retention window, whether context is user-specific or shared, and how the assistant resolves contradictions.

**Product meaning for Florence:** a useful family assistant needs household-level continuity: people, responsibilities, preferences, places, routines, active threads, and past outcomes. The unit is the parental/household relationship where appropriate, while private adult conversations can remain distinct product surfaces.

### 6. It can operate across accounts and real-world services

The official materials describe connected-service access, credentials for third-party accounts, payment methods for purchases, sharing relevant information with a service, and engaging third parties. The service may facilitate rides, purchases, medical appointment booking, and other transactions. ([Privacy Policy §§ assistant data and disclosure](https://instinct.co/privacy-policy), [Terms §3, “Actions”](https://instinct.co/terms))

**Verified:** Instinct's contract allows broad third-party and transactional action.

**Inference:** Instinct likely combines APIs, browser/computer use, messaging, and phone calls to achieve this breadth. The product page says it uses a phone and computer as humans do, but it does not publish a tool list or execution architecture.

## Interaction and task lifecycle

The public evidence supports this user-level sequence:

1. **Ask naturally.** The user texts or calls with an ordinary objective. ([Product page](https://instinct.co/))
2. **Use context.** The assistant can use connected data and prior interactions to understand the objective. ([Privacy Policy](https://instinct.co/privacy-policy))
3. **Think, plan, and act.** It chooses actions and may work through connected services or third parties. ([Privacy Policy](https://instinct.co/privacy-policy), [Terms §3](https://instinct.co/terms))
4. **Follow up.** It can proactively text/call, provide personalized suggestions, and send status-related push/text/email messages. ([Product page](https://instinct.co/), [Terms §6](https://instinct.co/terms))
5. **Reach an outcome.** Public examples end in a ride or booked service; the Terms cover purchases and commitments. ([Product page](https://instinct.co/), [Terms §3](https://instinct.co/terms))

This strongly implies that at least some work continues beyond one model response. It does **not** prove the mechanics of a durable background-job system.

### What is not publicly documented

Instinct does not publish:

- acknowledgement or response-time targets;
- task states such as accepted, running, blocked, failed, or completed;
- progress-message cadence;
- retry, timeout, cancellation, steering, or recovery semantics;
- whether the assistant works while the user is offline;
- parallel-task behavior;
- how it decides when to ask a follow-up instead of researching an answer;
- provider-confirmed receipts or completion evidence;
- reaction/Tapback behavior, typing indicators, read receipts, inline replies, or group-chat behavior;
- a complete list of tools, integrations, or available actions; or
- the model, agent framework, browser runtime, or task scheduler it uses.

Florence should compete on the **user-level outcome contract**, not assume undocumented Instinct internals are requirements.

## Capability map

| Surface | Verified Instinct capability | What remains inference or unknown | Florence implication |
| --- | --- | --- | --- |
| Conversation | Text and phone calls; outbound calls/texts; mobile/macOS apps; push/text/email updates | Exact channels beyond SMS, thread model, reactions, latency | Messages-first should remain primary, with immediate acknowledgement, natural progress, and optional voice/phone later. |
| Web and computer | Uses a phone and computer like a person; can interact with connected services | Browser engine, site coverage, form handling, API-vs-UI selection | Add a general public-page reader and browser/computer worker; search alone cannot complete arbitrary errands. |
| Email and messaging | Email and messaging context; private communications; connected-service actions | Exact providers beyond Google, send/reply semantics, contacts | Complete Gmail read/draft/reply/send workflows and add scoped contacts plus external communication. |
| Productivity | Gmail, Calendar, Drive, Docs, Sheets, Slides, Tasks | Other suites; document-editing depth | Finish full Workspace breadth and treat documents/tasks as action sources, not just searchable files. |
| Location and local services | Location context; ride and handyman examples | Maps/routing/local-search providers; calling mechanics | Add maps, places, routes, travel time, local-business research, calling, and appointment/booking workflows. |
| Travel | Ride arrangement is explicit | Flights/hotels are not named in official product material; generalized computer/service use makes them plausible | Add flight status/routes/options, hotel search, itinerary synthesis, and disruption follow-through as first-class family jobs. |
| Commerce | Payment information and purchases are explicitly supported by the contract | Merchant coverage, approval flow, refunds, receipts | Build shopping, subscriptions, service quotes, and purchase completion after read-only research and communication are reliable. |
| Health/admin | Health information and medical appointment booking are explicit examples | Provider portals and form coverage | Support finding appointments, forms, documents, and follow-up; this is a meaningful family-assistant category. |
| Memory/context | Prior-experience personalization, indexed external data, Vault | Schema, retrieval, sharing, conflict resolution | Build one coherent household context model plus active threads and outcomes, rather than isolated per-tool summaries. |
| Proactivity | Dropped-thread follow-up, proactive calls/texts, personalized suggestions | Trigger model, frequency, controls | Rank unfinished work and source changes by usefulness; send a few actionable prompts, not repetitive facts. |
| Background work | Plans/completes tasks, engages third parties, sends delivery/status updates | Durable job implementation and lifecycle | Persist work across turns and processes; keep going until outcome, genuine blocker, or honest failure. |

## Florence's current gap

The current model-visible Florence read tools are `search_family_memory`, `read_source`, `research_public_web`, `search_gmail`, `read_gmail_attachment`, `list_calendars`, and `read_calendar_window` in [`apps/api/src/reasoner.ts`](../../apps/api/src/reasoner.ts). Florence also has application-owned reminders, monitors, family-calendar operations, messaging, and reactions, but the foreground assistant cannot yet wield the broad set of tools implied by Instinct's public contract.

The largest gap is therefore not model quality or Google query construction. It is that Florence still behaves primarily as an answer-and-classify loop, while Instinct is positioned as an objective-to-outcome operator.

## Prioritized capability program

### P0 — Make ordinary requests turn into work

1. **Dedicated public-data tools.** Add maps, places, routes, time zones, weather, flight status and alternatives, hotel/restaurant/local-service search, and current factual research. These are distinct tools/data sources, not one generic “Google Search” behavior.
2. **Complete reminder control.** Let a parent create, list, change, cancel, pause, resume, run, and recur reminders through ordinary conversation.
3. **Public-page and PDF reading.** Follow a parent-supplied or search-result link and extract the useful content instead of stopping at search snippets.
4. **Durable multi-step work using those tools.** Once real capabilities exist, let an actionable message continue in the background, survive process boundaries, accept follow-up steering, and deliver meaningful progress plus a terminal result.
5. **Communication and broader Workspace.** Add Gmail draft/reply/send, contact lookup, Drive, Docs, Sheets, Slides, Tasks, and document/attachment understanding so Florence can turn artifacts into actions.
6. **Browser/computer use.** Add a real worker that can navigate sites, compare options, fill forms, and handle workflows for services without an API. This is essential to the long tail of personal tasks.
7. **Useful fallback behavior.** If Florence lacks a specialized tool, it should research, open pages, use the browser, call or message when available, or ask for the missing constraint. It should never choose silence, and it should not stop merely because the request did not match a hard-coded intent.

### P1 — Make Florence feel employed, not operated

1. **Context-to-opportunity engine.** Continuously notice new or changed email, calendar, tasks, documents, and active commitments; reconcile sources; rank what matters; and offer the next action. “What's on the docket?” should produce a useful household brief, not a raw event recap.
2. **Active-thread memory.** Track unfinished work, who owns it, what was tried, what is awaited, deadlines, and the last confirmed outcome. This is different from storing static facts.
3. **Conversational work cues.** Acknowledge immediately, optionally react like a person when appropriate, report meaningful progress on longer tasks, ask only genuinely blocking questions, and state completion in outcome language.
4. **Local-service execution.** Research providers, call or message them, compare replies, schedule the appointment, and confirm the result. The handyman example is a compact benchmark for the whole work engine.
5. **Travel disruption handling.** Resolve a flight number without asking for information already derivable from it, check live status and route, find alternatives, incorporate calendar/household constraints, and keep working until the family has a usable plan.
6. **Voice/phone surface.** Let a parent call Florence and let Florence place calls for tasks where voice remains the only practical interface.

### P2 — Expand the ambient assistant surface

1. **Device and screen context** for user-directed tasks where the relevant information is outside connected APIs.
2. **Audio/voice understanding** beyond attached voice notes, including calls and meeting-like material.
3. **Location-aware help** for travel time, errands, pickups, and nearby services.
4. **Commerce and account workflows** for groceries, tickets, subscriptions, quotes, and purchases.
5. **An extensible connector catalog** so new personal services can be added without teaching the reasoner a one-off hard-coded branch each time.

## Concrete competitive acceptance scenarios

Florence should be able to pass these end-to-end product scenarios:

1. **Flight disruption:** “DL 747 is delayed tonight—find options.” Florence resolves the route/date, checks status, researches alternatives, asks only for a missing preference it cannot infer, and returns a ranked plan while continuing any requested follow-through.
2. **Household docket:** Florence examines the relevant 90-day source horizon and near-term future, identifies the few unfinished or time-sensitive family items, explains why each matters, and proposes or performs the next step without repeating stale reminders.
3. **Dropped school thread:** Florence notices an unanswered school email or unsubmitted form, extracts the required action and deadline, checks the calendar, and prompts the right adult with a ready action.
4. **Book a handyman:** Florence gathers the problem details, finds appropriate providers, contacts them, compares availability/price, schedules the chosen option, and reports the confirmed outcome.
5. **Appointment logistics:** Florence finds a provider, checks both adults' relevant availability and travel time, obtains options, books the selected appointment, adds the household event, and retains the preparation/follow-up thread.
6. **Family travel planning:** Florence reconciles confirmations, calendars, preferences, location, and current public information; catches inconsistencies; builds a plan; and keeps monitoring meaningful changes.
7. **Subscription or bill chore:** Florence finds the account context, researches the current terms/options, contacts the provider or navigates the workflow, and carries the task to a confirmed resolution.

These scenarios test the real competitive property: **one ordinary message produces sustained, resourceful work across whatever tools the objective requires.**

## What can and cannot be concluded about Instinct

### High-confidence conclusions

- Instinct's intended interface is text/call rather than a new task UI.
- It is designed to use broad connected and device context.
- It publicly promises proactive follow-up and personalized suggestions.
- It positions itself as planning and completing real-world tasks, including external actions.
- Its formal Google surface is substantially broader than Gmail and Calendar.
- Its product contract permits credentials, payments, purchases, and third-party commitments.

### Strong inferences

- Some tasks must continue asynchronously beyond a single response.
- Generalized browser/computer use is likely essential to its long-tail coverage.
- Persistent indexed context supports cross-turn personalization and proactive detection.
- The product likely uses multiple execution modes—APIs, device/browser interaction, and human communication—even though it does not publish them.

### Unknowns that should not be copied as assumptions

- Whether every user request actually triggers work.
- How quickly Instinct acknowledges or starts a task.
- Whether its background execution survives restarts or retries safely.
- Whether it exposes progress, blockers, cancellation, or exact completion evidence well.
- Whether it supports group/family context as a first-class unit.
- Whether it reacts to messages or otherwise imitates native human messaging behavior.
- Which non-Google integrations are productized rather than accessed through general computer use.
- How reliable any advertised workflow is in practice.

## Bottom line for product direction

Florence should not define itself as “family Gmail and Calendar intelligence with search.” It should be a **general personal operator optimized for a household**. The family specialization supplies unusually valuable context and prioritization—children, adults, schedules, responsibilities, routines, and shared outcomes—while the execution layer must still reach the broad digital and real-world surfaces that Instinct targets.

The immediate order is: maps/travel/current-data tools, complete reminder control, public-page and PDF reading, durable multi-step work using those real tools, communication and complete Workspace, then browser/computer use and proactive active-thread management. Ambient device context and commerce follow. That gets Florence closer to Instinct's actual product promise without pretending the sparse public material proves implementation details it does not disclose.
