# Instinct capability benchmark for Florence

Research date: 2026-08-28

## Decision summary

Instinct's public product is not positioned as a Google assistant with web search. It is positioned as a **general personal operator** that:

1. is reached through text or a phone call rather than a new task interface;
2. uses broad context from communications, productivity software, devices, location, and prior experience;
3. plans and acts across connected services, a phone, a computer, and third parties;
4. follows up proactively on work the user has dropped; and
5. carries ordinary objectives through to external outcomes such as a ride, service appointment, purchase, or cancellation.

The founder's launch post adds concrete breadth to the official examples: early users reportedly planned cross-country road trips, bought groceries and concert tickets, cancelled subscriptions, and planned a wedding. ([Noah Shinn's launch post](https://x.com/noahrshinn/status/2092691344456351744))

The main Florence gap is therefore not one missing search provider. It is the combination of **tool breadth plus sustained execution**: turn an ordinary family objective into work, use the right digital or real-world surface, keep the thread alive, and return a useful outcome. Florence's household context can be a stronger prioritization layer than Instinct's individual context, but it should not constrain the execution layer to Gmail, Calendar, and public facts.

## Evidence boundary

This note uses first-party public material only:

- Instinct's [official product page](https://instinct.co/);
- founder Noah Shinn's [public launch post](https://x.com/noahrshinn/status/2092691344456351744);
- the [Privacy Policy](https://instinct.co/privacy-policy), revised August 26, 2026; and
- the [Terms of Service](https://instinct.co/terms), revised August 26, 2026.

No closed-beta account, private output, or client bundle was used. No public help center, integration catalog, release notes, detailed demo library, or task-lifecycle documentation could be located on Instinct's official domains. The policy and Terms expose capability surfaces, but they do not prove that every permitted action is available to every user or works reliably.

Evidence labels in this note mean:

- **Product claim:** Instinct advertises the behavior directly.
- **Founder-reported use:** Instinct's founder says early users performed the task; it is still not an independent reliability test.
- **Published contract:** Instinct's official policy or Terms names the data, integration, or action surface.
- **Inference:** the public evidence supports a likely product behavior but does not document it.
- **Unknown:** the public evidence does not establish the behavior.

## What the product says it actually does

| User-visible promise or example | Evidence | What it establishes | What it does not establish |
| --- | --- | --- | --- |
| Text or call the assistant; no new interface | Product claim on the [product page](https://instinct.co/) and founder's [launch post](https://x.com/noahrshinn/status/2092691344456351744) | Conversation is the primary ingress, including voice calls | Response latency, carrier/channel coverage, group threads, or native messaging details |
| Use a phone and computer as people do | Product claim on the [product page](https://instinct.co/) | The intended action surface is broader than fixed API integrations | Which apps/sites work, whether this is API, UI automation, or a mixture, and reliability |
| Follow up on dropped threads and proactively call or text | Product claim on the [product page](https://instinct.co/) | Outbound initiative and unfinished-thread recovery are core product behavior | Trigger logic, frequency, controls, or whether follow-up is durable across failures |
| Arrange an airport ride and book a handyman | Product claim on the [product page](https://instinct.co/) | Tasks are framed as completed real-world outcomes, including third-party coordination | Provider coverage, confirmation flow, payment handling, or success rate |
| Plan road trips and a wedding; buy groceries and concert tickets; cancel subscriptions | Founder-reported early use in the [launch post](https://x.com/noahrshinn/status/2092691344456351744) | The intended breadth includes travel, planning, commerce, recurring-account chores, and multi-step projects | Exact workflows, number of successful users, human intervention, or repeatability |
| Think, plan, and act to complete everyday tasks | Published contract in the [Privacy Policy](https://instinct.co/privacy-policy) | The service contract is action-oriented, not answer-only | Internal planner, model, scheduler, retry behavior, or task-state representation |
| Access and take actions across connected services | Published contract in the [Terms, Connected services and Actions](https://instinct.co/terms) | A generalized connector/action model exists; actions may include purchases and third-party commitments | The available connector catalog, per-connector operations, or default confirmation policy |
| Use Gmail, Calendar, Drive, Docs, Sheets, Slides, and Tasks | Published contract in the [Privacy Policy, Google Workspace](https://instinct.co/privacy-policy) | Instinct's named Google surface is the full productivity suite, not just inbox and calendar | Editing depth, search quality, or whether every action is enabled |
| Book rides or medical appointments using account credentials, payments, or relevant personal context | Published contract in the [Privacy Policy](https://instinct.co/privacy-policy) | The product is designed for authenticated and transactional workflows | Merchant/provider coverage, approval UX, or actual booking reliability |
| Send status updates through push, local, text, picture, alert, or email messages | Published contract in the [Terms, Use of the app](https://instinct.co/terms) | The product has multiple outbound delivery surfaces suitable for status and completion | Which channels the assistant itself uses for task progress or the cadence of updates |
| Personalize from current context and prior experience; index connected data; expose a Vault | Published contract in the [Privacy Policy](https://instinct.co/privacy-policy) and [Terms, Materials](https://instinct.co/terms) | Persistent context and a named memory-like surface are part of the product | Memory schema, retrieval quality, sharing model, or contradiction resolution |

## Published tool and integration surface

This is the broadest inventory supportable from first-party material. “Named” means Instinct identifies the surface; it does not mean every operation has been publicly demonstrated.

| Surface | Publicly named capability | Strength | Representative assistant job |
| --- | --- | --- | --- |
| Text and phone | Inbound text/call; proactive outbound text/call | Product claim | Ask naturally, get a follow-up without opening an app |
| Mac and mobile apps | Related macOS and mobile applications | Published contract | Maintain an assistant presence across devices |
| Gmail | Message content/metadata and actions through Workspace | Published contract | Find a thread, extract a requirement, draft or take the next action |
| Calendar | Event details and actions through Workspace | Published contract | Reconcile plans, schedule, update, and follow through |
| Drive, Docs, Sheets, Slides | File metadata/content and Workspace actions | Published contract | Read a document or spreadsheet and turn it into work |
| Google Tasks | Task data and Workspace actions | Published contract | Capture, find, update, or complete a task |
| Messaging and private communications | Messaging context is named on the product page and in policy | Product claim / published contract | Recover decisions and unfinished handoffs from conversations |
| Screen and general computer use | Screen context and phone/computer operation “as humans do” | Product claim | Navigate a long-tail service that lacks a dedicated connector |
| Audio and voice | Audio/voice context and phone calls | Product claim / published contract | Speak to the assistant or use audio as task context |
| Location | Location context, optional precise location | Product claim / published contract | Route, nearby service, pickup, errand, or travel-time work |
| Generic connected services | Interact, exchange data, index, and take actions | Published contract | Work across personal services beyond Google |
| Credentials | Sign into third-party accounts on a user's behalf | Published contract | Complete an authenticated portal workflow |
| Payments and commerce | Share payment method, make purchases, enter transactions | Published contract | Buy tickets/groceries or pay for a service |
| Third-party coordination | Engage third parties; rides and handyman are explicit | Product claim / published contract | Call, message, compare, schedule, and confirm |
| Health administration | Medical appointment and provider-email examples | Published contract | Book an appointment and summarize preparation material |
| Persistent context / Vault | Prior-experience personalization, indexed connected data, Vault | Published contract | Remember preferences, active work, and relevant history |

No first-party public source identifies a complete provider list beyond Google Workspace. Flights, hotels, maps, restaurants, ticketing merchants, grocery services, subscription vendors, healthcare portals, or home-service marketplaces should therefore be treated as **task categories**, not verified dedicated Instinct integrations. General phone/computer use or connected services may cover them, but the mechanism is unknown.

## User-facing task execution model

The public promise supports the following product-level loop:

1. **Receive an objective in a familiar channel.** The user texts or calls with an ordinary request. ([Product page](https://instinct.co/))
2. **Recover relevant context.** Instinct may use communications, Workspace, documents, devices, location, and prior experience. ([Product page](https://instinct.co/), [Privacy Policy](https://instinct.co/privacy-policy))
3. **Plan the work.** The policy explicitly describes understanding requests and planning and completing tasks. ([Privacy Policy](https://instinct.co/privacy-policy))
4. **Act across the required surfaces.** It may operate connected services, a phone or computer, share information with third parties, purchase, or enter a commitment. ([Product page](https://instinct.co/), [Terms](https://instinct.co/terms))
5. **Continue the thread.** It advertises proactive follow-up and has multiple outbound status channels. ([Product page](https://instinct.co/), [Terms](https://instinct.co/terms))
6. **Report an outcome.** The public examples end in an arranged ride, booked service, purchase, cancellation, or completed plan—not a promise to look later. ([Product page](https://instinct.co/), [founder launch post](https://x.com/noahrshinn/status/2092691344456351744))

This is an **observable outcome contract**, not proof of Instinct's runtime architecture. It strongly suggests that some work outlives one model response, but no first-party source documents durable jobs, queues, recovery after restart, retries, cancellation, steering, parallel execution, provider receipts, or exactly-once completion.

## Communication and “feels like a person” behavior

### Verified publicly

- The user can text or call instead of learning a task UI. ([Product page](https://instinct.co/))
- Instinct can proactively call or text about a dropped thread. ([Product page](https://instinct.co/))
- Its published app contract includes push, local, text, picture, alert, and email delivery/status messages. ([Terms](https://instinct.co/terms))
- The product language is outcome-oriented: arranging, booking, buying, cancelling, and planning.

### Not established publicly

- reactions or Tapbacks;
- typing indicators or read receipts;
- inline replies;
- iMessage versus SMS behavior;
- family/group-chat participation;
- acknowledgment latency;
- progress-message cadence; or
- how it communicates a blocker, failure, or cancellation.

Florence should still treat native conversational presence as a product requirement. Immediate acknowledgment, an appropriate lightweight reaction, useful progress only when work is long-running, and a clear terminal message make the assistant feel present. That is a Florence design conclusion; it is **not** a claim that Instinct publicly demonstrates reactions.

## Memory and context model

Instinct publicly establishes three layers:

1. **Current interaction context:** information in the present request and connected applications. ([Privacy Policy](https://instinct.co/privacy-policy))
2. **Indexed external context:** data copied/indexed from connected services. ([Terms](https://instinct.co/terms))
3. **Persistent personalized context:** suggestions based on prior experience plus a named Vault surface. ([Privacy Policy](https://instinct.co/privacy-policy), [Terms](https://instinct.co/terms))

Public material does not establish the entity model, retention horizon, retrieval strategy, active-task representation, source provenance shown to users, conflict handling, or whether any context is shared among people. Instinct's published contract is individual-account oriented. That leaves Florence a meaningful differentiation: one coherent household/parental-unit model for shared responsibilities and outcomes, with adult-private source context kept distinct where needed.

For Florence, “memory” should include more than retained facts. It needs active threads: objective, owner, household relevance, evidence, next step, deadline, attempts, blockers, and confirmed outcome. Otherwise a 90-day source horizon still condenses into isolated summaries instead of becoming useful when a later request or event makes old context relevant.

## Florence capability-gap matrix

The Florence baseline at this research date already includes Messages participation and reactions, Gmail/Calendar/source reading, household memory, public-web research, maps primitives, one-shot reminders, finite monitors, family-calendar operations, and an emerging flight-search path. The matrix focuses on the next user-visible gaps, not generic platform or safety infrastructure.

| User job | Instinct first-party evidence | Florence gap | Concrete Florence capability | Acceptance check |
| --- | --- | --- | --- | --- |
| Give Florence an objective and let it keep working | Think/plan/act, proactive follow-up, outcome-shaped examples | Foreground answers and application-owned follow-ups do not yet form one general sustained-work loop | Durable objective-to-outcome task that can use several capabilities, preserve state, accept steering, and return a terminal result | “Find and book a handyman” progresses past research, asks only a genuine blocker, survives a process restart, and ends with a confirmed result or honest failure |
| Use all ordinary Google work surfaces | Gmail, Calendar, Drive, Docs, Sheets, Slides, Tasks are named | Florence's usable Workspace surface centers on Gmail and Calendar | Read/search and appropriate write actions for Drive, Docs, Sheets, Slides, and Tasks | A school spreadsheet or Drive PDF becomes the right task/calendar action without manual copy/paste |
| Open arbitrary sites and finish long-tail errands | Uses a phone/computer like a person; generalized connected services | Search and specialized APIs cannot complete arbitrary portals and forms | Public-page/PDF reader first, then a bounded browser/computer worker that can navigate, compare, fill, and submit when authorized | A provider with no API can be researched, contacted, and scheduled with visible progress and a final receipt |
| Communicate outside the family thread | Email/messaging context, third-party engagement, proactive phone/text | Florence cannot generally draft/send email, look up contacts, call, or manage external replies | Contacts plus Gmail draft/reply/send; outbound SMS/email/calling jobs; reply tracking | “Email the camp for availability and let me know” produces the sent message and later reconciles the reply |
| Plan around place and movement | Location, airport ride, phone/computer use | Maps primitives are emerging, but they are not yet an end-to-end logistics assistant | Places, routes, live travel time, transit/driving tradeoffs, nearby services, pickup coordination | “Can I make pickup after this appointment?” resolves both locations/times and proposes the usable plan |
| Resolve travel disruption | Road-trip planning is founder-reported; airport ride is advertised | Flight lookup is emerging; itinerary and disruption follow-through remain incomplete | Flight status/options, airport/time-zone reasoning, itinerary extraction, hotel/ground transport, monitoring and rebooking assistance | A flight number resolves route/status without asking derivable questions, returns ranked alternatives, and continues requested follow-through |
| Complete commerce and account chores | Groceries, concert tickets, subscription cancellation are founder-reported; purchases are contractually supported | Florence can research but not generally complete merchant/account workflows | Product/service comparison, shopping cart or ticket flow, subscription discovery/cancellation, receipts and outcome tracking | “Cancel this trial before renewal” finds the account path, completes or reaches the true blocker, and confirms the external state |
| Coordinate local services | Handyman booking is an explicit product example | No general provider outreach, quote comparison, scheduling, and confirmation loop | Provider discovery, calls/messages, structured quote comparison, appointment booking | Florence contacts multiple suitable providers, compares replies, books the chosen slot, and updates the household |
| Handle healthcare and administrative work | Medical booking and provider-email processing are explicit policy examples | Source reading exists, but portal/form/appointment completion is not general | Provider search, appointment options, document/form extraction, portal/browser work, preparation/follow-up | A referral email becomes a booked option plus the right preparation checklist and calendar entry |
| Control reminders and tasks conversationally | Google Tasks is named; proactivity is advertised | One-shot creation exists, but full list/edit/cancel/pause/resume/recurrence is incomplete | Complete reminder and task control with natural corrections and ownership | “Move that reminder to tomorrow and make it weekly” updates the existing item rather than creating duplicates |
| Recover dropped household threads | Dropped-thread follow-up and prior-experience personalization are explicit | Finite monitors exist, but there is no general active-thread portfolio across every tool | Household active-thread memory plus source-change reconciliation, ranking, and useful next-action suggestions | A 90-day-old school email resurfaces only when a new deadline or later request makes it relevant, with the next action ready |
| Feel present in Messages | Text/call and proactive outbound communication are verified; reactions are unknown | Florence has reactions but must consistently pair them with real work and terminal feedback | Immediate receipt cue, natural concise acknowledgment, bounded meaningful progress, and exactly one useful completion/failure message | Every actionable message gets visible feedback immediately; longer work never goes silent and never spams progress |
| Talk instead of type | Calling and audio/voice context are explicit | Voice-note understanding is narrower than a live assistant call surface | Inbound voice/call interaction and outbound calling for practical workflows | A parent can call with an objective and later receive the same coherent task state in Messages |

## Priority conclusions for Florence

### P0: Make work real

1. **General sustained-work loop.** A broad tool catalog without durable objective state still produces “I’ll look into it” and then silence. Every actionable turn should start work, request one real blocker, or state a genuine capability limit.
2. **Complete the high-frequency assistant surfaces.** Finish reminder control, public-page/PDF reading, broader Workspace, contacts/email actions, and travel/current-data tools.
3. **Add long-tail reach.** General browser/computer use and external communication are what let a personal assistant handle the services that never receive a dedicated API integration.
4. **Use an outcome-shaped terminal contract.** “Booked,” “sent,” “cancelled,” “found these two live options,” or “blocked because X” is more useful than “I can help.”

### P1: Make Florence feel employed

1. **Active-thread memory.** Retain what remains unresolved, what was tried, who owns it, what changed, and how completion will be verified.
2. **Context-to-opportunity proactivity.** Reconcile the 90-day source horizon with current events and surface only consequential next actions. Do not repeatedly announce the same calendar fact.
3. **Person-like conversational cues.** React or acknowledge immediately, then provide only meaningful progress and one terminal result. The reaction is presence, not a substitute for work.
4. **External coordination.** Email, message, or call providers; track replies; compare options; and close the loop.

### P2: Expand the ambient and transactional surface

1. live voice and outbound phone calls;
2. location-aware errands and pickups;
3. commerce, tickets, groceries, subscriptions, and account workflows;
4. healthcare/admin forms and portals; and
5. an extensible connector catalog for personal services.

## Competitive acceptance scenarios

These scenarios test breadth and execution together:

1. **Flight disruption:** “DL 747 is delayed tonight—find options.” Florence resolves route/date/status, considers household constraints, returns ranked alternatives, and keeps working if asked to rebook or monitor.
2. **Dropped school thread:** Florence connects an older school email to a new deadline, extracts the required action, opens the attachment, and prompts the right adult with a ready next step.
3. **Handyman:** Florence gathers the problem details, finds providers, contacts them, compares price/availability, books the selected slot, and reports the confirmed outcome.
4. **Weekly groceries:** Florence uses preferences and the current household plan, prepares the order, resolves substitutions, completes the authorized purchase, and retains the useful preference changes.
5. **Subscription cleanup:** Florence identifies a renewal, finds the account workflow, cancels or reaches the real blocker, and verifies the changed external state.
6. **Appointment administration:** Florence finds options, reconciles adult availability, books the chosen appointment, adds the household event, and retains preparation/follow-up work.
7. **Wedding or family-trip project:** Florence holds a multi-week objective with subthreads across email, documents, vendors, travel, calendar, and payments without losing ownership or flooding the family chat.

## What remains unknown about Instinct

Public first-party sources do not establish:

- whether every ordinary request actually launches work;
- acknowledgment or completion latency;
- reactions, group threads, or other native messaging behavior;
- the exact non-Google integration/provider catalog;
- browser-versus-API execution choices;
- task durability, retries, timeout, cancellation, steering, or parallelism;
- progress cadence or provider-confirmed completion evidence;
- family/household shared context; or
- reliability of any advertised or founder-reported workflow.

Florence should copy the **product bar**—natural access, broad context, resourceful action, proactive continuity, and completed outcomes—without pretending sparse public claims reveal Instinct's implementation.

## Bottom line

Instinct's clearest competitive idea is not any one integration. It is that the assistant can use whatever ordinary surface a personal objective requires and remain responsible for the thread until something useful happens. Florence should become the household-specialized version of that general operator: broader than Google and web search, better at shared family context, and visibly at work from immediate acknowledgment through verified outcome.
