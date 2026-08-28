# Instinct capability benchmark for Florence

Research date: 2026-08-28

Source access date: 2026-08-28

## Bottom line

Instinct's public differentiation is not a named search engine or a long connector list. It is an **outcome contract**:

1. a person delegates in ordinary language over text or a call;
2. the assistant recovers relevant context from connected applications and devices;
3. it uses a phone or computer and interacts with outside people or services;
4. it continues a multi-step job instead of returning instructions; and
5. it follows up with the result.

Instinct's own product page leads with dropped-thread follow-up, proactive calls or texts, an airport ride, and a booked handyman. Its service documents explicitly describe planning and completing tasks, engaging third parties, signing into accounts, taking actions in Google Workspace and other connected services, making purchases, and entering transactions on the user's behalf. ([Product page](https://instinct.co/), [Privacy Policy](https://instinct.co/privacy-policy), [Terms of Service](https://instinct.co/terms))

For Florence, “more than Google Search” therefore means **more ways to finish household work**, not a collection of interchangeable search APIs. Search, maps, weather, flights, public pages, and PDFs establish facts. Gmail, Calendar, files, browser/computer use, calls, and texts carry those facts into action. A competitive result ends with a provider-confirmed booking, purchase, cancellation, sent message, changed account state, or honest blocker—not a list of links and not a promise to keep working.

Florence already has the right household-specific foundation: two-parent Messages participation, a parental-unit knowledge model, 90-day Gmail and Calendar ingestion, reminders, durable work, structured public tools, broad Google Workspace operations, authenticated browser work, and outbound phone/SMS providers. The largest remaining product gap is not agent infrastructure. It is repeatedly proving **complete cross-channel jobs through their final provider result or receipt**.

## Evidence levels

This benchmark separates three kinds of evidence:

1. **Company or founder statement.** Instinct's site and service documents establish the product contract. Founder Noah Shinn's launch post supplies founder-reported early-use examples. These are first-party claims, not independent proof of general reliability.
2. **Direct user report.** Public posts by testers describe their own use. They are primary evidence of what that person says happened, but the underlying account state is not independently verified.
3. **Secondary reporting.** TechCrunch aggregates tester accounts and links to the original posts. It is useful for finding competitive rehearsals, but it is not evidence of Instinct's internal implementation or universal capability.

Instinct remains in private access, and its public site has no feature catalog, demo library, API documentation, public provider matrix, or published reliability results. No first-party public app-store listing or company interview exposing additional product behavior was located in this pass. The company page says access is through a waitlist or an existing member's invitation. ([Product page](https://instinct.co/)) No closed-product mechanism is inferred below.

The Privacy Policy and Terms are used only where they state product inputs or actions. This document is a capability benchmark, not a privacy, legal, or security review.

## The first-party product contract

| Product behavior | First-party evidence | Competitive meaning for Florence | Evidence boundary |
| --- | --- | --- | --- |
| Ordinary delegation | Instinct says there are “no new interfaces” and that a user can text or call it. ([Product page](https://instinct.co/)) | A parent should not need a task-builder, commands, pasted URLs, or provider names. | The page does not establish iMessage, WhatsApp, group-chat, reaction, or latency behavior. |
| Context from applications and devices | Instinct names email, messaging, screen, audio, location, applications, and devices. ([Product page](https://instinct.co/)) | Florence should derive known route, schedule, family, and account context before asking the parent. | No indexing horizon, retrieval quality, source-ranking method, or family-sharing model is published. |
| Multi-step completion | Instinct says the assistant thinks, plans, and acts; its policy says the service understands requests and plans and completes tasks, including engaging third parties. ([Privacy Policy](https://instinct.co/privacy-policy)) | A useful job can survive multiple tool calls and return after the outside work finishes. | No persistence, retry, steering, cancellation, progress, or restart-recovery contract is published. |
| Phone and computer operation | Instinct says its model is trained to use a phone and computer in the way people do. ([Product page](https://instinct.co/)) | Long-tail household work may require a website, app, call, text, or combination—not another bespoke search provider. | No browser vendor, vision/accessibility mechanism, telephony provider, or operation list is disclosed. |
| Connected-service actions | Instinct's Terms say it may access, index, exchange data with, interact with, and take actions on connected third-party websites, applications, and services. ([Terms, “Connected services”](https://instinct.co/terms)) | Read and write paths across ordinary services are part of the baseline. | No complete connector list or per-service action depth is published. |
| Google Workspace action | Instinct names Gmail, Calendar, Drive, Docs, Sheets, Slides, and Tasks and says the assistant may take actions using Workspace information. ([Privacy Policy, “Google Workspace”](https://instinct.co/privacy-policy)) | Email, calendar, documents, files, and tasks should compose into one job rather than act as isolated chatbots. | The company does not publish attachment behavior, editing depth, draft/send behavior, Contacts support, or outcome rates. |
| Authenticated account work | The policy gives third-party credentials as an example of information the assistant may use to sign into accounts. ([Privacy Policy](https://instinct.co/privacy-policy)) | Account portals and services that lack APIs remain valid work surfaces. | Assisted-login UX, MFA, session continuity, and takeover behavior are not documented. |
| Purchases and binding transactions | The Terms cover purchases on connected services, sharing the user's payment method with that service, and entering agreements, commitments, or transactions on the user's behalf. ([Terms, “Actions”](https://instinct.co/terms)) | Research is not the finish line for buying, booking, or cancelling. The provider state must actually change. | Merchant coverage, payment UX, confirmation behavior, reconciliation, and success rate are not published. |
| Third-party coordination | Company examples include arranging an airport ride and booking a handyman; the policy also uses ride booking and medical-appointment booking as examples. ([Product page](https://instinct.co/), [Privacy Policy](https://instinct.co/privacy-policy)) | Florence should be able to contact or operate the service that can complete the job. | The evidence does not say whether each example used a call, text, email, browser, marketplace, or native integration. |
| Proactive responsibility | Instinct says it follows up on dropped threads and can proactively call or text the user. ([Product page](https://instinct.co/)) | Unfinished work should reappear because its state changed or a response is needed—not as repeated generic reminders. | Triggering, deduplication, cadence, escalation, and response-time guarantees are not published. |
| Status delivery | The Terms contemplate push, local, text, picture, alert, email, and other status messages associated with the app. ([Terms, “Use of the app”](https://instinct.co/terms)) | A long job needs visible acknowledgement, meaningful progress, and a terminal result in the channel the parent uses. | The Terms do not say which status messages are assistant-authored or how quickly they arrive. |

## Outcome evidence beyond the company page

### Founder-reported early use

In his launch post, founder Noah Shinn describes Instinct as a personal agent with text/call access and phone/computer use. He reports that early users told the company they had planned cross-country road trips, bought weekly groceries and concert tickets, cancelled hundreds of dollars of subscriptions, and used Instinct while planning a wedding. ([Founder launch post](https://x.com/noahrshinn/status/2092691344456351744))

These are first-party reports **about** user activity, not public demonstrations. X's ordinary page returned HTTP 403 to the research crawler; X's official embed endpoint confirmed the author, date, and opening product description but truncated the long post, so the detailed examples were cross-checked against an indexed reproduction. The original post remains the claim's source.

### Direct tester reports

- A tester reports asking Instinct to book a local restaurant. After the first attempt failed, they say it retrieved a signup code from Gmail and used the code to sign into Resy in its own browser. ([Anita Kirkovska's post](https://x.com/anitakirkovska/status/2090907681091403863)) This is the strongest public cross-channel example because the claimed path joins a booking objective, Gmail retrieval, authenticated browser work, and an external reservation service. The public text does not independently prove the final reservation state.
- Jesse Middleton reports daily use for travel booking and rebooking, restaurant reservations, email follow-ups, CRM management, and data-room work. ([Jesse Middleton's post](https://x.com/srcasm/status/2091211911920542200)) This is direct user testimony about breadth; it does not provide per-job receipts or a measured completion rate.
- Sheel Mohnot describes the product as approachable enough to set up for a parent and says the experience “feels like magic.” ([Sheel Mohnot's post](https://x.com/pitdesi/status/2090579987778937159)) This supports the low-interface usability aspiration, not a specific completed job.

### Secondary reporting

TechCrunch reports tester use for appointments and restaurant reservations, rides, inbox cleanup, information organization, shopping, cheap-flight discovery, travel rebooking, email follow-ups, CRM work, and data-room work. It links the direct reports above, including the Gmail-code-assisted Resy path. ([TechCrunch, August 24, 2026](https://techcrunch.com/2026/08/24/instincts-powerful-ai-assistant-is-raising-privacy-and-security-concerns/))

Use those reports as competitive test cases, not as an implementation inventory. In particular, neither TechCrunch nor Instinct publishes which provider or action mechanism handled each task.

## What counts as a competitive outcome

Florence should distinguish four levels of progress:

| Level | Example | Count as finished? |
| --- | --- | --- |
| Found | “Here are three flights/restaurants/handymen.” | No. Useful research, but the parent still owns the job. |
| Prepared | “I filled the form / drafted the email / built the cart.” | Only when the parent asked for preparation rather than completion. |
| Acted | “I submitted the booking / sent the message / placed the call.” | Not yet when provider acceptance can still fail or the requested result is unknown. |
| Verified | “The airline issued itinerary X,” “Resy confirmed 7:00 PM,” “the subscription is cancelled and renewal is off,” or “the dentist confirmed Wednesday at 3:30.” | Yes. This is the Instinct-level bar Florence should target. |

This ladder is an inference from Instinct's public outcome contract, not a claim that Instinct itself exposes these states. It prevents Florence from calling a search result, queued request, connected phone call, or clicked button a completed family job.

## Florence's competitive rehearsals

These are the smallest end-to-end jobs directly connected to the public evidence. They are product journeys, not mandates for separate provider SDKs.

| Journey | Ordinary parent request | Required cross-channel work | Terminal proof |
| --- | --- | --- | --- |
| Travel disruption | “DL 747 is delayed tonight. Find and move her to the best Delta option.” | Resolve the flight identifier and route, inspect itinerary constraints, find live alternatives, carry the selected option into rebooking, and continue through any email/account step. | New provider itinerary or an exact blocker that prevented rebooking. |
| Reservation or appointment | “Find dinner near school after pickup and book it,” or “arrange Violet's dentist appointment Tuesday or Wednesday after 3.” | Combine family schedule and location, search availability, use browser/email verification or call the business, and continue until booked. | Reservation/appointment confirmation with date, time, party/person, and provider reference. |
| Inbox-to-action | “Handle this school email and send Jackson the form.” | Find the thread and attachments, retrieve related Drive material, draft/forward, send the exact provider draft, then monitor a reply if the objective requires one. | Sent message identity plus delivery/reply state relevant to the request. |
| Household purchase | “Order our usual groceries for Sunday morning.” | Recover preferences and prior context, build the order, handle substitutions or one genuinely missing choice, complete checkout, and track fulfillment. | Order number, charged total, delivery/pickup window, and current provider state. |
| Subscription cancellation | “Cancel the service we no longer use.” | Identify the exact account, use the authenticated portal/email/call path, complete cancellation, and verify renewal state. | Provider cancellation confirmation or receipt and the effective end date. |
| Outside-person coordination | “Call the camp and find out whether Theo can switch weeks.” | Find the right contact, call or text with the complete known constraints, inspect the reply/transcript, and follow through on any agreed account change. | Transcript/message-backed answer and any provider-confirmed change. |
| Dropped-thread follow-up | “Stay on top of the school application.” | Reconcile the inbox, docket, family calendar, prior messages, and active work; contact the right party when needed; surface only a changed or actionable state. | A real reply, deadline, completed requirement, or one specific decision the family must make. |

The strongest available path may be a structured provider, authenticated browser, email, call, or text. The benchmark is the verified result, not tool count.

## What Instinct does not document publicly

No first-party public source reviewed establishes:

- a dedicated Google Search tool or a complete web/search provider inventory;
- dedicated maps, weather, flight-status, hotel, restaurant, grocery, ticketing, healthcare, home-service, ride, or subscription integrations;
- whether long-tail work uses browser control, native app control, APIs, people, or a mixture;
- a public browser action set, file download/upload surface, or phone/SMS operation set;
- task persistence, retries, timeouts, steering, cancellation, restart recovery, or an acknowledgement/completion SLA;
- a human-operator fallback or assisted-login/takeover flow;
- reactions, Tapbacks, inline replies, typing indicators, or read receipts;
- group or household accounts, shared-family memory, or multi-adult coordination; or
- measured success, latency, or reconciliation rates for any advertised or reported outcome.

Florence should not copy an imagined closed-product architecture. It should meet the visible product bar through its existing household model and durable-work path.

## Florence versus the public Instinct bar

This is implemented repository behavior as of the research date, not planned work.

| Lived capability | Florence today | Instinct public bar | Material Florence gap |
| --- | --- | --- | --- |
| Parent interaction | Private-parent and family-group Messages, text, voice notes, images/PDFs, inline context, reactions, typing, delivery state, and immediate work cues. ([Linq adapter](../../packages/linq/src/index.ts), [turn orchestration](../../apps/api/src/florence.ts)) | Text or call with no new task UI; proactive call/text. | No live inbound call into Florence; response and progress cadence still need two-phone product tuning. |
| Household context | Two-adult household, represented children, source-linked retained context, 90-day Gmail/Calendar ingestion, and a reconciled docket. ([Store](../../packages/database/src/store.ts), [turn orchestration](../../apps/api/src/florence.ts)) | Context from applications/devices and prior experience. | Retrieval and ranking must consistently turn broad history into the best next action without repetition. Instinct does not publicly claim Florence's shared-family model. |
| Public facts and travel | Web research, exact public HTML/PDF reading, maps/places/routes/time zones, U.S. weather/alerts, and flight alternatives. ([Reasoner](../../apps/api/src/reasoner.ts), [page reader](../../apps/api/src/public-page.ts), [maps](../../apps/api/src/maps.ts), [weather](../../apps/api/src/weather.ts), [flights](../../apps/api/src/flights.ts)) | Broad context feeding real work. | Carry selected travel/service options into provider-confirmed execution instead of ending at comparison. |
| Google work | Gmail reads/search/send/reply/labels and exact drafts/forwards/attachments; Contacts; personal/family Calendar; Drive, Docs, Sheets, Slides, and Tasks. Explicit group work routes through the initiating parent's connection. ([Google adapter](../../packages/google/src/workspace.ts), [reasoner tools](../../apps/api/src/reasoner.ts)) | Gmail, Calendar, Drive, Docs, Sheets, Slides, and Tasks context and actions. | General file download/edit/upload across non-Google sites and cross-channel reply/receipt follow-through remain. |
| Browser/computer work | Durable authenticated browser sessions, current-page snapshots/actions, exact initiating-message image/PDF upload, screenshot support, and parent takeover for sign-in/MFA. ([Browser adapter](../../apps/api/src/browser.ts), [turn orchestration](../../apps/api/src/florence.ts)) | Uses a phone and computer; may sign into connected services and transact. | Add site downloads and cross-provider file continuation; prove more binding real-site jobs through provider confirmation and retain the resulting reference/receipt. |
| Calls and texts | Durable Bland conversational outbound calls plus Twilio SMS/inbox and spoken calls. ([Telephony adapter](../../apps/api/src/telephony.ts), [turn orchestration](../../apps/api/src/florence.ts)) | Text/call interface, proactive contact, and third-party engagement. | No live inbound parent call; configured outbound paths still need live-account rehearsals through an actual booked or changed outcome. |
| Durable/proactive work | Checkpoints, retry, steering, cancellation, progress, reminders, finite monitors, and terminal delivery through the existing due-work path. ([Reasoner](../../apps/api/src/reasoner.ts), [store](../../packages/database/src/store.ts)) | Thinks/plans/acts, follows dropped threads, and proactively contacts the user. | Better evidence ranking, response tracking, deduplication, and cross-channel continuation—not another scheduler or task runtime. |
| External outcomes | Google actions, browser actions, sent email, calls, and texts can execute and be read back. | Rides, appointments, purchases, bookings, and transactions. | One provider-confirmed journey in each important family archetype, starting with reservation/rebooking/cancellation and final receipt capture. |

## Priority after browser, Gmail composition, and outbound communication

1. **Provider-confirmed completion:** run one real reservation, rebooking, purchase, or cancellation through the final confirmation/receipt.
2. **Cross-channel continuation:** begin with Gmail or Calendar, use browser/call/text as needed, and reconcile the provider result back into the same family task.
3. **Files across external sites:** download a form or receipt, understand or populate it, upload it where required, and retain the resulting provider reference.
4. **Travel execution:** turn itinerary extraction, flight status, alternative search, hotel/ground transport, and rebooking into one finished disruption journey.
5. **Live inbound voice:** let a parent call Florence to create or steer the same durable work used by Messages.
6. **Location-aware errands:** use current or explicitly shared location when it materially changes the best nearby action.

These priorities deliberately deepen current Florence modules. They do not justify a new connector marketplace, memory system, task runtime, scheduler, or generic action framework.

## Source register

| Source and evidence level | Exact evidence used | Limitation |
| --- | --- | --- |
| [Instinct product page](https://instinct.co/) — company statement | Applications/devices; email, messaging, screen, audio, location; phone/computer use; text/call interface; dropped-thread follow-up; proactive call/text; airport ride; handyman; private access. | Marketing claim with no provider list, mechanism, demonstration, or reliability data. |
| [Instinct Privacy Policy](https://instinct.co/privacy-policy) — company statement | Understand requests; plan and complete tasks; engage third parties; think/plan/act; account sign-in; ride and medical-appointment examples; Google Workspace context and actions. | Service scope, not operation depth or completion rate. Only capability clauses are used here. |
| [Instinct Terms of Service](https://instinct.co/terms) — company statement | Connected-service indexing/interactions/actions; purchases; agreements/commitments/transactions; Mac/mobile apps; push/text/picture/alert/email status surfaces. | Contract breadth does not prove that every action is enabled for every user or provider. |
| [Noah Shinn launch post](https://x.com/noahrshinn/status/2092691344456351744) — founder statement | Text/call and phone/computer positioning; founder-reported road trips, groceries, concert tickets, subscription cancellation, and wedding planning. | Early-user reports, not demos or measured results; long X text had an access limitation described above. |
| [Anita Kirkovska](https://x.com/anitakirkovska/status/2090907681091403863) — direct tester report | Restaurant objective; Gmail signup-code retrieval; Resy browser sign-in after an initially failed attempt. | First-person account; final reservation state and general reliability are not independently established. |
| [Jesse Middleton](https://x.com/srcasm/status/2091211911920542200) — direct tester report | Travel booking/rebooking, reservations, email follow-up, CRM, and data-room work. | Breadth testimony without per-job provider receipts or success measurements. |
| [Sheel Mohnot](https://x.com/pitdesi/status/2090579987778937159) — direct tester report | Low-interface experience described as approachable and suitable for setting up for a parent. | Subjective usability report, not a capability demonstration. |
| [TechCrunch](https://techcrunch.com/2026/08/24/instincts-powerful-ai-assistant-is-raising-privacy-and-security-concerns/) — secondary reporting | Aggregated tester workflows and links to the direct posts: appointments, reservations, rides, inbox work, shopping, cheap flights, rebooking, follow-ups, CRM, and data-room work. | Reporting about a private beta; useful for rehearsal selection, not an internal tool inventory or reliability claim. |

No inferred closed-product internals are treated as evidence. The benchmark is the user-visible job and its verified terminal state.
