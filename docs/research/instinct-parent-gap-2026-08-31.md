# Instinct's current parent bar and Florence's parent wedge

Research date: **August 31, 2026**  
Scope: the newest public Instinct evidence available through August 31, with emphasis on onboarding, messaging, background work, memory, Google Workspace, real-world actions, household behavior, privacy, and distribution.

## Executive conclusion

Instinct is already doing many of the errands people would casually call a “family assistant”: users report that it fills school forms, shops from a photograph of a fridge, emails a doctor's office in another language, reconciles a child's seat across family flight bookings, checks in for flights, places groceries, and buys travel insurance. Florence therefore cannot differentiate by publishing a longer list of parent task categories.

The stronger wedge is structural:

> **Instinct is a capable agent for one person. Florence can be the capable agent for a parental unit.**

That means Florence should match Instinct's natural conversation, persistence, memory, browser/phone execution, media handling, and provider-confirmed outcomes, then be materially better at:

1. two adults with independent identity, authority, sources, and private threads;
2. represented children with an explicit parent-authorized data contract;
3. audience-aware memory that distinguishes personal evidence from household truth;
4. one shared family calendar and one exact coordination thread;
5. proactive work selected from the household's current obligations, not generic engagement;
6. inspectable provenance, corrections, revocation, and deletion for family and child information; and
7. an individual-first path that is fully useful to one parent and can add another caregiver later without changing products.

This is an **inference and product recommendation**, grounded in the evidence below. Instinct has not published a household or multi-adult model.

## Strategic update from the August 31 evidence

The newest evidence supports Rebecca Kaden's framing, with one important refinement:

> **Multiplayer should be Florence's product architecture; external calling should be one of the execution surfaces that makes multiplayer useful.**

For Florence, “multiplayer” should not initially mean autonomous agents exchanging unrestricted context through a general protocol. It should mean:

- every parent begins with a fully useful private Florence;
- another parent or caregiver can join later with independent identity and consent;
- private evidence remains private while the minimum useful conclusion can cross into household work;
- Florence can route one decision to the adult who knows or can act;
- the family group receives one coordinated conclusion; and
- changes propagate through the rest of family life instead of ending at the first completed click.

This keeps single-player as the starting state while making household coordination the differentiated destination. Calling then matters because many high-friction parent obligations still end at a school office, pediatric practice, camp, coach, insurer, repair shop, or local business that requires a phone conversation.

## Method and evidence levels

This pass used only:

- Instinct's current official product, privacy, terms, and acceptable-use pages;
- public posts by Instinct founder Noah Shinn; and
- direct first-person reports from people describing their own Instinct use.

X was inspected through a signed-in, read-only browser session. No private messages, cookies, credential stores, or account settings were inspected, and no likes, follows, replies, or other account changes were made.

Evidence labels mean:

- **Company statement:** Instinct's own current public page or legal contract.
- **Founder statement:** a public claim by founder Noah Shinn; useful first-party evidence, but not an independent measurement.
- **Firsthand user report:** what a named tester says happened in their own account; strong evidence of a visible workflow, but not a reliability rate or audited implementation.
- **Inference:** a Florence product conclusion, not a claim about Instinct internals.

No public product documentation, API reference, release-note feed, reliability dashboard, or complete connector catalog was located. That absence should not be interpreted as proof that the private product lacks a capability.

## What changed or became newly visible after the August 22 snapshot

### 1. Instinct has a concrete payment approval path

On August 28, Noah Shinn announced an Instinct integration with Stripe Link. He said that when Instinct wants to purchase something for a Link user, it requests a one-time-use card authorized for the relevant amount; the user can explicitly review and approve the amount, and the merchant does not receive the user's raw payment details. He also said Instinct users who make purchases spend more than $1,300 per month on average through it, and named trips, weekly groceries, household essentials, appointments, repairs, and price shopping as use cases. ([Founder payment announcement](https://x.com/noahrshinn/status/2093368510449877180), [one-time-card explanation](https://x.com/noahrshinn/status/2093368511003574730))

**Evidence:** this establishes a first-party payment mechanism and an explicit amount-approval step. It does not establish the percentage of purchases that finish successfully, refund handling, family-member approval, budget rules, receipt reconciliation, or dispute handling.

**Florence implication:** Instinct has improved the parent-relevant buying bar. Florence should not describe a browser click or a candidate price as a purchase. When purchasing becomes a Florence priority, the lived contract should include the responsible adult, exact amount, merchant, item or service, cancellation/refund terms when material, provider receipt, and the household-visible conclusion. A general payment control plane is still unnecessary before two current family jobs require it.

### 2. Latency and scaling are being treated as product behavior

On August 29, Shinn acknowledged slower responses during rapidly increasing usage and promised a two-to-three-times speed improvement. On August 30, he said Instinct was slightly more than twice as fast and that the long-tail latency causing some complex tasks to take several minutes had been dramatically reduced. ([August 29 scaling update](https://x.com/noahrshinn/status/2093905019108213059), [August 30 latency update](https://x.com/noahrshinn/status/2094089088823402531))

**Evidence:** these are founder-reported changes, not independent latency measurements or a service-level guarantee.

**Florence implication:** speed is part of perceived capability. Florence needs immediate, natural acknowledgement; current-turn completion for quick reads; meaningful progress only when real work is continuing; and a terminal result that appears as soon as provider truth exists. The goal is not to make every outside service instant, but to prevent the family from wondering whether Florence heard, stalled, or finished.

### 3. The strongest new parent examples join memory, media, browser work, and artifacts

On August 30, Dan Peguine reported that Instinct:

- checked him into a flight and sent actual Apple Wallet boarding passes;
- ordered groceries on a Portuguese grocery site;
- ported a mobile number;
- filled Google Forms for his children's schools; and
- found and bought a travel-insurance policy.

In a follow-up on August 31, he added that it found discounts and ordered vitamins and protein powder, used a photograph of his fridge to adjust an existing grocery order, and emailed a doctor's office in Portuguese to reschedule an appointment. He characterized Instinct as having strong personality and memory and as willing to figure out large tasks. ([Peguine's August 30 report](https://x.com/danpeguine/status/2094199634008899811), [August 31 follow-up](https://x.com/danpeguine/status/2094398320311496962))

**Evidence:** this is one person's report. It establishes visible end-to-end examples, not general success rates, the exact approval moments, or the underlying tools.

On August 29, Chester Ng reported 1,069 texts and 21 agents running during his first week. His family-related examples included booking a family trip and dinner reservations, catching that one child was on a separate airline booking and getting her seated with the family, buying movie tickets for a date with his daughter, checking in for flights and placing passes in Wallet, and monitoring forgotten airline credits and gift-card expirations. He also described longer-running monitoring and shopping work. ([Ng's first-week report](https://x.com/chest/status/2093832864513110441))

On August 31, Simon Taylor reported saying, in effect, “move that thing back 90 minutes”; Instinct reportedly remembered the earlier booking and the pub's website, changed the reservation, and sent the confirmation. ([Taylor's memory-and-action report](https://x.com/sytaylor/status/2094346674911035447))

**Inference:** the important benchmark is no longer “can the agent answer about email or calendar?” It is whether a sparse reference, an image, prior work, a third-party site, and a returned artifact can compose into a verified result.

### 4. Multiplayer demand is now explicit, especially for families

On August 31, Rebecca Kaden said the most important missing capability in the consumer agents she uses is multiplayer coordination: her agent coordinating with her husband's or friends' agents. She ranked external phone calls second and described logistics calls as a recurring weekly burden. ([Kaden's multiplayer-and-calling post](https://x.com/rebeccakaden/status/2094411588950270409))

A separate multi-week Instinct user praised its fast browser automation, Vault experience, personality, reactions, proactive suggestions, follow-up, and automatic recovery from an Airbnb cancellation, but named the inability to use it with family as a primary downside. ([Sawit's multi-week review](https://x.com/tansawit/status/2094442071671398691))

The discussion under Kaden's post exposed the real design work behind the feature. One builder said its own multiplayer implementation had to distinguish personal from team context and collect consent before sharing; it does not yet coordinate independent agents across customers. Another respondent identified school schedules and documents as a particularly strong family use case while warning about the security risks of external inputs. ([Otto implementation note](https://x.com/adamse/status/2094436801733960079), [school-coordination and security reply](https://x.com/davidmytton/status/2094439505101001202))

**Evidence:** these posts establish current user demand and practitioner-reported design constraints. They do not prove that Instinct has no private or upcoming multi-user work.

**Florence implication:** Florence should not begin by inventing a generalized cross-vendor agent protocol. It already has the more direct parent seam: independently verified adults, private conversations, one exact family group, and one shared household truth. The launch bar is that one parent's objective can ask the other adult one focused private question, incorporate the answer without leaking its raw source, and close once in the family group. Open agent-to-agent interoperability can wait until a real parent job requires it.

### 5. Calling a personal agent is not the same as the agent calling the outside world

Instinct publicly says that users can call the assistant. The reviewed public material does not establish reliable outbound calls by Instinct to restaurants, schools, medical offices, insurers, repair shops, or other third parties.

The distinction is visible in current user feedback. Kaden's request concerns an agent making daytime logistics calls on the user's behalf. In another August 30 report, a tester asked Instinct for a difficult restaurant reservation; it reportedly checked availability, found none, and stopped rather than calling the restaurant or durably monitoring additional sources. ([Kaden](https://x.com/rebeccakaden/status/2094411588950270409), [restaurant failure report](https://x.com/GreggLuskin/status/2094263361802559847))

**Evidence:** these are user-reported needs and one low-engagement failure report, not proof that Instinct never makes outbound calls.

**Florence implication:** parent calling should be a first-class completion surface, not a voice demo. A useful call flow identifies Florence and the parent-defined purpose, shares only the necessary context, respects business and recipient consent rules, handles hold or callback state, captures what was actually agreed, and returns the result to the correct private or household audience. High-value starting jobs are school-office clarification, appointment rescheduling, camp or activity availability, repair coordination, and reservation recovery.

**Current Florence reality:** the durable path now includes conversational outbound calls, status, cancellation, persisted provider handles, duplicate-call protection, bounded recovery, quiet polling, an owned-caller-number requirement, deterministic AI disclosure, household-timezone propagation, and recording disabled for the parent beta ([telephony adapter](../../apps/api/src/telephony.ts), [reasoner tools](../../apps/api/src/reasoner.ts)). Its supported Bland model and baseline result distinctions are adapter-owned, so a narrower task-specific call request cannot remove voicemail, callback, uncertain-option, or failure outcomes. The existing synthetic dentist narrative resolves the exact practice and number before calling, carries transcript-backed start/end/location/confirmation evidence through a provider-committed Florence Calendar event, and only then reports the combined result ([communication narrative](../../apps/api/src/reasoner-tool-loops.communication.test.ts)). Florence uses the raw transcript to review completion but keeps only compact selected outcome facts in its terminal receipt, not the transcript or recording URL. Production still has no configured Bland credential or owned caller number, there is no live-provider rehearsal, and provider-side transcript retention/deletion has not been contracted or rehearsed. Before calling becomes a product claim, Florence therefore still needs the real two-phone acceptance run and a clear provider-retention contract. The first live run should use a non-sensitive local-service scenario, respect the family's supplied constraints, distinguish a firm result from an option or callback, return transcript-backed confirmation, and update the Family Calendar when it creates a dated commitment; child health or other sensitive calls wait until the provider contract is suitable.

### 6. Confident personality must remain truthful about background cadence

On August 31, Peter Liu reported that Instinct sounded as if it would notice ticket availability immediately, but on questioning disclosed that it was checking every 30 minutes. He still found its speed and confidence compelling, while contrasting that presentation with an agent that exposed its actual recurring check. ([Liu's same-task comparison](https://x.com/NewOrleansVC/status/2094441775326753153))

**Evidence:** this is one person's report, not a measured product-wide behavior.

**Florence implication:** Florence should preserve the warmth and confidence users praise while stating the real monitoring contract in parent language: what is being checked, approximately how often, what would trigger action, whether Florence may act or only alert, and when the work stops. For time-sensitive family work, a false impression of continuous monitoring is materially worse than a concise truthful cadence.

### 7. Instinct's current public privacy contract is more explicit, but still individual-centric

Instinct's [Privacy Policy](https://instinct.co/privacy-policy) and [Terms](https://instinct.co/terms) are both now marked revised August 26, 2026.

The current documents say:

- general service information and usage data may be used for model evaluation, fine-tuning, and training unless the user opts out; the opt-out is forward-looking, has a safety-review exception, and does not unwind models trained before the opt-out;
- information received directly from Google Workspace is excluded from model training and advertising;
- Vault materials are used to provide the service and are excluded from model training;
- disconnecting an external service does not automatically delete indexed data;
- users can request deletion of indexed Connected Service data through Instinct's Workspace, while the Google-specific clause describes deleting previously collected Google information through account deletion in the Workspace;
- the service may process messages, email, documents, audio, credentials, payment details, precise location, and health-related information depending on permissions; and
- autonomous actions can cause unintended payments or communications and can be influenced by misleading third-party instructions.

**Evidence:** these are current company statements, not an audit of storage, derived memories, subprocessors, backups, or completed deletion. The different descriptions of external-data deletion do not make the exact granularity and downstream effects self-evident.

The policy also says the service is not intended for children and that Instinct does not seek or knowingly collect children's personal information. The Terms require users to be at least 18. ([Children's privacy](https://instinct.co/privacy-policy), [age requirement](https://instinct.co/terms))

**Inference:** this is a major opening for Florence. Parents inevitably bring children's names, schools, schedules, medical appointments, forms, travel documents, and communications into family work. Florence needs a plain parent-authorized model for represented children, minimized retention, provenance, audience, correction, and deletion. “Not a child account” is necessary but not sufficient; the product should explain why the child's information is present and which participating adult may see or act on it.

## Current capability and positioning snapshot

| Area | Verifiable current evidence | What remains unverified publicly |
| --- | --- | --- |
| Onboarding | Peter Yang reported on August 21 that connecting iMessage, Google Workspace, and MCPs was unusually smooth and that Instinct suggested useful actions immediately after connection. ([Firsthand onboarding report](https://x.com/petergyang/status/2090814910720835633)) | Instinct does not publish the complete onboarding flow, consent screens, time-to-first-result, recovery behavior, or whether all setup outside provider consent can happen in text. |
| Interface and channels | Instinct says there are “no new interfaces,” and that users can text or call it while it uses a phone and computer as a person would. ([Product page](https://instinct.co/)) The Terms describe Mac, mobile, SMS sign-in, and phone/text contact. A user reports operating it through WhatsApp while finding connector setup easier on desktop. ([WhatsApp report](https://x.com/amartino/status/2094417432962056215)) | The public contract does not establish group iMessage, inline replies, read state, reliable external calling, or a household thread. The WhatsApp report is firsthand evidence, not a published channel-support contract. |
| Proactive and background work | The product page says Instinct follows up on dropped threads and proactively calls or texts. Ng reports 21 agents and work continuing across the week. ([Product page](https://instinct.co/), [Ng report](https://x.com/chest/status/2093832864513110441)) | Trigger selection, deduplication, interruption rules, retries, steering, cancellation, restart recovery, and terminal-delivery guarantees are not published. |
| Memory | The privacy policy describes personalized suggestions based on prior experience. Peguine reports strong memory; Taylor reports a sparse reference resolving to an earlier reservation and successful change. ([Privacy Policy](https://instinct.co/privacy-policy), [Peguine](https://x.com/danpeguine/status/2094199634008899811), [Taylor](https://x.com/sytaylor/status/2094346674911035447)) | No schema, provenance model, admission rule, correction behavior, household audience, or measured recall quality is published. |
| Google Workspace | The policy names Gmail, Calendar, Drive, Docs, Sheets, Slides, and Tasks and says the assistant may use their information to take actions. ([Privacy Policy](https://instinct.co/privacy-policy)) | A complete action catalog, attachment behavior, calendar-writing rules, cross-account reconciliation, and success rates are not published. |
| Browser, phone, and real-world action | Instinct says it uses a phone and computer. Founder and user reports establish visible examples involving purchases, arbitrary sites, forms, email, phone-number porting, reservations, check-in, monitoring, and returned Wallet passes. ([Product page](https://instinct.co/), [founder launch](https://x.com/noahrshinn/status/2092691344456351744), [Peguine](https://x.com/danpeguine/status/2094199634008899811)) | Exact executor/provider, authentication and takeover behavior, retry path, per-site reliability, and whether every claimed action was read back from the provider are not published. |
| Payments | Stripe Link supplies a one-time card for an approved amount, according to the founder. ([Announcement](https://x.com/noahrshinn/status/2093368511003574730)) | Family budgets, multi-adult authority, refunds, disputes, recurring purchases, receipts, and reconciliation are not documented. |
| Household or multi-user behavior | No public company or founder source reviewed documents a household, family group, shared memory, shared calendar, or multi-adult authority. The Terms describe an individual account, prohibit letting another person use it, and allow only one account at a time unless Instinct agrees otherwise. ([Terms, account section](https://instinct.co/terms)) Current users explicitly request family/multiplayer coordination. ([Kaden](https://x.com/rebeccakaden/status/2094411588950270409), [Sawit](https://x.com/tansawit/status/2094442071671398691)) | Independent household members could exist in the private product, but there is no public evidence for them. Demand is evidence of an opening, not proof of absence. |
| Privacy and deletion | Google content is excluded from training; general training has a forward-looking opt-out; Vault content is excluded; indexed external data has a Workspace deletion path; connector disconnect and deletion are separate. ([Privacy Policy](https://instinct.co/privacy-policy), [Terms](https://instinct.co/terms)) | Derived-memory deletion, subprocessors, backup expiry, confirmation by read-back, and household/child audience controls are not documented. |
| Distribution and price | The product page says access remains private while compute scales, via waitlist or member invitation. The founder describes invite-only beta. ([Product page](https://instinct.co/), [founder launch](https://x.com/noahrshinn/status/2092691344456351744)) | No verifiable public consumer price or general-availability date was found. The Terms anticipate paid services but publish no plan or fee. |

## What Florence must match before “for parents” is persuasive

Parent positioning does not excuse a weaker general agent. Florence must match these lived qualities:

1. **Ordinary language becomes finished work.** A parent should not need to choose a workflow, provider, or command.
2. **The agent remembers prior work well enough to act on a sparse reference.** “Move that dinner later” should resolve to the correct commitment and site, not prompt a questionnaire.
3. **Media and artifacts flow through the job.** Photos, PDFs, forms, confirmations, tickets, receipts, and Wallet-ready passes are part of the task, not dead-end attachments.
4. **Long-tail sites are valid work surfaces.** A Portuguese grocer, school Google Form, retailer, airline, activity site, or medical office is not “unsupported” merely because it lacks a Florence connector.
5. **Background work is durable and steerable.** Florence must keep working while the parent lives their life and then return once with a real outcome.
6. **The personality earns confidence.** Warmth matters because parents delegate ambiguous, fragmented work. Tone cannot compensate for a missing result, but a cold status bot will still feel inferior.
7. **Quick work feels quick.** Acknowledgement and useful first movement should be immediate even when outside completion takes time.

## Where Florence can be decisively better for parents

### 1. Make the parental unit—not an individual account—the knowledge unit

Instinct's public product is centered on “you.” Florence already has the stronger product contract in [`PLAN.md`](../../PLAN.md): two independently enrolled adults, separate private relationships and Google connections, an exact family group, represented children, and a shared Florence calendar.

The user-visible advantage should be concrete:

- each adult can ask privately without leaking raw personal sources;
- Florence can reconcile both schedules without flattening both inboxes into one feed;
- either adult can steer shared family work;
- a useful household conclusion returns to the group once, with ownership and next action clear; and
- one adult's private evidence becomes household truth only through a visible sharing boundary.

This is not a dashboard feature. It is the difference between “my assistant” and “our family can rely on this.”

### 2. Treat children as represented people with parent-authorized boundaries

Instinct users already report school forms and child travel work, while Instinct's public policy says it does not seek or knowingly collect children's personal information. Florence should make the unavoidable family reality legible:

- the child has no account and no independent Florence conversation;
- a participating parent identifies the child and can inspect retained facts;
- Florence keeps only information needed for useful family work;
- medical, school, travel-document, and other sensitive facts retain source and audience;
- either authorized adult can correct household truth, while raw personal-source details remain private to their owner; and
- reset and targeted deletion include child-derived facts and Florence-created provider artifacts.

That is a product capability, not compliance copy.

### 3. Compile family evidence into a docket with definitions of done

Instinct's impressive public stories are mostly reactive objectives supplied by one person. Florence can notice the unfinished work created by school email, calendars, forms, activities, travel, and another adult's schedule, then select the next useful action.

The distinction should be felt as:

- not “school form due Friday,” but “I filled what I can, need the emergency-contact choice from Sam, and will submit after you approve”;
- not “flight changed,” but “the four of you are still on the same flight, Maya's seat is separated, and I have the no-cost adjacent option ready”;
- not a repeated appointment reminder, but the one unresolved dependency, its owner, and its provider-confirmed closure.

Florence's existing one-docket and one-due-work design in [`PLAN.md`](../../PLAN.md) is the right seam. Do not create school, travel, grocery, and medical workflow engines.

### 4. Make household memory inspectable and audience-aware

Instinct's memory is repeatedly praised. Florence should not answer by storing more transcript. It should provide better semantics:

- current fact, preference, routine, commitment, or reusable artifact;
- who and what source support it;
- whether it is private to one adult or household-visible;
- what superseded it;
- how to correct or forget it; and
- whether deletion also removes indexed provider-derived data and downstream Florence artifacts.

The winning demonstration is not recalling trivia. It is using the corrected fact later, selecting the right action, and never exposing the wrong adult's source.

### 5. Offer individual-first, household-expandable onboarding

Do not ask a new parent to choose a permanent “solo” or “family” product. Start every parent in a fully useful private conversation, use the browser only for the disclosure and Google authorization that require it, and return to Messages with a specific useful result. Then offer to add another parent or caregiver.

“Just me for now” should finish onboarding, not activate a degraded escape hatch. Adding another adult later should preserve the same work and calendar while explicitly reviewing what may become shared. This combines Instinct's reported onboarding strength with Florence's differentiated household model.

### 6. Use a stricter family trust contract

Instinct has moved quickly on visible data deletion and amount-approved payments. Florence should still be clearer for families:

- no household conversational or retained-memory content used for model training;
- “disconnect,” “stop using,” “forget retained meaning,” “delete indexed source data,” and “delete the account/provider artifacts” are distinct plain-language actions;
- deletion reports success only after Florence's store and the relevant provider have been checked;
- reading, drafting, sharing with the household, communicating externally, scheduling, and spending have different earned authority; and
- the right adult approves the action, with the result delivered to the right audience.

Parents should not need to understand a policy framework. The conversation should make the consequence obvious at the moment it matters.

## The parent benchmark Florence should run next

These are evaluation narratives for the same general agent, not feature categories or implementation pipelines.

1. **Individual parent activation:** one parent texts normally, connects Google through a thin browser handoff, and Florence returns with one completed or approval-ready job discovered from the full review—not a setup-complete message.
2. **Later household expansion:** that parent invites another caregiver after using Florence alone. The second adult independently consents and connects; Florence previews what pre-existing facts/calendar items would become shared, creates the exact group, and coordinates one real obligation without leaking either adult's raw source.
3. **School form to verified closure:** Florence finds an outstanding school form, reads the attachment or page, fills derivable fields, asks one parent for the genuinely missing choice, obtains approval, submits, retains the receipt/reference, and updates only the useful household conclusion.
4. **Family travel reconciliation:** Florence joins confirmations, both adults' calendars, current flight state, seats, ground transport, and weather; detects a separated child or changed leg; prepares or executes the best fix; returns the provider confirmation and family-calendar change.
5. **Image-to-household action:** a parent sends a fridge or pantry photo. Florence updates the household grocery plan using known preferences and existing inventory, asks for the meaningful spend/substitution choice, then returns the final order and receipt.
6. **Sparse-reference memory:** days later a parent says “move that dinner back 90 minutes.” Florence resolves the correct reservation from household context, checks both adults' conflicts, gets any required approval, changes it, and returns the confirmation without asking which dinner.
7. **Trust teardown:** either adult disconnects Google, deletes one source-derived fact, corrects another, and resets the household. Florence explains and then verifies exactly what stopped, what was forgotten, what indexed data was deleted, and which Florence-created calendar/artifacts were removed.

## Strategic priority

The recommended order is:

1. make one-parent Florence fully useful without weakening the agent;
2. make add-a-caregiver-later safe and seamless;
3. perfect school/child evidence through one provider-confirmed, cross-adult closure;
4. enable and live-rehearse one transcript-backed external call that uses household constraints and updates shared truth;
5. perfect a cross-adult schedule/travel reconciliation;
6. prove audience-aware memory, correction, and deletion in the same lived household; and
7. only then deepen autonomous purchasing beyond approval-ready preparation.

Instinct's newest payment and parent-work examples raise the breadth bar, but they sharpen rather than erase Florence's opportunity. The moat is not “an AI that can fill a school form.” It is **an agent both adults can trust to notice the right family work, preserve the right boundaries, and finish it once for the household.**

## Source register

| Date | Source | Evidence level | Used for |
| --- | --- | --- | --- |
| Current on Aug. 31 | [Instinct product page](https://instinct.co/) | Company statement | Positioning, text/call, phone/computer use, proactive follow-up, private distribution |
| Revised Aug. 26 | [Privacy Policy](https://instinct.co/privacy-policy) | Company statement | Data sources, Google Workspace, training/opt-out, external-data deletion, location/health data, children's privacy |
| Revised Aug. 26 | [Terms](https://instinct.co/terms) | Company statement | Individual accounts, connected-service authority, actions, Vault training exclusion, indexed-data deletion, paid-service ambiguity, SMS/phone terms |
| Revised Aug. 20 | [Acceptable Use Policy](https://instinct.co/acceptable-use-policy) | Company statement | Child harm, sensitive-information, unauthorized-account, and action constraints |
| Aug. 26 | [Founder launch post](https://x.com/noahrshinn/status/2092691344456351744) | Founder statement | Product interface, private beta, early-user task breadth |
| Aug. 28 | [Stripe announcement](https://x.com/noahrshinn/status/2093368510449877180) and [Link detail](https://x.com/noahrshinn/status/2093368511003574730) | Founder statement | Purchase volume claim, use cases, one-time cards, explicit amount approval |
| Aug. 29–30 | [Scaling update](https://x.com/noahrshinn/status/2093905019108213059) and [latency follow-up](https://x.com/noahrshinn/status/2094089088823402531) | Founder statement | Slower-response acknowledgement and claimed speed improvement |
| Aug. 21 | [Peter Yang](https://x.com/petergyang/status/2090814910720835633) | Firsthand user report | iMessage/Google/MCP onboarding, immediate suggestions, single-thread limitation |
| Aug. 29 | [Chester Ng](https://x.com/chest/status/2093832864513110441) | Firsthand user report | 1,069 texts, 21 agents, family travel/seating, tickets, monitoring, Wallet passes |
| Aug. 30–31 | [Dan Peguine](https://x.com/danpeguine/status/2094199634008899811) and [follow-up](https://x.com/danpeguine/status/2094398320311496962) | Firsthand user report | Parent tasks, international sites/email, image-to-order, forms, insurance, artifacts, memory/personality |
| Aug. 31 | [Simon Taylor](https://x.com/sytaylor/status/2094346674911035447) | Firsthand user report | Sparse-reference memory, website reuse, reservation change, confirmation |
| Aug. 31 | [Rebecca Kaden](https://x.com/rebeccakaden/status/2094411588950270409) | Current user demand | Multiplayer coordination with husband/friends and outbound logistics calls as the two leading missing capabilities |
| Aug. 31 | [Sawit](https://x.com/tansawit/status/2094442071671398691) | Firsthand user report | Browser automation, Vault, personality/reactions, proactivity, cancellation recovery, and explicit wish for family multiplayer |
| Aug. 31 | [Peter Liu](https://x.com/NewOrleansVC/status/2094441775326753153) | Firsthand comparison | Speed/confidence and a mismatch between implied immediacy and an actual 30-minute monitor cadence |
| Aug. 31 | [Adam Seligman](https://x.com/adamse/status/2094436801733960079) and [David Mytton](https://x.com/davidmytton/status/2094439505101001202) | Practitioner reports | Personal/team context, sharing consent, channel sessions, school coordination, and external-input security challenges |
| Aug. 30 | [Gregg Luskin](https://x.com/GreggLuskin/status/2094263361802559847) | Firsthand user report | Reservation search stopped rather than escalating to a call or durable multi-source monitor |
| Aug. 31 | [Mariano Amartino](https://x.com/amartino/status/2094417432962056215) | Firsthand user report | WhatsApp use and connector setup being easier on desktop |

## Confidence and unknowns

**High confidence:** current official positioning and legal text; dates and wording of the cited public posts; no public household model or public price in the reviewed sources.

**Moderate confidence:** each named tester's reported experience. These are direct accounts but are not independently reproduced and do not establish general reliability.

**Unknown:** Instinct's internal memory architecture; exact tool/provider used for each action; success, error, retry, and latency distributions; private multi-user experiments; invite quotas; storage-level deletion semantics; and any private pricing. None should be filled in by inference.
