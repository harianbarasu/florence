# Ollie product capabilities Florence should generalize

Research date: 2026-08-28

## Decision summary

Ollie's strongest product idea is not a collection of family workflows. It is a **persistent household agent** that receives ordinary conversation and media, combines them with connected sources and retained context, takes or proposes the next useful action, remembers the resulting artifact or commitment, and follows up in the same text thread.

For Florence, the lesson is to keep one general agent loop and one connected household memory. Meals, trips, school, health, inbox work, and errands should be examples and evaluation cases—not routing categories in the runtime.

Recipes are an important concrete consequence. A useful recipe is not merely a fact sentence. It is a reusable, editable household artifact that can preserve a family's version, connect to preferences and constraints, appear in later plans, and produce or update another artifact such as a grocery list.

## Evidence boundary

This note uses first-party Ollie material only: the current [Explore page](https://ollie.ai/explore/), [homepage](https://ollie.ai/), [FAQ](https://ollie.ai/faq/), [family-assistant explainer](https://ollie.ai/what-is-a-family-assistant/), [Ollie-for-Meals page](https://ollie.ai/ai-meal-planning-for-families/), official articles on [recipe creation](https://ollie.ai/2024/05/31/how-does-ollie-ai-create-its-recipes/) and [meal plans with grocery lists](https://ollie.ai/2024/04/18/how-to-get-a-meal-plan-with-grocery-list-with-ollie/), Ollie's [Poke comparison](https://ollie.ai/vs/poke/), and its [privacy policy](https://ollie.ai/privacy-policy/).

Evidence labels used below:

- **Company-labeled real moment:** Ollie marks the conversation as a “Real moment.” This is stronger than an unbadged illustration but remains a company-curated example.
- **Official example:** Ollie displays the behavior in a product conversation, without claiming that the example is a real user exchange.
- **Product claim:** Ollie advertises or describes the behavior in prose.
- **Published data contract:** Ollie's policy names the data or use.
- **Inference:** a Florence product conclusion, not a claim about Ollie's implementation.

No public source establishes Ollie's reliability, recall, latency, alert precision, task-success rate, or internal agent architecture. Even the “Real moment” examples are not independent product tests.

## General product primitives evidenced by Ollie

| Primitive | First-party evidence | General implication for Florence |
| --- | --- | --- |
| One conversational ingress | Ollie says it works through text on iOS and Android, supports family group conversations, and can receive photos/media. Its connected-account description names email and calendar access. ([FAQ](https://ollie.ai/faq/), [privacy policy](https://ollie.ai/privacy-policy/)) | Treat any message, attachment, connected-source change, or family-thread contribution as context for the same agent—not as entry into a domain router. |
| Household context rather than isolated accounts | Ollie says it works best with whole-family context; its own Poke comparison describes shared briefings, household-spanning memory, partner coordination, children, schools, and shared routines. ([homepage](https://ollie.ai/), [Ollie vs. Poke](https://ollie.ai/vs/poke/)) | The useful unit is the household/parental unit, with people and audience attached to information, rather than independent personal Vaults that happen to share a calendar. Ollie's claims about Poke should not be treated as authoritative evidence about Poke itself. |
| Source watching and prioritization | The product says it brings together calendars, email, and to-dos, watches when the user cannot, surfaces what needs attention, and takes the next step. Explore examples rank an inbox, surface a school injury and deadline, detect calendar conflicts, and compose daily briefings. ([homepage](https://ollie.ai/), [Explore](https://ollie.ai/explore/)) | Ingestion should preserve enough source material for later retrieval; a separate relevance decision should determine what interrupts the family now. Do not mistake “read everything useful” for “send everything immediately.” |
| Cross-source synthesis | Official examples combine weather, schedule, family responsibility, meal plan, reminders, travel time, nearby preferences, and inbox-derived deadlines in one response. Emails and images become drafts, reminders, or calendar events. ([Explore](https://ollie.ai/explore/)) | The model should see a current household picture and choose the next useful move across all available tools. A briefing is a generated view of state, not a fixed concatenation of category summaries. |
| Action with an observable receipt | Examples show Gmail drafts left for review, calendar events created, reminders scheduled, shared lists updated, recipes saved, and ingredients synchronized. The FAQ says connected Calendar access can create events and email access can triage mail. ([Explore](https://ollie.ai/explore/), [FAQ](https://ollie.ai/faq/)) | Florence should distinguish a suggestion, prepared artifact, attempted effect, and confirmed external change. Its reply should say what actually happened and offer the smallest useful next move. |
| Durable proactive follow-through | Official examples include recurring reminders until confirmation, refill timing derived from supply, check-ins after silence, goal adjustment, future prompts tied to an open evening, and weekly trend summaries. ([Explore](https://ollie.ai/explore/)) | Generalize proactivity as triggers over time, source changes, conditions, unresolved work, and nonresponse. The same durable objective should be updated and resumed; repeated rediscovery must not create duplicate reminders or messages. |
| Work offload and delegation | In an official example, a parent unloads several unrelated concerns at once; Ollie separates them into completed reminders, proposed future work, and an already-covered calendar item. Group examples also preserve who owns pickup or the next confirmation. ([Explore](https://ollie.ai/explore/)) | Let one objective decompose into durable child steps, assign each step to Florence, a household member, or an outside party, and reconcile their outcomes back into the parent objective. The public evidence does not establish Ollie's internal sub-agent architecture. |
| Shared coordination and ownership | Explore shows a family group resolving schedule conflicts, multiple people contributing to one list, additions attributed to family members, pickup responsibility, travel ideas retained together, and a plan followed by a babysitter reminder. ([Explore](https://ollie.ai/explore/)) | Every commitment and work item should be able to name participants, owner, audience, waiting-on party, and next step. Coordination is one general capability that applies to any household objective. |
| Human-feeling presence | Ollie's examples use short text-native turns, lightweight emoji and reactions, remembered emotional context, low-pressure check-ins, and context-sensitive offers. One company-labeled real moment is a proactive check-in after difficult nights. ([Explore](https://ollie.ai/explore/)) | Human feel comes from timing, continuity, calibrated interruption, and actually helping—not a canned friendly voice. Reactions can acknowledge low-content turns; substantive requests still need a useful reply or visible action. |

## Recipes demonstrate the right memory-to-action loop

Ollie's official examples make recipes and meal plans durable and connected:

- A fridge photo plus a time limit and allergy constraint becomes a proposed meal; after acceptance, the recipe is saved to a cookbook and a low ingredient is added. ([Explore](https://ollie.ai/explore/))
- A weekly plan is revised from a household preference (“we don't love salmon”), and the grocery list changes with it. ([Explore](https://ollie.ai/explore/))
- A recipe is adapted around available ingredients, saved as the family's Mediterranean variation, then placed into a later meal plan. Another example synchronizes its ingredients into the grocery list. ([Explore](https://ollie.ai/explore/))
- Ollie's official meal-planning guide says families can save favorite recipes **with their own tweaks**, that grocery lists update when plans change, and that a prepared plan returns automatically for review. ([meal-plan guide](https://ollie.ai/2024/04/18/how-to-get-a-meal-plan-with-grocery-list-with-ollie/))
- Its recipe article says a recipe can be refined after a substitution or preference correction and saved in that personalized form. ([recipe article](https://ollie.ai/2024/05/31/how-does-ollie-ai-create-its-recipes/))

The generalized Florence pattern is:

```text
source or conversation
  -> retrieve people, constraints, preferences, and prior artifacts
  -> create or revise an artifact
  -> connect it to the current plan or commitment
  -> derive the next useful artifact or action
  -> preserve corrections and outcome for later reuse
```

This is not a `meal_planning` branch. The same loop applies when a school email becomes a calendar commitment and packing list, a trip idea becomes researched options and reservations to make, or a gift idea becomes a timed shopping task.

## Explore information architecture: useful for discovery, wrong for routing

The Explore page presents eleven browsing sections: **Email, Calendar, Mornings, Reminders, Family, Meals & Grocery, Lists, Health, Support, New Parents, and Accountability**. The sections are effective marketing and evaluation coverage. They are not clean execution boundaries. ([Explore](https://ollie.ai/explore/))

The examples continually cross those boundaries:

- inbox evidence creates drafts, reminders, and calendar changes;
- a calendar opening triggers a saved movie suggestion;
- a family profile changes a packing list;
- a meal preference changes both a plan and grocery list;
- a health log produces a trend, goal, and future check-in; and
- emotional context leads either to listening or to concrete task offload.

Florence should therefore use categories only as **facets, views, and evaluation tags**. Tool choice and continuation should come from the objective, current state, and dynamic capability catalog.

## Recommended household Vault model

Use one household knowledge graph/store with a small set of general object kinds. Add typed detail where it makes an artifact genuinely usable, but do not create separate agent runtimes for each kind.

| Vault object | What it holds | Ollie-derived examples |
| --- | --- | --- |
| People and groups | Identities, relationships, roles, schools, caregivers, relevant contacts | children, partners, teachers, family group, who handles pickup |
| Preferences, constraints, and routines | Stable or revisable choices and recurring patterns, with subject and provenance | allergies, dislikes, favorite place, cooking time, notification cadence, shared routines |
| Plans and commitments | Events, deadlines, intentions, responsibilities, participants, time, status | school deadline, trip, dinner plan, birthday, appointment, recurring goal |
| Reusable artifacts | Structured, editable objects with versions and links | recipes/cookbook, meal plans, packing lists, gift ideas, shopping lists, notes, generated stories |
| Active work and follow-ups | Objective, owner, next step, trigger, waiting-on, attempts, status, external handles, outcome | reply awaiting approval, deadline watch, reminder until confirmed, reservation still to make |
| Observations and logs | Timestamped events that can produce summaries or trends without pretending to be permanent facts | feeding, sleep, medication confirmation, goal check-in, source change |
| Sources and evidence | User message, email, calendar event, image, document, URL, or provider receipt behind another object | school email, flyer photo, calendar item, fridge photo, sent-draft receipt |

Every object should support relationships, household participants, audience, timestamps, source links, status, and flexible tags. The Vault UI can then offer views such as “Cookbook,” “Trips,” or “School” without making those views the core ontology.

### Recipe artifact

Recipes deserve a first-class Vault presentation because a vague retained fact cannot be cooked or revised. Store at least:

- name and family-visible description;
- ingredients, quantities, instructions, yield, and time;
- relevant constraints and the people they apply to;
- the family's substitutions, ratings, and version history;
- provenance or canonical source;
- links to meal plans, dates served, and generated grocery-list items; and
- retrieval aliases/tags so later conversation can find “the quiche we changed” naturally.

The core remains an artifact interface; `recipe` is one useful artifact type alongside list, plan, note, document, and other future objects.

## Product implications for Florence

1. **One general objective-to-outcome loop.** Capture the user's real objective, gather context, use any relevant tool sequence, preserve state across waits, accept corrections, and return an honest terminal result.
2. **Broad durable retrieval.** Retain and search useful household history and artifacts so old context becomes available when a new request, source change, or calendar event makes it relevant. Do not condense it into a handful of generic facts.
3. **General trigger model.** Support time-, event-, condition-, source-change-, and nonresponse-based continuation on the same work item, with duplicate suppression and adjustable cadence.
4. **Artifact-aware memory.** Put recipes, lists, plans, notes, and similar reusable outputs in the Vault as linked editable objects, not only prose memories or ephemeral chat output.
5. **Household-aware agency.** Resolve who said what, who owns the work, who should be told, and what belongs to the shared household picture before acting or messaging.
6. **Cross-source proactivity.** Use current commitments plus retained memory to infer one concrete next job Florence can remove. Offer or prepare that job; do not merely announce a calendar fact.
7. **Natural conversational closure.** Respond immediately enough to feel present, use reactions selectively, avoid canned status language, ask only genuine blockers, and close the loop with what changed or what remains.

## What the public evidence does not justify

Do not infer from Ollie's marketing that it has a universal browser agent, arbitrary external-service completion, reliable long-running jobs, or perfect memory. Public pages do not document the underlying planner, queue, retry/recovery behavior, provider receipts, or performance. They also do not show that every Explore example is available to every plan or connection.

The worthwhile benchmark is observable behavior: Florence should feel like one household teammate who remembers usable things, connects them to the present situation, takes the next step across whatever tools are available, and follows through without being re-prompted.

## 2026-08-31 recursive follow-up: what Ollie's "multiplayer" actually is

This follow-up was prompted by [Brian Distelburger's reply](https://x.com/bdistel/status/2094417512997818467) to Scott Belsky, which identifies Bill Lennon and Ollie as the closest product he has seen to the desired multi-person family-agent experience. `@blennon_` is not a separate product: it is the account of **Bill Lennon, Ollie's co-founder and CEO**. Ollie is operated by Confabulation Corporation. Its other co-founders are Christy Shannon (CMO) and Rushabh Doshi (CPTO). ([Ollie team](https://ollie.ai/about/))

### Bottom line

Ollie's public multiplayer model is **one Ollie identity inside one ordinary family group text**. The company says a spouse, nanny, home manager, or child can contribute to the same thread; official examples show multiple participants adding list items, resolving conflicts, supplying trip details, and receiving responsibility-specific reminders. One paid subscription covers the family group chat. ([homepage](https://ollie.ai/), [Explore](https://ollie.ai/explore/), [family calendar](https://ollie.ai/family-calendar/), [family reminders](https://ollie.ai/family-reminder/), [pricing](https://ollie.ai/pricing/))

That is real shared-assistant functionality, but it is not the architecture requested in the Instinct discussion:

- no public evidence shows one separately owned agent per adult;
- no public evidence shows agent-to-agent negotiation or controlled context exchange;
- no public material explains independent partner verification, per-adult authority, per-source visibility, private surprise planning, or conflict resolution between two connected accounts; and
- the live onboarding route does not collect or verify a second adult. It merely asks whether the user wants to start a group chat or keep a private one-to-one chat, then gives the same Ollie phone number and tells the user to create the thread.

This is therefore best classified as **shared-thread multiplayer**, not **federated personal-agent multiplayer**. The latter may exist privately, but the researched public contract does not establish it.

### Live onboarding, inspected August 31

Despite the homepage promise "No app, no login, just a text," clicking Get Started opens a six-step web flow at [ollie.ai/onboarding](https://ollie.ai/onboarding/):

1. Choose an initial job: Calendar, Email, Reminders, or All of it.
2. Choose a calendar provider (Google, Outlook, iCloud, Yahoo, or "something else").
3. Choose an email provider (Gmail, Outlook, iCloud, Yahoo, or "something else").
4. Supply a first name and time zone; the name is skippable.
5. Choose a writing voice: Sharp & Efficient, Warm & Reassuring, Relaxed & Easygoing, or Fun & Playful.
6. Choose **Start a group chat** ("You + your household share one thread with Ollie") or **Just me for now** ("Keep it a private one-on-one chat"), then scan a QR code or text Ollie's number.

This is notable for Florence: Ollie already uses an individual-or-household expansion choice and does not require a second adult to obtain value. But the choice occurs before the first useful result, and the public flow treats group creation as a messaging operation rather than a two-adult trust and account-linking ceremony.

The current web flow appears to be a direct product response to an early failure. On launch day, [Car Dealership Guy said](https://x.com/GuyDealership/status/2061973558083846383) he deleted Ollie after 20–30 setup texts required to add the 3–4 calendars and email accounts he and his wife used; he explicitly asked for web onboarding. Lennon replied that the team had web-based ideas to try. The new web provider selector reduces that friction, although it still does not explain which adult owns each connected source.

There is also a scope contradiction inside onboarding: the Email step says Ollie "reads only what you ask about," while Ollie's core product promise and privacy policy describe continuous inbox monitoring, unsolicited alerts, shared briefings, and deadline/conflict detection. A parent cannot give meaningful consent if the setup copy describes request-scoped reading while the product is actually offering proactive watching.

### What Ollie claims the household layer does

The strongest company claim is on its [Ollie-versus-Poke page](https://ollie.ai/vs/poke/): it says Ollie has a household-spanning "memory graph," a joint briefing, shared visibility, partner coordination, and knowledge of a partner's calendar, children's schools, and shared routines. Its [Explore page](https://ollie.ai/explore/) supplies observable product examples of:

- detecting a practice/recital collision in a family group;
- attributing shared-list additions to specific people;
- retaining trip ideas contributed by multiple participants;
- confirming a dinner plan and then assigning a babysitter follow-up;
- identifying whose turn it is for pickup; and
- updating a common calendar or household list.

These examples establish the intended product behavior, not reliability or the underlying data model. Some are labeled by Ollie as a "Real moment," but they remain company-curated rather than independently reproducible tests.

### Firsthand X evidence

The independently attributable public user corpus is much thinner than Ollie's marketing corpus, but it includes two strong longitudinal reports:

- [Cynthia Bell McGillis, June 11](https://x.com/cynthiamcgillis/status/2065184775745486987): after connecting personal email, she said Ollie reliably texted her about important messages; she also sent a screenshot of events and said calendar creation worked flawlessly. On [August 21](https://x.com/cynthiamcgillis/status/2090970074920386948), she identified herself as a paying user and recommended it for surfacing time-sensitive Gmail/Calendar work.
- [Christian / OptionsCJP, July 6](https://x.com/optionscjp/status/2074308637317796347): after several months, he reported connecting email, calendars, and work iMessage group chats; receiving morning and evening briefings; and using Ollie for group answers and recaps. He explicitly said the post was not paid and there was no referral program.
- [The same user, August 19](https://x.com/optionscjp/status/2090207786089238773): he reported a regression in group chats. Although Ollie agreed to stay in the background unless spoken to, it repeatedly answered everyone's messages. Bill Lennon replied asking for the user's Ollie phone number so the team could fix it.
- [Brooke K. Travis, August 24](https://x.com/btravisNYC/status/2091936494889939283): after 24 hours, she described setup as seamless and the text interface as useful for juggling, but did not yet document a completed household workflow.
- [Car Dealership Guy, June 2](https://x.com/GuyDealership/status/2061973558083846383): he abandoned and deleted the product because connecting multiple household calendars and email accounts took 20–30 back-and-forth texts. His follow-up says he and his wife needed 3–4 sources connected to capture the family picture.

The July-to-August OptionsCJP sequence is especially important: a shared agent must have **participation discipline**, not merely access to the shared thread. It needs a durable, auditable rule for when it is addressed, when it should stay silent, who owns a request, and when a new message changes an existing obligation.

### Trust, privacy, and authority boundary

Ollie now has a public [Trust Center](https://trust.ollie.ai/) and announced [SOC 2 compliance on August 21](https://x.com/heyollieai/status/2090853850135801900). Its current [privacy policy](https://ollie.ai/privacy-policy/) says:

- messages and media may be retained for up to 24 months unless deletion is requested;
- connected email and calendar data may be stored while active as needed to support product functionality;
- connected-account data is deleted from core services on account deletion and removed from logs/backups within up to 30 days;
- group participation is collected;
- service providers include Google, Stripe, Anthropic, OpenAI, Groq, Composio, Linqapp, Raindrop, Instacart, Amazon, and Walmart; and
- Ollie uses email/calendar data for alerts, shared family briefings, conflicts, deadlines, and user-requested monitoring.

There are public-contract inconsistencies worth tracking:

- On August 22, [Bill Lennon said](https://x.com/blennon_/status/2091270163475919288) Ollie "doesn't index/store your emails." The privacy policy, updated August 21, says connected email/calendar content may be stored while active as needed. The policy is the safer statement to rely on.
- The homepage invites users to add "even the kids" to the group, while the [Terms](https://ollie.ai/terms-of-service/) say the service is for adults and children may not independently use it. A child appearing in an adult-managed thread may be intended, but the boundary is not explained.
- The Terms make the inviting user responsible for having permission to include others and warn that all group-thread messages may be visible to all participants. They do not document independent consent or granular visibility for each adult.
- The comparison page advertises Telegram, while the current Terms and privacy policy enumerate SMS, MMS, RCS, and iMessage. The live product surface should be treated as ahead of or inconsistent with the legal copy until clarified.

The homepage's security section still contains stale "in progress" SOC 2 wording even though its footer, Trust Center, privacy policy, and official X account now say Ollie is SOC 2 Type I compliant.

### Technical-claim boundary

Bill Lennon's [launch thread](https://x.com/blennon_/status/2061868938443550842) says an "army of AI agents" scans email and messages, extracts what matters, plans the day, maintains memory, and works overnight. Ollie's older recipe material likewise describes collections of specialist agents. The public FAQ names Auth0/JWT authentication, OAuth-connected providers, HMAC-verified webhooks, and API-based model use. The privacy policy names multiple model and integration vendors.

These are company claims. There is no public source code, architecture document, reliability evaluation, or independent trace establishing that "army of agents" is more than an implementation/marketing description. The defensible benchmark remains the external behavior, not the claimed agent count.

The public action boundary also appears narrower than Instinct's. In a [June 3 founder reply](https://x.com/blennon_/status/2062189005878239668), Lennon says Ollie "doesn't make decisions for you"; it surfaces information so the couple can coordinate and decide. His example is revealing: his wife used Ollie to research summer-camp options, then he submitted the registration. Public evidence is strong for noticing, summarizing, reminders, lists, calendars, drafts, and coordination; it is weak for broad external purchasing, booking, negotiation, form submission, or calls. This is a company-stated product philosophy plus an evidence gap, not proof that those actions are impossible.

### Product consequence for Florence

Ollie removes the easy positioning whitespace. "A family AI in your texts that shares memory, watches school email, coordinates calendars, briefs both parents, and reduces nagging" is already an active competitor's exact promise.

Florence's defensible differentiation has to be deeper and demonstrable:

1. independently verify each adult and connect each adult's account privately;
2. preserve owner-private evidence while promoting only intentionally validated household truth;
3. let adults coordinate through one shared Florence without exposing private context;
4. resolve conflicting facts and responsibilities instead of merely sharing a thread;
5. distinguish a reminder, a request to another adult, a provider action, and provider-confirmed completion; and
6. stay appropriately silent in ambient group conversation while reliably owning assigned work.

Ollie is nevertheless the best public onboarding and distribution benchmark found for Florence so far: text-native, useful alone, household-expandable, and priced once for the family. Florence should not copy its shallow trust boundary, but it must match the simplicity of its lived surface.

One pricing detail cuts against ambient multiplayer: Ollie charges by **messages from Ollie**, including every reply it sends. At $25/month the family receives 150 monthly replies plus 10 daily replies; at $100/month it receives 1,000 monthly plus 15 daily. Because the documented failure mode is replying too often inside a group, the meter can make interruption mistakes feel financially punitive. Florence should meter completed value or usage less visibly, not make every conversational contribution feel like a scarce family resource.
