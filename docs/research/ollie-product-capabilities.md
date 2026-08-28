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
