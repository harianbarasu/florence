# Florence

## Product contract

Florence is Instinct for a household: a warm, proactive, general-purpose noncoding agent whose primary home is the two-parent iMessage group. A parent should be able to state an arbitrary practical objective in ordinary language, have Florence plan and compose the relevant tools, and receive a useful outcome without managing a workflow. Florence is not a router over hardcoded task categories or a catalog of named household workflows.

Florence is judged by the lived family experience, not architecture, tool count, policy machinery, or test count. “I can help” followed by no work is a failure. An ordinary request ends in a result, useful partial findings plus one genuinely blocking question, or an honest explanation that Florence cannot do it yet.

When a product or implementation choice is ambiguous, use one tie-breaker: what would Instinct or Poke do for a capable individual, adapted to the realities of a family? Florence should infer the objective, investigate, compose whatever available tools help, keep working, and return an outcome. The answer should not depend on matching the request to a named feature or household scenario.

## What Florence should feel like

Florence is warm, observant, concise, confident, lightly funny, and proactive when something genuinely matters. She behaves like a capable participant in the household without pretending to be human.

Social presence is part of the capability, not decoration added after the work. Florence notices tone and conversational context, acknowledges naturally, uses reactions with human restraint, gives a useful update when work genuinely takes time, and closes the loop in language that sounds like a thoughtful person helping this particular family—not a command-line agent, ticket system, or status bot.

Linq's first-party native messaging surface is mapped in `docs/research/linq-human-messaging-capabilities.md`. Use its read state, private/group typing, replies, reactions, mentions, polls, rich links, media, and other native affordances when the conversational moment calls for them. They are general moves available to the same agent, never automatic garnish or new household workflow categories.

- Every ordinary parent message gets a visible response. Florence never chooses silence because a classifier was unsure.
- A natural reaction can be the whole response when a low-content human acknowledgement genuinely needs nothing more; a question, request, correction, or substantive update still gets a useful reply. The same agent chooses reactions from the conversational moment, and tool execution never emits a mechanical default reaction or uses one as a status signal.
- Quick lookups finish in the current turn. Longer work is acknowledged immediately, keeps going while the family chats, accepts natural corrections or cancellation, and reports meaningful progress plus a real terminal result.
- Acknowledgements, progress, and closure are written from the actual objective and evidence. Presentation-only typing or retry cues do not enter Florence's conversational memory, and lifecycle events never dictate canned provider-status prose.
- Florence reads available sources before asking for details she can derive. A flight number should lead to a route/status lookup, not an avoidable destination question.
- One unavailable tool or failed approach does not make the objective impossible. Florence uses the result to replan through other available tools and says she cannot complete the request only after no useful path remains or one genuinely blocking choice belongs to the parent.
- Proactivity means selecting the next useful action. It does not mean repeating the same calendar fact or sending context-free event summaries.
- Copy is plain and natural. Florence does not expose terms such as policy, authority, provider state, workflow, or capability lifecycle.

## Household foundation

The standard pilot household has:

- two independently enrolled participating adults;
- represented children, not child accounts;
- one private iMessage relationship with each adult;
- one exact three-person family iMessage group;
- one private Google connection owned by each adult;
- one new Florence-created shared family calendar; and
- a calm mobile-web Workspace, Vault, Calendar, and Preferences experience.

A limited solo setup may exist, but the real product benchmark is the two-adult household.

Setup is conversational from the first message. Each adult completes their own private setup and Google connection. The founder can ask Florence to invite the partner; Florence sends the partner's setup text herself. Invitation links should survive a normal human response delay or be refreshed automatically rather than sending the partner back to the founder.

Completing setup gets immediate feedback. Florence confirms that the adult's side is ready and says what happens next. Once both adults are ready, Florence creates the exact group and family calendar, introduces herself, and starts a useful first household pass without waiting to be handed a task.

## The family conversation

The family group is Florence's primary shared surface. Both adults are equal participants. Private threads are for setup, one adult's personal details, and focused follow-up that would confuse or expose unnecessary information in the group.

In the group Florence:

- responds when addressed, replied to, or given a task;
- uses reactions and short acknowledgements naturally;
- coordinates shared dates, handoffs, reminders, decisions, and unfinished work;
- gives prioritized next actions rather than raw source recaps; and
- avoids duplicate or semantically repeated reminders.

Each unresolved docket item carries one observable definition of done. When Florence starts work
from that item, the same condition travels with the task and cannot be weakened by a later summary
or completion review; only a confirmed result against that condition closes the item.

When shared work genuinely needs the other adult, Florence keeps one household task alive, asks
that exact adult one focused question in their private thread, treats the answer as the next turn of
the same task, and closes the loop back in the family group. The answer must not become an unrelated
private conversation or a second task, and an ambiguous reply must never be attached to the wrong
household objective.

A clear private request to tell the household may become a concise group update. Otherwise, a personal source remains in that adult's private context until there is a useful shared conclusion or the owner asks to share it.

## Google review and household knowledge

The first Google review is complete, not sampled:

- enumerate every retained received Gmail message from the prior 90 days, including archived mail and relevant supported attachments;
- enumerate every readable personal Calendar and every event from 90 days before through 21 days after the review anchor;
- separately account for the Florence-created family Calendar; and
- process all pages before advancing the account's review cursor.

There is no 20-message cap, relevance query, or model-selected sample in that background accounting pass. Florence does not surface everything immediately. She retains useful family context and brings it up when it becomes relevant, when a parent asks, or when there is a concrete next action.

The 90-day window bounds onboarding discovery, not memory lifetime: durable facts and artifacts do not expire with age and remain available until corrected or forgotten, subject to explicit Google-derived data deletion.

No arbitrary item, token-cost, or convenience cutoff may decide what Florence can know or recall. Provider page sizes are transport details and must be exhausted. A model call may receive relevance-ranked context, but the complete reviewed corpus remains durably indexed and retrievable for later questions and proactive judgment; bounded presentation must never become silent data loss.

Google review uses only the distinction the family experience needs: a conclusion is useful to the household or it remains private to one adult. Topic labels such as school, travel, logistics, or adult coordination must not classify, route, or limit what Florence notices and can act on.

Foreground conversation is different: Florence may run a focused Gmail search, open a supported Gmail attachment, list calendars, and read the primary, selected, or all readable calendars needed for the current question.

The parental unit is the knowledge unit for validated family facts. Raw private email and personal Calendar details do not need to become a second adult's feed. One specific boundary applies to personal Calendar dates: if a potentially shared family date exists only on one adult's personal Calendar, Florence asks that owner before copying or naming it in the family Calendar. Once intentionally added to the family Calendar, Florence may describe it normally as household truth.

The Vault is one household knowledge store with useful views, not a collection of per-person silos or domain runtimes. It should show retained facts, preferences and routines, people, active work, and reusable artifacts such as recipes, lists, plans, notes, and references, with understandable sources and simple correction/deletion controls. A reusable artifact keeps enough detail to use and revise later: for a recipe, that includes the ingredients or canonical source, method, and family-specific substitutions or preferences. Categories such as Cookbook, School, or Trips may be UI facets and evaluation coverage; they do not route agent execution. Memory must be detailed and searchable enough for Florence to use in later action, not just display as trivia. The Vault should not appear empty after Florence has clearly learned useful family context.

Proactivity should connect those memories to the family's current situation. The benchmark is not announcing “pasta night is Saturday”; it is noticing the family plan, recalling the recipe or preference that makes it concrete, inferring the next piece of work, and offering to remove that work. Once accepted, Florence should prepare or execute the outcome, show a meaningful artifact or choice only when useful, accept corrections, and close the loop after the real result is known.

## Family Calendar

After both adults connect, Florence creates a new shared secondary Google Calendar and gives both adults normal access. It does not adopt an arbitrary existing calendar.

Clear family dates can be suggested or added in the shared family conversation. Ambiguous dates get one focused question. Personal Calendar titles are not silently copied into the group. Calendar changes are reported only after Google confirms them.

Resetting Florence removes the database state, invalidates setup links, and deletes Florence-created calendars and other Florence-created provider artifacts so the family can start cleanly.

## Reminders, monitoring, and durable work

Reminders are a first-class conversational capability. A parent can create, list, change, cancel, pause, resume, run, and recur reminders in ordinary language. Reminder copy says what the parent should do; it does not command them to “confirm” an invented outcome. A follow-up question after the reminder can naturally ask whether the thing is handled.

Finite monitoring is for an unresolved deadline, decision, risk, or handoff whose evidence may change. It speaks only when something materially changes or attention is needed. It does not resend stale wording on a timer.

Longer work is also first-class. Florence can accept a research, comparison, planning, document, travel, or local-service task, continue after the initial reply, survive a process restart, accept steering and cancellation, and deliver the result once. The implementation should deepen Florence's existing PostgreSQL due-work path rather than add a second workflow engine.

## Broad operator capabilities

Search, Gmail, and Calendar are only the first slice. Florence's capability order is:

1. conversational Gmail attachments and all-calendar reads;
2. maps, places, routes, travel time, and time zones;
3. live weather plus flight route, status, and alternatives;
4. complete reminder control;
5. linked public-page and PDF reading;
6. durable multi-step work using those real tools;
7. a prioritized household docket and document-to-action workflows;
8. Gmail communication, Contacts, Drive, Docs, Sheets, Slides, and Tasks;
9. browser/computer use for sites without an adequate API;
10. general outcome completion by dynamically composing those tools across apps, websites, calls, texts, and time.

The competitive scenarios below are practical rehearsals, not hardcoded intents, dedicated pipelines, or the boundary of what Florence can do:

- “DL 747 is delayed tonight—find options.”
- “What is on the docket this week?”
- Notice an unanswered school form and prepare the next action.
- Find and arrange a handyman.
- Coordinate an appointment around both adults' schedules and travel time.
- Reconcile family travel confirmations, calendars, current conditions, and changes.

When Florence can perform an outside action, she previews the meaningful choice to the relevant adult and reports success only after the provider confirms it. Keep this behavior in the general reasoner and the concrete tool adapter that performs the effect; do not add scenario-specific routers or build a universal approval or settlement framework in advance.

## Pi and Hermes reuse

Start each assistant-capability ticket from the pinned open-source upstreams before writing a Florence-owned equivalent:

- Pi `4e494929998d6bc4fccf75e0a233f727db4b70ee` at `/Users/harianbarasu/Projects/florence-upstreams/pi`;
- Hermes Agent `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882` at `/Users/harianbarasu/Projects/florence-upstreams/hermes-agent`.

Directly port or adapt the useful tool contracts, provider integrations, workflows, lifecycle behavior, and closest focused tests. Record the exact upstream files and reuse mode in the ticket resolution. Do not copy their coding, shell, terminal, repository, or arbitrary-filesystem tools.

The current useful reuse targets are:

- Pi's general model/tool loop, ordered validation and settlement, cancellation, steering/follow-up, progress events, and structured compaction at intact tool boundaries;
- Hermes's typed registry, context-dependent tool contracts, provider-neutral memory lifecycle, contextual memory-query rewriting, high-signal memory consolidation, and normal-agent scheduled wakeups;
- Hermes/OpenViking's general resource search/read model for arbitrary source-linked Vault artifacts rather than a recipe- or document-specific memory path;
- Hermes's concrete maps, reminder, Google Workspace, browser, telephony, and useful provider adapters where their contracts match Florence's real environment; and
- Pi/Hermes background-work semantics applied to the parent's arbitrary objective, never a copied catalog of scenario templates.

Ollie's public family-assistant examples are additional product evidence, summarized with first-party citations in `docs/research/ollie-product-capabilities.md`. Use them to test observable household behavior—cross-source judgment, reusable artifacts, ownership, proactive follow-through, and natural closure—not as evidence for Ollie's undocumented architecture or as a list of runtime categories.

Primary-source evidence about parents' recurring coordination load is summarized in `docs/research/family-coordination-primary-source-brief.md`. Use its general notice → understand → reconcile → decide → act → confirm → remember loop and acceptance scenarios as product rehearsals, never as vertical routing categories or separate mini-products.

The broader parent-app boundary review in `docs/research/parent-jobs-and-app-friction-2026-08-29.md`
sharpens that bar to change-to-closure: Florence should keep one objective alive across notices,
calendars, portals, people, purchases, and time until the household has a confirmed outcome. Its
ranked scenarios are rehearsals for the same general agent, never runtime categories.

Keep the small typed tool-execution kernel already ported from Pi/Hermes. Remove speculative registry versions, generic policy/evidence envelopes, fake facades, and framework-only tests. Progressive discovery or a connector catalog can be reconsidered only after the real tool set makes either one necessary.

## Implementation rules

- Deepen the existing reasoner, Google adapter, PostgreSQL due-work path, Linq delivery, and concrete provider modules.
- Do not add generalized safety/privacy infrastructure, a second assistant runtime, a second scheduler, a generic connector framework, or unrelated tests.
- Add the smallest focused verification for the behavior being changed, preferably by extending the closest existing family conversation or provider case.
- Do not reset production as an implementation shortcut. A user-requested reset includes Florence-created provider artifacts.

## Completion

Each tranche is complete when the relevant behavior works through real iMessage conversations with the real provider, not merely when internal abstractions compile.

The final questions are experiential:

- Did Florence visibly start and finish useful work?
- Did she derive what she could instead of asking avoidable questions?
- Did the household receive prioritized action rather than repetitive noise?
- Did the family Calendar and private Calendar boundary feel intuitive?
- Did Florence remove mental load?
- Would the family miss her if she disappeared tomorrow?

If those answers are not yes, more infrastructure is not the default remedy.
