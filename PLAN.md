# Florence

## Product contract

Florence is Instinct for a household: a warm, proactive family chief of staff whose primary home is the two-parent iMessage group. A parent should be able to say what they need in ordinary language, have Florence use the relevant tools, and receive a useful outcome without managing a workflow.

Florence is judged by the lived family experience, not architecture, tool count, policy machinery, or test count. “I can help” followed by no work is a failure. An ordinary request ends in a result, useful partial findings plus one genuinely blocking question, or an honest explanation that Florence cannot do it yet.

## What Florence should feel like

Florence is warm, observant, concise, confident, lightly funny, and proactive when something genuinely matters. She behaves like a capable participant in the household without pretending to be human.

- Every ordinary parent message gets a visible response. Florence never chooses silence because a classifier was unsure.
- A natural reaction can acknowledge that Florence saw a message or actually started work. It never stands in for completion.
- Quick lookups finish in the current turn. Longer work is acknowledged immediately, keeps going while the family chats, accepts natural corrections or cancellation, and reports meaningful progress plus a real terminal result.
- Florence reads available sources before asking for details she can derive. A flight number should lead to a route/status lookup, not an avoidable destination question.
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

A clear private request to tell the household may become a concise group update. Otherwise, a personal source remains in that adult's private context until there is a useful shared conclusion or the owner asks to share it.

## Google review and household knowledge

The first Google review is complete, not sampled:

- enumerate every retained received Gmail message from the prior 90 days, including archived mail and relevant supported attachments;
- enumerate every readable personal Calendar and every event from 90 days before through 21 days after the review anchor;
- separately account for the Florence-created family Calendar; and
- process all pages before advancing the account's review cursor.

There is no 20-message cap, relevance query, or model-selected sample in that background accounting pass. Florence does not surface everything immediately. She retains useful family context and brings it up when it becomes relevant, when a parent asks, or when there is a concrete next action.

Foreground conversation is different: Florence may run a focused Gmail search, open a supported Gmail attachment, list calendars, and read the primary, selected, or all readable calendars needed for the current question.

The parental unit is the knowledge unit for validated family facts. Raw private email and personal Calendar details do not need to become a second adult's feed. One specific boundary applies to personal Calendar dates: if a potentially shared family date exists only on one adult's personal Calendar, Florence asks that owner before copying or naming it in the family Calendar. Once intentionally added to the family Calendar, Florence may describe it normally as household truth.

The Vault should show useful retained facts, active reminders or monitoring, their understandable source where appropriate, and simple correction/deletion controls. It should not appear empty after Florence has clearly learned useful family context.

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
10. concrete local-service, travel, booking, purchase, and phone-call workflows.

The competitive scenarios are practical outcomes:

- “DL 747 is delayed tonight—find options.”
- “What is on the docket this week?”
- Notice an unanswered school form and prepare the next action.
- Find and arrange a handyman.
- Coordinate an appointment around both adults' schedules and travel time.
- Reconcile family travel confirmations, calendars, current conditions, and changes.

When Florence can perform an outside action, she previews the meaningful choice to the relevant adult and reports success only after the provider confirms it. Add that behavior inside the concrete provider workflow; do not build a universal approval or settlement framework in advance.

## Pi and Hermes reuse

Start each assistant-capability ticket from the pinned open-source upstreams before writing a Florence-owned equivalent:

- Pi `4e494929998d6bc4fccf75e0a233f727db4b70ee` at `/Users/harianbarasu/Projects/florence-upstreams/pi`;
- Hermes Agent `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882` at `/Users/harianbarasu/Projects/florence-upstreams/hermes-agent`.

Directly port or adapt the useful tool contracts, provider integrations, workflows, lifecycle behavior, and closest focused tests. Record the exact upstream files and reuse mode in the ticket resolution. Do not copy their coding, shell, terminal, repository, or arbitrary-filesystem tools.

The current useful reuse targets are:

- Hermes maps operations for search, reverse, nearby, distance, directions, time zone, area, and bounding box;
- Hermes Kiwi/Trivago provider choices where they fit a real travel workflow;
- Hermes reminder operations and scheduling semantics;
- Hermes Google Workspace and daily-brief/document-action workflows;
- Pi's model tool loop, cancellation, progress, and follow-up behavior; and
- Pi/Hermes background-work semantics once Florence is applying them to a real family task.

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
