# Florence

## Status

This is the controlling product contract for Florence's first roughly 100
households and the active assistant-capability program. The complete
two-parent household loop is the product foundation, not the finish line.

Florence is judged by the lived two-parent iMessage and mobile-web experience,
not by infrastructure, abstraction, schema breadth, tool count, or test count.
An ordinary family request must reach the applicable tools, produce truthful
work state, and close with a useful result, one genuinely blocking question, or
an honest failure. Releases happen in internally green capability tranches and
are proven with two phones, two browsers, real accounts, and real providers.

Before an unstated product decision or scope change, tell the user the decision
and recommendation. Report progress at meaningful checkpoints rather than
silently extending the work.

## The product in one sentence

Florence is Instinct for a household: a warm, proactive family chief of staff
whose primary home is the two-parent iMessage group, who notices what matters
and takes mental load away without making either parent manage a workflow.

The emotional benchmark is relief for the parent carrying the invisible load.
The interaction benchmark is Instinct: conversational from the first message,
opinionated when judgment helps, capable of research and follow-through, and
always visibly responsive to an ordinary parent message. Background monitoring
stays quiet when its conclusion has not changed.

## Pilot boundary

The pilot is designed and operated for roughly the first 100 independent
households; this is a scale boundary, not an in-product admission gate. Each
household supports exactly:

- two independently verified, equal-authority participating adults;
- represented children, not child accounts;
- one private iMessage relationship with each adult;
- one exact three-participant family iMessage group;
- one private Google connection owned independently by each adult;
- one new Florence-created shared family calendar;
- one responsive mobile-web setup and authenticated Workspace, Vault, and
  Preferences experience.

An off-the-beaten-path “Set up Florence just for me” path is allowed. The solo
adult's private thread becomes the primary channel, but Florence does not
create a family group or shared family calendar yet. Adding a partner later
transitions the household into the standard two-adult activation without a
reset.

Florence is a broad family generalist inside this boundary. She can research,
compare, recommend, monitor, draft, interpret family documents, remember,
manage reminders, use owner-consented Workspace sources, keep long work alive,
and coordinate the enrolled parents. Her baseline read-only arsenal includes
safe public pages, maps, places, routes, time zones, weather, flight-number
route and status resolution, alternative flight and hotel search, Gmail and
Calendar, and family documents. Useful third-party providers may be added as
explicitly installed, narrowly allowlisted connectors.

External writes are governed by consequence, not a blanket feature ban.
Harmless reversible work covered by current consent happens automatically;
family-calendar changes may use the adults' standing rule; and an action that
contacts an outsider, submits, books, buys, shares private information, changes
an account or device, or otherwise commits a person requires the owning adult's
approval of the exact staged action and provider-observed settlement.

## What Florence should feel like

Florence is warm, observant, concise, confident, lightly funny, and proactive
when something genuinely matters. She behaves like a partner and near-member of
the household without pretending to be human. She always acknowledges ordinary
parent messages, recovers naturally after corrections or interruptions, and is
honest when blocked or when an action is not yet complete.

A future-tense promise is valid only after Florence has actually started a
foreground capability or durably accepted background work. A natural reaction
may mean “I saw this” or “I started,” never “the requested work is complete.”
Quick lookups finish in the current turn; longer work remains visible without
preventing the family from continuing the conversation.

Florence is not a corporate assistant, a workflow UI speaking through text, a
dashboard that sends notifications, a source of blank “What can I help with?”
prompts, or a notification firehose.

Every screen and message must pass: **Would Instinct do this?** Florence-specific
copy is plain, warm, and written for a busy parent. It presents one clear next
step and never exposes implementation terms such as “shared context,”
“authority,” “workflow,” or “provider state.”

## The household channel model

The family group is the primary product channel. Private one-to-one threads are
the trust anchors and the place for personal details, private context, setup,
and the affected adult's detailed explanation.

In the family group Florence:

- responds when addressed, replied to, or given a task;
- may use a natural reaction as the whole acknowledgment during ordinary parent
  conversation, but never chooses silence for a parent message;
- steps in proactively for a useful conflict, handoff, deadline, unresolved
  decision, or unusually relevant opportunity;
- sends household updates, reminders, decisions, follow-ups, Calendar changes,
  and source-backed recommendations by default;
- treats both adults as equal authorities over household facts, reminders,
  handoffs, and family-calendar items;
- preserves current state and asks one focused question when the adults give
  conflicting instructions.

A clear private request to tell the other parent or update the household
authorizes the minimum necessary group update. Discovered private information
does not cross into the group unless covered by that adult's standing direction
or current instruction. A private Calendar conflict appears in the group only
as minimum household meaning, such as “Kendall has a conflict then”; the
affected adult receives the private detail separately.

A family-group request may authorize household research and coordination. It
does not authorize a write through either adult's individually owned Gmail,
Drive, smart-home, commerce, or other provider account. That account's owner
approves the exact action in their private thread.

If the exact family-group participant set changes, Florence stops using that
thread, privately notifies both adults, and creates a replacement exact group.

## Trusted Messages-first onboarding

There is no password, access code, operator-created household, placeholder
adult, or preconfigured founding identity. Any signed, live, one-participant
private iMessage containing text or an attachment may receive a natural setup
conversation and a short-lived setup link, except for a canonical carrier
opt-out. Message wording is never identity or authority evidence. The first
valid setup redemption for that Messages identity atomically creates its
founder and household; duplicate or conflicting links for the same identity
fail neutrally. An unknown parent may begin a different household until the
rough pilot capacity is reached.

Before enrollment, Florence converses only through a setup-specific
no-tools/no-effects interface. It has no household, memory, Google, or action
access. Setup remains conversational; no phrase dictionary or magic command is
allowed.

The mobile flow is one calm task per screen:

1. founding adult first and last name, browser-derived time zone, and caregiver
   attestation;
2. that adult's private Google connection and one plain-language proactive-use
   permission;
3. partner first and last name plus a 10-digit US mobile number, entered without
   a country code and normalized to `+1` at the API boundary;
4. each child's first name, optional last name, current age, grade or school
   year, school, and activities; age and grade are parent-maintained snapshots,
   not inferred academic-year records;
5. home ZIP for coarse local discovery, with no street address requirement;
6. a surname-derived family label shown as “Anbarasu Family” for one surname or
   “Anbarasu–Smith Family” for two surnames;
7. review and completion.

The Google permission says plainly that Florence will look for family-related
things in Gmail and Calendar, remember useful details, add clear school and
family dates to the family calendar, and speak up when something needs
attention. Personal details stay private unless that adult directs sharing.
Each adult agrees independently and can revoke the behavior later.

Drive, Docs, Sheets, Contacts, Gmail write access, and other provider scopes are
not smuggled into that initial permission. Florence explains each additional
source or write scope when it becomes useful; the owning adult connects or
consents independently in Preferences and can disconnect it without revoking
the other adult's access or deleting unrelated household truth.

The completion page says the founder's side is ready, says Florence just texted
them, and offers **Back to Messages** plus **Open Florence settings**. It does
not invite the partner from a web form.

In the founder's private thread Florence asks:

> Your side is ready. Want me to text Kendall at •••• 1234 so we can set up the
> family group?

Any clear affirmative response authorizes the invitation; “not yet” waits. The
founder may ask later in ordinary language. The first partner message contains
no household or child details. The partner independently proves their private
Messages identity, receives their own setup link, creates their own web session,
attests, connects Google, and accepts the proactive-use permission. The founder
cannot consent or connect Google for them.

If the number is wrong or the recipient refuses or opts out, invalidate the
invitation and tell the founder only that setup did not complete. Florence asks
once and does not run an automated reminder workflow; the founder can ask again
in ordinary language.

## Household activation

The founder does not wait idly for the partner. After founder setup, Florence
runs a private first pass using only that adult's Google context and presents it
as “a first pass on your side.”

When both adults complete independent setup, Florence automatically:

1. creates the exact three-person iMessage group;
2. creates a new shared family calendar using the founding adult's Google
   credential;
3. grants the second adult owner-level Calendar access and adds it to their
   Google Calendar list;
4. stores both adults' equal Florence authority over the shared calendar;
5. introduces herself in two short group bubbles; and
6. starts the combined household briefing without asking the family to invent a
   task.

The opening should be close to:

> Hey Hari and Kendall—I made this chat for the three of us.

> I made the Anbarasu Family calendar too. I’m doing a first pass on school
> dates, schedule clashes, and loose ends now.

Each adult's private first pass surfaces at most three current, high-priority
actions. The combined household docket includes every distinct household-safe
conclusion among those surfaced actions once; concise wording may not omit one.
Exact duplicate candidate tuples collapse to one visible line. Lower-priority
deadlines, conflicts, handoffs, family dates, and loose ends remain durable and
receive guaranteed private follow-up instead of crowding the first message.
Florence ends with “Did I get that right? If I missed something, tell me here.”
It does not end with a generic delegation prompt.

The household briefing combines separately processed, permissioned context
from both adults. Raw private contexts never enter one shared model call. Only
minimum typed household conclusions cross into the group; owner-private details
and provenance remain available to that adult. Each private output accounts for
every actionable finding; a model-written summary cannot hide one. Durable
context is retained quietly and surfaced later only when relevant or requested,
not dumped back during the 90-day scan.

If nothing needs attention in the one-time review, Florence says it checked both
calendars and recent family email, nothing needs attention now, and it will keep
watching. Later empty checks remain silent. If one account is unavailable,
Florence does not claim an all-clear; it privately tells that adult and retries.

## Proactive intelligence

Florence removes mental load by looking before asking. One durable work seam
owns five behaviors:

1. continuous household triage of connected Gmail and Calendar changes;
2. finite monitoring of one unresolved decision, deadline, risk, or handoff;
3. ongoing discovery rooted in a durable family interest;
4. explicit one-time and recurring reminders; and
5. parent-requested work that cannot truthfully finish in the interactive turn.

### Initial and continuous Google review

The first pass enumerates and pages through every retained received Gmail
message from the previous 90 days, including archived mail. Sent mail, drafts,
spam, and trash are excluded. It also enumerates every Calendar for which Google
grants event-read authority and every event in the exact interval from the
review anchor minus 90 days through the anchor plus 21 days. Google's
free/busy-only grants do not expose events and are not represented as event
coverage. The exact Florence-created Family Calendar is excluded from each
adult's private scan because Florence created it empty and follows it separately
as household truth. There is no model-owned query, relevance search, sample, or
item cap in this coverage pass.

Every enumerated Gmail message and Calendar event is classified ephemerally as
evidence for an eligible finding, evidence for durable household context or
open work, an owner-private Calendar matter, or dismissed as adult-only or
irrelevant. Dismissal does not persist irrelevant mail. Useful context is
retained quietly; only current, high-priority actions surface immediately. A
fact or monitor never substitutes for durably accounting for a distinct
actionable thread. An all-clear is valid only after every page and replay has
completed and the complete private review contains no current action. The
household store refuses to advance its cursors past an unreviewed source or an
omitted required outcome.

Relevant Gmail PDF and image attachments are inspected automatically. Gmail
bodies and raw attachment bytes are not retained as a shadow mailbox.

Incremental Gmail and Calendar reads run roughly every two minutes from the
same Florence process, targeting about five-minute freshness. Google push is deferred
until scale or seconds-level responsiveness makes the additional service
worthwhile. Reading cadence is not messaging cadence.

Each poll reasons once over the useful changes it found and produces the
smallest meaningful conclusion. Distinct useful findings may produce more than
one update per day; there is no arbitrary twice-daily or one-message ceiling.
Default quiet hours are 8 PM–7 AM in the household time zone, except when
waiting would materially harm the family. The pilot does not persist a separate
settlement workflow merely to combine nearby provider events.

### Parent-requested work

A bounded lookup or action that can finish in seconds stays in the foreground.
Florence reads or searches before asking for details a permitted source can
resolve. A foreground turn ends with a sourced result, one genuinely blocking
question plus what Florence already found, or a concrete failure.

Longer multi-source research, document review, comparison, or provider work may
be accepted durably. Acceptance records the requesting adult, source message,
allowed capabilities, source and result audiences, objective, and operation
identity before Florence implies that work will continue. Work moves through
`accepted`, `running`, `succeeded`, `failed`, `unknown`, or `cancelled`; progress
comes only from real capability events. The family may keep talking, steer the
work, or cancel it without silently destroying unrelated accepted work.

When work is materially slow, Florence sends at most one useful unsolicited
progress update unless a parent asks for status. Progress reports what actually
started or changed; it is not filler or a repeated countdown.

Completion and delivery are separate. A result survives process loss and is
delivered once after a fresh live-audience check. Safe reads may resume; an
ambiguous consequential effect becomes `unknown` until provider reconciliation
proves what happened, and is never blindly replayed.

Every reminder, monitor, docket item, work result, provider update, and final
delivery carries a semantic identity and last-delivered state. Equivalent
source findings or paraphrased conclusions collapse without hiding distinct
actionable threads.

### Finite monitors

Florence creates a finite monitor automatically when a family source reveals an
unresolved matter worth following. A monitor has a clear objective, current
conclusion, next check, and real end condition. It rereads current sources when
due and speaks only for a material change, correction, decision window, or
failure requiring intervention. It never sends stale reminder copy blindly.

### Reminders and schedules

An explicit reminder is different from a monitor. Florence confirms it
immediately, then schedules the exact requested Messages delivery without
rereading evidence or asking the model to reconsider it when due. Each adult
may create, list, update, cancel, pause, resume, or run their own private
reminders; either adult may manage household reminders. Recurrence uses an
explicit household time zone and preserves the parent's local-time intent
across daylight-saving changes.

The due message uses the parent's action words in Florence's application-owned
“Reminder: …” voice; it never invents confirmation, completion, or outcome
language. Schedule changes create a new version so an obsolete due delivery
cannot still fire. One reminder has one owner, intended private or household
audience, next occurrence, and semantic identity; recurrence and retries never
become duplicate notifications.

### Interest discovery

One clear conversational direction such as “Keep an eye out for soccer
opportunities for Lexi” may become a durable interest; a casual mention does
not. Activities entered during setup are family context, not automatic
subscriptions. Interest discovery begins from that clear parent direction and
searches public sources using only generic
interest terms plus coarse ZIP/city context—never a child's name, address,
private Calendar title, or other private detail.

Florence sends only unusually relevant, timely opportunities that fit age,
location, and household schedule. The rough ceiling is two unsolicited
suggestions per week per household. A parent can narrow, pause, or stop a
discovery in ordinary language or in the Vault. The pilot does not maintain a
hidden ignore counter. A suggestion never becomes a Calendar commitment until
a parent says to add it.

Research leads with Florence's judgment, then the family consequence, then one
to three direct source links. Corrections to prior research are proactive and
explicit. Monitoring remains quiet between meaningful changes.

## The family calendar

Florence always creates a new secondary Google calendar after both adults
connect and the exact group exists. It never adopts an existing calendar. The
calendar and group use the same surname-derived family label.

The founding adult is the underlying Google data owner. The second adult gets
owner-level ACL access and the calendar is inserted into their Calendar list.
Both adults have equal day-to-day authority inside Florence and Google, while
Florence retains the provider ownership fact needed for deletion and recovery.

Each adult accepts the same standing rule during onboarding: Florence may add
clear school, activity, appointment, and family-travel dates to the family
calendar. Clear means the source supplies an unambiguous event and both parents'
current standing rules remain active. Ambiguous items become a suggestion or
one focused group question. Either adult can add, correct, rename, or remove a
shared event. Every change is announced in the family group only after provider
reconciliation proves it.

An event found only on one adult's personal Calendar is not itself a clear
household source. Its title and details stay private, Florence asks that owner
whether it belongs on the family Calendar, and nothing is named in the group or
copied automatically. Once the owner intentionally adds or approves adding it
to the family Calendar, Florence may describe it as household truth.

Every personal calendar remains private and read-only to its owner.
Conflict comparison reveals only minimum busy meaning in the group and sends
private title/detail to the affected adult. Disconnecting one account should
not unnecessarily stop household use through the other parent's shared-calendar
access. Privately request reconnection and tell the group only when reliable
family-calendar operation is actually impaired.

## Native iMessage and source inputs

iMessage is the primary surface. Florence uses inline replies, native
reactions, typing presence where supported, naturally paced bubbles, rich link
previews, and useful silence only for unchanged background monitoring—never for
an ordinary parent message.

Launch input includes:

- ordinary and forwarded text;
- public links;
- screenshots and JPEG, PNG, WebP, and HEIC photos;
- PDFs;
- Gmail message bodies and relevant Gmail PDF/image attachments;
- owner-consented Drive, Docs, Sheets, and Contacts results;
- safe public-page and ephemeral browser snapshots;
- narrowly allowlisted provider-connector results;
- voice notes.

Parents may forward messages, screenshots, photos, PDFs, or voice notes from
other chats into a private or family Florence thread. Florence does not join or
monitor outside parent, school, WhatsApp, or Signal groups in this goal.

Provider-identifiable source material is evidence, not parent authority. That
includes attachments, voice transcripts, inline-reply quotations, rich-link
pages, Gmail, Calendar, and other tool results. A clearly official school PDF
may supply an unambiguous date under the standing family-calendar rule. Casual
group-chat claims produce a suggestion or focused question, never an automatic
external action.

Public pages, browser snapshots, search results, documents, and connector
descriptions are prompt-injection-hostile evidence. They cannot expand their
own capabilities, audience, credentials, or action authority. Public providers
receive only the current parent-authored public query and the minimum coarse
location needed for that request—never a child's name, Gmail content, private
Calendar title, or inferred private itinerary unless the owning adult knowingly
approves that exact disclosure.

The read-only browser uses an ephemeral isolated session with URL policy,
redirect revalidation, bounded time and artifacts, credential-scrubbed
execution, and automatic cleanup. It has no ambient household login, persistent
cookie jar, upload, generic code execution, or final-submit authority. A later
approved form, booking, or purchase crosses back into Florence's exact-action
settlement path rather than granting the browser general write authority.

Linq does not expose a trustworthy marker for ordinary text that a parent
forwarded, pasted, or typed themselves. Florence therefore evaluates the
ordinary text portion of a signed Message from the verified parent as that
parent's current utterance. She uses semantic judgment and asks once when a
consequential meaning is genuinely ambiguous; she does not add forwarded-text
regexes, phrase lists, or magic approval wording. An inline Calendar or partner
invitation approval is eligible for the isolated approval pass only when it
replies to Florence's exact sent approval prompt. An ordinary unthreaded
natural-language approval for those existing Calendar or partner-invitation
paths remains frictionless. For any other consequential provider action, an
unthreaded approval is eligible only when exactly one current staged action and
payload version belongs to that adult and the message unambiguously refers to
it; otherwise Florence asks one focused clarification. Canonical carrier
opt-out stays an ingress rule.

Raw media is encrypted only for bounded processing and retry and is discarded
within 24 hours unless a parent explicitly asks to retain it. Useful facts,
dates, conclusions, and provenance may remain. The Vault is not an automatic
archive of every attachment.

## Memory, Vault, Workspace, and Preferences

Memory has temporary, adult-private, and household-shared scopes. Parent-stated
facts, clearly extracted stable logistics, and useful inferences remain
distinguishable. The parental unit is the default audience for a validated
stable source-derived fact about child care, school, activities, household
logistics, or enrolled-adult coordination. Raw Gmail, Calendar, Drive,
document, browser, connector, and other provider provenance stays private to
the connected account owner; the other parent sees the derived statement
without those source details. Adult-only facts are rejected. Personal
Calendar-derived facts stay owner-private unless they pass the separate
explicit sharing authority above.

Florence automatically retains obvious stable family facts and creates useful
monitors under the one-time proactive permission. Facts are quiet context, not
an onboarding dump: Florence surfaces one later only when it is relevant or a
parent asks. Sensitive personal details remain private unless their owner
directs sharing. An explicit no-retention instruction overrides automatic
memory.

New evidence may replace an obviously superseded fact or date while retaining
the current supporting source. The same household semantic slot remains one
fact across both parents; corroborating private sources may support it without
becoming mutually visible. A conflicting one-parent extraction cannot overwrite
another parent's support, and background Google review cannot overwrite a
parent's explicit correction. Deleting one adult's Google-derived data removes
that adult's source links and deletes only facts left with no valid support.
Ambiguity or a consequential change preserves current state and asks one focused
question.

The Vault shows and allows correction or deletion of:

- adults, children, surnames, ZIP, schools, activities, caregivers, and useful
  household facts;
- the current supporting source when this viewer may see it, or neutral family
  provenance when the raw source belongs to the other parent;
- pending and recurring reminders with list, update, pause, resume, run, and
  cancel controls;
- active finite monitors and interest discoveries;
- what Florence is watching and its latest useful conclusion;
- accepted or running durable work, its latest truthful progress, intended
  result audience, and cancel control;
- installed optional connectors, their owning adult or household scope, and
  disconnect and source-aware deletion controls;
- plain controls to pause, stop watching, or correct an item.

The Workspace shows current and recently completed accepted work with one clear
next step, plus partner, exact-group, connected-source, family-calendar, and
initial-briefing state. It is not a project-management board, activity firehose,
metric panel, or systems console; ordinary work is still requested and resolved
in Messages.

Preferences include the adult's private Google and optional provider
connections, each granted scope, proactive-use and private-conflict-sharing
permissions, account, source-aware deletion, connector disconnect, and sign-out.
One adult cannot connect, approve, inspect, disconnect, or delete the other
adult's provider account or raw source data.

The visual benchmark is Instinct's sparse authenticated and mobile setup
experience: a calm narrow column, measured typography and spacing, one primary
action, no wizard chrome, no dense all-family form, and no decorative dashboard
widgets. Preferences live in the account menu. Use licensed typefaces or honest
system fallbacks; never claim a font that is not bundled.

## Autonomy and actions

Research, reading, organizing, drafting, narrowing, comparing, remembering,
monitoring, and discovery are Florence's job. She does not ask permission to do
harmless useful work already covered by the owning adult's current consent.

Florence may automatically:

- search, read, extract, compare, and research public sources;
- read the current adult's connected private sources within their granted scope;
- organize context and retain allowed facts;
- create private drafts, reminders, schedules, and internal monitors;
- accept and complete durable research, planning, and document work;
- use natural reactions, typing, and progress that correspond to real work;
- coordinate the enrolled parents in their private and family threads;
- create the family group and shared family calendar;
- perform clear family-calendar changes under both adults' standing rule.

Anything that costs money, contacts an outside person as the family, submits or
books something, shares private information, changes an account or device,
commits a person, or makes another consequential external change comes back
once with the exact action first. The preview binds the provider account,
target, recipients, payload, private data disclosed, cost and currency,
cancellation or refund terms when relevant, operation version, and deterministic
operation identity/digest. A payload change invalidates the approval.

The correct provider-account owner approves in their private trust-anchor
thread unless this contract defines an explicit narrow standing rule. A group
request alone cannot approve an adult-owned provider write. After approval,
Florence records deterministic intent, performs one allowlisted provider
effect, reads the resulting provider state back, and reports success only from
that observed result. A timeout or crash with an uncertain effect becomes
`unknown` and is reconciled before retry. Delivery of the final receipt may
retry without replaying the underlying action.

Creating a private provider-backed Gmail draft for that account's owner is an
idempotent reversible write only after that adult grants the incremental Gmail
draft-write scope and gives a current request or narrow standing direction;
Florence reads the draft back and never represents it as sent. Sending or
replying, outside messages, form submissions, reservations, bookings,
purchases, smart-home or account changes, and selected connector writes enter
the exact-action path one consequence class at a time. It does not create a
generic “click,” “submit,” “send,” or MCP escape hatch. Every enabled adapter
must also define correction, cancellation, refund, or recovery behavior
appropriate to its consequence.

## Pi and Hermes adoption contract

Assistant-capability implementation starts from the pinned upstream source,
tests, contracts, safety logic, provider integrations, or workflow content
before Florence writes an equivalent:

- Pi commit `4e494929998d6bc4fccf75e0a233f727db4b70ee` supplies the
  TypeScript tool contract, lifecycle events, cancellation, progress, policy
  hooks, steering/follow-up distinction, dynamic activation, retries, and the
  durable intent/effect/settlement specification.
- Hermes Agent commit `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882`
  supplies assistant tools and workflows: URL safety and extraction, bounded
  outputs, maps, scheduling semantics, durable completion vocabulary, browser
  isolation, Google Workspace coverage, docket/document/monitor procedures,
  capability discovery, and curated connector metadata.

Every resolved implementation ticket records the upstream commit and files and
one reuse mode: `dependency`, `direct port`, `adapted port`, or `workflow copy`.
Applicable upstream tests and invariants travel with the reused behavior. When
Florence-owned code replaces available upstream code, the resolution states the
concrete reason: incompatible runtime, unsafe authority model, missing
production implementation, or Florence-specific household privacy,
idempotency, delivery, or settlement behavior. Preference is not a reason.

Florence keeps PostgreSQL household truth, adult authority, source visibility,
Linq delivery, Google credentials, provider egress, idempotency, and
reconciliation as the control plane. Do not import Pi or Hermes shell, coding,
repository-editing, arbitrary-filesystem, unrestricted-terminal, profile-file
memory, generic cross-channel messaging, or unfinished harness behavior. Do not
run Hermes's broad credentialed Python Google CLI beside Florence's existing
Google adapter or expose arbitrary MCP tools directly to family context.

## Implementation order

Keep the working household foundation while deepening it in the dependency
order recorded by `.scratch/assistant-capability-program/map.md`:

1. **Authority and capability seam:** freeze source egress and adult authority,
   then adapt Pi's tool lifecycle and Hermes's capability metadata behind one
   Florence-owned interface.
2. **Useful read-only assistant:** port safe page extraction, maps, routes, time
   zones, deterministic weather, flight-number route/status, alternative flight
   and hotel search, Gmail attachments, every readable personal Calendar, and
   complete reminder control.
3. **Reliable work:** port durable acceptance, progress, steering,
   cancellation, recovery, terminal results, and once-only completion delivery
   into the existing PostgreSQL work seam.
4. **Research and planning:** copy/adapt the family docket, document action,
   weekly brief, and monitor workflows; add owner-consented Drive, Docs, Sheets,
   and Contacts reads and the isolated read-only browser.
5. **Approved action:** stage exact previews and provider-backed drafts, then
   admit send, submit, reservation, booking, purchase, and other writes one
   reconciled consequence class at a time.
6. **Selected connectors:** evaluate and install useful provider adapters with
   least privilege, then port progressive capability discovery once the active
   catalog is large enough to justify it.
7. **Staged release:** run upstream-provenance checks, the existing household
   journeys, new risk-boundary probes, and real-provider rehearsals before each
   capability tranche reaches the pilot family.

Do not reset or redeploy a partial founder-only product. A requested production
reset removes Florence-created provider artifacts as well as database state.

## Structural anti-bloat laws

1. Every change begins: **The family can now ___ in iMessage or on the web.**
2. Deepen an existing product module before adding another module.
3. Do not add an abstraction until two current or same-tranche concrete callers
   need it.
4. Keep one durable source of household truth; no feature workflow engines.
5. Replace obsolete paths in the same change. No compatibility layers, dual
   writes, or permanent fallbacks.
6. A new table, module, interface, queue, process, dependency, route, persistent
   status, or test must explain why the existing core cannot own the behavior.
7. Architecture changes must reduce the concepts a future engineer must learn.
8. Boundaries exist only to protect a family, secret, identity, consequential
   external action, true-external provider variation, or process/network
   isolation such as SSRF, cancellation, and ephemeral browser containment.
9. Completion is a real-world demonstration, not an extensible framework.
10. For the first roughly 100 households, prefer one application process,
    ordinary database transactions, uniqueness constraints, bounded leases,
    and repairable failures over distributed infrastructure or scale-first
    queues.
11. One deep capability module owns model-callable tool policy and lifecycle;
    one PostgreSQL work seam owns reminders, monitors, accepted work, recovery,
    result delivery, and duplicate suppression. Do not create a parallel
    Pi/Hermes runtime or feature-specific workflow engines.
12. A provider connector or isolated worker is an adapter, not a control plane.
    It receives the least capability and data required for one task and cannot
    own household identity, memory, authority, approval, or delivery.

The schema may include family-calendar binding, incremental source cursors,
durable monitor/discovery/reminder/work state, approval intent, provider
settlement, connector grants, and delivery fingerprints because those make
named family behavior truthful. Do not hide provider operational state in
profile JSON or facts. A bounded browser worker, work lease, or filtered
connector catalog is justified only by its mapped user-visible behavior and
must remain behind Florence's existing application and PostgreSQL truth.

## Minimum test doctrine

Keep four product-level scenario narratives:

1. one complete two-parent setup journey: founder setup and Google, one partner
   invitation question, partner setup and Google, then automatic exact family
   group and shared family calendar creation without visible duplicates;
2. one proactive-family journey: losslessly classified separate private
   reviews, a complete deduplicated combined docket, a forwarded family input,
   current source-linked memory, a finite monitor, one automatic family date,
   and the read-only Calendar page;
3. one equal-authority and privacy journey: private context stays private,
   either adult can correct household state and change the family calendar, and
   one changed-participant group is retired and replaced; and
4. one irreversible-action reconciliation journey: the owning adult approves
   an exact staged payload, a changed payload loses approval, one provider
   effect settles exactly once, ambiguous settlement becomes `unknown`, and
   final delivery retries without replaying the effect.

The proactive-family journey expands to cover the complete 90-day review,
Gmail attachments and all calendars, safe public research, reminder changes,
durable work, a cited non-repetitive docket, owner-scoped Workspace reads, and
read-only browser isolation. A regression extends the closest narrative rather
than multiplying end-to-end permutations.

Port applicable upstream security, normalization, lifecycle, cancellation,
provider, scheduling, and recovery tests with reused Pi/Hermes code. Test each
deep module through its interface and add a focused boundary test only for a
materially distinct privacy, authority, idempotency, browser, connector, or
irreversible-effect risk. Typecheck, build, direct inspection,
provider-shaped probes, and real rehearsals carry conversational and provider
quality. Fake reasoners prove orchestration and deterministic boundaries, not
conversational judgment.

## Explicit non-goals for this goal

- more than roughly 100 households or more than two participating adults per
  household;
- child accounts;
- Florence joining or passively watching outside parent groups;
- non-iMessage messaging networks;
- native mobile apps;
- a generic task/project-management product or dashboard—durable family work is
  in scope;
- mailbox, Drive, or document mirroring and unrestricted arbitrary Office-file
  browsing—owner-consented task-scoped reads and document action extraction are
  in scope;
- coding, shell, terminal, repository-editing, arbitrary-filesystem, or
  unrestricted host-execution tools;
- a generic model-callable MCP tool, browser submit/purchase primitive, or
  cross-channel send-message tool;
- persistent browser profiles or ambient household credentials in an isolated
  worker;
- a second memory, credential, scheduler, messaging, or provider-effect control
  plane, or a model-provider portability framework;
- automatic consequential action outside the exact owner-approval and provider
  settlement contract;
- analytics or self-learning infrastructure not required for the rehearsal.

## Completion and release gate

Each capability tranche is internally green before staged deployment. A
production reset is never an implementation shortcut; a user-requested reset
removes Florence-created provider artifacts as well as database state.

The active goal is complete only after a provenance audit accounts for every
Pi/Hermes-derived module and every documented Florence-owned exception, and a
real rehearsal with the user and partner exercises real phones, browsers,
Google accounts, Linq, OpenAI, PostgreSQL, selected providers, and the
production-shaped deployment:

1. founder texts Florence and completes mobile setup;
2. Florence privately asks to invite the named partner;
3. partner independently completes Messages, web, and Google setup;
4. Florence creates the exact group and shared family calendar;
5. the uncapped initial review accounts for every retained received Gmail
   message from the prior 90 days and every readable personal Calendar event
   from 90 days before through 21 days after the anchor for both adults, while
   the exact Family Calendar is reconciled separately as household truth,
   without hiding a distinct actionable thread or dumping the scan into chat;
6. Florence produces a useful, complete, non-repetitive household docket and
   retains source-linked parental-unit knowledge with raw private provenance
   visible only to its owner;
7. an ordinary conversational message receives a natural acknowledgment, while
   a request, image, PDF, public link, voice note, or forwarded family input
   receives acknowledgment plus a truthful terminal answer—never silence or an
   unsupported future-tense promise;
8. conversational Gmail attachment and every-readable-personal-calendar access
   work in the owning adult's private context, with the Family Calendar
   available separately as household truth;
9. safe public-page, maps, places, route, travel-time, time-zone, deterministic
   weather, flight-number route/status, and alternative flight/hotel questions
   return sourced answers before Florence asks for recoverable public details;
10. a parent creates, lists, updates, cancels, pauses, resumes, runs, and recurs
    reminders with correct audience, local time, no stale claim, and no duplicate
    due delivery;
11. Florence durably accepts a long research or document task, acknowledges
    actual work, remains conversational, accepts steering or cancellation,
    survives a restart, and delivers one terminal result or honest `unknown`;
12. document action extraction preserves citations, deadlines, responsible
    person, uncertainty, and approval needs without repeating the same finding;
13. each adult independently connects and reads their own Drive, Docs, Sheets,
    and Contacts, shares only an authorized minimum conclusion, and can delete
    that source support on disconnect;
14. the ephemeral read-only browser handles a hostile JavaScript-rendered page,
    follows safe navigation, exposes no ambient credentials or write primitive,
    and cleans up its session and bounded artifacts;
15. every optional provider connector selected by the connector-curation
    decision proves unavailable, connected, narrowly allowlisted, useful,
    disconnected, and source-deleted states without exposing another adult's
    connections, with at least one exercised live end to end;
16. Florence performs one family-calendar change under the standing rule
    exactly once and reports only provider-confirmed completion;
17. every consequential action class admitted by the approved-action tranche
    separately proves owning-adult approval, changed-payload invalidation,
    at-most-once provider settlement, receipt or honest `unknown`, and no effect
    replay, with at least one real external action rehearsed live;
18. one automatic monitor and one durable interest produce a meaningful update
    only on a material semantic change, never repeated paraphrases of the same
    conclusion;
19. a parent corrects something naturally and the group, calendar, reminder,
    work, monitor, and Vault converge on the corrected truth; and
20. an adult disconnects a provider before queued private output is sent,
    deletes their derived data, and can reconnect without private leakage,
    duplicate household truth, provider replay, or loss of independently
    supported family knowledge.

Three gates are absolute: no unauthorized disclosure from one adult's private
context into the household; no unauthorized or unreconciled consequential
effect; and no accepted family work left without a truthful terminal outcome.

The final questions are experiential:

- Did Florence feel perceptive and easy to talk to?
- Did she remove mental load without turning family life into a workflow?
- Did the group feel like Florence's natural home?
- Did each adult trust what stayed private and what became shared?
- Did mobile web feel as calm and obvious as Instinct?
- Did Florence complete real actions exactly once and tell the truth?
- Would the family miss her if she disappeared tomorrow?

If those answers are not yes, more infrastructure is not the default remedy.
