# Florence

## Status

This is the controlling product contract for the Florence household pilot. It
replaces the founder-only checkpoint and the old Phase 1/Phase 2 split.

Florence is judged by the lived two-parent iMessage and mobile-web experience,
not by infrastructure, abstraction, schema breadth, or test count. The next
production reset happens only after the complete household loop is ready for a
real rehearsal with two phones and two Google accounts.

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
quiet when nothing useful should be said.

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
compare, recommend, monitor, draft, interpret family documents, remember, and
coordinate the enrolled parents. The pilot's external writes are deliberately
narrow: partner invitation, exact family-group creation, and approved family
Calendar changes.

## What Florence should feel like

Florence is warm, observant, concise, confident, lightly funny, and proactive
when something genuinely matters. She behaves like a partner and near-member of
the household without pretending to be human. She is comfortable saying
nothing, recovers naturally after corrections or interruptions, and is honest
when blocked or when an action is not yet complete.

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
- may react or stay silent during ordinary parent conversation;
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
3. partner first and last name plus mobile number;
4. each child's first name, optional last name, school, and activities;
5. home ZIP for coarse local discovery, with no street address requirement;
6. a surname-derived family label shown as “Anbarasu Family” for one surname or
   “Anbarasu–Smith Family” for two surnames;
7. review and completion.

The Google permission says plainly that Florence will look for family-related
things in Gmail and Calendar, remember useful details, add clear school and
family dates to the family calendar, and speak up when something needs
attention. Personal details stay private unless that adult directs sharing.
Each adult agrees independently and can revoke the behavior later.

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

Florence returns with at most three findings that deserve attention now. It asks
“Did I get that right? If I missed something, tell me here.” If a real decision
is blocked, it asks that one concrete question instead. It does not end with a
generic delegation prompt.

The household briefing combines separately processed, permissioned context
from both adults. Raw private contexts never enter one shared model call. Only
minimum typed household conclusions cross into the group; owner-private details
and provenance remain available to that adult.

If nothing needs attention in the one-time review, Florence says it checked both
calendars and recent family email, nothing needs attention now, and it will keep
watching. Later empty checks remain silent. If one account is unavailable,
Florence does not claim an all-clear; it privately tells that adult and retries.

## Proactive intelligence

Florence removes mental load by looking before asking. One durable due-work seam
owns three behaviors:

1. continuous household triage of connected Gmail and Calendar changes;
2. finite monitoring of one unresolved decision, deadline, risk, or handoff;
3. ongoing discovery rooted in a durable family interest.

### Initial and continuous Google review

The first pass searches the previous 90 days for family-relevant material,
including children, schools, activities, appointments, travel, forms,
deadlines, and household logistics, while prioritizing the most recent two
weeks. Promotions, social mail, and irrelevant personal correspondence are not
summarized. Florence examines roughly the next three weeks of each adult's
Calendar.

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

### Finite monitors

Florence creates a finite monitor automatically when a family source reveals an
unresolved matter worth following. A monitor has a clear objective, current
conclusion, next check, and real end condition. It rereads current sources when
due and speaks only for a material change, correction, decision window, or
failure requiring intervention. It never sends stale reminder copy blindly.

### Interest discovery

One clear stable statement such as “Lexi likes soccer” may become a durable
interest; a casual mention does not. Interest discovery begins immediately
after household activation and searches public sources using only generic
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

Personal primary calendars remain private and read-only to their owner.
Conflict comparison reveals only minimum busy meaning in the group and sends
private title/detail to the affected adult. Disconnecting one account should
not unnecessarily stop household use through the other parent's shared-calendar
access. Privately request reconnection and tell the group only when reliable
family-calendar operation is actually impaired.

## Native iMessage and source inputs

iMessage is the primary surface. Florence uses inline replies, native
reactions, typing presence where supported, naturally paced bubbles, rich link
previews, and useful silence.

Launch input includes:

- ordinary and forwarded text;
- public links;
- screenshots and JPEG, PNG, WebP, and HEIC photos;
- PDFs;
- Gmail message bodies and relevant Gmail PDF/image attachments;
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

Linq does not expose a trustworthy marker for ordinary text that a parent
forwarded, pasted, or typed themselves. Florence therefore evaluates the
ordinary text portion of a signed Message from the verified parent as that
parent's current utterance. She uses semantic judgment and asks once when a
consequential meaning is genuinely ambiguous; she does not add forwarded-text
regexes, phrase lists, or magic approval wording. An inline Calendar or partner
invitation approval is eligible for the isolated approval pass only when it
replies to Florence's exact sent approval prompt. An ordinary unthreaded
natural-language approval remains frictionless. Canonical carrier opt-out stays
an ingress rule.

Raw media is encrypted only for bounded processing and retry and is discarded
within 24 hours unless a parent explicitly asks to retain it. Useful facts,
dates, conclusions, and provenance may remain. The Vault is not an automatic
archive of every attachment.

## Memory, Vault, Workspace, and Preferences

Memory has temporary, adult-private, and household-shared scopes. Parent-stated
facts, clearly extracted stable logistics, and useful inferences remain
distinguishable. Every retained item explains what Florence believes, how it
learned it, who can see it, and how to correct or delete it.

Florence automatically retains obvious stable family facts and creates useful
monitors under the one-time proactive permission. Sensitive personal details
remain private unless their owner directs sharing. An explicit no-retention
instruction overrides automatic memory.

New evidence may replace an obviously superseded fact or date while retaining
the current supporting source and telling the family what changed. Ambiguity or
a consequential change preserves current state and asks one focused question.

The Vault shows and allows correction or deletion of:

- adults, children, surnames, ZIP, schools, activities, caregivers, and useful
  household facts;
- the current supporting source and who can see each fact;
- active finite monitors and interest discoveries;
- what Florence is watching and its latest useful conclusion;
- plain controls to pause, stop watching, or correct an item.

The Workspace shows the one current household action plus partner, exact-group,
Google, family-calendar, and initial-briefing state. It is not an activity feed,
task dashboard, metric panel, or systems console.

Preferences include the adult's private Google connection, proactive-use and
private-conflict-sharing permissions, account, and sign-out.

The visual benchmark is Instinct's sparse authenticated and mobile setup
experience: a calm narrow column, measured typography and spacing, one primary
action, no wizard chrome, no dense all-family form, and no decorative dashboard
widgets. Preferences live in the account menu. Use licensed typefaces or honest
system fallbacks; never claim a font that is not bundled.

## Autonomy and actions

Research, reading, organizing, drafting, narrowing, comparing, remembering,
monitoring, and discovery are Florence's job. She does not ask permission to do
harmless useful work already covered by setup consent.

Florence may automatically:

- read and research;
- organize context and retain allowed facts;
- create drafts and internal monitors;
- coordinate the enrolled parents in their private and family threads;
- create the family group and shared family calendar;
- perform clear family-calendar changes under both adults' standing rule.

Anything that costs money, contacts an outside person as the family, submits or
books something, shares private information, commits a person, or makes another
irreversible external change comes back once with the exact action first.
Calendar or invitation success is reported only after the provider confirms it.

Sending external email, contacting schools, purchases, bookings, and form
submission are not in this goal. Florence may produce exact ready-to-send
drafts. Drive, Docs, Sheets, Slides, arbitrary Office files, PDF authoring, and
PDF editing are the immediate breadth follow-up after the household loop is
proven.

## Implementation order

Build one vertical household product, deepening existing modules and replacing
obsolete paths in place:

1. **Partner handoff:** collect surname/phone, move invitation approval into
   Messages, and complete the second adult's independent private setup.
2. **Household activation:** automatically create and bind the exact family
   group and Florence-created family calendar with equal adult authority.
3. **Initial intelligence:** run founder-private and combined household
   briefings from bounded Google reads and attachments.
4. **Durable proactivity:** replace static follow-up text promotion with direct
   Google polling, finite monitors, and interest discovery in the same Florence
   process.
5. **Native inputs:** complete HEIC, voice-note, and Gmail attachment handling.
6. **Web and Vault:** finish the Instinct-quality setup, Workspace, Vault,
   Preferences, monitoring controls, and settings handoff.
7. **Production rehearsal:** verify internally, reset once, deploy the
   integrated loop, and run the real two-parent household journey.

Do not deploy a partial founder-only product again.

## Structural anti-bloat laws

1. Every change begins: **The family can now ___ in iMessage or on the web.**
2. Deepen an existing product module before adding another module.
3. Do not add an abstraction until two current concrete callers need it.
4. Keep one durable source of household truth; no feature workflow engines.
5. Replace obsolete paths in the same change. No compatibility layers, dual
   writes, or permanent fallbacks.
6. A new table, module, interface, queue, process, dependency, route, persistent
   status, or test must explain why the existing core cannot own the behavior.
7. Architecture changes must reduce the concepts a future engineer must learn.
8. Boundaries exist only to protect a family, secret, identity, or irreversible
   external action.
9. Completion is a real-world demonstration, not an extensible framework.
10. For the first roughly 100 households, prefer one process, ordinary database
    transactions, uniqueness constraints, and repairable failures over leases,
    distributed locks, proof ledgers, workflow state, or scale-first queues.

The fresh pilot schema may include family-calendar binding, incremental source
cursors, and durable monitor/discovery state because those are user-visible
product behaviors. Do not hide provider operational state in profile JSON or
facts. Do not add a worker, generic queue, sync service, connector registry, or
workflow engine; the existing Florence loop owns due work.

## Minimum test doctrine

Keep exactly the minimum scenario suite:

1. one complete two-parent setup journey: founder setup and Google, one partner
   invitation question, partner setup and Google, then automatic exact family
   group and shared family calendar creation without visible duplicates;
2. one proactive-family journey: separate private reviews, a safe combined
   briefing, a forwarded family input, current source-linked memory, a finite
   monitor, one automatic family date, and the read-only Calendar page;
3. one equal-authority and privacy journey: private context stays private,
   either adult can correct household state and change the family calendar, and
   one changed-participant group is retired and replaced.

A regression extends the closest existing narrative. A fourth automated test
requires a genuinely different expensive boundary and explicit user approval.
Typecheck, build, direct inspection, provider-shaped probes, and the real
rehearsal carry the rest. Fake reasoners prove orchestration and deterministic
boundaries, not conversational quality.

## Explicit non-goals for this goal

- more than roughly 100 households or more than two participating adults per
  household;
- child accounts;
- Florence joining or passively watching outside parent groups;
- non-iMessage messaging networks;
- Drive, Docs, Sheets, Slides, and arbitrary Office-file browsing;
- sending external email or messages as a parent;
- contacting schools or institutions;
- purchasing, booking, or submitting forms;
- native mobile apps;
- generic task/project management;
- mailbox, Drive, or document mirroring;
- a connector registry, workflow engine, separate worker, or model-provider
  portability framework;
- analytics or self-learning infrastructure not required for the rehearsal.

## Completion and release gate

The next production reset occurs only after the integrated loop is internally
green. Completion requires a real rehearsal with the user and partner, real
phones, real Google accounts, Linq, OpenAI, PostgreSQL, and the production-shaped
deployment:

1. founder texts Florence and completes mobile setup;
2. Florence privately asks to invite the named partner;
3. partner independently completes Messages, web, and Google setup;
4. Florence creates the exact group and shared family calendar;
5. Florence produces a useful combined briefing without being handed a task;
6. a parent sends or forwards a real family message, image, PDF, or voice note;
7. Florence identifies a real conflict, deadline, handoff, or decision and
   stores inspectable household knowledge;
8. Florence performs one clear or approved family-calendar change exactly once
   and reports verified completion;
9. one automatic monitor produces a meaningful update or useful silence;
10. one durable interest produces a sourced, genuinely relevant opportunity;
11. a parent corrects something naturally and the group, calendar, monitor, and
    Vault converge on the corrected truth.

Two gates are absolute: no unauthorized disclosure from one adult's private
context into the household, and no unauthorized consequential action.

The final questions are experiential:

- Did Florence feel perceptive and easy to talk to?
- Did she remove mental load without turning family life into a workflow?
- Did the group feel like Florence's natural home?
- Did each adult trust what stayed private and what became shared?
- Did mobile web feel as calm and obvious as Instinct?
- Did Florence complete real actions exactly once and tell the truth?
- Would the family miss her if she disappeared tomorrow?

If those answers are not yes, more infrastructure is not the default remedy.
