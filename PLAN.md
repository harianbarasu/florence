# Florence — Product Contract

Status: active pilot contract

Date: 2026-08-12

Repository: `harianbarasu/florence`

Production: Railway API + worker + PostgreSQL

Florence is one persistent family Chief of Staff. She notices real family obligations, gets a person
to own them, follows up without blame, and closes the loop in iMessage. The model may interpret; it
never owns authority or durable state.

This document supersedes earlier Florence architecture plans. Product goals and safety invariants
survive. The former feature-specific state machines, broad connector framework, and Codex app-server
runtime do not.

## The promise

A parent should be able to:

1. Set up their family from a mobile web page.
2. Connect the other parent privately to the same family.
3. Add Florence to the exact two-parent iMessage group.
4. Mention a real obligation naturally.
5. See Florence ask for ownership, follow up at the useful time, and stop once it is handled.
6. Connect personal Google, privately review useful email-derived meaning, deliberately share only
   the minimum family meaning, and explicitly approve a Calendar write.

The product is not a chat dashboard, project manager, generic task engine, or autonomous agent
platform. The web app is setup and authority. The family conversation is where normal coordination
happens.

## Pilot journey

### 1. Mobile family setup

The founding parent signs into the private pilot session and creates a household. The dashboard can
collect and later edit:

- each adult's name and family role;
- each child's preferred name and aliases;
- birth year;
- school or daycare;
- current grade, academic year, and effective date as one coherent fact;
- activities; and
- household time zone.

Birth year is stored instead of a permanently stale age. Children are represented family members,
not authenticated users. A planned adult gains no authority merely by being entered in a form.

### 2. Private adult enrollment

The steward asks Florence to connect one planned adult. Florence issues a retry-stable, expiring,
one-use code; only its keyed digest is stored. The adult sends the whole code in a private iMessage
to Florence. The live Linq chat must contain exactly that sender. Redemption atomically verifies the
adult and binds the private conversation. The raw code never becomes family conversation history or
model context.

Each adult has a separate browser session and a separate Linq identity. Browser input cannot choose
which adult it becomes.

### 3. Exact family group bootstrap

After both adults are verified, Florence may accept the first message from one group only when the
live Linq chat contains exactly the two verified identity digests for exactly one household. Any
third participant, missing adult, ambiguous household, stale binding, or provider-read failure
rejects the bootstrap without output or retained family meaning.

Every outbound iMessage re-reads the live chat and requires the same audience and identity set before
sending. Changing participants invalidates delivery authority.

### 4. One family episode closes

An ordinary group message can propose one concrete family episode: a source-linked outcome with an
optional due time and suggested owner. Florence may ask one narrow question when meaning is missing.

The lifecycle is deliberately small:

- proposed;
- owned by one verified adult;
- updated or reassigned by an authorized current message;
- completed or cancelled.

Taking ownership must come from that adult. Silence, delivery, read state, habits, or model inference
never assign responsibility. A due timer causes reevaluation, not automatic authority. Follow-up is
neutral and factual. Completion cancels stale timers and prevents duplicate effects.

### 5. Private Gmail meaning can be promoted deliberately

An authenticated Google connection belongs to one verified adult. Refresh credentials are encrypted
outside household events and never enter browser responses, model input, logs, or signals.

Gmail polling emits only immutable message/thread/history identifiers. Florence reads normalized
message evidence just in time for that owner’s private context. The model may stage a private
candidate. Raw mail remains private. Only an explicit current private message from the owner may
promote a stored candidate's minimum operational meaning into family context.

No Gmail body, sender detail, or attachment enters the group unless the owner explicitly shares that
exact meaning.

### 6. Calendar writes require exact approval and proof

A stored private Gmail candidate may contain a proposed Calendar draft. Only the owner can approve
that exact candidate version and digest from a current private message. Florence then creates one
idempotent Calendar effect.

The Google adapter derives a deterministic event ID, inserts or reconciles the event, always rereads
it, and verifies the canonical summary, time, time zone, status, and Florence approval markers. Only
that reread proof can commit the receipt. Uncertain writes retry; mismatches fail closed.

## Authority and privacy invariants

- PostgreSQL events, signals, effects, timers, connections, and encrypted artifacts are canonical.
- `HouseholdChiefOfStaff.accept()` is the only household mutation ingress.
- A worker can propose. It cannot grant identity, widen disclosure, assign authority, or directly
  canonize state.
- Provider payloads contain evidence, never household or person authority.
- Webhooks are authenticated before business parsing and deduplicated by stable source identity.
- Every inbound Linq message is resolved against current app-owned conversation and identity state,
  then checked against a live provider observation before media is fetched or retained.
- Personal source data is private by default. Promotion must cite the current owner message and the
  exact stored candidate.
- Images are magic-byte checked, size bounded, encrypted, authority-bound to household and signal,
  stored in PostgreSQL for API/worker sharing, and deleted after bounded retention.
- Consequential external writes require exact app-owned approval and a reconciled provider receipt.
- Model output is strict structured data, parsed again by the application, and has no tools.
- Model retries reuse one persisted deliberation for the same immutable input.
- Timers and effects are idempotent and leased. A singleton worker heartbeat is observable.
- Fail closed on ambiguous identity, stale conversation authority, invalid model output, privacy
  uncertainty, or an unprovable external result.

## Architecture

Florence has one small product spine:

```text
Linq / dashboard / timer / receipt
              |
              v
     HouseholdSignal (immutable)
              |
              v
 HouseholdChiefOfStaff.accept()
              |
    +---------+----------+
    |         |          |
  events    timers     effects
    |                    |
 snapshot            Linq / Google
```

The production modules are intentionally narrow:

- `apps/api`: authenticated web commands, raw Linq webhook ingress, Google OAuth, static web app.
- `apps/worker`: deliberation, Gmail polling, due timers, effect execution, singleton lease.
- `apps/web`: mobile onboarding, members, connection and exception controls.
- `packages/control-plane`: domain policy and the sole mutation seam.
- `packages/database`: ordered household streams and durable work/effect state.
- `packages/runtime`: provider-neutral bounded model runtime.
- `packages/linq`, `packages/google`, `packages/artifacts`: deep provider boundaries.

Do not add a generic workflow engine, connector registry, policy DSL, agent framework, chat-history
store, or feature-specific batch/revision state machine before the pilot proves a product need.

## Acceptance evidence

Automated evidence must cover:

- signal and effect idempotency;
- exact household serialization and retry;
- two-adult identity/group authority;
- private-versus-group memory projection;
- encrypted cross-service image retrieval;
- one episode through ownership, timer, completion, and replay;
- Gmail candidate privacy and exact promotion;
- Calendar approval and provider reread proof;
- webhook signature and live participant drift;
- migration replay, worker singleton lease, and production image build.

The release is not a pilot until the same journey runs with:

- the production Linq number;
- two real adult identities and browser sessions;
- Railway API and worker on the same commit;
- the configured OpenAI model;
- one Google test account;
- one synthetic family obligation; and
- one completed episode with no duplicate output.

## Explicitly deferred

- public signup and general account recovery;
- more than two adults in an interactive pilot group;
- arbitrary caregiver/school/community chat participation;
- Gmail Pub/Sub/history backfill infrastructure;
- Calendar picker, multiple calendars, or generic connectors;
- HEIC decoding inside the worker;
- purchases, bookings, submissions, or communication outside the family;
- a planner/task/calendar dashboard;
- physical proof that a completed family obligation happened.

New scope must either improve safe episode closure for the pilot family or remove a demonstrated
blocker. Code that merely generalizes the architecture is out of scope.
