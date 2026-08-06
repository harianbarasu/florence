# Prior-session agent-engineering synthesis for Florence

Research date: 2026-08-05  
Prior Codex task mined: `019fcdde-cef2-7833-8f30-b6b9420dbec9`  
Status: architecture input and decision record; not a framework mandate or user-facing PRD  
Scope: agent harnesses, orchestrator/worker design, knowledge systems, memory, observability,
evaluation, security, and controlled improvement

## Executive decision

The strongest reusable conclusion from the prior task is:

> **Florence should be a persistent, source-aware family control plane with disposable model
> cognition. The Chief of Staff is a durable product relationship, not a long-lived model process;
> specialist agents are ephemeral workers, not durable identities or data owners.**

That leads to ten concrete decisions:

1. **Florence owns durable state.** PostgreSQL owns conversations, source references, household
   state, permissions, work, schedules, artifacts, decisions, effects, memory candidates, accepted
   memories, and audit. A model context, LangGraph checkpoint, sandbox disk, or vector index is never
   the product's source of truth.
2. **One accessible Chief of Staff fronts the system.** It may delegate to specialist workers, but
   users should not have to discover or manage an org chart of agents.
3. **Workers are ephemeral and proposal-only.** Each run receives a purpose-bound context projection,
   narrow tools, a budget, a deadline, and a typed completion contract. It returns a proposal and
   receipts, then exits.
4. **Parallelize breadth, not shared mutable state.** Fan out independent research, classification,
   or source analysis. Keep one authoritative workflow in charge of commitments, identity,
   permissions, calendar effects, memory promotion, and household state transitions.
5. **Context is compiled, not accumulated.** Retrieve the smallest useful set of source-backed facts,
   current state, policies, and tools for the task. More chat history, tools, or memories can reduce
   quality as well as increase cost.
6. **A chat is a versioned privacy and purpose boundary.** Every group message and derived artifact
   must retain the exact chat, participant-set epoch, source visibility, and sharing authority that
   allowed it to exist. A worker or answer may narrow scope but may never widen it.
7. **Private context never becomes group context by inference.** Private Gmail, calendar, DM, or
   memory may cause Florence to contact the owner privately or propose a safe share; it may not be
   quoted, summarized, hinted at, or used to personalize a group answer without an explicit sharing
   rule or share decision.
8. **General questions remain allowed.** “Family scope” should govern private retrieval, memory,
   proactive behavior, and action authority—not make Florence refuse harmless non-parent questions.
   Florence can answer from public/general knowledge without silently importing unrelated material
   into household memory.
9. **Learning is governed promotion.** Production interactions generate candidate memories,
   relevance rules, reminder defaults, skills, and harness changes. Evidence, evaluation, scope, and
   authority decide what becomes active. The system must not silently rewrite permissions, sharing,
   evaluators, budgets, or production behavior.
10. **Evaluate lived outcomes, not agent theater.** The important measures are privacy non-leakage,
    commitments caught, reminders delivered at useful times, false alarms, user corrections,
    successful recovery, and real end states—not how elaborate the trace or agent cast appears.

These decisions reinforce the current code rather than call for a rewrite. Florence already keeps
Deep Agents behind an application-owned `WorkerRuntime`, uses a read-only worker filesystem,
authorizes capabilities outside the model, requires schema-constrained output, issues tool receipts,
and verifies completion before accepting a result
([runtime adapter](../../src/runtime/deep-agents-worker-runtime.ts),
[runtime contracts](../../src/runtime/contracts.ts)).

## What the prior task established

The earlier task read first-party corpora from Stripe, Ramp, Sierra, Harvey, Decagon, Anthropic,
OpenAI, Palantir, Glean, Cursor, Factory, Cognition, Applied Compute, and others. The most important
cross-company lesson was not a particular framework. It was a causal sequence:

```text
recognizable job
→ live data, actions, corrections, and outcomes
→ durable objects and state transitions
→ repeatable deployment and release controls
→ context, memory, evaluation, and improvement
→ selective custom infrastructure
```

Stripe built Kai after broad internal agent sprawl and mature internal tools, and built Minions on
top of existing devboxes, source control, rules, and deterministic CI. Sierra built its sandbox
orchestration only after two adopted internal products hit the same execution constraints. Decagon
earned procedures, simulations, traces, versioning, and a proposal-based optimizer from a live
customer-support application. These are first-party accounts and their scale or outcome figures are
vendor-reported, but the repeated ordering is useful
([Stripe Knowledge AI Platform](https://stripe.dev/blog/meet-stripes-knowledge-ai-platform),
[Stripe Minions](https://stripe.dev/blog/minions-stripes-one-shot-end-to-end-coding-agents),
[Minions part 2](https://stripe.dev/blog/minions-stripes-one-shot-end-to-end-coding-agents-part-2),
[Sierra Agency](https://sierra.ai/blog/agency-secure-scalable-sandboxes-for-agents),
[Decagon AOPs](https://decagon.ai/blog/why-we-built-aop)).

For Florence, the recognizable job is broader than one workflow but still concrete:

> Turn the streams already surrounding a family—group chats, private messages, email, calendars,
> activities, documents, and explicit requests—into a calm, timely sense that the family is on top
> of what matters, without leaking one person's private life into another context.

The group-chat agent is both the product surface and a distribution loop. The platform underneath is
earned by making that loop trustworthy. A general “life ontology,” generic agent builder, or vast
multi-agent hierarchy is not the opening product.

### The consumer behavior the architecture must create

The core “how I use Florence” moment should be:

```text
family information arrives anywhere
→ Florence notices the consequential part
→ the right person or group gets a concise, source-backed understanding
→ Florence proposes or records the right commitment, reminder, or next step
→ ownership is acknowledged without blame
→ Florence checks that the loop actually closed
→ the correction/outcome improves the next instance
```

Users should experience this as one dependable participant in the conversation, not as a request to
configure agents. The engineering patterns exist to make that behavior reliable:

| Engineering pattern | Consumer behavior it enables |
|---|---|
| Source-specific ingestion and evidence envelopes | Florence can notice a changed practice email, school thread, or group message without losing origin or audience |
| Policy-aware relevance classification | Most inbound noise disappears while material family information surfaces |
| Chat/participant epochs and scope intersection | Florence can participate in existing groups without turning private connected accounts into group property |
| Durable household state | A commitment survives the chat where it was mentioned and remains visible to later reminders and questions |
| Ephemeral specialist delegation | Florence can do deep research or planning without users managing a roster of agents |
| Deterministic command/effect handlers | A reminder, acknowledgement, or calendar update occurs once and can be reconciled |
| Product-shaped evaluation and controlled learning | Florence asks less over time while preserving privacy, timing, and correction boundaries |

This loop is the architecture's acceptance criterion. A framework feature that does not materially
improve noticing, shared understanding, action, closure, or trusted learning is optional machinery.

## Recommended architecture

```text
Linq / Gmail / Calendar / later connectors
        │
        ▼
source adapters → encrypted source/evidence ledger → source-specific indexes
        │                         │
        │                         ▼
        │              relevance + privacy classification
        │                         │
        ▼                         ▼
durable inbox/events → app-owned household state + memory candidates
                                  │
user message / timer ─────────────┤
                                  ▼
                        policy-aware context compiler
                                  │
                                  ▼
                     Chief-of-Staff coordinator run
                                  │
                    independent bounded delegation
                                  ▼
                       ephemeral specialist workers
                                  │
                    typed proposals + source receipts
                                  ▼
                 deterministic domain and policy handlers
                                  │
                                  ▼
                      durable outbox / effect executor
                                  │
                                  ▼
                         Linq / Google / user
```

The architecture deliberately separates five layers that agent frameworks often blur:

| Layer | Durable owner | What a model may do |
|---|---|---|
| Source/evidence | Florence source ledger and connector metadata | Read a purpose-scoped projection; extract candidates |
| Household state | Florence domain aggregates and event history | Propose commands; never write directly |
| Work/run state | Florence jobs, attempts, leases, budgets, receipts, results | Execute one bounded attempt |
| Context/memory | Accepted facts plus source-linked candidates and projections | Retrieve; propose additions, corrections, or revocation |
| External effects | Deterministic outbox and connector adapters | Draft an exact effect intent; never hold write credentials |

Anthropic's managed-agent architecture similarly separates an append-only durable session from
finite model context, and separates orchestration from execution and credentials. The transferable
point is the seam, not Anthropic's service
([Managed Agents](https://www.anthropic.com/engineering/managed-agents)). LangGraph's own production
documentation also treats runs, checkpoints, queues, workers, and long-term stores as different
resources and requires idempotent side effects on replay
([Agent Server](https://langchain-ai.github.io/langgraph/concepts/langgraph_server/),
[persistence](https://docs.langchain.com/oss/python/langgraph/persistence)).

### Why the Chief of Staff is durable while cognition is ephemeral

The durable Chief of Staff consists of:

- a stable phone identity and household relationship;
- each adult's identity, consent, accounts, and private settings;
- chats and participant-set epochs;
- household goals, commitments, routines, projects, and policies;
- source-linked memory, artifacts, prior decisions, and corrections;
- scheduled work, proactive thresholds, and run history; and
- a versioned role/harness configuration.

Each interaction rehydrates a fresh execution from those records. The runtime may create fresh
specialists, but those specialists disappear when the work ends. Anthropic's long-running-agent
experiments found that continuity came from external feature lists, progress artifacts, clean
checkpoints, and incremental work—not from expecting a new context window to remember the previous
one ([effective harnesses](https://www.anthropic.com/engineering/effective-harnesses-for-long-running-agents)).
Lilian Weng's later synthesis reaches the same design: durable artifacts and inspectable backend
jobs should carry continuity outside the prompt
([Harness Engineering for Self-Improvement](https://lilianweng.github.io/posts/2026-07-04-harness/)).

## Orchestrator and ephemeral workers

### The orchestrator owns coordination, not every inference

The Chief-of-Staff coordinator should own:

- interpreting the current request and deciding whether it is informational, operational, or both;
- selecting the household/person/chat scope;
- reconciling cross-domain priorities and time;
- deciding whether delegation is worth its latency and cost;
- issuing bounded job contracts and strict capability subsets;
- collecting durable artifact references and receipts;
- synthesizing one coherent user response; and
- handing proposals to deterministic application services.

It should not own connector credentials, make direct database mutations, or become the only place a
plan exists.

### Fan out only when work is independently parallelizable

Anthropic reports that its orchestrator-worker research system works best for valuable,
breadth-first questions whose branches can explore independently. It reports much higher token use
than chat and says tightly dependent/shared-context work is a poor fit. It also recommends passing
artifact references rather than repeatedly compressing specialist outputs through a coordinator
([multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system)).

Good Florence fan-out candidates:

- parallel research across several products, schools, camps, trips, or providers;
- independent analysis of separate email threads or document families;
- restaurant or meal-plan research where sources can be divided cleanly;
- project discovery across several independent questions;
- generating several candidate plans that a single coordinator compares; and
- background source classification where each item has a closed schema.

Bad Florence fan-out candidates:

- resolving who owns one ambiguous family commitment;
- changing a shared calendar or sending a message;
- deciding what private source may be shown in a group;
- merging several adults' identity or memory records;
- performing ordered steps against one mutable external system; and
- grading and promoting the workers' own behavior.

Those jobs need one authoritative state machine even if individual read-only analyses are delegated.

### Worker contract

Every specialist attempt should be reconstructable from a durable contract containing at least:

```text
job_id, attempt_id, household_id, requesting_principal
conversation_id, participant_epoch, purpose, requested_outcome
source/context reference ids and their visibility scopes
allowed tools and capability-grant ids
model/provider/behavior version
token, model-call, tool-call, time, and delegation budgets
output schema and completion verifier
deadline, cancellation state, retry class, and parent job
```

The result should contain:

```text
typed proposal, completion status, limitations, warnings
source and tool receipts, artifact references, usage
proposed domain commands, never direct effects
```

The current Florence worker contracts and capability authorizer already implement most of this
shape. Preserve that boundary as more worker types arrive.

### Deep Agents and LangGraph

Deep Agents is an appropriate **replaceable specialist harness**: it is open source, model-neutral,
supports isolated subagents, progressive skills, filesystem/context offloading, typed tools, and
human interrupts. Its own security statement is equally important: the agent can do anything its
tools permit, so real enforcement belongs at the tool and sandbox boundary
([Deep Agents repository](https://github.com/langchain-ai/deepagents)).

Florence should therefore:

- keep Deep Agents entirely behind `WorkerRuntime`;
- keep PostgreSQL jobs and household aggregates authoritative;
- keep capability checks inside Florence's tool adapter on every call;
- continue to require typed result schemas and completion verification;
- keep workers' filesystem and source materializations read-only by default;
- make specialist definitions versioned configuration, not durable “employees”; and
- remain able to replace Deep Agents without migrating household data or work history.

Do **not** add Mastra, LangGraph Agent Server, or another framework as a second product control plane.
If one is evaluated later, it should implement the same Florence-owned runtime contract against the
same scenarios. Framework features may reduce adapter code; they do not inherit authority to own
identity, memory, schedules, permissions, or external effects.

## Knowledge and context system

### Source-first, not one opaque “family memory”

The prior task's Cerebras analysis is especially relevant. Cerebras describes source-specific
connectors, incremental/event-driven refresh, Slack-thread reconstruction, lexical plus semantic
retrieval, reranking, project scoping, narrow retrieval tools, and citation-backed synthesis. It
keeps source-specific derived artifacts rather than pretending one embedding is canonical
([Cerebras knowledge base](https://www.cerebras.ai/blog/how-we-built-our-knowledge-base)). Anthropic
likewise reports that contextual embeddings and BM25 complement each other and recommends evaluating
retrieval against the real corpus
([Contextual Retrieval](https://www.anthropic.com/engineering/contextual-retrieval)).

Florence should maintain distinct records for:

1. **Authoritative source:** the original message, thread, email, calendar event, attachment, or
   external record and its provider identity/version.
2. **Retrieval derivative:** chunks, embeddings, summaries, classifications, thread distillations,
   and relevance scores that can be rebuilt.
3. **Candidate fact or memory:** a source-linked proposition Florence may use cautiously or ask an
   adult to confirm.
4. **Accepted memory:** a scoped, temporally valid statement promoted by an authorized adult or
   previously approved learning rule.
5. **Typed household state:** commitments, reminders, projects, meal plans, and other product objects
   governed by domain rules.
6. **Run artifact:** a research memo, plan, or worker output belonging to one job/project until
   deliberately promoted.

A summary can improve retrieval without becoming truth. A chat message can establish that someone
said something without establishing that the household accepted it. A calendar event can be an
authoritative schedule record while its description remains untrusted prose.

### Evidence envelope

Every indexed item should preserve enough metadata to answer “where did this come from, who could
see it, and when was that true?” At minimum:

```text
household_id, source_account_id, provider, source_type, source_id
conversation/thread/event id, sender/author, source version or digest
occurred_at, modified_at, ingested_at, freshness state
visibility scope, participant-set epoch, source ACL snapshot
personal/group/household purpose scope
retention/deletion lineage and supersession
derivative kind and generating behavior version
exact source pointer or evidence span
```

Source-specific ingestion should reconstruct meaningful units. A new group reply should refresh or
rebuild the relevant thread window, not be embedded as a context-free sentence. Gmail sync should
preserve thread/account identity. Calendar updates should supersede prior provider versions without
erasing what Florence previously observed.

### Context compiler

The context compiler should follow a narrow ladder:

1. compact, always-on safety, identity, and current-channel policy;
2. the user's current request and exact conversation window;
3. accepted household state explicitly visible in this scope;
4. task-relevant source evidence with timestamps and citations;
5. relevant procedural skill(s);
6. only the tools and fields permitted for this attempt.

Anthropic describes context as a finite attention budget and recommends small high-signal
instructions, just-in-time retrieval, structured notes, compaction, and isolated subagents
([context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)).
Stripe's Kai similarly uses layered skills and selective tool loading rather than putting a large
catalog into every prompt; details about Kai's internal implementation are reported in LangChain's
customer case study and should be treated as such
([Kai case study](https://www.langchain.com/blog/how-stripe-built-their-knowledge-ai-platform-on-deep-agents)).

## Group-chat privacy and data access

This is the most important Florence-specific extension of the prior engineering work.

### The chat is an authority object

Represent each chat with:

- a stable provider conversation identifier;
- a current participant set and immutable participant-set epochs;
- the Florence identities, verification state, and household membership of each participant;
- a chat mode (`listen_only`, `respond_when_addressed`, or approved proactive behavior);
- the sources and memory classes permitted in that chat;
- retention/history-start policy for each participant epoch; and
- who approved the current behavior and when.

New membership must create a new epoch. It should not automatically expose history or derived memory
from before the person joined. Participant removal changes future visibility but does not rewrite who
historically received a message. Ambiguous identity or provider membership changes should pause
outbound proactive behavior until Florence re-establishes the exact chat identity.

### Scope monotonicity

For any context item or result:

```text
effective visibility =
  source visibility
  ∩ current task purpose
  ∩ requesting principal's authority
  ∩ chat policy
  ∩ every current participant's applicable sharing policy
```

The result may be narrower; it may never be broader. When policies differ, the most conservative
policy governs the shared response. If a participant is unverified or does not have a Florence
account, the safe default is **group-local context only**: Florence may use what is visible in that
chat plus public information, but may not import any participant's private connected sources.

This implements the user's product intuition without confusing relevance with permission. “All
participants have Florence” may unlock richer shared behavior, but only the intersection of their
approved sharing settings. It never licenses a union of everyone's private data.

### Existing group chats

Recommended default when Florence is dropped into an existing chat:

1. identify the exact chat and participant set;
2. explain briefly what it can read, retain, and share;
3. begin in group-local `respond_when_addressed` mode;
4. invite participants to complete private identity/consent setup in DM;
5. expand proactive or connected-source behavior only after the required participants approve it;
6. on later membership change, fall back to the safe mode for the new epoch.

This avoids forcing every useful group to become a household while preserving a path from a mom
chat or sports group into richer Florence participation. It also makes the viral loop compatible
with privacy rather than opposed to it.

### Private-to-shared promotion

Private source material can cross into a group only through an explicit derived artifact:

```text
private source
→ owner-visible candidate summary/action
→ explicit or pre-authorized sharing rule
→ group-safe artifact with source owner, scope, and expiry
→ group response
```

The group-safe artifact should contain the minimum information required. Other participants receive
neither the underlying private source nor hints that reveal unrelated content. A private signal may
cause Florence to ask its owner privately whether something should be shared; it may not make the
group response mysteriously more informed.

Ramp's agent-identity model—agent as an attributable actor tied to a sponsor, with bounded scopes,
expiry, revocation, and audit—is a useful precedent
([agentic identity](https://builders.ramp.com/post/agent-identity-introduction)). Sierra's MCP Gateway
similarly separates interactive user identity from service identity and pre-authorizes scheduled
workflows by customer/tool scope
([MCP Gateway](https://sierra.ai/blog/building-sierras-mcp-gateway-an-engineering-iceberg)). Florence
needs the consumer analogue for principal, household, chat, worker, and scheduled service actors.

## Security and effect boundaries

### Capability controls beat prompt promises

Untrusted email, group messages, web pages, attachments, and tool results share the same natural
language channel as instructions. The strong boundary is to avoid exposing authority in the first
place:

- ingest/analyze hostile content with no write tools or credentials;
- expose only purpose-specific source projections;
- mint short-lived, non-delegable capability grants;
- keep OAuth refresh tokens and write credentials outside model and sandbox processes;
- re-authorize every tool call against the durable job, attempt, household, scope, and purpose;
- require typed inputs and validate resource identities deterministically;
- turn external effects into outbox intents with idempotency and reconciliation; and
- audit actor, source, tool, decision, and effect without logging unnecessary private content.

Sierra keeps model-provider credentials outside agent sandboxes, uses per-runner identities and
filtered egress, and restores hibernated workers from ordered events and checkpoints
([Agency](https://sierra.ai/blog/agency-secure-scalable-sandboxes-for-agents)). Anthropic likewise
describes ephemeral containers, credential separation, egress controls, scoped identities, and
special caution around mutable remote tools
([containment](https://www.anthropic.com/engineering/how-we-contain-claude)). These are first-party
design accounts, not independent security audits, but the architecture principle is sound.

### Generative proposal, deterministic commit

Use models for:

- relevance and semantic classification;
- extraction and candidate entity resolution;
- open-ended planning and research;
- natural-language synthesis; and
- proposing reminders, memory, procedures, and actions.

Use deterministic application services for:

- identity and membership;
- visibility and consent;
- temporal ordering and idempotency;
- owner acknowledgement and commitment state;
- memory promotion/revocation;
- external writes and retries;
- budgets and cancellation; and
- final persistence and audit.

Decagon's AOP design explicitly combines flexible natural-language instructions with code-level
validation for sensitive steps
([AOPs](https://decagon.ai/blog/why-we-built-aop)). Ramp reports a similar division in risk
operations: agents gather and route context while approved policies/models own autonomous risk
decisions, with shadowing and bounded exposure
([Agentic Risk Operations](https://builders.ramp.com/post/agentic-risk-operations)).

## Durability and recovery

The durable run system should provide:

- idempotent inbox capture before acknowledging connector delivery;
- one authoritative job and attempt identity;
- leases and fencing for exclusive work;
- deadlines, cancellation, bounded retries, and terminal failure classes;
- append-only attempt events and privacy-safe receipts;
- transactional state mutation plus outbox enqueue;
- stable idempotency keys for every external effect;
- remote reconciliation after ambiguous outcomes;
- version pins for model, behavior, tools, source snapshots, and schemas; and
- recovery from the last accepted application checkpoint, not from a worker's recollection.

LangGraph cautions that a resumed node can rerun from its start, so pre-interrupt side effects must
be idempotent. Stripe's Minion “blueprints” likewise alternate bounded agent loops with deterministic
nodes, cap iteration, deny production authority in devboxes, and end in a human-reviewed artifact
([LangGraph interrupts](https://langchain-ai.github.io/langgraph/concepts/breakpoints/),
[Minions part 2](https://stripe.dev/blog/minions-stripes-one-shot-end-to-end-coding-agents-part-2)).

Florence's existing durable inboxes, leases, worker heartbeat, outbox, timers, and worker-attempt
model are the right foundation. Do not replace them with framework-native thread state merely to
obtain a prettier agent trace.

## Observability and evaluation

### Observe causes without storing a second private corpus

Each run should expose a privacy-safe causal record:

- purpose, scope, behavior/model/tool versions, and budgets;
- which source references and accepted memories were disclosed;
- classifier/router decisions and reason categories;
- subjobs, attempts, tool calls, artifacts, receipts, and completion checks;
- proposed commands and deterministic acceptance/rejection reason;
- user correction, dismissal, acknowledgement, or follow-up; and
- cost, latency, retries, and final real-world state.

Store references, structured categories, and content digests by default—not duplicate raw Gmail or
chat bodies in telemetry. Raw trace access should be short-lived, access-controlled incident data.

Weng's synthesis usefully distinguishes component observability, experience observability, and
decision observability: make editable components explicit, aggregate trajectories into evidence-
backed failure patterns, and attach each proposed edit to a falsifiable prediction. The primary
Self-Harness paper uses a weakness-mine → bounded-proposal → held-in/held-out-evaluation → accept
loop ([Weng](https://lilianweng.github.io/posts/2026-07-04-harness/),
[Self-Harness](https://arxiv.org/abs/2606.09498)).

### Product-shaped evaluation

This is not a recommendation for hundreds of low-value unit tests. Start with a small, curated
release set covering the highest-risk lived scenarios and add a case when production reveals a new
failure. Anthropic says its research-agent team began with roughly twenty representative queries
because early changes produced large effects
([multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system)).

Florence's core evaluation classes should be:

| Evaluation | Question | Examples |
|---|---|---|
| Privacy | Did any response reveal data outside the effective scope? | private Gmail in group; new group participant; removed participant; shared-memory intersection |
| Capture | Did Florence identify the real family item and source? | school deadline, sports change, invitation, bill, permission slip |
| Time | Did it interpret time zones, recurrence, lead time, and dependencies correctly? | before-school reminder; changed practice; travel day |
| Action/state | Did the right durable state transition occur once? | owner acknowledgement, reminder, calendar write, retry after timeout |
| Proactivity | Was the interruption useful, timely, non-blaming, and not repetitive? | false positive, duplicate email/chat notice, quiet period |
| General agency | Could Florence answer an unrelated request without polluting family memory or authority? | public research, drafting, ordinary questions |
| Delegation | Did fan-out improve completeness enough to justify latency/cost, with correct citations? | multi-option research versus a tightly coupled task |
| Recovery | Can work resume after worker/provider/redeploy failure without duplicate effects? | crash after send, expired lease, stale checkpoint |

Separate component metrics from user outcomes:

- ingestion freshness and deduplication;
- relevance precision/recall and abstention;
- privacy-policy decisions and zero-leak violations;
- extraction/entity/time correctness;
- context/tool selection;
- final answer support and citation quality;
- reminder usefulness, correction rate, and annoyance/dismissal;
- completed household outcome; and
- model/tool cost and latency.

Decagon's production lifecycle similarly layers previews, unit/integration checks, simulations,
traces, scheduled regression, QA, and staged releases
([test-driven agents](https://decagon.ai/blog/the-future-of-ai-agents-is-test-driven)). The useful
lesson is layered evidence, not copying the size of an enterprise test platform.

## Governed self-learning

Florence needs two different learning loops.

### Learning about the family

```text
source evidence or user correction
→ scoped candidate fact/preference/routine
→ policy and provenance validation
→ ask once when authority is missing
→ accepted scoped memory
→ use without asking again inside that scope
→ expire, supersede, revoke, or correct
```

An approval should be able to establish a durable rule, such as “school newsletters from this sender
are household-relevant” or “practice changes may be shared in this household chat,” so Florence does
not repeatedly ask. The rule must retain owner, scope, examples/evidence, exceptions, valid time, and
revocation. A rule learned for one adult/account/chat cannot silently generalize to another.

### Learning how to work for the family

Editable candidate surfaces may include:

- relevance and category rules;
- source/thread selection and context ordering;
- reminder lead-time defaults;
- skill and tool descriptions;
- delegation thresholds and worker configuration;
- response length/tone defaults; and
- procedural flows for recurring low-risk work.

The promotion loop should be:

```text
production evidence and correction
→ recurrent failure pattern
→ bounded candidate behavior change
→ replay on affected cases plus protected regression cases
→ explicit review when privacy, sharing, or autonomy changes
→ versioned promotion and staged rollout
→ monitor, retain, revise, or roll back
```

The following remain outside the editable loop:

- identity, consent, sharing, and privacy enforcement;
- capability and credential boundaries;
- audit and source lineage;
- test/evaluator definitions and protected holdouts;
- resource ceilings and cost accounting;
- the promotion/rollback mechanism; and
- rules that determine which external effects require approval.

Decagon markets Autopilot as self-improving, but its detailed product description says changes are
tested, shown as versioned diffs, and require human approval before production. That stricter design
is the reusable pattern
([Duet Autopilot](https://decagon.ai/blog/autopilot)). Generated or mined cases can expand coverage;
they cannot silently rewrite the protected standard that decides whether a change is safe.

## Build-versus-buy boundary

### Florence must own

- adult, household, chat, participant-epoch, and service identities;
- consent, visibility, sharing, retention, and deletion lineage;
- source/evidence envelopes and account/chat scoping;
- typed household state and temporal semantics;
- context compilation and scope monotonicity;
- durable jobs, leases, budgets, attempts, receipts, and recovery;
- capability grants and deterministic command/effect handlers;
- memory candidate, promotion, correction, expiry, and revocation;
- user-visible provenance and controls; and
- product-shaped evaluation, promotion, and rollback decisions.

### Florence can rent behind replaceable seams

- frontier, open-weight, or local models;
- the Deep Agents specialist loop;
- generic embedding and reranking models;
- sandbox compute;
- ordinary queue/database/cloud primitives;
- generic OAuth and connector libraries where they preserve Florence's boundary; and
- generic tracing transport, provided sensitive content remains under Florence policy.

Build custom model serving, a sandbox fleet, a universal retrieval engine, or a second orchestration
platform only after a measured Florence workload proves the supplier violates a required privacy,
reliability, cost, latency, or control invariant. Sierra explicitly describes making its custom
sandbox investment only after adopted products shared the same unmet constraints; Stripe's and
Harvey's engineering histories show the same pattern
([Sierra Agency](https://sierra.ai/blog/agency-secure-scalable-sandboxes-for-agents),
[Harvey cloud-agent infrastructure](https://www.harvey.ai/blog/why-we-built-our-own-cloud-agent-infrastructure)).

## Decision register

| ID | Decision | Consequence |
|---|---|---|
| PSE-001 | Persistent system, ephemeral cognition | No continuity may depend on a model session or specialist identity |
| PSE-002 | One user-facing Chief of Staff | Specialists remain an implementation detail unless a product surface genuinely needs one |
| PSE-003 | App-owned Postgres control plane | Framework checkpoints and memory are caches/adapters, not canonical state |
| PSE-004 | Deep Agents stays behind `WorkerRuntime` | Provider/framework replacement must not migrate household or work data |
| PSE-005 | Parallelize independent breadth only | Shared-state and effectful work stays under one authoritative workflow |
| PSE-006 | Exact source, principal, chat, participant epoch, and purpose on context | Every disclosure and derived artifact is reconstructable |
| PSE-007 | Scope monotonicity and conservative group intersection | No worker, memory, or answer can widen source visibility |
| PSE-008 | Private-to-shared requires an authorized derived artifact | Private connected sources never leak indirectly into group output |
| PSE-009 | General answers are allowed without private-scope expansion | “Family-first” is a context/action policy, not a refusal taxonomy |
| PSE-010 | Model output is proposal; deterministic services commit | Identity, permission, memory promotion, schedules, and effects are never free-form writes |
| PSE-011 | Source, derivative, candidate memory, accepted memory, state, and artifact remain distinct | Search quality cannot silently redefine household truth |
| PSE-012 | Learning uses versioned proposal/evaluation/promotion/rollback | Approval can reduce repeated questions without permitting silent policy drift |
| PSE-013 | Small, risk-shaped eval set plus production-derived cases | Quality work stays proportional and tied to lived outcomes |
| PSE-014 | Privacy-safe causal observability | Debuggability does not create a second uncontrolled corpus of family data |

## Source interpretation and limitations

- Company engineering blogs are primary sources for what those companies say they built, not
  independent proof of their scale, security, quality, or customer outcomes.
- The Stripe Kai implementation details attributed to Deep Agents come partly from LangChain's
  vendor/customer case study. Stripe's own Knowledge AI Platform post is the stronger source for
  Stripe's product claims; neither is an independent audit.
- Weng's harness essay is a high-quality research synthesis, not itself primary experimental
  evidence. Material self-improvement claims should be traced to papers such as
  [Self-Harness](https://arxiv.org/abs/2606.09498), and those results remain early and task-bound.
- Cerebras's post is a useful first-party retrieval architecture account. It does not, by itself,
  establish answer quality, ACL revocation correctness, or a promotion boundary from retrieved
  discussion to durable truth.
- Multi-agent gains reported by Anthropic are workload-specific and expensive. They support a
  conditional fan-out mechanism, not “more agents” as a default quality strategy.
- No cited framework or company resolves Florence's distinctive consumer problem: privacy and
  authority across changing real-world group chats, personal connected sources, shared household
  state, and proactive behavior. Florence must own that layer.
