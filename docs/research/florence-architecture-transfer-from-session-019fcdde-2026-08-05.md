# What Florence should carry forward from the institutional-intelligence research

- Research date: 2026-08-05
- Prior Codex task audited: `019fcdde-cef2-7833-8f30-b6b9420dbec9`
- Audience: product and company builders who do not need to be infrastructure specialists
- Status: research synthesis and architecture decision input; not an implementation plan

## Bottom line

Florence should not depend on an AI model “remembering the family.” It should keep a trustworthy,
editable family record outside the model and assemble only the small part needed for the task at
hand.

That leads to a simple product idea:

> **One dependable Chief of Staff on the outside; a source-aware family control plane on the
> inside; replaceable models and short-lived specialist workers underneath.**

Memory is therefore not one transcript, one summary, or one vector database. Florence needs to keep
five things distinct:

1. **Source evidence:** what a message, email, event, file, or person actually said.
2. **Retrieval material:** searchable chunks and summaries that help Florence find the source again.
3. **Accepted memory:** a scoped preference or fact that a person has stated or approved.
4. **Household state:** commitments, plans, reminders, participants, permissions, and action status.
5. **Skills:** versioned ways of doing recurring work.

The model may read a permitted slice of these records and propose a result. It should not silently
turn a search result into truth, change who may see private information, or directly commit an
external action.

For a nontechnical user, this should feel much simpler than the architecture sounds:

| What the person experiences | What Florence does underneath |
|---|---|
| “Florence remembers what matters.” | Keeps source-linked, scoped, editable memories outside the model |
| “Florence knows where that came from.” | Preserves the original source and shows it when useful |
| “Florence understands our family without oversharing.” | Applies person, chat, participant, purpose, and time boundaries before retrieval |
| “Florence can handle a familiar routine.” | Selects one small, reviewed skill instead of improvising the whole process |
| “Florence did what we asked, once.” | Uses a durable workflow and deterministic action handler with reconciliation |
| “Florence gets better without becoming unpredictable.” | Proposes memory or skill changes, tests them, and promotes them through explicit rules |

## How claims are separated in this note

Each research section distinguishes three levels:

- **Source fact:** what a linked primary source explicitly says its product or experiment does.
- **Prior-thread synthesis:** a conclusion assembled in the earlier Rivermill task from several
  sources. It is useful reasoning, but no single source proved it.
- **Florence decision:** the design choice recommended here for a consumer family product. This is an
  inference, not a claim made by Stripe, OpenAI, Palantir, Cerebras, or another company.

Company engineering blogs are primary evidence for what those companies say they built. They are
not independent audits of scale, safety, accuracy, or commercial success. Social posts, company
homepages, investor material, people pages, and market maps are weaker evidence and are not used to
justify architecture decisions by themselves.

## What was actually audited

This note reconciles two related bodies of work:

1. The exact prior task currently contains **45 turns and 130 unique exact HTTP targets**: 42 completed
   turns, two interrupted turns, and one failed turn. Those URL strings include duplicate links,
   private Slack and Google links, malformed/truncated links, people pages, market research, and
   architecture sources. They are all accounted for in Appendix A and Appendix B.
2. The task also created or consulted reconciled publisher-corpus syntheses. The original inventory
   records **15 publisher corpora and 3,314 substantive first-party bodies, plus two partner bodies**.
   That means prior agents catalogued and synthesized the corpus; it does **not** mean this Florence
   pass freshly reread 3,314 pages. The branch ledger and its limits are in Appendix C.
3. Since the original 44-turn snapshot, the previously in-progress cross-task turn completed its
   107-turn audit of the earlier Life-OS research task, and a separate 45th turn added three further
   bodies of work: a 40-post Prime Intellect corpus audit; an 18-post Nous corpus plus the live
   Hermes docs and pinned repositories; and Ribbit's 42-page 2026 *Power Letter*. Section 13 and
   Appendix C reconcile both changes instead of silently treating the earlier snapshot as current.

The recent-memory comparison uses material published or materially updated in the three-month window
from 2026-05-05 through 2026-08-05, with older sources included only when they explain the current
product boundary.

## Architecture in one picture

```text
messages, email, calendars, files, and explicit user statements
                              |
                              v
                source and permission ledger
                      /               \
                     /                 \
       rebuildable retrieval       accepted family state
       chunks and summaries        facts, preferences, plans,
                                   commitments, permissions
                     \                 /
                      \               /
                              v
                purpose-aware context compiler
                              |
                              v
                  one visible Chief of Staff
                              |
                 bounded, independent delegation
                              v
                  short-lived specialist workers
                     + a small skills library
                              |
                    proposals and source receipts
                              v
             deterministic validation and action service
                              |
                              v
                durable outcome and correction record
                              |
                              v
              proposed memory/skill improvements
```

The key distinction is between **durable relationship state** and **disposable cognition**. The Chief
of Staff is durable because its phone identity, permissions, accepted memories, commitments, and
history persist. It does not need one endlessly running model process.

## Decision matrix

| Area | Evidence status | Florence decision | Timing | Confidence |
|---|---|---|---|---|
| Source ownership and provider portability | Strong direct evidence for externalized state and replaceable runtimes; “sovereignty” benefits are partly strategic claims | Own identities, permissions, source lineage, state, evaluations, and exports; keep models behind adapters | Now | High |
| One giant context graph | Useful strategic idea, but not proven necessary; retrieval-only and full-context baselines are counterexamples | Use a small typed household model plus source-linked retrieval; do not begin with a universal life ontology or graph database | Now | High |
| Evidence versus accepted truth | Strongly motivated across sources, but the full boundary is cross-source synthesis | Keep source, derivative, candidate, accepted memory, household state, and run artifact distinct | Now | High |
| Memory | Direct evidence that synthesized, editable memory improves continuity; direct counterevidence that stale memory can hurt | Build source-linked, scoped, temporal memory with correction, expiry, and deletion; never treat a summary as complete truth | Now | High |
| Context compilation | Repeated direct support across Anthropic, Stripe, Cerebras, Cursor, Glean, and Weng | Compile the smallest permitted context for each task; keep full history addressable outside the prompt | Now | High |
| Persistent orchestrator and ephemeral workers | Durable state and isolated workers are directly supported; the exact one-speaker pattern is synthesis | Present one Chief of Staff; use short-lived workers for bounded independent work; keep canonical commits centralized | Now | High |
| Skills library | Direct support for progressive, versioned skills; strong evidence that more skills can reduce quality | Start with a small internal registry of reviewed “routines”; no marketplace or huge catalog | Now, small | High |
| Task, harness, runtime, and trace separation | Direct support from Prime `verifiers`, Prime Agent, and Hermes task/delegation machinery | Give each authoritative Florence run an explicit outcome contract, harness/skill identity, runtime identity, and immutable authority trace | Now at the contract seam | High |
| Recoverable worker state | Prime and Hermes directly show durable task boards, logs, and recoverable scratch outside canonical truth | Keep workers non-authoritative and bounded; permit revocable retained scratch only when a measured long task benefits | Later per task | High |
| Sandboxing and authorization | Strong first-party support; not independent security certification | Keep credentials outside model processes, issue narrow capabilities, and make group-chat scope a hard boundary | Now | High |
| Durable actions and outcomes | Strong direct support for external checkpoints, state machines, retries, and reviewable artifacts | Persist action intent, status, receipts, and reconciliation outside the model | Now | High |
| Self-improvement | Direct support for proposal/eval/review loops; autonomous self-modification remains risky and early | Mine failures and propose changes; protect permissions, evaluators, promotion, and rollback from the editable loop | After core evals | High |
| Group-chat wedge and unit of value | Ribbit supplies a strategic test, not consumer-market proof; Florence's loop metric is a product decision | Standardize on loops safely closed, beginning with acknowledged coverage; treat messages, sources, and agent runs as costs | Now | Medium-high |
| Custom model serving, sandbox fleet, agent platform | Supplier breadth and company histories argue for demand-gated investment | Rent behind replaceable seams until a measured Florence requirement is unmet | Defer | High |
| Open-weight models | Useful for weight-level control, but open weights are not the same as owning the product's intelligence | Preserve model portability; add private/open-weight serving only for a demonstrated privacy, cost, latency, or control need | Defer | Medium-high |

## 1. Sovereignty means control and portability, not owning a model today

**Source fact.** Palantir describes its Ontology as an operational layer that joins data, logic,
actions, and security, while its broader sovereign-AI material argues for institutional control over
context, evaluations, workflows, deployment, and sometimes model weights
([Ontology System](https://www.palantir.com/docs/foundry/architecture-center/ontology-system),
[Institutional Sovereignty paper](https://www.palantir.com/assets/xrfr7uokpv1b/7BF74dqccPeVFMHRmy7FO3/2a33ff9b4f9e11ba904445e637095960/Palantir_-_Institutional_Sovereignty_in_the_Age_of_AI.pdf)).
Open-weight systems can offer weight-level deployment and modification rights, but the Open Source
Initiative explicitly distinguishes open weights from a fully open-source AI system
([Open Source Initiative](https://opensource.org/ai/open-weights)).

One claim in the earlier strategic discussion was too broad: enterprise use of frontier providers
does not automatically mean a company's private data becomes training data. OpenAI says business
and API data is not used for training by default, and Anthropic says the same for commercial-product
inputs and outputs except for defined opt-in, feedback, or safety circumstances
([OpenAI business data](https://openai.com/business-data/),
[Anthropic commercial-data policy](https://privacy.claude.com/en/articles/7996885-how-do-you-use-personal-data-in-model-training)).

**Prior-thread synthesis.** The durable advantage is the institution's owned evidence, semantics,
permissions, evaluations, procedures, decisions, and outcomes. A model should be replaceable without
discarding those assets. Owning open weights is one possible final layer of sovereignty, not a
prerequisite for owning the other layers.

**Florence decision.** Florence should own:

- the identities of people, households, chats, and service actors;
- the visibility and consent rules around every source and memory;
- source lineage, accepted household state, commitments, and outcomes;
- the context-selection and memory-promotion rules;
- the user-visible export, correction, and deletion experience; and
- the product's evaluations and release decisions.

It should use provider-neutral interfaces for models, embeddings, rerankers, and worker runtimes.
It should not build its own foundation model, model-serving fleet, or consumer VPC product now. If a
provider later fails a measured requirement, Florence can replace that component without migrating
the family's identity or memory.

## 2. Use a small operational model, not a universal “life ontology”

**Source fact.** Palantir's Ontology models objects, properties, links, actions, functions, and
dynamic security; it is an operational representation rather than merely a semantic-search index
([Ontology overview](https://www.palantir.com/docs/foundry/ontology/overview)). Glean and Foundation
Capital use “context graph” for connector-backed information plus relationships and permissions,
but Foundation's piece is a strategic market thesis, not an architecture evaluation
([Glean context graph](https://www.glean.com/blog/how-do-you-build-a-context-graph),
[Foundation Capital](https://foundationcapital.com/ideas/context-graphs-ais-trillion-dollar-opportunity)).

**Prior-thread synthesis.** An ontology can be the canonical representation of accepted operational
objects, but it should not be the canonical representation of every source sentence, retrieval
chunk, working artifact, or model inference. A retrieval/context graph and an authority-bearing
operational model solve different problems. A graph-shaped domain also does not require a graph
database.

**Florence decision.** Start with a modest relational household model: people, households, chats,
participant epochs, source accounts, events, commitments, routines, projects, permissions, memories,
jobs, actions, and outcomes. Every material fact points back to evidence and carries scope and time.

Do not predesign a complete taxonomy of human life. Add an object or relationship when a real family
workflow needs stable meaning or an authorization rule needs it. Searchable evidence remains outside
that model and can be reinterpreted as the product learns.

## 3. Retrieval is not truth

**Source fact.** Cerebras describes source-specific connectors, Slack-thread reconstruction,
normalized derivatives, lexical and semantic retrieval, rarity and recency signals, rank fusion,
reranking, deduplication, context expansion, and cited synthesis
([Cerebras Knowledge](https://www.cerebras.ai/blog/how-we-built-our-knowledge-base)). Anthropic's
Contextual Retrieval combines document-aware chunks, BM25, embeddings, and reranking; it also notes
that putting a small corpus directly into context is a useful baseline when it fits
([Contextual Retrieval](https://www.anthropic.com/engineering/contextual-retrieval)).

Neither source establishes that a retrieved statement is current, authorized, mutually agreed, or
safe to promote into durable truth. A citation proves where text came from; it does not prove that
the source was correct or that the family accepted it.

**Prior-thread synthesis.** A useful knowledge system needs two synchronized representations:

- a **retrieval representation** optimized for finding and explaining source material; and
- an **accepted-state representation** optimized for permissioned, temporal, operational truth.

This separation is a cross-source design conclusion, not something Cerebras, Glean, or Palantir
proves by itself.

**Florence decision.** Maintain these record classes:

| Record class | Example | May a model create it directly? | Authority |
|---|---|---|---|
| Source evidence | The exact school email or group message | No; a connector captures it | Authoritative only for what the source contained |
| Retrieval derivative | Chunk, embedding, summary, classification | Yes, as rebuildable output | Navigation aid, never canonical truth |
| Candidate memory | “Practice may now be Tuesdays” | Yes, with source, scope, confidence, and expiry | Cautious use or confirmation only |
| Accepted memory | “I prefer morning appointments,” approved by that person | Propose only; promotion follows policy | Valid for named person, purpose, and time |
| Household state | Confirmed event, commitment, reminder, or sharing rule | Propose only; deterministic domain logic commits | Operational source of truth |
| Run artifact | A comparison memo or trip plan | Yes, inside one job/project | Working product until deliberately promoted |

Promotion should be risk-shaped rather than burdensome. An explicit low-risk statement from its
owner may become an accepted personal preference immediately. An inference from several chats should
remain a candidate. A consequential shared commitment, private-to-group disclosure, payment, or
calendar mutation needs the relevant person's authority.

## 4. Retrieval should be source-specific, permission-aware, and easy to rebuild

**Source fact.** Cerebras refetches a Slack thread when it changes rather than indexing each reply as
an isolated sentence, and exposes narrow search tools over one normalized query surface
([Cerebras Knowledge](https://www.cerebras.ai/blog/how-we-built-our-knowledge-base)). Anthropic treats
context as a finite attention budget and recommends just-in-time retrieval, progressive disclosure,
structured notes, and isolated exploration
([Effective context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)).

**Prior-thread synthesis.** Context should be compiled from durable records, not accumulated as an
ever-growing chat. Full source history and artifacts should remain addressable after compaction.
Search should use exact identifiers and words as well as semantic similarity.

**Florence decision.** Ingest each source according to its meaning:

- preserve thread, sender, account, event, attachment, version, and participant identity;
- refresh a meaningful thread or event unit when it changes;
- combine exact and semantic retrieval behind a replaceable interface;
- filter by permission and purpose before ranking, never after answer generation;
- score validity and supersession explicitly rather than relying on generic recency alone;
- attach source references to the answer and let a person inspect them; and
- rebuild summaries, chunks, and embeddings when their generator or source changes.

A universal enterprise-search engine is unnecessary for the first product. PostgreSQL-backed exact
and semantic retrieval is sufficient until real Florence corpora prove otherwise.

## 5. What the last three months of ChatGPT and Codex memory research actually solved

OpenAI is one useful comparison, not the center of this synthesis.

### What is directly established

**Source fact.** OpenAI's June 4, 2026 “Dreaming” article describes a background memory-synthesis
system that carries forward preferences and constraints, updates time-sensitive memories, lets a
person review or edit the synthesis, and evaluates continuity, preference use, and freshness. Its
compute comparison is an OpenAI-reported result, not an independent measurement
([Dreaming: Better memory for a more helpful ChatGPT](https://openai.com/index/chatgpt-memory-dreaming/)).

The current ChatGPT Memory FAQ says the summary is continually updated, high-level, and incomplete;
sources can help a person understand and correct personalization, but may not expose every factor.
It also says fully removing a remembered item can require deleting it from the relevant chats,
files, apps, and summary, while Temporary Chat neither uses nor creates memory
([ChatGPT Memory FAQ](https://help.openai.com/en/articles/8590148-memory-faq)).

OpenAI's May 14 work on sensitive conversations uses a different memory class: short, factual,
purpose-specific, time-limited summaries that are excluded from general memory. That is strong
evidence for separating memory by purpose and retention rather than putting every useful observation
in one global store
([Recognizing context in sensitive conversations](https://openai.com/index/chatgpt-recognize-context-in-sensitive-conversations/)).

Current Codex documentation says local Codex memories are separate from ChatGPT web memory. Eligible
past tasks can be summarized in the background into local, editable files with durable entries and
supporting evidence; controls for using memory and contributing to memory are separate. The docs also
say required team rules belong in `AGENTS.md` or checked-in documentation, not only in probabilistic
memory
([Codex memories](https://learn.chatgpt.com/docs/customization/memories)).

OpenAI's June 22 long-running-work paper recommends an open, editable, diffable memory vault for
people, decisions, open loops, daily notes, and project state. A Git-backed vault makes changes
reviewable rather than allowing vague impressions to accumulate invisibly
([Codex-maxxing for long-running work](https://openai.com/index/codex-maxxing-long-running-work/),
[paper](https://cdn.openai.com/pdf/8a9f00cf-d379-4e20-b06f-dd7ba5196a11/OAI_WhitePaper_Codex-maxxing26.pdf)).

Chronicle, an opt-in Codex research preview, uses recent screen context to help locate a source and
then has Codex read that source directly. Its screen capture is temporary while generated local
memory remains editable; the documentation also calls out prompt-injection and sensitive-screen
risks. This is a useful “memory points back to evidence” pattern, not proof that passive observation
is appropriate for Florence
([Chronicle](https://learn.chatgpt.com/docs/customization/chronicle)).

### What remains unsolved

These systems improve continuity, temporal updating, user correction, and source finding. They do
not by themselves solve:

- which adult has authority to establish a shared family fact;
- what a newly added group-chat participant may see;
- whether a remembered statement was evidence, inference, preference, promise, or accepted truth;
- complete provenance and deletion across every derivative;
- safe promotion from a private account into group-visible context;
- durable external actions and their real-world outcomes; or
- protection from a stale memory that still sounds plausible.

Ramp's controlled skills-and-memory benchmark is important counterevidence: many tested skills were
neutral or harmful, and a single stale fact produced a severe regression in one synthetic accounting
world. Its numbers are company-reported and domain-specific, but the direction is clear: more memory
is not automatically better
([Ramp Stack Benchmarking](https://builders.ramp.com/post/stack-benchmarking)).

### Florence's memory design

**Prior-thread synthesis.** The useful pattern is not “the model remembers.” It is “the product owns
an inspectable record, the model gets a purpose-built projection, and a person can correct the
record.” Summaries are indexes. Sources remain reachable. Durable rules are stored separately from
episodic notes.

**Florence decision.** Use this lifecycle:

```text
source or explicit statement
        -> scoped candidate
        -> authority, provenance, and time check
        -> accepted memory or typed household state
        -> purpose-aware use
        -> correction, expiry, supersession, or deletion
```

Every accepted memory should have an owner, subject, visibility, purpose, source or explicit
statement, valid time, review state, and deletion lineage. Florence should offer plain-language
views such as **What Florence knows about me**, **What our household shares**, and **Why Florence used
this**, with edit, forget, temporary, and source controls.

Memory synthesis may propose consolidation—for example, replacing several expired school-pickup
notes with the current routine—but deterministic policy decides what is replaced, and the older
evidence remains governed by its retention rule.

## 6. The context compiler is more important than a bigger prompt

**Source fact.** Stripe's Kai uses layered skills and selective tool access rather than loading a
large catalog into every task; the detailed Deep Agents implementation is reported partly through a
LangChain partner case study and should be treated as such
([Stripe KAI](https://stripe.dev/blog/meet-stripes-knowledge-ai-platform),
[LangChain case study](https://www.langchain.com/blog/how-stripe-built-their-knowledge-ai-platform-on-deep-agents)).
Cursor turns large outputs into searchable artifacts that remain discoverable after summaries, and
Weng's synthesis recommends durable, inspectable artifacts outside the active prompt
([Cursor dynamic context discovery](https://cursor.com/blog/dynamic-context-discovery),
[Weng harness synthesis](https://lilianweng.github.io/posts/2026-07-04-harness/)).

**Prior-thread synthesis.** A summary should help navigate old work, not replace the canonical
source. The active context should be a projection from durable state and should carry references to
larger artifacts instead of repeatedly compressing them.

**Florence decision.** Build each task context in this order:

1. compact identity, safety, current-chat, and privacy rules;
2. the exact current request and relevant conversation window;
3. accepted state visible to this person and this chat;
4. the smallest source-backed evidence set needed for the task;
5. one or a few relevant skills; and
6. only the tools and fields permitted for this attempt.

Record which sources, memories, skills, and tools were selected so a bad answer can be diagnosed
without storing a second uncontrolled copy of private content in telemetry.

## 7. One visible Chief of Staff; bounded workers behind it

**Source fact.** Anthropic separates a durable append-only session and stateless orchestration
“brain” from sandbox/tool “hands,” with credentials outside the model and sandbox
([Managed Agents](https://www.anthropic.com/engineering/managed-agents)). Stripe's Minions use
isolated environments and code-defined blueprints that alternate bounded model loops with
deterministic validation
([Minions](https://stripe.dev/blog/minions-stripes-one-shot-end-to-end-coding-agents),
[Minions part 2](https://stripe.dev/blog/minions-stripes-one-shot-end-to-end-coding-agents-part-2)).
Factory describes one orchestrator, narrow workers, fresh validators, and shared state outside
individual contexts
([How Missions Work](https://factory.ai/news/missions-architecture)).

There is also clear counterevidence to indiscriminate multi-agent systems. Anthropic says its
research system is valuable for independent breadth but costly and poorly suited to tightly coupled
work. Cognition warns that parallel writers fragment implicit decisions and recommends narrower
patterns such as fresh-context review, read-only retrieval, or a strong lead with scoped workers
([Anthropic multi-agent research](https://www.anthropic.com/engineering/multi-agent-research-system),
[Cognition: Don't Build Multi-Agents](https://cognition.com/blog/dont-build-multi-agents),
[what is working](https://cognition.com/blog/multi-agents-working)).

**Prior-thread synthesis.** Persist the system, not the specialist persona. Parallelize independent
breadth, not shared mutable state. One authoritative workflow should own each consequential state
transition.

**Florence decision.** Users interact with one Chief of Staff. It may delegate parallel product
research, separate document analysis, or independent plan alternatives. Workers receive a purpose,
source references, narrow tools, budgets, deadlines, and an output schema. They return proposals,
artifacts, and receipts, then disappear.

Workers should not:

- hold refresh tokens or broad credentials;
- add people, change sharing, promote memory, or write household state;
- send messages, mutate calendars, or spend money directly;
- delegate beyond their budget; or
- become durable characters a family must understand or manage.

## 8. Yes, Florence should have a skills library—but a small, governed one

This is the most directly transferable answer to the user's question about a skills library.

**Source fact.** Stripe says KAI connects to more than 1,000 skills and tools, gives domain owners a
place to govern agents and tool selections, uses per-session sandboxes and task-context boundaries,
and plans an improvement loop that proposes and tests skill edits for owner review
([Stripe KAI](https://stripe.dev/blog/meet-stripes-knowledge-ai-platform)). Anthropic defines skills
as folders of instructions, scripts, and resources that load progressively, and recommends starting
from concrete evaluations and using deterministic code for steps that should not be improvised
([Anthropic Agent Skills](https://www.anthropic.com/engineering/equipping-agents-for-the-real-world-with-agent-skills)).

Decagon's Agent Operating Procedures combine natural-language procedure with code validation, and
its Autopilot proposes versioned changes with source conversations and test results before human
approval
([AOPs](https://decagon.ai/blog/why-we-built-aop),
[Duet Autopilot](https://decagon.ai/blog/autopilot)). Ramp's benchmark, however, shows that a large or
stale skill bundle can make performance worse
([Stack Benchmarking](https://builders.ramp.com/post/stack-benchmarking)).

**Prior-thread synthesis.** Skills are **procedural memory**: reusable knowledge about how to perform
work. They are not evidence, personal facts, permissions, or canonical household state. Their value
comes from a governed lifecycle, not a proprietary file format or a large count.

**Florence decision.** Create a small internal skill registry now, but present it to nontechnical
users as **routines** or **ways Florence can help**. Use three layers:

| Layer | Example | Who controls it |
|---|---|---|
| Product skill | Turn a school update into a cited summary and proposed commitments | Florence release process |
| Household routine | On Sunday, prepare the coming week's family brief | Authorized household adult |
| Learned candidate | A proposed better way to recognize this family's sports changes | Inactive until it passes evidence, eval, and promotion rules |

A useful starter set is deliberately small:

- understand an incoming family update and identify what changed;
- turn an explicit request into a proposed commitment, owner, and due time;
- compare options with sources and decision criteria;
- prepare a household, school, sports, trip, or project brief;
- propose a calendar or reminder change and verify the result;
- produce a group-safe summary from information approved for sharing; and
- check whether an open loop actually closed.

Each skill should carry a stable ID and version, owner, purpose, required inputs, allowed tools,
output shape, risk class, approval rule, evidence examples, evaluation cases, status, and rollback
target. Only relevant skills should enter context.

A skill may explain a procedure and produce a proposal. It may not grant itself tools, widen a data
scope, declare an inference to be truth, or bypass deterministic approval and action services. Do
not build a public marketplace or hundreds of micro-skills before repeated family behavior proves
the need.

## 9. Authorization is a data boundary, not a sentence in the prompt

**Source fact.** Stripe says a KAI task's access boundary can be narrower than the user's identity
token and that unrelated customer contexts must not be combined
([Stripe KAI](https://stripe.dev/blog/meet-stripes-knowledge-ai-platform)). Sierra describes
per-runner identities, credentials outside agent sandboxes, filtered egress, ordered event recovery,
and isolated execution
([Sierra Agency](https://sierra.ai/blog/agency-secure-scalable-sandboxes-for-agents)). Ramp models an
agent as an attributable actor tied to a sponsor, with scopes, expiry, revocation, and audit
([Ramp agent identity](https://builders.ramp.com/post/agent-identity-introduction)).

These are first-party architecture accounts, not security certifications.

**Prior-thread synthesis.** Natural-language instructions cannot reliably neutralize hostile email,
web content, files, or group messages when the same process holds broad credentials. Effective scope
must only narrow as data moves through retrieval, workers, and answers.

**Florence decision.** Treat every chat and participant set as a versioned authority object. A new
participant creates a new epoch; it does not inherit prior private context. For shared output, use
the conservative intersection of source visibility, current purpose, requesting person's authority,
chat policy, and the applicable sharing rules of every participant.

Private material can reach a group only through a minimal, explicitly approved group-safe artifact.
Unverified participants receive group-local context plus public information, not a union of all
members' connected accounts.

Keep credentials outside model and sandbox processes. Mint short-lived, purpose-specific,
non-delegable capabilities. Reauthorize each tool call against the durable task. Give untrusted
content-analysis jobs no write tools. This is more important for Florence than enterprise VPC
deployment because family privacy boundaries change conversation by conversation.

## 10. Durable execution means remembering the action, not keeping the agent alive

**Source fact.** Cursor separates its agent loop and machine from append-only conversation state and
uses workflow infrastructure for retries, rewind, and diagnosis
([Cursor cloud-agent lessons](https://cursor.com/blog/cloud-agent-lessons)). Harvey separates durable
runs from ephemeral sandboxes and uses provider adapters, short-lived credentials, reviewable
artifacts, and resumability
([Harvey Spectre](https://www.harvey.ai/blog/building-spectre-internal-collaborative-cloud-agent-platform)).
Stripe's Minion blueprints and LangGraph's persistence guidance likewise externalize checkpoints and
warn that replayed work requires idempotent effects
([Minions part 2](https://stripe.dev/blog/minions-stripes-one-shot-end-to-end-coding-agents-part-2),
[LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence)).

**Prior-thread synthesis.** “Persistent agent” marketing often means a durable trigger, task record,
artifact set, or schedule around a sequence of finite executions. It does not require one model to
remain alive or remember everything.

**Florence decision.** Persist the requested outcome, source snapshot, state-machine position,
attempts, deadline, budget, approvals, action intent, receipts, remote identifiers, and final
reconciled result. External writes go through an outbox with stable idempotency keys. After an
ambiguous timeout, Florence checks the remote system before trying again.

The durable outcome—not the beauty of the agent trace—is what lets Florence later answer, “Did this
actually get done?” and learn which procedure worked.

## 11. Learning should propose changes; governance decides what becomes active

**Source fact.** Weng's synthesis describes weakness mining, bounded edits, held-in and held-out
evaluation, causal observability, and keeping evaluators and permissions outside the editable loop
([Harness Engineering for Self-Improvement](https://lilianweng.github.io/posts/2026-07-04-harness/)).
OpenAI's recent Tax AI account links source, citation, practitioner correction, and filed outcome,
then turns a failure cluster into an evaluated bounded Codex change
([Building self-improving tax agents with Codex](https://openai.com/index/building-self-improving-tax-agents-with-codex/)).
OpenAI's Deployment Simulation uses privacy-filtered replays, repository snapshots, tool exchanges,
read-only connectors, and simulators; its scale and performance figures are self-reported and its
own account notes rarity and fidelity limits
([Deployment Simulation](https://openai.com/index/deployment-simulation/)).

**Prior-thread synthesis.** Production interactions can generate candidate memories, skills,
retrieval rules, and harness changes. A candidate should include evidence, a narrow predicted benefit,
evaluation results, risk, and rollback. Success on one trace is not permission to rewrite production.

**Florence decision.** Protect these surfaces from automatic editing:

- identity, consent, privacy, and sharing enforcement;
- capabilities, credentials, and approval thresholds;
- evidence lineage and audit;
- protected evaluation cases and evaluator definitions;
- cost and resource ceilings; and
- the promotion and rollback mechanism itself.

Start with a small evaluation set shaped around real family risks: private-data leakage, correct
capture of commitments, time and recurrence, duplicate effects, useful proactivity, recovery, source
support, and user correction. Add a regression case when production reveals a distinct failure.
Evaluate real outcomes—useful reminders, loops closed, corrections, dismissals, privacy incidents,
and recovered actions—not the number of agents or length of a trace.

## 12. Build versus buy

**Source fact.** Sierra and Harvey describe building custom sandbox/runtime infrastructure only after
adopted products encountered repeated multi-model, security, retention, cost, or execution needs
([Sierra Agency](https://sierra.ai/blog/agency-secure-scalable-sandboxes-for-agents),
[Harvey cloud-agent infrastructure](https://www.harvey.ai/blog/why-we-built-our-own-cloud-agent-infrastructure)).
Ramp's background agent composes existing infrastructure and adds custom integration around its
actual repository and workflow
([Ramp Inspect](https://builders.ramp.com/post/why-we-built-our-background-agent)).

**Prior-thread synthesis.** The repeated order is application first, repeated behavior and evidence
second, reusable platform third, custom commodity infrastructure only after a measured constraint.
This is a strategic inference across company histories, not a law stated by any one source.

**Florence decision.** Use this boundary:

| Florence must own | Florence can rent behind a replaceable seam | Defer until measured need |
|---|---|---|
| Person, household, chat, and service identity | Frontier, open-weight, or local models | Custom model training/serving |
| Consent, scope, retention, deletion, and participant epochs | Embedding and reranking models | Universal retrieval platform |
| Source/evidence ledger and accepted household state | Sandbox compute and ordinary cloud primitives | Custom sandbox fleet |
| Memory candidates, promotion, correction, expiry, and revocation | Generic connector/OAuth libraries that preserve Florence's boundary | Public skills marketplace |
| Context compilation and capability grants | Replaceable specialist-agent harness | Second orchestration control plane |
| Durable work, effects, reconciliation, and outcomes | Generic tracing transport under Florence privacy policy | Graph database without a proven query need |
| Product-shaped evals, promotion, and rollback | Queue/database hosting | Consumer VPC or on-prem product |

Zero-data-retention and resumability can conflict: a provider cannot resume from state it never
retained. Florence should therefore keep the minimum durable state it needs under its own policy and
rehydrate providers from that state, rather than assuming one provider setting can provide both
properties.

## 13. What the newer Prime Intellect, Nous/Hermes, Ribbit, and Life-OS audits add

The 45th turn does not overturn the architecture above. It makes four boundaries more precise and
adds one product test that the original snapshot did not contain.

### Prime Intellect: own the evaluated family-work loop, not model infrastructure

**Source fact.** Prime Intellect describes an `environment` as the combination of task data, a
harness with tools and context management, a runtime, and reward or evaluation machinery. Its
`verifiers v1` work separates taskset, harness, runtime, and typed trace. Prime Agent also shows that
a long task can retain a recoverable session, append-only log, scratch kernel, goals, and budgets
without making that session the authoritative system of record. Prime's reward-hacking work is
direct evidence that a worker optimizing a target must not also control the protected evaluator or
promotion boundary. Sources: [Prime Lab](https://www.primeintellect.ai/blog/lab-is-open),
[verifiers v1](https://www.primeintellect.ai/blog/verifiers-v1),
[Prime Agent](https://www.primeintellect.ai/blog/prime-agent), and
[Systematic Reward Hacking](https://www.primeintellect.ai/blog/reward-hacking). The complete
40-post disposition is in the
[Prime Intellect audit](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/prime-intellect-blog-audit-2026-08-05.md>).

**Florence decision.** A Florence job should have four independently versioned identities even if
the first implementation stores them together:

| Contract | Florence meaning |
|---|---|
| Task | The family outcome, permitted evidence, destination, deadline, risk, completion rule, and escalation rule |
| Harness | The orchestrator/skill/worker procedure attempting that task |
| Runtime | The selected model, provider, sandbox, tools, budgets, and credentials |
| Trace | The immutable proposals, disclosures, tool/effect activity, approvals, receipts, and terminal result |

This is not a reason to build an RL platform or a generic environment marketplace. It is a reason
to make a coverage-loop evaluation portable across models and harnesses. Florence should be able to
ask: with the same authorized family episode and the same protected outcome criteria, did the new
procedure close the loop more reliably, earlier, with fewer interruptions, and without disclosure?

“Ephemeral worker” also becomes more exact. A worker is **non-authoritative and bounded**, not
necessarily erased after every model call. A difficult research or planning job may use recoverable
scratch state while it is active. That state has a retention limit, can be revoked and rebuilt, and
never becomes family truth merely because the session survived.

### Nous and Hermes: a strong horizontal reference, not Florence's durable brain

**Source fact.** Hermes implements provider routing; progressively loaded `SKILL.md` procedures;
small curated memory separate from searchable session history; fresh isolated child contexts; and a
durable task board outside child conversations. Leaf workers cannot delegate, ask the user, write
memory, send messages, or schedule recurring work. The separate self-evolution repository proposes
skill variants, evaluates them, and opens an inspectable change for human review rather than writing
production behavior directly. Sources: [Hermes docs](https://hermes-agent.nousresearch.com/docs),
[Hermes repository](https://github.com/NousResearch/hermes-agent), and
[Hermes self-evolution](https://github.com/NousResearch/hermes-agent-self-evolution). The complete
18-post Nous catalog and pinned-repository audit is in the
[Nous/Hermes audit](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/nous-hermes-blog-audit-2026-08-05.md>).

**Florence decision.** Hermes validates the intended control shape:

- the persistent Florence relationship and database own objectives, loops, permissions, memories,
  jobs, and receipts;
- child workers begin from an explicit disclosure packet and return a typed proposal;
- ordinary leaf workers cannot message a person or group, change memory, schedule future work,
  widen tools, or commit household state;
- a skill is a governed procedure, not a fact and not a new authority grant;
- active scratch, searchable task history, accepted personal memory, and household state remain
  separate; and
- provider fallback may preserve a casual answer, but an authoritative or proactive run that
  changes provider/model/configuration receives a new run identity and is reevaluated.

Florence may use Hermes, Deep Agents, Mastra, or another runtime behind this seam if one wins a
bounded evaluation. None should own Florence's person graph, participant epochs, disclosure policy,
coverage-loop state, accepted memory, action approval, or outcome ledger.

### The completed Life-OS audit: four different authorization crossings

**Prior-task synthesis.** The completed 107-turn cross-task audit separates four permissions that
agent products often collapse:

1. **Ingestion:** may Florence read or mirror this source?
2. **Retrieval/disclosure:** may this source or derivative enter this exact task and recipient scope?
3. **Acceptance:** who may promote a proposal into personal or shared accepted state?
4. **Destination/action:** may Florence send, write, purchase, schedule, or otherwise affect this
   target?

It also separates tools, skills, and workers; makes unresolved evidence an explicit operational
state; distinguishes learning family facts from learning how Florence should work; and keeps one
stable visible work owner over hidden disposable workers. The source is the completed
[cross-task strategy audit](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/prior-chat-019fca66-strategy-audit-2026-08-05.md>).

**Florence decision.** These crossings must be separate domain checks and receipts, not one broad
`has access` flag or a prompt instruction. Adding Florence to a chat grants ingestion for the new
participant epoch; it does not by itself grant disclosure from Gmail, acceptance of a household
fact, or permission to message that group. Registration and the participant-policy intersection
govern the latter independently.

### Ribbit: judge the group-chat wedge by the recursive loop it earns

**Source fact.** Ribbit's 2026 *Power Letter* argues, as an investor thesis rather than established
fact, that a bottleneck becomes a platform only when solving it earns the customer relationship,
operational context, evaluation data, and recursive product loop around the work. It also argues
for standardizing the work buyers need rather than the machinery used to produce it. The public
source and claim-by-claim treatment are recorded in the
[Ribbit audit](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/ribbit-power-letter-thesis-audit-2026-08-05.md>).

**Florence decision.** This is the right product test for the first wedge. Florence is not valuable
because it can ingest many parental sources or emit many reminders. The standardized unit of value
is a family loop brought to a safe terminal state—initially an acknowledged coverage commitment.
The wedge earns the larger family Chief-of-Staff product only if closing those loops creates:

- a trusted direct relationship with each registered participant;
- accurate, scoped family context and accepted routines;
- reusable failure and evaluation cases;
- lower interruption and earlier resolution over time;
- additional family jobs pulled into the same relationship; and
- privacy-preserving product learning that never assumes cross-family data rights.

The recursive family loop is therefore:

```text
permitted source or direct request
-> provisional family need
-> clarification under the smallest valid scope
-> accepted coverage or other commitment
-> useful timing and coordination
-> acknowledgement, contradiction, correction, or expiry
-> proposed routine / policy / skill improvement
-> protected evaluation and governed promotion
```

This reinforces the chosen north-star metric: **loops safely closed**, segmented by whether Florence
opened the loop proactively, how early it was resolved, how many unnecessary interruptions it
caused, and whether any privacy or correction failure occurred. Messages sent, sources ingested,
workers spawned, and tokens consumed are diagnostic costs—not product success.

### What these additions explicitly do not justify

They do not justify building custom model training, distributed inference, an RL environment hub, a
general agent runtime, a skill marketplace, a visible swarm, a universal family ontology, or a
parallel enterprise-style VPC product. They also do not weaken the registration/write gate or let a
worker self-promote a memory, permission, procedure, or action. The newer research makes Florence's
provider-neutral seams and evaluation model stronger; it does not widen v1.

## What should transfer from Rivermill—and what should not

The earlier task was about an enterprise system operating inside customer-controlled environments,
with immutable document evidence, a versioned ontology, expert review, explicit verification, and a
canonical business-fact ledger. That is valuable prior-thread context, but it is not external proof
and it is not Florence's product model.

| Rivermill assumption | Why it worked there | Florence adaptation |
|---|---|---|
| Expert verification before canonical truth | Consequential enterprise document extraction with trained reviewers | Risk-shaped promotion: explicit statements and low-risk personal preferences can be accepted cheaply; ambiguous or consequential shared facts need authority |
| One organizational ontology | One company governs business object definitions | Small household model plus person-specific and chat-specific scopes; no single adult silently defines another's private truth |
| Customer VPC as deployment invariant | Enterprise data-residency and procurement requirement | Strong app-owned encryption, isolation, export, and provider controls first; consumer VPC is not an MVP requirement |
| Immutable document corpus as the main evidence | Controlled insurance documents and review workflows | Noisy, changing chats, email, calendars, and direct statements with different validity and retention rules |
| Formal Review and Verification stages | High-value data admission can tolerate expert workflow | Plain-language confirm, correct, share, forget, and temporary controls; ask only in proportion to risk |
| Heavy FDE and deployment workbench | Bespoke enterprise deployments and schemas | One opinionated consumer product; household customization should be simple routines and settings |
| Large enterprise skill library and AgentStudio | Many domain teams and internal tools | Small product-owned registry; expose routines, not agent configuration |
| One institution's security boundary | Employees and systems under an organizational policy | Multiple adults, children, guests, changing groups, and overlapping private/shared contexts |
| Business decision and outcome ledger | Auditable institutional operations | Lightweight commitments, plans, reminders, actions, and outcomes; preserve dignity and avoid surveillance UX |

The strict Rivermill rule “candidate or Review approval never becomes verified truth without explicit
Verification” should not transfer literally. It would make a family assistant exhausting. The
underlying separation should transfer, while Florence uses authority and risk to decide when an
explicit confirmation is necessary.

## Tensions the research does not eliminate

| Tension | What the evidence says | Florence resolution |
|---|---|---|
| Continual memory vs stale-memory harm | OpenAI and Applied Compute show continuity benefits; Ramp shows stale or excessive memory can regress quality | Scope, timestamp, source, expiry, ablation, and correction are first-class |
| Context graph vs retrieval-only approaches | Palantir/Glean/Foundation argue for structured context; Cerebras and Anthropic show strong retrieval patterns | Use typed operational state where decisions require it; keep source retrieval alongside it |
| Persistent agents vs persistent state | Product marketing often calls scheduled or resumable systems “persistent agents”; engineering accounts externalize state | Persist the relationship, job, artifacts, and outcomes; keep cognition finite |
| Multi-agent gains vs coordination failures | Anthropic and Hebbia show useful decomposition; Cognition and Cursor document thrashing and fragmented decisions | Fan out independent reads and fresh review, never competing canonical writers |
| Zero retention vs recovery | Less provider retention improves exposure limits; recovery requires some durable state | Florence retains minimum encrypted state and rehydrates disposable providers |
| Citations vs truth | Retrieval systems can produce cited answers | Citation proves origin, not validity, scope, agreement, or freshness |
| Open weights vs ownership | Open weights improve deployment and weight-level control | Florence's core ownership is data, semantics, procedures, permissions, evals, and outcomes; weights remain optional |
| Custom infrastructure vs speed | Stripe, Sierra, Harvey, and others built substantial infrastructure after repeated demand | Rent first; build only against a measured failed invariant |

## Unknowns Florence still has to test

The research narrows the design, but it does not answer these product questions:

1. Which memories people actually expect Florence to retain automatically, ask about, or forget.
2. Whether source links increase trust or create clutter in ordinary family conversations.
3. The right defaults for children, guests, separated households, caregivers, and changing groups.
4. How deletion should propagate through summaries, embeddings, backups, derived group-safe artifacts,
   and audit requirements.
5. Which first five routines recur often enough to deserve skills rather than ordinary prompts.
6. What amount of proactive behavior feels calming rather than intrusive.
7. How much source material can be processed locally or under minimal-retention provider settings
   without unacceptable cost or latency.
8. Which retrieval failures actually occur in family corpora: missing exact dates, stale routines,
   cross-thread confusion, identity ambiguity, or semantic misses.
9. Which low-risk memories may be accepted from an explicit statement without a separate confirmation.
10. What legal and consent requirements apply to group participants who never create a Florence
    account.

These should become product studies and risk-shaped evaluation cases, not assumptions hidden in a
general “memory” feature.

## Recommended sequence

1. **Write the boundaries first.** Make source, derivative, candidate, accepted memory, household
   state, skill, artifact, and action distinct in the product contract.
2. **Make memory inspectable.** Ship scoped source links, edit/forget/temporary controls, expiry, and
   correction before trying aggressive automatic synthesis.
3. **Start the skills library small.** Register a few versioned product routines with typed outputs
   and evaluations; do not expose a technical builder to families yet.
4. **Keep execution disposable.** Rehydrate workers from app-owned state, with narrow tools and no
   direct commit authority.
5. **Close the outcome loop.** Persist whether a reminder, calendar action, message, or commitment
   actually succeeded and whether the family corrected or dismissed it.
6. **Add governed improvement.** Let production evidence propose memory and skill changes only after
   protected privacy, action, and regression evaluations exist.
7. **Revisit infrastructure later.** A custom retrieval engine, graph database, model host, or
   sandbox fleet needs a measured Florence workload and a failed supplier invariant.

The acceptance test is not “Florence has memory.” It is:

> **Florence recalls the right thing for the right person, at the right time, from a source they can
> understand; it forgets or corrects it when asked; and it never turns private or uncertain material
> into shared truth by accident.**

## Appendix A: exact 45-turn audit

This table accounts for every turn reported by the exact task audit. “Transfer” means the turn
contains an architecture or product principle used in this note. “Context only” means it informed
the Rivermill strategy but is not evidence for Florence's architecture. Incomplete turns are retained
rather than silently treated as completed research.

| # | Turn and status | Main subject | Treatment here |
|---:|---|---|---|
| 1 | `019fd53e-69e1-7692-ae9a-8c4cbcfc68e4` — completed | Prime Intellect, Nous/Hermes, and Ribbit | Core transfer: evaluated task environments, bounded recoverable workers, governed skills, and the loop-closure wedge test |
| 2 | `019fd52f-6a09-7642-a758-187b467abc79` — completed | Full 107-turn Life-OS cross-chat audit | Core transfer: stable orchestrator, separate tools/skills/workers, four authorization crossings, explicit uncertainty, and two learning loops |
| 3 | `019fd46c-a62b-7363-bbcc-0959fe371544` — completed | Stripe KAI, Cerebras, and Palantir layers | Core transfer: skills, retrieval, and operational state remain separate |
| 4 | `019fd326-5b85-74a1-9cf9-9ae2bedfc682` — completed | Retrieval plus operational data | Core transfer: synchronized retrieval and accepted-state representations |
| 5 | `019fd31a-63f1-7102-a6b6-20091e1cb0b9` — completed | Cerebras Knowledge | Core transfer: source-specific, cited, hybrid retrieval; not truth promotion |
| 6 | `019fd308-1e0b-7533-b570-7327965a84c3` — completed | Rivermill 30/60/90 plan | Context only; enterprise hydration roadmap does not transfer |
| 7 | `019fd2b4-cb38-7883-9fa7-174e89aae466` — completed | Immediate sale versus ownership mission | Adapted principle: solve a recognizable job before selling a platform thesis |
| 8 | `019fd2b3-1e95-7da3-959b-3054e437f23a` — completed | “AI-native Palantir” as archetype | Context only; avoids using a competitor analogy as Florence's product language |
| 9 | `019fd285-3fed-71e0-9fda-60a404c911a0` — completed | Customer-grounded company roadmap | Context only, except for evidence-before-platform sequencing |
| 10 | `019fd27b-47be-73d1-83b9-e245c6b1beb1` — completed | Customer VPC constraints | Adapt: preserve control boundaries; reject consumer VPC as an assumed MVP requirement |
| 11 | `019fd263-ee77-72a1-825c-283a1e6ee880` — completed | Four-person build-versus-buy | Core transfer: own differentiated state and governance; rent commodity runtime |
| 12 | `019fd263-24f0-76b3-88d2-48b040ff05fd` — interrupted | Recheck build-versus-buy | Retained as interrupted; no final conclusion imported |
| 13 | `019fd256-863a-7800-87a7-58b71bb5291f` — failed | Build-versus-buy request | Retained as failed; no evidence attributed to it |
| 14 | `019fd244-712c-7ac3-86a1-87578dd5a420` — completed | Internal knowledge tooling and company comparables | Transfer: deployment memory and source-aware internal knowledge; ARR analysis excluded |
| 15 | `019fcfe0-6b77-7ff1-8897-e8ec1616c9ae` — completed | Application-led authority layer and competition | Adapt: application before platform; insurance company plan excluded |
| 16 | `019fcf58-d72d-7542-8637-9fee88c13711` — interrupted | Publisher-corpus discovery and transcripts | Corpus-method input only; conclusions come from completed ledgers, not this interrupted turn |
| 17 | `019fcf50-63be-7d00-b8b2-1d44969c18f3` — completed | Mission-language clarity | Product-writing lesson only: use plain language for nontechnical users |
| 18 | `019fcf30-d816-7bf3-a28b-3347c2ceb4d6` — completed | Company thesis and insurance risk markets | Insurance-market analysis excluded; underlying evidence/action/outcome loop adapted |
| 19 | `019fcf2c-b933-7eb3-a902-a9f41c3e2c4a` — completed | Correct role of an ontology | Core transfer: ontology-native where operational meaning is needed, never ontology-only |
| 20 | `019fcf1f-839f-7bd3-bb33-12a7f12a38a5` — completed | Verified data foundation and corpus rights | Adapt: evidence quality and rights matter; Marsh-specific offer excluded |
| 21 | `019fcf1d-10af-79c3-b6bb-ab3d1b6a5882` — completed | First application versus extraction | Transfer: a complete user outcome earns infrastructure; placement use case excluded |
| 22 | `019fcee9-71de-7cb1-b1ee-cda96ab7fab0` — completed | Company type and stack ownership | Strategy context; portability and customer-owned state adapted |
| 23 | `019fcee0-6ece-7b92-acb5-4c17d69d541f` — completed | Product evidence versus platform ambition | Transfer: trustworthy proof engine and disposable experiments; vertical plan excluded |
| 24 | `019fcec2-35f9-7b51-9761-a10f458fb24d` — completed | Mission, operating system, speed | Context only; speed rhetoric is not architecture evidence |
| 25 | `019fcea2-06ed-76b0-b56e-0735d5d56742` — completed | Full mission/market/transcript synthesis | Context only except for source hierarchy and customer evidence priority |
| 26 | `019fce94-33c7-7012-af8c-99c425e7fb4d` — completed | Customer transcripts, normalization, and Vouch | Adapt: known-purpose normalization is possible; universal one-time extraction is not |
| 27 | `019fce92-6769-7b91-b02d-fd39c2b353dd` — completed | Plain-language ontology explanation | Transfer: shared operational model; terminology hidden from consumers |
| 28 | `019fce7f-e79c-7a03-9412-39a6229044ac` — completed | Sales evidence and decision/action/learning loop | Adapted architecture principle; people and prospect mapping excluded |
| 29 | `019fce7a-8e36-7f70-8943-dea2ac6cdf04` — completed | Incomplete customer evidence | Retained as a deliberate no-conclusion turn |
| 30 | `019fce73-52f7-7211-bcba-72e202efaa4b` — completed | Product sketches and longitudinal memory | Transfer: outcomes and corrections make memory more useful than static extraction |
| 31 | `019fce6f-afa3-7a82-a4f1-71f165820824` — completed | Distinctive mission language | Context only; no architecture fact imported |
| 32 | `019fce6b-aebe-7bc3-b6f3-4d31638c663b` — completed | Institutional learning mission | Adapted principle: durable learning needs explicit ownership and control |
| 33 | `019fce63-6007-7550-974f-6d75c781edb4` — completed | Generic thesis and governed decision episode | Transfer: generic “agents + memory” is not differentiation; authority matters |
| 34 | `019fce4f-ba22-74b2-948c-5c081750dd83` — completed | Open models as doctrine | Transfer: model portability is doctrine, not product mission |
| 35 | `019fce46-73ea-7df2-a43b-a3d3db567e4e` — completed | Mission and frontier-lab comparison | Recruiting/mission context only |
| 36 | `019fce33-7a58-7ec0-92f2-d16e26c014e7` — completed | Customer-owned compounding intelligence | Strategy origin for owned durable state; not direct external evidence |
| 37 | `019fce31-ef45-7e33-aa2a-a5a72845d1ea` — completed | Owning agents and skills on private intelligence | Transfer: skills and models must remain subordinate to owned context and governance |
| 38 | `019fce2d-a8d7-7503-920d-878bcd5e174b` — completed | Authoritative learning record | Transfer: source once where possible, interpret versionally, retain accepted state |
| 39 | `019fce28-3f6d-7bf3-9108-eee25f5d0777` — completed | “Extract once” feasibility | Core transfer: immutable source can be reused; interpretation cannot be permanently complete |
| 40 | `019fce24-a7e6-73c3-84fa-35d974b33a1f` — completed | Missing downstream use case | Transfer: infrastructure must close a real outcome loop |
| 41 | `019fce1e-c840-7423-8544-dde8eb50b66d` — completed | Evidence, decisions, actions, outcomes, evals | Core transfer: model- and runtime-independent learning record |
| 42 | `019fce1c-baea-7ac2-b07d-15771b821c08` — completed | Horizontal company ambition | Context only; insurance wedge excluded from Florence architecture |
| 43 | `019fce0a-b957-7683-82cd-74382436f278` — completed | Competitor landscape and mission | Market map excluded; source/control-plane distinctions adapted |
| 44 | `019fcdef-6e57-7562-ab82-cba406ddb1dd` — completed | Applied Compute and open-model layer | Transfer: procedural memory and model portability; “AI supercloud” strategy excluded |
| 45 | `019fcddf-1a27-7ac1-acdc-88d3aec37c9f` — completed | Initial Stripe, Weng, sovereignty, Palantir, and context-graph synthesis | Seed research; every major claim rechecked against stronger direct sources here |

## Appendix B: exact 130-HTTP-target inventory

This is an audit of 130 unique exact link targets in the 45-turn transcript, not a claim that they
represent 130 distinct or equally reliable primary sources. `Core` means useful direct
architecture/product evidence.
`Supporting` means policy, strategy, or product context. `Market-only` means it informed Rivermill's
market, customer, recruiting, or insurance work but does not support Florence architecture.
`Private/unusable` includes access-controlled and malformed strings. Repeated identical targets are
counted once; different URL forms pointing to the same underlying source remain visible.

| ID | Exact linked URL string | Audit treatment |
|---:|---|---|
| L001 | [Stripe KAI with trailing query marker](https://stripe.dev/blog/meet-stripes-knowledge-ai-platform?) | Duplicate form of L002 |
| L002 | [Stripe KAI](https://stripe.dev/blog/meet-stripes-knowledge-ai-platform) | Core: skills, sandboxes, task scope, domain-owner governance |
| L003 | [Cerebras Knowledge](https://www.cerebras.ai/blog/how-we-built-our-knowledge-base) | Core: source-specific hybrid retrieval; vendor claims flagged |
| L004 | [Cerebras X post](https://x.com/cerebras/status/2077822555159945507) | Supporting social pointer; L003 is stronger |
| L005 | [Palantir Ontology overview](https://www.palantir.com/docs/foundry/ontology/overview) | Core: operational object/action/security model |
| L006 | [Harvey cloud-agent infrastructure](https://www.harvey.ai/blog/why-we-built-our-own-cloud-agent-infrastructure) | Core: demand-gated custom runtime and auditability |
| L007 | [Decagon air-gapped deployment](https://decagon.ai/blog/what-an-air-gapped-ai-deployment-actually-requires) | Supporting enterprise deployment evidence; adapt, do not copy |
| L008 | [Sierra context engine](https://sierra.ai/blog/context-engine) | Core/strategy: durable context around rented intelligence |
| L009 | [Extend credits](https://docs.extend.ai/2026-02-09/general/how-credits-work) | Market/build-buy input for extraction; not Florence architecture |
| L010 | [Reducto Extract overview](https://docs.reducto.ai/extract/overview) | Market/build-buy input for extraction; not Florence architecture |
| L011 | [Census insurance-agency profile](https://data.census.gov/profile/524210_-_Insurance_agencies_and_brokerages?codeset=naics~524210) | Market-only |
| L012 | [Census County Business Patterns dataset](https://www2.census.gov/programs-surveys/cbp/datasets/2023/cbp23us.zip) | Market-only |
| L013 | [Guidewire FY2025 release](https://ir.guidewire.com/news-releases/news-release-details/guidewire-announces-fourth-quarter-and-fiscal-year-2025) | Market-only/company sizing |
| L014 | [Guidewire filing](https://www.sec.gov/Archives/edgar/data/1528396/000152839625000221/gwre-20250731.htm) | Market-only/company sizing |
| L015 | [Palantir 2025 10-K](https://investors.palantir.com/files/2025%20FY%20PLTR%2010-K.pdf) | Supporting company context; not engineering proof |
| L016 | [ServiceNow filing](https://www.sec.gov/Archives/edgar/data/1373715/000137371526000007/now-20251231.htm) | Market-only/company sizing |
| L017 | [Arclave](https://www.arclave.com/) | Market-only competitor/company page |
| L018 | [Anansi Labs](https://anansi-labs.ai/) | Market-only competitor/company page |
| L019 | [Palantir Ontology System](https://www.palantir.com/docs/foundry/architecture-center/ontology-system) | Core: data, logic, actions, and security |
| L020 | [OpenAI Frontier](https://openai.com/index/introducing-openai-frontier/) | Supporting provider/product context |
| L021 | [Applied Compute: Remember, Refine, Retrieve](https://www.appliedcompute.com/research/remember-refine-retrieve) | Core: trace/document-derived procedural memory; authority gap noted |
| L022 | [Foundation Capital context graphs](https://foundationcapital.com/ideas/context-graphs-ais-trillion-dollar-opportunity) | Supporting strategic thesis, not engineering validation |
| L023 | [OpenAI business-data policy](https://openai.com/business-data/) | Core policy evidence for correcting training-data overclaim |
| L024 | [Anthropic commercial-data policy, older URL](https://privacy.claude.com/en/articles/7996868-is-my-data-used-for-model-training) | Supporting policy evidence; see L121 for current framing |
| L025 | [Ramp background agent](https://builders.ramp.com/post/why-we-built-our-background-agent) | Core: isolated execution, workflow integration, evaluation |
| L026 | [Stripe Minions](https://stripe.dev/blog/minions-stripes-one-shot-end-to-end-coding-agents) | Core: bounded isolated workers and deterministic checks |
| L027 | [Stripe Minions part 2](https://stripe.dev/blog/minions-stripes-one-shot-end-to-end-coding-agents-part-2) | Core: blueprint/state-machine orchestration |
| L028 | [Harvey document processing](https://www.harvey.ai/blog/scaling-document-processing-across-harvey) | Supporting enterprise ingestion evidence |
| L029 | [Harvey Vault uploads](https://www.harvey.ai/blog/faster-more-reliable-vault-uploads) | Supporting ingestion/reliability evidence |
| L030 | [Sierra Agency](https://sierra.ai/blog/agency-secure-scalable-sandboxes-for-agents) | Core: isolation, credentials, recovery, egress |
| L031 | [Sierra AI-pilling lessons](https://sierra.ai/blog/ai-pilling-our-company-lessons-learned) | Supporting adoption/sequencing evidence |
| L032 | [Sierra Agents as a Service](https://sierra.ai/blog/agents-as-a-service) | Supporting product/strategy evidence |
| L033 | [Decagon blog index](https://decagon.ai/resources/blog) | Index, not affirmative evidence; corpus ledger supplies article dispositions |
| L034 | [Palantir developer toolchain](https://www.palantir.com/docs/foundry/dev-toolchain/overview) | Supporting enterprise workflow/tooling evidence |
| L035 | [OpenAI Codex overview](https://openai.com/codex/) | Supporting provider product page; current memory claims use specific docs |
| L036 | [GitHub Copilot custom agents](https://docs.github.com/en/copilot/concepts/agents/cloud-agent/about-custom-agents) | Supporting runtime comparison |
| L037 | [Lloyd's market](https://www.lloyds.com/about-lloyds/our-market/lloyds-market) | Insurance-market-only |
| L038 | [NAIC insurance-linked securities](https://content.naic.org/insurance-topics/insurance-linked-securities) | Insurance-market-only |
| L039 | [CFTC glossary](https://www.cftc.gov/LearnAndProtect/AdvisoriesAndArticles/CFTCGlossary/index.htm) | Insurance/financial-market-only |
| L040 | [Lloyd's delegated underwriting](https://www.lloyds.com/market-resources/delegated-authorities/market-knowledge/delegated-underwriting-guidance) | Insurance-market-only |
| L041 | [Whitespace specialty-insurance APIs](https://www.whitespace.co.uk/news-events/apis-in-specialty-insurance/) | Insurance-market-only |
| L042 | [Marsh digital trading](https://www.marsh.com/en/about/media/marsh-advances-data-first-digital-trading-transformation.html) | Customer/insurance context only |
| L043 | [Palantir AIP architecture](https://www.palantir.com/docs/foundry/architecture-center/aip-architecture) | Core/supporting: agents and evaluations over governed operational state |
| L044 | [Alfred Lin: speed](https://x.com/Alfred_Lin/status/2084636778791858256) | Weak social/operating advice; not architecture evidence |
| L045 | [X `@Keller` string](https://x.com/@Keller) | Malformed/noncanonical profile string; no architecture use |
| L046 | [X `@zipline` string](https://x.com/@zipline) | Malformed/noncanonical profile string; no architecture use |
| L047 | [X `@tanay_tandon` string](https://x.com/@tanay_tandon) | Malformed/noncanonical profile string; no architecture use |
| L048 | [X `@CommureOS` string](https://x.com/@CommureOS) | Malformed/noncanonical profile string; no architecture use |
| L049 | [Alfred Lin earlier post](https://x.com/Alfred_Lin/status/2044047176154923091) | Weak social/operating advice |
| L050 | [Analytics variant of L049](https://x.com/Alfred_Lin/status/2044047176154923091/analytics) | Duplicate/analytics-only form |
| L051 | [Sierra context engineering](https://sierra.ai/blog/context-engineering-the-key-to-great-agents) | Core: progressive, task-specific context |
| L052 | [Poetic](https://poetic.com/) | Market-only competitor/company page |
| L053 | [Distyl](https://www.distyl.ai/) | Market-only competitor/company page |
| L054 | [Fulcrum](https://www.withfulcrum.com/) | Market-only competitor/company page |
| L055 | [Vantel](https://www.vantel.com/) | Insurance competitor/product context only |
| L056 | [Private Rivermill Slack profile](https://rivermillworkspace.slack.com/team/U0ADRV3362W) | Private/access-controlled customer context |
| L057 | `https://www.cutoday.info/Fre…:` | Truncated and unusable URL string |
| L058 | `https://www.…:` | Truncated and unusable URL string |
| L059 | [SS&C Advent](https://www.advent.com/) | Financial-services market-only |
| L060 | [InvestCloud APL](https://www.investcloud.com/managed-account-solutions/apl/) | Financial-services market-only |
| L061 | [Ridgeline](https://ridgeline.ai/) | Financial-services market-only |
| L062 | [Nicolas Zerbib](https://www.stonepoint.com/team/nicolas-d-zerbib/) | People/investor research only |
| L063 | [Robert Mulcare](https://www.linkedin.com/in/robert-mulcare-92a15429/) | People research only |
| L064 | [Private Google Sheet, default view](https://docs.google.com/spreadsheets/d/1o2JRhysTHOeBnQvVHx19u9pR4_20Rq-oOFup4VViK9Q/edit?usp=sharing) | Private/access-controlled research input |
| L065 | [Patrick Dunne](https://hightoweradvisors.com/patrick-dunne.html) | People/customer research only |
| L066 | [Vertafore](https://www.vertafore.com/) | Insurance incumbent/market-only |
| L067 | [Applied Systems](https://www1.appliedsystems.com/) | Insurance incumbent/market-only |
| L068 | [CRC REDY Intel](https://www.crcgroup.com/About-Us/Latest-News/crc-introduces-redy-intel-the-ai-engine-powering-the-future-of-specialty-insurance) | Insurance customer/competitor context only |
| L069 | [Private Rivermill Slack profile](https://rivermillworkspace.slack.com/team/U0BBB4LH6JC) | Private/access-controlled customer context |
| L070 | [Private Rivermill Slack profile](https://rivermillworkspace.slack.com/team/U0ADVHLS8Q4) | Private/access-controlled customer context |
| L071 | [David de Picciotto](https://www.linkedin.com/in/david-de-picciotto/) | People research only |
| L072 | [Unique AI](https://www.unique.ai/) | Financial-services competitor context only |
| L073 | [Nevis Wealth](https://www.neviswealth.com/) | Financial-services market-only |
| L074 | [Arca Wealth](https://arcawealth.ai/) | Financial-services market-only |
| L075 | [PIT launch](https://pit.com/press/pit-launches-with-16m) | Financial-services/company market-only |
| L076 | [Private Google Sheet, range view](https://docs.google.com/spreadsheets/d/1o2JRhysTHOeBnQvVHx19u9pR4_20Rq-oOFup4VViK9Q/edit?gid=286067949#gid=286067949&range=A1) | Private/access-controlled variant of L064 |
| L077 | [John Zern](https://ryanspecialty.com/leader/john-g-zern/) | People/customer research only |
| L078 | [Stephen McAnena](https://www.linkedin.com/in/stephen-mcanena/) | People research only |
| L079 | [Michael Prestileo](https://www.linkedin.com/in/michaelprestileo/) | People research only |
| L080 | [Morgan Housel](https://www.linkedin.com/in/morgan-housel-5b473821/) | People research only |
| L081 | [Joseph Park](https://www.linkedin.com/in/josephrpark/) | People research only |
| L082 | [Thasunda Brown Duckett](https://www.linkedin.com/in/thasunda-brown-duckett-22b15523/) | People research only |
| L083 | [Randy Melville](https://www.linkedin.com/in/randy-melville-135aab6b/) | People research only |
| L084 | [Matthew Hamilton](https://www.summitpartners.com/team/matthew-hamilton) | People/investor research only |
| L085 | [Patriot GIS](https://patriotgis.com/) | Insurance/market-only |
| L086 | [Beyond Risk](https://www.beyondrisk.com/) | Insurance/market-only |
| L087 | [Andrew Lee](https://www.summitpartners.com/team/andrew-s-lee) | People/investor research only |
| L088 | [Paul Furer](https://www.summitpartners.com/team/paul-g-furer) | People/investor research only |
| L089 | [Zywave](https://www.zywave.com/) | Insurance incumbent/market-only |
| L090 | [Outmarket](https://outmarket.ai/) | Insurance competitor/market-only |
| L091 | [Xceedance](https://www.xceedance.com/) | Insurance services/market-only |
| L092 | [ReSource Pro](https://www.resourcepro.com/) | Insurance services/market-only |
| L093 | [Patra](https://www.patracorp.com/) | Insurance services/market-only |
| L094 | [Pierre-Olivier Desaulle](https://www.linkedin.com/in/pierre-olivier-desaulle-1673531/) | People research only |
| L095 | [Nicholas Lyons](https://www.linkedin.com/in/sir-nicholas-lyons-15838946/) | People research only |
| L096 | [Robert Hartman](https://www.linkedin.com/in/robert-hartman-397284/) | People research only |
| L097 | [Becky Redmond](https://www.linkedin.com/in/becky-redmond-7598763b/) | People research only |
| L098 | [Terrance Holbrook](https://www.linkedin.com/in/terrance-holbrook/) | People research only |
| L099 | [Zachary Elman](https://www.linkedin.com/in/zacharyelman/) | People research only |
| L100 | [NAIC producer licensing](https://content.naic.org/insurance-topics/producer-licensing) | Insurance regulation only |
| L101 | [OpenAI mission/about](https://openai.com/about/) | Mission/recruiting context, not architecture evidence |
| L102 | [Anthropic company](https://www.anthropic.com/company) | Mission/recruiting context, not architecture evidence |
| L103 | [Palantir careers](https://www.palantir.com/careers/index.html) | Recruiting context only |
| L104 | [Anthropic constitution](https://www.anthropic.com/constitution) | Supporting governance philosophy; not Florence authority design proof |
| L105 | [Patra quote comparison](https://www.patracorp.com/insurance-outsourcing-services/insurance-quote-comparison/) | Insurance competitor/use-case only |
| L106 | [FurtherAI product](https://www.furtherai.com/product) | Insurance competitor/use-case only |
| L107 | [Qumis](https://www.qumis.com/) | Insurance competitor/use-case only |
| L108 | [Applied Epic](https://www1.appliedsystems.com/en-us/solutions/for-agents/agency-management-system/applied-epic) | Insurance incumbent/use-case only |
| L109 | [Applied Systems acquisition of Cytora](https://www1.appliedsystems.com/en-uk/news/press-releases/2025/applied-systems-acquires-cytora/) | Insurance competitive context only |
| L110 | [Aon Broker Copilot](https://www.aon.com/en/about/better-decisions/aon-broker-copilot) | Insurance product context only |
| L111 | [WTW investor PDF](https://investors.wtwco.com/static-files/707fd56b-b6c9-4161-96e7-27e50625b211) | Insurance/company market-only |
| L112 | [Rhythm Garg X post](https://x.com/rhythmrg/status/2083742698322919866) | Weak strategic pointer about open-model infrastructure |
| L113 | [Applied Compute](https://www.appliedcompute.com/) | Supporting company/product context; research pages are stronger |
| L114 | [Applied Compute fundraise](https://www.appliedcompute.com/company/fundraise) | Company/market context only |
| L115 | [LangChain Stripe KAI case study](https://www.langchain.com/blog/how-stripe-built-their-knowledge-ai-platform-on-deep-agents) | Useful partner-reported implementation detail, not Stripe-authored proof |
| L116 | [Lilian Weng harness essay](https://lilianweng.github.io/posts/2026-07-04-harness/) | High-value synthesis, not a primary experimental paper |
| L117 | [Alana Levin sovereignty post](https://x.com/AlanaDLevin/status/2084435378723782686) | Strategic interpretation; training-data claim corrected with L023/L121 |
| L118 | [Palantir X post](https://x.com/PalantirTech/status/2083295381173993937) | Social pointer; L120 and Palantir docs are stronger |
| L119 | [Satya Nadella context-graph post](https://x.com/satyanadella/status/2076323181154230284) | Strategic corroboration, not engineering specification |
| L120 | [Palantir Institutional Sovereignty PDF](https://www.palantir.com/assets/xrfr7uokpv1b/7BF74dqccPeVFMHRmy7FO3/2a33ff9b4f9e11ba904445e637095960/Palantir_-_Institutional_Sovereignty_in_the_Age_of_AI.pdf) | Core/supporting first-party strategy and architecture material |
| L121 | [Anthropic commercial-data training policy](https://privacy.anthropic.com/en/articles/7996885-how-do-you-use-personal-data-in-model-training) | Core policy evidence for correcting the blanket training claim |
| L122 | [Open Source Initiative: open weights](https://opensource.org/ai/open-weights) | Core definitional evidence: open weights are not full system ownership |
| L123 | [Prime Intellect blog index](https://www.primeintellect.ai/blog) | Index for the completed 40-post audit; individual source dispositions are in Section 13 and Appendix C |
| L124 | [Nous Research blog index](https://nousresearch.com/blog) | Index for the completed 18-post audit; Hermes docs and repositories were discovered and audited separately |
| L125 | [Ribbit Perspective](https://www.ribbitcap.com/perspective) | First-party public listing for the 2026 *Power Letter*; strategic/investor evidence, not product validation |
| L126 | [Ribbit 2026 Power Letter public edition](https://docsend.com/view/bfjsdyp2vjj8k9hy) | Public primary source for the later 42-page thesis audit |
| L127 | [Prime Lab general availability](https://www.primeintellect.ai/blog/lab-is-open) | Core: task/environment/evaluation loop; model-training infrastructure remains a supplier layer |
| L128 | [Prime `verifiers v1`](https://www.primeintellect.ai/blog/verifiers-v1) | Core: explicit taskset, harness, runtime, and typed-trace separation |
| L129 | [Prime Agent](https://www.primeintellect.ai/blog/prime-agent) | Core/supporting: recoverable operational worker state remains distinct from authoritative product state |
| L130 | [Hermes Agent documentation](https://hermes-agent.nousresearch.com/docs) | Core implementation reference for provider routing, progressive skills, bounded memory, isolated delegation, and external durable work |

Count check: the ledger runs continuously from L001 through L130. It intentionally retains the
duplicate KAI form, analytics variant, private records, and two malformed/truncated strings because
removing them would make the transcript audit look cleaner than it was.

## Appendix C: derived engineering-corpus branch ledger

The exact corpus method and publisher counts are preserved in the original
[master inventory](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/ai-company-blog-master-inventory-2026-08-04.md>)
and [method note](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/ai-company-blog-corpus-method-2026-08-04.md>).
Those files distinguish discovery, editorial-boundary classification, full-body reading, and
analytical relevance. This Florence note audits the existing reconciled syntheses; it does not claim
to have repeated every body read.

### Reconciled publisher branches

| Publisher branch | Frozen substantive bodies | Reconciled ledger used | Directly useful evidence | Florence use and limit |
|---|---:|---|---|---|
| Sierra | 111 | [Sierra synthesis](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/sierra-blog-corpus-synthesis-2026-08-04.md>) | Sandboxes, credentials, event recovery, MCP identity, context engineering, application-to-platform sequence | Use capability boundaries, contextual loading, and demand-gated infrastructure. Do not import enterprise sales claims or assume the platform sequence is causal proof. |
| Harvey | 41 | [Harvey technical synthesis](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/harvey-technical-blog-corpus-synthesis-2026-08-04.md>) | Durable runs versus ephemeral sandboxes, provider adapters, short-lived credentials, resumability, document-processing reliability | Use the durable-run/runtime seam. Reported cost and customer outcomes remain vendor claims; legal-document scale does not define a family workload. |
| Stripe | 110 | [Stripe developer-blog synthesis](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/stripe-developer-blog-corpus-synthesis-2026-08-04.md>) | KAI skills/governance/task scope; Minion blueprints, isolation, deterministic validation; progressive steering | Use small progressive skills, proposal-only workers, and code-owned gates. Do not copy a 1,000-skill enterprise catalog or Stripe's mature internal platform. |
| Decagon | 130 + 2 linked partner bodies | [Decagon synthesis](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/decagon-blog-corpus-synthesis-2026-08-04.md>) | Versioned Agent Operating Procedures, deterministic validation, traces/simulations, proposal-and-approval improvement | Use governed procedure changes and layered evals. Do not infer that customer-support autonomy or vendor performance numbers generalize to families. |
| OpenAI | 1,009 | [OpenAI synthesis](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/openai-editorial-corpus-synthesis-2026-08-04.md>) | Externalized harness state, memory synthesis and controls, source-linked data work, bounded coding changes, deployment simulation | Use as one comparative supplier/research branch. It does not establish Florence's household authority model, and older product pages are not evidence of current behavior unless rechecked. |
| Anthropic | 424 | [Anthropic synthesis](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/anthropic-editorial-corpus-synthesis-2026-08-04.md>) | Managed-agent brain/hands seam, context engineering, isolated subagents, long-running artifacts, multi-agent limits, skills, containment | Use external state, narrow context, scoped workers, and explicit tool boundaries. Do not universalize coding/research experiments or self-reported gains. |
| Ramp | 47 | [Ramp Builders complete corpus](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/ramp-builders-complete-corpus-2026-08-04.md>) | Agent identity, background execution, Postgres workflow abstraction, risk separation, skills/memory benchmarking | Especially important counterevidence: stale or oversized memory can hurt. Ramp's accounting/risk domains and benchmarks are not family-product validation. |
| Palantir | 164 | [Palantir synthesis](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/palantir-editorial-corpus-synthesis-2026-08-04.md>) | Operational objects, actions, functions, security, audit, model/evaluation orchestration, sovereignty layers | Use operational-state and authorization vocabulary. Do not copy a heavyweight enterprise ontology, FDE motion, VPC assumption, or competitive training-data rhetoric. |
| Applied Compute | 19 | [Applied Compute complete corpus](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/applied-compute-complete-corpus-2026-08-04.md>) | Remember–Refine–Retrieve, trace-derived procedural memory, auditable natural-language memory, task-level episodes and evaluation | Use trace-derived candidates and model portability. It does not supply a complete authority/promotion model, and open-model infrastructure is not a near-term Florence need. |
| Glean | 829 substantive + one two-word stub; 88 routes without usable bodies among 918 editorial routes | [Glean synthesis](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/glean-editorial-corpus-synthesis-2026-08-04.md>) | Permission-aware context graphs, enterprise retrieval, filesystem context, progressive skills, trace learning | Use source permissions, progressive context, and candidate procedures. Do not equate an enterprise knowledge graph or trace consensus with accepted household truth. |
| Hebbia | 110 | [Hebbia synthesis](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/hebbia-editorial-corpus-synthesis-2026-08-04.md>) | Source-linked professional research, multi-agent decomposition, mixed deterministic/rubric evaluation, reusable skills and projects | Use bounded decomposition and cited work products. Its own corpus shows professionals still verify outputs; finance workbench success does not prove the need for canonical state in every task. |
| Rogo | 70 | [Rogo synthesis](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/rogo-editorial-corpus-synthesis-2026-08-04.md>) | Firm-owned model-independent context, agent/skill library, citations, permissions, evals, artifacts, scheduled work | Strong counterexample to ontology-first design: cited artifacts plus human judgment solve many tasks. Use accepted state only where ambiguity blocks a consequential transition. |
| Cursor | 108 | [Cursor whole-corpus handoff](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/cursor-editorial-corpus-part-3-2026-08-04.md>) | Append-only external state, checkpointed environments, dynamic context artifacts, cloud delegation, swarm economics and failure modes | Use addressable artifacts, recovery, and narrow delegation. Coding-agent workflows and synthetic swarm results do not directly validate household workflows. |
| Cognition | 82 | [Cognition synthesis](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/cognition-editorial-corpus-synthesis-2026-08-04.md>) | Isolation, identity chains, persistent state, recovery, integrations, and explicit warnings against loosely coordinated multi-agents | Use as disconfirming evidence for agent swarms. “One authoritative commit owner” remains Florence's inference from the documented failure mode. |
| Factory | 60 | [Factory synthesis](</Users/harianbarasu/Projects/Rivermill/docs/strategy/research/factory-editorial-corpus-synthesis-2026-08-04.md>) | Mission definitions, triggers, one orchestrator, narrow workers, fresh validators, external shared state, compression limits | Strong precedent for structured delegation and artifact indexes. It remains a software-development system, not proof of a personal-life ontology. |

### Sources outside the 15 publisher corpora

| Branch | Role in the prior research | Treatment in this note |
|---|---|---|
| Lilian Weng | Research synthesis connecting harnesses, context, artifacts, evaluation, permissions, and self-improvement | Used as high-quality synthesis; material experimental claims are not treated as Weng-authored primary results |
| LangChain/Deep Agents | Partner case study and framework documentation for Stripe KAI, subagents, files, skills, memory, permissions, and context management | Useful implementation detail; kept behind a replaceable worker-runtime seam; partner-reported Stripe detail is not treated as independent proof |
| Cerebras | Focused first-party knowledge-base architecture outside the frozen 15-branch count | Used for source-specific retrieval; its post does not establish ACL correctness, citation entailment, deletion, or truth promotion |
| Prime Intellect | Complete later 40-post first-party corpus audit plus current product/research docs | Adds taskset/harness/runtime/trace separation, protected evaluation, and recoverable-but-nonauthoritative worker state. Training, inference, compute, and environment hosting remain supplier layers, not Florence's product. |
| Nous/Hermes | Complete later 18-post Nous catalog plus live Hermes docs and pinned agent/self-evolution repositories | Strong horizontal reference for progressive skills, isolated leaf workers, external durable task state, provider portability, and proposal/eval/promotion. It is not Florence's memory, authority, or system of record. |
| Ribbit 2026 Power Letter | Public 42-page investor thesis, read and audited separately | Used only for the bottleneck-to-recursive-loop product test and work-level unit of value. Its market forecasts and systems-company claims are not treated as consumer validation. |
| Completed Life-OS cross-task audit | Full 107-turn task plus its local research artifacts | Supplies prior-task synthesis—tool/skill/worker taxonomy, four authorization crossings, explicit uncertainty, and separate factual/procedural learning loops—not independent external evidence. |
| Foundation Capital and executive/social context-graph posts | Strategic category arguments about context graphs, sovereignty, and ownership | Corroboration only; never the sole basis for a Florence design decision |
| Open Source Initiative and provider data policies | Definitions and current policy boundaries | Used to correct “open weights = ownership” and “frontier provider = training on secrets” overclaims |
| Insurance, company, investor, customer, recruiting, people, and social research | Rivermill market sizing, product wedge, team, fundraising, sales, and mission work | Explicitly non-transfer unless a separate engineering source establishes the architecture pattern |
| Private transcripts, Slack, Google Sheets, and local Rivermill files | Customer/company context and prior-thread synthesis | Not external evidence; no private content is reproduced here |

The original source estate also contained **6,898 normalized HTTP(S) strings across 204 files**. That
number includes duplicated corpora, documentation, social links, people pages, customer artifacts,
and export noise. It is a discovery/indexing count, not 6,898 affirmative sources and not a measure
of research quality.

## Appendix D: current-memory source supplement

The following recent sources were added specifically to answer the user's “last three months”
question. They are not part of the transcript's L001–L130 count unless already present there.

| Date | Source | Direct relevance |
|---|---|---|
| 2026-05-14 | [OpenAI: recognizing context in sensitive conversations](https://openai.com/index/chatgpt-recognize-context-in-sensitive-conversations/) | Purpose-specific, factual, time-limited memory class excluded from general memory |
| 2026-05-27 | [OpenAI: building self-improving tax agents with Codex](https://openai.com/index/building-self-improving-tax-agents-with-codex/) | Source/citation/correction/outcome lineage and bounded evaluated code changes |
| 2026-06-04 | [OpenAI: ChatGPT memory dreaming](https://openai.com/index/chatgpt-memory-dreaming/) | Background synthesis, temporal updates, user review/edit, continuity/freshness evals |
| 2026-06-16 | [OpenAI: Deployment Simulation](https://openai.com/index/deployment-simulation/) | Privacy-filtered production replay and simulation; scale/results remain self-reported |
| 2026-06-22 | [OpenAI: Codex-maxxing for long-running work](https://openai.com/index/codex-maxxing-long-running-work/) | Open, editable, diffable external memory vault for decisions and project state |
| Current as of 2026-08-05 | [ChatGPT Memory FAQ](https://help.openai.com/en/articles/8590148-memory-faq) | High-level/incomplete summary, sources and correction, deletion complexity, Temporary Chat |
| Current as of 2026-08-05 | [Codex memories](https://learn.chatgpt.com/docs/customization/memories) | Local memory separate from ChatGPT web, evidence-bearing editable files, separate use/contribution controls |
| Current as of 2026-08-05 | [Codex Chronicle](https://learn.chatgpt.com/docs/customization/chronicle) | Source-finding memory, temporary screen capture, editable local output, injection/sensitivity risks |

Together, these sources support source-linked, editable, purpose-specific memory outside the active
model context. They do not establish that any provider has “solved” household truth, multi-person
privacy, complete deletion, or safe autonomous memory promotion.
