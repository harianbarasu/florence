# Pi and Hermes as task/runtime components for Florence

Date: 2026-08-27

Research scope: official Pi documentation and source at commit [`4e49492`](https://github.com/earendil-works/pi-mono/tree/4e494929998d6bc4fccf75e0a233f727db4b70ee), official Hermes Agent documentation and source at commit [`6dcebea`](https://github.com/NousResearch/hermes-agent/tree/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882), and the current Florence repository. No third-party comparisons were used.

## Conclusion

Florence should not adopt Pi or Hermes as its application runtime to fix the current “do work” gap.

The flight exchange failed before background execution mattered. Florence made `web_search` available only when the parent's typed message already contained a public HTTP URL. “DL 747” therefore had no current-information tool, the decision schema had no task-acceptance/result state, and nothing prevented the model from returning the conversational promise “I'll prioritize the earliest workable option tonight.” No search or durable job followed that sentence.

The right near-term fix is inside Florence:

1. Make safe public search available for ordinary current-information and identifier-resolution requests, not only URL-bearing messages.
2. Keep bounded lookups such as flight status and alternatives in the foreground tool loop. A lookup turn must end in a sourced result, one truly blocking question, or an explicit failure—not a future-tense promise.
3. Add one small, PostgreSQL-owned work lifecycle only for genuinely long tasks. It should extend Florence's existing single due-work seam, not become a second workflow engine.
4. Borrow Hermes's best lifecycle patterns—durable acceptance, progress, completion delivery, and honest `unknown` recovery—but keep Florence's existing privacy, authority, idempotency, and provider reconciliation as the control plane.
5. Reconsider Pi only if Florence later needs to replace its inner model/tool loop. Its stable TypeScript loop and event stream are useful, but they do not supply durable background work. Do not embed Hermes wholesale; it is an opinionated Python agent platform whose session, messaging, memory, gateway, and execution systems would duplicate Florence's core.

## What is actually missing in Florence

Florence already has a bounded model/tool loop. `apps/api/src/reasoner.ts` runs up to five model rounds with four tool calls, validates a strict structured decision, and exposes memory, source, Gmail, and Calendar reads. The missing pieces are the capability gate and a truthful work contract:

- `apps/api/src/reasoner.ts:1960-1968` adds `web_search` only when `publicHttpUrlsInText(currentAuthoredText(...))` finds a URL. A flight number, restaurant name, product, weather question, or any other natural public-data request without a URL cannot search.
- `apps/api/src/reasoner.ts:437-476` has no generic `work` or `task` result. The model can return conversation, facts, reminders/monitors, interest discovery, Calendar action, a household update, a web-page handoff, and research URLs—but cannot atomically say “this task was accepted under these capabilities and this audience.”
- `apps/api/src/reasoner.ts:2016-2054` can finish with a parsed conversational decision without proving that a requested external lookup occurred.
- `apps/api/src/florence.ts:1088-1094` serializes every runtime caller behind one `#activeRun`; `#runCycle()` then processes inbound, outbound, activation, Calendar, initial intelligence, and due work sequentially. Moving every lookup into long-running work here would increase response latency and head-of-line blocking.
- `packages/database/src/store.ts:6151+` is nevertheless the right safety kernel: `commitTurn()` transactionally rechecks the live inbound/authority row and commits visible effects. That invariant should survive any agent-loop change.

This is why a larger agent runtime alone would not have helped: a runtime can execute only the tools and task contract Florence gives it.

## Capability comparison

| Concern | Pi | Hermes Agent | Relevance to Florence |
|---|---|---|---|
| Agent loop | Yes. Small model/tool loop with steering and follow-up queues. | Yes. Large multi-provider loop with retries, compression, interrupts, budgets, and tool dispatch. | Florence already has the essential loop; neither fixes its missing search policy by itself. |
| Tool calling | Typed tools, argument validation, sequential/parallel execution, lifecycle events. | Large registry, built-ins, plugin tools, concurrent execution, tool guardrails. | Useful implementation patterns, not a substitute for Florence-scoped tools. |
| Background tasks | Intentionally not built in to stable Pi; extensions/external processes are expected. | Yes: background delegation, cron, terminal processes, persistent goals/kanban. | Hermes demonstrates a useful lifecycle, but its in-flight delegated worker is still process-local. |
| Durable state | Append-only JSONL conversation trees; compaction and branching. | SQLite sessions/messages plus separate ledgers such as `async_delegations`. | Transcript durability is not domain-work durability. Florence should keep PostgreSQL authoritative. |
| Progress/events | Strong agent/message/turn/tool/queue/compaction/retry event stream. | Callback surfaces for status, steps, streaming, tool progress; async delegation progress monitor. | Pi's event vocabulary is especially clean; Hermes's persisted completion rail is more relevant to long work. |
| Compaction | Yes, automatic/manual summary plus branch summaries. | Yes, batch and optional micro/native compaction, with session lineage. | Useful only for long conversations/tasks. A family fact vault must not be a lossy transcript summary. |
| Web/browser | No built-in web search or browser in the core; add extensions/skills/tools. | Built-in multi-provider web search/extract and accessibility-tree browser automation. | Florence needs a safe public-search tool now; full browser automation is a later, separately sandboxed capability. |
| Sandbox | No built-in sandbox; use a container, VM, micro-VM, or extension. | Multiple execution backends; hardened Docker option and task-isolated browser sessions. | Neither sandbox supplies household data policy. Browser/shell workers must receive minimum credentials and scope. |
| MCP | Intentionally not built in; possible through extensions. | Built-in client for stdio, Streamable HTTP, and SSE with dynamic discovery/reconnect. | MCP is a transport, not an authority model. Use only for a concrete provider capability. |
| Approvals | No built-in permission popups; extensions may implement policy. | Dangerous-command approvals and optional memory/skill-write approval/staging. | Hermes approvals protect host/tool execution, not two-adult household sharing or Calendar authority. |
| Crash semantics | Stable SDK persists the conversation tree, not a replay-safe external-effect workflow. | Persists tool-call intent before execution and labels orphaned side effects `unknown`; async completion delivery is durable. | Hermes is safer and more honest, but still not exactly-once. Florence's provider idempotency/reconciliation remains necessary. |

## Pi assessment

### What Pi provides

Pi describes itself as a minimal agent harness. Its stable core is a good TypeScript library rather than a full task platform:

- [`packages/agent/src/agent-loop.ts:95-260`](https://github.com/earendil-works/pi-mono/blob/4e494929998d6bc4fccf75e0a233f727db4b70ee/packages/agent/src/agent-loop.ts#L95-L260) emits agent/turn/message events, asks the model, validates and executes tool calls, appends results, and continues until no work remains. It also drains steering and follow-up queues.
- [`packages/agent/src/agent-loop.ts:380-800`](https://github.com/earendil-works/pi-mono/blob/4e494929998d6bc4fccf75e0a233f727db4b70ee/packages/agent/src/agent-loop.ts#L380-L800) emits `tool_execution_start`, incremental updates, and `tool_execution_end`.
- The [SDK documentation](https://pi.dev/docs/latest/sdk) exposes `createAgentSession`, `prompt`, `steer`, `followUp`, `abort`, `compact`, `waitForIdle`, and subscriptions. [`sdk.md:180-234`](https://github.com/earendil-works/pi-mono/blob/4e494929998d6bc4fccf75e0a233f727db4b70ee/packages/coding-agent/docs/sdk.md#L180-L234) also has a useful preflight callback that distinguishes acceptance/queueing from pre-acceptance rejection.
- [`sdk.md:262-322`](https://github.com/earendil-works/pi-mono/blob/4e494929998d6bc4fccf75e0a233f727db4b70ee/packages/coding-agent/docs/sdk.md#L262-L322) documents message, tool, agent, turn, queue, compaction, and retry events. The same stream is available as JSONL in [JSON mode](https://pi.dev/docs/latest/json).
- [`session-manager.ts:845-855`](https://github.com/earendil-works/pi-mono/blob/4e494929998d6bc4fccf75e0a233f727db4b70ee/packages/coding-agent/src/core/session-manager.ts#L845-L855) stores sessions as append-only JSONL trees. [Compaction](https://pi.dev/docs/latest/compaction) summarizes older turns while preserving recent context and appends a compaction entry rather than rewriting history.
- The language/runtime fit is good: Florence is Node/TypeScript, and Pi publishes `@earendil-works/pi-agent-core` and `@earendil-works/pi-coding-agent`.

### What Pi explicitly does not provide

Pi deliberately leaves workflow policy out of the core. [`usage.md:305-310`](https://github.com/earendil-works/pi-mono/blob/4e494929998d6bc4fccf75e0a233f727db4b70ee/packages/coding-agent/docs/usage.md#L305-L310) says there is no built-in MCP, subagent system, permission popup, plan mode, to-do system, or background bash. Web/browser behavior comes from extensions or custom tools.

Pi also has no built-in sandbox. Its [security documentation](https://pi.dev/docs/latest/security) says project trust controls resource loading only; tools and extensions run with the process user's permissions, and unattended/untrusted work belongs in an OS/container/VM boundary.

Most importantly, the stable session store is a conversation/context log, not a task/effect ledger. It does not supply:

- durable acceptance and leasing for a long-running product task;
- retry/idempotency keys for Google Calendar, messaging, booking, or other providers;
- ownership/audience/visibility rules for two adults and a family group;
- revalidation of the live Linq group before a result crosses from private evidence into household-visible output;
- an `unknown` effect-reconciliation workflow after process loss.

There is a newer `AgentHarness` interface in the repository that appears to promise durable lanes, resume, queues, and replay controls. It is not a usable basis today: [`agent-harness.ts:347-420`](https://github.com/earendil-works/pi-mono/blob/4e494929998d6bc4fccf75e0a233f727db4b70ee/packages/agent/src/harness/agent-harness.ts#L347-L420) rejects restore and returns `HarnessNotImplemented` from `prompt`, `resume`, queueing, `waitForIdle`, and `runToCompletion`. It should be treated as an in-progress design, not a shipped durable runtime.

### Realistic Pi integration

**Possible later: embed the core loop.** Florence could wrap its existing read tools and structured final-decision validator in `@earendil-works/pi-agent-core`, then translate Pi events into typing/reaction/progress UI. This would buy a cleaner event contract, steering, follow-ups, and compaction.

**Not recommended now:** replacing `OpenAiFlorenceReasoner`. Florence already has a short tool loop, OpenAI Responses web search, encrypted reasoning continuation, a strict Zod final response, source validation, and audience-specific read capabilities. Rebuilding those controls around Pi is meaningful migration work while leaving the current search/task gap untouched.

**Do not use Pi session JSONL as product truth.** If Pi is adopted later, its session is disposable reasoning context. PostgreSQL continues to own messages, authority, facts, due work, idempotency, and result delivery.

Migration risk: **medium for an isolated inner-loop experiment; high for a wholesale runtime replacement.** The TypeScript fit lowers integration cost, but replay semantics, tool/result validation, OpenAI-specific web-source verification, and Florence's transactional commit boundary would all need adapters and regression coverage.

## Hermes Agent assessment

### What Hermes provides

Hermes is much closer to a complete agent product than a library:

- Its official [agent-loop documentation](https://hermes-agent.nousresearch.com/docs/developer-guide/agent-loop) describes a multi-provider loop with tool execution, interrupts, retries, iteration budgets, compaction, persistence, delegation, and progress callbacks. The source implementation is [`agent/conversation_loop.py::run_conversation`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/agent/conversation_loop.py#L1823-L2030).
- Hermes persists the assistant tool-call turn before side effects; if that append fails, it does not execute the tools ([`conversation_loop.py:7405-7439`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/agent/conversation_loop.py#L7405-L7439)). Tool progress/result rows are likewise flushed before UI projection ([`agent/tool_executor.py:211-239`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/agent/tool_executor.py#L211-L239)).
- Its [session storage](https://hermes-agent.nousresearch.com/docs/developer-guide/session-storage) uses SQLite for full history, metadata, full-text search, routing, compression lineage, and async delegation bookkeeping.
- [`tools/async_delegation.py:1-34`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/async_delegation.py#L1-L34) supports `delegate_task(background=true)`: it immediately returns a handle, executes a child on a daemon worker, and re-enters completion as a fresh turn rather than corrupting the current tool sequence.
- [`tools/async_delegation.py:145-175`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/async_delegation.py#L145-L175) and [`:250-397`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/async_delegation.py#L250-L397) persist dispatch, routing, terminal result, delivery state/attempts, and recovery metadata in an `async_delegations` ledger. [`:770-817`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/async_delegation.py#L770-L817) documents immediate dispatch handles, progress tokens, stale detection, and bounded concurrency.
- It has built-in multi-provider [`web_search_tool`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/web_tools.py#L838-L920), page extraction, and an [accessibility-tree browser](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/browser_tool.py#L1-L25) with local/cloud backends and task-isolated sessions.
- Its [MCP client](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/mcp_tool.py#L1-L84) supports stdio, Streamable HTTP, and SSE, plus discovery, reconnection, timeouts, environment filtering, and sampling.
- It can run commands in local, Docker, SSH, Singularity, Modal, Daytona, and Vercel environments. The Docker backend drops capabilities, sets `no-new-privileges`, applies resource/PID limits when available, and can disable network access ([`tools/environments/docker.py:353-382`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/environments/docker.py#L353-L382), [`:883-981`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/environments/docker.py#L883-L981)).
- It has dangerous shell-command approval, hard blocks, allowlists, and optional staged approval for memory/skill writes ([`tools/approval.py`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/approval.py), [`tools/write_approval.py:1-40`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/write_approval.py#L1-L40)).

### Hermes's durability limit

Hermes has substantially better crash accounting than Pi, but it is not exactly-once durable execution:

- A delegated child runs in a daemon thread. If the owning process dies, Hermes cannot resume that in-flight Python execution. [`recover_abandoned_delegations()`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/async_delegation.py#L343-L397) marks the result `unknown`; it durably restores completion delivery, not the lost computation itself.
- For a dangling side-effecting tool call, [`agent/replay_cleanup.py:138-180`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/agent/replay_cleanup.py#L138-L180) tells the model the effect may have happened and must be inspected before retry. That is the correct honest state, but domain-specific inspection and idempotency are still left to the tool/provider.
- Tool repetition guardrails can stop obvious loops; they do not make a Calendar mutation, text, booking, or purchase idempotent.

### What Hermes does not solve for Florence

Hermes's approvals and isolation operate at the agent/session/host level. They do not encode Florence's product boundary:

- exact participant identities and a live two-adult group trust anchor;
- adult-private Gmail/Calendar sources versus household-visible memory and family Calendar;
- minimal-crossing private conclusions and prohibition on copying private provider content into the group;
- equal adult authority plus standing permission over the Florence-created family Calendar;
- the rule that provider content is evidence, never current-command authority;
- provider-observed completion and deterministic idempotency for Calendar and Linq.

A generic Hermes child given both adults' Google data would therefore have more capability than Florence's policy permits unless Florence first constructs a narrowly scoped, provenance-preserving tool interface. MCP does not change that: it standardizes invocation, not who is authorized to see or do what.

### Realistic Hermes integration

**Recommended: borrow patterns, not the runtime.** The async-delegation design has the right product vocabulary for Florence: accepted handle, bounded concurrency, progress/heartbeat, persisted terminal outcome, durable completion delivery, and `unknown` after ambiguous process loss. Implement those concepts in Florence's PostgreSQL store and existing loop.

**Possible later: a narrow capability sidecar.** If a future task genuinely needs browser interaction or a specialized Hermes tool, Florence could call a stateless internal sidecar with an already-sanitized task and no household credentials, then validate the returned evidence. This is safer than making Hermes the household/session authority. MCP is another possible adapter only when the concrete provider warrants it.

**Not recommended: embed or replace Florence with Hermes.** Hermes is Python and includes its own messaging gateway, session DB, memory, cron, goals/todos, plugins, credential handling, browser, terminal, and approval system. Adopting it would create a second runtime and source of truth, a cross-process protocol, duplicated retention/session semantics, and a much larger dependency and credential surface. Preserving `commitTurn()` and Florence's live authority checks across that boundary would be harder than implementing the small missing work lifecycle directly.

Migration risk: **very high for adoption; medium-to-high for a tightly sandboxed sidecar.** The main risks are Python/Node operational complexity, duplicated SQLite/PostgreSQL truth, session/routing mismatch, unscoped credentials, prompt-injection surface from browser/MCP/tool descriptions, and accidental bypass of Florence's source/audience validator.

## What neither runtime solves

These are Florence application invariants, not generic agent-runtime features:

1. **Household privacy and authority.** A task needs immutable `household_id`, requesting adult, source channel/audience, permitted source visibility, allowed tools, and intended result audience. Before delivery or a consequential action, Florence must revalidate the current live channel/participant authority.
2. **Provider idempotency.** Every mutating provider operation needs a deterministic operation key, recorded intent, observed provider revision/state, and reconciliation. A generic “retry tool call” is unsafe.
3. **Durable product facts versus lossy context.** Pi/Hermes compaction is appropriate for model context. It cannot replace source-linked household facts, monitors, event provenance, or the Vault.
4. **Minimal crossing.** A private task may use one adult's Gmail/Calendar, but any group result must be an explicitly allowed household-safe conclusion. Generic session isolation does not derive that conclusion safely.
5. **Product judgment.** The runtime cannot decide which family requests should be foreground, background, suggested, or prohibited. Florence still needs capability and consequence policy.

## Recommended implementation for the flight/general-work gap

### Phase 1: make ordinary public lookup work in the current turn

1. Expose a zero-argument public-research function on every ordinary non-reaction turn. When selected, run OpenAI `web_search` in a separate context containing only the sanitized current authored request—not the household prompt, memories, messages, or Google context. Do not create a flight-number phrase matcher; let the model decide whether research is useful, then require and validate observable search use inside that isolated call.
2. Add an outcome invariant for lookup-like requests: either at least one appropriate tool completed, the reply asks one genuinely blocking question, or the reply reports a concrete tool/provider failure. Reject a decision whose only output promises later work.
3. Resolve identifiers before asking for recoverable details. A flight designator can usually supply carrier, route, schedule, and status; a confirmation code or private passenger detail should not be sent to public search.
4. Keep a bounded fast path—roughly seconds, not minutes—in the same tool loop. For the example: acknowledge naturally, search DL 747 and its route/status, search same-night alternatives under the user's stated constraints, then answer with the best options and sources.
5. UI behavior should reflect real lifecycle edges. A quick reaction/ack can happen when Florence accepts the request; if work remains after about 5–10 seconds, send one useful progress message. Do not imply work began until the tool call or durable task acceptance actually occurred.

OpenAI's native search already fits Florence's source-verification path and is a lower-risk first step than granting a generic agent browser or shell access.

### Phase 2: one generic, durable work record for genuinely long tasks

Extend the existing PostgreSQL due-work seam with a small task record rather than adding Pi/Hermes state:

```text
work_id
household_id
request_source_id
requesting_adult_id
request_audience              private | household
result_audience               private | household
visibility_scope              adult_private:<id> | household
objective
capabilities                  e.g. public_web, owner_gmail_read, owner_calendar_read
status                        accepted | running | succeeded | failed | unknown | cancelled
idempotency_key
attempt, lease_owner, lease_expires_at, heartbeat_at
progress_summary, result, error
accepted_at, started_at, finished_at
```

Required semantics:

- Commit `accepted` and the immediate visible acknowledgement in the same PostgreSQL transaction as handling the inbound message.
- Claim work with a bounded lease and concurrency; do not run long work under the single global `#activeRun` critical path.
- Persist progress and a completion/delivery outbox. Completion returns as a fresh Florence turn, following Hermes's clean message-ordering pattern.
- On process loss, reads may retry. Mutations become `unknown` until provider reconciliation proves their outcome; never blindly replay.
- Recheck live authority and derive the minimum allowed conclusion at result delivery time. A private task cannot silently become a group result.
- Do not place raw Google credentials or unrestricted household context in an agent subprocess. Give each run capability-bound application tools.
- Keep most requests foreground. Background work is for a multi-source comparison, long document review, monitoring, or other task that cannot meet the interactive latency budget—not for a single flight lookup.

This remains one durable Florence work seam and one source of truth, consistent with `AGENTS.md` and `PLAN.md`; it is not a feature-specific workflow engine.

### Phase 3: evaluate runtime libraries only after the product contract is proven

Measure the built-in implementation first: acceptance latency, tool-start latency, result latency, stalled/unknown count, duplicate effects, and privacy/audience violations. If the inner loop itself becomes the limiting maintenance burden, prototype Pi behind the existing reasoner interface. If interactive websites become a recurring validated need, add a separate browser worker with allowlisted egress, ephemeral isolated sessions, no ambient household credentials, and Florence-owned approval/result validation.

Hermes's source is a useful design reference for that worker. It is not the control plane.

## Decision

- **Pi:** no adoption now; possible later inner-loop experiment. Borrow its event vocabulary and acceptance/queue distinction.
- **Hermes:** no wholesale adoption. Borrow its durable completion and `unknown`-effect patterns; consider only a narrowly scoped sandboxed capability sidecar when a real browser/terminal use case exists.
- **Florence:** fix public-search availability and promise validation immediately, then add a small PostgreSQL-owned long-work lifecycle. Preserve `commitTurn()`, live authority checks, source visibility, provider idempotency, and provider reconciliation as non-negotiable boundaries.

## Primary sources

### Pi

- [Pi documentation](https://pi.dev/docs/latest)
- [Pi SDK](https://pi.dev/docs/latest/sdk)
- [Pi security model](https://pi.dev/docs/latest/security)
- [Pi compaction](https://pi.dev/docs/latest/compaction)
- [Official Pi source snapshot](https://github.com/earendil-works/pi-mono/tree/4e494929998d6bc4fccf75e0a233f727db4b70ee)

### Hermes Agent

- [Hermes Agent documentation](https://hermes-agent.nousresearch.com/docs/)
- [Agent loop internals](https://hermes-agent.nousresearch.com/docs/developer-guide/agent-loop)
- [Session storage](https://hermes-agent.nousresearch.com/docs/developer-guide/session-storage)
- [Subagent delegation](https://hermes-agent.nousresearch.com/docs/user-guide/features/delegation)
- [Official Hermes Agent source snapshot](https://github.com/NousResearch/hermes-agent/tree/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882)
