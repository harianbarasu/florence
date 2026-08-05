# Deep Agents and Mastra vs. the Life OS architecture

Research date: 2026-08-04  
Source policy: primary sources only (official documentation, repositories, and vendor engineering posts). This is a point-in-time review; both projects are moving quickly.

## Decision

Do not adopt either framework as the Life OS substrate.

| Candidate | Decision | Why |
|---|---|---|
| LangChain Deep Agents | **Adapt its harness patterns now; treat it as the leading future knowledge-worker runtime spike, not a v1 dependency.** Run that bounded TypeScript spike only if the official Codex baseline exposes a measured harness gap or Deep Agents gains an official ChatGPT-authenticated Codex model path. | It is the closer architectural fit: an opinionated inner agent loop with context offloading, virtual files, skills, and isolated subagents. Its native model path still expects a provider API key or local model, and its production persistence/deployment stack would duplicate Life OS orchestration. |
| Mastra | **Defer it in v1; retain a future optional Agent Controller/runtime spike for interactive, stateful agent UX.** That spike must sit behind `AgentRuntime` and must not adopt Mastra memory, schedules, workers, or storage as canonical state. A standalone eval/observability package is also worth reconsidering after the alpha if the home-grown eval layer proves expensive. | It is an excellent TypeScript fit but now spans the same product/control-plane territory Life OS deliberately owns. Its durable-agent and worker surfaces are beta and have recovery limitations that `pg-boss` already solves more directly. Its subscription-backed Mastra Code route crosses the plan's “official Codex surfaces only” boundary. |
| Official Codex app-server / SDK | **Continue the Phase 0 gate in `PLAN.md`.** | It is the only reviewed path that satisfies the hard requirement for official ChatGPT subscription authentication without a second agent control plane or undocumented token/endpoint handling. |

This reinforces, rather than changes, the core boundary in [`PLAN.md`](./PLAN.md): Life OS is the durable control plane; an agent runtime supplies disposable cognition.

## The layer each product actually solves

### Deep Agents: an inner agent harness, with an optional production runtime around it

Deep Agents describes itself as an **agent harness** built on LangChain and LangGraph. The open-source library supplies a tool-calling loop plus virtual filesystem tools, context offloading and summarization, skills, memory files, subagents, permissions, streaming, and human interrupts. Its default synchronous subagents are fresh, stateless contexts that return a single final result to the parent. That is almost exactly the *inside of one Life OS run*, not the Life OS system of record. [Deep Agents overview](https://docs.langchain.com/oss/javascript/deepagents/overview) · [Subagents](https://docs.langchain.com/oss/javascript/deepagents/subagents)

The production story is a separate, larger layer. `createDeepAgent` returns a LangGraph graph; checkpointers can preserve thread state, and a `StoreBackend` can preserve files across threads. Full hosted/self-hosted operation through LangSmith Agent Server adds assistants, threads, runs, cron jobs, PostgreSQL-backed checkpoints and long-term memory, a durable task queue, and Redis for signaling/cancellation/streaming. Those are useful capabilities, but they overlap Life OS `runs`, `run_attempts`, `run_events`, schedules, Postgres state, and `pg-boss` delivery. [Deep Agents backends](https://docs.langchain.com/oss/javascript/deepagents/backends) · [LangGraph persistence](https://docs.langchain.com/oss/javascript/langgraph/persistence) · [Agent Server architecture](https://docs.langchain.com/langsmith/agent-server)

LangChain's Stripe case study confirms the intended layering. Stripe used Deep Agents as the base LLM/harness layer, then built a Stripe-owned harness for security, infrastructure, and internal services, followed by a configuration layer and product UI. Stripe also built its own S3-backed virtual filesystem and sandbox sync boundary. That is evidence for keeping Life OS policy and canonical state outside the library, not for replacing them with it. [How Stripe built Kai on Deep Agents](https://www.langchain.com/blog/how-stripe-built-their-knowledge-ai-platform-on-deep-agents)

### Mastra: a TypeScript agent application platform and control layer

Mastra is broader. Its official repository presents one TypeScript framework for agents, graph workflows, human-in-the-loop, memory/context, MCP, evals, observability, servers, and deployment. Current storage domains cover memory, workflow snapshots, traces/logs/metrics, scores/datasets/experiments, background tasks, schedules, and durable thread/goal state. [Mastra repository](https://github.com/mastra-ai/mastra) · [Storage overview](https://mastra.ai/docs/storage/overview)

Its newer `AgentController` (formerly `Harness`) explicitly owns sessions, modes, threads, persisted state, permission prompts, subagents, model switching, steering, and UI events. In other words, it is a reusable product-level agent control layer, not merely a model adapter. That directly overlaps Life OS conversation, work management, orchestration, context/runtime lifecycle, approvals, and live run projection. [Agent Controller overview](https://mastra.ai/docs/agent-controller/overview) · [Rename and architecture explanation](https://mastra.ai/blog/build-claude-code-for-x-with-agentcontroller)

## Capability comparison

| Concern | Deep Agents | Mastra | Life OS consequence |
|---|---|---|---|
| Durable workflow | LangGraph checkpoints make a thread resumable; HITL requires a checkpointer. LangSmith deployment adds the queue/runtime. | Workflows persist snapshots for suspend/resume. Durable agents wrap the agent loop in a workflow; background tasks, schedules, workers, PubSub, and storage form a wider runtime. | Neither should become canonical task/run truth. Keep `pg-boss` delivery plus Life OS tables; runtime checkpoints may be attempt-local implementation detail only. |
| Crash behavior | Depends on the selected checkpointer/deployment. Agent Server is a separate queue and persistence system. | Durable agents are beta. A crashed run can remain `running`; recovery re-drives from the last snapshot, may repeat LLM/tool calls, and has no distributed lease/lock. Workers are beta, have no DLQ, require a single scheduler, and can leave runs stuck after an API crash. | `pg-boss` leases, retries, dead letters, and explicit idempotent action reconciliation remain the better fit for the single-host plan. [Durable agents](https://mastra.ai/docs/long-running-agents/durable-agents) · [Workers](https://mastra.ai/docs/deployment/workers) |
| Workspace/artifacts | Pluggable state, filesystem, durable store, composite, custom, and sandbox backends; built-in context offloading. | Persistent workspaces provide file operations, shell/sandboxes, search, LSP, skills, per-request resolution, approval hooks, and read-before-write checks. | Both validate the planned workspace materializer. Do not let either filesystem become canonical structured life data or bypass content-addressed artifacts. [Deep Agents backends](https://docs.langchain.com/oss/javascript/deepagents/backends) · [Mastra workspaces](https://mastra.ai/docs/workspace/overview) |
| Context and memory | Summarization, large-result offloading, always-loaded `AGENTS.md`, cross-thread store, and progressive skill loading. | Message history, working memory, semantic recall, and background-agent “observational memory”; thread/resource scopes determine sharing. | Borrow offloading and progressive disclosure. Do not adopt autonomous cross-thread personal memory: it conflicts with sourced claims, correction state, retention, disclosure manifests, and the separation of raw source/user statement/inference. [Deep Agents context engineering](https://docs.langchain.com/oss/javascript/deepagents/context-engineering) · [Mastra memory](https://mastra.ai/docs/memory/overview) |
| Subagents | Stable synchronous subagents provide context quarantine. Async subagents add stateful remote tasks, steering, and cancellation, but are preview and require an Agent Protocol server. | Supervisor agents delegate with message filtering, isolated memory, propagated approvals/cancellation, background dispatch, and completion scorers. Agent Controller adds another subagent lifecycle. | The synchronous Deep Agents shape is a good reference for Phase 1's two bounded specialists. Life OS should allocate workers, budgets, capabilities, and verification itself. Defer Deep Agents async subagents and Mastra background subagents because they create a second scheduler/run identity. [Async subagents](https://docs.langchain.com/oss/javascript/deepagents/async-subagents) · [Mastra supervisor agents](https://mastra.ai/docs/agents/supervisor-agents) |
| Skills | Agent Skills directories, progressive disclosure, optional scripts/assets; traced with LangSmith. | Agent- or workspace-level Agent Skills, dynamic per-request resolution, search, and versioned/content-addressed sources. | Strong conceptual alignment. Life OS still owns versions, provenance, evaluation, promotion, rollback, and immutable policy boundaries. [Deep Agents skills](https://docs.langchain.com/oss/javascript/deepagents/skills) · [Mastra skills](https://mastra.ai/docs/workspace/skills) |
| Evals/observability | LangSmith supplies traces, datasets, offline/online evaluators, monitoring, and hosted/self-hosted deployment. The Deep Agents model eval suite tests harness basics but says those results are not sufficient for long tasks. | Native hierarchical traces, logs, metrics, feedback, OpenTelemetry/exporters, scorers, datasets, experiments, and CI gates. | Mastra is stronger as an integrated local TypeScript toolkit; Deep Agents is strongest with LangSmith. Either telemetry path must redact sensitive context and feed Life OS-owned `eval_cases`, `eval_runs`, and promotion decisions. [LangSmith evaluation](https://docs.langchain.com/langsmith/evaluation) · [Mastra observability](https://mastra.ai/docs/observability/overview) · [Mastra evals](https://mastra.ai/docs/evals/overview) |
| Language fit | First-class, actively maintained TypeScript and Python implementations exist; the JS library is LangGraph-native. | TypeScript-first and fits the Node/Fastify monorepo directly. | Python is no longer a reason to reject Deep Agents. Mastra has the cleaner language fit, but architectural ownership outweighs that advantage. [Deep Agents JS repository](https://github.com/langchain-ai/deepagentsjs) · [Deep Agents Python repository](https://github.com/langchain-ai/deepagents) |
| Security/policy | Filesystem permissions and HITL exist, but the repository explicitly says it follows a “trust the LLM” model and boundaries must be enforced at tools/sandboxes. | Tool approvals, processors/guardrails, auth, workspaces, and enterprise authorization exist. | Neither replaces Life OS's typed `DomainCommand`, deterministic policy, approval, idempotent executor, reconciliation, or immutable audit perimeter. [Deep Agents security statement](https://github.com/langchain-ai/deepagentsjs#security) · [Mastra tool approval](https://mastra.ai/docs/agents/agent-approval) |

## Model providers and the ChatGPT/Codex subscription boundary

Native model use is straightforward but does **not** satisfy the plan's subscription requirement:

- Deep Agents accepts any LangChain tool-calling chat model. Its quickstart requires a provider API key, and the OpenAI integration uses `OPENAI_API_KEY`. [Deep Agents quickstart](https://docs.langchain.com/oss/javascript/deepagents/quickstart) · [LangChain OpenAI integration](https://docs.langchain.com/oss/javascript/integrations/providers/openai)
- Mastra's model router likewise authenticates OpenAI with `OPENAI_API_KEY`. Its OpenAI Agents SDK wrapper also explicitly requires that key. [Mastra OpenAI provider](https://mastra.ai/models/providers/openai) · [Mastra SDK agents](https://mastra.ai/docs/agents/sdk-agents)

There is a narrower official bridge: OpenAI documents `codex mcp-server`, and both frameworks can launch a local stdio MCP server. Combined with OpenAI's documentation that Codex CLI supports ChatGPT login and reuses its cached session, this implies that either framework can *delegate a tool call* to a locally authenticated Codex process. It does not make Codex the framework's native chat model; a Deep Agents or Mastra supervisor still needs its own API-backed or local model. This is an inference across the official interfaces, not a vendor-documented end-to-end integration. [OpenAI Codex MCP server](https://learn.chatgpt.com/docs/mcp-server) · [OpenAI Codex authentication](https://learn.chatgpt.com/docs/auth) · [LangChain MCP client](https://docs.langchain.com/oss/javascript/langchain/mcp) · [Mastra MCP client](https://mastra.ai/docs/mcp/overview)

Mastra's ACP documentation names Codex as an example, but OpenAI's Codex documentation does not expose ACP as a supported Codex surface. Treating a community `codex-acp` bridge as equivalent to official app-server/SDK support would weaken D-004/D-005. [Mastra ACP](https://mastra.ai/docs/agents/acp)

Most importantly, Mastra Code's own “OpenAI Codex” provider is not acceptable for Life OS. The repository implementation performs its own OAuth/token handling and targets the undocumented `https://chatgpt.com/backend-api/codex/responses` route rather than invoking an official Codex integration surface. It may work for Mastra Code, but it conflicts directly with the plan's ban on token extraction, undocumented endpoints, and silent auth coupling. [Mastra Code provider source](https://github.com/mastra-ai/mastra/blob/main/mastracode/sdk/src/providers/openai-codex.ts)

OpenAI explicitly documents ChatGPT subscription login for Codex CLI/local surfaces and app-server for deep product integration. Therefore the current direct local app-server-over-stdio gate remains the cleanest supported boundary. [Codex authentication](https://learn.chatgpt.com/docs/auth) · [Codex app-server](https://learn.chatgpt.com/docs/app-server) · [Codex SDK](https://learn.chatgpt.com/docs/codex-sdk)

## Concrete adoption boundaries

### Adopt as design patterns

- Ephemeral specialist contexts that return compact results.
- Virtual workspaces as context/artifact interfaces, with canonical records elsewhere.
- Progressive skill disclosure and versioned skill assets.
- Large-tool-output offloading, summarization, and typed event streams.
- Independent trajectory evals, completion checks, and trace-linked user feedback.

### Do not adopt in v1

- LangGraph/LangSmith Agent Server, Deep Agents async subagents, or its cross-thread store as the product runtime.
- Mastra Agent Controller, memory, goals, schedules, background-task manager, durable agents, workflow workers, PubSub, or server/auth layer.
- Any framework approval primitive as authorization for an external action.
- Mastra Code's ChatGPT OAuth/provider implementation or a community ACP bridge as the Codex runtime.

### Revisit triggers

1. The official Codex Phase 0 adapter fails a specific context-management or subagent benchmark that Deep Agents demonstrably passes.
2. OpenAI or a framework ships a documented, official ChatGPT-authenticated model/runtime adapter—without copied tokens or private endpoints.
3. `pg-boss` and Life OS run tables fail measured multi-host, long-suspension, or event-stream requirements.
4. The alpha's eval/trace implementation costs more to maintain than a privacy-preserving, local-only Mastra eval/observability integration.

If revisited, compare Deep Agents as the specialist knowledge-work harness and Mastra Agent Controller as the interactive session/steering harness against the same `AgentRuntime` contract and scenario evals. In both cases, Life OS retains canonical conversations, tasks, runs, permissions, approvals, actions, artifacts, and audit.

Until one of those triggers occurs, adding either framework would increase state ownership, recovery, security, and authentication complexity without removing a Life OS responsibility.
