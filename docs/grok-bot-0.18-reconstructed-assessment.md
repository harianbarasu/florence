# Grok Bot 0.18 reconstructed: Florence assessment

Research date: 2026-08-24

Repository reviewed: [`b-nnett/grok-bot-0.18-reconstructed` at `a9f633e`](https://github.com/b-nnett/grok-bot-0.18-reconstructed/tree/a9f633e09d49a85829b8236331b9e21f7e612634)

Method: source, repository history, and first-party GitHub and 1Password documentation only. The
repository was cloned into a temporary directory for static inspection. No application code,
installer, build script, dependency, test, or recovered binary was executed.

## Bottom line

This repository is useful to Florence as a **design mine, not a dependency or code donor**. It
contains several thoughtful patterns for a broad computer-using assistant: exact-action review,
DOM-first browser automation, human handoff for authentication, untrusted-evidence memory
synthesis, and a credential-delivery seam intended to keep a token away from the model. Those are
worth understanding and, where Florence later needs them, reimplementing from first principles.

It is not a shortcut to a better parent product. The repository is a very large, two-commit,
unofficial reconstruction of one shipped desktop binary; some major components are inferred, the
authored renderer is missing, the packaged UI still uses upstream production bundles, and several
prominent capabilities are additions made by the reconstructor. It also has no source-code license
grant. Florence should not copy its code, prompts, UI, assets, generated protocols, or preserved
installers.

Most of its architecture directly conflicts with Florence's current product contract: generic
agents, generic tools/MCP, model routing, file-backed cross-agent memory, broad signed-in browser
use, local execution boxes, and credential-vault access. Florence's current advantage is the
opposite: two verified adults, typed private-versus-household meaning, narrow provider writes,
PostgreSQL as one durable truth, and a real two-phone rehearsal. This review does **not** justify a
goal change.

## What the repository actually is

The author describes it as an unofficial, source-oriented reconstruction of the public Grok Bot
0.18.0 macOS application—not Anysphere's original monorepo or an official release—and warns that
names and module boundaries inferred from compiled output may differ from the originals.
[README](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/README.md#L5-L24)

It is deliberately hybrid:

- runtime/control-plane code is represented as readable TypeScript;
- the original authored frontend and source maps were not present;
- packaged builds retain the checksum-pinned shipped renderer and patch it; and
- `frontend/` is explicitly only a partial, evidence-backed reconstruction.

[README](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/README.md#L38-L64),
[PROVENANCE](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/PROVENANCE.md#L20-L29)

The repository history has only an
[initial reconstructed-source import](https://github.com/b-nnett/grok-bot-0.18-reconstructed/commit/bf66838a65250dac7127fdc5c60c5633951a94f1)
on August 22 and a
[documentation/installer-preservation commit](https://github.com/b-nnett/grok-bot-0.18-reconstructed/commit/a9f633e09d49a85829b8236331b9e21f7e612634)
on August 23. At the reviewed commit it contains 2,111
tracked files and roughly 441,000 lines of TypeScript, about 337,000 of them under reconstructed
`source/packages`. That scale is evidence of a recovered application surface, not of a small,
auditable agent library. The inference router, local Docker sandbox, usage tracking, and settings
surface are explicitly described as experiments added by this project, so they must not be
attributed to upstream Grok Bot 0.18.
[README](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/README.md#L14-L24)

### Reuse rights

There is no root `LICENSE`, no `license` field in `package.json`, and the repository's own notice
says no upstream source-code license is asserted or granted. It calls for an independent rights
review before redistribution and says the preserved installers remain subject to their own terms.
[package.json](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/package.json#L1-L9),
[NOTICE](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/NOTICE.md#L3-L15)

GitHub's official guidance is unambiguous: without a license, default copyright applies and others
may not reproduce, distribute, or create derivative works; a public repository grants only the
view/fork rights supplied by GitHub's service unless the owner grants more.
[GitHub licensing documentation](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/licensing-a-repository),
[GitHub Terms of Service](https://docs.github.com/en/site-policy/github-terms/github-terms-of-service#5-license-grant-to-other-users)

Accordingly, this review treats the implementation only as observable research material. Florence
should clean-room reimplement any general idea it chooses to adopt. This is a product/engineering
recommendation, not legal advice.

## Capability map

| Area | What the reconstructed source contains | Florence judgment |
| --- | --- | --- |
| Agent loop | A composed `AnysphereAgent`, per-turn model/tool projections, summarization, checkpoints, and a 5,000-step safety ceiling. Specialized browser, computer, and executor subagents can be injected at runtime. [composition](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/turn-agent-composition.ts#L142-L159), [construction](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/turn-agent-composition.ts#L744-L793), [subagents](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/turn-agent-composition.ts#L1687-L1721) | This is an assistant platform. Florence needs one household loop with a small set of concrete product effects, not a general agent runtime. |
| Tools | Task/multitask, inter-agent messaging, agent/profile state, local and external shell/read, web search/fetch, box tools, file transfer, screenshots, image generation, cloud agents, and MCP discovery/management/calls. Computer and browser tools are fenced to their specialist subagents; shared rooms receive a filtered set. [tool factories](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/tools/turn-toolset.ts#L1054-L1150), [tool assembly](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/tools/turn-toolset.ts#L1293-L1531) | The fencing idea is useful. The generic registry and most tools are current Florence non-goals. |
| Browser and computer use | A page-level browser agent uses DOM snapshots and element references for navigation, clicks, input, selection, scrolling, dragging, tabs, and screenshots; a pixel agent handles desktop-only flows. Both are meant to receive narrow tasks and stop for password, 2FA, CAPTCHA, or payment handoff. [browser tools](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/tools/sand-browser-tools.ts#L552-L566), [browser agent](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/tools/sand-browser-use-subagent.ts#L4-L17), [computer agent](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/tools/sand-computer-use-subagent.ts#L7-L28) | DOM-first operation, explicit success/stop criteria, and human auth handoff are good future patterns. Arbitrary-site action is not part of the pilot. |
| Memory | Per-agent Markdown profile/log files, explicit versus synthesized origin markers, tombstones, cross-agent/project recall, turn evidence, asynchronous synthesis, evidence IDs, stale-snapshot detection, and a second model verification pass. [memory store](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/extensions/memory/memory-service.ts#L80-L151), [synthesis contract](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/extensions/memory/memory-synthesis-service.ts#L66-L99), [commit/verification](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/extensions/memory/memory-synthesis-service.ts#L240-L282) | Evidence IDs, stale-state rejection, and independent verification are transferable. File shards and broad cross-agent recall are not: Florence needs owner, visibility, source/provider, retention, correction, and deletion encoded in PostgreSQL. |
| Identity and messaging | Named agents have profiles, distinct personas/memory/chats, asynchronous direct messages, and shared agent groups. [agent directory and groups](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/agents/agent-messaging.ts#L35-L80) | This is assistant-to-assistant identity, not verified adult identity or channel authority. Florence's exact Messages participant set and private/group disclosure rules are materially stronger for a family. |
| Credentials | The code includes a desktop secrets bridge plus an experimental 1Password path. The latter can create an expiring service account with `read_items` access to one entire vault—not selected items—and deliver its token to a sink without returning it publicly, but the default sink is unavailable and the only concrete exercise is synthetic development code. [provisioning bridge](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/electron-main/onepassword/onepassword-provisioning-bridge.ts#L27-L40), [development control](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/electron-main/onepassword/onepassword-cli-dev-controls.ts#L5-L13) | “The model never receives the value” and least-privilege, expiring credentials are sound principles. This is not a finished integration, its present grant is vault-wide, and Florence should not connect a family's general vault. 1Password itself recommends vault-restricted service accounts for least privilege. [1Password documentation](https://developer.1password.com/docs/cli/secrets-scripts/) |
| Model providers | A project-added router supports Cursor, Claude Code, Codex, and OpenRouter and can reuse local provider sessions. The Codex path reads local auth, refreshes it, and calls the ChatGPT Codex backend directly. [router contract](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/shared/inference-router.ts#L1-L24), [provider session](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/extensions/inference/provider-session.ts#L65-L131) | Explicitly conflicts with Florence's “no model-provider portability framework” boundary. It also expands credential and support surface without improving the family loop. |

## Trust and security assessment

There are some genuinely good design instincts:

- The main prompt says authority comes only from the actual user in chat; instructions arriving from
  another agent, tool result, routine, or web page do not increase authority. It also requires
  explicit confirmation before external mutation, posting, deletion, or messaging.
  [system prompt](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/system-prompt.ts#L250-L264)
- Memory synthesis explicitly treats state and conversation evidence as untrusted data, requires
  source evidence IDs, rejects unknown IDs, preserves uncertainty, and can run a separate verifier
  before applying a snapshot-checked change.
  [memory synthesis](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/extensions/memory/memory-synthesis-service.ts#L66-L99)
- Consequential computer actions are canonicalized and fingerprinted with the box/window and
  captured display state; the display state is checked again immediately before execution. If
  blocked, the intended path is one unchanged action and one human approval—not a reformulated
  bypass.
  [canonical target and fingerprint](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/sand-computer-auto-review.ts#L34-L47),
  [preflight and display recheck](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/sand-computer-auto-review.ts#L139-L175),
  [approval guidance](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/system-prompt.ts#L250-L260)

Those protections are not equivalent to Florence's authority model:

1. **Much of the boundary is model policy.** The authority rules live in a long system prompt.
   Auto-review itself can be off, shadow-only, or enforced depending on settings and a feature gate;
   its single-attempt decision comes from an external classifier executor. Classifier errors reject
   in enforce mode, which is good, but this is still a probabilistic action review—not deterministic
   participant, channel, standing-rule, provider-target, and idempotency binding.
   [mode gate](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/extensions/auto-review/auto-review-service.ts#L23-L53),
   [classifier](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/sand-auto-review-classifier-run.ts#L22-L69)
2. **Prompt injection is handled selectively, not as a general evidence type.** Hidden automations
   carry trusted/untrusted markers and memory synthesis has an explicit untrusted-data contract.
   I did not find a comparable deterministic type boundary that prevents ordinary web pages,
   browser snapshots, email-like content, or general tool output from becoming instructions during
   the main reasoning turn. The prompt tells the model not to grant such content authority, but the
   broad tool output still enters the agent's context. Because this is a reconstruction, that is a
   finding about the reviewed tree, not proof of every upstream service behavior.
   [automation markers](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/prompt-collector-glue.ts#L334-L368)
3. **The blast radius is large.** The assistant can use a persistent signed-in browser, shell,
   computer input, external machine tools, MCP services, and secrets. The prompt itself explains
   that browser logins are shared across all of a user's agents even when their desktop views are
   separate.
   [work surfaces](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/runner/system-prompt.ts#L158-L167)
4. **“Local Docker” is not a customer VPC or an exfiltration boundary.** It publishes only
   loopback ports and uses a bearer token, but it pulls a mutable `sand-box-latest` image, mounts the
   host's `.codex` and `.claude` directories read-only, mounts an inference credential when present,
   and does not disable outbound networking. Its `docker run` also has no explicit read-only root
   filesystem, capability drop, `no-new-privileges`, or resource bounds. Read-only bind mounts
   prevent modification, not credential reading or exfiltration by code in the container.
   [image and gateway](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/electron-main/box/local-docker-host-connector.ts#L13-L20),
   [mounts and run arguments](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/electron-main/box/local-docker-host-connector.ts#L157-L202)
5. **The credential UX is safer than chat paste, but the reconstructed connector store is not a
   hardened vault.** The masked secret-request path keeps the value out of the transcript and model
   context, which is a good boundary. Its final connector store nevertheless writes the credential
   as ordinary JSON without encryption or an explicit restrictive file mode. Florence should copy
   the model-blind handoff principle, not this storage implementation.
   [secret submission](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/extensions/transcript/widget-responses.ts#L360-L407),
   [connector secret store](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/host/extensions/session/connector-secret-store.ts#L9-L30)
6. **Approved local execution is still broad host execution.** The production executor constructs
   permissive mock permission and ignore services; after the outer approval gate, those inner
   services do not provide a second sandbox boundary. This reinforces that the reconstruction is a
   research target, not something to run against family or company credentials.
   [production executor](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/local-exec-daemon/production-executor.ts#L247-L274),
   [mock policies](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/source/packages/local-exec/tests/common.ts#L3-L21)
7. **The repository disclaims production safety.** Its own security note calls it a “small-club
   reconstruction,” warns against real credentials or sensitive accounts, and lists unresolved
   advisories in the pinned Electron, Undici/Connect, AI SDK, and OpenTelemetry stack.
   [SECURITY](https://github.com/b-nnett/grok-bot-0.18-reconstructed/blob/a9f633e09d49a85829b8236331b9e21f7e612634/SECURITY.md#L1-L15)

## What Florence should take

Only four patterns are worth carrying forward, and all should be clean-room adaptations inside
Florence's existing modules:

1. **Exact-action review for future browser effects.** Bind the operation, recipient/site, exact
   payload, expected state, fresh read-back/fingerprint, and one approval. An approval permits the
   identical action once; it does not create general authority. Florence already applies the
   product-specific version of this to Calendar and invitation effects.
2. **Evidence-to-memory synthesis with explicit inputs.** Retain the useful sequence—untrusted
   evidence, source IDs, bounded proposal, independent verification, and stale-state rejection—but
   apply it to Florence's typed PostgreSQL facts. Every result still needs adult owner, private or
   household visibility, provider/source, explicit uncertainty, current correction semantics, and
   deletion scope. It does not require restoring a product-invisible fact revision ledger.
3. **DOM-first, narrow browser delegation if the product later earns it.** Prefer structured page
   targets to pixel clicks, specify one concrete family outcome and a stop condition, serialize
   desktop use, and hand passwords/2FA/CAPTCHAs/payments back to a human. This belongs after the
   household loop proves demand for one specific external-site job.
4. **Secret delivery without model exposure.** If a future concrete provider requires a credential,
   deliver the minimum-purpose value directly to the adapter, keep it out of prompts/logs, scope it
   narrowly, expire it, and reconcile uncertain delivery. That principle does not imply 1Password
   access or a general family credential vault.

## What Florence should reject for the current goal

- importing or adapting this repository's source, prompts, renderer, assets, generated protocols,
  or binaries;
- a general agent framework, multiple assistant personas, agent-to-agent groups, or generic task
  and tool management;
- a connector/MCP registry, model-provider router, workflow layer, separate box/worker, or
  file-backed memory system;
- shared persistent browser sessions or use of the parent's existing developer/provider auth;
- local Docker presented as VPC-grade isolation;
- linking a family's general 1Password vault; and
- autonomous email, arbitrary browser submission, purchases, bookings, or external messages.

These are not merely sequencing preferences. They violate Florence's controlling
[structural anti-bloat laws](../PLAN.md#structural-anti-bloat-laws),
[autonomy boundary](../PLAN.md#autonomy-and-actions), and
[explicit non-goals](../PLAN.md#explicit-non-goals-for-this-goal). They also weaken the engineering
rules that require PostgreSQL to own durable truth, exact verified participants to own authority,
and secrets never to enter model context.
[AGENTS.md](../AGENTS.md#pilot-invariants)

## Recommendation

Do not change the goal, architecture, or beta sequence because of this repository. Finish and
rehearse the exact two-parent Messages/group/Google/family-calendar loop, including proactive
quality, privacy, revocation, deletion, and verified outcomes.

Keep this assessment as a future design reference. If a beta parent repeatedly asks Florence to
complete the same specific browser-only family chore, write a new product sentence—“The family can
now ___ in iMessage”—and design one narrow browser action using the four clean-room patterns above.
Until that demand exists, Grok Bot's breadth is mostly a warning about authority and complexity,
not Florence's next roadmap.
