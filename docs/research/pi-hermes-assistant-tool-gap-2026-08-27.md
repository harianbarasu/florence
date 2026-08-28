# Pi and Hermes assistant-tool gap for Florence

Date: 2026-08-27

Scope: current first-party source and documentation only. Pi is evaluated at release [`v0.84.3`](https://github.com/earendil-works/pi/releases/tag/v0.84.3), Hermes Agent at source commit [`6dcebea`](https://github.com/NousResearch/hermes-agent/tree/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882), and Florence at commit [`5e7114d`](https://github.com/harianbarasu/florence/tree/5e7114d0cdb79e713f61c0b6013bc29672ce4a2d). The user's separate permission to reuse Pi/Hermes material is taken as given; this is an architecture and product-capability assessment, not a license analysis.

## Answer

The earlier conclusion that Florence did not need to take anything from Pi or Hermes was too broad.

- **Pi does not contain the personal-assistant tools Florence is missing.** Its shipped tools are filesystem and shell tools for coding. Pi is useful as a source of TypeScript tool-loop contracts: typed tools, cancellation, real progress events, policy hooks, steering/follow-up queues, and dynamic tool activation.
- **Hermes does contain a substantial personal-assistant capability surface.** Florence should adapt several parts of it: a tool registry/toolset model, web extraction, safe browser-worker boundaries, scheduled-task CRUD, a generic work lifecycle, progressive tool disclosure, maps/travel connectors, Google Workspace coverage, document workflows, and a curated MCP connector layer.
- **Florence still should not replace its runtime wholesale with either project.** Pi lacks the assistant tools and durable work engine; Hermes is a Python agent product whose session, credentials, memory, messaging, cron, and execution systems would become a second control plane. Florence's household visibility, adult authority, source provenance, provider reconciliation, and PostgreSQL commit boundary remain the right product kernel.

The right decision is therefore **selective adoption, not no adoption and not wholesale adoption**.

## What Pi actually ships

Pi's built-in tool union is exactly `read`, `bash`, `powershell`, `edit`, `write`, `grep`, `find`, and `ls`; the normal defaults are `read`, `bash`, `edit`, and `write`. It does not ship web search, page extraction, a browser, email, Calendar, reminders, contacts, maps, weather, travel, messaging, reactions, or a personal-memory tool ([tool source](https://github.com/earendil-works/pi/blob/v0.84.3/packages/coding-agent/src/core/tools/index.ts#L95-L105), [SDK defaults](https://github.com/earendil-works/pi/blob/v0.84.3/packages/coding-agent/src/core/sdk.ts#L56-L76)). Pi describes itself as a minimal coding harness and expects product-specific capabilities to come from extensions and packages ([Pi documentation](https://pi.dev/docs/latest), [project philosophy](https://github.com/earendil-works/pi/blob/v0.84.3/README.md#permissions--containerization)).

That makes Pi's shell tool particularly unsuitable as a shortcut for Florence. Pi runs tools with the host user's permissions unless the application supplies an external sandbox; a shell with household Google credentials would collapse Florence's authority and privacy boundaries.

### Pi components worth taking

| Component | What is real today | Use for Florence |
|---|---|---|
| `@earendil-works/pi-agent-core` | General TypeScript agent loop with typed custom tools, streamed progress, lifecycle events, `AbortSignal`, sequential/parallel execution, steering and follow-up queues, `beforeToolCall`, `afterToolCall`, and context transforms ([package](https://github.com/earendil-works/pi/blob/v0.84.3/packages/agent/README.md), [loop](https://github.com/earendil-works/pi/blob/v0.84.3/packages/agent/src/agent-loop.ts#L155-L275), [tool boundary](https://github.com/earendil-works/pi/blob/v0.84.3/packages/agent/src/agent-loop.ts#L600-L790)). | Copy/adapt its tool and event vocabulary now. Consider the package as an inner-loop dependency only after a side-by-side migration test; it does not add assistant capabilities by itself. |
| Dynamic tools | Extensions can register tools, enable subsets at runtime, attach active-only prompt metadata, and lazily load tool definitions ([official extension docs](https://pi.dev/docs/latest/extensions), [example](https://github.com/earendil-works/pi/blob/v0.84.3/packages/coding-agent/examples/extensions/dynamic-tools.ts)). | Useful once Florence has many connectors. Adapt the active-tool registry and additive loading idea rather than placing every schema in every turn. |
| Retry lifecycle | The coding-agent session class classifies transient failures, removes the failed assistant message from retry context, uses abortable exponential backoff, and emits retry lifecycle events ([source](https://github.com/earendil-works/pi/blob/v0.84.3/packages/coding-agent/src/core/agent-session.ts#L2821-L2928)). | Borrow the classification/event pattern, not the TUI/session implementation. |
| Durable harness specification | Pi documents an intent → effect → settlement model, replay-safe versus never-replay tools, lanes, queues, and recovery ([specification](https://github.com/earendil-works/pi/blob/v0.84.3/packages/agent/docs/harness.md)). | Use it as a design checklist for Florence's PostgreSQL work record. |
| Durable harness implementation | The released implementation still returns `HarnessNotImplemented` for core operations including prompt, resume, steering, follow-up, and run-to-completion ([source](https://github.com/earendil-works/pi/blob/v0.84.3/packages/agent/src/harness/agent-harness.ts#L347-L420)). | Do not adopt it as Florence's worker engine. |
| Pi Chat | Adjacent first-party project with channel-scoped workspaces/memory, attachments, history lookup, typing, reply correlation, visible failures, and queued/completed/failed log records ([repository](https://github.com/earendil-works/pi-chat), [runtime](https://github.com/earendil-works/pi-chat/blob/9adbd29b40ee27ff1decf0fc87cbe180b40924f5/src/runtime.ts#L199-L319)). Its actual custom tools are only chat history, attach, secret request, and worker status ([source](https://github.com/earendil-works/pi-chat/blob/9adbd29b40ee27ff1decf0fc87cbe180b40924f5/index.ts#L917-L1073)). | Borrow trigger-message correlation, visible terminal status, and history-as-reference-only. Do not use its in-memory pending-job set as a durable queue. |

Pi's value is therefore **how tools execute**, not **which assistant tools exist**.

## What Hermes actually ships

Hermes has three different capability layers that must not be conflated:

1. **Built-in model tools** are Python functions registered in the core tool registry.
2. **Bundled skills** are instruction files plus scripts/CLIs copied into a user's Hermes profile. They are shipped, but many work by granting the model terminal access and relying on behavioral instructions rather than structural authorization.
3. **Optional MCP catalog entries** are shipped connector manifests pointing to external vendor-hosted servers. The manifest and client are Hermes code; the service tools live outside Hermes.

Hermes's official reference currently reports roughly 86 built-in tools and identifies their toolsets and runtime prerequisites ([built-in tools reference](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/website/docs/reference/tools-reference.md), [toolsets reference](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/website/docs/reference/toolsets-reference.md)).

### Assistant-relevant built-in Hermes tools

| Area | Actually shipped | Important qualification | Florence today |
|---|---|---|---|
| Public web | `web_search`, `web_extract`; multi-provider routing and page/PDF extraction ([source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/web_tools.py), [docs](https://hermes-agent.nousresearch.com/docs/user-guide/features/web-search)). | Search/extract are generic public tools, not private-account connectors. | Florence now has isolated `research_public_web`, but no reusable page/PDF URL extraction tool ([reasoner](../../apps/api/src/reasoner.ts#L1339-L1351)). |
| Interactive browser | Navigation, accessibility snapshots, click/type/scroll/back/press, screenshots, console, images, and CDP escape hatches ([source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/browser_tool.py), [docs](https://hermes-agent.nousresearch.com/docs/user-guide/features/browser)). | Some backends are local, some third-party cloud. Browser Use mode can execute model-written Python and is only enabled with terminal access. | No browser or form interaction. |
| Scheduled work | A single `cronjob` action tool supports create/list/update/pause/resume/run/remove, one-shot and recurring jobs, scoped toolsets, and result delivery ([source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/cronjob_tools.py), [docs](https://hermes-agent.nousresearch.com/docs/user-guide/features/cron)). | This is an agent scheduler, not a native phone reminder store. | One-shot reminders plus finite/interest monitors; no reminder update/cancel/recurrence and no generic scheduled task. |
| Delegated work | `delegate_task` supports foreground/batch/background work. Background dispatch returns a handle; terminal results have a durable delivery ledger and abandoned work becomes `unknown` ([source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/async_delegation.py), [docs](https://hermes-agent.nousresearch.com/docs/user-guide/features/delegation)). | The child itself runs on a daemon executor and cannot resume after process loss; recovery makes the ambiguity honest rather than completing the lost computation. | No generic accepted/running/completed task record; due work is limited to Google polls and two monitor types ([store](../../packages/database/src/store.ts#L517-L565)). |
| User clarification | `clarify` supports open-ended, single-select, multi-select, and batched questions ([tools reference](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/website/docs/reference/tools-reference.md#clarify-toolset)). | UI abstraction, not a work capability. | Florence asks conversational clarifications; no structured choice UI is available over Linq. |
| Memory and history | Curated persistent `memory`, FTS5-backed `session_search`, and a session-local `todo` list ([memory source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/memory_tool.py), [history source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/session_search_tool.py), [todo source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/todo_tool.py#L1-L15)). | Hermes memory is profile-file memory and `todo` is a model planning aid, not a durable human task system. | Florence's source-linked household Vault and visibility model are more appropriate; do not replace them with Hermes memory/todo. |
| Media | Vision analysis, image generation, text-to-speech, video analysis, and optional video generation ([tools reference](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/website/docs/reference/tools-reference.md#vision-toolset)). | Provider credentials and delivery support vary. | Florence can ingest JPEG/PNG/WebP, PDFs, and voice-note transcription, but does not generate media. |
| Smart home | Home Assistant entity/service discovery, reads, and service calls ([toolsets](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/toolsets.py#L73-L74)). | Credential-gated; service calls are consequential writes. | None. |
| Music | Seven Spotify tools for playback, devices, queue, search, playlists, albums, and library via a bundled plugin ([reference](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/website/docs/reference/tools-reference.md#spotify-toolset)). | OAuth and user-specific state. | None. |
| Public X search | Read-only public X search via xAI ([source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/x_search_tool.py)). | Off by default and not a general web search. | Generic public web search can cover most family-assistant needs. |
| Desktop control | `computer_use` and, in the Hermes desktop only, preview/terminal/window tools plus `react_to_message` ([computer-use source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/computer_use/tool.py), [reaction source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/react_to_message_tool.py)). | Desktop reaction is not available in messaging sessions. | Florence already sends application-owned Linq messages and reactions with authority rechecks; that is the better boundary. |
| MCP | Stdio, Streamable HTTP, and SSE clients; discovery, OAuth, filtering, reconnect, timeouts, resources/prompts, sampling, and dynamic tool registration ([source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/mcp_tool.py), [docs](https://hermes-agent.nousresearch.com/docs/user-guide/features/mcp)). | MCP standardizes invocation, not household privacy or action authority. | None. |

### Messaging is mostly a surface, not an agent tool

Hermes supports many messaging adapters, but the normal CLI/messaging toolsets intentionally do **not** expose a model-callable `send_message`; cron delivery, gateway notifiers, and the `hermes send` CLI own outbound delivery ([`toolsets.py`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/toolsets.py#L409-L415)). A `send_message_tool.py` implementation exists for standalone/gateway paths and includes reaction support on compatible platforms, but it is not part of the normal model tool list ([source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/send_message_tool.py)).

This is a pattern Florence should preserve: the model decides **what** to communicate, while the application owns routing, participant authority, idempotency, replies, and delivery. Florence already has the relevant Linq primitives, so copying Hermes's channel stack would be a regression rather than a capability gain.

### Assistant-relevant bundled Hermes skills

The bundled catalog is real shipped content, but a skill is often instructions around a terminal CLI rather than a typed, policy-enforced application tool ([official catalog](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/website/docs/reference/skills-catalog.md)).

| Domain | Shipped capability | Reuse judgment |
|---|---|---|
| Google Workspace | Gmail search/read/send/reply/labels; Calendar list/create/delete; Drive search/get/upload/download/folders/share/trash/delete; Contacts list; Sheets read/create/update/append; Docs read/create/append. The script uses Google OAuth and a stable JSON contract ([skill](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/productivity/google-workspace/SKILL.md), [implementation](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/productivity/google-workspace/scripts/google_api.py#L1-L54)). | Take the capability coverage, JSON shapes, daily-brief procedure, and approval/read-back rules. Do not run this broad Python CLI with Florence's credentials: Florence already has a narrower TypeScript Google adapter and structural authority checks. |
| Maps and places | Geocoding, reverse geocoding, nearby POIs, distance, turn-by-turn directions, timezone, area, and bounding-box search via Nominatim, Overpass, OSRM, and TimeAPI.io ([skill](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/productivity/maps/SKILL.md), [script](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/productivity/maps/scripts/maps_client.py)). | High-value and mostly read-only. Port the client/contract to TypeScript or run it as a credential-free isolated service. This is one of the clearest concrete additions for Florence. |
| Apple personal data | Apple Reminders add/list/edit/complete/delete, Apple Notes, iMessage/SMS, and Find My through macOS CLIs ([Reminders](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/apple/apple-reminders/SKILL.md), [Notes](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/apple/apple-notes/SKILL.md), [iMessage](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/apple/imessage/SKILL.md), [Find My](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/apple/findmy/SKILL.md)). | Not portable to Railway and would duplicate Linq messaging. The reminder CRUD semantics are useful; the macOS implementation is not. |
| Email triage | Generic IMAP/SMTP through Himalaya and a triage procedure that prioritizes threads and drafts responses ([catalog](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/website/docs/reference/skills-catalog.md#email)). | Take the triage/draft workflow. Add Gmail write/send only after a draft/approval/verified-send product contract exists. |
| Documents | OCR, cited document-to-action-items, PDF creation/edit/fill/merge, DOCX, XLSX, PowerPoint, Box, Notion, Airtable, meeting action items, and Teams meeting processing ([catalog](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/website/docs/reference/skills-catalog.md#productivity), [document action workflow](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/productivity/document-to-action-items/SKILL.md)). | Take the provenance and approval procedure first. File generation is useful later but less central than Gmail, Calendar, reminders, maps, travel, and durable work. |
| Briefs and planning | Daily brief reference, weekly review, meeting-action extraction, and source-grounded planning ([weekly review](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/productivity/weekly-review-planning/SKILL.md), [daily brief](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/productivity/google-workspace/references/daily-brief.md)). | Directly relevant to Florence's “what's on the docket?” product goal. Adapt these workflows to the parental-unit visibility model and 90-day evidence horizon. |
| Monitoring | Product/flight/hotel/listing price monitor; blog/RSS watcher; competitor/news monitor ([price monitor](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/productivity/product-price-monitor/SKILL.md), [blog watcher](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/research/blogwatcher/SKILL.md)). | Copy the exact-item contract, foreground baseline, last-good observation, alert fingerprint, cooldown, and duplicate-suppression rules into Florence's monitor design. |

### Travel and service connectors in Hermes's optional MCP catalog

These are not built-in Hermes implementations. Hermes ships reviewed manifests for external MCP servers and dynamically loads their remote tools.

| Connector | What the shipped manifest establishes | Florence relevance |
|---|---|---|
| Kiwi.com | Anonymous official MCP at `https://mcp.kiwi.com`; the default tool is `search-flight`; results include itineraries and booking links, but booking/payment stay on Kiwi ([manifest](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/optional-mcps/kiwi/manifest.yaml)). | High-value read-only specialist for the exact flight-alternatives scenario. Evaluate provider quality/terms, then expose only search, never booking. |
| trivago | Anonymous official MCP for hotel comparison; search only, with booking on linked sites ([manifest](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/optional-mcps/trivago/manifest.yaml)). | Useful read-only travel tool after flights/maps. |
| AllTrails | Anonymous official MCP with five verbose trail tools ([manifest](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/optional-mcps/alltrails/manifest.yaml)). | Family discovery, but lower priority; a good case for deferred tool loading. |
| Todoist | OAuth MCP for tasks/projects with selected compatibility/template tools excluded by default ([manifest](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/optional-mcps/todoist/manifest.yaml)). | Optional external task store after Florence has a first-class internal reminder/work model. |
| Calendly | OAuth MCP for scheduling links, events, and invitees ([manifest](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/optional-mcps/calendly/manifest.yaml)). | Useful for adult scheduling, not core family-calendar intelligence. |
| Dropbox / Notion / Canva | OAuth MCPs for files, knowledge, and design ([Dropbox](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/optional-mcps/dropbox/manifest.yaml), [Notion](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/optional-mcps/notion/manifest.yaml), [Canva](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/optional-mcps/canva/manifest.yaml)). | Later opt-in connectors; not reasons to give the core family agent unrestricted MCP access. |

Hermes has no dedicated built-in weather tool in its core tool or bundled-skill catalogs. Weather is handled through public web, a connected Home Assistant sensor, or an external connector. Florence should add a small deterministic weather provider rather than model-navigate weather sites.

## Florence capability gap, based on current source

Florence's model-visible foreground tools are currently `search_family_memory`, `read_source`, zero-argument isolated `research_public_web`, private-only `search_gmail`, and scoped `read_calendar_window` ([tool definitions](../../apps/api/src/reasoner.ts#L1310-L1387), [mounting](../../apps/api/src/reasoner.ts#L2044-L2051)). The application can additionally commit messages/reactions, source-linked memory operations, one-shot reminders, finite/interest monitors, household updates, setup links, and family Calendar operations through its [structured decision path](../../apps/api/src/reasoner.ts#L274-L475).

| Capability | Florence | Pi | Hermes | Decision |
|---|---|---|---|---|
| Public search | Yes, isolated current-request search | No | Yes | Florence's privacy isolation is stronger; keep it. |
| URL/page extraction | No general tool | No | Yes | Add a safe `read_public_page` tool. |
| Browser/form interaction | No | No | Yes | Add later as an isolated worker with explicit submit approval. |
| Gmail read/search | Private adult only | No | Skill | Keep Florence's scoped tool; add attachments and full-calendar parity. |
| Gmail draft/send/reply/labels | No | No | Skill | Add draft first; send/reply only after explicit approval and provider receipt. |
| Calendar read | Bounded scoped window | No | Skill | Expand personal calendar selection; keep provider/audience policy in Florence. |
| Family Calendar writes | Structured create/update/delete with policy | No | Skill has create/delete | Florence is already stronger for the family use case. |
| Reminder CRUD/recurrence | Create one-shot only | No | Cron CRUD + Apple skill | Add update/cancel/recurrence using one action-oriented tool. |
| Generic durable work | No | Specification only; implementation incomplete | Partial background-delegation ledger | Implement in PostgreSQL using both projects' lifecycle ideas. |
| Contacts | No | No | Google skill reads contacts | Add scoped contact lookup when external communication is designed. |
| Maps/places/routes/timezone | Generic web only | No | Bundled maps skill | Add early. |
| Flight/hotel specialist search | Generic web only | No | Optional Kiwi/trivago MCP | Add read-only specialist tools early if provider evaluation passes. |
| Weather | Generic web only | No | No dedicated tool | Add a small deterministic provider. |
| Files/docs | Image/PDF/voice input; no creation/editing | Coding file tools only | Broad document skills | Add extraction/provenance first; generation later. |
| Memory | Source-linked, visibility-aware family Vault | No assistant memory | Profile memory + session FTS | Keep Florence; do not import Hermes memory semantics. |
| Messages/reactions | App-owned Linq delivery/reaction with authority | Pi Chat channel patterns | App/gateway-owned; limited reaction tools | Florence already has the right ownership. |
| MCP/connectors | No | Extension only | Full client/catalog | Build a narrow connector layer after capability/approval metadata exists. |
| Smart home/music/media generation | No | No | Yes | Optional, later. |

Two existing gaps are especially cheap to close because Florence already has underlying provider support:

- Conversational Gmail cannot currently inspect attachments even though Florence's background Google review can.
- Conversational Calendar reads default to a limited connection/window while background review can enumerate calendars.

Those should be finished before adding a second Google implementation.

## What to take, concretely

### Reuse mode by source asset

| Source asset | Directly copy or depend on? | Recommended reuse |
|---|---|---|
| Pi `@earendil-works/pi-agent-core` | **Possible direct TypeScript dependency, but not now.** | First adapt the tool interface, cancellation, progress, hooks, events, and steering/follow-up semantics inside Florence. Evaluate the package only if Florence later replaces its inner Responses loop. |
| Pi durable `harness.md` | **Copy concepts only.** | Translate intent/effect/settlement and replay classes into PostgreSQL records. The corresponding implementation is not operational. |
| Pi Chat runtime | **Copy patterns only.** | Use trigger-message correlation, typing heartbeat, visible terminal status, and history-as-reference. Do not copy its process-local pending queue. |
| Hermes `tools/url_safety.py`, `web_result_cache.py`, and bounded-output helpers | **Port logic and tests to TypeScript.** | This is reusable security/normalization behavior for `read_public_page` and the future browser worker. |
| Hermes `tools/web_tools.py` | **Port the provider contract, not the Python runtime.** | Florence already uses OpenAI search; add extract/fetch behind its own isolated tool and reuse Hermes's normalized result/cache/error ideas. |
| Hermes Maps skill and `maps_client.py` | **Direct isolated script is feasible; a TypeScript port is preferable for production.** | The operation set, JSON output, and public providers are immediately useful and do not need household credentials. |
| Hermes Google Workspace skill | **Copy workflow text/output contracts; do not copy the broad credentialed CLI into production.** | Extend Florence's existing Google adapter with the missing surfaces and preserve application-enforced visibility/approval. |
| Hermes `cronjob_tools.py` and `async_delegation.py` | **Port state machines and schemas, not execution code.** | Rebuild them against Florence's PostgreSQL store, leases, outbox, Linq routing, and provider reconciliation. |
| Hermes `tools/registry.py`, `toolsets.py`, `tool_search.py` | **Port the data model/algorithm after the catalog grows.** | Add Florence-specific privacy, result-audience, consequence, and approval fields; keep discovery scoped to tools already authorized for the turn. |
| Hermes reaction prompt/schema | **Copy/adapt the behavioral language only.** | Florence's application-owned Linq reaction primitive is already better than a generic model delivery tool. Reactions should remain occasional human expression or a real work-start cue, never a substitute for status. |
| Hermes daily brief, weekly review, document-to-action-items, and price-monitor skills | **Direct workflow-content reuse is appropriate.** | Adapt the procedures to household sources, the parental-unit Vault, the 90-day evidence window, and minimal-crossing privacy. |
| Hermes optional MCP manifests | **Copy catalog metadata only if useful; the tools themselves are external services.** | Connect to selected vendor servers through a Florence-owned allowlisted client. Re-evaluate endpoint terms, auth, schemas, and data handling independently. |

### Take or port now

1. **A uniform tool contract and lifecycle from Pi.** Define every Florence capability with typed input/output, privacy scope, audience, consequence class, approval requirement, cancellation, timeout, concurrency, progress, and terminal outcome. Translate actual `tool_execution_start/update/end` events into reactions/progress; never infer work from model prose. Pi's `AgentTool` and loop types are the best TypeScript reference ([types](https://github.com/earendil-works/pi/blob/v0.84.3/packages/agent/src/types.ts#L360-L409)).
2. **Hermes's action-oriented scheduling shape.** One reminder/schedule tool should support create/list/update/pause/resume/run/remove rather than separate brittle gates. Florence must store the jobs in PostgreSQL and deliver through its existing outbox; copy the schema/semantics, not Hermes's local scheduler.
3. **Hermes's background completion vocabulary.** Use `accepted`, `running`, `succeeded`, `failed`, `unknown`, `cancelled`; persist the result and its delivery state; on ambiguous process loss report `unknown` and reconcile instead of replaying a potentially consequential action ([async ledger](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/async_delegation.py#L145-L175)).
4. **A safe public page extractor.** Port Hermes's URL normalization, sensitive-query rejection, SSRF/private-network protection, redirect revalidation, bounded result/spill behavior, and provider-independent result contract ([URL safety](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/url_safety.py), [web cache](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/web_result_cache.py)). Keep it in Florence's isolated public context.
5. **Maps and deterministic travel data.** Port the Maps skill's eight-operation contract and evaluate the Kiwi search-only MCP. Add a small weather API beside them. These tools should accept only the current adult-authored public query and coarse location needed for the task.
6. **Hermes's monitor procedures.** Copy the price-monitor rules for exact-item identity, successful foreground baseline, last-known-good state, cooldown, and duplicate alert fingerprint. Copy the daily/weekly brief coverage checklists, then adapt them to Florence's two-adult visibility policy.
7. **Complete Florence's existing Google reads.** Add conversational attachment reading and selectable/all-calendar reads through Florence's current adapter before considering Drive/Docs/Contacts.

### Take after the first tool tranche

1. **A capability registry/toolset model.** Port the useful shape from Hermes's [`tools/registry.py`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/registry.py) and [`toolsets.py`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/toolsets.py): schema, handler, availability check, credentials, consequence class, surface eligibility, and dynamic description. Add Florence-specific fields for household/adult visibility and allowed result audience.
2. **Progressive tool disclosure.** Hermes replaces large MCP/plugin catalogs with `tool_search`, `tool_describe`, and `tool_call`, while always scoping the catalog to tools granted to that session ([design/source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/tools/tool_search.py), [docs](https://hermes-agent.nousresearch.com/docs/user-guide/features/tool-search)). Pi also supports additive dynamic tool activation. Florence will need this when its catalog is large; it is unnecessary for five tools.
3. **An isolated browser worker.** Borrow per-task session isolation, automatic cleanup, credential-scrubbed subprocess environments, URL policy, and accessibility snapshots from Hermes. Start read-only. Clicking a final submit, booking, purchase, message send, or account change must cross back into a Florence-owned approval/effect tool.
4. **A narrow MCP connector layer.** Support explicitly installed servers, include-only tool filters, per-server credentials, timeouts, output caps, and tool discovery. The MCP bridge may expose only capabilities already granted by Florence's application policy. It must not be a back door around adult-private versus household visibility.
5. **Drive/Docs/Sheets/Contacts and document action extraction.** Add reads first. For writes, stage a preview, obtain approval, use deterministic operation keys, read the provider result back, and preserve source provenance.

### Do not take

- Pi's shell/filesystem coding tools or Hermes's terminal/code-execution tools for the family agent.
- Pi's unimplemented `AgentHarness` as a worker runtime.
- Hermes's profile-file memory or session-local `todo` as the family Vault or human task store.
- Hermes's Python Google CLI as a second set of production Google credentials and policies.
- Arbitrary MCP tools directly in the main family-context model request.
- Persistent logged-in browser profiles holding both adults' accounts.
- A model-callable generic cross-channel `send_message`; keep delivery application-owned.
- Coding Kanban, code review, repository, shell, and project-management tools unless Florence becomes a different product.

## Recommended build order

### Tranche 1: useful read-only assistant

1. `read_public_page`
2. maps/places/routes/timezone
3. deterministic weather
4. flight search (evaluate Kiwi search-only connector)
5. Gmail attachment reads
6. all/selectable personal calendars
7. reminder update/cancel/recurrence

These cover the highest-frequency family-assistant gaps with low external-effect risk.

### Tranche 2: reliable work

1. PostgreSQL generic work record
2. lease/heartbeat/cancellation
3. actual-tool-start reaction and progress events
4. terminal `succeeded`/`failed`/`unknown`
5. durable completion delivery and duplicate suppression
6. foreground/background admission rule

This is where Pi's lifecycle contracts and Hermes's async/cron patterns are most valuable.

### Tranche 3: research and planning

1. isolated browser worker
2. daily/weekly family docket procedure
3. document/attachment action extraction with citations
4. price/availability monitors
5. Drive/Docs/Sheets/Contacts reads

### Tranche 4: approved external action

1. Gmail draft, then approved send/reply
2. external contact lookup and approved messaging
3. approved form submission
4. read-only hotel/booking-option lookup
5. booking/purchase only if Florence gains a separate confirmation, payment, receipt, cancellation, and reconciliation product contract

Spotify, Home Assistant, media generation, Todoist, Notion, Dropbox, Canva, Calendly, and other MCP integrations can then be opt-in modules rather than part of every family's baseline prompt.

## Final decision

- **Pi:** take the TypeScript tool contract, lifecycle events, cancellation/progress, policy hooks, steering/follow-up distinction, dynamic activation, and durable-effect specification. Do not take its coding tools or unfinished harness.
- **Hermes:** take the assistant capability map and port the high-value patterns/tools: web extraction, URL safety, schedules, work ledger, maps, travel connector catalog, monitor procedures, tool registry/toolsets, progressive discovery, and isolated browser boundaries. Do not embed the whole Python runtime.
- **Florence:** keep its own PostgreSQL authority, source visibility, household policy, provider idempotency, Linq delivery, and Google adapter as the control plane. Build a larger tool arsenal behind those boundaries.

The practical implication is that Florence should indeed become much more tool-capable. The mistake would be equating “more tools” with “give a general agent shell/MCP/browser access.” The useful version is a broad **catalog of narrow, capability-scoped tools** whose starts, progress, approvals, effects, and results Florence can prove.
