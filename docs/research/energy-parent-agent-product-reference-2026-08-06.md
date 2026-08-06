# Energy as a product reference for Florence

Research date: 2026-08-06

Primary launch post: [Gabriel, August 6, 2026](https://x.com/gabriel1/status/2085418582192841147)

Official product surface: [Energy — Work at the speed of thought](https://energy-landing-c0013r.web.app/)
Installed build inspected read-only: Energy for macOS `0.7.25`

## Executive synthesis

Energy's strongest product idea is not “an agent that can do anything.” It is a much more legible behavior:

> **Describe a work outcome in one sentence; Energy gathers context, acts across the tools where the work lives, and returns a result to review.**

That is the closest thing in the evidence to Energy's consumer “how I...” statement: **Energy is how I hand off computer work.** The company says this directly in several equivalent forms: work that lives in a browser, inbox, or file can be handed off in a sentence; the user describes the outcome; the system plans and performs the steps; and the completed result comes back for review. [Official product page](https://energy-landing-c0013r.web.app/)

The product packages four difficult agent concepts into an ordinary workflow:

1. **Outcome-first input:** the user says what should be true, not which tools or agent steps to run.
2. **Persistent specialist:** an assistant owns a recognizable class of work instead of producing another disposable chat thread.
3. **Cross-tool execution:** the assistant can use connected apps and a signed-in browser profile.
4. **Reviewable completion:** the user sees a result and an audit trail, not merely an assertion that the agent worked.

The most important Florence transfer is therefore not Energy's desktop UI or its assistant list. It is the product loop:

```text
plain-language outcome
→ gather relevant context
→ plan and act across existing surfaces
→ continue in the background when appropriate
→ return a concrete, reviewable result
```

For Florence, the family version should be:

```text
family-relevant signal or request
→ reconcile people, time, source, and permissions
→ open the smallest necessary coordination loop
→ obtain an explicit human commitment
→ close the loop with a durable, audience-safe receipt
```

Energy demonstrates that users should not have to manage model, memory, skill, and automation primitives. Florence has an even stronger reason to hide those mechanics: parents should experience one Florence, while its orchestrator, skills, and ephemeral workers remain implementation details.

## Evidence standard and limitations

This memo uses three labels:

- **Observed:** visible directly in an official Energy product surface or founder-published product demo.
- **First-party claim:** stated by Energy or its founder, but not independently verified here.
- **Inference:** a product implication for Florence derived from the observed evidence or claim.

Only primary or first-party sources are cited. No claims from replies, press coverage, or third-party reviews are treated as evidence.

The evidence has important limits:

- Energy launched on the research date. This is launch-day product evidence, not longitudinal proof of retention, reliability, or user behavior.
- The canonical `getenergy.com` and `app.getenergy.com` endpoints returned TLS errors from the research environment. The product page used here is the fallback Firebase site that the founder linked while publicly debugging the launch; that provenance is visible in his [first-party X reply](https://x.com/gabriel1/status/2085439228679483431).
- The installed macOS app was inspected read-only. Its visible shell, packaged first-party instructions, skills, and compiled onboarding flow were examined without connecting accounts, sending messages, purchasing anything, or changing Florence code. Because this was not a clean first-run account, onboarding details taken from the packaged application are **build observations**, not evidence of activation or retention.
- A product demo proves what Energy chose to show, not that every run is dependable. Reliability, permission enforcement, and completion quality remain unverified.

## Evidence map

| Product question | Evidence | Classification | What is justified |
|---|---|---|---|
| What is the core behavior? | The official site says browser-, inbox-, and file-based work can be handed off in one sentence; its three-step explanation is “describe the outcome,” connect tools, and review the result. [Source](https://energy-landing-c0013r.web.app/) | First-party claim | Energy is positioning itself as outcome delegation, not question answering. |
| What does execution look like? | The launch demo visibly shows a task moving across a finance application and Gmail to resolve missing-receipt work. A second scenario begins from a broad product-research request and operates in a browser while the assistant thread remains visible. [Source](https://x.com/gabriel1/status/2085418582192841147) | Observed in official demo | The intended unit is multi-step, cross-app work rather than one response. |
| Is it proactive? | The founder says he asked Energy to check launch-post replies every five minutes and send newly reported bugs into Slack. [Source](https://x.com/gabriel1/status/2085433088176459990) | First-party claim | Energy supports at least scheduled recurring work that continues after the initiating prompt. |
| What is the interaction surface? | The official site and launch demo show a desktop-style workspace: specialist assistants in a sidebar, a conversational task surface, and visible browser/result state. [Sources](https://energy-landing-c0013r.web.app/), [launch demo](https://x.com/gabriel1/status/2085418582192841147) | Observed | Energy is a workbench with conversation inside it, not a messaging-native participant. |
| How does it get context? | Energy says it gathers from email, files, conversations, and connected tools. Named connections include Gmail, Google Drive, Google Calendar, Slack, and Outlook. It also says its browser uses the user's signed-in profiles. [Source](https://energy-landing-c0013r.web.app/) | First-party claim | Context is federated across tools and browser state rather than limited to the current prompt. |
| What is the memory abstraction? | The site contrasts disposable threads with named assistants and says assistants can be set up with a click instead of making the user manage memory, skills, and automations. [Source](https://energy-landing-c0013r.web.app/) | Observed positioning / first-party claim | Energy exposes persistent work roles while hiding the underlying agent configuration. No public evidence here establishes how memory is stored, retrieved, corrected, or scoped. |
| How are actions reviewed? | The product flow ends with a finished result to review. Energy also claims every action is recorded in an audit trail. [Source](https://energy-landing-c0013r.web.app/) | First-party claim | Review and provenance are part of the product promise, though the exact approval model is not specified. |
| How does activation begin? | The landing directs users to download Energy; the free plan says users start by connecting an existing ChatGPT account. Tool access is user-selected, and the site says the user controls what each connection may access. [Pricing](https://energy-landing-c0013r.web.app/pricing), [FAQ](https://energy-landing-c0013r.web.app/) | First-party claim | Energy reduces model-account friction and presents integrations as explicit choices. Actual onboarding duration and drop-off are unknown. |
| What is the app's information architecture? | The installed build shows a persistent outer organization/assistant/task rail, a section-level navigation surface for Tools, Browser, Memories, Skills, Organization, Settings, and Billing, and a focused main canvas. | Observed locally in build `0.7.25` | Energy separates stable identity/work context, control-plane categories, and the current object of attention. |
| How does first-run activation work in the installed build? | The packaged flow contains Welcome, Browser, Setup, Theme, and Assistants stages. It can connect a browser profile, connect a ChatGPT plan, import existing skills/memories/MCP configuration, and launch selected starter work immediately after setup. | Observed in the installed first-party application bundle | Energy tries to cross the setup-to-value gap by converting imported context into an immediate task, not by ending onboarding at “connected.” |
| What does the installed memory surface expose? | The Memories section exposes searchable Markdown context and imports; packaged memory instructions describe a curated graph of user, people, organizations, tasks, events, tools, websites, files, and dated source-linked facts. [Memory skill](</Applications/Energy.app/Contents/Resources/packages/codex-marketplaces/defaults/plugins/energy-defaults/skills/memory-write-skill/SKILL.md>) | Observed locally | Energy has both a user-facing inspection surface and a structured file convention. This describes Energy's build, not a suitable Florence system of record. |

## The “how I...” behavior

### Directly supported formulation

The best evidence-backed formulation is:

> **Energy is how I hand off work that happens on my computer.**

The narrower repeated action is “describe the outcome.” The reward is not prose; it is a completed or materially advanced result across the user's existing tools. [Official product page](https://energy-landing-c0013r.web.app/)

That framing resolves several consumer-agent adoption problems:

- The user need not know whether a job requires a browser, connector, skill, memory, automation, or a particular model.
- The user need not translate the outcome into an implementation plan.
- The product reuses tools and logins the user already has rather than requiring every workflow to be rebuilt inside Energy.
- Completion has a review surface, which is easier to trust than an invisible background assertion.

### What Energy does **not** yet prove

The first-party materials do not establish that ordinary users will remember to delegate, that the system is reliable enough to avoid supervision, or that “all computer work” is a coherent habitual wedge. The launch statement that users can do days of work in hours is a founder claim, not verified usage data. [Launch post](https://x.com/gabriel1/status/2085418582192841147)

The founder also says the eventual interface will not remain simple text-in/text-out because people do not consume information well that way. [Founder reply](https://x.com/gabriel1/status/2085445255508455590) This is useful product intent, but it also confirms the shipped interface is still evolving.

## Onboarding and activation

### What the official material shows

Energy's public activation path appears to be:

```text
download desktop product
→ connect an existing ChatGPT account or choose a paid Energy tier
→ connect selected work tools
→ choose or create a specialist assistant
→ describe an outcome
→ review a finished result
```

The free plan explicitly says users begin by connecting ChatGPT. [Official pricing](https://energy-landing-c0013r.web.app/pricing) The FAQ says users choose which apps to connect and what each may access. [Official FAQ](https://energy-landing-c0013r.web.app/) The product comparison says assistants are created with a click while memory, skills, and automations are handled underneath. [Official FAQ](https://energy-landing-c0013r.web.app/)

### What the installed build adds

**Observed locally:** build `0.7.25` packages a five-stage flow: Welcome, Browser, Setup, Theme, and Assistants. Browser setup detects profiles and offers personalized task suggestions. Setup can connect a ChatGPT plan and import skills, memories, and MCP configuration from existing agent products; it explicitly distinguishes connecting a plan from importing prior chats or memory. The final stage offers a general Main Assistant, Inbox Zero, and suggested starter work. Completing setup creates the selected assistants and starts the selected work immediately; multiple selections become separate work threads.

This is stronger than the public “download → connect → prompt” description. Energy uses onboarding to do three jobs at once:

1. acquire useful context;
2. demonstrate how work will be delegated; and
3. produce an immediate personalized result.

The packaged suggested-task skill reinforces that pattern. It derives a rolling list from recent browser activity, conversations, memory, and connected sources, prefers low-risk read/research work, and reserves write actions for cases with clear intent. [Suggested-task skill](</Applications/Energy.app/Contents/Resources/packages/codex-marketplaces/defaults/plugins/energy-defaults/skills/suggested-tasks/SKILL.md>)

### Product inference for Florence

Florence should copy the **progressive reveal**, not the exact sequence:

- Begin with a live family need in the surface parents already use.
- Ask for the minimum identity and household context required to make that need useful.
- Request a Gmail, Calendar, or other connection when it unlocks the next concrete loop—not as a long capability tour.
- Introduce rules or routines through natural confirmation after Florence observes a repeated need.
- Keep agent roles, memory stores, skills, and schedules out of the onboarding vocabulary.

Unlike Energy, Florence cannot treat one signed-in browser profile as the family's authority. Each adult and caregiver is an independent principal. Connections, private context, group-read permissions, and group-write permissions must remain attributable to the person who granted them.

The Florence equivalent of Energy's immediate starter task should be one **real family loop**, not a capability demo. For Jackson and Kendall, a successful activation ends when Florence identifies or accepts one concrete coverage need, routes it safely, records a commitment, and shows the closed state. Connecting Gmail or adding a partner is setup progress, not activation by itself.

## Proactivity and the loop model

Energy's clearest proactive example is a scheduled monitor: inspect launch replies every five minutes and route new bugs to Slack. [Founder post](https://x.com/gabriel1/status/2085433088176459990) The launch demo also presents a broad product-exploration workflow that spans product analytics, outreach, later meeting notes, product requests or bugs, and code changes. [Launch demo](https://x.com/gabriel1/status/2085418582192841147)

**Observed locally:** Energy's packaged automation policy says a recurring check should notify only when its condition is met, something meaningfully changed, or the user needs to act; unchanged checks should stay silent. [Automation instructions](</Applications/Energy.app/Contents/Resources/instructions/automations.md>) This is a small but important part of the product: being always on does not mean constantly speaking.

This supports four useful conclusions:

1. **An automation should be created from natural language.** The user states the continuing outcome; the system instantiates the schedule and tool work.
2. **Proactivity needs a destination.** Energy's recurring loop does not merely “notice” bugs—it routes them into Slack.
3. **The durable object is the workflow, not the initiating chat.** A scheduled task persists after the first conversation turn.
4. **Silence is a successful outcome.** A monitor that found no important change should not manufacture engagement.

For Florence, “scheduled task” is necessary but not sufficient. Family loops often close through people rather than software. The system must represent:

```text
detected need
→ current owner or unowned state
→ correct audience
→ request or reminder
→ explicit commitment
→ deadline / escalation state
→ closure receipt
```

Energy's example finishes when a software action has been performed. Florence's pickup loop does not finish when a message is sent; it finishes when an eligible person explicitly accepts. This human-acknowledgment boundary is the essential product difference.

## Observable memory and context model

### Direct evidence and first-party claims

Energy presents named specialists such as Inbox Zero, Research Scout, Project Captain, and Finance Keeper, and contrasts those persistent roles with a long list of disposable chat threads. [Official product page](https://energy-landing-c0013r.web.app/) Its FAQ explicitly groups memory, skills, and automations as machinery the product manages for the user. The privacy notice says the service processes connected Google Workspace content and webpage/browsing content when carrying out user-directed actions, and it may remember information to personalize later use. [Official privacy notice](https://energy-landing-c0013r.web.app/privacy)

The launch demo's sidebar similarly shows several task-shaped assistants rather than a model picker as the primary navigation. [Launch demo](https://x.com/gabriel1/status/2085418582192841147)

The installed product makes part of that context visible. **Observed locally:** the Memories section is a searchable file-oriented viewer whose primary objects include a user profile and imports. The packaged memory skill describes a canonical Markdown graph with indexes and entity files for people, companies, tasks, events, tools, websites, and files; it directs the agent to date facts, link them to evidence, distinguish recency and confidence, and treat stored memory as a potentially stale lead that must be verified. [Memory skill](</Applications/Energy.app/Contents/Resources/packages/codex-marketplaces/defaults/plugins/energy-defaults/skills/memory-write-skill/SKILL.md>) A separate indexing skill tells the agent to explore email, messages, calendar, files, browser history, and connected sources over time, prioritizing the recent month. [Indexing skill](</Applications/Energy.app/Contents/Resources/packages/codex-marketplaces/defaults/plugins/energy-defaults/skills/index-computer/SKILL.md>)

### What can reasonably be inferred

Energy's user-facing context model is **role-shaped**: a persistent assistant represents an area of responsibility, with its memory, skills, tools, and automations bundled behind the role. Its packaged memory is **entity- and file-shaped** underneath. These are product observations, not evidence of the retrieval algorithm used for any specific response.

### What remains unknown

Neither the public material nor the read-only build inspection establishes:

- which runtime rules actually govern fact promotion, correction, expiry, and retrieval;
- whether assistants learn rules from completed work;
- how conflicting context is reconciled;
- whether browser observations become durable memory;
- how permissions constrain memory retrieval; or
- whether the named assistants are long-running agents, prompt packages, or merely UI organization.

Florence should therefore borrow the **invisible complexity**, **inspectability**, and **source-aware facts** concepts without adopting Markdown files as its canonical data model. A family's identities, conversation audiences, permissions, source claims, routines, commitments, and loop states need typed records and deterministic policy. A parent-facing memory screen should show understandable facts, their source, who may use them, and a correction/forget control—not Energy's raw file tree.

## Interaction surface

Energy is visibly desktop-first. Its official demo and product page present a workspace with:

- a sidebar of specialist assistants;
- a natural-language task or conversation pane;
- connected browser or file state alongside the task; and
- a final review surface. [Official product page](https://energy-landing-c0013r.web.app/)

The site says Energy drives a real browser using signed-in profiles, while the privacy notice says the product includes desktop, web, and online services and can navigate webpages on the user's behalf. [Product page](https://energy-landing-c0013r.web.app/), [privacy notice](https://energy-landing-c0013r.web.app/privacy)

Florence should not copy this as its primary consumer surface. A parent-specific Chief of Staff has a better ambient interface: the conversations where family information and commitments already move. The web companion should instead handle exact controls, integrations, provenance, corrections, and review. Energy is evidence for a rich **review/control surface**, not evidence against iMessage-first interaction.

### Installed information architecture

The installed build reveals a useful three-layer hierarchy. **Observed locally:**

1. **Outer organization/assistant rail:** organization switcher, new-task entry, connection prompt, named assistants such as Main Assistant and Inbox Zero, and their work threads.
2. **Inner section navigation:** Tools, Browser, Memories, Skills, Organization, Settings, and Billing.
3. **Main canvas:** the selected task or control surface, sometimes paired with browser/file evidence.

The hierarchy is more important than its styling. The outer rail answers “whose workspace and which enduring helper/workstream?” The inner navigation answers “which part of the system?” The canvas answers “what needs attention now?” Florence should borrow those information jobs, not Energy's visual assets, labels, copy, or assistant roster.

### Florence shell mapping

| Layer | Energy's observed job | Florence pilot | Later Florence |
|---|---|---|---|
| **Outer household/assistant rail** | Holds organization identity, persistent assistants, and work threads. | Show the current household or private self, one persistent Florence identity, and high-level state such as setup/attention. Do not expose ephemeral workers or a roster of agent personas. | Add household switching, caregiver relationships, and durable loop history when real users need them. Keep one user-facing Florence. |
| **Inner section navigation** | Separates tools, browser, memories, skills, organization, and settings. | Use parent language: Today / Needs attention, People & permissions, Chats, Sources, Routines, and Memory & safety. The current pilot's Today, People, Chats, Sources, and Safety sections already approximate this layer. | Add a comprehensible skills/rules library and richer source controls only after users have established routines to manage. |
| **Main canvas** | Focuses the current task, configuration object, or supporting evidence. | Show exceptions and exact decisions: invitation context, chat readiness, connection status, a coverage request awaiting commitment, source provenance, or a correction. | Add optional web conversation, source/browser/file evidence, richer loop timelines, and artifacts without displacing iMessage. |

This yields a clean surface contract:

```text
iMessage / authorized group chats = tell Florence, receive prompts, commit, close
web companion = inspect, connect, correct, authorize, resolve exceptions
hidden orchestrator and ephemeral workers = research and propose bounded work
typed product state = permissions, sources, routines, commitments, and closure
```

For the Jackson/Kendall pilot, the web product should therefore **not** introduce a parallel chat habit. Its first job is control and confidence on a phone-sized screen. Web chat becomes valuable later for long-form requests, research, artifacts, or tasks that benefit from visible evidence. That sequencing preserves Florence's differentiated “already in the family conversation” behavior while leaving room for an Energy-like workbench as the product broadens.

## Evidence-based mapping to Florence

| Energy product choice | Florence transfer | Adopt, modify, or reject |
|---|---|---|
| “Describe the outcome” instead of configuring the workflow | Parents should say what changed or what must happen; Florence should derive the coordination plan. | **Adopt directly** |
| One sentence can start multi-tool work | A text, forwarded email, calendar change, or observed group message can open one family loop. | **Adopt and broaden inputs** |
| Named persistent specialist assistants | Keep stable domains and skills behind Florence, but present one coherent Chief of Staff to the family. | **Adopt internally; hide externally** |
| Memory, skills, and automations handled behind a click | Never ask nontechnical parents to manage an agent topology or prompt library. | **Adopt directly** |
| Signed-in browser can act anywhere the user can | Use scoped connectors and narrowly authorized browser work; never treat one person's browser access as household authority. | **Modify substantially** |
| Scheduled natural-language monitor | Florence should turn confirmed routines and timing preferences into durable monitors and reminders. | **Adopt with explicit rule establishment** |
| Finished result returned for review | Return a canonical loop state, source, owner, timing, and closure receipt. | **Adopt and make state explicit** |
| One user's tools supply context | Context belongs to people, households, relationships, and source conversations; retrieval must honor the audience and the most conservative applicable policy. | **Reject the single-principal assumption** |
| Desktop workbench as the primary interface | Use iMessage/group chat for ambient coordination and a mobile web control plane for sensitive setup. | **Do not copy as primary surface** |
| General-purpose scope | Florence may answer general questions, while family coordination remains its proactive domain and permission model. | **Adopt carefully** |
| Three-layer workspace hierarchy | Separate household/assistant identity, section navigation, and a focused decision canvas. | **Adopt the information hierarchy, not the visual design** |
| Personalized starter tasks from imported context | Use known family context to propose or run one immediate, low-risk family loop. | **Adopt with family scope and strict audience rules** |
| Raw Markdown memory browser | Expose friendly source-backed facts, permissions, correction, and forgetting controls. | **Reject as the parent-facing abstraction** |
| Rolling agent-suggested work | Surface only timely, high-confidence family outcomes; do not turn “proactive” into a generic task feed. | **Modify substantially** |

## What to copy and what to avoid

### Copy

- **One-sentence outcome delegation.** Parents should describe the change or desired state, not construct a workflow.
- **Immediate personalized value.** Setup should culminate in one real handled item, not a tour or a connection checklist.
- **A stable primary helper.** Florence is an enduring relationship even when work is delegated internally.
- **Source-visible completion.** Show the smallest useful evidence, resulting state, and remaining exception.
- **Silent monitoring.** No-change checks should not generate messages.
- **Context and behavior controls behind ordinary language.** Memories and recurring rules should be inspectable without requiring parents to understand prompts, skills, or agent topology.
- **A spacious control plane.** Use the web for complex inspection and correction that would be awkward or unsafe in a group chat.

### Avoid

- **An assistant zoo.** “Inbox agent,” “calendar agent,” and “school agent” are implementation boundaries, not identities a family should manage.
- **A blank general-agent canvas as first use.** Florence should lead with the coverage-loop behavior even though she can answer broader questions.
- **Raw memory files as the consumer model.** Parents need facts, sources, audiences, corrections, and deletion—not a knowledge-repository filesystem.
- **Single-user authority assumptions.** A connected browser or mailbox never grants household or group disclosure rights.
- **Activity as completion.** A sent message, agent run, or finished subtask is not a closed family loop.
- **Noisy personalization.** Suggested work that is merely plausible will make Florence feel intrusive. Timing, confidence, scope, and established family preference must all constrain proactivity.
- **Proprietary imitation.** The Florence shell may use the same abstract three-layer hierarchy, but should not reuse Energy's visual assets, distinctive copy, names, or pixel-level layout.

## Technical observations: useful, but not the product thesis

**Observed locally:** Energy `0.7.25` bundles a Codex app server and starts its default local process with fan-out and two multi-agent feature flags disabled. The build uses Energy-controlled model-provider proxy configuration. Its packaged base instructions direct the assistant to minimize user time and back-and-forth, use available context and tools before asking, batch necessary questions, and expose supporting sources. [Base instructions](</Applications/Energy.app/Contents/Resources/instructions/base.md>) The application also packages reusable skills for suggested work, computer indexing, and memory writing.

Two conclusions are justified:

1. Energy's polished user-facing notion of “assistants” should not be read as proof that its default runtime is a multi-agent fan-out system.
2. The valuable layer is the harness around the model—context acquisition, tools, reusable instructions, persistence, schedules, evidence, and interaction policy—not a particular model provider or visible agent graph.

For Florence, hidden ephemeral specialists remain a sensible implementation for bounded research, extraction, or drafting. They should return typed proposals to the orchestrator. Deterministic Florence code must continue to own identity, audience authorization, commitment state, timing, outbound effects, receipts, and loop closure. Energy supports the value of hiding this machinery; it does not supply evidence for outsourcing Florence's household state machine to an agent framework.

## The sharpest Florence product formulation after Energy

Energy reinforces that Florence should own an ordinary behavior, not an agent category.

Recommended consumer statement:

> **Florence is how our family makes sure things get handled.**

Recommended initiating behavior:

> **Tell Florence what changed—or let an authorized source put her in the loop.**

Recommended reward:

> **Florence turns scattered information into a real, acknowledged family plan and stays with it until the loop is closed.**

The differentiated mechanism is not simply “works across apps.” It is:

- context arrives from email, calendar, attachments, private chats, household groups, and read-only community groups;
- Florence reconciles that context without leaking one relationship's information into another;
- Florence chooses the smallest safe audience;
- Florence can proactively open a loop under a confirmed rule;
- responsibility closes only on an explicit commitment; and
- every participant sees only what their identity, relationship, and current conversation permit.

That is a harder system than Energy because Florence is multi-principal and relational. It is also a potentially stronger consumer wedge: the parent experiences relief in a familiar surface, while the orchestration remains invisible.

## Product questions Energy leaves open for Florence

The comparison suggests concrete questions for the Jackson/Kendall pilot:

1. Can a parent get the first real coordination win before connecting every data source?
2. Does Florence turn a natural message into a visible loop without requiring the parent to know a command?
3. Can a recurring rule be created conversationally, shown clearly, and corrected without exposing agent machinery?
4. Does every proactive message have an attributable source, policy, and reason for its timing?
5. When action crosses people rather than software, does Florence distinguish “asked,” “tentative,” “accepted,” and “closed”?
6. Can the web companion make provenance and permissions reviewable without becoming the product parents must live in?
7. Does Florence feel like one competent participant even when the orchestrator delegates to ephemeral specialists?

## Bottom line

Energy is a strong reference for **product compression**. It compresses models, tools, browser control, skills, memory, and schedules into one user behavior: hand off an outcome and review what came back. Its launch-day materials do not yet prove reliability or retention, and they reveal little about the actual memory architecture.

Florence should adopt that compression while changing the unit of completion. Energy closes software work with a result. Florence closes family work with an acknowledged human commitment, an audience-safe state transition, and continued follow-through. That is the parent-specific “how I...” behavior Energy helps clarify.

## First-party sources and local evidence inspected

Web evidence:

- [Energy launch post and demo](https://x.com/gabriel1/status/2085418582192841147)
- [Official Energy product surface](https://energy-landing-c0013r.web.app/)
- [Official pricing](https://energy-landing-c0013r.web.app/pricing)
- [Official privacy notice](https://energy-landing-c0013r.web.app/privacy)
- [Founder's launch-day recurring-monitor example](https://x.com/gabriel1/status/2085433088176459990)
- [Founder's fallback-site provenance](https://x.com/gabriel1/status/2085439228679483431)

Installed first-party build evidence, read-only:

- [Application metadata](</Applications/Energy.app/Contents/Info.plist>)
- [Base assistant policy](</Applications/Energy.app/Contents/Resources/instructions/base.md>)
- [Automation policy](</Applications/Energy.app/Contents/Resources/instructions/automations.md>)
- [Agent-messaging policy](</Applications/Energy.app/Contents/Resources/instructions/situational/agent-messaging.md>)
- [Suggested-task skill](</Applications/Energy.app/Contents/Resources/packages/codex-marketplaces/defaults/plugins/energy-defaults/skills/suggested-tasks/SKILL.md>)
- [Computer-indexing skill](</Applications/Energy.app/Contents/Resources/packages/codex-marketplaces/defaults/plugins/energy-defaults/skills/index-computer/SKILL.md>)
- [Memory-writing skill](</Applications/Energy.app/Contents/Resources/packages/codex-marketplaces/defaults/plugins/energy-defaults/skills/memory-write-skill/SKILL.md>)
- Packaged renderer and runtime inside `/Applications/Energy.app/Contents/Resources/app.asar`, inspected only for onboarding, navigation, built-in assistant configuration, and default process flags.

No private memory content, browser history, account data, or local filenames from the Energy workspace are quoted or used in this memo.
