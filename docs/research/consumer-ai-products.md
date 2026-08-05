# Consumer AI Products and the Right Entry Wedge for Life OS

Research current as of August 4, 2026.

## Executive conclusion

Life OS should be built as a consumer product first, but that does **not** mean launching a generic “AI for your whole life.”

The strongest strategy is:

> **Launch a narrow, repeated consumer behavior backed by a deliberately extensible Life OS engine.**

The initial product should feel like a Chief of Staff reachable in the place the user already communicates. Its promise is:

> **Text it once; it comes back handled.**

The first target is a high-agency, context-fragmented professional: a founder, operator, investor, consultant, or executive who already uses AI, has several live projects and curiosities, and carries too many open loops across email, calendars, links, notes, and their own head.

The first repeated loop should be:

1. The user sends a messy input: a link, thought, question, goal, email, screenshot, or “I should do this.”
2. The Chief of Staff understands why it matters, attaches it to durable context, and asks only if a missing answer would materially change the work.
3. An ephemeral worker researches or prepares the next useful artifact.
4. The Chief of Staff returns a concise result and keeps responsibility for the follow-through.
5. The user approves, corrects, or redirects it; those outcomes improve future judgment.

This is narrower than the eventual vision, but it is not a throwaway point solution. It exercises the core primitives the broad system will eventually need: durable context, project state, background execution, ephemeral workers, artifacts, proactive follow-up, approval policy, and one identity across channels.

The broad “Life OS” category is already being entered by platform companies. ChatGPT has memory, agents, scheduled tasks, and Pulse; Gemini now has Personal Intelligence, Daily Brief, and the experimental Spark personal agent; Claude has memory, research, connectors, and scheduled Cowork tasks; Perplexity has Computer, Comet, and cloud Tasks. Competing with all of that through a list of modules would be strategically weak. The opportunity is to make a more coherent and trustworthy behavior loop for a particular kind of person, then let breadth emerge from earned context.

## The decision

| Path | What it means | Advantage | Main problem | Verdict |
|---|---|---|---|---|
| A. Broad Life OS first | Build email, calendar, tasks, finance, health, media, learning, household, and project modules before finding a repeated behavior | Most faithful to the long-term vision | Long onboarding, delayed magic, too many trust surfaces, hard evaluation, and direct competition with platform assistants | Do not choose |
| B. Narrow point product first | Build a standalone research, reminders, or planning product without a common substrate | Fastest way to test a market behavior | Risks becoming another silo; context and integrations would later need to be rebuilt | Better, but incomplete |
| C. Narrow wedge on a Life OS engine | Expose one magical loop while building only the general primitives that loop requires | Fast learning, coherent UX, and architectural continuity with the larger vision | Requires discipline not to overbuild the hidden platform | **Recommended** |

“Consumer product first” should therefore mean **behavior first**, not “marketing site first” or “a collection of consumer dashboards first.” The system should prove that a user will repeatedly delegate open loops to it before it tries to model every domain of a life.

## Why a broad horizontal launch is now the wrong opening

### Platform assistants are becoming personal operating layers

The horizontal feature set is converging rapidly:

- ChatGPT combines saved memory and chat-history memory with controls to inspect, remove, or bypass it; its newer “Dreaming” work synthesizes memory in the background and evaluates whether it carries useful context, respects constraints, and stays current. [OpenAI memory controls](https://openai.com/index/memory-and-new-controls-for-chatgpt/), [OpenAI on memory Dreaming](https://openai.com/index/chatgpt-memory-dreaming/)
- ChatGPT Pulse performs proactive overnight research from conversation history, memory, feedback, and optional Gmail and Calendar context, then presents a daily set of visual cards. OpenAI explicitly describes early failures such as resurfacing completed work or suggesting irrelevant actions. [Introducing ChatGPT Pulse](https://openai.com/index/introducing-chatgpt-pulse/)
- Gemini Personal Intelligence can reason across Gmail, Photos, YouTube, and Search, while offering citations, explanations of which context was used, inline correction, and Temporary Chat. Google also documents over-personalization and temporal or relationship mistakes as live failure modes. [Gemini Personal Intelligence](https://blog.google/innovation-and-ai/products/gemini-app/personal-intelligence/), [Google’s Personal Intelligence technical paper](https://ai.google/static/documents/building_personal_intelligence.pdf)
- In May 2026 Google announced Daily Brief, a connected-app background digest, and Gemini Spark, a cloud personal agent built on its Antigravity harness. Google says Spark can use triggers, skills, and workflows continuously and is designed to ask before consequential actions such as sending an email or spending money. [The next evolution of the Gemini app](https://blog.google/innovation-and-ai/products/gemini-app/next-evolution-gemini-app/)
- Claude supports memory with user-visible categories and project separation, deep research over the web and connected Google data, and remote scheduled Cowork sessions with approval modes and run history. [Claude memory](https://support.claude.com/en/articles/11817273-use-claude-s-chat-search-and-memory-to-build-on-previous-context), [Claude Research](https://support.anthropic.com/en/articles/11088861-using-research-on-claude-ai), [scheduled Cowork tasks](https://support.claude.com/en/articles/13854387-schedule-recurring-tasks-in-claude-cowork)
- Perplexity Computer describes an orchestrator that delegates to specialist subagents; its Tasks product already runs isolated cloud agents on a schedule with connectors, tools, run history, retry behavior, credit accounting, and an explicit “needs attention” state rather than guessing. [Perplexity Computer](https://www.perplexity.ai/cs/hub/blog/introducing-perplexity-computer), [Perplexity Tasks](https://www.perplexity.ai/help-center/en/articles/11521526-perplexity-tasks)

These are first-party descriptions. They demonstrate product direction, not independent proof that the products are consistently good. But they do make one strategic point clear: “one assistant that knows everything” is no longer a sufficient entry wedge.

### Breadth creates an onboarding tax before it creates trust

A system cannot simply ask a new user to describe their entire life and expect a useful model. Human Chiefs of Staff learn by doing: they observe the principal’s priorities, see corrections, track what was followed through, and accumulate judgment from real episodes.

The product should do the same. Context should be earned and refined through useful work. Every completed loop should leave behind better project state, preference evidence, relationship context, and timing knowledge. Asking the user to populate a dozen modules before any payoff reverses that sequence.

### The product must be evaluated as judgment, not feature coverage

The hard part is not whether an agent can call Gmail, Calendar, search, or a task API. It is whether it:

- notices the right thing;
- understands why it matters now;
- chooses an appropriately ambitious next step;
- interrupts at the right time;
- knows when it needs a decision;
- carries responsibility across days without creating noise; and
- learns from correction without fossilizing a temporary preference.

A broad launch creates too many kinds of judgment failures at once. A narrow repeated loop creates a tractable set of real examples from which to build evaluations.

## What the current consumer products teach

The useful comparison is not a feature checklist. It is each product’s entry wedge, repeated loop, source of context, trust design, and path to expansion.

| Product | Entry wedge and repeated loop | Context and memory | Proactivity and action | Product lesson for Life OS |
|---|---|---|---|---|
| ChatGPT | General conversation; return for answers and work | Saved memories, past chats, background synthesis, user controls | Pulse, scheduled tasks, research and computer action with confirmations | General assistants will own commodity breadth; transparent memory and proactive relevance are product surfaces |
| Gemini | Google-native assistant with unusually rich personal data | Cross-app retrieval, long context, Personal Intelligence | Daily Brief, scheduled actions, Spark workflows | Existing data distribution is a moat; Life OS needs a coherent loop Google does not already make magical |
| Claude | Thoughtful knowledge work and artifacts | Project-separated memory and connectors | Research, Cowork, scheduled remote sessions, approval mode | Artifacts, project context, and inspectable sessions are more durable than chat alone |
| Perplexity | Citation-first answer and research engine | Search, connected sources, browser context | Comet browser actions, Computer orchestration, cloud Tasks | Background work needs explicit state, isolation, retries, and “needs attention” semantics |
| Poke | One proactive assistant inside messaging | Conversation plus connected email, calendar, reminders, web, MCP tools | Messages first, follows up, automations, approval loops | Channel can be the wedge; one identity and personality can make delegation habitual |
| Orchid | Personal inbox of work the agent handled or needs help with | Email and calendar context | Reads, prepares, waits for approval; “handled” versus “needs you” | A calm exception queue may be a better operational UI than a multi-agent dashboard |
| Gemini Notebook, formerly NotebookLM | Put trusted sources in; get grounded understanding and useful artifacts out | User-selected source corpus with citations | Source discovery and artifact generation | Narrow source trust plus tangible output can produce a fast magic moment |
| Granola | Every meeting automatically becomes a better note | Meeting audio/transcript, the user’s notes, cross-meeting corpus | Mostly preparation and synthesis rather than autonomous action | A repeated unavoidable event creates context automatically; the human’s notes remain the steering signal |
| Limitless | Ambient recall from a wearable | Continuous audio “lifelogs” | Recall and search | Ambient capture is powerful but adds privacy, consent, hardware, and continuity risk; it is not an MVP requirement |
| Character.AI | Ongoing relationship with a character | Pinned story memory, extracted editable facts, compression | Conversational continuity rather than real-world execution | Memory needs layers, visibility, and correction; continuity matters emotionally as well as functionally |
| Comet and Dia | The browser itself becomes the assistant’s context and hands | Current tabs, history, connected apps, explicit permission | Browser commands with scoped access and previews | Browser action must assume prompt injection and separate observation, drafting, and irreversible commitment |

### Poke: messaging is not merely another client

Poke is the closest product analogue for the proposed front door. It lives in Apple Messages, Telegram, WhatsApp, and RCS, while connecting to email, calendars, reminders, web search, and integrations. Its API lets an external event send a conversational message that Poke processes with its normal context and app access. Its “recipes” bundle onboarding context, a first behavior, and required integrations into something that can be shared. [Poke documentation](https://poke.com/docs), [Poke API](https://poke.com/docs/api), [Poke recipes](https://poke.com/docs/creating-recipes)

The important lesson is not “copy a chatbot in iMessage.” It is that a message thread can become a durable delegation relationship:

- the user does not need to open a special app to remember something;
- proactive output arrives in a socially legible form;
- the same identity can accept an instruction, report progress, ask for a decision, and follow up later; and
- shareable behaviors can eventually become a distribution surface.

Poke’s public release notes are also unusually valuable evidence about the unglamorous failures that decide whether this feels like a Chief of Staff: duplicate messages, timezone errors, wrong attendees, accidental actions, lost context, and approval loops. [Poke release notes](https://poke.com/docs/release-notes)

Cognition reported that Poke processed more than 100 million messages in three months and reached hundreds of thousands of users before its July 2026 acquisition. Those numbers are company-reported marketing claims, not independently audited evidence, but they are a useful signal that a message-native personal agent can achieve meaningful consumer pull. [Cognition’s Poke announcement](https://cognition.com/blog/interaction)

### Orchid: the operational UI is “handled” and “needs you”

Orchid’s beta framing is a personal AI that reads email and calendar, prepares the next action, and waits when approval is needed. Its “44 handled, 3 need you” metaphor is more relevant than whether the underlying product currently performs well. [Orchid beta announcement](https://orchid.ai/blog/orchid-beta-is-here)

For Life OS, this suggests that the web product should not expose a busy org chart of agents. It should expose:

- what the system believes is happening;
- what it handled;
- what it is working on;
- what needs the user’s decision;
- the artifact or draft it produced; and
- the evidence and permissions behind an action.

That is the cockpit. Agent topology is an implementation detail.

### Gemini Notebook: a fast magic moment comes from a constrained transformation

Google says the first NotebookLM prototype was built in six weeks by four to five part-time people. Its early loop was extremely simple: sources go in; useful grounded outputs come out. The team then used direct feedback and usage to refine formats such as Audio Overviews. [Developing NotebookLM](https://blog.google/innovation-and-ai/products/developing-notebooklm/)

The product, renamed Gemini Notebook in July 2026, remains differentiated from general chat by grounding answers and artifacts in a chosen corpus. [Notebook grounding](https://blog.google/innovation-and-ai/technology/ai/notebooklm-google-ai/), [Gemini Notebook announcement](https://blog.google/innovation-and-ai/products/gemini-notebook/notebooklm-gemini-notebook/)

The lesson is that a narrow product does not need a narrow future. A trusted project container plus one excellent transformation can become a home for many later behaviors.

### Granola: context is easiest to collect when it falls out of work

Granola entered through a repeated, unavoidable event: a meeting. It treats the user’s typed notes as a “steering wheel,” visually separates human notes from AI additions, and lets the user inspect supporting transcript quotes. [Granola announcement](https://www.granola.ai/blog/announcement)

Granola says it captures device audio locally, transcribes it, discards the audio, and retains the transcript and notes for later cross-meeting use. These are company disclosures, not a security audit, but the product pattern is strong: capture just enough context to make an immediate artifact, make provenance visible, and let accumulated artifacts become organizational memory. [Granola on meeting capture](https://www.granola.ai/blog/can-ai-listen-to-a-meeting-and-take-notes)

Life OS should seek the same compounding dynamic. A link, question, or open loop should immediately yield something useful, while also improving the user’s project memory.

### Limitless: ambient capture is a later option, not a starting requirement

Limitless exposed lifelogs with timestamps, speakers, transcripts, and search through an API and MCP interface. [Limitless API](https://www.limitless.ai/developers/docs/api)

But ambient audio creates severe consent, privacy, platform, and product-continuity burdens. Limitless was acquired by Meta in December 2025, stopped selling its pendant, and announced the shutdown of older Rewind capture functionality. [Limitless acquisition and product status](https://www.limitless.ai/)

Life OS does not need to solve omniscient capture to be useful. Explicit delegation plus connected read-only sources is a safer source of early context.

### Character.AI: memory is part of the relationship contract

Character.AI’s 2026 memory system separates explicit Story Memory, pinned information, automatically extracted editable Facts, and background compression. It also gives users a visualization of what memory is influencing a conversation. [Character.AI memory](https://blog.character.ai/memory/)

The domain is different, but the design principle transfers: users should be able to distinguish what they explicitly told the system, what it inferred, what is temporary, and what is protected. A utility-oriented Chief of Staff should use this continuity to help the user make progress, not optimize emotional dependency or time in the app.

## The proposed consumer wedge

### Product definition

**Working category:** a Chief of Staff in your messages.

**Promise:** text it once; it comes back handled.

**Initial target user:** a high-agency professional with several simultaneous work and personal projects, multiple email/calendar accounts, a habit of saving links and ideas, and a willingness to delegate research and preparation—but not yet unsupervised communication, purchases, or financial actions.

This is deliberately more specific than “everyone with a life.” The founding user is an unusually good design partner because the actual desired behavior is already clear:

- “Here is a link; tell me what it means for our project.”
- “I want to understand this domain well enough to speak fluently.”
- “This might be a company idea; investigate it.”
- “Promote this into a project and keep it moving.”
- “Look across what is happening and tell me what needs attention.”

### The first magic loop

The loop should be called **delegated follow-through**, not capture and not chat.

1. **Drop:** The user sends a link, thought, goal, screenshot, forwarded email, or natural-language request through iMessage or the web conversation.
2. **Orient:** The Chief of Staff identifies the relevant person, goal, project, horizon, and desired outcome. If confidence is sufficient, it proceeds. If not, it asks one material question.
3. **Delegate:** The durable Chief of Staff assigns an ephemeral worker a bounded job with relevant context and permissions.
4. **Produce:** The worker creates a persistent artifact: a researched answer, decision memo, plan, comparison, draft, or project brief with sources and open questions.
5. **Return:** The Chief of Staff summarizes what changed and labels the state: handled, working, needs you, or blocked.
6. **Carry:** If the item remains open, the system brings it back at an appropriate time or includes it in a concise brief.
7. **Learn:** Approval, correction, dismissal, and eventual outcome update the system’s judgment and memory.

The first magic moment is not “the AI answered my question.” ChatGPT and Perplexity already do that. It is:

> “I mentioned something once, it understood why it mattered, did useful work, and remembered to move it forward without making me manage it.”

### Three concrete initial scenarios

**A link becomes project intelligence**

The user texts an X post with: “How should this change Life OS?” The system reads linked material, finds authoritative supporting sources, produces a short project memo, updates the project’s assumptions, and returns the two decisions that actually changed. If the idea deserves deeper work, the user says “promote it,” and the same thread becomes an active project with a lead and next checkpoint.

**A curiosity becomes a learning path**

The user says: “I need to become fluent in financial markets for work.” The system asks about the situations in which fluency matters, builds a grounded learning map, prepares the next lesson or case at a useful cadence, connects new articles and podcast episodes to that map, and adapts based on what the user can explain or apply. It does not research random facts merely because they match a broad interest label.

**A loose obligation stops occupying working memory**

The user says: “I should figure out whether these credit cards are worth keeping.” Initially, the system requests the minimum structured information and produces a decision artifact, rather than logging into financial accounts or canceling anything. Later, after trust and connectors are built, the same job can incorporate statements and benefits automatically while keeping all external action behind confirmation.

### What the web app is for

The message thread should be the everyday front door. The web app should be the calm cockpit and source of truth:

- one conversation with the Chief of Staff;
- a “handled / working / needs you” inbox;
- projects and their artifacts;
- the system’s current understanding of the user, with edit/forget controls;
- approvals, permissions, and audit history;
- connected accounts and their scope;
- brief history and notification controls.

It should not begin as a collection of separate dashboards for workouts, meals, TV, books, restaurants, tasks, and finance. Those can eventually become views over shared events, people, goals, projects, and preferences. Requiring the user to switch personas or workspaces would undermine the holistic premise.

## Product boundaries for the MVP

### In scope

- One durable Chief of Staff identity shared by web and iMessage.
- Free-form capture of links, text, and lightweight attachments.
- Project and goal association, including “promote this to a project.”
- Ephemeral research/preparation workers.
- Persistent artifacts with source provenance.
- Background jobs, progress state, retry, and recovery.
- Proactive follow-up on open loops.
- A concise daily or periodic brief, learned from user feedback.
- Read-only calendar connection across multiple Google accounts after the core loop works.
- Inspectable, correctable memory.
- Explicit approval for anything with external impact.
- Full run traces for debugging and evaluation, while presenting users only the useful status and result.

### Explicitly out of scope for the first eight weeks

- Sending email or messages on the user’s behalf.
- Purchases, cancellations, trades, transfers, or financial-account mutation.
- Full YNAB replacement.
- Health optimization or meticulous meal/workout tracking.
- Household accounts and partner access.
- Ambient audio capture.
- A marketplace of agents.
- A visible multi-agent org chart.
- A bespoke dashboard for every life domain.
- An agent that edits its own production prompts, policies, or code without an offline evaluation and human-controlled promotion step.

These are sequencing decisions, not rejections of the long-term vision.

## Trust must be a product ladder

Consumer agents fail when permissions are represented as a single “full access” switch. Complete access may be the eventual user preference, but the system still needs internal boundaries, auditability, and graded autonomy.

| Level | System behavior | MVP policy |
|---|---|---|
| Observe | Read user-provided content and explicitly connected sources | Automatic within granted scopes |
| Think | Search, research, organize, infer, and update internal state | Automatic, with provenance |
| Prepare | Draft an email, calendar change, plan, form, or purchase recommendation | Automatic; clearly marked as a draft |
| Mutate internal state | Create or update a project, task, note, or memory | Reversible and visible; initially announce material changes |
| Reversible external action | Create a tentative calendar hold or save a draft | Confirm at first; later allow narrow standing rules |
| Consequential external action | Send communication, spend money, delete data, cancel service, trade, or publish | Explicit confirmation per action in the foreseeable product |

Browser products offer useful implementation patterns. Dia says a session begins without tab or write access, requires grants, shows the exact draft before filling it, limits cross-site navigation, and disallows irreversible actions. Perplexity’s Comet offers first-use choices such as allow once, always allow, or deny, while keeping much history local by default. [Dia security](https://www.diabrowser.com/security), [Comet privacy and permissions](https://www.perplexity.ai/help-center/comet/en/articles/12867415-comet-assistant-privacy-data-use)

Payments provide an even stronger analogy. Google’s agentic shopping work uses constraints such as merchant, product, amount, and time, with verifiable audit records; Stripe describes payment tokens scoped by seller, time, and amount. [Google agentic shopping guardrails](https://blog.google/products-and-platforms/products/shopping/google-shopping-cart/), [Stripe Agentic Commerce Suite](https://stripe.com/blog/agentic-commerce-suite)

The principle is: **grant a capability, not an identity-shaped blob of authority.**

## Memory and personalization: a governed model, not a vector database

Memory is the hardest product surface because being wrong with confidence is worse than asking again.

### Store immutable evidence before derived memory

The system should keep:

1. **Raw events:** user messages, connector events, approvals, corrections, task outcomes, and artifact versions.
2. **Project state:** goals, decisions, open questions, next actions, related artifacts, and activity.
3. **Explicit memory:** facts or preferences the user deliberately asks it to remember.
4. **Inferred memory:** a compact, revisable conclusion derived from evidence.
5. **Working context:** temporary state for a task or conversation, which should expire.

Every inferred memory should carry:

- provenance;
- scope, such as global, work, personal, project, or later household;
- confidence;
- when it was learned;
- when it was last confirmed;
- temporal validity or an expiry rule; and
- contradiction history.

Google’s Personal Intelligence paper is especially useful here. It describes combining tool calls, dense retrieval, and long-context reasoning to assemble context, and provides a failure taxonomy that includes incorrect retrieval, over-personalization, temporal mistakes, and relationship confusion. [Building Personal Intelligence](https://ai.google/static/documents/building_personal_intelligence.pdf)

OpenAI’s Dreaming work similarly treats memory as background synthesis rather than a flat list, and names staying current and honoring preferences as separate evaluation objectives. [OpenAI memory Dreaming](https://openai.com/index/chatgpt-memory-dreaming/)

### User controls are part of the model

The user should be able to ask:

- “What do you believe about this?”
- “Why did you think that?”
- “When did you learn it?”
- “That was temporary.”
- “Use this only for the wedding project.”
- “Forget this.”

Memory review should be a normal interaction, not a privacy settings graveyard. The system can periodically present a short summary of material changes to its understanding.

### Open memory systems are references, not automatic dependencies

Letta provides memory blocks, conversation search, a filesystem-backed memory model, scheduled/background agents, subagents, permissions, and channel adapters. Its “co” example explicitly pairs one agent per user with a background sleeptime agent. [Letta](https://github.com/letta-ai/letta), [Letta co](https://github.com/letta-ai/co)

Mem0 exposes user, session, agent, and organization scopes plus an evaluation framework for memory accuracy, cost, and latency. Its benchmark numbers should be treated as vendor-reported. [Mem0](https://github.com/mem0ai/mem0), [Mem0 memory evaluation](https://docs.mem0.ai/core-concepts/memory-evaluation)

Honcho models peers, sessions, messages, and asynchronous representations such as conclusions and summaries on Postgres and pgvector. [Honcho](https://github.com/plastic-labs/honcho)

These systems are useful design references. The MVP should still begin with an append-only event log and simple, inspectable derived memory in the existing database. Adopting a memory vendor before there are real failure cases would outsource the wrong problem: deciding what should be remembered and how its validity is evaluated.

## Engineering patterns that matter

### 1. One durable orchestrator, ephemeral workers

The user should have one continuous Chief of Staff relationship. Specialist workers should be created for bounded jobs and disappear after returning:

- the task;
- relevant context;
- allowed tools and data scopes;
- a budget and deadline;
- an expected artifact schema; and
- a clear stop or escalation condition.

Their reasoning state does not need to become permanent memory. Their evidence, artifact, outcome, errors, and useful conclusions do.

This structure avoids personality fragmentation, keeps specialist context small, and lets workers fail or retry without corrupting the principal relationship.

### 2. Durable state must live outside the model session

Anthropic’s managed-agent architecture uses a durable event log outside a disposable harness and separates the agent “brain” from sandboxed “hands.” Credentials are not exposed to the execution environment; tools reduce to a narrow execution interface. Anthropic reports lower time-to-first-token from this design, but those performance figures are company-reported. [Anthropic on managed agents](https://www.anthropic.com/engineering/managed-agents)

Perplexity Tasks reinforces the operational model: scheduled runs are isolated agents, have run histories, can retry, and can stop in a visible “needs attention” state. [Perplexity Tasks](https://www.perplexity.ai/help-center/en/articles/11521526-perplexity-tasks)

For Life OS, every background job needs:

- a durable job record;
- idempotency key;
- lease and heartbeat;
- event stream;
- explicit terminal state;
- retry policy;
- artifact checkpoint;
- budget accounting; and
- recovery behavior after the Mac runner or backend restarts.

### 3. Files and artifacts are working memory

The agent harness analysis by Lilian Weng argues for files as durable state, subagents as isolated parallel contexts, explicit task status, and preservation of failed attempts. It also warns that self-improvement with weak evaluators invites reward hacking; evaluators and permissions must stay outside the self-modification loop. [Agent harnesses](https://lilianweng.github.io/posts/2026-07-04-harness/)

Stripe’s Kai knowledge agent, described in a LangChain case study, similarly places artifacts beside the conversation, uses an S3-backed virtual filesystem, and treats the sandbox as a tool rather than the location of the durable agent. The article reports that Kai was initially built in a week and later spread to more than 5,000 Stripe users, but these are company and vendor-reported case-study metrics. [How Stripe built Kai on Deep Agents](https://www.langchain.com/blog/how-stripe-built-their-knowledge-ai-platform-on-deep-agents)

Life OS should therefore make the project artifact—not the transcript—the durable unit of useful work. A transcript explains how the system got there; the artifact is what the user revisits, edits, shares, or acts on.

### 4. Context must be compiled for each job

“The model knows my life” cannot mean sending an ever-growing life transcript on every call.

Anthropic describes context as a finite attention budget and recommends constructing the smallest high-signal set needed for the task. [Effective context engineering for AI agents](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)

LangChain’s Deep Agents work uses filesystem offloading for very large tool results, trims redundant inputs as context fills, and summarizes older material. [Context management for Deep Agents](https://www.langchain.com/blog/context-management-for-deepagents)

Stripe’s case study says Kai’s tool catalog grew to more than 500 MCP tools and its skill library past 1,000 skills. The system consequently moved toward two-pass selection and dynamic loading; the article reports quality degradation when roughly 150 skills were placed directly in context. These are first-party/customer engineering disclosures, not universal thresholds. [Stripe Kai case study](https://www.langchain.com/blog/how-stripe-built-their-knowledge-ai-platform-on-deep-agents)

The Life OS context compiler should select:

- the user request and recent local turns;
- relevant project state;
- a small set of evidence-backed memories;
- relevant people and deadlines;
- necessary source artifacts;
- allowed tools; and
- policy constraints.

It should log what was selected and why. That trace will be essential for diagnosing personalization failures.

### 5. Skills and tools need progressive disclosure

Do not expose every connector and agent capability to every run. Keep a small set of foundational skills pinned, retrieve candidate skills based on the task, then let the orchestrator choose among a bounded list. Load actual tool schemas only when needed.

This reduces token load, tool-selection errors, and accidental authority. It also allows a project lead to be “accessible” as a durable project identity while the actual workers remain ephemeral.

### 6. Sandboxing and approval fatigue must be designed together

Anthropic reports that Claude Code’s operating-system sandbox reduced permission prompts by 84 percent, while noting that experienced users otherwise tend to auto-approve and rely on interruption. [How Anthropic contains Claude](https://www.anthropic.com/engineering/how-we-contain-claude)

The lesson is not to eliminate approvals. It is to replace repetitive low-signal approvals with:

- safe execution boundaries;
- scoped credentials;
- policy-based standing permission for low-risk classes;
- exact previews for consequential actions;
- interruption and cancellation;
- and audit after the fact.

Stripe Projects uses scoped credentials, spend thresholds, and environment boundaries for coding agents, defaulting work to development environments. [Stripe Projects agent controls](https://stripe.com/blog/stripe-projects-adds-new-agents-providers-developer-controls)

### 7. Evaluation begins with real episodes

Anthropic recommends starting with roughly 20–50 tasks drawn from real failures, grading outcomes rather than whether an agent claimed success, and combining deterministic checks, model graders, human review, production monitoring, A/B tests, and transcript inspection. [Demystifying evals for AI agents](https://www.anthropic.com/engineering/demystifying-evals-for-ai-agents)

Life OS needs separate evaluations for:

- task outcome;
- context selection;
- memory correctness and freshness;
- source quality;
- tool and permission compliance;
- interruption relevance;
- escalation judgment;
- artifact usefulness;
- follow-through timing;
- and recovery after partial failure.

The test set should be built from the founding user’s real requests, including examples where the right behavior is to do nothing, ask, wait, or surface uncertainty.

## Recommended technical shape for the current project

The current TypeScript, Fastify, Postgres, React, and local Codex-runner direction is compatible with this product strategy. There is no reason to rewrite around Deep Agents or Mastra before the repeated loop is proven.

Use those systems as pattern libraries while keeping the runner behind an interface. The existing Codex subscription constraint makes the local Codex harness the practical execution backend now; a provider-neutral job contract preserves the option to use another harness later.

The minimum architecture is:

| Layer | Responsibility |
|---|---|
| Channel adapters | Normalize web and iMessage events into the same conversation and identity |
| Control plane | Users, conversations, projects, permissions, schedules, jobs, approvals, and state transitions |
| Event log | Append-only record of messages, connector events, decisions, corrections, and task outcomes |
| Context compiler | Select relevant project state, memory, artifacts, tools, and policy for a run |
| Orchestrator | Decide whether to answer, ask, delegate, schedule, or escalate |
| Job system | Durable queue, lease, heartbeat, retry, idempotency, status, and cancellation |
| Ephemeral runner | Invoke Codex for a bounded job with scoped tools and no durable identity of its own |
| Artifact store | Versioned research, plans, drafts, decisions, files, and provenance |
| Memory service | Derive, inspect, correct, expire, and scope memory from event evidence |
| Policy and approvals | Separate proposed actions from committed actions and enforce capability scopes |
| Observability and evals | Run traces, context snapshots, costs, failures, graders, and user feedback |

The Mac should act as a trusted local “hands” runner for local files and authenticated desktop capabilities. The hosted backend should remain the durable “brain” and control plane. A runner loss must pause or retry work rather than lose state.

Deep Agents is relevant because its published primitives—planning, filesystem state, subagents, context management, and async runs—match the eventual harness. [Deep Agents](https://www.langchain.com/deep-agents), [Deep Agents asynchronous subagents](https://www.langchain.com/blog/deep-agents-v0-5)

Mastra may also be a reasonable TypeScript framework to evaluate later, but choosing an orchestration framework is not the current product bottleneck. The crucial interfaces are the job contract, event model, artifact model, permission boundary, and evaluation harness. If those are clean, the execution engine can change.

## An eight-week learning plan

This is a product-learning plan, not a promise that the full Life OS will exist in eight weeks.

### Week 1: define the behavior and the evidence

- Write the product contract: “text it once; it comes back handled.”
- Collect 25 real founding-user episodes across links, company ideas, learning goals, project promotion, and open-loop follow-up.
- Label the desired outcome, necessary context, acceptable questions, forbidden actions, and completion evidence.
- Establish run tracing, artifact provenance, latency, token/cost, and outcome feedback.
- Manually concierge the workflow when automation is not ready; the goal is to learn the right behavior.

**Exit test:** five examples where the desired result is unambiguous enough to evaluate.

### Week 2: one identity across web and messages

- Make web and iMessage feed the same conversation and durable user identity.
- Support text and links first.
- Add explicit “remember,” “attach to project,” and “promote to project” controls through natural language.
- Show handled, working, needs you, and failed states in the web cockpit.

**Exit test:** the user can start a job in one channel and inspect or continue it in the other without explaining it again.

### Week 3: research and artifact production

- Implement a bounded ephemeral research worker.
- Require primary-source citations and separate evidence from inference.
- Save a versioned artifact to a project rather than only sending a chat answer.
- Let the user correct, redirect, or deepen it without restarting.

**Exit test:** at least 70 percent of the first ten real artifacts are judged useful enough to keep or act on after no more than one correction. This is a local learning threshold, not a market benchmark.

### Week 4: carry open loops

- Add durable follow-up state and schedules.
- Generate one concise daily or user-chosen brief from active projects and open loops.
- Add “not useful,” “wrong time,” “already done,” and “do not bring this up again” feedback.
- Be quiet when nothing is materially useful.

**Exit test:** at least half of proactive items are explicitly useful or lead to action, with no consequential false action. If this threshold is missed, improve relevance before increasing frequency.

### Week 5: read-only personal context

- Connect multiple Google Calendar accounts read-only.
- Use calendar context only for prioritization, preparation, and timing.
- If calendar quality is stable, add narrowly scoped Gmail search/read for user-requested jobs; do not ingest whole inboxes by default.
- Log which source account and item influenced each output.

**Exit test:** connected context reduces questions or improves timing without producing account confusion, wrong-attendee errors, or private-context leakage.

### Week 6: memory and trust controls

- Add explicit versus inferred memory, provenance, scope, freshness, and correction.
- Build “what do you know and why?” and forget flows.
- Add an approval ledger even though the MVP does not execute consequential external actions.
- Review every error in context selection, memory, time, identity, and project association.

**Exit test:** the user can correct a belief once and observe the change consistently in later relevant tasks.

### Week 7: small external dogfood cohort

- Recruit three to five people matching the target profile, not a broad friends-and-family cohort.
- Give each a concierge onboarding around three live projects and one recurring open loop.
- Test two framings: “Chief of Staff in your messages” and “text it once; it comes back handled.”
- Watch actual delegation behavior; do not rely only on interview enthusiasm.

**Exit test:** at least three users independently delegate a second real task after receiving the first result.

### Week 8: make the expansion decision

- Review retention by delegation days, not app opens.
- Identify the most repeated job and the highest-value source connector.
- Decide whether to deepen research/projects, calendar/email coordination, or proactive brief quality.
- Add no new domain module unless user behavior demands it.
- Write the next 50-task evaluation set from dogfood failures.

**Decision rule:** expand only if users repeatedly hand the product responsibility for an open loop. If they use it as a generic chatbot, the wedge is not yet working.

## Metrics that reflect the actual promise

OpenAI has publicly argued that ChatGPT should be optimized for user progress rather than time spent, while using return cadence as a signal of lasting utility. [Optimizing ChatGPT](https://openai.com/index/optimizing-chatgpt/)

Life OS should similarly avoid time-in-app as its north star.

### Activation

- Time from first message to first kept artifact.
- Percentage of users who delegate a second real task within seven days.
- Percentage who connect a read-only source after seeing value.

### Repeated value

- Delegation days per week.
- Open loops carried to a useful outcome.
- Artifacts kept, revisited, or acted on.
- Proactive items accepted, opened, or used.
- Percentage of jobs that require no repeated context explanation.

### Judgment and trust

- Material-question rate and unnecessary-question rate.
- Wrong-project, wrong-person, and stale-memory rates.
- Proactive false-positive and “wrong time” rates.
- Approval, correction, and dismissal patterns.
- Permission violations: target is zero.
- User-reported “I no longer had to carry this in my head.”

### Reliability

- Jobs completing with evidence of outcome.
- Duplicate action/message rate.
- Retry and recovery success.
- Time in “working” without a useful update.
- Jobs silently lost: target is zero.

### Early kill signals

- Users only ask questions they could have asked ChatGPT or Perplexity.
- Delegation stops after novelty.
- Most proactive output is ignored or marked noise.
- Onboarding requires more context work than the user gets back.
- Users will not trust the product with read-only calendar or email access after experiencing the core loop.
- The system creates more open loops than it closes.

## Distribution and business model implications

The first distribution channel should also be the product: iMessage via Linq, with the web app as cockpit. This reduces habit formation cost and makes proactive behavior natural.

Poke publicly offers free, Pro, and Ultra tiers, with its Ultra tier listed at $199 per month; many broader assistants also gate advanced agents, memory, schedules, or usage through subscriptions and credits. [Poke pricing](https://poke.com/)

That supports—but does not prove—a subscription model for high-value personal delegation. Pricing should not be optimized during the first eight weeks. The near-term question is whether users repeatedly delegate responsibility. If that behavior is strong, likely packaging is:

- a consumer subscription for the persistent Chief of Staff;
- usage or capacity tiers for background work;
- later household seats and shared scopes; and
- eventually specialized high-trust services or recipes.

Avoid advertising “unlimited agents.” Users buy outcomes and reduced cognitive load, not topology.

## Main risks

### The wedge is still too broad

“Anything you can text a Chief of Staff” can collapse into generic chat. The product must emphasize project-aware follow-through and proactive closure, and initially tune around a small number of episodes.

### Proactivity becomes noise

A scheduled summary is easy to build and easy to ignore. Proactivity should have a relevance threshold, quiet behavior, explicit timing feedback, and a record of whether the item led to action.

### Memory becomes confident fiction

Inferences must be evidence-backed, scoped, revisable, and time-aware. Corrections must outrank older derived beliefs.

### Integrations dominate product work

Multiple Google accounts are important, but connector breadth can consume the roadmap. Begin with read-only Calendar because it improves timing and preparation; add Gmail only for narrow user-requested retrieval before attempting inbox autonomy.

### Approval prompts become theater

If every low-risk action asks, the user will approve reflexively. Use sandboxing and scoped standing permissions for thinking and internal preparation, while keeping exact confirmation for consequential external effects.

### The architecture becomes a framework science project

Deep Agents, Mastra, Letta, Mem0, and similar systems contain useful ideas. None answers the hard product question of what deserves attention and follow-through. Preserve provider-neutral interfaces, but learn from actual user episodes before introducing framework complexity.

### “Self-learning” mutates behavior without accountability

The system should learn preferences and propose harness changes, but production prompts, policies, and code should change through versioned experiments with offline evaluation and explicit promotion. The evaluator and permission layer must remain outside the self-improvement loop.

### Platform companies copy the feature

They already have most features. Defensibility must come from a coherent relationship, accumulated high-quality context, trustworthy execution history, a specialized judgment loop for the target user, and eventually networked household/project workflows—not from a single connector or agent framework.

## Final recommendation

Build the Life OS engine, but **ship the Chief of Staff behavior**.

The user should not encounter “modules” or “multi-agent AI” at first. They should encounter one person-like operational relationship:

- it is always reachable;
- it understands the project behind the request;
- it delegates invisibly;
- it returns useful artifacts;
- it keeps open loops alive;
- it asks for judgment at the right boundary;
- it shows why it believed or did something; and
- it becomes more accurate through outcomes rather than a giant onboarding questionnaire.

If that loop becomes habitual, books, podcasts, learning, company research, wedding planning, finance, meals, restaurants, TV, household collaboration, and eventually external execution can all become manifestations of the same underlying system.

If that loop does not become habitual, building those modules first would only produce a larger product without a reason to return.

## Primary source map

The table below distinguishes product announcements and vendor claims from sources that expose implementation or operational detail. Neither category is independent validation.

| Product or system | Primary source | What it supports | Evidence type |
|---|---|---|---|
| ChatGPT | [Memory and controls](https://openai.com/index/memory-and-new-controls-for-chatgpt/) | Saved versus chat-history memory and user controls | Official product disclosure |
| ChatGPT | [Memory Dreaming](https://openai.com/index/chatgpt-memory-dreaming/) | Background synthesis and memory eval objectives | Official product/technical disclosure |
| ChatGPT | [Pulse](https://openai.com/index/introducing-chatgpt-pulse/) | Proactive daily research, connected context, early relevance failures | Official product disclosure |
| ChatGPT | [Agent](https://openai.com/index/introducing-chatgpt-agent/) and [system card](https://openai.com/index/chatgpt-agent-system-card/) | Browser, terminal, connectors, confirmations, and safety model | Official product and safety disclosure |
| ChatGPT | [Scheduled tasks](https://help.openai.com/en/articles/10291617-scheduled-tasks-in-chatgpt) | Recurring and delayed work | Official support documentation |
| OpenAI | [Optimizing ChatGPT](https://openai.com/index/optimizing-chatgpt/) | Progress rather than time-spent product objective | Official product philosophy |
| Gemini | [Personal Intelligence](https://blog.google/innovation-and-ai/products/gemini-app/personal-intelligence/) | Opt-in cross-app context, citations, correction, disclosed failures | Official product disclosure |
| Gemini | [Building Personal Intelligence](https://ai.google/static/documents/building_personal_intelligence.pdf) | Context packing architecture and failure taxonomy | Official technical paper |
| Gemini | [Scheduled actions](https://blog.google/innovation-and-ai/products/gemini-app/scheduled-actions-gemini-app/) | Recurring and one-off proactivity | Official product disclosure |
| Gemini | [Daily Brief and Spark](https://blog.google/innovation-and-ai/products/gemini-app/next-evolution-gemini-app/) | Connected daily digest and cloud personal-agent direction | Official product announcement |
| Google | [Universal Cart and AP2](https://blog.google/products-and-platforms/products/shopping/google-shopping-cart/) | Scoped constraints and audit records for agentic commerce | Official product/technical disclosure |
| Claude | [Memory](https://support.claude.com/en/articles/11817273-use-claude-s-chat-search-and-memory-to-build-on-previous-context) | Categorized, inspectable, project-separated memory | Official support documentation |
| Claude | [Research](https://support.anthropic.com/en/articles/11088861-using-research-on-claude-ai) | Web and Google connector research with citations | Official support documentation |
| Claude | [Scheduled Cowork tasks](https://support.claude.com/en/articles/13854387-schedule-recurring-tasks-in-claude-cowork) | Remote sessions, approval mode, schedules, and history | Official support documentation |
| Anthropic | [Effective context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents) | Context-budget and selection guidance | First-party engineering analysis |
| Anthropic | [Managed agents](https://www.anthropic.com/engineering/managed-agents) | Durable event log, disposable harness, brain/hands split | First-party engineering disclosure |
| Anthropic | [Containing Claude](https://www.anthropic.com/engineering/how-we-contain-claude) | Sandboxing and approval-fatigue measurements | First-party engineering disclosure |
| Anthropic | [Agent evals](https://www.anthropic.com/engineering/demystifying-evals-for-ai-agents) | Outcome grading, initial task count, mixed eval methods | First-party engineering guidance |
| Poke | [Documentation](https://poke.com/docs) | Message channels and connected capabilities | Official product documentation |
| Poke | [API](https://poke.com/docs/api) | Events entering the same conversational agent | Official API documentation |
| Poke | [MCP servers](https://poke.com/docs/mcp-servers) | Dynamic tools, per-user identity, and data isolation pattern | Official technical documentation |
| Poke | [Recipes](https://poke.com/docs/creating-recipes) | Packaged onboarding and shareable behaviors | Official product documentation |
| Poke | [Release notes](https://poke.com/docs/release-notes) | Reliability, approval, calendar, identity, and timezone failures | Official operational evidence |
| Poke | [Cognition acquisition announcement](https://cognition.com/blog/interaction) | Company-reported adoption and acquisition | Marketing claim; not independently verified |
| Orchid | [Beta announcement](https://orchid.ai/blog/orchid-beta-is-here) | Handled-versus-needs-you approval UX | Official product claim |
| Orchid | [The next app is no app](https://orchid.ai/blog/the-next-app-is-no-app) | One-thread interface thesis | Official product philosophy |
| Perplexity | [Tasks](https://www.perplexity.ai/help-center/en/articles/11521526-perplexity-tasks) | Isolated scheduled agents, retries, state, attention, and credit model | Official operational documentation |
| Perplexity | [Comet](https://www.perplexity.ai/help-center/en/articles/11172798-getting-started-with-comet) | Browser-native context and action | Official product documentation |
| Perplexity | [Comet privacy](https://www.perplexity.ai/help-center/comet/en/articles/12867415-comet-assistant-privacy-data-use) | Local context and explicit permission choices | Official privacy documentation |
| Perplexity | [Deep Research](https://www.perplexity.ai/help-center/en/articles/13600190-what-s-new-in-advanced-deep-research) | Progress, steering, and editable artifacts | Official product documentation |
| Perplexity | [Computer](https://www.perplexity.ai/cs/hub/blog/introducing-perplexity-computer) | Orchestrator and subagent product architecture | Official product claim |
| Gemini Notebook | [Developing NotebookLM](https://blog.google/innovation-and-ai/products/developing-notebooklm/) | Six-week prototype history and user-feedback approach | First-party product history |
| Gemini Notebook | [Grounding model](https://blog.google/innovation-and-ai/technology/ai/notebooklm-google-ai/) | Source-bounded answers and citations | Official product/technical disclosure |
| Gemini Notebook | [2026 rename and cloud computer](https://blog.google/innovation-and-ai/products/gemini-notebook/notebooklm-gemini-notebook/) | Product expansion and current name | Official product announcement |
| Granola | [Announcement](https://www.granola.ai/blog/announcement) | Human notes as steering, AI provenance, meeting wedge | Official product disclosure |
| Granola | [Meeting capture](https://www.granola.ai/blog/can-ai-listen-to-a-meeting-and-take-notes) | Local audio capture and retention claims | Official product/privacy claim |
| Limitless | [Product and acquisition status](https://www.limitless.ai/) | Hardware sunset, acquisition, and support status | Official company announcement |
| Limitless | [API](https://www.limitless.ai/developers/docs/api) | Lifelog structure and search | Official API documentation |
| Character.AI | [Memory](https://blog.character.ai/memory/) | Layered explicit, pinned, inferred, and compressed memory | Official product disclosure |
| Dia | [Security](https://www.diabrowser.com/security) | Scoped browser permissions, previews, and irreversible-action restrictions | Official security disclosure |
| Letta | [Letta](https://github.com/letta-ai/letta) and [co](https://github.com/letta-ai/co) | Memory blocks, background agents, filesystem, permissions, per-user pattern | Open-source implementation |
| Mem0 | [Mem0](https://github.com/mem0ai/mem0) and [evaluation](https://docs.mem0.ai/core-concepts/memory-evaluation) | Memory scopes and evaluation framework | Open-source implementation plus vendor docs |
| Honcho | [Honcho](https://github.com/plastic-labs/honcho) | Async representations over peers, sessions, and messages | Open-source implementation |
| LangChain | [Deep Agents](https://www.langchain.com/deep-agents) | Planning, filesystem, subagents, and context primitives | Official framework documentation |
| LangChain and Stripe | [Stripe Kai case study](https://www.langchain.com/blog/how-stripe-built-their-knowledge-ai-platform-on-deep-agents) | Layered harness, artifacts, virtual filesystem, dynamic skills/tools, reported adoption | Vendor/customer case study; metrics not independently verified |
| LangChain | [Context management](https://www.langchain.com/blog/context-management-for-deepagents) | Offloading, trimming, and summarization | First-party framework engineering |
| Stripe | [Projects agent controls](https://stripe.com/blog/stripe-projects-adds-new-agents-providers-developer-controls) | Scoped credentials, spend limits, and environment boundaries | Official product/engineering disclosure |
| Lilian Weng | [Agent harnesses](https://lilianweng.github.io/posts/2026-07-04-harness/) | Files, subagents, artifacts, traces, evals, and self-improvement cautions | Technical synthesis by a named researcher |
