# Product Research Notes

Research date: 2026-06-05.

## Ollie

Ollie is text-first and family-first. The useful primitives are:

- Shared family context through a group text.
- Assistant lives in SMS/MMS/RCS/iMessage, not a separate app parents must
  remember to open.
- Calendar and inbox watching in the background, with the assistant keeping the
  family in sync.
- Calendar sync and event creation from messy inputs like school flyers, PDFs,
  photos, and plain text.
- Morning and weekend briefings that include calendar, weather, meals, and a
  small number of reminders.
- Meal planning and groceries connected to schedule context.
- Public copy emphasizes no app download, no spam, STOP support, and a support
  link. Florence should keep HELP/SUPPORT/STOP as deterministic transport
  behavior that works even when ordinary replies are paused.
- Gentle accountability and emotional support, especially for overloaded or new
  parents.
- Privacy posture: adult-directed, text assistant, no model training on user
  data, STOP/HELP support, and deletion on request.
- Adult-use posture matters: parents/guardians manage household information;
  children should not independently operate the service.

Important product constraint: the assistant should reduce mental load rather
than give parents another inbox. This means selective surfacing is core product
behavior, not a nice-to-have.

Current public pages reviewed:

- [Ollie homepage](https://ollie.ai/)
- [Ollie family calendar](https://ollie.ai/family-calendar)
- [Ollie privacy policy](https://www.ollie.ai/privacy-policy/)
- [Ollie terms](https://ollie.ai/terms-of-service)

## Town

Town's most useful backend concepts for Florence:

- Tasks preserve context and track follow-through.
- Need-to-Know separates "the assistant saw this" from "the user should see
  this now."
- Routines have trigger, instructions, tools, and mode.
- Webhook-triggered routines are authenticated, idempotent, and treat inbound
  payloads as untrusted data instead of trusted instructions. Florence's typed
  source-ingest API should stay on this side of the line for families.
- Routines can be stock or custom, and can be created by describing the desired
  automation in plain language.
- Approval modes create progressive trust: read-only, approval-required, then
  autonomous.
- Morning briefings are high-signal, deduplicated, and time-sensitive rather
  than comprehensive.
- The assistant can operate across web app, email, iOS, Slack, and WhatsApp, but
  keeps one shared context. Florence should translate this to one shared
  household context across iMessage, admin endpoints, workers, and future tools.
- Town's onboarding reads recent email/calendar to build context, but it frames
  this as learning, not immediate action. Florence's first-sync backfill rule is
  the family-safe version of that idea.

Current public pages reviewed:

- [Town homepage](https://www.town.com/)
- [Town docs](https://www.town.com/docs)
- [Town assistant](https://www.town.com/docs/using-town/assistant)
- [Town web app](https://www.town.com/docs/using-town/web-app)
- [Town email](https://www.town.com/docs/using-town/email)
- [Town routines](https://www.town.com/docs/routines)
- [Town triggers](https://www.town.com/docs/reference/triggers)
- [Getting to know Town](https://www.town.com/learn)

## Linq

Linq gives Florence the transport layer:

- Partner API v3 base URL: `https://api.linqapp.com/api/partner/v3`.
- Bearer token authentication.
- Existing chat replies go to `/chats/{chat_id}/messages`.
- Webhooks are signed with HMAC-SHA256 over `{timestamp}.{raw_payload}`.
- Webhook handlers should reject stale timestamps and process asynchronously.
- Group chats are first-class but read receipts and typing indicators are not
  available in groups.
- Idempotency keys should be attached to outbound messages to avoid duplicate
  sends on retries.
- Inbound webhook retries must also be idempotent. Florence uses Linq
  `message.id` to skip duplicate inbound side effects.

## Hermes

Hermes should remain the agentic backend rather than the application framework.
Florence provides the household-specific policy, state, and transport context;
Hermes handles open-ended reasoning inside that bounded context.

## Poke

Poke validates the same messaging-native direction but pushes harder on memory
and extensibility:

- Poke now emphasizes Apple Messages and rich actions, with the assistant as a
  familiar contact rather than another app. TechCrunch reported on June 4, 2026
  that Apple approved Poke for Messages for Business, which matters because
  Apple required live-support readiness, clear AI-agent identification, and
  iMessage-native link previews/buttons.
- The current docs list Apple Messages, Telegram, WhatsApp, and RCS as supported
  surfaces, with email, calendar, reminders, web search, and integrations as
  core actions.
- Poke's docs expose help as a first-class path with email support. Florence
  should have a configurable human-support contact reachable from the same
  thread.
- The homepage frames Poke around fitting into existing messaging habits and
  proactively using integrated services plus memory at the right time.
- Poke Recipes set up integrations, automations, required onboarding context,
  prefilled first-message behavior, and shareable workflows.
- Poke Kitchen is the creator/admin surface for recipes, API keys, and
  integration templates.
- The public recipes surface frames setup as one-tap templates across categories
  like Calendar, Email, Productivity, Scheduling, Students, To-dos, and Travel.
  Florence should copy the setup pattern, not the category sprawl.
- Florence's setup/status replies should therefore end with one concrete next
  text action, so onboarding feels like installing the next useful recipe from
  inside iMessage rather than reading an operator checklist.
- Proactivity comes from integrated services plus memory "right on time."
- The developer surface is MCP: external tools inherit Poke's context and are
  composed with built-in email/calendar/reminder primitives.
- The Poke API accepts context-rich JSON messages from external automations and
  routes them into the assistant conversation.
- In Florence, API-triggered context must become household-scoped source items or
  pending actions; it should not be forwarded directly to Hermes with broad
  access to family context.
- Poke's API doc says any JSON body is forwarded as agent context. Florence
  should deliberately not copy this default for families: every external trigger
  needs a typed schema, household binding, idempotency key, and Need-to-Know
  classification before Hermes sees it.
- MCP requests include user identity headers so external tools can enforce
  per-user access, rate limits, and audit logging.
- Release notes show several time-related fixes: timezone verification,
  calendar events created in the wrong timezone, and reminders created in the
  wrong timezone. This directly validates Florence's strict timekeeper.
- Privacy positioning matters: maximum privacy by default, no training on user
  data in that mode, and account deletion controls.

For Florence, the takeaway is that memory must be product infrastructure, not
just chat logs. Each family needs an isolated memory namespace with provenance,
correction, deletion, and typed recall. Future integrations should use a small
tool boundary similar to MCP rather than special-casing every app. The minimum
Poke-inspired shape for Florence is message-native operation, recipes-like
source setup, proactive but sparse surfacing, strong privacy defaults, and a
tool/API boundary for external triggers.

Important adaptation: Poke is primarily individual-user oriented. Florence is a
two-parent household assistant, so recipes, memory, integrations, and API
messages need household roles. Helpers can participate, but durable setup facts,
privacy settings, source rules, connected accounts, and memory deletion belong
to parents.

Current public pages reviewed:

- [Poke homepage](https://poke.com/)
- [Poke docs](https://poke.com/docs)
- [Poke recipes directory](https://poke.com/recipes)
- [Poke recipes](https://poke.com/docs/creating-recipes)
- [Poke API](https://poke.com/docs/api)
- [Poke MCP servers](https://poke.com/docs/mcp-servers)
- [Poke managing integrations](https://poke.com/docs/managing-integrations)
- [Poke release notes](https://poke.com/docs/release-notes)
- [Poke FAQ](https://poke.com/faq)
- [TechCrunch: Apple approves Poke for Messages for Business](https://techcrunch.com/2026/06/04/apple-approves-poke-as-the-first-ai-agent-on-its-messages-for-business-platform/)

## Florence Product Implications

- The primary household surface is a shared iMessage thread.
- The backend must model people in that thread, not just messages. Florence now
  treats the first sender as the founding parent, requires an invite or
  confirmation for the second parent, and keeps later unconfirmed senders as
  helpers.
- Reminder ownership should use that member model. The first simple version is
  shared-thread owner labeling (`Alex: pack cleats`) before adding more complex
  direct-message routing.
- Source imports are allowed to be comprehensive, but user interruption is not.
  Need-to-Know is the product gate between ingestion and texting.
- First connected-source sync is backfill. Florence imports the batch and
  interrupts only for urgent actionable items so parents do not get a backlog
  dump right after connecting an account.
- Hermes receives only the active household's members, memories, recent
  transcript, and upcoming reminders. This preserves SaaS isolation while still
  letting Hermes reason with useful context.
- External automations should use a Florence API boundary that looks more like
  typed source ingestion than Poke's arbitrary JSON-to-agent forwarding. The
  family-safe sequence is validate, bind to one household, classify, store, then
  either stay quiet, ask for parent approval, or surface a short text.
- The Apple Messages lesson is operational as much as product: the assistant
  should identify itself clearly, provide a human-support escape hatch before
  scale, and prefer native message affordances over sending raw URLs or verbose
  app-like copy.
