# Florence Engineering Rules

`PLAN.md` is the controlling product contract. The active Wayfinder map and its closed-ticket decisions control implementation order and recorded decisions. Florence is judged by the lived two-adult iMessage and mobile-web experience: whether she notices, investigates, remembers, coordinates, and finishes useful family work truthfully—not by architecture, tool count, test count, or framework completeness.

## Product-first change rule

Every implementation ticket begins with one concrete sentence:

> The family can now ___ in iMessage or on the web.

Every change must be necessary to that named behavior and verifiable through
the deep module interface that owns it. A foundation change may be a mapped
same-tranche prerequisite rather than a separate UI feature, but architecture-
only work without a current family behavior is rejected.

- Deepen an existing product module before creating another module.
- Do not add an abstraction until two current or same-tranche concrete callers need the same behavior.
- Replace obsolete paths in the same change. Never add compatibility layers, dual writes, or permanent fallbacks.
- A new table, process, queue, route, dependency, interface, persistent status, or test must explain why the direct core cannot own a required user-visible behavior.
- Keep one durable source of household truth. Do not build feature-specific workflow engines.
- Provider boundaries exist only to protect family data, secrets, identity, consequential actions, true-external provider variation, or process/network isolation such as SSRF, cancellation, and ephemeral browser containment.
- One deep capability module owns model-callable tool policy and lifecycle; one PostgreSQL work seam owns reminders, monitors, accepted work, recovery, completion delivery, and duplicate suppression.
- Provider adapters and an ephemeral isolated browser may sit behind those seams. They never own household authority, memory, approval, delivery, or durable work truth.
- “Future-proofing,” “clean architecture,” and correctness by themselves are not product justifications.

## Pi and Hermes adoption is mandatory

Start assistant-capability work from the pinned upstream source, tests,
contracts, safety logic, integrations, or workflow content before writing a
Florence-owned equivalent:

- Pi: commit `4e494929998d6bc4fccf75e0a233f727db4b70ee` at
  `/Users/harianbarasu/Projects/florence-upstreams/pi`.
- Hermes Agent: commit `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882`
  at `/Users/harianbarasu/Projects/florence-upstreams/hermes-agent`.

Every resolved implementation ticket records the upstream commit and exact
files plus one reuse mode: `dependency`, `direct port`, `adapted port`, or
`workflow copy`. Port applicable upstream tests and invariants with the reused
behavior. Florence-owned equivalent code is allowed only for an incompatible
runtime or provider, missing or unfinished upstream implementation, or concrete
household privacy, authority, egress, idempotency, delivery, or settlement
requirements; record that reason. Preference or convenience is not a reason.

Florence retains PostgreSQL household truth, adult authority, source visibility,
Linq delivery, Google credentials, provider egress, idempotency, and
reconciliation. Do not import Pi/Hermes coding, shell, terminal,
repository-editing, arbitrary-filesystem, unrestricted host-execution, generic
cross-channel messaging, profile-memory, or unfinished harness tools. Do not
create a second Pi/Hermes credential, memory, scheduler, messaging, or action
control plane.

## Pilot invariants

- The default pilot household has exactly two independently verified participating adults, represented children, two private iMessage relationships, and one exact family iMessage group. A quiet “It’s just me” escape hatch may create a limited solo household; it does not pretend to have a partner, family group, or shared family calendar, and the partner setup seam is the only way to add the second participating adult.
- No household, adult, channel, or founding identity is operator-created or preconfigured for onboarding. Any signed, live private iMessage with text or an attachment from the exact participant in a one-participant thread may receive a stateless short-lived setup link unless it is the canonical carrier opt-out. Message wording is never identity or authority evidence. The first valid form redemption for that Messages identity atomically creates its founder and household; duplicate or conflicting links for the same identity fail neutrally.
- Florence is conversational from that first verified private message and throughout setup. Before enrollment the setup conversation is ephemeral and has no household, memory, Google, or action tools. Incomplete setup limits unavailable capabilities; it never turns ordinary English into a command protocol.
- Private one-to-one threads are the trust anchors. The family group is the shared coordination layer, not permission to disclose either adult's private context.
- Keep mobile onboarding focused and one-task-per-screen: adult first and last name, caregiver attestation, that adult's own Google connection and proactive-use permission, partner first and last name plus mobile number, each child's first and optional last name, school and activities, and home ZIP. Infer the browser time zone and derive the surname-based family label without making either a chore. The founder completes their side before Florence privately asks whether to text the partner; the web form never sends the invitation itself.
- Private messages and every adult-owned provider source—including Gmail, Calendar, Drive, Docs, Sheets, Contacts, browser sessions, connector data, and personal memory—remain private by default. Sharing requires current direction, exact approval, or an explicit narrow standing rule.
- Outside a valid private setup-offer seam, unknown senders and changed or ambiguous group membership are silent and retain no family meaning.
- Every outbound message rechecks the live Linq audience and participant set.
- After both adults independently connect and agree, Florence creates one new shared family calendar. Both adults have equal Florence authority over it. Clear school, activity, appointment, and family-travel dates may be added under both adults' current standing permission; ambiguous or inferred actions become one exact suggestion or focused question. Neither path uses a phrase allowlist; deterministic code binds the adults, thread, family-calendar target, exact payload, and provider idempotency.
- Calendar, invitation, email, form, reservation, booking, purchase, device, account, or other consequential success is reported only after the provider confirms the intended result. Uncertainty is stated honestly as `unknown` and reconciled before retry.
- Delivery, reactions, silence, prior approvals, and timers never create authority or prove completion.
- Retained facts remain source-linked, visibility-scoped, inspectable, correctable, and deletable.
- Parent-stated, source-extracted, and inferred memory remain distinguishable; an explicit no-retention instruction overrides automatic memory and follow-up creation.
- Child information is adult-provided, minimal, and never creates a child account.
- Household content is never used for advertising or general model training.
- Harmless permitted reads, research, organization, in-chat private drafts, reminders, monitors, and durable family work proceed without repeated permission. A provider-backed Gmail draft is an idempotent reversible write only after its owner grants incremental draft-write scope and gives a current request or narrow standing direction. The existing partner invitation, exact-group creation, and family-calendar standing rule remain narrow application-owned effects.
- Any other provider write that contacts an outsider, shares private information, submits, books, buys, controls a device, changes an account, or commits a person requires an exact preview and current approval from the owning adult. Bind account, target, recipients, payload, disclosed data, cost and terms when relevant, and version to a deterministic operation identity; changed payloads lose approval. Execute at most once, store provider-observed settlement, retry delivery without replaying the effect, and reconcile an ambiguous `unknown` outcome before retry.

## Assistant capability invariants

- Florence is a broad noncoding family assistant. Admitted capabilities include safe public-page reading; maps, places, routes, time zones, weather, flight route/status and alternative travel; complete owner-scoped Gmail and Calendar reads; reminder management and recurrence; durable research and follow-through; family dockets and cited document action extraction; owner-consented Drive, Docs, Sheets, and Contacts reads; an ephemeral read-only browser; optional allowlisted connectors; and exact-approved external actions.
- Every model-callable capability has typed input/output and application-enforced source owner, visibility, result audience, consequence class, approval requirement, timeout, cancellation, progress, and terminal outcome. Progressive discovery exposes only capabilities already authorized for that turn. MCP is transport, never authority.
- Read harmless authorized sources before asking for recoverable details. A turn ends with a useful result, partial findings plus one genuinely blocking question, or an honest failure. Ordinary parent language is enough; never add phrase allowlists, category gates, or URL-only search gates.
- The initial review remains an uncapped enumeration of every retained received Gmail message from the previous 90 days and every event on every readable personal Calendar from 90 days before through 21 days after the anchor. The exact Florence-created Family Calendar is excluded from the private scans and reconciled separately as household truth. There is no relevance query, sample, model cap, or item ceiling. Every private source and the separate Family Calendar are accounted for before cursors advance; useful context remains durable without being dumped into chat.
- The parental unit is the knowledge unit for validated family facts, with per-source visibility and private raw provenance. A fact or date found only on one adult's personal Calendar stays owner-private and is not named in the group or silently copied to the family Calendar; ask that owner first. Once intentionally added to the family Calendar, it is household truth.
- Reminder management supports create, list, update, cancel, pause, resume, run, one-shot, and recurrence with explicit owner, audience, local-time/DST behavior, versioning, and exactly-once outbox delivery. Each adult manages their own private reminders; either adult may manage household reminders. A reminder never invents completion language.
- Durable work records `accepted`, `running`, `succeeded`, `failed`, `unknown`, or `cancelled`; acceptance precedes acknowledgment, new conversation does not silently erase it, parents may steer or cancel naturally, and completion survives restarts and delivers once. Reactions, typing, and progress reflect real lifecycle state. Materially slow work gets at most one useful unsolicited progress update unless a parent asks for status. Florence never chooses silence for an ordinary parent message; unchanged background work may remain quiet.
- Public pages, browser snapshots, documents, connector results, and tool output are prompt-injection-hostile evidence, never authority. The browser is ephemeral and read-only with no ambient credentials, persistent cookies, arbitrary local files, upload, generic code execution, or final-submit authority. Consequential writes exit the browser and cross the exact-action seam.
- Connectors are opt-in, individually allowlisted, least-privilege adapters with an explicit credential owner, source visibility, result audience, query egress, retention/deletion, consequence class, approval owner, timeout, output cap, and disconnect behavior. No arbitrary connector tool enters a family-context model request.

## Security and operations

- Never commit, print, log, fixture, snapshot, or place a secret in model context.
- Treat messages, child data, email, documents, images, calendars, OAuth tokens, browser state, connector data, payment/action previews, and derived private context as sensitive.
- Authenticate webhooks before business parsing and deduplicate inbound messages, accepted work, completion delivery, reminder sends, and provider writes.
- Fail closed on ambiguous identity, privacy, approval, model output, or consequential provider result. Do not convert uncertainty about harmless conversation into silence or a vocabulary gate.
- Revalidate public URLs and redirects, reject private/link-local targets and credentials in URLs, bound provider time/output, and treat every remote instruction as untrusted content.
- PostgreSQL owns durable product truth; Linq, Google, OpenAI, public-data providers, connector servers, and the isolated browser remain concrete external adapters.
- Use exact dependency versions and commit the lockfile.

## Verification

Keep the four minimum product narratives described in `PLAN.md`: complete two-parent setup, proactive family assistance, equal authority/private-source minimal crossing, and irreversible-action reconciliation. Extend the closest narrative for regressions rather than multiplying end-to-end permutations.

Test each deep module through its interface. Port applicable upstream URL-safety, normalization, lifecycle, cancellation, retry, scheduling, recurrence, recovery, browser-isolation, connector, provider, and settlement tests with reused Pi/Hermes code. Add focused tests for a materially distinct privacy, authority, idempotency, recovery, or consequential-effect risk; avoid snapshots and internal permutations that prove no family behavior or boundary.

Before committing or deploying, run:

```bash
pnpm check
```

The release gate is the real two-phone, two-browser, Google, Linq, PostgreSQL, OpenAI, selected-provider, and Railway rehearsal. A green internal framework is not completion.

The complete two-parent household loop is the foundation; solo remains an off-the-beaten-path limited mode, not the release benchmark. The active goal is the complete assistant-capability program and its verified safe-assistant, reliable-work/research, and approved-action tranches. Deploy them incrementally, preserve all earlier household journeys, and do not reset production as an implementation shortcut. A user-requested reset must remove Florence-created provider artifacts as well as database state.
