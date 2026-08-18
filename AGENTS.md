# Florence Engineering Rules

`PLAN.md` is the controlling product contract. Florence is judged by the two-adult iMessage and web experience, not by architecture, test count, or framework completeness.

## Product-first change rule

Every implementation change begins with one concrete sentence:

> The family can now ___ in iMessage or on the web.

If that sentence is not visible and testable by the pilot family, reject the change.

- Deepen an existing product module before creating another module.
- Do not add an abstraction until two current concrete callers need the same behavior.
- Replace obsolete paths in the same change. Never add compatibility layers, dual writes, or permanent fallbacks.
- A new table, process, queue, route, dependency, interface, persistent status, or test must explain why the direct core cannot own a required user-visible behavior.
- Keep one durable source of household truth. Do not build feature-specific workflow engines.
- Provider boundaries exist only to protect family data, secrets, identity, or irreversible actions.
- “Future-proofing,” “clean architecture,” and correctness by themselves are not product justifications.

## Pilot invariants

- The pilot is one household, exactly two independently verified participating adults, represented children, two private iMessage relationships, and one exact family iMessage group.
- No household, adult, channel, or founding identity is operator-created or preconfigured for onboarding. While the pilot database is empty, any signed, live private iMessage with text or an attachment from the exact participant in a one-participant thread may receive a stateless short-lived setup link unless it is the canonical carrier opt-out. Message wording is never identity or authority evidence. The first valid form redemption atomically creates the founder and household; competing links then fail neutrally, and subsequent unknown senders stay silent.
- Florence is conversational from that first verified private message and throughout setup. Before enrollment the setup conversation is ephemeral and has no household, memory, Google, or action tools. Incomplete setup limits unavailable capabilities; it never turns ordinary English into a command protocol.
- Private one-to-one threads are the trust anchors. The family group is the shared coordination layer, not permission to disclose either adult's private context.
- Keep mobile onboarding focused and one-task-per-screen: adult first and last name, caregiver attestation, that adult's own Google connection and proactive-use permission, partner first and last name plus mobile number, each child's first and optional last name, school and activities, home ZIP, and an editable surname-derived family label. Infer the browser time zone without making it a chore. The founder completes their side before Florence privately asks whether to text the partner; the web form never sends the invitation itself.
- Private messages, Gmail, and personal memory remain private by default. Sharing requires a current direction, approval, or an explicit narrow standing rule.
- Outside the empty-database setup-offer seam, unknown senders and changed or ambiguous group membership are silent and retain no family meaning.
- Every outbound message rechecks the live Linq audience and participant set.
- After both adults independently connect and agree, Florence creates one new shared family calendar. Both adults have equal Florence authority over it. Clear school, activity, appointment, and family-travel dates may be added under both adults' current standing permission; ambiguous or inferred actions become one exact suggestion or focused question. Neither path uses a phrase allowlist; deterministic code binds the adults, thread, family-calendar target, exact payload, idempotency, and provider proof.
- Calendar or email success is reported only after provider reconciliation proves the intended result. Uncertainty is stated honestly.
- Delivery, reactions, silence, prior approvals, and timers never create authority or prove completion.
- Retained facts remain source-linked, visibility-scoped, inspectable, correctable, and deletable.
- Parent-stated, source-extracted, and inferred memory remain distinguishable; an explicit no-retention instruction overrides automatic memory and follow-up creation.
- Child information is adult-provided, minimal, and never creates a child account.
- Household content is never used for advertising or general model training.
- The only pilot provider writes are the explicit partner invitation, exact family-group creation, and family-calendar changes covered by current standing permission or exact approval. External email and messages may be drafted but not sent.

## Security and operations

- Never commit, print, log, fixture, snapshot, or place a secret in model context.
- Treat messages, child data, email, documents, images, calendars, OAuth tokens, and derived private context as sensitive.
- Authenticate webhooks before business parsing and deduplicate inbound messages and provider writes.
- Fail closed on ambiguous identity, privacy, approval, model output, or provider proof. Do not convert uncertainty about harmless conversation into silence or a vocabulary gate.
- PostgreSQL owns durable product truth; Linq, Google, and OpenAI remain concrete external adapters.
- Use exact dependency versions and commit the lockfile.

## Verification

Keep exactly the minimum scenario suite described in `PLAN.md`: the whole household journey, the privacy/authority boundary, and irreversible-action reconciliation. Extend those narratives for regressions; do not create helper, schema, component, reducer, snapshot, or permutation tests. A fourth automated test needs a genuinely different dangerous boundary and explicit user approval.

Before committing or deploying, run:

```bash
pnpm check
```

The release gate is the real two-phone, two-browser, Google, Linq, and Railway rehearsal. A green internal framework is not completion.

The active goal is the complete two-parent household loop. Do not deploy another founder-only partial product. Reset production only after founder and partner onboarding, the exact family group, the Florence-created family calendar, the combined proactive briefing, durable monitoring/discovery, and the existing three release narratives are internally green.
