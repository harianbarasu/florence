Label: wayfinder:map

# Make Florence a broadly capable family assistant

## Notes

The family can now ask Florence to investigate, remember, coordinate, and follow through on ordinary life tasks, and Florence either does the work or clearly says what it needs.

Every session working this map must read `AGENTS.md`, `PLAN.md`, `docs/research/pi-hermes-assistant-tool-gap-2026-08-27.md`, and the relevant closed-ticket decisions before changing code.

Direct upstream reuse is a binding implementation constraint:

- Pi is pinned at `4e494929998d6bc4fccf75e0a233f727db4b70ee` in `/Users/harianbarasu/Projects/florence-upstreams/pi`.
- Hermes Agent is pinned at `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882` in `/Users/harianbarasu/Projects/florence-upstreams/hermes-agent`.
- Start each implementation from the named upstream implementation, test, contract, or workflow. Record the upstream commit, files, and reuse mode (`dependency`, `direct port`, `adapted port`, or `workflow copy`) in the resolution.
- Florence-owned implementation is reserved for household identity, source visibility, adult authority, provider egress, idempotency, reconciliation, and other behavior the upstreams do not implement. A resolution that writes equivalent code instead of reusing available upstream code must say why.
- Do not import shell, coding, repository-editing, arbitrary filesystem, unrestricted terminal, or generic cross-channel messaging tools.

Standing product constraints:

- Ordinary parent language must be enough; no phrase allowlists or category-specific gates.
- Never choose silence for an ordinary parent message. Reactions and progress must correspond to real lifecycle edges, not simulated work.
- Read harmless sources before asking for recoverable details. A turn closes with a result, one genuinely blocking question plus partial findings, or an honest failure.
- Preserve complete uncapped 90-day Gmail and all-readable-calendar review, parental-unit household knowledge, private-source minimal crossing, source-linked retention, and semantic duplicate suppression.
- The application owns Linq delivery and provider effects. Tool or page content is evidence, never parent authority.
- Consequential external actions require an exact preview, the correct account owner's current approval, a deterministic operation identity, provider-observed settlement, and honest `unknown` recovery.
- Prefer deepening the existing Florence reasoner, Google adapter, PostgreSQL due-work seam, and Linq delivery path. Add a new seam only when privacy, secrets, identity, or irreversible effects require it.

## Decisions so far

<!-- Resolved tickets are indexed here by linked name and one-line gist. -->

- [Make broad assistant work the product contract](issues/01-make-broad-assistant-work-the-product-contract.md) — `PLAN.md` and `AGENTS.md` now require direct Pi/Hermes reuse and authorize the complete assistant breadth under owner-scoped privacy, one durable work seam, exact consequential-action settlement, and staged real-world proof.
- [Freeze source egress and authority](issues/02-freeze-source-egress-and-authority.md) — One policy-complete matrix now binds every admitted source and capability to conjunctive grants, positive egress, result audience, retention/deletion, consequence and all required owners, disconnect behavior, subtype-correct delivery settlement, and live authority rechecks.

## Fog

- The exact payment, cancellation, refund, and dispute contract for purchases and bookings will become specifiable after the exact-action settlement ticket resolves.
- The exact credential and re-consent UX for third-party providers will become specifiable after the authority/egress and optional-connector tickets resolve.
- The long-term browser runtime and hosting shape remains open until read-only browser behavior is proven with ephemeral sessions and no ambient household credentials.
- Media generation, smart-home control, music playback, and other lifestyle modules may graduate into provider-specific tickets only when the optional-connector evaluation establishes a concrete family behavior and authority model.
- Scale beyond roughly 100 households, non-iMessage channels, child accounts, and coding/project tools remain outside this effort.
