# Source Policy

Florence stores connected email and calendar candidates, then interrupts the
household only when the item is timely and actionable. Household preferences add
a correction loop on top of the default policy without turning the product into
a rules dashboard.

## Default Flow

- Normalize each connected item into a `SourceItem`.
- Bound source title, body, and sender fields before policy scoring; external
  automation must send a concise summary, not a raw email/PDF dump.
- Suppress events that are already stale.
- Surface urgent or upcoming action items.
- Surface high-signal school schedule changes, such as no-school days or early
  dismissal, even when the importer did not extract a reliable event time, as
  long as the source itself was observed recently.
- Store low-signal newsletters, promotions, recaps, and similar items quietly.
- Include surfaced items in daily briefings once when still relevant.
- For actionable future items, offer to add a reminder through the parent
  approval rail.

## First Sync Backfill

The first sync for a newly connected account is treated as backfill. Florence
imports the batch and advances the cursor, but it only texts the household for
urgent actionable items and recent high-signal school schedule changes.
Non-urgent planning items, low-signal requested topics, and historical context
stay stored for review and future briefings. Backfill items that Need-to-Know
would otherwise have surfaced can appear once in the next daily briefing if
still relevant. This avoids the "I connected email and got a pile of texts"
failure mode without losing useful setup context.

## Household Feedback

Parents can tune the source policy from the shared thread:

- `always tell me about <phrase>` records an `always_surface` preference.
- `mute <phrase>` records a `mute` preference.
- After Florence texts a source item, `mute this sender` or `mute this domain`
  records a mute rule from that item's email sender metadata.
- After Florence texts a source item, `always tell me about this sender` or
  `always tell me about this domain` records an always-surface rule from that
  item's email sender metadata.
- `not useful`, `not important`, `too noisy`, or `don't show this` applies a
  mute preference using the most recently surfaced source item's title.
- `more like this`, `useful`, or `good catch` applies an always-surface
  preference using the most recently surfaced source item's title.
- `source preferences` lists active source preferences for the household.

Source preference commands, status, and feedback-derived rules are parent-only.
Helpers can see source texts that arrive in the shared thread, but they cannot
view or change the household's source-rule settings. Florence also omits source
preferences from Hermes context on helper turns; parent turns still include them
so Hermes can reason with parent-controlled source policy.
Preference matching handles simple singular/plural variants, so a parent can
say `mute newsletter` and Florence will also keep `newsletters` quiet.

The policy applies stale-event suppression first. Mute preferences then win
before default keyword scoring. Always-surface preferences are a fallback
upgrade: a parent can ask to see low-signal items like `spirit wear`, but the
rule does not downgrade a permission slip or deadline that would already be
classified as urgent or upcoming actionable work. Florence still will not revive
events from the past.

Feedback is also written to an audit table so pilot review can inspect what
parents corrected without reading all raw email/calendar content.

Household-requested low-signal items do not automatically create reminders.
Florence only suggests reminder actions for source items classified as urgent or
upcoming actionable work.
High-signal schedule changes without a reliable event time are texted without a
reminder approval, because Florence should not guess the due time.
If the source has no reliable event time and was observed outside the no-time
recency window, Florence stores it quietly instead of interrupting the household.
Daily briefings are also quiet by default: Florence does not send an empty
briefing just to say nothing is due.

## Isolation

Preferences are scoped by household. A phrase muted or requested in one family
does not affect any other family. This is the same SaaS boundary used by durable
memory, pending actions, reminders, and connected-source storage.

## Source Size Boundary

Need-to-Know only reads bounded source fields:

- Title: 160 characters.
- Body/summary: 2000 characters.
- Sender/calendar label: 240 characters.

The public `/api/source-items` endpoint rejects larger submitted fields with
`413` so trusted automations learn to summarize first. Internal provider
normalization applies the same caps before storing and classifying candidates.

## Current HTTP Surface

- `GET /dev/source-review/{chat_id}` shows stored/surfaced/suppressed counts and
  recent surfaced and kept-quiet items. The texted/surfaced count is based on
  delivery-confirmed source messages; pending or failed source-surface messages
  remain visible through delivery health instead of being treated as seen.
- `GET /dev/source-preferences/{chat_id}` lists active household source
  preferences.

The `/dev/*` routes remain local smoke-test surfaces until a real authenticated
dashboard exists.

## Text Controls

- `source review` shows parents the same counts in iMessage with only short
  title samples, not full email bodies.
- `always tell me about ...` and `mute ...` tune future surfacing.
- `mute this sender` and `mute this domain` tune from the last surfaced
  item's sender metadata, not from raw email content.
- `not useful` and `more like this` tune from the last surfaced source item.

Helpers cannot run `source review`; connected-source titles are parent-only.
