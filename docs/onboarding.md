# Household Onboarding

Florence should feel useful from the iMessage thread, not from an admin wizard.
The setup flow is therefore text-native and derived from household state.

## Commands

- `setup`
- `setup status`
- `help setup`
- `help sources`
- `help calendar`
- `help memory`
- `help privacy`
- `help reminders`
- `my name is Sam`
- `invite partner +15555550101` (parent-only)
- `confirm partner +15555550101` (parent-only)
- `our child is Maya` (parent-only)
- `our kids are Maya and Leo` (parent-only)
- `set timezone America/New_York` (parent-only)
- `remind Alex tomorrow at 8am to pack cleats`
- `add soccer practice tomorrow at 5pm to calendar` (parent-only; household calendar)
- `connect google` (parent-only)
- `always tell me about permission slips` (parent-only)
- `mute newsletters` (parent-only)
- `source review` (parent-only)
- `handoff` (parent-only)
- `data summary` (parent-only)
- `delete my data` then `confirm delete household data` (parent-only)
- `support`

## Readiness Checklist

A household is marked ready for a pilot when Florence has:

- A founding parent plus an invited or confirmed second parent.
- Names for both parents.
- At least one child profile.
- At least one connected or imported email/calendar source.
- At least one source preference.

Readiness is intentionally derived, not manually toggled. If a parent pauses
memory or removes source rules later, the checklist can reflect that current
state.

The first sender in a new household becomes the founding parent. Later unknown
senders are helpers until a parent invites them through Linq or confirms them
with `confirm partner +15555550101`. Helpers may text in the thread and identify
themselves with `my name is ...`, but they cannot change durable household setup
details. Child profiles, timezone, partner invites, source connections, source
rules, privacy settings, memory writes, and pending-action approvals are
parent-only.

## Guided Next Action

`setup`, `setup status`, and `household status` should always make the next
useful text obvious. Florence still shows the readiness checklist, then ends
with one command-like action such as:

- `Text 'my name is Sam' so I know what to call you.`
- `Text 'invite partner +15555550101' with your partner's number.`
- `Text 'confirm partner +15555550101' if your partner is already in the thread.`
- `Text 'our kids are Maya and Leo' with your child or children's names.`
- `Text 'connect google' to add Google Calendar and Gmail.`
- `Text 'always tell me about permission slips' so I know what deserves a text.`

This is the Florence version of a recipe-led setup flow: parents can complete
setup entirely from iMessage, one small text at a time.

`support`, `human`, and `talk to a human` are always available as transport
commands. They return the configured Florence support contact without invoking
Hermes and still work after a parent pauses the household with `stop`.

`delete my data` is also deterministic and parent-only. Florence asks for the
exact confirmation text `confirm delete household data` before removing the
household's records from the environment. The confirmation only works after a
recent deletion request within `FLORENCE_DATA_DELETION_CONFIRMATION_TTL_MINUTES`.

`data summary` is deterministic and parent-only. It gives a count-level
inventory of household records without raw message or email bodies.

Topic-specific help commands are also deterministic transport commands. They
stay short enough for iMessage and work before Hermes or setup is complete.

## Household Handoff

Parents can text `handoff`, `what's open?`, or `pending approvals` to get a
read-only snapshot of unresolved household work. Florence lists active approval
codes plus upcoming reminders for the next week. The view is parent-only because
it can expose external actions awaiting approval.

## Two-Parent Thread

If the first parent starts in a one-to-one thread, they can text
`invite partner +15555550101`. Florence creates a new Linq group chat with the
first parent, the invited partner, and Florence's assigned sending number. After
Linq returns the new group chat id, Florence makes that group the household's
primary thread and keeps the old direct chat as an alias to the same household.
Retryable outbound deliveries, pending reminders, pending approvals, and unused
OAuth states are retargeted to the new group thread.

If both parents are already visible in a shared thread, the founding parent can
text `confirm partner +15555550101`. Florence promotes that sender to parent
without requiring a new Linq-created group chat.

## Why This Matters

The assistant cannot behave like a competent nanny if it only has a chat ID. It
needs to know who the parents are, who the children are, which sources matter,
and what should be quiet. The setup checklist makes those gaps visible without
adding a separate app surface.

Known household member names also let Florence label reminder ownership in the
shared thread, such as `Alex: pack cleats`, instead of dropping every reminder
into an undifferentiated household pile. If a member has not shared a name yet,
Florence uses a role label such as `unnamed parent` instead of displaying their
phone number in reminders or setup status.

Parents can also add household calendar items directly from iMessage. These are
internal Florence calendar context for agenda, prep, and briefings. They are not
Google Calendar writes until a future approved external-calendar executor exists.

## Connected Sources

When Google OAuth is configured, a parent can text `connect google` in the
household thread. Florence replies with a short-lived Google authorization link,
then sends a confirmation back to the same iMessage thread after the callback
completes.

The dev import and sync endpoints remain useful for smoke tests, but the product
setup path should stay text-native.
