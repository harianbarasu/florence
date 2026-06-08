# Timekeeping

Florence should never guess its way into stale reminders. All reminder parsing,
delivery, and briefing scheduling is anchored to the household timezone.

## Reminder Parsing

- Relative times like `in 20 minutes` are calculated from the current household
  local time.
- Calendar phrases like `tomorrow at 8am`, `next Friday at 5pm`, and `June 12`
  resolve in the household timezone.
- Bare 1-12 clock times like `tomorrow at 8` are treated as ambiguous; Florence
  asks directly for AM/PM instead of guessing. 24-hour times like
  `tomorrow at 17` are accepted.
- Daypart phrases use explicit local anchors:
  - `morning`: 8:00 AM
  - `afternoon` and `after school`: 3:00 PM
  - `evening`: 6:00 PM
  - `tonight` and `night`: 7:00 PM
- If a parsed reminder would land in the past, Florence asks for clarification
  instead of silently rolling it forward.

## Calendar Event Parsing

Parent-added household calendar events use the same parser and household
timezone rules as reminders. `add soccer practice tomorrow at 5pm to calendar`
creates internal Florence calendar context for agenda, prep, and briefings.
Bare times like `tomorrow at 5` still ask for AM/PM, and past times are refused.
This command does not write to Google Calendar.

Connected calendar providers must preserve the household-local date. Google
all-day events arrive as date-only values, so Florence interprets those dates at
household-local midnight before converting to UTC. A June 6 no-school day should
stay June 6 for a Los Angeles household, not drift to the prior evening.

## Delivery

The worker sends due reminders only inside the configured grace window. Reminders
older than that window are marked expired instead of being sent late.

Daily briefings run at most once per household local day after the configured
local briefing time, and only inside the configured briefing delivery grace
window. Florence skips the briefing when there are no reminders or relevant
source items, so an empty morning tick does not consume the day if a useful
held-back item appears later inside that window. After the window closes,
Florence skips the briefing instead of sending stale morning context later in
the day.

If a parent texts `stop`, the worker skips that household for reminder delivery
and stale cleanup until a parent texts `start`. A reminder due during the pause
can still send after restart if it is inside the delivery grace window; otherwise
it expires instead of arriving late. A daily briefing missed while stopped only
sends after restart if the restart happens inside the briefing delivery window.

Parents can clear pending reminders before delivery with `done ...` or
`cancel reminder ...`. Florence only updates one matching reminder; if the text
matches multiple active reminders, it asks the parent to be more specific instead
of guessing.

After Florence sends a reminder, a parent can reply `done` by itself. Florence
marks the recent sent reminder as handled only when there is one clear match; if
several reminders were sent recently, it asks for the item name instead.

If a reminder starts with the name of a known household member, Florence keeps
that person as the reminder owner. For example, `remind Alex tomorrow at 8am to
pack cleats` stores `pack cleats` as the task and labels the shared-thread
reminder as for Alex. This is owner labeling only; Florence still sends through
the household thread until direct per-person delivery is intentionally added.
If a reminder is associated with an unnamed member, Florence uses a role label
such as `unnamed parent` rather than exposing the member's phone number.

## On-Demand Agenda

When a parent asks `what's on deck today?`, Florence uses the household timezone
and only includes reminders and relevant source items due before the next local
midnight. Tomorrow's reminders stay out of today's agenda. Source items are shown
by title and time only, not full email bodies. This on-demand view is read-only:
source items can still appear later in the day even if they already appeared in
the morning briefing.

When someone asks `what should we prep for tomorrow?` or `tomorrow prep`,
Florence uses the next household-local calendar day. Today's items and the day
after tomorrow stay out. Source items are still title-and-time only, so parents
get the practical prep list without a pasted email body.
