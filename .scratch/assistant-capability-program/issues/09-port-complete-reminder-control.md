Label: wayfinder:task
Type: task
Status: open
Blocked by: 01, 04

# Port complete reminder control

## Question

How can either parent create, list, update, cancel, pause, resume, run, and recur reminders in ordinary language with correct ownership, audience, time zone, daylight-saving behavior, schedule versioning, and exactly-once due delivery?

Adapt Hermes's action-oriented contract and state transitions from `tools/cronjob_tools.py`, `cron/jobs.py`, and the focused schema, stale-claim, recurring, one-shot, completion, and rearm tests under `tests/cron/`. Keep storage in Florence's PostgreSQL due-work seam and delivery in Florence's outbox; do not import Hermes's scheduler, local files, session runtime, or coding cron jobs.
