Label: wayfinder:task
Type: task
Status: open
Blocked by: 04

# Complete Google reads in conversation

## Question

How can a parent ask Florence to inspect relevant Gmail attachments and any or all personal calendars in ordinary conversation, using the provider behavior Florence already has, without new OAuth scope, attachment retention, a second Google runtime, or private-source leakage?

Adapt the output contracts and coverage behavior from Hermes's `skills/productivity/google-workspace/scripts/google_api.py`, `skills/productivity/google-workspace/references/daily-brief.md`, and `tests/skills/test_google_workspace_api.py`, but deepen Florence's existing Google adapter and background attachment/all-calendar paths rather than importing Hermes credentials or its Python CLI.
