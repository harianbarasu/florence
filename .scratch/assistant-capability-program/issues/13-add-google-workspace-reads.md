Label: wayfinder:task
Type: task
Status: open
Blocked by: 01, 02, 04

# Add the useful Google Workspace surface

## Question

How can Florence search, read, create, and update the useful parts of each adult's Gmail, Calendar, Drive, Docs, Sheets, Slides, Tasks, and Contacts so a school document, spreadsheet, task, or email can become completed family work instead of a link or summary the parent must finish themselves?

Directly adapt Hermes's Gmail, Calendar, Drive, Docs, Sheets, and Contacts operations, stable JSON shapes, workflow guidance, and focused tests from `skills/productivity/google-workspace/scripts/google_api.py`, `skills/productivity/google-workspace/SKILL.md`, and `tests/skills/test_google_workspace_api.py` into Florence's existing TypeScript Google adapter. Add Slides and Tasks through that same adapter and the official Google APIs because the pinned Hermes source does not implement them. Start with the concrete family workflows that use each tool; do not run Hermes's credentialed Python CLI or create a second OAuth or connector framework.
