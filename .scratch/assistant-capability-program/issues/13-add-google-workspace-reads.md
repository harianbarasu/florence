Label: wayfinder:task
Type: task
Status: open
Blocked by: 01, 02, 04

# Add Google Workspace reads

## Question

How can each adult independently consent to let Florence search and read their Drive, Docs, Sheets, and Contacts for a current private task, with source-aware retention/deletion and no shadow document store, while exposing only minimum authorized conclusions to the household?

Port capability coverage, stable JSON shapes, and focused tests from Hermes's `skills/productivity/google-workspace/scripts/google_api.py` and `tests/skills/test_google_workspace_api.py` into Florence's existing TypeScript Google adapter. Do not run Hermes's credentialed Python CLI or create a second OAuth policy plane.
