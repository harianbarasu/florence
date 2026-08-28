Label: wayfinder:task
Type: task
Status: resolved
Blocked by: 01, 04

# Add the useful Google Workspace surface

## Question

How can Florence search, read, create, and update the useful parts of each adult's Gmail, Calendar, Drive, Docs, Sheets, Slides, Tasks, and Contacts so a school document, spreadsheet, task, or email can become completed family work instead of a link or summary the parent must finish themselves?

Directly adapt Hermes's Gmail, Calendar, Drive, Docs, Sheets, and Contacts operations, stable JSON shapes, workflow guidance, and focused tests from `skills/productivity/google-workspace/scripts/google_api.py`, `skills/productivity/google-workspace/SKILL.md`, and `tests/skills/test_google_workspace_api.py` into Florence's existing TypeScript Google adapter. Add Slides and Tasks through that same adapter and the official Google APIs because the pinned Hermes source does not implement them. Start with the concrete family workflows that use each tool; do not run Hermes's credentialed Python CLI or create a second OAuth or connector framework.

## Answer

Florence now uses one TypeScript Google Workspace adapter behind the existing Google connection and the existing foreground/durable tool lifecycle:

- Gmail can search/get messages and labels in private conversation; private durable work can send, reply, and change labels.
- Drive can search/read metadata; private durable work can create folders, share files, and move files to trash.
- Contacts can search and return the writable CONTACT source; private durable work can create or update contacts with the provider-required source etag.
- Docs can read every tab, create a document, and append to the first or a selected tab.
- Sheets can read ranges, create sheets, update with `USER_ENTERED`, and append replay-stable raw rows.
- Slides can read presentations, create them, and add title/body slides.
- Tasks can list task lists/tasks and create, update, or complete tasks.
- The existing Calendar path still owns broad personal reads and family-calendar create/update/delete. Private durable work can now list the owner's Calendars and read a selected window across persisted checkpoints.

Writes use the requesting private adult's exact active Google connection and the existing durable work row. Gmail sends and replies plus Drive-native creates derive the same semantic action key after a provider timeout; Contacts and Tasks reconcile retries through exact-content readback, and the other operations use provider-specific preflight or readback where needed. Ordinary foreground turns remain read-only for the new Workspace tools. No connector framework, policy registry, queue, runtime, migration, or generic safety layer was added.

Existing connections remain intact but need one OAuth reconnect to add Gmail modify, Drive, Tasks, and Contacts access. Drive already authorizes the Docs, Sheets, and Slides operations, so their redundant OAuth scopes and an unused broad Calendar scope were removed. Reconnect does not reset the household, retained history, links, or Family Calendar.

Direct upstream reuse:

- **Hermes Agent `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882`, `skills/productivity/google-workspace/scripts/google_api.py`: direct TypeScript adaptation** of Gmail MIME/reply behavior, Drive projections and native-file operations, People projections, Sheets `USER_ENTERED`/append behavior, and Docs indexing.
- **Hermes Agent at the same commit, `skills/productivity/google-workspace/SKILL.md`: workflow and tool-contract adaptation** for ordinary Gmail, Drive, Docs, Sheets, Calendar, and Contacts work.
- **Hermes Agent at the same commit, `tests/skills/test_google_workspace_api.py`: narrow Calendar/credential reference only.** The pinned file covers bridge credentials, Calendar `events list`, and credential-refresh persistence; it does not test Gmail or Drive behavior. Florence's Gmail/Drive provider fakes and durable retry cases are Florence-owned because the pinned Hermes suite has no equivalent assertions. The Python CLI harness itself was not imported.
- Slides, Tasks, bounded complete-or-fail pagination, multi-tab Docs, provider-error normalization, and replay reconciliation use the same adapter but are Florence TypeScript additions because the pinned Hermes Workspace skill does not implement them.

Deliberate issue-13 limits are explicit rather than overclaimed: arbitrary Drive binary/PDF download and upload were left to later external-file work. Gmail drafts, forwarding, attachments, and group-requested work through its initiating adult's Google account were subsequently completed in issue 16.

Verification: Google and API TypeScript checks pass; Biome and diff checks pass; the focused API suite covers private foreground read versus durable write, a timeout/replan with one semantic send identity, and durable Calendar list-to-selected-window checkpoints. Provider-fake checks cover Gmail reply/preflight, Drive share replay, People update metadata, nested Docs tabs and targeted append, and exact RAW Sheets append readback.
