# Connected Sources

Florence treats Gmail, Calendar, and future providers as connected accounts that
belong to one household. Provider fetchers should never send raw content to chat.
They normalize candidates, advance a cursor, and let Need-to-Know decide what is
worth surfacing.

Provider fetchers should also send concise summaries. Florence caps title,
body/summary, and sender fields before policy scoring, and the public source API
rejects oversized submissions with `413`.

## Account Boundary

- Each connected account belongs to one household.
- Accounts are keyed by `provider` and `external_account_id` within that
  household.
- Google accounts use the OpenID `sub` claim as `external_account_id`; the
  email address is only a display label because it can change.
- Each account stores its latest sync `cursor` and `last_synced_at_utc`.
- Each account stores sync failure state: `sync_failure_count`,
  `last_sync_error`, and `retry_after_utc`.
- Provider tokens live in `connected_account_tokens`, encrypted with
  `FLORENCE_TOKEN_ENCRYPTION_KEY`. They are never returned by HTTP inspection
  routes or inserted into Hermes context.
- Imported source external IDs are scoped by connected account id so two accounts
  with the same provider message id do not collide.

## Google OAuth

Florence implements Google's web-server OAuth flow for connected Gmail and
Calendar sources:

1. Configure `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`,
   `GOOGLE_REDIRECT_URI`, and `FLORENCE_TOKEN_ENCRYPTION_KEY`.
2. Have a parent text `connect google` in the household thread.
3. Florence replies with a short-lived `authorization_url`.
4. Google redirects back to `GET /oauth/google/callback`.
5. Florence validates the one-time `state`, exchanges the code, fetches
   userinfo, upserts the connected account, and stores the token payload
   encrypted.
6. Florence sends a confirmation back to the same iMessage thread.

The callback saves the connection before sending the iMessage confirmation. If
Linq is temporarily unavailable, Florence still returns a successful browser page,
records the failed confirmation in outbound delivery health, and the worker
retries the `oauth:` delivery with the same idempotency key. If a parent
disconnects Google before that retry succeeds, Florence cancels the stale
confirmation instead of texting that Google is connected.
If the household is deleted before Google redirects back, the one-time callback
fails closed and does not exchange the Google code or recreate the household.
If the household is stopped when Google redirects back, Florence saves the
connection but does not text the paused iMessage thread.
If Google or token encryption fails during the public callback, Florence returns
a generic browser error instead of echoing provider error text, tokens, email
addresses, phone numbers, or configured secrets.

The default scopes are `openid`, `email`, Gmail read-only, and Calendar
read-only. Florence requests offline access so the provider can refresh access
tokens for background source sync.

## Sync Flow

1. Resolve the household from the iMessage chat.
2. Upsert the connected account.
3. Normalize email/calendar payloads into typed `SourceItem` records.
   Date-only calendar events are interpreted in the household timezone before
   UTC storage so all-day school events stay on the parent's local calendar day.
4. Store each source item idempotently.
5. Run Need-to-Know with household source preferences.
6. On the first sync for a connected account, treat the batch as backfill:
   store it, advance the cursor, and only interrupt immediately for urgent
   actionable items and recent high-signal school schedule changes without a
   parsed event time. Non-urgent planning items and requested low-signal matches
   wait for later syncs or a controlled daily briefing slot.
7. Surface only relevant items and, when appropriate, propose an approved
   reminder action.
8. Advance the account cursor even if the batch contains only duplicate or quiet
   items.
9. Reset sync failure state after a successful provider sync.

Backfill is quiet, not discarded. If Need-to-Know would have surfaced an item but
the initial-sync rule held it back, Florence marks it as `initial_sync_backfill`
and lets the next daily briefing include it once when it is still relevant.
No-time items must also be observed recently before they can interrupt the
household; older no-time items are stored quietly for review.
Parents can also text `source review` to see counts and short title-only samples
of what Florence texted or kept quiet, then tune future behavior with
`always tell me about ...`, `mute ...`, `mute this sender`, or
`mute this domain`.

## Linq Attachments

Parents can send images or documents in the iMessage thread. Florence currently
treats those as source items when Linq includes attachment metadata:

- `image` and photo-like parts become `flyer` source items.
- Other non-text parts become `document` source items.
- A parent caption is used as the source title/body and goes through
  Need-to-Know.
- Bounded extracted text or descriptions supplied on the media part are used as
  the source summary, and Florence derives a due time when the text contains one.
- Media-only attachments with no extracted text are stored quietly and
  acknowledged with a request for one short line of context.
- Florence does not persist raw attachment URLs or media bytes in this build.

OCR and file extraction should be added behind the same typed source boundary:
extract bounded text or a title/body/event time, then feed that summary into
`ingest_source_item`, `_ingest_normalized_source_item`, or the Linq attachment
metadata path. Do not forward raw media payloads directly to Hermes.

## Provider Boundary

Provider fetchers implement a small adapter contract: given a connected account
and the current time, return normalized email/calendar payloads plus the next
cursor. The runner checks active accounts for households that have not texted
`stop`, dispatches to the matching provider, then calls the same
`sync_connected_sources` service method used by the dev HTTP surface.

The default `florence-worker` loop runs this source sync every
`FLORENCE_SOURCE_SYNC_INTERVAL_SECONDS` seconds and sends any surfaced messages
through Linq with their source-generated idempotency keys. Set the interval to
`0` when source polling is owned by a separate scheduler.

If a provider raises an exception, Florence records the failure on the connected
account and does not advance the cursor. The runner skips that account until
`retry_after_utc`. Backoff starts at 5 minutes and doubles, capped at 12 hours.

The built-in Google provider reads the encrypted connected-account token,
refreshes it when needed, then fetches:

- Recent Gmail `INBOX` message metadata/snippets.
- Upcoming events from the authenticated user's primary Google Calendar.

It still returns normalized candidates only. Florence's household policy, source
storage, and iMessage behavior stay provider-neutral.

## Current HTTP Surface

- `POST /api/source-items` imports one typed source item from a trusted external
  automation. It requires `FLORENCE_SOURCE_INGEST_API_KEY`, `chat_id`,
  `source_type`, `title`, and `external_id`. The `external_id` is required for
  idempotency. The `chat_id` must already belong to a known household; the
  public ingest API does not create households. This endpoint stores and
  classifies source items; it does not accept agent instructions or arbitrary
  JSON-to-Hermes forwarding. Submitted `title`, `body`, and `sender` fields must
  fit the source size boundary in `docs/source-policy.md`. If Linq delivery
  fails, reposting the same `chat_id`/`source_type`/`external_id` recovers
  the retryable outbound without creating another source item or reminder
  approval.
- `POST /dev/sync-sources` imports one batch for one connected account.
- `POST /dev/oauth/google/start` starts Google authorization for operator smoke
  tests; parent-facing setup should use `connect google`.
- `GET /oauth/google/callback` completes Google authorization.
- `GET /dev/connected-accounts/{chat_id}` lists household connected accounts and
  cursors for an existing household chat.

Parents can revoke Google source access from iMessage with `disconnect google`.
Florence marks the household account disabled, deletes the encrypted token row,
and stops polling Gmail/Calendar. Source sync jobs do not reactivate disabled
accounts; the same Google account becomes active again only after a new
parent-initiated OAuth callback. Retryable source deliveries from the disconnected
account are canceled. That command is still accepted after `stop`.

`/dev/*` endpoints are local smoke-test/operator surfaces and should be guarded
with `FLORENCE_ADMIN_API_KEY` or disabled once a real admin UI exists.
`/api/source-items` uses a separate source-ingest key so trusted automation can
keep running even when dev endpoints are disabled. The public Google callback is
protected by a short-lived one-time state value and generic failure responses.

## Primary References

- [Google OAuth web server flow](https://developers.google.com/identity/protocols/oauth2/web-server)
- [Google OpenID Connect userinfo and `sub`](https://developers.google.com/identity/openid-connect/openid-connect)
- [Gmail messages list/get](https://developers.google.com/workspace/gmail/api/guides/list-messages)
- [Google Calendar events list](https://developers.google.com/calendar/api/v3/reference/events/list)
