# Research: Integration Architecture for an AI-First Personal Chief of Staff

Research date: 2026-08-04  
Status: recommended architecture and connector sequence  
Source policy: primary sources only—official API documentation, provider specifications, and platform policies

## Executive recommendation

Build one connector control plane with a separate connection record for every authorized account, an append-only ingestion queue, provider-specific cursors, a normalized source mirror, and lineage-aware derived data. Agents receive scoped tool handles; they never receive OAuth refresh tokens, API keys, webhook secrets, or bank access URLs.

Start with manual structured records, allowlisted local files, browser capture, multiple read-only Google Calendar accounts, and a private Linq conversation with the owner. Add Gmail only after accepting its restricted-scope obligations. Add finance as read-only, using Plaid Transactions plus Liabilities for the production path and manual credit-card statement import. Add health later through the new Google Health API and a native Apple HealthKit companion—not through the retiring Google Fit or Fitbit Web APIs.

The governing distinction is:

- **External canonical sources:** Gmail, Google Calendar, bank/issuer systems, local project files, X, Google Health/Fitbit, and Apple Health. Life OS mirrors their records and derives summaries, entities, embeddings, alerts, and plans.
- **Life OS canonical sources:** connection metadata, consent history, action intents, approvals, run/audit events, personal context, captured-link records and user notes, projects, and initially meals, books, restaurants, and TV.
- **Transport, not source of record:** Linq moves messages; Life OS owns the retained conversation and task state. Linq's ephemeral mode removes message content after 24 hours and explicitly tells applications that need history to persist webhook content themselves. [Linq messaging lifecycle](https://docs.linqapp.com/guides/messaging/)

## 1. Connector control plane

Each external authorization should create an independent record like:

```text
connection_id
life_os_user_id
provider
provider_subject_id
display_label
granted_scopes
credential_secret_ref
status
webhook/channel IDs and expiration
sync cursor(s)
last_success / last_error / last_reconciled
created_at / revoked_at / deletion_status
```

Use a stable provider identity, not an email address, as the key. For Google, the OpenID Connect `sub` claim is unique across Google Accounts and never reused, while an email address may change; retain email only as a display label. [Google OpenID Connect reference](https://developers.google.com/identity/openid-connect/reference)

Every Gmail or Calendar account gets its own OAuth grant, token reference, cursors, watches, account label, and policy. Ask Google to show the account chooser when adding an account, verify the returned identity, and reject accidental attempts to attach the same provider subject twice.

For unattended sync, request offline access and persist the refresh token securely. Google recommends incremental authorization, narrow scopes, CSRF protection with `state`, and offline access only when the app must act while the user is absent. [Google OAuth web-server flow](https://developers.google.com/identity/protocols/oauth2/web-server) OAuth security guidance also recommends scope/resource restriction and protected or rotated refresh tokens. [OAuth 2.0 Security BCP, RFC 9700](https://datatracker.ietf.org/doc/html/rfc9700)

Store provider client secrets, refresh tokens, Plaid access tokens, SimpleFIN access URLs, Linq bearer tokens, webhook signing secrets, and encryption keys in a secrets manager or OS keychain, encrypted at rest and referenced by opaque IDs. Google explicitly says not to commit client credentials, not to transmit tokens in plaintext, and to revoke and permanently delete tokens when no longer needed. [Google OAuth policies](https://developers.google.com/identity/protocols/oauth2/policies)

## 2. Data model: raw mirror, normalized state, derived intelligence

Keep four layers separate:

1. **Raw ingestion envelope:** provider, connection, event/request ID, received time, schema version, payload hash, and encrypted payload or blob reference.
2. **Normalized source mirror:** immutable provider IDs plus current fields, tombstones, version/etag, source timestamps, and sync lineage.
3. **Derived intelligence:** extracted text, entities, embeddings, summaries, categories, suggested tasks, inferred preferences, and freshness/provenance.
4. **Action ledger:** user intent, proposed mutation, approval, stable idempotency key, executor attempt, provider result, and verification.

Derived rows must point to every source record they depend on so revocation, deletion, correction, or re-sync can invalidate and rebuild them. A vector index or chat transcript is never the canonical copy of email, events, transactions, health samples, or files.

| Source | Canonical record | Life OS stores |
|---|---|---|
| Gmail | Gmail message/thread and labels | Provider IDs, selected mirrored content, task/relationship derivations |
| Google Calendar | Calendar event on its owning calendar | Event mirror, cross-account conflict view, derived agenda |
| Linq | Life OS transcript; Linq is delivery transport | Verified webhook event, message/attachment copy, delivery state |
| Local projects | Local filesystem | Path/file identity, content hash, index, optional explicit snapshot |
| Browser/X/articles/PDFs | Life OS capture record; origin remains authoritative for live content | URL/source ID, user note, metadata, optional lawful snapshot/blob, derived text |
| Finance | Institution/issuer, accessed through aggregator | Read-only account/transaction/liability mirror and derived budgets/alerts |
| Health | Google Health/Fitbit or Apple HealthKit | Authorized sample mirror or summaries, provenance, health-specific retention |
| Meals/books/restaurants/TV | Life OS | First-party structured record plus optional external link |

## 3. Incremental-sync contract

Treat every push or webhook as an **invalidation hint**, not as a complete or exactly-once change log:

1. Verify its signature or channel identity.
2. Durably enqueue the envelope and return success quickly.
3. Deduplicate the delivery.
4. Pull authoritative changes with the stored cursor.
5. Apply all pages and tombstones transactionally.
6. Advance the cursor only after the batch commits.
7. Run periodic reconciliation so dropped notifications cannot create permanent drift.

### Gmail

Perform an initial bounded full sync, then use `history.list` from a saved `historyId`. Gmail says history is typically available for at least a week but can be shorter; an expired or unavailable `startHistoryId` returns `404` and requires a full sync. [Gmail synchronization](https://developers.google.com/workspace/gmail/api/guides/sync)

Server push uses Google Cloud Pub/Sub. A notification identifies the mailbox and new history ID, after which the client calls `history.list`; mailbox watches must be renewed at least every seven days, Google recommends daily renewal, delivery can be delayed or dropped, and each watched user is limited to one notification per second. [Gmail push notifications](https://developers.google.com/workspace/gmail/api/guides/push)

Use `(google_sub, gmail_message_id)` as the message key and upsert labels/thread state. The Gmail message ID is immutable; the message resource also supplies `threadId`, `historyId`, and `internalDate`. [Gmail message resource](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages)

Start read-only. `gmail.readonly`, `gmail.metadata`, `gmail.compose`, and `gmail.modify` are currently restricted scopes; a public app that stores or transmits restricted-scope data on servers must pass Google's restricted-scope process and security assessment. [Gmail scope classifications](https://developers.google.com/workspace/gmail/api/auth/scopes) Do not use Gmail data to train a generalized model; Google's verification guidance applies Limited Use restrictions to raw and derived Workspace data. [Google OAuth application-use guidance](https://support.google.com/cloud/answer/13805798)

### Google Calendar

Sync each selected calendar independently. An initial list returns `nextSyncToken`; subsequent calls include the prior token and return modifications plus deleted entries. A `410 Gone` means the token was invalidated and the local mirror must be cleared and fully resynchronized. [Calendar incremental sync](https://developers.google.com/workspace/calendar/api/guides/sync)

Push channels are per user and watched resource, expire, and cannot be renewed in place; replacement channels can overlap. Notifications contain headers but not the changed event, so they must trigger an incremental pull. [Calendar push notifications](https://developers.google.com/workspace/calendar/api/guides/push)

Use `(google_sub, calendar_id, event_id)` as identity and retain `etag`, `updated`, cancellation/tombstone state, recurrence identity, organizer, and attendees. For future writes, supply a deterministic client-generated event ID; Google documents that this prevents duplicate creation after an ambiguous retry. [Creating Calendar events](https://developers.google.com/workspace/calendar/api/guides/create-events)

### Linq v3 iMessage

Use one assigned Linq number as a private Chief-of-Staff front door first. Linq v3 sends and receives iMessage, RCS, and SMS; `message.received` webhooks contain inbound content, and outbound sends accept an `idempotency_key` of up to 255 characters. [Linq sending messages](https://docs.linqapp.com/guides/messaging/sending-messages/) [Linq webhook events](https://docs.linqapp.com/guides/webhooks/events/)

Pin an explicit webhook schema version, verify the signed raw body, enqueue asynchronously, and deduplicate by `event_id`. Linq documents at-least-once delivery, up to 10 retries over roughly 25 minutes, and possible duplicates. [Linq webhook delivery](https://docs.linqapp.com/guides/webhooks/)

Persist inbound attachments immediately or copy them into the artifact store according to policy; Linq can return short-lived signed media URLs and offers an ephemeral tier that removes attachment bytes after one day. [Linq attachments](https://docs.linqapp.com/guides/messaging/attachments/)

Initially permit automatic replies only inside the owner's dedicated conversation. Sending to third parties, creating group chats, or transmitting sensitive artifacts must go through the action ledger and approval policy.

### Local project files

Allowlist explicit roots. Default to read-only indexing; never recursively index the home directory. Exclude secret/config paths, credential files, build caches, VCS internals, and user-configured patterns. Resolve symlinks before enforcing the root boundary.

Use filesystem notifications for latency and periodic scans for correctness. Identify content with an allowlisted-root ID, normalized path, file identity where available, size/mtime, and cryptographic content hash. Index text as derived data; retain the source file as canonical. A user-requested project snapshot is a distinct immutable artifact, not a silent copy of every local file.

### Browser bookmarks, X posts, articles, PDFs, and links

Begin with a user-triggered “Send to Life OS” browser extension or share target. Capture URL, canonical URL, title, MIME type, selected text, user note, source account/profile, capture time, and optional file/blob. Use temporary `activeTab` access for capture; request Chrome's broader `bookmarks` permission only if importing the bookmark tree, because that permission can read and change bookmarks. [Chrome extension permissions](https://developer.chrome.com/docs/extensions/reference/permissions-list) The Chrome bookmarks API exposes the tree and change events when that permission is granted. [Chrome bookmarks API](https://developer.chrome.com/docs/extensions/reference/api/bookmarks)

For X, prefer a pasted post URL/ID and user note. If bookmark sync is worth the cost and policy surface, the official endpoint is `GET /2/users/:id/bookmarks`, requires user authentication, and has endpoint-specific rate limits. [X Bookmarks API](https://docs.x.com/x-api/users/get-bookmarks) [X API rate limits](https://docs.x.com/x-api/fundamentals/rate-limits) Do not scrape X. If X content is cached, update or delete it when the source is edited, protected, suspended, or deleted; X's policy requires content compliance, generally within 24 hours of a request. [X Developer Policy](https://docs.x.com/developer-terms/policy)

Store an original PDF as a hashed artifact and treat extracted text, OCR, chunks, citations, and summaries as replaceable derivations. For mutable web articles, retain capture time and content hash and never imply the snapshot is the current page.

## 4. Finance: read-only recommendation and aggregator comparison

The finance connector must expose **no payment, transfer, Auth/ACH, or credential-retrieval capability** to the agent. It may read balances, transactions, liabilities, and explicitly supplied statements; a deterministic service performs ingestion and reconciliation.

| Option | Officially documented strengths | Important gaps / fit |
|---|---|---|
| **Plaid** | Transactions covers depository and credit-card transactions with up to 24 months of history, cursor-based `/transactions/sync`, added/modified/removed records, and update webhooks; Investments and Liabilities add holdings and credit-card/loan terms. [Plaid Transactions](https://plaid.com/docs/transactions/) [Plaid financial-product comparison](https://plaid.com/docs/financial-insights/) | Richest reviewed production fit, but product/institution coverage varies. Its Statements product is US-only, supports depository accounts rather than credit cards, and documents coverage of roughly 40% of US depository accounts, so it cannot be the credit-card-statement plan. [Plaid Statements](https://plaid.com/docs/statements/) |
| **SimpleFIN v2 / Bridge** | The protocol is intentionally read-only, exposes account balances and transactions, gives transaction IDs unique within an account, and v2 adds `conn_id` to distinguish multiple logins to one institution. [SimpleFIN protocol](https://www.simplefin.org/protocol.html) | Attractive for a private prototype, but it is pull-oriented and sparse: no standard liabilities, PDF statements, or dependable rewards/benefits model. The Bridge says it uses MX and disclosed a May 2026 account-mixing incident affecting up to 39 users, so it is not the default recommendation without explicit risk acceptance. [SimpleFIN Bridge security](https://beta-bridge.simplefin.org/info/security) |
| **MX Platform API** | Returns transactions across a user's members/accounts, supports provider identifiers, pagination, updated-time filters, and optional merchant, recurring, classification, and geolocation enrichment; MX also documents resource-change webhooks. [MX transactions](https://docs.mx.com/api-reference/platform-api/v20111101/reference/list-transactions) [MX webhooks](https://docs.mx.com/api-reference/nexus/overview/webhooks/) | Viable enterprise alternative or coverage fallback, but direct integration adds another commercial/provider evaluation and does not remove the need for Life OS reconciliation or a statement/rewards strategy. |
| **Mastercard Open Finance (Finicity)** | Official materials describe consumer-permissioned balances, transaction histories, investment positions, categorization, and refresh/analytics products. [Mastercard transaction data](https://www.mastercard.com/us/en/business/open-finance/solutions/data/transactions.html) | Strong enterprise alternative, but broader underwriting/analytics capabilities are unnecessary for the first personal read-only connector. |

**Recommendation:** use Plaid Link with only Transactions and, where useful, Liabilities; never initialize payment, Transfer, or Auth products. Consume `/transactions/sync`, preserve its cursor, and apply `added`, `modified`, and `removed` changes. A pending card transaction can later appear as a removal plus a newly added posted transaction, so do not treat a pending transaction ID as permanent. [Plaid transaction states](https://plaid.com/docs/transactions/transactions-data/)

Treat webhooks as hints: Plaid tells receivers to handle duplicates and out-of-order delivery and to recover through polling after missed webhooks. [Plaid webhook guidance](https://plaid.com/docs/api/webhooks/) Verify Plaid's signed webhook JWT before enqueueing it. [Plaid webhook verification](https://plaid.com/docs/api/webhooks/webhook-verification/)

For **credit-card statements**, begin with manual PDF upload or a user-forwarded attachment. Preserve the original PDF, parse a derived statement record, and reconcile its totals and transactions against the account mirror. Plaid's current PDF Statements product does not support credit-card accounts. [Plaid Statements coverage](https://plaid.com/docs/statements/)

For **rewards and benefits**, maintain a small manual ledger initially: issuer/card, points balance and as-of date, annual-fee date, credits/perks, enrollment status, and source evidence. The reviewed standardized transaction/liability schemas cover balances, credit limits, APRs, due dates, payments, and transactions—not a normalized inventory of card rewards, transfer partners, expiring credits, or benefit eligibility. [Plaid Liabilities schema](https://plaid.com/docs/api/products/liabilities/) SimpleFIN permits custom currencies such as reward points in its protocol, but availability depends on the server supplying them. [SimpleFIN protocol](https://www.simplefin.org/protocol-v1.html)

## 5. Future health connectors

Do not start a new Google Fit or legacy Fitbit Web API integration. Google says the Google Fit APIs are supported only through the end of 2026, recommends Google Health API for cloud integrations and Health Connect for mobile/on-device data, and says the legacy Fitbit Web API will stop syncing in September 2026. [Google Fit migration guide](https://developer.android.com/health-and-fitness/health-connect/migration/fit) [Google Health API overview](https://developers.google.com/health/about)

For Fitbit and Pixel Watch data, use the new Google Health API with Google OAuth. It provides unified/reconciled streams, standardized data bundles, and webhooks; all Google Health scopes are currently classified as restricted and require privacy/security review. [Google Health API](https://developers.google.com/health) [Google Health API migration details](https://developers.google.com/health/about)

Apple Health requires a native Apple-platform companion. HealthKit is an on-device repository, grants read/write permission per data type, and deliberately does not reveal whether read permission was denied; a locked device may prevent background reads. [Apple HealthKit privacy](https://developer.apple.com/documentation/healthkit/protecting-user-privacy) Life OS should request only specific read types when needed, perform local incremental queries, and upload only the minimum samples or aggregates the user elects to sync. Health data may not be used for advertising and is subject to Apple's disclosure and privacy requirements. [Apple health and fitness guidance](https://developer.apple.com/health-fitness/)

Health arrives after connector deletion, provenance, encryption, and audit behavior are proven on lower-risk data. It gets a separate retention policy and must never be exposed to unrelated workers by default.

## 6. Manual domains first

Keep meals, books, restaurants, and TV first-party and manual initially. Use small typed records with `occurred_at`, optional rating/status, freeform note, source/capture link, attachments, and provenance; let the Chief of Staff turn conversational captures into proposed records that the user can correct.

Avoid early third-party connectors for these domains. The first product question is whether the structured views and capture loop are useful, not whether every service can be synchronized. External IDs can be added later without changing Life OS ownership of the personal record.

## 7. Idempotency and deduplication

Use unique constraints and upserts at the provider boundary:

| Stream/action | Stable key or rule |
|---|---|
| Gmail | `(connection_id, message_id)`; history IDs are cursors, not business IDs |
| Calendar | `(connection_id, calendar_id, event_id)` plus etag/version |
| Linq webhook | `(subscription_id, event_id)` |
| Linq send | Life OS `action_intent_id` copied to Linq `idempotency_key` |
| Browser capture | User capture ID; use canonical URL + content hash only for duplicate suggestion, not automatic deletion |
| Local file | Root ID + normalized path/file identity + content hash/version |
| SimpleFIN | `(connection_id, account_id, transaction_id)`; the protocol guarantees transaction-ID uniqueness only within an account. [SimpleFIN transaction schema](https://www.simplefin.org/protocol.html) |
| Plaid | `(item_id, account_id, transaction_id)` while honoring modified/removed arrays |
| Health | Provider source ID + data type + provider record ID/version; retain originating device/app |
| Calendar create | Deterministic Life OS action ID encoded as valid client-generated event ID |
| Gmail send | Create and retain a draft first; gate the single draft-send transition in the action ledger because Gmail does not document a general send idempotency key. Gmail supports direct send or send-from-draft. [Gmail sending guide](https://developers.google.com/workspace/gmail/api/guides/sending) |

Webhook handlers must be safe under duplicate, concurrent, and out-of-order delivery. A cursor row should be locked per connection, and the cursor plus mirrored changes should commit atomically.

## 8. Revocation, deletion, and disconnect

“Disconnect” is a workflow, not a boolean:

1. Mark the connection `revoking` and stop new sync/action leases.
2. Stop provider watches or delete webhook subscriptions where applicable.
3. Revoke/remove provider authorization.
4. Delete credentials, cursor/channel secrets, raw mirror data, and derived data reachable through lineage.
5. Preserve only minimal consent/audit evidence permitted by policy, without message or financial content.
6. Show completion or a retryable residual-state warning to the user.

Google instructs apps to revoke unneeded tokens and permanently delete them, and its OAuth endpoint supports token revocation. [Google OAuth policies](https://developers.google.com/identity/protocols/oauth2/policies) For Plaid, call `/item/remove`; this invalidates the Item access token and associated tokens, and Plaid recommends it for account disconnect/offboarding. [Plaid Item removal](https://plaid.com/docs/api/items/) Plaid also says data associated with revoked OAuth access should be deleted unless retention is still necessary for the user's requested service. [Plaid OAuth consent guidance](https://plaid.com/docs/link/oauth/)

For Linq, delete or disable webhook subscriptions and, when required, delete retained messages/attachments through the API; deleting a Linq record does not unsend a message already delivered to a recipient. [Linq webhook subscriptions](https://docs.linqapp.com/guides/webhooks/subscriptions/) [Linq FAQ](https://docs.linqapp.com/guides/resources/faq/)

Disconnecting a local root removes its index and derived data but never deletes the user's source files. Deleting a captured item removes its stored blob and derivations; deleting a user note is independent from whether the external URL still exists.

## 9. Phased connector order

### Phase 0 — connector kernel and owned data

- Build connection records, secret references, ingestion queue, cursor transactions, source lineage, tombstones, action ledger, deletion jobs, and audit UI.
- Ship manual meals/books/restaurants/TV, allowlisted read-only local projects, and user-triggered URL/PDF capture.

### Phase 1 — scheduling and the private conversational front door

- Add multiple Google Calendar accounts read-only, with per-account labeling, incremental sync, push renewal, and a merged agenda.
- Add Linq v3 inbound plus replies only in the owner's dedicated chat; persist the transcript at webhook receipt.

### Phase 2 — email and richer capture

- Add Gmail per account, initially read-only and narrowly filtered to the product's demonstrated needs; complete the restricted-scope/privacy work before broader use.
- Add optional Chrome bookmark import. Keep X as paste/share-by-URL unless the official Bookmarks API clearly justifies its access cost and compliance burden.

### Phase 3 — read-only finance

- Add Plaid Transactions and optionally Liabilities, with no money-movement products or tools.
- Add manual credit-card statement PDFs and the manual rewards/benefits ledger.
- Keep SimpleFIN as an explicitly accepted prototype fallback and MX/Mastercard as coverage alternatives, not simultaneous default connectors.

### Phase 4 — controlled external actions

- Add Calendar creation/update using deterministic IDs and approval policy.
- Add Gmail draft creation, then explicit send approval; add third-party Linq messaging only with recipient/purpose policy, preview, and audit.
- Finance remains read-only.

### Phase 5 — health

- Add Google Health API for Fitbit/Pixel cloud data and a native HealthKit companion for Apple Health.
- Request data types incrementally, isolate health capabilities, and implement health-specific export/deletion before enabling agent-derived coaching.

This order maximizes daily utility while making identity separation, sync correctness, deletion, and safe side effects prove themselves before financial and health data enter the system.
