# Outlook, Gmail, and multi-calendar integration

Date: 2026-08-06

## Decision

Outlook is a **moderate integration**, not a new product architecture. Microsoft Graph exposes Outlook.com and Microsoft 365 mail and calendars through the same API. Florence should support it, but first generalize a Google connection from one indivisible Gmail-and-Calendar bundle into:

```text
person
  -> connected account (provider + stable provider subject)
      -> capability: mail
      -> capability: calendar
          -> selected calendars and disclosure mode
```

The product default should be:

- personal/family account: offer Mail + Calendar;
- work account: Calendar on, Mail off;
- work calendar: availability-only by default, with full event details as an explicit choice;
- mail access: a separate, later grant when the user has a concrete reason to enable it.

This is both the least-invasive product and the cleanest implementation of multiple personal and work accounts.

## What Energy contributes to the UX

The locally inspected Energy 0.7.25 Tools screen presents Gmail, Google Calendar, Outlook, and Outlook Calendar as separate capability cards. Each opens a short capability explanation and one connect action. See the existing [Energy product-reference memo](./energy-parent-agent-product-reference-2026-08-06.md).

Florence should adopt the clarity but not Energy's tool marketplace:

1. Sources should be organized around **people and email addresses**, not a catalog of integrations.
2. Each account card should show Mail and Calendar independently.
3. Each capability should show an intelligible state: not connected, authorizing, initial sync, live, needs attention, paused, or disconnected.
4. Initial progress should be explicit: “Calendar ready,” “Recent mail scanning,” and “Older mail processing in the background.”
5. “Connect another account” should remain visible after every successful connection.
6. The iMessage prompt should explain the immediate outcome and send a private setup link; detailed scopes, privacy, corrections, and disconnection belong in the mobile web control plane.

Unlike Energy's single-user work context, every Florence source remains attributable to the individual who authorized it. Raw mail never becomes household-visible merely because the derived fact is useful to the household.

## Microsoft Graph feasibility

One multi-tenant Microsoft Entra application can use delegated access for personal Microsoft accounts and work or school accounts. Long-lived access uses the `offline_access` scope. Calendar-only can begin with a least-privilege read permission, and mail can be requested later through incremental consent. [Microsoft delegated access](https://learn.microsoft.com/en-us/graph/auth-v2-user), [Microsoft Graph permissions](https://learn.microsoft.com/en-us/graph/permissions-reference)

The major adoption risk is organizational policy rather than API capability. Even where a delegated permission does not inherently require administrator consent, a Microsoft 365 tenant can restrict user consent. Florence must treat “your employer blocked this connection” as a normal connection state rather than a product error. [Graph permission overview](https://learn.microsoft.com/en-us/graph/permissions-overview), [Microsoft consent policies](https://learn.microsoft.com/en-us/entra/identity/enterprise-apps/manage-app-consent-policies)

For ongoing synchronization, Graph supports change notifications for messages and events. Notifications are a wake-up signal, while delta queries maintain durable state. Subscriptions expire and must be renewed; lifecycle notifications signal reauthorization, removed subscriptions, and missed notifications. [Outlook change notifications](https://learn.microsoft.com/en-us/graph/outlook-change-notifications-overview), [lifecycle notifications](https://learn.microsoft.com/en-us/graph/change-notifications-lifecycle-events), [message delta](https://learn.microsoft.com/en-us/graph/api/message-delta?view=graph-rest-1.0), [calendar-event delta](https://learn.microsoft.com/en-us/graph/delta-query-events)

Graph supports shared and delegated Outlook calendars subject to the user's actual permissions. There is an important limitation: delegated shared-calendar permissions do not support change-notification subscriptions for items in those shared folders. Those calendars require periodic delta reconciliation or, in an enterprise-controlled deployment, application permissions. [Shared Outlook calendars](https://learn.microsoft.com/en-us/graph/outlook-get-shared-events-calendars), [sharing and delegation](https://learn.microsoft.com/en-us/graph/outlook-share-or-delegate-calendar)

Graph can throttle with HTTP 429 and `Retry-After`; Microsoft recommends change tracking and change notifications instead of repeated full scans. Florence's durable job and retry model already fits this. [Graph throttling guidance](https://learn.microsoft.com/en-us/graph/throttling)

## Google implications

Google also supports calendar-only first and later incremental authorization for Gmail. A work Google account should request Calendar read scopes without Gmail. Gmail access should be a separate user decision because Gmail read scopes are Restricted and can require OAuth verification and a security assessment for server-side use. Workspace administrators can also permit Calendar while blocking Gmail. [Google OAuth web-server flow](https://developers.google.com/identity/protocols/oauth2/web-server), [Gmail scopes](https://developers.google.com/workspace/gmail/api/auth/scopes), [OAuth verification](https://support.google.com/cloud/answer/13463073?hl=en), [Workspace app controls](https://support.google.com/a/answer/7281227?hl=en&p=app_access_apps)

Every Google account should run its own OAuth flow and be keyed by the stable OpenID Connect `sub`, not its mutable email address. [Google OpenID Connect](https://developers.google.com/identity/openid-connect/openid-connect)

Gmail initial ingestion enumerates messages and then retrieves accepted messages and attachments. Ongoing sync uses mailbox history IDs. Gmail push uses Cloud Pub/Sub, must be renewed at least every seven days, and can be delayed or dropped; periodic reconciliation remains required. [Gmail synchronization](https://developers.google.com/workspace/gmail/api/guides/sync), [Gmail push notifications](https://developers.google.com/workspace/gmail/api/guides/push)

Google Calendar requires a CalendarList sync and a separate Events sync token for each selected calendar. Push notifications contain no event data and only trigger incremental synchronization; channels expire and must be renewed. [Calendar incremental synchronization](https://developers.google.com/workspace/calendar/api/guides/sync), [Calendar push notifications](https://developers.google.com/workspace/calendar/api/guides/push)

Calendar event attachments generally provide metadata or links. Accessing the underlying Google Drive file is a separate permission decision; a Calendar grant should not silently expand into Drive. [Google Calendar event attachments](https://developers.google.com/workspace/calendar/api/guides/create-events)

## Florence's synchronization contract

For every enabled account capability:

1. authorize the smallest requested scope;
2. record the provider's stable account subject and the exact granted capabilities;
3. discover mail folders or calendars;
4. run a bounded, recent-first initial sync;
5. establish push/change notifications where supported;
6. persist provider cursors or delta links;
7. renew subscriptions before expiration;
8. periodically reconcile even when pushes appear healthy;
9. recover from expired cursors with a bounded resync;
10. expose per-capability health without exposing raw private content.

Mail and calendar must converge into provider-neutral artifacts while retaining provenance:

- `mail_message` and `mail_attachment` with account, folder, thread, sender, recipients, timestamps, and provider remote IDs;
- `calendar_event` with account, calendar, recurrence identity, time zone, participants, availability, disclosure mode, and provider remote IDs;
- one canonical event identity when the same invitation appears in both email and calendar;
- encrypted raw content and separately scoped derived family facts;
- tombstones and revisions so deletions and corrections propagate.

## Current-code impact

The Florence repository already supports multiple Google subjects per person, encrypted credentials, provider-scoped source identities, per-calendar privacy grants, generic encrypted jobs, and per-resource sync cursors. Outlook therefore fits the persistence direction.

The Google precursor was implemented in the August 2026 sync release: OAuth attempts retain their exact account profile, integrations expose independent Mail and Calendar capabilities, personal and work accounts have different least-privilege defaults, and sync/recovery/UI health are capability-aware. The remaining provider-neutral work before Microsoft is added is:

- make the active provider-subject uniqueness invariant provider-generic;
- generalize Google-specific authorization assurance naming;
- normalize Gmail labels and Outlook folders into a provider-neutral admission shape;
- dispatch calendar work by provider;
- permit Outlook mail/calendar in source-bridge rules;
- add concurrency-safe encrypted credential rotation.

Production-worthy Microsoft calendar-only support is roughly 8–10 logical areas and 14–17 production files. Full Outlook mail-and-calendar parity is roughly 9–11 logical areas and 17–20 production files. This is a bounded follow-on, but it should not be implemented as a thin adapter on top of the current inseparable Google bundle.

## Recommended sequence

1. Validate the shipped Google capability model with personal and work accounts.
2. Add Microsoft OAuth and calendar-only Graph sync.
3. Pilot with a real employer-managed Microsoft 365 account to expose admin-consent behavior.
4. Add Outlook mail only after the calendar path is reliable and a pilot user needs mail from that account.

This sequence gives Florence the multi-calendar context families need without unnecessarily asking for work-inbox access or letting provider-specific assumptions harden further.
