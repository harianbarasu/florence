# Linq Partner API v3: outbound receipt recovery

Research date: 2026-08-06

Status: first-party contract review; no authenticated requests were made
Source policy: Linq's current API reference, guides, and official TypeScript SDK documentation only

## Bottom line

Linq does expose a recovery read for an outbound message: retain the message UUID returned by the
send and call `GET /api/partner/v3/messages/{messageId}`. A successful send response is only an
accepted/queued result, not proof of device delivery. Florence should combine the message UUID,
the `X-Trace-ID` response header, versioned delivery webhooks, and periodic GET reconciliation.
There is no documented endpoint that retrieves a receipt by webhook `event_id` or by `trace_id`.

## Exact outbound contract

For a known Linq chat, the send endpoint is:

```http
POST https://api.linqapp.com/api/partner/v3/chats/{chatId}/messages
Authorization: Bearer ...
Content-Type: application/json

{
  "message": {
    "parts": [{ "type": "text", "value": "..." }],
    "idempotency_key": "<stable Florence effect id>"
  }
}
```

`idempotency_key` is a field inside the nested `message` body, **not an HTTP header**, and is capped
at 255 characters. Reusing a processed key returns the original response rather than sending a
second message. If the original message was deleted or an ephemeral message expired, reusing the
key returns `404` and does not resend it. [Send API reference](https://docs.linqapp.com/api/resources/chats/subresources/messages/methods/send/)
[Idempotency guide](https://docs.linqapp.com/guides/messaging/sending-messages/#idempotency)

The documented `200` body is:

```json
{
  "chat_id": "<chat UUID>",
  "message": {
    "id": "<message UUID>",
    "created_at": "<timestamp>",
    "delivery_status": "pending"
  }
}
```

The durable provider identifiers available immediately are therefore `chat_id` and `message.id`.
The send response schema permits any of the message-status enum values, although its example is
`pending`. Every API response also has an `X-Trace-ID` header; that trace ID is propagated into the
later webhook envelope and is the request-lifecycle correlation key. [Send API reference](https://docs.linqapp.com/api/resources/chats/subresources/messages/methods/send/)
[Debugging and trace correlation](https://docs.linqapp.com/guides/platform/debugging/)

There is a small documentation inconsistency: the migration guide and current success schema say a
successful response has no `trace_id` body field and exposes it through `X-Trace-ID`, while one
debugging paragraph says the response body and header carry it. Code should depend on the header and
only tolerate a body field if present. [V2-to-V3 response format](https://docs.linqapp.com/guides/resources/migration-v2-to-v3/#response-format)

## Recovery GET

The status/read endpoint is:

```http
GET https://api.linqapp.com/api/partner/v3/messages/{messageId}
```

It returns the message UUID, `chat_id`, `delivery_status`, `updated_at`, `sent_at`, `delivered_at`,
`read_at`, `service`, and message content. The documented status enum is:

```text
pending | queued | sent | delivered | received | read | failed
```

[Retrieve-message API reference](https://docs.linqapp.com/api/resources/messages/methods/retrieve/)

The published API has `GET /v3/webhook-events`, but that only lists subscribable event-type names.
No first-party reference reviewed documents an event-instance/receipt lookup by `event_id`,
`webhook-id`, or `trace_id`. The supported recovery handle is the outbound `message.id`.
[Webhook-events API](https://docs.linqapp.com/api/resources/webhook_events/)

## Delivery webhook contract

Florence should pin webhook version `2026-02-03`. Every payload has this envelope:

```json
{
  "api_version": "v3",
  "webhook_version": "2026-02-03",
  "event_type": "message.delivered",
  "event_id": "<event UUID>",
  "created_at": "<timestamp>",
  "trace_id": "<request trace>",
  "partner_id": "<partner>",
  "data": {}
}
```

Relevant `data` shapes in that version are:

| Event | Correlation and state fields |
| --- | --- |
| `message.sent` | `data.id` (message UUID), `data.chat.id`, `data.idempotency_key`, `direction: "outbound"`, `sent_at`; `delivered_at` and `read_at` are null |
| `message.delivered` | Same message-shaped payload and IDs; `sent_at` and `delivered_at` are set |
| `message.read` | Same message-shaped payload and IDs; `sent_at`, `delivered_at`, and `read_at` are set |
| `message.failed` | `data.message_id`, `data.chat_id`, numeric `code`, `reason`, and `failed_at` |

Use `event_id` to deduplicate deliveries and the provider message UUID to update the outbound effect.
`trace_id` is useful as a second correlation path and for support. SMS/MMS does not emit
`message.delivered` or `message.read`; it still emits `message.sent` and hard failures.
[Webhook event schemas](https://docs.linqapp.com/guides/webhooks/events/)
[Webhook OpenAPI models](https://docs.linqapp.com/api/resources/webhooks/)

Webhook delivery is at least once. Linq waits 10 seconds, makes up to 10 attempts over roughly 25
minutes with exponential backoff and jitter (capped at 10 minutes), and retries HTTP `5xx`, `429`,
connection timeout, and connection refusal. It does not retry other `4xx`, DNS failures, or invalid
hosts. A receiver must verify the signed raw body, respond quickly, and be idempotent on `event_id`.
[Webhook delivery guarantees](https://docs.linqapp.com/guides/webhooks/#delivery-guarantees)

## Terminality: documented facts and operational interpretation

Linq publishes the enum and event meanings but does **not** publish a formal terminal/nonterminal
transition table or a maximum time from `pending` to a final state. The classification below is the
safe operational interpretation for Florence:

| Status | Florence interpretation |
| --- | --- |
| `pending`, `queued` | Nonterminal; accepted but not confirmed sent. Keep reconciling. |
| `sent` | Nonterminal for iMessage/RCS delivery; it means sent from the Linq line, not delivered to the device. For SMS/MMS it is the strongest positive transport signal because those protocols produce no delivery/read receipt. |
| `delivered` | Delivery objective satisfied; it may still advance to `read`, so it is not the last possible provider state. |
| `read` | Terminal success for the delivery lifecycle. Read receipts may be disabled, so absence of `read` is not failure. |
| `failed` | Failure candidate, **not universally hard-terminal**. Linq explicitly says that for error `4001` with reason “Message delivery failed,” a later `message.delivered` for the same message ID can still arrive in rare cases. |
| `received` | Inbound-message state, not an outbound completion state. |

[Message event meanings](https://docs.linqapp.com/guides/webhooks/events/#message-events)
[Error 4001 late-delivery caveat](https://docs.linqapp.com/error/codes/4xxx/4001/)

The `message.failed` webhook currently documents delivery codes `3007`, `4001`, and `4005`. Because
the GET resource exposes only `delivery_status` and not the failure code/reason, Florence must retain
the failure webhook details when available. A missed webhook followed by GET `failed` cannot be
classified as a particular failure cause from the documented GET response alone.
[Webhook OpenAPI failure model](https://docs.linqapp.com/api/resources/webhooks/)

## Recommended Florence recovery rule

1. Persist the outbound intent and one stable Linq `idempotency_key` before making the request.
2. On `200`, atomically store `chat_id`, `message.id`, `X-Trace-ID`, `created_at`, and the returned
   status; mark the effect `submitted`, not delivered.
3. Apply verified webhooks idempotently by `event_id`, correlating primarily on message UUID and
   secondarily on trace ID.
4. Periodically call `GET /messages/{messageId}` for stale `submitted`, `pending`, `queued`, or
   `sent` effects. Treat `delivered`/`read` as confirmed delivery. Treat SMS/MMS `sent` as confirmed
   provider transport, not proof that a person saw the message.
5. If the initial HTTP result is ambiguous and no message UUID was stored, retry the **same payload
   with the same idempotency key**; Linq documents that this returns the original response. Never
   invent a new key for an ambiguous send.
6. On `failed`, retain the failure and recheck after a grace interval before any new send, because a
   late delivery is documented as possible. Any resend should be a new product decision/effect, not
   an automatic retry that risks a duplicate.
7. Keep Florence's receipt ledger durable. On the ephemeral tier, Linq deletes a message 24 hours
   after creation, emits no deletion webhook, and GET then returns `404`.
   [Ephemeral-message lifecycle](https://docs.linqapp.com/api/resources/messages/#ephemeral-messages-privacy-tier)

For outbound HTTP retries, Linq's official TypeScript SDK retries connection errors, `408`, `409`,
`429`, and `>=500` twice by default with short exponential backoff; raw HTTP callers must implement
their own bounded policy and respect `Retry-After` on `429`. Idempotency remains required because a
timeout can occur after Linq accepted the send. [Official TypeScript SDK retry behavior](https://docs.linqapp.com/api/typescript/#retries)
[Rate-limit handling](https://docs.linqapp.com/guides/platform/rate-limits/#handling-rate-limits)

## Explicit uncertainties

- Linq does not document a polling cadence, a send-to-delivery SLA, or a duration after which
  `pending`/`queued`/`sent` can safely be declared dead.
- Linq does not document a receipt/event lookup by webhook `event_id` or `trace_id`; only message-ID
  retrieval was found.
- `failed` is not always irreversible, and the docs provide no precise frequency or grace window for
  the documented late-delivery case.
- A success-response `trace_id` body field is inconsistently described; `X-Trace-ID` is the stable
  documented success-response location.
