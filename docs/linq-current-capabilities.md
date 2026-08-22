# Linq Partner API v3 capabilities Florence needs

Research date: 2026-08-16
Source policy: only Linq-owned documentation and Linq's canonical Partner API v3 OpenAPI. No
authenticated API calls were made.
Adapter reviewed: `packages/linq/src/index.ts`, working-tree SHA-256
`42797551808c977b5a80331ece8cdda36bb2a242d28dc14d4176301db53770ac`.

## Bottom line

Linq already exposes the native affordances Florence needs for an Instinct/Poke-like iMessage
experience: threaded replies, tapbacks, typing indicators, separate paced messages, PDFs and other
attachments, delivery/read events, audio, voice-memo bubbles, links, and edits. There is no reason
to replace Linq or build a messaging framework.

The current Florence adapter already covers the core path: signed and version-pinned inbound text,
media, late-message detection, reply targets, exact live audience re-checks, idempotent text/reply
sends, built-in outbound tapbacks, attachment retrieval, and parsing of sent/delivered/read/failed
events. The smallest remaining product delta is:

1. Confirm outbound reactions from `reaction.added` or message read-back instead of calling a `202
   accepted` response committed; bind the target message to the live chat and parse inbound reaction
   events too.
2. Add best-effort start/stop typing after Florence has established reply authority.
3. Apply the already-parsed delivery/read/failure proposals to Florence's outbound receipt state.
4. When edits enter product scope, admit `message.edited` as a correction tied to the original
   source rather than rewriting accepted history.
5. Remove three avoidable schema traps: nullable webhook chat hints, optional failure detail, and the
   deprecated attachment `status` field.

That is an extension of the existing adapter, not a new subsystem.

## Capability and adapter fit

| Capability | Linq's documented contract | Current Florence adapter | Exact gap |
| --- | --- | --- | --- |
| Inline replies | An existing-chat send accepts `message.reply_to = { message_id, part_index? }`; `part_index` is zero-based and defaults to `0`. The `2026-02-03` message webhook exposes the same relation, and `GET /v3/messages/{messageId}/thread` reads the resulting thread. [Send reference](https://docs.linqapp.com/api/resources/chats/subresources/messages/methods/send/), [message/thread reference](https://docs.linqapp.com/api/resources/messages/methods/list_messages_thread/), [canonical OpenAPI](https://cdn.linqapp.com/openapi/linq-api-v3.yaml) | `sendMessage()` sends `reply_to`; inbound proposals preserve both provider message ID and part index. | Adapter-complete. Keep carrying the part index beyond ingress if Florence needs to target a nonzero media/text part. A reply cannot be the first message of a newly created chat, which does not affect Florence's established chats. |
| Reactions | `POST /v3/messages/{messageId}/reactions` accepts `operation: add \| remove`, a standard tapback or `custom` emoji, and optional `part_index`. `reaction.added` / `reaction.removed` contain `chat_id`, `message_id`, `part_index`, type, actor, `is_from_me`, and time. Stickers are inbound-only. [Reaction guide](https://docs.linqapp.com/guides/messaging/reactions/), [reaction reference](https://docs.linqapp.com/api/resources/messages/methods/add_reaction/), [webhook events](https://docs.linqapp.com/guides/webhooks/events/) | `sendReaction()` supports adding the six standard tapbacks after a live audience check. Reaction webhooks are ignored. | The endpoint has **no documented idempotency key** and duplicate-request behavior is undocumented. Its response is queued/accepted, not proof the tapback appeared. The adapter's `committed` result is therefore too strong. Its comment that no read-back exists is incorrect: `GET /v3/messages/{messageId}` returns `chat_id` and `parts[].reactions`. Use that one bounded read to prove the target belongs to the re-observed chat and reconcile the owner's desired reaction; today only the chat—not the target message—is authority-checked. [Message read-back](https://docs.linqapp.com/api/resources/messages/methods/retrieve/) |
| Typing | Start with `POST /v3/chats/{chatId}/typing`; stop with `DELETE` on the same path. A send clears it; otherwise it lasts about 85–90 seconds and may be refreshed at 60 seconds. A `204` is only best-effort acceptance. [Typing guide](https://docs.linqapp.com/guides/chats/typing-indicators/), [canonical OpenAPI](https://cdn.linqapp.com/openapi/linq-api-v3.yaml) | No typing methods. Typing webhooks are ignored. | Add two tiny adapter calls. Start only after exact conversation authority is known; stop on useful silence/error, while a successful send auto-clears. No durable typing workflow is needed. Linq's sources conflict on groups: the OpenAPI says direct and group chats work, while the [group-chat guide](https://docs.linqapp.com/guides/chats/group-chats/) and FAQ say groups are unsupported. Treat group typing as unconfirmed until Linq resolves this or a live account check proves it. |
| Delivery and read state | `message.sent`, `message.delivered`, `message.read`, and `message.failed` are distinct. The initial message response is acceptance, not device delivery. `GET /v3/messages/{messageId}` exposes current status. Read receipts may be disabled; the group guide says group delivery/read receipts are unsupported. A failed event can rarely be followed by delivered for the same ID. [Webhook events](https://docs.linqapp.com/guides/webhooks/events/), [webhook schemas](https://docs.linqapp.com/api/resources/webhooks/), [message read-back](https://docs.linqapp.com/api/resources/messages/methods/retrieve/) | `unwrapLinqWebhook()` already emits normalized sent/delivered/read/failed proposals keyed by message, chat, event, trace, and optional message idempotency key. `sendMessage()` returns the provider message ID. However, `apps/api/src/linq-ingress.ts` currently acknowledges every non-`inbound_message` proposal as unsupported, so all four observations are dropped. | Route the existing proposals to the receipt record and add bounded GET recovery only for a stale accepted send. Do not present `sent` as delivered, absence of `read` as failure, or `failed` as permission for an immediate duplicate send. |
| Multiple parts and multiple bubbles | One `message.parts` array composes into one message bubble. Text and media may mix; consecutive text parts are invalid; a link must be the only part. Limits are 100 parts, 40 public-URL media parts, and 10,000 characters per text part. Message sends accept a stable `idempotency_key` up to 255 characters and return the original response on a retry. [Sending guide](https://docs.linqapp.com/guides/messaging/sending-messages/), [send reference](https://docs.linqapp.com/api/resources/chats/subresources/messages/methods/send/) | One adapter call sends one text part with one idempotency key. Inbound text parts are joined and inbound media is retained separately. | This is already the right primitive for Florence's 0–3 paced bubbles: make 0–3 separate sends, each with its own persisted key. Do not add a multipart abstraction for pacing. The only information loss is original inbound part order/index; preserve it only if product behavior actually needs cross-part targeting. |
| Attachments and PDFs | Media parts accept a public HTTPS `url` (10 MB) or pre-uploaded `attachment_id` (up to 100 MB). Linq explicitly supports PDF, images, video, audio, Office/iWork documents, ZIP, VCF, and ICS. Inbound media provides attachment ID, filename, MIME type, size, and a CDN URL; `GET /v3/attachments/{id}` refreshes metadata/download URL. [Attachment guide](https://docs.linqapp.com/guides/messaging/attachments/), [attachment reference](https://docs.linqapp.com/api/resources/attachments/methods/retrieve/) | Inbound media metadata is normalized, then `fetchMedia()` re-reads metadata, enforces a local 20 MB bound, restricts download to `cdn.linqapp.com`, and verifies MIME/name/length. PDFs enter the encrypted 24-hour document lane; JPEG/PNG/WebP enter the encrypted image lane; iPhone HEIC is decoded with the production libheif WASM path and stored as JPEG. No outbound media send exists. | Inbound launch formats are complete. No outbound attachment work is needed for the current product. Keep the metadata/length/domain checks; Linq's deprecated attachment `status` is not part of the proof. |
| Audio and voice notes | Ordinary audio is a media attachment. A native iMessage voice-memo bubble uses `POST /v3/chats/{chatId}/voicememo` with exactly one URL or attachment ID; lifecycle uses the normal message status events. [Voice-memo guide](https://docs.linqapp.com/guides/messaging/voice-memos/), [voice-memo reference](https://docs.linqapp.com/api/resources/chats/methods/send_voicememo/) | Florence fetches bounded FLAC, AAC, CAF, AIFF, AMR, M4A/MP4, MP3/MPEG, OGG, WAV, and WebM audio and transcribes it before accepting the turn. Native formats the transcription API cannot read are normalized to bounded mono WAV first. The clearly tagged transcript is conversational evidence; because Linq exposes no trustworthy direct-versus-forwarded marker, it cannot alone authorize retention, scheduling, opt-out, or a consequential effect. Raw audio is discarded after transcription. There is no outbound media or voice-memo method. | Inbound voice is launch-complete. Linq still does **not document** a reliable webhook flag that distinguishes a received native voice memo from an ordinary or forwarded audio file. Outbound voice memos also have no documented idempotency key, so defer them until Florence actually needs to speak. |
| Links | A `link` part produces a rich preview, must be the only part, and has a 2,048-character limit. A URL inside a text part remains valid and may auto-preview in iMessage. [Rich-link guide](https://docs.linqapp.com/guides/messaging/rich-link-previews/), [send reference](https://docs.linqapp.com/api/resources/chats/subresources/messages/methods/send/) | Inbound `link` parts are preserved as HTTPS text. When the current parent message contains a public HTTP(S) URL, the normal reasoner may use bounded web search; Florence emits only one to three direct URLs verified against the tool's returned sources. Outbound URLs remain ordinary text so iMessage can render them naturally. | Launch-complete. A dedicated crawler, link-preview endpoint, or outbound multipart abstraction is unnecessary. |
| Edit and delete | `PATCH /v3/messages/{messageId}` edits one text part, at most five times within 15 minutes, on iMessage; `message.edited` is available only in webhook version `2026-02-03`. `DELETE /v3/messages/{messageId}` only deletes Linq's API record and does **not** unsend it. No `message.deleted` event is documented. [Edit reference](https://docs.linqapp.com/api/resources/messages/methods/update/), [delete reference](https://docs.linqapp.com/api/resources/messages/methods/delete/), [webhook events](https://docs.linqapp.com/guides/webhooks/events/) | Edited events are ignored; no outbound edit/delete methods exist. | When supported, admit an inbound edit as a correction tied to the original source rather than silently rewriting accepted history. Do not add outbound edit/delete now: a corrective bubble is simpler, and provider delete cannot retract what the family saw. |

## Webhook contract and versioning

Linq webhooks use an envelope containing `api_version`, `webhook_version`, `event_type`, `event_id`,
`created_at`, `trace_id`, `partner_id`, and event-specific `data`. Delivery is at least once: Linq
documents ten attempts over roughly 25 minutes, so `event_id` is the deduplication key. The signing
scheme uses the raw request body plus `webhook-id`, `webhook-timestamp`, and `webhook-signature`, and
Linq recommends a five-minute replay window. [Webhook guide](https://docs.linqapp.com/guides/webhooks/)

`unwrapLinqWebhook()` correctly verifies the raw bytes with constant-time comparison, applies the
five-minute window, binds header and payload event IDs, checks the partner ID, and accepts only
`api_version: "v3"` plus `webhook_version: "2026-02-03"`. Deployment must therefore create the
subscription with an explicit `?version=2026-02-03`; Linq otherwise selects a version based on
subscription creation date. Do not add a heuristic dual-version parser. [Webhook versioning](https://docs.linqapp.com/guides/webhooks/#webhook-versioning)

The adapter also preserves `reconciled_at`, which Linq explicitly defines as late recovered history
that should not trigger an automatic reply. `linq-ingress.ts` acknowledges a non-null
`reconciledAt` without invoking Florence's reply loop. [Message webhook schema](https://docs.linqapp.com/api/resources/webhooks/)

Linq exposes provider structure for attachments, voice-note media, links, and inline-reply targets,
so Florence can keep that identifiable content in the evidence lane. Linq does **not** document a
forwarded/pasted marker for the ordinary text of an inbound Message. Florence therefore evaluates
ordinary text sent by the verified participant as that parent's current utterance instead of trying
to guess provenance with regexes or accepted-phrase lists. For the two isolated consequential
approval passes, an inline reply is eligible only when its `reply_to` resolves to Florence's exact
sent Calendar-offer or partner-invitation prompt; an unthreaded natural-language approval remains
eligible. Canonical carrier opt-out remains an ingress-level exception.

Three current parser assumptions are stricter than the published `2026-02-03` schema:

- `MessageEventV2.chat.is_group` and `owner_handle` are nullable in the canonical OpenAPI, while the
  adapter requires both. Treat them as provider hints and let the existing live `GET /chats/{id}`
  authority read supply the fail-closed classification.
- The failure schema requires `code` and `failed_at`, but publishes `chat_id`, `message_id`, and
  `reason` as optional. An unattributable failure cannot update an effect, but it should not make the
  webhook endpoint retry an otherwise authentic event forever.
- Attachment `status` is explicitly deprecated. Required identity, MIME, size, download URL, and
  byte verification are the useful evidence.

## Documented versus not documented

The following must not be inferred into product guarantees:

- **Documented:** message-send idempotency. **Not documented:** idempotency or safe retry semantics
  for reactions, typing, voice memos, or edits.
- **Documented:** a reaction request was accepted and assigned a trace ID. **Not documented:** that
  the `202` response proves the reaction rendered. Confirmation comes from event/read-back state.
- **Documented:** sent, delivered, read, and failed are different observations. **Not documented:**
  a delivery/read SLA or a time after which absence of either is failure.
- **Documented:** inbound audio arrives as media. **Not documented:** a stable inbound field that
  distinguishes a native voice note from an attached audio file.
- **Documented:** deleting removes Linq's retained record. **Not supported:** native unsend or a
  message-deleted webhook.
- **Conflicting first-party documentation:** group typing. The canonical OpenAPI currently says it
  works; Linq's group guide and FAQ say it does not. Private iMessage typing is the only safe launch
  assumption.
