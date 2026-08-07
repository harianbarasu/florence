# Linq v3: detecting a group invocation without replying in the group

Research date: 2026-08-06
Source policy: Linq's current official documentation and canonical OpenAPI, plus Florence source at
commit [`7589dc0`](https://github.com/harianbarasu/florence/tree/7589dc0d8e5a40dd1b83040c04c25ab43d3e252b). No authenticated Linq calls were made.

## Bottom line

Florence can reliably tell that an inbound message came from a particular participant in a group,
read its text, and see whether it is a reply to a known message. Linq does **not** currently expose a
structured inbound `mention` or `mention_range` in the `message.received` contract. Although those
fields exist on the **outbound** text-part request, the webhook's text-part response contains only
`type`, `value`, and `text_decorations`. A native iMessage mention therefore cannot be distinguished
contractually from ordinary text such as `Florence, can you help?`.

The safe v1 rule is consequently deterministic and private: in a read-only group, treat only a
leading, bounded textual address to Florence or a reply to a locally recorded Florence message as an
invocation; then create/send one direct chat to `sender_handle.handle`. Never enqueue a send to the
group. Do not use fuzzy model classification, surrounding group history, attachments, or a message
lookup to make this decision.

## API facts

| Need | Linq Partner API v3 fact | Consequence |
| --- | --- | --- |
| Message text | A `2026-02-03` `message.received` payload has top-level `data.parts`. A text response part has `type: "text"`, `value`, and optional `text_decorations`; media, link, and iMessage-app parts are separate variants. | Invocation matching can inspect the text `value` fields in the received message itself. |
| Sender | `data.sender_handle` is a full `ChatHandle`: required `id`, `handle`, `service`, and `joined_at`, with nullable/optional `is_me`, `status`, and `left_at`. | `sender_handle.handle` is the exact phone number or Apple ID email to use for a private handoff, after matching it to the authoritative live chat. |
| Chat type | `data.chat.id` identifies the chat and webhook `data.chat.is_group` is nullable/optional in the current OpenAPI model. `GET /v3/chats/{chatId}` returns required `is_group`. | Use the live GET as the authoritative group/direct classification. Do not depend solely on the nullable webhook hint. |
| Participants | The message webhook's `chat` object contains the chat ID, Florence's `owner_handle`, health, and group hint; it does **not** contain the full audience. `GET /v3/chats/{chatId}` returns `handles[]` with full participant records and membership status. | Reconcile the sender and exact current audience from the live chat before creating an effect. |
| Reply/reference | `data.reply_to` is nullable and contains the referenced `message_id` plus optional zero-based `part_index`. It does not embed the parent message or its sender. | A reply is an invocation only if that message ID matches Florence's own durable outbound receipt in the same conversation and current participant epoch. |
| Mentions | The send-request `TextPart` supports `mention` (participant handle) and `mention_range` for iMessage groups. The webhook `MessageEventV2` instead uses `schemas-TextPartResponse`, whose documented fields are `type`, `value`, and `text_decorations`; no inbound mention target or flag is defined. | Treat an apparent/native mention as text. There is no supported structured inbound mention detector to parse. |
| Private DM | `POST /v3/chats` accepts `from`, `to`, and an initial `message`; the OpenAPI explicitly says one `to` recipient creates an individual chat. Repeating the exact `from` + `to` reuses the existing chat and sends into it. The response includes `chat.is_group`, `chat.handles`, and the sent message. The first message cannot contain a URL and should use a stable `idempotency_key`. | Auto-DM is supported. Send to `[sender_handle.handle]`, then fail closed unless the returned audience is exactly Florence plus that one recipient and `is_group` is false. |

Primary Linq sources:

- [Canonical Partner API v3 OpenAPI](https://cdn.linqapp.com/openapi/linq-api-v3.yaml) — `MessageEventV2`, `schemas-TextPartResponse`, `TextPart`, `ReplyTo`, `ChatHandle`, `CreateChatRequest`, and `CreateChatResult`.
- [Webhook API models](https://docs.linqapp.com/api/resources/webhooks/) and the [message.received examples](https://docs.linqapp.com/guides/webhooks/events/).
- [Retrieve a chat](https://docs.linqapp.com/api/resources/chats/methods/retrieve/) — authoritative `handles[]` and `is_group`.
- [Create a chat](https://docs.linqapp.com/api/resources/chats/methods/create/) — one-recipient direct chat, exact-participant reuse, response audience, first-message restriction, and idempotency.
- [Send into an existing chat](https://docs.linqapp.com/api/resources/chats/subresources/messages/methods/send/) — outbound `mention`, `mention_range`, and `reply_to` request fields.

## Recommended inference and policy

This section is a Florence product recommendation, not a Linq guarantee.

1. Admit only a verified, non-reconciled `message.received` whose authoritative live chat is a group
   and whose sender is a current non-self participant.
2. Recognize an invocation without a model call when either:
   - the first textual clause begins with an exact Florence alias, such as `Florence, ...` or
     `Hey Florence ...`, using case-insensitive token boundaries and requiring request text after the
     address; or
   - `reply_to.message_id` matches a Florence outbound `effect_receipts.provider_receipt_id` for the
     same conversation **and current participant epoch**.
3. Do not count a mid-sentence name, fuzzy semantic reference, generic `can someone...`, reaction,
   attachment, edit, or a reply whose local Florence ownership cannot be proven.
4. Produce only a private activation effect addressed to that exact sender. The source group's mode
   must never be widened and no group-send effect should exist on this branch.
5. If Florence must resume the request after activation, introduce an explicit, encrypted,
   short-expiry `pending_private_activation` record containing only the invoked request and routing
   IDs. That is a deliberate exception/control-signal policy; the current `content_disabled` rule
   otherwise forbids retaining the group message. Never retain surrounding chat or fetch the replied
   message from Linq merely to classify the invocation.

This makes `Florence, remind me about Wednesday pickup` detectable, including the visible text of a
native iMessage mention, while acknowledging that Linq gives no cryptographic/structured proof that
the user selected Florence as an iMessage mention target. A reply to a locally proven Florence
message is the stronger signal.

## Florence code seams and current gap

- The adapter already parses sender, text parts, `chat.is_group`, and `reply_to`, then normalizes them
  into `LinqMessageReceivedEvent`: [`schemas.ts` lines 31-57 and 111-114](https://github.com/harianbarasu/florence/blob/7589dc0d8e5a40dd1b83040c04c25ab43d3e252b/src/adapters/linq/schemas.ts#L31-L57),
  [`webhook.ts` lines 238-260](https://github.com/harianbarasu/florence/blob/7589dc0d8e5a40dd1b83040c04c25ab43d3e252b/src/adapters/linq/webhook.ts#L238-L260), and
  [`contracts.ts` lines 83-93](https://github.com/harianbarasu/florence/blob/7589dc0d8e5a40dd1b83040c04c25ab43d3e252b/src/adapters/linq/contracts.ts#L83-L93).
- The webhook route already performs `GET /chats/{id}` before authoritative processing:
  [`server.ts` lines 168-184](https://github.com/harianbarasu/florence/blob/7589dc0d8e5a40dd1b83040c04c25ab43d3e252b/src/server.ts#L168-L184).
- Current `providerChatContextSchema` requires `is_group: boolean`, while Linq's current webhook
  OpenAPI permits it to be null/omitted. The live chat schema is the right source of truth; the
  webhook parser should tolerate the documented nullable hint rather than reject an otherwise valid
  event.
- Linq marks recovered historical messages with optional `reconciled_at` and advises suppressing
  auto-replies, but Florence's current webhook schema/contract drops that field. A private-invocation
  branch must preserve it and reject recovered events.
- Today a group in `content_disabled` mode is classified `routing_only` with `retainEvent: false`, so
  its invocation text never reaches orchestration:
  [`florence-application.ts` lines 345-414](https://github.com/harianbarasu/florence/blob/7589dc0d8e5a40dd1b83040c04c25ab43d3e252b/src/application/florence-application.ts#L345-L414) and
  [`classifyEvent` lines 2492-2512](https://github.com/harianbarasu/florence/blob/7589dc0d8e5a40dd1b83040c04c25ab43d3e252b/src/application/florence-application.ts#L2492-L2512).
- The existing content-disabled group behavior privately offers a registered household inviter the
  option to enroll unknown participants; it does not DM the invoking sender:
  [`queuePrivateGroupEnrollmentOffers` lines 1417-1457](https://github.com/harianbarasu/florence/blob/7589dc0d8e5a40dd1b83040c04c25ab43d3e252b/src/application/florence-application.ts#L1417-L1457).
- Florence already has the durable pattern needed to prove a reply target belongs to one of its own
  sends: match `replyTo.providerMessageId` to `effect_receipts.provider_receipt_id`, fenced to the
  same conversation and participant epoch:
  [`orchestrator.ts` lines 666-680](https://github.com/harianbarasu/florence/blob/7589dc0d8e5a40dd1b83040c04c25ab43d3e252b/src/runtime/orchestrator.ts#L666-L680).
- The transport for the private handoff already exists. `createDirectChat()` posts one recipient and
  verifies the returned chat is an exact direct audience; the effect executor selects it when the
  payload has `recipient` rather than `providerChatId`:
  [`client.ts` lines 311-370](https://github.com/harianbarasu/florence/blob/7589dc0d8e5a40dd1b83040c04c25ab43d3e252b/src/adapters/linq/client.ts#L311-L370) and
  [`linq-message-executor.ts` lines 48-68](https://github.com/harianbarasu/florence/blob/7589dc0d8e5a40dd1b83040c04c25ab43d3e252b/src/modules/effects/linq-message-executor.ts#L48-L68).
- Group silence is already enforced at the ordinary send-authority seam: both `content_disabled` and
  `read_enabled_write_disabled` fail authorization. Preserve that gate and add a separate exact-
  recipient private activation authorization, rather than treating the invocation as authority to
  write in the source group:
  [`authority.ts` lines 58-77 and 95-112](https://github.com/harianbarasu/florence/blob/7589dc0d8e5a40dd1b83040c04c25ab43d3e252b/src/modules/conversations/authority.ts#L58-L77).

The minimal implementation seam is therefore before the current `routing_only` decision in
`admitLinqWebhook`: classify a deterministic invocation as a narrow private-activation control
signal, enqueue an exact-recipient direct effect, and leave the source group write-disabled. No Linq
adapter work for inbound mentions is possible until Linq adds mention metadata to `MessageEventV2`.
