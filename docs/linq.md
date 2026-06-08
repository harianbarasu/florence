# Linq Transport

Florence uses Linq Partner API v3 as the iMessage/RCS/SMS transport.

## Required Settings

- `LINQ_API_KEY`
- `LINQ_WEBHOOK_SECRET`
- `LINQ_FROM_PHONE`
- `LINQ_BASE_URL`, defaulting to `https://api.linqapp.com/api/partner/v3`

`LINQ_FROM_PHONE` must be an assigned Linq phone number. Linq requires this
number in the `from` field when Florence creates a new chat.

## Existing Chats

Replies to an existing household thread use:

- `POST /chats/{chat_id}/messages`
- A `message.parts` array with a text part.
- `message.idempotency_key` for duplicate-send protection.

## Webhook Idempotency

When `LINQ_WEBHOOK_SECRET` is configured, Florence verifies the Linq webhook
signature over `{timestamp}.{raw_payload}` and rejects timestamps more than five
minutes away from the server clock. Signature headers may contain either the raw
hex digest or a `sha256=`-prefixed digest. Unsigned webhooks are only for local
SQLite smoke runs; Postgres-backed runtimes reject Linq webhooks until the secret
is configured.

Linq webhooks may be retried. Florence treats the inbound Linq `message.id` as
the message idempotency key. On the first attempt, Florence records the exact
outbound Linq payloads before sending them. If the same inbound message arrives
again, Florence skips command/agent side effects and only retries outbound
payloads that are still `pending` or `failed`; payloads already marked `sent`
are not replayed. Message webhooks without a stable message id, sender, chat id,
or any text/media payload are acknowledged and ignored before any household
state is created.
If the household moves from a one-to-one thread into a Linq-created partner
group, retryable outbound payloads follow the household to the group chat.

This matters for parent trust: a retry must not create duplicate reminders,
approval requests, source rules, memory writes, or group-chat invites.
After a parent confirms whole-household data deletion, Florence keeps a
short-lived hashed tombstone for each inbound Linq `chat_id` and `message.id`
that existed in the deleted household. That lets duplicate pre-deletion webhooks
be ignored without recreating the household that was just deleted.

## Attachments

Linq message `parts` can include non-text media such as images or documents.
Florence accepts media-only messages as long as Linq includes a stable message
id, sender, and chat id.

Current behavior is intentionally conservative:

- Text parts are joined and processed as the parent caption.
- Non-text parts are stored as bounded `flyer` or `document` source items with
  message-id-based idempotency.
- Bounded extracted text or descriptions supplied on the media part are treated
  as the source summary and can produce a parsed event time.
- Raw media bytes are not downloaded.
- Raw attachment URLs are not written into durable source bodies.
- If a parent sends only media with no extracted text, Florence acknowledges it
  and asks for one short line of context.
- If a parent sends a useful caption such as "permission slip due Friday", the
  caption goes through the same Need-to-Know policy as email and calendar items.

## Partner Invite

When a parent texts `invite partner +15555550101`, Florence creates a new Linq
group chat with:

- `from`: `LINQ_FROM_PHONE`
- `to`: the current parent's phone and the invited partner's phone
- An initial text-only message

The initial message intentionally contains no URL. Linq rejects links in the
first outbound message of a newly created chat.

After Linq returns the new group chat id, Florence migrates the household's
primary `chat_id` to the group and stores the old one-to-one chat as a household
alias. The invited phone is explicitly confirmed as the second parent during
that callback. That keeps old direct-thread messages from creating a second
household and prevents unrelated early senders from becoming parents by
accident.

## Primary References

- [Linq sending messages](https://docs.linqapp.com/guides/messaging/sending-messages/)
- [Linq group chats](https://docs.linqapp.com/guides/chats/group-chats/)
- [Linq create chat API](https://docs.linqapp.com/api/resources/chats/methods/create/)
- [Linq phone numbers](https://docs.linqapp.com/guides/phone-numbers/)
