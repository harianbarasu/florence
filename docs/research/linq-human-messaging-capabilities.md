# Linq capabilities for human, native Florence messaging

Research date: 2026-08-28

Implementation update: the current branch now marks accepted private chats read; keeps bounded, non-blocking typing active for private and group conversations while Florence works; and exposes one typed native-message surface for replies, mentions, rich links, public media, built-in or custom reaction add/remove, and group polls. Provider receipts remain on the existing durable outbox path. The broader capability map below records later possibilities and provider constraints; it is not a set of workflow categories or a prerequisite for product testing.

## Scope and conclusion

This note audits Florence's current Linq Partner API integration against Linq's first-party documentation and canonical V3 OpenAPI specification. The pasted audit checklist was treated as a hypothesis to verify; it is in fact copied from Linq's own [Best Practices](https://docs.linqapp.com/getting-started/best-practices/) and [Chat Health](https://docs.linqapp.com/guides/chats/chat-health/) pages.

Florence has a strong narrow foundation: verified and deduplicated webhooks, idempotent text sends, inline replies, paced bubbles, built-in tapbacks, delivery/read/failure observations, inbound images/PDFs/audio, and best-effort typing. But its provider adapter and model-facing conversation contract expose only a small fraction of Linq. The result is that Florence can _write like_ a person, yet often cannot _behave like_ one in Messages.

The highest-value product shape is not a set of scenario-specific flows. It is one general native-conversation capability beneath the same household agent: inspect current chat/line state, choose an appropriate conversational move, use the richest supported primitive, and reconcile the provider result. Dinner planning, travel, school logistics, and arbitrary family work should all use that same surface.

Three issues should precede polish:

1. Florence's opt-out implementation conflicts with Linq's documented semantics and can both miss stop requests and keep a locally stopped chat stopped after the provider has cleared it.
2. Florence discards `health_status` and phone reputation and starts new chats on one fixed line, so it cannot apply Linq's deliverability guidance or managed failover.
3. The webhook HTTP request can remain open while media downloads and voice transcription run, although Linq requires a response within 10 seconds and recommends acknowledging before asynchronous processing.

## What Linq documents

The [Linq documentation index](https://docs.linqapp.com/llms.txt) identifies the [V3 OpenAPI document](https://cdn.linqapp.com/openapi/linq-api-v3.yaml) as canonical. The messaging-relevant surface is:

| Capability | What Linq provides | Human Florence use |
| --- | --- | --- |
| Managed line/chat selection | `POST /v3/messages` with `to` and no `from` reuses a healthy chat, opens on the best line, or fails over from a flagged line; the response explains the choice. [Sending Messages](https://docs.linqapp.com/guides/messaging/sending-messages/) | Preserve continuity without exposing line management to the family. |
| Onboarding line selection | `GET /v3/available_number` returns the best line plus a time-limited multi-line `.vcf`; it is an onboarding call, not a per-send call. [Phone Numbers](https://docs.linqapp.com/guides/phone-numbers/) | Give a new parent the right Florence number/contact at signup and spread signups across healthy lines. |
| Text and paced multipart messages | Messages contain text, media, or link parts. Text supports formatting and animations. [Sending Messages](https://docs.linqapp.com/guides/messaging/sending-messages/) | Short natural bubbles, photos, documents, and occasional expressive emphasis. |
| Inline replies and thread reads | `reply_to` targets a message/part; a thread can later be read by message ID. [Message Details](https://docs.linqapp.com/guides/messaging/message-details/) | Answer the exact parent or request when a busy group has multiple topics. |
| Mentions | A group text part can mention one current participant and notify an iMessage recipient through a muted chat. [Mentions](https://docs.linqapp.com/guides/messaging/mentions/) | Direct a real decision or action to one parent without making every group message urgent. |
| Reactions | Six standard tapbacks plus custom Unicode emoji; add/remove and target a multipart index. Stickers are inbound-only. [Reactions](https://docs.linqapp.com/guides/messaging/reactions/) | Warm acknowledgement, humor, support, and lightweight agreement without a redundant bubble. |
| Typing indicators | Start/stop in DMs and groups; a start lasts about 85–90 seconds and should be refreshed every 60 seconds for longer work. The inbound group event does not identify which participant is typing. [Typing Indicators](https://docs.linqapp.com/guides/chats/typing-indicators/) | Immediate presence during reasoning and tool work; avoid replying over a parent who is still composing. |
| Read state | `POST /v3/chats/{chatId}/read` sends a read receipt in one-to-one iMessage/RCS; it has no effect in groups. [Chats](https://docs.linqapp.com/guides/chats/) | Give a private parent immediate feedback before the substantive answer is ready. |
| Delivery lifecycle | `message.sent`, `.delivered`, `.read`, `.failed`, and `.edited` webhooks, with trace IDs. Group delivery/read receipts are unsupported. [Webhook Events](https://docs.linqapp.com/guides/webhooks/events/) | Do not mistake acceptance for delivery; surface or recover from a real failure. |
| Contact identity | Configure native iMessage Name and Photo Sharing once per line, then share into an iMessage chat after its first outbound; retry at most once daily because saving is not confirmed. [Contact Cards](https://docs.linqapp.com/guides/contact-cards/) | Florence appears as Florence, with a recognizable name/photo rather than an unknown number. |
| Rich links | A link-only part produces an inline title/description/image on iMessage and RCS; SMS falls back to the URL. [Rich Link Previews](https://docs.linqapp.com/guides/messaging/rich-link-previews/) | Send one legible result/booking/menu/source card instead of a wall of URL text. |
| Attachments | Images, video, documents, and audio can be sent by HTTPS URL up to 10 MB or pre-uploaded and reused up to 100 MB. [Attachments](https://docs.linqapp.com/guides/messaging/attachments/) | Return a useful document, photo, itinerary, or generated household artifact in the thread. |
| Voice memos | A dedicated endpoint creates native inline voice-memo playback on iMessage and audio fallback on RCS/SMS. [Voice Memos](https://docs.linqapp.com/guides/messaging/voice-memos/) | A genuinely voice-appropriate response, not a text pretending to be one. |
| Polls | Native iMessage polls support options, votes, tallies, and lifecycle webhooks. Options are add-only; the question is a preceding text message. [Polls](https://docs.linqapp.com/guides/messaging/polls/) | Collect a real family choice without turning a general agent into a dinner or scheduling workflow. |
| Group lifecycle | Create, name, icon, add/remove participants, and leave; typing works in groups, while read/delivery receipts do not. [Group Chats](https://docs.linqapp.com/guides/chats/group-chats/) | Make the family thread identifiable and keep membership synchronized. |
| Effects and decorations | iMessage screen/bubble effects plus text styles/animations; effects are ignored on RCS/SMS. [Message Effects](https://docs.linqapp.com/guides/messaging/message-effects/) | Sparse celebration or emotional emphasis, never routine automation theater. |
| Editing | An outgoing iMessage text part can be edited up to five times within 15 minutes; `message.edited` confirms it. [Sending Messages](https://docs.linqapp.com/guides/messaging/sending-messages/#editing-messages) | Correct a typo or immediately wrong detail in place. |
| Location sharing | Request a participant's location, read active GeoJSON shares, and observe start/stop events; it is iMessage-only. [Location Sharing](https://docs.linqapp.com/guides/location-sharing/) | Fulfill an explicit general request such as coordinating pickup, without a special pickup workflow. |
| Group background | Color, animated, or photo background visible to both sides; updates are asynchronous and iMessage-only. [Chat Backgrounds](https://docs.linqapp.com/guides/chats/backgrounds/) | Optional family-selected personality, not an automatic default. |
| Capability/protocol selection | Default omission of `preferred_service` gives iMessage → RCS → SMS fallback; capability checks support product decisions about richer primitives. [Protocol Selection](https://docs.linqapp.com/guides/messaging/protocol-selection/) | Use the native feature when supported and degrade honestly. |
| Native cards | Linq-hosted experiences can render a link card; a partner's own iMessage app can render interactive cards. [Experiences](https://docs.linqapp.com/guides/messaging/experiences/), [iMessage Apps](https://docs.linqapp.com/guides/messaging/imessage-apps/) | Later-stage interactive handoff/RSVP cards. Not needed for the core human-feel tranche. |
| Blocked handles | Account-wide block/unblock drops inbound and rejects direct outbound. [Blocked Handles](https://docs.linqapp.com/guides/blocked-handles/) | Administrative abuse/spam handling, distinct from a household member's opt-out. |
| Phone forwarding | A Linq number can forward inbound calls; Linq does not itself place or answer calls. [Phone Numbers](https://docs.linqapp.com/guides/phone-numbers/#voice-calls) | Keep the visible Florence line coherent if the separate voice-agent stack is connected later. |

The canonical OpenAPI contains no scheduled-message endpoint. Message timing, backoff, reminders, and cadence must remain application-owned. `DELETE /v3/messages/{messageId}` only removes Linq's record; it does **not** unsend from recipients' devices.

## Current Florence implementation

Florence uses a hand-written `@florence/linq` client with no official Linq SDK dependency (`packages/linq/package.json`). That is not inherently wrong, but the custom surface currently implements only chat creation/read, typing, text send, built-in reaction send, and attachment fetch.

### What is already good

| Current behavior | Evidence | Assessment |
| --- | --- | --- |
| Uses V3 and a pinned webhook payload version | `packages/linq/src/index.ts:3-4`, `apps/api/src/linq-ingress.ts:18,111-120` | Good. Explicit versioning avoids silent payload drift. |
| Verifies Standard Webhooks against the raw body, applies a five-minute replay window, constant-time compares signatures, and checks partner ID | `packages/linq/src/index.ts:116-145,1169-1194`; raw buffer route in `apps/api/src/app.ts:419-435` | Matches Linq's [Webhooks](https://docs.linqapp.com/guides/webhooks/) guidance. |
| Deduplicates provider event IDs and uses deterministic source IDs | `packages/database/src/store.ts:16363-16426,16460-16475` | Correct for Linq's at-least-once webhook delivery. |
| Uses message idempotency keys and reconciles a reaction before treating an ambiguous non-idempotent mutation as safe | `packages/linq/src/index.ts:407-475,478-593`; `apps/api/src/florence.ts:2431-2454` | Strong provider discipline. |
| Supports native inline replies | `packages/linq/src/index.ts:181-192,418-435`; `apps/api/src/florence.ts:2420-2428` | Good. The prompt also limits replies to moments where they disambiguate (`apps/api/src/reasoner.ts:1702`). |
| Supports six standard tapbacks and tells the model to use them occasionally rather than mechanically | `packages/linq/src/index.ts:212-221,478-567`; `apps/api/src/reasoner.ts:838-852,1702` | Good human-feel foundation. |
| Supports one to three short bubbles with per-bubble delays up to five seconds | `apps/api/src/reasoner.ts:838-852`; staging at `apps/api/src/florence.ts:6560-6587` | Good. This makes the final response feel composed rather than dumped. |
| Starts and stops typing around foreground work | `packages/linq/src/index.ts:381-405`; `apps/api/src/florence.ts:1440-1459,1597-1604` | Partial. The orchestration currently enables it only for private chats and never refreshes it. |
| Reconciles sent/delivered/read/failed webhooks into provider truth | parser at `packages/linq/src/index.ts:150-163,1061-1116`; ingress at `apps/api/src/linq-ingress.ts:134-136`; persistence begins at `packages/database/src/store.ts:8286` | Good basis for reliable receipts. |
| Receives text/link/media, reply targets, and reactions; ingests images and PDFs and transcribes audio | `packages/linq/src/index.ts:996-1058,1119-1166`; `apps/api/src/linq-ingress.ts:165-208,375-452` | Useful input breadth, although unsupported media/events are silently narrowed. |
| Rechecks the live chat participant set before every send | `packages/linq/src/index.ts:361-379,673-690`; `apps/api/src/florence.ts:2388-2395` | Strong group/identity correctness. Preserve this if adopting the official SDK underneath. |

### What constrains native behavior

The model-facing output can request only six built-in reactions plus text bubbles (`apps/api/src/reasoner.ts:829-853`). The provider-facing `LinqSendMessage` can carry only text and an optional `replyTo` (`packages/linq/src/index.ts:181-192`). Consequently, Linq's richer primitives are not merely unchosen; they are structurally unreachable.

Other concrete constraints:

- `createChat` pins `from` to `LINQ_FROM_PHONE` and forces `preferred_service: "iMessage"` (`packages/linq/src/index.ts:286-315`; callers at `apps/api/src/florence.ts:5553-5558,5608-5613`; env wiring at `apps/api/src/app.ts:132-134`).
- Follow-up sends also force iMessage (`packages/linq/src/index.ts:426-435`), chat reads reject non-iMessage state (`packages/linq/src/index.ts:813-846`), and ingress rejects RCS/SMS (`apps/api/src/linq-ingress.ts:121-145`).
- The client can now issue typing calls for either audience, but orchestration still gates them to private chats (`apps/api/src/florence.ts:1448-1454`) and does not refresh after 60 seconds or consume inbound typing events.
- Inbound reactions are rejected unless they target part zero (`apps/api/src/linq-ingress.ts:165-170`); outbound reactions cannot use custom emoji, remove a reaction, or select a stored part index.
- Inbound video and non-PDF documents are ignored even though Linq can transport them; outbound media/link/voice is absent (`apps/api/src/linq-ingress.ts:375-462`).
- All webhook event types other than message lifecycle and reactions become `ignored` (`packages/linq/src/index.ts:147-173`). This drops typing, phone reputation, polls, group metadata/membership, edits, backgrounds, and location-sharing events documented by Linq.
- `LinqObservedChat` contains authority only, so the decoder discards `health_status` (`packages/linq/src/index.ts:224-231,813-878`).
- The send client discards Linq's structured error code/body and response trace ID (`packages/linq/src/index.ts:442-475`).

## Implementation priorities: human feel

These should be implemented as general conversation primitives available to the agent, not as meal/travel/school branches.

| Priority | Capability | Current status | Recommended general behavior |
| --- | --- | --- | --- |
| H0 | Native presence lifecycle | Partial | Mark an accepted private chat read promptly; start typing while a response is actually being composed; support groups; refresh every 60 seconds for long work; stop on send/abort/failure. Consume inbound typing to avoid answering while another message is visibly being composed. |
| H0 | One typed native message surface | Gap | Replace text-only `LinqSendMessage`/reasoner output with one provider-neutral `ConversationMove` union that can express text/media/link, reply target, mention, reaction, poll, effect, and optional voice. The same agent chooses a move from context and provider capability. |
| H0 | Reactions with full fidelity | Partial | Retain the current "occasional, meaningful, never a work-status signal" prompt. Add custom emoji, remove, and part targeting. A substantive request must still receive useful language or work. |
| H1 | Group mentions | Gap | Mention a parent only when a particular action/answer is theirs; the mute override makes indiscriminate mentions hostile. |
| H1 | Native polls | Gap | Let the general agent offer/use a poll whenever a family decision is genuinely best collected as a vote. The primitive is generic; dinner is only one rehearsal. |
| H1 | Florence identity and family-thread identity | Gap/unknown | Verify every active line has an active Florence contact card, share it after first outbound and at most once daily, and set a family-approved group name/icon. Dashboard-side contact-card setup is not visible in this repo. |
| H1 | Rich result sharing | Gap | Send a single useful URL as a rich link, and support outbound photos/documents. Keep a short human explanation separate when necessary because link parts must stand alone. |
| H1 | Reply-aware group conversation | Partial | Preserve existing selective `reply_to`; add mentions and group typing. Do not depend on group read/delivery receipts because Linq explicitly does not support them. |
| H2 | Voice memo | Receive only | Add only after Florence can produce an appropriate audio artifact. Use a real native voice memo, not canned TTS for ordinary answers. |
| H2 | Editing | Gap | Use within Linq's five-edit/15-minute window for immediate corrections. Treat the `message.edited` webhook as confirmation. Never label delete as unsend. |
| H2 | Effects/decorations | Gap | Allow sparingly for a real celebration, playfulness, or emphasis; never decorate routine reminders or simulate progress. |
| H2 | Location request | Gap | Expose as an explicit, general tool when a parent asks. Read the GeoJSON share at a modest interval; coordinates have no update webhook. |
| H3 | Backgrounds and native cards | Gap | User-selected polish or later interactive handoff. These should not block the core conversational tranche. |

## Implementation priorities: reliability, deliverability, and compliance

| Priority | Check | Status | Code evidence | Required correction |
| --- | --- | --- | --- | --- |
| R0 | Exact carrier opt-out keywords | Gap | `isCarrierMessagesOptOut` accepts only STOP/UNSUBSCRIBE/QUIT/END and does so case-insensitively (`packages/database/src/store.ts:16749-16750`). | Match Linq's documented whole-message grammar: case-sensitive `STOP`, `UNSUBSCRIBE`, `OPTOUT`, `CANCEL`, `END`, `QUIT`, with the documented case/space/hyphen flexibility for OPT OUT. |
| R0 | Conversational stop intent | Gap | The foreground prompt and validator force `stopMessaging` false (`apps/api/src/reasoner.ts:1704,8166-8168`), so "please stop messaging me" is not handled. | Add one general semantic stop-intent decision before any outbound, while keeping ordinary cancellation/negative affect distinct. Do not use a brittle phrase list. |
| R0 | Provider `OPTED_OUT` gating and resumption | Gap | Local `stopped_at` blocks every later inbound and is never cleared (`packages/database/src/store.ts:16337-16353,16476-16483`). `LinqObservedChat` discards health. | Treat live `health_status` as the provider gate. Linq clears a keyword opt-out on any later non-opt-out inbound; Florence's durable channel state must reconcile rather than remain stopped forever. |
| R0 | Error 2024 | Partial | HTTP 403 is non-retryable, but the send path discards the Linq code/body and does not stop future attempts (`packages/linq/src/index.ts:442-451`; retry classification at `:1380-1382`). | Parse structured Linq errors. On 2024, record provider opt-out and suppress future sends until current provider state clears. Do not retry. A courtesy override is currently not sent, which is safer than using `override_optout` repeatedly. |
| R0 | Chat health preflight | Gap | Chat decoding keeps only membership/authority (`packages/linq/src/index.ts:224-231,813-878`). | Retain `health_status`: normal on HEALTHY, slow/check reply rate on AT_RISK, pause on CRITICAL, never-send on OPTED_OUT. Linq marks this beta, so store its timestamp and retain a conservative application fallback. |
| R0 | Phone status/reputation | Gap | No `GET /v3/phone_numbers` client and `phone_number.status_updated` is ignored. | Cache line status/reputation, pause ineligible/CRITICAL lines, handle the webhook, and let managed selection place new contacts on healthy lines. |
| R0 | Managed line selection/failover | Gap for new/private chat creation | Partner invite and family group creation pin one configured `from` (`apps/api/src/florence.ts:5553-5558,5608-5613`). | Use `POST /v3/messages` with no `from` where Linq can manage a private conversation, capture `from_selection`, and atomically rebind if failover produces a new chat. Preserve an exact known group thread when that is materially required. |
| R0 | Onboarding number/contact | Gap | `LINQ_FROM_PHONE` and a fixed `FLORENCE_MESSAGES_URL` are required (`apps/api/src/app.ts:132-134`); there is no available-number call. | At new-user onboarding, call `GET /v3/available_number`, show the returned number/deeplink and `.vcf`, and use that line only when an explicit chat creation requires it. Do not call it per message. |
| R0 | Inbound-first/contact-card cadence | Gap | Florence initiates the partner invite via `POST /v3/chats` and never calls contact-card endpoints (`apps/api/src/florence.ts:5534-5588`). | Prefer that the invited parent message Florence first when product UX permits. Configure each line once and share the card after at least one outbound, at most once daily. If Florence initiates, treat reply generation and cadence as line-health-critical. |
| R0 | Webhook acknowledgement deadline | Gap for media/voice | The Fastify route awaits all ingress work (`apps/api/src/app.ts:419-449`), and ingress downloads/seals media and awaits transcription before returning (`apps/api/src/linq-ingress.ts:271-286,375-452`). | Verify and durably enqueue the raw/normalized event, return 2xx within Linq's 10-second timeout, then process artifacts asynchronously and idempotently. |
| R1 | Reply-paced cadence | Gap | Florence has per-message `notBefore` and reminder scheduling but no Linq health/reply-ratio backoff. | Maintain recent inbound/outbound counts per recipient/line. Aim for early reciprocal conversation and about 1 inbound per 2 outbound; follow Linq's one-day, few-days, then halt ladder when replies stop. Do not turn this into canned messages. |
| R1 | Line volume/ramp | Unknown/gap at scale | No per-line daily/new-conversation/ramp gate was found. | Keep under Linq's recommended ~7,000 combined messages/day/line, meaningfully under 50 new conversations/rolling 24h, and avoid several-fold volume jumps with weak replies. |
| R1 | Rate-limit handling | Partial | Idempotency is good, but every retryable delivery is retried after a fixed five seconds (`apps/api/src/florence.ts:2447-2453`); the Linq client discards `Retry-After`. | On 429 use `Retry-After`, otherwise bounded exponential backoff. Linq caps a sender-recipient pair at 30 messages/60 seconds. [Rate Limits](https://docs.linqapp.com/guides/platform/rate-limits/) |
| R1 | Traceability | Partial | Webhook trace IDs are parsed, but successful/error send responses discard `X-Trace-ID`/`trace_id` (`packages/linq/src/index.ts:454-470`). | Persist Linq trace ID with each outbound and webhook observation for exact provider reconciliation/support. [Debugging](https://docs.linqapp.com/guides/platform/debugging/) |
| R1 | Protocol fallback | Gap/intentionally iMessage-only today | Send and receive paths force/reject anything other than iMessage. | Decide explicitly whether the product remains iMessage-only. If not, omit `preferred_service` by default, capability-check only when selecting a rich-only feature, and degrade from the same general move rather than branching the agent by platform. |
| R2 | Blocked handles | Not implemented | No blocked-handle endpoint usage found. | Add only as an account/abuse administrative control. Do not confuse it with household opt-out state. |

## Unsupported or unknown: do not promise these

| Feature | Documented reality | Florence status |
| --- | --- | --- |
| Unsend | Linq delete removes its database record but does not remove the message from the recipient's device. | Unsupported; never present delete as unsend. |
| Provider-scheduled messages | No endpoint appears in the canonical V3 OpenAPI. | Timing must remain in Florence's scheduler/outbox. |
| Outbound stickers | Stickers are inbound-only. | Unsupported by Linq. Custom Unicode reactions are supported and currently missing. |
| Group delivery/read receipts | Linq says these do not exist. | Do not wait for or promise them. Use sent/failure plus conversational evidence. |
| Identity of a group participant who is typing | Typing webhooks contain only `chat_id`. | Florence can delay a response when someone is typing, but cannot know which parent. |
| Typing/effects/location on RCS/SMS | Typing, effects, and location are iMessage-only; SMS has substantially fewer receipt/rich features. | Capability-aware fallback is required if alternate protocols are enabled. |
| Webhook subscription filters in production | Subscription configuration is external to this repository. | Unknown. Code support alone does not prove Florence is subscribed to phone, typing, poll, edit, location, or group events. |
| Contact card configured in Linq dashboard | Dashboard state is external. | Unknown. The repo has no create/retrieve/share implementation, so even a configured card is not being shared through code. |
| Multiple production lines and their current reputation | Provider account state is external. | Unknown. Current code assumes a single `LINQ_FROM_PHONE`. |
| Account-specific experiences | `GET /v3/experiences` is authoritative for the account. | Unknown and not required for the core tranche. |

## Recommended implementation seam

Keep Florence's existing provider-authority and idempotency checks, but deepen the single Linq module instead of adding a second messaging framework:

1. **Provider state:** `ObservedConversation` should include service, chat health, line identity/status/reputation, membership, and capability facts.
2. **General move:** one typed `ConversationMove` should represent text/media/link, reply, mention, reaction, poll, voice, effect, or edit. It should describe the native move, not the product scenario.
3. **Lifecycle:** `observe -> admit by service/health -> mark read/typing -> execute -> reconcile webhook/trace -> update cadence` should serve foreground replies, proactive work, reminders, and durable task receipts alike.
4. **Agent judgment:** the main agent chooses the smallest natural move. Native primitives are tools, not workflows. A poll is available because a decision needs votes; an effect is available because the moment is celebratory; a mention is available because one parent truly needs the notification.
5. **Provider implementation:** using the official `@linqapp/sdk` underneath the existing Florence adapter would bring typed current endpoints, webhook unions, and standard 429 behavior. Preserve Florence's exact live-authority recheck, database idempotency, and effect reconciliation rather than replacing them with direct SDK calls throughout the app.

This seam is the Linq equivalent of the broader Florence goal: one capable general agent, with native affordances chosen from the actual family context, rather than a router over hard-coded use cases.

## Primary sources

- [Linq documentation index](https://docs.linqapp.com/llms.txt)
- [Canonical Linq V3 OpenAPI specification](https://cdn.linqapp.com/openapi/linq-api-v3.yaml)
- [Best Practices](https://docs.linqapp.com/getting-started/best-practices/)
- [Sending Messages](https://docs.linqapp.com/guides/messaging/sending-messages/)
- [Chat Health](https://docs.linqapp.com/guides/chats/chat-health/)
- [Phone Reputation](https://docs.linqapp.com/guides/phone-numbers/phone-reputation/)
- [Webhooks](https://docs.linqapp.com/guides/webhooks/) and [Webhook Events](https://docs.linqapp.com/guides/webhooks/events/)
- [Typing Indicators](https://docs.linqapp.com/guides/chats/typing-indicators/)
- [Protocol Selection](https://docs.linqapp.com/guides/messaging/protocol-selection/)
- [Group Chats](https://docs.linqapp.com/guides/chats/group-chats/)
- [Contact Cards](https://docs.linqapp.com/guides/contact-cards/) and [Sharing Contact Card](https://docs.linqapp.com/guides/chats/share-contact-card/)
- [Reactions](https://docs.linqapp.com/guides/messaging/reactions/), [Mentions](https://docs.linqapp.com/guides/messaging/mentions/), [Polls](https://docs.linqapp.com/guides/messaging/polls/)
- [Attachments](https://docs.linqapp.com/guides/messaging/attachments/), [Voice Memos](https://docs.linqapp.com/guides/messaging/voice-memos/), [Rich Link Previews](https://docs.linqapp.com/guides/messaging/rich-link-previews/)
- [Message Effects](https://docs.linqapp.com/guides/messaging/message-effects/), [Chat Backgrounds](https://docs.linqapp.com/guides/chats/backgrounds/), [Location Sharing](https://docs.linqapp.com/guides/location-sharing/)
- [Rate Limits](https://docs.linqapp.com/guides/platform/rate-limits/) and [Debugging](https://docs.linqapp.com/guides/platform/debugging/)
