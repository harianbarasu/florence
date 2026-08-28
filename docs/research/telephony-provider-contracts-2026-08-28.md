# Telephony provider contracts for Florence

Research date: 2026-08-28
Scope: official Bland, Vapi, and Twilio documentation only.

## Recommendation

For the first dentist or appointment-coordination path, use **Bland with Florence's existing durable-work runtime and poll the call resource**. Bland is the smallest provider-confirmed path from “call this office and accomplish this task” to an inspectable result: it accepts a natural-language task, returns a `call_id`, exposes stop/get operations, and returns the transcript, summary, disposition, and recording URL on the call resource. A webhook is not required for the first implementation.

The important constraint is that none of the three reviewed create endpoints documents a caller-supplied idempotency key. Florence therefore must claim an operation once before making the provider request, persist the provider ID as soon as it receives it, and never blindly repeat a create request whose response was lost. Bland provides the best recovery aid of the three because Florence can attach its own operation ID as `metadata`, list calls in a narrow destination/time window, then get candidate calls and match that metadata.

Vapi is a reasonable next choice if we prefer its assistant configuration, structured outputs, or imported-number workflow. Its current documentation is less satisfactory for an initial durable workflow because active calls end through a transient monitor control URL, and the reviewed official documentation does not establish how to cancel a not-yet-started scheduled single call. Twilio directly provides strong phone and SMS primitives, but not the AI agent that negotiates an appointment; Florence would also need to provide TwiML/conversation orchestration and post-call analysis.

## Contract comparison

| Capability | Bland | Vapi | Twilio |
| --- | --- | --- | --- |
| Start an outbound AI task call | `POST /v1/calls` with `phone_number` and `task` (or a pathway) | `POST /call` with an assistant, `phoneNumberId`, and customer number | `POST /Calls.json`; Florence must provide TwiML/application behavior |
| Inspect | `GET /v1/calls/{call_id}` | `GET /call/{id}` | `GET /Calls/{Sid}.json` |
| Stop/cancel | `POST /v1/calls/{call_id}/stop`, including a scheduled call | Active call: post `end-call` to the create response's `monitor.controlUrl`; scheduled-call cancellation was not verified | Update the Call resource with `Status=canceled` or `Status=completed` |
| Result | Summary, disposition, full and turn-level transcript, recording URL when enabled | `analysis.summary`; artifact transcript/messages/recording | Call status/duration/recording resources; no built-in AI-task transcript or summary contract on Call create/get |
| Provider-documented create idempotency key | None found | None found | None found for Messages or Calls |
| Completion without a new webhook | Poll get until terminal | Poll get until terminal | Poll get; status callbacks are preferable at scale but not required |

## Bland

### Create, get, and stop

[`POST /v1/calls`](https://docs.bland.ai/api-v1/post/calls) requires an E.164 `phone_number` and a `task`, unless a `pathway_id` supplies the conversation flow. A successful response includes a `call_id`. Relevant optional inputs include:

- `from` for an owned number; the documentation says uploaded Twilio numbers require the matching `encrypted_key` header. Otherwise Bland uses its number pool.
- `record: true` to make a recording available later.
- `summary_prompt` and `dispositions` to shape the result.
- `metadata` and `request_data` for Florence's own correlation/context.
- `webhook` and `webhook_events` for push delivery. The documented events include `queue`, `call`, `latency`, `webhook`, `tool`, `dynamic_data`, and `citations`; the `webhook` event fires after the call ends.

[`GET /v1/calls/{call_id}`](https://docs.bland.ai/api-v1/get/calls-id) returns lifecycle fields including `completed`, `queue_status`, `status`, and `error_message`, plus `summary`, `disposition`, `concatenated_transcript`, turn-level `transcripts`, `metadata`, and `recording_url` when recording was enabled. Documented terminal statuses include `completed`, `failed`, `busy`, `no-answer`, `canceled`, and `unknown`. The endpoint's queue states distinguish waiting, allocated, started, and complete calls.

[`POST /v1/calls/{call_id}/stop`](https://docs.bland.ai/api-v1/post/calls-id-stop) ends an active call and is also documented to cancel a scheduled call. A recording can alternatively be fetched as audio through [`GET /v1/recordings/{call_id}`](https://docs.bland.ai/api-v1/get/calls-id-recording).

### Results and webhooks

Bland's [post-call webhook guide](https://docs.bland.ai/tutorials/post-call-webhooks) shows the same useful result shape pushed after a call: call metadata, `summary`, `disposition`, `concatenated_transcript`, turn-level transcripts, and `recording_url`. The guide says audio processing can delay ordinary post-call delivery, while corrected transcripts/citations take roughly 30–60 seconds and require the `citations` event plus its dashboard setting. Bland also exposes [webhook delivery and attempt history by call ID](https://docs.bland.ai/api-v1/get/postcall-webhooks-get).

No webhook is needed for Florence's first flow. Polling call get until a terminal status gives Florence the same decision inputs without adding a public endpoint or another runtime.

### Idempotency and reconciliation

The reviewed Bland create-call contract does **not document** an `Idempotency-Key` header or equivalent request field. That is an absence from the published contract, not a claim that the provider can never deduplicate internally.

For an ambiguous create response, [`GET /v1/calls`](https://docs.bland.ai/api-v1/get/calls) can filter recent calls by destination/source and creation-time ranges. Florence should send a unique `florence_operation_id` in create-call `metadata`, list only the narrow target/time window, get each plausible candidate, and match its metadata. This is best-effort reconciliation, not atomic idempotency. If it yields zero or multiple plausible calls, Florence should mark the operation uncertain for review rather than place a second call.

## Vapi

### Create and caller identity

Vapi's [outbound-calling guide](https://docs.vapi.ai/calls/outbound-calling) creates a call with `POST /call`. It requires a saved or inline assistant (`assistantId` or `assistant`), the customer number, and `phoneNumberId`, which identifies the number Vapi calls from. The guide says outbound calling requires an imported number rather than Vapi's free test number; Vapi documents [importing a Twilio number](https://docs.vapi.ai/phone-numbers/import-twilio). [`GET /phone-number/{id}`](https://docs.vapi.ai/api-reference/phone-numbers/get) resolves the resource to its actual `number`, provider, organization, and status. Florence can therefore verify the caller identity instead of assuming an opaque ID represents the intended household/business number.

Vapi also permits scheduling through `schedulePlan`. The create response contains the call record and ID and, for a live call, monitor URLs.

### Get, end, and delete

[`GET /call/{id}`](https://docs.vapi.ai/api-reference/calls/get/) exposes status and `endedReason`, `analysis.summary`, transcript/messages, and recording artifacts. Vapi's [call-control guide](https://docs.vapi.ai/calls/call-features) documents posting `{"type":"end-call"}` to the create response's `monitor.controlUrl` to end an active call.

[`DELETE /call/{id}`](https://docs.vapi.ai/api-reference/calls/delete) deletes the stored call and its recordings. It should not be treated as the documented way to terminate a live call. The reviewed official pages did not establish a contract for canceling a scheduled single call before it starts; that remains an implementation blocker if Florence chooses Vapi scheduling.

### Results and webhooks

Vapi's [call-analysis documentation](https://docs.vapi.ai/assistants/call-analysis) says post-call analysis runs in the background, typically within seconds, and exposes a summary and configured structured outputs. Its [recording documentation](https://docs.vapi.ai/assistants/call-recording) places transcripts, messages, logs, and recordings on the call artifact. Recording and log URLs are private; the [artifact retrieval endpoints](https://docs.vapi.ai/assistants/retrieve-call-artifacts) return a short-lived signed redirect, so Florence should request a fresh URL rather than persist an expiring download link.

Vapi can push a [status update or `end-of-call-report`](https://docs.vapi.ai/server-url/events), whose payload includes recording, transcript, and messages. This is optional for the proposed first path because get-call polling exposes the finished record.

### Idempotency and reconciliation

The reviewed Vapi create-call contract does **not document** a caller-supplied idempotency key. Retaining the returned call ID is therefore essential. If the create response is ambiguous, a recent-calls query may let Florence compare customer number, caller number, and creation time, but the official material reviewed did not expose a clearly documented call-level custom correlation token comparable to Bland `metadata`. That makes recovery heuristic; Florence should not automatically retry an ambiguous create.

## Twilio

### Messages

[`POST /2010-04-01/Accounts/{AccountSid}/Messages.json`](https://www.twilio.com/docs/messaging/api/message-resource) creates a message. It needs `To`, a sender (`From` or `MessagingServiceSid`), and content (`Body`, `MediaUrl`, or `ContentSid`). The returned resource includes its Message SID and initial status. `StatusCallback` is optional.

The same resource documents:

- `GET /Messages/{Sid}.json` for one message.
- `GET /Messages.json` for a newest-first list, filterable by `To`, `From`, and sent-date bounds, with pagination through `next_page_uri`.
- statuses including `accepted`, `queued`, `sending`, `sent`, `delivered`, `undelivered`, `failed`, `receiving`, `received`, `scheduled`, `canceled`, and channel-specific `read`.

Twilio's [messaging webhook guide](https://www.twilio.com/docs/usage/webhooks/messaging-webhooks) supports status callbacks, but Florence can poll the Message resource for a low-volume first implementation.

### Calls

[`POST /2010-04-01/Accounts/{AccountSid}/Calls.json`](https://www.twilio.com/docs/voice/api/call-resource) requires `To` and `From`; `From` must be a Twilio number or verified outgoing caller ID. Florence must also supply how the call behaves through a URL, Application SID, or inline TwiML. That endpoint creates telephony, not a natural-language AI task.

`GET /Calls/{Sid}.json` fetches a Call, although Twilio calls out eventual consistency and recommends `StatusCallback` for real-time status. Documented statuses include `queued`, `ringing`, `in-progress`, `canceled`, `completed`, `busy`, `no-answer`, and `failed`. Updating an active Call with `Status=canceled` or `Status=completed` terminates it. A `completed` status only means the call connected and audio flowed; it does not establish that a dentist appointment was booked, or even that a human answered.

### Idempotency and reconciliation

Neither the current Message-create nor Call-create resource documentation publishes a client-supplied idempotency key. The similarly named [`I-Twilio-Idempotency-Token`](https://www.twilio.com/docs/usage/webhooks/webhooks-connection-overrides) applies to Twilio's retries when delivering a webhook **to Florence**; it is not a key Florence can send to deduplicate Message or Call creation.

If a Twilio create response is lost, Florence can list a narrow `To`/`From`/time window and compare message body or call timestamps, but neither resource exposes arbitrary Florence metadata. Treat this as heuristic reconciliation and do not blindly repeat the create request.

## Minimal provider-confirmed Florence flow

1. Create one durable Florence operation for “coordinate dentist appointment,” with an application-level operation ID, destination, permitted outcome/range, and a single owner. Atomically claim it before making any provider request.
2. Call Bland `POST /v1/calls` once with the dentist's E.164 number, a bounded task, and `metadata` containing the Florence operation/work IDs. Use an owned `from` number if a stable callback identity matters. Enable recording only if the product actually wants the audio; transcript and summary are the useful coordination outputs.
3. Persist `call_id` immediately. Poll call get with backoff until terminal. Do not add a webhook or new worker type for the first implementation.
4. If the user cancels, call the stop endpoint and continue polling until terminal.
5. On completion, record and surface the disposition, summary, and transcript-backed result. A provider `completed` state alone is transport status, not task success.
6. If the create response is ambiguous, pause automatic execution, query Bland's recent calls in a narrow window, and match the Florence metadata. Resume only with one unambiguous provider call; otherwise ask for review.
7. Add a webhook later only if polling latency or volume becomes a demonstrated problem. Add Twilio Messaging separately if Florence should text confirmations; it is not required to prove the voice coordination path.

## Uncertainties and access limitations

- “No idempotency key” means none was documented on the specific create endpoints reviewed on 2026-08-28; it is not proof of undocumented provider behavior.
- Vapi's generated API-reference pages were intermittently very large and timed out in the documentation crawler. The accessible official endpoint pages, guides, and first-party index/search material were used; no third-party API descriptions were substituted.
- Vapi active-call ending is documented through `monitor.controlUrl`, but a not-yet-started scheduled single-call cancellation contract was not found. `DELETE /call/{id}` is documented as data/recording deletion and must not be silently repurposed as cancellation.
- Provider status and generated analysis can arrive at different times. Polling should wait for the result fields Florence needs, not merely observe a transport-level terminal state and assume the task succeeded.
