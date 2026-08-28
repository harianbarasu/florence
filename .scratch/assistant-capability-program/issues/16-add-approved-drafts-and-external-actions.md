Label: wayfinder:task
Type: task
Status: open
Assignee: /root
Blocked by: 13, 14

# Add communication, bookings, and real errands

## Question

How can a parent call Florence for live help, have Florence draft, forward, and attach files in Gmail, route an explicit family-group request through the initiating adult's account, text or call people and businesses, and complete one real reservation, booking, purchase, cancellation, or service workflow at a time so the parent receives the outcome rather than instructions for doing it themselves?

Extend the Hermes-derived Gmail work with drafts, forwards, attachments, and result readback, and port navigation/form interaction from Hermes's browser implementation. Keep Florence and Linq for Messages delivery. Port the pinned Hermes telephony skill's useful Bland and Twilio operations into the existing durable worker. Add each outside action through a concrete family scenario and provider; ask only for a genuinely missing choice or at the actual commitment boundary, and do not build a generic action framework first.

## Progress

- Florence can create/read/send exact Gmail provider drafts, including replies, forwards, Gmail attachments, and Drive-file attachments. Group-requested work uses the initiating parent's connected Google account.
- Florence can place and poll Bland conversational calls, send/poll/read Twilio SMS, and place/poll Twilio spoken calls. Durable retries do not blindly repeat an ambiguous call or text, and cancelling the Florence task stops a tracked active call.
- Durable work carries the initiating parent's exact message, edits, reply context, images, and PDFs into the task instead of relying on a condensed model-written objective.
- Authenticated browser work can upload one exact image or PDF from that initiating Messages context into the current site, so a parent can send Florence a form once instead of being handed the upload step back.
- Provider review now covers the hard failure cases: Gmail forwards retrieve externally stored body parts and bound large draft readback; draft send binds the exact draft and Message-ID; Bland ambiguous starts reconcile by correlation metadata; calls cannot be overwritten by another call or cleared by a stale ID; phone polling is paced; and cancellation retains the provider ID until terminal status is confirmed.
- The existing camp-registration journey now reaches a provider-confirmed outcome: because the initiating request asks only to get the registration ready for final review, Florence pauses there; after the parent explicitly steers it to submit, Florence clicks Submit exactly once and, when the click result is uncertain but the refreshed provider page contains the confirmation, returns the exact camp session and confirmation reference without repeating the click.
- The Instinct benchmark is outcome-based rather than search-count-based: rebooking, reservations, inbox/attachment work, shopping, cancellation, and cross-channel follow-through are the next real-family rehearsals.
- Remaining before this ticket is resolved: live inbound phone help and a live provider rehearsal of a reservation, booking, purchase, cancellation, or service journey through final receipt. The camp journey uses a simulated provider response plus direct adapter verification; it is not evidence of a live transaction.

## Upstream reuse

- The browser session lifecycle, compact accessibility snapshots, ref-based navigation and form actions, owner handoff, and uncertain-effect handling are directly adapted from Hermes Agent's `plugins/browser/browserbase/provider.py` and `tools/browser_tool.py` at the pinned revision recorded in Florence's browser adapter.
- The upload command uses the existing pinned `agent-browser` 0.26 dependency's native `upload <ref> <path>` operation inside the same Hermes-derived Browserbase session. Pinned Hermes does not expose upload through `tools/browser_tool.py`, so Florence adds only the missing attachment-ref-to-temporary-file bridge; it does not add a filesystem tool or file-management framework.
- The camp registration's request-scope, steering, and exact terminal-result semantics are Florence-owned because pinned Hermes has no booking-execution implementation: its Kiwi, Trivago, and Calendly capabilities are search or remote manifests, not a durable workflow that follows an initial prepare-only request through a later submit instruction and verifies a provider commitment.
- Telephony directly adapts Hermes's `optional-skills/productivity/telephony/SKILL.md` and `scripts/telephony.py`. Florence owns provider reconciliation, cancellation, and ambiguity tests because upstream blindly posts starts and does not cover those failure cases.
- Gmail composition behavior follows Hermes's `skills/email/himalaya/references/message-composition.md`, `skills/email/himalaya/SKILL.md`, and `skills/email/email-inbox-triage/SKILL.md`. Florence owns the Google REST draft, forward, attachment, and reconciliation implementation because upstream provides CLI/MML guidance rather than those provider operations.
