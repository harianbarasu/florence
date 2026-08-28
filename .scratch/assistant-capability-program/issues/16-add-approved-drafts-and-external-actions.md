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
- Provider review now covers the hard failure cases: Gmail forwards retrieve externally stored body parts and bound large draft readback; draft send binds the exact draft and Message-ID; Bland ambiguous starts reconcile by correlation metadata; calls cannot be overwritten by another call or cleared by a stale ID; phone polling is paced; and cancellation retains the provider ID until terminal status is confirmed.
- The Instinct benchmark is outcome-based rather than search-count-based: rebooking, reservations, inbox/attachment work, shopping, cancellation, and cross-channel follow-through are the next real-family rehearsals.
- Remaining before this ticket is resolved: live inbound phone help and one provider-confirmed reservation, booking, purchase, cancellation, or service journey through final receipt.
