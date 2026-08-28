Label: wayfinder:task
Type: task
Status: open
Blocked by: 13, 14

# Add communication, bookings, and real errands

## Question

How can a parent call Florence for live help, have Florence send or reply in Gmail, text or call people and businesses, and complete one real reservation, booking, purchase, cancellation, or service workflow at a time so the parent receives the outcome rather than instructions for doing it themselves?

Port Gmail send/reply and read-back contracts from Hermes's Google Workspace skill and `google_api.py`, and navigation/form interaction from Hermes's browser implementation. Keep Florence and Linq for Messages delivery. Select a real bidirectional voice/telephony/SMS provider separately because the pinned Hermes source does not ship a general phone-call assistant tool. Add each outside action through a concrete family scenario and provider; ask only for a genuinely missing choice or at the actual commitment boundary, and do not build a generic action framework first.
