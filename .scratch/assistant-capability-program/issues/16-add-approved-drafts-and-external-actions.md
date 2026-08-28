Label: wayfinder:task
Type: task
Status: open
Blocked by: 13, 14, 15

# Add approved drafts and external actions

## Question

How can Florence first create provider-backed Gmail drafts, then send/reply, contact an outside person, submit a form, or complete a reservation/booking/purchase only after the owning adult approves the exact staged action and Florence can return a provider-confirmed receipt or honest unknown state?

Port the Gmail operation contracts and read-back rules from Hermes's Google Workspace skill and `google_api.py`, and reuse only the isolated browser operations admitted by the settlement interface. Implement each consequence class as its own Florence-owned adapter; do not expose a generic browser submit, generic MCP call, or generic send-message tool to the model.
