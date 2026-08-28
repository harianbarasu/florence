Label: wayfinder:prototype
Type: prototype
Status: resolved
Blocked by: 01, 02, 03, 10

# Design exact-action approval and settlement

## Question

What deep action interface can stage an exact preview, bind the correct account owner's approval to recipients/content/target/cost/version, perform one provider effect, read the provider state back, retry delivery without replaying the effect, and reconcile ambiguous outcomes before any Gmail send, form submission, reservation, booking, purchase, smart-home change, or account mutation is allowed?

Adapt Pi's intent/effect/settlement and replay-class model from `packages/agent/docs/harness.md`; port Hermes's approval and ambiguous-effect recovery patterns from `tools/approval.py`, `tools/write_approval.py`, `agent/replay_cleanup.py`, and execution-ledger tests. Florence must own adult authority, exact-action digesting, provider idempotency, receipts, cancellation/refund facts, and live Linq audience checks.

## Answer

Discarded as a standalone architecture prototype. Approval and result confirmation will be implemented inside each concrete provider workflow when Florence can actually perform that action.
