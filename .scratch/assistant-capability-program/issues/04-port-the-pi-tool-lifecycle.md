Label: wayfinder:task
Type: task
Status: open
Blocked by: 03

# Port the Pi tool lifecycle into Florence

## Question

How can the family receive truthful tool start, bounded progress, cancellation, terminal success/failure/unknown, and follow-up behavior through the chosen Florence capability interface without replacing Florence's transactional decision and delivery core?

Use an adapted port of Pi's `packages/agent/src/types.ts`, `packages/agent/src/agent-loop.ts`, `packages/agent/test/agent-loop.test.ts`, and relevant retry classification in `packages/coding-agent/src/core/agent-session.ts`. Preserve Florence's strict structured decision, source validation, `commitTurn()` authority recheck, and Linq-owned delivery.
