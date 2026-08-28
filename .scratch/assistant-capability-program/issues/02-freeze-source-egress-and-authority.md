Label: wayfinder:prototype
Type: prototype
Status: open
Blocked by: 01

# Freeze source egress and authority

## Question

What single source-egress and authority matrix covers each source owner, permitted query fields, provider, result audience, retention/deletion rule, consequence class, approval owner, and disconnect behavior for the newly approved capability breadth?

Adapt Pi's policy-hook shape from `packages/agent/src/types.ts` and `packages/agent/src/agent-loop.ts`, plus Hermes's capability metadata in `tools/registry.py` and `toolsets.py`. Florence must own the household-specific policy values and live audience rechecks.
