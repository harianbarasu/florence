Label: wayfinder:prototype
Type: prototype
Status: resolved
Blocked by: 01

# Freeze source egress and authority

## Question

What single source-egress and authority matrix covers each source owner, permitted query fields, provider, result audience, retention/deletion rule, consequence class, approval owner, and disconnect behavior for the newly approved capability breadth?

Adapt Pi's policy-hook shape from `packages/agent/src/types.ts` and `packages/agent/src/agent-loop.ts`, plus Hermes's capability metadata in `tools/registry.py` and `toolsets.py`. Florence must own the household-specific policy values and live audience rechecks.

## Answer

This prototype was discarded after the user corrected the program away from generalized safety/privacy infrastructure. The authority-matrix document was removed.

Concrete product boundaries remain where the family experience requires them: one adult's personal Calendar is not named or copied into the family Calendar without asking that adult, the shared family Calendar is household-visible, and Florence reports provider actions only after they actually happen. New providers should implement the smallest behavior their real capability needs rather than conforming to a universal policy framework.
