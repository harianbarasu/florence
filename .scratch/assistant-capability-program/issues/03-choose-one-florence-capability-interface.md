Label: wayfinder:prototype
Type: prototype
Status: resolved
Blocked by: 01, 02

# Choose one Florence capability interface

## Question

What 1–3-entry-point deep module can replace the current scattered model-tool wiring while keeping callers ignorant of schemas, availability, privacy, timeouts, cancellation, progress, output bounds, and terminal outcomes?

Design it twice from Pi's `AgentTool` and lifecycle types in `packages/agent/src/types.ts` and `packages/agent/src/agent-loop.ts`, and Hermes's registration/discovery implementations in `tools/registry.py`, `toolsets.py`, and `tools/tool_search.py`. The chosen interface must deepen the current reasoner rather than introduce a parallel runtime or a speculative connector framework.

## Answer

This prototype was discarded. The one-method `FlorenceCapabilities` facade was a middleman around one callback and added no family behavior, so the facade and design document were removed.

Florence keeps the small Pi/Hermes-derived typed tool-execution kernel inside the existing reasoner. New tools deepen that direct seam. A new public abstraction is allowed only when a concrete capability has a second real caller that needs it.
