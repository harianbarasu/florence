Label: wayfinder:prototype
Type: prototype
Status: open
Blocked by: 01, 02

# Choose one Florence capability interface

## Question

What 1–3-entry-point deep module can replace the current scattered model-tool wiring while keeping callers ignorant of schemas, availability, privacy, timeouts, cancellation, progress, output bounds, and terminal outcomes?

Design it twice from Pi's `AgentTool` and lifecycle types in `packages/agent/src/types.ts` and `packages/agent/src/agent-loop.ts`, and Hermes's registration/discovery implementations in `tools/registry.py`, `toolsets.py`, and `tools/tool_search.py`. The chosen interface must deepen the current reasoner rather than introduce a parallel runtime or a speculative connector framework.
