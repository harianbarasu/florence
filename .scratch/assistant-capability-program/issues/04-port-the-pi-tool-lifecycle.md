Label: wayfinder:task
Type: task
Status: resolved
Blocked by: 01

# Port the useful Pi/Hermes tool-execution kernel

## Question

How can Florence run several real assistant tools with typed inputs and outputs, cancellation, useful progress, and one terminal result without building a second assistant runtime or generalized policy framework?

## Answer

The existing reasoner now uses one small typed execution kernel adapted from Pi and Hermes. It validates model tool arguments and provider outputs, runs sequential or parallel calls, handles timeout/cancellation, bounds progress and results, preserves model-call order, and emits requested/admitted/running/progress/terminal events for natural presentation.

The earlier overbuilt pieces were removed after the user corrected the program toward product capability: the one-method `FlorenceCapabilities` facade, branded store locators, speculative registry generations, universal source/provenance envelopes, two-stage policy admission, generic read-only enforcement, outbound source/fact dependency replay, and framework-only test matrix. Concrete availability and source checks stay beside the actual Florence tool that needs them.

### Upstream reuse

- Pi `4e494929998d6bc4fccf75e0a233f727db4b70ee` — adapted port of ordered tool execution, cancellation, progress, terminal handling, and source-order results from `packages/agent/src/agent-loop.ts` and the closest cases in `packages/agent/test/agent-loop.test.ts`.
- Hermes Agent `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882` — adapted port of typed registry entries, duplicate-name rejection, availability, and dispatch from `tools/registry.py`.

### Verification

Four focused kernel tests cover malformed/truncated no-execution, timeout/cancellation, late progress, and parallel result ordering. Product tests cover actual Gmail attachments, calendar reads, and work cues.
