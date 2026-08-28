# Florence Engineering Rules

`PLAN.md` is the controlling product contract. Florence is judged by the lived two-adult iMessage and mobile-web experience: whether she notices, investigates, remembers, coordinates, and finishes useful family work—not by architecture, tool count, or test count.

## Product-first work

Every implementation change begins with one concrete sentence:

> The family can now ___ in iMessage or on the web.

If that sentence is not visible and testable by the pilot family, reject the change.

- Deepen an existing product module before creating another module.
- Do not add an abstraction until two current concrete callers need it.
- Replace obsolete paths in the same change; do not add compatibility layers or permanent fallbacks.
- A new table, process, queue, route, dependency, interface, persistent status, or test must explain why the existing core cannot own the user-visible behavior.
- Keep one durable source of household truth and one existing due-work path. Do not build feature-specific workflow engines.
- “Future-proofing,” “clean architecture,” generalized safety/privacy frameworks, and correctness by themselves are not product justifications.

## Reuse Pi and Hermes for real capabilities

Before writing an assistant capability from scratch, inspect the pinned upstream implementations:

- Pi commit `4e494929998d6bc4fccf75e0a233f727db4b70ee` at `/Users/harianbarasu/Projects/florence-upstreams/pi`.
- Hermes Agent commit `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882` at `/Users/harianbarasu/Projects/florence-upstreams/hermes-agent`.

Record the exact upstream files and reuse mode (`dependency`, `direct port`, `adapted port`, or `workflow copy`) when resolving an implementation ticket. Port the closest useful upstream behavior and its focused verification. Do not import coding, shell, terminal, repository-editing, or arbitrary-filesystem tools.

Use Florence-owned code for the family-specific experience and concrete provider integration. Do not create a second credential store, memory system, scheduler, messaging system, generic connector framework, or action control plane.

## Product behavior

- Florence is a broad noncoding household operator, not a Gmail/Calendar chatbot. Priority capabilities are maps, places, routes, time zones, weather, flights and travel alternatives, reminders, public pages and PDFs, durable multi-step work, household dockets, communication, broader Google Workspace, browser/computer use, and local services.
- Ordinary parent language is enough. Never add phrase allowlists, category gates, or URL-only search gates.
- Florence never chooses silence for an ordinary parent message. It may acknowledge with a natural reaction, but reactions and progress must correspond to real work.
- Read available harmless sources before asking for information Florence can derive. End with a useful result, partial findings plus one genuinely blocking question, or an honest failure.
- The initial review accounts for every retained received Gmail message from the prior 90 days and every event on every readable personal Calendar in the defined window. It retains useful context without dumping all of it into chat.
- The parental unit is the knowledge unit for validated family facts. A date found only on one adult's personal Calendar stays private and is offered to that owner before Florence copies or names it in the family Calendar. Once intentionally added there, it is household truth.
- Reminders support ordinary create/list/change/cancel/pause/resume/run/recurrence language. Longer work survives conversation turns, accepts steering or cancellation, and reports meaningful progress and a real terminal result.
- Keep the small Pi/Hermes-derived typed tool-execution kernel. Do not grow it into a universal policy, evidence, delivery, approval, or connector framework.

## Household foundation

- The standard household has two independently verified adults, represented children, a private Messages thread with each adult, one exact family group, each adult's own Google connection, and one Florence-created shared family calendar.
- Florence is conversational throughout setup. Incomplete setup limits unavailable capabilities but never turns ordinary language into a command protocol.
- The family group is the shared coordination layer; private threads remain the place for one adult's personal details.
- Both adults have equal Florence authority over the shared family calendar. Report a Calendar or other provider action only after it actually succeeds.
- Retained facts remain inspectable, correctable, and deletable. An explicit no-retention request wins.
- A user-requested reset removes Florence-created provider artifacts as well as database state.

## Verification

Extend the closest existing user narrative or focused provider test. Do not add framework matrices, snapshots, internal permutations, or unrelated tests.

Before committing or deploying, run:

```bash
pnpm check
```

Real completion is the two-phone, two-browser, live-account experience. A green internal framework is not a product result.
