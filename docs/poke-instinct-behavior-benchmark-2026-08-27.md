# Poke and Instinct: conversational-work benchmark

Research date: 2026-08-27

## Scope and identification

The Instinct referenced throughout this repository is confidently identified as
[Instinct at instinct.co](https://instinct.co/). `PLAN.md`, `README.md`, and the existing
[`instinct-state-2026-08-22.md`](./instinct-state-2026-08-22.md) all point to that product.

This note uses only public first-party product pages, documentation, release notes, FAQs, and
terms. It does not use an authenticated Instinct or Poke account, and it does not infer private
implementation details from marketing copy. That matters because neither company publicly
documents a response-time SLA, an iMessage-reaction policy, or a complete task lifecycle.

## Bottom line

The useful benchmark is not that either assistant sounds human. It is that an ordinary message is
treated as a request to **do the available work now**.

- Poke explicitly makes web search, email, calendar, reminders, and connected tools available from
  natural conversation. Its API describes one consistent flow: receive the message, decide which
  tools to use, take action, and respond.
- Instinct describes the broader version of the same contract: text or call the assistant, let it
  use connected devices and services, and have it follow up or complete real-world tasks.
- Neither product's public material proves that it reacts to every message, acknowledges within a
  particular number of seconds, or reports progress in a particular way. Poke does document native
  affordances such as typing indicators, read receipts, and inline replies on some messaging
  channels. It does **not** publicly document iMessage Tapbacks as an assistant behavior.
- Both products warn that model output or actions can be wrong. Their public documentation is much
  weaker on visible task-state and provider-confirmed completion than their “just ask” positioning.
  Florence can be materially better here.

For the Jackson flight example, this means “DL 747 is her original flight” should have triggered
public lookup immediately. The flight number is context to resolve, not a reason to wait for the
user to restate a route that search may recover. If one genuinely decision-relevant constraint is
still missing after lookup—such as whether another airline is acceptable—Florence can ask that
single question while presenting what she already found.

## What the official sources establish

| Product behavior | Poke's public contract | Instinct's public contract | Florence implication |
| --- | --- | --- | --- |
| Ordinary language becomes work | Poke says users can “chat naturally” to manage email, calendar, reminders, web search, and integrations. Its API says each inbound message is evaluated, tools are selected, action is taken, and a response is sent. [Poke docs](https://poke.com/docs), [Poke API](https://poke.com/docs/api) | Instinct says there are “no new interfaces”: users text or call, and the assistant uses a phone and computer as humans do. [Instinct product page](https://instinct.co/) | Do not require a URL, magic phrase, or category-specific intent before making harmless read/search tools available. |
| Search and identifier resolution | Web search is a built-in Poke capability, separate from connected integrations. Poke's release notes say it verifies web-search sources when available. [Poke docs](https://poke.com/docs), [release notes](https://poke.com/docs/release-notes) | Instinct publicly describes access to connected applications and devices but does not publish a search/tool catalog. [Instinct product page](https://instinct.co/) | Resolve public identifiers, current status, routes, places, and comparable options before asking the parent for information the web can supply. Include direct sources for time-sensitive claims. |
| Background and proactive work | Poke advertises proactive updates and background automations; its pricing and usage docs say automations run in the background and that the assistant keeps responding even in reduced-usage mode. [Poke pricing](https://poke.com/pricing), [usage and resets](https://poke.com/docs/usage-and-resets) | Instinct says it follows up on dropped threads, proactively calls or texts, and arranges real-world services. [Instinct product page](https://instinct.co/) | Future-tense language such as “I’ll prioritize that” is valid only after durable work actually exists. Otherwise search in the current turn or state the blocker. |
| Delegation beyond the model | Poke Human dispatches certain real-world tasks to an operator and says the operator sees only the details required for that task, not chat history, memories, or integrations. [Poke FAQ](https://poke.com/faq) | Instinct's terms authorize interaction with connected services and actions responsive to user input, including purchases and commitments. [Instinct terms](https://instinct.co/terms) | The useful pattern is explicit escalation to a capable executor with least-necessary context—not pretending that a conversational promise is execution. Florence should retain its narrower approval boundary. |
| Native messaging presence | Poke documents WhatsApp read receipts, typing indicators, and inline replies, plus iMessage inline-reply understanding. [Poke release notes](https://poke.com/docs/release-notes) | Instinct says the assistant can text and call, but its public pages do not document reactions, typing, read receipts, or inline replies. [Instinct product page](https://instinct.co/) | Use native cues to make real work feel present: a genuine reaction or typing cue can acknowledge receipt, but it must not substitute for a result. Do not claim competitor Tapback behavior without evidence. |
| Acceptance versus completion | Poke's API defines `success` as successful **message delivery to the assistant**; tool selection and action happen afterward. [Poke API](https://poke.com/docs/api) | Instinct distinguishes model output from actions taken on connected services. [Instinct terms](https://instinct.co/terms) | Preserve three distinct truths: the request was received, Florence is working, and the result was provider-verified. Never collapse them into one optimistic sentence. |
| Errors and verification | Poke's release notes warn that it can make mistakes and that important information should be verified; its terms say output may be inaccurate. [Poke release notes](https://poke.com/docs/release-notes), [Poke terms](https://poke.com/terms) | Instinct's terms say outputs and actions may be incorrect or incomplete and that action records may not always be accurate. [Instinct terms](https://instinct.co/terms) | If a lookup or action fails, say what failed and what the parent can do next. Report completion only from the relevant source/provider, not from model intent or request acceptance. |

## Recommended Florence interaction contract

These timings are product recommendations for Florence, not claims about either competitor:

1. **Acknowledge within 1–2 seconds when work will continue.** Prefer a native typing cue or a
   context-appropriate reaction. A reaction should mean “I saw this” or “I started,” never “done.”
2. **Do the first useful read/search before asking a question.** Resolve public identifiers and use
   already-authorized private context. Ask only for the smallest constraint that remains genuinely
   unknowable or consequential.
3. **Return ordinary lookups in roughly 10–30 seconds.** If the work is still running after about
   5–10 seconds, send one short truthful update such as “Checking the live options now.”
4. **By roughly 60 seconds, return a result or an honest blocker.** Do not leave an open-ended “I’ll
   look” message with no durable job and no later turn.
5. **Keep task state semantic, not chatty.** One receipt cue, at most one useful progress update, and
   then the result. Reactions and filler messages should not create duplicate work or notification
   noise.
6. **Separate research from authority.** Public lookup, comparison, and drafting can happen
   automatically. Purchases, outside contact, commitments, and private disclosure still require
   Florence's exact existing approval boundary.
7. **Close every loop.** The final message should be one of: a source-backed answer, a
   provider-confirmed outcome, one focused question with partial findings, or an explicit failure
   and next step.

## What to copy—and what not to overfit

Copy:

- Poke's “just ask” path from natural language to tools;
- the expectation that web search is a normal capability, not a link-only special case;
- Instinct's persistence and proactive follow-up as a product promise;
- native messaging cues that truthfully reflect active work;
- Poke Human's least-context escalation principle; and
- the distinction between request receipt and later action completion.

Do not overfit to:

- flight numbers as a bespoke intent;
- a reaction on every message;
- unsupported claims that Poke or Instinct has a particular response-time SLA;
- unsupported claims that either product's public UX always reports background progress; or
- Instinct's broad action authority. Florence's narrower family privacy and approval contract is an
  advantage, not missing parity.

## Source-confidence notes

**High confidence:** product identity; Poke's documented natural-language email/calendar/reminder/
web-search capabilities; Poke's API message-to-tool/action flow; Poke's public background-
automation positioning; Poke Human's stated data boundary; Instinct's text/call/device/action
positioning; and both products' published accuracy warnings.

**Not publicly established:** exact response latency, progress-message cadence, iMessage Tapback
behavior, internal orchestration architecture, and the frequency with which either product closes a
promised task successfully. Those should remain Florence product decisions rather than competitor
claims.
