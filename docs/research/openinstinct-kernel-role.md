# OpenInstinct's actual use of Kernel

Source baseline: `Merit-Systems/OpenInstinct` at commit [`480045dbc63008e7f99313d1683858cd8657b35a`](https://github.com/Merit-Systems/OpenInstinct/tree/480045dbc63008e7f99313d1683858cd8657b35a), plus Kernel's first-party documentation.

## Decisive conclusion

Florence does **not** need saved logins to get the main product value from Kernel. In OpenInstinct, Kernel is the cloud browser used after the agent has a known site and needs a real rendered page, deterministic interaction, visual reasoning, browser-local state, or a form/action. Public discovery and ordinary current-information lookup happen through `web_search`, while a known public page is tried through `web_fetch` before browser automation. OpenInstinct explicitly tells its browser worker not to use search engines or browse search-result pages. ([root routing instructions](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/instructions.md#L38-L40), [worker-routing rule](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/instructions.md#L16-L19), [browser skill](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/skills/browser-execution/SKILL.md#L8-L11))

The useful model is therefore:

`search/fetch -> useful target URL -> Kernel browser when interaction or rendered state adds value`

Kernel can read a public page when Florence already knows its URL, and OpenInstinct has browser benchmarks for exactly that. It is not the search index or discovery layer. ([browser benchmarks](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/lib/browser/benchmark-tasks.ts#L1-L36))

## What Kernel supplies

- An on-demand Chromium session with a session ID, Playwright/CDP endpoints, and a live-view URL. Kernel expects the agent to create, drive, and tear down these sessions. ([Kernel: Create](https://kernel.sh/docs/introduction/create))
- Fast, deterministic in-browser Playwright for navigation, DOM inspection, extraction, and interaction. OpenInstinct exposes this as a bounded worker tool. ([OpenInstinct Playwright tool](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/tools/execute_playwright_code.ts#L24-L40), [Kernel: Control](https://kernel.sh/docs/introduction/control))
- OS-level mouse, keyboard, and screenshots for pages where visual reasoning or coordinate input is more reliable. OpenInstinct keeps this as the fallback alongside Playwright. ([OpenInstinct computer tool](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/tools/computer_action.ts#L104-L173), [Kernel: Computer Controls](https://kernel.sh/docs/browsers/computer-controls))
- A resumable live browser while a task is waiting for an OTP, approval, CAPTCHA, or another human action; otherwise OpenInstinct deletes the session at the end of the bounded job. ([worker lifecycle](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/instructions.md#L22-L32), [Kernel: Live View](https://kernel.sh/docs/browsers/live-view))

## Session and profile persistence

OpenInstinct creates one stable Kernel profile per workspace, named from a hash of the workspace ID, and launches browser sessions against it. Ordinary sessions load the profile read-only, so multiple tasks can run concurrently. The worker reuses one browser for a bounded job and passes the known target as `start_url`. ([profile creation and naming](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/tools/manage_browsers.ts#L232-L255), [browser creation](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/tools/manage_browsers.ts#L48-L95))

Kernel profiles persist cookies and local storage across browser sessions. `save_changes: true` writes a session's state back to the profile when that browser closes; Kernel warns that concurrent writers replace rather than merge state. ([Kernel: Profiles](https://kernel.sh/docs/auth/profiles)) OpenInstinct consequently switches to a writable browser only immediately before login, permits one profile writer, and deletes it after authentication so the resulting cookies are retained. ([OpenInstinct browser skill](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/skills/browser-execution/SKILL.md#L9-L16), [writer coordination](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/tools/manage_browsers.ts#L55-L95))

Two different things are being persisted:

1. **Active task state:** the current Kernel browser session and its live page, retained only when the task is waiting for the user.
2. **Reusable browser state:** cookies/local storage in a Kernel profile, useful after a login has occurred.

Neither requires Florence to build a login-management UI before public browsing works.

## Where credentials and the vault fit

Saved credentials are an additive authenticated-action feature, not a prerequisite for browser research. A public/anonymous Kernel session uses no vault item. When a known site presents a sign-in form, OpenInstinct's worker lists only safe vault metadata and opaque handles, then its own server reads the encrypted secret and injects it into the focused form. ([safe vault listing](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/tools/list_vault.ts#L6-L18), [vault fill path](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/tools/fill_from_vault.ts#L23-L53), [encrypted secret store](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/lib/manager/server/secret-store.ts#L19-L41))

The pinned implementation uses OpenInstinct's own encrypted workspace vault and native form injection; it does not call Kernel Managed Auth. Kernel profiles retain the resulting authenticated cookies afterward. This separation is why the browser loop works without any saved credential and why login support can be added later without redesigning the general agent loop.

## Minimum Florence tranche to copy now

For the product the user described, copy the general browser loop first:

1. Keep public discovery/current facts on a broad search tool and use a lightweight fetch/read path for known public pages.
2. Route to Kernel for a known URL when Florence needs a rendered/JS-heavy page, visual inspection, browser-local state, clicking, comparison on the real site, or an external action.
3. Create one on-demand browser at the target URL; use Playwright first and computer controls when visual interaction is materially better.
4. Carry the same Kernel session through the durable Florence task. Keep it open only while waiting on genuinely blocking user input; close it on success, failure, or cancellation.
5. Optionally attach a read-only household profile now so ordinary browser state can be reused. Do not create a writable profile session until a login flow actually occurs.
6. Defer saved-login management and credential injection. Add them when Florence is ready to complete authenticated actions, without making them a gate for search, public research, recommendations, or anonymous website work.

In short: **Kernel-first browser execution is core; saved logins are optional phase-two depth.** For “searching and things,” Florence wants both layers: search/fetch for discovery, Kernel for actually using the web.
