Label: wayfinder:task
Type: task
Status: resolved
Blocked by: 04

# Read linked public pages and PDFs

## Question

How can Florence follow a parent-supplied or search-result link, read the useful content from a public page or PDF, and bring the answer back into the conversation instead of stopping at search snippets?

Directly port and adapt Hermes's `tools/url_safety.py`, `tools/web_result_cache.py`, bounded-output helpers in `tools/web_tools.py`, and the closest integration cases. Keep URL validation, redirects, timeouts, and byte bounds inside this concrete reader; do not create a reusable safety subsystem.

## Answer

Florence can now read the actual clean text of a public HTML page, plain-text page, Markdown page, or PDF that a parent put in the conversation or that public research returned. The same concrete tool is available in an ordinary turn and at a persisted durable-work checkpoint, so Florence can follow a result, inspect the source, and answer from the page instead of stopping at a search snippet or promising to look later.

The reader is one adapter on Florence's existing tool lifecycle. It fetches and extracts locally, follows bounded redirects, returns a deterministic 15,000-character head/tail view, and keeps a small successful-result memory cache. It does not add a provider framework, browser runtime, queue, database state, policy registry, generalized safety layer, or another research abstraction. The reasoner admits only links already supplied by the parent or returned by research; a model-invented URL is rejected rather than fetched.

### Upstream reuse

- Hermes Agent commit `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882`, `tools/url_safety.py` — adapted URL normalization, public-target validation, DNS-address checks, IP-pinned requests, and validation again after every redirect into this single concrete reader.
- Hermes Agent at the same commit, `tools/web_tools.py` — adapted raw extraction without another model summary, base64-image removal, the 15,000-character default, and deterministic 75/25 head-tail truncation.
- Hermes Agent at the same commit, `tools/web_result_cache.py` — adapted successful-only, bounded 20-minute memory caching.
- Hermes delegates page extraction to Firecrawl, Tavily, Exa, or Parallel and has no native HTML or PDF parser to port. Florence therefore owns the HTML cleanup and local `pdf-parse` extraction; that avoids adding a managed extraction gateway merely to read a link.
- Hermes's plugin registry, provider selection, gateway configuration, disk cache, website-policy subsystem, debug/profile machinery, container mounts, and coding-file tools were intentionally not imported because none of them unlocks this family behavior.

### Verification

- `pnpm check` passes: lint, every workspace typecheck, 38 tests with 4 existing database-gated tests skipped, and all builds.
- `pnpm --filter @florence/api exec vitest run src/public-page.test.ts src/reasoner-tool-loops.test.ts` passes 19 focused tests.
- A live probe reads `https://example.com` as clean HTML and W3C's public `dummy.pdf` as local PDF text.
- Focused coverage proves parent-supplied page reads, verified search-result follow-through, durable PDF work across checkpoints, successful cache behavior, redirect revalidation, unsupported/oversized response failures, and rejection of a model-invented URL.
