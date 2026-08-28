Label: wayfinder:task
Type: task
Status: open
Blocked by: 04

# Port safe public-page reading

## Question

How can Florence read a parent-supplied or search-result public HTTP(S) page or PDF, including redirects, while rejecting credential-bearing/private-network URLs, bounding time and bytes, isolating public context, treating content as hostile evidence, and preserving direct sources?

Directly port and adapt Hermes's `tools/url_safety.py`, `tools/web_result_cache.py`, bounded-output helpers in `tools/web_tools.py`, and relevant cases in `tests/integration/test_web_tools.py` and `tests/agent/test_proxy_and_url_validation.py`. Florence-owned code should be limited to TypeScript adaptation, OpenAI source integration, and household-data egress rules.
