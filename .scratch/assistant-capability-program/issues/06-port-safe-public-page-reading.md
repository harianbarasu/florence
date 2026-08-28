Label: wayfinder:task
Type: task
Status: open
Blocked by: 04

# Read linked public pages and PDFs

## Question

How can Florence follow a parent-supplied or search-result link, read the useful content from a public page or PDF, and bring the answer back into the conversation instead of stopping at search snippets?

Directly port and adapt Hermes's `tools/url_safety.py`, `tools/web_result_cache.py`, bounded-output helpers in `tools/web_tools.py`, and the closest integration cases. Keep URL validation, redirects, timeouts, and byte bounds inside this concrete reader; do not create a reusable safety subsystem.
