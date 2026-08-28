Label: wayfinder:task
Type: task
Status: open
Blocked by: 02, 04, 06, 10

# Port an isolated read-only browser

## Question

How can Florence inspect a public JavaScript-rendered page through an ephemeral, task-isolated browser with allowlisted egress, accessibility snapshots, bounded artifacts, cleanup, and prompt-injection defenses, but no ambient household credentials, stored login, upload, form submission, purchase, booking, or external message?

Port the minimum navigation/snapshot/session-isolation implementation from Hermes's `tools/browser_tool.py`, browser plugin sources, `tests/gateway/test_browser_control_broker_hardening.py`, `tests/gateway/test_browser_control_artifacts.py`, and `tests/plugins/browser/test_browser_provider_plugins.py`. Remove terminal and model-written-Python paths rather than wrapping them.
