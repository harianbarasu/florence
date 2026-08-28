Label: wayfinder:task
Type: task
Status: open
Blocked by: 04, 06, 10

# Give Florence authenticated browser and computer use

## Question

How can Florence use a browser or computer like a capable assistant for sites without an API—researching, comparing, signing in with the owning adult, navigating, filling forms, and carrying one real errand through to its actual commitment point—while the durable task keeps reporting progress and can be steered or cancelled?

Port the navigation, accessibility snapshot, click/type/scroll, screenshot, session, and cleanup behavior from Hermes's `tools/browser_tool.py`, browser plugin sources, and focused browser tests. Exclude terminal and model-written-Python paths. Prove the port with one named family errand; do not stop at a public-page reader or build a generic browser-policy framework.
