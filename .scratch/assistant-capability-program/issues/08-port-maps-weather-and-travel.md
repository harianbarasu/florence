Label: wayfinder:task
Type: task
Status: open
Blocked by: 04, 07

# Add live weather and flight disruption help

## Question

How can Florence answer an ordinary weather question and turn “DL 747 is delayed tonight—find options” into a live route/status lookup plus useful alternative flights before asking for information the flight number already supplies?

Use one concrete NOAA/NWS adapter for pilot weather. Adapt the minimal flight-search operation from Hermes's pinned `optional-mcps/kiwi/manifest.yaml` or its official provider client, and keep hotel search for a later real travel workflow. Do not build a generic MCP/provider framework.
