Label: wayfinder:research
Type: research
Status: resolved
Blocked by: 02, 03, 10, 15

# Curate optional family connectors

## Question

Which of Hermes's assistant-relevant optional connectors—Todoist, Dropbox, Notion, Calendly, AllTrails, Home Assistant, Spotify, media generation, and similar providers—earn a concrete family behavior, and what minimal read/write tool subset, credentials, authority owner, retention, approval, and disconnect rules should each receive?

Start from Hermes's pinned `optional-mcps/*/manifest.yaml`, `tools/mcp_tool.py`, Home Assistant and Spotify toolsets, and related tests. Reuse manifest/tool metadata and protocol code where safe; MCP remains transport and may expose only individually allowlisted capabilities already authorized by Florence.

## Answer

Discarded as a standalone connector-catalog research phase. Evaluate and port one provider only when it unlocks a named family behavior; start with the maps and flight providers in tickets 07 and 08.
