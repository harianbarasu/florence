Label: wayfinder:task
Type: task
Status: resolved
Blocked by: 04, 07

# Add live weather and flight disruption help

## Question

How can Florence answer an ordinary weather question and turn “DL 747 is delayed tonight—find options” into a live route/status lookup plus useful alternative flights before asking for information the flight number already supplies?

Use one concrete NOAA/NWS adapter for pilot weather. Adapt the minimal flight-search operation from Hermes's pinned `optional-mcps/kiwi/manifest.yaml` or its official provider client, and keep hotel search for a later real travel workflow. Do not build a generic MCP/provider framework.

## Answer

Florence now has two concrete, live tools in its existing foreground reasoner loop:

- `weather_forecast` resolves exact U.S. coordinates through NOAA/NWS, returns daily or hourly periods, the latest nearby station observation, active alerts, the NWS place/time zone, and attribution. Ordinary named-place questions use the existing `maps_search` tool first. Outside NWS coverage, the reasoner falls back to public research.
- `flights_search` searches live Kiwi.com itineraries after the route/date are known. A flight-number disruption first uses the existing isolated public researcher to recover the operating date, status, route, and local times; the same turn then searches alternatives without asking the parent for origin/destination. Results preserve airport-local times, complete legs/segments, prices, useful cheapest/shortest/earliest options, and verified Kiwi booking links. Same-day disruption searches start direct with self-transfer, overnight layover, and airport changes disabled, and may broaden only when useful options are absent.

### Upstream reuse

- **Hermes Agent `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882`, `optional-mcps/kiwi/manifest.yaml`: direct contract adaptation.** Florence uses the same anonymous `https://mcp.kiwi.com` endpoint and exact `search-flight` operation, preserves the provider's search constraints and structured itinerary result, excludes `feedback-to-devs`, and leaves booking/payment on Kiwi.
- Hermes has no Kiwi provider client; its only implementation is the manifest plus the large generic MCP runtime in `tools/mcp_tool.py`. Florence therefore implements one typed `KiwiFlightSearchClient` rather than importing a generic MCP/provider framework, as this ticket requires.
- **Hermes Agent `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882`, `ui-tui/src/sdk/apps/weather.tsx`: behavior adaptation.** Florence retains the bounded fetch/error behavior and current-condition vocabulary (place, condition, actual/feels-like temperature, humidity, wind). It replaces Hermes's wttr.in snapshot with NOAA/NWS's authoritative points workflow because this ticket requires forecasts and active alerts as well as observations.

### Verification

- Focused provider and ordinary-language reasoner tests pass for location-to-weather and flight-number-to-status-to-alternatives, including exactly one real work cue and verified status/booking URLs.
- A live Los Angeles NWS request resolved `Los Angeles, CA`, returned two forecast periods, station `FHMC1`, and three active alerts.
- A live JFK-to-LAX Kiwi request returned six direct alternatives, airport-local times, and a valid `kiwi.com` booking link.
- API typecheck and the focused API suite pass; full repository checks are recorded in the implementing commit.
