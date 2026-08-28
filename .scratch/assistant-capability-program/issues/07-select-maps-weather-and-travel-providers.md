Label: wayfinder:task
Type: task
Status: resolved
Blocked by: 04

# Port maps, places, routes, and time zones

## Question

How can a parent ask Florence to find a place, resolve an address, search nearby, compare routes and travel time, or determine a time zone without Florence falling back to generic web-search prose?

Directly adapt Hermes's operation contract and implementation from `skills/productivity/maps/SKILL.md` and `skills/productivity/maps/scripts/maps_client.py`, preserving search, reverse, nearby, distance, directions, time-zone, area, and bounding-box behavior. Choose the concrete maps provider inside this implementation ticket; do not add a provider-selection framework.

## Answer

A parent can now ask Florence in ordinary language to resolve a place or address, reverse-geocode coordinates, find nearby places, compare route distance and time, get turn-by-turn directions, resolve an exact time zone, inspect a named area's bounds, or search one category inside a bounding box. Eight strict model tools run through Florence's existing capability lifecycle and visible-work cue, then return structured provider results with map or directions links and attribution. Production wires the concrete client directly into the existing Florence reasoner seam; there is no provider framework, new runtime, database, scheduler, or policy layer.

The concrete provider stack is Nominatim for geocoding, Overpass for places, Valhalla for driving/walking/cycling routes, and TimeAPI for exact time zones. The client serializes and caches Nominatim reads, uses current Overpass fallbacks, bounds provider responses, honors cancellation/timeouts, and emits honest provider errors. A materially ambiguous name now returns its leading candidates instead of silently routing from the first geocoder hit. When a parent asks whether a returned place is open or otherwise current, Florence passes only the public place candidates into its existing isolated web lookup so that lookup can verify the selected result.

### Upstream reuse

- Hermes Agent `6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882` — adapted port of all eight operation contracts, category/tag vocabulary, dual bakery tags and religion filters, Overpass query construction, POI normalization and deduplication, distance sorting, promoted metadata, Google map/directions links, Haversine math, and area calculations from `skills/productivity/maps/SKILL.md` and `skills/productivity/maps/scripts/maps_client.py`.
- Florence corrects three upstream defects verified against live providers: Valhalla replaces OSRM so walking and cycling are real route modes, TimeAPI's offset is interpreted as total seconds, and an unavailable time-zone lookup never becomes a fabricated longitude-based exact zone. Native typed fetch with `AbortSignal` replaces Hermes's Python/shell execution.

### Verification

Six focused provider cases plus route and map-to-web tool-loop cases pass. Repo lint, all workspace typechecks, all tests, and all builds pass (18 tests passed and 3 database-dependent tests skipped). Live calls through `OpenStreetMapsClient` exercised all eight operations, resolved the Statue of Liberty, returned `America/Los_Angeles` with `-07:00`, produced materially different driving and walking durations for the same endpoints, returned three nearby Times Square restaurants with usable map and directions links, and refused to guess which distant Springfield the parent meant.
