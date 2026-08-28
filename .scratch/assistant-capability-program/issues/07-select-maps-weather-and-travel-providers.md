Label: wayfinder:research
Type: research
Status: open
Blocked by: 02

# Select maps, weather, and travel providers

## Question

Which concrete read-only providers and upstream adapters give Florence dependable geocoding, nearby places, directions, travel time, time zones, deterministic weather, flight-number route/status resolution, and alternative flight/hotel search with usable terms, attribution, outage behavior, and date/time-zone semantics?

Start from Hermes's `skills/productivity/maps/scripts/maps_client.py`, its maps skill contract, and the pinned `optional-mcps/kiwi`, `optional-mcps/trivago`, and relevant travel manifests. Hermes has no dedicated weather implementation; document why the chosen weather adapter must be Florence-owned or taken from another primary-source SDK.
