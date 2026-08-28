Label: wayfinder:task
Type: task
Status: open
Blocked by: 04

# Port maps, places, routes, and time zones

## Question

How can a parent ask Florence to find a place, resolve an address, search nearby, compare routes and travel time, or determine a time zone without Florence falling back to generic web-search prose?

Directly adapt Hermes's operation contract and implementation from `skills/productivity/maps/SKILL.md` and `skills/productivity/maps/scripts/maps_client.py`, preserving search, reverse, nearby, distance, directions, time-zone, area, and bounding-box behavior. Choose the concrete maps provider inside this implementation ticket; do not add a provider-selection framework.
