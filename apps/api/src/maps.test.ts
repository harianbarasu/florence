import { describe, expect, test, vi } from "vitest";
import { florenceMapsResultSchema, MapsProviderError, OpenStreetMapsClient } from "./maps.js";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function requestUrl(input: string | URL | Request): URL {
  return new URL(typeof input === "string" ? input : input instanceof URL ? input : input.url);
}

describe("OpenStreetMapsClient", () => {
  test("ports Hermes geocoding, reverse lookup, and area math with serialized cached Nominatim reads", async () => {
    let now = 0;
    const waits: number[] = [];
    const fetchMock = vi.fn<typeof fetch>(async (input) => {
      const url = requestUrl(input);
      if (url.pathname.endsWith("/reverse")) {
        return jsonResponse({
          lat: "40.758",
          lon: "-73.9855",
          display_name: "Times Square, Manhattan, New York",
          osm_type: "node",
          osm_id: 123,
          address: {
            road: "Broadway",
            city: "New York",
            state: "New York",
            postcode: "10036",
            country: "United States",
            country_code: "us",
          },
        });
      }
      if (url.searchParams.get("q") === "Manhattan") {
        return jsonResponse([
          {
            lat: "40.7896",
            lon: "-73.9599",
            display_name: "Manhattan, New York",
            type: "administrative",
            category: "boundary",
            osm_type: "relation",
            osm_id: 8398124,
            boundingbox: ["40.6839", "40.8775", "-74.0479", "-73.9067"],
          },
        ]);
      }
      return jsonResponse([
        {
          lat: "40.758",
          lon: "-73.9855",
          name: "Times Square",
          display_name: "Times Square, Manhattan, New York",
          type: "attraction",
          category: "tourism",
          osm_type: "node",
          osm_id: 987,
          importance: 0.8,
          boundingbox: ["40.757", "40.759", "-73.987", "-73.984"],
          address: {
            house_number: "1560",
            road: "Broadway",
            city: "New York",
            state: "New York",
            postcode: "10036",
            country: "United States",
          },
          extratags: {
            phone: "+12125550123",
            website: "www.timessquarenyc.org",
          },
        },
      ]);
    });
    const client = new OpenStreetMapsClient({
      fetch: fetchMock,
      now: () => now,
      async sleep(milliseconds) {
        waits.push(milliseconds);
        now += milliseconds;
      },
    });

    const firstSearch = await client.run({ operation: "search", query: "Times Square", limit: 1 });
    const cachedSearch = await client.run({ operation: "search", query: "Times Square", limit: 1 });
    const reverse = await client.run({
      operation: "reverse",
      coordinates: { lat: 40.758, lon: -73.9855 },
    });
    const area = await client.run({ operation: "area", query: "Manhattan" });

    expect(firstSearch).toEqual(cachedSearch);
    expect(firstSearch).toMatchObject({
      operation: "search",
      count: 1,
      results: [
        {
          name: "Times Square",
          address: "1560 Broadway, New York, New York, 10036, United States",
          lat: 40.758,
          lon: -73.9855,
          phone: "+12125550123",
          website: "https://www.timessquarenyc.org/",
        },
      ],
    });
    expect(reverse).toMatchObject({
      operation: "reverse",
      address: { road: "Broadway", city: "New York", countryCode: "us" },
    });
    expect(area).toMatchObject({
      operation: "area",
      boundingBox: { south: 40.6839, north: 40.8775, west: -74.0479, east: -73.9067 },
    });
    expect(area.operation === "area" && area.approximateAreaKm2).toBeGreaterThan(0);
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(waits).toEqual([1_000, 1_000]);
    const firstHeaders = new Headers(fetchMock.mock.calls[0]?.[1]?.headers);
    expect(firstHeaders.get("User-Agent")).toContain("github.com/harianbarasu/florence");
    expect(requestUrl(fetchMock.mock.calls[0]?.[0] ?? "").searchParams.get("extratags")).toBe("1");
    expect(florenceMapsResultSchema.safeParse(area).success).toBe(true);
  });

  test("ports Hermes nearby and bbox behavior while falling back to the current Overpass mirror", async () => {
    let privateCoffeeCalls = 0;
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = requestUrl(input);
      if (url.hostname === "overpass-api.de") return jsonResponse({}, 503);
      privateCoffeeCalls += 1;
      const body = String(init?.body ?? "");
      if (decodeURIComponent(body).includes("around:")) {
        return jsonResponse({
          elements: [
            {
              type: "node",
              id: 2,
              lat: 34.0525,
              lon: -118.2437,
              tags: { name: "Second Bakery", shop: "bakery" },
            },
            {
              type: "way",
              id: 1,
              center: { lat: 34.05221, lon: -118.24371 },
              tags: {
                name: "First Bakery",
                amenity: "bakery",
                cuisine: "pastry",
                opening_hours: "Mo-Su 07:00-17:00",
              },
            },
          ],
        });
      }
      return jsonResponse({
        elements: [
          {
            type: "node",
            id: 3,
            lat: 34.05,
            lon: -118.25,
            tags: { name: "Box Bakery", shop: "bakery" },
          },
        ],
      });
    });
    const client = new OpenStreetMapsClient({ fetch: fetchMock });

    const nearby = await client.run({
      operation: "nearby",
      center: { lat: 34.0522, lon: -118.2437 },
      categories: ["bakery"],
      radiusM: 1_000,
      limit: 2,
    });
    const bbox = await client.run({
      operation: "bbox",
      bounds: { south: 34.1, west: -118.1, north: 34, east: -118.3 },
      category: "bakery",
      limit: 1,
    });

    expect(nearby).toMatchObject({ operation: "nearby", count: 2 });
    expect(nearby.operation === "nearby" && nearby.results[0]).toMatchObject({
      name: "First Bakery",
      cuisine: "pastry",
      hours: "Mo-Su 07:00-17:00",
    });
    expect(nearby.operation === "nearby" && nearby.results[0]?.directionsUrl).toContain(
      "google.com/maps/dir",
    );
    expect(bbox).toMatchObject({
      operation: "bbox",
      bounds: { south: 34, west: -118.3, north: 34.1, east: -118.1 },
      results: [{ name: "Box Bakery" }],
    });
    expect(privateCoffeeCalls).toBe(2);
    const calls = fetchMock.mock.calls.map(([input]) => requestUrl(input).hostname);
    expect(calls).toEqual([
      "overpass-api.de",
      "overpass.private.coffee",
      "overpass-api.de",
      "overpass.private.coffee",
    ]);
    const nearbyBody = decodeURIComponent(String(fetchMock.mock.calls[1]?.[1]?.body));
    expect(nearbyBody).toContain('node["shop"="bakery"]');
    expect(nearbyBody).toContain('node["amenity"="bakery"]');
  });

  test("uses Valhalla's real bicycle and pedestrian costings for distance and directions", async () => {
    const fetchMock = vi.fn<typeof fetch>(async () =>
      jsonResponse({
        trip: {
          status: 0,
          status_message: "Found route between points",
          summary: { length: 1.25, time: 300 },
          legs: [
            {
              maneuvers: [
                {
                  type: 1,
                  instruction: "Walk east on 1st Street.",
                  length: 0.25,
                  time: 60,
                  street_names: ["1st Street"],
                },
                {
                  type: 4,
                  instruction: "Turn right onto Main Street.",
                  length: 1,
                  time: 240,
                  street_names: ["Main Street"],
                },
              ],
            },
          ],
        },
      }),
    );
    const client = new OpenStreetMapsClient({ fetch: fetchMock });
    const origin = { lat: 34.0522, lon: -118.2437 };
    const destination = { lat: 34.0622, lon: -118.2337 };

    const distance = await client.run({
      operation: "distance",
      origin,
      destination,
      mode: "cycling",
    });
    const directions = await client.run({
      operation: "directions",
      origin,
      destination,
      mode: "walking",
    });

    expect(distance).toMatchObject({
      operation: "distance",
      mode: "cycling",
      distanceM: 1_250,
      durationSeconds: 300,
    });
    expect(directions).toMatchObject({ operation: "directions", mode: "walking", stepCount: 2 });
    expect(directions.operation === "directions" && directions.steps[0]).toMatchObject({
      instruction: "Walk east on 1st Street.",
      distanceM: 250,
    });
    const [cyclingCall, walkingCall] = fetchMock.mock.calls;
    expect(requestUrl(cyclingCall?.[0] ?? "https://invalid.test").hostname).toBe(
      "valhalla1.openstreetmap.de",
    );
    expect(JSON.parse(String(cyclingCall?.[1]?.body))).toMatchObject({ costing: "bicycle" });
    expect(JSON.parse(String(walkingCall?.[1]?.body))).toMatchObject({ costing: "pedestrian" });
    expect(new Headers(walkingCall?.[1]?.headers).get("X-Client-Id")).toBe("FlorenceFamilyAssistant");
  });

  test("treats TimeAPI offset seconds as the total UTC offset and never fabricates a timezone", async () => {
    const exactClient = new OpenStreetMapsClient({
      fetch: vi.fn<typeof fetch>(async () =>
        jsonResponse({
          timeZone: "America/Los_Angeles",
          currentLocalTime: "2026-08-28T09:00:00",
          currentUtcOffset: { seconds: -25_200 },
        }),
      ),
    });
    const exact = await exactClient.run({
      operation: "timezone",
      coordinates: { lat: 34.0522, lon: -118.2437 },
    });
    expect(exact).toEqual(
      expect.objectContaining({
        operation: "timezone",
        timezone: "America/Los_Angeles",
        utcOffset: "-07:00",
        exact: true,
      }),
    );

    const unavailableClient = new OpenStreetMapsClient({
      fetch: vi.fn<typeof fetch>(async () => jsonResponse({ currentUtcOffset: { seconds: -25_200 } })),
    });
    await expect(
      unavailableClient.run({
        operation: "timezone",
        coordinates: { lat: 34.0522, lon: -118.2437 },
      }),
    ).rejects.toMatchObject({
      code: "invalid_response",
      safeMessage: "Exact timezone information is temporarily unavailable for that location.",
    });
  });

  test("refuses to route from a materially ambiguous first geocoder hit", async () => {
    const fetchMock = vi.fn<typeof fetch>(async () =>
      jsonResponse([
        {
          lat: "39.7990175",
          lon: "-89.6439575",
          display_name: "Springfield, Illinois, United States",
          importance: 0.61,
        },
        {
          lat: "42.1018764",
          lon: "-72.5886727",
          display_name: "Springfield, Massachusetts, United States",
          importance: 0.6,
        },
      ]),
    );
    const client = new OpenStreetMapsClient({ fetch: fetchMock });

    await expect(
      client.run({
        operation: "distance",
        origin: "Springfield",
        destination: { lat: 34.0522, lon: -118.2437 },
        mode: "driving",
      }),
    ).rejects.toMatchObject({
      code: "invalid_input",
      safeMessage: expect.stringContaining("Use place search"),
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(requestUrl(fetchMock.mock.calls[0]?.[0] ?? "https://invalid.test").searchParams.get("limit")).toBe(
      "5",
    );
  });

  test("rejects invalid or cancelled work before contacting a provider", async () => {
    const fetchMock = vi.fn<typeof fetch>();
    const client = new OpenStreetMapsClient({ fetch: fetchMock });
    await expect(client.run({ operation: "search", query: "x", limit: 99 })).rejects.toBeInstanceOf(
      MapsProviderError,
    );

    const controller = new AbortController();
    controller.abort();
    await expect(
      client.run({ operation: "search", query: "Los Angeles" }, controller.signal),
    ).rejects.toMatchObject({ code: "cancelled", retryable: false });
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
