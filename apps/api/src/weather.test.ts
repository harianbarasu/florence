import { describe, expect, test, vi } from "vitest";
import { NwsWeatherClient, weatherForecastResultSchema } from "./weather.js";

function jsonResponse(value: unknown, status = 200, cacheControl = "max-age=300"): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/geo+json", "Cache-Control": cacheControl },
  });
}

function requestUrl(input: string | URL | Request): URL {
  return new URL(typeof input === "string" ? input : input instanceof URL ? input : input.url);
}

const pointReply = {
  properties: {
    gridId: "LOX",
    gridX: 154,
    gridY: 44,
    forecast: "https://api.weather.gov/gridpoints/LOX/154,44/forecast",
    forecastHourly: "https://api.weather.gov/gridpoints/LOX/154,44/forecast/hourly",
    forecastOffice: "https://api.weather.gov/offices/LOX",
    observationStations: "https://api.weather.gov/gridpoints/LOX/154,44/stations",
    timeZone: "America/Los_Angeles",
    relativeLocation: { properties: { city: "Los Angeles", state: "CA" } },
  },
};

const forecastReply = {
  properties: {
    generatedAt: "2026-08-28T18:00:00+00:00",
    updated: "2026-08-28T17:45:00+00:00",
    periods: [
      {
        number: 1,
        name: "This Afternoon",
        startTime: "2026-08-28T12:00:00-07:00",
        endTime: "2026-08-28T18:00:00-07:00",
        isDaytime: true,
        temperature: 78,
        temperatureUnit: "F",
        probabilityOfPrecipitation: { value: 10 },
        windSpeed: "5 to 10 mph",
        windDirection: "SW",
        shortForecast: "Mostly Sunny",
        detailedForecast: "Mostly sunny, with a high near 78.",
        icon: "https://api.weather.gov/icons/land/day/few?size=medium",
      },
      {
        number: 2,
        name: "Tonight",
        startTime: "2026-08-28T18:00:00-07:00",
        endTime: "2026-08-29T06:00:00-07:00",
        isDaytime: false,
        temperature: 64,
        temperatureUnit: "F",
        probabilityOfPrecipitation: { value: null },
        windSpeed: "5 mph",
        windDirection: "W",
        shortForecast: "Partly Cloudy",
        detailedForecast: "Partly cloudy, with a low around 64.",
      },
    ],
  },
};

const stationsReply = {
  features: [
    {
      id: "https://api.weather.gov/stations/KLAX",
      properties: { stationIdentifier: "KLAX", name: "Los Angeles International Airport" },
    },
  ],
};

const observationReply = {
  properties: {
    timestamp: "2026-08-28T17:53:00+00:00",
    textDescription: "Mostly Cloudy",
    temperature: { value: 25.6, unitCode: "wmoUnit:degC" },
    heatIndex: { value: 26.7, unitCode: "wmoUnit:degC" },
    windChill: { value: null, unitCode: "wmoUnit:degC" },
    relativeHumidity: { value: 51.234, unitCode: "wmoUnit:percent" },
    windDirection: { value: 275, unitCode: "wmoUnit:degree_(angle)" },
    windSpeed: { value: 4, unitCode: "wmoUnit:m_s-1" },
    windGust: { value: 12, unitCode: "wmoUnit:mi_h-1" },
  },
};

const alertsReply = {
  features: [
    {
      id: "https://api.weather.gov/alerts/urn:oid:example",
      properties: {
        id: "urn:oid:example",
        event: "Heat Advisory",
        headline: "Heat Advisory issued August 28",
        areaDesc: "Los Angeles County",
        severity: "Moderate",
        certainty: "Likely",
        urgency: "Expected",
        sent: "2026-08-28T10:00:00-07:00",
        effective: "2026-08-28T10:00:00-07:00",
        onset: "2026-08-28T12:00:00-07:00",
        expires: "2026-08-28T20:00:00-07:00",
        ends: "2026-08-28T20:00:00-07:00",
        description: "Hot conditions are expected.",
        instruction: "Drink plenty of fluids.",
      },
    },
  ],
};

describe("NwsWeatherClient", () => {
  test("normalizes a points-discovered forecast, latest observation, and active alerts", async () => {
    const fetchMock = vi.fn<typeof fetch>(async (input) => {
      const url = requestUrl(input);
      if (url.pathname.startsWith("/points/")) return jsonResponse(pointReply);
      if (url.pathname.endsWith("/forecast")) return jsonResponse(forecastReply);
      if (url.pathname.endsWith("/stations")) return jsonResponse(stationsReply);
      if (url.pathname.endsWith("/observations/latest")) return jsonResponse(observationReply);
      if (url.pathname === "/alerts/active") return jsonResponse(alertsReply);
      return jsonResponse({}, 404);
    });
    const client = new NwsWeatherClient({ fetch: fetchMock, now: () => Date.parse("2026-08-28T18:01:00Z") });

    const result = await client.run({
      coordinates: { lat: 34.052235, lon: -118.243683 },
      kind: "daily",
      periodCount: 1,
    });

    expect(result).toMatchObject({
      kind: "daily",
      location: { city: "Los Angeles", state: "CA", timeZone: "America/Los_Angeles" },
      periods: [
        {
          name: "This Afternoon",
          temperature: 78,
          precipitationChancePercent: 10,
          condition: "Mostly Sunny",
        },
      ],
      observation: {
        stationId: "KLAX",
        condition: "Mostly Cloudy",
        temperatureC: 25.6,
        feelsLikeC: 26.7,
        humidityPercent: 51.2,
        windSpeedKph: 14.4,
        windGustKph: 19.3,
      },
      activeAlertCount: 1,
      alerts: [{ event: "Heat Advisory", instruction: "Drink plenty of fluids." }],
      attribution: { provider: "National Weather Service" },
    });
    expect(result.periods).toHaveLength(1);
    expect(weatherForecastResultSchema.safeParse(result).success).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(5);
    expect(requestUrl(fetchMock.mock.calls[0]?.[0] ?? "https://invalid.test").pathname).toBe(
      "/points/34.0522,-118.2437",
    );
    for (const [, init] of fetchMock.mock.calls) {
      const headers = new Headers(init?.headers);
      expect(headers.get("User-Agent")).toContain("FlorenceFamilyAssistant");
      expect(headers.get("Accept")).toBe("application/geo+json");
    }
  });

  test("does not poll alerts inside 30 seconds and tolerates having no reporting station", async () => {
    let now = 0;
    let alertCalls = 0;
    const fetchMock = vi.fn<typeof fetch>(async (input) => {
      const url = requestUrl(input);
      if (url.pathname.startsWith("/points/")) return jsonResponse(pointReply, 200, "max-age=3600");
      if (url.pathname.endsWith("/forecast/hourly")) return jsonResponse(forecastReply, 200, "max-age=3600");
      if (url.pathname.endsWith("/stations")) return jsonResponse({ features: [] }, 200, "max-age=3600");
      if (url.pathname === "/alerts/active") {
        alertCalls += 1;
        return jsonResponse({ features: [] }, 200, "max-age=1");
      }
      return jsonResponse({}, 404);
    });
    const client = new NwsWeatherClient({ fetch: fetchMock, now: () => now });
    const request = {
      coordinates: { lat: 34.0522, lon: -118.2437 },
      kind: "hourly" as const,
      periodCount: 2,
    };

    const first = await client.run(request);
    now += 29_000;
    await client.run(request);
    now += 2_000;
    await client.run(request);

    expect(first.observation).toBeNull();
    expect(first.periods).toHaveLength(2);
    expect(alertCalls).toBe(2);
  });

  test("propagates cancellation and retries a transient provider failure once", async () => {
    const controller = new AbortController();
    const waitingFetch = vi.fn<typeof fetch>(
      async (_input, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
        }),
    );
    const cancelledClient = new NwsWeatherClient({ fetch: waitingFetch });
    const cancelled = cancelledClient.run(
      { coordinates: { lat: 34.0522, lon: -118.2437 }, kind: "daily", periodCount: 2 },
      controller.signal,
    );
    controller.abort(new Error("parent cancelled"));

    await expect(cancelled).rejects.toMatchObject({ code: "cancelled" });

    const failedFetch = vi.fn<typeof fetch>(async () => jsonResponse({}, 503));
    const waits: number[] = [];
    const unavailableClient = new NwsWeatherClient({
      fetch: failedFetch,
      async sleep(milliseconds) {
        waits.push(milliseconds);
      },
    });
    await expect(
      unavailableClient.run({
        coordinates: { lat: 34.0522, lon: -118.2437 },
        kind: "daily",
        periodCount: 2,
      }),
    ).rejects.toMatchObject({
      code: "unavailable",
      retryable: true,
      status: 503,
    });
    expect(failedFetch).toHaveBeenCalledTimes(2);
    expect(waits).toEqual([250]);
  });
});
