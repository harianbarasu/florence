import { describe, expect, test, vi } from "vitest";
import {
  FlightsProviderError,
  type FlorenceFlightSearchRequest,
  flightSearchResultSchema,
  KiwiFlightSearchClient,
} from "./flights.js";

const basicRequest: FlorenceFlightSearchRequest = {
  operation: "search",
  origin: "LAX",
  destination: "JFK",
  departureDate: "2026-09-10",
};

function structured(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    query: "LAX → JFK on 10/09/2026, 1 adult",
    currency: "USD",
    passengers: { adults: 1, children: 0, infants: 0 },
    resultsCount: 0,
    itineraries: [],
    searchTimeMs: 25,
    error: null,
    ...overrides,
  };
}

function rpcResponse(init: RequestInit | undefined, value: Record<string, unknown>): Response {
  const call = JSON.parse(String(init?.body)) as { id: string };
  return new Response(
    JSON.stringify({
      jsonrpc: "2.0",
      id: call.id,
      result: {
        content: [{ type: "text", text: "This field is deliberately ignored." }],
        structuredContent: value,
        isError: false,
      },
    }),
    { status: 200, headers: { "Content-Type": "application/json" } },
  );
}

function itinerary(
  id: string,
  options: {
    price?: number;
    duration?: number;
    departure?: string;
    bookingUrl?: string;
  } = {},
): Record<string, unknown> {
  const departure = options.departure ?? "2026-09-10T12:00:00";
  return {
    id,
    price: options.price ?? 200,
    priceFormatted: `${options.price ?? 200} USD`,
    totalDurationSeconds: options.duration ?? 18_000,
    bookingUrl: options.bookingUrl ?? `https://kiwi.com/u/${id}`,
    imageId: "new-york-city_ny_us",
    baggage: { personalItem: 2, cabinBag: 1, checkedBag: 0 },
    outbound: {
      from: "LAX",
      to: "JFK",
      departureTime: departure,
      arrivalTime: "2026-09-10T20:00:00",
      durationSeconds: options.duration ?? 18_000,
      stops: 0,
      route: ["LAX", "JFK"],
      cabinClass: "Economy",
      segments: [
        {
          from: "LAX",
          to: "JFK",
          fromCity: "Los Angeles",
          toCity: "New York",
          fromName: "Los Angeles International",
          toName: "John F. Kennedy International",
          fromCountry: "United States",
          toCountry: "United States",
          departureTime: departure,
          arrivalTime: "2026-09-10T20:00:00",
          durationSeconds: options.duration ?? 18_000,
          carrier: "DL",
          carrierName: "Delta Air Lines",
          flightNumber: "DL747",
          cabinClass: "Economy",
        },
      ],
    },
    inbound: null,
  };
}

describe("KiwiFlightSearchClient", () => {
  test("calls only Kiwi search-flight and maps Florence ISO dates and family constraints exactly", async () => {
    const fetchMock = vi.fn<typeof fetch>(async (_input, init) => rpcResponse(init, structured()));
    const client = new KiwiFlightSearchClient({ fetch: fetchMock, retries: 0 });

    await client.search({
      operation: "search",
      origin: "Los Angeles",
      destination: "Paris",
      departureDate: "2026-10-03",
      departureDateEnd: "2026-10-05",
      returnDate: "2026-10-12",
      returnFlexDays: 2,
      adults: 2,
      children: 2,
      infants: 0,
      cabinClass: "premium_economy",
      currency: "usd",
      maxStops: 1,
      maxPrice: 4_000,
      maxDurationHours: 18,
      preferredAirlines: ["dl", "af"],
      outboundDepartureHours: { from: 6, to: 13 },
      maxLayoverHours: 4,
      checkedBagsPerAdult: [1, 1],
      cabinBagsPerAdult: [1, 1],
      checkedBagsPerChild: [0, 0],
      cabinBagsPerChild: [1, 1],
      requiredStopoverCountries: ["gb"],
      outboundWeekdays: [5, 6],
      sort: "quality",
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [input, init] = fetchMock.mock.calls[0] ?? [];
    expect(String(input)).toBe("https://mcp.kiwi.com/");
    expect(init?.method).toBe("POST");
    expect(new Headers(init?.headers).get("Accept")).toBe("application/json, text/event-stream");
    expect(new Headers(init?.headers).get("Content-Type")).toBe("application/json");
    expect(new Headers(init?.headers).get("MCP-Protocol-Version")).toBe("2025-03-26");
    const call = JSON.parse(String(init?.body));
    expect(call).toMatchObject({
      jsonrpc: "2.0",
      method: "tools/call",
      params: {
        name: "search-flight",
        arguments: {
          flyFrom: "Los Angeles",
          flyTo: "Paris",
          departureDate: "03/10/2026",
          departureDateTo: "05/10/2026",
          departureDateFlexDays: 0,
          returnDate: "12/10/2026",
          returnDateFlexDays: 2,
          adults: 2,
          children: 2,
          infants: 0,
          cabinClass: "W",
          currency: "USD",
          locale: "en",
          one_for_city: false,
          max_sector_stopovers: 1,
          price_to: 4_000,
          max_fly_duration: 18,
          select_airlines: "DL,AF",
          dtime_from: 6,
          dtime_to: 13,
          stopover_to: 4,
          adults_hold_bags: [1, 1],
          adults_hand_bags: [1, 1],
          children_hold_bags: [0, 0],
          children_hand_bags: [1, 1],
          allow_self_transfer: false,
          allow_overnight_stopovers: false,
          allow_diff_airport_connection: false,
          stopover_countries: "GB",
          fly_days: "5,6",
          sort: "quality",
        },
      },
    });
    expect(call.params.arguments).not.toHaveProperty("feedback-to-devs");
  });

  test("parses Streamable HTTP SSE and uses only structuredContent with the full itinerary shape", async () => {
    const fetchMock = vi.fn<typeof fetch>(async (_input, init) => {
      const call = JSON.parse(String(init?.body)) as { id: string };
      const envelope = {
        jsonrpc: "2.0",
        id: call.id,
        result: {
          content: [
            {
              type: "text",
              text: JSON.stringify(structured({ error: "The untrusted text path must not win" })),
            },
          ],
          structuredContent: structured({
            resultsCount: 1,
            itineraries: [itinerary("full-result")],
          }),
          isError: false,
        },
      };
      return new Response(`event: message\ndata: ${JSON.stringify(envelope)}\n\n`, {
        status: 200,
        headers: { "Content-Type": "text/event-stream" },
      });
    });
    const result = await new KiwiFlightSearchClient({ fetch: fetchMock, retries: 0 }).search(basicRequest);

    expect(result).toMatchObject({
      operation: "search",
      resultsCount: 1,
      returnedCount: 1,
      timeBasis: "provider_local_time_at_each_airport",
      provider: { name: "Kiwi.com", searchOnly: true, bookingOccursOnProvider: true },
      itineraries: [
        {
          id: "full-result",
          bookingUrl: "https://kiwi.com/u/full-result",
          outbound: {
            departureTime: "2026-09-10T12:00:00",
            segments: [
              {
                flightNumber: "DL747",
                carrierName: "Delta Air Lines",
                fromName: "Los Angeles International",
                toName: "John F. Kennedy International",
              },
            ],
          },
          highlights: ["cheapest", "shortest", "earliest"],
        },
      ],
    });
    expect(flightSearchResultSchema.safeParse(result).success).toBe(true);
  });

  test("bounds results while preserving cheapest, shortest, and earliest choices and strips unsafe booking links", async () => {
    const values = Array.from({ length: 12 }, (_, index) =>
      itinerary(`option-${index}`, {
        price: index === 11 ? 50 : 300 + index,
        duration: index === 10 ? 3_000 : 20_000 + index,
        departure:
          index === 9
            ? "2026-09-10T05:00:00"
            : `2026-09-10T${String(10 + (index % 10)).padStart(2, "0")}:00:00`,
        ...(index === 0 ? { bookingUrl: "https://attacker.example/book" } : {}),
      }),
    );
    const fetchMock = vi.fn<typeof fetch>(async (_input, init) =>
      rpcResponse(init, structured({ resultsCount: 12, itineraries: values })),
    );
    const result = await new KiwiFlightSearchClient({ fetch: fetchMock, retries: 0 }).search(basicRequest);

    expect(result.resultsCount).toBe(12);
    expect(result.returnedCount).toBe(8);
    expect(result.itineraries.map((item) => item.id)).toEqual(
      expect.arrayContaining(["option-9", "option-10", "option-11"]),
    );
    expect(result.highlights).toEqual({
      cheapestItineraryId: "option-11",
      shortestItineraryId: "option-10",
      earliestItineraryId: "option-9",
    });
    expect(result.itineraries.find((item) => item.id === "option-11")?.highlights).toContain("cheapest");
    expect(result.itineraries[0]?.bookingUrl).toBeNull();
  });

  test("reports provider-declared errors without falling back to text or retrying", async () => {
    const fetchMock = vi.fn<typeof fetch>(async (_input, init) => {
      const call = JSON.parse(String(init?.body)) as { id: string };
      return new Response(
        JSON.stringify({
          jsonrpc: "2.0",
          id: call.id,
          result: {
            content: [{ type: "text", text: JSON.stringify(structured()) }],
            structuredContent: structured(),
            isError: true,
          },
        }),
        { headers: { "Content-Type": "application/json" } },
      );
    });
    const client = new KiwiFlightSearchClient({ fetch: fetchMock, retries: 2 });

    await expect(client.search(basicRequest)).rejects.toMatchObject({
      code: "provider_error",
      safeMessage: "Kiwi.com could not complete that flight search.",
      retryable: false,
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  test("retries a transient HTTP failure once", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response("temporarily unavailable", { status: 503 }))
      .mockImplementationOnce(async (_input, init) => rpcResponse(init, structured()));
    const client = new KiwiFlightSearchClient({ fetch: fetchMock, retries: 1 });

    await expect(client.search(basicRequest)).resolves.toMatchObject({ operation: "search" });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  test("cancels in-flight provider work without retrying", async () => {
    const fetchMock = vi.fn<typeof fetch>(
      async (_input, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), {
            once: true,
          });
        }),
    );
    const client = new KiwiFlightSearchClient({ fetch: fetchMock, retries: 2 });
    const controller = new AbortController();
    const pending = client.search(basicRequest, controller.signal);
    controller.abort();

    await expect(pending).rejects.toEqual(expect.any(FlightsProviderError));
    await expect(pending).rejects.toMatchObject({ code: "cancelled", retryable: false });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  test("turns a provider timeout into a retryable typed error", async () => {
    const fetchMock = vi.fn<typeof fetch>(
      async (_input, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), {
            once: true,
          });
        }),
    );
    const client = new KiwiFlightSearchClient({ fetch: fetchMock, retries: 0, timeoutMs: 5 });

    await expect(client.search(basicRequest)).rejects.toMatchObject({
      code: "timeout",
      retryable: true,
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
