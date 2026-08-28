import { z } from "zod";

/**
 * Adapted port of Hermes Agent's approved Kiwi MCP catalog entry at commit
 * 6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882
 * (`optional-mcps/kiwi/manifest.yaml`). Florence keeps its anonymous,
 * vendor-hosted Streamable HTTP endpoint, exact `search-flight` operation,
 * full structured result, and search-only boundary. The unrelated
 * `feedback-to-devs` operation is intentionally not exposed.
 */

const DEFAULT_KIWI_MCP_URL = "https://mcp.kiwi.com";
const MCP_PROTOCOL_VERSION = "2025-03-26";
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_RETRIES = 1;
const MAX_RESPONSE_BYTES = 4 * 1_024 * 1_024;
const MAX_RETURNED_ITINERARIES = 8;
const MAX_PROVIDER_ITINERARIES = 100;
const MAX_SEGMENTS_PER_LEG = 12;
const MAX_LOCATION_CHARS = 100;

const isoDateSchema = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}$/, "Use an ISO date in YYYY-MM-DD format")
  .refine(isCalendarDate, "Use a real calendar date");
const airlineCodeSchema = z
  .string()
  .trim()
  .toUpperCase()
  .regex(/^[A-Z0-9]{2}$/);
const airportCodeSchema = z
  .string()
  .trim()
  .toUpperCase()
  .regex(/^[A-Z]{3}$/);
const countryCodeSchema = z
  .string()
  .trim()
  .toUpperCase()
  .regex(/^[A-Z]{2}$/);
const hourRangeSchema = z
  .object({ from: z.number().int().min(0).max(23), to: z.number().int().min(0).max(23) })
  .strict();
const bagCountSchema = z.number().int().min(0).max(2);
const weekdaySchema = z.number().int().min(0).max(6);

export const flightSearchRequestSchema = z
  .object({
    operation: z.literal("search"),
    origin: z.string().trim().min(1).max(MAX_LOCATION_CHARS),
    destination: z.string().trim().min(1).max(MAX_LOCATION_CHARS),
    departureDate: isoDateSchema,
    departureDateEnd: isoDateSchema.optional(),
    departureFlexDays: z.number().int().min(0).max(10).default(0),
    returnDate: isoDateSchema.optional(),
    returnDateEnd: isoDateSchema.optional(),
    returnFlexDays: z.number().int().min(0).max(10).default(0),
    adults: z.number().int().min(1).max(9).default(1),
    children: z.number().int().min(0).max(8).default(0),
    infants: z.number().int().min(0).max(4).default(0),
    cabinClass: z.enum(["economy", "premium_economy", "business", "first"]).optional(),
    currency: z
      .string()
      .trim()
      .toUpperCase()
      .regex(/^[A-Z]{3}$/)
      .default("USD"),
    locale: z.string().trim().min(1).max(10).default("en"),
    minNightsAtDestination: z.number().int().min(0).optional(),
    maxNightsAtDestination: z.number().int().min(0).optional(),
    onePerDestinationCity: z.boolean().default(false),
    maxStops: z.number().int().min(0).max(5).optional(),
    minPrice: z.number().int().min(0).optional(),
    maxPrice: z.number().int().min(0).optional(),
    maxDurationHours: z.number().int().min(1).max(72).optional(),
    preferredAirlines: z.array(airlineCodeSchema).min(1).max(20).optional(),
    excludedAirlines: z.array(airlineCodeSchema).min(1).max(20).optional(),
    outboundDepartureHours: hourRangeSchema.optional(),
    outboundArrivalHours: hourRangeSchema.optional(),
    returnDepartureHours: hourRangeSchema.optional(),
    returnArrivalHours: hourRangeSchema.optional(),
    minLayoverHours: z.number().int().min(0).max(48).optional(),
    maxLayoverHours: z.number().int().min(0).max(72).optional(),
    checkedBagsPerAdult: z.array(bagCountSchema).max(9).optional(),
    cabinBagsPerAdult: z.array(bagCountSchema.max(1)).max(9).optional(),
    checkedBagsPerChild: z.array(bagCountSchema).max(8).optional(),
    cabinBagsPerChild: z.array(bagCountSchema.max(1)).max(8).optional(),
    allowSelfTransfer: z.boolean().default(false),
    allowOvernightStopovers: z.boolean().default(false),
    allowAirportChanges: z.boolean().default(false),
    requiredStopoverAirports: z.array(airportCodeSchema).min(1).max(20).optional(),
    excludedStopoverAirports: z.array(airportCodeSchema).min(1).max(20).optional(),
    requiredStopoverCountries: z.array(countryCodeSchema).min(1).max(20).optional(),
    excludedStopoverCountries: z.array(countryCodeSchema).min(1).max(20).optional(),
    outboundWeekdays: z.array(weekdaySchema).min(1).max(7).optional(),
    returnWeekdays: z.array(weekdaySchema).min(1).max(7).optional(),
    sort: z.enum(["price", "duration", "quality", "date", "popularity"]).default("price"),
  })
  .strict()
  .superRefine((request, context) => {
    checkDateRange(request.departureDate, request.departureDateEnd, "departureDateEnd", context);
    if (request.departureDateEnd && request.departureFlexDays > 0) {
      context.addIssue({
        code: "custom",
        path: ["departureFlexDays"],
        message: "Do not combine a departure date range with flexible days",
      });
    }
    if (request.returnDateEnd && !request.returnDate) {
      context.addIssue({
        code: "custom",
        path: ["returnDateEnd"],
        message: "returnDateEnd requires returnDate",
      });
    }
    if (request.returnDate) {
      checkDateRange(request.returnDate, request.returnDateEnd, "returnDateEnd", context);
      if (request.returnDate < request.departureDate) {
        context.addIssue({
          code: "custom",
          path: ["returnDate"],
          message: "Return date cannot be before departure date",
        });
      }
    } else if (request.returnFlexDays > 0) {
      context.addIssue({
        code: "custom",
        path: ["returnFlexDays"],
        message: "returnFlexDays requires returnDate",
      });
    }
    if (request.returnDateEnd && request.returnFlexDays > 0) {
      context.addIssue({
        code: "custom",
        path: ["returnFlexDays"],
        message: "Do not combine a return date range with flexible days",
      });
    }
    checkMinMax(
      request.minNightsAtDestination,
      request.maxNightsAtDestination,
      "maxNightsAtDestination",
      context,
    );
    checkMinMax(request.minPrice, request.maxPrice, "maxPrice", context);
    checkMinMax(request.minLayoverHours, request.maxLayoverHours, "maxLayoverHours", context);
    if (request.preferredAirlines && request.excludedAirlines) {
      context.addIssue({
        code: "custom",
        path: ["excludedAirlines"],
        message: "Choose preferred airlines or excluded airlines, not both",
      });
    }
    if (request.infants > request.adults) {
      context.addIssue({
        code: "custom",
        path: ["infants"],
        message: "Each infant needs an accompanying adult",
      });
    }
    checkBagArray(request.checkedBagsPerAdult, request.adults, "checkedBagsPerAdult", context);
    checkBagArray(request.cabinBagsPerAdult, request.adults, "cabinBagsPerAdult", context);
    checkBagArray(request.checkedBagsPerChild, request.children, "checkedBagsPerChild", context);
    checkBagArray(request.cabinBagsPerChild, request.children, "cabinBagsPerChild", context);
  });

const nullableShortString = z.string().max(500).nullable().default(null);
const nullableCode = z.string().max(20).nullable().default(null);
const nullableNonNegativeInteger = z.number().int().nonnegative().nullable().default(null);

const rawPassengersSchema = z
  .object({
    adults: z.number().int().nonnegative().default(0),
    children: z.number().int().nonnegative().default(0),
    infants: z.number().int().nonnegative().default(0),
  })
  .default({ adults: 0, children: 0, infants: 0 });

const rawBaggageSchema = z
  .object({
    personalItem: z.number().int().nonnegative().default(0),
    cabinBag: z.number().int().nonnegative().default(0),
    checkedBag: z.number().int().nonnegative().default(0),
  })
  .default({ personalItem: 0, cabinBag: 0, checkedBag: 0 });

const rawSegmentSchema = z.object({
  from: nullableCode,
  to: nullableCode,
  fromCity: nullableShortString,
  toCity: nullableShortString,
  fromName: nullableShortString,
  toName: nullableShortString,
  fromCountry: nullableShortString,
  toCountry: nullableShortString,
  departureTime: nullableShortString,
  arrivalTime: nullableShortString,
  durationSeconds: nullableNonNegativeInteger,
  carrier: nullableCode,
  carrierName: nullableShortString,
  flightNumber: nullableCode,
  cabinClass: nullableShortString,
});

const rawLegSchema = z.object({
  from: nullableCode,
  to: nullableCode,
  departureTime: nullableShortString,
  arrivalTime: nullableShortString,
  durationSeconds: nullableNonNegativeInteger,
  stops: nullableNonNegativeInteger,
  route: z
    .array(z.string().max(20))
    .max(MAX_SEGMENTS_PER_LEG + 1)
    .default([]),
  cabinClass: nullableShortString,
  segments: z.array(rawSegmentSchema).max(MAX_SEGMENTS_PER_LEG).default([]),
});

const rawItinerarySchema = z.object({
  id: z.string().max(500).nullable().default(null),
  price: z.number().nonnegative().finite().nullable().default(null),
  priceFormatted: z.string().max(100).nullable().default(null),
  totalDurationSeconds: nullableNonNegativeInteger,
  bookingUrl: z.string().max(2_000).nullable().default(null),
  imageId: z.string().max(500).nullable().default(null),
  baggage: rawBaggageSchema.nullable().default(null),
  outbound: rawLegSchema.nullable().default(null),
  inbound: rawLegSchema.nullable().default(null),
});

const kiwiStructuredContentSchema = z.object({
  query: z.string().max(2_000),
  currency: z.string().max(10).nullable().default(null),
  passengers: rawPassengersSchema.nullable().default(null),
  resultsCount: z.number().int().nonnegative().default(0),
  itineraries: z.array(rawItinerarySchema).max(MAX_PROVIDER_ITINERARIES).default([]),
  searchTimeMs: z.number().int().nonnegative().default(0),
  error: z.string().max(2_000).nullable().default(null),
});

const flightSegmentSchema = rawSegmentSchema.strict();
const flightLegSchema = rawLegSchema
  .extend({
    segments: z.array(flightSegmentSchema).max(MAX_SEGMENTS_PER_LEG),
  })
  .strict();
const flightBaggageSchema = rawBaggageSchema;
const flightPassengersSchema = rawPassengersSchema;

const flightItinerarySchema = rawItinerarySchema
  .extend({
    bookingUrl: z.string().url().nullable(),
    baggage: flightBaggageSchema.nullable(),
    outbound: flightLegSchema.nullable(),
    inbound: flightLegSchema.nullable(),
    highlights: z.array(z.enum(["cheapest", "shortest", "earliest"])).max(3),
  })
  .strict();

export const flightSearchResultSchema = z
  .object({
    operation: z.literal("search"),
    query: z.string(),
    currency: z.string().nullable(),
    passengers: flightPassengersSchema.nullable(),
    resultsCount: z.number().int().nonnegative(),
    returnedCount: z.number().int().nonnegative().max(MAX_RETURNED_ITINERARIES),
    itineraries: z.array(flightItinerarySchema).max(MAX_RETURNED_ITINERARIES),
    searchTimeMs: z.number().int().nonnegative(),
    error: z.string().nullable(),
    highlights: z
      .object({
        cheapestItineraryId: z.string().nullable(),
        shortestItineraryId: z.string().nullable(),
        earliestItineraryId: z.string().nullable(),
      })
      .strict(),
    timeBasis: z.literal("provider_local_time_at_each_airport"),
    provider: z
      .object({
        name: z.literal("Kiwi.com"),
        searchOnly: z.literal(true),
        bookingOccursOnProvider: z.literal(true),
        url: z.literal("https://www.kiwi.com/"),
      })
      .strict(),
  })
  .strict();

export type FlorenceFlightSearchRequest = z.input<typeof flightSearchRequestSchema>;
export type FlorenceFlightSearchResult = z.infer<typeof flightSearchResultSchema>;

export interface FlorenceFlightsClient {
  search(request: FlorenceFlightSearchRequest, signal?: AbortSignal): Promise<FlorenceFlightSearchResult>;
}

export type FlightsProviderErrorCode =
  | "invalid_request"
  | "cancelled"
  | "timeout"
  | "unavailable"
  | "provider_error"
  | "invalid_response";

export class FlightsProviderError extends Error {
  readonly code: FlightsProviderErrorCode;
  readonly safeMessage: string;
  readonly retryable: boolean;
  readonly provider = "Kiwi.com";

  constructor(
    code: FlightsProviderErrorCode,
    safeMessage: string,
    options: { retryable?: boolean; cause?: unknown } = {},
  ) {
    super(safeMessage, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "FlightsProviderError";
    this.code = code;
    this.safeMessage = safeMessage;
    this.retryable = options.retryable ?? false;
  }
}

export interface KiwiFlightSearchClientOptions {
  readonly endpoint?: string;
  readonly fetch?: typeof fetch;
  readonly timeoutMs?: number;
  readonly retries?: number;
}

type ParsedFlightRequest = z.output<typeof flightSearchRequestSchema>;
type KiwiStructuredContent = z.output<typeof kiwiStructuredContentSchema>;
type RawItinerary = KiwiStructuredContent["itineraries"][number];
type FlightItinerary = FlorenceFlightSearchResult["itineraries"][number];

const cabinClassToKiwi = {
  economy: "M",
  premium_economy: "W",
  business: "C",
  first: "F",
} as const;

export class KiwiFlightSearchClient implements FlorenceFlightsClient {
  readonly #endpoint: string;
  readonly #fetch: typeof fetch;
  readonly #timeoutMs: number;
  readonly #retries: number;
  #requestNumber = 0;

  constructor(options: KiwiFlightSearchClientOptions = {}) {
    this.#endpoint = validEndpoint(options.endpoint ?? DEFAULT_KIWI_MCP_URL);
    this.#fetch = options.fetch ?? fetch;
    this.#timeoutMs = positiveInteger(options.timeoutMs ?? DEFAULT_TIMEOUT_MS, "timeoutMs");
    this.#retries = nonNegativeInteger(options.retries ?? DEFAULT_RETRIES, "retries");
  }

  async search(
    request: FlorenceFlightSearchRequest,
    signal?: AbortSignal,
  ): Promise<FlorenceFlightSearchResult> {
    let parsed: ParsedFlightRequest;
    try {
      parsed = flightSearchRequestSchema.parse(request);
    } catch (error) {
      throw new FlightsProviderError(
        "invalid_request",
        "The flight search details are incomplete or invalid.",
        {
          cause: error,
        },
      );
    }
    throwIfCancelled(signal);

    const arguments_ = toKiwiArguments(parsed);
    const id = `florence-kiwi-${++this.#requestNumber}`;
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id,
      method: "tools/call",
      params: { name: "search-flight", arguments: arguments_ },
    });

    let lastError: FlightsProviderError | undefined;
    for (let attempt = 0; attempt <= this.#retries; attempt += 1) {
      throwIfCancelled(signal);
      try {
        const envelope = await this.#post(body, id, signal);
        return normalizeStructuredResult(envelope);
      } catch (error) {
        const providerError = asFlightsProviderError(error, signal);
        lastError = providerError;
        if (!providerError.retryable || attempt === this.#retries) throw providerError;
      }
    }
    throw lastError ?? new FlightsProviderError("unavailable", "Flight search is temporarily unavailable.");
  }

  async #post(body: string, id: string, signal?: AbortSignal): Promise<KiwiStructuredContent> {
    const timeoutController = new AbortController();
    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      timeoutController.abort();
    }, this.#timeoutMs);
    const combined = combineSignals(signal, timeoutController.signal);

    try {
      const response = await this.#fetch(this.#endpoint, {
        method: "POST",
        headers: {
          Accept: "application/json, text/event-stream",
          "Content-Type": "application/json",
          "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
        },
        body,
        signal: combined.signal,
      });
      if (!response.ok) {
        const retryable = response.status === 429 || response.status >= 500;
        throw new FlightsProviderError(
          retryable ? "unavailable" : "provider_error",
          retryable
            ? "Flight search is temporarily unavailable."
            : "Kiwi.com could not complete that flight search.",
          { retryable },
        );
      }
      const text = await readBoundedText(response, MAX_RESPONSE_BYTES);
      throwIfCancelled(signal);
      return parseMcpResponse(text, response.headers.get("content-type"), id);
    } catch (error) {
      if (timedOut) {
        throw new FlightsProviderError("timeout", "Flight search took too long. Please try again.", {
          retryable: true,
          cause: error,
        });
      }
      if (signal?.aborted) {
        throw new FlightsProviderError("cancelled", "Flight search was cancelled.", { cause: error });
      }
      throw error;
    } finally {
      clearTimeout(timeout);
      combined.cleanup();
    }
  }
}

function toKiwiArguments(request: ParsedFlightRequest): Record<string, unknown> {
  const arguments_: Record<string, unknown> = {
    flyFrom: request.origin,
    flyTo: request.destination,
    departureDate: toKiwiDate(request.departureDate),
    departureDateFlexDays: request.departureFlexDays,
    adults: request.adults,
    children: request.children,
    infants: request.infants,
    currency: request.currency,
    locale: request.locale,
    one_for_city: request.onePerDestinationCity,
    allow_self_transfer: request.allowSelfTransfer,
    allow_overnight_stopovers: request.allowOvernightStopovers,
    allow_diff_airport_connection: request.allowAirportChanges,
    sort: request.sort,
  };

  add(arguments_, "departureDateTo", optionalKiwiDate(request.departureDateEnd));
  add(arguments_, "returnDate", optionalKiwiDate(request.returnDate));
  add(arguments_, "returnDateFlexDays", request.returnDate ? request.returnFlexDays : undefined);
  add(arguments_, "returnDateTo", optionalKiwiDate(request.returnDateEnd));
  add(arguments_, "cabinClass", request.cabinClass ? cabinClassToKiwi[request.cabinClass] : undefined);
  add(arguments_, "nights_in_dst_from", request.minNightsAtDestination);
  add(arguments_, "nights_in_dst_to", request.maxNightsAtDestination);
  add(arguments_, "max_sector_stopovers", request.maxStops);
  add(arguments_, "price_from", request.minPrice);
  add(arguments_, "price_to", request.maxPrice);
  add(arguments_, "max_fly_duration", request.maxDurationHours);
  add(arguments_, "select_airlines", join(request.preferredAirlines));
  add(arguments_, "exclude_airlines", join(request.excludedAirlines));
  addHourRange(arguments_, request.outboundDepartureHours, "dtime_from", "dtime_to");
  addHourRange(arguments_, request.outboundArrivalHours, "atime_from", "atime_to");
  addHourRange(arguments_, request.returnDepartureHours, "ret_dtime_from", "ret_dtime_to");
  addHourRange(arguments_, request.returnArrivalHours, "ret_atime_from", "ret_atime_to");
  add(arguments_, "stopover_from", request.minLayoverHours);
  add(arguments_, "stopover_to", request.maxLayoverHours);
  add(arguments_, "adults_hold_bags", request.checkedBagsPerAdult);
  add(arguments_, "adults_hand_bags", request.cabinBagsPerAdult);
  add(arguments_, "children_hold_bags", request.checkedBagsPerChild);
  add(arguments_, "children_hand_bags", request.cabinBagsPerChild);
  add(arguments_, "stopover_airports", join(request.requiredStopoverAirports));
  add(arguments_, "exclude_stopover_airports", join(request.excludedStopoverAirports));
  add(arguments_, "stopover_countries", join(request.requiredStopoverCountries));
  add(arguments_, "exclude_stopover_countries", join(request.excludedStopoverCountries));
  add(arguments_, "fly_days", joinNumbers(request.outboundWeekdays));
  add(arguments_, "ret_fly_days", joinNumbers(request.returnWeekdays));
  return arguments_;
}

function normalizeStructuredResult(raw: KiwiStructuredContent): FlorenceFlightSearchResult {
  if (raw.error?.trim()) {
    throw new FlightsProviderError("provider_error", "Kiwi.com could not complete that flight search.");
  }

  const normalized = deduplicateItineraries(raw.itineraries.map(normalizeItinerary));
  const cheapest = minimumBy(normalized, (item) => item.price);
  const shortest = minimumBy(normalized, (item) => item.totalDurationSeconds);
  const earliest = minimumBy(normalized, (item) => localTimestampNumber(item.outbound?.departureTime));
  const highlightByKey = new Map<string, Set<"cheapest" | "shortest" | "earliest">>();
  addHighlight(highlightByKey, cheapest, "cheapest");
  addHighlight(highlightByKey, shortest, "shortest");
  addHighlight(highlightByKey, earliest, "earliest");

  const required = new Set(
    [cheapest, shortest, earliest]
      .filter((item): item is FlightItinerary => item !== undefined)
      .map(itineraryKey),
  );
  const selected = normalized.filter((item) => required.has(itineraryKey(item)));
  for (const item of normalized) {
    if (selected.length >= MAX_RETURNED_ITINERARIES) break;
    if (!required.has(itineraryKey(item))) selected.push(item);
  }
  selected.sort((left, right) => normalized.indexOf(left) - normalized.indexOf(right));
  const itineraries = selected.map((item) => ({
    ...item,
    highlights: [...(highlightByKey.get(itineraryKey(item)) ?? [])],
  }));

  return flightSearchResultSchema.parse({
    operation: "search",
    query: raw.query,
    currency: raw.currency,
    passengers: raw.passengers,
    resultsCount: raw.resultsCount,
    returnedCount: itineraries.length,
    itineraries,
    searchTimeMs: raw.searchTimeMs,
    error: raw.error,
    highlights: {
      cheapestItineraryId: cheapest?.id ?? null,
      shortestItineraryId: shortest?.id ?? null,
      earliestItineraryId: earliest?.id ?? null,
    },
    timeBasis: "provider_local_time_at_each_airport",
    provider: {
      name: "Kiwi.com",
      searchOnly: true,
      bookingOccursOnProvider: true,
      url: "https://www.kiwi.com/",
    },
  });
}

function normalizeItinerary(raw: RawItinerary): Omit<FlightItinerary, "highlights"> {
  return {
    id: raw.id,
    price: raw.price,
    priceFormatted: raw.priceFormatted,
    totalDurationSeconds: raw.totalDurationSeconds,
    bookingUrl: validatedKiwiUrl(raw.bookingUrl),
    imageId: raw.imageId,
    baggage: raw.baggage,
    outbound: raw.outbound,
    inbound: raw.inbound,
  };
}

function deduplicateItineraries(
  values: readonly Omit<FlightItinerary, "highlights">[],
): Omit<FlightItinerary, "highlights">[] {
  const seen = new Set<string>();
  const result: Omit<FlightItinerary, "highlights">[] = [];
  for (const value of values) {
    const key = itineraryKey(value);
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(value);
  }
  return result;
}

function itineraryKey(value: Omit<FlightItinerary, "highlights">): string {
  return (
    value.id ??
    value.bookingUrl ??
    [
      value.outbound?.route.join("-"),
      value.outbound?.departureTime,
      value.inbound?.departureTime,
      value.price,
    ].join("|")
  );
}

function minimumBy<T>(values: readonly T[], score: (value: T) => number | null): T | undefined {
  let best: T | undefined;
  let bestScore = Number.POSITIVE_INFINITY;
  for (const value of values) {
    const candidate = score(value);
    if (candidate !== null && Number.isFinite(candidate) && candidate < bestScore) {
      best = value;
      bestScore = candidate;
    }
  }
  return best;
}

function addHighlight(
  highlights: Map<string, Set<"cheapest" | "shortest" | "earliest">>,
  item: Omit<FlightItinerary, "highlights"> | undefined,
  label: "cheapest" | "shortest" | "earliest",
): void {
  if (!item) return;
  const key = itineraryKey(item);
  const existing = highlights.get(key) ?? new Set();
  existing.add(label);
  highlights.set(key, existing);
}

function localTimestampNumber(value: string | null | undefined): number | null {
  if (!value) return null;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?/.exec(value);
  if (!match) return null;
  const parts = match.slice(1).map(Number);
  if (parts.some((part) => !Number.isFinite(part))) return null;
  const [year, month, day, hour, minute, second = 0] = parts as [
    number,
    number,
    number,
    number,
    number,
    number,
  ];
  return Date.UTC(year, month - 1, day, hour, minute, second);
}

function validatedKiwiUrl(value: string | null): string | null {
  if (!value) return null;
  try {
    const url = new URL(value);
    if (url.protocol !== "https:") return null;
    const hostname = url.hostname.toLowerCase();
    if (hostname !== "kiwi.com" && !hostname.endsWith(".kiwi.com")) return null;
    return url.toString();
  } catch {
    return null;
  }
}

function parseMcpResponse(
  text: string,
  contentType: string | null,
  expectedId: string,
): KiwiStructuredContent {
  let envelope: unknown;
  try {
    envelope =
      contentType?.toLowerCase().includes("text/event-stream") || /^\s*(?:event|data):/m.test(text)
        ? parseSseEnvelope(text, expectedId)
        : JSON.parse(text);
  } catch (error) {
    throw new FlightsProviderError("invalid_response", "Kiwi.com returned an unreadable flight result.", {
      retryable: true,
      cause: error,
    });
  }
  return parseJsonRpcEnvelope(envelope, expectedId);
}

function parseSseEnvelope(text: string, expectedId: string): unknown {
  const payloads: unknown[] = [];
  for (const block of text.replace(/\r\n/g, "\n").split("\n\n")) {
    const data = block
      .split("\n")
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).trimStart())
      .join("\n")
      .trim();
    if (!data || data === "[DONE]") continue;
    payloads.push(JSON.parse(data));
  }
  const matching = payloads.find((payload) => {
    if (!isRecord(payload)) return false;
    return payload.id === expectedId;
  });
  if (matching !== undefined) return matching;
  if (payloads.length === 1) return payloads[0];
  throw new Error("No matching JSON-RPC message in SSE response");
}

function parseJsonRpcEnvelope(envelope: unknown, expectedId: string): KiwiStructuredContent {
  if (!isRecord(envelope) || envelope.jsonrpc !== "2.0" || envelope.id !== expectedId) {
    throw new FlightsProviderError("invalid_response", "Kiwi.com returned an invalid flight result.", {
      retryable: true,
    });
  }
  if (isRecord(envelope.error)) {
    throw new FlightsProviderError("provider_error", "Kiwi.com could not complete that flight search.");
  }
  if (!isRecord(envelope.result)) {
    throw new FlightsProviderError("invalid_response", "Kiwi.com returned an incomplete flight result.", {
      retryable: true,
    });
  }
  if (envelope.result.isError === true) {
    throw new FlightsProviderError("provider_error", "Kiwi.com could not complete that flight search.");
  }
  // Deliberately do not parse result.content text: only MCP structuredContent is trusted.
  const parsed = kiwiStructuredContentSchema.safeParse(envelope.result.structuredContent);
  if (!parsed.success) {
    throw new FlightsProviderError("invalid_response", "Kiwi.com returned an incomplete flight result.", {
      retryable: true,
      cause: parsed.error,
    });
  }
  return parsed.data;
}

async function readBoundedText(response: Response, maximumBytes: number): Promise<string> {
  const length = Number(response.headers.get("content-length"));
  if (Number.isFinite(length) && length > maximumBytes) {
    throw new FlightsProviderError("invalid_response", "Kiwi.com's flight result was too large.");
  }
  const text = await response.text();
  if (Buffer.byteLength(text, "utf8") > maximumBytes) {
    throw new FlightsProviderError("invalid_response", "Kiwi.com's flight result was too large.");
  }
  return text;
}

function asFlightsProviderError(error: unknown, signal?: AbortSignal): FlightsProviderError {
  if (error instanceof FlightsProviderError) return error;
  if (signal?.aborted) {
    return new FlightsProviderError("cancelled", "Flight search was cancelled.", { cause: error });
  }
  return new FlightsProviderError("unavailable", "Flight search is temporarily unavailable.", {
    retryable: true,
    cause: error,
  });
}

function combineSignals(
  external: AbortSignal | undefined,
  timeout: AbortSignal,
): { signal: AbortSignal; cleanup: () => void } {
  const controller = new AbortController();
  const abort = () => controller.abort();
  if (external?.aborted || timeout.aborted) controller.abort();
  external?.addEventListener("abort", abort, { once: true });
  timeout.addEventListener("abort", abort, { once: true });
  return {
    signal: controller.signal,
    cleanup() {
      external?.removeEventListener("abort", abort);
      timeout.removeEventListener("abort", abort);
    },
  };
}

function throwIfCancelled(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw new FlightsProviderError("cancelled", "Flight search was cancelled.");
  }
}

function add(target: Record<string, unknown>, key: string, value: unknown): void {
  if (value !== undefined) target[key] = value;
}

function addHourRange(
  target: Record<string, unknown>,
  range: { from: number; to: number } | undefined,
  fromKey: string,
  toKey: string,
): void {
  if (!range) return;
  target[fromKey] = range.from;
  target[toKey] = range.to;
}

function join(values: readonly string[] | undefined): string | undefined {
  return values?.join(",");
}

function joinNumbers(values: readonly number[] | undefined): string | undefined {
  return values?.join(",");
}

function optionalKiwiDate(value: string | undefined): string | undefined {
  return value ? toKiwiDate(value) : undefined;
}

function toKiwiDate(value: string): string {
  const [year, month, day] = value.split("-");
  return `${day}/${month}/${year}`;
}

function isCalendarDate(value: string): boolean {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) return false;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day;
}

function checkDateRange(
  start: string,
  end: string | undefined,
  path: string,
  context: z.RefinementCtx,
): void {
  if (end && end < start) {
    context.addIssue({ code: "custom", path: [path], message: "End date cannot be before start date" });
  }
}

function checkMinMax(
  minimum: number | undefined,
  maximum: number | undefined,
  path: string,
  context: z.RefinementCtx,
): void {
  if (minimum !== undefined && maximum !== undefined && maximum < minimum) {
    context.addIssue({ code: "custom", path: [path], message: "Maximum cannot be below minimum" });
  }
}

function checkBagArray(
  values: readonly number[] | undefined,
  travelerCount: number,
  path: string,
  context: z.RefinementCtx,
): void {
  if (values && values.length !== travelerCount) {
    context.addIssue({
      code: "custom",
      path: [path],
      message: `Supply one bag count for each of the ${travelerCount} travelers in this group`,
    });
  }
}

function validEndpoint(value: string): string {
  const url = new URL(value);
  if (url.protocol !== "https:") throw new Error("Kiwi endpoint must use HTTPS");
  return url.toString();
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
  return value;
}

function nonNegativeInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value < 0) throw new Error(`${name} must be a non-negative integer`);
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
