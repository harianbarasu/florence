import { z } from "zod";

/**
 * Direct TypeScript adaptation of Hermes Agent's maps skill and maps_client.py
 * at commit 6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882.
 *
 * Florence keeps Hermes's eight operations, OSM category vocabulary, geocoding,
 * Overpass query construction, POI normalization, Haversine math, and result
 * semantics. Intentional provider corrections are local to this client:
 * Valhalla supplies real driving/walking/cycling routes, the current TimeAPI
 * offset is interpreted as total seconds, and timezone lookup never invents an
 * exact zone from longitude.
 */

const DEFAULT_NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search";
const DEFAULT_NOMINATIM_REVERSE_URL = "https://nominatim.openstreetmap.org/reverse";
const DEFAULT_OVERPASS_URLS = [
  "https://overpass-api.de/api/interpreter",
  "https://overpass.private.coffee/api/interpreter",
] as const;
const DEFAULT_VALHALLA_ROUTE_URL = "https://valhalla1.openstreetmap.de/route";
const DEFAULT_TIME_API_URL = "https://timeapi.io/api/timezone/coordinate";
const USER_AGENT = "FlorenceFamilyAssistant/0.1 (+https://github.com/harianbarasu/florence)";
const VALHALLA_CLIENT_ID = "FlorenceFamilyAssistant";

const DEFAULT_TIMEOUT_MS = 15_000;
const OVERPASS_TIMEOUT_MS = 30_000;
const DEFAULT_CACHE_TTL_MS = 15 * 60 * 1_000;
const MAX_CACHE_ENTRIES = 256;
const MAX_RESPONSE_BYTES = 2 * 1_024 * 1_024;
const NOMINATIM_INTERVAL_MS = 1_000;
const MAX_QUERY_CHARS = 300;
const MAX_RESULTS = 30;
const MAX_DIRECTION_STEPS = 120;
const MAX_RADIUS_METRES = 50_000;
const MAX_BBOX_SPAN_DEGREES = 10;

export const FLORENCE_MAP_CATEGORIES = [
  "restaurant",
  "cafe",
  "bar",
  "bakery",
  "convenience_store",
  "hospital",
  "pharmacy",
  "dentist",
  "doctor",
  "veterinary",
  "hotel",
  "guest_house",
  "camp_site",
  "supermarket",
  "bookshop",
  "laundry",
  "atm",
  "bank",
  "gas_station",
  "parking",
  "airport",
  "train_station",
  "bus_stop",
  "taxi",
  "car_wash",
  "car_rental",
  "bicycle_rental",
  "museum",
  "cinema",
  "theatre",
  "nightclub",
  "zoo",
  "school",
  "university",
  "library",
  "police",
  "fire_station",
  "post_office",
  "church",
  "mosque",
  "synagogue",
  "park",
  "gym",
  "swimming_pool",
  "playground",
  "stadium",
] as const;

const mapCategorySchema = z.enum(FLORENCE_MAP_CATEGORIES);
const mapTravelModeSchema = z.enum(["driving", "walking", "cycling"]);

export const mapCoordinatesSchema = z
  .object({
    lat: z.number().finite().min(-90).max(90),
    lon: z.number().finite().min(-180).max(180),
  })
  .strict();

const mapLocationInputSchema = z.union([z.string().trim().min(1).max(MAX_QUERY_CHARS), mapCoordinatesSchema]);

const mapBoundsSchema = z
  .object({
    south: z.number().finite().min(-90).max(90),
    west: z.number().finite().min(-180).max(180),
    north: z.number().finite().min(-90).max(90),
    east: z.number().finite().min(-180).max(180),
  })
  .strict();

export const mapSearchRequestSchema = z
  .object({
    operation: z.literal("search"),
    query: z.string().trim().min(1).max(MAX_QUERY_CHARS),
    limit: z.number().int().min(1).max(5).optional(),
  })
  .strict();

export const mapReverseRequestSchema = z
  .object({ operation: z.literal("reverse"), coordinates: mapCoordinatesSchema })
  .strict();

export const mapNearbyRequestSchema = z
  .object({
    operation: z.literal("nearby"),
    center: mapLocationInputSchema,
    categories: z.array(mapCategorySchema).min(1).max(5),
    radiusM: z.number().int().min(1).max(MAX_RADIUS_METRES).optional(),
    limit: z.number().int().min(1).max(MAX_RESULTS).optional(),
  })
  .strict();

export const mapDistanceRequestSchema = z
  .object({
    operation: z.literal("distance"),
    origin: mapLocationInputSchema,
    destination: mapLocationInputSchema,
    mode: mapTravelModeSchema.optional(),
  })
  .strict();

export const mapDirectionsRequestSchema = z
  .object({
    operation: z.literal("directions"),
    origin: mapLocationInputSchema,
    destination: mapLocationInputSchema,
    mode: mapTravelModeSchema.optional(),
  })
  .strict();

export const mapTimezoneRequestSchema = z
  .object({ operation: z.literal("timezone"), coordinates: mapCoordinatesSchema })
  .strict();

export const mapAreaRequestSchema = z
  .object({
    operation: z.literal("area"),
    query: z.string().trim().min(1).max(MAX_QUERY_CHARS),
  })
  .strict();

export const mapBboxRequestSchema = z
  .object({
    operation: z.literal("bbox"),
    bounds: mapBoundsSchema,
    category: mapCategorySchema,
    limit: z.number().int().min(1).max(MAX_RESULTS).optional(),
  })
  .strict();

export const florenceMapsRequestSchema = z.discriminatedUnion("operation", [
  mapSearchRequestSchema,
  mapReverseRequestSchema,
  mapNearbyRequestSchema,
  mapDistanceRequestSchema,
  mapDirectionsRequestSchema,
  mapTimezoneRequestSchema,
  mapAreaRequestSchema,
  mapBboxRequestSchema,
]);

const mapBoundingBoxResultSchema = z
  .object({
    south: z.number(),
    west: z.number(),
    north: z.number(),
    east: z.number(),
  })
  .strict();

const mapAttributionSchema = z
  .object({
    provider: z.string().min(1),
    label: z.string().min(1),
    url: z.string().url(),
  })
  .strict();

const osmAttribution = Object.freeze({
  provider: "OpenStreetMap",
  label: "© OpenStreetMap contributors",
  url: "https://www.openstreetmap.org/copyright",
});
const valhallaAttribution = Object.freeze({
  provider: "Valhalla",
  label: "Routing by Valhalla's FOSSGIS public service",
  url: "https://valhalla.openstreetmap.de/",
});
const timeApiAttribution = Object.freeze({
  provider: "TimeAPI.io",
  label: "Timezone data from TimeAPI.io",
  url: "https://timeapi.io/",
});

const mapSearchPlaceSchema = z
  .object({
    name: z.string(),
    displayName: z.string(),
    lat: z.number(),
    lon: z.number(),
    type: z.string(),
    category: z.string(),
    osmType: z.string(),
    osmId: z.string(),
    boundingBox: mapBoundingBoxResultSchema.nullable(),
    importance: z.number().nullable(),
    mapsUrl: z.string().url(),
  })
  .strict();

export const mapSearchResultSchema = z
  .object({
    operation: z.literal("search"),
    query: z.string(),
    count: z.number().int().nonnegative(),
    results: z.array(mapSearchPlaceSchema).max(5),
    attribution: z.array(mapAttributionSchema).min(1).max(2),
  })
  .strict();

const mapAddressSchema = z
  .object({
    houseNumber: z.string(),
    road: z.string(),
    neighbourhood: z.string(),
    suburb: z.string(),
    city: z.string(),
    county: z.string(),
    state: z.string(),
    postcode: z.string(),
    country: z.string(),
    countryCode: z.string(),
  })
  .strict();

export const mapReverseResultSchema = z
  .object({
    operation: z.literal("reverse"),
    coordinates: mapCoordinatesSchema,
    displayName: z.string(),
    address: mapAddressSchema,
    osmType: z.string(),
    osmId: z.string(),
    mapsUrl: z.string().url(),
    attribution: z.array(mapAttributionSchema).min(1).max(2),
  })
  .strict();

const resolvedMapLocationSchema = z
  .object({
    query: z.string().nullable(),
    displayName: z.string(),
    lat: z.number(),
    lon: z.number(),
  })
  .strict();

const nearbyPlaceSchema = z
  .object({
    name: z.string().nullable(),
    address: z.string(),
    lat: z.number(),
    lon: z.number(),
    osmType: z.string(),
    osmId: z.string(),
    category: mapCategorySchema,
    distanceM: z.number().nonnegative(),
    mapsUrl: z.string().url(),
    directionsUrl: z.string().url(),
    cuisine: z.string().optional(),
    hours: z.string().optional(),
    phone: z.string().optional(),
    website: z.string().optional(),
    tags: z.record(z.string(), z.string()),
  })
  .strict();

export const mapNearbyResultSchema = z
  .object({
    operation: z.literal("nearby"),
    center: resolvedMapLocationSchema,
    categories: z.array(mapCategorySchema).min(1).max(5),
    radiusM: z.number().int().positive(),
    count: z.number().int().nonnegative(),
    results: z.array(nearbyPlaceSchema).max(MAX_RESULTS),
    attribution: z.array(mapAttributionSchema).min(1).max(2),
  })
  .strict();

const routeBaseShape = {
  origin: resolvedMapLocationSchema,
  destination: resolvedMapLocationSchema,
  mode: mapTravelModeSchema,
  distanceM: z.number().nonnegative(),
  durationSeconds: z.number().nonnegative(),
  attribution: z.array(mapAttributionSchema).min(2).max(3),
} as const;

export const mapDistanceResultSchema = z
  .object({
    operation: z.literal("distance"),
    ...routeBaseShape,
    straightLineM: z.number().nonnegative(),
  })
  .strict();

const mapDirectionStepSchema = z
  .object({
    step: z.number().int().positive(),
    instruction: z.string().min(1),
    distanceM: z.number().nonnegative(),
    durationSeconds: z.number().nonnegative(),
    streetNames: z.array(z.string()).max(5),
    maneuverType: z.number().int().nullable(),
  })
  .strict();

export const mapDirectionsResultSchema = z
  .object({
    operation: z.literal("directions"),
    ...routeBaseShape,
    steps: z.array(mapDirectionStepSchema).max(MAX_DIRECTION_STEPS),
    stepCount: z.number().int().nonnegative(),
    stepsTruncated: z.boolean(),
  })
  .strict();

export const mapTimezoneResultSchema = z
  .object({
    operation: z.literal("timezone"),
    coordinates: mapCoordinatesSchema,
    timezone: z.string().min(1),
    utcOffset: z.string().regex(/^[+-]\d{2}:\d{2}(?::\d{2})?$/),
    currentLocalTime: z.string().nullable(),
    exact: z.literal(true),
    attribution: z.array(mapAttributionSchema).min(1).max(2),
  })
  .strict();

export const mapAreaResultSchema = z
  .object({
    operation: z.literal("area"),
    query: z.string(),
    displayName: z.string(),
    lat: z.number(),
    lon: z.number(),
    type: z.string(),
    category: z.string(),
    boundingBox: mapBoundingBoxResultSchema,
    dimensions: z.object({ widthKm: z.number().nonnegative(), heightKm: z.number().nonnegative() }).strict(),
    approximateAreaKm2: z.number().nonnegative(),
    osmType: z.string(),
    osmId: z.string(),
    attribution: z.array(mapAttributionSchema).min(1).max(2),
  })
  .strict();

export const mapBboxResultSchema = z
  .object({
    operation: z.literal("bbox"),
    bounds: mapBoundingBoxResultSchema,
    category: mapCategorySchema,
    count: z.number().int().nonnegative(),
    results: z.array(nearbyPlaceSchema).max(MAX_RESULTS),
    attribution: z.array(mapAttributionSchema).min(1).max(2),
  })
  .strict();

export const florenceMapsResultSchema = z.discriminatedUnion("operation", [
  mapSearchResultSchema,
  mapReverseResultSchema,
  mapNearbyResultSchema,
  mapDistanceResultSchema,
  mapDirectionsResultSchema,
  mapTimezoneResultSchema,
  mapAreaResultSchema,
  mapBboxResultSchema,
]);

export type FlorenceMapsRequest = z.input<typeof florenceMapsRequestSchema>;
export type FlorenceMapsResult = z.infer<typeof florenceMapsResultSchema>;
export type MapCoordinates = z.infer<typeof mapCoordinatesSchema>;
export type MapTravelMode = z.infer<typeof mapTravelModeSchema>;
export type MapCategory = z.infer<typeof mapCategorySchema>;

export type MapsProviderErrorCode =
  | "invalid_input"
  | "not_found"
  | "rate_limited"
  | "timeout"
  | "cancelled"
  | "unavailable"
  | "invalid_response";

export class MapsProviderError extends Error {
  readonly code: MapsProviderErrorCode;
  readonly safeMessage: string;
  readonly retryable: boolean;
  readonly provider: string | undefined;
  readonly status: number | undefined;

  constructor(
    code: MapsProviderErrorCode,
    safeMessage: string,
    options: {
      readonly retryable?: boolean;
      readonly provider?: string;
      readonly status?: number;
      readonly cause?: unknown;
    } = {},
  ) {
    super(safeMessage, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "MapsProviderError";
    this.code = code;
    this.safeMessage = safeMessage.slice(0, 300);
    this.retryable = options.retryable ?? false;
    this.provider = options.provider;
    this.status = options.status;
  }
}

export interface FlorenceMapsClient {
  run(request: FlorenceMapsRequest, signal?: AbortSignal): Promise<FlorenceMapsResult>;
}

export interface OpenStreetMapsClientOptions {
  readonly fetch?: typeof globalThis.fetch;
  readonly now?: () => number;
  readonly sleep?: (milliseconds: number, signal: AbortSignal) => Promise<void>;
  readonly timeoutMs?: number;
  readonly cacheTtlMs?: number;
  readonly nominatimSearchUrl?: string;
  readonly nominatimReverseUrl?: string;
  readonly overpassUrls?: readonly string[];
  readonly valhallaRouteUrl?: string;
  readonly timeApiUrl?: string;
}

type NormalizedMapsRequest = z.infer<typeof florenceMapsRequestSchema>;
type SearchResult = z.infer<typeof mapSearchResultSchema>;
type ReverseResult = z.infer<typeof mapReverseResultSchema>;
type NearbyResult = z.infer<typeof mapNearbyResultSchema>;
type DistanceResult = z.infer<typeof mapDistanceResultSchema>;
type DirectionsResult = z.infer<typeof mapDirectionsResultSchema>;
type TimezoneResult = z.infer<typeof mapTimezoneResultSchema>;
type AreaResult = z.infer<typeof mapAreaResultSchema>;
type BboxResult = z.infer<typeof mapBboxResultSchema>;
type ResolvedMapLocation = z.infer<typeof resolvedMapLocationSchema>;
type NearbyPlace = z.infer<typeof nearbyPlaceSchema>;

type CacheEntry = { readonly expiresAt: number; readonly value: unknown };

const categoryTags: Readonly<Record<MapCategory, readonly (readonly [key: string, value: string])[]>> = {
  restaurant: [["amenity", "restaurant"]],
  cafe: [["amenity", "cafe"]],
  bar: [["amenity", "bar"]],
  bakery: [
    ["shop", "bakery"],
    ["amenity", "bakery"],
  ],
  convenience_store: [["shop", "convenience"]],
  hospital: [["amenity", "hospital"]],
  pharmacy: [["amenity", "pharmacy"]],
  dentist: [["amenity", "dentist"]],
  doctor: [["amenity", "doctors"]],
  veterinary: [["amenity", "veterinary"]],
  hotel: [["tourism", "hotel"]],
  guest_house: [["tourism", "guest_house"]],
  camp_site: [["tourism", "camp_site"]],
  supermarket: [["shop", "supermarket"]],
  bookshop: [["shop", "books"]],
  laundry: [["shop", "laundry"]],
  atm: [["amenity", "atm"]],
  bank: [["amenity", "bank"]],
  gas_station: [["amenity", "fuel"]],
  parking: [["amenity", "parking"]],
  airport: [["aeroway", "aerodrome"]],
  train_station: [["railway", "station"]],
  bus_stop: [["highway", "bus_stop"]],
  taxi: [["amenity", "taxi"]],
  car_wash: [["amenity", "car_wash"]],
  car_rental: [["amenity", "car_rental"]],
  bicycle_rental: [["amenity", "bicycle_rental"]],
  museum: [["tourism", "museum"]],
  cinema: [["amenity", "cinema"]],
  theatre: [["amenity", "theatre"]],
  nightclub: [["amenity", "nightclub"]],
  zoo: [["tourism", "zoo"]],
  school: [["amenity", "school"]],
  university: [["amenity", "university"]],
  library: [["amenity", "library"]],
  police: [["amenity", "police"]],
  fire_station: [["amenity", "fire_station"]],
  post_office: [["amenity", "post_office"]],
  church: [["amenity", "place_of_worship"]],
  mosque: [["amenity", "place_of_worship"]],
  synagogue: [["amenity", "place_of_worship"]],
  park: [["leisure", "park"]],
  gym: [["leisure", "fitness_centre"]],
  swimming_pool: [["leisure", "swimming_pool"]],
  playground: [["leisure", "playground"]],
  stadium: [["leisure", "stadium"]],
};

const religionFilters: Readonly<Partial<Record<MapCategory, string>>> = {
  church: "christian",
  mosque: "muslim",
  synagogue: "jewish",
};

const valhallaCosting: Readonly<Record<MapTravelMode, "auto" | "pedestrian" | "bicycle">> = {
  driving: "auto",
  walking: "pedestrian",
  cycling: "bicycle",
};

const nominatimPlaceSchema = z
  .object({
    lat: z.union([z.string(), z.number()]),
    lon: z.union([z.string(), z.number()]),
    display_name: z.string().optional(),
    name: z.string().optional(),
    type: z.string().optional(),
    category: z.string().optional(),
    osm_type: z.string().optional(),
    osm_id: z.union([z.string(), z.number()]).optional(),
    boundingbox: z.array(z.union([z.string(), z.number()])).optional(),
    importance: z.number().optional(),
    address: z.record(z.string(), z.string()).optional(),
    error: z.string().optional(),
  })
  .passthrough();

const nominatimSearchResponseSchema = z.array(nominatimPlaceSchema).max(100);

const overpassElementSchema = z
  .object({
    type: z.enum(["node", "way", "relation"]),
    id: z.union([z.string(), z.number()]),
    lat: z.number().optional(),
    lon: z.number().optional(),
    center: z.object({ lat: z.number(), lon: z.number() }).passthrough().optional(),
    tags: z.record(z.string(), z.string()).optional(),
  })
  .passthrough();

const overpassResponseSchema = z
  .object({ elements: z.array(overpassElementSchema).max(10_000).optional() })
  .passthrough();

const valhallaManeuverSchema = z
  .object({
    instruction: z.string().optional(),
    verbal_pre_transition_instruction: z.string().optional(),
    length: z.number().optional(),
    time: z.number().optional(),
    type: z.number().int().optional(),
    street_names: z.array(z.string()).optional(),
  })
  .passthrough();

const valhallaResponseSchema = z
  .object({
    trip: z
      .object({
        status: z.number().optional(),
        status_message: z.string().optional(),
        summary: z
          .object({ length: z.number().optional(), time: z.number().optional() })
          .passthrough()
          .optional(),
        legs: z
          .array(z.object({ maneuvers: z.array(valhallaManeuverSchema).optional() }).passthrough())
          .optional(),
      })
      .passthrough()
      .optional(),
    error: z.string().optional(),
    error_code: z.number().optional(),
  })
  .passthrough();

const timeApiResponseSchema = z
  .object({
    timeZone: z.string().optional(),
    currentLocalTime: z.string().optional(),
    currentUtcOffset: z
      .object({
        /** TimeAPI currently returns the whole offset here, not a seconds component. */
        seconds: z.number().optional(),
        totalSeconds: z.number().optional(),
      })
      .passthrough()
      .optional(),
  })
  .passthrough();

type NominatimPlace = z.infer<typeof nominatimPlaceSchema>;
type OverpassElement = z.infer<typeof overpassElementSchema>;
type ValhallaResponse = z.infer<typeof valhallaResponseSchema>;

export class OpenStreetMapsClient implements FlorenceMapsClient {
  readonly #fetch: typeof globalThis.fetch;
  readonly #now: () => number;
  readonly #sleep: (milliseconds: number, signal: AbortSignal) => Promise<void>;
  readonly #timeoutMs: number;
  readonly #cacheTtlMs: number;
  readonly #nominatimSearchUrl: string;
  readonly #nominatimReverseUrl: string;
  readonly #overpassUrls: readonly string[];
  readonly #valhallaRouteUrl: string;
  readonly #timeApiUrl: string;
  readonly #cache = new Map<string, CacheEntry>();
  #nominatimTail: Promise<void> = Promise.resolve();
  #nextNominatimStart = 0;

  constructor(options: OpenStreetMapsClientOptions = {}) {
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#now = options.now ?? Date.now;
    this.#sleep = options.sleep ?? abortableSleep;
    this.#timeoutMs = clampInteger(options.timeoutMs ?? DEFAULT_TIMEOUT_MS, 1_000, 60_000);
    this.#cacheTtlMs = clampInteger(options.cacheTtlMs ?? DEFAULT_CACHE_TTL_MS, 1_000, 86_400_000);
    this.#nominatimSearchUrl = validEndpoint(options.nominatimSearchUrl ?? DEFAULT_NOMINATIM_SEARCH_URL);
    this.#nominatimReverseUrl = validEndpoint(options.nominatimReverseUrl ?? DEFAULT_NOMINATIM_REVERSE_URL);
    this.#overpassUrls = (options.overpassUrls ?? DEFAULT_OVERPASS_URLS).map(validEndpoint);
    if (this.#overpassUrls.length === 0 || this.#overpassUrls.length > 4) {
      throw new TypeError("OpenStreetMapsClient requires between one and four Overpass endpoints.");
    }
    this.#valhallaRouteUrl = validEndpoint(options.valhallaRouteUrl ?? DEFAULT_VALHALLA_ROUTE_URL);
    this.#timeApiUrl = validEndpoint(options.timeApiUrl ?? DEFAULT_TIME_API_URL);
  }

  async run(request: FlorenceMapsRequest, signal?: AbortSignal): Promise<FlorenceMapsResult> {
    const parsed = florenceMapsRequestSchema.safeParse(request);
    if (!parsed.success) {
      throw new MapsProviderError("invalid_input", "The map request was invalid.");
    }

    const localSignal = signal ?? new AbortController().signal;
    throwIfAborted(localSignal);

    let result: FlorenceMapsResult;
    switch (parsed.data.operation) {
      case "search":
        result = await this.#search(parsed.data, localSignal);
        break;
      case "reverse":
        result = await this.#reverse(parsed.data, localSignal);
        break;
      case "nearby":
        result = await this.#nearby(parsed.data, localSignal);
        break;
      case "distance":
        result = await this.#distance(parsed.data, localSignal);
        break;
      case "directions":
        result = await this.#directions(parsed.data, localSignal);
        break;
      case "timezone":
        result = await this.#timezone(parsed.data, localSignal);
        break;
      case "area":
        result = await this.#area(parsed.data, localSignal);
        break;
      case "bbox":
        result = await this.#bbox(parsed.data, localSignal);
        break;
    }

    const validated = florenceMapsResultSchema.safeParse(result);
    if (!validated.success) {
      throw new MapsProviderError(
        "invalid_response",
        "The maps provider returned a result Florence could not use.",
        { provider: "maps", cause: validated.error },
      );
    }
    return validated.data;
  }

  async #search(
    request: Extract<NormalizedMapsRequest, { operation: "search" }>,
    signal: AbortSignal,
  ): Promise<SearchResult> {
    const limit = request.limit ?? 5;
    const raw = await this.#nominatimSearch(request.query, limit, signal);
    const results = raw.slice(0, limit).map((item) => normalizeSearchPlace(item));
    return {
      operation: "search",
      query: request.query,
      count: results.length,
      results,
      attribution: [osmAttribution],
    };
  }

  async #reverse(
    request: Extract<NormalizedMapsRequest, { operation: "reverse" }>,
    signal: AbortSignal,
  ): Promise<ReverseResult> {
    const { lat, lon } = request.coordinates;
    const url = withQuery(this.#nominatimReverseUrl, {
      lat,
      lon,
      format: "jsonv2",
      addressdetails: 1,
    });
    const raw = await this.#nominatimJson(url, nominatimPlaceSchema, signal);
    if (raw.error) {
      throw new MapsProviderError("not_found", "No address was found for those coordinates.", {
        provider: "Nominatim",
      });
    }
    const address = raw.address ?? {};
    return {
      operation: "reverse",
      coordinates: { lat, lon },
      displayName: raw.display_name ?? "",
      address: {
        houseNumber: address.house_number ?? "",
        road: address.road ?? "",
        neighbourhood: address.neighbourhood ?? "",
        suburb: address.suburb ?? "",
        city: address.city ?? address.town ?? address.village ?? "",
        county: address.county ?? "",
        state: address.state ?? "",
        postcode: address.postcode ?? "",
        country: address.country ?? "",
        countryCode: address.country_code ?? "",
      },
      osmType: raw.osm_type ?? "",
      osmId: raw.osm_id === undefined ? "" : String(raw.osm_id),
      mapsUrl: googleMapsUrl(lat, lon),
      attribution: [osmAttribution],
    };
  }

  async #nearby(
    request: Extract<NormalizedMapsRequest, { operation: "nearby" }>,
    signal: AbortSignal,
  ): Promise<NearbyResult> {
    const center = await this.#resolveLocation(request.center, signal);
    const categories = [...new Set(request.categories)];
    const radiusM = request.radiusM ?? 500;
    const limit = request.limit ?? 10;
    const merged = new Map<string, NearbyPlace>();

    for (const category of categories) {
      const query = buildNearbyQuery(category, center, radiusM, limit);
      const raw = await this.#overpass(query, signal);
      for (const place of parseOverpassElements(raw.elements ?? [], center, category)) {
        const key = `${place.osmType}:${place.osmId}`;
        if (!merged.has(key)) merged.set(key, place);
      }
    }

    const results = [...merged.values()]
      .sort((left, right) => left.distanceM - right.distanceM)
      .slice(0, limit);
    return {
      operation: "nearby",
      center,
      categories,
      radiusM,
      count: results.length,
      results,
      attribution: [osmAttribution],
    };
  }

  async #distance(
    request: Extract<NormalizedMapsRequest, { operation: "distance" }>,
    signal: AbortSignal,
  ): Promise<DistanceResult> {
    const mode = request.mode ?? "driving";
    const origin = await this.#resolveLocation(request.origin, signal);
    const destination = await this.#resolveLocation(request.destination, signal);
    const route = await this.#route(origin, destination, mode, signal);
    return {
      operation: "distance",
      origin,
      destination,
      mode,
      distanceM: route.distanceM,
      durationSeconds: route.durationSeconds,
      straightLineM: round(haversineMetres(origin.lat, origin.lon, destination.lat, destination.lon), 1),
      attribution: [osmAttribution, valhallaAttribution],
    };
  }

  async #directions(
    request: Extract<NormalizedMapsRequest, { operation: "directions" }>,
    signal: AbortSignal,
  ): Promise<DirectionsResult> {
    const mode = request.mode ?? "driving";
    const origin = await this.#resolveLocation(request.origin, signal);
    const destination = await this.#resolveLocation(request.destination, signal);
    const route = await this.#route(origin, destination, mode, signal);
    const allSteps = (route.raw.trip?.legs ?? []).flatMap((leg) => leg.maneuvers ?? []);
    const steps = allSteps.slice(0, MAX_DIRECTION_STEPS).map((maneuver, index) => ({
      step: index + 1,
      instruction:
        maneuver.instruction?.trim() ||
        maneuver.verbal_pre_transition_instruction?.trim() ||
        "Continue on the route.",
      distanceM: round(nonNegative(maneuver.length, 0) * 1_000, 1),
      durationSeconds: round(nonNegative(maneuver.time, 0), 1),
      streetNames: (maneuver.street_names ?? []).slice(0, 5),
      maneuverType: maneuver.type ?? null,
    }));
    return {
      operation: "directions",
      origin,
      destination,
      mode,
      distanceM: route.distanceM,
      durationSeconds: route.durationSeconds,
      steps,
      stepCount: allSteps.length,
      stepsTruncated: allSteps.length > steps.length,
      attribution: [osmAttribution, valhallaAttribution],
    };
  }

  async #timezone(
    request: Extract<NormalizedMapsRequest, { operation: "timezone" }>,
    signal: AbortSignal,
  ): Promise<TimezoneResult> {
    const { lat, lon } = request.coordinates;
    const raw = await this.#requestJson(
      withQuery(this.#timeApiUrl, { latitude: lat, longitude: lon }),
      { method: "GET" },
      timeApiResponseSchema,
      signal,
      { provider: "TimeAPI.io", attempts: 2 },
    );
    const totalSeconds = raw.currentUtcOffset?.totalSeconds ?? raw.currentUtcOffset?.seconds;
    if (!raw.timeZone?.trim() || !Number.isFinite(totalSeconds)) {
      throw new MapsProviderError(
        "invalid_response",
        "Exact timezone information is temporarily unavailable for that location.",
        { provider: "TimeAPI.io", retryable: true },
      );
    }
    return {
      operation: "timezone",
      coordinates: { lat, lon },
      timezone: raw.timeZone,
      utcOffset: formatUtcOffset(totalSeconds ?? 0),
      currentLocalTime: raw.currentLocalTime ?? null,
      exact: true,
      attribution: [timeApiAttribution],
    };
  }

  async #area(
    request: Extract<NormalizedMapsRequest, { operation: "area" }>,
    signal: AbortSignal,
  ): Promise<AreaResult> {
    const [item] = await this.#nominatimSearch(request.query, 1, signal);
    if (!item) {
      throw new MapsProviderError("not_found", `No place was found for “${request.query}”.`, {
        provider: "Nominatim",
      });
    }
    const bounds = parseNominatimBounds(item.boundingbox);
    if (!bounds) {
      throw new MapsProviderError("invalid_response", "That place has no usable boundary.", {
        provider: "Nominatim",
      });
    }
    const lat = numeric(item.lat, "Nominatim latitude");
    const lon = numeric(item.lon, "Nominatim longitude");
    const averageLatitude = (bounds.south + bounds.north) / 2;
    const heightKm = haversineMetres(bounds.south, bounds.west, bounds.north, bounds.west) / 1_000;
    const widthKm = haversineMetres(averageLatitude, bounds.west, averageLatitude, bounds.east) / 1_000;
    return {
      operation: "area",
      query: request.query,
      displayName: item.display_name ?? "",
      lat,
      lon,
      type: item.type ?? "",
      category: item.category ?? "",
      boundingBox: bounds,
      dimensions: { widthKm: round(widthKm, 3), heightKm: round(heightKm, 3) },
      approximateAreaKm2: round(widthKm * heightKm, 3),
      osmType: item.osm_type ?? "",
      osmId: item.osm_id === undefined ? "" : String(item.osm_id),
      attribution: [osmAttribution],
    };
  }

  async #bbox(
    request: Extract<NormalizedMapsRequest, { operation: "bbox" }>,
    signal: AbortSignal,
  ): Promise<BboxResult> {
    const bounds = normalizeBounds(request.bounds);
    if (
      bounds.north - bounds.south > MAX_BBOX_SPAN_DEGREES ||
      bounds.east - bounds.west > MAX_BBOX_SPAN_DEGREES
    ) {
      throw new MapsProviderError("invalid_input", "That map area is too large for a places search.");
    }
    const limit = request.limit ?? 20;
    const center: ResolvedMapLocation = {
      query: null,
      displayName: "Bounding-box center",
      lat: (bounds.south + bounds.north) / 2,
      lon: (bounds.west + bounds.east) / 2,
    };
    const raw = await this.#overpass(buildBboxQuery(request.category, bounds, limit), signal);
    const results = parseOverpassElements(raw.elements ?? [], center, request.category)
      .sort((left, right) => left.distanceM - right.distanceM)
      .slice(0, limit);
    return {
      operation: "bbox",
      bounds,
      category: request.category,
      count: results.length,
      results,
      attribution: [osmAttribution],
    };
  }

  async #nominatimSearch(
    query: string,
    limit: number,
    signal: AbortSignal,
  ): Promise<readonly NominatimPlace[]> {
    const url = withQuery(this.#nominatimSearchUrl, {
      q: query,
      format: "jsonv2",
      limit,
      addressdetails: 1,
    });
    return this.#nominatimJson(url, nominatimSearchResponseSchema, signal);
  }

  async #nominatimJson<T>(url: string, schema: z.ZodType<T>, signal: AbortSignal): Promise<T> {
    const cacheKey = `GET ${url}`;
    const cached = this.#cacheGet<T>(cacheKey);
    if (cached !== undefined) return cached;

    const task = this.#nominatimTail.then(async () => {
      throwIfAborted(signal);
      const queuedCacheHit = this.#cacheGet<T>(cacheKey);
      if (queuedCacheHit !== undefined) return queuedCacheHit;

      let lastError: MapsProviderError | undefined;
      for (let attempt = 1; attempt <= 2; attempt += 1) {
        const waitMs = Math.max(0, this.#nextNominatimStart - this.#now());
        if (waitMs > 0) await this.#sleep(waitMs, signal);
        throwIfAborted(signal);
        this.#nextNominatimStart = this.#now() + NOMINATIM_INTERVAL_MS;
        try {
          const value = await this.#requestJson(url, { method: "GET" }, schema, signal, {
            provider: "Nominatim",
            attempts: 1,
          });
          this.#cacheSet(cacheKey, value);
          return value;
        } catch (error) {
          const providerError = asMapsProviderError(error, "Nominatim");
          lastError = providerError;
          if (!providerError.retryable || attempt === 2) throw providerError;
        }
      }
      throw lastError ?? new MapsProviderError("unavailable", "Nominatim is unavailable.");
    });

    this.#nominatimTail = task.then(
      () => undefined,
      () => undefined,
    );
    return task;
  }

  async #resolveLocation(input: string | MapCoordinates, signal: AbortSignal): Promise<ResolvedMapLocation> {
    if (typeof input !== "string") {
      return {
        query: null,
        displayName: `${input.lat}, ${input.lon}`,
        lat: input.lat,
        lon: input.lon,
      };
    }

    const candidates = await this.#nominatimSearch(input, 5, signal);
    const [item] = candidates;
    if (!item) {
      throw new MapsProviderError("not_found", `No place was found for “${input}”.`, {
        provider: "Nominatim",
      });
    }
    const ambiguous = materiallyAmbiguousCandidates(candidates);
    if (ambiguous.length > 1) {
      const choices = ambiguous
        .slice(0, 3)
        .map((candidate) => candidate.display_name ?? `${candidate.lat}, ${candidate.lon}`)
        .join("; ");
      throw new MapsProviderError(
        "invalid_input",
        `Several places match “${input}”: ${choices}. Use place search, choose the intended result, then retry with its coordinates.`,
        { provider: "Nominatim" },
      );
    }
    return {
      query: input,
      displayName: item.display_name ?? input,
      lat: numeric(item.lat, "Nominatim latitude"),
      lon: numeric(item.lon, "Nominatim longitude"),
    };
  }

  async #overpass(query: string, signal: AbortSignal) {
    let lastError: MapsProviderError | undefined;
    for (const endpoint of this.#overpassUrls) {
      try {
        return await this.#requestJson(
          endpoint,
          {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
            body: `data=${encodeURIComponent(query)}`,
          },
          overpassResponseSchema,
          signal,
          { provider: "Overpass", attempts: 1, timeoutMs: OVERPASS_TIMEOUT_MS },
        );
      } catch (error) {
        const providerError = asMapsProviderError(error, "Overpass");
        if (providerError.code === "cancelled") throw providerError;
        lastError = providerError;
      }
    }
    throw new MapsProviderError("unavailable", "Places search is temporarily unavailable.", {
      provider: "Overpass",
      retryable: true,
      cause: lastError,
    });
  }

  async #route(
    origin: ResolvedMapLocation,
    destination: ResolvedMapLocation,
    mode: MapTravelMode,
    signal: AbortSignal,
  ): Promise<{
    readonly raw: ValhallaResponse;
    readonly distanceM: number;
    readonly durationSeconds: number;
  }> {
    const body = JSON.stringify({
      locations: [
        { lat: origin.lat, lon: origin.lon, type: "break" },
        { lat: destination.lat, lon: destination.lon, type: "break" },
      ],
      costing: valhallaCosting[mode],
      units: "kilometers",
      language: "en-US",
      directions_type: "instructions",
    });
    const raw = await this.#requestJson(
      this.#valhallaRouteUrl,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Client-Id": VALHALLA_CLIENT_ID,
        },
        body,
      },
      valhallaResponseSchema,
      signal,
      { provider: "Valhalla", attempts: 2 },
    );
    if (!raw.trip || (raw.trip.status !== undefined && raw.trip.status !== 0)) {
      throw new MapsProviderError("not_found", "No usable route was found between those places.", {
        provider: "Valhalla",
      });
    }
    const distanceKm = raw.trip.summary?.length;
    const durationSeconds = raw.trip.summary?.time;
    if (!Number.isFinite(distanceKm) || !Number.isFinite(durationSeconds)) {
      throw new MapsProviderError("invalid_response", "The route result was incomplete.", {
        provider: "Valhalla",
        retryable: true,
      });
    }
    return {
      raw,
      distanceM: round(nonNegative(distanceKm, 0) * 1_000, 1),
      durationSeconds: round(nonNegative(durationSeconds, 0), 1),
    };
  }

  async #requestJson<T>(
    url: string,
    init: RequestInit,
    schema: z.ZodType<T>,
    signal: AbortSignal,
    options: {
      readonly provider: string;
      readonly attempts: number;
      readonly timeoutMs?: number;
    },
  ): Promise<T> {
    let lastError: MapsProviderError | undefined;
    for (let attempt = 1; attempt <= options.attempts; attempt += 1) {
      throwIfAborted(signal);
      try {
        const raw = await this.#fetchJsonOnce(
          url,
          init,
          signal,
          options.provider,
          options.timeoutMs ?? this.#timeoutMs,
        );
        const parsed = schema.safeParse(raw);
        if (!parsed.success) {
          throw new MapsProviderError(
            "invalid_response",
            `${options.provider} returned a result Florence could not use.`,
            { provider: options.provider, cause: parsed.error },
          );
        }
        return parsed.data;
      } catch (error) {
        const providerError = asMapsProviderError(error, options.provider);
        lastError = providerError;
        if (!providerError.retryable || attempt === options.attempts) throw providerError;
        await this.#sleep(Math.min(250 * attempt, 1_000), signal);
      }
    }
    throw lastError ?? new MapsProviderError("unavailable", `${options.provider} is unavailable.`);
  }

  async #fetchJsonOnce(
    url: string,
    init: RequestInit,
    signal: AbortSignal,
    provider: string,
    timeoutMs: number,
  ): Promise<unknown> {
    const controller = new AbortController();
    let timedOut = false;
    const abortFromCaller = () => controller.abort(signal.reason);
    signal.addEventListener("abort", abortFromCaller, { once: true });
    const timer = setTimeout(() => {
      timedOut = true;
      controller.abort(new Error("provider timeout"));
    }, timeoutMs);
    const headers = new Headers(init.headers);
    headers.set("Accept", "application/json");
    headers.set("User-Agent", USER_AGENT);

    try {
      const response = await this.#fetch(url, { ...init, headers, signal: controller.signal });
      if (!response.ok) {
        throw httpProviderError(provider, response.status);
      }
      const text = await response.text();
      if (Buffer.byteLength(text, "utf8") > MAX_RESPONSE_BYTES) {
        throw new MapsProviderError(
          "invalid_response",
          `${provider} returned more data than Florence can safely use.`,
          { provider },
        );
      }
      try {
        return JSON.parse(text) as unknown;
      } catch (error) {
        throw new MapsProviderError("invalid_response", `${provider} returned invalid JSON.`, {
          provider,
          cause: error,
        });
      }
    } catch (error) {
      if (error instanceof MapsProviderError) throw error;
      if (signal.aborted) {
        throw new MapsProviderError("cancelled", "The map request was cancelled.", {
          provider,
          cause: error,
        });
      }
      if (timedOut) {
        throw new MapsProviderError("timeout", `${provider} took too long to respond.`, {
          provider,
          retryable: true,
          cause: error,
        });
      }
      throw new MapsProviderError("unavailable", `${provider} could not be reached.`, {
        provider,
        retryable: true,
        cause: error,
      });
    } finally {
      clearTimeout(timer);
      signal.removeEventListener("abort", abortFromCaller);
    }
  }

  #cacheGet<T>(key: string): T | undefined {
    const entry = this.#cache.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt <= this.#now()) {
      this.#cache.delete(key);
      return undefined;
    }
    this.#cache.delete(key);
    this.#cache.set(key, entry);
    return entry.value as T;
  }

  #cacheSet(key: string, value: unknown): void {
    this.#cache.delete(key);
    while (this.#cache.size >= MAX_CACHE_ENTRIES) {
      const oldest = this.#cache.keys().next().value;
      if (oldest === undefined) break;
      this.#cache.delete(oldest);
    }
    this.#cache.set(key, { expiresAt: this.#now() + this.#cacheTtlMs, value });
  }
}

function normalizeSearchPlace(item: NominatimPlace): z.infer<typeof mapSearchPlaceSchema> {
  const lat = numeric(item.lat, "Nominatim latitude");
  const lon = numeric(item.lon, "Nominatim longitude");
  return {
    name: item.name ?? item.display_name ?? "",
    displayName: item.display_name ?? "",
    lat,
    lon,
    type: item.type ?? "",
    category: item.category ?? "",
    osmType: item.osm_type ?? "",
    osmId: item.osm_id === undefined ? "" : String(item.osm_id),
    boundingBox: parseNominatimBounds(item.boundingbox),
    importance: item.importance ?? null,
    mapsUrl: googleMapsUrl(lat, lon),
  };
}

function parseNominatimBounds(
  boundingBox: readonly (string | number)[] | undefined,
): z.infer<typeof mapBoundingBoxResultSchema> | null {
  if (!boundingBox || boundingBox.length < 4) return null;
  const south = Number(boundingBox[0]);
  const north = Number(boundingBox[1]);
  const west = Number(boundingBox[2]);
  const east = Number(boundingBox[3]);
  if (![south, north, west, east].every(Number.isFinite)) return null;
  return { south, west, north, east };
}

function materiallyAmbiguousCandidates(candidates: readonly NominatimPlace[]): readonly NominatimPlace[] {
  const [first] = candidates;
  if (!first || candidates.length < 2) return candidates.slice(0, 1);
  const firstLatitude = Number(first.lat);
  const firstLongitude = Number(first.lon);
  const firstImportance = first.importance ?? 0;
  if (!Number.isFinite(firstLatitude) || !Number.isFinite(firstLongitude)) {
    return candidates.slice(0, 1);
  }

  const competitive = candidates.filter((candidate, index) => {
    if (index === 0) return true;
    const latitude = Number(candidate.lat);
    const longitude = Number(candidate.lon);
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return false;
    const importance = candidate.importance ?? 0;
    const similarlyLikely = importance >= Math.max(0.25, firstImportance * 0.75);
    const materiallyDistant = haversineMetres(firstLatitude, firstLongitude, latitude, longitude) >= 50_000;
    return similarlyLikely && materiallyDistant;
  });
  return competitive.length > 1 ? competitive : candidates.slice(0, 1);
}

function normalizeBounds(
  bounds: z.infer<typeof mapBoundsSchema>,
): z.infer<typeof mapBoundingBoxResultSchema> {
  const south = Math.min(bounds.south, bounds.north);
  const north = Math.max(bounds.south, bounds.north);
  const west = Math.min(bounds.west, bounds.east);
  const east = Math.max(bounds.west, bounds.east);
  if (south === north || west === east) {
    throw new MapsProviderError("invalid_input", "A places search area needs two distinct corners.");
  }
  return { south, west, north, east };
}

function buildNearbyQuery(
  category: MapCategory,
  center: ResolvedMapLocation,
  radiusM: number,
  limit: number,
): string {
  const religion = religionFilters[category];
  const religionFilter = religion ? `["religion"="${religion}"]` : "";
  const body = categoryTags[category]
    .flatMap(([key, value]) => [
      `  node["${key}"="${value}"]${religionFilter}(around:${radiusM},${center.lat},${center.lon});`,
      `  way["${key}"="${value}"]${religionFilter}(around:${radiusM},${center.lat},${center.lon});`,
    ])
    .join("\n");
  return `[out:json][timeout:25];\n(\n${body}\n);\nout center ${limit};\n`;
}

function buildBboxQuery(
  category: MapCategory,
  bounds: z.infer<typeof mapBoundingBoxResultSchema>,
  limit: number,
): string {
  const religion = religionFilters[category];
  const religionFilter = religion ? `["religion"="${religion}"]` : "";
  const box = `${bounds.south},${bounds.west},${bounds.north},${bounds.east}`;
  const body = categoryTags[category]
    .flatMap(([key, value]) => [
      `  node["${key}"="${value}"]${religionFilter}(${box});`,
      `  way["${key}"="${value}"]${religionFilter}(${box});`,
    ])
    .join("\n");
  return `[out:json][timeout:25];\n(\n${body}\n);\nout center ${limit};\n`;
}

function parseOverpassElements(
  elements: readonly OverpassElement[],
  center: ResolvedMapLocation,
  category: MapCategory,
): NearbyPlace[] {
  const places: NearbyPlace[] = [];
  for (const element of elements) {
    const lat = element.type === "node" ? element.lat : element.center?.lat;
    const lon = element.type === "node" ? element.lon : element.center?.lon;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    const safeLat = lat ?? 0;
    const safeLon = lon ?? 0;
    const tags = element.tags ?? {};
    const address = [tags["addr:housenumber"], tags["addr:street"], tags["addr:city"]]
      .filter((part): part is string => Boolean(part))
      .join(", ");
    const base = {
      name: tags.name ?? tags["name:en"] ?? null,
      address,
      lat: safeLat,
      lon: safeLon,
      osmType: element.type,
      osmId: String(element.id),
      category,
      distanceM: round(haversineMetres(center.lat, center.lon, safeLat, safeLon), 1),
      mapsUrl: googleMapsUrl(safeLat, safeLon),
      directionsUrl: googleDirectionsUrl(center.lat, center.lon, safeLat, safeLon),
      tags: boundedTags(tags),
    } satisfies Omit<NearbyPlace, "cuisine" | "hours" | "phone" | "website">;
    places.push({
      ...base,
      ...(tags.cuisine ? { cuisine: tags.cuisine.slice(0, 300) } : {}),
      ...(tags.opening_hours ? { hours: tags.opening_hours.slice(0, 300) } : {}),
      ...(tags.phone ? { phone: tags.phone.slice(0, 100) } : {}),
      ...(tags.website ? { website: tags.website.slice(0, 500) } : {}),
    });
  }
  return places;
}

function boundedTags(tags: Readonly<Record<string, string>>): Record<string, string> {
  return Object.fromEntries(
    Object.entries(tags)
      .filter(([key]) => !["name", "name:en", "addr:housenumber", "addr:street", "addr:city"].includes(key))
      .sort(([left], [right]) => left.localeCompare(right))
      .slice(0, 24)
      .map(([key, value]) => [key.slice(0, 100), value.slice(0, 300)]),
  );
}

function googleMapsUrl(lat: number, lon: number): string {
  const url = new URL("https://www.google.com/maps/search/");
  url.searchParams.set("api", "1");
  url.searchParams.set("query", `${lat},${lon}`);
  return url.toString();
}

function googleDirectionsUrl(
  originLat: number,
  originLon: number,
  destinationLat: number,
  destinationLon: number,
): string {
  const url = new URL("https://www.google.com/maps/dir/");
  url.searchParams.set("api", "1");
  url.searchParams.set("origin", `${originLat},${originLon}`);
  url.searchParams.set("destination", `${destinationLat},${destinationLon}`);
  return url.toString();
}

function haversineMetres(
  latitude1: number,
  longitude1: number,
  latitude2: number,
  longitude2: number,
): number {
  const earthRadiusMetres = 6_371_000;
  const phi1 = degreesToRadians(latitude1);
  const phi2 = degreesToRadians(latitude2);
  const deltaPhi = degreesToRadians(latitude2 - latitude1);
  const deltaLambda = degreesToRadians(longitude2 - longitude1);
  const a = Math.sin(deltaPhi / 2) ** 2 + Math.cos(phi1) * Math.cos(phi2) * Math.sin(deltaLambda / 2) ** 2;
  return 2 * earthRadiusMetres * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function degreesToRadians(value: number): number {
  return (value * Math.PI) / 180;
}

function formatUtcOffset(totalSeconds: number): string {
  if (!Number.isFinite(totalSeconds) || Math.abs(totalSeconds) > 86_400) {
    throw new MapsProviderError("invalid_response", "The timezone offset was invalid.", {
      provider: "TimeAPI.io",
    });
  }
  const integralSeconds = Math.trunc(totalSeconds);
  const sign = integralSeconds < 0 ? "-" : "+";
  const absolute = Math.abs(integralSeconds);
  const hours = Math.floor(absolute / 3_600);
  const minutes = Math.floor((absolute % 3_600) / 60);
  const seconds = absolute % 60;
  const base = `${sign}${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
  return seconds === 0 ? base : `${base}:${String(seconds).padStart(2, "0")}`;
}

function withQuery(endpoint: string, values: Readonly<Record<string, string | number>>): string {
  const url = new URL(endpoint);
  for (const [key, value] of Object.entries(values)) url.searchParams.set(key, String(value));
  return url.toString();
}

function validEndpoint(value: string): string {
  const url = new URL(value);
  if (url.protocol !== "https:") throw new TypeError("Maps provider endpoints must use HTTPS.");
  return url.toString();
}

function numeric(value: string | number, label: string): number {
  const result = Number(value);
  if (!Number.isFinite(result)) {
    throw new MapsProviderError("invalid_response", `${label} was invalid.`, {
      provider: "Nominatim",
    });
  }
  return result;
}

function nonNegative(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : fallback;
}

function round(value: number, decimalPlaces: number): number {
  const factor = 10 ** decimalPlaces;
  return Math.round(value * factor) / factor;
}

function clampInteger(value: number, minimum: number, maximum: number): number {
  if (!Number.isFinite(value)) return minimum;
  return Math.min(maximum, Math.max(minimum, Math.trunc(value)));
}

function httpProviderError(provider: string, status: number): MapsProviderError {
  if (status === 404) {
    return new MapsProviderError("not_found", `${provider} could not find that map result.`, {
      provider,
      status,
    });
  }
  if (status === 429) {
    return new MapsProviderError("rate_limited", `${provider} is busy right now.`, {
      provider,
      status,
      retryable: true,
    });
  }
  const retryable = status === 408 || status === 425 || status >= 500;
  return new MapsProviderError("unavailable", `${provider} returned HTTP ${status}.`, {
    provider,
    status,
    retryable,
  });
}

function asMapsProviderError(error: unknown, provider: string): MapsProviderError {
  if (error instanceof MapsProviderError) return error;
  return new MapsProviderError("unavailable", `${provider} could not complete the request.`, {
    provider,
    retryable: true,
    cause: error,
  });
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) {
    throw new MapsProviderError("cancelled", "The map request was cancelled.");
  }
}

function abortableSleep(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (milliseconds <= 0) return Promise.resolve();
  throwIfAborted(signal);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    const onAbort = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      reject(new MapsProviderError("cancelled", "The map request was cancelled."));
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}
