import { z } from "zod";

/**
 * Adapted port of Hermes Agent's bounded weather fetch and current-condition
 * vocabulary from ui-tui/src/sdk/apps/weather.tsx at commit
 * 6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882.
 *
 * Florence replaces Hermes's wttr.in location lookup with the official
 * api.weather.gov points workflow so a single coordinate request can return
 * the NWS forecast, nearest-station observation, and active warnings.
 */

const DEFAULT_NWS_API_URL = "https://api.weather.gov";
const USER_AGENT = "FlorenceFamilyAssistant/0.1 (+https://github.com/harianbarasu/florence)";
const DEFAULT_TIMEOUT_MS = 10_000;
const MAX_RESPONSE_BYTES = 2 * 1_024 * 1_024;
const MAX_CACHE_ENTRIES = 64;
const MAX_ALERTS = 20;
const MIN_ALERT_CACHE_MS = 30_000;
const MAX_CACHE_TTL_MS = 24 * 60 * 60 * 1_000;

const weatherCoordinatesSchema = z
  .object({
    lat: z.number().finite().min(-90).max(90),
    lon: z.number().finite().min(-180).max(180),
  })
  .strict();

const dailyForecastRequestSchema = z
  .object({
    coordinates: weatherCoordinatesSchema,
    kind: z.literal("daily"),
    periodCount: z.number().int().min(1).max(14),
  })
  .strict();

const hourlyForecastRequestSchema = z
  .object({
    coordinates: weatherCoordinatesSchema,
    kind: z.literal("hourly"),
    periodCount: z.number().int().min(1).max(48),
  })
  .strict();

export const weatherForecastRequestSchema = z.discriminatedUnion("kind", [
  dailyForecastRequestSchema,
  hourlyForecastRequestSchema,
]);

const weatherAttributionSchema = z
  .object({
    provider: z.literal("National Weather Service"),
    label: z.literal("Weather data from the U.S. National Weather Service"),
    url: z.string().url(),
  })
  .strict();

const weatherLocationSchema = z
  .object({
    city: z.string().min(1),
    state: z.string().min(1),
    timeZone: z.string().min(1),
    forecastOfficeUrl: z.string().url(),
    gridId: z.string().min(1),
    gridX: z.number().int(),
    gridY: z.number().int(),
  })
  .strict();

const weatherObservationSchema = z
  .object({
    stationId: z.string().min(1),
    stationName: z.string().min(1),
    stationUrl: z.string().url(),
    observedAt: z.string().datetime({ offset: true }),
    condition: z.string(),
    temperatureC: z.number().nullable(),
    feelsLikeC: z.number().nullable(),
    humidityPercent: z.number().nullable(),
    windSpeedKph: z.number().nullable(),
    windGustKph: z.number().nullable(),
    windDirectionDegrees: z.number().nullable(),
  })
  .strict();

const weatherForecastPeriodSchema = z
  .object({
    number: z.number().int(),
    name: z.string(),
    startTime: z.string().datetime({ offset: true }),
    endTime: z.string().datetime({ offset: true }),
    isDaytime: z.boolean(),
    temperature: z.number(),
    temperatureUnit: z.string(),
    precipitationChancePercent: z.number().min(0).max(100).nullable(),
    windSpeed: z.string(),
    windDirection: z.string(),
    condition: z.string(),
    detailedForecast: z.string(),
    iconUrl: z.string().url().nullable(),
  })
  .strict();

const weatherAlertSchema = z
  .object({
    id: z.string().min(1),
    event: z.string(),
    headline: z.string(),
    areaDescription: z.string(),
    severity: z.string(),
    certainty: z.string(),
    urgency: z.string(),
    sentAt: z.string().datetime({ offset: true }).nullable(),
    effectiveAt: z.string().datetime({ offset: true }).nullable(),
    onsetAt: z.string().datetime({ offset: true }).nullable(),
    expiresAt: z.string().datetime({ offset: true }).nullable(),
    endsAt: z.string().datetime({ offset: true }).nullable(),
    description: z.string(),
    instruction: z.string(),
  })
  .strict();

export const weatherForecastResultSchema = z
  .object({
    kind: z.enum(["daily", "hourly"]),
    coordinates: weatherCoordinatesSchema,
    location: weatherLocationSchema,
    requestedPeriodCount: z.number().int().positive(),
    forecastGeneratedAt: z.string().datetime({ offset: true }).nullable(),
    forecastUpdatedAt: z.string().datetime({ offset: true }).nullable(),
    periods: z.array(weatherForecastPeriodSchema).min(1).max(48),
    observation: weatherObservationSchema.nullable(),
    activeAlertCount: z.number().int().nonnegative(),
    alertsTruncated: z.boolean(),
    alerts: z.array(weatherAlertSchema).max(MAX_ALERTS),
    fetchedAt: z.string().datetime({ offset: true }),
    attribution: weatherAttributionSchema,
  })
  .strict();

export type FlorenceWeatherRequest = z.infer<typeof weatherForecastRequestSchema>;
export type FlorenceWeatherResult = z.infer<typeof weatherForecastResultSchema>;

export type WeatherProviderErrorCode =
  | "invalid_input"
  | "not_found"
  | "unavailable"
  | "timeout"
  | "cancelled"
  | "invalid_response";

export class WeatherProviderError extends Error {
  readonly code: WeatherProviderErrorCode;
  readonly safeMessage: string;
  readonly retryable: boolean;
  readonly provider: string | undefined;
  readonly status: number | undefined;

  constructor(
    code: WeatherProviderErrorCode,
    safeMessage: string,
    options: {
      readonly retryable?: boolean;
      readonly provider?: string;
      readonly status?: number;
      readonly cause?: unknown;
    } = {},
  ) {
    super(safeMessage, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "WeatherProviderError";
    this.code = code;
    this.safeMessage = safeMessage.slice(0, 300);
    this.retryable = options.retryable ?? false;
    this.provider = options.provider;
    this.status = options.status;
  }
}

export interface FlorenceWeatherClient {
  run(request: FlorenceWeatherRequest, signal?: AbortSignal): Promise<FlorenceWeatherResult>;
}

export interface NwsWeatherClientOptions {
  readonly fetch?: typeof globalThis.fetch;
  readonly now?: () => number;
  readonly sleep?: (milliseconds: number, signal: AbortSignal) => Promise<void>;
  readonly timeoutMs?: number;
  readonly apiUrl?: string;
}

const pointResponseSchema = z
  .object({
    properties: z
      .object({
        gridId: z.string().min(1),
        gridX: z.number().int(),
        gridY: z.number().int(),
        forecast: z.string().url(),
        forecastHourly: z.string().url(),
        forecastOffice: z.string().url(),
        observationStations: z.string().url(),
        timeZone: z.string().min(1),
        relativeLocation: z.object({
          properties: z.object({ city: z.string().min(1), state: z.string().min(1) }).passthrough(),
        }),
      })
      .passthrough(),
  })
  .passthrough();

const probabilitySchema = z
  .object({
    value: z.number().nullable().optional(),
  })
  .passthrough();

const forecastPeriodProviderSchema = z
  .object({
    number: z.number().int(),
    name: z.string().optional(),
    startTime: z.string(),
    endTime: z.string(),
    isDaytime: z.boolean(),
    temperature: z.number(),
    temperatureUnit: z.string().optional(),
    probabilityOfPrecipitation: probabilitySchema.nullable().optional(),
    windSpeed: z.string().optional(),
    windDirection: z.string().optional(),
    shortForecast: z.string().optional(),
    detailedForecast: z.string().optional(),
    icon: z.string().optional(),
  })
  .passthrough();

const forecastResponseSchema = z
  .object({
    properties: z
      .object({
        generatedAt: z.string().nullable().optional(),
        updated: z.string().nullable().optional(),
        periods: z.array(forecastPeriodProviderSchema).max(200),
      })
      .passthrough(),
  })
  .passthrough();

const stationFeatureSchema = z
  .object({
    id: z.string().url(),
    properties: z
      .object({
        stationIdentifier: z.string().optional(),
        name: z.string().optional(),
      })
      .passthrough(),
  })
  .passthrough();

const stationCollectionSchema = z.object({ features: z.array(stationFeatureSchema).max(500) }).passthrough();

const quantitySchema = z
  .object({
    value: z.number().nullable().optional(),
    unitCode: z.string().optional(),
  })
  .nullable()
  .optional();

const observationResponseSchema = z
  .object({
    properties: z
      .object({
        timestamp: z.string().datetime({ offset: true }),
        textDescription: z.string().optional(),
        temperature: quantitySchema,
        windChill: quantitySchema,
        heatIndex: quantitySchema,
        relativeHumidity: quantitySchema,
        windDirection: quantitySchema,
        windSpeed: quantitySchema,
        windGust: quantitySchema,
      })
      .passthrough(),
  })
  .passthrough();

const alertPropertiesSchema = z
  .object({
    id: z.string().optional(),
    event: z.string().nullable().optional(),
    headline: z.string().nullable().optional(),
    areaDesc: z.string().nullable().optional(),
    severity: z.string().nullable().optional(),
    certainty: z.string().nullable().optional(),
    urgency: z.string().nullable().optional(),
    sent: z.string().nullable().optional(),
    effective: z.string().nullable().optional(),
    onset: z.string().nullable().optional(),
    expires: z.string().nullable().optional(),
    ends: z.string().nullable().optional(),
    description: z.string().nullable().optional(),
    instruction: z.string().nullable().optional(),
  })
  .passthrough();

const alertFeatureSchema = z
  .object({
    id: z.string().optional(),
    properties: alertPropertiesSchema,
  })
  .passthrough();

const alertCollectionSchema = z.object({ features: z.array(alertFeatureSchema).max(500) }).passthrough();

type ParsedWeatherRequest = z.infer<typeof weatherForecastRequestSchema>;
type PointResponse = z.infer<typeof pointResponseSchema>;
type ForecastResponse = z.infer<typeof forecastResponseSchema>;
type StationFeature = z.infer<typeof stationFeatureSchema>;
type ObservationResponse = z.infer<typeof observationResponseSchema>;
type AlertFeature = z.infer<typeof alertFeatureSchema>;
type Quantity = z.infer<typeof quantitySchema>;

type CacheEntry = { readonly expiresAt: number; readonly value: unknown };

type RequestJsonOptions = {
  readonly fallbackCacheMs: number;
  readonly minimumCacheMs?: number;
  readonly attempts?: number;
};

type FetchJsonResult = {
  readonly value: unknown;
  readonly cacheTtlMs: number;
};

export class NwsWeatherClient implements FlorenceWeatherClient {
  readonly #fetch: typeof globalThis.fetch;
  readonly #now: () => number;
  readonly #sleep: (milliseconds: number, signal: AbortSignal) => Promise<void>;
  readonly #timeoutMs: number;
  readonly #apiUrl: string;
  readonly #cache = new Map<string, CacheEntry>();

  constructor(options: NwsWeatherClientOptions = {}) {
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#now = options.now ?? Date.now;
    this.#sleep = options.sleep ?? abortableSleep;
    this.#timeoutMs = clampInteger(options.timeoutMs ?? DEFAULT_TIMEOUT_MS, 1_000, 60_000);
    this.#apiUrl = normalizeApiUrl(options.apiUrl ?? DEFAULT_NWS_API_URL);
  }

  async run(request: FlorenceWeatherRequest, signal?: AbortSignal): Promise<FlorenceWeatherResult> {
    const parsed = weatherForecastRequestSchema.safeParse(request);
    if (!parsed.success) {
      throw new WeatherProviderError("invalid_input", "The weather request was invalid.");
    }
    const localSignal = signal ?? new AbortController().signal;
    throwIfAborted(localSignal);

    const point = await this.#loadPoint(parsed.data, localSignal);
    const forecastUrl =
      parsed.data.kind === "hourly" ? point.properties.forecastHourly : point.properties.forecast;
    const [forecast, alerts, observation] = await Promise.all([
      this.#requestJson(forecastUrl, forecastResponseSchema, localSignal, {
        fallbackCacheMs: 5 * 60_000,
        attempts: 2,
      }),
      this.#loadAlerts(parsed.data, localSignal),
      this.#loadObservation(point, localSignal),
    ]);

    const result = normalizeResult(parsed.data, point, forecast, observation, alerts, this.#now());
    const validated = weatherForecastResultSchema.safeParse(result);
    if (!validated.success) {
      throw new WeatherProviderError(
        "invalid_response",
        "The National Weather Service returned weather Florence could not use.",
        { provider: "National Weather Service", cause: validated.error },
      );
    }
    return validated.data;
  }

  async #loadPoint(request: ParsedWeatherRequest, signal: AbortSignal): Promise<PointResponse> {
    const coordinates = `${formatCoordinate(request.coordinates.lat)},${formatCoordinate(request.coordinates.lon)}`;
    return this.#requestJson(`${this.#apiUrl}/points/${coordinates}`, pointResponseSchema, signal, {
      fallbackCacheMs: 6 * 60 * 60_000,
      attempts: 2,
    });
  }

  async #loadAlerts(request: ParsedWeatherRequest, signal: AbortSignal): Promise<readonly AlertFeature[]> {
    const url = new URL(`${this.#apiUrl}/alerts/active`);
    url.searchParams.set(
      "point",
      `${formatCoordinate(request.coordinates.lat)},${formatCoordinate(request.coordinates.lon)}`,
    );
    const response = await this.#requestJson(url.toString(), alertCollectionSchema, signal, {
      fallbackCacheMs: MIN_ALERT_CACHE_MS,
      minimumCacheMs: MIN_ALERT_CACHE_MS,
      attempts: 1,
    });
    return response.features;
  }

  async #loadObservation(
    point: PointResponse,
    signal: AbortSignal,
  ): Promise<{ readonly station: StationFeature; readonly report: ObservationResponse } | null> {
    try {
      const stations = await this.#requestJson(
        point.properties.observationStations,
        stationCollectionSchema,
        signal,
        { fallbackCacheMs: 6 * 60 * 60_000, attempts: 2 },
      );
      const station = stations.features[0];
      if (!station) return null;
      const report = await this.#requestJson(
        `${station.id.replace(/\/$/, "")}/observations/latest`,
        observationResponseSchema,
        signal,
        { fallbackCacheMs: 60_000, attempts: 2 },
      );
      return { station, report };
    } catch (error) {
      const providerError = asWeatherProviderError(error);
      if (providerError.code === "cancelled") throw providerError;
      return null;
    }
  }

  async #requestJson<T>(
    url: string,
    schema: z.ZodType<T>,
    signal: AbortSignal,
    options: RequestJsonOptions,
  ): Promise<T> {
    const cached = this.#cacheGet<T>(url);
    if (cached !== undefined) return cached;

    const attempts = clampInteger(options.attempts ?? 1, 1, 3);
    let lastError: WeatherProviderError | undefined;
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      throwIfAborted(signal);
      try {
        const fetched = await this.#fetchJsonOnce(url, signal, options);
        const parsed = schema.safeParse(fetched.value);
        if (!parsed.success) {
          throw new WeatherProviderError(
            "invalid_response",
            "The National Weather Service returned a response Florence could not use.",
            { provider: "National Weather Service", cause: parsed.error },
          );
        }
        if (fetched.cacheTtlMs > 0) this.#cacheSet(url, parsed.data, fetched.cacheTtlMs);
        return parsed.data;
      } catch (error) {
        const providerError = asWeatherProviderError(error);
        lastError = providerError;
        if (!providerError.retryable || attempt === attempts) throw providerError;
        await this.#sleep(Math.min(250 * attempt, 1_000), signal);
      }
    }
    throw (
      lastError ?? new WeatherProviderError("unavailable", "The National Weather Service is unavailable.")
    );
  }

  async #fetchJsonOnce(
    url: string,
    signal: AbortSignal,
    options: RequestJsonOptions,
  ): Promise<FetchJsonResult> {
    const controller = new AbortController();
    let timedOut = false;
    const abortFromCaller = () => controller.abort(signal.reason);
    signal.addEventListener("abort", abortFromCaller, { once: true });
    const timer = setTimeout(() => {
      timedOut = true;
      controller.abort(new Error("provider timeout"));
    }, this.#timeoutMs);

    try {
      const response = await this.#fetch(url, {
        headers: {
          Accept: "application/geo+json",
          "User-Agent": USER_AGENT,
        },
        signal: controller.signal,
      });
      if (!response.ok) throw httpProviderError(response.status);
      const text = await response.text();
      if (Buffer.byteLength(text, "utf8") > MAX_RESPONSE_BYTES) {
        throw new WeatherProviderError(
          "invalid_response",
          "The National Weather Service returned more weather data than Florence can use.",
          { provider: "National Weather Service" },
        );
      }
      let value: unknown;
      try {
        value = JSON.parse(text) as unknown;
      } catch (error) {
        throw new WeatherProviderError(
          "invalid_response",
          "The National Weather Service returned invalid JSON.",
          { provider: "National Weather Service", cause: error },
        );
      }
      return {
        value,
        cacheTtlMs: responseCacheTtlMs(
          response.headers.get("Cache-Control"),
          options.fallbackCacheMs,
          options.minimumCacheMs ?? 0,
        ),
      };
    } catch (error) {
      if (error instanceof WeatherProviderError) throw error;
      if (signal.aborted) {
        throw new WeatherProviderError("cancelled", "The weather request was cancelled.", {
          provider: "National Weather Service",
          cause: error,
        });
      }
      if (timedOut) {
        throw new WeatherProviderError("timeout", "The National Weather Service took too long to respond.", {
          provider: "National Weather Service",
          retryable: true,
          cause: error,
        });
      }
      throw new WeatherProviderError("unavailable", "The National Weather Service could not be reached.", {
        provider: "National Weather Service",
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

  #cacheSet(key: string, value: unknown, ttlMs: number): void {
    this.#cache.delete(key);
    while (this.#cache.size >= MAX_CACHE_ENTRIES) {
      const oldest = this.#cache.keys().next().value;
      if (oldest === undefined) break;
      this.#cache.delete(oldest);
    }
    this.#cache.set(key, { expiresAt: this.#now() + ttlMs, value });
  }
}

function normalizeResult(
  request: ParsedWeatherRequest,
  point: PointResponse,
  forecast: ForecastResponse,
  observation: { readonly station: StationFeature; readonly report: ObservationResponse } | null,
  alerts: readonly AlertFeature[],
  now: number,
): FlorenceWeatherResult {
  const pointProperties = point.properties;
  const periods = forecast.properties.periods.slice(0, request.periodCount).map((period) => ({
    number: period.number,
    name: boundedText(period.name ?? "", 120),
    startTime: period.startTime,
    endTime: period.endTime,
    isDaytime: period.isDaytime,
    temperature: period.temperature,
    temperatureUnit: boundedText(period.temperatureUnit ?? "", 20),
    precipitationChancePercent: percentOrNull(period.probabilityOfPrecipitation?.value),
    windSpeed: boundedText(period.windSpeed ?? "", 120),
    windDirection: boundedText(period.windDirection ?? "", 40),
    condition: boundedText(period.shortForecast ?? "", 300),
    detailedForecast: boundedText(period.detailedForecast ?? "", 2_000),
    iconUrl: validUrlOrNull(period.icon),
  }));
  if (periods.length === 0) {
    throw new WeatherProviderError(
      "invalid_response",
      "The National Weather Service returned no forecast periods.",
      {
        provider: "National Weather Service",
      },
    );
  }

  return {
    kind: request.kind,
    coordinates: request.coordinates,
    location: {
      city: pointProperties.relativeLocation.properties.city,
      state: pointProperties.relativeLocation.properties.state,
      timeZone: pointProperties.timeZone,
      forecastOfficeUrl: pointProperties.forecastOffice,
      gridId: pointProperties.gridId,
      gridX: pointProperties.gridX,
      gridY: pointProperties.gridY,
    },
    requestedPeriodCount: request.periodCount,
    forecastGeneratedAt: validDateTimeOrNull(forecast.properties.generatedAt),
    forecastUpdatedAt: validDateTimeOrNull(forecast.properties.updated),
    periods,
    observation: observation === null ? null : normalizeObservation(observation),
    activeAlertCount: alerts.length,
    alertsTruncated: alerts.length > MAX_ALERTS,
    alerts: alerts.slice(0, MAX_ALERTS).map(normalizeAlert),
    fetchedAt: new Date(now).toISOString(),
    attribution: {
      provider: "National Weather Service",
      label: "Weather data from the U.S. National Weather Service",
      url: "https://www.weather.gov/documentation/services-web-api",
    },
  };
}

function normalizeObservation(input: {
  readonly station: StationFeature;
  readonly report: ObservationResponse;
}): NonNullable<FlorenceWeatherResult["observation"]> {
  const properties = input.report.properties;
  const temperatureC = temperatureCelsius(properties.temperature);
  const heatIndexC = temperatureCelsius(properties.heatIndex);
  const windChillC = temperatureCelsius(properties.windChill);
  return {
    stationId: input.station.properties.stationIdentifier ?? stationIdFromUrl(input.station.id),
    stationName: boundedText(input.station.properties.name ?? "Nearest reporting station", 200),
    stationUrl: input.station.id,
    observedAt: properties.timestamp,
    condition: boundedText(properties.textDescription || "Unknown", 300),
    temperatureC,
    feelsLikeC: heatIndexC ?? windChillC ?? temperatureC,
    humidityPercent: percentOrNull(properties.relativeHumidity?.value),
    windSpeedKph: speedKph(properties.windSpeed),
    windGustKph: speedKph(properties.windGust),
    windDirectionDegrees: degreesOrNull(properties.windDirection?.value),
  };
}

function normalizeAlert(alert: AlertFeature): FlorenceWeatherResult["alerts"][number] {
  const properties = alert.properties;
  const id = properties.id ?? alert.id;
  if (!id) {
    throw new WeatherProviderError(
      "invalid_response",
      "The National Weather Service returned an alert without an ID.",
      {
        provider: "National Weather Service",
      },
    );
  }
  return {
    id: boundedText(id, 500),
    event: boundedText(properties.event ?? "", 200),
    headline: boundedText(properties.headline ?? "", 500),
    areaDescription: boundedText(properties.areaDesc ?? "", 1_000),
    severity: boundedText(properties.severity ?? "Unknown", 40),
    certainty: boundedText(properties.certainty ?? "Unknown", 40),
    urgency: boundedText(properties.urgency ?? "Unknown", 40),
    sentAt: validDateTimeOrNull(properties.sent),
    effectiveAt: validDateTimeOrNull(properties.effective),
    onsetAt: validDateTimeOrNull(properties.onset),
    expiresAt: validDateTimeOrNull(properties.expires),
    endsAt: validDateTimeOrNull(properties.ends),
    description: boundedText(properties.description ?? "", 4_000),
    instruction: boundedText(properties.instruction ?? "", 2_000),
  };
}

function formatCoordinate(value: number): string {
  return Number(value.toFixed(4)).toString();
}

function temperatureCelsius(quantity: Quantity): number | null {
  const value = quantity?.value;
  if (value === null || value === undefined || !Number.isFinite(value)) return null;
  const unit = quantity?.unitCode ?? "";
  if (unit.endsWith(":degF")) return round((value - 32) * (5 / 9), 1);
  if (unit.endsWith(":K")) return round(value - 273.15, 1);
  return round(value, 1);
}

function speedKph(quantity: Quantity): number | null {
  const value = quantity?.value;
  if (value === null || value === undefined || !Number.isFinite(value)) return null;
  const unit = quantity?.unitCode ?? "";
  if (unit.endsWith(":m_s-1")) return round(value * 3.6, 1);
  if (unit.endsWith(":mi_h-1")) return round(value * 1.609344, 1);
  if (unit.endsWith(":kn")) return round(value * 1.852, 1);
  return round(value, 1);
}

function percentOrNull(value: number | null | undefined): number | null {
  if (value === null || value === undefined || !Number.isFinite(value)) return null;
  return Math.min(100, Math.max(0, round(value, 1)));
}

function degreesOrNull(value: number | null | undefined): number | null {
  if (value === null || value === undefined || !Number.isFinite(value)) return null;
  return round(((value % 360) + 360) % 360, 1);
}

function validDateTimeOrNull(value: string | null | undefined): string | null {
  if (!value || !Number.isFinite(Date.parse(value))) return null;
  return value;
}

function validUrlOrNull(value: string | undefined): string | null {
  if (!value) return null;
  try {
    return new URL(value).toString();
  } catch {
    return null;
  }
}

function stationIdFromUrl(value: string): string {
  return value.split("/").filter(Boolean).at(-1) ?? "unknown";
}

function boundedText(value: string, maxLength: number): string {
  return value.trim().slice(0, maxLength);
}

function responseCacheTtlMs(cacheControl: string | null, fallbackMs: number, minimumMs: number): number {
  const noStore = cacheControl?.toLowerCase().includes("no-store") ?? false;
  const match = cacheControl?.match(/(?:^|,)\s*max-age\s*=\s*(\d+)/i);
  const providerMs = match?.[1] === undefined ? fallbackMs : Number(match[1]) * 1_000;
  if (noStore && minimumMs === 0) return 0;
  return Math.min(
    MAX_CACHE_TTL_MS,
    Math.max(minimumMs, Number.isFinite(providerMs) ? providerMs : fallbackMs),
  );
}

function httpProviderError(status: number): WeatherProviderError {
  if (status === 404) {
    return new WeatherProviderError(
      "not_found",
      "The National Weather Service has no forecast for those coordinates.",
      {
        provider: "National Weather Service",
        status,
      },
    );
  }
  if (status === 400 || status === 422) {
    return new WeatherProviderError(
      "invalid_input",
      "The National Weather Service rejected those coordinates.",
      {
        provider: "National Weather Service",
        status,
      },
    );
  }
  return new WeatherProviderError("unavailable", `The National Weather Service answered ${status}.`, {
    provider: "National Weather Service",
    retryable: status === 408 || status === 425 || status === 429 || status >= 500,
    status,
  });
}

function asWeatherProviderError(error: unknown): WeatherProviderError {
  if (error instanceof WeatherProviderError) return error;
  return new WeatherProviderError(
    "unavailable",
    "The National Weather Service could not complete the request.",
    {
      provider: "National Weather Service",
      retryable: true,
      cause: error,
    },
  );
}

function normalizeApiUrl(value: string): string {
  const url = new URL(value);
  if (url.protocol !== "https:" && url.hostname !== "localhost" && url.hostname !== "127.0.0.1") {
    throw new TypeError("NwsWeatherClient requires an HTTPS endpoint.");
  }
  return url.toString().replace(/\/$/, "");
}

function clampInteger(value: number, minimum: number, maximum: number): number {
  if (!Number.isFinite(value)) return minimum;
  return Math.min(maximum, Math.max(minimum, Math.trunc(value)));
}

function round(value: number, decimalPlaces: number): number {
  const factor = 10 ** decimalPlaces;
  return Math.round((value + Number.EPSILON) * factor) / factor;
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) {
    throw new WeatherProviderError("cancelled", "The weather request was cancelled.");
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
      reject(new WeatherProviderError("cancelled", "The weather request was cancelled."));
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

export const defaultFlorenceWeatherClient: FlorenceWeatherClient = new NwsWeatherClient();
