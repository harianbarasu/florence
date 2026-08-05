import type { FlorenceConfig } from "../config.js";

/**
 * Provider metadata readiness with a single bounded request per cache interval.
 * It intentionally never invokes a model or sends prompt/customer content.
 */
export class CachedModelReadiness {
  readonly #config: FlorenceConfig;
  readonly #cacheMs: number;
  #cached: { ready: boolean; expiresAt: number } | null = null;
  #checking: Promise<boolean> | null = null;

  public constructor(config: FlorenceConfig, cacheMs = 30_000) {
    this.#config = config;
    this.#cacheMs = cacheMs;
  }

  public async isReady(): Promise<boolean> {
    const now = Date.now();
    if (this.#cached !== null && this.#cached.expiresAt > now) return this.#cached.ready;
    if (this.#checking !== null) return this.#checking;
    const checking = this.#check()
      .then(
        (ready) => ready,
        () => false,
      )
      .then((ready) => {
        this.#cached = { ready, expiresAt: Date.now() + this.#cacheMs };
        return ready;
      })
      .finally(() => {
        if (this.#checking === checking) this.#checking = null;
      });
    this.#checking = checking;
    return checking;
  }

  async #check(): Promise<boolean> {
    const request = modelReadinessRequest(this.#config);
    const response = await fetch(request.url, {
      method: "GET",
      headers: request.headers,
      signal: AbortSignal.timeout(2_000),
    });
    return response.ok;
  }
}

function modelReadinessRequest(config: FlorenceConfig): { url: string; headers: Record<string, string> } {
  switch (config.MODEL_PROVIDER) {
    case "openai":
      return {
        url: new URL("models", ensureTrailingSlash(config.OPENAI_BASE_URL)).toString(),
        headers: { authorization: `Bearer ${config.OPENAI_API_KEY as string}` },
      };
    case "anthropic":
      return {
        url: "https://api.anthropic.com/v1/models",
        headers: {
          "x-api-key": config.ANTHROPIC_API_KEY as string,
          "anthropic-version": "2023-06-01",
        },
      };
    case "open-weight":
      return {
        url: new URL("models", ensureTrailingSlash(config.OPEN_WEIGHT_BASE_URL as string)).toString(),
        headers: config.OPEN_WEIGHT_API_KEY ? { authorization: `Bearer ${config.OPEN_WEIGHT_API_KEY}` } : {},
      };
  }
}

function ensureTrailingSlash(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}
