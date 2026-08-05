import { z } from "zod";
import {
  type ModelCapabilityProfile,
  ModelCapabilityProfileSchema,
  ModelRouteCapabilitiesSchema,
} from "./contracts.js";
import { ModelGatewayError } from "./errors.js";
import { supportsCapabilityProfile } from "./gateway.js";

const BaseRouteConfigSchema = z.object({
  routeId: z.string().min(1),
  profile: ModelCapabilityProfileSchema,
  model: z.string().min(1),
  version: z.string().min(1).optional(),
  capabilities: ModelRouteCapabilitiesSchema,
  maxRetries: z.number().int().min(0).max(10).optional(),
  timeoutMs: z.number().int().positive().optional(),
});

const OpenAIRouteConfigSchema = BaseRouteConfigSchema.extend({
  provider: z.literal("openai"),
  apiKey: z.string().min(1),
}).strict();

const AnthropicRouteConfigSchema = BaseRouteConfigSchema.extend({
  provider: z.literal("anthropic"),
  apiKey: z.string().min(1),
}).strict();

const OpenAICompatibleRouteConfigSchema = BaseRouteConfigSchema.extend({
  provider: z.literal("openai-compatible"),
  baseUrl: z.url(),
  apiKey: z.string().min(1).optional(),
}).strict();

export const ModelRouteConfigSchema = z.discriminatedUnion("provider", [
  OpenAIRouteConfigSchema,
  AnthropicRouteConfigSchema,
  OpenAICompatibleRouteConfigSchema,
]);

export type ModelRouteConfig = z.infer<typeof ModelRouteConfigSchema>;

export const ModelGatewayConfigSchema = z
  .object({
    routes: z.array(ModelRouteConfigSchema).min(1).max(ModelCapabilityProfileSchema.options.length),
    openAICompatibleBaseUrlAllowlist: z.array(z.url()).max(50).default([]),
  })
  .strict();

export type ModelGatewayConfig = z.input<typeof ModelGatewayConfigSchema>;
export type ValidatedModelGatewayConfig = z.output<typeof ModelGatewayConfigSchema>;

export function validateModelGatewayConfig(config: ModelGatewayConfig): ValidatedModelGatewayConfig {
  const parsed = ModelGatewayConfigSchema.safeParse(config);
  if (!parsed.success) {
    throw new ModelGatewayError("permanent");
  }

  const routeIds = new Set<string>();
  const profiles = new Set<ModelCapabilityProfile>();
  const allowlist = new Set(
    parsed.data.openAICompatibleBaseUrlAllowlist.map((value) => normalizeAllowedBaseUrl(value)),
  );

  for (const route of parsed.data.routes) {
    if (routeIds.has(route.routeId) || profiles.has(route.profile)) {
      throw new ModelGatewayError("permanent");
    }
    routeIds.add(route.routeId);
    profiles.add(route.profile);

    if (!supportsCapabilityProfile(route.profile, route.capabilities)) {
      throw new ModelGatewayError("unsupported_capability");
    }

    if (route.provider === "openai-compatible") {
      const normalized = normalizeAllowedBaseUrl(route.baseUrl);
      if (!allowlist.has(normalized)) {
        throw new ModelGatewayError("permanent");
      }
      route.baseUrl = normalized;
    }
  }

  parsed.data.openAICompatibleBaseUrlAllowlist = [...allowlist];
  return parsed.data;
}

export function normalizeAllowedBaseUrl(value: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new ModelGatewayError("permanent");
  }

  if (url.username !== "" || url.password !== "" || url.search !== "" || url.hash !== "") {
    throw new ModelGatewayError("permanent");
  }

  const localHost = url.hostname === "localhost" || url.hostname === "127.0.0.1" || url.hostname === "[::1]";
  if (url.protocol !== "https:" && !(url.protocol === "http:" && localHost)) {
    throw new ModelGatewayError("permanent");
  }

  url.pathname = url.pathname.replace(/\/+$/, "");
  return url.toString().replace(/\/$/, "");
}
