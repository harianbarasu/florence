import type { FlorenceConfig } from "../config.js";
import {
  createModelGateway,
  type ModelCapabilityProfile,
  type ModelGateway,
  type ModelGatewayConfig,
  type ModelRouteCapabilities,
  type ModelRouteConfig,
} from "../models/index.js";

const ALL_CAPABILITIES: ModelRouteCapabilities = Object.freeze({
  structuredOutput: true,
  toolCalling: true,
  vision: true,
  documentUnderstanding: true,
  longContext: true,
  privateProcessing: true,
});

const ROUTE_IDS: Readonly<Record<ModelCapabilityProfile, string>> = Object.freeze({
  classification_extraction: "route.classification.v1",
  tool_planning: "route.meal_plan.v1",
  vision_document: "route.vision.v1",
  long_context_research: "route.family_research.v1",
  private_processing: "route.private.v1",
});

const PROFILES = Object.keys(ROUTE_IDS) as ModelCapabilityProfile[];

export function modelGatewayConfigFromFlorenceConfig(config: FlorenceConfig): ModelGatewayConfig {
  const routes = PROFILES.map((profile) => routeFor(config, profile));
  return {
    routes,
    openAICompatibleBaseUrlAllowlist:
      config.MODEL_PROVIDER === "open-weight" && config.OPEN_WEIGHT_BASE_URL
        ? [config.OPEN_WEIGHT_BASE_URL]
        : [],
  };
}

export function createConfiguredModelGateway(config: FlorenceConfig): ModelGateway {
  return createModelGateway(modelGatewayConfigFromFlorenceConfig(config));
}

function routeFor(config: FlorenceConfig, profile: ModelCapabilityProfile): ModelRouteConfig {
  const common = {
    routeId: ROUTE_IDS[profile],
    profile,
    capabilities: ALL_CAPABILITIES,
    maxRetries: 2,
    timeoutMs: profile === "long_context_research" ? 120_000 : 45_000,
  } as const;

  switch (config.MODEL_PROVIDER) {
    case "openai":
      if (!config.OPENAI_API_KEY) throw new Error("MODEL_PROVIDER=openai requires OPENAI_API_KEY");
      return {
        ...common,
        provider: "openai",
        apiKey: config.OPENAI_API_KEY,
        model: config.OPENAI_MODEL,
      };
    case "anthropic":
      if (!config.ANTHROPIC_API_KEY) {
        throw new Error("MODEL_PROVIDER=anthropic requires ANTHROPIC_API_KEY");
      }
      return {
        ...common,
        provider: "anthropic",
        apiKey: config.ANTHROPIC_API_KEY,
        model: config.ANTHROPIC_MODEL,
      };
    case "open-weight":
      if (!config.OPEN_WEIGHT_BASE_URL || !config.OPEN_WEIGHT_MODEL) {
        throw new Error("MODEL_PROVIDER=open-weight requires OPEN_WEIGHT_BASE_URL and OPEN_WEIGHT_MODEL");
      }
      return {
        ...common,
        provider: "openai-compatible",
        baseUrl: config.OPEN_WEIGHT_BASE_URL,
        ...(config.OPEN_WEIGHT_API_KEY ? { apiKey: config.OPEN_WEIGHT_API_KEY } : {}),
        model: config.OPEN_WEIGHT_MODEL,
      };
  }
}
