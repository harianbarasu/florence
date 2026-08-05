import {
  MODEL_CAPABILITY_REQUIREMENTS,
  type ModelCallOptions,
  type ModelCapabilityProfile,
  ModelCapabilityProfileSchema,
  type ModelCompletionRequest,
  type ModelCompletionResult,
  ModelCompletionResultSchema,
  type ModelGateway,
  ModelMessageSchema,
  type ModelProviderAdapter,
  type ModelRouteCapabilities,
  ModelRouteCapabilitiesSchema,
  ModelToolDefinitionSchema,
} from "./contracts.js";
import { asModelGatewayError, ModelGatewayError } from "./errors.js";

export class RoutedModelGateway implements ModelGateway {
  readonly #routes: ReadonlyMap<ModelCapabilityProfile, ModelProviderAdapter>;

  constructor(routes: ReadonlyMap<ModelCapabilityProfile, ModelProviderAdapter>) {
    const validated = new Map<ModelCapabilityProfile, ModelProviderAdapter>();

    for (const [profileCandidate, adapter] of routes) {
      const profile = ModelCapabilityProfileSchema.parse(profileCandidate);
      ModelRouteCapabilitiesSchema.parse(adapter.capabilities);
      assertCapabilities(profile, adapter.capabilities);
      validated.set(profile, adapter);
    }

    this.#routes = validated;
  }

  async complete(
    profile: ModelCapabilityProfile,
    request: ModelCompletionRequest,
    options?: ModelCallOptions,
  ): Promise<ModelCompletionResult> {
    if (options?.signal?.aborted) {
      throw new ModelGatewayError("cancelled");
    }

    const parsedProfile = ModelCapabilityProfileSchema.safeParse(profile);
    if (!parsedProfile.success) {
      throw new ModelGatewayError("unsupported_capability");
    }

    validateRequest(request);
    const adapter = this.#routes.get(parsedProfile.data);
    if (adapter === undefined) {
      throw new ModelGatewayError("unsupported_capability");
    }

    try {
      const candidate = await adapter.complete(request, options);
      const parsed = ModelCompletionResultSchema.safeParse(candidate);
      if (!parsed.success || !sameRoute(parsed.data.route, adapter.route)) {
        throw new ModelGatewayError("invalid_output");
      }

      if (request.responseSchema !== undefined) {
        const structured = parsed.data.content.find((part) => part.type === "structured_result");
        if (structured === undefined || !request.responseSchema.safeParse(structured.value).success) {
          throw new ModelGatewayError("invalid_output");
        }
      }

      return parsed.data;
    } catch (error) {
      throw asModelGatewayError(error, options?.signal);
    }
  }
}

export function supportsCapabilityProfile(
  profile: ModelCapabilityProfile,
  capabilities: ModelRouteCapabilities,
): boolean {
  const requirements = MODEL_CAPABILITY_REQUIREMENTS[profile];
  return Object.entries(requirements).every(
    ([capability, required]) =>
      required !== true || capabilities[capability as keyof ModelRouteCapabilities] === true,
  );
}

function assertCapabilities(profile: ModelCapabilityProfile, capabilities: ModelRouteCapabilities): void {
  if (!supportsCapabilityProfile(profile, capabilities)) {
    throw new ModelGatewayError("unsupported_capability");
  }
}

function validateRequest(request: ModelCompletionRequest): void {
  if (!Array.isArray(request.messages) || request.messages.length === 0) {
    throw new ModelGatewayError("permanent");
  }

  if (!request.messages.every((message) => ModelMessageSchema.safeParse(message).success)) {
    throw new ModelGatewayError("permanent");
  }

  if (request.tools !== undefined) {
    if (!request.tools.every((tool) => ModelToolDefinitionSchema.safeParse(tool).success)) {
      throw new ModelGatewayError("permanent");
    }
    const names = new Set(request.tools.map((tool) => tool.name));
    if (names.size !== request.tools.length) {
      throw new ModelGatewayError("permanent");
    }
  }

  if (
    request.maxOutputTokens !== undefined &&
    (!Number.isSafeInteger(request.maxOutputTokens) || request.maxOutputTokens <= 0)
  ) {
    throw new ModelGatewayError("permanent");
  }

  if (
    request.temperature !== undefined &&
    (!Number.isFinite(request.temperature) || request.temperature < 0 || request.temperature > 2)
  ) {
    throw new ModelGatewayError("permanent");
  }
}

function sameRoute(left: ModelCompletionResult["route"], right: ModelProviderAdapter["route"]): boolean {
  return (
    left.routeId === right.routeId &&
    left.provider === right.provider &&
    left.model === right.model &&
    left.version === right.version
  );
}
