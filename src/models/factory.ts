import { type ModelGatewayConfig, validateModelGatewayConfig } from "./config.js";
import type { ModelCapabilityProfile, ModelProviderAdapter } from "./contracts.js";
import { RoutedModelGateway } from "./gateway.js";
import { createProviderAdapter } from "./provider-adapters.js";

export function createModelGateway(config: ModelGatewayConfig): RoutedModelGateway {
  const validated = validateModelGatewayConfig(config);
  const routes = new Map<ModelCapabilityProfile, ModelProviderAdapter>();
  for (const route of validated.routes) {
    routes.set(route.profile, createProviderAdapter(route));
  }
  return new RoutedModelGateway(routes);
}
