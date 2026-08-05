import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
  createModelGateway,
  type ModelGatewayConfig,
  ModelGatewayError,
  type ModelRouteCapabilities,
  normalizeAllowedBaseUrl,
  RoutedModelGateway,
  validateModelGatewayConfig,
} from "../../src/models/index.js";
import { providerJsonSchema } from "../../src/models/provider-adapters.js";
import { allCapabilities } from "./fixtures.js";

function config(
  provider: "openai" | "anthropic" | "openai-compatible",
  capabilities: ModelRouteCapabilities = allCapabilities,
): ModelGatewayConfig {
  const base = {
    routeId: `${provider}-route`,
    profile: "tool_planning" as const,
    model: `${provider}-model`,
    capabilities,
  };
  if (provider === "openai-compatible") {
    return {
      routes: [{ ...base, provider, baseUrl: "https://models.example.test/v1" }],
      openAICompatibleBaseUrlAllowlist: ["https://models.example.test/v1/"],
    };
  }
  return {
    routes: [{ ...base, provider, apiKey: "placeholder" }],
  };
}

describe("model provider configuration", () => {
  it.each(["openai", "anthropic", "openai-compatible"] as const)(
    "builds the %s route without making a live call",
    (provider) => {
      expect(createModelGateway(config(provider))).toBeInstanceOf(RoutedModelGateway);
    },
  );

  it("requires an exact normalized allowlist entry for compatible endpoints", () => {
    const candidate = config("openai-compatible");
    candidate.openAICompatibleBaseUrlAllowlist = ["https://models.example.test/v2"];

    expect(() => validateModelGatewayConfig(candidate)).toThrowError(new ModelGatewayError("permanent"));
  });

  it("allows HTTP only for explicitly allowlisted loopback endpoints", () => {
    expect(normalizeAllowedBaseUrl("http://localhost:11434/v1/")).toBe("http://localhost:11434/v1");
    expect(() => normalizeAllowedBaseUrl("http://models.example.test/v1")).toThrowError(
      new ModelGatewayError("permanent"),
    );
  });

  it("rejects URLs containing credentials, queries, or fragments", () => {
    for (const value of [
      "https://user:pass@models.example.test/v1",
      "https://models.example.test/v1?tenant=a",
      "https://models.example.test/v1#fragment",
    ]) {
      expect(() => normalizeAllowedBaseUrl(value)).toThrowError(new ModelGatewayError("permanent"));
    }
  });

  it("rejects duplicate capability routes and ineligible declared capabilities", () => {
    const duplicate = config("openai");
    const first = duplicate.routes.at(0);
    if (first === undefined) {
      throw new Error("The test fixture must contain one route.");
    }
    duplicate.routes = [first, { ...first, routeId: "second" }];
    expect(() => validateModelGatewayConfig(duplicate)).toThrowError(new ModelGatewayError("permanent"));

    expect(() =>
      validateModelGatewayConfig(config("anthropic", { ...allCapabilities, toolCalling: false })),
    ).toThrowError(new ModelGatewayError("unsupported_capability"));
  });

  it("converts transformed discriminated contracts into provider-safe input JSON Schema", () => {
    const contract = z.strictObject({
      result: z.discriminatedUnion("kind", [
        z.strictObject({ kind: z.literal("ignore"), detail: z.string().optional() }),
        z.strictObject({
          kind: z.literal("scheduled"),
          at: z.string().transform((value) => new Date(value).toISOString()),
        }),
      ]),
    });

    const schema = providerJsonSchema(contract);
    const serialized = JSON.stringify(schema);
    expect(serialized).toContain('"type":"object"');
    expect(serialized).toContain('"anyOf"');
    expect(serialized).not.toContain('"oneOf"');
  });
});
