import { describe, expect, it, vi } from "vitest";
import {
  type ModelCompletionRequest,
  ModelGatewayError,
  RoutedModelGateway,
} from "../../src/models/index.js";
import { allCapabilities, fakeRoute, modelResult, providerAdapter } from "./fixtures.js";

const request: ModelCompletionRequest = {
  messages: [{ role: "user", parts: [{ type: "text", text: "Classify this." }] }],
};

describe("RoutedModelGateway", () => {
  it("routes by app-owned capability profile and preserves normalized metadata", async () => {
    const complete = vi.fn(async () => modelResult());
    const gateway = new RoutedModelGateway(
      new Map([["classification_extraction", providerAdapter(complete)]]),
    );

    const result = await gateway.complete("classification_extraction", request);

    expect(complete).toHaveBeenCalledOnce();
    expect(result.route).toEqual(fakeRoute);
    expect(result.usage).toEqual({ inputTokens: 2, outputTokens: 1, totalTokens: 3 });
  });

  it("fails closed when a configured route did not pass the capability gate", () => {
    expect(
      () =>
        new RoutedModelGateway(
          new Map([
            [
              "tool_planning",
              providerAdapter(async () => modelResult(), {
                ...allCapabilities,
                toolCalling: false,
              }),
            ],
          ]),
        ),
    ).toThrowError(new ModelGatewayError("unsupported_capability"));

    expect(
      () =>
        new RoutedModelGateway(
          new Map([
            [
              "private_processing",
              providerAdapter(async () => modelResult(), {
                ...allCapabilities,
                vision: false,
              }),
            ],
          ]),
        ),
    ).toThrowError(new ModelGatewayError("unsupported_capability"));
  });

  it("validates structured output again with the application schema", async () => {
    const gateway = new RoutedModelGateway(
      new Map([
        [
          "classification_extraction",
          providerAdapter(async () =>
            modelResult({
              content: [{ type: "structured_result", value: { answer: 42 } }],
            }),
          ),
        ],
      ]),
    );

    await expect(
      gateway.complete("classification_extraction", {
        ...request,
        responseSchema: (await import("zod")).z.object({ answer: (await import("zod")).z.string() }),
      }),
    ).rejects.toMatchObject({ code: "invalid_output" });
  });

  it("rejects adapter output whose route identity was changed", async () => {
    const gateway = new RoutedModelGateway(
      new Map([
        [
          "classification_extraction",
          providerAdapter(async () => modelResult({ route: { ...fakeRoute, model: "unexpected-model" } })),
        ],
      ]),
    );

    await expect(gateway.complete("classification_extraction", request)).rejects.toMatchObject({
      code: "invalid_output",
    });
  });

  it("honors cancellation without invoking an adapter", async () => {
    const complete = vi.fn(async () => modelResult());
    const gateway = new RoutedModelGateway(
      new Map([["classification_extraction", providerAdapter(complete)]]),
    );
    const controller = new AbortController();
    controller.abort();

    await expect(
      gateway.complete("classification_extraction", request, { signal: controller.signal }),
    ).rejects.toMatchObject({ code: "cancelled" });
    expect(complete).not.toHaveBeenCalled();
  });

  it("maps provider failures to stable retry categories without exposing raw details", async () => {
    const gateway = new RoutedModelGateway(
      new Map([
        [
          "classification_extraction",
          providerAdapter(async () => {
            throw { status: 429, message: "sensitive provider request detail" };
          }),
        ],
      ]),
    );

    await expect(gateway.complete("classification_extraction", request)).rejects.toMatchObject({
      code: "rate_limited",
      retryable: true,
      message: "The model provider rate limited the request.",
    });
  });
});
