import { ChatAnthropic } from "@langchain/anthropic";
import type { BaseChatModel } from "@langchain/core/language_models/chat_models";
import { HumanMessage, SystemMessage } from "@langchain/core/messages";
import { ChatOpenAI } from "@langchain/openai";
import type { z } from "zod";
import type { FlorenceConfig } from "../../config.js";
import type { ModelGateway, StructuredModelRequest } from "./contracts.js";

export class LangChainModelGateway implements ModelGateway {
  readonly #config: FlorenceConfig;

  public constructor(config: FlorenceConfig) {
    this.#config = config;
    // Fail during process startup rather than after durable work is claimed.
    switch (config.model.provider) {
      case "openai":
        requireValue(config.model.openai.apiKey, "OPENAI_API_KEY");
        break;
      case "anthropic":
        requireValue(config.model.anthropic.apiKey, "ANTHROPIC_API_KEY");
        break;
      case "open_weight":
        requireValue(config.model.openWeight.model, "OPEN_WEIGHT_MODEL");
        requireValue(config.model.openWeight.baseUrl, "OPEN_WEIGHT_BASE_URL");
        break;
    }
  }

  public async completeStructured<Schema extends z.ZodType>(
    request: StructuredModelRequest<Schema>,
  ): Promise<z.output<Schema>> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), request.timeoutMs);
    try {
      const structured = this.#model(request.maxOutputTokens).withStructuredOutput(request.schema, {
        name: request.schemaName,
        includeRaw: false,
      });
      const output = await structured.invoke(
        [
          new SystemMessage(request.system),
          request.images && request.images.length > 0
            ? new HumanMessage({
                contentBlocks: [
                  { type: "text", text: request.user },
                  ...request.images.map((image) => ({
                    type: "image" as const,
                    mimeType: image.mimeType,
                    data: image.dataBase64,
                  })),
                ],
              })
            : new HumanMessage(request.user),
        ],
        { signal: controller.signal },
      );
      return request.schema.parse(output);
    } finally {
      clearTimeout(timeout);
    }
  }

  #model(maxOutputTokens: number): BaseChatModel {
    if (!Number.isInteger(maxOutputTokens) || maxOutputTokens < 1) {
      throw new Error("maxOutputTokens must be a positive integer");
    }
    const config = this.#config;
    switch (config.model.provider) {
      case "openai":
        return new ChatOpenAI({
          apiKey: requireValue(config.model.openai.apiKey, "OPENAI_API_KEY"),
          model: config.model.openai.model,
          configuration: { baseURL: config.model.openai.baseUrl },
          maxTokens: maxOutputTokens,
          maxRetries: 2,
        });
      case "anthropic":
        return new ChatAnthropic({
          anthropicApiKey: requireValue(config.model.anthropic.apiKey, "ANTHROPIC_API_KEY"),
          model: config.model.anthropic.model,
          temperature: 0,
          maxTokens: maxOutputTokens,
          maxRetries: 2,
        });
      case "open_weight":
        return new ChatOpenAI({
          apiKey: config.model.openWeight.apiKey ?? "not-required",
          model: requireValue(config.model.openWeight.model, "OPEN_WEIGHT_MODEL"),
          configuration: { baseURL: requireValue(config.model.openWeight.baseUrl, "OPEN_WEIGHT_BASE_URL") },
          temperature: 0,
          maxTokens: maxOutputTokens,
          maxRetries: 2,
        });
    }
  }
}

function requireValue(value: string | undefined, name: string): string {
  if (!value) throw new Error(`${name} is required`);
  return value;
}
