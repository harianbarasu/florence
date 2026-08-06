import { ChatAnthropic } from "@langchain/anthropic";
import type { BaseChatModel } from "@langchain/core/language_models/chat_models";
import { HumanMessage, SystemMessage } from "@langchain/core/messages";
import { ChatOpenAI } from "@langchain/openai";
import type { z } from "zod";
import type { FlorenceConfig } from "../../config.js";
import type { ModelGateway, StructuredModelRequest } from "./contracts.js";

export class LangChainModelGateway implements ModelGateway {
  readonly #model: BaseChatModel;

  public constructor(config: FlorenceConfig) {
    switch (config.model.provider) {
      case "openai":
        this.#model = new ChatOpenAI({
          apiKey: requireValue(config.model.openai.apiKey, "OPENAI_API_KEY"),
          model: config.model.openai.model,
          configuration: { baseURL: config.model.openai.baseUrl },
          temperature: 0,
          maxRetries: 2,
        });
        break;
      case "anthropic":
        this.#model = new ChatAnthropic({
          anthropicApiKey: requireValue(config.model.anthropic.apiKey, "ANTHROPIC_API_KEY"),
          model: config.model.anthropic.model,
          temperature: 0,
          maxRetries: 2,
        });
        break;
      case "open_weight":
        this.#model = new ChatOpenAI({
          apiKey: config.model.openWeight.apiKey ?? "not-required",
          model: requireValue(config.model.openWeight.model, "OPEN_WEIGHT_MODEL"),
          configuration: { baseURL: requireValue(config.model.openWeight.baseUrl, "OPEN_WEIGHT_BASE_URL") },
          temperature: 0,
          maxRetries: 2,
        });
        break;
    }
  }

  public async completeStructured<Schema extends z.ZodType>(
    request: StructuredModelRequest<Schema>,
  ): Promise<z.output<Schema>> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), request.timeoutMs);
    try {
      const structured = this.#model.withStructuredOutput(request.schema, {
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
}

function requireValue(value: string | undefined, name: string): string {
  if (!value) throw new Error(`${name} is required`);
  return value;
}
