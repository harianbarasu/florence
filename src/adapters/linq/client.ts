import { z } from "zod";
import type { LinqConfig } from "./config.js";

export const linqSendTextInputSchema = z
  .object({
    chatId: z.string().min(1),
    text: z.string().min(1).max(10_000),
    idempotencyKey: z.string().min(1).max(255),
    replyTo: z
      .object({
        messageId: z.string().min(1),
        partIndex: z.number().int().nonnegative().optional(),
      })
      .strict()
      .optional(),
  })
  .strict();

export type LinqSendTextInput = z.infer<typeof linqSendTextInputSchema>;

export interface LinqSendReceipt {
  provider: "linq";
  providerMessageId: string;
  chatId: string;
  idempotencyKey: string;
}

export interface LinqOutboundSender {
  sendText(input: LinqSendTextInput): Promise<LinqSendReceipt>;
}

export type LinqFetch = (
  input: string | URL,
  init?: RequestInit,
) => Promise<Pick<Response, "ok" | "status" | "json">>;

export class LinqApiError extends Error {
  override readonly name = "LinqApiError";

  constructor(
    message: string,
    readonly status: number | null,
    readonly retryable: boolean,
  ) {
    super(message);
  }
}

export class LinqClient implements LinqOutboundSender {
  readonly #fetch: LinqFetch;

  constructor(
    private readonly config: Pick<LinqConfig, "apiBaseUrl" | "apiKey" | "requestTimeoutMs">,
    fetchImplementation: LinqFetch = globalThis.fetch,
  ) {
    this.#fetch = fetchImplementation;
  }

  async sendText(rawInput: LinqSendTextInput): Promise<LinqSendReceipt> {
    const input = linqSendTextInputSchema.parse(rawInput);
    const replyTo = input.replyTo
      ? {
          message_id: input.replyTo.messageId,
          ...(input.replyTo.partIndex === undefined ? {} : { part_index: input.replyTo.partIndex }),
        }
      : undefined;
    const body = {
      message: {
        parts: [{ type: "text", value: input.text }],
        idempotency_key: input.idempotencyKey,
        ...(replyTo ? { reply_to: replyTo } : {}),
      },
    };

    let response: Awaited<ReturnType<LinqFetch>>;
    try {
      response = await this.#fetch(
        `${this.config.apiBaseUrl}/chats/${encodeURIComponent(input.chatId)}/messages`,
        {
          method: "POST",
          headers: {
            authorization: `Bearer ${this.config.apiKey}`,
            "content-type": "application/json",
          },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(this.config.requestTimeoutMs),
        },
      );
    } catch (error) {
      throw new LinqApiError(
        error instanceof Error && error.name === "TimeoutError"
          ? "Linq request timed out"
          : "Linq request failed before a response was received",
        null,
        true,
      );
    }

    if (!response.ok) {
      throw new LinqApiError(
        `Linq rejected the send with HTTP ${response.status}`,
        response.status,
        response.status === 408 || response.status === 429 || response.status >= 500,
      );
    }

    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      throw new LinqApiError("Linq returned an invalid send response", response.status, false);
    }
    const providerMessageId = extractMessageId(payload);
    if (providerMessageId === null) {
      throw new LinqApiError("Linq send response did not include a message ID", response.status, false);
    }

    return {
      provider: "linq",
      providerMessageId,
      chatId: input.chatId,
      idempotencyKey: input.idempotencyKey,
    };
  }
}

function extractMessageId(payload: unknown): string | null {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return null;
  }
  const record = payload as Record<string, unknown>;
  const message =
    typeof record.message === "object" && record.message !== null && !Array.isArray(record.message)
      ? (record.message as Record<string, unknown>)
      : null;
  for (const value of [record.id, record.message_id, message?.id]) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}
