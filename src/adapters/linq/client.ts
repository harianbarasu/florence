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

export const linqChatSchema = z
  .object({
    id: z.string().min(1),
    isGroup: z.boolean(),
    displayName: z.string().nullable(),
    service: z.string().nullable(),
    healthStatus: z.enum(["HEALTHY", "AT_RISK", "CRITICAL", "OPTED_OUT"]),
    activeHandles: z.array(z.string().min(1)).min(2),
    selfHandles: z.array(z.string().min(1)).min(1),
    participantHandles: z.array(z.string().min(1)).min(1),
  })
  .strict()
  .superRefine((chat, context) => {
    const active = new Set(chat.activeHandles);
    for (const handle of [...chat.selfHandles, ...chat.participantHandles]) {
      if (!active.has(handle)) {
        context.addIssue({ code: "custom", message: "A classified chat handle must be active" });
      }
    }
    if (new Set(chat.activeHandles).size !== chat.activeHandles.length) {
      context.addIssue({ code: "custom", message: "Chat handles must be unique" });
    }
  });

export type LinqChat = z.infer<typeof linqChatSchema>;

export interface LinqOutboundSender {
  sendText(input: LinqSendTextInput): Promise<LinqSendReceipt>;
}

export interface LinqChatReader {
  getChat(chatId: string): Promise<LinqChat>;
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

export class LinqClient implements LinqOutboundSender, LinqChatReader {
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

  async getChat(chatId: string): Promise<LinqChat> {
    const parsedChatId = z.string().min(1).max(500).parse(chatId);
    let response: Awaited<ReturnType<LinqFetch>>;
    try {
      response = await this.#fetch(`${this.config.apiBaseUrl}/chats/${encodeURIComponent(parsedChatId)}`, {
        method: "GET",
        headers: { authorization: `Bearer ${this.config.apiKey}` },
        signal: AbortSignal.timeout(this.config.requestTimeoutMs),
      });
    } catch (error) {
      throw new LinqApiError(
        error instanceof Error && error.name === "TimeoutError"
          ? "Linq chat lookup timed out"
          : "Linq chat lookup failed before a response was received",
        null,
        true,
      );
    }

    if (!response.ok) {
      throw new LinqApiError(
        `Linq rejected the chat lookup with HTTP ${response.status}`,
        response.status,
        response.status === 408 || response.status === 429 || response.status >= 500,
      );
    }

    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      throw new LinqApiError("Linq returned an invalid chat response", response.status, false);
    }
    return normalizeChat(payload, response.status);
  }
}

function normalizeChat(payload: unknown, status: number): LinqChat {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    throw new LinqApiError("Linq returned an invalid chat response", status, false);
  }
  const record = payload as Record<string, unknown>;
  const rawHandles = Array.isArray(record.handles) ? record.handles : [];
  const handles = rawHandles.flatMap((value) => {
    if (typeof value !== "object" || value === null || Array.isArray(value)) return [];
    const handle = value as Record<string, unknown>;
    if (typeof handle.handle !== "string" || !handle.handle.trim()) return [];
    const state = typeof handle.status === "string" ? handle.status : "active";
    if (state !== "active") return [];
    return [{ value: handle.handle.trim(), isSelf: handle.is_me === true }];
  });
  const activeHandles = [...new Set(handles.map((handle) => handle.value))];
  const selfHandles = [...new Set(handles.filter((handle) => handle.isSelf).map((handle) => handle.value))];
  const participantHandles = [
    ...new Set(handles.filter((handle) => !handle.isSelf).map((handle) => handle.value)),
  ];
  const health =
    typeof record.health_status === "object" &&
    record.health_status !== null &&
    !Array.isArray(record.health_status)
      ? (record.health_status as Record<string, unknown>).status
      : undefined;
  const parsed = linqChatSchema.safeParse({
    id: record.id,
    isGroup: record.is_group,
    displayName: typeof record.display_name === "string" ? record.display_name : null,
    service: typeof record.service === "string" ? record.service : null,
    healthStatus: health,
    activeHandles,
    selfHandles,
    participantHandles,
  });
  if (!parsed.success) {
    throw new LinqApiError("Linq chat response did not match the pinned contract", status, false);
  }
  return parsed.data;
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
