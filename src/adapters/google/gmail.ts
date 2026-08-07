import type { OAuth2Client } from "google-auth-library";
import { type gmail_v1, google } from "googleapis";
import { htmlToText } from "html-to-text";
import type { GmailAttachmentReference, NormalizedGmailMessage } from "./contracts.js";

export interface GmailHistoryPage {
  historyId: string;
  messageIds: readonly string[];
  nextPageToken?: string;
}

export interface GmailMessagePage {
  messageIds: readonly string[];
  nextPageToken?: string;
  resultSizeEstimate: number;
}

export class GmailAdapter {
  readonly #gmail: gmail_v1.Gmail;

  public constructor(auth: OAuth2Client) {
    this.#gmail = google.gmail({ version: "v1", auth });
  }

  public async profile(): Promise<{ email: string; historyId: string; messagesTotal: number }> {
    const response = await this.#gmail.users.getProfile({ userId: "me" });
    return {
      email: requireString(response.data.emailAddress, "Gmail profile has no email").toLowerCase(),
      historyId: requireString(response.data.historyId, "Gmail profile has no history ID"),
      messagesTotal: response.data.messagesTotal ?? 0,
    };
  }

  public async history(startHistoryId: string, pageToken?: string): Promise<GmailHistoryPage> {
    const response = await this.#gmail.users.history.list({
      userId: "me",
      startHistoryId,
      historyTypes: ["messageAdded", "messageDeleted", "labelAdded", "labelRemoved"],
      maxResults: 500,
      ...(pageToken ? { pageToken } : {}),
    });
    const messageIds = new Set<string>();
    for (const history of response.data.history ?? []) {
      for (const message of history.messages ?? []) if (message.id) messageIds.add(message.id);
      for (const added of history.messagesAdded ?? [])
        if (added.message?.id) messageIds.add(added.message.id);
      for (const deleted of history.messagesDeleted ?? [])
        if (deleted.message?.id) messageIds.add(deleted.message.id);
      for (const labels of history.labelsAdded ?? [])
        if (labels.message?.id) messageIds.add(labels.message.id);
      for (const labels of history.labelsRemoved ?? [])
        if (labels.message?.id) messageIds.add(labels.message.id);
    }
    return {
      historyId: requireString(response.data.historyId, "Gmail history response has no history ID"),
      messageIds: [...messageIds],
      ...(response.data.nextPageToken ? { nextPageToken: response.data.nextPageToken } : {}),
    };
  }

  public async listMessages(
    query: string,
    pageToken?: string,
    requestedPageSize = 100,
  ): Promise<GmailMessagePage> {
    const maxResults = Number.isFinite(requestedPageSize)
      ? Math.max(1, Math.min(Math.trunc(requestedPageSize), 500))
      : 100;
    const response = await this.#gmail.users.messages.list({
      userId: "me",
      q: query,
      maxResults,
      includeSpamTrash: false,
      ...(pageToken ? { pageToken } : {}),
    });
    return {
      messageIds: (response.data.messages ?? []).flatMap((message) => (message.id ? [message.id] : [])),
      resultSizeEstimate: response.data.resultSizeEstimate ?? 0,
      ...(response.data.nextPageToken ? { nextPageToken: response.data.nextPageToken } : {}),
    };
  }

  public async message(messageId: string, metadataOnly = false): Promise<NormalizedGmailMessage> {
    const response = await this.#gmail.users.messages.get({
      userId: "me",
      id: messageId,
      format: metadataOnly ? "metadata" : "full",
      ...(metadataOnly
        ? { metadataHeaders: ["From", "To", "Cc", "Subject", "Message-ID", "Content-Type"] }
        : {}),
    });
    return normalizeMessage(response.data);
  }

  public async attachment(messageId: string, attachmentId: string): Promise<Buffer> {
    const response = await this.#gmail.users.messages.attachments.get({
      userId: "me",
      messageId,
      id: attachmentId,
    });
    return decodeBase64Url(requireString(response.data.data, "Gmail attachment has no data"));
  }
}

export function normalizeMessage(message: gmail_v1.Schema$Message): NormalizedGmailMessage {
  const id = requireString(message.id, "Gmail message has no ID");
  const payload = message.payload;
  const headers = new Map(
    (payload?.headers ?? []).flatMap((header) =>
      header.name && header.value ? [[header.name.toLowerCase(), header.value] as const] : [],
    ),
  );
  const collected = collectParts(payload, []);
  const plainText = collected.textParts
    .map((part) => decodeBase64Url(part).toString("utf8"))
    .join("\n\n")
    .trim();
  const html = collected.htmlParts
    .map((part) => decodeBase64Url(part).toString("utf8"))
    .join("\n")
    .trim();
  const text =
    plainText ||
    (html ? htmlToText(html, { wordwrap: false, selectors: [{ selector: "img", format: "skip" }] }) : "");
  return {
    id,
    threadId: requireString(message.threadId, "Gmail message has no thread ID"),
    historyId: message.historyId ?? "0",
    internalDate: new Date(Number(message.internalDate ?? Date.now())),
    labelIds: message.labelIds ?? [],
    from: headers.get("from") ?? null,
    to: splitAddresses(headers.get("to")),
    cc: splitAddresses(headers.get("cc")),
    subject: headers.get("subject") ?? null,
    messageIdHeader: headers.get("message-id") ?? null,
    text,
    html: html || null,
    attachments: collected.attachments,
    snippet: message.snippet ?? "",
  };
}

function collectParts(
  part: gmail_v1.Schema$MessagePart | undefined,
  path: readonly string[],
): { textParts: string[]; htmlParts: string[]; attachments: GmailAttachmentReference[] } {
  const result = {
    textParts: [] as string[],
    htmlParts: [] as string[],
    attachments: [] as GmailAttachmentReference[],
  };
  if (!part) return result;
  const partId = (part.partId ?? path.join(".")) || "0";
  if (part.body?.data && part.mimeType === "text/plain") result.textParts.push(part.body.data);
  if (part.body?.data && part.mimeType === "text/html") result.htmlParts.push(part.body.data);
  if (part.body?.attachmentId) {
    const contentId = (part.headers ?? []).find(
      (header) => header.name?.toLowerCase() === "content-id",
    )?.value;
    result.attachments.push({
      attachmentId: part.body.attachmentId,
      partId,
      filename: part.filename || `attachment-${partId}`,
      mimeType: part.mimeType ?? "application/octet-stream",
      size: part.body.size ?? 0,
      inline:
        Boolean(contentId) ||
        (part.headers ?? []).some(
          (header) =>
            header.name?.toLowerCase() === "content-disposition" &&
            header.value?.toLowerCase().includes("inline"),
        ),
      ...(contentId ? { contentId } : {}),
    });
  }
  for (const [index, child] of (part.parts ?? []).entries()) {
    const nested = collectParts(child, [...path, String(index)]);
    result.textParts.push(...nested.textParts);
    result.htmlParts.push(...nested.htmlParts);
    result.attachments.push(...nested.attachments);
  }
  return result;
}

function splitAddresses(value: string | undefined): string[] {
  return (
    value
      ?.split(",")
      .map((entry) => entry.trim())
      .filter(Boolean) ?? []
  );
}

function decodeBase64Url(value: string): Buffer {
  return Buffer.from(value.replace(/-/gu, "+").replace(/_/gu, "/"), "base64");
}

function requireString(value: string | null | undefined, message: string): string {
  if (!value) throw new Error(message);
  return value;
}
