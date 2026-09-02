import { createCipheriv, createHash } from "node:crypto";
import { describe, expect, test, vi } from "vitest";
import { GoogleConnection, type GoogleConnectionStore } from "./index.js";
import { executeGoogleWorkspaceOperation } from "./workspace.js";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function messagePayload(input: {
  messageId: string;
  messageHeaderId: string;
  body: string;
  subject: string;
}): Record<string, unknown> {
  return {
    id: input.messageId,
    threadId: `thread-${input.messageId}`,
    historyId: "123",
    labelIds: ["DRAFT"],
    snippet: input.body.slice(0, 100),
    payload: {
      mimeType: "text/plain",
      headers: [
        { name: "From", value: "Parent <parent@example.com>" },
        { name: "To", value: "school@example.com" },
        { name: "Subject", value: input.subject },
        { name: "Message-ID", value: input.messageHeaderId },
      ],
      body: {
        size: Buffer.byteLength(input.body),
        data: Buffer.from(input.body, "utf8").toString("base64url"),
      },
    },
  };
}

function refreshTokenEnvelope(input: {
  key: Buffer;
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  refreshToken?: string;
}): string {
  const nonce = Buffer.alloc(12, 7);
  const cipher = createCipheriv("aes-256-gcm", input.key, nonce);
  cipher.setAAD(
    Buffer.from(
      `florence-google-refresh-v1\0${input.connectionId}\0${input.householdId}\0${input.ownerAdultId}`,
    ),
  );
  const ciphertext = Buffer.concat([
    cipher.update(input.refreshToken ?? "refresh-token", "utf8"),
    cipher.final(),
  ]);
  return [
    "g1",
    nonce.toString("base64url"),
    ciphertext.toString("base64url"),
    cipher.getAuthTag().toString("base64url"),
  ].join(".");
}

function calendarEvent(index: number): Record<string, unknown> {
  const startsAt = new Date(Date.UTC(2026, 7, 29, index));
  const endsAt = new Date(startsAt.getTime() + 30 * 60_000);
  return {
    id: `event-${index.toString().padStart(2, "0")}`,
    etag: `"revision-${index}"`,
    updated: "2026-08-28T18:00:00.000Z",
    status: "confirmed",
    summary: `Family event ${index}`,
    start: { dateTime: startsAt.toISOString(), timeZone: "America/Los_Angeles" },
    end: { dateTime: endsAt.toISOString(), timeZone: "America/Los_Angeles" },
  };
}

function searchableGmailMessage(index: number): Record<string, unknown> {
  const text = `School update ${index}`;
  return {
    id: `search-message-${index}`,
    threadId: `search-thread-${index}`,
    historyId: String(2_000 + index),
    internalDate: String(Date.UTC(2026, 7, 28, 16, index)),
    labelIds: ["INBOX"],
    snippet: text,
    payload: {
      mimeType: "text/plain",
      headers: [
        { name: "From", value: "School <school@example.com>" },
        { name: "To", value: "Parent <parent@example.com>" },
        { name: "Subject", value: `School note ${index}` },
        { name: "Date", value: `Fri, 28 Aug 2026 16:${String(index).padStart(2, "0")}:00 -0700` },
      ],
      body: {
        size: Buffer.byteLength(text),
        data: Buffer.from(text, "utf8").toString("base64url"),
      },
    },
  };
}

describe("Gmail draft provider journey", () => {
  test("forwards external text, compacts provider bodies, and binds draft identity before reconciliation", async () => {
    const idempotencyKey = "forward-school-note";
    const messageHeaderId = `<florence-${createHash("sha256").update(idempotencyKey).digest("hex")}@actions.florence.invalid>`;
    const wrongMessageHeaderId = `<florence-${"f".repeat(64)}@actions.florence.invalid>`;
    const sourceBody = "The permission slip is attached to the school portal.";
    const largeDraftBody = "Schedule details. ".repeat(25_000);
    const draftMessage = messagePayload({
      messageId: "draft-message-1",
      messageHeaderId,
      body: largeDraftBody,
      subject: "Fwd: Permission slip",
    });
    const calls: string[] = [];
    let createdRaw = "";
    let matchingSentQueryCount = 0;

    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = new URL(String(input));
      const method = init?.method ?? "GET";
      calls.push(`${method} ${url.pathname}${url.search}`);

      if (url.pathname === "/gmail/v1/users/me/messages" && method === "GET") {
        const query = url.searchParams.get("q") ?? "";
        if (query.includes("f".repeat(64))) {
          return jsonResponse({ messages: [{ id: "unrelated-sent", threadId: "thread-unrelated" }] });
        }
        matchingSentQueryCount += 1;
        return jsonResponse(
          matchingSentQueryCount === 1
            ? {}
            : { messages: [{ id: "sent-message-1", threadId: "thread-draft-message-1" }] },
        );
      }
      if (url.pathname === "/gmail/v1/users/me/drafts" && method === "GET") {
        return jsonResponse({});
      }
      if (url.pathname === "/gmail/v1/users/me/messages/source-1" && method === "GET") {
        return jsonResponse({
          id: "source-1",
          threadId: "thread-source-1",
          labelIds: ["INBOX"],
          snippet: sourceBody,
          payload: {
            mimeType: "multipart/alternative",
            headers: [
              { name: "From", value: "Teacher <teacher@example.com>" },
              { name: "To", value: "Parent <parent@example.com>" },
              { name: "Date", value: "Fri, 28 Aug 2026 09:00:00 -0700" },
              { name: "Subject", value: "Permission slip" },
              { name: "Message-ID", value: "<source-1@example.com>" },
            ],
            parts: [
              {
                partId: "0",
                mimeType: "text/plain",
                body: { attachmentId: "external-body-1", size: Buffer.byteLength(sourceBody) },
              },
              {
                partId: "1",
                mimeType: "text/html",
                body: {
                  size: 31,
                  data: Buffer.from("<p>Fallback HTML body</p>").toString("base64url"),
                },
              },
            ],
          },
        });
      }
      if (
        url.pathname === "/gmail/v1/users/me/messages/source-1/attachments/external-body-1" &&
        method === "GET"
      ) {
        return jsonResponse({
          size: Buffer.byteLength(sourceBody),
          data: Buffer.from(sourceBody, "utf8").toString("base64url"),
        });
      }
      if (url.pathname === "/gmail/v1/users/me/drafts" && method === "POST") {
        const request = JSON.parse(String(init?.body)) as { message: { raw: string } };
        createdRaw = request.message.raw;
        return jsonResponse({ id: "draft-1" });
      }
      if (url.pathname === "/gmail/v1/users/me/drafts/draft-1" && method === "GET") {
        return jsonResponse({ id: "draft-1", message: draftMessage });
      }
      throw new Error(`Unexpected provider request: ${method} ${url}`);
    });

    const createResult = await executeGoogleWorkspaceOperation({
      fetch: fetchMock,
      accessToken: "google-access-token",
      operation: {
        operation: "gmail_draft_create",
        mode: "forward",
        messageId: "source-1",
        to: ["school@example.com"],
        cc: [],
        bcc: [],
        body: "Forwarding this for context.",
        bodyFormat: "plain",
        includeSourceAttachments: false,
        attachments: [],
        idempotencyKey,
      },
    });

    const decodedMime = Buffer.from(createdRaw, "base64url").toString("utf8");
    const encodedMimeBody = decodedMime.split("\r\n\r\n")[1]?.replace(/\r\n/g, "") ?? "";
    const forwardedBody = Buffer.from(encodedMimeBody, "base64").toString("utf8");
    expect(forwardedBody).toContain(sourceBody);
    expect(forwardedBody).not.toContain("Fallback HTML body");
    expect(Buffer.byteLength(JSON.stringify(createResult), "utf8")).toBeLessThan(180_000);

    const getResult = await executeGoogleWorkspaceOperation({
      fetch: fetchMock,
      accessToken: "google-access-token",
      operation: { operation: "gmail_draft_get", draftId: "draft-1" },
    });
    expect(Buffer.byteLength(JSON.stringify(getResult), "utf8")).toBeLessThan(180_000);
    expect(JSON.stringify(getResult)).toContain('"bodyTruncated":true');

    const callsBeforeMismatch = calls.length;
    await expect(
      executeGoogleWorkspaceOperation({
        fetch: fetchMock,
        accessToken: "google-access-token",
        operation: {
          operation: "gmail_draft_send",
          draftId: "draft-1",
          messageHeaderId: wrongMessageHeaderId,
        },
      }),
    ).rejects.toMatchObject({ code: "reconciliation_failed" });
    expect(calls.slice(callsBeforeMismatch)).toEqual(["GET /gmail/v1/users/me/drafts/draft-1?format=full"]);

    const callsBeforeReconciliation = calls.length;
    const reconciled = await executeGoogleWorkspaceOperation({
      fetch: fetchMock,
      accessToken: "google-access-token",
      operation: {
        operation: "gmail_draft_send",
        draftId: "draft-1",
        messageHeaderId,
      },
    });
    expect(reconciled.result).toMatchObject({ status: "already_done", draftId: "draft-1" });
    expect(calls.slice(callsBeforeReconciliation, callsBeforeReconciliation + 2)).toEqual([
      "GET /gmail/v1/users/me/drafts/draft-1?format=full",
      expect.stringContaining("GET /gmail/v1/users/me/messages?"),
    ]);
  });
});

describe("Gmail search continuation", () => {
  test("workspace search returns one provider page and validates its opaque continuation", async () => {
    const listRequests: URL[] = [];
    const fetchMock = vi.fn<typeof fetch>(async (input) => {
      const url = new URL(String(input));
      if (url.pathname === "/gmail/v1/users/me/messages") {
        listRequests.push(url);
        const pageToken = url.searchParams.get("pageToken");
        return jsonResponse(
          pageToken === null
            ? {
                messages: [searchableGmailMessage(0), searchableGmailMessage(1)],
                nextPageToken: "provider-page-2",
              }
            : { messages: [searchableGmailMessage(2)] },
        );
      }
      const match = /^\/gmail\/v1\/users\/me\/messages\/search-message-(\d+)$/.exec(url.pathname);
      if (match) return jsonResponse(searchableGmailMessage(Number(match[1])));
      throw new Error(`Unexpected provider request: ${url}`);
    });
    const cursorKey = Buffer.alloc(32, 9);
    const first = await executeGoogleWorkspaceOperation({
      fetch: fetchMock,
      accessToken: "google-access-token",
      cursorKey,
      cursorScope: "connection-1",
      operation: { operation: "gmail_search", query: "school updates", limit: 2 },
    });
    expect(first.result).toMatchObject({ status: "truncated", complete: false });
    expect(
      (first.result.messages as unknown[]).map((message) => (message as { messageId: string }).messageId),
    ).toEqual(["search-message-0", "search-message-1"]);
    const cursor = first.result.nextCursor;
    expect(typeof cursor).toBe("string");
    if (typeof cursor !== "string") throw new Error("Expected a Gmail search continuation cursor");
    expect(cursor).not.toContain("provider-page-2");
    expect(cursor).not.toContain("school updates");

    const second = await executeGoogleWorkspaceOperation({
      fetch: fetchMock,
      accessToken: "another-access-token",
      cursorKey,
      cursorScope: "connection-1",
      operation: { operation: "gmail_search", query: "school updates", limit: 1, cursor },
    });
    expect(second.result).toMatchObject({ status: "complete", complete: true, nextCursor: null });
    expect(
      (second.result.messages as unknown[]).map((message) => (message as { messageId: string }).messageId),
    ).toEqual(["search-message-2"]);
    expect(listRequests.map((url) => url.searchParams.get("pageToken"))).toEqual([null, "provider-page-2"]);
    expect(listRequests.map((url) => url.searchParams.get("maxResults"))).toEqual(["2", "1"]);

    await expect(
      executeGoogleWorkspaceOperation({
        fetch: fetchMock,
        accessToken: "another-access-token",
        cursorKey,
        cursorScope: "connection-1",
        operation: { operation: "gmail_search", query: "another query", limit: 1, cursor },
      }),
    ).rejects.toThrow(/cursor is invalid|another query/i);
    const tamperIndex = Math.floor(cursor.length / 2);
    const tampered = `${cursor.slice(0, tamperIndex)}${cursor[tamperIndex] === "a" ? "b" : "a"}${cursor.slice(tamperIndex + 1)}`;
    await expect(
      executeGoogleWorkspaceOperation({
        fetch: fetchMock,
        accessToken: "another-access-token",
        cursorKey,
        cursorScope: "connection-1",
        operation: { operation: "gmail_search", query: "school updates", limit: 1, cursor: tampered },
      }),
    ).rejects.toThrow(/cursor is invalid/i);
    expect(listRequests).toHaveLength(2);
  });

  test("live Gmail evidence search continues without a Florence total-result cutoff", async () => {
    const connectionId = "connection-search";
    const householdId = "household-search";
    const ownerAdultId = "adult-search";
    const key = Buffer.alloc(32, 4);
    const listRequests: URL[] = [];
    let tokenNumber = 0;
    const fetchMock = vi.fn<typeof fetch>(async (input) => {
      const url = new URL(String(input));
      if (url.origin === "https://oauth2.googleapis.com") {
        tokenNumber += 1;
        return jsonResponse({ access_token: `access-token-${tokenNumber}`, token_type: "Bearer" });
      }
      if (url.pathname === "/gmail/v1/users/me/messages") {
        listRequests.push(url);
        return jsonResponse(
          url.searchParams.get("pageToken") === null
            ? {
                messages: [searchableGmailMessage(0), searchableGmailMessage(1)],
                nextPageToken: "provider-page-2",
              }
            : { messages: [searchableGmailMessage(2)] },
        );
      }
      const match = /^\/gmail\/v1\/users\/me\/messages\/search-message-(\d+)$/.exec(url.pathname);
      if (match) return jsonResponse(searchableGmailMessage(Number(match[1])));
      throw new Error(`Unexpected provider request: ${url}`);
    });
    const store = {
      async readActiveGoogleCredential() {
        return {
          connectionId,
          householdId,
          ownerAdultId,
          refreshTokenEnvelope: refreshTokenEnvelope({ key, connectionId, householdId, ownerAdultId }),
        };
      },
    } as unknown as GoogleConnectionStore;
    const google = new GoogleConnection({
      store,
      clientId: "client-id",
      clientSecret: "client-secret",
      redirectUri: "https://app.tryflorence.com/oauth/google/callback",
      encryptionKey: key,
      fetch: fetchMock,
    });
    const search = (cursor?: string, limit = 2) =>
      google.searchGmail({
        householdId,
        ownerAdultId,
        connectionId,
        query: "school updates",
        limit,
        ...(cursor === undefined ? {} : { cursor }),
      });

    const first = await search();
    expect(first).toMatchObject({ status: "truncated", complete: false });
    expect(first.messages.map((message) => message.messageId)).toEqual([
      "search-message-0",
      "search-message-1",
    ]);
    expect(first.nextCursor).not.toBeNull();
    if (first.nextCursor === null) throw new Error("Expected a live Gmail search continuation cursor");
    expect(first.nextCursor).not.toContain("provider-page-2");

    const second = await search(first.nextCursor, 1);
    expect(second).toMatchObject({ status: "complete", complete: true, nextCursor: null });
    expect(second.messages.map((message) => message.messageId)).toEqual(["search-message-2"]);
    expect(listRequests.map((url) => url.searchParams.get("pageToken"))).toEqual([null, "provider-page-2"]);

    await expect(
      google.searchGmail({
        householdId,
        ownerAdultId,
        connectionId,
        query: "different query",
        limit: 1,
        cursor: first.nextCursor,
      }),
    ).rejects.toThrow(/cursor is invalid|another query/i);
    expect(listRequests).toHaveLength(2);
  });
});

describe("Gmail inbound attachment reads", () => {
  test("reads every message and source identity in an exact thread", async () => {
    const threadId = "thread-school-planning";
    const messages = Array.from({ length: 25 }, (_, index) => ({
      id: `message-${index}`,
      threadId,
      historyId: `message-history-${index}`,
      labelIds: index === 24 ? ["INBOX", "STARRED"] : ["INBOX"],
      snippet: `Planning update ${index}`,
      payload: {
        mimeType: "text/plain",
        headers: [
          { name: "From", value: `Family member ${index} <person-${index}@example.com>` },
          { name: "To", value: "Parent <parent@example.com>" },
          { name: "Subject", value: `School planning ${index}` },
          { name: "Date", value: `Fri, 28 Aug 2026 ${String(index).padStart(2, "0")}:00:00 -0700` },
          { name: "Message-ID", value: `<message-${index}@example.com>` },
        ],
        body: {
          size: Buffer.byteLength(`Thread message ${index}; verification code 123456.`),
          data: Buffer.from(`Thread message ${index}; verification code 123456.`, "utf8").toString(
            "base64url",
          ),
        },
        parts:
          index === 24
            ? [
                {
                  partId: "school-form",
                  filename: "school-form.pdf",
                  mimeType: "application/pdf",
                  body: { attachmentId: "provider-attachment-24", size: 4_096 },
                },
              ]
            : [],
      },
    }));
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = new URL(String(input));
      expect(init?.headers).toMatchObject({ authorization: "Bearer google-access-token" });
      expect(url.pathname).toBe(`/gmail/v1/users/me/threads/${threadId}`);
      expect(url.searchParams.get("format")).toBe("full");
      return jsonResponse({ id: threadId, historyId: "thread-history-25", messages });
    });

    const result = await executeGoogleWorkspaceOperation({
      fetch: fetchMock,
      accessToken: "google-access-token",
      operation: { operation: "gmail_thread_get", threadId },
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(result.operation).toBe("gmail_thread_get");
    expect(result.result.thread).toMatchObject({
      threadId,
      historyId: "thread-history-25",
    });
    const thread = result.result.thread as {
      messages: Array<Record<string, unknown>>;
    };
    expect(thread.messages).toHaveLength(25);
    expect(thread.messages[0]).toMatchObject({
      messageId: "message-0",
      threadId,
      historyId: "message-history-0",
      body: "Thread message 0; verification code 123456.",
    });
    expect(thread.messages[24]).toMatchObject({
      messageId: "message-24",
      threadId,
      historyId: "message-history-24",
      labelIds: ["INBOX", "STARRED"],
      attachments: [
        {
          attachmentId: "provider-attachment-24",
          partId: "school-form",
          filename: "school-form.pdf",
          mimeType: "application/pdf",
          sizeBytes: 4_096,
        },
      ],
    });
  });

  test("returns and reads every supported attachment when a message contains more than twenty", async () => {
    const connectionId = "connection-attachments";
    const householdId = "household-attachments";
    const ownerAdultId = "adult-attachments";
    const messageId = "message-attachments";
    const threadId = "thread-attachments";
    const historyId = "456";
    const key = Buffer.alloc(32, 8);
    const pngSignature = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
    const attachmentBytes = Array.from({ length: 25 }, (_, index) => Buffer.from([...pngSignature, index]));
    const message = {
      id: messageId,
      threadId,
      historyId,
      internalDate: String(Date.UTC(2026, 7, 28, 19)),
      labelIds: ["INBOX"],
      snippet: "Twenty-five photos are attached.",
      payload: {
        mimeType: "multipart/mixed",
        headers: [
          { name: "From", value: "School <school@example.com>" },
          { name: "Subject", value: "Class photos" },
        ],
        parts: attachmentBytes.map((bytes, index) => ({
          partId: `part-${index}`,
          filename: `photo-${index}.png`,
          mimeType: "image/png",
          body: {
            attachmentId: `attachment-${index}`,
            size: bytes.byteLength,
          },
        })),
      },
    };
    const readAttachmentIds: string[] = [];
    const fetchMock = vi.fn<typeof fetch>(async (input) => {
      const url = new URL(String(input));
      if (url.origin === "https://oauth2.googleapis.com") {
        return jsonResponse({ access_token: "access-token", token_type: "Bearer" });
      }
      if (
        url.origin === "https://gmail.googleapis.com" &&
        url.pathname === `/gmail/v1/users/me/messages/${messageId}`
      ) {
        return jsonResponse(message);
      }
      const attachmentPrefix = `/gmail/v1/users/me/messages/${messageId}/attachments/attachment-`;
      if (url.origin === "https://gmail.googleapis.com" && url.pathname.startsWith(attachmentPrefix)) {
        const index = Number(url.pathname.slice(attachmentPrefix.length));
        const bytes = attachmentBytes[index];
        if (!bytes) throw new Error(`Unexpected attachment index: ${index}`);
        readAttachmentIds.push(`attachment-${index}`);
        return jsonResponse({ size: bytes.byteLength, data: bytes.toString("base64url") });
      }
      throw new Error(`Unexpected provider request: ${url}`);
    });
    const store = {
      async readActiveGoogleCredential() {
        return {
          connectionId,
          householdId,
          ownerAdultId,
          refreshTokenEnvelope: refreshTokenEnvelope({ key, connectionId, householdId, ownerAdultId }),
        };
      },
    } as unknown as GoogleConnectionStore;
    const google = new GoogleConnection({
      store,
      clientId: "client-id",
      clientSecret: "client-secret",
      redirectUri: "https://app.tryflorence.com/oauth/google/callback",
      encryptionKey: key,
      fetch: fetchMock,
    });

    const evidence = await google.readGmailMessage({
      householdId,
      ownerAdultId,
      connectionId,
      messageId,
      threadId,
      historyId,
    });

    expect(evidence.attachmentsStatus).toBe("complete");
    expect(evidence.attachments.map((attachment) => attachment.attachmentId)).toEqual(
      attachmentBytes.map((_, index) => `attachment-${index}`),
    );
    const reads = await Promise.all(
      evidence.attachments.map((attachment) =>
        google.readGmailAttachment({ householdId, ownerAdultId, connectionId, attachment }),
      ),
    );
    expect(reads.map((read) => Buffer.from(read.bytes).toString("hex"))).toEqual(
      attachmentBytes.map((bytes) => bytes.toString("hex")),
    );
    expect(readAttachmentIds.sort()).toEqual(attachmentBytes.map((_, index) => `attachment-${index}`).sort());
  });
});

describe("Family Calendar solo expansion", () => {
  test("creates for one parent, shares that calendar in place later, and makes the retry read-only", async () => {
    const householdId = "household-calendar";
    const founderAdultId = "adult-founder";
    const founderConnectionId = "connection-founder";
    const partnerAdultId = "adult-partner";
    const partnerConnectionId = "connection-partner";
    const founderEmail = "founder@example.com";
    const partnerEmail = "partner@example.com";
    const calendarId = "florence-family-calendar";
    const summary = "De la Cruz Family";
    const timeZone = "America/Los_Angeles";
    const key = Buffer.alloc(32, 9);
    const tokenRefreshes: string[] = [];
    const calendarWrites: string[] = [];
    const partnerProviderRequests: string[] = [];
    let calendarDescription: string | null = null;
    let partnerOwnerInstalled = false;
    let partnerCalendarListInstalled = false;
    let creationLatchCalls = 0;

    const connectionView = (input: { connectionId: string; ownerAdultId: string; emailLabel: string }) => ({
      ...input,
      householdId,
      status: "active" as const,
      grantedScopes: [],
      lastError: null,
      createdAt: "2026-08-31T12:00:00.000Z",
      updatedAt: "2026-08-31T12:00:00.000Z",
    });
    const founderConnection = connectionView({
      connectionId: founderConnectionId,
      ownerAdultId: founderAdultId,
      emailLabel: founderEmail,
    });
    const partnerConnection = connectionView({
      connectionId: partnerConnectionId,
      ownerAdultId: partnerAdultId,
      emailLabel: partnerEmail,
    });
    const credential = (input: { connectionId: string; ownerAdultId: string; refreshToken: string }) => ({
      connectionId: input.connectionId,
      householdId,
      ownerAdultId: input.ownerAdultId,
      refreshTokenEnvelope: refreshTokenEnvelope({ key, householdId, ...input }),
    });
    const founderCredential = credential({
      connectionId: founderConnectionId,
      ownerAdultId: founderAdultId,
      refreshToken: "founder-refresh-token",
    });
    const partnerCredential = credential({
      connectionId: partnerConnectionId,
      ownerAdultId: partnerAdultId,
      refreshToken: "partner-refresh-token",
    });

    const store = {
      async readActiveGoogleCredential(input: {
        connectionId: string;
        householdId: string;
        ownerAdultId: string;
      }) {
        if (input.householdId !== householdId) return null;
        if (input.connectionId === founderConnectionId && input.ownerAdultId === founderAdultId) {
          return founderCredential;
        }
        if (input.connectionId === partnerConnectionId && input.ownerAdultId === partnerAdultId) {
          return partnerCredential;
        }
        return null;
      },
      async listActive(input: { householdId: string; ownerAdultId: string }) {
        if (input.householdId !== householdId) return [];
        if (input.ownerAdultId === founderAdultId) return [founderConnection];
        if (input.ownerAdultId === partnerAdultId) return [partnerConnection];
        return [];
      },
      async beginFamilyCalendarCreation() {
        creationLatchCalls += 1;
        return { createAllowed: true, calendarId: null };
      },
    } as unknown as GoogleConnectionStore;
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = new URL(String(input));
      const method = init?.method ?? "GET";
      if (url.origin === "https://oauth2.googleapis.com") {
        const refreshToken = new URLSearchParams(String(init?.body)).get("refresh_token");
        if (!refreshToken) throw new Error("Expected a Google refresh token");
        tokenRefreshes.push(refreshToken);
        return jsonResponse({
          access_token:
            refreshToken === "founder-refresh-token" ? "founder-access-token" : "partner-access-token",
          token_type: "Bearer",
        });
      }
      if (url.origin !== "https://www.googleapis.com") {
        throw new Error(`Unexpected provider request: ${method} ${url}`);
      }

      const authorization = new Headers(init?.headers).get("authorization");
      if (authorization === "Bearer partner-access-token") {
        partnerProviderRequests.push(`${method} ${url.pathname}`);
      }
      if (method !== "GET") calendarWrites.push(`${method} ${url.pathname}`);

      if (url.pathname === "/calendar/v3/users/me/calendarList" && method === "GET") {
        return jsonResponse({ items: [] });
      }
      if (url.pathname === "/calendar/v3/calendars" && method === "POST") {
        const body = JSON.parse(String(init?.body)) as {
          summary: string;
          description: string;
          timeZone: string;
        };
        expect(body).toMatchObject({ summary, timeZone });
        calendarDescription = body.description;
        return jsonResponse({ id: calendarId });
      }
      if (url.pathname === `/calendar/v3/calendars/${calendarId}` && method === "GET") {
        return jsonResponse({ id: calendarId, summary, description: calendarDescription, timeZone });
      }
      if (url.pathname === `/calendar/v3/calendars/${calendarId}/acl` && method === "GET") {
        return jsonResponse({
          items: [
            {
              id: "founder-owner-rule",
              role: "owner",
              scope: { type: "user", value: founderEmail },
            },
            ...(partnerOwnerInstalled
              ? [
                  {
                    id: "partner-owner-rule",
                    role: "owner",
                    scope: { type: "user", value: partnerEmail },
                  },
                ]
              : []),
          ],
        });
      }
      if (url.pathname === `/calendar/v3/calendars/${calendarId}/acl` && method === "POST") {
        expect(JSON.parse(String(init?.body))).toEqual({
          role: "owner",
          scope: { type: "user", value: partnerEmail },
        });
        partnerOwnerInstalled = true;
        return jsonResponse({
          id: "partner-owner-rule",
          role: "owner",
          scope: { type: "user", value: partnerEmail },
        });
      }
      if (url.pathname === `/calendar/v3/users/me/calendarList/${calendarId}` && method === "GET") {
        return partnerCalendarListInstalled
          ? jsonResponse({
              id: calendarId,
              summary,
              timeZone,
              accessRole: "owner",
              selected: true,
              primary: false,
            })
          : new Response(null, { status: 404 });
      }
      if (url.pathname === "/calendar/v3/users/me/calendarList" && method === "POST") {
        expect(JSON.parse(String(init?.body))).toEqual({ id: calendarId, selected: true });
        partnerCalendarListInstalled = true;
        return jsonResponse({
          id: calendarId,
          summary,
          timeZone,
          accessRole: "owner",
          selected: true,
          primary: false,
        });
      }
      throw new Error(`Unexpected provider request: ${method} ${url}`);
    });
    const google = new GoogleConnection({
      store,
      clientId: "client-id",
      clientSecret: "client-secret",
      redirectUri: "https://app.tryflorence.com/oauth/google/callback",
      encryptionKey: key,
      fetch: fetchMock,
    });
    const baseInput = {
      householdId,
      founderAdultId,
      founderConnectionId,
      summary,
      timeZone,
    } as const;

    const solo = await google.provisionFamilyCalendar({ ...baseInput, partner: null });
    expect(solo).toMatchObject({ calendarId, founderConnectionId, partnerConnectionId: null });
    expect(tokenRefreshes).toEqual(["founder-refresh-token"]);
    expect(calendarWrites).toEqual(["POST /calendar/v3/calendars"]);
    expect(partnerProviderRequests).toEqual([]);
    expect(creationLatchCalls).toBe(1);

    const shared = await google.provisionFamilyCalendar({
      ...baseInput,
      calendarId: solo.calendarId,
      partner: { adultId: partnerAdultId, connectionId: partnerConnectionId },
    });
    expect(shared).toMatchObject({ calendarId, founderConnectionId, partnerConnectionId });
    expect(calendarWrites).toEqual([
      "POST /calendar/v3/calendars",
      `POST /calendar/v3/calendars/${calendarId}/acl`,
      "POST /calendar/v3/users/me/calendarList",
    ]);
    expect(partnerProviderRequests).toEqual([
      `GET /calendar/v3/users/me/calendarList/${calendarId}`,
      "POST /calendar/v3/users/me/calendarList",
      `GET /calendar/v3/users/me/calendarList/${calendarId}`,
    ]);
    expect(creationLatchCalls).toBe(1);

    const writesAfterSharing = [...calendarWrites];
    const retried = await google.provisionFamilyCalendar({
      ...baseInput,
      calendarId: solo.calendarId,
      partner: { adultId: partnerAdultId, connectionId: partnerConnectionId },
    });
    expect(retried).toMatchObject({ calendarId, founderConnectionId, partnerConnectionId });
    expect(calendarWrites).toEqual(writesAfterSharing);
    expect(creationLatchCalls).toBe(1);
    expect(tokenRefreshes.filter((token) => token === "partner-refresh-token")).toHaveLength(2);
  });
});

describe("initial Family Calendar review", () => {
  test("exhausts every provider page inside the fixed review window", async () => {
    const connectionId = "connection-1";
    const householdId = "household-1";
    const ownerAdultId = "adult-1";
    const key = Buffer.alloc(32, 3);
    const calendarRequests: URL[] = [];
    const fetchMock = vi.fn<typeof fetch>(async (input) => {
      const url = new URL(String(input));
      if (url.origin === "https://oauth2.googleapis.com") {
        return jsonResponse({ access_token: "access-token", token_type: "Bearer" });
      }
      if (url.origin === "https://www.googleapis.com") {
        calendarRequests.push(url);
        const pageToken = url.searchParams.get("pageToken");
        return jsonResponse({
          timeZone: "America/Los_Angeles",
          items:
            pageToken === null
              ? Array.from({ length: 50 }, (_, index) => calendarEvent(index))
              : [calendarEvent(50)],
          ...(pageToken === null ? { nextPageToken: "page-2" } : {}),
        });
      }
      throw new Error(`Unexpected provider request: ${url}`);
    });
    const store = {
      async readActiveGoogleCredential() {
        return {
          connectionId,
          householdId,
          ownerAdultId,
          refreshTokenEnvelope: refreshTokenEnvelope({ key, connectionId, householdId, ownerAdultId }),
        };
      },
    } as unknown as GoogleConnectionStore;
    const google = new GoogleConnection({
      store,
      clientId: "client-id",
      clientSecret: "client-secret",
      redirectUri: "https://app.tryflorence.com/oauth/google/callback",
      encryptionKey: key,
      fetch: fetchMock,
    });

    const result = await google.readInitialCalendarReview({
      householdId,
      ownerAdultId,
      connectionId,
      calendarId: "family-calendar",
      currentTime: "2026-08-28T12:00:00.000-07:00",
      limit: 50,
    });

    expect(result.status).toBe("complete");
    expect(result.events.map((event) => event.providerEventId)).toEqual(
      Array.from({ length: 51 }, (_, index) => `event-${index.toString().padStart(2, "0")}`),
    );
    expect(calendarRequests).toHaveLength(2);
    expect(calendarRequests.map((url) => url.searchParams.get("pageToken"))).toEqual([null, "page-2"]);
    expect(new Set(calendarRequests.map((url) => url.searchParams.get("timeMin"))).size).toBe(1);
    expect(new Set(calendarRequests.map((url) => url.searchParams.get("timeMax"))).size).toBe(1);

    calendarRequests.length = 0;
    const bounded = await google.readCalendarWindow({
      householdId,
      ownerAdultId,
      connectionId,
      calendarId: "family-calendar",
      timeMin: "2026-08-28T19:00:00.000Z",
      timeMax: "2026-09-18T19:00:00.000Z",
      limit: 50,
    });
    expect(bounded).toMatchObject({ status: "truncated", events: { length: 50 } });
    expect(calendarRequests).toHaveLength(1);
  });
});

describe("personal Calendar window continuation", () => {
  test("makes every exhaustively observed event reachable from both account and exact reads", async () => {
    const connectionId = "connection-1";
    const householdId = "household-1";
    const ownerAdultId = "adult-1";
    const calendarId = "personal-calendar";
    const key = Buffer.alloc(32, 4);
    const providerEventRequests: URL[] = [];
    const fetchMock = vi.fn<typeof fetch>(async (input) => {
      const url = new URL(String(input));
      if (url.origin === "https://oauth2.googleapis.com") {
        return jsonResponse({ access_token: "access-token", token_type: "Bearer" });
      }
      if (url.origin !== "https://www.googleapis.com") {
        throw new Error(`Unexpected provider request: ${url}`);
      }
      const target = {
        id: calendarId,
        summary: "Personal calendar",
        timeZone: "America/Los_Angeles",
        accessRole: "owner",
        primary: true,
      };
      if (url.pathname === `/calendar/v3/users/me/calendarList/${calendarId}`) {
        return jsonResponse(target);
      }
      if (url.pathname === "/calendar/v3/users/me/calendarList") {
        return jsonResponse({
          items: [
            target,
            {
              id: "availability-only-calendar",
              summary: "Availability only",
              timeZone: "America/Los_Angeles",
              accessRole: "freeBusyReader",
              primary: false,
            },
          ],
        });
      }
      if (url.pathname === `/calendar/v3/calendars/${calendarId}/events`) {
        providerEventRequests.push(url);
        const secondPage = url.searchParams.get("pageToken") === "event-page-2";
        return jsonResponse({
          timeZone: "America/Los_Angeles",
          items: secondPage
            ? Array.from({ length: 23 }, (_, index) => calendarEvent(index + 50))
            : Array.from({ length: 50 }, (_, index) => calendarEvent(index)),
          ...(secondPage ? {} : { nextPageToken: "event-page-2" }),
        });
      }
      throw new Error(`Unexpected provider request: ${url}`);
    });
    const store = {
      async readActiveGoogleCredential() {
        return {
          connectionId,
          householdId,
          ownerAdultId,
          refreshTokenEnvelope: refreshTokenEnvelope({ key, connectionId, householdId, ownerAdultId }),
        };
      },
    } as unknown as GoogleConnectionStore;
    const google = new GoogleConnection({
      store,
      clientId: "client-id",
      clientSecret: "client-secret",
      redirectUri: "https://app.tryflorence.com/oauth/google/callback",
      encryptionKey: key,
      fetch: fetchMock,
    });
    const expectedIds = Array.from(
      { length: 73 },
      (_, index) => `event-${index.toString().padStart(2, "0")}`,
    );
    const timeMin = "2026-08-28T00:00:00.000Z";
    const timeMax = "2026-09-02T00:00:00.000Z";

    for (const readWindow of [
      (cursor?: string) =>
        google.readPersonalCalendarWindow({
          householdId,
          ownerAdultId,
          connectionId,
          excludedFamilyCalendarId: null,
          timeMin,
          timeMax,
          limit: 20,
          ...(cursor === undefined ? {} : { cursor }),
        }),
      (cursor?: string) =>
        google.readExactCalendarWindow({
          householdId,
          ownerAdultId,
          connectionId,
          calendarId,
          timeMin,
          timeMax,
          limit: 20,
          ...(cursor === undefined ? {} : { cursor }),
        }),
    ]) {
      const observedIds: string[] = [];
      let cursor: string | undefined;
      let pageCount = 0;
      do {
        const page = await readWindow(cursor);
        pageCount += 1;
        expect(page.totalEventCount).toBe(73);
        observedIds.push(...page.events.map((event) => event.providerEventId));
        if (page.nextCursor) {
          expect(page.nextCursor).not.toContain(calendarId);
          expect(page.nextCursor).not.toContain("event-");
        }
        cursor = page.nextCursor ?? undefined;
      } while (cursor !== undefined);

      expect(pageCount).toBe(4);
      expect(observedIds).toEqual(expectedIds);
      expect(new Set(observedIds).size).toBe(73);
    }
    expect(providerEventRequests).toHaveLength(16);
    expect(providerEventRequests.every((url) => url.searchParams.get("maxResults") === "50")).toBe(true);
  });

  test("rejects stale, query-mismatched, and tampered continuation cursors", async () => {
    const connectionId = "connection-2";
    const householdId = "household-2";
    const ownerAdultId = "adult-2";
    const calendarId = "changing-calendar";
    const key = Buffer.alloc(32, 5);
    let changedRevision = false;
    const fetchMock = vi.fn<typeof fetch>(async (input) => {
      const url = new URL(String(input));
      if (url.origin === "https://oauth2.googleapis.com") {
        return jsonResponse({ access_token: "access-token", token_type: "Bearer" });
      }
      if (url.pathname === "/calendar/v3/users/me/calendarList") {
        return jsonResponse({
          items: [
            {
              id: calendarId,
              summary: "Changing calendar",
              timeZone: "America/Los_Angeles",
              accessRole: "owner",
              primary: true,
            },
          ],
        });
      }
      if (url.pathname === `/calendar/v3/calendars/${calendarId}/events`) {
        const events = Array.from({ length: 12 }, (_, index) => calendarEvent(index));
        if (changedRevision) events[0] = { ...events[0], etag: '"changed-revision"' };
        return jsonResponse({ timeZone: "America/Los_Angeles", items: events });
      }
      throw new Error(`Unexpected provider request: ${url}`);
    });
    const store = {
      async readActiveGoogleCredential() {
        return {
          connectionId,
          householdId,
          ownerAdultId,
          refreshTokenEnvelope: refreshTokenEnvelope({ key, connectionId, householdId, ownerAdultId }),
        };
      },
    } as unknown as GoogleConnectionStore;
    const google = new GoogleConnection({
      store,
      clientId: "client-id",
      clientSecret: "client-secret",
      redirectUri: "https://app.tryflorence.com/oauth/google/callback",
      encryptionKey: key,
      fetch: fetchMock,
    });
    const baseInput = {
      householdId,
      ownerAdultId,
      connectionId,
      excludedFamilyCalendarId: null,
      timeMin: "2026-08-28T00:00:00.000Z",
      timeMax: "2026-09-02T00:00:00.000Z",
      limit: 5,
    } as const;
    const first = await google.readPersonalCalendarWindow(baseInput);
    const cursor = first.nextCursor;
    expect(cursor).not.toBeNull();
    if (cursor === null) throw new Error("Expected a Personal Calendar continuation cursor");

    changedRevision = true;
    await expect(google.readPersonalCalendarWindow({ ...baseInput, cursor })).rejects.toThrow(
      /cursor is stale/i,
    );

    changedRevision = false;
    await expect(
      google.readPersonalCalendarWindow({
        ...baseInput,
        calendarIds: [calendarId],
        cursor,
      }),
    ).rejects.toThrow(/invalid|another read/i);

    const tamperIndex = Math.floor((cursor?.length ?? 0) / 2);
    const tampered = `${cursor?.slice(0, tamperIndex)}${cursor?.[tamperIndex] === "a" ? "b" : "a"}${cursor?.slice(
      tamperIndex + 1,
    )}`;
    await expect(google.readPersonalCalendarWindow({ ...baseInput, cursor: tampered })).rejects.toThrow(
      /cursor is invalid/i,
    );
  });
});
