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
}): string {
  const nonce = Buffer.alloc(12, 7);
  const cipher = createCipheriv("aes-256-gcm", input.key, nonce);
  cipher.setAAD(
    Buffer.from(
      `florence-google-refresh-v1\0${input.connectionId}\0${input.householdId}\0${input.ownerAdultId}`,
    ),
  );
  const ciphertext = Buffer.concat([cipher.update("refresh-token", "utf8"), cipher.final()]);
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
