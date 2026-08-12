import { randomUUID } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import {
  GOOGLE_SCOPES,
  type GoogleCalendarEffect,
  GoogleConnection,
  type GoogleConnectionError,
  type GoogleConnectionStore,
  type GoogleConnectionView,
  type GoogleScope,
  type PendingGoogleConnection,
} from "./index.js";

describe("GoogleConnection", () => {
  it("binds one-use OAuth state to the browser session and keeps credentials opaque", async () => {
    const store = new MemoryStore();
    const requests: { url: string; body: string | null }[] = [];
    const fetch = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = String(input);
      requests.push({ url, body: init?.body?.toString() ?? null });
      if (url.endsWith("/token")) {
        return Response.json({
          access_token: "short-lived-access",
          refresh_token: "never-store-this-plaintext",
          scope: GOOGLE_SCOPES.join(" "),
          token_type: "Bearer",
        });
      }
      if (url.endsWith("/userinfo")) {
        return Response.json({
          sub: "stable-google-subject",
          email: "parent@example.com",
          email_verified: true,
        });
      }
      if (url.endsWith("/revoke")) return new Response(null, { status: 200 });
      return new Response(null, { status: 404 });
    });
    const google = service(store, fetch);
    const sessionBindingDigest = "a".repeat(64);
    const householdId = randomUUID();
    const ownerAdultId = randomUUID();
    const begun = await google.begin({
      householdId,
      ownerAdultId,
      sessionBindingDigest,
      now: "2026-08-12T16:00:00.000Z",
    });
    const authorization = new URL(begun.authorizationUrl);
    expect(authorization.origin + authorization.pathname).toBe(
      "https://accounts.google.com/o/oauth2/v2/auth",
    );
    expect(authorization.searchParams.get("scope")?.split(" ")).toEqual(GOOGLE_SCOPES);
    expect(authorization.searchParams.get("state")).toBeTruthy();
    expect(JSON.stringify(store.rows)).not.toContain(authorization.searchParams.get("state"));
    expect(await google.status({ householdId, ownerAdultId })).toEqual([]);

    await expect(
      google.finish({
        state: authorization.searchParams.get("state") as string,
        code: "auth-code",
        sessionBindingDigest: "b".repeat(64),
        now: "2026-08-12T16:01:00.000Z",
      }),
    ).rejects.toMatchObject({ code: "invalid_state" });
    expect(fetch).not.toHaveBeenCalled();

    const connected = await google.finish({
      state: authorization.searchParams.get("state") as string,
      code: "auth-code",
      sessionBindingDigest,
      now: "2026-08-12T16:01:00.000Z",
    });
    expect(connected).toMatchObject({
      householdId,
      ownerAdultId,
      status: "active",
      emailLabel: "parent@example.com",
      grantedScopes: GOOGLE_SCOPES,
    });
    expect(JSON.stringify(store.rows)).not.toContain("never-store-this-plaintext");
    expect(store.rows[0]?.refreshTokenEnvelope).toMatch(/^g1\./);
    await expect(
      google.finish({
        state: authorization.searchParams.get("state") as string,
        code: "auth-code",
        sessionBindingDigest,
        now: "2026-08-12T16:02:00.000Z",
      }),
    ).rejects.toMatchObject({ code: "invalid_state" });

    const disconnected = await google.disconnect({
      connectionId: connected.connectionId,
      householdId,
      ownerAdultId,
      now: "2026-08-12T16:03:00.000Z",
    });
    expect(disconnected).toMatchObject({
      providerRevocation: "confirmed",
      connection: { status: "disconnected" },
    });
    expect(requests.at(-1)).toEqual({
      url: "https://oauth2.googleapis.com/revoke",
      body: "token=never-store-this-plaintext",
    });
    expect(store.rows[0]?.refreshTokenEnvelope).toBeNull();
    expect(await google.status({ householdId, ownerAdultId })).toEqual([]);
  });

  it("fails closed when the exact grant is not returned", async () => {
    const store = new MemoryStore();
    const google = service(store, async (input) => {
      if (String(input).endsWith("/token")) {
        return Response.json({
          access_token: "access",
          refresh_token: "refresh",
          scope: "openid email",
          token_type: "Bearer",
        });
      }
      throw new Error("identity must not be requested");
    });
    const begun = await google.begin({
      householdId: randomUUID(),
      ownerAdultId: randomUUID(),
      sessionBindingDigest: "c".repeat(64),
      now: "2026-08-12T16:00:00.000Z",
    });
    await expect(
      google.finish({
        state: new URL(begun.authorizationUrl).searchParams.get("state") as string,
        code: "code",
        sessionBindingDigest: "c".repeat(64),
        now: "2026-08-12T16:01:00.000Z",
      }),
    ).rejects.toEqual(expect.objectContaining<Partial<GoogleConnectionError>>({ code: "invalid_grant" }));
    expect(
      await google.status({
        householdId: begun.connection.householdId,
        ownerAdultId: begun.connection.ownerAdultId,
      }),
    ).toEqual([]);
  });

  it("polls a bounded initial window, preserves paginated history, and reads private evidence", async () => {
    const store = new MemoryStore();
    const fetch = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = String(input);
      const body = init?.body?.toString() ?? "";
      if (url.endsWith("/token") && body.includes("grant_type=authorization_code")) {
        return Response.json({
          access_token: "initial-access",
          refresh_token: "private-refresh",
          scope: GOOGLE_SCOPES.join(" "),
          token_type: "Bearer",
        });
      }
      if (url.endsWith("/token")) {
        expect(body).toContain("refresh_token=private-refresh");
        return Response.json({ access_token: "ephemeral-access", token_type: "Bearer" });
      }
      if (url.endsWith("/userinfo")) {
        return Response.json({ sub: "sub-1", email: "parent@example.com", email_verified: true });
      }
      if (url.endsWith("/profile")) return Response.json({ historyId: "100" });
      if (url.includes("/messages?") && !url.includes("/messages/m1")) {
        expect(url).toContain("maxResults=2");
        expect(url).toContain("newer_than%3A90d");
        return Response.json({ messages: [{ id: "m0", threadId: "t0" }] });
      }
      if (url.includes("/history?")) {
        expect(url).toContain("startHistoryId=100");
        return Response.json({
          history: [
            { id: "101", messagesAdded: [{ message: { id: "m1", threadId: "t1" } }] },
            { id: "102", messagesAdded: [{ message: { id: "m2", threadId: "t2" } }] },
          ],
          historyId: "999",
          nextPageToken: "unvisited-page",
        });
      }
      if (url.includes("/messages/m0?format=metadata")) {
        return Response.json({ id: "m0", threadId: "t0", historyId: "90" });
      }
      if (url.includes("/messages/m1?format=metadata")) {
        return Response.json({ id: "m1", threadId: "t1", historyId: "101" });
      }
      if (url.includes("/messages/m2?format=metadata")) {
        return Response.json({ id: "m2", threadId: "t2", historyId: "102" });
      }
      if (url.includes("/messages/m1?format=full")) {
        return Response.json({
          id: "m1",
          threadId: "t1",
          historyId: "101",
          internalDate: "1786550400000",
          snippet: "fallback",
          payload: {
            mimeType: "multipart/alternative",
            headers: [
              { name: "From", value: "School <office@school.test>" },
              { name: "Subject", value: "Field trip" },
            ],
            parts: [
              {
                mimeType: "text/plain",
                body: { data: Buffer.from("The form is due Friday.").toString("base64url") },
              },
            ],
          },
        });
      }
      return new Response(null, { status: 404 });
    });
    const google = service(store, fetch);
    const householdId = randomUUID();
    const ownerAdultId = randomUUID();
    const sessionBindingDigest = "d".repeat(64);
    const begun = await google.begin({
      householdId,
      ownerAdultId,
      sessionBindingDigest,
      now: "2026-08-12T16:00:00.000Z",
    });
    const connection = await google.finish({
      state: new URL(begun.authorizationUrl).searchParams.get("state") as string,
      code: "code",
      sessionBindingDigest,
      now: "2026-08-12T16:01:00.000Z",
    });

    const initial = await google.claimNextGmailSync({
      owner: "gmail-worker",
      now: "2026-08-12T16:02:00.000Z",
      leaseUntil: "2026-08-12T16:03:00.000Z",
      limit: 2,
    });
    if (!initial) throw new Error("missing initial Gmail claim");
    expect(initial).toMatchObject({
      cursor: null,
      changes: [{ messageId: "m0", threadId: "t0", historyId: "90" }],
      nextCursor: "100",
    });
    await google.releaseGmailSync({
      connectionId: connection.connectionId,
      owner: "gmail-worker",
      nextAt: "2026-08-12T16:04:00.000Z",
      cursor: initial.nextCursor,
    });

    const incremental = await google.claimNextGmailSync({
      owner: "gmail-worker",
      now: "2026-08-12T16:04:00.000Z",
      leaseUntil: "2026-08-12T16:05:00.000Z",
      limit: 2,
    });
    expect(incremental?.changes).toHaveLength(2);
    expect(incremental?.nextCursor).toBe("102");

    await expect(
      google.readGmailMessage({
        householdId,
        ownerAdultId,
        connectionId: connection.connectionId,
        messageId: "m1",
        threadId: "t1",
        historyId: "101",
      }),
    ).resolves.toEqual({
      messageId: "m1",
      threadId: "t1",
      historyId: "101",
      from: "School <office@school.test>",
      subject: "Field trip",
      sentAt: "2026-08-12T16:00:00.000Z",
      text: "The form is due Friday.",
    });
    await expect(
      google.readGmailMessage({
        householdId,
        ownerAdultId,
        connectionId: connection.connectionId,
        messageId: "m1",
        threadId: "t1",
        historyId: "100",
      }),
    ).rejects.toMatchObject({ code: "provider_rejected" });
  });

  it("inserts an approved event, rereads it, and returns only canonical proof", async () => {
    let inserted: Record<string, unknown> | null = null;
    const requests: { url: string; method: string }[] = [];
    const fetch = calendarProvider(async (url, init) => {
      requests.push({ url, method: init?.method ?? "GET" });
      if (init?.method === "POST") {
        inserted = JSON.parse(String(init.body)) as Record<string, unknown>;
        return Response.json(inserted, { status: 201 });
      }
      if (!inserted) throw new Error("event was not inserted");
      return Response.json({ ...inserted, status: "confirmed", etag: '"etag-1"' });
    });
    const { google, effect } = await connectedCalendar(fetch);

    const result = await google.executeCalendar(effect);

    expect(result).toMatchObject({ status: "committed" });
    if (result.status !== "committed") throw new Error("event was not committed");
    const eventId = result.providerReceiptId;
    expect(inserted).toMatchObject({ id: eventId });
    expect(result.detail.length).toBeLessThanOrEqual(2_000);
    expect(JSON.parse(result.detail)).toEqual({
      provider: "google-calendar",
      eventId,
      etag: '"etag-1"',
      digest: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    expect(result.detail).not.toContain(effect.payload.title);
    expect(requests).toEqual([
      {
        url: "https://www.googleapis.com/calendar/v3/calendars/primary/events?sendUpdates=none",
        method: "POST",
      },
      {
        url: `https://www.googleapis.com/calendar/v3/calendars/primary/events/${eventId}`,
        method: "GET",
      },
    ]);
  });

  it("reconciles an ambiguous insert and a repeated 409 through the same deterministic event ID", async () => {
    let inserted: Record<string, unknown> | null = null;
    let insertAttempts = 0;
    const fetch = calendarProvider(async (_url, init) => {
      if (init?.method === "POST") {
        insertAttempts += 1;
        const body = JSON.parse(String(init.body)) as Record<string, unknown>;
        if (inserted && body.id !== inserted.id) throw new Error("event ID changed across retries");
        inserted = body;
        if (insertAttempts === 1) throw new DOMException("request timed out", "AbortError");
        return new Response(null, { status: 409 });
      }
      if (!inserted) return new Response(null, { status: 404 });
      return Response.json({ ...inserted, status: "confirmed", etag: '"etag-stable"' });
    });
    const { google, effect } = await connectedCalendar(fetch);

    const first = await google.executeCalendar(effect);
    const second = await google.executeCalendar(effect);

    expect(first).toMatchObject({ status: "committed" });
    expect(second).toMatchObject({ status: "committed" });
    if (first.status !== "committed" || second.status !== "committed") {
      throw new Error("idempotent event was not committed");
    }
    expect(first.providerReceiptId).toBe(second.providerReceiptId);
    expect(inserted).toMatchObject({ id: first.providerReceiptId });
    expect(insertAttempts).toBe(2);
  });

  it("fails closed when the reread event differs from the exact approved draft", async () => {
    let inserted: Record<string, unknown> | null = null;
    const fetch = calendarProvider(async (_url, init) => {
      if (init?.method === "POST") {
        inserted = JSON.parse(String(init.body)) as Record<string, unknown>;
        return Response.json(inserted, { status: 201 });
      }
      if (!inserted) return new Response(null, { status: 404 });
      return Response.json({
        ...inserted,
        summary: "Provider-rewritten title",
        status: "confirmed",
        etag: '"etag-mismatch"',
      });
    });
    const { google, effect } = await connectedCalendar(fetch);

    await expect(google.executeCalendar(effect)).resolves.toMatchObject({
      status: "failed",
      providerReceiptId: null,
      detail: "Google Calendar did not preserve the exact approved event",
    });
  });

  it("throws when an ambiguous insert cannot be reconciled because Google is transiently unavailable", async () => {
    const fetch = calendarProvider(async (_url, init) =>
      init?.method === "POST" ? new Response(null, { status: 503 }) : new Response(null, { status: 503 }),
    );
    const { google, effect } = await connectedCalendar(fetch);

    await expect(google.executeCalendar(effect)).rejects.toThrow(
      "Google Calendar proof read returned HTTP 503",
    );
  });
});

async function connectedCalendar(fetch: typeof globalThis.fetch): Promise<{
  google: GoogleConnection;
  effect: GoogleCalendarEffect;
}> {
  const store = new MemoryStore();
  const google = service(store, fetch);
  const householdId = randomUUID();
  const ownerAdultId = randomUUID();
  const sessionBindingDigest = "f".repeat(64);
  const begun = await google.begin({
    householdId,
    ownerAdultId,
    sessionBindingDigest,
    now: "2026-08-12T16:00:00.000Z",
  });
  const connection = await google.finish({
    state: new URL(begun.authorizationUrl).searchParams.get("state") as string,
    code: "calendar-code",
    sessionBindingDigest,
    now: "2026-08-12T16:01:00.000Z",
  });
  return {
    google,
    effect: {
      id: randomUUID(),
      householdId,
      idempotencyKey: `calendar:${randomUUID()}`,
      kind: "google.calendar.create",
      connectionId: connection.connectionId,
      ownerAdultId,
      actionId: randomUUID(),
      approvalDigest: "a".repeat(64),
      candidateId: randomUUID(),
      candidateVersion: 1,
      candidateDigest: "b".repeat(64),
      payload: {
        title: "School field trip",
        startsAt: "2026-08-20T16:00:00.000Z",
        endsAt: "2026-08-20T17:30:00.000Z",
        timeZone: "America/Los_Angeles",
        location: "Lincoln Elementary",
      },
    },
  };
}

function calendarProvider(
  handleCalendar: (url: string, init?: RequestInit) => Promise<Response>,
): typeof globalThis.fetch {
  return vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
    const url = String(input);
    const body = init?.body?.toString() ?? "";
    if (url.endsWith("/token") && body.includes("grant_type=authorization_code")) {
      return Response.json({
        access_token: "oauth-access",
        refresh_token: "calendar-refresh",
        scope: GOOGLE_SCOPES.join(" "),
        token_type: "Bearer",
      });
    }
    if (url.endsWith("/token")) {
      expect(body).toContain("refresh_token=calendar-refresh");
      return Response.json({ access_token: "calendar-access", token_type: "Bearer" });
    }
    if (url.endsWith("/userinfo")) {
      return Response.json({
        sub: "calendar-subject",
        email: "parent@example.com",
        email_verified: true,
      });
    }
    if (url.includes("/calendar/v3/calendars/primary/events")) {
      return handleCalendar(url, init);
    }
    throw new Error(`Unexpected Google request: ${url}`);
  }) as typeof globalThis.fetch;
}

function service(store: GoogleConnectionStore, fetch: typeof globalThis.fetch): GoogleConnection {
  return new GoogleConnection({
    store,
    clientId: "client-id",
    clientSecret: "client-secret",
    redirectUri: "https://florence.example.test/oauth/google/callback",
    encryptionKey: Buffer.alloc(32, 7),
    fetch,
  });
}

type Row = GoogleConnectionView & {
  stateDigest: string;
  sessionBindingDigest: string | null;
  stateExpiresAt: string;
  stateConsumedAt: string | null;
  googleSubjectDigest: string | null;
  refreshTokenEnvelope: string | null;
  gmailCursor: string | null;
  syncAvailableAt: string;
  syncLeaseOwner: string | null;
};

class MemoryStore implements GoogleConnectionStore {
  readonly rows: Row[] = [];

  async createPending(input: Parameters<GoogleConnectionStore["createPending"]>[0]) {
    const row: Row = {
      connectionId: input.connectionId,
      householdId: input.householdId,
      ownerAdultId: input.ownerAdultId,
      status: "pending",
      emailLabel: null,
      grantedScopes: [],
      lastError: null,
      createdAt: input.now,
      updatedAt: input.now,
      stateDigest: input.stateDigest,
      sessionBindingDigest: input.sessionBindingDigest,
      stateExpiresAt: input.stateExpiresAt,
      stateConsumedAt: null,
      googleSubjectDigest: null,
      refreshTokenEnvelope: null,
      gmailCursor: null,
      syncAvailableAt: input.now,
      syncLeaseOwner: null,
    };
    this.rows.push(row);
    return view(row);
  }

  async consumePendingState(input: Parameters<GoogleConnectionStore["consumePendingState"]>[0]) {
    const row = this.rows.find(
      (candidate) =>
        candidate.status === "pending" &&
        candidate.stateDigest === input.stateDigest &&
        candidate.sessionBindingDigest === input.sessionBindingDigest &&
        candidate.stateConsumedAt === null &&
        candidate.stateExpiresAt >= input.now,
    );
    if (!row) return null;
    row.stateConsumedAt = input.now;
    return {
      connectionId: row.connectionId,
      householdId: row.householdId,
      ownerAdultId: row.ownerAdultId,
      stateDigest: row.stateDigest,
      sessionBindingDigest: row.sessionBindingDigest as string,
    } satisfies PendingGoogleConnection;
  }

  async activate(input: Parameters<GoogleConnectionStore["activate"]>[0]) {
    const row = this.rows.find(
      (candidate) =>
        candidate.connectionId === input.connectionId && candidate.stateDigest === input.stateDigest,
    );
    if (!row?.stateConsumedAt) throw new Error("not current");
    row.status = "active";
    row.sessionBindingDigest = null;
    row.googleSubjectDigest = input.googleSubjectDigest;
    row.emailLabel = input.emailLabel;
    row.grantedScopes = input.grantedScopes;
    row.refreshTokenEnvelope = input.refreshTokenEnvelope;
    row.lastError = null;
    row.updatedAt = input.now;
    return view(row);
  }

  async markPendingFailure(input: Parameters<GoogleConnectionStore["markPendingFailure"]>[0]) {
    const row = this.rows.find((candidate) => candidate.connectionId === input.connectionId);
    if (row) row.lastError = input.error;
  }

  async listActive(input: Parameters<GoogleConnectionStore["listActive"]>[0]) {
    return this.rows
      .filter(
        (row) =>
          row.status === "active" &&
          row.householdId === input.householdId &&
          row.ownerAdultId === input.ownerAdultId,
      )
      .map(view);
  }

  async disconnect(input: Parameters<GoogleConnectionStore["disconnect"]>[0]) {
    const row = this.rows.find(
      (candidate) =>
        candidate.connectionId === input.connectionId &&
        candidate.householdId === input.householdId &&
        candidate.ownerAdultId === input.ownerAdultId &&
        candidate.status !== "disconnected",
    );
    if (!row) return null;
    const refreshTokenEnvelope = row.refreshTokenEnvelope;
    row.status = "disconnected";
    row.refreshTokenEnvelope = null;
    row.updatedAt = input.now;
    return { view: view(row), refreshTokenEnvelope };
  }

  async readActiveGoogleCredential(
    input: Parameters<GoogleConnectionStore["readActiveGoogleCredential"]>[0],
  ) {
    const row = this.rows.find(
      (candidate) =>
        candidate.status === "active" &&
        candidate.connectionId === input.connectionId &&
        candidate.householdId === input.householdId &&
        candidate.ownerAdultId === input.ownerAdultId,
    );
    return row?.refreshTokenEnvelope
      ? {
          connectionId: row.connectionId,
          householdId: row.householdId,
          ownerAdultId: row.ownerAdultId,
          refreshTokenEnvelope: row.refreshTokenEnvelope,
          gmailCursor: row.gmailCursor,
        }
      : null;
  }

  async claimNextGmailSync(input: Parameters<GoogleConnectionStore["claimNextGmailSync"]>[0]) {
    const row = this.rows.find(
      (candidate) =>
        candidate.status === "active" &&
        candidate.syncAvailableAt <= input.now &&
        candidate.syncLeaseOwner === null,
    );
    if (!row?.refreshTokenEnvelope) return null;
    row.syncLeaseOwner = input.owner;
    return {
      connectionId: row.connectionId,
      householdId: row.householdId,
      ownerAdultId: row.ownerAdultId,
      refreshTokenEnvelope: row.refreshTokenEnvelope,
      gmailCursor: row.gmailCursor,
      leaseOwner: input.owner,
    };
  }

  async releaseGmailSync(input: Parameters<GoogleConnectionStore["releaseGmailSync"]>[0]) {
    const row = this.rows.find(
      (candidate) =>
        candidate.connectionId === input.connectionId && candidate.syncLeaseOwner === input.owner,
    );
    if (!row) throw new Error("stale claim");
    if (input.cursor !== undefined) row.gmailCursor = input.cursor;
    row.lastError = input.error ?? null;
    row.syncAvailableAt = input.nextAt;
    row.syncLeaseOwner = null;
  }
}

function view(row: Row): GoogleConnectionView {
  const {
    connectionId,
    householdId,
    ownerAdultId,
    status,
    emailLabel,
    grantedScopes,
    lastError,
    createdAt,
    updatedAt,
  } = row;
  return {
    connectionId,
    householdId,
    ownerAdultId,
    status,
    emailLabel,
    grantedScopes: grantedScopes as readonly GoogleScope[],
    lastError,
    createdAt,
    updatedAt,
  };
}
