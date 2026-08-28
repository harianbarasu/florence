import { createHash } from "node:crypto";
import { describe, expect, test, vi } from "vitest";
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
