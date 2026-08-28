import { createHash } from "node:crypto";
import { describe, expect, test, vi } from "vitest";
import { ProviderTelephonyClient } from "./telephony.js";

const runIdentity = { workId: "work-123", callId: "tool-call-456" } as const;
const operationDigest = createHash("sha256").update(runIdentity.callId).digest("hex");
const twilioAccountSid = `AC${"1".repeat(32)}`;

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("ProviderTelephonyClient provider contracts", () => {
  test("starts a Bland task call with Florence correlation metadata", async () => {
    const fetchMock = vi.fn<typeof fetch>(async () => jsonResponse({ call_id: "bland-call-1" }));
    const client = new ProviderTelephonyClient({
      bland: { apiKey: "bland-secret", baseUrl: "https://bland.test/v1" },
      fetch: fetchMock,
    });

    const result = await client.run({
      ...runIdentity,
      attempt: 1,
      operation: {
        kind: "ai_call_start",
        provider: "bland",
        to: "+1 (415) 555-0123",
        task: "Ask the dentist for Tuesday or Wednesday openings and report what they say.",
        firstSentence: "Hi, I'm calling for the Williams family.",
      },
    });

    expect(result).toMatchObject({
      kind: "accepted",
      provider: "bland",
      providerId: "bland-call-1",
      toPhoneNumberMasked: "••••••••0123",
    });
    const [url, init] = fetchMock.mock.calls[0] ?? [];
    expect(String(url)).toBe("https://bland.test/v1/calls");
    expect(new Headers(init?.headers).get("authorization")).toBe("bland-secret");
    expect(JSON.parse(String(init?.body))).toMatchObject({
      phone_number: "+14155550123",
      model: "enhanced",
      record: true,
      wait_for_greeting: true,
      metadata: {
        florence_work_id: "work-123",
        florence_operation_id: operationDigest,
      },
    });
  });

  test("reconciles an ambiguous Bland create by its Florence metadata without another POST", async () => {
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = new URL(String(input));
      if (init?.method === "POST") throw new TypeError("connection closed after upload");
      if (url.pathname === "/v1/calls") {
        return jsonResponse({ calls: [{ call_id: "bland-call-recovered" }] });
      }
      return jsonResponse({
        call_id: "bland-call-recovered",
        queue_status: "started",
        completed: false,
        metadata: {
          florence_work_id: runIdentity.workId,
          florence_operation_id: operationDigest,
        },
      });
    });
    const client = new ProviderTelephonyClient({
      bland: { apiKey: "bland-secret", baseUrl: "https://bland.test/v1" },
      fetch: fetchMock,
    });
    const operation = {
      kind: "ai_call_start",
      provider: "bland",
      to: "+14155550123",
      task: "Ask for appointment openings.",
    } as const;

    const first = await client.run({ ...runIdentity, attempt: 1, operation });
    const retry = await client.run({ ...runIdentity, attempt: 2, operation });

    expect(first).toMatchObject({
      kind: "progress",
      providerId: "bland-call-recovered",
      providerStatus: "started",
    });
    expect(retry).toMatchObject({
      kind: "progress",
      providerId: "bland-call-recovered",
      providerStatus: "started",
    });
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === "POST")).toHaveLength(1);
    const listUrl = new URL(String(fetchMock.mock.calls[1]?.[0]));
    expect(listUrl.pathname).toBe("/v1/calls");
    expect(listUrl.searchParams.get("to_number")).toBe("+14155550123");
    expect(listUrl.searchParams.get("limit")).toBe("20");
    expect(listUrl.searchParams.get("start_date")).toBeTruthy();
    expect(listUrl.searchParams.get("end_date")).toBeTruthy();
  });

  test("keeps a durable correlation handle when an ambiguous Bland call is not visible yet", async () => {
    const fetchMock = vi.fn<typeof fetch>(async (_input, init) => {
      if (init?.method === "POST") throw new TypeError("connection closed after upload");
      return jsonResponse({ calls: [] });
    });
    const client = new ProviderTelephonyClient({
      bland: { apiKey: "bland-secret", baseUrl: "https://bland.test/v1" },
      fetch: fetchMock,
    });
    const operation = {
      kind: "ai_call_start",
      provider: "bland",
      to: "+14155550123",
      task: "Ask for appointment openings.",
    } as const;

    const first = await client.run({ ...runIdentity, attempt: 1, operation });
    const retry = await client.run({ ...runIdentity, attempt: 2, operation });

    expect(first).toMatchObject({
      kind: "progress",
      providerId: expect.stringMatching(/^pending_bland_[0-9a-f]{64}_/),
      providerStatus: "reconciling",
    });
    expect(first.reason).toContain("not visible yet");
    expect(retry).toMatchObject({ kind: "progress", providerId: first.providerId });
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === "POST")).toHaveLength(1);
  });

  test("resolves a durable pending Bland handle when the matching call becomes visible later", async () => {
    let listReads = 0;
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      const url = new URL(String(input));
      if (init?.method === "POST") throw new TypeError("connection closed after upload");
      if (url.pathname === "/v1/calls") {
        listReads += 1;
        return listReads === 1
          ? jsonResponse({ calls: [] })
          : jsonResponse({ calls: [{ call_id: "bland-call-visible-later" }] });
      }
      return jsonResponse({
        call_id: "bland-call-visible-later",
        queue_status: "started",
        completed: false,
        to: "+14155550123",
        metadata: {
          florence_work_id: runIdentity.workId,
          florence_operation_id: operationDigest,
        },
      });
    });
    const client = new ProviderTelephonyClient({
      bland: { apiKey: "bland-secret", baseUrl: "https://bland.test/v1" },
      fetch: fetchMock,
    });

    const ambiguousCreate = await client.run({
      ...runIdentity,
      attempt: 1,
      operation: {
        kind: "ai_call_start",
        provider: "bland",
        to: "+14155550123",
        task: "Ask for appointment openings.",
      },
    });
    expect(ambiguousCreate).toMatchObject({
      kind: "progress",
      providerId: expect.stringMatching(/^pending_bland_[0-9a-f]{64}_/),
      providerStatus: "reconciling",
    });
    if (!ambiguousCreate.providerId) throw new Error("Bland did not return a pending correlation handle");

    const resolvedStatus = await client.run({
      workId: runIdentity.workId,
      callId: "status-tool-call-789",
      attempt: 1,
      operation: {
        kind: "ai_call_status",
        provider: "bland",
        providerCallId: ambiguousCreate.providerId,
      },
    });

    expect(resolvedStatus).toMatchObject({
      kind: "progress",
      providerId: "bland-call-visible-later",
      providerStatus: "started",
      toPhoneNumberMasked: "••••••••0123",
    });
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === "POST")).toHaveLength(1);
    expect(listReads).toBe(2);
  });

  test("waits for Bland post-call output after its documented completion fields", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(jsonResponse({ queue_status: "Complete", completed: true }))
      .mockResolvedValueOnce(
        jsonResponse({
          queue_status: "Complete",
          completed: true,
          summary: "The dentist booked Tuesday at 3 PM.",
          disposition: "booked",
        }),
      );
    const client = new ProviderTelephonyClient({
      bland: { apiKey: "bland-secret", baseUrl: "https://bland.test/v1" },
      fetch: fetchMock,
    });
    const statusInput = {
      ...runIdentity,
      attempt: 1,
      operation: {
        kind: "ai_call_status",
        provider: "bland",
        providerCallId: "bland-call-1",
      } as const,
    };

    const processing = await client.run(statusInput);
    const ready = await client.run(statusInput);

    expect(processing).toMatchObject({
      kind: "progress",
      providerStatus: "completed",
      summary: null,
    });
    expect(processing.reason).toContain("still preparing");
    expect(ready).toMatchObject({
      kind: "completed",
      providerStatus: "completed",
      summary: "The dentist booked Tuesday at 3 PM.",
      disposition: "booked",
    });
  });

  test("keeps a Bland stop request in progress until the call itself is terminal", async () => {
    const fetchMock = vi.fn<typeof fetch>(async () =>
      jsonResponse({ status: "success", message: "Call ended successfully." }),
    );
    const client = new ProviderTelephonyClient({
      bland: { apiKey: "bland-secret", baseUrl: "https://bland.test/v1" },
      fetch: fetchMock,
    });

    const stopped = await client.run({
      ...runIdentity,
      attempt: 1,
      operation: {
        kind: "ai_call_cancel",
        provider: "bland",
        providerCallId: "bland-call-1",
      },
    });

    expect(stopped).toMatchObject({
      kind: "progress",
      providerId: "bland-call-1",
      providerStatus: "cancel-requested",
    });
  });

  test("maps a Twilio SMS to its form contract and returned SID", async () => {
    const fetchMock = vi.fn<typeof fetch>(async () => jsonResponse({ sid: "SM123", status: "queued" }));
    const client = new ProviderTelephonyClient({
      twilio: {
        accountSid: twilioAccountSid,
        authToken: "twilio-secret",
        phoneNumber: "+13105550100",
        baseUrl: "https://twilio.test/2010-04-01/Accounts",
      },
      fetch: fetchMock,
    });

    const result = await client.run({
      ...runIdentity,
      attempt: 1,
      operation: {
        kind: "sms_send",
        provider: "twilio",
        to: "+13105550199",
        body: "The Tuesday appointment works. Please confirm.",
      },
    });

    expect(result).toMatchObject({
      kind: "accepted",
      provider: "twilio",
      providerId: "SM123",
      providerStatus: "queued",
    });
    const [url, init] = fetchMock.mock.calls[0] ?? [];
    expect(String(url)).toBe(`https://twilio.test/2010-04-01/Accounts/${twilioAccountSid}/Messages.json`);
    expect(new Headers(init?.headers).get("authorization")).toBe(
      `Basic ${Buffer.from(`${twilioAccountSid}:twilio-secret`).toString("base64")}`,
    );
    expect(Object.fromEntries(new URLSearchParams(String(init?.body)))).toEqual({
      To: "+13105550199",
      From: "+13105550100",
      Body: "The Tuesday appointment works. Please confirm.",
    });
  });

  test("recovers an accepted Twilio text after its durable step is reclaimed without another POST", async () => {
    const fetchMock = vi.fn<typeof fetch>(async (input, init) => {
      expect(init?.method).toBe("GET");
      const url = new URL(String(input));
      expect(url.pathname).toBe(`/2010-04-01/Accounts/${twilioAccountSid}/Messages.json`);
      expect(url.searchParams.get("To")).toBe("+13105550199");
      expect(url.searchParams.get("From")).toBe("+13105550100");
      return jsonResponse({
        messages: [
          {
            sid: "SM-recovered-after-reclaim",
            status: "queued",
            direction: "outbound-api",
            from: "+13105550100",
            to: "+13105550199",
            body: "The Tuesday appointment works. Please confirm.",
            num_media: "0",
            date_created: new Date().toUTCString(),
          },
        ],
      });
    });
    const client = new ProviderTelephonyClient({
      twilio: {
        accountSid: twilioAccountSid,
        authToken: "twilio-secret",
        phoneNumber: "+13105550100",
        baseUrl: "https://twilio.test/2010-04-01/Accounts",
      },
      fetch: fetchMock,
    });

    const recovered = await client.run({
      ...runIdentity,
      attempt: 2,
      operation: {
        kind: "sms_send",
        provider: "twilio",
        to: "+13105550199",
        body: "The Tuesday appointment works. Please confirm.",
      },
    });

    expect(recovered).toMatchObject({
      kind: "progress",
      providerId: "SM-recovered-after-reclaim",
      providerStatus: "queued",
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  test("keeps and resolves a Twilio text correlation handle when create confirmation is lost", async () => {
    let listReads = 0;
    const fetchMock = vi.fn<typeof fetch>(async (_input, init) => {
      if (init?.method === "POST") throw new TypeError("connection closed after upload");
      listReads += 1;
      return listReads === 1
        ? jsonResponse({ messages: [] })
        : jsonResponse({
            messages: [
              {
                sid: "SM-visible-later",
                status: "delivered",
                direction: "outbound-api",
                from: "+13105550100",
                to: "+13105550199",
                body: "Please confirm the 3 PM appointment.",
                num_media: "0",
                date_created: new Date().toUTCString(),
              },
            ],
          });
    });
    const client = new ProviderTelephonyClient({
      twilio: {
        accountSid: twilioAccountSid,
        authToken: "twilio-secret",
        phoneNumber: "+13105550100",
        baseUrl: "https://twilio.test/2010-04-01/Accounts",
      },
      fetch: fetchMock,
    });

    const ambiguous = await client.run({
      ...runIdentity,
      attempt: 1,
      operation: {
        kind: "sms_send",
        provider: "twilio",
        to: "+13105550199",
        body: "Please confirm the 3 PM appointment.",
      },
    });
    expect(ambiguous).toMatchObject({
      kind: "progress",
      providerId: expect.stringMatching(/^pending_twilio_sms_/),
      providerStatus: "reconciling",
    });
    if (!ambiguous.providerId) throw new Error("Twilio did not return a pending message handle");

    const resolved = await client.run({
      workId: runIdentity.workId,
      callId: "check-text-789",
      attempt: 1,
      operation: {
        kind: "sms_status",
        provider: "twilio",
        messageSid: ambiguous.providerId,
      },
    });

    expect(resolved).toMatchObject({
      kind: "completed",
      providerId: "SM-visible-later",
      providerStatus: "delivered",
    });
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === "POST")).toHaveLength(1);
  });

  test("returns a durable Twilio text handle when the task is interrupted after dispatch", async () => {
    const controller = new AbortController();
    const fetchMock = vi.fn<typeof fetch>(async (_input, init) => {
      expect(init?.method).toBe("POST");
      controller.abort(new Error("The parent steered the task"));
      throw new DOMException("The operation was aborted", "AbortError");
    });
    const client = new ProviderTelephonyClient({
      twilio: {
        accountSid: twilioAccountSid,
        authToken: "twilio-secret",
        phoneNumber: "+13105550100",
        baseUrl: "https://twilio.test/2010-04-01/Accounts",
      },
      fetch: fetchMock,
    });

    const interrupted = await client.run(
      {
        ...runIdentity,
        attempt: 1,
        operation: {
          kind: "sms_send",
          provider: "twilio",
          to: "+13105550199",
          body: "Please confirm the 3 PM appointment.",
        },
      },
      controller.signal,
    );

    expect(interrupted).toMatchObject({
      kind: "progress",
      providerId: expect.stringMatching(/^pending_twilio_sms_/),
      providerStatus: "reconciling",
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  test("keeps a Twilio call correlation handle when the log cannot prove which call it created", async () => {
    let listReads = 0;
    const fetchMock = vi.fn<typeof fetch>(async (_input, init) => {
      if (init?.method === "POST") throw new TypeError("connection closed after upload");
      listReads += 1;
      return listReads === 1
        ? jsonResponse({ calls: [] })
        : jsonResponse({
            calls: [
              {
                sid: "CA-visible-later",
                status: "ringing",
                direction: "outbound-api",
                from: "+13105550100",
                to: "+13105550199",
                date_created: new Date().toUTCString(),
              },
            ],
          });
    });
    const client = new ProviderTelephonyClient({
      twilio: {
        accountSid: twilioAccountSid,
        authToken: "twilio-secret",
        phoneNumber: "+13105550100",
        baseUrl: "https://twilio.test/2010-04-01/Accounts",
      },
      fetch: fetchMock,
    });

    const ambiguous = await client.run({
      ...runIdentity,
      attempt: 1,
      operation: {
        kind: "call_start",
        provider: "twilio",
        to: "+13105550199",
        message: "This is Florence confirming the 3 PM appointment.",
      },
    });
    expect(ambiguous).toMatchObject({
      kind: "progress",
      providerId: expect.stringMatching(/^pending_twilio_call_/),
      providerStatus: "reconciling",
    });
    if (!ambiguous.providerId) throw new Error("Twilio did not return a pending call handle");

    const checked = await client.run({
      workId: runIdentity.workId,
      callId: "check-call-789",
      attempt: 1,
      operation: {
        kind: "call_status",
        provider: "twilio",
        callSid: ambiguous.providerId,
      },
    });

    expect(checked).toMatchObject({
      kind: "progress",
      providerId: ambiguous.providerId,
      providerStatus: "reconciling",
    });
    expect(checked.reason).toContain("could not prove");
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === "POST")).toHaveLength(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
