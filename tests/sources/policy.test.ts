import { describe, expect, it } from "vitest";
import {
  assessMailMetadata,
  isFullMailContentAdmitted,
  planNewestFirstMailBackfill,
  projectCalendarArtifact,
  SourceCommandSchema,
} from "../../src/modules/sources/index.js";

describe("source admission policy", () => {
  it("never retrieves spam or trash, but does not discard family mail mislabeled as promotional", () => {
    expect(
      assessMailMetadata({
        labelIds: ["INBOX", "SPAM"],
        from: "teacher@example.test",
        subject: "Field trip permission",
        snippet: "Please sign",
        hasAttachments: true,
      }),
    ).toEqual({
      ingestMetadata: false,
      bodyRetrieval: "never",
      reasons: ["provider_spam_or_trash"],
    });

    expect(
      assessMailMetadata({
        labelIds: ["CATEGORY_PROMOTIONS"],
        from: "school@example.test",
        subject: "Tomorrow's pickup change",
        snippet: "Dismissal is early",
        hasAttachments: false,
      }).bodyRetrieval,
    ).toBe("now");
  });

  it("makes full retrieval and model processing share one metadata-admission boundary", () => {
    const admitted = assessMailMetadata({
      labelIds: ["CATEGORY_PROMOTIONS"],
      from: "coach@example.test",
      subject: "Saturday practice moved",
      snippet: "The game starts at noon",
      hasAttachments: false,
    });
    const deferred = assessMailMetadata({
      labelIds: ["CATEGORY_PROMOTIONS"],
      from: "store@example.test",
      subject: "This weekend's offers",
      snippet: "Save on selected items",
      hasAttachments: false,
    });

    expect(admitted.bodyRetrieval).toBe("now");
    expect(isFullMailContentAdmitted(admitted)).toBe(true);
    expect(deferred.bodyRetrieval).toBe("after_metadata_triage");
    expect(isFullMailContentAdmitted(deferred)).toBe(false);
  });

  it("makes availability-only calendar records structurally incapable of leaking event detail", () => {
    const projected = projectCalendarArtifact(
      {
        remoteEventId: "private-event-id",
        start: "2026-08-06T15:00:00-07:00",
        end: "2026-08-06T16:00:00-07:00",
        status: "confirmed",
        title: "Pediatric specialist",
        description: "Sensitive diagnosis",
        location: "Clinic",
        attendees: [{ email: "child@example.test" }],
      },
      "availability_only",
    );

    expect(projected).toEqual({
      identityDigest: expect.stringMatching(/^[a-f0-9]{64}$/u),
      start: "2026-08-06T15:00:00-07:00",
      end: "2026-08-06T16:00:00-07:00",
      status: "confirmed",
      busy: true,
    });
    expect(JSON.stringify(projected)).not.toContain("Pediatric");
    expect(JSON.stringify(projected)).not.toContain("private-event-id");
  });
});

describe("newest-first backfill planning", () => {
  it("always prioritizes live work, produces contiguous history windows, and gates older history", () => {
    const withoutOlder = planNewestFirstMailBackfill({
      asOf: "2026-08-05T20:00:00.000Z",
      olderHistoryEnabled: false,
    });
    expect(withoutOlder.map((stage) => stage.kind)).toEqual([
      "live",
      "newest_30_days",
      "days_31_to_90",
      "days_91_to_365",
    ]);
    expect(withoutOlder[1]?.afterExclusive).toBe(withoutOlder[2]?.beforeOrEqual);
    expect(withoutOlder[2]?.afterExclusive).toBe(withoutOlder[3]?.beforeOrEqual);

    const withOlder = planNewestFirstMailBackfill({
      asOf: "2026-08-05T20:00:00.000Z",
      olderHistoryEnabled: true,
    });
    expect(withOlder[0]).toMatchObject({ kind: "live", priority: 0, silent: false });
    expect(withOlder.at(-1)).toMatchObject({
      kind: "older_history",
      priority: 40,
      beforeOrEqual: withOlder[3]?.afterExclusive,
      silent: true,
    });
  });
});

describe("source command privacy contracts", () => {
  it("rejects non-local OAuth returns and calendar events without a configured resource digest", () => {
    const personId = "00000000-0000-4000-8000-000000000001";
    const integrationId = "00000000-0000-4000-8000-000000000002";
    const digest = "a".repeat(64);
    expect(
      SourceCommandSchema.safeParse({
        kind: "begin_oauth_attempt",
        personId,
        provider: "google",
        stateDigest: digest,
        pkceVerifier: "v".repeat(43),
        returnPath: "//attacker.example/steal",
        expectedPersonControlEpoch: 1,
        createdAt: "2026-08-05T20:00:00.000Z",
        expiresAt: "2026-08-05T20:10:00.000Z",
      }).success,
    ).toBe(false);

    expect(
      SourceCommandSchema.safeParse({
        kind: "ingest_source",
        integrationId,
        artifactKind: "calendar_event",
        origin: { system: "google.calendar", remoteObjectId: "event-1" },
        scope: { kind: "person", personId },
        content: { identityDigest: digest, start: "now", end: "later", status: "confirmed", busy: true },
        occurredAt: "2026-08-05T20:00:00.000Z",
        capturedAt: "2026-08-05T20:00:00.000Z",
        requestedRetentionUntil: "2026-08-12T20:00:00.000Z",
      }).success,
    ).toBe(false);
  });
});
