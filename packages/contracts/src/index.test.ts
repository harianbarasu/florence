import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  conversationMessageSignalSchema,
  familyMemberUpsertedSignalSchema,
  gmailMessageChangedSignalSchema,
  householdProfileSchema,
  householdSignalSchema,
  householdSnapshotSchema,
  workerProposalSchema,
  workerSignalSchema,
} from "./index.js";

const now = "2026-08-11T12:00:00.000Z";
const envelope = (householdId = randomUUID()) => ({
  signalId: randomUUID(),
  householdId,
  occurredAt: now,
  idempotencyKey: `test:${randomUUID()}`,
});
const adult = (id = randomUUID()) => ({
  id,
  kind: "adult" as const,
  role: "steward" as const,
  displayName: "Jackson",
  relationship: "self",
});
const binding = (adultId: string) => ({
  conversationId: randomUUID(),
  audience: "private" as const,
  authorityVersion: 1,
  participantSetDigest: "a".repeat(64),
  authorizedAdultIds: [adultId],
  providerConversationId: "imessage:chat:42",
});

describe("Florence contracts", () => {
  it("accepts image-only messages but rejects empty messages", () => {
    const message = {
      ...envelope(),
      type: "conversation.message" as const,
      conversationId: randomUUID(),
      audience: "private" as const,
      authorityVersion: 1,
      participantSetDigest: "a".repeat(64),
      senderAdultId: randomUUID(),
      text: null,
      images: [{ assetId: randomUUID(), mimeType: "image/jpeg" as const }],
      replyToSignalId: null,
      source: {
        system: "linq-v3" as const,
        providerEventId: "event-42",
        providerMessageId: "message-42",
      },
    };

    expect(conversationMessageSignalSchema.safeParse(message).success).toBe(true);
    expect(conversationMessageSignalSchema.safeParse({ ...message, images: [] }).success).toBe(false);
  });

  it("protects household and conversation authority in worker snapshots", () => {
    const adultId = randomUUID();
    const result = householdSnapshotSchema.safeParse({
      householdId: randomUUID(),
      timeZone: "America/Los_Angeles",
      asOf: now,
      members: [
        { ...adult(adultId), status: "verified", sourceSignalIds: [randomUUID()] },
        {
          ...adult(adultId),
          displayName: "Alex",
          status: "verified",
          sourceSignalIds: [randomUUID()],
        },
      ],
      conversation: { ...binding(randomUUID()), id: randomUUID(), recentTurns: [] },
      memories: [],
      openEpisodes: [],
    });

    expect(result.success).toBe(false);
  });

  it("rejects legacy worker commands", () => {
    expect(
      workerProposalSchema.safeParse({
        type: "accept_ownership",
        episodeId: randomUUID(),
        sourceSignalIds: [randomUUID()],
      }).success,
    ).toBe(false);
    expect(workerProposalSchema.safeParse({ type: "project.create", title: "Legacy" }).success).toBe(false);
  });

  it("accepts onboarding facts without sending them to the worker", () => {
    const householdId = randomUUID();
    const foundingAdult = { id: randomUUID(), displayName: "Jackson" };
    const created = {
      ...envelope(householdId),
      type: "household.created" as const,
      name: "The Barasus",
      timeZone: "America/Los_Angeles",
      foundingAdult,
    };
    const bound = {
      ...envelope(householdId),
      type: "conversation.bound" as const,
      actorAdultId: foundingAdult.id,
      ...binding(foundingAdult.id),
    };

    expect(householdSignalSchema.safeParse(created).success).toBe(true);
    expect(householdSignalSchema.safeParse(bound).success).toBe(true);
    expect(workerSignalSchema.safeParse(created).success).toBe(false);
  });

  it("keeps Gmail source signals identifier-only, promotion model-structured, and Calendar approval app-owned", () => {
    const gmail = {
      ...envelope(),
      type: "gmail.message.changed" as const,
      ownerAdultId: randomUUID(),
      connectionId: randomUUID(),
      messageId: "gmail-message-42",
      threadId: "gmail-thread-7",
      historyId: "991",
    };
    expect(gmailMessageChangedSignalSchema.safeParse(gmail).success).toBe(true);
    expect(
      gmailMessageChangedSignalSchema.safeParse({ ...gmail, subject: "Private diagnosis" }).success,
    ).toBe(false);
    expect(workerSignalSchema.safeParse(gmail).success).toBe(true);
    expect(
      workerProposalSchema.safeParse({
        type: "promote_gmail_candidate",
        candidateId: randomUUID(),
        version: 1,
        candidateDigest: "b".repeat(64),
        responseText: "Shared exactly that family-relevant line.",
        sourceSignalIds: [gmail.signalId, randomUUID()],
      }).success,
    ).toBe(true);
    expect(
      workerProposalSchema.safeParse({
        type: "promote_gmail_candidate",
        candidateId: randomUUID(),
        version: 1,
        candidateDigest: "b".repeat(64),
        responseText: "Shared.",
        sourceSignalIds: [gmail.signalId],
        householdMeaning: "Model must not rewrite the stored promotion.",
      }).success,
    ).toBe(false);
    expect(
      workerProposalSchema.safeParse({
        type: "approve_gmail_calendar",
        candidateId: randomUUID(),
        version: 1,
        candidateDigest: "c".repeat(64),
        sourceSignalIds: [gmail.signalId, randomUUID()],
      }).success,
    ).toBe(true);
    expect(
      workerProposalSchema.safeParse({
        type: "approve_gmail_calendar",
        sourceSignalIds: [gmail.signalId, randomUUID()],
      }).success,
    ).toBe(false);
  });

  it("records app-derived member status and exposes the onboarding profile", () => {
    const member = adult();
    const signal = {
      ...envelope(),
      type: "family.member.upserted" as const,
      actorAdultId: member.id,
      member,
      status: "verified" as const,
    };

    expect(familyMemberUpsertedSignalSchema.safeParse(signal).success).toBe(true);
    expect(
      familyMemberUpsertedSignalSchema.safeParse({
        ...signal,
        member: { ...member, status: "planned" },
      }).success,
    ).toBe(false);
    expect(
      familyMemberUpsertedSignalSchema.safeParse({
        ...signal,
        member: {
          ...member,
          kind: "child",
          role: "dependent",
          currentGrade: "3rd",
        },
        status: "represented",
      }).success,
    ).toBe(false);
    const profile = householdProfileSchema.parse({
      householdId: signal.householdId,
      name: "The Barasus",
      timeZone: "America/Los_Angeles",
      version: 2,
      members: [{ ...member, status: "verified" }],
      identityBoundAdultIds: [member.id],
      onboardingComplete: true,
    });
    expect(profile.members[0]?.status).toBe("verified");
  });
});
