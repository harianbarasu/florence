import { createHash, randomBytes, randomUUID } from "node:crypto";
import { createRequire } from "node:module";
import { EncryptedImageVault } from "@florence/artifacts";
import { type HouseholdSignal, type WorkerInput, workerInputSchema } from "@florence/contracts";
import { HouseholdChiefOfStaff } from "@florence/control-plane";
import { linqIdentitySubjectDigest, migrateDatabase, PostgresFlorenceRepository } from "@florence/database";
import { GatewayWorkerRuntime, type ModelGateway, type ModelRequest } from "@florence/runtime";
import { describe, expect, it } from "vitest";
import { type DeliverableEffect, type EffectExecutor, FlorenceWorker } from "./index.js";

const databaseUrl = process.env.FLORENCE_TEST_DATABASE_URL;
const databaseSchema = process.env.FLORENCE_TEST_POSTGRES_SCHEMA ?? "florence_test";
const describeWithDatabase = databaseUrl ? describe.sequential : describe.skip;
const envelopeKeys = ["signalId", "householdId", "occurredAt", "idempotencyKey"] as const;
type SignalEnvelopeKey = (typeof envelopeKeys)[number];
type SignalBody = HouseholdSignal extends infer Signal
  ? Signal extends HouseholdSignal
    ? Omit<Signal, SignalEnvelopeKey>
    : never
  : never;

const png = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl2nVQAAAAASUVORK5CYII=",
  "base64",
);

class RehearsalGateway implements ModelGateway {
  readonly calls: { input: WorkerInput; request: ModelRequest }[] = [];

  constructor(private readonly dueAt: string) {}

  async generate(request: ModelRequest): Promise<unknown> {
    const evidence = request.content[0];
    if (evidence?.type !== "text") throw new Error("Florence did not send structured evidence");
    const input = workerInputSchema.parse(JSON.parse(evidence.text.slice(evidence.text.indexOf("\n") + 1)));
    this.calls.push({ input, request });
    const signal = input.signal;
    if (signal.type !== "conversation.message") throw new Error("Unexpected cognition signal");

    let proposals: unknown;
    if (signal.text?.includes("permission slip")) {
      proposals = [
        {
          type: "propose_episode",
          title: "Harper permission slip",
          outcome: "Harper's school permission slip is returned today.",
          dueAt: this.dueAt,
          suggestedOwnerAdultId: null,
          responseText: "Harper's permission slip is due today. Who can own returning it?",
          sourceSignalIds: [signal.signalId],
        },
      ];
    } else if (signal.text === "I'll take it.") {
      const episode = input.snapshot.openEpisodes[0];
      if (!episode) throw new Error("Ownership cognition did not receive the open episode");
      proposals = [
        {
          type: "set_episode_owner",
          episodeId: episode.id,
          ownerAdultId: signal.senderAdultId,
          responseText: "Jackson owns returning Harper's permission slip.",
          sourceSignalIds: [signal.signalId],
        },
      ];
    } else if (signal.text === "Handled—the signed slip is in Harper's backpack.") {
      const episode = input.snapshot.openEpisodes[0];
      if (!episode) throw new Error("Completion cognition did not receive the open episode");
      proposals = [
        {
          type: "complete_episode",
          episodeId: episode.id,
          result: "The signed permission slip is in Harper's backpack.",
          responseText: "Handled. I closed the permission-slip follow-up.",
          sourceSignalIds: [signal.signalId],
        },
      ];
    } else {
      throw new Error(`Unexpected rehearsal message: ${signal.text ?? "[image]"}`);
    }

    return { proposals };
  }
}

class RecordingExecutor implements EffectExecutor {
  readonly effects: DeliverableEffect[] = [];

  constructor(private readonly now: () => Date) {}

  async execute(effect: DeliverableEffect) {
    this.effects.push(effect);
    return {
      status: "committed" as const,
      providerReceiptId: `linq-receipt-${effect.id}`,
      detail: null,
      occurredAt: this.now().toISOString(),
    };
  }
}

describeWithDatabase("Jackson family rehearsal", () => {
  it("closes one image-backed Linq obligation without duplicate cognition or effects", async () => {
    const connectionString = databaseUrl as string;
    const householdId = randomUUID();
    const jacksonId = randomUUID();
    const partnerId = randomUUID();
    const childId = randomUUID();
    const conversationId = randomUUID();
    const participantSetDigest = digestIds([jacksonId, partnerId]);
    let repository: PostgresFlorenceRepository | undefined;

    try {
      await migrateDatabase({ connectionString, schema: databaseSchema });
      repository = new PostgresFlorenceRepository({
        connectionString,
        schema: databaseSchema,
        applicationName: "florence-jackson-rehearsal",
      });
      const startedAt = new Date(Date.now() + 5_000);
      const dueAt = new Date(startedAt.getTime() + 3_600_000);
      let clock = startedAt;
      const now = () => clock;
      const makeSignal = signalFactory(householdId, now);
      const groupMessage = (
        senderAdultId: string,
        text: string,
        images: Extract<HouseholdSignal, { type: "conversation.message" }>["images"] = [],
        replyToSignalId: string | null = null,
      ): Extract<HouseholdSignal, { type: "conversation.message" }> =>
        makeSignal({
          type: "conversation.message",
          conversationId,
          audience: "group",
          authorityVersion: 1,
          participantSetDigest,
          senderAdultId,
          text,
          images,
          replyToSignalId,
          source: {
            system: "linq-v3",
            providerEventId: `linq-event-${randomUUID()}`,
            providerMessageId: `linq-message-${randomUUID()}`,
          },
        });
      const vault = new EncryptedImageVault({
        store: repository,
        encryptionKey: randomBytes(32),
      });
      const gateway = new RehearsalGateway(dueAt.toISOString());
      const executor = new RecordingExecutor(now);
      const runtime = new GatewayWorkerRuntime(gateway, vault);
      const chief = new HouseholdChiefOfStaff(repository, runtime, now);
      const worker = new FlorenceWorker(repository, runtime, executor, {
        workerId: `jackson-rehearsal-${householdId}`,
        now,
        pollIntervalMs: 1,
      });

      const onboarding: HouseholdSignal[] = [
        makeSignal({
          type: "household.created",
          name: "The Jackson family",
          timeZone: "America/Los_Angeles",
          foundingAdult: { id: jacksonId, displayName: "Jackson" },
        }),
        makeSignal({
          type: "adult.enrollment.issued",
          actorAdultId: jacksonId,
          adultId: jacksonId,
          challengeDigest: "a".repeat(64),
          expiresAt: new Date(startedAt.getTime() + 86_400_000).toISOString(),
        }),
        makeSignal({
          type: "adult.enrollment.redeemed",
          adultId: jacksonId,
          challengeDigest: "a".repeat(64),
          identitySubjectDigest: linqIdentitySubjectDigest("linq-handle-jackson"),
          consentVersion: "pilot-v1",
          consentedAt: new Date(startedAt.getTime() + 2_000).toISOString(),
          conversationId: randomUUID(),
          providerConversationId: "linq-private-jackson",
        }),
        makeSignal({
          type: "family.member.upserted",
          actorAdultId: jacksonId,
          member: {
            id: partnerId,
            kind: "adult",
            role: "steward",
            displayName: "Alex",
            relationship: "Co-parent",
          },
          status: "planned",
        }),
        makeSignal({
          type: "adult.enrollment.issued",
          actorAdultId: jacksonId,
          adultId: partnerId,
          challengeDigest: "b".repeat(64),
          expiresAt: new Date(startedAt.getTime() + 86_400_000).toISOString(),
        }),
        makeSignal({
          type: "adult.enrollment.redeemed",
          adultId: partnerId,
          challengeDigest: "b".repeat(64),
          identitySubjectDigest: linqIdentitySubjectDigest("linq-handle-alex"),
          consentVersion: "pilot-v1",
          consentedAt: new Date(startedAt.getTime() + 5_000).toISOString(),
          conversationId: randomUUID(),
          providerConversationId: "linq-private-alex",
        }),
        makeSignal({
          type: "family.member.upserted",
          actorAdultId: jacksonId,
          member: {
            id: childId,
            kind: "child",
            role: "dependent",
            displayName: "Harper",
            relationship: "Child",
            birthYear: 2017,
            school: "Lakeside Elementary",
            currentGrade: "3rd",
            academicYear: "2026-2027",
            gradeEffectiveFrom: "2026-08-10",
          },
          status: "represented",
        }),
      ];
      const groupBinding = makeSignal({
        type: "conversation.bound",
        actorAdultId: jacksonId,
        conversationId,
        audience: "group",
        authorityVersion: 1,
        participantSetDigest,
        authorizedAdultIds: [jacksonId, partnerId],
        providerConversationId: "linq-group-jackson-family",
      });
      onboarding.push(groupBinding);
      for (const signal of onboarding) await acceptAndSettle(chief, worker, signal);

      await expect(
        repository.resolveLinqIngressAuthority({
          providerConversationId: "linq-group-jackson-family",
          providerHandleId: "linq-handle-jackson",
          replyToProviderMessageId: null,
          occurredAt: new Date(Date.parse(groupBinding.occurredAt) + 1).toISOString(),
        }),
      ).resolves.toMatchObject({
        householdId,
        conversationId,
        audience: "group",
        authorityVersion: 1,
        participantSetDigest,
        senderAdultId: jacksonId,
      });
      expect((await chief.profile(householdId))?.onboardingComplete).toBe(true);

      const imageAssetId = randomUUID();
      const obligation = groupMessage(partnerId, "The attached permission slip for Harper is due today.", [
        { assetId: imageAssetId, mimeType: "image/png" },
      ]);
      const stored = await vault.store({
        assetId: imageAssetId,
        householdId,
        signalId: obligation.signalId,
        declaredMimeType: "image/png",
        bytes: png,
      });
      expect(stored.image).toEqual(obligation.images[0]);
      await acceptAndSettle(chief, worker, obligation);

      const firstCall = gateway.calls[0];
      expect(firstCall?.input.snapshot.members).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: jacksonId, displayName: "Jackson", status: "verified" }),
          expect.objectContaining({ id: partnerId, displayName: "Alex", status: "verified" }),
          expect.objectContaining({
            id: childId,
            displayName: "Harper",
            school: "Lakeside Elementary",
            currentGrade: "3rd",
          }),
        ]),
      );
      const imageContent = firstCall?.request.content.find((content) => content.type === "image");
      expect(imageContent?.type === "image" ? Buffer.from(imageContent.bytes) : null).toEqual(png);

      const proposed = (await repository.loadEvents(householdId)).find(
        (event) => event.type === "episode.proposed",
      );
      expect(proposed).toMatchObject({ title: "Harper permission slip" });
      if (proposed?.type !== "episode.proposed")
        throw new Error("The image-backed obligation was not proposed");

      await acceptAndSettle(chief, worker, groupMessage(jacksonId, "I'll take it.", [], obligation.signalId));
      expect(
        (await repository.loadEvents(householdId)).find((event) => event.type === "episode.owner.changed"),
      ).toMatchObject({
        episodeId: proposed.episodeId,
        ownerAdultId: jacksonId,
      });

      clock = new Date(dueAt.getTime() + 1_000);
      await drain(worker);
      expect(executor.effects.map((effect) => effect.payload.text)).toContain(
        "Harper permission slip is still open. Is it handled, or should we reassign it?",
      );

      const completion = groupMessage(
        jacksonId,
        "Handled—the signed slip is in Harper's backpack.",
        [],
        obligation.signalId,
      );
      expect(Date.parse(completion.occurredAt)).toBeGreaterThan(dueAt.getTime());
      await acceptAndSettle(chief, worker, completion);

      const effectsBeforeReplay = executor.effects.map((effect) => effect.idempotencyKey);
      const eventsBeforeReplay = await repository.loadEvents(householdId);
      expect((await chief.accept(completion)).disposition).toBe("duplicate");
      expect(await worker.runOnce()).toBe(false);

      const finalEvents = await repository.loadEvents(householdId);
      expect(gateway.calls).toHaveLength(3);
      expect(executor.effects).toHaveLength(6);
      expect(executor.effects).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            expectedAudience: "group",
            expectedParticipantIdentityDigests: [
              linqIdentitySubjectDigest("linq-handle-alex"),
              linqIdentitySubjectDigest("linq-handle-jackson"),
            ].sort(),
          }),
        ]),
      );
      expect(executor.effects.map((effect) => effect.idempotencyKey)).toEqual(effectsBeforeReplay);
      expect(finalEvents).toHaveLength(eventsBeforeReplay.length);
      expect(finalEvents.filter((event) => event.type === "episode.proposed")).toHaveLength(1);
      expect(finalEvents).toContainEqual(
        expect.objectContaining({ type: "episode.completed", episodeId: proposed.episodeId }),
      );
      await expect(
        repository.claimNextDueTimer({
          leaseOwner: "jackson-rehearsal-final-timer-check",
          now: new Date(dueAt.getTime() + 86_400_000).toISOString(),
          leaseUntil: new Date(dueAt.getTime() + 86_460_000).toISOString(),
        }),
      ).resolves.toBeNull();
    } finally {
      await repository?.close();
      await cleanupHousehold(connectionString, databaseSchema, householdId);
    }
  });
});

function signalFactory(householdId: string, now: () => Date) {
  let sequence = 0;
  return <Body extends SignalBody>(body: Body): Body & Record<SignalEnvelopeKey, string> => {
    sequence += 1;
    const signalId = randomUUID();
    return {
      ...body,
      signalId,
      householdId,
      occurredAt: new Date(now().getTime() + sequence * 1_000).toISOString(),
      idempotencyKey: `jackson-rehearsal:${signalId}`,
    };
  };
}

async function acceptAndSettle(
  chief: HouseholdChiefOfStaff,
  worker: FlorenceWorker,
  signal: HouseholdSignal,
): Promise<void> {
  expect((await chief.accept(signal)).disposition).toBe("accepted");
  await drain(worker);
}

async function drain(worker: FlorenceWorker): Promise<void> {
  for (let count = 0; count < 30; count += 1) {
    if (!(await worker.runOnce())) return;
  }
  throw new Error("The rehearsal worker did not become idle");
}

function digestIds(ids: readonly string[]): string {
  return createHash("sha256")
    .update(JSON.stringify([...ids].sort()))
    .digest("hex");
}

type SqlClient = {
  (parts: TemplateStringsArray, ...values: unknown[]): Promise<unknown[]>;
  end(options: { timeout: number }): Promise<void>;
};

async function cleanupHousehold(
  connectionString: string,
  schema: string,
  householdId: string,
): Promise<void> {
  const requireFromDatabase = createRequire(
    new URL("../../../packages/database/package.json", import.meta.url),
  );
  const postgres = requireFromDatabase("postgres") as (
    value: string,
    options: { max: number; connection: { search_path: string } },
  ) => SqlClient;
  const sql = postgres(connectionString, { max: 1, connection: { search_path: schema } });
  try {
    await sql`delete from image_assets where household_id = ${householdId}`;
    await sql`delete from outbox_effects where household_id = ${householdId}`;
    await sql`delete from episode_timers where household_id = ${householdId}`;
    await sql`delete from household_events where household_id = ${householdId}`;
    await sql`delete from household_signals where household_id = ${householdId}`;
    await sql`delete from household_streams where household_id = ${householdId}`;
  } finally {
    await sql.end({ timeout: 5 });
  }
}
