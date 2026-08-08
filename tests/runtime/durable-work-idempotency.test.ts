import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { Database } from "../../src/db/client.js";
import { DurableWork, type EnqueueJobInput } from "../../src/modules/work/index.js";
import { canonicalDigest, canonicalJson } from "../../src/shared/canonical-json.js";
import { SecretBox } from "../../src/shared/crypto.js";

const secretBox = new SecretBox(
  "test-v1",
  JSON.stringify({ "test-v1": Buffer.alloc(32, 12).toString("base64") }),
);

describe("durable work idempotency", () => {
  it("reuses only identical active or successful work under current authority", async () => {
    const input = jobInput();
    const identical = existingJob(input, "succeeded");

    await expect(new DurableWork(fakeDatabase(identical), secretBox).enqueue(input)).resolves.toEqual({
      jobId: identical.id,
      created: false,
    });

    await expect(
      new DurableWork(
        fakeDatabase({ ...identical, payload_digest: canonicalDigest({ other: true }) }),
        secretBox,
      ).enqueue(input),
    ).rejects.toThrow(/idempotency/i);

    await expect(
      new DurableWork(fakeDatabase({ ...identical, status: "cancelled" }), secretBox).enqueue(input),
    ).rejects.toThrow(/recovery path/i);

    await expect(
      new DurableWork(fakeDatabase({ ...identical, authority_current: false }), secretBox).enqueue(input),
    ).rejects.toThrow(/recovery path/i);

    const expiredInput = { ...input, deadlineAt: new Date("2020-08-08T20:05:00Z") };
    await expect(
      new DurableWork(fakeDatabase(existingJob(expiredInput, "pending")), secretBox).enqueue(expiredInput),
    ).rejects.toThrow(/recovery path/i);
  });

  it("clears an expired deadline only for an explicitly opted-in recovery generation", async () => {
    const expiredAt = new Date("2026-08-08T19:00:00Z");
    const insertedValues: unknown[] = [];
    const database = redriveDatabase(expiredAt, insertedValues);

    await expect(
      new DurableWork(database, secretBox).redriveDeadCurrentAuthority({
        kind: "orchestrate.linq_message",
        idempotencyNamespace: "job-redrive:conversation",
        now: new Date("2026-08-08T20:00:00Z"),
        lookbackMs: 24 * 60 * 60_000,
        bucketMs: 5 * 60_000,
        maxGenerations: 2,
        attentionErrorCodes: ["conversation_response_window_expired"],
        clearDeadlineOnRedrive: true,
      }),
    ).resolves.toBe(1);

    expect(
      insertedValues.some((value) => value instanceof Date && value.getTime() === expiredAt.getTime()),
    ).toBe(false);
  });
});

function jobInput(): EnqueueJobInput {
  return {
    kind: "orchestrate.linq_message",
    idempotencyKey: `test-job:${randomUUID()}`,
    payload: { internalProviderEventId: randomUUID() },
    person: { id: randomUUID(), controlEpoch: 2 },
    conversation: { id: randomUUID(), authorityVersion: 3 },
    deadlineAt: new Date("2030-08-08T20:05:00Z"),
    maxAttempts: 5,
    priority: 20,
  };
}

function existingJob(input: EnqueueJobInput, status: string) {
  return {
    id: randomUUID(),
    status,
    job_kind: input.kind,
    household_id: input.household?.id ?? null,
    person_id: input.person?.id ?? null,
    conversation_id: input.conversation?.id ?? null,
    integration_id: input.integration?.id ?? null,
    task_id: input.taskId ?? null,
    payload_digest: canonicalDigest(input.payload),
    max_attempts: input.maxAttempts ?? 8,
    priority: input.priority ?? 100,
    deadline_at: input.deadlineAt ?? null,
    person_control_epoch: input.person?.controlEpoch ?? null,
    household_control_epoch: input.household?.controlEpoch ?? null,
    conversation_authority_version: input.conversation?.authorityVersion ?? null,
    integration_control_epoch: input.integration?.controlEpoch ?? null,
    case_key_digest: input.caseKeyDigest ?? null,
    authority_current: true,
  };
}

function fakeDatabase(row: ReturnType<typeof existingJob>): Database {
  return (async (strings: TemplateStringsArray) => {
    const query = canonicalJson([...strings]);
    if (!query.includes("from jobs job")) throw new Error(`Unexpected query: ${query}`);
    return [row];
  }) as unknown as Database;
}

function redriveDatabase(expiredAt: Date, insertedValues: unknown[]): Database {
  const database = async (strings: TemplateStringsArray, ...values: unknown[]) => {
    const query = canonicalJson([...strings]);
    if (query.includes("select job.id") && query.includes("from jobs job")) {
      return [
        {
          id: randomUUID(),
          job_kind: "orchestrate.linq_message",
          household_id: null,
          person_id: null,
          conversation_id: null,
          integration_id: null,
          task_id: null,
          idempotency_key: `orchestrate:linq:${randomUUID()}`,
          payload_digest: "a".repeat(64),
          payload_ciphertext: Buffer.from("encrypted"),
          payload_key_version: "test-v1",
          max_attempts: 5,
          priority: 20,
          deadline_at: expiredAt,
          person_control_epoch: null,
          household_control_epoch: null,
          conversation_authority_version: null,
          integration_control_epoch: null,
          case_key_digest: null,
        },
      ];
    }
    if (query.includes("insert into jobs")) {
      insertedValues.push(...values);
      return [{ id: randomUUID() }];
    }
    if (query.includes("update jobs")) return [];
    throw new Error(`Unexpected query: ${query}`);
  };
  Object.assign(database, { array: (values: readonly unknown[]) => values });
  return database as unknown as Database;
}
