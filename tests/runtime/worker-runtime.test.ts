import { describe, expect, it, vi } from "vitest";
import { z } from "zod";
import {
  FakeWorkerRuntime,
  type WorkerResultPayload,
  WorkerRuntimeError,
  type WorkerTool,
} from "../../src/runtime/index.js";
import { workerJob, workerPayload } from "./fixtures.js";

describe("FakeWorkerRuntime contract", () => {
  it("validates and enriches model-authored payloads with authoritative job identity", async () => {
    const runtime = new FakeWorkerRuntime(workerPayload);
    const job = workerJob();

    const result = await runtime.run(job);

    expect(result).toMatchObject({
      jobId: job.jobId,
      attemptId: job.attemptId,
      householdId: job.householdId,
      baseHouseholdVersion: job.baseHouseholdVersion,
      policyVersion: job.policyVersion,
      modelRouteId: job.modelRouteId,
      modelCapabilityProfile: job.modelCapabilityProfile,
      outputContractRef: job.outputContractRef,
      summary: workerPayload.summary,
    });
    expect(runtime.calls).toEqual([job]);
  });

  it("rejects invalid worker output", async () => {
    const invalid = { ...workerPayload, confidence: 2 } as unknown as WorkerResultPayload;
    const runtime = new FakeWorkerRuntime(invalid);

    await expect(runtime.run(workerJob())).rejects.toMatchObject({ code: "invalid_output" });
  });

  it("rejects mismatched result identity", async () => {
    const runtime = new FakeWorkerRuntime(async (job) => ({
      ...workerPayload,
      jobId: "different-job",
      attemptId: job.attemptId,
      householdId: job.householdId,
      baseHouseholdVersion: job.baseHouseholdVersion,
      policyVersion: job.policyVersion,
      modelRouteId: job.modelRouteId,
      modelCapabilityProfile: job.modelCapabilityProfile,
      outputContractRef: job.outputContractRef,
      diagnostics: {
        durationMs: 0,
        modelCalls: 0,
        toolCalls: 0,
        usage: {},
        traceReferences: [],
      },
    }));

    await expect(runtime.run(workerJob())).rejects.toMatchObject({ code: "invalid_output" });
  });

  it("honors cancellation and still cleans attempt resources", async () => {
    const cleanup = vi.fn(async () => undefined);
    const runtime = new FakeWorkerRuntime(workerPayload);
    const controller = new AbortController();
    controller.abort();

    await expect(runtime.run(workerJob(), { signal: controller.signal, cleanup })).rejects.toMatchObject({
      code: "cancelled",
    });
    expect(cleanup).toHaveBeenCalledOnce();
  });

  it("requires the attempt tool set to exactly match the job allowlist and capabilities", async () => {
    const execute = vi.fn(async () => ({ ok: true }));
    const tool: WorkerTool = {
      name: "lookup_evidence",
      description: "Read one evidence grant.",
      inputSchema: z.object({ reference: z.string() }),
      requiredCapabilityIds: ["read-evidence"],
      execute,
    };
    const runtime = new FakeWorkerRuntime(workerPayload);

    await expect(
      runtime.run(workerJob({ allowedToolNames: [tool.name], capabilityIds: [] }), { tools: [tool] }),
    ).rejects.toMatchObject({ code: "invalid_job" });

    await expect(
      runtime.run(workerJob({ allowedToolNames: [], capabilityIds: ["read-evidence"] }), {
        tools: [tool],
      }),
    ).rejects.toMatchObject({ code: "invalid_job" });
    expect(execute).not.toHaveBeenCalled();
  });

  it("fails closed if attempt cleanup fails", async () => {
    const runtime = new FakeWorkerRuntime(workerPayload);

    await expect(
      runtime.run(workerJob(), {
        cleanup: async () => {
          throw new Error("raw cleanup detail");
        },
      }),
    ).rejects.toEqual(new WorkerRuntimeError("cleanup_failed"));
  });

  it("never admits one adult's personal context under another adult's grant", async () => {
    const runtime = new FakeWorkerRuntime(workerPayload);
    const personalJob = workerJob({
      scopeGrant: {
        grantId: "personal-grant",
        visibility: "personal",
        adultId: "adult-a",
        purpose: "private source review",
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      },
    });

    await expect(
      runtime.run(personalJob, {
        context: [
          {
            reference: "private-b",
            visibility: "personal",
            adultId: "adult-b",
            content: "Private context belonging to another adult.",
          },
        ],
      }),
    ).rejects.toMatchObject({ code: "invalid_job" });

    await expect(
      runtime.run(personalJob, {
        context: [
          {
            reference: "private-a",
            visibility: "personal",
            adultId: "adult-a",
            content: "Private context covered by this grant.",
          },
        ],
      }),
    ).resolves.toMatchObject({ jobId: personalJob.jobId });
  });
});
