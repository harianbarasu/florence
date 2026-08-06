import { describe, expect, it, vi } from "vitest";
import { FlorenceApplication } from "../../src/application/index.js";
import type { ClaimedJob } from "../../src/modules/work/index.js";
import { dispatchTimerProcessJob } from "../../src/runtime/timer-runtime.js";

const timerPayload = {
  id: "10000000-0000-4000-8000-000000000001",
  kind: "coverage.notification",
  coverageLoopId: "10000000-0000-4000-8000-000000000002",
  expectedDomainVersion: 3,
  dueAt: "2026-08-05T19:00:00.000Z",
};

function job(kind: string, payload: unknown): ClaimedJob {
  return {
    id: "10000000-0000-4000-8000-000000000003",
    kind,
    payload,
    attemptCount: 1,
    maxAttempts: 5,
    leaseToken: "10000000-0000-4000-8000-000000000004",
    deadlineAt: null,
    fence: {},
  };
}

describe("timer-process dispatch", () => {
  it("routes the durable timer job to the timer runtime instead of the unsupported-job path", async () => {
    const process = vi.fn().mockResolvedValue(undefined);

    await expect(dispatchTimerProcessJob(job("timer.process", timerPayload), { process })).resolves.toBe(
      true,
    );
    expect(process).toHaveBeenCalledOnce();
    expect(process).toHaveBeenCalledWith(timerPayload);
  });

  it("leaves unrelated jobs for the worker's existing dispatcher", async () => {
    const process = vi.fn().mockResolvedValue(undefined);

    await expect(dispatchTimerProcessJob(job("maintenance.tick", {}), { process })).resolves.toBe(false);
    expect(process).not.toHaveBeenCalled();
  });

  it("fails closed before invoking the runtime when durable timer identity is incomplete", async () => {
    const process = vi.fn().mockResolvedValue(undefined);
    const { dueAt: _dueAt, ...incomplete } = timerPayload;

    await expect(dispatchTimerProcessJob(job("timer.process", incomplete), { process })).rejects.toThrow();
    expect(process).not.toHaveBeenCalled();
  });

  it("reenters the application mutation seam before the timer runtime", async () => {
    const process = vi.fn().mockResolvedValue(undefined);
    const application = new FlorenceApplication(null as never, null as never, null as never, {
      process,
    });

    await expect(application.process({ kind: "timer.process", timer: timerPayload })).resolves.toMatchObject({
      disposition: "timer_processed",
      ids: { timerId: timerPayload.id },
    });
    expect(process).toHaveBeenCalledOnce();
    expect(process).toHaveBeenCalledWith(timerPayload);
  });
});
