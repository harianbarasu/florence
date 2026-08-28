import type { EnrolledTurnRef } from "@florence/database";

/** Durable result of Florence's one enrolled-parent capability run. */
export type RespondReceipt = Readonly<{
  sourceId: string;
  disposition: "committed" | "superseded" | "authority_lost" | "retry_scheduled";
}>;

/**
 * The sole application-facing entry point for an enrolled assistant turn.
 *
 * Tool schemas, provider selection, policy admission, evidence, lifecycle state, decision
 * validation, and commit construction remain private to the implementation.
 */
export interface FlorenceCapabilities {
  respond(turn: EnrolledTurnRef, signal: AbortSignal): Promise<RespondReceipt>;
}

/**
 * Freeze the one application seam after setup has promoted a store-produced inbound locator.
 * Florence owns transport/setup orchestration; the responder owns the enrolled capability run.
 */
export function createFlorenceCapabilities(respond: FlorenceCapabilities["respond"]): FlorenceCapabilities {
  return Object.freeze({ respond });
}
