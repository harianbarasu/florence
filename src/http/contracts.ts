import type { CalendarPushHeaders, GmailPubSubEvent } from "../adapters/google/index.js";
import type { LinqInboundEvent } from "../adapters/linq/index.js";

/**
 * Resolving either method acknowledges that the delivery is durably recorded or
 * was durably deduplicated. Rejections must leave the provider free to retry.
 */
export interface DurableIngress {
  acceptLinq(event: LinqInboundEvent): Promise<void>;
  acceptGmailPush(event: GmailPubSubEvent): Promise<void>;
  acceptCalendarPush(headers: CalendarPushHeaders): Promise<"accepted" | "unauthorized">;
}

export type GoogleOAuthStartResult =
  | { kind: "redirect"; authorizationUrl: string }
  | { kind: "expired" }
  | { kind: "invalid" };

export type GoogleOAuthCompletionResult =
  | { kind: "connected" }
  | { kind: "declined" }
  | { kind: "expired" }
  | { kind: "invalid" };

/**
 * The implementation owns OAuth state, PKCE material, identity binding, token
 * encryption, and durable grant persistence. None of those values are returned
 * through this interface.
 */
export interface GoogleOAuthHandoff {
  start(input: { handoffToken: string }): Promise<GoogleOAuthStartResult>;
  complete(input: {
    state: string;
    code: string | null;
    providerError: string | null;
  }): Promise<GoogleOAuthCompletionResult>;
}

export interface ReadinessProbe {
  isReady(): Promise<boolean>;
}

export interface OperatorStatus {
  status: "ok" | "degraded";
  checks: Readonly<Record<string, "ok" | "degraded" | "unavailable">>;
}

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

export type OperatorDeleteResult = "accepted" | "already_deleted" | "not_found";

/**
 * Export payloads must contain app-owned user data only; connector credentials,
 * OAuth tokens, encryption keys, and model traces are forbidden by contract.
 */
export interface HouseholdOperations {
  status(): Promise<OperatorStatus>;
  exportHousehold(input: { householdId: string }): Promise<JsonObject | null>;
  deleteHousehold(input: { householdId: string; idempotencyKey: string }): Promise<OperatorDeleteResult>;
}

export interface FlorenceHttpServices {
  ingress: DurableIngress;
  googleOAuth: GoogleOAuthHandoff;
  readiness: ReadinessProbe;
  operations: HouseholdOperations;
}
