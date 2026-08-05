import type { CalendarPushHeaders, GmailPubSubEvent } from "../adapters/google/index.js";
import type { LinqRecoveredMessageEvent, LinqWebhookEvent } from "../adapters/linq/index.js";

/**
 * Resolving either method acknowledges that the delivery is durably recorded or
 * was durably deduplicated. Rejections must leave the provider free to retry.
 */
export interface DurableIngress {
  acceptLinq(event: LinqWebhookEvent): Promise<void>;
  acceptLinqRecovered(event: LinqRecoveredMessageEvent): Promise<void>;
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
  semanticTimers?:
    | {
        readonly status: "ok" | "degraded" | "unavailable";
        readonly deadCount: number | null;
      }
    | undefined;
}

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

export type CustomerExportConsumption =
  | { readonly status: "download"; readonly filename: string; readonly artifact: JsonObject }
  | { readonly status: "expired" | "invalid" | "consumed" | "unavailable" };

/** Resolves a signed, single-use customer export handoff without exposing its signing implementation. */
export interface CustomerExportHandoff {
  consumeExportToken(token: string): Promise<CustomerExportConsumption>;
}

/** Operational control-plane access is deliberately read-only. */
export interface HouseholdOperations {
  status(): Promise<OperatorStatus>;
}

export interface FlorenceHttpServices {
  ingress: DurableIngress;
  googleOAuth: GoogleOAuthHandoff;
  customerExport: CustomerExportHandoff;
  readiness: ReadinessProbe;
  operations: HouseholdOperations;
}
