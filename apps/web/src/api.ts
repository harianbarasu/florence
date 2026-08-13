import {
  type AcceptanceReceipt,
  acceptanceReceiptSchema,
  type FamilyMemberProfile,
  type HouseholdProfile,
  householdProfileSchema,
} from "@florence/contracts";

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "";

export class FlorenceRequestError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "FlorenceRequestError";
  }
}

export type CreateHouseholdInput = {
  commandId: string;
  occurredAt: string;
  name: string;
  timeZone: string;
  foundingAdultDisplayName: string;
  secondAdultDisplayName: string;
  secondAdultRole: "steward" | "caregiver";
  secondAdultRelationship: string;
};

export type UpsertMemberInput = Omit<FamilyMemberProfile, "id" | "status"> & {
  commandId: string;
  occurredAt: string;
};

export type IssueLinqEnrollmentInput = { commandId: string; occurredAt: string };

export type GoogleConnectionView = {
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  status: "active";
  emailLabel: string;
  grantedScopes: readonly string[];
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
};

async function requestJson(path: string, init?: RequestInit): Promise<unknown> {
  const response = await fetch(`${API_BASE}${path}`, {
    credentials: "include",
    ...init,
    headers: {
      Accept: "application/json",
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...init?.headers,
    },
  });
  const payload = (await response.json()) as unknown;
  if (!response.ok) {
    throw new FlorenceRequestError(
      response.status,
      readError(payload) ?? `Florence request failed (${response.status})`,
    );
  }
  return payload;
}

export async function getSession(): Promise<{ adultId: string }> {
  const payload = await requestJson("/api/v1/session");
  return { adultId: String(property(payload, "adultId")) };
}

export async function createSession(accessCode: string): Promise<{ adultId: string }> {
  const payload = await requestJson("/api/v1/session", {
    method: "POST",
    headers: { Authorization: `Bearer ${accessCode}` },
  });
  return { adultId: String(property(payload, "adultId")) };
}

export async function deleteSession(): Promise<void> {
  await requestJson("/api/v1/session", { method: "DELETE" });
}

export async function listHouseholds(): Promise<HouseholdProfile[]> {
  const payload = await requestJson("/api/v1/households");
  return householdProfileSchema.array().parse(property(payload, "households"));
}

export async function getHousehold(householdId: string): Promise<HouseholdProfile> {
  const payload = await requestJson(`/api/v1/households/${householdId}`);
  return householdProfileSchema.parse(property(payload, "household"));
}

export async function createHousehold(
  input: CreateHouseholdInput,
): Promise<{ householdId: string; receipt: AcceptanceReceipt }> {
  const payload = await requestJson("/api/v1/households", {
    method: "POST",
    body: JSON.stringify(input),
  });
  return {
    householdId: String(property(payload, "householdId")),
    receipt: acceptanceReceiptSchema.parse(property(payload, "receipt")),
  };
}

export async function upsertFamilyMember(
  householdId: string,
  memberId: string,
  input: UpsertMemberInput,
): Promise<AcceptanceReceipt> {
  const payload = await requestJson(`/api/v1/households/${householdId}/members/${memberId}`, {
    method: "PUT",
    body: JSON.stringify(input),
  });
  return acceptanceReceiptSchema.parse(property(payload, "receipt"));
}

export async function issueLinqEnrollment(
  householdId: string,
  adultId: string,
  input: IssueLinqEnrollmentInput,
): Promise<{ code: string; expiresAt: string }> {
  const payload = await requestJson(`/api/v1/households/${householdId}/members/${adultId}/linq-enrollment`, {
    method: "POST",
    body: JSON.stringify(input),
  });
  return {
    code: String(property(payload, "code")),
    expiresAt: String(property(payload, "expiresAt")),
  };
}

export async function listGoogleConnections(householdId: string): Promise<GoogleConnectionView[]> {
  const payload = await requestJson(`/api/v1/households/${householdId}/google-connections`);
  const connections = property(payload, "connections");
  if (!Array.isArray(connections)) throw new Error("Florence returned invalid Google connections");
  return connections.map(parseGoogleConnection);
}

export async function startGoogleConnection(householdId: string): Promise<{ authorizationUrl: string }> {
  const payload = await requestJson(`/api/v1/households/${householdId}/google-connections`, {
    method: "POST",
  });
  const authorizationUrl = property(payload, "authorizationUrl");
  if (typeof authorizationUrl !== "string" || !URL.canParse(authorizationUrl)) {
    throw new Error("Florence returned an invalid Google authorization link");
  }
  return { authorizationUrl };
}

export async function disconnectGoogleConnection(householdId: string, connectionId: string): Promise<void> {
  await requestJson(`/api/v1/households/${householdId}/google-connections/${connectionId}`, {
    method: "DELETE",
  });
}

function parseGoogleConnection(value: unknown): GoogleConnectionView {
  if (!value || typeof value !== "object") throw new Error("Florence returned an invalid Google connection");
  const row = value as Record<string, unknown>;
  const grantedScopes = row.grantedScopes;
  if (
    typeof row.connectionId !== "string" ||
    typeof row.householdId !== "string" ||
    typeof row.ownerAdultId !== "string" ||
    row.status !== "active" ||
    typeof row.emailLabel !== "string" ||
    !Array.isArray(grantedScopes) ||
    grantedScopes.some((scope) => typeof scope !== "string") ||
    (row.lastError !== null && typeof row.lastError !== "string") ||
    typeof row.createdAt !== "string" ||
    typeof row.updatedAt !== "string"
  ) {
    throw new Error("Florence returned an invalid Google connection");
  }
  return {
    connectionId: row.connectionId,
    householdId: row.householdId,
    ownerAdultId: row.ownerAdultId,
    status: row.status,
    emailLabel: row.emailLabel,
    grantedScopes: grantedScopes as string[],
    lastError: row.lastError as string | null,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function property(payload: unknown, key: string): unknown {
  if (typeof payload !== "object" || payload === null || !(key in payload)) {
    throw new Error(`Florence returned an invalid ${key} response`);
  }
  return payload[key as keyof typeof payload];
}

function readError(payload: unknown): string | null {
  if (typeof payload !== "object" || payload === null || !("error" in payload)) return null;
  const error = payload.error;
  if (typeof error === "string") return error;
  if (typeof error === "object" && error !== null && "message" in error) {
    return typeof error.message === "string" ? error.message : null;
  }
  return null;
}
