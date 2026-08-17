import {
  type DisconnectGoogleConnectionInput,
  type FamilyMemberInput,
  googleStartResponseSchema,
  type MessagesInviteResponse,
  messagesInviteResponseSchema,
  type PatchFactInput,
  type PreferencesInput,
  type PutHouseholdInput,
  type WorkspaceView,
  workspaceResponseSchema,
} from "@florence/contracts";

export class FlorenceRequestError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "FlorenceRequestError";
  }
}

async function requestJson(path: string, init?: RequestInit): Promise<unknown> {
  const response = await fetch(path, {
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

async function requestWorkspace(path: string, init?: RequestInit): Promise<WorkspaceView> {
  return workspaceResponseSchema.parse(await requestJson(path, init)).workspace;
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

export function getWorkspace(): Promise<WorkspaceView> {
  return requestWorkspace("/api/v1/workspace");
}

export function putHousehold(input: PutHouseholdInput): Promise<WorkspaceView> {
  return requestWorkspace("/api/v1/vault/household", {
    method: "PUT",
    body: JSON.stringify(input),
  });
}

export function putFamilyMember(memberId: string, input: FamilyMemberInput): Promise<WorkspaceView> {
  return requestWorkspace(`/api/v1/vault/members/${encodeURIComponent(memberId)}`, {
    method: "PUT",
    body: JSON.stringify(input),
  });
}

export async function issueMessagesInvite(adultId: string): Promise<MessagesInviteResponse> {
  return messagesInviteResponseSchema.parse(
    await requestJson(`/api/v1/vault/adults/${encodeURIComponent(adultId)}/messages-invite`, {
      method: "POST",
    }),
  );
}

export function patchVaultFact(factId: string, input: PatchFactInput): Promise<WorkspaceView> {
  return requestWorkspace(`/api/v1/vault/facts/${encodeURIComponent(factId)}`, {
    method: "PATCH",
    body: JSON.stringify(input),
  });
}

export function deleteVaultFact(factId: string): Promise<WorkspaceView> {
  return requestWorkspace(`/api/v1/vault/facts/${encodeURIComponent(factId)}`, { method: "DELETE" });
}

export function deleteVaultDocument(documentId: string): Promise<WorkspaceView> {
  return requestWorkspace(`/api/v1/vault/documents/${encodeURIComponent(documentId)}`, {
    method: "DELETE",
  });
}

export function putPreferences(input: PreferencesInput): Promise<WorkspaceView> {
  return requestWorkspace("/api/v1/preferences", {
    method: "PUT",
    body: JSON.stringify(input),
  });
}

export async function startGoogleConnection(): Promise<{ authorizationUrl: string }> {
  return googleStartResponseSchema.parse(
    await requestJson("/api/v1/workspace/google-connections", { method: "POST" }),
  );
}

export function disconnectGoogleConnection(connectionId: string): Promise<WorkspaceView> {
  const input: DisconnectGoogleConnectionInput = { connectionId };
  return requestWorkspace("/api/v1/workspace/google-connections", {
    method: "DELETE",
    body: JSON.stringify(input),
  });
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
