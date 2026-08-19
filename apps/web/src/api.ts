import {
  type CompleteFamilyOnboardingInput,
  type DisconnectGoogleConnectionInput,
  type FamilyCalendarMonthView,
  type FamilyMemberMutationInput,
  familyCalendarMonthViewSchema,
  googleStartResponseSchema,
  type PatchFactInput,
  type PatchWatchInput,
  type PreferencesInput,
  type SessionResponse,
  type SetupSessionInput,
  sessionResponseSchema,
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

export async function getSession(): Promise<SessionResponse> {
  return sessionResponseSchema.parse(await requestJson("/api/v1/session"));
}

export async function createSession(input: SetupSessionInput): Promise<SessionResponse> {
  return sessionResponseSchema.parse(
    await requestJson("/api/v1/session", {
      method: "POST",
      body: JSON.stringify(input),
    }),
  );
}

export async function deleteSession(): Promise<void> {
  await requestJson("/api/v1/session", { method: "DELETE" });
}

export function getWorkspace(): Promise<WorkspaceView> {
  return requestWorkspace("/api/v1/workspace");
}

export async function getFamilyCalendarMonth(month: string): Promise<FamilyCalendarMonthView> {
  return familyCalendarMonthViewSchema.parse(
    await requestJson(`/api/v1/calendar?month=${encodeURIComponent(month)}`),
  );
}

export function completeFamilyOnboarding(input: CompleteFamilyOnboardingInput): Promise<WorkspaceView> {
  return requestWorkspace("/api/v1/vault/household", {
    method: "PUT",
    body: JSON.stringify(input),
  });
}

export function putFamilyMember(memberId: string, input: FamilyMemberMutationInput): Promise<WorkspaceView> {
  return requestWorkspace(`/api/v1/vault/members/${encodeURIComponent(memberId)}`, {
    method: "PUT",
    body: JSON.stringify(input),
  });
}

export function patchVaultFact(factId: string, input: PatchFactInput): Promise<WorkspaceView> {
  return requestWorkspace(`/api/v1/vault/facts/${encodeURIComponent(factId)}`, {
    method: "PATCH",
    body: JSON.stringify(input),
  });
}

export function deleteVaultFact(factId: string): Promise<WorkspaceView> {
  return requestWorkspace(`/api/v1/vault/facts/${encodeURIComponent(factId)}`, {
    method: "DELETE",
  });
}

export function patchVaultWatch(workId: string, input: PatchWatchInput): Promise<WorkspaceView> {
  return requestWorkspace(`/api/v1/vault/watches/${encodeURIComponent(workId)}`, {
    method: "PATCH",
    body: JSON.stringify(input),
  });
}

export function deleteVaultWatch(workId: string): Promise<WorkspaceView> {
  return requestWorkspace(`/api/v1/vault/watches/${encodeURIComponent(workId)}`, {
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

function readError(payload: unknown): string | null {
  if (typeof payload !== "object" || payload === null || !("error" in payload)) return null;
  const error = payload.error;
  if (typeof error === "string") return error;
  if (typeof error === "object" && error !== null && "message" in error) {
    return typeof error.message === "string" ? error.message : null;
  }
  return null;
}
