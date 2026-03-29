"use client";

import type {
  FlorenceConnectUrlResponse,
  FlorenceConnectionsResponse,
  FlorenceSessionResponse,
  FlorenceSettingsResponse,
  FlorenceSetupResponse,
} from "@/lib/types";

export class FlorenceApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "FlorenceApiError";
    this.status = status;
  }
}

function withQuery(path: string, params: Record<string, string | undefined>) {
  const url = new URL(path, window.location.origin);
  for (const [key, value] of Object.entries(params)) {
    if (value) {
      url.searchParams.set(key, value);
    }
  }
  return `${url.pathname}${url.search}`;
}

async function parseResponse<T>(response: Response): Promise<T> {
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? await response.json().catch(() => null)
    : null;
  if (!response.ok || (payload && payload.ok === false)) {
    throw new FlorenceApiError(
      String(payload?.error || response.statusText || "florence_request_failed"),
      response.status,
    );
  }
  return payload as T;
}

export async function getSession(token?: string): Promise<FlorenceSessionResponse> {
  const response = await fetch(withQuery("/api/florence/session", { token }), {
    method: "GET",
    cache: "no-store",
  });
  return parseResponse<FlorenceSessionResponse>(response);
}

export async function getSetup(token?: string): Promise<FlorenceSetupResponse> {
  const response = await fetch(withQuery("/api/florence/setup", { token }), {
    method: "GET",
    cache: "no-store",
  });
  return parseResponse<FlorenceSetupResponse>(response);
}

export async function saveSetupProfile(payload: Record<string, unknown>) {
  const response = await fetch("/api/florence/setup/profile", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return parseResponse<FlorenceSetupResponse>(response);
}

export async function startGoogleConnect(token?: string): Promise<FlorenceConnectUrlResponse> {
  const response = await fetch("/api/florence/google/start", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(token ? { token } : {}),
  });
  return parseResponse<FlorenceConnectUrlResponse>(response);
}

export async function addGoogleAccount(token?: string): Promise<FlorenceConnectUrlResponse> {
  const response = await fetch("/api/florence/google/add-account", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(token ? { token } : {}),
  });
  return parseResponse<FlorenceConnectUrlResponse>(response);
}

export async function getConnections(token?: string): Promise<FlorenceConnectionsResponse> {
  const response = await fetch(withQuery("/api/florence/google/connections", { token }), {
    method: "GET",
    cache: "no-store",
  });
  return parseResponse<FlorenceConnectionsResponse>(response);
}

export async function disconnectGoogleAccount(connectionId: string, token?: string) {
  const response = await fetch("/api/florence/google/disconnect", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({
      connectionId,
      ...(token ? { token } : {}),
    }),
  });
  return parseResponse<FlorenceConnectionsResponse>(response);
}

export async function getSettings(token?: string): Promise<FlorenceSettingsResponse> {
  const response = await fetch(withQuery("/api/florence/settings", { token }), {
    method: "GET",
    cache: "no-store",
  });
  return parseResponse<FlorenceSettingsResponse>(response);
}

export async function saveSettings(payload: Record<string, unknown>) {
  const response = await fetch("/api/florence/settings", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return parseResponse<FlorenceSettingsResponse>(response);
}
