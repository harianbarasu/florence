export interface Viewer {
  person: {
    id: string;
    name: string;
    phone: string;
    timezone: string;
  };
  households: {
    id: string;
    name: string;
    role: string;
    memberCount: number;
  }[];
  csrfToken: string;
  session: {
    assuranceKind: "base" | "google_connect" | "account_controls";
    assuranceExpiresAt: string | null;
  };
}

export type FamilyRole = "steward" | "caregiver" | "participant" | "dependent";

export interface PeopleView {
  households: {
    id: string;
    name: string;
    status: string;
    viewerRole: FamilyRole;
    canInvite: boolean;
    canAddDependent: boolean;
    members: {
      id: string;
      name: string;
      role: FamilyRole;
      self: boolean;
      represented: boolean;
      context: {
        aliases: string[];
        birthYear: number | null;
        school: string;
        activities: string[];
      } | null;
    }[];
    eligibleParticipants: {
      personId: string;
      conversationId: string;
      name: string;
      registered: boolean;
    }[];
    coverageGroups: {
      conversationId: string;
      label: string;
      active: boolean;
      approvedCount: number;
      requiredCount: number;
      viewerApproved: boolean;
      canApprove: boolean;
      blockedReason: string | null;
    }[];
  }[];
  invitations: {
    id: string;
    householdId: string;
    householdName: string;
    personName: string;
    role: Exclude<FamilyRole, "dependent">;
    action: "approve" | "accept";
    canAct: boolean;
    detail: string;
    expiresAt: string;
    sharedContext: {
      children: {
        preferredName: string;
        aliases: string[];
        birthYear: number | null;
        school: string;
        activities: string[];
      }[];
    } | null;
  }[];
}

export interface HomeView {
  monitoring: {
    status: "healthy" | "learning" | "attention";
    label: string;
    detail: string;
  };
  attentionCount: number;
  items: ExceptionItem[];
  onboarding: {
    completed: number;
    total: number;
    next: string | null;
    detail: string | null;
    href: string | null;
    actionLabel: string | null;
  } | null;
}

export interface ExceptionItem {
  id: string;
  kind: "coverage" | "approval" | "private_review" | "connection" | "privacy";
  phase?: "open" | "awaiting" | "confirmed";
  title: string;
  detail: string;
  urgency: "routine" | "soon" | "now";
  changedAt?: string;
  href?: string;
}

export interface ChatView {
  id: string;
  kind: "direct" | "group";
  title: string;
  mode: "registration_required" | "observe_only" | "trusted_write_enabled" | "paused";
  epochId: string;
  epochStartedAt: string;
  participants: {
    id: string;
    name: string;
    registered: boolean;
    consented: boolean;
  }[];
  retentionDays: number | null;
  proactive: boolean;
  blockedReason: string | null;
}

export interface SourceView {
  connections: {
    id: string;
    label: string;
    email: string;
    accountKind: "personal_family" | "work";
    accountKindLabel: string;
    status: string;
    statusLabel: string;
    mail: {
      liveState: "waiting" | "watching" | "paused" | "needs_attention";
      liveLabel: string;
      lastCheckedAt: string | null;
      historyState: "waiting" | "running" | "complete" | "needs_attention";
      historyLabel: string;
      milestones: {
        id: "newest_30_days" | "days_31_to_90" | "days_91_to_365" | "older_history";
        label: string;
        detail: string;
        state: "waiting" | "running" | "complete" | "needs_attention";
        stateLabel: string;
      }[];
    } | null;
    calendar: {
      syncState: "waiting" | "ready" | "paused" | "needs_attention";
      syncLabel: string;
      catalogLabel: string;
      lastCheckedAt: string | null;
    } | null;
    calendars: {
      id: string;
      name: string;
      primary: boolean;
      timezone: string | null;
      mode: "full_private" | "availability_only" | "off";
    }[];
  }[];
  privateReviews: {
    id: string;
    kind: string;
    title: string;
    summary: string;
    details: string[];
    sourceLabel: string;
    proposedAt: string;
    expiresAt: string | null;
    destinations: {
      conversationId: string;
      label: string;
      participantCount: number;
    }[];
    preparingShare: boolean;
    shareProposal: {
      actionIntentId: string;
      minimumMeaning: string;
      canCreateStandingRule: boolean;
      standingRuleLabel: string | null;
      actionDigest: string;
      dataDigest: string;
      policyDigest: string;
      targetDigest: string;
    } | null;
  }[];
  memories: {
    id: string;
    label: string;
    scope: string;
    source: string;
    asOf: string;
  }[];
  rules: {
    id: string;
    label: string;
    source: string;
    destination: string;
  }[];
}

export interface RoutineView {
  destinations: {
    conversationId: string;
    householdId: string;
    label: string;
    participantCount: number;
    canCreate: boolean;
  }[];
  people: {
    personId: string;
    householdId: string;
    name: string;
    self: boolean;
  }[];
  routines: {
    id: string;
    householdId: string;
    title: string;
    sharedMeaning: string;
    cadence: string;
    time: string;
    destination: { conversationId: string; label: string };
    status: "active" | "paused" | "retired";
    holder: { personId: string; name: string; standing: boolean } | null;
    version: number;
    canRevise: boolean;
    canManage: boolean;
    weekdays: number[];
    startsOn: string;
    endsOn: string | null;
    timeZone: string;
    localEventTime: string;
    earliestUsefulMinutesBefore: number;
    lastResponsibleMinutesBefore: number;
    notificationMode: "exceptions_only" | "always" | "silent";
    standingSelfCoverage: boolean;
  }[];
}

export interface DataSafetyView {
  paused: boolean;
  sessions: { id: string; createdAt: string; lastSeenAt: string; current: boolean }[];
  connections: { id: string; provider: "google"; email: string; status: string }[];
  deletion: { status: string; requestedAt: string | null } | null;
}

export class ApiError extends Error {
  public constructor(
    message: string,
    public readonly status: number,
  ) {
    super(message);
  }
}

export async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path, {
    cache: "no-store",
    credentials: "same-origin",
    headers: { Accept: "application/json" },
  });
  if (!response.ok) throw new ApiError(await safeError(response), response.status);
  return (await response.json()) as T;
}

export async function postJson<T>(path: string, csrfToken: string, body: unknown): Promise<T> {
  const response = await fetch(path, {
    method: "POST",
    credentials: "same-origin",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-CSRF-Token": csrfToken,
    },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new ApiError(await safeError(response), response.status);
  return (await response.json()) as T;
}

async function safeError(response: Response): Promise<string> {
  const body = (await response.json().catch(() => null)) as { error?: string } | null;
  return body?.error ?? `Request failed (${response.status})`;
}
