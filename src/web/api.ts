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
  onboardingCompleted: boolean;
  csrfToken: string;
  session: {
    assuranceKind:
      | "base"
      | "onboarding"
      | "google_connect"
      | "account_controls"
      | "household_invitation"
      | "private_bridge_standing";
    assuranceExpiresAt: string | null;
  };
}

export type OnboardingStep =
  | "confirm_profile"
  | "create_household"
  | "choose_household"
  | "adults"
  | "children"
  | "review_shared_context"
  | "google"
  | "review"
  | "complete";

export interface OnboardingView {
  completed: boolean;
  branch: "starter" | "invited_adult" | "caregiver";
  step: OnboardingStep;
  progress: {
    current: number;
    total: number;
  };
  person: {
    name: string;
    timeZone: string;
    profileReviewed: boolean;
    profileReviewVersion: number;
  };
  household: {
    id: string;
    name: string;
    versions: {
      membership: number;
      roster: number;
      intake: number;
      membershipOnboarding: number;
    };
    sharedIntakeComplete: boolean;
    adultRosterReviewed: boolean;
    adults: {
      id: string;
      version: number;
      displayName: string;
      role: "steward" | "caregiver";
      matchedPersonId: string | null;
      invitationId: string | null;
      progress: "not_connected" | "awaiting_steward_approval" | "awaiting_acceptance" | "joined";
    }[];
    children: {
      id: string;
      name: string;
      aliases: string[];
      birthYear: number | null;
      school: string | null;
      activities: string[];
    }[];
  } | null;
  householdChoices: {
    id: string;
    name: string;
    role: string;
    sharedIntakeComplete: boolean;
  }[];
  google: {
    decision: "undecided" | "connected" | "skipped";
    accountEmail: string | null;
    status: string | null;
  };
  eligibleInvitees: {
    personId: string;
    identityId: string;
    conversationId: string;
    participantEpochId: string;
    participantDigest: string;
    name: string;
    registered: boolean;
  }[];
}

export type FamilyRole = "steward" | "caregiver" | "participant" | "dependent";

export interface PeopleView {
  households: {
    id: string;
    name: string;
    status: string;
    rosterVersion: number;
    intakeVersion: number;
    viewerRole: FamilyRole;
    canInvite: boolean;
    canAddDependent: boolean;
    plannedAdults: {
      id: string;
      version: number;
      displayName: string;
      role: "steward" | "caregiver";
      matchedPersonId: string | null;
      progress: "not_connected" | "awaiting_steward_approval" | "awaiting_acceptance" | "joined";
    }[];
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
      identityId: string;
      participantEpochId: string;
      participantDigest: string;
      name: string;
      registered: boolean;
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
  kind: "coverage" | "private_review" | "connection" | "privacy" | "request";
  phase?: "open" | "awaiting" | "confirmed";
  title: string;
  detail: string;
  urgency: "routine" | "soon" | "now";
  changedAt?: string;
  href?: string;
}

export interface ChatView {
  id: string;
  title: string;
  status: "interactive" | "read_only";
  statusLabel: "Florence can help here" | "Florence is read-only here";
  reason: string;
  participants: {
    id: string;
    name: string;
  }[];
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
      outboundText: string;
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
