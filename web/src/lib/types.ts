export type FlorenceSyncPhase =
  | "connect_google"
  | "account_connected"
  | "syncing_inbox"
  | "syncing_calendar"
  | "finding_family_sources"
  | "initial_sync_running"
  | "collect_household_profile"
  | "ready"
  | "attention_needed";

export type FlorenceConnectionSync = {
  initialSyncState: "pending" | "queued" | "running" | "ready" | "attention_needed";
  initialSyncCompletedAt: string | null;
  queuedAt: string | null;
  startedAt: string | null;
  phase: FlorenceSyncPhase;
  lastSyncStatus: string | null;
  lastSyncCompletedAt: string | null;
  lastSyncError: string | null;
  gmailLastSyncedAt: string | null;
  calendarLastSyncedAt: string | null;
  gmailItemCount: number;
  calendarItemCount: number;
  candidateCount: number;
};

export type FlorenceGoogleConnection = {
  id: string;
  householdId: string;
  memberId: string;
  email: string;
  connectedScopes: string[];
  active: boolean;
  primaryWebAccount: boolean;
  metadata: Record<string, unknown>;
  sync: FlorenceConnectionSync;
};

export type FlorenceChild = {
  id: string;
  fullName: string;
  birthdate: string | null;
  metadata: Record<string, unknown>;
};

export type FlorenceProfileItem = {
  id: string;
  kind: string;
  label: string;
  memberId: string | null;
  childId: string | null;
  metadata: Record<string, unknown>;
};

export type FlorenceSuggestion = {
  label: string;
  detail?: string;
  selected?: boolean;
  metadata?: Record<string, unknown>;
};

export type FlorenceCandidatePreview = {
  id: string;
  sourceKind: string;
  sourceIdentifier: string;
  title: string;
  summary: string;
  state: string;
  confidenceBps: number;
  requiresConfirmation: boolean;
  metadata: Record<string, unknown>;
};

export type FlorenceSetupResponse = {
  ok: true;
  household: {
    id: string;
    name: string;
    timezone: string;
    settings: Record<string, unknown>;
  };
  member: {
    id: string;
    householdId: string;
    displayName: string;
    role: string;
    metadata: Record<string, unknown>;
  };
  session: {
    householdId: string;
    memberId: string;
    threadId: string;
    stage: string;
    variant: string;
    googleConnected: boolean;
    isComplete: boolean;
  };
  setup: {
    phase: FlorenceSyncPhase;
    missing: string[];
    googleConnected: boolean;
    initialSyncComplete: boolean;
    requiredProfileComplete: boolean;
    readyForChat: boolean;
    requiredFields: {
      kids: boolean;
      schools: boolean;
      activities: boolean;
    };
  };
  sync: {
    primaryConnectionId: string | null;
    primary: FlorenceConnectionSync;
    connections: FlorenceGoogleConnection[];
  };
  profile: {
    children: FlorenceChild[];
    schools: FlorenceProfileItem[];
    activities: FlorenceProfileItem[];
  };
  suggestions: {
    schools: FlorenceSuggestion[];
    activities: FlorenceSuggestion[];
    contacts: FlorenceSuggestion[];
  };
  preview: {
    candidates: FlorenceCandidatePreview[];
    candidateCount: number;
  };
  googleConnectUrl: string | null;
};

export type FlorenceSessionResponse = {
  ok: true;
  resolvedVia: string;
  authEmail: string | null;
  household: FlorenceSetupResponse["household"];
  member: FlorenceSetupResponse["member"];
  setup: FlorenceSetupResponse;
};

export type FlorenceConnectionsResponse = {
  ok: true;
  connections: FlorenceGoogleConnection[];
};

export type FlorenceSettingsResponse = {
  ok: true;
  household: FlorenceSetupResponse["household"];
  member: FlorenceSetupResponse["member"];
  managerProfile: Record<string, unknown>;
};

export type FlorenceConnectUrlResponse = {
  ok: true;
  connectUrl: string;
};
