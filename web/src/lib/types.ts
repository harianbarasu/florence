export type FlorenceSyncPhase =
  | "web_consent"
  | "connect_google"
  | "account_connected"
  | "syncing_inbox"
  | "syncing_calendar"
  | "finding_family_sources"
  | "classify_calendars"
  | "collect_household_profile"
  | "collect_top_priorities"
  | "collect_trust_defaults"
  | "initial_sync_running"
  | "activation_review"
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
  calendarClassificationComplete: boolean;
  availableCalendars: FlorenceGoogleCalendar[];
  metadata: Record<string, unknown>;
  sync: FlorenceConnectionSync;
};

export type FlorenceGoogleCalendar = {
  id: string;
  summary: string;
  timezone: string;
  accessRole: string | null;
  primary: boolean;
  selected: boolean;
  hidden: boolean;
  usageMode: "planning_and_conflicts" | "conflicts_only" | "ignore" | null;
  detailVisibility: "full_details" | "busy_only" | null;
  configured: boolean;
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

export type FlorenceHouseholdAdult = {
  id: string;
  householdId: string;
  displayName: string;
  role: string;
  status: string;
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

export type FlorenceSetupPreferences = {
  consent: {
    termsAcceptedAt: string | null;
    privacyAcceptedAt: string | null;
  };
  priorities: {
    topPriorities: string[];
    topPriorityOther: string | null;
    painPoints: string[];
    painPointOther: string | null;
    updatedAt: string | null;
  };
  trustDefaults: {
    allowGoogleDataProcessing: boolean | null;
    allowHouseholdLogisticsSharing: boolean | null;
    privateCalendarHandling: "conflicts_only" | "full_details" | null;
    askBeforeSensitiveShare: boolean | null;
    updatedAt: string | null;
  };
  activation: {
    action: string | null;
    completedAt: string | null;
  };
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
    calendarClassificationComplete: boolean;
    topPrioritiesComplete: boolean;
    trustDefaultsComplete: boolean;
    activationComplete: boolean;
    readyForChat: boolean;
    requiredFields: {
      consent: boolean;
      googleAccount: boolean;
      calendarClassification: boolean;
      kids: boolean;
      schools: boolean;
      activities: boolean;
      topPriorities: boolean;
      trustDefaults: boolean;
      initialSync: boolean;
      activationReview: boolean;
    };
  };
  sync: {
    primaryConnectionId: string | null;
    primary: FlorenceConnectionSync;
    connections: FlorenceGoogleConnection[];
  };
  profile: {
    adults: FlorenceHouseholdAdult[];
    children: FlorenceChild[];
    schools: FlorenceProfileItem[];
    activities: FlorenceProfileItem[];
  };
  preferences: FlorenceSetupPreferences;
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

export type FlorenceReviewResponse = {
  ok: true;
  household: FlorenceSetupResponse["household"];
  member: FlorenceSetupResponse["member"];
  setup: FlorenceSetupResponse["setup"];
  counts: {
    total: number;
    pending: number;
    confirmed: number;
    rejected: number;
    quarantined: number;
  };
  nextPrompt: {
    candidateId: string;
    text: string;
  } | null;
  candidates: FlorenceCandidatePreview[];
};

export type FlorenceCalendarEvent = {
  id: string;
  title: string;
  startsAt: string | null;
  endsAt: string | null;
  timezone: string | null;
  allDay: boolean;
  location: string | null;
  description: string | null;
  sourceCandidateId: string | null;
  status: string;
  metadata: Record<string, unknown>;
};

export type FlorenceCalendarResponse = {
  ok: true;
  household: FlorenceSetupResponse["household"];
  member: FlorenceSetupResponse["member"];
  setup: FlorenceSetupResponse["setup"];
  range: {
    start: string | null;
    end: string | null;
  };
  counts: {
    total: number;
    confirmed: number;
    tentative: number;
    cancelled: number;
  };
  events: FlorenceCalendarEvent[];
};
