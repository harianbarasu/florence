"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ArrowRight,
  CalendarDays,
  CheckCircle2,
  CircleAlert,
  ExternalLink,
  LoaderCircle,
  Mail,
  Plus,
  RefreshCw,
  ShieldCheck,
  Sparkles,
  Trash2,
  Users,
} from "lucide-react";
import { toast } from "sonner";
import {
  FlorenceApiError,
  getSetup,
  saveReviewDecision,
  saveSetupProfile,
  startGoogleConnect,
} from "@/lib/florence-api";
import { withToken } from "@/lib/routes";
import type {
  FlorenceCandidatePreview,
  FlorenceGoogleConnection,
  FlorenceSetupResponse,
  FlorenceSyncPhase,
} from "@/lib/types";
import { cn, splitLines } from "@/lib/utils";
import { Alert } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { Textarea } from "@/components/ui/textarea";

type SetupStepKey =
  | "web_consent"
  | "connect_google"
  | "classify_calendars"
  | "collect_household_profile"
  | "collect_top_priorities"
  | "collect_trust_defaults"
  | "initial_sync_running"
  | "activation_review"
  | "ready"
  | "attention_needed";

type EditableChild = {
  name: string;
  details: string;
};

type CalendarSelectionDraft = Record<
  string,
  {
    connectionId: string;
    calendarId: string;
    usageMode: "planning_and_conflicts" | "conflicts_only" | "ignore";
    detailVisibility: "full_details" | "busy_only" | null;
  }
>;

const priorityOptions = [
  "school schedule changes",
  "pickups and dropoffs",
  "sports and activity logistics",
  "calendar conflicts",
  "reminders and deadlines",
  "weekend planning",
  "keeping both adults in sync",
  "who is responsible for what",
  "birthday parties and social events",
  "school forms and admin",
  "camp logistics",
  "travel planning",
  "meal planning",
  "after-school coordination",
  "inbox triage for family stuff",
] as const;

const painPointOptions = [
  "I miss schedule changes",
  "I forget forms and deadlines",
  "my partner and I get out of sync",
  "pickups get confusing",
  "activities conflict",
  "too much important info is buried in email",
  "I do not know what Florence should track",
] as const;

function calendarDraftKey(connectionId: string, calendarId: string) {
  return `${connectionId}::${calendarId}`;
}

function initializeCalendarSelections(connections: FlorenceGoogleConnection[]): CalendarSelectionDraft {
  const draft: CalendarSelectionDraft = {};

  for (const connection of connections) {
    for (const calendar of connection.availableCalendars) {
      if (calendar.hidden) {
        continue;
      }

      const usageMode =
        calendar.usageMode ??
        (calendar.primary || calendar.selected ? "planning_and_conflicts" : "ignore");
      const detailVisibility =
        usageMode === "ignore"
          ? null
          : calendar.detailVisibility ??
            (usageMode === "conflicts_only" ? "busy_only" : "full_details");

      draft[calendarDraftKey(connection.id, calendar.id)] = {
        connectionId: connection.id,
        calendarId: calendar.id,
        usageMode,
        detailVisibility,
      };
    }
  }

  return draft;
}

function normalizeStep(phase: FlorenceSyncPhase): SetupStepKey {
  if (
    phase === "account_connected" ||
    phase === "syncing_inbox" ||
    phase === "syncing_calendar" ||
    phase === "finding_family_sources"
  ) {
    return "initial_sync_running";
  }
  return phase;
}

function resolveCurrentStep(setup: FlorenceSetupResponse["setup"]): SetupStepKey {
  if (setup.readyForChat) {
    return "ready";
  }

  if (setup.phase === "attention_needed") {
    if (setup.requiredFields.consent) {
      return "web_consent";
    }
    if (setup.requiredFields.googleAccount) {
      return "connect_google";
    }
    if (setup.requiredFields.calendarClassification) {
      return "classify_calendars";
    }
    if (setup.requiredFields.kids || setup.requiredFields.schools || setup.requiredFields.activities) {
      return "collect_household_profile";
    }
    if (setup.requiredFields.topPriorities) {
      return "collect_top_priorities";
    }
    if (setup.requiredFields.trustDefaults) {
      return "collect_trust_defaults";
    }
    if (setup.requiredFields.initialSync) {
      return "initial_sync_running";
    }
    if (setup.requiredFields.activationReview) {
      return "activation_review";
    }
    return "attention_needed";
  }

  return normalizeStep(setup.phase);
}

function buildSteps(data: FlorenceSetupResponse) {
  const { requiredFields } = data.setup;
  return [
    { key: "web_consent" as const, label: "Consent", complete: !requiredFields.consent },
    { key: "connect_google" as const, label: "Connect Google", complete: !requiredFields.googleAccount },
    {
      key: "classify_calendars" as const,
      label: "Classify calendars",
      complete: !requiredFields.calendarClassification,
    },
    {
      key: "collect_household_profile" as const,
      label: "Household basics",
      complete: !requiredFields.kids && !requiredFields.schools && !requiredFields.activities,
    },
    {
      key: "collect_top_priorities" as const,
      label: "Top priorities",
      complete: !requiredFields.topPriorities,
    },
    {
      key: "collect_trust_defaults" as const,
      label: "Trust defaults",
      complete: !requiredFields.trustDefaults,
    },
    {
      key: "initial_sync_running" as const,
      label: "First sync",
      complete: !requiredFields.initialSync,
    },
    {
      key: "activation_review" as const,
      label: "Review",
      complete: !requiredFields.activationReview,
    },
  ];
}

function progressForSteps(steps: ReturnType<typeof buildSteps>) {
  const completed = steps.filter((step) => step.complete).length;
  return Math.round((completed / steps.length) * 100);
}

function humanizePhase(value: string) {
  return value.replaceAll("_", " ");
}

function formatTimestamp(value: string | null) {
  if (!value) {
    return "Not yet";
  }

  try {
    return new Intl.DateTimeFormat(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    }).format(new Date(value));
  } catch {
    return value;
  }
}

function suggestionButtonTone(selected: boolean) {
  return selected ? "secondary" : "outline";
}

function candidateTone(candidate: FlorenceCandidatePreview) {
  if (candidate.state === "confirmed") {
    return "success" as const;
  }
  if (candidate.state === "rejected") {
    return "outline" as const;
  }
  return "warning" as const;
}

function countConfiguredCalendars(connections: FlorenceGoogleConnection[]) {
  return connections.reduce(
    (total, connection) =>
      total + connection.availableCalendars.filter((calendar) => calendar.configured && !calendar.hidden).length,
    0,
  );
}

export function SetupScreen({ token }: { token?: string }) {
  const router = useRouter();
  const queryClient = useQueryClient();
  const setupQuery = useQuery({
    queryKey: ["florence", "setup", token],
    queryFn: () => getSetup(token),
  });
  const data = setupQuery.data;

  const [initializedKey, setInitializedKey] = useState<string | null>(null);
  const [acceptTerms, setAcceptTerms] = useState(false);
  const [acceptPrivacy, setAcceptPrivacy] = useState(false);
  const [parentDisplayName, setParentDisplayName] = useState("");
  const [children, setChildren] = useState<EditableChild[]>([{ name: "", details: "" }]);
  const [schoolsText, setSchoolsText] = useState("");
  const [activitiesText, setActivitiesText] = useState("");
  const [calendarSelections, setCalendarSelections] = useState<CalendarSelectionDraft>({});
  const [selectedPriorities, setSelectedPriorities] = useState<string[]>([]);
  const [topPriorityOther, setTopPriorityOther] = useState("");
  const [selectedPainPoints, setSelectedPainPoints] = useState<string[]>([]);
  const [painPointOther, setPainPointOther] = useState("");
  const [allowGoogleDataProcessing, setAllowGoogleDataProcessing] = useState(true);
  const [allowHouseholdLogisticsSharing, setAllowHouseholdLogisticsSharing] = useState(true);
  const [askBeforeSensitiveShare, setAskBeforeSensitiveShare] = useState(true);
  const [privateCalendarHandling, setPrivateCalendarHandling] = useState<"conflicts_only" | "full_details">(
    "conflicts_only",
  );

  useEffect(() => {
    if (data?.setup.readyForChat) {
      router.replace(withToken("/calendar", token));
    }
  }, [data?.setup.readyForChat, router, token]);

  useEffect(() => {
    if (!data) {
      return;
    }

    const nextKey = JSON.stringify({
      householdId: data.household.id,
      memberId: data.member.id,
      phase: data.setup.phase,
      connections: data.sync.connections.map((connection) => ({
        id: connection.id,
        calendars: connection.availableCalendars.length,
        classification: connection.calendarClassificationComplete,
      })),
      children: data.profile.children.length,
      schools: data.profile.schools.length,
      activities: data.profile.activities.length,
      prioritiesUpdatedAt: data.preferences.priorities.updatedAt,
      trustUpdatedAt: data.preferences.trustDefaults.updatedAt,
      consent: data.preferences.consent,
      activationCompletedAt: data.preferences.activation.completedAt,
    });

    if (initializedKey === nextKey) {
      return;
    }

    setAcceptTerms(Boolean(data.preferences.consent.termsAcceptedAt));
    setAcceptPrivacy(Boolean(data.preferences.consent.privacyAcceptedAt));
    setParentDisplayName(data.member.displayName || "");
    setChildren(
      data.profile.children.length
        ? data.profile.children.map((child) => ({
            name: child.fullName,
            details: String(child.metadata?.details || ""),
          }))
        : [{ name: "", details: "" }],
    );
    setSchoolsText(data.profile.schools.map((item) => item.label).join("\n"));
    setActivitiesText(data.profile.activities.map((item) => item.label).join("\n"));
    setCalendarSelections(initializeCalendarSelections(data.sync.connections));
    setSelectedPriorities(data.preferences.priorities.topPriorities);
    setTopPriorityOther(data.preferences.priorities.topPriorityOther || "");
    setSelectedPainPoints(data.preferences.priorities.painPoints);
    setPainPointOther(data.preferences.priorities.painPointOther || "");
    setAllowGoogleDataProcessing(data.preferences.trustDefaults.allowGoogleDataProcessing ?? true);
    setAllowHouseholdLogisticsSharing(data.preferences.trustDefaults.allowHouseholdLogisticsSharing ?? true);
    setAskBeforeSensitiveShare(data.preferences.trustDefaults.askBeforeSensitiveShare ?? true);
    setPrivateCalendarHandling(data.preferences.trustDefaults.privateCalendarHandling || "conflicts_only");
    setInitializedKey(nextKey);
  }, [data, initializedKey]);

  const connectMutation = useMutation({
    mutationFn: () => startGoogleConnect(token),
    onSuccess: (payload) => {
      window.location.assign(payload.connectUrl);
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Unable to start Google connection");
    },
  });

  const setupMutation = useMutation({
    mutationFn: (variables: { payload: Record<string, unknown>; successMessage: string }) =>
      saveSetupProfile({
        ...(token ? { token } : {}),
        ...variables.payload,
      }),
    onSuccess: (payload, variables) => {
      queryClient.setQueryData(["florence", "setup", token], payload);
      queryClient.invalidateQueries({ queryKey: ["florence", "review", token] });
      queryClient.invalidateQueries({ queryKey: ["florence", "connections", token] });
      queryClient.invalidateQueries({ queryKey: ["florence", "calendar", token] });
      toast.success(variables.successMessage);
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Unable to save this step");
    },
  });

  const reviewMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) =>
      saveReviewDecision({
        ...(token ? { token } : {}),
        ...payload,
      }),
    onSuccess: (payload) => {
      queryClient.setQueryData(["florence", "review", token], payload);
      queryClient.invalidateQueries({ queryKey: ["florence", "setup", token] });
      queryClient.invalidateQueries({ queryKey: ["florence", "calendar", token] });
      if (payload.setup.readyForChat) {
        router.replace(withToken("/calendar", token));
        return;
      }
      toast.success("Review updated.");
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Unable to save review decision");
    },
  });

  const currentStep = data ? resolveCurrentStep(data.setup) : "web_consent";
  const steps = data ? buildSteps(data) : [];
  const progressValue = data ? progressForSteps(steps) : 0;
  const visibleCalendars = data
    ? data.sync.connections.flatMap((connection) =>
        connection.availableCalendars
          .filter((calendar) => !calendar.hidden)
          .map((calendar) => ({ connection, calendar })),
      )
    : [];
  const canSaveProfile = useMemo(
    () =>
      children.some((child) => child.name.trim()) &&
      splitLines(schoolsText).length > 0 &&
      splitLines(activitiesText).length > 0,
    [activitiesText, children, schoolsText],
  );

  if (setupQuery.isLoading) {
    return (
      <div className="grid gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Loading Florence setup</CardTitle>
            <CardDescription>Pulling your onboarding state and Google sync status.</CardDescription>
          </CardHeader>
          <CardContent className="grid gap-4">
            <Skeleton className="h-3 w-full" />
            <div className="grid gap-3 lg:grid-cols-4">
              <Skeleton className="h-24" />
              <Skeleton className="h-24" />
              <Skeleton className="h-24" />
              <Skeleton className="h-24" />
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (setupQuery.error) {
    const error = setupQuery.error as FlorenceApiError;
    return (
      <Card>
        <CardHeader>
          <CardTitle>Setup couldn&apos;t be loaded</CardTitle>
          <CardDescription>
            {error.message === "unknown_web_google_identity"
              ? "This Google account is not linked to a Florence household yet."
              : "Florence returned an error while loading setup."}
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4">
          <Alert tone={error.message === "unknown_web_google_identity" ? "warning" : "destructive"}>
            {error.message === "unknown_web_google_identity"
              ? "Open a Florence setup link from text, then sign in with the matching Google account."
              : error.message}
          </Alert>
          <Button variant="outline" onClick={() => setupQuery.refetch()}>
            Try again
          </Button>
        </CardContent>
      </Card>
    );
  }

  if (!data) {
    return null;
  }

  const saveConsent = () => {
    setupMutation.mutate({
      payload: {
        consent: {
          acceptTerms,
          acceptPrivacy,
        },
      },
      successMessage: "Consent saved.",
    });
  };

  const saveCalendarSelections = () => {
    setupMutation.mutate({
      payload: {
        calendarSelections: Object.values(calendarSelections).map((selection) => ({
          connectionId: selection.connectionId,
          calendarId: selection.calendarId,
          usageMode: selection.usageMode,
          detailVisibility: selection.detailVisibility,
        })),
      },
      successMessage: "Calendar policies saved.",
    });
  };

  const saveProfile = () => {
    setupMutation.mutate({
      payload: {
        parentDisplayName,
        children: children
          .map((child) => ({
            name: child.name.trim(),
            details: child.details.trim(),
          }))
          .filter((child) => child.name),
        schools: splitLines(schoolsText),
        activities: splitLines(activitiesText),
      },
      successMessage: "Household basics saved.",
    });
  };

  const savePriorities = () => {
    setupMutation.mutate({
      payload: {
        topPriorities: selectedPriorities,
        topPriorityOther: topPriorityOther.trim() || null,
        painPoints: selectedPainPoints,
        painPointOther: painPointOther.trim() || null,
      },
      successMessage: "Top priorities saved.",
    });
  };

  const saveTrustDefaults = () => {
    setupMutation.mutate({
      payload: {
        trustDefaults: {
          allowGoogleDataProcessing,
          allowHouseholdLogisticsSharing,
          privateCalendarHandling,
          askBeforeSensitiveShare,
        },
      },
      successMessage: "Trust defaults saved.",
    });
  };

  const renderStepCard = () => {
    if (currentStep === "web_consent") {
      return (
        <Card>
          <CardHeader>
            <CardTitle>Welcome to Florence</CardTitle>
            <CardDescription>
              Florence helps your household stay on top of school, sports, calendar conflicts, and the family logistics
              that usually hide in Gmail and shared calendars.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-5">
            <Alert>
              Florence needs consent to process your Google data and turn it into structured family planning. The raw
              source data stays behind the scenes.
            </Alert>

            <div className="grid gap-4">
              <label className="flex items-start gap-3 rounded-2xl border p-4">
                <input
                  type="checkbox"
                  className="mt-1 h-4 w-4 accent-[var(--primary)]"
                  checked={acceptTerms}
                  onChange={(event) => setAcceptTerms(event.target.checked)}
                />
                <div className="space-y-1">
                  <div className="font-medium">I accept the Terms of Service</div>
                  <div className="text-sm text-muted-foreground">This allows Florence to operate as your household agent.</div>
                </div>
              </label>

              <label className="flex items-start gap-3 rounded-2xl border p-4">
                <input
                  type="checkbox"
                  className="mt-1 h-4 w-4 accent-[var(--primary)]"
                  checked={acceptPrivacy}
                  onChange={(event) => setAcceptPrivacy(event.target.checked)}
                />
                <div className="space-y-1">
                  <div className="font-medium">I accept the Privacy Policy</div>
                  <div className="text-sm text-muted-foreground">
                    Florence may privately process Gmail and Calendar to find family logistics, deadlines, and conflicts.
                  </div>
                </div>
              </label>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <Button onClick={saveConsent} disabled={!acceptTerms || !acceptPrivacy || setupMutation.isPending}>
                {setupMutation.isPending ? "Saving..." : "Continue"}
                <ArrowRight className="h-4 w-4" />
              </Button>
              <div className="text-sm text-muted-foreground">You can revisit this from Settings later.</div>
            </div>
          </CardContent>
        </Card>
      );
    }

    if (currentStep === "connect_google") {
      return (
        <Card>
          <CardHeader>
            <CardTitle>Connect Google</CardTitle>
            <CardDescription>
              Florence is Google-only right now. Connect the Google account you want Florence to use for Gmail and Calendar.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-5">
            {data.setup.phase === "attention_needed" ? (
              <Alert tone="warning">
                {data.sync.primary.lastSyncError || "Florence needs the Google connection to be retried."}
              </Alert>
            ) : null}

            <div className="rounded-[1.5rem] border bg-accent/35 p-5">
              <div className="flex flex-wrap items-center justify-between gap-4">
                <div className="space-y-1">
                  <div className="text-sm font-semibold">Google handoff</div>
                  <div className="text-sm text-muted-foreground">
                    The next step opens Google consent so Florence can scan Gmail and Calendar for family logistics.
                  </div>
                </div>
                <Button onClick={() => connectMutation.mutate()} disabled={connectMutation.isPending}>
                  {connectMutation.isPending ? "Opening Google..." : "Connect Google"}
                  <ExternalLink className="h-4 w-4" />
                </Button>
              </div>
            </div>

            {data.googleConnectUrl ? (
              <div className="text-sm text-muted-foreground">
                If the button above stalls, use the direct handoff{" "}
                <Link href={data.googleConnectUrl} className="font-medium text-primary underline-offset-4 hover:underline">
                  here
                </Link>
                .
              </div>
            ) : null}
          </CardContent>
        </Card>
      );
    }

    if (currentStep === "classify_calendars") {
      return (
        <Card>
          <CardHeader>
            <CardTitle>Classify connected calendars</CardTitle>
            <CardDescription>
              Tell Florence which calendars are for family planning, which should only block off time, and which should be ignored.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-5">
            {!visibleCalendars.length ? (
              <Alert tone="warning">
                Florence does not have a calendar catalog yet. Refresh the page or reconnect the Google account if this does not populate.
              </Alert>
            ) : null}

            <div className="grid gap-4">
              {data.sync.connections.map((connection) => {
                const calendars = connection.availableCalendars.filter((calendar) => !calendar.hidden);
                if (!calendars.length) {
                  return null;
                }

                return (
                  <div key={connection.id} className="rounded-[1.5rem] border p-5">
                    <div className="mb-4 flex flex-wrap items-center gap-3">
                      <div className="font-medium">{connection.email}</div>
                      {connection.primaryWebAccount ? <Badge variant="secondary">Primary account</Badge> : null}
                    </div>

                    <div className="grid gap-4">
                      {calendars.map((calendar) => {
                        const selection = calendarSelections[calendarDraftKey(connection.id, calendar.id)];
                        if (!selection) {
                          return null;
                        }

                        return (
                          <div key={calendar.id} className="rounded-2xl border bg-card/70 p-4">
                            <div className="mb-3 flex flex-wrap items-center gap-2">
                              <div className="font-medium">{calendar.summary}</div>
                              {calendar.primary ? <Badge variant="secondary">Primary</Badge> : null}
                              {calendar.configured ? <Badge variant="outline">Saved</Badge> : null}
                            </div>

                            <div className="grid gap-3">
                              <div className="grid gap-2">
                                <div className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">
                                  Usage mode
                                </div>
                                <div className="flex flex-wrap gap-2">
                                  {[
                                    ["planning_and_conflicts", "Planning + conflicts"],
                                    ["conflicts_only", "Conflicts only"],
                                    ["ignore", "Ignore"],
                                  ].map(([value, label]) => (
                                    <Button
                                      key={value}
                                      type="button"
                                      size="sm"
                                      variant={selection.usageMode === value ? "default" : "outline"}
                                      onClick={() =>
                                        setCalendarSelections((current) => {
                                          const detailVisibility =
                                            value === "ignore"
                                              ? null
                                              : value === "conflicts_only"
                                                ? "busy_only"
                                                : current[calendarDraftKey(connection.id, calendar.id)]
                                                    ?.detailVisibility === "busy_only"
                                                  ? "full_details"
                                                  : current[calendarDraftKey(connection.id, calendar.id)]
                                                      ?.detailVisibility || "full_details";

                                          return {
                                            ...current,
                                            [calendarDraftKey(connection.id, calendar.id)]: {
                                              ...selection,
                                              usageMode: value as
                                                | "planning_and_conflicts"
                                                | "conflicts_only"
                                                | "ignore",
                                              detailVisibility,
                                            },
                                          };
                                        })
                                      }
                                    >
                                      {label}
                                    </Button>
                                  ))}
                                </div>
                              </div>

                              {selection.usageMode !== "ignore" ? (
                                <div className="grid gap-2">
                                  <div className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">
                                    Detail level
                                  </div>
                                  <div className="flex flex-wrap gap-2">
                                    {[
                                      ["full_details", "Full details"],
                                      ["busy_only", "Busy only"],
                                    ].map(([value, label]) => (
                                      <Button
                                        key={value}
                                        type="button"
                                        size="sm"
                                        variant={selection.detailVisibility === value ? "default" : "outline"}
                                        onClick={() =>
                                          setCalendarSelections((current) => ({
                                            ...current,
                                            [calendarDraftKey(connection.id, calendar.id)]: {
                                              ...selection,
                                              detailVisibility: value as "full_details" | "busy_only",
                                            },
                                          }))
                                        }
                                      >
                                        {label}
                                      </Button>
                                    ))}
                                  </div>
                                  {selection.usageMode === "conflicts_only" ? (
                                    <div className="text-sm text-muted-foreground">
                                      Conflict-only calendars are fetched for scan accounting and busy-block awareness, not household candidate creation.
                                    </div>
                                  ) : null}
                                </div>
                              ) : null}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              })}
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <Button
                onClick={saveCalendarSelections}
                disabled={!visibleCalendars.length || setupMutation.isPending}
              >
                {setupMutation.isPending ? "Saving..." : "Save calendar policies"}
              </Button>
              <Button variant="outline" onClick={() => setupQuery.refetch()}>
                <RefreshCw className="h-4 w-4" />
                Refresh calendars
              </Button>
            </div>
          </CardContent>
        </Card>
      );
    }

    if (currentStep === "collect_household_profile") {
      return (
        <Card>
          <CardHeader>
            <CardTitle>Household basics</CardTitle>
            <CardDescription>
              Florence needs the minimum household graph before it should act like a tuned family agent.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-6">
            <div className="grid gap-2">
              <Label htmlFor="parent-name">Your display name</Label>
              <Input
                id="parent-name"
                value={parentDisplayName}
                onChange={(event) => setParentDisplayName(event.target.value)}
                placeholder="e.g. Jackson"
              />
            </div>

            <div className="grid gap-4">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <Label>Children</Label>
                  <div className="mt-1 text-sm text-muted-foreground">Start with the children Florence should ground against schools and activities.</div>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setChildren((current) => [...current, { name: "", details: "" }])}
                >
                  <Plus className="h-4 w-4" />
                  Add child
                </Button>
              </div>

              <div className="grid gap-3">
                {children.map((child, index) => (
                  <div key={`${index}-${child.name}`} className="rounded-2xl border p-4">
                    <div className="mb-3 flex items-center justify-between gap-3">
                      <div className="font-medium">Child {index + 1}</div>
                      {children.length > 1 ? (
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => setChildren((current) => current.filter((_, itemIndex) => itemIndex !== index))}
                        >
                          <Trash2 className="h-4 w-4" />
                          Remove
                        </Button>
                      ) : null}
                    </div>
                    <div className="grid gap-3 sm:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
                      <div className="grid gap-2">
                        <Label>Child name</Label>
                        <Input
                          value={child.name}
                          onChange={(event) =>
                            setChildren((current) =>
                              current.map((item, itemIndex) =>
                                itemIndex === index ? { ...item, name: event.target.value } : item,
                              ),
                            )
                          }
                          placeholder="e.g. Violet"
                        />
                      </div>
                      <div className="grid gap-2">
                        <Label>Optional details</Label>
                        <Input
                          value={child.details}
                          onChange={(event) =>
                            setChildren((current) =>
                              current.map((item, itemIndex) =>
                                itemIndex === index ? { ...item, details: event.target.value } : item,
                              ),
                            )
                          }
                          placeholder="e.g. 2nd grade, soccer"
                        />
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="grid gap-2">
              <Label htmlFor="schools">Schools or daycares</Label>
              <Textarea
                id="schools"
                value={schoolsText}
                onChange={(event) => setSchoolsText(event.target.value)}
                placeholder={"e.g. Westchester Elementary\nBrightwheel"}
              />
              {data.suggestions.schools.length ? (
                <div className="flex flex-wrap gap-2">
                  {data.suggestions.schools.map((suggestion) => (
                    <Button
                      key={suggestion.label}
                      type="button"
                      size="sm"
                      variant={suggestionButtonTone(suggestion.selected || false)}
                      onClick={() => {
                        const next = new Set(splitLines(schoolsText));
                        next.add(suggestion.label);
                        setSchoolsText(Array.from(next).join("\n"));
                      }}
                    >
                      {suggestion.label}
                    </Button>
                  ))}
                </div>
              ) : null}
            </div>

            <div className="grid gap-2">
              <Label htmlFor="activities">Activities</Label>
              <Textarea
                id="activities"
                value={activitiesText}
                onChange={(event) => setActivitiesText(event.target.value)}
                placeholder={"e.g. Baseball\nPiano lessons"}
              />
              {data.suggestions.activities.length ? (
                <div className="flex flex-wrap gap-2">
                  {data.suggestions.activities.map((suggestion) => (
                    <Button
                      key={suggestion.label}
                      type="button"
                      size="sm"
                      variant={suggestionButtonTone(suggestion.selected || false)}
                      onClick={() => {
                        const next = new Set(splitLines(activitiesText));
                        next.add(suggestion.label);
                        setActivitiesText(Array.from(next).join("\n"));
                      }}
                    >
                      {suggestion.label}
                    </Button>
                  ))}
                </div>
              ) : null}
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <Button onClick={saveProfile} disabled={!canSaveProfile || setupMutation.isPending}>
                {setupMutation.isPending ? "Saving..." : "Save household basics"}
              </Button>
              <div className="text-sm text-muted-foreground">
                Adults: {data.profile.adults.map((adult) => adult.displayName).join(", ") || "No adults listed yet"}
              </div>
            </div>
          </CardContent>
        </Card>
      );
    }

    if (currentStep === "collect_top_priorities") {
      return (
        <Card>
          <CardHeader>
            <CardTitle>Top priorities</CardTitle>
            <CardDescription>
              This is onboarding, not just personalization. Florence uses it to rank what it surfaces first and it gives the company real demand signal.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-6">
            <div className="grid gap-3">
              <div className="flex items-center justify-between gap-3">
                <Label>What do you want the most help with right away?</Label>
                <Badge variant="outline">Pick up to 3</Badge>
              </div>
              <div className="flex flex-wrap gap-2">
                {priorityOptions.map((option) => {
                  const active = selectedPriorities.includes(option);
                  return (
                    <Button
                      key={option}
                      type="button"
                      size="sm"
                      variant={active ? "default" : "outline"}
                      onClick={() =>
                        setSelectedPriorities((current) => {
                          if (current.includes(option)) {
                            return current.filter((item) => item !== option);
                          }
                          if (current.length >= 3) {
                            return current;
                          }
                          return [...current, option];
                        })
                      }
                    >
                      {option}
                    </Button>
                  );
                })}
              </div>
              <div className="grid gap-2">
                <Label htmlFor="priority-other">Other priority</Label>
                <Input
                  id="priority-other"
                  value={topPriorityOther}
                  onChange={(event) => setTopPriorityOther(event.target.value)}
                  placeholder="Something specific we should hear from users"
                />
              </div>
            </div>

            <div className="grid gap-3">
              <div className="flex items-center justify-between gap-3">
                <Label>Where do things fall through today?</Label>
                <Badge variant="outline">Pick up to 2</Badge>
              </div>
              <div className="flex flex-wrap gap-2">
                {painPointOptions.map((option) => {
                  const active = selectedPainPoints.includes(option);
                  return (
                    <Button
                      key={option}
                      type="button"
                      size="sm"
                      variant={active ? "default" : "outline"}
                      onClick={() =>
                        setSelectedPainPoints((current) => {
                          if (current.includes(option)) {
                            return current.filter((item) => item !== option);
                          }
                          if (current.length >= 2) {
                            return current;
                          }
                          return [...current, option];
                        })
                      }
                    >
                      {option}
                    </Button>
                  );
                })}
              </div>
              <div className="grid gap-2">
                <Label htmlFor="pain-point-other">Other pain point</Label>
                <Input
                  id="pain-point-other"
                  value={painPointOther}
                  onChange={(event) => setPainPointOther(event.target.value)}
                  placeholder="Anything else that regularly slips through"
                />
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <Button onClick={savePriorities} disabled={!selectedPriorities.length || setupMutation.isPending}>
                {setupMutation.isPending ? "Saving..." : "Save priorities"}
              </Button>
              <div className="text-sm text-muted-foreground">Florence will use this to rank the first review queue.</div>
            </div>
          </CardContent>
        </Card>
      );
    }

    if (currentStep === "collect_trust_defaults") {
      return (
        <Card>
          <CardHeader>
            <CardTitle>Trust defaults</CardTitle>
            <CardDescription>
              Set the household privacy pact before Florence starts surfacing structured family logistics.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-5">
            <label className="flex items-start gap-3 rounded-2xl border p-4">
              <input
                type="checkbox"
                className="mt-1 h-4 w-4 accent-[var(--primary)]"
                checked={allowGoogleDataProcessing}
                onChange={(event) => setAllowGoogleDataProcessing(event.target.checked)}
              />
              <div className="space-y-1">
                <div className="font-medium">Florence may privately process Gmail and Calendar</div>
                <div className="text-sm text-muted-foreground">This enables the inbox and calendar inference pipeline.</div>
              </div>
            </label>

            <label className="flex items-start gap-3 rounded-2xl border p-4">
              <input
                type="checkbox"
                className="mt-1 h-4 w-4 accent-[var(--primary)]"
                checked={allowHouseholdLogisticsSharing}
                onChange={(event) => setAllowHouseholdLogisticsSharing(event.target.checked)}
              />
              <div className="space-y-1">
                <div className="font-medium">Florence may share routine household logistics with household adults</div>
                <div className="text-sm text-muted-foreground">Think school updates, pickups, sports, and schedule changes.</div>
              </div>
            </label>

            <label className="flex items-start gap-3 rounded-2xl border p-4">
              <input
                type="checkbox"
                className="mt-1 h-4 w-4 accent-[var(--primary)]"
                checked={askBeforeSensitiveShare}
                onChange={(event) => setAskBeforeSensitiveShare(event.target.checked)}
              />
              <div className="space-y-1">
                <div className="font-medium">Ask before sharing sensitive child or adult-private information</div>
                <div className="text-sm text-muted-foreground">This is the default for anything beyond routine logistics.</div>
              </div>
            </label>

            <div className="grid gap-2 rounded-2xl border p-4">
              <Label>Private calendar handling</Label>
              <div className="flex flex-wrap gap-2">
                {[
                  ["conflicts_only", "Conflicts only"],
                  ["full_details", "Full details"],
                ].map(([value, label]) => (
                  <Button
                    key={value}
                    type="button"
                    size="sm"
                    variant={privateCalendarHandling === value ? "default" : "outline"}
                    onClick={() => setPrivateCalendarHandling(value as "conflicts_only" | "full_details")}
                  >
                    {label}
                  </Button>
                ))}
              </div>
              <div className="text-sm text-muted-foreground">
                Conflict-only keeps private events as a busy block without exposing titles or details.
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <Button onClick={saveTrustDefaults} disabled={setupMutation.isPending}>
                {setupMutation.isPending ? "Saving..." : "Save trust defaults"}
              </Button>
            </div>
          </CardContent>
        </Card>
      );
    }

    if (currentStep === "initial_sync_running" || currentStep === "attention_needed") {
      return (
        <Card>
          <CardHeader>
            <CardTitle>
              {data.setup.initialSyncComplete ? "First sync complete" : "First sync is running"}
            </CardTitle>
            <CardDescription>
              Florence is processing Gmail and Calendar in the background. People will leave this page, so Florence texts when the first pass is done.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-5">
            <Alert tone={data.setup.phase === "attention_needed" ? "warning" : "default"}>
              {data.setup.phase === "attention_needed"
                ? data.sync.primary.lastSyncError || "Florence needs attention before the first sync can finish."
                : "You can leave this page. Florence will text when the first Gmail and Calendar pass is ready and when it is time to come back to the web app."}
            </Alert>

            <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
              <div className="rounded-2xl border p-4">
                <div className="text-sm font-medium text-muted-foreground">Inbox scanned</div>
                <div className="mt-2 text-3xl font-semibold">{data.sync.primary.gmailItemCount}</div>
              </div>
              <div className="rounded-2xl border p-4">
                <div className="text-sm font-medium text-muted-foreground">Calendar scanned</div>
                <div className="mt-2 text-3xl font-semibold">{data.sync.primary.calendarItemCount}</div>
              </div>
              <div className="rounded-2xl border p-4">
                <div className="text-sm font-medium text-muted-foreground">Candidates surfaced</div>
                <div className="mt-2 text-3xl font-semibold">{data.preview.candidateCount}</div>
              </div>
              <div className="rounded-2xl border p-4">
                <div className="text-sm font-medium text-muted-foreground">Sync phase</div>
                <div className="mt-2 text-lg font-semibold">{humanizePhase(data.sync.primary.phase)}</div>
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <Button variant="outline" onClick={() => setupQuery.refetch()}>
                <RefreshCw className="h-4 w-4" />
                Refresh status
              </Button>
              <Button asChild>
                <Link href={withToken("/connections", token)}>
                  Open connections
                  <ArrowRight className="h-4 w-4" />
                </Link>
              </Button>
            </div>
          </CardContent>
        </Card>
      );
    }

    if (currentStep === "activation_review") {
      return (
        <Card>
          <CardHeader>
            <CardTitle>Review Florence&apos;s first findings</CardTitle>
            <CardDescription>
              This is the trust-building step. Confirm or reject a few items so Florence starts from the right model of your household.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-5">
            {!data.preview.candidates.length ? (
              <Alert>
                Florence has not surfaced review items yet. You can skip for now and come back from the Review page later.
              </Alert>
            ) : null}

            <div className="grid gap-3">
              {data.preview.candidates.map((candidate) => (
                <div key={candidate.id} className="rounded-2xl border p-4">
                  <div className="mb-2 flex flex-wrap items-center gap-2">
                    <div className="font-medium">{candidate.title}</div>
                    <Badge variant={candidateTone(candidate)}>{humanizePhase(candidate.state)}</Badge>
                  </div>
                  <p className="text-sm leading-6 text-muted-foreground">{candidate.summary}</p>
                  {candidate.state === "pending_review" ? (
                    <div className="mt-4 flex flex-wrap gap-2">
                      <Button
                        size="sm"
                        onClick={() =>
                          reviewMutation.mutate({
                            candidateId: candidate.id,
                            decision: "confirm",
                          })
                        }
                        disabled={reviewMutation.isPending}
                      >
                        Confirm
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() =>
                          reviewMutation.mutate({
                            candidateId: candidate.id,
                            decision: "reject",
                          })
                        }
                        disabled={reviewMutation.isPending}
                      >
                        Reject
                      </Button>
                    </div>
                  ) : null}
                </div>
              ))}
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <Button
                variant="outline"
                onClick={() => reviewMutation.mutate({ decision: "skip_activation" })}
                disabled={reviewMutation.isPending}
              >
                {reviewMutation.isPending ? "Saving..." : "Skip for now"}
              </Button>
              <Button asChild>
                <Link href={withToken("/review", token)}>
                  Open full review queue
                  <ArrowRight className="h-4 w-4" />
                </Link>
              </Button>
            </div>
          </CardContent>
        </Card>
      );
    }

    return (
      <Card>
        <CardHeader>
          <CardTitle>Florence is ready</CardTitle>
          <CardDescription>Opening your household calendar.</CardDescription>
        </CardHeader>
        <CardContent>
          <Button asChild>
            <Link href={withToken("/calendar", token)}>Open calendar</Link>
          </Button>
        </CardContent>
      </Card>
    );
  };

  return (
    <div className="grid gap-6">
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.2fr)_minmax(300px,0.8fr)]">
        <Card className="overflow-hidden">
          <CardHeader className="bg-[linear-gradient(135deg,rgba(28,91,122,0.08),rgba(176,106,51,0.05))]">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div className="space-y-2">
                <Badge variant={data.setup.phase === "attention_needed" ? "warning" : "outline"}>
                  {currentStep === "ready" ? "Ready" : humanizePhase(currentStep)}
                </Badge>
                <CardTitle className="text-2xl">Finish Florence onboarding</CardTitle>
                <CardDescription className="max-w-2xl">
                  Google-only setup now ends in the real product: Calendar, Review, Connections, and Settings.
                </CardDescription>
              </div>
              <div className="rounded-[1.25rem] border bg-background/75 px-4 py-3 text-right">
                <div className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">Progress</div>
                <div className="mt-1 text-3xl font-semibold">{progressValue}%</div>
              </div>
            </div>
          </CardHeader>
          <CardContent className="grid gap-5 pt-6">
            <Progress value={progressValue} />
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              {steps.map((step, index) => (
                <div
                  key={step.key}
                  className={cn(
                    "rounded-2xl border p-4 transition-colors",
                    step.key === currentStep ? "border-primary bg-primary/5" : "bg-card/70",
                  )}
                >
                  <div className="mb-2 flex items-center justify-between gap-3">
                    <div className="text-sm font-medium">
                      {index + 1}. {step.label}
                    </div>
                    <Badge
                      variant={
                        step.complete
                          ? "success"
                          : step.key === currentStep && data.setup.phase === "attention_needed"
                            ? "warning"
                            : "outline"
                      }
                    >
                      {step.complete ? "Done" : step.key === currentStep ? "Current" : "Queued"}
                    </Badge>
                  </div>
                </div>
              ))}
            </div>
            {data.setup.phase === "attention_needed" ? (
              <Alert tone="warning">
                Florence needs attention before onboarding can continue cleanly. The current blocker is{" "}
                {data.sync.primary.lastSyncError || "the Google sync or setup contract"}.
              </Alert>
            ) : null}
          </CardContent>
        </Card>

        <div className="grid gap-4">
          <Card>
            <CardContent className="grid gap-2 pt-6">
              <div className="flex items-center gap-3">
                <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-primary/10 text-primary">
                  <Mail className="h-5 w-5" />
                </div>
                <div>
                  <div className="text-sm font-semibold">Inbox</div>
                  <div className="text-2xl font-semibold">{data.sync.primary.gmailItemCount}</div>
                </div>
              </div>
              <p className="text-sm text-muted-foreground">Messages scanned from the primary Google account.</p>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="grid gap-2 pt-6">
              <div className="flex items-center gap-3">
                <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-primary/10 text-primary">
                  <CalendarDays className="h-5 w-5" />
                </div>
                <div>
                  <div className="text-sm font-semibold">Calendars</div>
                  <div className="text-2xl font-semibold">{countConfiguredCalendars(data.sync.connections)}</div>
                </div>
              </div>
              <p className="text-sm text-muted-foreground">Calendars currently classified for Florence.</p>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="grid gap-2 pt-6">
              <div className="flex items-center gap-3">
                <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-primary/10 text-primary">
                  <Users className="h-5 w-5" />
                </div>
                <div>
                  <div className="text-sm font-semibold">Household</div>
                  <div className="text-2xl font-semibold">{data.profile.children.length}</div>
                </div>
              </div>
              <p className="text-sm text-muted-foreground">Children currently grounded in Florence.</p>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="grid gap-2 pt-6">
              <div className="flex items-center gap-3">
                <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-primary/10 text-primary">
                  <ShieldCheck className="h-5 w-5" />
                </div>
                <div>
                  <div className="text-sm font-semibold">Sync status</div>
                  <div className="text-lg font-semibold">{humanizePhase(data.sync.primary.phase)}</div>
                </div>
              </div>
              <p className="text-sm text-muted-foreground">
                Last Gmail sync: {formatTimestamp(data.sync.primary.gmailLastSyncedAt)}
              </p>
            </CardContent>
          </Card>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.15fr)_minmax(300px,0.85fr)]">
        {renderStepCard()}

        <div className="grid gap-6">
          <Card>
            <CardHeader>
              <CardTitle>What Florence found so far</CardTitle>
              <CardDescription>Activation is built around concrete inferred items, not an abstract success score.</CardDescription>
            </CardHeader>
            <CardContent className="grid gap-3">
              {data.preview.candidates.length ? (
                data.preview.candidates.slice(0, 4).map((candidate) => (
                  <div key={candidate.id} className="rounded-2xl border p-4">
                    <div className="mb-1 flex items-center gap-2">
                      <Sparkles className="h-4 w-4 text-primary" />
                      <div className="font-medium">{candidate.title}</div>
                    </div>
                    <p className="text-sm leading-6 text-muted-foreground">{candidate.summary}</p>
                  </div>
                ))
              ) : (
                <Alert>Florence has not surfaced candidate items yet.</Alert>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>What happens next</CardTitle>
            </CardHeader>
            <CardContent className="grid gap-3 text-sm text-muted-foreground">
              <div>Calendar becomes the default home once setup is complete.</div>
              <div>Review becomes the trust surface for uncertain imports.</div>
              <div>Connections is where Google accounts and calendar policies live.</div>
              <div>Settings stays lightweight and secondary.</div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Quick links</CardTitle>
            </CardHeader>
            <CardContent className="grid gap-3">
              <Button asChild variant="outline">
                <Link href={withToken("/review", token)}>
                  Open review queue
                  <ArrowRight className="h-4 w-4" />
                </Link>
              </Button>
              <Button asChild variant="outline">
                <Link href={withToken("/connections", token)}>
                  Open connections
                  <ArrowRight className="h-4 w-4" />
                </Link>
              </Button>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
