"use client";

import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  CalendarDays,
  CheckCircle2,
  CircleAlert,
  ExternalLink,
  LoaderCircle,
  Mail,
  Plus,
  Sparkles,
  Users,
} from "lucide-react";
import { toast } from "sonner";
import {
  FlorenceApiError,
  getSetup,
  saveSetupProfile,
  startGoogleConnect,
} from "@/lib/florence-api";
import type { FlorenceSetupResponse } from "@/lib/types";
import { splitLines } from "@/lib/utils";
import { Alert } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import { Textarea } from "@/components/ui/textarea";

type EditableChild = {
  name: string;
  details: string;
};

function progressForSync(
  setup: FlorenceSetupResponse["setup"],
  phase: FlorenceSetupResponse["sync"]["primary"]["phase"],
) {
  if (!setup.googleConnected) {
    return 12;
  }
  if (phase === "account_connected") {
    return 28;
  }
  if (phase === "syncing_calendar") {
    return 46;
  }
  if (phase === "syncing_inbox") {
    return 64;
  }
  if (phase === "finding_family_sources" || setup.phase === "initial_sync_running") {
    return 82;
  }
  if (setup.phase === "collect_household_profile") {
    return 92;
  }
  if (setup.readyForChat) {
    return 100;
  }
  return 18;
}

function syncHeadline(data: FlorenceSetupResponse) {
  if (!data.setup.googleConnected) {
    return "Finishing the Google account handoff";
  }
  if (data.setup.phase === "attention_needed") {
    return "Florence needs attention before sync can finish";
  }
  if (!data.setup.initialSyncComplete) {
    return "Florence is pulling your inbox and calendar";
  }
  if (!data.setup.requiredProfileComplete) {
    return "Add the household details Florence needs to match cleanly";
  }
  return "Florence is ready for chat";
}

function syncBody(data: FlorenceSetupResponse) {
  if (!data.setup.googleConnected) {
    return "The web sign-in is complete. Florence should continue directly into Google consent for Gmail and Calendar without another Florence-owned step.";
  }
  if (data.setup.phase === "attention_needed") {
    return data.sync.primary.lastSyncError || "The connection needs to be retried.";
  }
  if (!data.setup.initialSyncComplete) {
    return "You can leave this page if needed. Florence will text you when the first pass is done.";
  }
  if (!data.setup.requiredProfileComplete) {
    return "Now that Florence has a first pass through the inbox and calendar, confirm the kids, schools, and activities so future matching gets sharper.";
  }
  return "Go back to iMessage and start using Florence normally. This dashboard stays available for setup, accounts, and settings.";
}

function phaseBadgeVariant(setup: FlorenceSetupResponse["setup"]) {
  if (setup.readyForChat) {
    return "success" as const;
  }
  if (setup.phase === "attention_needed") {
    return "warning" as const;
  }
  return "outline" as const;
}

export function SetupScreen({ token }: { token?: string }) {
  const queryClient = useQueryClient();
  const setupQuery = useQuery({
    queryKey: ["florence", "setup", token],
    queryFn: () => getSetup(token),
  });
  const data = setupQuery.data;

  const [parentDisplayName, setParentDisplayName] = useState("");
  const [children, setChildren] = useState<EditableChild[]>([{ name: "", details: "" }]);
  const [schoolsText, setSchoolsText] = useState("");
  const [activitiesText, setActivitiesText] = useState("");
  const [initializedKey, setInitializedKey] = useState<string | null>(null);
  const [autoConnectStarted, setAutoConnectStarted] = useState(false);
  const [autoConnectFailed, setAutoConnectFailed] = useState(false);

  useEffect(() => {
    if (!data) {
      return;
    }
    const nextKey = `${data.household.id}:${data.member.id}`;
    if (initializedKey === nextKey) {
      return;
    }
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
    setInitializedKey(nextKey);
  }, [data, initializedKey]);

  const connectMutation = useMutation({
    mutationFn: () => startGoogleConnect(token),
    onSuccess: (payload) => {
      window.location.assign(payload.connectUrl);
    },
    onError: (error) => {
      setAutoConnectFailed(true);
      setAutoConnectStarted(false);
      toast.error(error instanceof Error ? error.message : "Unable to start Google connection");
    },
  });

  useEffect(() => {
    if (!data || data.setup.googleConnected || autoConnectStarted || autoConnectFailed) {
      return;
    }

    setAutoConnectStarted(true);

    if (data.googleConnectUrl) {
      window.location.assign(data.googleConnectUrl);
      return;
    }

    connectMutation.mutate();
  }, [autoConnectFailed, autoConnectStarted, connectMutation, data]);

  const profileMutation = useMutation({
    mutationFn: () =>
      saveSetupProfile({
        ...(token ? { token } : {}),
        parentDisplayName,
        children: children
          .map((child) => ({
            name: child.name.trim(),
            details: child.details.trim(),
          }))
          .filter((child) => child.name),
        schools: splitLines(schoolsText),
        activities: splitLines(activitiesText),
      }),
    onSuccess: (payload) => {
      queryClient.setQueryData(["florence", "setup", token], payload);
      toast.success(
        payload.setup.readyForChat
          ? "Florence is ready for iMessage."
          : "Household details saved.",
      );
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Unable to save household details");
    },
  });

  const canSaveProfile = useMemo(() => {
    return (
      children.some((child) => child.name.trim()) &&
      splitLines(schoolsText).length > 0 &&
      splitLines(activitiesText).length > 0
    );
  }, [activitiesText, children, schoolsText]);

  if (setupQuery.isLoading) {
    return (
      <div className="grid gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Loading Florence setup</CardTitle>
            <CardDescription>Pulling your current onboarding state.</CardDescription>
          </CardHeader>
          <CardContent>
            <Progress value={24} />
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
              ? "Ask Florence in chat for a fresh setup link, then open that link while signed into the right Google account."
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

  const progressValue = progressForSync(data.setup, data.sync.primary.phase);
  const readinessItems = [
    {
      title: "Google account",
      complete: data.setup.googleConnected,
      description: data.setup.googleConnected
        ? `${data.sync.connections.length} Google account${data.sync.connections.length === 1 ? "" : "s"} connected`
        : "Florence is continuing into Google consent automatically.",
    },
    {
      title: "Initial sync",
      complete: data.setup.initialSyncComplete,
      description:
        data.setup.phase === "attention_needed"
          ? data.sync.primary.lastSyncError || "The first sync needs to be retried."
          : data.setup.initialSyncComplete
            ? "The first Gmail and Calendar pass is complete."
            : "Inbox and calendar are still being scanned.",
    },
    {
      title: "Household profile",
      complete: data.setup.requiredProfileComplete,
      description: data.setup.requiredProfileComplete
        ? "Kids, schools, and activities are grounded enough for matching."
        : "Add the core household details Florence needs for family context.",
    },
    {
      title: "Ready for iMessage",
      complete: data.setup.readyForChat,
      description: data.setup.readyForChat
        ? "Florence is ready to run normally in iMessage."
        : "Florence will switch into normal chat mode once the checklist is complete.",
    },
  ];

  return (
    <div className="grid gap-6">
      <div className="grid gap-6 lg:grid-cols-[minmax(0,1.28fr)_minmax(280px,0.72fr)]">
        <Card>
          <CardHeader>
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div>
                <CardTitle>{syncHeadline(data)}</CardTitle>
                <CardDescription className="mt-1.5 max-w-2xl">
                  {syncBody(data)}
                </CardDescription>
              </div>
              <Badge variant={phaseBadgeVariant(data.setup)}>
                {data.setup.readyForChat ? "Ready" : data.setup.phase.replaceAll("_", " ")}
              </Badge>
            </div>
          </CardHeader>
          <CardContent className="grid gap-5">
            <div>
              <div className="mb-2 flex items-center justify-between gap-3 text-sm">
                <div className="font-medium">Onboarding progress</div>
                <div className="text-muted-foreground">{progressValue}%</div>
              </div>
              <Progress value={progressValue} />
            </div>

            <div className="grid gap-3 md:grid-cols-2">
              {readinessItems.map((item) => (
                <div
                  key={item.title}
                  className="rounded-lg border p-4"
                >
                  <div className="mb-1 flex items-center justify-between gap-3">
                    <div className="text-sm font-medium">{item.title}</div>
                    <Badge
                      variant={
                        item.complete
                          ? "success"
                          : item.title === "Initial sync" && data.setup.phase === "attention_needed"
                            ? "warning"
                            : "outline"
                      }
                    >
                      {item.complete
                        ? "Complete"
                        : item.title === "Initial sync" && data.setup.phase === "attention_needed"
                          ? "Needs attention"
                          : "In progress"}
                    </Badge>
                  </div>
                  <p className="text-sm text-muted-foreground">{item.description}</p>
                </div>
              ))}
            </div>
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
                  <div className="text-sm font-semibold">Inbox scanned</div>
                  <div className="text-2xl font-semibold">{data.sync.primary.gmailItemCount}</div>
                </div>
              </div>
              <p className="text-sm leading-6 text-muted-foreground">
                Messages Florence has already indexed from the primary Google account.
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="grid gap-2 pt-6">
              <div className="flex items-center gap-3">
                <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-primary/10 text-primary">
                  <CalendarDays className="h-5 w-5" />
                </div>
                <div>
                  <div className="text-sm font-semibold">Calendar scanned</div>
                  <div className="text-2xl font-semibold">{data.sync.primary.calendarItemCount}</div>
                </div>
              </div>
              <p className="text-sm leading-6 text-muted-foreground">
                Calendar events Florence has already folded into the first sync pass.
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="grid gap-2 pt-6">
              <div className="flex items-center gap-3">
                <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-primary/10 text-primary">
                  <Sparkles className="h-5 w-5" />
                </div>
                <div>
                  <div className="text-sm font-semibold">Candidates found</div>
                  <div className="text-2xl font-semibold">{data.preview.candidateCount}</div>
                </div>
              </div>
              <p className="text-sm leading-6 text-muted-foreground">
                Family-relevant items Florence surfaced from the first pass.
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="grid gap-2 pt-6">
              <div className="flex items-center gap-3">
                <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-primary/10 text-primary">
                  <Users className="h-5 w-5" />
                </div>
                <div>
                  <div className="text-sm font-semibold">Kids grounded</div>
                  <div className="text-2xl font-semibold">{data.profile.children.length}</div>
                </div>
              </div>
              <p className="text-sm leading-6 text-muted-foreground">
                Children Florence can already anchor across schools, activities, and messages.
              </p>
            </CardContent>
          </Card>
        </div>
      </div>

      {!data.setup.googleConnected ? (
        <Card>
          <CardHeader>
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div>
                <CardTitle className="flex items-center gap-2">
                  <LoaderCircle className="h-5 w-5 animate-spin text-primary" />
                  Continuing to Google
                </CardTitle>
                <CardDescription className="mt-1.5 max-w-2xl">
                  This page hands off directly into the Gmail and Calendar consent flow.
                  If the browser blocks the redirect, use the fallback link below.
                </CardDescription>
              </div>
              <Badge variant={autoConnectFailed ? "warning" : "secondary"}>
                {autoConnectFailed ? "Redirect blocked" : "Auto handoff"}
              </Badge>
            </div>
          </CardHeader>
          <CardContent className="flex flex-wrap items-center gap-3">
            {data.googleConnectUrl ? (
              <Button asChild>
                <a href={data.googleConnectUrl}>
                  Continue to Google
                  <ExternalLink className="h-4 w-4" />
                </a>
              </Button>
            ) : null}
            <Button
              variant="outline"
              onClick={() => {
                setAutoConnectFailed(false);
                setAutoConnectStarted(false);
              }}
              disabled={connectMutation.isPending}
            >
              {connectMutation.isPending ? "Retrying..." : "Retry handoff"}
            </Button>
            {autoConnectFailed ? (
              <div className="text-sm text-muted-foreground">
                Florence couldn&apos;t open the Google consent link automatically. Retrying will regenerate the handoff
                if needed.
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">
                If nothing happens, the fallback link opens the same connect flow directly.
              </div>
            )}
          </CardContent>
        </Card>
      ) : null}

      {data.setup.googleConnected && !data.setup.initialSyncComplete ? (
        <Alert tone={data.setup.phase === "attention_needed" ? "warning" : "default"}>
          <div className="flex items-start gap-3">
            <CircleAlert className="mt-0.5 h-4 w-4 shrink-0" />
            <div className="space-y-1">
              <div className="font-medium">
                {data.setup.phase === "attention_needed"
                  ? "The first sync needs attention"
                  : "The first sync is still running"}
              </div>
              <div className="text-sm leading-6">
                {data.setup.phase === "attention_needed"
                  ? data.sync.primary.lastSyncError || "Reconnect Google or try again later."
                  : "You can leave this page. Florence will text you when the first Gmail and Calendar pass is done."}
              </div>
            </div>
          </div>
        </Alert>
      ) : null}

      <div className="grid gap-6 lg:grid-cols-[minmax(0,1.12fr)_minmax(280px,0.88fr)]">
        <Card>
          <CardHeader>
            <CardTitle>Household profile</CardTitle>
            <CardDescription>
              Kids, schools, and activities are required before Florence should behave like a tuned family agent.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-6">
            <div className="grid gap-2">
              <Label htmlFor="parent-name">Your name</Label>
              <Input
                id="parent-name"
                value={parentDisplayName}
                onChange={(event) => setParentDisplayName(event.target.value)}
                placeholder="e.g. Sarah"
              />
            </div>

            <div className="grid gap-4">
              <div className="flex items-center justify-between">
                <div>
                  <Label>Kids</Label>
                  <p className="mt-1 text-sm text-muted-foreground">Add one child per card.</p>
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
                  <div key={`${index}-${child.name}`} className="rounded-lg border p-4">
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
                          placeholder="e.g. Alex"
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
                placeholder={"e.g. Westlake Elementary\nSunrise Preschool"}
              />
              {data.suggestions.schools.length ? (
                <div className="flex flex-wrap gap-2">
                  {data.suggestions.schools.map((suggestion) => (
                    <Button
                      key={suggestion.label}
                      type="button"
                      size="sm"
                      variant={suggestion.selected ? "secondary" : "outline"}
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
              <Label htmlFor="activities">Activities, teams, and recurring classes</Label>
              <Textarea
                id="activities"
                value={activitiesText}
                onChange={(event) => setActivitiesText(event.target.value)}
                placeholder={"e.g. Soccer practice\nPiano lessons"}
              />
              {data.suggestions.activities.length ? (
                <div className="flex flex-wrap gap-2">
                  {data.suggestions.activities.map((suggestion) => (
                    <Button
                      key={suggestion.label}
                      type="button"
                      size="sm"
                      variant={suggestion.selected ? "secondary" : "outline"}
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
              <Button onClick={() => profileMutation.mutate()} disabled={!canSaveProfile || profileMutation.isPending}>
                {profileMutation.isPending
                  ? "Saving..."
                  : data.setup.readyForChat
                    ? "Update profile"
                    : "Finish setup"}
                <CheckCircle2 className="h-4 w-4" />
              </Button>
              {!data.setup.initialSyncComplete ? (
                <div className="text-sm text-muted-foreground">
                  Florence will use these details once the first sync finishes.
                </div>
              ) : null}
            </div>
          </CardContent>
        </Card>

        <div className="grid gap-6">
          <Card>
            <CardHeader>
              <CardTitle>What Florence found</CardTitle>
              <CardDescription>A lightweight preview from the first sync pass.</CardDescription>
            </CardHeader>
            <CardContent className="grid gap-3">
              {data.preview.candidates.length ? (
                data.preview.candidates.map((candidate) => (
                  <div key={candidate.id} className="rounded-lg border p-4">
                    <div className="mb-1 flex items-center justify-between gap-3">
                      <div className="font-medium">{candidate.title}</div>
                      <Badge variant={candidate.state === "pending_review" ? "warning" : "outline"}>
                        {candidate.state.replaceAll("_", " ")}
                      </Badge>
                    </div>
                    <p className="text-sm leading-6 text-muted-foreground">{candidate.summary}</p>
                  </div>
                ))
              ) : (
                <Alert>
                  Florence hasn&apos;t surfaced review candidates yet. That can mean the first sync is still running or
                  there just wasn&apos;t much family-relevant signal in the initial window.
                </Alert>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Ready means</CardTitle>
            </CardHeader>
            <CardContent className="grid gap-3 text-sm leading-6 text-muted-foreground">
              <div>Florence has one connected Google account.</div>
              <div>The first Gmail and Calendar pass is complete.</div>
              <div>Kids, schools, and activities are grounded enough for family matching.</div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
