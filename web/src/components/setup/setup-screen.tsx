"use client";

import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ArrowRight,
  CalendarDays,
  CheckCircle2,
  CircleAlert,
  ExternalLink,
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
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import { Textarea } from "@/components/ui/textarea";
import { Stepper } from "@/components/setup/stepper";

type EditableChild = {
  name: string;
  details: string;
};

function currentStepForPhase(phase: string) {
  if (phase === "connect_google") {
    return 1;
  }
  if (phase === "initial_sync_running" || phase === "attention_needed") {
    return 2;
  }
  if (phase === "collect_household_profile") {
    return 3;
  }
  return 4;
}

function progressForSync(setup: FlorenceSetupResponse["setup"], phase: FlorenceSetupResponse["sync"]["primary"]["phase"]) {
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
    return "Connect your first Google account";
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
    return "Use the same Google account you signed in with. Florence will start syncing Gmail and Calendar immediately after consent.";
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
  return "Go back to iMessage and start using Florence normally. This page is just for setup and account management.";
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
      window.location.href = payload.connectUrl;
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Unable to start Google connection");
    },
  });

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
      toast.success(payload.setup.readyForChat ? "Florence is ready for iMessage." : "Household details saved.");
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
  const currentStep = currentStepForPhase(data.setup.phase);

  return (
    <div className="grid gap-6">
      <Card className="overflow-hidden">
        <CardHeader className="gap-4 border-b border-border/70 bg-[linear-gradient(180deg,rgba(28,91,122,0.08),rgba(28,91,122,0.03))]">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">
                Setup
              </div>
              <CardTitle className="text-2xl">{syncHeadline(data)}</CardTitle>
              <CardDescription className="mt-2 max-w-3xl text-sm leading-7">{syncBody(data)}</CardDescription>
            </div>
            <Badge variant={data.setup.readyForChat ? "success" : data.setup.phase === "attention_needed" ? "warning" : "outline"}>
              {data.setup.readyForChat ? "Ready" : data.setup.phase.replaceAll("_", " ")}
            </Badge>
          </div>
          <Stepper currentStep={currentStep} steps={["Google", "Sync", "Household", "Ready"]} />
        </CardHeader>
        <CardContent className="grid gap-5 pt-6">
          <div className="grid gap-3 rounded-[1.25rem] border border-border/70 bg-white/70 p-4">
            <div className="flex items-center justify-between gap-3 text-sm">
              <div className="font-medium">Onboarding progress</div>
              <div className="text-muted-foreground">{progressValue}%</div>
            </div>
            <Progress value={progressValue} />
            <div className="grid gap-2 text-sm text-muted-foreground sm:grid-cols-2 xl:grid-cols-4">
              <div className="flex items-center gap-2">
                <Mail className="h-4 w-4 text-primary" />
                {data.sync.primary.gmailItemCount} inbox items scanned
              </div>
              <div className="flex items-center gap-2">
                <CalendarDays className="h-4 w-4 text-primary" />
                {data.sync.primary.calendarItemCount} calendar events scanned
              </div>
              <div className="flex items-center gap-2">
                <Sparkles className="h-4 w-4 text-primary" />
                {data.preview.candidateCount} candidate items found
              </div>
              <div className="flex items-center gap-2">
                <Users className="h-4 w-4 text-primary" />
                {data.profile.children.length} kids grounded so far
              </div>
            </div>
          </div>

          {!data.setup.googleConnected ? (
            <Card className="border-dashed">
              <CardHeader>
                <CardTitle>Start with the first Google account</CardTitle>
                <CardDescription>
                  This should be the same Google identity you used to sign into the web control plane.
                </CardDescription>
              </CardHeader>
              <CardContent className="flex flex-wrap gap-3">
                <Button onClick={() => connectMutation.mutate()} disabled={connectMutation.isPending}>
                  {connectMutation.isPending ? "Opening Google…" : "Connect Google"}
                  <ArrowRight className="h-4 w-4" />
                </Button>
                {data.googleConnectUrl ? (
                  <Button asChild variant="outline">
                    <a href={data.googleConnectUrl} target="_blank" rel="noreferrer">
                      Open raw connect link
                      <ExternalLink className="h-4 w-4" />
                    </a>
                  </Button>
                ) : null}
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
        </CardContent>
      </Card>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.2fr)_minmax(320px,0.8fr)]">
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
                placeholder="Maya"
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
                  <div key={`${index}-${child.name}`} className="rounded-[1.25rem] border border-border/70 bg-white/70 p-4">
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
                          placeholder="Theo"
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
                          placeholder="1st grade, baseball, nickname Theo"
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
                placeholder={"Wish Community School\nYoung Minds Preschool"}
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
                placeholder={"Theo baseball\nViolet dance\nBoth - Musical Beginnings"}
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
                {profileMutation.isPending ? "Saving…" : data.setup.readyForChat ? "Update profile" : "Finish setup"}
                <CheckCircle2 className="h-4 w-4" />
              </Button>
              {!data.setup.initialSyncComplete ? (
                <div className="text-sm text-muted-foreground">Florence will use these details once the first sync finishes.</div>
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
                  <div key={candidate.id} className="rounded-[1.25rem] border border-border/70 bg-white/70 p-4">
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
