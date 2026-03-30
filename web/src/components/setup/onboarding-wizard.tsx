"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ArrowRight,
  CheckCircle2,
  ExternalLink,
  LoaderCircle,
  MessageCircle,
  Plus,
  Sparkles,
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
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";

type ProfileStep = "name" | "kids" | "schools" | "activities";

type EditableChild = {
  name: string;
  details: string;
};

function OnboardingLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex min-h-screen flex-col items-center px-4 py-12 sm:py-16">
      <div className="mb-10 flex items-center gap-2.5">
        <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-primary text-primary-foreground">
          <Sparkles className="h-4.5 w-4.5" />
        </div>
        <span className="text-lg font-semibold">Florence</span>
      </div>
      <div className="w-full max-w-md">{children}</div>
    </div>
  );
}

function LoadingScreen() {
  return (
    <OnboardingLayout>
      <div className="flex flex-col items-center gap-4 py-16 text-center">
        <LoaderCircle className="h-8 w-8 animate-spin text-primary" />
        <p className="text-sm text-muted-foreground">Loading your setup...</p>
      </div>
    </OnboardingLayout>
  );
}

function ErrorScreen({
  error,
  onRetry,
}: {
  error: FlorenceApiError;
  onRetry: () => void;
}) {
  const isUnknownIdentity = error.message === "unknown_web_google_identity";

  return (
    <OnboardingLayout>
      <div className="flex flex-col items-center gap-4 text-center">
        <h1 className="text-xl font-semibold">
          {isUnknownIdentity ? "Account not found" : "Something went wrong"}
        </h1>
        <p className="text-sm leading-relaxed text-muted-foreground">
          {isUnknownIdentity
            ? "This Google account isn't linked to a Florence household yet. Ask Florence in iMessage for a fresh setup link."
            : "Florence couldn't load your setup. Please try again."}
        </p>
        <Button variant="outline" onClick={onRetry}>
          Try again
        </Button>
      </div>
    </OnboardingLayout>
  );
}

function ConnectGoogleScreen({
  data,
  autoConnectFailed,
  connectPending,
  onConnect,
  onRetry,
}: {
  data: FlorenceSetupResponse;
  autoConnectFailed: boolean;
  connectPending: boolean;
  onConnect: () => void;
  onRetry: () => void;
}) {
  return (
    <OnboardingLayout>
      <div className="flex flex-col items-center gap-6 text-center">
        {!autoConnectFailed ? (
          <>
            <LoaderCircle className="h-8 w-8 animate-spin text-primary" />
            <div>
              <h1 className="text-xl font-semibold">Connecting to Google</h1>
              <p className="mt-2 text-sm text-muted-foreground">
                Taking you to Google to connect Gmail and Calendar...
              </p>
            </div>
          </>
        ) : (
          <>
            <h1 className="text-xl font-semibold">Connect your Google account</h1>
            <p className="text-sm leading-relaxed text-muted-foreground">
              Florence needs access to your Gmail and Calendar to understand your
              family&apos;s schedule.
            </p>
          </>
        )}

        <div className="flex w-full flex-col gap-3">
          {data.googleConnectUrl ? (
            <Button asChild size="lg" className="w-full">
              <a href={data.googleConnectUrl}>
                Continue with Google
                <ExternalLink className="h-4 w-4" />
              </a>
            </Button>
          ) : (
            <Button
              size="lg"
              className="w-full"
              onClick={onConnect}
              disabled={connectPending}
            >
              {connectPending ? "Connecting..." : "Connect Google account"}
              <ArrowRight className="h-4 w-4" />
            </Button>
          )}

          {autoConnectFailed && (
            <Button variant="ghost" size="sm" onClick={onRetry}>
              Retry
            </Button>
          )}
        </div>
      </div>
    </OnboardingLayout>
  );
}

function SyncWaitingScreen({ hasError, errorMessage, onRetry }: {
  hasError: boolean;
  errorMessage: string | null;
  onRetry: () => void;
}) {
  if (hasError) {
    return (
      <OnboardingLayout>
        <div className="flex flex-col items-center gap-6 text-center">
          <h1 className="text-xl font-semibold">Something went wrong</h1>
          <p className="text-sm leading-relaxed text-muted-foreground">
            {errorMessage || "The sync ran into an issue. Let's try again."}
          </p>
          <Button onClick={onRetry}>Retry</Button>
        </div>
      </OnboardingLayout>
    );
  }

  return (
    <OnboardingLayout>
      <div className="flex flex-col items-center gap-6 text-center">
        <LoaderCircle className="h-8 w-8 animate-spin text-primary" />
        <div>
          <h1 className="text-xl font-semibold">
            Scanning your inbox and calendar
          </h1>
          <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
            Florence is looking through your Gmail and Calendar for family-relevant
            items. This can take a few minutes.
          </p>
        </div>
        <p className="text-xs text-muted-foreground">
          Feel free to close this page — we&apos;ll text you in iMessage when
          it&apos;s ready.
        </p>
      </div>
    </OnboardingLayout>
  );
}

function ParentNameScreen({
  value,
  onChange,
  onNext,
}: {
  value: string;
  onChange: (v: string) => void;
  onNext: () => void;
}) {
  return (
    <OnboardingLayout>
      <div className="flex flex-col gap-6">
        <div>
          <h1 className="text-xl font-semibold">
            What should Florence call you?
          </h1>
          <p className="mt-2 text-sm text-muted-foreground">
            This is how Florence will refer to you in conversation.
          </p>
        </div>
        <div className="grid gap-2">
          <Label htmlFor="parent-name">Your name</Label>
          <Input
            id="parent-name"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            placeholder="e.g. Sarah"
            autoFocus
          />
        </div>
        <Button size="lg" onClick={onNext} className="w-full">
          Next
          <ArrowRight className="h-4 w-4" />
        </Button>
      </div>
    </OnboardingLayout>
  );
}

function KidsScreen({
  children,
  onChange,
  onNext,
}: {
  children: EditableChild[];
  onChange: (children: EditableChild[]) => void;
  onNext: () => void;
}) {
  const hasAtLeastOneKid = children.some((c) => c.name.trim());

  return (
    <OnboardingLayout>
      <div className="flex flex-col gap-6">
        <div>
          <h1 className="text-xl font-semibold">Who are your kids?</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            Florence uses this to recognize family-related messages and events.
          </p>
        </div>
        <div className="grid gap-3">
          {children.map((child, index) => (
            <div key={index} className="grid gap-3 rounded-lg border p-4">
              <div className="grid gap-2">
                <Label>Name</Label>
                <Input
                  value={child.name}
                  onChange={(e) =>
                    onChange(
                      children.map((c, i) =>
                        i === index ? { ...c, name: e.target.value } : c,
                      ),
                    )
                  }
                  placeholder="e.g. Alex"
                  autoFocus={index === 0}
                />
              </div>
              <div className="grid gap-2">
                <Label>
                  Details{" "}
                  <span className="font-normal text-muted-foreground">
                    (optional)
                  </span>
                </Label>
                <Input
                  value={child.details}
                  onChange={(e) =>
                    onChange(
                      children.map((c, i) =>
                        i === index ? { ...c, details: e.target.value } : c,
                      ),
                    )
                  }
                  placeholder="e.g. 2nd grade, soccer"
                />
              </div>
            </div>
          ))}
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => onChange([...children, { name: "", details: "" }])}
          className="w-fit"
        >
          <Plus className="h-4 w-4" />
          Add another child
        </Button>
        <Button
          size="lg"
          onClick={onNext}
          disabled={!hasAtLeastOneKid}
          className="w-full"
        >
          Next
          <ArrowRight className="h-4 w-4" />
        </Button>
      </div>
    </OnboardingLayout>
  );
}

function SchoolsScreen({
  value,
  onChange,
  suggestions,
  onNext,
  onSkip,
}: {
  value: string;
  onChange: (v: string) => void;
  suggestions: FlorenceSetupResponse["suggestions"]["schools"];
  onNext: () => void;
  onSkip: () => void;
}) {
  return (
    <OnboardingLayout>
      <div className="flex flex-col gap-6">
        <div>
          <h1 className="text-xl font-semibold">Any schools or daycares?</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            Helps Florence match school-related emails and calendar events.
          </p>
        </div>
        {suggestions.length > 0 && (
          <div>
            <p className="mb-2 text-sm font-medium">
              Found in your calendar
            </p>
            <div className="flex flex-wrap gap-2">
              {suggestions.map((s) => (
                <Button
                  key={s.label}
                  type="button"
                  size="sm"
                  variant={
                    splitLines(value).includes(s.label) ? "secondary" : "outline"
                  }
                  onClick={() => {
                    const current = new Set(splitLines(value));
                    if (current.has(s.label)) {
                      current.delete(s.label);
                    } else {
                      current.add(s.label);
                    }
                    onChange(Array.from(current).join("\n"));
                  }}
                >
                  {s.label}
                </Button>
              ))}
            </div>
          </div>
        )}
        <div className="grid gap-2">
          <Label htmlFor="schools">
            {suggestions.length > 0 ? "Or type them in" : "One per line"}
          </Label>
          <Textarea
            id="schools"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            placeholder={"e.g. Westlake Elementary\nSunrise Preschool"}
            rows={3}
          />
        </div>
        <div className="flex flex-col gap-2">
          <Button size="lg" onClick={onNext} className="w-full">
            Next
            <ArrowRight className="h-4 w-4" />
          </Button>
          <Button variant="ghost" onClick={onSkip} className="w-full">
            Skip for now
          </Button>
        </div>
      </div>
    </OnboardingLayout>
  );
}

function ActivitiesScreen({
  value,
  onChange,
  suggestions,
  onFinish,
  onSkip,
  isPending,
}: {
  value: string;
  onChange: (v: string) => void;
  suggestions: FlorenceSetupResponse["suggestions"]["activities"];
  onFinish: () => void;
  onSkip: () => void;
  isPending: boolean;
}) {
  return (
    <OnboardingLayout>
      <div className="flex flex-col gap-6">
        <div>
          <h1 className="text-xl font-semibold">
            Any recurring activities?
          </h1>
          <p className="mt-2 text-sm text-muted-foreground">
            Sports, classes, lessons — anything that repeats on a schedule.
          </p>
        </div>
        {suggestions.length > 0 && (
          <div>
            <p className="mb-2 text-sm font-medium">
              Found in your calendar
            </p>
            <div className="flex flex-wrap gap-2">
              {suggestions.map((s) => (
                <Button
                  key={s.label}
                  type="button"
                  size="sm"
                  variant={
                    splitLines(value).includes(s.label) ? "secondary" : "outline"
                  }
                  onClick={() => {
                    const current = new Set(splitLines(value));
                    if (current.has(s.label)) {
                      current.delete(s.label);
                    } else {
                      current.add(s.label);
                    }
                    onChange(Array.from(current).join("\n"));
                  }}
                >
                  {s.label}
                </Button>
              ))}
            </div>
          </div>
        )}
        <div className="grid gap-2">
          <Label htmlFor="activities">
            {suggestions.length > 0 ? "Or type them in" : "One per line"}
          </Label>
          <Textarea
            id="activities"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            placeholder={"e.g. Soccer practice\nPiano lessons"}
            rows={3}
          />
        </div>
        <div className="flex flex-col gap-2">
          <Button
            size="lg"
            onClick={onFinish}
            disabled={isPending}
            className="w-full"
          >
            {isPending ? "Saving..." : "Finish setup"}
            <CheckCircle2 className="h-4 w-4" />
          </Button>
          <Button
            variant="ghost"
            onClick={onSkip}
            disabled={isPending}
            className="w-full"
          >
            Skip for now
          </Button>
        </div>
      </div>
    </OnboardingLayout>
  );
}

function DoneScreen() {
  return (
    <OnboardingLayout>
      <div className="flex flex-col items-center gap-6 text-center">
        <div className="flex h-14 w-14 items-center justify-center rounded-full bg-emerald-100 text-emerald-700">
          <CheckCircle2 className="h-7 w-7" />
        </div>
        <div>
          <h1 className="text-xl font-semibold">You&apos;re all set</h1>
          <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
            Florence is ready. Head back to iMessage and start chatting —
            Florence will handle the rest from there.
          </p>
        </div>
        <Button size="lg" variant="outline" className="w-full" asChild>
          <a href="sms:">
            <MessageCircle className="h-4 w-4" />
            Open iMessage
          </a>
        </Button>
      </div>
    </OnboardingLayout>
  );
}

export function OnboardingWizard({
  token,
  userName,
}: {
  token?: string;
  userName?: string;
}) {
  const queryClient = useQueryClient();

  const setupQuery = useQuery({
    queryKey: ["florence", "setup", token],
    queryFn: () => getSetup(token),
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data) return false;
      // Poll during sync
      if (data.setup.googleConnected && !data.setup.initialSyncComplete) {
        return 3000;
      }
      return false;
    },
  });
  const data = setupQuery.data;

  // Auto-connect state
  const [autoConnectStarted, setAutoConnectStarted] = useState(false);
  const [autoConnectFailed, setAutoConnectFailed] = useState(false);

  // Profile form state
  const [profileStep, setProfileStep] = useState<ProfileStep>("name");
  const [parentDisplayName, setParentDisplayName] = useState("");
  const [children, setChildren] = useState<EditableChild[]>([
    { name: "", details: "" },
  ]);
  const [schoolsText, setSchoolsText] = useState("");
  const [activitiesText, setActivitiesText] = useState("");
  const [initializedKey, setInitializedKey] = useState<string | null>(null);

  // Initialize form from backend data
  useEffect(() => {
    if (!data) return;
    const nextKey = `${data.household.id}:${data.member.id}`;
    if (initializedKey === nextKey) return;

    setParentDisplayName(
      data.member.displayName || userName || "",
    );
    setChildren(
      data.profile.children.length
        ? data.profile.children.map((child) => ({
            name: child.fullName,
            details: String(child.metadata?.details || ""),
          }))
        : [{ name: "", details: "" }],
    );
    setSchoolsText(data.profile.schools.map((item) => item.label).join("\n"));
    setActivitiesText(
      data.profile.activities.map((item) => item.label).join("\n"),
    );
    setInitializedKey(nextKey);
  }, [data, initializedKey, userName]);

  // Auto-redirect to Google
  const connectMutation = useMutation({
    mutationFn: () => startGoogleConnect(token),
    onSuccess: (payload) => {
      window.location.assign(payload.connectUrl);
    },
    onError: (error) => {
      setAutoConnectFailed(true);
      setAutoConnectStarted(false);
      toast.error(
        error instanceof Error
          ? error.message
          : "Unable to start Google connection",
      );
    },
  });

  useEffect(() => {
    if (
      !data ||
      data.setup.googleConnected ||
      autoConnectStarted ||
      autoConnectFailed
    )
      return;

    setAutoConnectStarted(true);
    if (data.googleConnectUrl) {
      window.location.assign(data.googleConnectUrl);
      return;
    }
    connectMutation.mutate();
  }, [autoConnectFailed, autoConnectStarted, connectMutation, data]);

  // Save profile
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
      toast.success("Florence is ready for iMessage.");
    },
    onError: (error) => {
      toast.error(
        error instanceof Error
          ? error.message
          : "Unable to save household details",
      );
    },
  });

  function handleFinishProfile() {
    profileMutation.mutate();
  }

  // --- Screen routing ---

  if (setupQuery.isLoading) {
    return <LoadingScreen />;
  }

  if (setupQuery.error) {
    return (
      <ErrorScreen
        error={setupQuery.error as FlorenceApiError}
        onRetry={() => setupQuery.refetch()}
      />
    );
  }

  if (!data) return null;

  // 1. Google not connected
  if (!data.setup.googleConnected) {
    return (
      <ConnectGoogleScreen
        data={data}
        autoConnectFailed={autoConnectFailed}
        connectPending={connectMutation.isPending}
        onConnect={() => connectMutation.mutate()}
        onRetry={() => {
          setAutoConnectFailed(false);
          setAutoConnectStarted(false);
        }}
      />
    );
  }

  // 2. Sync in progress
  if (!data.setup.initialSyncComplete) {
    return (
      <SyncWaitingScreen
        hasError={data.setup.phase === "attention_needed"}
        errorMessage={data.sync.primary.lastSyncError}
        onRetry={() => setupQuery.refetch()}
      />
    );
  }

  // 3. Profile incomplete — show stepped form
  if (!data.setup.readyForChat) {
    switch (profileStep) {
      case "name":
        return (
          <ParentNameScreen
            value={parentDisplayName}
            onChange={setParentDisplayName}
            onNext={() => setProfileStep("kids")}
          />
        );
      case "kids":
        return (
          <KidsScreen
            children={children}
            onChange={setChildren}
            onNext={() => setProfileStep("schools")}
          />
        );
      case "schools":
        return (
          <SchoolsScreen
            value={schoolsText}
            onChange={setSchoolsText}
            suggestions={data.suggestions.schools}
            onNext={() => setProfileStep("activities")}
            onSkip={() => {
              setSchoolsText("");
              setProfileStep("activities");
            }}
          />
        );
      case "activities":
        return (
          <ActivitiesScreen
            value={activitiesText}
            onChange={setActivitiesText}
            suggestions={data.suggestions.activities}
            onFinish={handleFinishProfile}
            onSkip={() => {
              setActivitiesText("");
              handleFinishProfile();
            }}
            isPending={profileMutation.isPending}
          />
        );
    }
  }

  // 4. Done
  return <DoneScreen />;
}
