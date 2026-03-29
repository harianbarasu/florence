"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { FlorenceApiError, getSettings, saveSettings } from "@/lib/florence-api";
import { Alert } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export function SettingsScreen({ token }: { token?: string }) {
  const queryClient = useQueryClient();
  const settingsQuery = useQuery({
    queryKey: ["florence", "settings", token],
    queryFn: () => getSettings(token),
  });

  const [householdName, setHouseholdName] = useState("");
  const [timezone, setTimezone] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [initializedKey, setInitializedKey] = useState<string | null>(null);

  useEffect(() => {
    const data = settingsQuery.data;
    if (!data) {
      return;
    }
    const nextKey = `${data.household.id}:${data.member.id}`;
    if (initializedKey === nextKey) {
      return;
    }
    setHouseholdName(data.household.name);
    setTimezone(data.household.timezone);
    setDisplayName(data.member.displayName);
    setInitializedKey(nextKey);
  }, [initializedKey, settingsQuery.data]);

  const settingsMutation = useMutation({
    mutationFn: () =>
      saveSettings({
        ...(token ? { token } : {}),
        householdName,
        timezone,
        memberDisplayName: displayName,
      }),
    onSuccess: (payload) => {
      queryClient.setQueryData(["florence", "settings", token], payload);
      toast.success("Settings saved.");
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Unable to save settings");
    },
  });

  if (settingsQuery.isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Loading settings</CardTitle>
          <CardDescription>Pulling the current Florence household metadata.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  if (settingsQuery.error) {
    const error = settingsQuery.error as FlorenceApiError;
    return (
      <Card>
        <CardHeader>
          <CardTitle>Settings couldn&apos;t be loaded</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4">
          <Alert tone={error.message === "unknown_web_google_identity" ? "warning" : "destructive"}>
            {error.message === "unknown_web_google_identity"
              ? "Sign in with a linked Google account, or ask Florence in chat for a fresh management link."
              : error.message}
          </Alert>
          <Button variant="outline" onClick={() => settingsQuery.refetch()}>
            Try again
          </Button>
        </CardContent>
      </Card>
    );
  }

  const data = settingsQuery.data;
  if (!data) {
    return null;
  }

  return (
    <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_360px]">
      <Card>
        <CardHeader>
          <CardTitle>Settings</CardTitle>
          <CardDescription>Keep this small. The web app exists for management, not for daily Florence use.</CardDescription>
        </CardHeader>
        <CardContent className="grid gap-5">
          <div className="grid gap-2">
            <Label htmlFor="household-name">Household name</Label>
            <Input
              id="household-name"
              value={householdName}
              onChange={(event) => setHouseholdName(event.target.value)}
            />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="timezone">Timezone</Label>
            <Input id="timezone" value={timezone} onChange={(event) => setTimezone(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="display-name">Your display name</Label>
            <Input id="display-name" value={displayName} onChange={(event) => setDisplayName(event.target.value)} />
          </div>
          <div className="flex items-center gap-3">
            <Button onClick={() => settingsMutation.mutate()} disabled={settingsMutation.isPending}>
              {settingsMutation.isPending ? "Saving…" : "Save settings"}
            </Button>
            <div className="text-sm text-muted-foreground">Billing will live here later.</div>
          </div>
        </CardContent>
      </Card>

      <div className="grid gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Bound identity</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-2 text-sm text-muted-foreground">
            <div>
              <span className="font-medium text-foreground">Member:</span> {data.member.displayName}
            </div>
            <div>
              <span className="font-medium text-foreground">Household:</span> {data.household.name}
            </div>
            <div>
              <span className="font-medium text-foreground">Timezone:</span> {data.household.timezone}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>What stays in iMessage</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-2 text-sm text-muted-foreground">
            <div>Household planning and normal Florence use</div>
            <div>Quick reviews and share/private confirmations</div>
            <div>Shared family coordination</div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
