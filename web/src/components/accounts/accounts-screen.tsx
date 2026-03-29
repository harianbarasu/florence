"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertCircle, ArrowUpRight, CheckCircle2, Link2Off, Mail, Plus } from "lucide-react";
import { toast } from "sonner";
import {
  addGoogleAccount,
  disconnectGoogleAccount,
  FlorenceApiError,
  getConnections,
} from "@/lib/florence-api";
import type { FlorenceGoogleConnection } from "@/lib/types";
import { Alert } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

function connectionTone(connection: FlorenceGoogleConnection) {
  if (connection.sync.initialSyncState === "attention_needed") {
    return "warning" as const;
  }
  if (connection.sync.initialSyncState === "ready") {
    return "success" as const;
  }
  return "outline" as const;
}

export function AccountsScreen({ token }: { token?: string }) {
  const queryClient = useQueryClient();
  const connectionsQuery = useQuery({
    queryKey: ["florence", "connections", token],
    queryFn: () => getConnections(token),
  });

  const addMutation = useMutation({
    mutationFn: () => addGoogleAccount(token),
    onSuccess: (payload) => {
      window.location.href = payload.connectUrl;
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Unable to start Google connect");
    },
  });

  const disconnectMutation = useMutation({
    mutationFn: (connectionId: string) => disconnectGoogleAccount(connectionId, token),
    onSuccess: (payload) => {
      queryClient.setQueryData(["florence", "connections", token], payload);
      toast.success("Google account disconnected.");
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Unable to disconnect the account");
    },
  });

  if (connectionsQuery.isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Loading accounts</CardTitle>
          <CardDescription>Pulling connected Google accounts from Florence.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  if (connectionsQuery.error) {
    const error = connectionsQuery.error as FlorenceApiError;
    return (
      <Card>
        <CardHeader>
          <CardTitle>Accounts couldn&apos;t be loaded</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4">
          <Alert tone={error.message === "unknown_web_google_identity" ? "warning" : "destructive"}>
            {error.message === "unknown_web_google_identity"
              ? "Finish setup from a Florence chat link first, or sign in with a linked Google account."
              : error.message}
          </Alert>
          <Button variant="outline" onClick={() => connectionsQuery.refetch()}>
            Try again
          </Button>
        </CardContent>
      </Card>
    );
  }

  const connections = connectionsQuery.data?.connections || [];

  return (
    <div className="grid gap-6">
      <Card>
        <CardHeader className="sm:flex-row sm:items-center sm:justify-between">
          <div className="space-y-1.5">
            <CardTitle>Connected Google accounts</CardTitle>
            <CardDescription>
              Your first connected account makes Florence usable. Additional accounts widen family coverage without
              blocking chat readiness.
            </CardDescription>
          </div>
          <Button onClick={() => addMutation.mutate()} disabled={addMutation.isPending}>
            <Plus className="h-4 w-4" />
            Add another Google account
          </Button>
        </CardHeader>
      </Card>

      <div className="grid gap-4">
        {connections.length ? (
          connections.map((connection) => (
            <Card key={connection.id}>
              <CardHeader className="sm:flex-row sm:items-start sm:justify-between">
                <div className="space-y-2">
                  <div className="flex flex-wrap items-center gap-2">
                    <CardTitle className="text-lg">{connection.email}</CardTitle>
                    {connection.primaryWebAccount ? <Badge variant="secondary">Primary web account</Badge> : null}
                    <Badge variant={connectionTone(connection)}>
                      {connection.sync.initialSyncState.replaceAll("_", " ")}
                    </Badge>
                  </div>
                  <CardDescription className="flex items-center gap-2">
                    <Mail className="h-4 w-4" />
                    {connection.connectedScopes.join(", ")}
                  </CardDescription>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => disconnectMutation.mutate(connection.id)}
                  disabled={disconnectMutation.isPending}
                >
                  <Link2Off className="h-4 w-4" />
                  Disconnect
                </Button>
              </CardHeader>
              <CardContent className="grid gap-3 text-sm text-muted-foreground sm:grid-cols-2 xl:grid-cols-4">
                <div className="rounded-lg border p-4">
                  <div className="mb-2 font-medium text-foreground">Inbox</div>
                  <div>{connection.sync.gmailItemCount} items in first pass</div>
                </div>
                <div className="rounded-lg border p-4">
                  <div className="mb-2 font-medium text-foreground">Calendar</div>
                  <div>{connection.sync.calendarItemCount} events in first pass</div>
                </div>
                <div className="rounded-lg border p-4">
                  <div className="mb-2 font-medium text-foreground">Candidates</div>
                  <div>{connection.sync.candidateCount} surfaced</div>
                </div>
                <div className="rounded-lg border p-4">
                  <div className="mb-2 font-medium text-foreground">Status</div>
                  <div className="flex items-center gap-2">
                    {connection.sync.initialSyncState === "ready" ? (
                      <CheckCircle2 className="h-4 w-4 text-emerald-700" />
                    ) : connection.sync.initialSyncState === "attention_needed" ? (
                      <AlertCircle className="h-4 w-4 text-amber-700" />
                    ) : (
                      <ArrowUpRight className="h-4 w-4 text-primary" />
                    )}
                    {connection.sync.phase.replaceAll("_", " ")}
                  </div>
                </div>
              </CardContent>
            </Card>
          ))
        ) : (
          <Alert tone="warning">No linked Google accounts yet. Finish Setup first to make Florence useful in chat.</Alert>
        )}
      </div>
    </div>
  );
}
