"use client";

import Link from "next/link";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowRight, CheckCircle2, CircleAlert, Clock3, XCircle } from "lucide-react";
import { toast } from "sonner";
import { FlorenceApiError, getReview, saveReviewDecision } from "@/lib/florence-api";
import { withToken } from "@/lib/routes";
import type { FlorenceCandidatePreview } from "@/lib/types";
import { Alert } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

function humanize(value: string) {
  return value.replaceAll("_", " ");
}

function badgeVariantForCandidate(candidate: FlorenceCandidatePreview) {
  if (candidate.state === "confirmed") {
    return "success" as const;
  }
  if (candidate.state === "rejected") {
    return "outline" as const;
  }
  return "warning" as const;
}

export function ReviewScreen({ token }: { token?: string }) {
  const queryClient = useQueryClient();
  const reviewQuery = useQuery({
    queryKey: ["florence", "review", token],
    queryFn: () => getReview(token),
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
      toast.success("Review updated.");
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Unable to save review decision");
    },
  });

  if (reviewQuery.isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Loading review queue</CardTitle>
          <CardDescription>Pulling Florence&apos;s inferred household items.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  if (reviewQuery.error) {
    const error = reviewQuery.error as FlorenceApiError;
    return (
      <Card>
        <CardHeader>
          <CardTitle>Review couldn&apos;t be loaded</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4">
          <Alert tone={error.message === "unknown_web_google_identity" ? "warning" : "destructive"}>
            {error.message === "unknown_web_google_identity"
              ? "Finish setup from a Florence link first, or sign in with the matching Google account."
              : error.message}
          </Alert>
          <Button variant="outline" onClick={() => reviewQuery.refetch()}>
            Try again
          </Button>
        </CardContent>
      </Card>
    );
  }

  const data = reviewQuery.data;
  if (!data) {
    return null;
  }

  const pendingCandidates = data.candidates.filter((candidate) => candidate.state === "pending_review");

  return (
    <div className="grid gap-6">
      <Card>
        <CardHeader className="gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div className="space-y-1.5">
            <CardTitle>Review queue</CardTitle>
            <CardDescription>
              Florence earns trust by showing what it inferred and letting the household confirm or reject it.
            </CardDescription>
          </div>
          {!data.setup.readyForChat ? (
            <Button asChild variant="outline">
              <Link href={withToken("/setup", token)}>
                Back to setup
                <ArrowRight className="h-4 w-4" />
              </Link>
            </Button>
          ) : null}
        </CardHeader>
      </Card>

      {data.nextPrompt ? (
        <Alert>
          <div className="space-y-1">
            <div className="font-medium">Next up</div>
            <div>{data.nextPrompt.text}</div>
          </div>
        </Alert>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <Card>
          <CardContent className="pt-6">
            <div className="text-sm font-medium text-muted-foreground">Pending</div>
            <div className="mt-2 flex items-center gap-2 text-3xl font-semibold">
              <Clock3 className="h-5 w-5 text-primary" />
              {data.counts.pending}
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-sm font-medium text-muted-foreground">Confirmed</div>
            <div className="mt-2 flex items-center gap-2 text-3xl font-semibold">
              <CheckCircle2 className="h-5 w-5 text-emerald-700" />
              {data.counts.confirmed}
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-sm font-medium text-muted-foreground">Rejected</div>
            <div className="mt-2 flex items-center gap-2 text-3xl font-semibold">
              <XCircle className="h-5 w-5 text-muted-foreground" />
              {data.counts.rejected}
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-sm font-medium text-muted-foreground">Quarantined</div>
            <div className="mt-2 flex items-center gap-2 text-3xl font-semibold">
              <CircleAlert className="h-5 w-5 text-amber-700" />
              {data.counts.quarantined}
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-sm font-medium text-muted-foreground">Total</div>
            <div className="mt-2 text-3xl font-semibold">{data.counts.total}</div>
          </CardContent>
        </Card>
      </div>

      {!pendingCandidates.length ? (
        <Card>
          <CardContent className="grid gap-4 pt-6">
            <Alert tone="success">No pending review items right now. Florence has either been reviewed or has nothing uncertain left to ask about.</Alert>
            {!data.setup.readyForChat ? (
              <Button
                variant="outline"
                onClick={() => reviewMutation.mutate({ decision: "skip_activation" })}
                disabled={reviewMutation.isPending}
              >
                {reviewMutation.isPending ? "Saving..." : "Skip review for now"}
              </Button>
            ) : null}
          </CardContent>
        </Card>
      ) : null}

      <div className="grid gap-4">
        {data.candidates.map((candidate) => (
          <Card key={candidate.id}>
            <CardHeader className="gap-3">
              <div className="flex flex-wrap items-center gap-2">
                <CardTitle className="text-lg">{candidate.title}</CardTitle>
                <Badge variant={badgeVariantForCandidate(candidate)}>{humanize(candidate.state)}</Badge>
              </div>
              <CardDescription>{candidate.summary}</CardDescription>
            </CardHeader>
            <CardContent className="grid gap-4">
              <div className="text-sm text-muted-foreground">
                Source: {humanize(candidate.sourceKind)} · Confidence {Math.round(candidate.confidenceBps / 100)}%
              </div>
              {candidate.state === "pending_review" ? (
                <div className="flex flex-wrap gap-2">
                  <Button
                    size="sm"
                    onClick={() => reviewMutation.mutate({ candidateId: candidate.id, decision: "confirm" })}
                    disabled={reviewMutation.isPending}
                  >
                    Confirm
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => reviewMutation.mutate({ candidateId: candidate.id, decision: "reject" })}
                    disabled={reviewMutation.isPending}
                  >
                    Reject
                  </Button>
                </div>
              ) : null}
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}
