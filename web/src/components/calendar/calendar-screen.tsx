"use client";

import Link from "next/link";
import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { ArrowRight, CalendarDays, Clock3, MapPin } from "lucide-react";
import { FlorenceApiError, getCalendar } from "@/lib/florence-api";
import { withToken } from "@/lib/routes";
import { Alert } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

function startOfMonth(date: Date) {
  return new Date(date.getFullYear(), date.getMonth(), 1);
}

function endOfMonth(date: Date) {
  return new Date(date.getFullYear(), date.getMonth() + 1, 0, 23, 59, 59, 999);
}

function toIsoDateTime(value: Date) {
  return value.toISOString();
}

function groupLabel(value: string | null) {
  if (!value) {
    return "No scheduled date";
  }

  try {
    return new Intl.DateTimeFormat(undefined, {
      weekday: "long",
      month: "short",
      day: "numeric",
    }).format(new Date(value));
  } catch {
    return value;
  }
}

function timeRange(startsAt: string | null, endsAt: string | null, allDay: boolean) {
  if (allDay) {
    return "All day";
  }
  if (!startsAt) {
    return "Time TBD";
  }

  try {
    const start = new Intl.DateTimeFormat(undefined, {
      hour: "numeric",
      minute: "2-digit",
    }).format(new Date(startsAt));
    if (!endsAt) {
      return start;
    }
    const end = new Intl.DateTimeFormat(undefined, {
      hour: "numeric",
      minute: "2-digit",
    }).format(new Date(endsAt));
    return `${start} to ${end}`;
  } catch {
    return startsAt;
  }
}

function badgeVariant(status: string) {
  if (status === "confirmed") {
    return "success" as const;
  }
  if (status === "tentative") {
    return "warning" as const;
  }
  return "outline" as const;
}

export function CalendarScreen({ token }: { token?: string }) {
  const range = useMemo(() => {
    const now = new Date();
    return {
      start: toIsoDateTime(startOfMonth(now)),
      end: toIsoDateTime(endOfMonth(now)),
    };
  }, []);

  const calendarQuery = useQuery({
    queryKey: ["florence", "calendar", token, range.start, range.end],
    queryFn: () => getCalendar({ token, start: range.start, end: range.end }),
  });

  if (calendarQuery.isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Loading calendar</CardTitle>
          <CardDescription>Pulling Florence household events.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  if (calendarQuery.error) {
    const error = calendarQuery.error as FlorenceApiError;
    return (
      <Card>
        <CardHeader>
          <CardTitle>Calendar couldn&apos;t be loaded</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4">
          <Alert tone={error.message === "unknown_web_google_identity" ? "warning" : "destructive"}>
            {error.message === "unknown_web_google_identity"
              ? "Finish setup from a Florence link first, or sign in with the matching Google account."
              : error.message}
          </Alert>
          <Button variant="outline" onClick={() => calendarQuery.refetch()}>
            Try again
          </Button>
        </CardContent>
      </Card>
    );
  }

  const data = calendarQuery.data;
  if (!data) {
    return null;
  }

  if (!data.setup.readyForChat) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Calendar unlocks after onboarding</CardTitle>
          <CardDescription>Finish setup and the activation review first.</CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4">
          <Alert tone="warning">
            Florence is not fully ready yet. The calendar becomes the default home once setup and the initial review pass are complete.
          </Alert>
          <Button asChild>
            <Link href={withToken("/setup", token)}>
              Return to setup
              <ArrowRight className="h-4 w-4" />
            </Link>
          </Button>
        </CardContent>
      </Card>
    );
  }

  const groupedEvents = data.events.reduce<Record<string, typeof data.events>>((accumulator, event) => {
    const key = event.startsAt || event.endsAt || "undated";
    accumulator[key] = accumulator[key] || [];
    accumulator[key].push(event);
    return accumulator;
  }, {});

  const orderedGroups = Object.entries(groupedEvents).sort(([left], [right]) => {
    if (left === "undated") {
      return 1;
    }
    if (right === "undated") {
      return -1;
    }
    return left.localeCompare(right);
  });

  return (
    <div className="grid gap-6">
      <Card>
        <CardHeader className="gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div className="space-y-1.5">
            <CardTitle>Calendar</CardTitle>
            <CardDescription>
              Florence&apos;s canonical household layer. This is the surface for confirmed and tentative family items.
            </CardDescription>
          </div>
          <Badge variant="outline">
            {data.range.start && data.range.end ? "Current month" : "Open range"}
          </Badge>
        </CardHeader>
      </Card>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <Card>
          <CardContent className="pt-6">
            <div className="text-sm font-medium text-muted-foreground">Confirmed</div>
            <div className="mt-2 text-3xl font-semibold">{data.counts.confirmed}</div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-sm font-medium text-muted-foreground">Tentative</div>
            <div className="mt-2 text-3xl font-semibold">{data.counts.tentative}</div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-sm font-medium text-muted-foreground">Cancelled</div>
            <div className="mt-2 text-3xl font-semibold">{data.counts.cancelled}</div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-sm font-medium text-muted-foreground">Total</div>
            <div className="mt-2 text-3xl font-semibold">{data.counts.total}</div>
          </CardContent>
        </Card>
      </div>

      {!orderedGroups.length ? (
        <Card>
          <CardContent className="grid gap-4 pt-6">
            <Alert>No household events are on the canonical Florence calendar yet.</Alert>
            <Button asChild variant="outline">
              <Link href={withToken("/review", token)}>
                Open review queue
                <ArrowRight className="h-4 w-4" />
              </Link>
            </Button>
          </CardContent>
        </Card>
      ) : null}

      <div className="grid gap-4">
        {orderedGroups.map(([groupKey, events]) => (
          <Card key={groupKey}>
            <CardHeader>
              <CardTitle className="text-lg">{groupLabel(groupKey === "undated" ? null : groupKey)}</CardTitle>
            </CardHeader>
            <CardContent className="grid gap-3">
              {events.map((event) => (
                <div key={event.id} className="rounded-2xl border p-4">
                  <div className="mb-2 flex flex-wrap items-center gap-2">
                    <div className="font-medium">{event.title}</div>
                    <Badge variant={badgeVariant(event.status)}>{event.status}</Badge>
                  </div>
                  <div className="grid gap-2 text-sm text-muted-foreground">
                    <div className="flex items-center gap-2">
                      <Clock3 className="h-4 w-4" />
                      {timeRange(event.startsAt, event.endsAt, event.allDay)}
                    </div>
                    {event.location ? (
                      <div className="flex items-center gap-2">
                        <MapPin className="h-4 w-4" />
                        {event.location}
                      </div>
                    ) : null}
                    {event.description ? <div>{event.description}</div> : null}
                  </div>
                </div>
              ))}
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}
