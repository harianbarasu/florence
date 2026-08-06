import { canonicalDigest } from "../../shared/canonical-json.js";
import type { CalendarPrivacyMode, JsonObject } from "./contracts.js";

export type MailBodyRetrieval = "now" | "after_metadata_triage" | "never";

export interface MailMetadata {
  readonly labelIds: readonly string[];
  readonly from: string | null;
  readonly subject: string | null;
  readonly snippet: string;
  readonly hasAttachments: boolean;
}

export interface MailAdmission {
  readonly ingestMetadata: boolean;
  readonly bodyRetrieval: MailBodyRetrieval;
  readonly reasons: readonly string[];
}

/**
 * The single boundary for retrieving or processing private mail content.
 * Metadata-only admissions are retained for sync/reclassification, but cannot
 * expose a body or attachment to persistence, extraction, or a model.
 */
export function isFullMailContentAdmitted(admission: MailAdmission): boolean {
  return admission.ingestMetadata && admission.bodyRetrieval === "now";
}

const FAMILY_SIGNAL =
  /\b(?:school|teacher|class(?:room)?|camp|daycare|preschool|coach|practice|game|tournament|pickup|pick-up|dropoff|drop-off|dismissal|permission|field trip|activity|lesson|recital|pediatric|appointment|birthday|parent|guardian|caregiver|babysitt|tuition|lunch|meal|allerg|vacation|flight|hotel)\b/iu;

/** Cheap deterministic admission precedes any body retrieval or model call. */
export function assessMailMetadata(metadata: MailMetadata): MailAdmission {
  const labels = new Set(metadata.labelIds.map((label) => label.toUpperCase()));
  if (labels.has("SPAM") || labels.has("TRASH")) {
    return { ingestMetadata: false, bodyRetrieval: "never", reasons: ["provider_spam_or_trash"] };
  }

  const searchable = [metadata.from, metadata.subject, metadata.snippet].filter(Boolean).join(" ");
  if (FAMILY_SIGNAL.test(searchable)) {
    return {
      ingestMetadata: true,
      bodyRetrieval: "now",
      reasons: ["family_signal_in_metadata"],
    };
  }

  if (metadata.hasAttachments && (labels.has("INBOX") || labels.has("IMPORTANT") || labels.has("STARRED"))) {
    return {
      ingestMetadata: true,
      bodyRetrieval: "now",
      reasons: ["direct_mail_with_attachment"],
    };
  }

  if (labels.has("CATEGORY_PROMOTIONS") || labels.has("CATEGORY_SOCIAL") || labels.has("CATEGORY_FORUMS")) {
    return {
      ingestMetadata: true,
      bodyRetrieval: "after_metadata_triage",
      reasons: ["bulk_category_requires_metadata_triage"],
    };
  }

  return {
    ingestMetadata: true,
    bodyRetrieval: labels.has("INBOX") || labels.has("IMPORTANT") ? "now" : "after_metadata_triage",
    reasons: labels.has("INBOX") || labels.has("IMPORTANT") ? ["direct_mail"] : ["metadata_triage"],
  };
}

export interface CalendarArtifact {
  readonly remoteEventId: string;
  readonly start: string;
  readonly end: string;
  readonly status: string;
  readonly transparency?: string;
  readonly title?: string | null;
  readonly description?: string | null;
  readonly location?: string | null;
  readonly recurrenceId?: string | null;
  readonly attendees?: readonly JsonObject[];
}

/** Applies calendar privacy before bytes enter durable source persistence. */
export function projectCalendarArtifact(
  artifact: CalendarArtifact,
  mode: CalendarPrivacyMode,
): JsonObject | null {
  if (mode === "off") return null;
  const identityDigest = canonicalDigest({ kind: "calendar_event", id: artifact.remoteEventId });
  const minimum: JsonObject = {
    identityDigest,
    start: artifact.start,
    end: artifact.end,
    status: artifact.status,
    busy: artifact.status !== "cancelled" && artifact.transparency !== "transparent",
  };
  if (mode === "availability_only") return minimum;
  return {
    ...minimum,
    remoteEventId: artifact.remoteEventId,
    title: artifact.title ?? null,
    description: artifact.description ?? null,
    location: artifact.location ?? null,
    recurrenceId: artifact.recurrenceId ?? null,
    attendees: [...(artifact.attendees ?? [])],
  };
}

export type MailBackfillStageKind =
  | "live"
  | "newest_30_days"
  | "days_31_to_90"
  | "days_91_to_365"
  | "older_history";

export interface MailBackfillStage {
  readonly kind: MailBackfillStageKind;
  readonly priority: number;
  readonly afterExclusive: string | null;
  readonly beforeOrEqual: string | null;
  readonly silent: boolean;
}

export function planNewestFirstMailBackfill(input: {
  readonly asOf: string;
  readonly olderHistoryEnabled: boolean;
}): readonly MailBackfillStage[] {
  const asOf = requireInstant(input.asOf);
  const day30 = subtractUtcDays(asOf, 30);
  const day90 = subtractUtcDays(asOf, 90);
  const day365 = subtractUtcDays(asOf, 365);
  const stages: MailBackfillStage[] = [
    {
      kind: "live",
      priority: 0,
      afterExclusive: null,
      beforeOrEqual: null,
      silent: false,
    },
    {
      kind: "newest_30_days",
      priority: 10,
      afterExclusive: day30.toISOString(),
      beforeOrEqual: asOf.toISOString(),
      silent: true,
    },
    {
      kind: "days_31_to_90",
      priority: 20,
      afterExclusive: day90.toISOString(),
      beforeOrEqual: day30.toISOString(),
      silent: true,
    },
    {
      kind: "days_91_to_365",
      priority: 30,
      afterExclusive: day365.toISOString(),
      beforeOrEqual: day90.toISOString(),
      silent: true,
    },
  ];
  if (input.olderHistoryEnabled) {
    stages.push({
      kind: "older_history",
      priority: 40,
      afterExclusive: null,
      beforeOrEqual: day365.toISOString(),
      silent: true,
    });
  }
  return stages;
}

export function planCalendarSyncWindow(asOfCandidate: string): {
  readonly startsAt: string;
  readonly endsAt: string;
} {
  const asOf = requireInstant(asOfCandidate);
  const startsAt = new Date(asOf);
  startsAt.setUTCFullYear(startsAt.getUTCFullYear() - 1);
  const endsAt = new Date(asOf);
  endsAt.setUTCMonth(endsAt.getUTCMonth() + 18);
  return { startsAt: startsAt.toISOString(), endsAt: endsAt.toISOString() };
}

function subtractUtcDays(value: Date, days: number): Date {
  return new Date(value.getTime() - days * 24 * 60 * 60 * 1_000);
}

function requireInstant(candidate: string): Date {
  const result = new Date(candidate);
  if (Number.isNaN(result.getTime())) throw new Error("Expected a valid instant");
  return result;
}
