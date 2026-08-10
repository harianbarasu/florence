import { z } from "zod";

export const EntityIdSchema = z.string().uuid();
export const DigestSchema = z.string().regex(/^[a-f0-9]{64}$/u);
export const InstantSchema = z.iso.datetime({ offset: true });

export const JsonValueSchema: z.ZodType<JsonValue> = z.lazy(() =>
  z.union([
    z.string(),
    z.number().finite(),
    z.boolean(),
    z.null(),
    z.array(JsonValueSchema),
    z.record(z.string(), JsonValueSchema),
  ]),
);
export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

export const JsonObjectSchema = z.record(z.string(), JsonValueSchema);
export type JsonObject = z.infer<typeof JsonObjectSchema>;

const DurableKindSchema = z
  .string()
  .trim()
  .min(1)
  .max(240)
  .regex(/^[a-z0-9][a-z0-9._:-]*$/u);

export const SourceScopeSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    kind: z.literal("person"),
    personId: EntityIdSchema,
  }),
  z.strictObject({
    kind: z.literal("conversation_epoch"),
    participantEpochId: EntityIdSchema,
  }),
]);
export type SourceScope = z.infer<typeof SourceScopeSchema>;

export const ConversationSourceAccessModeSchema = z.enum(["unanimously_shared", "independent_private_views"]);
export type ConversationSourceAccessMode = z.infer<typeof ConversationSourceAccessModeSchema>;

export const SourceArtifactKindSchema = z.enum([
  "mail_message",
  "calendar_event",
  "conversation_message",
  "attachment_manifest",
]);
export type SourceArtifactKind = z.infer<typeof SourceArtifactKindSchema>;

export const SourceOriginSchema = z.strictObject({
  system: z
    .string()
    .trim()
    .min(1)
    .max(80)
    .regex(/^[a-z0-9][a-z0-9._-]*$/u),
  remoteObjectId: z.string().min(1).max(2_000),
  remoteRevisionId: z.string().min(1).max(1_000).optional(),
});
export type SourceOrigin = z.infer<typeof SourceOriginSchema>;

export const IntegrationStatusSchema = z.enum([
  "active",
  "paused",
  "reauth_required",
  "revocation_pending",
  "revoked",
  "error",
]);
export type IntegrationStatus = z.infer<typeof IntegrationStatusSchema>;

export const IntegrationCapabilitySchema = z.enum(["mail", "calendar"]);
export type IntegrationCapability = z.infer<typeof IntegrationCapabilitySchema>;

export const IntegrationAccountKindSchema = z.enum(["personal_family", "work"]);
export type IntegrationAccountKind = z.infer<typeof IntegrationAccountKindSchema>;

const IntegrationReconnectTargetSchema = z.strictObject({
  integrationId: EntityIdSchema,
  expectedControlEpoch: z.number().int().positive(),
  externalSubjectDigest: DigestSchema,
});

const IntegrationCapabilitiesSchema = z
  .array(IntegrationCapabilitySchema)
  .min(1)
  .max(IntegrationCapabilitySchema.options.length)
  .refine((capabilities) => new Set(capabilities).size === capabilities.length, {
    message: "Integration capabilities must be unique",
  });

export const SyncCursorStateSchema = z.enum(["initial", "active", "exhausted", "expired", "error"]);
export type SyncCursorState = z.infer<typeof SyncCursorStateSchema>;

export const CalendarPrivacyModeSchema = z.enum(["full_private", "availability_only", "off"]);
export type CalendarPrivacyMode = z.infer<typeof CalendarPrivacyModeSchema>;

const ConnectIntegrationCommandSchema = z.strictObject({
  kind: z.literal("connect_integration"),
  personId: EntityIdSchema,
  provider: DurableKindSchema,
  externalSubjectDigest: DigestSchema,
  accountKind: IntegrationAccountKindSchema,
  activeCapabilities: IntegrationCapabilitiesSchema,
  credentials: JsonObjectSchema,
  expectedPersonControlEpoch: z.number().int().positive(),
  reconnectTarget: IntegrationReconnectTargetSchema.nullable(),
  connectedAt: InstantSchema,
});

const SetIntegrationStatusCommandSchema = z.strictObject({
  kind: z.literal("set_integration_status"),
  integrationId: EntityIdSchema,
  personId: EntityIdSchema,
  expectedControlEpoch: z.number().int().positive(),
  status: z.enum(["active", "paused", "reauth_required", "error"]),
  changedAt: InstantSchema,
});

const BeginIntegrationRevocationCommandSchema = z.strictObject({
  kind: z.literal("begin_integration_revocation"),
  integrationId: EntityIdSchema,
  personId: EntityIdSchema,
  expectedControlEpoch: z.number().int().positive(),
  requestedAt: InstantSchema,
});

const CompleteIntegrationRevocationCommandSchema = z.strictObject({
  kind: z.literal("complete_integration_revocation"),
  integrationId: EntityIdSchema,
  personId: EntityIdSchema,
  expectedControlEpoch: z.number().int().positive(),
  completedAt: InstantSchema,
});

const BeginOAuthAttemptCommandSchema = z.strictObject({
  kind: z.literal("begin_oauth_attempt"),
  personId: EntityIdSchema,
  provider: DurableKindSchema,
  initiatingSessionId: EntityIdSchema,
  stateDigest: DigestSchema,
  nonce: z.string().min(32).max(512),
  nonceDigest: DigestSchema,
  pkceVerifier: z.string().min(43).max(512),
  returnPath: z
    .string()
    .min(1)
    .max(1_000)
    .refine(
      (value) =>
        value.startsWith("/") &&
        !value.startsWith("//") &&
        ![...value].some((character) => character.charCodeAt(0) < 32),
      "Return path must be a local path without control characters",
    ),
  requestedCapabilities: IntegrationCapabilitiesSchema,
  accountKind: IntegrationAccountKindSchema,
  reconnectTarget: IntegrationReconnectTargetSchema.nullable(),
  expectedPersonControlEpoch: z.number().int().positive(),
  expiresAt: InstantSchema,
  createdAt: InstantSchema,
});

const ConsumeOAuthAttemptCommandSchema = z.strictObject({
  kind: z.literal("consume_oauth_attempt"),
  provider: DurableKindSchema,
  stateDigest: DigestSchema,
  consumedAt: InstantSchema,
});

const CheckpointCursorCommandSchema = z.strictObject({
  kind: z.literal("checkpoint_cursor"),
  integrationId: EntityIdSchema,
  personId: EntityIdSchema,
  expectedIntegrationControlEpoch: z.number().int().positive(),
  resourceKind: DurableKindSchema,
  cursor: JsonValueSchema.nullable(),
  state: SyncCursorStateSchema,
  expectedUpdatedAt: InstantSchema.nullable(),
  checkpointAt: InstantSchema.nullable(),
  updatedAt: InstantSchema,
});

const ConfigureCalendarPrivacyCommandSchema = z.strictObject({
  kind: z.literal("configure_calendar_privacy"),
  integrationId: EntityIdSchema,
  personId: EntityIdSchema,
  expectedIntegrationControlEpoch: z.number().int().positive(),
  calendarIdDigest: DigestSchema,
  mode: CalendarPrivacyModeSchema,
  changedAt: InstantSchema,
});

const ResetIntegrationSyncCommandSchema = z.strictObject({
  kind: z.literal("reset_integration_sync"),
  integrationId: EntityIdSchema,
  personId: EntityIdSchema,
  expectedIntegrationControlEpoch: z.number().int().positive(),
  affectedCapability: IntegrationCapabilitySchema,
  resetAt: InstantSchema,
});

const ReconcileCalendarCatalogCommandSchema = z.strictObject({
  kind: z.literal("reconcile_calendar_catalog"),
  integrationId: EntityIdSchema,
  personId: EntityIdSchema,
  expectedIntegrationControlEpoch: z.number().int().positive(),
  activeCalendarIdDigests: z
    .array(DigestSchema)
    .max(5_000)
    .refine((digests) => new Set(digests).size === digests.length, {
      message: "Active calendar digests must be unique",
    }),
  reconciledAt: InstantSchema,
});

const IngestSourceCommandSchema = z
  .strictObject({
    kind: z.literal("ingest_source"),
    integrationId: EntityIdSchema.nullable(),
    expectedIntegrationControlEpoch: z.number().int().positive().nullable(),
    artifactKind: SourceArtifactKindSchema,
    origin: SourceOriginSchema,
    resourceDigest: DigestSchema.optional(),
    correlationDigest: DigestSchema.optional(),
    scope: SourceScopeSchema,
    conversationAccessMode: ConversationSourceAccessModeSchema.optional(),
    content: JsonObjectSchema,
    occurredAt: InstantSchema,
    capturedAt: InstantSchema,
    requestedRetentionUntil: InstantSchema,
  })
  .superRefine((command, context) => {
    if ((command.integrationId === null) !== (command.expectedIntegrationControlEpoch === null)) {
      context.addIssue({
        code: "custom",
        path: ["expectedIntegrationControlEpoch"],
        message: "Provider source mutations require a complete integration authority fence",
      });
    }
    if (command.artifactKind === "calendar_event" && command.resourceDigest === undefined) {
      context.addIssue({
        code: "custom",
        path: ["resourceDigest"],
        message: "Calendar ingestion requires its configured calendar digest",
      });
    }
    const gmailCorrelated =
      (command.origin.system === "gmail" && command.artifactKind === "mail_message") ||
      (command.origin.system === "gmail.attachment" && command.artifactKind === "attachment_manifest");
    if (gmailCorrelated && command.correlationDigest === undefined) {
      context.addIssue({
        code: "custom",
        path: ["correlationDigest"],
        message: "Gmail thread evidence requires its private frontier digest",
      });
    }
    if (!gmailCorrelated && command.correlationDigest !== undefined) {
      context.addIssue({
        code: "custom",
        path: ["correlationDigest"],
        message: "Only Gmail thread evidence may set a correlation digest",
      });
    }
    if (command.scope.kind === "person" && command.conversationAccessMode !== undefined) {
      context.addIssue({
        code: "custom",
        path: ["conversationAccessMode"],
        message: "Person sources cannot set a conversation access mode",
      });
    }
    if (command.scope.kind === "conversation_epoch" && command.conversationAccessMode === undefined) {
      context.addIssue({
        code: "custom",
        path: ["conversationAccessMode"],
        message: "Conversation sources require an explicit access mode",
      });
    }
  });

const StoreBlobCommandSchema = z
  .strictObject({
    kind: z.literal("store_blob"),
    sourceRevisionId: EntityIdSchema,
    scope: SourceScopeSchema,
    integrationId: EntityIdSchema.nullable(),
    expectedIntegrationControlEpoch: z.number().int().positive().nullable(),
    blobKind: DurableKindSchema,
    mimeType: z.string().trim().min(1).max(200),
    bytes: z.instanceof(Uint8Array),
    storedAt: InstantSchema,
  })
  .superRefine((command, context) => {
    if ((command.integrationId === null) !== (command.expectedIntegrationControlEpoch === null)) {
      context.addIssue({
        code: "custom",
        path: ["expectedIntegrationControlEpoch"],
        message: "Source blob writes require a complete integration authority fence",
      });
    }
  });

const StoreDerivativeCommandSchema = z
  .strictObject({
    kind: z.literal("store_derivative"),
    sourceRevisionId: EntityIdSchema,
    scope: SourceScopeSchema,
    integrationId: EntityIdSchema.nullable(),
    expectedIntegrationControlEpoch: z.number().int().positive().nullable(),
    derivativeKind: DurableKindSchema,
    content: JsonValueSchema,
    requestedRetentionUntil: InstantSchema,
    createdAt: InstantSchema,
  })
  .superRefine((command, context) => {
    if ((command.integrationId === null) !== (command.expectedIntegrationControlEpoch === null)) {
      context.addIssue({
        code: "custom",
        path: ["expectedIntegrationControlEpoch"],
        message: "Source derivative writes require a complete integration authority fence",
      });
    }
  });

const ProposePrivateCandidateCommandSchema = z
  .strictObject({
    kind: z.literal("propose_private_candidate"),
    personId: EntityIdSchema,
    integrationId: EntityIdSchema.nullable(),
    expectedIntegrationControlEpoch: z.number().int().positive().nullable(),
    candidateKind: DurableKindSchema,
    content: JsonObjectSchema,
    evidenceSourceRevisionIds: z.array(EntityIdSchema).min(1).max(100),
    confidence: z.number().min(0).max(1),
    proposedAt: InstantSchema,
    requestedExpiresAt: InstantSchema,
  })
  .superRefine((command, context) => {
    if ((command.integrationId === null) !== (command.expectedIntegrationControlEpoch === null)) {
      context.addIssue({
        code: "custom",
        path: ["expectedIntegrationControlEpoch"],
        message: "Private source proposals require a complete integration authority fence",
      });
    }
  });

const ReviewPrivateCandidateCommandSchema = z.strictObject({
  kind: z.literal("review_private_candidate"),
  candidateId: EntityIdSchema,
  personId: EntityIdSchema,
  decision: z.enum(["accepted", "rejected"]),
  reviewedAt: InstantSchema,
});

const MarkSourceDeletedCommandSchema = z
  .strictObject({
    kind: z.literal("mark_source_deleted"),
    integrationId: EntityIdSchema.nullable(),
    expectedIntegrationControlEpoch: z.number().int().positive().nullable(),
    artifactKind: SourceArtifactKindSchema,
    origin: SourceOriginSchema,
    correlationDigest: DigestSchema.optional(),
    scope: SourceScopeSchema,
    deletedAt: InstantSchema,
  })
  .superRefine((command, context) => {
    if ((command.integrationId === null) !== (command.expectedIntegrationControlEpoch === null)) {
      context.addIssue({
        code: "custom",
        path: ["expectedIntegrationControlEpoch"],
        message: "Provider deletion requires a complete integration authority fence",
      });
    }
    if (
      command.correlationDigest !== undefined &&
      command.origin.system !== "gmail" &&
      command.origin.system !== "gmail.attachment"
    ) {
      context.addIssue({
        code: "custom",
        path: ["correlationDigest"],
        message: "Only Gmail deletion may set a thread correlation digest",
      });
    }
  });

const InvalidateEpochCommandSchema = z.strictObject({
  kind: z.literal("invalidate_conversation_epoch"),
  participantEpochId: EntityIdSchema,
  invalidatedAt: InstantSchema,
});

const GrantConversationPrivateViewsCommandSchema = z.strictObject({
  kind: z.literal("grant_conversation_private_views"),
  participantEpochId: EntityIdSchema,
  personId: EntityIdSchema,
  grantedAt: InstantSchema,
});

const SweepRetentionCommandSchema = z.strictObject({
  kind: z.literal("sweep_retention"),
  asOf: InstantSchema,
  limit: z.number().int().min(1).max(2_000).default(500),
});

export const SourceCommandSchema = z.discriminatedUnion("kind", [
  ConnectIntegrationCommandSchema,
  SetIntegrationStatusCommandSchema,
  BeginIntegrationRevocationCommandSchema,
  CompleteIntegrationRevocationCommandSchema,
  BeginOAuthAttemptCommandSchema,
  ConsumeOAuthAttemptCommandSchema,
  CheckpointCursorCommandSchema,
  ConfigureCalendarPrivacyCommandSchema,
  ResetIntegrationSyncCommandSchema,
  ReconcileCalendarCatalogCommandSchema,
  IngestSourceCommandSchema,
  StoreBlobCommandSchema,
  StoreDerivativeCommandSchema,
  ProposePrivateCandidateCommandSchema,
  ReviewPrivateCandidateCommandSchema,
  MarkSourceDeletedCommandSchema,
  InvalidateEpochCommandSchema,
  GrantConversationPrivateViewsCommandSchema,
  SweepRetentionCommandSchema,
]);
export type SourceCommand = z.infer<typeof SourceCommandSchema>;

const ReadIntegrationAccessQuerySchema = z.strictObject({
  kind: z.literal("integration_access"),
  integrationId: EntityIdSchema,
  personId: EntityIdSchema,
  expectedControlEpoch: z.number().int().positive(),
  requiredCapability: IntegrationCapabilitySchema,
});

const ReadIntegrationProfileQuerySchema = z.strictObject({
  kind: z.literal("integration_profile"),
  integrationId: EntityIdSchema,
  personId: EntityIdSchema,
  expectedControlEpoch: z.number().int().positive(),
});

const ReadOAuthAttemptAccessQuerySchema = z.strictObject({
  kind: z.literal("oauth_attempt_access"),
  provider: DurableKindSchema,
  stateDigest: DigestSchema,
  asOf: InstantSchema,
});

const ReadCursorQuerySchema = z.strictObject({
  kind: z.literal("sync_cursor"),
  integrationId: EntityIdSchema,
  personId: EntityIdSchema,
  expectedIntegrationControlEpoch: z.number().int().positive(),
  resourceKind: DurableKindSchema,
});

const ReadCalendarPrivacyQuerySchema = z.strictObject({
  kind: z.literal("calendar_privacy"),
  integrationId: EntityIdSchema,
  personId: EntityIdSchema,
  expectedIntegrationControlEpoch: z.number().int().positive(),
  calendarIdDigest: DigestSchema,
});

const ReadSourceRevisionQuerySchema = z
  .strictObject({
    kind: z.literal("source_revision"),
    sourceRevisionId: EntityIdSchema,
    scope: SourceScopeSchema,
    integrationId: EntityIdSchema.optional(),
    expectedIntegrationControlEpoch: z.number().int().positive().optional(),
    privateViewerPersonId: EntityIdSchema.optional(),
    asOf: InstantSchema,
  })
  .superRefine((query, context) => {
    if ((query.integrationId === undefined) !== (query.expectedIntegrationControlEpoch === undefined)) {
      context.addIssue({
        code: "custom",
        path: ["expectedIntegrationControlEpoch"],
        message: "Integration-fenced reads require both integration identity and authority epoch",
      });
    }
    if (query.scope.kind === "person" && query.privateViewerPersonId !== undefined) {
      context.addIssue({
        code: "custom",
        path: ["privateViewerPersonId"],
        message: "Person sources cannot use a conversation private view",
      });
    }
  });

const ReadPrivateEpochContextQuerySchema = z.strictObject({
  kind: z.literal("private_epoch_context"),
  participantEpochId: EntityIdSchema,
  viewerPersonId: EntityIdSchema,
  beforeSourceRevisionId: EntityIdSchema,
  asOf: InstantSchema,
  limit: z.number().int().min(1).max(24).default(12),
});

const ReadBlobQuerySchema = z
  .strictObject({
    kind: z.literal("source_blob"),
    sourceBlobId: EntityIdSchema,
    scope: SourceScopeSchema,
    privateViewerPersonId: EntityIdSchema.optional(),
    asOf: InstantSchema,
  })
  .superRefine((query, context) => {
    if (query.scope.kind === "person" && query.privateViewerPersonId !== undefined) {
      context.addIssue({
        code: "custom",
        path: ["privateViewerPersonId"],
        message: "Person sources cannot use a conversation private view",
      });
    }
  });

const ReadDerivativeQuerySchema = z
  .strictObject({
    kind: z.literal("source_derivative"),
    sourceDerivativeId: EntityIdSchema,
    scope: SourceScopeSchema,
    privateViewerPersonId: EntityIdSchema.optional(),
    asOf: InstantSchema,
  })
  .superRefine((query, context) => {
    if (query.scope.kind === "person" && query.privateViewerPersonId !== undefined) {
      context.addIssue({
        code: "custom",
        path: ["privateViewerPersonId"],
        message: "Person sources cannot use a conversation private view",
      });
    }
  });

const ReadPendingCandidatesQuerySchema = z.strictObject({
  kind: z.literal("pending_private_candidates"),
  personId: EntityIdSchema,
  asOf: InstantSchema,
  limit: z.number().int().min(1).max(200).default(50),
});

export const SourceQuerySchema = z.discriminatedUnion("kind", [
  ReadIntegrationAccessQuerySchema,
  ReadIntegrationProfileQuerySchema,
  ReadOAuthAttemptAccessQuerySchema,
  ReadCursorQuerySchema,
  ReadCalendarPrivacyQuerySchema,
  ReadSourceRevisionQuerySchema,
  ReadPrivateEpochContextQuerySchema,
  ReadBlobQuerySchema,
  ReadDerivativeQuerySchema,
  ReadPendingCandidatesQuerySchema,
]);
export type SourceQuery = z.infer<typeof SourceQuerySchema>;

export interface IntegrationView {
  readonly integrationId: string;
  readonly personId: string;
  readonly provider: string;
  readonly accountKind: IntegrationAccountKind;
  readonly activeCapabilities: readonly IntegrationCapability[];
  readonly lastAuthorizedCapabilities: readonly IntegrationCapability[];
  readonly status: IntegrationStatus;
  readonly controlEpoch: number;
  readonly connectedAt: string;
  readonly updatedAt: string;
}

export type SourceMutationResult =
  | ({ readonly kind: "integration_connected" } & IntegrationView)
  | ({ readonly kind: "integration_status_changed" } & IntegrationView)
  | {
      readonly kind: "integration_revocation_started";
      readonly integrationId: string;
      readonly controlEpoch: number;
      readonly invalidatedRevisionCount: number;
      readonly revokedCandidateCount: number;
    }
  | {
      readonly kind: "integration_revocation_completed";
      readonly integrationId: string;
      readonly controlEpoch: number;
      readonly duplicate: boolean;
    }
  | {
      readonly kind: "oauth_attempt_started";
      readonly oauthAttemptId: string;
      readonly expiresAt: string;
    }
  | {
      readonly kind: "oauth_attempt_consumed";
      readonly oauthAttemptId: string;
      readonly personId: string;
      readonly provider: string;
      readonly initiatingSessionId: string;
      readonly nonceDigest: string;
      readonly returnPath: string;
      readonly personControlEpoch: number;
      readonly requestedCapabilities: readonly IntegrationCapability[];
      readonly accountKind: IntegrationAccountKind;
      readonly reconnectTarget: z.infer<typeof IntegrationReconnectTargetSchema> | null;
    }
  | {
      readonly kind: "cursor_checkpointed";
      readonly integrationId: string;
      readonly resourceKind: string;
      readonly state: SyncCursorState;
      readonly checkpointAt: string | null;
      readonly updatedAt: string;
    }
  | {
      readonly kind: "calendar_privacy_configured";
      readonly integrationId: string;
      readonly calendarIdDigest: string;
      readonly mode: CalendarPrivacyMode;
      readonly grantVersion: number;
      readonly integrationControlEpoch: number;
    }
  | {
      readonly kind: "integration_sync_reset";
      readonly integrationId: string;
      readonly integrationControlEpoch: number;
      readonly affectedCapability: IntegrationCapability;
    }
  | {
      readonly kind: "calendar_catalog_reconciled";
      readonly integrationId: string;
      readonly integrationControlEpoch: number;
      readonly retiredCalendarCount: number;
      readonly resetRequired: boolean;
    }
  | {
      readonly kind: "source_ingested";
      readonly sourceObjectId: string;
      readonly sourceRevisionId: string;
      readonly revisionNumber: number;
      readonly contentDigest: string;
      readonly scopeDigest: string;
      readonly retentionUntil: string;
      readonly rawContentStored: boolean;
      readonly privateViewCount: number;
      readonly correlationDigest: string | null;
      readonly duplicate: boolean;
    }
  | {
      readonly kind: "blob_stored";
      readonly sourceBlobId: string;
      readonly contentDigest: string;
      readonly retentionUntil: string;
      readonly duplicate: boolean;
    }
  | {
      readonly kind: "derivative_stored";
      readonly sourceDerivativeId: string;
      readonly contentDigest: string;
      readonly retentionUntil: string;
      readonly duplicate: boolean;
    }
  | {
      readonly kind: "private_candidate_proposed";
      readonly candidateId: string;
      readonly contentDigest: string;
      readonly expiresAt: string;
      readonly duplicate: boolean;
    }
  | {
      readonly kind: "private_candidate_reviewed";
      readonly candidateId: string;
      readonly decision: "accepted" | "rejected";
      readonly reviewedAt: string;
      readonly duplicate: boolean;
    }
  | {
      readonly kind: "source_deleted";
      readonly sourceObjectId: string | null;
      readonly invalidatedRevisionCount: number;
      readonly revokedCandidateCount: number;
    }
  | {
      readonly kind: "conversation_epoch_invalidated";
      readonly participantEpochId: string;
      readonly invalidatedRevisionCount: number;
      readonly revokedCandidateCount: number;
    }
  | {
      readonly kind: "conversation_private_views_granted";
      readonly participantEpochId: string;
      readonly personId: string;
      readonly privateViewCount: number;
    }
  | {
      readonly kind: "retention_swept";
      readonly expiredRevisionCount: number;
      readonly expiredBlobCount: number;
      readonly expiredDerivativeCount: number;
      readonly expiredCandidateCount: number;
      readonly expiredOAuthAttemptCount: number;
    };

export type SourceReadResult =
  | {
      readonly kind: "integration_access";
      readonly integration: IntegrationView;
      readonly credentials: JsonObject;
    }
  | {
      readonly kind: "integration_profile";
      readonly integration: IntegrationView;
      readonly accountEmail: string;
    }
  | {
      readonly kind: "oauth_attempt_access";
      readonly oauthAttemptId: string;
      readonly personId: string;
      readonly provider: string;
      readonly initiatingSessionId: string;
      readonly pkceVerifier: string;
      readonly nonce: string;
      readonly nonceDigest: string;
      readonly returnPath: string;
      readonly personControlEpoch: number;
      readonly requestedCapabilities: readonly IntegrationCapability[];
      readonly accountKind: IntegrationAccountKind;
      readonly reconnectTarget: z.infer<typeof IntegrationReconnectTargetSchema> | null;
      readonly expiresAt: string;
    }
  | {
      readonly kind: "sync_cursor";
      readonly integrationId: string;
      readonly resourceKind: string;
      readonly state: SyncCursorState;
      readonly cursor: JsonValue | null;
      readonly checkpointAt: string | null;
      readonly updatedAt: string;
    }
  | {
      readonly kind: "calendar_privacy";
      readonly integrationId: string;
      readonly calendarIdDigest: string;
      readonly mode: CalendarPrivacyMode;
      readonly grantVersion: number;
    }
  | {
      readonly kind: "source_revision";
      readonly sourceRevisionId: string;
      readonly sourceObjectId: string;
      readonly revisionNumber: number;
      readonly scopeDigest: string;
      readonly contentDigest: string;
      readonly content: JsonObject;
      readonly occurredAt: string;
      readonly capturedAt: string;
      readonly retentionUntil: string;
      readonly accessExpiresAt: string;
    }
  | {
      readonly kind: "private_epoch_context";
      readonly participantEpochId: string;
      readonly viewerPersonId: string;
      readonly beforeSourceRevisionId: string;
      readonly accessExpiresAt: string;
      readonly revisions: readonly {
        readonly sourceRevisionId: string;
        readonly artifactKind: SourceArtifactKind;
        readonly content: JsonObject;
        readonly occurredAt: string;
        readonly capturedAt: string;
        readonly accessExpiresAt: string;
      }[];
    }
  | {
      readonly kind: "source_blob";
      readonly sourceBlobId: string;
      readonly sourceRevisionId: string;
      readonly blobKind: string;
      readonly mimeType: string;
      readonly contentDigest: string;
      readonly bytes: Uint8Array;
      readonly retentionUntil: string;
    }
  | {
      readonly kind: "source_derivative";
      readonly sourceDerivativeId: string;
      readonly sourceRevisionId: string;
      readonly derivativeKind: string;
      readonly scopeDigest: string;
      readonly contentDigest: string;
      readonly content: JsonValue;
      readonly retentionUntil: string;
    }
  | {
      readonly kind: "pending_private_candidates";
      readonly personId: string;
      readonly candidates: readonly {
        readonly candidateId: string;
        readonly candidateKind: string;
        readonly contentDigest: string;
        readonly content: JsonObject;
        readonly evidenceSourceRevisionIds: readonly string[];
        readonly confidence: number;
        readonly proposedAt: string;
        readonly expiresAt: string | null;
      }[];
    };

/**
 * The seam for encrypted, scoped source persistence. Callers supply normalized,
 * provider-neutral commands; this module owns every privacy and persistence
 * invariant needed to commit or read them safely.
 */
export interface SourceIntelligence {
  apply(command: SourceCommand): Promise<SourceMutationResult>;
  read(query: SourceQuery): Promise<SourceReadResult>;
}
