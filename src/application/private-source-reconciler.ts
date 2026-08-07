import { createHash, randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../db/client.js";
import { type PrivateBridgeSourceFrontier, PrivateSourceBridge } from "../modules/bridges/index.js";
import {
  normalizePrivateSourceReconciliation,
  PRODUCT_SKILLS,
  privateSourceReconciliationDecisionSchema,
} from "../modules/orchestration/skills.js";
import {
  gmailThreadCaseDigest,
  gmailThreadFrontierLockKey,
  type JsonObject,
  JsonObjectSchema,
  PostgresSourceIntelligence,
  privateSourceIntegrationLockKey,
  type SourceArtifactKind,
} from "../modules/sources/index.js";
import { canonicalDigest, canonicalJson } from "../shared/canonical-json.js";
import type { SecretBox } from "../shared/crypto.js";
import { ConflictError, StaleAuthorityError, UnauthorizedError } from "../shared/errors.js";
import type { PrivateSourceReconciliationProposal } from "./contracts.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

const DigestSchema = z.string().regex(/^[a-f0-9]{64}$/u);
const CitationSchema = z.strictObject({
  sourceRevisionId: z.string().uuid(),
  support: z.string().trim().min(1).max(1_000),
});
const ReconciliationDecisionSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    kind: z.literal("unchanged"),
    evidence: z.array(CitationSchema).min(1).max(32),
  }),
  z.strictObject({
    kind: z.literal("coverage_needed"),
    requiredOutcome: z.string().trim().min(1).max(2_000),
    changedFact: z.string().trim().min(1).max(2_000).nullable(),
    timeFacts: z.array(z.string().trim().min(1).max(1_000)).max(30),
    uncertainties: z.array(z.string().trim().min(1).max(1_000)).max(30),
    sensitivity: z.enum(["ordinary", "personal", "sensitive"]),
    evidence: z.array(CitationSchema).min(1).max(32),
  }),
  z.strictObject({
    kind: z.literal("coverage_cancelled"),
    reason: z.enum(["cancelled", "superseded"]),
    evidence: z.array(CitationSchema).min(1).max(32),
  }),
]);
const ReconciliationProposalSchema = z.strictObject({
  workerAttemptId: z.string().uuid(),
  anchorSourceRevisionId: z.string().uuid(),
  expectedFrontierDigest: DigestSchema,
  decision: ReconciliationDecisionSchema,
});

export type PrivateSourceNotReadyReason =
  | "calendar_frontier_pending"
  | "gmail_thread_frontier_incomplete"
  | "source_evidence_unreadable"
  | "attachment_extraction_incomplete"
  | "attachment_omitted"
  | "model_frontier_incomplete";

export interface PrivateSourceFrontierEvidence {
  readonly sourceRevisionId: string;
  readonly artifactKind: SourceArtifactKind;
  readonly occurredAt: string;
  readonly content: JsonObject;
}

export interface PrivateSourceFrontierImageEvidence {
  readonly sourceRevisionId: string;
  readonly mimeType: string;
  readonly dataBase64: string;
  readonly sha256: string;
}

export type PrivateSourceFrontierCompileResult =
  | { readonly kind: "unavailable"; readonly reason: string }
  | {
      readonly kind: "not_ready";
      readonly reason: PrivateSourceNotReadyReason;
      readonly retryable: boolean;
    }
  | {
      readonly kind: "ready";
      readonly anchorSourceRevisionId: string;
      readonly frontierDigest: string;
      readonly newestThreadRevisionId: string;
      readonly evidence: readonly PrivateSourceFrontierEvidence[];
      readonly images: readonly PrivateSourceFrontierImageEvidence[];
    };

export type PrivateSourceReconciliationResult =
  | {
      readonly kind: "duplicate";
      readonly candidateId: string | null;
    }
  | {
      readonly kind: "unchanged";
      readonly candidateId: string | null;
    }
  | {
      readonly kind: "candidate_created";
      readonly candidateId: string;
    }
  | {
      readonly kind: "loop_update_review_created" | "loop_update_review_pending";
      readonly candidateId: string;
      readonly loopId: string;
    }
  | {
      readonly kind: "cancelled";
      readonly loopId: string | null;
      readonly correction: "not_needed" | "not_authorized" | "queued";
    };

interface CurrentRevisionRow {
  readonly id: string;
  readonly owner_person_id: string;
  readonly integration_id: string;
  readonly integration_control_epoch: number | string;
  readonly provider: string;
  readonly object_kind: SourceArtifactKind;
  readonly correlation_digest: string | null;
  readonly content_digest: string;
  readonly content_ciphertext: Buffer;
  readonly occurred_at: Date;
}

interface ReadyFrontier extends Extract<PrivateSourceFrontierCompileResult, { kind: "ready" }> {
  readonly ownerPersonId: string;
  readonly integrationId: string;
  readonly integrationControlEpoch: number;
  readonly caseKeyDigest: string;
  readonly evidenceDigests: readonly {
    readonly sourceRevisionId: string;
    readonly contentDigest: string;
  }[];
}

interface CurrentImageBlobRow {
  readonly id: string;
  readonly source_revision_id: string;
  readonly blob_kind: string;
  readonly mime_type: string;
  readonly content_digest: string;
  readonly byte_length: number | string;
  readonly ciphertext: Buffer;
}

interface FrontierRow {
  readonly id: string;
  readonly version: number | string;
  readonly frontier_digest: string;
  readonly source_generation: number | string;
  readonly reconciled_generation: number | string;
  readonly current_candidate_id: string | null;
  readonly disposition: "quiet" | "candidate";
}

interface CandidateRow {
  readonly id: string;
  readonly candidate_kind: "coverage_proposal" | "coverage_loop_update_review";
  readonly content_digest: string;
  readonly content_ciphertext: Buffer;
  readonly status: "pending" | "accepted" | "rejected" | "expired" | "revoked";
}

interface WorkerProposalRow {
  readonly attempt_status: string;
  readonly skill_key: string;
  readonly skill_version: number | string;
  readonly skill_version_status: string;
  readonly output_contract: string;
  readonly output_digest: string;
  readonly output_ciphertext: Buffer;
  readonly reconciliation_status: string;
  readonly attempt_evaluation_release_id: string;
  readonly version_evaluation_release_id: string;
  readonly evaluation_status: string;
  readonly active_skill_version_id: string;
  readonly attempt_skill_version_id: string;
  readonly active_evaluation_release_id: string;
}

interface AuthorizedWorkerProposal {
  readonly decision: z.infer<typeof ReconciliationDecisionSchema>;
  readonly reconciliationStatus: "pending" | "accepted" | "partially_accepted";
}

const MAX_PUBLIC_EVIDENCE_ITEMS = 64;
const MAX_PUBLIC_EVIDENCE_CONTENT_CHARS = 96_000;
const MAX_PUBLIC_ITEM_CONTENT_CHARS = 12_000;
const MAX_PRIVATE_SOURCE_IMAGES = 5;
const MAX_PRIVATE_SOURCE_IMAGE_BYTES = 5 * 1024 * 1024;
const MAX_PRIVATE_SOURCE_TOTAL_IMAGE_BYTES = 8 * 1024 * 1024;

/**
 * Compiles and reconciles one Gmail-thread case. It owns correlation, frontier
 * readiness, encrypted evidence access, and atomic replacement; workers see a
 * bounded snapshot and may only return a digest-fenced proposal.
 */
export class PrivateSourceReconciler {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
    private readonly options: { readonly rawRetentionDays: number },
  ) {}

  public async compile(input: {
    readonly anchorSourceRevisionId: string;
    readonly requestedAt: string;
  }): Promise<PrivateSourceFrontierCompileResult> {
    const requestedAt = requireInstant(input.requestedAt);
    const compiled = await this.compileCurrent(input.anchorSourceRevisionId, requestedAt);
    return publicCompileResult(compiled);
  }

  /** Called only by FlorenceApplication.process(), with its transaction executor. */
  public async apply(
    proposalCandidate: PrivateSourceReconciliationProposal,
    reconciledAtCandidate: string,
  ): Promise<PrivateSourceReconciliationResult> {
    const proposal = ReconciliationProposalSchema.parse(proposalCandidate);
    const reconciledAt = requireInstant(reconciledAtCandidate);
    return inTransaction(this.database, async (transaction) => {
      const lockTarget = await new PrivateSourceReconciler(
        transaction,
        this.secretBox,
        this.options,
      ).locateCaseLock(proposal.anchorSourceRevisionId, reconciledAt);
      if (!lockTarget) {
        throw new StaleAuthorityError("Private source frontier is not currently available");
      }
      await transaction`
        select pg_advisory_xact_lock(
          hashtextextended(${privateSourceIntegrationLockKey(lockTarget.integrationId)}, 0)
        )
      `;
      await transaction`
        select pg_advisory_xact_lock(
          hashtextextended(${gmailThreadFrontierLockKey({
            ownerPersonId: lockTarget.ownerPersonId,
            integrationId: lockTarget.integrationId,
            caseKeyDigest: lockTarget.caseKeyDigest,
          })}, 0)
        )
      `;
      const current = await new PrivateSourceReconciler(
        transaction,
        this.secretBox,
        this.options,
      ).compileCurrent(proposal.anchorSourceRevisionId, reconciledAt);
      if (current.kind !== "ready" || current.frontierDigest !== proposal.expectedFrontierDigest) {
        throw new StaleAuthorityError("Private source frontier changed before reconciliation");
      }
      const workerProposal = await loadAuthorizedWorkerProposal(
        transaction,
        this.secretBox,
        proposal.workerAttemptId,
      );
      if (canonicalJson(workerProposal.decision) !== canonicalJson(proposal.decision)) {
        throw new UnauthorizedError("Private source proposal differs from its governed worker result");
      }
      validateCitations(proposal, current);

      const finish = async <Result extends PrivateSourceReconciliationResult>(
        result: Result,
        reconciliationStatus: "accepted" | "partially_accepted" = "accepted",
      ): Promise<Result> => {
        await settleWorkerProposal(
          transaction,
          proposal.workerAttemptId,
          workerProposal.reconciliationStatus,
          reconciliationStatus,
          reconciledAt,
        );
        return result;
      };

      const frontiers = await transaction<FrontierRow[]>`
        select id, version, frontier_digest, source_generation, reconciled_generation,
          current_candidate_id, disposition
        from private_source_frontiers
        where owner_person_id = ${current.ownerPersonId}
          and integration_id = ${current.integrationId}
          and case_kind = 'gmail_thread'
          and case_key_digest = ${current.caseKeyDigest}
        for update
      `;
      const frontier = frontiers[0] ?? null;
      if (frontier?.frontier_digest === current.frontierDigest) {
        if (Number(frontier.reconciled_generation) !== Number(frontier.source_generation)) {
          await transaction`
            update private_source_frontiers
            set reconciled_generation = source_generation, reconciled_at = ${reconciledAt},
              updated_at = ${reconciledAt}
            where id = ${frontier.id} and version = ${Number(frontier.version)}
              and source_generation = ${Number(frontier.source_generation)}
          `;
        }
        const duplicateCandidate = frontier.current_candidate_id
          ? await loadCandidate(transaction, frontier.current_candidate_id, current.ownerPersonId)
          : null;
        const updateLoopId = duplicateCandidate
          ? (openUpdateReviewContext(this.secretBox, duplicateCandidate)?.loopId ?? null)
          : null;
        if (
          updateLoopId &&
          duplicateCandidate &&
          ["pending", "accepted"].includes(duplicateCandidate.status)
        ) {
          return finish(
            {
              kind: "loop_update_review_pending",
              candidateId: duplicateCandidate.id,
              loopId: updateLoopId,
            },
            "partially_accepted",
          );
        }
        return finish({ kind: "duplicate", candidateId: frontier.current_candidate_id });
      }

      const candidate = frontier?.current_candidate_id
        ? await loadCandidate(transaction, frontier.current_candidate_id, current.ownerPersonId)
        : null;
      const bridge = new PrivateSourceBridge(transaction, this.secretBox, this.options.rawRetentionDays);

      if (proposal.decision.kind === "unchanged") {
        const updateContext = candidate ? openUpdateReviewContext(this.secretBox, candidate) : null;
        if (updateContext && candidate && ["pending", "accepted"].includes(candidate.status)) {
          await writeFrontier(transaction, {
            prior: frontier,
            current,
            currentCandidateId: candidate.id,
            disposition: "candidate",
            reconciledAt,
          });
          return finish(
            {
              kind: "loop_update_review_pending",
              candidateId: candidate.id,
              loopId: updateContext.loopId,
            },
            "partially_accepted",
          );
        }
        if (updateContext && candidate) {
          const acceptedAnchor = await resolveAcceptedLoopAnchor(
            transaction,
            this.secretBox,
            bridge,
            current.ownerPersonId,
            candidate,
          );
          if (acceptedAnchor) {
            await writeFrontier(transaction, {
              prior: frontier,
              current,
              currentCandidateId: acceptedAnchor.candidate.id,
              disposition: "candidate",
              reconciledAt,
            });
            return finish({ kind: "unchanged", candidateId: acceptedAnchor.candidate.id });
          }
        }
        const preserveCurrentCoverage =
          candidate?.candidate_kind === "coverage_proposal" &&
          ["pending", "accepted"].includes(candidate.status);
        await writeFrontier(transaction, {
          prior: frontier,
          current,
          currentCandidateId: preserveCurrentCoverage ? candidate.id : null,
          disposition: preserveCurrentCoverage ? "candidate" : "quiet",
          reconciledAt,
        });
        return finish({
          kind: "unchanged",
          candidateId: preserveCurrentCoverage ? candidate.id : null,
        });
      }

      const evidenceSourceRevisionIds = proposal.decision.evidence
        .map((citation) => citation.sourceRevisionId)
        .sort();
      let pendingCandidateRevoked = false;
      const revokePendingCandidate = async (): Promise<void> => {
        if (candidate?.status !== "pending" || pendingCandidateRevoked) return;
        await bridge.cancelPendingCandidateWork({
          ownerPersonId: current.ownerPersonId,
          candidateId: candidate.id,
          candidateContentDigest: candidate.content_digest,
          cancelledAt: reconciledAt,
        });
        const revoked = await transaction<{ readonly id: string }[]>`
          update knowledge_candidates set status = 'revoked', reviewed_at = ${reconciledAt}
          where id = ${candidate.id} and status = 'pending'
          returning id
        `;
        if (!revoked[0]) {
          throw new StaleAuthorityError("Private source candidate changed during replacement");
        }
        pendingCandidateRevoked = true;
      };
      const acceptedAnchor = candidate
        ? await resolveAcceptedLoopAnchor(
            transaction,
            this.secretBox,
            bridge,
            current.ownerPersonId,
            candidate,
          )
        : null;
      if (acceptedAnchor) {
        if (proposal.decision.kind === "coverage_cancelled") {
          await revokePendingCandidate();
          const replacementSourceFrontier = await writeFrontier(transaction, {
            prior: frontier,
            current,
            currentCandidateId: null,
            disposition: "quiet",
            reconciledAt,
          });
          const withdrawn = await bridge.withdrawAcceptedCandidate({
            ownerPersonId: current.ownerPersonId,
            candidateId: acceptedAnchor.candidate.id,
            candidateContentDigest: acceptedAnchor.candidate.content_digest,
            intent: proposal.decision.reason === "cancelled" ? "cancel" : "supersede",
            evidenceSourceRevisionIds,
            replacementSourceFrontier,
            withdrawnAt: reconciledAt,
          });
          return finish({
            kind: "cancelled",
            loopId: withdrawn.kind === "not_found" ? null : withdrawn.loopId,
            correction: withdrawn.kind === "not_found" ? "not_needed" : withdrawn.correction.kind,
          });
        }

        await revokePendingCandidate();
        const updateReview = await new PostgresSourceIntelligence(transaction, this.secretBox, {
          rawRetentionDays: this.options.rawRetentionDays,
          privateCandidateRetentionDays: 7,
        }).apply({
          kind: "propose_private_candidate",
          personId: current.ownerPersonId,
          integrationId: current.integrationId,
          expectedIntegrationControlEpoch: current.integrationControlEpoch,
          candidateKind: "coverage_loop_update_review",
          content: JsonObjectSchema.parse({
            existingLoopId: acceptedAnchor.reference.loopId,
            sourceActionIntentId: acceptedAnchor.reference.actionIntentId,
            priorCandidateId: acceptedAnchor.candidate.id,
            requiredOutcome: proposal.decision.requiredOutcome,
            changedFact: proposal.decision.changedFact,
            timeFacts: [...proposal.decision.timeFacts],
            uncertainties: [...proposal.decision.uncertainties],
            sensitivity: proposal.decision.sensitivity,
            reviewStatus: "requires_separate_owner_authorization",
            disclosureStatus: "private_owner_only",
          }),
          evidenceSourceRevisionIds,
          confidence: deterministicPrivateCandidateConfidence(proposal.decision),
          proposedAt: reconciledAt.toISOString(),
          requestedExpiresAt: new Date(reconciledAt.getTime() + 7 * 86_400_000).toISOString(),
        });
        if (updateReview.kind !== "private_candidate_proposed") {
          throw new ConflictError("Private coverage-loop update review was not created");
        }
        await writeFrontier(transaction, {
          prior: frontier,
          current,
          currentCandidateId: updateReview.candidateId,
          disposition: "candidate",
          reconciledAt,
        });
        return finish(
          {
            kind: "loop_update_review_created",
            candidateId: updateReview.candidateId,
            loopId: acceptedAnchor.reference.loopId,
          },
          "partially_accepted",
        );
      }

      await revokePendingCandidate();

      if (proposal.decision.kind === "coverage_cancelled") {
        await writeFrontier(transaction, {
          prior: frontier,
          current,
          currentCandidateId: null,
          disposition: "quiet",
          reconciledAt,
        });
        return finish({ kind: "cancelled", loopId: null, correction: "not_needed" });
      }

      const proposed = await new PostgresSourceIntelligence(transaction, this.secretBox, {
        rawRetentionDays: this.options.rawRetentionDays,
        privateCandidateRetentionDays: 7,
      }).apply({
        kind: "propose_private_candidate",
        personId: current.ownerPersonId,
        integrationId: current.integrationId,
        expectedIntegrationControlEpoch: current.integrationControlEpoch,
        candidateKind: "coverage_proposal",
        content: JsonObjectSchema.parse({
          requiredOutcome: proposal.decision.requiredOutcome,
          changedFact: proposal.decision.changedFact,
          timeFacts: [...proposal.decision.timeFacts],
          uncertainties: [...proposal.decision.uncertainties],
          sensitivity: proposal.decision.sensitivity,
          disclosureStatus: "private_owner_only",
        }),
        evidenceSourceRevisionIds,
        confidence: deterministicPrivateCandidateConfidence(proposal.decision),
        proposedAt: reconciledAt.toISOString(),
        requestedExpiresAt: new Date(reconciledAt.getTime() + 7 * 86_400_000).toISOString(),
      });
      if (proposed.kind !== "private_candidate_proposed") {
        throw new ConflictError("Private coverage candidate was not created");
      }
      await writeFrontier(transaction, {
        prior: frontier,
        current,
        currentCandidateId: proposed.candidateId,
        disposition: "candidate",
        reconciledAt,
      });
      return finish({ kind: "candidate_created", candidateId: proposed.candidateId });
    });
  }

  private async compileCurrent(
    anchorSourceRevisionId: string,
    requestedAt: Date,
  ): Promise<
    Extract<PrivateSourceFrontierCompileResult, { kind: "unavailable" | "not_ready" }> | ReadyFrontier
  > {
    const anchorRows = await this.database<CurrentRevisionRow[]>`
      select revision.id, revision.owner_person_id, object.integration_id,
        integration.control_epoch as integration_control_epoch,
        object.provider, object.object_kind, object.correlation_digest,
        revision.content_digest, revision.content_ciphertext, revision.occurred_at
      from source_revisions revision
      join source_objects object on object.id = revision.source_object_id
      join integrations integration on integration.id = object.integration_id
      join people person on person.id = revision.owner_person_id
      join integration_capabilities capability
        on capability.integration_id = integration.id and capability.capability = 'mail'
      where revision.id = ${anchorSourceRevisionId}
        and object.provider = 'gmail' and object.object_kind = 'mail_message'
        and object.status = 'active'
        and revision.revision_number = object.latest_revision_number
        and revision.revoked_at is null and revision.retention_until > ${requestedAt}
        and revision.content_ciphertext is not null
        and person.status = 'registered'
        and integration.person_id = revision.owner_person_id
        and integration.provider = 'google' and integration.status = 'active'
        and capability.status = 'active'
      for share of revision, object, integration, person, capability
    `;
    const anchor = anchorRows[0];
    if (!anchor?.correlation_digest) {
      return { kind: "unavailable", reason: "anchor_is_not_current_gmail_evidence" };
    }
    const anchorContent = decryptSourceData(this.secretBox, anchor);
    const anchorThreadId = anchorContent && stringField(anchorContent, "threadId");
    if (!anchorContent || !anchorThreadId) {
      return { kind: "unavailable", reason: "anchor_has_no_gmail_thread" };
    }
    let caseKeyDigest: string;
    try {
      caseKeyDigest = gmailThreadCaseDigest({
        integrationId: anchor.integration_id,
        threadId: anchorThreadId,
      });
    } catch {
      return { kind: "unavailable", reason: "anchor_has_invalid_gmail_thread" };
    }
    if (caseKeyDigest !== anchor.correlation_digest) {
      return { kind: "unavailable", reason: "anchor_correlation_mismatch" };
    }

    const mailRows = await this.database<CurrentRevisionRow[]>`
      select revision.id, revision.owner_person_id, object.integration_id,
        integration.control_epoch as integration_control_epoch,
        object.provider, object.object_kind, object.correlation_digest,
        revision.content_digest, revision.content_ciphertext, revision.occurred_at
      from source_revisions revision
      join source_objects object on object.id = revision.source_object_id
      join integrations integration on integration.id = object.integration_id
      where object.integration_id = ${anchor.integration_id}
        and revision.owner_person_id = ${anchor.owner_person_id}
        and object.correlation_digest = ${caseKeyDigest}
        and object.provider = 'gmail' and object.object_kind = 'mail_message'
        and object.status = 'active'
        and revision.revision_number = object.latest_revision_number
        and revision.revoked_at is null and revision.retention_until > ${requestedAt}
        and revision.content_ciphertext is not null
        and integration.status = 'active'
        and integration.control_epoch = ${Number(anchor.integration_control_epoch)}
      order by revision.occurred_at desc, revision.id desc
      limit 101
      for share of revision, object, integration
    `;
    if (mailRows.length > 100) {
      return {
        kind: "not_ready",
        reason: "gmail_thread_frontier_incomplete",
        retryable: false,
      };
    }
    if (!mailRows.some((row) => row.id === anchor.id)) {
      return { kind: "unavailable", reason: "anchor_outside_bounded_thread_frontier" };
    }

    const attachmentRows = await this.database<CurrentRevisionRow[]>`
      select revision.id, revision.owner_person_id, object.integration_id,
        integration.control_epoch as integration_control_epoch,
        object.provider, object.object_kind, object.correlation_digest,
        revision.content_digest, revision.content_ciphertext, revision.occurred_at
      from source_revisions revision
      join source_objects object on object.id = revision.source_object_id
      join integrations integration on integration.id = object.integration_id
      where object.integration_id = ${anchor.integration_id}
        and revision.owner_person_id = ${anchor.owner_person_id}
        and object.correlation_digest = ${caseKeyDigest}
        and object.provider = 'gmail.attachment'
        and object.object_kind = 'attachment_manifest'
        and object.status = 'active'
        and revision.revision_number = object.latest_revision_number
        and revision.revoked_at is null and revision.retention_until > ${requestedAt}
        and revision.content_ciphertext is not null
        and integration.status = 'active'
        and integration.control_epoch = ${Number(anchor.integration_control_epoch)}
      order by revision.occurred_at desc, revision.id desc
      limit 5001
      for share of revision, object, integration
    `;
    if (attachmentRows.length > 5_000) {
      return {
        kind: "not_ready",
        reason: "gmail_thread_frontier_incomplete",
        retryable: false,
      };
    }

    const mailById = new Map<string, { readonly row: CurrentRevisionRow; readonly data: JsonObject }>();
    for (const row of mailRows) {
      const data = decryptSourceData(this.secretBox, row);
      const threadId = data && stringField(data, "threadId");
      if (!data || !threadId) {
        return { kind: "not_ready", reason: "source_evidence_unreadable", retryable: false };
      }
      if (gmailThreadCaseDigest({ integrationId: anchor.integration_id, threadId }) !== caseKeyDigest) {
        return { kind: "unavailable", reason: "thread_correlation_mismatch" };
      }
      mailById.set(row.id, { row, data });
    }
    if (!mailById.has(anchor.id)) {
      return { kind: "unavailable", reason: "anchor_content_is_unreadable" };
    }

    const threadEvidence: Array<{
      readonly row: CurrentRevisionRow;
      readonly data: JsonObject;
    }> = [...mailById.values()];
    const frontierAttachmentEvidence: Array<{
      readonly row: CurrentRevisionRow;
      readonly data: JsonObject;
    }> = [];
    const images: PrivateSourceFrontierImageEvidence[] = [];
    const imageDigests: Array<{ readonly sourceRevisionId: string; readonly contentDigest: string }> = [];
    let imageBytes = 0;
    const expectedAttachmentKeys = new Set<string>();
    for (const parent of mailById.values()) {
      if (!isFullContentMail(parent.data)) continue;
      const inventory = parent.data.attachments;
      if (!Array.isArray(inventory)) {
        return { kind: "not_ready", reason: "source_evidence_unreadable", retryable: false };
      }
      for (const item of inventory) {
        const attachment = JsonObjectSchema.safeParse(item);
        if (!attachment.success) {
          return { kind: "not_ready", reason: "source_evidence_unreadable", retryable: false };
        }
        const partId = stringField(attachment.data, "partId");
        if (!partId) {
          return { kind: "not_ready", reason: "source_evidence_unreadable", retryable: false };
        }
        const mimeType = stringField(attachment.data, "mimeType")?.toLowerCase() ?? "";
        if (attachment.data.inline === true && mimeType.startsWith("image/")) continue;
        expectedAttachmentKeys.add(`${parent.row.id}:${partId}`);
      }
    }
    const currentAttachmentKeys = new Set<string>();
    for (const row of attachmentRows) {
      const data = decryptSourceData(this.secretBox, row);
      const parentId = data && stringField(data, "parentSourceRevisionId");
      const parent = parentId ? mailById.get(parentId) : undefined;
      if (!data) {
        return { kind: "not_ready", reason: "source_evidence_unreadable", retryable: false };
      }
      // Gmail can leave an attachment object active after its parent revision
      // was superseded or ceased to admit full content. It is then stale case
      // debris, not part of the current thread frontier.
      if (!parent || !isFullContentMail(parent.data)) continue;
      const partId = stringField(data, "partId");
      if (!partId) {
        return { kind: "not_ready", reason: "source_evidence_unreadable", retryable: false };
      }
      const attachmentKey = `${parent.row.id}:${partId}`;
      if (!expectedAttachmentKeys.has(attachmentKey)) continue;
      currentAttachmentKeys.add(attachmentKey);
      frontierAttachmentEvidence.push({ row, data });
      const attachmentKind = stringField(data, "kind");
      if (attachmentKind === "omitted" || attachmentKind === "unsupported") continue;
      const attachmentMime = (
        stringField(data, "detectedMime") ??
        stringField(data, "declaredMime") ??
        ""
      ).toLowerCase();
      if (attachmentKind === "image" || attachmentMime.startsWith("image/")) {
        if (data.inline === true || images.length >= MAX_PRIVATE_SOURCE_IMAGES) continue;
        const loaded = await this.loadBoundedImageEvidence({
          attachment: row,
          partId,
          mimeType: attachmentMime,
          requestedAt,
          maximumBytes: Math.min(
            MAX_PRIVATE_SOURCE_IMAGE_BYTES,
            MAX_PRIVATE_SOURCE_TOTAL_IMAGE_BYTES - imageBytes,
          ),
        });
        if (!loaded) continue;
        threadEvidence.push({ row, data });
        images.push(loaded.image);
        imageDigests.push({ sourceRevisionId: row.id, contentDigest: loaded.contentDigest });
        imageBytes += loaded.byteLength;
        continue;
      }
      if (!hasSubstantiveExtractedText(attachmentKind, stringField(data, "text"))) continue;
      threadEvidence.push({ row, data });
    }
    if ([...expectedAttachmentKeys].some((key) => !currentAttachmentKeys.has(key))) {
      return { kind: "not_ready", reason: "gmail_thread_frontier_incomplete", retryable: true };
    }
    threadEvidence.sort(compareEvidenceNewestFirst);
    const newestMail = [...mailById.values()].sort(compareEvidenceNewestFirst)[0];
    if (!newestMail) return { kind: "unavailable", reason: "thread_has_no_current_mail" };

    const calendar = await this.compileCalendarEvidence(anchor, requestedAt, [...mailById.values()]);
    if (calendar.kind === "not_ready") return calendar;
    const modelEvidence = [...threadEvidence, ...calendar.evidence];
    const frontierEvidence = [...mailById.values(), ...frontierAttachmentEvidence, ...calendar.evidence];
    const completeEvidence = modelEvidence
      .sort(
        (left, right) =>
          left.row.occurred_at.getTime() - right.row.occurred_at.getTime() ||
          left.row.id.localeCompare(right.row.id),
      )
      .map(({ row, data }) => ({
        sourceRevisionId: row.id,
        artifactKind: row.object_kind,
        occurredAt: row.occurred_at.toISOString(),
        content: data,
      }));
    const evidenceDigests = frontierEvidence
      .map(({ row }) => ({ sourceRevisionId: row.id, contentDigest: row.content_digest }))
      .sort((left, right) => left.sourceRevisionId.localeCompare(right.sourceRevisionId));
    imageDigests.sort((left, right) => left.sourceRevisionId.localeCompare(right.sourceRevisionId));
    const integrationControlEpoch = Number(anchor.integration_control_epoch);
    const frontierDigest = canonicalDigest({
      schemaVersion: 2,
      integrationControlEpoch,
      caseKeyDigest,
      revisions: evidenceDigests,
      imageBlobs: imageDigests,
    });
    const evidence = boundPublicEvidence(completeEvidence);
    if (evidence === null) {
      return { kind: "not_ready", reason: "model_frontier_incomplete", retryable: false };
    }
    return {
      kind: "ready",
      anchorSourceRevisionId: anchor.id,
      frontierDigest,
      newestThreadRevisionId: newestMail.row.id,
      evidence,
      images,
      ownerPersonId: anchor.owner_person_id,
      integrationId: anchor.integration_id,
      integrationControlEpoch,
      caseKeyDigest,
      evidenceDigests,
    };
  }

  private async loadBoundedImageEvidence(input: {
    readonly attachment: CurrentRevisionRow;
    readonly partId: string;
    readonly mimeType: string;
    readonly requestedAt: Date;
    readonly maximumBytes: number;
  }): Promise<{
    readonly image: PrivateSourceFrontierImageEvidence;
    readonly contentDigest: string;
    readonly byteLength: number;
  } | null> {
    if (input.maximumBytes < 1 || !input.mimeType.startsWith("image/")) return null;
    const rows = await this.database<CurrentImageBlobRow[]>`
      select blob.id, blob.source_revision_id, blob.blob_kind, blob.mime_type,
        blob.content_digest, blob.byte_length, blob.ciphertext
      from source_blobs blob
      join source_revisions revision on revision.id = blob.source_revision_id
      join source_objects object on object.id = revision.source_object_id
      join integrations integration on integration.id = object.integration_id
      where blob.source_revision_id = ${input.attachment.id}
        and blob.blob_kind = ${`gmail_attachment:${input.partId}`}
        and blob.retention_until > ${input.requestedAt}
        and blob.byte_length between 1 and ${input.maximumBytes}
        and revision.owner_person_id = ${input.attachment.owner_person_id}
        and revision.revoked_at is null and revision.retention_until > ${input.requestedAt}
        and revision.revision_number = object.latest_revision_number
        and object.integration_id = ${input.attachment.integration_id}
        and object.provider = 'gmail.attachment'
        and object.object_kind = 'attachment_manifest' and object.status = 'active'
        and integration.person_id = revision.owner_person_id
        and integration.provider = 'google' and integration.status = 'active'
        and integration.control_epoch = ${Number(input.attachment.integration_control_epoch)}
      order by blob.created_at desc, blob.id desc
      limit 2
      for share of blob, revision, object, integration
    `;
    if (rows.length !== 1) return null;
    const row = rows[0];
    if (!row || row.source_revision_id !== input.attachment.id) return null;
    const byteLength = Number(row.byte_length);
    if (!Number.isSafeInteger(byteLength) || byteLength < 1 || byteLength > input.maximumBytes) {
      return null;
    }
    const mimeType = row.mime_type.toLowerCase();
    if (mimeType !== input.mimeType || !mimeType.startsWith("image/")) return null;
    let bytes: Buffer;
    try {
      bytes = this.secretBox.decrypt(
        JSON.parse(row.ciphertext.toString("utf8")),
        `florence:source-blob:${row.id}:bytes`,
      );
    } catch {
      return null;
    }
    if (bytes.length !== byteLength) return null;
    const sha256 = createHash("sha256").update(bytes).digest("hex");
    if (sha256 !== row.content_digest) return null;
    return {
      image: {
        sourceRevisionId: input.attachment.id,
        mimeType,
        dataBase64: bytes.toString("base64"),
        sha256,
      },
      contentDigest: row.content_digest,
      byteLength,
    };
  }

  /** Finds advisory-lock identity without taking row locks in the opposite order. */
  private async locateCaseLock(
    anchorSourceRevisionId: string,
    requestedAt: Date,
  ): Promise<{
    readonly ownerPersonId: string;
    readonly integrationId: string;
    readonly caseKeyDigest: string;
  } | null> {
    const rows = await this.database<CurrentRevisionRow[]>`
      select revision.id, revision.owner_person_id, object.integration_id,
        integration.control_epoch as integration_control_epoch,
        object.provider, object.object_kind, object.correlation_digest,
        revision.content_digest, revision.content_ciphertext, revision.occurred_at
      from source_revisions revision
      join source_objects object on object.id = revision.source_object_id
      join integrations integration on integration.id = object.integration_id
      join people person on person.id = revision.owner_person_id
      join integration_capabilities capability
        on capability.integration_id = integration.id and capability.capability = 'mail'
      where revision.id = ${anchorSourceRevisionId}
        and object.provider = 'gmail' and object.object_kind = 'mail_message'
        and object.status = 'active'
        and revision.revision_number = object.latest_revision_number
        and revision.revoked_at is null and revision.retention_until > ${requestedAt}
        and revision.content_ciphertext is not null
        and person.status = 'registered'
        and integration.person_id = revision.owner_person_id
        and integration.provider = 'google' and integration.status = 'active'
        and capability.status = 'active'
    `;
    const anchor = rows[0];
    if (!anchor?.correlation_digest) return null;
    const content = decryptSourceData(this.secretBox, anchor);
    const threadId = content && stringField(content, "threadId");
    if (!threadId) return null;
    const caseKeyDigest = gmailThreadCaseDigest({
      integrationId: anchor.integration_id,
      threadId,
    });
    if (caseKeyDigest !== anchor.correlation_digest) return null;
    return {
      ownerPersonId: anchor.owner_person_id,
      integrationId: anchor.integration_id,
      caseKeyDigest,
    };
  }

  private async compileCalendarEvidence(
    anchor: CurrentRevisionRow,
    requestedAt: Date,
    mailEvidence: readonly { readonly row: CurrentRevisionRow; readonly data: JsonObject }[],
  ): Promise<
    | {
        readonly kind: "not_ready";
        readonly reason: PrivateSourceNotReadyReason;
        readonly retryable: boolean;
      }
    | {
        readonly kind: "ready";
        readonly evidence: readonly {
          readonly row: CurrentRevisionRow;
          readonly data: JsonObject;
        }[];
      }
  > {
    const calendarCapabilities = await this.database<{ readonly active: boolean }[]>`
      select exists (
        select 1 from integration_capabilities
        where integration_id = ${anchor.integration_id}
          and capability = 'calendar' and status = 'active'
      ) as active
    `;
    if (!calendarCapabilities[0]?.active) return { kind: "ready", evidence: [] };

    const catalogs = await this.database<{ readonly state: string; readonly checkpoint_at: Date | null }[]>`
      select state, checkpoint_at from sync_cursors
      where integration_id = ${anchor.integration_id} and resource_kind = 'calendar_catalog'
    `;
    if (
      !catalogs[0] ||
      catalogs[0].checkpoint_at === null ||
      !["active", "exhausted"].includes(catalogs[0].state)
    ) {
      return { kind: "not_ready", reason: "calendar_frontier_pending", retryable: true };
    }

    const grants = await this.database<
      {
        readonly calendar_id_digest: string | null;
        readonly mode: string | null;
        readonly created_at: Date;
        readonly cursor_state: string | null;
        readonly checkpoint_at: Date | null;
        readonly cursor_updated_at: Date | null;
      }[]
    >`
      select grant_row.scope->>'calendarIdDigest' as calendar_id_digest,
        grant_row.scope->>'mode' as mode, grant_row.created_at,
        cursor.state as cursor_state, cursor.checkpoint_at, cursor.updated_at as cursor_updated_at
      from integration_grants grant_row
      left join sync_cursors cursor
        on cursor.integration_id = grant_row.integration_id
        and cursor.resource_kind = 'calendar:' || (grant_row.scope->>'calendarIdDigest')
      where grant_row.integration_id = ${anchor.integration_id}
        and grant_row.grant_kind = 'calendar_privacy'
        and grant_row.status = 'active'
        and grant_row.scope->>'mode' <> 'off'
    `;
    if (
      grants.some(
        (grant) =>
          !grant.calendar_id_digest ||
          !DigestSchema.safeParse(grant.calendar_id_digest).success ||
          !["full_private", "availability_only"].includes(grant.mode ?? "") ||
          grant.checkpoint_at === null ||
          grant.cursor_updated_at === null ||
          grant.cursor_updated_at < grant.created_at ||
          !["active", "exhausted"].includes(grant.cursor_state ?? ""),
      )
    ) {
      return { kind: "not_ready", reason: "calendar_frontier_pending", retryable: true };
    }
    if (!grants.some((grant) => grant.mode === "full_private")) {
      return { kind: "ready", evidence: [] };
    }

    const rows = await this.database<CurrentRevisionRow[]>`
      select revision.id, revision.owner_person_id, object.integration_id,
        integration.control_epoch as integration_control_epoch,
        object.provider, object.object_kind, object.correlation_digest,
        revision.content_digest, revision.content_ciphertext, revision.occurred_at
      from source_revisions revision
      join source_objects object on object.id = revision.source_object_id
      join integrations integration on integration.id = object.integration_id
      where object.integration_id = ${anchor.integration_id}
        and revision.owner_person_id = ${anchor.owner_person_id}
        and object.provider = 'google.calendar' and object.object_kind = 'calendar_event'
        and object.status = 'active'
        and revision.revision_number = object.latest_revision_number
        and revision.revoked_at is null and revision.retention_until > ${requestedAt}
        and revision.content_ciphertext is not null
        and integration.status = 'active'
        and integration.control_epoch = ${Number(anchor.integration_control_epoch)}
      order by revision.occurred_at desc, revision.id desc
      limit 5001
      for share of revision, object, integration
    `;
    if (rows.length > 5_000) {
      return { kind: "not_ready", reason: "model_frontier_incomplete", retryable: false };
    }
    const horizon = new Date(requestedAt);
    horizon.setUTCMonth(horizon.getUTCMonth() + 18);
    const threadTokens = calendarMatchTokens(mailEvidence.map((entry) => entry.data));
    const relevant: Array<{ readonly row: CurrentRevisionRow; readonly data: JsonObject }> = [];
    for (const row of rows) {
      const data = decryptSourceData(this.secretBox, row);
      if (!data) {
        return { kind: "not_ready", reason: "source_evidence_unreadable", retryable: false };
      }
      // Busy-only calendars deliberately omit the provider event identifier and
      // all descriptive fields. They can establish availability, but cannot be
      // correlated to a private Gmail case without widening their approved
      // disclosure. Mixed calendar modes are therefore safe: only full-private
      // projections participate in semantic reconciliation.
      if (!stringField(data, "remoteEventId")) continue;
      const start = dateField(data, "start");
      const end = dateField(data, "end") ?? start;
      if (!hasCalendarTokenOverlap(threadTokens, data)) continue;
      const isCancellation = stringField(data, "status") === "cancelled";
      if (!isCancellation && (!start || !end || end <= requestedAt || start > horizon)) continue;
      relevant.push({ row, data });
    }
    const evidence = relevant.sort((left, right) => {
      const leftStart = dateField(left.data, "start")?.getTime() ?? Number.MAX_SAFE_INTEGER;
      const rightStart = dateField(right.data, "start")?.getTime() ?? Number.MAX_SAFE_INTEGER;
      return leftStart - rightStart || left.row.id.localeCompare(right.row.id);
    });
    if (evidence.length > 50) {
      return { kind: "not_ready", reason: "model_frontier_incomplete", retryable: false };
    }
    return { kind: "ready", evidence };
  }
}

function publicCompileResult(
  result: Extract<PrivateSourceFrontierCompileResult, { kind: "unavailable" | "not_ready" }> | ReadyFrontier,
): PrivateSourceFrontierCompileResult {
  if (result.kind !== "ready") return result;
  return {
    kind: "ready",
    anchorSourceRevisionId: result.anchorSourceRevisionId,
    frontierDigest: result.frontierDigest,
    newestThreadRevisionId: result.newestThreadRevisionId,
    evidence: result.evidence,
    images: result.images,
  };
}

function decryptSourceData(secretBox: SecretBox, row: CurrentRevisionRow): JsonObject | null {
  try {
    const envelope = JsonObjectSchema.parse(
      JSON.parse(
        secretBox
          .decrypt(
            JSON.parse(row.content_ciphertext.toString("utf8")),
            `florence:source-revision:${row.id}:content`,
          )
          .toString("utf8"),
      ),
    );
    return JsonObjectSchema.parse(envelope.data);
  } catch {
    return null;
  }
}

function isFullContentMail(content: JsonObject): boolean {
  return content.bodyRetrieval === "now" && typeof content.text === "string";
}

function stringField(content: JsonObject, key: string): string | null {
  const value = content[key];
  return typeof value === "string" && value.length > 0 ? value : null;
}

function hasSubstantiveExtractedText(kind: string | null, text: string | null): boolean {
  if (!kind || !text?.trim()) return false;
  if (kind !== "pdf") return true;
  const withoutGeneratedPageMarkers = text.replace(/^\s*\[Page \d+\]\s*$/gimu, "").trim();
  return /[\p{L}\p{N}]/u.test(withoutGeneratedPageMarkers);
}

function dateField(content: JsonObject, key: string): Date | null {
  const value = stringField(content, key);
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function compareEvidenceNewestFirst(
  left: { readonly row: CurrentRevisionRow },
  right: { readonly row: CurrentRevisionRow },
): number {
  return (
    right.row.occurred_at.getTime() - left.row.occurred_at.getTime() ||
    right.row.id.localeCompare(left.row.id)
  );
}

function validateCitations(
  proposal: z.infer<typeof ReconciliationProposalSchema>,
  current: ReadyFrontier,
): void {
  const citations = proposal.decision.evidence;
  const citationIds = citations.map((citation) => citation.sourceRevisionId);
  if (new Set(citationIds).size !== citationIds.length) {
    throw new ConflictError("Private source citations must be unique");
  }
  const currentIds = new Set(current.evidence.map((evidence) => evidence.sourceRevisionId));
  if (citationIds.some((id) => !currentIds.has(id))) {
    throw new UnauthorizedError("Private source proposal cited evidence outside the current frontier");
  }
  if (
    proposal.decision.kind === "coverage_cancelled" &&
    !citationIds.includes(current.newestThreadRevisionId)
  ) {
    throw new ConflictError("Coverage cancellation must cite the newest current Gmail message");
  }
}

async function loadAuthorizedWorkerProposal(
  transaction: Transaction,
  secretBox: SecretBox,
  workerAttemptId: string,
): Promise<AuthorizedWorkerProposal> {
  const rows = await transaction<WorkerProposalRow[]>`
    select attempt.status as attempt_status, skill.skill_key,
      version.version as skill_version, version.status as skill_version_status,
      result.output_contract, result.output_digest, result.output_ciphertext,
      result.reconciliation_status,
      attempt.evaluation_release_id as attempt_evaluation_release_id,
      version.evaluation_release_id as version_evaluation_release_id,
      evaluation.status as evaluation_status,
      release.skill_version_id as active_skill_version_id,
      attempt.skill_version_id as attempt_skill_version_id,
      release.evaluation_release_id as active_evaluation_release_id
    from worker_attempts attempt
    join worker_results result on result.worker_attempt_id = attempt.id
    join skill_versions version on version.id = attempt.skill_version_id
    join skills skill on skill.id = version.skill_id
    join evaluation_releases evaluation on evaluation.id = attempt.evaluation_release_id
    join skill_release_events release
      on release.skill_id = skill.id and release.channel = 'production' and release.active
    where attempt.id = ${workerAttemptId}
    for update of attempt, result
  `;
  const row = rows[0];
  const governedSkill = PRODUCT_SKILLS.privateSourceReconcile;
  if (!row) throw new UnauthorizedError("Governed private source worker result does not exist");
  if (
    row.attempt_status !== "succeeded" ||
    row.skill_key !== governedSkill.id ||
    Number(row.skill_version) !== governedSkill.version ||
    row.skill_version_status !== "approved" ||
    row.output_contract !== governedSkill.outputSchemaName ||
    row.evaluation_status !== "active" ||
    row.attempt_evaluation_release_id !== row.version_evaluation_release_id ||
    row.active_skill_version_id !== row.attempt_skill_version_id ||
    row.active_evaluation_release_id !== row.attempt_evaluation_release_id
  ) {
    throw new UnauthorizedError("Private source worker attempt is not the active governed release");
  }
  if (
    !(["pending", "accepted", "partially_accepted"] as const).includes(
      row.reconciliation_status as "pending" | "accepted" | "partially_accepted",
    )
  ) {
    throw new UnauthorizedError("Private source worker result is no longer commit-eligible");
  }
  let parsed: z.infer<typeof ReconciliationDecisionSchema>;
  try {
    const plaintext = secretBox
      .decrypt(JSON.parse(row.output_ciphertext.toString("utf8")), "worker-result")
      .toString("utf8");
    const raw = JSON.parse(plaintext) as unknown;
    if (canonicalDigest(raw) !== row.output_digest) {
      throw new Error("digest mismatch");
    }
    parsed = normalizePrivateSourceReconciliation(privateSourceReconciliationDecisionSchema.parse(raw));
  } catch {
    throw new UnauthorizedError("Private source worker result could not be authenticated");
  }
  return {
    decision: parsed,
    reconciliationStatus: row.reconciliation_status as AuthorizedWorkerProposal["reconciliationStatus"],
  };
}

async function settleWorkerProposal(
  transaction: Transaction,
  workerAttemptId: string,
  currentStatus: AuthorizedWorkerProposal["reconciliationStatus"],
  nextStatus: "accepted" | "partially_accepted",
  reconciledAt: Date,
): Promise<void> {
  if (currentStatus !== "pending") return;
  const updated = await transaction<{ readonly id: string }[]>`
    update worker_results
    set reconciliation_status = ${nextStatus}, reconciled_at = ${reconciledAt}
    where worker_attempt_id = ${workerAttemptId} and reconciliation_status = 'pending'
    returning id
  `;
  if (!updated[0]) throw new StaleAuthorityError("Private source worker result settled concurrently");
}

async function loadCandidate(
  transaction: Transaction,
  candidateId: string,
  ownerPersonId: string,
): Promise<CandidateRow | null> {
  const rows = await transaction<CandidateRow[]>`
    select id, candidate_kind, content_digest, content_ciphertext, status
    from knowledge_candidates
    where id = ${candidateId} and owner_person_id = ${ownerPersonId}
      and scope_kind = 'person'
      and candidate_kind in ('coverage_proposal', 'coverage_loop_update_review')
  `;
  return rows[0] ?? null;
}

async function resolveAcceptedLoopAnchor(
  transaction: Transaction,
  secretBox: SecretBox,
  bridge: PrivateSourceBridge,
  ownerPersonId: string,
  candidate: CandidateRow,
): Promise<{
  readonly candidate: CandidateRow;
  readonly reference: { readonly actionIntentId: string; readonly loopId: string };
} | null> {
  if (candidate.candidate_kind === "coverage_proposal") {
    if (candidate.status !== "accepted") return null;
    const reference = await bridge.resolveAcceptedCandidateLoop({
      ownerPersonId,
      candidateId: candidate.id,
      candidateContentDigest: candidate.content_digest,
    });
    return reference ? { candidate, reference } : null;
  }

  const update = openUpdateReviewContext(secretBox, candidate);
  if (!update) return null;
  const prior = await loadCandidate(transaction, update.priorCandidateId, ownerPersonId);
  if (prior?.candidate_kind !== "coverage_proposal" || prior.status !== "accepted") {
    throw new StaleAuthorityError("Coverage-loop update review lost its accepted source candidate");
  }
  const reference = await bridge.resolveAcceptedCandidateLoop({
    ownerPersonId,
    candidateId: prior.id,
    candidateContentDigest: prior.content_digest,
  });
  if (
    !reference ||
    reference.loopId !== update.loopId ||
    reference.actionIntentId !== update.actionIntentId
  ) {
    throw new StaleAuthorityError("Coverage-loop update review no longer matches its exact loop");
  }
  return { candidate: prior, reference };
}

function openUpdateReviewContext(
  secretBox: SecretBox,
  candidate: CandidateRow,
): {
  readonly loopId: string;
  readonly actionIntentId: string;
  readonly priorCandidateId: string;
} | null {
  if (candidate.candidate_kind !== "coverage_loop_update_review") return null;
  try {
    const content = JsonObjectSchema.parse(
      JSON.parse(
        secretBox
          .decrypt(
            JSON.parse(candidate.content_ciphertext.toString("utf8")),
            `florence:knowledge-candidate:${candidate.id}:content`,
          )
          .toString("utf8"),
      ),
    );
    return {
      loopId: z.string().uuid().parse(content.existingLoopId),
      actionIntentId: z.string().uuid().parse(content.sourceActionIntentId),
      priorCandidateId: z.string().uuid().parse(content.priorCandidateId),
    };
  } catch {
    throw new UnauthorizedError("Private coverage-loop update review could not be authenticated");
  }
}

async function writeFrontier(
  transaction: Transaction,
  input: {
    readonly prior: FrontierRow | null;
    readonly current: ReadyFrontier;
    readonly currentCandidateId: string | null;
    readonly disposition: "quiet" | "candidate";
    readonly reconciledAt: Date;
  },
): Promise<PrivateBridgeSourceFrontier> {
  const evidenceIds = input.current.evidenceDigests.map((evidence) => evidence.sourceRevisionId).sort();
  if (input.prior) {
    const rows = await transaction<
      {
        readonly id: string;
        readonly integration_id: string;
        readonly case_key_digest: string;
        readonly version: number | string;
        readonly frontier_digest: string;
        readonly source_generation: number | string;
      }[]
    >`
      update private_source_frontiers
      set version = version + 1, frontier_digest = ${input.current.frontierDigest},
        reconciled_generation = source_generation,
        evidence_source_revision_ids = ${transaction.array(evidenceIds)}::uuid[],
        current_candidate_id = ${input.currentCandidateId}, disposition = ${input.disposition},
        reconciled_at = ${input.reconciledAt}, updated_at = ${input.reconciledAt}
      where id = ${input.prior.id}
      returning id, integration_id, case_key_digest, version, frontier_digest, source_generation
    `;
    return privateBridgeSourceFrontier(rows[0], input.current.integrationControlEpoch);
  }
  const frontierId = randomUUID();
  const rows = await transaction<
    {
      readonly id: string;
      readonly integration_id: string;
      readonly case_key_digest: string;
      readonly version: number | string;
      readonly frontier_digest: string;
      readonly source_generation: number | string;
    }[]
  >`
    insert into private_source_frontiers (
      id, owner_person_id, integration_id, case_kind, case_key_digest,
      version, frontier_digest, source_generation, reconciled_generation,
      evidence_source_revision_ids,
      current_candidate_id, disposition, reconciled_at, created_at, updated_at
    ) values (
      ${frontierId}, ${input.current.ownerPersonId}, ${input.current.integrationId},
      'gmail_thread', ${input.current.caseKeyDigest}, 1, ${input.current.frontierDigest}, 0, 0,
      ${transaction.array(evidenceIds)}::uuid[], ${input.currentCandidateId},
      ${input.disposition}, ${input.reconciledAt}, ${input.reconciledAt}, ${input.reconciledAt}
    )
    returning id, integration_id, case_key_digest, version, frontier_digest, source_generation
  `;
  return privateBridgeSourceFrontier(rows[0], input.current.integrationControlEpoch);
}

function privateBridgeSourceFrontier(
  row:
    | {
        readonly id: string;
        readonly integration_id: string;
        readonly case_key_digest: string;
        readonly version: number | string;
        readonly frontier_digest: string;
        readonly source_generation: number | string;
      }
    | undefined,
  integrationControlEpoch: number,
): PrivateBridgeSourceFrontier {
  if (!row) throw new StaleAuthorityError("Private source frontier could not be committed");
  return {
    frontierId: row.id,
    integrationId: row.integration_id,
    integrationControlEpoch,
    caseKind: "gmail_thread",
    caseKeyDigest: row.case_key_digest,
    version: Number(row.version),
    frontierDigest: row.frontier_digest,
    sourceGeneration: Number(row.source_generation),
  };
}

function deterministicPrivateCandidateConfidence(
  decision: Extract<z.infer<typeof ReconciliationDecisionSchema>, { kind: "coverage_needed" }>,
): number {
  const evidenceBoost = Math.min(Math.max(decision.evidence.length - 1, 0), 4) * 0.03;
  const timeFactBoost = Math.min(decision.timeFacts.length, 2) * 0.02;
  const changedFactBoost = decision.changedFact === null ? 0 : 0.02;
  const uncertaintyPenalty = Math.min(decision.uncertainties.length, 5) * 0.04;
  const confidence = 0.76 + evidenceBoost + timeFactBoost + changedFactBoost - uncertaintyPenalty;
  return Number(Math.min(0.94, Math.max(0.55, confidence)).toFixed(4));
}

function requireInstant(value: string): Date {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new ConflictError("Expected a valid reconciliation instant");
  return parsed;
}

function boundPublicEvidence(
  complete: readonly PrivateSourceFrontierEvidence[],
): readonly PrivateSourceFrontierEvidence[] | null {
  if (complete.length > MAX_PUBLIC_EVIDENCE_ITEMS) return null;
  let totalChars = 0;
  for (const evidence of complete) {
    const chars = canonicalJson(evidence.content).length;
    if (chars > MAX_PUBLIC_ITEM_CONTENT_CHARS) return null;
    totalChars += chars;
    if (totalChars > MAX_PUBLIC_EVIDENCE_CONTENT_CHARS) return null;
  }
  return complete;
}

const CALENDAR_TOKEN_STOP_WORDS = new Set([
  "about",
  "after",
  "again",
  "calendar",
  "could",
  "event",
  "family",
  "from",
  "have",
  "into",
  "please",
  "school",
  "that",
  "their",
  "there",
  "these",
  "this",
  "with",
  "would",
]);

function calendarMatchTokens(contents: readonly JsonObject[]): ReadonlySet<string> {
  const fields = contents.flatMap((content) =>
    ["subject", "snippet", "text", "from"].flatMap((key) => {
      const value = stringField(content, key);
      return value ? [value] : [];
    }),
  );
  return new Set(tokenizeCalendarText(fields.join(" ")).slice(0, 128));
}

function hasCalendarTokenOverlap(threadTokens: ReadonlySet<string>, event: JsonObject): boolean {
  if (threadTokens.size === 0) return false;
  const eventText = ["title", "description", "location"].flatMap((key) => {
    const value = stringField(event, key);
    return value ? [value] : [];
  });
  const overlaps = tokenizeCalendarText(eventText.join(" ")).filter((token) => threadTokens.has(token));
  return overlaps.some((token) => token.length >= 7) || new Set(overlaps).size >= 2;
}

function tokenizeCalendarText(value: string): string[] {
  return [
    ...new Set(
      (value.toLowerCase().match(/[\p{L}\p{N}]{4,}/gu) ?? []).filter(
        (token) => !CALENDAR_TOKEN_STOP_WORDS.has(token),
      ),
    ),
  ].sort();
}

function inTransaction<Result>(
  executor: Executor,
  operation: (transaction: Transaction) => Promise<Result>,
): Promise<Result> {
  return "begin" in executor
    ? (executor.begin(operation) as unknown as Promise<Result>)
    : operation(executor);
}
