import { randomUUID } from "node:crypto";
import { Temporal } from "@js-temporal/polyfill";
import type { TransactionSql } from "postgres";
import { extractDocument } from "../adapters/content/extract.js";
import {
  LinqAttachmentError,
  type LinqChatSnapshot,
  type LinqDownloadedAttachment,
  type LinqMessageReceivedEvent,
} from "../adapters/linq/index.js";
import type { AppEnvelope, ProcessReceipt } from "../application/contracts.js";
import { reconcileCoverageTimers } from "../application/coverage-timer-reconciliation.js";
import type { StoredLinqEvent } from "../application/florence-application.js";
import type { FlorenceConfig } from "../config.js";
import type { Database } from "../db/client.js";
import { PrivateSourceBridge } from "../modules/bridges/index.js";
import {
  type ConversationAuthoritySnapshot,
  PostgresConversationAuthority,
} from "../modules/conversations/index.js";
import {
  type CoverageLoop,
  createCoverageLoop,
  PostgresCoordination,
} from "../modules/coordination/index.js";
import { EffectOutbox } from "../modules/effects/index.js";
import type { WorkerRuntime } from "../modules/orchestration/contracts.js";
import { GENERAL_ANSWER_SKILL, PRODUCT_SKILLS } from "../modules/orchestration/skills.js";
import { JsonObjectSchema, PostgresSourceIntelligence, type SourceScope } from "../modules/sources/index.js";
import type { SecretBox } from "../shared/crypto.js";

type Transaction = TransactionSql<Record<string, never>>;

interface EventRow {
  id: string;
  provider_event_id: string;
  envelope_ciphertext: Buffer;
}

interface CoverageResponseTargetRow {
  readonly id: string;
  readonly state: string;
  readonly proposed_holder_person_id: string | null;
  readonly acknowledged_by_person_id: string | null;
  readonly destination_conversation_id: string;
  readonly participant_epoch_id: string;
  readonly participant_set_digest: string;
  readonly external_channel_id: string;
  readonly household_id: string;
  readonly household_control_epoch: number | string;
  readonly household_timezone: string;
}

interface MessageContext {
  readonly row: EventRow;
  readonly record: StoredLinqEvent;
  readonly event: LinqMessageReceivedEvent;
  readonly text: string;
  readonly sourceRevisionId: string;
  readonly evidenceSourceRevisionIds: readonly string[];
  readonly images: readonly { mimeType: string; dataBase64: string; sha256: string }[];
  readonly snapshot: ConversationAuthoritySnapshot;
  readonly household: { id: string; controlEpoch: number; timezone: string } | null;
}

interface CurrentCoverageContext {
  readonly loop: CoverageLoop;
}

interface LinqAttachmentReader {
  fetchAttachment(
    providerAttachmentId: string,
    options?: { maxBytes?: number; signal?: AbortSignal },
  ): Promise<LinqDownloadedAttachment>;
  getChat?(providerChatId: string, signal?: AbortSignal): Promise<LinqChatSnapshot>;
}

interface ApplicationMutationProcessor {
  process(input: AppEnvelope): Promise<ProcessReceipt>;
}

export class FlorenceOrchestrator {
  readonly #sources: PostgresSourceIntelligence;

  public constructor(
    private readonly database: Database,
    private readonly config: FlorenceConfig,
    private readonly secretBox: SecretBox,
    private readonly workers: WorkerRuntime,
    private readonly attachmentReader: LinqAttachmentReader | null = null,
    private readonly mutationProcessor: ApplicationMutationProcessor | null = null,
  ) {
    this.#sources = new PostgresSourceIntelligence(database, secretBox, {
      rawRetentionDays: config.defaults.rawSourceRetentionDays,
      privateCandidateRetentionDays: 7,
    });
  }

  public async processLinqMessage(internalProviderEventId: string): Promise<string> {
    const context = await this.compileLinqContext(internalProviderEventId);
    if (!context) return "stale_or_ineligible";

    const replyTargetLoopId = await this.loadReplyTargetCoverageLoopId(context);
    const acknowledgment = await this.tryExplicitCoverageResponse(context, replyTargetLoopId);
    if (acknowledgment) return acknowledgment;

    const currentCoverage = await this.loadCurrentCoverageContext(context);

    const need = await this.workers.run({
      attemptId: randomUUID(),
      taskVersionId: randomUUID(),
      skill: PRODUCT_SKILLS.needInterpret,
      authorizedContext: [
        `Current instant: ${new Date().toISOString()}`,
        `Conversation audience: ${context.record.routing.chatKind}`,
        `Exact source revision ID: ${context.sourceRevisionId}`,
        `Current exact-audience coverage loops: ${JSON.stringify(
          currentCoverage.map(({ loop }) => ({
            loopId: loop.loopId,
            state: loop.state,
            minimumSharedMeaning: loop.minimumSharedMeaning,
            proposedHolderPersonId: loop.proposedHolderPersonId,
            acknowledgedByPersonId: loop.acknowledgment?.personId ?? null,
            unresolvedFacts: loop.unresolvedFacts,
            eventAt: loop.timing.eventAt,
            deadlineAt: loop.timing.deadlineAt,
          })),
        )}`,
        `Exact replied-to coverage loop ID: ${replyTargetLoopId ?? "none"}`,
        `Message: ${context.text}`,
        ...(context.images.length > 0
          ? [`Attached images available to inspect: ${context.images.length}`]
          : []),
      ].join("\n"),
      ...(context.images.length > 0 ? { images: context.images } : {}),
      goal: "Determine whether this exact admitted message creates or changes a family coverage need.",
      deadline: new Date(Date.now() + 45_000),
      budget: { maxModelCalls: 1, maxOutputTokens: 1_500 },
    });
    if (need.status !== "proposed" || !need.proposal) {
      await this.workers.reconcile(need.attemptId, "rejected");
      return "interpretation_failed";
    }

    if (need.proposal.disposition === "propose_coverage") {
      const provisionalDisposition = await this.resolveProvisionalCoverage(
        context,
        need.proposal,
        currentCoverage,
        replyTargetLoopId,
      );
      const changedFactDisposition = provisionalDisposition
        ? null
        : await this.applyChangedFactToCurrentLoop(context, need.proposal, currentCoverage);
      const disposition =
        provisionalDisposition ??
        changedFactDisposition ??
        (await this.proposeCoverage(context, need.proposal));
      await this.workers.reconcile(
        need.attemptId,
        disposition.includes("failed") ? "rejected" : disposition.includes("stale") ? "stale" : "accepted",
      );
      return disposition;
    }
    if (need.proposal.disposition === "private_review") {
      if (!context.record.routing.senderPersonId) return "private_review_without_owner";
      if (context.record.routing.chatKind !== "direct") {
        await this.workers.reconcile(need.attemptId, "accepted");
        return "group_review_kept_in_chat_scope";
      }
      await this.#sources.apply({
        kind: "propose_private_candidate",
        personId: context.record.routing.senderPersonId,
        candidateKind: "family_message_review",
        content: jsonObject({
          requiredOutcome: need.proposal.requiredOutcome,
          changedFact: need.proposal.changedFact,
          uncertainties: need.proposal.uncertainties,
        }),
        evidenceSourceRevisionIds: [...context.evidenceSourceRevisionIds],
        confidence: 0.65,
        proposedAt: new Date().toISOString(),
        requestedExpiresAt: new Date(Date.now() + 7 * 86_400_000).toISOString(),
      });
      await this.workers.reconcile(need.attemptId, "accepted");
      return "private_review_created";
    }

    if (isExplicitQuestion(context)) {
      await this.workers.reconcile(need.attemptId, "partially_accepted");
      return this.answerGeneralQuestion(context);
    }
    await this.workers.reconcile(need.attemptId, "accepted");
    return "quiet_ignore";
  }

  /** Private integrations can propose family meaning, but never disclose it without a bridge approval. */
  public async processPrivateSourceRevision(sourceRevisionId: string, personId: string): Promise<string> {
    const source = await this.#sources.read({
      kind: "source_revision",
      sourceRevisionId,
      scope: { kind: "person", personId },
      asOf: new Date().toISOString(),
    });
    if (source.kind !== "source_revision") return "source_unavailable";
    const images = await this.loadAuthorizedPrivateImages(sourceRevisionId, personId);
    const proposal = await this.workers.run({
      attemptId: randomUUID(),
      taskVersionId: randomUUID(),
      skill: PRODUCT_SKILLS.needInterpret,
      authorizedContext: [
        `Current instant: ${new Date().toISOString()}`,
        `Exact source revision ID: ${sourceRevisionId}`,
        `Private source content: ${JSON.stringify(source.content)}`,
        ...(images.length > 0 ? [`Attached images available to inspect: ${images.length}`] : []),
      ].join("\n"),
      ...(images.length > 0 ? { images } : {}),
      goal: "Determine whether this private source contains a current family coverage need or useful private review item.",
      deadline: new Date(Date.now() + 60_000),
      budget: { maxModelCalls: 1, maxOutputTokens: 1_500 },
    });
    if (proposal.status !== "proposed" || !proposal.proposal) {
      await this.workers.reconcile(proposal.attemptId, "rejected");
      return "private_source_interpretation_failed";
    }
    if (proposal.proposal.disposition === "ignore") {
      await this.workers.reconcile(proposal.attemptId, "accepted");
      return "private_source_quiet_ignore";
    }
    const candidate = await this.#sources.apply({
      kind: "propose_private_candidate",
      personId,
      candidateKind:
        proposal.proposal.disposition === "propose_coverage" ? "coverage_proposal" : "private_review",
      content: jsonObject({
        requiredOutcome: proposal.proposal.requiredOutcome,
        changedFact: proposal.proposal.changedFact,
        timeFacts: proposal.proposal.timeFacts,
        uncertainties: proposal.proposal.uncertainties,
        sensitivity: proposal.proposal.sensitivity,
        disclosureStatus: "private_owner_only",
      }),
      evidenceSourceRevisionIds: [sourceRevisionId],
      confidence: proposal.proposal.disposition === "propose_coverage" ? 0.85 : 0.65,
      proposedAt: new Date().toISOString(),
      requestedExpiresAt: new Date(Date.now() + 7 * 86_400_000).toISOString(),
    });
    const created = candidate.kind === "private_candidate_proposed";
    await this.workers.reconcile(proposal.attemptId, created ? "accepted" : "rejected");
    if (!created) return "private_candidate_failed";
    if (candidate.kind === "private_candidate_proposed" && candidate.candidateId) {
      const standingIntent = await new PrivateSourceBridge(
        this.database,
        this.secretBox,
        this.config.defaults.rawSourceRetentionDays,
      ).tryPrepareStandingCandidate(personId, candidate.candidateId);
      if (standingIntent) return "private_candidate_matched_standing_rule";
    }
    return "private_candidate_created";
  }

  public async proposePrivateBridge(actionIntentId: string): Promise<string> {
    if (!this.mutationProcessor) throw new Error("Private bridge mutation seam is not configured");
    const bridge = new PrivateSourceBridge(
      this.database,
      this.secretBox,
      this.config.defaults.rawSourceRetentionDays,
    );
    const context = await bridge.loadProposalContext(actionIntentId);
    const participantLabels = await this.loadCurrentParticipantLabels(context.currentParticipantPersonIds);
    const minimum = await this.workers.run({
      attemptId: randomUUID(),
      taskVersionId: randomUUID(),
      skill: PRODUCT_SKILLS.minimumDisclosure,
      authorizedContext: [
        `Current instant: ${new Date().toISOString()}`,
        `Exact destination participant epoch ID: ${context.destination.participantEpochId}`,
        `Exact evidence source revision IDs: ${context.evidenceSourceRevisionIds.join(", ")}`,
        `Private derived candidate (not destination content): ${JSON.stringify(context.candidate)}`,
        "The source owner has not approved disclosure yet.",
        "Propose one short family-facing meaning. Exclude raw content, explanations, sender identity, and unrelated detail.",
      ].join("\n"),
      goal: "Propose the minimum family meaning for one exact current group epoch.",
      deadline: new Date(Date.now() + 45_000),
      budget: { maxModelCalls: 1, maxOutputTokens: 1_000 },
    });
    if (minimum.status !== "proposed" || !minimum.proposal) {
      await this.workers.reconcile(minimum.attemptId, "rejected");
      await bridge.cancelPendingProposal(actionIntentId);
      return "private_bridge_minimum_disclosure_failed";
    }
    const commitment = await this.workers.run({
      attemptId: randomUUID(),
      taskVersionId: randomUUID(),
      skill: PRODUCT_SKILLS.commitmentPropose,
      authorizedContext: [
        `Current instant: ${new Date().toISOString()}`,
        `Household time zone: ${context.destination.timeZone}`,
        `Current participant person IDs: ${context.currentParticipantPersonIds.join(", ")}`,
        `Current participant label-to-person-ID map: ${JSON.stringify(participantLabels)}`,
        `Exact source revision IDs: ${context.evidenceSourceRevisionIds.join(", ")}`,
        `Approved-for-review minimum meaning proposal: ${minimum.proposal.minimumMeaning}`,
        `Private derived candidate: ${JSON.stringify(context.candidate)}`,
        "Do not treat any person as committed. Propose only the open coverage outcome and timing.",
      ].join("\n"),
      goal: "Propose a coverage loop from the minimum meaning without sending or assigning it.",
      deadline: new Date(Date.now() + 45_000),
      budget: { maxModelCalls: 1, maxOutputTokens: 1_200 },
    });
    if (commitment.status !== "proposed" || !commitment.proposal) {
      await this.workers.reconcile(minimum.attemptId, "rejected");
      await this.workers.reconcile(commitment.attemptId, "rejected");
      await bridge.cancelPendingProposal(actionIntentId);
      return "private_bridge_commitment_failed";
    }
    try {
      const receipt = await this.mutationProcessor.process({
        kind: "private_bridge.proposal",
        proposal: {
          actionIntentId,
          minimumDisclosure: minimum.proposal,
          commitment: commitment.proposal,
        },
      });
      await this.workers.reconcile(minimum.attemptId, "accepted");
      await this.workers.reconcile(commitment.attemptId, "accepted");
      return receipt.disposition;
    } catch (error) {
      await this.workers.reconcile(minimum.attemptId, "stale");
      await this.workers.reconcile(commitment.attemptId, "stale");
      await bridge.cancelPendingProposal(actionIntentId);
      throw error;
    }
  }

  private async loadAuthorizedPrivateImages(sourceRevisionId: string, personId: string) {
    const blobs = await this.database<
      { id: string; mime_type: string; byte_length: number | string; content_digest: string }[]
    >`
      select blob.id, blob.mime_type, blob.byte_length, blob.content_digest
      from source_blobs blob
      join source_revisions revision on revision.id = blob.source_revision_id
      where blob.source_revision_id = ${sourceRevisionId}
        and revision.owner_person_id = ${personId}
        and revision.revoked_at is null and revision.retention_until > now()
        and blob.retention_until > now() and blob.mime_type like 'image/%'
        and blob.byte_length <= ${5 * 1024 * 1024}
      order by blob.created_at limit 3
    `;
    const images: { mimeType: string; dataBase64: string; sha256: string }[] = [];
    let totalBytes = 0;
    for (const blob of blobs) {
      const byteLength = Number(blob.byte_length);
      if (totalBytes + byteLength > 8 * 1024 * 1024) break;
      const opened = await this.#sources.read({
        kind: "source_blob",
        sourceBlobId: blob.id,
        scope: { kind: "person", personId },
        asOf: new Date().toISOString(),
      });
      if (opened.kind !== "source_blob") continue;
      images.push({
        mimeType: blob.mime_type,
        dataBase64: Buffer.from(opened.bytes).toString("base64"),
        sha256: blob.content_digest,
      });
      totalBytes += byteLength;
    }
    return images;
  }

  private async loadCurrentParticipantLabels(
    currentParticipantPersonIds: readonly string[],
  ): Promise<readonly { label: string; personId: string }[]> {
    const exactIds = [...new Set(currentParticipantPersonIds)];
    if (exactIds.length === 0) return [];
    const rows = await this.database<
      { readonly id: string; readonly display_name_ciphertext: Buffer | null }[]
    >`
      select id, display_name_ciphertext from people
      where id = any(${this.database.array(exactIds)}::uuid[]) and status = 'registered'
    `;
    const opened = rows.flatMap((row) => {
      if (!row.display_name_ciphertext) return [];
      try {
        const label = this.secretBox
          .decrypt(JSON.parse(row.display_name_ciphertext.toString("utf8")), `person-display-name:${row.id}`)
          .toString("utf8")
          .trim();
        return label ? [{ label, personId: row.id }] : [];
      } catch {
        return [];
      }
    });
    const labelCounts = new Map<string, number>();
    for (const entry of opened) {
      const key = entry.label.toLowerCase();
      labelCounts.set(key, (labelCounts.get(key) ?? 0) + 1);
    }
    return opened
      .filter((entry) => labelCounts.get(entry.label.toLowerCase()) === 1)
      .sort((left, right) => left.label.localeCompare(right.label));
  }

  private async compileLinqContext(internalProviderEventId: string): Promise<MessageContext | null> {
    const rows = await this.database<EventRow[]>`
      select id, provider_event_id, envelope_ciphertext from provider_events
      where id = ${internalProviderEventId} and provider = 'linq' and processing_status = 'processed'
    `;
    const row = rows[0];
    if (!row) return null;
    const record = JSON.parse(
      this.secretBox
        .decrypt(
          JSON.parse(row.envelope_ciphertext.toString("utf8")),
          `provider-event:${row.provider_event_id}`,
        )
        .toString("utf8"),
    ) as StoredLinqEvent;
    if (record.classification !== "full" || record.event?.eventType !== "linq.message.received") return null;
    const conversations = new PostgresConversationAuthority(this.database);
    const snapshot = await conversations.snapshot(record.routing.conversationId);
    if (
      snapshot.participantEpochId !== record.routing.participantEpochId ||
      snapshot.participantSetDigest !== record.routing.appParticipantDigest ||
      snapshot.conversationStatus !== "active"
    )
      return null;

    const event = record.event;
    const messageText = event.message.parts
      .flatMap((part) => (part.kind === "text" ? [part.text] : part.kind === "link" ? [part.url] : []))
      .join("\n")
      .trim();
    const attachmentReferences = event.message.parts.flatMap((part) =>
      part.kind === "attachment"
        ? [
            {
              id: part.providerAttachmentId,
              filename: part.filename ?? null,
              mediaType: part.mediaType ?? null,
              sizeBytes: part.sizeBytes ?? null,
            },
          ]
        : [],
    );
    if (!messageText && attachmentReferences.length === 0) return null;
    const ownerPersonId = record.routing.chatKind === "direct" ? record.routing.senderPersonId : null;
    const scope: SourceScope = ownerPersonId
      ? { kind: "person", personId: ownerPersonId }
      : { kind: "conversation_epoch", participantEpochId: record.routing.participantEpochId };
    const retentionUntil = new Date(Date.now() + this.config.defaults.rawSourceRetentionDays * 86_400_000);
    const ingested = await this.#sources.apply({
      kind: "ingest_source",
      integrationId: null,
      artifactKind: "conversation_message",
      origin: {
        system: "linq",
        remoteObjectId: event.message.providerMessageId,
      },
      scope,
      content: jsonObject({
        text: messageText,
        sentAt: event.message.sentAt,
        replyTo: event.message.replyTo ?? null,
        attachmentReferences,
      }),
      occurredAt: event.message.sentAt,
      capturedAt: new Date().toISOString(),
      requestedRetentionUntil: retentionUntil.toISOString(),
    });
    if (ingested.kind !== "source_ingested") return null;
    const attachments = await this.ingestLinqAttachments({
      references: attachmentReferences.slice(0, 5),
      parentSourceRevisionId: ingested.sourceRevisionId,
      scope,
      occurredAt: event.message.sentAt,
      retentionUntil,
    });
    const text = [
      messageText,
      ...attachments.map((attachment) =>
        attachment.text
          ? `[Attachment: ${attachment.filename}]\n${attachment.text}`
          : `[Attachment: ${attachment.filename}]`,
      ),
    ]
      .filter(Boolean)
      .join("\n\n")
      .trim();
    const household = await resolveHousehold(
      this.database,
      record.routing.conversationId,
      record.routing.senderPersonId,
    );
    return {
      row,
      record,
      event,
      text,
      sourceRevisionId: ingested.sourceRevisionId,
      evidenceSourceRevisionIds: [
        ingested.sourceRevisionId,
        ...attachments.map((attachment) => attachment.sourceRevisionId),
      ],
      images: attachments.flatMap((attachment) => (attachment.image ? [attachment.image] : [])),
      snapshot,
      household,
    };
  }

  private async ingestLinqAttachments(input: {
    references: readonly {
      id: string;
      filename: string | null;
      mediaType: string | null;
      sizeBytes: number | null;
    }[];
    parentSourceRevisionId: string;
    scope: SourceScope;
    occurredAt: string;
    retentionUntil: Date;
  }): Promise<
    {
      sourceRevisionId: string;
      filename: string;
      text: string;
      image: { mimeType: string; dataBase64: string; sha256: string } | null;
    }[]
  > {
    if (!this.attachmentReader) return [];
    const output: {
      sourceRevisionId: string;
      filename: string;
      text: string;
      image: { mimeType: string; dataBase64: string; sha256: string } | null;
    }[] = [];
    let imageBytes = 0;
    for (const reference of input.references) {
      let downloaded: LinqDownloadedAttachment;
      try {
        downloaded = await this.attachmentReader.fetchAttachment(reference.id, {
          maxBytes: 15 * 1024 * 1024,
        });
      } catch (error) {
        if (error instanceof LinqAttachmentError && error.code === "download_failed") throw error;
        continue;
      }
      const bytes = Buffer.from(downloaded.bytes);
      let extracted: Awaited<ReturnType<typeof extractDocument>> | null = null;
      try {
        extracted = await extractDocument(
          bytes,
          downloaded.responseMediaType ?? downloaded.declaredMediaType,
        );
      } catch {
        // The bounded raw artifact is still useful for provenance even when no extractor supports it.
      }
      const capturedAt = new Date();
      const detectedMime =
        extracted?.detectedMime ?? downloaded.responseMediaType ?? downloaded.declaredMediaType;
      const ingested = await this.#sources.apply({
        kind: "ingest_source",
        integrationId: null,
        artifactKind: "attachment_manifest",
        origin: { system: "linq.attachment", remoteObjectId: downloaded.providerAttachmentId },
        scope: input.scope,
        content: JsonObjectSchema.parse({
          parentSourceRevisionId: input.parentSourceRevisionId,
          filename: downloaded.filename || reference.filename || "attachment",
          declaredMime: downloaded.declaredMediaType,
          detectedMime,
          kind: extracted?.kind ?? "unsupported",
          text: extracted?.text ?? "",
          metadata: extracted?.metadata ?? { bytes: downloaded.sizeBytes, unsupportedExtraction: true },
          untrustedEvidence: true,
        }),
        occurredAt: input.occurredAt,
        capturedAt: capturedAt.toISOString(),
        requestedRetentionUntil: input.retentionUntil.toISOString(),
      });
      if (ingested.kind !== "source_ingested") continue;
      await this.#sources.apply({
        kind: "store_blob",
        sourceRevisionId: ingested.sourceRevisionId,
        scope: input.scope,
        blobKind: `linq_attachment:${downloaded.providerAttachmentId}`,
        mimeType: detectedMime,
        bytes: new Uint8Array(bytes),
        storedAt: capturedAt.toISOString(),
      });
      if (extracted) {
        await this.#sources.apply({
          kind: "store_derivative",
          sourceRevisionId: ingested.sourceRevisionId,
          scope: input.scope,
          derivativeKind: `attachment_text:${downloaded.providerAttachmentId}`,
          content: JsonObjectSchema.parse({
            filename: downloaded.filename,
            detectedMime,
            kind: extracted.kind,
            text: extracted.text,
            metadata: extracted.metadata,
            untrustedEvidence: true,
          }),
          requestedRetentionUntil: input.retentionUntil.toISOString(),
          createdAt: capturedAt.toISOString(),
        });
      }
      const canAttachImage =
        detectedMime.startsWith("image/") &&
        bytes.length <= 5 * 1024 * 1024 &&
        imageBytes + bytes.length <= 8 * 1024 * 1024;
      output.push({
        sourceRevisionId: ingested.sourceRevisionId,
        filename: downloaded.filename || reference.filename || "attachment",
        text: extracted?.text ?? "",
        image: canAttachImage
          ? {
              mimeType: detectedMime,
              dataBase64: bytes.toString("base64"),
              sha256: downloaded.sha256,
            }
          : null,
      });
      if (canAttachImage) imageBytes += bytes.length;
    }
    return output;
  }

  private async loadReplyTargetCoverageLoopId(context: MessageContext): Promise<string | null> {
    const providerMessageId = context.event.message.replyTo?.providerMessageId;
    if (!providerMessageId) return null;
    const rows = await this.database<{ readonly coverage_loop_id: string }[]>`
      select effect.coverage_loop_id
      from effect_receipts receipt
      join outbox effect on effect.id = receipt.outbox_id
      where receipt.provider_receipt_id = ${providerMessageId}
        and effect.conversation_id = ${context.record.routing.conversationId}
        and effect.participant_epoch_id = ${context.record.routing.participantEpochId}
        and effect.coverage_loop_id is not null
      order by receipt.occurred_at desc
      limit 1
    `;
    return rows[0]?.coverage_loop_id ?? null;
  }

  private async tryExplicitCoverageResponse(
    context: MessageContext,
    replyTargetLoopId: string | null,
  ): Promise<string | null> {
    const personId = context.record.routing.senderPersonId;
    if (!personId) return null;
    const loops = await this.database<CoverageResponseTargetRow[]>`
      select loop.id, loop.state, loop.proposed_holder_person_id,
        loop.acknowledged_by_person_id, loop.destination_conversation_id,
        loop.participant_epoch_id, loop.participant_set_digest,
        channel.external_channel_id, household.id as household_id,
        household.control_epoch as household_control_epoch,
        household.timezone as household_timezone
      from coverage_loops loop
      join conversations destination on destination.id = loop.destination_conversation_id
      join participant_epochs epoch on epoch.id = destination.current_epoch_id
        and epoch.id = loop.participant_epoch_id and epoch.ended_at is null
        and epoch.participant_set_digest = loop.participant_set_digest
      join households household on household.id = loop.household_id
        and household.status in ('onboarding', 'active', 'paused')
      join conversation_channels channel on channel.conversation_id = destination.id
        and channel.provider = 'linq' and channel.status = 'active'
      where destination.status = 'active'
        and exists (
          select 1 from epoch_participants participant
          where participant.participant_epoch_id = epoch.id and participant.person_id = ${personId}
        )
        and (
          destination.id = ${context.record.routing.conversationId}
          or (
            ${context.record.routing.chatKind === "direct"}
            and destination.kind = 'group'
            and exists (
              select 1 from household_memberships membership
              where membership.household_id = loop.household_id
                and membership.person_id = ${personId} and membership.status = 'active'
            )
          )
        )
        and (
          (loop.proposed_holder_person_id = ${personId} and loop.state in ('awaiting_response', 'at_risk'))
          or (loop.proposed_holder_person_id is null and loop.state in ('open', 'at_risk'))
          or (loop.acknowledged_by_person_id = ${personId} and loop.state = 'covered')
        )
      order by loop.last_transition_at desc limit 8
    `;
    if (loops.length === 0) return null;

    const coordination = new PostgresCoordination(this.database, this.secretBox);
    const candidateLoops = (
      await Promise.all(loops.map(async (row) => ({ row, loop: await coordination.load(row.id) })))
    ).flatMap((candidate) => (candidate.loop ? [{ row: candidate.row, loop: candidate.loop }] : []));
    if (candidateLoops.length === 0) return null;

    const deterministic = deterministicCoverageResponse(context.text, replyTargetLoopId !== null);
    let action: "acknowledge" | "decline" | null = deterministic;
    let target =
      action && replyTargetLoopId
        ? chooseDeterministicCoverageTarget(candidateLoops, personId, action, replyTargetLoopId)
        : null;
    let responseAttemptId: string | null = null;
    if (!target) {
      const interpreted = await this.workers.run({
        attemptId: randomUUID(),
        taskVersionId: randomUUID(),
        skill: PRODUCT_SKILLS.responseInterpret,
        authorizedContext: [
          `Authenticated sender person ID: ${personId}`,
          `Conversation audience: ${context.record.routing.chatKind}`,
          `Exact replied-to coverage loop ID: ${replyTargetLoopId ?? "none"}`,
          `Current response-eligible loops: ${JSON.stringify(
            candidateLoops.map(({ loop }) => ({
              loopId: loop.loopId,
              state: loop.state,
              minimumSharedMeaning: loop.minimumSharedMeaning,
              proposedHolderPersonId: loop.proposedHolderPersonId,
              acknowledgedByPersonId: loop.acknowledgment?.personId ?? null,
              eventAt: loop.timing.eventAt,
              deadlineAt: loop.timing.deadlineAt,
            })),
          )}`,
          `Message: ${context.text}`,
        ].join("\n"),
        goal: "Interpret whether this message explicitly accepts or declines one current coverage loop.",
        deadline: new Date(Date.now() + 30_000),
        budget: { maxModelCalls: 1, maxOutputTokens: 600 },
      });
      responseAttemptId = interpreted.attemptId;
      if (interpreted.status !== "proposed" || !interpreted.proposal) {
        await this.workers.reconcile(interpreted.attemptId, "rejected");
        return null;
      }
      const proposal = interpreted.proposal;
      if (proposal.disposition === "not_response") {
        await this.workers.reconcile(interpreted.attemptId, "accepted");
        return null;
      }
      action =
        proposal.disposition === "acknowledge" || proposal.disposition === "decline"
          ? proposal.disposition
          : null;
      const interpretedAction = action;
      target =
        interpretedAction && proposal.targetLoopId
          ? (candidateLoops.find(
              ({ loop }) =>
                loop.loopId === proposal.targetLoopId &&
                isCoverageResponseEligible(loop, personId, interpretedAction),
            ) ?? null)
          : null;
      const safelyInterpreted =
        target !== null &&
        proposal.explicitSelfStatement &&
        proposal.confidence >= 0.8 &&
        (replyTargetLoopId === null || target.loop.loopId === replyTargetLoopId);
      if (!safelyInterpreted) {
        await this.workers.reconcile(interpreted.attemptId, "rejected");
        await this.queueCoverageResponseClarification(context, candidateLoops, replyTargetLoopId);
        return "coverage_response_ambiguous";
      }
    }

    if (!action || !target) return null;
    if (responseAttemptId) await this.workers.reconcile(responseAttemptId, "accepted");
    const acknowledges = action === "acknowledge";
    const declines = action === "decline";
    const onlyLoop = target.row;
    const responderLabel =
      (await this.loadCurrentParticipantLabels([personId])).find((entry) => entry.personId === personId)
        ?.label ?? "Someone";

    const crossConversation = onlyLoop.destination_conversation_id !== context.record.routing.conversationId;
    let liveDestination: LinqChatSnapshot | null = null;
    if (crossConversation) {
      if (!this.attachmentReader?.getChat || !this.mutationProcessor) {
        return "coverage_destination_reconciliation_unavailable";
      }
      liveDestination = await this.attachmentReader.getChat(onlyLoop.external_channel_id);
      const reconciled = await this.mutationProcessor.process({
        kind: "linq.reconcile_chat",
        liveChat: liveDestination,
      });
      if (
        reconciled.ids.conversationId !== onlyLoop.destination_conversation_id ||
        liveDestination.kind !== "group"
      ) {
        return "coverage_destination_changed";
      }
    }

    return this.database.begin(async (transaction) => {
      const coordination = new PostgresCoordination(transaction, this.secretBox);
      let current = await coordination.loadForUpdate(onlyLoop.id);
      if (!current) return "coverage_disappeared";
      const occurredAt = new Date();
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        onlyLoop.destination_conversation_id,
      );
      if (
        snapshot.participantEpochId !== current.destination.participantEpochId ||
        snapshot.participantSetDigest !== current.destination.participantSetDigest
      ) {
        return "coverage_destination_stale";
      }
      if (
        acknowledges &&
        current.proposedHolderPersonId === null &&
        (current.state === "open" || current.state === "at_risk")
      ) {
        current = (
          await coordination.transition({
            loopId: current.loopId,
            command: {
              kind: "request_coverage",
              transitionId: randomUUID(),
              expectedVersion: current.version,
              actorPersonId: personId,
              requestedPersonId: personId,
              occurredAt: occurredAt.toISOString(),
              evidenceRefs: [...context.evidenceSourceRevisionIds],
            },
          })
        ).loop;
      }
      const recordsRisk =
        declines && current.state === "covered" && current.acknowledgment?.personId === personId;
      const decision = await coordination.transition({
        loopId: current.loopId,
        command: recordsRisk
          ? {
              kind: "record_risk",
              transitionId: randomUUID(),
              expectedVersion: current.version,
              actorPersonId: personId,
              occurredAt: occurredAt.toISOString(),
              proposedHolderPersonId: null,
              evidenceRefs: [...context.evidenceSourceRevisionIds],
            }
          : acknowledges
            ? {
                kind: "acknowledge_coverage",
                transitionId: randomUUID(),
                expectedVersion: current.version,
                actorPersonId: personId,
                acknowledgment: "explicit_self",
                visibility:
                  !crossConversation && context.record.routing.chatKind === "group" ? "shared" : "private",
                occurredAt: occurredAt.toISOString(),
                evidenceRefs: [...context.evidenceSourceRevisionIds],
              }
            : {
                kind: "decline_coverage",
                transitionId: randomUUID(),
                expectedVersion: current.version,
                actorPersonId: personId,
                visibility: "private",
                occurredAt: occurredAt.toISOString(),
                evidenceRefs: [...context.evidenceSourceRevisionIds],
              },
      });
      const destinationContext: MessageContext = crossConversation
        ? {
            ...context,
            record: {
              ...context.record,
              routing: {
                conversationId: onlyLoop.destination_conversation_id,
                participantEpochId: current.destination.participantEpochId,
                appParticipantDigest: current.destination.participantSetDigest,
                providerParticipantDigest:
                  liveDestination?.activeParticipantDigest ??
                  context.record.routing.providerParticipantDigest,
                liveIdentityIds: snapshot.participants.map((participant) => participant.personIdentityId),
                senderIdentityId: null,
                senderPersonId: personId,
                providerChatId: onlyLoop.external_channel_id,
                chatKind: "group",
              },
            },
            snapshot,
            household: {
              id: onlyLoop.household_id,
              controlEpoch: Number(onlyLoop.household_control_epoch),
              timezone: onlyLoop.household_timezone,
            },
          }
        : { ...context, snapshot };
      const proactiveRule = crossConversation
        ? snapshot.rules.find(
            (rule) =>
              rule.active &&
              rule.participantSetDigest === snapshot.participantSetDigest &&
              rule.allowedOperations.includes("proactive_coverage"),
          )
        : null;
      const queued = await queueConversationEffect({
        transaction,
        secretBox: this.secretBox,
        context: destinationContext,
        snapshot,
        text: acknowledges
          ? crossConversation
            ? `Covered — ${sentenceFragment(decision.loop.minimumSharedMeaning)}.`
            : `Covered — ${responderLabel} has ${sentenceFragment(decision.loop.minimumSharedMeaning)}.`
          : `Coverage is still open: ${decision.loop.minimumSharedMeaning}`,
        sendKind: crossConversation ? "proactive" : "direct_response",
        operation: crossConversation
          ? "proactive_coverage"
          : acknowledges
            ? "coverage_closure"
            : "coverage_state_change",
        ruleId: proactiveRule?.ruleId ?? null,
        idempotencyKey: `coverage:${decision.loop.loopId}:v${decision.loop.version}`,
        coverageLoop: { id: decision.loop.loopId, version: decision.loop.version },
      });
      await reconcileCoverageTimers({
        transaction,
        loop: decision.loop,
        snapshot,
        now: occurredAt,
        allowReminder: queued,
      });
      return acknowledges
        ? "coverage_acknowledged"
        : recordsRisk
          ? "coverage_holder_withdrew_privately"
          : "coverage_declined";
    });
  }

  private async queueCoverageResponseClarification(
    context: MessageContext,
    candidates: readonly { readonly loop: CoverageLoop }[],
    replyTargetLoopId: string | null,
  ): Promise<void> {
    const exact = replyTargetLoopId
      ? candidates.find(({ loop }) => loop.loopId === replyTargetLoopId)?.loop
      : null;
    const meanings = (exact ? [exact] : candidates.map(({ loop }) => loop))
      .slice(0, 3)
      .map((loop) => loop.minimumSharedMeaning);
    const text =
      meanings.length === 1
        ? `Just to be sure, what should I record for “${meanings[0]}”?`
        : `Which coverage item do you mean: ${meanings.map((meaning) => `“${meaning}”`).join("; ")}?`;
    await this.database.begin(async (transaction) => {
      const snapshot = await new PostgresConversationAuthority(transaction).snapshot(
        context.record.routing.conversationId,
      );
      await queueConversationEffect({
        transaction,
        secretBox: this.secretBox,
        context,
        snapshot,
        text,
        sendKind: "direct_response",
        operation: "coverage_coordination",
        idempotencyKey: `coverage-response-ambiguous:${context.row.id}`,
      });
    });
  }

  private async loadCurrentCoverageContext(
    context: MessageContext,
  ): Promise<readonly CurrentCoverageContext[]> {
    if (!context.household) return [];
    const rows = await this.database<{ readonly id: string }[]>`
      select loop.id
      from coverage_loops loop
      where loop.household_id = ${context.household.id}
        and loop.destination_conversation_id = ${context.record.routing.conversationId}
        and loop.participant_epoch_id = ${context.record.routing.participantEpochId}
        and loop.participant_set_digest = ${context.record.routing.appParticipantDigest}
        and loop.state in ('provisional', 'open', 'awaiting_response', 'covered', 'at_risk')
      order by loop.last_transition_at desc, loop.id
      limit 12
    `;
    const coordination = new PostgresCoordination(this.database, this.secretBox);
    const current: CurrentCoverageContext[] = [];
    for (const row of rows) {
      const loop = await coordination.load(row.id);
      if (loop) current.push({ loop });
    }
    return current;
  }

  private async resolveProvisionalCoverage(
    context: MessageContext,
    interpretation: (typeof PRODUCT_SKILLS.needInterpret.outputSchema)["_output"],
    currentCoverage: readonly CurrentCoverageContext[],
    replyTargetLoopId: string | null,
  ): Promise<string | null> {
    if (!context.household || !context.record.routing.senderPersonId) return null;
    const senderPersonId = context.record.routing.senderPersonId;
    const provisional = currentCoverage.filter(({ loop }) => loop.state === "provisional");
    if (provisional.length === 0) return null;
    const requestedLoopId = replyTargetLoopId ?? interpretation.priorLoopId;
    let matched = requestedLoopId
      ? provisional.find(({ loop }) => loop.loopId === requestedLoopId)?.loop
      : undefined;
    if (!matched && !requestedLoopId && interpretation.changedFact) {
      if (provisional.length === 1) matched = provisional[0]?.loop;
      else {
        await this.queueCoverageResponseClarification(context, provisional, null);
        return "coverage_fact_ambiguous";
      }
    }
    if (!matched) return null;

    const currentPeople = context.snapshot.participants.map((participant) => participant.personId);
    const participantLabels = await this.loadCurrentParticipantLabels(currentPeople);
    const commitment = await this.workers.run({
      attemptId: randomUUID(),
      taskVersionId: randomUUID(),
      skill: PRODUCT_SKILLS.commitmentPropose,
      authorizedContext: [
        `Current instant: ${new Date().toISOString()}`,
        `Household time zone: ${context.household.timezone}`,
        `Current participant person IDs: ${currentPeople.join(", ")}`,
        `Current participant label-to-person-ID map: ${JSON.stringify(participantLabels)}`,
        `Exact provisional loop: ${JSON.stringify({
          loopId: matched.loopId,
          minimumSharedMeaning: matched.minimumSharedMeaning,
          unresolvedFacts: matched.unresolvedFacts,
          proposedHolderPersonId: matched.proposedHolderPersonId,
          timing: matched.timing,
        })}`,
        `Newly interpreted changed fact: ${interpretation.changedFact ?? "none"}`,
        `New message answering the loop: ${context.text}`,
        `Exact new evidence source revision IDs: ${context.evidenceSourceRevisionIds.join(", ")}`,
        "Revise this exact loop rather than proposing another one. Preserve facts already established. Return every fact that is still unresolved in unresolvedFacts.",
      ].join("\n"),
      ...(context.images.length > 0 ? { images: context.images } : {}),
      goal: "Apply this answer to the exact provisional coverage loop and ask only the next blocking question.",
      deadline: new Date(Date.now() + 45_000),
      budget: { maxModelCalls: 1, maxOutputTokens: 1_200 },
    });
    if (commitment.status !== "proposed" || !commitment.proposal) {
      await this.workers.reconcile(commitment.attemptId, "rejected");
      return "coverage_fact_resolution_failed";
    }
    const proposal = commitment.proposal;
    const proposedPersonId =
      proposal.proposedPersonId && currentPeople.includes(proposal.proposedPersonId)
        ? proposal.proposedPersonId
        : matched.proposedHolderPersonId && currentPeople.includes(matched.proposedHolderPersonId)
          ? matched.proposedHolderPersonId
          : null;
    const unresolvedFacts = [...new Set(proposal.unresolvedFacts)];
    const timing = resolveProposalTiming(proposal, context.household.timezone);
    const proposedLabel =
      participantLabels.find((entry) => entry.personId === proposedPersonId)?.label ?? null;
    const disposition = await this.database.begin(async (transaction) => {
      const latest = await new PostgresConversationAuthority(transaction).snapshot(
        context.record.routing.conversationId,
      );
      if (
        latest.participantEpochId !== context.record.routing.participantEpochId ||
        latest.participantSetDigest !== context.record.routing.appParticipantDigest
      ) {
        return "coverage_fact_resolution_stale";
      }
      const coordination = new PostgresCoordination(transaction, this.secretBox);
      const current = await coordination.loadForUpdate(matched.loopId);
      if (
        current?.state !== "provisional" ||
        current.version !== matched.version ||
        current.destination.conversationId !== context.record.routing.conversationId ||
        current.destination.participantEpochId !== context.record.routing.participantEpochId ||
        current.destination.participantSetDigest !== context.record.routing.appParticipantDigest
      ) {
        return "coverage_fact_resolution_stale";
      }
      let persisted = (
        await coordination.transition({
          loopId: current.loopId,
          command: {
            kind: "resolve_facts",
            transitionId: randomUUID(),
            expectedVersion: current.version,
            actorPersonId: senderPersonId,
            occurredAt: new Date().toISOString(),
            minimumSharedMeaning: proposal.outcome,
            unresolvedFacts,
            proposedHolderPersonId: proposedPersonId,
            timing,
            evidenceRefs: [...context.evidenceSourceRevisionIds],
          },
        })
      ).loop;
      if (persisted.state === "open" && proposedPersonId) {
        persisted = (
          await coordination.transition({
            loopId: persisted.loopId,
            command: {
              kind: "request_coverage",
              transitionId: randomUUID(),
              expectedVersion: persisted.version,
              actorPersonId: senderPersonId,
              requestedPersonId: proposedPersonId,
              occurredAt: new Date().toISOString(),
              evidenceRefs: [...context.evidenceSourceRevisionIds],
            },
          })
        ).loop;
      }
      const text = coveragePrompt(persisted, proposedLabel, proposal.consequentialQuestion);
      const queued = await queueConversationEffect({
        transaction,
        secretBox: this.secretBox,
        context,
        snapshot: latest,
        text,
        sendKind: "direct_response",
        operation: "coverage_coordination",
        idempotencyKey: `coverage:${persisted.loopId}:v${persisted.version}:facts`,
        coverageLoop: { id: persisted.loopId, version: persisted.version },
      });
      await reconcileCoverageTimers({
        transaction,
        loop: persisted,
        snapshot: latest,
        now: new Date(),
        allowReminder: queued,
      });
      return `coverage_${persisted.state}`;
    });
    await this.workers.reconcile(commitment.attemptId, disposition.includes("stale") ? "stale" : "accepted");
    return disposition;
  }

  private async applyChangedFactToCurrentLoop(
    context: MessageContext,
    interpretation: (typeof PRODUCT_SKILLS.needInterpret.outputSchema)["_output"],
    currentCoverage: readonly CurrentCoverageContext[],
  ): Promise<string | null> {
    if (!interpretation.changedFact || !interpretation.priorLoopId || !context.household) return null;
    const matched = currentCoverage.find(({ loop }) => loop.loopId === interpretation.priorLoopId)?.loop;
    if (!matched) return null;
    if (matched.state !== "covered") return `coverage_already_${matched.state}`;

    const assessment = await this.workers.run({
      attemptId: randomUUID(),
      taskVersionId: randomUUID(),
      skill: PRODUCT_SKILLS.outcomeAssess,
      authorizedContext: [
        `Current instant: ${new Date().toISOString()}`,
        `Exact prior coverage loop: ${JSON.stringify({
          loopId: matched.loopId,
          state: matched.state,
          minimumSharedMeaning: matched.minimumSharedMeaning,
          proposedHolderPersonId: matched.proposedHolderPersonId,
          acknowledgedByPersonId: matched.acknowledgment?.personId ?? null,
          eventAt: matched.timing.eventAt,
          deadlineAt: matched.timing.deadlineAt,
        })}`,
        `Changed fact: ${interpretation.changedFact}`,
        `New message: ${context.text}`,
        `Exact new evidence source revision IDs: ${context.evidenceSourceRevisionIds.join(", ")}`,
      ].join("\n"),
      ...(context.images.length > 0 ? { images: context.images } : {}),
      goal: "Assess whether the new admitted evidence invalidates the existing acknowledged coverage.",
      deadline: new Date(Date.now() + 45_000),
      budget: { maxModelCalls: 1, maxOutputTokens: 1_000 },
    });
    if (assessment.status !== "proposed" || !assessment.proposal) {
      await this.workers.reconcile(assessment.attemptId, "rejected");
      return "coverage_change_assessment_failed";
    }
    const reopen =
      assessment.proposal.reopenRecommended &&
      ["contradicted", "corrected", "missed", "expired", "superseded"].includes(
        assessment.proposal.proposedOutcome,
      );
    if (!reopen) {
      await this.workers.reconcile(assessment.attemptId, "accepted");
      return "coverage_change_assessed_no_reopen";
    }

    const explicitAddress = isExplicitQuestion(context) || /\bflorence\b/iu.test(context.text);
    const sendKind =
      context.record.routing.chatKind === "direct" || explicitAddress ? "direct_response" : "proactive";
    const proactiveRule = context.snapshot.rules.find(
      (rule) =>
        rule.active &&
        rule.participantSetDigest === context.snapshot.participantSetDigest &&
        rule.allowedOperations.includes("proactive_coverage"),
    );
    const maySend = sendKind === "direct_response" || proactiveRule !== undefined;
    const disposition = await this.database.begin(async (transaction) => {
      const latest = await new PostgresConversationAuthority(transaction).snapshot(
        context.record.routing.conversationId,
      );
      if (
        latest.participantEpochId !== context.record.routing.participantEpochId ||
        latest.participantSetDigest !== context.record.routing.appParticipantDigest
      ) {
        return "coverage_change_stale_before_commit";
      }
      const coordination = new PostgresCoordination(transaction, this.secretBox);
      const current = await coordination.loadForUpdate(matched.loopId);
      if (
        !current ||
        current.householdId !== context.household?.id ||
        current.destination.conversationId !== context.record.routing.conversationId ||
        current.destination.participantEpochId !== context.record.routing.participantEpochId ||
        current.destination.participantSetDigest !== context.record.routing.appParticipantDigest
      ) {
        return "coverage_change_stale_before_commit";
      }
      if (current.state !== "covered") return `coverage_already_${current.state}`;
      const decision = await coordination.transition({
        loopId: current.loopId,
        command: {
          kind: "record_risk",
          transitionId: randomUUID(),
          expectedVersion: current.version,
          actorPersonId: context.record.routing.senderPersonId,
          occurredAt: new Date().toISOString(),
          proposedHolderPersonId: current.acknowledgment?.personId ?? current.proposedHolderPersonId,
          evidenceRefs: [...context.evidenceSourceRevisionIds],
        },
      });
      const queued = maySend
        ? await queueConversationEffect({
            transaction,
            secretBox: this.secretBox,
            context,
            snapshot: latest,
            text: `Coverage needs reconfirmation: ${decision.loop.minimumSharedMeaning}`,
            sendKind,
            operation: sendKind === "proactive" ? "proactive_coverage" : "coverage_coordination",
            ruleId: sendKind === "proactive" ? (proactiveRule?.ruleId ?? null) : null,
            idempotencyKey: `coverage:${decision.loop.loopId}:v${decision.loop.version}:reopened`,
            coverageLoop: { id: decision.loop.loopId, version: decision.loop.version },
          })
        : false;
      await reconcileCoverageTimers({
        transaction,
        loop: decision.loop,
        snapshot: latest,
        now: new Date(),
        allowReminder: queued,
      });
      return queued ? "coverage_reopened_at_risk" : "coverage_reopened_silently";
    });
    await this.workers.reconcile(assessment.attemptId, disposition.includes("stale") ? "stale" : "accepted");
    return disposition;
  }

  private async proposeCoverage(
    context: MessageContext,
    interpretation: (typeof PRODUCT_SKILLS.needInterpret.outputSchema)["_output"],
  ): Promise<string> {
    const existing = await this.database<{ id: string; state: string }[]>`
      select id, state from coverage_loops
      where source_evidence_refs @> ${this.database.json([context.sourceRevisionId])}
        and state not in ('cancelled', 'superseded', 'dismissed', 'expired_uncovered')
      order by created_at desc limit 1
    `;
    if (existing[0]) return `coverage_already_${existing[0].state}`;
    if (!context.household) {
      const personId = context.record.routing.senderPersonId;
      if (personId) {
        await this.#sources.apply({
          kind: "propose_private_candidate",
          personId,
          candidateKind: "coverage_needs_household",
          content: jsonObject({
            requiredOutcome: interpretation.requiredOutcome,
            uncertainties: interpretation.uncertainties,
          }),
          evidenceSourceRevisionIds: [...context.evidenceSourceRevisionIds],
          confidence: 0.8,
          proposedAt: new Date().toISOString(),
          requestedExpiresAt: new Date(Date.now() + 7 * 86_400_000).toISOString(),
        });
      }
      return "coverage_candidate_needs_household";
    }
    const currentPeople = context.snapshot.participants.map((participant) => participant.personId);
    const participantLabels = await this.loadCurrentParticipantLabels(currentPeople);
    const commitment = await this.workers.run({
      attemptId: randomUUID(),
      taskVersionId: randomUUID(),
      skill: PRODUCT_SKILLS.commitmentPropose,
      authorizedContext: [
        `Current instant: ${new Date().toISOString()}`,
        `Household time zone: ${context.household.timezone}`,
        `Current participant person IDs: ${currentPeople.join(", ")}`,
        `Current participant label-to-person-ID map: ${JSON.stringify(participantLabels)}`,
        `Required outcome: ${interpretation.requiredOutcome ?? "unknown"}`,
        `Time facts: ${interpretation.timeFacts.join("; ")}`,
        `Uncertainties: ${interpretation.uncertainties.join("; ")}`,
        `Message: ${context.text}`,
        `Exact source revision ID: ${context.sourceRevisionId}`,
      ].join("\n"),
      ...(context.images.length > 0 ? { images: context.images } : {}),
      goal: "Propose the minimum next coverage commitment without treating anyone as committed.",
      deadline: new Date(Date.now() + 45_000),
      budget: { maxModelCalls: 1, maxOutputTokens: 1_500 },
    });
    if (commitment.status !== "proposed" || !commitment.proposal) {
      await this.workers.reconcile(commitment.attemptId, "rejected");
      return "commitment_failed";
    }
    const proposal = commitment.proposal;
    const proposedPersonId =
      proposal.proposedPersonId && currentPeople.includes(proposal.proposedPersonId)
        ? proposal.proposedPersonId
        : soleAlternateCoverageCandidate(context.text, currentPeople, context.record.routing.senderPersonId);
    const proposedLabel =
      participantLabels.find((entry) => entry.personId === proposedPersonId)?.label ?? null;
    const explicitAddress = isExplicitQuestion(context) || /\bflorence\b/iu.test(context.text);
    const proactiveRule = context.snapshot.rules.find(
      (rule) =>
        rule.active &&
        rule.participantSetDigest === context.snapshot.participantSetDigest &&
        rule.allowedOperations.includes("proactive_coverage"),
    );
    const sendKind =
      context.record.routing.chatKind === "direct" || explicitAddress ? "direct_response" : "proactive";
    if (sendKind === "proactive" && !proactiveRule) {
      await this.workers.reconcile(commitment.attemptId, "rejected");
      return "coverage_detected_without_group_rule";
    }

    const timing = resolveProposalTiming(proposal, context.household.timezone);
    const unresolved = [...new Set([...interpretation.uncertainties, ...proposal.unresolvedFacts])];
    const loop = createCoverageLoop({
      loopId: randomUUID(),
      householdId: context.household.id,
      minimumSharedMeaning: proposal.outcome,
      unresolvedFacts: unresolved,
      proposedHolderPersonId: proposedPersonId,
      timing,
      planVersion: 1,
      notificationMode: "always",
      destination: {
        conversationId: context.record.routing.conversationId,
        participantEpochId: context.record.routing.participantEpochId,
        participantSetDigest: context.record.routing.appParticipantDigest,
        audience: context.record.routing.chatKind === "group" ? "group" : "private",
      },
      sourceEvidenceRefs: [...context.evidenceSourceRevisionIds],
      occurredAt: new Date().toISOString(),
    });
    const disposition = await this.database.begin(async (transaction) => {
      const conversations = new PostgresConversationAuthority(transaction);
      const latest = await conversations.snapshot(context.record.routing.conversationId);
      if (
        latest.participantEpochId !== context.record.routing.participantEpochId ||
        latest.participantSetDigest !== context.record.routing.appParticipantDigest
      )
        return "coverage_stale_before_commit";
      const coordination = new PostgresCoordination(transaction, this.secretBox);
      let persisted = await coordination.create(loop);
      if (persisted.state === "open" && proposedPersonId) {
        persisted = (
          await coordination.transition({
            loopId: persisted.loopId,
            command: {
              kind: "request_coverage",
              transitionId: randomUUID(),
              expectedVersion: persisted.version,
              actorPersonId: context.record.routing.senderPersonId ?? proposedPersonId,
              requestedPersonId: proposedPersonId,
              occurredAt: new Date().toISOString(),
              evidenceRefs: [...context.evidenceSourceRevisionIds],
            },
          })
        ).loop;
      }
      const message = coveragePrompt(persisted, proposedLabel, proposal.consequentialQuestion);
      const queued = await queueConversationEffect({
        transaction,
        secretBox: this.secretBox,
        context,
        snapshot: latest,
        text: message,
        sendKind,
        operation: sendKind === "proactive" ? "proactive_coverage" : "coverage_coordination",
        ruleId: sendKind === "proactive" ? (proactiveRule?.ruleId ?? null) : null,
        idempotencyKey: `coverage:${persisted.loopId}:v${persisted.version}:open`,
        coverageLoop: { id: persisted.loopId, version: persisted.version },
      });
      await reconcileCoverageTimers({
        transaction,
        loop: persisted,
        snapshot: latest,
        now: new Date(),
        allowReminder: queued,
      });
      return `coverage_${persisted.state}`;
    });
    await this.workers.reconcile(commitment.attemptId, disposition.includes("stale") ? "stale" : "accepted");
    return disposition;
  }

  private async answerGeneralQuestion(context: MessageContext): Promise<string> {
    const answer = await this.workers.run({
      attemptId: randomUUID(),
      taskVersionId: randomUUID(),
      skill: GENERAL_ANSWER_SKILL,
      authorizedContext: `Current instant: ${new Date().toISOString()}\nUser question: ${context.text}`,
      ...(context.images.length > 0 ? { images: context.images } : {}),
      goal: "Answer the explicit question without creating a durable project or using unrelated private context.",
      deadline: new Date(Date.now() + 45_000),
      budget: { maxModelCalls: 1, maxOutputTokens: 1_500 },
    });
    if (answer.status !== "proposed" || !answer.proposal) {
      await this.workers.reconcile(answer.attemptId, "rejected");
      return "general_answer_failed";
    }
    const proposal = answer.proposal;
    await this.database.begin(async (transaction) => {
      const latest = await new PostgresConversationAuthority(transaction).snapshot(
        context.record.routing.conversationId,
      );
      await queueConversationEffect({
        transaction,
        secretBox: this.secretBox,
        context,
        snapshot: latest,
        text: proposal.uncertainty ? `${proposal.answer}\n\n${proposal.uncertainty}` : proposal.answer,
        sendKind: "direct_response",
        operation: "general_answer",
        idempotencyKey: `general-answer:${context.row.id}`,
      });
    });
    await this.workers.reconcile(answer.attemptId, "accepted");
    return "general_answer_queued";
  }
}

async function queueConversationEffect(input: {
  transaction: Transaction;
  secretBox: SecretBox;
  context: MessageContext;
  snapshot: ConversationAuthoritySnapshot;
  text: string;
  sendKind: "direct_response" | "proactive";
  operation: string;
  ruleId?: string | null;
  idempotencyKey: string;
  coverageLoop?: { readonly id: string; readonly version: number };
}): Promise<boolean> {
  const { transaction, context, snapshot } = input;
  if (!snapshot.participantEpochId || !snapshot.participantSetDigest) return false;
  const authority = await new PostgresConversationAuthority(transaction).authorizeSend({
    conversationId: context.record.routing.conversationId,
    expectedParticipantEpochId: snapshot.participantEpochId,
    expectedParticipantSetDigest: snapshot.participantSetDigest,
    liveParticipantIdentityIds: [...context.record.routing.liveIdentityIds],
    sendKind: input.sendKind,
    operation: input.operation,
    ruleId: input.ruleId ?? null,
  });
  if (!authority.allowed) return false;
  const person = context.record.routing.senderPersonId
    ? await transaction<{ control_epoch: number | string }[]>`
        select control_epoch from people where id = ${context.record.routing.senderPersonId}
      `
    : [];
  await new EffectOutbox(transaction, input.secretBox).authorizeAndEnqueue({
    ...(context.record.routing.senderPersonId
      ? { actorPersonId: context.record.routing.senderPersonId }
      : {}),
    effectKind: "linq.message",
    idempotencyKey: input.idempotencyKey,
    ...(input.coverageLoop ? { coverageLoop: input.coverageLoop } : {}),
    data: { text: input.text },
    policy: {
      authorityVersion: snapshot.authorityVersion,
      operation: input.operation,
      sendKind: input.sendKind,
    },
    target: {
      providerChatId: context.record.routing.providerChatId,
      participantEpochId: snapshot.participantEpochId,
    },
    payload: {
      providerChatId: context.record.routing.providerChatId,
      expectedProviderParticipantDigest: context.record.routing.providerParticipantDigest,
      text: input.text,
    },
    reasonCodes: ["current_conversation_authority", input.operation],
    authorizationExpiresAt: new Date(Date.now() + (input.coverageLoop ? 24 * 60 * 60_000 : 5 * 60_000)),
    participantEpochId: snapshot.participantEpochId,
    expectedParticipantDigest: snapshot.participantSetDigest,
    conversation: { id: context.record.routing.conversationId, authorityVersion: snapshot.authorityVersion },
    ...(person[0] && context.record.routing.senderPersonId
      ? {
          person: {
            id: context.record.routing.senderPersonId,
            controlEpoch: Number(person[0].control_epoch),
          },
        }
      : {}),
    ...(context.household
      ? { household: { id: context.household.id, controlEpoch: context.household.controlEpoch } }
      : {}),
  });
  return true;
}

function coveragePrompt(
  loop: CoverageLoop,
  proposedHolderLabel: string | null,
  consequentialQuestion: string | null,
): string {
  if (loop.state === "provisional") {
    const nextQuestion =
      consequentialQuestion ??
      (loop.unresolvedFacts[0]
        ? `What should I use for ${loop.unresolvedFacts[0]}?`
        : "What detail am I missing?");
    return `I have “${loop.minimumSharedMeaning}” open, but I need one detail. ${nextQuestion}`;
  }
  if (loop.state === "awaiting_response") {
    return proposedHolderLabel
      ? `${proposedHolderLabel}, can you take ${sentenceFragment(loop.minimumSharedMeaning)}?`
      : `Can whoever is taking “${loop.minimumSharedMeaning}” confirm it?`;
  }
  return `“${loop.minimumSharedMeaning}” is still uncovered. Who can take it?`;
}

function soleAlternateCoverageCandidate(
  message: string,
  currentPeople: readonly string[],
  senderPersonId: string | null,
): string | null {
  if (!senderPersonId || currentPeople.length !== 2 || !currentPeople.includes(senderPersonId)) return null;
  const normalized = message.toLocaleLowerCase("en-US").replace(/[’]/gu, "'").replace(/\s+/gu, " ");
  if (!/\bi (?:can't|cannot|won't be able to|am not able to|am unavailable)\b/u.test(normalized)) {
    return null;
  }
  return currentPeople.find((personId) => personId !== senderPersonId) ?? null;
}

function sentenceFragment(value: string): string {
  const fragment = value.trim().replace(/[.!?]+$/gu, "");
  if (!fragment) return "this coverage item";
  return fragment;
}

function deterministicCoverageResponse(
  message: string,
  hasExactReplyTarget: boolean,
): "acknowledge" | "decline" | null {
  const normalized = message.toLowerCase().replace(/[’]/gu, "'").replace(/\s+/gu, " ").trim();
  // Conditional and third-party promises are not the sender's own commitment.
  // Leave them to the semantic interpreter instead of mutating loop state.
  if (/\b(?:but|if|maybe|might|probably|unless|except)\b/u.test(normalized)) return null;
  if (/\bi(?:'ll| will| can) (?:ask|have|get)\b[^,.!?]{0,80}\bto\b/u.test(normalized)) {
    return null;
  }
  if (
    /\b(?:i (?:can't|cannot|won't|am not able to|am unavailable)|i'm not available|can't do (?:it|that)|cannot do (?:it|that))\b/u.test(
      normalized,
    )
  ) {
    return "decline";
  }
  if (
    /^(?:yes[, ]+)?(?:i have it|i've got it|i got it|got it|leave it with me)$/u.test(
      normalized.replace(/[.!]$/u, ""),
    ) ||
    /\bi(?:'ll| will| can) (?:cover|handle|take|do|make|pick|get|collect|drop|drive|bring|watch|babysit)\b/u.test(
      normalized,
    )
  ) {
    return "acknowledge";
  }
  if (
    hasExactReplyTarget &&
    /^(?:yes|yep|yeah|sure|ok|okay|absolutely|works for me|i can)[.!]?$/u.test(normalized)
  ) {
    return "acknowledge";
  }
  return null;
}

function chooseDeterministicCoverageTarget<
  Candidate extends { readonly row: CoverageResponseTargetRow; readonly loop: CoverageLoop },
>(
  candidates: readonly Candidate[],
  personId: string,
  action: "acknowledge" | "decline",
  replyTargetLoopId: string | null,
): Candidate | null {
  const eligible = candidates.filter(({ loop }) => isCoverageResponseEligible(loop, personId, action));
  if (replyTargetLoopId) {
    return eligible.find(({ loop }) => loop.loopId === replyTargetLoopId) ?? null;
  }
  return eligible.length === 1 ? (eligible[0] ?? null) : null;
}

function isCoverageResponseEligible(
  loop: CoverageLoop,
  personId: string,
  action: "acknowledge" | "decline",
): boolean {
  if (action === "acknowledge") {
    return (
      (loop.proposedHolderPersonId === personId &&
        (loop.state === "awaiting_response" || loop.state === "at_risk")) ||
      (loop.proposedHolderPersonId === null && (loop.state === "open" || loop.state === "at_risk"))
    );
  }
  return (
    (loop.proposedHolderPersonId === personId && loop.state === "awaiting_response") ||
    (loop.acknowledgment?.personId === personId && loop.state === "covered")
  );
}

async function resolveHousehold(
  database: Database,
  conversationId: string,
  personId: string | null,
): Promise<{ id: string; controlEpoch: number; timezone: string } | null> {
  const rows = await database<{ id: string; control_epoch: number | string; timezone: string }[]>`
    select household.id, household.control_epoch, household.timezone
    from conversations conversation
    join households household on household.id = conversation.household_id
    where conversation.id = ${conversationId} and household.status in ('onboarding', 'active', 'paused')
    union all
    select household.id, household.control_epoch, household.timezone
    from household_memberships membership
    join households household on household.id = membership.household_id
    where ${personId}::uuid is not null and membership.person_id = ${personId}
      and membership.status = 'active' and household.status in ('onboarding', 'active', 'paused')
      and not exists (select 1 from conversations conversation where conversation.id = ${conversationId} and conversation.household_id is not null)
    order by id limit 1
  `;
  const row = rows[0];
  return row ? { id: row.id, controlEpoch: Number(row.control_epoch), timezone: row.timezone } : null;
}

function resolveProposalTiming(
  proposal: (typeof PRODUCT_SKILLS.commitmentPropose.outputSchema)["_output"],
  fallbackTimeZone: string,
) {
  const now = Temporal.Now.instant();
  const timeZone = safeTimeZone(proposal.timeZone, fallbackTimeZone);
  const event = proposal.eventAt ? Temporal.Instant.from(proposal.eventAt) : null;
  const deadline = proposal.deadlineAt ? Temporal.Instant.from(proposal.deadlineAt) : null;
  let lastResponsible = deadline ?? (event ? event.subtract({ minutes: 30 }) : now.add({ hours: 24 }));
  if (Temporal.Instant.compare(lastResponsible, now) <= 0) lastResponsible = now.add({ minutes: 15 });
  const anchor = event ?? deadline ?? lastResponsible;
  return {
    timeZone,
    localDate: anchor.toZonedDateTimeISO(timeZone).toPlainDate().toString(),
    eventAt: event?.toString() ?? null,
    deadlineAt: deadline?.toString() ?? null,
    preparationMinutes: 0,
    travelMinutes: 0,
    earliestUsefulAt: now.toString(),
    lastResponsibleAt: lastResponsible.toString(),
    resolutionPolicy: "wall_clock_compatible" as const,
  };
}

function safeTimeZone(candidate: string, fallback: string): string {
  try {
    Temporal.Now.zonedDateTimeISO(candidate);
    return candidate;
  } catch {
    return fallback;
  }
}

function isExplicitQuestion(context: MessageContext): boolean {
  if (context.record.routing.chatKind === "direct")
    return /\?|^(?:who|what|when|where|why|how|can|could|would|should|is|are|do|does)\b/iu.test(
      context.text.trim(),
    );
  return /\bflorence\b/iu.test(context.text) && /\?/u.test(context.text);
}

function jsonObject(value: unknown) {
  return JsonObjectSchema.parse(JSON.parse(JSON.stringify(value)));
}
