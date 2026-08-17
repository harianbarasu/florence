import { createHash, randomUUID } from "node:crypto";
import type { EncryptedImageVault } from "@florence/artifacts";
import {
  type FamilyMemberInput,
  familyMemberInputSchema,
  familyMemberProfileSchema,
  type PreferencesInput,
  type PutHouseholdInput,
  preferencesInputSchema,
  putHouseholdInputSchema,
  type VaultSource,
  type WorkspaceView,
  workspaceViewSchema,
} from "@florence/contracts";
import type {
  AcceptInboundInput,
  AcceptInboundReactionInput,
  AcceptInboundResult,
  ApprovedCalendarAction,
  BootstrapMessagesGroupInput,
  CalendarActionDraft,
  CalendarOfferApproval,
  CalendarOfferDraft,
  CommitTurnInput,
  FactDraft,
  FactRecord,
  FamilyMemberRecord,
  FollowUpDraft,
  HouseholdRecord,
  InboundTurn,
  JsonObject,
  LinqAuthority,
  MessagesEnrollmentResult,
  PostgresFlorenceStore,
  RedeemMessagesEnrollmentInput,
  SourceRecord,
} from "@florence/database";
import {
  type BeginGoogleConnectionResult,
  GoogleCalendarTransientError,
  type GoogleConnection,
  type GoogleConnectionView,
} from "@florence/google";
import {
  type LinqClient,
  LinqError,
  type LinqMessageStatusProposal,
  type LinqReaction,
  type LinqReactionProposal,
} from "@florence/linq";
import type { EnrollmentCodes } from "./enrollment.js";
import {
  type FlorenceDecision,
  type FlorenceReadTools,
  type FlorenceReasoner,
  FlorenceReasonerError,
  type FlorenceReasonerInput,
  type FlorenceSource,
} from "./reasoner.js";

const DEFAULT_PREFERENCES: PreferencesInput = {
  appearance: "system",
  privateMemoryDefault: "private",
  proactivity: "important_only",
  responseStyle: "concise",
};
const LOOP_IDLE_MS = 250;
const RETRY_MS = 15_000;

export class Florence {
  readonly #store: PostgresFlorenceStore;
  readonly #linq: LinqClient;
  readonly #google: GoogleConnection | null;
  readonly #reasoner: FlorenceReasoner | null;
  readonly #enrollmentCodes: EnrollmentCodes;
  readonly #imageVault: EncryptedImageVault | null;
  readonly #messagesUrl: string | null;
  readonly #forwardingEmail: string | null;
  readonly #now: () => Date;
  #activeRun: Promise<boolean> | null = null;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #started = false;

  constructor(input: {
    store: PostgresFlorenceStore;
    linq: LinqClient;
    google: GoogleConnection | null;
    reasoner: FlorenceReasoner | null;
    enrollmentCodes: EnrollmentCodes;
    imageVault: EncryptedImageVault | null;
    messagesUrl: string | null;
    forwardingEmail: string | null;
    now?: () => Date;
  }) {
    this.#store = input.store;
    this.#linq = input.linq;
    this.#google = input.google;
    this.#reasoner = input.reasoner;
    this.#enrollmentCodes = input.enrollmentCodes;
    this.#imageVault = input.imageVault;
    this.#messagesUrl = nullableText(input.messagesUrl);
    this.#forwardingEmail = nullableText(input.forwardingEmail);
    this.#now = input.now ?? (() => new Date());
  }

  async workspaceForAdult(adultId: string): Promise<WorkspaceView> {
    const household = await this.#householdForAdultOrNull(adultId);
    return workspaceViewSchema.parse(workspace(adultId, household, this.#messagesUrl, this.#forwardingEmail));
  }

  async putHousehold(adultId: string, untrustedInput: PutHouseholdInput): Promise<WorkspaceView> {
    const input = putHouseholdInputSchema.parse(untrustedInput);
    const householdIds = await this.#store.listHouseholdIdsForAdult(adultId);
    const householdId = householdIds[0] ?? deterministicUuid(`household\0${adultId}`);
    if (householdIds.length > 1) throw new Error("The two-adult pilot cannot span multiple households");
    await this.#store.createHousehold({
      householdId,
      name: input.name,
      timeZone: input.timeZone,
      founder: {
        adultId,
        displayName: input.foundingAdultDisplayName,
        profile: { relationship: "Parent" },
      },
      occurredAt: this.#now().toISOString(),
    });
    return this.workspaceForAdult(adultId);
  }

  async putMember(
    adultId: string,
    memberId: string,
    untrustedInput: FamilyMemberInput,
  ): Promise<WorkspaceView> {
    const input = familyMemberInputSchema.parse(untrustedInput);
    const household = await this.#householdForAdult(adultId);
    await this.#store.upsertMember({
      householdId: household.id,
      actorAdultId: adultId,
      memberId,
      member: {
        kind: input.kind,
        role: input.role,
        displayName: input.displayName,
        profile: memberProfile(input),
      },
      occurredAt: this.#now().toISOString(),
    });
    return this.workspaceForAdult(adultId);
  }

  async issueMessagesInvite(
    adultId: string,
    invitedAdultId: string,
  ): Promise<{ invite: { code: string; expiresAt: string }; workspace: WorkspaceView }> {
    const household = await this.#householdForAdult(adultId);
    const commandId = randomUUID();
    const invite = this.#enrollmentCodes.issue({
      commandId,
      householdId: household.id,
      adultId: invitedAdultId,
    });
    const issuedAt = this.#now();
    const expiresAt = new Date(issuedAt.getTime() + 30 * 60_000).toISOString();
    await this.#store.issueMessagesEnrollment({
      householdId: household.id,
      actorAdultId: adultId,
      adultId: invitedAdultId,
      challengeDigest: invite.challengeDigest,
      issuedAt: issuedAt.toISOString(),
      expiresAt,
    });
    return { invite: { code: invite.code, expiresAt }, workspace: await this.workspaceForAdult(adultId) };
  }

  async correctFact(adultId: string, factId: string, statement: string): Promise<WorkspaceView> {
    const household = await this.#householdForAdult(adultId);
    await this.#store.correctFact({ householdId: household.id, adultId, factId, statement });
    return this.workspaceForAdult(adultId);
  }

  async deleteFact(adultId: string, factId: string): Promise<WorkspaceView> {
    const household = await this.#householdForAdult(adultId);
    await this.#store.deleteFact({ householdId: household.id, adultId, factId });
    return this.workspaceForAdult(adultId);
  }

  async deleteDocument(adultId: string, documentId: string): Promise<WorkspaceView> {
    const household = await this.#householdForAdult(adultId);
    await this.#store.deleteDocument({ householdId: household.id, adultId, documentId });
    return this.workspaceForAdult(adultId);
  }

  async savePreferences(adultId: string, untrustedInput: PreferencesInput): Promise<WorkspaceView> {
    const preferences = preferencesInputSchema.parse(untrustedInput);
    const household = await this.#householdForAdult(adultId);
    await this.#store.savePreferences({ householdId: household.id, adultId, preferences });
    return this.workspaceForAdult(adultId);
  }

  async beginGoogle(adultId: string, sessionBindingDigest: string): Promise<BeginGoogleConnectionResult> {
    const google = this.#requiredGoogle();
    const household = await this.#householdForAdult(adultId);
    return google.begin({
      householdId: household.id,
      ownerAdultId: adultId,
      sessionBindingDigest,
      now: this.#now().toISOString(),
    });
  }

  finishGoogle(input: {
    state: string;
    code: string;
    sessionBindingDigest: string;
  }): Promise<GoogleConnectionView> {
    return this.#requiredGoogle().finish({ ...input, now: this.#now().toISOString() });
  }

  async disconnectGoogle(adultId: string, connectionId: string): Promise<WorkspaceView> {
    const household = await this.#householdForAdult(adultId);
    await this.#requiredGoogle().disconnect({
      connectionId,
      householdId: household.id,
      ownerAdultId: adultId,
      now: this.#now().toISOString(),
    });
    return this.workspaceForAdult(adultId);
  }

  resolveLinqAuthority(
    input: Parameters<PostgresFlorenceStore["resolveLinqAuthority"]>[0],
  ): Promise<LinqAuthority | null> {
    return this.#store.resolveLinqAuthority(input);
  }

  redeemMessagesEnrollment(input: RedeemMessagesEnrollmentInput): Promise<MessagesEnrollmentResult | null> {
    return this.#store.redeemMessagesEnrollment(input);
  }

  async bootstrapMessagesGroup(input: BootstrapMessagesGroupInput): Promise<AcceptInboundResult | null> {
    const result = await this.#store.bootstrapMessagesGroup(input);
    if (result?.disposition === "accepted") this.#wake();
    return result;
  }

  async acceptInbound(input: AcceptInboundInput): Promise<AcceptInboundResult | null> {
    const result = await this.#store.acceptInbound(input);
    if (result?.disposition === "accepted") this.#wake();
    return result;
  }

  async acceptInboundReaction(input: AcceptInboundReactionInput): Promise<AcceptInboundResult | null> {
    const result = await this.#store.acceptInboundReaction(input);
    if (result?.disposition === "accepted") this.#wake();
    return result;
  }

  recordLinqObservation(
    input: LinqMessageStatusProposal | LinqReactionProposal,
  ): ReturnType<PostgresFlorenceStore["recordLinqObservation"]> {
    return this.#store.recordLinqObservation(input);
  }

  runOnce(): Promise<boolean> {
    if (this.#activeRun) return this.#activeRun;
    const run = this.#runCycle().finally(() => {
      if (this.#activeRun === run) this.#activeRun = null;
    });
    this.#activeRun = run;
    return run;
  }

  start(): void {
    if (this.#started) return;
    this.#started = true;
    this.#schedule(0);
  }

  stop(): void {
    this.#started = false;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
  }

  async #runCycle(): Promise<boolean> {
    let worked = false;
    const inbound = await this.#store.readNextInbound(this.#now().toISOString());
    if (inbound) {
      await this.#handleInbound(inbound);
      worked = true;
    }
    if (await this.#store.promoteDueFollowUp({ now: this.#now().toISOString() })) worked = true;
    const outbound = await this.#store.readNextOutbound(this.#now().toISOString());
    if (outbound) {
      await this.#deliverOutbound(outbound.sourceId);
      worked = true;
    }
    const calendar = await this.#store.readNextCalendarAction(this.#now().toISOString());
    if (calendar) {
      await this.#executeCalendar(calendar);
      worked = true;
    }
    return worked;
  }

  async #handleInbound(turn: InboundTurn): Promise<void> {
    const observed = await this.#linq.observeChat(turn.authority.providerConversationId);
    if (!sameAuthority(observed, turn.authority)) {
      await this.#store.commitTurn({ sourceId: turn.message.sourceId, handledAt: this.#now().toISOString() });
      return;
    }
    if (!this.#reasoner) {
      await this.#store.retryInbound({
        sourceId: turn.message.sourceId,
        retryAt: later(this.#now(), RETRY_MS),
        error: "Florence reasoning is not configured",
      });
      return;
    }

    const expectedAuthority = {
      audience: turn.authority.audience,
      participantIdentityDigests: turn.authority.expectedParticipantIdentityDigests,
    };
    const typing =
      turn.authority.audience === "private" &&
      (await this.#setTyping({
        providerConversationId: turn.authority.providerConversationId,
        expectedAuthority,
        active: true,
      }));
    try {
      const context = await this.#reasonerContext(turn);
      const decision = await this.#reasoner.decide(context.input, context.reads);
      await this.#store.commitTurn(decisionCommit(turn, decision, this.#now()));
    } catch (error) {
      if (error instanceof FlorenceReasonerError && !error.retryable) {
        await this.#store.commitTurn({
          sourceId: turn.message.sourceId,
          handledAt: this.#now().toISOString(),
        });
        return;
      }
      await this.#store.retryInbound({
        sourceId: turn.message.sourceId,
        retryAt: later(this.#now(), RETRY_MS),
        error: errorText(error),
      });
    } finally {
      if (typing) {
        await this.#setTyping({
          providerConversationId: turn.authority.providerConversationId,
          expectedAuthority,
          active: false,
        });
      }
    }
  }

  async #reasonerContext(turn: InboundTurn): Promise<{
    input: FlorenceReasonerInput;
    reads: FlorenceReadTools;
  }> {
    const members = new Map(turn.household.members.map((member) => [member.id, member.displayName]));
    const visibleSources = memorySources(turn.facts);
    const sourceIndex = new Map(visibleSources.map((source) => [source.sourceId, source]));
    const visibility = turn.authority.audience === "group" ? "shared" : "adult_private";
    const currentDocuments = turn.currentDocuments ?? [];
    const repliedMessage = turn.replyTarget;
    const indexMessage = (message: InboundTurn["message"] | InboundTurn["recentMessages"][number]) => {
      const text = turnText(message);
      sourceIndex.set(message.sourceId, {
        sourceId: message.sourceId,
        recordId: null,
        kind: "message",
        visibility,
        label:
          message.speaker === "florence" ? "Florence" : (members.get(message.speaker) ?? "Family message"),
        occurredAt: message.occurredAt,
        text,
      });
    };
    indexMessage(turn.message);
    if (repliedMessage) indexMessage(repliedMessage);
    for (const message of turn.recentMessages) indexMessage(message);
    for (const document of currentDocuments) {
      sourceIndex.set(document.id, {
        sourceId: document.id,
        recordId: null,
        kind: "document",
        visibility,
        label: document.filename,
        occurredAt: turn.message.occurredAt,
        text: `Attached PDF: ${document.filename}`,
      });
    }

    const googleConnections =
      turn.authority.audience === "private" && this.#google
        ? await this.#google.status({
            householdId: turn.authority.householdId,
            ownerAdultId: turn.authority.senderAdultId,
          })
        : [];
    const input: FlorenceReasonerInput = {
      household: {
        householdId: turn.household.id,
        name: turn.household.name,
        timeZone: turn.household.timeZone,
        adultNames: turn.household.members
          .filter((member) => member.kind === "adult")
          .map((member) => member.displayName),
        familyProfile: JSON.stringify(
          turn.household.members.map((member) => ({
            id: member.id,
            name: member.displayName,
            kind: member.kind,
            role: member.role,
            profile: member.profile,
          })),
        ),
      },
      audience: turn.authority.audience,
      currentAdultId: turn.authority.senderAdultId,
      currentMessage: {
        sourceId: turn.message.sourceId,
        senderName: members.get(turn.message.speaker) ?? "Family member",
        moveKind: turn.message.moveKind,
        text: turnText(turn.message),
        occurredAt: turn.message.occurredAt,
        images: turn.message.images.map(reasonerImage),
        pdfs: currentDocuments.map((document) => ({
          documentId: document.id,
          filename: document.filename,
          mimeType: document.mimeType,
          contentDigest: document.contentDigest,
        })),
        replyTo: repliedMessage
          ? {
              sourceId: repliedMessage.sourceId,
              senderName:
                repliedMessage.speaker === "florence"
                  ? "Florence"
                  : (members.get(repliedMessage.speaker) ?? "Family member"),
              text: turnText(repliedMessage),
              occurredAt: repliedMessage.occurredAt,
            }
          : null,
      },
      recentMessages: turn.recentMessages.slice(-24).map((message) => ({
        sourceId: message.sourceId,
        senderName:
          message.speaker === "florence" ? "Florence" : (members.get(message.speaker) ?? "Family member"),
        text: turnText(message),
        occurredAt: message.occurredAt,
      })),
      visibleSources,
      pendingFollowUps: turn.pendingFollowUps.map((followUp) => ({
        followUpId: followUp.id,
        at: followUp.dueAt,
        text: followUp.text,
        sourceIds: [...followUp.sourceIds],
      })),
      pendingCalendarOffers: turn.pendingCalendarOffers.map((offer) => ({
        proposalId: offer.id,
        connectionId: offer.connectionId,
        event: offer.event,
        sourceIds: [offer.basisSourceId],
      })),
      googleConnections: googleConnections.flatMap((connection) =>
        connection.status === "active" && connection.emailLabel
          ? [{ connectionId: connection.connectionId, emailLabel: connection.emailLabel }]
          : [],
      ),
    };

    const reads: FlorenceReadTools = {
      searchFamilyMemory: async ({ query, limit }) => searchSources(visibleSources, query).slice(0, limit),
      readCalendarWindow: async ({ connectionId, timeMin, timeMax, limit }) => {
        if (
          !this.#google ||
          turn.authority.audience !== "private" ||
          !googleConnections.some(
            (connection) => connection.connectionId === connectionId && connection.status === "active",
          )
        ) {
          return { status: "unavailable", events: [] };
        }
        try {
          return await this.#google.readCalendarWindow({
            householdId: turn.authority.householdId,
            ownerAdultId: turn.authority.senderAdultId,
            connectionId,
            timeMin,
            timeMax,
            limit,
          });
        } catch (error) {
          if (error instanceof GoogleCalendarTransientError) {
            throw new FlorenceReasonerError("transient", "Google Calendar is temporarily unavailable", {
              cause: error,
            });
          }
          throw error;
        }
      },
      readSource: async ({ sourceId }) => sourceIndex.get(sourceId) ?? null,
      readCurrentImage: async ({ assetId, mimeType }) => {
        if (!turn.message.images.some((image) => image.assetId === assetId && image.mimeType === mimeType)) {
          throw new Error("The image is not attached to the current message");
        }
        if (!this.#imageVault) throw new Error("Florence image reading is not configured");
        return this.#imageVault.read({
          householdId: turn.household.id,
          signalId: turn.message.sourceId,
          image: { assetId, mimeType },
        });
      },
      readCurrentPdf: async ({ documentId, filename, mimeType, contentDigest }) => {
        const document = currentDocuments.find(
          (candidate) =>
            candidate.id === documentId &&
            candidate.filename === filename &&
            candidate.mimeType === mimeType &&
            candidate.contentDigest === contentDigest,
        );
        if (!document) throw new Error("The PDF is not attached to the current message");
        if (!this.#imageVault) throw new Error("Florence PDF reading is not configured");
        return this.#imageVault.openPdf({
          documentId: document.id,
          householdId: turn.household.id,
          signalId: turn.message.sourceId,
          filename: document.filename,
          mimeType: document.mimeType,
          contentDigest: document.contentDigest,
          contentEnvelope: document.contentEnvelope,
          discardAfter: document.discardAfter,
          now: this.#now(),
        });
      },
      searchGmail: async ({ connectionId, query, limit }) => {
        if (!this.#google || turn.authority.audience !== "private") return [];
        const evidence = await this.#google.searchGmail({
          householdId: turn.authority.householdId,
          ownerAdultId: turn.authority.senderAdultId,
          connectionId,
          query,
          limit,
        });
        return Promise.all(
          evidence.map(async (message) => {
            const source = await this.#store.recordGmailEvidence({
              householdId: turn.authority.householdId,
              ownerAdultId: turn.authority.senderAdultId,
              connectionId,
              ...message,
            });
            const result: FlorenceSource = {
              sourceId: source.id,
              recordId: null,
              kind: "gmail",
              visibility: "adult_private",
              label: message.subject ?? message.from,
              occurredAt: message.sentAt,
              text: message.text,
            };
            sourceIndex.set(result.sourceId, result);
            return result;
          }),
        );
      },
    };
    return { input, reads };
  }

  async #deliverOutbound(sourceId: string): Promise<void> {
    const outbound = await this.#store.beginOutbound({ sourceId, now: this.#now().toISOString() });
    if (!outbound) return;
    try {
      const result =
        outbound.moveKind === "reaction"
          ? await this.#linq.sendReaction({
              idempotencyKey: outbound.idempotencyKey,
              providerConversationId: outbound.providerConversationId,
              expectedAuthority: outbound.expectedAuthority,
              targetProviderMessageId: requiredText(
                outbound.replyToProviderMessageId,
                "Reaction target message",
              ),
              reaction: reaction(outbound.reaction),
            })
          : await this.#linq.sendMessage({
              idempotencyKey: outbound.idempotencyKey,
              providerConversationId: outbound.providerConversationId,
              expectedAuthority: outbound.expectedAuthority,
              text: requiredText(outbound.text, "Outbound message"),
              ...(outbound.moveKind === "reply"
                ? {
                    replyTo: {
                      providerMessageId: requiredText(
                        outbound.replyToProviderMessageId,
                        "Reply target message",
                      ),
                    },
                  }
                : {}),
            });
      if (result.status === "committed") {
        await this.#store.completeOutbound({
          sourceId,
          providerMessageId: result.providerReceiptId,
          receiptDetail: {
            provider: "linq-v3",
            idempotencyKey: result.idempotencyKey,
            providerState: result.providerState,
          },
          sentAt: result.occurredAt,
        });
      } else {
        await this.#store.retryOutbound({ sourceId, retryAt: null, error: result.detail });
      }
    } catch (error) {
      await this.#store.retryOutbound({
        sourceId,
        retryAt: error instanceof LinqError && error.retryable ? later(this.#now(), 5_000) : null,
        error: errorText(error),
      });
    }
  }

  async #executeCalendar(action: ApprovedCalendarAction): Promise<void> {
    if (!this.#google) {
      await this.#store.retryCalendarAction({
        id: action.id,
        retryAt: later(this.#now(), RETRY_MS),
        error: "Google Calendar is not configured",
      });
      return;
    }
    try {
      const result = await this.#google.executeCalendar(action);
      if (result.status === "failed") {
        await this.#store.failCalendarAction({
          id: action.id,
          error: result.detail,
          failureText: `I couldn’t confirm that “${action.event.title}” was added correctly. Please check Google Calendar before trying again.`,
          failedAt: result.occurredAt,
        });
        return;
      }
      const proof = calendarProof(result.detail, result.providerReceiptId);
      await this.#store.completeCalendarAction({
        id: action.id,
        providerEventId: result.providerReceiptId,
        providerEtag: proof.etag,
        proofDigest: proof.digest,
        proof,
        confirmationText: `Added “${action.event.title}” to your calendar.`,
        committedAt: result.occurredAt,
      });
    } catch (error) {
      await this.#store.retryCalendarAction({
        id: action.id,
        retryAt: later(this.#now(), RETRY_MS),
        error: errorText(error),
      });
    }
  }

  async #householdForAdult(adultId: string): Promise<HouseholdRecord> {
    const household = await this.#householdForAdultOrNull(adultId);
    if (!household) throw new Error("The adult does not belong to a Florence household");
    return household;
  }

  async #householdForAdultOrNull(adultId: string): Promise<HouseholdRecord | null> {
    const ids = await this.#store.listHouseholdIdsForAdult(adultId);
    if (ids.length > 1) throw new Error("The two-adult pilot cannot span multiple households");
    return ids[0] ? await this.#store.readHousehold({ householdId: ids[0], viewerAdultId: adultId }) : null;
  }

  #requiredGoogle(): GoogleConnection {
    if (!this.#google) throw new Error("Google Workspace is not configured");
    return this.#google;
  }

  async #setTyping(input: Parameters<LinqClient["setTyping"]>[0]): Promise<boolean> {
    try {
      return await this.#linq.setTyping(input);
    } catch {
      return false;
    }
  }

  #wake(): void {
    void this.runOnce().catch((error: unknown) => console.error("Florence loop failed", error));
  }

  #schedule(delayMs: number): void {
    if (!this.#started || this.#timer) return;
    this.#timer = setTimeout(() => {
      this.#timer = null;
      void this.runOnce()
        .then((worked) => this.#schedule(worked ? 0 : LOOP_IDLE_MS))
        .catch((error: unknown) => {
          console.error("Florence loop failed", error);
          this.#schedule(RETRY_MS);
        });
    }, delayMs);
  }
}

function workspace(
  adultId: string,
  household: HouseholdRecord | null,
  messagesUrl: string | null,
  forwardingEmail: string | null,
): WorkspaceView {
  const viewer = household?.members.find((member) => member.id === adultId) ?? null;
  const adults = household?.members.filter((member) => member.kind === "adult") ?? [];
  const activeChannels =
    household?.channels.filter((channel) => !channel.revokedAt && !channel.stoppedAt) ?? [];
  const contacts =
    household?.facts.flatMap((fact) => {
      const source = fact.sources[0];
      if ((fact.kind !== "address" && fact.kind !== "phone") || !source) return [];
      return [
        {
          id: fact.id,
          kind: fact.kind,
          label: fact.label,
          value: factStatement(fact),
          visibility: fact.visibility,
          source: vaultSource(source),
          editable: true,
          deletable: true,
        },
      ];
    }) ?? [];
  return {
    viewer: { adultId, displayName: viewer?.displayName ?? null },
    workspace: {
      messagesUrl,
      forwardingEmail,
      googleConnections:
        household?.googleConnections.flatMap((connection) =>
          connection.status === "active" && connection.emailLabel
            ? [
                {
                  connectionId: connection.connectionId,
                  status: "active" as const,
                  emailLabel: connection.emailLabel,
                  lastError: connection.lastError,
                },
              ]
            : [],
        ) ?? [],
      setup: {
        householdCreated: household !== null,
        secondAdultAdded: adults.length === 2,
        bothAdultsMessagesConnected:
          adults.length === 2 && adults.every((adult) => adult.messagesIdentity === "connected"),
        familyGroupConnected: activeChannels.some((channel) => channel.audience === "group"),
      },
    },
    vault: household
      ? {
          name: household.name,
          timeZone: household.timeZone,
          members: household.members.map(memberView),
          contacts,
          facts: household.facts.flatMap((fact) => {
            const source = fact.sources[0];
            if (!source || fact.kind === "address" || fact.kind === "phone") return [];
            return [
              {
                id: fact.id,
                statement: factStatement(fact),
                visibility: fact.visibility,
                source: vaultSource(source),
                recordedAt: source.occurredAt,
                editable: true,
                deletable: true,
              },
            ];
          }),
          documents: household.documents.map((document) => ({
            id: document.id,
            fileName: document.filename,
            mimeType: document.mimeType,
            visibility: document.visibility,
            source: {
              id: document.id,
              kind: "document" as const,
              label: document.filename,
              occurredAt: document.occurredAt,
            },
            retainedAt: document.occurredAt,
            deletable: true,
          })),
        }
      : null,
    preferences: preferences(viewer?.preferences),
  };
}

function memberView(member: FamilyMemberRecord) {
  const candidate = {
    id: member.id,
    kind: member.kind,
    role: member.role,
    displayName: member.displayName,
    relationship: profileString(member.profile, "relationship") ?? defaultRelationship(member),
    ...(profileStrings(member.profile, "aliases")
      ? { aliases: profileStrings(member.profile, "aliases") }
      : {}),
    ...(profileNumber(member.profile, "birthYear")
      ? { birthYear: profileNumber(member.profile, "birthYear") }
      : {}),
    ...(profileString(member.profile, "school") ? { school: profileString(member.profile, "school") } : {}),
    ...(profileString(member.profile, "currentGrade")
      ? { currentGrade: profileString(member.profile, "currentGrade") }
      : {}),
    ...(profileString(member.profile, "academicYear")
      ? { academicYear: profileString(member.profile, "academicYear") }
      : {}),
    ...(profileString(member.profile, "gradeEffectiveFrom")
      ? { gradeEffectiveFrom: profileString(member.profile, "gradeEffectiveFrom") }
      : {}),
    ...(profileStrings(member.profile, "activities")
      ? { activities: profileStrings(member.profile, "activities") }
      : {}),
    status: member.status,
    messagesIdentity: member.kind === "child" ? null : member.messagesIdentity,
  };
  return familyMemberProfileSchema.parse(candidate);
}

function memberProfile(member: FamilyMemberInput): JsonObject {
  return {
    relationship: member.relationship,
    ...(member.aliases ? { aliases: member.aliases } : {}),
    ...(member.birthYear ? { birthYear: member.birthYear } : {}),
    ...(member.school ? { school: member.school } : {}),
    ...(member.currentGrade ? { currentGrade: member.currentGrade } : {}),
    ...(member.academicYear ? { academicYear: member.academicYear } : {}),
    ...(member.gradeEffectiveFrom ? { gradeEffectiveFrom: member.gradeEffectiveFrom } : {}),
    ...(member.activities ? { activities: member.activities } : {}),
  };
}

function memorySources(facts: readonly FactRecord[]): FlorenceSource[] {
  return facts.flatMap((fact) =>
    fact.sources.map((source) => ({
      sourceId: source.id,
      recordId: fact.id,
      kind: "memory" as const,
      visibility: fact.visibility === "household" ? ("shared" as const) : ("adult_private" as const),
      label: fact.label,
      occurredAt: source.occurredAt,
      text: factStatement(fact),
    })),
  );
}

function decisionCommit(turn: InboundTurn, decision: FlorenceDecision, now: Date): CommitTurnInput {
  if (
    turn.message.moveKind === "reaction" &&
    (decision.facts.length > 0 || decision.followUp !== null || decision.calendar !== null)
  ) {
    throw new FlorenceReasonerError(
      "invalid_output",
      "A reaction can express affect but cannot authorize durable or consequential changes",
    );
  }
  const responseTargetSourceId =
    turn.message.moveKind === "reaction" ? turn.message.replyToSourceId : turn.message.sourceId;
  if (!responseTargetSourceId) {
    throw new FlorenceReasonerError("invalid_output", "An inbound reaction has no Florence target");
  }
  const turnId = deterministicUuid(`turn\0${turn.message.sourceId}`);
  const bubbles =
    decision.calendar?.mode === "offer"
      ? [{ text: calendarOfferText(decision.calendar.event), delayMs: 0 }]
      : decision.conversation.bubbles;
  const facts: FactDraft[] = [];
  const deleteFactIds: string[] = [];
  for (const [index, change] of decision.facts.entries()) {
    if (change.operation === "forget") {
      const existing = turn.facts.find((fact) => fact.id === change.factId);
      const canDelete =
        turn.authority.audience === "group"
          ? existing?.visibility === "household"
          : existing?.visibility === "private" && existing.ownerAdultId === turn.authority.senderAdultId;
      if (!canDelete) {
        throw new FlorenceReasonerError(
          "invalid_output",
          "A conversation cannot forget memory outside its write scope",
        );
      }
      deleteFactIds.push(change.factId);
      continue;
    }
    const existing =
      change.operation === "correct" ? turn.facts.find((fact) => fact.id === change.factId) : null;
    const statement = change.statement;
    const visibility = turn.authority.audience === "group" ? "household" : "private";
    const ownerAdultId = visibility === "private" ? turn.authority.senderAdultId : null;
    const sameScope = existing?.visibility === visibility && existing.ownerAdultId === ownerAdultId;
    const slot = existing?.slot ?? `general:${sha256(statement.toLocaleLowerCase())}`;
    facts.push({
      id: sameScope ? existing.id : deterministicUuid(`fact\0${turn.message.sourceId}\0${index}`),
      subjectPersonId: existing?.subjectPersonId ?? null,
      kind: existing?.kind ?? "general",
      slot,
      label: existing?.label ?? statement.slice(0, 160),
      value: { statement },
      visibility,
      ownerAdultId,
      sourceIds: change.sourceIds,
    });
  }
  const followUps: FollowUpDraft[] =
    decision.followUp?.operation === "schedule"
      ? [
          {
            id: deterministicUuid(`follow-up\0${turn.message.sourceId}`),
            dedupeKey: `follow-up:${turn.message.sourceId}`,
            text: decision.followUp.text,
            dueAt: decision.followUp.at,
            sourceIds: decision.followUp.sourceIds,
          },
        ]
      : [];
  const outbound: NonNullable<CommitTurnInput["outbound"]>[number][] = [];
  if (decision.conversation.reaction) {
    outbound.push({
      sourceId: deterministicUuid(`outbound\0${turnId}\0reaction`),
      idempotencyKey: `turn:${turn.message.sourceId}:reaction`,
      moveKind: "reaction",
      reaction: decision.conversation.reaction,
      replyToSourceId: responseTargetSourceId,
      turnId,
      turnPart: -1,
      notBefore: now.toISOString(),
    });
  }
  let delay = 0;
  bubbles.forEach((bubble, index) => {
    delay += bubble.delayMs;
    outbound.push({
      sourceId: deterministicUuid(`outbound\0${turnId}\0${index}`),
      idempotencyKey: `turn:${turn.message.sourceId}:bubble:${index}`,
      moveKind: index === 0 && decision.conversation.replyToCurrentMessage ? "reply" : "message",
      text: bubble.text,
      ...(index === 0 && decision.conversation.replyToCurrentMessage
        ? { replyToSourceId: responseTargetSourceId }
        : {}),
      turnId,
      turnPart: index as 0 | 1 | 2,
      notBefore: new Date(now.getTime() + delay).toISOString(),
    });
  });
  const calendar = calendarCommit(turn, decision);
  return {
    sourceId: turn.message.sourceId,
    facts,
    deleteFactIds,
    followUps,
    cancelFollowUpIds: decision.followUp?.operation === "cancel" ? [decision.followUp.followUpId] : [],
    outbound,
    ...calendar,
    handledAt: now.toISOString(),
  };
}

function calendarOfferText(event: CalendarActionDraft["event"]): string {
  const format = new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: event.timeZone,
  });
  const location = event.location ? `\n${event.location}` : "";
  return `I can add this to your calendar:\n\n${event.title}\n${format.format(new Date(event.startsAt))} – ${format.format(new Date(event.endsAt))}\n${event.timeZone}${location}\n\nWant me to add it?`;
}

function calendarCommit(
  turn: InboundTurn,
  decision: FlorenceDecision,
): Pick<CommitTurnInput, "calendarOffers" | "approveCalendarOffers" | "calendarActions"> {
  if (!decision.calendar) return {};
  if (decision.calendar.mode === "approve") {
    const offer = turn.pendingCalendarOffers.find(
      (candidate) => candidate.id === decision.calendar?.proposalId,
    );
    if (!offer) throw new Error("The Calendar offer is no longer pending");
    const approval: CalendarOfferApproval = {
      offerId: offer.id,
      approvalDigest: sha256(
        JSON.stringify({
          approvalMessageId: turn.message.sourceId,
          proposalDigest: offer.proposalDigest,
          text: turn.message.text,
        }),
      ),
    };
    return { approveCalendarOffers: [approval] };
  }
  const sourceId = decision.calendar.sourceIds[0];
  if (!sourceId) throw new Error("A Calendar decision requires a source");
  const proposalDigest = sha256(
    JSON.stringify({
      connectionId: decision.calendar.connectionId,
      event: decision.calendar.event,
      sourceIds: decision.calendar.sourceIds,
    }),
  );
  const id = deterministicUuid(`calendar\0${turn.message.sourceId}`);
  const actionId = deterministicUuid(`calendar-action\0${turn.message.sourceId}`);
  if (decision.calendar.mode === "offer") {
    const offer: CalendarOfferDraft = {
      id,
      actionId,
      connectionId: decision.calendar.connectionId,
      ownerAdultId: turn.authority.senderAdultId,
      basisSourceId: sourceId,
      proposalDigest,
      event: decision.calendar.event,
    };
    return { calendarOffers: [offer] };
  }
  const action: CalendarActionDraft = {
    id,
    actionId,
    connectionId: decision.calendar.connectionId,
    ownerAdultId: turn.authority.senderAdultId,
    basisSourceId: sourceId,
    approvalMessageId: turn.message.sourceId,
    approvalDigest: sha256(
      JSON.stringify({
        approvalMessageId: turn.message.sourceId,
        proposalDigest,
        text: turn.message.text,
      }),
    ),
    proposalDigest,
    event: decision.calendar.event,
  };
  return { calendarActions: [action] };
}

function preferences(value: JsonObject | undefined): PreferencesInput {
  const parsed = preferencesInputSchema.safeParse({ ...DEFAULT_PREFERENCES, ...(value ?? {}) });
  return parsed.success ? parsed.data : DEFAULT_PREFERENCES;
}

function vaultSource(source: SourceRecord): VaultSource {
  const kind =
    source.kind === "linq_message"
      ? "message"
      : source.kind === "gmail"
        ? "gmail"
        : source.kind === "web"
          ? "web"
          : "document";
  return { id: source.id, kind, label: source.label, occurredAt: source.occurredAt };
}

function factStatement(fact: FactRecord): string {
  const value: unknown = fact.value;
  if (typeof value === "string") return value;
  if (isRecord(value) && typeof value.statement === "string") return value.statement;
  return JSON.stringify(value);
}

function reasonerImage(image: InboundTurn["message"]["images"][number]): {
  assetId: string;
  mimeType: "image/jpeg" | "image/png" | "image/webp";
} {
  if (image.mimeType === "image/heic") {
    throw new Error("Current-message HEIC bytes were not normalized before persistence");
  }
  return { assetId: image.assetId, mimeType: image.mimeType };
}

function turnText(turn: InboundTurn["message"] | InboundTurn["recentMessages"][number]): string {
  if (turn.text?.trim()) return turn.text;
  if (turn.reaction) return `Reacted ${turn.reaction}`;
  return "Shared a family attachment.";
}

function searchSources(sources: readonly FlorenceSource[], query: string): FlorenceSource[] {
  const words = query.toLocaleLowerCase().split(/\s+/).filter(Boolean);
  return sources.filter((source) => {
    const haystack = `${source.label}\n${source.text}`.toLocaleLowerCase();
    return words.every((word) => haystack.includes(word));
  });
}

function calendarProof(detail: string, eventId: string): JsonObject & { etag: string; digest: string } {
  const parsed: unknown = JSON.parse(detail);
  if (
    !isRecord(parsed) ||
    parsed.provider !== "google-calendar" ||
    parsed.eventId !== eventId ||
    typeof parsed.etag !== "string" ||
    typeof parsed.digest !== "string" ||
    !/^[0-9a-f]{64}$/.test(parsed.digest)
  ) {
    throw new Error("Google Calendar returned an invalid proof receipt");
  }
  return parsed as JsonObject & { etag: string; digest: string };
}

function sameAuthority(
  observed: { audience: "private" | "group"; participantIdentityDigests: readonly string[] },
  expected: LinqAuthority,
): boolean {
  return (
    observed.audience === expected.audience &&
    observed.participantIdentityDigests.length === expected.expectedParticipantIdentityDigests.length &&
    observed.participantIdentityDigests.every(
      (digest, index) => digest === expected.expectedParticipantIdentityDigests[index],
    )
  );
}

function reaction(value: string | null): LinqReaction {
  if (
    value === "love" ||
    value === "like" ||
    value === "dislike" ||
    value === "laugh" ||
    value === "emphasize" ||
    value === "question"
  ) {
    return value;
  }
  throw new Error("Outbound reaction is invalid");
}

function defaultRelationship(member: FamilyMemberRecord): string {
  if (member.kind === "child") return "Child";
  return member.role === "steward" ? "Parent" : "Caregiver";
}

function profileString(profile: JsonObject, key: string): string | null {
  const value = profile[key];
  return typeof value === "string" && value.trim() ? value : null;
}

function profileStrings(profile: JsonObject, key: string): string[] | null {
  const value = profile[key];
  return Array.isArray(value) && value.every((item) => typeof item === "string") ? value : null;
}

function profileNumber(profile: JsonObject, key: string): number | null {
  const value = profile[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function nullableText(value: string | null): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function requiredText(value: string | null, label: string): string {
  if (!value?.trim()) throw new Error(`${label} is required`);
  return value;
}

function later(now: Date, milliseconds: number): string {
  return new Date(now.getTime() + milliseconds).toISOString();
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function deterministicUuid(value: string): string {
  const digest = sha256(value);
  const variant = ((Number.parseInt(digest[16] ?? "0", 16) & 3) | 8).toString(16);
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-5${digest.slice(13, 16)}-${variant}${digest.slice(17, 20)}-${digest.slice(20, 32)}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : "Unknown Florence loop failure";
}
