import { createHash } from "node:crypto";
import postgres from "postgres";

export type JsonValue = postgres.JSONValue;
export type JsonObject = Readonly<Record<string, JsonValue>>;
export type Visibility = "private" | "household";
export type Audience = "private" | "group";
export type ImageReference = {
  assetId: string;
  mimeType: "image/jpeg" | "image/png" | "image/webp" | "image/heic";
};
const MAX_CURRENT_PDFS = 3;
const MAX_PDF_ENVELOPE_BYTES = 20 * 1024 * 1024 + 16 * 1024;
const CALENDAR_CLAIM_MS = 2 * 60_000;
export type GoogleScope =
  | "openid"
  | "email"
  | "https://www.googleapis.com/auth/gmail.readonly"
  | "https://www.googleapis.com/auth/calendar.events.owned";

export class FlorenceStoreConflict extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FlorenceStoreConflict";
  }
}

export class FlorenceStoreUnauthorized extends Error {
  constructor(message = "This adult cannot change that household data") {
    super(message);
    this.name = "FlorenceStoreUnauthorized";
  }
}

export type PostgresFlorenceStoreOptions = {
  connectionString: string;
  maxConnections?: number;
};

export type FamilyMemberRecord = {
  id: string;
  householdId: string;
  kind: "adult" | "child";
  role: "steward" | "caregiver" | "dependent";
  adultSlot: 1 | 2 | null;
  displayName: string;
  status: "planned" | "verified" | "represented";
  messagesIdentity: "not_invited" | "invited" | "connected" | null;
  profile: JsonObject;
  preferences: JsonObject;
};

export type LinqChannelRecord = {
  id: string;
  householdId: string;
  audience: Audience;
  providerConversationId: string;
  adultIds: readonly string[];
  participantIdentityDigests: readonly string[];
  authorityDigest: string;
  boundAt: string;
  revokedAt: string | null;
  stoppedAt: string | null;
};

export type SourceRecord = {
  id: string;
  kind: "linq_message" | "gmail" | "google_file" | "document" | "web";
  visibility: Visibility;
  ownerAdultId: string | null;
  label: string;
  metadata: JsonObject;
  occurredAt: string;
};

export type FactRecord = {
  id: string;
  householdId: string;
  subjectPersonId: string | null;
  kind:
    | "identity"
    | "school"
    | "caregiver"
    | "activity"
    | "schedule"
    | "address"
    | "phone"
    | "contact"
    | "preference"
    | "safety"
    | "general";
  slot: string;
  label: string;
  value: JsonValue;
  visibility: Visibility;
  ownerAdultId: string | null;
  sources: readonly SourceRecord[];
  correctedAt: string | null;
  updatedAt: string;
};

export type CurrentMessageDocument = {
  id: string;
  parentSourceId: string;
  filename: string;
  mimeType: "application/pdf";
  contentDigest: string;
  contentEnvelope: Uint8Array;
  discardAfter: string;
};

export type GoogleConnectionStatus = "pending" | "active" | "disconnected";
export type GoogleConnectionView = {
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  status: GoogleConnectionStatus;
  emailLabel: string | null;
  grantedScopes: readonly GoogleScope[];
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
};

export type HouseholdRecord = {
  id: string;
  name: string;
  timeZone: string;
  members: readonly FamilyMemberRecord[];
  channels: readonly LinqChannelRecord[];
  facts: readonly FactRecord[];
  googleConnections: readonly GoogleConnectionView[];
};

export type CreateHouseholdInput = {
  householdId: string;
  name: string;
  timeZone: string;
  founder: { adultId: string; displayName: string; profile?: JsonObject };
  occurredAt: string;
};

export type UpsertMemberInput = {
  householdId: string;
  actorAdultId: string;
  memberId: string;
  member: {
    kind: "adult" | "child";
    role: "steward" | "caregiver" | "dependent";
    displayName: string;
    profile?: JsonObject;
  };
  occurredAt: string;
};

export type IssueMessagesEnrollmentInput = {
  householdId: string;
  actorAdultId: string;
  adultId: string;
  challengeDigest: string;
  expiresAt: string;
  issuedAt: string;
};

export type RedeemMessagesEnrollmentInput = {
  challengeDigest: string;
  identitySubjectDigest: string;
  consentVersion: string;
  consentedAt: string;
  providerConversationId: string;
  occurredAt: string;
};

export type MessagesEnrollmentResult = {
  disposition: "accepted" | "duplicate";
  householdId: string;
  adultId: string;
  channel: LinqChannelRecord;
};

export type InboundDocumentInput = {
  documentId: string;
  externalKey: string;
  filename: string;
  mimeType: string;
  contentDigest: string;
  contentEnvelope: Uint8Array;
  discardAfter?: string;
};

export type AcceptInboundInput = {
  providerConversationId: string;
  audience: Audience;
  participantIdentityDigests: readonly string[];
  senderIdentitySubjectDigest: string;
  providerEventId: string;
  providerMessageId: string;
  replyToProviderMessageId?: string | null;
  text: string | null;
  images?: readonly ImageReference[];
  documents?: readonly InboundDocumentInput[];
  supersedesSourceId?: string | null;
  discardSupersededFacts?: boolean;
  occurredAt: string;
};

export type AcceptInboundReactionInput = {
  providerConversationId: string;
  audience: Audience;
  participantIdentityDigests: readonly string[];
  senderIdentitySubjectDigest: string;
  providerEventId: string;
  targetProviderMessageId: string;
  reaction: string;
  partIndex: number;
  occurredAt: string;
};

export type AcceptInboundResult = {
  disposition: "accepted" | "duplicate" | "stopped";
  sourceId: string;
  householdId: string;
  channelId: string;
};

export type BootstrapMessagesGroupInput = AcceptInboundInput & {
  audience: "group";
};

export type LinqAuthority = {
  householdId: string;
  channelId: string;
  audience: Audience;
  providerConversationId: string;
  senderAdultId: string;
  adultIds: readonly string[];
  expectedParticipantIdentityDigests: readonly string[];
  authorityDigest: string;
  replyToSourceId: string | null;
  stopped: boolean;
};

export type ConversationTurn = {
  sourceId: string;
  speaker: "florence" | string;
  moveKind: "message" | "reply" | "reaction";
  text: string | null;
  reaction: string | null;
  images: readonly ImageReference[];
  replyToSourceId: string | null;
  occurredAt: string;
};

export type PendingFollowUp = {
  id: string;
  text: string;
  dueAt: string;
  sourceIds: readonly string[];
};

export type InboundTurn = {
  message: ConversationTurn & { speaker: string };
  supersededMessages: readonly (ConversationTurn & { speaker: string })[];
  replyTarget: ConversationTurn | null;
  authority: LinqAuthority;
  household: { id: string; name: string; timeZone: string; members: readonly FamilyMemberRecord[] };
  facts: readonly FactRecord[];
  currentDocuments?: readonly CurrentMessageDocument[];
  recentMessages: readonly ConversationTurn[];
  pendingFollowUps: readonly PendingFollowUp[];
  pendingCalendarOffers: readonly CalendarOffer[];
};

export type FactDraft = Omit<FactRecord, "householdId" | "sources" | "correctedAt" | "updatedAt"> & {
  sourceIds: readonly string[];
};

export type FollowUpDraft = {
  id: string;
  dedupeKey: string;
  text: string;
  dueAt: string;
  sourceIds: readonly string[];
};

export type OutboundDraft = {
  sourceId: string;
  idempotencyKey: string;
  moveKind: "message" | "reply" | "reaction";
  text?: string | null;
  reaction?: string | null;
  replyToSourceId?: string | null;
  turnId: string;
  turnPart: -1 | 0 | 1 | 2;
  notBefore: string;
};

export type CalendarEventDraft = {
  title: string;
  startsAt: string;
  endsAt: string;
  timeZone: string;
  location: string | null;
};

export type CalendarActionDraft = {
  id: string;
  actionId: string;
  connectionId: string;
  ownerAdultId: string;
  basisSourceId: string | null;
  approvalMessageId: string;
  approvalDigest: string;
  proposalDigest: string;
  operation?: "create" | "update";
  event: CalendarEventDraft;
};

export type CalendarOfferDraft = Omit<CalendarActionDraft, "approvalMessageId" | "approvalDigest"> & {
  basisSourceId: string;
};

export type CalendarOfferApproval = { offerId: string; approvalDigest: string };

export type CalendarOffer = {
  id: string;
  actionId: string;
  connectionId: string;
  ownerAdultId: string;
  basisSourceId: string;
  proposalDigest: string;
  event: CalendarEventDraft;
};

export type CommitTurnInput = {
  sourceId: string;
  facts?: readonly FactDraft[];
  deleteFactIds?: readonly string[];
  followUps?: readonly FollowUpDraft[];
  cancelFollowUpIds?: readonly string[];
  outbound?: readonly OutboundDraft[];
  calendarOffers?: readonly CalendarOfferDraft[];
  approveCalendarOffers?: readonly CalendarOfferApproval[];
  calendarActions?: readonly CalendarActionDraft[];
  handledAt: string;
};

export type CommitTurnResult = "committed" | "superseded";

export type OutboundMessage = {
  sourceId: string;
  idempotencyKey: string;
  providerConversationId: string;
  expectedAuthority: { audience: Audience; participantIdentityDigests: readonly string[] };
  moveKind: "message" | "reply" | "reaction";
  text: string | null;
  reaction: string | null;
  replyToProviderMessageId: string | null;
};

export type LinqOutboundObservation =
  | {
      kind: "message_status";
      providerEventId: string;
      providerConversationId: string;
      providerMessageId: string;
      idempotencyKey: string | null;
      status: "sent" | "delivered" | "read" | "failed";
      occurredAt: string;
      traceId: string;
      failure: { code: number; reason: string | null } | null;
    }
  | {
      kind: "reaction";
      providerEventId: string;
      providerConversationId: string;
      targetProviderMessageId: string;
      operation: "added" | "removed";
      reaction: string;
      partIndex: number;
      isFromMe: boolean;
      occurredAt: string;
      traceId: string;
    };

export type LinqObservationResult = "applied" | "duplicate" | "unmatched";

export type ApprovedCalendarAction = {
  id: string;
  actionId: string;
  householdId: string;
  connectionId: string;
  ownerAdultId: string;
  approvalMessageId: string;
  approvalDigest: string;
  proposalDigest: string;
  event: CalendarEventDraft;
};

export type PendingGoogleConnection = {
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  stateDigest: string;
  sessionBindingDigest: string;
};

export type ActiveGoogleCredential = {
  connectionId: string;
  householdId: string;
  ownerAdultId: string;
  refreshTokenEnvelope: string;
};

type PersonRow = {
  id: string;
  household_id: string;
  kind: "adult" | "child";
  role: "steward" | "caregiver" | "dependent";
  adult_slot: 1 | 2 | null;
  display_name: string;
  status: "planned" | "verified" | "represented";
  identity_subject_digest: string | null;
  invitation_expires_at: Date | null;
  invitation_consumed_at: Date | null;
  profile: JsonObject;
  preferences: JsonObject;
};

type ChannelRow = {
  id: string;
  household_id: string;
  audience: Audience;
  provider_conversation_id: string;
  adult_one_id: string;
  identity_one_digest: string;
  adult_two_id: string | null;
  identity_two_digest: string | null;
  authority_digest: string;
  bound_at: Date;
  revoked_at: Date | null;
  stopped_at: Date | null;
};

type SourceRow = {
  id: string;
  kind: SourceRecord["kind"];
  visibility: Visibility;
  owner_adult_id: string | null;
  label: string;
  metadata: JsonObject;
  occurred_at: Date;
};

type GoogleConnectionRow = {
  id: string;
  household_id: string;
  owner_adult_id: string;
  status: GoogleConnectionStatus;
  state_digest: string;
  session_binding_digest: string | null;
  state_expires_at: Date;
  state_consumed_at: Date | null;
  google_subject_digest: string | null;
  email_label: string | null;
  granted_scopes: GoogleScope[];
  refresh_token_envelope: string | null;
  last_error: string | null;
  created_at: Date;
  updated_at: Date;
};

type LinqObservationRow = {
  source_id: string;
  status: "pending" | "sending" | "sent" | "failed";
  move_kind: "message" | "reply" | "reaction";
  provider_message_id: string | null;
  sent_at: Date | null;
  receipt_detail: JsonValue | null;
};

export class PostgresFlorenceStore {
  readonly #sql: ReturnType<typeof postgres>;
  #closed = false;

  constructor(options: string | PostgresFlorenceStoreOptions) {
    const { connectionString, maxConnections = 10 } =
      typeof options === "string" ? { connectionString: options, maxConnections: 10 } : options;
    this.#sql = postgres(connectionString, { max: maxConnections });
  }

  async ready(): Promise<void> {
    const [row] = await this.#sql<{ ready: boolean }[]>`
      select to_regclass('public.households') is not null
        and to_regclass('public.messages') is not null
        and to_regclass('public.calendar_actions') is not null as ready
    `;
    if (!row?.ready) throw new Error("The direct Florence database baseline is not installed");
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    await this.#sql.end({ timeout: 5 });
  }

  async listHouseholdIdsForAdult(adultId: string): Promise<readonly string[]> {
    const rows = await this.#sql<{ household_id: string }[]>`
      select household_id from people
      where id=${adultId} and kind='adult' and status='verified'
      order by household_id
    `;
    return rows.map((row) => row.household_id);
  }

  async readHousehold(input: {
    householdId: string;
    viewerAdultId?: string;
  }): Promise<HouseholdRecord | null> {
    if (input.viewerAdultId) {
      const [viewer] = await this.#sql<{ id: string }[]>`
        select id from people where household_id=${input.householdId}
          and id=${input.viewerAdultId} and kind='adult' and status='verified'
      `;
      if (!viewer) return null;
    }
    const [household] = await this.#sql<{ id: string; name: string; time_zone: string }[]>`
      select id,name,time_zone from households where id=${input.householdId}
    `;
    if (!household) return null;

    const members = await this.#sql<PersonRow[]>`
      select id,household_id,kind,role,adult_slot,display_name,status,
             identity_subject_digest,invitation_expires_at,invitation_consumed_at,
             profile,preferences
      from people where household_id=${input.householdId}
      order by adult_slot nulls last,created_at,id
    `;
    const channels = await this.#sql<ChannelRow[]>`
      select * from linq_channels where household_id=${input.householdId}
      order by bound_at,id
    `;
    const facts = await this.#readFacts(input.householdId, input.viewerAdultId ?? null);
    const googleRows = input.viewerAdultId
      ? await this.#sql<GoogleConnectionRow[]>`
          select * from google_connections where household_id=${input.householdId}
            and owner_adult_id=${input.viewerAdultId} order by created_at,id
        `
      : [];
    const memberRecords = members.map(personRecord);
    const channelRecords = channels.map(channelRecord);
    return {
      id: household.id,
      name: household.name,
      timeZone: household.time_zone,
      members: memberRecords,
      channels: channelRecords,
      facts,
      googleConnections: googleRows.map(googleConnectionView),
    };
  }

  async createHousehold(input: CreateHouseholdInput): Promise<HouseholdRecord> {
    const occurredAt = instant(input.occurredAt);
    await this.#sql.begin(async (sql) => {
      const [existing] = await sql<{ id: string }[]>`select id from households limit 1 for update`;
      if (existing) {
        if (existing.id !== input.householdId)
          throw new FlorenceStoreConflict("The pilot household already exists");
        const [founder] = await sql<{ id: string }[]>`
          select id from people where household_id=${input.householdId}
            and id=${input.founder.adultId} and kind='adult' and status='verified'
        `;
        if (!founder) throw new FlorenceStoreUnauthorized();
        await sql`
          update households set name=${required(input.name, "Household name")},
            time_zone=${required(input.timeZone, "Household time zone")},updated_at=${occurredAt}
          where id=${input.householdId}
        `;
        await sql`
          update people set display_name=${required(input.founder.displayName, "Founder display name")},
            updated_at=${occurredAt}
          where id=${input.founder.adultId}
        `;
        return;
      }
      await sql`
        insert into households (id,name,time_zone,created_at,updated_at)
        values (${input.householdId},${required(input.name, "Household name")},
          ${required(input.timeZone, "Household time zone")},${occurredAt},${occurredAt})
      `;
      await sql`
        insert into people (id,household_id,kind,role,adult_slot,display_name,status,profile,created_at,updated_at)
        values (${input.founder.adultId},${input.householdId},'adult','steward',1,
          ${required(input.founder.displayName, "Founder display name")},'verified',
          ${sql.json(input.founder.profile ?? {})},${occurredAt},${occurredAt})
      `;
    });
    return requiredHousehold(
      await this.readHousehold({ householdId: input.householdId, viewerAdultId: input.founder.adultId }),
    );
  }

  async upsertMember(input: UpsertMemberInput): Promise<FamilyMemberRecord> {
    const occurredAt = instant(input.occurredAt);
    const profile = input.member.profile ?? {};
    const row = await this.#sql.begin(async (sql) => {
      await requireSteward(sql, input.householdId, input.actorAdultId);
      const [existing] = await sql<PersonRow[]>`
        select * from people where household_id=${input.householdId} and id=${input.memberId} for update
      `;
      if (existing && existing.kind !== input.member.kind) {
        throw new FlorenceStoreConflict("A family member cannot change between adult and child");
      }
      if (input.member.kind === "adult" && input.member.role === "dependent") {
        throw new FlorenceStoreConflict("An adult cannot have the dependent role");
      }
      if (input.member.kind === "child" && input.member.role !== "dependent") {
        throw new FlorenceStoreConflict("A child must have the dependent role");
      }
      if (existing) {
        const [updated] = await sql<PersonRow[]>`
          update people set display_name=${required(input.member.displayName, "Member display name")},
            role=${existing.status === "verified" ? existing.role : input.member.role},
            profile=${sql.json(input.member.profile ?? existing.profile)},updated_at=${occurredAt}
          where id=${existing.id} returning *
        `;
        return updated;
      }
      const adultSlot = input.member.kind === "adult" ? 2 : null;
      const status = input.member.kind === "adult" ? "planned" : "represented";
      const [inserted] = await sql<PersonRow[]>`
        insert into people (id,household_id,kind,role,adult_slot,display_name,status,profile,created_at,updated_at)
        values (${input.memberId},${input.householdId},${input.member.kind},${input.member.role},${adultSlot},
          ${required(input.member.displayName, "Member display name")},${status},${sql.json(profile)},${occurredAt},${occurredAt})
        returning *
      `;
      return inserted;
    });
    if (!row) throw new Error("The family member was not saved");
    return personRecord(row);
  }

  async savePreferences(input: {
    householdId: string;
    adultId: string;
    preferences: JsonObject;
  }): Promise<HouseholdRecord> {
    const [updated] = await this.#sql`
      update people set preferences=${this.#sql.json(input.preferences)},updated_at=now()
      where household_id=${input.householdId} and id=${input.adultId}
        and kind='adult' and status='verified' returning id
    `;
    if (!updated) throw new FlorenceStoreUnauthorized();
    return requiredHousehold(
      await this.readHousehold({ householdId: input.householdId, viewerAdultId: input.adultId }),
    );
  }

  async correctFact(input: {
    householdId: string;
    adultId: string;
    factId: string;
    statement: string;
  }): Promise<HouseholdRecord> {
    await this.#requireVerifiedAdult(input.householdId, input.adultId);
    const [updated] = await this.#sql`
      update facts set value=${this.#sql.json({ statement: required(input.statement, "Fact statement") })},
        corrected_at=now(),updated_at=now()
      where id=${input.factId} and household_id=${input.householdId}
        and (visibility='household' or owner_adult_id=${input.adultId}) returning id
    `;
    if (!updated) throw new FlorenceStoreUnauthorized("That fact is not visible to this adult");
    return requiredHousehold(
      await this.readHousehold({ householdId: input.householdId, viewerAdultId: input.adultId }),
    );
  }

  async deleteFact(input: {
    householdId: string;
    adultId: string;
    factId: string;
  }): Promise<HouseholdRecord> {
    await this.#requireVerifiedAdult(input.householdId, input.adultId);
    const deleted = await this.#sql`
      delete from facts where id=${input.factId} and household_id=${input.householdId}
        and (visibility='household' or owner_adult_id=${input.adultId}) returning id
    `;
    if (deleted.length === 0) throw new FlorenceStoreUnauthorized("That fact is not visible to this adult");
    return requiredHousehold(
      await this.readHousehold({ householdId: input.householdId, viewerAdultId: input.adultId }),
    );
  }

  async issueMessagesEnrollment(input: IssueMessagesEnrollmentInput): Promise<FamilyMemberRecord> {
    assertDigest(input.challengeDigest, "Messages enrollment challenge");
    const issuedAt = instant(input.issuedAt);
    const expiresAt = instant(input.expiresAt);
    if (expiresAt <= issuedAt)
      throw new FlorenceStoreConflict("A Messages invitation must expire after issue");
    const adult = await this.#sql.begin(async (sql) => {
      await requireSteward(sql, input.householdId, input.actorAdultId);
      const [row] = await sql<PersonRow[]>`
        update people set invitation_digest=${input.challengeDigest},invitation_expires_at=${expiresAt},
          invitation_consumed_at=null,updated_at=${issuedAt}
        where household_id=${input.householdId} and id=${input.adultId}
          and kind='adult' and status in ('planned','verified') and identity_subject_digest is null
        returning *
      `;
      return row;
    });
    if (!adult) throw new FlorenceStoreConflict("The invited adult is not waiting for Messages setup");
    return personRecord(adult);
  }

  async redeemMessagesEnrollment(
    input: RedeemMessagesEnrollmentInput,
  ): Promise<MessagesEnrollmentResult | null> {
    assertDigest(input.challengeDigest, "Messages enrollment challenge");
    assertDigest(input.identitySubjectDigest, "Messages identity");
    const occurredAt = instant(input.occurredAt);
    const consentedAt = instant(input.consentedAt);
    return this.#sql.begin(async (sql) => {
      const [adult] = await sql<PersonRow[]>`
        select * from people where invitation_digest=${input.challengeDigest} for update
      `;
      if (!adult) return null;
      const [existingChannel] = await sql<ChannelRow[]>`
        select * from linq_channels where household_id=${adult.household_id}
          and audience='private' and adult_one_id=${adult.id} and revoked_at is null
        limit 1
      `;
      if (
        adult.identity_subject_digest !== null ||
        (adult.status === "verified" && adult.invitation_consumed_at)
      ) {
        if (
          adult.identity_subject_digest === input.identitySubjectDigest &&
          existingChannel?.provider_conversation_id === input.providerConversationId
        ) {
          return {
            disposition: "duplicate" as const,
            householdId: adult.household_id,
            adultId: adult.id,
            channel: channelRecord(existingChannel),
          };
        }
        return null;
      }
      const invitation = await sql<{ invitation_expires_at: Date | null }[]>`
        select invitation_expires_at from people where id=${adult.id}
      `;
      if (!invitation[0]?.invitation_expires_at || invitation[0].invitation_expires_at < occurredAt)
        return null;
      const [identityOwner] = await sql<{ id: string }[]>`
        select id from people where identity_subject_digest=${input.identitySubjectDigest} and id<>${adult.id}
      `;
      if (identityOwner) return null;
      await sql`
        update people set status='verified',identity_subject_digest=${input.identitySubjectDigest},
          consent_version=${required(input.consentVersion, "Messages consent version")},consented_at=${consentedAt},
          invitation_consumed_at=${occurredAt},updated_at=${occurredAt}
        where id=${adult.id}
      `;
      const channelId = deterministicUuid(`linq-private\0${input.providerConversationId}`);
      const authorityDigest = digestStrings([adult.id, input.identitySubjectDigest]);
      const [channel] = await sql<ChannelRow[]>`
        insert into linq_channels (
          id,household_id,audience,provider_conversation_id,adult_one_id,identity_one_digest,
          authority_digest,bound_at
        ) values (${channelId},${adult.household_id},'private',${input.providerConversationId},
          ${adult.id},${input.identitySubjectDigest},${authorityDigest},${occurredAt})
        returning *
      `;
      if (!channel) throw new Error("The private Messages channel was not bound");
      return {
        disposition: "accepted" as const,
        householdId: adult.household_id,
        adultId: adult.id,
        channel: channelRecord(channel),
      };
    });
  }

  async bootstrapMessagesGroup(input: BootstrapMessagesGroupInput): Promise<AcceptInboundResult | null> {
    const observed = sortedDigests(input.participantIdentityDigests);
    if (observed.length !== 2 || new Set(observed).size !== 2) return null;
    return this.#sql.begin(async (sql) => {
      const adults = await sql<PersonRow[]>`
        select * from people where kind='adult' and status='verified'
          and identity_subject_digest in ${sql(observed)} order by id
      `;
      if (
        adults.length !== 2 ||
        adults.some((adult) => adult.household_id !== adults[0]?.household_id) ||
        !adults.some((adult) => adult.identity_subject_digest === input.senderIdentitySubjectDigest)
      ) {
        return null;
      }
      const householdId = adults[0]?.household_id;
      if (!householdId) return null;
      const [currentGroup] = await sql<ChannelRow[]>`
        select * from linq_channels where household_id=${householdId}
          and audience='group' and revoked_at is null for update
      `;
      let channel = currentGroup;
      if (channel) {
        if (
          channel.provider_conversation_id !== input.providerConversationId ||
          !sameStrings(channelIdentityDigests(channel), observed)
        )
          return null;
      } else {
        const first = adults[0];
        const second = adults[1];
        if (!first?.identity_subject_digest || !second?.identity_subject_digest) return null;
        const [inserted] = await sql<ChannelRow[]>`
          insert into linq_channels (
            id,household_id,audience,provider_conversation_id,adult_one_id,identity_one_digest,
            adult_two_id,identity_two_digest,authority_digest,bound_at
          ) values (${deterministicUuid(`linq-group\0${input.providerConversationId}`)},${householdId},'group',
            ${input.providerConversationId},${first.id},${first.identity_subject_digest},${second.id},
            ${second.identity_subject_digest},${digestStrings([first.id, second.id, ...observed])},${instant(input.occurredAt)})
          returning *
        `;
        channel = inserted;
      }
      if (!channel) return null;
      const sender = adults.find(
        (adult) => adult.identity_subject_digest === input.senderIdentitySubjectDigest,
      );
      if (!sender) return null;
      return insertInbound(sql, channel, sender.id, input);
    });
  }

  async resolveLinqAuthority(input: {
    providerConversationId: string;
    audience: Audience;
    participantIdentityDigests: readonly string[];
    senderIdentitySubjectDigest: string;
    replyToProviderMessageId?: string | null;
    occurredAt: string;
  }): Promise<LinqAuthority | null> {
    const occurredAt = instant(input.occurredAt);
    const [channel] = await this.#sql<ChannelRow[]>`
      select * from linq_channels where provider_conversation_id=${input.providerConversationId}
        and audience=${input.audience} and revoked_at is null and bound_at<=${occurredAt}
      limit 1
    `;
    if (!channel) return null;
    const expected = channelIdentityDigests(channel);
    if (!sameStrings(expected, sortedDigests(input.participantIdentityDigests))) return null;
    const senderAdultId =
      channel.identity_one_digest === input.senderIdentitySubjectDigest
        ? channel.adult_one_id
        : channel.identity_two_digest === input.senderIdentitySubjectDigest
          ? channel.adult_two_id
          : null;
    if (!senderAdultId) return null;
    const [reply] = input.replyToProviderMessageId
      ? await this.#sql<{ source_id: string }[]>`
          select source_id from messages where channel_id=${channel.id}
            and provider_message_id=${input.replyToProviderMessageId} limit 1
        `
      : [];
    return authorityRecord(channel, senderAdultId, reply?.source_id ?? null);
  }

  async acceptInbound(input: AcceptInboundInput): Promise<AcceptInboundResult | null> {
    const authority = await this.resolveLinqAuthority(input);
    if (!authority) return null;
    const [channel] = await this.#sql<ChannelRow[]>`
      select * from linq_channels where id=${authority.channelId} and authority_digest=${authority.authorityDigest}
        and revoked_at is null
    `;
    if (!channel) return null;
    return this.#sql.begin((sql) => insertInbound(sql, channel, authority.senderAdultId, input));
  }

  async acceptInboundReaction(input: AcceptInboundReactionInput): Promise<AcceptInboundResult | null> {
    const authority = await this.resolveLinqAuthority({
      ...input,
      replyToProviderMessageId: input.targetProviderMessageId,
    });
    if (!authority) return null;
    const [channel] = await this.#sql<ChannelRow[]>`
      select * from linq_channels where id=${authority.channelId} and authority_digest=${authority.authorityDigest}
        and revoked_at is null
    `;
    if (!channel) return null;
    return this.#sql.begin((sql) => insertInboundReaction(sql, channel, authority.senderAdultId, input));
  }

  async stageTurnCue(input: {
    sourceId: string;
    cue: "reaction" | "work";
    occurredAt: string;
  }): Promise<string | null> {
    assertUuid(input.sourceId, "Inbound source ID");
    const occurredAt = instant(input.occurredAt);
    return this.#sql.begin(async (sql) => {
      const [turn] = await sql<
        {
          source_id: string;
          household_id: string;
          channel_id: string;
          visibility: Visibility;
          owner_adult_id: string | null;
          metadata: JsonValue;
        }[]
      >`
        select m.source_id,s.household_id,m.channel_id,s.visibility,s.owner_adult_id,s.metadata
        from messages m join sources s on s.id=m.source_id join linq_channels c on c.id=m.channel_id
        where m.source_id=${input.sourceId} and m.direction='inbound' and m.status='received'
          and c.revoked_at is null and c.stopped_at is null
        for update of m
      `;
      if (!turn) return null;
      const rootSourceId = await supersessionRoot(sql, turn.channel_id, turn.source_id, turn.metadata);
      const cueTurnId = deterministicUuid(`cue-turn\0${rootSourceId}`);
      const sourceId = deterministicUuid(`cue\0${rootSourceId}\0${input.cue}`);
      await insertOutbound(sql, {
        sourceId,
        idempotencyKey: `cue:${rootSourceId}:${input.cue}`,
        moveKind: input.cue === "reaction" ? "reaction" : "message",
        ...(input.cue === "reaction"
          ? { reaction: "emphasize", replyToSourceId: rootSourceId, turnPart: -1 as const }
          : { text: "I’m looking through this now.", turnPart: 0 as const }),
        turnId: cueTurnId,
        notBefore: occurredAt.toISOString(),
        householdId: turn.household_id,
        channelId: turn.channel_id,
        visibility: turn.visibility,
        ownerAdultId: turn.owner_adult_id,
        occurredAt,
      });
      return sourceId;
    });
  }

  async recordGmailEvidence(input: {
    householdId: string;
    ownerAdultId: string;
    connectionId: string;
    messageId: string;
    threadId: string;
    historyId: string;
    from: string;
    subject: string | null;
    sentAt: string;
    text: string;
  }): Promise<SourceRecord> {
    const messageId = required(input.messageId, "Gmail message ID");
    const connectionId = required(input.connectionId, "Google connection ID");
    const externalKey = `${connectionId}:${messageId}`;
    const sourceId = deterministicUuid(`gmail-source\0${input.householdId}\0${externalKey}`);
    const occurredAt = instant(input.sentAt);
    return this.#sql.begin(async (sql) => {
      const [connection] = await sql<{ id: string }[]>`
        select id from google_connections where id=${input.connectionId}
          and household_id=${input.householdId} and owner_adult_id=${input.ownerAdultId}
          and status='active'
      `;
      if (!connection)
        throw new FlorenceStoreUnauthorized("That Gmail connection is not owned by this adult");
      const [existing] = await sql<SourceRow[]>`
        select id,kind,visibility,owner_adult_id,label,metadata,occurred_at from sources
        where household_id=${input.householdId} and kind='gmail' and external_key=${externalKey}
      `;
      if (existing) {
        if (existing.id !== sourceId || existing.owner_adult_id !== input.ownerAdultId) {
          throw new FlorenceStoreConflict("A Gmail message reference conflicts with stored evidence");
        }
        return sourceRecord(existing);
      }
      const [source] = await sql<SourceRow[]>`
        insert into sources (
          id,household_id,kind,visibility,owner_adult_id,external_key,label,metadata,occurred_at
        ) values (${sourceId},${input.householdId},'gmail','private',${input.ownerAdultId},
          ${externalKey},${bounded(input.subject ?? `Email from ${input.from}`, 500)},
          ${sql.json({
            connectionId,
            messageId,
            threadId: required(input.threadId, "Gmail thread ID"),
            historyId: required(input.historyId, "Gmail history ID"),
            from: required(input.from, "Gmail sender"),
            subject: input.subject,
            sentAt: occurredAt.toISOString(),
          })},${occurredAt})
        returning id,kind,visibility,owner_adult_id,label,metadata,occurred_at
      `;
      if (!source) throw new Error("The Gmail evidence was not saved");
      return sourceRecord(source);
    });
  }

  async readNextInbound(now: string = new Date().toISOString()): Promise<InboundTurn | null> {
    const current = instant(now);
    await this.#sql`
      update documents set content_envelope=null
      where retained=false and discard_after<=${current} and content_envelope is not null
    `;
    const [row] = await this.#sql<
      {
        source_id: string;
        household_id: string;
        channel_id: string;
        sender_adult_id: string;
        move_kind: "message" | "reply" | "reaction";
        text: string | null;
        reaction: string | null;
        images: JsonValue;
        reply_to_source_id: string | null;
        occurred_at: Date;
      }[]
    >`
      select m.source_id,s.household_id,m.channel_id,m.sender_adult_id,m.move_kind,m.text,m.reaction,m.images,
             m.reply_to_source_id,s.occurred_at
      from messages m join sources s on s.id=m.source_id
      join linq_channels c on c.id=m.channel_id
      where m.direction='inbound' and m.status='received'
        and coalesce(m.retry_at,m.not_before)<=${current}
        and c.revoked_at is null and c.stopped_at is null
      order by coalesce(m.retry_at,m.not_before),s.occurred_at,m.source_id limit 1
    `;
    if (!row) return null;
    const [channel] = await this.#sql<ChannelRow[]>`select * from linq_channels where id=${row.channel_id}`;
    const [household] = await this.#sql<{ id: string; name: string; time_zone: string }[]>`
      select id,name,time_zone from households where id=${row.household_id}
    `;
    if (!channel || !household) return null;
    const members = await this.#sql<PersonRow[]>`
      select * from people where household_id=${row.household_id} order by adult_slot nulls last,created_at,id
    `;
    const supersededRows = await this.#sql<
      {
        source_id: string;
        sender_adult_id: string;
        move_kind: "message" | "reply" | "reaction";
        text: string | null;
        reaction: string | null;
        images: JsonValue;
        reply_to_source_id: string | null;
        occurred_at: Date;
        depth: number;
      }[]
    >`
      with recursive superseded as (
        select prior_message.source_id,prior_message.sender_adult_id,prior_message.move_kind,
          prior_message.text,prior_message.reaction,prior_message.images,prior_message.reply_to_source_id,
          prior_source.occurred_at,prior_source.metadata,1 as depth,
          array[current_source.id,prior_source.id] as path
        from sources current_source
        join sources prior_source
          on prior_source.id::text=current_source.metadata->>'supersedesSourceId'
        join messages prior_message on prior_message.source_id=prior_source.id
        where current_source.id=${row.source_id} and prior_message.channel_id=${row.channel_id}
          and prior_message.direction='inbound'
        union all
        select prior_message.source_id,prior_message.sender_adult_id,prior_message.move_kind,
          prior_message.text,prior_message.reaction,prior_message.images,prior_message.reply_to_source_id,
          prior_source.occurred_at,prior_source.metadata,superseded.depth+1,
          superseded.path||prior_source.id
        from superseded
        join sources prior_source on prior_source.id::text=superseded.metadata->>'supersedesSourceId'
        join messages prior_message on prior_message.source_id=prior_source.id
        where prior_message.channel_id=${row.channel_id} and prior_message.direction='inbound'
          and not prior_source.id=any(superseded.path) and superseded.depth<100
      )
      select source_id,sender_adult_id,move_kind,text,reaction,images,reply_to_source_id,occurred_at,depth
      from superseded order by depth desc
    `;
    const activeSourceIds = [...supersededRows.map((message) => message.source_id), row.source_id];
    const currentDocumentRows = await this.#sql<
      {
        id: string;
        parent_source_id: string;
        filename: string;
        mime_type: string;
        content_digest: string;
        content_envelope: Uint8Array;
        discard_after: Date;
      }[]
    >`
      select s.id,s.parent_source_id,d.filename,d.mime_type,d.content_digest,d.content_envelope,d.discard_after
      from sources s join sources parent on parent.id=s.parent_source_id
      join documents d on d.source_id=s.id
      where s.household_id=${row.household_id} and s.parent_source_id in ${this.#sql(activeSourceIds)}
        and s.visibility=parent.visibility and s.owner_adult_id is not distinct from parent.owner_adult_id
        and s.kind='document' and d.mime_type='application/pdf' and d.retained=false
        and d.content_envelope is not null and d.discard_after>${current}
      order by parent.occurred_at,s.id
    `;
    const privateViewer = channel.audience === "private" ? row.sender_adult_id : null;
    const recentRows = await this.#sql<
      {
        source_id: string;
        sender_adult_id: string | null;
        direction: "inbound" | "outbound";
        move_kind: "message" | "reply" | "reaction";
        text: string | null;
        reaction: string | null;
        images: JsonValue;
        reply_to_source_id: string | null;
        occurred_at: Date;
      }[]
    >`
      select m.source_id,m.sender_adult_id,m.direction,m.move_kind,m.text,m.reaction,m.images,
             m.reply_to_source_id,s.occurred_at
      from messages m join sources s on s.id=m.source_id
      where m.channel_id=${row.channel_id} and m.source_id not in ${this.#sql(activeSourceIds)}
        and m.status in ('handled','sent')
      order by s.occurred_at desc,m.turn_part desc,m.source_id desc limit 100
    `;
    const [replyTargetRow] = row.reply_to_source_id
      ? await this.#sql<
          {
            source_id: string;
            sender_adult_id: string | null;
            direction: "inbound" | "outbound";
            move_kind: "message" | "reply" | "reaction";
            text: string | null;
            reaction: string | null;
            images: JsonValue;
            reply_to_source_id: string | null;
            occurred_at: Date;
          }[]
        >`
          select m.source_id,m.sender_adult_id,m.direction,m.move_kind,m.text,m.reaction,m.images,
                 m.reply_to_source_id,s.occurred_at
          from messages m join sources s on s.id=m.source_id
          where m.source_id=${row.reply_to_source_id} and m.channel_id=${row.channel_id}
          limit 1
        `
      : [];
    const offerRows =
      channel.audience === "private"
        ? await this.#sql<
            {
              id: string;
              action_id: string;
              connection_id: string;
              owner_adult_id: string;
              basis_source_id: string;
              payload_digest: string;
              payload: JsonValue;
            }[]
          >`
            select id,action_id,connection_id,owner_adult_id,basis_source_id,payload_digest,payload
            from calendar_actions where household_id=${row.household_id}
              and owner_adult_id=${row.sender_adult_id} and status='offered'
            order by created_at,id
          `
        : [];
    const followUpRows = await this.#sql<{ id: string; text: string; due_at: Date; source_ids: string[] }[]>`
      select f.id,f.text,f.due_at,array_agg(fs.source_id order by fs.source_id) as source_ids
      from follow_ups f join follow_up_sources fs on fs.follow_up_id=f.id
      where f.household_id=${row.household_id} and f.channel_id=${row.channel_id} and f.status='scheduled'
      group by f.id,f.text,f.due_at order by f.due_at,f.id
    `;
    const authority = authorityRecord(channel, row.sender_adult_id, row.reply_to_source_id);
    return {
      message: {
        sourceId: row.source_id,
        speaker: row.sender_adult_id,
        moveKind: row.move_kind,
        text: row.text,
        reaction: row.reaction,
        images: imageReferences(row.images),
        replyToSourceId: row.reply_to_source_id,
        occurredAt: row.occurred_at.toISOString(),
      },
      supersededMessages: supersededRows.map((message) => ({
        sourceId: message.source_id,
        speaker: message.sender_adult_id,
        moveKind: message.move_kind,
        text: message.text,
        reaction: message.reaction,
        images: imageReferences(message.images),
        replyToSourceId: message.reply_to_source_id,
        occurredAt: message.occurred_at.toISOString(),
      })),
      replyTarget: replyTargetRow
        ? {
            sourceId: replyTargetRow.source_id,
            speaker:
              replyTargetRow.direction === "outbound"
                ? "florence"
                : (replyTargetRow.sender_adult_id as string),
            moveKind: replyTargetRow.move_kind,
            text: replyTargetRow.text,
            reaction: replyTargetRow.reaction,
            images: imageReferences(replyTargetRow.images),
            replyToSourceId: replyTargetRow.reply_to_source_id,
            occurredAt: replyTargetRow.occurred_at.toISOString(),
          }
        : null,
      authority,
      household: {
        id: household.id,
        name: household.name,
        timeZone: household.time_zone,
        members: members.map(personRecord),
      },
      facts: await this.#readFacts(row.household_id, privateViewer, channel.audience === "group"),
      currentDocuments: currentDocumentRows.map((document) => ({
        id: document.id,
        parentSourceId: document.parent_source_id,
        filename: document.filename,
        mimeType: pdfMimeType(document.mime_type),
        contentDigest: document.content_digest,
        contentEnvelope: document.content_envelope,
        discardAfter: document.discard_after.toISOString(),
      })),
      recentMessages: recentRows.reverse().map((turn) => ({
        sourceId: turn.source_id,
        speaker: turn.direction === "outbound" ? "florence" : (turn.sender_adult_id as string),
        moveKind: turn.move_kind,
        text: turn.text,
        reaction: turn.reaction,
        images: imageReferences(turn.images),
        replyToSourceId: turn.reply_to_source_id,
        occurredAt: turn.occurred_at.toISOString(),
      })),
      pendingFollowUps: followUpRows.map((followUp) => ({
        id: followUp.id,
        text: followUp.text,
        dueAt: followUp.due_at.toISOString(),
        sourceIds: followUp.source_ids,
      })),
      pendingCalendarOffers: offerRows.map((offer) => ({
        id: offer.id,
        actionId: offer.action_id,
        connectionId: offer.connection_id,
        ownerAdultId: offer.owner_adult_id,
        basisSourceId: offer.basis_source_id,
        proposalDigest: offer.payload_digest,
        event: calendarEvent(offer.payload),
      })),
    };
  }

  async commitTurn(input: CommitTurnInput): Promise<CommitTurnResult> {
    const handledAt = instant(input.handledAt);
    return this.#sql.begin(async (sql) => {
      const [turn] = await sql<
        {
          source_id: string;
          household_id: string;
          channel_id: string;
          sender_adult_id: string;
          audience: Audience;
          visibility: Visibility;
          move_kind: "message" | "reply" | "reaction";
          status: "received" | "handled";
          metadata: JsonValue;
          created_at: Date;
          revoked_at: Date | null;
          stopped_at: Date | null;
        }[]
      >`
        select m.source_id,s.household_id,m.channel_id,m.sender_adult_id,c.audience,s.visibility,
          m.move_kind,m.status,s.metadata,s.created_at,c.revoked_at,c.stopped_at
        from messages m join sources s on s.id=m.source_id join linq_channels c on c.id=m.channel_id
        where m.source_id=${input.sourceId} and m.direction='inbound' for update of m,s
      `;
      if (!turn) throw new FlorenceStoreConflict("The inbound message is no longer awaiting a turn");
      if (supersededBySourceId(turn.metadata)) return "superseded";
      if (turn.status !== "received" || turn.revoked_at || turn.stopped_at) {
        throw new FlorenceStoreConflict("The inbound message is no longer awaiting a turn");
      }
      const [newer] = await sql<{ source_id: string }[]>`
        select newer.source_id from messages newer
        join sources newer_source on newer_source.id=newer.source_id
        where newer.channel_id=${turn.channel_id} and newer.direction='inbound'
          and newer.move_kind in ('message','reply') and newer.source_id<>${turn.source_id}
          and (newer_source.created_at,newer_source.id) > (${turn.created_at},${turn.source_id}::uuid)
        order by newer_source.created_at,newer_source.id limit 1
        for share of newer,newer_source
      `;
      if (newer) {
        if (turn.move_kind === "reaction") {
          await sql`
            update messages set status='handled',handled_at=${handledAt},retry_at=null,
              last_error='Superseded by a newer message in this conversation'
            where source_id=${turn.source_id} and direction='inbound' and status='received'
          `;
          return "superseded";
        }
        await markInboundSuperseded(sql, [turn.source_id], turn.source_id, newer.source_id, handledAt);
        return "superseded";
      }
      if (
        turn.audience !== "private" &&
        ((input.calendarOffers?.length ?? 0) > 0 ||
          (input.approveCalendarOffers?.length ?? 0) > 0 ||
          (input.calendarActions?.length ?? 0) > 0)
      ) {
        throw new FlorenceStoreUnauthorized("Calendar changes require the owning adult's private thread");
      }

      for (const fact of input.facts ?? []) {
        if (fact.sourceIds.length === 0) throw new FlorenceStoreConflict("A fact requires a source");
        await assertSourcesVisible(
          sql,
          turn.household_id,
          turn.audience,
          turn.sender_adult_id,
          fact.sourceIds,
        );
        if (turn.audience === "group" && fact.visibility !== "household") {
          throw new FlorenceStoreUnauthorized("A group message cannot create a private fact");
        }
        if (fact.visibility === "private" && fact.ownerAdultId !== turn.sender_adult_id) {
          throw new FlorenceStoreUnauthorized("A private fact belongs to the adult in this conversation");
        }
        if (turn.audience === "private" && fact.visibility === "household") {
          throw new FlorenceStoreUnauthorized("A private message cannot create or change household memory");
        }
        const [existing] = await sql<{ id: string }[]>`
          select id from facts where household_id=${turn.household_id} and slot=${fact.slot}
            and visibility=${fact.visibility} and owner_adult_id is not distinct from ${fact.ownerAdultId}
          for update
        `;
        const factId = existing?.id ?? fact.id;
        if (existing) {
          await sql`
            update facts set subject_person_id=${fact.subjectPersonId},kind=${fact.kind},label=${fact.label},
              value=${sql.json(fact.value)},corrected_at=${handledAt},updated_at=${handledAt}
            where id=${factId}
          `;
          await sql`delete from fact_sources where fact_id=${factId}`;
        } else {
          await sql`
            insert into facts (id,household_id,subject_person_id,kind,slot,label,value,visibility,owner_adult_id,created_at,updated_at)
            values (${factId},${turn.household_id},${fact.subjectPersonId},${fact.kind},${fact.slot},${fact.label},
              ${sql.json(fact.value)},${fact.visibility},${fact.ownerAdultId},${handledAt},${handledAt})
          `;
        }
        for (const sourceId of unique(fact.sourceIds)) {
          await sql`insert into fact_sources (fact_id,source_id) values (${factId},${sourceId})`;
        }
      }
      for (const factId of unique(input.deleteFactIds ?? [])) {
        const deleted = await sql`
          delete from facts where id=${factId} and household_id=${turn.household_id}
            and ((${turn.audience}='group' and visibility='household')
              or (${turn.audience}='private' and visibility='private' and owner_adult_id=${turn.sender_adult_id}))
          returning id
        `;
        if (deleted.length !== 1) {
          throw new FlorenceStoreUnauthorized(
            "A turn tried to forget a fact outside its conversation audience",
          );
        }
      }

      for (const followUp of input.followUps ?? []) {
        if (followUp.sourceIds.length === 0) throw new FlorenceStoreConflict("A follow-up requires a source");
        await assertSourcesVisible(
          sql,
          turn.household_id,
          turn.audience,
          turn.sender_adult_id,
          followUp.sourceIds,
        );
        const inserted = await sql<{ id: string }[]>`
          insert into follow_ups (id,household_id,channel_id,dedupe_key,text,due_at,created_at)
          values (${followUp.id},${turn.household_id},${turn.channel_id},${followUp.dedupeKey},
            ${required(followUp.text, "Follow-up text")},${instant(followUp.dueAt)},${handledAt})
          on conflict (dedupe_key) do nothing returning id
        `;
        if (inserted.length > 0) {
          for (const sourceId of unique(followUp.sourceIds)) {
            await sql`insert into follow_up_sources (follow_up_id,source_id) values (${followUp.id},${sourceId})`;
          }
        }
      }
      const cancelFollowUpIds = unique(input.cancelFollowUpIds ?? []);
      if (cancelFollowUpIds.length > 0) {
        const cancelled = await sql`
          update follow_ups set status='cancelled',cancelled_at=${handledAt}
          where household_id=${turn.household_id} and channel_id=${turn.channel_id}
            and id in ${sql(cancelFollowUpIds)} and status='scheduled' returning id
        `;
        if (cancelled.length !== cancelFollowUpIds.length) {
          throw new FlorenceStoreConflict("A follow-up is no longer pending in this conversation");
        }
      }

      for (const outbound of input.outbound ?? []) {
        if (outbound.replyToSourceId) {
          await assertSourcesVisible(sql, turn.household_id, turn.audience, turn.sender_adult_id, [
            outbound.replyToSourceId,
          ]);
        }
        await insertOutbound(sql, {
          ...outbound,
          householdId: turn.household_id,
          channelId: turn.channel_id,
          parentSourceId: turn.source_id,
          visibility: turn.visibility,
          ownerAdultId: turn.visibility === "private" ? turn.sender_adult_id : null,
          occurredAt: handledAt,
        });
      }

      for (const offer of input.calendarOffers ?? []) {
        if (offer.ownerAdultId !== turn.sender_adult_id || !offer.basisSourceId) {
          throw new FlorenceStoreUnauthorized("A Calendar offer belongs to the adult in this conversation");
        }
        validateCalendarEvent(offer.event);
        assertDigest(offer.proposalDigest, "Calendar proposal");
        await assertSourcesVisible(sql, turn.household_id, turn.audience, turn.sender_adult_id, [
          offer.basisSourceId,
        ]);
        const [connection] = await sql<{ id: string }[]>`
          select id from google_connections where id=${offer.connectionId}
            and household_id=${turn.household_id} and owner_adult_id=${offer.ownerAdultId}
            and status='active'
        `;
        if (!connection)
          throw new FlorenceStoreUnauthorized("That Google connection is not owned by this adult");
        await sql`
          insert into calendar_actions (
            id,household_id,owner_adult_id,connection_id,basis_source_id,operation,action_id,
            payload,payload_digest,status,retry_at,created_at
          ) values (${offer.id},${turn.household_id},${offer.ownerAdultId},${offer.connectionId},
            ${offer.basisSourceId},${offer.operation ?? "create"},${offer.actionId},${sql.json(offer.event)},
            ${offer.proposalDigest},'offered',${handledAt},${handledAt})
        `;
      }

      for (const approval of input.approveCalendarOffers ?? []) {
        assertDigest(approval.approvalDigest, "Calendar approval");
        const approved = await sql`
          update calendar_actions set status='pending',approval_source_id=${turn.source_id},
            approval_digest=${approval.approvalDigest},retry_at=${handledAt},last_error=null
          where id=${approval.offerId} and household_id=${turn.household_id}
            and owner_adult_id=${turn.sender_adult_id} and status='offered' returning id
        `;
        if (approved.length !== 1) {
          throw new FlorenceStoreConflict("The Calendar offer is no longer awaiting this adult's approval");
        }
      }

      for (const action of input.calendarActions ?? []) {
        if (action.ownerAdultId !== turn.sender_adult_id || action.approvalMessageId !== turn.source_id) {
          throw new FlorenceStoreUnauthorized(
            "A Calendar action needs the current adult's exact approval message",
          );
        }
        validateCalendarEvent(action.event);
        assertDigest(action.approvalDigest, "Calendar approval");
        assertDigest(action.proposalDigest, "Calendar proposal");
        if (action.basisSourceId) {
          await assertSourcesVisible(sql, turn.household_id, turn.audience, turn.sender_adult_id, [
            action.basisSourceId,
          ]);
        }
        const [connection] = await sql<{ id: string }[]>`
          select id from google_connections where id=${action.connectionId}
            and household_id=${turn.household_id} and owner_adult_id=${action.ownerAdultId}
            and status='active'
        `;
        if (!connection)
          throw new FlorenceStoreUnauthorized("The approving adult does not own that Google connection");
        await sql`
          insert into calendar_actions (
            id,household_id,owner_adult_id,connection_id,basis_source_id,approval_source_id,
            operation,action_id,approval_digest,payload,payload_digest,status,retry_at,created_at
          ) values (${action.id},${turn.household_id},${action.ownerAdultId},${action.connectionId},
            ${action.basisSourceId},${action.approvalMessageId},${action.operation ?? "create"},${action.actionId},
            ${action.approvalDigest},${sql.json(action.event)},${action.proposalDigest},'pending',${handledAt},${handledAt})
        `;
      }

      const handled = await sql`
        update messages set status='handled',handled_at=${handledAt},retry_at=null,last_error=null
        where source_id=${turn.source_id} and status='received' returning source_id
      `;
      if (handled.length !== 1) throw new FlorenceStoreConflict("The inbound turn changed before commit");
      return "committed";
    });
  }

  async retryInbound(input: { sourceId: string; retryAt: string; error: string }): Promise<void> {
    const updated = await this.#sql`
      update messages set retry_at=${instant(input.retryAt)},last_error=${bounded(input.error, 2_000)}
      where source_id=${input.sourceId} and direction='inbound' and status='received' returning source_id
    `;
    if (updated.length !== 1) throw new FlorenceStoreConflict("The inbound message is no longer retryable");
  }

  async readNextOutbound(now: string = new Date().toISOString()): Promise<OutboundMessage | null> {
    const current = instant(now);
    const stale = new Date(current.getTime() - 2 * 60_000);
    await this.#sql`
      update messages set status='failed',last_error='Reaction delivery became ambiguous and was not retried'
      where direction='outbound' and move_kind='reaction' and status='sending' and sending_at<=${stale}
    `;
    await this.#sql`
      update messages set status='failed',sending_at=null,retry_at=null,
        last_error='A stale work cue was suppressed after interruption'
      where direction='outbound' and move_kind in ('message','reply') and status='sending'
        and sending_at<=${stale} and idempotency_key like 'cue:%'
    `;
    await this.#sql`
      update messages set status='pending',sending_at=null,retry_at=${current},
        last_error='Recovering an idempotent Linq send after interruption'
      where direction='outbound' and move_kind in ('message','reply')
        and status='sending' and sending_at<=${stale} and idempotency_key not like 'cue:%'
    `;
    await this.#sql`
      update messages m set status='failed',last_error='Messages authority is no longer active'
      from linq_channels c where c.id=m.channel_id and m.direction='outbound' and m.status='pending'
        and (c.revoked_at is not null or c.stopped_at is not null)
    `;
    await this.#sql`
      update follow_ups f set status='cancelled',cancelled_at=${current}
      from messages m where f.sent_message_source_id=m.source_id and f.status='queued' and m.status='failed'
    `;
    const [row] = await this.#sql<{ source_id: string }[]>`
      select m.source_id from messages m join sources s on s.id=m.source_id
      where m.direction='outbound' and m.status='pending'
        and coalesce(m.retry_at,m.not_before)<=${current}
      order by coalesce(m.retry_at,m.not_before),s.occurred_at,m.turn_part,m.source_id limit 1
    `;
    return row ? this.#readOutbound(row.source_id) : null;
  }

  async beginOutbound(input: { sourceId: string; now: string }): Promise<OutboundMessage | null> {
    const started = await this.#sql`
      update messages m set status='sending',sending_at=${instant(input.now)},last_error=null
      from linq_channels c where m.source_id=${input.sourceId} and m.channel_id=c.id
        and m.direction='outbound' and m.status='pending'
        and c.revoked_at is null and c.stopped_at is null returning m.source_id
    `;
    return started.length === 1 ? this.#readOutbound(input.sourceId) : null;
  }

  async completeOutbound(input: {
    sourceId: string;
    providerMessageId: string;
    receiptDetail?: JsonObject | null;
    sentAt: string;
  }): Promise<void> {
    const sentAt = instant(input.sentAt);
    await this.#sql.begin(async (sql) => {
      const [current] = await sql<
        {
          status: "pending" | "sending" | "sent" | "failed";
          move_kind: "message" | "reply" | "reaction";
          provider_message_id: string | null;
          receipt_detail: JsonValue | null;
        }[]
      >`
        select status,move_kind,provider_message_id,receipt_detail from messages where source_id=${input.sourceId}
          and direction='outbound' for update
      `;
      if (!current) throw new FlorenceStoreConflict("The outbound message does not exist");
      if (
        current.move_kind !== "reaction" &&
        current.provider_message_id !== null &&
        current.provider_message_id !== input.providerMessageId
      ) {
        throw new FlorenceStoreConflict("The Linq receipt conflicts with the committed message");
      }
      const receiptDetail = mergeLinqAcceptance(
        current.receipt_detail,
        input.providerMessageId,
        input.sentAt,
        input.receiptDetail ?? {},
      );
      if (current.status === "sent") {
        await sql`
          update messages set receipt_detail=${sql.json(receiptDetail)} where source_id=${input.sourceId}
        `;
        return;
      }
      const reactionConfirmed =
        current.move_kind === "reaction" && receiptDetail.providerState === "reaction_added";
      if (current.status === "failed") {
        if (!reactionConfirmed) {
          await sql`
            update messages set receipt_detail=${sql.json(receiptDetail)} where source_id=${input.sourceId}
          `;
          return;
        }
      } else if (current.status !== "sending") {
        throw new FlorenceStoreConflict("The outbound message was not begun");
      }
      await sql`
        update messages set status='sent',
          provider_message_id=${current.provider_message_id ?? input.providerMessageId},sent_at=${sentAt},
          receipt_detail=${sql.json(receiptDetail)},sending_at=null,retry_at=null,last_error=null
        where source_id=${input.sourceId}
      `;
      await sql`
        update follow_ups set status='sent'
        where sent_message_source_id=${input.sourceId} and status='queued'
      `;
    });
  }

  async retryOutbound(input: { sourceId: string; retryAt: string | null; error: string }): Promise<void> {
    const [current] = await this.#sql<{ move_kind: "message" | "reply" | "reaction" }[]>`
      select move_kind from messages where source_id=${input.sourceId}
        and direction='outbound' and status='sending'
    `;
    if (!current) throw new FlorenceStoreConflict("The outbound message is no longer retryable");
    const retryAt = input.retryAt;
    const updated = await this.#sql`
      update messages set status=${retryAt ? "pending" : "failed"},sending_at=null,
        retry_at=${retryAt ? instant(retryAt) : null},last_error=${bounded(input.error, 2_000)}
      where source_id=${input.sourceId} and direction='outbound' and status='sending' returning source_id
    `;
    if (updated.length !== 1) throw new FlorenceStoreConflict("The outbound message is no longer retryable");
    if (!retryAt) {
      await this.#sql`
        update follow_ups set status='cancelled',cancelled_at=now()
        where sent_message_source_id=${input.sourceId} and status='queued'
      `;
    }
  }

  async recordLinqObservation(input: LinqOutboundObservation): Promise<LinqObservationResult> {
    const occurredAt = instant(input.occurredAt);
    required(input.providerEventId, "Linq event ID");
    required(input.providerConversationId, "Linq conversation ID");
    required(input.traceId, "Linq trace ID");
    if (input.kind === "reaction") {
      required(input.targetProviderMessageId, "Linq reaction target message ID");
      if (!input.isFromMe || input.partIndex !== 0) return "unmatched";
    } else {
      required(input.providerMessageId, "Linq message ID");
    }
    return this.#sql.begin(async (sql) => {
      const rows =
        input.kind === "message_status"
          ? await sql<LinqObservationRow[]>`
              select m.source_id,m.status,m.move_kind,m.provider_message_id,m.sent_at,m.receipt_detail
              from messages m join linq_channels c on c.id=m.channel_id
              where m.direction='outbound' and m.move_kind in ('message','reply')
                and c.provider_conversation_id=${input.providerConversationId}
                and (m.provider_message_id=${input.providerMessageId}
                  or (${input.idempotencyKey}::text is not null and m.idempotency_key=${input.idempotencyKey}))
              for update of m
            `
          : await sql<LinqObservationRow[]>`
              select reaction.source_id,reaction.status,reaction.move_kind,reaction.provider_message_id,
                reaction.sent_at,reaction.receipt_detail
              from messages reaction join linq_channels c on c.id=reaction.channel_id
              join messages target on target.source_id=reaction.reply_to_source_id
                and target.channel_id=reaction.channel_id
              where reaction.direction='outbound' and reaction.move_kind='reaction'
                and c.provider_conversation_id=${input.providerConversationId}
                and target.provider_message_id=${input.targetProviderMessageId}
                and reaction.reaction=${input.reaction}
              for update of reaction
            `;
      if (rows.length !== 1) return "unmatched";
      const current = rows[0];
      if (!current) return "unmatched";
      if (
        input.kind === "message_status" &&
        current.provider_message_id !== null &&
        current.provider_message_id !== input.providerMessageId
      ) {
        return "unmatched";
      }
      const merged = mergeLinqObservation(current.receipt_detail, input);
      if (merged.duplicate) return "duplicate";
      const succeeded =
        merged.providerState === "sent" ||
        merged.providerState === "delivered" ||
        merged.providerState === "read" ||
        merged.providerState === "reaction_added";
      const failed = merged.providerState === "failed" || merged.providerState === "reaction_removed";
      const status = succeeded ? "sent" : failed ? "failed" : current.status;
      const providerMessageId =
        current.provider_message_id ??
        (input.kind === "message_status"
          ? input.providerMessageId
          : `reaction-event:${input.providerEventId}`);
      await sql`
        update messages set status=${status},provider_message_id=${providerMessageId},
          sent_at=${succeeded ? (current.sent_at ?? occurredAt) : current.sent_at},
          receipt_detail=${sql.json(merged.detail)},sending_at=null,retry_at=null,
          last_error=${failed ? merged.lastError : null}
        where source_id=${current.source_id}
      `;
      if (current.move_kind !== "reaction") {
        if (succeeded) {
          await sql`
            update follow_ups set status='sent'
            where sent_message_source_id=${current.source_id} and status='queued'
          `;
        } else if (failed) {
          await sql`
            update follow_ups set status='cancelled',cancelled_at=${occurredAt}
            where sent_message_source_id=${current.source_id} and status='queued'
          `;
        }
      }
      return "applied";
    });
  }

  async promoteDueFollowUp(input: { now: string }): Promise<OutboundMessage | null> {
    const now = instant(input.now);
    const sourceId = await this.#sql.begin(async (sql) => {
      const [followUp] = await sql<
        {
          id: string;
          household_id: string;
          channel_id: string;
          text: string;
          audience: Audience;
          adult_one_id: string;
        }[]
      >`
        select f.id,f.household_id,f.channel_id,f.text,c.audience,c.adult_one_id
        from follow_ups f join linq_channels c on c.id=f.channel_id
        where f.status='scheduled' and f.due_at<=${now}
          and c.revoked_at is null and c.stopped_at is null
        order by f.due_at,f.id for update of f skip locked limit 1
      `;
      if (!followUp) return null;
      const outboundSourceId = deterministicUuid(`follow-up-message\0${followUp.id}`);
      await insertOutbound(sql, {
        sourceId: outboundSourceId,
        idempotencyKey: `follow-up:${followUp.id}`,
        moveKind: "message",
        text: followUp.text,
        reaction: null,
        replyToSourceId: null,
        turnId: deterministicUuid(`follow-up-turn\0${followUp.id}`),
        turnPart: 0,
        notBefore: now.toISOString(),
        householdId: followUp.household_id,
        channelId: followUp.channel_id,
        visibility: followUp.audience === "group" ? "household" : "private",
        ownerAdultId: followUp.audience === "private" ? followUp.adult_one_id : null,
        occurredAt: now,
      });
      await sql`
        update follow_ups set status='queued',sent_message_source_id=${outboundSourceId}
        where id=${followUp.id} and status='scheduled'
      `;
      return outboundSourceId;
    });
    return sourceId ? this.#readOutbound(sourceId) : null;
  }

  async readNextCalendarAction(
    now: string = new Date().toISOString(),
  ): Promise<ApprovedCalendarAction | null> {
    const claimedAt = instant(now);
    return this.#sql.begin(async (sql) => {
      const [row] = await sql<
        {
          id: string;
          action_id: string;
          household_id: string;
          connection_id: string;
          owner_adult_id: string;
          approval_source_id: string | null;
          approval_digest: string;
          payload_digest: string;
          payload: JsonValue;
          approval_metadata: JsonValue;
        }[]
      >`
        select a.id,a.action_id,a.household_id,a.connection_id,a.owner_adult_id,a.approval_source_id,
               a.approval_digest,a.payload_digest,a.payload,approval.metadata as approval_metadata
        from calendar_actions a join sources approval on approval.id=a.approval_source_id
        where a.status='pending' and a.retry_at<=${claimedAt}
        order by a.retry_at,a.created_at,a.id for update of a skip locked limit 1
      `;
      if (!row?.approval_source_id) return null;
      if (supersededBySourceId(row.approval_metadata)) {
        await sql`
          update calendar_actions set status='failed',retry_at=${claimedAt},
            last_error='Superseded before provider execution by a newer message in this conversation'
          where id=${row.id} and status='pending'
        `;
        return null;
      }
      await sql`
        update calendar_actions set retry_at=${new Date(claimedAt.getTime() + CALENDAR_CLAIM_MS)}
        where id=${row.id} and status='pending'
      `;
      return {
        id: row.id,
        actionId: row.action_id,
        householdId: row.household_id,
        connectionId: row.connection_id,
        ownerAdultId: row.owner_adult_id,
        approvalMessageId: row.approval_source_id,
        approvalDigest: row.approval_digest,
        proposalDigest: row.payload_digest,
        event: calendarEvent(row.payload),
      };
    });
  }

  async completeCalendarAction(input: {
    id: string;
    providerEventId: string;
    providerEtag: string;
    proofDigest: string;
    proof: JsonObject;
    confirmationText: string;
    committedAt: string;
  }): Promise<void> {
    assertDigest(input.proofDigest, "Calendar proof");
    const committedAt = instant(input.committedAt);
    const confirmationText = bounded(required(input.confirmationText, "Calendar confirmation"), 10_000);
    await this.#sql.begin(async (sql) => {
      const [current] = await sql<
        {
          status: "offered" | "pending" | "committed" | "failed";
          provider_event_id: string | null;
          proof_digest: string | null;
          committed_at: Date | null;
          action_id: string;
          household_id: string;
          owner_adult_id: string;
          approval_source_id: string | null;
          channel_id: string | null;
          direction: "inbound" | "outbound" | null;
          sender_adult_id: string | null;
          audience: Audience | null;
          adult_one_id: string | null;
        }[]
      >`
        select a.status,a.provider_event_id,a.proof_digest,a.committed_at,a.action_id,a.household_id,
          a.owner_adult_id,a.approval_source_id,approval.channel_id,approval.direction,
          approval.sender_adult_id,c.audience,c.adult_one_id
        from calendar_actions a
        left join messages approval on approval.source_id=a.approval_source_id
        left join linq_channels c on c.id=approval.channel_id
        where a.id=${input.id} for update of a
      `;
      if (!current) throw new FlorenceStoreConflict("The Calendar action does not exist");
      if (
        !current.approval_source_id ||
        !current.channel_id ||
        current.direction !== "inbound" ||
        current.sender_adult_id !== current.owner_adult_id ||
        current.audience !== "private" ||
        current.adult_one_id !== current.owner_adult_id
      ) {
        throw new FlorenceStoreConflict("The Calendar approval is not bound to its adult's private thread");
      }
      if (current.status === "committed") {
        if (
          current.provider_event_id !== input.providerEventId ||
          current.proof_digest !== input.proofDigest
        ) {
          throw new FlorenceStoreConflict("The Calendar proof conflicts with the committed action");
        }
      } else if (current.status !== "pending") {
        throw new FlorenceStoreConflict("The Calendar action cannot be completed");
      } else {
        await sql`
          update calendar_actions set status='committed',provider_event_id=${required(input.providerEventId, "Google Calendar event ID")},
            provider_etag=${required(input.providerEtag, "Google Calendar etag")},
            proof_digest=${input.proofDigest},proof=${sql.json(input.proof)},committed_at=${committedAt},last_error=null
          where id=${input.id}
        `;
      }
      const confirmationAt = current.committed_at ?? committedAt;
      await insertOutbound(sql, {
        sourceId: deterministicUuid(`calendar-confirmation\0${current.action_id}`),
        idempotencyKey: `calendar-confirmation:${current.action_id}`,
        moveKind: "reply",
        text: confirmationText,
        reaction: null,
        replyToSourceId: current.approval_source_id,
        turnId: deterministicUuid(`calendar-confirmation-turn\0${current.action_id}`),
        turnPart: 0,
        notBefore: confirmationAt.toISOString(),
        householdId: current.household_id,
        channelId: current.channel_id,
        visibility: "private",
        ownerAdultId: current.owner_adult_id,
        occurredAt: confirmationAt,
      });
    });
  }

  async failCalendarAction(input: {
    id: string;
    error: string;
    failureText: string;
    failedAt: string;
  }): Promise<void> {
    const failedAt = instant(input.failedAt);
    const error = bounded(required(input.error, "Calendar failure"), 2_000);
    const failureText = bounded(required(input.failureText, "Calendar failure message"), 10_000);
    await this.#sql.begin(async (sql) => {
      const [current] = await sql<
        {
          status: "offered" | "pending" | "committed" | "failed";
          action_id: string;
          household_id: string;
          owner_adult_id: string;
          approval_source_id: string | null;
          retry_at: Date;
          channel_id: string | null;
          direction: "inbound" | "outbound" | null;
          sender_adult_id: string | null;
          audience: Audience | null;
          adult_one_id: string | null;
        }[]
      >`
        select a.status,a.action_id,a.household_id,a.owner_adult_id,a.approval_source_id,a.retry_at,
          approval.channel_id,approval.direction,approval.sender_adult_id,c.audience,c.adult_one_id
        from calendar_actions a
        left join messages approval on approval.source_id=a.approval_source_id
        left join linq_channels c on c.id=approval.channel_id
        where a.id=${input.id} for update of a
      `;
      if (!current) throw new FlorenceStoreConflict("The Calendar action does not exist");
      if (
        !current.approval_source_id ||
        !current.channel_id ||
        current.direction !== "inbound" ||
        current.sender_adult_id !== current.owner_adult_id ||
        current.audience !== "private" ||
        current.adult_one_id !== current.owner_adult_id
      ) {
        throw new FlorenceStoreConflict("The Calendar approval is not bound to its adult's private thread");
      }
      if (current.status === "pending") {
        await sql`
          update calendar_actions set status='failed',retry_at=${failedAt},last_error=${error}
          where id=${input.id}
        `;
      } else if (current.status !== "failed") {
        throw new FlorenceStoreConflict("The Calendar action cannot fail from its current state");
      }
      const notificationAt = current.status === "failed" ? current.retry_at : failedAt;
      await insertOutbound(sql, {
        sourceId: deterministicUuid(`calendar-failure\0${current.action_id}`),
        idempotencyKey: `calendar-failure:${current.action_id}`,
        moveKind: "reply",
        text: failureText,
        reaction: null,
        replyToSourceId: current.approval_source_id,
        turnId: deterministicUuid(`calendar-failure-turn\0${current.action_id}`),
        turnPart: 0,
        notBefore: notificationAt.toISOString(),
        householdId: current.household_id,
        channelId: current.channel_id,
        visibility: "private",
        ownerAdultId: current.owner_adult_id,
        occurredAt: notificationAt,
      });
    });
  }

  async retryCalendarAction(input: { id: string; retryAt: string; error: string }): Promise<void> {
    const updated = await this.#sql`
      update calendar_actions set retry_at=${instant(input.retryAt)},last_error=${bounded(input.error, 2_000)}
      where id=${input.id} and status='pending' returning id
    `;
    if (updated.length !== 1) throw new FlorenceStoreConflict("The Calendar action is no longer retryable");
  }

  async createPending(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
    stateDigest: string;
    sessionBindingDigest: string;
    stateExpiresAt: string;
    now: string;
  }): Promise<GoogleConnectionView> {
    assertDigest(input.stateDigest, "Google OAuth state");
    assertDigest(input.sessionBindingDigest, "Google OAuth session binding");
    await this.#requireVerifiedAdult(input.householdId, input.ownerAdultId);
    const [row] = await this.#sql<GoogleConnectionRow[]>`
      insert into google_connections (
        id,household_id,owner_adult_id,status,state_digest,session_binding_digest,state_expires_at,
        created_at,updated_at
      ) values (${input.connectionId},${input.householdId},${input.ownerAdultId},'pending',${input.stateDigest},
        ${input.sessionBindingDigest},${instant(input.stateExpiresAt)},${instant(input.now)},${instant(input.now)})
      returning *
    `;
    if (!row) throw new Error("The Google connection was not created");
    return googleConnectionView(row);
  }

  async consumePendingState(input: {
    stateDigest: string;
    sessionBindingDigest: string;
    now: string;
  }): Promise<PendingGoogleConnection | null> {
    const [row] = await this.#sql<GoogleConnectionRow[]>`
      update google_connections set state_consumed_at=${instant(input.now)},updated_at=${instant(input.now)}
      where status='pending' and state_digest=${input.stateDigest}
        and session_binding_digest=${input.sessionBindingDigest} and state_consumed_at is null
        and state_expires_at>=${instant(input.now)} returning *
    `;
    return row?.session_binding_digest
      ? {
          connectionId: row.id,
          householdId: row.household_id,
          ownerAdultId: row.owner_adult_id,
          stateDigest: row.state_digest,
          sessionBindingDigest: row.session_binding_digest,
        }
      : null;
  }

  async activate(input: {
    connectionId: string;
    stateDigest: string;
    googleSubjectDigest: string;
    emailLabel: string;
    grantedScopes: readonly GoogleScope[];
    refreshTokenEnvelope: string;
    now: string;
  }): Promise<GoogleConnectionView> {
    assertDigest(input.googleSubjectDigest, "Google identity");
    const [row] = await this.#sql<GoogleConnectionRow[]>`
      update google_connections set status='active',google_subject_digest=${input.googleSubjectDigest},
        email_label=${required(input.emailLabel, "Google account email")},granted_scopes=${this.#sql.array([...input.grantedScopes])},
        refresh_token_envelope=${required(input.refreshTokenEnvelope, "Google refresh token envelope")},
        session_binding_digest=null,last_error=null,updated_at=${instant(input.now)}
      where id=${input.connectionId} and state_digest=${input.stateDigest}
        and state_consumed_at is not null and status='pending' returning *
    `;
    if (!row) throw new FlorenceStoreConflict("Google OAuth state is no longer current");
    return googleConnectionView(row);
  }

  async markPendingFailure(input: {
    connectionId: string;
    stateDigest: string;
    error: string;
    now: string;
  }): Promise<void> {
    await this.#sql`
      update google_connections set last_error=${bounded(input.error, 2_000)},updated_at=${instant(input.now)}
      where id=${input.connectionId} and state_digest=${input.stateDigest} and status='pending'
    `;
  }

  async listActive(input: {
    householdId: string;
    ownerAdultId: string;
  }): Promise<readonly GoogleConnectionView[]> {
    const rows = await this.#sql<GoogleConnectionRow[]>`
      select * from google_connections where household_id=${input.householdId}
        and owner_adult_id=${input.ownerAdultId} and status='active' order by created_at,id
    `;
    return rows.map(googleConnectionView);
  }

  async disconnect(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
    now: string;
  }): Promise<{ view: GoogleConnectionView; refreshTokenEnvelope: string | null } | null> {
    return this.#sql.begin(async (sql) => {
      const [current] = await sql<GoogleConnectionRow[]>`
        select * from google_connections where id=${input.connectionId}
          and household_id=${input.householdId} and owner_adult_id=${input.ownerAdultId}
          and status<>'disconnected' for update
      `;
      if (!current) return null;
      const [row] = await sql<GoogleConnectionRow[]>`
        update google_connections set status='disconnected',refresh_token_envelope=null,
          updated_at=${instant(input.now)} where id=${input.connectionId} returning *
      `;
      if (!row) throw new Error("The Google connection was not disconnected");
      return { view: googleConnectionView(row), refreshTokenEnvelope: current.refresh_token_envelope };
    });
  }

  async readActiveGoogleCredential(input: {
    connectionId: string;
    householdId: string;
    ownerAdultId: string;
  }): Promise<ActiveGoogleCredential | null> {
    const [row] = await this.#sql<GoogleConnectionRow[]>`
      select * from google_connections where id=${input.connectionId}
        and household_id=${input.householdId} and owner_adult_id=${input.ownerAdultId} and status='active'
    `;
    return row?.refresh_token_envelope
      ? {
          connectionId: row.id,
          householdId: row.household_id,
          ownerAdultId: row.owner_adult_id,
          refreshTokenEnvelope: row.refresh_token_envelope,
        }
      : null;
  }

  async #readFacts(
    householdId: string,
    viewerAdultId: string | null,
    householdOnly = false,
  ): Promise<readonly FactRecord[]> {
    const rows = await this.#sql<
      {
        id: string;
        household_id: string;
        subject_person_id: string | null;
        kind: FactRecord["kind"];
        slot: string;
        label: string;
        value: JsonValue;
        visibility: Visibility;
        owner_adult_id: string | null;
        corrected_at: Date | null;
        updated_at: Date;
      }[]
    >`
      select id,household_id,subject_person_id,kind,slot,label,value,visibility,owner_adult_id,
             corrected_at,updated_at
      from facts where household_id=${householdId}
        and (${householdOnly} or visibility='household' or owner_adult_id=${viewerAdultId})
        and (not ${householdOnly} or visibility='household')
      order by kind,label,id
    `;
    if (rows.length === 0) return [];
    const sourceRows = await this.#sql<({ fact_id: string } & SourceRow)[]>`
      select fs.fact_id,s.id,s.kind,s.visibility,s.owner_adult_id,s.label,s.metadata,s.occurred_at
      from fact_sources fs join sources s on s.id=fs.source_id
      where fs.fact_id in ${this.#sql(rows.map((row) => row.id))}
        and (s.visibility='household' or s.owner_adult_id=${viewerAdultId})
      order by s.occurred_at,s.id
    `;
    const sources = new Map<string, SourceRecord[]>();
    for (const row of sourceRows) {
      const list = sources.get(row.fact_id) ?? [];
      list.push(sourceRecord(row));
      sources.set(row.fact_id, list);
    }
    return rows.map((row) => ({
      id: row.id,
      householdId: row.household_id,
      subjectPersonId: row.subject_person_id,
      kind: row.kind,
      slot: row.slot,
      label: row.label,
      value: row.value,
      visibility: row.visibility,
      ownerAdultId: row.owner_adult_id,
      sources: sources.get(row.id) ?? [],
      correctedAt: row.corrected_at?.toISOString() ?? null,
      updatedAt: row.updated_at.toISOString(),
    }));
  }

  async #readOutbound(sourceId: string): Promise<OutboundMessage | null> {
    const [row] = await this.#sql<
      {
        source_id: string;
        idempotency_key: string;
        provider_conversation_id: string;
        audience: Audience;
        identity_one_digest: string;
        identity_two_digest: string | null;
        move_kind: "message" | "reply" | "reaction";
        text: string | null;
        reaction: string | null;
        reply_provider_message_id: string | null;
      }[]
    >`
      select m.source_id,m.idempotency_key,c.provider_conversation_id,c.audience,
             c.identity_one_digest,c.identity_two_digest,m.move_kind,m.text,m.reaction,
             reply.provider_message_id as reply_provider_message_id
      from messages m join linq_channels c on c.id=m.channel_id
      left join messages reply on reply.source_id=m.reply_to_source_id
      where m.source_id=${sourceId} and m.direction='outbound' and m.status in ('pending','sending')
        and c.revoked_at is null and c.stopped_at is null
    `;
    if (!row) return null;
    return {
      sourceId: row.source_id,
      idempotencyKey: row.idempotency_key,
      providerConversationId: row.provider_conversation_id,
      expectedAuthority: {
        audience: row.audience,
        participantIdentityDigests: [row.identity_one_digest, row.identity_two_digest]
          .filter((value): value is string => value !== null)
          .sort(),
      },
      moveKind: row.move_kind,
      text: row.text,
      reaction: row.reaction,
      replyToProviderMessageId: row.reply_provider_message_id,
    };
  }

  async #requireVerifiedAdult(householdId: string, adultId: string): Promise<void> {
    const [adult] = await this.#sql<{ id: string }[]>`
      select id from people where household_id=${householdId} and id=${adultId}
        and kind='adult' and status='verified'
    `;
    if (!adult) throw new FlorenceStoreUnauthorized();
  }
}

type OutboundInsert = OutboundDraft & {
  householdId: string;
  channelId: string;
  parentSourceId?: string;
  visibility: Visibility;
  ownerAdultId: string | null;
  occurredAt: Date;
};

function personRecord(row: PersonRow): FamilyMemberRecord {
  const messagesIdentity =
    row.kind === "child"
      ? null
      : row.identity_subject_digest !== null
        ? "connected"
        : row.invitation_consumed_at === null &&
            row.invitation_expires_at !== null &&
            row.invitation_expires_at >= new Date()
          ? "invited"
          : "not_invited";
  return {
    id: row.id,
    householdId: row.household_id,
    kind: row.kind,
    role: row.role,
    adultSlot: row.adult_slot,
    displayName: row.display_name,
    status: row.status,
    messagesIdentity,
    profile: row.profile,
    preferences: row.preferences,
  };
}

function channelRecord(row: ChannelRow): LinqChannelRecord {
  return {
    id: row.id,
    householdId: row.household_id,
    audience: row.audience,
    providerConversationId: row.provider_conversation_id,
    adultIds: [row.adult_one_id, row.adult_two_id].filter((value): value is string => value !== null),
    participantIdentityDigests: channelIdentityDigests(row),
    authorityDigest: row.authority_digest,
    boundAt: row.bound_at.toISOString(),
    revokedAt: row.revoked_at?.toISOString() ?? null,
    stoppedAt: row.stopped_at?.toISOString() ?? null,
  };
}

function authorityRecord(
  row: ChannelRow,
  senderAdultId: string,
  replyToSourceId: string | null,
): LinqAuthority {
  return {
    householdId: row.household_id,
    channelId: row.id,
    audience: row.audience,
    providerConversationId: row.provider_conversation_id,
    senderAdultId,
    adultIds: [row.adult_one_id, row.adult_two_id].filter((value): value is string => value !== null),
    expectedParticipantIdentityDigests: channelIdentityDigests(row),
    authorityDigest: row.authority_digest,
    replyToSourceId,
    stopped: row.stopped_at !== null,
  };
}

function channelIdentityDigests(row: ChannelRow): string[] {
  return [row.identity_one_digest, row.identity_two_digest]
    .filter((value): value is string => value !== null)
    .sort();
}

function sourceRecord(row: SourceRow): SourceRecord {
  return {
    id: row.id,
    kind: row.kind,
    visibility: row.visibility,
    ownerAdultId: row.owner_adult_id,
    label: row.label,
    metadata: row.metadata,
    occurredAt: row.occurred_at.toISOString(),
  };
}

function googleConnectionView(row: GoogleConnectionRow): GoogleConnectionView {
  return {
    connectionId: row.id,
    householdId: row.household_id,
    ownerAdultId: row.owner_adult_id,
    status: row.status,
    emailLabel: row.email_label,
    grantedScopes: [...row.granted_scopes],
    lastError: row.last_error,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

async function requireSteward(
  sql: postgres.TransactionSql,
  householdId: string,
  adultId: string,
): Promise<void> {
  const [actor] = await sql<{ id: string }[]>`
    select id from people where household_id=${householdId} and id=${adultId}
      and kind='adult' and role='steward' and status='verified'
  `;
  if (!actor) throw new FlorenceStoreUnauthorized("A verified household steward must make this change");
}

async function assertSourcesVisible(
  sql: postgres.TransactionSql,
  householdId: string,
  audience: Audience,
  adultId: string,
  sourceIds: readonly string[],
): Promise<void> {
  const ids = unique(sourceIds);
  if (ids.length === 0) throw new FlorenceStoreConflict("At least one source is required");
  const rows = await sql<{ id: string }[]>`
    select id from sources where household_id=${householdId} and id in ${sql(ids)}
      and (visibility='household' or (${audience}='private' and owner_adult_id=${adultId}))
  `;
  if (rows.length !== ids.length) {
    throw new FlorenceStoreUnauthorized("A turn cited a source outside its conversation audience");
  }
}

type SupersessionTail = {
  chainSourceIds: string[];
  tailSourceId: string;
};

async function resolveSupersessionTail(
  sql: postgres.TransactionSql,
  channelId: string,
  requestedSourceId: string,
  incomingSourceId: string,
): Promise<SupersessionTail> {
  const chainSourceIds: string[] = [];
  const seen = new Set<string>();
  let sourceId = requestedSourceId;
  for (let depth = 0; depth < 100; depth += 1) {
    if (sourceId === incomingSourceId || seen.has(sourceId)) {
      throw new FlorenceStoreConflict("An inbound supersession chain contains a cycle");
    }
    seen.add(sourceId);
    const [message] = await sql<{ source_id: string; metadata: JsonValue }[]>`
      select m.source_id,s.metadata from messages m join sources s on s.id=m.source_id
      where m.source_id=${sourceId} and m.channel_id=${channelId} and m.direction='inbound'
      for update of m,s
    `;
    if (!message) {
      throw new FlorenceStoreUnauthorized("A message can only supersede an inbound turn in its conversation");
    }
    chainSourceIds.push(message.source_id);
    const nextSourceId = supersededBySourceId(message.metadata);
    if (!nextSourceId) return { chainSourceIds, tailSourceId: message.source_id };
    sourceId = nextSourceId;
  }
  throw new FlorenceStoreConflict("An inbound supersession chain is too long");
}

async function supersessionRoot(
  sql: postgres.TransactionSql,
  channelId: string,
  currentSourceId: string,
  currentMetadata: JsonValue,
): Promise<string> {
  const seen = new Set([currentSourceId]);
  let rootSourceId = currentSourceId;
  let metadata = currentMetadata;
  for (let depth = 0; depth < 100; depth += 1) {
    const priorSourceId = supersedesSourceId(metadata);
    if (!priorSourceId) return rootSourceId;
    if (seen.has(priorSourceId)) {
      throw new FlorenceStoreConflict("An inbound supersession chain contains a cycle");
    }
    seen.add(priorSourceId);
    const [prior] = await sql<{ source_id: string; metadata: JsonValue }[]>`
      select m.source_id,s.metadata from messages m join sources s on s.id=m.source_id
      where m.source_id=${priorSourceId} and m.channel_id=${channelId} and m.direction='inbound'
      for share of m,s
    `;
    if (!prior) throw new FlorenceStoreConflict("An inbound supersession chain is incomplete");
    rootSourceId = prior.source_id;
    metadata = prior.metadata;
  }
  throw new FlorenceStoreConflict("An inbound supersession chain is too long");
}

async function markInboundSuperseded(
  sql: postgres.TransactionSql,
  chainSourceIds: readonly string[],
  tailSourceId: string,
  newerSourceId: string,
  handledAt: Date,
): Promise<void> {
  const chain = unique(chainSourceIds);
  if (chain.length === 0 || chain.at(-1) !== tailSourceId) {
    throw new FlorenceStoreConflict("An inbound supersession chain is invalid");
  }
  const [newer] = await sql<{ metadata: JsonValue; visibility: Visibility; owner_adult_id: string | null }[]>`
    select metadata,visibility,owner_adult_id from sources where id=${newerSourceId} for update
  `;
  if (!newer) throw new FlorenceStoreConflict("The superseding inbound message does not exist");
  const discardSupersededFacts = jsonRecord(newer.metadata).discardSupersededFacts;
  if (discardSupersededFacts !== undefined && typeof discardSupersededFacts !== "boolean") {
    throw new FlorenceStoreConflict("Stored inbound retention boundaries are invalid");
  }
  const existingPrior = supersedesSourceId(newer.metadata);
  if (existingPrior && existingPrior !== tailSourceId) {
    throw new FlorenceStoreConflict("The superseding inbound message belongs to another turn chain");
  }
  await sql`
    update sources set metadata=metadata||${sql.json({ supersededBySourceId: newerSourceId })}
    where id=${tailSourceId}
  `;
  if (!existingPrior) {
    await sql`
      update sources set metadata=metadata||${sql.json({ supersedesSourceId: tailSourceId })}
      where id=${newerSourceId}
    `;
  }
  await sql`
    update messages set status='handled',handled_at=${handledAt},retry_at=null,
      last_error='Superseded by a newer message in this conversation'
    where source_id in ${sql(chain)} and direction='inbound' and status='received'
  `;
  const finalTurnIds = chain.map((sourceId) => deterministicUuid(`turn\0${sourceId}`));
  const calendarIds = chain.map((sourceId) => deterministicUuid(`calendar\0${sourceId}`));
  await sql`
    update messages set status='failed',retry_at=null,
      last_error='Superseded before delivery by a newer message in this conversation'
    where direction='outbound' and status='pending' and turn_id in ${sql(finalTurnIds)}
  `;
  if (discardSupersededFacts === true) {
    await sql`
      delete from facts fact
      where ((${newer.visibility}='household' and fact.visibility='household')
          or (${newer.visibility}='private' and fact.visibility='private'
            and fact.owner_adult_id=${newer.owner_adult_id}))
        and exists (
        select 1 from fact_sources fs join sources source on source.id=fs.source_id
        where fs.fact_id=fact.id
          and (source.id in ${sql(chain)} or source.parent_source_id in ${sql(chain)})
      ) and not exists (
        select 1 from fact_sources fs join sources source on source.id=fs.source_id
        where fs.fact_id=fact.id
          and source.id not in ${sql(chain)}
          and (source.parent_source_id is null or source.parent_source_id not in ${sql(chain)})
      )
    `;
  }
  await sql`
    update messages reminder set status='failed',retry_at=null,
      last_error='Superseded before delivery by a newer message in this conversation'
    from follow_ups f
    where f.sent_message_source_id=reminder.source_id and f.status='queued'
      and reminder.status='pending' and exists (
        select 1 from follow_up_sources fs join sources source on source.id=fs.source_id
        where fs.follow_up_id=f.id
          and (source.id in ${sql(chain)} or source.parent_source_id in ${sql(chain)})
      )
  `;
  await sql`
    update follow_ups f set status='cancelled',cancelled_at=${handledAt}
    where f.status in ('scheduled','queued') and exists (
      select 1 from follow_up_sources fs join sources source on source.id=fs.source_id
      where fs.follow_up_id=f.id
        and (source.id in ${sql(chain)} or source.parent_source_id in ${sql(chain)})
    )
  `;
  await sql`
    delete from calendar_actions action where action.status='offered'
      and (action.id in ${sql(calendarIds)} or action.basis_source_id in (
          select source.id from sources source
          where source.id in ${sql(chain)} or source.parent_source_id in ${sql(chain)}
        ))
  `;
  await sql`
    update calendar_actions set status='failed',retry_at=${handledAt},
      last_error='Superseded before provider execution by a newer message in this conversation'
    where status='pending' and retry_at<=${handledAt} and approval_source_id in ${sql(chain)}
  `;
}

function supersedesSourceId(metadata: JsonValue): string | null {
  return supersessionMetadataId(metadata, "supersedesSourceId");
}

function supersededBySourceId(metadata: JsonValue): string | null {
  return supersessionMetadataId(metadata, "supersededBySourceId");
}

function supersessionMetadataId(metadata: JsonValue, key: string): string | null {
  const value = jsonRecord(metadata)[key];
  if (value === undefined) return null;
  if (typeof value !== "string") {
    throw new FlorenceStoreConflict("Stored inbound supersession metadata is invalid");
  }
  assertUuid(value, "Stored inbound supersession source ID");
  return value;
}

async function insertInbound(
  sql: postgres.TransactionSql,
  channel: ChannelRow,
  senderAdultId: string,
  input: AcceptInboundInput,
): Promise<AcceptInboundResult> {
  if (
    input.audience !== channel.audience ||
    !sameStrings(channelIdentityDigests(channel), sortedDigests(input.participantIdentityDigests))
  ) {
    throw new FlorenceStoreUnauthorized("Current Linq participants do not match the bound channel");
  }
  const expectedSenderDigest =
    channel.adult_one_id === senderAdultId ? channel.identity_one_digest : channel.identity_two_digest;
  if (expectedSenderDigest !== input.senderIdentitySubjectDigest) {
    throw new FlorenceStoreUnauthorized("The Linq sender is not a bound household adult");
  }
  const images = validateImageReferences(input.images ?? []);
  const occurredAt = instant(input.occurredAt);
  const documents = validateInboundDocuments(input.documents ?? [], occurredAt);
  const sourceId = deterministicUuid(`linq-v3\0signal\0${input.providerEventId}`);
  let requestedSupersedesSourceId = input.supersedesSourceId ?? null;
  if (requestedSupersedesSourceId) {
    assertUuid(requestedSupersedesSourceId, "Superseded inbound source ID");
  }
  const [existing] = await sql<
    {
      source_id: string;
      provider_message_id: string;
      channel_id: string;
      text: string | null;
      images: JsonValue;
    }[]
  >`
    select source_id,provider_message_id,channel_id,text,images from messages
    where provider_event_id=${input.providerEventId} limit 1
  `;
  if (existing) {
    const existingDocuments = await sql<
      {
        document_id: string;
        external_key: string;
        filename: string;
        mime_type: string;
        content_digest: string;
        retained: boolean;
        discard_after: Date | null;
      }[]
    >`
      select s.id as document_id,s.external_key,d.filename,d.mime_type,d.content_digest,
             d.retained,d.discard_after
      from sources s join documents d on d.source_id=s.id
      where s.parent_source_id=${sourceId} order by s.id
    `;
    if (
      existing.source_id !== sourceId ||
      existing.provider_message_id !== input.providerMessageId ||
      existing.channel_id !== channel.id ||
      existing.text !== input.text ||
      !sameImageReferences(imageReferences(existing.images), images) ||
      !sameInboundDocuments(existingDocuments, documents)
    ) {
      throw new FlorenceStoreConflict("A Linq event ID was reused with different content");
    }
    return {
      disposition: "duplicate",
      sourceId,
      householdId: channel.household_id,
      channelId: channel.id,
    };
  }
  if (!requestedSupersedesSourceId) {
    const [pendingTurn] = await sql<{ source_id: string }[]>`
      select inbound.source_id
      from messages pending
      join sources pending_source on pending_source.id=pending.source_id
      join messages inbound on inbound.source_id=pending_source.parent_source_id
      join sources inbound_source on inbound_source.id=inbound.source_id
      where pending.channel_id=${channel.id} and pending.direction='outbound' and pending.status='pending'
        and inbound.direction='inbound' and inbound.move_kind in ('message','reply')
      order by inbound_source.created_at desc,inbound.source_id desc limit 1
      for update of inbound,inbound_source
    `;
    requestedSupersedesSourceId = pendingTurn?.source_id ?? null;
  }
  if (!requestedSupersedesSourceId) {
    const [pendingCalendarTurn] = await sql<{ source_id: string }[]>`
      select inbound.source_id
      from calendar_actions action
      join messages inbound on inbound.source_id=action.approval_source_id
      join sources inbound_source on inbound_source.id=inbound.source_id
      where action.status='pending' and action.retry_at=inbound.handled_at
        and inbound.channel_id=${channel.id} and inbound.direction='inbound'
        and inbound.move_kind in ('message','reply')
      order by inbound_source.created_at desc,inbound.source_id desc limit 1
      for update of action,inbound,inbound_source
    `;
    requestedSupersedesSourceId = pendingCalendarTurn?.source_id ?? null;
  }
  if (requestedSupersedesSourceId === sourceId) {
    throw new FlorenceStoreConflict("An inbound message cannot supersede itself");
  }
  const stop = isClearMessagesOptOut(input.text);
  if (channel.stopped_at && !stop) {
    return {
      disposition: "stopped",
      sourceId,
      householdId: channel.household_id,
      channelId: channel.id,
    };
  }
  if (input.text === null && images.length === 0 && documents.length === 0) {
    throw new FlorenceStoreConflict("An inbound message needs text, an image, or a document");
  }
  const supersession = requestedSupersedesSourceId
    ? await resolveSupersessionTail(sql, channel.id, requestedSupersedesSourceId, sourceId)
    : null;
  const visibility: Visibility = channel.audience === "group" ? "household" : "private";
  const ownerAdultId = visibility === "private" ? senderAdultId : null;
  const [reply] = input.replyToProviderMessageId
    ? await sql<{ source_id: string }[]>`
        select source_id from messages where channel_id=${channel.id}
          and provider_message_id=${input.replyToProviderMessageId} limit 1
      `
    : [];
  await sql`
    insert into sources (id,household_id,kind,visibility,owner_adult_id,external_key,label,metadata,occurred_at)
    values (${sourceId},${channel.household_id},'linq_message',${visibility},${ownerAdultId},
      ${`inbound:${input.providerEventId}`},${bounded(input.text ?? "Family attachment", 500)},
      ${sql.json({
        providerMessageId: input.providerMessageId,
        ...(supersession ? { supersedesSourceId: supersession.tailSourceId } : {}),
        ...(input.discardSupersededFacts ? { discardSupersededFacts: true } : {}),
      })},${occurredAt})
  `;
  await sql`
    insert into messages (
      source_id,channel_id,direction,sender_adult_id,move_kind,text,images,has_attachments,
      provider_event_id,provider_message_id,reply_to_source_id,turn_id,turn_part,not_before,status,handled_at
    ) values (${sourceId},${channel.id},'inbound',${senderAdultId},'message',${input.text},
      ${sql.json(images)},${images.length > 0 || documents.length > 0},
      ${input.providerEventId},${input.providerMessageId},
      ${reply?.source_id ?? null},${sourceId},0,${occurredAt},${stop ? "handled" : "received"},
      ${stop ? occurredAt : null})
  `;
  for (const document of documents) {
    const discardAfter = instant(document.discardAfter);
    await sql`
      insert into sources (
        id,household_id,kind,visibility,owner_adult_id,external_key,parent_source_id,label,metadata,occurred_at
      ) values (${document.documentId},${channel.household_id},'document',${visibility},${ownerAdultId},
        ${document.externalKey},${sourceId},${required(document.filename, "Document filename")},
        ${sql.json({ mimeType: document.mimeType })},${occurredAt})
    `;
    await sql`
      insert into documents (
        source_id,saved_by_adult_id,filename,mime_type,content_digest,retained,content_envelope,discard_after
      ) values (${document.documentId},${senderAdultId},${document.filename},${document.mimeType},
        ${document.contentDigest},false,${Buffer.from(document.contentEnvelope)},${discardAfter})
    `;
  }
  if (supersession) {
    await markInboundSuperseded(
      sql,
      supersession.chainSourceIds,
      supersession.tailSourceId,
      sourceId,
      occurredAt,
    );
  }
  if (stop) await sql`update linq_channels set stopped_at=${occurredAt} where id=${channel.id}`;
  return {
    disposition: stop ? "stopped" : "accepted",
    sourceId,
    householdId: channel.household_id,
    channelId: channel.id,
  };
}

async function insertInboundReaction(
  sql: postgres.TransactionSql,
  channel: ChannelRow,
  senderAdultId: string,
  input: AcceptInboundReactionInput,
): Promise<AcceptInboundResult | null> {
  if (
    input.audience !== channel.audience ||
    !sameStrings(channelIdentityDigests(channel), sortedDigests(input.participantIdentityDigests))
  ) {
    throw new FlorenceStoreUnauthorized("Current Linq participants do not match the bound channel");
  }
  const expectedSenderDigest =
    channel.adult_one_id === senderAdultId ? channel.identity_one_digest : channel.identity_two_digest;
  if (expectedSenderDigest !== input.senderIdentitySubjectDigest) {
    throw new FlorenceStoreUnauthorized("The Linq reaction sender is not a bound household adult");
  }
  const providerEventId = required(input.providerEventId, "Linq reaction event ID");
  const targetProviderMessageId = required(input.targetProviderMessageId, "Linq reaction target message ID");
  const reaction = required(input.reaction, "Linq reaction");
  if (reaction.length > 500) throw new FlorenceStoreConflict("A Linq reaction is too long");
  if (!Number.isSafeInteger(input.partIndex) || input.partIndex < 0) {
    throw new FlorenceStoreConflict("A Linq reaction part index is invalid");
  }
  const occurredAt = instant(input.occurredAt);
  const sourceId = deterministicUuid(`linq-v3\0signal\0${providerEventId}`);
  const providerMessageId = `inbound-reaction:${deterministicUuid(
    `linq-v3\0reaction-message\0${providerEventId}`,
  )}`;
  const [existing] = await sql<
    {
      source_id: string;
      channel_id: string;
      sender_adult_id: string;
      move_kind: "message" | "reply" | "reaction";
      text: string | null;
      reaction: string | null;
      provider_message_id: string;
      reply_to_source_id: string | null;
      turn_id: string;
      turn_part: number;
      metadata: JsonObject;
      occurred_at: Date;
    }[]
  >`
    select m.source_id,m.channel_id,m.sender_adult_id,m.move_kind,m.text,m.reaction,
      m.provider_message_id,m.reply_to_source_id,m.turn_id,m.turn_part,s.metadata,s.occurred_at
    from messages m join sources s on s.id=m.source_id
    where m.provider_event_id=${providerEventId} limit 1
  `;
  const [target] = await sql<
    {
      source_id: string;
      direction: "inbound" | "outbound";
      move_kind: "message" | "reply" | "reaction";
      status: "received" | "handled" | "pending" | "sending" | "sent" | "failed";
    }[]
  >`
    select source_id,direction,move_kind,status from messages where channel_id=${channel.id}
      and provider_message_id=${targetProviderMessageId} limit 1 for share
  `;
  const florenceTarget =
    target?.direction === "outbound" && (target.move_kind === "message" || target.move_kind === "reply");
  if (existing) {
    const metadata = jsonRecord(existing.metadata);
    if (
      !target ||
      !florenceTarget ||
      existing.source_id !== sourceId ||
      existing.channel_id !== channel.id ||
      existing.sender_adult_id !== senderAdultId ||
      existing.move_kind !== "reaction" ||
      existing.text !== null ||
      existing.reaction !== reaction ||
      existing.provider_message_id !== providerMessageId ||
      existing.reply_to_source_id !== target.source_id ||
      existing.turn_id !== sourceId ||
      existing.turn_part !== -1 ||
      existing.occurred_at.getTime() !== occurredAt.getTime() ||
      metadata.targetProviderMessageId !== targetProviderMessageId ||
      metadata.partIndex !== input.partIndex
    ) {
      throw new FlorenceStoreConflict("A Linq reaction event ID was reused with different content");
    }
    return {
      disposition: "duplicate",
      sourceId,
      householdId: channel.household_id,
      channelId: channel.id,
    };
  }
  if (channel.stopped_at) {
    return {
      disposition: "stopped",
      sourceId,
      householdId: channel.household_id,
      channelId: channel.id,
    };
  }
  if (!target || !florenceTarget || target.status !== "sent") return null;
  const visibility: Visibility = channel.audience === "group" ? "household" : "private";
  const ownerAdultId = visibility === "private" ? senderAdultId : null;
  await sql`
    insert into sources (id,household_id,kind,visibility,owner_adult_id,external_key,label,metadata,occurred_at)
    values (${sourceId},${channel.household_id},'linq_message',${visibility},${ownerAdultId},
      ${`inbound:${providerEventId}`},${bounded(`Reacted ${reaction}`, 500)},
      ${sql.json({ providerMessageId, targetProviderMessageId, partIndex: input.partIndex })},${occurredAt})
  `;
  await sql`
    insert into messages (
      source_id,channel_id,direction,sender_adult_id,move_kind,text,reaction,images,has_attachments,
      provider_event_id,provider_message_id,reply_to_source_id,turn_id,turn_part,not_before,status
    ) values (${sourceId},${channel.id},'inbound',${senderAdultId},'reaction',null,${reaction},'[]'::jsonb,
      false,${providerEventId},${providerMessageId},${target.source_id},${sourceId},-1,${occurredAt},'received')
  `;
  return {
    disposition: "accepted",
    sourceId,
    householdId: channel.household_id,
    channelId: channel.id,
  };
}

export function isClearMessagesOptOut(text: string | null | undefined): boolean {
  if (!text?.trim()) return false;
  const request = text
    .trim()
    .toLowerCase()
    .replaceAll("’", "'")
    .replace(/[.!?]+$/g, "")
    .replace(/[,;:]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/^florence\s+/, "")
    .replace(/^(?:please\s+|(?:can|could|would|will)\s+you(?:\s+please)?\s+)/, "")
    .replace(/\s+please$/, "")
    .trim();

  return [
    /^stop$/,
    /^unsubscribe(?: me)?$/,
    /^opt (?:me )?out$/,
    /^stop (?:all )?(?:messages|texts)$/,
    /^stop (?:messaging|texting|contacting) me(?: (?:now|anymore|again))?$/,
    /^stop sending me (?:any )?(?:more )?(?:messages|texts)(?: (?:now|anymore))?$/,
    /^(?:do not|don'?t) (?:message|text|contact) me(?: (?:anymore|again))?$/,
    /^(?:do not|don'?t) send me (?:any )?(?:more )?(?:messages|texts)$/,
    /^never (?:message|text|contact) me again$/,
    /^no more (?:messages|texts)$/,
    /^i (?:want|need) to (?:unsubscribe|opt out)$/,
    /^i(?:'d| would) like to (?:unsubscribe|opt out)$/,
  ].some((pattern) => pattern.test(request));
}

async function insertOutbound(sql: postgres.TransactionSql, input: OutboundInsert): Promise<void> {
  if (input.moveKind === "reaction") {
    if (input.turnPart !== -1 || !input.reaction || input.text) {
      throw new FlorenceStoreConflict("A reaction uses turn part -1 and contains only a reaction");
    }
  } else if (input.turnPart < 0 || !input.text?.trim()) {
    throw new FlorenceStoreConflict("A message bubble needs text and turn part 0 through 2");
  }
  const [existing] = await sql<{ source_id: string }[]>`
    select source_id from messages where idempotency_key=${input.idempotencyKey}
  `;
  if (existing) {
    if (existing.source_id !== input.sourceId) {
      throw new FlorenceStoreConflict("A Linq idempotency key was reused for another message");
    }
    return;
  }
  await sql`
    insert into sources (
      id,household_id,kind,visibility,owner_adult_id,external_key,parent_source_id,label,metadata,occurred_at
    )
    values (${input.sourceId},${input.householdId},'linq_message',${input.visibility},${input.ownerAdultId},
      ${`outbound:${input.idempotencyKey}`},${input.parentSourceId ?? null},
      ${bounded(input.text ?? input.reaction ?? "Florence response", 500)},'{}'::jsonb,${input.occurredAt})
  `;
  await sql`
    insert into messages (
      source_id,channel_id,direction,move_kind,text,reaction,reply_to_source_id,turn_id,turn_part,
      idempotency_key,not_before,status
    ) values (${input.sourceId},${input.channelId},'outbound',${input.moveKind},${input.text ?? null},
      ${input.reaction ?? null},${input.replyToSourceId ?? null},${input.turnId},${input.turnPart},
      ${input.idempotencyKey},${instant(input.notBefore)},'pending')
  `;
}

function imageReferences(value: JsonValue): ImageReference[] {
  if (!Array.isArray(value)) throw new FlorenceStoreConflict("Stored message images are invalid");
  return validateImageReferences(
    value.map((item) => {
      if (!isRecord(item) || typeof item.assetId !== "string" || typeof item.mimeType !== "string") {
        throw new FlorenceStoreConflict("Stored message images are invalid");
      }
      return { assetId: item.assetId, mimeType: item.mimeType as ImageReference["mimeType"] };
    }),
  );
}

function validateImageReferences(values: readonly ImageReference[]): ImageReference[] {
  if (values.length > 10) throw new FlorenceStoreConflict("A message can contain at most ten images");
  const result: ImageReference[] = [];
  const assetIds = new Set<string>();
  for (const value of values) {
    assertUuid(value.assetId, "Image asset ID");
    if (!isImageMimeType(value.mimeType)) throw new FlorenceStoreConflict("An image MIME type is invalid");
    if (assetIds.has(value.assetId))
      throw new FlorenceStoreConflict("An image cannot appear twice in one message");
    assetIds.add(value.assetId);
    result.push({ assetId: value.assetId, mimeType: value.mimeType });
  }
  return result;
}

type ValidatedInboundDocument = Omit<InboundDocumentInput, "discardAfter"> & {
  discardAfter: string;
};

function validateInboundDocuments(
  values: readonly InboundDocumentInput[],
  occurredAt: Date,
): ValidatedInboundDocument[] {
  if (values.length > MAX_CURRENT_PDFS) {
    throw new FlorenceStoreConflict("A message can contain at most three PDFs");
  }
  const documentIds = new Set<string>();
  const externalKeys = new Set<string>();
  const documents = values.map((document) => {
    assertUuid(document.documentId, "Document ID");
    if (documentIds.has(document.documentId)) {
      throw new FlorenceStoreConflict("A document cannot appear twice in one message");
    }
    documentIds.add(document.documentId);
    const externalKey = required(document.externalKey, "Document provider key");
    if (externalKeys.has(externalKey)) {
      throw new FlorenceStoreConflict("A document provider key cannot appear twice in one message");
    }
    externalKeys.add(externalKey);
    if (!document.filename.trim() || document.filename.length > 500) {
      throw new FlorenceStoreConflict("A document filename is invalid");
    }
    if (document.mimeType !== "application/pdf") {
      throw new FlorenceStoreConflict("An inbound document must be a PDF");
    }
    assertDigest(document.contentDigest, "Document content");
    if (
      document.contentEnvelope.byteLength < 1 ||
      document.contentEnvelope.byteLength > MAX_PDF_ENVELOPE_BYTES
    ) {
      throw new FlorenceStoreConflict("A PDF envelope exceeds its storage limit");
    }
    const discardAfter = instant(
      document.discardAfter ?? new Date(occurredAt.getTime() + 24 * 60 * 60_000).toISOString(),
    );
    if (discardAfter <= occurredAt) {
      throw new FlorenceStoreConflict("An inbound PDF discard time must follow its message");
    }
    return {
      documentId: document.documentId,
      externalKey,
      filename: document.filename,
      mimeType: "application/pdf" as const,
      contentDigest: document.contentDigest,
      contentEnvelope: document.contentEnvelope,
      discardAfter: discardAfter.toISOString(),
    };
  });
  return documents.sort((left, right) => left.documentId.localeCompare(right.documentId));
}

function sameInboundDocuments(
  existing: readonly {
    document_id: string;
    external_key: string;
    filename: string;
    mime_type: string;
    content_digest: string;
    retained: boolean;
    discard_after: Date | null;
  }[],
  received: readonly ValidatedInboundDocument[],
): boolean {
  return (
    existing.length === received.length &&
    existing.every((document, index) => {
      const candidate = received[index];
      return (
        candidate !== undefined &&
        document.document_id === candidate.documentId &&
        document.external_key === candidate.externalKey &&
        document.filename === candidate.filename &&
        document.mime_type === candidate.mimeType &&
        document.content_digest === candidate.contentDigest &&
        document.retained === false &&
        document.discard_after?.toISOString() === candidate.discardAfter
      );
    })
  );
}

type LinqProviderState =
  | "accepted"
  | "sent"
  | "delivered"
  | "read"
  | "failed"
  | "reaction_added"
  | "reaction_removed";

type LinqProviderTruth = {
  state: LinqProviderState;
  occurredAt: string;
  receipt: Record<string, JsonValue>;
};

const LINQ_OBSERVATION_SLOTS = [
  "messageSent",
  "messageDelivered",
  "messageRead",
  "messageFailed",
  "reactionAdded",
  "reactionRemoved",
] as const;

function mergeLinqAcceptance(
  current: JsonValue | null,
  providerReceiptId: string,
  occurredAt: string,
  detail: JsonObject,
): JsonObject {
  const existing = jsonRecord(current);
  const requestedState = linqProviderState(detail.providerState) ?? "accepted";
  const existingTruth = latestLinqTruth(existing);
  const incomingTruth =
    requestedState === "accepted" ? null : { state: requestedState, occurredAt, receipt: jsonRecord(detail) };
  const truth = newestLinqTruth(existingTruth, incomingTruth);
  return {
    ...existing,
    ...detail,
    provider: "linq-v3",
    acceptance: { providerReceiptId, occurredAt },
    providerState: truth?.state ?? requestedState,
    providerStateAt: truth?.occurredAt ?? occurredAt,
  };
}

function mergeLinqObservation(
  current: JsonValue | null,
  input: LinqOutboundObservation,
): {
  detail: JsonObject;
  providerState: LinqProviderState;
  duplicate: boolean;
  lastError: string | null;
} {
  const existing = jsonRecord(current);
  const observations = jsonRecord(existing.observations);
  const slot =
    input.kind === "reaction"
      ? input.operation === "added"
        ? "reactionAdded"
        : "reactionRemoved"
      : input.status === "sent"
        ? "messageSent"
        : input.status === "delivered"
          ? "messageDelivered"
          : input.status === "read"
            ? "messageRead"
            : "messageFailed";
  const state: LinqProviderState =
    input.kind === "reaction"
      ? input.operation === "added"
        ? "reaction_added"
        : "reaction_removed"
      : input.status;
  const prior = jsonRecord(observations[slot]);
  const priorEventId = prior.providerEventId;
  const priorOccurredAt = prior.occurredAt;
  const duplicate =
    priorEventId === input.providerEventId ||
    (typeof priorOccurredAt === "string" && Date.parse(priorOccurredAt) >= Date.parse(input.occurredAt));
  if (!duplicate) {
    observations[slot] =
      input.kind === "reaction"
        ? {
            providerEventId: input.providerEventId,
            traceId: input.traceId,
            occurredAt: input.occurredAt,
            state,
            targetProviderMessageId: input.targetProviderMessageId,
            reaction: input.reaction,
            partIndex: input.partIndex,
          }
        : {
            providerEventId: input.providerEventId,
            traceId: input.traceId,
            occurredAt: input.occurredAt,
            state,
            providerMessageId: input.providerMessageId,
            idempotencyKey: input.idempotencyKey,
            ...(input.failure ? { failure: input.failure } : {}),
          };
  }
  const detail: JsonObject = { ...existing, provider: "linq-v3", observations };
  const truth = latestLinqTruth(detail);
  if (!truth || truth.state === "accepted") {
    throw new FlorenceStoreConflict("The Linq observation has no provider delivery state");
  }
  const merged: JsonObject = {
    ...detail,
    providerState: truth.state,
    providerStateAt: truth.occurredAt,
  };
  return {
    detail: merged,
    providerState: truth.state,
    duplicate,
    lastError: linqTruthError(truth),
  };
}

function latestLinqTruth(detail: Record<string, JsonValue>): LinqProviderTruth | null {
  const state = linqProviderState(detail.providerState);
  const occurredAt = detail.providerStateAt;
  let latest =
    state && typeof occurredAt === "string" && !Number.isNaN(Date.parse(occurredAt))
      ? { state, occurredAt, receipt: detail }
      : null;
  const observations = jsonRecord(detail.observations);
  for (const slot of LINQ_OBSERVATION_SLOTS) {
    const receipt = jsonRecord(observations[slot]);
    const observedState = linqProviderState(receipt.state);
    const observedAt = receipt.occurredAt;
    if (!observedState || typeof observedAt !== "string" || Number.isNaN(Date.parse(observedAt))) {
      continue;
    }
    latest = newestLinqTruth(latest, { state: observedState, occurredAt: observedAt, receipt });
  }
  return latest;
}

function newestLinqTruth(
  left: LinqProviderTruth | null,
  right: LinqProviderTruth | null,
): LinqProviderTruth | null {
  if (!left) return right;
  if (!right) return left;
  const timeDifference = Date.parse(right.occurredAt) - Date.parse(left.occurredAt);
  if (timeDifference !== 0) return timeDifference > 0 ? right : left;
  return linqStatePriority(right.state) >= linqStatePriority(left.state) ? right : left;
}

function linqProviderState(value: JsonValue | undefined): LinqProviderState | null {
  return value === "accepted" ||
    value === "sent" ||
    value === "delivered" ||
    value === "read" ||
    value === "failed" ||
    value === "reaction_added" ||
    value === "reaction_removed"
    ? value
    : null;
}

function linqStatePriority(state: LinqProviderState): number {
  if (state === "read") return 7;
  if (state === "delivered" || state === "reaction_removed") return 6;
  if (state === "sent" || state === "reaction_added") return 5;
  if (state === "failed") return 4;
  return 0;
}

function linqTruthError(truth: LinqProviderTruth): string | null {
  if (truth.state === "reaction_removed") return "Linq reaction was removed";
  if (truth.state !== "failed") return null;
  const failure = jsonRecord(truth.receipt.failure);
  const reason = failure.reason;
  const code = failure.code;
  if (typeof reason === "string" && reason.trim()) return bounded(reason, 2_000);
  return typeof code === "number" ? `Linq delivery failed (${code})` : "Linq delivery failed";
}

function jsonRecord(value: JsonValue | null | undefined): Record<string, JsonValue> {
  return value !== undefined && isRecord(value) ? { ...value } : {};
}

function pdfMimeType(value: string): "application/pdf" {
  if (value !== "application/pdf") throw new FlorenceStoreConflict("Stored PDF type is invalid");
  return value;
}

function sameImageReferences(left: readonly ImageReference[], right: readonly ImageReference[]): boolean {
  return (
    left.length === right.length &&
    left.every(
      (image, index) => image.assetId === right[index]?.assetId && image.mimeType === right[index]?.mimeType,
    )
  );
}

function isImageMimeType(value: string): value is ImageReference["mimeType"] {
  return ["image/jpeg", "image/png", "image/webp", "image/heic"].includes(value);
}

function calendarEvent(value: JsonValue): CalendarEventDraft {
  if (!isRecord(value)) throw new FlorenceStoreConflict("A Calendar action has invalid event data");
  const title = stringField(value, "title");
  const startsAt = stringField(value, "startsAt");
  const endsAt = stringField(value, "endsAt");
  const timeZone = stringField(value, "timeZone");
  const locationValue = value.location;
  if (locationValue !== null && typeof locationValue !== "string") {
    throw new FlorenceStoreConflict("A Calendar location must be text or null");
  }
  const event = { title, startsAt, endsAt, timeZone, location: locationValue };
  validateCalendarEvent(event);
  return event;
}

function validateCalendarEvent(event: CalendarEventDraft): void {
  required(event.title, "Calendar title");
  required(event.timeZone, "Calendar time zone");
  if (event.location !== null) required(event.location, "Calendar location");
  if (explicitInstant(event.endsAt) <= explicitInstant(event.startsAt)) {
    throw new FlorenceStoreConflict("A Calendar event must end after it starts");
  }
}

function isRecord(value: JsonValue): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringField(value: Record<string, JsonValue>, key: string): string {
  const field = value[key];
  if (typeof field !== "string" || !field.trim()) {
    throw new FlorenceStoreConflict(`Calendar ${key} is required`);
  }
  return field;
}

function instant(value: string): Date {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) throw new FlorenceStoreConflict("A timestamp is invalid");
  return date;
}

function explicitInstant(value: string): Date {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)) {
    throw new FlorenceStoreConflict("A Calendar timestamp must include Z or a UTC offset");
  }
  return instant(value);
}

function required(value: string, name: string): string {
  const trimmed = value.trim();
  if (!trimmed) throw new FlorenceStoreConflict(`${name} is required`);
  return trimmed;
}

function bounded(value: string, limit: number): string {
  return value.length <= limit ? value : value.slice(0, limit);
}

function assertDigest(value: string, name: string): void {
  if (!/^[0-9a-f]{64}$/.test(value)) throw new FlorenceStoreConflict(`${name} digest is invalid`);
}

function assertUuid(value: string, name: string): void {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) {
    throw new FlorenceStoreConflict(`${name} is invalid`);
  }
}

function sortedDigests(values: readonly string[]): string[] {
  const result = [...values];
  for (const value of result) assertDigest(value, "Linq participant identity");
  return result.sort();
}

function digestStrings(values: readonly string[]): string {
  return createHash("sha256")
    .update(JSON.stringify([...values].sort()))
    .digest("hex");
}

function deterministicUuid(value: string): string {
  const digest = createHash("sha256").update(value).digest("hex");
  const variant = ((Number.parseInt(digest[16] ?? "0", 16) & 3) | 8).toString(16);
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-5${digest.slice(13, 16)}-${variant}${digest.slice(17, 20)}-${digest.slice(20, 32)}`;
}

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function unique<T>(values: readonly T[]): T[] {
  return [...new Set(values)];
}

function requiredHousehold(value: HouseholdRecord | null): HouseholdRecord {
  if (!value) throw new Error("The household was not found after its committed change");
  return value;
}
