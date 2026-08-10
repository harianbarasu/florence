import type { Database } from "../../db/client.js";
import type { SecretBox } from "../../shared/crypto.js";
import { NotFoundError } from "../../shared/errors.js";
import { PostgresCoordination } from "../coordination/index.js";
import { PostgresSourceIntelligence } from "../sources/index.js";

/** Produces a scoped, inspectable portability export without exposing credentials or blob bytes. */
export class PostgresDataExporter {
  public constructor(
    private readonly database: Database,
    private readonly secretBox: SecretBox,
    private readonly rawRetentionDays: number,
  ) {}

  public async exportPerson(personId: string): Promise<unknown> {
    const people = await this.database<
      {
        id: string;
        status: string;
        timezone: string | null;
        quiet_hours: unknown;
        consented_at: Date | null;
        registered_at: Date | null;
        created_at: Date;
        display_name_ciphertext: Buffer | null;
      }[]
    >`
      select id, status, timezone, quiet_hours, consented_at, registered_at, created_at,
        display_name_ciphertext
      from people where id = ${personId} and status in ('registered', 'stopped')
    `;
    const person = people[0];
    if (!person) throw new NotFoundError("Florence person is unavailable for export");
    const identityRows = await this.database<
      {
        id: string;
        kind: string;
        issuer: string;
        subject_digest: string;
        subject_ciphertext: Buffer | null;
        display_label_ciphertext: Buffer | null;
        status: string;
        observed_at: Date;
        verified_at: Date | null;
        revoked_at: Date | null;
      }[]
    >`
      select id, kind, issuer, subject_digest, subject_ciphertext,
        display_label_ciphertext, status, observed_at, verified_at, revoked_at
      from person_identities where person_id = ${personId} order by created_at
    `;
    const identities = identityRows.map((identity) => ({
      id: identity.id,
      kind: identity.kind,
      issuer: identity.issuer,
      value: decryptIdentityValue(this.secretBox, identity.id, "subject", identity.subject_ciphertext),
      label: decryptIdentityValue(
        this.secretBox,
        identity.id,
        "display-label",
        identity.display_label_ciphertext,
      ),
      subjectDigest: identity.subject_digest,
      status: identity.status,
      observedAt: identity.observed_at,
      verifiedAt: identity.verified_at,
      revokedAt: identity.revoked_at,
    }));
    const memberships = await this.database<
      { household_id: string; role: string; status: string; joined_at: Date | null; ended_at: Date | null }[]
    >`
      select household_id, role, status, joined_at, ended_at
      from household_memberships where person_id = ${personId} order by created_at
    `;
    const relationships = await this.database<
      {
        household_id: string;
        person_id: string;
        role: string;
        status: string;
        display_name_ciphertext: Buffer | null;
      }[]
    >`
      select member.household_id, member.person_id, member.role, member.status,
        person.display_name_ciphertext
      from household_memberships viewer_membership
      join membership_capabilities read_capability
        on read_capability.membership_id = viewer_membership.id
        and read_capability.capability = 'household.read'
        and read_capability.status = 'active'
        and read_capability.scope = '{}'::jsonb
      join households household on household.id = viewer_membership.household_id
        and household.status in ('onboarding', 'active', 'paused')
      join household_memberships member on member.household_id = household.id
        and member.status = 'active'
      join people person on person.id = member.person_id
      where viewer_membership.person_id = ${personId}
        and viewer_membership.status = 'active'
        and (
          member.role <> 'dependent'
          or viewer_membership.role = 'steward'
          or exists (
            select 1
            from household_onboarding_intakes intake
            join membership_onboarding viewer_context
              on viewer_context.membership_id = viewer_membership.id
              and viewer_context.shared_context_household_intake_version = intake.version
            where intake.household_id = household.id
              and intake.child_roster_reviewed_at is not null
          )
        )
      order by member.household_id, member.created_at
    `;
    const conversations = await this.database`
      select conversation.id, conversation.kind, conversation.status, conversation.purpose,
        conversation.household_id, conversation.current_epoch_id, conversation.created_at,
        conversation.updated_at
      from conversations conversation
      where exists(
        select 1 from epoch_participants participant
        where participant.participant_epoch_id = conversation.current_epoch_id
          and participant.person_id = ${personId}
      )
      order by conversation.created_at
    `;
    const integrationRows = await this.database<
      {
        id: string;
        provider: string;
        account_kind: string;
        status: string;
        last_authorized_capabilities: readonly string[];
        control_epoch: number | string;
        connected_at: Date;
        updated_at: Date;
      }[]
    >`
      select id, provider, account_kind, status, last_authorized_capabilities,
        control_epoch, connected_at, updated_at
      from integrations where person_id = ${personId} order by connected_at
    `;
    const integrationIds = integrationRows.map((integration) => integration.id);
    const capabilityRows =
      integrationIds.length === 0
        ? []
        : await this.database<
            {
              integration_id: string;
              capability: string;
              status: string;
              granted_at: Date;
              revoked_at: Date | null;
            }[]
          >`
          select integration_id, capability, status, granted_at, revoked_at
          from integration_capabilities
          where integration_id = any(${this.database.array(integrationIds)}::uuid[])
          order by integration_id, capability
        `;
    const grantRows =
      integrationIds.length === 0
        ? []
        : await this.database<
            {
              id: string;
              integration_id: string;
              grant_kind: string;
              scope: unknown;
              destination_household_id: string | null;
              destination_conversation_id: string | null;
              status: string;
              version: number | string;
              expires_at: Date | null;
              revoked_at: Date | null;
              created_at: Date;
            }[]
          >`
          select id, integration_id, grant_kind, scope, destination_household_id,
            destination_conversation_id, status, version, expires_at, revoked_at, created_at
          from integration_grants
          where integration_id = any(${this.database.array(integrationIds)}::uuid[])
          order by integration_id, created_at, id
        `;
    const sources = new PostgresSourceIntelligence(this.database, this.secretBox, {
      rawRetentionDays: this.rawRetentionDays,
      privateCandidateRetentionDays: 7,
    });
    const integrations = [] as unknown[];
    for (const integration of integrationRows) {
      let accountEmail: string | null = null;
      try {
        const profile = await sources.read({
          kind: "integration_profile",
          integrationId: integration.id,
          personId,
          expectedControlEpoch: Number(integration.control_epoch),
        });
        accountEmail = profile.kind === "integration_profile" ? profile.accountEmail : null;
      } catch {
        accountEmail = null;
      }
      integrations.push({
        id: integration.id,
        provider: integration.provider,
        accountKind: integration.account_kind,
        status: integration.status,
        accountEmail,
        lastAuthorizedCapabilities: integration.last_authorized_capabilities,
        connectedAt: integration.connected_at,
        updatedAt: integration.updated_at,
        capabilities: capabilityRows
          .filter((capability) => capability.integration_id === integration.id)
          .map((capability) => ({
            capability: capability.capability,
            status: capability.status,
            grantedAt: capability.granted_at,
            revokedAt: capability.revoked_at,
          })),
        grants: grantRows
          .filter((grant) => grant.integration_id === integration.id)
          .map((grant) => ({
            id: grant.id,
            kind: grant.grant_kind,
            scope: grant.scope,
            destinationHouseholdId: grant.destination_household_id,
            destinationConversationId: grant.destination_conversation_id,
            status: grant.status,
            version: Number(grant.version),
            expiresAt: grant.expires_at,
            revokedAt: grant.revoked_at,
            createdAt: grant.created_at,
          })),
      });
    }
    const revisionRows = await this.database<{ id: string }[]>`
      select id from source_revisions
      where owner_person_id = ${personId} and revoked_at is null and retention_until > now()
      order by occurred_at desc limit 10000
    `;
    const sourceRevisions = [] as unknown[];
    for (const revision of revisionRows) {
      try {
        const result = await sources.read({
          kind: "source_revision",
          sourceRevisionId: revision.id,
          scope: { kind: "person", personId },
          asOf: new Date().toISOString(),
        });
        if (result.kind === "source_revision") sourceRevisions.push(result);
      } catch {
        // A concurrently expired or revoked source is deliberately omitted.
      }
    }
    const loopRows = await this.database<{ id: string }[]>`
      select loop.id
      from household_memberships viewer_membership
      join membership_capabilities read_capability
        on read_capability.membership_id = viewer_membership.id
        and read_capability.capability = 'household.read'
        and read_capability.status = 'active'
        and read_capability.scope = '{}'::jsonb
      join households household on household.id = viewer_membership.household_id
        and household.status = 'active'
      join coverage_loops loop on loop.household_id = household.id
      join conversations destination on destination.id = loop.destination_conversation_id
        and destination.household_id = household.id and destination.status = 'active'
      join participant_epochs epoch on epoch.id = loop.participant_epoch_id
        and destination.current_epoch_id = epoch.id and epoch.ended_at is null
        and epoch.participant_set_digest = loop.participant_set_digest
      join epoch_participants viewer_participant
        on viewer_participant.participant_epoch_id = epoch.id
        and viewer_participant.person_id = ${personId}
        and viewer_participant.registration_status = 'registered'
        and viewer_participant.consented_at is not null
      join participant_policies viewer_policy on viewer_policy.conversation_id = destination.id
        and viewer_policy.person_id = ${personId}
        and viewer_policy.status = 'active' and viewer_policy.allow_content_processing
      where viewer_membership.person_id = ${personId}
        and viewer_membership.status = 'active'
        and family_membership_onboarding_is_current(viewer_membership.id)
      order by loop.created_at
    `;
    const coordination = new PostgresCoordination(this.database, this.secretBox);
    const coverageLoops = [] as unknown[];
    for (const row of loopRows) {
      const loop = await coordination.load(row.id);
      if (loop) coverageLoops.push(loop);
    }
    const memoryRows = await this.database<
      {
        id: string;
        scope_kind: string;
        owner_person_id: string | null;
        household_id: string | null;
        conversation_id: string | null;
        memory_key: string;
        version: number | string;
        expires_at: Date | null;
        created_at: Date;
        updated_at: Date;
        revision_id: string;
        revision: number | string;
        content_digest: string;
        content_ciphertext: Buffer;
        scope_digest: string;
        evidence_refs: unknown;
        accepted_by_person_id: string | null;
        effective_at: Date;
      }[]
    >`
      select memory.id, memory.scope_kind, memory.owner_person_id, memory.household_id,
        memory.conversation_id, memory.memory_key, memory.version, memory.expires_at,
        memory.created_at, memory.updated_at, revision.id as revision_id,
        revision.revision, revision.content_digest, revision.content_ciphertext,
        revision.scope_digest, revision.evidence_refs, revision.accepted_by_person_id,
        revision.effective_at
      from memory_records memory
      join memory_revisions revision on revision.id = memory.current_revision_id
      where memory.status = 'accepted'
        and (
          (memory.scope_kind = 'person' and memory.owner_person_id = ${personId})
          or (
            memory.scope_kind = 'household'
            and exists (
              select 1
              from household_memberships viewer_membership
              join membership_capabilities read_capability
                on read_capability.membership_id = viewer_membership.id
                and read_capability.capability = 'household.read'
                and read_capability.status = 'active'
                and read_capability.scope = '{}'::jsonb
              join households household on household.id = viewer_membership.household_id
                and household.status in ('onboarding', 'active', 'paused')
              where viewer_membership.person_id = ${personId}
                and viewer_membership.status = 'active'
                and viewer_membership.household_id = memory.household_id
                and family_membership_onboarding_is_current(viewer_membership.id)
            )
          )
        )
      order by revision.effective_at, memory.id
      limit 10000
    `;
    const memories = memoryRows.map((memory) => ({
      id: memory.id,
      scopeKind: memory.scope_kind,
      ownerPersonId: memory.owner_person_id,
      householdId: memory.household_id,
      conversationId: memory.conversation_id,
      key: memory.memory_key,
      version: Number(memory.version),
      expiresAt: memory.expires_at,
      createdAt: memory.created_at,
      updatedAt: memory.updated_at,
      currentRevision: {
        id: memory.revision_id,
        revision: Number(memory.revision),
        contentDigest: memory.content_digest,
        content: decryptMemoryContent(this.secretBox, memory.revision_id, memory.content_ciphertext),
        scopeDigest: memory.scope_digest,
        evidenceRefs: memory.evidence_refs,
        acceptedByPersonId: memory.accepted_by_person_id,
        effectiveAt: memory.effective_at,
      },
    }));
    const sessions = await this.database`
      select id, assurance_kind, created_at, last_seen_at, idle_expires_at,
        absolute_expires_at, revoked_at
      from person_sessions where person_id = ${personId} order by created_at
    `;
    const audit = await this.database`
      select event.id, event.household_id, event.conversation_id, event.sequence,
        event.actor_kind, event.event_type, event.target_type, event.target_id,
        event.reason_codes, event.evidence_refs, event.occurred_at
      from audit_events event
      where (
        (event.household_id is null and event.person_id = ${personId})
        or exists (
          select 1
          from household_memberships viewer_membership
          join membership_capabilities read_capability
            on read_capability.membership_id = viewer_membership.id
            and read_capability.capability = 'household.read'
            and read_capability.status = 'active'
            and read_capability.scope = '{}'::jsonb
          join households household on household.id = viewer_membership.household_id
            and household.status in ('onboarding', 'active', 'paused')
          where viewer_membership.person_id = ${personId}
            and viewer_membership.status = 'active'
            and viewer_membership.household_id = event.household_id
            and family_membership_onboarding_is_current(viewer_membership.id)
        )
      )
      and (
        event.conversation_id is null
        or exists (
          select 1
          from conversations conversation
          join participant_epochs epoch on epoch.id = conversation.current_epoch_id
            and epoch.ended_at is null
          join epoch_participants viewer_participant
            on viewer_participant.participant_epoch_id = epoch.id
            and viewer_participant.person_id = ${personId}
            and viewer_participant.registration_status = 'registered'
            and viewer_participant.consented_at is not null
          join participant_policies viewer_policy on viewer_policy.conversation_id = conversation.id
            and viewer_policy.person_id = ${personId}
            and viewer_policy.status = 'active' and viewer_policy.allow_content_processing
          where conversation.id = event.conversation_id
            and conversation.status = 'active'
            and (event.household_id is null or conversation.household_id = event.household_id)
        )
      )
      order by event.occurred_at limit 20000
    `;
    return {
      format: "florence-person-export-v1",
      exportedAt: new Date().toISOString(),
      person: {
        id: person.id,
        displayName: decryptDisplayName(this.secretBox, person.id, person.display_name_ciphertext),
        status: person.status,
        timezone: person.timezone,
        quietHours: person.quiet_hours,
        consentedAt: person.consented_at,
        registeredAt: person.registered_at,
        createdAt: person.created_at,
      },
      identities,
      memberships,
      relationships: relationships.map((entry) => ({
        householdId: entry.household_id,
        personId: entry.person_id,
        displayName: decryptDisplayName(this.secretBox, entry.person_id, entry.display_name_ciphertext),
        role: entry.role,
        status: entry.status,
      })),
      conversations,
      integrations,
      sourceRevisions,
      coverageLoops,
      memories,
      sessions,
      audit,
      omitted: [
        "OAuth credentials and security tokens",
        "raw attachment bytes (attachment manifests and extracted text are included)",
        "other people's private sources",
      ],
    };
  }
}

function decryptDisplayName(
  secretBox: SecretBox,
  personId: string,
  ciphertext: Buffer | null,
): string | null {
  if (!ciphertext) return null;
  try {
    return secretBox
      .decrypt(JSON.parse(ciphertext.toString("utf8")), `person-display-name:${personId}`)
      .toString("utf8");
  } catch {
    return null;
  }
}

function decryptIdentityValue(
  secretBox: SecretBox,
  identityId: string,
  kind: "subject" | "display-label",
  ciphertext: Buffer | null,
): string | null {
  if (!ciphertext) return null;
  try {
    return secretBox
      .decrypt(
        JSON.parse(ciphertext.toString("utf8")),
        kind === "subject" ? `identity-subject:${identityId}` : `identity-display-label:${identityId}`,
      )
      .toString("utf8");
  } catch {
    return null;
  }
}

function decryptMemoryContent(secretBox: SecretBox, revisionId: string, ciphertext: Buffer): unknown | null {
  try {
    const plaintext = secretBox
      .decrypt(JSON.parse(ciphertext.toString("utf8")), `florence:memory-revision:${revisionId}:content`)
      .toString("utf8");
    try {
      return JSON.parse(plaintext) as unknown;
    } catch {
      return plaintext;
    }
  } catch {
    return null;
  }
}
