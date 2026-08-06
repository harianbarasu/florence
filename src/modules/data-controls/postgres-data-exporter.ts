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
    const identities = await this.database`
      select kind, issuer, subject_digest, status, observed_at, verified_at, revoked_at
      from person_identities where person_id = ${personId} order by created_at
    `;
    const memberships = await this.database<
      { household_id: string; role: string; status: string; joined_at: Date | null; ended_at: Date | null }[]
    >`
      select household_id, role, status, joined_at, ended_at
      from household_memberships where person_id = ${personId} order by created_at
    `;
    const householdIds = memberships.map((entry) => entry.household_id);
    const relationships =
      householdIds.length === 0
        ? []
        : await this.database<
            {
              household_id: string;
              person_id: string;
              role: string;
              status: string;
              display_name_ciphertext: Buffer | null;
            }[]
          >`
          select membership.household_id, membership.person_id, membership.role, membership.status,
            person.display_name_ciphertext
          from household_memberships membership
          join people person on person.id = membership.person_id
          where membership.household_id = any(${this.database.array(householdIds)}::uuid[])
          order by membership.household_id, membership.created_at
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
      { id: string; provider: string; status: string; connected_at: Date; updated_at: Date }[]
    >`
      select id, provider, status, connected_at, updated_at
      from integrations where person_id = ${personId} order by connected_at
    `;
    const sources = new PostgresSourceIntelligence(this.database, this.secretBox, {
      rawRetentionDays: this.rawRetentionDays,
      privateCandidateRetentionDays: 7,
    });
    const integrations = [] as unknown[];
    for (const integration of integrationRows) {
      let accountEmail: string | null = null;
      if (integration.status !== "revoked") {
        try {
          const profile = await sources.read({
            kind: "integration_profile",
            integrationId: integration.id,
            personId,
            expectedControlEpoch: await integrationControlEpoch(this.database, integration.id),
          });
          accountEmail = profile.kind === "integration_profile" ? profile.accountEmail : null;
        } catch {
          accountEmail = null;
        }
      }
      integrations.push({ ...integration, accountEmail });
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
    const loopRows =
      householdIds.length === 0
        ? []
        : await this.database<{ id: string }[]>`
          select id from coverage_loops where household_id = any(${this.database.array(householdIds)}::uuid[])
          order by created_at
        `;
    const coordination = new PostgresCoordination(this.database, this.secretBox);
    const coverageLoops = [] as unknown[];
    for (const row of loopRows) {
      const loop = await coordination.load(row.id);
      if (loop) coverageLoops.push(loop);
    }
    const sessions = await this.database`
      select id, assurance_kind, created_at, last_seen_at, idle_expires_at,
        absolute_expires_at, revoked_at
      from person_sessions where person_id = ${personId} order by created_at
    `;
    const audit = await this.database`
      select id, household_id, conversation_id, sequence, actor_kind, event_type,
        target_type, target_id, reason_codes, evidence_refs, occurred_at
      from audit_events
      where person_id = ${personId}
        or (${this.database.array(householdIds)}::uuid[] <> '{}'::uuid[] and household_id = any(${this.database.array(householdIds)}::uuid[]))
      order by occurred_at limit 20000
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

async function integrationControlEpoch(database: Database, integrationId: string): Promise<number> {
  const rows = await database<{ control_epoch: number | string }[]>`
    select control_epoch from integrations where id = ${integrationId}
  `;
  return Number(rows[0]?.control_epoch ?? 0);
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
