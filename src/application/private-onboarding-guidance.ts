import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../db/client.js";
import { canonicalDigest } from "../shared/canonical-json.js";
import { StaleAuthorityError } from "../shared/errors.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

const GuidanceInputSchema = z.strictObject({
  personId: z.string().uuid(),
  expectedPersonControlEpoch: z.number().int().positive(),
});

export type PrivateOnboardingGuidanceStep =
  | "create_household"
  | "choose_household"
  | "connect_google"
  | "reconnect_google"
  | "add_first_child"
  | "add_first_routine"
  | "wait_for_google"
  | "ready";

export type PrivateOnboardingGuidanceAction =
  | "people_handoff"
  | "google_handoff"
  | "sources_handoff"
  | "none";

export interface PrivateOnboardingGuidance {
  readonly stateDigest: string;
  readonly currentWork: "google_syncing" | "google_current" | "google_attention" | null;
  readonly householdCount: number;
  readonly household: {
    readonly id: string;
    readonly controlEpoch: number;
    readonly dependentCount: number;
    readonly activeRoutineCount: number;
  } | null;
  readonly recommendedNextStep: {
    readonly kind: PrivateOnboardingGuidanceStep;
    readonly action: PrivateOnboardingGuidanceAction;
    readonly returnPath: "/people" | "/sources" | null;
  };
}

export interface PrivateOnboardingGuidanceProvider {
  projectPrivateGuidance(input: {
    readonly personId: string;
    readonly expectedPersonControlEpoch: number;
  }): Promise<PrivateOnboardingGuidance>;
}

interface PersonRow {
  readonly control_epoch: number | string;
  readonly google_activation_suppressed_at: Date | null;
}

interface MembershipRow {
  readonly household_id: string;
  readonly household_control_epoch: number | string;
  readonly membership_version: number | string;
  readonly can_govern: boolean;
  readonly can_coordinate: boolean;
}

interface HouseholdSetupRow {
  readonly dependent_count: number | string;
  readonly active_routine_count: number | string;
}

interface IntegrationRow {
  readonly id: string;
  readonly status: "active" | "paused" | "reauth_required" | "error";
  readonly control_epoch: number | string;
  readonly information_current_control_epoch: number | string | null;
  readonly capabilities: readonly string[];
}

interface GuidanceState {
  readonly personControlEpoch: number;
  readonly googleActivationSuppressed: boolean;
  readonly memberships: readonly {
    readonly householdId: string;
    readonly householdControlEpoch: number;
    readonly membershipVersion: number;
    readonly canGovern: boolean;
    readonly canCoordinate: boolean;
  }[];
  readonly householdSetup: {
    readonly dependentCount: number;
    readonly activeRoutineCount: number;
  } | null;
  readonly integrations: readonly {
    readonly id: string;
    readonly status: IntegrationRow["status"];
    readonly controlEpoch: number;
    readonly informationCurrentControlEpoch: number | null;
    readonly capabilities: readonly string[];
  }[];
}

/**
 * Selects one privacy-fenced next setup step for a registered person. The
 * model may phrase this recommendation, but it never chooses the step or sees
 * names, source content, invitation targets, or handoff tokens.
 */
export class PostgresPrivateOnboardingGuidance implements PrivateOnboardingGuidanceProvider {
  public constructor(private readonly database: Executor) {}

  public async projectPrivateGuidance(
    inputCandidate: z.input<typeof GuidanceInputSchema>,
  ): Promise<PrivateOnboardingGuidance> {
    const input = GuidanceInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const people = await transaction<PersonRow[]>`
        select control_epoch, google_activation_suppressed_at
        from people
        where id = ${input.personId} and status = 'registered'
          and control_epoch = ${input.expectedPersonControlEpoch}
        for share
      `;
      const person = people[0];
      if (!person) throw new StaleAuthorityError("Private guidance person authority changed");

      const membershipRows = await transaction<MembershipRow[]>`
        select membership.household_id, household.control_epoch as household_control_epoch,
          household.membership_version,
          exists(
            select 1 from membership_capabilities capability
            where capability.membership_id = membership.id
              and capability.capability = 'household.govern' and capability.status = 'active'
          ) as can_govern,
          exists(
            select 1 from membership_capabilities capability
            where capability.membership_id = membership.id
              and capability.capability = 'coordination.coordinate' and capability.status = 'active'
          ) as can_coordinate
        from household_memberships membership
        join households household on household.id = membership.household_id
          and household.status in ('onboarding', 'active', 'paused')
        join membership_capabilities read_capability on read_capability.membership_id = membership.id
          and read_capability.capability = 'household.read' and read_capability.status = 'active'
        where membership.person_id = ${input.personId} and membership.status = 'active'
        order by membership.household_id
        for share of membership, household, read_capability
      `;
      const memberships = membershipRows.map((row) => ({
        householdId: row.household_id,
        householdControlEpoch: Number(row.household_control_epoch),
        membershipVersion: Number(row.membership_version),
        canGovern: row.can_govern,
        canCoordinate: row.can_coordinate,
      }));

      let householdSetup: GuidanceState["householdSetup"] = null;
      const onlyMembership = memberships.length === 1 ? memberships[0] : null;
      if (onlyMembership) {
        const householdId = onlyMembership.householdId;
        const setupRows = await transaction<HouseholdSetupRow[]>`
          select
            count(*) filter (
              where member.status = 'active' and member.role = 'dependent'
            ) as dependent_count,
            (
              select count(*) from routines routine
              where routine.household_id = ${householdId} and routine.status = 'active'
            ) as active_routine_count
          from household_memberships member
          where member.household_id = ${householdId}
        `;
        const setup = setupRows[0];
        householdSetup = {
          dependentCount: Number(setup?.dependent_count ?? 0),
          activeRoutineCount: Number(setup?.active_routine_count ?? 0),
        };
      }

      const integrationRows = await transaction<IntegrationRow[]>`
        select integration.id, integration.status, integration.control_epoch,
          integration.information_current_control_epoch,
          array(
            select capability.capability
            from integration_capabilities capability
            where capability.integration_id = integration.id and capability.status = 'active'
            order by capability.capability
          ) as capabilities
        from integrations integration
        where integration.person_id = ${input.personId}
          and integration.provider = 'google'
          and integration.account_kind = 'personal_family'
          and integration.status <> 'revoked'
        order by integration.id
        for share of integration
      `;
      const integrations = integrationRows.map((row) => ({
        id: row.id,
        status: row.status,
        controlEpoch: Number(row.control_epoch),
        informationCurrentControlEpoch:
          row.information_current_control_epoch === null
            ? null
            : Number(row.information_current_control_epoch),
        capabilities: [...row.capabilities],
      }));
      const state: GuidanceState = {
        personControlEpoch: Number(person.control_epoch),
        googleActivationSuppressed: person.google_activation_suppressed_at !== null,
        memberships,
        householdSetup,
        integrations,
      };
      return projectGuidance(state);
    });
  }
}

export function projectGuidance(state: GuidanceState): PrivateOnboardingGuidance {
  const activeGoogle = state.integrations.filter((integration) => integration.status === "active");
  const googleNeedsAttention = state.integrations.some(
    (integration) => integration.status === "reauth_required" || integration.status === "error",
  );
  const googleSyncing = activeGoogle.some(
    (integration) => integration.informationCurrentControlEpoch !== integration.controlEpoch,
  );
  const googleCurrent =
    activeGoogle.length > 0 &&
    activeGoogle.every(
      (integration) => integration.informationCurrentControlEpoch === integration.controlEpoch,
    );
  const currentWork: PrivateOnboardingGuidance["currentWork"] = googleNeedsAttention
    ? "google_attention"
    : googleSyncing
      ? "google_syncing"
      : googleCurrent
        ? "google_current"
        : null;
  const membership = state.memberships.length === 1 ? (state.memberships[0] ?? null) : null;
  const setup = membership ? state.householdSetup : null;
  const recommendedNextStep = chooseNextStep({
    householdCount: state.memberships.length,
    canGovern: membership?.canGovern ?? false,
    canCoordinate: membership?.canCoordinate ?? false,
    dependentCount: setup?.dependentCount ?? null,
    activeRoutineCount: setup?.activeRoutineCount ?? null,
    googleConnectionCount: state.integrations.length,
    googleNeedsAttention,
    googleActivationSuppressed: state.googleActivationSuppressed,
    googleSyncing,
  });
  return {
    stateDigest: canonicalDigest(state),
    currentWork,
    householdCount: state.memberships.length,
    household:
      membership && setup
        ? {
            id: membership.householdId,
            controlEpoch: membership.householdControlEpoch,
            dependentCount: setup.dependentCount,
            activeRoutineCount: setup.activeRoutineCount,
          }
        : null,
    recommendedNextStep,
  };
}

function chooseNextStep(input: {
  readonly householdCount: number;
  readonly canGovern: boolean;
  readonly canCoordinate: boolean;
  readonly dependentCount: number | null;
  readonly activeRoutineCount: number | null;
  readonly googleConnectionCount: number;
  readonly googleNeedsAttention: boolean;
  readonly googleActivationSuppressed: boolean;
  readonly googleSyncing: boolean;
}): PrivateOnboardingGuidance["recommendedNextStep"] {
  if (input.householdCount === 0) return guidanceStep("create_household", "people_handoff", "/people");
  if (input.householdCount > 1) return guidanceStep("choose_household", "none", null);
  if (input.googleNeedsAttention) {
    return guidanceStep("reconnect_google", "google_handoff", "/sources");
  }
  if (input.googleConnectionCount === 0 && !input.googleActivationSuppressed) {
    return guidanceStep("connect_google", "google_handoff", "/sources");
  }
  if (input.canGovern && input.dependentCount === 0) {
    return guidanceStep("add_first_child", "people_handoff", "/people");
  }
  if (input.canCoordinate && input.activeRoutineCount === 0) {
    return guidanceStep("add_first_routine", "sources_handoff", "/sources");
  }
  if (input.googleSyncing) return guidanceStep("wait_for_google", "none", null);
  return guidanceStep("ready", "none", null);
}

function guidanceStep(
  kind: PrivateOnboardingGuidanceStep,
  action: PrivateOnboardingGuidanceAction,
  returnPath: "/people" | "/sources" | null,
): PrivateOnboardingGuidance["recommendedNextStep"] {
  return { kind, action, returnPath };
}

function inTransaction<Result>(
  executor: Executor,
  operation: (transaction: Transaction) => Promise<Result>,
): Promise<Result> {
  return "begin" in executor
    ? (executor.begin("isolation level repeatable read", operation) as unknown as Promise<Result>)
    : operation(executor);
}
