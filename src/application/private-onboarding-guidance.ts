import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { Database } from "../db/client.js";
import {
  type FamilyOnboardingProjection,
  type FamilyOnboardingStepKind,
  PostgresFamilyOnboarding,
} from "../modules/relationships/index.js";
import { canonicalDigest } from "../shared/canonical-json.js";
import type { SecretBox } from "../shared/crypto.js";
import { StaleAuthorityError } from "../shared/errors.js";

type Transaction = TransactionSql<Record<string, never>>;
type Executor = Database | Transaction;

const GuidanceInputSchema = z.strictObject({
  personId: z.string().uuid(),
  expectedPersonControlEpoch: z.number().int().positive(),
});

export type PrivateOnboardingGuidanceStep = FamilyOnboardingStepKind;

export interface PrivateOnboardingGuidance {
  readonly stateDigest: string;
  readonly currentWork: "onboarding_incomplete" | null;
  readonly householdCount: number;
  readonly household: {
    readonly id: string;
    readonly controlEpoch: number;
  } | null;
  readonly recommendedNextStep: {
    readonly kind: PrivateOnboardingGuidanceStep;
    readonly action: "onboarding_handoff" | "none";
    readonly returnPath: "/onboarding" | null;
  };
}

export interface PrivateOnboardingGuidanceProvider {
  projectPrivateGuidance(input: {
    readonly personId: string;
    readonly expectedPersonControlEpoch: number;
  }): Promise<PrivateOnboardingGuidance>;
}

/**
 * Provides conversational guidance from the same canonical projection that
 * drives the onboarding wizard. The model may phrase this one app-selected
 * step, but it cannot reorder setup or create a different action destination.
 */
export class PostgresPrivateOnboardingGuidance implements PrivateOnboardingGuidanceProvider {
  public constructor(
    private readonly database: Executor,
    private readonly secretBox: SecretBox,
  ) {}

  public async projectPrivateGuidance(
    inputCandidate: z.input<typeof GuidanceInputSchema>,
  ): Promise<PrivateOnboardingGuidance> {
    const input = GuidanceInputSchema.parse(inputCandidate);
    return inTransaction(this.database, async (transaction) => {
      const people = await transaction<{ readonly control_epoch: number | string }[]>`
        select control_epoch from people
        where id = ${input.personId} and status = 'registered'
          and control_epoch = ${input.expectedPersonControlEpoch}
        for share
      `;
      if (!people[0]) throw new StaleAuthorityError("Private guidance person authority changed");

      const projection = await new PostgresFamilyOnboarding(this.secretBox).project(transaction, {
        actorPersonId: input.personId,
        personId: input.personId,
      });
      let householdControlEpoch: number | null = null;
      if (projection.household) {
        const households = await transaction<{ readonly control_epoch: number | string }[]>`
          select control_epoch from households
          where id = ${projection.household.householdId}
            and status in ('onboarding', 'active', 'paused')
          for share
        `;
        const household = households[0];
        if (!household) throw new StaleAuthorityError("Private guidance household authority changed");
        householdControlEpoch = Number(household.control_epoch);
      }
      return projectGuidance({
        personControlEpoch: Number(people[0].control_epoch),
        projection,
        householdControlEpoch,
      });
    });
  }
}

export function projectGuidance(input: {
  readonly personControlEpoch: number;
  readonly projection: FamilyOnboardingProjection;
  readonly householdControlEpoch: number | null;
}): PrivateOnboardingGuidance {
  const step = input.projection.nextStep.kind;
  const complete = step === "complete";
  const household = input.projection.household;
  if ((household === null) !== (input.householdControlEpoch === null)) {
    throw new StaleAuthorityError("Private guidance household projection is incomplete");
  }
  const digestState = {
    personControlEpoch: input.personControlEpoch,
    profile: {
      authorityVersion: input.projection.profile.authorityVersion,
      controlEpoch: input.projection.profile.controlEpoch,
      reviewVersion: input.projection.profile.reviewVersion,
      onboardingVersion: input.projection.profile.onboardingVersion,
      selectedHouseholdId: input.projection.profile.selectedHouseholdId,
    },
    householdChoices: input.projection.householdChoices.map((choice) => ({
      householdId: choice.householdId,
      membershipId: choice.membershipId,
      role: choice.role,
    })),
    household: household
      ? {
          householdId: household.householdId,
          controlEpoch: input.householdControlEpoch,
          membershipId: household.membershipId,
          membershipVersion: household.membershipVersion,
          intakeVersion: household.intakeVersion,
          membershipOnboardingVersion: household.membershipOnboardingVersion,
          coordinatorDisposition: household.coordinatorDisposition,
          coordinatorInvitationResolved: household.coordinatorInvitationResolved,
          coordinatorInviteDeferred: household.coordinatorInviteDeferred,
          childRosterReviewed: household.childRosterReviewed,
          childRosterReviewedByPersonId: household.childRosterReviewedByPersonId,
          sharedContextReviewed: household.sharedContextReviewed,
          googleDecision: household.googleDecision,
          completed: household.completed,
        }
      : null,
    nextStep: input.projection.nextStep,
  };
  return {
    stateDigest: canonicalDigest(digestState),
    currentWork: complete ? null : "onboarding_incomplete",
    householdCount: input.projection.householdChoices.length,
    household:
      household && input.householdControlEpoch !== null
        ? { id: household.householdId, controlEpoch: input.householdControlEpoch }
        : null,
    recommendedNextStep: {
      kind: step,
      action: complete ? "none" : "onboarding_handoff",
      returnPath: complete ? null : "/onboarding",
    },
  };
}

function inTransaction<Result>(
  executor: Executor,
  operation: (transaction: Transaction) => Promise<Result>,
): Promise<Result> {
  return "begin" in executor
    ? (executor.begin("isolation level repeatable read", operation) as unknown as Promise<Result>)
    : operation(executor);
}
