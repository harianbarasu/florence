import { randomUUID } from "node:crypto";
import type { TransactionSql } from "postgres";
import { z } from "zod";
import type { SecretBox } from "../../shared/crypto.js";
import { ConflictError, NotFoundError, UnauthorizedError } from "../../shared/errors.js";

export type FamilyOnboardingTransaction = TransactionSql<Record<string, never>>;

const MAX_HOUSEHOLDS = 8;
const MAX_CHILDREN = 16;
const MAX_ADULT_INTENTS = 16;
const AdultIntentRoleSchema = z.enum(["steward", "caregiver"]);

export type FamilyOnboardingStepKind =
  | "confirm_profile"
  | "create_household"
  | "choose_household"
  | "adults"
  | "children"
  | "review_shared_context"
  | "google"
  | "review"
  | "complete";

export interface FamilyOnboardingHouseholdChoice {
  readonly householdId: string;
  readonly membershipId: string;
  readonly role: "steward" | "caregiver" | "participant";
  readonly timezone: string;
}

export interface FamilyOnboardingChild {
  readonly personId: string;
  readonly displayName: string;
  readonly aliases: readonly string[];
  readonly birthYear: number | null;
  readonly school: string;
  readonly activities: readonly string[];
}

export interface FamilyOnboardingAdultIntent {
  readonly id: string;
  readonly version: number;
  readonly displayName: string;
  readonly role: "steward" | "caregiver";
  readonly matchedPersonId: string | null;
  readonly invitationId: string | null;
  /** Derived from canonical invitations and memberships; never stored on the intent. */
  readonly status: "not_invited" | "invited" | "joined";
}

export interface FamilyOnboardingHousehold {
  readonly householdId: string;
  readonly membershipId: string;
  readonly membershipVersion: number;
  readonly role: "steward" | "caregiver" | "participant";
  readonly timezone: string;
  readonly intakeVersion: number;
  readonly adultRosterReviewed: boolean;
  readonly adultRosterReviewedByPersonId: string | null;
  readonly adults: readonly FamilyOnboardingAdultIntent[];
  readonly childRosterReviewed: boolean;
  readonly childRosterReviewedByPersonId: string | null;
  readonly children: readonly FamilyOnboardingChild[];
  readonly sharedContextReviewed: boolean;
  readonly membershipOnboardingVersion: number;
  readonly googleDecision: "connected" | "limited" | null;
  readonly completed: boolean;
}

export interface FamilyOnboardingPolicyState {
  readonly personId: string;
  readonly profile: {
    readonly displayName: string | null;
    readonly timezone: string | null;
    readonly authorityVersion: number;
    readonly controlEpoch: number;
    readonly reviewVersion: number;
    readonly onboardingVersion: number;
    readonly selectedHouseholdId: string | null;
    readonly remindersSent: number;
    readonly lastRemindedAt: string | null;
    readonly remindersSuppressedAt: string | null;
    readonly lastProgressedAt: string | null;
  };
  readonly householdChoices: readonly FamilyOnboardingHouseholdChoice[];
  readonly selectedHousehold: FamilyOnboardingHousehold | null;
  readonly selectionIsStale: boolean;
}

export type FamilyOnboardingNextStep =
  | { readonly kind: "confirm_profile" }
  | { readonly kind: "create_household" }
  | { readonly kind: "choose_household" }
  | { readonly kind: "adults"; readonly householdId: string; readonly expectedVersion: number }
  | { readonly kind: "children"; readonly householdId: string; readonly expectedVersion: number }
  | {
      readonly kind: "review_shared_context";
      readonly householdId: string;
      readonly expectedVersion: number;
    }
  | { readonly kind: "google"; readonly householdId: string }
  | {
      readonly kind: "review";
      readonly householdId: string;
      readonly expectedIntakeVersion: number;
      readonly expectedMembershipOnboardingVersion: number;
    }
  | { readonly kind: "complete"; readonly householdId: string };

export interface FamilyOnboardingProjection {
  readonly personId: string;
  readonly profile: FamilyOnboardingPolicyState["profile"];
  readonly householdChoices: readonly FamilyOnboardingHouseholdChoice[];
  readonly household: FamilyOnboardingHousehold | null;
  readonly nextStep: FamilyOnboardingNextStep;
}

/**
 * Resolves one legal next step from already-authorized, bounded state. Keeping
 * this policy pure makes every caller share the same ordering and fail-closed
 * household-selection rules.
 */
export function projectFamilyOnboardingStep(state: FamilyOnboardingPolicyState): FamilyOnboardingNextStep {
  if (
    state.profile.reviewVersion === 0 ||
    state.profile.displayName === null ||
    state.profile.timezone === null
  ) {
    return { kind: "confirm_profile" };
  }
  if (state.householdChoices.length === 0) return { kind: "create_household" };
  if (state.selectionIsStale || !state.selectedHousehold) {
    return { kind: "choose_household" };
  }
  const household = state.selectedHousehold;
  if (household.completed) return { kind: "complete", householdId: household.householdId };
  // Supporting adults complete only their private branch. They can review
  // shared context once a steward has finished it, but they never receive
  // onboarding steps that mutate the household's adult or child rosters.
  if (household.role !== "steward") {
    if (
      household.childRosterReviewed &&
      household.childRosterReviewedByPersonId !== state.personId &&
      !household.sharedContextReviewed
    ) {
      return {
        kind: "review_shared_context",
        householdId: household.householdId,
        expectedVersion: household.membershipOnboardingVersion,
      };
    }
    if (household.googleDecision === null) {
      return { kind: "google", householdId: household.householdId };
    }
    return {
      kind: "review",
      householdId: household.householdId,
      expectedIntakeVersion: household.intakeVersion,
      expectedMembershipOnboardingVersion: household.membershipOnboardingVersion,
    };
  }
  if (!household.adultRosterReviewed) {
    return {
      kind: "adults",
      householdId: household.householdId,
      expectedVersion: household.intakeVersion,
    };
  }
  if (!household.childRosterReviewed) {
    return {
      kind: "children",
      householdId: household.householdId,
      expectedVersion: household.intakeVersion,
    };
  }
  if (household.childRosterReviewedByPersonId !== state.personId && !household.sharedContextReviewed) {
    return {
      kind: "review_shared_context",
      householdId: household.householdId,
      expectedVersion: household.membershipOnboardingVersion,
    };
  }
  if (household.googleDecision === null) {
    return { kind: "google", householdId: household.householdId };
  }
  return {
    kind: "review",
    householdId: household.householdId,
    expectedIntakeVersion: household.intakeVersion,
    expectedMembershipOnboardingVersion: household.membershipOnboardingVersion,
  };
}

export interface FamilyOnboarding {
  project(
    transaction: FamilyOnboardingTransaction,
    input: { readonly actorPersonId: string; readonly personId: string },
  ): Promise<FamilyOnboardingProjection>;
  confirmProfile(
    transaction: FamilyOnboardingTransaction,
    input: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly expectedPersonAuthorityVersion: number;
      readonly expectedProfileReviewVersion: number;
      readonly confirmedAt: Date;
    },
  ): Promise<{ readonly version: number }>;
  selectHousehold(
    transaction: FamilyOnboardingTransaction,
    input: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedPersonOnboardingVersion: number;
      readonly selectedAt: Date;
    },
  ): Promise<{ readonly version: number }>;
  saveAdultRoster(
    transaction: FamilyOnboardingTransaction,
    input: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedMembershipVersion: number;
      readonly expectedIntakeVersion: number;
      readonly adults: readonly {
        readonly id?: string | undefined;
        readonly displayName: string;
        readonly role: "steward" | "caregiver";
      }[];
      readonly reviewedAt: Date;
    },
  ): Promise<{ readonly version: number }>;
  markChildRosterReviewed(
    transaction: FamilyOnboardingTransaction,
    input: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedMembershipVersion: number;
      readonly expectedIntakeVersion: number;
      readonly reviewedAt: Date;
    },
  ): Promise<{ readonly version: number }>;
  prepareAdultIntentInvitation(
    transaction: FamilyOnboardingTransaction,
    input: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedMembershipVersion: number;
      readonly adultIntentId: string;
      readonly expectedIntentVersion: number;
      readonly proposedDisplayName: string;
      readonly role: "steward" | "caregiver";
      readonly matchedPersonId: string;
      readonly preparedAt: Date;
    },
  ): Promise<{ readonly intentVersion: number }>;
  bindAdultIntentInvitation(
    transaction: FamilyOnboardingTransaction,
    input: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedMembershipVersion: number;
      readonly adultIntentId: string;
      readonly expectedIntentVersion: number;
      readonly matchedPersonId: string;
      readonly invitationId: string;
      readonly boundAt: Date;
    },
  ): Promise<{ readonly version: number }>;
  reviewSharedContext(
    transaction: FamilyOnboardingTransaction,
    input: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedMembershipVersion: number;
      readonly expectedIntakeVersion: number;
      readonly expectedMembershipOnboardingVersion: number;
      readonly reviewedAt: Date;
    },
  ): Promise<{ readonly version: number }>;
  completeMembership(
    transaction: FamilyOnboardingTransaction,
    input: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedMembershipVersion: number;
      readonly expectedProfileReviewVersion: number;
      readonly expectedIntakeVersion: number;
      readonly expectedMembershipOnboardingVersion: number;
      readonly completedAt: Date;
    },
  ): Promise<{ readonly version: number }>;
  touchProgress(
    transaction: FamilyOnboardingTransaction,
    input: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly expectedPersonControlEpoch: number;
      readonly progressedAt: Date;
    },
  ): Promise<{ readonly version: number }>;
  recordReminderSent(
    transaction: FamilyOnboardingTransaction,
    input: {
      readonly personId: string;
      readonly expectedPersonControlEpoch: number;
      readonly expectedPersonOnboardingVersion: number;
      readonly sentAt: Date;
    },
  ): Promise<{ readonly version: number; readonly remindersSent: number }>;
  suppressReminders(
    transaction: FamilyOnboardingTransaction,
    input: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly expectedPersonOnboardingVersion: number;
      readonly suppressedAt: Date;
    },
  ): Promise<{ readonly version: number }>;
}

interface PersonRow {
  readonly id: string;
  readonly status: string;
  readonly authority_version: number | string;
  readonly control_epoch: number | string;
  readonly display_name_ciphertext: Buffer | null;
  readonly timezone: string | null;
  readonly profile_review_version: number | string | null;
  readonly person_onboarding_version: number | string | null;
  readonly selected_household_id: string | null;
  readonly reminders_sent: number | string | null;
  readonly last_reminded_at: Date | null;
  readonly reminders_suppressed_at: Date | null;
  readonly last_progressed_at: Date | null;
}

interface MembershipRow {
  readonly household_id: string;
  readonly membership_id: string;
  readonly membership_version: number | string;
  readonly role: string;
  readonly timezone: string;
}

interface IntakeRow {
  readonly adult_roster_reviewed_by_person_id: string | null;
  readonly adult_roster_reviewed_at: Date | null;
  readonly child_roster_reviewed_by_person_id: string | null;
  readonly child_roster_reviewed_at: Date | null;
  readonly version: number | string;
}

interface AdultIntentRow {
  readonly id: string;
  readonly display_name_ciphertext: Buffer;
  readonly role: string;
  readonly matched_person_id: string | null;
  readonly invitation_id: string | null;
  readonly invitation_current_pending: boolean;
  readonly joined: boolean;
  readonly version: number | string;
}

interface ChildRow {
  readonly person_id: string;
  readonly display_name_ciphertext: Buffer | null;
  readonly aliases_ciphertext: Buffer | null;
  readonly birth_year: number | null;
  readonly school_ciphertext: Buffer | null;
  readonly activities_ciphertext: Buffer | null;
}

interface MembershipOnboardingRow {
  readonly version: number | string;
  readonly shared_context_household_intake_version: number | string | null;
  readonly completed_household_intake_version: number | string | null;
  readonly completed_at: Date | null;
}

/** PostgreSQL adapter for the family-onboarding seam. */
export class PostgresFamilyOnboarding implements FamilyOnboarding {
  public constructor(private readonly secretBox: SecretBox) {}

  public async project(
    transaction: FamilyOnboardingTransaction,
    inputCandidate: { readonly actorPersonId: string; readonly personId: string },
  ): Promise<FamilyOnboardingProjection> {
    const input = exactPersonInputSchema.parse(inputCandidate);
    requireSelf(input.actorPersonId, input.personId);
    const person = await loadPerson(transaction, input.personId);
    const displayName = openText(
      this.secretBox,
      person.display_name_ciphertext,
      `person-display-name:${person.id}`,
      80,
    );
    const memberships = await loadMemberships(transaction, input.personId);
    if (memberships.length > MAX_HOUSEHOLDS) {
      throw new ConflictError("Choose a family with Florence before continuing setup");
    }
    const choices = memberships.map(toHouseholdChoice);
    const selected = selectCurrentMembership(memberships, person.selected_household_id);
    const selectionIsStale =
      person.selected_household_id !== null &&
      !memberships.some((membership) => membership.household_id === person.selected_household_id);
    const household = selected ? await this.loadHouseholdState(transaction, input.personId, selected) : null;
    const state: FamilyOnboardingPolicyState = {
      personId: input.personId,
      profile: {
        displayName,
        timezone: person.timezone,
        authorityVersion: Number(person.authority_version),
        controlEpoch: Number(person.control_epoch),
        reviewVersion: Number(person.profile_review_version ?? 0),
        onboardingVersion: Number(person.person_onboarding_version ?? 0),
        selectedHouseholdId: person.selected_household_id,
        remindersSent: Number(person.reminders_sent ?? 0),
        lastRemindedAt: person.last_reminded_at?.toISOString() ?? null,
        remindersSuppressedAt: person.reminders_suppressed_at?.toISOString() ?? null,
        lastProgressedAt: person.last_progressed_at?.toISOString() ?? null,
      },
      householdChoices: choices,
      selectedHousehold: household,
      selectionIsStale,
    };
    return {
      personId: input.personId,
      profile: state.profile,
      householdChoices: choices,
      household,
      nextStep: projectFamilyOnboardingStep(state),
    };
  }

  public async confirmProfile(
    transaction: FamilyOnboardingTransaction,
    inputCandidate: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly expectedPersonAuthorityVersion: number;
      readonly expectedProfileReviewVersion: number;
      readonly confirmedAt: Date;
    },
  ): Promise<{ readonly version: number }> {
    const input = z
      .strictObject({
        ...exactPersonInputSchema.shape,
        expectedPersonAuthorityVersion: z.number().int().positive(),
        expectedProfileReviewVersion: z.number().int().nonnegative(),
        confirmedAt: z.date(),
      })
      .parse(inputCandidate);
    requireSelf(input.actorPersonId, input.personId);
    const people = await transaction<PersonRow[]>`
      select person.id, person.status, person.authority_version, person.control_epoch,
        person.display_name_ciphertext, person.timezone,
        onboarding.profile_review_version,
        onboarding.version as person_onboarding_version,
        onboarding.selected_household_id, onboarding.reminders_sent,
        onboarding.last_reminded_at, onboarding.reminders_suppressed_at,
        onboarding.last_progressed_at
      from people person
      left join person_onboarding onboarding on onboarding.person_id = person.id
      where person.id = ${input.personId}
      for update of person
    `;
    const person = people[0];
    requireRegisteredPerson(person);
    if (Number(person.authority_version) !== input.expectedPersonAuthorityVersion) {
      throw new ConflictError("Your profile changed before it could be confirmed");
    }
    const currentVersion = Number(person.profile_review_version ?? 0);
    requireVersion(currentVersion, input.expectedProfileReviewVersion, "profile review");
    if (!person.display_name_ciphertext || !person.timezone) {
      throw new ConflictError("Add your name and time zone before confirming your profile");
    }
    const version = currentVersion + 1;
    if (person.person_onboarding_version === null) {
      await transaction`
        insert into person_onboarding (
          person_id, profile_reviewed_by_person_id, reviewed_person_authority_version,
          profile_review_version, profile_reviewed_at, last_progressed_at,
          version, created_at, updated_at
        ) values (
          ${input.personId}, ${input.actorPersonId}, ${input.expectedPersonAuthorityVersion},
          ${version}, ${input.confirmedAt}, ${input.confirmedAt},
          1, ${input.confirmedAt}, ${input.confirmedAt}
        )
      `;
    } else {
      const updated = await transaction<{ readonly profile_review_version: number | string }[]>`
        update person_onboarding
        set profile_reviewed_by_person_id = ${input.actorPersonId},
          reviewed_person_authority_version = ${input.expectedPersonAuthorityVersion},
          profile_review_version = profile_review_version + 1,
          profile_reviewed_at = ${input.confirmedAt},
          last_progressed_at = greatest(last_progressed_at, ${input.confirmedAt}),
          reminders_suppressed_at = null,
          version = version + 1, updated_at = ${input.confirmedAt}
        where person_id = ${input.personId}
          and profile_review_version = ${input.expectedProfileReviewVersion}
        returning profile_review_version
      `;
      if (!updated[0]) throw new ConflictError("Your profile review changed before it was saved");
    }
    return { version };
  }

  public async selectHousehold(
    transaction: FamilyOnboardingTransaction,
    inputCandidate: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedPersonOnboardingVersion: number;
      readonly selectedAt: Date;
    },
  ): Promise<{ readonly version: number }> {
    const input = z
      .strictObject({
        ...exactHouseholdInputSchema.shape,
        expectedPersonOnboardingVersion: z.number().int().nonnegative(),
        selectedAt: z.date(),
      })
      .parse(inputCandidate);
    requireSelf(input.actorPersonId, input.personId);
    await requireAdultMembership(transaction, input, false, undefined, true);
    if (input.expectedPersonOnboardingVersion === 0) {
      const rows = await transaction<{ readonly version: number | string }[]>`
        insert into person_onboarding (
          person_id, selected_household_id, last_progressed_at,
          version, created_at, updated_at
        ) values (
          ${input.personId}, ${input.householdId}, ${input.selectedAt},
          1, ${input.selectedAt}, ${input.selectedAt}
        ) on conflict (person_id) do nothing
        returning version
      `;
      if (!rows[0]) throw new ConflictError("Your family selection changed before it was saved");
      return { version: Number(rows[0].version) };
    }
    const rows = await transaction<{ readonly version: number | string }[]>`
      update person_onboarding
      set selected_household_id = ${input.householdId}, version = version + 1,
        reminders_suppressed_at = null,
        last_progressed_at = greatest(last_progressed_at, ${input.selectedAt}),
        updated_at = ${input.selectedAt}
      where person_id = ${input.personId} and version = ${input.expectedPersonOnboardingVersion}
      returning version
    `;
    if (!rows[0]) throw new ConflictError("Your family selection changed before it was saved");
    return { version: Number(rows[0].version) };
  }

  public async saveAdultRoster(
    transaction: FamilyOnboardingTransaction,
    inputCandidate: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedMembershipVersion: number;
      readonly expectedIntakeVersion: number;
      readonly adults: readonly {
        readonly id?: string | undefined;
        readonly displayName: string;
        readonly role: "steward" | "caregiver";
      }[];
      readonly reviewedAt: Date;
    },
  ): Promise<{ readonly version: number }> {
    const input = z
      .strictObject({
        ...exactHouseholdInputSchema.shape,
        expectedMembershipVersion: z.number().int().positive(),
        expectedIntakeVersion: z.number().int().nonnegative(),
        adults: z
          .array(
            z.strictObject({
              id: z.string().uuid().optional(),
              displayName: z.string().trim().min(1).max(80),
              role: AdultIntentRoleSchema,
            }),
          )
          .max(MAX_ADULT_INTENTS),
        reviewedAt: z.date(),
      })
      .superRefine((value, context) => {
        const ids = value.adults.flatMap((adult) => (adult.id ? [adult.id] : []));
        if (new Set(ids).size !== ids.length) {
          context.addIssue({
            code: "custom",
            message: "Each adult can appear only once in the onboarding roster",
            path: ["adults"],
          });
        }
      })
      .parse(inputCandidate);
    requireSelf(input.actorPersonId, input.personId);
    await requireAdultMembership(transaction, input, true, input.expectedMembershipVersion);
    const current = await lockIntake(transaction, input.householdId);
    const currentVersion = Number(current?.version ?? 0);
    requireVersion(currentVersion, input.expectedIntakeVersion, "family intake");
    const existing = await lockAdultIntents(transaction, input.householdId);
    const existingById = new Map(existing.map((intent) => [intent.id, intent] as const));
    for (const adult of input.adults) {
      if (adult.id && !existingById.has(adult.id)) {
        throw new ConflictError("The adult roster changed before it was saved");
      }
    }

    const retainedIds: string[] = [];
    for (const adult of input.adults) {
      const id = adult.id ?? randomUUID();
      retainedIds.push(id);
      const prior = existingById.get(id);
      if (prior?.invitation_id) {
        const priorName = openText(
          this.secretBox,
          prior.display_name_ciphertext,
          `household-onboarding-adult-intent-name:${id}`,
          80,
        );
        if (priorName !== adult.displayName || prior.role !== adult.role) {
          throw new ConflictError("An invited adult cannot be renamed or assigned a different role");
        }
      }
      const encryptedName = this.secretBox.encrypt(
        adult.displayName,
        `household-onboarding-adult-intent-name:${id}`,
      );
      if (prior) {
        await transaction`
          update household_onboarding_adult_intents
          set display_name_ciphertext = ${Buffer.from(JSON.stringify(encryptedName), "utf8")},
            display_name_key_version = ${encryptedName.kid}, role = ${adult.role},
            recorded_by_person_id = ${input.actorPersonId}, version = version + 1,
            updated_at = ${input.reviewedAt}
          where id = ${id} and household_id = ${input.householdId}
        `;
      } else {
        await transaction`
          insert into household_onboarding_adult_intents (
            id, household_id, display_name_ciphertext, display_name_key_version,
            role, recorded_by_person_id, version, created_at, updated_at
          ) values (
            ${id}, ${input.householdId},
            ${Buffer.from(JSON.stringify(encryptedName), "utf8")}, ${encryptedName.kid},
            ${adult.role}, ${input.actorPersonId}, 1, ${input.reviewedAt}, ${input.reviewedAt}
          )
        `;
      }
    }
    const removedIds = existing.map((intent) => intent.id).filter((id) => !retainedIds.includes(id));
    if (
      removedIds.some((id) => {
        const intent = existingById.get(id);
        return intent?.matched_person_id !== null || intent.invitation_id !== null;
      })
    ) {
      throw new ConflictError("An invited adult cannot be removed from the onboarding roster");
    }
    if (removedIds.length > 0) {
      await transaction`
        delete from household_onboarding_adult_intents
        where household_id = ${input.householdId}
          and id = any(${transaction.array(removedIds)}::uuid[])
      `;
    }

    const version = currentVersion + 1;
    if (!current) {
      await transaction`
        insert into household_onboarding_intakes (
          household_id, adult_roster_reviewed_by_person_id, adult_roster_reviewed_at,
          version, created_at, updated_at
        ) values (
          ${input.householdId}, ${input.actorPersonId}, ${input.reviewedAt},
          ${version}, ${input.reviewedAt}, ${input.reviewedAt}
        )
      `;
    } else {
      const updated = await transaction<{ readonly version: number | string }[]>`
        update household_onboarding_intakes
        set adult_roster_reviewed_by_person_id = ${input.actorPersonId},
          adult_roster_reviewed_at = ${input.reviewedAt}, version = version + 1,
          updated_at = ${input.reviewedAt}
        where household_id = ${input.householdId} and version = ${input.expectedIntakeVersion}
        returning version
      `;
      if (!updated[0]) throw new ConflictError("Your family intake changed before it was saved");
    }
    await touchPersonProgress(transaction, input.personId, input.reviewedAt);
    return { version };
  }

  public async markChildRosterReviewed(
    transaction: FamilyOnboardingTransaction,
    inputCandidate: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedMembershipVersion: number;
      readonly expectedIntakeVersion: number;
      readonly reviewedAt: Date;
    },
  ): Promise<{ readonly version: number }> {
    const input = z
      .strictObject({
        ...exactHouseholdInputSchema.shape,
        expectedMembershipVersion: z.number().int().positive(),
        expectedIntakeVersion: z.number().int().positive(),
        reviewedAt: z.date(),
      })
      .parse(inputCandidate);
    requireSelf(input.actorPersonId, input.personId);
    await requireAdultMembership(transaction, input, true, input.expectedMembershipVersion);
    const current = await lockIntake(transaction, input.householdId);
    if (!current?.adult_roster_reviewed_at) {
      throw new ConflictError("Review the adults who help this family before reviewing the children");
    }
    requireVersion(Number(current.version), input.expectedIntakeVersion, "family intake");
    const householdRows = await transaction<{ readonly membership_version: number | string }[]>`
      select membership_version from households
      where id = ${input.householdId} and status in ('onboarding', 'active')
      for update
    `;
    const household = householdRows[0];
    if (!household) throw new NotFoundError("Family does not exist");
    const roster = await transaction<
      { readonly child_count: number | string; readonly profiled_count: number | string }[]
    >`
      select count(*) as child_count, count(profile.person_id) as profiled_count
      from household_memberships membership
      left join dependent_profiles profile on profile.person_id = membership.person_id
      where membership.household_id = ${input.householdId}
        and membership.role = 'dependent' and membership.status = 'active'
    `;
    const childCount = Number(roster[0]?.child_count ?? 0);
    if (childCount < 1 || Number(roster[0]?.profiled_count ?? 0) !== childCount) {
      throw new ConflictError("Save at least one child before confirming the family roster");
    }
    const rows = await transaction<{ readonly version: number | string }[]>`
      update household_onboarding_intakes
      set child_roster_reviewed_by_person_id = ${input.actorPersonId},
        child_roster_reviewed_at = ${input.reviewedAt},
        child_roster_household_membership_version = ${Number(household.membership_version)},
        version = version + 1, updated_at = ${input.reviewedAt}
      where household_id = ${input.householdId} and version = ${input.expectedIntakeVersion}
      returning version
    `;
    if (!rows[0]) throw new ConflictError("Your family intake changed before it was saved");
    await touchPersonProgress(transaction, input.personId, input.reviewedAt);
    return { version: Number(rows[0].version) };
  }

  public async prepareAdultIntentInvitation(
    transaction: FamilyOnboardingTransaction,
    inputCandidate: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedMembershipVersion: number;
      readonly adultIntentId: string;
      readonly expectedIntentVersion: number;
      readonly proposedDisplayName: string;
      readonly role: "steward" | "caregiver";
      readonly matchedPersonId: string;
      readonly preparedAt: Date;
    },
  ): Promise<{ readonly intentVersion: number }> {
    const input = z
      .strictObject({
        ...exactHouseholdInputSchema.shape,
        expectedMembershipVersion: z.number().int().positive(),
        adultIntentId: z.string().uuid(),
        expectedIntentVersion: z.number().int().positive(),
        proposedDisplayName: z.string().trim().min(1).max(80),
        role: AdultIntentRoleSchema,
        matchedPersonId: z.string().uuid(),
        preparedAt: z.date(),
      })
      .parse(inputCandidate);
    requireSelf(input.actorPersonId, input.personId);
    await requireAdultMembership(transaction, input, true, input.expectedMembershipVersion);
    const rows = await transaction<
      {
        readonly display_name_ciphertext: Buffer;
        readonly role: string;
        readonly matched_person_id: string | null;
        readonly invitation_id: string | null;
        readonly version: number | string;
      }[]
    >`
      select display_name_ciphertext, role, matched_person_id, invitation_id, version
      from household_onboarding_adult_intents
      where id = ${input.adultIntentId} and household_id = ${input.householdId}
      for update
    `;
    const intent = rows[0];
    if (!intent) throw new NotFoundError("That onboarding adult is no longer in the family roster");
    if (Number(intent.version) !== input.expectedIntentVersion) {
      throw new ConflictError("The adult roster changed before the invitation was prepared");
    }
    const displayName = openText(
      this.secretBox,
      intent.display_name_ciphertext,
      `household-onboarding-adult-intent-name:${input.adultIntentId}`,
      80,
    );
    if (displayName !== input.proposedDisplayName || intent.role !== input.role) {
      throw new ConflictError("The adult’s name or family role changed before the invitation was created");
    }
    if (!intent.invitation_id || !intent.matched_person_id) {
      return { intentVersion: Number(intent.version) };
    }
    const bindings = await transaction<
      {
        readonly status: string;
        readonly current_pending: boolean;
        readonly joined: boolean;
      }[]
    >`
      select invitation.status,
        invitation.status = 'pending'
          and invitation.expires_at > ${input.preparedAt}
          and invitation.household_membership_version = household.membership_version
          and invitation.requested_role = ${intent.role}
          and identity.person_id = ${intent.matched_person_id}
          and identity.status in ('observed', 'verified')
          and exists(
            select 1
            from conversations source_conversation
            join participant_epochs source_epoch
              on source_epoch.id = source_conversation.current_epoch_id
              and source_epoch.id = invitation.source_participant_epoch_id
              and source_epoch.ended_at is null
              and source_epoch.participant_set_digest = invitation.source_participant_digest
            join epoch_participants source_invitee
              on source_invitee.participant_epoch_id = source_epoch.id
              and source_invitee.person_identity_id = invitation.invitee_identity_id
              and source_invitee.person_id = identity.person_id
            where source_conversation.id = invitation.source_conversation_id
              and source_conversation.kind = 'group'
              and source_conversation.status = 'active'
          )
          and (
            invitation.source_revision_id is null
            or exists(
              select 1
              from source_revisions source_revision
              join source_objects source_object on source_object.id = source_revision.source_object_id
                and source_object.status = 'active'
                and source_object.latest_revision_number = source_revision.revision_number
              where source_revision.id = invitation.source_revision_id
                and source_revision.participant_epoch_id = invitation.source_participant_epoch_id
                and source_revision.revoked_at is null
                and source_revision.retention_until > ${input.preparedAt}
            )
          ) as current_pending,
        invitation.status = 'accepted'
          and invitation.accepted_by_person_id = ${intent.matched_person_id}
          and invitation.requested_role = ${intent.role}
          and identity.person_id = ${intent.matched_person_id}
          and exists(
            select 1 from household_memberships joined_membership
            where joined_membership.household_id = invitation.household_id
              and joined_membership.person_id = ${intent.matched_person_id}
              and joined_membership.role = ${intent.role}
              and joined_membership.status = 'active'
          ) as joined
      from invitations invitation
      join households household on household.id = invitation.household_id
      join person_identities identity on identity.id = invitation.invitee_identity_id
      where invitation.id = ${intent.invitation_id}
        and invitation.household_id = ${input.householdId}
      for update of invitation
    `;
    const binding = bindings[0];
    if (!binding) {
      throw new ConflictError("The linked family invitation is no longer available");
    }
    if (binding.joined) {
      throw new ConflictError("That family adult has already joined and cannot be rematched");
    }
    if (binding.current_pending && intent.matched_person_id === input.matchedPersonId) {
      return { intentVersion: Number(intent.version) };
    }
    if (binding.status === "pending") {
      await transaction`
        update invitations
        set status = 'revoked', updated_at = ${input.preparedAt}
        where id = ${intent.invitation_id} and status = 'pending'
      `;
    }
    const reset = await transaction<{ readonly version: number | string }[]>`
      update household_onboarding_adult_intents
      set matched_person_id = null, invitation_id = null,
        version = version + 1, updated_at = ${input.preparedAt}
      where id = ${input.adultIntentId} and household_id = ${input.householdId}
        and version = ${input.expectedIntentVersion}
      returning version
    `;
    if (!reset[0]) {
      throw new ConflictError("The adult roster changed before the invitation was prepared");
    }
    return { intentVersion: Number(reset[0].version) };
  }

  public async bindAdultIntentInvitation(
    transaction: FamilyOnboardingTransaction,
    inputCandidate: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedMembershipVersion: number;
      readonly adultIntentId: string;
      readonly expectedIntentVersion: number;
      readonly matchedPersonId: string;
      readonly invitationId: string;
      readonly boundAt: Date;
    },
  ): Promise<{ readonly version: number }> {
    const input = z
      .strictObject({
        ...exactHouseholdInputSchema.shape,
        expectedMembershipVersion: z.number().int().positive(),
        adultIntentId: z.string().uuid(),
        expectedIntentVersion: z.number().int().positive(),
        matchedPersonId: z.string().uuid(),
        invitationId: z.string().uuid(),
        boundAt: z.date(),
      })
      .parse(inputCandidate);
    requireSelf(input.actorPersonId, input.personId);
    await requireAdultMembership(transaction, input, true, input.expectedMembershipVersion);
    const intentRows = await transaction<
      {
        readonly display_name_ciphertext: Buffer;
        readonly role: string;
        readonly matched_person_id: string | null;
        readonly invitation_id: string | null;
        readonly version: number | string;
      }[]
    >`
      select display_name_ciphertext, role, matched_person_id, invitation_id, version
      from household_onboarding_adult_intents
      where id = ${input.adultIntentId} and household_id = ${input.householdId}
      for update
    `;
    const intent = intentRows[0];
    if (!intent || Number(intent.version) !== input.expectedIntentVersion) {
      throw new ConflictError("The adult roster changed before the invitation was linked");
    }
    const invitationRows = await transaction<
      {
        readonly household_id: string;
        readonly invitee_person_id: string;
        readonly requested_role: string;
        readonly proposed_display_name_ciphertext: Buffer | null;
        readonly status: string;
      }[]
    >`
      select invitation.household_id, identity.person_id as invitee_person_id,
        invitation.requested_role, invitation.proposed_display_name_ciphertext,
        invitation.status
      from invitations invitation
      join person_identities identity on identity.id = invitation.invitee_identity_id
      where invitation.id = ${input.invitationId}
      for update of invitation
    `;
    const invitation = invitationRows[0];
    const intentName = openText(
      this.secretBox,
      intent.display_name_ciphertext,
      `household-onboarding-adult-intent-name:${input.adultIntentId}`,
      80,
    );
    const invitationName = invitation?.proposed_display_name_ciphertext
      ? openText(
          this.secretBox,
          invitation.proposed_display_name_ciphertext,
          `invitation-proposed-display-name:${input.invitationId}`,
          80,
        )
      : null;
    if (
      !invitation ||
      invitation.household_id !== input.householdId ||
      invitation.invitee_person_id !== input.matchedPersonId ||
      invitation.requested_role !== intent.role ||
      invitationName !== intentName ||
      !["pending", "accepted"].includes(invitation.status)
    ) {
      throw new ConflictError("The exact family invitation no longer matches this onboarding adult");
    }
    if (intent.matched_person_id && intent.matched_person_id !== input.matchedPersonId) {
      throw new ConflictError("That onboarding adult is already matched to someone else");
    }
    if (intent.matched_person_id === input.matchedPersonId && intent.invitation_id === input.invitationId) {
      return { version: Number(intent.version) };
    }
    const rows = await transaction<{ readonly version: number | string }[]>`
      update household_onboarding_adult_intents
      set matched_person_id = ${input.matchedPersonId}, invitation_id = ${input.invitationId},
        version = version + 1, updated_at = ${input.boundAt}
      where id = ${input.adultIntentId} and household_id = ${input.householdId}
        and version = ${input.expectedIntentVersion}
      returning version
    `;
    if (!rows[0]) throw new ConflictError("The adult roster changed before the invitation was linked");
    return { version: Number(rows[0].version) };
  }

  public async reviewSharedContext(
    transaction: FamilyOnboardingTransaction,
    inputCandidate: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedMembershipVersion: number;
      readonly expectedIntakeVersion: number;
      readonly expectedMembershipOnboardingVersion: number;
      readonly reviewedAt: Date;
    },
  ): Promise<{ readonly version: number }> {
    const input = z
      .strictObject({
        ...exactHouseholdInputSchema.shape,
        expectedMembershipVersion: z.number().int().positive(),
        expectedIntakeVersion: z.number().int().positive(),
        expectedMembershipOnboardingVersion: z.number().int().nonnegative(),
        reviewedAt: z.date(),
      })
      .parse(inputCandidate);
    requireSelf(input.actorPersonId, input.personId);
    const membership = await requireAdultMembership(
      transaction,
      input,
      false,
      input.expectedMembershipVersion,
    );
    const intake = await lockIntake(transaction, input.householdId);
    if (!intake?.child_roster_reviewed_at || Number(intake.version) !== input.expectedIntakeVersion) {
      throw new ConflictError("The shared family context changed before it was reviewed");
    }
    const current = await lockMembershipOnboarding(transaction, membership.membershipId);
    const currentVersion = Number(current?.version ?? 0);
    requireVersion(currentVersion, input.expectedMembershipOnboardingVersion, "family membership onboarding");
    const version = currentVersion + 1;
    if (current?.completed_at) {
      if (membership.role === "steward") {
        throw new ConflictError("A completed parent setup cannot be reopened as a supporting adult review");
      }
      if (
        Number(current.shared_context_household_intake_version ?? 0) === input.expectedIntakeVersion &&
        Number(current.completed_household_intake_version ?? -1) === input.expectedIntakeVersion
      ) {
        return { version: currentVersion };
      }
      const reopened = await transaction<{ readonly version: number | string }[]>`
        update membership_onboarding
        set shared_context_reviewed_by_person_id = ${input.actorPersonId},
          shared_context_household_intake_version = ${input.expectedIntakeVersion},
          shared_context_reviewed_at = ${input.reviewedAt},
          completed_by_person_id = null, completed_membership_version = null,
          completed_profile_review_version = null, completed_household_intake_version = null,
          completed_google_decision = null, completed_at = null,
          version = version + 1, updated_at = ${input.reviewedAt}
        where membership_id = ${membership.membershipId}
          and version = ${input.expectedMembershipOnboardingVersion}
          and completed_at is not null
        returning version
      `;
      if (!reopened[0]) throw new ConflictError("Your family review changed before it was saved");
      await touchPersonProgress(transaction, input.personId, input.reviewedAt);
      return { version: Number(reopened[0].version) };
    }
    if (!current) {
      await transaction`
        insert into membership_onboarding (
          membership_id, shared_context_reviewed_by_person_id,
          shared_context_household_intake_version, shared_context_reviewed_at,
          version, created_at, updated_at
        ) values (
          ${membership.membershipId}, ${input.actorPersonId}, ${input.expectedIntakeVersion},
          ${input.reviewedAt}, ${version}, ${input.reviewedAt}, ${input.reviewedAt}
        )
      `;
    } else {
      const rows = await transaction<{ readonly version: number | string }[]>`
        update membership_onboarding
        set shared_context_reviewed_by_person_id = ${input.actorPersonId},
          shared_context_household_intake_version = ${input.expectedIntakeVersion},
          shared_context_reviewed_at = ${input.reviewedAt}, version = version + 1,
          updated_at = ${input.reviewedAt}
        where membership_id = ${membership.membershipId}
          and version = ${input.expectedMembershipOnboardingVersion}
          and completed_at is null
        returning version
      `;
      if (!rows[0]) throw new ConflictError("Your family review changed before it was saved");
    }
    await touchPersonProgress(transaction, input.personId, input.reviewedAt);
    return { version };
  }

  public async completeMembership(
    transaction: FamilyOnboardingTransaction,
    inputCandidate: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly householdId: string;
      readonly expectedMembershipVersion: number;
      readonly expectedProfileReviewVersion: number;
      readonly expectedIntakeVersion: number;
      readonly expectedMembershipOnboardingVersion: number;
      readonly completedAt: Date;
    },
  ): Promise<{ readonly version: number }> {
    const input = z
      .strictObject({
        ...exactHouseholdInputSchema.shape,
        expectedMembershipVersion: z.number().int().positive(),
        expectedProfileReviewVersion: z.number().int().positive(),
        expectedIntakeVersion: z.number().int().nonnegative(),
        expectedMembershipOnboardingVersion: z.number().int().nonnegative(),
        completedAt: z.date(),
      })
      .parse(inputCandidate);
    requireSelf(input.actorPersonId, input.personId);
    const projectionInput = { actorPersonId: input.actorPersonId, personId: input.personId };
    let projection = await this.project(transaction, projectionInput);
    if (
      projection.profile.selectedHouseholdId === null &&
      projection.householdChoices.length === 1 &&
      projection.householdChoices[0]?.householdId === input.householdId
    ) {
      await this.selectHousehold(transaction, {
        actorPersonId: input.actorPersonId,
        personId: input.personId,
        householdId: input.householdId,
        expectedPersonOnboardingVersion: projection.profile.onboardingVersion,
        selectedAt: input.completedAt,
      });
      projection = await this.project(transaction, projectionInput);
    }
    const household = projection.household;
    if (!household || projection.nextStep.kind !== "review") {
      if (projection.nextStep.kind === "complete" && household) {
        return { version: household.membershipOnboardingVersion };
      }
      throw new ConflictError("Finish the current onboarding step before launching Florence");
    }
    if (
      household.householdId !== input.householdId ||
      household.membershipVersion !== input.expectedMembershipVersion ||
      projection.profile.reviewVersion !== input.expectedProfileReviewVersion ||
      household.intakeVersion !== input.expectedIntakeVersion ||
      household.membershipOnboardingVersion !== input.expectedMembershipOnboardingVersion ||
      household.googleDecision === null
    ) {
      throw new ConflictError("Your onboarding review changed before it was confirmed");
    }
    const version = input.expectedMembershipOnboardingVersion + 1;
    if (input.expectedMembershipOnboardingVersion === 0) {
      await transaction`
        insert into membership_onboarding (
          membership_id, completed_by_person_id, completed_membership_version,
          completed_profile_review_version, completed_household_intake_version,
          completed_google_decision, completed_at, version, created_at, updated_at
        ) values (
          ${household.membershipId}, ${input.actorPersonId}, ${input.expectedMembershipVersion},
          ${input.expectedProfileReviewVersion}, ${input.expectedIntakeVersion},
          ${household.googleDecision}, ${input.completedAt}, ${version},
          ${input.completedAt}, ${input.completedAt}
        )
      `;
    } else {
      const rows = await transaction<{ readonly version: number | string }[]>`
        update membership_onboarding
        set completed_by_person_id = ${input.actorPersonId},
          completed_membership_version = ${input.expectedMembershipVersion},
          completed_profile_review_version = ${input.expectedProfileReviewVersion},
          completed_household_intake_version = ${input.expectedIntakeVersion},
          completed_google_decision = ${household.googleDecision},
          completed_at = ${input.completedAt}, version = version + 1,
          updated_at = ${input.completedAt}
        where membership_id = ${household.membershipId}
          and version = ${input.expectedMembershipOnboardingVersion}
          and completed_at is null
        returning version
      `;
      if (!rows[0]) throw new ConflictError("Your final review changed before it was saved");
    }
    await touchPersonProgress(transaction, input.personId, input.completedAt);
    return { version };
  }

  public async touchProgress(
    transaction: FamilyOnboardingTransaction,
    inputCandidate: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly expectedPersonControlEpoch: number;
      readonly progressedAt: Date;
    },
  ): Promise<{ readonly version: number }> {
    const input = z
      .strictObject({
        ...exactPersonInputSchema.shape,
        expectedPersonControlEpoch: z.number().int().positive(),
        progressedAt: z.date(),
      })
      .parse(inputCandidate);
    requireSelf(input.actorPersonId, input.personId);
    const people = await transaction<{ readonly id: string }[]>`
      select id from people
      where id = ${input.personId} and status = 'registered'
        and control_epoch = ${input.expectedPersonControlEpoch}
      for update
    `;
    if (!people[0]) throw new UnauthorizedError("Your Florence account changed before setup resumed");
    return { version: await touchPersonProgress(transaction, input.personId, input.progressedAt) };
  }

  public async recordReminderSent(
    transaction: FamilyOnboardingTransaction,
    inputCandidate: {
      readonly personId: string;
      readonly expectedPersonControlEpoch: number;
      readonly expectedPersonOnboardingVersion: number;
      readonly sentAt: Date;
    },
  ): Promise<{ readonly version: number; readonly remindersSent: number }> {
    const input = z
      .strictObject({
        personId: z.string().uuid(),
        expectedPersonControlEpoch: z.number().int().positive(),
        expectedPersonOnboardingVersion: z.number().int().positive(),
        sentAt: z.date(),
      })
      .parse(inputCandidate);
    const projection = await this.project(transaction, {
      actorPersonId: input.personId,
      personId: input.personId,
    });
    if (projection.nextStep.kind === "complete") {
      throw new ConflictError("Completed onboarding cannot receive a setup reminder");
    }
    const rows = await transaction<
      { readonly version: number | string; readonly reminders_sent: number | string }[]
    >`
      update person_onboarding onboarding
      set reminders_sent = reminders_sent + 1, last_reminded_at = ${input.sentAt},
        version = version + 1,
        updated_at = ${input.sentAt}
      where onboarding.person_id = ${input.personId}
        and onboarding.version = ${input.expectedPersonOnboardingVersion}
        and onboarding.reminders_sent < 2
        and onboarding.reminders_suppressed_at is null
        and exists(
          select 1 from people person
          where person.id = onboarding.person_id and person.status = 'registered'
            and person.control_epoch = ${input.expectedPersonControlEpoch}
        )
      returning version, reminders_sent
    `;
    const row = rows[0];
    if (!row) throw new ConflictError("This onboarding reminder is no longer current");
    return { version: Number(row.version), remindersSent: Number(row.reminders_sent) };
  }

  public async suppressReminders(
    transaction: FamilyOnboardingTransaction,
    inputCandidate: {
      readonly actorPersonId: string;
      readonly personId: string;
      readonly expectedPersonOnboardingVersion: number;
      readonly suppressedAt: Date;
    },
  ): Promise<{ readonly version: number }> {
    const input = z
      .strictObject({
        ...exactPersonInputSchema.shape,
        expectedPersonOnboardingVersion: z.number().int().nonnegative(),
        suppressedAt: z.date(),
      })
      .parse(inputCandidate);
    requireSelf(input.actorPersonId, input.personId);
    await loadPerson(transaction, input.personId);
    if (input.expectedPersonOnboardingVersion === 0) {
      await transaction`
        insert into person_onboarding (
          person_id, reminders_suppressed_at, last_progressed_at,
          version, created_at, updated_at
        ) values (
          ${input.personId}, ${input.suppressedAt}, ${input.suppressedAt},
          1, ${input.suppressedAt}, ${input.suppressedAt}
        )
      `;
      return { version: 1 };
    }
    const rows = await transaction<{ readonly version: number | string }[]>`
      update person_onboarding
      set reminders_suppressed_at = ${input.suppressedAt},
        last_progressed_at = greatest(last_progressed_at, ${input.suppressedAt}),
        version = version + 1,
        updated_at = ${input.suppressedAt}
      where person_id = ${input.personId} and version = ${input.expectedPersonOnboardingVersion}
      returning version
    `;
    if (!rows[0]) throw new ConflictError("Your reminder preference changed before it was saved");
    return { version: Number(rows[0].version) };
  }

  private async loadHouseholdState(
    transaction: FamilyOnboardingTransaction,
    personId: string,
    membership: MembershipRow,
  ): Promise<FamilyOnboardingHousehold> {
    const intakes = await transaction<IntakeRow[]>`
      select adult_roster_reviewed_by_person_id, adult_roster_reviewed_at,
        child_roster_reviewed_by_person_id, child_roster_reviewed_at, version
      from household_onboarding_intakes where household_id = ${membership.household_id}
    `;
    const intake = intakes[0];
    const adultRows = await transaction<AdultIntentRow[]>`
      select intent.id, intent.display_name_ciphertext, intent.role,
        intent.matched_person_id, intent.invitation_id,
        coalesce(
          invitation.status = 'pending'
            and invitation.expires_at > now()
            and invitation.household_membership_version = household.membership_version
            and invitation.requested_role = intent.role
            and invited_identity.person_id = intent.matched_person_id
            and invited_identity.status in ('observed', 'verified')
            and exists(
              select 1
              from conversations source_conversation
              join participant_epochs source_epoch
                on source_epoch.id = source_conversation.current_epoch_id
                and source_epoch.id = invitation.source_participant_epoch_id
                and source_epoch.ended_at is null
                and source_epoch.participant_set_digest = invitation.source_participant_digest
              join epoch_participants source_invitee
                on source_invitee.participant_epoch_id = source_epoch.id
                and source_invitee.person_identity_id = invitation.invitee_identity_id
                and source_invitee.person_id = invited_identity.person_id
              where source_conversation.id = invitation.source_conversation_id
                and source_conversation.kind = 'group'
                and source_conversation.status = 'active'
            )
            and (
              invitation.source_revision_id is null
              or exists(
                select 1
                from source_revisions source_revision
                join source_objects source_object on source_object.id = source_revision.source_object_id
                  and source_object.status = 'active'
                  and source_object.latest_revision_number = source_revision.revision_number
                where source_revision.id = invitation.source_revision_id
                  and source_revision.participant_epoch_id = invitation.source_participant_epoch_id
                  and source_revision.revoked_at is null
                  and source_revision.retention_until > now()
              )
            ),
          false
        ) as invitation_current_pending,
        coalesce(
          invitation.status = 'accepted'
            and invitation.accepted_by_person_id = intent.matched_person_id
            and invitation.requested_role = intent.role
            and invited_identity.person_id = intent.matched_person_id
            and exists(
              select 1 from household_memberships joined_membership
              where joined_membership.household_id = intent.household_id
                and joined_membership.person_id = intent.matched_person_id
                and joined_membership.role = intent.role
                and joined_membership.status = 'active'
            ),
          false
        ) as joined,
        intent.version
      from household_onboarding_adult_intents intent
      join households household on household.id = intent.household_id
      left join invitations invitation on invitation.id = intent.invitation_id
        and invitation.household_id = intent.household_id
      left join person_identities invited_identity on invited_identity.id = invitation.invitee_identity_id
      where intent.household_id = ${membership.household_id}
      order by intent.created_at, intent.id
      limit ${MAX_ADULT_INTENTS + 1}
    `;
    if (adultRows.length > MAX_ADULT_INTENTS) {
      throw new ConflictError("This family has too many adults for onboarding");
    }
    const childRows = await transaction<ChildRow[]>`
      select child.id as person_id, child.display_name_ciphertext,
        profile.aliases_ciphertext, profile.birth_year,
        profile.school_ciphertext, profile.activities_ciphertext
      from household_memberships child_membership
      join people child on child.id = child_membership.person_id
        and child.status in ('provisional', 'registered')
      left join dependent_profiles profile on profile.person_id = child.id
      where child_membership.household_id = ${membership.household_id}
        and child_membership.role = 'dependent' and child_membership.status = 'active'
      order by child_membership.joined_at, child.id
      limit ${MAX_CHILDREN + 1}
    `;
    if (childRows.length > MAX_CHILDREN) {
      throw new ConflictError("This family has too many child profiles for onboarding");
    }
    const onboardingRows = await transaction<MembershipOnboardingRow[]>`
      select version, shared_context_household_intake_version,
        completed_household_intake_version, completed_at
      from membership_onboarding where membership_id = ${membership.membership_id}
    `;
    const membershipOnboarding = onboardingRows[0];
    const intakeVersion = Number(intake?.version ?? 0);
    const role = parseAdultRole(membership.role);
    const childRosterReviewed = intake?.child_roster_reviewed_at !== null && intake !== undefined;
    const sharedContextReviewed =
      intakeVersion > 0 &&
      Number(membershipOnboarding?.shared_context_household_intake_version ?? 0) === intakeVersion;
    const completionIsCurrent =
      membershipOnboarding?.completed_at !== null &&
      membershipOnboarding !== undefined &&
      (role === "steward" ||
        !childRosterReviewed ||
        (sharedContextReviewed &&
          Number(membershipOnboarding.completed_household_intake_version ?? -1) === intakeVersion));
    const googleRows = await transaction<{ readonly connected: boolean; readonly suppressed: boolean }[]>`
      select exists(
        select 1 from integrations integration
        where integration.person_id = ${personId}
          and integration.provider = 'google'
          and integration.account_kind = 'personal_family'
          and integration.status = 'active'
      ) as connected,
      person.google_activation_suppressed_at is not null as suppressed
      from people person where person.id = ${personId}
    `;
    const google = googleRows[0];
    const googleDecision = google?.connected ? "connected" : google?.suppressed ? "limited" : null;
    return {
      householdId: membership.household_id,
      membershipId: membership.membership_id,
      membershipVersion: Number(membership.membership_version),
      role,
      timezone: membership.timezone,
      intakeVersion,
      adultRosterReviewed: intake?.adult_roster_reviewed_at !== null && intake !== undefined,
      adultRosterReviewedByPersonId: intake?.adult_roster_reviewed_by_person_id ?? null,
      adults: adultRows.map((adult) => hydrateAdultIntent(this.secretBox, adult)),
      childRosterReviewed,
      childRosterReviewedByPersonId: intake?.child_roster_reviewed_by_person_id ?? null,
      children: childRows.map((child) => hydrateChild(this.secretBox, child)),
      sharedContextReviewed,
      membershipOnboardingVersion: Number(membershipOnboarding?.version ?? 0),
      googleDecision,
      completed: completionIsCurrent,
    };
  }
}

const exactPersonInputSchema = z.strictObject({
  actorPersonId: z.string().uuid(),
  personId: z.string().uuid(),
});

const exactHouseholdInputSchema = z.strictObject({
  ...exactPersonInputSchema.shape,
  householdId: z.string().uuid(),
});

function requireSelf(actorPersonId: string, personId: string): void {
  if (actorPersonId !== personId) {
    throw new UnauthorizedError("You can only complete your own private onboarding");
  }
}

function requireRegisteredPerson(person: PersonRow | undefined): asserts person is PersonRow {
  if (person?.status !== "registered") {
    throw new UnauthorizedError("A registered private Florence identity is required");
  }
}

async function loadPerson(transaction: FamilyOnboardingTransaction, personId: string): Promise<PersonRow> {
  const rows = await transaction<PersonRow[]>`
    select person.id, person.status, person.authority_version, person.control_epoch,
      person.display_name_ciphertext, person.timezone,
      onboarding.profile_review_version,
      onboarding.version as person_onboarding_version,
      onboarding.selected_household_id, onboarding.reminders_sent,
      onboarding.last_reminded_at, onboarding.reminders_suppressed_at,
      onboarding.last_progressed_at
    from people person
    left join person_onboarding onboarding on onboarding.person_id = person.id
    where person.id = ${personId}
  `;
  const person = rows[0];
  requireRegisteredPerson(person);
  return person;
}

async function loadMemberships(
  transaction: FamilyOnboardingTransaction,
  personId: string,
): Promise<MembershipRow[]> {
  return transaction<MembershipRow[]>`
    select household.id as household_id, membership.id as membership_id,
      membership.version as membership_version, membership.role, household.timezone
    from household_memberships membership
    join households household on household.id = membership.household_id
      and household.status in ('onboarding', 'active')
    join membership_capabilities capability on capability.membership_id = membership.id
      and capability.capability = 'household.read' and capability.status = 'active'
    where membership.person_id = ${personId}
      and membership.status = 'active' and membership.role <> 'dependent'
    order by membership.joined_at, membership.id
    limit ${MAX_HOUSEHOLDS + 1}
  `;
}

function selectCurrentMembership(
  memberships: readonly MembershipRow[],
  selectedHouseholdId: string | null,
): MembershipRow | null {
  return selectedHouseholdId
    ? (memberships.find((membership) => membership.household_id === selectedHouseholdId) ?? null)
    : null;
}

function toHouseholdChoice(membership: MembershipRow): FamilyOnboardingHouseholdChoice {
  return {
    householdId: membership.household_id,
    membershipId: membership.membership_id,
    role: parseAdultRole(membership.role),
    timezone: membership.timezone,
  };
}

function parseAdultRole(role: string): FamilyOnboardingHouseholdChoice["role"] {
  if (role === "steward" || role === "caregiver" || role === "participant") return role;
  throw new UnauthorizedError("A child profile cannot complete adult onboarding");
}

async function requireAdultMembership(
  transaction: FamilyOnboardingTransaction,
  input: { readonly personId: string; readonly householdId: string },
  requireGovern: boolean,
  expectedMembershipVersion?: number,
  allowSelectionChange = false,
): Promise<{
  readonly membershipId: string;
  readonly version: number;
  readonly role: FamilyOnboardingHouseholdChoice["role"];
}> {
  const rows = await transaction<
    {
      readonly membership_id: string;
      readonly version: number | string;
      readonly role: string;
      readonly can_read: boolean;
      readonly can_govern: boolean;
      readonly active_membership_count: number | string;
      readonly selected_household_id: string | null;
    }[]
  >`
    select membership.id as membership_id, membership.version, membership.role,
      exists(
        select 1 from membership_capabilities capability
        where capability.membership_id = membership.id
          and capability.capability = 'household.read' and capability.status = 'active'
      ) as can_read,
      exists(
        select 1 from membership_capabilities capability
        where capability.membership_id = membership.id
          and capability.capability = 'household.govern' and capability.status = 'active'
      ) as can_govern,
      (
        select count(*) from household_memberships active_membership
        join households active_household on active_household.id = active_membership.household_id
          and active_household.status in ('onboarding', 'active')
        join membership_capabilities read_capability
          on read_capability.membership_id = active_membership.id
          and read_capability.capability = 'household.read'
          and read_capability.status = 'active'
        where active_membership.person_id = ${input.personId}
          and active_membership.status = 'active' and active_membership.role <> 'dependent'
      ) as active_membership_count,
      onboarding.selected_household_id
    from household_memberships membership
    join households household on household.id = membership.household_id
      and household.status in ('onboarding', 'active')
    left join person_onboarding onboarding on onboarding.person_id = membership.person_id
    where membership.household_id = ${input.householdId}
      and membership.person_id = ${input.personId}
      and membership.status = 'active' and membership.role <> 'dependent'
    for update of membership, household
  `;
  const membership = rows[0];
  if (!membership?.can_read || (requireGovern && !membership.can_govern)) {
    throw new UnauthorizedError("You cannot change this family onboarding");
  }
  if (
    !allowSelectionChange &&
    Number(membership.active_membership_count) > 1 &&
    membership.selected_household_id !== input.householdId
  ) {
    throw new ConflictError("Choose this family before continuing its onboarding");
  }
  const version = Number(membership.version);
  if (expectedMembershipVersion !== undefined) {
    requireVersion(version, expectedMembershipVersion, "family membership");
  }
  return { membershipId: membership.membership_id, version, role: parseAdultRole(membership.role) };
}

async function lockIntake(
  transaction: FamilyOnboardingTransaction,
  householdId: string,
): Promise<IntakeRow | undefined> {
  const rows = await transaction<IntakeRow[]>`
    select adult_roster_reviewed_by_person_id, adult_roster_reviewed_at,
      child_roster_reviewed_by_person_id, child_roster_reviewed_at, version
    from household_onboarding_intakes where household_id = ${householdId}
    for update
  `;
  return rows[0];
}

async function lockAdultIntents(
  transaction: FamilyOnboardingTransaction,
  householdId: string,
): Promise<AdultIntentRow[]> {
  const rows = await transaction<AdultIntentRow[]>`
    select intent.id, intent.display_name_ciphertext, intent.role,
      intent.matched_person_id, intent.invitation_id,
      coalesce(
        invitation.status = 'pending'
          and invitation.expires_at > now()
          and invitation.household_membership_version = household.membership_version
          and invitation.requested_role = intent.role
          and invited_identity.person_id = intent.matched_person_id
          and invited_identity.status in ('observed', 'verified')
          and exists(
            select 1
            from conversations source_conversation
            join participant_epochs source_epoch
              on source_epoch.id = source_conversation.current_epoch_id
              and source_epoch.id = invitation.source_participant_epoch_id
              and source_epoch.ended_at is null
              and source_epoch.participant_set_digest = invitation.source_participant_digest
            join epoch_participants source_invitee
              on source_invitee.participant_epoch_id = source_epoch.id
              and source_invitee.person_identity_id = invitation.invitee_identity_id
              and source_invitee.person_id = invited_identity.person_id
            where source_conversation.id = invitation.source_conversation_id
              and source_conversation.kind = 'group'
              and source_conversation.status = 'active'
          )
          and (
            invitation.source_revision_id is null
            or exists(
              select 1
              from source_revisions source_revision
              join source_objects source_object on source_object.id = source_revision.source_object_id
                and source_object.status = 'active'
                and source_object.latest_revision_number = source_revision.revision_number
              where source_revision.id = invitation.source_revision_id
                and source_revision.participant_epoch_id = invitation.source_participant_epoch_id
                and source_revision.revoked_at is null
                and source_revision.retention_until > now()
            )
          ),
        false
      ) as invitation_current_pending,
      coalesce(
        invitation.status = 'accepted'
          and invitation.accepted_by_person_id = intent.matched_person_id
          and invitation.requested_role = intent.role
          and invited_identity.person_id = intent.matched_person_id
          and exists(
            select 1 from household_memberships joined_membership
            where joined_membership.household_id = intent.household_id
              and joined_membership.person_id = intent.matched_person_id
              and joined_membership.role = intent.role
              and joined_membership.status = 'active'
          ),
        false
      ) as joined,
      intent.version
    from household_onboarding_adult_intents intent
    join households household on household.id = intent.household_id
    left join invitations invitation on invitation.id = intent.invitation_id
      and invitation.household_id = intent.household_id
    left join person_identities invited_identity on invited_identity.id = invitation.invitee_identity_id
    where intent.household_id = ${householdId}
    order by intent.created_at, intent.id
    limit ${MAX_ADULT_INTENTS + 1}
    for update of intent
  `;
  if (rows.length > MAX_ADULT_INTENTS) {
    throw new ConflictError("This family has too many adults for onboarding");
  }
  return rows;
}

async function lockMembershipOnboarding(
  transaction: FamilyOnboardingTransaction,
  membershipId: string,
): Promise<MembershipOnboardingRow | undefined> {
  const rows = await transaction<MembershipOnboardingRow[]>`
    select version, shared_context_household_intake_version,
      completed_household_intake_version, completed_at
    from membership_onboarding where membership_id = ${membershipId}
    for update
  `;
  return rows[0];
}

async function touchPersonProgress(
  transaction: FamilyOnboardingTransaction,
  personId: string,
  progressedAt: Date,
): Promise<number> {
  const rows = await transaction<{ readonly version: number | string }[]>`
    insert into person_onboarding (
      person_id, last_progressed_at, version, created_at, updated_at
    ) values (${personId}, ${progressedAt}, 1, ${progressedAt}, ${progressedAt})
    on conflict (person_id) do update set
      last_progressed_at = greatest(person_onboarding.last_progressed_at, excluded.last_progressed_at),
      reminders_suppressed_at = null,
      version = person_onboarding.version + 1,
      updated_at = excluded.updated_at
    returning version
  `;
  const row = rows[0];
  if (!row) throw new ConflictError("Onboarding progress could not be recorded");
  return Number(row.version);
}

function requireVersion(current: number, expected: number, label: string): void {
  if (current !== expected) throw new ConflictError(`The ${label} changed before it was saved`);
}

function hydrateChild(secretBox: SecretBox, row: ChildRow): FamilyOnboardingChild {
  return {
    personId: row.person_id,
    displayName:
      openText(secretBox, row.display_name_ciphertext, `person-display-name:${row.person_id}`, 80) ?? "Child",
    aliases: openList(secretBox, row.aliases_ciphertext, `dependent-aliases:${row.person_id}`, 12, 80),
    birthYear: row.birth_year,
    school: openText(secretBox, row.school_ciphertext, `dependent-school:${row.person_id}`, 160) ?? "",
    activities: openList(
      secretBox,
      row.activities_ciphertext,
      `dependent-activities:${row.person_id}`,
      24,
      120,
    ),
  };
}

function hydrateAdultIntent(secretBox: SecretBox, row: AdultIntentRow): FamilyOnboardingAdultIntent {
  return {
    id: row.id,
    version: Number(row.version),
    displayName:
      openText(
        secretBox,
        row.display_name_ciphertext,
        `household-onboarding-adult-intent-name:${row.id}`,
        80,
      ) ?? "Family member",
    role: AdultIntentRoleSchema.parse(row.role),
    matchedPersonId: row.matched_person_id,
    invitationId: row.invitation_id,
    status: row.joined ? "joined" : row.invitation_current_pending ? "invited" : "not_invited",
  };
}

function openText(
  secretBox: SecretBox,
  ciphertext: Buffer | null,
  purpose: string,
  maxLength: number,
): string | null {
  if (!ciphertext) return null;
  try {
    const value = secretBox
      .decrypt(JSON.parse(ciphertext.toString("utf8")), purpose)
      .toString("utf8")
      .trim();
    return value.length > 0 && value.length <= maxLength ? value : null;
  } catch {
    return null;
  }
}

function openList(
  secretBox: SecretBox,
  ciphertext: Buffer | null,
  purpose: string,
  maxItems: number,
  maxLength: number,
): string[] {
  const text = openText(secretBox, ciphertext, purpose, 32_000);
  if (!text) return [];
  try {
    const values = z.array(z.string().trim().min(1).max(maxLength)).max(maxItems).parse(JSON.parse(text));
    return [...new Set(values)];
  } catch {
    return [];
  }
}
