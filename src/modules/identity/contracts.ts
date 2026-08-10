import { z } from "zod";

export const EntityIdSchema = z.string().uuid();
export const DigestSchema = z.string().regex(/^[a-f0-9]{64}$/u);
export const InstantSchema = z.iso.datetime({ offset: true });

export const PersonStatusSchema = z.enum([
  "provisional",
  "registered",
  "stopped",
  "deletion_fenced",
  "merged",
  "deleted",
]);
export type PersonStatus = z.infer<typeof PersonStatusSchema>;

export const IdentityKindSchema = z.enum(["phone", "email", "provider_handle", "provider_account"]);
export type IdentityKind = z.infer<typeof IdentityKindSchema>;

export const IdentityStatusSchema = z.enum(["observed", "pending_claim", "verified", "revoked"]);
export type IdentityStatus = z.infer<typeof IdentityStatusSchema>;

export const MembershipRoleSchema = z.enum(["steward", "caregiver", "participant", "dependent"]);
export type MembershipRole = z.infer<typeof MembershipRoleSchema>;

/** Roles are labels. These explicit grants are the relationship-local authority. */
export const MembershipCapabilitySchema = z.enum([
  "household.read",
  "household.govern",
  "membership.invite",
  "membership.suspend",
  "conversation.manage",
  "coordination.originate",
  "coordination.coordinate",
  "source.use",
  "source.bridge",
  "action.approve",
  "data.export",
]);
export type MembershipCapability = z.infer<typeof MembershipCapabilitySchema>;

export const StewardCapabilities = MembershipCapabilitySchema.options;

export const IdentityPrincipalSchema = z.strictObject({
  personId: EntityIdSchema,
  identityId: EntityIdSchema,
  personStatus: PersonStatusSchema,
  identityStatus: IdentityStatusSchema,
  identityAuthorityVersion: z.number().int().positive(),
});
export type IdentityPrincipal = z.infer<typeof IdentityPrincipalSchema>;

export const ObserveIdentityInputSchema = z.strictObject({
  kind: IdentityKindSchema,
  issuer: z.string().trim().min(1).max(100),
  subjectDigest: DigestSchema,
  observedAt: InstantSchema,
});
export type ObserveIdentityInput = z.infer<typeof ObserveIdentityInputSchema>;

export const ClaimIdentityInputSchema = z
  .strictObject({
    identityId: EntityIdSchema,
    confirmedByIdentityId: EntityIdSchema,
    expectedIdentityAuthorityVersion: z.number().int().positive(),
    targetPersonId: EntityIdSchema.optional(),
    consentedAt: InstantSchema,
    timezone: z.string().trim().min(1).max(100),
  })
  .refine((claim) => claim.identityId === claim.confirmedByIdentityId, {
    message: "An identity claim requires confirmation from that exact private identity",
    path: ["confirmedByIdentityId"],
  });
export type ClaimIdentityInput = z.infer<typeof ClaimIdentityInputSchema>;

export const BindProviderAccountIdentityInputSchema = z.strictObject({
  personId: EntityIdSchema,
  expectedPersonControlEpoch: z.number().int().positive(),
  issuer: z.literal("google"),
  subjectDigest: DigestSchema,
  subject: z.string().trim().min(1).max(255),
  verifiedEmail: z.string().trim().email().max(320),
  boundAt: InstantSchema,
});
export type BindProviderAccountIdentityInput = z.infer<typeof BindProviderAccountIdentityInputSchema>;

export const RefreshProviderAccountIdentityLabelInputSchema = z.strictObject({
  personId: EntityIdSchema,
  expectedPersonControlEpoch: z.number().int().positive(),
  identityId: EntityIdSchema,
  expectedIdentityAuthorityVersion: z.number().int().positive(),
  issuer: z.literal("google"),
  subjectDigest: DigestSchema,
  verifiedEmail: z.string().trim().email().max(320),
  observedAt: InstantSchema,
});
export type RefreshProviderAccountIdentityLabelInput = z.infer<
  typeof RefreshProviderAccountIdentityLabelInputSchema
>;

export const ProviderAccountIdentityBindingSchema = z.strictObject({
  identity: IdentityPrincipalSchema,
  duplicate: z.boolean(),
});
export type ProviderAccountIdentityBinding = z.infer<typeof ProviderAccountIdentityBindingSchema>;

export const HouseholdMembershipSchema = z.strictObject({
  membershipId: EntityIdSchema,
  householdId: EntityIdSchema,
  personId: EntityIdSchema,
  role: MembershipRoleSchema,
  status: z.enum(["invited", "active", "suspended", "left", "revoked"]),
  capabilities: z.array(MembershipCapabilitySchema),
  version: z.number().int().positive(),
});
export type HouseholdMembership = z.infer<typeof HouseholdMembershipSchema>;

export const CreateHouseholdInputSchema = z.strictObject({
  founderPersonId: EntityIdSchema,
  timezone: z.string().trim().min(1).max(100),
  createdAt: InstantSchema,
});
export type CreateHouseholdInput = z.infer<typeof CreateHouseholdInputSchema>;

export const CreateHouseholdResultSchema = z.strictObject({
  householdId: EntityIdSchema,
  membership: HouseholdMembershipSchema,
  membershipVersion: z.number().int().positive(),
});
export type CreateHouseholdResult = z.infer<typeof CreateHouseholdResultSchema>;

export const InviteHouseholdMemberInputSchema = z
  .strictObject({
    householdId: EntityIdSchema,
    inviterPersonId: EntityIdSchema,
    inviteeIdentityId: EntityIdSchema.optional(),
    inviteeSubjectDigest: DigestSchema,
    tokenDigest: DigestSchema,
    requestedRole: MembershipRoleSchema,
    requestedCapabilities: z.array(MembershipCapabilitySchema).max(MembershipCapabilitySchema.options.length),
    expiresAt: InstantSchema,
    createdAt: InstantSchema,
  })
  .refine(
    (invitation) =>
      new Set(invitation.requestedCapabilities).size === invitation.requestedCapabilities.length,
    {
      message: "Requested capabilities must be unique",
      path: ["requestedCapabilities"],
    },
  );
export type InviteHouseholdMemberInput = z.infer<typeof InviteHouseholdMemberInputSchema>;

export const HouseholdInvitationSchema = z.strictObject({
  invitationId: EntityIdSchema,
  householdId: EntityIdSchema,
  requestedRole: MembershipRoleSchema,
  requestedCapabilities: z.array(MembershipCapabilitySchema),
  status: z.enum(["pending", "accepted", "declined", "expired", "revoked"]),
  householdMembershipVersion: z.number().int().positive(),
  requiredApproverPersonIds: z.array(EntityIdSchema),
  approvedByPersonIds: z.array(EntityIdSchema),
  expiresAt: InstantSchema,
});
export type HouseholdInvitation = z.infer<typeof HouseholdInvitationSchema>;

export const ApproveInvitationInputSchema = z.strictObject({
  invitationId: EntityIdSchema,
  approverPersonId: EntityIdSchema,
  approvedAt: InstantSchema,
});
export type ApproveInvitationInput = z.infer<typeof ApproveInvitationInputSchema>;

export const AcceptInvitationInputSchema = z.strictObject({
  invitationId: EntityIdSchema,
  inviteePersonId: EntityIdSchema,
  tokenDigest: DigestSchema,
  acceptedAt: InstantSchema,
});
export type AcceptInvitationInput = z.infer<typeof AcceptInvitationInputSchema>;

export const LeaveHouseholdInputSchema = z.strictObject({
  householdId: EntityIdSchema,
  personId: EntityIdSchema,
  leftAt: InstantSchema,
});
export type LeaveHouseholdInput = z.infer<typeof LeaveHouseholdInputSchema>;

export const SuspendCaregiverInputSchema = z.strictObject({
  householdId: EntityIdSchema,
  stewardPersonId: EntityIdSchema,
  caregiverPersonId: EntityIdSchema,
  suspendedAt: InstantSchema,
});
export type SuspendCaregiverInput = z.infer<typeof SuspendCaregiverInputSchema>;

export interface IdentityRelationships {
  observeIdentity(input: ObserveIdentityInput): Promise<IdentityPrincipal>;
  claimIdentity(input: ClaimIdentityInput): Promise<IdentityPrincipal>;
  bindProviderAccountIdentity(
    input: BindProviderAccountIdentityInput,
  ): Promise<ProviderAccountIdentityBinding>;
  createHousehold(input: CreateHouseholdInput): Promise<CreateHouseholdResult>;
  inviteMember(input: InviteHouseholdMemberInput): Promise<HouseholdInvitation>;
  approveInvitation(input: ApproveInvitationInput): Promise<HouseholdInvitation>;
  acceptInvitation(input: AcceptInvitationInput): Promise<HouseholdMembership>;
  leaveHousehold(input: LeaveHouseholdInput): Promise<HouseholdMembership>;
  suspendCaregiver(input: SuspendCaregiverInput): Promise<HouseholdMembership>;
}
