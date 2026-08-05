import type {
  ApplicationInterpreterPort,
  ConversationInboxItem,
  ConversationInterpretationContext,
  GmailInboxItem,
  GmailTriageContext,
} from "../application/index.js";
import { canonicalizeLinqHandle, type PreparedInvitation } from "./runtime-store.js";

export interface OnboardingInvitationDirectory {
  prepareInvitation(input: {
    householdId: string;
    invitedByAdultId: string;
    inviteeHandle: string;
    expiresAt: string;
  }): Promise<PreparedInvitation>;
}

/** Keeps identity creation deterministic while delegating semantic household work to the model. */
export class OnboardingAwareInterpreter implements ApplicationInterpreterPort {
  public constructor(
    private readonly model: ApplicationInterpreterPort,
    private readonly invitations: OnboardingInvitationDirectory,
    private readonly now: () => Date = () => new Date(),
  ) {}

  public async interpretConversation(
    input: ConversationInboxItem,
    context: ConversationInterpretationContext,
  ): Promise<unknown> {
    const text = input.text.normalize("NFKC").trim();
    const normalized = text.toLowerCase();
    const onboarding = context.onboarding;

    if (
      onboarding.phase === "awaiting_initiator_consent" &&
      input.channel.scope === "personal" &&
      input.senderAdultId === onboarding.initiatorAdultId
    ) {
      return explicitConsent(normalized)
        ? onboardingClassification("consent", "The initiating adult explicitly consented.")
        : preConsentIgnore("The initiating adult has not explicitly consented yet.");
    }

    if (
      onboarding.phase === "awaiting_invitation" &&
      input.channel.scope === "personal" &&
      input.senderAdultId === onboarding.initiatorAdultId &&
      /\b(?:invite|add)\b/u.test(normalized)
    ) {
      const handle = extractInviteeHandle(text);
      if (handle !== null) {
        const invitation = await this.invitations.prepareInvitation({
          householdId: input.householdId,
          invitedByAdultId: input.senderAdultId,
          inviteeHandle: handle,
          expiresAt: new Date(this.now().getTime() + 7 * 24 * 60 * 60_000).toISOString(),
        });
        return {
          ...onboardingClassification(
            "invite_adult",
            "The initiating adult explicitly invited one identified adult.",
          ),
          invitedAdultId: invitation.adultId,
        };
      }
    }

    if (
      onboarding.phase === "awaiting_invitee_consent" &&
      input.channel.scope === "personal" &&
      input.senderAdultId === onboarding.invitedAdultId
    ) {
      return explicitInviteAcceptance(normalized)
        ? onboardingClassification(
            "accept_invite",
            "The invited adult explicitly accepted and consented in their own DM.",
          )
        : preConsentIgnore("The invited adult has not explicitly accepted and consented yet.");
    }

    if (
      onboarding.phase === "awaiting_group" &&
      input.channel.scope === "household" &&
      /\b(?:connect|register|family|household)\b/u.test(normalized)
    ) {
      return onboardingClassification(
        "register_group",
        "A consented adult explicitly identified the verified household group.",
      );
    }

    if (
      onboarding.phase === "building_profile" &&
      input.channel.scope === "household" &&
      /\b(?:confirm|approve|looks good)\b/u.test(normalized) &&
      /\b(?:profile|routines?|details?|anchors?|looks good)\b/u.test(normalized)
    ) {
      return onboardingClassification(
        "confirm_profile",
        "A verified adult explicitly confirmed the shared profile.",
      );
    }

    return this.model.interpretConversation(input, context);
  }

  public triageGmail(input: GmailInboxItem, context: GmailTriageContext): Promise<unknown> {
    return this.model.triageGmail(input, context);
  }
}

export function extractInviteeHandle(text: string): string | null {
  const email = text.match(/[\p{L}\p{N}.!#$%&'*+/=?^_`{|}~-]+@[\p{L}\p{N}.-]+\.[\p{L}]{2,63}/iu)?.[0];
  if (email) return canonicalizeLinqHandle(email);
  const candidate = text.match(/\+[1-9](?:[\s().-]*\d){7,14}/u)?.[0];
  if (!candidate) return null;
  try {
    return canonicalizeLinqHandle(candidate);
  } catch {
    return null;
  }
}

function explicitConsent(text: string): boolean {
  return /^(?:i\s+(?:consent|agree)|i\s+agree\s+and\s+consent|yes,?\s+i\s+(?:consent|agree))\b/u.test(text);
}

function explicitInviteAcceptance(text: string): boolean {
  return /^(?:i\s+(?:accept|consent|agree)|accept|yes,?\s+i\s+(?:accept|consent|agree))\b/u.test(text);
}

function onboardingClassification(
  action: "consent" | "invite_adult" | "accept_invite" | "register_group" | "confirm_profile",
  rationale: string,
) {
  return { intent: "onboarding" as const, action, confidence: 1, rationale };
}

function preConsentIgnore(rationale: string) {
  return { intent: "ignore" as const, confidence: 1, rationale };
}
