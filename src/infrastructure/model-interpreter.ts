import type {
  ApplicationInterpreterPort,
  ConversationInterpretationContext,
  GmailTriageContext,
} from "../application/index.js";
import {
  ConversationClassificationSchema,
  type ConversationInboxItem,
  type GmailInboxItem,
  GmailTriageResultSchema,
} from "../application/index.js";
import type { ModelCapabilityProfile, ModelCompletionResult, ModelGateway } from "../models/index.js";

const CONVERSATION_SYSTEM_PROMPT = `You are Florence's household-intake classifier. Return exactly the supplied schema.

Florence is an adult-only, iMessage-first family Chief of Staff. Classify the message using only the supplied household context and stable IDs. Ordinary conversation is ignore. A commitment must materially affect the household, dependents, shared resources, or family obligations. Research and meal planning are request-led: never initiate them from mere interest. A mixed work/personal request may include only its household consequence. Never assign blame. Never treat silence as ownership, approval, or completion. Never invent an adult, episode, date, deadline, owner, consent, or completed action. Use onboarding actions only when the message explicitly performs the valid next step. While onboarding is building_profile, an explicit household-group description of dependents, school or childcare, recurring activities, routine anchors, or dietary constraints is update_profile; extract only directly stated facts, keep each subject stable enough for later correction, and never infer an unknown detail. After activation, use update_profile only for an explicit shared-profile addition or correction, not ordinary conversation. A personal-DM message can never update the shared profile. For a pending private Gmail promotion, approve_promotion is one-time by default; set rememberForMatchingSource true only when the adult explicitly says always, future, remember, or create a rule. Never infer a standing rule from yes, approve, or silence. Use revoke_policy only when the adult explicitly revokes one of the supplied activePolicies, preserving its exact ID and version. Use daily_brief_request only for an explicit request. If evidence is ambiguous, choose ignore with an honest rationale; the application can ask safely later.`;

const GMAIL_SYSTEM_PROMPT = `You are Florence's private Gmail triage classifier. Return exactly the supplied schema.

The mailbox belongs to one adult and is private by default. Ignore spam, trash, promotions, newsletters without a current family consequence, unrelated work, and stale noise. Retain or review useful private context without sharing it. Interrupt privately only for a genuinely time-sensitive family risk. A proposed family episode requires a concrete household outcome. Medical, financial, employment, legal, relationship, access-code, and similarly sensitive content must remain private unless the adult later approves a minimum-meaning promotion. minimumHouseholdMeaning must disclose only what another adult needs to coordinate the family outcome; never copy private email text, access details, diagnoses, money amounts, work content, or unrelated names. A trusted sender is not blanket sharing authority. Never invent a deadline, owner, or event.`;

export class ModelApplicationInterpreter implements ApplicationInterpreterPort {
  public constructor(private readonly gateway: ModelGateway) {}

  public async interpretConversation(
    input: ConversationInboxItem,
    context: ConversationInterpretationContext,
  ): Promise<unknown> {
    const result = await this.gateway.complete("classification_extraction", {
      messages: [
        { role: "system", parts: [{ type: "text", text: CONVERSATION_SYSTEM_PROMPT }] },
        {
          role: "user",
          parts: [
            {
              type: "text",
              text: JSON.stringify({
                task: "classify_household_conversation",
                message: input,
                context,
              }),
            },
          ],
        },
      ],
      responseSchema: ConversationClassificationSchema,
      responseSchemaName: "florence_conversation_classification",
      maxOutputTokens: 4_000,
    });
    return structuredValue(result, "classification_extraction");
  }

  public async triageGmail(input: GmailInboxItem, context: GmailTriageContext): Promise<unknown> {
    const result = await this.gateway.complete("private_processing", {
      messages: [
        { role: "system", parts: [{ type: "text", text: GMAIL_SYSTEM_PROMPT }] },
        {
          role: "user",
          parts: [
            {
              type: "text",
              text: JSON.stringify({
                task: "triage_private_gmail",
                mailboxOwnerAdultId: input.ownerAdultId,
                message: input,
                approvedSharingRules: context.activeSharingRules,
              }),
            },
          ],
        },
      ],
      responseSchema: GmailTriageResultSchema,
      responseSchemaName: "florence_private_gmail_triage",
      maxOutputTokens: 4_000,
    });
    return structuredValue(result, "private_processing");
  }
}

function structuredValue(result: ModelCompletionResult, profile: ModelCapabilityProfile): unknown {
  const values = result.content.filter((part) => part.type === "structured_result");
  if (values.length !== 1 || values[0]?.type !== "structured_result") {
    throw new Error(`Florence model route did not return one structured result for ${profile}`);
  }
  return values[0].value;
}
