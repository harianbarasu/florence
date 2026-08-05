import { createHash } from "node:crypto";
import {
  CONSENT_DISCLOSURE_VERSION,
  TRANSFER_ADULT_CONSENT_DISCLOSURE,
} from "../application/consent-disclosures.js";
import { type ApplicationOutboxIntent, ApplicationOutboxIntentSchema } from "../application/index.js";
import type { PrivateCommandHandler } from "./provider-processor.js";
import type { InvitationTransferResolution } from "./runtime-store.js";

export interface InvitationTransferDirectory {
  resolveInvitationTransfer(input: {
    invitationId?: string;
    sourceHouseholdId: string;
    sourceAdultId: string;
    sourceBindingId: string;
    externalChatId: string;
    externalHandle: string;
  }): Promise<InvitationTransferResolution>;
}

export interface InvitationTransferOutbox {
  enqueueApplicationIntent(intent: ApplicationOutboxIntent): Promise<{ rowId: string }>;
}

/** Deterministic, private command flow for moving one Linq identity between household directories. */
export class InvitationTransferCommandService implements PrivateCommandHandler {
  public constructor(
    private readonly directory: InvitationTransferDirectory,
    private readonly outbox: InvitationTransferOutbox,
  ) {}

  public async handle(
    input: Parameters<PrivateCommandHandler["handle"]>[0],
  ): Promise<{ handled: boolean; classification?: string }> {
    const normalized = input.text.normalize("NFKC").trim().toLowerCase();
    const transferContext =
      input.replyTo?.responseContext?.kind === "invitation_transfer"
        ? input.replyTo.responseContext
        : undefined;

    if (transferContext !== undefined) {
      if (
        input.bindingId === undefined ||
        input.externalHandle === undefined ||
        transferContext.sourceBindingId !== input.bindingId
      ) {
        await this.queuePrivateMessage(
          input,
          "identity-changed",
          "I couldn’t verify that transfer request against this Florence conversation. No changes were made. Ask the inviting adult to send a new invitation, then send “join my pending Florence household” here.",
        );
        return { handled: true, classification: "invitation_transfer:identity_changed" };
      }
      if (isTransferDecline(normalized)) {
        await this.queuePrivateMessage(
          input,
          "declined",
          "No changes were made. This iMessage identity remains connected to its current Florence household.",
        );
        return { handled: true, classification: "invitation_transfer:declined" };
      }
      const transfer = await this.directory.resolveInvitationTransfer({
        invitationId: transferContext.invitationId,
        sourceHouseholdId: input.householdId,
        sourceAdultId: input.adultId,
        sourceBindingId: input.bindingId,
        externalChatId: input.channelId,
        externalHandle: input.externalHandle,
      });
      if (transfer.status === "unavailable" && transfer.reason === "source_connections_active") {
        await this.queuePrivateMessage(
          input,
          "source-connections-active-saga",
          transferDisclosureMessage(
            "This Florence identity still has one or more Google accounts connected to its current household, so the transfer is paused and no identity data was moved. In this private DM, send “disconnect all Google accounts” and wait for Florence to confirm before accepting the transfer.",
          ),
          transferContext,
        );
        return { handled: true, classification: "invitation_transfer:source_connections_active" };
      }
      if (transfer.status === "unavailable") {
        await this.queuePrivateMessage(
          input,
          "state-changed",
          "I couldn’t safely complete that transfer because the invitation or account state changed. No identity or household data was moved. Ask the inviting adult to send a new invitation, then send “join my pending Florence household” here.",
        );
        return { handled: true, classification: `invitation_transfer:${transfer.reason}` };
      }
      await this.queuePrivateMessage(
        input,
        "confirmation-needed",
        transferDisclosureMessage(
          isTransferConfirmation(normalized)
            ? "No changes have been made. The prior acceptance could not be bound to a current, fully disclosed transfer offer."
            : "No changes have been made. Review this transfer offer before deciding.",
        ),
        transferContext,
      );
      return { handled: true, classification: "invitation_transfer:confirmation_needed" };
    }

    if (!isTransferJoinCommand(normalized)) return { handled: false };
    if (input.bindingId === undefined || input.externalHandle === undefined) {
      await this.queuePrivateMessage(
        input,
        "identity-unavailable",
        "I couldn’t verify this Florence identity for a household transfer. No changes were made. Please try again from your usual private Florence conversation.",
      );
      return { handled: true, classification: "invitation_transfer:identity_unavailable" };
    }
    const transfer = await this.directory.resolveInvitationTransfer({
      sourceHouseholdId: input.householdId,
      sourceAdultId: input.adultId,
      sourceBindingId: input.bindingId,
      externalChatId: input.channelId,
      externalHandle: input.externalHandle,
    });
    if (transfer.status === "unavailable") {
      if (transfer.reason === "source_connections_active") {
        await this.queuePrivateMessage(
          input,
          "source-connections-active",
          "This Florence identity still has one or more Google accounts connected to its current household, so I won’t transfer it yet. In this private DM, send “disconnect all Google accounts,” wait for Florence to confirm the disconnect, then send “join my pending Florence household” again. No account or household data was moved.",
        );
        return { handled: true, classification: "invitation_transfer:source_connections_active" };
      }
      await this.queuePrivateMessage(
        input,
        `unavailable-${transfer.reason}`,
        "I couldn’t find one current invitation that can safely be joined from this Florence conversation. No changes were made. Ask the inviting adult to send a new invitation, then try again here.",
      );
      return { handled: true, classification: `invitation_transfer:${transfer.reason}` };
    }

    await this.queuePrivateMessage(
      input,
      "offer",
      transferDisclosureMessage(
        "Florence found one current invitation for this iMessage identity to join another household. No changes have been made.",
      ),
      {
        kind: "invitation_transfer",
        invitationId: transfer.invitationId,
        sourceBindingId: input.bindingId,
        consentDisclosureVersion: CONSENT_DISCLOSURE_VERSION,
      },
    );
    return { handled: true, classification: "invitation_transfer:offered" };
  }

  private async queuePrivateMessage(
    input: Pick<Parameters<PrivateCommandHandler["handle"]>[0], "householdId" | "adultId" | "idempotencyKey">,
    suffix: string,
    body: string,
    responseContext?: Extract<
      NonNullable<Parameters<PrivateCommandHandler["handle"]>[0]["replyTo"]>["responseContext"],
      { kind: "invitation_transfer" }
    >,
  ): Promise<void> {
    const intentId = `system_${stableId(`${input.idempotencyKey}:invitation-transfer:${suffix}`)}`;
    await this.outbox.enqueueApplicationIntent(
      ApplicationOutboxIntentSchema.parse({
        intentId,
        householdId: input.householdId,
        idempotencyKey: `florence:${intentId}`,
        kind: "conversation.send",
        targetScope: { kind: "personal", adultId: input.adultId },
        messageClass: "onboarding",
        ...(responseContext === undefined ? {} : { responseContext }),
        body,
      }),
    );
  }
}

function transferDisclosureMessage(prefix: string): string {
  return `${prefix}\n\n${TRANSFER_ADULT_CONSENT_DISCLOSURE}`;
}

export function isTransferConfirmation(text: string): boolean {
  return /^i accept and confirm transfer[.!]?$/u.test(text);
}

function isTransferDecline(text: string): boolean {
  return /^i decline transfer[.!]?$/u.test(text);
}

function isTransferJoinCommand(text: string): boolean {
  return /^join my pending florence (?:household|family)[.!]?$/u.test(text);
}

function stableId(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 32);
}
