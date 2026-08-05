import { createHash } from "node:crypto";
import { type ApplicationOutboxIntent, ApplicationOutboxIntentSchema } from "../application/index.js";
import type { CalendarSyncWork } from "./google-calendar-sync.js";
import type { GmailSyncWork, GoogleSyncConnection } from "./google-sync.js";
import type { GoogleOAuthConnectedEvent } from "./http-services.js";
import type { PrivateCommandHandler } from "./provider-processor.js";

export interface PrivateCommandOutbox {
  enqueueApplicationIntent(intent: ApplicationOutboxIntent): Promise<{ rowId: string }>;
}

export interface PrivateCommandGoogleDirectory {
  listOwnedGoogleConnections(input: {
    householdId: string;
    adultId: string;
    includeRevoked?: boolean;
  }): Promise<readonly GoogleSyncConnection[]>;
}

export interface PrivateCommandGoogleQueue {
  enqueueGoogleSyncWork(input: {
    idempotencyKey: string;
    householdId: string;
    work: GmailSyncWork;
  }): Promise<{ jobId: string; created: boolean }>;
}

export interface PrivateCommandCalendarQueue {
  enqueueCalendarSyncWork(input: {
    idempotencyKey: string;
    householdId: string;
    work: CalendarSyncWork;
  }): Promise<{ jobId: string; created: boolean }>;
}

export interface GoogleLinkIssuer {
  issue(input: {
    householdId: string;
    adultId: string;
    returnConversationId: string;
    accountLabel: string;
    loginHint?: string;
  }): string;
}

export interface PrivateGoogleCommandServiceOptions {
  outbox: PrivateCommandOutbox;
  directory: PrivateCommandGoogleDirectory;
  googleQueue: PrivateCommandGoogleQueue;
  calendarQueue?: PrivateCommandCalendarQueue;
  linkIssuer: GoogleLinkIssuer;
  gmailSyncEnabled?: boolean;
}

export class PrivateGoogleCommandService implements PrivateCommandHandler {
  public constructor(private readonly options: PrivateGoogleCommandServiceOptions) {}

  public async handle(input: {
    householdId: string;
    adultId: string;
    channelId: string;
    messageId: string;
    text: string;
    occurredAt: string;
    idempotencyKey: string;
  }): Promise<{ handled: boolean; classification?: string }> {
    const text = input.text.normalize("NFKC").trim();
    const normalized = text.toLowerCase();
    if (/^(?:connect|link|add)\b[\s\S]*\b(?:google|gmail|calendar)\b/u.test(normalized)) {
      const label = accountLabel(normalized);
      const loginHint = text.match(/[\p{L}\p{N}.!#$%&'*+/=?^_`{|}~-]+@[\p{L}\p{N}.-]+\.[\p{L}]{2,63}/iu)?.[0];
      const url = this.options.linkIssuer.issue({
        householdId: input.householdId,
        adultId: input.adultId,
        returnConversationId: input.channelId,
        accountLabel: label,
        ...(loginHint ? { loginHint: loginHint.toLowerCase() } : {}),
      });
      await this.queuePrivateMessage(
        input,
        "google-connect",
        `Open this private, time-limited link to connect your ${label}: ${url}\n\nThe account remains yours and private by default. Florence will ask before sharing family meaning unless you later approve a rule.`,
      );
      return { handled: true, classification: "google:connect_link_issued" };
    }

    if (/^(?:list|show|which|what)\b[\s\S]*\b(?:google|gmail|calendar)\b/u.test(normalized)) {
      const connections = await this.options.directory.listOwnedGoogleConnections({
        householdId: input.householdId,
        adultId: input.adultId,
      });
      const body =
        connections.length === 0
          ? "You do not have an active Google account connected."
          : `Your private Google connections:\n${connections
              .map((connection) => `• ${connectionLabel(connection)}`)
              .join("\n")}`;
      await this.queuePrivateMessage(input, "google-list", body);
      return { handled: true, classification: "google:connections_listed" };
    }

    if (
      /^(?:disconnect|unlink|revoke|remove)\b[\s\S]*\b(?:google|gmail|calendar|account)\b/u.test(normalized)
    ) {
      const connections = await this.options.directory.listOwnedGoogleConnections({
        householdId: input.householdId,
        adultId: input.adultId,
      });
      const selected = selectConnections(connections, normalized);
      if (selected.length === 0) {
        await this.queuePrivateMessage(
          input,
          "google-disconnect-none",
          connections.length === 0
            ? "There is no active Google account to disconnect."
            : `Name the account label or email to disconnect. Your active connections are: ${connections
                .map(connectionLabel)
                .join(", ")}.`,
        );
        return { handled: true, classification: "google:disconnect_needs_account" };
      }
      for (const connection of selected) {
        const idempotencyKey = `google:${connection.id}:revoke:${stableId(input.idempotencyKey)}`;
        if (this.options.calendarQueue !== undefined) {
          await this.options.calendarQueue.enqueueCalendarSyncWork({
            householdId: input.householdId,
            idempotencyKey,
            work: {
              kind: "revoke",
              householdId: input.householdId,
              adultId: input.adultId,
              connectionId: connection.id,
              calendarId: "primary",
            },
          });
        } else {
          await this.options.googleQueue.enqueueGoogleSyncWork({
            householdId: input.householdId,
            idempotencyKey,
            work: {
              kind: "revoke",
              householdId: input.householdId,
              adultId: input.adultId,
              connectionId: connection.id,
            },
          });
        }
      }
      await this.queuePrivateMessage(
        input,
        "google-disconnect",
        `Disconnecting ${selected.map(connectionLabel).join(", ")}. Florence will stop access locally even if Google is temporarily unavailable.`,
      );
      return { handled: true, classification: "google:disconnect_queued" };
    }

    return { handled: false };
  }

  public async onGoogleConnected(event: GoogleOAuthConnectedEvent): Promise<void> {
    if (this.options.gmailSyncEnabled !== false) {
      await this.options.googleQueue.enqueueGoogleSyncWork({
        householdId: event.householdId,
        idempotencyKey: `google:${event.connectionId}:start`,
        work: {
          kind: "start",
          householdId: event.householdId,
          adultId: event.adultId,
          connectionId: event.connectionId,
          depth: "full_history",
        },
      });
    }
    await this.queuePrivateMessage(
      {
        householdId: event.householdId,
        adultId: event.adultId,
        idempotencyKey: `google:${event.connectionId}:connected`,
      },
      "google-connected",
      this.options.gmailSyncEnabled === false
        ? `${event.accountLabel} is connected${event.email ? ` as ${event.email}` : ""}. Florence is privately synchronizing your primary calendar. Calendar details stay personal unless you explicitly promote family meaning.`
        : `${event.accountLabel} is connected${event.email ? ` as ${event.email}` : ""}. Florence is starting mail with the most recent 90 days, then will work backward through one year and older history without delaying new mail, while privately synchronizing your primary calendar. Raw mail and calendar details stay private unless you approve a minimum household meaning or a sharing rule.`,
    );
  }

  private async queuePrivateMessage(
    input: { householdId: string; adultId: string; idempotencyKey: string },
    suffix: string,
    body: string,
  ): Promise<void> {
    const intentId = `system_${stableId(`${input.idempotencyKey}:${suffix}`)}`;
    await this.options.outbox.enqueueApplicationIntent(
      ApplicationOutboxIntentSchema.parse({
        intentId,
        householdId: input.householdId,
        idempotencyKey: `florence:${intentId}`,
        kind: "conversation.send",
        targetScope: { kind: "personal", adultId: input.adultId },
        messageClass: "status",
        body,
      }),
    );
  }
}

function selectConnections(
  connections: readonly GoogleSyncConnection[],
  normalizedCommand: string,
): GoogleSyncConnection[] {
  if (/\b(?:all|every)\b/u.test(normalizedCommand)) return [...connections];
  const matching = connections.filter((connection) => {
    const label = String(connection.metadata.accountLabel ?? "").toLowerCase();
    return (
      (connection.email !== null && normalizedCommand.includes(connection.email.toLowerCase())) ||
      (label.length > 0 && normalizedCommand.includes(label))
    );
  });
  if (matching.length > 0) return matching;
  return connections.length === 1 ? [connections[0] as GoogleSyncConnection] : [];
}

function connectionLabel(connection: GoogleSyncConnection): string {
  const label = String(connection.metadata.accountLabel ?? "Google account");
  return connection.email ? `${label} (${connection.email})` : label;
}

function accountLabel(command: string): string {
  if (/\bwork\b/u.test(command)) return "work Google account";
  if (/\bpersonal\b/u.test(command)) return "personal Google account";
  return "Google account";
}

function stableId(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
