import { createHash } from "node:crypto";
import { GOOGLE_CALENDAR_READONLY_SCOPE, GOOGLE_GMAIL_READONLY_SCOPE } from "../adapters/google/index.js";
import { type ApplicationOutboxIntent, ApplicationOutboxIntentSchema } from "../application/index.js";
import type { CalendarSyncWork } from "./google-calendar-sync.js";
import type { GmailSyncWork, GoogleSyncConnection } from "./google-sync.js";
import type { GoogleOAuthConnectedEvent, GoogleOAuthFailedEvent } from "./http-services.js";
import type { PrivateCommandHandler } from "./provider-processor.js";

/** Integration-independent private router; optional connectors contribute handlers without owning control. */
export class PrivateCommandRouter implements PrivateCommandHandler {
  public constructor(private readonly handlers: readonly PrivateCommandHandler[]) {
    if (handlers.length === 0) throw new Error("Private command routing requires a core handler");
  }

  public async handle(
    input: Parameters<PrivateCommandHandler["handle"]>[0],
  ): Promise<{ handled: boolean; classification?: string }> {
    for (const handler of this.handlers) {
      const result = await handler.handle(input);
      if (result.handled) return result;
    }
    return { handled: false };
  }
}

export interface PrivateCommandOutbox {
  enqueueApplicationIntent(intent: ApplicationOutboxIntent): Promise<{ rowId: string }>;
}

export interface PrivateCommandGoogleDirectory {
  listOwnedGoogleConnections(input: {
    householdId: string;
    adultId: string;
    includeRevoked?: boolean;
  }): Promise<readonly GoogleSyncConnection[]>;
  listOwnedGoogleCalendars?(input: { householdId: string; adultId: string }): Promise<
    readonly {
      connectionId: string;
      connectionLabel: string;
      calendarId: string;
      displayName: string;
      status: "active" | "excluded" | "deleted";
      primary: boolean;
    }[]
  >;
  setOwnedGoogleCalendarEnabled?(input: {
    householdId: string;
    adultId: string;
    connectionId: string;
    calendarId: string;
    enabled: boolean;
  }): Promise<"updated" | "not_found" | "primary_required" | "unavailable">;
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

    const calendarSelection = normalized.match(
      /^(include|sync|track|enable|exclude|ignore|disable)\s+(?:the\s+)?calendar\s+["“]?(.+?)["”]?$/u,
    );
    if (
      calendarSelection &&
      this.options.directory.listOwnedGoogleCalendars &&
      this.options.directory.setOwnedGoogleCalendarEnabled
    ) {
      const enabled = !["exclude", "ignore", "disable"].includes(calendarSelection[1] as string);
      const requestedName = (calendarSelection[2] as string).trim().toLocaleLowerCase("en-US");
      const calendars = await this.options.directory.listOwnedGoogleCalendars({
        householdId: input.householdId,
        adultId: input.adultId,
      });
      const matches = calendars.filter((calendar) => {
        const displayName = calendar.displayName.trim().toLocaleLowerCase("en-US");
        const qualified = `${calendar.connectionLabel} / ${calendar.displayName}`
          .trim()
          .toLocaleLowerCase("en-US");
        return requestedName === displayName || requestedName === qualified;
      });
      if (matches.length !== 1) {
        const explanation =
          matches.length === 0
            ? `I could not find a calendar named “${calendarSelection[2]}”.`
            : `“${calendarSelection[2]}” matches more than one account.`;
        await this.queuePrivateMessage(
          input,
          "google-calendar-selection-ambiguous",
          `${explanation} Say the exact “account / calendar” name from “show my calendars.”`,
        );
        return { handled: true, classification: "google:calendar_selection_ambiguous" };
      }
      const calendar = matches[0] as (typeof matches)[number];
      const result = await this.options.directory.setOwnedGoogleCalendarEnabled({
        householdId: input.householdId,
        adultId: input.adultId,
        connectionId: calendar.connectionId,
        calendarId: calendar.calendarId,
        enabled,
      });
      if (result === "updated" && enabled && this.options.calendarQueue) {
        await this.options.calendarQueue.enqueueCalendarSyncWork({
          householdId: input.householdId,
          idempotencyKey: `${input.idempotencyKey}:calendar-enable`,
          work: {
            kind: "start",
            householdId: input.householdId,
            adultId: input.adultId,
            connectionId: calendar.connectionId,
            calendarId: calendar.calendarId,
          },
        });
      }
      const body =
        result === "primary_required"
          ? "Florence must keep your primary calendar enabled so availability does not silently become incomplete."
          : result === "unavailable"
            ? `“${calendar.displayName}” is no longer available from Google. Re-add it there first.`
            : result === "not_found"
              ? "That calendar changed while I was updating it. Say “show my calendars” and try again."
              : enabled
                ? `I’ll privately include “${calendar.displayName}” from ${calendar.connectionLabel}. I’m synchronizing it now.`
                : `I’ll stop using “${calendar.displayName}” from ${calendar.connectionLabel}. Its private availability projection has been removed.`;
      await this.queuePrivateMessage(input, "google-calendar-selection", body);
      return { handled: true, classification: `google:calendar_${enabled ? "included" : "excluded"}` };
    }

    if (
      /^(?:list|show|which|what)\b[\s\S]*\bcalendars?\b/u.test(normalized) &&
      this.options.directory.listOwnedGoogleCalendars
    ) {
      const calendars = await this.options.directory.listOwnedGoogleCalendars({
        householdId: input.householdId,
        adultId: input.adultId,
      });
      const body =
        calendars.length === 0
          ? "I have not discovered any Google calendars yet. If you just connected, synchronization may still be starting."
          : `Your private calendar coverage:\n${calendars
              .map(
                (calendar) =>
                  `• ${calendar.connectionLabel} / ${calendar.displayName} — ${
                    calendar.status === "active" ? "included" : calendar.status
                  }${calendar.primary ? " (primary)" : ""}`,
              )
              .join("\n")}\n\nUse “include calendar account / name” or “exclude calendar account / name.”`;
      await this.queuePrivateMessage(input, "google-calendar-list", body);
      return { handled: true, classification: "google:calendars_listed" };
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
    const gmailGranted = event.grantedScopes.includes(GOOGLE_GMAIL_READONLY_SCOPE);
    const calendarGranted = event.grantedScopes.includes(GOOGLE_CALENDAR_READONLY_SCOPE);
    const gmailReady = this.options.gmailSyncEnabled !== false && gmailGranted;
    const calendarReady = this.options.calendarQueue !== undefined && calendarGranted;
    if (gmailReady) {
      await this.options.googleQueue.enqueueGoogleSyncWork({
        householdId: event.householdId,
        idempotencyKey: `google:${event.connectionId}:activation:${event.activationId}:${
          event.hadPriorGmailState ? "continue" : "start"
        }`,
        work: event.hadPriorGmailState
          ? {
              kind: "continue",
              householdId: event.householdId,
              adultId: event.adultId,
              connectionId: event.connectionId,
            }
          : {
              kind: "start",
              householdId: event.householdId,
              adultId: event.adultId,
              connectionId: event.connectionId,
              depth: "full_history",
            },
      });
    }
    if (!gmailReady || !calendarReady) {
      const missing = [
        ...(gmailGranted
          ? this.options.gmailSyncEnabled === false
            ? ["Florence's Gmail synchronization service"]
            : []
          : ["Gmail read permission"]),
        ...(calendarGranted
          ? this.options.calendarQueue === undefined
            ? ["Florence's Calendar synchronization service"]
            : []
          : ["Calendar read permission"]),
      ];
      await this.queuePrivateMessage(
        {
          householdId: event.householdId,
          adultId: event.adultId,
          idempotencyKey: `google:${event.connectionId}:activation:${event.activationId}:connected-incomplete`,
        },
        "google-connected-incomplete",
        `${event.accountLabel} connected${event.email ? ` as ${event.email}` : ""}, but setup is not complete because ${missing.join(" and ")} ${missing.length === 1 ? "is" : "are"} unavailable. I have not marked your household onboarding complete. Reconnect after that is resolved.`,
      );
      return;
    }
    await this.queuePrivateMessage(
      {
        householdId: event.householdId,
        adultId: event.adultId,
        idempotencyKey: `google:${event.connectionId}:activation:${event.activationId}:connected`,
      },
      "google-connected",
      `${event.accountLabel} is connected${event.email ? ` as ${event.email}` : ""}. Florence is ${
        event.hadPriorGmailState
          ? "resuming mail from its durable cursor"
          : "starting mail with the most recent 90 days, then working backward through one year and older history without delaying new mail"
      }. I’m also privately discovering your primary and selected Google calendars, including shared calendars you show in Google. Say “show my calendars” after synchronization to review coverage. Raw mail and calendar details stay private unless you approve a minimum household meaning or a sharing rule.`,
    );
  }

  public async onGoogleConnectionFailed(event: GoogleOAuthFailedEvent): Promise<void> {
    const account = event.accountLabel ?? "Google account";
    const body =
      event.reason === "declined"
        ? `${account} was not connected because permission was declined. Nothing was shared. When you're ready, reply “connect my Google account” here for a fresh private link.`
        : event.reason === "invalid"
          ? `${account} was not connected because the private handoff could not be verified. Nothing was shared. Reply “connect my Google account” here for a fresh link.`
          : `${account} could not be connected because Google or Florence was temporarily unavailable. Nothing was shared. Reply “connect my Google account” here to try again.`;
    await this.queuePrivateMessage(
      {
        householdId: event.householdId,
        adultId: event.adultId,
        idempotencyKey: `google-oauth:${event.failureRef}:${event.reason}`,
      },
      "google-connect-failed",
      body,
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
