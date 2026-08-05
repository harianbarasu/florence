import { createHash } from "node:crypto";
import { GOOGLE_CALENDAR_READONLY_SCOPE, GOOGLE_GMAIL_READONLY_SCOPE } from "../adapters/google/index.js";
import { type ApplicationOutboxIntent, ApplicationOutboxIntentSchema } from "../application/index.js";
import {
  canonicalGoogleAccountAlias,
  googleAccountAliasKey,
  normalizedEmail,
} from "../security/durable-identity-privacy.js";
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

export interface PrivateGoogleCalendar {
  connectionId: string;
  connectionAlias: string;
  connectionEmail: string | null;
  calendarId: string;
  displayName: string;
  status: "active" | "excluded" | "deleted";
  primary: boolean;
}

export interface PrivateCommandGoogleDirectory {
  listOwnedGoogleConnections(input: {
    householdId: string;
    adultId: string;
    includeRevoked?: boolean;
  }): Promise<readonly GoogleSyncConnection[]>;
  listOwnedGoogleCalendars?(input: {
    householdId: string;
    adultId: string;
  }): Promise<readonly PrivateGoogleCalendar[]>;
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
      const parsed = parseGoogleConnectCommand(text);
      if (parsed === null) {
        await this.queuePrivateMessage(
          input,
          "google-connect-needs-alias",
          "Give this Google account a short private alias using an exact command like “connect Google as Acme work”. Aliases can be up to 80 characters and cannot contain slashes.",
        );
        return { handled: true, classification: "google:connect_needs_alias" };
      }
      const connections = await this.options.directory.listOwnedGoogleConnections({
        householdId: input.householdId,
        adultId: input.adultId,
      });
      const loginHint = parsed.loginHint;
      const sameEmailConnection =
        loginHint === undefined
          ? undefined
          : connections.find(
              (connection) =>
                connection.email !== null && normalizedEmail(connection.email) === normalizedEmail(loginHint),
            );
      const aliasKey = googleAccountAliasKey(parsed.accountAlias);
      const aliasCollision = connections.find(
        (connection) =>
          connection.id !== sameEmailConnection?.id &&
          googleAccountAliasKey(connectionAlias(connection)) === aliasKey,
      );
      if (aliasCollision !== undefined) {
        await this.queuePrivateMessage(
          input,
          "google-connect-alias-in-use",
          `“${parsed.accountAlias}” is already the private alias for ${privateConnectionLabel(aliasCollision)}. No link was issued. Choose a distinct exact alias, for example “connect Google as Acme work”.${privateConnectionList(connections)}`,
        );
        return { handled: true, classification: "google:connect_alias_in_use" };
      }
      const url = this.options.linkIssuer.issue({
        householdId: input.householdId,
        adultId: input.adultId,
        returnConversationId: input.channelId,
        accountLabel: parsed.accountAlias,
        ...(parsed.loginHint ? { loginHint: parsed.loginHint } : {}),
      });
      await this.queuePrivateMessage(
        input,
        "google-connect",
        `Open this private, time-limited link to connect “${parsed.accountAlias}”: ${url}\n\nThe account and verified email remain yours and private by default. Florence will ask before sharing family meaning unless you later approve a rule.${privateConnectionList(connections)}`,
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
      const requestedName = normalizePrivateSelector(calendarSelection[2] as string);
      const calendars = await this.options.directory.listOwnedGoogleCalendars({
        householdId: input.householdId,
        adultId: input.adultId,
      });
      const matches = calendars.filter((calendar) => {
        return privateCalendarSelectors(calendar).includes(requestedName);
      });
      if (matches.length !== 1) {
        const explanation =
          matches.length === 0
            ? `I could not find a calendar named “${calendarSelection[2]}”.`
            : `“${calendarSelection[2]}” matches more than one account.`;
        const choices = matches.length === 0 ? calendars : matches;
        await this.queuePrivateMessage(
          input,
          "google-calendar-selection-ambiguous",
          `${explanation} Use one exact private name from “show my calendars.”${privateCalendarList(choices, 12)}`,
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
                ? `I’ll privately include “${calendar.displayName}” from ${privateCalendarAccountLabel(calendar)}. I’m synchronizing it now.`
                : `I’ll stop using “${calendar.displayName}” from ${privateCalendarAccountLabel(calendar)}. Its private availability projection has been removed.`;
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
                  `• ${privateCalendarQualifiedName(calendar)} — ${
                    calendar.status === "active" ? "included" : calendar.status
                  }${calendar.primary ? " (primary)" : ""}`,
              )
              .join(
                "\n",
              )}\n\nUse the exact private name, as in “include calendar account / name” or “exclude calendar account / name.”`;
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
              .map((connection) => `• ${privateConnectionLabel(connection)}`)
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
                .map(privateConnectionLabel)
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
        `Disconnecting ${selected.map(privateConnectionLabel).join(", ")}. Florence will stop access locally even if Google is temporarily unavailable.`,
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
      `${event.accountLabel} is connected as ${event.email}. Florence is ${
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
        ? `${account} was not connected because permission was declined. Nothing was shared. When you're ready, reply with an exact alias such as “connect Google as Acme work”.`
        : event.reason === "alias_in_use"
          ? `${account} was not connected because that private alias is already in use. Nothing was shared. Choose a distinct exact alias, for example “connect Google as Acme work”.`
          : event.reason === "invalid"
            ? `${account} was not connected because the private handoff could not be verified. Nothing was shared. Reply with an exact alias such as “connect Google as Acme work” for a fresh link.`
            : `${account} could not be connected because Google or Florence was temporarily unavailable. Nothing was shared. Reply with an exact alias such as “connect Google as Acme work” to try again.`;
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
    const label = googleAccountAliasKey(connectionAlias(connection));
    return (
      (connection.email !== null && normalizedCommand.includes(connection.email.toLowerCase())) ||
      normalizedCommand.includes(label)
    );
  });
  if (matching.length > 0) return matching;
  return connections.length === 1 ? [connections[0] as GoogleSyncConnection] : [];
}

function parseGoogleConnectCommand(text: string): { accountAlias: string; loginHint?: string } | null {
  const emailMatch = text.match(/[\p{L}\p{N}.!#$%&'*+/=?^_`{|}~-]+@[\p{L}\p{N}.-]+\.[\p{L}]{2,63}/iu);
  const loginHint = emailMatch?.[0] === undefined ? undefined : normalizedEmail(emailMatch[0]);
  const command = (emailMatch ? text.replace(emailMatch[0], " ") : text)
    .normalize("NFKC")
    .trim()
    .replace(/\s+/gu, " ");
  const explicit = command.match(
    /^(?:connect|link|add)\s+(?:my\s+)?(?:google(?:\s+(?:account|calendar))?|gmail(?:\s+account)?|calendar)\s+as\s+(.+)$/iu,
  );
  const generic = command.match(
    /^(?:connect|link|add)\s+(?:my\s+)?(?:(personal|work)\s+)?(?:google(?:\s+(?:account|calendar))?|gmail(?:\s+account)?|calendar)$/iu,
  );
  const rawAlias =
    explicit?.[1] === undefined
      ? generic?.[1] === undefined
        ? generic
          ? "Google account"
          : null
        : `${generic[1].toLocaleLowerCase("en-US")} Google account`
      : stripMatchingQuotes(explicit[1]);
  if (rawAlias === null) return null;
  try {
    return {
      accountAlias: canonicalGoogleAccountAlias(rawAlias),
      ...(loginHint === undefined ? {} : { loginHint }),
    };
  } catch {
    return null;
  }
}

function connectionAlias(connection: GoogleSyncConnection): string {
  return canonicalGoogleAccountAlias(String(connection.metadata.accountLabel ?? ""));
}

function privateConnectionLabel(connection: GoogleSyncConnection): string {
  const alias = connectionAlias(connection);
  return connection.email ? `${alias} (${normalizedEmail(connection.email)})` : alias;
}

function privateConnectionList(connections: readonly GoogleSyncConnection[]): string {
  if (connections.length === 0) return "";
  return `\n\nYour existing private connections:\n${connections
    .map((connection) => `• ${privateConnectionLabel(connection)}`)
    .join("\n")}`;
}

function privateCalendarAccountLabel(calendar: PrivateGoogleCalendar): string {
  return calendar.connectionEmail
    ? `${calendar.connectionAlias} (${normalizedEmail(calendar.connectionEmail)})`
    : calendar.connectionAlias;
}

function privateCalendarQualifiedName(calendar: PrivateGoogleCalendar): string {
  return `${privateCalendarAccountLabel(calendar)} / ${calendar.displayName}`;
}

function privateCalendarSelectors(calendar: PrivateGoogleCalendar): readonly string[] {
  const alias = canonicalGoogleAccountAlias(calendar.connectionAlias);
  const privateAccount = privateCalendarAccountLabel(calendar);
  const selectors = new Set([
    normalizePrivateSelector(calendar.displayName),
    normalizePrivateSelector(`${alias} / ${calendar.displayName}`),
    normalizePrivateSelector(`${privateAccount} / ${calendar.displayName}`),
  ]);
  if (calendar.connectionEmail !== null) {
    selectors.add(normalizePrivateSelector(`${calendar.connectionEmail} / ${calendar.displayName}`));
  }
  if (calendar.primary) {
    selectors.add(normalizePrivateSelector(alias));
    selectors.add(normalizePrivateSelector(privateAccount));
    selectors.add(normalizePrivateSelector(`${alias} / primary`));
    selectors.add(normalizePrivateSelector(`${privateAccount} / primary`));
    if (calendar.connectionEmail !== null) {
      selectors.add(normalizePrivateSelector(calendar.connectionEmail));
      selectors.add(normalizePrivateSelector(`${calendar.connectionEmail} / primary`));
    }
  }
  return [...selectors];
}

function privateCalendarList(calendars: readonly PrivateGoogleCalendar[], limit: number): string {
  if (calendars.length === 0) return "";
  const visible = calendars.slice(0, limit);
  const remaining = calendars.length - visible.length;
  return `\n\nPrivate choices:\n${visible
    .map((calendar) => `• ${privateCalendarQualifiedName(calendar)}`)
    .join(
      "\n",
    )}${remaining > 0 ? `\n• …and ${remaining} more; say “show my calendars” for the full list.` : ""}`;
}

function normalizePrivateSelector(value: string): string {
  return value
    .normalize("NFKC")
    .trim()
    .replace(/\s*\/\s*/gu, " / ")
    .replace(/\s+/gu, " ")
    .toLocaleLowerCase("en-US");
}

function stripMatchingQuotes(value: string): string {
  const trimmed = value.trim();
  const pairs = [
    ['"', '"'],
    ["'", "'"],
    ["“", "”"],
  ] as const;
  for (const [open, close] of pairs) {
    if (trimmed.startsWith(open) && trimmed.endsWith(close)) {
      return trimmed.slice(open.length, -close.length).trim();
    }
  }
  return trimmed;
}

function stableId(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
