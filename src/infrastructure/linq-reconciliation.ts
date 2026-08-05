import { createHash, randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { z } from "zod";
import {
  LinqApiError,
  type LinqChatSnapshot,
  type LinqMessagesPage,
  type LinqPhoneNumber,
  type LinqRecoveredMessageEvent,
  type LinqWebhookSubscription,
  normalizeRecoveredLinqMessage,
} from "../adapters/linq/index.js";
import type { Database } from "../db/client.js";

const integrationDigestSchema = z.string().regex(/^sha256:[a-f0-9]{64}$/u);
const ownerSchema = z.string().trim().min(1).max(200);
const instantSchema = z.iso.datetime({ offset: true });
const MAX_STALE_SWEEP_MS = 20 * 60_000;

export interface LinqReconciliationReader {
  listChatsPage(input?: {
    from?: string;
    cursor?: string;
    limit?: number;
  }): Promise<{ chats: LinqChatSnapshot[]; nextCursor: string | null }>;
  getChatSnapshot(chatId: string): Promise<LinqChatSnapshot>;
  listMessagesPage(input: { chatId: string; cursor?: string; limit?: number }): Promise<LinqMessagesPage>;
  listWebhookSubscriptions(): Promise<LinqWebhookSubscription[]>;
  listPhoneNumbers(): Promise<LinqPhoneNumber[]>;
}

export interface LinqRecoveredIngress {
  acceptLinqRecovered(event: LinqRecoveredMessageEvent): Promise<void>;
}

type SubscriptionStatus = "unknown" | "active" | "missing" | "inactive" | "misconfigured";

export type LinqReconciliationLease = {
  integrationDigest: string;
  leaseToken: string;
  attempt: number;
  sweepStartedAt: string | null;
  liveNotBeforeAt: string | null;
  chatPageLoaded: boolean;
  chatCursor: string | null;
  nextChatCursor: string | null;
  remainingChatIds: string[];
  messageCursor: string | null;
  linePresent: boolean | null;
  lineReputation: string | null;
  subscriptionStatus: SubscriptionStatus;
  lastFullSweepAt: string | null;
};

type ReconciliationRow = {
  integration_digest: string;
  lease_token: string;
  attempt: number;
  sweep_started_at: Date | null;
  live_not_before_at: Date | null;
  chat_page_loaded: boolean;
  chat_cursor: string | null;
  next_chat_cursor: string | null;
  remaining_chat_ids: string[];
  message_cursor: string | null;
  line_present: boolean | null;
  line_reputation: string | null;
  subscription_status: SubscriptionStatus;
  last_full_sweep_at: Date | null;
};

export class PostgresLinqReconciliationStore {
  readonly #integrationDigest: string;

  public constructor(
    private readonly database: Database,
    integrationDigest: string,
  ) {
    this.#integrationDigest = integrationDigestSchema.parse(integrationDigest);
  }

  public async claim(input: {
    owner: string;
    leaseSeconds: number;
  }): Promise<LinqReconciliationLease | null> {
    const owner = ownerSchema.parse(input.owner);
    const leaseSeconds = z.number().int().min(30).max(3_600).parse(input.leaseSeconds);
    const leaseToken = randomUUID();
    await this.database`
      insert into linq_reconciliation_state (integration_digest)
      values (${this.#integrationDigest})
      on conflict (integration_digest) do nothing
    `;
    const rows = await this.database<ReconciliationRow[]>`
      update linq_reconciliation_state set
        status = 'leased', lease_owner = ${owner}, lease_token = ${leaseToken},
        lease_expires_at = now() + (${leaseSeconds} * interval '1 second'),
        attempt = least(attempt + 1, 1000), updated_at = now()
      where integration_digest = ${this.#integrationDigest}
        and available_at <= now()
        and (status = 'pending' or lease_expires_at <= now())
      returning integration_digest, lease_token, attempt, sweep_started_at,
        live_not_before_at, chat_page_loaded, chat_cursor, next_chat_cursor,
        remaining_chat_ids, message_cursor, line_present, line_reputation,
        subscription_status, last_full_sweep_at
    `;
    const row = rows[0];
    return row === undefined ? null : leaseFromRow(row);
  }

  public async releaseProgress(lease: LinqReconciliationLease, availableAt: string): Promise<boolean> {
    instantSchema.parse(availableAt);
    const rows = await this.database<{ integration_digest: string }[]>`
      update linq_reconciliation_state set
        status = 'pending', available_at = ${availableAt},
        sweep_started_at = ${lease.sweepStartedAt},
        live_not_before_at = ${lease.liveNotBeforeAt},
        chat_page_loaded = ${lease.chatPageLoaded}, chat_cursor = ${lease.chatCursor},
        next_chat_cursor = ${lease.nextChatCursor}, remaining_chat_ids = ${lease.remainingChatIds},
        message_cursor = ${lease.messageCursor}, line_present = ${lease.linePresent},
        line_reputation = ${lease.lineReputation}, subscription_status = ${lease.subscriptionStatus},
        attempt = 0, lease_owner = null, lease_token = null, lease_expires_at = null,
        last_error_code = null, updated_at = now()
      where integration_digest = ${this.#integrationDigest}
        and status = 'leased' and lease_token = ${lease.leaseToken}
      returning integration_digest
    `;
    return rows.length === 1;
  }

  public async completeSweep(
    lease: LinqReconciliationLease,
    input: { coveredThroughAt: string; nextAvailableAt: string },
  ): Promise<boolean> {
    instantSchema.parse(input.coveredThroughAt);
    instantSchema.parse(input.nextAvailableAt);
    const rows = await this.database<{ integration_digest: string }[]>`
      update linq_reconciliation_state set
        status = 'pending', available_at = ${input.nextAvailableAt}, sweep_started_at = null,
        live_not_before_at = null, chat_page_loaded = false, chat_cursor = null,
        next_chat_cursor = null, remaining_chat_ids = '{}'::text[], message_cursor = null,
        attempt = 0, lease_owner = null, lease_token = null, lease_expires_at = null,
        last_full_sweep_at = ${input.coveredThroughAt}, last_error_code = null, updated_at = now()
      where integration_digest = ${this.#integrationDigest}
        and status = 'leased' and lease_token = ${lease.leaseToken}
      returning integration_digest
    `;
    return rows.length === 1;
  }

  public async releaseFailure(
    lease: LinqReconciliationLease,
    input: { retryAt: string; errorCode: string },
  ): Promise<boolean> {
    instantSchema.parse(input.retryAt);
    const errorCode = z
      .string()
      .regex(/^[a-z][a-z0-9_.-]{0,99}$/u)
      .parse(input.errorCode);
    const rows = await this.database<{ integration_digest: string }[]>`
      update linq_reconciliation_state set
        status = 'pending', available_at = ${input.retryAt}, lease_owner = null,
        lease_token = null, lease_expires_at = null, last_error_code = ${errorCode}, updated_at = now()
      where integration_digest = ${this.#integrationDigest}
        and status = 'leased' and lease_token = ${lease.leaseToken}
      returning integration_digest
    `;
    return rows.length === 1;
  }

  public async recordWebhookIngress(): Promise<void> {
    await this.database`
      insert into linq_reconciliation_state (integration_digest, last_webhook_ingress_at)
      values (${this.#integrationDigest}, now())
      on conflict (integration_digest) do update set
        last_webhook_ingress_at = excluded.last_webhook_ingress_at,
        updated_at = now()
    `;
  }

  public async isHealthy(asOf = new Date()): Promise<boolean> {
    const rows = await this.database<
      {
        line_present: boolean | null;
        subscription_status: SubscriptionStatus;
        last_full_sweep_at: Date | null;
      }[]
    >`
      select line_present, subscription_status, last_full_sweep_at
      from linq_reconciliation_state where integration_digest = ${this.#integrationDigest}
    `;
    const row = rows[0];
    return Boolean(
      row?.line_present === true &&
        row.subscription_status === "active" &&
        row.last_full_sweep_at !== null &&
        asOf.getTime() - row.last_full_sweep_at.getTime() <= MAX_STALE_SWEEP_MS,
    );
  }
}

export interface LinqReconciliationHostOptions {
  readonly store: PostgresLinqReconciliationStore;
  readonly reader: LinqReconciliationReader;
  readonly ingress: LinqRecoveredIngress;
  readonly integrationId: string;
  readonly fromPhone: string;
  readonly expectedWebhookUrl: string;
  readonly owner: string;
  readonly pollIntervalMs?: number;
  readonly leaseSeconds?: number;
  readonly sweepIntervalMs?: number;
  readonly now?: () => Date;
}

export class LinqReconciliationHost {
  readonly #owner: string;
  readonly #pollIntervalMs: number;
  readonly #leaseSeconds: number;
  readonly #sweepIntervalMs: number;
  readonly #now: () => Date;

  public constructor(private readonly options: LinqReconciliationHostOptions) {
    this.#owner = ownerSchema.parse(options.owner);
    this.#pollIntervalMs = z
      .number()
      .int()
      .min(250)
      .max(60_000)
      .parse(options.pollIntervalMs ?? 1_000);
    this.#leaseSeconds = z
      .number()
      .int()
      .min(30)
      .max(3_600)
      .parse(options.leaseSeconds ?? 300);
    this.#sweepIntervalMs = z
      .number()
      .int()
      .min(60_000)
      .max(60 * 60_000)
      .parse(options.sweepIntervalMs ?? 5 * 60_000);
    this.#now = options.now ?? (() => new Date());
    integrationDigestSchema.parse(options.integrationId);
    z.string()
      .regex(/^\+[1-9]\d{1,14}$/u)
      .parse(options.fromPhone);
    const webhookUrl = new URL(options.expectedWebhookUrl);
    if (webhookUrl.protocol !== "https:" && webhookUrl.hostname !== "localhost") {
      throw new Error("Linq webhook target must use HTTPS");
    }
  }

  public async run(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      await this.runOnce(signal);
      if (!signal.aborted) await sleep(this.#pollIntervalMs, undefined, { signal });
    }
  }

  public async runOnce(signal?: AbortSignal): Promise<"idle" | "progress" | "completed" | "retried"> {
    assertNotAborted(signal);
    const lease = await this.options.store.claim({ owner: this.#owner, leaseSeconds: this.#leaseSeconds });
    if (lease === null) return "idle";
    try {
      if (lease.sweepStartedAt === null) await this.#beginSweep(lease);
      assertNotAborted(signal);
      if (!lease.chatPageLoaded) await this.#loadChatPage(lease);
      assertNotAborted(signal);

      if (lease.remainingChatIds.length === 0) {
        return await this.#advanceOrComplete(lease);
      }
      await this.#recoverMessagePage(lease);
      assertNotAborted(signal);
      if (lease.remainingChatIds.length === 0) return await this.#advanceOrComplete(lease);
      const released = await this.options.store.releaseProgress(lease, this.#now().toISOString());
      if (!released) throw new Error("Linq reconciliation lease was lost");
      return "progress";
    } catch (error) {
      if (signal?.aborted) {
        await this.#retry(lease, "linq_reconciliation.aborted", 1_000);
        return "retried";
      }
      if (error instanceof LinqCursorError) {
        resetCursor(lease, error.scope);
        const released = await this.options.store.releaseProgress(lease, this.#now().toISOString());
        if (!released) throw new Error("Linq reconciliation lease was lost");
        return "progress";
      }
      if (error instanceof LinqApiError) {
        const delay =
          error.retryAfterSeconds === null
            ? retryDelayMs(lease.attempt)
            : Math.min(60 * 60_000, Math.max(1_000, error.retryAfterSeconds * 1_000));
        await this.#retry(
          lease,
          error.retryable ? "linq_reconciliation.transient" : "linq_reconciliation.provider",
          delay,
        );
        return "retried";
      }
      await this.#retry(lease, "linq_reconciliation.unexpected", retryDelayMs(lease.attempt));
      return "retried";
    }
  }

  async #beginSweep(lease: LinqReconciliationLease): Promise<void> {
    const now = this.#now().toISOString();
    const [phoneNumbers, subscriptions] = await Promise.all([
      this.options.reader.listPhoneNumbers(),
      this.options.reader.listWebhookSubscriptions(),
    ]);
    const line = phoneNumbers.find((phone) => phone.phoneNumber === this.options.fromPhone);
    lease.sweepStartedAt = now;
    lease.liveNotBeforeAt = lease.lastFullSweepAt ?? now;
    lease.linePresent = line !== undefined;
    lease.lineReputation = line?.reputation.status ?? null;
    lease.subscriptionStatus = subscriptionStatus(
      subscriptions,
      this.options.expectedWebhookUrl,
      this.options.fromPhone,
    );
  }

  async #loadChatPage(lease: LinqReconciliationLease): Promise<void> {
    let page: Awaited<ReturnType<LinqReconciliationReader["listChatsPage"]>>;
    try {
      page = await this.options.reader.listChatsPage({
        from: this.options.fromPhone,
        ...(lease.chatCursor === null ? {} : { cursor: lease.chatCursor }),
        limit: 100,
      });
    } catch (error) {
      if (isInvalidCursor(error) && lease.chatCursor !== null) throw new LinqCursorError("chats");
      throw error;
    }
    lease.remainingChatIds = page.chats.map((chat) => chat.id);
    lease.nextChatCursor = page.nextCursor;
    lease.chatPageLoaded = true;
    lease.messageCursor = null;
  }

  async #recoverMessagePage(lease: LinqReconciliationLease): Promise<void> {
    const chatId = lease.remainingChatIds[0];
    if (chatId === undefined || lease.liveNotBeforeAt === null) throw new Error("Invalid Linq sweep state");
    let chat: LinqChatSnapshot;
    try {
      chat = await this.options.reader.getChatSnapshot(chatId);
    } catch (error) {
      if (error instanceof LinqApiError && error.status === 404) {
        lease.remainingChatIds = lease.remainingChatIds.slice(1);
        lease.messageCursor = null;
        return;
      }
      throw error;
    }

    let page: LinqMessagesPage;
    try {
      page = await this.options.reader.listMessagesPage({
        chatId,
        ...(lease.messageCursor === null ? {} : { cursor: lease.messageCursor }),
        limit: 100,
      });
    } catch (error) {
      if (isInvalidCursor(error) && lease.messageCursor !== null) throw new LinqCursorError("messages");
      throw error;
    }
    const recoveredAt = this.#now().toISOString();
    for (const message of page.messages) {
      const event = normalizeRecoveredLinqMessage({
        integrationId: this.options.integrationId,
        selfHandle: this.options.fromPhone,
        chat,
        message,
        recoveredAt,
        liveNotBefore: lease.liveNotBeforeAt,
      });
      if (event !== null) await this.options.ingress.acceptLinqRecovered(event);
    }
    if (page.nextCursor === null) {
      lease.remainingChatIds = lease.remainingChatIds.slice(1);
      lease.messageCursor = null;
    } else {
      lease.messageCursor = page.nextCursor;
    }
  }

  async #advanceOrComplete(lease: LinqReconciliationLease): Promise<"progress" | "completed"> {
    if (lease.nextChatCursor !== null) {
      lease.chatCursor = lease.nextChatCursor;
      lease.nextChatCursor = null;
      lease.chatPageLoaded = false;
      lease.messageCursor = null;
      const released = await this.options.store.releaseProgress(lease, this.#now().toISOString());
      if (!released) throw new Error("Linq reconciliation lease was lost");
      return "progress";
    }
    if (lease.sweepStartedAt === null) throw new Error("Invalid Linq sweep state");
    const completedAt = this.#now();
    const completed = await this.options.store.completeSweep(lease, {
      // A sweep can only prove coverage up to the instant it began. Advancing this
      // boundary to completion would strand messages created after their chat was
      // scanned but before the final page completed.
      coveredThroughAt: lease.sweepStartedAt,
      nextAvailableAt: new Date(completedAt.getTime() + this.#sweepIntervalMs).toISOString(),
    });
    if (!completed) throw new Error("Linq reconciliation lease was lost");
    return "completed";
  }

  async #retry(lease: LinqReconciliationLease, errorCode: string, delayMs: number): Promise<void> {
    const retryAt = new Date(this.#now().getTime() + delayMs).toISOString();
    const released = await this.options.store.releaseFailure(lease, { retryAt, errorCode });
    if (!released) throw new Error("Linq reconciliation lease was lost");
  }
}

export function linqIntegrationDigest(fromPhone: string): string {
  const normalized = z
    .string()
    .regex(/^\+[1-9]\d{1,14}$/u)
    .parse(fromPhone);
  return `sha256:${createHash("sha256").update(`linq-line\0${normalized}`).digest("hex")}`;
}

function leaseFromRow(row: ReconciliationRow): LinqReconciliationLease {
  return {
    integrationDigest: integrationDigestSchema.parse(row.integration_digest),
    leaseToken: z.uuid().parse(row.lease_token),
    attempt: z.number().int().min(1).max(1000).parse(row.attempt),
    sweepStartedAt: row.sweep_started_at?.toISOString() ?? null,
    liveNotBeforeAt: row.live_not_before_at?.toISOString() ?? null,
    chatPageLoaded: row.chat_page_loaded,
    chatCursor: row.chat_cursor,
    nextChatCursor: row.next_chat_cursor,
    remainingChatIds: z.array(z.string().min(1).max(500)).max(100).parse(row.remaining_chat_ids),
    messageCursor: row.message_cursor,
    linePresent: row.line_present,
    lineReputation: row.line_reputation,
    subscriptionStatus: row.subscription_status,
    lastFullSweepAt: row.last_full_sweep_at?.toISOString() ?? null,
  };
}

function subscriptionStatus(
  subscriptions: readonly LinqWebhookSubscription[],
  expectedUrl: string,
  fromPhone: string,
): SubscriptionStatus {
  const exact = subscriptions.find((subscription) => subscription.targetUrl === expectedUrl);
  if (exact === undefined) return subscriptions.length === 0 ? "missing" : "misconfigured";
  if (!exact.isActive) return "inactive";
  const receivesMessages = exact.subscribedEvents.includes("message.received");
  const receivesLine =
    exact.phoneNumbers === null || exact.phoneNumbers.length === 0 || exact.phoneNumbers.includes(fromPhone);
  return receivesMessages && receivesLine ? "active" : "misconfigured";
}

class LinqCursorError extends Error {
  public constructor(readonly scope: "chats" | "messages") {
    super("Linq reconciliation cursor is no longer valid");
  }
}

function isInvalidCursor(error: unknown): boolean {
  return error instanceof LinqApiError && error.status === 400;
}

function resetCursor(lease: LinqReconciliationLease, scope: "chats" | "messages"): void {
  if (scope === "messages") {
    lease.messageCursor = null;
    return;
  }
  lease.sweepStartedAt = null;
  lease.liveNotBeforeAt = null;
  lease.chatPageLoaded = false;
  lease.chatCursor = null;
  lease.nextChatCursor = null;
  lease.remainingChatIds = [];
  lease.messageCursor = null;
}

function retryDelayMs(attempt: number): number {
  return Math.min(15 * 60_000, 5_000 * 2 ** Math.max(0, Math.min(8, attempt - 1)));
}

function assertNotAborted(signal?: AbortSignal): void {
  if (signal?.aborted) throw signal.reason ?? new Error("aborted");
}
