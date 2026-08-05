import { createHash } from "node:crypto";
import { lookup } from "node:dns/promises";
import { request as httpsRequest, type RequestOptions } from "node:https";
import { isIP } from "node:net";
import { isDeepStrictEqual } from "node:util";
import { z } from "zod";
import { LinqApiError, type LinqOutboundSender } from "../adapters/linq/index.js";
import {
  ApplicationOutboxIntentSchema,
  type EffectExecutionReceipt,
  EffectExecutionReceiptSchema,
  type HouseholdApplicationSnapshot,
  HouseholdApplicationSnapshotSchema,
} from "../application/contracts.js";
import type { ApplicationEffectExecutorPort, WorkerContextPort } from "../application/ports.js";
import type { ProjectionTimerIntent } from "../db/application-store.js";
import { type DurableScope, DurableScopeSchema, type FamilyEpisode } from "../domain/index.js";
import { JsonValueSchema } from "../models/contracts.js";
import {
  type WorkerAttemptOptions,
  type WorkerJob,
  WorkerJobSchema,
  type WorkerTool,
  type WorkerToolExecutionContext,
} from "../runtime/index.js";

const HOUSEHOLD_SCHEDULE_CAPABILITY = "capability.household_schedule.read";
const RESEARCH_CAPABILITY = "capability.research.read";
const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);

const LinqChannelTargetSchema = z.strictObject({
  householdId: z.string().min(1),
  targetScope: DurableScopeSchema,
  chatId: z.string().min(1).max(500),
  status: z.enum(["active", "inactive"]),
});

export type LinqChannelTarget = z.infer<typeof LinqChannelTargetSchema>;

/** Resolves an app-owned scope to one currently authorized Linq channel. */
export interface LinqChannelDirectory {
  resolveTarget(input: {
    readonly householdId: string;
    readonly targetScope: DurableScope;
  }): Promise<LinqChannelTarget | null>;
}

/** The narrow timer surface needed by domain effects. */
export interface WorkerTimerStore {
  scheduleTimer(
    input: ProjectionTimerIntent & { readonly householdId: string },
  ): Promise<{ readonly rowId: string }>;
  cancelTimer(input: { readonly householdId: string; readonly timerKey: string }): Promise<boolean>;
}

export interface ProductionApplicationEffectExecutorOptions {
  readonly sender: LinqOutboundSender;
  readonly channelDirectory: LinqChannelDirectory;
  readonly timerStore: WorkerTimerStore;
  readonly now?: () => Date;
}

function scopeEquals(left: DurableScope, right: DurableScope): boolean {
  if (left.kind === "household") return right.kind === "household";
  return right.kind === "personal" && left.adultId === right.adultId;
}

function stableReceipt(prefix: string, ...values: readonly string[]): string {
  const digest = createHash("sha256").update(values.join("\0")).digest("hex");
  return `${prefix}_${digest}`;
}

/**
 * Executes the deliberately small set of production-safe effects. Unknown effects
 * and external actions without a dedicated executor fail closed.
 */
export class ProductionApplicationEffectExecutor implements ApplicationEffectExecutorPort {
  readonly #sender: LinqOutboundSender;
  readonly #channelDirectory: LinqChannelDirectory;
  readonly #timerStore: WorkerTimerStore;
  readonly #now: () => Date;

  constructor(options: ProductionApplicationEffectExecutorOptions) {
    this.#sender = options.sender;
    this.#channelDirectory = options.channelDirectory;
    this.#timerStore = options.timerStore;
    this.#now = options.now ?? (() => new Date());
  }

  async execute(
    rawIntent: Parameters<ApplicationEffectExecutorPort["execute"]>[0],
  ): Promise<EffectExecutionReceipt> {
    const intentResult = ApplicationOutboxIntentSchema.safeParse(rawIntent);
    if (!intentResult.success || intentResult.data.kind === "worker.run") {
      return this.#receipt("permanent_failure");
    }
    const intent = intentResult.data;

    if (intent.kind === "conversation.send") {
      return this.#sendMessage(intent.householdId, intent.targetScope, intent.body, intent.idempotencyKey);
    }

    const effect = intent.effect;
    if (effect.householdId !== intent.householdId || effect.idempotencyKey !== intent.idempotencyKey) {
      return this.#receipt("permanent_failure");
    }

    switch (effect.kind) {
      case "send_message":
        return this.#sendMessage(effect.householdId, effect.targetScope, effect.body, effect.idempotencyKey);
      case "schedule_timer":
        try {
          const stored = await this.#timerStore.scheduleTimer({
            householdId: effect.householdId,
            timerKey: effect.timerId,
            episodeKey: effect.episodeId,
            triggerKind: "domain.timer",
            planVersion: effect.temporalPlanVersion,
            dueAt: effect.at,
            payload: {
              timerId: effect.timerId,
              episodeId: effect.episodeId,
              temporalPlanVersion: effect.temporalPlanVersion,
              triggerId: effect.triggerId,
            },
          });
          return this.#receipt("succeeded", stableReceipt("timer_schedule", effect.intentId, stored.rowId));
        } catch {
          return this.#receipt("retryable_failure");
        }
      case "cancel_timer":
        try {
          await this.#timerStore.cancelTimer({
            householdId: effect.householdId,
            timerKey: effect.timerId,
          });
          return this.#receipt("succeeded", stableReceipt("timer_cancel", effect.intentId, effect.timerId));
        } catch {
          return this.#receipt("retryable_failure");
        }
      case "execute_external_action":
        return EffectExecutionReceiptSchema.parse({
          status: "permanent_failure",
          receiptRef: stableReceipt("unsupported_action", effect.intentId, effect.action.actionId),
          recordedAt: this.#now().toISOString(),
          externalAction: {
            receiptId: stableReceipt("action_receipt", effect.intentId, effect.approvalId),
            actionId: effect.action.actionId,
            actionDigest: effect.action.actionDigest,
            outcome: "failed",
          },
        });
      case "enqueue_worker":
        return this.#receipt("permanent_failure");
    }
  }

  async #sendMessage(
    householdId: string,
    targetScope: DurableScope,
    body: string,
    idempotencyKey: string,
  ): Promise<EffectExecutionReceipt> {
    let rawTarget: LinqChannelTarget | null;
    try {
      rawTarget = await this.#channelDirectory.resolveTarget({ householdId, targetScope });
    } catch {
      return this.#receipt("retryable_failure");
    }

    const targetResult = LinqChannelTargetSchema.safeParse(rawTarget);
    if (
      !targetResult.success ||
      targetResult.data.status !== "active" ||
      targetResult.data.householdId !== householdId ||
      !scopeEquals(targetResult.data.targetScope, targetScope)
    ) {
      return this.#receipt("permanent_failure");
    }

    try {
      const sent = await this.#sender.sendText({
        chatId: targetResult.data.chatId,
        text: body,
        idempotencyKey,
      });
      if (
        sent.provider !== "linq" ||
        sent.chatId !== targetResult.data.chatId ||
        sent.idempotencyKey !== idempotencyKey
      ) {
        return this.#receipt("permanent_failure");
      }
      return this.#receipt(
        "succeeded",
        stableReceipt("linq_send", sent.chatId, sent.providerMessageId, sent.idempotencyKey),
      );
    } catch (error) {
      return this.#receipt(
        error instanceof LinqApiError && !error.retryable ? "permanent_failure" : "retryable_failure",
      );
    }
  }

  #receipt(status: EffectExecutionReceipt["status"], receiptRef?: string): EffectExecutionReceipt {
    return EffectExecutionReceiptSchema.parse({
      status,
      ...(receiptRef === undefined ? {} : { receiptRef }),
      recordedAt: this.#now().toISOString(),
    });
  }
}

export type WorkerServiceErrorCode =
  | "aborted"
  | "blocked_url"
  | "budget_exhausted"
  | "invalid_context"
  | "invalid_url"
  | "network_unavailable"
  | "redirect_limit"
  | "response_too_large"
  | "unsupported_content"
  | "unsupported_tool";

/** Safe, stable error codes; raw network and private-data details are never exposed. */
export class WorkerServiceError extends Error {
  override readonly name = "WorkerServiceError";

  constructor(readonly code: WorkerServiceErrorCode) {
    super(code);
  }
}

export interface ResearchAddress {
  readonly address: string;
  readonly family: 4 | 6;
}

export interface ResearchNetworkResponse {
  readonly status: number;
  readonly headers: Readonly<Record<string, string | undefined>>;
  readonly body: Uint8Array;
}

export interface ResearchNetworkRequest {
  readonly url: URL;
  readonly address: ResearchAddress;
  readonly signal: AbortSignal;
  readonly maxBytes: number;
  readonly timeoutMs: number;
}

/**
 * Injectable low-level network seam. The production implementation pins the
 * already-vetted DNS address so validation and connection cannot diverge.
 */
export interface ResearchNetworkPort {
  resolve(hostname: string, signal: AbortSignal): Promise<readonly ResearchAddress[]>;
  request(input: ResearchNetworkRequest): Promise<ResearchNetworkResponse>;
}

export class PinnedHttpsResearchNetwork implements ResearchNetworkPort {
  async resolve(hostname: string, signal: AbortSignal): Promise<readonly ResearchAddress[]> {
    if (signal.aborted) throw new WorkerServiceError("aborted");
    let addresses: readonly { address: string; family: number }[];
    try {
      addresses = await abortable(lookup(hostname, { all: true, verbatim: true }), signal);
    } catch {
      throw new WorkerServiceError("network_unavailable");
    }
    if (signal.aborted) throw new WorkerServiceError("aborted");
    return addresses.flatMap((entry) =>
      entry.family === 4 || entry.family === 6 ? [{ address: entry.address, family: entry.family }] : [],
    );
  }

  request(input: ResearchNetworkRequest): Promise<ResearchNetworkResponse> {
    const hostname = normalizeHostname(input.url.hostname);
    const options: RequestOptions = {
      protocol: "https:",
      hostname: input.address.address,
      family: input.address.family,
      port: 443,
      method: "GET",
      path: `${input.url.pathname}${input.url.search}`,
      headers: {
        accept: "text/html,application/xhtml+xml,text/plain;q=0.9",
        "accept-encoding": "identity",
        host: isIP(hostname) === 6 ? `[${hostname}]` : hostname,
        "user-agent": "FlorenceResearch/1.0",
      },
      rejectUnauthorized: true,
      signal: input.signal,
    };
    if (isIP(hostname) === 0) options.servername = hostname;

    return new Promise((resolve, reject) => {
      let settled = false;
      const finishReject = (code: WorkerServiceErrorCode) => {
        if (settled) return;
        settled = true;
        reject(new WorkerServiceError(code));
      };
      const request = httpsRequest(options, (response) => {
        const chunks: Buffer[] = [];
        let received = 0;
        response.on("data", (chunk: Buffer | string) => {
          const buffer = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
          received += buffer.byteLength;
          if (received > input.maxBytes) {
            request.destroy();
            finishReject("response_too_large");
            return;
          }
          chunks.push(buffer);
        });
        response.on("end", () => {
          if (settled) return;
          settled = true;
          const headers: Record<string, string | undefined> = {};
          for (const [key, value] of Object.entries(response.headers)) {
            headers[key] = Array.isArray(value) ? value.join(", ") : value;
          }
          resolve({
            status: response.statusCode ?? 0,
            headers,
            body: Buffer.concat(chunks),
          });
        });
        response.on("error", () => finishReject("network_unavailable"));
      });
      request.setTimeout(input.timeoutMs, () => {
        request.destroy();
        finishReject("network_unavailable");
      });
      request.on("error", () => {
        finishReject(input.signal.aborted ? "aborted" : "network_unavailable");
      });
      request.end();
    });
  }
}

export interface ResearchLimits {
  readonly maxQueriesPerJob: number;
  readonly maxUrlsPerCall: number;
  readonly maxResultsPerQuery: number;
  readonly maxRedirects: number;
  readonly maxBytesPerResponse: number;
  readonly maxTotalBytesPerJob: number;
  readonly maxExtractedCharacters: number;
  readonly timeoutMs: number;
  readonly searchEndpoint: string;
}

const DEFAULT_RESEARCH_LIMITS: ResearchLimits = Object.freeze({
  maxQueriesPerJob: 3,
  maxUrlsPerCall: 5,
  maxResultsPerQuery: 5,
  maxRedirects: 3,
  maxBytesPerResponse: 256 * 1024,
  maxTotalBytesPerJob: 1024 * 1024,
  maxExtractedCharacters: 16_000,
  timeoutMs: 8_000,
  searchEndpoint: "https://html.duckduckgo.com/html/",
});

export interface ScopedWorkerContextOptions {
  readonly now?: () => Date;
  readonly researchNetwork?: ResearchNetworkPort;
  readonly researchLimits?: Partial<ResearchLimits>;
}

interface ResearchBudget {
  queriesRemaining: number;
  bytesRemaining: number;
}

interface ExtractedSource {
  readonly url: string;
  readonly title: string;
  readonly text: string;
  readonly contentType: string;
  readonly truncated: boolean;
}

interface ResearchFailure {
  readonly url: string;
  readonly code: WorkerServiceErrorCode;
}

/** Builds minimum-necessary context and only the two bounded, read-only tools. */
export class ScopedWorkerContext implements WorkerContextPort {
  readonly #now: () => Date;
  readonly #researchNetwork: ResearchNetworkPort;
  readonly #researchLimits: ResearchLimits;

  constructor(options: ScopedWorkerContextOptions = {}) {
    this.#now = options.now ?? (() => new Date());
    this.#researchNetwork = options.researchNetwork ?? new PinnedHttpsResearchNetwork();
    this.#researchLimits = parseResearchLimits({
      ...DEFAULT_RESEARCH_LIMITS,
      ...options.researchLimits,
    });
  }

  async contextFor(
    rawJob: WorkerJob,
    rawSnapshot: HouseholdApplicationSnapshot,
  ): Promise<WorkerAttemptOptions> {
    const job = WorkerJobSchema.parse(rawJob);
    const snapshot = HouseholdApplicationSnapshotSchema.parse(rawSnapshot);
    const now = this.#now().getTime();
    if (
      job.householdId !== snapshot.aggregate.householdId ||
      job.baseHouseholdVersion !== snapshot.aggregate.version ||
      job.policyVersion !== snapshot.aggregate.policyVersion ||
      Date.parse(job.deadline) <= now ||
      Date.parse(job.scopeGrant.expiresAt) <= now
    ) {
      throw new WorkerServiceError("invalid_context");
    }
    if (job.scopeGrant.visibility === "personal") {
      const adultId = job.scopeGrant.adultId;
      if (
        adultId === undefined ||
        !snapshot.aggregate.verifiedAdultIds.some((candidate) => candidate === adultId)
      ) {
        throw new WorkerServiceError("invalid_context");
      }
    }

    const workerRecord = snapshot.projection.workers.find(
      (record) => record.job.jobId === job.jobId && record.job.attemptId === job.attemptId,
    );
    if (workerRecord?.status !== "queued" || !isDeepStrictEqual(workerRecord.job, job)) {
      throw new WorkerServiceError("invalid_context");
    }
    const episode = snapshot.aggregate.episodes.find(
      (candidate) => candidate.episodeId === workerRecord.episodeId,
    );
    if (episode === undefined || !scopeCanRead(job, episode.scope)) {
      throw new WorkerServiceError("invalid_context");
    }
    const evidenceById = new Map<string, (typeof episode.evidence)[number]>(
      episode.evidence.map((item) => [item.evidenceId, item]),
    );
    if (job.evidenceRefs.some((reference) => !evidenceById.has(reference))) {
      throw new WorkerServiceError("invalid_context");
    }

    const context = [
      {
        reference: job.scopeGrant.grantId,
        visibility: job.scopeGrant.visibility,
        ...(job.scopeGrant.visibility === "personal" ? { adultId: job.scopeGrant.adultId } : {}),
        mediaType: "application/json",
        content: JSON.stringify({
          schemaVersion: 1,
          purpose: job.scopeGrant.purpose,
          householdTimeZone: snapshot.aggregate.timeZone,
          episode: {
            episodeId: episode.episodeId,
            type: episode.type,
            state: episode.state,
            title: episode.title,
            requiredOutcome: episode.requiredOutcome,
            scope: episode.scope,
          },
          evidence: job.evidenceRefs.map((reference) => {
            const evidence = evidenceById.get(reference);
            if (evidence === undefined) throw new WorkerServiceError("invalid_context");
            return {
              evidenceId: evidence.evidenceId,
              source: evidence.source,
              observedAt: evidence.observedAt,
              revision: evidence.revision,
            };
          }),
        }),
      },
    ];

    const tools: WorkerTool[] = [];
    for (const toolName of job.allowedToolNames) {
      switch (toolName) {
        case "household_schedule":
          requireCapability(job, HOUSEHOLD_SCHEDULE_CAPABILITY);
          tools.push(createHouseholdScheduleTool(job, snapshot));
          break;
        case "research_sources":
          requireCapability(job, RESEARCH_CAPABILITY);
          tools.push(createResearchSourcesTool(job, this.#researchNetwork, this.#researchLimits));
          break;
        default:
          throw new WorkerServiceError("unsupported_tool");
      }
    }

    return { context, tools };
  }
}

function parseResearchLimits(input: ResearchLimits): ResearchLimits {
  return z
    .strictObject({
      maxQueriesPerJob: z.number().int().min(0).max(100),
      maxUrlsPerCall: z.number().int().positive().max(20),
      maxResultsPerQuery: z.number().int().positive().max(20),
      maxRedirects: z.number().int().min(0).max(10),
      maxBytesPerResponse: z
        .number()
        .int()
        .positive()
        .max(10 * 1024 * 1024),
      maxTotalBytesPerJob: z
        .number()
        .int()
        .positive()
        .max(50 * 1024 * 1024),
      maxExtractedCharacters: z.number().int().positive().max(100_000),
      timeoutMs: z.number().int().positive().max(60_000),
      searchEndpoint: z.url(),
    })
    .parse(input);
}

function requireCapability(job: WorkerJob, capability: string): void {
  if (!job.capabilityIds.includes(capability)) {
    throw new WorkerServiceError("invalid_context");
  }
}

function scopeCanRead(job: WorkerJob, scope: DurableScope): boolean {
  if (job.scopeGrant.visibility === "household") return scope.kind === "household";
  return scope.kind === "household" || scope.adultId === job.scopeGrant.adultId;
}

function assertExecutionContext(
  job: WorkerJob,
  requiredCapability: string,
  context: WorkerToolExecutionContext,
): void {
  if (
    context.signal.aborted ||
    context.jobId !== job.jobId ||
    context.attemptId !== job.attemptId ||
    context.householdId !== job.householdId ||
    !context.capabilityIds.includes(requiredCapability) ||
    !job.capabilityIds.includes(requiredCapability)
  ) {
    throw new WorkerServiceError(context.signal.aborted ? "aborted" : "invalid_context");
  }
}

function createHouseholdScheduleTool(job: WorkerJob, snapshot: HouseholdApplicationSnapshot): WorkerTool {
  const inputSchema = z.strictObject({
    limit: z.number().int().min(1).max(50).default(25),
    includeCompleted: z.boolean().default(false),
  });
  return {
    name: "household_schedule",
    description: "Read the authorized household schedule projection. This tool never writes.",
    inputSchema,
    requiredCapabilityIds: [HOUSEHOLD_SCHEDULE_CAPABILITY],
    async execute(rawInput, context) {
      assertExecutionContext(job, HOUSEHOLD_SCHEDULE_CAPABILITY, context);
      const input = inputSchema.parse(rawInput);
      const visible = snapshot.aggregate.episodes.filter(
        (episode) =>
          scopeCanRead(job, episode.scope) &&
          episode.temporalPlan !== undefined &&
          (input.includeCompleted || !isTerminalEpisode(episode)),
      );
      const selected = visible.slice(0, input.limit);
      return JsonValueSchema.parse({
        timeZone: snapshot.aggregate.timeZone,
        routineAnchors: snapshot.aggregate.routineAnchors,
        episodes: selected.map((episode) => ({
          episodeId: episode.episodeId,
          type: episode.type,
          state: episode.state,
          title: episode.title,
          requiredOutcome: episode.requiredOutcome,
          scope: episode.scope,
          ownerStatus: episode.owner.status,
          temporalPlan: episode.temporalPlan,
        })),
        truncated: selected.length < visible.length,
      });
    },
  };
}

function isTerminalEpisode(episode: FamilyEpisode): boolean {
  return ["completed", "dismissed", "superseded", "failed"].includes(episode.state);
}

function createResearchSourcesTool(
  job: WorkerJob,
  network: ResearchNetworkPort,
  limits: ResearchLimits,
): WorkerTool {
  const inputSchema = z
    .strictObject({
      query: z.string().trim().min(1).max(300).optional(),
      urls: z.array(z.url()).min(1).max(limits.maxUrlsPerCall).optional(),
      maxResults: z.number().int().min(1).max(limits.maxResultsPerQuery).default(3),
    })
    .superRefine((input, context) => {
      if ((input.query === undefined) === (input.urls === undefined)) {
        context.addIssue({ code: "custom", message: "Provide exactly one of query or urls" });
      }
    });
  const budget: ResearchBudget = {
    queriesRemaining: limits.maxQueriesPerJob,
    bytesRemaining: limits.maxTotalBytesPerJob,
  };
  let tail: Promise<void> = Promise.resolve();

  return {
    name: "research_sources",
    description:
      "Fetch public HTTPS sources or search the public web, returning bounded text and source URLs. This tool never writes or sends credentials.",
    inputSchema,
    requiredCapabilityIds: [RESEARCH_CAPABILITY],
    execute(rawInput, context) {
      const operation = tail.then(async () => {
        assertExecutionContext(job, RESEARCH_CAPABILITY, context);
        const input = inputSchema.parse(rawInput);
        let query: string | null = null;
        let candidates: readonly { url: string; title?: string }[];
        const failures: ResearchFailure[] = [];

        if (input.query !== undefined) {
          if (budget.queriesRemaining <= 0) {
            throw new WorkerServiceError("budget_exhausted");
          }
          budget.queriesRemaining -= 1;
          query = input.query;
          candidates = await searchPublicWeb(
            input.query,
            input.maxResults,
            network,
            limits,
            budget,
            context.signal,
          );
        } else {
          candidates = (input.urls ?? []).slice(0, input.maxResults).map((url) => ({ url }));
        }

        const sources: ExtractedSource[] = [];
        for (const candidate of deduplicateCandidates(candidates)) {
          if (sources.length >= input.maxResults) break;
          try {
            const source = await fetchExtractedSource(candidate.url, network, limits, budget, context.signal);
            sources.push({
              ...source,
              title: source.title || candidate.title || source.url,
            });
          } catch (error) {
            failures.push({
              url: safeDisplayUrl(candidate.url),
              code: error instanceof WorkerServiceError ? error.code : "network_unavailable",
            });
          }
        }

        return JsonValueSchema.parse({
          query,
          sources,
          failures,
          budget: {
            queriesRemaining: budget.queriesRemaining,
            bytesRemaining: budget.bytesRemaining,
          },
        });
      });
      tail = operation.then(
        () => undefined,
        () => undefined,
      );
      return operation;
    },
  };
}

async function searchPublicWeb(
  query: string,
  maxResults: number,
  network: ResearchNetworkPort,
  limits: ResearchLimits,
  budget: ResearchBudget,
  signal: AbortSignal,
): Promise<readonly { url: string; title?: string }[]> {
  const searchUrl = new URL(limits.searchEndpoint);
  searchUrl.searchParams.set("q", query);
  const response = await fetchWithRedirects(searchUrl, network, limits, budget, signal);
  ensureReadableResponse(response);
  const html = new TextDecoder().decode(response.body);
  return extractSearchResults(html, maxResults);
}

async function fetchExtractedSource(
  rawUrl: string,
  network: ResearchNetworkPort,
  limits: ResearchLimits,
  budget: ResearchBudget,
  signal: AbortSignal,
): Promise<ExtractedSource> {
  let url: URL;
  try {
    url = new URL(rawUrl);
  } catch {
    throw new WorkerServiceError("invalid_url");
  }
  const response = await fetchWithRedirects(url, network, limits, budget, signal);
  const contentType = ensureReadableResponse(response);
  const decoded = new TextDecoder().decode(response.body);
  const title = contentType.includes("html") ? extractHtmlTitle(decoded) : "";
  const fullText = contentType.includes("html") ? extractHtmlText(decoded) : decoded.trim();
  const text = fullText.slice(0, limits.maxExtractedCharacters);
  return {
    url: response.url.toString(),
    title,
    text,
    contentType,
    truncated: text.length < fullText.length,
  };
}

interface FollowedResponse extends ResearchNetworkResponse {
  readonly url: URL;
}

async function fetchWithRedirects(
  initialUrl: URL,
  network: ResearchNetworkPort,
  limits: ResearchLimits,
  budget: ResearchBudget,
  signal: AbortSignal,
): Promise<FollowedResponse> {
  let current = initialUrl;
  const visited = new Set<string>();
  for (let redirects = 0; redirects <= limits.maxRedirects; redirects += 1) {
    if (signal.aborted) throw new WorkerServiceError("aborted");
    current = normalizePublicUrl(current);
    if (visited.has(current.toString())) throw new WorkerServiceError("redirect_limit");
    visited.add(current.toString());
    const requestSignal = AbortSignal.any([signal, AbortSignal.timeout(limits.timeoutMs)]);
    const address = await resolvePublicAddress(current.hostname, network, requestSignal);
    if (budget.bytesRemaining <= 0) throw new WorkerServiceError("budget_exhausted");
    const allowedBytes = Math.min(limits.maxBytesPerResponse, budget.bytesRemaining);
    budget.bytesRemaining -= allowedBytes;
    const response = await network.request({
      url: current,
      address,
      signal: requestSignal,
      maxBytes: allowedBytes,
      timeoutMs: limits.timeoutMs,
    });
    if (response.body.byteLength > allowedBytes) {
      throw new WorkerServiceError("response_too_large");
    }
    budget.bytesRemaining += allowedBytes - response.body.byteLength;

    if (!REDIRECT_STATUSES.has(response.status)) return { ...response, url: current };
    const location = response.headers.location;
    if (location === undefined || redirects === limits.maxRedirects) {
      throw new WorkerServiceError("redirect_limit");
    }
    try {
      current = new URL(location, current);
    } catch {
      throw new WorkerServiceError("invalid_url");
    }
  }
  throw new WorkerServiceError("redirect_limit");
}

function ensureReadableResponse(response: FollowedResponse): string {
  if (response.status < 200 || response.status >= 300) {
    throw new WorkerServiceError("network_unavailable");
  }
  const encoding = response.headers["content-encoding"]?.toLowerCase();
  if (encoding !== undefined && encoding !== "identity") {
    throw new WorkerServiceError("unsupported_content");
  }
  const contentType = (response.headers["content-type"] ?? "text/plain")
    .split(";", 1)[0]
    ?.trim()
    .toLowerCase();
  if (
    contentType !== "text/html" &&
    contentType !== "application/xhtml+xml" &&
    contentType !== "text/plain"
  ) {
    throw new WorkerServiceError("unsupported_content");
  }
  return contentType;
}

function normalizePublicUrl(input: URL): URL {
  const url = new URL(input.toString());
  if (
    url.protocol !== "https:" ||
    url.username !== "" ||
    url.password !== "" ||
    (url.port !== "" && url.port !== "443") ||
    [...url.searchParams.keys()].some(isSensitiveQueryParameter)
  ) {
    throw new WorkerServiceError("blocked_url");
  }
  const hostname = normalizeHostname(url.hostname);
  if (
    hostname.length === 0 ||
    hostname.length > 253 ||
    hostname === "localhost" ||
    hostname.endsWith(".localhost") ||
    hostname.endsWith(".local") ||
    hostname.endsWith(".internal") ||
    hostname.endsWith(".home") ||
    hostname.endsWith(".lan") ||
    hostname.endsWith(".onion") ||
    hostname === "metadata.google.internal" ||
    hostname === "instance-data" ||
    (isIP(hostname) === 0 && !hostname.includes("."))
  ) {
    throw new WorkerServiceError("blocked_url");
  }
  url.hostname = hostname;
  url.hash = "";
  return url;
}

function normalizeHostname(hostname: string): string {
  const withoutBrackets =
    hostname.startsWith("[") && hostname.endsWith("]") ? hostname.slice(1, -1) : hostname;
  return withoutBrackets.toLowerCase().replace(/\.$/, "");
}

async function resolvePublicAddress(
  hostname: string,
  network: ResearchNetworkPort,
  signal: AbortSignal,
): Promise<ResearchAddress> {
  const normalized = normalizeHostname(hostname);
  const literalFamily = isIP(normalized);
  const addresses =
    literalFamily === 0
      ? await network.resolve(normalized, signal)
      : [{ address: normalized, family: literalFamily as 4 | 6 }];
  if (
    addresses.length === 0 ||
    addresses.some(
      (entry) =>
        (entry.family !== 4 && entry.family !== 6) ||
        isIP(entry.address) !== entry.family ||
        !isPublicIp(entry.address),
    )
  ) {
    throw new WorkerServiceError("blocked_url");
  }
  const first = addresses[0];
  if (first === undefined) throw new WorkerServiceError("blocked_url");
  return first;
}

function isPublicIp(address: string): boolean {
  const family = isIP(address);
  if (family === 4) return isPublicIpv4(address);
  if (family === 6) return isPublicIpv6(address);
  return false;
}

function isPublicIpv4(address: string): boolean {
  const octets = address.split(".").map(Number);
  if (octets.length !== 4 || octets.some((value) => !Number.isInteger(value) || value < 0 || value > 255)) {
    return false;
  }
  const [a = 0, b = 0] = octets;
  return !(
    a === 0 ||
    a === 10 ||
    a === 127 ||
    (a === 100 && b >= 64 && b <= 127) ||
    (a === 169 && b === 254) ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && b === 0) ||
    (a === 192 && b === 168) ||
    (a === 198 && (b === 18 || b === 19)) ||
    (a === 198 && b === 51) ||
    (a === 203 && b === 0) ||
    a >= 224
  );
}

function isPublicIpv6(address: string): boolean {
  const value = ipv6ToBigInt(address);
  if (value === null) return false;
  const blockedRanges: readonly [bigint, number][] = [
    [0n, 96],
    [0x64ff9b00000000000000000000n, 96],
    [0x64ff9b00010000000000000000000000n, 48],
    [0x10000000000000000000000000n, 64],
    [0x20010000000000000000000000000000n, 32],
    [0x20010002000000000000000000000000n, 48],
    [0x20010db8000000000000000000000000n, 32],
    [0x20010010000000000000000000000000n, 28],
    [0x20010020000000000000000000000000n, 28],
    [0x20020000000000000000000000000000n, 16],
    [0xfc000000000000000000000000000000n, 7],
    [0xfe800000000000000000000000000000n, 10],
    [0xfec00000000000000000000000000000n, 10],
    [0xff000000000000000000000000000000n, 8],
  ];
  if (blockedRanges.some(([base, prefix]) => samePrefix(value, base, prefix))) return false;
  const mappedPrefix = 0x00000000000000000000ffff00000000n;
  if (samePrefix(value, mappedPrefix, 96)) {
    const ipv4Value = Number(value & 0xffffffffn);
    return isPublicIpv4([24, 16, 8, 0].map((shift) => String((ipv4Value >>> shift) & 255)).join("."));
  }
  return true;
}

function samePrefix(value: bigint, base: bigint, prefix: number): boolean {
  const shift = BigInt(128 - prefix);
  return value >> shift === base >> shift;
}

function ipv6ToBigInt(address: string): bigint | null {
  const lower = address.toLowerCase();
  const doubleColonParts = lower.split("::");
  if (doubleColonParts.length > 2) return null;
  const parseSide = (side: string): number[] | null => {
    if (side === "") return [];
    const groups: number[] = [];
    for (const token of side.split(":")) {
      if (token.includes(".")) {
        const octets = token.split(".").map(Number);
        if (
          octets.length !== 4 ||
          octets.some((value) => !Number.isInteger(value) || value < 0 || value > 255)
        ) {
          return null;
        }
        groups.push((octets[0] ?? 0) * 256 + (octets[1] ?? 0));
        groups.push((octets[2] ?? 0) * 256 + (octets[3] ?? 0));
      } else {
        if (!/^[0-9a-f]{1,4}$/.test(token)) return null;
        groups.push(Number.parseInt(token, 16));
      }
    }
    return groups;
  };
  const left = parseSide(doubleColonParts[0] ?? "");
  const right = parseSide(doubleColonParts[1] ?? "");
  if (left === null || right === null) return null;
  const hasCompression = doubleColonParts.length === 2;
  const missing = 8 - left.length - right.length;
  if ((!hasCompression && missing !== 0) || (hasCompression && missing < 1)) return null;
  const groups = [...left, ...Array.from({ length: missing }, () => 0), ...right];
  if (groups.length !== 8) return null;
  return groups.reduce((value, group) => (value << 16n) | BigInt(group), 0n);
}

function deduplicateCandidates(
  candidates: readonly { url: string; title?: string }[],
): readonly { url: string; title?: string }[] {
  const seen = new Set<string>();
  return candidates.filter((candidate) => {
    const key = safeDisplayUrl(candidate.url);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function extractSearchResults(html: string, maxResults: number): readonly { url: string; title?: string }[] {
  const results: { url: string; title?: string }[] = [];
  const anchorPattern = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  for (const match of html.matchAll(anchorPattern)) {
    const href = decodeHtmlEntities(match[1] ?? "");
    const url = normalizeSearchResultUrl(href);
    if (url === null) continue;
    const title = extractHtmlText(match[2] ?? "").slice(0, 500);
    results.push({ url, ...(title === "" ? {} : { title }) });
    if (results.length >= maxResults) break;
  }
  return deduplicateCandidates(results);
}

function normalizeSearchResultUrl(href: string): string | null {
  let result: URL;
  try {
    result = new URL(href, "https://html.duckduckgo.com");
  } catch {
    return null;
  }
  if (result.hostname.endsWith("duckduckgo.com")) {
    const redirected = result.searchParams.get("uddg");
    if (redirected === null) return null;
    try {
      result = new URL(redirected);
    } catch {
      return null;
    }
  }
  if (result.protocol !== "https:") return null;
  result.hash = "";
  return result.toString();
}

function extractHtmlTitle(html: string): string {
  const match = /<title\b[^>]*>([\s\S]*?)<\/title>/i.exec(html);
  return extractHtmlText(match?.[1] ?? "").slice(0, 500);
}

function extractHtmlText(html: string): string {
  return decodeHtmlEntities(
    html
      .replace(/<!--[\s\S]*?-->/g, " ")
      .replace(/<(script|style|noscript|svg)\b[^>]*>[\s\S]*?<\/\1>/gi, " ")
      .replace(/<[^>]+>/g, " "),
  )
    .replace(/\s+/g, " ")
    .trim();
}

function decodeHtmlEntities(value: string): string {
  return value
    .replace(/&#x([0-9a-f]+);/gi, (_match, code: string) => safeCodePoint(code, 16))
    .replace(/&#([0-9]+);/g, (_match, code: string) => safeCodePoint(code, 10))
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&amp;/gi, "&");
}

function safeCodePoint(value: string, radix: number): string {
  const codePoint = Number.parseInt(value, radix);
  if (!Number.isFinite(codePoint) || codePoint < 0 || codePoint > 0x10ffff) return "�";
  try {
    return String.fromCodePoint(codePoint);
  } catch {
    return "�";
  }
}

function safeDisplayUrl(rawUrl: string): string {
  try {
    const url = new URL(rawUrl);
    url.username = "";
    url.password = "";
    url.hash = "";
    for (const key of [...url.searchParams.keys()]) {
      if (isSensitiveQueryParameter(key)) url.searchParams.delete(key);
    }
    return url.toString().slice(0, 2_000);
  } catch {
    return "invalid_url";
  }
}

function isSensitiveQueryParameter(key: string): boolean {
  return /(?:^|[_-])(?:access[_-]?token|api[_-]?key|auth|authorization|code|credential|password|secret|signature|token|x[_-]?amz)(?:$|[_-])/i.test(
    key,
  );
}

function abortable<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(new WorkerServiceError("aborted"));
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(new WorkerServiceError("aborted"));
    signal.addEventListener("abort", onAbort, { once: true });
    operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}
