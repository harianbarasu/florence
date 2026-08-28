import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, open, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

/**
 * Adapted port of Hermes Agent's Browserbase provider and agent-browser tool at
 * commit 6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882
 * (`plugins/browser/browserbase/provider.py`, `tools/browser_tool.py`). Florence
 * keeps the Browserbase session lifecycle, task-scoped agent-browser sessions,
 * compact accessibility snapshots, ref-based actions, 500px scrolling,
 * bounded command execution, timeout generation recovery, and human live-view
 * handoff. It intentionally omits Hermes's provider registry, local-browser
 * fallback, terminal/eval/console tools, and generic policy machinery.
 *
 * The exact CLI dependency is agent-browser 0.26.0. Its `--session` isolates
 * the local command daemon while `--cdp` attaches that daemon to the durable
 * Browserbase session.
 */

const DEFAULT_BASE_URL = "https://api.browserbase.com";
const DEFAULT_API_TIMEOUT_MS = 20_000;
const DEFAULT_COMMAND_TIMEOUT_MS = 30_000;
const DEFAULT_OPEN_TIMEOUT_MS = 60_000;
const DEFAULT_SESSION_TIMEOUT_SECONDS = 3_600;
const DEFAULT_MAX_WAIT_MS = 10_000;
const DEFAULT_SNAPSHOT_CHARS = 12_000;
const AGENT_BROWSER_MAX_OUTPUT_CHARS = 15_000;
const DEFAULT_SCREENSHOT_BYTES = 128 * 1_024;
const DEFAULT_SCREENSHOT_QUALITY = 40;
const RETRY_SCREENSHOT_QUALITY = 18;
const MAX_PROVIDER_RESPONSE_BYTES = 1 * 1_024 * 1_024;
const MAX_COMMAND_STDOUT_BYTES = 256 * 1_024;
const MAX_COMMAND_STDERR_BYTES = 64 * 1_024;
const MAX_INPUT_CHARS = 20_000;

export interface FlorenceBrowserSession {
  readonly sessionId: string;
  readonly expiresAt: string;
}

export type FlorenceBrowserOperation =
  | { readonly kind: "navigate"; readonly url: string }
  | { readonly kind: "snapshot"; readonly compact?: boolean }
  | { readonly kind: "click"; readonly ref: string }
  | { readonly kind: "type"; readonly ref: string; readonly text: string }
  | { readonly kind: "select"; readonly ref: string; readonly values: readonly string[] }
  | { readonly kind: "check"; readonly ref: string; readonly checked: boolean }
  | { readonly kind: "press"; readonly key: string }
  | { readonly kind: "scroll"; readonly direction: "up" | "down" }
  | { readonly kind: "wait"; readonly milliseconds: number }
  | { readonly kind: "back" }
  | { readonly kind: "screenshot" }
  | { readonly kind: "owner_handoff" };

export type FlorenceBrowserObservationKind = "page" | "owner_handoff" | "uncertain_effect";

export interface FlorenceBrowserScreenshot {
  readonly mimeType: "image/jpeg";
  readonly bytes: Uint8Array<ArrayBuffer>;
}

export interface FlorenceBrowserObservation {
  readonly kind: FlorenceBrowserObservationKind;
  readonly reason: string | null;
  readonly url: string;
  readonly title: string;
  readonly snapshot: string;
  readonly refCount: number;
  readonly truncated: boolean;
  readonly liveViewUrl?: string;
  readonly screenshot?: FlorenceBrowserScreenshot;
}

export interface FlorenceBrowserRunInput {
  readonly workId: string;
  readonly ownerAdultId: string;
  readonly callId: string;
  readonly attempt: number;
  readonly session: FlorenceBrowserSession | null;
  readonly operation: FlorenceBrowserOperation;
}

export interface FlorenceBrowserRunResult {
  readonly session: FlorenceBrowserSession;
  readonly observation: FlorenceBrowserObservation;
}

export type FlorenceBrowserErrorCode =
  | "invalid_input"
  | "unavailable"
  | "transient"
  | "invalid_response"
  | "session_expired"
  | "cancelled";

export class FlorenceBrowserError extends Error {
  readonly code: FlorenceBrowserErrorCode;
  readonly retryable: boolean;
  readonly safeMessage: string;

  constructor(
    code: FlorenceBrowserErrorCode,
    safeMessage: string,
    options: { readonly retryable?: boolean; readonly cause?: unknown } = {},
  ) {
    super("Florence browser operation failed", { cause: options.cause });
    this.name = "FlorenceBrowserError";
    this.code = code;
    this.retryable = options.retryable ?? (code === "transient" || code === "invalid_response");
    this.safeMessage = boundedString(safeMessage, 500, "Florence could not finish that browser step.");
  }
}

export interface FlorenceBrowserClient {
  run(input: FlorenceBrowserRunInput, signal?: AbortSignal): Promise<FlorenceBrowserRunResult>;
  close(session: FlorenceBrowserSession, signal?: AbortSignal): Promise<void>;
  closeAll(signal?: AbortSignal): Promise<void>;
}

export interface FlorenceBrowserCommandInput {
  readonly executable: string;
  readonly args: readonly string[];
  readonly environment: NodeJS.ProcessEnv;
  readonly timeoutMs: number;
  readonly signal: AbortSignal;
}

export interface FlorenceBrowserCommandResult {
  readonly exitCode: number | null;
  readonly stdout: string;
  readonly stderr: string;
  readonly timedOut: boolean;
  readonly cancelled: boolean;
  readonly stdoutTruncated: boolean;
}

export type FlorenceBrowserCommandRunner = (
  input: FlorenceBrowserCommandInput,
) => Promise<FlorenceBrowserCommandResult>;

export interface BrowserbaseBrowserClientOptions {
  readonly apiKey: string;
  readonly projectId?: string;
  readonly baseUrl?: string;
  readonly executable?: string;
  readonly fetch?: typeof globalThis.fetch;
  readonly commandRunner?: FlorenceBrowserCommandRunner;
  readonly now?: () => number;
  readonly apiTimeoutMs?: number;
  readonly commandTimeoutMs?: number;
  readonly openTimeoutMs?: number;
  readonly sessionTimeoutSeconds?: number;
  readonly maxWaitMs?: number;
  readonly maxSnapshotChars?: number;
  readonly maxScreenshotBytes?: number;
  readonly screenshotQuality?: number;
}

interface BrowserbaseSessionDetails {
  readonly session: FlorenceBrowserSession;
  readonly projectId: string | null;
  readonly connectUrl: string;
  readonly status: string;
}

interface CommandEnvelope {
  readonly data: Readonly<Record<string, unknown>>;
}

interface SnapshotObservation {
  readonly snapshot: string;
  readonly refCount: number;
  readonly truncated: boolean;
}

interface PageMetadata {
  readonly url: string;
  readonly title: string;
}

const UNCERTAIN_RETRY_OPERATIONS = new Set<FlorenceBrowserOperation["kind"]>([
  "click",
  "press",
  "scroll",
  "back",
]);

export class BrowserbaseBrowserClient implements FlorenceBrowserClient {
  readonly #apiKey: string;
  readonly #projectId: string | undefined;
  readonly #baseUrl: string;
  readonly #executable: string;
  readonly #fetch: typeof globalThis.fetch;
  readonly #commandRunner: FlorenceBrowserCommandRunner;
  readonly #now: () => number;
  readonly #apiTimeoutMs: number;
  readonly #commandTimeoutMs: number;
  readonly #openTimeoutMs: number;
  readonly #sessionTimeoutSeconds: number;
  readonly #maxWaitMs: number;
  readonly #maxSnapshotChars: number;
  readonly #maxScreenshotBytes: number;
  readonly #screenshotQuality: number;
  readonly #activeSessions = new Map<
    string,
    { readonly session: FlorenceBrowserSession; readonly projectId: string | null }
  >();
  readonly #commandGenerations = new Map<string, number>();

  constructor(options: BrowserbaseBrowserClientOptions) {
    this.#apiKey = requireNonEmpty(options.apiKey, "Browserbase API key", 10_000);
    this.#projectId = optionalNonEmpty(options.projectId, "Browserbase project ID", 500);
    this.#baseUrl = validBaseUrl(options.baseUrl ?? DEFAULT_BASE_URL);
    this.#executable = requireNonEmpty(
      options.executable ?? "agent-browser",
      "agent-browser executable",
      4_096,
    );
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#commandRunner = options.commandRunner ?? runBrowserCommand;
    this.#now = options.now ?? Date.now;
    this.#apiTimeoutMs = boundedInteger(options.apiTimeoutMs ?? DEFAULT_API_TIMEOUT_MS, 1_000, 60_000);
    this.#commandTimeoutMs = boundedInteger(
      options.commandTimeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS,
      1_000,
      120_000,
    );
    this.#openTimeoutMs = boundedInteger(options.openTimeoutMs ?? DEFAULT_OPEN_TIMEOUT_MS, 1_000, 120_000);
    this.#sessionTimeoutSeconds = boundedInteger(
      options.sessionTimeoutSeconds ?? DEFAULT_SESSION_TIMEOUT_SECONDS,
      60,
      21_600,
    );
    this.#maxWaitMs = boundedInteger(options.maxWaitMs ?? DEFAULT_MAX_WAIT_MS, 250, 30_000);
    this.#maxSnapshotChars = boundedInteger(
      options.maxSnapshotChars ?? DEFAULT_SNAPSHOT_CHARS,
      1_000,
      AGENT_BROWSER_MAX_OUTPUT_CHARS,
    );
    this.#maxScreenshotBytes = boundedInteger(
      options.maxScreenshotBytes ?? DEFAULT_SCREENSHOT_BYTES,
      16 * 1_024,
      160 * 1_024,
    );
    this.#screenshotQuality = boundedInteger(options.screenshotQuality ?? DEFAULT_SCREENSHOT_QUALITY, 10, 60);
  }

  async run(input: FlorenceBrowserRunInput, signal?: AbortSignal): Promise<FlorenceBrowserRunResult> {
    const localSignal = signal ?? new AbortController().signal;
    throwIfAborted(localSignal);
    validateRunInput(input);

    let sessionDetails: BrowserbaseSessionDetails;
    try {
      sessionDetails = await this.#resolveSession(input, localSignal);
    } catch (error) {
      throw asBrowserError(error, localSignal);
    }

    const session = sessionDetails.session;
    const createdThisRun = session.sessionId !== input.session?.sessionId;
    this.#activeSessions.set(session.sessionId, {
      session,
      projectId: sessionDetails.projectId,
    });

    let actionMayHaveHappened = false;
    try {
      if (input.attempt > 1 && UNCERTAIN_RETRY_OPERATIONS.has(input.operation.kind)) {
        try {
          await this.#recycleCommandGeneration(session.sessionId);
          const observation = await this.#observePage(
            sessionDetails,
            input.operation,
            localSignal,
            "uncertain_effect",
            `Florence did not repeat ${input.operation.kind} because its earlier effect is uncertain; the current page was read again instead.`,
          );
          return { session, observation };
        } catch (error) {
          if (localSignal.aborted) throw asBrowserError(error, localSignal);
          return {
            session,
            observation: unreadableUncertainObservation(input.operation.kind, error),
          };
        }
      }

      const screenshot =
        input.operation.kind === "screenshot"
          ? await this.#captureScreenshot(sessionDetails, localSignal)
          : undefined;

      if (input.operation.kind !== "snapshot" && input.operation.kind !== "screenshot") {
        if (input.operation.kind !== "owner_handoff") {
          actionMayHaveHappened = UNCERTAIN_RETRY_OPERATIONS.has(input.operation.kind);
          await this.#performOperation(sessionDetails, input.operation, localSignal);
        }
      }

      const observationKind: FlorenceBrowserObservationKind =
        input.operation.kind === "owner_handoff" ? "owner_handoff" : "page";
      const observation = await this.#observePage(
        sessionDetails,
        input.operation,
        localSignal,
        observationKind,
        input.operation.kind === "owner_handoff"
          ? "The owner can take over this same live browser session, then tell Florence to continue."
          : null,
        screenshot,
      );
      return { session, observation };
    } catch (error) {
      const browserError = asBrowserError(error, localSignal);
      if (browserError.code === "invalid_input") {
        if (createdThisRun) await this.#bestEffortClose(session);
        throw browserError;
      }
      if (actionMayHaveHappened && !localSignal.aborted) {
        try {
          await this.#recycleCommandGeneration(session.sessionId);
          const observation = await this.#observePage(
            sessionDetails,
            input.operation,
            localSignal,
            "uncertain_effect",
            `The ${input.operation.kind} may already have happened; Florence read the current page instead of repeating it.`,
          );
          return { session, observation };
        } catch (observationError) {
          if (localSignal.aborted) throw asBrowserError(observationError, localSignal);
          return {
            session,
            observation: unreadableUncertainObservation(input.operation.kind, observationError),
          };
        }
      }
      if (createdThisRun) await this.#bestEffortClose(session);
      throw browserError;
    }
  }

  async close(session: FlorenceBrowserSession, signal?: AbortSignal): Promise<void> {
    const localSignal = signal ?? new AbortController().signal;
    validateSession(session);
    throwIfAborted(localSignal);

    await this.#stopAgentBrowserDaemon(session.sessionId, localSignal);

    let projectId = this.#activeSessions.get(session.sessionId)?.projectId ?? this.#projectId ?? null;
    if (!projectId) {
      try {
        projectId = (await this.#readSession(session.sessionId, localSignal)).projectId;
      } catch (error) {
        if (error instanceof FlorenceBrowserError && error.code === "session_expired") {
          this.#forgetSession(session.sessionId);
          return;
        }
        throw asBrowserError(error, localSignal);
      }
    }

    const body: Record<string, unknown> = { status: "REQUEST_RELEASE" };
    if (projectId) body.projectId = projectId;
    const response = await this.#request(`/v1/sessions/${encodeURIComponent(session.sessionId)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: localSignal,
    });
    if (!response.ok && response.status !== 404 && response.status !== 410) {
      throw httpError(response.status, "Browserbase could not release the browser session.");
    }
    this.#forgetSession(session.sessionId);
  }

  async closeAll(signal?: AbortSignal): Promise<void> {
    const sessions = [...this.#activeSessions.values()].map((entry) => entry.session);
    const results = await Promise.allSettled(sessions.map((session) => this.close(session, signal)));
    const firstFailure = results.find((result) => result.status === "rejected");
    if (firstFailure?.status === "rejected") throw firstFailure.reason;
  }

  async #resolveSession(
    input: FlorenceBrowserRunInput,
    signal: AbortSignal,
  ): Promise<BrowserbaseSessionDetails> {
    if (!input.session) {
      if (input.operation.kind !== "navigate") {
        throw new FlorenceBrowserError("invalid_input", "Start this browser task by navigating to a page.");
      }
      return this.#createSession(input, signal);
    }

    validateSession(input.session);
    if (sessionExpired(input.session, this.#now())) {
      if (input.operation.kind === "navigate") {
        this.#forgetSession(input.session.sessionId);
        return this.#createSession(input, signal);
      }
      throw new FlorenceBrowserError(
        "session_expired",
        "That browser session expired. Navigate to the page again to continue.",
      );
    }

    try {
      return await this.#readSession(input.session.sessionId, signal);
    } catch (error) {
      if (
        error instanceof FlorenceBrowserError &&
        error.code === "session_expired" &&
        input.operation.kind === "navigate"
      ) {
        this.#forgetSession(input.session.sessionId);
        return this.#createSession(input, signal);
      }
      throw error;
    }
  }

  async #createSession(
    input: FlorenceBrowserRunInput,
    signal: AbortSignal,
  ): Promise<BrowserbaseSessionDetails> {
    const body: Record<string, unknown> = {
      keepAlive: true,
      timeout: this.#sessionTimeoutSeconds,
      userMetadata: {
        florenceWorkId: input.workId,
        ownerAdultId: input.ownerAdultId,
      },
    };
    if (this.#projectId) body.projectId = this.#projectId;

    let response = await this.#request("/v1/sessions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal,
    });
    if (response.status === 402 && body.keepAlive === true) {
      delete body.keepAlive;
      response = await this.#request("/v1/sessions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal,
      });
    }
    if (!response.ok) throw httpError(response.status, "Browserbase could not start a browser session.");
    const details = parseSessionDetails(await readJsonResponse(response), "created");
    if (details.status !== "RUNNING" && details.status !== "PENDING") {
      throw new FlorenceBrowserError("unavailable", "Browserbase could not start a running browser session.");
    }
    this.#commandGenerations.set(details.session.sessionId, 0);
    return details;
  }

  async #readSession(sessionId: string, signal: AbortSignal): Promise<BrowserbaseSessionDetails> {
    const response = await this.#request(`/v1/sessions/${encodeURIComponent(sessionId)}`, {
      method: "GET",
      signal,
    });
    if (response.status === 404 || response.status === 410) {
      throw new FlorenceBrowserError(
        "session_expired",
        "That browser session is no longer running. Navigate to the page again to continue.",
      );
    }
    if (!response.ok) throw httpError(response.status, "Browserbase could not resume the browser session.");
    const details = parseSessionDetails(await readJsonResponse(response), "resumed");
    if (details.status !== "RUNNING" && details.status !== "PENDING") {
      throw new FlorenceBrowserError(
        "session_expired",
        "That browser session is no longer running. Navigate to the page again to continue.",
      );
    }
    return details;
  }

  async #performOperation(
    session: BrowserbaseSessionDetails,
    operation: Exclude<
      FlorenceBrowserOperation,
      { readonly kind: "snapshot" | "screenshot" | "owner_handoff" }
    >,
    signal: AbortSignal,
  ): Promise<void> {
    let command: string;
    let args: readonly string[];
    let timeoutMs = this.#commandTimeoutMs;

    switch (operation.kind) {
      case "navigate":
        command = "open";
        args = [validNavigationUrl(operation.url)];
        timeoutMs = this.#openTimeoutMs;
        break;
      case "click":
        command = "click";
        args = [normalizeRef(operation.ref)];
        break;
      case "type":
        command = "fill";
        args = [normalizeRef(operation.ref), boundedInput(operation.text, "Browser text")];
        break;
      case "select":
        if (operation.values.length < 1 || operation.values.length > 20) {
          throw new FlorenceBrowserError("invalid_input", "Choose between one and twenty dropdown values.");
        }
        command = "select";
        args = [
          normalizeRef(operation.ref),
          ...operation.values.map((value) => boundedInput(value, "Dropdown value")),
        ];
        break;
      case "check":
        command = operation.checked ? "check" : "uncheck";
        args = [normalizeRef(operation.ref)];
        break;
      case "press":
        command = "press";
        args = [boundedString(operation.key.trim(), 100, "Enter")];
        break;
      case "scroll":
        command = "scroll";
        args = [operation.direction, "500"];
        break;
      case "wait":
        if (
          !Number.isInteger(operation.milliseconds) ||
          operation.milliseconds < 0 ||
          operation.milliseconds > this.#maxWaitMs
        ) {
          throw new FlorenceBrowserError(
            "invalid_input",
            `Browser waits must be between 0 and ${this.#maxWaitMs} milliseconds.`,
          );
        }
        command = "wait";
        args = [String(operation.milliseconds)];
        timeoutMs = Math.max(this.#commandTimeoutMs, operation.milliseconds + 5_000);
        break;
      case "back":
        command = "back";
        args = [];
        break;
    }

    await this.#agentBrowser(session, command, args, signal, timeoutMs);
  }

  async #observePage(
    session: BrowserbaseSessionDetails,
    operation: FlorenceBrowserOperation,
    signal: AbortSignal,
    kind: FlorenceBrowserObservationKind,
    reason: string | null,
    screenshot?: FlorenceBrowserScreenshot,
  ): Promise<FlorenceBrowserObservation> {
    const snapshotArgs = operation.kind === "snapshot" && operation.compact === false ? [] : ["-c"];
    const snapshotEnvelope = await this.#agentBrowser(
      session,
      "snapshot",
      snapshotArgs,
      signal,
      this.#commandTimeoutMs,
    );
    const snapshot = parseSnapshot(snapshotEnvelope.data, this.#maxSnapshotChars);
    const metadata = await this.#readPageMetadata(session, signal);

    let liveViewUrl: string | undefined;
    if (kind === "owner_handoff") {
      liveViewUrl = await this.#readLiveViewUrl(session.session.sessionId, signal);
    }

    return {
      kind,
      reason,
      url: metadata.url,
      title: metadata.title,
      snapshot: snapshot.snapshot,
      refCount: snapshot.refCount,
      truncated: snapshot.truncated,
      ...(liveViewUrl ? { liveViewUrl } : {}),
      ...(screenshot ? { screenshot } : {}),
    };
  }

  async #readPageMetadata(session: BrowserbaseSessionDetails, signal: AbortSignal): Promise<PageMetadata> {
    const urlResult = await this.#agentBrowser(session, "get", ["url"], signal, this.#commandTimeoutMs);
    const titleResult = await this.#agentBrowser(session, "get", ["title"], signal, this.#commandTimeoutMs);
    return {
      url: readBoundedRecordString(urlResult.data, "url", 4_096),
      title: readBoundedRecordString(titleResult.data, "title", 2_000),
    };
  }

  async #captureScreenshot(
    session: BrowserbaseSessionDetails,
    signal: AbortSignal,
  ): Promise<FlorenceBrowserScreenshot> {
    const directory = await mkdtemp(join(tmpdir(), "florence-browser-shot-"));
    const path = join(directory, "viewport.jpg");
    try {
      for (const quality of [...new Set([this.#screenshotQuality, RETRY_SCREENSHOT_QUALITY])]) {
        await this.#agentBrowser(session, "screenshot", [path], signal, this.#commandTimeoutMs, [
          "--screenshot-format",
          "jpeg",
          "--screenshot-quality",
          String(quality),
        ]);
        const bytes = await readBoundedFile(path, this.#maxScreenshotBytes);
        if (bytes) return { mimeType: "image/jpeg", bytes };
      }
      throw new FlorenceBrowserError(
        "invalid_response",
        "The browser screenshot was too large to keep with this task.",
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }

  async #readLiveViewUrl(sessionId: string, signal: AbortSignal): Promise<string> {
    const response = await this.#request(`/v1/sessions/${encodeURIComponent(sessionId)}/debug`, {
      method: "GET",
      signal,
    });
    if (!response.ok) throw httpError(response.status, "Browserbase could not open the live browser view.");
    const value = await readJsonResponse(response);
    if (!isRecord(value))
      throw invalidProviderResponse("Browserbase returned an unreadable live browser view.");
    return validHttpUrl(value.debuggerFullscreenUrl, "Browserbase returned an invalid live browser view.");
  }

  async #agentBrowser(
    session: BrowserbaseSessionDetails,
    command: string,
    commandArguments: readonly string[],
    signal: AbortSignal,
    timeoutMs: number,
    extraGlobalArguments: readonly string[] = [],
  ): Promise<CommandEnvelope> {
    const sessionName = this.#commandSessionName(session.session.sessionId);
    const socketDirectory = commandSocketDirectory();
    await mkdir(socketDirectory, { recursive: true, mode: 0o700 });
    const args = [
      "--session",
      sessionName,
      "--cdp",
      session.connectUrl,
      "--json",
      "--max-output",
      String(AGENT_BROWSER_MAX_OUTPUT_CHARS),
      ...extraGlobalArguments,
      command,
      ...commandArguments,
    ];

    let result: FlorenceBrowserCommandResult;
    try {
      result = await this.#commandRunner({
        executable: this.#executable,
        args,
        environment: {
          ...process.env,
          AGENT_BROWSER_SOCKET_DIR: socketDirectory,
          AGENT_BROWSER_IDLE_TIMEOUT_MS: String(this.#sessionTimeoutSeconds * 1_000),
        },
        timeoutMs,
        signal,
      });
    } catch (error) {
      if (signal.aborted) {
        throw new FlorenceBrowserError("cancelled", "Browser work was cancelled.", { cause: error });
      }
      await this.#recycleCommandGeneration(session.session.sessionId);
      throw new FlorenceBrowserError("transient", "The browser command could not start. Please try again.", {
        retryable: true,
        cause: error,
      });
    }

    if (result.cancelled || signal.aborted) {
      throw new FlorenceBrowserError("cancelled", "Browser work was cancelled.");
    }
    if (result.timedOut) {
      await this.#recycleCommandGeneration(session.session.sessionId);
      throw new FlorenceBrowserError(
        "transient",
        "The browser took too long. Florence can reconnect and try again.",
        {
          retryable: true,
        },
      );
    }
    if (result.stdoutTruncated) {
      await this.#recycleCommandGeneration(session.session.sessionId);
      throw invalidProviderResponse("The browser returned more command data than Florence could read.");
    }

    let envelope: ParsedCommandEnvelope;
    try {
      envelope = parseCommandEnvelope(result.stdout);
    } catch (error) {
      await this.#recycleCommandGeneration(session.session.sessionId);
      throw error;
    }
    if (result.exitCode !== 0 || !envelope.success) {
      const failure = commandFailure(envelope.error);
      if (failure.code !== "invalid_input") {
        await this.#recycleCommandGeneration(session.session.sessionId);
      }
      throw failure;
    }
    return { data: envelope.data };
  }

  async #stopAgentBrowserDaemon(sessionId: string, signal: AbortSignal): Promise<void> {
    const sessionName = this.#commandSessionName(sessionId);
    await this.#stopAgentBrowserSession(sessionName, signal);
  }

  async #stopAgentBrowserSession(
    sessionName: string,
    signal: AbortSignal = new AbortController().signal,
  ): Promise<void> {
    const socketDirectory = commandSocketDirectory();
    let closed = false;
    try {
      const result = await this.#commandRunner({
        executable: this.#executable,
        args: [
          "--session",
          sessionName,
          "--json",
          "--max-output",
          String(AGENT_BROWSER_MAX_OUTPUT_CHARS),
          "close",
        ],
        environment: { ...process.env, AGENT_BROWSER_SOCKET_DIR: socketDirectory },
        timeoutMs: Math.min(this.#commandTimeoutMs, 10_000),
        signal,
      });
      if (!result.cancelled && !result.timedOut && !result.stdoutTruncated && result.exitCode === 0) {
        closed = parseCommandEnvelope(result.stdout).success;
      }
    } catch {
      // Releasing Browserbase below is authoritative; daemon cleanup is best effort.
    }
    if (closed) {
      await Promise.allSettled(
        ["pid", "stream", "engine", "version", "sock"].map((suffix) =>
          rm(join(socketDirectory, `${sessionName}.${suffix}`), { force: true }),
        ),
      );
    }
  }

  async #request(path: string, init: RequestInit & { readonly signal: AbortSignal }): Promise<Response> {
    const timeoutController = new AbortController();
    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      timeoutController.abort();
    }, this.#apiTimeoutMs);
    const combinedSignal = AbortSignal.any([init.signal, timeoutController.signal]);
    try {
      return await this.#fetch(`${this.#baseUrl}${path}`, {
        ...init,
        headers: {
          Accept: "application/json",
          "X-BB-API-Key": this.#apiKey,
          ...init.headers,
        },
        signal: combinedSignal,
      });
    } catch (error) {
      if (init.signal.aborted) {
        throw new FlorenceBrowserError("cancelled", "Browser work was cancelled.", { cause: error });
      }
      throw new FlorenceBrowserError(
        "transient",
        timedOut
          ? "Browserbase took too long to respond. Please try again."
          : "Browserbase is temporarily unavailable. Please try again.",
        { retryable: true, cause: error },
      );
    } finally {
      clearTimeout(timeout);
    }
  }

  #commandSessionName(sessionId: string): string {
    const generation = this.#commandGenerations.get(sessionId) ?? 0;
    return `florence_${digest(sessionId)}_${generation}`;
  }

  async #recycleCommandGeneration(sessionId: string): Promise<void> {
    const sessionName = this.#commandSessionName(sessionId);
    await this.#stopAgentBrowserSession(sessionName);
    this.#commandGenerations.set(sessionId, (this.#commandGenerations.get(sessionId) ?? 0) + 1);
  }

  #forgetSession(sessionId: string): void {
    this.#activeSessions.delete(sessionId);
    this.#commandGenerations.delete(sessionId);
  }

  async #bestEffortClose(session: FlorenceBrowserSession): Promise<void> {
    try {
      await this.close(session);
    } catch {
      // Provider expiry remains the final cleanup bound when release fails.
    }
  }
}

interface ParsedCommandEnvelope {
  readonly success: boolean;
  readonly data: Readonly<Record<string, unknown>>;
  readonly error: string | null;
}

function parseCommandEnvelope(text: string): ParsedCommandEnvelope {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch (error) {
    throw invalidProviderResponse("agent-browser returned an unreadable result.", error);
  }
  if (!isRecord(value) || typeof value.success !== "boolean") {
    throw invalidProviderResponse("agent-browser returned an incomplete result.");
  }
  const data = isRecord(value.data) ? value.data : {};
  const error = typeof value.error === "string" ? boundedString(value.error, 1_000, "") : null;
  return { success: value.success, data, error };
}

function parseSnapshot(data: Readonly<Record<string, unknown>>, maximumChars: number): SnapshotObservation {
  if (typeof data.snapshot !== "string") {
    throw invalidProviderResponse("agent-browser returned an incomplete page snapshot.");
  }
  const refs = data.refs;
  const refCount = isRecord(refs) ? Object.keys(refs).length : Array.isArray(refs) ? refs.length : 0;
  const truncated = truncateSnapshot(data.snapshot, maximumChars);
  return {
    snapshot: truncated.value,
    refCount,
    truncated: truncated.truncated,
  };
}

function truncateSnapshot(value: string, maximumChars: number): { value: string; truncated: boolean } {
  const providerTruncated = value.includes("[truncated: showing ");
  if (value.length <= maximumChars) return { value, truncated: providerTruncated };

  const note = "\n[... accessibility snapshot truncated; take another snapshot after narrowing the page ...]";
  const budget = Math.max(1, maximumChars - note.length);
  const candidate = value.slice(0, budget);
  const lineBoundary = candidate.lastIndexOf("\n");
  const prefix = lineBoundary > 0 ? candidate.slice(0, lineBoundary) : candidate;
  return { value: `${prefix}${note}`, truncated: true };
}

function parseSessionDetails(value: unknown, context: "created" | "resumed"): BrowserbaseSessionDetails {
  if (!isRecord(value)) {
    throw invalidProviderResponse(`Browserbase returned an unreadable ${context} session.`);
  }
  const sessionId = readBoundedRecordString(value, "id", 500);
  const expiresAt = readBoundedRecordString(value, "expiresAt", 100);
  const connectUrl = validCdpUrl(value.connectUrl);
  const projectId =
    typeof value.projectId === "string" ? boundedString(value.projectId, 500, "") || null : null;
  const status = typeof value.status === "string" ? boundedString(value.status, 100, "") : "RUNNING";
  if (!Number.isFinite(Date.parse(expiresAt))) {
    throw invalidProviderResponse(`Browserbase returned an invalid ${context} session expiry.`);
  }
  return {
    session: { sessionId, expiresAt },
    projectId,
    connectUrl,
    status,
  };
}

async function readJsonResponse(response: Response): Promise<unknown> {
  const contentLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(contentLength) && contentLength > MAX_PROVIDER_RESPONSE_BYTES) {
    throw invalidProviderResponse("Browserbase returned too much session data.");
  }
  const text = await response.text();
  if (Buffer.byteLength(text, "utf8") > MAX_PROVIDER_RESPONSE_BYTES) {
    throw invalidProviderResponse("Browserbase returned too much session data.");
  }
  try {
    return JSON.parse(text);
  } catch (error) {
    throw invalidProviderResponse("Browserbase returned unreadable session data.", error);
  }
}

function commandFailure(providerError: string | null): FlorenceBrowserError {
  const normalized = providerError?.toLowerCase() ?? "";
  if (normalized.includes("ref") || normalized.includes("element") || normalized.includes("not found")) {
    return new FlorenceBrowserError(
      "invalid_input",
      "That page element is no longer available. Read the current snapshot and use its latest reference.",
    );
  }
  return new FlorenceBrowserError(
    "transient",
    "The browser could not complete that page action. Read the current page and try again.",
    { retryable: true },
  );
}

function httpError(status: number, message: string): FlorenceBrowserError {
  const transient = status === 408 || status === 409 || status === 425 || status === 429 || status >= 500;
  return new FlorenceBrowserError(transient ? "transient" : "unavailable", message, {
    retryable: transient,
  });
}

function invalidProviderResponse(message: string, cause?: unknown): FlorenceBrowserError {
  return new FlorenceBrowserError("invalid_response", message, { retryable: true, cause });
}

function asBrowserError(error: unknown, signal: AbortSignal): FlorenceBrowserError {
  if (error instanceof FlorenceBrowserError) return error;
  if (signal.aborted) {
    return new FlorenceBrowserError("cancelled", "Browser work was cancelled.", { cause: error });
  }
  return new FlorenceBrowserError("transient", "The browser is temporarily unavailable.", {
    retryable: true,
    cause: error,
  });
}

function unreadableUncertainObservation(
  operation: FlorenceBrowserOperation["kind"],
  _cause?: unknown,
): FlorenceBrowserObservation {
  return {
    kind: "uncertain_effect",
    reason: `The ${operation} may already have happened, but Florence could not read the current page. Take a fresh snapshot before another action.`,
    url: "",
    title: "",
    snapshot: "",
    refCount: 0,
    truncated: false,
  };
}

function validateRunInput(input: FlorenceBrowserRunInput): void {
  validateNonEmptyInput(input.workId, "Browser work ID", 500);
  validateNonEmptyInput(input.ownerAdultId, "Browser owner adult ID", 500);
  validateNonEmptyInput(input.callId, "Browser call ID", 500);
  if (!Number.isInteger(input.attempt) || input.attempt < 1 || input.attempt > 100) {
    throw new FlorenceBrowserError("invalid_input", "Browser attempt must be a positive integer.");
  }
}

function validateSession(session: FlorenceBrowserSession): void {
  validateNonEmptyInput(session.sessionId, "Browser session ID", 500);
  if (!Number.isFinite(Date.parse(session.expiresAt))) {
    throw new FlorenceBrowserError("invalid_input", "The saved browser session has an invalid expiry.");
  }
}

function validateNonEmptyInput(value: string, label: string, maximumChars: number): void {
  if (typeof value !== "string" || !value.trim() || value.length > maximumChars) {
    throw new FlorenceBrowserError(
      "invalid_input",
      `${label} is required and must be at most ${maximumChars} characters.`,
    );
  }
}

function sessionExpired(session: FlorenceBrowserSession, now: number): boolean {
  return Date.parse(session.expiresAt) <= now;
}

function validNavigationUrl(value: string): string {
  let url: URL;
  try {
    url = new URL(value.trim());
  } catch (error) {
    throw new FlorenceBrowserError("invalid_input", "Navigate with a complete HTTP or HTTPS URL.", {
      cause: error,
    });
  }
  if ((url.protocol !== "http:" && url.protocol !== "https:") || url.username || url.password) {
    throw new FlorenceBrowserError("invalid_input", "Navigate with a complete HTTP or HTTPS URL.");
  }
  return url.href;
}

function validBaseUrl(value: string): string {
  let url: URL;
  try {
    url = new URL(value.trim());
  } catch (error) {
    throw new TypeError("Browserbase base URL is invalid", { cause: error });
  }
  if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash) {
    throw new TypeError("Browserbase base URL must be an HTTPS origin");
  }
  return url.href.replace(/\/$/, "");
}

function validCdpUrl(value: unknown): string {
  if (typeof value !== "string") {
    throw invalidProviderResponse("Browserbase did not return a browser connection.");
  }
  let url: URL;
  try {
    url = new URL(value);
  } catch (error) {
    throw invalidProviderResponse("Browserbase returned an invalid browser connection.", error);
  }
  if (
    url.protocol !== "wss:" &&
    url.protocol !== "ws:" &&
    url.protocol !== "https:" &&
    url.protocol !== "http:"
  ) {
    throw invalidProviderResponse("Browserbase returned an invalid browser connection.");
  }
  return url.href;
}

function validHttpUrl(value: unknown, message: string): string {
  if (typeof value !== "string") throw invalidProviderResponse(message);
  try {
    const url = new URL(value);
    if (url.protocol !== "https:" && url.protocol !== "http:") throw new Error("Invalid protocol");
    return url.href;
  } catch (error) {
    throw invalidProviderResponse(message, error);
  }
}

function normalizeRef(value: string): string {
  const normalized = value.trim().replace(/^@/, "");
  const withPrefix = /^\d+$/.test(normalized) ? `e${normalized}` : normalized;
  if (!/^e\d+$/.test(withPrefix)) {
    throw new FlorenceBrowserError(
      "invalid_input",
      "Use an element reference from the current snapshot, such as @e3.",
    );
  }
  return `@${withPrefix}`;
}

function boundedInput(value: string, label: string): string {
  if (typeof value !== "string" || value.length > MAX_INPUT_CHARS) {
    throw new FlorenceBrowserError("invalid_input", `${label} is too long.`);
  }
  return value;
}

function requireNonEmpty(value: string, label: string, maximumChars: number): string {
  if (typeof value !== "string" || !value.trim() || value.length > maximumChars) {
    throw new TypeError(`${label} is required and must be at most ${maximumChars} characters`);
  }
  return value.trim();
}

function optionalNonEmpty(
  value: string | undefined,
  label: string,
  maximumChars: number,
): string | undefined {
  if (value === undefined) return undefined;
  return requireNonEmpty(value, label, maximumChars);
}

function boundedString(value: string, maximumChars: number, fallback: string): string {
  const trimmed = value.trim();
  if (!trimmed) return fallback;
  return trimmed.length <= maximumChars ? trimmed : trimmed.slice(0, maximumChars);
}

function boundedInteger(value: number, minimum: number, maximum: number): number {
  if (!Number.isFinite(value)) return minimum;
  return Math.min(maximum, Math.max(minimum, Math.trunc(value)));
}

function readBoundedRecordString(
  value: Readonly<Record<string, unknown>>,
  key: string,
  maximumChars: number,
): string {
  const candidate = value[key];
  if (typeof candidate !== "string" || candidate.length > maximumChars) {
    throw invalidProviderResponse(`agent-browser returned an invalid page ${key}.`);
  }
  return candidate;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function digest(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 16);
}

function commandSocketDirectory(): string {
  return join(tmpdir(), "fb");
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) throw new FlorenceBrowserError("cancelled", "Browser work was cancelled.");
}

async function readBoundedFile(path: string, maximumBytes: number): Promise<Uint8Array<ArrayBuffer> | null> {
  const handle = await open(path, "r");
  try {
    const buffer = Buffer.alloc(maximumBytes + 1);
    const { bytesRead } = await handle.read(buffer, 0, buffer.byteLength, 0);
    if (bytesRead < 4 || bytesRead > maximumBytes) return null;
    if (buffer[0] !== 0xff || buffer[1] !== 0xd8) {
      throw invalidProviderResponse("agent-browser did not return a JPEG screenshot.");
    }
    return new Uint8Array(buffer.subarray(0, bytesRead));
  } finally {
    await handle.close();
  }
}

async function readBoundedOutputFile(
  path: string,
  maximumBytes: number,
): Promise<{ readonly value: string; readonly truncated: boolean }> {
  const handle = await open(path, "r");
  try {
    const buffer = Buffer.alloc(maximumBytes + 1);
    const { bytesRead } = await handle.read(buffer, 0, buffer.byteLength, 0);
    const truncated = bytesRead > maximumBytes;
    return {
      value: buffer.subarray(0, Math.min(bytesRead, maximumBytes)).toString("utf8"),
      truncated,
    };
  } finally {
    await handle.close();
  }
}

async function runBrowserCommand(input: FlorenceBrowserCommandInput): Promise<FlorenceBrowserCommandResult> {
  throwIfAborted(input.signal);
  const outputDirectory = await mkdtemp(join(tmpdir(), "florence-browser-command-"));
  const stdoutPath = join(outputDirectory, "stdout");
  const stderrPath = join(outputDirectory, "stderr");
  const stdoutHandle = await open(stdoutPath, "w", 0o600);
  const stderrHandle = await open(stderrPath, "w", 0o600);

  let child: ReturnType<typeof spawn>;
  try {
    child = spawn(input.executable, input.args, {
      shell: false,
      stdio: ["ignore", stdoutHandle.fd, stderrHandle.fd],
      env: input.environment,
      windowsHide: true,
      detached: process.platform !== "win32",
    });
  } catch (error) {
    await stdoutHandle.close();
    await stderrHandle.close();
    await rm(outputDirectory, { recursive: true, force: true });
    throw error;
  }
  await stdoutHandle.close();
  await stderrHandle.close();

  let timedOut = false;
  let cancelled = false;
  const kill = () => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    try {
      if (process.platform !== "win32" && child.pid) process.kill(-child.pid, "SIGKILL");
      else child.kill("SIGKILL");
    } catch {
      try {
        child.kill("SIGKILL");
      } catch {
        // The process already exited.
      }
    }
  };

  try {
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        timedOut = true;
        kill();
      }, input.timeoutMs);
      const abort = () => {
        cancelled = true;
        kill();
      };
      const cleanup = () => {
        clearTimeout(timeout);
        input.signal.removeEventListener("abort", abort);
      };
      input.signal.addEventListener("abort", abort, { once: true });
      child.once("error", (error) => {
        cleanup();
        reject(error);
      });
      child.once("close", () => {
        cleanup();
        resolve();
      });
    });

    const stdout = await readBoundedOutputFile(stdoutPath, MAX_COMMAND_STDOUT_BYTES);
    const stderr = await readBoundedOutputFile(stderrPath, MAX_COMMAND_STDERR_BYTES);
    return {
      exitCode: child.exitCode,
      stdout: stdout.value,
      stderr: stderr.value,
      timedOut,
      cancelled,
      stdoutTruncated: stdout.truncated,
    };
  } finally {
    kill();
    await rm(outputDirectory, { recursive: true, force: true });
  }
}
