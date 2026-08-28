import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, open, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Kernel from "@onkernel/sdk";
import type { ComputerBatchParams } from "@onkernel/sdk/resources/browsers/computer";

/**
 * Kernel-first browser execution is directly adapted from OpenInstinct commit
 * 480045dbc63008e7f99313d1683858cd8657b35a (`manage_browsers.ts`,
 * `execute_playwright_code.ts`, `computer_action.ts`, and
 * `capture_browser_image.ts`). Florence keeps OpenInstinct's persistent profile,
 * in-browser Playwright, native computer control, visual observation, and live
 * handoff primitives inside Florence's existing durable PostgreSQL work loop.
 *
 * Existing ref-based operations remain an adapted port of Hermes Agent commit
 * 6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882. The exact CLI dependency is
 * agent-browser 0.26.0; its task-scoped daemon attaches over CDP to either
 * provider. Browserbase remains only as a rollout fallback when Kernel is not
 * configured. Florence intentionally omits both upstreams' provider registries,
 * second agent runtimes, and arbitrary total-task or tool-call ceilings.
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
const MAX_UPLOAD_BYTES = 20 * 1_024 * 1_024;
const MAX_UPLOAD_FILENAME_BYTES = 255;
const DEFAULT_KERNEL_SESSION_TIMEOUT_SECONDS = 259_200;
const DEFAULT_KERNEL_PLAYWRIGHT_TIMEOUT_SECONDS = 60;
const MAX_KERNEL_PLAYWRIGHT_TIMEOUT_SECONDS = 300;
const MAX_PLAYWRIGHT_CODE_CHARS = 50_000;
const MAX_COMPUTER_ACTIONS_PER_CALL = 50;
const MAX_BROWSER_IMAGE_BYTES = MAX_UPLOAD_BYTES;

export interface FlorenceBrowserSession {
  readonly sessionId: string;
  readonly expiresAt: string;
}

export type FlorenceBrowserOperation =
  | { readonly kind: "navigate"; readonly url: string }
  | { readonly kind: "snapshot"; readonly compact?: boolean }
  | { readonly kind: "click"; readonly ref: string }
  | { readonly kind: "type"; readonly ref: string; readonly text: string }
  | { readonly kind: "upload"; readonly ref: string; readonly attachmentRef: string }
  | { readonly kind: "select"; readonly ref: string; readonly values: readonly string[] }
  | { readonly kind: "check"; readonly ref: string; readonly checked: boolean }
  | { readonly kind: "press"; readonly key: string }
  | { readonly kind: "scroll"; readonly direction: "up" | "down" }
  | { readonly kind: "wait"; readonly milliseconds: number }
  | { readonly kind: "back" }
  | { readonly kind: "screenshot" }
  | {
      readonly kind: "capture";
      readonly source: "viewport" | "full_page" | "element" | "image_resource";
      readonly label: string;
      readonly selector?: string;
      readonly region?: {
        readonly x: number;
        readonly y: number;
        readonly width: number;
        readonly height: number;
      };
    }
  | {
      readonly kind: "playwright";
      readonly code: string;
      readonly timeoutSeconds?: number;
    }
  | {
      readonly kind: "computer";
      readonly actions: readonly FlorenceBrowserComputerAction[];
      readonly screenshot: boolean;
    }
  | { readonly kind: "owner_handoff" };

export type FlorenceBrowserComputerAction =
  | {
      readonly type: "click_mouse";
      readonly x: number;
      readonly y: number;
      readonly button?: "left" | "right" | "middle";
      readonly clickType?: "down" | "up" | "click";
      readonly numClicks?: number;
    }
  | { readonly type: "move_mouse"; readonly x: number; readonly y: number }
  | { readonly type: "type_text"; readonly text: string }
  | { readonly type: "press_key"; readonly keys: readonly string[] }
  | {
      readonly type: "scroll";
      readonly x: number;
      readonly y: number;
      readonly deltaX?: number;
      readonly deltaY?: number;
    }
  | {
      readonly type: "drag_mouse";
      readonly path: readonly { readonly x: number; readonly y: number }[];
      readonly button?: "left" | "right" | "middle";
    }
  | { readonly type: "sleep"; readonly milliseconds: number };

export type FlorenceBrowserObservationKind = "page" | "owner_handoff" | "uncertain_effect";

export interface FlorenceBrowserScreenshot {
  readonly mimeType: "image/jpeg" | "image/png" | "image/webp";
  readonly bytes: Uint8Array;
}

export interface FlorenceBrowserSelectedImage {
  readonly assetId: string;
  readonly signalId: string;
  readonly workId: string;
  readonly mimeType: "image/jpeg" | "image/png" | "image/webp";
  readonly filename: string;
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
  readonly selectedImage?: FlorenceBrowserSelectedImage;
}

export interface FlorenceBrowserRunInput {
  readonly householdId: string;
  readonly workId: string;
  readonly ownerAdultId: string;
  readonly callId: string;
  readonly attempt: number;
  readonly session: FlorenceBrowserSession | null;
  readonly operation: FlorenceBrowserOperation;
  readonly uploadFile?: {
    readonly filename: string;
    readonly bytes: Uint8Array;
  };
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

export interface KernelBrowserClientOptions {
  readonly apiKey: string;
  readonly projectId?: string;
  readonly client?: Kernel;
  readonly executable?: string;
  readonly commandRunner?: FlorenceBrowserCommandRunner;
  readonly now?: () => number;
  readonly commandTimeoutMs?: number;
  readonly openTimeoutMs?: number;
  readonly sessionTimeoutSeconds?: number;
  readonly maxWaitMs?: number;
  readonly maxSnapshotChars?: number;
  readonly maxScreenshotBytes?: number;
}

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

interface KernelSessionDetails {
  readonly session: FlorenceBrowserSession;
  readonly connectUrl: string;
  readonly liveViewUrl: string | null;
  readonly profileId: string;
  readonly profileName: string;
  readonly saveChanges: boolean;
}

const UNCERTAIN_RETRY_OPERATIONS = new Set<FlorenceBrowserOperation["kind"]>([
  "click",
  "upload",
  "press",
  "scroll",
  "back",
  "playwright",
  "computer",
]);

export class KernelBrowserClient implements FlorenceBrowserClient {
  readonly #kernel: Kernel;
  readonly #executable: string;
  readonly #commandRunner: FlorenceBrowserCommandRunner;
  readonly #now: () => number;
  readonly #commandTimeoutMs: number;
  readonly #openTimeoutMs: number;
  readonly #sessionTimeoutSeconds: number;
  readonly #maxWaitMs: number;
  readonly #maxSnapshotChars: number;
  readonly #maxScreenshotBytes: number;
  readonly #activeSessions = new Map<string, FlorenceBrowserSession>();
  readonly #profileWriters = new Map<string, string>();
  readonly #commandGenerations = new Map<string, number>();

  constructor(options: KernelBrowserClientOptions) {
    const apiKey = requireNonEmpty(options.apiKey, "Kernel API key", 10_000);
    const projectId = optionalNonEmpty(options.projectId, "Kernel project ID", 500);
    this.#kernel =
      options.client ??
      new Kernel({
        apiKey,
        ...(projectId ? { projectID: projectId } : {}),
        maxRetries: 0,
        timeout: DEFAULT_API_TIMEOUT_MS,
      });
    this.#executable = requireNonEmpty(
      options.executable ?? "agent-browser",
      "agent-browser executable",
      4_096,
    );
    this.#commandRunner = options.commandRunner ?? runBrowserCommand;
    this.#now = options.now ?? Date.now;
    this.#commandTimeoutMs = boundedInteger(
      options.commandTimeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS,
      1_000,
      120_000,
    );
    this.#openTimeoutMs = boundedInteger(options.openTimeoutMs ?? DEFAULT_OPEN_TIMEOUT_MS, 1_000, 120_000);
    this.#sessionTimeoutSeconds = boundedInteger(
      options.sessionTimeoutSeconds ?? DEFAULT_KERNEL_SESSION_TIMEOUT_SECONDS,
      60,
      DEFAULT_KERNEL_SESSION_TIMEOUT_SECONDS,
    );
    this.#maxWaitMs = boundedInteger(options.maxWaitMs ?? DEFAULT_MAX_WAIT_MS, 250, 30_000);
    this.#maxSnapshotChars = boundedInteger(
      options.maxSnapshotChars ?? DEFAULT_SNAPSHOT_CHARS,
      1_000,
      AGENT_BROWSER_MAX_OUTPUT_CHARS,
    );
    this.#maxScreenshotBytes = boundedInteger(
      options.maxScreenshotBytes ?? MAX_BROWSER_IMAGE_BYTES,
      16 * 1_024,
      MAX_BROWSER_IMAGE_BYTES,
    );
  }

  async run(input: FlorenceBrowserRunInput, signal?: AbortSignal): Promise<FlorenceBrowserRunResult> {
    const localSignal = signal ?? new AbortController().signal;
    throwIfAborted(localSignal);
    validateRunInput(input);

    let sessionDetails: KernelSessionDetails;
    try {
      sessionDetails = await this.#resolveSession(input, localSignal);
      if (input.operation.kind === "owner_handoff") {
        sessionDetails = await this.#writableHandoffSession(input, sessionDetails, localSignal);
      }
    } catch (error) {
      throw asKernelBrowserError(error, localSignal);
    }

    const session = sessionDetails.session;
    const createdThisRun = session.sessionId !== input.session?.sessionId;
    this.#rememberSession(sessionDetails);

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
          if (localSignal.aborted) throw asKernelBrowserError(error, localSignal);
          return {
            session,
            observation: unreadableUncertainObservation(input.operation.kind, error),
          };
        }
      }

      let screenshot: FlorenceBrowserScreenshot | undefined;
      let operationResult: string | undefined;
      switch (input.operation.kind) {
        case "snapshot":
        case "owner_handoff":
          break;
        case "screenshot":
          screenshot = await this.#captureScreenshot(sessionDetails, localSignal);
          break;
        case "capture":
          screenshot = await this.#captureUserVisibleImage(sessionDetails, input.operation, localSignal);
          break;
        case "playwright":
          actionMayHaveHappened = true;
          operationResult = await this.#executePlaywright(sessionDetails, input.operation, localSignal);
          break;
        case "computer":
          actionMayHaveHappened = true;
          await this.#executeComputer(sessionDetails, input.operation.actions, localSignal);
          if (input.operation.screenshot) {
            screenshot = await this.#captureScreenshot(sessionDetails, localSignal);
          }
          break;
        default:
          actionMayHaveHappened = UNCERTAIN_RETRY_OPERATIONS.has(input.operation.kind);
          await this.#performLegacyOperation(sessionDetails, input.operation, input.uploadFile, localSignal);
      }

      const observationKind: FlorenceBrowserObservationKind =
        input.operation.kind === "owner_handoff" ? "owner_handoff" : "page";
      const observation = await this.#observePage(
        sessionDetails,
        input.operation,
        localSignal,
        observationKind,
        input.operation.kind === "owner_handoff"
          ? "The owner can take over this same live browser session, then tell Florence to continue. Sign-in changes will be available to later work."
          : null,
        screenshot,
        operationResult,
      );
      return { session, observation };
    } catch (error) {
      const browserError = asKernelBrowserError(error, localSignal);
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
          if (localSignal.aborted) throw asKernelBrowserError(observationError, localSignal);
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
    try {
      await this.#kernel.browsers.deleteByID(session.sessionId, { signal: localSignal });
    } catch (error) {
      if (kernelErrorStatus(error) !== 404 && kernelErrorStatus(error) !== 410) {
        throw asKernelBrowserError(error, localSignal);
      }
    } finally {
      this.#forgetSession(session.sessionId);
    }
  }

  async closeAll(signal?: AbortSignal): Promise<void> {
    const sessions = [...this.#activeSessions.values()];
    const results = await Promise.allSettled(sessions.map((session) => this.close(session, signal)));
    const firstFailure = results.find((result) => result.status === "rejected");
    if (firstFailure?.status === "rejected") throw firstFailure.reason;
  }

  async #resolveSession(input: FlorenceBrowserRunInput, signal: AbortSignal): Promise<KernelSessionDetails> {
    if (!input.session) {
      if (input.operation.kind !== "navigate") {
        throw new FlorenceBrowserError("invalid_input", "Start this browser task by navigating to a page.");
      }
      return this.#createSession(input, false, input.operation.url, signal);
    }

    validateSession(input.session);
    if (sessionExpired(input.session, this.#now())) {
      if (input.operation.kind === "navigate") {
        this.#forgetSession(input.session.sessionId);
        return this.#createSession(input, false, input.operation.url, signal);
      }
      throw new FlorenceBrowserError(
        "session_expired",
        "That browser session expired. Navigate to the page again to continue.",
      );
    }

    try {
      const browser = await this.#kernel.browsers.retrieve(input.session.sessionId, {}, { signal });
      return this.#sessionDetails(browser, input.householdId);
    } catch (error) {
      if (kernelErrorStatus(error) === 404 || kernelErrorStatus(error) === 410) {
        if (input.operation.kind === "navigate") {
          this.#forgetSession(input.session.sessionId);
          return this.#createSession(input, false, input.operation.url, signal);
        }
        throw new FlorenceBrowserError(
          "session_expired",
          "That browser session is no longer running. Navigate to the page again to continue.",
          { cause: error },
        );
      }
      throw error;
    }
  }

  async #createSession(
    input: Pick<FlorenceBrowserRunInput, "householdId" | "ownerAdultId" | "workId">,
    saveChanges: boolean,
    startUrl: string,
    signal: AbortSignal,
  ): Promise<KernelSessionDetails> {
    const profileName = kernelProfileName(input.householdId);
    const activeWriter = this.#profileWriters.get(profileName);
    if (saveChanges && activeWriter) {
      throw new FlorenceBrowserError(
        "transient",
        "Another browser session is saving this household's sign-in state. Florence can continue after it finishes.",
        { retryable: true },
      );
    }
    const profile = await this.#ensureProfile(profileName, signal);
    if (saveChanges) {
      const providerWriter = await this.#findActiveProfileWriter(profile.id, signal);
      if (providerWriter) {
        throw new FlorenceBrowserError(
          "transient",
          "Another browser session is saving this household's sign-in state. Florence can continue after it finishes.",
          { retryable: true },
        );
      }
    }
    const browser = await this.#kernel.browsers.create(
      {
        profile: { id: profile.id, save_changes: saveChanges },
        start_url: validNavigationUrl(startUrl),
        stealth: true,
        timeout_seconds: this.#sessionTimeoutSeconds,
        tags: {
          florence_household: digest(input.householdId),
          florence_owner: digest(input.ownerAdultId),
          florence_work: digest(input.workId),
        },
      },
      { signal },
    );
    const details = this.#sessionDetails(browser, input.householdId);
    this.#commandGenerations.set(details.session.sessionId, 0);
    this.#rememberSession(details);
    return details;
  }

  async #ensureProfile(profileName: string, signal: AbortSignal): Promise<{ readonly id: string }> {
    try {
      return await this.#kernel.profiles.retrieve(profileName, { signal });
    } catch (error) {
      if (kernelErrorStatus(error) !== 404) throw error;
    }
    try {
      return await this.#kernel.profiles.create({ name: profileName }, { signal });
    } catch (error) {
      if (kernelErrorStatus(error) !== 409) throw error;
      return this.#kernel.profiles.retrieve(profileName, { signal });
    }
  }

  async #findActiveProfileWriter(profileId: string, signal: AbortSignal): Promise<boolean> {
    for await (const browser of this.#kernel.browsers.list(
      { query: profileId, status: "active" },
      { signal },
    )) {
      if (browser.profile?.id === profileId && browser.profile_save_changes) return true;
    }
    return false;
  }

  async #writableHandoffSession(
    input: FlorenceBrowserRunInput,
    current: KernelSessionDetails,
    signal: AbortSignal,
  ): Promise<KernelSessionDetails> {
    if (current.saveChanges) return current;
    const page = await this.#readPageMetadata(current, signal);
    await this.close(current.session, signal);
    return this.#createSession(input, true, page.url, signal);
  }

  #sessionDetails(
    browser: {
      readonly session_id: string;
      readonly cdp_ws_url: string;
      readonly browser_live_view_url?: string;
      readonly profile?: { readonly id: string; readonly name?: string | null };
      readonly profile_save_changes?: boolean;
      readonly timeout_seconds: number;
    },
    householdId: string,
  ): KernelSessionDetails {
    const profileName = kernelProfileName(householdId);
    if (!browser.profile?.id) {
      throw invalidProviderResponse("Kernel returned a browser without its persistent profile.");
    }
    if (browser.profile.name && browser.profile.name !== profileName) {
      throw invalidProviderResponse("Kernel returned a browser for a different household profile.");
    }
    const timeoutSeconds = boundedInteger(
      browser.timeout_seconds,
      60,
      DEFAULT_KERNEL_SESSION_TIMEOUT_SECONDS,
    );
    return {
      session: {
        sessionId: requireNonEmpty(browser.session_id, "Kernel browser session ID", 500),
        expiresAt: new Date(this.#now() + timeoutSeconds * 1_000).toISOString(),
      },
      connectUrl: validCdpUrl(browser.cdp_ws_url),
      liveViewUrl: browser.browser_live_view_url
        ? validHttpUrl(browser.browser_live_view_url, "Kernel returned an invalid live browser view.")
        : null,
      profileId: browser.profile.id,
      profileName,
      saveChanges: browser.profile_save_changes === true,
    };
  }

  async #executePlaywright(
    session: KernelSessionDetails,
    operation: Extract<FlorenceBrowserOperation, { readonly kind: "playwright" }>,
    signal: AbortSignal,
  ): Promise<string> {
    const code = boundedPlaywrightCode(operation.code);
    const timeoutSeconds = boundedInteger(
      operation.timeoutSeconds ?? DEFAULT_KERNEL_PLAYWRIGHT_TIMEOUT_SECONDS,
      1,
      MAX_KERNEL_PLAYWRIGHT_TIMEOUT_SECONDS,
    );
    const response = await this.#kernel.browsers.playwright.execute(
      session.session.sessionId,
      { code, timeout_sec: timeoutSeconds },
      { signal },
    );
    if (!response.success) {
      throw new FlorenceBrowserError(
        "transient",
        boundedString(
          response.error ?? response.stderr ?? "The browser program did not finish.",
          1_000,
          "The browser program did not finish.",
        ),
        { retryable: true },
      );
    }
    return boundedPlaywrightOutput(response.result, response.stdout);
  }

  async #executeComputer(
    session: KernelSessionDetails,
    actions: readonly FlorenceBrowserComputerAction[],
    signal: AbortSignal,
  ): Promise<void> {
    const batches = computerActionBatches(actions);
    for (const batch of batches) {
      throwIfAborted(signal);
      await this.#kernel.browsers.computer.batch(session.session.sessionId, { actions: batch }, { signal });
    }
  }

  async #performLegacyOperation(
    session: KernelSessionDetails,
    operation: Exclude<
      FlorenceBrowserOperation,
      | { readonly kind: "snapshot" | "screenshot" | "capture" | "owner_handoff" }
      | { readonly kind: "playwright" | "computer" }
    >,
    uploadFile: FlorenceBrowserRunInput["uploadFile"],
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
      case "upload":
        if (!uploadFile) {
          throw new FlorenceBrowserError(
            "invalid_input",
            "Choose a parent-provided file before uploading it to this page.",
          );
        }
        await this.#uploadFile(session, operation.ref, uploadFile, signal);
        return;
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

  async #uploadFile(
    session: KernelSessionDetails,
    ref: string,
    uploadFile: NonNullable<FlorenceBrowserRunInput["uploadFile"]>,
    signal: AbortSignal,
  ): Promise<void> {
    const filename = sanitizedUploadFilename(uploadFile.filename);
    const bytes = boundedUploadBytes(uploadFile.bytes);
    const directory = await mkdtemp(join(tmpdir(), "florence-browser-upload-"));
    const path = join(directory, filename);
    try {
      const handle = await open(path, "wx", 0o600);
      try {
        await handle.writeFile(bytes);
      } finally {
        await handle.close();
      }
      await this.#agentBrowser(session, "upload", [normalizeRef(ref), path], signal, this.#commandTimeoutMs);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }

  async #observePage(
    session: KernelSessionDetails,
    operation: FlorenceBrowserOperation,
    signal: AbortSignal,
    kind: FlorenceBrowserObservationKind,
    reason: string | null,
    screenshot?: FlorenceBrowserScreenshot,
    operationResult?: string,
  ): Promise<FlorenceBrowserObservation> {
    const snapshotArgs = operation.kind === "snapshot" && operation.compact === false ? [] : ["-c"];
    const snapshotEnvelope = await this.#agentBrowser(
      session,
      "snapshot",
      snapshotArgs,
      signal,
      this.#commandTimeoutMs,
    );
    const parsed = parseSnapshot(snapshotEnvelope.data, this.#maxSnapshotChars);
    const merged = operationResult
      ? truncateSnapshot(`${operationResult}\n\n${parsed.snapshot}`, this.#maxSnapshotChars)
      : { value: parsed.snapshot, truncated: parsed.truncated };
    const metadata = await this.#readPageMetadata(session, signal);
    return {
      kind,
      reason,
      url: metadata.url,
      title: metadata.title,
      snapshot: merged.value,
      refCount: parsed.refCount,
      truncated: merged.truncated,
      ...(kind === "owner_handoff" && session.liveViewUrl ? { liveViewUrl: session.liveViewUrl } : {}),
      ...(screenshot ? { screenshot } : {}),
    };
  }

  async #readPageMetadata(session: KernelSessionDetails, signal: AbortSignal): Promise<PageMetadata> {
    const urlResult = await this.#agentBrowser(session, "get", ["url"], signal, this.#commandTimeoutMs);
    const titleResult = await this.#agentBrowser(session, "get", ["title"], signal, this.#commandTimeoutMs);
    return {
      url: readBoundedRecordString(urlResult.data, "url", 4_096),
      title: readBoundedRecordString(titleResult.data, "title", 2_000),
    };
  }

  async #captureScreenshot(
    session: KernelSessionDetails,
    signal: AbortSignal,
    region?: { readonly x: number; readonly y: number; readonly width: number; readonly height: number },
  ): Promise<FlorenceBrowserScreenshot> {
    const response = await this.#kernel.browsers.computer.captureScreenshot(
      session.session.sessionId,
      region ? { region } : undefined,
      { signal },
    );
    const bytes = await readBoundedResponseBytes(response, this.#maxScreenshotBytes);
    if (
      bytes.byteLength < 8 ||
      bytes[0] !== 0x89 ||
      bytes[1] !== 0x50 ||
      bytes[2] !== 0x4e ||
      bytes[3] !== 0x47
    ) {
      throw invalidProviderResponse("Kernel returned an unreadable browser screenshot.");
    }
    return { mimeType: "image/png", bytes };
  }

  async #captureUserVisibleImage(
    session: KernelSessionDetails,
    operation: Extract<FlorenceBrowserOperation, { readonly kind: "capture" }>,
    signal: AbortSignal,
  ): Promise<FlorenceBrowserScreenshot> {
    switch (operation.source) {
      case "viewport":
        return this.#captureScreenshot(session, signal, operation.region);
      case "full_page":
        return this.#capturePlaywrightImage(session, { kind: "full_page" }, signal);
      case "element":
        return this.#capturePlaywrightImage(
          session,
          { kind: "element", selector: requireCaptureSelector(operation.selector) },
          signal,
        );
      case "image_resource": {
        const selector = requireCaptureSelector(operation.selector);
        try {
          return await this.#captureImageResource(session, selector, signal);
        } catch (error) {
          if (signal.aborted) throw error;
          return this.#capturePlaywrightImage(session, { kind: "element", selector }, signal);
        }
      }
    }
  }

  async #captureImageResource(
    session: KernelSessionDetails,
    selector: string,
    signal: AbortSignal,
  ): Promise<FlorenceBrowserScreenshot> {
    const result = await this.#kernel.browsers.playwright.execute(
      session.session.sessionId,
      {
        code: `
const image = page.locator(${JSON.stringify(selector)}).first();
await image.waitFor({ state: "visible" });
return await image.evaluate((element) => {
  if (!(element instanceof HTMLImageElement)) throw new Error("The selected element is not an image.");
  return { url: element.currentSrc || element.src };
});`,
        timeout_sec: DEFAULT_KERNEL_PLAYWRIGHT_TIMEOUT_SECONDS,
      },
      { signal },
    );
    if (!result.success || !isRecord(result.result) || typeof result.result.url !== "string") {
      throw invalidProviderResponse(result.error ?? "The selected image resource was unavailable.");
    }
    const url = new URL(result.result.url);
    if (url.protocol !== "https:" && url.protocol !== "http:") {
      throw invalidProviderResponse("The selected image resource was unavailable.");
    }
    const response = await this.#kernel.browsers.fetch(session.session.sessionId, url, {
      method: "GET",
      headers: { accept: "image/webp,image/png,image/jpeg,*/*;q=0.1" },
      timeout_ms: MAX_KERNEL_PLAYWRIGHT_TIMEOUT_SECONDS * 1_000,
      signal,
    });
    const bytes = await readBoundedResponseBytes(response, this.#maxScreenshotBytes);
    const mimeType = browserImageMimeType(bytes);
    if (!mimeType) throw invalidProviderResponse("The selected resource was not a supported image.");
    if (mimeType === "image/webp") {
      throw invalidProviderResponse("The selected resource needs a Messages-compatible rendering.");
    }
    return { mimeType, bytes };
  }

  async #capturePlaywrightImage(
    session: KernelSessionDetails,
    target: { readonly kind: "full_page" } | { readonly kind: "element"; readonly selector: string },
    signal: AbortSignal,
  ): Promise<FlorenceBrowserScreenshot> {
    const remotePath = `/tmp/florence-browser-image-${randomUUID()}.png`;
    try {
      const screenshotCode =
        target.kind === "full_page"
          ? `await page.screenshot({ animations: "disabled", caret: "hide", fullPage: true, path: ${JSON.stringify(remotePath)}, type: "png" });`
          : `
const target = page.locator(${JSON.stringify(target.selector)}).first();
await target.waitFor({ state: "visible" });
await target.screenshot({ animations: "disabled", caret: "hide", path: ${JSON.stringify(remotePath)}, type: "png" });`;
      const result = await this.#kernel.browsers.playwright.execute(
        session.session.sessionId,
        {
          code: `${screenshotCode}\nreturn true;`,
          timeout_sec: MAX_KERNEL_PLAYWRIGHT_TIMEOUT_SECONDS,
        },
        { signal },
      );
      if (!result.success) {
        throw invalidProviderResponse(result.error ?? "Kernel could not capture the selected browser image.");
      }
      const response = await this.#kernel.browsers.fs.readFile(
        session.session.sessionId,
        { path: remotePath },
        { signal },
      );
      const bytes = await readBoundedResponseBytes(response, this.#maxScreenshotBytes);
      if (browserImageMimeType(bytes) !== "image/png") {
        throw invalidProviderResponse("Kernel returned an unreadable selected browser image.");
      }
      return { mimeType: "image/png", bytes };
    } finally {
      await this.#kernel.browsers.fs
        .deleteFile(session.session.sessionId, { path: remotePath }, { signal })
        .catch(() => undefined);
    }
  }

  async #agentBrowser(
    session: KernelSessionDetails,
    command: string,
    commandArguments: readonly string[],
    signal: AbortSignal,
    timeoutMs: number,
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
        { retryable: true },
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
      // Deleting the Kernel browser below is authoritative; daemon cleanup is best effort.
    }
    if (closed) {
      await Promise.allSettled(
        ["pid", "stream", "engine", "version", "sock"].map((suffix) =>
          rm(join(socketDirectory, `${sessionName}.${suffix}`), { force: true }),
        ),
      );
    }
  }

  #commandSessionName(sessionId: string): string {
    const generation = this.#commandGenerations.get(sessionId) ?? 0;
    return `florence_${digest(sessionId)}_${generation}`;
  }

  async #recycleCommandGeneration(sessionId: string): Promise<void> {
    await this.#stopAgentBrowserDaemon(sessionId, new AbortController().signal);
    this.#commandGenerations.set(sessionId, (this.#commandGenerations.get(sessionId) ?? 0) + 1);
  }

  #rememberSession(details: KernelSessionDetails): void {
    this.#activeSessions.set(details.session.sessionId, details.session);
    if (details.saveChanges) this.#profileWriters.set(details.profileName, details.session.sessionId);
  }

  #forgetSession(sessionId: string): void {
    this.#activeSessions.delete(sessionId);
    this.#commandGenerations.delete(sessionId);
    for (const [profileName, writerSessionId] of this.#profileWriters) {
      if (writerSessionId === sessionId) this.#profileWriters.delete(profileName);
    }
  }

  async #bestEffortClose(session: FlorenceBrowserSession): Promise<void> {
    try {
      await this.close(session);
    } catch {
      // Kernel's provider timeout remains the final cleanup bound when deletion fails.
    }
  }
}

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

      if (input.operation.kind === "capture") {
        throw new FlorenceBrowserError(
          "unavailable",
          "Selected browser images require Florence's Kernel browser.",
          { retryable: false },
        );
      }

      if (input.operation.kind !== "snapshot" && input.operation.kind !== "screenshot") {
        if (input.operation.kind !== "owner_handoff") {
          if (input.operation.kind === "playwright" || input.operation.kind === "computer") {
            throw new FlorenceBrowserError(
              "unavailable",
              "This browser provider does not support the deeper browser operator. Configure Kernel to use it.",
              { retryable: false },
            );
          }
          actionMayHaveHappened = UNCERTAIN_RETRY_OPERATIONS.has(input.operation.kind);
          await this.#performOperation(sessionDetails, input.operation, input.uploadFile, localSignal);
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
        householdId: input.householdId,
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
      { readonly kind: "snapshot" | "screenshot" | "capture" | "owner_handoff" }
    >,
    uploadFile: FlorenceBrowserRunInput["uploadFile"],
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
      case "upload":
        if (!uploadFile) {
          throw new FlorenceBrowserError(
            "invalid_input",
            "Choose a parent-provided file before uploading it to this page.",
          );
        }
        await this.#uploadFile(session, operation.ref, uploadFile, signal);
        return;
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
      case "playwright":
      case "computer":
        throw new FlorenceBrowserError(
          "unavailable",
          "This browser provider does not support the deeper browser operator. Configure Kernel to use it.",
          { retryable: false },
        );
    }

    await this.#agentBrowser(session, command, args, signal, timeoutMs);
  }

  async #uploadFile(
    session: BrowserbaseSessionDetails,
    ref: string,
    uploadFile: NonNullable<FlorenceBrowserRunInput["uploadFile"]>,
    signal: AbortSignal,
  ): Promise<void> {
    const filename = sanitizedUploadFilename(uploadFile.filename);
    const bytes = boundedUploadBytes(uploadFile.bytes);
    const directory = await mkdtemp(join(tmpdir(), "florence-browser-upload-"));
    const path = join(directory, filename);
    try {
      const handle = await open(path, "wx", 0o600);
      try {
        await handle.writeFile(bytes);
      } finally {
        await handle.close();
      }
      await this.#agentBrowser(session, "upload", [normalizeRef(ref), path], signal, this.#commandTimeoutMs);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
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

export function kernelProfileName(householdId: string): string {
  validateNonEmptyInput(householdId, "Browser household ID", 500);
  return `florence-${createHash("sha256")
    .update(`florence-kernel-profile\0${householdId}`)
    .digest("hex")
    .slice(0, 40)}`;
}

function boundedPlaywrightCode(value: string): string {
  if (typeof value !== "string" || !value.trim() || value.length > MAX_PLAYWRIGHT_CODE_CHARS) {
    throw new FlorenceBrowserError(
      "invalid_input",
      `A browser program is required and must be at most ${MAX_PLAYWRIGHT_CODE_CHARS} characters.`,
    );
  }
  return value;
}

function boundedPlaywrightOutput(result: unknown, stdout: string | undefined): string {
  const parts: string[] = [];
  if (result !== undefined) {
    let serialized: string;
    try {
      serialized = typeof result === "string" ? result : JSON.stringify(result);
    } catch {
      serialized = "[The browser program returned a value that could not be serialized.]";
    }
    parts.push(`Playwright result:\n${serialized}`);
  }
  if (stdout?.trim()) parts.push(`Playwright log:\n${stdout.trim()}`);
  return truncateSnapshot(
    parts.join("\n\n") || "Playwright completed successfully.",
    AGENT_BROWSER_MAX_OUTPUT_CHARS,
  ).value;
}

function computerActionBatches(
  actions: readonly FlorenceBrowserComputerAction[],
): readonly ComputerBatchParams.Action[][] {
  if (!Array.isArray(actions) || actions.length < 1 || actions.length > MAX_COMPUTER_ACTIONS_PER_CALL) {
    throw new FlorenceBrowserError(
      "invalid_input",
      `A computer action call must contain between 1 and ${MAX_COMPUTER_ACTIONS_PER_CALL} actions. Florence can continue with another call afterward.`,
    );
  }
  return [actions.map(computerBatchAction)];
}

function computerBatchAction(action: FlorenceBrowserComputerAction): ComputerBatchParams.Action {
  switch (action.type) {
    case "click_mouse":
      return {
        type: action.type,
        click_mouse: {
          x: finiteCoordinate(action.x, "click x"),
          y: finiteCoordinate(action.y, "click y"),
          ...(action.button ? { button: action.button } : {}),
          ...(action.clickType ? { click_type: action.clickType } : {}),
          ...(action.numClicks === undefined ? {} : { num_clicks: boundedInteger(action.numClicks, 1, 10) }),
        },
      };
    case "move_mouse":
      return {
        type: action.type,
        move_mouse: {
          x: finiteCoordinate(action.x, "move x"),
          y: finiteCoordinate(action.y, "move y"),
        },
      };
    case "type_text":
      return {
        type: action.type,
        type_text: { text: boundedInput(action.text, "Computer text") },
      };
    case "press_key":
      if (action.keys.length < 1 || action.keys.length > 20) {
        throw new FlorenceBrowserError("invalid_input", "A computer key action needs one to twenty keys.");
      }
      return {
        type: action.type,
        press_key: {
          keys: action.keys.map((key) => requireNonEmpty(key, "Computer key", 100)),
        },
      };
    case "scroll":
      return {
        type: action.type,
        scroll: {
          x: finiteCoordinate(action.x, "scroll x"),
          y: finiteCoordinate(action.y, "scroll y"),
          ...(action.deltaX === undefined
            ? {}
            : { delta_x: finiteCoordinate(action.deltaX, "horizontal scroll") }),
          ...(action.deltaY === undefined
            ? {}
            : { delta_y: finiteCoordinate(action.deltaY, "vertical scroll") }),
        },
      };
    case "drag_mouse":
      if (action.path.length < 2 || action.path.length > 100) {
        throw new FlorenceBrowserError("invalid_input", "A computer drag needs between two and 100 points.");
      }
      return {
        type: action.type,
        drag_mouse: {
          path: action.path.map(({ x, y }) => [finiteCoordinate(x, "drag x"), finiteCoordinate(y, "drag y")]),
          ...(action.button ? { button: action.button } : {}),
        },
      };
    case "sleep":
      return {
        type: action.type,
        sleep: { duration_ms: boundedInteger(action.milliseconds, 0, DEFAULT_MAX_WAIT_MS) },
      };
  }
}

function finiteCoordinate(value: number, label: string): number {
  if (!Number.isFinite(value)) {
    throw new FlorenceBrowserError("invalid_input", `${label} must be a finite number.`);
  }
  return value;
}

function requireCaptureSelector(value: string | undefined): string {
  const selector = value?.trim();
  if (!selector || selector.length > 2_000) {
    throw new FlorenceBrowserError(
      "invalid_input",
      "Choose the exact page element Florence should show in the final result.",
    );
  }
  return selector;
}

function browserImageMimeType(bytes: Uint8Array): "image/jpeg" | "image/png" | "image/webp" | null {
  if (bytes.byteLength >= 3 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) {
    return "image/jpeg";
  }
  if (
    bytes.byteLength >= 8 &&
    bytes[0] === 0x89 &&
    bytes[1] === 0x50 &&
    bytes[2] === 0x4e &&
    bytes[3] === 0x47 &&
    bytes[4] === 0x0d &&
    bytes[5] === 0x0a &&
    bytes[6] === 0x1a &&
    bytes[7] === 0x0a
  ) {
    return "image/png";
  }
  if (
    bytes.byteLength >= 12 &&
    Buffer.from(bytes.subarray(0, 4)).toString("ascii") === "RIFF" &&
    Buffer.from(bytes.subarray(8, 12)).toString("ascii") === "WEBP"
  ) {
    return "image/webp";
  }
  return null;
}

async function readBoundedResponseBytes(
  response: Response,
  maximumBytes: number,
): Promise<Uint8Array<ArrayBuffer>> {
  if (!response.ok) {
    throw invalidProviderResponse("Kernel could not capture the browser screenshot.");
  }
  const contentLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(contentLength) && contentLength > maximumBytes) {
    throw invalidProviderResponse("The browser screenshot was too large to keep with this task.");
  }
  if (!response.body) {
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.byteLength < 1 || bytes.byteLength > maximumBytes) {
      throw invalidProviderResponse("The browser screenshot was too large to keep with this task.");
    }
    return bytes;
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const result = await reader.read();
      if (result.done) break;
      total += result.value.byteLength;
      if (total > maximumBytes) {
        throw invalidProviderResponse("The browser screenshot was too large to keep with this task.");
      }
      chunks.push(result.value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

function kernelErrorStatus(error: unknown): number | null {
  if (!isRecord(error)) return null;
  if (typeof error.status === "number") return error.status;
  if (typeof error.statusCode === "number") return error.statusCode;
  return null;
}

function asKernelBrowserError(error: unknown, signal: AbortSignal): FlorenceBrowserError {
  if (error instanceof FlorenceBrowserError) return error;
  if (signal.aborted) {
    return new FlorenceBrowserError("cancelled", "Browser work was cancelled.", { cause: error });
  }
  const status = kernelErrorStatus(error);
  const transient =
    status === 408 || status === 409 || status === 425 || status === 429 || (status ?? 0) >= 500;
  if (transient) {
    return new FlorenceBrowserError("transient", "The browser is temporarily unavailable.", {
      retryable: true,
      cause: error,
    });
  }
  if (status === 401 || status === 403) {
    return new FlorenceBrowserError("unavailable", "Kernel browser access is unavailable.", {
      retryable: false,
      cause: error,
    });
  }
  return new FlorenceBrowserError("invalid_response", "Kernel returned an unreadable browser result.", {
    retryable: true,
    cause: error,
  });
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
  validateNonEmptyInput(input.householdId, "Browser household ID", 500);
  validateNonEmptyInput(input.workId, "Browser work ID", 500);
  validateNonEmptyInput(input.ownerAdultId, "Browser owner adult ID", 500);
  validateNonEmptyInput(input.callId, "Browser call ID", 500);
  if (!Number.isSafeInteger(input.attempt) || input.attempt < 1) {
    throw new FlorenceBrowserError("invalid_input", "Browser attempt must be a positive integer.");
  }
  if (input.operation.kind === "capture") {
    validateNonEmptyInput(input.operation.label, "Browser image label", 200);
    if (input.operation.source === "element" || input.operation.source === "image_resource") {
      requireCaptureSelector(input.operation.selector);
    } else if (input.operation.selector !== undefined) {
      throw new FlorenceBrowserError(
        "invalid_input",
        "A browser image selector is only valid for an element or image resource.",
      );
    }
    if (input.operation.region) {
      if (input.operation.source !== "viewport") {
        throw new FlorenceBrowserError(
          "invalid_input",
          "A browser image region is only valid for a viewport capture.",
        );
      }
      const { x, y, width, height } = input.operation.region;
      if (![x, y, width, height].every(Number.isFinite) || x < 0 || y < 0 || width <= 0 || height <= 0) {
        throw new FlorenceBrowserError("invalid_input", "The selected browser image region is invalid.");
      }
    }
  }
  if (input.operation.kind === "upload") {
    normalizeRef(input.operation.ref);
    validateNonEmptyInput(input.operation.attachmentRef, "Browser attachment reference", 500);
    if (input.attempt === 1 && !input.uploadFile) {
      throw new FlorenceBrowserError(
        "invalid_input",
        "Choose a parent-provided file before uploading it to this page.",
      );
    }
    if (input.uploadFile) {
      sanitizedUploadFilename(input.uploadFile.filename);
      boundedUploadBytes(input.uploadFile.bytes);
    }
  } else if (input.uploadFile !== undefined) {
    throw new FlorenceBrowserError(
      "invalid_input",
      "Browser file bytes may only be supplied for an upload step.",
    );
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

function sanitizedUploadFilename(value: string): string {
  if (typeof value !== "string") {
    throw new FlorenceBrowserError("invalid_input", "The upload filename is invalid.");
  }
  const normalized = value.normalize("NFKC").trim();
  if (!normalized || Buffer.byteLength(normalized, "utf8") > MAX_UPLOAD_FILENAME_BYTES) {
    throw new FlorenceBrowserError(
      "invalid_input",
      `The upload filename must be at most ${MAX_UPLOAD_FILENAME_BYTES} bytes.`,
    );
  }
  const sanitized = [...normalized]
    .map((character) => {
      const codePoint = character.codePointAt(0) ?? 0;
      return codePoint < 32 || codePoint === 127 || '<>:"/\\|?*'.includes(character) ? "_" : character;
    })
    .join("")
    .replace(/^\.+/, "")
    .trim();
  if (!sanitized) return "attachment";
  return sanitized;
}

function boundedUploadBytes(value: Uint8Array): Uint8Array<ArrayBuffer> {
  if (!(value instanceof Uint8Array) || value.byteLength < 1 || value.byteLength > MAX_UPLOAD_BYTES) {
    throw new FlorenceBrowserError(
      "invalid_input",
      `Browser uploads must be between 1 byte and ${MAX_UPLOAD_BYTES} bytes.`,
    );
  }
  return new Uint8Array(value);
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
