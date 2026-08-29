import { createHash } from "node:crypto";
import { promises as dns } from "node:dns";
import * as http from "node:http";
import * as https from "node:https";
import { BlockList, isIP } from "node:net";
import { basename } from "node:path";
import { PDFParse } from "pdf-parse";
import { z } from "zod";

/**
 * Concrete public-page reader adapted from Hermes Agent's URL normalization,
 * SSRF-safe connect, redirect revalidation, base64-image cleanup, contiguous
 * continuation, and successful-result cache at commit
 * 6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882 (`tools/url_safety.py`,
 * `tools/web_tools.py`, and `tools/web_result_cache.py`). Florence performs
 * the fetch and PDF extraction locally and intentionally omits Hermes's
 * provider/plugin/config, disk-cache, and secret-query policy machinery.
 */

const DEFAULT_CHAR_LIMIT = 15_000;
const DEFAULT_TIMEOUT_MS = 20_000;
const DEFAULT_MAX_REDIRECTS = 5;
const DEFAULT_MAX_HTML_BYTES = 4 * 1_024 * 1_024;
const DEFAULT_MAX_PDF_BYTES = 16 * 1_024 * 1_024;
const DEFAULT_CACHE_TTL_MS = 20 * 60_000;
const DEFAULT_MAX_CACHE_ENTRIES = 64;
const DEFAULT_MAX_CACHE_BYTES = 32 * 1_024 * 1_024;
const USER_AGENT = "FlorenceFamilyAssistant/0.1 (+https://github.com/harianbarasu/florence)";

const HTML_CONTENT_TYPES = new Set([
  "application/xhtml+xml",
  "text/html",
  "text/markdown",
  "text/plain",
  "text/x-markdown",
]);
const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);
const PRIVATE_HOST_SUFFIXES = [".internal", ".lan", ".local", ".localhost", ".home"];
const BLOCKED_IPS = buildBlockedIpList();

export const publicPageRequestSchema = z
  .object({
    url: z.string().trim().min(1).max(8_192),
    offset: z.number().int().nonnegative().default(0),
    contentFingerprint: z
      .string()
      .regex(/^[a-f0-9]{64}$/)
      .nullable()
      .default(null),
    charLimit: z.number().int().min(1_000).max(500_000).default(DEFAULT_CHAR_LIMIT),
  })
  .strict()
  .superRefine((request, context) => {
    if (request.offset > 0 && request.contentFingerprint === null) {
      context.addIssue({
        code: "custom",
        path: ["contentFingerprint"],
        message: "Continuation requires the exact content fingerprint returned by the preceding chunk.",
      });
    }
  });

export const publicPageResultSchema = z
  .object({
    requestedUrl: z.string().url(),
    finalUrl: z.string().url(),
    kind: z.enum(["html", "pdf"]),
    title: z.string().min(1).max(1_000).nullable(),
    filename: z.string().min(1).max(500).nullable(),
    text: z.string(),
    offset: z.number().int().nonnegative().default(0),
    nextOffset: z.number().int().nonnegative().nullable().default(null),
    contentFingerprint: z.string().regex(/^[a-f0-9]{64}$/),
    truncated: z.boolean(),
    totalCleanCharacters: z.number().int().nonnegative(),
    totalCleanBytes: z.number().int().nonnegative(),
    responseBytes: z.number().int().nonnegative(),
    fetchedAt: z.string().datetime({ offset: true }),
  })
  .strict();

export type FlorencePublicPageRequest = z.input<typeof publicPageRequestSchema>;
export type FlorencePublicPageResult = z.infer<typeof publicPageResultSchema>;

export type PublicPageErrorCode =
  | "invalid_input"
  | "blocked_url"
  | "dns_failure"
  | "timeout"
  | "cancelled"
  | "redirect_limit"
  | "http_error"
  | "unsupported_content_type"
  | "response_too_large"
  | "network_error"
  | "extraction_failed"
  | "invalid_response";

export class PublicPageError extends Error {
  readonly code: PublicPageErrorCode;
  readonly safeMessage: string;
  readonly retryable: boolean;
  readonly status: number | undefined;
  readonly url: string | undefined;

  constructor(
    code: PublicPageErrorCode,
    safeMessage: string,
    options: {
      readonly retryable?: boolean;
      readonly status?: number;
      readonly url?: string;
      readonly cause?: unknown;
    } = {},
  ) {
    super(safeMessage, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "PublicPageError";
    this.code = code;
    this.safeMessage = safeMessage.slice(0, 300);
    this.retryable = options.retryable ?? false;
    this.status = options.status;
    this.url = options.url;
  }
}

export interface FlorencePublicPageClient {
  run(request: FlorencePublicPageRequest, signal?: AbortSignal): Promise<FlorencePublicPageResult>;
}

export type PublicPageResolvedAddress = {
  readonly address: string;
  readonly family: 4 | 6;
};

export type PublicPageNetworkResponse = {
  readonly status: number;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: Uint8Array;
};

export interface PublicPageNetwork {
  request(input: {
    readonly url: URL;
    readonly addresses: readonly PublicPageResolvedAddress[];
    readonly signal: AbortSignal;
    readonly maxHtmlBytes: number;
    readonly maxPdfBytes: number;
  }): Promise<PublicPageNetworkResponse>;
}

export type PublicPageReaderOptions = {
  readonly network?: PublicPageNetwork;
  readonly resolveHost?: (
    hostname: string,
    signal: AbortSignal,
  ) => Promise<readonly PublicPageResolvedAddress[]>;
  readonly now?: () => number;
  readonly timeoutMs?: number;
  readonly maxRedirects?: number;
  readonly maxHtmlBytes?: number;
  readonly maxPdfBytes?: number;
  readonly cacheTtlMs?: number;
  readonly maxCacheEntries?: number;
  readonly maxCacheBytes?: number;
};

type CacheEntry = {
  readonly expiresAt: number;
  readonly document: ExtractedPublicDocument;
};

type PageKind = "html" | "pdf";

type ExtractedPublicDocument = {
  readonly requestedUrl: string;
  readonly finalUrl: string;
  readonly kind: PageKind;
  readonly title: string | null;
  readonly filename: string | null;
  readonly cleanText: string;
  readonly contentFingerprint: string;
  readonly totalCleanBytes: number;
  readonly responseBytes: number;
  readonly fetchedAt: string;
};

export class PublicPageReader implements FlorencePublicPageClient {
  readonly #network: PublicPageNetwork;
  readonly #resolveHost: NonNullable<PublicPageReaderOptions["resolveHost"]>;
  readonly #now: () => number;
  readonly #timeoutMs: number;
  readonly #maxRedirects: number;
  readonly #maxHtmlBytes: number;
  readonly #maxPdfBytes: number;
  readonly #cacheTtlMs: number;
  readonly #maxCacheEntries: number;
  readonly #maxCacheBytes: number;
  #cachedBytes = 0;
  readonly #cache = new Map<string, CacheEntry>();

  constructor(options: PublicPageReaderOptions = {}) {
    this.#network = options.network ?? new NativePublicPageNetwork();
    this.#resolveHost = options.resolveHost ?? resolvePublicAddresses;
    this.#now = options.now ?? Date.now;
    this.#timeoutMs = clampInteger(options.timeoutMs ?? DEFAULT_TIMEOUT_MS, 1_000, 120_000);
    this.#maxRedirects = clampInteger(options.maxRedirects ?? DEFAULT_MAX_REDIRECTS, 0, 10);
    this.#maxHtmlBytes = clampInteger(
      options.maxHtmlBytes ?? DEFAULT_MAX_HTML_BYTES,
      1_024,
      32 * 1_024 * 1_024,
    );
    this.#maxPdfBytes = clampInteger(options.maxPdfBytes ?? DEFAULT_MAX_PDF_BYTES, 1_024, 64 * 1_024 * 1_024);
    this.#cacheTtlMs = clampInteger(options.cacheTtlMs ?? DEFAULT_CACHE_TTL_MS, 1_000, 24 * 60 * 60_000);
    this.#maxCacheEntries = clampInteger(options.maxCacheEntries ?? DEFAULT_MAX_CACHE_ENTRIES, 1, 500);
    this.#maxCacheBytes = clampInteger(
      options.maxCacheBytes ?? DEFAULT_MAX_CACHE_BYTES,
      1,
      512 * 1_024 * 1_024,
    );
  }

  async run(request: FlorencePublicPageRequest, signal?: AbortSignal): Promise<FlorencePublicPageResult> {
    const parsed = publicPageRequestSchema.safeParse(request);
    if (!parsed.success) {
      throw new PublicPageError("invalid_input", "The public page request was invalid.", {
        cause: parsed.error,
      });
    }

    const requested = normalizePublicUrl(parsed.data.url);
    const cacheKey = requested.toString();
    const cached = this.#cacheGet(cacheKey);
    if (cached) {
      return windowPublicDocument(
        cached,
        parsed.data.offset,
        parsed.data.charLimit,
        parsed.data.contentFingerprint,
      );
    }

    const timeoutController = new AbortController();
    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      timeoutController.abort();
    }, this.#timeoutMs);
    timeout.unref?.();
    const combinedSignal = combineSignals(signal, timeoutController.signal);

    try {
      const fetched = await this.#fetchWithRedirects(requested, combinedSignal);
      const kind = detectPageKind(fetched.response, fetched.url);
      const limit = kind === "pdf" ? this.#maxPdfBytes : this.#maxHtmlBytes;
      if (fetched.response.body.byteLength > limit) {
        throw new PublicPageError(
          "response_too_large",
          `The public ${kind === "pdf" ? "PDF" : "page"} was too large to read.`,
          { url: fetched.url.toString() },
        );
      }

      const extracted =
        kind === "pdf"
          ? await extractPdf(fetched.response.body, fetched.response.headers, fetched.url)
          : extractHtmlOrText(fetched.response.body, fetched.response.headers, fetched.url);
      const cleanText = removeBase64Images(cleanTextSpacing(extracted.text));
      if (!cleanText) {
        throw new PublicPageError(
          "extraction_failed",
          `The public ${kind === "pdf" ? "PDF" : "page"} did not contain readable text.`,
          { url: fetched.url.toString() },
        );
      }
      const document: ExtractedPublicDocument = {
        requestedUrl: requested.toString(),
        finalUrl: fetched.url.toString(),
        kind,
        title: extracted.title,
        filename: extracted.filename,
        cleanText,
        contentFingerprint: createHash("sha256").update(cleanText, "utf8").digest("hex"),
        totalCleanBytes: Buffer.byteLength(cleanText, "utf8"),
        responseBytes: fetched.response.body.byteLength,
        fetchedAt: new Date(this.#now()).toISOString(),
      };
      const result = windowPublicDocument(
        document,
        parsed.data.offset,
        parsed.data.charLimit,
        parsed.data.contentFingerprint,
      );
      this.#cacheSet(cacheKey, document);
      return result;
    } catch (error) {
      if (error instanceof PublicPageError) throw error;
      if (combinedSignal.aborted) {
        throw new PublicPageError(
          timedOut ? "timeout" : "cancelled",
          timedOut ? "The public page took too long to load." : "The public page read was cancelled.",
          { retryable: timedOut, cause: error, url: requested.toString() },
        );
      }
      throw new PublicPageError("network_error", "The public page could not be loaded.", {
        retryable: true,
        cause: error,
        url: requested.toString(),
      });
    } finally {
      clearTimeout(timeout);
    }
  }

  async #fetchWithRedirects(
    requested: URL,
    signal: AbortSignal,
  ): Promise<{ readonly url: URL; readonly response: PublicPageNetworkResponse }> {
    let current = requested;
    const visited = new Set<string>();
    for (let redirects = 0; ; redirects += 1) {
      throwIfAborted(signal);
      if (visited.has(current.toString())) {
        throw new PublicPageError("redirect_limit", "The public page redirected in a loop.", {
          url: current.toString(),
        });
      }
      visited.add(current.toString());
      const addresses = await validatePublicTarget(current, this.#resolveHost, signal);
      let response: PublicPageNetworkResponse;
      try {
        response = await this.#network.request({
          url: current,
          addresses,
          signal,
          maxHtmlBytes: this.#maxHtmlBytes,
          maxPdfBytes: this.#maxPdfBytes,
        });
      } catch (error) {
        if (error instanceof PublicPageError) throw error;
        if (signal.aborted) throw error;
        throw new PublicPageError("network_error", "The public page could not be loaded.", {
          retryable: true,
          cause: error,
          url: current.toString(),
        });
      }

      if (REDIRECT_STATUSES.has(response.status)) {
        if (redirects >= this.#maxRedirects) {
          throw new PublicPageError("redirect_limit", "The public page redirected too many times.", {
            status: response.status,
            url: current.toString(),
          });
        }
        const location = response.headers.location?.trim();
        if (!location) {
          throw new PublicPageError("invalid_response", "The public page returned an invalid redirect.", {
            status: response.status,
            url: current.toString(),
          });
        }
        try {
          current = normalizePublicUrl(new URL(location, current).toString());
        } catch (error) {
          if (error instanceof PublicPageError) throw error;
          throw new PublicPageError("invalid_response", "The public page returned an invalid redirect.", {
            status: response.status,
            cause: error,
            url: current.toString(),
          });
        }
        continue;
      }

      if (response.status < 200 || response.status >= 300) {
        throw new PublicPageError("http_error", `The public page returned HTTP ${response.status}.`, {
          retryable: response.status === 408 || response.status === 429 || response.status >= 500,
          status: response.status,
          url: current.toString(),
        });
      }
      return { url: current, response };
    }
  }

  #cacheGet(key: string): ExtractedPublicDocument | undefined {
    const entry = this.#cache.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt <= this.#now()) {
      this.#cacheDelete(key);
      return undefined;
    }
    this.#cache.delete(key);
    this.#cache.set(key, entry);
    return entry.document;
  }

  #cacheSet(key: string, document: ExtractedPublicDocument): void {
    const now = this.#now();
    for (const [cacheKey, entry] of this.#cache) {
      if (entry.expiresAt <= now) this.#cacheDelete(cacheKey);
    }
    this.#cacheDelete(key);
    if (document.totalCleanBytes > this.#maxCacheBytes) return;
    this.#cache.set(key, { expiresAt: now + this.#cacheTtlMs, document });
    this.#cachedBytes += document.totalCleanBytes;
    while (this.#cache.size > this.#maxCacheEntries || this.#cachedBytes > this.#maxCacheBytes) {
      const oldest = this.#cache.keys().next().value;
      if (oldest === undefined) break;
      this.#cacheDelete(oldest);
    }
  }

  #cacheDelete(key: string): void {
    const entry = this.#cache.get(key);
    if (!entry) return;
    this.#cache.delete(key);
    this.#cachedBytes -= entry.document.totalCleanBytes;
  }
}

class NativePublicPageNetwork implements PublicPageNetwork {
  async request(input: {
    readonly url: URL;
    readonly addresses: readonly PublicPageResolvedAddress[];
    readonly signal: AbortSignal;
    readonly maxHtmlBytes: number;
    readonly maxPdfBytes: number;
  }): Promise<PublicPageNetworkResponse> {
    let lastError: unknown;
    for (const address of input.addresses) {
      try {
        return await requestPinnedAddress(input, address);
      } catch (error) {
        if (error instanceof PublicPageError || input.signal.aborted) throw error;
        lastError = error;
      }
    }
    throw new PublicPageError("network_error", "The public page could not be loaded.", {
      retryable: true,
      cause: lastError,
      url: input.url.toString(),
    });
  }
}

function requestPinnedAddress(
  input: {
    readonly url: URL;
    readonly signal: AbortSignal;
    readonly maxHtmlBytes: number;
    readonly maxPdfBytes: number;
  },
  address: PublicPageResolvedAddress,
): Promise<PublicPageNetworkResponse> {
  return new Promise((resolve, reject) => {
    const secure = input.url.protocol === "https:";
    const client = secure ? https : http;
    const port = input.url.port ? Number.parseInt(input.url.port, 10) : secure ? 443 : 80;
    const hostHeader = input.url.port ? `${input.url.hostname}:${input.url.port}` : input.url.hostname;
    const request = client.request(
      {
        protocol: input.url.protocol,
        hostname: address.address,
        family: address.family,
        port,
        method: "GET",
        path: `${input.url.pathname}${input.url.search}`,
        headers: {
          accept: "text/html,application/xhtml+xml,application/pdf,text/plain,text/markdown;q=0.9,*/*;q=0.1",
          "accept-encoding": "identity",
          "user-agent": USER_AGENT,
          host: hostHeader,
        },
        servername: secure ? stripIpv6Brackets(input.url.hostname) : undefined,
        signal: input.signal,
      },
      (response) => {
        const headers = flattenHeaders(response.headers);
        const contentEncoding = headers["content-encoding"]?.trim().toLowerCase();
        if (contentEncoding && contentEncoding !== "identity") {
          response.resume();
          reject(
            new PublicPageError(
              "invalid_response",
              "The public page returned an unsupported content encoding.",
              { status: response.statusCode ?? 0, url: input.url.toString() },
            ),
          );
          return;
        }

        const maxBytes = contentTypeIsPdf(headers["content-type"]) ? input.maxPdfBytes : input.maxHtmlBytes;
        const declaredLength = parseContentLength(headers["content-length"]);
        if (declaredLength !== null && declaredLength > maxBytes) {
          response.destroy();
          reject(
            new PublicPageError("response_too_large", "The public page was too large to read.", {
              status: response.statusCode ?? 0,
              url: input.url.toString(),
            }),
          );
          return;
        }

        const chunks: Buffer[] = [];
        let received = 0;
        response.on("data", (chunk: Buffer | string) => {
          const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
          received += buffer.byteLength;
          if (received > maxBytes) {
            response.destroy();
            reject(
              new PublicPageError("response_too_large", "The public page was too large to read.", {
                status: response.statusCode ?? 0,
                url: input.url.toString(),
              }),
            );
            return;
          }
          chunks.push(buffer);
        });
        response.on("end", () => {
          resolve({
            status: response.statusCode ?? 0,
            headers,
            body: Buffer.concat(chunks, received),
          });
        });
        response.on("error", reject);
      },
    );
    request.on("error", reject);
    request.end();
  });
}

async function resolvePublicAddresses(
  hostname: string,
  signal: AbortSignal,
): Promise<readonly PublicPageResolvedAddress[]> {
  throwIfAborted(signal);
  try {
    const records = await dns.lookup(hostname, { all: true, verbatim: true });
    throwIfAborted(signal);
    return records.flatMap((record): PublicPageResolvedAddress[] =>
      record.family === 4 || record.family === 6 ? [{ address: record.address, family: record.family }] : [],
    );
  } catch (error) {
    if (signal.aborted) throw error;
    throw new PublicPageError("dns_failure", "The public page hostname could not be resolved.", {
      retryable: true,
      cause: error,
    });
  }
}

async function validatePublicTarget(
  url: URL,
  resolver: NonNullable<PublicPageReaderOptions["resolveHost"]>,
  signal: AbortSignal,
): Promise<readonly PublicPageResolvedAddress[]> {
  const hostname = url.hostname.toLowerCase().replace(/\.$/, "");
  if (isBlockedHostname(hostname)) {
    throw new PublicPageError("blocked_url", "Florence can only read public internet pages.", {
      url: url.toString(),
    });
  }

  const literalFamily = isIP(stripIpv6Brackets(hostname));
  const addresses = literalFamily
    ? [{ address: stripIpv6Brackets(hostname), family: literalFamily as 4 | 6 }]
    : await resolver(hostname, signal);
  if (addresses.length === 0) {
    throw new PublicPageError("dns_failure", "The public page hostname could not be resolved.", {
      retryable: true,
      url: url.toString(),
    });
  }

  const unique = new Map<string, PublicPageResolvedAddress>();
  for (const address of addresses) {
    if (isIP(address.address) !== address.family || isBlockedIp(address.address)) {
      throw new PublicPageError("blocked_url", "Florence can only read public internet pages.", {
        url: url.toString(),
      });
    }
    unique.set(`${address.family}:${address.address}`, address);
  }
  return [...unique.values()].slice(0, 8);
}

function normalizePublicUrl(input: string): URL {
  const repaired = input.trim().replace(/^([A-Za-z][A-Za-z0-9+.-]*:\/\/)\s+/, "$1");
  let parsed: URL;
  try {
    parsed = new URL(repaired);
  } catch (error) {
    throw new PublicPageError("invalid_input", "Use a complete public http or https URL.", {
      cause: error,
    });
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new PublicPageError("invalid_input", "Use a public http or https URL.");
  }
  if (!parsed.hostname || parsed.username || parsed.password) {
    throw new PublicPageError("invalid_input", "Use a public URL without embedded credentials.");
  }
  parsed.hash = "";
  return parsed;
}

function detectPageKind(response: PublicPageNetworkResponse, url: URL): PageKind {
  const contentType = normalizeContentType(response.headers["content-type"]);
  if (contentType === "application/pdf" || startsWithPdfMagic(response.body)) return "pdf";
  if (contentType && !HTML_CONTENT_TYPES.has(contentType)) {
    throw new PublicPageError(
      "unsupported_content_type",
      `Florence cannot read public pages with content type ${contentType}.`,
      { status: response.status, url: url.toString() },
    );
  }
  return "html";
}

function extractHtmlOrText(
  body: Uint8Array,
  headers: Readonly<Record<string, string>>,
  url: URL,
): { readonly text: string; readonly title: string | null; readonly filename: string | null } {
  const contentType = normalizeContentType(headers["content-type"]);
  const source = decodeBody(body, headers["content-type"]);
  const filename = filenameFromHeadersOrUrl(headers, url);
  if (contentType === "text/plain" || contentType === "text/markdown" || contentType === "text/x-markdown") {
    return { text: source, title: null, filename };
  }

  const titleMatch = source.match(/<title\b[^>]*>([\s\S]*?)<\/title\s*>/i);
  const title = titleMatch?.[1]
    ? boundedNullableText(decodeHtmlEntities(stripTags(titleMatch[1])), 1_000)
    : null;
  let clean = source
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<(script|style|noscript|template|svg|canvas|head)\b[^>]*>[\s\S]*?<\/\1\s*>/gi, " ")
    .replace(/<img\b[^>]*>/gi, (tag) => {
      const alt = tag.match(/\balt\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/i);
      const label = decodeHtmlEntities(alt?.[1] ?? alt?.[2] ?? alt?.[3] ?? "").trim();
      return label ? `\n[IMAGE: ${label}]\n` : "\n[IMAGE]\n";
    })
    .replace(/<(br|hr)\b[^>]*\/?>/gi, "\n")
    .replace(
      /<\/?(address|article|aside|blockquote|dd|div|dl|dt|fieldset|figcaption|figure|footer|form|h[1-6]|header|li|main|nav|ol|p|pre|section|table|tbody|td|tfoot|th|thead|tr|ul)\b[^>]*>/gi,
      "\n",
    )
    .replace(/<[^>]+>/g, " ");
  clean = decodeHtmlEntities(clean);
  return { text: clean, title, filename };
}

async function extractPdf(
  body: Uint8Array,
  headers: Readonly<Record<string, string>>,
  url: URL,
): Promise<{ readonly text: string; readonly title: string | null; readonly filename: string | null }> {
  const parser = new PDFParse({ data: Uint8Array.from(body), stopAtErrors: false });
  try {
    const textResult = await parser.getText();
    let title: string | null = null;
    try {
      const infoResult = await parser.getInfo();
      const candidate =
        typeof infoResult.info?.Title === "string"
          ? infoResult.info.Title
          : typeof infoResult.info?.title === "string"
            ? infoResult.info.title
            : null;
      title = boundedNullableText(candidate, 1_000);
    } catch {
      // Metadata is optional; text extraction is the useful result.
    }
    return {
      text: textResult.text,
      title,
      filename: filenameFromHeadersOrUrl(headers, url),
    };
  } catch (error) {
    throw new PublicPageError("extraction_failed", "Florence could not read text from this PDF.", {
      cause: error,
      url: url.toString(),
    });
  } finally {
    await parser.destroy();
  }
}

/**
 * Adapted port of Hermes Agent's full-text continuation at pinned commit
 * 6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882 (`tools/web_tools.py:630-660,
 * 693-796`). Hermes stores omitted page text for later `read_file` calls;
 * Florence intentionally has no arbitrary-filesystem tool, so the existing
 * public-page call exposes the same continuation as exact character windows.
 */
function windowPublicDocument(
  document: ExtractedPublicDocument,
  offset: number,
  charLimit: number,
  expectedFingerprint: string | null,
): FlorencePublicPageResult {
  if (offset > 0 && expectedFingerprint !== document.contentFingerprint) {
    throw new PublicPageError(
      "invalid_input",
      "This public page changed since the previous chunk. Restart reading it at offset 0.",
      { url: document.finalUrl },
    );
  }
  if (offset > document.cleanText.length) {
    throw new PublicPageError("invalid_input", "The public page offset was past the end of the text.", {
      url: document.finalUrl,
    });
  }
  const endOffset = Math.min(offset + charLimit, document.cleanText.length);
  const nextOffset = endOffset < document.cleanText.length ? endOffset : null;
  const result = publicPageResultSchema.safeParse({
    requestedUrl: document.requestedUrl,
    finalUrl: document.finalUrl,
    kind: document.kind,
    title: document.title,
    filename: document.filename,
    text: document.cleanText.slice(offset, endOffset),
    offset,
    nextOffset,
    contentFingerprint: document.contentFingerprint,
    truncated: offset > 0 || nextOffset !== null,
    totalCleanCharacters: document.cleanText.length,
    totalCleanBytes: document.totalCleanBytes,
    responseBytes: document.responseBytes,
    fetchedAt: document.fetchedAt,
  });
  if (!result.success) {
    throw new PublicPageError(
      "invalid_response",
      "The public page produced a result Florence could not use.",
      { cause: result.error, url: document.finalUrl },
    );
  }
  return result.data;
}

function removeBase64Images(text: string): string {
  return text
    .replace(/!\[([^\]]*)\]\(\s*data:image\/[^;\s]+;base64,[A-Za-z0-9+/=\s]+\)/gi, (_match, alt: string) =>
      alt.trim() ? `[IMAGE: ${alt.trim()}]` : "[IMAGE]",
    )
    .replace(/\(\s*data:image\/[^;\s]+;base64,[A-Za-z0-9+/=\s]+\)/gi, "[IMAGE]")
    .replace(/data:image\/[^;\s]+;base64,[A-Za-z0-9+/=]+/gi, "[IMAGE]");
}

function cleanTextSpacing(text: string): string {
  return text
    .replace(/\r\n?/g, "\n")
    .replace(/[\t\f\v\u00a0 ]+/g, " ")
    .split("\n")
    .map((line) => line.trim())
    .join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function stripTags(text: string): string {
  return text.replace(/<[^>]+>/g, " ");
}

function decodeHtmlEntities(text: string): string {
  const named: Readonly<Record<string, string>> = {
    amp: "&",
    apos: "'",
    gt: ">",
    hellip: "…",
    laquo: "«",
    ldquo: "“",
    lsquo: "‘",
    lt: "<",
    mdash: "—",
    middot: "·",
    nbsp: " ",
    ndash: "–",
    quot: '"',
    raquo: "»",
    rdquo: "”",
    rsquo: "’",
  };
  return text.replace(/&(#(?:x[0-9a-f]+|\d+)|[a-z][a-z0-9]+);/gi, (entity, key: string) => {
    if (key.startsWith("#")) {
      const hex = key[1]?.toLowerCase() === "x";
      const value = Number.parseInt(key.slice(hex ? 2 : 1), hex ? 16 : 10);
      if (Number.isInteger(value) && value > 0 && value <= 0x10ffff) {
        try {
          return String.fromCodePoint(value);
        } catch {
          return "�";
        }
      }
      return "�";
    }
    return named[key.toLowerCase()] ?? entity;
  });
}

function decodeBody(body: Uint8Array, rawContentType: string | undefined): string {
  const charset = rawContentType?.match(/charset\s*=\s*["']?([^;"'\s]+)/i)?.[1]?.toLowerCase();
  const encoding = charset === "iso-8859-1" || charset === "latin1" ? "windows-1252" : "utf-8";
  return new TextDecoder(encoding).decode(body);
}

function filenameFromHeadersOrUrl(headers: Readonly<Record<string, string>>, url: URL): string | null {
  const disposition = headers["content-disposition"];
  if (disposition) {
    const encoded = disposition.match(/filename\*\s*=\s*UTF-8''([^;]+)/i)?.[1];
    if (encoded) {
      try {
        const decoded = safeFilename(decodeURIComponent(encoded.trim().replace(/^"|"$/g, "")));
        if (decoded) return decoded;
      } catch {
        // Fall through to the ordinary filename or URL path.
      }
    }
    const plain = disposition.match(/filename\s*=\s*(?:"([^"]+)"|([^;\s]+))/i);
    const filename = safeFilename(plain?.[1] ?? plain?.[2] ?? "");
    if (filename) return filename;
  }
  try {
    return safeFilename(decodeURIComponent(basename(url.pathname)));
  } catch {
    return safeFilename(basename(url.pathname));
  }
}

function safeFilename(value: string): string | null {
  const clean = basename(value.replace(/\\/g, "/"))
    .split("")
    .filter((character) => {
      const code = character.charCodeAt(0);
      return code > 31 && code !== 127;
    })
    .join("")
    .trim()
    .slice(0, 500);
  return clean || null;
}

function boundedNullableText(value: string | null | undefined, max: number): string | null {
  if (!value) return null;
  const clean = cleanTextSpacing(value).slice(0, max);
  return clean || null;
}

function normalizeContentType(value: string | undefined): string | null {
  const normalized = value?.split(";", 1)[0]?.trim().toLowerCase();
  return normalized || null;
}

function contentTypeIsPdf(value: string | undefined): boolean {
  return normalizeContentType(value) === "application/pdf";
}

function startsWithPdfMagic(body: Uint8Array): boolean {
  return body.byteLength >= 5 && new TextDecoder("ascii").decode(body.slice(0, 5)) === "%PDF-";
}

function flattenHeaders(headers: http.IncomingHttpHeaders): Readonly<Record<string, string>> {
  const flattened: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (Array.isArray(value)) flattened[key.toLowerCase()] = value.join(", ");
    else if (value !== undefined) flattened[key.toLowerCase()] = String(value);
  }
  return flattened;
}

function parseContentLength(value: string | undefined): number | null {
  if (!value || !/^\d+$/.test(value.trim())) return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function isBlockedHostname(hostname: string): boolean {
  const bare = stripIpv6Brackets(hostname);
  if (!bare || bare === "localhost") return true;
  return PRIVATE_HOST_SUFFIXES.some((suffix) => bare.endsWith(suffix));
}

function stripIpv6Brackets(hostname: string): string {
  return hostname.startsWith("[") && hostname.endsWith("]") ? hostname.slice(1, -1) : hostname;
}

function isBlockedIp(address: string): boolean {
  const family = isIP(address);
  if (family === 4) return BLOCKED_IPS.check(address, "ipv4");
  if (family === 6) return BLOCKED_IPS.check(address, "ipv6");
  return true;
}

function buildBlockedIpList(): BlockList {
  const list = new BlockList();
  const ipv4Subnets = [
    ["0.0.0.0", 8],
    ["10.0.0.0", 8],
    ["100.64.0.0", 10],
    ["127.0.0.0", 8],
    ["169.254.0.0", 16],
    ["172.16.0.0", 12],
    ["192.0.0.0", 24],
    ["192.0.2.0", 24],
    ["192.88.99.0", 24],
    ["192.168.0.0", 16],
    ["198.18.0.0", 15],
    ["198.51.100.0", 24],
    ["203.0.113.0", 24],
    ["224.0.0.0", 4],
    ["240.0.0.0", 4],
  ] as const;
  for (const [network, prefix] of ipv4Subnets) {
    list.addSubnet(network, prefix, "ipv4");
    list.addSubnet(`::ffff:${network}`, 96 + prefix, "ipv6");
  }
  const ipv6Subnets = [
    ["::", 96],
    ["::1", 128],
    ["64:ff9b:1::", 48],
    ["100::", 64],
    ["2001:2::", 48],
    ["2001:10::", 28],
    ["2001:20::", 28],
    ["2001:db8::", 32],
    ["3fff::", 20],
    ["5f00::", 16],
    ["fc00::", 7],
    ["fe80::", 10],
    ["fec0::", 10],
    ["ff00::", 8],
  ] as const;
  for (const [network, prefix] of ipv6Subnets) list.addSubnet(network, prefix, "ipv6");
  return list;
}

function combineSignals(first: AbortSignal | undefined, second: AbortSignal): AbortSignal {
  return first ? AbortSignal.any([first, second]) : second;
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) throw signal.reason ?? new DOMException("Aborted", "AbortError");
}

function clampInteger(value: number, minimum: number, maximum: number): number {
  return Math.min(maximum, Math.max(minimum, Math.floor(value)));
}
