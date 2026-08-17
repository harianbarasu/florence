const requestTimeoutMs = 10_000;

try {
  const baseUrl = productionBaseUrl(commandArgument(process.argv.slice(2)));
  const completed: string[] = [];

  const health = await getJson(baseUrl, "/api/health");
  assert(health.status === "ok" && health.service === "florence-api", "health payload is invalid");
  completed.push("health");

  const landing = await get(baseUrl, "/");
  const landingHtml = await landing.text();
  assert(landingHtml.includes("<title>Florence</title>"), "landing page is not Florence");
  assert(landing.headers.has("content-security-policy"), "landing page is missing CSP");
  assert(landing.headers.get("referrer-policy") === "no-referrer", "unsafe referrer policy");
  if (baseUrl.protocol === "https:") {
    assert(landing.headers.has("strict-transport-security"), "HTTPS response is missing HSTS");
  }
  const bundlePath = landingHtml.match(/<script[^>]+src="(\/assets\/[^"]+\.js)"/u)?.[1];
  assert(bundlePath, "landing page does not reference a built application bundle");
  const bundle = await get(baseUrl, bundlePath);
  assert(bundle.headers.get("content-type")?.includes("javascript"), "bundle has the wrong type");
  completed.push("web_bundle");

  process.stdout.write(`${JSON.stringify({ ok: true, origin: baseUrl.origin, checks: completed })}\n`);
} catch (error) {
  const message = error instanceof Error ? error.message : "unknown smoke failure";
  process.stderr.write(`Florence production smoke check failed: ${message}\n`);
  process.exitCode = 1;
}

function productionBaseUrl(argument: string | undefined): URL {
  if (!argument) throw new Error("usage: pnpm smoke:production -- https://florence.example.com");
  const url = new URL(argument);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  ) {
    throw new Error("smoke target must be a clean HTTP(S) origin");
  }
  if (url.protocol !== "https:" && !["localhost", "127.0.0.1", "::1"].includes(url.hostname)) {
    throw new Error("non-local smoke targets must use HTTPS");
  }
  return url;
}

function commandArgument(arguments_: string[]): string | undefined {
  const normalized = arguments_[0] === "--" ? arguments_.slice(1) : arguments_;
  return normalized.length === 1 ? normalized[0] : undefined;
}

async function getJson(baseUrl: URL, pathname: string): Promise<Record<string, unknown>> {
  const response = await get(baseUrl, pathname);
  assert(response.headers.get("content-type")?.includes("application/json"), `${pathname} is not JSON`);
  const value: unknown = await response.json();
  assert(value !== null && typeof value === "object" && !Array.isArray(value), `${pathname} JSON is invalid`);
  return value as Record<string, unknown>;
}

async function get(baseUrl: URL, pathname: string): Promise<Response> {
  const response = await fetch(new URL(pathname, baseUrl), {
    headers: { accept: "application/json, text/html;q=0.9, */*;q=0.1" },
    redirect: "manual",
    signal: AbortSignal.timeout(requestTimeoutMs),
  });
  if (response.status !== 200) throw new Error(`${pathname} returned HTTP ${response.status}`);
  return response;
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
