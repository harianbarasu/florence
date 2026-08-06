const REQUEST_TIMEOUT_MS = 10_000;

try {
  const baseUrl = productionBaseUrl(process.argv[2]);
  const completed: string[] = [];

  const health = await getJson(baseUrl, "/healthz");
  assert(health.ok === true && health.service === "florence-web", "liveness payload is invalid");
  completed.push("liveness");

  const readiness = await getJson(baseUrl, "/readyz");
  assert(readiness.ok === true && readiness.database === true, "readiness payload is invalid");
  completed.push("readiness");

  const landing = await get(baseUrl, "/");
  const landingHtml = await landing.text();
  assert(landingHtml.includes("<title>Florence</title>"), "landing page is not the Florence application");
  assert(landing.headers.has("content-security-policy"), "landing page is missing Content-Security-Policy");
  assert(landing.headers.get("referrer-policy") === "no-referrer", "landing page referrer policy is unsafe");
  if (baseUrl.protocol === "https:") {
    assert(landing.headers.has("strict-transport-security"), "HTTPS response is missing HSTS");
  }

  const bundlePath = landingHtml.match(/<script[^>]+src="(\/assets\/[^"]+\.js)"/u)?.[1];
  assert(bundlePath, "landing page does not reference a built application bundle");
  const bundle = await get(baseUrl, bundlePath);
  assert(
    bundle.headers.get("content-type")?.includes("javascript") === true,
    "application bundle has the wrong content type",
  );
  completed.push("web_bundle");

  for (const path of ["/privacy", "/terms"] as const) {
    const response = await get(baseUrl, path);
    assert(response.headers.get("content-type")?.includes("text/html") === true, `${path} is not HTML`);
    completed.push(path.slice(1));
  }

  process.stdout.write(`${JSON.stringify({ ok: true, origin: baseUrl.origin, checks: completed })}\n`);
} catch (error) {
  const message = error instanceof Error ? error.message : "unknown smoke-check failure";
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
    url.search ||
    url.hash ||
    url.pathname !== "/"
  ) {
    throw new Error("the smoke-check target must be a clean HTTP(S) origin");
  }
  if (url.protocol !== "https:" && !["localhost", "127.0.0.1", "::1"].includes(url.hostname)) {
    throw new Error("the smoke-check target must use HTTPS unless it is local");
  }
  return url;
}

async function getJson(baseUrl: URL, path: string): Promise<Record<string, unknown>> {
  const response = await get(baseUrl, path);
  assert(response.headers.get("content-type")?.includes("application/json") === true, `${path} is not JSON`);
  const body: unknown = await response.json();
  assert(body !== null && typeof body === "object" && !Array.isArray(body), `${path} JSON is invalid`);
  return body as Record<string, unknown>;
}

async function get(baseUrl: URL, path: string): Promise<Response> {
  const response = await fetch(new URL(path, baseUrl), {
    method: "GET",
    redirect: "manual",
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    headers: { accept: "application/json, text/html;q=0.9, */*;q=0.1" },
  });
  if (response.status !== 200) throw new Error(`${path} returned HTTP ${response.status}`);
  return response;
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
