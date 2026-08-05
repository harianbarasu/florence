import process from "node:process";

const positionalArguments = process.argv.slice(2).filter((argument) => argument !== "--");
const rawBaseUrl = positionalArguments[0] ?? process.env.FLORENCE_SMOKE_BASE_URL;
if (!rawBaseUrl) {
  process.stderr.write("Usage: pnpm smoke:production -- https://your-florence-host\n");
  process.exitCode = 2;
} else {
  await run(new URL(rawBaseUrl));
}

async function run(baseUrl) {
  const checks = [
    { path: "/healthz", contentType: "application/json", body: { status: "ok" } },
    { path: "/readyz", contentType: "application/json", body: { status: "ready" } },
    { path: "/", contentType: "text/html", includes: "family Chief of Staff" },
    { path: "/privacy", contentType: "text/html", includes: "Personal Gmail and calendar" },
    { path: "/terms", contentType: "text/html", includes: "External communication" },
  ];

  for (const check of checks) {
    const url = new URL(check.path, baseUrl);
    const response = await fetchWithTimeout(url);
    if (!response.ok) fail(`${check.path} returned ${response.status}`);
    const contentType = response.headers.get("content-type") ?? "";
    if (!contentType.toLowerCase().includes(check.contentType)) {
      fail(`${check.path} returned an unexpected content type`);
    }
    if (response.headers.get("cache-control") !== "no-store") {
      fail(`${check.path} is missing cache-control: no-store`);
    }
    if (check.body) {
      const body = await response.json();
      if (JSON.stringify(body) !== JSON.stringify(check.body)) {
        fail(`${check.path} returned an unexpected body`);
      }
    } else {
      const body = await response.text();
      if (!body.includes(check.includes)) fail(`${check.path} returned unexpected content`);
    }
    process.stdout.write(`ok ${check.path}\n`);
  }

  const missingRoute = await fetchWithTimeout(new URL("/__florence_smoke_missing__", baseUrl));
  if (missingRoute.status !== 404) fail("unknown routes must return 404");
  if (missingRoute.headers.get("cache-control") !== "no-store") {
    fail("unknown routes must not be cached");
  }
  process.stdout.write("ok 404 policy\n");

  const operatorToken = process.env.FLORENCE_ADMIN_API_KEY;
  if (operatorToken) {
    const response = await fetchWithTimeout(new URL("/operator/status", baseUrl), {
      headers: { authorization: `Bearer ${operatorToken}` },
    });
    if (!response.ok) fail(`/operator/status returned ${response.status}`);
    const body = await response.json();
    if (body.status !== "ok" && body.status !== "degraded") {
      fail("/operator/status returned an unexpected body");
    }
    process.stdout.write("ok /operator/status\n");
  }
}

async function fetchWithTimeout(url, init = {}) {
  try {
    return await fetch(url, {
      ...init,
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
    });
  } catch {
    fail(`${url.pathname} could not be reached`);
  }
}

function fail(message) {
  process.stderr.write(`smoke failed: ${message}\n`);
  process.exitCode = 1;
  throw new Error("production smoke failed");
}
