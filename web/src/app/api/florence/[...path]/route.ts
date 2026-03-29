import { auth } from "@/auth";

const ALLOWED_PATHS = new Set([
  "session",
  "setup",
  "setup/profile",
  "google/start",
  "google/connections",
  "google/add-account",
  "google/disconnect",
  "settings",
]);

function florenceApiBaseUrl() {
  const baseUrl = (process.env.FLORENCE_API_BASE_URL || "").trim().replace(/\/$/, "");
  if (!baseUrl) {
    throw new Error("missing_florence_api_base_url");
  }
  return baseUrl;
}

async function forwardRequest(
  request: Request,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const { path } = await params;
  const joinedPath = path.join("/");
  if (!ALLOWED_PATHS.has(joinedPath)) {
    return Response.json({ ok: false, error: "unknown_florence_api_path" }, { status: 404 });
  }

  const session = await auth();
  const incomingUrl = new URL(request.url);
  const upstream = new URL(`${florenceApiBaseUrl()}/v1/web/${joinedPath}`);
  incomingUrl.searchParams.forEach((value, key) => {
    upstream.searchParams.append(key, value);
  });

  const headers = new Headers();
  const contentType = request.headers.get("content-type");
  if (contentType) {
    headers.set("content-type", contentType);
  }
  if (session?.user?.email) {
    headers.set("x-florence-auth-email", session.user.email);
  }

  const body = request.method === "GET" ? undefined : await request.text();
  let upstreamResponse: Response;
  try {
    upstreamResponse = await fetch(upstream, {
      method: request.method,
      headers,
      body,
      cache: "no-store",
    });
  } catch (error) {
    return Response.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "florence_proxy_failed",
      },
      { status: 502 },
    );
  }

  const responseBody = await upstreamResponse.text();
  return new Response(responseBody, {
    status: upstreamResponse.status,
    headers: {
      "content-type": upstreamResponse.headers.get("content-type") || "application/json; charset=utf-8",
    },
  });
}

export async function GET(request: Request, context: { params: Promise<{ path: string[] }> }) {
  return forwardRequest(request, context);
}

export async function POST(request: Request, context: { params: Promise<{ path: string[] }> }) {
  return forwardRequest(request, context);
}
