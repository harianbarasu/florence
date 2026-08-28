import { describe, expect, it } from "vitest";
import {
  type PublicPageError,
  type PublicPageNetwork,
  type PublicPageNetworkResponse,
  PublicPageReader,
  type PublicPageResolvedAddress,
} from "./public-page.js";

const PUBLIC_ADDRESS: PublicPageResolvedAddress = { address: "93.184.216.34", family: 4 };
const resolvePublicHost = async (): Promise<readonly PublicPageResolvedAddress[]> => [PUBLIC_ADDRESS];

describe("PublicPageReader", () => {
  it("extracts deterministic HTML text, removes base64 images, truncates head/tail, and caches success", async () => {
    const middle = `MIDDLE-SHOULD-BE-OMITTED ${"m".repeat(1_000)}`;
    const html = `<!doctype html>
      <html><head><title>  Family &amp; school updates </title>
      <script>SECRET SCRIPT</script><style>.secret { color: red }</style></head>
      <body><main><h1>START OF FAMILY PAGE</h1>
      <p>${"Important beginning. ".repeat(45)}</p>
      <p>${middle}</p>
      <img alt="permission slip" src="data:image/png;base64,${"A".repeat(2_000)}">
      <p>${"Useful ending. ".repeat(45)} END OF FAMILY PAGE</p></main></body></html>`;
    const network = new FakeNetwork(() => htmlResponse(html));
    const reader = new PublicPageReader({
      network,
      resolveHost: resolvePublicHost,
      now: () => Date.parse("2026-08-28T12:00:00Z"),
    });

    const request = { url: " https:// public.test/family ", charLimit: 1_000 } as const;
    const first = await reader.run(request);
    const second = await reader.run(request);

    expect(first).toEqual(second);
    expect(network.urls).toEqual(["https://public.test/family"]);
    expect(first).toMatchObject({
      requestedUrl: "https://public.test/family",
      finalUrl: "https://public.test/family",
      kind: "html",
      title: "Family & school updates",
      filename: "family",
      truncated: true,
      fetchedAt: "2026-08-28T12:00:00.000Z",
    });
    expect(first.text).toContain("START OF FAMILY PAGE");
    expect(first.text).toContain("END OF FAMILY PAGE");
    expect(first.text).toContain("[TRUNCATED]");
    expect(first.text).not.toContain("MIDDLE-SHOULD-BE-OMITTED");
    expect(first.text).not.toContain("data:image");
    expect(first.text).not.toContain("SECRET SCRIPT");
    expect(first.totalCleanCharacters).toBeGreaterThan(1_000);
    expect(first.totalCleanBytes).toBeGreaterThanOrEqual(first.totalCleanCharacters);
    expect(first.responseBytes).toBe(Buffer.byteLength(html));
  });

  it("extracts a fetched PDF locally and returns its filename without base64", async () => {
    const pdf = makePdf("Family docket PDF: pickup is at 2:45 PM.");
    const network = new FakeNetwork(() => ({
      status: 200,
      headers: {
        "content-type": "application/pdf",
        "content-disposition": `attachment; filename="school-docket.pdf"`,
      },
      body: pdf,
    }));
    const reader = new PublicPageReader({ network, resolveHost: resolvePublicHost });

    const result = await reader.run({ url: "https://public.test/files/download" });

    expect(result.kind).toBe("pdf");
    expect(result.filename).toBe("school-docket.pdf");
    expect(result.text).toContain("Family docket PDF: pickup is at 2:45 PM.");
    expect(result.text).not.toContain("base64");
    expect(result.responseBytes).toBe(pdf.byteLength);
    expect(result.truncated).toBe(false);
  });

  it("follows and revalidates a public relative redirect but blocks a mapped-private redirect", async () => {
    const publicNetwork = new FakeNetwork((url) =>
      url.pathname === "/start"
        ? { status: 302, headers: { location: "/final" }, body: new Uint8Array() }
        : htmlResponse("<html><body><p>Final public page</p></body></html>"),
    );
    const publicReader = new PublicPageReader({
      network: publicNetwork,
      resolveHost: resolvePublicHost,
    });

    const result = await publicReader.run({ url: "https://public.test/start" });

    expect(result.finalUrl).toBe("https://public.test/final");
    expect(result.text).toBe("Final public page");
    expect(publicNetwork.urls).toEqual(["https://public.test/start", "https://public.test/final"]);

    const privateNetwork = new FakeNetwork(() => ({
      status: 302,
      headers: { location: "http://[::ffff:127.0.0.1]/admin" },
      body: new Uint8Array(),
    }));
    const privateReader = new PublicPageReader({
      network: privateNetwork,
      resolveHost: resolvePublicHost,
    });

    await expect(privateReader.run({ url: "https://public.test/start" })).rejects.toMatchObject({
      code: "blocked_url",
    });
    expect(privateNetwork.urls).toEqual(["https://public.test/start"]);
  });

  it("rejects oversized and unsupported responses with stable errors", async () => {
    const oversizedReader = new PublicPageReader({
      network: new FakeNetwork(() => ({
        status: 200,
        headers: { "content-type": "text/plain" },
        body: Buffer.alloc(1_025, "a"),
      })),
      resolveHost: resolvePublicHost,
      maxHtmlBytes: 1_024,
    });
    await expect(oversizedReader.run({ url: "https://public.test/huge" })).rejects.toMatchObject({
      code: "response_too_large",
    });

    const unsupportedReader = new PublicPageReader({
      network: new FakeNetwork(() => ({
        status: 200,
        headers: { "content-type": "image/png" },
        body: Buffer.from("not a page"),
      })),
      resolveHost: resolvePublicHost,
    });
    await expect(unsupportedReader.run({ url: "https://public.test/image" })).rejects.toEqual(
      expect.objectContaining<Partial<PublicPageError>>({
        code: "unsupported_content_type",
        status: 200,
      }),
    );
  });
});

class FakeNetwork implements PublicPageNetwork {
  readonly urls: string[] = [];
  readonly #response: (url: URL) => PublicPageNetworkResponse;

  constructor(response: (url: URL) => PublicPageNetworkResponse) {
    this.#response = response;
  }

  async request(input: {
    readonly url: URL;
    readonly addresses: readonly PublicPageResolvedAddress[];
    readonly signal: AbortSignal;
    readonly maxHtmlBytes: number;
    readonly maxPdfBytes: number;
  }): Promise<PublicPageNetworkResponse> {
    expect(input.addresses).toEqual([PUBLIC_ADDRESS]);
    expect(input.signal.aborted).toBe(false);
    this.urls.push(input.url.toString());
    return this.#response(input.url);
  }
}

function htmlResponse(html: string): PublicPageNetworkResponse {
  return {
    status: 200,
    headers: { "content-type": "text/html; charset=utf-8" },
    body: Buffer.from(html),
  };
}

function makePdf(text: string): Uint8Array {
  const escaped = text.replace(/([\\()])/g, "\\$1");
  const stream = `BT\n/F1 14 Tf\n72 720 Td\n(${escaped}) Tj\nET\n`;
  const objects = [
    "<< /Type /Catalog /Pages 2 0 R >>",
    "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
    "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
    `<< /Length ${Buffer.byteLength(stream)} >>\nstream\n${stream}endstream`,
    "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
  ];
  let source = "%PDF-1.4\n";
  const offsets = [0];
  for (const [index, object] of objects.entries()) {
    offsets.push(Buffer.byteLength(source));
    source += `${index + 1} 0 obj\n${object}\nendobj\n`;
  }
  const xrefOffset = Buffer.byteLength(source);
  source += `xref\n0 ${objects.length + 1}\n`;
  source += "0000000000 65535 f \n";
  for (const offset of offsets.slice(1)) {
    source += `${offset.toString().padStart(10, "0")} 00000 n \n`;
  }
  source += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xrefOffset}\n%%EOF\n`;
  return Buffer.from(source, "binary");
}
