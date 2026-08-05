import type { FastifyReply } from "fastify";

export type HandoffPage =
  | "home"
  | "privacy"
  | "terms"
  | "oauth_connected"
  | "oauth_declined"
  | "oauth_expired"
  | "oauth_invalid"
  | "oauth_unavailable";

const pages: Readonly<Record<HandoffPage, { title: string; heading: string; body: string }>> = {
  home: {
    title: "Florence",
    heading: "Florence",
    body: "A private family Chief of Staff that works through your existing conversation.",
  },
  privacy: {
    title: "Florence privacy",
    heading: "Privacy",
    body: "Personal Gmail and calendar information stays personal unless its owner explicitly approves sharing. You can disconnect integrations, export your data, or request deletion from your private Florence conversation.",
  },
  terms: {
    title: "Florence terms",
    heading: "Terms",
    body: "Florence helps organize household work. External communication and account-changing actions require app-owned approval. Keep control requests in your verified private Florence conversation.",
  },
  oauth_connected: {
    title: "Google account connected",
    heading: "Connection complete",
    body: "Your Google account is connected. You can close this window and return to Florence.",
  },
  oauth_declined: {
    title: "Google connection cancelled",
    heading: "Nothing was connected",
    body: "Google access was not granted. Close this window and return to Florence whenever you want to try again.",
  },
  oauth_expired: {
    title: "Connection link expired",
    heading: "This link has expired",
    body: "Close this window and ask Florence for a fresh private connection link.",
  },
  oauth_invalid: {
    title: "Invalid connection request",
    heading: "This request is not valid",
    body: "Nothing was connected. Close this window and request a fresh private link from Florence.",
  },
  oauth_unavailable: {
    title: "Connection temporarily unavailable",
    heading: "Please try again later",
    body: "Nothing was connected. Close this window and return to Florence to try again.",
  },
};

export function sendHandoffPage(reply: FastifyReply, page: HandoffPage, statusCode = 200): FastifyReply {
  const content = pages[page];
  reply
    .code(statusCode)
    .header("content-type", "text/html; charset=utf-8")
    .header("cache-control", "no-store")
    .header("pragma", "no-cache");
  return reply.send(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${content.title}</title>
${pageStyles()}
</head>
<body>
<main>
<div class="mark" aria-hidden="true">F</div>
<p class="eyebrow">Florence · Family Chief of Staff</p>
<h1>${content.heading}</h1>
<p>${content.body}</p>
<p class="foot">Private by default. Shared only with your direction.</p>
</main>
</body>
</html>`);
}

/** A confirmation hop prevents iMessage link previews from consuming a single-use export. */
export function sendCustomerExportHandoff(reply: FastifyReply, token: string): FastifyReply {
  const downloadPath = `/control/export/${encodeURIComponent(token)}/download`;
  reply
    .code(200)
    .header("content-type", "text/html; charset=utf-8")
    .header("cache-control", "no-store")
    .header("pragma", "no-cache");
  return reply.send(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Download your Florence data</title>
${pageStyles()}
</head>
<body>
<main>
<div class="mark" aria-hidden="true">F</div>
<p class="eyebrow">Florence · Private data control</p>
<h1>Your private Florence export</h1>
<p>This link works once. Download it only on a device you trust.</p>
<p><a class="button" href="${downloadPath}" rel="noreferrer">Download my data</a></p>
<p class="foot">Florence never asks for this link in a group conversation.</p>
</main>
</body>
</html>`);
}

function pageStyles(): string {
  return `<style>
:root { color-scheme: light; font-family: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f4f1eb; color: #1d2a26; }
* { box-sizing: border-box; }
body { min-height: 100vh; margin: 0; display: grid; place-items: center; padding: 28px; background: radial-gradient(circle at top, #fffaf0 0, #f4f1eb 58%, #e7ece7 100%); }
main { width: min(100%, 620px); padding: clamp(30px, 7vw, 64px); border: 1px solid rgba(29, 42, 38, .12); border-radius: 28px; background: rgba(255, 255, 255, .84); box-shadow: 0 24px 70px rgba(29, 42, 38, .10); backdrop-filter: blur(16px); }
.mark { width: 44px; height: 44px; display: grid; place-items: center; border-radius: 14px; background: #254f42; color: #fffaf0; font-family: Georgia, serif; font-size: 25px; }
.eyebrow { margin: 24px 0 10px; color: #527066; font-size: 12px; font-weight: 700; letter-spacing: .1em; text-transform: uppercase; }
h1 { margin: 0 0 18px; font-family: Georgia, "Times New Roman", serif; font-size: clamp(36px, 8vw, 60px); font-weight: 500; line-height: 1.02; letter-spacing: -.035em; }
p { margin: 0; font-size: 18px; line-height: 1.62; }
.foot { margin-top: 30px; padding-top: 22px; border-top: 1px solid rgba(29, 42, 38, .12); color: #60716b; font-size: 14px; }
.button { display: inline-flex; margin-top: 24px; padding: 13px 18px; border-radius: 999px; background: #254f42; color: white; font-size: 16px; font-weight: 700; text-decoration: none; }
.button:hover { background: #193a30; }
.button:focus-visible { outline: 3px solid #b57a35; outline-offset: 3px; }
@media (prefers-reduced-transparency: reduce) { main { background: white; backdrop-filter: none; } }
</style>`;
}
