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
</head>
<body>
<main>
<h1>${content.heading}</h1>
<p>${content.body}</p>
</main>
</body>
</html>`);
}
