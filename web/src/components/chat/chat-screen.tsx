"use client";

import Link from "next/link";
import { ArrowLeft, Loader2, RefreshCw, Send, Sparkles } from "lucide-react";
import { FormEvent, KeyboardEvent, useEffect, useMemo, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { getWebChat, sendWebChatMessage, FlorenceApiError } from "@/lib/florence-api";
import type { FlorenceWebChatMessage, FlorenceWebChatResponse } from "@/lib/types";

function formatTime(timestamp: number) {
  if (!timestamp) {
    return "";
  }
  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(timestamp * 1000));
}

function normalizeError(error: unknown) {
  if (error instanceof FlorenceApiError) {
    if (error.message === "web_chat_disabled") {
      return "Web chat is disabled for this deployment.";
    }
    if (error.message === "web_chat_auth_required") {
      return "Sign in with Google to use web chat.";
    }
    if (error.message === "unknown_web_google_identity") {
      return "This Google account is not connected to a Florence household yet.";
    }
    if (
      error.message === "web_chat_proxy_secret_unconfigured" ||
      error.message === "missing_web_chat_proxy_secret"
    ) {
      return "Web chat is missing its server configuration.";
    }
    return error.message;
  }
  return error instanceof Error ? error.message : "chat_request_failed";
}

function ChatBubble({ message }: { message: FlorenceWebChatMessage }) {
  const isUser = message.role === "user";
  return (
    <div className={`flex w-full ${isUser ? "justify-end" : "justify-start"}`}>
      <div
        className={[
          "max-w-[88%] rounded-2xl px-4 py-3 text-sm leading-6 shadow-sm sm:max-w-[74%]",
          isUser
            ? "bg-primary text-primary-foreground"
            : "border border-border bg-card text-card-foreground",
        ].join(" ")}
      >
        <div className="whitespace-pre-wrap break-words">{message.body}</div>
        <div className={`mt-2 text-[0.7rem] ${isUser ? "text-primary-foreground/70" : "text-muted-foreground"}`}>
          {formatTime(message.createdAt)}
        </div>
      </div>
    </div>
  );
}

export function ChatScreen() {
  const [chat, setChat] = useState<FlorenceWebChatResponse | null>(null);
  const [draft, setDraft] = useState("");
  const [loading, setLoading] = useState(true);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const transcriptRef = useRef<HTMLDivElement | null>(null);

  const messages = useMemo(() => chat?.messages ?? [], [chat]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    getWebChat()
      .then((response) => {
        if (!cancelled) {
          setChat(response);
          setError(null);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(normalizeError(err));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const transcript = transcriptRef.current;
    if (transcript) {
      transcript.scrollTop = transcript.scrollHeight;
    }
  }, [messages.length, sending]);

  async function refresh() {
    setLoading(true);
    try {
      const response = await getWebChat();
      setChat(response);
      setError(null);
    } catch (err) {
      setError(normalizeError(err));
    } finally {
      setLoading(false);
    }
  }

  async function submit(event?: FormEvent) {
    event?.preventDefault();
    const message = draft.trim();
    if (!message || sending) {
      return;
    }
    setDraft("");
    setSending(true);
    setError(null);
    try {
      const response = await sendWebChatMessage(message);
      setChat(response);
    } catch (err) {
      setError(normalizeError(err));
      setDraft(message);
    } finally {
      setSending(false);
    }
  }

  function handleKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      void submit();
    }
  }

  return (
    <main className="flex min-h-screen flex-col bg-background text-foreground">
      <header className="border-b border-border bg-card/80 backdrop-blur">
        <div className="mx-auto flex h-16 w-full max-w-5xl items-center justify-between gap-3 px-4 sm:px-6">
          <div className="flex min-w-0 items-center gap-3">
            <Button asChild variant="ghost" size="icon" aria-label="Back">
              <Link href="/">
                <ArrowLeft className="h-4 w-4" />
              </Link>
            </Button>
            <div className="flex min-w-0 items-center gap-3">
              <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-primary text-primary-foreground">
                <Sparkles className="h-5 w-5" />
              </div>
              <div className="min-w-0">
                <h1 className="truncate text-base font-semibold sm:text-lg">Florence Web Chat</h1>
                <p className="truncate text-xs text-muted-foreground">
                  {chat ? `${chat.household.name} · ${chat.channel.type}` : "Test surface"}
                </p>
              </div>
            </div>
          </div>
          <Button type="button" variant="outline" size="sm" onClick={refresh} disabled={loading || sending}>
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            <span className="hidden sm:inline">Refresh</span>
          </Button>
        </div>
      </header>

      <section className="mx-auto flex min-h-0 w-full max-w-5xl flex-1 flex-col px-4 py-4 sm:px-6">
        <div
          ref={transcriptRef}
          className="min-h-[55vh] flex-1 overflow-y-auto rounded-2xl border border-border bg-white/55 p-3 shadow-sm sm:p-5"
        >
          {loading && !chat ? (
            <div className="flex h-full min-h-[45vh] items-center justify-center text-muted-foreground">
              <Loader2 className="h-5 w-5 animate-spin" />
            </div>
          ) : messages.length ? (
            <div className="grid gap-3">
              {messages.map((message) => (
                <ChatBubble key={message.id} message={message} />
              ))}
              {sending && (
                <div className="flex justify-start">
                  <div className="rounded-2xl border border-border bg-card px-4 py-3 text-sm text-muted-foreground shadow-sm">
                    <Loader2 className="h-4 w-4 animate-spin" />
                  </div>
                </div>
              )}
            </div>
          ) : (
            <div className="flex h-full min-h-[45vh] items-center justify-center text-sm text-muted-foreground">
              No messages yet.
            </div>
          )}
        </div>

        {error && (
          <div className="mt-3 rounded-xl border border-destructive/30 bg-destructive/10 px-4 py-3 text-sm text-destructive">
            {error}
          </div>
        )}

        <form onSubmit={submit} className="mt-3 flex gap-2">
          <Textarea
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            onKeyDown={handleKeyDown}
            disabled={sending}
            placeholder="Message Florence"
            className="min-h-14 resize-none rounded-2xl bg-card py-3"
            rows={2}
          />
          <Button type="submit" size="icon" disabled={!draft.trim() || sending} aria-label="Send">
            {sending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
          </Button>
        </form>
      </section>
    </main>
  );
}
