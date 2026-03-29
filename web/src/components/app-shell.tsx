import Link from "next/link";
import { Sparkles, UserRound } from "lucide-react";
import type { ReactNode } from "react";
import { signOut } from "@/auth";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

const navItems = [
  { href: "/setup", label: "Setup" },
  { href: "/accounts", label: "Accounts" },
  { href: "/settings", label: "Settings" },
] as const;

const pageMeta: Record<
  string,
  { eyebrow: string; title: string; description: string; badge: string }
> = {
  "/setup": {
    eyebrow: "Household onboarding",
    title: "Connect Florence to your family's operating system",
    description:
      "Handle setup, watch the first sync, and finish the household profile from a real dashboard instead of a throwaway page.",
    badge: "Setup",
  },
  "/accounts": {
    eyebrow: "Connected accounts",
    title: "Manage the Google accounts Florence can use",
    description:
      "See which Google identities are attached to the household and add or remove accounts without touching the iMessage flow.",
    badge: "Accounts",
  },
  "/settings": {
    eyebrow: "Assistant controls",
    title: "Adjust Florence behavior without leaving the control plane",
    description:
      "Use the web app for account-level settings while the normal day-to-day experience stays in iMessage.",
    badge: "Settings",
  },
};

export async function AppShell({
  currentPath,
  userName,
  userEmail,
  children,
}: {
  currentPath: string;
  userName: string;
  userEmail: string;
  children: ReactNode;
}) {
  async function handleSignOut() {
    "use server";
    await signOut({ redirectTo: "/login" });
  }

  const meta = pageMeta[currentPath] || pageMeta["/setup"];

  return (
    <div className="min-h-screen bg-primary p-2 pt-0">
      {/* Top nav bar */}
      <nav className="flex h-14 shrink-0 items-center px-4">
        <Link href="/setup" className="flex items-center gap-3">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-white/20">
            <Sparkles className="h-4 w-4 text-white" />
          </div>
          <span className="font-semibold text-white">Florence</span>
          <Badge className="border-0 bg-white/20 text-white">
            {meta.badge}
          </Badge>
        </Link>

        {/* Desktop nav links */}
        <div className="hidden flex-1 items-center justify-center gap-1 lg:flex">
          {navItems.map((item) => {
            const active = currentPath === item.href;
            return (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  "rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                  active
                    ? "bg-white/20 text-white"
                    : "text-white/70 hover:bg-white/10 hover:text-white",
                )}
              >
                {item.label}
              </Link>
            );
          })}
        </div>

        {/* User & sign out */}
        <div className="ml-auto flex items-center gap-3">
          <div className="hidden items-center gap-2 sm:flex">
            <div className="flex h-8 w-8 items-center justify-center rounded-full bg-white/20">
              <UserRound className="h-4 w-4 text-white" />
            </div>
            <span className="text-sm text-white/80">{userName}</span>
          </div>
          <form action={handleSignOut}>
            <Button
              variant="ghost"
              size="sm"
              className="text-white/70 hover:bg-white/10 hover:text-white"
            >
              Sign out
            </Button>
          </form>
        </div>
      </nav>

      {/* Main content area */}
      <div className="flex min-h-[calc(100vh-64px)] flex-col overflow-hidden rounded-xl bg-background shadow-sm">
        {/* Mobile nav */}
        <div className="flex gap-2 overflow-x-auto border-b px-4 py-2 lg:hidden">
          {navItems.map((item) => {
            const active = currentPath === item.href;
            return (
              <Button
                key={item.href}
                asChild
                variant={active ? "default" : "ghost"}
                size="sm"
                className="shrink-0"
              >
                <Link href={item.href}>{item.label}</Link>
              </Button>
            );
          })}
        </div>

        {/* Page header */}
        <header className="border-b px-6 py-5 sm:px-8">
          <div className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">
            {meta.eyebrow}
          </div>
          <h1 className="mt-1 text-xl font-semibold tracking-tight sm:text-2xl">
            {meta.title}
          </h1>
          <p className="mt-1.5 max-w-2xl text-sm text-muted-foreground">
            {meta.description}
          </p>
        </header>

        {/* Scrollable content */}
        <main className="flex-1 overflow-y-auto p-6 sm:p-8">
          {children}
        </main>
      </div>
    </div>
  );
}
