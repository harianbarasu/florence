import Link from "next/link";
import { Home, Settings, UserRound, Waypoints } from "lucide-react";
import type { ReactNode } from "react";
import { signOut } from "@/auth";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

const navItems = [
  { href: "/setup", label: "Setup", icon: Home },
  { href: "/accounts", label: "Accounts", icon: Waypoints },
  { href: "/settings", label: "Settings", icon: Settings },
];

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

  return (
    <div className="min-h-screen px-3 py-3 sm:px-4">
      <div className="mx-auto grid min-h-[calc(100vh-1.5rem)] max-w-7xl gap-3 lg:grid-cols-[260px_minmax(0,1fr)]">
        <aside className="rounded-[1.5rem] border border-sidebar-border bg-sidebar/95 p-4 shadow-[0_18px_50px_rgba(49,33,18,0.08)]">
          <div className="mb-8 flex items-start justify-between gap-3">
            <div>
              <div className="mb-2 inline-flex items-center rounded-full bg-white/80 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-primary">
                Florence
              </div>
              <h1 className="text-xl font-semibold">Control Plane</h1>
              <p className="mt-1 text-sm leading-6 text-muted-foreground">
                Minimal web controls. Daily usage stays in iMessage.
              </p>
            </div>
            <Badge variant="secondary" className="hidden sm:inline-flex">
              Web
            </Badge>
          </div>

          <nav className="grid gap-2">
            {navItems.map((item) => {
              const Icon = item.icon;
              const active = currentPath === item.href;
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={cn(
                    "flex items-center gap-3 rounded-2xl px-4 py-3 text-sm font-medium transition-colors",
                    active
                      ? "bg-sidebar-primary text-sidebar-primary-foreground"
                      : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                  )}
                >
                  <Icon className="h-4 w-4" />
                  {item.label}
                </Link>
              );
            })}
          </nav>

          <div className="mt-8 rounded-[1.25rem] border border-border/70 bg-white/65 p-4">
            <div className="mb-3 flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-primary/10 text-primary">
                <UserRound className="h-5 w-5" />
              </div>
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold">{userName}</div>
                <div className="truncate text-xs text-muted-foreground">{userEmail}</div>
              </div>
            </div>
            <form action={handleSignOut}>
              <Button variant="outline" className="w-full">
                Sign out
              </Button>
            </form>
          </div>
        </aside>

        <main className="overflow-hidden rounded-[1.75rem] border border-border/70 bg-card/95 shadow-[0_18px_50px_rgba(49,33,18,0.08)]">
          <div className="border-b border-border/70 px-5 py-4 sm:px-8">
            <div className="mb-3 flex items-center justify-between gap-4">
              <div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">
                  Florence
                </div>
                <div className="text-lg font-semibold">Onboarding and account management</div>
              </div>
              <Badge variant="outline" className="hidden sm:inline-flex">
                iMessage is still the core UX
              </Badge>
            </div>
            <div className="flex gap-2 overflow-x-auto pb-1 lg:hidden">
              {navItems.map((item) => {
                const active = currentPath === item.href;
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={cn(
                      "rounded-full border px-3 py-2 text-sm font-medium whitespace-nowrap",
                      active ? "border-primary bg-primary text-primary-foreground" : "border-border bg-white",
                    )}
                  >
                    {item.label}
                  </Link>
                );
              })}
            </div>
          </div>
          <div className="px-5 py-5 sm:px-8 sm:py-8">{children}</div>
        </main>
      </div>
    </div>
  );
}
