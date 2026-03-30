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

const pageLabels: Record<string, string> = {
  "/setup": "Setup",
  "/accounts": "Accounts",
  "/settings": "Settings",
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

  const label = pageLabels[currentPath] || "Setup";

  return (
    <div className="min-h-screen bg-primary p-2 pt-0">
      <nav className="flex h-14 shrink-0 items-center px-4">
        <Link href="/setup" className="flex items-center gap-3">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-white/20">
            <Sparkles className="h-4 w-4 text-white" />
          </div>
          <span className="font-semibold text-white">Florence</span>
          <Badge className="border-0 bg-white/20 text-white">{label}</Badge>
        </Link>

        <div className="hidden flex-1 items-center justify-center gap-1 sm:flex">
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

      <main className="min-h-[calc(100vh-64px)] overflow-hidden rounded-xl bg-background p-6 shadow-sm sm:p-8">
        {children}
      </main>
    </div>
  );
}
