import Link from "next/link";
import {
  ChevronRight,
  Home,
  Settings,
  Sparkles,
  UserRound,
  Waypoints,
} from "lucide-react";
import type { ReactNode } from "react";
import { signOut } from "@/auth";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarRail,
} from "@/components/ui/sidebar";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

const navItems = [
  {
    href: "/setup",
    label: "Setup",
    description: "Connect Google and finish onboarding",
    icon: Home,
  },
  {
    href: "/accounts",
    label: "Accounts",
    description: "Review connected Google accounts",
    icon: Waypoints,
  },
  {
    href: "/settings",
    label: "Settings",
    description: "Tune Florence defaults",
    icon: Settings,
  },
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
    <SidebarProvider className="bg-transparent">
      <div className="min-h-screen w-full px-3 py-3 sm:px-4">
        <div className="mx-auto flex min-h-[calc(100vh-1.5rem)] max-w-7xl overflow-hidden rounded-[2rem] border border-border/70 bg-card/95 shadow-[0_24px_80px_rgba(49,33,18,0.12)] backdrop-blur-sm">
          <Sidebar className="border-r border-sidebar-border/80 bg-[linear-gradient(180deg,#f7f0e4_0%,#f2eadc_100%)]">
            <SidebarHeader className="border-b border-sidebar-border/80 p-5">
              <SidebarMenu>
                <SidebarMenuItem>
                  <SidebarMenuButton asChild size="lg">
                    <Link href="/setup">
                      <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-sidebar-primary text-sidebar-primary-foreground shadow-sm">
                        <Sparkles className="h-5 w-5" />
                      </div>
                      <div className="grid flex-1 text-left leading-tight">
                        <span className="truncate text-sm font-semibold">Florence</span>
                        <span className="truncate text-xs text-muted-foreground">
                          Family control plane
                        </span>
                      </div>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              </SidebarMenu>
            </SidebarHeader>

            <SidebarContent className="flex flex-col gap-6 p-4">
              <SidebarGroup>
                <SidebarGroupLabel>Workspace</SidebarGroupLabel>
                <SidebarMenu>
                  {navItems.map((item) => {
                    const Icon = item.icon;
                    const active = currentPath === item.href;
                    return (
                      <SidebarMenuItem key={item.href}>
                        <SidebarMenuButton asChild isActive={active} size="lg">
                          <Link href={item.href}>
                            <Icon className="h-4 w-4 shrink-0" />
                            <div className="grid min-w-0 flex-1 gap-0.5 text-left">
                              <span className="truncate">{item.label}</span>
                              <span
                                className={cn(
                                  "truncate text-xs",
                                  active
                                    ? "text-sidebar-primary-foreground/80"
                                    : "text-muted-foreground",
                                )}
                              >
                                {item.description}
                              </span>
                            </div>
                            <ChevronRight className="h-4 w-4 opacity-60" />
                          </Link>
                        </SidebarMenuButton>
                      </SidebarMenuItem>
                    );
                  })}
                </SidebarMenu>
              </SidebarGroup>

              <div className="rounded-[1.5rem] border border-sidebar-border/80 bg-white/75 p-4 text-sm shadow-sm">
                <div className="mb-2 inline-flex items-center rounded-full bg-accent px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-accent-foreground">
                  Primary UX
                </div>
                <p className="leading-6 text-muted-foreground">
                  Florence still lives in iMessage. This dashboard is only for setup,
                  account management, and admin controls.
                </p>
              </div>
            </SidebarContent>

            <SidebarFooter className="border-t border-sidebar-border/80 p-4">
              <div className="rounded-[1.5rem] border border-sidebar-border/80 bg-white/80 p-4 shadow-sm">
                <div className="mb-4 flex items-center gap-3">
                  <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-primary/10 text-primary">
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
            </SidebarFooter>
            <SidebarRail />
          </Sidebar>

          <SidebarInset className="bg-transparent">
            <header className="border-b border-border/70 bg-[linear-gradient(180deg,rgba(255,255,255,0.72),rgba(255,255,255,0.58))] px-5 py-5 sm:px-8">
              <div className="flex flex-col gap-5 xl:flex-row xl:items-end xl:justify-between">
                <div className="max-w-3xl">
                  <div className="mb-3 flex flex-wrap items-center gap-2">
                    <Badge variant="secondary">Florence</Badge>
                    <Badge variant="outline" className="hidden sm:inline-flex">
                      Web control plane
                    </Badge>
                  </div>
                  <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">
                    {meta.eyebrow}
                  </div>
                  <h1 className="mt-2 text-2xl font-semibold tracking-tight text-balance sm:text-3xl">
                    {meta.title}
                  </h1>
                  <p className="mt-3 max-w-2xl text-sm leading-7 text-muted-foreground sm:text-base">
                    {meta.description}
                  </p>
                </div>

                <div className="grid gap-3 sm:grid-cols-2 xl:w-[360px]">
                  <div className="rounded-[1.5rem] border border-border/70 bg-white/70 p-4 shadow-sm">
                    <div className="text-xs font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                      Surface
                    </div>
                    <div className="mt-2 text-sm font-medium">Daily usage stays in iMessage</div>
                  </div>
                  <div className="rounded-[1.5rem] border border-border/70 bg-[linear-gradient(180deg,rgba(28,91,122,0.12),rgba(28,91,122,0.03))] p-4 shadow-sm">
                    <div className="text-xs font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                      Section
                    </div>
                    <div className="mt-2 text-sm font-medium">{meta.badge}</div>
                  </div>
                </div>
              </div>

              <div className="mt-5 flex gap-2 overflow-x-auto pb-1 lg:hidden">
                {navItems.map((item) => {
                  const active = currentPath === item.href;
                  return (
                    <Button
                      key={item.href}
                      asChild
                      variant={active ? "default" : "outline"}
                      size="sm"
                      className="shrink-0"
                    >
                      <Link href={item.href}>{item.label}</Link>
                    </Button>
                  );
                })}
              </div>
            </header>

            <main className="flex-1 overflow-y-auto px-5 py-6 sm:px-8 sm:py-8">
              {children}
            </main>
          </SidebarInset>
        </div>
      </div>
    </SidebarProvider>
  );
}
