import Link from "next/link";
import {
  CalendarDays,
  CheckSquare,
  Link2,
  Settings,
  Sparkles,
  UserRound,
  Wand2,
} from "lucide-react";
import type { ReactNode } from "react";
import { signOut } from "@/auth";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
} from "@/components/ui/sidebar";
import { withToken } from "@/lib/routes";
import { cn } from "@/lib/utils";

const primaryNavItems = [
  { href: "/calendar", label: "Calendar", icon: CalendarDays },
  { href: "/review", label: "Review", icon: CheckSquare },
  { href: "/connections", label: "Connections", icon: Link2 },
] as const;

const utilityNavItems = [
  { href: "/settings", label: "Settings", icon: Settings },
] as const;

const setupNavItem = { href: "/setup", label: "Setup", icon: Wand2 } as const;

const pageLabels: Record<string, string> = {
  "/setup": "Setup",
  "/calendar": "Calendar",
  "/review": "Review",
  "/connections": "Connections",
  "/settings": "Settings",
};

export async function AppShell({
  currentPath,
  userName,
  userEmail,
  token,
  children,
}: {
  currentPath: string;
  userName: string;
  userEmail: string;
  token?: string;
  children: ReactNode;
}) {
  async function handleSignOut() {
    "use server";
    await signOut({ redirectTo: "/login" });
  }

  const label = pageLabels[currentPath] || "Setup";
  const navItems = currentPath === "/setup" ? [setupNavItem, ...primaryNavItems] : primaryNavItems;

  return (
    <SidebarProvider className="bg-[radial-gradient(circle_at_top_left,_rgba(28,91,122,0.16),_transparent_38%),linear-gradient(180deg,_#f7f1e7_0%,_#f3ece2_100%)]">
      <Sidebar>
        <SidebarHeader className="space-y-4">
          <Link href={withToken("/calendar", token)} className="flex items-center gap-3">
            <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-sidebar-primary text-sidebar-primary-foreground shadow-sm">
              <Sparkles className="h-5 w-5" />
            </div>
            <div className="space-y-1">
              <div className="text-base font-semibold">Florence</div>
              <div className="text-xs text-muted-foreground">Household calendar, review, and connections</div>
            </div>
          </Link>
          <div className="flex items-center gap-2">
            <Badge variant={currentPath === "/setup" ? "secondary" : "outline"}>{label}</Badge>
            <div className="text-xs text-muted-foreground">Web control plane</div>
          </div>
        </SidebarHeader>

        <SidebarContent className="space-y-6">
          <SidebarGroup>
            <SidebarGroupLabel>Primary</SidebarGroupLabel>
            <SidebarMenu>
              {navItems.map((item) => {
                const Icon = item.icon;
                return (
                  <SidebarMenuItem key={item.href}>
                    <SidebarMenuButton asChild isActive={currentPath === item.href} size="lg">
                      <Link href={withToken(item.href, token)}>
                        <Icon className="h-4 w-4" />
                        <span>{item.label}</span>
                      </Link>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroup>

          <SidebarGroup>
            <SidebarGroupLabel>Workspace</SidebarGroupLabel>
            <SidebarMenu>
              {utilityNavItems.map((item) => {
                const Icon = item.icon;
                return (
                  <SidebarMenuItem key={item.href}>
                    <SidebarMenuButton asChild isActive={currentPath === item.href}>
                      <Link href={withToken(item.href, token)}>
                        <Icon className="h-4 w-4" />
                        <span>{item.label}</span>
                      </Link>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroup>
        </SidebarContent>

        <SidebarFooter className="space-y-3">
          <div className="rounded-[1.25rem] border border-sidebar-border/80 bg-white/40 p-4">
            <div className="flex items-start gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-sidebar-primary/10 text-sidebar-primary">
                <UserRound className="h-4 w-4" />
              </div>
              <div className="min-w-0 space-y-1">
                <div className="truncate text-sm font-semibold">{userName}</div>
                <div className="truncate text-xs text-muted-foreground">{userEmail}</div>
              </div>
            </div>
          </div>
          <form action={handleSignOut}>
            <Button variant="outline" className="w-full">
              Sign out
            </Button>
          </form>
        </SidebarFooter>
      </Sidebar>

      <SidebarInset>
        <div className="border-b border-border/70 bg-background/70 backdrop-blur-sm">
          <div className="flex flex-col gap-4 px-5 py-5 sm:px-8">
            <div className="flex flex-wrap items-center justify-between gap-4">
              <div className="space-y-1">
                <div className="text-xs font-semibold uppercase tracking-[0.22em] text-muted-foreground">Florence</div>
                <div className="text-2xl font-semibold tracking-tight">{label}</div>
              </div>
              <Badge variant={currentPath === "/setup" ? "secondary" : "outline"}>{currentPath === "/setup" ? "Onboarding" : "Household view"}</Badge>
            </div>

            <div className="flex gap-2 overflow-x-auto pb-1 lg:hidden">
              {[...navItems, ...utilityNavItems].map((item) => {
                const active = currentPath === item.href;
                return (
                  <Link
                    key={item.href}
                    href={withToken(item.href, token)}
                    className={cn(
                      "shrink-0 rounded-full px-4 py-2 text-sm font-medium transition-colors",
                      active ? "bg-primary text-primary-foreground" : "border border-border bg-card text-card-foreground",
                    )}
                  >
                    {item.label}
                  </Link>
                );
              })}
            </div>
          </div>
        </div>

        <main className="min-h-[calc(100vh-145px)] px-5 py-6 sm:px-8 sm:py-8">{children}</main>
      </SidebarInset>
    </SidebarProvider>
  );
}
