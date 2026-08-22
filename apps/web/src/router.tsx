import { createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import { AppShell, CalendarPage, PreferencesPage, VaultPage, WorkspacePage } from "./App";

const rootRoute = createRootRoute({ component: AppShell });
const workspaceRoute = createRoute({ getParentRoute: () => rootRoute, path: "/", component: WorkspacePage });
const vaultRoute = createRoute({ getParentRoute: () => rootRoute, path: "/vault", component: VaultPage });
const calendarRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/calendar",
  component: CalendarPage,
});
const preferencesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/preferences",
  component: PreferencesPage,
});

const routeTree = rootRoute.addChildren([workspaceRoute, calendarRoute, vaultRoute, preferencesRoute]);
export const router = createRouter({ routeTree, defaultPreload: "intent", scrollRestoration: true });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
