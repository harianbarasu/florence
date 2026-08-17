import { createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import { AppShell, PreferencesPage, VaultPage, WorkspacePage } from "./App";

const rootRoute = createRootRoute({ component: AppShell });
const workspaceRoute = createRoute({ getParentRoute: () => rootRoute, path: "/", component: WorkspacePage });
const vaultRoute = createRoute({ getParentRoute: () => rootRoute, path: "/vault", component: VaultPage });
const preferencesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/preferences",
  component: PreferencesPage,
});

const routeTree = rootRoute.addChildren([workspaceRoute, vaultRoute, preferencesRoute]);
export const router = createRouter({ routeTree, defaultPreload: "intent", scrollRestoration: true });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
