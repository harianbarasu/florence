import { createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import { AppShell, HomePage, OnboardingPage, PeoplePage, SettingsPage } from "./App";

const rootRoute = createRootRoute({ component: AppShell });
const homeRoute = createRoute({ getParentRoute: () => rootRoute, path: "/", component: HomePage });
const onboardingRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/onboarding",
  component: OnboardingPage,
});
const peopleRoute = createRoute({ getParentRoute: () => rootRoute, path: "/people", component: PeoplePage });
const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/settings",
  component: SettingsPage,
});

const routeTree = rootRoute.addChildren([homeRoute, onboardingRoute, peopleRoute, settingsRoute]);
export const router = createRouter({ routeTree, defaultPreload: "intent", scrollRestoration: true });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
