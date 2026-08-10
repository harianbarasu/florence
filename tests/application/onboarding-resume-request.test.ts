import { describe, expect, it } from "vitest";
import { isOnboardingResumeRequest } from "../../src/application/florence-application.js";

describe("onboarding resume requests", () => {
  it.each([
    "continue setup",
    "Resume my onboarding!",
    "Can I get an updated link",
    "updated link",
    "new onboarding link",
    "send me a fresh setup link",
    "the link is expired",
    "URL again?",
  ])("routes %j to the app-owned onboarding handoff", (message) => {
    expect(isOnboardingResumeRequest(message)).toBe(true);
  });

  it.each([
    "send me a review link",
    "I need a new Google link",
    "privacy link",
    "open account settings",
    "what is this link",
    "I have a new link",
  ])("keeps %j out of onboarding handoff routing", (message) => {
    expect(isOnboardingResumeRequest(message)).toBe(false);
  });
});
