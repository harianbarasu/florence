"""Lazy runtime exports for Florence services."""

from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "FlorenceCandidateReviewService": ("florence.runtime.candidate_review", "FlorenceCandidateReviewService"),
    "FlorenceEntrypointResult": ("florence.runtime.entrypoints", "FlorenceEntrypointResult"),
    "FlorenceEntrypointService": ("florence.runtime.entrypoints", "FlorenceEntrypointService"),
    "FlorenceGoogleAccountLinkService": ("florence.runtime.google_services", "FlorenceGoogleAccountLinkService"),
    "FlorenceGoogleSyncPersistenceService": ("florence.runtime.google_services", "FlorenceGoogleSyncPersistenceService"),
    "FlorenceGoogleSyncWorkerService": ("florence.runtime.google_services", "FlorenceGoogleSyncWorkerService"),
    "FlorenceGroupShareService": ("florence.runtime.group_share", "FlorenceGroupShareService"),
    "FlorenceHouseholdManagerService": ("florence.runtime.household_manager", "FlorenceHouseholdManagerService"),
    "FlorenceIdentityResolver": ("florence.runtime.resolver", "FlorenceIdentityResolver"),
    "FlorenceOnboardingSessionService": ("florence.runtime.onboarding_service", "FlorenceOnboardingSessionService"),
    "FlorenceProductionService": ("florence.runtime.production", "FlorenceProductionService"),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str):
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(name)
    module_name, attr_name = target
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
