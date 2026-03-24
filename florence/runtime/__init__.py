"""Lazy runtime exports for Florence services."""

from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "CandidateReviewPrompt": ("florence.runtime.services", "CandidateReviewPrompt"),
    "CandidateReviewResult": ("florence.runtime.services", "CandidateReviewResult"),
    "FlorenceCandidateReviewService": ("florence.runtime.services", "FlorenceCandidateReviewService"),
    "FlorenceEntrypointResult": ("florence.runtime.entrypoints", "FlorenceEntrypointResult"),
    "FlorenceEntrypointService": ("florence.runtime.entrypoints", "FlorenceEntrypointService"),
    "FlorenceGoogleAccountLinkService": ("florence.runtime.services", "FlorenceGoogleAccountLinkService"),
    "FlorenceGoogleCallbackResult": ("florence.runtime.services", "FlorenceGoogleCallbackResult"),
    "FlorenceGoogleConnectLink": ("florence.runtime.services", "FlorenceGoogleConnectLink"),
    "FlorenceGoogleOauthConfig": ("florence.runtime.entrypoints", "FlorenceGoogleOauthConfig"),
    "FlorenceGoogleSyncPersistenceService": ("florence.runtime.services", "FlorenceGoogleSyncPersistenceService"),
    "FlorenceGoogleSyncCycleResult": ("florence.runtime.services", "FlorenceGoogleSyncCycleResult"),
    "FlorenceGoogleSyncWorkerService": ("florence.runtime.services", "FlorenceGoogleSyncWorkerService"),
    "FlorenceHouseholdChatReply": ("florence.runtime.chat", "FlorenceHouseholdChatReply"),
    "FlorenceHouseholdChatService": ("florence.runtime.chat", "FlorenceHouseholdChatService"),
    "FlorenceHouseholdQueryService": ("florence.runtime.services", "FlorenceHouseholdQueryService"),
    "FlorenceHTTPResult": ("florence.runtime.production", "FlorenceHTTPResult"),
    "FlorenceIdentityResolver": ("florence.runtime.resolver", "FlorenceIdentityResolver"),
    "FlorenceOnboardingSessionService": ("florence.runtime.services", "FlorenceOnboardingSessionService"),
    "FlorenceProductionService": ("florence.runtime.production", "FlorenceProductionService"),
    "FlorenceResolvedTransportContext": ("florence.runtime.resolver", "FlorenceResolvedTransportContext"),
    "FlorenceSyncScheduler": ("florence.runtime.scheduler", "FlorenceSyncScheduler"),
    "display_name_from_handle": ("florence.runtime.resolver", "display_name_from_handle"),
    "household_name_from_display_name": ("florence.runtime.resolver", "household_name_from_display_name"),
    "infer_identity_kind": ("florence.runtime.resolver", "infer_identity_kind"),
    "normalize_identity_value": ("florence.runtime.resolver", "normalize_identity_value"),
    "run_florence_google_sync": ("florence.runtime.jobs", "run_florence_google_sync"),
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
