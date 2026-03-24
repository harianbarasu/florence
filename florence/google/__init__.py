"""Google OAuth and continuous sync services for Florence."""

from florence.google.oauth import (
    FLORENCE_GOOGLE_OAUTH_SCOPES,
    build_google_oauth_connect_url,
    decode_google_oauth_state,
    encode_google_oauth_state,
    exchange_google_code_for_tokens,
    fetch_google_user_email,
    fetch_primary_google_calendar,
    refresh_google_access_token,
)
from florence.google.fetch import (
    build_gmail_sync_item,
    build_parent_calendar_sync_item,
    list_recent_gmail_sync_items,
    list_recent_parent_calendar_sync_items,
)
from florence.google.sync import (
    FlorenceGoogleSyncBatch,
    FlorenceGoogleSyncResult,
    build_google_import_candidates,
)
from florence.google.types import (
    FlorenceGoogleOauthState,
    GmailSyncItem,
    GoogleCalendarMetadata,
    GoogleTokenResponse,
    ParentCalendarSyncItem,
)

__all__ = [
    "FLORENCE_GOOGLE_OAUTH_SCOPES",
    "FlorenceGoogleOauthState",
    "FlorenceGoogleSyncBatch",
    "FlorenceGoogleSyncResult",
    "GmailSyncItem",
    "GoogleCalendarMetadata",
    "GoogleTokenResponse",
    "ParentCalendarSyncItem",
    "build_gmail_sync_item",
    "build_google_import_candidates",
    "build_google_oauth_connect_url",
    "build_parent_calendar_sync_item",
    "decode_google_oauth_state",
    "encode_google_oauth_state",
    "exchange_google_code_for_tokens",
    "fetch_google_user_email",
    "fetch_primary_google_calendar",
    "list_recent_gmail_sync_items",
    "list_recent_parent_calendar_sync_items",
    "refresh_google_access_token",
]
