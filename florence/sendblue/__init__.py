"""Sendblue transport support for Florence."""

from florence.sendblue.adapter import (
    build_sendblue_group_thread_id,
    build_sendblue_thread_id,
    parse_sendblue_payload,
)
from florence.sendblue.client import (
    FlorenceSendblueClient,
    FlorenceSendbluePermanentOptOutError,
    FlorenceSendblueSendResult,
)

__all__ = [
    "build_sendblue_group_thread_id",
    "build_sendblue_thread_id",
    "FlorenceSendblueClient",
    "FlorenceSendbluePermanentOptOutError",
    "FlorenceSendblueSendResult",
    "parse_sendblue_payload",
]
