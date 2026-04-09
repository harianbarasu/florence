"""User-facing text safety helpers for Florence replies."""

from __future__ import annotations

import re

_INTERNAL_MEMBER_ID_RE = re.compile(r"\bmem_[A-Za-z0-9_]+\b")
_INTERNAL_OBJECT_ID_RE = re.compile(
    r"\b(?:hh|chan|msg|work|evt|routine|linkreq|nudge|ident|pref|school|child)_[A-Za-z0-9_]+\b"
)


def scrub_internal_ids(text: str) -> str:
    """Remove Florence internal ids from any user-facing text."""
    if not text:
        return text
    scrubbed = _INTERNAL_MEMBER_ID_RE.sub("another parent", text)
    scrubbed = _INTERNAL_OBJECT_ID_RE.sub("that item", scrubbed)
    return scrubbed
