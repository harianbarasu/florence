"""Shared relevance hints and helpers for Florence Google candidate scoring."""

from __future__ import annotations

LOGISTICS_HINTS = (
    "no class",
    "class canceled",
    "class cancelled",
    "canceled",
    "cancelled",
    "closed",
    "family day",
    "picture day",
    "picture retake",
    "photo retake",
    "recital",
    "field trip",
    "early dismissal",
    "dismissal",
    "late start",
    "no school",
    "open house",
    "conference",
    "concert",
    "performance",
    "practice",
    "orientation",
    "graduation",
    "ceremony",
    "spirit day",
    "holiday",
    "break",
    "schedule",
    "calendar",
    "pickup",
    "pick up",
    "pickup window",
    "drop off",
    "dropoff",
    "arrival",
    "check-in",
    "permission slip",
    "permission form",
    "waiver",
    "release form",
    "due",
    "deadline",
    "dress code",
    "uniform",
    "costume",
    "bring",
    "snack",
    "lunch order",
    "supplies list",
    "school supplies",
    "volunteer",
    "game",
    "match",
    "lesson",
    "tournament",
    "camp",
    "birthday party",
    "party",
    "dentist",
    "doctor",
    "pediatric",
)

PROMOTIONAL_HINTS = (
    "unsubscribe",
    "sale",
    "discount",
    "offer",
    "marketing",
    "promo",
    "promotional",
    "shop now",
    "buy now",
    "new arrivals",
    "collection",
    "spring/summer",
    "podcast",
    "episode",
    "newsletter",
    "digest",
    "sponsored",
    "recruiting",
    "recruiter",
    "job alert",
    "career",
    "linkedin",
    "connections",
    "puzzle",
    "political",
    "politics",
    "election",
    "opinion",
    "news",
    "new york times",
    "recipe",
    "shopping",
    "listen now",
    "watch now",
    "new brand",
    "creator",
    "red carpet",
)

CHILD_ACTIVITY_HINTS = (
    "birthday",
    "party",
    "class",
    "lesson",
    "practice",
    "game",
    "recital",
    "music",
    "band",
    "voice",
    "soccer",
    "baseball",
    "basketball",
    "dance",
    "gymnastics",
    "coaching",
    "camp",
    "playdate",
    "school",
)

PERSONAL_CALENDAR_HINTS = (
    "work",
    "meeting",
    "zoom",
    "1:1",
    "lunch",
    "coffee",
    "gym",
    "yoga",
    "pilates",
    "client",
    "flight",
    "travel",
    "haircut",
    "date night",
)

ALL_DAY_HINTS = (
    "picture day",
    "field trip",
    "no school",
    "holiday",
    "break",
    "spirit day",
)

AMBIGUITY_HINTS = (
    "group a",
    "group b",
    "if ",
    "depending on",
    "tbd",
    "to be determined",
    "tentative",
    "unless",
    "waitlist",
)

SCHOOL_SENDER_HINTS = (
    "school",
    "charter",
    "academy",
    "district",
    "classroom",
    "teacher",
    "principal",
    "pta",
    "pto",
    "registrar",
    "attendance",
    ".edu",
)


GMAIL_LOW_VALUE_CATEGORY_LABELS = frozenset(
    {
        "CATEGORY_PROMOTIONS",
        "CATEGORY_SOCIAL",
        "CATEGORY_FORUMS",
    }
)

GMAIL_NON_ACTIONABLE_LABELS = frozenset({"SENT", "DRAFT", "TRASH", "SPAM"})


def count_hint_hits(source: str, hints: tuple[str, ...]) -> int:
    return sum(1 for hint in hints if hint in source)


def normalize_gmail_label_ids(label_ids: tuple[str, ...] | list[str] | set[str]) -> set[str]:
    return {str(label).strip().upper() for label in label_ids if str(label).strip()}


def gmail_label_suppression_reason(label_ids: tuple[str, ...] | list[str] | set[str]) -> str | None:
    labels = normalize_gmail_label_ids(label_ids)
    if not labels:
        return None
    if labels.intersection(GMAIL_NON_ACTIONABLE_LABELS):
        return "non_inbox_gmail_label"
    if "UNREAD" not in labels:
        return "gmail_already_read"
    if labels.intersection(GMAIL_LOW_VALUE_CATEGORY_LABELS):
        return "low_value_gmail_category"
    return None
