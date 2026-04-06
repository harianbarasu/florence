"""Persistence-backed Florence runtime services."""

from __future__ import annotations

import hashlib
import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from florence.contracts import (
    HouseholdContext,
    HouseholdBriefingKind,
    HouseholdProfileKind,
)
from florence.onboarding import (
    OnboardingPrompt,
    OnboardingStage,
    build_onboarding_prompt,
)
from florence.state import FlorenceStateDB


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_local_time_spec(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", value, flags=re.IGNORECASE)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2) or 0)
    meridiem = (match.group(3) or "").lower()
    if meridiem == "pm" and hour < 12:
        hour += 12
    if meridiem == "am" and hour == 12:
        hour = 0
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return (hour, minute)


def _extract_local_time_from_preferences(
    text: str | None,
    *,
    keywords: tuple[str, ...],
    default_hour: int,
    default_minute: int,
) -> tuple[int, int]:
    if text:
        lowered = text.lower()
        for keyword in keywords:
            idx = lowered.find(keyword.lower())
            if idx < 0:
                continue
            window_start = max(0, idx - 8)
            window_end = min(len(text), idx + len(keyword) + 48)
            parsed = _parse_local_time_spec(text[window_start:window_end])
            if parsed is not None:
                return parsed
    return (default_hour, default_minute)


def _local_schedule_days(*, text: str | None, kind: HouseholdBriefingKind) -> list[int]:
    lowered = (text or "").lower()
    if kind == HouseholdBriefingKind.WEEKLY:
        if "friday" in lowered:
            return [4]
        if "saturday" in lowered:
            return [5]
        if "monday" in lowered:
            return [0]
        return [6]
    if "daily" in lowered or "every day" in lowered:
        return [0, 1, 2, 3, 4, 5, 6]
    if kind == HouseholdBriefingKind.EVENING and "school night" in lowered:
        return [0, 1, 2, 3, 6]
    if "weekend" in lowered and "weekday" not in lowered:
        return [5, 6]
    return [0, 1, 2, 3, 4]


def _next_due_local_schedule_iso(
    *,
    household_timezone: str,
    hour: int,
    minute: int,
    days: list[int],
    now: datetime,
) -> str:
    zone = ZoneInfo(household_timezone)
    local_now = now.astimezone(zone)
    for offset in range(0, 8):
        candidate_date = local_now.date() + timedelta(days=offset)
        if candidate_date.weekday() not in days:
            continue
        candidate_local = datetime(
            candidate_date.year,
            candidate_date.month,
            candidate_date.day,
            hour,
            minute,
            tzinfo=zone,
        )
        if candidate_local <= local_now:
            continue
        return candidate_local.astimezone(timezone.utc).isoformat()
    fallback = local_now + timedelta(days=1)
    return datetime(
        fallback.year,
        fallback.month,
        fallback.day,
        hour,
        minute,
        tzinfo=zone,
    ).astimezone(timezone.utc).isoformat()


def _stable_id(prefix: str, *parts: str) -> str:
    raw = ":".join(parts).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()[:20]
    return f"{prefix}_{digest}"


def _clean_label(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(str(value).split()).strip(" ,.;:-")
    return normalized or None


def _sorted_unique(values: set[str]) -> list[str]:
    return sorted(value for value in values if value)


def _metadata_list(metadata: dict[str, object], key: str) -> list[str]:
    raw = metadata.get(key)
    if not isinstance(raw, list):
        return []
    values: list[str] = []
    seen: set[str] = set()
    for item in raw:
        cleaned = _clean_label(str(item))
        if cleaned is None:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        values.append(cleaned)
    return values


def _merge_metadata_list(metadata: dict[str, object], key: str, values: list[str]) -> None:
    merged = _metadata_list(metadata, key)
    seen = {value.lower() for value in merged}
    for raw in values:
        cleaned = _clean_label(raw)
        if cleaned is None:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        merged.append(cleaned)
    if merged:
        metadata[key] = merged


def _grounding_hints_from_settings(settings: dict[str, object] | None) -> dict[str, object]:
    if settings is None:
        return {}
    raw = settings.get("grounding_hints")
    return dict(raw) if isinstance(raw, dict) else {}


def _index_hint_entries(
    hints: dict[str, object],
    *,
    key: str,
    detail_fields: tuple[str, ...],
) -> dict[str, dict[str, list[str]]]:
    indexed: dict[str, dict[str, list[str]]] = {}
    raw_entries = hints.get(key)
    if not isinstance(raw_entries, list):
        return indexed

    for entry in raw_entries:
        if not isinstance(entry, dict):
            continue
        label = _clean_label(str(entry.get("label") or ""))
        if label is None:
            continue
        bucket = indexed.setdefault(label.lower(), {"label": label})
        for field in detail_fields:
            values = entry.get(field)
            if isinstance(values, list):
                bucket[field] = _sorted_unique(
                    {
                        *_metadata_list(bucket, field),
                        *(
                            cleaned
                            for value in values
                            if (cleaned := _clean_label(str(value))) is not None
                        ),
                    }
                )
    return indexed


def _format_grounding_hint_line(label: str, *, primary: list[str], secondary: list[str]) -> str:
    details: list[str] = []
    if primary:
        details.append(", ".join(primary[:2]))
    if secondary:
        details.append(", ".join(secondary[:2]))
    if not details:
        return f"- {label}"
    return f"- {label} ({'; '.join(details)})"


def _augment_onboarding_prompt(
    prompt: OnboardingPrompt | None,
    *,
    settings: dict[str, object] | None,
) -> OnboardingPrompt | None:
    if prompt is None:
        return None

    hints = _grounding_hints_from_settings(settings)
    if prompt.stage == OnboardingStage.COLLECT_CHILD_SCHOOL:
        school_hints = list(_index_hint_entries(hints, key="schools", detail_fields=("domains", "platforms", "contacts")).values())
        if not school_hints:
            return prompt
        lines = [prompt.text, "Google already surfaced a few likely school signals:"]
        for entry in school_hints[:3]:
            lines.append(
                _format_grounding_hint_line(
                    str(entry["label"]),
                    primary=list(entry.get("platforms", [])),
                    secondary=list(entry.get("contacts", [])) or list(entry.get("domains", [])),
                )
            )
        lines.append("Reply with the school or daycare names I should use, even if they match the suggestions.")
        return replace(prompt, text="\n".join(lines))

    if prompt.stage == OnboardingStage.COLLECT_CHILD_ACTIVITIES:
        activity_hints = list(_index_hint_entries(hints, key="activities", detail_fields=("locations", "contacts")).values())
        if not activity_hints:
            return prompt
        lines = [prompt.text, "Google also found likely activity signals:"]
        for entry in activity_hints[:4]:
            lines.append(
                _format_grounding_hint_line(
                    str(entry["label"]),
                    primary=list(entry.get("locations", [])),
                    secondary=list(entry.get("contacts", [])),
                )
            )
        lines.append("Reply with the activity names I should use. If helpful, include the child, like Ava soccer.")
        return replace(prompt, text="\n".join(lines))

    return prompt


def _build_household_context(
    store: FlorenceStateDB,
    *,
    household_id: str,
    actor_member_id: str,
    channel_id: str,
) -> HouseholdContext:
    children = store.list_child_profiles(household_id=household_id)
    schools = store.list_household_profile_items(household_id=household_id, kind=HouseholdProfileKind.SCHOOL)
    activities = store.list_household_profile_items(household_id=household_id, kind=HouseholdProfileKind.ACTIVITY)
    household = store.get_household(household_id)
    grounding_hints = _grounding_hints_from_settings(household.settings if household is not None else None)
    school_hint_entries = _index_hint_entries(
        grounding_hints,
        key="schools",
        detail_fields=("domains", "platforms", "contacts"),
    )
    activity_hint_entries = _index_hint_entries(
        grounding_hints,
        key="activities",
        detail_fields=("locations", "contacts"),
    )
    child_aliases: set[str] = set()
    school_domains: set[str] = set()
    school_platforms: set[str] = set()
    contact_names: set[str] = set()
    location_labels: set[str] = set()

    for child in children:
        child_aliases.update(_metadata_list(child.metadata, "aliases"))
        first_name = _clean_label(child.full_name.split()[0] if child.full_name.strip() else None)
        cleaned_name = _clean_label(child.full_name)
        if first_name is not None and cleaned_name is not None and first_name.lower() != cleaned_name.lower():
            child_aliases.add(first_name)

    for school in schools:
        school_domains.update(_metadata_list(school.metadata, "domains"))
        school_platforms.update(_metadata_list(school.metadata, "platforms"))
        contact_names.update(_metadata_list(school.metadata, "contacts"))

    for activity in activities:
        contact_names.update(_metadata_list(activity.metadata, "contacts"))
        location_labels.update(_metadata_list(activity.metadata, "locations"))

    for entry in school_hint_entries.values():
        school_domains.update(str(value) for value in entry.get("domains", []))
        school_platforms.update(str(value) for value in entry.get("platforms", []))
        contact_names.update(str(value) for value in entry.get("contacts", []))

    for entry in activity_hint_entries.values():
        contact_names.update(str(value) for value in entry.get("contacts", []))
        location_labels.update(str(value) for value in entry.get("locations", []))

    raw_contacts = grounding_hints.get("contacts")
    if isinstance(raw_contacts, list):
        contact_names.update(
            cleaned
            for value in raw_contacts
            if (cleaned := _clean_label(str(value))) is not None
        )

    raw_locations = grounding_hints.get("locations")
    if isinstance(raw_locations, list):
        location_labels.update(
            cleaned
            for value in raw_locations
            if (cleaned := _clean_label(str(value))) is not None
        )

    return HouseholdContext(
        household_id=household_id,
        actor_member_id=actor_member_id,
        channel_id=channel_id,
        visible_child_names=[child.full_name for child in children],
        child_aliases=_sorted_unique(child_aliases),
        school_labels=[item.label for item in schools],
        school_domains=_sorted_unique(school_domains),
        school_platforms=_sorted_unique(school_platforms),
        activity_labels=[item.label for item in activities],
        contact_names=_sorted_unique(contact_names),
        location_labels=_sorted_unique(location_labels),
    )
