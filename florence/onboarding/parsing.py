"""Shared parsing helpers for Florence onboarding inputs."""

from __future__ import annotations

import re


def split_names(text: str) -> list[str]:
    normalized = re.sub(r"\b(?:and|&)\b", ",", text, flags=re.IGNORECASE)
    return [part.strip(" .,!?:;") for part in normalized.split(",") if part.strip(" .,!?:;")]


def split_entries(text: str) -> list[str]:
    if "\n" in text:
        return [part.strip(" .,!?:;") for part in text.splitlines() if part.strip(" .,!?:;")]
    if ";" in text:
        return [part.strip(" .,!?:;") for part in text.split(";") if part.strip(" .,!?:;")]
    return split_names(text)


def split_labels(text: str) -> list[str]:
    if re.search(r"^\s*none\b", text, re.IGNORECASE):
        return []
    return split_names(text)


def extract_child_names(entries: list[str]) -> list[str]:
    child_names: list[str] = []
    seen: set[str] = set()
    stopwords = {
        "he",
        "she",
        "they",
        "we",
        "i",
        "it",
        "my",
        "our",
        "his",
        "her",
        "their",
        "kid",
        "kids",
        "child",
        "children",
        "son",
        "daughter",
    }
    name_pattern = re.compile(
        r"\b([A-Z][A-Za-z'’-]*(?:\s+[A-Z][A-Za-z'’-]*)?)\s+(?:is|turns|goes|attends|starts|starting)\b",
    )

    def normalize(candidate: str) -> str | None:
        cleaned = " ".join(candidate.split()).strip(" .,!?:;\"'")
        if not cleaned:
            return None
        lowered = cleaned.lower()
        if lowered in stopwords:
            return None
        if lowered.endswith("'s") or lowered.endswith("’s"):
            cleaned = cleaned[:-2].strip()
            lowered = cleaned.lower()
        if not cleaned or lowered in stopwords:
            return None
        if cleaned.islower():
            cleaned = " ".join(part.capitalize() for part in cleaned.split())
        return cleaned

    def add_name(candidate: str) -> None:
        normalized = normalize(candidate)
        if normalized is None:
            return
        key = normalized.lower()
        if key in seen:
            return
        seen.add(key)
        child_names.append(normalized)

    for entry in entries:
        matched_names = [match.group(1) for match in name_pattern.finditer(entry)]
        if matched_names:
            for candidate in matched_names:
                add_name(candidate)
            continue

        head = re.split(r"\s*(?:-|:|\(|,)\s*", entry, maxsplit=1)[0]
        first_token_match = re.match(r"\s*([A-Za-z][A-Za-z'’-]*)", head)
        if first_token_match:
            add_name(first_token_match.group(1))
            continue

        cleaned = " ".join(head.split()).strip(" .,!?:;")
        add_name(cleaned)

    return child_names
