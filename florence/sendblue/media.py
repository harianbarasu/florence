"""Sendblue media ingestion helpers for Florence webhook payloads."""

from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx

from florence.media.openai_extract import (
    compact_text,
    extract_image_text_with_openai,
    extract_pdf_text_with_openai,
)

logger = logging.getLogger(__name__)

_MAX_MEDIA_PARTS = 3
_MAX_MEDIA_BYTES = 8_000_000


@dataclass(slots=True)
class _SendblueMediaRef:
    url: str
    mime_type: str | None = None
    filename: str | None = None


def _read_string(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _read_object(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _read_array(value: Any) -> list[Any] | None:
    return value if isinstance(value, list) else None


def _strip_html_tags(html: str) -> str:
    import re

    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()


def _decode_data_url(raw_url: str) -> tuple[bytes, str | None]:
    prefix, _, payload = raw_url.partition(",")
    if not payload:
        return b"", None
    mime_type = None
    if prefix.startswith("data:"):
        mime_type = prefix[5:].split(";")[0].strip() or None
    try:
        decoded = base64.b64decode(payload, validate=False)
    except Exception:
        return b"", mime_type
    return decoded, mime_type


def _download_media_bytes(ref: _SendblueMediaRef, *, timeout_seconds: float) -> tuple[bytes, str | None]:
    raw_url = ref.url.strip()
    if raw_url.startswith("data:"):
        return _decode_data_url(raw_url)
    response = httpx.get(raw_url, timeout=timeout_seconds)
    response.raise_for_status()
    mime_type = response.headers.get("content-type")
    return response.content[:_MAX_MEDIA_BYTES], mime_type


def _guess_filename_from_url(url: str) -> str | None:
    path = urlparse(url).path
    if not path:
        return None
    tail = path.rsplit("/", 1)[-1].strip()
    return tail or None


def _extract_ref_from_object(obj: dict[str, Any]) -> _SendblueMediaRef | None:
    url = (
        _read_string(obj.get("url"))
        or _read_string(obj.get("media_url"))
        or _read_string(obj.get("file_url"))
        or _read_string(obj.get("image_url"))
        or _read_string(obj.get("attachment_url"))
        or _read_string(obj.get("content_url"))
        or _read_string(obj.get("download_url"))
        or _read_string(obj.get("proxy_url"))
    )
    if not url:
        value = _read_string(obj.get("value"))
        if value and (value.startswith("http://") or value.startswith("https://") or value.startswith("data:")):
            url = value
    if not url:
        return None

    mime_type = (
        _read_string(obj.get("mime_type"))
        or _read_string(obj.get("content_type"))
        or _read_string(obj.get("media_type"))
        or _read_string(obj.get("mimeType"))
    )
    filename = (
        _read_string(obj.get("filename"))
        or _read_string(obj.get("file_name"))
        or _read_string(obj.get("name"))
        or _guess_filename_from_url(url)
    )
    return _SendblueMediaRef(url=url, mime_type=mime_type, filename=filename)


def _extract_media_refs(payload: dict[str, Any]) -> list[_SendblueMediaRef]:
    refs: list[_SendblueMediaRef] = []
    seen_urls: set[str] = set()
    candidates: list[_SendblueMediaRef] = []

    direct = _extract_ref_from_object(payload)
    if direct is not None:
        candidates.append(direct)

    for key in ("media", "file", "document", "attachment", "image"):
        nested = _read_object(payload.get(key))
        if nested is None:
            continue
        nested_ref = _extract_ref_from_object(nested)
        if nested_ref is not None:
            candidates.append(nested_ref)

    for key in ("attachments", "files", "media", "images"):
        for item in _read_array(payload.get(key)) or []:
            if not isinstance(item, dict):
                continue
            nested_ref = _extract_ref_from_object(item)
            if nested_ref is not None:
                candidates.append(nested_ref)

    for ref in candidates:
        key = ref.url.strip()
        if not key or key in seen_urls:
            continue
        seen_urls.add(key)
        refs.append(ref)
    return refs


def _extract_media_text(ref: _SendblueMediaRef, *, content: bytes, content_type: str | None) -> str | None:
    mime_type = (ref.mime_type or content_type or "").split(";")[0].strip().lower()
    filename = (ref.filename or "").lower()

    if mime_type.startswith("text/plain") or filename.endswith(".txt"):
        text = content.decode("utf-8", errors="ignore").strip()
        return compact_text(text, max_length=2_000) if text else None
    if "html" in mime_type or filename.endswith((".html", ".htm")):
        text = _strip_html_tags(content.decode("utf-8", errors="ignore"))
        return compact_text(text, max_length=2_000) if text else None
    if mime_type == "application/pdf" or filename.endswith(".pdf"):
        return extract_pdf_text_with_openai(
            pdf_bytes=content,
            filename=ref.filename,
            api_key_env_names=("FLORENCE_CHAT_MEDIA_OPENAI_API_KEY", "OPENAI_API_KEY"),
            base_url_env_name="FLORENCE_CHAT_MEDIA_OPENAI_BASE_URL",
            model_env_name="FLORENCE_CHAT_MEDIA_MODEL",
            log_label="Sendblue PDF extraction",
            system_text=(
                "Extract plain text details from this PDF. Preserve dates, times, names, locations, "
                "deadlines, and required items when present."
            ),
            max_output_chars=4_500,
        )
    if mime_type.startswith("image/") or filename.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".heic", ".heif")):
        final_mime = mime_type if mime_type.startswith("image/") else "image/jpeg"
        return extract_image_text_with_openai(
            image_bytes=content,
            mime_type=final_mime,
            filename=ref.filename,
            api_key_env_names=("FLORENCE_CHAT_MEDIA_OPENAI_API_KEY", "OPENAI_API_KEY"),
            base_url_env_name="FLORENCE_CHAT_MEDIA_OPENAI_BASE_URL",
            model_env_name="FLORENCE_CHAT_MEDIA_MODEL",
            log_label="Sendblue image extraction",
            max_output_chars=5_000,
        )
    return None


def enrich_sendblue_payload_with_media_text(
    payload: dict[str, Any],
    *,
    timeout_seconds: float = 20.0,
) -> bool:
    refs = _extract_media_refs(payload)
    if not refs:
        return False

    snippets: list[str] = []
    for ref in refs[:_MAX_MEDIA_PARTS]:
        try:
            content, content_type = _download_media_bytes(ref, timeout_seconds=timeout_seconds)
        except Exception:
            logger.exception("Failed to download Sendblue media payload for %s", ref.url)
            continue
        text = _extract_media_text(ref, content=content, content_type=content_type)
        if not text:
            continue
        label = ref.filename or ref.mime_type or content_type or "attachment"
        snippets.append(f"{label}: {text}")

    if not snippets:
        return False

    existing = _read_string(payload.get("content")) or ""
    suffix = "Media context extracted from attachments:\n" + "\n".join(snippets)
    payload["content"] = f"{existing}\n\n{suffix}" if existing else suffix
    return True
