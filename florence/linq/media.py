"""Linq media ingestion helpers for Florence webhook payloads."""

from __future__ import annotations

import base64
import logging
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

_MAX_MEDIA_PARTS = 3
_MAX_MEDIA_BYTES = 8_000_000
_MAX_MEDIA_TEXT_CHARS = 6_000


@dataclass(slots=True)
class _LinqMediaRef:
    url: str
    mime_type: str | None = None
    filename: str | None = None


def _read_string(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _read_object(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _read_array(value: Any) -> list[Any] | None:
    return value if isinstance(value, list) else None


def _compact_text(raw: str, max_length: int = _MAX_MEDIA_TEXT_CHARS) -> str:
    normalized = " ".join(raw.split())
    if len(normalized) <= max_length:
        return normalized
    return f"{normalized[: max_length - 1].rstrip()}..."


def _strip_html_tags(html: str) -> str:
    import re

    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()


def _response_output_text(response: Any) -> str:
    output_text = getattr(response, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()
    if isinstance(response, dict):
        candidate = response.get("output_text")
        if isinstance(candidate, str):
            return candidate.strip()
    return ""


def _media_model() -> str:
    return os.getenv("FLORENCE_CHAT_MEDIA_MODEL", "gpt-5.4").strip() or "gpt-5.4"


def _openai_client():
    api_key = (
        os.getenv("FLORENCE_CHAT_MEDIA_OPENAI_API_KEY", "").strip()
        or os.getenv("OPENAI_API_KEY", "").strip()
    )
    if not api_key:
        return None
    base_url = os.getenv("FLORENCE_CHAT_MEDIA_OPENAI_BASE_URL", "https://api.openai.com/v1").strip()
    from openai import OpenAI

    return OpenAI(api_key=api_key, base_url=base_url)


def _extract_pdf_text_with_gpt(*, pdf_bytes: bytes, filename: str | None) -> str | None:
    client = _openai_client()
    if client is None:
        logger.warning("Skipping Linq PDF extraction: OPENAI_API_KEY not configured")
        return None
    try:
        response = client.responses.create(
            model=_media_model(),
            input=[
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": (
                                "Extract plain text details from this PDF. Preserve dates, times, names, locations, "
                                "deadlines, and required items when present."
                            ),
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_file",
                            "filename": filename or "attachment.pdf",
                            "file_data": base64.b64encode(pdf_bytes).decode("ascii"),
                        },
                        {"type": "input_text", "text": "Return plain text only."},
                    ],
                },
            ],
            max_output_tokens=1_500,
        )
    except Exception:
        logger.exception("Linq PDF extraction failed for %s", filename or "attachment.pdf")
        return None
    text = _response_output_text(response)
    return _compact_text(text, max_length=4_500) if text else None


def _extract_image_text_with_gpt(*, image_bytes: bytes, mime_type: str, filename: str | None) -> str | None:
    client = _openai_client()
    if client is None:
        logger.warning("Skipping Linq image extraction: OPENAI_API_KEY not configured")
        return None

    data_url = f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}"
    try:
        response = client.responses.create(
            model=_media_model(),
            input=[
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": (
                                "Extract useful logistics text from this screenshot/image. Focus on names, dates, times, "
                                "locations, tasks, and deadlines. Keep it concise."
                            ),
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "input_image", "image_url": data_url},
                        {"type": "input_text", "text": "Return plain text only."},
                    ],
                },
            ],
            max_output_tokens=1_200,
        )
    except Exception:
        logger.exception("Linq image extraction failed for %s", filename or "image")
        return None
    text = _response_output_text(response)
    return _compact_text(text, max_length=2_500) if text else None


def _decode_data_url(raw_url: str) -> tuple[bytes, str | None]:
    # format: data:<mime>;base64,<payload>
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


def _download_media_bytes(
    ref: _LinqMediaRef,
    *,
    linq_api_key: str | None,
    timeout_seconds: float,
) -> tuple[bytes, str | None]:
    raw_url = ref.url.strip()
    if raw_url.startswith("data:"):
        return _decode_data_url(raw_url)

    headers: dict[str, str] | None = None
    response = httpx.get(raw_url, headers=headers, timeout=timeout_seconds)
    if response.status_code in {401, 403} and linq_api_key:
        response = httpx.get(
            raw_url,
            headers={"authorization": f"Bearer {linq_api_key}"},
            timeout=timeout_seconds,
        )
    response.raise_for_status()
    mime_type = response.headers.get("content-type")
    content = response.content[:_MAX_MEDIA_BYTES]
    return content, mime_type


def _guess_filename_from_url(url: str) -> str | None:
    path = urlparse(url).path
    if not path:
        return None
    tail = path.rsplit("/", 1)[-1].strip()
    return tail or None


def _extract_ref_from_object(obj: dict[str, Any]) -> _LinqMediaRef | None:
    url = (
        _read_string(obj.get("url"))
        or _read_string(obj.get("download_url"))
        or _read_string(obj.get("media_url"))
        or _read_string(obj.get("file_url"))
        or _read_string(obj.get("source_url"))
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
    filename = _read_string(obj.get("filename")) or _read_string(obj.get("name"))
    if filename is None:
        filename = _guess_filename_from_url(url)
    return _LinqMediaRef(url=url, mime_type=mime_type, filename=filename)


def _extract_media_refs(parts: list[Any]) -> list[_LinqMediaRef]:
    refs: list[_LinqMediaRef] = []
    seen_urls: set[str] = set()
    for part in parts:
        if not isinstance(part, dict):
            continue
        part_type = (_read_string(part.get("type")) or "").lower()
        if part_type == "text":
            continue

        candidates: list[_LinqMediaRef] = []
        direct = _extract_ref_from_object(part)
        if direct is not None:
            candidates.append(direct)

        for key in ("media", "file", "document", "attachment", "image"):
            nested = _read_object(part.get(key))
            if nested is None:
                continue
            nested_ref = _extract_ref_from_object(nested)
            if nested_ref is not None:
                if nested_ref.mime_type is None:
                    nested_ref.mime_type = direct.mime_type if direct is not None else None
                if nested_ref.filename is None:
                    nested_ref.filename = direct.filename if direct is not None else None
                candidates.append(nested_ref)

        for ref in candidates:
            key = ref.url.strip()
            if not key or key in seen_urls:
                continue
            seen_urls.add(key)
            refs.append(ref)
    return refs


def _extract_media_text(ref: _LinqMediaRef, *, content: bytes, content_type: str | None) -> str | None:
    mime_type = (ref.mime_type or content_type or "").split(";")[0].strip().lower()
    filename = (ref.filename or "").lower()

    if mime_type.startswith("text/plain") or filename.endswith(".txt"):
        text = content.decode("utf-8", errors="ignore").strip()
        return _compact_text(text, max_length=2_000) if text else None
    if "html" in mime_type or filename.endswith(".html") or filename.endswith(".htm"):
        text = _strip_html_tags(content.decode("utf-8", errors="ignore"))
        return _compact_text(text, max_length=2_000) if text else None
    if mime_type == "application/pdf" or filename.endswith(".pdf"):
        return _extract_pdf_text_with_gpt(pdf_bytes=content, filename=ref.filename)
    if mime_type.startswith("image/") or filename.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".heic", ".heif")):
        final_mime = mime_type if mime_type.startswith("image/") else "image/jpeg"
        return _extract_image_text_with_gpt(image_bytes=content, mime_type=final_mime, filename=ref.filename)
    return None


def _message_parts(payload: dict[str, Any]) -> list[Any] | None:
    data = _read_object(payload.get("data")) or payload
    parts = _read_array(data.get("parts"))
    if parts is not None:
        return parts
    message = _read_object(data.get("message")) or {}
    return _read_array(message.get("parts"))


def enrich_linq_payload_with_media_text(
    payload: dict[str, Any],
    *,
    linq_api_key: str | None = None,
    timeout_seconds: float = 20.0,
) -> bool:
    parts = _message_parts(payload)
    if not parts:
        return False

    refs = _extract_media_refs(parts)
    if not refs:
        return False

    extracted_lines: list[str] = []
    for ref in refs[:_MAX_MEDIA_PARTS]:
        try:
            content, content_type = _download_media_bytes(
                ref,
                linq_api_key=linq_api_key,
                timeout_seconds=timeout_seconds,
            )
        except Exception:
            logger.exception("Failed to download Linq media payload for %s", ref.url)
            continue

        text = _extract_media_text(ref, content=content, content_type=content_type)
        if not text:
            continue
        label = ref.filename or (ref.mime_type or content_type or "attachment").split(";")[0]
        extracted_lines.append(f"{label}: {text}")

    if not extracted_lines:
        return False

    parts.append(
        {
            "type": "text",
            "value": "Media context extracted from attachments:\n" + "\n".join(f"- {line}" for line in extracted_lines),
        }
    )
    return True

