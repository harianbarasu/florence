"""Inbound attachment ingestion: PDFs become text, images become vision input.

Extracted text is persisted on the message row so later turns still see it;
image bytes are only held in memory for the current turn.
"""

from __future__ import annotations

import base64
import io
import logging
from typing import Any

from florence.linq import LinqClient

log = logging.getLogger("florence.attachments")

MAX_PDF_TEXT_CHARS = 8000
MAX_IMAGE_BYTES = 6 * 1024 * 1024
IMAGE_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp", "image/heic"}


async def ingest_attachments(
    attachments: list[dict[str, Any]], linq: LinqClient
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Returns (updated_attachment_dicts, image_parts_for_this_turn)."""
    updated: list[dict[str, Any]] = []
    image_parts: list[dict[str, Any]] = []
    for att in attachments:
        att = dict(att)
        if att.get("ingested"):
            updated.append(att)
            continue
        att["ingested"] = True
        url = att.get("url")
        content_type = (att.get("content_type") or "").lower().split(";")[0].strip()
        filename = att.get("filename") or att.get("kind") or "attachment"
        if not url:
            updated.append(att)
            continue
        try:
            data, fetched_type = await linq.download_media(url)
            content_type = content_type or (fetched_type or "").lower().split(";")[0].strip()
            if content_type == "application/pdf" or str(filename).lower().endswith(".pdf"):
                att["extracted_text"] = _pdf_text(data)
            elif content_type in IMAGE_TYPES or str(att.get("kind")) == "media":
                if content_type in IMAGE_TYPES and len(data) <= MAX_IMAGE_BYTES:
                    image_parts.append(
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{content_type};base64,{base64.b64encode(data).decode()}"
                            },
                        }
                    )
                    att["vision"] = True
                else:
                    att["note"] = f"unsupported or oversized media ({content_type}, {len(data)} bytes)"
            else:
                att["note"] = f"unhandled attachment type: {content_type or 'unknown'}"
        except Exception as exc:  # noqa: BLE001 - a bad attachment must not kill the turn
            log.warning("attachment ingest failed for %s: %s", filename, exc)
            att["note"] = "could not fetch attachment"
        updated.append(att)
    return updated, image_parts


def _pdf_text(data: bytes) -> str:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    chunks: list[str] = []
    total = 0
    for page in reader.pages[:25]:
        text = (page.extract_text() or "").strip()
        if not text:
            continue
        chunks.append(text)
        total += len(text)
        if total >= MAX_PDF_TEXT_CHARS:
            break
    combined = "\n".join(chunks)
    return " ".join(combined.split())[:MAX_PDF_TEXT_CHARS] or "(no extractable text — possibly a scanned image)"
