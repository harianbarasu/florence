"""Direct OpenAI media extraction helpers for Florence ingestion."""

from __future__ import annotations

import base64
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_PDF_EXTRACTION_PROMPT = (
    "Extract plain text details from this PDF. Preserve dates, times, names, locations, fees, "
    "deadlines, required items, and logistics when present."
)

DEFAULT_IMAGE_EXTRACTION_PROMPT = (
    "Extract useful household context from this screenshot or image. "
    "If it contains text, focus on names, dates, times, locations, tasks, deadlines, and required items. "
    "For screenshots, tables, calendars, schedules, forms, or dense text images, do faithful OCR-style extraction of all visible logistics-relevant text instead of only summarizing the top few items. "
    "Preserve date ranges, row-by-row closures, holidays, dismissal notes, and other schedule details when visible. "
    "For school flyers, forms, or logistics screenshots, also call out permission slips, uniforms, materials to bring, "
    "follow-up actions, and who the update is from when visible. "
    "If it is a pantry, fridge, grocery, meal, or food photo, list the visible ingredients, groceries, or meal-relevant items. "
    "If some text is unclear, say that briefly instead of inventing details. "
    "Return plain text that helps Florence handle logistics or meal and grocery planning."
)


def compact_text(raw: str, *, max_length: int) -> str:
    normalized = " ".join(raw.split())
    if len(normalized) <= max_length:
        return normalized
    return f"{normalized[: max_length - 1].rstrip()}..."


def response_output_text(response: Any) -> str:
    output_text = getattr(response, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()
    if isinstance(response, dict):
        candidate = response.get("output_text")
        if isinstance(candidate, str):
            return candidate.strip()
    return ""


def build_pdf_data_url(pdf_bytes: bytes) -> str:
    encoded = base64.b64encode(pdf_bytes).decode("ascii")
    return f"data:application/pdf;base64,{encoded}"


def build_image_data_url(image_bytes: bytes, mime_type: str) -> str:
    encoded = base64.b64encode(image_bytes).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _openai_client(
    *,
    api_key_env_names: tuple[str, ...],
    base_url_env_name: str,
    default_base_url: str,
):
    api_key = ""
    for env_name in api_key_env_names:
        api_key = os.getenv(env_name, "").strip()
        if api_key:
            break
    if not api_key:
        return None
    base_url = os.getenv(base_url_env_name, default_base_url).strip() or default_base_url
    from openai import OpenAI

    return OpenAI(api_key=api_key, base_url=base_url)


def extract_pdf_text_with_openai(
    *,
    pdf_bytes: bytes,
    filename: str | None,
    api_key_env_names: tuple[str, ...],
    base_url_env_name: str,
    model_env_name: str,
    log_label: str,
    default_model: str = "gpt-5.4",
    default_base_url: str = "https://api.openai.com/v1",
    system_text: str = DEFAULT_PDF_EXTRACTION_PROMPT,
    max_output_tokens: int = 1_500,
    max_output_chars: int = 5_000,
) -> str | None:
    client = _openai_client(
        api_key_env_names=api_key_env_names,
        base_url_env_name=base_url_env_name,
        default_base_url=default_base_url,
    )
    if client is None:
        logger.warning("%s skipped for %s: OPENAI_API_KEY not configured", log_label, filename or "attachment.pdf")
        return None
    model = os.getenv(model_env_name, default_model).strip() or default_model
    try:
        response = client.responses.create(
            model=model,
            input=[
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": system_text}],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_file",
                            "filename": filename or "attachment.pdf",
                            "file_data": build_pdf_data_url(pdf_bytes),
                        },
                        {"type": "input_text", "text": "Return plain text only."},
                    ],
                },
            ],
            max_output_tokens=max_output_tokens,
        )
    except Exception:
        logger.exception("%s failed for %s (model=%s)", log_label, filename or "attachment.pdf", model)
        return None
    text = response_output_text(response)
    return compact_text(text, max_length=max_output_chars) if text else None


def extract_image_text_with_openai(
    *,
    image_bytes: bytes,
    mime_type: str,
    filename: str | None,
    api_key_env_names: tuple[str, ...],
    base_url_env_name: str,
    model_env_name: str,
    log_label: str,
    default_model: str = "gpt-5.4",
    default_base_url: str = "https://api.openai.com/v1",
    system_text: str = DEFAULT_IMAGE_EXTRACTION_PROMPT,
    max_output_tokens: int = 1_800,
    max_output_chars: int = 5_000,
) -> str | None:
    client = _openai_client(
        api_key_env_names=api_key_env_names,
        base_url_env_name=base_url_env_name,
        default_base_url=default_base_url,
    )
    if client is None:
        logger.warning("%s skipped for %s: OPENAI_API_KEY not configured", log_label, filename or "image")
        return None
    model = os.getenv(model_env_name, default_model).strip() or default_model
    try:
        response = client.responses.create(
            model=model,
            input=[
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": system_text}],
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "input_image", "image_url": build_image_data_url(image_bytes, mime_type)},
                        {"type": "input_text", "text": "Return plain text only."},
                    ],
                },
            ],
            max_output_tokens=max_output_tokens,
        )
    except Exception:
        logger.exception("%s failed for %s (model=%s)", log_label, filename or "image", model)
        return None
    text = response_output_text(response)
    return compact_text(text, max_length=max_output_chars) if text else None
