#!/usr/bin/env python3
"""
Standalone Web Tools Module

This module provides generic web tools that work with multiple backend providers.
Backend is selected during ``hermes tools`` setup (web.backend in config.yaml).

Available tools:
- web_search_tool: Search the web for information
- web_extract_tool: Extract content from specific web pages
- web_crawl_tool: Crawl websites with specific instructions (Firecrawl only)

Backend compatibility:
- Firecrawl: https://docs.firecrawl.dev/introduction (search, extract, crawl)
- Parallel: https://docs.parallel.ai (search, extract)

LLM Processing:
- Uses OpenRouter API with Gemini 3 Flash Preview for intelligent content extraction
- Extracts key excerpts and creates markdown summaries to reduce token usage

Debug Mode:
- Set WEB_TOOLS_DEBUG=true to enable detailed logging
- Creates web_tools_debug_UUID.json in ./logs directory
- Captures all tool calls, results, and compression metrics

Usage:
    from web_tools import web_search_tool, web_extract_tool, web_crawl_tool
    
    # Search the web
    results = web_search_tool("Python machine learning libraries", limit=3)
    
    # Extract content from URLs  
    content = web_extract_tool(["https://example.com"], format="markdown")
    
    # Crawl a website
    crawl_data = web_crawl_tool("example.com", "Find contact information")
"""

import base64
import json
import logging
import os
import re
import asyncio
from html import unescape
from urllib.parse import urlparse
from typing import List, Dict, Any, Optional
import httpx
from firecrawl import Firecrawl
from agent.auxiliary_client import async_call_llm, extract_content_or_reasoning
from tools.debug_helpers import DebugSession
from tools.url_safety import is_safe_url
from tools.website_policy import check_website_access

logger = logging.getLogger(__name__)

_HTTP_FETCH_HEADERS = {
    "User-Agent": "Hermes-Agent/1.0 (+https://hermes-agent.nousresearch.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml,text/plain,application/pdf;q=0.9,*/*;q=0.8",
}

_OPENAI_DEFAULT_BASE_URL = "https://api.openai.com/v1"
_ANTHROPIC_WEB_DEFAULT_MODEL = os.getenv("ANTHROPIC_WEB_MODEL", "").strip() or "claude-sonnet-4-20250514"
_OPENAI_WEB_DEFAULT_MODEL = os.getenv("OPENAI_WEB_MODEL", "").strip() or "gpt-4.1-mini"
_OPENAI_WEB_SEARCH_TOOL_TYPE = "web_search"
_ANTHROPIC_WEB_SEARCH_TOOL_TYPE = "web_search_20260209"
_ANTHROPIC_WEB_FETCH_TOOL_TYPE = "web_fetch_20260209"
_USER_LOCATION_OPENAI = {
    "type": "approximate",
    "country": "US",
    "timezone": "America/Los_Angeles",
}
_USER_LOCATION_ANTHROPIC = {
    "type": "approximate",
    "country": "US",
    "timezone": "America/Los_Angeles",
}
_SEARCH_RESULTS_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "title": {"type": "string"},
                    "url": {"type": "string"},
                    "description": {"type": "string"},
                },
                "required": ["title", "url", "description"],
            },
        }
    },
    "required": ["results"],
}
_EXTRACT_RESULT_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "title": {"type": "string"},
        "content": {"type": "string"},
    },
    "required": ["title", "content"],
}


# ─── Backend Selection ────────────────────────────────────────────────────────

def _has_env(name: str) -> bool:
    val = os.getenv(name)
    return bool(val and val.strip())

def _load_web_config() -> dict:
    """Load the ``web:`` section from ~/.hermes/config.yaml."""
    try:
        from hermes_cli.config import load_config
        return load_config().get("web", {})
    except (ImportError, Exception):
        return {}

def _get_backend() -> str:
    """Determine which web backend to use.

    Reads ``web.backend`` from config.yaml (set by ``hermes tools``).
    Falls back to whichever API key is present for users who configured
    keys manually without running setup.
    """
    configured = (_load_web_config().get("backend") or "").lower().strip()
    if configured in ("parallel", "firecrawl", "tavily", "exa", "openai", "anthropic"):
        return configured

    # Fallback for manual / legacy config — use whichever key is present.
    has_firecrawl = _has_env("FIRECRAWL_API_KEY") or _has_env("FIRECRAWL_API_URL")
    has_parallel = _has_env("PARALLEL_API_KEY")
    has_tavily = _has_env("TAVILY_API_KEY")
    has_exa = _has_env("EXA_API_KEY")
    has_openai = _has_env("OPENAI_API_KEY")
    has_anthropic = _has_anthropic_web_credentials()
    if has_exa and not has_firecrawl and not has_parallel and not has_tavily:
        return "exa"
    if has_tavily and not has_firecrawl and not has_parallel:
        return "tavily"
    if has_parallel and not has_firecrawl:
        return "parallel"
    if has_openai and not has_firecrawl and not has_parallel and not has_tavily and not has_exa and not has_anthropic:
        return "openai"
    if has_anthropic and not has_firecrawl and not has_parallel and not has_tavily and not has_exa and not has_openai:
        return "anthropic"
    if has_openai and has_anthropic and not has_firecrawl and not has_parallel and not has_tavily and not has_exa:
        return "openai"

    # Default to firecrawl (backward compat, or when both are set)
    return "firecrawl"


def _has_anthropic_web_credentials() -> bool:
    """Best-effort Anthropic credential probe for web tooling."""
    try:
        from agent.anthropic_adapter import resolve_anthropic_token
    except Exception:
        return bool(os.getenv("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_TOKEN"))
    try:
        return bool(resolve_anthropic_token())
    except Exception:
        return False

# ─── Firecrawl Client ────────────────────────────────────────────────────────

_firecrawl_client = None

def _get_firecrawl_client():
    """Get or create the Firecrawl client (lazy initialization).

    Uses the cloud API by default (requires FIRECRAWL_API_KEY).
    Set FIRECRAWL_API_URL to point at a self-hosted instance instead —
    in that case the API key is optional (set USE_DB_AUTHENTICATION=false
    on your Firecrawl server to disable auth entirely).
    """
    global _firecrawl_client
    if _firecrawl_client is None:
        api_key = os.getenv("FIRECRAWL_API_KEY")
        api_url = os.getenv("FIRECRAWL_API_URL")
        if not api_key and not api_url:
            logger.error("Firecrawl client initialization failed: missing configuration.")
            raise ValueError(
                "Firecrawl client not configured. "
                "Set FIRECRAWL_API_KEY (cloud) or FIRECRAWL_API_URL (self-hosted). "
                "This tool requires Firecrawl to be available."
            )
        kwargs = {}
        if api_key:
            kwargs["api_key"] = api_key
        if api_url:
            kwargs["api_url"] = api_url
        _firecrawl_client = Firecrawl(**kwargs)
    return _firecrawl_client

# ─── Parallel Client ─────────────────────────────────────────────────────────

_parallel_client = None
_async_parallel_client = None
_openai_web_client = None
_anthropic_web_client = None

def _get_parallel_client():
    """Get or create the Parallel sync client (lazy initialization).

    Requires PARALLEL_API_KEY environment variable.
    """
    from parallel import Parallel
    global _parallel_client
    if _parallel_client is None:
        api_key = os.getenv("PARALLEL_API_KEY")
        if not api_key:
            raise ValueError(
                "PARALLEL_API_KEY environment variable not set. "
                "Get your API key at https://parallel.ai"
            )
        _parallel_client = Parallel(api_key=api_key)
    return _parallel_client


def _get_async_parallel_client():
    """Get or create the Parallel async client (lazy initialization).

    Requires PARALLEL_API_KEY environment variable.
    """
    from parallel import AsyncParallel
    global _async_parallel_client
    if _async_parallel_client is None:
        api_key = os.getenv("PARALLEL_API_KEY")
        if not api_key:
            raise ValueError(
                "PARALLEL_API_KEY environment variable not set. "
                "Get your API key at https://parallel.ai"
            )
        _async_parallel_client = AsyncParallel(api_key=api_key)
    return _async_parallel_client


def _get_openai_web_client():
    """Get or create the OpenAI client used for native web search."""
    global _openai_web_client
    if _openai_web_client is None:
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise ValueError(
                "OPENAI_API_KEY environment variable not set. "
                "OpenAI web search requires a direct OpenAI API key."
            )
        from openai import OpenAI

        base_url = (os.getenv("OPENAI_BASE_URL") or _OPENAI_DEFAULT_BASE_URL).strip() or _OPENAI_DEFAULT_BASE_URL
        _openai_web_client = OpenAI(api_key=api_key, base_url=base_url.rstrip("/"))
    return _openai_web_client


def _get_anthropic_web_client():
    """Get or create the Anthropic client used for native web search and fetch."""
    global _anthropic_web_client
    if _anthropic_web_client is None:
        from agent.anthropic_adapter import build_anthropic_client, resolve_anthropic_token

        token = resolve_anthropic_token()
        if not token:
            raise ValueError(
                "Anthropic credentials not found. "
                "Set ANTHROPIC_API_KEY or ANTHROPIC_TOKEN, or sign in with Claude Code."
            )
        _anthropic_web_client = build_anthropic_client(token)
    return _anthropic_web_client

# ─── Tavily Client ───────────────────────────────────────────────────────────

_TAVILY_BASE_URL = "https://api.tavily.com"


def _tavily_request(endpoint: str, payload: dict) -> dict:
    """Send a POST request to the Tavily API.

    Auth is provided via ``api_key`` in the JSON body (no header-based auth).
    Raises ``ValueError`` if ``TAVILY_API_KEY`` is not set.
    """
    api_key = os.getenv("TAVILY_API_KEY")
    if not api_key:
        raise ValueError(
            "TAVILY_API_KEY environment variable not set. "
            "Get your API key at https://app.tavily.com/home"
        )
    payload["api_key"] = api_key
    url = f"{_TAVILY_BASE_URL}/{endpoint.lstrip('/')}"
    logger.info("Tavily %s request to %s", endpoint, url)
    response = httpx.post(url, json=payload, timeout=60)
    response.raise_for_status()
    return response.json()


def _normalize_tavily_search_results(response: dict) -> dict:
    """Normalize Tavily /search response to the standard web search format.

    Tavily returns ``{results: [{title, url, content, score, ...}]}``.
    We map to ``{success, data: {web: [{title, url, description, position}]}}``.
    """
    web_results = []
    for i, result in enumerate(response.get("results", [])):
        web_results.append({
            "title": result.get("title", ""),
            "url": result.get("url", ""),
            "description": result.get("content", ""),
            "position": i + 1,
        })
    return {"success": True, "data": {"web": web_results}}


def _normalize_tavily_documents(response: dict, fallback_url: str = "") -> List[Dict[str, Any]]:
    """Normalize Tavily /extract or /crawl response to the standard document format.

    Maps results to ``{url, title, content, raw_content, metadata}`` and
    includes any ``failed_results`` / ``failed_urls`` as error entries.
    """
    documents: List[Dict[str, Any]] = []
    for result in response.get("results", []):
        url = result.get("url", fallback_url)
        raw = result.get("raw_content", "") or result.get("content", "")
        documents.append({
            "url": url,
            "title": result.get("title", ""),
            "content": raw,
            "raw_content": raw,
            "metadata": {"sourceURL": url, "title": result.get("title", "")},
        })
    # Handle failed results
    for fail in response.get("failed_results", []):
        documents.append({
            "url": fail.get("url", fallback_url),
            "title": "",
            "content": "",
            "raw_content": "",
            "error": fail.get("error", "extraction failed"),
            "metadata": {"sourceURL": fail.get("url", fallback_url)},
        })
    for fail_url in response.get("failed_urls", []):
        url_str = fail_url if isinstance(fail_url, str) else str(fail_url)
        documents.append({
            "url": url_str,
            "title": "",
            "content": "",
            "raw_content": "",
            "error": "extraction failed",
            "metadata": {"sourceURL": url_str},
        })
    return documents


DEFAULT_MIN_LENGTH_FOR_SUMMARIZATION = 5000

# Allow per-task override via env var
DEFAULT_SUMMARIZER_MODEL = os.getenv("AUXILIARY_WEB_EXTRACT_MODEL", "").strip() or None

_debug = DebugSession("web_tools", env_var="WEB_TOOLS_DEBUG")


async def process_content_with_llm(
    content: str, 
    url: str = "", 
    title: str = "",
    model: str = DEFAULT_SUMMARIZER_MODEL,
    min_length: int = DEFAULT_MIN_LENGTH_FOR_SUMMARIZATION
) -> Optional[str]:
    """
    Process web content using LLM to create intelligent summaries with key excerpts.
    
    This function uses Gemini 3 Flash Preview (or specified model) via OpenRouter API 
    to intelligently extract key information and create markdown summaries,
    significantly reducing token usage while preserving all important information.
    
    For very large content (>500k chars), uses chunked processing with synthesis.
    For extremely large content (>2M chars), refuses to process entirely.
    
    Args:
        content (str): The raw content to process
        url (str): The source URL (for context, optional)
        title (str): The page title (for context, optional)
        model (str): The model to use for processing (default: google/gemini-3-flash-preview)
        min_length (int): Minimum content length to trigger processing (default: 5000)
        
    Returns:
        Optional[str]: Processed markdown content, or None if content too short or processing fails
    """
    # Size thresholds
    MAX_CONTENT_SIZE = 2_000_000  # 2M chars - refuse entirely above this
    CHUNK_THRESHOLD = 500_000     # 500k chars - use chunked processing above this
    CHUNK_SIZE = 100_000          # 100k chars per chunk
    MAX_OUTPUT_SIZE = 5000        # Hard cap on final output size
    
    try:
        content_len = len(content)
        
        # Refuse if content is absurdly large
        if content_len > MAX_CONTENT_SIZE:
            size_mb = content_len / 1_000_000
            logger.warning("Content too large (%.1fMB > 2MB limit). Refusing to process.", size_mb)
            return f"[Content too large to process: {size_mb:.1f}MB. Try using web_crawl with specific extraction instructions, or search for a more focused source.]"
        
        # Skip processing if content is too short
        if content_len < min_length:
            logger.debug("Content too short (%d < %d chars), skipping LLM processing", content_len, min_length)
            return None
        
        # Create context information
        context_info = []
        if title:
            context_info.append(f"Title: {title}")
        if url:
            context_info.append(f"Source: {url}")
        context_str = "\n".join(context_info) + "\n\n" if context_info else ""
        
        # Check if we need chunked processing
        if content_len > CHUNK_THRESHOLD:
            logger.info("Content large (%d chars). Using chunked processing...", content_len)
            return await _process_large_content_chunked(
                content, context_str, model, CHUNK_SIZE, MAX_OUTPUT_SIZE
            )
        
        # Standard single-pass processing for normal content
        logger.info("Processing content with LLM (%d characters)", content_len)
        
        processed_content = await _call_summarizer_llm(content, context_str, model)
        
        if processed_content:
            # Enforce output cap
            if len(processed_content) > MAX_OUTPUT_SIZE:
                processed_content = processed_content[:MAX_OUTPUT_SIZE] + "\n\n[... summary truncated for context management ...]"
            
            # Log compression metrics
            processed_length = len(processed_content)
            compression_ratio = processed_length / content_len if content_len > 0 else 1.0
            logger.info("Content processed: %d -> %d chars (%.1f%%)", content_len, processed_length, compression_ratio * 100)
        
        return processed_content
        
    except Exception as e:
        logger.debug("Error processing content with LLM: %s", e)
        return f"[Failed to process content: {str(e)[:100]}. Content size: {len(content):,} chars]"


async def _call_summarizer_llm(
    content: str, 
    context_str: str, 
    model: str, 
    max_tokens: int = 20000,
    is_chunk: bool = False,
    chunk_info: str = ""
) -> Optional[str]:
    """
    Make a single LLM call to summarize content.
    
    Args:
        content: The content to summarize
        context_str: Context information (title, URL)
        model: Model to use
        max_tokens: Maximum output tokens
        is_chunk: Whether this is a chunk of a larger document
        chunk_info: Information about chunk position (e.g., "Chunk 2/5")
        
    Returns:
        Summarized content or None on failure
    """
    if is_chunk:
        # Chunk-specific prompt - aware that this is partial content
        system_prompt = """You are an expert content analyst processing a SECTION of a larger document. Your job is to extract and summarize the key information from THIS SECTION ONLY.

Important guidelines for chunk processing:
1. Do NOT write introductions or conclusions - this is a partial document
2. Focus on extracting ALL key facts, figures, data points, and insights from this section
3. Preserve important quotes, code snippets, and specific details verbatim
4. Use bullet points and structured formatting for easy synthesis later
5. Note any references to other sections (e.g., "as mentioned earlier", "see below") without trying to resolve them

Your output will be combined with summaries of other sections, so focus on thorough extraction rather than narrative flow."""

        user_prompt = f"""Extract key information from this SECTION of a larger document:

{context_str}{chunk_info}

SECTION CONTENT:
{content}

Extract all important information from this section in a structured format. Focus on facts, data, insights, and key details. Do not add introductions or conclusions."""

    else:
        # Standard full-document prompt
        system_prompt = """You are an expert content analyst. Your job is to process web content and create a comprehensive yet concise summary that preserves all important information while dramatically reducing bulk.

Create a well-structured markdown summary that includes:
1. Key excerpts (quotes, code snippets, important facts) in their original format
2. Comprehensive summary of all other important information
3. Proper markdown formatting with headers, bullets, and emphasis

Your goal is to preserve ALL important information while reducing length. Never lose key facts, figures, insights, or actionable information. Make it scannable and well-organized."""

        user_prompt = f"""Please process this web content and create a comprehensive markdown summary:

{context_str}CONTENT TO PROCESS:
{content}

Create a markdown summary that captures all key information in a well-organized, scannable format. Include important quotes and code snippets in their original formatting. Focus on actionable information, specific details, and unique insights."""

    # Call the LLM with retry logic
    max_retries = 6
    retry_delay = 2
    last_error = None

    for attempt in range(max_retries):
        try:
            call_kwargs = {
                "task": "web_extract",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": 0.1,
                "max_tokens": max_tokens,
            }
            if model:
                call_kwargs["model"] = model
            response = await async_call_llm(**call_kwargs)
            content = extract_content_or_reasoning(response)
            if content:
                return content
            # Reasoning-only / empty response — let the retry loop handle it
            logger.warning("LLM returned empty content (attempt %d/%d), retrying", attempt + 1, max_retries)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
                continue
            return content  # Return whatever we got after exhausting retries
        except RuntimeError:
            logger.warning("No auxiliary model available for web content processing")
            return None
        except Exception as api_error:
            last_error = api_error
            if attempt < max_retries - 1:
                logger.warning("LLM API call failed (attempt %d/%d): %s", attempt + 1, max_retries, str(api_error)[:100])
                logger.warning("Retrying in %ds...", retry_delay)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
            else:
                raise last_error
    
    return None


async def _process_large_content_chunked(
    content: str, 
    context_str: str, 
    model: str, 
    chunk_size: int,
    max_output_size: int
) -> Optional[str]:
    """
    Process large content by chunking, summarizing each chunk in parallel,
    then synthesizing the summaries.
    
    Args:
        content: The large content to process
        context_str: Context information
        model: Model to use
        chunk_size: Size of each chunk in characters
        max_output_size: Maximum final output size
        
    Returns:
        Synthesized summary or None on failure
    """
    # Split content into chunks
    chunks = []
    for i in range(0, len(content), chunk_size):
        chunk = content[i:i + chunk_size]
        chunks.append(chunk)
    
    logger.info("Split into %d chunks of ~%d chars each", len(chunks), chunk_size)
    
    # Summarize each chunk in parallel
    async def summarize_chunk(chunk_idx: int, chunk_content: str) -> tuple[int, Optional[str]]:
        """Summarize a single chunk."""
        try:
            chunk_info = f"[Processing chunk {chunk_idx + 1} of {len(chunks)}]"
            summary = await _call_summarizer_llm(
                chunk_content, 
                context_str, 
                model, 
                max_tokens=10000,
                is_chunk=True,
                chunk_info=chunk_info
            )
            if summary:
                logger.info("Chunk %d/%d summarized: %d -> %d chars", chunk_idx + 1, len(chunks), len(chunk_content), len(summary))
            return chunk_idx, summary
        except Exception as e:
            logger.warning("Chunk %d/%d failed: %s", chunk_idx + 1, len(chunks), str(e)[:50])
            return chunk_idx, None
    
    # Run all chunk summarizations in parallel
    tasks = [summarize_chunk(i, chunk) for i, chunk in enumerate(chunks)]
    results = await asyncio.gather(*tasks)
    
    # Collect successful summaries in order
    summaries = []
    for chunk_idx, summary in sorted(results, key=lambda x: x[0]):
        if summary:
            summaries.append(f"## Section {chunk_idx + 1}\n{summary}")
    
    if not summaries:
        logger.debug("All chunk summarizations failed")
        return "[Failed to process large content: all chunk summarizations failed]"
    
    logger.info("Got %d/%d chunk summaries", len(summaries), len(chunks))
    
    # If only one chunk succeeded, just return it (with cap)
    if len(summaries) == 1:
        result = summaries[0]
        if len(result) > max_output_size:
            result = result[:max_output_size] + "\n\n[... truncated ...]"
        return result
    
    # Synthesize the summaries into a final summary
    logger.info("Synthesizing %d summaries...", len(summaries))
    
    combined_summaries = "\n\n---\n\n".join(summaries)
    
    synthesis_prompt = f"""You have been given summaries of different sections of a large document. 
Synthesize these into ONE cohesive, comprehensive summary that:
1. Removes redundancy between sections
2. Preserves all key facts, figures, and actionable information
3. Is well-organized with clear structure
4. Is under {max_output_size} characters

{context_str}SECTION SUMMARIES:
{combined_summaries}

Create a single, unified markdown summary."""

    try:
        call_kwargs = {
            "task": "web_extract",
            "messages": [
                {"role": "system", "content": "You synthesize multiple summaries into one cohesive, comprehensive summary. Be thorough but concise."},
                {"role": "user", "content": synthesis_prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 20000,
        }
        if model:
            call_kwargs["model"] = model
        response = await async_call_llm(**call_kwargs)
        final_summary = extract_content_or_reasoning(response)

        # Retry once on empty content (reasoning-only response)
        if not final_summary:
            logger.warning("Synthesis LLM returned empty content, retrying once")
            response = await async_call_llm(**call_kwargs)
            final_summary = extract_content_or_reasoning(response)

        # Enforce hard cap
        if len(final_summary) > max_output_size:
            final_summary = final_summary[:max_output_size] + "\n\n[... summary truncated for context management ...]"
        
        original_len = len(content)
        final_len = len(final_summary)
        compression = final_len / original_len if original_len > 0 else 1.0
        
        logger.info("Synthesis complete: %d -> %d chars (%.2f%%)", original_len, final_len, compression * 100)
        return final_summary
        
    except Exception as e:
        logger.warning("Synthesis failed: %s", str(e)[:100])
        # Fall back to concatenated summaries with truncation
        fallback = "\n\n".join(summaries)
        if len(fallback) > max_output_size:
            fallback = fallback[:max_output_size] + "\n\n[... truncated due to synthesis failure ...]"
        return fallback


def clean_base64_images(text: str) -> str:
    """
    Remove base64 encoded images from text to reduce token count and clutter.
    
    This function finds and removes base64 encoded images in various formats:
    - (data:image/png;base64,...)
    - (data:image/jpeg;base64,...)
    - (data:image/svg+xml;base64,...)
    - data:image/[type];base64,... (without parentheses)
    
    Args:
        text: The text content to clean
        
    Returns:
        Cleaned text with base64 images replaced with placeholders
    """
    # Pattern to match base64 encoded images wrapped in parentheses
    # Matches: (data:image/[type];base64,[base64-string])
    base64_with_parens_pattern = r'\(data:image/[^;]+;base64,[A-Za-z0-9+/=]+\)'
    
    # Pattern to match base64 encoded images without parentheses
    # Matches: data:image/[type];base64,[base64-string]
    base64_pattern = r'data:image/[^;]+;base64,[A-Za-z0-9+/=]+'
    
    # Replace parentheses-wrapped images first
    cleaned_text = re.sub(base64_with_parens_pattern, '[BASE64_IMAGE_REMOVED]', text)
    
    # Then replace any remaining non-parentheses images
    cleaned_text = re.sub(base64_pattern, '[BASE64_IMAGE_REMOVED]', cleaned_text)
    
    return cleaned_text


def _normalize_preserving_line_structure(raw: str) -> str:
    text = raw.replace("\r\n", "\n").replace("\r", "\n")
    normalized_lines: List[str] = []
    blank_pending = False
    for line in text.split("\n"):
        cleaned = " ".join(line.split())
        if not cleaned:
            if normalized_lines and not blank_pending:
                normalized_lines.append("")
            blank_pending = True
            continue
        blank_pending = False
        normalized_lines.append(cleaned)

    while normalized_lines and not normalized_lines[-1]:
        normalized_lines.pop()
    return "\n".join(normalized_lines).strip()


def _strip_html_tags(html: str) -> str:
    text = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\\1>", " ", html)
    text = re.sub(r"(?i)<br\\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|section|article|li|tr|h1|h2|h3|h4|h5|h6)>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    return _normalize_preserving_line_structure(unescape(text))


def _extract_title_from_html(html: str, fallback_url: str) -> str:
    match = re.search(r"(?is)<title[^>]*>(.*?)</title>", html)
    if match:
        title = " ".join(unescape(match.group(1)).split()).strip()
        if title:
            return title
    parsed = urlparse(fallback_url)
    if parsed.path and parsed.path != "/":
        tail = parsed.path.rstrip("/").split("/")[-1]
        if tail:
            return tail
    return fallback_url


def _content_type_is_pdf(content_type: str, url: str, filename: str) -> bool:
    lowered = (content_type or "").lower()
    return "application/pdf" in lowered or url.lower().endswith(".pdf") or filename.lower().endswith(".pdf")


def _derive_filename(url: str, content_disposition: str = "") -> str:
    match = re.search(r'filename\\*?=(?:UTF-8\'\')?"?([^";]+)"?', content_disposition or "", flags=re.IGNORECASE)
    if match:
        return match.group(1).strip().strip('"')
    parsed = urlparse(url)
    tail = parsed.path.rstrip("/").split("/")[-1]
    return tail or "document"


def _parse_model_json(text: Any) -> Optional[dict]:
    if not isinstance(text, str):
        return None
    candidate = text.strip()
    if not candidate:
        return None
    if candidate.startswith("```"):
        candidate = re.sub(r"^```(?:json)?\\s*", "", candidate, count=1)
        candidate = re.sub(r"\\s*```$", "", candidate, count=1)
    try:
        parsed = json.loads(candidate)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _normalize_search_results_payload(payload: Optional[dict], *, limit: int) -> dict:
    raw_results = payload.get("results") if isinstance(payload, dict) else []
    if not isinstance(raw_results, list):
        raw_results = []
    web_results = []
    for item in raw_results:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        title = str(item.get("title") or "").strip() or url
        description = str(item.get("description") or "").strip()
        web_results.append({
            "url": url,
            "title": title,
            "description": description,
            "position": len(web_results) + 1,
        })
        if len(web_results) >= limit:
            break
    return {"success": True, "data": {"web": web_results}}


def _normalize_extract_result_payload(payload: Optional[dict], *, fallback_title: str = "") -> dict:
    title = fallback_title
    content = ""
    if isinstance(payload, dict):
        title = str(payload.get("title") or fallback_title).strip() or fallback_title
        content = str(payload.get("content") or "").strip()
    return {"title": title, "content": content}


def _openai_search_result_fallback(response: Any, limit: int) -> dict:
    web_results = []
    seen_urls = set()
    output_items = getattr(response, "output", None) or []
    for item in output_items:
        if getattr(item, "type", None) != "message":
            continue
        for part in getattr(item, "content", []) or []:
            if getattr(part, "type", None) != "output_text":
                continue
            text = getattr(part, "text", "") or ""
            for annotation in getattr(part, "annotations", []) or []:
                if getattr(annotation, "type", None) != "url_citation":
                    continue
                url = str(getattr(annotation, "url", "") or "").strip()
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)
                start = max(0, int(getattr(annotation, "start_index", 0) or 0) - 80)
                end = min(len(text), int(getattr(annotation, "end_index", 0) or 0) + 220)
                snippet = " ".join(text[start:end].split())
                web_results.append({
                    "url": url,
                    "title": str(getattr(annotation, "title", "") or url).strip() or url,
                    "description": snippet,
                    "position": len(web_results) + 1,
                })
                if len(web_results) >= limit:
                    return {"success": True, "data": {"web": web_results}}
    return {"success": True, "data": {"web": web_results}}


def _anthropic_search_result_fallback(response: Any, limit: int) -> dict:
    web_results = []
    seen_urls = set()
    for block in getattr(response, "content", []) or []:
        if getattr(block, "type", None) != "web_search_tool_result":
            continue
        entries = getattr(block, "content", None)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            url = str(getattr(entry, "url", "") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            web_results.append({
                "url": url,
                "title": str(getattr(entry, "title", "") or url).strip() or url,
                "description": "",
                "position": len(web_results) + 1,
            })
            if len(web_results) >= limit:
                return {"success": True, "data": {"web": web_results}}
    return {"success": True, "data": {"web": web_results}}


def _extract_anthropic_text(response: Any) -> str:
    parts: List[str] = []
    for block in getattr(response, "content", []) or []:
        if getattr(block, "type", None) == "text" and getattr(block, "text", None):
            parts.append(block.text)
    return "\n".join(parts).strip()


def _openai_search(query: str, limit: int = 5) -> dict:
    from tools.interrupt import is_interrupted

    if is_interrupted():
        return {"error": "Interrupted", "success": False}

    logger.info("OpenAI web search: '%s' (limit=%d)", query, limit)
    prompt = (
        f"Search the public web for the user's query and return up to {limit} strong results. "
        "Descriptions should be brief, factual, and grounded in the source."
    )
    response = _get_openai_web_client().responses.create(
        model=_OPENAI_WEB_DEFAULT_MODEL,
        instructions=prompt,
        input=query,
        max_output_tokens=1200,
        include=["web_search_call.action.sources"],
        text={
            "format": {
                "type": "json_schema",
                "name": "web_search_results",
                "strict": True,
                "description": "Structured web search results.",
                "schema": _SEARCH_RESULTS_JSON_SCHEMA,
            },
            "verbosity": "low",
        },
        tools=[
            {
                "type": _OPENAI_WEB_SEARCH_TOOL_TYPE,
                "search_context_size": "medium",
                "user_location": _USER_LOCATION_OPENAI,
            }
        ],
    )
    parsed = _parse_model_json(getattr(response, "output_text", ""))
    normalized = _normalize_search_results_payload(parsed, limit=limit)
    if normalized["data"]["web"]:
        return normalized
    return _openai_search_result_fallback(response, limit)


def _anthropic_search(query: str, limit: int = 5) -> dict:
    from tools.interrupt import is_interrupted

    if is_interrupted():
        return {"error": "Interrupted", "success": False}

    logger.info("Anthropic web search: '%s' (limit=%d)", query, limit)
    response = _get_anthropic_web_client().beta.messages.create(
        model=_ANTHROPIC_WEB_DEFAULT_MODEL,
        max_tokens=1200,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Search the public web for: {query}\n\n"
                    f"Return up to {limit} useful results with title, url, and a short grounded description."
                ),
            }
        ],
        output_config={
            "format": {
                "type": "json_schema",
                "schema": _SEARCH_RESULTS_JSON_SCHEMA,
            }
        },
        tools=[
            {
                "name": "web_search",
                "type": _ANTHROPIC_WEB_SEARCH_TOOL_TYPE,
                "max_uses": 1,
                "user_location": _USER_LOCATION_ANTHROPIC,
            }
        ],
    )
    parsed = _parse_model_json(_extract_anthropic_text(response))
    normalized = _normalize_search_results_payload(parsed, limit=limit)
    if normalized["data"]["web"]:
        return normalized
    return _anthropic_search_result_fallback(response, limit)


def _openai_extract_pdf_text(pdf_bytes: bytes, filename: str, url: str) -> str:
    response = _get_openai_web_client().responses.create(
        model=_OPENAI_WEB_DEFAULT_MODEL,
        input=[
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "Extract readable plain text from this PDF. Preserve headings, lists, dates, times, "
                            "names, places, fees, deadlines, and logistics-relevant details. Return plain text only."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_file",
                        "filename": filename or "document.pdf",
                        "file_data": "data:application/pdf;base64," + base64.b64encode(pdf_bytes).decode("ascii"),
                    },
                    {"type": "input_text", "text": f"Source URL: {url}"},
                ],
            },
        ],
        max_output_tokens=4000,
    )
    return _normalize_preserving_line_structure(getattr(response, "output_text", "") or "")


async def _direct_http_extract(urls: List[str]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    from tools.interrupt import is_interrupted

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers=_HTTP_FETCH_HEADERS,
        timeout=httpx.Timeout(timeout=60.0, connect=10.0),
    ) as client:
        for url in urls:
            if is_interrupted():
                results.append({"url": url, "error": "Interrupted", "title": ""})
                continue

            blocked = check_website_access(url)
            if blocked:
                logger.info("Blocked web_extract for %s by rule %s", blocked["host"], blocked["rule"])
                results.append({
                    "url": url,
                    "title": "",
                    "content": "",
                    "raw_content": "",
                    "error": blocked["message"],
                    "blocked_by_policy": {"host": blocked["host"], "rule": blocked["rule"], "source": blocked["source"]},
                })
                continue

            try:
                response = await client.get(url)
                response.raise_for_status()
                final_url = str(response.url)
                final_blocked = check_website_access(final_url)
                if final_blocked:
                    logger.info("Blocked redirected web_extract for %s by rule %s", final_blocked["host"], final_blocked["rule"])
                    results.append({
                        "url": final_url,
                        "title": "",
                        "content": "",
                        "raw_content": "",
                        "error": final_blocked["message"],
                        "blocked_by_policy": {"host": final_blocked["host"], "rule": final_blocked["rule"], "source": final_blocked["source"]},
                    })
                    continue

                content_type = response.headers.get("content-type", "")
                filename = _derive_filename(final_url, response.headers.get("content-disposition", ""))
                if _content_type_is_pdf(content_type, final_url, filename):
                    content = _openai_extract_pdf_text(response.content, filename, final_url)
                    title = filename
                else:
                    body = response.text
                    title = _extract_title_from_html(body, final_url)
                    if "html" in content_type.lower():
                        content = _strip_html_tags(body)
                    else:
                        content = _normalize_preserving_line_structure(unescape(body))

                results.append({
                    "url": final_url,
                    "title": title,
                    "content": content,
                    "raw_content": content,
                    "metadata": {"sourceURL": final_url, "title": title},
                })
            except Exception as exc:
                logger.debug("Direct fetch failed for %s: %s", url, exc)
                results.append({
                    "url": url,
                    "title": "",
                    "content": "",
                    "raw_content": "",
                    "error": str(exc),
                })
    return results


async def _anthropic_extract(urls: List[str]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    from tools.interrupt import is_interrupted

    client = _get_anthropic_web_client()
    for url in urls:
        if is_interrupted():
            results.append({"url": url, "error": "Interrupted", "title": ""})
            continue
        try:
            response = client.beta.messages.create(
                model=_ANTHROPIC_WEB_DEFAULT_MODEL,
                max_tokens=4000,
                messages=[
                    {
                        "role": "user",
                        "content": (
                            f"Fetch this URL and extract the readable page content: {url}\n\n"
                            "Return JSON with fields {title, content}. "
                            "The content should preserve headings, lists, dates, names, logistics-relevant details, "
                            "and other key text from the page. Do not add commentary outside the JSON."
                        ),
                    }
                ],
                output_config={
                    "format": {
                        "type": "json_schema",
                        "schema": _EXTRACT_RESULT_JSON_SCHEMA,
                    }
                },
                tools=[
                    {
                        "name": "web_fetch",
                        "type": _ANTHROPIC_WEB_FETCH_TOOL_TYPE,
                        "max_uses": 1,
                        "max_content_tokens": 12000,
                    }
                ],
            )
            parsed = _parse_model_json(_extract_anthropic_text(response))
            extracted = _normalize_extract_result_payload(parsed)
            title = extracted["title"]
            content = extracted["content"]
            if not content:
                raw_title = title
                for block in getattr(response, "content", []) or []:
                    if getattr(block, "type", None) != "web_fetch_tool_result":
                        continue
                    payload = getattr(block, "content", None)
                    if getattr(payload, "type", None) != "web_fetch_result":
                        continue
                    doc = getattr(payload, "content", None)
                    raw_title = str(getattr(doc, "title", "") or raw_title).strip() or raw_title
                    source = getattr(doc, "source", None)
                    if getattr(source, "type", None) == "text":
                        content = _normalize_preserving_line_structure(str(getattr(source, "data", "") or ""))
                        break
                title = raw_title
            if not content:
                content = _extract_anthropic_text(response)
            title = title or url
            results.append({
                "url": url,
                "title": title,
                "content": content,
                "raw_content": content,
                "metadata": {"sourceURL": url, "title": title},
            })
        except Exception as exc:
            logger.debug("Anthropic fetch failed for %s: %s", url, exc)
            results.append({
                "url": url,
                "title": "",
                "content": "",
                "raw_content": "",
                "error": str(exc),
            })
    return results


# ─── Exa Client ──────────────────────────────────────────────────────────────

_exa_client = None

def _get_exa_client():
    """Get or create the Exa client (lazy initialization).

    Requires EXA_API_KEY environment variable.
    """
    from exa_py import Exa
    global _exa_client
    if _exa_client is None:
        api_key = os.getenv("EXA_API_KEY")
        if not api_key:
            raise ValueError(
                "EXA_API_KEY environment variable not set. "
                "Get your API key at https://exa.ai"
            )
        _exa_client = Exa(api_key=api_key)
        _exa_client.headers["x-exa-integration"] = "hermes-agent"
    return _exa_client


# ─── Exa Search & Extract Helpers ─────────────────────────────────────────────

def _exa_search(query: str, limit: int = 10) -> dict:
    """Search using the Exa SDK and return results as a dict."""
    from tools.interrupt import is_interrupted
    if is_interrupted():
        return {"error": "Interrupted", "success": False}

    logger.info("Exa search: '%s' (limit=%d)", query, limit)
    response = _get_exa_client().search(
        query,
        num_results=limit,
        contents={
            "highlights": True,
        },
    )

    web_results = []
    for i, result in enumerate(response.results or []):
        highlights = result.highlights or []
        web_results.append({
            "url": result.url or "",
            "title": result.title or "",
            "description": " ".join(highlights) if highlights else "",
            "position": i + 1,
        })

    return {"success": True, "data": {"web": web_results}}


def _exa_extract(urls: List[str]) -> List[Dict[str, Any]]:
    """Extract content from URLs using the Exa SDK.

    Returns a list of result dicts matching the structure expected by the
    LLM post-processing pipeline (url, title, content, metadata).
    """
    from tools.interrupt import is_interrupted
    if is_interrupted():
        return [{"url": u, "error": "Interrupted", "title": ""} for u in urls]

    logger.info("Exa extract: %d URL(s)", len(urls))
    response = _get_exa_client().get_contents(
        urls,
        text=True,
    )

    results = []
    for result in response.results or []:
        content = result.text or ""
        url = result.url or ""
        title = result.title or ""
        results.append({
            "url": url,
            "title": title,
            "content": content,
            "raw_content": content,
            "metadata": {"sourceURL": url, "title": title},
        })

    return results


# ─── Parallel Search & Extract Helpers ────────────────────────────────────────

def _parallel_search(query: str, limit: int = 5) -> dict:
    """Search using the Parallel SDK and return results as a dict."""
    from tools.interrupt import is_interrupted
    if is_interrupted():
        return {"error": "Interrupted", "success": False}

    mode = os.getenv("PARALLEL_SEARCH_MODE", "agentic").lower().strip()
    if mode not in ("fast", "one-shot", "agentic"):
        mode = "agentic"

    logger.info("Parallel search: '%s' (mode=%s, limit=%d)", query, mode, limit)
    response = _get_parallel_client().beta.search(
        search_queries=[query],
        objective=query,
        mode=mode,
        max_results=min(limit, 20),
    )

    web_results = []
    for i, result in enumerate(response.results or []):
        excerpts = result.excerpts or []
        web_results.append({
            "url": result.url or "",
            "title": result.title or "",
            "description": " ".join(excerpts) if excerpts else "",
            "position": i + 1,
        })

    return {"success": True, "data": {"web": web_results}}


async def _parallel_extract(urls: List[str]) -> List[Dict[str, Any]]:
    """Extract content from URLs using the Parallel async SDK.

    Returns a list of result dicts matching the structure expected by the
    LLM post-processing pipeline (url, title, content, metadata).
    """
    from tools.interrupt import is_interrupted
    if is_interrupted():
        return [{"url": u, "error": "Interrupted", "title": ""} for u in urls]

    logger.info("Parallel extract: %d URL(s)", len(urls))
    response = await _get_async_parallel_client().beta.extract(
        urls=urls,
        full_content=True,
    )

    results = []
    for result in response.results or []:
        content = result.full_content or ""
        if not content:
            content = "\n\n".join(result.excerpts or [])
        url = result.url or ""
        title = result.title or ""
        results.append({
            "url": url,
            "title": title,
            "content": content,
            "raw_content": content,
            "metadata": {"sourceURL": url, "title": title},
        })

    for error in response.errors or []:
        results.append({
            "url": error.url or "",
            "title": "",
            "content": "",
            "error": error.content or error.error_type or "extraction failed",
            "metadata": {"sourceURL": error.url or ""},
        })

    return results


def web_search_tool(query: str, limit: int = 5) -> str:
    """
    Search the web for information using available search API backend.

    This function provides a generic interface for web search that can work
    with multiple backends (Parallel or Firecrawl).

    Note: This function returns search result metadata only (URLs, titles, descriptions).
    Use web_extract_tool to get full content from specific URLs.
    
    Args:
        query (str): The search query to look up
        limit (int): Maximum number of results to return (default: 5)
    
    Returns:
        str: JSON string containing search results with the following structure:
             {
                 "success": bool,
                 "data": {
                     "web": [
                         {
                             "title": str,
                             "url": str,
                             "description": str,
                             "position": int
                         },
                         ...
                     ]
                 }
             }
    
    Raises:
        Exception: If search fails or API key is not set
    """
    debug_call_data = {
        "parameters": {
            "query": query,
            "limit": limit
        },
        "error": None,
        "results_count": 0,
        "original_response_size": 0,
        "final_response_size": 0
    }
    
    try:
        from tools.interrupt import is_interrupted
        if is_interrupted():
            return json.dumps({"error": "Interrupted", "success": False})

        # Dispatch to the configured backend
        backend = _get_backend()
        if backend == "parallel":
            response_data = _parallel_search(query, limit)
            debug_call_data["results_count"] = len(response_data.get("data", {}).get("web", []))
            result_json = json.dumps(response_data, indent=2, ensure_ascii=False)
            debug_call_data["final_response_size"] = len(result_json)
            _debug.log_call("web_search_tool", debug_call_data)
            _debug.save()
            return result_json

        if backend == "exa":
            response_data = _exa_search(query, limit)
            debug_call_data["results_count"] = len(response_data.get("data", {}).get("web", []))
            result_json = json.dumps(response_data, indent=2, ensure_ascii=False)
            debug_call_data["final_response_size"] = len(result_json)
            _debug.log_call("web_search_tool", debug_call_data)
            _debug.save()
            return result_json

        if backend == "openai":
            response_data = _openai_search(query, limit)
            debug_call_data["results_count"] = len(response_data.get("data", {}).get("web", []))
            result_json = json.dumps(response_data, indent=2, ensure_ascii=False)
            debug_call_data["final_response_size"] = len(result_json)
            _debug.log_call("web_search_tool", debug_call_data)
            _debug.save()
            return result_json

        if backend == "anthropic":
            response_data = _anthropic_search(query, limit)
            debug_call_data["results_count"] = len(response_data.get("data", {}).get("web", []))
            result_json = json.dumps(response_data, indent=2, ensure_ascii=False)
            debug_call_data["final_response_size"] = len(result_json)
            _debug.log_call("web_search_tool", debug_call_data)
            _debug.save()
            return result_json

        if backend == "tavily":
            logger.info("Tavily search: '%s' (limit: %d)", query, limit)
            raw = _tavily_request("search", {
                "query": query,
                "max_results": min(limit, 20),
                "include_raw_content": False,
                "include_images": False,
            })
            response_data = _normalize_tavily_search_results(raw)
            debug_call_data["results_count"] = len(response_data.get("data", {}).get("web", []))
            result_json = json.dumps(response_data, indent=2, ensure_ascii=False)
            debug_call_data["final_response_size"] = len(result_json)
            _debug.log_call("web_search_tool", debug_call_data)
            _debug.save()
            return result_json

        logger.info("Searching the web for: '%s' (limit: %d)", query, limit)

        response = _get_firecrawl_client().search(
            query=query,
            limit=limit
        )

        # The response is a SearchData object with web, news, and images attributes
        # When not scraping, the results are directly in these attributes
        web_results = []

        # Check if response has web attribute (SearchData object)
        if hasattr(response, 'web'):
            # Response is a SearchData object with web attribute
            if response.web:
                # Convert each SearchResultWeb object to dict
                for result in response.web:
                    if hasattr(result, 'model_dump'):
                        # Pydantic model - use model_dump
                        web_results.append(result.model_dump())
                    elif hasattr(result, '__dict__'):
                        # Regular object - use __dict__
                        web_results.append(result.__dict__)
                    elif isinstance(result, dict):
                        # Already a dict
                        web_results.append(result)
        elif hasattr(response, 'model_dump'):
            # Response has model_dump method - use it to get dict
            response_dict = response.model_dump()
            if 'web' in response_dict and response_dict['web']:
                web_results = response_dict['web']
        elif isinstance(response, dict):
            # Response is already a dictionary
            if 'web' in response and response['web']:
                web_results = response['web']
        
        results_count = len(web_results)
        logger.info("Found %d search results", results_count)
        
        # Build response with just search metadata (URLs, titles, descriptions)
        response_data = {
            "success": True,
            "data": {
                "web": web_results
            }
        }
        
        # Capture debug information
        debug_call_data["results_count"] = results_count
        
        # Convert to JSON
        result_json = json.dumps(response_data, indent=2, ensure_ascii=False)
        
        debug_call_data["final_response_size"] = len(result_json)
        
        # Log debug information
        _debug.log_call("web_search_tool", debug_call_data)
        _debug.save()
        
        return result_json
        
    except Exception as e:
        error_msg = f"Error searching web: {str(e)}"
        logger.debug("%s", error_msg)
        
        debug_call_data["error"] = error_msg
        _debug.log_call("web_search_tool", debug_call_data)
        _debug.save()
        
        return json.dumps({"error": error_msg}, ensure_ascii=False)


async def web_extract_tool(
    urls: List[str], 
    format: str = None, 
    use_llm_processing: bool = True,
    model: str = DEFAULT_SUMMARIZER_MODEL,
    min_length: int = DEFAULT_MIN_LENGTH_FOR_SUMMARIZATION
) -> str:
    """
    Extract content from specific web pages using available extraction API backend.
    
    This function provides a generic interface for web content extraction that
    can work with multiple backends. Currently uses Firecrawl.
    
    Args:
        urls (List[str]): List of URLs to extract content from
        format (str): Desired output format ("markdown" or "html", optional)
        use_llm_processing (bool): Whether to process content with LLM for summarization (default: True)
        model (str): The model to use for LLM processing (default: google/gemini-3-flash-preview)
        min_length (int): Minimum content length to trigger LLM processing (default: 5000)
    
    Returns:
        str: JSON string containing extracted content. If LLM processing is enabled and successful,
             the 'content' field will contain the processed markdown summary instead of raw content.
    
    Raises:
        Exception: If extraction fails or API key is not set
    """
    debug_call_data = {
        "parameters": {
            "urls": urls,
            "format": format,
            "use_llm_processing": use_llm_processing,
            "model": model,
            "min_length": min_length
        },
        "error": None,
        "pages_extracted": 0,
        "pages_processed_with_llm": 0,
        "original_response_size": 0,
        "final_response_size": 0,
        "compression_metrics": [],
        "processing_applied": []
    }
    
    try:
        logger.info("Extracting content from %d URL(s)", len(urls))

        # ── SSRF protection — filter out private/internal URLs before any backend ──
        safe_urls = []
        ssrf_blocked: List[Dict[str, Any]] = []
        for url in urls:
            if not is_safe_url(url):
                ssrf_blocked.append({
                    "url": url, "title": "", "content": "",
                    "error": "Blocked: URL targets a private or internal network address",
                })
            else:
                safe_urls.append(url)

        # Dispatch only safe URLs to the configured backend
        if not safe_urls:
            results = []
        else:
            backend = _get_backend()

            if backend == "parallel":
                results = await _parallel_extract(safe_urls)
            elif backend == "exa":
                results = _exa_extract(safe_urls)
            elif backend == "openai":
                results = await _direct_http_extract(safe_urls)
            elif backend == "anthropic":
                results = await _anthropic_extract(safe_urls)
            elif backend == "tavily":
                logger.info("Tavily extract: %d URL(s)", len(safe_urls))
                raw = _tavily_request("extract", {
                    "urls": safe_urls,
                    "include_images": False,
                })
                results = _normalize_tavily_documents(raw, fallback_url=safe_urls[0] if safe_urls else "")
            else:
                # ── Firecrawl extraction ──
                # Determine requested formats for Firecrawl v2
                formats: List[str] = []
                if format == "markdown":
                    formats = ["markdown"]
                elif format == "html":
                    formats = ["html"]
                else:
                    # Default: request markdown for LLM-readiness and include html as backup
                    formats = ["markdown", "html"]

                # Always use individual scraping for simplicity and reliability
                # Batch scraping adds complexity without much benefit for small numbers of URLs
                results: List[Dict[str, Any]] = []

                from tools.interrupt import is_interrupted as _is_interrupted
                for url in safe_urls:
                    if _is_interrupted():
                        results.append({"url": url, "error": "Interrupted", "title": ""})
                        continue

                    # Website policy check — block before fetching
                    blocked = check_website_access(url)
                    if blocked:
                        logger.info("Blocked web_extract for %s by rule %s", blocked["host"], blocked["rule"])
                        results.append({
                            "url": url, "title": "", "content": "",
                            "error": blocked["message"],
                            "blocked_by_policy": {"host": blocked["host"], "rule": blocked["rule"], "source": blocked["source"]},
                        })
                        continue

                    try:
                        logger.info("Scraping: %s", url)
                        scrape_result = _get_firecrawl_client().scrape(
                            url=url,
                            formats=formats
                        )

                        # Process the result - properly handle object serialization
                        metadata = {}
                        title = ""
                        content_markdown = None
                        content_html = None

                        # Extract data from the scrape result
                        if hasattr(scrape_result, 'model_dump'):
                            # Pydantic model - use model_dump to get dict
                            result_dict = scrape_result.model_dump()
                            content_markdown = result_dict.get('markdown')
                            content_html = result_dict.get('html')
                            metadata = result_dict.get('metadata', {})
                        elif hasattr(scrape_result, '__dict__'):
                            # Regular object with attributes
                            content_markdown = getattr(scrape_result, 'markdown', None)
                            content_html = getattr(scrape_result, 'html', None)

                            # Handle metadata - convert to dict if it's an object
                            metadata_obj = getattr(scrape_result, 'metadata', {})
                            if hasattr(metadata_obj, 'model_dump'):
                                metadata = metadata_obj.model_dump()
                            elif hasattr(metadata_obj, '__dict__'):
                                metadata = metadata_obj.__dict__
                            elif isinstance(metadata_obj, dict):
                                metadata = metadata_obj
                            else:
                                metadata = {}
                        elif isinstance(scrape_result, dict):
                            # Already a dictionary
                            content_markdown = scrape_result.get('markdown')
                            content_html = scrape_result.get('html')
                            metadata = scrape_result.get('metadata', {})

                        # Ensure metadata is a dict (not an object)
                        if not isinstance(metadata, dict):
                            if hasattr(metadata, 'model_dump'):
                                metadata = metadata.model_dump()
                            elif hasattr(metadata, '__dict__'):
                                metadata = metadata.__dict__
                            else:
                                metadata = {}

                        # Get title from metadata
                        title = metadata.get("title", "")

                        # Re-check final URL after redirect
                        final_url = metadata.get("sourceURL", url)
                        final_blocked = check_website_access(final_url)
                        if final_blocked:
                            logger.info("Blocked redirected web_extract for %s by rule %s", final_blocked["host"], final_blocked["rule"])
                            results.append({
                                "url": final_url, "title": title, "content": "", "raw_content": "",
                                "error": final_blocked["message"],
                                "blocked_by_policy": {"host": final_blocked["host"], "rule": final_blocked["rule"], "source": final_blocked["source"]},
                            })
                            continue

                        # Choose content based on requested format
                        chosen_content = content_markdown if (format == "markdown" or (format is None and content_markdown)) else content_html or content_markdown or ""

                        results.append({
                            "url": final_url,
                            "title": title,
                            "content": chosen_content,
                            "raw_content": chosen_content,
                            "metadata": metadata  # Now guaranteed to be a dict
                        })

                    except Exception as scrape_err:
                        logger.debug("Scrape failed for %s: %s", url, scrape_err)
                        results.append({
                            "url": url,
                            "title": "",
                            "content": "",
                            "raw_content": "",
                            "error": str(scrape_err)
                        })

        # Merge any SSRF-blocked results back in
        if ssrf_blocked:
            results = ssrf_blocked + results

        response = {"results": results}
        
        pages_extracted = len(response.get('results', []))
        logger.info("Extracted content from %d pages", pages_extracted)
        
        debug_call_data["pages_extracted"] = pages_extracted
        debug_call_data["original_response_size"] = len(json.dumps(response))
        
        # Process each result with LLM if enabled
        if use_llm_processing:
            logger.info("Processing extracted content with LLM (parallel)...")
            debug_call_data["processing_applied"].append("llm_processing")
            
            # Prepare tasks for parallel processing
            async def process_single_result(result):
                """Process a single result with LLM and return updated result with metrics."""
                url = result.get('url', 'Unknown URL')
                title = result.get('title', '')
                raw_content = result.get('raw_content', '') or result.get('content', '')
                
                if not raw_content:
                    return result, None, "no_content"
                
                original_size = len(raw_content)
                
                # Process content with LLM
                processed = await process_content_with_llm(
                    raw_content, url, title, model, min_length
                )
                
                if processed:
                    processed_size = len(processed)
                    compression_ratio = processed_size / original_size if original_size > 0 else 1.0
                    
                    # Update result with processed content
                    result['content'] = processed
                    result['raw_content'] = raw_content
                    
                    metrics = {
                        "url": url,
                        "original_size": original_size,
                        "processed_size": processed_size,
                        "compression_ratio": compression_ratio,
                        "model_used": model
                    }
                    return result, metrics, "processed"
                else:
                    metrics = {
                        "url": url,
                        "original_size": original_size,
                        "processed_size": original_size,
                        "compression_ratio": 1.0,
                        "model_used": None,
                        "reason": "content_too_short"
                    }
                    return result, metrics, "too_short"
            
            # Run all LLM processing in parallel
            results_list = response.get('results', [])
            tasks = [process_single_result(result) for result in results_list]
            processed_results = await asyncio.gather(*tasks)
            
            # Collect metrics and print results
            for result, metrics, status in processed_results:
                url = result.get('url', 'Unknown URL')
                if status == "processed":
                    debug_call_data["compression_metrics"].append(metrics)
                    debug_call_data["pages_processed_with_llm"] += 1
                    logger.info("%s (processed)", url)
                elif status == "too_short":
                    debug_call_data["compression_metrics"].append(metrics)
                    logger.info("%s (no processing - content too short)", url)
                else:
                    logger.warning("%s (no content to process)", url)
        else:
            # Print summary of extracted pages for debugging (original behavior)
            for result in response.get('results', []):
                url = result.get('url', 'Unknown URL')
                content_length = len(result.get('raw_content', ''))
                logger.info("%s (%d characters)", url, content_length)
        
        # Trim output to minimal fields per entry: title, content, error
        trimmed_results = [
            {
                "url": r.get("url", ""),
                "title": r.get("title", ""),
                "content": r.get("content", ""),
                "error": r.get("error"),
                **({  "blocked_by_policy": r["blocked_by_policy"]} if "blocked_by_policy" in r else {}),
            }
            for r in response.get("results", [])
        ]
        trimmed_response = {"results": trimmed_results}

        if trimmed_response.get("results") == []:
            result_json = json.dumps({"error": "Content was inaccessible or not found"}, ensure_ascii=False)

            cleaned_result = clean_base64_images(result_json)
        
        else:
            result_json = json.dumps(trimmed_response, indent=2, ensure_ascii=False)
            
            cleaned_result = clean_base64_images(result_json)
        
        debug_call_data["final_response_size"] = len(cleaned_result)
        debug_call_data["processing_applied"].append("base64_image_removal")
        
        # Log debug information
        _debug.log_call("web_extract_tool", debug_call_data)
        _debug.save()
        
        return cleaned_result
            
    except Exception as e:
        error_msg = f"Error extracting content: {str(e)}"
        logger.debug("%s", error_msg)
        
        debug_call_data["error"] = error_msg
        _debug.log_call("web_extract_tool", debug_call_data)
        _debug.save()
        
        return json.dumps({"error": error_msg}, ensure_ascii=False)


async def web_crawl_tool(
    url: str, 
    instructions: str = None, 
    depth: str = "basic", 
    use_llm_processing: bool = True,
    model: str = DEFAULT_SUMMARIZER_MODEL,
    min_length: int = DEFAULT_MIN_LENGTH_FOR_SUMMARIZATION
) -> str:
    """
    Crawl a website with specific instructions using available crawling API backend.
    
    This function provides a generic interface for web crawling that can work
    with multiple backends. Currently uses Firecrawl.
    
    Args:
        url (str): The base URL to crawl (can include or exclude https://)
        instructions (str): Instructions for what to crawl/extract using LLM intelligence (optional)
        depth (str): Depth of extraction ("basic" or "advanced", default: "basic")
        use_llm_processing (bool): Whether to process content with LLM for summarization (default: True)
        model (str): The model to use for LLM processing (default: google/gemini-3-flash-preview)
        min_length (int): Minimum content length to trigger LLM processing (default: 5000)
    
    Returns:
        str: JSON string containing crawled content. If LLM processing is enabled and successful,
             the 'content' field will contain the processed markdown summary instead of raw content.
             Each page is processed individually.
    
    Raises:
        Exception: If crawling fails or API key is not set
    """
    debug_call_data = {
        "parameters": {
            "url": url,
            "instructions": instructions,
            "depth": depth,
            "use_llm_processing": use_llm_processing,
            "model": model,
            "min_length": min_length
        },
        "error": None,
        "pages_crawled": 0,
        "pages_processed_with_llm": 0,
        "original_response_size": 0,
        "final_response_size": 0,
        "compression_metrics": [],
        "processing_applied": []
    }
    
    try:
        backend = _get_backend()

        # Tavily supports crawl via its /crawl endpoint
        if backend == "tavily":
            # Ensure URL has protocol
            if not url.startswith(('http://', 'https://')):
                url = f'https://{url}'

            # SSRF protection — block private/internal addresses
            if not is_safe_url(url):
                return json.dumps({"results": [{"url": url, "title": "", "content": "",
                    "error": "Blocked: URL targets a private or internal network address"}]}, ensure_ascii=False)

            # Website policy check
            blocked = check_website_access(url)
            if blocked:
                logger.info("Blocked web_crawl for %s by rule %s", blocked["host"], blocked["rule"])
                return json.dumps({"results": [{"url": url, "title": "", "content": "", "error": blocked["message"],
                    "blocked_by_policy": {"host": blocked["host"], "rule": blocked["rule"], "source": blocked["source"]}}]}, ensure_ascii=False)

            from tools.interrupt import is_interrupted as _is_int
            if _is_int():
                return json.dumps({"error": "Interrupted", "success": False})

            logger.info("Tavily crawl: %s", url)
            payload: Dict[str, Any] = {
                "url": url,
                "limit": 20,
                "extract_depth": depth,
            }
            if instructions:
                payload["instructions"] = instructions
            raw = _tavily_request("crawl", payload)
            results = _normalize_tavily_documents(raw, fallback_url=url)

            response = {"results": results}
            # Fall through to the shared LLM processing and trimming below
            # (skip the Firecrawl-specific crawl logic)
            pages_crawled = len(response.get('results', []))
            logger.info("Crawled %d pages", pages_crawled)
            debug_call_data["pages_crawled"] = pages_crawled
            debug_call_data["original_response_size"] = len(json.dumps(response))

            # Process each result with LLM if enabled
            if use_llm_processing:
                logger.info("Processing crawled content with LLM (parallel)...")
                debug_call_data["processing_applied"].append("llm_processing")

                async def _process_tavily_crawl(result):
                    page_url = result.get('url', 'Unknown URL')
                    title = result.get('title', '')
                    content = result.get('content', '')
                    if not content:
                        return result, None, "no_content"
                    original_size = len(content)
                    processed = await process_content_with_llm(content, page_url, title, model, min_length)
                    if processed:
                        result['raw_content'] = content
                        result['content'] = processed
                        metrics = {"url": page_url, "original_size": original_size, "processed_size": len(processed),
                                   "compression_ratio": len(processed) / original_size if original_size else 1.0, "model_used": model}
                        return result, metrics, "processed"
                    metrics = {"url": page_url, "original_size": original_size, "processed_size": original_size,
                               "compression_ratio": 1.0, "model_used": None, "reason": "content_too_short"}
                    return result, metrics, "too_short"

                tasks = [_process_tavily_crawl(r) for r in response.get('results', [])]
                processed_results = await asyncio.gather(*tasks)
                for result, metrics, status in processed_results:
                    if status == "processed":
                        debug_call_data["compression_metrics"].append(metrics)
                        debug_call_data["pages_processed_with_llm"] += 1

            trimmed_results = [{"url": r.get("url", ""), "title": r.get("title", ""), "content": r.get("content", ""), "error": r.get("error"),
                **({  "blocked_by_policy": r["blocked_by_policy"]} if "blocked_by_policy" in r else {})} for r in response.get("results", [])]
            result_json = json.dumps({"results": trimmed_results}, indent=2, ensure_ascii=False)
            cleaned_result = clean_base64_images(result_json)
            debug_call_data["final_response_size"] = len(cleaned_result)
            _debug.log_call("web_crawl_tool", debug_call_data)
            _debug.save()
            return cleaned_result

        # web_crawl requires Firecrawl — Parallel has no crawl API
        if not (os.getenv("FIRECRAWL_API_KEY") or os.getenv("FIRECRAWL_API_URL")):
            return json.dumps({
                "error": "web_crawl requires Firecrawl. Set FIRECRAWL_API_KEY, "
                         "or use web_search + web_extract instead.",
                "success": False,
            }, ensure_ascii=False)

        # Ensure URL has protocol
        if not url.startswith(('http://', 'https://')):
            url = f'https://{url}'
            logger.info("Added https:// prefix to URL: %s", url)
        
        instructions_text = f" with instructions: '{instructions}'" if instructions else ""
        logger.info("Crawling %s%s", url, instructions_text)
        
        # SSRF protection — block private/internal addresses
        if not is_safe_url(url):
            return json.dumps({"results": [{"url": url, "title": "", "content": "",
                "error": "Blocked: URL targets a private or internal network address"}]}, ensure_ascii=False)

        # Website policy check — block before crawling
        blocked = check_website_access(url)
        if blocked:
            logger.info("Blocked web_crawl for %s by rule %s", blocked["host"], blocked["rule"])
            return json.dumps({"results": [{"url": url, "title": "", "content": "", "error": blocked["message"],
                "blocked_by_policy": {"host": blocked["host"], "rule": blocked["rule"], "source": blocked["source"]}}]}, ensure_ascii=False)

        # Use Firecrawl's v2 crawl functionality
        # Docs: https://docs.firecrawl.dev/features/crawl
        # The crawl() method automatically waits for completion and returns all data
        
        # Build crawl parameters - keep it simple
        crawl_params = {
            "limit": 20,  # Limit number of pages to crawl
            "scrape_options": {
                "formats": ["markdown"]  # Just markdown for simplicity
            }
        }
        
        # Note: The 'prompt' parameter is not documented for crawl
        # Instructions are typically used with the Extract endpoint, not Crawl
        if instructions:
            logger.info("Instructions parameter ignored (not supported in crawl API)")
        
        from tools.interrupt import is_interrupted as _is_int
        if _is_int():
            return json.dumps({"error": "Interrupted", "success": False})

        try:
            crawl_result = _get_firecrawl_client().crawl(
                url=url,
                **crawl_params
            )
        except Exception as e:
            logger.debug("Crawl API call failed: %s", e)
            raise

        pages: List[Dict[str, Any]] = []
        
        # Process crawl results - the crawl method returns a CrawlJob object with data attribute
        data_list = []
        
        # The crawl_result is a CrawlJob object with a 'data' attribute containing list of Document objects
        if hasattr(crawl_result, 'data'):
            data_list = crawl_result.data if crawl_result.data else []
            logger.info("Status: %s", getattr(crawl_result, 'status', 'unknown'))
            logger.info("Retrieved %d pages", len(data_list))
            
            # Debug: Check other attributes if no data
            if not data_list:
                logger.debug("CrawlJob attributes: %s", [attr for attr in dir(crawl_result) if not attr.startswith('_')])
                logger.debug("Status: %s", getattr(crawl_result, 'status', 'N/A'))
                logger.debug("Total: %s", getattr(crawl_result, 'total', 'N/A'))
                logger.debug("Completed: %s", getattr(crawl_result, 'completed', 'N/A'))
                
        elif isinstance(crawl_result, dict) and 'data' in crawl_result:
            data_list = crawl_result.get("data", [])
        else:
            logger.warning("Unexpected crawl result type")
            logger.debug("Result type: %s", type(crawl_result))
            if hasattr(crawl_result, '__dict__'):
                logger.debug("Result attributes: %s", list(crawl_result.__dict__.keys()))
        
        for item in data_list:
            # Process each crawled page - properly handle object serialization
            page_url = "Unknown URL"
            title = ""
            content_markdown = None
            content_html = None
            metadata = {}
            
            # Extract data from the item
            if hasattr(item, 'model_dump'):
                # Pydantic model - use model_dump to get dict
                item_dict = item.model_dump()
                content_markdown = item_dict.get('markdown')
                content_html = item_dict.get('html')
                metadata = item_dict.get('metadata', {})
            elif hasattr(item, '__dict__'):
                # Regular object with attributes
                content_markdown = getattr(item, 'markdown', None)
                content_html = getattr(item, 'html', None)
                
                # Handle metadata - convert to dict if it's an object
                metadata_obj = getattr(item, 'metadata', {})
                if hasattr(metadata_obj, 'model_dump'):
                    metadata = metadata_obj.model_dump()
                elif hasattr(metadata_obj, '__dict__'):
                    metadata = metadata_obj.__dict__
                elif isinstance(metadata_obj, dict):
                    metadata = metadata_obj
                else:
                    metadata = {}
            elif isinstance(item, dict):
                # Already a dictionary
                content_markdown = item.get('markdown')
                content_html = item.get('html')
                metadata = item.get('metadata', {})
            
            # Ensure metadata is a dict (not an object)
            if not isinstance(metadata, dict):
                if hasattr(metadata, 'model_dump'):
                    metadata = metadata.model_dump()
                elif hasattr(metadata, '__dict__'):
                    metadata = metadata.__dict__
                else:
                    metadata = {}
            
            # Extract URL and title from metadata
            page_url = metadata.get("sourceURL", metadata.get("url", "Unknown URL"))
            title = metadata.get("title", "")
            
            # Re-check crawled page URL against policy
            page_blocked = check_website_access(page_url)
            if page_blocked:
                logger.info("Blocked crawled page %s by rule %s", page_blocked["host"], page_blocked["rule"])
                pages.append({
                    "url": page_url, "title": title, "content": "", "raw_content": "",
                    "error": page_blocked["message"],
                    "blocked_by_policy": {"host": page_blocked["host"], "rule": page_blocked["rule"], "source": page_blocked["source"]},
                })
                continue

            # Choose content (prefer markdown)
            content = content_markdown or content_html or ""
            
            pages.append({
                "url": page_url,
                "title": title,
                "content": content,
                "raw_content": content,
                "metadata": metadata  # Now guaranteed to be a dict
            })

        response = {"results": pages}
        
        pages_crawled = len(response.get('results', []))
        logger.info("Crawled %d pages", pages_crawled)
        
        debug_call_data["pages_crawled"] = pages_crawled
        debug_call_data["original_response_size"] = len(json.dumps(response))
        
        # Process each result with LLM if enabled
        if use_llm_processing:
            logger.info("Processing crawled content with LLM (parallel)...")
            debug_call_data["processing_applied"].append("llm_processing")
            
            # Prepare tasks for parallel processing
            async def process_single_crawl_result(result):
                """Process a single crawl result with LLM and return updated result with metrics."""
                page_url = result.get('url', 'Unknown URL')
                title = result.get('title', '')
                content = result.get('content', '')
                
                if not content:
                    return result, None, "no_content"
                
                original_size = len(content)
                
                # Process content with LLM
                processed = await process_content_with_llm(
                    content, page_url, title, model, min_length
                )
                
                if processed:
                    processed_size = len(processed)
                    compression_ratio = processed_size / original_size if original_size > 0 else 1.0
                    
                    # Update result with processed content
                    result['raw_content'] = content
                    result['content'] = processed
                    
                    metrics = {
                        "url": page_url,
                        "original_size": original_size,
                        "processed_size": processed_size,
                        "compression_ratio": compression_ratio,
                        "model_used": model
                    }
                    return result, metrics, "processed"
                else:
                    metrics = {
                        "url": page_url,
                        "original_size": original_size,
                        "processed_size": original_size,
                        "compression_ratio": 1.0,
                        "model_used": None,
                        "reason": "content_too_short"
                    }
                    return result, metrics, "too_short"
            
            # Run all LLM processing in parallel
            results_list = response.get('results', [])
            tasks = [process_single_crawl_result(result) for result in results_list]
            processed_results = await asyncio.gather(*tasks)
            
            # Collect metrics and print results
            for result, metrics, status in processed_results:
                page_url = result.get('url', 'Unknown URL')
                if status == "processed":
                    debug_call_data["compression_metrics"].append(metrics)
                    debug_call_data["pages_processed_with_llm"] += 1
                    logger.info("%s (processed)", page_url)
                elif status == "too_short":
                    debug_call_data["compression_metrics"].append(metrics)
                    logger.info("%s (no processing - content too short)", page_url)
                else:
                    logger.warning("%s (no content to process)", page_url)
        else:
            # Print summary of crawled pages for debugging (original behavior)
            for result in response.get('results', []):
                page_url = result.get('url', 'Unknown URL')
                content_length = len(result.get('content', ''))
                logger.info("%s (%d characters)", page_url, content_length)
        
        # Trim output to minimal fields per entry: title, content, error
        trimmed_results = [
            {
                "url": r.get("url", ""),
                "title": r.get("title", ""),
                "content": r.get("content", ""),
                "error": r.get("error"),
                **({  "blocked_by_policy": r["blocked_by_policy"]} if "blocked_by_policy" in r else {}),
            }
            for r in response.get("results", [])
        ]
        trimmed_response = {"results": trimmed_results}
        
        result_json = json.dumps(trimmed_response, indent=2, ensure_ascii=False)
        # Clean base64 images from crawled content
        cleaned_result = clean_base64_images(result_json)
        
        debug_call_data["final_response_size"] = len(cleaned_result)
        debug_call_data["processing_applied"].append("base64_image_removal")
        
        # Log debug information
        _debug.log_call("web_crawl_tool", debug_call_data)
        _debug.save()
        
        return cleaned_result
        
    except Exception as e:
        error_msg = f"Error crawling website: {str(e)}"
        logger.debug("%s", error_msg)
        
        debug_call_data["error"] = error_msg
        _debug.log_call("web_crawl_tool", debug_call_data)
        _debug.save()
        
        return json.dumps({"error": error_msg}, ensure_ascii=False)


# Convenience function to check if API key is available
def check_firecrawl_api_key() -> bool:
    """
    Check if the Firecrawl API key is available in environment variables.

    Returns:
        bool: True if API key is set, False otherwise
    """
    return bool(os.getenv("FIRECRAWL_API_KEY"))


def check_web_api_key() -> bool:
    """Check if any supported web backend credentials are available."""
    if _has_anthropic_web_credentials():
        return True
    return bool(
        os.getenv("EXA_API_KEY")
        or os.getenv("PARALLEL_API_KEY")
        or os.getenv("FIRECRAWL_API_KEY")
        or os.getenv("FIRECRAWL_API_URL")
        or os.getenv("TAVILY_API_KEY")
        or os.getenv("OPENAI_API_KEY")
    )


def check_auxiliary_model() -> bool:
    """Check if an auxiliary text model is available for LLM content processing."""
    try:
        from agent.auxiliary_client import resolve_provider_client
        for p in ("openrouter", "nous", "custom", "codex"):
            client, _ = resolve_provider_client(p)
            if client is not None:
                return True
        return False
    except Exception:
        return False


def get_debug_session_info() -> Dict[str, Any]:
    """Get information about the current debug session."""
    return _debug.get_session_info()


if __name__ == "__main__":
    """
    Simple test/demo when run directly
    """
    print("🌐 Standalone Web Tools Module")
    print("=" * 40)
    
    # Check if API keys are available
    web_available = check_web_api_key()
    nous_available = check_auxiliary_model()

    if web_available:
        backend = _get_backend()
        print(f"✅ Web backend: {backend}")
        if backend == "exa":
            print("   Using Exa API (https://exa.ai)")
        elif backend == "parallel":
            print("   Using Parallel API (https://parallel.ai)")
        elif backend == "tavily":
            print("   Using Tavily API (https://tavily.com)")
        elif backend == "openai":
            print("   Using OpenAI Responses web search (https://platform.openai.com/)")
        elif backend == "anthropic":
            print("   Using Anthropic web search / fetch (https://console.anthropic.com/)")
        else:
            print("   Using Firecrawl API (https://firecrawl.dev)")
    else:
        print("❌ No web search backend configured")
        print("Set EXA_API_KEY, PARALLEL_API_KEY, TAVILY_API_KEY, FIRECRAWL_API_KEY, OPENAI_API_KEY, or Anthropic credentials")

    if not nous_available:
        print("❌ No auxiliary model available for LLM content processing")
        print("Set OPENROUTER_API_KEY, configure Nous Portal, or set OPENAI_BASE_URL + OPENAI_API_KEY")
        print("⚠️  Without an auxiliary model, LLM content processing will be disabled")
    else:
        print(f"✅ Auxiliary model available: {DEFAULT_SUMMARIZER_MODEL}")

    if not web_available:
        exit(1)

    print("🛠️  Web tools ready for use!")
    
    if nous_available:
        print(f"🧠 LLM content processing available with {DEFAULT_SUMMARIZER_MODEL}")
        print(f"   Default min length for processing: {DEFAULT_MIN_LENGTH_FOR_SUMMARIZATION} chars")
    
    # Show debug mode status
    if _debug.active:
        print(f"🐛 Debug mode ENABLED - Session ID: {_debug.session_id}")
        print(f"   Debug logs will be saved to: {_debug.log_dir}/web_tools_debug_{_debug.session_id}.json")
    else:
        print("🐛 Debug mode disabled (set WEB_TOOLS_DEBUG=true to enable)")
    
    print("\nBasic usage:")
    print("  from web_tools import web_search_tool, web_extract_tool, web_crawl_tool")
    print("  import asyncio")
    print("")
    print("  # Search (synchronous)")
    print("  results = web_search_tool('Python tutorials')")
    print("")
    print("  # Extract and crawl (asynchronous)")
    print("  async def main():")
    print("      content = await web_extract_tool(['https://example.com'])")
    print("      crawl_data = await web_crawl_tool('example.com', 'Find docs')")
    print("  asyncio.run(main())")
    
    if nous_available:
        print("\nLLM-enhanced usage:")
        print("  # Content automatically processed for pages >5000 chars (default)")
        print("  content = await web_extract_tool(['https://python.org/about/'])")
        print("")
        print("  # Customize processing parameters")
        print("  crawl_data = await web_crawl_tool(")
        print("      'docs.python.org',")
        print("      'Find key concepts',")
        print("      model='google/gemini-3-flash-preview',")
        print("      min_length=3000")
        print("  )")
        print("")
        print("  # Disable LLM processing")
        print("  raw_content = await web_extract_tool(['https://example.com'], use_llm_processing=False)")
    
    print("\nDebug mode:")
    print("  # Enable debug logging")
    print("  export WEB_TOOLS_DEBUG=true")
    print("  # Debug logs capture:")
    print("  # - All tool calls with parameters")
    print("  # - Original API responses")
    print("  # - LLM compression metrics")
    print("  # - Final processed results")
    print("  # Logs saved to: ./logs/web_tools_debug_UUID.json")
    
    print("\n📝 Run 'python test_web_tools_llm.py' to test LLM processing capabilities")


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
from tools.registry import registry

WEB_SEARCH_SCHEMA = {
    "name": "web_search",
    "description": "Search the web for information on any topic. Returns up to 5 relevant results with titles, URLs, and descriptions.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The search query to look up on the web"
            }
        },
        "required": ["query"]
    }
}

WEB_EXTRACT_SCHEMA = {
    "name": "web_extract",
    "description": "Extract content from web page URLs. Returns page content in markdown format. Also works with PDF URLs (arxiv papers, documents, etc.) — pass the PDF link directly and it converts to markdown text. Pages under 5000 chars return full markdown; larger pages are LLM-summarized and capped at ~5000 chars per page. Pages over 2M chars are refused. If a URL fails or times out, use the browser tool to access it instead.",
    "parameters": {
        "type": "object",
        "properties": {
            "urls": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of URLs to extract content from (max 5 URLs per call)",
                "maxItems": 5
            }
        },
        "required": ["urls"]
    }
}

registry.register(
    name="web_search",
    toolset="web",
    schema=WEB_SEARCH_SCHEMA,
    handler=lambda args, **kw: web_search_tool(args.get("query", ""), limit=5),
    check_fn=check_web_api_key,
    requires_env=["EXA_API_KEY", "PARALLEL_API_KEY", "FIRECRAWL_API_KEY", "TAVILY_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "ANTHROPIC_TOKEN"],
    emoji="🔍",
)
registry.register(
    name="web_extract",
    toolset="web",
    schema=WEB_EXTRACT_SCHEMA,
    handler=lambda args, **kw: web_extract_tool(
        args.get("urls", [])[:5] if isinstance(args.get("urls"), list) else [], "markdown"),
    check_fn=check_web_api_key,
    requires_env=["EXA_API_KEY", "PARALLEL_API_KEY", "FIRECRAWL_API_KEY", "TAVILY_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "ANTHROPIC_TOKEN"],
    is_async=True,
    emoji="📄",
)
