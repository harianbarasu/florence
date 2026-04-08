import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tools import web_tools


def _openai_message_with_citations(text: str, annotations: list[object]) -> SimpleNamespace:
    return SimpleNamespace(
        type="message",
        content=[
            SimpleNamespace(
                type="output_text",
                text=text,
                annotations=annotations,
            )
        ],
    )


def _anthropic_text_block(text: str) -> SimpleNamespace:
    return SimpleNamespace(type="text", text=text)


def _anthropic_web_search_tool_result(entries: list[object]) -> SimpleNamespace:
    return SimpleNamespace(type="web_search_tool_result", content=entries)


def _anthropic_web_fetch_tool_result(*, title: str, content: str, url: str) -> SimpleNamespace:
    source = SimpleNamespace(type="text", data=content)
    document = SimpleNamespace(title=title, source=source)
    payload = SimpleNamespace(type="web_fetch_result", content=document, url=url)
    return SimpleNamespace(type="web_fetch_tool_result", content=payload)


def test_openai_search_uses_structured_web_search():
    response = SimpleNamespace(output_text=json.dumps({
        "results": [
            {
                "title": "District Calendar",
                "url": "https://schools.example/calendar",
                "description": "Official district calendar for the current year.",
            }
        ]
    }))
    mock_client = MagicMock()
    mock_client.responses.create.return_value = response

    with patch.object(web_tools, "_get_openai_web_client", return_value=mock_client):
        result = web_tools._openai_search("district calendar", limit=3)

    assert result["success"] is True
    assert result["data"]["web"][0]["url"] == "https://schools.example/calendar"
    call = mock_client.responses.create.call_args.kwargs
    assert call["tools"][0]["type"] == "web_search"
    assert call["text"]["format"]["type"] == "json_schema"


def test_openai_search_falls_back_to_url_citations():
    annotation = SimpleNamespace(
        type="url_citation",
        url="https://camp.example",
        title="Camp Schedule",
        start_index=0,
        end_index=13,
    )
    response = SimpleNamespace(
        output_text="not json",
        output=[_openai_message_with_citations("Camp Schedule has updated hours for summer pickup.", [annotation])],
    )
    mock_client = MagicMock()
    mock_client.responses.create.return_value = response

    with patch.object(web_tools, "_get_openai_web_client", return_value=mock_client):
        result = web_tools._openai_search("camp pickup hours", limit=2)

    assert result["data"]["web"][0]["title"] == "Camp Schedule"
    assert result["data"]["web"][0]["url"] == "https://camp.example"


def test_anthropic_search_uses_beta_messages_and_schema():
    response = SimpleNamespace(content=[_anthropic_text_block(json.dumps({
        "results": [
            {
                "title": "School Lunch Menu",
                "url": "https://district.example/lunch",
                "description": "This week's lunch menu from the district site.",
            }
        ]
    }))])
    mock_client = MagicMock()
    mock_client.beta.messages.create.return_value = response

    with patch.object(web_tools, "_get_anthropic_web_client", return_value=mock_client):
        result = web_tools._anthropic_search("school lunch menu", limit=3)

    assert result["success"] is True
    assert result["data"]["web"][0]["title"] == "School Lunch Menu"
    call = mock_client.beta.messages.create.call_args.kwargs
    assert call["tools"][0]["type"] == "web_search_20260209"
    assert call["output_config"]["format"]["type"] == "json_schema"


def test_anthropic_search_falls_back_to_tool_results():
    entry = SimpleNamespace(url="https://pta.example", title="PTA Events")
    response = SimpleNamespace(content=[_anthropic_web_search_tool_result([entry])])
    mock_client = MagicMock()
    mock_client.beta.messages.create.return_value = response

    with patch.object(web_tools, "_get_anthropic_web_client", return_value=mock_client):
        result = web_tools._anthropic_search("pta events", limit=2)

    assert result["data"]["web"][0]["url"] == "https://pta.example"
    assert result["data"]["web"][0]["title"] == "PTA Events"


@pytest.mark.asyncio
async def test_web_extract_dispatches_to_openai_direct_fetch():
    extracted = [{
        "url": "https://school.example/forms",
        "title": "Forms",
        "content": "Permission slip due Friday.",
        "raw_content": "Permission slip due Friday.",
        "metadata": {"sourceURL": "https://school.example/forms", "title": "Forms"},
    }]

    with patch.object(web_tools, "_get_backend", return_value="openai"), \
         patch.object(web_tools, "is_safe_url", return_value=True), \
         patch.object(web_tools, "_direct_http_extract", new=AsyncMock(return_value=extracted)):
        result = json.loads(await web_tools.web_extract_tool(["https://school.example/forms"], use_llm_processing=False))

    assert result["results"][0]["title"] == "Forms"
    assert result["results"][0]["content"] == "Permission slip due Friday."


@pytest.mark.asyncio
async def test_anthropic_extract_falls_back_to_web_fetch_result_block():
    response = SimpleNamespace(content=[
        _anthropic_web_fetch_tool_result(
            title="Portal Notice",
            content="Portal form closes on Thursday at 5pm.",
            url="https://portal.example/forms",
        )
    ])
    mock_client = MagicMock()
    mock_client.beta.messages.create.return_value = response

    with patch.object(web_tools, "_get_anthropic_web_client", return_value=mock_client):
        result = await web_tools._anthropic_extract(["https://portal.example/forms"])

    assert result[0]["title"] == "Portal Notice"
    assert "Thursday at 5pm" in result[0]["content"]
