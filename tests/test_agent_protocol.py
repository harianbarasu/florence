from florence.agent_protocol import extract_agent_proposals
from florence.models import SourcePreferenceKind


def test_extract_agent_proposals_accepts_only_first_hidden_payload():
    bundle = extract_agent_proposals(
        "Got it.\n"
        "```florence\n"
        '{"source_preferences":[{"preference":"always_surface","phrase":"permission slips"}]}\n'
        "```\n"
        "```florence\n"
        '{"actions":[{"type":"create_reminder","payload":{"title":"Hidden second action","due_at_utc":"2026-06-06T15:00:00+00:00"}}]}\n'
        "```"
    )

    assert bundle.reply_text == "Got it."
    assert bundle.actions == []
    assert bundle.memories == []
    assert len(bundle.source_preferences) == 1
    assert bundle.source_preferences[0].preference == SourcePreferenceKind.ALWAYS_SURFACE
    assert bundle.source_preferences[0].phrase == "permission slips"


def test_extract_agent_proposals_ignores_oversized_hidden_payload():
    oversized_text = "x" * 5000
    bundle = extract_agent_proposals(
        "I can help.\n"
        "```florence\n"
        f'{{"memories":[{{"kind":"fact","text":"{oversized_text}"}}]}}\n'
        "```"
    )

    assert bundle.reply_text == "I can help."
    assert bundle.actions == []
    assert bundle.memories == []
    assert bundle.source_preferences == []
    assert bundle.rejected_proposal_count == 1


def test_extract_agent_proposals_rejects_contact_details_in_memory_text():
    bundle = extract_agent_proposals(
        "Got it.\n"
        "```florence\n"
        '{"memories":[{"kind":"fact","text":"Grandma backup phone is +15555550123."}]}\n'
        "```"
    )

    assert bundle.reply_text == "Got it."
    assert bundle.memories == []
    assert bundle.rejected_proposal_count == 1


def test_extract_agent_proposals_drops_contact_detail_memory_subject():
    bundle = extract_agent_proposals(
        "Got it.\n"
        "```florence\n"
        '{"memories":[{"kind":"preference","subject":"teacher@example.com","text":"Likes short updates."}]}\n'
        "```"
    )

    assert len(bundle.memories) == 1
    assert bundle.memories[0].text == "Likes short updates."
    assert bundle.memories[0].subject is None
    assert bundle.rejected_proposal_count == 0


def test_extract_agent_proposals_rejects_contact_details_in_source_preferences():
    bundle = extract_agent_proposals(
        "I will stay selective.\n"
        "```florence\n"
        '{"source_preferences":[{"preference":"always_surface","phrase":"teacher@example.com"}]}\n'
        "```"
    )

    assert bundle.reply_text == "I will stay selective."
    assert bundle.source_preferences == []
    assert bundle.rejected_proposal_count == 1
