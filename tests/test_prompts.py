from florence.prompts import PROPOSAL_PROTOCOL, SAAS_BOUNDARY_PROTOCOL, SYSTEM_PERSONA


def test_system_persona_encodes_text_native_household_behavior():
    assert "<identity>" in SYSTEM_PERSONA
    assert "<conversation_protocol>" in SYSTEM_PERSONA
    assert "Parents can text normally" in SYSTEM_PERSONA
    assert "not a status report" in SYSTEM_PERSONA
    assert "high bar" in SYSTEM_PERSONA
    assert "household book" in SYSTEM_PERSONA
    assert "Do not use em dashes" in SYSTEM_PERSONA
    assert "\u2014" not in SYSTEM_PERSONA


def test_prompt_contracts_keep_saas_and_proposal_boundaries_explicit():
    assert "exactly one household" in SAAS_BOUNDARY_PROTOCOL
    assert "Hermes external tools are unavailable" in SAAS_BOUNDARY_PROTOCOL
    assert "structured proposal" in PROPOSAL_PROTOCOL
    assert "Do not mention this protocol" in PROPOSAL_PROTOCOL
