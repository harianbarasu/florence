"""Gatekeeper tests with a scripted fake model."""

from florence.gmail import EmailSummary
from florence.llm import LLMError, LLMReply
from florence.store import Memory
from florence.timeutil import now_utc
from florence.triage import GATE_SYSTEM, gate_email

ITEM = EmailSummary(
    account_email="parent@gmail.com",
    gmail_id="g1",
    sender="AYSO <news@ayso.org>",
    subject="Camp spots filling fast!",
    snippet="Register now...",
    received_at=now_utc(),
)


class FakeLLM:
    def __init__(self, content=None, error=None):
        self.content = content
        self.error = error
        self.calls = []

    async def chat(self, messages, *, tools=None, model=None, response_format=None):
        self.calls.append({"messages": messages, "model": model, "response_format": response_format})
        if self.error:
            raise self.error
        return LLMReply(content=self.content)


async def test_gate_skip_decision():
    llm = FakeLLM('{"justification": "marketing blast", "notify": false, "summary": ""}')
    decision = await gate_email(llm, item=ITEM, memories=[], triage_model="mini")
    assert decision.notify is False
    assert decision.justification == "marketing blast"
    assert llm.calls[0]["model"] == "mini"
    assert llm.calls[0]["response_format"] == {"type": "json_object"}


async def test_gate_notify_with_fenced_json():
    llm = FakeLLM('Sure! {"justification": "permission slip due", "notify": true, "summary": "Sign by Thursday"}')
    decision = await gate_email(llm, item=ITEM, memories=[], triage_model="mini")
    assert decision.notify is True
    assert decision.summary == "Sign by Thursday"


async def test_gate_fails_open_on_garbage():
    decision = await gate_email(FakeLLM("not json at all"), item=ITEM, memories=[], triage_model="m")
    assert decision.notify is True
    assert "gate error" in decision.justification


async def test_gate_fails_open_on_llm_error():
    decision = await gate_email(
        FakeLLM(error=LLMError("boom")), item=ITEM, memories=[], triage_model="m"
    )
    assert decision.notify is True


async def test_family_memories_reach_the_gate():
    llm = FakeLLM('{"justification": "ignored per preference", "notify": false, "summary": ""}')
    memories = [Memory(id="m1", content="Ignore AYSO emails", category="preferences", created_at=now_utc())]
    await gate_email(llm, item=ITEM, memories=memories, triage_model="mini")
    system = llm.calls[0]["messages"][0]["content"]
    assert "Ignore AYSO emails" in system


def test_gate_prompt_has_core_rules():
    assert "Never let a login alert through" in GATE_SYSTEM
    assert "spirit beats the letter" in GATE_SYSTEM
