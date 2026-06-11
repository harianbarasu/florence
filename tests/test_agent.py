"""Agent-loop tests with a scripted fake model and in-memory store."""

import json
from datetime import timedelta, timezone


from florence.agent import Deps, run_turn, split_bubbles
from florence.config import Settings
from florence.llm import LLMReply
from florence.store import Chat, Household, Member, Memory, StoredMessage, TaskItem
from florence.timeutil import now_utc

SETTINGS = Settings(model_api_key="test-key", admin_api_key="x")
HOUSEHOLD = Household(
    id="h1", name="The Test family", timezone="America/Los_Angeles", primary_chat_id="sandbox-c1", stopped=False
)
CHAT = Chat(chat_id="sandbox-c1", household_id="h1", kind="group")
SARAH = Member(id="m1", household_id="h1", phone="+15555550100", name="Sarah", role="parent")


class FakeStore:
    def __init__(self):
        self.tasks: list[dict] = []
        self.memories: list[dict] = []
        self.outbound: list[dict] = []
        self.events: list[dict] = []
        self.history: list[StoredMessage] = [
            StoredMessage(
                id=1,
                chat_id="sandbox-c1",
                direction="inbound",
                sender_phone="+15555550100",
                sender_name=None,
                body="remind me tomorrow at 8am to pack Maya's cleats",
                attachments=[],
                created_at=now_utc(),
            )
        ]

    async def chats_of(self, household_id):
        return [CHAT]

    async def members_of(self, household_id):
        return [SARAH]

    async def list_memories(self, household_id, limit=80):
        return [
            Memory(id=m["id"], content=m["content"], category=m.get("category"), created_at=now_utc())
            for m in self.memories
        ]

    async def upcoming_tasks(self, household_id, within_days=14, limit=20):
        return []

    async def gmail_accounts(self, household_id=None):
        return []

    async def recent_messages(self, chat_id, limit=40):
        return self.history[-limit:]

    async def recent_other_chat_messages(self, household_id, exclude_chat_id, limit=8):
        return []

    async def household_by_id(self, household_id):
        return HOUSEHOLD

    async def record_message(self, **kwargs):
        if kwargs.get("direction") == "outbound":
            self.outbound.append(kwargs)
        return len(self.outbound) + 100

    async def create_task(self, **kwargs):
        kwargs["id"] = f"t{len(self.tasks) + 1}"
        self.tasks.append(kwargs)
        return kwargs["id"]

    async def get_task(self, household_id, task_id):
        for t in self.tasks:
            if t["id"] == task_id:
                return TaskItem(
                    id=t["id"],
                    household_id=household_id,
                    chat_id=t.get("chat_id"),
                    kind=t.get("kind", "reminder"),
                    title=t["title"],
                    notes=t.get("notes"),
                    due_at=t["due_at"],
                    recurrence=t.get("recurrence"),
                    status="pending",
                    attempts=0,
                )
        return None

    async def add_memory(self, household_id, content, category, created_by):
        memory_id = f"mem{len(self.memories) + 1}"
        self.memories.append({"id": memory_id, "content": content, "category": category})
        return memory_id

    async def log_event(self, kind, household_id=None, payload=None):
        self.events.append({"kind": kind, "payload": payload})


class FakeLLM:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls: list[list[dict]] = []

    async def chat(self, messages, tools=None):
        self.calls.append(messages)
        return self.replies.pop(0)


class FakeLinq:
    def __init__(self):
        self.sent = []
        self.live = False

    async def send_text(self, *, chat_id, text, idempotency_key):
        self.sent.append((chat_id, text))
        return {}

    async def start_typing(self, chat_id):
        pass

    async def stop_typing(self, chat_id):
        pass


class FakeGmail:
    configured = False


def tool_call(name, args, call_id="call_1"):
    return {
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": json.dumps(args)},
    }


def make_deps(llm):
    return Deps(settings=SETTINGS, store=FakeStore(), linq=FakeLinq(), gmail=FakeGmail(), llm=llm)


async def test_turn_schedules_reminder_then_replies():
    tomorrow = (now_utc() + timedelta(days=1)).astimezone(
        timezone(timedelta(hours=-7))
    ).strftime("%Y-%m-%d") + " 08:00"
    llm = FakeLLM(
        [
            LLMReply(content="", tool_calls=[tool_call("schedule_reminder", {"title": "Pack Maya's cleats", "when_local": tomorrow})]),
            LLMReply(content="Done — I'll nudge you tomorrow at 8am."),
        ]
    )
    deps = make_deps(llm)
    result = await run_turn(deps, household=HOUSEHOLD, chat=CHAT, member=SARAH)

    assert result.error is None
    assert len(deps.store.tasks) == 1
    assert deps.store.tasks[0]["title"] == "Pack Maya's cleats"
    assert [s["text"] for s in result.sent] == ["Done — I'll nudge you tomorrow at 8am."]
    # The tool result fed back to the model includes the human-readable fire time.
    tool_message = llm.calls[1][-1]
    assert tool_message["role"] == "tool"
    assert "will_fire" in tool_message["content"]


async def test_past_time_tool_error_lets_model_recover():
    llm = FakeLLM(
        [
            LLMReply(content="", tool_calls=[tool_call("schedule_reminder", {"title": "x", "when_local": "2020-01-01 08:00"})]),
            LLMReply(content="Hm, give me a real time?"),
        ]
    )
    deps = make_deps(llm)
    result = await run_turn(deps, household=HOUSEHOLD, chat=CHAT, member=SARAH)
    tool_message = llm.calls[1][-1]
    assert "in the past" in tool_message["content"]
    assert deps.store.tasks == []
    assert result.error is None


async def test_empty_reply_means_silence():
    llm = FakeLLM([LLMReply(content="")])
    deps = make_deps(llm)
    result = await run_turn(deps, household=HOUSEHOLD, chat=CHAT, member=SARAH)
    assert result.sent == []
    assert deps.store.outbound == []


async def test_remember_tool_saves_memory():
    llm = FakeLLM(
        [
            LLMReply(content="", tool_calls=[tool_call("remember", {"content": "Maya plays soccer on Tuesdays", "category": "activities"})]),
            LLMReply(content="Noted!"),
        ]
    )
    deps = make_deps(llm)
    await run_turn(deps, household=HOUSEHOLD, chat=CHAT, member=SARAH)
    assert deps.store.memories[0]["content"] == "Maya plays soccer on Tuesdays"


async def test_system_prompt_contains_context():
    llm = FakeLLM([LLMReply(content="hi")])
    deps = make_deps(llm)
    await run_turn(deps, household=HOUSEHOLD, chat=CHAT, member=SARAH)
    system = llm.calls[0][0]
    assert system["role"] == "system"
    assert "Sarah" in system["content"]
    assert "America/Los_Angeles" in system["content"]
    assert "It is " in system["content"]
    # Conversation history is present with sender attribution.
    assert any("Sarah:" in str(m.get("content", "")) for m in llm.calls[0])


async def test_unknown_tool_returns_error_to_model():
    llm = FakeLLM(
        [
            LLMReply(content="", tool_calls=[tool_call("order_groceries", {})]),
            LLMReply(content="I can't do that yet."),
        ]
    )
    deps = make_deps(llm)
    result = await run_turn(deps, household=HOUSEHOLD, chat=CHAT, member=SARAH)
    tool_message = llm.calls[1][-1]
    assert "unknown tool" in tool_message["content"]
    assert result.error is None


def test_split_bubbles():
    assert split_bubbles("one line") == ["one line"]
    assert split_bubbles("first thought\n\nsecond thought") == ["first thought", "second thought"]
    # Too many chunks: keep as one message.
    many = "\n\n".join(f"chunk {i}" for i in range(5))
    assert len(split_bubbles(many)) == 1
    # Oversized chunks: keep as one message.
    big = ("x" * 700) + "\n\n" + "y"
    assert len(split_bubbles(big)) == 1
