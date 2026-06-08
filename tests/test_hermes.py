import importlib
import os
import subprocess
import sys
import threading
import types
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

import florence.hermes as hermes_module
from florence.config import Settings
from florence.hermes import (
    HermesBackend,
    HermesSaaSContractError,
    hermes_preflight_scope,
    hermes_runtime_home_context,
)
from florence.models import (
    Household,
    HouseholdMember,
    HouseholdPrivacy,
    HouseholdReadiness,
    MemoryKind,
    MemoryRecord,
    MemberRole,
    PrivacyMode,
    SourcePreference,
    SourcePreferenceKind,
)
from florence.tone import fallback_reply


PINNED_HERMES_REF = "0123456789abcdef0123456789abcdef01234567"


def _minimal_hermes_call_kwargs(now: datetime):
    household = Household(
        id="household-1",
        chat_id="chat-1",
        timezone="America/Los_Angeles",
        created_at=now,
    )
    actor = HouseholdMember(
        id="member-1",
        household_id=household.id,
        phone="+15555550100",
        role=MemberRole.PARENT,
        display_name="Sam",
        created_at_utc=now,
        last_seen_at_utc=now,
    )
    return {
        "household": household,
        "user_text": "Can you help me think through tomorrow?",
        "conversation_history": [],
        "upcoming": [],
        "memories": [],
        "members": [actor],
        "actor": actor,
        "now_utc": now,
        "source_preferences": [],
        "privacy": HouseholdPrivacy(
            household_id=household.id,
            mode=PrivacyMode.MAXIMUM,
            memory_enabled=True,
            product_analytics_opt_in=False,
            updated_at_utc=now,
        ),
        "readiness": HouseholdReadiness(
            household_id=household.id,
            timezone=household.timezone,
            parent_count=1,
            named_parent_count=1,
            child_count=0,
            connected_account_count=0,
            source_preference_count=0,
            memory_count=0,
            ready=False,
            missing=["Invite or confirm your partner as the second parent."],
        ),
    }


def test_hermes_system_message_uses_supplied_now():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household = Household(
        id="household-1",
        chat_id="chat-1",
        timezone="America/Los_Angeles",
        created_at=now,
    )
    actor = HouseholdMember(
        id="member-1",
        household_id=household.id,
        phone="+15555550100",
        role=MemberRole.PARENT,
        display_name="Sam",
        created_at_utc=now,
        last_seen_at_utc=now,
    )
    backend = HermesBackend(Settings(db_path=":memory:"))

    system_message = backend._system_message(
        household=household,
        upcoming=[],
        memories=[],
        members=[actor],
        actor=actor,
        now_utc=now,
        source_preferences=[],
        privacy=HouseholdPrivacy(
            household_id=household.id,
            mode=PrivacyMode.MAXIMUM,
            memory_enabled=True,
            product_analytics_opt_in=False,
            updated_at_utc=now,
        ),
        readiness=HouseholdReadiness(
            household_id=household.id,
            timezone=household.timezone,
            parent_count=1,
            named_parent_count=1,
            child_count=0,
            connected_account_count=0,
            source_preference_count=0,
            memory_count=0,
            ready=False,
            missing=["Invite or confirm your partner as the second parent."],
        ),
    )

    assert "Current local time: Fri, Jun 5 at 9:00 AM" in system_message


def test_module_path_cleanup_tolerates_namespace_path_errors(tmp_path):
    class BrokenNamespacePath:
        def __iter__(self):
            raise KeyError("plugins")

        def __len__(self):
            raise KeyError("plugins")

    module = types.SimpleNamespace(__file__=None, __spec__=None, __path__=BrokenNamespacePath())

    assert not hermes_module._module_loaded_from_path(module, tmp_path)


def test_hermes_system_message_includes_household_policy_context():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household = Household(
        id="household-1",
        chat_id="chat-1",
        timezone="America/Los_Angeles",
        created_at=now,
    )
    actor = HouseholdMember(
        id="member-1",
        household_id=household.id,
        phone="+15555550100",
        role=MemberRole.PARENT,
        display_name="Sam",
        created_at_utc=now,
        last_seen_at_utc=now,
    )
    backend = HermesBackend(Settings(db_path=":memory:"))

    system_message = backend._system_message(
        household=household,
        upcoming=[],
        memories=[],
        members=[actor],
        actor=actor,
        now_utc=now,
        source_preferences=[
            SourcePreference(
                id="source-rule-1",
                household_id=household.id,
                phrase="permission slips",
                preference=SourcePreferenceKind.ALWAYS_SURFACE,
                created_at_utc=now,
                updated_at_utc=now,
            ),
            SourcePreference(
                id="source-rule-2",
                household_id=household.id,
                phrase="newsletters",
                preference=SourcePreferenceKind.MUTE,
                created_at_utc=now,
                updated_at_utc=now,
            ),
        ],
        privacy=HouseholdPrivacy(
            household_id=household.id,
            mode=PrivacyMode.MAXIMUM,
            memory_enabled=False,
            product_analytics_opt_in=True,
            updated_at_utc=now,
        ),
        readiness=HouseholdReadiness(
            household_id=household.id,
            timezone=household.timezone,
            parent_count=1,
            named_parent_count=1,
            child_count=0,
            connected_account_count=0,
            source_preference_count=2,
            memory_count=0,
            ready=False,
            missing=[
                "Invite or confirm your partner as the second parent.",
                "Tell me your child or children's names, like 'our kids are Maya and Leo'.",
            ],
        ),
    )

    assert "Household setup/readiness:" in system_message
    assert "- Not ready yet." in system_message
    assert "Invite or confirm your partner as the second parent." in system_message
    assert "Household privacy controls:" in system_message
    assert "- Memory: paused" in system_message
    assert "- Product analytics: on" in system_message
    assert "Household connected-source rules:" in system_message
    assert "- always surface: permission slips" in system_message
    assert "- mute: newsletters" in system_message
    assert system_message.count("Florence structured proposal protocol:") == 1


def test_hermes_system_message_uses_saas_boundary_and_redacts_unnamed_member_phones():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household = Household(
        id="household-1",
        chat_id="chat-1",
        timezone="America/Los_Angeles",
        created_at=now,
    )
    parent = HouseholdMember(
        id="member-parent",
        household_id=household.id,
        phone="+15555550100",
        role=MemberRole.PARENT,
        display_name=None,
        created_at_utc=now,
        last_seen_at_utc=now,
    )
    helper = HouseholdMember(
        id="member-helper",
        household_id=household.id,
        phone="+15555550101",
        role=MemberRole.HELPER,
        display_name=None,
        created_at_utc=now,
        last_seen_at_utc=now,
    )
    backend = HermesBackend(Settings(db_path=":memory:"))

    system_message = backend._system_message(
        household=household,
        upcoming=[],
        memories=[
            MemoryRecord(
                id="memory-1",
                household_id=household.id,
                kind=MemoryKind.PREFERENCE,
                text="Maya likes pasta.",
                confidence=0.8,
                created_at_utc=now,
                updated_at_utc=now,
                asserted_by_member_id=parent.id,
            )
        ],
        members=[parent, helper],
        actor=parent,
        now_utc=now,
        source_preferences=[],
        privacy=HouseholdPrivacy(
            household_id=household.id,
            mode=PrivacyMode.MAXIMUM,
            memory_enabled=True,
            product_analytics_opt_in=False,
            updated_at_utc=now,
        ),
        readiness=HouseholdReadiness(
            household_id=household.id,
            timezone=household.timezone,
            parent_count=1,
            named_parent_count=0,
            child_count=0,
            connected_account_count=0,
            source_preference_count=0,
            memory_count=1,
            ready=False,
            missing=["Have each parent text 'my name is ...'."],
        ),
    )

    assert "Florence SaaS boundary:" in system_message
    assert "Treat this as an ephemeral SaaS turn." in system_message
    assert "Hermes external tools are unavailable in this SaaS pilot." in system_message
    assert "Do not attempt or claim external web/tool access" in system_message
    assert "Current sender: unnamed parent (parent)" in system_message
    assert "- unnamed parent: parent" in system_message
    assert "- unnamed helper: helper" in system_message
    assert "Maya likes pasta. (from unnamed parent)" in system_message
    assert "+15555550100" not in system_message
    assert "+15555550101" not in system_message


def test_hermes_system_message_redacts_phone_shaped_display_names():
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household = Household(
        id="household-1",
        chat_id="chat-1",
        timezone="America/Los_Angeles",
        created_at=now,
    )
    phone_only_parent = HouseholdMember(
        id="member-parent",
        household_id=household.id,
        phone="+15555550100",
        role=MemberRole.PARENT,
        display_name="+15555550100",
        created_at_utc=now,
        last_seen_at_utc=now,
    )
    mixed_name_parent = HouseholdMember(
        id="member-parent-2",
        household_id=household.id,
        phone="+15555550101",
        role=MemberRole.PARENT,
        display_name="Alex +15555550101",
        created_at_utc=now,
        last_seen_at_utc=now,
    )
    backend = HermesBackend(Settings(db_path=":memory:"))

    system_message = backend._system_message(
        household=household,
        upcoming=[],
        memories=[],
        members=[phone_only_parent, mixed_name_parent],
        actor=phone_only_parent,
        now_utc=now,
        source_preferences=[],
        privacy=HouseholdPrivacy(
            household_id=household.id,
            mode=PrivacyMode.MAXIMUM,
            memory_enabled=True,
            product_analytics_opt_in=False,
            updated_at_utc=now,
        ),
        readiness=HouseholdReadiness(
            household_id=household.id,
            timezone=household.timezone,
            parent_count=2,
            named_parent_count=2,
            child_count=0,
            connected_account_count=0,
            source_preference_count=0,
            memory_count=0,
            ready=False,
            missing=["Tell me your child or children's names, like 'our kids are Maya and Leo'."],
        ),
    )

    assert "Current sender: unnamed parent (parent)" in system_message
    assert "- unnamed parent: parent" in system_message
    assert "- Alex [phone number]: parent" in system_message
    assert "+15555550100" not in system_message
    assert "+15555550101" not in system_message


def test_hermes_backend_redacts_phone_numbers_from_runtime_context(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        assert kwargs.get('enabled_toolsets') == []\n"
        "        assert kwargs.get('skip_memory') is True\n"
        "        assert kwargs.get('save_trajectories') is False\n"
        "        assert kwargs.get('session_id', '').startswith('florence-turn-')\n"
        "        assert 'household-1' not in kwargs.get('session_id', '')\n"
        "        assert 'chat-1' not in kwargs.get('session_id', '')\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        history_blob = '\\n'.join(message.get('content', '') for message in conversation_history)\n"
        "        runtime_blob = user_message + '\\n' + history_blob\n"
        "        assert '+15555550101' not in runtime_blob\n"
        "        assert '(555) 555-0102' not in runtime_blob\n"
        "        assert '[phone number]' in runtime_blob\n"
        "        assert '2026-06-05' in user_message\n"
        "        assert '+15555550103' not in system_message\n"
        "        assert '555.555.0104' not in system_message\n"
        "        assert '+15555550105' not in system_message\n"
        "        assert '[phone number]' in system_message\n"
        "        return {'final_response': 'ok'}\n"
    )
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household = Household(
        id="household-1",
        chat_id="chat-1",
        timezone="America/Los_Angeles",
        created_at=now,
    )
    actor = HouseholdMember(
        id="member-1",
        household_id=household.id,
        phone="+15555550100",
        role=MemberRole.PARENT,
        display_name="Sam",
        created_at_utc=now,
        last_seen_at_utc=now,
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            hermes_agent_path=str(hermes_path),
            hermes_strict=True,
        )
    )

    response = backend.complete(
        household=household,
        user_text="Remind me on 2026-06-05 after I confirm +15555550101.",
        conversation_history=[
            {"role": "user", "content": "confirm partner (555) 555-0102"},
            {"role": "assistant", "content": "Got it."},
        ],
        upcoming=[("Call +15555550103 before pickup", now)],
        memories=[
            MemoryRecord(
                id="memory-1",
                household_id=household.id,
                kind=MemoryKind.PREFERENCE,
                text="Grandma's backup number is 555.555.0104.",
                confidence=0.8,
                created_at_utc=now,
                updated_at_utc=now,
                asserted_by_member_id=actor.id,
            )
        ],
        members=[actor],
        actor=actor,
        now_utc=now,
        source_preferences=[
            SourcePreference(
                id="source-rule-1",
                household_id=household.id,
                phrase="surface messages from +15555550105",
                preference=SourcePreferenceKind.ALWAYS_SURFACE,
                created_at_utc=now,
                updated_at_utc=now,
            )
        ],
        privacy=HouseholdPrivacy(
            household_id=household.id,
            mode=PrivacyMode.MAXIMUM,
            memory_enabled=True,
            product_analytics_opt_in=False,
            updated_at_utc=now,
        ),
        readiness=HouseholdReadiness(
            household_id=household.id,
            timezone=household.timezone,
            parent_count=1,
            named_parent_count=1,
            child_count=0,
            connected_account_count=0,
            source_preference_count=0,
            memory_count=1,
            ready=False,
            missing=["Invite or confirm your partner as the second parent."],
        ),
    )

    assert response == "ok"


def test_hermes_backend_redacts_email_addresses_from_runtime_context(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        history_blob = '\\n'.join(message.get('content', '') for message in conversation_history)\n"
        "        runtime_blob = user_message + '\\n' + history_blob\n"
        "        assert 'alex@example.com' not in runtime_blob\n"
        "        assert 'teacher@example.com' not in runtime_blob\n"
        "        assert 'alex@example.com' not in system_message\n"
        "        assert 'nanny@example.com' not in system_message\n"
        "        assert 'school@example.com' not in system_message\n"
        "        assert 'coach@example.com' not in system_message\n"
        "        assert '[email address]' in runtime_blob\n"
        "        assert '[email address]' in system_message\n"
        "        return {'final_response': 'ok'}\n"
    )
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    household = Household(
        id="household-1",
        chat_id="chat-1",
        timezone="America/Los_Angeles",
        created_at=now,
    )
    actor = HouseholdMember(
        id="member-1",
        household_id=household.id,
        phone="+15555550100",
        role=MemberRole.PARENT,
        display_name="Sam alex@example.com",
        created_at_utc=now,
        last_seen_at_utc=now,
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            hermes_agent_path=str(hermes_path),
            hermes_strict=True,
        )
    )

    response = backend.complete(
        household=household,
        user_text="Can you help me loop in alex@example.com?",
        conversation_history=[
            {"role": "user", "content": "Teacher is teacher@example.com"},
            {"role": "assistant", "content": "I will be careful."},
        ],
        upcoming=[("Email coach@example.com about pickup", now)],
        memories=[
            MemoryRecord(
                id="memory-1",
                household_id=household.id,
                kind=MemoryKind.FACT,
                text="Backup nanny email is nanny@example.com.",
                confidence=0.8,
                created_at_utc=now,
                updated_at_utc=now,
                asserted_by_member_id=actor.id,
            )
        ],
        members=[actor],
        actor=actor,
        now_utc=now,
        source_preferences=[
            SourcePreference(
                id="source-rule-1",
                household_id=household.id,
                phrase="surface mail from school@example.com",
                preference=SourcePreferenceKind.ALWAYS_SURFACE,
                created_at_utc=now,
                updated_at_utc=now,
            )
        ],
        privacy=HouseholdPrivacy(
            household_id=household.id,
            mode=PrivacyMode.MAXIMUM,
            memory_enabled=True,
            product_analytics_opt_in=False,
            updated_at_utc=now,
        ),
        readiness=HouseholdReadiness(
            household_id=household.id,
            timezone=household.timezone,
            parent_count=1,
            named_parent_count=1,
            child_count=0,
            connected_account_count=0,
            source_preference_count=1,
            memory_count=1,
            ready=False,
            missing=["Invite or confirm your partner as the second parent."],
        ),
    )

    assert response == "ok"


def test_hermes_backend_sets_runtime_home_before_importing_aiagent(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    runtime_home = tmp_path / "hermes-runtime-home"
    marker = tmp_path / "hermes-home-marker.txt"
    previous_home = os.environ.get("HERMES_HOME")
    (hermes_path / "run_agent.py").write_text(
        "import os\n"
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text(os.environ.get('HERMES_HOME', ''))\n"
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        Path(os.environ['HERMES_HOME'], 'private-runtime-file.txt').write_text(user_message)\n"
        "        return {'final_response': 'ok'}\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            hermes_agent_path=str(hermes_path),
            hermes_runtime_home=str(runtime_home),
            hermes_strict=True,
        )
    )
    try:
        response = backend.complete(**_minimal_hermes_call_kwargs(datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)))
    finally:
        if previous_home is None:
            os.environ.pop("HERMES_HOME", None)
        else:
            os.environ["HERMES_HOME"] = previous_home

    assert response == "ok"
    recorded_home = Path(marker.read_text())
    assert recorded_home.parent == runtime_home.resolve()
    assert recorded_home.name.startswith("florence-turn-")
    assert not recorded_home.exists()


def test_hermes_backend_cleans_runtime_home_after_strict_runtime_error(tmp_path, monkeypatch):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    runtime_home = tmp_path / "hermes-runtime-home"
    marker = tmp_path / "hermes-failing-home-marker.txt"
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "operator-hermes-home"))
    (hermes_path / "run_agent.py").write_text(
        "import os\n"
        "from pathlib import Path\n"
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        f"        Path({str(marker)!r}).write_text(os.environ['HERMES_HOME'])\n"
        "        Path(os.environ['HERMES_HOME'], 'private-runtime-file.txt').write_text(user_message)\n"
        "        raise RuntimeError('provider boom')\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            hermes_agent_path=str(hermes_path),
            hermes_runtime_home=str(runtime_home),
            hermes_strict=True,
        )
    )

    with pytest.raises(RuntimeError, match="provider boom"):
        backend.complete(
            **_minimal_hermes_call_kwargs(datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc))
        )

    recorded_home = Path(marker.read_text())
    assert recorded_home.parent == runtime_home.resolve()
    assert recorded_home.name.startswith("florence-turn-")
    assert not recorded_home.exists()
    assert os.environ["HERMES_HOME"] == str(tmp_path / "operator-hermes-home")


def test_hermes_backend_scopes_configured_checkout_to_turn_python_path(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    marker = tmp_path / "hermes-python-path-marker.txt"
    (hermes_path / "helper_module.py").write_text(
        "def marker_text():\n"
        "    return 'helper imported during hermes turn'\n"
    )
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        import helper_module\n"
        f"        open({str(marker)!r}, 'w').write(helper_module.marker_text())\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        import helper_module\n"
        "        return {'final_response': helper_module.marker_text()}\n"
    )
    path_text = str(hermes_path.resolve())
    while path_text in sys.path:
        sys.path.remove(path_text)
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            hermes_agent_path=str(hermes_path),
            hermes_strict=True,
        )
    )

    response = backend.complete(
        **_minimal_hermes_call_kwargs(datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc))
    )

    assert response == "helper imported during hermes turn"
    assert marker.read_text() == "helper imported during hermes turn"
    assert path_text not in sys.path


def test_hermes_backend_clears_checkout_modules_between_turns(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    module_name = "stateful_hermes_helper"
    sys.modules.pop(module_name, None)
    (hermes_path / f"{module_name}.py").write_text(
        "import os\n"
        "RUNTIME_HOME_AT_IMPORT = os.environ['HERMES_HOME']\n"
    )
    (hermes_path / "run_agent.py").write_text(
        f"import {module_name}\n"
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        f"        return {{'final_response': {module_name}.RUNTIME_HOME_AT_IMPORT}}\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            hermes_agent_path=str(hermes_path),
            hermes_strict=True,
        )
    )
    try:
        first = backend.complete(
            **_minimal_hermes_call_kwargs(datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc))
        )
        second = backend.complete(
            **_minimal_hermes_call_kwargs(datetime(2026, 6, 5, 16, 5, tzinfo=timezone.utc))
        )

        assert first != second
        assert "florence-turn-" in first
        assert "florence-turn-" in second
        assert module_name not in sys.modules
    finally:
        sys.modules.pop(module_name, None)


def test_hermes_backend_clears_checkout_modules_after_strict_runtime_error(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    module_name = "failing_hermes_helper"
    sys.modules.pop(module_name, None)
    (hermes_path / f"{module_name}.py").write_text(
        "VALUE = 'checkout module loaded during failing turn'\n"
    )
    (hermes_path / "run_agent.py").write_text(
        f"import {module_name}\n"
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        raise RuntimeError('provider boom after module import')\n"
    )
    path_text = str(hermes_path.resolve())
    while path_text in sys.path:
        sys.path.remove(path_text)
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            hermes_agent_path=str(hermes_path),
            hermes_strict=True,
        )
    )
    try:
        with pytest.raises(RuntimeError, match="provider boom after module import"):
            backend.complete(
                **_minimal_hermes_call_kwargs(datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc))
            )

        assert module_name not in sys.modules
        assert path_text not in sys.path
    finally:
        sys.modules.pop(module_name, None)
        while path_text in sys.path:
            sys.path.remove(path_text)


def test_hermes_backend_shadows_preexisting_module_name_during_turn(tmp_path):
    stale_path = tmp_path / "stale"
    hermes_path = tmp_path / "hermes-agent"
    stale_path.mkdir()
    hermes_path.mkdir()
    module_name = "shared_hermes_helper"
    sys.modules.pop(module_name, None)
    (stale_path / f"{module_name}.py").write_text("VALUE = 'stale module cache'\n")
    (hermes_path / f"{module_name}.py").write_text(
        "import os\n"
        "VALUE = os.environ['HERMES_HOME']\n"
    )
    (hermes_path / "run_agent.py").write_text(
        f"import {module_name}\n"
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        f"        return {{'final_response': {module_name}.VALUE}}\n"
    )
    sys.path.insert(0, str(stale_path))
    try:
        stale_module = importlib.import_module(module_name)
        assert stale_module.VALUE == "stale module cache"
        backend = HermesBackend(
            Settings(
                db_path=":memory:",
                hermes_agent_path=str(hermes_path),
                hermes_strict=True,
            )
        )

        response = backend.complete(
            **_minimal_hermes_call_kwargs(datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc))
        )

        assert response != "stale module cache"
        assert "florence-turn-" in response
        assert sys.modules[module_name] is stale_module
        assert getattr(sys.modules[module_name], "VALUE") == "stale module cache"
    finally:
        sys.modules.pop(module_name, None)
        while str(stale_path) in sys.path:
            sys.path.remove(str(stale_path))


def test_hermes_runtime_home_context_serializes_process_global_home(tmp_path):
    runtime_home = tmp_path / "hermes-runtime-home"
    settings = Settings(db_path=":memory:", hermes_runtime_home=str(runtime_home))
    holder_entered = threading.Event()
    holder_release = threading.Event()
    contender_entered = threading.Event()
    errors: list[BaseException] = []

    def hold_runtime_home() -> None:
        try:
            with hermes_runtime_home_context(settings, scope="holder", cleanup=True):
                holder_entered.set()
                holder_release.wait(timeout=2)
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    def enter_contender_runtime_home() -> None:
        try:
            with hermes_runtime_home_context(settings, scope="contender", cleanup=True):
                contender_entered.set()
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    holder = threading.Thread(target=hold_runtime_home)
    contender = threading.Thread(target=enter_contender_runtime_home)
    holder.start()
    assert holder_entered.wait(timeout=1)
    contender.start()

    assert not contender_entered.wait(timeout=0.1)
    holder_release.set()
    holder.join(timeout=1)
    contender.join(timeout=1)

    assert errors == []
    assert contender_entered.is_set()
    assert not (runtime_home / "holder").exists()
    assert not (runtime_home / "contender").exists()


def test_hermes_runtime_home_context_serializes_across_processes(tmp_path):
    pytest.importorskip("fcntl")
    runtime_home = tmp_path / "hermes-runtime-home"
    holder_ready = tmp_path / "holder-ready"
    holder_release = tmp_path / "holder-release"
    contender_entered = tmp_path / "contender-entered"
    project_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = (
        str(project_root)
        if not env.get("PYTHONPATH")
        else f"{project_root}{os.pathsep}{env['PYTHONPATH']}"
    )
    holder_script = (
        "import sys, time\n"
        "from pathlib import Path\n"
        "from florence.config import Settings\n"
        "from florence.hermes import hermes_runtime_home_context\n"
        "runtime_home = Path(sys.argv[1])\n"
        "ready = Path(sys.argv[2])\n"
        "release = Path(sys.argv[3])\n"
        "settings = Settings(db_path=':memory:', hermes_runtime_home=str(runtime_home))\n"
        "with hermes_runtime_home_context(settings, scope='holder', cleanup=True):\n"
        "    ready.write_text('ready')\n"
        "    deadline = time.monotonic() + 5\n"
        "    while not release.exists() and time.monotonic() < deadline:\n"
        "        time.sleep(0.05)\n"
    )
    contender_script = (
        "import sys\n"
        "from pathlib import Path\n"
        "from florence.config import Settings\n"
        "from florence.hermes import hermes_runtime_home_context\n"
        "runtime_home = Path(sys.argv[1])\n"
        "entered = Path(sys.argv[2])\n"
        "settings = Settings(db_path=':memory:', hermes_runtime_home=str(runtime_home))\n"
        "with hermes_runtime_home_context(settings, scope='contender', cleanup=True):\n"
        "    entered.write_text('entered')\n"
    )
    holder = subprocess.Popen(
        [sys.executable, "-c", holder_script, str(runtime_home), str(holder_ready), str(holder_release)],
        cwd=project_root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    contender: subprocess.Popen[str] | None = None
    try:
        deadline = time.monotonic() + 5
        while not holder_ready.exists() and time.monotonic() < deadline:
            if holder.poll() is not None:
                stdout, stderr = holder.communicate()
                raise AssertionError(f"holder exited early: {holder.returncode}\n{stdout}\n{stderr}")
            time.sleep(0.05)
        assert holder_ready.exists()
        contender = subprocess.Popen(
            [sys.executable, "-c", contender_script, str(runtime_home), str(contender_entered)],
            cwd=project_root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        time.sleep(0.25)
        assert not contender_entered.exists()

        holder_release.write_text("release")
        holder_stdout, holder_stderr = holder.communicate(timeout=5)
        contender_stdout, contender_stderr = contender.communicate(timeout=5)
    finally:
        for process in (holder, contender):
            if process is not None and process.poll() is None:
                process.terminate()
                process.wait(timeout=5)

    assert holder.returncode == 0, holder_stderr
    assert contender is not None
    assert contender.returncode == 0, contender_stderr
    assert holder_stdout == ""
    assert contender_stdout == ""
    assert contender_entered.exists()
    assert not (runtime_home / "holder").exists()
    assert not (runtime_home / "contender").exists()


def test_hermes_preflight_scope_is_unique_path_segment():
    first = hermes_preflight_scope()
    second = hermes_preflight_scope()

    assert first != second
    assert first.startswith("florence-preflight-")
    assert second.startswith("florence-preflight-")
    assert "/" not in first
    assert "\\" not in first


def test_hermes_backend_rejects_toolsets_at_runtime_before_loading_aiagent(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    import_marker = tmp_path / "hermes-imported.txt"
    (hermes_path / "run_agent.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(import_marker)!r}).write_text('loaded')\n"
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        return {'final_response': 'should not run'}\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            hermes_agent_path=str(hermes_path),
            hermes_enabled_toolsets=("web",),
        )
    )

    assert backend.complete(**_minimal_hermes_call_kwargs(now)) == fallback_reply()
    assert not import_marker.exists()


def test_hermes_backend_strict_mode_raises_when_toolsets_are_configured(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    import_marker = tmp_path / "hermes-imported.txt"
    (hermes_path / "run_agent.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(import_marker)!r}).write_text('loaded')\n"
        "class AIAgent:\n"
        "    pass\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            hermes_agent_path=str(hermes_path),
            hermes_enabled_toolsets=("web",),
            hermes_strict=True,
        )
    )

    with pytest.raises(RuntimeError, match="Hermes toolsets are disabled"):
        backend.complete(**_minimal_hermes_call_kwargs(now))
    assert not import_marker.exists()


def test_hermes_backend_loads_aiagent_from_configured_checkout(tmp_path, monkeypatch):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    source = 'configured-checkout'\n"
    )

    class AmbientAIAgent:
        source = "ambient-module"

    monkeypatch.setitem(sys.modules, "run_agent", types.SimpleNamespace(AIAgent=AmbientAIAgent))
    path_text = str(hermes_path.resolve())
    while path_text in sys.path:
        sys.path.remove(path_text)
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            hermes_agent_path=str(hermes_path),
        )
    )

    AIAgent = backend._load_ai_agent()

    assert AIAgent.source == "configured-checkout"
    assert path_text not in sys.path


def test_hermes_backend_allows_ambient_aiagent_only_for_local_sqlite(monkeypatch):
    class AmbientAIAgent:
        source = "ambient-local-module"

    monkeypatch.setitem(sys.modules, "run_agent", types.SimpleNamespace(AIAgent=AmbientAIAgent))
    backend = HermesBackend(Settings(db_path=":memory:"))

    AIAgent = backend._load_ai_agent()

    assert AIAgent.source == "ambient-local-module"


def test_hermes_backend_rejects_ambient_aiagent_for_postgres_saas(monkeypatch):
    class AmbientAIAgent:
        source = "ambient-production-module"

    monkeypatch.setitem(sys.modules, "run_agent", types.SimpleNamespace(AIAgent=AmbientAIAgent))
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            database_url="postgresql://florence:secret@db:5432/florence",
            hermes_strict=True,
        )
    )

    with pytest.raises(RuntimeError, match="FLORENCE_HERMES_AGENT_PATH is required"):
        backend._load_ai_agent()


def test_hermes_backend_raises_contract_error_in_postgres_without_configured_checkout(monkeypatch):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    class AmbientAIAgent:
        def __init__(self, **kwargs):
            raise AssertionError("ambient run_agent should not be used for Postgres SaaS")

    monkeypatch.setitem(sys.modules, "run_agent", types.SimpleNamespace(AIAgent=AmbientAIAgent))
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            database_url="postgresql://florence:secret@db:5432/florence",
            hermes_strict=True,
        )
    )

    with pytest.raises(HermesSaaSContractError, match="FLORENCE_HERMES_AGENT_PATH is required"):
        backend.complete(**_minimal_hermes_call_kwargs(now))


def test_hermes_backend_rejects_nonstrict_postgres_runtime_before_import(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    import_marker = tmp_path / "hermes-imported.txt"
    (hermes_path / ".florence-hermes-ref").write_text(PINNED_HERMES_REF)
    (hermes_path / "run_agent.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(import_marker)!r}).write_text('loaded')\n"
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        return {'final_response': 'should not run'}\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            database_url="postgresql://florence:secret@db:5432/florence",
            hermes_agent_path=str(hermes_path),
            hermes_agent_ref=PINNED_HERMES_REF,
            hermes_provider="openrouter",
            hermes_model="nousresearch/hermes-test",
            hermes_strict=False,
        )
    )

    with pytest.raises(HermesSaaSContractError, match="FLORENCE_HERMES_STRICT must be true"):
        backend.complete(**_minimal_hermes_call_kwargs(now))
    assert not import_marker.exists()


def test_hermes_backend_rejects_postgres_when_interprocess_lock_unavailable(
    tmp_path, monkeypatch
):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    import_marker = tmp_path / "hermes-imported.txt"
    (hermes_path / ".florence-hermes-ref").write_text(PINNED_HERMES_REF)
    (hermes_path / "run_agent.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(import_marker)!r}).write_text('loaded')\n"
        "class AIAgent:\n"
        "    pass\n"
    )
    monkeypatch.setattr(hermes_module, "fcntl", None)
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            database_url="postgresql://florence:secret@db:5432/florence",
            hermes_agent_path=str(hermes_path),
            hermes_agent_ref=PINNED_HERMES_REF,
            hermes_provider="openrouter",
            hermes_model="nousresearch/hermes-test",
            hermes_strict=True,
        )
    )

    with pytest.raises(
        HermesSaaSContractError,
        match="POSIX fcntl file locking is required",
    ):
        backend.complete(**_minimal_hermes_call_kwargs(now))
    assert not import_marker.exists()


def test_hermes_backend_can_fallback_after_local_sqlite_runtime_failure(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        pass\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        raise RuntimeError('provider temporarily unavailable')\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            hermes_agent_path=str(hermes_path),
            hermes_strict=False,
        )
    )

    assert backend.complete(**_minimal_hermes_call_kwargs(now)) == fallback_reply()


def test_hermes_backend_rejects_unpinned_postgres_checkout_before_import(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    import_marker = tmp_path / "hermes-imported.txt"
    (hermes_path / "run_agent.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(import_marker)!r}).write_text('loaded')\n"
        "class AIAgent:\n"
        "    pass\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            database_url="postgresql://florence:secret@db:5432/florence",
            hermes_agent_path=str(hermes_path),
            hermes_provider="openrouter",
            hermes_model="nousresearch/hermes-test",
            hermes_agent_ref="main",
            hermes_strict=True,
        )
    )

    with pytest.raises(RuntimeError, match="HERMES_AGENT_REF must be a full pinned Git commit SHA"):
        backend.complete(**_minimal_hermes_call_kwargs(now))
    assert not import_marker.exists()


def test_hermes_backend_rejects_abbreviated_postgres_ref_before_import(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / ".florence-hermes-ref").write_text(PINNED_HERMES_REF)
    import_marker = tmp_path / "hermes-imported.txt"
    (hermes_path / "run_agent.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(import_marker)!r}).write_text('loaded')\n"
        "class AIAgent:\n"
        "    pass\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            database_url="postgresql://florence:secret@db:5432/florence",
            hermes_agent_path=str(hermes_path),
            hermes_provider="openrouter",
            hermes_model="nousresearch/hermes-test",
            hermes_agent_ref=PINNED_HERMES_REF[:12],
            hermes_strict=True,
        )
    )

    with pytest.raises(RuntimeError, match="HERMES_AGENT_REF must be a full pinned Git commit SHA"):
        backend.complete(**_minimal_hermes_call_kwargs(now))
    assert not import_marker.exists()


def test_hermes_backend_rejects_mismatched_postgres_checkout_before_import(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / ".florence-hermes-ref").write_text("fedcba9876543210fedcba9876543210fedcba98")
    import_marker = tmp_path / "hermes-imported.txt"
    (hermes_path / "run_agent.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(import_marker)!r}).write_text('loaded')\n"
        "class AIAgent:\n"
        "    pass\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            database_url="postgresql://florence:secret@db:5432/florence",
            hermes_agent_path=str(hermes_path),
            hermes_provider="openrouter",
            hermes_model="nousresearch/hermes-test",
            hermes_agent_ref=PINNED_HERMES_REF,
            hermes_strict=True,
        )
    )

    with pytest.raises(RuntimeError, match="checkout ref does not match HERMES_AGENT_REF"):
        backend.complete(**_minimal_hermes_call_kwargs(now))
    assert not import_marker.exists()


def test_hermes_backend_rejects_abbreviated_checkout_ref_before_import(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / ".florence-hermes-ref").write_text(PINNED_HERMES_REF[:12])
    import_marker = tmp_path / "hermes-imported.txt"
    (hermes_path / "run_agent.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(import_marker)!r}).write_text('loaded')\n"
        "class AIAgent:\n"
        "    pass\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            database_url="postgresql://florence:secret@db:5432/florence",
            hermes_agent_path=str(hermes_path),
            hermes_provider="openrouter",
            hermes_model="nousresearch/hermes-test",
            hermes_agent_ref=PINNED_HERMES_REF,
            hermes_strict=True,
        )
    )

    with pytest.raises(RuntimeError, match="checkout ref does not match HERMES_AGENT_REF"):
        backend.complete(**_minimal_hermes_call_kwargs(now))
    assert not import_marker.exists()


def test_hermes_backend_requires_explicit_postgres_provider_and_model(tmp_path):
    now = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / ".florence-hermes-ref").write_text(PINNED_HERMES_REF)
    import_marker = tmp_path / "hermes-imported.txt"
    (hermes_path / "run_agent.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(import_marker)!r}).write_text('loaded')\n"
        "class AIAgent:\n"
        "    pass\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            database_url="postgresql://florence:secret@db:5432/florence",
            hermes_agent_path=str(hermes_path),
            hermes_agent_ref=PINNED_HERMES_REF,
            hermes_strict=True,
        )
    )

    with pytest.raises(RuntimeError, match="FLORENCE_HERMES_PROVIDER is required"):
        backend.complete(**_minimal_hermes_call_kwargs(now))
    assert not import_marker.exists()


def test_hermes_backend_allows_pinned_postgres_checkout(tmp_path):
    hermes_path = tmp_path / "hermes-agent"
    hermes_path.mkdir()
    (hermes_path / ".florence-hermes-ref").write_text(PINNED_HERMES_REF)
    (hermes_path / "run_agent.py").write_text(
        "class AIAgent:\n"
        "    def __init__(self, **kwargs):\n"
        "        assert kwargs.get('provider') == 'openrouter'\n"
        "        assert kwargs.get('model') == 'nousresearch/hermes-test'\n"
        "        assert kwargs.get('api_key') == 'hermes-key'\n"
        "        assert kwargs.get('base_url') == 'https://hermes.example/v1'\n"
        "        assert kwargs.get('skip_memory') is True\n"
        "        assert kwargs.get('save_trajectories') is False\n"
        "    def run_conversation(self, user_message, system_message=None, conversation_history=None):\n"
        "        return {'final_response': 'ok'}\n"
    )
    backend = HermesBackend(
        Settings(
            db_path=":memory:",
            database_url="postgresql://florence:secret@db:5432/florence",
            hermes_agent_path=str(hermes_path),
            hermes_agent_ref=PINNED_HERMES_REF,
            hermes_provider="openrouter",
            hermes_model="nousresearch/hermes-test",
            hermes_api_key="hermes-key",
            hermes_base_url="https://hermes.example/v1",
            hermes_strict=True,
        )
    )

    assert backend.complete(
        **_minimal_hermes_call_kwargs(datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc))
    ) == "ok"
