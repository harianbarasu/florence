import json

from florence.config import (
    FlorenceBlueBubblesRuntimeConfig,
    FlorenceGoogleRuntimeConfig,
    FlorenceHermesRuntimeConfig,
    FlorenceLinqRuntimeConfig,
    FlorenceServerRuntimeConfig,
    FlorenceSettings,
)
from florence.runtime.production import FlorenceProductionService
from florence.state import FlorenceStateDB


class _FakeHouseholdChatService:
    def respond(self, **kwargs):
        class _Reply:
            text = f"Shared household reply: {kwargs['message_text']}"

        return _Reply()


def _build_settings(tmp_path):
    return FlorenceSettings(
        server=FlorenceServerRuntimeConfig(
            host="127.0.0.1",
            port=8081,
            public_base_url="https://florence.example.com",
            sync_interval_seconds=300.0,
            db_path=tmp_path / "florence.db",
        ),
        google=FlorenceGoogleRuntimeConfig(
            client_id=None,
            client_secret=None,
            redirect_uri=None,
            state_secret=None,
        ),
        bluebubbles=FlorenceBlueBubblesRuntimeConfig(
            base_url=None,
            password=None,
            webhook_secret=None,
        ),
        linq=FlorenceLinqRuntimeConfig(
            api_key=None,
            webhook_secret=None,
        ),
        hermes=FlorenceHermesRuntimeConfig(
            model="anthropic/claude-opus-4.6",
            max_iterations=4,
        ),
    )


def test_production_service_bootstrap_and_send_app_messages(tmp_path):
    settings = _build_settings(tmp_path)
    store = FlorenceStateDB(settings.server.db_path)
    service = FlorenceProductionService(settings, store=store)
    service.app.app_chat_service.household_chat_service = _FakeHouseholdChatService()

    bootstrap = service.handle_app_bootstrap(parent_name="Maya", household_name=None, timezone="America/Los_Angeles")
    bootstrap_payload = json.loads(bootstrap.body)
    assert bootstrap_payload["ok"] is True
    household_id = bootstrap_payload["household"]["id"]
    member_id = bootstrap_payload["member"]["id"]

    private_turn = service.handle_app_send_message(
        household_id=household_id,
        member_id=member_id,
        scope="private",
        text="done",
    )
    private_payload = json.loads(private_turn.body)
    assert private_payload["assistantMessage"]["body"] == "What are your children's first names?"

    threads_result = service.handle_app_threads(household_id=household_id, member_id=member_id)
    threads_payload = json.loads(threads_result.body)
    assert len(threads_payload["threads"]) == 2

    messages_result = service.handle_app_messages(
        channel_id=threads_payload["threads"][1]["id"],
        limit=10,
    )
    messages_payload = json.loads(messages_result.body)
    assert len(messages_payload["messages"]) >= 2
    store.close()
