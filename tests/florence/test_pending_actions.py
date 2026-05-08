from datetime import datetime, timezone

from florence.contracts import Channel, ChannelType, Household
from florence.messaging.channel_log import FlorenceChannelLog
from florence.messaging.pending_actions import active_pending_target_ids, latest_pending_action
from florence.messaging.protocol_types import (
    CANDIDATE_REVIEW_PROMPT_KIND,
    PENDING_ACTION_EXPIRES_AT_KEY,
    build_candidate_review_prompt_metadata,
)
from florence.state import FlorenceStateDB


def _store_with_channel(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya household", timezone="America/Los_Angeles"))
    store.upsert_channel(
        Channel(
            id="chan_dm_123",
            household_id="hh_123",
            provider="linq",
            provider_channel_id="dm-thread-123",
            channel_type=ChannelType.PARENT_DM,
            title="Maya",
        )
    )
    return store


def test_latest_pending_action_returns_active_prompt_targets(tmp_path):
    store = _store_with_channel(tmp_path)
    channel_log = FlorenceChannelLog(store)
    metadata = build_candidate_review_prompt_metadata(
        "cand_123",
        candidate_ids=["cand_123", "cand_456"],
    )
    channel_log.append_assistant_message(
        household_id="hh_123",
        channel_id="chan_dm_123",
        body="Review these two items.",
        metadata=metadata,
    )

    active = latest_pending_action(
        channel_log,
        channel_id="chan_dm_123",
        protocol_kind=CANDIDATE_REVIEW_PROMPT_KIND,
        action_type="candidate_review",
        target_kind="imported_candidate",
    )

    assert active is not None
    assert active.action.target_id == "cand_123"
    assert active_pending_target_ids(active) == ["cand_123", "cand_456"]
    store.close()


def test_latest_pending_action_hides_expired_prompt_unless_requested(tmp_path):
    store = _store_with_channel(tmp_path)
    channel_log = FlorenceChannelLog(store)
    metadata = build_candidate_review_prompt_metadata("cand_expired")
    metadata[PENDING_ACTION_EXPIRES_AT_KEY] = datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat()
    channel_log.append_assistant_message(
        household_id="hh_123",
        channel_id="chan_dm_123",
        body="Expired review prompt.",
        metadata=metadata,
    )

    hidden = latest_pending_action(
        channel_log,
        channel_id="chan_dm_123",
        protocol_kind=CANDIDATE_REVIEW_PROMPT_KIND,
        action_type="candidate_review",
        target_kind="imported_candidate",
    )
    included = latest_pending_action(
        channel_log,
        channel_id="chan_dm_123",
        protocol_kind=CANDIDATE_REVIEW_PROMPT_KIND,
        action_type="candidate_review",
        target_kind="imported_candidate",
        include_expired=True,
    )

    assert hidden is None
    assert included is not None
    assert active_pending_target_ids(included) == ["cand_expired"]
    store.close()
