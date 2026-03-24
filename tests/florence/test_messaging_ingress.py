from florence.messaging import (
    FlorenceInboundMessage,
    FlorenceMessagingIngressService,
    FlorenceResolvedInboundMessage,
)
from florence.runtime import (
    FlorenceCandidateReviewService,
    FlorenceHouseholdQueryService,
    FlorenceOnboardingSessionService,
)
from florence.state import FlorenceStateDB


class _StubGoogleAccountLinkService:
    def build_connect_link(self, *, household_id: str, member_id: str, thread_id: str):
        class _Link:
            url = "https://example.com/google/connect"

        return _Link()


def test_dm_parent_name_reply_includes_friendly_google_link(tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    review_service = FlorenceCandidateReviewService(store)
    onboarding_service = FlorenceOnboardingSessionService(
        store,
        candidate_review_service=review_service,
    )
    ingress = FlorenceMessagingIngressService(
        store,
        onboarding_service,
        review_service,
        FlorenceHouseholdQueryService(store),
        google_account_link_service=_StubGoogleAccountLinkService(),
    )

    result = ingress.handle_message(
        FlorenceResolvedInboundMessage(
            household_id="hh_123",
            member_id="mem_123",
            channel_id="chan_dm_123",
            thread_id="dm_thread_123",
            message=FlorenceInboundMessage(
                provider="linq",
                message_id="msg_123",
                thread_id="dm_thread_123",
                sender_handle="+15555550123",
                body="Maya",
                is_group_chat=False,
            ),
        )
    )

    assert result.consumed is True
    assert result.reply_text is not None
    assert "First step:" in result.reply_text
    assert "https://example.com/google/connect" in result.reply_text
    assert "reply done here" in result.reply_text.lower()
    store.close()
