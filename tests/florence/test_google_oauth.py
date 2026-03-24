from florence.google import (
    FlorenceGoogleOauthState,
    build_google_oauth_connect_url,
    decode_google_oauth_state,
    encode_google_oauth_state,
)


def test_google_oauth_state_round_trip():
    state = FlorenceGoogleOauthState(
        household_id="hh_123",
        member_id="mem_123",
        thread_id="thread_123",
        nonce="nonce_123",
        issued_at_ms=1_700_000_000_000,
    )

    encoded = encode_google_oauth_state(state, "secret-value")
    decoded = decode_google_oauth_state(
        encoded,
        "secret-value",
        now_ms=1_700_000_100_000,
    )

    assert decoded == state


def test_google_oauth_state_rejects_expired_payload():
    state = FlorenceGoogleOauthState(
        household_id="hh_123",
        member_id="mem_123",
        thread_id=None,
        nonce="nonce_123",
        issued_at_ms=1_700_000_000_000,
    )

    encoded = encode_google_oauth_state(state, "secret-value")

    try:
        decode_google_oauth_state(
            encoded,
            "secret-value",
            max_age_seconds=60,
            now_ms=1_700_000_100_000,
        )
    except ValueError as exc:
        assert str(exc) == "google_oauth_state_expired"
    else:
        raise AssertionError("Expected expired OAuth state to be rejected")


def test_build_google_oauth_connect_url_contains_google_oauth_parameters():
    state = FlorenceGoogleOauthState(
        household_id="hh_123",
        member_id="mem_123",
        thread_id=None,
        nonce="nonce_123",
        issued_at_ms=1_700_000_000_000,
    )

    url = build_google_oauth_connect_url(
        client_id="client-id",
        redirect_uri="https://example.com/callback",
        state_payload=state,
        state_secret="secret-value",
    )

    assert "accounts.google.com/o/oauth2/v2/auth" in url
    assert "client_id=client-id" in url
    assert "redirect_uri=https%3A%2F%2Fexample.com%2Fcallback" in url
    assert "access_type=offline" in url
    assert "prompt=consent" in url
    assert "state=" in url
