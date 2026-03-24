from florence.linq import parse_linq_payload


def test_linq_adapter_parses_2026_message_received_payload():
    inbound = parse_linq_payload(
        {
            "api_version": "v3",
            "webhook_version": "2026-02-03",
            "event_type": "message.received",
            "event_id": "evt_123",
            "trace_id": "trace_123",
            "data": {
                "chat": {
                    "id": "chat_123",
                    "is_group": False,
                },
                "id": "msg_123",
                "direction": "inbound",
                "sender_handle": {
                    "handle": "+15555550123",
                    "is_me": False,
                },
                "parts": [{"type": "text", "value": "Hello Florence"}],
                "service": "iMessage",
                "sent_at": "2026-03-23T02:00:00Z",
            },
        }
    )

    assert inbound.provider == "linq"
    assert inbound.message_id == "msg_123"
    assert inbound.thread_id == "chat_123"
    assert inbound.sender_handle == "+15555550123"
    assert inbound.body == "Hello Florence"
    assert inbound.is_group_chat is False
    assert inbound.is_from_me is False
    assert inbound.event_type == "message.received"

