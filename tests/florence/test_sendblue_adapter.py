from florence.sendblue import build_sendblue_thread_id, parse_sendblue_payload


def test_sendblue_adapter_parses_inbound_message_payload():
    inbound = parse_sendblue_payload(
        {
            "content": "Hello Florence",
            "is_outbound": False,
            "status": "RECEIVED",
            "message_handle": "msg_123",
            "date_sent": "2026-03-29T19:41:20.932Z",
            "from_number": "+15555550123",
            "number": "+15555550123",
            "to_number": "+15122164639",
            "sendblue_number": "+15122164639",
            "group_id": "",
            "participants": ["+15555550123", "+15122164639"],
            "service": "iMessage",
            "message_type": "message",
        }
    )

    assert inbound.provider == "sendblue"
    assert inbound.message_id == "msg_123"
    assert inbound.thread_id == build_sendblue_thread_id(
        sendblue_number="+15122164639",
        contact_number="+15555550123",
    )
    assert inbound.sender_handle == "+15555550123"
    assert inbound.body == "Hello Florence"
    assert inbound.is_group_chat is False
    assert inbound.is_from_me is False
    assert inbound.event_type == "RECEIVED"
