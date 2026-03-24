from florence.bluebubbles import parse_bluebubbles_payload


def test_parse_bluebubbles_payload_normalizes_basic_message():
    inbound = parse_bluebubbles_payload(
        {
            "type": "new-message",
            "data": {
                "message": {
                    "guid": "msg_123",
                    "text": "Hi Florence",
                    "isFromMe": False,
                },
                "chat": {
                    "chatGuid": "iMessage;-;chat123",
                    "participants": ["+15555550123", "+15555550124"],
                },
                "sender": {
                    "address": "+15555550123",
                },
            },
        }
    )

    assert inbound.source_message_id == "msg_123"
    assert inbound.chat_guid == "iMessage;-;chat123"
    assert inbound.body == "Hi Florence"
    assert inbound.sender_handle == "+15555550123"
    assert inbound.is_group_chat is True


def test_parse_bluebubbles_payload_respects_explicit_direct_chat_flag():
    inbound = parse_bluebubbles_payload(
        {
            "type": "new-message",
            "data": {
                "message": {
                    "guid": "msg_456",
                    "text": "Hi Florence",
                    "isFromMe": False,
                },
                "chat": {
                    "chatGuid": "iMessage;+;15555550123",
                    "participants": ["+15555550123"],
                    "isGroup": False,
                },
                "sender": {
                    "address": "+15555550123",
                },
            },
        }
    )

    assert inbound.is_group_chat is False


def test_parse_bluebubbles_payload_accepts_participant_handles_alias():
    inbound = parse_bluebubbles_payload(
        {
            "type": "new-message",
            "data": {
                "message": {
                    "guid": "msg_789",
                    "text": "Hi Florence",
                    "isFromMe": False,
                },
                "chat": {
                    "chatIdentifier": "iMessage;-;chat456",
                    "participantHandles": ["+15555550123", "+15555550124"],
                },
                "sender": {
                    "address": "+15555550123",
                },
            },
        }
    )

    assert inbound.chat_guid == "iMessage;-;chat456"
    assert inbound.participants == ["+15555550123", "+15555550124"]
    assert inbound.is_group_chat is True
