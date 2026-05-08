"""Transport-agnostic Florence messaging primitives."""

from florence.messaging.ingress_types import (
    FlorenceMessagingIngressResult,
    FlorenceResolvedInboundMessage,
)
from florence.messaging.types import FlorenceInboundAttachment, FlorenceInboundMessage

__all__ = [
    "FlorenceInboundAttachment",
    "FlorenceInboundMessage",
    "FlorenceMessagingIngressResult",
    "FlorenceMessagingIngressService",
    "FlorenceResolvedInboundMessage",
]


def __getattr__(name: str):
    if name == "FlorenceMessagingIngressService":
        from florence.messaging.ingress import FlorenceMessagingIngressService

        return FlorenceMessagingIngressService
    raise AttributeError(name)
