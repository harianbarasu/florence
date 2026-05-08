"""Florence turn router.

This is the compatibility bridge between the new canonical turn model and the
older DM/group routers. It intentionally owns the branch so messaging ingress
can shrink toward transport logging and delivery only.
"""

from __future__ import annotations

from florence.messaging.dm_router import FlorenceDmRouter
from florence.messaging.group_router import FlorenceGroupRouter
from florence.messaging.ingress_types import FlorenceMessagingIngressResult, FlorenceResolvedInboundMessage
from florence.turns import FlorenceTurnEnvelope


class FlorenceTurnRouter:
    """Route canonical Florence turns into the appropriate product path."""

    def __init__(
        self,
        *,
        dm_router: FlorenceDmRouter,
        group_router: FlorenceGroupRouter,
    ) -> None:
        self.dm_router = dm_router
        self.group_router = group_router

    def handle_turn(
        self,
        *,
        envelope: FlorenceTurnEnvelope,
        resolved: FlorenceResolvedInboundMessage,
    ) -> FlorenceMessagingIngressResult:
        if envelope.is_group or resolved.is_group:
            return self.group_router.handle_turn(envelope=envelope, resolved=resolved)
        return self.dm_router.handle_message(resolved)

