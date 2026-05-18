"""Async Python client for the SessionWeave relay.

Two surfaces are exposed:

* ``SessionWeaveClient`` — entry point; creates / describes / joins sessions
  and yields ``Participant`` handles bound to a single session+token pair.
* ``Participant`` — bound handle. Drives commands, responses, leaves, and
  exposes a live event stream over the WebSocket events endpoint
  (``Participant.events``) plus a pull-mode ``drain_inbox`` for runtimes
  that can't keep a socket open.

The client speaks the relay's REST surface (``/v1/...``) for everything
*except* live event push, which uses ``wss://.../events?token=<bearer>``.
The Bearer token is the participant_id today (sub-project #1 contract);
sub-project #3 will swap it for a JWT without changing this surface.
"""
from sessionweave_client.client import (
    Participant,
    SessionWeaveClient,
)
from sessionweave_client.errors import (
    AlreadyRespondedError,
    InvalidDeliveryModeError,
    NotInSessionError,
    ParticipantEvictedError,
    SessionNotFoundError,
    SessionWeaveError,
    TooManyWaitsError,
    ValidationError,
    WaitAlreadyInFlightError,
)
from sessionweave_client.types import (
    Event,
    SessionInfo,
    WaitForEventResult,
)

__version__ = "0.4.0"

__all__ = [
    "AlreadyRespondedError",
    "Event",
    "InvalidDeliveryModeError",
    "NotInSessionError",
    "ParticipantEvictedError",
    "Participant",
    "SessionInfo",
    "SessionNotFoundError",
    "SessionWeaveClient",
    "SessionWeaveError",
    "TooManyWaitsError",
    "ValidationError",
    "WaitAlreadyInFlightError",
    "WaitForEventResult",
    "__version__",
]
