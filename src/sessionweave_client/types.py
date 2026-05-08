"""Shape of values exchanged with the relay.

These are deliberately thin dataclasses (not pydantic) so the SDK
adds zero runtime dependencies beyond ``httpx`` + ``websockets``.
The relay is the source of truth for validation; clients construct
plain dicts and the relay rejects shape errors.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class SessionInfo:
    """A snapshot of a session as returned by ``GET /v1/sessions/{sid}``."""

    session_id: str
    expires_at: str
    config: dict[str, int]
    purpose: dict[str, Any] | None
    roster: list[dict[str, Any]]


@dataclass(frozen=True, slots=True)
class Event:
    """One event from the WS stream or REST inbox drain.

    The wire format is identical between the two transports:
    ``{"name": "<event>", "payload": {...}}``. ``name`` examples include
    ``"command.received"``, ``"result.received"``, ``"command.completed"``,
    ``"participant.joined"``, ``"participant.left"``, ``"session.ended"``,
    ``"session.purpose_updated"``.
    """

    name: str
    payload: dict[str, Any]


# Dispatch enums match the relay's schema. ``"all"`` and ``"any"`` require
# at least one of ``target.role`` or ``target.capability``; ``"specific"``
# uses ``target.participant_ids``.
DispatchMode = Literal["specific", "all", "any"]
ResponseStatus = Literal["ok", "error"]
DeliveryMode = Literal["push", "pull"]
