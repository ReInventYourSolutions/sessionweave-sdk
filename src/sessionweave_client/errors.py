"""Exception hierarchy mirroring the relay's ``error.code`` values.

Every relay error response has shape ``{"error": {"code": "...", "message": "..."}}``.
The client maps a known set of codes to dedicated subclasses so callers
can ``except NotInSessionError`` (etc.) without parsing strings; unknown
codes surface as the base ``SessionWeaveError``.
"""
from __future__ import annotations


class SessionWeaveError(Exception):
    """Base error raised by the SessionWeave client.

    Attributes:
        code: the relay's ``error.code`` (e.g. ``"not_in_session"``)
            or a synthetic code (``"http_error"``, ``"transport"``).
        status: the HTTP status the relay returned, if any.
        message: human-readable message from the relay.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status: int | None = None,
    ) -> None:
        super().__init__(f"[{code}] {message}" + (f" (HTTP {status})" if status else ""))
        self.code = code
        self.message = message
        self.status = status


class SessionNotFoundError(SessionWeaveError):
    """Raised when the relay reports ``session_not_found`` (HTTP 404)."""


class NotInSessionError(SessionWeaveError):
    """Raised when the bearer token isn't associated with the session
    (or has been evicted). Maps to the relay's ``not_in_session`` code
    (HTTP 401).
    """


class AlreadyRespondedError(SessionWeaveError):
    """Raised when posting a second response for the same command_id
    (HTTP 409)."""


class ValidationError(SessionWeaveError):
    """Raised on schema/validation rejection by the relay (HTTP 400 / 422).
    Wraps any ``validation_error`` or pydantic-style failure with
    structured details left in ``message``.
    """


class InvalidDeliveryModeError(SessionWeaveError):
    """Raised when ``wait_for_event`` is called on a non-pull participant."""


class WaitAlreadyInFlightError(SessionWeaveError):
    """Raised when a second ``wait_for_event`` is attempted while one is active."""


class ParticipantEvictedError(SessionWeaveError):
    """Raised when the participant is removed from the session mid-wait."""


class TooManyWaitsError(SessionWeaveError):
    """Raised when the relay's per-session concurrent-wait cap is exceeded."""


class PermissionDeniedError(SessionWeaveError):
    """Raised when an operation lacks the right token — typically
    ``session_delete`` with a bad ``creator_token``, or a brute-force
    attempt that hit the per-sid rate limiter (HTTP 403). Relay 0.16.0+.
    """


class NoHistoryError(SessionWeaveError):
    """Raised when ``get_events`` is called on an ephemeral session —
    one created without a ``persistence`` config. The relay returns
    HTTP 409 ``no_history``. Relay 0.16.0+.
    """


class PersistenceFailedError(SessionWeaveError):
    """Raised when a persistence operation fails server-side: disk full,
    permission denied, integrity check failure (HTTP 503 or 500).
    Relay 0.16.0+.
    """


class ParticipantPausedError(SessionWeaveError):
    """Raised when a command targets a participant a supervisor has halted
    (HTTP 409 ``participant_paused``). Send ``continue`` first."""


class NoteNotFoundError(SessionWeaveError):
    """Raised when replying to a supervisor note the session does not hold
    (HTTP 404 ``note_not_found``)."""


class ParticipantRemovedError(SessionWeaveError):
    """Raised when the creator or admin removed this participant from the
    session (HTTP 403 ``participant_removed``). Stop work and sub-agents;
    do not try to leave."""


_CODE_MAP: dict[str, type[SessionWeaveError]] = {
    "session_not_found": SessionNotFoundError,
    "not_in_session": NotInSessionError,
    "already_responded": AlreadyRespondedError,
    "validation_error": ValidationError,
    "invalid_config": ValidationError,
    "invalid_dispatch_filters": ValidationError,
    "invalid_targets": ValidationError,
    "invalid_delivery_mode": InvalidDeliveryModeError,
    "wait_already_in_flight": WaitAlreadyInFlightError,
    "participant_evicted": ParticipantEvictedError,
    "too_many_waits": TooManyWaitsError,
    "permission_denied": PermissionDeniedError,
    "no_history": NoHistoryError,
    "persistence_failed": PersistenceFailedError,
    "persistence_corrupted": PersistenceFailedError,
    "participant_paused": ParticipantPausedError,
    "note_not_found": NoteNotFoundError,
    "participant_removed": ParticipantRemovedError,
}


def from_response(status: int, body: dict[str, object] | None) -> SessionWeaveError:
    """Build the right exception for a relay error envelope.

    The relay returns one of two shapes (history reasons):

      * nested:  ``{"error": {"code": "<code>", "message": "<msg>"}}``
      * flat:    ``{"error": "<code>", "message": "<msg>"}``

    Both are recognised — codes map through ``_CODE_MAP`` and unknown
    codes fall back to the base ``SessionWeaveError``.
    """
    if not isinstance(body, dict):
        return SessionWeaveError("http_error",
                                  f"HTTP {status}" if body is None else str(body),
                                  status=status)
    err = body.get("error")
    if isinstance(err, dict):
        code = str(err.get("code") or "http_error")
        message = str(err.get("message") or "")
    elif isinstance(err, str):
        code = err
        message = str(body.get("message") or err)
    else:
        code = "http_error"
        message = str(body)
    cls = _CODE_MAP.get(code, SessionWeaveError)
    return cls(code, message, status=status)
