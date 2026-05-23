"""Async REST + WS client for the SessionWeave relay."""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx
import websockets

from sessionweave_client.errors import SessionWeaveError, from_response
from sessionweave_client.types import (
    DeliveryMode,
    DispatchMode,
    Event,
    ResponseStatus,
    SessionInfo,
    WaitForEventResult,
)

DEFAULT_BASE_URL = "https://relay.sessionweave.com"


def _ws_base(http_base: str) -> str:
    parsed = urlparse(http_base)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunparse((scheme, parsed.netloc, "", "", "", ""))


def _raise_for_status(r: httpx.Response) -> None:
    if r.is_success:
        return
    body: dict[str, Any] | None
    try:
        body = r.json()
    except ValueError:
        body = None
    raise from_response(r.status_code, body)


class Participant:
    """A bound participant handle.

    Wraps one ``(session_id, participant_token)`` pair and exposes the
    operations the relay supports for an authenticated participant. The
    client stays open as long as the underlying ``SessionWeaveClient``
    does — typically the whole lifetime of a worker process.

    Authentication: ``token`` is sent as ``Authorization: Bearer <token>``
    on every REST call, and as ``?token=<token>`` on the WS upgrade
    (browsers can't set custom headers on a WebSocket handshake).
    """

    __slots__ = (
        "_owner",
        "_role",
        "participant_id",
        "relay_epoch",
        "session_id",
        "token",
    )

    def __init__(
        self,
        owner: SessionWeaveClient,
        *,
        session_id: str,
        participant_id: str,
        token: str,
        role: str | None,
        relay_epoch: int = 0,
    ) -> None:
        self._owner = owner
        self.session_id = session_id
        self.participant_id = participant_id
        self.token = token
        self._role = role
        # Per-process startup epoch the relay returned at join time.
        # On a future ``session_resume`` (or any subsequent ``GET
        # /v1/info``) a different value means the relay has restarted
        # — the participant should treat any in-memory cursor
        # (``last_acked_seq``, buffered events) as invalid. Requires
        # relay >= 0.15.0; older relays return 0 so consumers can
        # default-check.
        self.relay_epoch = relay_epoch

    @property
    def role(self) -> str | None:
        return self._role

    @property
    def _auth(self) -> dict[str, str]:
        return {"authorization": f"Bearer {self.token}"}

    async def list_participants(self) -> list[dict[str, Any]]:
        r = await self._owner._http.get(
            f"/v1/sessions/{self.session_id}/participants", headers=self._auth
        )
        _raise_for_status(r)
        return list(r.json()["roster"])

    async def update_self(
        self,
        *,
        role: str | None = None,
        objective: str | None = None,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if role is not None:
            body["role"] = role
        if objective is not None:
            body["objective"] = objective
        if capabilities is not None:
            body["capabilities"] = capabilities
        if metadata is not None:
            body["metadata"] = metadata
        r = await self._owner._http.patch(
            f"/v1/sessions/{self.session_id}/participants/me",
            headers=self._auth,
            json=body,
        )
        _raise_for_status(r)
        return dict(r.json())

    async def update_purpose(self, purpose: dict[str, Any]) -> dict[str, Any]:
        r = await self._owner._http.patch(
            f"/v1/sessions/{self.session_id}/purpose",
            headers=self._auth,
            json={"purpose": purpose},
        )
        _raise_for_status(r)
        return dict(r.json())

    async def send(
        self,
        *,
        payload: dict[str, Any],
        dispatch: DispatchMode = "specific",
        target: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
        command_id: str | None = None,
    ) -> dict[str, Any]:
        """Dispatch a command.

        Pass ``command_id`` (a caller-generated stable string, typically
        ``uuid.uuid4().hex``) to make retries idempotent: a second
        ``send`` with the same id returns the original dispatch's
        response without creating a duplicate event or tracker.
        Available on relay 0.16.0+.
        """
        body: dict[str, Any] = {
            "dispatch": dispatch,
            "target": target or {},
            "payload": payload,
        }
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        if command_id is not None:
            body["command_id"] = command_id
        r = await self._owner._http.post(
            f"/v1/sessions/{self.session_id}/commands",
            headers=self._auth,
            json=body,
        )
        _raise_for_status(r)
        return dict(r.json())

    async def respond(
        self,
        command_id: str,
        *,
        payload: dict[str, Any],
        status: ResponseStatus = "ok",
    ) -> None:
        r = await self._owner._http.post(
            f"/v1/sessions/{self.session_id}/commands/{command_id}/responses",
            headers=self._auth,
            json={"status": status, "payload": payload},
        )
        _raise_for_status(r)

    async def drain_inbox(self) -> list[Event]:
        """Pop and return any buffered events for this participant.

        REST clients are pull-mode by definition; this is the canonical
        way to consume events without keeping a WS open. Calling this
        while a WS is also subscribed is fine but produces duplicates
        — pick one or the other for a given client.
        """
        r = await self._owner._http.post(
            f"/v1/sessions/{self.session_id}/participants/me/inbox/drain",
            headers=self._auth,
        )
        _raise_for_status(r)
        return [Event(name=m["name"], payload=m["payload"]) for m in r.json()["messages"]]

    async def wait_for_event(
        self,
        *,
        names: list[str] | None = None,
        timeout_seconds: float = 60.0,
    ) -> WaitForEventResult:
        """Long-poll for events on this participant's inbox.

        Pull-mode primitive. Blocks server-side until at least one event
        matching the optional ``names`` filter arrives in the buffer, or
        the ``timeout_seconds`` budget elapses. Non-matching events are
        re-buffered and remain visible to subsequent ``drain_inbox`` or
        ``wait_for_event`` calls.

        Token cost on the caller side is zero while the relay is
        blocked — no inferences happen until the call returns.

        The HTTP read timeout for *this call* is automatically extended
        to ``timeout_seconds + 10`` so the connection survives the
        full server-side budget. Without this override, the client's
        per-client ``httpx`` default (typically 5–30 s) would close the
        socket before the relay finishes waiting; the relay's
        ``0.17.3+`` disconnect-cancellation path then voids the wait
        and the next call proceeds cleanly — but every aborted call
        still produces an ``httpx.ReadTimeout`` you'd otherwise have
        to retry by hand. The per-call extension makes the long-poll
        feel like a real long-poll.

        Raises:
            InvalidDeliveryModeError: caller is not in pull mode.
            WaitAlreadyInFlightError: another wait is already running for
                this participant.
            ParticipantEvictedError: the participant was removed from
                the session while waiting.
            TooManyWaitsError: per-session concurrent-wait cap reached.
        """
        body: dict[str, Any] = {"timeout_seconds": timeout_seconds}
        if names is not None:
            body["names"] = names
        # Per-call read timeout = server-side budget + 10 s slack so the
        # HTTP connection outlives the relay's own deadline. See the
        # docstring above for the failure mode this avoids.
        per_call_timeout = httpx.Timeout(
            timeout=float(timeout_seconds) + 10.0,
            connect=10.0,
        )
        r = await self._owner._http.post(
            f"/v1/sessions/{self.session_id}/participants/me/inbox/wait",
            headers=self._auth,
            json=body,
            timeout=per_call_timeout,
        )
        _raise_for_status(r)
        data = r.json()
        return WaitForEventResult(
            messages=[Event(name=m["name"], payload=m["payload"]) for m in data["messages"]],
            timed_out=bool(data["timed_out"]),
        )

    async def leave(self, *, feedback: dict[str, Any] | None = None) -> None:
        """Leave the session, optionally attaching exit ``feedback``.

        Skill v0.11.0+ makes ``feedback`` part of the standard close-out
        flow. The relay propagates it on the ``participant.left`` event
        broadcast to remaining participants — host drivers can collect
        per-worker exit reports without instrumenting the workers
        themselves. ``feedback`` is a free-form JSON object; the
        relay enforces a 4 KiB cap. The skill's recommended shape is
        ``{"summary": "...", "rating": 1..5, "issues": [...], "metrics": {...}}``.
        """
        request = self._owner._http.build_request(
            "DELETE",
            f"/v1/sessions/{self.session_id}/participants/me",
            headers=self._auth,
            json={"feedback": feedback} if feedback is not None else None,
        )
        r = await self._owner._http.send(request)
        _raise_for_status(r)

    @asynccontextmanager
    async def events(self) -> AsyncIterator[AsyncIterator[Event]]:
        """Live event stream over the WebSocket events endpoint.

        Usage:

            async with me.events() as stream:
                async for evt in stream:
                    if evt.name == "command.received":
                        await me.respond(evt.payload["command_id"], payload={"a": "..."})

        The relay drains any pending buffered events at attach-time and
        forwards them over the socket before the live stream begins, so
        a fresh connection doesn't need a separate REST drain to seed
        itself. On disconnect (your context exit, or peer-side close)
        any in-flight events the relay had handed to the WS subscriber
        but couldn't deliver are pushed back to the per-participant
        buffer — call ``drain_inbox`` on reconnect to recover them.

        IMPORTANT: don't block the asyncio loop while this context is
        open. A synchronous ``subprocess.run`` or other >10s blocking
        call inside ``async with me.events():`` starves the websockets
        ping/pong handler, the relay closes the connection, and the
        stream raises ``StopAsyncIteration`` after the next ``__anext__``.
        Wrap blocking work with ``await asyncio.to_thread(...)`` so the
        loop keeps servicing the WS.
        """
        ws_base = _ws_base(self._owner.base_url)
        url = (
            f"{ws_base}/v1/sessions/{self.session_id}/participants/me/events"
            f"?token={self.token}"
        )
        async with websockets.connect(url) as ws:
            async def _iter() -> AsyncIterator[Event]:
                try:
                    while True:
                        raw = await ws.recv()
                        msg = json.loads(raw)
                        yield Event(name=msg["name"], payload=msg["payload"])
                except websockets.exceptions.ConnectionClosed:
                    return

            yield _iter()


class SessionWeaveClient:
    """Top-level handle. Holds the underlying httpx client.

    Use as an async context manager so the connection pool closes
    deterministically:

        async with SessionWeaveClient() as client:
            session = await client.create_session(purpose={"kind": "demo"})
            me = await client.join(session["session_id"], role="worker")
            ...

    For one-off scripts, ``SessionWeaveClient()`` followed by
    ``await client.aclose()`` is equivalent.
    """

    __slots__ = ("_http", "base_url")

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        timeout: float | httpx.Timeout = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def __aenter__(self) -> SessionWeaveClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def info(self) -> dict[str, Any]:
        r = await self._http.get("/v1/info")
        _raise_for_status(r)
        return dict(r.json())

    async def create_session(
        self,
        *,
        purpose: dict[str, Any] | None = None,
        config: dict[str, int] | None = None,
        persistence: dict[str, Any] | None = None,
        creator_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a new session.

        ``persistence`` (relay 0.16.0+, opt-in) is a dict ``{mode, ttl_seconds?}``:

        - ``{"mode": "ttl", "ttl_seconds": <int>}`` keeps the session's full
          state on disk for at most ``ttl_seconds`` of inactivity, then
          purges automatically.
        - ``{"mode": "until_deleted"}`` keeps it until the host explicitly
          deletes (bounded by a relay-wide safety cap, default 90 days).

        When ``persistence`` is None (the default) the relay holds the
        session only in RAM — **Zero Data Retention** by design. The
        relay never writes a session's contents to disk in that case.

        ``creator_id`` (relay 0.17.0+) is an optional opaque handle
        identifying the creator (e.g. a cockpit user_id). If you pass
        one, the relay echoes it back verbatim in the response; if you
        don't, the relay mints a uuid4 so the response always has one.
        Use it on the client side to display "created by X" without
        exposing the creator_token. Not persisted on the relay yet —
        relay-side enforcement (creator-only ops bound to a user
        identity) is a future change once relay auth lands.

        The response always includes a one-time ``creator_token``: store
        it to authorise future ``delete_session()`` calls AND as the
        bearer for the per-session firehose (relay 0.17.0+ —
        ``GET /v1/sessions/{sid}/firehose?token=<creator_token>`` opens
        the supervisor stream that delivers every event in the session,
        including chat between other participants). The token is the
        only way to delete the session unless a relay operator
        configures ``SW_SUPERVISOR_ADMIN_TOKEN``.
        """
        body: dict[str, Any] = {}
        if purpose is not None:
            body["purpose"] = purpose
        if config is not None:
            body["config"] = config
        if persistence is not None:
            body["persistence"] = persistence
        if creator_id is not None:
            body["creator_id"] = creator_id
        r = await self._http.post("/v1/sessions", json=body)
        _raise_for_status(r)
        return dict(r.json())

    async def delete_session(
        self,
        session_id: str,
        *,
        creator_token: str,
    ) -> dict[str, Any]:
        """Delete a session (relay 0.16.0+).

        Accepts the per-session ``creator_token`` returned at
        ``create_session``. Successful deletes return
        ``{"deleted_at": "<iso8601>"}``. After deletion the session and
        its events are purged from the relay; subsequent ``describe`` or
        ``get_events`` calls return ``SessionNotFoundError``.

        Operators with the relay's ``SW_SUPERVISOR_ADMIN_TOKEN`` can also
        delete any session — pass that as ``creator_token`` here.
        """
        r = await self._http.request(
            "DELETE",
            f"/v1/sessions/{session_id}",
            headers={"authorization": f"Bearer {creator_token}"},
        )
        _raise_for_status(r)
        return dict(r.json())

    async def get_events(
        self,
        session_id: str,
        *,
        after_seq: int | None = None,
        before_seq: int | None = None,
        kind: list[str] | str | None = None,
        participant_id: list[str] | str | None = None,
        limit: int = 200,
        order: str = "asc",
    ) -> dict[str, Any]:
        """Browse the timeline of a persisted session (relay 0.16.0+).

        Returns ``{session_id, events, next_cursor, has_more, session_state}``.
        ``session_state`` is ``"hot"`` if the session is currently in RAM,
        ``"cold"`` if it's only in the persistence store (no hydrate is
        triggered by this call), or ``"ended"`` if it has been marked
        ended (events still readable until purge).

        Raises ``NoHistoryError`` (HTTP 409) when called on an ephemeral
        session (one created without ``persistence``).

        Filters ``kind`` and ``participant_id`` accept either a single
        string or a list; cursor via ``after_seq`` / ``before_seq``.
        """
        params: dict[str, str] = {"limit": str(limit), "order": order}
        if after_seq is not None:
            params["after_seq"] = str(after_seq)
        if before_seq is not None:
            params["before_seq"] = str(before_seq)
        # Filter out empty strings so a sloppy ``[..., ""]`` doesn't produce
        # a trailing-comma CSV that the relay would parse as a phantom
        # empty-string filter.
        if kind is not None:
            kind_csv = kind if isinstance(kind, str) else ",".join(
                k for k in kind if k
            )
            if kind_csv:
                params["kind"] = kind_csv
        if participant_id is not None:
            pid_csv = participant_id if isinstance(participant_id, str) else ",".join(
                p for p in participant_id if p
            )
            if pid_csv:
                params["participant_id"] = pid_csv
        r = await self._http.get(
            f"/v1/sessions/{session_id}/events", params=params,
        )
        _raise_for_status(r)
        return dict(r.json())

    async def describe(self, session_id: str) -> SessionInfo:
        r = await self._http.get(f"/v1/sessions/{session_id}")
        _raise_for_status(r)
        body = r.json()
        return SessionInfo(
            session_id=body["session_id"],
            expires_at=body["expires_at"],
            config=body["config"],
            purpose=body.get("purpose"),
            roster=body.get("roster", []),
        )

    async def join(
        self,
        session_id: str,
        *,
        role: str | None = None,
        objective: str | None = None,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        delivery_mode: DeliveryMode | None = None,
    ) -> Participant:
        body: dict[str, Any] = {}
        if role is not None:
            body["role"] = role
        if objective is not None:
            body["objective"] = objective
        if capabilities is not None:
            body["capabilities"] = capabilities
        if metadata is not None:
            body["metadata"] = metadata
        if delivery_mode is not None:
            body["delivery_mode"] = delivery_mode
        r = await self._http.post(f"/v1/sessions/{session_id}/participants", json=body)
        _raise_for_status(r)
        out = r.json()
        return Participant(
            self,
            session_id=session_id,
            participant_id=out["participant_id"],
            token=out["participant_token"],
            role=out.get("role"),
            relay_epoch=int(out.get("relay_epoch", 0) or 0),
        )

    async def __aexit_cleanup__(self) -> None:  # pragma: no cover - convenience
        with suppress(SessionWeaveError):
            await self.aclose()
