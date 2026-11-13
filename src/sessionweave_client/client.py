"""Async REST + WS client for the SessionWeave relay."""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any
from urllib.parse import parse_qs, urlparse, urlunparse

import httpx
import websockets

from sessionweave_client.errors import SessionWeaveError, from_response
from sessionweave_client.types import (
    DeliveryMode,
    DispatchMode,
    Event,
    OrderKind,
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


def _bearer(token: str) -> dict[str, str]:
    return {"authorization": f"Bearer {token}"}


def _parse_invite(link: str) -> tuple[str, str, str, str]:
    """Extract ``(session_id, join_token)`` from an invite link.

    Returns ``(session_id, join_token, http_scheme, netloc)`` where
    ``http_scheme`` is ``http`` for ``swv``/``http`` and ``https`` for
    ``swvs``/``https``. Accepts ``swv://host/<sid>?jt=<jt>``, ``swvs://...`` and the HTTP(S)
    form ``https://host/invite/<sid>?jt=<jt>``. Surrounding whitespace
    is ignored. Raises ``ValueError`` when the link is malformed.
    """
    parsed = urlparse(link.strip())
    if parsed.scheme not in ("swv", "swvs", "http", "https"):
        raise ValueError(f"unsupported invite link scheme: {parsed.scheme!r}")
    segments = [seg for seg in parsed.path.split("/") if seg]
    if parsed.scheme in ("http", "https"):
        if len(segments) != 2 or segments[0] != "invite":
            raise ValueError("invite URL must look like /invite/<session_id>?jt=<token>")
        sid = segments[1]
    else:
        if len(segments) != 1:
            raise ValueError("invite link must look like swv://host/<session_id>?jt=<token>")
        sid = segments[0]
    jts = parse_qs(parsed.query).get("jt", [])
    if not jts or not jts[0]:
        raise ValueError("invite link is missing its join token (jt)")
    scheme = "https" if parsed.scheme in ("swvs", "https") else "http"
    return sid, jts[0], scheme, parsed.netloc.lower()


class Participant:
    """A bound participant handle.

    Wraps one ``(session_id, participant_token)`` pair and exposes the
    operations the relay supports for an authenticated participant. The
    client stays open as long as the underlying ``SessionWeaveClient``
    does — typically the whole lifetime of a worker process.

    Authentication: ``token`` is the secret ``participant_token`` returned
    at join time (distinct from ``participant_id``) and is sent as
    ``Authorization: Bearer <token>`` on every REST call, and as ``?token=<token>`` on the WS upgrade
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

    async def reply_to_note(self, note_id: str, message: str) -> dict[str, Any]:
        """Reply in a supervisor note's thread (participant token).

        Raises ``NoteNotFoundError`` for an unknown ``note_id`` and
        ``PermissionDeniedError`` if the note was addressed to someone else.
        """
        r = await self._owner._http.post(
            f"/v1/sessions/{self.session_id}/supervisor/notes/{note_id}/replies",
            headers=self._auth,
            json={"message": message},
        )
        _raise_for_status(r)
        return dict(r.json())

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
            me = await client.join(
                session["session_id"], join_token=session["join_token"], role="worker",
            )
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

        The response is returned unchanged and carries one-time
        credentials (store them; the relay never returns them again):
        ``creator_token`` (delete the session, update its purpose, mint
        join / supervisor tokens), ``supervisor_token`` (+
        ``supervisor_token_id``; read-only oversight and the firehose),
        and a first ``join_token`` (valid 15 minutes, multi-use) with
        its ready-made ``invite_url`` / ``invite_link``. Hand the join
        token (or the link) to participants and call ``join()`` /
        ``join_link()``. A relay operator's ``SW_ADMIN_TOKEN`` is
        accepted wherever a creator token is.
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

    async def delete_session(self, session_id: str, token: str) -> dict[str, Any]:
        """Delete a session. ``token`` is the ``creator_token`` (or the
        relay operator's ``SW_ADMIN_TOKEN``).

        Successful deletes return ``{"deleted_at": "<iso8601>"}``. After
        deletion the session and its events are purged from the relay.
        """
        r = await self._http.request(
            "DELETE", f"/v1/sessions/{session_id}", headers=_bearer(token),
        )
        _raise_for_status(r)
        return dict(r.json())

    async def update_purpose(
        self, session_id: str, creator_token: str, purpose: dict[str, Any]
    ) -> dict[str, Any]:
        """Replace the session's purpose. Creator (or admin) token only."""
        r = await self._http.patch(
            f"/v1/sessions/{session_id}/purpose",
            headers=_bearer(creator_token),
            json={"purpose": purpose},
        )
        _raise_for_status(r)
        return dict(r.json())

    async def mint_join_token(self, session_id: str, creator_token: str) -> dict[str, Any]:
        """Mint a fresh join token (15 min, multi-use). Returns
        ``{join_token, expires_at, invite_url, invite_link}``."""
        r = await self._http.post(
            f"/v1/sessions/{session_id}/join-tokens", headers=_bearer(creator_token),
        )
        _raise_for_status(r)
        return dict(r.json())

    async def mint_supervisor_token(
        self, session_id: str, creator_token: str
    ) -> dict[str, Any]:
        """Mint an additional supervisor token. Returns
        ``{token_id, supervisor_token}`` (the secret is shown once)."""
        r = await self._http.post(
            f"/v1/sessions/{session_id}/supervisor-tokens",
            headers=_bearer(creator_token),
        )
        _raise_for_status(r)
        return dict(r.json())

    async def list_supervisor_tokens(
        self, session_id: str, creator_token: str
    ) -> list[dict[str, Any]]:
        """List supervisor tokens as ``[{token_id, created_at, revoked_at}]``."""
        r = await self._http.get(
            f"/v1/sessions/{session_id}/supervisor-tokens",
            headers=_bearer(creator_token),
        )
        _raise_for_status(r)
        return list(r.json())

    async def revoke_supervisor_token(
        self, session_id: str, creator_token: str, token_id: str
    ) -> None:
        """Revoke one supervisor token by its ``token_id``."""
        r = await self._http.delete(
            f"/v1/sessions/{session_id}/supervisor-tokens/{token_id}",
            headers=_bearer(creator_token),
        )
        _raise_for_status(r)

    async def post_order(
        self,
        session_id: str,
        token: str,
        *,
        kind: OrderKind,
        target: str = "all",
        message: str | None = None,
    ) -> dict[str, Any]:
        """Post a supervision order (supervisor, creator or admin token).

        ``kind`` is ``note``, ``halt``, ``continue`` or ``redirect``; ``note``
        and ``redirect`` need a ``message``. ``target`` is ``"all"`` or a
        participant_id. Returns ``{note_id, kind, target, message, ts_ms,
        affected}``.
        """
        body: dict[str, Any] = {"kind": kind, "target": target}
        if message is not None:
            body["message"] = message
        r = await self._http.post(
            f"/v1/sessions/{session_id}/supervisor/notes",
            headers=_bearer(token),
            json=body,
        )
        _raise_for_status(r)
        return dict(r.json())

    async def remove_participant(
        self, session_id: str, token: str, participant_id: str
    ) -> dict[str, Any]:
        """Remove a participant (creator or admin token). Returns
        ``{removed: [...], join_tokens_invalidated: n}``."""
        r = await self._http.delete(
            f"/v1/sessions/{session_id}/participants/{participant_id}",
            headers=_bearer(token),
        )
        _raise_for_status(r)
        return dict(r.json())

    async def access(self, session_id: str, token: str) -> dict[str, Any]:
        """Introspect any valid token: returns ``{role, participant_id}``."""
        r = await self._http.get(
            f"/v1/sessions/{session_id}/access", headers=_bearer(token),
        )
        _raise_for_status(r)
        return dict(r.json())

    async def get_events(
        self,
        session_id: str,
        *,
        token: str,
        after_seq: int | None = None,
        before_seq: int | None = None,
        kind: list[str] | str | None = None,
        participant_id: list[str] | str | None = None,
        limit: int = 200,
        order: str = "asc",
    ) -> dict[str, Any]:
        """Browse the timeline of a persisted session (relay 0.16.0+).

        ``token`` may be a participant, join, supervisor, creator or
        admin token.

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
            f"/v1/sessions/{session_id}/events", params=params, headers=_bearer(token),
        )
        _raise_for_status(r)
        return dict(r.json())

    async def describe(self, session_id: str, *, token: str) -> SessionInfo:
        """Snapshot a session. ``token``: participant, join, supervisor,
        creator or admin token."""
        r = await self._http.get(f"/v1/sessions/{session_id}", headers=_bearer(token))
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
        join_token: str,
        role: str | None = None,
        objective: str | None = None,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        delivery_mode: DeliveryMode | None = None,
    ) -> Participant:
        """Join a session. ``join_token`` (15 min, multi-use) is sent as the
        Bearer; the relay answers with a secret ``participant_token`` that
        the returned ``Participant`` uses from then on. A creator token or
        the admin token is also accepted in ``join_token``."""
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
        r = await self._http.post(
            f"/v1/sessions/{session_id}/participants",
            headers=_bearer(join_token),
            json=body,
        )
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

    async def join_link(self, link: str, **kw: Any) -> Participant:
        """Join from an invite link (``swv(s)://host/<sid>?jt=<jt>`` or
        ``http(s)://host/invite/<sid>?jt=<jt>``). Extra keyword args go to
        ``join()``. Raises ``ValueError`` on a malformed link, or when the
        link's host or scheme (swv/http vs swvs/https) does not match this
        client's ``base_url`` - the join token is never sent to a relay
        the link does not name."""
        sid, jt, scheme, netloc = _parse_invite(link)
        base = urlparse(self.base_url)
        if scheme != base.scheme or netloc != base.netloc.lower():
            raise ValueError(
                f"invite link points at {scheme}://{netloc}, "
                f"but this client talks to {self.base_url}"
            )
        return await self.join(sid, join_token=jt, **kw)

    async def __aexit_cleanup__(self) -> None:  # pragma: no cover - convenience
        with suppress(SessionWeaveError):
            await self.aclose()
