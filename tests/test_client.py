"""Unit tests using ``httpx.MockTransport`` so the SDK can be tested
without spinning up the relay. The integration story is covered by
the relay's own test suite plus the runnable examples in
``examples/``.
"""
from __future__ import annotations

from typing import Any

import httpx
import pytest

from sessionweave_client import (
    NotInSessionError,
    Participant,
    SessionNotFoundError,
    SessionWeaveClient,
    SessionWeaveError,
)


def _client_with_routes(routes: dict[tuple[str, str], httpx.Response]) -> SessionWeaveClient:
    """Build a client whose underlying httpx is replaced by a mock
    transport returning fixed responses keyed by ``(method, path)``."""

    def handler(request: httpx.Request) -> httpx.Response:
        key = (request.method, request.url.path)
        if key not in routes:
            raise AssertionError(f"unexpected request {key}")
        return routes[key]

    transport = httpx.MockTransport(handler)
    c = SessionWeaveClient(base_url="http://relay.invalid")
    # Replace the underlying httpx client with one that uses our transport.
    # ``__slots__`` forbids attribute additions, so reuse the existing slot.
    c._http = httpx.AsyncClient(transport=transport, base_url="http://relay.invalid")
    return c


@pytest.mark.asyncio
async def test_info_round_trip() -> None:
    client = _client_with_routes({
        ("GET", "/v1/info"): httpx.Response(
            200, json={"name": "sessionweave", "version": "0.9.3", "transports": ["http"]}
        ),
    })
    async with client:
        info = await client.info()
        assert info["name"] == "sessionweave"
        assert "http" in info["transports"]


@pytest.mark.asyncio
async def test_create_and_join_returns_bound_participant() -> None:
    client = _client_with_routes({
        ("POST", "/v1/sessions"): httpx.Response(
            201, json={"session_id": "s1", "expires_at": "z", "config": {}, "purpose": None}
        ),
        ("POST", "/v1/sessions/s1/participants"): httpx.Response(
            201,
            json={
                "participant_id": "p1",
                "participant_token": "p1",
                "role": "worker",
                "session_id": "s1",
            },
        ),
    })
    async with client:
        sess = await client.create_session(purpose={"kind": "demo"})
        me = await client.join(sess["session_id"], role="worker", capabilities=["respond"])
        assert isinstance(me, Participant)
        assert me.session_id == "s1"
        assert me.token == "p1"
        assert me.role == "worker"


@pytest.mark.asyncio
async def test_describe_unknown_session_raises_typed_error() -> None:
    client = _client_with_routes({
        ("GET", "/v1/sessions/missing"): httpx.Response(
            404,
            json={"error": {"code": "session_not_found", "message": "session not found"}},
        ),
    })
    async with client:
        with pytest.raises(SessionNotFoundError) as exc:
            await client.describe("missing")
        assert exc.value.code == "session_not_found"
        assert exc.value.status == 404


@pytest.mark.asyncio
async def test_drain_inbox_returns_typed_events() -> None:
    client = _client_with_routes({
        ("POST", "/v1/sessions/s1/participants/me/inbox/drain"): httpx.Response(
            200,
            json={
                "messages": [
                    {"name": "command.received", "payload": {"command_id": "c1"}},
                    {"name": "command.completed", "payload": {"command_id": "c1"}},
                ]
            },
        ),
    })
    async with client:
        me = Participant(
            client, session_id="s1", participant_id="p1", token="p1", role=None
        )
        events = await me.drain_inbox()
        assert [e.name for e in events] == ["command.received", "command.completed"]
        assert events[0].payload["command_id"] == "c1"


@pytest.mark.asyncio
async def test_send_with_dispatch_all_passes_target_through() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = request.read().decode()
        return httpx.Response(201, json={"command_id": "c1", "delivered_to": ["p2"]})

    transport = httpx.MockTransport(handler)
    c = SessionWeaveClient(base_url="http://relay.invalid")
    c._http = httpx.AsyncClient(transport=transport, base_url="http://relay.invalid")
    async with c:
        me = Participant(c, session_id="s1", participant_id="p1", token="p1", role=None)
        out = await me.send(
            payload={"q": "?"},
            dispatch="all",
            target={"capability": "respond"},
            timeout_seconds=30,
        )
        import json as _json

        assert out["command_id"] == "c1"
        sent = _json.loads(captured["body"])
        assert sent["dispatch"] == "all"
        assert sent["target"] == {"capability": "respond"}
        assert sent["timeout_seconds"] == 30
        assert sent["payload"] == {"q": "?"}


@pytest.mark.asyncio
async def test_bad_bearer_returns_not_in_session() -> None:
    client = _client_with_routes({
        ("GET", "/v1/sessions/s1/participants"): httpx.Response(
            401,
            json={"error": {"code": "not_in_session", "message": "unknown token"}},
        ),
    })
    async with client:
        me = Participant(
            client, session_id="s1", participant_id="p1", token="bad", role=None
        )
        with pytest.raises(NotInSessionError):
            await me.list_participants()


@pytest.mark.asyncio
async def test_unknown_error_code_falls_back_to_base() -> None:
    client = _client_with_routes({
        ("POST", "/v1/sessions"): httpx.Response(
            500,
            json={"error": {"code": "weirdness", "message": "boom"}},
        ),
    })
    async with client:
        with pytest.raises(SessionWeaveError) as exc:
            await client.create_session()
        assert exc.value.code == "weirdness"
        assert exc.value.status == 500
