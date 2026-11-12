"""Unit tests using ``httpx.MockTransport`` so the SDK can be tested
without spinning up the relay. The integration story is covered by
the relay's own test suite plus the runnable examples in
``examples/``.
"""
from __future__ import annotations

import json
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
        me = await client.join(
            sess["session_id"], join_token="jt", role="worker", capabilities=["respond"]
        )
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
            await client.describe("missing", token="t")
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


@pytest.mark.asyncio
async def test_wait_for_event_extends_httpx_read_timeout() -> None:
    """SDK 0.7.0+: wait_for_event passes a per-call httpx timeout =
    ``timeout_seconds + 10`` so the HTTP connection survives the
    server-side budget. Without this, an SDK with a client-default
    ``httpx`` timeout of 30 s would close the socket before the
    relay's 60 s wait completed, surfacing as ``httpx.ReadTimeout``
    on the caller side. Relay 0.17.3+ has its own disconnect safety
    net, but eliminating the spurious read-timeout on the client
    makes the long-poll feel like one."""
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["timeout_extensions"] = request.extensions.get("timeout")
        return httpx.Response(
            200, json={"messages": [], "timed_out": True}
        )

    transport = httpx.MockTransport(handler)
    c = SessionWeaveClient(base_url="http://relay.invalid")
    c._http = httpx.AsyncClient(transport=transport, base_url="http://relay.invalid")
    async with c:
        me = Participant(c, session_id="s1", participant_id="p1", token="p1", role=None)
        await me.wait_for_event(timeout_seconds=90.0)

    ext = captured["timeout_extensions"]
    assert isinstance(ext, dict), f"expected dict, got {type(ext).__name__}: {ext!r}"
    # httpx represents the per-call Timeout as a dict
    # {"connect": ..., "read": ..., "write": ..., "pool": ...} in
    # request.extensions.
    assert ext.get("read") == 100.0, (
        f"read timeout should be 90 + 10 = 100, got {ext.get('read')!r}"
    )


def _recording_client(
    responses: dict[tuple[str, str], httpx.Response],
) -> tuple[SessionWeaveClient, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        key = (request.method, request.url.path)
        if key not in responses:
            raise AssertionError(f"unexpected request {key}")
        return responses[key]

    c = SessionWeaveClient(base_url="http://relay.invalid")
    c._http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://relay.invalid"
    )
    return c, seen


_JOIN_OK = httpx.Response(
    201,
    json={
        "participant_id": "pid1",
        "participant_token": "secret-ptok",
        "role": "worker",
        "session_id": "s1",
    },
)


@pytest.mark.asyncio
async def test_join_sends_join_token_as_bearer() -> None:
    client, seen = _recording_client({("POST", "/v1/sessions/s1/participants"): _JOIN_OK})
    async with client:
        me = await client.join("s1", join_token="jt-1", role="worker")
    assert seen[0].headers["authorization"] == "Bearer jt-1"
    assert me.participant_id == "pid1"
    assert me.token == "secret-ptok"
    assert me.token != me.participant_id


@pytest.mark.asyncio
async def test_join_requires_join_token() -> None:
    async with SessionWeaveClient(base_url="http://relay.invalid") as client:
        with pytest.raises(TypeError):
            await client.join("s1", role="worker")  # type: ignore[call-arg]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "link",
    [
        "swv://relay.invalid/s1?jt=tok123",
        "swvs://relay.invalid/s1?jt=tok123",
        "http://relay.invalid/invite/s1?jt=tok123",
        "https://relay.invalid/invite/s1?jt=tok123",
        "  \n swvs://relay.invalid/s1?jt=tok123  \n",
    ],
)
async def test_join_link_parses_links(link: str) -> None:
    client, seen = _recording_client({("POST", "/v1/sessions/s1/participants"): _JOIN_OK})
    client.base_url = "https://relay.invalid" if "s://" in link else "http://relay.invalid"
    async with client:
        me = await client.join_link(link, role="worker")
    assert seen[0].headers["authorization"] == "Bearer tok123"
    assert me.session_id == "s1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "link",
    ["swv://relay.invalid/s1", "swv://relay.invalid/?jt=x", "ftp://relay.invalid/s1?jt=x", ""],
)
async def test_join_link_rejects_malformed(link: str) -> None:
    async with SessionWeaveClient(base_url="http://relay.invalid") as client:
        with pytest.raises(ValueError):
            await client.join_link(link)


@pytest.mark.asyncio
async def test_describe_and_get_events_send_token() -> None:
    client, seen = _recording_client({
        ("GET", "/v1/sessions/s1"): httpx.Response(
            200,
            json={"session_id": "s1", "expires_at": "z", "config": {}, "purpose": None,
                  "roster": []},
        ),
        ("GET", "/v1/sessions/s1/events"): httpx.Response(
            200, json={"session_id": "s1", "events": [], "has_more": False},
        ),
    })
    async with client:
        info = await client.describe("s1", token="sup-tok")
        page = await client.get_events("s1", token="sup-tok", limit=5)
    assert info.session_id == "s1"
    assert page["events"] == []
    assert [r.headers["authorization"] for r in seen] == ["Bearer sup-tok"] * 2


@pytest.mark.asyncio
async def test_delete_session_uses_token() -> None:
    client, seen = _recording_client({
        ("DELETE", "/v1/sessions/s1"): httpx.Response(200, json={"deleted_at": "t"}),
    })
    async with client:
        out = await client.delete_session("s1", "creator-tok")
    assert out == {"deleted_at": "t"}
    assert seen[0].headers["authorization"] == "Bearer creator-tok"


@pytest.mark.asyncio
async def test_update_purpose_moved_to_client() -> None:
    assert not hasattr(Participant, "update_purpose")
    client, seen = _recording_client({
        ("PATCH", "/v1/sessions/s1/purpose"): httpx.Response(200, json={"purpose": {"a": 1}}),
    })
    async with client:
        out = await client.update_purpose("s1", "creator-tok", {"a": 1})
    assert out == {"purpose": {"a": 1}}
    assert seen[0].headers["authorization"] == "Bearer creator-tok"
    assert json.loads(seen[0].content) == {"purpose": {"a": 1}}


@pytest.mark.asyncio
async def test_credential_minting_and_access() -> None:
    client, seen = _recording_client({
        ("POST", "/v1/sessions/s1/join-tokens"): httpx.Response(
            201, json={"join_token": "jt2", "expires_at": "z", "invite_url": "u",
                       "invite_link": "l"}),
        ("POST", "/v1/sessions/s1/supervisor-tokens"): httpx.Response(
            201, json={"token_id": "tid", "supervisor_token": "st"}),
        ("GET", "/v1/sessions/s1/supervisor-tokens"): httpx.Response(
            200, json=[{"token_id": "tid", "created_at": "c", "revoked_at": None}]),
        ("DELETE", "/v1/sessions/s1/supervisor-tokens/tid"): httpx.Response(204),
        ("GET", "/v1/sessions/s1/access"): httpx.Response(
            200, json={"role": "creator", "participant_id": None}),
    })
    async with client:
        assert (await client.mint_join_token("s1", "ct"))["join_token"] == "jt2"
        assert (await client.mint_supervisor_token("s1", "ct"))["token_id"] == "tid"
        listed = await client.list_supervisor_tokens("s1", "ct")
        assert listed[0]["token_id"] == "tid"
        assert await client.revoke_supervisor_token("s1", "ct", "tid") is None
        assert (await client.access("s1", "ct"))["role"] == "creator"
    assert all(r.headers["authorization"] == "Bearer ct" for r in seen)


@pytest.mark.asyncio
async def test_flat_error_shape_maps_code() -> None:
    client, _ = _recording_client({
        ("GET", "/v1/sessions/s1/access"): httpx.Response(429, json={"error": "rate_limited"}),
    })
    async with client:
        with pytest.raises(SessionWeaveError) as ei:
            await client.access("s1", "bad")
    assert ei.value.code == "rate_limited"
    assert ei.value.status == 429


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "link",
    [
        "swvs://evil.invalid/s1?jt=tok",
        "swv://relay.invalid/s1?jt=tok",  # http link vs https client
        "https://evil.invalid/invite/s1?jt=tok",
        "http://relay.invalid:8080/invite/s1?jt=tok",
    ],
)
async def test_join_link_rejects_other_relay(link: str) -> None:
    client, seen = _recording_client({})
    client.base_url = "https://relay.invalid"
    async with client:
        with pytest.raises(ValueError):
            await client.join_link(link)
    assert seen == []
