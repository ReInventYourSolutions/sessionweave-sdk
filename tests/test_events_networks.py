"""Token networks, approvals and account events, via ``httpx.MockTransport``."""
from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from sessionweave_client import SessionWeaveClient, SessionWeaveError


def _client(
    seen: list[httpx.Request], *, response: httpx.Response | None = None
) -> SessionWeaveClient:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return response or httpx.Response(
            200, json={"events": [], "next": 0, "truncated": False}
        )

    c = SessionWeaveClient(base_url="http://relay.invalid", account_token="acct")
    c._http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://relay.invalid"
    )
    return c


def _body(req: httpx.Request) -> Any:
    return json.loads(req.content) if req.content else None


@pytest.mark.asyncio
async def test_create_my_token_network_fields() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        await c.create_my_token("ci", network_policy="approved", approved_networks=["1.2.3.4"])
        await c.create_my_token("plain")
    assert _body(seen[0]) == {
        "label": "ci", "network_policy": "approved", "approved_networks": ["1.2.3.4"]
    }
    assert _body(seen[1]) == {"label": "plain"}


@pytest.mark.asyncio
async def test_my_token_networks_get_put() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        await c.my_token_networks("t/1")
        await c.set_my_token_networks("t/1", network_policy="any")
    assert seen[0].method == "GET" and seen[0].url.raw_path == b"/v1/me/tokens/t%2F1/networks"
    assert seen[1].method == "PUT" and _body(seen[1]) == {"network_policy": "any"}
    assert seen[1].headers["authorization"] == "Bearer acct"


@pytest.mark.asyncio
async def test_my_approvals() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        await c.my_network_approvals()
        await c.approve_my_network("a1")
        await c.approve_my_network("a1", scope="network")
        await c.deny_my_network("a1")
    assert seen[0].url.path == "/v1/me/network-approvals"
    assert seen[1].url.path == "/v1/me/network-approvals/a1/approve"
    assert _body(seen[1]) == {"scope": "ip"}
    assert _body(seen[2]) == {"scope": "network"}
    assert seen[3].url.path == "/v1/me/network-approvals/a1/deny"


@pytest.mark.asyncio
async def test_my_events_params_and_timeout() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        r = await c.my_events(after=5, limit=10, wait=20)
        await c.my_events()
    assert r == {"events": [], "next": 0, "truncated": False}
    q = seen[0].url.params
    assert (q["after"], q["limit"], q["wait"]) == ("5", "10", "20")
    assert seen[0].extensions["timeout"]["read"] >= 30
    assert dict(seen[1].url.params) == {"after": "0"}


@pytest.mark.asyncio
async def test_admin_networks_and_approvals() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        await c.admin_token_networks("adm", "acc", "tok")
        await c.admin_set_token_networks(
            "adm", "acc", "tok", network_policy="approved", approved_networks=[]
        )
        await c.admin_network_approvals("adm", "acc")
        await c.admin_approve_network("adm", "acc", "ap", scope="network")
        await c.admin_deny_network("adm", "acc", "ap")
    assert seen[0].url.path == "/v1/accounts/acc/tokens/tok/networks"
    assert seen[0].headers["authorization"] == "Bearer adm"
    assert seen[1].method == "PUT"
    assert _body(seen[1]) == {"network_policy": "approved", "approved_networks": []}
    assert seen[2].url.path == "/v1/accounts/acc/network-approvals"
    assert seen[3].url.path == "/v1/accounts/acc/network-approvals/ap/approve"
    assert _body(seen[3]) == {"scope": "network"}
    assert seen[4].url.path.endswith("/deny")


@pytest.mark.asyncio
async def test_admin_create_token_and_events() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        await c.admin_create_account_token(
            "adm", "acc", "l", network_policy="any", approved_networks=["x"]
        )
        await c.admin_account_events("adm", after=3, account_id="acc", limit=7, wait=15)
    assert _body(seen[0]) == {"label": "l", "network_policy": "any", "approved_networks": ["x"]}
    assert seen[1].url.path == "/v1/accounts/events"
    q = seen[1].url.params
    assert (q["after"], q["account_id"], q["limit"], q["wait"]) == ("3", "acc", "7", "15")
    assert seen[1].extensions["timeout"]["read"] >= 25


@pytest.mark.asyncio
async def test_network_errors_surface_code() -> None:
    for status, code in ((403, "network_not_approved"), (404, "approval_not_found")):
        seen: list[httpx.Request] = []
        resp = httpx.Response(status, json={"error": {"code": code, "message": "nope"}})
        async with _client(seen, response=resp) as c:
            with pytest.raises(SessionWeaveError) as ei:
                await c.my_events()
        assert ei.value.code == code and ei.value.message == "nope" and ei.value.status == status
