"""Machines and agent-definition methods, via ``httpx.MockTransport``."""
from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from sessionweave_client import SessionWeaveClient, SessionWeaveError

KEY = "lk_" + "a" * 32


def _client(
    seen: list[httpx.Request], *, response: httpx.Response | None = None, token: str = "acct"
) -> SessionWeaveClient:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return response or httpx.Response(200, json={"launchers": []})

    c = SessionWeaveClient(base_url="http://relay.invalid", account_token=token)
    c._http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://relay.invalid"
    )
    return c


def _body(req: httpx.Request) -> Any:
    return json.loads(req.content) if req.content else None


@pytest.mark.asyncio
async def test_deploy_my_definition() -> None:
    seen: list[httpx.Request] = []
    resp = httpx.Response(200, json={"definition_id": "obs", "revision": 1, "state": "sent"})
    async with _client(seen, response=resp) as c:
        out = await c.deploy_my_definition(KEY, {"id": "obs/x"})
    assert seen[0].method == "PUT"
    raw = seen[0].url.raw_path.decode().split("?")[0]
    assert raw == f"/v1/me/launchers/{KEY}/definitions/obs%2Fx"
    assert _body(seen[0]) == {"definition": {"id": "obs/x"}} and out["revision"] == 1
    assert seen[0].headers["authorization"] == "Bearer acct"


@pytest.mark.asyncio
async def test_list_get_remove_paths() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        await c.my_launchers()
        await c.my_launcher(KEY)
        await c.remove_my_definition(KEY, "d1")
    assert [(r.method, r.url.path) for r in seen] == [
        ("GET", "/v1/me/launchers"),
        ("GET", f"/v1/me/launchers/{KEY}"),
        ("DELETE", f"/v1/me/launchers/{KEY}/definitions/d1"),
    ]


@pytest.mark.asyncio
async def test_browse_query_params() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        await c.browse_my_launcher(KEY, "/srv", cursor="b", limit=50, hidden=True)
        await c.browse_my_launcher(KEY)
    assert dict(seen[0].url.params) == {"path": "/srv", "cursor": "b", "limit": "50", "hidden": "1"}
    assert seen[0].url.path == f"/v1/me/launchers/{KEY}/browse"
    assert dict(seen[1].url.params) == {}


@pytest.mark.asyncio
async def test_processes_query_params() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        await c.my_launcher_processes(KEY, cursor=42, limit=20, query="nginx")
    assert dict(seen[0].url.params) == {"cursor": "42", "limit": "20", "q": "nginx"}


@pytest.mark.asyncio
async def test_forget_returns_none_on_204() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen, response=httpx.Response(204)) as c:
        await c.forget_my_launcher(KEY)
        await c.forget_launcher(KEY)
    assert seen[0].method == "DELETE" and seen[0].url.path == f"/v1/me/launchers/{KEY}"
    assert seen[1].url.path == f"/v1/launchers/{KEY}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,code",
    [
        (409, "definition_in_use"),
        (409, "launcher_offline"),
        (409, "definition_limit"),
        (504, "launcher_timeout"),
        (502, "launcher_bad_reply"),
        (404, "path_not_found"),
        (503, "definitions_unavailable"),
    ],
)
async def test_errors_raise_sessionweave_error(status: int, code: str) -> None:
    seen: list[httpx.Request] = []
    resp = httpx.Response(status, json={"error": {"code": code, "message": "m"}})
    async with _client(seen, response=resp) as c:
        with pytest.raises(SessionWeaveError) as ei:
            await c.browse_my_launcher(KEY, "/x")
    assert ei.value.code == code and ei.value.status == status


@pytest.mark.asyncio
async def test_requires_account_token() -> None:
    with pytest.raises(ValueError, match="account_token"):
        await SessionWeaveClient(base_url="http://x.invalid", account_token="").my_launchers()


@pytest.mark.asyncio
async def test_open_mode_methods_send_no_credentials() -> None:
    seen: list[httpx.Request] = []
    resp = httpx.Response(200, json={"launchers": []})
    async with _client(seen, response=resp) as c:
        await c.launchers()
        await c.launcher(KEY)
        await c.deploy_definition(KEY, {"id": "d"})
        await c.remove_definition(KEY, "d")
        await c.browse_launcher(KEY, "/srv")
        await c.launcher_processes(KEY, query="x")
    assert [r.url.path for r in seen] == [
        "/v1/launchers",
        f"/v1/launchers/{KEY}",
        f"/v1/launchers/{KEY}/definitions/d",
        f"/v1/launchers/{KEY}/definitions/d",
        f"/v1/launchers/{KEY}/browse",
        f"/v1/launchers/{KEY}/processes",
    ]
    assert all("authorization" not in r.headers for r in seen)
