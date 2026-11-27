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
        out = await c.deploy_my_definition(KEY, {"id": "obs.x"})
    assert seen[0].method == "PUT"
    raw = seen[0].url.raw_path.decode().split("?")[0]
    assert raw == f"/v1/me/launchers/{KEY}/definitions/obs.x"
    assert _body(seen[0]) == {"definition": {"id": "obs.x"}} and out["revision"] == 1
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


@pytest.mark.asyncio
async def test_processes_cursor_accepts_digit_string() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        await c.my_launcher_processes(KEY, cursor="42")
        await c.launcher_processes(KEY, cursor=0)
    assert dict(seen[0].url.params) == {"cursor": "42"}
    assert dict(seen[1].url.params) == {"cursor": "0"}
    async with _client([]) as c:
        for bad in ("abc", "", "-1", "4 2"):
            with pytest.raises(ValueError, match="cursor"):
                await c.my_launcher_processes(KEY, cursor=bad)


@pytest.mark.asyncio
async def test_path_and_query_values_are_encoded() -> None:
    seen: list[httpx.Request] = []
    odd = "a b\\c?d#e&f=g%h"
    async with _client(seen) as c:
        await c.my_launcher(odd)
        await c.remove_my_definition(KEY, odd)
        await c.browse_my_launcher(KEY, "/srv/my dir&x=1#f?", cursor="a&b=c")
        await c.my_launcher_processes(KEY, query="ng inx&a=b#c")
    enc = "a%20b%5Cc%3Fd%23e%26f%3Dg%25h"
    assert seen[0].url.raw_path.decode() == f"/v1/me/launchers/{enc}"
    assert seen[1].url.raw_path.decode() == f"/v1/me/launchers/{KEY}/definitions/{enc}"
    assert dict(seen[2].url.params) == {"path": "/srv/my dir&x=1#f?", "cursor": "a&b=c"}
    assert "#" not in seen[2].url.raw_path.decode()
    assert dict(seen[3].url.params) == {"q": "ng inx&a=b#c"}
    assert seen[3].url.raw_path.decode().count("?") == 1


@pytest.mark.asyncio
async def test_open_mode_deploy_body_and_no_credentials() -> None:
    seen: list[httpx.Request] = []
    d = {"id": "obs.x", "runtime": "claude"}
    async with _client(seen, token="acct") as c:
        await c.deploy_definition(KEY, d)
    assert seen[0].method == "PUT"
    assert _body(seen[0]) == {"definition": d}
    assert seen[0].url.raw_path.decode() == f"/v1/launchers/{KEY}/definitions/obs.x"
    assert "authorization" not in seen[0].headers


@pytest.mark.asyncio
@pytest.mark.parametrize("definition", [{}, {"id": ""}, {"id": None}, {"id": "  "}])
async def test_deploy_requires_definition_id(definition: dict[str, Any]) -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        with pytest.raises(ValueError, match="id"):
            await c.deploy_my_definition(KEY, definition)
        with pytest.raises(ValueError, match="id"):
            await c.deploy_definition(KEY, definition)
    assert seen == []


BAD_SEGMENTS = ["", ".", "..", "...", "a/b", "/", "../x", "x/.."]


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", BAD_SEGMENTS)
async def test_bad_path_segments_rejected_before_any_request(bad: str) -> None:
    seen: list[httpx.Request] = []
    d = {"id": bad}
    async with _client(seen) as c:
        calls = [
            c.my_launcher(bad),
            c.forget_my_launcher(bad),
            c.browse_my_launcher(bad),
            c.my_launcher_processes(bad),
            c.launcher(bad),
            c.forget_launcher(bad),
            c.browse_launcher(bad),
            c.launcher_processes(bad),
            c.remove_my_definition(KEY, bad),
            c.remove_definition(KEY, bad),
            c.remove_my_definition(bad, "d1"),
            c.remove_definition(bad, "d1"),
            c.deploy_my_definition(bad, {"id": "d1"}),
            c.deploy_definition(bad, {"id": "d1"}),
        ]
        if bad.strip():
            calls += [c.deploy_my_definition(KEY, d), c.deploy_definition(KEY, d)]
        for coro in calls:
            with pytest.raises(ValueError, match=r"path segment|id"):
                await coro
    assert seen == []


@pytest.mark.asyncio
@pytest.mark.parametrize("ok", ["a.b", ".hidden", "a..b", "d_1-x", "lk_" + "b" * 32])
async def test_normal_ids_still_encode(ok: str) -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        await c.remove_my_definition(KEY, ok)
        await c.remove_definition(KEY, ok)
    assert seen[0].url.raw_path.decode() == f"/v1/me/launchers/{KEY}/definitions/{ok}"
    assert seen[1].url.raw_path.decode() == f"/v1/launchers/{KEY}/definitions/{ok}"
